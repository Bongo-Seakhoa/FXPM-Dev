"""Authenticated, bounded composition of immutable native-history runs.

This module is intentionally standalone and has no activation or acquisition
side effects.  It describes a composite run as ordered per-cell references to
immutable origin artifacts, verifies those origins from the native root, and
streams rows across the references without ever materialising a merged file or
dataframe.

Legacy artifacts may be adopted only with an explicit limitation attestation.
That attestation is part of the composite manifest hash and states that the
original pagination-attempt ledger is unavailable; it never upgrades or
renames legacy provenance as the current V5 acquisition contract.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import stat
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, BinaryIO, Iterator, Mapping, Optional

from pm_native_consensus import (
    CONSENSUS_POLICY as NATIVE_CONSENSUS_POLICY,
    REFERENCE_GRAPH as NATIVE_REFERENCE_GRAPH,
    REQUIRED_AUDIT_WINDOWS,
)
from pm_native_lifecycle import (
    PIN_SCHEMA,
    NativeLifecycleAuthority,
    validate_durable_local_root,
)


COMPOSITE_SCHEMA_VERSION = "fxpm.native_composite.run.v1"
SEGMENT_SCHEMA_VERSION = "fxpm.native_composite.segment.v1"
LEGACY_ADOPTION_SCHEMA_VERSION = "fxpm.native_composite.legacy_adoption.v1"
SHORTFALL_SCHEMA_VERSION = "fxpm.native_composite.acquisition_shortfall.v1"
PROTECTION_GRAPH_SCHEMA_VERSION = "fxpm.native_composite.protection_graph.v1"
RECEIPT_SCHEMA_VERSION = "fxpm.native_composite.receipt.v1"
PUBLICATION_RECEIPT_SCHEMA_VERSION = "fxpm.native_composite.publication_receipt.v1"
CURRENT_PROVENANCE_SCHEMA_VERSION = "fxpm-native-bars-v1"
CURRENT_ACQUISITION_CONTRACT_VERSION = (
    "2026-07-12-native-calendar-acquisition-v3"
)
CURRENT_PAGINATION_CONTRACT_VERSION = "fxpm.native_history.pagination.v2"
CURRENT_WINDOW_CONTRACT_VERSION = "fxpm-timeframe-evidence-v3"
CURRENT_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = (
    "fxpm.native_history.timeframe_request_map.v5"
)
BROKER_SESSION_ALIGNMENT_MODE = (
    "broker_observed_successor_or_nominal_close_v1"
)
FIXED_ALIGNMENT_MODE = "fixed_utc_native_boundary_v1"
LEGACY_MISSING_ATTEMPT_LEDGER = "MISSING_ORIGINAL_PAGINATION_ATTEMPT_LEDGER"
LEGACY_ABSENT_PAGINATION_CONTRACT = "ABSENT_IN_ORIGINAL_PROVENANCE"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:@+\-]{1,160}$")
_PATH_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.@+\-]{1,240}$")
_UTC_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[T ]"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?:Z|\+00:00)$"
)
_TIMEFRAME_ORDER = ("M5", "M15", "M30", "H1", "H4", "D1")
_TIMEFRAMES = frozenset(_TIMEFRAME_ORDER)
_TIMEFRAME_SECONDS = {
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}
_DEFAULT_PAGE_DAYS = {
    "M5": 30,
    "M15": 90,
    "M30": 180,
    "H1": 365,
    "H4": 1460,
    "D1": 3650,
}
_CSV_HEADER = (
    "time",
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
_CSV_PROVENANCE_COLUMNS = _CSV_HEADER[1:]
_MAX_PROTECTION_ROOTS = 96_000
_MAX_RECEIPT_CELLS = 600
_MAX_SCHEMA_CELLS = 600
_MAX_SCHEMA_SEGMENTS_PER_CELL = 64
_MAX_SCHEMA_TOTAL_SEGMENTS = 32_000
_SHORTFALL_REASON_CODES = frozenset(
    {
        "BROKER_HISTORY_ABSENT",
        "BROKER_PAGE_EMPTY",
        "BROKER_BOUNDARY_TRUNCATED",
    }
)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_V5_COMMON_REQUEST_KEYS = (
    "window_contract_version",
    "timeframe_request_map_schema_version",
    "horizon_years",
    "requested_information_cutoff_utc",
    "requested_horizon_start_utc",
    "common_requested_start_utc",
    "required_evidence_lead_start_utc",
    "warmup_days",
    "safety_days",
    "closed_bar_slack_seconds",
    "common_as_of_utc",
    "boundary_tolerance_days",
    "embargo_days",
    "timeframe_window_years",
    "timeframe_calendar_boundaries",
    "timeframe_calendar_boundaries_sha256",
    "timeframe_required_evidence_lead_start_utc",
    "timeframe_source_request_intervals",
    "timeframe_request_map_sha256",
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_consumer_map",
    "timeframe_audit_windows",
    "timeframe_audit_reference_intervals",
    "timeframe_audit_only_intervals",
    "timeframe_consensus_policy",
)
_WINDOWS_UNSAFE_ATTRIBUTES = (
    0x00000400  # REPARSE_POINT (junctions included)
    | 0x00001000  # OFFLINE
    | 0x00040000  # RECALL_ON_OPEN
    | 0x00100000  # UNPINNED
    | 0x00400000  # RECALL_ON_DATA_ACCESS
)


class NativeCompositeError(RuntimeError):
    """Base class for fail-closed composite errors."""


class CompositeSchemaError(NativeCompositeError):
    """A composite schema or canonical value is invalid."""


class CompositeIntegrityError(NativeCompositeError):
    """An origin artifact, hash, identity, or stitch invariant failed."""


class CompositeCapacityError(NativeCompositeError):
    """A finite manifest, artifact, row, cell, or segment cap was exceeded."""


class CompositePathError(NativeCompositeError):
    """An origin reference is unsafe or escapes the durable native root."""


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    if depth > 48:
        raise CompositeSchemaError("canonical JSON nesting exceeds 48 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CompositeSchemaError("canonical JSON forbids NaN and infinity")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CompositeSchemaError("canonical JSON keys must be strings")
            result[key] = _json_safe(item, depth=depth + 1)
        return result
    if isinstance(value, (tuple, list)):
        return [_json_safe(item, depth=depth + 1) for item in value]
    raise CompositeSchemaError(
        f"unsupported canonical JSON value: {type(value).__name__}"
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _timeframe_request_hash_payload(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": request.get("timeframe_request_map_schema_version"),
        "window_contract_version": request.get("window_contract_version"),
        "common_as_of_utc": request.get("common_as_of_utc"),
        "timeframe_window_years": request.get("timeframe_window_years"),
        "timeframe_calendar_boundaries": request.get(
            "timeframe_calendar_boundaries"
        ),
        "timeframe_required_evidence_lead_start_utc": request.get(
            "timeframe_required_evidence_lead_start_utc"
        ),
        "timeframe_source_request_intervals": request.get(
            "timeframe_source_request_intervals"
        ),
        "timeframe_strategy_evidence_intervals": request.get(
            "timeframe_strategy_evidence_intervals"
        ),
        "timeframe_audit_reference_map": request.get(
            "timeframe_audit_reference_map"
        ),
        "timeframe_audit_consumer_map": request.get(
            "timeframe_audit_consumer_map"
        ),
        "timeframe_audit_windows": request.get("timeframe_audit_windows"),
        "timeframe_audit_reference_intervals": request.get(
            "timeframe_audit_reference_intervals"
        ),
        "timeframe_audit_only_intervals": request.get(
            "timeframe_audit_only_intervals"
        ),
        "timeframe_consensus_policy": request.get("timeframe_consensus_policy"),
    }


def _request_interval(value: Any, label: str) -> tuple[str, str]:
    if not isinstance(value, Mapping):
        raise CompositeIntegrityError(f"{label} is not an interval object")
    start = _normal_timestamp(value.get("start_utc"), f"{label}.start_utc")
    end = _normal_timestamp(
        value.get("end_exclusive_utc"), f"{label}.end_exclusive_utc"
    )
    if _timestamp(start) >= _timestamp(end):
        raise CompositeIntegrityError(f"{label} is empty or reversed")
    return start, end


def _validate_v5_common_request(
    request: Any,
    *,
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(request, Mapping):
        raise CompositeIntegrityError(f"{label} lacks a V5 request object")
    missing = [key for key in _V5_COMMON_REQUEST_KEYS if key not in request]
    if missing:
        raise CompositeIntegrityError(
            f"{label} lacks immutable V5 request fields: {missing}"
        )
    if (
        request.get("window_contract_version") != CURRENT_WINDOW_CONTRACT_VERSION
        or request.get("timeframe_request_map_schema_version")
        != CURRENT_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        raise CompositeIntegrityError(
            f"{label} is not the exact current V5 timeframe request contract"
        )
    boundaries = request.get("timeframe_calendar_boundaries")
    if not isinstance(boundaries, Mapping) or not boundaries:
        raise CompositeIntegrityError(f"{label} has no timeframe boundary map")
    if request.get("timeframe_calendar_boundaries_sha256") != _canonical_sha256(
        boundaries
    ):
        raise CompositeIntegrityError(f"{label} timeframe boundary hash mismatch")
    if request.get("timeframe_request_map_sha256") != _canonical_sha256(
        _timeframe_request_hash_payload(request)
    ):
        raise CompositeIntegrityError(f"{label} timeframe request-map hash mismatch")
    if request.get("timeframe_consensus_policy") != NATIVE_CONSENSUS_POLICY:
        raise CompositeIntegrityError(f"{label} consensus policy mismatch")

    reference_map = request.get("timeframe_audit_reference_map")
    consumer_map = request.get("timeframe_audit_consumer_map")
    expected_references = {
        timeframe: list(NATIVE_REFERENCE_GRAPH[timeframe])
        for timeframe in _TIMEFRAME_ORDER
    }
    expected_consumers = {
        timeframe: [
            target
            for target in _TIMEFRAME_ORDER
            if timeframe in NATIVE_REFERENCE_GRAPH[target]
        ]
        for timeframe in _TIMEFRAME_ORDER
    }
    if reference_map != expected_references or consumer_map != expected_consumers:
        raise CompositeIntegrityError(f"{label} native audit graph mismatch")

    years = request.get("timeframe_window_years")
    leads = request.get("timeframe_required_evidence_lead_start_utc")
    source_intervals = request.get("timeframe_source_request_intervals")
    strategy_intervals = request.get("timeframe_strategy_evidence_intervals")
    audit_windows = request.get("timeframe_audit_windows")
    audit_reference_intervals = request.get("timeframe_audit_reference_intervals")
    audit_only_intervals = request.get("timeframe_audit_only_intervals")
    maps = (
        years,
        leads,
        source_intervals,
        strategy_intervals,
        audit_windows,
        audit_reference_intervals,
        audit_only_intervals,
    )
    if any(not isinstance(item, Mapping) for item in maps):
        raise CompositeIntegrityError(f"{label} contains a malformed timeframe map")
    for timeframe in boundaries:
        if timeframe not in _TIMEFRAMES:
            raise CompositeIntegrityError(f"{label} names an unsupported timeframe")
        if any(timeframe not in item for item in maps):
            raise CompositeIntegrityError(
                f"{label} omits a mapped projection for {timeframe}"
            )
        cell_boundaries = boundaries[timeframe]
        cell_years = years[timeframe]
        cell_windows = audit_windows[timeframe]
        if not all(
            isinstance(item, Mapping)
            for item in (cell_boundaries, cell_years, cell_windows)
        ):
            raise CompositeIntegrityError(
                f"{label} has malformed calendar projections for {timeframe}"
            )
        if set(cell_boundaries) != {
            "optimizer_train",
            "tuning_validation",
            "final_lockbox",
        } or set(cell_years) != set(cell_boundaries):
            raise CompositeIntegrityError(
                f"{label} has incomplete evidence partitions for {timeframe}"
            )
        for partition in cell_boundaries:
            boundary = cell_boundaries[partition]
            if not isinstance(boundary, Mapping):
                raise CompositeIntegrityError(
                    f"{label} has malformed {timeframe} {partition} boundary"
                )
            start = _normal_timestamp(
                boundary.get("start_ts"), f"{label}.{timeframe}.{partition}.start"
            )
            end = _normal_timestamp(
                boundary.get("end_ts"), f"{label}.{timeframe}.{partition}.end"
            )
            if _timestamp(start) >= _timestamp(end):
                raise CompositeIntegrityError(
                    f"{label} has reversed {timeframe} {partition} boundary"
                )
            _artifact_positive_integer(
                cell_years[partition],
                f"{label}.{timeframe}.{partition}.years",
            )
        if set(cell_windows) != set(REQUIRED_AUDIT_WINDOWS):
            raise CompositeIntegrityError(
                f"{label} has incomplete audit windows for {timeframe}"
            )
        for window_name, interval in cell_windows.items():
            _request_interval(interval, f"{label}.{timeframe}.{window_name}")
        _request_interval(
            source_intervals[timeframe], f"{label}.{timeframe}.source_interval"
        )
        _request_interval(
            strategy_intervals[timeframe],
            f"{label}.{timeframe}.strategy_interval",
        )
        _normal_timestamp(leads[timeframe], f"{label}.{timeframe}.lead_start")
        references = audit_reference_intervals[timeframe]
        if not isinstance(references, Mapping) or set(references) != set(
            NATIVE_REFERENCE_GRAPH[timeframe]
        ):
            raise CompositeIntegrityError(
                f"{label} has incomplete reference intervals for {timeframe}"
            )
        for reference, windows in references.items():
            if not isinstance(windows, Mapping) or set(windows) != set(
                REQUIRED_AUDIT_WINDOWS
            ):
                raise CompositeIntegrityError(
                    f"{label} has malformed {timeframe}/{reference} audit windows"
                )
            for window_name, interval in windows.items():
                _request_interval(
                    interval,
                    f"{label}.{timeframe}.{reference}.{window_name}",
                )
        if not isinstance(audit_only_intervals[timeframe], list):
            raise CompositeIntegrityError(
                f"{label} audit-only intervals are malformed for {timeframe}"
            )
    return {key: _json_safe(request[key]) for key in _V5_COMMON_REQUEST_KEYS}


def _validate_v5_leaf_projection(
    request: Mapping[str, Any],
    common: Mapping[str, Any],
    timeframe: str,
    *,
    label: str,
) -> None:
    if any(_json_safe(request.get(key)) != common[key] for key in common):
        raise CompositeIntegrityError(
            f"{label} does not match the run's hash-bound V5 request contract"
        )
    checks = {
        "cell_timeframe": timeframe,
        "cell_timeframe_window_years": common["timeframe_window_years"][timeframe],
        "cell_calendar_boundaries": common["timeframe_calendar_boundaries"][timeframe],
        "cell_required_evidence_lead_start_utc": common[
            "timeframe_required_evidence_lead_start_utc"
        ][timeframe],
        "source_request_interval": common["timeframe_source_request_intervals"][
            timeframe
        ],
        "strategy_evidence_interval": common[
            "timeframe_strategy_evidence_intervals"
        ][timeframe],
        "audit_reference_timeframes": common["timeframe_audit_reference_map"][
            timeframe
        ],
        "audit_consumer_targets": common["timeframe_audit_consumer_map"][timeframe],
        "audit_windows": common["timeframe_audit_windows"][timeframe],
        "audit_reference_intervals": common[
            "timeframe_audit_reference_intervals"
        ][timeframe],
        "audit_only_intervals": common["timeframe_audit_only_intervals"][timeframe],
    }
    missing = [
        field
        for field, expected in checks.items()
        if field not in request or _json_safe(request[field]) != _json_safe(expected)
    ]
    if missing:
        raise CompositeIntegrityError(
            f"{label} has false or missing V5 leaf projections: {missing}"
        )


def _strict_json_mapping(raw: bytes, label: str) -> Mapping[str, Any]:
    def object_without_duplicates(
        pairs: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    try:
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
        )
        safe = _json_safe(value)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
        CompositeSchemaError,
    ) as exc:
        raise CompositeIntegrityError(
            f"{label} is not strict bounded UTF-8 JSON"
        ) from exc
    if not isinstance(safe, Mapping):
        raise CompositeIntegrityError(f"{label} is not a JSON object")
    return safe


def _normal_identifier(value: Any, name: str, *, upper: bool = False) -> str:
    result = str(value or "").strip()
    if upper:
        result = result.upper()
    if not _IDENTIFIER_RE.fullmatch(result):
        raise CompositeSchemaError(f"{name} is not a canonical identifier")
    return result


def _normal_source_label(value: Any, name: str) -> str:
    if not isinstance(value, str) or value != value.strip() or not value:
        raise CompositeSchemaError(f"{name} is not a canonical source label")
    if len(value) > 256 or any(ord(character) < 32 for character in value):
        raise CompositeSchemaError(f"{name} is not a canonical source label")
    return value


def _normal_sha256(value: Any, name: str) -> str:
    result = str(value or "")
    if not _SHA256_RE.fullmatch(result):
        raise CompositeSchemaError(f"{name} must be a lowercase SHA-256 digest")
    return result


def _normal_positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CompositeSchemaError(f"{name} must be a positive integer")
    return value


def _artifact_positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CompositeIntegrityError(
            f"{name} is not a positive integer in the hashed artifact"
        )
    return value


def _artifact_nonnegative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CompositeIntegrityError(
            f"{name} is not a nonnegative integer in the hashed artifact"
        )
    return value


def _utc_nanoseconds(value: Any, name: str) -> int:
    raw = str(value or "").strip()
    match = _UTC_TIMESTAMP_RE.fullmatch(raw)
    if match is None:
        raise CompositeSchemaError(
            f"{name} must be a canonical UTC ISO-8601 timestamp"
        )
    try:
        whole = datetime.strptime(
            f"{match.group('date')}T{match.group('time')}",
            "%Y-%m-%dT%H:%M:%S",
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise CompositeSchemaError(
            f"{name} must be a valid UTC ISO-8601 timestamp"
        ) from exc
    delta = whole - _EPOCH
    whole_seconds = delta.days * 86_400 + delta.seconds
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return whole_seconds * 1_000_000_000 + int(fraction or "0")


def _normal_timestamp(value: Any, name: str) -> str:
    nanoseconds = _utc_nanoseconds(value, name)
    if nanoseconds % 1_000:
        raise CompositeSchemaError(f"{name} exceeds microsecond precision")
    parsed = _EPOCH + timedelta(microseconds=nanoseconds // 1_000)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _normal_relative_path(value: Any, name: str) -> str:
    raw = str(value or "")
    if not raw or "\\" in raw or "\x00" in raw:
        raise CompositeSchemaError(f"{name} is not a canonical POSIX path")
    path = PurePosixPath(raw)
    if path.is_absolute() or raw != path.as_posix():
        raise CompositeSchemaError(f"{name} must be a canonical relative path")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise CompositeSchemaError(f"{name} contains traversal or empty components")
    if any(
        _PATH_COMPONENT_RE.fullmatch(part) is None
        or part.endswith((".", " "))
        for part in path.parts
    ):
        raise CompositeSchemaError(
            f"{name} contains an unsafe or non-portable component"
        )
    return path.as_posix()


def _finite_positive_float(value: Any, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CompositeSchemaError(f"{name} must be finite and positive") from exc
    if not math.isfinite(result) or result <= 0.0:
        raise CompositeSchemaError(f"{name} must be finite and positive")
    return result


@dataclass(frozen=True)
class BrokerSourceIdentity:
    broker: str
    server: str
    account_id: str
    source_kind: str
    source_environment_sha256: str

    def __post_init__(self) -> None:
        for name in ("broker", "server"):
            object.__setattr__(
                self,
                name,
                _normal_source_label(getattr(self, name), name),
            )
        for name in ("account_id", "source_kind"):
            object.__setattr__(
                self,
                name,
                _normal_identifier(getattr(self, name), name, upper=False),
            )
        object.__setattr__(
            self,
            "source_environment_sha256",
            _normal_sha256(
                self.source_environment_sha256, "source_environment_sha256"
            ),
        )

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LegacyAdoptionAttestation:
    origin_manifest_sha256: str
    bars_sha256: str
    provenance_sha256: str
    bars_bytes: int
    provenance_bytes: int
    rows: int
    original_provenance_schema_version: str
    original_acquisition_contract_version: str
    original_pagination_contract_version: str
    origin_manifest_declared_immutable: bool
    limitation_codes: tuple[str, ...] = (LEGACY_MISSING_ATTEMPT_LEDGER,)
    original_attempt_ledger_present: bool = False
    eligible_as_v5: bool = False
    provenance_classification: str = "LEGACY_ADOPTED_NOT_V5"
    schema_version: str = LEGACY_ADOPTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LEGACY_ADOPTION_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported legacy adoption schema")
        for name in (
            "origin_manifest_sha256",
            "bars_sha256",
            "provenance_sha256",
        ):
            object.__setattr__(
                self, name, _normal_sha256(getattr(self, name), name)
            )
        for name in ("bars_bytes", "provenance_bytes", "rows"):
            object.__setattr__(
                self, name, _normal_positive_integer(getattr(self, name), name)
            )
        for name in (
            "original_provenance_schema_version",
            "original_acquisition_contract_version",
            "original_pagination_contract_version",
        ):
            object.__setattr__(
                self, name, _normal_identifier(getattr(self, name), name)
            )
        raw_limitations = tuple(self.limitation_codes)
        normalized_limitations = tuple(
            _normal_identifier(item, "limitation_code", upper=True)
            for item in raw_limitations
        )
        limitations = tuple(sorted(set(normalized_limitations)))
        if normalized_limitations != raw_limitations or len(limitations) != len(
            raw_limitations
        ):
            raise CompositeSchemaError(
                "legacy limitation codes must be unique and canonical"
            )
        if limitations != (LEGACY_MISSING_ATTEMPT_LEDGER,):
            raise CompositeSchemaError(
                "legacy adoption must state the missing original attempt ledger"
            )
        object.__setattr__(self, "limitation_codes", limitations)
        if self.original_attempt_ledger_present is not False:
            raise CompositeSchemaError(
                "missing-ledger adoption cannot claim an original attempt ledger"
            )
        if self.eligible_as_v5 is not False:
            raise CompositeSchemaError("legacy adoption can never be V5 eligible")
        if self.origin_manifest_declared_immutable is not True:
            raise CompositeSchemaError(
                "a mutable legacy origin cannot be adopted as immutable authority"
            )
        if self.provenance_classification != "LEGACY_ADOPTED_NOT_V5":
            raise CompositeSchemaError(
                "legacy provenance classification cannot masquerade as V5"
            )
        if (
            self.original_acquisition_contract_version
            == CURRENT_ACQUISITION_CONTRACT_VERSION
            or self.original_pagination_contract_version
            == CURRENT_PAGINATION_CONTRACT_VERSION
        ):
            raise CompositeSchemaError(
                "current V5 contracts cannot be labelled as legacy adoption"
            )

    def canonical(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "limitation_codes": list(self.limitation_codes),
        }


@dataclass(frozen=True)
class AcquisitionShortfall:
    start_utc: str
    end_exclusive_utc: str
    reason_code: str
    evidence_sha256: str
    schema_version: str = SHORTFALL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SHORTFALL_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported acquisition shortfall schema")
        object.__setattr__(
            self, "start_utc", _normal_timestamp(self.start_utc, "shortfall.start_utc")
        )
        object.__setattr__(
            self,
            "end_exclusive_utc",
            _normal_timestamp(
                self.end_exclusive_utc, "shortfall.end_exclusive_utc"
            ),
        )
        if _timestamp(self.start_utc) >= _timestamp(self.end_exclusive_utc):
            raise CompositeSchemaError("shortfall interval is empty or reversed")
        object.__setattr__(
            self,
            "reason_code",
            _normal_identifier(self.reason_code, "shortfall.reason_code", upper=True),
        )
        if self.reason_code not in _SHORTFALL_REASON_CODES:
            raise CompositeSchemaError("unsupported acquisition shortfall reason")
        object.__setattr__(
            self,
            "evidence_sha256",
            _normal_sha256(self.evidence_sha256, "shortfall.evidence_sha256"),
        )

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompositeSegment:
    segment_id: str
    role: str
    origin_contract_kind: str
    origin_run_id: str
    origin_manifest_schema_version: str
    origin_manifest_relative_path: str
    origin_manifest_sha256: str
    bars_relative_path: str
    bars_sha256: str
    bars_bytes: int
    rows: int
    provenance_relative_path: str
    provenance_sha256: str
    provenance_bytes: int
    origin_provenance_schema_version: str
    origin_acquisition_contract_version: str
    origin_pagination_contract_version: str
    interval_start_utc: str
    interval_end_exclusive_utc: str
    first_timestamp_utc: str
    last_timestamp_utc: str
    first_close: float
    last_close: float
    symbol: str
    timeframe: str
    source_identity: BrokerSourceIdentity
    preceding_shortfall: Optional[AcquisitionShortfall] = None
    legacy_adoption: Optional[LegacyAdoptionAttestation] = None
    schema_version: str = SEGMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SEGMENT_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported composite segment schema")
        object.__setattr__(
            self, "segment_id", _normal_identifier(self.segment_id, "segment_id")
        )
        role = _normal_identifier(self.role, "role", upper=True)
        if role not in {"ORIGIN", "DELTA"}:
            raise CompositeSchemaError("segment role must be ORIGIN or DELTA")
        object.__setattr__(self, "role", role)
        contract_kind = _normal_identifier(
            self.origin_contract_kind, "origin_contract_kind", upper=True
        )
        if contract_kind not in {"V5_NATIVE", "LEGACY_ADOPTED"}:
            raise CompositeSchemaError(
                "origin_contract_kind must be V5_NATIVE or LEGACY_ADOPTED"
            )
        object.__setattr__(self, "origin_contract_kind", contract_kind)
        for name in (
            "origin_run_id",
            "origin_manifest_schema_version",
            "origin_provenance_schema_version",
            "origin_acquisition_contract_version",
            "origin_pagination_contract_version",
        ):
            object.__setattr__(
                self, name, _normal_identifier(getattr(self, name), name)
            )
        for name in (
            "origin_manifest_relative_path",
            "bars_relative_path",
            "provenance_relative_path",
        ):
            object.__setattr__(
                self, name, _normal_relative_path(getattr(self, name), name)
            )
        expected_prefix = ("runs", self.origin_run_id)
        for name in (
            "origin_manifest_relative_path",
            "bars_relative_path",
            "provenance_relative_path",
        ):
            parts = PurePosixPath(getattr(self, name)).parts
            if parts[:2] != expected_prefix:
                raise CompositeSchemaError(
                    f"{name} is outside its immutable origin run"
                )
        if self.origin_manifest_relative_path != (
            f"runs/{self.origin_run_id}/run_manifest.json"
        ):
            raise CompositeSchemaError(
                "origin manifest must be the run's canonical run_manifest.json"
            )
        if self.bars_relative_path == self.provenance_relative_path:
            raise CompositeSchemaError("bars and provenance paths must differ")
        if not self.bars_relative_path.endswith(".csv"):
            raise CompositeSchemaError("bars artifact must be a CSV file")
        if not self.provenance_relative_path.endswith(".json"):
            raise CompositeSchemaError("provenance artifact must be JSON")
        for name in (
            "origin_manifest_sha256",
            "bars_sha256",
            "provenance_sha256",
        ):
            object.__setattr__(
                self, name, _normal_sha256(getattr(self, name), name)
            )
        for name in ("bars_bytes", "provenance_bytes", "rows"):
            object.__setattr__(
                self, name, _normal_positive_integer(getattr(self, name), name)
            )
        for name in (
            "interval_start_utc",
            "interval_end_exclusive_utc",
            "first_timestamp_utc",
            "last_timestamp_utc",
        ):
            object.__setattr__(
                self, name, _normal_timestamp(getattr(self, name), name)
            )
        start = _timestamp(self.interval_start_utc)
        end = _timestamp(self.interval_end_exclusive_utc)
        first = _timestamp(self.first_timestamp_utc)
        last = _timestamp(self.last_timestamp_utc)
        if start >= end:
            raise CompositeSchemaError("segment interval is empty or reversed")
        if not (start <= first < end):
            raise CompositeSchemaError(
                "segment first timestamp lies outside its half-open interval"
            )
        if first > last or last > end:
            raise CompositeSchemaError(
                "segment last timestamp is unordered or beyond its interval sentinel"
            )
        object.__setattr__(
            self, "first_close", _finite_positive_float(self.first_close, "first_close")
        )
        object.__setattr__(
            self, "last_close", _finite_positive_float(self.last_close, "last_close")
        )
        object.__setattr__(
            self, "symbol", _normal_identifier(self.symbol, "symbol", upper=True)
        )
        timeframe = _normal_identifier(self.timeframe, "timeframe", upper=True)
        if timeframe not in _TIMEFRAMES:
            raise CompositeSchemaError(f"unsupported timeframe: {timeframe}")
        object.__setattr__(self, "timeframe", timeframe)
        if not isinstance(self.source_identity, BrokerSourceIdentity):
            raise CompositeSchemaError(
                "source_identity must be BrokerSourceIdentity"
            )
        if self.preceding_shortfall is not None and not isinstance(
            self.preceding_shortfall, AcquisitionShortfall
        ):
            raise CompositeSchemaError(
                "preceding_shortfall must be AcquisitionShortfall"
            )
        if contract_kind == "V5_NATIVE":
            if self.legacy_adoption is not None:
                raise CompositeSchemaError(
                    "V5 native segments cannot carry a legacy attestation"
                )
            if (
                self.origin_provenance_schema_version
                != CURRENT_PROVENANCE_SCHEMA_VERSION
                or self.origin_acquisition_contract_version
                != CURRENT_ACQUISITION_CONTRACT_VERSION
                or self.origin_pagination_contract_version
                != CURRENT_PAGINATION_CONTRACT_VERSION
            ):
                raise CompositeSchemaError(
                    "V5 segment does not declare the exact current contracts"
                )
        else:
            if not isinstance(self.legacy_adoption, LegacyAdoptionAttestation):
                raise CompositeSchemaError(
                    "legacy segments require a truthful adoption attestation"
                )
            adoption = self.legacy_adoption
            expected = (
                self.origin_manifest_sha256,
                self.bars_sha256,
                self.provenance_sha256,
                self.bars_bytes,
                self.provenance_bytes,
                self.rows,
                self.origin_provenance_schema_version,
                self.origin_acquisition_contract_version,
                self.origin_pagination_contract_version,
            )
            supplied = (
                adoption.origin_manifest_sha256,
                adoption.bars_sha256,
                adoption.provenance_sha256,
                adoption.bars_bytes,
                adoption.provenance_bytes,
                adoption.rows,
                adoption.original_provenance_schema_version,
                adoption.original_acquisition_contract_version,
                adoption.original_pagination_contract_version,
            )
            if supplied != expected:
                raise CompositeSchemaError(
                    "legacy adoption attestation does not bind the segment"
                )

    def canonical(self) -> dict[str, Any]:
        value = asdict(self)
        value["source_identity"] = self.source_identity.canonical()
        value["preceding_shortfall"] = (
            self.preceding_shortfall.canonical()
            if self.preceding_shortfall is not None
            else None
        )
        value["legacy_adoption"] = (
            self.legacy_adoption.canonical()
            if self.legacy_adoption is not None
            else None
        )
        return value


@dataclass(frozen=True)
class CompositeCell:
    symbol: str
    timeframe: str
    segments: tuple[CompositeSegment, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "symbol", _normal_identifier(self.symbol, "symbol", upper=True)
        )
        timeframe = _normal_identifier(self.timeframe, "timeframe", upper=True)
        if timeframe not in _TIMEFRAMES:
            raise CompositeSchemaError(f"unsupported timeframe: {timeframe}")
        object.__setattr__(self, "timeframe", timeframe)
        segments = tuple(self.segments)
        if not segments:
            raise CompositeSchemaError("composite cell requires at least one segment")
        if len(segments) > _MAX_SCHEMA_SEGMENTS_PER_CELL:
            raise CompositeCapacityError("composite cell exceeds schema segment cap")
        if any(not isinstance(item, CompositeSegment) for item in segments):
            raise CompositeSchemaError("cell contains a non-segment value")
        object.__setattr__(self, "segments", segments)
        if segments[0].role != "ORIGIN" or any(
            item.role != "DELTA" for item in segments[1:]
        ):
            raise CompositeSchemaError(
                "cell requires one leading ORIGIN followed only by DELTA segments"
            )
        if len({item.segment_id for item in segments}) != len(segments):
            raise CompositeSchemaError("cell segment identifiers are not unique")
        source = segments[0].source_identity
        previous: Optional[CompositeSegment] = None
        for segment in segments:
            if segment.symbol != self.symbol or segment.timeframe != self.timeframe:
                raise CompositeSchemaError("segment/cell symbol or timeframe mismatch")
            if segment.source_identity != source:
                raise CompositeSchemaError("cell segments change broker/source identity")
            if previous is None:
                if segment.preceding_shortfall is not None:
                    raise CompositeSchemaError(
                        "origin segment cannot claim a preceding shortfall"
                    )
                previous = segment
                continue
            previous_end = _timestamp(previous.interval_end_exclusive_utc)
            current_start = _timestamp(segment.interval_start_utc)
            if current_start < previous_end:
                raise CompositeSchemaError("segment intervals overlap or are reordered")
            if current_start > previous_end:
                shortfall = segment.preceding_shortfall
                if shortfall is None:
                    raise CompositeSchemaError(
                        "segment interval gap lacks acquisition-shortfall evidence"
                    )
                if (
                    shortfall.start_utc != previous.interval_end_exclusive_utc
                    or shortfall.end_exclusive_utc != segment.interval_start_utc
                    or shortfall.evidence_sha256 != segment.provenance_sha256
                ):
                    raise CompositeSchemaError(
                        "acquisition shortfall does not exactly cover the interval gap"
                    )
            elif segment.preceding_shortfall is not None:
                raise CompositeSchemaError(
                    "contiguous segments cannot declare a nonexistent shortfall"
                )
            previous_last = _timestamp(previous.last_timestamp_utc)
            current_first = _timestamp(segment.first_timestamp_utc)
            if current_first < previous_last:
                raise CompositeSchemaError("physical rows overlap beyond a boundary")
            if current_first == previous_last and not (
                previous.interval_end_exclusive_utc
                == segment.interval_start_utc
                == previous.last_timestamp_utc
                == segment.first_timestamp_utc
            ):
                raise CompositeSchemaError(
                    "duplicate timestamp is not an exact segment-boundary sentinel"
                )
            previous = segment

    @property
    def cell_id(self) -> str:
        return f"{self.symbol}:{self.timeframe}"

    def canonical(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "segments": [item.canonical() for item in self.segments],
        }


@dataclass(frozen=True)
class CompositeRunManifest:
    composite_run_id: str
    created_at_utc: str
    source_identity: BrokerSourceIdentity
    cells: tuple[CompositeCell, ...]
    immutable_record: bool = True
    schema_version: str = COMPOSITE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != COMPOSITE_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported composite run schema")
        if self.immutable_record is not True:
            raise CompositeSchemaError("composite run must be immutable")
        object.__setattr__(
            self,
            "composite_run_id",
            _normal_identifier(self.composite_run_id, "composite_run_id"),
        )
        object.__setattr__(
            self,
            "created_at_utc",
            _normal_timestamp(self.created_at_utc, "created_at_utc"),
        )
        if not isinstance(self.source_identity, BrokerSourceIdentity):
            raise CompositeSchemaError(
                "source_identity must be BrokerSourceIdentity"
            )
        raw_cells = tuple(self.cells)
        if not raw_cells or any(
            not isinstance(item, CompositeCell) for item in raw_cells
        ):
            raise CompositeSchemaError("composite run requires valid cells")
        if len(raw_cells) > _MAX_SCHEMA_CELLS:
            raise CompositeCapacityError("composite run exceeds schema cell cap")
        if sum(len(item.segments) for item in raw_cells) > _MAX_SCHEMA_TOTAL_SEGMENTS:
            raise CompositeCapacityError("composite run exceeds schema segment cap")
        cells = tuple(sorted(raw_cells, key=lambda item: item.cell_id))
        if len({item.cell_id for item in cells}) != len(cells):
            raise CompositeSchemaError("composite run cell identifiers are not unique")
        if any(
            segment.source_identity != self.source_identity
            for cell in cells
            for segment in cell.segments
        ):
            raise CompositeSchemaError(
                "composite run contains a different broker/source identity"
            )
        object.__setattr__(self, "cells", cells)

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "immutable_record": self.immutable_record,
            "composite_run_id": self.composite_run_id,
            "created_at_utc": self.created_at_utc,
            "source_identity": self.source_identity.canonical(),
            "cells": [item.canonical() for item in self.cells],
        }

    def to_bytes(self) -> bytes:
        return _canonical_bytes(self.canonical())

    @property
    def manifest_sha256(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()


@dataclass(frozen=True)
class CompositeLimits:
    max_cells: int = 600
    max_segments_per_cell: int = 32
    max_total_segments: int = 8_000
    max_manifest_bytes: int = 8 * 1024 * 1024
    max_origin_manifest_bytes: int = 8 * 1024 * 1024
    max_provenance_bytes: int = 4 * 1024 * 1024
    max_row_bytes: int = 64 * 1024
    max_bars_bytes_per_segment: int = 2 * 1024 * 1024 * 1024
    max_rows_per_segment: int = 25_000_000
    max_total_declared_bars_bytes: int = 512 * 1024 * 1024 * 1024
    max_total_declared_rows: int = 2_000_000_000

    def __post_init__(self) -> None:
        hard_caps = {
            "max_cells": 600,
            "max_segments_per_cell": 64,
            "max_total_segments": 32_000,
            "max_manifest_bytes": 16 * 1024 * 1024,
            "max_origin_manifest_bytes": 16 * 1024 * 1024,
            "max_provenance_bytes": 16 * 1024 * 1024,
            "max_row_bytes": 1024 * 1024,
            "max_bars_bytes_per_segment": 16 * 1024 * 1024 * 1024,
            "max_rows_per_segment": 100_000_000,
            "max_total_declared_bars_bytes": 4 * 1024 * 1024 * 1024 * 1024,
            "max_total_declared_rows": 10_000_000_000,
        }
        for name, hard_cap in hard_caps.items():
            value = _normal_positive_integer(getattr(self, name), name)
            if value > hard_cap:
                raise CompositeSchemaError(f"{name} exceeds its institutional hard cap")
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class ProtectionArtifact:
    relative_path: str
    sha256: str
    size: int
    artifact_kind: str
    origin_run_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "relative_path",
            _normal_relative_path(self.relative_path, "protection relative_path"),
        )
        object.__setattr__(
            self, "sha256", _normal_sha256(self.sha256, "protection sha256")
        )
        object.__setattr__(
            self, "size", _normal_positive_integer(self.size, "protection size")
        )
        object.__setattr__(
            self,
            "artifact_kind",
            _normal_identifier(self.artifact_kind, "artifact_kind", upper=True),
        )
        if self.artifact_kind not in {
            "ORIGIN_MANIFEST",
            "BARS",
            "PROVENANCE",
        }:
            raise CompositeSchemaError("unsupported protection artifact kind")
        object.__setattr__(
            self,
            "origin_run_id",
            _normal_identifier(self.origin_run_id, "origin_run_id"),
        )
        if PurePosixPath(self.relative_path).parts[:2] != (
            "runs",
            self.origin_run_id,
        ):
            raise CompositeSchemaError(
                "protection artifact is outside its immutable origin run"
            )

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompositeProtectionGraph:
    composite_run_id: str
    manifest_sha256: str
    roots: tuple[ProtectionArtifact, ...]
    schema_version: str = PROTECTION_GRAPH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROTECTION_GRAPH_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported protection graph schema")
        object.__setattr__(
            self,
            "composite_run_id",
            _normal_identifier(self.composite_run_id, "composite_run_id"),
        )
        object.__setattr__(
            self,
            "manifest_sha256",
            _normal_sha256(self.manifest_sha256, "manifest_sha256"),
        )
        roots = tuple(sorted(self.roots, key=lambda item: item.relative_path))
        if not roots or any(not isinstance(item, ProtectionArtifact) for item in roots):
            raise CompositeSchemaError("protection graph has no valid roots")
        if len(roots) > _MAX_PROTECTION_ROOTS:
            raise CompositeCapacityError("protection graph root cap exceeded")
        if len({item.relative_path for item in roots}) != len(roots):
            raise CompositeSchemaError("protection graph repeats an origin artifact")
        object.__setattr__(self, "roots", roots)

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "composite_run_id": self.composite_run_id,
            "manifest_sha256": self.manifest_sha256,
            "roots": [item.canonical() for item in self.roots],
        }

    @property
    def graph_sha256(self) -> str:
        return _canonical_sha256(self.canonical())


@dataclass(frozen=True)
class CellVerificationReceipt:
    cell_id: str
    segments: int
    physical_rows: int
    stitched_rows: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "cell_id", _normal_identifier(self.cell_id, "receipt.cell_id")
        )
        for name in ("segments", "physical_rows", "stitched_rows"):
            object.__setattr__(
                self, name, _normal_positive_integer(getattr(self, name), name)
            )
        if self.stitched_rows > self.physical_rows:
            raise CompositeSchemaError(
                "receipt stitched rows exceed physical rows"
            )

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompositeVerificationReceipt:
    composite_run_id: str
    manifest_sha256: str
    protection_graph_sha256: str
    verified_artifacts_sha256: str
    cells: tuple[CellVerificationReceipt, ...]
    total_segments: int
    total_physical_rows: int
    total_stitched_rows: int
    v5_native_segments: int
    legacy_adopted_segments: int
    limitation_codes: tuple[str, ...]
    schema_version: str = RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECEIPT_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported composite receipt schema")
        object.__setattr__(
            self,
            "composite_run_id",
            _normal_identifier(self.composite_run_id, "composite_run_id"),
        )
        for name in (
            "manifest_sha256",
            "protection_graph_sha256",
            "verified_artifacts_sha256",
        ):
            object.__setattr__(
                self, name, _normal_sha256(getattr(self, name), name)
            )
        cells = tuple(sorted(self.cells, key=lambda item: item.cell_id))
        if not cells or any(
            not isinstance(item, CellVerificationReceipt) for item in cells
        ):
            raise CompositeSchemaError("receipt requires valid cell receipts")
        if len(cells) > _MAX_RECEIPT_CELLS:
            raise CompositeCapacityError("receipt cell cap exceeded")
        if len({item.cell_id for item in cells}) != len(cells):
            raise CompositeSchemaError("receipt repeats a cell identifier")
        object.__setattr__(self, "cells", cells)
        for name in (
            "total_segments",
            "total_physical_rows",
            "total_stitched_rows",
        ):
            object.__setattr__(
                self, name, _normal_positive_integer(getattr(self, name), name)
            )
        for name in ("v5_native_segments", "legacy_adopted_segments"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise CompositeSchemaError(f"{name} must be a nonnegative integer")
        if self.v5_native_segments + self.legacy_adopted_segments != self.total_segments:
            raise CompositeSchemaError("receipt segment classifications do not add up")
        if self.total_physical_rows != sum(item.physical_rows for item in cells):
            raise CompositeSchemaError("receipt physical-row total is inconsistent")
        if self.total_stitched_rows != sum(item.stitched_rows for item in cells):
            raise CompositeSchemaError("receipt stitched-row total is inconsistent")
        if self.total_segments != sum(item.segments for item in cells):
            raise CompositeSchemaError("receipt segment total is inconsistent")
        raw_limitations = tuple(self.limitation_codes)
        normalized_limitations = tuple(
            _normal_identifier(item, "limitation_code", upper=True)
            for item in raw_limitations
        )
        limitations = tuple(sorted(set(normalized_limitations)))
        if normalized_limitations != raw_limitations or len(limitations) != len(
            raw_limitations
        ):
            raise CompositeSchemaError("receipt limitation codes are not canonical")
        if self.legacy_adopted_segments and limitations != (
            LEGACY_MISSING_ATTEMPT_LEDGER,
        ):
            raise CompositeSchemaError("legacy receipt omits its provenance limitation")
        if not self.legacy_adopted_segments and limitations:
            raise CompositeSchemaError("V5-only receipt cannot claim legacy limitations")
        object.__setattr__(self, "limitation_codes", limitations)

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "composite_run_id": self.composite_run_id,
            "manifest_sha256": self.manifest_sha256,
            "protection_graph_sha256": self.protection_graph_sha256,
            "verified_artifacts_sha256": self.verified_artifacts_sha256,
            "cells": [item.canonical() for item in self.cells],
            "total_segments": self.total_segments,
            "total_physical_rows": self.total_physical_rows,
            "total_stitched_rows": self.total_stitched_rows,
            "v5_native_segments": self.v5_native_segments,
            "legacy_adopted_segments": self.legacy_adopted_segments,
            "limitation_codes": list(self.limitation_codes),
        }

    @property
    def receipt_sha256(self) -> str:
        return _canonical_sha256(self.canonical())


@dataclass(frozen=True)
class LifecyclePinPublicationReceipt:
    composite_run_id: str
    manifest_sha256: str
    protection_graph_sha256: str
    verification_receipt_sha256: str
    lifecycle_authority_id: str
    lifecycle_root_key: str
    pin_id: str
    pin_payload_sha256: str
    protected_roots_sha256: str
    schema_version: str = PUBLICATION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PUBLICATION_RECEIPT_SCHEMA_VERSION:
            raise CompositeSchemaError("unsupported publication receipt schema")
        object.__setattr__(
            self,
            "composite_run_id",
            _normal_identifier(self.composite_run_id, "composite_run_id"),
        )
        object.__setattr__(
            self, "pin_id", _normal_identifier(self.pin_id, "pin_id")
        )
        for name in (
            "manifest_sha256",
            "protection_graph_sha256",
            "verification_receipt_sha256",
            "lifecycle_authority_id",
            "lifecycle_root_key",
            "pin_payload_sha256",
            "protected_roots_sha256",
        ):
            object.__setattr__(
                self, name, _normal_sha256(getattr(self, name), name)
            )

    def canonical(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def receipt_sha256(self) -> str:
        return _canonical_sha256(self.canonical())


@dataclass(frozen=True)
class CompositeBarRow:
    timestamp_utc: str
    close_timestamp_utc: str
    close: float
    header: tuple[str, ...]
    values: tuple[str, ...]
    segment_id: str
    origin_run_id: str

    @property
    def mapping(self) -> Mapping[str, str]:
        return MappingProxyType(dict(zip(self.header, self.values)))


@dataclass(frozen=True)
class _RawBarRow:
    timestamp_utc: str
    close_timestamp_utc: str
    close: float
    values: tuple[str, ...]
    raw_content: bytes


@dataclass(frozen=True)
class _BarsScan:
    rows: int
    bytes: int
    sha256: str
    first: _RawBarRow
    last: _RawBarRow


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int
    nlink: int


class NativeCompositeResolver:
    """Verify and stream one immutable composite manifest."""

    def __init__(
        self,
        native_root: Path | str,
        manifest: CompositeRunManifest,
        *,
        limits: CompositeLimits = CompositeLimits(),
    ) -> None:
        if not isinstance(manifest, CompositeRunManifest):
            raise CompositeSchemaError("manifest must be CompositeRunManifest")
        if not isinstance(limits, CompositeLimits):
            raise CompositeSchemaError("limits must be CompositeLimits")
        self.native_root = validate_durable_local_root(
            Path(native_root), label="native_composite_root", must_exist=True
        )
        self.manifest = manifest
        self.limits = limits
        self._path_identities: dict[tuple[int, int], str] = {}
        self._path_hashes: dict[str, str] = {}
        self._path_sizes: dict[str, int] = {}
        self._protection_graph: Optional[CompositeProtectionGraph] = None
        self._receipt: Optional[CompositeVerificationReceipt] = None
        self._pin_authority: Optional[NativeLifecycleAuthority] = None
        self._pin_id: Optional[str] = None
        self._publication_receipt: Optional[LifecyclePinPublicationReceipt] = None
        self._enforce_manifest_caps()

    def _enforce_manifest_caps(self) -> None:
        if len(self.manifest.cells) > self.limits.max_cells:
            raise CompositeCapacityError("composite cell cap exceeded")
        total_segments = 0
        total_rows = 0
        total_bars_bytes = 0
        for cell in self.manifest.cells:
            if len(cell.segments) > self.limits.max_segments_per_cell:
                raise CompositeCapacityError(
                    f"segment cap exceeded for {cell.cell_id}"
                )
            total_segments += len(cell.segments)
            for segment in cell.segments:
                if segment.rows > self.limits.max_rows_per_segment:
                    raise CompositeCapacityError(
                        f"row cap exceeded for segment {segment.segment_id}"
                    )
                if segment.bars_bytes > self.limits.max_bars_bytes_per_segment:
                    raise CompositeCapacityError(
                        f"bars byte cap exceeded for segment {segment.segment_id}"
                    )
                total_rows += segment.rows
                total_bars_bytes += segment.bars_bytes
        if total_segments > self.limits.max_total_segments:
            raise CompositeCapacityError("composite total segment cap exceeded")
        if total_rows > self.limits.max_total_declared_rows:
            raise CompositeCapacityError("composite total declared row cap exceeded")
        if total_bars_bytes > self.limits.max_total_declared_bars_bytes:
            raise CompositeCapacityError(
                "composite total declared bars byte cap exceeded"
            )
        payload = self.manifest.to_bytes()
        if len(payload) > self.limits.max_manifest_bytes:
            raise CompositeCapacityError("composite manifest byte cap exceeded")

    def _fresh_root(self) -> None:
        current = validate_durable_local_root(
            self.native_root, label="native_composite_root", must_exist=True
        )
        if current != self.native_root:
            raise CompositePathError("native root identity changed")

    @staticmethod
    def _windows_attributes(path: Path) -> int:
        if os.name != "nt":
            return 0
        import ctypes

        value = int(ctypes.windll.kernel32.GetFileAttributesW(str(path)))
        if value == 0xFFFFFFFF:
            raise CompositePathError(f"cannot inspect Windows path attributes: {path}")
        return value

    @contextmanager
    def _secure_file(
        self, relative_path: str
    ) -> Iterator[tuple[Path, BinaryIO, _FileIdentity]]:
        self._fresh_root()
        relative = _normal_relative_path(relative_path, "origin artifact path")
        lexical = self.native_root.joinpath(*PurePosixPath(relative).parts)
        cursor = self.native_root
        for part in PurePosixPath(relative).parts:
            cursor = cursor / part
            if not os.path.lexists(cursor):
                raise CompositeIntegrityError(f"origin artifact is missing: {relative}")
            if cursor.is_symlink():
                raise CompositePathError(f"symlink origin path is forbidden: {relative}")
            attributes = self._windows_attributes(cursor)
            if attributes & _WINDOWS_UNSAFE_ATTRIBUTES:
                raise CompositePathError(
                    f"reparse/offline/unpinned origin path is forbidden: {relative}"
                )
        resolved = lexical.resolve(strict=True)
        try:
            resolved.relative_to(self.native_root)
        except ValueError as exc:
            raise CompositePathError(f"origin path escapes native root: {relative}") from exc
        observed = lexical.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode):
            raise CompositePathError(f"origin artifact is not a regular file: {relative}")
        if int(observed.st_nlink) != 1:
            raise CompositePathError(
                f"hardlinked origin artifact is forbidden: {relative}"
            )
        identity = _FileIdentity(
            int(observed.st_dev),
            int(observed.st_ino),
            int(observed.st_size),
            int(observed.st_mtime_ns),
            int(observed.st_nlink),
        )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        try:
            descriptor = os.open(lexical, flags)
        except OSError as exc:
            raise CompositePathError(
                f"cannot no-follow open origin artifact: {relative}"
            ) from exc
        handle = os.fdopen(descriptor, "rb", buffering=0)
        try:
            opened = os.fstat(handle.fileno())
            opened_identity = _FileIdentity(
                int(opened.st_dev),
                int(opened.st_ino),
                int(opened.st_size),
                int(opened.st_mtime_ns),
                int(opened.st_nlink),
            )
            if opened_identity != identity or not stat.S_ISREG(opened.st_mode):
                raise CompositePathError(
                    f"origin file identity changed while opening: {relative}"
                )
            identity_key = (identity.device, identity.inode)
            prior = self._path_identities.get(identity_key)
            if prior is not None and prior != relative:
                raise CompositePathError(
                    f"origin file identity is aliased by {prior} and {relative}"
                )
            self._path_identities[identity_key] = relative
            yield lexical, handle, identity
        finally:
            handle.close()

    @staticmethod
    def _assert_handle_identity(
        handle: BinaryIO, identity: _FileIdentity, relative_path: str
    ) -> None:
        observed = os.fstat(handle.fileno())
        after = _FileIdentity(
            int(observed.st_dev),
            int(observed.st_ino),
            int(observed.st_size),
            int(observed.st_mtime_ns),
            int(observed.st_nlink),
        )
        if after != identity or not stat.S_ISREG(observed.st_mode):
            raise CompositeIntegrityError(
                f"origin artifact changed on its verified handle: {relative_path}"
            )

    def _load_json_artifact(
        self,
        relative_path: str,
        expected_sha256: str,
        *,
        expected_bytes: Optional[int],
        cap: int,
        label: str,
    ) -> tuple[Path, Mapping[str, Any]]:
        expected_sha = _normal_sha256(expected_sha256, "artifact sha256")
        prior_hash = self._path_hashes.get(relative_path)
        if prior_hash is not None and prior_hash != expected_sha:
            raise CompositeIntegrityError(
                f"origin artifact has conflicting hash claims: {relative_path}"
            )
        with self._secure_file(relative_path) as (path, handle, identity):
            if identity.size > cap:
                raise CompositeCapacityError(f"{label} exceeds its byte cap")
            if expected_bytes is not None and identity.size != expected_bytes:
                raise CompositeIntegrityError(
                    f"{label} byte count does not match its binding"
                )
            raw = handle.read(cap + 1)
            if len(raw) > cap:
                raise CompositeCapacityError(f"{label} exceeds its byte cap")
            if len(raw) != identity.size or hashlib.sha256(raw).hexdigest() != expected_sha:
                raise CompositeIntegrityError(f"{label} hash/size binding mismatch")
            self._assert_handle_identity(handle, identity, relative_path)
        self._path_hashes[relative_path] = expected_sha
        self._path_sizes[relative_path] = len(raw)
        return path, _strict_json_mapping(raw, label)

    @staticmethod
    def _bounded_line(handle: BinaryIO, cap: int, label: str) -> bytes:
        line = handle.readline(cap + 1)
        if len(line) > cap:
            raise CompositeCapacityError(f"{label} exceeds row byte cap")
        return line

    @staticmethod
    def _parse_csv_values(raw_line: bytes, label: str) -> tuple[str, ...]:
        try:
            text = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CompositeIntegrityError(f"{label} is not UTF-8") from exc
        try:
            rows = list(csv.reader([text], strict=True))
        except csv.Error as exc:
            raise CompositeIntegrityError(f"{label} is malformed CSV") from exc
        if len(rows) != 1:
            raise CompositeIntegrityError(f"{label} contains a multiline CSV record")
        return tuple(rows[0])

    def _raw_bar_row(
        self, raw_line: bytes, label: str, timeframe: str
    ) -> _RawBarRow:
        values = self._parse_csv_values(raw_line, label)
        if len(values) != len(_CSV_HEADER):
            raise CompositeIntegrityError(f"{label} field count does not match header")
        timestamp = _normal_timestamp(values[0], f"{label}.time")
        close_timestamp = _normal_timestamp(
            values[10], f"{label}._BAR_CLOSE_TS"
        )
        open_ns = _utc_nanoseconds(values[0], f"{label}.time")
        source_first_ns = _utc_nanoseconds(
            values[8], f"{label}._SOURCE_FIRST_TS"
        )
        source_last_ns = _utc_nanoseconds(
            values[9], f"{label}._SOURCE_LAST_TS"
        )
        close_ns = _utc_nanoseconds(values[10], f"{label}._BAR_CLOSE_TS")
        duration_ns = _TIMEFRAME_SECONDS[timeframe] * 1_000_000_000
        if source_first_ns != open_ns or source_last_ns != close_ns - 1:
            raise CompositeIntegrityError(
                f"{label} source interval does not exactly bind the native bar"
            )
        if open_ns % 1_000_000_000 or close_ns % 1_000_000_000:
            raise CompositeIntegrityError(f"{label} bar boundaries are sub-second")
        if timeframe in {"H4", "D1"}:
            observed_duration = close_ns - open_ns
            if not duration_ns // 2 <= observed_duration <= duration_ns * 3 // 2:
                raise CompositeIntegrityError(
                    f"{label} violates broker-observed {timeframe} successor bounds"
                )
        else:
            if close_ns - open_ns != duration_ns:
                raise CompositeIntegrityError(
                    f"{label} duration does not match native timeframe {timeframe}"
                )
            if open_ns % duration_ns:
                raise CompositeIntegrityError(
                    f"{label} open is not aligned to native timeframe {timeframe}"
                )
        prices = [
            _finite_positive_float(values[index], f"{label}.{_CSV_HEADER[index]}")
            for index in range(1, 5)
        ]
        if prices[1] < max(prices[0], prices[2], prices[3]):
            raise CompositeIntegrityError(f"{label} high price is inconsistent")
        if prices[2] > min(prices[0], prices[1], prices[3]):
            raise CompositeIntegrityError(f"{label} low price is inconsistent")
        for index in range(5, 8):
            raw = values[index]
            try:
                count = int(raw)
            except (TypeError, ValueError, OverflowError) as exc:
                raise CompositeIntegrityError(
                    f"{label}.{_CSV_HEADER[index]} is not a nonnegative integer"
                ) from exc
            if count < 0 or str(count) != raw:
                raise CompositeIntegrityError(
                    f"{label}.{_CSV_HEADER[index]} is not canonical"
                )
        return _RawBarRow(
            timestamp_utc=timestamp,
            close_timestamp_utc=close_timestamp,
            close=prices[3],
            values=values,
            raw_content=raw_line,
        )

    @staticmethod
    def _validate_observed_successor(
        previous: _RawBarRow, current: _RawBarRow, timeframe: str
    ) -> None:
        if timeframe not in {"H4", "D1"}:
            return
        nominal_ns = _TIMEFRAME_SECONDS[timeframe] * 1_000_000_000
        previous_open_ns = _utc_nanoseconds(
            previous.timestamp_utc, "previous bar open"
        )
        previous_close_ns = _utc_nanoseconds(
            previous.close_timestamp_utc, "previous bar close"
        )
        current_open_ns = _utc_nanoseconds(current.timestamp_utc, "current bar open")
        successor_delta = current_open_ns - previous_open_ns
        expected_close = (
            current_open_ns
            if nominal_ns // 2 <= successor_delta <= nominal_ns * 3 // 2
            else previous_open_ns + nominal_ns
        )
        if previous_close_ns != expected_close:
            raise CompositeIntegrityError(
                f"{timeframe} close is not bound to its observed broker successor"
            )

    def _scan_bars(self, segment: CompositeSegment) -> tuple[Path, _BarsScan]:
        digest = hashlib.sha256()
        total_bytes = 0
        rows = 0
        first: Optional[_RawBarRow] = None
        last: Optional[_RawBarRow] = None
        with self._secure_file(segment.bars_relative_path) as (
            path,
            handle,
            identity,
        ):
            if identity.size != segment.bars_bytes:
                raise CompositeIntegrityError(
                    f"bars byte count mismatch: {segment.bars_relative_path}"
                )
            if identity.size > self.limits.max_bars_bytes_per_segment:
                raise CompositeCapacityError(
                    f"bars artifact exceeds byte cap: {segment.bars_relative_path}"
                )
            header_line = self._bounded_line(
                handle, self.limits.max_row_bytes, "CSV header"
            )
            if not header_line:
                raise CompositeIntegrityError("bars CSV is empty")
            digest.update(header_line)
            total_bytes += len(header_line)
            header = self._parse_csv_values(header_line, "CSV header")
            if header != _CSV_HEADER:
                raise CompositeIntegrityError("bars CSV header is not canonical")
            while True:
                raw_line = self._bounded_line(
                    handle, self.limits.max_row_bytes, "bars CSV row"
                )
                if not raw_line:
                    break
                digest.update(raw_line)
                total_bytes += len(raw_line)
                row = self._raw_bar_row(
                    raw_line, f"bars row {rows + 1}", segment.timeframe
                )
                if rows + 1 > segment.rows:
                    raise CompositeIntegrityError(
                        f"bars row count exceeds claim: {segment.bars_relative_path}"
                    )
                if total_bytes > segment.bars_bytes:
                    raise CompositeIntegrityError(
                        f"bars byte count exceeds claim: {segment.bars_relative_path}"
                    )
                row_open = _timestamp(row.timestamp_utc)
                row_close = _timestamp(row.close_timestamp_utc)
                interval_start = _timestamp(segment.interval_start_utc)
                interval_end = _timestamp(segment.interval_end_exclusive_utc)
                owned_bar = (
                    interval_start <= row_open < interval_end
                    and row_close <= interval_end
                )
                successor_sentinel = row_open == interval_end
                if not (owned_bar or successor_sentinel):
                    raise CompositeIntegrityError(
                        f"bars row lies outside segment interval: {segment.segment_id}"
                    )
                if last is not None and _timestamp(row.timestamp_utc) <= _timestamp(
                    last.timestamp_utc
                ):
                    raise CompositeIntegrityError(
                        "bars CSV timestamps are duplicated or reordered"
                    )
                if last is not None:
                    self._validate_observed_successor(last, row, segment.timeframe)
                if first is None:
                    first = row
                last = row
                rows += 1
            if first is None or last is None:
                raise CompositeIntegrityError("bars CSV contains no data rows")
            supplied_sha = digest.hexdigest()
            if supplied_sha != segment.bars_sha256:
                raise CompositeIntegrityError(
                    f"bars hash mismatch: {segment.bars_relative_path}"
                )
            if total_bytes != segment.bars_bytes or total_bytes != identity.size:
                raise CompositeIntegrityError(
                    f"bars byte count mismatch: {segment.bars_relative_path}"
                )
            if rows != segment.rows:
                raise CompositeIntegrityError(
                    f"bars row count mismatch: {segment.bars_relative_path}"
                )
            if (
                first.timestamp_utc != segment.first_timestamp_utc
                or last.timestamp_utc != segment.last_timestamp_utc
                or first.close != segment.first_close
                or last.close != segment.last_close
            ):
                raise CompositeIntegrityError(
                    f"bars first/last evidence mismatch: {segment.bars_relative_path}"
                )
            self._assert_handle_identity(
                handle, identity, segment.bars_relative_path
            )
        self._path_hashes[segment.bars_relative_path] = segment.bars_sha256
        self._path_sizes[segment.bars_relative_path] = total_bytes
        return path, _BarsScan(
            rows=rows,
            bytes=total_bytes,
            sha256=supplied_sha,
            first=first,
            last=last,
        )

    def _verify_source_environment(
        self,
        source_environment: Any,
        identity: BrokerSourceIdentity,
        label: str,
    ) -> None:
        if not isinstance(source_environment, Mapping):
            raise CompositeIntegrityError(f"{label} lacks source_environment")
        if _canonical_sha256(source_environment) != identity.source_environment_sha256:
            raise CompositeIntegrityError(f"{label} source environment hash mismatch")
        account = source_environment.get("account")
        if not isinstance(account, Mapping):
            raise CompositeIntegrityError(f"{label} lacks broker account identity")
        supplied = (
            str(account.get("company") or ""),
            str(account.get("server") or ""),
            str(account.get("login") or ""),
        )
        expected = (identity.broker, identity.server, identity.account_id)
        if supplied != expected:
            raise CompositeIntegrityError(f"{label} broker/source identity mismatch")

    def _verify_origin_manifest(
        self, segment: CompositeSegment
    ) -> tuple[
        Mapping[str, Any],
        Mapping[str, Any],
        Optional[Mapping[str, Any]],
    ]:
        _, manifest = self._load_json_artifact(
            segment.origin_manifest_relative_path,
            segment.origin_manifest_sha256,
            expected_bytes=None,
            cap=self.limits.max_origin_manifest_bytes,
            label="origin run manifest",
        )
        if manifest.get("schema_version") != segment.origin_manifest_schema_version:
            raise CompositeIntegrityError("origin run manifest schema mismatch")
        if manifest.get("run_id") != segment.origin_run_id:
            raise CompositeIntegrityError("origin run manifest run_id mismatch")
        if manifest.get("complete") is not True:
            raise CompositeIntegrityError("origin run manifest is incomplete")
        declared_immutable = manifest.get("immutable_record") is True
        if not declared_immutable:
            raise CompositeIntegrityError("origin run manifest is not immutable")
        if segment.legacy_adoption is not None and (
            segment.legacy_adoption.origin_manifest_declared_immutable
            != declared_immutable
        ):
            raise CompositeIntegrityError(
                "legacy attestation misstates manifest immutability"
            )
        self._verify_source_environment(
            manifest.get("source_environment"),
            segment.source_identity,
            "origin run manifest",
        )
        results = manifest.get("results")
        if not isinstance(results, list):
            raise CompositeIntegrityError("origin run manifest lacks result cells")
        identities: set[tuple[str, str]] = set()
        observed_counts = {"success": 0, "partial": 0, "failed": 0}
        for item in results:
            if not isinstance(item, Mapping):
                raise CompositeIntegrityError(
                    "origin run manifest contains a malformed result cell"
                )
            identity = (
                str(item.get("symbol") or "").upper(),
                str(item.get("timeframe") or "").upper(),
            )
            if (
                not identity[0]
                or identity[1] not in _TIMEFRAMES
                or identity in identities
            ):
                raise CompositeIntegrityError(
                    "origin run manifest repeats or misidentifies a result cell"
                )
            identities.add(identity)
            status = str(item.get("status") or "").lower()
            if status not in observed_counts:
                raise CompositeIntegrityError(
                    "origin run manifest contains an unsupported result status"
                )
            observed_counts[status] += 1
        counts = manifest.get("counts")
        if not isinstance(counts, Mapping):
            raise CompositeIntegrityError("origin run manifest lacks terminal counts")
        declared_counts = {
            key: _artifact_nonnegative_integer(counts.get(key), f"counts.{key}")
            for key in observed_counts
        }
        declared_total = _artifact_nonnegative_integer(
            counts.get("total"), "counts.total"
        )
        if (
            declared_counts != observed_counts
            or declared_total != len(results)
            or sum(observed_counts.values()) != len(results)
        ):
            raise CompositeIntegrityError("origin run manifest counts are inconsistent")
        v5_common: Optional[Mapping[str, Any]] = None
        if segment.origin_contract_kind == "V5_NATIVE":
            manifest_request = manifest.get("request")
            v5_common = _validate_v5_common_request(
                manifest_request, label="origin run manifest request"
            )
            assert isinstance(manifest_request, Mapping)
            symbols = manifest_request.get("symbols")
            timeframes = manifest_request.get("timeframes")
            if (
                not isinstance(symbols, list)
                or not symbols
                or not isinstance(timeframes, list)
                or not timeframes
                or len(set(symbols)) != len(symbols)
                or len(set(timeframes)) != len(timeframes)
                or any(
                    not isinstance(symbol, str)
                    or _normal_identifier(symbol, "request symbol", upper=True)
                    != symbol
                    for symbol in symbols
                )
                or any(timeframe not in _TIMEFRAMES for timeframe in timeframes)
            ):
                raise CompositeIntegrityError(
                    "origin run manifest has malformed requested cells"
                )
            requested_cells = {
                (symbol, timeframe)
                for symbol in symbols
                for timeframe in timeframes
            }
            if identities != requested_cells:
                raise CompositeIntegrityError(
                    "origin run results do not terminally cover every requested cell"
                )
            if any(
                timeframe not in v5_common["timeframe_calendar_boundaries"]
                for timeframe in timeframes
            ):
                raise CompositeIntegrityError(
                    "origin run request-map omits a requested timeframe"
                )
            retries = _artifact_positive_integer(
                manifest_request.get("retries"), "manifest request.retries"
            )
            if retries > 64:
                raise CompositeCapacityError("manifest retry cap exceeded")
            resume = manifest.get("resume")
            if not isinstance(resume, Mapping):
                raise CompositeIntegrityError("origin run lacks resume disposition totals")
            disposition_counts = {
                "newly_acquired": sum(
                    item.get("resume_disposition") == "newly_acquired"
                    for item in results
                ),
                "reused": sum(
                    item.get("resume_disposition") == "reused" for item in results
                ),
            }
            if any(
                item.get("resume_disposition") not in {"newly_acquired", "reused"}
                for item in results
            ) or (
                _artifact_nonnegative_integer(
                    resume.get("newly_acquired_cells"),
                    "resume.newly_acquired_cells",
                )
                != disposition_counts["newly_acquired"]
                or _artifact_nonnegative_integer(
                    resume.get("reused_cells"), "resume.reused_cells"
                )
                != disposition_counts["reused"]
            ):
                raise CompositeIntegrityError(
                    "origin run resume dispositions are inconsistent"
                )
        matches = [
            item
            for item in results
            if isinstance(item, Mapping)
            and str(item.get("symbol") or "").upper() == segment.symbol
            and str(item.get("timeframe") or "").upper() == segment.timeframe
        ]
        if len(matches) != 1:
            raise CompositeIntegrityError(
                "origin run manifest does not contain exactly one matching cell"
            )
        result = matches[0]
        status = str(result.get("status") or "").lower()
        expected_leaf = PurePosixPath(segment.bars_relative_path).parent
        if (
            status not in {"success", "partial"}
            or _artifact_positive_integer(result.get("rows"), "result.rows")
            != segment.rows
            or result.get("sha256") != segment.bars_sha256
            or _normal_relative_path(
                result.get("relative_leaf"), "result.relative_leaf"
            )
            != expected_leaf.relative_to(
                PurePosixPath("runs") / segment.origin_run_id
            ).as_posix()
        ):
            raise CompositeIntegrityError("origin run result/cell binding mismatch")
        return manifest, result, v5_common

    def _verify_v5_pagination(
        self,
        *,
        provenance: Mapping[str, Any],
        pagination: Mapping[str, Any],
        timeframe: str,
        original_start: str,
        original_end: str,
        expected_retries: int,
        status: str,
    ) -> Mapping[str, Any]:
        page_attempts = pagination.get("page_attempts")
        if not isinstance(page_attempts, list) or not page_attempts:
            raise CompositeIntegrityError(
                "V5 provenance lacks its original pagination attempt ledger"
            )
        if len(page_attempts) > 100_000:
            raise CompositeCapacityError("pagination attempt ledger cap exceeded")
        source_environment = provenance.get("source_environment")
        terminal = (
            source_environment.get("terminal")
            if isinstance(source_environment, Mapping)
            else None
        )
        max_bars = 0
        if isinstance(terminal, Mapping):
            max_bars = _artifact_nonnegative_integer(
                terminal.get("max_bars", 0), "source terminal.max_bars"
            )
        seconds = _TIMEFRAME_SECONDS[timeframe]
        default_slots = max(1, _DEFAULT_PAGE_DAYS[timeframe] * 86_400 // seconds)
        if max_bars > 0:
            default_slots = min(default_slots, max(1, int(max_bars * 0.90)))
        expected_page_span = default_slots * seconds
        if (
            _artifact_positive_integer(
                pagination.get("page_span_seconds"), "pagination.page_span_seconds"
            )
            != expected_page_span
            or _artifact_positive_integer(
                pagination.get("retries_per_page"), "pagination.retries_per_page"
            )
            != expected_retries
        ):
            raise CompositeIntegrityError(
                "V5 pagination span/retry contract does not match acquisition"
            )

        previous_end: Optional[str] = None
        attempts_total = 0
        copy_attempts_total = 0
        page_records: dict[tuple[str, str], tuple[int, int]] = {}
        for index, item in enumerate(page_attempts):
            if not isinstance(item, Mapping):
                raise CompositeIntegrityError(
                    "V5 pagination ledger contains a malformed page"
                )
            page_start, page_end = _request_interval(
                item, f"page_attempts[{index}]"
            )
            if previous_end is not None and page_start != previous_end:
                raise CompositeIntegrityError(
                    "V5 pagination attempt intervals are not contiguous"
                )
            page_duration = int(
                (_timestamp(page_end) - _timestamp(page_start)).total_seconds()
            )
            if page_duration > expected_page_span or (
                index < len(page_attempts) - 1
                and page_duration != expected_page_span
            ):
                raise CompositeIntegrityError(
                    "V5 pagination pages do not exactly partition the source request"
                )
            attempts = _artifact_positive_integer(
                item.get("attempts"), f"page_attempts[{index}].attempts"
            )
            copy_attempts = _artifact_positive_integer(
                item.get("copy_rates_range_attempts"),
                f"page_attempts[{index}].copy_rates_range_attempts",
            )
            if attempts > expected_retries or copy_attempts < attempts:
                raise CompositeIntegrityError(
                    "V5 page outer/copy retry counts are impossible"
                )
            if copy_attempts > 1_000_000:
                raise CompositeCapacityError("page copy-attempt cap exceeded")
            identity = (page_start, page_end)
            if identity in page_records:
                raise CompositeIntegrityError("V5 pagination repeats a page interval")
            page_records[identity] = (attempts, copy_attempts)
            attempts_total += attempts
            copy_attempts_total += copy_attempts
            previous_end = page_end
        if (
            next(iter(page_records))[0] != original_start
            or previous_end != original_end
            or _artifact_positive_integer(
                pagination.get("pages_requested"), "pagination.pages_requested"
            )
            != len(page_attempts)
            or _artifact_positive_integer(
                pagination.get("attempts_total"), "pagination.attempts_total"
            )
            != attempts_total
            or _artifact_positive_integer(
                pagination.get("copy_rates_range_attempts_total"),
                "pagination.copy_rates_range_attempts_total",
            )
            != copy_attempts_total
        ):
            raise CompositeIntegrityError(
                "V5 pagination totals or source bounds are inconsistent"
            )

        diagnostic_intervals: dict[str, set[tuple[str, str]]] = {}
        missing_intervals: list[tuple[str, str]] = []
        empty_missing_intervals: list[tuple[str, str]] = []
        truncated_missing_intervals: list[tuple[str, str]] = []
        for diagnostic_name in (
            "empty_pages",
            "failed_pages",
            "truncated_pages",
        ):
            diagnostics = pagination.get(diagnostic_name)
            if not isinstance(diagnostics, list) or len(diagnostics) > len(
                page_attempts
            ):
                raise CompositeIntegrityError(
                    f"V5 pagination {diagnostic_name} is malformed"
                )
            seen: set[tuple[str, str]] = set()
            for diagnostic in diagnostics:
                if not isinstance(diagnostic, Mapping):
                    raise CompositeIntegrityError(
                        f"V5 pagination {diagnostic_name} is malformed"
                    )
                interval = _request_interval(diagnostic, diagnostic_name)
                if interval not in page_records or interval in seen:
                    raise CompositeIntegrityError(
                        f"V5 pagination {diagnostic_name} repeats or leaves the ledger"
                    )
                seen.add(interval)
                attempts, copy_attempts = page_records[interval]
                if (
                    _artifact_positive_integer(
                        diagnostic.get("attempts"), f"{diagnostic_name}.attempts"
                    )
                    != attempts
                    or _artifact_positive_integer(
                        diagnostic.get("copy_rates_range_attempts"),
                        f"{diagnostic_name}.copy_rates_range_attempts",
                    )
                    != copy_attempts
                ):
                    raise CompositeIntegrityError(
                        f"V5 pagination {diagnostic_name} retry identity mismatch"
                    )
                if diagnostic_name == "empty_pages":
                    missing_intervals.append(interval)
                    empty_missing_intervals.append(interval)
                elif diagnostic_name == "truncated_pages":
                    page_start, page_end = interval
                    actual_first = _normal_timestamp(
                        diagnostic.get("actual_first_open_utc"),
                        "truncated actual_first_open_utc",
                    )
                    actual_last = _normal_timestamp(
                        diagnostic.get("actual_last_open_utc"),
                        "truncated actual_last_open_utc",
                    )
                    if not (
                        _timestamp(page_start)
                        <= _timestamp(actual_first)
                        <= _timestamp(actual_last)
                        < _timestamp(page_end)
                    ):
                        raise CompositeIntegrityError(
                            "truncated-page observations leave their page"
                        )
                    if actual_first != page_start:
                        missing_intervals.append((page_start, actual_first))
                        truncated_missing_intervals.append(
                            (page_start, actual_first)
                        )
                    trailing_start = (
                        _timestamp(actual_last) + timedelta(seconds=seconds)
                    ).isoformat(timespec="microseconds").replace("+00:00", "Z")
                    if _timestamp(trailing_start) < _timestamp(page_end):
                        missing_intervals.append((trailing_start, page_end))
                        truncated_missing_intervals.append(
                            (trailing_start, page_end)
                        )
            diagnostic_intervals[diagnostic_name] = seen
        empty = diagnostic_intervals["empty_pages"]
        failed = diagnostic_intervals["failed_pages"]
        truncated = diagnostic_intervals["truncated_pages"]
        if (
            empty & failed
            or truncated & (empty | failed)
            or failed
        ):
            raise CompositeIntegrityError(
                "published V5 artifact has conflicting or failed page dispositions"
            )
        pages_with_data = _artifact_positive_integer(
            pagination.get("pages_with_data"), "pagination.pages_with_data"
        )
        if pages_with_data != len(page_attempts) - len(empty) - len(failed):
            raise CompositeIntegrityError("V5 page disposition totals are inconsistent")
        if status == "partial" and not (empty or truncated):
            raise CompositeIntegrityError(
                "partial V5 artifact lacks page-level shortfall diagnostics"
            )
        return {
            "page_intervals": tuple(page_records),
            "missing_intervals": tuple(missing_intervals),
            "empty_missing_intervals": tuple(empty_missing_intervals),
            "truncated_missing_intervals": tuple(
                truncated_missing_intervals
            ),
        }

    @staticmethod
    def _interval_is_covered(
        start: str, end: str, intervals: tuple[tuple[str, str], ...]
    ) -> bool:
        cursor = _timestamp(start)
        target = _timestamp(end)
        for interval_start, interval_end in sorted(
            intervals, key=lambda item: _timestamp(item[0])
        ):
            observed_start = _timestamp(interval_start)
            observed_end = _timestamp(interval_end)
            if observed_end <= cursor:
                continue
            if observed_start > cursor:
                return False
            cursor = max(cursor, observed_end)
            if cursor >= target:
                return True
        return cursor >= target

    def _verify_provenance(
        self,
        segment: CompositeSegment,
        origin_manifest: Mapping[str, Any],
        origin_result: Mapping[str, Any],
        manifest_v5_common: Optional[Mapping[str, Any]],
        bars_scan: _BarsScan,
    ) -> Mapping[str, Any]:
        _, provenance = self._load_json_artifact(
            segment.provenance_relative_path,
            segment.provenance_sha256,
            expected_bytes=segment.provenance_bytes,
            cap=self.limits.max_provenance_bytes,
            label="origin provenance",
        )
        exact_fields = (
            provenance.get("run_id") == segment.origin_run_id,
            str(provenance.get("symbol") or "").upper() == segment.symbol,
            str(provenance.get("timeframe") or "").upper() == segment.timeframe,
            provenance.get("schema_version")
            == segment.origin_provenance_schema_version,
            provenance.get("acquisition_contract_version")
            == segment.origin_acquisition_contract_version,
            str(provenance.get("source_kind") or "")
            == segment.source_identity.source_kind,
        )
        if not all(exact_fields):
            raise CompositeIntegrityError("origin provenance binding mismatch")
        status = str(provenance.get("status") or "").lower()
        origin_status = str(origin_result.get("status") or "").lower()
        if status not in {"success", "partial"} or status != origin_status:
            raise CompositeIntegrityError("origin provenance status mismatch")
        if provenance.get("complete") is not (status == "success"):
            raise CompositeIntegrityError(
                "origin provenance completion flag contradicts its status"
            )
        if provenance.get("immutable_record") is not True:
            raise CompositeIntegrityError("origin provenance is not immutable")
        if (
            _artifact_positive_integer(provenance.get("rows"), "provenance.rows")
            != segment.rows
            or provenance.get("sha256") != segment.bars_sha256
        ):
            raise CompositeIntegrityError("origin provenance row/hash mismatch")
        if (
            provenance.get("columns") != list(_CSV_PROVENANCE_COLUMNS)
            or provenance.get("bar_close_column") != "_BAR_CLOSE_TS"
            or provenance.get("bar_interval_semantics")
            != "half_open_[bar_open,bar_close)"
            or provenance.get("timestamp_semantics") != "bar_open_utc"
            or provenance.get("timezone") != "UTC"
            or _artifact_positive_integer(
                provenance.get("timeframe_seconds"), "provenance.timeframe_seconds"
            )
            != _TIMEFRAME_SECONDS[segment.timeframe]
        ):
            raise CompositeIntegrityError(
                "origin provenance does not describe the exact native bar schema"
            )
        if provenance.get("failure_status") != origin_result.get("failure_status"):
            raise CompositeIntegrityError(
                "origin provenance failure status does not match the run manifest"
            )
        self._verify_source_environment(
            provenance.get("source_environment"),
            segment.source_identity,
            "origin provenance",
        )
        artifact = provenance.get("artifact")
        if not isinstance(artifact, Mapping):
            raise CompositeIntegrityError("origin provenance lacks artifact binding")
        relative = _normal_relative_path(
            artifact.get("relative_path"), "provenance artifact.relative_path"
        )
        provenance_parent = PurePosixPath(segment.provenance_relative_path).parent
        bound_bars = (provenance_parent / relative).as_posix()
        if (
            bound_bars != segment.bars_relative_path
            or artifact.get("sha256") != segment.bars_sha256
            or _artifact_positive_integer(artifact.get("bytes"), "artifact.bytes")
            != segment.bars_bytes
            or _artifact_positive_integer(artifact.get("rows"), "artifact.rows")
            != segment.rows
        ):
            raise CompositeIntegrityError("origin provenance artifact binding mismatch")
        if (
            _normal_timestamp(provenance.get("start_ts"), "provenance.start_ts")
            != segment.first_timestamp_utc
            or _normal_timestamp(provenance.get("end_ts"), "provenance.end_ts")
            != segment.last_timestamp_utc
        ):
            raise CompositeIntegrityError("origin provenance first/last mismatch")
        request = provenance.get("request")
        if not isinstance(request, Mapping):
            raise CompositeIntegrityError("origin provenance lacks request binding")
        if segment.origin_contract_kind == "V5_NATIVE":
            if manifest_v5_common is None:
                raise CompositeIntegrityError(
                    "V5 leaf lacks an authenticated run-manifest request"
                )
            _validate_v5_common_request(request, label="origin provenance request")
            _validate_v5_leaf_projection(
                request,
                manifest_v5_common,
                segment.timeframe,
                label="origin provenance request",
            )
            expected_alignment = (
                BROKER_SESSION_ALIGNMENT_MODE
                if segment.timeframe in {"H4", "D1"}
                else FIXED_ALIGNMENT_MODE
            )
            expected_slack = 1 if segment.timeframe in {"H4", "D1"} else 0
            if (
                request.get("alignment_mode") != expected_alignment
                or _artifact_nonnegative_integer(
                    request.get("successor_sentinel_fetch_slack_seconds"),
                    "request.successor_sentinel_fetch_slack_seconds",
                )
                != expected_slack
                or _normal_timestamp(
                    request.get("last_fully_closed_bar_open_utc"),
                    "request.last_fully_closed_bar_open_utc",
                )
                != segment.last_timestamp_utc
            ):
                raise CompositeIntegrityError(
                    "V5 leaf native alignment/closure projection mismatch"
                )
            source_interval = request.get("source_request_interval")
            if not isinstance(source_interval, Mapping):
                raise CompositeIntegrityError(
                    "V5 provenance lacks its source request interval"
                )
            original_start = _normal_timestamp(
                source_interval.get("start_utc"), "source_request_interval.start_utc"
            )
            original_end = _normal_timestamp(
                source_interval.get("end_exclusive_utc"),
                "source_request_interval.end_exclusive_utc",
            )
        else:
            original_start = _normal_timestamp(
                request.get("native_fetch_start_utc"),
                "legacy request.native_fetch_start_utc",
            )
            original_end = _normal_timestamp(
                request.get("native_fetch_end_exclusive_utc"),
                "legacy request.native_fetch_end_exclusive_utc",
            )
        segment_start = _timestamp(segment.interval_start_utc)
        segment_end = _timestamp(segment.interval_end_exclusive_utc)
        if not (
            _timestamp(original_start) <= segment_start < segment_end
            <= _timestamp(original_end)
        ):
            raise CompositeIntegrityError(
                "segment interval is outside its hashed source request"
            )
        if segment.interval_start_utc != original_start and (
            status != "partial"
            or segment.interval_start_utc != segment.first_timestamp_utc
        ):
            raise CompositeIntegrityError(
                "only a partial artifact may trim missing leading history"
            )
        derived_last_close = bars_scan.last.close_timestamp_utc
        if segment.interval_end_exclusive_utc != original_end and (
            status != "partial"
            or segment.interval_end_exclusive_utc != derived_last_close
        ):
            raise CompositeIntegrityError(
                "only a partial artifact may trim missing trailing history"
            )
        pagination = provenance.get("pagination")
        if not isinstance(pagination, Mapping):
            raise CompositeIntegrityError("origin provenance lacks pagination evidence")
        page_attempts = pagination.get("page_attempts")
        attempt_ledger_present = isinstance(page_attempts, list)
        pagination_contract = str(pagination.get("contract_version") or "")
        if (
            segment.origin_contract_kind == "LEGACY_ADOPTED"
            and not pagination_contract
        ):
            pagination_contract = LEGACY_ABSENT_PAGINATION_CONTRACT
        if pagination_contract != segment.origin_pagination_contract_version:
            raise CompositeIntegrityError("origin pagination contract mismatch")
        pagination_evidence: Optional[Mapping[str, Any]] = None
        if segment.origin_contract_kind == "V5_NATIVE":
            manifest_request = origin_manifest.get("request")
            if not isinstance(manifest_request, Mapping):
                raise CompositeIntegrityError("V5 run manifest request is unavailable")
            pagination_evidence = self._verify_v5_pagination(
                provenance=provenance,
                pagination=pagination,
                timeframe=segment.timeframe,
                original_start=original_start,
                original_end=original_end,
                expected_retries=_artifact_positive_integer(
                    manifest_request.get("retries"), "manifest request.retries"
                ),
                status=status,
            )
            if (
                provenance.get("schema_version")
                != CURRENT_PROVENANCE_SCHEMA_VERSION
                or provenance.get("acquisition_contract_version")
                != CURRENT_ACQUISITION_CONTRACT_VERSION
                or pagination_contract != CURRENT_PAGINATION_CONTRACT_VERSION
            ):
                raise CompositeIntegrityError(
                    "origin provenance is not the exact current V5 contract"
                )
        else:
            if attempt_ledger_present:
                raise CompositeIntegrityError(
                    "legacy missing-ledger attestation is factually false"
                )
            adoption = segment.legacy_adoption
            if adoption is None or adoption.original_attempt_ledger_present:
                raise CompositeIntegrityError("legacy attestation is missing or false")
        if segment.preceding_shortfall is not None:
            shortfall = segment.preceding_shortfall
            if (
                segment.origin_contract_kind != "V5_NATIVE"
                or status != "partial"
                or provenance.get("failure_status") != "partial_history"
                or pagination_evidence is None
                or not (
                    _timestamp(original_start)
                    <= _timestamp(shortfall.start_utc)
                    < _timestamp(shortfall.end_exclusive_utc)
                    <= _timestamp(original_end)
                )
                or not self._interval_is_covered(
                    shortfall.start_utc,
                    shortfall.end_exclusive_utc,
                    pagination_evidence["page_intervals"],
                )
            ):
                raise CompositeIntegrityError(
                    "shortfall is not a requested, partial V5 acquisition interval"
                )
            if shortfall.reason_code == "BROKER_PAGE_EMPTY":
                missing_evidence = pagination_evidence[
                    "empty_missing_intervals"
                ]
            elif shortfall.reason_code == "BROKER_BOUNDARY_TRUNCATED":
                missing_evidence = pagination_evidence[
                    "truncated_missing_intervals"
                ]
            else:
                missing_evidence = pagination_evidence["missing_intervals"]
            if not self._interval_is_covered(
                shortfall.start_utc,
                shortfall.end_exclusive_utc,
                missing_evidence,
            ):
                raise CompositeIntegrityError(
                    "shortfall lacks compatible page-level diagnostic evidence"
                )
        return provenance

    def _verify_segment(self, segment: CompositeSegment) -> _BarsScan:
        # Manifest identity is verified before either referenced leaf is used.
        manifest, result, v5_common = self._verify_origin_manifest(segment)
        _, scan = self._scan_bars(segment)
        self._verify_provenance(
            segment,
            manifest,
            result,
            v5_common,
            scan,
        )
        return scan

    def _protection_graph_from_manifest(self) -> CompositeProtectionGraph:
        artifacts: dict[str, ProtectionArtifact] = {}
        for cell in self.manifest.cells:
            for segment in cell.segments:
                candidates = (
                    (
                        segment.origin_manifest_relative_path,
                        segment.origin_manifest_sha256,
                        "ORIGIN_MANIFEST",
                    ),
                    (segment.bars_relative_path, segment.bars_sha256, "BARS"),
                    (
                        segment.provenance_relative_path,
                        segment.provenance_sha256,
                        "PROVENANCE",
                    ),
                )
                for relative_path, digest, kind in candidates:
                    candidate = ProtectionArtifact(
                        relative_path,
                        digest,
                        self._path_sizes[relative_path],
                        kind,
                        segment.origin_run_id,
                    )
                    prior = artifacts.get(relative_path)
                    if prior is not None and prior != candidate:
                        raise CompositeIntegrityError(
                            "one origin artifact has conflicting protection bindings"
                        )
                    artifacts[relative_path] = candidate
        return CompositeProtectionGraph(
            self.manifest.composite_run_id,
            self.manifest.manifest_sha256,
            tuple(artifacts.values()),
        )

    def verify(self) -> CompositeVerificationReceipt:
        self._fresh_root()
        self._enforce_manifest_caps()
        self._protection_graph = None
        self._receipt = None
        self._publication_receipt = None
        self._path_identities = {}
        self._path_hashes = {}
        self._path_sizes = {}
        cell_receipts: list[CellVerificationReceipt] = []
        total_segments = 0
        total_physical_rows = 0
        total_stitched_rows = 0
        v5_native_segments = 0
        legacy_adopted_segments = 0
        for cell in self.manifest.cells:
            scans: list[_BarsScan] = []
            for segment in cell.segments:
                scans.append(self._verify_segment(segment))
            duplicates = 0
            for index, (segment, scan) in enumerate(zip(cell.segments, scans)):
                has_successor_sentinel = (
                    scan.last.timestamp_utc == segment.interval_end_exclusive_utc
                )
                if has_successor_sentinel:
                    if index + 1 >= len(cell.segments):
                        raise CompositeIntegrityError(
                            "terminal segment cannot expose a successor sentinel"
                        )
                    current = cell.segments[index + 1]
                    current_scan = scans[index + 1]
                    if not (
                        segment.interval_end_exclusive_utc
                        == current.interval_start_utc
                        == current_scan.first.timestamp_utc
                        and scan.last.values == current_scan.first.values
                        and scan.last.raw_content == current_scan.first.raw_content
                    ):
                        raise CompositeIntegrityError(
                            "boundary sentinel lacks an immediate byte-identical successor"
                        )
                    duplicates += 1
                if index + 1 < len(cell.segments):
                    self._validate_observed_successor(
                        scan.last,
                        scans[index + 1].first,
                        cell.timeframe,
                    )
            physical_rows = sum(item.rows for item in scans)
            stitched_rows = physical_rows - duplicates
            cell_receipts.append(
                CellVerificationReceipt(
                    cell.cell_id,
                    len(cell.segments),
                    physical_rows,
                    stitched_rows,
                )
            )
            total_segments += len(cell.segments)
            v5_native_segments += sum(
                item.origin_contract_kind == "V5_NATIVE" for item in cell.segments
            )
            legacy_adopted_segments += sum(
                item.origin_contract_kind == "LEGACY_ADOPTED"
                for item in cell.segments
            )
            total_physical_rows += physical_rows
            total_stitched_rows += stitched_rows
        graph = self._protection_graph_from_manifest()
        expected_paths = {
            item.relative_path for item in graph.roots
        }
        if expected_paths != set(self._path_hashes) or expected_paths != set(
            self._path_sizes
        ):
            missing = sorted(expected_paths - set(self._path_hashes))
            extra = sorted(set(self._path_hashes) - expected_paths)
            raise CompositeIntegrityError(
                f"protection graph/artifact verification mismatch: missing={missing}, extra={extra}"
            )
        verified_artifacts = [
            {
                "relative_path": path,
                "sha256": self._path_hashes[path],
                "size": self._path_sizes[path],
            }
            for path in sorted(self._path_hashes)
        ]
        receipt = CompositeVerificationReceipt(
            composite_run_id=self.manifest.composite_run_id,
            manifest_sha256=self.manifest.manifest_sha256,
            protection_graph_sha256=graph.graph_sha256,
            verified_artifacts_sha256=_canonical_sha256(verified_artifacts),
            cells=tuple(cell_receipts),
            total_segments=total_segments,
            total_physical_rows=total_physical_rows,
            total_stitched_rows=total_stitched_rows,
            v5_native_segments=v5_native_segments,
            legacy_adopted_segments=legacy_adopted_segments,
            limitation_codes=(
                (LEGACY_MISSING_ATTEMPT_LEDGER,)
                if legacy_adopted_segments
                else ()
            ),
        )
        self._protection_graph = graph
        self._receipt = receipt
        return receipt

    @property
    def protection_graph(self) -> CompositeProtectionGraph:
        if self._protection_graph is None:
            self.verify()
        assert self._protection_graph is not None
        return self._protection_graph

    @property
    def receipt(self) -> CompositeVerificationReceipt:
        """Return an audit receipt; this does not authorize row publication."""
        if self._receipt is None:
            self.verify()
        assert self._receipt is not None
        return self._receipt

    def _verify_publication_pin(
        self,
        authority: NativeLifecycleAuthority,
        pin_id: str,
    ) -> LifecyclePinPublicationReceipt:
        if not isinstance(authority, NativeLifecycleAuthority):
            raise CompositeIntegrityError(
                "publication requires a NativeLifecycleAuthority verifier"
            )
        if authority.native_root.resolve(strict=True) != self.native_root:
            raise CompositeIntegrityError(
                "lifecycle pin authority protects a different native root"
            )
        if self._protection_graph is None or self._receipt is None:
            raise CompositeIntegrityError(
                "composite must be verified before lifecycle pin authorization"
            )
        normalized_pin_id = _normal_identifier(pin_id, "pin_id")
        try:
            payload = authority.verify_pin(normalized_pin_id)
        except Exception as exc:
            raise CompositeIntegrityError(
                "lifecycle pin is absent, retired, or unauthenticated"
            ) from exc
        if (
            not isinstance(payload, Mapping)
            or payload.get("schema_version") != PIN_SCHEMA
            or payload.get("pin_id") != normalized_pin_id
            or payload.get("root_key") != authority.root_key
        ):
            raise CompositeIntegrityError("lifecycle pin payload binding mismatch")
        roots = payload.get("roots")
        if not isinstance(roots, list):
            raise CompositeIntegrityError("lifecycle pin has no bounded root set")
        supplied_roots: list[dict[str, Any]] = []
        for item in roots:
            if not isinstance(item, Mapping):
                raise CompositeIntegrityError("lifecycle pin root is malformed")
            supplied_roots.append(
                {
                    "relative_path": _normal_relative_path(
                        item.get("relative_path"), "lifecycle pin relative_path"
                    ),
                    "sha256": _normal_sha256(
                        item.get("sha256"), "lifecycle pin sha256"
                    ),
                    "size": _normal_positive_integer(
                        item.get("size"), "lifecycle pin size"
                    ),
                }
            )
        supplied_roots.sort(key=lambda item: item["relative_path"])
        expected_roots = sorted(
            (
                {
                    "relative_path": item.relative_path,
                    "sha256": item.sha256,
                    "size": item.size,
                }
                for item in self._protection_graph.roots
            ),
            key=lambda item: item["relative_path"],
        )
        if supplied_roots != expected_roots:
            raise CompositeIntegrityError(
                "lifecycle pin does not protect every and only composite origin"
            )
        return LifecyclePinPublicationReceipt(
            composite_run_id=self.manifest.composite_run_id,
            manifest_sha256=self.manifest.manifest_sha256,
            protection_graph_sha256=self._protection_graph.graph_sha256,
            verification_receipt_sha256=self._receipt.receipt_sha256,
            lifecycle_authority_id=authority.authority_id,
            lifecycle_root_key=authority.root_key,
            pin_id=normalized_pin_id,
            pin_payload_sha256=_canonical_sha256(payload),
            protected_roots_sha256=_canonical_sha256(expected_roots),
        )

    def authorize_publication(
        self,
        authority: NativeLifecycleAuthority,
        pin_id: str,
    ) -> LifecyclePinPublicationReceipt:
        """Bind publication to one live authenticated lifecycle pin.

        This is an API boundary only: callers must first create the durable pin
        transaction through :mod:`pm_native_lifecycle`.  This method never
        mutates lifecycle state or claims that external acquisition/activation
        integration has occurred.
        """

        self.verify()
        receipt = self._verify_publication_pin(authority, pin_id)
        self._pin_authority = authority
        self._pin_id = receipt.pin_id
        self._publication_receipt = receipt
        return receipt

    @property
    def publication_receipt(self) -> LifecyclePinPublicationReceipt:
        if self._publication_receipt is None:
            raise CompositeIntegrityError(
                "composite publication has no authenticated lifecycle pin"
            )
        return self._publication_receipt

    def _require_publication_pin(self) -> LifecyclePinPublicationReceipt:
        if self._pin_authority is None or self._pin_id is None:
            raise CompositeIntegrityError(
                "row publication requires an authenticated lifecycle pin"
            )
        receipt = self._verify_publication_pin(
            self._pin_authority,
            self._pin_id,
        )
        self._publication_receipt = receipt
        return receipt

    def _iter_segment_rows(
        self, segment: CompositeSegment
    ) -> Iterator[tuple[_RawBarRow, CompositeBarRow]]:
        digest = hashlib.sha256()
        total_bytes = 0
        rows = 0
        previous_raw: Optional[_RawBarRow] = None
        with self._secure_file(segment.bars_relative_path) as (
            _,
            handle,
            identity,
        ):
            if identity.size != segment.bars_bytes:
                raise CompositeIntegrityError("bars artifact changed before iteration")
            if identity.size > self.limits.max_bars_bytes_per_segment:
                raise CompositeCapacityError("bars artifact exceeds iteration byte cap")
            header_line = self._bounded_line(
                handle, self.limits.max_row_bytes, "CSV header"
            )
            digest.update(header_line)
            total_bytes += len(header_line)
            if self._parse_csv_values(header_line, "CSV header") != _CSV_HEADER:
                raise CompositeIntegrityError("bars CSV header changed before iteration")
            while True:
                raw_line = self._bounded_line(
                    handle, self.limits.max_row_bytes, "bars CSV row"
                )
                if not raw_line:
                    break
                digest.update(raw_line)
                total_bytes += len(raw_line)
                raw = self._raw_bar_row(
                    raw_line, f"bars row {rows + 1}", segment.timeframe
                )
                rows += 1
                if rows > segment.rows or total_bytes > segment.bars_bytes:
                    raise CompositeIntegrityError(
                        "bars artifact exceeds its bounded iteration claim"
                    )
                row_open = _timestamp(raw.timestamp_utc)
                row_close = _timestamp(raw.close_timestamp_utc)
                interval_start = _timestamp(segment.interval_start_utc)
                interval_end = _timestamp(segment.interval_end_exclusive_utc)
                if not (
                    (
                        interval_start <= row_open < interval_end
                        and row_close <= interval_end
                    )
                    or row_open == interval_end
                ):
                    raise CompositeIntegrityError(
                        "bars row left its segment interval during iteration"
                    )
                if previous_raw is not None:
                    self._validate_observed_successor(
                        previous_raw, raw, segment.timeframe
                    )
                previous_raw = raw
                # A boundary successor sentinel is physical verification evidence,
                # not an owned half-open row.  The next contiguous segment emits
                # its identical first row after verify() proves the pair.
                if raw.timestamp_utc != segment.interval_end_exclusive_utc:
                    yield raw, CompositeBarRow(
                        timestamp_utc=raw.timestamp_utc,
                        close_timestamp_utc=raw.close_timestamp_utc,
                        close=raw.close,
                        header=_CSV_HEADER,
                        values=raw.values,
                        segment_id=segment.segment_id,
                        origin_run_id=segment.origin_run_id,
                    )
            self._assert_handle_identity(
                handle, identity, segment.bars_relative_path
            )
            if (
                digest.hexdigest() != segment.bars_sha256
                or total_bytes != segment.bars_bytes
                or rows != segment.rows
            ):
                raise CompositeIntegrityError(
                    "bars artifact changed during bounded streaming iteration"
                )

    def iter_cell_rows(
        self, symbol: str, timeframe: str
    ) -> Iterator[CompositeBarRow]:
        normalized_symbol = _normal_identifier(symbol, "symbol", upper=True)
        normalized_timeframe = _normal_identifier(
            timeframe, "timeframe", upper=True
        )
        matching = [
            item
            for item in self.manifest.cells
            if item.symbol == normalized_symbol
            and item.timeframe == normalized_timeframe
        ]
        if len(matching) != 1:
            raise CompositeSchemaError(
                f"composite contains no unique cell for {normalized_symbol}:{normalized_timeframe}"
            )
        # Two-pass policy: cryptographically verify all artifacts before the
        # first row can be observed, then stream the selected cell with bounded
        # line reads and a final same-handle hash/identity check.
        self.verify()
        self._require_publication_pin()
        cell = matching[0]

        def generate() -> Iterator[CompositeBarRow]:
            previous_raw: Optional[_RawBarRow] = None
            previous_segment: Optional[CompositeSegment] = None
            for segment in cell.segments:
                first_in_segment = True
                for raw, public in self._iter_segment_rows(segment):
                    if previous_raw is not None:
                        current_time = _timestamp(raw.timestamp_utc)
                        previous_time = _timestamp(previous_raw.timestamp_utc)
                        if current_time < previous_time:
                            raise CompositeIntegrityError(
                                "stitched rows are reordered"
                            )
                        if current_time == previous_time:
                            allowed_boundary = (
                                first_in_segment
                                and previous_segment is not None
                                and previous_segment.interval_end_exclusive_utc
                                == segment.interval_start_utc
                                == raw.timestamp_utc
                            )
                            if not allowed_boundary or (
                                previous_raw.values != raw.values
                                or previous_raw.raw_content != raw.raw_content
                            ):
                                raise CompositeIntegrityError(
                                    "stitched duplicate is not an identical boundary row"
                                )
                            first_in_segment = False
                            previous_segment = segment
                            continue
                    yield public
                    previous_raw = raw
                    previous_segment = segment
                    first_in_segment = False

        return generate()


__all__ = [
    "AcquisitionShortfall",
    "BrokerSourceIdentity",
    "CellVerificationReceipt",
    "CompositeBarRow",
    "CompositeCapacityError",
    "CompositeCell",
    "CompositeIntegrityError",
    "CompositeLimits",
    "CompositePathError",
    "CompositeProtectionGraph",
    "CompositeRunManifest",
    "CompositeSchemaError",
    "CompositeSegment",
    "CompositeVerificationReceipt",
    "COMPOSITE_SCHEMA_VERSION",
    "CURRENT_ACQUISITION_CONTRACT_VERSION",
    "CURRENT_PAGINATION_CONTRACT_VERSION",
    "CURRENT_PROVENANCE_SCHEMA_VERSION",
    "CURRENT_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION",
    "CURRENT_WINDOW_CONTRACT_VERSION",
    "LEGACY_ABSENT_PAGINATION_CONTRACT",
    "LEGACY_MISSING_ATTEMPT_LEDGER",
    "LEGACY_ADOPTION_SCHEMA_VERSION",
    "PROTECTION_GRAPH_SCHEMA_VERSION",
    "RECEIPT_SCHEMA_VERSION",
    "SEGMENT_SCHEMA_VERSION",
    "SHORTFALL_SCHEMA_VERSION",
    "LegacyAdoptionAttestation",
    "LifecyclePinPublicationReceipt",
    "NativeCompositeError",
    "NativeCompositeResolver",
    "ProtectionArtifact",
    "PUBLICATION_RECEIPT_SCHEMA_VERSION",
]
