"""Standalone, fail-closed lifecycle authority for native-market writers.

This module deliberately has no imports from acquisition, audit, activation,
MetaTrader, or the storage implementation.  It supplies the coordination
primitive those systems must eventually share:

* one operating-system lock per canonical native root, backed by a monotonic
  SQLite fencing token;
* bounded, per-volume growth reservations which distinguish peak allocation
  from final retained bytes;
* a crash-explicit operation state machine; and
* authenticated pointer receipts and evidence pins.

Creating an authority writes its bounded SQLite database beneath the explicitly
supplied ``state_root`` and two tiny root-keyed coordination files beside the
``native_root``.  The shared sibling lock/fence location prevents callers from
creating split-brain authorities merely by choosing different state roots.  It
never creates, deletes, archives, restores, or otherwise changes native or cold
evidence.  Pointer publication is an explicit helper and is the only method in
this module which can write beneath ``native_root``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import sqlite3
import stat
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence


MIB = 1024 * 1024
DATABASE_SCHEMA = "fxpm-native-lifecycle-db-v4"
OPERATION_RECORD_SCHEMA = "fxpm-native-lifecycle-operation-record-v1"
OPERATION_LEDGER_STATE_SCHEMA = "fxpm-native-lifecycle-operation-ledger-v1"
POINTER_RECEIPT_SCHEMA = "fxpm-native-pointer-cas-receipt-v1"
POINTER_HEAD_SCHEMA = "fxpm-native-pointer-current-head-v1"
POINTER_LEDGER_STATE_SCHEMA = "fxpm-native-pointer-ledger-state-v1"
PIN_SCHEMA = "fxpm-native-lifecycle-pin-v1"
RESERVATION_RECEIPT_SCHEMA = "fxpm-native-reservation-receipt-v1"
RUNTIME_WITNESS_SCHEMA = "fxpm-runtime-monotonic-witness-v1"
FENCE_ANCHOR_SCHEMA = "fxpm-native-lifecycle-fence-anchor-v1"
BROKER_EXECUTION_CLAIM_SCHEMA = "fxpm-broker-execution-claim-v2"
BROKER_ACCOUNT_HEAD_SCHEMA = "fxpm-broker-account-execution-head-v2"
BROKER_HIGH_WATER_SCHEMA = "fxpm-broker-account-high-water-v1"
BROKER_LEDGER_STATE_SCHEMA = "fxpm-broker-ledger-state-v2"
RUNTIME_CLAIM_HEAD_SCHEMA = "fxpm-runtime-claim-head-v2"
RUNTIME_ACCOUNT_BINDING_SCHEMA = "fxpm-runtime-account-binding-v1"
ARCHIVE_HEAD_SCHEMA = "fxpm-lifecycle-archive-head-v1"
OPERATION_TOMBSTONE_SCHEMA = "fxpm-operation-tombstone-v1"
PIN_TOMBSTONE_SCHEMA = "fxpm-pin-tombstone-v1"
RUNTIME_TOMBSTONE_SCHEMA = "fxpm-runtime-tombstone-v2"
ABSENT_POINTER = "ABSENT"
GENESIS_SHA256 = hashlib.sha256(b"").hexdigest()
SQLITE_APPLICATION_ID = 0x4658504D  # ASCII "FXPM"
SQLITE_USER_VERSION = 4
COORDINATED_ROLLBACK_BOUNDARY = (
    "A coordinated rollback of both lifecycle SQLite and its signed fence anchor, "
    "or compromise of the lifecycle HMAC key, is outside this local authority. "
    "Funded deployment must reconcile broker/source high-water marks against an "
    "OS-, hardware-, or independently hosted monotonic authority before trading."
)
# Leave a small guard band below SQLite's signed-64-bit ceiling so additions
# and version increments can be rejected deterministically before binding.
SQLITE_SIGNED_COUNTER_MAX = 9_223_372_036_854_775_000


class NativeLifecycleError(RuntimeError):
    """Base class for lifecycle failures."""


class LifecycleRootError(NativeLifecycleError):
    """A supplied root is not a supported, hydrated local filesystem root."""


class LifecycleLockTimeout(NativeLifecycleError):
    """The root-keyed operating-system lock was not obtained by its deadline."""


class LifecycleBusyError(NativeLifecycleError):
    """SQLite remained busy beyond the configured bounded deadline."""


class LifecycleBoundError(NativeLifecycleError):
    """A hard row, byte, file, run, payload, or filesystem bound was reached."""


class LifecycleDatabaseFullError(LifecycleBoundError):
    """SQLite refused a transaction because the fixed page cap was reached."""


class LifecycleStateError(NativeLifecycleError):
    """An operation or lease does not satisfy the strict state contract."""


class LifecycleIntegrityError(NativeLifecycleError):
    """Authenticated state, a pointer, pin, or immutable path is inconsistent."""


class LifecycleCASMismatch(LifecycleIntegrityError):
    """A pointer no longer has the exact value expected by a publisher."""


class LifecycleRecoveryRequired(LifecycleStateError):
    """Publication may have happened but durable lifecycle acknowledgement did not."""


class OperationKind(str, Enum):
    NEW_ACQUISITION = "NEW_ACQUISITION"
    RESUME_ACQUISITION = "RESUME_ACQUISITION"
    AUDIT = "AUDIT"
    TERMS = "TERMS"
    ARCHIVE = "ARCHIVE"
    RESTORE = "RESTORE"
    RUNTIME_STATE = "RUNTIME_STATE"
    OPTIMIZER_WORKSPACE = "OPTIMIZER_WORKSPACE"
    GENERIC_OPERATIONAL = "GENERIC_OPERATIONAL"


class OperationState(str, Enum):
    PREPARED = "PREPARED"
    WRITING = "WRITING"
    STAGED = "STAGED"
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"
    RECOVERY_BLOCKED = "RECOVERY_BLOCKED"


TERMINAL_STATES = {OperationState.COMMITTED.value, OperationState.ABORTED.value}
STRICT_TRANSITIONS: Mapping[str, frozenset[str]] = {
    OperationState.PREPARED.value: frozenset(
        {OperationState.WRITING.value, OperationState.ABORTING.value}
    ),
    OperationState.WRITING.value: frozenset(
        {OperationState.STAGED.value, OperationState.ABORTING.value}
    ),
    OperationState.STAGED.value: frozenset(
        {OperationState.PUBLISHING.value, OperationState.ABORTING.value}
    ),
    OperationState.PUBLISHING.value: frozenset(
        {OperationState.PUBLISHED.value, OperationState.ABORTING.value}
    ),
    OperationState.PUBLISHED.value: frozenset({OperationState.COMMITTED.value}),
    OperationState.ABORTING.value: frozenset({OperationState.ABORTED.value}),
    OperationState.RECOVERY_BLOCKED.value: frozenset(
        {OperationState.ABORTING.value}
    ),
    OperationState.COMMITTED.value: frozenset(),
    OperationState.ABORTED.value: frozenset(),
}


def _nonnegative(name: str, value: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, bool) or result != value:
        raise ValueError(f"{name} must be an exact integer")
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    if result > SQLITE_SIGNED_COUNTER_MAX:
        raise ValueError(f"{name} exceeds the signed SQLite counter bound")
    return result


@dataclass(frozen=True)
class OperationalGrowth:
    """Peak/final growth for one explicitly allowed operational subdomain."""

    domain: str
    final_bytes: int
    peak_bytes: int
    files_delta: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "domain", _safe_identifier(self.domain, label="quota domain", max_length=64))
        for name in ("final_bytes", "peak_bytes", "files_delta"):
            object.__setattr__(self, name, _nonnegative(name, getattr(self, name)))
        if self.peak_bytes < self.final_bytes:
            raise ValueError("operational peak_bytes cannot be below final_bytes")


@dataclass(frozen=True)
class OperationalDomain:
    """A canonical external root governed by the institution-wide authority."""

    root: Path
    max_bytes: int
    max_files: int
    max_active_reservations: int = 600

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))
        for name in ("max_bytes", "max_files", "max_active_reservations"):
            raw = getattr(self, name)
            try:
                value = int(raw)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"operational domain {name} must be an integer") from exc
            if isinstance(raw, bool) or value != raw:
                raise ValueError(f"operational domain {name} must be an exact integer")
            if value < 1:
                raise ValueError(f"operational domain {name} must be positive")
            if value > SQLITE_SIGNED_COUNTER_MAX:
                raise ValueError(f"operational domain {name} exceeds SQLite's bound")
            object.__setattr__(self, name, value)
        if self.max_active_reservations > 600:
            raise ValueError("max_active_reservations cannot exceed the 100x6 FxPM stream cap")


@dataclass(frozen=True)
class GrowthBudget:
    """A vector reservation, separating retained growth from allocation peak.

    ``*_peak_bytes`` is the maximum cumulative allocation that the operation
    may materialize.  ``*_final_bytes`` is the retained growth after staging
    is removed.  Restore therefore reserves two copies at peak but only one
    copy against the final hot-store cap.
    """

    kind: str
    native_final_bytes: int = 0
    native_peak_bytes: int = 0
    cold_final_bytes: int = 0
    cold_peak_bytes: int = 0
    native_files_delta: int = 0
    cold_files_delta: int = 0
    complete_run_delta: int = 0
    inflight_run_delta: int = 0
    operational: tuple[OperationalGrowth, ...] = ()

    def __post_init__(self) -> None:
        try:
            kind = OperationKind(
                self.kind.value if isinstance(self.kind, OperationKind) else str(self.kind)
            ).value
        except ValueError as exc:
            raise ValueError(f"unsupported lifecycle operation kind: {self.kind!r}") from exc
        object.__setattr__(self, "kind", kind)
        for name in (
            "native_final_bytes",
            "native_peak_bytes",
            "cold_final_bytes",
            "cold_peak_bytes",
            "native_files_delta",
            "cold_files_delta",
            "complete_run_delta",
            "inflight_run_delta",
        ):
            object.__setattr__(self, name, _nonnegative(name, getattr(self, name)))
        if self.native_peak_bytes < self.native_final_bytes:
            raise ValueError("native_peak_bytes cannot be below native_final_bytes")
        if self.cold_peak_bytes < self.cold_final_bytes:
            raise ValueError("cold_peak_bytes cannot be below cold_final_bytes")
        normalized: list[OperationalGrowth] = []
        seen: set[str] = set()
        for raw in self.operational:
            item = raw if isinstance(raw, OperationalGrowth) else OperationalGrowth(**raw)
            if item.domain in seen:
                raise ValueError(f"duplicate operational quota domain: {item.domain}")
            seen.add(item.domain)
            normalized.append(item)
        object.__setattr__(self, "operational", tuple(sorted(normalized, key=lambda item: item.domain)))

    @classmethod
    def new_acquisition(
        cls,
        *,
        expected_run_bytes: int,
        file_count: int,
        auxiliary_bytes: int = 0,
        auxiliary_files: int = 0,
    ) -> "GrowthBudget":
        amount = _nonnegative("expected_run_bytes", expected_run_bytes) + _nonnegative(
            "auxiliary_bytes", auxiliary_bytes
        )
        files = _nonnegative("file_count", file_count) + _nonnegative(
            "auxiliary_files", auxiliary_files
        )
        return cls(
            kind=OperationKind.NEW_ACQUISITION.value,
            native_final_bytes=amount,
            native_peak_bytes=amount,
            native_files_delta=files,
            complete_run_delta=1,
            inflight_run_delta=1,
        )

    @classmethod
    def resume_acquisition(
        cls,
        *,
        expected_final_run_bytes: int,
        existing_materialized_run_bytes: int,
        remaining_file_count: int,
        auxiliary_bytes: int = 0,
        auxiliary_files: int = 0,
    ) -> "GrowthBudget":
        expected = _nonnegative("expected_final_run_bytes", expected_final_run_bytes)
        existing = _nonnegative(
            "existing_materialized_run_bytes", existing_materialized_run_bytes
        )
        if existing > expected:
            raise ValueError(
                "existing materialized run bytes exceed the declared final run bound"
            )
        amount = expected - existing + _nonnegative("auxiliary_bytes", auxiliary_bytes)
        files = _nonnegative("remaining_file_count", remaining_file_count) + _nonnegative(
            "auxiliary_files", auxiliary_files
        )
        return cls(
            kind=OperationKind.RESUME_ACQUISITION.value,
            native_final_bytes=amount,
            native_peak_bytes=amount,
            native_files_delta=files,
            complete_run_delta=1,
            inflight_run_delta=0,
        )

    @classmethod
    def audit(cls, *, expected_bytes: int, file_count: int) -> "GrowthBudget":
        amount = _nonnegative("expected_bytes", expected_bytes)
        return cls(
            kind=OperationKind.AUDIT.value,
            native_final_bytes=amount,
            native_peak_bytes=amount,
            native_files_delta=_nonnegative("file_count", file_count),
        )

    @classmethod
    def terms(cls, *, expected_bytes: int, file_count: int) -> "GrowthBudget":
        amount = _nonnegative("expected_bytes", expected_bytes)
        return cls(
            kind=OperationKind.TERMS.value,
            native_final_bytes=amount,
            native_peak_bytes=amount,
            native_files_delta=_nonnegative("file_count", file_count),
        )

    @classmethod
    def archive(cls, *, expected_archive_bytes: int, file_count: int = 1) -> "GrowthBudget":
        amount = _nonnegative("expected_archive_bytes", expected_archive_bytes)
        return cls(
            kind=OperationKind.ARCHIVE.value,
            cold_final_bytes=amount,
            cold_peak_bytes=amount,
            cold_files_delta=_nonnegative("file_count", file_count),
        )

    @classmethod
    def restore(
        cls,
        *,
        missing_payload_bytes: int,
        missing_file_count: int,
        restores_complete_run: bool = False,
    ) -> "GrowthBudget":
        amount = _nonnegative("missing_payload_bytes", missing_payload_bytes)
        return cls(
            kind=OperationKind.RESTORE.value,
            native_final_bytes=amount,
            native_peak_bytes=2 * amount,
            native_files_delta=_nonnegative("missing_file_count", missing_file_count),
            complete_run_delta=1 if restores_complete_run else 0,
        )

    @classmethod
    def operational_growth(
        cls,
        *,
        kind: OperationKind | str,
        domain: str,
        final_bytes: int,
        peak_bytes: int,
        file_count: int,
    ) -> "GrowthBudget":
        resolved = OperationKind(kind.value if isinstance(kind, OperationKind) else str(kind)).value
        if resolved not in {
            OperationKind.RUNTIME_STATE.value,
            OperationKind.OPTIMIZER_WORKSPACE.value,
            OperationKind.GENERIC_OPERATIONAL.value,
        }:
            raise ValueError("operational_growth requires an operational operation kind")
        return cls(
            kind=resolved,
            operational=(
                OperationalGrowth(
                    domain=domain,
                    final_bytes=final_bytes,
                    peak_bytes=peak_bytes,
                    files_delta=file_count,
                ),
            ),
        )

    def with_top_up(
        self,
        *,
        native_final_bytes: int = 0,
        native_peak_bytes: int = 0,
        cold_final_bytes: int = 0,
        cold_peak_bytes: int = 0,
        native_files: int = 0,
        cold_files: int = 0,
        operational: Sequence[OperationalGrowth] = (),
    ) -> "GrowthBudget":
        """Return a larger budget; run deltas are deliberately immutable."""
        merged = {item.domain: item for item in self.operational}
        for raw in operational:
            item = raw if isinstance(raw, OperationalGrowth) else OperationalGrowth(**raw)
            prior = merged.get(item.domain)
            merged[item.domain] = (
                item
                if prior is None
                else OperationalGrowth(
                    domain=item.domain,
                    final_bytes=prior.final_bytes + item.final_bytes,
                    peak_bytes=prior.peak_bytes + item.peak_bytes,
                    files_delta=prior.files_delta + item.files_delta,
                )
            )
        return GrowthBudget(
            kind=self.kind,
            native_final_bytes=self.native_final_bytes
            + _nonnegative("native_final_bytes", native_final_bytes),
            native_peak_bytes=self.native_peak_bytes
            + _nonnegative("native_peak_bytes", native_peak_bytes),
            cold_final_bytes=self.cold_final_bytes
            + _nonnegative("cold_final_bytes", cold_final_bytes),
            cold_peak_bytes=self.cold_peak_bytes
            + _nonnegative("cold_peak_bytes", cold_peak_bytes),
            native_files_delta=self.native_files_delta
            + _nonnegative("native_files", native_files),
            cold_files_delta=self.cold_files_delta
            + _nonnegative("cold_files", cold_files),
            complete_run_delta=self.complete_run_delta,
            inflight_run_delta=self.inflight_run_delta,
            operational=tuple(merged.values()),
        )


@dataclass(frozen=True)
class LifecycleLimits:
    """All capacity and retry limits are finite and explicit."""

    max_hot_bytes: int
    max_cold_bytes: int
    min_volume_free_bytes: int
    max_hot_files: int
    max_cold_files: int
    max_complete_hot_runs: int
    max_inflight_runs: int
    max_operations: int = 20_000
    max_active_operations: int = 32
    max_broker_execution_claims: int = 200_000
    max_broker_account_scopes: int = 2_048
    max_broker_reorder_sequences: int = 64
    max_operation_reorder_sequences: int = 64
    broker_full_audit_batch_rows: int = 256
    max_payload_bytes: int = 128 * 1024
    max_files_per_operation: int = 100_000
    max_pins: int = 2_048
    max_pin_payload_bytes: int = MIB
    max_pointer_bytes: int = MIB
    max_scan_entries: int = 500_000
    max_db_bytes: int = 64 * MIB
    page_size: int = 4096
    journal_size_limit: int = 4 * MIB
    db_busy_deadline_seconds: float = 5.0
    lock_deadline_seconds: float = 10.0
    lease_seconds: float = 3600.0

    def __post_init__(self) -> None:
        integer_fields = (
            "max_hot_bytes",
            "max_cold_bytes",
            "min_volume_free_bytes",
            "max_hot_files",
            "max_cold_files",
            "max_complete_hot_runs",
            "max_inflight_runs",
            "max_operations",
            "max_active_operations",
            "max_broker_execution_claims",
            "max_broker_account_scopes",
            "max_payload_bytes",
            "max_files_per_operation",
            "max_pins",
            "max_pin_payload_bytes",
            "max_pointer_bytes",
            "max_scan_entries",
            "max_db_bytes",
            "page_size",
            "journal_size_limit",
            "broker_full_audit_batch_rows",
        )
        for name in integer_fields:
            raw = getattr(self, name)
            try:
                value = int(raw)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{name} must be an integer") from exc
            if isinstance(raw, bool) or value != raw:
                raise ValueError(f"{name} must be an exact integer")
            if value < 1:
                raise ValueError(f"{name} must be positive")
            if value > SQLITE_SIGNED_COUNTER_MAX:
                raise ValueError(f"{name} exceeds SQLite's signed counter bound")
            object.__setattr__(self, name, value)
        for name in (
            "max_broker_reorder_sequences",
            "max_operation_reorder_sequences",
        ):
            raw = getattr(self, name)
            value = _nonnegative(name, raw)
            object.__setattr__(self, name, value)
        if max(
            self.max_broker_reorder_sequences,
            self.max_operation_reorder_sequences,
        ) >= self.max_broker_execution_claims:
            raise ValueError(
                "broker live-row cap must exceed each admitted reorder window"
            )
        if self.page_size < 512 or self.page_size > 65536 or self.page_size & (self.page_size - 1):
            raise ValueError("page_size must be a SQLite power-of-two page size")
        if self.max_db_bytes < 16 * self.page_size:
            raise ValueError("max_db_bytes must provide at least sixteen SQLite pages")
        if self.max_db_bytes % self.page_size:
            raise ValueError("max_db_bytes must be an exact multiple of page_size")
        for name in ("db_busy_deadline_seconds", "lock_deadline_seconds", "lease_seconds"):
            value = float(getattr(self, name))
            if not (0.01 <= value <= 7 * 24 * 3600):
                raise ValueError(f"{name} is outside its finite supported range")
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class RecoveryEntry:
    operation_id: str
    previous_state: str
    current_state: str
    previous_fencing_token: int
    current_fencing_token: int
    instruction: str


@dataclass(frozen=True)
class StoreUsage:
    bytes: int
    files: int
    complete_runs: int = 0
    inflight_runs: int = 0


@dataclass(frozen=True)
class ReservationSnapshot:
    operation_id: str
    state: str
    budget: GrowthBudget
    materialized_native_peak_bytes: int
    materialized_native_final_bytes: int
    materialized_cold_peak_bytes: int
    materialized_cold_final_bytes: int
    materialized_native_files: int
    materialized_cold_files: int
    materialized_operational: Mapping[str, Mapping[str, int]]


@dataclass(frozen=True)
class RuntimeStorageReservation:
    """Structural adapter for ``pm_state_runtime.StorageReservation``."""

    authority_identity_sha256: str
    reservation_id: str
    operational_domain_root: str
    stream_subpath: str
    stream_identity_sha256: str
    reserved_peak_bytes: int
    institution_cap_bytes: int
    admitted_streams_cap: int
    receipt_mac: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimeMonotonicWitness:
    """Externally authoritative high-water marks for one runtime stream."""

    authority_identity_sha256: str
    reservation_id: str
    witness_version: int
    writer_epoch: int
    reducer_contract_sha256: str
    max_bar_sequence: int
    bar_chain_sha256: str
    checkpoint_generation: int
    checkpoint_sha256: str
    max_order_operation_sequence: int
    max_order_event_sequence: int
    order_chain_sha256: str
    fencing_token: int
    receipt_mac: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimeWitnessProposal:
    writer_epoch: int
    reducer_contract_sha256: str
    max_bar_sequence: int
    bar_chain_sha256: str
    checkpoint_generation: int
    checkpoint_sha256: str
    max_order_operation_sequence: int
    max_order_event_sequence: int
    order_chain_sha256: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerExecutionClaim:
    """Authenticated institution-wide ownership of one broker execution."""

    authority_identity_sha256: str
    claim_id: str
    account_scope_sha256: str
    broker_execution_id: str
    broker_execution_sequence: int
    claim_sequence: int
    stream_identity_sha256: str
    operation_id: str
    operation_sequence: int
    payload_sha256: str
    fencing_token: int
    receipt_mac: str

    def canonical(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerAccountHighWaterReceipt:
    """Independent broker-history observation used to retire old ticket IDs."""

    account_scope_sha256: str
    observed_sequence_highwater: int
    source_snapshot_sha256: str
    observed_at: str
    observation_id: str
    mac: str
    schema_version: str = BROKER_HIGH_WATER_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_HIGH_WATER_SCHEMA:
            raise ValueError("broker high-water schema mismatch")
        if not _is_sha256(self.account_scope_sha256):
            raise ValueError("broker high-water account digest is invalid")
        _nonnegative(
            "observed_sequence_highwater", self.observed_sequence_highwater
        )
        if not _is_sha256(self.source_snapshot_sha256):
            raise ValueError("broker high-water snapshot digest is invalid")
        _safe_identifier(
            self.observation_id, label="observation_id", max_length=256
        )
        try:
            parsed = datetime.fromisoformat(
                str(self.observed_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("observed_at must be ISO-8601") from exc
        if parsed.tzinfo is None:
            raise ValueError("observed_at must carry a timezone")
        canonical_timestamp = parsed.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        if self.observed_at != canonical_timestamp:
            raise ValueError("observed_at must use canonical UTC representation")
        if not isinstance(self.mac, str) or len(self.mac) != 64:
            raise ValueError("broker high-water MAC is invalid")

    def body(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "account_scope_sha256": self.account_scope_sha256,
            "observed_sequence_highwater": self.observed_sequence_highwater,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "observed_at": self.observed_at,
            "observation_id": self.observation_id,
        }

    def canonical(self) -> dict[str, Any]:
        return {**self.body(), "mac": self.mac}

    @classmethod
    def issue(
        cls,
        key: bytes,
        *,
        account_scope_sha256: str,
        observed_sequence_highwater: int,
        source_snapshot_sha256: str,
        observed_at: str,
        observation_id: str,
    ) -> "BrokerAccountHighWaterReceipt":
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("broker high-water key must contain at least 32 bytes")
        account = str(account_scope_sha256).lower()
        snapshot = str(source_snapshot_sha256).lower()
        if not _is_sha256(account) or not _is_sha256(snapshot):
            raise ValueError("broker high-water digests must be SHA-256")
        body = {
            "schema_version": BROKER_HIGH_WATER_SCHEMA,
            "account_scope_sha256": account,
            "observed_sequence_highwater": _nonnegative(
                "observed_sequence_highwater", observed_sequence_highwater
            ),
            "source_snapshot_sha256": snapshot,
            "observed_at": str(observed_at),
            "observation_id": _safe_identifier(
                observation_id, label="observation_id", max_length=256
            ),
        }
        try:
            parsed = datetime.fromisoformat(
                body["observed_at"].replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("observed_at must be ISO-8601") from exc
        if parsed.tzinfo is None:
            raise ValueError("observed_at must carry a timezone")
        body["observed_at"] = parsed.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        material = b"broker-account-high-water\x00" + _canonical_bytes(body)
        return cls(
            **body,
            mac=hmac.new(key, material, hashlib.sha256).hexdigest(),
        )


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _bounded_canonical_bytes(
    value: Mapping[str, Any], *, max_bytes: int, label: str
) -> bytes:
    """Canonicalize hostile public JSON without an unbounded intermediate."""

    bound = _nonnegative(f"{label} max_bytes", max_bytes)
    if bound < 2:
        raise LifecycleBoundError(f"{label} byte bound is too small")
    stack: list[tuple[Any, int]] = [(value, 0)]
    active: set[int] = set()
    nodes = 0
    while stack:
        item, depth = stack.pop()
        if depth < 0:
            active.discard(id(item))
            continue
        nodes += 1
        if nodes > bound:
            raise LifecycleBoundError(f"{label} structure exceeds its byte bound")
        if depth > 64:
            raise LifecycleBoundError(f"{label} nesting exceeds 64 levels")
        if item is None or isinstance(item, (bool, int, float)):
            if isinstance(item, int) and not isinstance(item, bool):
                if item.bit_length() > bound * 4:
                    raise LifecycleBoundError(f"{label} integer exceeds its byte bound")
            if isinstance(item, float) and not (float("-inf") < item < float("inf")):
                raise LifecycleIntegrityError(f"{label} contains a non-finite number")
            continue
        if isinstance(item, str):
            if len(item) > bound or len(item.encode("utf-8")) > bound:
                raise LifecycleBoundError(f"{label} string exceeds its byte bound")
            continue
        if isinstance(item, Mapping):
            identity = id(item)
            if identity in active:
                raise LifecycleIntegrityError(f"{label} contains a reference cycle")
            active.add(identity)
            stack.append((item, -1))
            try:
                entries: list[tuple[Any, Any]] = []
                for index, pair in enumerate(item.items()):
                    if index >= bound:
                        raise LifecycleBoundError(
                            f"{label} mapping exceeds its byte bound"
                        )
                    entries.append(pair)
            except (TypeError, ValueError, RuntimeError) as exc:
                raise LifecycleIntegrityError(f"{label} mapping is unreadable") from exc
            for key, child in reversed(entries):
                if not isinstance(key, str):
                    raise LifecycleIntegrityError(
                        f"{label} object keys must be strings"
                    )
                stack.append((child, depth + 1))
            continue
        if isinstance(item, (list, tuple)):
            identity = id(item)
            if identity in active:
                raise LifecycleIntegrityError(f"{label} contains a reference cycle")
            active.add(identity)
            stack.append((item, -1))
            if len(item) > bound:
                raise LifecycleBoundError(f"{label} sequence exceeds its byte bound")
            for child in reversed(item):
                stack.append((child, depth + 1))
            continue
        raise LifecycleIntegrityError(
            f"{label} contains unsupported JSON type {type(item).__name__}"
        )

    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        check_circular=True,
    )
    chunks: list[bytes] = []
    total = 1  # canonical trailing newline
    try:
        for chunk in encoder.iterencode(value):
            encoded = chunk.encode("utf-8")
            total += len(encoded)
            if total > bound:
                raise LifecycleBoundError(f"{label} exceeds its byte bound")
            chunks.append(encoded)
    except LifecycleBoundError:
        raise
    except (TypeError, ValueError, RecursionError, OverflowError) as exc:
        raise LifecycleIntegrityError(f"{label} is not canonical JSON") from exc
    return b"".join(chunks) + b"\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path, *, max_bytes: Optional[int] = None) -> tuple[str, int]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise LifecycleIntegrityError(f"required file is unavailable: {path}") from exc
    forbidden_windows_attributes = (
        0x00000400  # REPARSE_POINT
        | 0x00001000  # OFFLINE
        | 0x00040000  # RECALL_ON_OPEN
        | 0x00100000  # UNPINNED
        | 0x00400000  # RECALL_ON_DATA_ACCESS
    )
    if (
        not stat.S_ISREG(before.st_mode)
        or path.is_symlink()
        or int(before.st_nlink) != 1
        or int(getattr(before, "st_file_attributes", 0))
        & forbidden_windows_attributes
    ):
        raise LifecycleIntegrityError(
            f"required file is not a regular unaliased local file: {path}"
        )
    size = int(before.st_size)
    if max_bytes is not None and (size < 0 or size > max_bytes):
        raise LifecycleIntegrityError(f"file exceeds its authenticated byte bound: {path}")
    digest = hashlib.sha256()
    observed = 0
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or int(opened.st_nlink) != 1
                or (int(opened.st_dev), int(opened.st_ino))
                != (int(before.st_dev), int(before.st_ino))
            ):
                raise LifecycleIntegrityError(
                    f"required file identity changed before authentication: {path}"
                )
            while True:
                chunk = handle.read(MIB)
                if not chunk:
                    break
                observed += len(chunk)
                if max_bytes is not None and observed > max_bytes:
                    raise LifecycleIntegrityError(
                        f"file grew beyond its authenticated byte bound: {path}"
                    )
                digest.update(chunk)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise LifecycleIntegrityError(f"required file is unreadable: {path}") from exc
    try:
        final = path.lstat()
    except OSError as exc:
        raise LifecycleIntegrityError(
            f"required file disappeared during authentication: {path}"
        ) from exc
    if (
        observed != size
        or int(after.st_size) != size
        or int(after.st_nlink) != 1
        or int(final.st_nlink) != 1
        or (int(final.st_dev), int(final.st_ino))
        != (int(before.st_dev), int(before.st_ino))
    ):
        raise LifecycleIntegrityError(f"file changed while authenticating it: {path}")
    return digest.hexdigest(), observed


def _read_bounded_file(path: Path, *, max_bytes: int, label: str) -> bytes:
    """Read one small control file without a stat/read growth or alias race."""

    bound = _nonnegative(f"{label} byte bound", max_bytes)
    if bound <= 0:
        raise ValueError(f"{label} byte bound must be positive")
    try:
        before = path.lstat()
    except OSError as exc:
        raise LifecycleIntegrityError(f"{label} is unavailable: {path}") from exc
    forbidden_windows_attributes = (
        0x00000400
        | 0x00001000
        | 0x00040000
        | 0x00100000
        | 0x00400000
    )
    if (
        not stat.S_ISREG(before.st_mode)
        or path.is_symlink()
        or int(before.st_nlink) != 1
        or int(getattr(before, "st_file_attributes", 0))
        & forbidden_windows_attributes
    ):
        raise LifecycleIntegrityError(f"{label} is not a regular unaliased local file")
    size = int(before.st_size)
    if size <= 0 or size > bound:
        raise LifecycleIntegrityError(f"{label} size is invalid")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or int(opened.st_nlink) != 1
                or (int(opened.st_dev), int(opened.st_ino))
                != (int(before.st_dev), int(before.st_ino))
            ):
                raise LifecycleIntegrityError(f"{label} identity changed before read")
            payload = handle.read(bound + 1)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise LifecycleIntegrityError(f"{label} is unreadable") from exc
    try:
        final = path.lstat()
    except OSError as exc:
        raise LifecycleIntegrityError(f"{label} disappeared during read") from exc
    if (
        len(payload) != size
        or len(payload) > bound
        or int(after.st_size) != size
        or int(after.st_nlink) != 1
        or int(final.st_nlink) != 1
        or (int(final.st_dev), int(final.st_ino))
        != (int(before.st_dev), int(before.st_ino))
    ):
        raise LifecycleIntegrityError(f"{label} changed while being read")
    return payload


def _is_sha256(value: Any) -> bool:
    text = str(value or "").lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _path_parts_casefold(path: Path) -> tuple[str, ...]:
    return tuple(part.casefold() for part in path.parts)


def _looks_cloud_managed(path: Path) -> bool:
    parts = _path_parts_casefold(path)
    if any(part == "onedrive" or part.startswith("onedrive -") for part in parts):
        return True
    for name in ("OneDrive", "OneDriveCommercial", "OneDriveConsumer"):
        raw = os.environ.get(name)
        if not raw:
            continue
        try:
            cloud = Path(raw).absolute()
            path.absolute().relative_to(cloud)
            return True
        except (OSError, ValueError):
            continue
    return False


def _windows_attributes(path: Path) -> int:
    if os.name != "nt":
        return 0
    import ctypes

    value = ctypes.windll.kernel32.GetFileAttributesW(str(path))
    if value == 0xFFFFFFFF:
        raise LifecycleRootError(f"cannot inspect Windows attributes for {path}")
    return int(value)


def _windows_drive_type(path: Path) -> int:
    if os.name != "nt":
        raise LifecycleRootError("Windows drive type requested on a non-Windows host")
    import ctypes

    drive_root = path.anchor or (path.drive + "\\")
    return int(ctypes.windll.kernel32.GetDriveTypeW(str(drive_root)))


def _validate_existing_component(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise LifecycleRootError(f"cannot inspect {label}: {path}") from exc
    if stat.S_ISLNK(mode) or path.is_symlink():
        raise LifecycleRootError(f"{label} contains a symlink/reparse component: {path}")
    if os.name == "nt":
        attributes = _windows_attributes(path)
        file_attribute_reparse_point = 0x00000400
        file_attribute_offline = 0x00001000
        file_attribute_recall_on_open = 0x00040000
        file_attribute_unpinned = 0x00100000
        file_attribute_recall_on_data_access = 0x00400000
        forbidden = (
            file_attribute_reparse_point
            | file_attribute_offline
            | file_attribute_recall_on_open
            | file_attribute_unpinned
            | file_attribute_recall_on_data_access
        )
        if attributes & forbidden:
            raise LifecycleRootError(
                f"{label} is reparse/offline/unpinned cloud storage: {path}"
            )


def _existing_ancestor(path: Path) -> Path:
    candidate = path
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise LifecycleRootError(f"no existing filesystem ancestor for {path}")
        candidate = parent
    return candidate


def _validate_local_root(
    supplied: Path,
    *,
    label: str,
    must_exist: bool,
    allow_unsafe_roots: bool,
) -> Path:
    path = Path(supplied)
    if not path.is_absolute():
        raise LifecycleRootError(f"{label} must be an explicit absolute path")
    raw = str(path)
    if raw.startswith("\\\\") or raw.startswith("//"):
        raise LifecycleRootError(f"{label} cannot be a UNC/network path")
    lexical = path.absolute()
    if not allow_unsafe_roots and _looks_cloud_managed(lexical):
        raise LifecycleRootError(f"{label} cannot be inside OneDrive/cloud storage")
    ancestor = _existing_ancestor(lexical)
    if must_exist and not lexical.exists():
        raise LifecycleRootError(f"{label} does not exist: {lexical}")
    if lexical.exists() and not lexical.is_dir():
        raise LifecycleRootError(f"{label} must be a directory: {lexical}")
    cursor = ancestor
    while True:
        if not allow_unsafe_roots:
            _validate_existing_component(cursor, label)
        if cursor.parent == cursor:
            break
        cursor = cursor.parent
    if os.name == "nt":
        drive_type = _windows_drive_type(ancestor)
        # Only DRIVE_FIXED=3 is a durable authority. Mapped network drives
        # report DRIVE_REMOTE=4 even when they have a drive letter; RAM disks
        # are local but do not survive the rollback/restart threat model.
        if drive_type != 3:
            raise LifecycleRootError(
                f"{label} is not on a fixed local/hydrated volume (drive type {drive_type})"
            )
    return lexical.resolve(strict=must_exist)


def validate_durable_local_root(
    supplied: Path,
    *,
    label: str = "storage_root",
    must_exist: bool = True,
) -> Path:
    """Canonical local durability policy shared by runtime/storage modules."""
    return _validate_local_root(
        Path(supplied),
        label=label,
        must_exist=must_exist,
        allow_unsafe_roots=False,
    )


def _contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _inside_git_worktree(path: Path) -> bool:
    cursor = _existing_ancestor(path)
    while True:
        if (cursor / ".git").exists():
            return True
        if cursor.parent == cursor:
            return False
        cursor = cursor.parent


def _volume_key(path: Path) -> str:
    ancestor = _existing_ancestor(path)
    try:
        device = int(ancestor.stat().st_dev)
    except OSError as exc:
        raise LifecycleRootError(f"cannot identify filesystem volume for {path}") from exc
    anchor = os.path.normcase(ancestor.anchor or str(ancestor))
    return f"device:{device}:{anchor}"


def _filesystem_identity(path: Path) -> Mapping[str, Any]:
    try:
        observed = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise LifecycleRootError(f"cannot identify filesystem object: {path}") from exc
    if stat.S_ISLNK(observed.st_mode):
        raise LifecycleRootError(f"filesystem identity target is a symlink: {path}")
    if stat.S_ISREG(observed.st_mode) and int(observed.st_nlink) != 1:
        raise LifecycleRootError(
            f"filesystem identity target is a hard-linked alias: {path}"
        )
    if os.name == "nt" and _windows_attributes(path) & (
        0x00000400 | 0x00001000 | 0x00040000 | 0x00100000 | 0x00400000
    ):
        raise LifecycleRootError(
            f"filesystem identity target is reparse/offline/unpinned: {path}"
        )
    return {
        "device": int(observed.st_dev),
        "inode": int(observed.st_ino),
        "kind": int(stat.S_IFMT(observed.st_mode)),
        "volume_key": _volume_key(path),
    }


def _tree_usage(
    root: Path,
    *,
    max_entries: int,
    include_runs: bool,
    excluded_roots: Sequence[Path] = (),
    excluded_relative_files: Sequence[str] = (),
) -> StoreUsage:
    total_bytes = 0
    files = 0
    entries = 0
    excluded_directories = {
        os.path.normcase(str(Path(path).resolve(strict=False)))
        for path in excluded_roots
    }
    excluded_files = {
        os.path.normcase(str((root / relative).resolve(strict=False)))
        for relative in excluded_relative_files
    }
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            iterator = os.scandir(directory)
        except OSError as exc:
            raise LifecycleIntegrityError(f"cannot scan bounded store: {directory}") from exc
        with iterator:
            for entry in iterator:
                entries += 1
                if entries > max_entries:
                    raise LifecycleBoundError(
                        f"store scan exceeds max_scan_entries={max_entries}"
                    )
                try:
                    # Windows DirEntry.stat() reports st_nlink=0 even for a
                    # normal file; os.lstat() supplies the real link count
                    # required for alias rejection.
                    entry_stat = os.lstat(entry.path)
                    windows_attributes = int(
                        getattr(entry_stat, "st_file_attributes", 0)
                    )
                    forbidden_windows_attributes = (
                        0x00000400  # REPARSE_POINT
                        | 0x00001000  # OFFLINE
                        | 0x00040000  # RECALL_ON_OPEN
                        | 0x00100000  # UNPINNED
                        | 0x00400000  # RECALL_ON_DATA_ACCESS
                    )
                    if entry.is_symlink() or (
                        windows_attributes & forbidden_windows_attributes
                    ):
                        raise LifecycleIntegrityError(
                            "symlink/reparse/offline entries are forbidden in "
                            f"lifecycle-controlled stores: {entry.path}"
                        )
                    if entry.is_dir(follow_symlinks=False):
                        directory_path = Path(entry.path)
                        if not excluded_directories or os.path.normcase(
                            str(directory_path.resolve(strict=False))
                        ) not in excluded_directories:
                            stack.append(directory_path)
                    elif entry.is_file(follow_symlinks=False):
                        if int(entry_stat.st_nlink) != 1:
                            raise LifecycleIntegrityError(
                                "hard-linked aliases are forbidden in lifecycle-controlled "
                                f"stores: {entry.path}"
                            )
                        size = int(entry_stat.st_size)
                        if size < 0:
                            raise LifecycleIntegrityError(f"negative file size: {entry.path}")
                        if not excluded_files or os.path.normcase(
                            str(Path(entry.path).resolve(strict=False))
                        ) not in excluded_files:
                            total_bytes += size
                            files += 1
                    else:
                        raise LifecycleIntegrityError(
                            f"non-regular storage entry is forbidden: {entry.path}"
                        )
                except OSError as exc:
                    raise LifecycleIntegrityError(
                        f"store entry changed during bounded scan: {entry.path}"
                    ) from exc
    complete = inflight = 0
    if include_runs:
        runs = root / "runs"
        if runs.is_dir():
            observed_runs = 0
            for run in runs.iterdir():
                observed_runs += 1
                if observed_runs > max_entries:
                    raise LifecycleBoundError(
                        f"run classification exceeds max_scan_entries={max_entries}"
                    )
                if run.is_symlink() or not run.is_dir():
                    continue
                manifest = run / "run_manifest.json"
                is_complete = False
                if manifest.is_file() and manifest.stat().st_size <= MIB:
                    try:
                        raw = _read_bounded_file(
                            manifest,
                            max_bytes=MIB,
                            label="run manifest",
                        )
                        payload = json.loads(raw.decode("utf-8"))
                        is_complete = isinstance(payload, Mapping) and payload.get("complete") is True
                    except (UnicodeError, json.JSONDecodeError):
                        is_complete = False
                if is_complete:
                    complete += 1
                else:
                    inflight += 1
    return StoreUsage(total_bytes, files, complete, inflight)


class _OSFileLock:
    """A one-byte cross-process lock; the file itself is never a lease authority."""

    def __init__(self, path: Path, timeout_seconds: float) -> None:
        self.path = path
        self.timeout_seconds = float(timeout_seconds)
        self._handle: Optional[Any] = None
        self._identity: Optional[tuple[int, int]] = None

    def acquire(self) -> None:
        deadline = time.monotonic() + self.timeout_seconds
        if self.path.is_symlink():
            raise LifecycleIntegrityError("root lifecycle lock cannot be a symlink")
        try:
            handle = self.path.open("a+b")
        except OSError as exc:
            raise LifecycleIntegrityError(
                f"root lifecycle lock cannot be opened safely: {self.path}"
            ) from exc
        try:
            opened = os.fstat(handle.fileno())
            forbidden_windows_attributes = (
                0x00000400 | 0x00001000 | 0x00040000 | 0x00100000 | 0x00400000
            )
            if (
                not stat.S_ISREG(opened.st_mode)
                or int(opened.st_nlink) != 1
                or int(getattr(opened, "st_file_attributes", 0))
                & forbidden_windows_attributes
            ):
                raise LifecycleIntegrityError(
                    "root lifecycle lock must be one regular unaliased local file"
                )
            current = self.path.lstat()
            if (
                not stat.S_ISREG(current.st_mode)
                or int(current.st_nlink) != 1
                or (int(current.st_dev), int(current.st_ino))
                != (int(opened.st_dev), int(opened.st_ino))
            ):
                raise LifecycleIntegrityError(
                    "root lifecycle lock path changed while opening it"
                )
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
                os.fsync(handle.fileno())
            elif handle.tell() != 1:
                raise LifecycleIntegrityError(
                    "root lifecycle lock exceeds its fixed one-byte format"
                )
            while True:
                try:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except (BlockingIOError, OSError):
                    if time.monotonic() >= deadline:
                        raise LifecycleLockTimeout(
                            f"root lifecycle lock deadline exceeded: {self.path}"
                        )
                    time.sleep(min(0.025, max(0.001, deadline - time.monotonic())))
                    continue
                try:
                    after = self.path.lstat()
                except OSError as exc:
                    raise LifecycleIntegrityError(
                        "root lifecycle lock path disappeared during acquisition"
                    ) from exc
                if (
                    not stat.S_ISREG(after.st_mode)
                    or int(after.st_nlink) != 1
                    or (int(after.st_dev), int(after.st_ino))
                    != (int(opened.st_dev), int(opened.st_ino))
                ):
                    raise LifecycleIntegrityError(
                        "root lifecycle lock path was replaced during acquisition"
                    )
                self._handle = handle
                self._identity = (int(opened.st_dev), int(opened.st_ino))
                return
        except BaseException:
            handle.close()
            raise

    def assert_held(self) -> None:
        handle = self._handle
        identity = self._identity
        if handle is None or identity is None:
            raise LifecycleStateError("root lifecycle lock is not held")
        try:
            opened = os.fstat(handle.fileno())
            current = self.path.lstat()
        except OSError as exc:
            raise LifecycleIntegrityError(
                "root lifecycle lock disappeared while leased"
            ) from exc
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or int(opened.st_nlink) != 1
            or int(current.st_nlink) != 1
            or (int(opened.st_dev), int(opened.st_ino)) != identity
            or (int(current.st_dev), int(current.st_ino)) != identity
        ):
            raise LifecycleIntegrityError(
                "root lifecycle lock was replaced or aliased while leased"
            )

    def release(self) -> None:
        handle, self._handle = self._handle, None
        self._identity = None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _sqlite_is_full(exc: BaseException) -> bool:
    return (
        getattr(exc, "sqlite_errorcode", None) == getattr(sqlite3, "SQLITE_FULL", 13)
        or "database or disk is full" in str(exc).lower()
    )


def _safe_identifier(value: str, *, label: str, max_length: int = 128) -> str:
    text = str(value or "").strip()
    if not text or len(text) > max_length:
        raise ValueError(f"{label} must contain 1..{max_length} characters")
    if any(ord(ch) < 32 for ch in text):
        raise ValueError(f"{label} contains control characters")
    return text


def _budget_from_row(row: sqlite3.Row) -> GrowthBudget:
    try:
        payload = json.loads(str(row["budget_json"]))
    except (json.JSONDecodeError, TypeError) as exc:
        raise LifecycleIntegrityError("stored growth budget is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise LifecycleIntegrityError("stored growth budget must be an object")
    canonical = _canonical_bytes(payload)
    if _sha256_bytes(canonical) != str(row["budget_sha256"]):
        raise LifecycleIntegrityError("stored growth budget digest mismatch")
    try:
        return GrowthBudget(**payload)
    except (TypeError, ValueError) as exc:
        raise LifecycleIntegrityError("stored growth budget is invalid") from exc


class NativeLifecycleAuthority:
    """Root-scoped, bounded lifecycle database and operating-system lease."""

    def __init__(
        self,
        *,
        native_root: Path,
        cold_root: Path,
        state_root: Path,
        limits: LifecycleLimits,
        auth_key: bytes,
        broker_highwater_key: bytes,
        operational_domains: Optional[Mapping[str, OperationalDomain]] = None,
        provision: bool = False,
        provisioning_identity: Optional[str] = None,
        allow_unsafe_roots: bool = False,
    ) -> None:
        if allow_unsafe_roots:
            raise LifecycleRootError(
                "allow_unsafe_roots is not supported by the production lifecycle authority"
            )
        if not isinstance(auth_key, (bytes, bytearray)) or len(auth_key) < 32:
            raise ValueError("auth_key must contain at least 32 bytes")
        if (
            not isinstance(broker_highwater_key, (bytes, bytearray))
            or len(broker_highwater_key) < 32
        ):
            raise ValueError(
                "broker_highwater_key must contain at least 32 external-authority bytes"
            )
        if hmac.compare_digest(bytes(auth_key), bytes(broker_highwater_key)):
            raise ValueError(
                "broker high-water authority key must be independent of lifecycle key"
            )
        self._broker_highwater_key = bytes(broker_highwater_key)
        self.broker_highwater_key_id = hashlib.sha256(
            self._broker_highwater_key
        ).hexdigest()
        self.limits = limits
        self._allow_unsafe_roots = False
        self.native_root = _validate_local_root(
            native_root,
            label="native_root",
            must_exist=True,
            allow_unsafe_roots=allow_unsafe_roots,
        )
        self.cold_root = _validate_local_root(
            cold_root,
            label="cold_root",
            must_exist=True,
            allow_unsafe_roots=allow_unsafe_roots,
        )
        if _contains(self.native_root, self.cold_root) or _contains(
            self.cold_root, self.native_root
        ):
            raise LifecycleRootError(
                "native_root and cold_root must be disjoint storage authorities"
            )
        self.coordination_root = _validate_local_root(
            self.native_root.parent,
            label="native coordination root",
            must_exist=True,
            allow_unsafe_roots=False,
        )
        requested_state = _validate_local_root(
            state_root,
            label="state_root",
            must_exist=False,
            allow_unsafe_roots=allow_unsafe_roots,
        )
        for store_name, store in (("native_root", self.native_root), ("cold_root", self.cold_root)):
            if _contains(store, requested_state) or _contains(requested_state, store):
                raise LifecycleRootError(
                    f"state_root must be outside and disjoint from {store_name}"
                )
        if not allow_unsafe_roots and _inside_git_worktree(requested_state):
            raise LifecycleRootError("state_root cannot be located inside a Git worktree/repository")
        if not requested_state.exists():
            if not provision:
                raise LifecycleIntegrityError(
                    "state_root is missing; explicit provision=True is required for first creation"
                )
            requested_state.mkdir(parents=True, exist_ok=False)
        self.state_root = _validate_local_root(
            requested_state,
            label="state_root",
            must_exist=True,
            allow_unsafe_roots=allow_unsafe_roots,
        )
        self.operational_domains: dict[str, OperationalDomain] = {}
        for raw_name, raw_domain in (operational_domains or {}).items():
            name = _safe_identifier(raw_name, label="quota domain", max_length=64)
            domain = (
                raw_domain
                if isinstance(raw_domain, OperationalDomain)
                else OperationalDomain(**raw_domain)
            )
            root = _validate_local_root(
                domain.root,
                label=f"operational domain {name}",
                must_exist=True,
                allow_unsafe_roots=allow_unsafe_roots,
            )
            canonical = OperationalDomain(
                root=root,
                max_bytes=domain.max_bytes,
                max_files=domain.max_files,
                max_active_reservations=domain.max_active_reservations,
            )
            for store_name, store in (
                ("native_root", self.native_root),
                ("cold_root", self.cold_root),
                ("state_root", self.state_root),
            ):
                if _contains(store, root) or _contains(root, store):
                    raise LifecycleRootError(
                        f"operational domain {name} must be disjoint from {store_name}"
                    )
            for other_name, other in self.operational_domains.items():
                if _contains(other.root, root) or _contains(root, other.root):
                    raise LifecycleRootError(
                        f"operational domains {other_name} and {name} overlap"
                    )
            self.operational_domains[name] = canonical
        self._root_identities: dict[str, Mapping[str, Any]] = {
            "native_root": _filesystem_identity(self.native_root),
            "cold_root": _filesystem_identity(self.cold_root),
            "state_root": _filesystem_identity(self.state_root),
            "coordination_root": _filesystem_identity(self.coordination_root),
            **{
                f"operational:{name}": _filesystem_identity(domain.root)
                for name, domain in self.operational_domains.items()
            },
        }
        canonical_root = os.path.normcase(str(self.native_root))
        self.root_key = hashlib.sha256(canonical_root.encode("utf-8")).hexdigest()
        authority_contract = {
            "root_key": self.root_key,
            "native_root": canonical_root,
            "cold_root": os.path.normcase(str(self.cold_root)),
            "state_root": os.path.normcase(str(self.state_root)),
            "operational_domains": {
                name: {
                    "root": os.path.normcase(str(domain.root)),
                    "max_bytes": domain.max_bytes,
                    "max_files": domain.max_files,
                    "max_active_reservations": domain.max_active_reservations,
                    "volume_key": _volume_key(domain.root),
                }
                for name, domain in sorted(self.operational_domains.items())
            },
            "root_identities": self._root_identities,
            "broker_highwater_key_id": self.broker_highwater_key_id,
        }
        self.authority_id = _sha256_bytes(_canonical_bytes(authority_contract))
        self.institution_cap_bytes = (
            self.limits.max_hot_bytes
            + self.limits.max_cold_bytes
            + self.limits.max_db_bytes
            + sum(domain.max_bytes for domain in self.operational_domains.values())
        )
        if self.institution_cap_bytes > SQLITE_SIGNED_COUNTER_MAX:
            raise LifecycleBoundError(
                "aggregate institution byte cap exceeds SQLite's signed counter bound"
            )
        self._auth_key = hmac.new(
            bytes(auth_key),
            ("fxpm-native-lifecycle:" + self.root_key).encode("ascii"),
            hashlib.sha256,
        ).digest()
        self.auth_key_id = hashlib.sha256(bytes(auth_key)).hexdigest()
        if provisioning_identity is None:
            raise ValueError("provisioning_identity is required for creation and reopen")
        self.provisioning_identity = _safe_identifier(
            provisioning_identity, label="provisioning_identity", max_length=256
        )
        self.provisioning_binding = hmac.new(
            self._auth_key,
            ("provision:" + self.provisioning_identity + ":" + self.authority_id).encode(
                "utf-8"
            ),
            hashlib.sha256,
        ).hexdigest()
        stem = f"native-lifecycle-{self.root_key[:24]}"
        self.db_path = self.state_root / f"{stem}.sqlite3"
        coordination_stem = f"native-lifecycle-{self.root_key}"
        self.lock_path = self.coordination_root / f".{coordination_stem}.lock"
        self.fence_path = self.coordination_root / f".{coordination_stem}.fence.json"
        init_lock = _OSFileLock(self.lock_path, self.limits.lock_deadline_seconds)
        init_lock.acquire()
        try:
            db_object_exists = self.db_path.exists() or self.db_path.is_symlink()
            fence_object_exists = (
                self.fence_path.exists() or self.fence_path.is_symlink()
            )
            if provision and (db_object_exists or fence_object_exists):
                raise LifecycleIntegrityError(
                    "lifecycle DB/fence already exists; provision=True cannot reopen "
                    "or overwrite an authority already bound to this native root"
                )
            if not provision and (
                not self.db_path.is_file() or self.db_path.is_symlink()
            ):
                raise LifecycleIntegrityError(
                    "lifecycle DB is missing or unsafe; refusing silent recreation/replay"
                )
            if not provision and (
                not self.fence_path.is_file() or self.fence_path.is_symlink()
            ):
                raise LifecycleIntegrityError(
                    "lifecycle fence anchor is missing or unsafe; refusing silent "
                    "recreation/replay"
                )
            if not provision:
                _filesystem_identity(self.db_path)
            if provision:
                # Bind the canonical native root before creating caller-selected
                # state.  A crash can therefore leave a recoverable fail-closed
                # fence, never an unbound DB that permits an alternate state root
                # to become a second authority.
                self._write_fence_anchor(committed_token=0, pending_token=None)
            connection = self._connect(initialize=provision)
            try:
                self._verify_database_identity(connection, deep=True)
                # Creation/reopen performs the expensive archive-row audit once.
                # Runtime leases verify only the authenticated fixed-size head;
                # operators repeat the row audit explicitly on their cadence.
                self._load_archive_head(connection, verify_rows=True)
                if not provision:
                    self._reconcile_fence_anchor(connection)
            finally:
                connection.close()
        finally:
            init_lock.release()
        self._db_identity = _filesystem_identity(self.db_path)

    def _assert_roots_unchanged(self) -> None:
        current = {
            "native_root": _filesystem_identity(self.native_root),
            "cold_root": _filesystem_identity(self.cold_root),
            "state_root": _filesystem_identity(self.state_root),
            "coordination_root": _filesystem_identity(self.coordination_root),
            **{
                f"operational:{name}": _filesystem_identity(domain.root)
                for name, domain in self.operational_domains.items()
            },
        }
        if current != self._root_identities:
            raise LifecycleIntegrityError(
                "a lifecycle-controlled root was replaced after authority binding"
            )
        if hasattr(self, "_db_identity") and _filesystem_identity(self.db_path) != self._db_identity:
            raise LifecycleIntegrityError(
                "the lifecycle DB file was replaced after authority binding"
            )

    def _fence_unsigned(
        self,
        *,
        committed_token: int,
        pending_token: Optional[int],
        broker_ledger_version: int,
        broker_ledger_claim_count: int,
        broker_ledger_sha256: str,
        pending_broker_ledger_version: Optional[int],
        pending_broker_ledger_claim_count: Optional[int],
        pending_broker_ledger_sha256: Optional[str],
    ) -> Mapping[str, Any]:
        committed = _nonnegative("committed fencing token", committed_token)
        pending = None if pending_token is None else _nonnegative(
            "pending fencing token", pending_token
        )
        if pending is not None and pending != committed + 1:
            raise LifecycleIntegrityError("fence pending token must be committed+1")
        ledger_version = _nonnegative(
            "broker ledger version", broker_ledger_version
        )
        ledger_count = _nonnegative(
            "broker ledger claim count", broker_ledger_claim_count
        )
        if not _is_sha256(broker_ledger_sha256):
            raise LifecycleIntegrityError("broker ledger fence digest is invalid")
        pending_values = (
            pending_broker_ledger_version,
            pending_broker_ledger_claim_count,
            pending_broker_ledger_sha256,
        )
        if any(value is not None for value in pending_values) and not all(
            value is not None for value in pending_values
        ):
            raise LifecycleIntegrityError(
                "pending broker ledger fence fields must be all present or all absent"
            )
        if pending_broker_ledger_version is not None:
            pending_ledger_version = _nonnegative(
                "pending broker ledger version", pending_broker_ledger_version
            )
            pending_ledger_count = _nonnegative(
                "pending broker ledger claim count",
                pending_broker_ledger_claim_count,
            )
            if pending_ledger_version != ledger_version + 1:
                raise LifecycleIntegrityError(
                    "pending broker ledger version must be committed+1"
                )
            if pending_ledger_count != ledger_count + 1:
                raise LifecycleIntegrityError(
                    "one broker claim must advance the logical count exactly once"
                )
            if not _is_sha256(pending_broker_ledger_sha256):
                raise LifecycleIntegrityError(
                    "pending broker ledger fence digest is invalid"
                )
        else:
            pending_ledger_version = None
            pending_ledger_count = None
        return {
            "schema_version": FENCE_ANCHOR_SCHEMA,
            "authority_id": self.authority_id,
            "root_key": self.root_key,
            "provisioning_binding": self.provisioning_binding,
            "committed_token": committed,
            "pending_token": pending,
            "broker_ledger_version": ledger_version,
            "broker_ledger_claim_count": ledger_count,
            "broker_ledger_sha256": broker_ledger_sha256,
            "pending_broker_ledger_version": pending_ledger_version,
            "pending_broker_ledger_claim_count": pending_ledger_count,
            "pending_broker_ledger_sha256": pending_broker_ledger_sha256,
        }

    def _write_fence_anchor(
        self,
        *,
        committed_token: int,
        pending_token: Optional[int],
        broker_ledger_version: Optional[int] = None,
        broker_ledger_claim_count: Optional[int] = None,
        broker_ledger_sha256: Optional[str] = None,
        pending_broker_ledger_version: Optional[int] = None,
        pending_broker_ledger_claim_count: Optional[int] = None,
        pending_broker_ledger_sha256: Optional[str] = None,
    ) -> None:
        if self.fence_path.exists() or self.fence_path.is_symlink():
            self._read_fence_anchor()
        if broker_ledger_version is None:
            if self.fence_path.is_file():
                current = self._read_fence_anchor()
                broker_ledger_version = int(current["broker_ledger_version"])
                broker_ledger_claim_count = int(
                    current["broker_ledger_claim_count"]
                )
                broker_ledger_sha256 = str(current["broker_ledger_sha256"])
                if pending_broker_ledger_version is None:
                    pending_broker_ledger_version = current[
                        "pending_broker_ledger_version"
                    ]
                    pending_broker_ledger_claim_count = current[
                        "pending_broker_ledger_claim_count"
                    ]
                    pending_broker_ledger_sha256 = current[
                        "pending_broker_ledger_sha256"
                    ]
            else:
                broker_ledger_version = 0
                broker_ledger_claim_count = 0
                broker_ledger_sha256 = GENESIS_SHA256
        unsigned = self._fence_unsigned(
            committed_token=committed_token,
            pending_token=pending_token,
            broker_ledger_version=broker_ledger_version,
            broker_ledger_claim_count=broker_ledger_claim_count,
            broker_ledger_sha256=str(broker_ledger_sha256),
            pending_broker_ledger_version=pending_broker_ledger_version,
            pending_broker_ledger_claim_count=pending_broker_ledger_claim_count,
            pending_broker_ledger_sha256=pending_broker_ledger_sha256,
        )
        envelope = dict(unsigned)
        envelope["record_hmac"] = self._sign(unsigned)
        payload = _canonical_bytes(envelope)
        if len(payload) > 4096:
            raise LifecycleBoundError("fence anchor exceeds its fixed byte cap")
        if self.fence_path.exists() and self.fence_path.is_symlink():
            raise LifecycleIntegrityError("fence anchor cannot be a symlink")
        staging = self.fence_path.parent / f".{self.fence_path.name}.{uuid.uuid4().hex}.tmp"
        try:
            with staging.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(staging, self.fence_path)
            observed = self._read_fence_anchor()
            if observed != unsigned:
                raise LifecycleIntegrityError("fence anchor verify-after-write mismatch")
            if os.name != "nt":
                directory_fd = os.open(self.fence_path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                staging.unlink()
            except FileNotFoundError:
                pass

    def _read_fence_anchor(self) -> Mapping[str, Any]:
        try:
            raw = _read_bounded_file(
                self.fence_path,
                max_bytes=4096,
                label="lifecycle fence anchor",
            )
            envelope = json.loads(raw.decode("utf-8"))
        except (LifecycleIntegrityError, UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleIntegrityError("lifecycle fence anchor is unreadable") from exc
        if not isinstance(envelope, Mapping):
            raise LifecycleIntegrityError("lifecycle fence anchor must be an object")
        unsigned = dict(envelope)
        signature = str(unsigned.pop("record_hmac", ""))
        expected_keys = {
            "schema_version",
            "authority_id",
            "root_key",
            "provisioning_binding",
            "committed_token",
            "pending_token",
            "broker_ledger_version",
            "broker_ledger_claim_count",
            "broker_ledger_sha256",
            "pending_broker_ledger_version",
            "pending_broker_ledger_claim_count",
            "pending_broker_ledger_sha256",
        }
        if set(unsigned) != expected_keys:
            raise LifecycleIntegrityError("lifecycle fence anchor fields are not exact")
        if not hmac.compare_digest(signature, self._sign(unsigned)):
            raise LifecycleIntegrityError("lifecycle fence anchor HMAC mismatch")
        expected = self._fence_unsigned(
            committed_token=int(unsigned["committed_token"]),
            pending_token=(
                None
                if unsigned["pending_token"] is None
                else int(unsigned["pending_token"])
            ),
            broker_ledger_version=int(unsigned["broker_ledger_version"]),
            broker_ledger_claim_count=int(unsigned["broker_ledger_claim_count"]),
            broker_ledger_sha256=str(unsigned["broker_ledger_sha256"]),
            pending_broker_ledger_version=(
                None
                if unsigned["pending_broker_ledger_version"] is None
                else int(unsigned["pending_broker_ledger_version"])
            ),
            pending_broker_ledger_claim_count=(
                None
                if unsigned["pending_broker_ledger_claim_count"] is None
                else int(unsigned["pending_broker_ledger_claim_count"])
            ),
            pending_broker_ledger_sha256=(
                None
                if unsigned["pending_broker_ledger_sha256"] is None
                else str(unsigned["pending_broker_ledger_sha256"])
            ),
        )
        if unsigned != expected:
            raise LifecycleIntegrityError("lifecycle fence anchor authority mismatch")
        return expected

    def _verify_complete_broker_ledger(
        self, connection: sqlite3.Connection
    ) -> Mapping[str, Any]:
        """Verify the fixed authenticated hot-path state without scanning claims."""

        state = self._load_broker_ledger_state(connection)
        return {
            "version": int(state["ledger_version"]),
            "claim_count": int(state["total_claim_count"]),
            "sha256": str(state["accumulator_sha256"]),
        }

    def _check_fence_coherence(self, connection: sqlite3.Connection) -> Mapping[str, Any]:
        anchor = self._read_fence_anchor()
        row = connection.execute(
            "SELECT fencing_token FROM lease WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("lifecycle lease row is missing")
        database_token = int(row[0])
        committed = int(anchor["committed_token"])
        pending = anchor["pending_token"]
        allowed = {committed} if pending is None else {committed, int(pending)}
        if database_token not in allowed:
            raise LifecycleRecoveryRequired(
                "lifecycle DB/fence token mismatch indicates rollback or ambiguous commit"
            )
        ledger = self._verify_complete_broker_ledger(connection)
        committed_ledger = (
            int(anchor["broker_ledger_version"]),
            int(anchor["broker_ledger_claim_count"]),
            str(anchor["broker_ledger_sha256"]),
        )
        pending_ledger = (
            None
            if anchor["pending_broker_ledger_version"] is None
            else (
                int(anchor["pending_broker_ledger_version"]),
                int(anchor["pending_broker_ledger_claim_count"]),
                str(anchor["pending_broker_ledger_sha256"]),
            )
        )
        observed_ledger = (
            int(ledger["version"]),
            int(ledger["claim_count"]),
            str(ledger["sha256"]),
        )
        allowed_ledgers = {committed_ledger}
        if pending_ledger is not None:
            allowed_ledgers.add(pending_ledger)
        if observed_ledger not in allowed_ledgers:
            raise LifecycleRecoveryRequired(
                "broker ledger/external fence mismatch indicates deletion, rollback, "
                "or ambiguous commit"
            )
        return anchor

    def _reconcile_fence_anchor(self, connection: sqlite3.Connection) -> int:
        anchor = self._check_fence_coherence(connection)
        database_token = int(
            connection.execute(
                "SELECT fencing_token FROM lease WHERE singleton=1"
            ).fetchone()[0]
        )
        committed = int(anchor["committed_token"])
        pending = anchor["pending_token"]
        if database_token == committed:
            selected_token = committed
        elif pending is not None and database_token == int(pending):
            selected_token = int(pending)
        else:
            raise LifecycleRecoveryRequired(
                "lifecycle pending fence cannot be reconciled"
            )
        observed = self._verify_complete_broker_ledger(connection)
        committed_ledger = (
            int(anchor["broker_ledger_version"]),
            int(anchor["broker_ledger_claim_count"]),
            str(anchor["broker_ledger_sha256"]),
        )
        pending_ledger = (
            None
            if anchor["pending_broker_ledger_version"] is None
            else (
                int(anchor["pending_broker_ledger_version"]),
                int(anchor["pending_broker_ledger_claim_count"]),
                str(anchor["pending_broker_ledger_sha256"]),
            )
        )
        observed_ledger = (
            int(observed["version"]),
            int(observed["claim_count"]),
            str(observed["sha256"]),
        )
        if observed_ledger == committed_ledger:
            selected_ledger = committed_ledger
        elif pending_ledger is not None and observed_ledger == pending_ledger:
            selected_ledger = pending_ledger
        else:  # pragma: no cover - guarded by _check_fence_coherence
            raise LifecycleRecoveryRequired(
                "broker ledger pending fence cannot be reconciled"
            )
        if pending is not None or pending_ledger is not None:
            self._write_fence_anchor(
                committed_token=selected_token,
                pending_token=None,
                broker_ledger_version=selected_ledger[0],
                broker_ledger_claim_count=selected_ledger[1],
                broker_ledger_sha256=selected_ledger[2],
                pending_broker_ledger_version=None,
                pending_broker_ledger_claim_count=None,
                pending_broker_ledger_sha256=None,
            )
        return selected_token

    def _connect(self, *, initialize: bool = False) -> sqlite3.Connection:
        if not initialize and hasattr(self, "_root_identities"):
            self._assert_roots_unchanged()
        if not initialize and not self.db_path.is_file():
            raise LifecycleIntegrityError(
                "lifecycle DB disappeared; refusing silent recreation"
            )
        connection = sqlite3.connect(
            self.db_path,
            timeout=0.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=0")
            connection.execute("PRAGMA foreign_keys=ON")
            if not self.db_path.exists() or self.db_path.stat().st_size == 0:
                connection.execute(f"PRAGMA page_size={self.limits.page_size}")
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            if page_size != self.limits.page_size:
                raise LifecycleIntegrityError(
                    f"lifecycle DB page_size drift: {page_size} != {self.limits.page_size}"
                )
            mode = str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
            if mode != "delete":
                raise LifecycleIntegrityError(f"lifecycle DB journal mode is not DELETE: {mode}")
            connection.execute("PRAGMA synchronous=FULL")
            if int(connection.execute("PRAGMA synchronous").fetchone()[0]) != 2:
                raise LifecycleIntegrityError("lifecycle DB synchronous mode is not FULL")
            if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise LifecycleIntegrityError("lifecycle DB foreign_keys are not enabled")
            max_pages = self.limits.max_db_bytes // self.limits.page_size
            actual_max_pages = int(
                connection.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if actual_max_pages != max_pages:
                raise LifecycleIntegrityError("SQLite refused the fixed max_page_count")
            connection.execute(
                f"PRAGMA journal_size_limit={self.limits.journal_size_limit}"
            )
            if initialize:
                self._initialize_schema(connection)
            self._verify_database_identity(connection)
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            if page_count * page_size > self.limits.max_db_bytes:
                raise LifecycleBoundError("lifecycle DB already exceeds its fixed page cap")
            if not initialize:
                self._check_fence_coherence(connection)
            return connection
        except BaseException:
            connection.close()
            raise

    def _initialize_schema(self, connection: sqlite3.Connection) -> None:
        statements = (
            """CREATE TABLE IF NOT EXISTS meta (
                   key TEXT PRIMARY KEY,
                   value TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS lease (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   owner TEXT,
                   pid INTEGER,
                   acquired_ns INTEGER,
                   heartbeat_ns INTEGER,
                   expires_ns INTEGER
               )""",
            """CREATE TABLE IF NOT EXISTS operations (
                   operation_id TEXT PRIMARY KEY,
                   kind TEXT NOT NULL,
                   state TEXT NOT NULL,
                   owner TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   created_ns INTEGER NOT NULL,
                   updated_ns INTEGER NOT NULL,
                   payload_json TEXT NOT NULL,
                   payload_sha256 TEXT NOT NULL,
                   payload_bytes INTEGER NOT NULL,
                   file_count INTEGER NOT NULL,
                   budget_json TEXT NOT NULL,
                   budget_sha256 TEXT NOT NULL,
                   materialized_native_peak_bytes INTEGER NOT NULL DEFAULT 0,
                   materialized_native_final_bytes INTEGER NOT NULL DEFAULT 0,
                   materialized_cold_peak_bytes INTEGER NOT NULL DEFAULT 0,
                   materialized_cold_final_bytes INTEGER NOT NULL DEFAULT 0,
                   materialized_native_files INTEGER NOT NULL DEFAULT 0,
                   materialized_cold_files INTEGER NOT NULL DEFAULT 0,
                   materialized_operational_json TEXT NOT NULL DEFAULT '{}',
                   recovery_from TEXT,
                   pointer_receipt_json TEXT,
                   pointer_receipt_hmac TEXT,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS pins (
                   pin_id TEXT PRIMARY KEY,
                   created_ns INTEGER NOT NULL,
                   payload_json TEXT NOT NULL,
                   payload_sha256 TEXT NOT NULL,
                   payload_bytes INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS operation_ledger_state (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   total_count INTEGER NOT NULL CHECK(total_count >= 0),
                   active_count INTEGER NOT NULL CHECK(active_count >= 0),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS pointer_heads (
                   pointer_relative_path TEXT PRIMARY KEY,
                   operation_id TEXT NOT NULL UNIQUE,
                   receipt_json TEXT NOT NULL,
                   receipt_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                   published_ns INTEGER NOT NULL CHECK(published_ns > 0),
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS pointer_ledger_state (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   head_count INTEGER NOT NULL CHECK(head_count >= 0),
                   mutation_count INTEGER NOT NULL CHECK(mutation_count >= 0),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS runtime_reservations (
                   reservation_id TEXT PRIMARY KEY,
                   domain TEXT NOT NULL,
                   operational_root TEXT NOT NULL,
                   root_device TEXT NOT NULL,
                   root_inode TEXT NOT NULL,
                   stream_identity_sha256 TEXT NOT NULL,
                   reserved_peak_bytes INTEGER NOT NULL CHECK(reserved_peak_bytes > 0),
                   institution_cap_bytes INTEGER NOT NULL CHECK(institution_cap_bytes > 0),
                   admitted_streams_cap INTEGER NOT NULL CHECK(admitted_streams_cap > 0),
                   fencing_token INTEGER NOT NULL,
                   status TEXT NOT NULL,
                   created_ns INTEGER NOT NULL,
                   updated_ns INTEGER NOT NULL,
                   receipt_mac TEXT NOT NULL,
                   UNIQUE(operational_root),
                   UNIQUE(stream_identity_sha256)
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS runtime_witnesses (
                   reservation_id TEXT PRIMARY KEY
                       REFERENCES runtime_reservations(reservation_id) ON DELETE RESTRICT,
                   witness_version INTEGER NOT NULL CHECK(witness_version >= 0),
                   writer_epoch INTEGER NOT NULL CHECK(writer_epoch >= 0),
                   reducer_contract_sha256 TEXT NOT NULL,
                   max_bar_sequence INTEGER NOT NULL CHECK(max_bar_sequence >= 0),
                   bar_chain_sha256 TEXT NOT NULL,
                   checkpoint_generation INTEGER NOT NULL CHECK(checkpoint_generation >= 0),
                   checkpoint_sha256 TEXT NOT NULL,
                   max_order_operation_sequence INTEGER NOT NULL
                       CHECK(max_order_operation_sequence >= 0),
                   max_order_event_sequence INTEGER NOT NULL CHECK(max_order_event_sequence >= 0),
                   order_chain_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS runtime_account_bindings (
                   reservation_id TEXT PRIMARY KEY
                       REFERENCES runtime_reservations(reservation_id) ON DELETE RESTRICT,
                   account_scope_sha256 TEXT NOT NULL,
                   stream_identity_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                   created_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS broker_account_heads (
                   account_scope_sha256 TEXT PRIMARY KEY,
                   claim_count INTEGER NOT NULL CHECK(claim_count >= 0),
                   last_claim_sequence INTEGER NOT NULL CHECK(last_claim_sequence >= 0),
                   last_claim_sha256 TEXT NOT NULL,
                   external_highwater_sequence INTEGER NOT NULL CHECK(external_highwater_sequence >= 0),
                   external_receipt_sha256 TEXT NOT NULL,
                   retired_execution_floor INTEGER NOT NULL CHECK(retired_execution_floor >= 0),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS runtime_claim_heads (
                   reservation_id TEXT PRIMARY KEY
                       REFERENCES runtime_reservations(reservation_id) ON DELETE RESTRICT,
                   account_scope_sha256 TEXT NOT NULL,
                   stream_identity_sha256 TEXT NOT NULL,
                   max_operation_sequence INTEGER NOT NULL CHECK(max_operation_sequence >= 0),
                   retired_operation_floor INTEGER NOT NULL CHECK(retired_operation_floor >= 0),
                   fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS broker_ledger_state (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   ledger_version INTEGER NOT NULL CHECK(ledger_version >= 0),
                   total_claim_count INTEGER NOT NULL CHECK(total_claim_count >= 0),
                   live_claim_count INTEGER NOT NULL CHECK(live_claim_count >= 0),
                   account_count INTEGER NOT NULL CHECK(account_count >= 0),
                   accumulator_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS broker_execution_claims (
                   claim_id TEXT PRIMARY KEY,
                   account_scope_sha256 TEXT NOT NULL
                       REFERENCES broker_account_heads(account_scope_sha256)
                       ON DELETE RESTRICT,
                   broker_execution_id TEXT NOT NULL,
                   broker_execution_sequence INTEGER NOT NULL
                       CHECK(broker_execution_sequence > 0),
                   claim_sequence INTEGER NOT NULL CHECK(claim_sequence > 0),
                   stream_identity_sha256 TEXT NOT NULL,
                   operation_id TEXT NOT NULL,
                   operation_sequence INTEGER NOT NULL CHECK(operation_sequence > 0),
                   payload_sha256 TEXT NOT NULL,
                   previous_claim_sha256 TEXT NOT NULL,
                   claim_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                   created_ns INTEGER NOT NULL,
                   receipt_mac TEXT NOT NULL,
                   record_hmac TEXT NOT NULL,
                   UNIQUE(account_scope_sha256,broker_execution_id),
                   UNIQUE(account_scope_sha256,broker_execution_sequence),
                   UNIQUE(account_scope_sha256,claim_sequence),
                   UNIQUE(account_scope_sha256,stream_identity_sha256,operation_id),
                   UNIQUE(account_scope_sha256,stream_identity_sha256,operation_sequence)
               ) WITHOUT ROWID""",
            "CREATE INDEX IF NOT EXISTS operations_state_idx ON operations(state)",
            "CREATE INDEX IF NOT EXISTS runtime_reservations_status_idx "
            "ON runtime_reservations(domain,status)",
            "CREATE INDEX IF NOT EXISTS broker_execution_account_idx "
            "ON broker_execution_claims(account_scope_sha256,claim_sequence)",
            """CREATE TRIGGER IF NOT EXISTS operation_insert_guard
                   AFTER INSERT ON operations
                   BEGIN
                     UPDATE operation_ledger_state
                        SET total_count=total_count+1,
                            active_count=active_count +
                              CASE WHEN NEW.state NOT IN ('COMMITTED','ABORTED')
                                   THEN 1 ELSE 0 END
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS operation_delete_guard
                   AFTER DELETE ON operations
                   BEGIN
                     UPDATE operation_ledger_state
                        SET total_count=total_count-1,
                            active_count=active_count -
                              CASE WHEN OLD.state NOT IN ('COMMITTED','ABORTED')
                                   THEN 1 ELSE 0 END
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS operation_state_guard
                   AFTER UPDATE OF state ON operations
                   BEGIN
                     UPDATE operation_ledger_state
                        SET active_count=active_count +
                              CASE WHEN NEW.state NOT IN ('COMMITTED','ABORTED')
                                   THEN 1 ELSE 0 END -
                              CASE WHEN OLD.state NOT IN ('COMMITTED','ABORTED')
                                   THEN 1 ELSE 0 END
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS pointer_head_insert_guard
                   AFTER INSERT ON pointer_heads
                   BEGIN
                     UPDATE pointer_ledger_state
                        SET head_count=head_count+1,
                            mutation_count=mutation_count+1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS pointer_head_update_guard
                   AFTER UPDATE ON pointer_heads
                   BEGIN
                     UPDATE pointer_ledger_state
                        SET mutation_count=mutation_count+1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS pointer_head_delete_guard
                   AFTER DELETE ON pointer_heads
                   BEGIN
                     UPDATE pointer_ledger_state
                        SET head_count=head_count-1,
                            mutation_count=mutation_count+1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS broker_claim_insert_guard
                   AFTER INSERT ON broker_execution_claims
                   BEGIN
                     UPDATE broker_ledger_state
                        SET live_claim_count=live_claim_count+1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS broker_claim_delete_guard
                   AFTER DELETE ON broker_execution_claims
                   BEGIN
                     UPDATE broker_ledger_state
                        SET live_claim_count=live_claim_count-1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS broker_account_insert_guard
                   AFTER INSERT ON broker_account_heads
                   BEGIN
                     UPDATE broker_ledger_state
                        SET account_count=account_count+1
                      WHERE singleton=1;
                   END""",
            """CREATE TRIGGER IF NOT EXISTS broker_account_delete_guard
                   AFTER DELETE ON broker_account_heads
                   BEGIN
                     UPDATE broker_ledger_state
                        SET account_count=account_count-1
                      WHERE singleton=1;
                   END""",
            """CREATE TABLE IF NOT EXISTS lifecycle_archive_head (
                   singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                   operation_count INTEGER NOT NULL CHECK(operation_count >= 0),
                   operation_sha256 TEXT NOT NULL,
                   pin_count INTEGER NOT NULL CHECK(pin_count >= 0),
                   pin_sha256 TEXT NOT NULL,
                   runtime_count INTEGER NOT NULL CHECK(runtime_count >= 0),
                   runtime_sha256 TEXT NOT NULL,
                   broker_count INTEGER NOT NULL CHECK(broker_count >= 0),
                   broker_sha256 TEXT NOT NULL,
                   fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                   updated_ns INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               )""",
            """CREATE TABLE IF NOT EXISTS operation_tombstones (
                   operation_id TEXT PRIMARY KEY,
                   source_record_sha256 TEXT NOT NULL,
                   tombstone_sha256 TEXT NOT NULL,
                   retired_ns INTEGER NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS pin_tombstones (
                   pin_id TEXT PRIMARY KEY,
                   source_record_sha256 TEXT NOT NULL,
                   tombstone_sha256 TEXT NOT NULL,
                   retired_ns INTEGER NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
            """CREATE TABLE IF NOT EXISTS runtime_tombstones (
                   reservation_id TEXT PRIMARY KEY,
                   operational_root TEXT NOT NULL UNIQUE,
                   stream_identity_sha256 TEXT NOT NULL UNIQUE,
                   account_scope_sha256 TEXT,
                   reservation_sha256 TEXT NOT NULL,
                   witness_sha256 TEXT NOT NULL,
                   runtime_claim_head_sha256 TEXT NOT NULL,
                   max_operation_sequence INTEGER NOT NULL CHECK(max_operation_sequence >= 0),
                   retired_operation_floor INTEGER NOT NULL CHECK(retired_operation_floor >= 0),
                   tombstone_sha256 TEXT NOT NULL,
                   retired_ns INTEGER NOT NULL,
                   fencing_token INTEGER NOT NULL,
                   record_hmac TEXT NOT NULL
               ) WITHOUT ROWID""",
        )
        now = time.time_ns()

        def create() -> None:
            connection.execute(f"PRAGMA application_id={SQLITE_APPLICATION_ID}")
            connection.execute(f"PRAGMA user_version={SQLITE_USER_VERSION}")
            for statement in statements:
                connection.execute(statement)
            expected_meta = {
                "schema": DATABASE_SCHEMA,
                "root_key": self.root_key,
                "native_root": os.path.normcase(str(self.native_root)),
                "cold_root": os.path.normcase(str(self.cold_root)),
                "auth_key_id": self.auth_key_id,
                "broker_highwater_key_id": self.broker_highwater_key_id,
                "page_size": str(self.limits.page_size),
                "max_db_bytes": str(self.limits.max_db_bytes),
                "authority_id": self.authority_id,
                "provisioning_binding": self.provisioning_binding,
            }
            for key, value in expected_meta.items():
                row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
                if row is None:
                    connection.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, value))
                elif str(row[0]) != value:
                    raise LifecycleIntegrityError(f"lifecycle DB metadata drift for {key}")
            connection.execute(
                "INSERT OR IGNORE INTO lease(singleton,fencing_token,owner,pid,acquired_ns,heartbeat_ns,expires_ns) "
                "VALUES(1,0,NULL,NULL,?,?,?)",
                (now, now, now),
            )
            operation_state = {
                "total_count": 0,
                "active_count": 0,
                "fencing_token": 0,
                "updated_ns": now,
            }
            connection.execute(
                "INSERT OR IGNORE INTO operation_ledger_state VALUES(1,?,?,?,?,?)",
                (
                    operation_state["total_count"],
                    operation_state["active_count"],
                    operation_state["fencing_token"],
                    operation_state["updated_ns"],
                    self._sign(
                        self._operation_ledger_state_fields(operation_state)
                    ),
                ),
            )
            pointer_state = {
                "head_count": 0,
                "mutation_count": 0,
                "fencing_token": 0,
                "updated_ns": now,
            }
            connection.execute(
                "INSERT OR IGNORE INTO pointer_ledger_state VALUES(1,?,?,?,?,?)",
                (
                    pointer_state["head_count"],
                    pointer_state["mutation_count"],
                    pointer_state["fencing_token"],
                    pointer_state["updated_ns"],
                    self._sign(self._pointer_ledger_state_fields(pointer_state)),
                ),
            )
            archive_head = {
                "operation_count": 0,
                "operation_sha256": GENESIS_SHA256,
                "pin_count": 0,
                "pin_sha256": GENESIS_SHA256,
                "runtime_count": 0,
                "runtime_sha256": GENESIS_SHA256,
                "broker_count": 0,
                "broker_sha256": GENESIS_SHA256,
                "fencing_token": 0,
                "updated_ns": now,
            }
            connection.execute(
                "INSERT OR IGNORE INTO lifecycle_archive_head VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    archive_head["operation_count"],
                    archive_head["operation_sha256"],
                    archive_head["pin_count"],
                    archive_head["pin_sha256"],
                    archive_head["runtime_count"],
                    archive_head["runtime_sha256"],
                    archive_head["broker_count"],
                    archive_head["broker_sha256"],
                    archive_head["fencing_token"],
                    archive_head["updated_ns"],
                    self._sign(self._archive_head_fields(archive_head)),
                ),
            )
            ledger_state = {
                "ledger_version": 0,
                "total_claim_count": 0,
                "live_claim_count": 0,
                "account_count": 0,
                "accumulator_sha256": GENESIS_SHA256,
                "fencing_token": 0,
                "updated_ns": now,
            }
            connection.execute(
                "INSERT OR IGNORE INTO broker_ledger_state VALUES(1,?,?,?,?,?,?,?,?)",
                (
                    ledger_state["ledger_version"],
                    ledger_state["total_claim_count"],
                    ledger_state["live_claim_count"],
                    ledger_state["account_count"],
                    ledger_state["accumulator_sha256"],
                    ledger_state["fencing_token"],
                    ledger_state["updated_ns"],
                    self._sign(self._broker_ledger_state_fields(ledger_state)),
                ),
            )

        self._transaction(connection, create)

    def _verify_database_identity(
        self, connection: sqlite3.Connection, *, deep: bool = False
    ) -> None:
        if int(connection.execute("PRAGMA application_id").fetchone()[0]) != SQLITE_APPLICATION_ID:
            raise LifecycleIntegrityError("lifecycle DB application_id mismatch")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SQLITE_USER_VERSION:
            raise LifecycleIntegrityError("lifecycle DB user_version mismatch")
        if deep:
            quick = connection.execute("PRAGMA quick_check(1)").fetchone()
            if quick is None or str(quick[0]).lower() != "ok":
                raise LifecycleIntegrityError(
                    "lifecycle DB quick integrity check failed"
                )
            foreign = connection.execute("PRAGMA foreign_key_check").fetchone()
            if foreign is not None:
                raise LifecycleIntegrityError(
                    "lifecycle DB foreign-key integrity check failed"
                )
        def normalized_sql(value: Any) -> str:
            return "".join(str(value or "").split()).casefold()

        required_broker_triggers = {
            "broker_claim_insert_guard": (
                "CREATE TRIGGER broker_claim_insert_guard AFTER INSERT ON "
                "broker_execution_claims BEGIN UPDATE broker_ledger_state SET "
                "live_claim_count=live_claim_count+1 WHERE singleton=1; END"
            ),
            "broker_claim_delete_guard": (
                "CREATE TRIGGER broker_claim_delete_guard AFTER DELETE ON "
                "broker_execution_claims BEGIN UPDATE broker_ledger_state SET "
                "live_claim_count=live_claim_count-1 WHERE singleton=1; END"
            ),
            "broker_account_insert_guard": (
                "CREATE TRIGGER broker_account_insert_guard AFTER INSERT ON "
                "broker_account_heads BEGIN UPDATE broker_ledger_state SET "
                "account_count=account_count+1 WHERE singleton=1; END"
            ),
            "broker_account_delete_guard": (
                "CREATE TRIGGER broker_account_delete_guard AFTER DELETE ON "
                "broker_account_heads BEGIN UPDATE broker_ledger_state SET "
                "account_count=account_count-1 WHERE singleton=1; END"
            ),
        }
        observed_broker_triggers = {
            str(row[0]): normalized_sql(row[1])
            for row in connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND "
                "tbl_name IN ('broker_execution_claims','broker_account_heads',"
                "'broker_ledger_state')"
            )
        }
        expected_broker_triggers = {
            name: normalized_sql(sql)
            for name, sql in required_broker_triggers.items()
        }
        if observed_broker_triggers != expected_broker_triggers:
            raise LifecycleIntegrityError(
                "broker ledger guard-trigger schema mismatch"
            )
        required_operation_triggers = {
            "operation_insert_guard": (
                "CREATE TRIGGER operation_insert_guard AFTER INSERT ON operations "
                "BEGIN UPDATE operation_ledger_state SET total_count=total_count+1,"
                "active_count=active_count+CASE WHEN NEW.state NOT IN "
                "('COMMITTED','ABORTED') THEN 1 ELSE 0 END WHERE singleton=1; END"
            ),
            "operation_delete_guard": (
                "CREATE TRIGGER operation_delete_guard AFTER DELETE ON operations "
                "BEGIN UPDATE operation_ledger_state SET total_count=total_count-1,"
                "active_count=active_count-CASE WHEN OLD.state NOT IN "
                "('COMMITTED','ABORTED') THEN 1 ELSE 0 END WHERE singleton=1; END"
            ),
            "operation_state_guard": (
                "CREATE TRIGGER operation_state_guard AFTER UPDATE OF state ON operations "
                "BEGIN UPDATE operation_ledger_state SET active_count=active_count+"
                "CASE WHEN NEW.state NOT IN ('COMMITTED','ABORTED') THEN 1 ELSE 0 END-"
                "CASE WHEN OLD.state NOT IN ('COMMITTED','ABORTED') THEN 1 ELSE 0 END "
                "WHERE singleton=1; END"
            ),
        }
        observed_operation_triggers = {
            str(row[0]): normalized_sql(row[1])
            for row in connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
                "AND tbl_name='operations'"
            )
        }
        expected_operation_triggers = {
            name: normalized_sql(sql)
            for name, sql in required_operation_triggers.items()
        }
        if observed_operation_triggers != expected_operation_triggers:
            raise LifecycleIntegrityError(
                "operation ledger guard-trigger schema mismatch"
            )
        required_pointer_triggers = {
            "pointer_head_insert_guard": (
                "CREATE TRIGGER pointer_head_insert_guard AFTER INSERT ON pointer_heads "
                "BEGIN UPDATE pointer_ledger_state SET head_count=head_count+1,"
                "mutation_count=mutation_count+1 WHERE singleton=1; END"
            ),
            "pointer_head_update_guard": (
                "CREATE TRIGGER pointer_head_update_guard AFTER UPDATE ON pointer_heads "
                "BEGIN UPDATE pointer_ledger_state SET mutation_count=mutation_count+1 "
                "WHERE singleton=1; END"
            ),
            "pointer_head_delete_guard": (
                "CREATE TRIGGER pointer_head_delete_guard AFTER DELETE ON pointer_heads "
                "BEGIN UPDATE pointer_ledger_state SET head_count=head_count-1,"
                "mutation_count=mutation_count+1 WHERE singleton=1; END"
            ),
        }
        observed_pointer_triggers = {
            str(row[0]): normalized_sql(row[1])
            for row in connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
                "AND tbl_name='pointer_heads'"
            )
        }
        expected_pointer_triggers = {
            name: normalized_sql(sql)
            for name, sql in required_pointer_triggers.items()
        }
        if observed_pointer_triggers != expected_pointer_triggers:
            raise LifecycleIntegrityError(
                "pointer ledger guard-trigger schema mismatch"
            )
        expected = {
            "schema": DATABASE_SCHEMA,
            "root_key": self.root_key,
            "native_root": os.path.normcase(str(self.native_root)),
            "cold_root": os.path.normcase(str(self.cold_root)),
            "auth_key_id": self.auth_key_id,
            "broker_highwater_key_id": self.broker_highwater_key_id,
            "page_size": str(self.limits.page_size),
            "max_db_bytes": str(self.limits.max_db_bytes),
            "authority_id": self.authority_id,
            "provisioning_binding": self.provisioning_binding,
        }
        try:
            rows = {
                str(row["key"]): str(row["value"])
                for row in connection.execute("SELECT key,value FROM meta")
            }
        except sqlite3.DatabaseError as exc:
            raise LifecycleIntegrityError("lifecycle DB metadata table is unavailable") from exc
        for key, value in expected.items():
            if rows.get(key) != value:
                raise LifecycleIntegrityError(f"lifecycle DB metadata drift for {key}")
        self._load_operation_ledger_state(connection)
        self._load_pointer_ledger_state(connection)
        self._load_archive_head(connection, verify_rows=False)

    def _transaction(self, connection: sqlite3.Connection, callback: Callable[[], Any]) -> Any:
        deadline = time.monotonic() + self.limits.db_busy_deadline_seconds
        while True:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if _sqlite_is_full(exc):
                    raise LifecycleDatabaseFullError("lifecycle DB page cap reached") from exc
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                if time.monotonic() >= deadline:
                    raise LifecycleBusyError("SQLite BEGIN IMMEDIATE deadline exceeded") from exc
                time.sleep(min(0.025, max(0.001, deadline - time.monotonic())))
                continue
            commit_started = False
            try:
                result = callback()
                commit_started = True
                connection.execute("COMMIT")
            except BaseException as exc:
                in_transaction = bool(connection.in_transaction)
                if in_transaction:
                    try:
                        connection.execute("ROLLBACK")
                    except sqlite3.DatabaseError:
                        pass
                elif commit_started:
                    raise LifecycleRecoveryRequired(
                        "SQLite commit outcome is ambiguous; explicit reopen/reconciliation "
                        "is required before retry"
                    ) from exc
                if _sqlite_is_full(exc):
                    raise LifecycleDatabaseFullError(
                        "lifecycle DB page cap reached; transaction rolled back"
                    ) from exc
                if isinstance(exc, sqlite3.OperationalError):
                    message = str(exc).lower()
                    if "locked" in message or "busy" in message:
                        if time.monotonic() >= deadline:
                            raise LifecycleBusyError(
                                "SQLite transaction deadline exceeded"
                            ) from exc
                        time.sleep(
                            min(0.025, max(0.001, deadline - time.monotonic()))
                        )
                        continue
                raise
            try:
                db_size = self.db_path.stat().st_size
            except OSError as exc:
                raise LifecycleRecoveryRequired(
                    "SQLite committed but post-commit DB verification is unavailable"
                ) from exc
            if db_size > self.limits.max_db_bytes:
                raise LifecycleRecoveryRequired(
                    "SQLite committed beyond its fixed byte cap; explicit recovery is required"
                )
            return result

    def lease(self, owner: str, *, timeout_seconds: Optional[float] = None) -> "LifecycleLease":
        return LifecycleLease(
            authority=self,
            owner=_safe_identifier(owner, label="lease owner"),
            timeout_seconds=(
                self.limits.lock_deadline_seconds
                if timeout_seconds is None
                else float(timeout_seconds)
            ),
        )._acquire()

    def _sign(self, value: Mapping[str, Any]) -> str:
        return hmac.new(self._auth_key, _canonical_bytes(value), hashlib.sha256).hexdigest()

    def _operation_signed_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return the complete mutable operation record covered by its MAC.

        Plain payload/budget digests detect accidental corruption but are not
        an authority: an offline writer can recompute them.  The record MAC
        therefore binds every field which affects recovery, admission,
        materialization, pointer publication, or later tombstoning.
        """

        return {
            "schema_version": OPERATION_RECORD_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "operation_id": str(row["operation_id"]),
            "kind": str(row["kind"]),
            "state": str(row["state"]),
            "owner": str(row["owner"]),
            "fencing_token": int(row["fencing_token"]),
            "created_ns": int(row["created_ns"]),
            "updated_ns": int(row["updated_ns"]),
            "payload_json": str(row["payload_json"]),
            "payload_sha256": str(row["payload_sha256"]),
            "payload_bytes": int(row["payload_bytes"]),
            "file_count": int(row["file_count"]),
            "budget_json": str(row["budget_json"]),
            "budget_sha256": str(row["budget_sha256"]),
            "materialized_native_peak_bytes": int(
                row["materialized_native_peak_bytes"]
            ),
            "materialized_native_final_bytes": int(
                row["materialized_native_final_bytes"]
            ),
            "materialized_cold_peak_bytes": int(
                row["materialized_cold_peak_bytes"]
            ),
            "materialized_cold_final_bytes": int(
                row["materialized_cold_final_bytes"]
            ),
            "materialized_native_files": int(row["materialized_native_files"]),
            "materialized_cold_files": int(row["materialized_cold_files"]),
            "materialized_operational_json": str(
                row["materialized_operational_json"]
            ),
            "recovery_from": (
                None if row["recovery_from"] is None else str(row["recovery_from"])
            ),
            "pointer_receipt_json": (
                None
                if row["pointer_receipt_json"] is None
                else str(row["pointer_receipt_json"])
            ),
            "pointer_receipt_hmac": (
                None
                if row["pointer_receipt_hmac"] is None
                else str(row["pointer_receipt_hmac"])
            ),
        }

    def _validate_operation_row(self, row: Mapping[str, Any]) -> None:
        try:
            fields = self._operation_signed_fields(row)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LifecycleIntegrityError("operation record is malformed") from exc
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("operation record HMAC mismatch")
        if fields["state"] not in STRICT_TRANSITIONS:
            raise LifecycleIntegrityError("operation record state is invalid")
        try:
            OperationKind(fields["kind"])
        except ValueError as exc:
            raise LifecycleIntegrityError("operation record kind is invalid") from exc
        payload_raw = fields["payload_json"].encode("utf-8")
        if (
            len(payload_raw) != fields["payload_bytes"]
            or len(payload_raw) > self.limits.max_payload_bytes
            or _sha256_bytes(payload_raw) != fields["payload_sha256"]
        ):
            raise LifecycleIntegrityError("operation payload binding is invalid")
        try:
            payload = json.loads(payload_raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleIntegrityError("operation payload is unreadable") from exc
        if not isinstance(payload, Mapping):
            raise LifecycleIntegrityError("operation payload must remain an object")
        budget = _budget_from_row(row)
        if budget.kind != fields["kind"]:
            raise LifecycleIntegrityError("operation kind/budget binding is invalid")
        expected_file_count = (
            budget.native_files_delta
            + budget.cold_files_delta
            + sum(item.files_delta for item in budget.operational)
        )
        if fields["file_count"] != expected_file_count:
            raise LifecycleIntegrityError("operation budget/file-count binding is invalid")
        if not 0 <= fields["file_count"] <= self.limits.max_files_per_operation:
            raise LifecycleIntegrityError("operation file count exceeds its bound")
        materialized_raw = fields["materialized_operational_json"].encode("utf-8")
        if len(materialized_raw) > self.limits.max_payload_bytes:
            raise LifecycleIntegrityError(
                "operation materialization vector exceeds its authenticated bound"
            )
        try:
            materialized = json.loads(materialized_raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleIntegrityError(
                "operation materialization vector is unreadable"
            ) from exc
        if not isinstance(materialized, Mapping):
            raise LifecycleIntegrityError(
                "operation materialization vector must remain an object"
            )
        scalars = {
            "native_peak": fields["materialized_native_peak_bytes"],
            "native_final": fields["materialized_native_final_bytes"],
            "cold_peak": fields["materialized_cold_peak_bytes"],
            "cold_final": fields["materialized_cold_final_bytes"],
            "native_files": fields["materialized_native_files"],
            "cold_files": fields["materialized_cold_files"],
        }
        for name, raw in scalars.items():
            scalars[name] = _nonnegative(f"operation materialized {name}", raw)
        if (
            scalars["native_final"] > scalars["native_peak"]
            or scalars["cold_final"] > scalars["cold_peak"]
            or scalars["native_peak"] > budget.native_peak_bytes
            or scalars["native_final"] > budget.native_final_bytes
            or scalars["cold_peak"] > budget.cold_peak_bytes
            or scalars["cold_final"] > budget.cold_final_bytes
            or scalars["native_files"] > budget.native_files_delta
            or scalars["cold_files"] > budget.cold_files_delta
        ):
            raise LifecycleIntegrityError(
                "operation materialization exceeds its authenticated budget"
            )
        operational_bounds = {item.domain: item for item in budget.operational}
        if len(materialized) > len(self.operational_domains):
            raise LifecycleIntegrityError(
                "operation materialization domain vector is unbounded"
            )
        for name, raw in materialized.items():
            if not isinstance(raw, Mapping) or set(raw) != {"peak", "final", "files"}:
                raise LifecycleIntegrityError(
                    "operation materialization domain entry is invalid"
                )
            bound = operational_bounds.get(str(name))
            values = {
                key: _nonnegative(
                    f"operation materialized {name}.{key}", raw[key]
                )
                for key in ("peak", "final", "files")
            }
            if bound is None and any(values.values()):
                raise LifecycleIntegrityError(
                    "operation materialization names an unreserved domain"
                )
            if bound is not None and (
                values["final"] > values["peak"]
                or values["peak"] > bound.peak_bytes
                or values["final"] > bound.final_bytes
                or values["files"] > bound.files_delta
            ):
                raise LifecycleIntegrityError(
                    "operation domain materialization exceeds its budget"
                )
        recovery_from = fields["recovery_from"]
        if fields["state"] == OperationState.RECOVERY_BLOCKED.value:
            if recovery_from not in STRICT_TRANSITIONS or recovery_from in TERMINAL_STATES:
                raise LifecycleIntegrityError(
                    "recovery-blocked operation has no valid source state"
                )
        elif recovery_from is not None:
            raise LifecycleIntegrityError(
                "non-recovery operation carries a recovery source state"
            )
        if (fields["pointer_receipt_json"] is None) != (
            fields["pointer_receipt_hmac"] is None
        ):
            raise LifecycleIntegrityError(
                "operation pointer receipt fields are only partially present"
            )
        if fields["pointer_receipt_json"] is not None:
            raw_receipt = fields["pointer_receipt_json"].encode("utf-8")
            if not 0 < len(raw_receipt) <= 4096:
                raise LifecycleIntegrityError(
                    "operation pointer receipt exceeds its fixed bound"
                )
            try:
                receipt = json.loads(raw_receipt.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise LifecycleIntegrityError(
                    "operation pointer receipt is unreadable"
                ) from exc
            validated_receipt = self._validate_pointer_receipt_envelope(receipt)
            if (
                validated_receipt["operation_id"] != fields["operation_id"]
                or validated_receipt["receipt_hmac"]
                != fields["pointer_receipt_hmac"]
            ):
                raise LifecycleIntegrityError(
                    "operation pointer receipt exact binding mismatch"
                )
            receipt_state_is_valid = fields["state"] in {
                OperationState.PUBLISHED.value,
                OperationState.COMMITTED.value,
            } or (
                fields["state"] == OperationState.RECOVERY_BLOCKED.value
                and recovery_from == OperationState.PUBLISHED.value
            )
            if not receipt_state_is_valid:
                raise LifecycleIntegrityError(
                    "operation carries a receipt outside a published state"
                )

    def _operation_record_value(
        self, row: Mapping[str, Any], **changes: Any
    ) -> dict[str, Any]:
        value = dict(row)
        value.update(changes)
        value["record_hmac"] = self._sign(self._operation_signed_fields(value))
        return value

    def _operation_ledger_state_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": OPERATION_LEDGER_STATE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "total_count": int(row["total_count"]),
            "active_count": int(row["active_count"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    def _load_operation_ledger_state(
        self, connection: sqlite3.Connection
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM operation_ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("operation ledger state is missing")
        try:
            fields = self._operation_ledger_state_fields(row)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LifecycleIntegrityError("operation ledger state is malformed") from exc
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("operation ledger state HMAC mismatch")
        total = _nonnegative("operation ledger total_count", fields["total_count"])
        active = _nonnegative("operation ledger active_count", fields["active_count"])
        if active > total or total > self.limits.max_operations:
            raise LifecycleIntegrityError("operation ledger counters are impossible")
        return row

    def _refresh_operation_ledger_state(
        self, connection: sqlite3.Connection, *, fencing_token: int
    ) -> None:
        row = connection.execute(
            "SELECT * FROM operation_ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("operation ledger state is missing")
        value = {
            "total_count": _nonnegative(
                "operation ledger total_count", row["total_count"]
            ),
            "active_count": _nonnegative(
                "operation ledger active_count", row["active_count"]
            ),
            "fencing_token": _nonnegative(
                "operation ledger fencing_token", fencing_token
            ),
            "updated_ns": time.time_ns(),
        }
        if value["active_count"] > value["total_count"]:
            raise LifecycleIntegrityError("operation ledger counters are impossible")
        connection.execute(
            "UPDATE operation_ledger_state SET total_count=?,active_count=?,"
            "fencing_token=?,updated_ns=?,record_hmac=? WHERE singleton=1",
            (
                value["total_count"],
                value["active_count"],
                value["fencing_token"],
                value["updated_ns"],
                self._sign(self._operation_ledger_state_fields(value)),
            ),
        )

    def _validate_pointer_receipt_envelope(
        self, receipt: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        if not isinstance(receipt, Mapping):
            raise LifecycleIntegrityError("pointer receipt must be an object")
        try:
            _bounded_canonical_bytes(
                receipt,
                max_bytes=4096,
                label="pointer receipt",
            )
        except (LifecycleBoundError, LifecycleIntegrityError) as exc:
            raise LifecycleIntegrityError(
                "pointer receipt is not bounded canonical JSON"
            ) from exc
        expected_keys = {
            "schema_version",
            "root_key",
            "operation_id",
            "pointer_relative_path",
            "expected_sha256",
            "expected_bytes",
            "new_sha256",
            "new_bytes",
            "fencing_token",
            "published_ns",
            "receipt_hmac",
        }
        if set(receipt) != expected_keys:
            raise LifecycleIntegrityError("pointer receipt fields are not exact")
        unsigned = {key: receipt[key] for key in expected_keys - {"receipt_hmac"}}
        signature = str(receipt["receipt_hmac"])
        if not hmac.compare_digest(signature, self._sign(unsigned)):
            raise LifecycleIntegrityError("pointer receipt HMAC mismatch")
        if unsigned["schema_version"] != POINTER_RECEIPT_SCHEMA:
            raise LifecycleIntegrityError("pointer receipt schema mismatch")
        if unsigned["root_key"] != self.root_key:
            raise LifecycleIntegrityError("pointer receipt belongs to another native root")
        operation_id = _safe_identifier(
            str(unsigned["operation_id"]), label="operation_id"
        )
        relative = _safe_identifier(
            str(unsigned["pointer_relative_path"]),
            label="pointer_relative_path",
            max_length=512,
        )
        canonical_relative = PurePosixPath(relative).as_posix()
        if relative != canonical_relative:
            raise LifecycleIntegrityError("pointer receipt path is not canonical")
        self._safe_native_relative(relative)
        expected_sha = str(unsigned["expected_sha256"])
        if expected_sha != ABSENT_POINTER and not _is_sha256(expected_sha):
            raise LifecycleIntegrityError("pointer receipt expected digest is invalid")
        new_sha = str(unsigned["new_sha256"])
        if not _is_sha256(new_sha):
            raise LifecycleIntegrityError("pointer receipt new digest is invalid")
        try:
            expected_bytes = _nonnegative(
                "pointer receipt expected_bytes", unsigned["expected_bytes"]
            )
            new_bytes = _nonnegative(
                "pointer receipt new_bytes", unsigned["new_bytes"]
            )
            fencing_token = _nonnegative(
                "pointer receipt fencing_token", unsigned["fencing_token"]
            )
            published_ns = _nonnegative(
                "pointer receipt published_ns", unsigned["published_ns"]
            )
        except ValueError as exc:
            raise LifecycleIntegrityError("pointer receipt counters are invalid") from exc
        if (
            expected_bytes > self.limits.max_pointer_bytes
            or not 0 < new_bytes <= self.limits.max_pointer_bytes
            or fencing_token <= 0
            or published_ns <= 0
            or (expected_sha == ABSENT_POINTER and expected_bytes != 0)
        ):
            raise LifecycleIntegrityError("pointer receipt byte/token binding is invalid")
        return {
            **unsigned,
            "operation_id": operation_id,
            "pointer_relative_path": relative,
            "expected_sha256": expected_sha,
            "expected_bytes": expected_bytes,
            "new_sha256": new_sha,
            "new_bytes": new_bytes,
            "fencing_token": fencing_token,
            "published_ns": published_ns,
            "receipt_hmac": signature,
        }

    def _pointer_head_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "schema_version": POINTER_HEAD_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "pointer_relative_path": str(row["pointer_relative_path"]),
            "operation_id": str(row["operation_id"]),
            "receipt_json": str(row["receipt_json"]),
            "receipt_sha256": str(row["receipt_sha256"]),
            "fencing_token": int(row["fencing_token"]),
            "published_ns": int(row["published_ns"]),
        }

    def _validate_pointer_head(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            fields = self._pointer_head_fields(row)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LifecycleIntegrityError("pointer head record is malformed") from exc
        raw = fields["receipt_json"].encode("utf-8")
        if not 0 < len(raw) <= 4096:
            raise LifecycleIntegrityError("pointer head receipt exceeds its fixed bound")
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("pointer head record HMAC mismatch")
        if _sha256_bytes(raw) != fields["receipt_sha256"]:
            raise LifecycleIntegrityError("pointer head receipt digest mismatch")
        try:
            receipt = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleIntegrityError("pointer head receipt is unreadable") from exc
        validated = self._validate_pointer_receipt_envelope(receipt)
        if (
            validated["pointer_relative_path"] != fields["pointer_relative_path"]
            or validated["operation_id"] != fields["operation_id"]
            or validated["fencing_token"] != fields["fencing_token"]
            or validated["published_ns"] != fields["published_ns"]
        ):
            raise LifecycleIntegrityError("pointer head exact receipt binding mismatch")
        return validated

    def _pointer_ledger_state_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": POINTER_LEDGER_STATE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "head_count": int(row["head_count"]),
            "mutation_count": int(row["mutation_count"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    def _load_pointer_ledger_state(
        self, connection: sqlite3.Connection
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM pointer_ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("pointer ledger state is missing")
        try:
            fields = self._pointer_ledger_state_fields(row)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LifecycleIntegrityError("pointer ledger state is malformed") from exc
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("pointer ledger state HMAC mismatch")
        try:
            count = _nonnegative("pointer ledger head_count", fields["head_count"])
            _nonnegative(
                "pointer ledger mutation_count", fields["mutation_count"]
            )
        except ValueError as exc:
            raise LifecycleIntegrityError("pointer ledger counters are invalid") from exc
        if count > self.limits.max_operations:
            raise LifecycleIntegrityError("pointer ledger head count exceeds its cap")
        return row

    def _refresh_pointer_ledger_state(
        self, connection: sqlite3.Connection, *, fencing_token: int
    ) -> None:
        row = connection.execute(
            "SELECT * FROM pointer_ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("pointer ledger state is missing")
        value = {
            "head_count": _nonnegative(
                "pointer ledger head_count", row["head_count"]
            ),
            "mutation_count": _nonnegative(
                "pointer ledger mutation_count", row["mutation_count"]
            ),
            "fencing_token": _nonnegative(
                "pointer ledger fencing_token", fencing_token
            ),
            "updated_ns": time.time_ns(),
        }
        if value["head_count"] > self.limits.max_operations:
            raise LifecycleBoundError("pointer ledger head cap reached")
        connection.execute(
            "UPDATE pointer_ledger_state SET head_count=?,mutation_count=?,"
            "fencing_token=?,updated_ns=?,record_hmac=? WHERE singleton=1",
            (
                value["head_count"],
                value["mutation_count"],
                value["fencing_token"],
                value["updated_ns"],
                self._sign(self._pointer_ledger_state_fields(value)),
            ),
        )

    def _archive_head_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "schema_version": ARCHIVE_HEAD_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "operation_count": int(row["operation_count"]),
            "operation_sha256": str(row["operation_sha256"]),
            "pin_count": int(row["pin_count"]),
            "pin_sha256": str(row["pin_sha256"]),
            "runtime_count": int(row["runtime_count"]),
            "runtime_sha256": str(row["runtime_sha256"]),
            "broker_count": int(row["broker_count"]),
            "broker_sha256": str(row["broker_sha256"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    @staticmethod
    def _archive_digest(category: str, values: Sequence[str]) -> str:
        if not values:
            return GENESIS_SHA256
        return _sha256_bytes(
            _canonical_bytes(
                {
                    "schema_version": "fxpm-lifecycle-tombstone-set-v1",
                    "category": category,
                    "tombstone_sha256": sorted(values),
                }
            )
        )

    def _validate_operation_tombstone(self, row: Mapping[str, Any]) -> None:
        body = {
            "schema_version": OPERATION_TOMBSTONE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "operation_id": str(row["operation_id"]),
            "source_record_sha256": str(row["source_record_sha256"]),
            "retired_ns": int(row["retired_ns"]),
            "fencing_token": int(row["fencing_token"]),
        }
        _safe_identifier(body["operation_id"], label="retired operation_id")
        if not _is_sha256(body["source_record_sha256"]):
            raise LifecycleIntegrityError(
                "operation tombstone source digest is invalid"
            )
        expected_sha = _sha256_bytes(_canonical_bytes(body))
        if not hmac.compare_digest(str(row["tombstone_sha256"]), expected_sha):
            raise LifecycleIntegrityError("operation tombstone digest mismatch")
        if not hmac.compare_digest(
            str(row["record_hmac"]),
            self._sign({**body, "tombstone_sha256": expected_sha}),
        ):
            raise LifecycleIntegrityError("operation tombstone HMAC mismatch")

    def _validate_pin_tombstone(self, row: Mapping[str, Any]) -> None:
        body = {
            "schema_version": PIN_TOMBSTONE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "pin_id": str(row["pin_id"]),
            "source_record_sha256": str(row["source_record_sha256"]),
            "retired_ns": int(row["retired_ns"]),
            "fencing_token": int(row["fencing_token"]),
        }
        _safe_identifier(body["pin_id"], label="retired pin_id")
        if not _is_sha256(body["source_record_sha256"]):
            raise LifecycleIntegrityError("pin tombstone source digest is invalid")
        expected_sha = _sha256_bytes(_canonical_bytes(body))
        if not hmac.compare_digest(str(row["tombstone_sha256"]), expected_sha):
            raise LifecycleIntegrityError("pin tombstone digest mismatch")
        if not hmac.compare_digest(
            str(row["record_hmac"]),
            self._sign({**body, "tombstone_sha256": expected_sha}),
        ):
            raise LifecycleIntegrityError("pin tombstone HMAC mismatch")

    def _load_archive_head(
        self, connection: sqlite3.Connection, *, verify_rows: bool = True
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM lifecycle_archive_head WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("lifecycle archive head is missing")
        fields = self._archive_head_fields(row)
        for category in ("operation", "pin", "runtime", "broker"):
            _nonnegative(f"{category} archive count", fields[f"{category}_count"])
            if not _is_sha256(fields[f"{category}_sha256"]):
                raise LifecycleIntegrityError(
                    f"{category} archive head digest is invalid"
                )
        if not hmac.compare_digest(str(row["record_hmac"]), self._sign(fields)):
            raise LifecycleIntegrityError("lifecycle archive head HMAC mismatch")
        if int(row["broker_count"]) != 0 or str(
            row["broker_sha256"]
        ) != GENESIS_SHA256:
            raise LifecycleIntegrityError(
                "legacy broker tombstone copying is forbidden"
            )
        if verify_rows:
            tables = {
                "operation": (
                    "operation_tombstones",
                    self._validate_operation_tombstone,
                ),
                "pin": ("pin_tombstones", self._validate_pin_tombstone),
                "runtime": (
                    "runtime_tombstones",
                    self._validate_runtime_tombstone,
                ),
            }
            for category, (table, validator) in tables.items():
                rows = connection.execute(
                    f"SELECT * FROM {table} ORDER BY tombstone_sha256 LIMIT ?",
                    (self.limits.max_scan_entries + 1,),
                ).fetchall()
                if len(rows) > self.limits.max_scan_entries:
                    raise LifecycleBoundError(
                        f"{category} tombstone scan cap reached"
                    )
                for item in rows:
                    validator(item)
                values = [str(item["tombstone_sha256"]) for item in rows]
                if any(not _is_sha256(value) for value in values):
                    raise LifecycleIntegrityError(
                        f"{category} tombstone digest is invalid"
                    )
                if int(row[f"{category}_count"]) != len(values):
                    raise LifecycleIntegrityError(
                        f"{category} archive count/rows diverged"
                    )
                if str(row[f"{category}_sha256"]) != self._archive_digest(
                    category, values
                ):
                    raise LifecycleIntegrityError(
                        f"{category} archive digest/rows diverged"
                    )
        return row

    def _refresh_archive_head(
        self, connection: sqlite3.Connection, *, fencing_token: int
    ) -> None:
        old = self._load_archive_head(connection, verify_rows=False)
        updated: dict[str, Any] = {}
        tables = {
            "operation": "operation_tombstones",
            "pin": "pin_tombstones",
            "runtime": "runtime_tombstones",
        }
        for category, table in tables.items():
            rows = connection.execute(
                f"SELECT tombstone_sha256 FROM {table} ORDER BY tombstone_sha256 LIMIT ?",
                (self.limits.max_scan_entries + 1,),
            ).fetchall()
            if len(rows) > self.limits.max_scan_entries:
                raise LifecycleBoundError(f"{category} tombstone scan cap reached")
            values = [str(item[0]) for item in rows]
            updated[f"{category}_count"] = len(values)
            updated[f"{category}_sha256"] = self._archive_digest(category, values)
        updated["broker_count"] = 0
        updated["broker_sha256"] = GENESIS_SHA256
        updated["fencing_token"] = fencing_token
        updated["updated_ns"] = max(time.time_ns(), int(old["updated_ns"]) + 1)
        fields = self._archive_head_fields(updated)
        connection.execute(
            "UPDATE lifecycle_archive_head SET operation_count=?,operation_sha256=?,"
            "pin_count=?,pin_sha256=?,runtime_count=?,runtime_sha256=?,"
            "broker_count=?,broker_sha256=?,fencing_token=?,updated_ns=?,record_hmac=? "
            "WHERE singleton=1",
            (
                updated["operation_count"], updated["operation_sha256"],
                updated["pin_count"], updated["pin_sha256"],
                updated["runtime_count"], updated["runtime_sha256"],
                updated["broker_count"], updated["broker_sha256"],
                updated["fencing_token"], updated["updated_ns"], self._sign(fields),
            ),
        )

    def _preflight_db_growth(
        self, connection: sqlite3.Connection, *, estimated_bytes: int
    ) -> None:
        estimate = _nonnegative("estimated lifecycle DB growth", estimated_bytes)
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        max_pages = int(connection.execute("PRAGMA max_page_count").fetchone()[0])
        available = (max_pages - page_count + free_pages) * page_size
        # One extra B-tree page covers split/rollback geometry not represented
        # by the logical record bytes.
        required = estimate + page_size
        if required > available:
            raise LifecycleDatabaseFullError(
                "authenticated lifecycle growth would exceed the fixed DB cap"
            )

    @property
    def identity_sha256(self) -> str:
        """Canonical institution-wide authority identity for narrow adapters."""
        return self.authority_id

    def _runtime_domain_for_root(self, supplied: Path) -> tuple[str, OperationalDomain, Path]:
        self._assert_roots_unchanged()
        root = _validate_local_root(
            supplied,
            label="runtime operational_root",
            must_exist=True,
            allow_unsafe_roots=self._allow_unsafe_roots,
        )
        matches = [
            (name, domain)
            for name, domain in self.operational_domains.items()
            if root != domain.root and _contains(domain.root, root)
        ]
        if len(matches) != 1:
            raise LifecycleRootError(
                "runtime operational_root must be an exact subroot of one configured domain"
            )
        name, domain = matches[0]
        return name, domain, root

    def _runtime_signed_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        domain_name = str(row["domain"])
        domain = self.operational_domains.get(domain_name)
        if domain is None:
            raise LifecycleIntegrityError("runtime reservation domain is no longer configured")
        root_identity = _filesystem_identity(Path(str(row["operational_root"])))
        if (
            int(row["root_device"]) != root_identity["device"]
            or int(row["root_inode"]) != root_identity["inode"]
        ):
            raise LifecycleIntegrityError(
                "runtime stream root was replaced after reservation admission"
            )
        return {
            "schema_version": "fxpm-runtime-storage-reservation-v1",
            "authority_identity_sha256": self.authority_id,
            "reservation_id": str(row["reservation_id"]),
            "operational_root": os.path.normcase(str(row["operational_root"])),
            "root_device": int(row["root_device"]),
            "root_inode": int(row["root_inode"]),
            "stream_identity_sha256": str(row["stream_identity_sha256"]),
            "reserved_peak_bytes": int(row["reserved_peak_bytes"]),
            "institution_cap_bytes": int(row["institution_cap_bytes"]),
            "admitted_streams_cap": int(row["admitted_streams_cap"]),
            "domain": domain_name,
            "domain_root": os.path.normcase(str(domain.root)),
            "domain_cap_bytes": domain.max_bytes,
            "domain_cap_files": domain.max_files,
            "volume_key": _volume_key(domain.root),
            "fencing_token": int(row["fencing_token"]),
            "status": str(row["status"]),
        }

    def _runtime_adapter_from_row(self, row: Mapping[str, Any]) -> RuntimeStorageReservation:
        signed = self._runtime_signed_fields(row)
        expected_mac = self._sign(signed)
        stored_mac = str(row["receipt_mac"])
        if not hmac.compare_digest(stored_mac, expected_mac):
            raise LifecycleIntegrityError("runtime reservation receipt MAC mismatch")
        return RuntimeStorageReservation(
            authority_identity_sha256=self.authority_id,
            reservation_id=str(row["reservation_id"]),
            operational_domain_root=str(
                self.operational_domains[str(row["domain"])].root
            ),
            stream_subpath=Path(str(row["operational_root"]))
            .resolve(strict=False)
            .relative_to(self.operational_domains[str(row["domain"])].root)
            .as_posix(),
            stream_identity_sha256=str(row["stream_identity_sha256"]),
            reserved_peak_bytes=int(row["reserved_peak_bytes"]),
            institution_cap_bytes=int(row["institution_cap_bytes"]),
            admitted_streams_cap=int(row["admitted_streams_cap"]),
            receipt_mac=expected_mac,
        )

    def _runtime_tombstone_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "schema_version": RUNTIME_TOMBSTONE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "reservation_id": str(row["reservation_id"]),
            "operational_root": str(row["operational_root"]),
            "stream_identity_sha256": str(row["stream_identity_sha256"]),
            "account_scope_sha256": row["account_scope_sha256"],
            "reservation_sha256": str(row["reservation_sha256"]),
            "witness_sha256": str(row["witness_sha256"]),
            "runtime_claim_head_sha256": str(
                row["runtime_claim_head_sha256"]
            ),
            "max_operation_sequence": int(row["max_operation_sequence"]),
            "retired_operation_floor": int(row["retired_operation_floor"]),
            "retired_ns": int(row["retired_ns"]),
            "fencing_token": int(row["fencing_token"]),
        }

    def _validate_runtime_tombstone(self, row: Mapping[str, Any]) -> None:
        fields = self._runtime_tombstone_fields(row)
        for name in (
            "stream_identity_sha256",
            "reservation_sha256",
            "witness_sha256",
            "runtime_claim_head_sha256",
        ):
            if not _is_sha256(fields[name]):
                raise LifecycleIntegrityError(f"runtime tombstone {name} is invalid")
        if fields["account_scope_sha256"] is not None and not _is_sha256(
            fields["account_scope_sha256"]
        ):
            raise LifecycleIntegrityError(
                "runtime tombstone account scope is invalid"
            )
        maximum = _nonnegative(
            "runtime tombstone max_operation_sequence",
            fields["max_operation_sequence"],
        )
        floor = _nonnegative(
            "runtime tombstone retired_operation_floor",
            fields["retired_operation_floor"],
        )
        if floor > maximum:
            raise LifecycleIntegrityError(
                "runtime tombstone operation floor exceeds its high-water"
            )
        if fields["account_scope_sha256"] is None and (
            fields["runtime_claim_head_sha256"] != GENESIS_SHA256
            or maximum != 0
            or floor != 0
        ):
            raise LifecycleIntegrityError(
                "unbound runtime tombstone carries broker claim state"
            )
        expected_sha = _sha256_bytes(_canonical_bytes(fields))
        if not hmac.compare_digest(str(row["tombstone_sha256"]), expected_sha):
            raise LifecycleIntegrityError("runtime tombstone digest mismatch")
        if not hmac.compare_digest(
            str(row["record_hmac"]),
            self._sign({**fields, "tombstone_sha256": expected_sha}),
        ):
            raise LifecycleIntegrityError("runtime tombstone HMAC mismatch")

    def _witness_signed_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "schema_version": RUNTIME_WITNESS_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "reservation_id": str(row["reservation_id"]),
            "witness_version": int(row["witness_version"]),
            "writer_epoch": int(row["writer_epoch"]),
            "reducer_contract_sha256": str(row["reducer_contract_sha256"]),
            "max_bar_sequence": int(row["max_bar_sequence"]),
            "bar_chain_sha256": str(row["bar_chain_sha256"]),
            "checkpoint_generation": int(row["checkpoint_generation"]),
            "checkpoint_sha256": str(row["checkpoint_sha256"]),
            "max_order_operation_sequence": int(row["max_order_operation_sequence"]),
            "max_order_event_sequence": int(row["max_order_event_sequence"]),
            "order_chain_sha256": str(row["order_chain_sha256"]),
            "fencing_token": int(row["fencing_token"]),
        }

    def _witness_from_row(self, row: Mapping[str, Any]) -> RuntimeMonotonicWitness:
        signed = self._witness_signed_fields(row)
        for name in (
            "reducer_contract_sha256",
            "bar_chain_sha256",
            "checkpoint_sha256",
            "order_chain_sha256",
        ):
            if not _is_sha256(signed[name]):
                raise LifecycleIntegrityError(f"runtime witness {name} is invalid")
        expected = self._sign(signed)
        if not hmac.compare_digest(str(row["record_hmac"]), expected):
            raise LifecycleIntegrityError("runtime monotonic witness HMAC mismatch")
        return RuntimeMonotonicWitness(
            **{
                key: value
                for key, value in signed.items()
                if key != "schema_version"
            },
            receipt_mac=expected,
        )

    def _runtime_account_binding_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": RUNTIME_ACCOUNT_BINDING_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "reservation_id": str(row["reservation_id"]),
            "account_scope_sha256": str(row["account_scope_sha256"]),
            "stream_identity_sha256": str(row["stream_identity_sha256"]),
            "fencing_token": int(row["fencing_token"]),
            "created_ns": int(row["created_ns"]),
        }

    def _validate_runtime_account_binding(self, row: Mapping[str, Any]) -> None:
        signed = self._runtime_account_binding_fields(row)
        for name in ("account_scope_sha256", "stream_identity_sha256"):
            if not _is_sha256(signed[name]):
                raise LifecycleIntegrityError(f"runtime account binding {name} is invalid")
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(signed)
        ):
            raise LifecycleIntegrityError("runtime account binding HMAC mismatch")

    def _broker_account_head_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": BROKER_ACCOUNT_HEAD_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "account_scope_sha256": str(row["account_scope_sha256"]),
            "claim_count": int(row["claim_count"]),
            "last_claim_sequence": int(row["last_claim_sequence"]),
            "last_claim_sha256": str(row["last_claim_sha256"]),
            "external_highwater_sequence": int(
                row["external_highwater_sequence"]
            ),
            "external_receipt_sha256": str(row["external_receipt_sha256"]),
            "retired_execution_floor": int(row["retired_execution_floor"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    def _validate_broker_account_head(self, row: Mapping[str, Any]) -> None:
        signed = self._broker_account_head_fields(row)
        if not _is_sha256(signed["account_scope_sha256"]):
            raise LifecycleIntegrityError("broker account scope digest is invalid")
        if not _is_sha256(signed["last_claim_sha256"]):
            raise LifecycleIntegrityError("broker account head digest is invalid")
        if not _is_sha256(signed["external_receipt_sha256"]):
            raise LifecycleIntegrityError(
                "broker account external receipt digest is invalid"
            )
        count = _nonnegative("broker account claim_count", signed["claim_count"])
        sequence = _nonnegative(
            "broker account last_claim_sequence", signed["last_claim_sequence"]
        )
        if count != sequence:
            raise LifecycleIntegrityError("broker account claim count/sequence diverged")
        if (count == 0) != (signed["last_claim_sha256"] == GENESIS_SHA256):
            raise LifecycleIntegrityError("broker account genesis head is inconsistent")
        external = _nonnegative(
            "broker external high-water",
            signed["external_highwater_sequence"],
        )
        retired = _nonnegative(
            "broker retired execution floor",
            signed["retired_execution_floor"],
        )
        if retired > external:
            raise LifecycleIntegrityError(
                "broker retired floor exceeds external high-water"
            )
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(signed)
        ):
            raise LifecycleIntegrityError("broker account head HMAC mismatch")

    def _broker_claim_public_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "schema_version": BROKER_EXECUTION_CLAIM_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "claim_id": str(row["claim_id"]),
            "account_scope_sha256": str(row["account_scope_sha256"]),
            "broker_execution_id": str(row["broker_execution_id"]),
            "broker_execution_sequence": int(row["broker_execution_sequence"]),
            "claim_sequence": int(row["claim_sequence"]),
            "stream_identity_sha256": str(row["stream_identity_sha256"]),
            "operation_id": str(row["operation_id"]),
            "operation_sequence": int(row["operation_sequence"]),
            "payload_sha256": str(row["payload_sha256"]),
            "fencing_token": int(row["fencing_token"]),
        }

    def _broker_claim_chain_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            **self._broker_claim_public_fields(row),
            "previous_claim_sha256": str(row["previous_claim_sha256"]),
            "created_ns": int(row["created_ns"]),
        }

    def _broker_claim_record_fields(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            **self._broker_claim_chain_fields(row),
            "claim_sha256": str(row["claim_sha256"]),
            "receipt_mac": str(row["receipt_mac"]),
        }

    def _broker_claim_from_row(self, row: Mapping[str, Any]) -> BrokerExecutionClaim:
        public = self._broker_claim_public_fields(row)
        for name in (
            "account_scope_sha256",
            "stream_identity_sha256",
            "payload_sha256",
            "previous_claim_sha256",
            "claim_sha256",
        ):
            if not _is_sha256(row[name]):
                raise LifecycleIntegrityError(f"broker execution claim {name} is invalid")
        _safe_identifier(str(row["claim_id"]), label="claim_id")
        _safe_identifier(
            str(row["broker_execution_id"]),
            label="broker_execution_id",
            max_length=256,
        )
        _safe_identifier(str(row["operation_id"]), label="operation_id")
        if _nonnegative(
            "broker_execution_sequence", row["broker_execution_sequence"]
        ) <= 0:
            raise LifecycleIntegrityError("broker execution sequence must be positive")
        if _nonnegative("claim_sequence", row["claim_sequence"]) <= 0:
            raise LifecycleIntegrityError("broker claim sequence must be positive")
        if _nonnegative("operation_sequence", row["operation_sequence"]) <= 0:
            raise LifecycleIntegrityError("broker operation sequence must be positive")
        expected_claim_sha = _sha256_bytes(
            _canonical_bytes(self._broker_claim_chain_fields(row))
        )
        if not hmac.compare_digest(str(row["claim_sha256"]), expected_claim_sha):
            raise LifecycleIntegrityError("broker execution claim chain digest mismatch")
        expected_receipt = self._sign(public)
        if not hmac.compare_digest(str(row["receipt_mac"]), expected_receipt):
            raise LifecycleIntegrityError("broker execution claim receipt HMAC mismatch")
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(self._broker_claim_record_fields(row))
        ):
            raise LifecycleIntegrityError("broker execution claim record HMAC mismatch")
        return BrokerExecutionClaim(
            **{key: value for key, value in public.items() if key != "schema_version"},
            receipt_mac=expected_receipt,
        )

    def _broker_ledger_state_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": BROKER_LEDGER_STATE_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "ledger_version": int(row["ledger_version"]),
            "total_claim_count": int(row["total_claim_count"]),
            "live_claim_count": int(row["live_claim_count"]),
            "account_count": int(row["account_count"]),
            "accumulator_sha256": str(row["accumulator_sha256"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    def _load_broker_ledger_state(
        self, connection: sqlite3.Connection, *, verify_counts: bool = False
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM broker_ledger_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("broker ledger state is missing")
        fields = self._broker_ledger_state_fields(row)
        for name in (
            "ledger_version",
            "total_claim_count",
            "live_claim_count",
            "account_count",
        ):
            _nonnegative(f"broker ledger {name}", fields[name])
        if fields["ledger_version"] != fields["total_claim_count"]:
            raise LifecycleIntegrityError("broker ledger version/count diverged")
        if fields["live_claim_count"] > fields["total_claim_count"]:
            raise LifecycleIntegrityError("broker live count exceeds logical count")
        if not _is_sha256(fields["accumulator_sha256"]):
            raise LifecycleIntegrityError("broker ledger accumulator is invalid")
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("broker ledger state HMAC mismatch")
        if verify_counts:
            live = int(
                connection.execute(
                    "SELECT COUNT(*) FROM broker_execution_claims"
                ).fetchone()[0]
            )
            accounts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM broker_account_heads"
                ).fetchone()[0]
            )
            if live != fields["live_claim_count"]:
                raise LifecycleIntegrityError(
                    "broker live claim count indicates deletion or injection"
                )
            if accounts != fields["account_count"]:
                raise LifecycleIntegrityError(
                    "broker account count indicates deletion or injection"
                )
        return row

    def _write_broker_ledger_state(
        self, connection: sqlite3.Connection, fields: Mapping[str, Any]
    ) -> None:
        signed = self._broker_ledger_state_fields(fields)
        connection.execute(
            "UPDATE broker_ledger_state SET ledger_version=?,total_claim_count=?,"
            "live_claim_count=?,account_count=?,accumulator_sha256=?,"
            "fencing_token=?,updated_ns=?,record_hmac=? WHERE singleton=1",
            (
                signed["ledger_version"],
                signed["total_claim_count"],
                signed["live_claim_count"],
                signed["account_count"],
                signed["accumulator_sha256"],
                signed["fencing_token"],
                signed["updated_ns"],
                self._sign(signed),
            ),
        )

    def _runtime_claim_head_fields(
        self, row: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "schema_version": RUNTIME_CLAIM_HEAD_SCHEMA,
            "authority_identity_sha256": self.authority_id,
            "reservation_id": str(row["reservation_id"]),
            "account_scope_sha256": str(row["account_scope_sha256"]),
            "stream_identity_sha256": str(row["stream_identity_sha256"]),
            "max_operation_sequence": int(row["max_operation_sequence"]),
            "retired_operation_floor": int(row["retired_operation_floor"]),
            "fencing_token": int(row["fencing_token"]),
            "updated_ns": int(row["updated_ns"]),
        }

    def _validate_runtime_claim_head(self, row: Mapping[str, Any]) -> None:
        fields = self._runtime_claim_head_fields(row)
        for name in ("account_scope_sha256", "stream_identity_sha256"):
            if not _is_sha256(fields[name]):
                raise LifecycleIntegrityError(
                    f"runtime claim head {name} is invalid"
                )
        maximum = _nonnegative(
            "max_operation_sequence", fields["max_operation_sequence"]
        )
        floor = _nonnegative(
            "retired_operation_floor", fields["retired_operation_floor"]
        )
        if floor > maximum:
            raise LifecycleIntegrityError(
                "runtime retired operation floor exceeds its high-water"
            )
        if not hmac.compare_digest(
            str(row["record_hmac"]), self._sign(fields)
        ):
            raise LifecycleIntegrityError("runtime claim head HMAC mismatch")

    def _verify_broker_highwater_receipt(
        self,
        value: Any,
        *,
        account_scope_sha256: str,
        requested_sequence: int,
    ) -> BrokerAccountHighWaterReceipt:
        if isinstance(value, BrokerAccountHighWaterReceipt):
            receipt = value
        elif isinstance(value, Mapping):
            if set(value) != set(BrokerAccountHighWaterReceipt.__dataclass_fields__):
                raise LifecycleIntegrityError(
                    "broker high-water receipt fields are not exact"
                )
            try:
                receipt = BrokerAccountHighWaterReceipt(**dict(value))
            except (TypeError, ValueError) as exc:
                raise LifecycleIntegrityError(
                    "broker high-water receipt is malformed"
                ) from exc
        else:
            raise LifecycleIntegrityError(
                "an independent broker high-water receipt is required"
            )
        body = receipt.body()
        if receipt.schema_version != BROKER_HIGH_WATER_SCHEMA:
            raise LifecycleIntegrityError("broker high-water schema mismatch")
        if receipt.account_scope_sha256 != account_scope_sha256:
            raise LifecycleIntegrityError("broker high-water account mismatch")
        if not _is_sha256(receipt.source_snapshot_sha256):
            raise LifecycleIntegrityError("broker high-water snapshot is invalid")
        observed = _nonnegative(
            "observed_sequence_highwater", receipt.observed_sequence_highwater
        )
        if observed < requested_sequence:
            raise LifecycleIntegrityError(
                "broker high-water does not cover the claimed execution"
            )
        material = b"broker-account-high-water\x00" + _canonical_bytes(body)
        expected = hmac.new(
            self._broker_highwater_key, material, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(str(receipt.mac), expected):
            raise LifecycleIntegrityError("broker high-water receipt HMAC mismatch")
        return receipt

    @staticmethod
    def _broker_claim_inputs(
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
    ) -> Mapping[str, Any]:
        account_scope = str(account_scope_sha256).lower()
        payload = str(payload_sha256).lower()
        if not _is_sha256(account_scope):
            raise ValueError("account_scope_sha256 must be a SHA-256 digest")
        if not _is_sha256(payload):
            raise ValueError("payload_sha256 must be a SHA-256 digest")
        execution_id = _safe_identifier(
            broker_execution_id,
            label="broker_execution_id",
            max_length=256,
        )
        execution_sequence = _nonnegative(
            "broker_execution_sequence", broker_execution_sequence
        )
        if execution_sequence <= 0:
            raise ValueError("broker_execution_sequence must be positive")
        if execution_id != str(execution_sequence):
            raise ValueError(
                "broker_execution_id must be the canonical decimal execution sequence"
            )
        local_operation_sequence = _nonnegative(
            "operation_sequence", operation_sequence
        )
        if local_operation_sequence <= 0:
            raise ValueError("operation_sequence must be positive")
        return {
            "account_scope_sha256": account_scope,
            "broker_execution_id": execution_id,
            "broker_execution_sequence": execution_sequence,
            "operation_id": _safe_identifier(operation_id, label="operation_id"),
            "operation_sequence": local_operation_sequence,
            "payload_sha256": payload,
        }

    @staticmethod
    def _proposal_value(proposed: Any) -> RuntimeWitnessProposal:
        if isinstance(proposed, RuntimeWitnessProposal):
            value = proposed
        else:
            if hasattr(proposed, "canonical"):
                raw = proposed.canonical()
            elif isinstance(proposed, Mapping):
                raw = dict(proposed)
            else:
                raise TypeError("proposed_witness must be a proposal mapping/dataclass")
            names = {
                "writer_epoch",
                "reducer_contract_sha256",
                "max_bar_sequence",
                "bar_chain_sha256",
                "checkpoint_generation",
                "checkpoint_sha256",
                "max_order_operation_sequence",
                "max_order_event_sequence",
                "order_chain_sha256",
            }
            if set(raw) != names:
                raise ValueError("runtime witness proposal fields are not exact")
            value = RuntimeWitnessProposal(**raw)
        for name in (
            "writer_epoch",
            "max_bar_sequence",
            "checkpoint_generation",
            "max_order_operation_sequence",
            "max_order_event_sequence",
        ):
            observed = getattr(value, name)
            try:
                _nonnegative(f"runtime witness {name}", observed)
            except ValueError as exc:
                raise ValueError(
                    f"runtime witness {name} must be a bounded non-negative integer"
                ) from exc
        for name in (
            "reducer_contract_sha256",
            "bar_chain_sha256",
            "checkpoint_sha256",
            "order_chain_sha256",
        ):
            if not _is_sha256(getattr(value, name)):
                raise ValueError(f"runtime witness {name} must be SHA-256")
        if value.max_order_event_sequence < value.max_order_operation_sequence:
            raise ValueError("order event sequence cannot trail operation sequence")
        return value

    def reserve_runtime_storage(
        self,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> RuntimeStorageReservation:
        """Atomically admit or idempotently reopen one exact runtime stream."""
        stream = str(stream_identity_sha256).lower()
        if not _is_sha256(stream):
            raise ValueError("stream_identity_sha256 must be a lowercase SHA-256 digest")
        requested = _nonnegative("requested_peak_bytes", requested_peak_bytes)
        if requested <= 0:
            raise ValueError("requested_peak_bytes must be positive")
        domain_name, domain, root = self._runtime_domain_for_root(operational_root)
        if requested > domain.max_bytes or requested > self.institution_cap_bytes:
            raise LifecycleBoundError("runtime request exceeds its domain/institution cap")
        with self.lease(f"runtime-admission-{stream[:16]}") as lease:
            return lease._reserve_runtime_storage(
                domain_name=domain_name,
                domain=domain,
                operational_root=root,
                stream_identity_sha256=stream,
                requested_peak_bytes=requested,
            )

    def read_runtime_witness(self, reservation: Any) -> RuntimeMonotonicWitness:
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        with self.lease(f"runtime-witness-read-{reservation_id[:12]}") as lease:
            return lease._read_runtime_witness(reservation)

    def cas_runtime_witness(
        self,
        reservation: Any,
        expected_witness: Any,
        proposed_witness: RuntimeWitnessProposal | Mapping[str, Any] | Any,
    ) -> RuntimeMonotonicWitness:
        """Advance one stream witness by an exact authenticated compare-and-swap."""
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        proposal = self._proposal_value(proposed_witness)
        with self.lease(f"runtime-witness-cas-{reservation_id[:12]}") as lease:
            return lease._cas_runtime_witness(
                reservation,
                expected_witness=expected_witness,
                proposed=proposal,
            )

    def claim_broker_execution(
        self,
        reservation: Any,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Any,
    ) -> BrokerExecutionClaim:
        """Atomically claim one positive broker fill across the institution.

        Definitive ``REJECTED_NO_EFFECT`` results never call this method and
        ticket/deal zero is therefore outside the execution uniqueness ledger.
        """
        values = self._broker_claim_inputs(
            account_scope_sha256=account_scope_sha256,
            broker_execution_id=broker_execution_id,
            broker_execution_sequence=broker_execution_sequence,
            operation_id=operation_id,
            operation_sequence=operation_sequence,
            payload_sha256=payload_sha256,
        )
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        with self.lease(f"broker-claim-{reservation_id[:12]}") as lease:
            return lease._claim_broker_execution(
                reservation,
                broker_highwater_receipt=broker_highwater_receipt,
                **values,
            )

    def verify_broker_execution_claim(
        self,
        claim: Any,
        reservation: Any,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Any,
    ) -> None:
        """Verify a claim against its current authenticated ledger row."""
        values = self._broker_claim_inputs(
            account_scope_sha256=account_scope_sha256,
            broker_execution_id=broker_execution_id,
            broker_execution_sequence=broker_execution_sequence,
            operation_id=operation_id,
            operation_sequence=operation_sequence,
            payload_sha256=payload_sha256,
        )
        claim_id = _safe_identifier(
            str(getattr(claim, "claim_id", "")), label="claim_id"
        )
        with self.lease(f"broker-verify-{claim_id[:12]}") as lease:
            lease._verify_broker_execution_claim(
                claim,
                reservation,
                broker_highwater_receipt=broker_highwater_receipt,
                **values,
            )

    def audit_broker_ledger(self) -> Mapping[str, Any]:
        """Run the explicit bounded-batch audit excluded from runtime hot paths."""

        with self.lease("explicit-broker-ledger-audit") as lease:
            return lease._audit_broker_ledger()

    def audit_lifecycle_archive(self) -> Mapping[str, Any]:
        """Verify every bounded lifecycle tombstone outside runtime hot paths."""

        with self.lease("explicit-lifecycle-archive-audit") as lease:
            return lease._audit_lifecycle_archive()

    def verify_runtime_storage(
        self,
        reservation: Any,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> None:
        """Verify the exact adapter contract expected by ``pm_state_runtime``."""
        domain_name, _domain, root = self._runtime_domain_for_root(operational_root)
        stream = str(stream_identity_sha256).lower()
        requested = _nonnegative("requested_peak_bytes", requested_peak_bytes)
        supplied = {
            name: getattr(reservation, name, None)
            for name in (
                "authority_identity_sha256",
                "reservation_id",
                "operational_domain_root",
                "stream_subpath",
                "stream_identity_sha256",
                "reserved_peak_bytes",
                "institution_cap_bytes",
                "admitted_streams_cap",
                "receipt_mac",
            )
        }
        if supplied["authority_identity_sha256"] != self.authority_id:
            raise LifecycleIntegrityError("runtime reservation authority mismatch")
        supplied_domain_root = Path(
            str(supplied["operational_domain_root"])
        ).resolve(strict=False)
        if supplied_domain_root != self.operational_domains[domain_name].root:
            raise LifecycleIntegrityError("runtime reservation domain-root mismatch")
        subpath = Path(str(supplied["stream_subpath"]))
        if subpath.is_absolute() or ".." in subpath.parts or not subpath.parts:
            raise LifecycleIntegrityError("runtime reservation stream subpath is unsafe")
        if (supplied_domain_root / subpath).resolve(strict=False) != root:
            raise LifecycleIntegrityError("runtime reservation exact subroot mismatch")
        if supplied["stream_identity_sha256"] != stream:
            raise LifecycleIntegrityError("runtime reservation stream binding mismatch")
        if int(supplied["reserved_peak_bytes"] or -1) < requested:
            raise LifecycleBoundError("runtime reservation is below the requested DB+journal peak")
        reservation_id = _safe_identifier(
            str(supplied["reservation_id"] or ""), label="reservation_id"
        )
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM runtime_reservations WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None or str(row["status"]) != "ACTIVE":
            raise LifecycleStateError("runtime reservation is absent or inactive")
        if str(row["domain"]) != domain_name:
            raise LifecycleIntegrityError("runtime reservation domain mismatch")
        expected = self._runtime_adapter_from_row(row)
        if expected.canonical() != supplied:
            raise LifecycleIntegrityError("runtime reservation receipt is stale or forged")

    def release_runtime_storage(self, reservation: Any) -> None:
        """Explicitly release admission; evidence files are never deleted."""
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        with self.lease(f"runtime-release-{reservation_id[:16]}") as lease:
            lease._release_runtime_storage(reservation)

    def _safe_native_relative(self, value: Any) -> Path:
        text = str(value or "").replace("\\", "/")
        pure = PurePosixPath(text)
        if (
            not text
            or pure.is_absolute()
            or "." in pure.parts
            or ".." in pure.parts
            or any(":" in part for part in pure.parts)
        ):
            raise LifecycleIntegrityError(f"unsafe native relative path: {value!r}")
        lexical = self.native_root / Path(*pure.parts)
        cursor = self.native_root
        for part in pure.parts:
            cursor = cursor / part
            if cursor.exists() and cursor.is_symlink():
                raise LifecycleIntegrityError(f"symlinked native path is forbidden: {value!r}")
        try:
            lexical.resolve(strict=False).relative_to(self.native_root)
        except ValueError as exc:
            raise LifecycleIntegrityError(f"native path escapes its root: {value!r}") from exc
        return lexical

    def verify_pointer_receipt(self, receipt: Mapping[str, Any]) -> Mapping[str, Any]:
        self._assert_roots_unchanged()
        validated = self._validate_pointer_receipt_envelope(receipt)
        pointer_relative = str(validated["pointer_relative_path"])
        connection = self._connect()
        try:
            head = connection.execute(
                "SELECT * FROM pointer_heads WHERE pointer_relative_path=?",
                (pointer_relative,),
            ).fetchone()
        finally:
            connection.close()
        if head is None:
            raise LifecycleIntegrityError(
                "pointer receipt is not backed by a current authenticated pointer head"
            )
        current_receipt = self._validate_pointer_head(head)
        if dict(current_receipt) != dict(validated):
            raise LifecycleIntegrityError(
                "pointer receipt is not the current acknowledged publication"
            )
        pointer = self._safe_native_relative(pointer_relative)
        digest, size = _sha256_file(pointer, max_bytes=self.limits.max_pointer_bytes)
        if digest != validated["new_sha256"] or size != validated["new_bytes"]:
            raise LifecycleIntegrityError("published pointer no longer matches its CAS receipt")
        return dict(receipt)

    def verify_reservation_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        required_domain: Optional[str] = None,
        require_active: bool = True,
    ) -> Mapping[str, Any]:
        """Authenticate a quota receipt and bind it to the current DB row."""
        if not isinstance(receipt, Mapping):
            raise LifecycleIntegrityError("reservation receipt must be an object")
        unsigned = dict(receipt)
        signature = str(unsigned.pop("receipt_hmac", ""))
        if not hmac.compare_digest(signature, self._sign(unsigned)):
            raise LifecycleIntegrityError("reservation receipt HMAC mismatch")
        if unsigned.get("schema_version") != RESERVATION_RECEIPT_SCHEMA:
            raise LifecycleIntegrityError("reservation receipt schema mismatch")
        if unsigned.get("authority_id") != self.authority_id:
            raise LifecycleIntegrityError("reservation receipt authority identity mismatch")
        if unsigned.get("root_key") != self.root_key:
            raise LifecycleIntegrityError("reservation receipt belongs to another native root")
        operation_id = _safe_identifier(
            str(unsigned.get("operation_id", "")), label="operation_id"
        )
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise LifecycleIntegrityError("reservation receipt operation no longer exists")
        self._validate_operation_row(row)
        if int(unsigned.get("fencing_token", -1)) != int(row["fencing_token"]):
            raise LifecycleIntegrityError("reservation receipt has been fenced by a newer lease")
        if str(unsigned.get("state", "")) != str(row["state"]):
            raise LifecycleIntegrityError("reservation receipt state is stale")
        if str(unsigned.get("budget_sha256", "")) != str(row["budget_sha256"]):
            raise LifecycleIntegrityError("reservation receipt budget is stale")
        materialized = {
            "native_peak": int(row["materialized_native_peak_bytes"]),
            "native_final": int(row["materialized_native_final_bytes"]),
            "cold_peak": int(row["materialized_cold_peak_bytes"]),
            "cold_final": int(row["materialized_cold_final_bytes"]),
            "native_files": int(row["materialized_native_files"]),
            "cold_files": int(row["materialized_cold_files"]),
            "operational": json.loads(str(row["materialized_operational_json"])),
        }
        if unsigned.get("materialized_sha256") != _sha256_bytes(
            _canonical_bytes(materialized)
        ):
            raise LifecycleIntegrityError("reservation receipt materialization is stale")
        if require_active and (
            str(row["state"]) in TERMINAL_STATES
            or str(row["state"]) == OperationState.RECOVERY_BLOCKED.value
            or unsigned.get("verification_status") != "ACTIVE"
        ):
            raise LifecycleStateError("reservation receipt is not an active growth permit")
        if (
            int(unsigned.get("institution_cap_bytes", -1)) != self.institution_cap_bytes
            or int(unsigned.get("min_volume_free_bytes", -1))
            != self.limits.min_volume_free_bytes
        ):
            raise LifecycleIntegrityError("reservation receipt institutional cap drift")
        domains = unsigned.get("operational_domains")
        if not isinstance(domains, list):
            raise LifecycleIntegrityError("reservation receipt domain vector is invalid")
        configured = {
            name: domain for name, domain in self.operational_domains.items()
        }
        for item in domains:
            if not isinstance(item, Mapping):
                raise LifecycleIntegrityError("reservation receipt domain entry is invalid")
            name = str(item.get("domain", ""))
            domain = configured.get(name)
            if domain is None:
                raise LifecycleIntegrityError("reservation receipt names an unknown domain")
            if (
                os.path.normcase(str(domain.root)) != item.get("allowed_root")
                or domain.max_bytes != int(item.get("domain_cap_bytes", -1))
                or domain.max_files != int(item.get("domain_cap_files", -1))
                or _volume_key(domain.root) != item.get("volume_key")
            ):
                raise LifecycleIntegrityError("reservation receipt domain authority drift")
        if required_domain is not None:
            required = _safe_identifier(
                required_domain, label="required_domain", max_length=64
            )
            if required not in {str(item.get("domain")) for item in domains}:
                raise LifecycleIntegrityError(
                    f"reservation receipt does not authorize required domain {required}"
                )
        return dict(receipt)

    def _verify_pin_row(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        pin_id = _safe_identifier(str(row["pin_id"]), label="pin_id")
        raw = str(row["payload_json"]).encode("utf-8")
        if len(raw) != int(row["payload_bytes"]) or len(raw) > self.limits.max_pin_payload_bytes:
            raise LifecycleIntegrityError("pin payload byte count is invalid")
        if _sha256_bytes(raw) != str(row["payload_sha256"]):
            raise LifecycleIntegrityError("pin payload digest mismatch")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleIntegrityError("pin payload is unreadable") from exc
        signed = {
            "pin_id": pin_id,
            "created_ns": int(row["created_ns"]),
            "payload_sha256": str(row["payload_sha256"]),
            "payload_bytes": int(row["payload_bytes"]),
            "root_key": self.root_key,
        }
        if not hmac.compare_digest(str(row["record_hmac"]), self._sign(signed)):
            raise LifecycleIntegrityError("pin record HMAC mismatch")
        if not isinstance(payload, Mapping) or payload.get("schema_version") != PIN_SCHEMA:
            raise LifecycleIntegrityError("pin payload schema mismatch")
        roots = payload.get("roots")
        if not isinstance(roots, list) or len(roots) > self.limits.max_files_per_operation:
            raise LifecycleIntegrityError("pin roots are invalid or unbounded")
        for root in roots:
            if not isinstance(root, Mapping) or not _is_sha256(root.get("sha256")):
                raise LifecycleIntegrityError("pin root binding is invalid")
            path = self._safe_native_relative(root.get("relative_path"))
            expected_size = int(root.get("size", -1))
            if expected_size < 0:
                raise LifecycleIntegrityError("pin root size binding is invalid")
            try:
                digest, size = _sha256_file(path, max_bytes=expected_size)
            except LifecycleIntegrityError as exc:
                raise LifecycleIntegrityError(
                    f"pinned evidence changed: {path}"
                ) from exc
            if digest != root.get("sha256") or size != expected_size:
                raise LifecycleIntegrityError(f"pinned evidence changed: {path}")
        return payload

    def verify_pin(self, pin_id: str) -> Mapping[str, Any]:
        pin_id = _safe_identifier(pin_id, label="pin_id")
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM pins WHERE pin_id=?", (pin_id,)).fetchone()
        finally:
            connection.close()
        if row is None:
            raise LifecycleIntegrityError(f"unknown pin: {pin_id}")
        return self._verify_pin_row(row)

    def retire_pin(self, pin_id: str) -> None:
        """Replace one verified pin with an authenticated non-reusable tombstone."""

        with self.lease("runtime-pin-retirement") as lease:
            lease._retire_pin(pin_id)

    def list_operations(self) -> list[Mapping[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM operations ORDER BY created_ns,operation_id"
            ).fetchall()
            result: list[Mapping[str, Any]] = []
            for row in rows:
                self._validate_operation_row(row)
                result.append(
                    {
                        name: row[name]
                        for name in (
                            "operation_id",
                            "kind",
                            "state",
                            "owner",
                            "fencing_token",
                            "created_ns",
                            "updated_ns",
                            "recovery_from",
                        )
                    }
                )
            return result
        finally:
            connection.close()


class LifecycleLease:
    """An acquired OS lease and its monotonic SQLite fencing token."""

    def __init__(
        self,
        *,
        authority: NativeLifecycleAuthority,
        owner: str,
        timeout_seconds: float,
    ) -> None:
        self.authority = authority
        self.owner = owner
        self.timeout_seconds = timeout_seconds
        self.fencing_token = -1
        self.recovery_report: tuple[RecoveryEntry, ...] = ()
        self._lock = _OSFileLock(authority.lock_path, timeout_seconds)
        self._connection: Optional[sqlite3.Connection] = None
        self._closed = True

    def _acquire(self) -> "LifecycleLease":
        if not (0.01 <= self.timeout_seconds <= 7 * 24 * 3600):
            raise ValueError("lease timeout is outside its finite supported range")
        self._lock.acquire()
        try:
            self._connection = self.authority._connect()
            committed_token = self.authority._reconcile_fence_anchor(self._connection)
            token = committed_token + 1
            if token <= 0 or token > SQLITE_SIGNED_COUNTER_MAX:
                raise LifecycleBoundError("lifecycle fencing token is exhausted")
            now = time.time_ns()
            expires = now + int(self.authority.limits.lease_seconds * 1_000_000_000)
            self.authority._write_fence_anchor(
                committed_token=committed_token,
                pending_token=token,
            )

            def acquire_row() -> tuple[RecoveryEntry, ...]:
                current = self._connection.execute(
                    "SELECT fencing_token FROM lease WHERE singleton=1"
                ).fetchone()
                if current is None:
                    raise LifecycleIntegrityError("lifecycle lease row is missing")
                if int(current[0]) != committed_token:
                    raise LifecycleRecoveryRequired(
                        "lifecycle fence changed during lease acquisition"
                    )
                self._connection.execute(
                    "UPDATE lease SET fencing_token=?,owner=?,pid=?,acquired_ns=?,heartbeat_ns=?,expires_ns=? "
                    "WHERE singleton=1",
                    (token, self.owner, os.getpid(), now, now, expires),
                )
                reports: list[RecoveryEntry] = []
                rows = self._connection.execute(
                    "SELECT * FROM operations "
                    "WHERE state NOT IN (?,?)",
                    tuple(TERMINAL_STATES),
                ).fetchall()
                for row in rows:
                    self.authority._validate_operation_row(row)
                    old_token = int(row["fencing_token"])
                    if old_token == token:
                        continue
                    old_state = str(row["state"])
                    recovery_from = (
                        str(row["recovery_from"])
                        if old_state == OperationState.RECOVERY_BLOCKED.value
                        and row["recovery_from"]
                        else old_state
                    )
                    updated = self.authority._operation_record_value(
                        row,
                        state=OperationState.RECOVERY_BLOCKED.value,
                        owner=self.owner,
                        fencing_token=token,
                        updated_ns=now,
                        recovery_from=recovery_from,
                    )
                    self._connection.execute(
                        "UPDATE operations SET state=?,owner=?,fencing_token=?,updated_ns=?,"
                        "recovery_from=?,record_hmac=? "
                        "WHERE operation_id=?",
                        (
                            updated["state"],
                            updated["owner"],
                            updated["fencing_token"],
                            updated["updated_ns"],
                            updated["recovery_from"],
                            updated["record_hmac"],
                            row["operation_id"],
                        ),
                    )
                    reports.append(
                        RecoveryEntry(
                            operation_id=str(row["operation_id"]),
                            previous_state=recovery_from,
                            current_state=OperationState.RECOVERY_BLOCKED.value,
                            previous_fencing_token=old_token,
                            current_fencing_token=token,
                            instruction=(
                                "No filesystem path was treated as proof. Explicitly resume the recorded "
                                "pre-crash state or move to ABORTING after verifying external evidence."
                            ),
                        )
                    )
                self.authority._refresh_operation_ledger_state(
                    self._connection, fencing_token=token
                )
                return tuple(reports)

            try:
                report = self.authority._transaction(self._connection, acquire_row)
            except BaseException:
                # If the DB transaction rolled back, this clears the pending token;
                # if SQLite committed before surfacing an error, it finalizes it.
                # Either outcome is derived from the two durable copies while the
                # root-scoped OS lock is still held.
                self.authority._reconcile_fence_anchor(self._connection)
                raise
            # This is deliberately after the SQLite commit. A crash here leaves a
            # signed {committed,pending} anchor that the next holder can reconcile
            # unambiguously against the DB token.
            self.authority._write_fence_anchor(
                committed_token=token,
                pending_token=None,
            )
            self.fencing_token = token
            self.recovery_report = report
            self._closed = False
            return self
        except BaseException:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._lock.release()
            raise

    def __enter__(self) -> "LifecycleLease":
        self._require_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _require_open(self) -> sqlite3.Connection:
        if self._closed or self._connection is None:
            raise LifecycleStateError("lifecycle lease is closed")
        return self._connection

    def _assert_fence(self) -> sqlite3.Row:
        self._lock.assert_held()
        self.authority._assert_roots_unchanged()
        connection = self._require_open()
        row = connection.execute("SELECT * FROM lease WHERE singleton=1").fetchone()
        if (
            row is None
            or str(row["owner"] or "") != self.owner
            or int(row["fencing_token"]) != self.fencing_token
            or int(row["pid"] or -1) != os.getpid()
        ):
            raise LifecycleStateError("lease fencing token is stale")
        self.authority._load_archive_head(connection, verify_rows=False)
        anchor = self.authority._check_fence_coherence(connection)
        if (
            int(anchor["committed_token"]) != self.fencing_token
            or anchor["pending_token"] is not None
        ):
            raise LifecycleRecoveryRequired(
                "lease token is not the committed external lifecycle fence"
            )
        return row

    def heartbeat(self) -> None:
        connection = self._require_open()
        now = time.time_ns()
        expires = now + int(self.authority.limits.lease_seconds * 1_000_000_000)

        def update() -> None:
            self._assert_fence()
            changed = connection.execute(
                "UPDATE lease SET heartbeat_ns=?,expires_ns=? WHERE singleton=1 AND owner=? "
                "AND fencing_token=? AND pid=?",
                (now, expires, self.owner, self.fencing_token, os.getpid()),
            ).rowcount
            if changed != 1:
                raise LifecycleStateError("lease heartbeat was fenced")

        self.authority._transaction(connection, update)

    def close(self) -> None:
        if self._closed:
            return
        connection = self._connection
        try:
            if connection is not None:
                now = time.time_ns()

                def release_row() -> None:
                    self._lock.assert_held()
                    row = connection.execute("SELECT * FROM lease WHERE singleton=1").fetchone()
                    if (
                        row is not None
                        and str(row["owner"] or "") == self.owner
                        and int(row["fencing_token"]) == self.fencing_token
                        and int(row["pid"] or -1) == os.getpid()
                    ):
                        connection.execute(
                            "UPDATE lease SET owner=NULL,pid=NULL,heartbeat_ns=?,expires_ns=? "
                            "WHERE singleton=1",
                            (now, now),
                        )

                self.authority._transaction(connection, release_row)
        finally:
            self._closed = True
            if connection is not None:
                connection.close()
            self._connection = None
            self._lock.release()

    def _operation_row(self, operation_id: str, *, require_fence: bool = True) -> sqlite3.Row:
        connection = self._require_open()
        operation_id = _safe_identifier(operation_id, label="operation_id")
        row = connection.execute(
            "SELECT * FROM operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if row is None:
            tombstone = connection.execute(
                "SELECT * FROM operation_tombstones WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if tombstone is not None:
                self._verify_operation_tombstone(tombstone)
                raise LifecycleStateError(
                    f"retired lifecycle operation cannot be reused: {operation_id}"
                )
            raise LifecycleStateError(f"unknown lifecycle operation: {operation_id}")
        self.authority._validate_operation_row(row)
        if require_fence and (
            str(row["owner"]) != self.owner
            or int(row["fencing_token"]) != self.fencing_token
        ):
            raise LifecycleStateError("operation belongs to a stale fencing token")
        return row

    def _verify_operation_tombstone(self, row: Mapping[str, Any]) -> None:
        self.authority._validate_operation_tombstone(row)

    def _compact_terminal_operations(self, connection: sqlite3.Connection) -> int:
        rows = connection.execute(
            "SELECT * FROM operations WHERE state IN (?,?) ORDER BY updated_ns,operation_id LIMIT ?",
            (*tuple(sorted(TERMINAL_STATES)), self.authority.limits.max_operations),
        ).fetchall()
        if not rows:
            return 0
        for row in rows:
            self.authority._validate_operation_row(row)
        estimated = sum(
            len(_canonical_bytes(dict(row))) + 512
            for row in rows
        )
        self.authority._preflight_db_growth(
            connection, estimated_bytes=estimated
        )
        now = time.time_ns()
        for offset, row in enumerate(rows):
            source_sha = _sha256_bytes(_canonical_bytes(dict(row)))
            body = {
                "schema_version": OPERATION_TOMBSTONE_SCHEMA,
                "authority_identity_sha256": self.authority.authority_id,
                "operation_id": str(row["operation_id"]),
                "source_record_sha256": source_sha,
                "retired_ns": now + offset,
                "fencing_token": self.fencing_token,
            }
            tombstone_sha = _sha256_bytes(_canonical_bytes(body))
            connection.execute(
                "INSERT INTO operation_tombstones VALUES(?,?,?,?,?,?)",
                (
                    body["operation_id"], source_sha, tombstone_sha,
                    body["retired_ns"], self.fencing_token,
                    self.authority._sign(
                        {**body, "tombstone_sha256": tombstone_sha}
                    ),
                ),
            )
            connection.execute(
                "DELETE FROM operations WHERE operation_id=?",
                (body["operation_id"],),
            )
        self.authority._refresh_archive_head(
            connection, fencing_token=self.fencing_token
        )
        self.authority._refresh_operation_ledger_state(
            connection, fencing_token=self.fencing_token
        )
        self.authority._load_archive_head(connection)
        return len(rows)

    @staticmethod
    def _materialized(row: sqlite3.Row) -> dict[str, Any]:
        try:
            operational = json.loads(str(row["materialized_operational_json"]))
        except (json.JSONDecodeError, TypeError) as exc:
            raise LifecycleIntegrityError("materialized operational vector is unreadable") from exc
        if not isinstance(operational, Mapping):
            raise LifecycleIntegrityError("materialized operational vector must be an object")
        normalized_operational: dict[str, dict[str, int]] = {}
        for domain, raw in operational.items():
            if not isinstance(raw, Mapping):
                raise LifecycleIntegrityError("materialized operational entry must be an object")
            normalized_operational[str(domain)] = {
                "peak": _nonnegative("operational peak", raw.get("peak", 0)),
                "final": _nonnegative("operational final", raw.get("final", 0)),
                "files": _nonnegative("operational files", raw.get("files", 0)),
            }
        return {
            "native_peak": int(row["materialized_native_peak_bytes"]),
            "native_final": int(row["materialized_native_final_bytes"]),
            "cold_peak": int(row["materialized_cold_peak_bytes"]),
            "cold_final": int(row["materialized_cold_final_bytes"]),
            "native_files": int(row["materialized_native_files"]),
            "cold_files": int(row["materialized_cold_files"]),
            "operational": normalized_operational,
        }

    def _capacity_check(
        self,
        budget: GrowthBudget,
        *,
        candidate_materialized: Optional[Mapping[str, int]] = None,
        exclude_operation_id: Optional[str] = None,
    ) -> None:
        limits = self.authority.limits
        budget_file_count = (
            budget.native_files_delta
            + budget.cold_files_delta
            + sum(item.files_delta for item in budget.operational)
        )
        if budget_file_count > limits.max_files_per_operation:
            raise LifecycleBoundError("operation file count exceeds max_files_per_operation")
        hot = _tree_usage(
            self.authority.native_root,
            max_entries=limits.max_scan_entries,
            include_runs=True,
        )
        cold = _tree_usage(
            self.authority.cold_root,
            max_entries=limits.max_scan_entries,
            include_runs=False,
        )
        projected_hot_bytes = hot.bytes
        projected_cold_bytes = cold.bytes
        projected_hot_files = hot.files
        projected_cold_files = cold.files
        projected_complete = hot.complete_runs
        projected_inflight = hot.inflight_runs
        connection = self._require_open()
        runtime_rows = connection.execute(
            "SELECT * FROM runtime_reservations WHERE status='ACTIVE'"
        ).fetchall()
        runtime_projections: list[
            tuple[sqlite3.Row, OperationalDomain, Path, StoreUsage]
        ] = []
        runtime_roots_by_domain: dict[str, list[Path]] = {
            name: [] for name in self.authority.operational_domains
        }
        for runtime_row in runtime_rows:
            domain_name = str(runtime_row["domain"])
            domain = self.authority.operational_domains.get(domain_name)
            if domain is None:
                raise LifecycleIntegrityError(
                    f"runtime reservation names unknown domain {domain_name}"
                )
            # This verifies the exact subroot identity and the persisted receipt
            # MAC before its capacity promise can influence another admission.
            self.authority._runtime_adapter_from_row(runtime_row)
            subroot = Path(str(runtime_row["operational_root"])).resolve(strict=False)
            if not _contains(domain.root, subroot) or subroot == domain.root:
                raise LifecycleIntegrityError("runtime reservation subroot escaped its domain")
            retained = _tree_usage(
                subroot,
                max_entries=limits.max_scan_entries,
                include_runs=False,
                # The rollback journal is transient and can reappear at its
                # full peak after this scan; it never consumes the retained
                # portion of the reservation.
                excluded_relative_files=("state_runtime.sqlite3-journal",),
            )
            observed = _tree_usage(
                subroot,
                max_entries=limits.max_scan_entries,
                include_runs=False,
            )
            reserved = int(runtime_row["reserved_peak_bytes"])
            if observed.bytes > reserved or observed.files > 3:
                raise LifecycleBoundError(
                    "runtime stream exceeded its exact DB/journal/lock reservation"
                )
            runtime_projections.append((runtime_row, domain, subroot, retained))
            runtime_roots_by_domain[domain_name].append(subroot)
        operational_usage = {
            name: _tree_usage(
                domain.root,
                max_entries=limits.max_scan_entries,
                include_runs=False,
                excluded_roots=runtime_roots_by_domain[name],
            )
            for name, domain in self.authority.operational_domains.items()
        }
        projected_operational_bytes = {
            name: usage.bytes for name, usage in operational_usage.items()
        }
        projected_operational_files = {
            name: usage.files for name, usage in operational_usage.items()
        }
        outstanding_by_volume: dict[str, int] = {}

        def add_outstanding(root: Path, amount: int) -> None:
            key = _volume_key(root)
            outstanding_by_volume[key] = outstanding_by_volume.get(key, 0) + max(0, amount)

        rows = connection.execute(
            "SELECT * FROM operations WHERE state NOT IN (?,?)",
            tuple(TERMINAL_STATES),
        ).fetchall()
        for row in rows:
            if exclude_operation_id and str(row["operation_id"]) == exclude_operation_id:
                continue
            self.authority._validate_operation_row(row)
            active_budget = _budget_from_row(row)
            materialized = self._materialized(row)
            projected_hot_bytes += max(
                0, active_budget.native_final_bytes - materialized["native_final"]
            )
            projected_cold_bytes += max(
                0, active_budget.cold_final_bytes - materialized["cold_final"]
            )
            projected_hot_files += max(
                0, active_budget.native_files_delta - materialized["native_files"]
            )
            projected_cold_files += max(
                0, active_budget.cold_files_delta - materialized["cold_files"]
            )
            projected_complete += active_budget.complete_run_delta
            projected_inflight += active_budget.inflight_run_delta
            for item in active_budget.operational:
                if item.domain not in self.authority.operational_domains:
                    raise LifecycleIntegrityError(
                        f"stored reservation names unknown operational domain {item.domain}"
                    )
                observed = materialized["operational"].get(
                    item.domain, {"peak": 0, "final": 0, "files": 0}
                )
                projected_operational_bytes[item.domain] += max(
                    0, item.final_bytes - observed["final"]
                )
                projected_operational_files[item.domain] += max(
                    0, item.files_delta - observed["files"]
                )
                add_outstanding(
                    self.authority.operational_domains[item.domain].root,
                    item.peak_bytes - observed["peak"],
                )
            add_outstanding(
                self.authority.native_root,
                active_budget.native_peak_bytes - materialized["native_peak"],
            )
            add_outstanding(
                self.authority.cold_root,
                active_budget.cold_peak_bytes - materialized["cold_peak"],
            )

        for runtime_row, domain, _subroot, retained in runtime_projections:
            domain_name = str(runtime_row["domain"])
            reserved = int(runtime_row["reserved_peak_bytes"])
            outstanding = max(0, reserved - retained.bytes)
            # Active runtime subroots were excluded from the domain scan, so
            # their contractual peak/file vector is added exactly once and is
            # independent of a concurrently appearing/disappearing journal.
            projected_operational_bytes[domain_name] += reserved
            # Runtime state owns one DB, one transient rollback journal, and
            # one lifetime writer lock.
            projected_operational_files[domain_name] += 3
            add_outstanding(domain.root, outstanding)

        materialized = {
            "native_peak": 0,
            "native_final": 0,
            "cold_peak": 0,
            "cold_final": 0,
            "native_files": 0,
            "cold_files": 0,
            "operational": {},
        }
        if candidate_materialized:
            for key, value in candidate_materialized.items():
                if key == "operational":
                    if not isinstance(value, Mapping):
                        raise LifecycleIntegrityError(
                            "candidate operational materialization must be an object"
                        )
                    materialized["operational"] = {
                        str(name): {
                            "peak": int(raw["peak"]),
                            "final": int(raw["final"]),
                            "files": int(raw["files"]),
                        }
                        for name, raw in value.items()
                    }
                else:
                    materialized[key] = int(value)
        projected_hot_bytes += max(0, budget.native_final_bytes - materialized["native_final"])
        projected_cold_bytes += max(0, budget.cold_final_bytes - materialized["cold_final"])
        projected_hot_files += max(0, budget.native_files_delta - materialized["native_files"])
        projected_cold_files += max(0, budget.cold_files_delta - materialized["cold_files"])
        projected_complete += budget.complete_run_delta
        projected_inflight += budget.inflight_run_delta
        for item in budget.operational:
            if item.domain not in self.authority.operational_domains:
                raise LifecycleBoundError(
                    f"growth budget names unconfigured operational domain {item.domain}"
                )
            observed = materialized["operational"].get(
                item.domain, {"peak": 0, "final": 0, "files": 0}
            )
            projected_operational_bytes[item.domain] += max(
                0, item.final_bytes - observed["final"]
            )
            projected_operational_files[item.domain] += max(
                0, item.files_delta - observed["files"]
            )
            add_outstanding(
                self.authority.operational_domains[item.domain].root,
                item.peak_bytes - observed["peak"],
            )
        add_outstanding(
            self.authority.native_root,
            budget.native_peak_bytes - materialized["native_peak"],
        )
        add_outstanding(
            self.authority.cold_root,
            budget.cold_peak_bytes - materialized["cold_peak"],
        )

        reasons: list[str] = []
        if projected_hot_bytes > limits.max_hot_bytes:
            reasons.append(
                f"projected hot bytes {projected_hot_bytes} exceed cap {limits.max_hot_bytes}"
            )
        if projected_cold_bytes > limits.max_cold_bytes:
            reasons.append(
                f"projected cold bytes {projected_cold_bytes} exceed cap {limits.max_cold_bytes}"
            )
        if projected_hot_files > limits.max_hot_files:
            reasons.append(
                f"projected hot files {projected_hot_files} exceed cap {limits.max_hot_files}"
            )
        if projected_cold_files > limits.max_cold_files:
            reasons.append(
                f"projected cold files {projected_cold_files} exceed cap {limits.max_cold_files}"
            )
        if projected_complete > limits.max_complete_hot_runs:
            reasons.append(
                f"projected complete runs {projected_complete} exceed cap "
                f"{limits.max_complete_hot_runs}"
            )
        if projected_inflight > limits.max_inflight_runs:
            reasons.append(
                f"projected inflight runs {projected_inflight} exceed cap {limits.max_inflight_runs}"
            )
        for name, domain in self.authority.operational_domains.items():
            if projected_operational_bytes[name] > domain.max_bytes:
                reasons.append(
                    f"projected {name} bytes {projected_operational_bytes[name]} exceed "
                    f"domain cap {domain.max_bytes}"
                )
            if projected_operational_files[name] > domain.max_files:
                reasons.append(
                    f"projected {name} files {projected_operational_files[name]} exceed "
                    f"domain cap {domain.max_files}"
                )

        # The current DB is already reflected in disk free.  Reserve all
        # remaining pages plus a worst-case rollback journal of one full DB;
        # journal_size_limit bounds post-transaction retention, not peak undo.
        db_size = self.authority.db_path.stat().st_size
        state_headroom = max(0, limits.max_db_bytes - db_size) + limits.max_db_bytes
        state_volume = _volume_key(self.authority.state_root)
        outstanding_by_volume[state_volume] = (
            outstanding_by_volume.get(state_volume, 0) + state_headroom
        )
        volume_paths = {
            _volume_key(self.authority.native_root): self.authority.native_root,
            _volume_key(self.authority.cold_root): self.authority.cold_root,
            state_volume: self.authority.state_root,
        }
        for domain in self.authority.operational_domains.values():
            volume_paths[_volume_key(domain.root)] = domain.root
        for key, path in volume_paths.items():
            free = int(shutil.disk_usage(_existing_ancestor(path)).free)
            projected_free = free - outstanding_by_volume.get(key, 0)
            if projected_free < limits.min_volume_free_bytes:
                reasons.append(
                    f"projected free bytes on {key} ({projected_free}) fall below reserve "
                    f"{limits.min_volume_free_bytes}"
                )
        if reasons:
            raise LifecycleBoundError("; ".join(reasons))

    def prepare_operation(
        self,
        operation_id: str,
        budget: GrowthBudget,
        *,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> ReservationSnapshot:
        connection = self._require_open()
        operation_id = _safe_identifier(operation_id, label="operation_id")
        if not isinstance(budget, GrowthBudget):
            raise TypeError("budget must be a GrowthBudget")
        payload_value: Mapping[str, Any] = {} if payload is None else payload
        if not isinstance(payload_value, Mapping):
            raise ValueError("operation payload must be an object")
        payload_bytes = _bounded_canonical_bytes(
            payload_value,
            max_bytes=self.authority.limits.max_payload_bytes,
            label="operation payload",
        )
        file_count = (
            budget.native_files_delta
            + budget.cold_files_delta
            + sum(item.files_delta for item in budget.operational)
        )
        if file_count > self.authority.limits.max_files_per_operation:
            raise LifecycleBoundError("operation file count exceeds max_files_per_operation")
        budget_value = asdict(budget)
        budget_bytes = _canonical_bytes(budget_value)
        self._capacity_check(budget)
        now = time.time_ns()
        value: dict[str, Any] = {
            "operation_id": operation_id,
            "kind": budget.kind,
            "state": OperationState.PREPARED.value,
            "owner": self.owner,
            "fencing_token": self.fencing_token,
            "created_ns": now,
            "updated_ns": now,
            "payload_json": payload_bytes.decode("utf-8"),
            "payload_sha256": _sha256_bytes(payload_bytes),
            "payload_bytes": len(payload_bytes),
            "file_count": file_count,
            "budget_json": budget_bytes.decode("utf-8"),
            "budget_sha256": _sha256_bytes(budget_bytes),
            "materialized_native_peak_bytes": 0,
            "materialized_native_final_bytes": 0,
            "materialized_cold_peak_bytes": 0,
            "materialized_cold_final_bytes": 0,
            "materialized_native_files": 0,
            "materialized_cold_files": 0,
            "materialized_operational_json": "{}",
            "recovery_from": None,
            "pointer_receipt_json": None,
            "pointer_receipt_hmac": None,
            "record_hmac": "",
        }
        value["record_hmac"] = self.authority._sign(
            self.authority._operation_signed_fields(value)
        )

        def insert() -> None:
            self._assert_fence()
            retired = connection.execute(
                "SELECT * FROM operation_tombstones WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if retired is not None:
                self._verify_operation_tombstone(retired)
                raise LifecycleStateError(
                    f"retired operation_id cannot be reused: {operation_id}"
                )
            ledger = self.authority._load_operation_ledger_state(connection)
            total = int(ledger["total_count"])
            active = int(ledger["active_count"])
            if total >= self.authority.limits.max_operations:
                self._compact_terminal_operations(connection)
                ledger = self.authority._load_operation_ledger_state(connection)
                total = int(ledger["total_count"])
                active = int(ledger["active_count"])
            if total >= self.authority.limits.max_operations:
                raise LifecycleBoundError(
                    "operation live-row cap reached with no terminal records to compact"
                )
            if active >= self.authority.limits.max_active_operations:
                raise LifecycleBoundError("active operation cap reached before reservation")
            try:
                connection.execute(
                    """INSERT INTO operations(
                           operation_id,kind,state,owner,fencing_token,created_ns,updated_ns,
                           payload_json,payload_sha256,payload_bytes,file_count,budget_json,
                           budget_sha256,materialized_native_peak_bytes,
                           materialized_native_final_bytes,materialized_cold_peak_bytes,
                           materialized_cold_final_bytes,materialized_native_files,
                           materialized_cold_files,materialized_operational_json,recovery_from,
                           pointer_receipt_json,pointer_receipt_hmac,record_hmac
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    tuple(value[name] for name in value),
                )
            except sqlite3.IntegrityError as exc:
                raise LifecycleStateError(f"duplicate operation_id: {operation_id}") from exc
            self.authority._refresh_operation_ledger_state(
                connection, fencing_token=self.fencing_token
            )

        self.authority._transaction(connection, insert)
        return self.reservation(operation_id)

    def reservation(self, operation_id: str) -> ReservationSnapshot:
        row = self._operation_row(operation_id)
        budget = _budget_from_row(row)
        materialized = self._materialized(row)
        return ReservationSnapshot(
            operation_id=str(row["operation_id"]),
            state=str(row["state"]),
            budget=budget,
            materialized_native_peak_bytes=int(row["materialized_native_peak_bytes"]),
            materialized_native_final_bytes=int(row["materialized_native_final_bytes"]),
            materialized_cold_peak_bytes=int(row["materialized_cold_peak_bytes"]),
            materialized_cold_final_bytes=int(row["materialized_cold_final_bytes"]),
            materialized_native_files=int(row["materialized_native_files"]),
            materialized_cold_files=int(row["materialized_cold_files"]),
            materialized_operational=materialized["operational"],
        )

    def _compact_released_runtime_rows(
        self, connection: sqlite3.Connection
    ) -> int:
        rows = connection.execute(
            "SELECT * FROM runtime_reservations WHERE status='RELEASED' "
            "ORDER BY updated_ns,reservation_id LIMIT ?",
            (self.authority.limits.max_operations,),
        ).fetchall()
        if not rows:
            return 0
        self.authority._preflight_db_growth(
            connection, estimated_bytes=sum(1536 for _ in rows)
        )
        now = time.time_ns()
        for offset, row in enumerate(rows):
            reservation = self.authority._runtime_adapter_from_row(row)
            witness_row = connection.execute(
                "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
                (row["reservation_id"],),
            ).fetchone()
            if witness_row is None:
                raise LifecycleIntegrityError(
                    "released runtime lost its monotonic witness"
                )
            witness = self.authority._witness_from_row(witness_row)
            binding = connection.execute(
                "SELECT * FROM runtime_account_bindings WHERE reservation_id=?",
                (row["reservation_id"],),
            ).fetchone()
            account_scope: Optional[str] = None
            if binding is not None:
                self.authority._validate_runtime_account_binding(binding)
                account_scope = str(binding["account_scope_sha256"])
            claim_head = connection.execute(
                "SELECT * FROM runtime_claim_heads WHERE reservation_id=?",
                (row["reservation_id"],),
            ).fetchone()
            claim_head_sha = GENESIS_SHA256
            max_operation_sequence = 0
            retired_operation_floor = 0
            if claim_head is not None:
                self.authority._validate_runtime_claim_head(claim_head)
                if account_scope is None:
                    raise LifecycleIntegrityError(
                        "released runtime claim head lacks an account binding"
                    )
                if (
                    str(claim_head["account_scope_sha256"]) != account_scope
                    or str(claim_head["stream_identity_sha256"])
                    != str(row["stream_identity_sha256"])
                ):
                    raise LifecycleIntegrityError(
                        "released runtime claim head/binding mismatch"
                    )
                max_operation_sequence = int(
                    witness.max_order_operation_sequence
                )
                if int(claim_head["max_operation_sequence"]) > max_operation_sequence:
                    raise LifecycleIntegrityError(
                        "runtime claim head exceeds its external order witness"
                    )
                # A released runtime can never originate another local
                # operation, so its witnessed operation high-water is final.
                retired_operation_floor = max_operation_sequence
                claim_head_sha = _sha256_bytes(
                    _canonical_bytes(
                        {
                            **self.authority._runtime_claim_head_fields(
                                claim_head
                            ),
                            "record_hmac": str(claim_head["record_hmac"]),
                        }
                    )
                )
            elif account_scope is not None:
                raise LifecycleIntegrityError(
                    "released bound runtime lost its broker claim head"
                )
            reservation_sha = _sha256_bytes(
                _canonical_bytes(reservation.canonical())
            )
            witness_sha = _sha256_bytes(_canonical_bytes(witness.canonical()))
            value: dict[str, Any] = {
                "reservation_id": str(row["reservation_id"]),
                "operational_root": str(row["operational_root"]),
                "stream_identity_sha256": str(row["stream_identity_sha256"]),
                "account_scope_sha256": account_scope,
                "reservation_sha256": reservation_sha,
                "witness_sha256": witness_sha,
                "runtime_claim_head_sha256": claim_head_sha,
                "max_operation_sequence": max_operation_sequence,
                "retired_operation_floor": retired_operation_floor,
                "retired_ns": now + offset,
                "fencing_token": self.fencing_token,
            }
            tombstone_sha = _sha256_bytes(
                _canonical_bytes(
                    {
                        "schema_version": RUNTIME_TOMBSTONE_SCHEMA,
                        "authority_identity_sha256": self.authority.authority_id,
                        **value,
                    }
                )
            )
            fields = {
                "schema_version": RUNTIME_TOMBSTONE_SCHEMA,
                "authority_identity_sha256": self.authority.authority_id,
                **value,
            }
            connection.execute(
                "INSERT INTO runtime_tombstones VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    value["reservation_id"], value["operational_root"],
                    value["stream_identity_sha256"], account_scope,
                    reservation_sha, witness_sha, claim_head_sha,
                    max_operation_sequence, retired_operation_floor,
                    tombstone_sha,
                    value["retired_ns"], self.fencing_token,
                    self.authority._sign(
                        {**fields, "tombstone_sha256": tombstone_sha}
                    ),
                ),
            )
            connection.execute(
                "DELETE FROM runtime_account_bindings WHERE reservation_id=?",
                (value["reservation_id"],),
            )
            connection.execute(
                "DELETE FROM runtime_claim_heads WHERE reservation_id=?",
                (value["reservation_id"],),
            )
            connection.execute(
                "DELETE FROM runtime_witnesses WHERE reservation_id=?",
                (value["reservation_id"],),
            )
            connection.execute(
                "DELETE FROM runtime_reservations WHERE reservation_id=?",
                (value["reservation_id"],),
            )
        self.authority._refresh_archive_head(
            connection, fencing_token=self.fencing_token
        )
        return len(rows)

    def _reserve_runtime_storage(
        self,
        *,
        domain_name: str,
        domain: OperationalDomain,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> RuntimeStorageReservation:
        connection = self._require_open()
        self._assert_fence()
        canonical_root = os.path.normcase(str(operational_root))
        archived = connection.execute(
            "SELECT * FROM runtime_tombstones WHERE operational_root=? "
            "OR stream_identity_sha256=?",
            (canonical_root, stream_identity_sha256),
        ).fetchall()
        if archived:
            for tombstone in archived:
                self.authority._validate_runtime_tombstone(tombstone)
            raise LifecycleIntegrityError(
                "retired runtime root/stream binding cannot be reused"
            )
        existing = connection.execute(
            "SELECT * FROM runtime_reservations WHERE operational_root=? "
            "OR stream_identity_sha256=?",
            (canonical_root, stream_identity_sha256),
        ).fetchall()
        if len(existing) > 1:
            raise LifecycleIntegrityError("runtime root/stream uniqueness is inconsistent")
        if existing:
            row = existing[0]
            if (
                str(row["operational_root"]) != canonical_root
                or str(row["stream_identity_sha256"]) != stream_identity_sha256
                or str(row["domain"]) != domain_name
            ):
                raise LifecycleIntegrityError(
                    "runtime root or stream identity is already bound to another reservation"
                )
            if str(row["status"]) != "ACTIVE":
                raise LifecycleStateError(
                    "released runtime reservation cannot be silently resurrected"
                )
            current_reserved = int(row["reserved_peak_bytes"])
            if requested_peak_bytes <= current_reserved:
                return self.authority._runtime_adapter_from_row(row)
            delta = requested_peak_bytes - current_reserved
            self._capacity_check(
                GrowthBudget.operational_growth(
                    kind=OperationKind.RUNTIME_STATE,
                    domain=domain_name,
                    final_bytes=delta,
                    peak_bytes=delta,
                    file_count=0,
                )
            )
            now = time.time_ns()
            updated = dict(row)
            updated.update(
                {
                    "reserved_peak_bytes": requested_peak_bytes,
                    "fencing_token": self.fencing_token,
                    "updated_ns": now,
                }
            )
            updated["receipt_mac"] = self.authority._sign(
                self.authority._runtime_signed_fields(updated)
            )

            def top_up() -> None:
                self._assert_fence()
                changed = connection.execute(
                    "UPDATE runtime_reservations SET reserved_peak_bytes=?,fencing_token=?,"
                    "updated_ns=?,receipt_mac=? WHERE reservation_id=? AND status='ACTIVE' "
                    "AND reserved_peak_bytes=?",
                    (
                        requested_peak_bytes,
                        self.fencing_token,
                        now,
                        updated["receipt_mac"],
                        row["reservation_id"],
                        current_reserved,
                    ),
                ).rowcount
                if changed != 1:
                    raise LifecycleStateError("runtime reservation changed during top-up")

            self.authority._transaction(connection, top_up)
            fresh = connection.execute(
                "SELECT * FROM runtime_reservations WHERE reservation_id=?",
                (row["reservation_id"],),
            ).fetchone()
            return self.authority._runtime_adapter_from_row(fresh)

        # A nested stream root would make domain accounting and ownership
        # ambiguous. Released bindings remain tombstoned, so a later stream
        # cannot capture either an ancestor or descendant of their evidence.
        for bound_row in connection.execute(
            "SELECT operational_root FROM runtime_reservations"
        ).fetchall():
            bound_root = Path(str(bound_row["operational_root"])).resolve(strict=False)
            if _contains(bound_root, operational_root) or _contains(
                operational_root, bound_root
            ):
                raise LifecycleIntegrityError(
                    "runtime stream roots cannot overlap an existing binding"
                )
        for bound_row in connection.execute(
            "SELECT operational_root FROM runtime_tombstones"
        ).fetchall():
            bound_root = Path(str(bound_row["operational_root"])).resolve(strict=False)
            if _contains(bound_root, operational_root) or _contains(
                operational_root, bound_root
            ):
                raise LifecycleIntegrityError(
                    "runtime stream roots cannot overlap a retired binding"
                )

        active_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM runtime_reservations WHERE domain=? AND status='ACTIVE'",
                (domain_name,),
            ).fetchone()[0]
        )
        total_count = int(
            connection.execute("SELECT COUNT(*) FROM runtime_reservations").fetchone()[0]
        )
        if active_count >= domain.max_active_reservations:
            raise LifecycleBoundError("runtime domain active-stream cardinality cap reached")
        if total_count >= self.authority.limits.max_operations:
            self.authority._transaction(
                connection,
                lambda: self._compact_released_runtime_rows(connection),
            )
            total_count = int(
                connection.execute("SELECT COUNT(*) FROM runtime_reservations").fetchone()[0]
            )
        if total_count >= self.authority.limits.max_operations:
            raise LifecycleBoundError(
                "runtime live-row cap reached with no released witness to compact"
            )
        usage = _tree_usage(
            operational_root,
            max_entries=self.authority.limits.max_scan_entries,
            include_runs=False,
            excluded_relative_files=("state_runtime.sqlite3-journal",),
        )
        if usage.bytes > requested_peak_bytes:
            raise LifecycleBoundError("runtime root already exceeds its requested peak")
        remaining_bytes = requested_peak_bytes - usage.bytes
        remaining_files = max(0, 3 - usage.files)
        self._capacity_check(
            GrowthBudget.operational_growth(
                kind=OperationKind.RUNTIME_STATE,
                domain=domain_name,
                final_bytes=remaining_bytes,
                peak_bytes=remaining_bytes,
                file_count=remaining_files,
            )
        )
        reservation_id = "runtime-" + _sha256_bytes(
            _canonical_bytes(
                {
                    "authority_id": self.authority.authority_id,
                    "domain": domain_name,
                    "operational_root": canonical_root,
                    "stream_identity_sha256": stream_identity_sha256,
                }
            )
        )[:48]
        now = time.time_ns()
        value: dict[str, Any] = {
            "reservation_id": reservation_id,
            "domain": domain_name,
            "operational_root": canonical_root,
            "root_device": str(_filesystem_identity(operational_root)["device"]),
            "root_inode": str(_filesystem_identity(operational_root)["inode"]),
            "stream_identity_sha256": stream_identity_sha256,
            "reserved_peak_bytes": requested_peak_bytes,
            "institution_cap_bytes": self.authority.institution_cap_bytes,
            "admitted_streams_cap": domain.max_active_reservations,
            "fencing_token": self.fencing_token,
            "status": "ACTIVE",
            "created_ns": now,
            "updated_ns": now,
            "receipt_mac": "",
        }
        value["receipt_mac"] = self.authority._sign(
            self.authority._runtime_signed_fields(value)
        )
        witness: dict[str, Any] = {
            "reservation_id": reservation_id,
            "witness_version": 0,
            "writer_epoch": 0,
            "reducer_contract_sha256": GENESIS_SHA256,
            "max_bar_sequence": 0,
            "bar_chain_sha256": GENESIS_SHA256,
            "checkpoint_generation": 0,
            "checkpoint_sha256": GENESIS_SHA256,
            "max_order_operation_sequence": 0,
            "max_order_event_sequence": 0,
            "order_chain_sha256": GENESIS_SHA256,
            "fencing_token": self.fencing_token,
            "updated_ns": now,
            "record_hmac": "",
        }
        witness["record_hmac"] = self.authority._sign(
            self.authority._witness_signed_fields(witness)
        )

        def insert() -> None:
            self._assert_fence()
            try:
                connection.execute(
                    """INSERT INTO runtime_reservations(
                           reservation_id,domain,operational_root,root_device,root_inode,
                           stream_identity_sha256,
                           reserved_peak_bytes,institution_cap_bytes,admitted_streams_cap,
                           fencing_token,status,created_ns,updated_ns,receipt_mac
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    tuple(
                        value[key]
                        for key in (
                            "reservation_id",
                            "domain",
                            "operational_root",
                            "root_device",
                            "root_inode",
                            "stream_identity_sha256",
                            "reserved_peak_bytes",
                            "institution_cap_bytes",
                            "admitted_streams_cap",
                            "fencing_token",
                            "status",
                            "created_ns",
                            "updated_ns",
                            "receipt_mac",
                        )
                    ),
                )
                connection.execute(
                    """INSERT INTO runtime_witnesses(
                           reservation_id,witness_version,writer_epoch,reducer_contract_sha256,
                           max_bar_sequence,
                           bar_chain_sha256,checkpoint_generation,checkpoint_sha256,
                           max_order_operation_sequence,max_order_event_sequence,
                           order_chain_sha256,fencing_token,updated_ns,record_hmac
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    tuple(
                        witness[key]
                        for key in (
                            "reservation_id",
                            "witness_version",
                            "writer_epoch",
                            "reducer_contract_sha256",
                            "max_bar_sequence",
                            "bar_chain_sha256",
                            "checkpoint_generation",
                            "checkpoint_sha256",
                            "max_order_operation_sequence",
                            "max_order_event_sequence",
                            "order_chain_sha256",
                            "fencing_token",
                            "updated_ns",
                            "record_hmac",
                        )
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise LifecycleIntegrityError(
                    "runtime root/stream was concurrently bound"
                ) from exc

        self.authority._transaction(connection, insert)
        row = connection.execute(
            "SELECT * FROM runtime_reservations WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        return self.authority._runtime_adapter_from_row(row)

    def _validated_runtime_reservation_row(self, reservation: Any) -> sqlite3.Row:
        connection = self._require_open()
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        row = connection.execute(
            "SELECT * FROM runtime_reservations WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if row is None or str(row["status"]) != "ACTIVE":
            raise LifecycleStateError("runtime reservation is absent or inactive")
        expected = self.authority._runtime_adapter_from_row(row)
        supplied = {
            name: getattr(reservation, name, None)
            for name in expected.canonical()
        }
        if supplied != expected.canonical():
            raise LifecycleIntegrityError("runtime reservation receipt is stale or forged")
        return row

    def _validated_runtime_claim_context(
        self, reservation: Any
    ) -> Mapping[str, Any]:
        """Resolve a claim verifier against an active, released, or retired stream."""

        connection = self._require_open()
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")),
            label="reservation_id",
        )
        row = connection.execute(
            "SELECT * FROM runtime_reservations WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if row is not None:
            expected = self.authority._runtime_adapter_from_row(row)
            status = str(row["status"])
            if status == "ACTIVE":
                supplied = {
                    name: getattr(reservation, name, None)
                    for name in expected.canonical()
                }
                if supplied != expected.canonical():
                    raise LifecycleIntegrityError(
                        "runtime reservation receipt is stale or forged"
                    )
            elif status == "RELEASED":
                if (
                    getattr(reservation, "authority_identity_sha256", None)
                    != self.authority.authority_id
                    or getattr(reservation, "reservation_id", None)
                    != reservation_id
                    or getattr(reservation, "stream_identity_sha256", None)
                    != str(row["stream_identity_sha256"])
                ):
                    raise LifecycleIntegrityError(
                        "released runtime claim context is stale or forged"
                    )
            else:
                raise LifecycleIntegrityError(
                    "runtime claim context has an invalid status"
                )
            return {
                "status": status,
                "reservation_id": reservation_id,
                "stream_identity_sha256": str(row["stream_identity_sha256"]),
                "tombstone": None,
            }
        tombstone = connection.execute(
            "SELECT * FROM runtime_tombstones WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if tombstone is None:
            raise LifecycleIntegrityError("runtime claim context is absent")
        self.authority._validate_runtime_tombstone(tombstone)
        if (
            getattr(reservation, "authority_identity_sha256", None)
            != self.authority.authority_id
            or getattr(reservation, "reservation_id", None) != reservation_id
            or getattr(reservation, "stream_identity_sha256", None)
            != str(tombstone["stream_identity_sha256"])
        ):
            raise LifecycleIntegrityError(
                "retired runtime claim context is stale or forged"
            )
        return {
            "status": "RETIRED",
            "reservation_id": reservation_id,
            "stream_identity_sha256": str(
                tombstone["stream_identity_sha256"]
            ),
            "tombstone": tombstone,
        }

    def _read_runtime_witness(self, reservation: Any) -> RuntimeMonotonicWitness:
        connection = self._require_open()
        self._assert_fence()
        runtime = self._validated_runtime_reservation_row(reservation)
        row = connection.execute(
            "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
            (runtime["reservation_id"],),
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("runtime monotonic witness is missing")
        return self.authority._witness_from_row(row)

    def _cas_runtime_witness(
        self,
        reservation: Any,
        *,
        expected_witness: Any,
        proposed: RuntimeWitnessProposal,
    ) -> RuntimeMonotonicWitness:
        connection = self._require_open()
        self._assert_fence()
        runtime = self._validated_runtime_reservation_row(reservation)
        row = connection.execute(
            "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
            (runtime["reservation_id"],),
        ).fetchone()
        if row is None:
            raise LifecycleIntegrityError("runtime monotonic witness is missing")
        current = self.authority._witness_from_row(row)
        expected_fields = current.canonical()
        supplied_expected = {
            name: getattr(expected_witness, name, None) for name in expected_fields
        }
        if isinstance(expected_witness, Mapping):
            supplied_expected = dict(expected_witness)
        if supplied_expected != expected_fields:
            raise LifecycleCASMismatch(
                "runtime monotonic witness CAS expected value is stale or forged"
            )
        proposal = proposed.canonical()
        old = {
            "writer_epoch": current.writer_epoch,
            "reducer_contract_sha256": current.reducer_contract_sha256,
            "max_bar_sequence": current.max_bar_sequence,
            "bar_chain_sha256": current.bar_chain_sha256,
            "checkpoint_generation": current.checkpoint_generation,
            "checkpoint_sha256": current.checkpoint_sha256,
            "max_order_operation_sequence": current.max_order_operation_sequence,
            "max_order_event_sequence": current.max_order_event_sequence,
            "order_chain_sha256": current.order_chain_sha256,
        }
        for counter in (
            "writer_epoch",
            "max_bar_sequence",
            "checkpoint_generation",
            "max_order_operation_sequence",
            "max_order_event_sequence",
        ):
            if int(proposal[counter]) < int(old[counter]):
                raise LifecycleCASMismatch(f"runtime witness counter regressed: {counter}")
        if old["reducer_contract_sha256"] != GENESIS_SHA256:
            if proposal["reducer_contract_sha256"] != old["reducer_contract_sha256"]:
                raise LifecycleCASMismatch("runtime reducer contract digest is immutable")
        elif proposal["writer_epoch"] > 0 and proposal["reducer_contract_sha256"] == GENESIS_SHA256:
            raise LifecycleCASMismatch(
                "first runtime writer epoch must bind a reducer contract digest"
            )
        elif (
            proposal["reducer_contract_sha256"] != GENESIS_SHA256
            and proposal["writer_epoch"] == old["writer_epoch"]
        ):
            raise LifecycleCASMismatch(
                "runtime reducer contract can only bind with a new writer epoch"
            )
        for counter, digest_name in (
            ("max_bar_sequence", "bar_chain_sha256"),
            ("checkpoint_generation", "checkpoint_sha256"),
        ):
            if int(proposal[counter]) == int(old[counter]):
                if proposal[digest_name] != old[digest_name]:
                    raise LifecycleCASMismatch(
                        f"runtime witness hash changed without advancing {counter}"
                    )
            elif proposal[digest_name] == old[digest_name]:
                raise LifecycleCASMismatch(
                    f"runtime witness {counter} advanced without a new hash"
                )
        order_advanced = (
            proposal["max_order_operation_sequence"]
            > old["max_order_operation_sequence"]
            or proposal["max_order_event_sequence"] > old["max_order_event_sequence"]
        )
        if order_advanced and proposal["order_chain_sha256"] == old["order_chain_sha256"]:
            raise LifecycleCASMismatch("runtime order counters advanced without a new hash")
        if not order_advanced and proposal["order_chain_sha256"] != old["order_chain_sha256"]:
            raise LifecycleCASMismatch("runtime order hash changed without advancing counters")
        if proposal == old:
            raise LifecycleCASMismatch("runtime witness no-op CAS is forbidden")
        now = time.time_ns()
        updated: dict[str, Any] = {
            "reservation_id": current.reservation_id,
            "witness_version": current.witness_version + 1,
            **proposal,
            "fencing_token": self.fencing_token,
            "updated_ns": now,
            "record_hmac": "",
        }
        if updated["witness_version"] > SQLITE_SIGNED_COUNTER_MAX:
            raise LifecycleBoundError("runtime witness version is exhausted")
        updated["record_hmac"] = self.authority._sign(
            self.authority._witness_signed_fields(updated)
        )

        def update() -> None:
            self._assert_fence()
            changed = connection.execute(
                """UPDATE runtime_witnesses SET
                       witness_version=?,writer_epoch=?,reducer_contract_sha256=?,
                       max_bar_sequence=?,bar_chain_sha256=?,
                       checkpoint_generation=?,checkpoint_sha256=?,
                       max_order_operation_sequence=?,max_order_event_sequence=?,
                       order_chain_sha256=?,fencing_token=?,updated_ns=?,record_hmac=?
                   WHERE reservation_id=? AND witness_version=? AND record_hmac=?""",
                (
                    updated["witness_version"],
                    updated["writer_epoch"],
                    updated["reducer_contract_sha256"],
                    updated["max_bar_sequence"],
                    updated["bar_chain_sha256"],
                    updated["checkpoint_generation"],
                    updated["checkpoint_sha256"],
                    updated["max_order_operation_sequence"],
                    updated["max_order_event_sequence"],
                    updated["order_chain_sha256"],
                    updated["fencing_token"],
                    updated["updated_ns"],
                    updated["record_hmac"],
                    updated["reservation_id"],
                    current.witness_version,
                    current.receipt_mac,
                ),
            ).rowcount
            if changed != 1:
                raise LifecycleCASMismatch("runtime witness CAS lost a concurrent update")

        self.authority._transaction(connection, update)
        fresh = connection.execute(
            "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
            (updated["reservation_id"],),
        ).fetchone()
        return self.authority._witness_from_row(fresh)

    def _validated_account_binding(
        self,
        *,
        reservation_id: str,
        stream_identity_sha256: str,
        account_scope_sha256: str,
    ) -> Optional[sqlite3.Row]:
        connection = self._require_open()
        row = connection.execute(
            "SELECT * FROM runtime_account_bindings WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if row is None:
            return None
        self.authority._validate_runtime_account_binding(row)
        if (
            str(row["stream_identity_sha256"]) != stream_identity_sha256
            or str(row["account_scope_sha256"]) != account_scope_sha256
        ):
            raise LifecycleIntegrityError(
                "runtime stream is already bound to a different broker account scope"
            )
        return row

    @staticmethod
    def _validate_monotonic_operation_identity(
        stream_identity_sha256: str,
        operation_id: str,
        operation_sequence: int,
    ) -> int:
        parts = str(operation_id).split(":")
        try:
            writer_epoch = int(parts[1])
            encoded_sequence = int(parts[2])
        except (IndexError, ValueError) as exc:
            raise LifecycleIntegrityError(
                "broker operation_id is not a monotonic runtime identity"
            ) from exc
        if (
            len(parts) != 3
            or parts[0] != stream_identity_sha256[:32]
            or writer_epoch <= 0
            or encoded_sequence != operation_sequence
        ):
            raise LifecycleIntegrityError(
                "broker operation_id/sequence/stream binding is not canonical"
            )
        return writer_epoch

    def _compact_recent_broker_window(
        self, connection: sqlite3.Connection, *, rows_to_free: int
    ) -> int:
        removed = 0
        while removed < rows_to_free:
            row = connection.execute(
                """SELECT c.*,
                          a.claim_count AS _a_claim_count,
                          a.last_claim_sequence AS _a_last_claim_sequence,
                          a.last_claim_sha256 AS _a_last_claim_sha256,
                          a.external_highwater_sequence AS _a_external_highwater_sequence,
                          a.external_receipt_sha256 AS _a_external_receipt_sha256,
                          a.retired_execution_floor AS _a_retired_execution_floor,
                          a.fencing_token AS _a_fencing_token,
                          a.updated_ns AS _a_updated_ns,
                          a.record_hmac AS _a_record_hmac,
                          r.reservation_id AS _r_reservation_id,
                          r.account_scope_sha256 AS _r_account_scope_sha256,
                          r.stream_identity_sha256 AS _r_stream_identity_sha256,
                          r.max_operation_sequence AS _r_max_operation_sequence,
                          r.retired_operation_floor AS _r_retired_operation_floor,
                          r.fencing_token AS _r_fencing_token,
                          r.updated_ns AS _r_updated_ns,
                          r.record_hmac AS _r_record_hmac,
                          t.reservation_id AS _t_reservation_id,
                          t.operational_root AS _t_operational_root,
                          t.stream_identity_sha256 AS _t_stream_identity_sha256,
                          t.account_scope_sha256 AS _t_account_scope_sha256,
                          t.reservation_sha256 AS _t_reservation_sha256,
                          t.witness_sha256 AS _t_witness_sha256,
                          t.runtime_claim_head_sha256 AS _t_runtime_claim_head_sha256,
                          t.max_operation_sequence AS _t_max_operation_sequence,
                          t.retired_operation_floor AS _t_retired_operation_floor,
                          t.tombstone_sha256 AS _t_tombstone_sha256,
                          t.retired_ns AS _t_retired_ns,
                          t.fencing_token AS _t_fencing_token,
                          t.record_hmac AS _t_record_hmac
                   FROM broker_execution_claims AS c
                   JOIN broker_account_heads AS a
                     ON a.account_scope_sha256=c.account_scope_sha256
                   LEFT JOIN runtime_claim_heads AS r
                     ON r.account_scope_sha256=c.account_scope_sha256
                    AND r.stream_identity_sha256=c.stream_identity_sha256
                   LEFT JOIN runtime_tombstones AS t
                     ON t.account_scope_sha256=c.account_scope_sha256
                    AND t.stream_identity_sha256=c.stream_identity_sha256
                   WHERE c.broker_execution_sequence<=a.retired_execution_floor
                     AND (
                       (r.reservation_id IS NOT NULL AND t.reservation_id IS NULL
                        AND c.operation_sequence<=r.retired_operation_floor)
                       OR
                       (t.reservation_id IS NOT NULL AND r.reservation_id IS NULL
                        AND c.operation_sequence<=t.retired_operation_floor)
                     )
                   ORDER BY c.created_ns,c.claim_id LIMIT 1"""
            ).fetchone()
            if row is None:
                break
            self.authority._broker_claim_from_row(row)
            account_head = {
                "account_scope_sha256": row["account_scope_sha256"],
                "claim_count": row["_a_claim_count"],
                "last_claim_sequence": row["_a_last_claim_sequence"],
                "last_claim_sha256": row["_a_last_claim_sha256"],
                "external_highwater_sequence": row[
                    "_a_external_highwater_sequence"
                ],
                "external_receipt_sha256": row[
                    "_a_external_receipt_sha256"
                ],
                "retired_execution_floor": row[
                    "_a_retired_execution_floor"
                ],
                "fencing_token": row["_a_fencing_token"],
                "updated_ns": row["_a_updated_ns"],
                "record_hmac": row["_a_record_hmac"],
            }
            self.authority._validate_broker_account_head(account_head)
            if int(row["broker_execution_sequence"]) > int(
                account_head["retired_execution_floor"]
            ):
                raise LifecycleIntegrityError(
                    "broker compaction selected an unretired execution"
                )
            runtime_head = (
                None
                if row["_r_reservation_id"] is None
                else {
                    "reservation_id": row["_r_reservation_id"],
                    "account_scope_sha256": row["_r_account_scope_sha256"],
                    "stream_identity_sha256": row[
                        "_r_stream_identity_sha256"
                    ],
                    "max_operation_sequence": row[
                        "_r_max_operation_sequence"
                    ],
                    "retired_operation_floor": row[
                        "_r_retired_operation_floor"
                    ],
                    "fencing_token": row["_r_fencing_token"],
                    "updated_ns": row["_r_updated_ns"],
                    "record_hmac": row["_r_record_hmac"],
                }
            )
            runtime_tombstone = (
                None
                if row["_t_reservation_id"] is None
                else {
                    "reservation_id": row["_t_reservation_id"],
                    "operational_root": row["_t_operational_root"],
                    "stream_identity_sha256": row[
                        "_t_stream_identity_sha256"
                    ],
                    "account_scope_sha256": row[
                        "_t_account_scope_sha256"
                    ],
                    "reservation_sha256": row["_t_reservation_sha256"],
                    "witness_sha256": row["_t_witness_sha256"],
                    "runtime_claim_head_sha256": row[
                        "_t_runtime_claim_head_sha256"
                    ],
                    "max_operation_sequence": row[
                        "_t_max_operation_sequence"
                    ],
                    "retired_operation_floor": row[
                        "_t_retired_operation_floor"
                    ],
                    "tombstone_sha256": row["_t_tombstone_sha256"],
                    "retired_ns": row["_t_retired_ns"],
                    "fencing_token": row["_t_fencing_token"],
                    "record_hmac": row["_t_record_hmac"],
                }
            )
            if (runtime_head is None) == (runtime_tombstone is None):
                raise LifecycleIntegrityError(
                    "broker claim has ambiguous runtime retirement authority"
                )
            if runtime_head is not None:
                self.authority._validate_runtime_claim_head(runtime_head)
                floor = int(runtime_head["retired_operation_floor"])
            else:
                self.authority._validate_runtime_tombstone(runtime_tombstone)
                floor = int(runtime_tombstone["retired_operation_floor"])
            if int(row["operation_sequence"]) > floor:
                raise LifecycleIntegrityError(
                    "broker compaction selected an unretired operation"
                )
            connection.execute(
                "DELETE FROM broker_execution_claims WHERE claim_id=?",
                (row["claim_id"],),
            )
            removed += 1
        return removed

    def _claim_broker_execution(
        self,
        reservation: Any,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Any,
    ) -> BrokerExecutionClaim:
        """Bounded recent-window claim with independent broker high-water."""

        connection = self._require_open()
        self._assert_fence()
        runtime = self._validated_runtime_reservation_row(reservation)
        reservation_id = str(runtime["reservation_id"])
        stream_identity = str(runtime["stream_identity_sha256"])
        operation_writer_epoch = self._validate_monotonic_operation_identity(
            stream_identity, operation_id, operation_sequence
        )
        witness_row = connection.execute(
            "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if witness_row is None:
            raise LifecycleIntegrityError(
                "broker claim runtime witness is absent"
            )
        witnessed = self.authority._witness_from_row(witness_row)
        if (
            operation_writer_epoch != witnessed.writer_epoch
            or operation_sequence > witnessed.max_order_operation_sequence
        ):
            raise LifecycleRecoveryRequired(
                "broker claim operation is not covered by the external runtime witness"
            )
        highwater_receipt = self.authority._verify_broker_highwater_receipt(
            broker_highwater_receipt,
            account_scope_sha256=account_scope_sha256,
            requested_sequence=broker_execution_sequence,
        )
        highwater_sha = _sha256_bytes(
            _canonical_bytes(highwater_receipt.canonical())
        )
        before_ledger = self.authority._verify_complete_broker_ledger(connection)
        pending_ledger: Optional[Mapping[str, Any]] = None

        def claim() -> BrokerExecutionClaim:
            nonlocal pending_ledger
            self._assert_fence()
            fresh_runtime = self._validated_runtime_reservation_row(reservation)
            if str(fresh_runtime["stream_identity_sha256"]) != stream_identity:
                raise LifecycleIntegrityError(
                    "runtime stream identity changed during broker claim"
                )
            # Snapshot the authenticated counters before guard triggers stage
            # any account/claim row changes in this transaction.
            ledger_row = self.authority._load_broker_ledger_state(
                connection, verify_counts=False
            )
            conflicts = connection.execute(
                """SELECT * FROM broker_execution_claims
                   WHERE account_scope_sha256=? AND (
                       broker_execution_id=? OR broker_execution_sequence=? OR
                       (stream_identity_sha256=? AND
                        (operation_id=? OR operation_sequence=?)))
                   LIMIT 2""",
                (
                    account_scope_sha256,
                    broker_execution_id,
                    broker_execution_sequence,
                    stream_identity,
                    operation_id,
                    operation_sequence,
                ),
            ).fetchall()
            if len(conflicts) > 1:
                raise LifecycleIntegrityError(
                    "broker recent-window uniqueness resolves to multiple claims"
                )
            binding = self._validated_account_binding(
                reservation_id=reservation_id,
                stream_identity_sha256=stream_identity,
                account_scope_sha256=account_scope_sha256,
            )
            head = connection.execute(
                "SELECT * FROM broker_account_heads WHERE account_scope_sha256=?",
                (account_scope_sha256,),
            ).fetchone()
            stream_head = connection.execute(
                "SELECT * FROM runtime_claim_heads WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if head is not None:
                self.authority._validate_broker_account_head(head)
                if (
                    highwater_receipt.observed_sequence_highwater
                    < int(head["external_highwater_sequence"])
                ):
                    raise LifecycleRecoveryRequired(
                        "broker high-water receipt rolled back"
                    )
            if stream_head is not None:
                self.authority._validate_runtime_claim_head(stream_head)
            if conflicts:
                observed = self.authority._broker_claim_from_row(conflicts[0])
                expected = {
                    "account_scope_sha256": account_scope_sha256,
                    "broker_execution_id": broker_execution_id,
                    "broker_execution_sequence": broker_execution_sequence,
                    "stream_identity_sha256": stream_identity,
                    "operation_id": operation_id,
                    "operation_sequence": operation_sequence,
                    "payload_sha256": payload_sha256,
                }
                if any(
                    getattr(observed, name) != value
                    for name, value in expected.items()
                ):
                    raise LifecycleCASMismatch(
                        "broker execution/recent operation was already claimed differently"
                    )
                if binding is None or head is None or stream_head is None:
                    raise LifecycleIntegrityError(
                        "broker recent claim lost its authority records"
                    )
                return observed

            now = time.time_ns()
            new_account = head is None
            if binding is None:
                binding_value = {
                    "reservation_id": reservation_id,
                    "account_scope_sha256": account_scope_sha256,
                    "stream_identity_sha256": stream_identity,
                    "fencing_token": self.fencing_token,
                    "created_ns": now,
                }
                connection.execute(
                    "INSERT INTO runtime_account_bindings VALUES(?,?,?,?,?,?)",
                    (
                        reservation_id,
                        account_scope_sha256,
                        stream_identity,
                        self.fencing_token,
                        now,
                        self.authority._sign(
                            self.authority._runtime_account_binding_fields(
                                binding_value
                            )
                        ),
                    ),
                )
            if head is None:
                account_count = int(ledger_row["account_count"])
                if account_count >= self.authority.limits.max_broker_account_scopes:
                    raise LifecycleBoundError(
                        "broker account-scope row cap reached before claim"
                    )
                account_value: dict[str, Any] = {
                    "account_scope_sha256": account_scope_sha256,
                    "claim_count": 0,
                    "last_claim_sequence": 0,
                    "last_claim_sha256": GENESIS_SHA256,
                    "external_highwater_sequence": (
                        highwater_receipt.observed_sequence_highwater
                    ),
                    "external_receipt_sha256": highwater_sha,
                    "retired_execution_floor": max(
                        0,
                        highwater_receipt.observed_sequence_highwater
                        - self.authority.limits.max_broker_reorder_sequences
                        - 1,
                    ),
                    "fencing_token": self.fencing_token,
                    "updated_ns": now,
                }
                account_value["record_hmac"] = self.authority._sign(
                    self.authority._broker_account_head_fields(account_value)
                )
                connection.execute(
                    "INSERT INTO broker_account_heads VALUES(?,?,?,?,?,?,?,?,?,?)",
                    tuple(
                        account_value[name]
                        for name in (
                            "account_scope_sha256",
                            "claim_count",
                            "last_claim_sequence",
                            "last_claim_sha256",
                            "external_highwater_sequence",
                            "external_receipt_sha256",
                            "retired_execution_floor",
                            "fencing_token",
                            "updated_ns",
                            "record_hmac",
                        )
                    ),
                )
                head = account_value
            else:
                account_value = dict(head)
                account_value.update(
                    {
                        "external_highwater_sequence": (
                            highwater_receipt.observed_sequence_highwater
                        ),
                        "external_receipt_sha256": highwater_sha,
                        "retired_execution_floor": max(
                            int(head["retired_execution_floor"]),
                            highwater_receipt.observed_sequence_highwater
                            - self.authority.limits.max_broker_reorder_sequences
                            - 1,
                        ),
                        "fencing_token": self.fencing_token,
                        "updated_ns": now,
                    }
                )
            if broker_execution_sequence <= int(
                account_value["retired_execution_floor"]
            ):
                raise LifecycleCASMismatch(
                    "broker execution is at/below the authenticated retired floor"
                )

            if stream_head is None:
                stream_value: dict[str, Any] = {
                    "reservation_id": reservation_id,
                    "account_scope_sha256": account_scope_sha256,
                    "stream_identity_sha256": stream_identity,
                    "max_operation_sequence": operation_sequence,
                    "retired_operation_floor": max(
                        0,
                        operation_sequence
                        - self.authority.limits.max_operation_reorder_sequences
                        - 1,
                    ),
                    "fencing_token": self.fencing_token,
                    "updated_ns": now,
                }
                stream_value["record_hmac"] = self.authority._sign(
                    self.authority._runtime_claim_head_fields(stream_value)
                )
                connection.execute(
                    "INSERT INTO runtime_claim_heads VALUES(?,?,?,?,?,?,?,?)",
                    tuple(
                        stream_value[name]
                        for name in (
                            "reservation_id",
                            "account_scope_sha256",
                            "stream_identity_sha256",
                            "max_operation_sequence",
                            "retired_operation_floor",
                            "fencing_token",
                            "updated_ns",
                            "record_hmac",
                        )
                    ),
                )
            else:
                if (
                    str(stream_head["account_scope_sha256"])
                    != account_scope_sha256
                    or str(stream_head["stream_identity_sha256"])
                    != stream_identity
                ):
                    raise LifecycleIntegrityError(
                        "runtime claim head changed account/stream binding"
                    )
                maximum = max(
                    int(stream_head["max_operation_sequence"]),
                    operation_sequence,
                )
                stream_value = dict(stream_head)
                stream_value.update(
                    {
                        "max_operation_sequence": maximum,
                        "retired_operation_floor": max(
                            int(stream_head["retired_operation_floor"]),
                            maximum
                            - self.authority.limits.max_operation_reorder_sequences
                            - 1,
                        ),
                        "fencing_token": self.fencing_token,
                        "updated_ns": now,
                    }
                )
            if operation_sequence <= int(
                stream_value["retired_operation_floor"]
            ):
                raise LifecycleCASMismatch(
                    "runtime operation is at/below the authenticated retired floor"
                )
            stream_value["record_hmac"] = self.authority._sign(
                self.authority._runtime_claim_head_fields(stream_value)
            )
            connection.execute(
                "UPDATE runtime_claim_heads SET max_operation_sequence=?,"
                "retired_operation_floor=?,fencing_token=?,updated_ns=?,"
                "record_hmac=? WHERE reservation_id=?",
                (
                    stream_value["max_operation_sequence"],
                    stream_value["retired_operation_floor"],
                    stream_value["fencing_token"],
                    stream_value["updated_ns"],
                    stream_value["record_hmac"],
                    reservation_id,
                ),
            )
            account_value["record_hmac"] = self.authority._sign(
                self.authority._broker_account_head_fields(account_value)
            )
            connection.execute(
                "UPDATE broker_account_heads SET external_highwater_sequence=?,"
                "external_receipt_sha256=?,retired_execution_floor=?,"
                "fencing_token=?,updated_ns=?,record_hmac=? "
                "WHERE account_scope_sha256=?",
                (
                    account_value["external_highwater_sequence"],
                    account_value["external_receipt_sha256"],
                    account_value["retired_execution_floor"],
                    account_value["fencing_token"],
                    account_value["updated_ns"],
                    account_value["record_hmac"],
                    account_scope_sha256,
                ),
            )

            live_count = int(ledger_row["live_claim_count"])
            removed = 0
            if live_count >= self.authority.limits.max_broker_execution_claims:
                removed = self._compact_recent_broker_window(
                    connection,
                    rows_to_free=(
                        live_count
                        - self.authority.limits.max_broker_execution_claims
                        + 1
                    ),
                )
            if live_count - removed >= self.authority.limits.max_broker_execution_claims:
                raise LifecycleBoundError(
                    "broker recent window is full; external/local high-waters "
                    "have not retired an eligible exact claim"
                )
            claim_sequence = int(account_value["last_claim_sequence"]) + 1
            claim_id = "broker-" + _sha256_bytes(
                _canonical_bytes(
                    {
                        "authority_identity_sha256": self.authority.authority_id,
                        "account_scope_sha256": account_scope_sha256,
                        "broker_execution_id": broker_execution_id,
                    }
                )
            )[:48]
            value: dict[str, Any] = {
                "claim_id": claim_id,
                "account_scope_sha256": account_scope_sha256,
                "broker_execution_id": broker_execution_id,
                "broker_execution_sequence": broker_execution_sequence,
                "claim_sequence": claim_sequence,
                "stream_identity_sha256": stream_identity,
                "operation_id": operation_id,
                "operation_sequence": operation_sequence,
                "payload_sha256": payload_sha256,
                "previous_claim_sha256": str(
                    account_value["last_claim_sha256"]
                ),
                "claim_sha256": "",
                "fencing_token": self.fencing_token,
                "created_ns": now,
                "receipt_mac": "",
                "record_hmac": "",
            }
            value["claim_sha256"] = _sha256_bytes(
                _canonical_bytes(self.authority._broker_claim_chain_fields(value))
            )
            value["receipt_mac"] = self.authority._sign(
                self.authority._broker_claim_public_fields(value)
            )
            value["record_hmac"] = self.authority._sign(
                self.authority._broker_claim_record_fields(value)
            )
            connection.execute(
                """INSERT INTO broker_execution_claims(
                       claim_id,account_scope_sha256,broker_execution_id,
                       broker_execution_sequence,claim_sequence,
                       stream_identity_sha256,operation_id,operation_sequence,
                       payload_sha256,previous_claim_sha256,claim_sha256,
                       fencing_token,created_ns,receipt_mac,record_hmac
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                tuple(
                    value[name]
                    for name in (
                        "claim_id",
                        "account_scope_sha256",
                        "broker_execution_id",
                        "broker_execution_sequence",
                        "claim_sequence",
                        "stream_identity_sha256",
                        "operation_id",
                        "operation_sequence",
                        "payload_sha256",
                        "previous_claim_sha256",
                        "claim_sha256",
                        "fencing_token",
                        "created_ns",
                        "receipt_mac",
                        "record_hmac",
                    )
                ),
            )
            account_value.update(
                {
                    "claim_count": claim_sequence,
                    "last_claim_sequence": claim_sequence,
                    "last_claim_sha256": value["claim_sha256"],
                    "record_hmac": "",
                }
            )
            account_value["record_hmac"] = self.authority._sign(
                self.authority._broker_account_head_fields(account_value)
            )
            connection.execute(
                "UPDATE broker_account_heads SET claim_count=?,last_claim_sequence=?,"
                "last_claim_sha256=?,external_highwater_sequence=?,"
                "external_receipt_sha256=?,retired_execution_floor=?,"
                "fencing_token=?,updated_ns=?,record_hmac=? "
                "WHERE account_scope_sha256=?",
                (
                    account_value["claim_count"],
                    account_value["last_claim_sequence"],
                    account_value["last_claim_sha256"],
                    account_value["external_highwater_sequence"],
                    account_value["external_receipt_sha256"],
                    account_value["retired_execution_floor"],
                    account_value["fencing_token"],
                    account_value["updated_ns"],
                    account_value["record_hmac"],
                    account_scope_sha256,
                ),
            )
            next_version = int(ledger_row["ledger_version"]) + 1
            accumulator = _sha256_bytes(
                _canonical_bytes(
                    {
                        "schema_version": "fxpm-broker-ledger-accumulator-v2",
                        "previous_sha256": str(
                            ledger_row["accumulator_sha256"]
                        ),
                        "ledger_version": next_version,
                        "claim_sha256": value["claim_sha256"],
                    }
                )
            )
            ledger_value = {
                "ledger_version": next_version,
                "total_claim_count": int(ledger_row["total_claim_count"]) + 1,
                "live_claim_count": live_count - removed + 1,
                "account_count": int(ledger_row["account_count"])
                + (1 if new_account else 0),
                "accumulator_sha256": accumulator,
                "fencing_token": self.fencing_token,
                "updated_ns": now,
            }
            self.authority._write_broker_ledger_state(connection, ledger_value)
            pending_ledger = {
                "version": next_version,
                "claim_count": ledger_value["total_claim_count"],
                "sha256": accumulator,
            }
            anchor = self.authority._read_fence_anchor()
            if (
                int(anchor["broker_ledger_version"])
                != int(before_ledger["version"])
                or int(anchor["broker_ledger_claim_count"])
                != int(before_ledger["claim_count"])
                or str(anchor["broker_ledger_sha256"])
                != str(before_ledger["sha256"])
            ):
                raise LifecycleRecoveryRequired(
                    "broker ledger changed before pending-fence publication"
                )
            self.authority._write_fence_anchor(
                committed_token=self.fencing_token,
                pending_token=None,
                broker_ledger_version=int(before_ledger["version"]),
                broker_ledger_claim_count=int(before_ledger["claim_count"]),
                broker_ledger_sha256=str(before_ledger["sha256"]),
                pending_broker_ledger_version=next_version,
                pending_broker_ledger_claim_count=ledger_value[
                    "total_claim_count"
                ],
                pending_broker_ledger_sha256=accumulator,
            )
            return self.authority._broker_claim_from_row(value)

        try:
            result = self.authority._transaction(connection, claim)
        except BaseException:
            self.authority._reconcile_fence_anchor(connection)
            raise
        if pending_ledger is not None:
            self.authority._write_fence_anchor(
                committed_token=self.fencing_token,
                pending_token=None,
                broker_ledger_version=int(pending_ledger["version"]),
                broker_ledger_claim_count=int(pending_ledger["claim_count"]),
                broker_ledger_sha256=str(pending_ledger["sha256"]),
                pending_broker_ledger_version=None,
                pending_broker_ledger_claim_count=None,
                pending_broker_ledger_sha256=None,
            )
            self.authority._check_fence_coherence(connection)
        return result

    def _audit_lifecycle_archive(self) -> Mapping[str, Any]:
        connection = self._require_open()
        self._assert_fence()
        started = time.monotonic()
        self.authority._verify_database_identity(connection, deep=True)
        row = self.authority._load_archive_head(
            connection, verify_rows=True
        )
        return {
            "schema_version": "fxpm.explicit-lifecycle-archive-audit.v1",
            "operation_count": int(row["operation_count"]),
            "pin_count": int(row["pin_count"]),
            "runtime_count": int(row["runtime_count"]),
            "operation_sha256": str(row["operation_sha256"]),
            "pin_sha256": str(row["pin_sha256"]),
            "runtime_sha256": str(row["runtime_sha256"]),
            "elapsed_seconds": time.monotonic() - started,
        }

    def _audit_broker_ledger(self) -> Mapping[str, Any]:
        connection = self._require_open()
        self._assert_fence()
        started = time.monotonic()
        self.authority._verify_database_identity(connection, deep=True)
        archive = self.authority._load_archive_head(
            connection, verify_rows=True
        )
        state = self.authority._load_broker_ledger_state(connection)
        heads: dict[str, sqlite3.Row] = {}
        cursor = connection.execute(
            "SELECT * FROM broker_account_heads ORDER BY account_scope_sha256"
        )
        while True:
            batch = cursor.fetchmany(
                self.authority.limits.broker_full_audit_batch_rows
            )
            if not batch:
                break
            for row in batch:
                self.authority._validate_broker_account_head(row)
                account = str(row["account_scope_sha256"])
                if account in heads:
                    raise LifecycleIntegrityError(
                        "explicit broker audit found duplicate account head"
                    )
                heads[account] = row
                if len(heads) > self.authority.limits.max_broker_account_scopes:
                    raise LifecycleBoundError(
                        "explicit broker audit exceeded account bound"
                    )
        if len(heads) != int(state["account_count"]):
            raise LifecycleIntegrityError(
                "explicit broker audit account count mismatch"
            )
        logical_count = sum(int(row["claim_count"]) for row in heads.values())
        if logical_count != int(state["total_claim_count"]):
            raise LifecycleIntegrityError(
                "explicit broker audit logical count mismatch"
            )
        stream_heads: dict[tuple[str, str], sqlite3.Row] = {}
        cursor = connection.execute(
            "SELECT * FROM runtime_claim_heads ORDER BY reservation_id"
        )
        while True:
            batch = cursor.fetchmany(
                self.authority.limits.broker_full_audit_batch_rows
            )
            if not batch:
                break
            for row in batch:
                self.authority._validate_runtime_claim_head(row)
                key = (
                    str(row["account_scope_sha256"]),
                    str(row["stream_identity_sha256"]),
                )
                if key in stream_heads:
                    raise LifecycleIntegrityError(
                        "explicit broker audit found duplicate runtime claim head"
                    )
                stream_heads[key] = row
        retired_streams: dict[tuple[str, str], sqlite3.Row] = {}
        cursor = connection.execute(
            "SELECT * FROM runtime_tombstones "
            "WHERE account_scope_sha256 IS NOT NULL ORDER BY reservation_id"
        )
        retired_count = 0
        while True:
            batch = cursor.fetchmany(
                self.authority.limits.broker_full_audit_batch_rows
            )
            if not batch:
                break
            for row in batch:
                retired_count += 1
                if retired_count > self.authority.limits.max_scan_entries:
                    raise LifecycleBoundError(
                        "explicit broker audit exceeded runtime archive bound"
                    )
                self.authority._validate_runtime_tombstone(row)
                key = (
                    str(row["account_scope_sha256"]),
                    str(row["stream_identity_sha256"]),
                )
                if key in stream_heads or key in retired_streams:
                    raise LifecycleIntegrityError(
                        "explicit broker audit found ambiguous runtime authority"
                    )
                retired_streams[key] = row
        seen = 0
        cursor = connection.execute(
            "SELECT * FROM broker_execution_claims "
            "ORDER BY account_scope_sha256,claim_sequence"
        )
        while True:
            batch = cursor.fetchmany(
                self.authority.limits.broker_full_audit_batch_rows
            )
            if not batch:
                break
            for row in batch:
                seen += 1
                if seen > self.authority.limits.max_broker_execution_claims:
                    raise LifecycleBoundError(
                        "explicit broker audit exceeded recent-window bound"
                    )
                claim = self.authority._broker_claim_from_row(row)
                head = heads.get(claim.account_scope_sha256)
                stream_head = stream_heads.get(
                    (
                        claim.account_scope_sha256,
                        claim.stream_identity_sha256,
                    )
                )
                retired_stream = retired_streams.get(
                    (
                        claim.account_scope_sha256,
                        claim.stream_identity_sha256,
                    )
                )
                if head is None or (stream_head is None) == (retired_stream is None):
                    raise LifecycleIntegrityError(
                        "explicit broker audit found an orphan recent claim"
                    )
                operation_highwater = int(
                    stream_head["max_operation_sequence"]
                    if stream_head is not None
                    else retired_stream["max_operation_sequence"]
                )
                if claim.operation_sequence > operation_highwater:
                    raise LifecycleIntegrityError(
                        "explicit broker audit claim exceeds runtime high-water"
                    )
                if claim.claim_sequence > int(head["claim_count"]):
                    raise LifecycleIntegrityError(
                        "explicit broker audit claim is ahead of its head"
                    )
                self._validate_monotonic_operation_identity(
                    claim.stream_identity_sha256,
                    claim.operation_id,
                    claim.operation_sequence,
                )
        if seen != int(state["live_claim_count"]):
            raise LifecycleIntegrityError(
                "explicit broker audit recent count mismatch"
            )
        anchor = self.authority._check_fence_coherence(connection)
        return {
            "schema_version": "fxpm.explicit-broker-ledger-audit.v1",
            "ledger_version": int(state["ledger_version"]),
            "total_claim_count": int(state["total_claim_count"]),
            "live_claim_count": seen,
            "account_count": len(heads),
            "accumulator_sha256": str(state["accumulator_sha256"]),
            "fence_sha256": str(anchor["broker_ledger_sha256"]),
            "archive_operation_count": int(archive["operation_count"]),
            "archive_pin_count": int(archive["pin_count"]),
            "archive_runtime_count": int(archive["runtime_count"]),
            "elapsed_seconds": time.monotonic() - started,
        }

    def _verify_broker_execution_claim(
        self,
        claim: Any,
        reservation: Any,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Any,
    ) -> None:
        connection = self._require_open()
        self._assert_fence()
        runtime = self._validated_runtime_claim_context(reservation)
        stream_identity = str(runtime["stream_identity_sha256"])
        reservation_id = str(runtime["reservation_id"])
        self._validate_monotonic_operation_identity(
            stream_identity, operation_id, operation_sequence
        )
        self.authority._verify_broker_highwater_receipt(
            broker_highwater_receipt,
            account_scope_sha256=account_scope_sha256,
            requested_sequence=broker_execution_sequence,
        )
        names = tuple(BrokerExecutionClaim.__dataclass_fields__)
        supplied = {name: getattr(claim, name, None) for name in names}
        claim_id = _safe_identifier(str(supplied["claim_id"] or ""), label="claim_id")
        public = {
            "schema_version": BROKER_EXECUTION_CLAIM_SCHEMA,
            **{
                name: supplied[name]
                for name in names
                if name != "receipt_mac"
            },
        }
        if not hmac.compare_digest(
            str(supplied["receipt_mac"]), self.authority._sign(public)
        ):
            raise LifecycleIntegrityError(
                "broker execution claim receipt is forged"
            )
        expected = {
            "authority_identity_sha256": self.authority.authority_id,
            "account_scope_sha256": account_scope_sha256,
            "broker_execution_id": broker_execution_id,
            "broker_execution_sequence": broker_execution_sequence,
            "stream_identity_sha256": stream_identity,
            "operation_id": operation_id,
            "operation_sequence": operation_sequence,
            "payload_sha256": payload_sha256,
        }
        if any(supplied[name] != value for name, value in expected.items()):
            raise LifecycleIntegrityError(
                "broker execution claim exact binding mismatch"
            )
        row = connection.execute(
            "SELECT * FROM broker_execution_claims WHERE claim_id=?", (claim_id,)
        ).fetchone()
        runtime_tombstone = runtime["tombstone"]
        if runtime_tombstone is None:
            binding = self._validated_account_binding(
                reservation_id=reservation_id,
                stream_identity_sha256=stream_identity,
                account_scope_sha256=account_scope_sha256,
            )
            if binding is None:
                raise LifecycleIntegrityError(
                    "broker execution claim account binding is absent"
                )
        elif str(runtime_tombstone["account_scope_sha256"]) != account_scope_sha256:
            raise LifecycleIntegrityError(
                "retired runtime broker account binding mismatch"
            )
        head = connection.execute(
            "SELECT * FROM broker_account_heads WHERE account_scope_sha256=?",
            (account_scope_sha256,),
        ).fetchone()
        if head is None:
            raise LifecycleIntegrityError("broker execution account head is absent")
        self.authority._validate_broker_account_head(head)
        if runtime_tombstone is None:
            stream_head = connection.execute(
                "SELECT * FROM runtime_claim_heads WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if stream_head is None:
                raise LifecycleIntegrityError("runtime claim head is absent")
            self.authority._validate_runtime_claim_head(stream_head)
            operation_floor = int(stream_head["retired_operation_floor"])
        else:
            operation_floor = int(
                runtime_tombstone["retired_operation_floor"]
            )
        if row is not None:
            observed = self.authority._broker_claim_from_row(row)
            if supplied != observed.canonical():
                raise LifecycleIntegrityError(
                    "broker execution claim receipt is stale or forged"
                )
            return
        if (
            broker_execution_sequence
            > int(head["retired_execution_floor"])
            or operation_sequence
            > operation_floor
            or int(supplied["claim_sequence"]) > int(head["claim_count"])
        ):
            raise LifecycleIntegrityError(
                "broker claim vanished inside the bounded recent window"
            )

    def _release_runtime_storage(self, reservation: Any) -> None:
        connection = self._require_open()
        reservation_id = _safe_identifier(
            str(getattr(reservation, "reservation_id", "")), label="reservation_id"
        )
        row = connection.execute(
            "SELECT * FROM runtime_reservations WHERE reservation_id=?", (reservation_id,)
        ).fetchone()
        if row is None:
            raise LifecycleStateError("unknown runtime reservation")
        expected = self.authority._runtime_adapter_from_row(row)
        supplied = {
            name: getattr(reservation, name, None)
            for name in expected.canonical()
        }
        if expected.canonical() != supplied:
            raise LifecycleIntegrityError("runtime release receipt is stale or forged")
        if str(row["status"]) != "ACTIVE":
            raise LifecycleStateError("runtime reservation is already inactive")
        now = time.time_ns()
        updated = dict(row)
        updated.update(
            {"status": "RELEASED", "fencing_token": self.fencing_token, "updated_ns": now}
        )
        updated["receipt_mac"] = self.authority._sign(
            self.authority._runtime_signed_fields(updated)
        )

        def release() -> None:
            self._assert_fence()
            witness_row = connection.execute(
                "SELECT * FROM runtime_witnesses WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if witness_row is None:
                raise LifecycleIntegrityError(
                    "released runtime lost its monotonic witness"
                )
            witness = self.authority._witness_from_row(witness_row)
            claim_head = connection.execute(
                "SELECT * FROM runtime_claim_heads WHERE reservation_id=?",
                (reservation_id,),
            ).fetchone()
            if claim_head is not None:
                self.authority._validate_runtime_claim_head(claim_head)
                if (
                    int(claim_head["max_operation_sequence"])
                    > witness.max_order_operation_sequence
                ):
                    raise LifecycleIntegrityError(
                        "runtime claim head exceeds its external order witness"
                    )
                claim_value = dict(claim_head)
                claim_value.update(
                    {
                        "max_operation_sequence": witness.max_order_operation_sequence,
                        "retired_operation_floor": witness.max_order_operation_sequence,
                        "fencing_token": self.fencing_token,
                        "updated_ns": now,
                    }
                )
                connection.execute(
                    "UPDATE runtime_claim_heads SET max_operation_sequence=?,"
                    "retired_operation_floor=?,fencing_token=?,updated_ns=?,"
                    "record_hmac=? WHERE reservation_id=?",
                    (
                        claim_value["max_operation_sequence"],
                        claim_value["retired_operation_floor"],
                        claim_value["fencing_token"],
                        claim_value["updated_ns"],
                        self.authority._sign(
                            self.authority._runtime_claim_head_fields(
                                claim_value
                            )
                        ),
                        reservation_id,
                    ),
                )
            changed = connection.execute(
                "UPDATE runtime_reservations SET status='RELEASED',fencing_token=?,"
                "updated_ns=?,receipt_mac=? WHERE reservation_id=? AND status='ACTIVE'",
                (
                    self.fencing_token,
                    now,
                    updated["receipt_mac"],
                    reservation_id,
                ),
            ).rowcount
            if changed != 1:
                raise LifecycleStateError("runtime reservation release was fenced")

        self.authority._transaction(connection, release)

    def reservation_receipt(self, operation_id: str) -> Mapping[str, Any]:
        """Issue a narrow authenticated permit for an external bounded writer."""
        self._assert_fence()
        row = self._operation_row(operation_id)
        budget = _budget_from_row(row)
        materialized = self._materialized(row)
        domains: list[Mapping[str, Any]] = []
        for item in budget.operational:
            domain = self.authority.operational_domains.get(item.domain)
            if domain is None:
                raise LifecycleIntegrityError(
                    f"reservation references unknown operational domain {item.domain}"
                )
            observed = materialized["operational"].get(
                item.domain, {"peak": 0, "final": 0, "files": 0}
            )
            domains.append(
                {
                    "domain": item.domain,
                    "allowed_root": os.path.normcase(str(domain.root)),
                    "volume_key": _volume_key(domain.root),
                    "reserved_peak_bytes": item.peak_bytes,
                    "reserved_final_bytes": item.final_bytes,
                    "reserved_db_and_journal_peak_bytes": (
                        item.peak_bytes
                        if budget.kind == OperationKind.RUNTIME_STATE.value
                        else None
                    ),
                    "reserved_files": item.files_delta,
                    "materialized_peak_bytes": observed["peak"],
                    "materialized_final_bytes": observed["final"],
                    "materialized_files": observed["files"],
                    "domain_cap_bytes": domain.max_bytes,
                    "domain_cap_files": domain.max_files,
                }
            )
        state = str(row["state"])
        unsigned = {
            "schema_version": RESERVATION_RECEIPT_SCHEMA,
            "authority_id": self.authority.authority_id,
            "root_key": self.authority.root_key,
            "operation_id": str(row["operation_id"]),
            "operation_kind": str(row["kind"]),
            "state": state,
            "verification_status": (
                "ACTIVE"
                if state not in TERMINAL_STATES
                and state != OperationState.RECOVERY_BLOCKED.value
                else state
            ),
            "fencing_token": int(row["fencing_token"]),
            "budget_sha256": str(row["budget_sha256"]),
            "materialized_sha256": _sha256_bytes(_canonical_bytes(materialized)),
            "institution_cap_bytes": self.authority.institution_cap_bytes,
            "min_volume_free_bytes": self.authority.limits.min_volume_free_bytes,
            "operational_domains": domains,
            "issued_ns": time.time_ns(),
        }
        receipt = dict(unsigned)
        receipt["receipt_hmac"] = self.authority._sign(unsigned)
        return receipt

    def record_materialized(
        self,
        operation_id: str,
        *,
        native_peak_bytes: Optional[int] = None,
        native_final_bytes: Optional[int] = None,
        cold_peak_bytes: Optional[int] = None,
        cold_final_bytes: Optional[int] = None,
        native_files: Optional[int] = None,
        cold_files: Optional[int] = None,
        operational: Optional[Mapping[str, Mapping[str, int]]] = None,
    ) -> ReservationSnapshot:
        """Persist absolute, monotonic materialization counters.

        Callers must top up first if any absolute value would exceed its
        reservation.  Recording is intentionally not an after-the-fact growth
        permit; integration must invoke this immediately before each bounded
        write increment.
        """
        connection = self._require_open()
        supplied = {
            "native_peak": native_peak_bytes,
            "native_final": native_final_bytes,
            "cold_peak": cold_peak_bytes,
            "cold_final": cold_final_bytes,
            "native_files": native_files,
            "cold_files": cold_files,
        }
        for key, value in supplied.items():
            if value is not None:
                supplied[key] = _nonnegative(key, value)
        operational_supplied: dict[str, dict[str, int]] = {}
        for name, raw in (operational or {}).items():
            domain = _safe_identifier(name, label="quota domain", max_length=64)
            if domain not in self.authority.operational_domains:
                raise LifecycleBoundError(f"unknown operational quota domain: {domain}")
            if not isinstance(raw, Mapping) or set(raw) != {"peak", "final", "files"}:
                raise ValueError(
                    "operational materialization requires exact peak/final/files fields"
                )
            operational_supplied[domain] = {
                key: _nonnegative(f"{domain}.{key}", raw[key])
                for key in ("peak", "final", "files")
            }
        now = time.time_ns()

        def update() -> None:
            self._assert_fence()
            row = self._operation_row(operation_id)
            if str(row["state"]) in TERMINAL_STATES:
                raise LifecycleStateError("cannot materialize against a terminal reservation")
            old = self._materialized(row)
            proposed = {
                key: old[key] if value is None else int(value)
                for key, value in supplied.items()
            }
            proposed_operational = {
                name: dict(values) for name, values in old["operational"].items()
            }
            proposed_operational.update(operational_supplied)
            for key, value in proposed.items():
                if value < old[key]:
                    raise LifecycleStateError(f"materialized {key} cannot decrease")
            for domain, values in proposed_operational.items():
                prior = old["operational"].get(domain, {"peak": 0, "final": 0, "files": 0})
                for key, value in values.items():
                    if value < prior[key]:
                        raise LifecycleStateError(
                            f"materialized {domain}.{key} cannot decrease"
                        )
            budget = _budget_from_row(row)
            bounds = {
                "native_peak": budget.native_peak_bytes,
                "native_final": budget.native_final_bytes,
                "cold_peak": budget.cold_peak_bytes,
                "cold_final": budget.cold_final_bytes,
                "native_files": budget.native_files_delta,
                "cold_files": budget.cold_files_delta,
            }
            excess = [key for key, value in proposed.items() if value > bounds[key]]
            if excess:
                raise LifecycleBoundError(
                    "materialization exceeds reservation; top-up required before growth: "
                    + ",".join(excess)
                )
            operational_bounds = {item.domain: item for item in budget.operational}
            for domain, values in proposed_operational.items():
                bound = operational_bounds.get(domain)
                if bound is None:
                    if any(values.values()):
                        raise LifecycleBoundError(
                            f"materialization has no reservation for domain {domain}"
                        )
                    continue
                if (
                    values["peak"] > bound.peak_bytes
                    or values["final"] > bound.final_bytes
                    or values["files"] > bound.files_delta
                ):
                    raise LifecycleBoundError(
                        f"materialization exceeds {domain} reservation; top-up required before growth"
                    )
                if values["final"] > values["peak"]:
                    raise LifecycleStateError(
                        f"{domain} final materialization exceeds peak allocation"
                    )
            if proposed["native_final"] > proposed["native_peak"]:
                raise LifecycleStateError("native final materialization exceeds peak allocation")
            if proposed["cold_final"] > proposed["cold_peak"]:
                raise LifecycleStateError("cold final materialization exceeds peak allocation")
            updated = self.authority._operation_record_value(
                row,
                materialized_native_peak_bytes=proposed["native_peak"],
                materialized_native_final_bytes=proposed["native_final"],
                materialized_cold_peak_bytes=proposed["cold_peak"],
                materialized_cold_final_bytes=proposed["cold_final"],
                materialized_native_files=proposed["native_files"],
                materialized_cold_files=proposed["cold_files"],
                materialized_operational_json=_canonical_bytes(
                    proposed_operational
                ).decode("utf-8"),
                updated_ns=now,
            )
            connection.execute(
                """UPDATE operations SET
                       materialized_native_peak_bytes=?,materialized_native_final_bytes=?,
                       materialized_cold_peak_bytes=?,materialized_cold_final_bytes=?,
                       materialized_native_files=?,materialized_cold_files=?,
                       materialized_operational_json=?,updated_ns=?,record_hmac=?
                   WHERE operation_id=?""",
                (
                    updated["materialized_native_peak_bytes"],
                    updated["materialized_native_final_bytes"],
                    updated["materialized_cold_peak_bytes"],
                    updated["materialized_cold_final_bytes"],
                    updated["materialized_native_files"],
                    updated["materialized_cold_files"],
                    updated["materialized_operational_json"],
                    updated["updated_ns"],
                    updated["record_hmac"],
                    operation_id,
                ),
            )

        self.authority._transaction(connection, update)
        return self.reservation(operation_id)

    def top_up(
        self,
        operation_id: str,
        *,
        native_final_bytes: int = 0,
        native_peak_bytes: int = 0,
        cold_final_bytes: int = 0,
        cold_peak_bytes: int = 0,
        native_files: int = 0,
        cold_files: int = 0,
        operational: Sequence[OperationalGrowth] = (),
    ) -> ReservationSnapshot:
        connection = self._require_open()
        row = self._operation_row(operation_id)
        if str(row["state"]) in TERMINAL_STATES:
            raise LifecycleStateError("cannot top up a terminal reservation")
        current = _budget_from_row(row)
        enlarged = current.with_top_up(
            native_final_bytes=native_final_bytes,
            native_peak_bytes=native_peak_bytes,
            cold_final_bytes=cold_final_bytes,
            cold_peak_bytes=cold_peak_bytes,
            native_files=native_files,
            cold_files=cold_files,
            operational=operational,
        )
        materialized = self._materialized(row)
        self._capacity_check(
            enlarged,
            candidate_materialized=materialized,
            exclude_operation_id=operation_id,
        )
        budget_bytes = _canonical_bytes(asdict(enlarged))
        now = time.time_ns()

        def update() -> None:
            self._assert_fence()
            fresh = self._operation_row(operation_id)
            if str(fresh["budget_sha256"]) != str(row["budget_sha256"]):
                raise LifecycleStateError("reservation changed during top-up")
            updated = self.authority._operation_record_value(
                fresh,
                budget_json=budget_bytes.decode("utf-8"),
                budget_sha256=_sha256_bytes(budget_bytes),
                file_count=(
                    enlarged.native_files_delta
                    + enlarged.cold_files_delta
                    + sum(item.files_delta for item in enlarged.operational)
                ),
                updated_ns=now,
            )
            connection.execute(
                "UPDATE operations SET budget_json=?,budget_sha256=?,file_count=?,updated_ns=?,"
                "record_hmac=? "
                "WHERE operation_id=?",
                (
                    updated["budget_json"],
                    updated["budget_sha256"],
                    updated["file_count"],
                    updated["updated_ns"],
                    updated["record_hmac"],
                    operation_id,
                ),
            )

        self.authority._transaction(connection, update)
        return self.reservation(operation_id)

    def transition(self, operation_id: str, target_state: OperationState | str) -> ReservationSnapshot:
        connection = self._require_open()
        try:
            target = OperationState(
                target_state.value
                if isinstance(target_state, OperationState)
                else str(target_state)
            ).value
        except ValueError as exc:
            raise LifecycleStateError(f"unknown lifecycle state: {target_state!r}") from exc
        if target == OperationState.RECOVERY_BLOCKED.value:
            raise LifecycleStateError("RECOVERY_BLOCKED is assigned only by startup recovery")
        now = time.time_ns()

        def update() -> None:
            self._assert_fence()
            row = self._operation_row(operation_id)
            current = str(row["state"])
            if target not in STRICT_TRANSITIONS[current]:
                raise LifecycleStateError(f"invalid lifecycle transition {current} -> {target}")
            updated = self.authority._operation_record_value(
                row, state=target, updated_ns=now
            )
            connection.execute(
                "UPDATE operations SET state=?,updated_ns=?,record_hmac=? "
                "WHERE operation_id=?",
                (
                    updated["state"],
                    updated["updated_ns"],
                    updated["record_hmac"],
                    operation_id,
                ),
            )
            self.authority._refresh_operation_ledger_state(
                connection, fencing_token=self.fencing_token
            )

        self.authority._transaction(connection, update)
        return self.reservation(operation_id)

    def resolve_recovery(
        self,
        operation_id: str,
        *,
        action: str,
        note: str,
    ) -> ReservationSnapshot:
        """Resolve a startup block without deriving success from filesystem paths."""
        connection = self._require_open()
        action = str(action).strip().upper()
        note = str(note).strip()
        if not note or len(note.encode("utf-8")) > 4096:
            raise ValueError("recovery note must contain 1..4096 UTF-8 bytes")
        now = time.time_ns()

        def update() -> None:
            self._assert_fence()
            row = self._operation_row(operation_id)
            if str(row["state"]) != OperationState.RECOVERY_BLOCKED.value:
                raise LifecycleStateError("operation is not RECOVERY_BLOCKED")
            recovery_from = str(row["recovery_from"] or "")
            if action == "ABORT":
                target = OperationState.ABORTING.value
            elif action == "RESUME":
                if recovery_from not in STRICT_TRANSITIONS or recovery_from in TERMINAL_STATES:
                    raise LifecycleStateError("recorded recovery state cannot be resumed")
                if recovery_from == OperationState.PUBLISHED.value:
                    raw_receipt = row["pointer_receipt_json"]
                    if not raw_receipt:
                        raise LifecycleRecoveryRequired(
                            "PUBLISHED recovery requires a durable authenticated pointer receipt"
                        )
                    try:
                        receipt = json.loads(str(raw_receipt))
                    except json.JSONDecodeError as exc:
                        raise LifecycleIntegrityError("stored pointer receipt is unreadable") from exc
                    self.authority.verify_pointer_receipt(receipt)
                target = recovery_from
            else:
                raise ValueError("recovery action must be ABORT or RESUME")
            payload = json.loads(str(row["payload_json"]))
            if not isinstance(payload, Mapping):
                raise LifecycleIntegrityError("operation payload is not an object")
            amended = dict(payload)
            amended["last_recovery"] = {
                "action": action,
                "note": note,
                "resolved_ns": now,
                "from_state": recovery_from,
            }
            payload_bytes = _canonical_bytes(amended)
            if len(payload_bytes) > self.authority.limits.max_payload_bytes:
                raise LifecycleBoundError("recovery note would exceed operation payload cap")
            updated = self.authority._operation_record_value(
                row,
                state=target,
                recovery_from=None,
                payload_json=payload_bytes.decode("utf-8"),
                payload_sha256=_sha256_bytes(payload_bytes),
                payload_bytes=len(payload_bytes),
                updated_ns=now,
            )
            connection.execute(
                "UPDATE operations SET state=?,recovery_from=NULL,payload_json=?,payload_sha256=?,"
                "payload_bytes=?,updated_ns=?,record_hmac=? WHERE operation_id=?",
                (
                    updated["state"],
                    updated["payload_json"],
                    updated["payload_sha256"],
                    updated["payload_bytes"],
                    updated["updated_ns"],
                    updated["record_hmac"],
                    operation_id,
                ),
            )
            self.authority._refresh_operation_ledger_state(
                connection, fencing_token=self.fencing_token
            )

        self.authority._transaction(connection, update)
        return self.reservation(operation_id)

    def cas_publish_pointer(
        self,
        operation_id: str,
        *,
        pointer_relative_path: str,
        expected_sha256: Optional[str],
        payload: Mapping[str, Any],
        _pre_replace_hook: Optional[Callable[[], None]] = None,
    ) -> Mapping[str, Any]:
        """Atomically replace one pointer after two exact CAS observations.

        The global lease serializes cooperative writers.  The second digest
        observation closes the staging interval, and verify-after-write binds
        the receipt.  If the file replacement succeeds but the SQLite receipt
        transaction fails, the operation remains PUBLISHING and this method
        raises ``LifecycleRecoveryRequired``; it never infers success later by
        merely finding the target path.
        """
        connection = self._require_open()
        self._assert_fence()
        row = self._operation_row(operation_id)
        if str(row["state"]) != OperationState.PUBLISHING.value:
            raise LifecycleStateError("pointer CAS requires operation state PUBLISHING")
        if not isinstance(payload, Mapping):
            raise ValueError("pointer payload must be an object")
        expected = ABSENT_POINTER if expected_sha256 is None else str(expected_sha256).lower()
        if expected != ABSENT_POINTER and not _is_sha256(expected):
            raise ValueError("expected_sha256 must be None or a lowercase SHA-256 digest")
        pointer_relative = _safe_identifier(
            str(pointer_relative_path).replace("\\", "/"),
            label="pointer_relative_path",
            max_length=512,
        )
        pointer_relative = PurePosixPath(pointer_relative).as_posix()
        pointer = self.authority._safe_native_relative(pointer_relative)
        if not pointer.parent.is_dir():
            raise LifecycleIntegrityError("pointer parent must already exist")

        def fingerprint() -> tuple[str, int]:
            if not pointer.exists():
                return ABSENT_POINTER, 0
            if pointer.is_symlink() or not pointer.is_file():
                raise LifecycleIntegrityError("pointer target is not a regular file")
            return _sha256_file(pointer, max_bytes=self.authority.limits.max_pointer_bytes)

        observed, observed_bytes = fingerprint()
        if observed != expected:
            raise LifecycleCASMismatch(
                f"pointer CAS expected {expected}, observed {observed}"
            )
        current_head = connection.execute(
            "SELECT * FROM pointer_heads WHERE pointer_relative_path=?",
            (pointer_relative,),
        ).fetchone()
        if current_head is None:
            pointer_state = self.authority._load_pointer_ledger_state(connection)
            if int(pointer_state["head_count"]) >= self.authority.limits.max_operations:
                raise LifecycleBoundError(
                    "pointer head cap reached before filesystem publication"
                )
        else:
            current_receipt = self.authority._validate_pointer_head(current_head)
            if (
                current_receipt["new_sha256"] != expected
                or current_receipt["new_bytes"] != observed_bytes
            ):
                raise LifecycleCASMismatch(
                    "pointer bytes do not match the authenticated current head"
                )
        new_bytes = _bounded_canonical_bytes(
            payload,
            max_bytes=self.authority.limits.max_pointer_bytes,
            label="pointer payload",
        )
        new_sha = _sha256_bytes(new_bytes)
        # The pointer-head and operation acknowledgement are small, but they
        # must have durable DB space before the external filesystem replace.
        self.authority._preflight_db_growth(
            connection, estimated_bytes=2 * self.authority.limits.page_size
        )
        staging = pointer.parent / f".{pointer.name}.{uuid.uuid4().hex}.tmp"
        replaced = False
        try:
            with staging.open("xb") as handle:
                handle.write(new_bytes)
                handle.flush()
                os.fsync(handle.fileno())
            staged_sha, staged_size = _sha256_file(
                staging, max_bytes=self.authority.limits.max_pointer_bytes
            )
            if staged_sha != new_sha or staged_size != len(new_bytes):
                raise LifecycleIntegrityError("staged pointer failed self-verification")
            if _pre_replace_hook is not None:
                _pre_replace_hook()
            second, second_bytes = fingerprint()
            if second != expected or second_bytes != observed_bytes:
                raise LifecycleCASMismatch(
                    "pointer drifted after staging; publication was not attempted"
                )
            os.replace(staging, pointer)
            replaced = True
            final_sha, final_size = _sha256_file(
                pointer, max_bytes=self.authority.limits.max_pointer_bytes
            )
            if final_sha != new_sha or final_size != len(new_bytes):
                raise LifecycleRecoveryRequired(
                    "pointer replacement did not verify; explicit recovery is required"
                )
            published_ns = time.time_ns()
            receipt_unsigned = {
                "schema_version": POINTER_RECEIPT_SCHEMA,
                "root_key": self.authority.root_key,
                "operation_id": operation_id,
                "pointer_relative_path": pointer_relative,
                "expected_sha256": expected,
                "expected_bytes": observed_bytes,
                "new_sha256": new_sha,
                "new_bytes": len(new_bytes),
                "fencing_token": self.fencing_token,
                "published_ns": published_ns,
            }
            receipt = dict(receipt_unsigned)
            receipt["receipt_hmac"] = self.authority._sign(receipt_unsigned)
            receipt_bytes = _bounded_canonical_bytes(
                receipt,
                max_bytes=4096,
                label="pointer receipt",
            )
            receipt_json = receipt_bytes.decode("utf-8")
            now = time.time_ns()

            def acknowledge() -> None:
                self._assert_fence()
                fresh = self._operation_row(operation_id)
                if str(fresh["state"]) != OperationState.PUBLISHING.value:
                    raise LifecycleStateError("operation changed during pointer publication")
                prior_head = connection.execute(
                    "SELECT * FROM pointer_heads WHERE pointer_relative_path=?",
                    (pointer_relative,),
                ).fetchone()
                if prior_head is None:
                    pointer_state = self.authority._load_pointer_ledger_state(
                        connection
                    )
                    if int(pointer_state["head_count"]) >= self.authority.limits.max_operations:
                        raise LifecycleBoundError(
                            "pointer head cap reached before publication acknowledgement"
                        )
                else:
                    prior_receipt = self.authority._validate_pointer_head(
                        prior_head
                    )
                    if (
                        prior_receipt["new_sha256"] != expected
                        or prior_receipt["new_bytes"] != observed_bytes
                    ):
                        raise LifecycleCASMismatch(
                            "pointer current head changed during publication"
                        )
                head_value = {
                    "pointer_relative_path": pointer_relative,
                    "operation_id": operation_id,
                    "receipt_json": receipt_json,
                    "receipt_sha256": _sha256_bytes(receipt_bytes),
                    "fencing_token": self.fencing_token,
                    "published_ns": published_ns,
                    "record_hmac": "",
                }
                head_value["record_hmac"] = self.authority._sign(
                    self.authority._pointer_head_fields(head_value)
                )
                if prior_head is None:
                    connection.execute(
                        "INSERT INTO pointer_heads VALUES(?,?,?,?,?,?,?)",
                        tuple(head_value[name] for name in head_value),
                    )
                else:
                    connection.execute(
                        "UPDATE pointer_heads SET operation_id=?,receipt_json=?,"
                        "receipt_sha256=?,fencing_token=?,published_ns=?,record_hmac=? "
                        "WHERE pointer_relative_path=?",
                        (
                            head_value["operation_id"],
                            head_value["receipt_json"],
                            head_value["receipt_sha256"],
                            head_value["fencing_token"],
                            head_value["published_ns"],
                            head_value["record_hmac"],
                            pointer_relative,
                        ),
                    )
                self.authority._refresh_pointer_ledger_state(
                    connection, fencing_token=self.fencing_token
                )
                updated = self.authority._operation_record_value(
                    fresh,
                    state=OperationState.PUBLISHED.value,
                    pointer_receipt_json=receipt_json,
                    pointer_receipt_hmac=receipt["receipt_hmac"],
                    updated_ns=now,
                )
                connection.execute(
                    "UPDATE operations SET state=?,pointer_receipt_json=?,pointer_receipt_hmac=?,"
                    "updated_ns=?,record_hmac=? WHERE operation_id=?",
                    (
                        updated["state"],
                        updated["pointer_receipt_json"],
                        updated["pointer_receipt_hmac"],
                        updated["updated_ns"],
                        updated["record_hmac"],
                        operation_id,
                    ),
                )
                self.authority._refresh_operation_ledger_state(
                    connection, fencing_token=self.fencing_token
                )

            try:
                self.authority._transaction(connection, acknowledge)
            except BaseException as exc:
                raise LifecycleRecoveryRequired(
                    "pointer was verified after replacement but lifecycle acknowledgement failed"
                ) from exc
            return receipt
        finally:
            if not replaced:
                try:
                    staging.unlink()
                except FileNotFoundError:
                    pass

    def _verify_pin_tombstone(self, row: Mapping[str, Any]) -> None:
        self.authority._validate_pin_tombstone(row)

    def _retire_pin(self, pin_id: str) -> None:
        connection = self._require_open()
        self._assert_fence()
        pin_id = _safe_identifier(pin_id, label="pin_id")

        def retire() -> None:
            self._assert_fence()
            row = connection.execute(
                "SELECT * FROM pins WHERE pin_id=?", (pin_id,)
            ).fetchone()
            if row is None:
                tombstone = connection.execute(
                    "SELECT * FROM pin_tombstones WHERE pin_id=?", (pin_id,)
                ).fetchone()
                if tombstone is not None:
                    self._verify_pin_tombstone(tombstone)
                    return
                raise LifecycleStateError(f"unknown pin: {pin_id}")
            # Retirement must not launder already-deleted or altered evidence
            # into an audit-clean tombstone.  Re-read every bounded root while
            # the lifecycle lease is held and fail before the archive changes.
            self.authority._verify_pin_row(row)
            source_sha = _sha256_bytes(_canonical_bytes(dict(row)))
            body = {
                "schema_version": PIN_TOMBSTONE_SCHEMA,
                "authority_identity_sha256": self.authority.authority_id,
                "pin_id": pin_id,
                "source_record_sha256": source_sha,
                "retired_ns": time.time_ns(),
                "fencing_token": self.fencing_token,
            }
            tombstone_sha = _sha256_bytes(_canonical_bytes(body))
            self.authority._preflight_db_growth(
                connection, estimated_bytes=len(_canonical_bytes(body)) + 512
            )
            connection.execute(
                "INSERT INTO pin_tombstones VALUES(?,?,?,?,?,?)",
                (
                    pin_id, source_sha, tombstone_sha, body["retired_ns"],
                    self.fencing_token,
                    self.authority._sign(
                        {**body, "tombstone_sha256": tombstone_sha}
                    ),
                ),
            )
            connection.execute("DELETE FROM pins WHERE pin_id=?", (pin_id,))
            self.authority._refresh_archive_head(
                connection, fencing_token=self.fencing_token
            )

        self.authority._transaction(connection, retire)

    def pin_evidence(self, pin_id: str, paths: Iterable[Path]) -> Mapping[str, Any]:
        connection = self._require_open()
        pin_id = _safe_identifier(pin_id, label="pin_id")
        roots: list[Mapping[str, Any]] = []
        encoded_root_bytes = 0
        for supplied in paths:
            if len(roots) >= self.authority.limits.max_files_per_operation:
                raise LifecycleBoundError("pin root count exceeds max_files_per_operation")
            path = Path(supplied)
            if not path.is_absolute():
                path = self.authority._safe_native_relative(path.as_posix())
            try:
                relative = path.resolve(strict=True).relative_to(self.authority.native_root)
            except (OSError, ValueError) as exc:
                raise LifecycleIntegrityError(f"pin path is outside native_root: {path}") from exc
            if path.is_symlink() or not path.is_file():
                raise LifecycleIntegrityError("only regular immutable files can be pinned")
            digest, size = _sha256_file(path)
            root_value = {
                "relative_path": relative.as_posix(),
                "sha256": digest,
                "size": size,
            }
            root_bytes = _bounded_canonical_bytes(
                root_value,
                max_bytes=self.authority.limits.max_pin_payload_bytes,
                label="pin root",
            )
            encoded_root_bytes += len(root_bytes)
            if encoded_root_bytes > self.authority.limits.max_pin_payload_bytes:
                raise LifecycleBoundError(
                    "pin roots exceed max_pin_payload_bytes before aggregation"
                )
            roots.append(root_value)
        if not roots:
            raise ValueError("a pin must bind at least one evidence file")
        payload = {
            "schema_version": PIN_SCHEMA,
            "root_key": self.authority.root_key,
            "pin_id": pin_id,
            "roots": sorted(roots, key=lambda item: str(item["relative_path"])),
        }
        payload_bytes = _bounded_canonical_bytes(
            payload,
            max_bytes=self.authority.limits.max_pin_payload_bytes,
            label="pin payload",
        )
        created = time.time_ns()
        signed = {
            "pin_id": pin_id,
            "created_ns": created,
            "payload_sha256": _sha256_bytes(payload_bytes),
            "payload_bytes": len(payload_bytes),
            "root_key": self.authority.root_key,
        }
        signature = self.authority._sign(signed)

        def insert() -> None:
            self._assert_fence()
            retired = connection.execute(
                "SELECT * FROM pin_tombstones WHERE pin_id=?", (pin_id,)
            ).fetchone()
            if retired is not None:
                self._verify_pin_tombstone(retired)
                raise LifecycleStateError(f"retired pin_id cannot be reused: {pin_id}")
            count = int(connection.execute("SELECT COUNT(*) FROM pins").fetchone()[0])
            if count >= self.authority.limits.max_pins:
                raise LifecycleBoundError("pin row cap reached before write")
            try:
                connection.execute(
                    "INSERT INTO pins(pin_id,created_ns,payload_json,payload_sha256,payload_bytes,record_hmac) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        pin_id,
                        created,
                        payload_bytes.decode("utf-8"),
                        signed["payload_sha256"],
                        len(payload_bytes),
                        signature,
                    ),
                )
                inserted = connection.execute(
                    "SELECT * FROM pins WHERE pin_id=?", (pin_id,)
                ).fetchone()
                if inserted is None:  # pragma: no cover - same transaction
                    raise LifecycleIntegrityError("pin insert vanished before verification")
                self.authority._verify_pin_row(inserted)
            except sqlite3.IntegrityError as exc:
                raise LifecycleStateError(f"duplicate pin_id: {pin_id}") from exc

        self.authority._transaction(connection, insert)
        return self.authority.verify_pin(pin_id)


__all__ = [
    "ABSENT_POINTER",
    "BROKER_EXECUTION_CLAIM_SCHEMA",
    "BrokerExecutionClaim",
    "COORDINATED_ROLLBACK_BOUNDARY",
    "DATABASE_SCHEMA",
    "FENCE_ANCHOR_SCHEMA",
    "PIN_SCHEMA",
    "POINTER_HEAD_SCHEMA",
    "POINTER_LEDGER_STATE_SCHEMA",
    "POINTER_RECEIPT_SCHEMA",
    "RESERVATION_RECEIPT_SCHEMA",
    "RUNTIME_WITNESS_SCHEMA",
    "GENESIS_SHA256",
    "GrowthBudget",
    "LifecycleBoundError",
    "LifecycleBusyError",
    "LifecycleCASMismatch",
    "LifecycleDatabaseFullError",
    "LifecycleIntegrityError",
    "LifecycleLimits",
    "LifecycleLockTimeout",
    "LifecycleRecoveryRequired",
    "LifecycleRootError",
    "LifecycleStateError",
    "NativeLifecycleAuthority",
    "NativeLifecycleError",
    "OperationKind",
    "OperationState",
    "OperationalDomain",
    "OperationalGrowth",
    "RecoveryEntry",
    "ReservationSnapshot",
    "RuntimeMonotonicWitness",
    "RuntimeStorageReservation",
    "RuntimeWitnessProposal",
    "StoreUsage",
    "validate_durable_local_root",
]
