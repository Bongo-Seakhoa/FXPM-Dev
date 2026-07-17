from __future__ import annotations

import base64
import hashlib
import hmac
import json
import operator
import shutil
import sqlite3
import subprocess
import sys
import time
import types
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import pm_state_runtime as runtime_module
from pm_state_contract import DEFAULT_REGISTRY_HASH
from pm_state_runtime import (
    AuthenticatedCadenceReceipt,
    AuthenticatedReducerRelease,
    AuthenticatedOrderResult,
    AuthenticatedStateEvidence,
    AuthenticatedStateRuntime,
    AuthenticationError,
    BindingMismatchError,
    BrokerExecutionClaim,
    CapacityExceededError,
    IntegrityError,
    LockTimeoutError,
    OrderTransitionError,
    RecoveryRequiredError,
    RuntimeLimits,
    RuntimeMonotonicWitness,
    SourceEpoch,
    StorageLocationError,
    StorageReservation,
    WinnerRecord,
    WinnerRoster,
)


KEY = b"state-runtime-test-key-material-32+"
_CADENCE_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(
    hashlib.sha256(b"state-runtime-v5-cadence-authority").digest()
)
_REDUCER_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(
    hashlib.sha256(b"state-runtime-reducer-release-authority").digest()
)
_BROKER_RESULT_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(
    hashlib.sha256(b"state-runtime-broker-result-authority").digest()
)


def _public_bytes(private_key: Ed25519PrivateKey) -> bytes:
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


CADENCE_KEYS = {"tests-v5-cadence": _public_bytes(_CADENCE_PRIVATE_KEY)}
REDUCER_KEYS = {"tests-reducer-release": _public_bytes(_REDUCER_PRIVATE_KEY)}
BROKER_RESULT_KEYS = {"tests-broker-result": _public_bytes(_BROKER_RESULT_PRIVATE_KEY)}


def _external_signature(
    private_key: Ed25519PrivateKey, domain: str, body: Mapping[str, Any]
) -> str:
    material = domain.encode("ascii") + b"\x00" + runtime_module._canonical_bytes(body)
    return base64.b64encode(private_key.sign(material)).decode("ascii")


MUTABLE_REDUCER_STEP = 1
DYNAMIC_LOOKUP_ALIAS = getattr
DYNAMIC_MODULE_NAMESPACE = types.ModuleType("tests.dynamic-reducer-namespace")
DYNAMIC_MODULE_NAMESPACE.value = 1
DYNAMIC_MODULE_NAMESPACE.__getattr__ = lambda name: name


def mutable_reducer_helper(value: float) -> float:
    return value * MUTABLE_REDUCER_STEP


class CountingEngine:
    @property
    def identity(self) -> Mapping[str, Any]:
        return {
            "engine_id": "tests.counting-engine",
            "schema_version": "v1",
            "code_sha256": "1" * 64,
            "nested": {"transition": "strict"},
        }

    def initial_state(self, *args: Any) -> Mapping[str, Any]:
        del args
        return {"bars": 0, "close_sum": 0.0}

    def validate_state_evidence(
        self, evidence: AuthenticatedStateEvidence, payload: Any
    ) -> None:
        if evidence.node != "controller:REGIME_HYSTERESIS":
            raise ValueError("unexpected evidence node")
        if payload["controller_state"] != "authenticated-replay":
            raise ValueError("unexpected evidence payload")

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "bars": int(state["bars"]) + 1,
            "close_sum": float(state["close_sum"]) + float(bar["close"]),
        }

    def initial_order_state(self, *args: Any) -> Mapping[str, Any]:
        del args
        return {"applied": []}

    def apply_order_result(
        self,
        state: Mapping[str, Any],
        action: str,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "applied": [
                *state["applied"],
                {"action": action, "request": dict(request), "result": dict(result)},
            ]
        }


class OversizedStateEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del state, bar
        return {"payload": "x" * 500_000}


class ImpostorEngine(CountingEngine):
    """Claims the same identity but executes different transition bytecode."""

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "bars": int(state["bars"]) + 2,
            "close_sum": float(state["close_sum"]) - float(bar["close"]),
        }


class GlobalDependentEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "bars": int(state["bars"]) + MUTABLE_REDUCER_STEP,
            "close_sum": float(state["close_sum"])
            + mutable_reducer_helper(float(bar["close"])),
        }


class ConfiguredEngine(CountingEngine):
    def __init__(self, multiplier: int) -> None:
        self.multiplier = multiplier

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "bars": int(state["bars"]) + self.multiplier,
            "close_sum": float(state["close_sum"]) + float(bar["close"]),
        }


class SelfMutatingConfiguredEngine(ConfiguredEngine):
    """Hostile reducer that changes its signed instance surface mid-call."""

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        result = super().apply_closed_bar(state, bar)
        self.multiplier += 1
        return result


class DynamicGetattrEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        helper = getattr(sys.modules[__name__], "mutable_reducer_helper")
        return {"bars": int(state["bars"]) + 1, "close_sum": helper(bar["close"])}


class LocalImportEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        import decimal

        return {
            "bars": int(state["bars"]) + 1,
            "close_sum": float(decimal.Decimal(str(bar["close"]))),
        }


class SysModulesEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        module = sys.modules[__name__]
        return {
            "bars": int(state["bars"]) + int(module.MUTABLE_REDUCER_STEP),
            "close_sum": float(bar["close"]),
        }


class NestedDynamicLookupEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        def hidden_lookup() -> Any:
            return getattr(runtime_module, "_json_safe")

        hidden_lookup()
        return super().apply_closed_bar(state, bar)


class DunderMutationEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        object.__setattr__(self, "hostile", True)
        return super().apply_closed_bar(state, bar)


class AliasedDynamicLookupEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        DYNAMIC_LOOKUP_ALIAS(runtime_module, "_json_safe")
        return super().apply_closed_bar(state, bar)


class DynamicModuleNamespaceEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del state
        return {
            "bars": int(DYNAMIC_MODULE_NAMESPACE.value),
            "close_sum": float(bar["close"]),
        }


class CustomDispatchEngine(CountingEngine):
    def __getattribute__(self, name: str) -> Any:
        return object.__getattribute__(self, name)


class DispatchingEngineMeta(type):
    pass


class CustomMetaclassEngine(CountingEngine, metaclass=DispatchingEngineMeta):
    pass


class CustomMetaclassDependencyEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del state
        return {
            "bars": int(CustomMetaclassEngine is not None),
            "close_sum": float(bar["close"]),
        }


class AttrGetterEngine(CountingEngine):
    apply_closed_bar = operator.attrgetter("close")


class MutableInputAttackEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        state["bars"] = 999  # type: ignore[index]
        return super().apply_closed_bar(state, bar)


BOUND_MUTABLE_CONFIG = {"step": 1}
BOUND_MUTABLE_GET = BOUND_MUTABLE_CONFIG.get


class BoundBuiltinReceiverEngine(CountingEngine):
    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return {
            "bars": int(state["bars"]) + int(BOUND_MUTABLE_GET("step", 0)),
            "close_sum": float(state["close_sum"]) + float(bar["close"]),
        }


def issue_reducer_release(
    engine: CountingEngine,
    *,
    release_id: str = "tests-counting-engine-release-v1",
    issued_at: str = "2026-01-01T00:00:00Z",
) -> AuthenticatedReducerRelease:
    identity = runtime_module._engine_identity(engine)
    manifest = runtime_module._engine_release_manifest(engine)
    configuration_sha = runtime_module._engine_instance_configuration_sha256(engine)
    body = {
        "schema_version": "fxpm.authenticated-reducer-release.v3",
        "release_id": release_id,
        "issued_at": runtime_module._normal_timestamp(issued_at, "issued_at"),
        "python_cache_tag": str(sys.implementation.cache_tag),
        "engine_module": type(engine).__module__,
        "engine_qualname": type(engine).__qualname__,
        "function_allowlist": manifest,
        "declared_identity_sha256": runtime_module._canonical_sha256(identity),
        "implementation_sha256": runtime_module._canonical_sha256(manifest),
        "instance_configuration_sha256": configuration_sha,
        "state_registry_hash": DEFAULT_REGISTRY_HASH,
        "authority_key_id": "tests-reducer-release",
    }
    return AuthenticatedReducerRelease(
        **body,
        signature_base64=_external_signature(
            _REDUCER_PRIVATE_KEY, "fxpm-reducer-release", body
        ),
    )


class FakeStorageAuthority:
    """Test-only bounded stand-in for pm_native_lifecycle's shared authority."""

    identity_sha256 = "2" * 64

    def __init__(
        self, domain_root: Path, *, institution_cap_bytes: int, max_streams: int = 600
    ) -> None:
        self.domain_root = domain_root.resolve(strict=False)
        self.institution_cap_bytes = institution_cap_bytes
        self.max_streams = max_streams
        self._reservations: dict[str, StorageReservation] = {}
        self._witnesses: dict[str, RuntimeMonotonicWitness] = {}
        self._broker_claims: dict[tuple[str, str], BrokerExecutionClaim] = {}
        self._broker_sequence_claims: dict[tuple[str, int], BrokerExecutionClaim] = {}
        self._broker_claim_highwater: dict[str, int] = {}
        self.fail_witness_cas = False

    @staticmethod
    def _receipt_mac(body: Mapping[str, Any]) -> str:
        payload = json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        return hmac.new(KEY, payload, hashlib.sha256).hexdigest()

    def reserve_runtime_storage(
        self,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> StorageReservation:
        root = operational_root.resolve(strict=False)
        try:
            subpath = root.relative_to(self.domain_root)
        except ValueError as exc:
            raise BindingMismatchError("root escaped test storage domain") from exc
        reservation_id = "r-" + stream_identity_sha256[:32]
        existing = self._reservations.get(reservation_id)
        if existing is not None:
            return existing
        if len(self._reservations) >= self.max_streams:
            raise CapacityExceededError("institution stream cap reached")
        if (
            sum(item.reserved_peak_bytes for item in self._reservations.values())
            + requested_peak_bytes
            > self.institution_cap_bytes
        ):
            raise CapacityExceededError("institution byte cap reached")
        body = {
            "authority_identity_sha256": self.identity_sha256,
            "reservation_id": reservation_id,
            "operational_domain_root": str(self.domain_root),
            "stream_subpath": str(subpath),
            "stream_identity_sha256": stream_identity_sha256,
            "reserved_peak_bytes": requested_peak_bytes,
            "institution_cap_bytes": self.institution_cap_bytes,
            "admitted_streams_cap": self.max_streams,
        }
        receipt = StorageReservation(**body, receipt_mac=self._receipt_mac(body))
        self._reservations[reservation_id] = receipt
        witness_body = {
            "authority_identity_sha256": self.identity_sha256,
            "reservation_id": reservation_id,
            "witness_version": 0,
            "writer_epoch": 0,
            "max_bar_sequence": 0,
            "bar_chain_sha256": hashlib.sha256(b"").hexdigest(),
            "checkpoint_generation": 0,
            "checkpoint_sha256": hashlib.sha256(b"").hexdigest(),
            "max_order_operation_sequence": 0,
            "max_order_event_sequence": 0,
            "order_chain_sha256": hashlib.sha256(b"").hexdigest(),
            "reducer_contract_sha256": hashlib.sha256(b"").hexdigest(),
            "fencing_token": 0,
        }
        self._witnesses[reservation_id] = RuntimeMonotonicWitness(
            **witness_body, receipt_mac=self._receipt_mac(witness_body)
        )
        return receipt

    def verify_runtime_storage(
        self,
        reservation: StorageReservation,
        *,
        operational_root: Path,
        stream_identity_sha256: str,
        requested_peak_bytes: int,
    ) -> None:
        body = reservation.canonical()
        supplied_mac = body.pop("receipt_mac")
        assert hmac.compare_digest(self._receipt_mac(body), supplied_mac)
        assert self._reservations[reservation.reservation_id] == reservation
        assert reservation.stream_identity_sha256 == stream_identity_sha256
        assert reservation.reserved_peak_bytes >= requested_peak_bytes
        assert (self.domain_root / reservation.stream_subpath).resolve(
            strict=False
        ) == operational_root.resolve(strict=False)

    def read_runtime_witness(
        self, reservation: StorageReservation
    ) -> RuntimeMonotonicWitness:
        witness = self._witnesses[reservation.reservation_id]
        body = witness.canonical()
        supplied_mac = body.pop("receipt_mac")
        assert hmac.compare_digest(self._receipt_mac(body), supplied_mac)
        return witness

    def cas_runtime_witness(
        self,
        reservation: StorageReservation,
        expected_witness: RuntimeMonotonicWitness,
        proposed_witness: Mapping[str, Any],
    ) -> RuntimeMonotonicWitness:
        if self.fail_witness_cas:
            raise OSError("injected witness CAS outage")
        current = self.read_runtime_witness(reservation)
        if current != expected_witness:
            raise IntegrityError("witness CAS conflict")
        expected_fields = {
            "writer_epoch",
            "max_bar_sequence",
            "bar_chain_sha256",
            "checkpoint_generation",
            "checkpoint_sha256",
            "max_order_operation_sequence",
            "max_order_event_sequence",
            "order_chain_sha256",
            "reducer_contract_sha256",
        }
        assert set(proposed_witness) == expected_fields
        proposed = dict(proposed_witness)
        if current.writer_epoch == 0:
            assert proposed["writer_epoch"] == 1
            assert (
                proposed["reducer_contract_sha256"] != hashlib.sha256(b"").hexdigest()
            )
        else:
            assert proposed["writer_epoch"] == current.writer_epoch
            assert (
                proposed["reducer_contract_sha256"] == current.reducer_contract_sha256
            )
        for counter, digest in (
            ("max_bar_sequence", "bar_chain_sha256"),
            ("checkpoint_generation", "checkpoint_sha256"),
            ("max_order_event_sequence", "order_chain_sha256"),
        ):
            assert proposed[counter] >= getattr(current, counter)
            if proposed[counter] == getattr(current, counter):
                assert proposed[digest] == getattr(current, digest)
            else:
                assert proposed[digest] != getattr(current, digest)
        assert (
            proposed["max_order_operation_sequence"]
            >= current.max_order_operation_sequence
        )
        assert (
            proposed["max_order_event_sequence"]
            >= proposed["max_order_operation_sequence"]
        )
        body = {
            "authority_identity_sha256": self.identity_sha256,
            "reservation_id": reservation.reservation_id,
            "witness_version": current.witness_version + 1,
            **proposed,
            "fencing_token": current.fencing_token + 1,
        }
        advanced = RuntimeMonotonicWitness(**body, receipt_mac=self._receipt_mac(body))
        self._witnesses[reservation.reservation_id] = advanced
        return advanced

    def claim_broker_execution(
        self,
        reservation: StorageReservation,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Mapping[str, Any],
    ) -> BrokerExecutionClaim:
        assert isinstance(broker_highwater_receipt, Mapping)
        self.verify_runtime_storage(
            reservation,
            operational_root=self.domain_root / reservation.stream_subpath,
            stream_identity_sha256=reservation.stream_identity_sha256,
            requested_peak_bytes=reservation.reserved_peak_bytes,
        )
        id_key = (account_scope_sha256, broker_execution_id)
        sequence_key = (account_scope_sha256, broker_execution_sequence)
        existing_id = self._broker_claims.get(id_key)
        existing_sequence = self._broker_sequence_claims.get(sequence_key)
        existing = existing_id or existing_sequence
        if existing is not None:
            expected = (
                account_scope_sha256,
                broker_execution_id,
                broker_execution_sequence,
                reservation.stream_identity_sha256,
                operation_id,
                operation_sequence,
                payload_sha256,
            )
            supplied = (
                existing.account_scope_sha256,
                existing.broker_execution_id,
                existing.broker_execution_sequence,
                existing.stream_identity_sha256,
                existing.operation_id,
                existing.operation_sequence,
                existing.payload_sha256,
            )
            if supplied != expected or existing_id != existing_sequence:
                raise OrderTransitionError(
                    "broker execution is already claimed by another operation"
                )
            return existing
        if len(self._broker_claims) >= 4096:
            raise CapacityExceededError("test broker execution claim cap reached")
        claim_sequence = self._broker_claim_highwater.get(account_scope_sha256, 0) + 1
        body = {
            "authority_identity_sha256": self.identity_sha256,
            "claim_id": f"claim-{account_scope_sha256[:16]}-{claim_sequence:020d}",
            "account_scope_sha256": account_scope_sha256,
            "broker_execution_id": broker_execution_id,
            "broker_execution_sequence": broker_execution_sequence,
            "claim_sequence": claim_sequence,
            "stream_identity_sha256": reservation.stream_identity_sha256,
            "operation_id": operation_id,
            "operation_sequence": operation_sequence,
            "payload_sha256": payload_sha256,
            "fencing_token": claim_sequence,
        }
        claim = BrokerExecutionClaim(**body, receipt_mac=self._receipt_mac(body))
        self._broker_claims[id_key] = claim
        self._broker_sequence_claims[sequence_key] = claim
        self._broker_claim_highwater[account_scope_sha256] = claim_sequence
        return claim

    def verify_broker_execution_claim(
        self,
        claim: BrokerExecutionClaim,
        reservation: StorageReservation,
        *,
        account_scope_sha256: str,
        broker_execution_id: str,
        broker_execution_sequence: int,
        operation_id: str,
        operation_sequence: int,
        payload_sha256: str,
        broker_highwater_receipt: Mapping[str, Any],
    ) -> None:
        assert isinstance(broker_highwater_receipt, Mapping)
        body = claim.canonical()
        supplied_mac = body.pop("receipt_mac")
        assert hmac.compare_digest(self._receipt_mac(body), supplied_mac)
        expected = self._broker_claims[(account_scope_sha256, broker_execution_id)]
        assert expected == claim
        assert (
            self._broker_sequence_claims[
                (account_scope_sha256, broker_execution_sequence)
            ]
            == claim
        )
        assert claim.authority_identity_sha256 == self.identity_sha256
        assert claim.stream_identity_sha256 == reservation.stream_identity_sha256
        assert claim.operation_id == operation_id
        assert claim.operation_sequence == operation_sequence
        assert claim.payload_sha256 == payload_sha256


@pytest.fixture
def limits() -> RuntimeLimits:
    return RuntimeLimits(
        max_db_bytes=2 * 1024 * 1024,
        max_total_storage_bytes=5 * 1024 * 1024,
        max_json_bytes=32 * 1024,
        max_state_bytes=128 * 1024,
        max_bar_events=8,
        max_bar_bytes=128 * 1024,
        max_bar_segments=4,
        max_segment_events=2,
        max_segment_bytes=32 * 1024,
        max_order_operations=3,
        max_order_events=9,
        max_pending_orders=3,
        busy_timeout_seconds=0.2,
        busy_retry_seconds=0.01,
        scan_batch_rows=2,
    )


def _test_source_epoch() -> SourceEpoch:
    return SourceEpoch(
        broker="MT5",
        server="Demo-Server",
        account_id="10001",
        data_source="MT5_NATIVE",
        source_instance_id="terminal-1",
        session_calendar_id="mt5-demo-utc-v1",
        source_artifact_sha256=hashlib.sha256(b"native-v5-artifact").hexdigest(),
        epoch_id="epoch-2026-01",
        started_at="2026-01-01T00:00:00Z",
    )


@pytest.fixture
def source() -> SourceEpoch:
    return _test_source_epoch()


@pytest.fixture
def roster() -> WinnerRoster:
    winner = WinnerRecord(
        cell_id="EURUSD:M5:ALL",
        symbol="EURUSD",
        timeframe="M5",
        regime="ALL",
        strategy_name="ADXDIStrengthStrategy",
        parameters={"nested": {"period": 14}},
        include_regime_controller=True,
        initial_error_bound=1.0,
        tolerance=1e-6,
    )
    return WinnerRoster(
        roster_id="roster-1",
        created_at="2026-01-01T00:00:00Z",
        registry_hash=DEFAULT_REGISTRY_HASH,
        winners=(winner,),
    )


def evidence(source: SourceEpoch, roster: WinnerRoster) -> AuthenticatedStateEvidence:
    return AuthenticatedStateEvidence.issue_for_default_contract(
        KEY,
        source,
        roster,
        "controller:REGIME_HYSTERESIS",
        {"controller_state": "authenticated-replay"},
        symbol="EURUSD",
        timeframe="M5",
    )


def authority(
    tmp_path: Path, limits: RuntimeLimits, *, streams: int = 8
) -> FakeStorageAuthority:
    return FakeStorageAuthority(
        tmp_path,
        institution_cap_bytes=limits.max_total_storage_bytes * streams,
    )


def runtime_kwargs(
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    storage_authority: FakeStorageAuthority,
    *,
    engine: CountingEngine | None = None,
) -> dict[str, Any]:
    resolved_engine = engine or CountingEngine()
    return {
        "hmac_key": KEY,
        "cadence_public_keys": CADENCE_KEYS,
        "reducer_release_public_keys": REDUCER_KEYS,
        "broker_result_public_keys": BROKER_RESULT_KEYS,
        "source": source,
        "roster": roster,
        "symbol": "EURUSD",
        "timeframe": "M5",
        "engine": resolved_engine,
        "reducer_release": issue_reducer_release(resolved_engine),
        "storage_authority": storage_authority,
        "limits": limits,
    }


def provision_ready(
    root: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    storage_authority: FakeStorageAuthority,
    *,
    engine: CountingEngine | None = None,
) -> AuthenticatedStateRuntime:
    runtime = AuthenticatedStateRuntime(
        root,
        **runtime_kwargs(source, roster, limits, storage_authority, engine=engine),
        state_evidence=(evidence(source, roster),),
        provision=True,
    )
    assert runtime.recovery.status == "AUTHENTICATED_BOOTSTRAP_REQUIRED"
    runtime.bootstrap_checkpoint()
    assert runtime.trading_ready
    return runtime


def issue_cadence(
    source: SourceEpoch,
    *,
    receipt_id: str,
    issued_at: str,
    symbol: str,
    timeframe: str,
    source_sequence: int,
    predecessor_source_sequence: int,
    predecessor_close_time: str | None,
    predecessor_phase_seconds: int | None,
    open_time: str,
    close_time: str,
    boundary_kind: str,
    duration_kind: str,
    source_session_id: str,
    source_connection_id: str,
) -> AuthenticatedCadenceReceipt:
    canonical_open = runtime_module._normal_timestamp(open_time, "open_time")
    canonical_timeframe = timeframe.upper()
    nominal = runtime_module._TIMEFRAME_SECONDS[canonical_timeframe]
    observed_phase = (
        int(runtime_module._timestamp_value(canonical_open).timestamp()) % nominal
    )
    if predecessor_phase_seconds is None:
        phase_transition = "GENESIS"
    else:
        delta = (observed_phase - predecessor_phase_seconds) % nominal
        phase_transition = (
            "SAME"
            if delta == 0
            else "FORWARD_ONE_HOUR"
            if delta == 3600
            else "BACKWARD_ONE_HOUR"
            if delta == (nominal - 3600) % nominal
            else "INVALID"
        )
    body = {
        "receipt_id": receipt_id,
        "issued_at": issued_at,
        "acquisition_schema": "fxpm.native-acquisition.v5",
        "activation_schema": "fxpm.native-activation.v5",
        "source_epoch_sha256": source.identity_sha256,
        "source_artifact_sha256": source.source_artifact_sha256,
        "broker": source.broker,
        "server": source.server,
        "account_id": source.account_id,
        "data_source": source.data_source,
        "source_instance_id": source.source_instance_id,
        "session_calendar_id": source.session_calendar_id,
        "symbol": symbol.upper(),
        "timeframe": canonical_timeframe,
        "source_sequence": source_sequence,
        "predecessor_source_sequence": predecessor_source_sequence,
        "predecessor_close_time": predecessor_close_time,
        "predecessor_phase_seconds": predecessor_phase_seconds,
        "open_time": canonical_open,
        "close_time": runtime_module._normal_timestamp(close_time, "close_time"),
        "observed_phase_seconds": observed_phase,
        "phase_transition": phase_transition,
        "boundary_kind": boundary_kind.upper(),
        "duration_kind": duration_kind.upper(),
        "source_session_id": source_session_id,
        "source_connection_id": source_connection_id,
        "authority_key_id": "tests-v5-cadence",
        "schema_version": "fxpm.authenticated-cadence-receipt.v2",
    }
    draft = AuthenticatedCadenceReceipt(
        **body, signature_base64=base64.b64encode(b"0" * 64).decode("ascii")
    )
    return replace(
        draft,
        signature_base64=_external_signature(
            _CADENCE_PRIVATE_KEY,
            "fxpm-v5-acquisition-cadence",
            draft.body(),
        ),
    )


def bar(index: int) -> dict[str, Any]:
    source = _test_source_epoch()
    minute = index * 5
    hour, minute = divmod(minute, 60)
    next_hour, next_minute = divmod(index * 5 + 5, 60)
    value = {
        "closed": True,
        "symbol": "EURUSD",
        "timeframe": "M5",
        "data_source": source.data_source,
        "source_instance_id": source.source_instance_id,
        "session_calendar_id": source.session_calendar_id,
        "source_artifact_sha256": source.source_artifact_sha256,
        "source_epoch_sha256": source.identity_sha256,
        "source_sequence": index + 1,
        "source_session_id": "session-1",
        "source_connection_id": "connection-1",
        "session_gap": False,
        "source_reconnect": False,
        "open_time": f"2025-01-01T{hour:02d}:{minute:02d}:00Z",
        "close_time": f"2025-01-01T{next_hour:02d}:{next_minute:02d}:00Z",
        "open": 1.10 + index / 1000,
        "high": 1.12 + index / 1000,
        "low": 1.08 + index / 1000,
        "close": 1.11 + index / 1000,
        "tick_volume": 100 + index,
        "spread": 2,
        "real_volume": 0,
    }
    value["cadence_receipt"] = issue_cadence(
        source,
        receipt_id=f"m5-cadence-{index + 1}",
        issued_at="2026-01-01T00:00:00Z",
        symbol="EURUSD",
        timeframe="M5",
        source_sequence=index + 1,
        predecessor_source_sequence=index,
        predecessor_close_time=(None if index == 0 else value["open_time"]),
        predecessor_phase_seconds=None if index == 0 else 0,
        open_time=value["open_time"],
        close_time=value["close_time"],
        boundary_kind="GENESIS" if index == 0 else "CONTIGUOUS",
        duration_kind="NOMINAL",
        source_session_id=value["source_session_id"],
        source_connection_id=value["source_connection_id"],
    )
    return value


def resign_bar_cadence(
    value: Mapping[str, Any],
    *,
    predecessor_close_time: str | None = None,
    predecessor_phase_seconds: int | None = None,
    duration_kind: str = "NOMINAL",
) -> dict[str, Any]:
    """Reissue independent cadence evidence after a test mutates its bar."""

    updated = dict(value)
    source = _test_source_epoch()
    source_sequence = int(updated["source_sequence"])
    if source_sequence == 1:
        predecessor_sequence = 0
        predecessor_close = None
        predecessor_phase = None
        boundary_kind = "GENESIS"
    else:
        predecessor_sequence = source_sequence - 1
        predecessor_close = (
            str(updated["open_time"])
            if predecessor_close_time is None
            else predecessor_close_time
        )
        predecessor_phase = (
            0 if predecessor_phase_seconds is None else predecessor_phase_seconds
        )
        boundary_kind = (
            "CONTIGUOUS"
            if predecessor_close == str(updated["open_time"])
            else "SESSION_GAP"
        )
    updated["cadence_receipt"] = issue_cadence(
        source,
        receipt_id=f"resigned-{source_sequence}-{hashlib.sha256(str(updated).encode()).hexdigest()[:12]}",
        issued_at="2026-01-01T00:00:00Z",
        symbol=str(updated["symbol"]),
        timeframe=str(updated["timeframe"]),
        source_sequence=source_sequence,
        predecessor_source_sequence=predecessor_sequence,
        predecessor_close_time=predecessor_close,
        predecessor_phase_seconds=predecessor_phase,
        open_time=str(updated["open_time"]),
        close_time=str(updated["close_time"]),
        boundary_kind=boundary_kind,
        duration_kind=duration_kind,
        source_session_id=str(updated["source_session_id"]),
        source_connection_id=str(updated["source_connection_id"]),
    )
    return updated


def broker_result(
    intent: runtime_module.OrderIntentReceipt,
    result: Mapping[str, Any],
    *,
    highwater_receipt: Mapping[str, Any] | None = None,
) -> AuthenticatedOrderResult:
    normalized_result = dict(result)
    execution_sequence = int(normalized_result.get("ticket", intent.operation_sequence))
    normalized_result.setdefault("ticket", execution_sequence)
    normalized_result.setdefault("retcode", 10009)
    normalized_result.setdefault("status", "FILLED")
    source = _test_source_epoch()
    account_scope_sha256 = runtime_module._canonical_sha256(
        {
            "schema_version": "fxpm.broker-account-scope.v1",
            "broker": source.broker,
            "server": source.server,
            "account_id": source.account_id,
        }
    )
    broker_highwater_receipt = highwater_receipt or {
        "schema_version": "fxpm-broker-account-high-water-test-v1",
        "account_scope_sha256": account_scope_sha256,
        "observed_sequence_highwater": execution_sequence,
        "source_snapshot_sha256": hashlib.sha256(
            f"test-broker-history-{execution_sequence}".encode()
        ).hexdigest(),
        "observed_at": "2026-01-01T00:00:00Z",
        "observation_id": f"test-observation-{execution_sequence}",
        "mac": "a" * 64,
    }
    body = {
        "schema_version": "fxpm.authenticated-order-result.v2",
        "operation_id": intent.operation_id,
        "operation_sequence": intent.operation_sequence,
        "intent_event_hash": intent.event_hash,
        "request_sha256": intent.request_sha256,
        "result_outcome": "EXECUTED",
        "broker_execution_id": str(execution_sequence),
        "broker_execution_sequence": execution_sequence,
        "broker_highwater_receipt": dict(broker_highwater_receipt),
        "reconciliation_receipt_id": f"EXECUTION:{execution_sequence}",
        "reconciliation_method": "MT5_HISTORY_OR_POSITION",
        "reconciled_at": "2026-01-01T00:00:00.000000Z",
        "result": normalized_result,
        "result_sha256": runtime_module._canonical_sha256(normalized_result),
        "authority_key_id": "tests-broker-result",
    }
    draft = AuthenticatedOrderResult(
        **body, signature_base64=base64.b64encode(b"0" * 64).decode("ascii")
    )
    return replace(
        draft,
        signature_base64=_external_signature(
            _BROKER_RESULT_PRIVATE_KEY,
            "fxpm-broker-order-result",
            draft.body(),
        ),
    )


def broker_no_effect(
    intent: runtime_module.OrderIntentReceipt,
    result: Mapping[str, Any],
    *,
    reconciliation_receipt_id: str,
) -> AuthenticatedOrderResult:
    normalized_result = dict(result)
    external_receipt_id = runtime_module._normal_identifier(
        reconciliation_receipt_id, "reconciliation_receipt_id"
    )
    receipt_id = "NO_EFFECT:" + runtime_module._canonical_sha256(
        {
            "schema_version": "fxpm.no-effect-reconciliation-receipt.v2",
            "operation_id": intent.operation_id,
            "request_sha256": intent.request_sha256,
            "external_receipt_id": external_receipt_id,
        }
    )
    body = {
        "schema_version": "fxpm.authenticated-order-result.v2",
        "operation_id": intent.operation_id,
        "operation_sequence": intent.operation_sequence,
        "intent_event_hash": intent.event_hash,
        "request_sha256": intent.request_sha256,
        "result_outcome": "REJECTED_NO_EFFECT",
        "broker_execution_id": None,
        "broker_execution_sequence": None,
        "broker_highwater_receipt": None,
        "reconciliation_receipt_id": receipt_id,
        "reconciliation_method": "MT5_HISTORY_OR_POSITION",
        "reconciled_at": "2026-01-01T00:00:00.000000Z",
        "result": normalized_result,
        "result_sha256": runtime_module._canonical_sha256(normalized_result),
        "authority_key_id": "tests-broker-result",
    }
    draft = AuthenticatedOrderResult(
        **body, signature_base64=base64.b64encode(b"0" * 64).decode("ascii")
    )
    return replace(
        draft,
        signature_base64=_external_signature(
            _BROKER_RESULT_PRIVATE_KEY,
            "fxpm-broker-order-result",
            draft.body(),
        ),
    )


def reopen(
    root: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    storage_authority: FakeStorageAuthority,
    *,
    engine: CountingEngine | None = None,
    key: bytes = KEY,
) -> AuthenticatedStateRuntime:
    kwargs = runtime_kwargs(source, roster, limits, storage_authority, engine=engine)
    kwargs["hmac_key"] = key
    return AuthenticatedStateRuntime(root, **kwargs)


def test_identities_are_canonical_immutable_and_registry_bound(
    source: SourceEpoch, roster: WinnerRoster
) -> None:
    assert len(source.identity_sha256) == 64
    assert len(roster.identity_sha256) == 64
    with pytest.raises(TypeError):
        roster.winners[0].parameters["nested"]["period"] = 99
    with pytest.raises(BindingMismatchError):
        WinnerRoster(
            "bad",
            "2026-01-01T00:00:00Z",
            "0" * 64,
            roster.winners,
        )


def test_external_key_and_explicit_provisioning_are_mandatory(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    with pytest.raises(ValueError):
        AuthenticatedStateRuntime(
            tmp_path / "short-key",
            **{
                **runtime_kwargs(source, roster, limits, storage),
                "hmac_key": b"short",
            },
            provision=True,
        )
    with pytest.raises(RecoveryRequiredError, match="provision=True"):
        AuthenticatedStateRuntime(
            tmp_path / "missing",
            **runtime_kwargs(source, roster, limits, storage),
        )


def test_cloud_synchronised_path_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(StorageLocationError):
        runtime_module.validate_local_storage_root(tmp_path / "OneDrive" / "stream")


def test_authenticated_evidence_is_recomputed_not_a_free_form_hash(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    forged = replace(evidence(source, roster), mac="0" * 64)
    with pytest.raises(AuthenticationError):
        AuthenticatedStateRuntime(
            tmp_path / "forged",
            **runtime_kwargs(source, roster, limits, storage),
            state_evidence=(forged,),
            provision=True,
        )
    runtime = AuthenticatedStateRuntime(
        tmp_path / "missing-evidence",
        **runtime_kwargs(source, roster, limits, storage),
        provision=True,
    )
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        assert "STATE_CONTRACT_QUARANTINED" in " ".join(runtime.recovery.reasons)
        with pytest.raises(RecoveryRequiredError):
            runtime.bootstrap_checkpoint()
    finally:
        runtime.close()


def test_bootstrap_checkpoint_binds_recomputed_resolutions_and_pragmas(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        checkpoint = runtime.active_checkpoint()
        assert checkpoint is not None and checkpoint.slot == "A"
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
            assert (
                connection.execute("PRAGMA application_id").fetchone()[0] == 0x4658504D
            )
            assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
            payload = json.loads(
                connection.execute(
                    "SELECT payload FROM checkpoint_slots WHERE slot='A'"
                ).fetchone()[0]
            )
        finally:
            connection.close()
        decision = payload["state_resolutions"][0]["resolution"]
        assert decision["status"] == "ADMISSIBLE"
        assert decision["decision_hash"]
        assert payload["storage_reservation_sha256"]
    finally:
        runtime.close()


def test_runtime_owned_bar_state_replays_exactly_after_restart(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    first = runtime.append_closed_bar(bar(0))
    second = runtime.append_closed_bar(bar(1))
    assert second.sequence == first.sequence + 1
    runtime.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.trading_ready
        assert runtime.recovery.bar_sequence == 2
        assert runtime.append_closed_bar(bar(1)) == second
        with pytest.raises(IntegrityError):
            runtime.append_closed_bar({**bar(0), "close": 1.115})
    finally:
        runtime.close()


def test_reducer_and_state_capacity_fail_before_durable_append(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = authority(tmp_path, limits)
    engine = CountingEngine()
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage, engine=engine)
    try:

        def fail_reducer(stage: str) -> None:
            if stage == "before_bar_reducer":
                raise RuntimeError("injected reducer failure")

        monkeypatch.setattr(runtime_module, "_fault_point", fail_reducer)
        with pytest.raises(RuntimeError, match="reducer"):
            runtime.append_closed_bar(bar(0))
        assert runtime.recovery.bar_sequence == 0
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        assert runtime.append_closed_bar(bar(0)).sequence == 1
    finally:
        runtime.close()
    oversized = provision_ready(
        tmp_path / "oversized",
        source,
        roster,
        limits,
        storage,
        engine=OversizedStateEngine(),
    )
    try:
        with pytest.raises(CapacityExceededError):
            oversized.append_closed_bar(bar(0))
        assert oversized.recovery.bar_sequence == 0
    finally:
        oversized.close()
    runtime = reopen(root, source, roster, limits, storage, engine=CountingEngine())
    try:
        assert runtime.trading_ready and runtime.recovery.bar_sequence == 1
    finally:
        runtime.close()


def test_bar_caps_are_finite_and_checkpoint_compaction_is_atomic(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
) -> None:
    limits = RuntimeLimits(
        max_db_bytes=2 * 1024 * 1024,
        max_total_storage_bytes=5 * 1024 * 1024,
        max_json_bytes=32 * 1024,
        max_state_bytes=128 * 1024,
        max_bar_events=2,
        max_bar_bytes=64 * 1024,
        max_bar_segments=2,
        max_segment_events=1,
        max_segment_bytes=16 * 1024,
        max_order_operations=2,
        max_order_events=6,
        max_pending_orders=2,
        busy_timeout_seconds=0.2,
        busy_retry_seconds=0.01,
        scan_batch_rows=1,
    )
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        runtime.append_closed_bar(bar(0))
        runtime.append_closed_bar(bar(1))
        with pytest.raises(CapacityExceededError):
            runtime.append_closed_bar(bar(2))
        checkpoint = runtime.publish_checkpoint()
        assert checkpoint.bar_sequence == 2
        assert runtime.append_closed_bar(bar(2)).sequence == 3
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0] == 1
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM checkpoint_slots").fetchone()[
                    0
                ]
                <= 2
            )
        finally:
            connection.close()
    finally:
        runtime.close()


def test_checkpoint_never_accepts_stale_caller_state(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        runtime.append_closed_bar(bar(0))
        runtime.append_closed_bar(bar(1))
        checkpoint = runtime.publish_checkpoint()
        connection = sqlite3.connect(runtime.db_path)
        try:
            payload = json.loads(
                connection.execute(
                    "SELECT payload FROM checkpoint_slots WHERE slot=?",
                    (checkpoint.slot,),
                ).fetchone()[0]
            )
        finally:
            connection.close()
        assert payload["bar_sequence"] == 2
        assert payload["state"]["bars"] == 2
        assert not hasattr(runtime, "checkpoint_state")
    finally:
        runtime.close()


def test_before_commit_fault_rolls_back_checkpoint_and_disk_full_fault_rolls_back_bar(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        runtime.append_closed_bar(bar(0))

        def fail_before(stage: str) -> None:
            if stage == "before_commit":
                raise OSError("simulated disk full")

        monkeypatch.setattr(runtime_module, "_fault_point", fail_before)
        with pytest.raises(OSError, match="disk full"):
            runtime.publish_checkpoint()
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        assert runtime.active_checkpoint().generation == 1
        assert runtime.refresh_recovery().trading_ready
        runtime.publish_checkpoint()
        with pytest.raises(OSError):
            monkeypatch.setattr(runtime_module, "_fault_point", fail_before)
            runtime.append_closed_bar(bar(1))
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        assert runtime.append_closed_bar(bar(1)).sequence == 2
    finally:
        runtime.close()


def test_after_commit_ambiguity_is_idempotent_for_bar_and_checkpoint(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)

    def fail_after(stage: str) -> None:
        if stage == "after_commit":
            raise OSError("ambiguous post-commit failure")

    try:
        monkeypatch.setattr(runtime_module, "_fault_point", fail_after)
        with pytest.raises(OSError, match="post-commit"):
            runtime.append_closed_bar(bar(0))
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        receipt = runtime.append_closed_bar(bar(0))
        assert receipt.sequence == 1
        monkeypatch.setattr(runtime_module, "_fault_point", fail_after)
        with pytest.raises(OSError, match="post-commit"):
            runtime.publish_checkpoint()
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        checkpoint = runtime.publish_checkpoint()
        assert checkpoint.generation == 2
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM checkpoint_slots").fetchone()[
                    0
                ]
                == 2
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0] == 0
            )
        finally:
            connection.close()
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE checkpoint_slots SET mac='00' WHERE slot=(SELECT slot FROM active_checkpoint)",
        "UPDATE runtime_state SET state_sha256='" + "0" * 64 + "' WHERE singleton=1",
    ],
)
def test_authenticated_corruption_reopens_management_only(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    mutation: str,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    runtime.append_closed_bar(bar(0))
    db_path = runtime.db_path
    runtime.close()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(mutation)
        connection.commit()
    finally:
        connection.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        with pytest.raises(RecoveryRequiredError):
            runtime.append_closed_bar(bar(1))
    finally:
        runtime.close()


def test_bar_gap_or_reorder_fails_closed(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    runtime.append_closed_bar(bar(0))
    runtime.append_closed_bar(bar(1))
    db_path = runtime.db_path
    runtime.close()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DELETE FROM bar_events WHERE sequence=1")
        connection.commit()
    finally:
        connection.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
    finally:
        runtime.close()


def test_writer_fence_and_sqlite_busy_retry_are_wall_time_bounded(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    started = time.monotonic()
    with pytest.raises(LockTimeoutError):
        reopen(root, source, roster, limits, storage)
    assert time.monotonic() - started < 1.0
    blocker = sqlite3.connect(runtime.db_path, isolation_level=None, timeout=0)
    try:
        blocker.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        with pytest.raises(LockTimeoutError):
            runtime.append_closed_bar(bar(0))
        assert time.monotonic() - started < 1.0
        blocker.execute("ROLLBACK")
        assert runtime.append_closed_bar(bar(0)).sequence == 1
    finally:
        blocker.close()
        runtime.close()


def test_subprocess_crash_rolls_back_sqlite_journal(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    runtime.append_closed_bar(bar(0))
    db_path = runtime.db_path
    runtime.close()
    script = (
        "import os,sqlite3;"
        f"c=sqlite3.connect({str(db_path)!r},isolation_level=None);"
        "c.execute('PRAGMA journal_mode=DELETE');"
        "c.execute('PRAGMA synchronous=FULL');"
        "c.execute('BEGIN IMMEDIATE');"
        "c.execute(\"UPDATE runtime_state SET state_sha256='" + "0" * 64 + "'\");"
        "os._exit(23)"
    )
    result = subprocess.run([sys.executable, "-c", script], check=False)
    assert result.returncode == 23
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.trading_ready
        assert runtime.recovery.bar_sequence == 1
    finally:
        runtime.close()


def test_order_crash_protocol_exposes_exact_intent_and_result_recovery(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    intent = runtime.begin_order_intent(
        client_sequence=1, action="BUY", request={"lots": 0.25}
    )
    assert runtime.recovery.pending_orders[0].phase == "INTENT"
    runtime.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        pending = runtime.pending_order_recovery()
        assert pending[0].operation_id == intent.operation_id
        assert pending[0].phase == "INTENT"
        assert pending[0].intent_receipt == intent
        result = runtime.record_order_result(
            pending[0].intent_receipt,
            broker_result(
                pending[0].intent_receipt,
                {"ticket": 9001, "status": "FILLED"},
            ),
        )
        assert runtime.recovery.pending_orders[0].phase == "RESULT"
    finally:
        runtime.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        recovered_result = runtime.pending_order_recovery()[0].result_receipt
        assert recovered_result == result
        assert recovered_result is not None
        applied = runtime.apply_order_result_state(recovered_result)
        assert applied.result_event_hash == result.event_hash
        assert runtime.trading_ready
        assert runtime.apply_order_result_state(result) == applied
    finally:
        runtime.close()


def test_result_local_state_and_applied_are_one_atomic_transition(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    intent = runtime.begin_order_intent(
        client_sequence=1, action="SELL", request={"lots": 0.10}
    )
    result = runtime.record_order_result(
        intent, broker_result(intent, {"ticket": 7, "status": "FILLED"})
    )

    def fail_before(stage: str) -> None:
        if stage == "before_commit":
            raise OSError("power loss before order commit")

    try:
        monkeypatch.setattr(runtime_module, "_fault_point", fail_before)
        with pytest.raises(OSError):
            runtime.apply_order_result_state(result)
        monkeypatch.setattr(runtime_module, "_fault_point", lambda stage: None)
        assert runtime.refresh_recovery().pending_orders[0].phase == "RESULT"
        applied = runtime.apply_order_result_state(result)
        assert runtime.trading_ready
        connection = sqlite3.connect(runtime.db_path)
        try:
            phase = connection.execute(
                "SELECT phase FROM order_operations WHERE operation_sequence=1"
            ).fetchone()[0]
            local_count = connection.execute(
                "SELECT COUNT(*) FROM order_local_state"
            ).fetchone()[0]
        finally:
            connection.close()
        assert (phase, local_count) == ("APPLIED", 1)
        assert applied.local_state_sha256
    finally:
        runtime.close()


def test_operation_ids_and_client_sequences_cannot_be_reused_after_retirement(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
) -> None:
    limits = RuntimeLimits(
        max_db_bytes=2 * 1024 * 1024,
        max_total_storage_bytes=5 * 1024 * 1024,
        max_json_bytes=32 * 1024,
        max_state_bytes=128 * 1024,
        max_bar_events=4,
        max_bar_bytes=64 * 1024,
        max_bar_segments=2,
        max_segment_events=2,
        max_segment_bytes=32 * 1024,
        max_order_operations=1,
        max_order_events=3,
        max_pending_orders=1,
        busy_timeout_seconds=0.2,
        busy_retry_seconds=0.01,
        scan_batch_rows=1,
    )
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        first = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        first_result = runtime.record_order_result(
            first, broker_result(first, {"ticket": 1})
        )
        runtime.apply_order_result_state(first_result)
        second = runtime.begin_order_intent(
            client_sequence=2, action="BUY", request={"lots": 0.2}
        )
        assert second.operation_sequence == 2
        assert second.operation_id != first.operation_id
        with pytest.raises(OrderTransitionError, match="retired"):
            runtime.record_order_result(first, broker_result(first, {"ticket": 1}))
        second_result = runtime.record_order_result(
            second, broker_result(second, {"ticket": 2})
        )
        runtime.apply_order_result_state(second_result)
        with pytest.raises(OrderTransitionError, match="stale"):
            runtime.begin_order_intent(
                client_sequence=1, action="BUY", request={"lots": 0.1}
            )
    finally:
        runtime.close()


def test_applied_requires_exact_result_receipt_and_order_corruption_fails_closed(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    intent = runtime.begin_order_intent(
        client_sequence=1, action="BUY", request={"lots": 0.1}
    )
    with pytest.raises(TypeError):
        runtime.record_order_result(intent, {"ticket": 1})
    signed_result = broker_result(intent, {"ticket": 1})
    with pytest.raises(AuthenticationError):
        runtime.record_order_result(
            intent,
            replace(
                signed_result,
                signature_base64=base64.b64encode(b"0" * 64).decode(),
            ),
        )
    result = runtime.record_order_result(intent, signed_result)
    forged = replace(result, event_hash="0" * 64)
    with pytest.raises(OrderTransitionError):
        runtime.apply_order_result_state(forged)
    runtime.apply_order_result_state(result)
    db_path = runtime.db_path
    runtime.close()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "UPDATE order_book_state SET payload_sha256=? WHERE singleton=1",
            ("0" * 64,),
        )
        connection.commit()
    finally:
        connection.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
    finally:
        runtime.close()


def test_order_compaction_keeps_signed_anchor_and_exact_replay(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
) -> None:
    limits = RuntimeLimits(
        max_db_bytes=2 * 1024 * 1024,
        max_total_storage_bytes=5 * 1024 * 1024,
        max_json_bytes=32 * 1024,
        max_state_bytes=128 * 1024,
        max_bar_events=4,
        max_bar_bytes=64 * 1024,
        max_bar_segments=2,
        max_segment_events=2,
        max_segment_bytes=32 * 1024,
        max_order_operations=1,
        max_order_events=3,
        max_pending_orders=1,
        busy_timeout_seconds=0.2,
        busy_retry_seconds=0.01,
        scan_batch_rows=1,
    )
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    one = runtime.begin_order_intent(
        client_sequence=1, action="BUY", request={"lots": 0.1}
    )
    runtime.apply_order_result_state(
        runtime.record_order_result(one, broker_result(one, {"ticket": 1}))
    )
    two = runtime.begin_order_intent(
        client_sequence=2, action="SELL", request={"lots": 0.1}
    )
    runtime.apply_order_result_state(
        runtime.record_order_result(two, broker_result(two, {"ticket": 2}))
    )
    runtime.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.trading_ready
        connection = sqlite3.connect(runtime.db_path)
        try:
            retired = connection.execute(
                "SELECT retired_through_sequence FROM order_meta"
            ).fetchone()[0]
            anchor = connection.execute(
                "SELECT anchor_event_sequence FROM order_anchor_state"
            ).fetchone()[0]
        finally:
            connection.close()
        assert retired == 1
        assert anchor == 3
    finally:
        runtime.close()


def test_shared_storage_authority_bounds_aggregate_admission(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = FakeStorageAuthority(
        tmp_path, institution_cap_bytes=limits.max_total_storage_bytes
    )
    first = provision_ready(tmp_path / "one", source, roster, limits, storage)
    try:
        with pytest.raises(CapacityExceededError, match="institution byte cap"):
            AuthenticatedStateRuntime(
                tmp_path / "two",
                **runtime_kwargs(source, roster, limits, storage),
                state_evidence=(evidence(source, roster),),
                provision=True,
            )
    finally:
        first.close()


def test_sqlite_application_identity_tampering_is_management_only(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    db_path = runtime.db_path
    runtime.close()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("PRAGMA application_id=0")
    finally:
        connection.close()
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        assert "application_id" in " ".join(runtime.recovery.reasons)
    finally:
        runtime.close()


def test_external_witness_rejects_whole_valid_database_rollback_and_id_reuse(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    old_copy = tmp_path / "old-valid.sqlite3"
    runtime.close()
    shutil.copy2(root / "state_runtime.sqlite3", old_copy)
    runtime = reopen(root, source, roster, limits, storage)
    intent = runtime.begin_order_intent(
        client_sequence=1, action="BUY", request={"lots": 0.1}
    )
    witnessed = storage.read_runtime_witness(runtime.storage_reservation)
    assert witnessed.max_order_operation_sequence == 1
    assert witnessed.max_order_event_sequence == 1
    runtime.close()
    shutil.copy2(old_copy, root / "state_runtime.sqlite3")
    runtime = reopen(root, source, roster, limits, storage)
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        assert "MONOTONIC_WITNESS_FAILURE" in " ".join(runtime.recovery.reasons)
        with pytest.raises(RecoveryRequiredError):
            runtime.begin_order_intent(
                client_sequence=1, action="BUY", request={"lots": 0.1}
            )
        stream = runtime.storage_reservation.stream_identity_sha256[:32]
        would_reuse = f"{stream}:{1:020d}:{1:020d}"
        assert would_reuse == intent.operation_id
    finally:
        runtime.close()


def test_intent_is_not_returned_until_external_witness_cas_succeeds(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    storage.fail_witness_cas = True
    try:
        with pytest.raises(runtime_module.PublicationError, match="WITNESS"):
            runtime.begin_order_intent(
                client_sequence=1, action="BUY", request={"lots": 0.1}
            )
        witnessed = storage.read_runtime_witness(runtime.storage_reservation)
        assert witnessed.max_order_operation_sequence == 0
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        storage.fail_witness_cas = False
        recovery = runtime.refresh_recovery()
        assert recovery.pending_orders[0].phase == "INTENT"
        assert (
            storage.read_runtime_witness(
                runtime.storage_reservation
            ).max_order_operation_sequence
            == 1
        )
    finally:
        runtime.close()


def test_reducer_bytecode_contract_rejects_same_claimed_identity(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    runtime.publish_checkpoint()
    runtime.close()
    runtime = reopen(
        root,
        source,
        roster,
        limits,
        storage,
        engine=ImpostorEngine(),
    )
    try:
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        assert "engine_implementation_sha256" in " ".join(runtime.recovery.reasons)
        with pytest.raises(RecoveryRequiredError):
            runtime.append_closed_bar(bar(0))
    finally:
        runtime.close()


def test_live_database_tamper_is_checked_again_before_order_admission(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    connection = sqlite3.connect(runtime.db_path)
    try:
        connection.execute("UPDATE runtime_state SET mac='00' WHERE singleton=1")
        connection.commit()
    finally:
        connection.close()
    try:
        with pytest.raises(AuthenticationError):
            runtime.begin_order_intent(
                client_sequence=1, action="BUY", request={"lots": 0.1}
            )
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM order_operations").fetchone()[
                    0
                ]
                == 0
            )
        finally:
            connection.close()
    finally:
        runtime.close()


def test_operation_ids_are_globally_namespaced_across_streams(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    first = provision_ready(tmp_path / "one", source, roster, limits, storage)
    second = provision_ready(tmp_path / "two", source, roster, limits, storage)
    try:
        first_intent = first.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        second_intent = second.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        assert first_intent.operation_sequence == second_intent.operation_sequence == 1
        assert first_intent.operation_id != second_intent.operation_id
        assert first_intent.client_request_id != second_intent.client_request_id
    finally:
        first.close()
        second.close()


def test_duplicate_broker_execution_sequence_is_rejected_after_application(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        first = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        runtime.apply_order_result_state(
            runtime.record_order_result(
                first, broker_result(first, {"ticket": 777, "status": "FILLED"})
            )
        )
        second = runtime.begin_order_intent(
            client_sequence=2, action="BUY", request={"lots": 0.1}
        )
        with pytest.raises(OrderTransitionError, match="already claimed"):
            runtime.record_order_result(
                second,
                broker_result(second, {"ticket": 777, "status": "FILLED"}),
            )
        valid = runtime.record_order_result(
            second, broker_result(second, {"ticket": 778, "status": "FILLED"})
        )
        runtime.apply_order_result_state(valid)
    finally:
        runtime.close()


def test_database_symlink_escape_is_rejected(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    runtime = provision_ready(root, source, roster, limits, storage)
    db_path = runtime.db_path
    runtime.close()
    outside = tmp_path / "outside.sqlite3"
    db_path.replace(outside)
    try:
        db_path.symlink_to(outside)
    except OSError as exc:
        outside.replace(db_path)
        pytest.skip(f"symbolic links are unavailable: {exc}")
    with pytest.raises(StorageLocationError):
        reopen(root, source, roster, limits, storage)


def test_transitive_reducer_global_and_helper_change_fails_closed(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    global MUTABLE_REDUCER_STEP

    storage = authority(tmp_path, limits)
    root = tmp_path / "stream"
    MUTABLE_REDUCER_STEP = 1
    runtime = provision_ready(
        root,
        source,
        roster,
        limits,
        storage,
        engine=GlobalDependentEngine(),
    )
    runtime.close()
    try:
        MUTABLE_REDUCER_STEP = 100
        runtime = reopen(
            root,
            source,
            roster,
            limits,
            storage,
            engine=GlobalDependentEngine(),
        )
        try:
            assert runtime.recovery.status == "MANAGEMENT_ONLY"
            assert "engine_implementation_sha256" in " ".join(runtime.recovery.reasons)
            with pytest.raises(RecoveryRequiredError):
                runtime.append_closed_bar(bar(0))
        finally:
            runtime.close()
    finally:
        MUTABLE_REDUCER_STEP = 1


def test_transitive_reducer_helper_replacement_and_live_config_fail_closed(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    global mutable_reducer_helper

    original_helper = mutable_reducer_helper
    storage = authority(tmp_path, limits)
    helper_root = tmp_path / "helper-stream"
    runtime = provision_ready(
        helper_root,
        source,
        roster,
        limits,
        storage,
        engine=GlobalDependentEngine(),
    )
    runtime.close()

    def replacement_helper(value: float) -> float:
        return value + 999.0

    try:
        mutable_reducer_helper = replacement_helper
        runtime = reopen(
            helper_root,
            source,
            roster,
            limits,
            storage,
            engine=GlobalDependentEngine(),
        )
        try:
            assert runtime.recovery.status == "MANAGEMENT_ONLY"
        finally:
            runtime.close()
    finally:
        mutable_reducer_helper = original_helper

    configured = ConfiguredEngine(1)
    runtime = provision_ready(
        tmp_path / "configured-stream",
        source,
        roster,
        limits,
        storage,
        engine=configured,
    )
    try:
        configured.multiplier = 100
        with pytest.raises(BindingMismatchError, match="configuration changed"):
            runtime.append_closed_bar(bar(0))
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
    finally:
        runtime.close()


def test_reducer_self_mutation_is_detected_after_call_and_rolled_back(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(
        tmp_path / "self-mutating-stream",
        source,
        roster,
        limits,
        storage,
        engine=SelfMutatingConfiguredEngine(1),
    )
    try:
        with pytest.raises(BindingMismatchError, match="configuration changed"):
            runtime.append_closed_bar(bar(0))
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert (
                connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0] == 0
            )
            assert (
                connection.execute("SELECT bar_sequence FROM runtime_state").fetchone()[
                    0
                ]
                == 0
            )
        finally:
            connection.close()
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "engine",
    (
        DynamicGetattrEngine(),
        LocalImportEngine(),
        SysModulesEngine(),
        NestedDynamicLookupEngine(),
        DunderMutationEngine(),
        AliasedDynamicLookupEngine(),
        DynamicModuleNamespaceEngine(),
        CustomMetaclassDependencyEngine(),
    ),
    ids=(
        "dynamic-getattr",
        "local-import",
        "sys-modules",
        "nested-dynamic-getattr",
        "dunder-mutation",
        "aliased-dynamic-getattr",
        "dynamic-module-namespace",
        "custom-metaclass-dependency",
    ),
)
def test_reducer_release_refuses_dynamic_dependency_escape_hatches(
    engine: CountingEngine,
) -> None:
    with pytest.raises(ValueError, match="unauditable dynamic surface"):
        issue_reducer_release(
            engine,
            release_id="hostile-release",
            issued_at="2026-01-01T00:00:00Z",
        )


@pytest.mark.parametrize(
    ("engine", "message"),
    (
        (CustomDispatchEngine(), "custom instance dispatch"),
        (CustomMetaclassEngine(), "custom metaclass dispatch"),
        (AttrGetterEngine(), "plain Python function"),
    ),
    ids=("custom-getattribute", "custom-metaclass", "operator-attrgetter"),
)
def test_reducer_release_refuses_dispatch_bypass_objects(
    engine: CountingEngine, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        issue_reducer_release(
            engine,
            release_id="dispatch-bypass-release",
            issued_at="2026-01-01T00:00:00Z",
        )


def test_reducer_receives_only_private_immutable_journal_inputs(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    private_source = AuthenticatedStateRuntime._reducer_input_value(source)
    private_roster = AuthenticatedStateRuntime._reducer_input_value(roster)
    assert private_source is not source
    assert private_roster is not roster
    assert private_source.canonical() == source.canonical()
    assert private_roster.canonical() == roster.canonical()
    storage = authority(tmp_path, limits)
    runtime = provision_ready(
        tmp_path / "immutable-input-stream",
        source,
        roster,
        limits,
        storage,
        engine=MutableInputAttackEngine(),
    )
    try:
        supplied = bar(0)
        with pytest.raises(TypeError):
            runtime.append_closed_bar(supplied)
        assert supplied["close"] == 1.11
        with sqlite3.connect(runtime.db_path) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0] == 0
            )
            assert (
                connection.execute("SELECT bar_sequence FROM runtime_state").fetchone()[
                    0
                ]
                == 0
            )
    finally:
        runtime.close()


def test_committed_mutation_never_returns_success_after_management_only_transition(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(
        tmp_path / "post-commit-fence", source, roster, limits, storage
    )
    original_refresh = runtime._refresh_recovery_after_mutation

    def force_management_only() -> runtime_module.RuntimeRecovery:
        observed = original_refresh()
        runtime._recovery = runtime_module.RuntimeRecovery(
            "MANAGEMENT_ONLY",
            ("INJECTED_POST_COMMIT_FENCE",),
            observed.pending_orders,
            observed.bar_sequence,
            observed.checkpoint_generation,
        )
        return runtime._recovery

    monkeypatch.setattr(
        runtime, "_refresh_recovery_after_mutation", force_management_only
    )
    try:
        with pytest.raises(RecoveryRequiredError, match="MANAGEMENT_ONLY"):
            runtime.append_closed_bar(bar(0))
        with sqlite3.connect(runtime.db_path) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM bar_events").fetchone()[0] == 1
            )
        assert runtime.recovery.status == "MANAGEMENT_ONLY"
    finally:
        runtime.close()


def test_reducer_release_is_independently_authenticated(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    kwargs = runtime_kwargs(source, roster, limits, storage)
    kwargs["reducer_release"] = replace(
        kwargs["reducer_release"],
        signature_base64=base64.b64encode(b"0" * 64).decode(),
    )
    with pytest.raises(AuthenticationError, match="Ed25519"):
        AuthenticatedStateRuntime(
            tmp_path / "forged-release",
            **kwargs,
            state_evidence=(evidence(source, roster),),
            provision=True,
        )


def test_reducer_release_rejects_mutable_bound_builtin_receiver() -> None:
    with pytest.raises(ValueError, match="bound builtin receiver"):
        issue_reducer_release(
            BoundBuiltinReceiverEngine(),
            release_id="tests-bound-builtin-receiver-must-fail",
        )


def test_order_only_change_forces_fresh_checkpoint(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        initial = runtime.active_checkpoint()
        assert initial is not None and initial.generation == 1
        intent = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        applied = runtime.apply_order_result_state(
            runtime.record_order_result(
                intent,
                broker_result(intent, {"ticket": 1001, "status": "FILLED"}),
            )
        )
        checkpoint = runtime.publish_checkpoint()
        assert checkpoint.generation == 2
        connection = sqlite3.connect(runtime.db_path)
        try:
            payload = json.loads(
                connection.execute(
                    "SELECT payload FROM checkpoint_slots WHERE slot=?",
                    (checkpoint.slot,),
                ).fetchone()[0]
            )
        finally:
            connection.close()
        assert payload["order_event_sequence"] == 3
        assert payload["order_max_operation_sequence"] == 1
        assert payload["order_max_client_sequence"] == 1
        assert payload["order_max_broker_execution_sequence"] == 1001
        assert payload["order_applied_operation_sequence"] == 1
        assert payload["order_state_sha256"] == applied.local_state_sha256
        assert runtime.publish_checkpoint().generation == 2
    finally:
        runtime.close()


def test_account_wide_execution_claim_rejects_cross_stream_reuse(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    first = provision_ready(tmp_path / "one", source, roster, limits, storage)
    second = provision_ready(tmp_path / "two", source, roster, limits, storage)
    try:
        first_intent = first.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        second_intent = second.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        first.record_order_result(
            first_intent,
            broker_result(first_intent, {"ticket": 777, "status": "FILLED"}),
        )
        with pytest.raises(OrderTransitionError, match="already claimed"):
            second.record_order_result(
                second_intent,
                broker_result(second_intent, {"ticket": 777, "status": "FILLED"}),
            )
        assert second.recovery.pending_orders[0].phase == "INTENT"
    finally:
        first.close()
        second.close()


def test_definitive_no_effect_result_closes_intent_without_execution_claim(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        intent = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        rejected = broker_no_effect(
            intent,
            {"ticket": 0, "retcode": 10019, "status": "REJECTED"},
            reconciliation_receipt_id="mt5-request-9001-retcode-10019",
        )
        applied = runtime.apply_order_result_state(
            runtime.record_order_result(intent, rejected)
        )
        assert applied.operation_id == intent.operation_id
        assert runtime.trading_ready
        assert not storage._broker_claims
        connection = sqlite3.connect(runtime.db_path)
        try:
            assert (
                connection.execute(
                    "SELECT max_broker_execution_sequence FROM order_meta"
                ).fetchone()[0]
                == 0
            )
        finally:
            connection.close()
    finally:
        runtime.close()


def test_broker_semantics_reject_execution_disguised_as_no_effect(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        intent = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        with pytest.raises(ValueError, match="execution-success retcode"):
            broker_no_effect(
                intent,
                {"ticket": 7001, "retcode": 10009, "status": "FILLED"},
                reconciliation_receipt_id="forged-no-effect-fill-7001",
            )
        legitimate = broker_result(
            intent,
            {
                "ticket": 7001,
                "deal": 7001,
                "order": 6999,
                "position": 5001,
                "retcode": 10009,
                "status": "FILLED",
            },
        )
        assert legitimate.broker_execution_sequence == 7001
        with pytest.raises(ValueError, match="identity is inconsistent"):
            broker_result(
                intent,
                {
                    "ticket": 7001,
                    "order": 7002,
                    "retcode": 10009,
                    "status": "FILLED",
                },
            )
        with sqlite3.connect(runtime.db_path) as connection:
            assert (
                connection.execute(
                    "SELECT phase FROM order_operations WHERE operation_sequence=1"
                ).fetchone()[0]
                == "INTENT"
            )
    finally:
        runtime.close()


def test_broker_result_requires_the_registered_external_authority(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        intent = runtime.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        authentic = broker_result(intent, {"ticket": 7001})
        attacker = Ed25519PrivateKey.from_private_bytes(
            hashlib.sha256(b"untrusted-broker-result-authority").digest()
        )
        forged = replace(
            authentic,
            signature_base64=_external_signature(
                attacker, "fxpm-broker-order-result", authentic.body()
            ),
        )
        with pytest.raises(AuthenticationError, match="Ed25519"):
            runtime.record_order_result(intent, forged)
        assert runtime.recovery.pending_orders[0].phase == "INTENT"
    finally:
        runtime.close()


def test_bar_envelope_rejects_wrong_duration_source_and_grid_phase(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    runtime = provision_ready(tmp_path / "stream", source, roster, limits, storage)
    try:
        wrong_duration = resign_bar_cadence(
            {**bar(0), "close_time": "2025-01-02T00:00:00Z"}
        )
        with pytest.raises(AuthenticationError, match="duration.*contract"):
            runtime.append_closed_bar(wrong_duration)
        with pytest.raises(BindingMismatchError, match="data_source"):
            runtime.append_closed_bar({**bar(0), "data_source": "RESAMPLED_M5"})
        with pytest.raises(BindingMismatchError, match="source_instance_id"):
            runtime.append_closed_bar(
                {**bar(0), "source_instance_id": "different-terminal"}
            )
        with pytest.raises(BindingMismatchError, match="session_calendar_id"):
            runtime.append_closed_bar(
                {**bar(0), "session_calendar_id": "different-calendar"}
            )
        missing_identity = dict(bar(0))
        missing_identity.pop("source_epoch_sha256")
        with pytest.raises(BindingMismatchError, match="source_epoch_sha256"):
            runtime.append_closed_bar(missing_identity)
        wrong_phase = resign_bar_cadence(
            {
                **bar(0),
                "open_time": "2025-01-01T00:04:00Z",
                "close_time": "2025-01-01T00:09:00Z",
            }
        )
        with pytest.raises(AuthenticationError, match="grid phase"):
            runtime.append_closed_bar(wrong_phase)
        runtime.append_closed_bar(bar(0))
        with pytest.raises(OrderTransitionError, match="source_sequence"):
            runtime.append_closed_bar(
                resign_bar_cadence({**bar(1), "source_sequence": 1})
            )
        with pytest.raises(OrderTransitionError, match="source_sequence"):
            runtime.append_closed_bar(
                resign_bar_cadence({**bar(1), "source_sequence": 3})
            )
        unmarked_reconnect = resign_bar_cadence(
            {**bar(1), "source_connection_id": "connection-2"}
        )
        with pytest.raises(OrderTransitionError, match="reconnect marker"):
            runtime.append_closed_bar(unmarked_reconnect)
        runtime.append_closed_bar(
            resign_bar_cadence(
                {
                    **bar(1),
                    "source_connection_id": "connection-2",
                    "source_reconnect": True,
                }
            )
        )
        session_gap = resign_bar_cadence(
            {
                **bar(2),
                "open_time": "2025-01-02T00:00:00Z",
                "close_time": "2025-01-02T00:05:00Z",
                "source_connection_id": "connection-2",
                "source_session_id": "session-2",
            },
            predecessor_close_time="2025-01-01T00:10:00Z",
            predecessor_phase_seconds=0,
        )
        with pytest.raises(BindingMismatchError, match="session_gap"):
            runtime.append_closed_bar(session_gap)
        runtime.append_closed_bar({**session_gap, "session_gap": True})
    finally:
        runtime.close()


def test_authenticated_cadence_accepts_h4_d1_dst_but_rejects_relabel_and_one_second_d1(
    source: SourceEpoch,
) -> None:
    h4_shift = issue_cadence(
        source,
        receipt_id="h4-dst-forward",
        issued_at="2026-01-01T00:00:00Z",
        symbol="EURUSD",
        timeframe="H4",
        source_sequence=2,
        predecessor_source_sequence=1,
        predecessor_close_time="2025-03-30T01:00:00Z",
        predecessor_phase_seconds=0,
        open_time="2025-03-30T01:00:00Z",
        close_time="2025-03-30T04:00:00Z",
        boundary_kind="CONTIGUOUS",
        duration_kind="DST_SHORT",
        source_session_id="session-dst",
        source_connection_id="connection-dst",
    )
    h4_shift.verify(CADENCE_KEYS, source, "EURUSD", "H4")
    assert h4_shift.phase_transition == "FORWARD_ONE_HOUR"

    d1_long = issue_cadence(
        source,
        receipt_id="d1-dst-long",
        issued_at="2026-01-01T00:00:00Z",
        symbol="EURUSD",
        timeframe="D1",
        source_sequence=1,
        predecessor_source_sequence=0,
        predecessor_close_time=None,
        predecessor_phase_seconds=None,
        open_time="2025-10-26T00:00:00Z",
        close_time="2025-10-27T01:00:00Z",
        boundary_kind="GENESIS",
        duration_kind="DST_LONG",
        source_session_id="session-dst",
        source_connection_id="connection-dst",
    )
    d1_long.verify(CADENCE_KEYS, source, "EURUSD", "D1")

    one_second_d1 = issue_cadence(
        source,
        receipt_id="forged-one-second-d1",
        issued_at="2026-01-01T00:00:00Z",
        symbol="EURUSD",
        timeframe="D1",
        source_sequence=1,
        predecessor_source_sequence=0,
        predecessor_close_time=None,
        predecessor_phase_seconds=None,
        open_time="2025-01-01T00:00:00Z",
        close_time="2025-01-01T00:00:01Z",
        boundary_kind="GENESIS",
        duration_kind="NOMINAL",
        source_session_id="session-1",
        source_connection_id="connection-1",
    )
    with pytest.raises(AuthenticationError, match="duration.*contract"):
        one_second_d1.verify(CADENCE_KEYS, source, "EURUSD", "D1")

    m5 = bar(0)["cadence_receipt"]
    relabelled = replace(m5, timeframe="D1")
    with pytest.raises(AuthenticationError, match="external Ed25519"):
        relabelled.verify(CADENCE_KEYS, source, "EURUSD", "D1")


def test_cadence_receipt_is_mandatory_forgery_safe_and_separately_keyed(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    storage = authority(tmp_path, limits)
    same_key_kwargs = runtime_kwargs(source, roster, limits, storage)
    same_key_kwargs["broker_result_public_keys"] = {
        "duplicate-cadence-key": next(iter(CADENCE_KEYS.values()))
    }
    with pytest.raises(AuthenticationError, match="independent"):
        AuthenticatedStateRuntime(
            tmp_path / "same-key",
            **same_key_kwargs,
            state_evidence=(evidence(source, roster),),
            provision=True,
        )

    runtime = provision_ready(
        tmp_path / "cadence-stream", source, roster, limits, storage
    )
    try:
        assert runtime.production_integrated is False
        missing = bar(0)
        missing.pop("cadence_receipt")
        with pytest.raises(AuthenticationError, match="cadence receipt"):
            runtime.append_closed_bar(missing)
        forged = bar(0)
        forged["cadence_receipt"] = replace(
            forged["cadence_receipt"],
            signature_base64=base64.b64encode(b"0" * 64).decode(),
        )
        with pytest.raises(AuthenticationError, match="Ed25519"):
            runtime.append_closed_bar(forged)
    finally:
        runtime.close()


def test_external_evidence_adapters_expose_no_local_signing_oracle() -> None:
    assert not hasattr(AuthenticatedCadenceReceipt, "issue")
    assert not hasattr(AuthenticatedReducerRelease, "issue")
    assert not hasattr(AuthenticatedOrderResult, "issue")
    assert not hasattr(AuthenticatedOrderResult, "issue_no_effect")


def test_incremental_heads_have_near_linear_verified_work_and_scheduled_replay(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    scale_limits = replace(
        limits,
        max_bar_events=64,
        max_bar_bytes=512 * 1024,
        max_bar_segments=8,
        max_segment_events=8,
        max_segment_bytes=64 * 1024,
        full_audit_interval_mutations=16,
    )
    storage = authority(tmp_path, scale_limits)
    runtime = provision_ready(
        tmp_path / "incremental-scaling",
        source,
        roster,
        scale_limits,
        storage,
    )
    try:
        before = dict(runtime.verification_metrics)
        mutation_count = 31
        for index in range(mutation_count):
            assert runtime.append_closed_bar(bar(index)).sequence == index + 1
        after = dict(runtime.verification_metrics)
        full_audits = after["full_audits"] - before["full_audits"]
        incremental = after["incremental_audits"] - before["incremental_audits"]
        replayed_bars = after["replayed_bar_events"] - before["replayed_bar_events"]
        assert full_audits == 1
        assert replayed_bars == 16
        assert incremental <= mutation_count * 2
        assert incremental + replayed_bars <= mutation_count * 3
        assert runtime.trading_ready
    finally:
        runtime.close()


def test_windows_mapped_network_drive_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if runtime_module.os.name != "nt":
        pytest.skip("Windows drive classification is platform-specific")
    import ctypes

    monkeypatch.setattr(
        ctypes.windll.kernel32,
        "GetDriveTypeW",
        lambda root: 4,  # DRIVE_REMOTE, including mapped network letters
    )
    with pytest.raises(StorageLocationError, match="fixed local volume"):
        runtime_module.validate_local_storage_root(tmp_path)


def test_canonical_lifecycle_authority_executes_runtime_claim_contract(
    tmp_path: Path,
    source: SourceEpoch,
    roster: WinnerRoster,
    limits: RuntimeLimits,
) -> None:
    from pm_native_lifecycle import (
        BrokerAccountHighWaterReceipt,
        LifecycleLimits,
        NativeLifecycleAuthority,
        OperationalDomain,
    )

    native_root = tmp_path / "native"
    cold_root = tmp_path / "cold"
    operational_root = tmp_path / "operational"
    for root in (native_root, cold_root, operational_root):
        root.mkdir()
    lifecycle_limits = LifecycleLimits(
        max_hot_bytes=16 * 1024 * 1024,
        max_cold_bytes=16 * 1024 * 1024,
        min_volume_free_bytes=1,
        max_hot_files=10_000,
        max_cold_files=10_000,
        max_complete_hot_runs=20,
        max_inflight_runs=4,
        max_operations=200,
        max_active_operations=16,
        max_payload_bytes=64 * 1024,
        max_files_per_operation=2_000,
        max_pins=32,
        max_pin_payload_bytes=64 * 1024,
        max_pointer_bytes=64 * 1024,
        max_scan_entries=20_000,
        max_db_bytes=2 * 1024 * 1024,
        page_size=4096,
        journal_size_limit=64 * 1024,
        db_busy_deadline_seconds=2.0,
        lock_deadline_seconds=4.0,
        lease_seconds=30.0,
    )
    lifecycle = NativeLifecycleAuthority(
        native_root=native_root,
        cold_root=cold_root,
        state_root=tmp_path / "authority-state",
        limits=lifecycle_limits,
        auth_key=b"fxpm-native-lifecycle-test-key!!",
        broker_highwater_key=b"independent-broker-highwater-key!!",
        operational_domains={
            "runtime_state": OperationalDomain(
                operational_root,
                max_bytes=limits.max_total_storage_bytes * 3,
                max_files=100,
                max_active_reservations=3,
            )
        },
        provision=True,
        provisioning_identity="pytest-state-runtime-integration-v1",
    )
    first = provision_ready(operational_root / "one", source, roster, limits, lifecycle)
    second = provision_ready(
        operational_root / "two", source, roster, limits, lifecycle
    )
    try:

        def real_highwater(
            runtime: AuthenticatedStateRuntime, sequence: int
        ) -> Mapping[str, Any]:
            return BrokerAccountHighWaterReceipt.issue(
                b"independent-broker-highwater-key!!",
                account_scope_sha256=runtime.execution_account_scope_sha256,
                observed_sequence_highwater=sequence,
                source_snapshot_sha256=hashlib.sha256(
                    f"integration-broker-history-{sequence}".encode()
                ).hexdigest(),
                observed_at="2026-01-01T00:00:00Z",
                observation_id=f"integration-{sequence}",
            ).canonical()

        first_intent = first.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        first.apply_order_result_state(
            first.record_order_result(
                first_intent,
                broker_result(
                    first_intent,
                    {"ticket": 7001, "status": "FILLED"},
                    highwater_receipt=real_highwater(first, 7001),
                ),
            )
        )
        second_intent = second.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        with pytest.raises(OrderTransitionError, match="already claimed"):
            second.record_order_result(
                second_intent,
                broker_result(
                    second_intent,
                    {"ticket": 7001, "status": "FILLED"},
                    highwater_receipt=real_highwater(second, 7001),
                ),
            )
    finally:
        first.close()
        second.close()
