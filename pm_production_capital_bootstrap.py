"""Run-start, fail-closed bootstrap for production Capital V8 authority.

The bootstrap freezes the *policy* candidate roster before any historical or
current search evidence may be opened.  Phase-local ``CandidateTrade`` rosters
do not exist at that point; the returned factory derives them only from the
coordinator-owned historical/current states and the coordinator independently
reconstructs them again before replay.

No object produced here selects a numeric cap.  Capital V8 remains the sole
owner of selection after genuine synchronized replay.  Demo runs may use
ephemeral run-scoped Ed25519 keys.  Funded runs have no local authorization
signing path: an independently provisioned public key and a frozen path to a
pre-existing externally signed envelope are mandatory.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import stat
import threading
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

import pm_cap_calibration as cap
import pm_cap_replay_producer as cap_replay
import pm_capital_authority_factory as authority_factory
import pm_authority_spool as authority_spool
import pm_causal_process as causal
import pm_portfolio_replay as replay
import pm_production_authority_coordinator as coordinator


CAPITAL_BOOTSTRAP_SCHEMA = "fxpm.production-capital-bootstrap.v1"
CAPITAL_BOOTSTRAP_IDENTITY_SCHEMA = "fxpm.production-capital-bootstrap-identity.v1"
CAPITAL_FORWARD_PHASE_BINDING_SCHEMA = (
    "fxpm.production-capital-forward-phase-binding.v1"
)
FUNDED_AUTHORIZATION_TRUST_SCHEMA = "fxpm.funded-authorization-trust.v1"
FUNDED_AUTHORIZATION_REQUEST_SCHEMA = "fxpm.funded-authorization-request.v1"
FUNDED_AUTHORIZATION_REQUEST_ISSUED_SCHEMA = (
    "fxpm.funded-authorization-request-issued.v1"
)
FUNDED_AUTHORIZATION_RESPONSE_SCHEMA = "fxpm.funded-authorization-response.v1"
FUNDED_AUTHORIZATION_RESPONSE_SIGNATURE_KIND = "fxpm-funded-authorization-response-v1"
FUNDED_AUTHORIZATION_REQUEST_ISSUED_SIGNATURE_KIND = (
    "fxpm-funded-authorization-request-issued-v1"
)
FUNDED_AUTHORIZATION_CHANNEL_POLICY = (
    "content-addressed-request-deterministic-ingress-freeze-v1"
)
DEMO_PROBATION_SCOPE = authority_factory.DEMO_PROBATION_SCOPE
FUNDED_LIVE_SCOPE = authority_factory.FUNDED_LIVE_SCOPE
MAX_BOOTSTRAP_JSON_BYTES = 4 * 1024 * 1024
MAX_BOOTSTRAP_GENERATION_BYTES = 8 * 1024 * 1024
MAX_BOOTSTRAP_ARTIFACTS = 2
MAX_BOOTSTRAP_DIRECTORIES = 16
MAX_BOOTSTRAP_DEPTH = 4
MAX_BOOTSTRAP_PATH_CHARS = 1024
MAX_AUTHORIZATION_EXCHANGE_ARTIFACTS = 4
MAX_AUTHORIZATION_EXCHANGE_BYTES = 16 * 1024 * 1024
MAX_AUTHORIZATION_EXCHANGE_DIRECTORIES = 16
MAX_AUTHORIZATION_EXCHANGE_DEPTH = 6
MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS = 1024
MAX_AUTHORIZATION_EXCHANGE_DERIVED_PATH_CHARS = 128
MAX_NATIVE_SOURCE_CELLS = 10_000
_GENERATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", re.ASCII)
_HEX = frozenset("0123456789abcdef")
_WINDOW_KEYS = frozenset({"start_date", "end_date"})
_CONTRACT_KEYS = frozenset(
    {
        "book_contract_version",
        "refit_contract_version",
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    }
)
_NATIVE_IDENTITY_KEYS = frozenset(
    {
        "native_activation_id",
        "native_activation_manifest_sha256",
        "native_activation_roster_sha256",
        "native_bars_sha256_by_source",
        "native_sidecar_sha256_by_source",
    }
)
_ARTIFACT_DESCRIPTOR_KEYS = frozenset(
    {"format", "relative_path", "sha256", "size_bytes"}
)
_ARTIFACT_FORMAT = "fxpm-canonical-json-v1"
_CAPITAL_FORWARD_PHASE_BINDING_KEYS = frozenset(
    {
        "schema_version",
        "generation_id",
        "generation_context_sha256",
        "config_sha256",
        "native_universe_sha256",
        "authorization_scope",
        "bootstrap_identity",
        "bootstrap_identity_sha256",
        "bootstrap_generation_root",
        "bootstrap_receipt_descriptor",
        "capital_precommitment_descriptor",
        "capital_precommitment",
        "capital_precommitment_body_sha256",
        "capital_precommitment_envelope_sha256",
        "anchor_plan",
        "current_selection_window",
        "contract_lineage",
        "margin_terms_sha256",
        "broker_terms_sha256",
        "created_before_process_evidence_access",
        "confers_numeric_capital_publication_or_trading_authority",
    }
)
_FUNDED_AUTHORIZATION_TRUST_KEYS = frozenset(
    {
        "schema_version",
        "authorization_key_id",
        "authorization_public_key_sha256",
        "authorization_exchange_root",
        "channel_policy",
    }
)
_FUNDED_AUTHORIZATION_REQUEST_KEYS = frozenset(
    {
        "schema_version",
        "generation_id",
        "generation_context_sha256",
        "bootstrap_identity_sha256",
        "authorization_key_id",
        "authorization_body",
        "authorization_body_sha256",
        "response_signature_kind",
    }
)
_FUNDED_AUTHORIZATION_RESPONSE_KEYS = frozenset(
    {
        "schema_version",
        "request_sha256",
        "generation_id",
        "generation_context_sha256",
        "bootstrap_identity_sha256",
        "authorization_body_sha256",
        "authorization_envelope",
    }
)
_FUNDED_AUTHORIZATION_REQUEST_ISSUED_KEYS = frozenset(
    {
        "schema_version",
        "request_sha256",
        "generation_id",
        "generation_context_sha256",
        "bootstrap_identity_sha256",
        "authorization_body_sha256",
        "issuance_key_id",
    }
)
_SIGNED_ENVELOPE_KEYS = frozenset(
    {"schema_version", "key_id", "body", "signature_base64"}
)
_AUTHORIZATION_BODY_FIELDS = frozenset(
    {
        "schema_version",
        "authority_key_id",
        "pending_artifact_content_sha256",
        "calibration_decision_sha256",
        "selected_policy_point_id",
        "selected_max_total_stop_risk_pct",
        "selected_capital_envelope",
        "source_lineage_sha256",
        "constraints_sha256",
        "issued_at_utc",
        "expires_at_utc",
    }
)
_REPLAY_BODY_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "precommitment_sha256",
        "precommitment_envelope_sha256",
        "generated_at_utc",
    }
)
_BOOTSTRAP_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "authorization_scope",
        "identity",
        "identity_sha256",
        "precommitment_body_sha256",
        "precommitment_envelope_sha256",
        "precommitment_artifact",
        "trusted_role_keys",
        "created_before_process_evidence_access",
        "confers_numeric_capital_authority",
        "confers_funded_live_authority",
    }
)


class ProductionCapitalBootstrapError(ValueError):
    """Typed run-start capital bootstrap failure."""

    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


def _fail(code: str) -> None:
    raise ProductionCapitalBootstrapError(code)


@dataclass(frozen=True, slots=True)
class FundedAuthorityBoundary:
    """Externally provisioned funded trust boundary.

    The replay/precommit private keys sign evidence and the pre-search plan;
    neither can authorize a funded numeric cap.  Funded authorization exposes
    only a public-key source.  The independently frozen config pins that key
    and an immutable exchange-root/channel policy.  Post-calibration requests
    are content-addressed there; an external authority supplies the signed
    response on a deterministic request-derived ingress path.  The bootstrap
    never receives a funded signing callback or other signing capability.
    """

    precommitment_signing_key_source: authority_factory.SigningKeySource
    replay_signing_key_source: authority_factory.SigningKeySource
    authorization_public_key_source: authority_factory.KeyMaterialSource


@dataclass(frozen=True, slots=True)
class BootstrapArtifactDescriptor:
    format: str
    relative_path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CapitalAuthorizationScopeBinding:
    authorization_scope: str
    bootstrap_identity_sha256: str
    config_sha256: str
    authority_key_id: str
    authority_public_key_sha256: str

    def require(self, authorization_scope: str) -> None:
        if authorization_scope != self.authorization_scope:
            _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH")


@dataclass(frozen=True, slots=True)
class ProductionCapitalBootstrap:
    generation_id: str
    authorization_scope: str
    generation_root: Path
    identity_sha256: str
    candidate_policy_roster_sha256: str
    precommitment: BootstrapArtifactDescriptor
    bootstrap_receipt: BootstrapArtifactDescriptor
    forward_phase_binding: "CapitalForwardPhaseBinding"
    capital_factory: Callable[
        [coordinator.CapitalAuthorityRequest], coordinator.CapitalAuthorityInputs
    ]

    def require_authorization_scope(self, value: str) -> None:
        if value != self.authorization_scope:
            _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH")


_CAPITAL_FORWARD_PHASE_BINDING_SEAL = object()


@dataclass(frozen=True, slots=True)
class CapitalForwardPhaseBinding:
    """Verifier-issued pre-evidence bridge from Capital bootstrap to Forward.

    The exact signed Capital precommitment remains the authority.  This object
    merely carries a bounded canonical projection plus the content-addressed
    descriptors needed to reopen and verify that signature after construction.
    """

    _body_json: bytes
    receipt_sha256: str
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))


def _canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ProductionCapitalBootstrapError("CAPITAL_BOOTSTRAP_JSON_INVALID") from exc
    if not 0 < len(encoded) <= MAX_BOOTSTRAP_JSON_BYTES:
        _fail("CAPITAL_BOOTSTRAP_JSON_BOUND_EXCEEDED")
    return encoded


def _snapshot(value: Any) -> Any:
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _object(value: Any, code: str) -> dict[str, Any]:
    copied = _snapshot(value)
    if type(copied) is not dict:
        _fail(code)
    return copied


def _sha_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        _fail(code)
    return value


def _domain_signature_payload(
    domain: bytes, key_id: str, body: Mapping[str, Any], code: str
) -> bytes:
    if (
        type(key_id) is not str
        or not key_id
        or "\x00" in key_id
        or not key_id.isascii()
    ):
        _fail(code)
    exact = _object(body, code)
    return domain + b"\x00" + key_id.encode("ascii") + b"\x00" + _canonical_bytes(exact)


def funded_authorization_response_signature_payload(
    key_id: str, body: Mapping[str, Any]
) -> bytes:
    """Return the domain-separated payload an external authority must sign."""

    return _domain_signature_payload(
        b"FXPM_FUNDED_AUTHORIZATION_RESPONSE_V1",
        key_id,
        body,
        "FUNDED_AUTHORIZATION_RESPONSE_BODY_INVALID",
    )


def _request_issued_signature_payload(key_id: str, body: Mapping[str, Any]) -> bytes:
    return _domain_signature_payload(
        b"FXPM_FUNDED_AUTHORIZATION_REQUEST_ISSUED_V1",
        key_id,
        body,
        "FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID",
    )


def _funded_authorization_trust(config: Mapping[str, Any]) -> dict[str, Any]:
    pipeline = config.get("pipeline")
    if type(pipeline) is not dict:
        _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISSING")
    trust = _object(
        pipeline.get("funded_authorization_trust"),
        "CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISSING",
    )
    if (
        set(trust) != _FUNDED_AUTHORIZATION_TRUST_KEYS
        or trust.get("schema_version") != FUNDED_AUTHORIZATION_TRUST_SCHEMA
        or type(trust.get("authorization_key_id")) is not str
        or not trust["authorization_key_id"]
        or type(trust.get("authorization_exchange_root")) is not str
        or not trust["authorization_exchange_root"]
        or trust.get("channel_policy") != FUNDED_AUTHORIZATION_CHANNEL_POLICY
    ):
        _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_INVALID")
    _sha(
        trust.get("authorization_public_key_sha256"),
        "CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_INVALID",
    )
    return trust


def _utc_now() -> datetime:
    """Trusted production clock seam; tests monkeypatch the module function."""

    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_text(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _validation_time(value: Any) -> tuple[str, datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None or value.microsecond:
            _fail("CAPITAL_BOOTSTRAP_VALIDATION_TIME_INVALID")
        normalized = value.astimezone(timezone.utc)
        return _utc_text(normalized), normalized
    try:
        return authority_factory._canonical_utc(
            value, "CAPITAL_BOOTSTRAP_VALIDATION_TIME_INVALID"
        )
    except authority_factory.CapitalAuthorityFactoryError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_VALIDATION_TIME_INVALID"
        ) from exc


def _window(value: Any, code: str) -> tuple[dict[str, str], date, date]:
    raw = _object(value, code)
    if set(raw) != _WINDOW_KEYS:
        _fail(code)
    try:
        start = date.fromisoformat(raw["start_date"])
        end = date.fromisoformat(raw["end_date"])
    except (TypeError, ValueError) as exc:
        raise ProductionCapitalBootstrapError(code) from exc
    if (
        raw["start_date"] != start.isoformat()
        or raw["end_date"] != end.isoformat()
        or end <= start
    ):
        _fail(code)
    return raw, start, end


def _validated_native_identity(value: Any) -> dict[str, Any]:
    identity = _object(value, "CAPITAL_BOOTSTRAP_NATIVE_IDENTITY_INVALID")
    if set(identity) != _NATIVE_IDENTITY_KEYS:
        _fail("CAPITAL_BOOTSTRAP_NATIVE_IDENTITY_INVALID")
    activation_id = identity["native_activation_id"]
    if type(activation_id) is not str or not 1 <= len(activation_id) <= 256:
        _fail("CAPITAL_BOOTSTRAP_NATIVE_IDENTITY_INVALID")
    _sha(
        identity["native_activation_manifest_sha256"],
        "CAPITAL_BOOTSTRAP_NATIVE_MANIFEST_HASH_INVALID",
    )
    _sha(
        identity["native_activation_roster_sha256"],
        "CAPITAL_BOOTSTRAP_NATIVE_ROSTER_HASH_INVALID",
    )
    bars = identity["native_bars_sha256_by_source"]
    sidecars = identity["native_sidecar_sha256_by_source"]
    if (
        type(bars) is not dict
        or type(sidecars) is not dict
        or not bars
        or set(bars) != set(sidecars)
        or len(bars) > MAX_NATIVE_SOURCE_CELLS
    ):
        _fail("CAPITAL_BOOTSTRAP_NATIVE_SOURCE_ROSTER_INVALID")
    for source_id in sorted(bars):
        if (
            type(source_id) is not str
            or not 1 <= len(source_id) <= 256
            or source_id != source_id.strip()
        ):
            _fail("CAPITAL_BOOTSTRAP_NATIVE_SOURCE_ROSTER_INVALID")
        _sha(bars[source_id], "CAPITAL_BOOTSTRAP_NATIVE_BARS_HASH_INVALID")
        _sha(sidecars[source_id], "CAPITAL_BOOTSTRAP_NATIVE_SIDECAR_HASH_INVALID")
    return identity


def _validated_contract_lineage(
    value: Any, native_identity: Mapping[str, Any]
) -> dict[str, Any]:
    contract = _object(value, "CAPITAL_BOOTSTRAP_CONTRACT_LINEAGE_INVALID")
    if set(contract) != _CONTRACT_KEYS:
        _fail("CAPITAL_BOOTSTRAP_CONTRACT_LINEAGE_INVALID")
    for name in ("book_contract_version", "refit_contract_version"):
        if type(contract[name]) is not str or not contract[name]:
            _fail("CAPITAL_BOOTSTRAP_CONTRACT_LINEAGE_INVALID")
    for name in _CONTRACT_KEYS - {
        "book_contract_version",
        "refit_contract_version",
    }:
        _sha(contract[name], f"CAPITAL_BOOTSTRAP_{name.upper()}_INVALID")
    if contract["native_source_contract_sha256"] != _sha_json(native_identity):
        _fail("CAPITAL_BOOTSTRAP_NATIVE_CONTRACT_MISMATCH")
    if (
        contract["replay_contract_sha256"]
        != coordinator._capital_replay_contract_sha256()
    ):
        _fail("CAPITAL_BOOTSTRAP_REPLAY_CONTRACT_MISMATCH")
    return contract


def _capital_anchor_plan(
    process_anchors: Sequence[Mapping[str, Any]],
    inner_windows_by_anchor: Mapping[str, Mapping[str, str]],
) -> list[dict[str, Any]]:
    anchor_ids = [str(anchor["anchor_id"]) for anchor in process_anchors]
    if type(inner_windows_by_anchor) is not dict or set(inner_windows_by_anchor) != set(
        anchor_ids
    ):
        _fail("CAPITAL_BOOTSTRAP_INNER_WINDOW_ROSTER_INVALID")
    result: list[dict[str, Any]] = []
    for anchor in process_anchors:
        anchor_id = anchor["anchor_id"]
        inner, _, inner_end = _window(
            inner_windows_by_anchor[anchor_id],
            "CAPITAL_BOOTSTRAP_INNER_WINDOW_INVALID",
        )
        try:
            outer_start = date.fromisoformat(anchor["forward_start_date"])
            outer_end_inclusive = date.fromisoformat(anchor["forward_end_date"])
        except (TypeError, ValueError) as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_PROCESS_ANCHOR_INVALID"
            ) from exc
        outer_end = outer_end_inclusive + timedelta(days=1)
        if inner_end > outer_start:
            _fail("CAPITAL_BOOTSTRAP_INNER_OUTER_LEAKAGE")
        result.append(
            {
                "anchor_id": anchor_id,
                "inner_window": inner,
                "outer_window": {
                    "start_date": outer_start.isoformat(),
                    "end_date": outer_end.isoformat(),
                },
            }
        )
    return result


def _absolute_root(value: Path, code: str) -> Path:
    try:
        root = authority_factory._absolute_path(Path(value), code)
    except authority_factory.CapitalAuthorityFactoryError as exc:
        raise ProductionCapitalBootstrapError(code) from exc
    if root.exists() and (authority_factory._is_reparse(root) or not root.is_dir()):
        _fail(code)
    return root


def _ensure_directory(path: Path) -> None:
    if not path.is_absolute():
        _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            current.mkdir()
        except FileExistsError:
            pass
        if authority_factory._is_reparse(current) or not current.is_dir():
            _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")


def _existing_regular_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if len(str(root)) > MAX_BOOTSTRAP_PATH_CHARS:
        _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_LENGTH_EXCEEDED")
    files: list[Path] = []
    total_bytes = 0
    directory_count = 1
    stack = [(root, 0)]
    while stack:
        directory, depth = stack.pop()
        if depth > MAX_BOOTSTRAP_DEPTH:
            _fail("CAPITAL_BOOTSTRAP_STORAGE_DEPTH_EXCEEDED")
        if authority_factory._is_reparse(directory) or not directory.is_dir():
            _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    item = directory / entry.name
                    if len(str(item)) > MAX_BOOTSTRAP_PATH_CHARS:
                        _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_LENGTH_EXCEEDED")
                    info = os.lstat(item)
                    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
                    if stat.S_ISLNK(info.st_mode) or attributes & 0x400:
                        _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")
                    if stat.S_ISDIR(info.st_mode):
                        child_depth = depth + 1
                        if child_depth > MAX_BOOTSTRAP_DEPTH:
                            _fail("CAPITAL_BOOTSTRAP_STORAGE_DEPTH_EXCEEDED")
                        directory_count += 1
                        if directory_count > MAX_BOOTSTRAP_DIRECTORIES:
                            _fail("CAPITAL_BOOTSTRAP_STORAGE_DIRECTORY_BOUND_EXCEEDED")
                        stack.append((item, child_depth))
                    elif stat.S_ISREG(info.st_mode):
                        if info.st_nlink != 1:
                            _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")
                        files.append(item)
                        total_bytes += info.st_size
                    else:
                        _fail("CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID")
                    if len(files) > MAX_BOOTSTRAP_ARTIFACTS:
                        _fail("CAPITAL_BOOTSTRAP_STORAGE_ARTIFACT_BOUND_EXCEEDED")
                    if total_bytes > MAX_BOOTSTRAP_GENERATION_BYTES:
                        _fail("CAPITAL_BOOTSTRAP_STORAGE_BYTE_BOUND_EXCEEDED")
        except OSError as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_STORAGE_PATH_INVALID"
            ) from exc
    return files


def _file_identity(info: os.stat_result) -> tuple[int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _read_exact_regular_file(path: Path) -> bytes:
    try:
        authority_factory._reject_reparse_chain(path)
        path_before = path.lstat()
    except OSError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_ARTIFACT_INVALID"
        ) from exc
    if (
        authority_factory._is_reparse(path)
        or not stat.S_ISREG(path_before.st_mode)
        or path_before.st_nlink != 1
    ):
        _fail("CAPITAL_BOOTSTRAP_ARTIFACT_INVALID")
    if not 0 < path_before.st_size <= MAX_BOOTSTRAP_JSON_BYTES:
        _fail("CAPITAL_BOOTSTRAP_ARTIFACT_SIZE_INVALID")
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_ARTIFACT_INVALID"
        ) from exc
    chunks: list[bytes] = []
    total = 0
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            _fail("CAPITAL_BOOTSTRAP_ARTIFACT_INVALID")
        while True:
            chunk = os.read(
                descriptor,
                min(64 * 1024, MAX_BOOTSTRAP_JSON_BYTES + 1 - total),
            )
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_BOOTSTRAP_JSON_BYTES:
                _fail("CAPITAL_BOOTSTRAP_ARTIFACT_SIZE_INVALID")
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_after = path.lstat()
    if (
        after.st_nlink != 1
        or path_after.st_nlink != 1
        or _file_identity(path_before) != _file_identity(before)
        or _file_identity(before) != _file_identity(after)
        or _file_identity(after) != _file_identity(path_after)
        or total != before.st_size
    ):
        _fail("CAPITAL_BOOTSTRAP_ARTIFACT_CHANGED_DURING_READ")
    return b"".join(chunks)


def _content_descriptor(root: Path, value: Any) -> BootstrapArtifactDescriptor:
    encoded = _canonical_bytes(value)
    digest = hashlib.sha256(encoded).hexdigest()
    relative = PurePosixPath("objects", "sha256", digest[:2], f"{digest}.json")
    target = root.joinpath(*relative.parts)
    _ensure_directory(target.parent)
    if target.exists():
        observed = _read_exact_regular_file(target)
        if observed != encoded:
            _fail("CAPITAL_BOOTSTRAP_CONTENT_ADDRESS_COLLISION")
    else:
        temporary = target.parent / f".{uuid.uuid4().hex}.part"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_BINARY", 0)),
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError:
                observed = _read_exact_regular_file(target)
                if observed != encoded:
                    _fail("CAPITAL_BOOTSTRAP_CONTENT_ADDRESS_COLLISION")
            authority_spool._fsync_directory(target.parent)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
    return BootstrapArtifactDescriptor(
        format=_ARTIFACT_FORMAT,
        relative_path=relative.as_posix(),
        sha256=digest,
        size_bytes=len(encoded),
    )


def _read_descriptor(
    root: Path, value: BootstrapArtifactDescriptor | Mapping[str, Any]
) -> dict[str, Any]:
    raw = value.to_dict() if type(value) is BootstrapArtifactDescriptor else value
    descriptor = _object(raw, "CAPITAL_BOOTSTRAP_DESCRIPTOR_INVALID")
    if (
        set(descriptor) != _ARTIFACT_DESCRIPTOR_KEYS
        or descriptor["format"] != _ARTIFACT_FORMAT
        or type(descriptor["relative_path"]) is not str
        or type(descriptor["size_bytes"]) is not int
        or not 0 < descriptor["size_bytes"] <= MAX_BOOTSTRAP_JSON_BYTES
    ):
        _fail("CAPITAL_BOOTSTRAP_DESCRIPTOR_INVALID")
    _sha(descriptor["sha256"], "CAPITAL_BOOTSTRAP_DESCRIPTOR_INVALID")
    relative = PurePosixPath(descriptor["relative_path"])
    expected_relative = PurePosixPath(
        "objects",
        "sha256",
        descriptor["sha256"][:2],
        f"{descriptor['sha256']}.json",
    )
    if (
        relative.is_absolute()
        or relative.as_posix() != descriptor["relative_path"]
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative != expected_relative
    ):
        _fail("CAPITAL_BOOTSTRAP_DESCRIPTOR_INVALID")
    target = root.joinpath(*relative.parts)
    encoded = _read_exact_regular_file(target)
    if (
        len(encoded) != descriptor["size_bytes"]
        or hashlib.sha256(encoded).hexdigest() != descriptor["sha256"]
    ):
        _fail("CAPITAL_BOOTSTRAP_ARTIFACT_HASH_MISMATCH")
    try:
        decoded = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_ARTIFACT_JSON_INVALID"
        ) from exc
    if type(decoded) is not dict or _canonical_bytes(decoded) != encoded:
        _fail("CAPITAL_BOOTSTRAP_ARTIFACT_JSON_INVALID")
    return decoded


def verify_capital_forward_phase_binding(
    generation_context: authority_spool.GenerationSpoolContext,
    value: CapitalForwardPhaseBinding,
) -> dict[str, Any]:
    """Reopen the signed Capital precommit behind one Forward phase binding."""

    if (
        type(generation_context) is not authority_spool.GenerationSpoolContext
        or type(value) is not CapitalForwardPhaseBinding
        or value._seal is not _CAPITAL_FORWARD_PHASE_BINDING_SEAL
        or type(value._body_json) is not bytes
        or type(value.receipt_sha256) is not str
        or hashlib.sha256(value._body_json).hexdigest() != value.receipt_sha256
    ):
        _fail("CAPITAL_FORWARD_PHASE_BINDING_NOT_ISSUED")
    try:
        authority_spool._assert_context_current(generation_context)
    except authority_spool.AuthoritySpoolError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_FORWARD_PHASE_CONTEXT_INVALID"
        ) from exc
    try:
        body = json.loads(value._body_json.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_FORWARD_PHASE_BINDING_INVALID"
        ) from exc
    if (
        type(body) is not dict
        or set(body) != _CAPITAL_FORWARD_PHASE_BINDING_KEYS
        or _canonical_bytes(body) != value._body_json
        or body.get("schema_version") != CAPITAL_FORWARD_PHASE_BINDING_SCHEMA
        or body.get("generation_id") != generation_context.generation_id
        or body.get("generation_context_sha256") != generation_context.context_sha256
        or body.get("config_sha256") != generation_context.config_sha256
        or body.get("native_universe_sha256")
        != generation_context.native_universe_sha256
        or body.get("created_before_process_evidence_access") is not True
        or body.get("confers_numeric_capital_publication_or_trading_authority")
        is not False
    ):
        _fail("CAPITAL_FORWARD_PHASE_BINDING_INVALID")
    _sha(
        body.get("bootstrap_identity_sha256"),
        "CAPITAL_FORWARD_PHASE_BOOTSTRAP_IDENTITY_INVALID",
    )
    _sha(
        body.get("capital_precommitment_body_sha256"),
        "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_INVALID",
    )
    _sha(
        body.get("capital_precommitment_envelope_sha256"),
        "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_INVALID",
    )
    _sha(
        body.get("margin_terms_sha256"),
        "CAPITAL_FORWARD_PHASE_MARGIN_TERMS_INVALID",
    )
    _sha(
        body.get("broker_terms_sha256"),
        "CAPITAL_FORWARD_PHASE_BROKER_TERMS_INVALID",
    )
    identity = _object(
        body.get("bootstrap_identity"),
        "CAPITAL_FORWARD_PHASE_BOOTSTRAP_IDENTITY_INVALID",
    )
    if (
        _sha_json(identity) != body["bootstrap_identity_sha256"]
        or identity.get("generation_id") != generation_context.generation_id
        or identity.get("generation_context_sha256")
        != generation_context.context_sha256
        or identity.get("config_sha256") != generation_context.config_sha256
        or identity.get("generation_native_universe_sha256")
        != generation_context.native_universe_sha256
        or identity.get("authorization_scope") != body.get("authorization_scope")
        or identity.get("margin_terms_sha256") != body["margin_terms_sha256"]
        or identity.get("broker_terms_sha256") != body["broker_terms_sha256"]
    ):
        _fail("CAPITAL_FORWARD_PHASE_BOOTSTRAP_IDENTITY_INVALID")

    raw_root = body.get("bootstrap_generation_root")
    if type(raw_root) is not str or not raw_root:
        _fail("CAPITAL_FORWARD_PHASE_STORAGE_INVALID")
    root = _absolute_root(Path(raw_root), "CAPITAL_FORWARD_PHASE_STORAGE_INVALID")
    if str(root) != raw_root or not root.is_dir():
        _fail("CAPITAL_FORWARD_PHASE_STORAGE_INVALID")
    bootstrap_descriptor = _object(
        body.get("bootstrap_receipt_descriptor"),
        "CAPITAL_FORWARD_PHASE_BOOTSTRAP_DESCRIPTOR_INVALID",
    )
    precommit_descriptor = _object(
        body.get("capital_precommitment_descriptor"),
        "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_DESCRIPTOR_INVALID",
    )
    bootstrap_receipt = _read_descriptor(root, bootstrap_descriptor)
    precommit_envelope = _read_descriptor(root, precommit_descriptor)
    trusted = bootstrap_receipt.get("trusted_role_keys")
    precommit_key = trusted.get("precommitment") if type(trusted) is dict else None
    if (
        bootstrap_receipt.get("schema_version") != CAPITAL_BOOTSTRAP_SCHEMA
        or bootstrap_receipt.get("identity") != identity
        or bootstrap_receipt.get("identity_sha256") != body["bootstrap_identity_sha256"]
        or bootstrap_receipt.get("precommitment_artifact") != precommit_descriptor
        or bootstrap_receipt.get("created_before_process_evidence_access") is not True
        or bootstrap_receipt.get("confers_numeric_capital_authority") is not False
        or bootstrap_receipt.get("confers_funded_live_authority") is not False
        or type(precommit_key) is not dict
        or set(precommit_key) != {"key_id", "public_key_base64"}
    ):
        _fail("CAPITAL_FORWARD_PHASE_BOOTSTRAP_RECEIPT_INVALID")
    try:
        public_key = base64.b64decode(precommit_key["public_key_base64"], validate=True)
    except (TypeError, ValueError) as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_KEY_INVALID"
        ) from exc
    if len(public_key) != 32:
        _fail("CAPITAL_FORWARD_PHASE_PRECOMMITMENT_KEY_INVALID")
    try:
        verified, meta = cap._verified_envelope(
            precommit_envelope,
            kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys={str(precommit_key["key_id"]): public_key},
            missing_reason="CAPITAL_FORWARD_PHASE_PRECOMMITMENT_MISSING",
        )
    except Exception as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_INVALID"
        ) from exc
    exact_precommit = _object(
        body.get("capital_precommitment"),
        "CAPITAL_FORWARD_PHASE_PRECOMMITMENT_INVALID",
    )
    if (
        verified != exact_precommit
        or meta.get("body_sha256") != body["capital_precommitment_body_sha256"]
        or meta.get("envelope_sha256") != body["capital_precommitment_envelope_sha256"]
        or bootstrap_receipt.get("precommitment_body_sha256") != meta.get("body_sha256")
        or bootstrap_receipt.get("precommitment_envelope_sha256")
        != meta.get("envelope_sha256")
        or exact_precommit.get("precommitment_id") != body["bootstrap_identity_sha256"]
        or exact_precommit.get("anchor_plan") != body.get("anchor_plan")
        or exact_precommit.get("current_selection_window")
        != body.get("current_selection_window")
        or exact_precommit.get("contract_lineage") != body.get("contract_lineage")
    ):
        _fail("CAPITAL_FORWARD_PHASE_PRECOMMITMENT_INVALID")
    if _sha_json(body["anchor_plan"]) != identity.get("capital_anchor_plan_sha256"):
        _fail("CAPITAL_FORWARD_PHASE_ANCHOR_PLAN_INVALID")
    if _sha_json(body["current_selection_window"]) != identity.get(
        "current_selection_window_sha256"
    ) or _sha_json(body["contract_lineage"]) != identity.get("contract_lineage_sha256"):
        _fail("CAPITAL_FORWARD_PHASE_PRECOMMITMENT_PROJECTION_INVALID")
    return _snapshot(body)


def verify_capital_forward_phase_binding_body(
    generation_context: authority_spool.GenerationSpoolContext,
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Rehydrate only a projection whose referenced Ed25519 envelope verifies.

    This is the post-disposal path used by Forward's standalone verifier.  The
    temporary seal below grants nothing: the main verifier reopens both
    content-addressed bootstrap artifacts and verifies the independently
    signed Capital precommitment before returning.
    """

    body = _object(value, "CAPITAL_FORWARD_PHASE_BINDING_INVALID")
    raw = _canonical_bytes(body)
    candidate = CapitalForwardPhaseBinding(
        _body_json=raw,
        receipt_sha256=hashlib.sha256(raw).hexdigest(),
        _seal=_CAPITAL_FORWARD_PHASE_BINDING_SEAL,
    )
    return verify_capital_forward_phase_binding(generation_context, candidate)


def _loaded_demo_key() -> authority_factory._LoadedSigningKey:
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return authority_factory._LoadedSigningKey(
        key_id=authority_factory.canonical_ed25519_key_id(public),
        public_key_bytes=public,
        private_key=private,
    )


def _signed_envelope(
    key: authority_factory._LoadedSigningKey, kind: str, body: Mapping[str, Any]
) -> dict[str, Any]:
    exact = _object(body, "CAPITAL_BOOTSTRAP_SIGNATURE_BODY_INVALID")
    signature = key.private_key.sign(
        cap.capital_authority_signature_payload(kind, key.key_id, exact)
    )
    return {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": key.key_id,
        "body": exact,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }


def _signed_request_issued_envelope(
    key: authority_factory._LoadedSigningKey, body: Mapping[str, Any]
) -> dict[str, Any]:
    exact = _object(body, "FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID")
    signature = key.private_key.sign(
        _request_issued_signature_payload(key.key_id, exact)
    )
    return {
        "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
        "key_id": key.key_id,
        "body": exact,
        "signature_base64": base64.b64encode(signature).decode("ascii"),
    }


def _bounded_exchange_files(root: Path) -> list[Path]:
    if len(str(root)) > MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS:
        _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_LENGTH_EXCEEDED")
    files: list[Path] = []
    total_bytes = 0
    directory_count = 1
    stack = [(root, 0)]
    while stack:
        directory, depth = stack.pop()
        if depth > MAX_AUTHORIZATION_EXCHANGE_DEPTH:
            _fail("FUNDED_AUTHORIZATION_EXCHANGE_DEPTH_EXCEEDED")
        if authority_factory._is_reparse(directory) or not directory.is_dir():
            _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_INVALID")
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    item = directory / entry.name
                    if len(str(item)) > MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS:
                        _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_LENGTH_EXCEEDED")
                    info = os.lstat(item)
                    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
                    if stat.S_ISLNK(info.st_mode) or attributes & 0x400:
                        _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_INVALID")
                    if stat.S_ISDIR(info.st_mode):
                        child_depth = depth + 1
                        if child_depth > MAX_AUTHORIZATION_EXCHANGE_DEPTH:
                            _fail("FUNDED_AUTHORIZATION_EXCHANGE_DEPTH_EXCEEDED")
                        directory_count += 1
                        if directory_count > MAX_AUTHORIZATION_EXCHANGE_DIRECTORIES:
                            _fail(
                                "FUNDED_AUTHORIZATION_EXCHANGE_DIRECTORY_BOUND_EXCEEDED"
                            )
                        stack.append((item, child_depth))
                    elif stat.S_ISREG(info.st_mode):
                        if info.st_nlink != 1:
                            _fail("FUNDED_AUTHORIZATION_EXCHANGE_HARDLINK_FORBIDDEN")
                        files.append(item)
                        total_bytes += info.st_size
                    else:
                        _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_INVALID")
                    if len(files) > MAX_AUTHORIZATION_EXCHANGE_ARTIFACTS:
                        _fail("FUNDED_AUTHORIZATION_EXCHANGE_ARTIFACT_BOUND_EXCEEDED")
                    if total_bytes > MAX_AUTHORIZATION_EXCHANGE_BYTES:
                        _fail("FUNDED_AUTHORIZATION_EXCHANGE_BYTE_BOUND_EXCEEDED")
        except OSError as exc:
            raise ProductionCapitalBootstrapError(
                "FUNDED_AUTHORIZATION_EXCHANGE_PATH_INVALID"
            ) from exc
    return files


class _ExternalAuthorizationTransport:
    __slots__ = (
        "_bootstrap_identity_sha256",
        "_exchange_root",
        "_generation_context_sha256",
        "_generation_id",
        "_issuance_key",
        "_key_id",
        "_lock",
        "_public_key",
        "_used",
        "_validator",
    )

    def __init__(
        self,
        *,
        bootstrap_identity_sha256: str,
        exchange_root: Path,
        generation_context_sha256: str,
        generation_id: str,
        issuance_key: authority_factory._LoadedSigningKey,
        key_id: str,
        public_key: bytes,
        validator: Callable[[Mapping[str, Any]], None],
    ) -> None:
        self._bootstrap_identity_sha256 = bootstrap_identity_sha256
        self._exchange_root = exchange_root
        self._generation_context_sha256 = generation_context_sha256
        self._generation_id = generation_id
        self._issuance_key = issuance_key
        self._key_id = key_id
        self._public_key = bytes(public_key)
        self._validator = validator
        self._lock = threading.Lock()
        self._used = False

    def __repr__(self) -> str:
        return "<two-stage-external-funded-authorization-transport>"

    def _request(self, exact: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": FUNDED_AUTHORIZATION_REQUEST_SCHEMA,
            "generation_id": self._generation_id,
            "generation_context_sha256": self._generation_context_sha256,
            "bootstrap_identity_sha256": self._bootstrap_identity_sha256,
            "authorization_key_id": self._key_id,
            "authorization_body": _snapshot(exact),
            "authorization_body_sha256": _sha_json(exact),
            "response_signature_kind": FUNDED_AUTHORIZATION_RESPONSE_SIGNATURE_KIND,
        }

    def _response_path(self, request_sha256: str) -> Path:
        path = self._exchange_root / "incoming" / f"{request_sha256}.json"
        _ensure_directory(path.parent)
        return path

    def _issued_envelope(
        self,
        request_descriptor: BootstrapArtifactDescriptor,
        exact: Mapping[str, Any],
    ) -> dict[str, Any]:
        body = {
            "schema_version": FUNDED_AUTHORIZATION_REQUEST_ISSUED_SCHEMA,
            "request_sha256": request_descriptor.sha256,
            "generation_id": self._generation_id,
            "generation_context_sha256": self._generation_context_sha256,
            "bootstrap_identity_sha256": self._bootstrap_identity_sha256,
            "authorization_body_sha256": _sha_json(exact),
            "issuance_key_id": self._issuance_key.key_id,
        }
        if set(body) != _FUNDED_AUTHORIZATION_REQUEST_ISSUED_KEYS:
            _fail("FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID")
        return _signed_request_issued_envelope(self._issuance_key, body)

    def _verify_issued(
        self,
        envelope: Mapping[str, Any],
        request_descriptor: BootstrapArtifactDescriptor,
        exact: Mapping[str, Any],
    ) -> None:
        signed = _object(envelope, "FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID")
        if (
            set(signed) != _SIGNED_ENVELOPE_KEYS
            or signed.get("schema_version") != cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION
            or signed.get("key_id") != self._issuance_key.key_id
        ):
            _fail("FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID")
        body = _object(
            signed.get("body"), "FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID"
        )
        if (
            set(body) != _FUNDED_AUTHORIZATION_REQUEST_ISSUED_KEYS
            or body.get("schema_version") != FUNDED_AUTHORIZATION_REQUEST_ISSUED_SCHEMA
            or body.get("request_sha256") != request_descriptor.sha256
            or body.get("generation_id") != self._generation_id
            or body.get("generation_context_sha256") != self._generation_context_sha256
            or body.get("bootstrap_identity_sha256") != self._bootstrap_identity_sha256
            or body.get("authorization_body_sha256") != _sha_json(exact)
            or body.get("issuance_key_id") != self._issuance_key.key_id
        ):
            _fail("FUNDED_AUTHORIZATION_REQUEST_ISSUED_BINDING_MISMATCH")
        try:
            signature = base64.b64decode(signed.get("signature_base64"), validate=True)
            if len(signature) != 64:
                _fail("FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID")
            Ed25519PublicKey.from_public_bytes(
                self._issuance_key.public_key_bytes
            ).verify(
                signature,
                _request_issued_signature_payload(self._issuance_key.key_id, body),
            )
        except Exception as exc:
            raise ProductionCapitalBootstrapError(
                "FUNDED_AUTHORIZATION_REQUEST_ISSUED_INVALID"
            ) from exc

    def _pending(
        self,
        request_descriptor: BootstrapArtifactDescriptor,
        response_path: Path,
    ) -> None:
        relative_response = response_path.relative_to(self._exchange_root).as_posix()
        raise coordinator.ProductionAuthorityBlocked(
            "PENDING_EXTERNAL_AUTHORIZATION",
            (
                f"request_sha256={request_descriptor.sha256}",
                f"request_path={request_descriptor.relative_path}",
                f"response_path={relative_response}",
            ),
        )

    def __call__(self, body: Mapping[str, Any]) -> Mapping[str, Any]:
        exact = _object(body, "FUNDED_AUTHORIZATION_BODY_INVALID")
        self._validator(exact)
        with self._lock:
            if self._used:
                _fail("FUNDED_AUTHORIZATION_TRANSPORT_REUSED")
            self._used = True
        request = self._request(exact)
        if set(request) != _FUNDED_AUTHORIZATION_REQUEST_KEYS:
            _fail("FUNDED_AUTHORIZATION_REQUEST_INVALID")
        request_descriptor = _content_descriptor(self._exchange_root, request)
        if _read_descriptor(self._exchange_root, request_descriptor) != request:
            _fail("FUNDED_AUTHORIZATION_REQUEST_FREEZE_FAILED")
        issued_envelope = self._issued_envelope(request_descriptor, exact)
        issued_root = self._exchange_root / "issued"
        issued_digest = _sha_json(issued_envelope)
        issued_target = (
            issued_root
            / "objects"
            / "sha256"
            / issued_digest[:2]
            / f"{issued_digest}.json"
        )
        issued_existed = issued_target.exists() or issued_target.is_symlink()
        issued_descriptor = _content_descriptor(issued_root, issued_envelope)
        frozen_issued = _read_descriptor(issued_root, issued_descriptor)
        self._verify_issued(frozen_issued, request_descriptor, exact)
        response_path = self._response_path(request_descriptor.sha256)
        _bounded_exchange_files(self._exchange_root)
        if not issued_existed:
            self._pending(request_descriptor, response_path)
        if not response_path.exists():
            self._pending(request_descriptor, response_path)
        try:
            encoded = _read_exact_regular_file(response_path)
            decoded = json.loads(encoded.decode("utf-8"))
        except (
            OSError,
            ProductionCapitalBootstrapError,
            UnicodeError,
            json.JSONDecodeError,
        ) as exc:
            raise ProductionCapitalBootstrapError(
                "FUNDED_AUTHORIZATION_RESPONSE_MISSING_OR_INVALID"
            ) from exc
        if type(decoded) is not dict or _canonical_bytes(decoded) != encoded:
            _fail("FUNDED_AUTHORIZATION_RESPONSE_MISSING_OR_INVALID")
        response_envelope = _object(decoded, "FUNDED_AUTHORIZATION_RESPONSE_INVALID")
        if (
            set(response_envelope) != _SIGNED_ENVELOPE_KEYS
            or response_envelope.get("schema_version")
            != cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION
            or response_envelope.get("key_id") != self._key_id
        ):
            _fail("FUNDED_AUTHORIZATION_KEY_SUBSTITUTION")
        try:
            response_body = _object(
                response_envelope.get("body"),
                "FUNDED_AUTHORIZATION_RESPONSE_INVALID",
            )
            signature = base64.b64decode(
                response_envelope.get("signature_base64"), validate=True
            )
            if len(signature) != 64:
                _fail("FUNDED_AUTHORIZATION_RESPONSE_INVALID")
            Ed25519PublicKey.from_public_bytes(self._public_key).verify(
                signature,
                funded_authorization_response_signature_payload(
                    self._key_id, response_body
                ),
            )
        except Exception as exc:
            raise ProductionCapitalBootstrapError(
                "FUNDED_AUTHORIZATION_RESPONSE_INVALID"
            ) from exc
        if (
            set(response_body) != _FUNDED_AUTHORIZATION_RESPONSE_KEYS
            or response_body.get("schema_version")
            != FUNDED_AUTHORIZATION_RESPONSE_SCHEMA
            or response_body.get("request_sha256") != request_descriptor.sha256
            or response_body.get("generation_id") != self._generation_id
            or response_body.get("generation_context_sha256")
            != self._generation_context_sha256
            or response_body.get("bootstrap_identity_sha256")
            != self._bootstrap_identity_sha256
            or response_body.get("authorization_body_sha256") != _sha_json(exact)
        ):
            _fail("FUNDED_AUTHORIZATION_RESPONSE_BINDING_MISMATCH")
        authorization_envelope = _object(
            response_body.get("authorization_envelope"),
            "FUNDED_AUTHORIZATION_RECEIPT_INVALID",
        )
        if authorization_envelope.get("key_id") != self._key_id:
            _fail("FUNDED_AUTHORIZATION_KEY_SUBSTITUTION")
        try:
            verified, _ = cap._verified_envelope(
                authorization_envelope,
                kind=cap.AUTHORIZATION_SIGNATURE_KIND,
                trusted_public_keys={self._key_id: self._public_key},
                missing_reason="FUNDED_AUTHORIZATION_MISSING",
            )
        except Exception as exc:
            raise ProductionCapitalBootstrapError(
                "FUNDED_AUTHORIZATION_RECEIPT_INVALID"
            ) from exc
        if verified != exact:
            _fail("FUNDED_AUTHORIZATION_BODY_SUBSTITUTION")
        accepted_root = self._exchange_root / "accepted"
        _ensure_directory(accepted_root)
        accepted_descriptor = _content_descriptor(accepted_root, response_envelope)
        accepted = _read_descriptor(accepted_root, accepted_descriptor)
        if accepted != response_envelope:
            _fail("FUNDED_AUTHORIZATION_RESPONSE_FREEZE_FAILED")
        accepted_relative = PurePosixPath(accepted_descriptor.relative_path)
        accepted_path = accepted_root.joinpath(*accepted_relative.parts)
        ingress_info = response_path.lstat()
        accepted_info = accepted_path.lstat()
        if (
            ingress_info.st_nlink != 1
            or accepted_info.st_nlink != 1
            or (ingress_info.st_dev, ingress_info.st_ino)
            == (accepted_info.st_dev, accepted_info.st_ino)
        ):
            _fail("FUNDED_AUTHORIZATION_RESPONSE_INODE_NOT_ISOLATED")
        _bounded_exchange_files(self._exchange_root)
        return authorization_envelope


class _ScopeBoundAuthorizationSigner:
    __slots__ = ("_binding", "_delegate")

    def __init__(
        self,
        delegate: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        binding: CapitalAuthorizationScopeBinding,
    ) -> None:
        self._delegate = delegate
        self._binding = binding

    @property
    def scope_binding(self) -> CapitalAuthorizationScopeBinding:
        return self._binding

    def require_authorization_scope(self, authorization_scope: str) -> None:
        self._binding.require(authorization_scope)

    def __repr__(self) -> str:
        return (
            "<scope-bound-capital-authorization-signer "
            f"scope={self._binding.authorization_scope} "
            f"delegate={self._delegate!r}>"
        )

    def __call__(self, body: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._delegate(body)


class _CoordinatorDerivedReplayLoader:
    """Compute Capital outputs only from the coordinator-frozen candidates.

    The pre-search bootstrap cannot possess phase-local ``CandidateTrade``
    objects.  They first exist after historical replay and current refit, so
    this loader is deliberately constructed inside the lazy capital factory.
    A caller-supplied function is never on this authority path.
    """

    __slots__ = (
        "_base_policy",
        "_candidates_by_phase",
        "_config_payload",
        "_current_state",
        "_current_verified",
        "_historical_state",
        "_lock",
        "_rosters",
    )

    _HISTORICAL_STATUSES = ("HISTORICAL_OOS_REPLAY",)
    _CURRENT_STATUSES = ("PROCESS_VALIDATED_CURRENT_REFIT",)

    def __init__(
        self,
        *,
        config_payload: Mapping[str, Any],
        historical_state: coordinator._HistoricalState,
        current_state: coordinator._CurrentState,
        candidates_by_phase: Mapping[str, Sequence[replay.CandidateTrade]],
        rosters: Mapping[str, Mapping[str, Any]],
    ) -> None:
        phase_candidates: dict[str, tuple[replay.CandidateTrade, ...]] = {}
        for phase_id, values in candidates_by_phase.items():
            candidates = tuple(values)
            if not candidates or any(
                type(value) is not replay.CandidateTrade for value in candidates
            ):
                _fail("CAPITAL_BOOTSTRAP_REPLAY_CANDIDATE_ROSTER_INVALID")
            normalized = tuple(value.normalized() for value in candidates)
            phase_candidates[str(phase_id)] = normalized
        if set(phase_candidates) != set(rosters):
            _fail("CAPITAL_BOOTSTRAP_REPLAY_PHASE_ROSTER_MISMATCH")
        for phase_id, candidates in phase_candidates.items():
            try:
                derived = replay.portfolio_replay_candidate_roster_receipt(candidates)
            except Exception as exc:
                raise ProductionCapitalBootstrapError(
                    "CAPITAL_BOOTSTRAP_REPLAY_ROSTER_DERIVATION_FAILED"
                ) from exc
            if derived != dict(rosters[phase_id]):
                _fail("CAPITAL_BOOTSTRAP_REPLAY_ROSTER_STATE_MISMATCH")

        self._base_policy: replay.PortfolioPolicy | None = None
        self._candidates_by_phase = phase_candidates
        self._config_payload = _snapshot(config_payload)
        self._current_state = current_state
        self._current_verified = False
        self._historical_state = historical_state
        self._lock = threading.Lock()
        self._rosters = {key: dict(value) for key, value in rosters.items()}

    def __repr__(self) -> str:
        return "<coordinator-derived-synchronized-capital-replay-loader>"

    def _phase_policy(self, phase_id: str) -> replay.PortfolioPolicy:
        if self._base_policy is None:
            pipeline = self._config_payload.get("pipeline")
            if type(pipeline) is not dict:
                _fail("CAPITAL_BOOTSTRAP_REPLAY_POLICY_CONFIG_INVALID")
            initial_capital = pipeline.get("initial_capital")
            if (
                type(initial_capital) not in {int, float}
                or type(initial_capital) is bool
                or not math.isfinite(float(initial_capital))
                or float(initial_capital) <= 0.0
            ):
                _fail("CAPITAL_BOOTSTRAP_REPLAY_INITIAL_CAPITAL_INVALID")
            try:
                base_policy = replay.portfolio_policy_from_config_payload(
                    self._config_payload, float(initial_capital)
                )
            except Exception as exc:
                raise ProductionCapitalBootstrapError(
                    "CAPITAL_BOOTSTRAP_REPLAY_POLICY_DERIVATION_FAILED"
                ) from exc
            matching_policies = [
                value
                for balance, value in self._current_state.policies.items()
                if type(balance) in {int, float}
                and type(balance) is not bool
                and float(balance) == float(initial_capital)
            ]
            if len(matching_policies) != 1 or _sha_json(
                asdict(base_policy)
            ) != _sha_json(matching_policies[0]):
                _fail("CAPITAL_BOOTSTRAP_REPLAY_POLICY_STATE_MISMATCH")
            if base_policy.allowed_promotion_statuses != self._CURRENT_STATUSES:
                _fail("CAPITAL_BOOTSTRAP_CURRENT_REPLAY_STATUS_POLICY_INVALID")
            self._base_policy = base_policy
        return replace(
            self._base_policy,
            allowed_promotion_statuses=(
                self._CURRENT_STATUSES
                if phase_id == "CURRENT|CURRENT_SELECTION"
                else self._HISTORICAL_STATUSES
            ),
        )

    def _reverify_current_authority(self) -> None:
        with self._lock:
            if self._current_verified:
                return
            current = self._current_state
            historical = self._historical_state
            product = current.product
            try:
                current_book, _ = coordinator._owned_book_decision(
                    product.current_book_decision,
                    code_prefix="CURRENT",
                )
                accepted_payload = product.accepted_book_generation_receipt.get(
                    "payload"
                )
                current_cutoff = (
                    accepted_payload.get("current_cutoff_utc")
                    if type(accepted_payload) is dict
                    else None
                )
                symbols = current.roster.get("symbols")
                if type(symbols) is not list or not symbols:
                    _fail("CAPITAL_BOOTSTRAP_CURRENT_REPLAY_SYMBOL_ROSTER_INVALID")
                derived_roster, errors = replay.build_current_refit_replay_roster(
                    product.staged_ledger,
                    symbols,
                    current_book_decision=current_book,
                    book_evidence_family=None,
                    historical_process_decision=historical.process_decision,
                    causal_precommit_payload=historical.precommit_payload,
                    causal_evidence_payload=historical.evidence_payload,
                    causal_reconciliation_payload=historical.reconciliation_payload,
                    accepted_book_generation_receipt=(
                        product.accepted_book_generation_receipt
                    ),
                    current_refit_receipt=product.current_refit_receipt,
                    as_of_utc=current_cutoff,
                    current_refit_raw_manifest=product.raw_manifest_path,
                    current_refit_artifact_root=product.artifact_root,
                )
                if (
                    errors
                    or derived_roster.get("authoritative") is not True
                    or dict(derived_roster) != current.roster
                ):
                    _fail("CAPITAL_BOOTSTRAP_CURRENT_REPLAY_AUTHORITY_MISMATCH")
                candidate_errors = replay.validate_current_refit_replay_candidates(
                    self._candidates_by_phase["CURRENT|CURRENT_SELECTION"],
                    derived_roster,
                    replayed_cell_ids=product.replayed_cell_ids,
                )
                if candidate_errors:
                    _fail("CAPITAL_BOOTSTRAP_CURRENT_REPLAY_CANDIDATES_INVALID")
            except ProductionCapitalBootstrapError:
                raise
            except Exception as exc:
                raise ProductionCapitalBootstrapError(
                    "CAPITAL_BOOTSTRAP_CURRENT_REPLAY_REVERIFY_FAILED"
                ) from exc
            self._current_verified = True

    def __call__(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        supplied = _object(request, "CAPITAL_BOOTSTRAP_REPLAY_REQUEST_INVALID")
        phase_id = f"{supplied.get('anchor_id')}|{supplied.get('phase')}"
        candidates = self._candidates_by_phase.get(phase_id)
        frozen = self._rosters.get(phase_id)
        if candidates is None or frozen is None:
            _fail("CAPITAL_BOOTSTRAP_REPLAY_PHASE_NOT_COORDINATOR_DERIVED")
        if supplied.get("frozen_candidate_roster") != frozen:
            _fail("CAPITAL_BOOTSTRAP_REPLAY_REQUEST_ROSTER_SUBSTITUTION")
        expected_statuses = (
            self._CURRENT_STATUSES
            if phase_id == "CURRENT|CURRENT_SELECTION"
            else self._HISTORICAL_STATUSES
        )
        if {value.promotion_status for value in candidates} != set(expected_statuses):
            _fail("CAPITAL_BOOTSTRAP_REPLAY_CANDIDATE_STATUS_INVALID")
        try:
            if replay.portfolio_replay_candidate_roster_receipt(candidates) != frozen:
                _fail("CAPITAL_BOOTSTRAP_REPLAY_CANDIDATES_CHANGED")
            policy = cap_replay.capital_policy_for_point(
                self._phase_policy(phase_id),
                supplied.get("policy"),
            )
            simulator = replay.SynchronizedPortfolioSimulator(policy)
            if phase_id == "CURRENT|CURRENT_SELECTION":
                # Raw current-refit authority is reopened once per Capital run.
                # Policy-point replays then share those immutable verified
                # candidates instead of rereading the same bounded spool for
                # every precommitted policy point.
                self._reverify_current_authority()
                result = simulator._run(candidates, allow_current_refit=True)
                expected_mode = "CURRENT_REFIT_REVERIFIED"
            else:
                result = simulator.run(candidates)
                expected_mode = "STANDARD_SYNCHRONIZED_REPLAY"
        except ProductionCapitalBootstrapError:
            raise
        except Exception as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_SYNCHRONIZED_REPLAY_FAILED"
            ) from exc

        if type(result) is not dict or set(result) != cap_replay._RESULT_KEYS:
            _fail("CAPITAL_BOOTSTRAP_SYNCHRONIZED_REPLAY_OUTPUT_INVALID")
        receipt = result.get("input_receipt")
        if (
            type(receipt) is not dict
            or any(receipt.get(key) != value for key, value in frozen.items())
            or receipt.get("execution_authority_mode") != expected_mode
            or _sha_json(result.get("policy")) != _sha_json(asdict(policy))
            or result.get("summary", {}).get("candidate_roster_sha256")
            != frozen["candidate_roster_sha256"]
        ):
            _fail("CAPITAL_BOOTSTRAP_SYNCHRONIZED_REPLAY_BINDING_INVALID")
        return result


class _BootstrapCapitalFactory:
    __slots__ = (
        "_authorization_key",
        "_authorization_exchange_root",
        "_authorization_scope",
        "_bootstrap_descriptor",
        "_bootstrap_root",
        "_broker_terms_sha256",
        "_config_payload",
        "_identity",
        "_margin_terms_sha256",
        "_output_root",
        "_policy",
        "_precommitment_descriptor",
        "_precommitment_public_key",
        "_process_precommitment_payload",
        "_replay_key",
        "_shard_attempt_limit",
    )

    def __init__(
        self,
        *,
        authorization_key: authority_factory._LoadedSigningKey | tuple[str, bytes],
        authorization_exchange_root: Path | None,
        authorization_scope: str,
        bootstrap_descriptor: BootstrapArtifactDescriptor,
        bootstrap_root: Path,
        broker_terms_sha256: str,
        config_payload: Mapping[str, Any],
        identity: Mapping[str, Any],
        margin_terms_sha256: str,
        output_root: Path,
        policy: authority_factory.CapitalAuthorityPolicy,
        precommitment_descriptor: BootstrapArtifactDescriptor,
        precommitment_public_key: tuple[str, bytes],
        process_precommitment_payload: Mapping[str, Any],
        replay_key: authority_factory._LoadedSigningKey,
        shard_attempt_limit: int,
    ) -> None:
        self._authorization_key = authorization_key
        self._authorization_exchange_root = authorization_exchange_root
        self._authorization_scope = authorization_scope
        self._bootstrap_descriptor = bootstrap_descriptor
        self._bootstrap_root = bootstrap_root
        self._broker_terms_sha256 = broker_terms_sha256
        self._config_payload = _snapshot(config_payload)
        self._identity = _snapshot(identity)
        self._margin_terms_sha256 = margin_terms_sha256
        self._output_root = output_root
        self._policy = policy
        self._precommitment_descriptor = precommitment_descriptor
        self._precommitment_public_key = (
            precommitment_public_key[0],
            bytes(precommitment_public_key[1]),
        )
        self._process_precommitment_payload = _snapshot(process_precommitment_payload)
        self._replay_key = replay_key
        self._shard_attempt_limit = shard_attempt_limit

    def __repr__(self) -> str:
        return (
            "<production-capital-bootstrap-factory "
            f"scope={self._authorization_scope} frozen-before-search>"
        )

    @property
    def authorization_scope(self) -> str:
        return self._authorization_scope

    @property
    def scope_binding(self) -> CapitalAuthorizationScopeBinding:
        key_id, public_key = (
            (
                self._authorization_key.key_id,
                self._authorization_key.public_key_bytes,
            )
            if isinstance(self._authorization_key, authority_factory._LoadedSigningKey)
            else self._authorization_key
        )
        return CapitalAuthorizationScopeBinding(
            authorization_scope=self._authorization_scope,
            bootstrap_identity_sha256=_sha_json(self._identity),
            config_sha256=self._identity["config_sha256"],
            authority_key_id=key_id,
            authority_public_key_sha256=hashlib.sha256(public_key).hexdigest(),
        )

    def require_authorization_scope(self, authorization_scope: str) -> None:
        self.scope_binding.require(authorization_scope)

    def _verified_precommitment(self) -> tuple[dict[str, Any], dict[str, Any]]:
        bootstrap = _read_descriptor(self._bootstrap_root, self._bootstrap_descriptor)
        authorization_key_id, authorization_public = (
            (
                self._authorization_key.key_id,
                self._authorization_key.public_key_bytes,
            )
            if isinstance(self._authorization_key, authority_factory._LoadedSigningKey)
            else self._authorization_key
        )
        expected_role_keys = {
            "precommitment": {
                "key_id": self._precommitment_public_key[0],
                "public_key_base64": base64.b64encode(
                    self._precommitment_public_key[1]
                ).decode("ascii"),
            },
            "replay": {
                "key_id": self._replay_key.key_id,
                "public_key_base64": base64.b64encode(
                    self._replay_key.public_key_bytes
                ).decode("ascii"),
            },
            "authorization": {
                "key_id": authorization_key_id,
                "public_key_base64": base64.b64encode(authorization_public).decode(
                    "ascii"
                ),
            },
        }
        if (
            set(bootstrap) != _BOOTSTRAP_RECEIPT_KEYS
            or bootstrap.get("schema_version") != CAPITAL_BOOTSTRAP_SCHEMA
            or bootstrap.get("identity") != self._identity
            or bootstrap.get("identity_sha256") != _sha_json(self._identity)
            or bootstrap.get("precommitment_artifact")
            != self._precommitment_descriptor.to_dict()
            or bootstrap.get("trusted_role_keys") != expected_role_keys
            or bootstrap.get("authorization_scope") != self._authorization_scope
            or bootstrap.get("created_before_process_evidence_access") is not True
            or bootstrap.get("confers_numeric_capital_authority") is not False
            or bootstrap.get("confers_funded_live_authority") is not False
            or self._identity.get("config_payload_sha256")
            != _sha_json(self._config_payload)
        ):
            _fail("CAPITAL_BOOTSTRAP_RECEIPT_BINDING_INVALID")
        envelope = _read_descriptor(
            self._bootstrap_root, self._precommitment_descriptor
        )
        key_id, public_key = self._precommitment_public_key
        try:
            body, meta = cap._verified_envelope(
                envelope,
                kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
                trusted_public_keys={key_id: public_key},
                missing_reason="CAP_PRECOMMITMENT_MISSING",
            )
            policies, _, _ = cap._precommitment(body, self._policy.constraints)
        except Exception as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_PRECOMMITMENT_VERIFICATION_FAILED"
            ) from exc
        if (
            envelope.get("key_id") != key_id
            or body.get("precommitment_id") != _sha_json(self._identity)
            or list(policies.values()) != list(self._policy.candidate_policies)
            or meta["envelope_sha256"] != self._precommitment_descriptor.sha256
            or bootstrap.get("precommitment_body_sha256") != meta["body_sha256"]
            or bootstrap.get("precommitment_envelope_sha256") != meta["envelope_sha256"]
        ):
            _fail("CAPITAL_BOOTSTRAP_PRECOMMITMENT_BINDING_INVALID")
        return envelope, body

    def __call__(
        self, request: coordinator.CapitalAuthorityRequest
    ) -> coordinator.CapitalAuthorityInputs:
        if (
            type(request) is not coordinator.CapitalAuthorityRequest
            or type(request._historical_state) is not coordinator._HistoricalState
            or type(request._current_state) is not coordinator._CurrentState
        ):
            _fail("CAPITAL_BOOTSTRAP_AUTHORITY_REQUEST_INVALID")
        if request.authorization_scope != self._authorization_scope:
            _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_MISMATCH")
        precommitment, precommit_body = self._verified_precommitment()
        _, precommit_meta = cap._verified_envelope(
            precommitment,
            kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys={
                self._precommitment_public_key[0]: self._precommitment_public_key[1]
            },
            missing_reason="CAP_PRECOMMITMENT_MISSING",
        )
        generated_text, validation = _validation_time(request.validation_time_utc)
        process_not_before = datetime.fromisoformat(
            self._identity["process_evidence_access_not_before_utc"].replace(
                "Z", "+00:00"
            )
        )
        process_deadline = datetime.fromisoformat(
            self._identity["process_evidence_access_deadline_utc"].replace(
                "Z", "+00:00"
            )
        )
        if validation < process_not_before or validation > process_deadline:
            _fail("CAPITAL_BOOTSTRAP_VALIDATION_OUTSIDE_PROCESS_WINDOW")
        issued_text = generated_text
        expires_text = _utc_text(
            validation + timedelta(seconds=self._policy.authorization_max_ttl_seconds)
        )

        phase_specs = authority_factory._phase_specs(precommit_body)
        historical = request._historical_state
        current = request._current_state
        try:
            coordinator_process = _object(
                historical.precommit_payload,
                "CAPITAL_BOOTSTRAP_COORDINATOR_PROCESS_INVALID",
            )
            causal._validate_precommit_payload(coordinator_process)
        except causal.ProcessVerificationError as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_COORDINATOR_PROCESS_INVALID"
            ) from exc
        if (
            coordinator_process != self._process_precommitment_payload
            or _sha_json(coordinator_process)
            != self._identity["process_precommitment_sha256"]
        ):
            _fail("CAPITAL_BOOTSTRAP_COORDINATOR_PROCESS_MISMATCH")
        candidates_by_phase = {
            str(phase_id): tuple(values)
            for phase_id, values in historical.capital_candidates_by_phase.items()
        }
        current_phase = "CURRENT|CURRENT_SELECTION"
        if current_phase in candidates_by_phase:
            _fail("CAPITAL_CURRENT_CANDIDATES_CALLER_SUPPLIED")
        candidates_by_phase[current_phase] = tuple(current.candidates)
        try:
            rosters = authority_factory._derived_rosters(
                phase_specs, candidates_by_phase
            )
        except authority_factory.CapitalAuthorityFactoryError as exc:
            raise ProductionCapitalBootstrapError(exc.args[0]) from exc

        lineages_by_phase = {
            str(phase_id): _object(value, "CAPITAL_BOOTSTRAP_REPLAY_LINEAGE_INVALID")
            for phase_id, value in historical.capital_lineage_by_phase.items()
        }
        if current_phase in lineages_by_phase:
            _fail("CAPITAL_CURRENT_LINEAGE_CALLER_SUPPLIED")
        try:
            lineages_by_phase[current_phase] = (
                coordinator.derive_current_capital_lineage(
                    current,
                    precommit_body["contract_lineage"],
                    margin_terms_sha256=self._margin_terms_sha256,
                    broker_terms_sha256=self._broker_terms_sha256,
                )
            )
            lineages = authority_factory._verified_lineages(
                phase_specs,
                lineages_by_phase,
                precommit_body["contract_lineage"],
            )
        except (
            authority_factory.CapitalAuthorityFactoryError,
            coordinator.ProductionAuthorityBlocked,
        ) as exc:
            code = getattr(exc, "code", str(exc))
            raise ProductionCapitalBootstrapError(code) from exc

        expected_requests = authority_factory._expected_replay_requests(
            phase_specs, self._policy.candidate_policies, rosters
        )
        genuine_replay_loader = _CoordinatorDerivedReplayLoader(
            config_payload=self._config_payload,
            historical_state=historical,
            current_state=current,
            candidates_by_phase=candidates_by_phase,
            rosters=rosters,
        )
        replay_transport = authority_factory._VerifiedReplayOutputTransport(
            genuine_replay_loader, expected_requests
        )

        def validate_replay_body(body: Mapping[str, Any]) -> None:
            fields_present = set(body)
            if not _REPLAY_BODY_COMMON_FIELDS.issubset(
                fields_present
            ) or fields_present not in {
                cap._REPLAY_INLINE_KEYS,
                cap._REPLAY_SHARDED_KEYS,
            }:
                _fail("CAPITAL_BOOTSTRAP_REPLAY_SIGNING_BODY_SCHEMA_INVALID")
            if (
                body["schema_version"] != cap.CAP_REPLAY_SCHEMA_VERSION
                or body["precommitment_sha256"] != precommit_meta["body_sha256"]
                or body["precommitment_envelope_sha256"]
                != precommit_meta["envelope_sha256"]
                or body["generated_at_utc"] != generated_text
            ):
                _fail("CAPITAL_BOOTSTRAP_REPLAY_SIGNING_BODY_BINDING_MISMATCH")

        allowed_policies = {
            str(item["policy_id"]): item for item in self._policy.candidate_policies
        }
        authority_key_id = (
            self._authorization_key.key_id
            if isinstance(self._authorization_key, authority_factory._LoadedSigningKey)
            else self._authorization_key[0]
        )

        def validate_authorization_body(body: Mapping[str, Any]) -> None:
            if set(body) != _AUTHORIZATION_BODY_FIELDS:
                _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_BODY_SCHEMA_INVALID")
            selected_id = body.get("selected_policy_point_id")
            selected = body.get("selected_max_total_stop_risk_pct")
            policy = allowed_policies.get(selected_id)
            if (
                body.get("schema_version") != cap.CAP_AUTHORIZATION_SCHEMA_VERSION
                or body.get("authority_key_id") != authority_key_id
                or body.get("issued_at_utc") != issued_text
                or body.get("expires_at_utc") != expires_text
                or body.get("constraints_sha256")
                != cap.canonical_json_sha256(asdict(self._policy.constraints))
                or policy is None
                or type(selected) is bool
                or not isinstance(selected, (int, float))
                or not math.isfinite(float(selected))
                or float(selected) != float(policy["max_total_stop_risk_pct"])
            ):
                _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_BODY_BINDING_MISMATCH")

        replay_signer = authority_factory._ExactEd25519Signer(
            self._replay_key,
            cap.REPLAY_EVIDENCE_SIGNATURE_KIND,
            validate_replay_body,
        )
        if isinstance(self._authorization_key, authority_factory._LoadedSigningKey):
            authorization_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]] = (
                authority_factory._ExactEd25519Signer(
                    self._authorization_key,
                    cap.AUTHORIZATION_SIGNATURE_KIND,
                    validate_authorization_body,
                )
            )
            authority_public = self._authorization_key.public_key_bytes
        else:
            if self._authorization_exchange_root is None:
                _fail("FUNDED_AUTHORIZATION_EXCHANGE_ROOT_MISSING")
            funded_trust = _funded_authorization_trust(self._config_payload)
            expected_exchange_root = (
                Path(funded_trust["authorization_exchange_root"])
                / self._identity["generation_id"]
                / _sha_json(self._identity)
            )
            if (
                self._authorization_exchange_root != expected_exchange_root
                or _sha_json(funded_trust)
                != self._identity["funded_authorization_trust_sha256"]
            ):
                _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISMATCH")
            authority_public = self._authorization_key[1]
            if (
                authority_key_id != funded_trust["authorization_key_id"]
                or hashlib.sha256(authority_public).hexdigest()
                != funded_trust["authorization_public_key_sha256"]
            ):
                _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISMATCH")
            authorization_signer = _ExternalAuthorizationTransport(
                bootstrap_identity_sha256=_sha_json(self._identity),
                exchange_root=self._authorization_exchange_root,
                generation_context_sha256=self._identity["generation_context_sha256"],
                generation_id=self._identity["generation_id"],
                issuance_key=self._replay_key,
                key_id=authority_key_id,
                public_key=authority_public,
                validator=validate_authorization_body,
            )

        scope_binding = self.scope_binding
        if (
            scope_binding.authority_key_id != authority_key_id
            or scope_binding.authority_public_key_sha256
            != hashlib.sha256(authority_public).hexdigest()
        ):
            _fail("CAPITAL_BOOTSTRAP_SCOPE_BINDING_INVALID")
        authorization_signer = _ScopeBoundAuthorizationSigner(
            authorization_signer, scope_binding
        )

        return coordinator.CapitalAuthorityInputs(
            output_root=self._output_root,
            constraints=self._policy.constraints,
            precommitment_receipt=precommitment,
            trusted_precommitment_public_keys={
                self._precommitment_public_key[0]: self._precommitment_public_key[1]
            },
            frozen_candidate_rosters_by_phase=rosters,
            lineage_by_phase=lineages,
            replay_output_loader=replay_transport,
            generated_at_utc=generated_text,
            replay_evidence_signer=replay_signer,
            trusted_replay_public_keys={
                self._replay_key.key_id: self._replay_key.public_key_bytes
            },
            authority_key_id=authority_key_id,
            authorization_issued_at_utc=issued_text,
            authorization_expires_at_utc=expires_text,
            authorization_signer=authorization_signer,
            trusted_authority_public_keys={authority_key_id: authority_public},
            authorization_scope=scope_binding.authorization_scope,
            bootstrap_identity_sha256=scope_binding.bootstrap_identity_sha256,
            capital_config_sha256=scope_binding.config_sha256,
            authority_public_key_sha256=(scope_binding.authority_public_key_sha256),
            shard_attempt_limit=self._shard_attempt_limit,
        )


def bootstrap_production_capital_authority(
    *,
    bootstrap_root: Path,
    output_root: Path,
    authorization_scope: str,
    generation_context: authority_spool.GenerationSpoolContext,
    config_payload: Mapping[str, Any],
    native_source_identity: Mapping[str, Any],
    process_precommitment_payload: Mapping[str, Any],
    inner_selection_windows_by_anchor: Mapping[str, Mapping[str, str]],
    current_selection_window: Mapping[str, str],
    contract_lineage: Mapping[str, Any],
    margin_terms_sha256: str,
    broker_terms_sha256: str,
    replay_output_loader: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    | None = None,
    funded_boundary: FundedAuthorityBoundary | None = None,
    shard_attempt_limit: int = 8,
    environment: Mapping[str, str] | None = None,
) -> ProductionCapitalBootstrap:
    """Freeze and persist capital authority before process evidence access.

    ``replay_output_loader`` remains accepted so older test/caller keyword sets
    do not fail at bootstrap construction.  It is intentionally
    non-authoritative and is never invoked: genuine replay is derived later
    from coordinator-owned ``CandidateTrade`` objects inside the lazy factory.
    """

    if type(generation_context) is not authority_spool.GenerationSpoolContext:
        _fail("CAPITAL_BOOTSTRAP_GENERATION_CONTEXT_INVALID")
    try:
        authority_spool._assert_context_current(generation_context)
    except authority_spool.AuthoritySpoolError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_GENERATION_CONTEXT_INVALID"
        ) from exc
    generation_id = generation_context.generation_id
    if type(generation_id) is not str or not _GENERATION_ID.fullmatch(generation_id):
        _fail("CAPITAL_BOOTSTRAP_GENERATION_ID_INVALID")
    generation_context_sha256 = _sha(
        generation_context.context_sha256,
        "CAPITAL_BOOTSTRAP_GENERATION_CONTEXT_HASH_INVALID",
    )
    config_sha256 = _sha(
        generation_context.config_sha256,
        "CAPITAL_BOOTSTRAP_CONFIG_HASH_INVALID",
    )
    margin_terms_sha256 = _sha(
        margin_terms_sha256, "CAPITAL_BOOTSTRAP_MARGIN_TERMS_HASH_INVALID"
    )
    broker_terms_sha256 = _sha(
        broker_terms_sha256, "CAPITAL_BOOTSTRAP_BROKER_TERMS_HASH_INVALID"
    )
    if replay_output_loader is not None and not callable(replay_output_loader):
        _fail("CAPITAL_BOOTSTRAP_REPLAY_OUTPUT_LOADER_INVALID")
    if type(shard_attempt_limit) is not int or not 1 <= shard_attempt_limit <= 1024:
        _fail("CAPITAL_BOOTSTRAP_SHARD_ATTEMPT_LIMIT_INVALID")
    config = _object(config_payload, "CAPITAL_BOOTSTRAP_CONFIG_INVALID")
    if _sha_json(config) != config_sha256:
        _fail("CAPITAL_BOOTSTRAP_CONFIG_PAYLOAD_CONTEXT_MISMATCH")
    try:
        policy = authority_factory.load_capital_authority_policy(config)
    except authority_factory.CapitalAuthorityFactoryError as exc:
        raise ProductionCapitalBootstrapError(str(exc)) from exc
    if authorization_scope not in policy.permitted_deployment_scopes:
        _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_NOT_CONFIGURED")
    if authorization_scope not in {DEMO_PROBATION_SCOPE, FUNDED_LIVE_SCOPE}:
        _fail("CAPITAL_BOOTSTRAP_AUTHORIZATION_SCOPE_INVALID")

    process_payload = _object(
        process_precommitment_payload,
        "CAPITAL_BOOTSTRAP_PROCESS_PRECOMMITMENT_INVALID",
    )
    try:
        (
            _,
            process_anchors,
            process_authorized,
            process_not_before,
            process_deadline,
        ) = (
            causal._validate_precommit_payload(process_payload)
        )
    except causal.ProcessVerificationError as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_PROCESS_PRECOMMITMENT_INVALID"
        ) from exc
    created = _utc_now()
    if created.tzinfo is None or created.utcoffset() is None:
        _fail("CAPITAL_BOOTSTRAP_CLOCK_INVALID")
    created = created.astimezone(timezone.utc).replace(microsecond=0)
    if created < process_authorized:
        _fail("CAPITAL_BOOTSTRAP_CLOCK_PRECEDES_PROCESS_AUTHORIZATION")
    if created >= process_deadline:
        _fail("CAPITAL_BOOTSTRAP_CREATED_AFTER_PROCESS_EVIDENCE_DEADLINE")
    # Capital has its own signed precommitment boundary.  Keeping this later
    # than bootstrap creation preserves the strict Capital verifier contract
    # without delaying the already-frozen historical process or requiring a
    # caller sleep.
    capital_not_before = created + timedelta(seconds=1)

    native_identity = _validated_native_identity(native_source_identity)
    contract = _validated_contract_lineage(contract_lineage, native_identity)
    capital_anchors = _capital_anchor_plan(
        process_anchors, inner_selection_windows_by_anchor
    )
    current_window, _, _ = _window(
        current_selection_window,
        "CAPITAL_BOOTSTRAP_CURRENT_WINDOW_INVALID",
    )

    env: Mapping[str, str] = os.environ if environment is None else environment
    if not isinstance(env, Mapping):
        _fail("CAPITAL_BOOTSTRAP_KEY_ENVIRONMENT_INVALID")
    if authorization_scope == DEMO_PROBATION_SCOPE:
        if funded_boundary is not None:
            _fail("CAPITAL_BOOTSTRAP_FUNDED_BOUNDARY_IN_DEMO")
        precommit_key = _loaded_demo_key()
        replay_key = _loaded_demo_key()
        authorization_key: authority_factory._LoadedSigningKey | tuple[str, bytes] = (
            _loaded_demo_key()
        )
        authorization_exchange_base = None
        funded_trust = None
    else:
        if type(funded_boundary) is not FundedAuthorityBoundary:
            _fail("FUNDED_AUTHORIZATION_BOUNDARY_MISSING")
        funded_trust = _funded_authorization_trust(config)
        try:
            authorization_exchange_base = authority_factory._absolute_path(
                Path(funded_trust["authorization_exchange_root"]),
                "CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_EXCHANGE_ROOT_INVALID",
            )
            if (
                len(str(authorization_exchange_base))
                + MAX_AUTHORIZATION_EXCHANGE_DERIVED_PATH_CHARS
                > MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS
            ):
                _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_LENGTH_EXCEEDED")
            if (
                str(authorization_exchange_base)
                != funded_trust["authorization_exchange_root"]
                or not authorization_exchange_base.exists()
                or authority_factory._is_reparse(authorization_exchange_base)
                or not authorization_exchange_base.is_dir()
            ):
                _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_EXCHANGE_ROOT_INVALID")
        except (
            OSError,
            TypeError,
            authority_factory.CapitalAuthorityFactoryError,
        ) as exc:
            raise ProductionCapitalBootstrapError(
                "CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_EXCHANGE_ROOT_INVALID"
            ) from exc
        try:
            precommit_key = authority_factory._load_signing_key(
                funded_boundary.precommitment_signing_key_source,
                environment=env,
                code="CAPITAL_BOOTSTRAP_PRECOMMITMENT_SIGNING",
            )
            replay_key = authority_factory._load_signing_key(
                funded_boundary.replay_signing_key_source,
                environment=env,
                code="CAPITAL_BOOTSTRAP_REPLAY_SIGNING",
            )
            authorization_key = authority_factory._load_public_key(
                funded_boundary.authorization_public_key_source,
                environment=env,
                code="CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_PUBLIC_KEY",
            )
        except authority_factory.CapitalAuthorityFactoryError as exc:
            raise ProductionCapitalBootstrapError(str(exc)) from exc
        authorization_key_id, authorization_public = authorization_key
        if (
            authorization_key_id != funded_trust["authorization_key_id"]
            or hashlib.sha256(authorization_public).hexdigest()
            != funded_trust["authorization_public_key_sha256"]
        ):
            _fail("CAPITAL_BOOTSTRAP_FUNDED_AUTHORIZATION_TRUST_MISMATCH")

    authorization_key_id = (
        authorization_key.key_id
        if isinstance(authorization_key, authority_factory._LoadedSigningKey)
        else authorization_key[0]
    )
    role_key_ids = {
        precommit_key.key_id,
        replay_key.key_id,
        authorization_key_id,
    }
    if len(role_key_ids) != 3:
        _fail("CAPITAL_BOOTSTRAP_KEY_ROLES_NOT_SEPARATED")

    candidate_policies = [dict(item) for item in policy.candidate_policies]
    observed_totals = {
        float(item["max_total_stop_risk_pct"]) for item in candidate_policies
    }
    required_totals = {
        float(value) for value in cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    }
    if not required_totals.issubset(observed_totals):
        _fail("CAPITAL_BOOTSTRAP_CANDIDATE_COVERAGE_INCOMPLETE")

    identity = {
        "schema_version": CAPITAL_BOOTSTRAP_IDENTITY_SCHEMA,
        "authorization_scope": authorization_scope,
        "generation_id": generation_id,
        "generation_context_sha256": generation_context_sha256,
        "config_sha256": config_sha256,
        "generation_native_universe_sha256": generation_context.native_universe_sha256,
        "config_payload_sha256": _sha_json(config),
        "native_source_identity": native_identity,
        "native_source_identity_sha256": _sha_json(native_identity),
        "process_id": process_payload["process_id"],
        "process_precommitment_sha256": _sha_json(process_payload),
        "process_definition_sha256": process_payload["process_definition_sha256"],
        "process_evidence_access_not_before_utc": _utc_text(process_not_before),
        "process_evidence_access_deadline_utc": _utc_text(process_deadline),
        "candidate_policy_roster_sha256": _sha_json(candidate_policies),
        "required_total_stop_risk_candidates_pct": list(
            cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
        ),
        "capital_anchor_plan_sha256": _sha_json(capital_anchors),
        "current_selection_window_sha256": _sha_json(current_window),
        "contract_lineage_sha256": _sha_json(contract),
        "margin_terms_sha256": margin_terms_sha256,
        "broker_terms_sha256": broker_terms_sha256,
        "created_at_utc": _utc_text(created),
        "precommitment_key_id": precommit_key.key_id,
        "replay_key_id": replay_key.key_id,
        "authorization_key_id": authorization_key_id,
        "funded_authorization_exchange_root": (
            str(authorization_exchange_base)
            if authorization_scope == FUNDED_LIVE_SCOPE
            else None
        ),
        "funded_authorization_channel_policy": (
            FUNDED_AUTHORIZATION_CHANNEL_POLICY
            if authorization_scope == FUNDED_LIVE_SCOPE
            else None
        ),
        "funded_authorization_trust_sha256": (
            _sha_json(funded_trust) if funded_trust is not None else None
        ),
    }
    precommitment_id = _sha_json(identity)
    authorization_exchange_root = (
        authorization_exchange_base / generation_id / precommitment_id
        if authorization_exchange_base is not None
        else None
    )
    if authorization_exchange_root is not None:
        if (
            len(str(authorization_exchange_root))
            + MAX_AUTHORIZATION_EXCHANGE_DERIVED_PATH_CHARS
            > MAX_AUTHORIZATION_EXCHANGE_PATH_CHARS
        ):
            _fail("FUNDED_AUTHORIZATION_EXCHANGE_PATH_LENGTH_EXCEEDED")
        _ensure_directory(authorization_exchange_root)
        _bounded_exchange_files(authorization_exchange_root)
    precommitment_body = {
        "schema_version": cap.CAP_PRECOMMIT_SCHEMA_VERSION,
        "precommitment_id": precommitment_id,
        "created_at_utc": _utc_text(created),
        "evidence_access_not_before_utc": _utc_text(capital_not_before),
        "candidate_policies": candidate_policies,
        "anchor_plan": capital_anchors,
        "current_selection_window": current_window,
        "selection_rule": _snapshot(cap._FROZEN_SELECTION_RULE),
        "constraints_sha256": cap.canonical_json_sha256(asdict(policy.constraints)),
        "contract_lineage": contract,
    }
    precommitment = _signed_envelope(
        precommit_key, cap.PRECOMMITMENT_SIGNATURE_KIND, precommitment_body
    )
    try:
        verified_body, precommit_meta = cap._verified_envelope(
            precommitment,
            kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys={precommit_key.key_id: precommit_key.public_key_bytes},
            missing_reason="CAP_PRECOMMITMENT_MISSING",
        )
        verified_policies, _, _ = cap._precommitment(verified_body, policy.constraints)
    except Exception as exc:
        raise ProductionCapitalBootstrapError(
            "CAPITAL_BOOTSTRAP_PRECOMMITMENT_SELF_VERIFICATION_FAILED"
        ) from exc
    if (
        verified_body != precommitment_body
        or list(verified_policies.values()) != candidate_policies
    ):
        _fail("CAPITAL_BOOTSTRAP_PRECOMMITMENT_SELF_VERIFICATION_FAILED")

    root = _absolute_root(bootstrap_root, "CAPITAL_BOOTSTRAP_ROOT_INVALID")
    generation_root = root / generation_id
    _ensure_directory(generation_root)
    if _existing_regular_files(generation_root):
        _fail("CAPITAL_BOOTSTRAP_GENERATION_ALREADY_POPULATED")
    precommitment_descriptor = _content_descriptor(generation_root, precommitment)
    authorization_public = (
        authorization_key.public_key_bytes
        if isinstance(authorization_key, authority_factory._LoadedSigningKey)
        else authorization_key[1]
    )
    bootstrap_receipt_body = {
        "schema_version": CAPITAL_BOOTSTRAP_SCHEMA,
        "authorization_scope": authorization_scope,
        "identity": identity,
        "identity_sha256": precommitment_id,
        "precommitment_body_sha256": precommit_meta["body_sha256"],
        "precommitment_envelope_sha256": precommit_meta["envelope_sha256"],
        "precommitment_artifact": precommitment_descriptor.to_dict(),
        "trusted_role_keys": {
            "precommitment": {
                "key_id": precommit_key.key_id,
                "public_key_base64": base64.b64encode(
                    precommit_key.public_key_bytes
                ).decode("ascii"),
            },
            "replay": {
                "key_id": replay_key.key_id,
                "public_key_base64": base64.b64encode(
                    replay_key.public_key_bytes
                ).decode("ascii"),
            },
            "authorization": {
                "key_id": authorization_key_id,
                "public_key_base64": base64.b64encode(authorization_public).decode(
                    "ascii"
                ),
            },
        },
        "created_before_process_evidence_access": True,
        "confers_numeric_capital_authority": False,
        "confers_funded_live_authority": False,
    }
    bootstrap_descriptor = _content_descriptor(generation_root, bootstrap_receipt_body)
    files = _existing_regular_files(generation_root)
    if len(files) != MAX_BOOTSTRAP_ARTIFACTS:
        _fail("CAPITAL_BOOTSTRAP_STORAGE_BOUND_INVALID")

    resolved_output_root = _absolute_root(
        output_root, "CAPITAL_BOOTSTRAP_OUTPUT_ROOT_INVALID"
    )
    factory = _BootstrapCapitalFactory(
        authorization_key=authorization_key,
        authorization_exchange_root=authorization_exchange_root,
        authorization_scope=authorization_scope,
        bootstrap_descriptor=bootstrap_descriptor,
        bootstrap_root=generation_root,
        broker_terms_sha256=broker_terms_sha256,
        config_payload=config,
        identity=identity,
        margin_terms_sha256=margin_terms_sha256,
        output_root=resolved_output_root,
        policy=policy,
        precommitment_descriptor=precommitment_descriptor,
        precommitment_public_key=(
            precommit_key.key_id,
            precommit_key.public_key_bytes,
        ),
        process_precommitment_payload=process_payload,
        replay_key=replay_key,
        shard_attempt_limit=shard_attempt_limit,
    )
    # Reopen and verify both content-addressed artifacts before returning a
    # callable that may later observe search/replay evidence.
    factory._verified_precommitment()
    forward_binding_body = {
        "schema_version": CAPITAL_FORWARD_PHASE_BINDING_SCHEMA,
        "generation_id": generation_id,
        "generation_context_sha256": generation_context_sha256,
        "config_sha256": config_sha256,
        "native_universe_sha256": generation_context.native_universe_sha256,
        "authorization_scope": authorization_scope,
        "bootstrap_identity": identity,
        "bootstrap_identity_sha256": precommitment_id,
        "bootstrap_generation_root": str(generation_root),
        "bootstrap_receipt_descriptor": bootstrap_descriptor.to_dict(),
        "capital_precommitment_descriptor": precommitment_descriptor.to_dict(),
        "capital_precommitment": precommitment_body,
        "capital_precommitment_body_sha256": precommit_meta["body_sha256"],
        "capital_precommitment_envelope_sha256": precommit_meta["envelope_sha256"],
        "anchor_plan": capital_anchors,
        "current_selection_window": current_window,
        "contract_lineage": contract,
        "margin_terms_sha256": margin_terms_sha256,
        "broker_terms_sha256": broker_terms_sha256,
        "created_before_process_evidence_access": True,
        "confers_numeric_capital_publication_or_trading_authority": False,
    }
    forward_binding_raw = _canonical_bytes(forward_binding_body)
    forward_phase_binding = CapitalForwardPhaseBinding(
        _body_json=forward_binding_raw,
        receipt_sha256=hashlib.sha256(forward_binding_raw).hexdigest(),
        _seal=_CAPITAL_FORWARD_PHASE_BINDING_SEAL,
    )
    verify_capital_forward_phase_binding(generation_context, forward_phase_binding)
    return ProductionCapitalBootstrap(
        generation_id=generation_id,
        authorization_scope=authorization_scope,
        generation_root=generation_root,
        identity_sha256=precommitment_id,
        candidate_policy_roster_sha256=identity["candidate_policy_roster_sha256"],
        precommitment=precommitment_descriptor,
        bootstrap_receipt=bootstrap_descriptor,
        forward_phase_binding=forward_phase_binding,
        capital_factory=factory,
    )


__all__ = [
    "CAPITAL_BOOTSTRAP_IDENTITY_SCHEMA",
    "CAPITAL_BOOTSTRAP_SCHEMA",
    "CAPITAL_FORWARD_PHASE_BINDING_SCHEMA",
    "DEMO_PROBATION_SCOPE",
    "FUNDED_AUTHORIZATION_CHANNEL_POLICY",
    "FUNDED_AUTHORIZATION_REQUEST_ISSUED_SCHEMA",
    "FUNDED_AUTHORIZATION_REQUEST_ISSUED_SIGNATURE_KIND",
    "FUNDED_AUTHORIZATION_REQUEST_SCHEMA",
    "FUNDED_AUTHORIZATION_RESPONSE_SCHEMA",
    "FUNDED_AUTHORIZATION_RESPONSE_SIGNATURE_KIND",
    "FUNDED_AUTHORIZATION_TRUST_SCHEMA",
    "FUNDED_LIVE_SCOPE",
    "BootstrapArtifactDescriptor",
    "CapitalAuthorizationScopeBinding",
    "CapitalForwardPhaseBinding",
    "FundedAuthorityBoundary",
    "ProductionCapitalBootstrap",
    "ProductionCapitalBootstrapError",
    "bootstrap_production_capital_authority",
    "funded_authorization_response_signature_payload",
    "verify_capital_forward_phase_binding",
    "verify_capital_forward_phase_binding_body",
]
