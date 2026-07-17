"""Fail-closed capital-authority inputs and external signing transport."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import os
import re
import stat
import threading
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

import pm_cap_calibration as cap
import pm_cap_replay_producer as cap_replay
import pm_portfolio_replay as replay
import pm_production_authority_coordinator as coordinator

CapitalAuthorityInputs = coordinator.CapitalAuthorityInputs


CAPITAL_AUTHORITY_POLICY_SCHEMA = "fxpm.capital-authority-policy.v1"
DEMO_PROBATION_SCOPE = "DEMO_PROBATION"
FUNDED_LIVE_SCOPE = "FUNDED_LIVE"
MAX_AUTHORIZATION_TTL_SECONDS = 31 * 24 * 60 * 60
MAX_KEY_FILE_BYTES = 4096
MAX_PRECOMMITMENT_BYTES = 4 * 1024 * 1024

_POLICY_CONFIG_FIELDS = frozenset(
    {
        "schema_version",
        "constraints",
        "candidate_policy_points",
        "permitted_deployment_scopes",
        "authorization_max_ttl_seconds",
    }
)
_POLICY_POINT_FIELDS = frozenset(
    {
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
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
_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_KEY_ID_PREFIX = "ed25519-sha256:"


class CapitalAuthorityFactoryError(ValueError):
    """A typed pre-CAS authority construction failure."""


def _fail(code: str) -> None:
    raise CapitalAuthorityFactoryError(code)


@dataclass(frozen=True, slots=True)
class KeyMaterialSource:
    """One raw 32-byte key, sourced from an absolute path or base64 env var."""

    path: Path | None = None
    base64_env_var: str | None = None


@dataclass(frozen=True, slots=True)
class SigningKeySource:
    """An externally provisioned Ed25519 private/public pair."""

    private_key: KeyMaterialSource
    public_key: KeyMaterialSource


@dataclass(frozen=True, slots=True)
class CapitalAuthorityPolicy:
    constraints: cap.CapCalibrationConstraints
    candidate_policies: tuple[Mapping[str, Any], ...]
    permitted_deployment_scopes: tuple[str, ...]
    authorization_max_ttl_seconds: int


@dataclass(frozen=True, slots=True)
class _LoadedSigningKey:
    key_id: str
    public_key_bytes: bytes
    private_key: Ed25519PrivateKey


def canonical_ed25519_key_id(public_key_bytes: bytes) -> str:
    if type(public_key_bytes) is not bytes or len(public_key_bytes) != 32:
        _fail("PUBLIC_KEY_LENGTH_INVALID")
    try:
        Ed25519PublicKey.from_public_bytes(public_key_bytes)
    except ValueError as exc:
        raise CapitalAuthorityFactoryError("PUBLIC_KEY_INVALID") from exc
    return _KEY_ID_PREFIX + hashlib.sha256(public_key_bytes).hexdigest()


def _json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    _fail("JSON_NONFINITE_NUMBER")


def _json_copy(value: Any, code: str) -> Any:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_json_pairs,
            parse_constant=_reject_constant,
        )
    except CapitalAuthorityFactoryError:
        raise
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise CapitalAuthorityFactoryError(code) from exc


def _json_object(value: Any, code: str) -> dict[str, Any]:
    copied = _json_copy(value, code)
    if type(copied) is not dict:
        _fail(code)
    return copied


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
                _fail("PATH_REPARSE_POINT_FORBIDDEN")


def _absolute_path(value: Path, code: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        _fail(code)
    result = Path(os.path.abspath(candidate))
    _reject_reparse_chain(result)
    return result


def _read_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
    private: bool,
    code: str,
) -> bytes:
    target = _absolute_path(path, f"{code}_PATH_NOT_ABSOLUTE")
    try:
        info = target.lstat()
    except OSError as exc:
        raise CapitalAuthorityFactoryError(f"{code}_MISSING") from exc
    if _is_reparse(target) or not stat.S_ISREG(info.st_mode):
        _fail(f"{code}_NOT_SAFE_REGULAR_FILE")
    if private and os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o077:
        _fail(f"{code}_PERMISSIONS_TOO_BROAD")
    if not 0 < info.st_size <= maximum_bytes:
        _fail(f"{code}_SIZE_INVALID")
    try:
        with target.open("rb") as handle:
            encoded = handle.read(maximum_bytes + 1)
    except OSError as exc:
        raise CapitalAuthorityFactoryError(f"{code}_READ_FAILED") from exc
    if len(encoded) != info.st_size or len(encoded) > maximum_bytes:
        _fail(f"{code}_SIZE_CHANGED")
    return encoded


def _material_bytes(
    source: KeyMaterialSource,
    *,
    environment: Mapping[str, str],
    private: bool,
    code: str,
) -> bytes:
    if type(source) is not KeyMaterialSource:
        _fail(f"{code}_SOURCE_INVALID")
    path_selected = source.path is not None
    env_selected = source.base64_env_var is not None
    if path_selected == env_selected:
        _fail(f"{code}_SOURCE_AMBIGUOUS")
    if path_selected:
        material = _read_regular_file(
            Path(source.path),
            maximum_bytes=MAX_KEY_FILE_BYTES,
            private=private,
            code=code,
        )
    else:
        name = source.base64_env_var
        if type(name) is not str or not _ENV_NAME.fullmatch(name):
            _fail(f"{code}_ENV_NAME_INVALID")
        text = environment.get(name)
        if type(text) is not str or not text or text != text.strip():
            _fail(f"{code}_ENV_MISSING")
        try:
            material = base64.b64decode(text, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise CapitalAuthorityFactoryError(f"{code}_ENV_ENCODING_INVALID") from exc
        if base64.b64encode(material).decode("ascii") != text:
            _fail(f"{code}_ENV_ENCODING_NOT_CANONICAL")
    if len(material) != 32:
        _fail(f"{code}_LENGTH_INVALID")
    return material


def _load_public_key(
    source: KeyMaterialSource,
    *,
    environment: Mapping[str, str],
    code: str,
) -> tuple[str, bytes]:
    public_bytes = _material_bytes(
        source,
        environment=environment,
        private=False,
        code=code,
    )
    key_id = canonical_ed25519_key_id(public_bytes)
    return key_id, public_bytes


def _load_signing_key(
    source: SigningKeySource,
    *,
    environment: Mapping[str, str],
    code: str,
) -> _LoadedSigningKey:
    if type(source) is not SigningKeySource:
        _fail(f"{code}_SOURCE_INVALID")
    private_bytes = bytearray(
        _material_bytes(
            source.private_key,
            environment=environment,
            private=True,
            code=f"{code}_PRIVATE_KEY",
        )
    )
    public_bytes = _material_bytes(
        source.public_key,
        environment=environment,
        private=False,
        code=f"{code}_PUBLIC_KEY",
    )
    try:
        private_key = Ed25519PrivateKey.from_private_bytes(bytes(private_bytes))
    except ValueError as exc:
        raise CapitalAuthorityFactoryError(f"{code}_PRIVATE_KEY_INVALID") from exc
    finally:
        private_bytes[:] = b"\0" * len(private_bytes)
    derived = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    if not hmac.compare_digest(derived, public_bytes):
        _fail(f"{code}_KEY_PAIR_MISMATCH")
    return _LoadedSigningKey(
        key_id=canonical_ed25519_key_id(public_bytes),
        public_key_bytes=bytes(public_bytes),
        private_key=private_key,
    )


def _load_json_file(path: Path, *, maximum_bytes: int, code: str) -> dict[str, Any]:
    encoded = _read_regular_file(
        path,
        maximum_bytes=maximum_bytes,
        private=False,
        code=code,
    )
    try:
        value = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_json_pairs,
            parse_constant=_reject_constant,
        )
    except CapitalAuthorityFactoryError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CapitalAuthorityFactoryError(f"{code}_JSON_INVALID") from exc
    if type(value) is not dict:
        _fail(f"{code}_JSON_OBJECT_REQUIRED")
    cap.canonical_json_sha256(value)
    return value


def load_capital_authority_policy(
    config_payload: Mapping[str, Any],
) -> CapitalAuthorityPolicy:
    config = _json_object(config_payload, "CONFIG_INVALID")
    pipeline = config.get("pipeline")
    if type(pipeline) is not dict:
        _fail("PIPELINE_CONFIG_MISSING")
    raw = pipeline.get("production_capital_authority")
    if type(raw) is not dict or set(raw) != _POLICY_CONFIG_FIELDS:
        _fail("CAPITAL_AUTHORITY_POLICY_CONFIG_MISSING_OR_INVALID")
    if raw["schema_version"] != CAPITAL_AUTHORITY_POLICY_SCHEMA:
        _fail("CAPITAL_AUTHORITY_POLICY_VERSION_INVALID")

    constraint_fields = {item.name for item in fields(cap.CapCalibrationConstraints)}
    raw_constraints = raw["constraints"]
    if type(raw_constraints) is not dict or set(raw_constraints) != constraint_fields:
        _fail("CAPITAL_AUTHORITY_CONSTRAINTS_NOT_EXPLICIT")
    try:
        constraints = cap.CapCalibrationConstraints(**raw_constraints)
        constraints.validate()
    except (TypeError, ValueError) as exc:
        raise CapitalAuthorityFactoryError(
            "CAPITAL_AUTHORITY_CONSTRAINTS_INVALID"
        ) from exc

    raw_points = raw["candidate_policy_points"]
    if type(raw_points) is not list or any(
        type(point) is not dict or set(point) != _POLICY_POINT_FIELDS
        for point in raw_points
    ):
        _fail("CAPITAL_AUTHORITY_CANDIDATE_POINTS_INVALID")
    try:
        candidate_policies = cap.build_frozen_production_candidate_roster(raw_points)
    except (TypeError, ValueError) as exc:
        raise CapitalAuthorityFactoryError(
            "CAPITAL_AUTHORITY_CANDIDATE_ROSTER_INVALID"
        ) from exc
    if len(candidate_policies) > constraints.maximum_candidate_policies:
        _fail("CAPITAL_AUTHORITY_CANDIDATES_EXCEED_CONSTRAINT")

    raw_scopes = raw["permitted_deployment_scopes"]
    if (
        type(raw_scopes) is not list
        or not raw_scopes
        or any(type(scope) is not str for scope in raw_scopes)
        or len(set(raw_scopes)) != len(raw_scopes)
        or set(raw_scopes) - {DEMO_PROBATION_SCOPE, FUNDED_LIVE_SCOPE}
    ):
        _fail("CAPITAL_AUTHORITY_SCOPE_ALLOWLIST_INVALID")
    scopes = tuple(sorted(raw_scopes))
    ttl = raw["authorization_max_ttl_seconds"]
    if type(ttl) is not int or not 1 <= ttl <= MAX_AUTHORIZATION_TTL_SECONDS:
        _fail("CAPITAL_AUTHORITY_TTL_INVALID")
    return CapitalAuthorityPolicy(
        constraints=constraints,
        candidate_policies=tuple(candidate_policies),
        permitted_deployment_scopes=scopes,
        authorization_max_ttl_seconds=ttl,
    )


def _canonical_utc(value: Any, code: str) -> tuple[str, datetime]:
    if type(value) is not str:
        _fail(code)
    probe = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(probe)
    except ValueError as exc:
        raise CapitalAuthorityFactoryError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    parsed = parsed.astimezone(timezone.utc)
    canonical = parsed.isoformat().replace("+00:00", "Z")
    if canonical != value:
        _fail(code)
    return canonical, parsed


def _phase_specs(precommit_body: Mapping[str, Any]) -> list[dict[str, Any]]:
    phases: list[dict[str, Any]] = []
    for anchor in precommit_body["anchor_plan"]:
        phases.extend(
            [
                {
                    "anchor_id": anchor["anchor_id"],
                    "phase": "INNER_SELECTION",
                    "window": dict(anchor["inner_window"]),
                },
                {
                    "anchor_id": anchor["anchor_id"],
                    "phase": "OUTER_EVALUATION",
                    "window": dict(anchor["outer_window"]),
                },
            ]
        )
    phases.append(
        {
            "anchor_id": "CURRENT",
            "phase": "CURRENT_SELECTION",
            "window": dict(precommit_body["current_selection_window"]),
        }
    )
    return phases


def _derived_rosters(
    phase_specs: Sequence[Mapping[str, Any]],
    candidates_by_phase: Mapping[str, Sequence[replay.CandidateTrade]],
) -> dict[str, dict[str, Any]]:
    phase_ids = {f"{spec['anchor_id']}|{spec['phase']}" for spec in phase_specs}
    if type(candidates_by_phase) is not dict or set(candidates_by_phase) != phase_ids:
        _fail("CAPITAL_REPLAY_PHASE_ROSTER_INCOMPLETE")
    result: dict[str, dict[str, Any]] = {}
    for phase_id in sorted(phase_ids):
        raw_candidates = candidates_by_phase[phase_id]
        if not isinstance(raw_candidates, Sequence) or isinstance(
            raw_candidates, (str, bytes, bytearray)
        ):
            _fail("CAPITAL_REPLAY_CANDIDATE_SEQUENCE_INVALID")
        candidates = tuple(raw_candidates)
        if any(type(item) is not replay.CandidateTrade for item in candidates):
            _fail("CAPITAL_REPLAY_CANDIDATE_TYPE_INVALID")
        try:
            supplied = cap_replay._frozen_roster(
                replay.portfolio_replay_candidate_roster_receipt(candidates)
            )
        except (TypeError, ValueError) as exc:
            raise CapitalAuthorityFactoryError(
                "CAPITAL_REPLAY_ROSTER_RECEIPT_INVALID"
            ) from exc
        result[phase_id] = supplied
    return result


def _verified_lineages(
    phase_specs: Sequence[Mapping[str, Any]],
    lineage_by_phase: Mapping[str, Mapping[str, Any]],
    contract_lineage: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    phase_ids = {f"{spec['anchor_id']}|{spec['phase']}" for spec in phase_specs}
    if type(lineage_by_phase) is not dict or set(lineage_by_phase) != phase_ids:
        _fail("CAPITAL_REPLAY_PHASE_LINEAGE_INCOMPLETE")
    result: dict[str, dict[str, Any]] = {}
    for phase_id in sorted(phase_ids):
        try:
            lineage = cap_replay._lineage_without_output(
                _json_object(
                    lineage_by_phase[phase_id],
                    "CAPITAL_REPLAY_LINEAGE_INVALID",
                )
            )
        except (TypeError, ValueError) as exc:
            raise CapitalAuthorityFactoryError(
                "CAPITAL_REPLAY_LINEAGE_INVALID"
            ) from exc
        if any(lineage.get(name) != value for name, value in contract_lineage.items()):
            _fail("CAPITAL_REPLAY_LINEAGE_NOT_PRECOMMITTED")
        expected_role = (
            "PROCESS_VALIDATED_CURRENT_REFIT"
            if phase_id == "CURRENT|CURRENT_SELECTION"
            else "ANCHOR_LOCAL_REFIT"
        )
        if lineage["book_role"] != expected_role:
            _fail("CAPITAL_REPLAY_BOOK_ROLE_INVALID")
        result[phase_id] = lineage
    return result


class _VerifiedReplayOutputTransport:
    __slots__ = ("_delegate", "_expected", "_lock", "_seen")

    def __init__(
        self,
        delegate: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        expected: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self._delegate = delegate
        self._expected = dict(expected)
        self._lock = threading.Lock()
        self._seen: set[str] = set()

    def __repr__(self) -> str:
        return "<verified-capital-replay-output-transport>"

    def __call__(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        supplied = _json_object(request, "CAPITAL_REPLAY_REQUEST_INVALID")
        attempt_id = supplied.get("attempt_id")
        if type(attempt_id) is not str or self._expected.get(attempt_id) != supplied:
            _fail("CAPITAL_REPLAY_REQUEST_NOT_PRECOMMITTED")
        with self._lock:
            if attempt_id in self._seen:
                _fail("CAPITAL_REPLAY_REQUEST_REPEATED")
            self._seen.add(attempt_id)
        output = self._delegate(supplied)
        if type(output) is not dict or set(output) != cap_replay._RESULT_KEYS:
            _fail("CAPITAL_REPLAY_OUTPUT_INVALID")
        return output


class _ExactEd25519Signer:
    __slots__ = ("_key", "_kind", "_validator")

    def __init__(
        self,
        key: _LoadedSigningKey,
        kind: str,
        validator: Callable[[Mapping[str, Any]], None],
    ) -> None:
        self._key = key
        self._kind = kind
        self._validator = validator

    def __repr__(self) -> str:
        return f"<external-ed25519-signer kind={self._kind}>"

    def __call__(self, body: Mapping[str, Any]) -> Mapping[str, Any]:
        exact = _json_object(body, f"CAPITAL_{self._kind}_BODY_INVALID")
        self._validator(exact)
        signature = self._key.private_key.sign(
            cap.capital_authority_signature_payload(
                self._kind,
                self._key.key_id,
                exact,
            )
        )
        return {
            "schema_version": cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION,
            "key_id": self._key.key_id,
            "body": exact,
            "signature_base64": base64.b64encode(signature).decode("ascii"),
        }


def _expected_replay_requests(
    phase_specs: Sequence[Mapping[str, Any]],
    policies: Sequence[Mapping[str, Any]],
    rosters: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for spec in phase_specs:
        phase_id = f"{spec['anchor_id']}|{spec['phase']}"
        for policy in sorted(policies, key=lambda value: str(value["policy_id"])):
            attempt_id = f"{phase_id}|{policy['policy_id']}"
            expected[attempt_id] = {
                "attempt_id": attempt_id,
                "anchor_id": spec["anchor_id"],
                "phase": spec["phase"],
                "window": dict(spec["window"]),
                "policy": dict(policy),
                "frozen_candidate_roster": dict(rosters[phase_id]),
            }
    return expected


def build_capital_authority_inputs(
    *,
    authority_request: coordinator.CapitalAuthorityRequest,
    config_payload: Mapping[str, Any],
    output_root: Path,
    precommitment_receipt_path: Path,
    precommitment_public_key_source: KeyMaterialSource,
    replay_signing_key_source: SigningKeySource,
    authorization_signing_key_source: SigningKeySource,
    margin_terms_sha256: str,
    broker_terms_sha256: str,
    replay_output_loader: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    generated_at_utc: str,
    authorization_issued_at_utc: str,
    authorization_expires_at_utc: str,
    authorization_scope: str = DEMO_PROBATION_SCOPE,
    allow_funded_authorization: bool = False,
    shard_attempt_limit: int = 8,
    environment: Mapping[str, str] | None = None,
) -> CapitalAuthorityInputs:
    """Derive exact coordinator inputs from coordinator-verified states.

    The public API intentionally accepts neither a frozen current candidate
    receipt nor a current lineage mapping.  Both are derived here after the
    coordinator has verified the current physical ledger and replay evidence.
    """

    if (
        type(authority_request) is not coordinator.CapitalAuthorityRequest
        or type(authority_request._historical_state) is not coordinator._HistoricalState
        or type(authority_request._current_state) is not coordinator._CurrentState
    ):
        _fail("CAPITAL_AUTHORITY_REQUEST_INVALID")
    historical_state = authority_request._historical_state
    current_state = authority_request._current_state

    policy = load_capital_authority_policy(config_payload)
    if authority_request.authorization_scope != authorization_scope:
        _fail("CAPITAL_AUTHORIZATION_SCOPE_REQUEST_MISMATCH")
    if authorization_scope not in policy.permitted_deployment_scopes:
        _fail("CAPITAL_AUTHORITY_SCOPE_NOT_CONFIGURED")
    if authorization_scope == FUNDED_LIVE_SCOPE:
        if allow_funded_authorization is not True:
            _fail("FUNDED_CAPITAL_AUTHORIZATION_NOT_EXPLICITLY_ENABLED")
    elif authorization_scope != DEMO_PROBATION_SCOPE:
        _fail("CAPITAL_AUTHORITY_SCOPE_INVALID")
    if type(allow_funded_authorization) is not bool:
        _fail("FUNDED_CAPITAL_AUTHORIZATION_FLAG_INVALID")
    if type(shard_attempt_limit) is not int or not 1 <= shard_attempt_limit <= 1024:
        _fail("CAPITAL_REPLAY_SHARD_ATTEMPT_LIMIT_INVALID")
    if not callable(replay_output_loader):
        _fail("CAPITAL_REPLAY_OUTPUT_LOADER_MISSING")

    resolved_output_root = _absolute_path(
        output_root, "CAPITAL_OUTPUT_ROOT_NOT_ABSOLUTE"
    )
    if resolved_output_root.exists() and (
        _is_reparse(resolved_output_root) or not resolved_output_root.is_dir()
    ):
        _fail("CAPITAL_OUTPUT_ROOT_INVALID")

    env: Mapping[str, str] = os.environ if environment is None else environment
    if not isinstance(env, Mapping):
        _fail("CAPITAL_KEY_ENVIRONMENT_INVALID")
    precommit_key_id, precommit_public = _load_public_key(
        precommitment_public_key_source,
        environment=env,
        code="CAPITAL_PRECOMMIT_PUBLIC_KEY",
    )
    replay_key = _load_signing_key(
        replay_signing_key_source,
        environment=env,
        code="CAPITAL_REPLAY_SIGNING",
    )
    authorization_key = _load_signing_key(
        authorization_signing_key_source,
        environment=env,
        code="CAPITAL_AUTHORIZATION_SIGNING",
    )
    if replay_key.key_id == authorization_key.key_id:
        _fail("CAPITAL_SIGNING_ROLE_KEYS_NOT_SEPARATED")

    precommitment = _load_json_file(
        precommitment_receipt_path,
        maximum_bytes=MAX_PRECOMMITMENT_BYTES,
        code="CAPITAL_PRECOMMITMENT_RECEIPT",
    )
    if precommitment.get("key_id") != precommit_key_id:
        _fail("CAPITAL_PRECOMMITMENT_KEY_ID_MISMATCH")
    precommit_keys = {precommit_key_id: precommit_public}
    try:
        precommit_body, precommit_meta = cap._verified_envelope(
            precommitment,
            kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys=precommit_keys,
            missing_reason="CAP_PRECOMMITMENT_MISSING",
        )
        parsed_policies, _, precommit_state = cap._precommitment(
            precommit_body, policy.constraints
        )
    except (TypeError, ValueError) as exc:
        raise CapitalAuthorityFactoryError(
            "CAPITAL_PRECOMMITMENT_VERIFICATION_FAILED"
        ) from exc
    if list(parsed_policies.values()) != list(policy.candidate_policies):
        _fail("CAPITAL_PRECOMMITMENT_POLICY_ROSTER_CONFIG_MISMATCH")

    generated_text, generated = _canonical_utc(
        generated_at_utc, "CAPITAL_REPLAY_GENERATION_TIME_INVALID"
    )
    issued_text, issued = _canonical_utc(
        authorization_issued_at_utc,
        "CAPITAL_AUTHORIZATION_ISSUE_TIME_INVALID",
    )
    expires_text, expires = _canonical_utc(
        authorization_expires_at_utc,
        "CAPITAL_AUTHORIZATION_EXPIRY_TIME_INVALID",
    )
    _, validation_time = _canonical_utc(
        authority_request.validation_time_utc, "CAPITAL_VALIDATION_TIME_INVALID"
    )
    if generated < precommit_state["not_before"] or generated > validation_time:
        _fail("CAPITAL_REPLAY_GENERATION_TIME_OUTSIDE_AUTHORITY_WINDOW")
    if not generated <= issued <= validation_time < expires:
        _fail("CAPITAL_AUTHORIZATION_NOT_CURRENT")
    ttl = expires - issued
    if ttl <= timedelta(0) or ttl > timedelta(
        seconds=policy.authorization_max_ttl_seconds
    ):
        _fail("CAPITAL_AUTHORIZATION_TTL_EXCEEDED")

    phase_specs = _phase_specs(precommit_body)
    candidates_by_phase = {
        str(phase_id): tuple(values)
        for phase_id, values in historical_state.capital_candidates_by_phase.items()
    }
    current_phase = "CURRENT|CURRENT_SELECTION"
    if current_phase in candidates_by_phase:
        _fail("CAPITAL_CURRENT_CANDIDATES_CALLER_SUPPLIED")
    candidates_by_phase[current_phase] = tuple(current_state.candidates)
    rosters = _derived_rosters(phase_specs, candidates_by_phase)
    lineages_by_phase = {
        str(phase_id): _json_object(value, "CAPITAL_REPLAY_LINEAGE_INVALID")
        for phase_id, value in historical_state.capital_lineage_by_phase.items()
    }
    if current_phase in lineages_by_phase:
        _fail("CAPITAL_CURRENT_LINEAGE_CALLER_SUPPLIED")
    try:
        lineages_by_phase[current_phase] = coordinator.derive_current_capital_lineage(
            current_state,
            precommit_body["contract_lineage"],
            margin_terms_sha256=margin_terms_sha256,
            broker_terms_sha256=broker_terms_sha256,
        )
    except coordinator.ProductionAuthorityBlocked as exc:
        raise CapitalAuthorityFactoryError(exc.code) from exc
    lineages = _verified_lineages(
        phase_specs,
        lineages_by_phase,
        precommit_body["contract_lineage"],
    )
    expected_requests = _expected_replay_requests(
        phase_specs, policy.candidate_policies, rosters
    )
    replay_transport = _VerifiedReplayOutputTransport(
        replay_output_loader, expected_requests
    )

    def validate_replay_body(body: Mapping[str, Any]) -> None:
        fields_present = set(body)
        if not _REPLAY_BODY_COMMON_FIELDS.issubset(
            fields_present
        ) or fields_present not in {
            cap._REPLAY_INLINE_KEYS,
            cap._REPLAY_SHARDED_KEYS,
        }:
            _fail("CAPITAL_REPLAY_SIGNING_BODY_SCHEMA_INVALID")
        if (
            body["schema_version"] != cap.CAP_REPLAY_SCHEMA_VERSION
            or body["precommitment_sha256"] != precommit_meta["body_sha256"]
            or body["precommitment_envelope_sha256"]
            != precommit_meta["envelope_sha256"]
            or body["generated_at_utc"] != generated_text
        ):
            _fail("CAPITAL_REPLAY_SIGNING_BODY_BINDING_MISMATCH")

    def validate_authorization_body(body: Mapping[str, Any]) -> None:
        if set(body) != _AUTHORIZATION_BODY_FIELDS:
            _fail("CAPITAL_AUTHORIZATION_SIGNING_BODY_SCHEMA_INVALID")
        if (
            body["schema_version"] != cap.CAP_AUTHORIZATION_SCHEMA_VERSION
            or body["authority_key_id"] != authorization_key.key_id
            or body["issued_at_utc"] != issued_text
            or body["expires_at_utc"] != expires_text
            or body["constraints_sha256"]
            != cap.canonical_json_sha256(asdict(policy.constraints))
        ):
            _fail("CAPITAL_AUTHORIZATION_SIGNING_BODY_BINDING_MISMATCH")
        selected = body["selected_max_total_stop_risk_pct"]
        allowed = {
            item["max_total_stop_risk_pct"] for item in policy.candidate_policies
        }
        if type(selected) is bool or not isinstance(selected, (int, float)):
            _fail("CAPITAL_AUTHORIZATION_SELECTED_CAP_INVALID")
        if not math.isfinite(float(selected)) or float(selected) not in allowed:
            _fail("CAPITAL_AUTHORIZATION_SELECTED_CAP_NOT_PRECOMMITTED")

    replay_signer = _ExactEd25519Signer(
        replay_key,
        cap.REPLAY_EVIDENCE_SIGNATURE_KIND,
        validate_replay_body,
    )
    authorization_signer = _ExactEd25519Signer(
        authorization_key,
        cap.AUTHORIZATION_SIGNATURE_KIND,
        validate_authorization_body,
    )
    capital_config_sha256 = cap.canonical_json_sha256(config_payload)
    bootstrap_identity_sha256 = cap.canonical_json_sha256(
        {
            "schema_version": "fxpm.legacy-capital-authority-input-identity.v1",
            "authorization_scope": authorization_scope,
            "capital_config_sha256": capital_config_sha256,
            "precommitment_sha256": precommit_meta["body_sha256"],
            "precommitment_envelope_sha256": precommit_meta["envelope_sha256"],
        }
    )
    authority_public_key_sha256 = hashlib.sha256(
        authorization_key.public_key_bytes
    ).hexdigest()
    return CapitalAuthorityInputs(
        output_root=resolved_output_root,
        constraints=policy.constraints,
        precommitment_receipt=precommitment,
        trusted_precommitment_public_keys=precommit_keys,
        frozen_candidate_rosters_by_phase=rosters,
        lineage_by_phase=lineages,
        replay_output_loader=replay_transport,
        generated_at_utc=generated_text,
        replay_evidence_signer=replay_signer,
        trusted_replay_public_keys={replay_key.key_id: replay_key.public_key_bytes},
        authority_key_id=authorization_key.key_id,
        authorization_issued_at_utc=issued_text,
        authorization_expires_at_utc=expires_text,
        authorization_signer=authorization_signer,
        trusted_authority_public_keys={
            authorization_key.key_id: authorization_key.public_key_bytes
        },
        authorization_scope=authorization_scope,
        bootstrap_identity_sha256=bootstrap_identity_sha256,
        capital_config_sha256=capital_config_sha256,
        authority_public_key_sha256=authority_public_key_sha256,
        shard_attempt_limit=shard_attempt_limit,
    )


class _LazyCapitalAuthorityFactory:
    __slots__ = ("_kwargs", "_precommitment_sha256")

    def __init__(self, kwargs: Mapping[str, Any]) -> None:
        copied = dict(kwargs)
        environment = copied.get("environment")
        if environment is not None:
            if not isinstance(environment, Mapping):
                _fail("CAPITAL_KEY_ENVIRONMENT_INVALID")
            copied["environment"] = dict(environment)
        copied["config_payload"] = _json_object(
            copied["config_payload"], "CONFIG_INVALID"
        )
        raw = _read_regular_file(
            copied["precommitment_receipt_path"],
            maximum_bytes=MAX_PRECOMMITMENT_BYTES,
            private=False,
            code="CAPITAL_PRECOMMITMENT_RECEIPT",
        )
        self._precommitment_sha256 = hashlib.sha256(raw).hexdigest()
        self._kwargs = copied

    def __repr__(self) -> str:
        return "<lazy-capital-authority-factory precommitment-frozen>"

    def __call__(
        self, request: coordinator.CapitalAuthorityRequest
    ) -> CapitalAuthorityInputs:
        raw = _read_regular_file(
            self._kwargs["precommitment_receipt_path"],
            maximum_bytes=MAX_PRECOMMITMENT_BYTES,
            private=False,
            code="CAPITAL_PRECOMMITMENT_RECEIPT",
        )
        if hashlib.sha256(raw).hexdigest() != self._precommitment_sha256:
            _fail("CAPITAL_PRECOMMITMENT_CHANGED_AFTER_FACTORY_FREEZE")
        return build_capital_authority_inputs(authority_request=request, **self._kwargs)


def build_capital_authority_factory(
    *,
    config_payload: Mapping[str, Any],
    output_root: Path,
    precommitment_receipt_path: Path,
    precommitment_public_key_source: KeyMaterialSource,
    replay_signing_key_source: SigningKeySource,
    authorization_signing_key_source: SigningKeySource,
    margin_terms_sha256: str,
    broker_terms_sha256: str,
    replay_output_loader: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    generated_at_utc: str,
    authorization_issued_at_utc: str,
    authorization_expires_at_utc: str,
    authorization_scope: str = DEMO_PROBATION_SCOPE,
    allow_funded_authorization: bool = False,
    shard_attempt_limit: int = 8,
    environment: Mapping[str, str] | None = None,
) -> Callable[[coordinator.CapitalAuthorityRequest], CapitalAuthorityInputs]:
    """Freeze static precommit/key configuration before any evidence callback."""

    return _LazyCapitalAuthorityFactory(
        {
            "config_payload": config_payload,
            "output_root": output_root,
            "precommitment_receipt_path": precommitment_receipt_path,
            "precommitment_public_key_source": precommitment_public_key_source,
            "replay_signing_key_source": replay_signing_key_source,
            "authorization_signing_key_source": authorization_signing_key_source,
            "margin_terms_sha256": margin_terms_sha256,
            "broker_terms_sha256": broker_terms_sha256,
            "replay_output_loader": replay_output_loader,
            "generated_at_utc": generated_at_utc,
            "authorization_issued_at_utc": authorization_issued_at_utc,
            "authorization_expires_at_utc": authorization_expires_at_utc,
            "authorization_scope": authorization_scope,
            "allow_funded_authorization": allow_funded_authorization,
            "shard_attempt_limit": shard_attempt_limit,
            "environment": environment,
        }
    )


__all__ = [
    "CAPITAL_AUTHORITY_POLICY_SCHEMA",
    "DEMO_PROBATION_SCOPE",
    "FUNDED_LIVE_SCOPE",
    "CapitalAuthorityFactoryError",
    "CapitalAuthorityPolicy",
    "KeyMaterialSource",
    "SigningKeySource",
    "build_capital_authority_factory",
    "build_capital_authority_inputs",
    "canonical_ed25519_key_id",
    "load_capital_authority_policy",
]
