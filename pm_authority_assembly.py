"""Atomic pre-CAS assembly for optimization authority inputs."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import pm_optimization_authority as authority


ASSEMBLY_SCHEMA = "fxpm.optimization-authority-pre-cas-assembly.v1"
MAX_ASSEMBLY_METADATA_BYTES = 4 * 1024 * 1024
_EXPECTED_STATUS = {
    "DEMO_PROBATION": "DEMO_PROBATION_AUTHORIZED",
    "FUNDED_LIVE": "FUNDED_AUTHORIZED",
}


class AuthorityAssemblyBlocked(RuntimeError):
    def __init__(self, result: Mapping[str, Any]):
        self.result = _json_copy(result)
        self.errors = tuple(str(value) for value in result.get("errors", []))
        detail = ",".join(self.errors[:8]) or "RAW_AUTHORITY_NOT_AUTHORIZED"
        super().__init__(f"AUTHORITY_ASSEMBLY_BLOCKED:{detail}")


@dataclass(frozen=True)
class RawAuthorityInputs:
    detector_ledger_records: Sequence[Mapping[str, Any]]
    historical_process_decision: Mapping[str, Any]
    causal_precommit_payload: Mapping[str, Any]
    causal_evidence_payload: Mapping[str, Any]
    causal_reconciliation_payload: Mapping[str, Any]
    historical_book_decision: Mapping[str, Any]
    accepted_book_generation_receipt: Mapping[str, Any]
    current_book_decision: Mapping[str, Any]
    current_refit_receipt: Mapping[str, Any]
    current_refit_raw_manifest_path: Path
    current_refit_artifact_root: Path
    portfolio_replay_evidence: Mapping[str, Any]
    portfolio_expected_lineage: Mapping[str, Any]
    portfolio_expected_policies: Mapping[str, Any]
    portfolio_validation_policy: Mapping[str, Any]
    capital_artifact: Mapping[str, Any]
    capital_expected_lineage: Mapping[str, Any]
    capital_constraints: Mapping[str, Any]
    capital_expected_selected_policy_point_id: Any
    capital_replay_evidence_root: Path
    capital_trusted_authority_public_keys: Mapping[str, bytes]
    capital_trusted_replay_public_keys: Mapping[str, bytes]
    capital_trusted_precommitment_public_keys: Mapping[str, bytes]
    capital_precommitment_receipt: Any = None
    capital_replay_evidence_receipt: Any = None


@dataclass(frozen=True)
class AuthorityAssembly:
    directory: Path
    metadata_path: Path
    index_paths: tuple[tuple[str, Path], ...]
    descriptors_canonical_json: bytes
    chain_receipt_canonical_json: bytes
    verification_result_canonical_json: bytes


def _reject_constant(value: str) -> None:
    raise ValueError(f"JSON_NONFINITE_CONSTANT:{value}")


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise ValueError(f"JSON_DUPLICATE_KEY:{key}")
        result[key] = value
    return result


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _receipt_sha(value: Any) -> str:
    raw = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _json_copy(value: Any) -> Any:
    raw = _canonical_bytes(value)
    if len(raw) > authority.MAX_INDEX_BYTES:
        raise ValueError("AUTHORITY_INPUT_EXCEEDS_INDEX_BOUND")
    return json.loads(raw.decode("ascii"), object_pairs_hook=_pairs)


def _is_reparse(path: Path) -> bool:
    info = path.lstat()
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(attributes & 0x400)


def _safe_existing(root: Path, path: Path, *, directory: bool) -> Path:
    resolved = Path(path).resolve(strict=True)
    resolved.relative_to(root)
    current = root
    if _is_reparse(current):
        raise ValueError("AUTHORITY_ROOT_REPARSE_FORBIDDEN")
    for part in resolved.relative_to(root).parts:
        current = current / part
        if _is_reparse(current):
            raise ValueError("AUTHORITY_PATH_REPARSE_FORBIDDEN")
    mode = resolved.stat().st_mode
    if directory and not stat.S_ISDIR(mode):
        raise ValueError("AUTHORITY_DIRECTORY_REQUIRED")
    if not directory and not stat.S_ISREG(mode):
        raise ValueError("AUTHORITY_REGULAR_FILE_REQUIRED")
    return resolved


def _relative(root: Path, path: Path, *, directory: bool) -> str:
    resolved = _safe_existing(root, path, directory=directory)
    value = resolved.relative_to(root).as_posix()
    if not value or value.startswith("."):
        raise ValueError("AUTHORITY_RELATIVE_PATH_INVALID")
    return value


def _read_json(path: Path, maximum: int) -> tuple[dict[str, Any], bytes]:
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
        raise ValueError("AUTHORITY_JSON_FILE_SIZE_INVALID")
    raw = path.read_bytes()
    after = path.stat()
    if len(raw) != before.st_size or (before.st_size, before.st_mtime_ns) != (
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError("AUTHORITY_JSON_FILE_CHANGED")
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_constant=_reject_constant,
    )
    if type(value) is not dict:
        raise ValueError("AUTHORITY_JSON_ROOT_INVALID")
    _canonical_bytes(value)
    return value, raw


def _public_keys(value: Mapping[str, bytes]) -> dict[str, str]:
    if type(value) is not dict:
        raise ValueError("AUTHORITY_PUBLIC_KEY_ROSTER_INVALID")
    result: dict[str, str] = {}
    for key_id, raw in value.items():
        if type(key_id) is not str or not key_id or type(raw) is not bytes or len(raw) != 32:
            raise ValueError("AUTHORITY_PUBLIC_KEY_INVALID")
        result[key_id] = base64.b64encode(raw).decode("ascii")
    return result


def _build_indexes(
    *,
    deployment_root: Path,
    native_universe: Any,
    raw: RawAuthorityInputs,
) -> dict[str, dict[str, Any]]:
    historical = _json_copy(raw.historical_book_decision)
    current = _json_copy(raw.current_book_decision)
    causal = _json_copy(raw.historical_process_decision)
    raw_manifest_relative = _relative(
        deployment_root, raw.current_refit_raw_manifest_path, directory=False
    )
    artifact_root_relative = _relative(
        deployment_root, raw.current_refit_artifact_root, directory=True
    )
    replay_root_relative = _relative(
        deployment_root, raw.capital_replay_evidence_root, directory=True
    )
    from pm_model_lifecycle import canonical_receipt_sha256

    indexes = {
        "regime_support": {
            "schema_version": authority.INDEX_SCHEMAS["regime_support"],
            "current_book_multiplicity_receipt_sha256": current.get("receipt_sha256"),
            "native_activation_id": getattr(native_universe, "activation_id", None),
            "native_activation_manifest_sha256": getattr(
                native_universe, "activation_manifest_sha256", None
            ),
            "detector_ledger_records": _json_copy(list(raw.detector_ledger_records)),
        },
        "causal_process": {
            "schema_version": authority.INDEX_SCHEMAS["causal_process"],
            "historical_process_decision": causal,
            "precommit_payload": _json_copy(raw.causal_precommit_payload),
            "evidence_payload": _json_copy(raw.causal_evidence_payload),
            "reconciliation_payload": _json_copy(raw.causal_reconciliation_payload),
            "historical_book_receipt_sha256": historical.get("receipt_sha256"),
        },
        "historical_book": {
            "schema_version": authority.INDEX_SCHEMAS["historical_book"],
            "book_decision": historical,
        },
        "current_process_book": {
            "schema_version": authority.INDEX_SCHEMAS["current_process_book"],
            "accepted_book_generation_receipt": _json_copy(
                raw.accepted_book_generation_receipt
            ),
            "current_book_decision": current,
            "historical_process_decision_sha256": canonical_receipt_sha256(causal),
        },
        "current_refit_execution": {
            "schema_version": authority.INDEX_SCHEMAS["current_refit_execution"],
            "current_refit_receipt": _json_copy(raw.current_refit_receipt),
            "raw_manifest_relative_path": raw_manifest_relative,
            "artifact_root_relative_path": artifact_root_relative,
        },
        "synchronized_portfolio_replay": {
            "schema_version": authority.INDEX_SCHEMAS[
                "synchronized_portfolio_replay"
            ],
            "evidence": _json_copy(raw.portfolio_replay_evidence),
            "expected_lineage": _json_copy(raw.portfolio_expected_lineage),
            "expected_policies": _json_copy(raw.portfolio_expected_policies),
            "validation_policy": _json_copy(raw.portfolio_validation_policy),
        },
        "capital_calibration": {
            "schema_version": authority.INDEX_SCHEMAS["capital_calibration"],
            "artifact": _json_copy(raw.capital_artifact),
            "expected_lineage": _json_copy(raw.capital_expected_lineage),
            "constraints": _json_copy(raw.capital_constraints),
            "expected_selected_policy_point_id": _json_copy(
                raw.capital_expected_selected_policy_point_id
            ),
            "replay_evidence_root_relative_path": replay_root_relative,
            "trusted_authority_public_keys": _public_keys(
                raw.capital_trusted_authority_public_keys
            ),
            "trusted_replay_public_keys": _public_keys(
                raw.capital_trusted_replay_public_keys
            ),
            "trusted_precommitment_public_keys": _public_keys(
                raw.capital_trusted_precommitment_public_keys
            ),
            "precommitment_receipt": _json_copy(raw.capital_precommitment_receipt),
            "replay_evidence_receipt": _json_copy(
                raw.capital_replay_evidence_receipt
            ),
        },
    }
    if tuple(indexes) != authority.ARTIFACTS:
        raise RuntimeError("AUTHORITY_INDEX_INTERNAL_ROSTER_INVALID")
    return indexes


def _write_exclusive(path: Path, raw: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _descriptors(
    root: Path, directory: Path, encoded: Mapping[str, bytes]
) -> dict[str, dict[str, Any]]:
    return {
        name: {
            "relative_path": (directory / f"{name}.json").relative_to(root).as_posix(),
            "sha256": hashlib.sha256(encoded[name]).hexdigest(),
            "size_bytes": len(encoded[name]),
            "schema_version": authority.INDEX_SCHEMAS[name],
        }
        for name in authority.ARTIFACTS
    }


def _chain_receipt(
    *,
    generation_id: str,
    authorization_scope: str,
    config_relative_path: str,
    config_sha256: str,
    ledger_sha256: str,
    native_universe: Any,
    requested_symbols: Sequence[str],
    descriptors: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    body = {
        "schema_version": authority.CHAIN_RECEIPT_SCHEMA,
        "generation_id": generation_id,
        "authorization_scope": authorization_scope,
        "config_relative_path": config_relative_path,
        "config_sha256": config_sha256,
        "ledger_sha256": ledger_sha256,
        "native_activation_id": getattr(native_universe, "activation_id", None),
        "native_activation_manifest_sha256": getattr(
            native_universe, "activation_manifest_sha256", None
        ),
        "requested_symbols": list(requested_symbols),
        "authority_artifact_sha256_by_name": {
            name: descriptors[name]["sha256"] for name in authority.ARTIFACTS
        },
    }
    return {**body, "receipt_sha256": _receipt_sha(body)}


def assemble_authority_directory(
    *,
    deployment_root: Path,
    target_name: str,
    generation_id: str,
    authorization_scope: str,
    active_config_path: Path,
    config: Any,
    config_payload: Mapping[str, Any],
    staged_ledger_path: Path,
    ledger: Mapping[str, Any],
    requested_symbols: Sequence[str],
    native_universe: Any,
    validation_time_utc: Any,
    raw: RawAuthorityInputs,
) -> AuthorityAssembly:
    """Publish raw-reverified authority inputs without touching active state."""
    root = Path(deployment_root).resolve(strict=True)
    if _is_reparse(root) or not root.is_dir():
        raise ValueError("AUTHORITY_DEPLOYMENT_ROOT_INVALID")
    if (
        type(target_name) is not str
        or not target_name
        or Path(target_name).name != target_name
        or target_name.startswith(".")
    ):
        raise ValueError("AUTHORITY_TARGET_NAME_INVALID")
    if type(generation_id) is not str or not generation_id.strip():
        raise ValueError("AUTHORITY_GENERATION_ID_INVALID")
    if authorization_scope not in _EXPECTED_STATUS:
        raise ValueError("AUTHORITY_SCOPE_INVALID")
    symbols = tuple(requested_symbols)
    if not symbols:
        raise ValueError("AUTHORITY_SYMBOL_ROSTER_EMPTY")

    config_path = _safe_existing(root, active_config_path, directory=False)
    ledger_path = _safe_existing(root, staged_ledger_path, directory=False)
    observed_config, config_raw = _read_json(config_path, authority.MAX_CONFIG_BYTES)
    observed_ledger, ledger_raw = _read_json(ledger_path, authority.MAX_LEDGER_BYTES)
    if observed_config != _json_copy(config_payload):
        raise ValueError("AUTHORITY_CONFIG_CALLER_SNAPSHOT_MISMATCH")
    if observed_ledger != _json_copy(ledger):
        raise ValueError("AUTHORITY_LEDGER_CALLER_SNAPSHOT_MISMATCH")

    indexes = _build_indexes(
        deployment_root=root,
        native_universe=native_universe,
        raw=raw,
    )
    encoded = {name: _canonical_bytes(indexes[name]) for name in authority.ARTIFACTS}
    if any(not value or len(value) > authority.MAX_INDEX_BYTES for value in encoded.values()):
        raise ValueError("AUTHORITY_INDEX_SIZE_INVALID")
    if sum(map(len, encoded.values())) > authority.MAX_TOTAL_INDEX_BYTES:
        raise ValueError("AUTHORITY_INDEX_TOTAL_SIZE_INVALID")

    target = root / target_name
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"AUTHORITY_TARGET_EXISTS:{target_name}")
    token = uuid.uuid4().hex
    stage = root / f".{target_name}.stage-{token}"
    probe_path = root / f".{target_name}.probe-{token}.json"
    stage.mkdir(mode=0o700)
    try:
        for name in authority.ARTIFACTS:
            _write_exclusive(stage / f"{name}.json", encoded[name])
        probe_descriptors = _descriptors(root, stage, encoded)
        final_descriptors = _descriptors(root, target, encoded)
        receipt = _chain_receipt(
            generation_id=generation_id,
            authorization_scope=authorization_scope,
            config_relative_path=config_path.relative_to(root).as_posix(),
            config_sha256=hashlib.sha256(config_raw).hexdigest(),
            ledger_sha256=hashlib.sha256(ledger_raw).hexdigest(),
            native_universe=native_universe,
            requested_symbols=symbols,
            descriptors=probe_descriptors,
        )
        metadata = {
            "schema_version": ASSEMBLY_SCHEMA,
            "state": "RAW_REVERIFIED_PENDING_FINAL_CAS",
            "generation_id": generation_id,
            "authorization_scope": authorization_scope,
            "authority_artifacts": final_descriptors,
            "authority_chain_receipt": receipt,
        }
        metadata_raw = _canonical_bytes(metadata)
        if len(metadata_raw) > MAX_ASSEMBLY_METADATA_BYTES:
            raise ValueError("AUTHORITY_ASSEMBLY_METADATA_TOO_LARGE")
        _write_exclusive(stage / "authority_assembly.json", metadata_raw)

        manifest = {
            "schema_version": authority.GENERATION_SCHEMA,
            "generation_id": generation_id,
            "generation_status": "PROMOTED",
            "ledger_sha256": hashlib.sha256(ledger_raw).hexdigest(),
            "technical_failures": {},
            "authority_chain_receipt": receipt,
            "authority_artifacts": probe_descriptors,
        }
        _write_exclusive(probe_path, _canonical_bytes(manifest))
        result = authority.verify_live_readiness_authority_chain(
            manifest=manifest,
            manifest_path=probe_path,
            authority_index_paths={
                name: stage / f"{name}.json" for name in authority.ARTIFACTS
            },
            ledger=observed_ledger,
            ledger_path=ledger_path,
            config=config,
            config_payload=observed_config,
            config_path=config_path,
            requested_symbols=symbols,
            native_universe=native_universe,
            validation_time_utc=validation_time_utc,
            require_funded_authorization=authorization_scope == "FUNDED_LIVE",
        )
        if (
            result.get("status") != _EXPECTED_STATUS[authorization_scope]
            or result.get("errors") != []
            or set(result.get("raw_reverification", {}).values()) != {True}
        ):
            raise AuthorityAssemblyBlocked(result)
        for name, expected in encoded.items():
            if (stage / f"{name}.json").read_bytes() != expected:
                raise RuntimeError("AUTHORITY_INDEX_CHANGED_AFTER_VERIFICATION")
        if target.exists() or target.is_symlink():
            raise FileExistsError(f"AUTHORITY_TARGET_EXISTS:{target_name}")
        os.rename(stage, target)
        return AuthorityAssembly(
            directory=target,
            metadata_path=target / "authority_assembly.json",
            index_paths=tuple(
                (name, target / f"{name}.json") for name in authority.ARTIFACTS
            ),
            descriptors_canonical_json=_canonical_bytes(final_descriptors),
            chain_receipt_canonical_json=_canonical_bytes(receipt),
            verification_result_canonical_json=_canonical_bytes(result),
        )
    finally:
        try:
            probe_path.unlink(missing_ok=True)
        finally:
            if stage.exists():
                shutil.rmtree(stage)


__all__ = [
    "ASSEMBLY_SCHEMA",
    "AuthorityAssembly",
    "AuthorityAssemblyBlocked",
    "RawAuthorityInputs",
    "assemble_authority_directory",
]
