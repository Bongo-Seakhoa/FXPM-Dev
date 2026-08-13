"""Restart-safe authority for managing existing DEMO positions."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import shutil
import stat
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence


CAPSULE_SCHEMA = "fxpm.demo-management-authority.v1"
REVERIFY_SCHEMA = "fxpm.demo-management-reverification.v1"
MANAGEMENT_CONTEXT_SCHEMA = "fxpm.demo-management-only-context.v1"
POSITION_MANAGEMENT_CONTEXT_SCHEMA = "fxpm.demo-position-management-context.v1"
EXTERNAL_PINS_SCHEMA = "fxpm.demo-management-external-pins.v1"
RETENTION_GUARD_SCHEMA = "fxpm.demo-management-retention-guard.v1"
MANAGEMENT_ONLY = "MANAGEMENT_ONLY"
BLOCKED = "BLOCKED"
CAPSULE_WRITTEN = "CAPSULE_WRITTEN"
CAPSULE_VALID = "CAPSULE_VALID"
RETENTION_AUTHORIZED = "RETENTION_AUTHORIZED"

AUTHORITY_ARTIFACTS = (
    "regime_support",
    "causal_process",
    "historical_book",
    "current_process_book",
    "current_refit_execution",
    "synchronized_portfolio_replay",
    "capital_calibration",
)
RAW_COMPONENTS = ("native_data_state", *AUTHORITY_ARTIFACTS)
SOURCE_ROLES = (
    "config",
    "ledger",
    "generation_manifest",
    *AUTHORITY_ARTIFACTS,
    "current_refit_raw_manifest",
    "native_activation_manifest",
)
DERIVED_ROLES = (
    "authority_result",
    "execution_projection",
    "detector_projection",
    "execution_policies",
)
MANAGEMENT_PERMISSIONS = {
    "open_new_positions": False,
    "increase_positions": False,
    "widen_stops": False,
    "tighten_stops": True,
    "reduce_positions": True,
    "close_positions": True,
}
NO_MANAGEMENT_PERMISSIONS = {name: False for name in MANAGEMENT_PERMISSIONS}

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ROLE = re.compile(r"^[a-z0-9_.:-]{1,128}$")
_DESCRIPTOR_FIELDS = {
    "role",
    "source_relative_path",
    "capsule_relative_path",
    "size_bytes",
    "sha256",
}
_EXTERNAL_DESCRIPTOR_FIELDS = {
    "role",
    "logical_relative_path",
    "size_bytes",
    "sha256",
}
_EXTERNAL_ROLES = {
    "current_refit_artifact",
    "capital_replay_artifact",
    "native_bars",
    "native_sidecar",
}
_NATIVE_BINDING_FIELDS = {
    "activation_id",
    "activation_manifest_sha256",
    "bars_logical_relative_path",
    "bars_size_bytes",
    "bars_sha256",
    "sidecar_logical_relative_path",
    "sidecar_size_bytes",
    "sidecar_sha256",
}
_ACCOUNT_FIELDS = {
    "login",
    "server",
    "currency",
    "trade_mode",
    "margin_mode",
    "trade_allowed",
    "trade_expert",
    "hedge_allowed",
}
_CELL_AUTHORITY_FIELDS = {
    "cell_id",
    "candidate_id",
    "strategy_id",
    "config_sha256",
    "historical_process_decision_sha256",
    "accepted_book_generation_receipt_sha256",
    "parameter_sha256",
    "output_artifact_sha256",
    "winner_decision_sha256",
    "winner_book_entry_sha256",
    "source_snapshot_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
}
_CHAIN_FIELDS = {
    "schema_version",
    "generation_id",
    "authorization_scope",
    "config_relative_path",
    "config_sha256",
    "ledger_sha256",
    "native_activation_id",
    "native_activation_manifest_sha256",
    "requested_symbols",
    "authority_artifact_sha256_by_name",
    "receipt_sha256",
}
_AUTHORITY_RESULT_FIELDS = {
    "schema_version",
    "status",
    "errors",
    "raw_reverification",
    "selected_cell_ids",
    "possible_slot_count",
    "source_unavailable_cell_ids",
    "regime_unobserved_cell_ids",
    "no_local_winner_cell_ids",
    "insufficient_evidence_cell_ids",
    "technical_cell_ids",
    "unsearched_cell_ids",
    "missing_observed_cell_ids",
    "native_activation_id",
    "native_activation_manifest_sha256",
    "ledger_sha256",
    "selected_capital_envelope",
    "capital_config_matches",
    "capital_execution_enforcement_reverified",
    "funded_live_authorized",
}
_CAPSULE_FIELDS = {
    "schema_version",
    "capsule_id",
    "generation_id",
    "source_root",
    "created_at_utc",
    "authorized_at_utc",
    "valid_until_utc",
    "valid_until_by_symbol",
    "account",
    "requested_symbols",
    "selected_cell_ids",
    "cell_authority",
    "cell_authority_sha256",
    "authority_result_sha256",
    "authority_chain_receipt_sha256",
    "detector_projection_sha256",
    "execution_projection_sha256",
    "capital_policy_sha256",
    "position_policy_sha256",
    "native_activation_id",
    "native_activation_manifest_sha256",
    "selected_native_source_bindings",
    "selected_native_source_bindings_sha256",
    "source_snapshots",
    "derived_artifacts",
    "external_closure",
    "external_closure_sha256",
    "permissions",
}


@dataclass(frozen=True)
class ManagementLimits:
    max_capsules: int = 8
    max_store_bytes: int = 4 * 1024**3
    max_capsule_bytes: int = 800 * 1024**2
    max_metadata_bytes: int = 64 * 1024**2
    max_external_references: int = 20_000
    max_external_reference_bytes: int = 2 * 1024**4
    max_external_file_bytes: int = 16 * 1024**3
    max_store_files: int = 256
    max_prune_references: int = 4096
    max_pin_records: int = 160_000
    max_retention_candidates: int = 4096


class _Blocked(ValueError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _is_sha(value: Any) -> bool:
    return type(value) is str and _HEX64.fullmatch(value) is not None


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            raise _Blocked("JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise _Blocked(f"JSON_NONFINITE:{value}")


def _json_bytes(raw: bytes, maximum_bytes: int) -> Any:
    if not 0 < len(raw) <= maximum_bytes:
        raise _Blocked("JSON_SIZE_INVALID")
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=_constant,
        )
    except _Blocked:
        raise
    except Exception as exc:
        raise _Blocked("JSON_INVALID") from exc


def _utc(value: Any, code: str) -> datetime:
    try:
        parsed = (
            value
            if type(value) is datetime
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except Exception as exc:
        raise _Blocked(code) from exc


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _account_value(account: Any, name: str) -> Any:
    if isinstance(account, Mapping):
        return account.get(name)
    return getattr(account, name, None)


def _account_identity(account: Any) -> dict[str, Any]:
    try:
        raw_login = _account_value(account, "login")
        raw_trade_mode = _account_value(account, "trade_mode")
        raw_margin_mode = _account_value(account, "margin_mode")
        if any(
            type(value) is not int
            for value in (raw_login, raw_trade_mode, raw_margin_mode)
        ):
            raise TypeError
        login = raw_login
        trade_mode = raw_trade_mode
        margin_mode = raw_margin_mode
    except (TypeError, ValueError, OverflowError) as exc:
        raise _Blocked("MANAGEMENT_ACCOUNT_IDENTITY_INVALID") from exc
    server = _account_value(account, "server")
    currency = _account_value(account, "currency")
    identity = {
        "login": login,
        "server": server,
        "currency": currency,
        "trade_mode": trade_mode,
        "margin_mode": margin_mode,
        "trade_allowed": _account_value(account, "trade_allowed"),
        "trade_expert": _account_value(account, "trade_expert"),
        "hedge_allowed": _account_value(account, "hedge_allowed"),
    }
    if (
        login <= 0
        or type(server) is not str
        or not server
        or server != server.strip()
        or any(ord(character) < 32 for character in server)
        or len(server.encode("utf-8")) > 256
        or type(currency) is not str
        or not currency
        or currency != currency.strip()
        or any(ord(character) < 32 for character in currency)
        or len(currency.encode("utf-8")) > 32
        or trade_mode != 0
        or margin_mode != 2
        or identity["trade_allowed"] is not True
        or identity["trade_expert"] is not True
        or identity["hedge_allowed"] is not True
    ):
        raise _Blocked("MANAGEMENT_ACCOUNT_NOT_EXACT_DEMO_HEDGING")
    return identity


def _uses_reparse(root: Path, path: Path) -> bool:
    try:
        current = root
        root_stat = current.lstat()
        if (
            current.is_symlink()
            or int(getattr(root_stat, "st_file_attributes", 0)) & 0x400
        ):
            return True
        for part in path.relative_to(root).parts:
            current = current / part
            info = current.lstat()
            if (
                current.is_symlink()
                or int(getattr(info, "st_file_attributes", 0)) & 0x400
            ):
                return True
        return False
    except Exception:
        return True


def _regular_path(root: Path, value: Any, code: str) -> Path:
    try:
        lexical = Path(os.path.abspath(os.fspath(value)))
        lexical.relative_to(root)
        if _uses_reparse(root, lexical):
            raise _Blocked(code)
        path = lexical.resolve(strict=True)
        path.relative_to(root)
        info = lexical.stat()
    except Exception as exc:
        raise _Blocked(code) from exc
    if not stat.S_ISREG(info.st_mode):
        raise _Blocked(code)
    return path


def _regular_directory(root: Path, value: Any, code: str) -> Path:
    try:
        lexical = Path(os.path.abspath(os.fspath(value)))
        lexical.relative_to(root)
        if _uses_reparse(root, lexical):
            raise _Blocked(code)
        path = lexical.resolve(strict=True)
        path.relative_to(root)
        info = lexical.stat()
    except Exception as exc:
        raise _Blocked(code) from exc
    if not stat.S_ISDIR(info.st_mode):
        raise _Blocked(code)
    return path


def _file_digest(path: Path, maximum_bytes: int, code: str) -> tuple[int, str]:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_size <= 0
                or before.st_size > maximum_bytes
            ):
                raise _Blocked(code)
            remaining = before.st_size
            while remaining:
                chunk = handle.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise _Blocked(code)
                digest.update(chunk)
                remaining -= len(chunk)
            if handle.read(1):
                raise _Blocked(code)
            after = os.fstat(handle.fileno())
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if identity_before != identity_after:
            raise _Blocked(code)
        return before.st_size, digest.hexdigest()
    except _Blocked:
        raise
    except Exception as exc:
        raise _Blocked(code) from exc


def _source_descriptor(
    root: Path,
    path: Path,
    role: str,
    maximum_bytes: int,
) -> dict[str, Any]:
    if _SAFE_ROLE.fullmatch(role) is None:
        raise _Blocked("MANAGEMENT_SOURCE_ROLE_INVALID")
    resolved = _regular_path(root, path, "MANAGEMENT_SOURCE_PATH_INVALID")
    size, digest = _file_digest(
        resolved, maximum_bytes, "MANAGEMENT_SOURCE_FILE_INVALID"
    )
    relative = resolved.relative_to(root).as_posix()
    return {
        "role": role,
        "source_relative_path": relative,
        "capsule_relative_path": f"closure/{relative}",
        "size_bytes": size,
        "sha256": digest,
    }


def _read_json_file(path: Path, maximum_bytes: int) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_size <= 0
                or before.st_size > maximum_bytes
            ):
                raise _Blocked("MANAGEMENT_JSON_FILE_INVALID")
            raw = handle.read(before.st_size)
            if len(raw) != before.st_size or handle.read(1):
                raise _Blocked("MANAGEMENT_JSON_FILE_INVALID")
            after = os.fstat(handle.fileno())
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise _Blocked("MANAGEMENT_JSON_FILE_CHANGED")
    except _Blocked:
        raise
    except Exception as exc:
        raise _Blocked("MANAGEMENT_JSON_FILE_INVALID") from exc
    value = _json_bytes(raw, maximum_bytes)
    if type(value) is not dict:
        raise _Blocked("MANAGEMENT_JSON_OBJECT_REQUIRED")
    return value


def _write_exact(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _copy_exact(source: Path, destination: Path, descriptor: Mapping[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    expected_size = descriptor["size_bytes"]
    expected_sha = descriptor["sha256"]
    digest = hashlib.sha256()
    copied = 0
    with source.open("rb") as reader, destination.open("xb") as writer:
        before = os.fstat(reader.fileno())
        while copied < expected_size:
            chunk = reader.read(min(1024 * 1024, expected_size - copied))
            if not chunk:
                raise _Blocked("MANAGEMENT_SOURCE_CHANGED_DURING_COPY")
            writer.write(chunk)
            digest.update(chunk)
            copied += len(chunk)
        if reader.read(1):
            raise _Blocked("MANAGEMENT_SOURCE_CHANGED_DURING_COPY")
        after = os.fstat(reader.fileno())
        writer.flush()
        os.fsync(writer.fileno())
    if (
        copied != expected_size
        or digest.hexdigest() != expected_sha
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise _Blocked("MANAGEMENT_SOURCE_CHANGED_DURING_COPY")


@contextmanager
def _store_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".management.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
        handle = os.fdopen(descriptor, "r+b")
        path_info = lock_path.lstat()
        file_info = os.fstat(handle.fileno())
        if (
            not stat.S_ISREG(path_info.st_mode)
            or lock_path.is_symlink()
            or int(getattr(path_info, "st_file_attributes", 0)) & 0x400
            or (path_info.st_dev, path_info.st_ino)
            != (file_info.st_dev, file_info.st_ino)
            or file_info.st_size > 1
        ):
            raise _Blocked("MANAGEMENT_STORE_LOCK_INVALID")
    except Exception:
        if "handle" in locals():
            handle.close()
        raise
    locked = False
    try:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = True
        yield
    except OSError as exc:
        raise _Blocked("MANAGEMENT_STORE_LOCKED") from exc
    finally:
        if locked:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        handle.close()


def _safe_relative(value: Any, code: str) -> Path:
    if type(value) is not str or not value:
        raise _Blocked(code)
    path = Path(value)
    if path.is_absolute() or path.drive or path.root or ".." in path.parts:
        raise _Blocked(code)
    return path


def _store_root(value: Any, *, create: bool) -> Path:
    try:
        lexical = Path(os.path.abspath(os.fspath(value)))
        if create:
            lexical.mkdir(parents=True, exist_ok=True)
        info = lexical.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or lexical.is_symlink()
            or int(getattr(info, "st_file_attributes", 0)) & 0x400
        ):
            raise _Blocked("MANAGEMENT_STORE_REPARSE_FORBIDDEN")
        return lexical.resolve(strict=True)
    except _Blocked:
        raise
    except Exception as exc:
        raise _Blocked("MANAGEMENT_STORE_INVALID") from exc


def _descriptor_target(capsule_dir: Path, descriptor: Mapping[str, Any]) -> Path:
    relative = _safe_relative(
        descriptor.get("capsule_relative_path"),
        "MANAGEMENT_CAPSULE_PATH_TRAVERSAL",
    )
    if relative.as_posix() == ".":
        raise _Blocked("MANAGEMENT_CAPSULE_PATH_TRAVERSAL")
    try:
        lexical = Path(os.path.abspath(capsule_dir / relative))
        lexical.relative_to(capsule_dir)
        if _uses_reparse(capsule_dir, lexical):
            raise _Blocked("MANAGEMENT_CAPSULE_REPARSE_FORBIDDEN")
        target = lexical.resolve(strict=True)
        target.relative_to(capsule_dir)
    except Exception as exc:
        raise _Blocked("MANAGEMENT_CAPSULE_PATH_INVALID") from exc
    return target


def _binding_value(binding: Any, name: str) -> Any:
    return getattr(binding, name, None)


def _successful_authority(
    authority: Any,
    binding: Any,
) -> dict[str, Any]:
    if type(authority) is not dict:
        raise _Blocked("MANAGEMENT_AUTHORITY_RESULT_INVALID")
    raw = authority.get("raw_reverification")
    selected = authority.get("selected_cell_ids")
    if (
        set(authority) != _AUTHORITY_RESULT_FIELDS
        or authority.get("schema_version") != "fxpm.live-readiness-authority-result.v2"
        or authority.get("status") != "DEMO_PROBATION_AUTHORIZED"
        or authority.get("errors") != []
        or type(raw) is not dict
        or set(raw) != set(RAW_COMPONENTS)
        or any(raw[name] is not True for name in RAW_COMPONENTS)
        or authority.get("funded_live_authorized") is not False
        or authority.get("capital_config_matches") is not True
        or authority.get("capital_execution_enforcement_reverified") is not True
        or type(authority.get("selected_capital_envelope")) is not dict
        or type(selected) is not list
        or selected != sorted(set(selected))
        or any(type(value) is not str or value.count("|") != 2 for value in selected)
    ):
        raise _Blocked("MANAGEMENT_AUTHORITY_NOT_SUCCESSFUL_DEMO")
    if (
        _sha(authority) != _binding_value(binding, "authority_result_sha256")
        or authority.get("ledger_sha256") != _binding_value(binding, "ledger_sha256")
        or tuple(selected) != tuple(_binding_value(binding, "selected_cell_ids") or ())
        or authority.get("native_activation_id")
        != _binding_value(binding, "native_activation_id")
        or authority.get("native_activation_manifest_sha256")
        != _binding_value(binding, "native_activation_manifest_sha256")
    ):
        raise _Blocked("MANAGEMENT_AUTHORITY_BINDING_MISMATCH")
    return json.loads(_canonical_bytes(authority).decode("utf-8"))


def _projection_cell(
    projection: Mapping[str, Any], cell_id: str
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    symbol, timeframe, regime = cell_id.split("|")
    symbol_payload = projection.get(symbol)
    grid = (
        symbol_payload.get("regime_configs")
        if isinstance(symbol_payload, Mapping)
        else None
    )
    timeframe_payload = grid.get(timeframe) if isinstance(grid, Mapping) else None
    cell = (
        timeframe_payload.get(regime)
        if isinstance(timeframe_payload, Mapping)
        else None
    )
    if not isinstance(symbol_payload, Mapping) or not isinstance(cell, Mapping):
        raise _Blocked("MANAGEMENT_SELECTED_CELL_CONFIG_MISSING")
    return symbol_payload, cell


def _cell_authority(
    projection: Mapping[str, Any], selected_cell_ids: Sequence[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidates: set[str] = set()
    for cell_id in selected_cell_ids:
        _symbol_payload, cell = _projection_cell(projection, cell_id)
        artifact = cell.get("artifact_meta")
        binding = (
            artifact.get("current_refit_binding")
            if isinstance(artifact, Mapping)
            else None
        )
        strategy = cell.get("strategy_name")
        if (
            type(binding) is not dict
            or type(strategy) is not str
            or not strategy
            or str(cell.get("promotion_status") or "").upper()
            != "PROCESS_VALIDATED_CURRENT_REFIT"
        ):
            raise _Blocked("MANAGEMENT_SELECTED_CELL_CONFIG_INVALID")
        names = {
            "candidate_id": "current_refit_cell_id",
            "historical_process_decision_sha256": (
                "historical_process_decision_sha256"
            ),
            "accepted_book_generation_receipt_sha256": (
                "accepted_book_generation_receipt_sha256"
            ),
            "parameter_sha256": "parameter_sha256",
            "output_artifact_sha256": "output_artifact_sha256",
            "winner_decision_sha256": "winner_decision_sha256",
            "winner_book_entry_sha256": "winner_book_entry_sha256",
            "source_snapshot_sha256": "source_snapshot_sha256",
            "regime_fit_receipt_sha256": "regime_fit_receipt_sha256",
            "regime_detector_record_sha256": "regime_detector_record_sha256",
            "regime_detector_parameters_sha256": ("regime_detector_parameters_sha256"),
            "regime_detector_code_sha256": "regime_detector_code_sha256",
        }
        if any(not _is_sha(binding.get(source)) for source in names.values()):
            raise _Blocked("MANAGEMENT_SELECTED_CELL_FINGERPRINT_INVALID")
        candidate = binding["current_refit_cell_id"]
        if candidate in candidates:
            raise _Blocked("MANAGEMENT_SELECTED_CANDIDATE_DUPLICATE")
        candidates.add(candidate)
        row = {
            "cell_id": cell_id,
            "strategy_id": strategy,
            "config_sha256": _sha(cell),
            **{target: binding[source] for target, source in names.items()},
        }
        if set(row) != _CELL_AUTHORITY_FIELDS:
            raise _Blocked("MANAGEMENT_CELL_AUTHORITY_INTERNAL_INVALID")
        rows.append(row)
    return rows


def _validity(
    projection: Mapping[str, Any],
    selected_cell_ids: Sequence[str],
    authorized_at: datetime,
    created_at: datetime,
) -> tuple[str, dict[str, str]]:
    expiries: dict[str, datetime] = {}
    for cell_id in selected_cell_ids:
        symbol = cell_id.split("|", 1)[0]
        symbol_payload, _cell = _projection_cell(projection, cell_id)
        expiry = _utc(
            symbol_payload.get("valid_until"),
            "MANAGEMENT_VALID_UNTIL_INVALID",
        )
        previous = expiries.setdefault(symbol, expiry)
        if previous != expiry:
            raise _Blocked("MANAGEMENT_SYMBOL_VALIDITY_CONFLICT")
    if (
        authorized_at < created_at - timedelta(minutes=15)
        or authorized_at > created_at + timedelta(minutes=5)
        or not expiries
        or min(expiries.values()) <= created_at
    ):
        raise _Blocked("MANAGEMENT_AUTHORIZATION_CHRONOLOGY_INVALID")
    return _iso(min(expiries.values())), {
        symbol: _iso(expiry) for symbol, expiry in sorted(expiries.items())
    }


def _manifest_indexes(
    source_root: Path,
    manifest: Mapping[str, Any],
) -> dict[str, Path]:
    artifacts = manifest.get("authority_artifacts")
    if type(artifacts) is not dict or set(artifacts) != set(AUTHORITY_ARTIFACTS):
        raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_ROSTER_INVALID")
    paths: dict[str, Path] = {}
    aggregate_size = 0
    for name in AUTHORITY_ARTIFACTS:
        descriptor = artifacts[name]
        if type(descriptor) is not dict or set(descriptor) != {
            "relative_path",
            "sha256",
            "size_bytes",
            "schema_version",
        }:
            raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_DESCRIPTOR_INVALID")
        relative = _safe_relative(
            descriptor["relative_path"], "MANAGEMENT_AUTHORITY_INDEX_PATH_INVALID"
        )
        path = _regular_path(
            source_root,
            source_root / relative,
            "MANAGEMENT_AUTHORITY_INDEX_PATH_INVALID",
        )
        size, digest = _file_digest(
            path, 64 * 1024**2, "MANAGEMENT_AUTHORITY_INDEX_FILE_INVALID"
        )
        if (
            type(descriptor["size_bytes"]) is not int
            or descriptor["size_bytes"] != size
            or descriptor["sha256"] != digest
            or descriptor["schema_version"]
            != f"fxpm.{name.replace('_', '-')}-authority-index.v1"
        ):
            raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_BINDING_INVALID")
        aggregate_size += size
        if aggregate_size > 256 * 1024**2:
            raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_AGGREGATE_BOUND_EXCEEDED")
        paths[name] = path
    return paths


def _chain_receipt(
    manifest: Mapping[str, Any],
    *,
    generation_id: str,
    ledger_sha256: str,
    config_sha256: str,
    requested_symbols: Sequence[str],
    native_activation_id: str,
    native_activation_sha256: str,
) -> dict[str, Any]:
    receipt = manifest.get("authority_chain_receipt")
    if type(receipt) is not dict or set(receipt) != _CHAIN_FIELDS:
        raise _Blocked("MANAGEMENT_CHAIN_RECEIPT_INVALID")
    body = dict(receipt)
    receipt_sha = body.pop("receipt_sha256", None)
    if (
        receipt.get("schema_version") != "fxpm.optimization-authority-chain-receipt.v1"
        or not _is_sha(receipt_sha)
        or _sha(body) != receipt_sha
        or receipt.get("generation_id") != generation_id
        or receipt.get("authorization_scope") != "DEMO_PROBATION"
        or receipt.get("ledger_sha256") != ledger_sha256
        or receipt.get("config_sha256") != config_sha256
        or receipt.get("requested_symbols") != list(requested_symbols)
        or receipt.get("native_activation_id") != native_activation_id
        or receipt.get("native_activation_manifest_sha256") != native_activation_sha256
    ):
        raise _Blocked("MANAGEMENT_CHAIN_RECEIPT_BINDING_INVALID")
    return json.loads(_canonical_bytes(receipt).decode("utf-8"))


def _logical_join(*parts: Any) -> str:
    path = Path()
    for part in parts:
        path /= _safe_relative(part, "MANAGEMENT_CLOSURE_PATH_INVALID")
    normalized = _safe_relative(
        path.as_posix(), "MANAGEMENT_CLOSURE_PATH_INVALID"
    ).as_posix()
    if normalized in {"", "."}:
        raise _Blocked("MANAGEMENT_CLOSURE_PATH_INVALID")
    return normalized


def _current_refit_specs(
    index: Mapping[str, Any],
    raw_manifest: Mapping[str, Any],
    limits: ManagementLimits,
) -> tuple[str, str, list[dict[str, Any]]]:
    if (
        type(index) is not dict
        or set(index)
        != {
            "schema_version",
            "current_refit_receipt",
            "raw_manifest_relative_path",
            "artifact_root_relative_path",
        }
        or index.get("schema_version")
        != "fxpm.current-refit-execution-authority-index.v1"
    ):
        raise _Blocked("MANAGEMENT_CURRENT_REFIT_INDEX_INVALID")
    raw_relative = _safe_relative(
        index["raw_manifest_relative_path"],
        "MANAGEMENT_CURRENT_REFIT_MANIFEST_PATH_INVALID",
    ).as_posix()
    artifact_root = _safe_relative(
        index["artifact_root_relative_path"],
        "MANAGEMENT_CURRENT_REFIT_ARTIFACT_ROOT_INVALID",
    ).as_posix()
    required = {
        "schema_version",
        "executor_contract_version",
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
        "current_cutoff_utc",
        "bundle_artifact_ids",
        "artifact_descriptors",
        "cell_requests",
        "manifest_sha256",
    }
    descriptors = raw_manifest.get("artifact_descriptors")
    if (
        type(raw_manifest) is not dict
        or set(raw_manifest) != required
        or raw_manifest.get("schema_version") != "fxpm.raw-current-refit-manifest.v2"
        or type(descriptors) is not list
        or not 1 <= len(descriptors) <= limits.max_external_references
    ):
        raise _Blocked("MANAGEMENT_CURRENT_REFIT_RAW_MANIFEST_INVALID")
    body = dict(raw_manifest)
    manifest_sha = body.pop("manifest_sha256", None)
    if not _is_sha(manifest_sha) or _sha(body) != manifest_sha:
        raise _Blocked("MANAGEMENT_CURRENT_REFIT_RAW_MANIFEST_HASH_INVALID")
    exact = {
        "artifact_id",
        "relative_path",
        "size_bytes",
        "content_sha256",
        "semantic_sha256",
        "format_version",
    }
    specs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    total = 0
    for value in descriptors:
        if type(value) is not dict or set(value) != exact:
            raise _Blocked("MANAGEMENT_CURRENT_REFIT_ARTIFACT_DESCRIPTOR_INVALID")
        artifact_id = value["artifact_id"]
        descriptor_body = dict(value)
        descriptor_body.pop("artifact_id")
        size = value["size_bytes"]
        relative = _safe_relative(
            value["relative_path"],
            "MANAGEMENT_CURRENT_REFIT_ARTIFACT_PATH_INVALID",
        ).as_posix()
        if (
            not _is_sha(artifact_id)
            or _sha(descriptor_body) != artifact_id
            or artifact_id in seen_ids
            or relative in seen_paths
            or type(size) is not int
            or not 0 < size <= limits.max_external_file_bytes
            or not _is_sha(value["content_sha256"])
            or not _is_sha(value["semantic_sha256"])
            or type(value["format_version"]) is not str
            or not value["format_version"]
        ):
            raise _Blocked("MANAGEMENT_CURRENT_REFIT_ARTIFACT_DESCRIPTOR_INVALID")
        seen_ids.add(artifact_id)
        seen_paths.add(relative)
        total += size
        if total > limits.max_external_reference_bytes:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BOUND_EXCEEDED")
        specs.append(
            {
                "role": "current_refit_artifact",
                "logical_relative_path": _logical_join(artifact_root, relative),
                "size_bytes": size,
                "sha256": value["content_sha256"],
            }
        )
    return raw_relative, artifact_root, specs


def _capital_replay_specs(
    index: Mapping[str, Any], limits: ManagementLimits
) -> tuple[str, list[dict[str, Any]]]:
    if (
        type(index) is not dict
        or index.get("schema_version") != "fxpm.capital-calibration-authority-index.v1"
    ):
        raise _Blocked("MANAGEMENT_CAPITAL_INDEX_INVALID")
    replay_root = _safe_relative(
        index.get("replay_evidence_root_relative_path"),
        "MANAGEMENT_CAPITAL_REPLAY_ROOT_INVALID",
    ).as_posix()
    envelope = index.get("replay_evidence_receipt")
    body = envelope.get("body") if type(envelope) is dict else None
    if type(body) is not dict:
        raise _Blocked("MANAGEMENT_CAPITAL_REPLAY_RECEIPT_INVALID")
    if body.get("storage_mode") != "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1":
        if "attempt_shards" in body:
            raise _Blocked("MANAGEMENT_CAPITAL_REPLAY_STORAGE_MODE_INVALID")
        return replay_root, []
    descriptors = body.get("attempt_shards")
    if type(descriptors) is not list or not 1 <= len(descriptors) <= min(
        512, limits.max_external_references
    ):
        raise _Blocked("MANAGEMENT_CAPITAL_REPLAY_SHARD_ROSTER_INVALID")
    exact = {
        "shard_index",
        "relative_path",
        "file_sha256",
        "byte_size",
        "attempt_count",
        "daily_observation_count",
        "first_attempt_id",
        "last_attempt_id",
    }
    specs: list[dict[str, Any]] = []
    for index_value, value in enumerate(descriptors):
        if type(value) is not dict or set(value) != exact:
            raise _Blocked("MANAGEMENT_CAPITAL_REPLAY_SHARD_DESCRIPTOR_INVALID")
        digest = value["file_sha256"]
        relative = _safe_relative(
            value["relative_path"],
            "MANAGEMENT_CAPITAL_REPLAY_SHARD_PATH_INVALID",
        ).as_posix()
        size = value["byte_size"]
        if (
            value["shard_index"] != index_value
            or not _is_sha(digest)
            or relative != f"attempt_shards/attempts_{index_value:04d}_{digest}.json"
            or type(size) is not int
            or not 0 < size <= min(8 * 1024**2, limits.max_external_file_bytes)
        ):
            raise _Blocked("MANAGEMENT_CAPITAL_REPLAY_SHARD_DESCRIPTOR_INVALID")
        specs.append(
            {
                "role": "capital_replay_artifact",
                "logical_relative_path": _logical_join(replay_root, relative),
                "size_bytes": size,
                "sha256": digest,
            }
        )
    return replay_root, specs


def _native_specs(
    *,
    source_root: Path,
    config_path: Path,
    config_payload: Mapping[str, Any],
    activation_manifest: Mapping[str, Any],
    selected_cell_ids: Sequence[str],
    activation_id: str,
    activation_sha256: str,
    validate_native_root: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    pipeline = config_payload.get("pipeline")
    if type(pipeline) is not dict:
        raise _Blocked("MANAGEMENT_CONFIG_PIPELINE_INVALID")
    raw_data_dir = pipeline.get("data_dir", "data")
    if type(raw_data_dir) is not str or not raw_data_dir:
        raise _Blocked("MANAGEMENT_NATIVE_DATA_ROOT_INVALID")
    data_dir_path = Path(raw_data_dir)
    data_dir_value = (
        data_dir_path
        if data_dir_path.is_absolute()
        else config_path.parent / data_dir_path
    )
    try:
        data_dir = Path(os.path.abspath(data_dir_value))
        data_dir.relative_to(source_root)
    except Exception as exc:
        raise _Blocked("MANAGEMENT_NATIVE_DATA_ROOT_INVALID") from exc
    if validate_native_root:
        data_dir = _regular_directory(
            source_root, data_dir, "MANAGEMENT_NATIVE_DATA_ROOT_INVALID"
        )
        native_root = _regular_directory(
            source_root, data_dir / "native", "MANAGEMENT_NATIVE_ROOT_INVALID"
        )
    else:
        native_root = data_dir / "native"
        try:
            native_root.relative_to(source_root)
        except ValueError as exc:
            raise _Blocked("MANAGEMENT_NATIVE_ROOT_INVALID") from exc
    native_prefix = native_root.relative_to(source_root).as_posix()
    entries = activation_manifest.get("entries")
    if (
        activation_manifest.get("activation_id") != activation_id
        or type(entries) is not dict
    ):
        raise _Blocked("MANAGEMENT_NATIVE_ACTIVATION_MANIFEST_INVALID")
    groups = sorted({"|".join(cell_id.split("|")[:2]) for cell_id in selected_cell_ids})
    specs: list[dict[str, Any]] = []
    partial: dict[str, dict[str, Any]] = {}
    for group in groups:
        symbol, timeframe = group.split("|", 1)
        symbol_entries = entries.get(symbol)
        entry = symbol_entries.get(timeframe) if type(symbol_entries) is dict else None
        if type(entry) is not dict:
            raise _Blocked("MANAGEMENT_SELECTED_NATIVE_ENTRY_MISSING")
        bars_relative = _safe_relative(
            entry.get("bars_relative_path"),
            "MANAGEMENT_NATIVE_BARS_PATH_INVALID",
        ).as_posix()
        sidecar_relative = _safe_relative(
            entry.get("sidecar_relative_path"),
            "MANAGEMENT_NATIVE_SIDECAR_PATH_INVALID",
        ).as_posix()
        bars_sha = entry.get("bars_sha256")
        sidecar_sha = entry.get("sidecar_sha256")
        if not _is_sha(bars_sha) or not _is_sha(sidecar_sha):
            raise _Blocked("MANAGEMENT_SELECTED_NATIVE_HASH_INVALID")
        bars_logical = _logical_join(native_prefix, bars_relative)
        sidecar_logical = _logical_join(native_prefix, sidecar_relative)
        specs.extend(
            [
                {
                    "role": "native_bars",
                    "logical_relative_path": bars_logical,
                    "size_bytes": None,
                    "sha256": bars_sha,
                },
                {
                    "role": "native_sidecar",
                    "logical_relative_path": sidecar_logical,
                    "size_bytes": None,
                    "sha256": sidecar_sha,
                },
            ]
        )
        partial[group] = {
            "activation_id": activation_id,
            "activation_manifest_sha256": activation_sha256,
            "bars_logical_relative_path": bars_logical,
            "bars_sha256": bars_sha,
            "sidecar_logical_relative_path": sidecar_logical,
            "sidecar_sha256": sidecar_sha,
        }
    return specs, partial


def _external_closure(
    source_root: Path,
    specs: Sequence[Mapping[str, Any]],
    limits: ManagementLimits,
) -> dict[str, dict[str, Any]]:
    if not specs or len(specs) > limits.max_external_references:
        raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BOUND_EXCEEDED")
    closure: dict[str, dict[str, Any]] = {}
    logical_roles: set[tuple[str, str]] = set()
    observed_by_path: dict[str, tuple[int, str]] = {}
    total = 0
    for spec in specs:
        role = spec.get("role")
        logical = _safe_relative(
            spec.get("logical_relative_path"),
            "MANAGEMENT_EXTERNAL_REFERENCE_PATH_INVALID",
        ).as_posix()
        if role not in _EXTERNAL_ROLES or (role, logical) in logical_roles:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_ROLE_INVALID")
        logical_roles.add((role, logical))
        path = _regular_path(
            source_root,
            source_root / logical,
            "MANAGEMENT_EXTERNAL_REFERENCE_PATH_INVALID",
        )
        expected_size = spec.get("size_bytes")
        expected_sha = spec.get("sha256")
        if logical in observed_by_path:
            size, digest = observed_by_path[logical]
        else:
            size, digest = _file_digest(
                path,
                limits.max_external_file_bytes,
                "MANAGEMENT_EXTERNAL_REFERENCE_INVALID",
            )
            observed_by_path[logical] = (size, digest)
            total += size
            if total > limits.max_external_reference_bytes:
                raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BOUND_EXCEEDED")
        if (
            expected_size is not None and expected_size != size
        ) or digest != expected_sha:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BINDING_MISMATCH")
        body = {
            "role": role,
            "logical_relative_path": logical,
            "size_bytes": size,
            "sha256": digest,
        }
        closure_id = _sha(body)
        if closure_id in closure:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_DUPLICATE")
        closure[closure_id] = body
    return dict(sorted(closure.items()))


def _native_bindings(
    partial: Mapping[str, Mapping[str, Any]],
    closure: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    by_role_path = {
        (item["role"], item["logical_relative_path"]): item for item in closure.values()
    }
    bindings: dict[str, dict[str, Any]] = {}
    for group, value in sorted(partial.items()):
        bars = by_role_path.get(("native_bars", value["bars_logical_relative_path"]))
        sidecar = by_role_path.get(
            ("native_sidecar", value["sidecar_logical_relative_path"])
        )
        if bars is None or sidecar is None:
            raise _Blocked("MANAGEMENT_SELECTED_NATIVE_CLOSURE_MISSING")
        binding = {
            **value,
            "bars_size_bytes": bars["size_bytes"],
            "sidecar_size_bytes": sidecar["size_bytes"],
        }
        if set(binding) != _NATIVE_BINDING_FIELDS:
            raise _Blocked("MANAGEMENT_SELECTED_NATIVE_BINDING_INVALID")
        bindings[group] = binding
    return bindings


def _validate_closure_specs(
    specs: Sequence[Mapping[str, Any]],
    closure: Mapping[str, Mapping[str, Any]],
) -> None:
    if len(specs) != len(closure):
        raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_ROSTER_MISMATCH")
    by_role_path = {
        (item.get("role"), item.get("logical_relative_path")): item
        for item in closure.values()
    }
    if len(by_role_path) != len(closure):
        raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_ROSTER_MISMATCH")
    consumed: set[tuple[Any, Any]] = set()
    for spec in specs:
        key = (spec.get("role"), spec.get("logical_relative_path"))
        item = by_role_path.get(key)
        if item is None or key in consumed or item.get("sha256") != spec.get("sha256"):
            raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_BINDING_MISMATCH")
        expected_size = spec.get("size_bytes")
        if expected_size is not None and item.get("size_bytes") != expected_size:
            raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_BINDING_MISMATCH")
        consumed.add(key)
    if len(consumed) != len(closure):
        raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_ROSTER_MISMATCH")


def _limits(limits: ManagementLimits) -> ManagementLimits:
    if type(limits) is not ManagementLimits:
        raise _Blocked("MANAGEMENT_LIMITS_INVALID")
    values = vars(limits)
    if any(type(value) is not int or value <= 0 for value in values.values()):
        raise _Blocked("MANAGEMENT_LIMITS_INVALID")
    return limits


def _capsule_directories(root: Path, limits: ManagementLimits) -> list[Path]:
    entries: list[Path] = []
    for item in root.iterdir():
        if item.name == ".management.lock":
            continue
        if item.name.startswith(".staging-"):
            raise _Blocked("MANAGEMENT_STALE_STAGING_PRESENT")
        info = item.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or item.is_symlink()
            or int(getattr(info, "st_file_attributes", 0)) & 0x400
            or _HEX64.fullmatch(item.name) is None
        ):
            raise _Blocked("MANAGEMENT_STORE_ENTRY_INVALID")
        entries.append(item)
        if len(entries) > limits.max_capsules:
            raise _Blocked("MANAGEMENT_CAPSULE_COUNT_BOUND_REACHED")
    return sorted(entries)


def _store_usage(root: Path, limits: ManagementLimits) -> tuple[int, int]:
    total = 0
    count = 0
    for directory in _capsule_directories(root, limits):
        for path in directory.rglob("*"):
            if path.is_file():
                if _uses_reparse(directory, path):
                    raise _Blocked("MANAGEMENT_STORE_REPARSE_FORBIDDEN")
                count += 1
                if count > limits.max_store_files:
                    raise _Blocked("MANAGEMENT_STORE_FILE_BOUND_REACHED")
                total += path.stat().st_size
                if total > limits.max_store_bytes:
                    raise _Blocked("MANAGEMENT_STORE_BYTE_BOUND_REACHED")
    return total, count


def _safe_result(
    status: str,
    errors: Sequence[str] = (),
    *,
    capsule_id: Optional[str] = None,
    generation_id: Optional[str] = None,
    candidate_ids: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "schema_version": REVERIFY_SCHEMA,
        "status": status,
        "errors": list(dict.fromkeys(str(value) for value in errors if str(value))),
        "capsule_id": capsule_id,
        "generation_id": generation_id,
        "managed_candidate_ids": list(candidate_ids),
        "permissions": dict(
            MANAGEMENT_PERMISSIONS
            if status == MANAGEMENT_ONLY
            else NO_MANAGEMENT_PERMISSIONS
        ),
    }


def create_demo_management_capsule(
    store_root: Any,
    *,
    authorization_report: Any,
    authority_result: Mapping[str, Any],
    execution_projection: Mapping[str, Any],
    position_config: Any,
    account_info: Any,
    native_universe: Any,
    authorized_at_utc: Any,
    limits: ManagementLimits = ManagementLimits(),
) -> dict[str, Any]:
    """Capture one successful DEMO authority generation atomically."""

    try:
        limits = _limits(limits)
        if authorization_report is None or authorization_report.ok is not True:
            raise _Blocked("MANAGEMENT_DEMO_AUTHORIZATION_REQUIRED")
        binding = getattr(authorization_report, "_execution_binding", None)
        if binding is None or _binding_value(binding, "schema_version") != (
            "fxpm.demo-execution-binding.v1"
        ):
            raise _Blocked("MANAGEMENT_EXECUTION_BINDING_REQUIRED")
        authority = _successful_authority(dict(authority_result), binding)
        if type(execution_projection) is not dict or not execution_projection:
            raise _Blocked("MANAGEMENT_EXECUTION_PROJECTION_INVALID")
        projection = json.loads(_canonical_bytes(execution_projection).decode("utf-8"))
        if _sha(projection) != _binding_value(binding, "projection_sha256"):
            raise _Blocked("MANAGEMENT_EXECUTION_PROJECTION_BINDING_MISMATCH")
        detector_projection = _json_bytes(
            str(_binding_value(binding, "detector_projection_json") or "").encode(
                "utf-8"
            ),
            limits.max_metadata_bytes,
        )
        if type(detector_projection) is not dict or _sha(
            detector_projection
        ) != _binding_value(binding, "detector_projection_sha256"):
            raise _Blocked("MANAGEMENT_DETECTOR_PROJECTION_BINDING_MISMATCH")
        requested = tuple(_binding_value(binding, "requested_symbols") or ())
        selected = tuple(_binding_value(binding, "selected_cell_ids") or ())
        if (
            not requested
            or requested != tuple(sorted(set(requested)))
            or not selected
            or selected != tuple(sorted(set(selected)))
            or tuple(authority["selected_cell_ids"]) != selected
        ):
            raise _Blocked("MANAGEMENT_AUTHORIZED_ROSTER_INVALID")
        cell_authority = _cell_authority(projection, selected)

        try:
            from pm_demo_readiness import _position_policy_payload

            position_policy = _position_policy_payload(position_config)
        except Exception as exc:
            raise _Blocked("MANAGEMENT_POSITION_POLICY_INVALID") from exc
        if _sha(position_policy) != _binding_value(binding, "position_config_sha256"):
            raise _Blocked("MANAGEMENT_POSITION_POLICY_BINDING_MISMATCH")
        envelope = authority.get("selected_capital_envelope")
        expected_envelope = dict(
            _binding_value(binding, "selected_capital_envelope") or ()
        )
        if type(envelope) is not dict or envelope != expected_envelope:
            raise _Blocked("MANAGEMENT_CAPITAL_POLICY_BINDING_MISMATCH")
        capital_policy = {
            "capital_config_sha256": _binding_value(binding, "capital_config_sha256"),
            "selected_capital_envelope": envelope,
        }
        if not _is_sha(capital_policy["capital_config_sha256"]):
            raise _Blocked("MANAGEMENT_CAPITAL_POLICY_INVALID")

        created_at = datetime.now(timezone.utc)
        authorized_at = _utc(authorized_at_utc, "MANAGEMENT_AUTHORIZED_AT_INVALID")
        valid_until, valid_by_symbol = _validity(
            projection, selected, authorized_at, created_at
        )
        account = _account_identity(account_info)

        ledger_path = Path(str(_binding_value(binding, "ledger_path") or ""))
        config_path = Path(str(_binding_value(binding, "config_path") or ""))
        manifest_path = ledger_path.with_name(f"{ledger_path.stem}.generation.json")
        source_root = manifest_path.parent.resolve(strict=True)
        manifest = _read_json_file(manifest_path, 4 * 1024**2)
        generation_id = manifest.get("generation_id")
        if (
            manifest.get("schema_version") != "fxpm.optimization-generation.v2"
            or manifest.get("generation_status") != "PROMOTED"
            or manifest.get("technical_failures") not in ({}, None)
            or type(generation_id) is not str
            or not generation_id
            or generation_id != generation_id.strip()
            or len(generation_id.encode("utf-8")) > 256
            or any(ord(character) < 32 for character in generation_id)
        ):
            raise _Blocked("MANAGEMENT_GENERATION_MANIFEST_INVALID")
        index_paths = _manifest_indexes(source_root, manifest)

        config_payload = _read_json_file(config_path, 4 * 1024**2)
        current_refit_index = _read_json_file(
            index_paths["current_refit_execution"], 64 * 1024**2
        )
        raw_manifest_relative = _safe_relative(
            current_refit_index.get("raw_manifest_relative_path"),
            "MANAGEMENT_CURRENT_REFIT_MANIFEST_PATH_INVALID",
        ).as_posix()
        raw_manifest_path = _regular_path(
            source_root,
            source_root / raw_manifest_relative,
            "MANAGEMENT_CURRENT_REFIT_MANIFEST_PATH_INVALID",
        )
        raw_manifest = _read_json_file(raw_manifest_path, 16 * 1024**2)
        _, artifact_root_relative, current_refit_specs = _current_refit_specs(
            current_refit_index, raw_manifest, limits
        )
        _regular_directory(
            source_root,
            source_root / artifact_root_relative,
            "MANAGEMENT_CURRENT_REFIT_ARTIFACT_ROOT_INVALID",
        )

        capital_index = _read_json_file(
            index_paths["capital_calibration"], 64 * 1024**2
        )
        replay_root_relative, capital_specs = _capital_replay_specs(
            capital_index, limits
        )
        _regular_directory(
            source_root,
            source_root / replay_root_relative,
            "MANAGEMENT_CAPITAL_REPLAY_ROOT_INVALID",
        )

        activation_id = _binding_value(binding, "native_activation_id")
        activation_sha = _binding_value(binding, "native_activation_manifest_sha256")
        if (
            getattr(native_universe, "activation_id", None) != activation_id
            or getattr(native_universe, "activation_manifest_sha256", None)
            != activation_sha
        ):
            raise _Blocked("MANAGEMENT_NATIVE_AUTHORITY_MISMATCH")
        activation_manifest_path = _regular_path(
            source_root,
            getattr(native_universe, "activation_manifest_path", None),
            "MANAGEMENT_NATIVE_ACTIVATION_MANIFEST_PATH_INVALID",
        )
        activation_manifest = _read_json_file(activation_manifest_path, 64 * 1024**2)
        native_specs, partial_native_bindings = _native_specs(
            source_root=source_root,
            config_path=config_path,
            config_payload=config_payload,
            activation_manifest=activation_manifest,
            selected_cell_ids=selected,
            activation_id=activation_id,
            activation_sha256=activation_sha,
        )

        maximum_by_role = {
            "config": 4 * 1024**2,
            "ledger": 512 * 1024**2,
            "generation_manifest": 4 * 1024**2,
            **{name: 64 * 1024**2 for name in AUTHORITY_ARTIFACTS},
            "current_refit_raw_manifest": 16 * 1024**2,
            "native_activation_manifest": 64 * 1024**2,
        }
        paths = {
            "config": config_path,
            "ledger": ledger_path,
            "generation_manifest": manifest_path,
            **index_paths,
            "current_refit_raw_manifest": raw_manifest_path,
            "native_activation_manifest": activation_manifest_path,
        }
        descriptors = {
            role: _source_descriptor(source_root, path, role, maximum_by_role[role])
            for role, path in paths.items()
        }
        if descriptors["config"]["sha256"] != _binding_value(binding, "config_sha256"):
            raise _Blocked("MANAGEMENT_CONFIG_BINDING_MISMATCH")
        if descriptors["ledger"]["sha256"] != _binding_value(binding, "ledger_sha256"):
            raise _Blocked("MANAGEMENT_LEDGER_BINDING_MISMATCH")
        if manifest.get("ledger_sha256") != descriptors["ledger"]["sha256"]:
            raise _Blocked("MANAGEMENT_MANIFEST_LEDGER_BINDING_MISMATCH")
        if descriptors["native_activation_manifest"]["sha256"] != activation_sha:
            raise _Blocked("MANAGEMENT_NATIVE_ACTIVATION_MANIFEST_HASH_MISMATCH")
        if _read_json_file(manifest_path, 4 * 1024**2) != manifest:
            raise _Blocked("MANAGEMENT_GENERATION_MANIFEST_CHANGED")

        chain = _chain_receipt(
            manifest,
            generation_id=generation_id,
            ledger_sha256=descriptors["ledger"]["sha256"],
            config_sha256=descriptors["config"]["sha256"],
            requested_symbols=requested,
            native_activation_id=_binding_value(binding, "native_activation_id"),
            native_activation_sha256=_binding_value(
                binding, "native_activation_manifest_sha256"
            ),
        )
        declared = chain.get("authority_artifact_sha256_by_name")
        if type(declared) is not dict or declared != {
            name: descriptors[name]["sha256"] for name in AUTHORITY_ARTIFACTS
        }:
            raise _Blocked("MANAGEMENT_CHAIN_INDEX_BINDING_MISMATCH")
        config_relative = _safe_relative(
            chain.get("config_relative_path"), "MANAGEMENT_CONFIG_PATH_INVALID"
        )
        if (source_root / config_relative).resolve(strict=True) != config_path.resolve(
            strict=True
        ):
            raise _Blocked("MANAGEMENT_CONFIG_PATH_BINDING_MISMATCH")
        external = _external_closure(
            source_root,
            [*current_refit_specs, *capital_specs, *native_specs],
            limits,
        )
        native_bindings = _native_bindings(partial_native_bindings, external)

        policies = {"capital": capital_policy, "position": position_policy}
        derived_values = {
            "authority_result": authority,
            "execution_projection": projection,
            "detector_projection": detector_projection,
            "execution_policies": policies,
        }
        derived_raw = {
            name: _canonical_bytes(value) for name, value in derived_values.items()
        }
        if any(
            not raw or len(raw) > limits.max_metadata_bytes
            for raw in derived_raw.values()
        ):
            raise _Blocked("MANAGEMENT_DERIVED_ARTIFACT_BOUND_EXCEEDED")
        derived_descriptors = {
            name: {
                "role": name,
                "source_relative_path": "",
                "capsule_relative_path": f"derived/{name}.json",
                "size_bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
            for name, raw in derived_raw.items()
        }
        body = {
            "schema_version": CAPSULE_SCHEMA,
            "generation_id": generation_id,
            "source_root": str(source_root),
            "created_at_utc": _iso(created_at),
            "authorized_at_utc": _iso(authorized_at),
            "valid_until_utc": valid_until,
            "valid_until_by_symbol": valid_by_symbol,
            "account": account,
            "requested_symbols": list(requested),
            "selected_cell_ids": list(selected),
            "cell_authority": cell_authority,
            "cell_authority_sha256": _sha(cell_authority),
            "authority_result_sha256": _sha(authority),
            "authority_chain_receipt_sha256": chain["receipt_sha256"],
            "detector_projection_sha256": _sha(detector_projection),
            "execution_projection_sha256": _sha(projection),
            "capital_policy_sha256": _sha(capital_policy),
            "position_policy_sha256": _sha(position_policy),
            "native_activation_id": activation_id,
            "native_activation_manifest_sha256": activation_sha,
            "selected_native_source_bindings": native_bindings,
            "selected_native_source_bindings_sha256": _sha(native_bindings),
            "source_snapshots": descriptors,
            "derived_artifacts": derived_descriptors,
            "external_closure": external,
            "external_closure_sha256": _sha(external),
            "permissions": dict(MANAGEMENT_PERMISSIONS),
        }
        capsule_id = _sha(body)
        document = {**body, "capsule_id": capsule_id}
        document_raw = _canonical_bytes(document)
        if len(document_raw) > limits.max_metadata_bytes:
            raise _Blocked("MANAGEMENT_CAPSULE_METADATA_BOUND_EXCEEDED")
        captured_bytes = sum(item["size_bytes"] for item in descriptors.values())
        captured_bytes += sum(
            item["size_bytes"] for item in derived_descriptors.values()
        )
        captured_bytes += len(document_raw)
        if captured_bytes > limits.max_capsule_bytes:
            raise _Blocked("MANAGEMENT_CAPSULE_BYTE_BOUND_EXCEEDED")

        store = _store_root(store_root, create=True)
        with _store_lock(store):
            if (
                _external_closure(
                    source_root,
                    [*current_refit_specs, *capital_specs, *native_specs],
                    limits,
                )
                != external
            ):
                raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_CHANGED")
            final = store / capsule_id
            directories = _capsule_directories(store, limits)
            current_size, current_files = _store_usage(store, limits)
            if final.exists():
                loaded = _load_capsule(store, capsule_id, limits, False)
                candidates = [row["candidate_id"] for row in loaded["cell_authority"]]
                return _safe_result(
                    CAPSULE_WRITTEN,
                    capsule_id=capsule_id,
                    generation_id=generation_id,
                    candidate_ids=candidates,
                )
            if len(directories) >= limits.max_capsules:
                raise _Blocked("MANAGEMENT_CAPSULE_COUNT_BOUND_REACHED")
            if current_size + captured_bytes > limits.max_store_bytes:
                raise _Blocked("MANAGEMENT_STORE_BYTE_BOUND_REACHED")
            expected_files = 1 + len(descriptors) + len(derived_descriptors)
            if current_files + expected_files > limits.max_store_files:
                raise _Blocked("MANAGEMENT_STORE_FILE_BOUND_REACHED")
            stage = store / f".staging-{uuid.uuid4().hex}"
            stage.mkdir()
            try:
                for role, descriptor in descriptors.items():
                    source = source_root / descriptor["source_relative_path"]
                    target = stage / descriptor["capsule_relative_path"]
                    _copy_exact(source, target, descriptor)
                for role, raw in derived_raw.items():
                    _write_exact(
                        stage / derived_descriptors[role]["capsule_relative_path"], raw
                    )
                _write_exact(stage / "capsule.json", document_raw)
                os.replace(stage, final)
                try:
                    _load_capsule(store, capsule_id, limits, False)
                except Exception:
                    shutil.rmtree(final)
                    raise
            finally:
                if stage.exists():
                    shutil.rmtree(stage)
        return _safe_result(
            CAPSULE_WRITTEN,
            capsule_id=capsule_id,
            generation_id=generation_id,
            candidate_ids=[row["candidate_id"] for row in cell_authority],
        )
    except Exception as exc:
        code = (
            str(exc)
            if isinstance(exc, _Blocked)
            else f"MANAGEMENT_CAPSULE_WRITE_FAILED:{type(exc).__name__}"
        )
        return _safe_result(BLOCKED, [code])


def _validate_descriptor(
    value: Any,
    *,
    role: str,
    external: bool,
) -> dict[str, Any]:
    fields_exact = _EXTERNAL_DESCRIPTOR_FIELDS if external else _DESCRIPTOR_FIELDS
    if (
        type(value) is not dict
        or set(value) != fields_exact
        or value.get("role") != role
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_DESCRIPTOR_INVALID")
    if (
        type(value.get("size_bytes")) is not int
        or value["size_bytes"] <= 0
        or not _is_sha(value.get("sha256"))
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_DESCRIPTOR_INVALID")
    source_relative = value.get(
        "logical_relative_path" if external else "source_relative_path"
    )
    if external and value.get("role") not in _EXTERNAL_ROLES:
        raise _Blocked("MANAGEMENT_CAPSULE_DESCRIPTOR_INVALID")
    if external or role not in DERIVED_ROLES or source_relative != "":
        _safe_relative(
            source_relative,
            "MANAGEMENT_CAPSULE_SOURCE_PATH_INVALID",
        )
    if not external:
        _safe_relative(
            value.get("capsule_relative_path"),
            "MANAGEMENT_CAPSULE_PATH_TRAVERSAL",
        )
    return value


def _load_capsule(
    store_root: Path,
    capsule_id: str,
    limits: ManagementLimits,
    validate_external: bool,
) -> dict[str, Any]:
    limits = _limits(limits)
    if not _is_sha(capsule_id):
        raise _Blocked("MANAGEMENT_CAPSULE_ID_INVALID")
    store = _store_root(store_root, create=False)
    _store_usage(store, limits)
    capsule_lexical = store / capsule_id
    if _uses_reparse(store, capsule_lexical):
        raise _Blocked("MANAGEMENT_CAPSULE_DIRECTORY_INVALID")
    capsule_dir = capsule_lexical.resolve(strict=True)
    capsule_dir.relative_to(store)
    if capsule_dir.name != capsule_id:
        raise _Blocked("MANAGEMENT_CAPSULE_DIRECTORY_INVALID")
    capsule_path = _regular_path(
        capsule_dir,
        capsule_dir / "capsule.json",
        "MANAGEMENT_CAPSULE_FILE_INVALID",
    )
    document = _read_json_file(capsule_path, limits.max_metadata_bytes)
    if type(document) is not dict or set(document) != _CAPSULE_FIELDS:
        raise _Blocked("MANAGEMENT_CAPSULE_FIELDS_INVALID")
    body = dict(document)
    declared_id = body.pop("capsule_id", None)
    if declared_id != capsule_id or _sha(body) != capsule_id:
        raise _Blocked("MANAGEMENT_CAPSULE_HASH_INVALID")
    if document["schema_version"] != CAPSULE_SCHEMA:
        raise _Blocked("MANAGEMENT_CAPSULE_SCHEMA_INVALID")
    if document["permissions"] != MANAGEMENT_PERMISSIONS:
        raise _Blocked("MANAGEMENT_CAPSULE_PERMISSIONS_INVALID")
    account = document["account"]
    if (
        type(account) is not dict
        or set(account) != _ACCOUNT_FIELDS
        or _account_identity(account) != account
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_ACCOUNT_INVALID")
    if (
        type(document["generation_id"]) is not str
        or not document["generation_id"]
        or document["generation_id"] != document["generation_id"].strip()
        or len(document["generation_id"].encode("utf-8")) > 256
        or any(ord(character) < 32 for character in document["generation_id"])
        or not _is_sha(document["authority_result_sha256"])
        or not _is_sha(document["authority_chain_receipt_sha256"])
        or not _is_sha(document["detector_projection_sha256"])
        or not _is_sha(document["execution_projection_sha256"])
        or not _is_sha(document["capital_policy_sha256"])
        or not _is_sha(document["position_policy_sha256"])
        or not _is_sha(document["native_activation_manifest_sha256"])
        or not _is_sha(document["selected_native_source_bindings_sha256"])
        or not _is_sha(document["external_closure_sha256"])
        or type(document["native_activation_id"]) is not str
        or not document["native_activation_id"]
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_BINDING_INVALID")
    authorized = _utc(document["authorized_at_utc"], "MANAGEMENT_CAPSULE_TIME_INVALID")
    created = _utc(document["created_at_utc"], "MANAGEMENT_CAPSULE_TIME_INVALID")
    valid = _utc(document["valid_until_utc"], "MANAGEMENT_CAPSULE_TIME_INVALID")
    if (
        not created - timedelta(minutes=15)
        <= authorized
        <= (created + timedelta(minutes=5))
        or not created < valid
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_CHRONOLOGY_INVALID")
    selected = document["selected_cell_ids"]
    requested = document["requested_symbols"]
    cells = document["cell_authority"]
    if (
        type(selected) is not list
        or selected != sorted(set(selected))
        or type(requested) is not list
        or requested != sorted(set(requested))
        or type(cells) is not list
        or len(cells) != len(selected)
        or not selected
        or not requested
        or any(
            type(row) is not dict or set(row) != _CELL_AUTHORITY_FIELDS for row in cells
        )
        or [row.get("cell_id") for row in cells] != selected
        or _sha(cells) != document["cell_authority_sha256"]
        or any(cell_id.count("|") != 2 for cell_id in selected)
        or not {cell_id.split("|", 1)[0] for cell_id in selected}.issubset(requested)
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_CELL_AUTHORITY_INVALID")
    if any(
        not _is_sha(row.get(name))
        for row in cells
        for name in _CELL_AUTHORITY_FIELDS
        if name not in {"cell_id", "strategy_id"}
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_CELL_FINGERPRINT_INVALID")
    if any(
        type(row["strategy_id"]) is not str or not row["strategy_id"] for row in cells
    ) or len({row["candidate_id"] for row in cells}) != len(cells):
        raise _Blocked("MANAGEMENT_CAPSULE_CELL_AUTHORITY_INVALID")
    native_bindings = document["selected_native_source_bindings"]
    selected_groups = sorted({"|".join(cell_id.split("|")[:2]) for cell_id in selected})
    if (
        type(native_bindings) is not dict
        or list(native_bindings) != selected_groups
        or _sha(native_bindings) != document["selected_native_source_bindings_sha256"]
        or any(
            type(value) is not dict
            or set(value) != _NATIVE_BINDING_FIELDS
            or value.get("activation_id") != document["native_activation_id"]
            or value.get("activation_manifest_sha256")
            != document["native_activation_manifest_sha256"]
            or not _is_sha(value.get("bars_sha256"))
            or not _is_sha(value.get("sidecar_sha256"))
            or type(value.get("bars_size_bytes")) is not int
            or value["bars_size_bytes"] <= 0
            or type(value.get("sidecar_size_bytes")) is not int
            or value["sidecar_size_bytes"] <= 0
            for value in native_bindings.values()
        )
    ):
        raise _Blocked("MANAGEMENT_SELECTED_NATIVE_BINDING_INVALID")
    snapshots = document["source_snapshots"]
    derived = document["derived_artifacts"]
    external_refs = document["external_closure"]
    if type(snapshots) is not dict or set(snapshots) != set(SOURCE_ROLES):
        raise _Blocked("MANAGEMENT_CAPSULE_SOURCE_ROSTER_INVALID")
    if type(derived) is not dict or set(derived) != set(DERIVED_ROLES):
        raise _Blocked("MANAGEMENT_CAPSULE_DERIVED_ROSTER_INVALID")
    if (
        type(external_refs) is not dict
        or not external_refs
        or len(external_refs) > limits.max_external_references
        or _sha(external_refs) != document["external_closure_sha256"]
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_EXTERNAL_ROSTER_INVALID")
    capsule_targets: list[Path] = []
    captured_bytes = capsule_path.stat().st_size
    for roster in (snapshots, derived):
        for role, value in roster.items():
            descriptor = _validate_descriptor(value, role=role, external=False)
            target = _descriptor_target(capsule_dir, descriptor)
            capsule_targets.append(target)
            size, digest = _file_digest(
                target,
                limits.max_capsule_bytes,
                "MANAGEMENT_CAPSULE_SNAPSHOT_INVALID",
            )
            if size != descriptor["size_bytes"] or digest != descriptor["sha256"]:
                raise _Blocked("MANAGEMENT_CAPSULE_SNAPSHOT_HASH_MISMATCH")
            captured_bytes += size
            if captured_bytes > limits.max_capsule_bytes:
                raise _Blocked("MANAGEMENT_CAPSULE_BYTE_BOUND_EXCEEDED")
    if len(set(capsule_targets)) != len(capsule_targets):
        raise _Blocked("MANAGEMENT_CAPSULE_PATH_ALIAS_FORBIDDEN")
    expected_files = {capsule_path, *capsule_targets}
    observed_files: set[Path] = set()
    for path in capsule_dir.rglob("*"):
        if _uses_reparse(capsule_dir, path):
            raise _Blocked("MANAGEMENT_CAPSULE_REPARSE_FORBIDDEN")
        if path.is_file():
            observed_files.add(path.resolve(strict=True))
        elif not path.is_dir():
            raise _Blocked("MANAGEMENT_CAPSULE_ENTRY_INVALID")
    if observed_files != expected_files or len(observed_files) > limits.max_store_files:
        raise _Blocked("MANAGEMENT_CAPSULE_FILE_ROSTER_INVALID")

    source_root_value = document["source_root"]
    if (
        type(source_root_value) is not str
        or not source_root_value
        or not Path(source_root_value).is_absolute()
    ):
        raise _Blocked("MANAGEMENT_CAPSULE_SOURCE_ROOT_INVALID")
    source_root = Path(source_root_value)
    if validate_external:
        source_root = _regular_directory(
            source_root.resolve(strict=True),
            source_root,
            "MANAGEMENT_CAPSULE_SOURCE_ROOT_INVALID",
        )
    external_total = 0
    observed_external_paths: set[str] = set()
    for closure_id, value in external_refs.items():
        if not _is_sha(closure_id) or closure_id != _sha(value):
            raise _Blocked("MANAGEMENT_EXTERNAL_CLOSURE_ID_INVALID")
        descriptor = _validate_descriptor(value, role=value.get("role"), external=True)
        logical = descriptor["logical_relative_path"]
        if logical not in observed_external_paths:
            observed_external_paths.add(logical)
            external_total += descriptor["size_bytes"]
        if external_total > limits.max_external_reference_bytes:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BOUND_EXCEEDED")
        if descriptor["size_bytes"] > limits.max_external_file_bytes:
            raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_BOUND_EXCEEDED")
        if validate_external:
            path = _regular_path(
                source_root,
                source_root / logical,
                "MANAGEMENT_EXTERNAL_REFERENCE_PATH_INVALID",
            )
            size, digest = _file_digest(
                path,
                limits.max_external_file_bytes,
                "MANAGEMENT_EXTERNAL_REFERENCE_INVALID",
            )
            if size != descriptor["size_bytes"] or digest != descriptor["sha256"]:
                raise _Blocked("MANAGEMENT_EXTERNAL_REFERENCE_CHANGED")
    derived_hash_bindings = {
        "authority_result": "authority_result_sha256",
        "execution_projection": "execution_projection_sha256",
        "detector_projection": "detector_projection_sha256",
    }
    for role, field in derived_hash_bindings.items():
        if derived[role]["sha256"] != document[field]:
            raise _Blocked("MANAGEMENT_DERIVED_BINDING_INVALID")
    authority = _read_json_file(
        _descriptor_target(capsule_dir, derived["authority_result"]),
        limits.max_metadata_bytes,
    )
    raw = authority.get("raw_reverification")
    if (
        set(authority) != _AUTHORITY_RESULT_FIELDS
        or authority.get("schema_version") != "fxpm.live-readiness-authority-result.v2"
        or authority.get("status") != "DEMO_PROBATION_AUTHORIZED"
        or authority.get("errors") != []
        or type(raw) is not dict
        or set(raw) != set(RAW_COMPONENTS)
        or any(raw[name] is not True for name in RAW_COMPONENTS)
        or authority.get("selected_cell_ids") != selected
        or authority.get("funded_live_authorized") is not False
        or authority.get("capital_config_matches") is not True
        or authority.get("capital_execution_enforcement_reverified") is not True
        or authority.get("native_activation_id") != document["native_activation_id"]
        or authority.get("native_activation_manifest_sha256")
        != document["native_activation_manifest_sha256"]
    ):
        raise _Blocked("MANAGEMENT_AUTHORITY_RESULT_INVALID")
    projection = _read_json_file(
        _descriptor_target(capsule_dir, derived["execution_projection"]),
        limits.max_metadata_bytes,
    )
    detector = _read_json_file(
        _descriptor_target(capsule_dir, derived["detector_projection"]),
        limits.max_metadata_bytes,
    )
    if not projection or not detector or _cell_authority(projection, selected) != cells:
        raise _Blocked("MANAGEMENT_EXECUTION_PROJECTION_INVALID")
    expected_valid, expected_valid_by_symbol = _validity(
        projection,
        selected,
        authorized,
        created,
    )
    if (
        document["valid_until_utc"] != expected_valid
        or document["valid_until_by_symbol"] != expected_valid_by_symbol
    ):
        raise _Blocked("MANAGEMENT_VALIDITY_BINDING_INVALID")
    policies_path = _descriptor_target(capsule_dir, derived["execution_policies"])
    policies = _read_json_file(policies_path, limits.max_metadata_bytes)
    if (
        set(policies) != {"capital", "position"}
        or type(policies.get("capital")) is not dict
        or type(policies.get("position")) is not dict
        or _sha(policies["capital"]) != document["capital_policy_sha256"]
        or _sha(policies["position"]) != document["position_policy_sha256"]
        or policies["capital"].get("selected_capital_envelope")
        != authority.get("selected_capital_envelope")
        or not _is_sha(policies["capital"].get("capital_config_sha256"))
    ):
        raise _Blocked("MANAGEMENT_POLICY_BINDING_INVALID")
    manifest = _read_json_file(
        _descriptor_target(capsule_dir, snapshots["generation_manifest"]),
        4 * 1024**2,
    )
    if (
        manifest.get("schema_version") != "fxpm.optimization-generation.v2"
        or manifest.get("generation_status") != "PROMOTED"
        or manifest.get("technical_failures") not in ({}, None)
        or manifest.get("generation_id") != document["generation_id"]
        or manifest.get("ledger_sha256") != snapshots["ledger"]["sha256"]
        or authority.get("ledger_sha256") != snapshots["ledger"]["sha256"]
    ):
        raise _Blocked("MANAGEMENT_GENERATION_MANIFEST_INVALID")
    artifacts = manifest.get("authority_artifacts")
    if type(artifacts) is not dict or set(artifacts) != set(AUTHORITY_ARTIFACTS):
        raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_ROSTER_INVALID")
    for name in AUTHORITY_ARTIFACTS:
        item = artifacts[name]
        if (
            type(item) is not dict
            or set(item) != {"relative_path", "sha256", "size_bytes", "schema_version"}
            or item.get("relative_path") != snapshots[name]["source_relative_path"]
            or item.get("sha256") != snapshots[name]["sha256"]
            or item.get("size_bytes") != snapshots[name]["size_bytes"]
            or item.get("schema_version")
            != f"fxpm.{name.replace('_', '-')}-authority-index.v1"
        ):
            raise _Blocked("MANAGEMENT_AUTHORITY_INDEX_BINDING_INVALID")
    receipt = _chain_receipt(
        manifest,
        generation_id=document["generation_id"],
        ledger_sha256=snapshots["ledger"]["sha256"],
        config_sha256=snapshots["config"]["sha256"],
        requested_symbols=requested,
        native_activation_id=document["native_activation_id"],
        native_activation_sha256=document["native_activation_manifest_sha256"],
    )
    if (
        receipt["receipt_sha256"] != document["authority_chain_receipt_sha256"]
        or receipt["config_relative_path"]
        != snapshots["config"]["source_relative_path"]
        or receipt["authority_artifact_sha256_by_name"]
        != {name: snapshots[name]["sha256"] for name in AUTHORITY_ARTIFACTS}
    ):
        raise _Blocked("MANAGEMENT_CHAIN_RECEIPT_BINDING_INVALID")
    if (
        snapshots["native_activation_manifest"]["sha256"]
        != document["native_activation_manifest_sha256"]
    ):
        raise _Blocked("MANAGEMENT_NATIVE_ACTIVATION_MANIFEST_HASH_MISMATCH")

    captured_config_path = _descriptor_target(capsule_dir, snapshots["config"])
    captured_config = _read_json_file(captured_config_path, 4 * 1024**2)
    current_index = _read_json_file(
        _descriptor_target(capsule_dir, snapshots["current_refit_execution"]),
        64 * 1024**2,
    )
    current_raw = _read_json_file(
        _descriptor_target(capsule_dir, snapshots["current_refit_raw_manifest"]),
        16 * 1024**2,
    )
    raw_relative, _artifact_root, current_specs = _current_refit_specs(
        current_index, current_raw, limits
    )
    if snapshots["current_refit_raw_manifest"]["source_relative_path"] != raw_relative:
        raise _Blocked("MANAGEMENT_CURRENT_REFIT_MANIFEST_BINDING_MISMATCH")
    capital_index = _read_json_file(
        _descriptor_target(capsule_dir, snapshots["capital_calibration"]),
        64 * 1024**2,
    )
    _replay_root, capital_specs = _capital_replay_specs(capital_index, limits)
    activation_manifest = _read_json_file(
        _descriptor_target(capsule_dir, snapshots["native_activation_manifest"]),
        64 * 1024**2,
    )
    native_specs, native_partial = _native_specs(
        source_root=Path(document["source_root"]),
        config_path=(
            Path(document["source_root"]) / snapshots["config"]["source_relative_path"]
        ),
        config_payload=captured_config,
        activation_manifest=activation_manifest,
        selected_cell_ids=selected,
        activation_id=document["native_activation_id"],
        activation_sha256=document["native_activation_manifest_sha256"],
        validate_native_root=False,
    )
    all_specs = [*current_specs, *capital_specs, *native_specs]
    _validate_closure_specs(all_specs, external_refs)
    if _native_bindings(native_partial, external_refs) != native_bindings:
        raise _Blocked("MANAGEMENT_SELECTED_NATIVE_BINDING_MISMATCH")
    return document


def load_demo_management_capsule(
    store_root: Any,
    capsule_id: str,
    *,
    limits: ManagementLimits = ManagementLimits(),
) -> dict[str, Any]:
    """Validate a capsule and return no entry configuration."""

    try:
        document = _load_capsule(
            Path(store_root), capsule_id, limits, validate_external=False
        )
        return _safe_result(
            CAPSULE_VALID,
            capsule_id=capsule_id,
            generation_id=document["generation_id"],
            candidate_ids=[row["candidate_id"] for row in document["cell_authority"]],
        )
    except Exception as exc:
        code = (
            str(exc)
            if isinstance(exc, _Blocked)
            else f"MANAGEMENT_CAPSULE_LOAD_FAILED:{type(exc).__name__}"
        )
        return _safe_result(BLOCKED, [code], capsule_id=capsule_id)


def _captured_path(store: Path, capsule_id: str, descriptor: Mapping[str, Any]) -> Path:
    capsule_dir = (store / capsule_id).resolve(strict=True)
    return _descriptor_target(capsule_dir, descriptor)


def _pipeline_config(payload: Mapping[str, Any], config_path: Path) -> Any:
    from pm_core import (
        PipelineConfig,
        RetiredScoringConfigError,
        strict_pipeline_config_kwargs,
    )

    pipeline = payload.get("pipeline")
    if type(pipeline) is not dict:
        raise _Blocked("MANAGEMENT_CONFIG_PIPELINE_INVALID")
    try:
        config = PipelineConfig(**strict_pipeline_config_kwargs(pipeline))
    except RetiredScoringConfigError as exc:
        raise _Blocked(
            "MANAGEMENT_CONFIG_RETIRED_SCORING_KEY:" + ",".join(exc.keys)
        ) from exc
    setattr(config, "_deployment_config_path", str(config_path))
    return config


def _position_management_context(
    store: Path,
    capsule_id: str,
    document: Mapping[str, Any],
    limits: ManagementLimits,
) -> dict[str, Any]:
    derived = document["derived_artifacts"]
    projection = _read_json_file(
        _captured_path(store, capsule_id, derived["execution_projection"]),
        limits.max_metadata_bytes,
    )
    detector = _read_json_file(
        _captured_path(store, capsule_id, derived["detector_projection"]),
        limits.max_metadata_bytes,
    )
    policies = _read_json_file(
        _captured_path(store, capsule_id, derived["execution_policies"]),
        limits.max_metadata_bytes,
    )
    value = {
        "schema_version": POSITION_MANAGEMENT_CONTEXT_SCHEMA,
        "status": MANAGEMENT_ONLY,
        "capsule_id": capsule_id,
        "generation_id": document["generation_id"],
        "account": document["account"],
        "permissions": MANAGEMENT_PERMISSIONS,
        "authorized_at_utc": document["authorized_at_utc"],
        "valid_until_utc": document["valid_until_utc"],
        "valid_until_by_symbol": document["valid_until_by_symbol"],
        "selected_cell_ids": document["selected_cell_ids"],
        "cell_authority": document["cell_authority"],
        "execution_projection": projection,
        "detector_projection": detector,
        "capital_policy": policies["capital"],
        "position_policy": policies["position"],
    }
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def reverify_demo_management_capsule(
    store_root: Any,
    capsule_id: str,
    *,
    account_info: Any,
    native_universe: Any,
    validation_time_utc: Any = None,
    limits: ManagementLimits = ManagementLimits(),
) -> dict[str, Any]:
    """Reverify captured authority for management only."""

    try:
        store = _store_root(store_root, create=False)
        document = _load_capsule(store, capsule_id, limits, validate_external=True)
        account = _account_identity(account_info)
        if account != document["account"]:
            raise _Blocked("MANAGEMENT_ACCOUNT_SWAP_FORBIDDEN")
        validation_time = _utc(
            datetime.now(timezone.utc)
            if validation_time_utc is None
            else validation_time_utc,
            "MANAGEMENT_VALIDATION_TIME_INVALID",
        )
        authorized_at = _utc(
            document["authorized_at_utc"], "MANAGEMENT_AUTHORIZED_AT_INVALID"
        )
        if validation_time < authorized_at:
            raise _Blocked("MANAGEMENT_VALIDATION_PRECEDES_AUTHORIZATION")
        if (
            getattr(native_universe, "activation_id", None)
            != document["native_activation_id"]
            or getattr(native_universe, "activation_manifest_sha256", None)
            != document["native_activation_manifest_sha256"]
        ):
            raise _Blocked("MANAGEMENT_NATIVE_AUTHORITY_MISMATCH")

        try:
            module = __import__("pm_optimization_authority")
            verifier = getattr(module, "verify_live_readiness_authority_chain")
            signature = inspect.signature(verifier)
        except Exception as exc:
            raise _Blocked("MANAGEMENT_RAW_VERIFIER_UNAVAILABLE") from exc
        if (
            not callable(verifier)
            or getattr(verifier, "__module__", None) != "pm_optimization_authority"
            or "management_only_context" not in signature.parameters
        ):
            raise _Blocked("MANAGEMENT_RAW_VERIFIER_CONTEXT_UNSUPPORTED")

        snapshots = document["source_snapshots"]
        derived = document["derived_artifacts"]
        manifest_path = _captured_path(
            store, capsule_id, snapshots["generation_manifest"]
        )
        config_path = _captured_path(store, capsule_id, snapshots["config"])
        ledger_path = _captured_path(store, capsule_id, snapshots["ledger"])
        manifest = _read_json_file(manifest_path, 4 * 1024**2)
        config_payload = _read_json_file(config_path, 4 * 1024**2)
        ledger = _read_json_file(ledger_path, 512 * 1024**2)
        expected_result_path = _captured_path(
            store, capsule_id, derived["authority_result"]
        )
        expected_result = _read_json_file(
            expected_result_path, limits.max_metadata_bytes
        )
        config = _pipeline_config(config_payload, config_path)
        index_paths = {
            name: _captured_path(store, capsule_id, snapshots[name])
            for name in AUTHORITY_ARTIFACTS
        }
        source_root = Path(document["source_root"]).resolve(strict=True)
        external = {
            closure_id: {
                **descriptor,
                "absolute_path": str(
                    (source_root / descriptor["logical_relative_path"]).resolve(
                        strict=True
                    )
                ),
            }
            for closure_id, descriptor in document["external_closure"].items()
        }
        activation_manifest_path = _captured_path(
            store, capsule_id, snapshots["native_activation_manifest"]
        )
        context = {
            "schema_version": MANAGEMENT_CONTEXT_SCHEMA,
            "capsule_id": capsule_id,
            "generation_id": document["generation_id"],
            "scope": MANAGEMENT_ONLY,
            "captured_source_root": str(source_root),
            "allow_expired_entry_management": True,
            "allow_inactive_generation_pointer": True,
            "allow_new_entries": False,
            "allow_position_increase": False,
            "allow_stop_widening": False,
            "captured_activation_manifest_path": str(activation_manifest_path),
            "captured_activation_manifest_sha256": document[
                "native_activation_manifest_sha256"
            ],
            "external_closure": external,
            "selected_native_source_bindings": document[
                "selected_native_source_bindings"
            ],
        }
        result = verifier(
            manifest=manifest,
            manifest_path=manifest_path,
            authority_index_paths=index_paths,
            ledger=ledger,
            ledger_path=ledger_path,
            config=config,
            config_payload=config_payload,
            config_path=config_path,
            requested_symbols=tuple(document["requested_symbols"]),
            native_universe=native_universe,
            validation_time_utc=validation_time,
            require_funded_authorization=False,
            management_only_context=context,
        )
        raw = result.get("raw_reverification") if type(result) is dict else None
        stable_fields = _AUTHORITY_RESULT_FIELDS - {"status"}
        if (
            type(result) is not dict
            or set(result) != _AUTHORITY_RESULT_FIELDS
            or result.get("status") != MANAGEMENT_ONLY
            or any(
                result.get(name) != expected_result.get(name) for name in stable_fields
            )
            or type(raw) is not dict
            or set(raw) != set(RAW_COMPONENTS)
            or any(raw[name] is not True for name in RAW_COMPONENTS)
        ):
            raise _Blocked("MANAGEMENT_RAW_REVERIFICATION_FAILED")
        response = _safe_result(
            MANAGEMENT_ONLY,
            capsule_id=capsule_id,
            generation_id=document["generation_id"],
            candidate_ids=[row["candidate_id"] for row in document["cell_authority"]],
        )
        response["management_context"] = _position_management_context(
            store, capsule_id, document, limits
        )
        return response
    except Exception as exc:
        code = (
            str(exc)
            if isinstance(exc, _Blocked)
            else f"MANAGEMENT_REVERIFY_FAILED:{type(exc).__name__}"
        )
        return _safe_result(BLOCKED, [code], capsule_id=capsule_id)


def _external_pins_locked(store: Path, limits: ManagementLimits) -> dict[str, Any]:
    pins_by_path: dict[str, dict[str, Any]] = {}
    capsule_ids: list[str] = []
    generation_ids: set[str] = set()
    candidate_ids: set[str] = set()
    directories = _capsule_directories(store, limits)
    _store_usage(store, limits)
    for directory in directories:
        document = _load_capsule(store, directory.name, limits, validate_external=False)
        capsule_ids.append(directory.name)
        generation_ids.add(document["generation_id"])
        candidate_ids.update(row["candidate_id"] for row in document["cell_authority"])
        source_root = Path(document["source_root"])
        for item in document["external_closure"].values():
            logical = item["logical_relative_path"]
            absolute = str(Path(os.path.abspath(source_root / logical)))
            try:
                Path(absolute).relative_to(source_root)
            except ValueError as exc:
                raise _Blocked("MANAGEMENT_PIN_PATH_INVALID") from exc
            previous = pins_by_path.get(absolute)
            identity = (item["size_bytes"], item["sha256"])
            if previous is not None and identity != (
                previous["size_bytes"],
                previous["sha256"],
            ):
                raise _Blocked("MANAGEMENT_PIN_IDENTITY_CONFLICT")
            if previous is None:
                if len(pins_by_path) >= limits.max_pin_records:
                    raise _Blocked("MANAGEMENT_PIN_RECORD_BOUND_REACHED")
                previous = {
                    "logical_relative_path": logical,
                    "absolute_path": absolute,
                    "size_bytes": item["size_bytes"],
                    "sha256": item["sha256"],
                    "roles": [],
                    "capsule_ids": [],
                }
                pins_by_path[absolute] = previous
            previous["roles"].append(item["role"])
            previous["capsule_ids"].append(directory.name)
    pins: dict[str, dict[str, Any]] = {}
    for item in pins_by_path.values():
        item["roles"] = sorted(set(item["roles"]))
        item["capsule_ids"] = sorted(set(item["capsule_ids"]))
        identity = {
            key: item[key]
            for key in (
                "logical_relative_path",
                "absolute_path",
                "size_bytes",
                "sha256",
            )
        }
        pin_id = _sha(identity)
        if pin_id in pins:
            raise _Blocked("MANAGEMENT_PIN_ID_COLLISION")
        pins[pin_id] = item
    pins = dict(sorted(pins.items()))
    return {
        "schema_version": EXTERNAL_PINS_SCHEMA,
        "status": MANAGEMENT_ONLY,
        "errors": [],
        "capsule_ids": sorted(capsule_ids),
        "generation_ids": sorted(generation_ids),
        "candidate_ids": sorted(candidate_ids),
        "external_pins": pins,
        "external_pins_sha256": _sha(pins),
    }


def _pin_failure(exc: Exception) -> dict[str, Any]:
    code = (
        str(exc)
        if isinstance(exc, _Blocked)
        else f"MANAGEMENT_PIN_LIST_FAILED:{type(exc).__name__}"
    )
    return {
        "schema_version": EXTERNAL_PINS_SCHEMA,
        "status": BLOCKED,
        "errors": [code],
        "capsule_ids": [],
        "generation_ids": [],
        "candidate_ids": [],
        "external_pins": {},
        "external_pins_sha256": _sha({}),
    }


def list_demo_management_external_pins(
    store_root: Any,
    *,
    limits: ManagementLimits = ManagementLimits(),
) -> dict[str, Any]:
    """Return exact external-file retention pins from valid capsules."""

    try:
        limits = _limits(limits)
        store = _store_root(store_root, create=False)
        with _store_lock(store):
            return _external_pins_locked(store, limits)
    except Exception as exc:
        return _pin_failure(exc)


def _retention_failure(exc: Exception) -> dict[str, Any]:
    code = (
        str(exc)
        if isinstance(exc, _Blocked)
        else f"MANAGEMENT_RETENTION_GUARD_FAILED:{type(exc).__name__}"
    )
    return {
        "schema_version": RETENTION_GUARD_SCHEMA,
        "status": BLOCKED,
        "errors": [code],
        "candidate_paths": [],
        "authorized_paths": [],
        "blocked_paths": {},
        "retention_epoch_sha256": None,
    }


def _retention_verdict_locked(
    store: Path,
    candidate_paths: Sequence[Any],
    limits: ManagementLimits,
) -> dict[str, Any]:
    if (
        not isinstance(candidate_paths, (tuple, list))
        or not 1 <= len(candidate_paths) <= limits.max_retention_candidates
    ):
        raise _Blocked("MANAGEMENT_RETENTION_REQUEST_INVALID")
    candidates: list[Path] = []
    for value in candidate_paths:
        try:
            raw = os.fspath(value)
            if type(raw) is not str or not raw or len(raw.encode("utf-8")) > 32_768:
                raise ValueError
            path = Path(os.path.abspath(raw)).resolve(strict=False)
        except Exception as exc:
            raise _Blocked("MANAGEMENT_RETENTION_PATH_INVALID") from exc
        candidates.append(path)
    if len(set(candidates)) != len(candidates):
        raise _Blocked("MANAGEMENT_RETENTION_PATH_DUPLICATE")
    pin_document = _external_pins_locked(store, limits)
    pins = pin_document["external_pins"]
    blocked: dict[str, list[str]] = {}
    for candidate in sorted(candidates):
        blockers: list[str] = []
        try:
            candidate.relative_to(store)
            blockers.append("CAPSULE_STORE")
        except ValueError:
            try:
                store.relative_to(candidate)
                blockers.append("CAPSULE_STORE")
            except ValueError:
                pass
        for pin_id, pin in pins.items():
            pinned = Path(pin["absolute_path"]).resolve(strict=False)
            try:
                pinned.relative_to(candidate)
                blockers.append(pin_id)
            except ValueError:
                pass
        if blockers:
            blocked[str(candidate)] = sorted(set(blockers))
    normalized = sorted(str(path) for path in candidates)
    epoch = _sha(
        {
            "capsule_ids": pin_document["capsule_ids"],
            "external_pins_sha256": pin_document["external_pins_sha256"],
        }
    )
    if blocked:
        return {
            "schema_version": RETENTION_GUARD_SCHEMA,
            "status": BLOCKED,
            "errors": ["MANAGEMENT_RETENTION_PINNED_PATH"],
            "candidate_paths": normalized,
            "authorized_paths": [],
            "blocked_paths": dict(sorted(blocked.items())),
            "retention_epoch_sha256": epoch,
        }
    return {
        "schema_version": RETENTION_GUARD_SCHEMA,
        "status": RETENTION_AUTHORIZED,
        "errors": [],
        "candidate_paths": normalized,
        "authorized_paths": normalized,
        "blocked_paths": {},
        "retention_epoch_sha256": epoch,
    }


@contextmanager
def hold_demo_management_retention_guard(
    store_root: Any,
    candidate_paths: Sequence[Any],
    *,
    limits: ManagementLimits = ManagementLimits(),
) -> Iterator[dict[str, Any]]:
    """Hold the capsule lock while guarded retention runs."""

    lock = None
    try:
        limits = _limits(limits)
        store = _store_root(store_root, create=False)
        lock = _store_lock(store)
        lock.__enter__()
        try:
            verdict = _retention_verdict_locked(store, candidate_paths, limits)
        except Exception as exc:
            verdict = _retention_failure(exc)
    except Exception as exc:
        yield _retention_failure(exc)
        return
    try:
        yield verdict
    finally:
        if lock is not None:
            lock.__exit__(None, None, None)


def prune_demo_management_capsules(
    store_root: Any,
    capsule_ids: Sequence[str],
    *,
    referenced_generation_ids: Sequence[str] = (),
    referenced_candidate_ids: Sequence[str] = (),
    limits: ManagementLimits = ManagementLimits(),
) -> dict[str, Any]:
    """Remove only explicitly unreferenced capsules."""

    removed: list[str] = []
    blocked: list[str] = []
    errors: list[str] = []
    try:
        limits = _limits(limits)
        if (
            not isinstance(capsule_ids, (tuple, list))
            or len(capsule_ids) > limits.max_capsules
            or any(not _is_sha(value) for value in capsule_ids)
            or len(set(capsule_ids)) != len(capsule_ids)
            or not isinstance(referenced_generation_ids, (tuple, list))
            or not isinstance(referenced_candidate_ids, (tuple, list))
            or len(referenced_generation_ids) > limits.max_prune_references
            or len(referenced_candidate_ids) > limits.max_prune_references
            or any(
                type(value) is not str or not value or len(value.encode("utf-8")) > 256
                for value in referenced_generation_ids
            )
            or any(not _is_sha(value) for value in referenced_candidate_ids)
        ):
            raise _Blocked("MANAGEMENT_PRUNE_REQUEST_INVALID")
        generations = set(referenced_generation_ids)
        candidates = set(referenced_candidate_ids)
        store = _store_root(store_root, create=False)
        with _store_lock(store):
            for capsule_id in capsule_ids:
                try:
                    document = _load_capsule(
                        store, capsule_id, limits, validate_external=False
                    )
                    owned_candidates = {
                        row["candidate_id"] for row in document["cell_authority"]
                    }
                    if (
                        document["generation_id"] in generations
                        or owned_candidates & candidates
                    ):
                        blocked.append(capsule_id)
                        continue
                    target = (store / capsule_id).resolve(strict=True)
                    target.relative_to(store)
                    if _uses_reparse(store, target):
                        raise _Blocked("MANAGEMENT_PRUNE_REPARSE_FORBIDDEN")
                    shutil.rmtree(target)
                    removed.append(capsule_id)
                except Exception as exc:
                    errors.append(
                        str(exc)
                        if isinstance(exc, _Blocked)
                        else f"MANAGEMENT_PRUNE_FAILED:{capsule_id}:{type(exc).__name__}"
                    )
    except Exception as exc:
        errors.append(
            str(exc)
            if isinstance(exc, _Blocked)
            else f"MANAGEMENT_PRUNE_FAILED:{type(exc).__name__}"
        )
    return {
        "schema_version": REVERIFY_SCHEMA,
        "status": "PRUNE_COMPLETE" if not errors else BLOCKED,
        "errors": list(dict.fromkeys(errors)),
        "removed_capsule_ids": removed,
        "referenced_capsule_ids": blocked,
    }


__all__ = [
    "AUTHORITY_ARTIFACTS",
    "BLOCKED",
    "CAPSULE_SCHEMA",
    "CAPSULE_VALID",
    "CAPSULE_WRITTEN",
    "EXTERNAL_PINS_SCHEMA",
    "RETENTION_GUARD_SCHEMA",
    "MANAGEMENT_CONTEXT_SCHEMA",
    "MANAGEMENT_ONLY",
    "MANAGEMENT_PERMISSIONS",
    "NO_MANAGEMENT_PERMISSIONS",
    "POSITION_MANAGEMENT_CONTEXT_SCHEMA",
    "RETENTION_AUTHORIZED",
    "ManagementLimits",
    "create_demo_management_capsule",
    "hold_demo_management_retention_guard",
    "list_demo_management_external_pins",
    "load_demo_management_capsule",
    "prune_demo_management_capsules",
    "reverify_demo_management_capsule",
]
