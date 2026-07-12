"""Immutable, exact-cell authorization for funded-live FXPM entries.

Optimization output is evidence, not authority to trade.  This module keeps
those concepts separate: a funded entry is authorized only when a detached,
hash-pinned review artifact approves the exact immutable winner-ledger bytes
and the exact strategy/data/deployment contract for every tradeable cell.

The artifact format deliberately has no writer in the live process.  A review
workflow may use :func:`build_cell_contract` and :func:`canonical_sha256` to
prepare an artifact, but it must provide the explicit review decision and the
configured whole-file SHA-256 out of band.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple


AUTHORIZATION_SCHEMA = "fxpm-funded-promotion-authorization-v1"
CELL_CONTRACT_SCHEMA = "fxpm-funded-promotion-cell-v1"
APPROVED_STATUSES = {"APPROVED"}
APPROVED_METHODS = {"HUMAN", "CONTROLLED"}


def _jsonable(value: Any) -> Any:
    """Return a deterministic JSON-compatible representation."""
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        _jsonable(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strict_json_loads(text: str) -> Any:
    """Load JSON while rejecting duplicate object keys and non-finite numbers.

    Promotion artifacts are security-sensitive contracts.  Python's default
    decoder silently keeps the last duplicate key and accepts NaN/Infinity;
    both behaviours make a human-reviewed byte contract ambiguous.
    """
    def _object(pairs):
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def _constant(value: str) -> Any:
        raise ValueError(f"non-finite JSON number: {value}")

    return json.loads(
        text,
        object_pairs_hook=_object,
        parse_constant=_constant,
    )


# Backwards-compatible private alias for the verifier's established call site.
_strict_json_loads = strict_json_loads


def authorization_payload_sha256(payload: Mapping[str, Any]) -> str:
    """Hash the semantic artifact, excluding its self-referential hash."""
    body = dict(payload)
    body.pop("payload_sha256", None)
    return canonical_sha256(body)


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _normalized_cell_id(symbol: Any, timeframe: Any, regime: Any) -> str:
    parts = tuple(str(value or "").strip().upper() for value in (symbol, timeframe, regime))
    return "|".join(parts)


def _timeframe_source(data_window_meta: Mapping[str, Any], timeframe: str) -> Dict[str, Any]:
    sources = data_window_meta.get("timeframe_sources") or {}
    if not isinstance(sources, Mapping):
        return {}
    source = sources.get(timeframe) or sources.get(timeframe.upper()) or {}
    return dict(source) if isinstance(source, Mapping) else {}


def _timeframe_evidence(data_window_meta: Mapping[str, Any], timeframe: str) -> Dict[str, Any]:
    evidence = data_window_meta.get("timeframe_evidence") or {}
    if not isinstance(evidence, Mapping):
        return {}
    verdict = evidence.get(timeframe) or evidence.get(timeframe.upper()) or {}
    return dict(verdict) if isinstance(verdict, Mapping) else {}


def build_cell_contract(
    symbol: str,
    timeframe: str,
    regime: str,
    regime_config: Any,
    symbol_config: Any,
) -> Dict[str, Any]:
    """Build the complete semantic contract that an approval must pin."""
    symbol_name = str(symbol or "").strip().upper()
    tf = str(timeframe or "").strip().upper()
    rg = str(regime or "").strip().upper()
    artifact_meta = dict(_get(regime_config, "artifact_meta", {}) or {})
    if not artifact_meta:
        artifact_meta = dict(_get(symbol_config, "artifact_meta", {}) or {})
    data_window_meta = dict(_get(symbol_config, "data_window_meta", {}) or {})
    source = _timeframe_source(data_window_meta, tf)
    evidence = _timeframe_evidence(data_window_meta, tf)
    activation_entry = dict(source.get("activation_entry") or {})
    deployment = dict(artifact_meta.get("deployment_contract") or {})
    parameters = dict(_get(regime_config, "parameters", {}) or {})
    validation_evidence = dict(_get(regime_config, "validation_evidence", {}) or {})
    holdout_metrics = dict(_get(regime_config, "holdout_metrics", {}) or {})
    governance_policy = dict(_get(regime_config, "governance_policy", {}) or {})
    trained_at = _get(regime_config, "trained_at", None)
    valid_until = _get(regime_config, "valid_until", None) or _get(symbol_config, "valid_until", None)
    valid_until_text = str(
        valid_until.isoformat() if isinstance(valid_until, datetime) else valid_until or ""
    )
    try:
        valid_until_parsed = datetime.fromisoformat(valid_until_text.replace("Z", "+00:00"))
    except ValueError:
        valid_until_basis = "invalid"
    else:
        valid_until_basis = (
            "offset_aware" if valid_until_parsed.tzinfo is not None else "host_local_legacy_naive"
        )

    return {
        "schema_version": CELL_CONTRACT_SCHEMA,
        "cell_id": _normalized_cell_id(symbol_name, tf, rg),
        "symbol": symbol_name,
        "timeframe": tf,
        "regime": rg,
        "promotion_status": str(_get(regime_config, "promotion_status", "BLOCKED") or "BLOCKED").upper(),
        "strategy_name": str(_get(regime_config, "strategy_name", "") or ""),
        "parameters_sha256": canonical_sha256(parameters),
        "artifact_meta_sha256": canonical_sha256(artifact_meta),
        "artifact_version": str(artifact_meta.get("artifact_version") or ""),
        "deployment_contract": {
            "contract": str(deployment.get("contract") or ""),
            "status": str(deployment.get("status") or ""),
            "sha1": str(deployment.get("sha1") or ""),
            "sha256": canonical_sha256(deployment),
        },
        "native_lineage": {
            "source_kind": str(source.get("source_kind") or ""),
            "provenance_valid": bool(source.get("provenance_valid", False)),
            "bars_sha256": str(source.get("sha256") or "").lower(),
            "sidecar_sha256": str(activation_entry.get("sidecar_sha256") or "").lower(),
            "activation_id": str(source.get("activation_id") or ""),
            "activation_manifest_sha256": str(source.get("activation_manifest_sha256") or "").lower(),
            "data_window_fingerprint": str(data_window_meta.get("data_window_fingerprint") or ""),
            "evidence_contract_version": str(data_window_meta.get("evidence_contract_version") or ""),
            "timeframe_evidence_passed": bool(evidence.get("passed", False)),
            "timeframe_evidence_source_sha256": str(evidence.get("source_sha256") or "").lower(),
            "timeframe_evidence_sha256": canonical_sha256(evidence),
        },
        "validation_evidence_sha256": canonical_sha256(validation_evidence),
        "final_lockbox_metrics_sha256": canonical_sha256(holdout_metrics),
        "governance_policy_sha256": canonical_sha256(governance_policy),
        "trained_at": str(trained_at.isoformat() if isinstance(trained_at, datetime) else trained_at or ""),
        "valid_until": valid_until_text,
        "valid_until_clock_basis": valid_until_basis,
    }


def cell_contract_sha256(contract: Mapping[str, Any]) -> str:
    return canonical_sha256(contract)


def _iter_symbol_configs(configs: Any) -> Iterable[Tuple[str, Any]]:
    if isinstance(configs, Mapping):
        for key, value in configs.items():
            if value is None:
                continue
            symbol = str(_get(value, "symbol", key) or key).strip().upper()
            if symbol:
                yield symbol, value


def _iter_tradeable_cells(
    configs: Any,
    allowed_statuses: Sequence[str],
) -> Iterable[Tuple[str, str, str, Any, Any]]:
    allowed = {str(value or "").strip().upper() for value in allowed_statuses}
    allowed -= {"", "BLOCKED", "PAPER_ONLY", "NO_TRADE", "UNKNOWN"}
    for symbol, symbol_config in _iter_symbol_configs(configs):
        regimes = _get(symbol_config, "regime_configs", {}) or {}
        if not isinstance(regimes, Mapping):
            continue
        for timeframe, regime_map in regimes.items():
            if not isinstance(regime_map, Mapping):
                continue
            for regime, cell in regime_map.items():
                if cell is None:
                    continue
                strategy = str(_get(cell, "strategy_name", "") or "")
                meta = dict(_get(cell, "artifact_meta", {}) or {})
                status = str(_get(cell, "promotion_status", "BLOCKED") or "BLOCKED").upper()
                if strategy and strategy.upper() != "NO_TRADE" and not meta.get("no_trade") and status in allowed:
                    yield symbol, str(timeframe).upper(), str(regime).upper(), cell, symbol_config


def _parse_aware_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _parse_cell_expiry_utc(value: Any) -> Optional[datetime]:
    """Parse model expiry, explicitly supporting the pipeline's legacy local clock.

    Promotion/review timestamps are always required to carry an offset.  Winner
    ledgers historically persisted schedule timestamps without one, so those
    are interpreted as host-local wall time (the same convention used by the
    scheduler) and converted to UTC for the outlives-cell comparison.
    """
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.astimezone(timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _is_sha1(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 40 and all(ch in "0123456789abcdef" for ch in text)


@dataclass
class PromotionAuthorizationResult:
    valid: bool = False
    errors: list[str] = field(default_factory=list)
    approved_cells: Set[str] = field(default_factory=set)
    authorization_id: str = ""
    artifact_path: Optional[Path] = None
    artifact_sha256: str = ""
    payload_sha256: str = ""

    def authorizes(self, symbol: str, timeframe: str, regime: str) -> bool:
        return self.valid and _normalized_cell_id(symbol, timeframe, regime) in self.approved_cells

    def concise_reason(self) -> str:
        return "; ".join(self.errors[:8]) if self.errors else "AUTHORIZED"


def verify_promotion_authorization(
    config: Any,
    symbol_configs: Any,
    *,
    ledger_path: Path,
    config_path: Optional[Path] = None,
    now: Optional[datetime] = None,
    verify_active_native: bool = True,
) -> PromotionAuthorizationResult:
    """Verify the entire detached authorization and exact tradeable cell set."""
    result = PromotionAuthorizationResult()
    probe = now or datetime.now(timezone.utc)
    if probe.tzinfo is None:
        probe = probe.replace(tzinfo=timezone.utc)
    else:
        probe = probe.astimezone(timezone.utc)

    raw_path = str(_get(config, "production_promotion_authorization_path", "") or "").strip()
    configured_sha = str(_get(config, "production_promotion_authorization_sha256", "") or "").strip().lower()
    if not raw_path:
        result.errors.append("PROMOTION_AUTHORIZATION_PATH_MISSING")
        return result
    if not _is_sha256(configured_sha):
        result.errors.append("PROMOTION_AUTHORIZATION_SHA256_NOT_PINNED")
        return result
    base = Path(config_path).resolve().parent if config_path else Path.cwd()
    artifact_path = Path(raw_path)
    if not artifact_path.is_absolute():
        artifact_path = base / artifact_path
    result.artifact_path = artifact_path
    if not artifact_path.is_file():
        result.errors.append("PROMOTION_AUTHORIZATION_FILE_MISSING")
        return result
    try:
        observed_sha = sha256_file(artifact_path)
        result.artifact_sha256 = observed_sha
        if observed_sha.lower() != configured_sha:
            result.errors.append("PROMOTION_AUTHORIZATION_FILE_SHA256_MISMATCH")
        payload = _strict_json_loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        result.errors.append(f"PROMOTION_AUTHORIZATION_UNREADABLE:{type(exc).__name__}")
        return result
    if not isinstance(payload, Mapping):
        result.errors.append("PROMOTION_AUTHORIZATION_NOT_OBJECT")
        return result
    if str(payload.get("schema_version") or "") != AUTHORIZATION_SCHEMA:
        result.errors.append("PROMOTION_AUTHORIZATION_SCHEMA_INVALID")

    declared_payload_sha = str(payload.get("payload_sha256") or "").lower()
    observed_payload_sha = authorization_payload_sha256(payload)
    result.payload_sha256 = observed_payload_sha
    if not _is_sha256(declared_payload_sha) or declared_payload_sha != observed_payload_sha:
        result.errors.append("PROMOTION_AUTHORIZATION_PAYLOAD_SHA256_MISMATCH")

    result.authorization_id = str(payload.get("authorization_id") or "").strip()
    if not result.authorization_id:
        result.errors.append("PROMOTION_AUTHORIZATION_ID_MISSING")
    if str(payload.get("authorization_status") or "").strip().upper() not in APPROVED_STATUSES:
        result.errors.append("PROMOTION_AUTHORIZATION_STATUS_NOT_APPROVED")

    issued_at = _parse_aware_utc(payload.get("issued_at"))
    expires_at = _parse_aware_utc(payload.get("expires_at"))
    if issued_at is None:
        result.errors.append("PROMOTION_AUTHORIZATION_ISSUED_AT_INVALID")
    elif issued_at > probe:
        result.errors.append("PROMOTION_AUTHORIZATION_NOT_YET_ISSUED")
    if expires_at is None:
        result.errors.append("PROMOTION_AUTHORIZATION_EXPIRES_AT_INVALID")
    elif expires_at <= probe:
        result.errors.append("PROMOTION_AUTHORIZATION_EXPIRED")
    if issued_at is not None and expires_at is not None and expires_at <= issued_at:
        result.errors.append("PROMOTION_AUTHORIZATION_TIME_RANGE_INVALID")
    max_age_days = max(1, int(_get(config, "production_promotion_authorization_max_age_days", 31) or 31))
    if (
        issued_at is not None
        and expires_at is not None
        and (expires_at - issued_at).total_seconds() > max_age_days * 86400
    ):
        result.errors.append("PROMOTION_AUTHORIZATION_EXPIRY_EXCEEDS_MAX_AGE")
    if issued_at is not None and (probe - issued_at).total_seconds() > max_age_days * 86400:
        result.errors.append("PROMOTION_AUTHORIZATION_TOO_OLD")

    review = payload.get("review") or {}
    if not isinstance(review, Mapping):
        review = {}
    if str(review.get("status") or "").strip().upper() not in APPROVED_STATUSES:
        result.errors.append("PROMOTION_REVIEW_STATUS_NOT_APPROVED")
    method = str(review.get("method") or "").strip().upper()
    if method not in APPROVED_METHODS:
        result.errors.append("PROMOTION_REVIEW_METHOD_INVALID")
    reviewer_id = str(review.get("reviewer_id") or "").strip()
    prepared_by = str(review.get("prepared_by") or "").strip()
    if not reviewer_id:
        result.errors.append("PROMOTION_REVIEWER_ID_MISSING")
    if not prepared_by:
        result.errors.append("PROMOTION_PREPARER_ID_MISSING")
    if (
        method == "HUMAN"
        and reviewer_id
        and prepared_by
        and reviewer_id.casefold() == prepared_by.casefold()
    ):
        result.errors.append("PROMOTION_REVIEW_SEPARATION_OF_DUTIES_FAILED")
    if method == "CONTROLLED" and (
        not str(review.get("control_id") or "").strip()
        or not str(review.get("control_version") or "").strip()
    ):
        result.errors.append("PROMOTION_CONTROL_IDENTITY_MISSING")
    if not str(review.get("decision_reference") or "").strip():
        result.errors.append("PROMOTION_REVIEW_DECISION_REFERENCE_MISSING")
    reviewed_at = _parse_aware_utc(review.get("reviewed_at"))
    if reviewed_at is None:
        result.errors.append("PROMOTION_REVIEWED_AT_INVALID")
    elif reviewed_at > probe or (issued_at is not None and reviewed_at > issued_at):
        result.errors.append("PROMOTION_REVIEW_TIME_INVALID")
    elif (probe - reviewed_at).total_seconds() > max_age_days * 86400:
        result.errors.append("PROMOTION_REVIEW_TOO_OLD")

    ledger_path = Path(ledger_path)
    if not ledger_path.is_file():
        result.errors.append("PROMOTION_WINNER_LEDGER_MISSING")
        return result
    observed_ledger_sha = sha256_file(ledger_path)
    declared_ledger_sha = str(payload.get("winner_ledger_sha256") or "").strip().lower()
    if not _is_sha256(declared_ledger_sha) or declared_ledger_sha != observed_ledger_sha:
        result.errors.append("PROMOTION_WINNER_LEDGER_SHA256_MISMATCH")

    allowed_statuses = _get(config, "live_allowed_promotion_statuses", ["PROMOTED"]) or ["PROMOTED"]
    if isinstance(allowed_statuses, str):
        allowed_statuses = [item.strip() for item in allowed_statuses.split(",")]
    expected: Dict[str, Dict[str, Any]] = {}
    for symbol, timeframe, regime, cell, symbol_config in _iter_tradeable_cells(symbol_configs, allowed_statuses):
        contract = build_cell_contract(symbol, timeframe, regime, cell, symbol_config)
        expected[contract["cell_id"]] = contract

    rows = payload.get("cells") or []
    if not isinstance(rows, list):
        result.errors.append("PROMOTION_AUTHORIZATION_CELLS_NOT_LIST")
        rows = []
    approved_contracts: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            result.errors.append("PROMOTION_AUTHORIZATION_CELL_NOT_OBJECT")
            continue
        cell_id = str(row.get("cell_id") or "").strip().upper()
        contract = row.get("contract") or {}
        if not cell_id or not isinstance(contract, Mapping):
            result.errors.append("PROMOTION_AUTHORIZATION_CELL_CONTRACT_MISSING")
            continue
        if cell_id in approved_contracts:
            result.errors.append(f"PROMOTION_AUTHORIZATION_DUPLICATE_CELL:{cell_id}")
            continue
        if str(row.get("decision") or "").strip().upper() != "APPROVE":
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_NOT_APPROVED:{cell_id}")
            continue
        contract_dict = dict(contract)
        if str(contract_dict.get("cell_id") or "").strip().upper() != cell_id:
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_ID_MISMATCH:{cell_id}")
        declared_contract_sha = str(row.get("contract_sha256") or "").strip().lower()
        if not _is_sha256(declared_contract_sha) or declared_contract_sha != cell_contract_sha256(contract_dict):
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_SHA256_MISMATCH:{cell_id}")
        approved_contracts[cell_id] = contract_dict

    expected_ids = set(expected)
    approved_ids = set(approved_contracts)
    if expected_ids != approved_ids:
        missing = ",".join(sorted(expected_ids - approved_ids)[:12]) or "none"
        extra = ",".join(sorted(approved_ids - expected_ids)[:12]) or "none"
        result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_SET_MISMATCH:missing={missing}:extra={extra}")

    for cell_id in sorted(expected_ids & approved_ids):
        expected_contract = expected[cell_id]
        authorized_contract = approved_contracts[cell_id]
        if canonical_sha256(expected_contract) != canonical_sha256(authorized_contract):
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_CONTRACT_DRIFT:{cell_id}")
            continue
        native = expected_contract["native_lineage"]
        required_native = (
            native.get("source_kind") == "broker_native"
            and bool(native.get("provenance_valid"))
            and bool(native.get("timeframe_evidence_passed"))
            and _is_sha256(native.get("bars_sha256"))
            and _is_sha256(native.get("sidecar_sha256"))
            and _is_sha256(native.get("activation_manifest_sha256"))
            and str(native.get("timeframe_evidence_source_sha256") or "").lower()
            == str(native.get("bars_sha256") or "").lower()
            and bool(native.get("activation_id"))
            and _is_sha1(native.get("data_window_fingerprint"))
            and bool(native.get("evidence_contract_version"))
        )
        if not required_native:
            result.errors.append(f"PROMOTION_AUTHORIZATION_NATIVE_LINEAGE_INCOMPLETE:{cell_id}")
            continue
        deployment = expected_contract["deployment_contract"]
        if (
            deployment.get("contract") != "funded_live_deployment_v1"
            or deployment.get("status") != "PASS"
            or not _is_sha1(deployment.get("sha1"))
        ):
            result.errors.append(f"PROMOTION_AUTHORIZATION_DEPLOYMENT_CONTRACT_INVALID:{cell_id}")
        if (
            not str(expected_contract.get("strategy_name") or "").strip()
            or not str(expected_contract.get("artifact_version") or "").strip()
            or not str(expected_contract.get("trained_at") or "").strip()
        ):
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_IDENTITY_INCOMPLETE:{cell_id}")
        valid_until = _parse_cell_expiry_utc(expected_contract.get("valid_until"))
        if valid_until is None:
            result.errors.append(f"PROMOTION_AUTHORIZATION_CELL_EXPIRY_INVALID:{cell_id}")
        elif expires_at is not None and expires_at > valid_until:
            result.errors.append(f"PROMOTION_AUTHORIZATION_OUTLIVES_CELL:{cell_id}")

        if verify_active_native:
            try:
                from pm_native_activation import resolve_active_native_artifact

                active_data_dir = Path(_get(config, "data_dir", "data"))
                if not active_data_dir.is_absolute():
                    active_data_dir = (base / active_data_dir).resolve()
                resolution = resolve_active_native_artifact(
                    active_data_dir,
                    expected_contract["symbol"],
                    expected_contract["timeframe"],
                    production_contract_required=True,
                )
            except Exception as exc:
                result.errors.append(f"PROMOTION_AUTHORIZATION_NATIVE_RESOLUTION_FAILED:{cell_id}:{type(exc).__name__}")
            else:
                entry = dict(resolution.entry or {})
                if not resolution.valid:
                    result.errors.append(f"PROMOTION_AUTHORIZATION_ACTIVE_NATIVE_INVALID:{cell_id}")
                elif (
                    str(resolution.activation_id or "") != str(native.get("activation_id") or "")
                    or str(resolution.activation_manifest_sha256 or "").lower()
                    != str(native.get("activation_manifest_sha256") or "").lower()
                    or str(entry.get("bars_sha256") or "").lower() != str(native.get("bars_sha256") or "").lower()
                    or str(entry.get("sidecar_sha256") or "").lower() != str(native.get("sidecar_sha256") or "").lower()
                ):
                    result.errors.append(f"PROMOTION_AUTHORIZATION_ACTIVE_NATIVE_DRIFT:{cell_id}")

    scope = {
        "winner_ledger_sha256": observed_ledger_sha,
        "cells": [
            {
                "cell_id": cell_id,
                "contract_sha256": cell_contract_sha256(expected[cell_id]),
            }
            for cell_id in sorted(expected)
        ],
    }
    declared_scope_sha = str(review.get("scope_sha256") or "").strip().lower()
    if not _is_sha256(declared_scope_sha) or declared_scope_sha != canonical_sha256(scope):
        result.errors.append("PROMOTION_REVIEW_SCOPE_SHA256_MISMATCH")

    result.approved_cells = approved_ids
    result.valid = not result.errors and bool(result.approved_cells)
    if not result.approved_cells:
        result.errors.append("PROMOTION_AUTHORIZATION_NO_APPROVED_CELLS")
        result.valid = False
    return result


def build_review_scope(ledger_sha256: str, contracts: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return the exact scope a reviewer must attest to (authoring helper)."""
    ordered = sorted((dict(contract) for contract in contracts), key=lambda item: str(item.get("cell_id", "")))
    return {
        "winner_ledger_sha256": str(ledger_sha256).lower(),
        "cells": [
            {
                "cell_id": str(contract.get("cell_id") or "").upper(),
                "contract_sha256": cell_contract_sha256(contract),
            }
            for contract in ordered
        ],
    }
