"""Temporal model-book contracts for champions, challengers, and current refits.

The final lockbox validates a frozen parameter set through the latest evidence
cutoff.  Re-optimizing after observing that lockbox creates a *new challenger*;
it is not a refit that inherits the champion's validation.  This module keeps
the legacy two roles explicit so an edge candidate cannot silently overwrite a
funded champion or recycle overlapping observations as fresh evidence.  A
third, separately authenticated lineage represents a deterministic refit of a
historically validated *process*.  That lineage inherits process evidence only
and remains non-funded until detached probation and portfolio authorization.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


TEMPORAL_LINEAGE_VERSION = "2026-07-11-two-book-temporal-lineage-v1"
PROCESS_REFIT_LINEAGE_VERSION = "2026-07-13-process-validated-current-refit-v1"
PROCESS_VALIDATION_RECEIPT_VERSION = "fxpm-process-validation-receipt-v1"
CURRENT_REFIT_RECEIPT_VERSION = "fxpm-current-refit-receipt-v1"
AUTHENTICATED_RECEIPT_VERSION = "fxpm-authenticated-receipt-v1"
LOCKBOX_CHAMPION_ROLE = "LOCKBOX_VALIDATED_CHAMPION"
PRELOCKBOX_CHAMPION_ROLE = "FROZEN_PRELOCKBOX_CHAMPION"
EDGE_CHALLENGER_ROLE = "LATEST_DATA_EDGE_CHALLENGER"
PROCESS_VALIDATED_CURRENT_REFIT_ROLE = "PROCESS_VALIDATED_CURRENT_REFIT"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_RECEIPT_BYTES = 1_048_576
_MAX_RECEIPT_NODES = 50_000
_MAX_RECEIPT_DEPTH = 16
_MAX_RECEIPT_LIST = 256
_MAX_RECEIPT_STRING = 16_384
_MIN_AUTH_KEY_BYTES = 32


def _utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _preflight_json(value: Any) -> None:
    """Bound and validate a receipt before canonical serialization."""
    stack = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_RECEIPT_NODES:
            raise ValueError("receipt node cap exceeded")
        if depth > _MAX_RECEIPT_DEPTH:
            raise ValueError("receipt depth cap exceeded")
        if current is None or isinstance(current, (bool, int)):
            continue
        if isinstance(current, float):
            if not math.isfinite(current):
                raise ValueError("receipt contains a non-finite number")
            continue
        if isinstance(current, str):
            if len(current.encode("utf-8")) > _MAX_RECEIPT_STRING:
                raise ValueError("receipt string cap exceeded")
            continue
        if isinstance(current, Mapping):
            if len(current) > _MAX_RECEIPT_LIST:
                raise ValueError("receipt mapping cap exceeded")
            for key, item in current.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or len(key.encode("utf-8")) > _MAX_RECEIPT_STRING
                ):
                    raise ValueError("receipt keys must be non-empty strings")
                stack.append((item, depth + 1))
            continue
        if isinstance(current, (list, tuple)):
            if len(current) > _MAX_RECEIPT_LIST:
                raise ValueError("receipt list cap exceeded")
            stack.extend((item, depth + 1) for item in current)
            continue
        raise ValueError(f"receipt contains unsupported type: {type(current).__name__}")


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    _preflight_json(value)
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("receipt is not canonical JSON") from exc
    if len(encoded) > _MAX_RECEIPT_BYTES:
        raise ValueError("receipt byte cap exceeded")
    return encoded


def canonical_receipt_sha256(value: Mapping[str, Any]) -> str:
    """Return the canonical SHA-256 identity of a bounded JSON receipt."""
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _auth_key(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("receipt authentication key must be bytes")
    key = bytes(value)
    if len(key) < _MIN_AUTH_KEY_BYTES:
        raise ValueError("receipt authentication key is too short")
    return key


def _authentication_message(key_id: str, payload_bytes: bytes) -> bytes:
    encoded_key_id = key_id.encode("utf-8")
    return b"\x00".join(
        (
            AUTHENTICATED_RECEIPT_VERSION.encode("ascii"),
            b"HMAC-SHA256",
            str(len(encoded_key_id)).encode("ascii"),
            encoded_key_id,
            hashlib.sha256(payload_bytes).hexdigest().encode("ascii"),
            payload_bytes,
        )
    )


def seal_authenticated_receipt(
    payload: Mapping[str, Any],
    *,
    key_id: str,
    signing_key: bytes,
) -> Dict[str, Any]:
    """Create a detached, bounded HMAC envelope around a typed receipt."""
    if not isinstance(payload, Mapping):
        raise ValueError("receipt payload must be a mapping")
    if not isinstance(key_id, str):
        raise ValueError("receipt key_id is invalid")
    normalized_key_id = key_id.strip()
    if not normalized_key_id or len(normalized_key_id.encode("utf-8")) > 256:
        raise ValueError("receipt key_id is invalid")
    payload_copy = json.loads(_canonical_json_bytes(payload).decode("utf-8"))
    payload_bytes = _canonical_json_bytes(payload_copy)
    key = _auth_key(signing_key)
    return {
        "schema_version": AUTHENTICATED_RECEIPT_VERSION,
        "payload": payload_copy,
        "authentication": {
            "scheme": "HMAC-SHA256",
            "key_id": normalized_key_id,
            "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "hmac_sha256": hmac.new(
                key,
                _authentication_message(normalized_key_id, payload_bytes),
                hashlib.sha256,
            ).hexdigest(),
        },
    }


def _open_authenticated_receipt(
    receipt: Mapping[str, Any],
    verification_keys: Mapping[str, bytes],
) -> Tuple[Dict[str, Any], str]:
    if not isinstance(receipt, Mapping) or set(receipt) != {
        "schema_version",
        "payload",
        "authentication",
    }:
        raise ValueError("authenticated receipt envelope is invalid")
    if receipt.get("schema_version") != AUTHENTICATED_RECEIPT_VERSION:
        raise ValueError("authenticated receipt schema is invalid")
    payload = receipt.get("payload")
    authentication = receipt.get("authentication")
    if not isinstance(payload, Mapping) or not isinstance(authentication, Mapping):
        raise ValueError("authenticated receipt payload or authentication is invalid")
    if set(authentication) != {
        "scheme",
        "key_id",
        "payload_sha256",
        "hmac_sha256",
    } or authentication.get("scheme") != "HMAC-SHA256":
        raise ValueError("authenticated receipt authentication contract is invalid")
    key_id = _required_text(
        authentication.get("key_id"), name="authenticated receipt key_id"
    )
    if key_id not in verification_keys:
        raise ValueError("authenticated receipt key is unavailable")
    payload_bytes = _canonical_json_bytes(payload)
    expected_payload_sha = hashlib.sha256(payload_bytes).hexdigest()
    raw_payload_sha = authentication.get("payload_sha256")
    raw_hmac = authentication.get("hmac_sha256")
    if not isinstance(raw_payload_sha, str) or raw_payload_sha != raw_payload_sha.lower():
        raise ValueError("authenticated receipt payload digest is not canonical")
    if not isinstance(raw_hmac, str) or raw_hmac != raw_hmac.lower():
        raise ValueError("authenticated receipt HMAC is not canonical")
    declared_payload_sha = raw_payload_sha
    declared_hmac = raw_hmac
    if not _SHA256_RE.fullmatch(declared_payload_sha) or not hmac.compare_digest(
        declared_payload_sha,
        expected_payload_sha,
    ):
        raise ValueError("authenticated receipt payload digest mismatch")
    expected_hmac = hmac.new(
        _auth_key(verification_keys[key_id]),
        _authentication_message(key_id, payload_bytes),
        hashlib.sha256,
    ).hexdigest()
    if not _SHA256_RE.fullmatch(declared_hmac) or not hmac.compare_digest(
        declared_hmac,
        expected_hmac,
    ):
        raise ValueError("authenticated receipt HMAC mismatch")
    normalized_payload = json.loads(payload_bytes.decode("utf-8"))
    normalized_receipt = {
        "schema_version": AUTHENTICATED_RECEIPT_VERSION,
        "payload": normalized_payload,
        "authentication": dict(authentication),
    }
    return normalized_payload, canonical_receipt_sha256(normalized_receipt)


def _exact_keys(value: Any, expected: Sequence[str], *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(expected):
        raise ValueError(f"{name} fields are invalid")
    return value


def _required_sha(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{name} is not a SHA-256 digest")
    return normalized


def _required_text(value: Any, *, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} is invalid")
    normalized = value.strip()
    if not normalized or len(normalized.encode("utf-8")) > maximum:
        raise ValueError(f"{name} is invalid")
    return normalized


def _required_utc(value: Any, *, name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is not an explicit UTC timestamp")
    try:
        raw = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} is not an explicit UTC timestamp") from exc
    if raw.tzinfo is None or raw.utcoffset() is None:
        raise ValueError(f"{name} is not an explicit UTC timestamp")
    parsed = raw.astimezone(timezone.utc)
    if parsed is None:
        raise ValueError(f"{name} is not a UTC timestamp")
    return parsed


def build_lockbox_champion_lineage(data_window_meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Describe when frozen champion parameters were selected and evidenced."""
    windows = dict((data_window_meta or {}).get("windows", {}) or {})
    tuning = dict(windows.get("tuning_validation", {}) or {})
    lockbox = dict(windows.get("final_lockbox", {}) or {})
    selection_cutoff = _utc(tuning.get("end_ts") or tuning.get("end_ts_exclusive"))
    lockbox_start = _utc(lockbox.get("start_ts"))
    lockbox_end = _utc(lockbox.get("end_ts") or lockbox.get("end_ts_exclusive"))
    evidence_as_of = _utc((data_window_meta or {}).get("as_of_utc")) or lockbox_end

    errors = []
    if selection_cutoff is None:
        errors.append("tuning selection cutoff missing")
    if lockbox_start is None or lockbox_end is None or lockbox_start >= lockbox_end:
        errors.append("final lockbox interval missing or invalid")
    if (
        selection_cutoff is not None
        and lockbox_start is not None
        and selection_cutoff > lockbox_start
    ):
        errors.append("parameter selection overlaps final lockbox")
    if evidence_as_of is not None and lockbox_end is not None and lockbox_end > evidence_as_of:
        errors.append("lockbox ends after evidence as-of cutoff")

    selection_age_days = None
    evidence_lag_days = None
    if evidence_as_of is not None and selection_cutoff is not None:
        selection_age_days = max(0.0, (evidence_as_of - selection_cutoff).total_seconds() / 86_400.0)
    if evidence_as_of is not None and lockbox_end is not None:
        evidence_lag_days = max(0.0, (evidence_as_of - lockbox_end).total_seconds() / 86_400.0)

    return {
        "schema_version": TEMPORAL_LINEAGE_VERSION,
        "book_role": LOCKBOX_CHAMPION_ROLE,
        "contract_status": "FAIL" if errors else "PASS",
        "contract_errors": errors,
        "funded_role_eligible": not errors,
        "parameter_selection_cutoff_utc": _iso(selection_cutoff),
        "final_lockbox_start_utc": _iso(lockbox_start),
        "performance_evidenced_through_utc": _iso(lockbox_end),
        "evidence_as_of_utc": _iso(evidence_as_of),
        "parameter_selection_age_days_at_asof": selection_age_days,
        "evidence_lag_days_at_asof": evidence_lag_days,
        "parameters_frozen_during_final_lockbox": True,
        "edge_refit_performed": False,
        "overlapping_lockbox_reuse_prohibited": True,
    }


def build_prelockbox_champion_lineage(data_window_meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Describe a Phase-A champion before any final-lockbox outcome is read."""
    windows = dict((data_window_meta or {}).get("windows", {}) or {})
    tuning = dict(windows.get("tuning_validation", {}) or {})
    lockbox = dict(windows.get("final_lockbox", {}) or {})
    selection_cutoff = _utc(tuning.get("end_ts") or tuning.get("end_ts_exclusive"))
    lockbox_start = _utc(lockbox.get("start_ts"))
    lockbox_end = _utc(lockbox.get("end_ts") or lockbox.get("end_ts_exclusive"))
    errors = []
    if selection_cutoff is None:
        errors.append("tuning selection cutoff missing")
    if lockbox_start is None or lockbox_end is None or lockbox_start >= lockbox_end:
        errors.append("final lockbox interval missing or invalid")
    if (
        selection_cutoff is not None
        and lockbox_start is not None
        and selection_cutoff > lockbox_start
    ):
        errors.append("parameter selection overlaps final lockbox")
    return {
        "schema_version": TEMPORAL_LINEAGE_VERSION,
        "book_role": PRELOCKBOX_CHAMPION_ROLE,
        "contract_status": "FAIL" if errors else "FROZEN_PRELOCKBOX",
        "contract_errors": errors,
        "funded_role_eligible": False,
        "parameter_selection_cutoff_utc": _iso(selection_cutoff),
        # Evidence is intentionally capped at the tuning selection cutoff.
        "performance_evidenced_through_utc": _iso(selection_cutoff),
        "final_lockbox_start_utc": _iso(lockbox_start),
        "final_lockbox_end_utc": _iso(lockbox_end),
        "final_lockbox_outcomes_observed": False,
        "parameters_frozen_for_future_final_lockbox": not errors,
        "parameters_frozen_during_final_lockbox": False,
        "overlapping_lockbox_reuse_prohibited": True,
        "joint_roster_release_required": True,
    }


def build_empty_edge_challenger_book(data_window_meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Reserve a separate paper-only book for a future latest-data refit.

    Generating a genuine challenger requires another optimization surface and
    materially increases run cost, so the main institutional optimization does
    not pretend to have performed it.  The explicit empty book is preferable to
    silently replacing the champion with an unvalidated edge selection.
    """
    as_of = _utc((data_window_meta or {}).get("as_of_utc"))
    return {
        "schema_version": TEMPORAL_LINEAGE_VERSION,
        "book_role": EDGE_CHALLENGER_ROLE,
        "generation_status": "NOT_BUILT",
        "funded_role_eligible": False,
        "paper_only": True,
        "as_of_utc": _iso(as_of),
        "regime_configs": {},
        "qualification_contract": {
            "must_not_overwrite_champion": True,
            "must_not_inherit_champion_lockbox_verdict": True,
            "requires_future_nonoverlapping_forward_evidence": True,
            "requires_exact_cell_outcome_attribution": True,
            "requires_new_promotion_authorization": True,
        },
        "implementation_status": (
            "The production optimizer may populate this book with fixed-family "
            "latest-5y refits; every generated cell remains paper-only until "
            "strictly later, non-overlapping forward evidence is accumulated."
        ),
    }


def build_edge_challenger_lineage(
    *,
    fit_start_utc: Any,
    fit_end_utc: Any,
    parent_champion: Mapping[str, Any],
) -> Dict[str, Any]:
    """Describe a real latest-data refit that has no inherited OOS verdict."""
    fit_start = _utc(fit_start_utc)
    fit_end = _utc(fit_end_utc)
    errors = []
    if fit_start is None or fit_end is None or fit_start >= fit_end:
        errors.append("edge fit interval missing or invalid")
    parent_role = str((parent_champion or {}).get("book_role", "") or "").upper()
    if parent_role != LOCKBOX_CHAMPION_ROLE:
        errors.append("parent is not a lockbox champion")
    return {
        "schema_version": TEMPORAL_LINEAGE_VERSION,
        "book_role": EDGE_CHALLENGER_ROLE,
        "contract_status": "FAIL" if errors else "PAPER_ONLY",
        "contract_errors": errors,
        "funded_role_eligible": False,
        "paper_only": True,
        "fit_start_utc": _iso(fit_start),
        "fit_cutoff_utc": _iso(fit_end),
        "parameter_selection_cutoff_utc": _iso(fit_end),
        "performance_evidenced_through_utc": None,
        "parameters_frozen_during_final_lockbox": False,
        "edge_refit_performed": not errors,
        "champion_lockbox_verdict_inherited": False,
        "requires_forward_evidence_strictly_after_utc": _iso(fit_end),
        "requires_future_nonoverlapping_forward_evidence": True,
        "parent_champion_selection_cutoff_utc": (
            parent_champion or {}
        ).get("parameter_selection_cutoff_utc"),
        "parent_champion_evidenced_through_utc": (
            parent_champion or {}
        ).get("performance_evidenced_through_utc"),
    }


_PROCESS_CONTRACT_FIELDS = (
    "code_contract_sha256",
    "frozen_config_sha256",
    "candidate_domain_sha256",
    "decision_rule_sha256",
    "source_policy_sha256",
    "regime_policy_sha256",
    "horizon_policy_sha256",
    "scoring_policy_sha256",
    "cost_policy_sha256",
    "risk_policy_sha256",
)

_ANCHOR_FIELDS = (
    "anchor_id",
    "process_sha256",
    "fit_start_utc",
    "selection_information_cutoff_utc",
    "purge_end_utc",
    "oos_start_utc",
    "oos_end_utc",
    "selection_max_input_utc",
    "evaluation_max_input_utc",
    "post_oos_data_used_for_selection",
    "source_snapshot_sha256",
    "regime_params_sha256",
    "horizon_decisions_sha256",
    "trial_ledger_sha256",
    "selected_book_sha256",
    "cell_roster_sha256",
    "cell_evidence_sha256",
    "portfolio_replay_sha256",
    "outcome_sha256",
    "status",
)

_PROCESS_DECISION_FIELDS = (
    "status",
    "decision_rule_sha256",
    "all_precommitted_anchors_consumed",
    "interior_holdout_used_as_final_gate",
    "dependence_adjusted",
    "multiple_testing_adjusted",
    "cell_evidence_complete",
    "common_portfolio_replay_complete",
    "process_level_evidence_only",
)

_PROCESS_RECEIPT_FIELDS = (
    "schema_version",
    "process_contract",
    "process_sha256",
    "minimum_required_anchors",
    "precommitted_anchor_ids",
    "anchors",
    "anchor_bundle_sha256",
    "cell_evidence_bundle_sha256",
    "portfolio_replay_bundle_sha256",
    "dependence_correction_sha256",
    "probation_calibration_sha256",
    "decision",
)

_REFIT_AUTHORIZATION_FIELDS = (
    "authorization_id",
    "process_validation_receipt_sha256",
    "process_sha256",
    "fit_start_utc",
    "fit_end_utc",
    "issued_at_utc",
    "source_snapshot_sha256",
    "regime_params_sha256",
    "horizon_decisions_sha256",
    "trial_ledger_sha256",
    "cell_roster_sha256",
    "code_contract_sha256",
    "frozen_config_sha256",
    "probation_calibration_sha256",
    "one_touch_nonce_sha256",
)

_REFIT_EXECUTION_FIELDS = (
    "authorization_sha256",
    "process_sha256",
    "started_at_utc",
    "completed_at_utc",
    "max_input_timestamp_utc",
    "output_book_sha256",
    "selected_candidate_bundle_sha256",
    "execution_trace_sha256",
    "attempts",
    "status",
    "refit_process_frozen",
    "post_authorization_policy_change",
    "parameter_level_oos_evidence_claimed",
)

_CURRENT_REFIT_RECEIPT_FIELDS = (
    "schema_version",
    "authorization",
    "authorization_sha256",
    "execution",
    "execution_sha256",
)


def _validate_process_validation_payload(
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    _exact_keys(payload, _PROCESS_RECEIPT_FIELDS, name="process validation receipt")
    if payload.get("schema_version") != PROCESS_VALIDATION_RECEIPT_VERSION:
        raise ValueError("process validation receipt schema is invalid")

    process_contract = _exact_keys(
        payload.get("process_contract"),
        _PROCESS_CONTRACT_FIELDS,
        name="process contract",
    )
    normalized_contract = {
        name: _required_sha(process_contract.get(name), name=name)
        for name in _PROCESS_CONTRACT_FIELDS
    }
    process_sha = _required_sha(payload.get("process_sha256"), name="process_sha256")
    if process_sha != canonical_receipt_sha256(normalized_contract):
        raise ValueError("process_sha256 does not bind the exact process contract")

    minimum = payload.get("minimum_required_anchors")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or not 3 <= minimum <= 64:
        raise ValueError("minimum_required_anchors is invalid")
    precommitted_ids = payload.get("precommitted_anchor_ids")
    anchors = payload.get("anchors")
    if not isinstance(precommitted_ids, list) or not isinstance(anchors, list):
        raise ValueError("precommitted anchors are invalid")
    if not minimum <= len(precommitted_ids) == len(anchors) <= 64:
        raise ValueError("the complete precommitted anchor set is required")
    normalized_ids = [
        _required_text(value, name="anchor_id") for value in precommitted_ids
    ]
    if len(set(normalized_ids)) != len(normalized_ids):
        raise ValueError("precommitted anchor ids are not unique")

    normalized_anchors = []
    previous_oos_start: Optional[datetime] = None
    previous_oos_end: Optional[datetime] = None
    for expected_id, raw_anchor in zip(normalized_ids, anchors):
        anchor = _exact_keys(raw_anchor, _ANCHOR_FIELDS, name="anchor")
        anchor_id = _required_text(anchor.get("anchor_id"), name="anchor_id")
        if anchor_id != expected_id:
            raise ValueError("anchor order differs from the precommitted set")
        if _required_sha(anchor.get("process_sha256"), name="anchor process_sha256") != process_sha:
            raise ValueError("an anchor used a different process")

        fit_start = _required_utc(anchor.get("fit_start_utc"), name="fit_start_utc")
        selection_cutoff = _required_utc(
            anchor.get("selection_information_cutoff_utc"),
            name="selection_information_cutoff_utc",
        )
        purge_end = _required_utc(anchor.get("purge_end_utc"), name="purge_end_utc")
        oos_start = _required_utc(anchor.get("oos_start_utc"), name="oos_start_utc")
        oos_end = _required_utc(anchor.get("oos_end_utc"), name="oos_end_utc")
        selection_max_input = _required_utc(
            anchor.get("selection_max_input_utc"),
            name="selection_max_input_utc",
        )
        evaluation_max_input = _required_utc(
            anchor.get("evaluation_max_input_utc"),
            name="evaluation_max_input_utc",
        )
        if not (
            fit_start
            < selection_cutoff
            < purge_end
            <= oos_start
            < oos_end
        ):
            raise ValueError("anchor fit/purge/OOS chronology is invalid")
        if not fit_start <= selection_max_input <= selection_cutoff:
            raise ValueError("anchor selection used information outside its frozen fit interval")
        if not oos_start <= evaluation_max_input < oos_end:
            raise ValueError("anchor evaluation information boundary is invalid")
        if anchor.get("post_oos_data_used_for_selection") is not False:
            raise ValueError("anchor selection used post-OOS data")
        if str(anchor.get("status") or "").upper() != "COMPLETE":
            raise ValueError("every precommitted anchor must be evaluated exactly once")
        if previous_oos_start is not None and oos_start <= previous_oos_start:
            raise ValueError("anchor OOS starts are not strictly chronological")
        if previous_oos_end is not None and oos_end <= previous_oos_end:
            raise ValueError("anchor OOS ends are not strictly chronological")
        previous_oos_start = oos_start
        previous_oos_end = oos_end

        normalized_anchor = {
            "anchor_id": anchor_id,
            "process_sha256": process_sha,
            "fit_start_utc": _iso(fit_start),
            "selection_information_cutoff_utc": _iso(selection_cutoff),
            "purge_end_utc": _iso(purge_end),
            "oos_start_utc": _iso(oos_start),
            "oos_end_utc": _iso(oos_end),
            "selection_max_input_utc": _iso(selection_max_input),
            "evaluation_max_input_utc": _iso(evaluation_max_input),
            "post_oos_data_used_for_selection": False,
            "status": "COMPLETE",
        }
        for name in (
            "source_snapshot_sha256",
            "regime_params_sha256",
            "horizon_decisions_sha256",
            "trial_ledger_sha256",
            "selected_book_sha256",
            "cell_roster_sha256",
            "cell_evidence_sha256",
            "portfolio_replay_sha256",
            "outcome_sha256",
        ):
            normalized_anchor[name] = _required_sha(anchor.get(name), name=name)
        normalized_anchors.append(normalized_anchor)

    anchor_bundle_sha = _required_sha(
        payload.get("anchor_bundle_sha256"), name="anchor_bundle_sha256"
    )
    if anchor_bundle_sha != canonical_receipt_sha256({"anchors": normalized_anchors}):
        raise ValueError("anchor_bundle_sha256 does not bind every anchor")

    decision = _exact_keys(
        payload.get("decision"), _PROCESS_DECISION_FIELDS, name="process decision"
    )
    if str(decision.get("status") or "").upper() != "PASS":
        raise ValueError("the precommitted process decision did not pass")
    if _required_sha(
        decision.get("decision_rule_sha256"), name="decision_rule_sha256"
    ) != normalized_contract["decision_rule_sha256"]:
        raise ValueError("the process decision rule changed")
    required_true = (
        "all_precommitted_anchors_consumed",
        "dependence_adjusted",
        "multiple_testing_adjusted",
        "cell_evidence_complete",
        "common_portfolio_replay_complete",
        "process_level_evidence_only",
    )
    if any(decision.get(name) is not True for name in required_true):
        raise ValueError("the process decision is missing a mandatory evidence gate")
    if decision.get("interior_holdout_used_as_final_gate") is not False:
        raise ValueError("an interior holdout cannot be the final deployment gate")

    return {
        "process_contract": normalized_contract,
        "process_sha256": process_sha,
        "anchor_count": len(normalized_anchors),
        "anchor_bundle_sha256": anchor_bundle_sha,
        "first_anchor_oos_start_utc": normalized_anchors[0]["oos_start_utc"],
        "performance_evidenced_through_utc": normalized_anchors[-1]["oos_end_utc"],
        "cell_evidence_bundle_sha256": _required_sha(
            payload.get("cell_evidence_bundle_sha256"),
            name="cell_evidence_bundle_sha256",
        ),
        "portfolio_replay_bundle_sha256": _required_sha(
            payload.get("portfolio_replay_bundle_sha256"),
            name="portfolio_replay_bundle_sha256",
        ),
        "dependence_correction_sha256": _required_sha(
            payload.get("dependence_correction_sha256"),
            name="dependence_correction_sha256",
        ),
        "probation_calibration_sha256": _required_sha(
            payload.get("probation_calibration_sha256"),
            name="probation_calibration_sha256",
        ),
    }


def _validate_current_refit_payload(
    payload: Mapping[str, Any],
    *,
    process: Mapping[str, Any],
    process_receipt_sha256: str,
    as_of: datetime,
) -> Dict[str, Any]:
    _exact_keys(payload, _CURRENT_REFIT_RECEIPT_FIELDS, name="current refit receipt")
    if payload.get("schema_version") != CURRENT_REFIT_RECEIPT_VERSION:
        raise ValueError("current refit receipt schema is invalid")
    authorization = _exact_keys(
        payload.get("authorization"),
        _REFIT_AUTHORIZATION_FIELDS,
        name="current refit authorization",
    )
    authorization_sha = _required_sha(
        payload.get("authorization_sha256"), name="authorization_sha256"
    )
    if authorization_sha != canonical_receipt_sha256(dict(authorization)):
        raise ValueError("authorization_sha256 does not bind the authorization")
    if _required_sha(
        authorization.get("process_validation_receipt_sha256"),
        name="process_validation_receipt_sha256",
    ) != process_receipt_sha256:
        raise ValueError("current refit is bound to a different process receipt")
    process_sha = str(process["process_sha256"])
    if _required_sha(authorization.get("process_sha256"), name="process_sha256") != process_sha:
        raise ValueError("current refit authorization changed the validated process")

    fit_start = _required_utc(authorization.get("fit_start_utc"), name="fit_start_utc")
    fit_end = _required_utc(authorization.get("fit_end_utc"), name="fit_end_utc")
    issued_at = _required_utc(authorization.get("issued_at_utc"), name="issued_at_utc")
    if not fit_start < fit_end == as_of <= issued_at:
        raise ValueError("current refit authorization is not frozen through the as-of cutoff")
    process_evidence_end = _required_utc(
        process.get("performance_evidenced_through_utc"),
        name="performance_evidenced_through_utc",
    )
    if fit_end <= process_evidence_end:
        raise ValueError("current refit does not extend beyond historical process evidence")
    contract = process["process_contract"]
    if _required_sha(
        authorization.get("code_contract_sha256"), name="code_contract_sha256"
    ) != contract["code_contract_sha256"]:
        raise ValueError("current refit code differs from the validated process")
    if _required_sha(
        authorization.get("frozen_config_sha256"), name="frozen_config_sha256"
    ) != contract["frozen_config_sha256"]:
        raise ValueError("current refit configuration differs from the validated process")
    if _required_sha(
        authorization.get("probation_calibration_sha256"),
        name="probation_calibration_sha256",
    ) != process["probation_calibration_sha256"]:
        raise ValueError("current refit changed the calibrated probation contract")

    normalized_authorization = {
        "authorization_id": _required_text(
            authorization.get("authorization_id"), name="authorization_id"
        ),
        "process_validation_receipt_sha256": process_receipt_sha256,
        "process_sha256": process_sha,
        "fit_start_utc": _iso(fit_start),
        "fit_end_utc": _iso(fit_end),
        "issued_at_utc": _iso(issued_at),
        "code_contract_sha256": contract["code_contract_sha256"],
        "frozen_config_sha256": contract["frozen_config_sha256"],
        "probation_calibration_sha256": process["probation_calibration_sha256"],
    }
    for name in (
        "source_snapshot_sha256",
        "regime_params_sha256",
        "horizon_decisions_sha256",
        "trial_ledger_sha256",
        "cell_roster_sha256",
        "one_touch_nonce_sha256",
    ):
        normalized_authorization[name] = _required_sha(authorization.get(name), name=name)
    if authorization_sha != canonical_receipt_sha256(normalized_authorization):
        raise ValueError("authorization contains a non-canonical or unbound field")

    execution = _exact_keys(
        payload.get("execution"), _REFIT_EXECUTION_FIELDS, name="current refit execution"
    )
    execution_sha = _required_sha(payload.get("execution_sha256"), name="execution_sha256")
    if execution_sha != canonical_receipt_sha256(dict(execution)):
        raise ValueError("execution_sha256 does not bind the execution")
    if _required_sha(
        execution.get("authorization_sha256"), name="execution authorization_sha256"
    ) != authorization_sha:
        raise ValueError("current refit execution used another authorization")
    if _required_sha(execution.get("process_sha256"), name="execution process_sha256") != process_sha:
        raise ValueError("current refit execution changed the validated process")
    started_at = _required_utc(execution.get("started_at_utc"), name="started_at_utc")
    completed_at = _required_utc(execution.get("completed_at_utc"), name="completed_at_utc")
    max_input = _required_utc(
        execution.get("max_input_timestamp_utc"), name="max_input_timestamp_utc"
    )
    if not issued_at <= started_at <= completed_at:
        raise ValueError("current refit was not authorized before execution")
    if not fit_start <= max_input <= fit_end:
        raise ValueError("current refit used information outside its frozen fit window")
    attempts = execution.get("attempts")
    if isinstance(attempts, bool) or attempts != 1:
        raise ValueError("current refit was not a one-touch execution")
    if str(execution.get("status") or "").upper() != "COMPLETE":
        raise ValueError("current refit execution is incomplete")
    if execution.get("refit_process_frozen") is not True:
        raise ValueError("current refit process was not frozen")
    if execution.get("post_authorization_policy_change") is not False:
        raise ValueError("current refit policy changed after authorization")
    if execution.get("parameter_level_oos_evidence_claimed") is not False:
        raise ValueError("current refit falsely claims parameter-level OOS evidence")

    normalized_execution = {
        "authorization_sha256": authorization_sha,
        "process_sha256": process_sha,
        "started_at_utc": _iso(started_at),
        "completed_at_utc": _iso(completed_at),
        "max_input_timestamp_utc": _iso(max_input),
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    for name in (
        "output_book_sha256",
        "selected_candidate_bundle_sha256",
        "execution_trace_sha256",
    ):
        normalized_execution[name] = _required_sha(execution.get(name), name=name)
    if execution_sha != canonical_receipt_sha256(normalized_execution):
        raise ValueError("execution contains a non-canonical or unbound field")
    return {
        "authorization_id": normalized_authorization["authorization_id"],
        "authorization_sha256": authorization_sha,
        "execution_sha256": execution_sha,
        "fit_start_utc": normalized_authorization["fit_start_utc"],
        "fit_end_utc": normalized_authorization["fit_end_utc"],
        "source_snapshot_sha256": normalized_authorization["source_snapshot_sha256"],
        "regime_params_sha256": normalized_authorization["regime_params_sha256"],
        "horizon_decisions_sha256": normalized_authorization["horizon_decisions_sha256"],
        "trial_ledger_sha256": normalized_authorization["trial_ledger_sha256"],
        "cell_roster_sha256": normalized_authorization["cell_roster_sha256"],
        "output_book_sha256": normalized_execution["output_book_sha256"],
        "completed_at_utc": normalized_execution["completed_at_utc"],
    }


def build_process_validated_current_refit_lineage(
    *,
    process_validation_receipt: Mapping[str, Any],
    current_refit_receipt: Mapping[str, Any],
    verification_keys: Mapping[str, bytes],
    as_of_utc: Any,
) -> Dict[str, Any]:
    """Build the distinct probation lineage for a current deterministic refit.

    The historical anchors validate the frozen *process*.  They do not grant
    parameter-level OOS evidence to the refitted parameter vector.  This
    function therefore never marks the refit funded-live eligible; portfolio
    promotion and evidence-calibrated probation remain separate authorities.
    """
    as_of: Optional[datetime] = None
    errors = []
    process: Dict[str, Any] = {}
    refit: Dict[str, Any] = {}
    process_receipt_sha: Optional[str] = None
    refit_receipt_sha: Optional[str] = None
    try:
        as_of = _required_utc(as_of_utc, name="as_of_utc")
        process_payload, process_receipt_sha = _open_authenticated_receipt(
            process_validation_receipt, verification_keys
        )
        process = _validate_process_validation_payload(process_payload)
        refit_payload, refit_receipt_sha = _open_authenticated_receipt(
            current_refit_receipt, verification_keys
        )
        refit = _validate_current_refit_payload(
            refit_payload,
            process=process,
            process_receipt_sha256=process_receipt_sha,
            as_of=as_of,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        errors.append(str(exc))

    passed = not errors
    return {
        "schema_version": PROCESS_REFIT_LINEAGE_VERSION,
        "book_role": PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
        "contract_status": "PROBATION_ONLY" if passed else "FAIL",
        "contract_errors": errors,
        "funded_role_eligible": False,
        "probation_role_eligible": False,
        "probation_candidate_eligible": passed,
        "production_integrated": False,
        "production_promotable": False,
        "paper_only_until_detached_probation_authorization": True,
        "process_level_evidence_inherited": passed,
        "parameter_level_oos_evidence_inherited": False,
        "champion_lockbox_verdict_inherited": False,
        "middle_or_interior_holdout_is_final_gate": False,
        "strict_leave_future_out_anchor_count": process.get("anchor_count"),
        "process_sha256": process.get("process_sha256"),
        "process_validation_receipt_sha256": process_receipt_sha,
        "anchor_bundle_sha256": process.get("anchor_bundle_sha256"),
        "cell_evidence_bundle_sha256": process.get("cell_evidence_bundle_sha256"),
        "portfolio_replay_bundle_sha256": process.get("portfolio_replay_bundle_sha256"),
        "dependence_correction_sha256": process.get("dependence_correction_sha256"),
        "probation_calibration_sha256": process.get("probation_calibration_sha256"),
        "historical_process_evidenced_through_utc": process.get(
            "performance_evidenced_through_utc"
        ),
        "current_refit_receipt_sha256": refit_receipt_sha,
        "current_refit_authorization_sha256": refit.get("authorization_sha256"),
        "current_refit_execution_sha256": refit.get("execution_sha256"),
        "current_refit_fit_start_utc": refit.get("fit_start_utc"),
        "parameter_selection_cutoff_utc": refit.get("fit_end_utc"),
        "current_source_snapshot_sha256": refit.get("source_snapshot_sha256"),
        "current_regime_params_sha256": refit.get("regime_params_sha256"),
        "current_horizon_decisions_sha256": refit.get("horizon_decisions_sha256"),
        "current_trial_ledger_sha256": refit.get("trial_ledger_sha256"),
        "current_cell_roster_sha256": refit.get("cell_roster_sha256"),
        "current_output_book_sha256": refit.get("output_book_sha256"),
        "current_refit_completed_at_utc": refit.get("completed_at_utc"),
        "evidence_as_of_utc": _iso(as_of),
        "requires_detached_probation_authorization": True,
        "requires_external_one_touch_registry": True,
        "requires_common_concurrent_portfolio_replay": True,
        "requires_evidence_calibrated_probation": True,
        "requires_exact_cell_outcome_attribution": True,
    }


def temporal_role_block_reason(validation_evidence: Mapping[str, Any]) -> Optional[str]:
    """Return why an artifact cannot occupy the funded champion book."""
    temporal = dict((validation_evidence or {}).get("temporal_lineage", {}) or {})
    release = dict((validation_evidence or {}).get("two_phase_release", {}) or {})
    if not temporal:
        return None  # Compatibility artifacts are rejected by newer artifact gates elsewhere.
    role = str(temporal.get("book_role", "") or "").upper()
    if role == PRELOCKBOX_CHAMPION_ROLE:
        evidence_sha = str(release.get("portfolio_evidence_sha256") or "").lower()
        roster_sha = str(release.get("prelockbox_roster_sha256") or "").lower()
        released = (
            release.get("release_scope") == "ALL_FROZEN_TRADEABLE_CELLS"
            and release.get("survivor_pruning_performed") is False
            and release.get("detached_authorization_required") is True
            and len(evidence_sha) == 64
            and len(roster_sha) == 64
        )
        return None if released else "pre-lockbox champion has no verified joint release marker"
    if role == PROCESS_VALIDATED_CURRENT_REFIT_ROLE:
        if str(temporal.get("contract_status", "") or "").upper() != "PROBATION_ONLY":
            return "process-validated current refit lineage contract failed"
        return (
            "process-validated current refit requires detached probation and "
            "portfolio promotion authorization"
        )
    if role != LOCKBOX_CHAMPION_ROLE:
        return f"temporal book role {role or '<missing>'} is paper/challenger only"
    if str(temporal.get("contract_status", "") or "").upper() != "PASS":
        return "temporal champion lineage contract failed"
    if not bool(temporal.get("parameters_frozen_during_final_lockbox", False)):
        return "champion parameters were not frozen during final lockbox"
    return None
