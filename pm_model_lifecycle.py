"""Temporal model-book contracts for champions, challengers, and current refits.

The final lockbox validates a frozen parameter set through the latest evidence
cutoff.  Re-optimizing after observing that lockbox creates a *new challenger*;
it is not a refit that inherits the champion's validation.  This module keeps
the legacy two roles explicit so an edge candidate cannot silently overwrite a
funded champion or recycle overlapping observations as fresh evidence.  A
third, exactly hash-bound typed lineage represents a latest-cutoff rerun of a
historically validated *process*, followed by deterministic current fits of the
newly selected Winner Book.  Historical winners are evidence inputs only: they
cannot be carried forward or merely re-optimized.  The new lineage inherits
process evidence only and remains non-funded until detached probation and
portfolio authorization.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


TEMPORAL_LINEAGE_VERSION = "2026-07-11-two-book-temporal-lineage-v1"
PROCESS_REFIT_LINEAGE_VERSION = "fxpm.process-validated-current-refit-lineage.v4"
ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION = (
    "fxpm.accepted-current-process-book-generation.v2"
)
CURRENT_REFIT_RECEIPT_VERSION = "fxpm.current-refit-receipt.v5"
TYPED_RECEIPT_VERSION = "fxpm.typed-canonical-receipt.v1"
CURRENT_PROCESS_BOOK_RECEIPT_TYPE = "fxpm.current-process-book-generation"
CURRENT_REFIT_EXECUTION_RECEIPT_TYPE = "fxpm.current-refit-execution"
LOCKBOX_CHAMPION_ROLE = "LOCKBOX_VALIDATED_CHAMPION"
PRELOCKBOX_CHAMPION_ROLE = "FROZEN_PRELOCKBOX_CHAMPION"
EDGE_CHALLENGER_ROLE = "LATEST_DATA_EDGE_CHALLENGER"
PROCESS_VALIDATED_CURRENT_REFIT_ROLE = "PROCESS_VALIDATED_CURRENT_REFIT"
CURRENT_REFIT_TIMEFRAME_ORDER = ("M5", "M15", "M30", "H1", "H4", "D1")
CURRENT_REFIT_REGIMES = frozenset({"TREND", "RANGE", "BREAKOUT", "CHOP"})

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_RECEIPT_BYTES = 16_777_216
_MAX_RECEIPT_NODES = 200_000
_MAX_RECEIPT_DEPTH = 16
_MAX_RECEIPT_LIST = 4_096
_MAX_RECEIPT_STRING = 16_384
_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS = 300


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


def build_typed_receipt(
    payload: Mapping[str, Any],
    *,
    receipt_type: str,
) -> Dict[str, Any]:
    """Build a bounded canonical receipt with no fictitious trust boundary.

    FxPM's optimizer both produces and consumes these local artifacts.  A
    symmetric signing key in that same process cannot authenticate an external
    authority; it only adds an unconstructible configuration surface.  This
    wrapper therefore provides the property the local workflow can honestly
    enforce: exact type, canonical bytes, and payload identity.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("receipt payload must be a mapping")
    if not isinstance(receipt_type, str):
        raise ValueError("receipt type is invalid")
    normalized_type = receipt_type.strip()
    if not normalized_type or len(normalized_type.encode("utf-8")) > 256:
        raise ValueError("receipt type is invalid")
    payload_copy = json.loads(_canonical_json_bytes(payload).decode("utf-8"))
    payload_bytes = _canonical_json_bytes(payload_copy)
    return {
        "schema_version": TYPED_RECEIPT_VERSION,
        "receipt_type": normalized_type,
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload": payload_copy,
    }


def _open_typed_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_receipt_type: str,
) -> Tuple[Dict[str, Any], str]:
    if not isinstance(receipt, Mapping) or set(receipt) != {
        "schema_version",
        "receipt_type",
        "payload_sha256",
        "payload",
    }:
        raise ValueError("typed receipt envelope is invalid")
    if receipt.get("schema_version") != TYPED_RECEIPT_VERSION:
        raise ValueError("typed receipt schema is invalid")
    if (
        _required_text(receipt.get("receipt_type"), name="receipt_type")
        != expected_receipt_type
    ):
        raise ValueError("typed receipt type is invalid")
    payload = receipt.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("typed receipt payload is invalid")
    payload_bytes = _canonical_json_bytes(payload)
    expected_payload_sha = hashlib.sha256(payload_bytes).hexdigest()
    declared_payload_sha = receipt.get("payload_sha256")
    if (
        not isinstance(declared_payload_sha, str)
        or declared_payload_sha != declared_payload_sha.lower()
        or not _SHA256_RE.fullmatch(declared_payload_sha)
        or declared_payload_sha != expected_payload_sha
    ):
        raise ValueError("typed receipt payload digest mismatch")
    normalized_payload = json.loads(payload_bytes.decode("utf-8"))
    normalized_receipt = {
        "schema_version": TYPED_RECEIPT_VERSION,
        "receipt_type": expected_receipt_type,
        "payload_sha256": expected_payload_sha,
        "payload": normalized_payload,
    }
    return normalized_payload, canonical_receipt_sha256(normalized_receipt)


def open_typed_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_receipt_type: str,
) -> Tuple[Dict[str, Any], str]:
    """Open one exact local typed receipt and return payload plus receipt id."""

    return _open_typed_receipt(
        receipt,
        expected_receipt_type=expected_receipt_type,
    )


def _exact_keys(value: Any, expected: Sequence[str], *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(expected):
        raise ValueError(f"{name} fields are invalid")
    return value


def _required_sha(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{name} is not a SHA-256 digest")
    return value


def _required_positive_decimal_text(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a decimal string")
    normalized = value.strip()
    try:
        parsed = Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} must be positive and finite")
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


def build_lockbox_champion_lineage(
    data_window_meta: Mapping[str, Any],
) -> Dict[str, Any]:
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
    if (
        evidence_as_of is not None
        and lockbox_end is not None
        and lockbox_end > evidence_as_of
    ):
        errors.append("lockbox ends after evidence as-of cutoff")

    selection_age_days = None
    evidence_lag_days = None
    if evidence_as_of is not None and selection_cutoff is not None:
        selection_age_days = max(
            0.0, (evidence_as_of - selection_cutoff).total_seconds() / 86_400.0
        )
    if evidence_as_of is not None and lockbox_end is not None:
        evidence_lag_days = max(
            0.0, (evidence_as_of - lockbox_end).total_seconds() / 86_400.0
        )

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


def build_prelockbox_champion_lineage(
    data_window_meta: Mapping[str, Any],
) -> Dict[str, Any]:
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


def build_empty_edge_challenger_book(
    data_window_meta: Mapping[str, Any],
) -> Dict[str, Any]:
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
        "parent_champion_selection_cutoff_utc": (parent_champion or {}).get(
            "parameter_selection_cutoff_utc"
        ),
        "parent_champion_evidenced_through_utc": (parent_champion or {}).get(
            "performance_evidenced_through_utc"
        ),
    }


_ACCEPTED_BOOK_GENERATION_FIELDS = (
    "schema_version",
    "generation_id",
    "historical_process_decision_sha256",
    "process_id",
    "process_definition_sha256",
    "generation_sha256",
    "current_cutoff_utc",
    "maximum_input_timestamp_utc",
    "source_snapshot_bundle_sha256",
    "regime_params_sha256",
    "horizon_decisions_sha256",
    "full_cell_outcome_roster_sha256",
    "selected_winner_roster_sha256",
    "trial_ledger_bundle_sha256",
    "winner_bundle_sha256",
    "output_book_sha256",
    "book_multiplicity_receipt_sha256",
    "full_process_execution_trace_sha256",
    "started_at_utc",
    "completed_at_utc",
    "status",
    "selection_scope",
    "precommitted_cell_count",
    "selected_winner_count",
    "attempts",
    "identical_frozen_process_executed",
    "post_cutoff_data_used",
    "complete_precommitted_roster_processed",
    "current_winner_selection_completed",
    "historical_anchor_winner_inputs_used",
    "survivor_only_reoptimization",
    "parameter_level_oos_evidence_claimed",
    "funded_live_authority",
)

_REFIT_AUTHORIZATION_FIELDS = (
    "authorization_id",
    "historical_process_decision_sha256",
    "process_id",
    "process_definition_sha256",
    "accepted_book_generation_receipt_sha256",
    "accepted_book_generation_sha256",
    "current_cutoff_utc",
    "issued_at_utc",
    "source_snapshot_bundle_sha256",
    "regime_params_sha256",
    "horizon_decisions_sha256",
    "selected_winner_roster_sha256",
    "one_touch_nonce_sha256",
)

_REFIT_EXECUTION_FIELDS = (
    "authorization_sha256",
    "process_definition_sha256",
    "accepted_book_generation_sha256",
    "started_at_utc",
    "completed_at_utc",
    "current_cutoff_utc",
    "output_book_sha256",
    "cell_execution_bundle_sha256",
    "execution_trace_sha256",
    "cell_executions",
    "attempts",
    "status",
    "refit_process_frozen",
    "post_authorization_policy_change",
    "parameter_level_oos_evidence_claimed",
)

_REFIT_CELL_FIELDS = (
    "current_refit_cell_id",
    "historical_process_decision_sha256",
    "accepted_book_generation_receipt_sha256",
    "symbol",
    "timeframe",
    "regime",
    "strategy_id",
    "winner_process_candidate_id",
    "winner_process_sha256",
    "winner_horizon_process_sha256",
    "winner_trial_definition_sha256",
    "winner_refit_recipe_sha256",
    "winner_decision_sha256",
    "winner_book_entry_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "source_snapshot_sha256",
    "parameter_sha256",
    "output_artifact_sha256",
    "execution_trace_sha256",
    "attempts",
    "status",
    "refit_process_frozen",
    "parameter_level_oos_evidence_claimed",
)

_REFIT_CELL_IDENTITY_FIELDS = tuple(
    name
    for name in _REFIT_CELL_FIELDS
    if name
    not in {
        "current_refit_cell_id",
        "execution_trace_sha256",
        "attempts",
        "status",
        "refit_process_frozen",
        "parameter_level_oos_evidence_claimed",
    }
)

_CURRENT_REFIT_RECEIPT_FIELDS = (
    "schema_version",
    "authorization",
    "authorization_sha256",
    "execution",
    "execution_sha256",
)


def _validate_historical_process_decision(
    decision: Mapping[str, Any],
    *,
    verification_time: datetime,
) -> Dict[str, Any]:
    """Validate the stable semantic surface of a Causal process decision.

    The Causal module owns its schema.  Lifecycle deliberately does not pin that
    schema string; it binds the exact complete body hash plus the stable semantic
    identities and dependence geometry needed downstream.
    """

    if not isinstance(decision, Mapping):
        raise ValueError("historical process decision must be a mapping")
    required = {
        "authority_scope",
        "permitted_use",
        "explicit_non_authorities",
        "process_id",
        "process_definition_sha256",
        "policy_sha256",
        "precommit_receipt_sha256",
        "evidence_receipt_sha256",
        "reconciliation_receipt_sha256",
        "evaluated_at_utc",
        "metrics",
        "gates",
        "decision",
        "failure_reasons",
    }
    if not required.issubset(decision):
        raise ValueError(
            "historical process decision is missing stable semantic fields"
        )
    if (
        decision.get("authority_scope") != "OFFLINE_PROCESS_VALIDATION_ONLY"
        or decision.get("permitted_use") != "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT"
        or decision.get("decision") != "OFFLINE_PROCESS_VALIDATION_PASS"
    ):
        raise ValueError(
            "historical process decision did not pass within offline scope"
        )
    explicit_non_authorities = decision.get("explicit_non_authorities")
    required_non_authorities = {
        "CURRENT_REFIT",
        "MODEL_PROMOTION",
        "CAPITAL_ALLOCATION",
        "FUNDED_LIVE_TRADING",
        "LIVE_TRADING",
        "PROFITABILITY_CLAIM",
    }
    if not isinstance(
        explicit_non_authorities, list
    ) or not required_non_authorities.issubset(explicit_non_authorities):
        raise ValueError(
            "historical process decision exceeds its evidence-only authority"
        )
    gates = decision.get("gates")
    failures = decision.get("failure_reasons")
    if (
        not isinstance(gates, Mapping)
        or not gates
        or any(type(value) is not bool or value is not True for value in gates.values())
        or failures != []
    ):
        raise ValueError("historical process decision contains a failed evidence gate")
    metrics = decision.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("historical process decision metrics are invalid")
    integer_geometry = {}
    for name in (
        "anchor_count",
        "total_calendar_days",
        "completed_trade_count",
        "sample_size_hac_floor_calendar_days",
        "primary_hac_bandwidth_calendar_days",
        "sensitivity_hac_bandwidth_calendar_days",
        "most_influential_block_calendar_days",
        "small_cluster_degrees_of_freedom",
    ):
        value = metrics.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"historical process {name} is invalid")
        integer_geometry[name] = value
    if (
        integer_geometry["anchor_count"] < 1
        or integer_geometry["total_calendar_days"] < integer_geometry["anchor_count"]
        or integer_geometry["primary_hac_bandwidth_calendar_days"] < 1
        or integer_geometry["sensitivity_hac_bandwidth_calendar_days"]
        < integer_geometry["primary_hac_bandwidth_calendar_days"]
        or integer_geometry["most_influential_block_calendar_days"] < 1
        or integer_geometry["small_cluster_degrees_of_freedom"]
        != integer_geometry["anchor_count"] - 1
    ):
        raise ValueError("historical process dependence geometry is inconsistent")
    effective_blocks = _required_positive_decimal_text(
        metrics.get("effective_dependence_blocks"),
        name="effective_dependence_blocks",
    )
    small_cluster_critical = _required_positive_decimal_text(
        metrics.get("small_cluster_one_sided_critical_value"),
        name="small_cluster_one_sided_critical_value",
    )
    evaluated_at = _required_utc(
        decision.get("evaluated_at_utc"), name="evaluated_at_utc"
    )
    maximum_trusted_time = verification_time + timedelta(
        seconds=_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS
    )
    if evaluated_at > maximum_trusted_time:
        raise ValueError("historical process decision claims a future trusted clock")
    decision_sha = canonical_receipt_sha256(dict(decision))
    return {
        "historical_process_decision_sha256": decision_sha,
        "process_id": _required_text(decision.get("process_id"), name="process_id"),
        "process_definition_sha256": _required_sha(
            decision.get("process_definition_sha256"), name="process_definition_sha256"
        ),
        "policy_sha256": _required_sha(
            decision.get("policy_sha256"), name="policy_sha256"
        ),
        "precommit_receipt_sha256": _required_sha(
            decision.get("precommit_receipt_sha256"), name="precommit_receipt_sha256"
        ),
        "evidence_receipt_sha256": _required_sha(
            decision.get("evidence_receipt_sha256"), name="evidence_receipt_sha256"
        ),
        "reconciliation_receipt_sha256": _required_sha(
            decision.get("reconciliation_receipt_sha256"),
            name="reconciliation_receipt_sha256",
        ),
        "evaluated_at_utc": _iso(evaluated_at),
        "anchor_count": integer_geometry["anchor_count"],
        "total_calendar_days": integer_geometry["total_calendar_days"],
        "completed_trade_count": integer_geometry["completed_trade_count"],
        "effective_dependence_blocks": effective_blocks,
        "sample_size_hac_floor_calendar_days": integer_geometry[
            "sample_size_hac_floor_calendar_days"
        ],
        "primary_hac_bandwidth_calendar_days": integer_geometry[
            "primary_hac_bandwidth_calendar_days"
        ],
        "sensitivity_hac_bandwidth_calendar_days": integer_geometry[
            "sensitivity_hac_bandwidth_calendar_days"
        ],
        "most_influential_block_calendar_days": integer_geometry[
            "most_influential_block_calendar_days"
        ],
        "small_cluster_degrees_of_freedom": integer_geometry[
            "small_cluster_degrees_of_freedom"
        ],
        "small_cluster_one_sided_critical_value": small_cluster_critical,
    }


def _validate_accepted_book_generation_payload(
    payload: Mapping[str, Any],
    *,
    process: Mapping[str, Any],
    verification_time: datetime,
) -> Dict[str, Any]:
    """Validate the post-decision, full latest-cutoff process rerun.

    This receipt is not an accepted historical survivor Book.  It proves that
    the frozen process validated by the historical anchors was rerun from the
    full precommitted 2,400-cell universe at one current cutoff and produced a
    new current Winner Book.  The selected winners may therefore differ from
    every historical anchor; using any historical winner as an input is
    explicitly forbidden.
    """

    _exact_keys(
        payload,
        _ACCEPTED_BOOK_GENERATION_FIELDS,
        name="accepted book generation receipt",
    )
    if payload.get("schema_version") != ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION:
        raise ValueError("accepted book generation receipt schema is invalid")
    normalized = {
        "schema_version": ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
        "generation_id": _required_text(
            payload.get("generation_id"), name="generation_id"
        ),
        "historical_process_decision_sha256": _required_sha(
            payload.get("historical_process_decision_sha256"),
            name="historical_process_decision_sha256",
        ),
        "process_id": _required_text(payload.get("process_id"), name="process_id"),
        "process_definition_sha256": _required_sha(
            payload.get("process_definition_sha256"), name="process_definition_sha256"
        ),
        "current_cutoff_utc": _iso(
            _required_utc(payload.get("current_cutoff_utc"), name="current_cutoff_utc")
        ),
        "maximum_input_timestamp_utc": _iso(
            _required_utc(
                payload.get("maximum_input_timestamp_utc"),
                name="maximum_input_timestamp_utc",
            )
        ),
        "source_snapshot_bundle_sha256": _required_sha(
            payload.get("source_snapshot_bundle_sha256"),
            name="source_snapshot_bundle_sha256",
        ),
        "regime_params_sha256": _required_sha(
            payload.get("regime_params_sha256"), name="regime_params_sha256"
        ),
        "horizon_decisions_sha256": _required_sha(
            payload.get("horizon_decisions_sha256"), name="horizon_decisions_sha256"
        ),
        "full_cell_outcome_roster_sha256": _required_sha(
            payload.get("full_cell_outcome_roster_sha256"),
            name="full_cell_outcome_roster_sha256",
        ),
        "selected_winner_roster_sha256": _required_sha(
            payload.get("selected_winner_roster_sha256"),
            name="selected_winner_roster_sha256",
        ),
        "trial_ledger_bundle_sha256": _required_sha(
            payload.get("trial_ledger_bundle_sha256"),
            name="trial_ledger_bundle_sha256",
        ),
        "winner_bundle_sha256": _required_sha(
            payload.get("winner_bundle_sha256"), name="winner_bundle_sha256"
        ),
        "output_book_sha256": _required_sha(
            payload.get("output_book_sha256"), name="output_book_sha256"
        ),
        "book_multiplicity_receipt_sha256": _required_sha(
            payload.get("book_multiplicity_receipt_sha256"),
            name="book_multiplicity_receipt_sha256",
        ),
        "full_process_execution_trace_sha256": _required_sha(
            payload.get("full_process_execution_trace_sha256"),
            name="full_process_execution_trace_sha256",
        ),
        "started_at_utc": _iso(
            _required_utc(payload.get("started_at_utc"), name="started_at_utc")
        ),
        "completed_at_utc": _iso(
            _required_utc(payload.get("completed_at_utc"), name="completed_at_utc")
        ),
        "status": str(payload.get("status") or "").upper(),
        "selection_scope": str(payload.get("selection_scope") or "").upper(),
        "precommitted_cell_count": payload.get("precommitted_cell_count"),
        "selected_winner_count": payload.get("selected_winner_count"),
        "attempts": payload.get("attempts"),
        "identical_frozen_process_executed": payload.get(
            "identical_frozen_process_executed"
        ),
        "post_cutoff_data_used": payload.get("post_cutoff_data_used"),
        "complete_precommitted_roster_processed": payload.get(
            "complete_precommitted_roster_processed"
        ),
        "current_winner_selection_completed": payload.get(
            "current_winner_selection_completed"
        ),
        "historical_anchor_winner_inputs_used": payload.get(
            "historical_anchor_winner_inputs_used"
        ),
        "survivor_only_reoptimization": payload.get("survivor_only_reoptimization"),
        "parameter_level_oos_evidence_claimed": payload.get(
            "parameter_level_oos_evidence_claimed"
        ),
        "funded_live_authority": payload.get("funded_live_authority"),
    }
    if (
        normalized["historical_process_decision_sha256"]
        != process["historical_process_decision_sha256"]
    ):
        raise ValueError(
            "accepted book is bound to another historical process decision"
        )
    if (
        normalized["process_id"] != process["process_id"]
        or normalized["process_definition_sha256"]
        != process["process_definition_sha256"]
    ):
        raise ValueError("accepted book is bound to another global process")
    if normalized["status"] != "COMPLETE":
        raise ValueError("current full-process book generation is incomplete")
    if normalized["selection_scope"] != "FULL_CURRENT_PROCESS_ALL_PRECOMMITTED_CELLS":
        raise ValueError("current Book was not selected from the full process scope")
    precommitted_count = normalized["precommitted_cell_count"]
    selected_count = normalized["selected_winner_count"]
    if isinstance(precommitted_count, bool) or precommitted_count != 2_400:
        raise ValueError("current process did not evaluate all 2,400 cells")
    if (
        isinstance(selected_count, bool)
        or not isinstance(selected_count, int)
        or not 1 <= selected_count <= precommitted_count
    ):
        raise ValueError("current selected Winner count is invalid")
    if isinstance(normalized["attempts"], bool) or normalized["attempts"] != 1:
        raise ValueError("current full process was not executed exactly once")
    if normalized["identical_frozen_process_executed"] is not True:
        raise ValueError("current Book did not execute the validated frozen process")
    if normalized["post_cutoff_data_used"] is not False:
        raise ValueError("current Book selection used post-cutoff data")
    if normalized["complete_precommitted_roster_processed"] is not True:
        raise ValueError("current Book generation omitted the precommitted roster")
    if normalized["current_winner_selection_completed"] is not True:
        raise ValueError("current Winner and Book selection was not completed")
    if normalized["historical_anchor_winner_inputs_used"] is not False:
        raise ValueError("historical anchor winners were reused in current selection")
    if normalized["survivor_only_reoptimization"] is not False:
        raise ValueError("current Book generation used survivor-only reoptimization")
    if normalized["parameter_level_oos_evidence_claimed"] is not False:
        raise ValueError("current Book falsely claims parameter-level OOS evidence")
    if normalized["funded_live_authority"] is not False:
        raise ValueError("current Book generation cannot grant funded-live authority")
    process_evaluated = _required_utc(
        process["evaluated_at_utc"], name="historical process evaluated_at_utc"
    )
    current_cutoff = _required_utc(
        normalized["current_cutoff_utc"], name="current book current_cutoff_utc"
    )
    maximum_input = _required_utc(
        normalized["maximum_input_timestamp_utc"],
        name="current book maximum_input_timestamp_utc",
    )
    started_at = _required_utc(
        normalized["started_at_utc"], name="current book started_at_utc"
    )
    completed_at = _required_utc(
        normalized["completed_at_utc"], name="current book completed_at_utc"
    )
    if not process_evaluated <= current_cutoff <= started_at <= completed_at:
        raise ValueError(
            "current Book chronology must follow the process decision and data cutoff"
        )
    if completed_at > verification_time + timedelta(
        seconds=_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS
    ):
        raise ValueError("current Book claims a future trusted completion clock")
    if maximum_input > current_cutoff:
        raise ValueError("current Book maximum input exceeds its frozen cutoff")
    generation_identity = {
        name: normalized[name]
        for name in _ACCEPTED_BOOK_GENERATION_FIELDS
        if name != "generation_sha256"
    }
    generation_sha = _required_sha(
        payload.get("generation_sha256"), name="generation_sha256"
    )
    if generation_sha != canonical_receipt_sha256(generation_identity):
        raise ValueError("generation_sha256 does not bind the accepted book generation")
    normalized["generation_sha256"] = generation_sha
    if canonical_receipt_sha256(normalized) != canonical_receipt_sha256(dict(payload)):
        raise ValueError("accepted book generation contains a non-canonical field")
    return normalized


def compute_current_refit_cell_id(identity: Mapping[str, Any]) -> str:
    """Return the canonical identity of one independently refitted specialist."""

    exact = _exact_keys(
        identity, _REFIT_CELL_IDENTITY_FIELDS, name="refit cell identity"
    )
    return canonical_receipt_sha256(
        {"schema_version": CURRENT_REFIT_RECEIPT_VERSION, "cell_identity": dict(exact)}
    )


def _normalize_current_refit_cell(
    raw: Any,
    *,
    process_decision_sha256: str,
    book_receipt_sha256: str,
    current_cutoff: datetime,
) -> Dict[str, Any]:
    cell = _exact_keys(raw, _REFIT_CELL_FIELDS, name="current refit cell")
    symbol = _required_text(cell.get("symbol"), name="symbol", maximum=64)
    timeframe = _required_text(
        cell.get("timeframe"), name="timeframe", maximum=8
    ).upper()
    regime = _required_text(cell.get("regime"), name="regime", maximum=16).upper()
    if timeframe not in CURRENT_REFIT_TIMEFRAME_ORDER:
        raise ValueError("current refit timeframe is invalid")
    if regime not in CURRENT_REFIT_REGIMES:
        raise ValueError("current refit regime is invalid")
    fit_start = _required_utc(cell.get("fit_start_utc"), name="cell fit_start_utc")
    fit_end = _required_utc(cell.get("fit_end_utc"), name="cell fit_end_utc")
    latest_bar = _required_utc(
        cell.get("latest_closed_bar_utc"), name="cell latest_closed_bar_utc"
    )
    if not fit_start < latest_bar <= fit_end == current_cutoff:
        raise ValueError(
            "cell refit did not use its independent history through the common cutoff"
        )
    normalized = {
        "historical_process_decision_sha256": _required_sha(
            cell.get("historical_process_decision_sha256"),
            name="cell historical_process_decision_sha256",
        ),
        "accepted_book_generation_receipt_sha256": _required_sha(
            cell.get("accepted_book_generation_receipt_sha256"),
            name="cell accepted_book_generation_receipt_sha256",
        ),
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "strategy_id": _required_text(
            cell.get("strategy_id"), name="strategy_id", maximum=256
        ),
        "winner_process_candidate_id": _required_text(
            cell.get("winner_process_candidate_id"),
            name="winner_process_candidate_id",
            maximum=512,
        ),
        "winner_process_sha256": _required_sha(
            cell.get("winner_process_sha256"), name="winner_process_sha256"
        ),
        "winner_horizon_process_sha256": _required_sha(
            cell.get("winner_horizon_process_sha256"),
            name="winner_horizon_process_sha256",
        ),
        "winner_trial_definition_sha256": _required_sha(
            cell.get("winner_trial_definition_sha256"),
            name="winner_trial_definition_sha256",
        ),
        "winner_refit_recipe_sha256": _required_sha(
            cell.get("winner_refit_recipe_sha256"),
            name="winner_refit_recipe_sha256",
        ),
        "winner_decision_sha256": _required_sha(
            cell.get("winner_decision_sha256"), name="winner_decision_sha256"
        ),
        "winner_book_entry_sha256": _required_sha(
            cell.get("winner_book_entry_sha256"), name="winner_book_entry_sha256"
        ),
        "regime_fit_receipt_sha256": _required_sha(
            cell.get("regime_fit_receipt_sha256"),
            name="regime_fit_receipt_sha256",
        ),
        "regime_detector_record_sha256": _required_sha(
            cell.get("regime_detector_record_sha256"),
            name="regime_detector_record_sha256",
        ),
        "regime_detector_parameters_sha256": _required_sha(
            cell.get("regime_detector_parameters_sha256"),
            name="regime_detector_parameters_sha256",
        ),
        "regime_detector_code_sha256": _required_sha(
            cell.get("regime_detector_code_sha256"),
            name="regime_detector_code_sha256",
        ),
        "fit_start_utc": _iso(fit_start),
        "fit_end_utc": _iso(fit_end),
        "latest_closed_bar_utc": _iso(latest_bar),
        "source_snapshot_sha256": _required_sha(
            cell.get("source_snapshot_sha256"), name="source_snapshot_sha256"
        ),
        "parameter_sha256": _required_sha(
            cell.get("parameter_sha256"), name="parameter_sha256"
        ),
        "output_artifact_sha256": _required_sha(
            cell.get("output_artifact_sha256"), name="output_artifact_sha256"
        ),
    }
    if normalized["historical_process_decision_sha256"] != process_decision_sha256:
        raise ValueError(
            "current refit cell is bound to another historical process decision"
        )
    if normalized["accepted_book_generation_receipt_sha256"] != book_receipt_sha256:
        raise ValueError(
            "current refit cell is bound to another accepted book generation"
        )
    expected_id = compute_current_refit_cell_id(normalized)
    if (
        _required_sha(cell.get("current_refit_cell_id"), name="current_refit_cell_id")
        != expected_id
    ):
        raise ValueError("current_refit_cell_id does not bind the exact specialist")
    attempts = cell.get("attempts")
    if isinstance(attempts, bool) or attempts != 1:
        raise ValueError("each specialist must be refitted exactly once")
    if str(cell.get("status") or "").upper() != "COMPLETE":
        raise ValueError("current refit cell is incomplete")
    if cell.get("refit_process_frozen") is not True:
        raise ValueError("current refit cell changed the validated refit process")
    if cell.get("parameter_level_oos_evidence_claimed") is not False:
        raise ValueError(
            "a current refit cell cannot inherit parameter-level OOS evidence"
        )
    return {
        "current_refit_cell_id": expected_id,
        **normalized,
        "execution_trace_sha256": _required_sha(
            cell.get("execution_trace_sha256"), name="cell execution_trace_sha256"
        ),
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "parameter_level_oos_evidence_claimed": False,
    }


def compute_current_winner_roster_sha256(
    cells: Sequence[Mapping[str, Any]],
) -> str:
    """Hash the stable current Winner identities selected by the full rerun."""

    fields = (
        "symbol",
        "timeframe",
        "regime",
        "strategy_id",
        "winner_process_candidate_id",
        "winner_process_sha256",
        "winner_horizon_process_sha256",
        "winner_trial_definition_sha256",
        "winner_refit_recipe_sha256",
        "winner_decision_sha256",
        "winner_book_entry_sha256",
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    rows = [{name: cell[name] for name in fields} for cell in cells]
    timeframe_order = {
        timeframe: index
        for index, timeframe in enumerate(CURRENT_REFIT_TIMEFRAME_ORDER)
    }
    try:
        rows.sort(
            key=lambda row: (
                row["symbol"],
                timeframe_order[row["timeframe"]],
                row["regime"],
                row["strategy_id"],
                row["winner_decision_sha256"],
            )
        )
    except KeyError as exc:
        raise ValueError("current Winner roster contains an invalid timeframe") from exc
    return canonical_receipt_sha256({"selected_winners": rows})


def _validate_current_refit_payload(
    payload: Mapping[str, Any],
    *,
    process: Mapping[str, Any],
    book: Mapping[str, Any],
    book_receipt_sha256: str,
    as_of: datetime,
    verification_time: datetime,
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
    if (
        _required_sha(
            authorization.get("historical_process_decision_sha256"),
            name="historical_process_decision_sha256",
        )
        != process["historical_process_decision_sha256"]
    ):
        raise ValueError(
            "current refit is bound to a different historical process decision"
        )
    if (
        _required_text(authorization.get("process_id"), name="process_id")
        != process["process_id"]
    ):
        raise ValueError("current refit authorization changed the validated process")
    if (
        _required_sha(
            authorization.get("process_definition_sha256"),
            name="process_definition_sha256",
        )
        != process["process_definition_sha256"]
    ):
        raise ValueError("current refit authorization changed the validated process")
    if (
        _required_sha(
            authorization.get("accepted_book_generation_receipt_sha256"),
            name="accepted_book_generation_receipt_sha256",
        )
        != book_receipt_sha256
    ):
        raise ValueError("current refit authorization used another current Book")
    if (
        _required_sha(
            authorization.get("accepted_book_generation_sha256"),
            name="accepted_book_generation_sha256",
        )
        != book["generation_sha256"]
    ):
        raise ValueError("current refit authorization changed the current Book")

    current_cutoff = _required_utc(
        authorization.get("current_cutoff_utc"), name="current_cutoff_utc"
    )
    issued_at = _required_utc(authorization.get("issued_at_utc"), name="issued_at_utc")
    if current_cutoff != as_of:
        raise ValueError(
            "current refit authorization changed the common current cutoff"
        )
    if current_cutoff != _required_utc(
        book.get("current_cutoff_utc"), name="book current_cutoff_utc"
    ):
        raise ValueError("current refit cutoff differs from the full current process")
    book_completed = _required_utc(
        book.get("completed_at_utc"), name="book completed_at_utc"
    )
    if not current_cutoff <= book_completed <= issued_at:
        raise ValueError("current refit authorization predates current Book completion")
    normalized_authorization = {
        "authorization_id": _required_text(
            authorization.get("authorization_id"), name="authorization_id"
        ),
        "historical_process_decision_sha256": process[
            "historical_process_decision_sha256"
        ],
        "process_id": process["process_id"],
        "process_definition_sha256": process["process_definition_sha256"],
        "accepted_book_generation_receipt_sha256": book_receipt_sha256,
        "accepted_book_generation_sha256": book["generation_sha256"],
        "current_cutoff_utc": _iso(current_cutoff),
        "issued_at_utc": _iso(issued_at),
    }
    for name in (
        "source_snapshot_bundle_sha256",
        "regime_params_sha256",
        "horizon_decisions_sha256",
        "selected_winner_roster_sha256",
        "one_touch_nonce_sha256",
    ):
        normalized_authorization[name] = _required_sha(
            authorization.get(name), name=name
        )
    for name in (
        "source_snapshot_bundle_sha256",
        "regime_params_sha256",
        "horizon_decisions_sha256",
        "selected_winner_roster_sha256",
    ):
        if normalized_authorization[name] != book[name]:
            raise ValueError(f"current refit {name} differs from the current Book")
    if authorization_sha != canonical_receipt_sha256(normalized_authorization):
        raise ValueError("authorization contains a non-canonical or unbound field")

    execution = _exact_keys(
        payload.get("execution"),
        _REFIT_EXECUTION_FIELDS,
        name="current refit execution",
    )
    execution_sha = _required_sha(
        payload.get("execution_sha256"), name="execution_sha256"
    )
    if execution_sha != canonical_receipt_sha256(dict(execution)):
        raise ValueError("execution_sha256 does not bind the execution")
    if (
        _required_sha(
            execution.get("authorization_sha256"), name="execution authorization_sha256"
        )
        != authorization_sha
    ):
        raise ValueError("current refit execution used another authorization")
    if (
        _required_sha(
            execution.get("process_definition_sha256"),
            name="execution process_definition_sha256",
        )
        != process["process_definition_sha256"]
    ):
        raise ValueError("current refit execution changed the validated process")
    if (
        _required_sha(
            execution.get("accepted_book_generation_sha256"),
            name="execution accepted_book_generation_sha256",
        )
        != book["generation_sha256"]
    ):
        raise ValueError("current refit execution changed the accepted book generation")
    if (
        _required_utc(
            execution.get("current_cutoff_utc"), name="execution current_cutoff_utc"
        )
        != current_cutoff
    ):
        raise ValueError(
            "current refit cells do not share the authorized current cutoff"
        )
    started_at = _required_utc(execution.get("started_at_utc"), name="started_at_utc")
    completed_at = _required_utc(
        execution.get("completed_at_utc"), name="completed_at_utc"
    )
    if not issued_at <= started_at <= completed_at:
        raise ValueError("current refit was not authorized before execution")
    if completed_at > verification_time + timedelta(
        seconds=_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS
    ):
        raise ValueError("current refit claims a future trusted completion clock")
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
    raw_cells = execution.get("cell_executions")
    if (
        not isinstance(raw_cells, list)
        or len(raw_cells) != book["selected_winner_count"]
    ):
        raise ValueError("current refit cell execution roster is invalid")
    cells = [
        _normalize_current_refit_cell(
            item,
            process_decision_sha256=process["historical_process_decision_sha256"],
            book_receipt_sha256=book_receipt_sha256,
            current_cutoff=current_cutoff,
        )
        for item in raw_cells
    ]
    order = {
        timeframe: index
        for index, timeframe in enumerate(CURRENT_REFIT_TIMEFRAME_ORDER)
    }

    def sort_key(cell: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            cell["symbol"],
            order[cell["timeframe"]],
            cell["regime"],
            cell["strategy_id"],
            cell["current_refit_cell_id"],
        )

    if cells != sorted(cells, key=sort_key):
        raise ValueError("current refit cells are not in canonical roster order")
    cell_ids = [cell["current_refit_cell_id"] for cell in cells]
    cell_slots = [(cell["symbol"], cell["timeframe"], cell["regime"]) for cell in cells]
    if len(set(cell_ids)) != len(cell_ids) or len(set(cell_slots)) != len(cell_slots):
        raise ValueError("current refit cell roster contains duplicate specialists")
    # Process definitions may legitimately repeat across cells; the cell slot,
    # cell id, Winner decision, and Book-entry receipts carry specialization.
    # Reusing a cell-specific decision or Book entry would instead relabel one
    # accepted outcome as two experts and is therefore invalid.
    for name in ("winner_decision_sha256", "winner_book_entry_sha256"):
        identities = [cell[name] for cell in cells]
        if len(set(identities)) != len(identities):
            raise ValueError(f"current refit cells reuse {name}")
    detector_fields = (
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    detector_by_group: Dict[tuple[str, str], tuple[str, ...]] = {}
    group_by_fit: Dict[str, tuple[str, str]] = {}
    group_by_record: Dict[str, tuple[str, str]] = {}
    for cell in cells:
        group = (cell["symbol"], cell["timeframe"])
        detector = tuple(cell[name] for name in detector_fields)
        previous = detector_by_group.setdefault(group, detector)
        if previous != detector:
            raise ValueError(
                "current refit sibling regimes changed their regime detector"
            )
        for identity, seen in (
            (detector[0], group_by_fit),
            (detector[1], group_by_record),
        ):
            owner = seen.setdefault(identity, group)
            if owner != group:
                raise ValueError(
                    "current refit regime detector identity was relabelled across groups"
                )
    if (
        compute_current_winner_roster_sha256(cells)
        != normalized_authorization["selected_winner_roster_sha256"]
    ):
        raise ValueError(
            "current refit cell roster does not match the current Book roster"
        )
    cell_bundle_sha = canonical_receipt_sha256({"cell_executions": cells})
    if (
        _required_sha(
            execution.get("cell_execution_bundle_sha256"),
            name="cell_execution_bundle_sha256",
        )
        != cell_bundle_sha
    ):
        raise ValueError("cell_execution_bundle_sha256 does not bind every cell refit")
    normalized_execution = {
        "authorization_sha256": authorization_sha,
        "process_definition_sha256": process["process_definition_sha256"],
        "accepted_book_generation_sha256": book["generation_sha256"],
        "started_at_utc": _iso(started_at),
        "completed_at_utc": _iso(completed_at),
        "current_cutoff_utc": _iso(current_cutoff),
        "output_book_sha256": _required_sha(
            execution.get("output_book_sha256"), name="output_book_sha256"
        ),
        "cell_execution_bundle_sha256": cell_bundle_sha,
        "execution_trace_sha256": _required_sha(
            execution.get("execution_trace_sha256"), name="execution_trace_sha256"
        ),
        "cell_executions": cells,
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    if execution_sha != canonical_receipt_sha256(normalized_execution):
        raise ValueError("execution contains a non-canonical or unbound field")
    return {
        "authorization_id": normalized_authorization["authorization_id"],
        "authorization_sha256": authorization_sha,
        "execution_sha256": execution_sha,
        "current_cutoff_utc": normalized_authorization["current_cutoff_utc"],
        "source_snapshot_bundle_sha256": normalized_authorization[
            "source_snapshot_bundle_sha256"
        ],
        "regime_params_sha256": normalized_authorization["regime_params_sha256"],
        "horizon_decisions_sha256": normalized_authorization[
            "horizon_decisions_sha256"
        ],
        "selected_winner_roster_sha256": normalized_authorization[
            "selected_winner_roster_sha256"
        ],
        "cell_execution_bundle_sha256": cell_bundle_sha,
        "output_book_sha256": normalized_execution["output_book_sha256"],
        "completed_at_utc": normalized_execution["completed_at_utc"],
        "cells": cells,
    }


def build_process_validated_current_refit_lineage(
    *,
    historical_process_decision: Mapping[str, Any],
    accepted_book_generation_receipt: Mapping[str, Any],
    current_refit_receipt: Mapping[str, Any],
    as_of_utc: Any,
) -> Dict[str, Any]:
    """Build the probation lineage for a full current rerun and current fits.

    The historical anchors validate the frozen *process*, not their winners.
    Only after a passing Causal decision may that full process search all 2,400
    cells at the latest closed cutoff and select a new Book.  Those newly
    selected winners are then fit through that same cutoff.  Neither the Book
    nor its fitted parameters inherit parameter-level OOS evidence, so this
    function never grants funded-live eligibility.
    """
    as_of: Optional[datetime] = None
    errors = []
    process: Dict[str, Any] = {}
    book: Dict[str, Any] = {}
    refit: Dict[str, Any] = {}
    book_receipt_sha: Optional[str] = None
    refit_receipt_sha: Optional[str] = None
    try:
        as_of = _required_utc(as_of_utc, name="as_of_utc")
        verification_time = datetime.now(timezone.utc)
        process = _validate_historical_process_decision(
            historical_process_decision,
            verification_time=verification_time,
        )
        book_payload, book_receipt_sha = _open_typed_receipt(
            accepted_book_generation_receipt,
            expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
        )
        book = _validate_accepted_book_generation_payload(
            book_payload,
            process=process,
            verification_time=verification_time,
        )
        refit_payload, refit_receipt_sha = _open_typed_receipt(
            current_refit_receipt,
            expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
        )
        refit = _validate_current_refit_payload(
            refit_payload,
            process=process,
            book=book,
            book_receipt_sha256=book_receipt_sha,
            as_of=as_of,
            verification_time=verification_time,
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
        "historical_process_decision_sha256": process.get(
            "historical_process_decision_sha256"
        ),
        "process_id": process.get("process_id"),
        "process_definition_sha256": process.get("process_definition_sha256"),
        "causal_policy_sha256": process.get("policy_sha256"),
        "causal_precommit_receipt_sha256": process.get("precommit_receipt_sha256"),
        "causal_evidence_receipt_sha256": process.get("evidence_receipt_sha256"),
        "causal_reconciliation_receipt_sha256": process.get(
            "reconciliation_receipt_sha256"
        ),
        "historical_process_evaluated_at_utc": process.get("evaluated_at_utc"),
        "accepted_book_generation_receipt_sha256": book_receipt_sha,
        "accepted_book_generation_sha256": book.get("generation_sha256"),
        "current_process_output_book_sha256": book.get("output_book_sha256"),
        "current_process_full_cell_outcome_roster_sha256": book.get(
            "full_cell_outcome_roster_sha256"
        ),
        "current_process_selected_winner_roster_sha256": book.get(
            "selected_winner_roster_sha256"
        ),
        "current_process_precommitted_cell_count": book.get("precommitted_cell_count"),
        "current_process_selected_winner_count": book.get("selected_winner_count"),
        "current_process_started_at_utc": book.get("started_at_utc"),
        "current_process_completed_at_utc": book.get("completed_at_utc"),
        "causal_total_calendar_days": process.get("total_calendar_days"),
        "causal_completed_trade_count": process.get("completed_trade_count"),
        "causal_effective_dependence_blocks": process.get(
            "effective_dependence_blocks"
        ),
        "causal_primary_hac_bandwidth_calendar_days": process.get(
            "primary_hac_bandwidth_calendar_days"
        ),
        "causal_sensitivity_hac_bandwidth_calendar_days": process.get(
            "sensitivity_hac_bandwidth_calendar_days"
        ),
        "causal_most_influential_block_calendar_days": process.get(
            "most_influential_block_calendar_days"
        ),
        "causal_sample_size_hac_floor_calendar_days": process.get(
            "sample_size_hac_floor_calendar_days"
        ),
        "causal_small_cluster_degrees_of_freedom": process.get(
            "small_cluster_degrees_of_freedom"
        ),
        "causal_small_cluster_one_sided_critical_value": process.get(
            "small_cluster_one_sided_critical_value"
        ),
        "current_refit_receipt_sha256": refit_receipt_sha,
        "current_refit_authorization_sha256": refit.get("authorization_sha256"),
        "current_refit_execution_sha256": refit.get("execution_sha256"),
        "current_cutoff_utc": refit.get("current_cutoff_utc"),
        "parameter_selection_cutoff_utc": refit.get("current_cutoff_utc"),
        "current_source_snapshot_bundle_sha256": refit.get(
            "source_snapshot_bundle_sha256"
        ),
        "current_regime_params_sha256": refit.get("regime_params_sha256"),
        "current_horizon_decisions_sha256": refit.get("horizon_decisions_sha256"),
        "current_selected_winner_roster_sha256": refit.get(
            "selected_winner_roster_sha256"
        ),
        "current_cell_execution_bundle_sha256": refit.get(
            "cell_execution_bundle_sha256"
        ),
        "current_refit_cell_count": len(refit.get("cells", [])),
        "current_refit_cells": refit.get("cells", []),
        "current_refit_output_book_sha256": refit.get("output_book_sha256"),
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
        return (
            None
            if released
            else "pre-lockbox champion has no verified joint release marker"
        )
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
