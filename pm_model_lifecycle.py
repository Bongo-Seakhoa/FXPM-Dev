"""Temporal model-book contract for validated champions and edge challengers.

The final lockbox validates a frozen parameter set through the latest evidence
cutoff.  Re-optimizing after observing that lockbox creates a *new challenger*;
it is not a refit that inherits the champion's validation.  This module keeps
the two roles explicit so an edge candidate cannot silently overwrite a funded
champion or recycle overlapping observations as fresh evidence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional


TEMPORAL_LINEAGE_VERSION = "2026-07-11-two-book-temporal-lineage-v1"
LOCKBOX_CHAMPION_ROLE = "LOCKBOX_VALIDATED_CHAMPION"
PRELOCKBOX_CHAMPION_ROLE = "FROZEN_PRELOCKBOX_CHAMPION"
EDGE_CHALLENGER_ROLE = "LATEST_DATA_EDGE_CHALLENGER"


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
    if role != LOCKBOX_CHAMPION_ROLE:
        return f"temporal book role {role or '<missing>'} is paper/challenger only"
    if str(temporal.get("contract_status", "") or "").upper() != "PASS":
        return "temporal champion lineage contract failed"
    if not bool(temporal.get("parameters_frozen_during_final_lockbox", False)):
        return "champion parameters were not frozen during final lockbox"
    return None
