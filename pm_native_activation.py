"""Strict resolver for one hash-pinned native-history activation.

The resolver never scans for CSV candidates and never falls back to legacy or
M5-resampled data.  ``data/native/ACTIVE.json`` atomically points to one
immutable activation manifest. That manifest pins exact data/audited-sidecar
hashes for every eligible symbol/timeframe cell and pins availability-only
NO_TRADE records for structurally short-history symbols.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ACTIVE_POINTER_SCHEMA = "fxpm-native-active-pointer-v1"
ACTIVATION_MANIFEST_SCHEMA = "fxpm-native-activation-v1"
NATIVE_RUN_MANIFEST_SCHEMA = "fxpm.native_history_run_manifest.v1"
NATIVE_BARS_SCHEMA = "fxpm-native-bars-v1"
NATIVE_PARITY_SCHEMA = "fxpm-native-m5-parity-v1"
NATIVE_ACQUISITION_CONTRACT = "2026-07-10-native-calendar-acquisition-v1"
AUDITED_SIDECAR_CONTRACT = "2026-07-10-native-audited-sidecar-v1"
UNIVERSE_ELIGIBILITY_SCHEMA = "fxpm-native-universe-eligibility-v2"
NATIVE_MINIMUM_SESSION_COVERAGE = 0.95
NATIVE_SESSION_COVERAGE_METHOD = "train_only_robust_weekly_session_rate_v1"
NATIVE_EVIDENCE_PARTITIONS: Tuple[str, ...] = (
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
)
NATIVE_PRODUCTION_TIMEFRAMES: Tuple[str, ...] = (
    "M5",
    "M15",
    "M30",
    "H1",
    "H4",
    "D1",
)
NATIVE_AVAILABILITY_EXCLUSION_FAILURES = frozenset({
    "partial_history",
    "no_history",
    "no_fully_closed_history",
})
NATIVE_TIMEFRAME_SECONDS: Dict[str, int] = {
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}


def classify_native_session_history(
    coverage: Any,
    *,
    minimum: float = NATIVE_MINIMUM_SESSION_COVERAGE,
) -> Tuple[str, List[str], Dict[str, Optional[float]]]:
    """Classify immutable session-history sufficiency without performance data.

    ``insufficient`` is an allowed whole-symbol NO_TRADE input. Missing,
    non-finite, or out-of-range evidence is ``invalid`` and therefore remains
    activation-blocking. The production threshold is deliberately pinned so
    an operator cannot tune the universe after inspecting the acquired data.
    """

    try:
        threshold = float(minimum)
    except (TypeError, ValueError, OverflowError):
        threshold = float("nan")
    if not math.isfinite(threshold) or not math.isclose(
        threshold,
        NATIVE_MINIMUM_SESSION_COVERAGE,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError(
            "native production session-coverage threshold must equal "
            f"{NATIVE_MINIMUM_SESSION_COVERAGE:.2f}"
        )

    normalized: Dict[str, Optional[float]] = {}
    invalid: List[str] = []
    insufficient: List[str] = []
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        raw = coverage.get(partition) if isinstance(coverage, Mapping) else None
        if isinstance(raw, bool):
            normalized[partition] = None
            invalid.append(f"SESSION_COVERAGE_UNKNOWN:{partition}")
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError, OverflowError):
            value = float("nan")
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            normalized[partition] = None
            invalid.append(f"SESSION_COVERAGE_UNKNOWN:{partition}")
            continue
        normalized[partition] = value
        if value < threshold:
            insufficient.append(
                f"SESSION_COVERAGE_LOW:{partition}:{value:.6f}<{threshold:.6f}"
            )
    if invalid:
        return "invalid", invalid, normalized
    if insufficient:
        return "insufficient", insufficient, normalized
    return "sufficient", [], normalized


def classify_native_availability_cell(
    status: Any,
    failure_status: Any,
    rows: Any,
) -> Tuple[str, Optional[str]]:
    """Classify one acquisition result using availability facts only.

    The acquisition manifest is the immutable universe input, so its compact
    result rows must be internally coherent.  In particular, an arbitrary
    failure label cannot be changed to ``no_history`` to hide a fetch, mapping,
    write, or data-quality fault from activation.
    """
    normalized_status = str(status or "").strip().lower()
    failure = str(failure_status or "").strip().lower()
    if isinstance(rows, bool):
        return "invalid", "ROWS_BOOLEAN"
    try:
        published_rows = int(rows or 0)
    except (TypeError, ValueError, OverflowError):
        return "invalid", "ROWS_INVALID"
    if published_rows < 0:
        return "invalid", "ROWS_NEGATIVE"

    if normalized_status == "success" and not failure:
        if published_rows <= 0:
            return "invalid", "SUCCESS_WITHOUT_PUBLISHED_ROWS"
        return "success", None

    if failure == "partial_history":
        if normalized_status != "partial":
            return "invalid", "PARTIAL_HISTORY_STATUS_MISMATCH"
        if published_rows <= 0:
            return "invalid", "PARTIAL_HISTORY_WITHOUT_PUBLISHED_ROWS"
        return "unavailable", None

    if failure in {"no_history", "no_fully_closed_history"}:
        if normalized_status != "failed":
            return "invalid", f"{failure.upper()}_STATUS_MISMATCH"
        if published_rows != 0:
            return "invalid", f"{failure.upper()}_WITH_PUBLISHED_ROWS"
        return "unavailable", None

    return "invalid", f"NON_AVAILABILITY_FAILURE:{failure or normalized_status or 'unknown'}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _contained_path(root: Path, relative: Any) -> Optional[Path]:
    text = str(relative or "").strip()
    if not text or Path(text).is_absolute():
        return None
    root = root.resolve()
    candidate = (root / text).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    return candidate


def _verify_pinned_file(
    root: Path,
    relative: Any,
    expected_sha256: Any,
    *,
    error_prefix: str,
) -> Tuple[Optional[Path], List[str]]:
    path = _contained_path(root, relative)
    if path is None:
        return None, [f"{error_prefix}_PATH_UNSAFE"]
    if not path.is_file() or path.is_symlink():
        return path, [f"{error_prefix}_MISSING_OR_UNSAFE"]
    expected = str(expected_sha256 or "").strip().lower()
    if not _is_sha256(expected):
        return path, [f"{error_prefix}_SHA256_INVALID"]
    if sha256_file(path).lower() != expected:
        return path, [f"{error_prefix}_SHA256_MISMATCH"]
    return path, []


def _read_evidence_object(
    path: Optional[Path],
    *,
    error_prefix: str,
) -> Tuple[Optional[Mapping[str, Any]], List[str]]:
    if path is None or not path.is_file():
        return None, []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, [f"{error_prefix}_INVALID_JSON"]
    if not isinstance(payload, Mapping):
        return None, [f"{error_prefix}_NOT_OBJECT"]
    return payload, []


def _verify_activation_cell_evidence(
    native_root: Path,
    *,
    symbol: str,
    timeframe: str,
    entry: Any,
    run_cell: Any,
    audited_cell: Any,
    expected_symbol_status: str,
    activation_id: str,
    run_id: str,
    run_manifest_relative_path: Any,
    run_manifest_sha256: Any,
    run_request: Mapping[str, Any],
    contract_minimum: float,
) -> List[str]:
    """Bind one manifest decision to the immutable acquisition/audit bytes.

    Hash pinning alone is insufficient when an operator can edit an activation
    manifest and repin ``ACTIVE.json``.  This validator therefore derives the
    cell's admissible universe state from the authenticated run result,
    provenance, audited sidecar and parity report, and requires every copied
    manifest claim to agree with those records.
    """

    cell_id = f"{symbol}|{timeframe}"
    errors: List[str] = []
    if not isinstance(entry, Mapping):
        return [f"NATIVE_CELL_EVIDENCE_ENTRY_MISSING:{cell_id}"]
    if not isinstance(run_cell, Mapping):
        return [f"NATIVE_CELL_EVIDENCE_RUN_CELL_MISSING:{cell_id}"]

    allowed_entry_fields = {
        "status",
        "audit_status",
        "universe_eligibility_status",
        "symbol",
        "timeframe",
        "run_id",
        "bars_relative_path",
        "bars_sha256",
        "run_manifest_sha256",
        "sidecar_relative_path",
        "sidecar_sha256",
        "parity_relative_path",
        "parity_sha256",
        "coverage_method",
        "expected_session_coverage_by_partition",
        "observed_session_bars_by_partition",
        "expected_session_bars_by_partition",
        "reason_codes",
    }
    if set(entry) - allowed_entry_fields:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_FIELDS_INVALID:{cell_id}")
    if str(entry.get("symbol") or "") != symbol:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_SYMBOL_MISMATCH:{cell_id}")
    if str(entry.get("timeframe") or "") != timeframe:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_TIMEFRAME_MISMATCH:{cell_id}")
    if str(entry.get("run_id") or "") != run_id:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_RUN_ID_MISMATCH:{cell_id}")
    if str(entry.get("run_manifest_sha256") or "").lower() != str(
        run_manifest_sha256 or ""
    ).lower():
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_RUN_MANIFEST_MISMATCH:{cell_id}")

    classification, coherence_error = classify_native_availability_cell(
        run_cell.get("status"),
        run_cell.get("failure_status"),
        run_cell.get("rows"),
    )
    if classification == "invalid":
        errors.append(
            f"NATIVE_CELL_EVIDENCE_RUN_AVAILABILITY_INVALID:{cell_id}:{coherence_error or 'unknown'}"
        )
    if str(run_cell.get("symbol") or "") != symbol or str(
        run_cell.get("timeframe") or ""
    ) != timeframe:
        errors.append(f"NATIVE_CELL_EVIDENCE_RUN_CELL_ID_MISMATCH:{cell_id}")
    if str(entry.get("bars_sha256") or "").lower() != str(
        run_cell.get("sha256") or ""
    ).lower():
        errors.append(f"NATIVE_CELL_EVIDENCE_RUN_BARS_MISMATCH:{cell_id}")

    sidecar_path, evidence_errors = _verify_pinned_file(
        native_root,
        entry.get("sidecar_relative_path"),
        entry.get("sidecar_sha256"),
        error_prefix=f"NATIVE_CELL_EVIDENCE_SIDECAR:{cell_id}",
    )
    errors.extend(evidence_errors)
    parity_path, evidence_errors = _verify_pinned_file(
        native_root,
        entry.get("parity_relative_path"),
        entry.get("parity_sha256"),
        error_prefix=f"NATIVE_CELL_EVIDENCE_PARITY:{cell_id}",
    )
    errors.extend(evidence_errors)
    sidecar, evidence_errors = _read_evidence_object(
        sidecar_path,
        error_prefix=f"NATIVE_CELL_EVIDENCE_SIDECAR:{cell_id}",
    )
    errors.extend(evidence_errors)
    parity, evidence_errors = _read_evidence_object(
        parity_path,
        error_prefix=f"NATIVE_CELL_EVIDENCE_PARITY:{cell_id}",
    )
    errors.extend(evidence_errors)

    provenance_relative = None
    if isinstance(sidecar, Mapping):
        provenance_relative = sidecar.get("source_provenance_relative_path")
    expected_provenance_relative = str(
        (Path(str(entry.get("bars_relative_path") or "")).parent / "provenance.json").as_posix()
    )
    if str(provenance_relative or "").replace("\\", "/") != expected_provenance_relative:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_PATH_MISMATCH:{cell_id}")
    provenance_sha = (
        sidecar.get("source_provenance_sha256")
        if isinstance(sidecar, Mapping)
        else None
    )
    provenance_path, evidence_errors = _verify_pinned_file(
        native_root,
        provenance_relative,
        provenance_sha,
        error_prefix=f"NATIVE_CELL_EVIDENCE_PROVENANCE:{cell_id}",
    )
    errors.extend(evidence_errors)
    provenance, evidence_errors = _read_evidence_object(
        provenance_path,
        error_prefix=f"NATIVE_CELL_EVIDENCE_PROVENANCE:{cell_id}",
    )
    errors.extend(evidence_errors)

    bars_path = _contained_path(native_root, entry.get("bars_relative_path"))
    failure = str(run_cell.get("failure_status") or "").strip().lower()
    bars_required = classification == "success" or (
        classification == "unavailable" and failure == "partial_history"
    )
    if bars_required:
        _bars_path, evidence_errors = _verify_pinned_file(
            native_root,
            entry.get("bars_relative_path"),
            entry.get("bars_sha256"),
            error_prefix=f"NATIVE_CELL_EVIDENCE_BARS:{cell_id}",
        )
        errors.extend(evidence_errors)
    elif bars_path is None:
        errors.append(f"NATIVE_CELL_EVIDENCE_BARS_PATH_UNSAFE:{cell_id}")
    elif bars_path.exists():
        errors.append(f"NATIVE_CELL_EVIDENCE_NO_HISTORY_BARS_PRESENT:{cell_id}")

    if not all(isinstance(value, Mapping) for value in (sidecar, parity, provenance)):
        return list(dict.fromkeys(errors))

    assert isinstance(sidecar, Mapping)
    assert isinstance(parity, Mapping)
    assert isinstance(provenance, Mapping)
    if str(sidecar.get("schema_version") or "") != NATIVE_BARS_SCHEMA:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_SCHEMA_INVALID:{cell_id}")
    if str(sidecar.get("audited_sidecar_contract") or "") != AUDITED_SIDECAR_CONTRACT:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_CONTRACT_INVALID:{cell_id}")
    if str(parity.get("schema_version") or "") != NATIVE_PARITY_SCHEMA:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_SCHEMA_INVALID:{cell_id}")
    if str(provenance.get("schema_version") or "") != NATIVE_BARS_SCHEMA:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_SCHEMA_INVALID:{cell_id}")
    for payload, label in (
        (sidecar, "SIDECAR"),
        (parity, "PARITY"),
        (provenance, "PROVENANCE"),
    ):
        if str(payload.get("symbol") or "") != symbol or str(
            payload.get("timeframe") or ""
        ) != timeframe:
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_CELL_MISMATCH:{cell_id}")
        if str(payload.get("run_id") or "") != run_id:
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_RUN_ID_MISMATCH:{cell_id}")
    if str(sidecar.get("activation_id") or "") != activation_id:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_ACTIVATION_MISMATCH:{cell_id}")
    if str(parity.get("activation_id") or "") != activation_id:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_ACTIVATION_MISMATCH:{cell_id}")
    if provenance.get("immutable_record") is not True:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_NOT_IMMUTABLE:{cell_id}")
    if str(provenance.get("acquisition_contract_version") or "") != NATIVE_ACQUISITION_CONTRACT:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_CONTRACT_INVALID:{cell_id}")
    if str(provenance.get("canonical_symbol") or "") != symbol:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_CANONICAL_SYMBOL_MISMATCH:{cell_id}")
    if provenance.get("timeframe_seconds") != NATIVE_TIMEFRAME_SECONDS.get(timeframe):
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_TIMEFRAME_SECONDS_MISMATCH:{cell_id}")
    if str(provenance.get("status") or "").lower() != str(
        run_cell.get("status") or ""
    ).lower() or str(provenance.get("failure_status") or "").lower() != failure:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_STATUS_MISMATCH:{cell_id}")
    if provenance.get("rows") != run_cell.get("rows"):
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_ROWS_MISMATCH:{cell_id}")

    for payload, label in ((sidecar, "SIDECAR"), (parity, "PARITY")):
        if str(payload.get("run_manifest_relative_path") or "").replace(
            "\\", "/"
        ) != str(run_manifest_relative_path or "").replace("\\", "/"):
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_RUN_MANIFEST_PATH_MISMATCH:{cell_id}")
        if str(payload.get("run_manifest_sha256") or "").lower() != str(
            run_manifest_sha256 or ""
        ).lower():
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_RUN_MANIFEST_SHA_MISMATCH:{cell_id}")

    expected_bars_sha = str(entry.get("bars_sha256") or "").lower()
    if str(sidecar.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_BARS_MISMATCH:{cell_id}")
    if str(parity.get("source_bars_sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_BARS_MISMATCH:{cell_id}")
    if str(provenance.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_BARS_MISMATCH:{cell_id}")
    for payload, label in ((parity, "PARITY"), (audited_cell, "AUDIT")):
        if isinstance(payload, Mapping) and str(
            payload.get("source_provenance_sha256") or ""
        ).lower() != str(provenance_sha or "").lower():
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_PROVENANCE_MISMATCH:{cell_id}")

    expected_coverage = sidecar.get("expected_session_coverage_by_partition")
    expected_observed = sidecar.get("observed_session_bars_by_partition")
    expected_counts = sidecar.get("expected_session_bars_by_partition")
    if classification == "success":
        if provenance.get("expected_session_coverage_by_partition") != expected_coverage:
            errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_COVERAGE_MISMATCH:{cell_id}")
        if provenance.get("observed_session_bars_by_partition") != expected_observed:
            errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_OBSERVED_MISMATCH:{cell_id}")
        if provenance.get("expected_session_bars_by_partition") != expected_counts:
            errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_EXPECTED_MISMATCH:{cell_id}")
    for payload, label in (
        (entry, "ENTRY"),
        (sidecar, "SIDECAR"),
        (parity, "PARITY"),
        (audited_cell, "AUDIT"),
    ):
        if not isinstance(payload, Mapping):
            continue
        if payload.get("expected_session_coverage_by_partition") != expected_coverage:
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_COVERAGE_MISMATCH:{cell_id}")
        if label != "PARITY":
            if payload.get("observed_session_bars_by_partition") != expected_observed:
                errors.append(f"NATIVE_CELL_EVIDENCE_{label}_OBSERVED_MISMATCH:{cell_id}")
            if payload.get("expected_session_bars_by_partition") != expected_counts:
                errors.append(f"NATIVE_CELL_EVIDENCE_{label}_EXPECTED_MISMATCH:{cell_id}")

    provenance_request = (
        provenance.get("request") if isinstance(provenance.get("request"), Mapping) else {}
    )
    expected_boundaries = run_request.get("common_calendar_boundaries")
    if provenance_request.get("common_calendar_boundaries") != expected_boundaries:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_BOUNDARIES_MISMATCH:{cell_id}")
    if sidecar.get("common_calendar_boundaries") != expected_boundaries:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_BOUNDARIES_MISMATCH:{cell_id}")

    coverage_details = provenance.get("expected_session_coverage_details_by_partition")
    coverage_method = (
        str(coverage_details.get("method") or "")
        if isinstance(coverage_details, Mapping)
        else ""
    )
    if str(entry.get("coverage_method") or "") != coverage_method:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_COVERAGE_METHOD_MISMATCH:{cell_id}")
    if isinstance(audited_cell, Mapping) and str(
        audited_cell.get("coverage_method") or ""
    ) != coverage_method:
        errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_COVERAGE_METHOD_MISMATCH:{cell_id}")

    entry_audit_status = str(entry.get("audit_status") or "").upper()
    if str(sidecar.get("activation_status") or "").upper() != entry_audit_status:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_STATUS_MISMATCH:{cell_id}")
    if str(parity.get("cell_activation_status") or "").upper() != entry_audit_status:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_STATUS_MISMATCH:{cell_id}")
    if sidecar.get("complete") is not (entry_audit_status == "PASS"):
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_COMPLETE_MISMATCH:{cell_id}")
    entry_reasons = list(entry.get("reason_codes") or [])
    if list(sidecar.get("activation_reason_codes") or []) != entry_reasons:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_REASON_MISMATCH:{cell_id}")
    if list(parity.get("cell_reason_codes") or []) != entry_reasons:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_REASON_MISMATCH:{cell_id}")
    if str(sidecar.get("parity_status") or "").upper() != str(
        parity.get("status") or ""
    ).upper():
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_SIDECAR_MISMATCH:{cell_id}")

    history_status, history_reasons, _normalized = classify_native_session_history(
        expected_coverage,
        minimum=contract_minimum,
    )
    derived_status = "invalid"
    if classification == "unavailable":
        derived_status = "excluded_no_trade"
    elif classification == "success" and history_status == "sufficient":
        derived_status = "eligible"
    elif classification == "success" and history_status == "insufficient":
        derived_status = "excluded_no_trade"
    if expected_symbol_status == "eligible" and derived_status != "eligible":
        errors.append(
            f"NATIVE_CELL_EVIDENCE_UNIVERSE_STATUS_MISMATCH:{cell_id}:{derived_status}!={expected_symbol_status}"
        )
    elif expected_symbol_status == "excluded_no_trade" and derived_status == "invalid":
        errors.append(
            f"NATIVE_CELL_EVIDENCE_UNIVERSE_STATUS_INVALID:{cell_id}:{derived_status}"
        )
    expected_entry_status = "active" if expected_symbol_status == "eligible" else "excluded_no_trade"
    if str(entry.get("status") or "").lower() != expected_entry_status:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_STATUS_MISMATCH:{cell_id}")
    if str(entry.get("universe_eligibility_status") or "").lower() != expected_symbol_status:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_UNIVERSE_STATUS_MISMATCH:{cell_id}")

    if classification == "success":
        if str(parity.get("status") or "").upper() != "PASS":
            errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_NOT_PASS:{cell_id}")
        expected_audit_status = "PASS" if history_status == "sufficient" else "FAIL"
        if entry_audit_status != expected_audit_status:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_STATUS_DERIVATION_MISMATCH:{cell_id}")
    if isinstance(audited_cell, Mapping):
        if classification == "unavailable":
            expected_source = "ACQUISITION_UNAVAILABLE"
            expected_parity = "NOT_APPLICABLE"
            expected_coverage_status = "NOT_APPLICABLE"
            expected_coverage_reasons: List[str] = []
        else:
            expected_source = "PASS"
            expected_parity = str(parity.get("status") or "").upper()
            expected_coverage_status = (
                "INSUFFICIENT" if history_status == "insufficient" else "PASS"
            )
            expected_coverage_reasons = history_reasons
        if str(audited_cell.get("source_validation_status") or "").upper() != expected_source:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_SOURCE_STATUS_MISMATCH:{cell_id}")
        if str(audited_cell.get("parity_status") or "").upper() != expected_parity:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_PARITY_STATUS_MISMATCH:{cell_id}")
        if str(audited_cell.get("coverage_status") or "").upper() != expected_coverage_status:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_COVERAGE_STATUS_MISMATCH:{cell_id}")
        if list(audited_cell.get("coverage_reason_codes") or []) != expected_coverage_reasons:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_COVERAGE_REASON_MISMATCH:{cell_id}")

    return list(dict.fromkeys(errors))


@dataclass
class ActiveNativeResolution:
    symbol: str
    timeframe: str
    data_path: Optional[Path] = None
    sidecar_path: Optional[Path] = None
    activation_id: Optional[str] = None
    activation_manifest_path: Optional[Path] = None
    activation_manifest_sha256: Optional[str] = None
    entry: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors and self.data_path is not None and self.sidecar_path is not None


@dataclass
class ActiveNativeUniverse:
    """Hash-pinned availability-only universe decision from one activation.

    ``eligible_symbols`` may consume native bars. ``excluded_symbols`` are
    whole-symbol NO_TRADE decisions caused only by immutable source
    availability facts.  The activation manifest hash pins both sets, so a
    performance outcome cannot silently change the optimization universe.
    """

    activation_id: Optional[str] = None
    activation_manifest_path: Optional[Path] = None
    activation_manifest_sha256: Optional[str] = None
    requested_symbols: Tuple[str, ...] = ()
    required_timeframes: Tuple[str, ...] = ()
    eligible_symbols: Tuple[str, ...] = ()
    excluded_symbols: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    entries: Dict[str, Any] = field(default_factory=dict)
    explicit_contract: bool = False
    errors: List[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors and bool(self.activation_id)

    def symbol_status(self, symbol: str) -> str:
        canonical = str(symbol or "").strip().upper()
        if canonical in self.excluded_symbols:
            return "excluded_no_trade"
        if canonical in self.eligible_symbols:
            return "eligible"
        return "unbound"

    def exclusion_binding(self, symbol: str) -> Dict[str, Any]:
        """Return the exact marker payload a ledger NO_TRADE row must carry."""
        canonical = str(symbol or "").strip().upper()
        record = dict(self.excluded_symbols.get(canonical) or {})
        if not record:
            return {}
        return {
            "schema_version": UNIVERSE_ELIGIBILITY_SCHEMA,
            "decision_basis": "source_availability_only",
            "activation_id": self.activation_id,
            "activation_manifest_sha256": self.activation_manifest_sha256,
            "symbol": canonical,
            "status": "excluded_no_trade",
            "required_timeframes": list(self.required_timeframes),
            "reason_codes": list(record.get("reason_codes") or []),
        }


def _load_active_manifest(
    data_dir: Path,
) -> Tuple[Optional[Dict[str, Any]], Optional[Path], Optional[str], List[str]]:
    """Load and authenticate ACTIVE.json plus its immutable manifest."""
    errors: List[str] = []
    native_root = (Path(data_dir) / "native").resolve()
    pointer_path = native_root / "ACTIVE.json"
    if not pointer_path.exists():
        return None, None, None, ["NATIVE_ACTIVE_POINTER_MISSING"]
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception:
        return None, None, None, ["NATIVE_ACTIVE_POINTER_INVALID_JSON"]
    if not isinstance(pointer, Mapping):
        return None, None, None, ["NATIVE_ACTIVE_POINTER_NOT_OBJECT"]
    if str(pointer.get("schema_version")) != ACTIVE_POINTER_SCHEMA:
        errors.append("NATIVE_ACTIVE_POINTER_SCHEMA_INVALID")
    manifest_path = _contained_path(native_root, pointer.get("manifest_relative_path"))
    if manifest_path is None:
        return None, None, None, errors + ["NATIVE_ACTIVE_MANIFEST_PATH_UNSAFE"]
    if not manifest_path.exists():
        return None, manifest_path, None, errors + ["NATIVE_ACTIVE_MANIFEST_MISSING"]
    observed_sha = sha256_file(manifest_path)
    if observed_sha.lower() != str(pointer.get("manifest_sha256") or "").lower():
        return (
            None,
            manifest_path,
            observed_sha,
            errors + ["NATIVE_ACTIVE_MANIFEST_SHA256_MISMATCH"],
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None, manifest_path, observed_sha, errors + ["NATIVE_ACTIVE_MANIFEST_INVALID_JSON"]
    if not isinstance(manifest, Mapping):
        return None, manifest_path, observed_sha, errors + ["NATIVE_ACTIVE_MANIFEST_NOT_OBJECT"]
    manifest = dict(manifest)
    if str(manifest.get("schema_version")) != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("NATIVE_ACTIVE_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status", "")).lower() != "active":
        errors.append("NATIVE_ACTIVE_MANIFEST_NOT_ACTIVE")
    activation_id = str(manifest.get("activation_id") or "")
    if not activation_id or activation_id != str(pointer.get("activation_id") or ""):
        errors.append("NATIVE_ACTIVE_ACTIVATION_ID_MISMATCH")
    return manifest, manifest_path, observed_sha, list(dict.fromkeys(errors))


def resolve_active_native_universe(
    data_dir: Path,
    configured_symbols: Optional[Iterable[str]] = None,
    *,
    required_timeframes: Sequence[str] = NATIVE_PRODUCTION_TIMEFRAMES,
    verify_exclusion_evidence: bool = True,
) -> ActiveNativeUniverse:
    """Resolve the availability-only eligible/excluded symbol roster.

    New activations carry an explicit ``universe_eligibility`` contract.  A
    legacy manifest without that block remains compatible only as an
    all-eligible roster; it can never manufacture an excluded symbol.
    """
    result = ActiveNativeUniverse()
    native_root = (Path(data_dir) / "native").resolve()
    manifest, manifest_path, manifest_sha, errors = _load_active_manifest(data_dir)
    result.activation_manifest_path = manifest_path
    result.activation_manifest_sha256 = manifest_sha
    result.errors.extend(errors)
    if manifest is None:
        return result
    explicit_manifest = isinstance(manifest.get("universe_eligibility"), Mapping)
    if explicit_manifest and manifest.get("native_is_signal_authority") is not True:
        result.errors.append("NATIVE_ACTIVE_SIGNAL_AUTHORITY_INVALID")
    if explicit_manifest and manifest.get("silent_fallback_allowed") is not False:
        result.errors.append("NATIVE_ACTIVE_SILENT_FALLBACK_NOT_FORBIDDEN")
    run_manifest_path: Optional[Path] = None
    run_manifest_errors: List[str] = []
    verified_run_manifest: Optional[Mapping[str, Any]] = None
    verified_run_request: Mapping[str, Any] = {}
    if explicit_manifest:
        run_manifest_path, run_manifest_errors = _verify_pinned_file(
            native_root,
            manifest.get("run_manifest_relative_path"),
            manifest.get("run_manifest_sha256"),
            error_prefix="NATIVE_ACTIVE_RUN_MANIFEST",
        )
        result.errors.extend(run_manifest_errors)
    if explicit_manifest and run_manifest_path is not None and not run_manifest_errors:
        try:
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
        except Exception:
            run_manifest = None
            result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_INVALID_JSON")
        if not isinstance(run_manifest, Mapping):
            result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_NOT_OBJECT")
        else:
            if str(run_manifest.get("schema_version") or "") != NATIVE_RUN_MANIFEST_SCHEMA:
                result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_SCHEMA_INVALID")
            if str(run_manifest.get("run_id") or "") != str(manifest.get("run_id") or ""):
                result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_ID_MISMATCH")
            if run_manifest.get("immutable_record") is not True:
                result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_NOT_IMMUTABLE")
            if run_manifest.get("complete") is not True:
                result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_NOT_COMPLETE")
            verified_run_manifest = run_manifest
            verified_run_request = (
                run_manifest.get("request")
                if isinstance(run_manifest.get("request"), Mapping)
                else {}
            )
    result.activation_id = str(manifest.get("activation_id") or "") or None
    entries = manifest.get("entries") if isinstance(manifest.get("entries"), Mapping) else {}
    result.entries = dict(entries)
    raw_entry_symbols = [str(value or "").strip() for value in entries]
    canonical_entry_symbols = [value.upper() for value in raw_entry_symbols]
    if (
        any(raw != canonical for raw, canonical in zip(raw_entry_symbols, canonical_entry_symbols))
        or len(canonical_entry_symbols) != len(set(canonical_entry_symbols))
    ):
        result.errors.append("NATIVE_UNIVERSE_ENTRY_SYMBOL_KEYS_NOT_CANONICAL_UNIQUE")
    required = tuple(
        dict.fromkeys(
            str(timeframe or "").strip().upper()
            for timeframe in required_timeframes
            if str(timeframe or "").strip()
        )
    )
    configured = tuple(
        sorted(
            {
                str(symbol or "").strip().upper()
                for symbol in (configured_symbols or ())
                if str(symbol or "").strip()
            }
        )
    )
    contract = manifest.get("universe_eligibility")
    if isinstance(contract, Mapping):
        result.explicit_contract = True
        if str(contract.get("schema_version") or "") != UNIVERSE_ELIGIBILITY_SCHEMA:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_SCHEMA_INVALID")
        if str(contract.get("decision_basis") or "") != "source_availability_only":
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_BASIS_INVALID")
        if contract.get("performance_inputs_used") is not False:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_PERFORMANCE_INPUTS_NOT_FORBIDDEN")
        try:
            contract_minimum = float(contract.get("minimum_session_coverage"))
            classify_native_session_history({}, minimum=contract_minimum)
        except (TypeError, ValueError, OverflowError):
            contract_minimum = NATIVE_MINIMUM_SESSION_COVERAGE
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_POLICY_INVALID")
        try:
            manifest_minimum = float(manifest.get("minimum_session_coverage"))
        except (TypeError, ValueError, OverflowError):
            manifest_minimum = float("nan")
        if not math.isfinite(manifest_minimum) or not math.isclose(
            manifest_minimum,
            NATIVE_MINIMUM_SESSION_COVERAGE,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            result.errors.append("NATIVE_ACTIVE_SESSION_COVERAGE_POLICY_INVALID")
        elif not math.isclose(
            manifest_minimum,
            contract_minimum,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_POLICY_MISMATCH")
        if str(contract.get("session_coverage_method") or "") != NATIVE_SESSION_COVERAGE_METHOD:
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_METHOD_INVALID")
        declared_partitions = tuple(
            str(value or "").strip()
            for value in (contract.get("session_coverage_partitions") or [])
            if str(value or "").strip()
        )
        if declared_partitions != NATIVE_EVIDENCE_PARTITIONS:
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_PARTITIONS_INVALID")
        raw_declared_required = [
            str(value or "").strip()
            for value in (contract.get("required_timeframes") or [])
            if str(value or "").strip()
        ]
        declared_required = tuple(
            str(value or "").strip().upper()
            for value in (contract.get("required_timeframes") or [])
            if str(value or "").strip()
        )
        if (
            list(declared_required) != raw_declared_required
            or len(declared_required) != len(set(declared_required))
        ):
            result.errors.append("NATIVE_UNIVERSE_REQUIRED_TIMEFRAMES_NOT_CANONICAL_UNIQUE")
        if not declared_required:
            result.errors.append("NATIVE_UNIVERSE_REQUIRED_TIMEFRAMES_MISSING")
        elif required and declared_required != required:
            result.errors.append("NATIVE_UNIVERSE_REQUIRED_TIMEFRAMES_MISMATCH")
        result.required_timeframes = declared_required or required
        raw_requested = [
            str(value or "").strip()
            for value in (contract.get("requested_symbols") or [])
            if str(value or "").strip()
        ]
        raw_eligible = [
            str(value or "").strip()
            for value in (contract.get("eligible_symbols") or [])
            if str(value or "").strip()
        ]
        requested_canonical = [value.upper() for value in raw_requested]
        eligible_canonical = [value.upper() for value in raw_eligible]
        if (
            raw_requested != requested_canonical
            or len(requested_canonical) != len(set(requested_canonical))
        ):
            result.errors.append("NATIVE_UNIVERSE_REQUESTED_SYMBOLS_NOT_CANONICAL_UNIQUE")
        if (
            raw_eligible != eligible_canonical
            or len(eligible_canonical) != len(set(eligible_canonical))
        ):
            result.errors.append("NATIVE_UNIVERSE_ELIGIBLE_SYMBOLS_NOT_CANONICAL_UNIQUE")
        requested = tuple(sorted(set(requested_canonical)))
        eligible = tuple(sorted(set(eligible_canonical)))
        raw_excluded = (
            contract.get("excluded_symbols")
            if isinstance(contract.get("excluded_symbols"), Mapping)
            else {}
        )
        raw_excluded_symbols = [str(value or "").strip() for value in raw_excluded]
        canonical_excluded_symbols = [value.upper() for value in raw_excluded_symbols]
        if (
            raw_excluded_symbols != canonical_excluded_symbols
            or len(canonical_excluded_symbols) != len(set(canonical_excluded_symbols))
        ):
            result.errors.append("NATIVE_UNIVERSE_EXCLUDED_SYMBOL_KEYS_NOT_CANONICAL_UNIQUE")
        run_cells: Dict[Tuple[str, str], Mapping[str, Any]] = {}
        if isinstance(verified_run_manifest, Mapping):
            run_request = verified_run_request
            raw_run_symbols = [
                str(value or "").strip()
                for value in (run_request.get("symbols") or [])
                if str(value or "").strip()
            ]
            raw_run_timeframes = [
                str(value or "").strip()
                for value in (run_request.get("timeframes") or [])
                if str(value or "").strip()
            ]
            if (
                raw_run_symbols != [value.upper() for value in raw_run_symbols]
                or len(raw_run_symbols) != len(set(raw_run_symbols))
            ):
                result.errors.append("NATIVE_UNIVERSE_RUN_SYMBOLS_NOT_CANONICAL_UNIQUE")
            if (
                raw_run_timeframes != [value.upper() for value in raw_run_timeframes]
                or len(raw_run_timeframes) != len(set(raw_run_timeframes))
            ):
                result.errors.append("NATIVE_UNIVERSE_RUN_TIMEFRAMES_NOT_CANONICAL_UNIQUE")
            run_symbols = {
                str(value or "").strip().upper()
                for value in (run_request.get("symbols") or [])
                if str(value or "").strip()
            }
            run_timeframes = tuple(
                str(value or "").strip().upper()
                for value in (run_request.get("timeframes") or [])
                if str(value or "").strip()
            )
            if run_symbols != set(requested):
                result.errors.append("NATIVE_UNIVERSE_RUN_SYMBOL_ROSTER_MISMATCH")
            if run_timeframes != tuple(declared_required or required):
                result.errors.append("NATIVE_UNIVERSE_RUN_TIMEFRAME_ROSTER_MISMATCH")
            run_results = verified_run_manifest.get("results")
            if not isinstance(run_results, list):
                result.errors.append("NATIVE_UNIVERSE_RUN_RESULTS_INVALID")
            else:
                for item in run_results:
                    if not isinstance(item, Mapping):
                        result.errors.append("NATIVE_UNIVERSE_RUN_RESULT_NOT_OBJECT")
                        continue
                    key = (
                        str(item.get("symbol") or "").strip().upper(),
                        str(item.get("timeframe") or "").strip().upper(),
                    )
                    if str(item.get("symbol") or "").strip() != key[0] or str(
                        item.get("timeframe") or ""
                    ).strip() != key[1]:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_RUN_CELL_ID_NOT_CANONICAL:{key[0]}|{key[1]}"
                        )
                    if key in run_cells:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_RUN_DUPLICATE_CELL:{key[0]}|{key[1]}"
                        )
                    run_cells[key] = item
                expected_run_cells = {
                    (symbol, timeframe)
                    for symbol in requested
                    for timeframe in tuple(declared_required or required)
                }
                if set(run_cells) != expected_run_cells:
                    result.errors.append("NATIVE_UNIVERSE_RUN_CELL_ROSTER_MISMATCH")
        excluded: Dict[str, Dict[str, Any]] = {}
        for raw_symbol, raw_record in raw_excluded.items():
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol or not isinstance(raw_record, Mapping):
                result.errors.append("NATIVE_UNIVERSE_EXCLUSION_RECORD_INVALID")
                continue
            record = dict(raw_record)
            allowed_record_fields = {
                "status",
                "decision_basis",
                "reason_codes",
                "availability_cells",
                "audited_sufficiency_cells",
            }
            if set(record) - allowed_record_fields:
                result.errors.append(f"NATIVE_UNIVERSE_EXCLUSION_FIELDS_INVALID:{symbol}")
            if str(record.get("status") or "") != "excluded_no_trade":
                result.errors.append(f"NATIVE_UNIVERSE_EXCLUSION_STATUS_INVALID:{symbol}")
            if str(record.get("decision_basis") or "") != "source_availability_only":
                result.errors.append(f"NATIVE_UNIVERSE_EXCLUSION_BASIS_INVALID:{symbol}")
            reasons = [str(value) for value in (record.get("reason_codes") or []) if str(value)]
            if not reasons:
                result.errors.append(f"NATIVE_UNIVERSE_EXCLUSION_REASONS_MISSING:{symbol}")
            availability_cells = (
                record.get("availability_cells")
                if isinstance(record.get("availability_cells"), Mapping)
                else {}
            )
            expected_timeframes = tuple(declared_required or required)
            if set(str(value).upper() for value in availability_cells) != set(
                expected_timeframes
            ):
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_AVAILABILITY_GRID_MISMATCH:{symbol}"
                )
            unavailable_count = 0
            expected_acquisition_reason_codes: List[str] = []
            expected_coverage_reason_codes: List[str] = []
            audited_cells = (
                record.get("audited_sufficiency_cells")
                if isinstance(record.get("audited_sufficiency_cells"), Mapping)
                else {}
            )
            if set(str(value).upper() for value in audited_cells) != set(
                expected_timeframes
            ):
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_AUDIT_GRID_MISMATCH:{symbol}"
                )
            for timeframe in expected_timeframes:
                cell = availability_cells.get(timeframe)
                if not isinstance(cell, Mapping):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_AVAILABILITY_CELL_MISSING:{symbol}|{timeframe}"
                    )
                    continue
                if set(cell) - {"status", "failure_status", "rows"}:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_AVAILABILITY_FIELDS_INVALID:{symbol}|{timeframe}"
                    )
                failure = str(cell.get("failure_status") or "").strip().lower()
                run_cell = run_cells.get((symbol, timeframe))
                if not isinstance(run_cell, Mapping) or any(
                    run_cell.get(field_name) != cell.get(field_name)
                    for field_name in ("status", "failure_status", "rows")
                ):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_RUN_BINDING_MISMATCH:{symbol}|{timeframe}"
                    )
                classification, coherence_error = classify_native_availability_cell(
                    cell.get("status"),
                    failure,
                    cell.get("rows"),
                )
                if classification == "unavailable":
                    unavailable_count += 1
                    expected_acquisition_reason_codes.append(
                        f"{timeframe}:{failure.upper()}"
                    )
                elif classification != "success":
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_NON_AVAILABILITY_REASON:{symbol}|{timeframe}:{coherence_error or 'unknown'}"
                    )
                    continue
                audit_cell = audited_cells.get(timeframe)
                if not isinstance(audit_cell, Mapping):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_AUDIT_CELL_MISSING:{symbol}|{timeframe}"
                    )
                    continue
                allowed_audit_fields = {
                    "source_validation_status",
                    "parity_status",
                    "coverage_status",
                    "coverage_reason_codes",
                    "expected_session_coverage_by_partition",
                    "observed_session_bars_by_partition",
                    "expected_session_bars_by_partition",
                    "coverage_method",
                    "source_provenance_sha256",
                    "audited_sidecar_sha256",
                    "parity_sha256",
                }
                if set(audit_cell) - allowed_audit_fields:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_AUDIT_FIELDS_INVALID:{symbol}|{timeframe}"
                    )
                source_status = str(
                    audit_cell.get("source_validation_status") or ""
                ).upper()
                parity_status = str(audit_cell.get("parity_status") or "").upper()
                coverage_status = str(audit_cell.get("coverage_status") or "").upper()
                if classification == "unavailable":
                    if source_status != "ACQUISITION_UNAVAILABLE":
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_SOURCE_STATUS_INVALID:{symbol}|{timeframe}"
                        )
                    if parity_status != "NOT_APPLICABLE" or coverage_status != "NOT_APPLICABLE":
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_UNAVAILABLE_AUDIT_INVALID:{symbol}|{timeframe}"
                        )
                    continue
                if source_status != "PASS":
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_SOURCE_STATUS_INVALID:{symbol}|{timeframe}"
                    )
                if parity_status != "PASS":
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_PARITY_STATUS_INVALID:{symbol}|{timeframe}"
                    )
                if str(audit_cell.get("coverage_method") or "") != NATIVE_SESSION_COVERAGE_METHOD:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_COVERAGE_METHOD_INVALID:{symbol}|{timeframe}"
                    )
                history_status, history_reasons, _normalized = classify_native_session_history(
                    audit_cell.get("expected_session_coverage_by_partition"),
                    minimum=contract_minimum,
                )
                expected_coverage_status = (
                    "INSUFFICIENT" if history_status == "insufficient" else "PASS"
                )
                if history_status == "invalid":
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_COVERAGE_INVALID:{symbol}|{timeframe}"
                    )
                if coverage_status != expected_coverage_status:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_COVERAGE_STATUS_INVALID:{symbol}|{timeframe}"
                    )
                recorded_coverage_reasons = [
                    str(value)
                    for value in (audit_cell.get("coverage_reason_codes") or [])
                    if str(value)
                ]
                if recorded_coverage_reasons != history_reasons:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_COVERAGE_REASON_MISMATCH:{symbol}|{timeframe}"
                    )
                if history_status == "insufficient":
                    unavailable_count += 1
                    expected_coverage_reason_codes.extend(
                        f"{timeframe}:{reason}" for reason in history_reasons
                    )
                for field_name in (
                    "observed_session_bars_by_partition",
                    "expected_session_bars_by_partition",
                ):
                    counts = audit_cell.get(field_name)
                    if not isinstance(counts, Mapping) or set(counts) != set(
                        NATIVE_EVIDENCE_PARTITIONS
                    ):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_{field_name.upper()}_INVALID:{symbol}|{timeframe}"
                        )
                        continue
                    for value in counts.values():
                        try:
                            numeric = float(value)
                        except (TypeError, ValueError, OverflowError):
                            numeric = float("nan")
                        if not math.isfinite(numeric) or numeric < 0.0:
                            result.errors.append(
                                f"NATIVE_UNIVERSE_EXCLUSION_{field_name.upper()}_INVALID:{symbol}|{timeframe}"
                            )
                            break
                for field_name in (
                    "source_provenance_sha256",
                    "audited_sidecar_sha256",
                    "parity_sha256",
                ):
                    if not _is_sha256(audit_cell.get(field_name)):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_{field_name.upper()}_INVALID:{symbol}|{timeframe}"
                        )
                symbol_entry = (
                    entries.get(symbol)
                    if isinstance(entries.get(symbol), Mapping)
                    else {}
                )
                manifest_entry = (
                    symbol_entry.get(timeframe)
                    if isinstance(symbol_entry, Mapping)
                    and isinstance(symbol_entry.get(timeframe), Mapping)
                    else {}
                )
                if str(audit_cell.get("audited_sidecar_sha256") or "").lower() != str(
                    manifest_entry.get("sidecar_sha256") or ""
                ).lower():
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_SIDECAR_BINDING_MISMATCH:{symbol}|{timeframe}"
                    )
                if str(audit_cell.get("parity_sha256") or "").lower() != str(
                    manifest_entry.get("parity_sha256") or ""
                ).lower():
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_PARITY_BINDING_MISMATCH:{symbol}|{timeframe}"
                    )
            if unavailable_count == 0:
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_HAS_NO_PROVEN_UNAVAILABLE_CELL:{symbol}"
                )
            expected_reason_codes = list(
                dict.fromkeys(
                    [
                        *expected_acquisition_reason_codes,
                        *expected_coverage_reason_codes,
                    ]
                )
            )
            if reasons != expected_reason_codes:
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_REASON_BINDING_MISMATCH:{symbol}"
                )
            excluded[symbol] = record

        requested_set = set(requested)
        eligible_set = set(eligible)
        excluded_set = set(excluded)
        if not requested_set:
            result.errors.append("NATIVE_UNIVERSE_REQUESTED_SYMBOLS_MISSING")
        if eligible_set & excluded_set:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBLE_EXCLUDED_OVERLAP")
        if eligible_set | excluded_set != requested_set:
            result.errors.append("NATIVE_UNIVERSE_PARTITION_INCOMPLETE")
        entry_symbols = {
            str(value or "").strip().upper()
            for value in entries
            if str(value or "").strip()
        }
        if entry_symbols != requested_set:
            result.errors.append("NATIVE_UNIVERSE_ENTRY_SYMBOL_ROSTER_MISMATCH")
        result.requested_symbols = requested
        result.eligible_symbols = eligible
        result.excluded_symbols = excluded
    else:
        # Backward compatibility is deliberately one-way: a legacy activation
        # can provide eligible data but cannot claim availability exclusions.
        result.explicit_contract = False
        result.required_timeframes = required
        result.requested_symbols = tuple(sorted(str(key).upper() for key in entries))
        result.eligible_symbols = result.requested_symbols
        result.excluded_symbols = {}

    if configured:
        missing = sorted(set(configured) - set(result.requested_symbols))
        if missing:
            result.errors.extend(
                f"NATIVE_UNIVERSE_SYMBOL_UNBOUND:{symbol}" for symbol in missing
            )
        extra = sorted(set(result.requested_symbols) - set(configured))
        if extra:
            result.errors.extend(
                f"NATIVE_UNIVERSE_CONFIG_ROSTER_MISSING_SYMBOL:{symbol}"
                for symbol in extra
            )
    for symbol in result.eligible_symbols:
        symbol_entries = entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        raw_timeframe_keys = [str(value or "").strip() for value in symbol_entries]
        canonical_timeframe_keys = [value.upper() for value in raw_timeframe_keys]
        if (
            raw_timeframe_keys != canonical_timeframe_keys
            or len(canonical_timeframe_keys) != len(set(canonical_timeframe_keys))
        ):
            result.errors.append(
                f"NATIVE_ACTIVE_TIMEFRAME_KEYS_NOT_CANONICAL_UNIQUE:{symbol}"
            )
        if {
            str(value or "").strip().upper()
            for value in symbol_entries
            if str(value or "").strip()
        } != set(result.required_timeframes):
            result.errors.append(f"NATIVE_ACTIVE_TIMEFRAME_ROSTER_MISMATCH:{symbol}")
        for timeframe in result.required_timeframes:
            entry = symbol_entries.get(timeframe) if isinstance(symbol_entries, Mapping) else None
            cell_id = f"{symbol}|{timeframe}"
            if result.explicit_contract:
                run_cell = run_cells.get((symbol, timeframe))
                if (
                    not isinstance(run_cell, Mapping)
                    or str(run_cell.get("status") or "").lower() != "success"
                    or str(run_cell.get("failure_status") or "").strip()
                ):
                    result.errors.append(f"NATIVE_ACTIVE_RUN_CELL_NOT_SUCCESS:{cell_id}")
            if not isinstance(entry, Mapping):
                result.errors.append(f"NATIVE_ACTIVE_CELL_MISSING:{cell_id}")
            elif str(entry.get("status") or "").lower() != "active":
                result.errors.append(f"NATIVE_ACTIVE_CELL_NOT_ACTIVE:{cell_id}")
            elif result.explicit_contract:
                if str(entry.get("bars_sha256") or "").lower() != str(
                    run_cell.get("sha256") if isinstance(run_cell, Mapping) else ""
                ).lower():
                    result.errors.append(f"NATIVE_ACTIVE_RUN_BARS_BINDING_MISMATCH:{cell_id}")
                if str(entry.get("audit_status") or "").upper() != "PASS":
                    result.errors.append(f"NATIVE_ACTIVE_CELL_AUDIT_NOT_PASS:{cell_id}")
                if str(entry.get("coverage_method") or "") != NATIVE_SESSION_COVERAGE_METHOD:
                    result.errors.append(f"NATIVE_ACTIVE_CELL_COVERAGE_METHOD_INVALID:{cell_id}")
                history_status, _history_reasons, _normalized = classify_native_session_history(
                    entry.get("expected_session_coverage_by_partition"),
                    minimum=contract_minimum,
                )
                if history_status != "sufficient":
                    result.errors.append(f"NATIVE_ACTIVE_CELL_COVERAGE_NOT_PASS:{cell_id}")
    for symbol in result.excluded_symbols:
        symbol_entries = entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        raw_timeframe_keys = [str(value or "").strip() for value in symbol_entries]
        canonical_timeframe_keys = [value.upper() for value in raw_timeframe_keys]
        if (
            raw_timeframe_keys != canonical_timeframe_keys
            or len(canonical_timeframe_keys) != len(set(canonical_timeframe_keys))
        ):
            result.errors.append(
                f"NATIVE_EXCLUDED_TIMEFRAME_KEYS_NOT_CANONICAL_UNIQUE:{symbol}"
            )
        if {
            str(value or "").strip().upper()
            for value in symbol_entries
            if str(value or "").strip()
        } != set(result.required_timeframes):
            result.errors.append(f"NATIVE_EXCLUDED_TIMEFRAME_ROSTER_MISMATCH:{symbol}")
        for timeframe in result.required_timeframes:
            entry = symbol_entries.get(timeframe) if isinstance(symbol_entries, Mapping) else None
            cell_id = f"{symbol}|{timeframe}"
            if not isinstance(entry, Mapping):
                result.errors.append(f"NATIVE_EXCLUDED_AUDIT_CELL_MISSING:{cell_id}")
            elif str(entry.get("status") or "").lower() != "excluded_no_trade":
                result.errors.append(f"NATIVE_EXCLUDED_AUDIT_CELL_STATUS_INVALID:{cell_id}")

    if result.explicit_contract and verify_exclusion_evidence:
        for symbol in result.requested_symbols:
            expected_symbol_status = (
                "eligible" if symbol in set(result.eligible_symbols) else "excluded_no_trade"
            )
            symbol_entries = entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
            excluded_record = result.excluded_symbols.get(symbol) or {}
            audited_cells = (
                excluded_record.get("audited_sufficiency_cells")
                if isinstance(excluded_record.get("audited_sufficiency_cells"), Mapping)
                else {}
            )
            for timeframe in result.required_timeframes:
                result.errors.extend(
                    _verify_activation_cell_evidence(
                        native_root,
                        symbol=symbol,
                        timeframe=timeframe,
                        entry=(
                            symbol_entries.get(timeframe)
                            if isinstance(symbol_entries, Mapping)
                            else None
                        ),
                        run_cell=run_cells.get((symbol, timeframe)),
                        audited_cell=(
                            audited_cells.get(timeframe)
                            if isinstance(audited_cells, Mapping)
                            else None
                        ),
                        expected_symbol_status=expected_symbol_status,
                        activation_id=str(result.activation_id or ""),
                        run_id=str(manifest.get("run_id") or ""),
                        run_manifest_relative_path=manifest.get(
                            "run_manifest_relative_path"
                        ),
                        run_manifest_sha256=manifest.get("run_manifest_sha256"),
                        run_request=verified_run_request,
                        contract_minimum=contract_minimum,
                    )
                )

        raw_counts = manifest.get("counts")
        if not isinstance(raw_counts, Mapping):
            result.errors.append("NATIVE_ACTIVE_COUNTS_MISSING")
        else:
            observed_status_counts = {
                "active": 0,
                "excluded_no_trade": 0,
                "pending": 0,
                "failed": 0,
            }
            for symbol_entries in entries.values():
                if not isinstance(symbol_entries, Mapping):
                    continue
                for entry in symbol_entries.values():
                    status = (
                        str(entry.get("status") or "").strip().lower()
                        if isinstance(entry, Mapping)
                        else ""
                    )
                    if status in observed_status_counts:
                        observed_status_counts[status] += 1
                    else:
                        result.errors.append(
                            f"NATIVE_ACTIVE_ENTRY_STATUS_UNKNOWN:{status or 'missing'}"
                        )
            expected_total = len(result.requested_symbols) * len(
                result.required_timeframes
            )
            expected_counts = {
                **observed_status_counts,
                "total": expected_total,
                "eligible_symbols": len(result.eligible_symbols),
                "excluded_symbols": len(result.excluded_symbols),
            }
            for field_name, expected_value in expected_counts.items():
                raw_value = raw_counts.get(field_name)
                if isinstance(raw_value, bool) or raw_value != expected_value:
                    result.errors.append(
                        f"NATIVE_ACTIVE_COUNT_MISMATCH:{field_name}"
                    )
            if sum(observed_status_counts.values()) != expected_total:
                result.errors.append("NATIVE_ACTIVE_ENTRY_STATUS_PARTITION_INCOMPLETE")
            if observed_status_counts["pending"] or observed_status_counts["failed"]:
                result.errors.append("NATIVE_ACTIVE_MANIFEST_CONTAINS_NONFINAL_CELLS")
    return result


def validate_native_exclusion_no_trade_grid(
    symbol_payload: Any,
    expected_binding: Mapping[str, Any],
    *,
    required_timeframes: Sequence[str] = NATIVE_PRODUCTION_TIMEFRAMES,
    required_regimes: Sequence[str] = ("TREND", "RANGE", "BREAKOUT", "CHOP"),
) -> List[str]:
    """Verify that an excluded symbol is a complete manifest-bound NO_TRADE grid."""
    symbol = str((expected_binding or {}).get("symbol") or "").strip().upper()
    if not isinstance(symbol_payload, Mapping):
        return [f"NATIVE_EXCLUDED_LEDGER_SYMBOL_MISSING:{symbol or 'UNKNOWN'}"]
    errors: List[str] = []
    if str(symbol_payload.get("symbol") or "").strip().upper() != symbol:
        errors.append(f"NATIVE_EXCLUDED_LEDGER_SYMBOL_MISMATCH:{symbol}")
    artifact = (
        symbol_payload.get("artifact_meta")
        if isinstance(symbol_payload.get("artifact_meta"), Mapping)
        else {}
    )
    if dict(artifact.get("native_exclusion_binding") or {}) != dict(expected_binding or {}):
        errors.append(f"NATIVE_EXCLUDED_SYMBOL_BINDING_MISMATCH:{symbol}")
    validation = (
        symbol_payload.get("validation_evidence")
        if isinstance(symbol_payload.get("validation_evidence"), Mapping)
        else {}
    )
    if dict(validation.get("native_exclusion_binding") or {}) != dict(
        expected_binding or {}
    ):
        errors.append(f"NATIVE_EXCLUDED_SYMBOL_VALIDATION_BINDING_MISMATCH:{symbol}")
    if str(symbol_payload.get("strategy_name") or "").upper() != "NO_TRADE":
        errors.append(f"NATIVE_EXCLUDED_SYMBOL_NOT_NO_TRADE:{symbol}")
    if symbol_payload.get("is_validated") is not False:
        errors.append(f"NATIVE_EXCLUDED_SYMBOL_VALIDATION_FLAG_INVALID:{symbol}")
    default_config = symbol_payload.get("default_config")
    if not isinstance(default_config, Mapping):
        errors.append(f"NATIVE_EXCLUDED_DEFAULT_CONFIG_MISSING:{symbol}")
    else:
        if str(default_config.get("strategy_name") or "").upper() != "NO_TRADE":
            errors.append(f"NATIVE_EXCLUDED_DEFAULT_CONFIG_TRADEABLE:{symbol}")
        if str(default_config.get("promotion_status") or "").upper() != "NO_TRADE":
            errors.append(f"NATIVE_EXCLUDED_DEFAULT_CONFIG_STATUS_INVALID:{symbol}")
        default_artifact = (
            default_config.get("artifact_meta")
            if isinstance(default_config.get("artifact_meta"), Mapping)
            else {}
        )
        if dict(default_artifact.get("native_exclusion_binding") or {}) != dict(
            expected_binding or {}
        ):
            errors.append(f"NATIVE_EXCLUDED_DEFAULT_CONFIG_BINDING_MISMATCH:{symbol}")
        default_validation = (
            default_config.get("validation_evidence")
            if isinstance(default_config.get("validation_evidence"), Mapping)
            else {}
        )
        if dict(default_validation.get("native_exclusion_binding") or {}) != dict(
            expected_binding or {}
        ):
            errors.append(
                f"NATIVE_EXCLUDED_DEFAULT_CONFIG_VALIDATION_BINDING_MISMATCH:{symbol}"
            )
    grid = (
        symbol_payload.get("regime_configs")
        if isinstance(symbol_payload.get("regime_configs"), Mapping)
        else {}
    )
    exact_timeframes = tuple(
        str(value or "").strip().upper() for value in required_timeframes
    )
    if tuple(grid.keys()) != exact_timeframes and set(grid.keys()) != set(
        exact_timeframes
    ):
        errors.append(f"NATIVE_EXCLUDED_TIMEFRAME_GRID_NOT_EXACT:{symbol}")
    elif any(str(key) != str(key).upper() for key in grid):
        errors.append(f"NATIVE_EXCLUDED_TIMEFRAME_GRID_NOT_CANONICAL:{symbol}")
    for raw_timeframe in required_timeframes:
        timeframe = str(raw_timeframe or "").strip().upper()
        regimes = grid.get(timeframe) if isinstance(grid.get(timeframe), Mapping) else {}
        exact_regimes = tuple(str(value or "").strip().upper() for value in required_regimes)
        if set(regimes.keys()) != set(exact_regimes):
            errors.append(f"NATIVE_EXCLUDED_REGIME_GRID_NOT_EXACT:{symbol}|{timeframe}")
        elif any(str(key) != str(key).upper() for key in regimes):
            errors.append(f"NATIVE_EXCLUDED_REGIME_GRID_NOT_CANONICAL:{symbol}|{timeframe}")
        for raw_regime in required_regimes:
            regime = str(raw_regime or "").strip().upper()
            cell_id = f"{symbol}|{timeframe}|{regime}"
            cell = regimes.get(regime) if isinstance(regimes, Mapping) else None
            if not isinstance(cell, Mapping):
                errors.append(f"NATIVE_EXCLUDED_CELL_MISSING:{cell_id}")
                continue
            if str(cell.get("strategy_name") or "").upper() != "NO_TRADE":
                errors.append(f"NATIVE_EXCLUDED_CELL_TRADEABLE:{cell_id}")
            if str(cell.get("promotion_status") or "").upper() != "NO_TRADE":
                errors.append(f"NATIVE_EXCLUDED_CELL_STATUS_INVALID:{cell_id}")
            cell_artifact = (
                cell.get("artifact_meta")
                if isinstance(cell.get("artifact_meta"), Mapping)
                else {}
            )
            if dict(cell_artifact.get("native_exclusion_binding") or {}) != dict(
                expected_binding or {}
            ):
                errors.append(f"NATIVE_EXCLUDED_CELL_BINDING_MISMATCH:{cell_id}")
            cell_validation = (
                cell.get("validation_evidence")
                if isinstance(cell.get("validation_evidence"), Mapping)
                else {}
            )
            if dict(cell_validation.get("native_exclusion_binding") or {}) != dict(
                expected_binding or {}
            ):
                errors.append(
                    f"NATIVE_EXCLUDED_CELL_VALIDATION_BINDING_MISMATCH:{cell_id}"
                )
    return errors


def resolve_active_native_artifact(
    data_dir: Path,
    symbol: str,
    timeframe: str,
) -> ActiveNativeResolution:
    """Resolve and verify one active native artifact without any fallback."""

    canonical_symbol = str(symbol or "").strip().upper()
    tf = str(timeframe or "").strip().upper()
    result = ActiveNativeResolution(symbol=canonical_symbol, timeframe=tf)
    evidence_verified = False
    native_root = (Path(data_dir) / "native").resolve()
    pointer_path = native_root / "ACTIVE.json"
    if not pointer_path.exists():
        result.errors.append("NATIVE_ACTIVE_POINTER_MISSING")
        return result
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception:
        result.errors.append("NATIVE_ACTIVE_POINTER_INVALID_JSON")
        return result
    if not isinstance(pointer, Mapping):
        result.errors.append("NATIVE_ACTIVE_POINTER_NOT_OBJECT")
        return result
    if str(pointer.get("schema_version")) != ACTIVE_POINTER_SCHEMA:
        result.errors.append("NATIVE_ACTIVE_POINTER_SCHEMA_INVALID")

    manifest_path = _contained_path(native_root, pointer.get("manifest_relative_path"))
    if manifest_path is None:
        result.errors.append("NATIVE_ACTIVE_MANIFEST_PATH_UNSAFE")
        return result
    result.activation_manifest_path = manifest_path
    if not manifest_path.exists():
        result.errors.append("NATIVE_ACTIVE_MANIFEST_MISSING")
        return result
    observed_manifest_sha = sha256_file(manifest_path)
    declared_manifest_sha = str(pointer.get("manifest_sha256") or "").lower()
    result.activation_manifest_sha256 = observed_manifest_sha
    if observed_manifest_sha.lower() != declared_manifest_sha:
        result.errors.append("NATIVE_ACTIVE_MANIFEST_SHA256_MISMATCH")
        return result
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        result.errors.append("NATIVE_ACTIVE_MANIFEST_INVALID_JSON")
        return result
    if not isinstance(manifest, Mapping):
        result.errors.append("NATIVE_ACTIVE_MANIFEST_NOT_OBJECT")
        return result
    if str(manifest.get("schema_version")) != ACTIVATION_MANIFEST_SCHEMA:
        result.errors.append("NATIVE_ACTIVE_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status", "")).lower() != "active":
        result.errors.append("NATIVE_ACTIVE_MANIFEST_NOT_ACTIVE")
    result.activation_id = str(manifest.get("activation_id") or "") or None
    if result.activation_id != str(pointer.get("activation_id") or ""):
        result.errors.append("NATIVE_ACTIVE_ACTIVATION_ID_MISMATCH")

    universe = manifest.get("universe_eligibility")
    if isinstance(universe, Mapping):
        declared_required = tuple(
            str(value or "").strip().upper()
            for value in (universe.get("required_timeframes") or [])
            if str(value or "").strip()
        )
        semantic_universe = resolve_active_native_universe(
            data_dir,
            required_timeframes=declared_required or (tf,),
            verify_exclusion_evidence=False,
        )
        if semantic_universe.errors:
            result.errors.extend(semantic_universe.errors)
            return result
        if canonical_symbol in semantic_universe.excluded_symbols:
            result.errors.append("NATIVE_ACTIVE_SYMBOL_EXCLUDED_NO_TRADE")
            return result
        if canonical_symbol not in set(semantic_universe.eligible_symbols):
            result.errors.append("NATIVE_ACTIVE_SYMBOL_NOT_ELIGIBLE")
            return result

    symbols = manifest.get("entries")
    symbol_entries = symbols.get(canonical_symbol) if isinstance(symbols, Mapping) else None
    entry = symbol_entries.get(tf) if isinstance(symbol_entries, Mapping) else None
    if not isinstance(entry, Mapping):
        result.errors.append("NATIVE_ACTIVE_CELL_MISSING")
        return result
    result.entry = dict(entry)
    if str(entry.get("status", "")).lower() != "active":
        result.errors.append("NATIVE_ACTIVE_CELL_NOT_ACTIVE")
    if str(entry.get("symbol", "")).upper() != canonical_symbol:
        result.errors.append("NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH")
    if str(entry.get("timeframe", "")).upper() != tf:
        result.errors.append("NATIVE_ACTIVE_CELL_TIMEFRAME_MISMATCH")

    if isinstance(universe, Mapping):
        run_manifest_path, evidence_errors = _verify_pinned_file(
            native_root,
            manifest.get("run_manifest_relative_path"),
            manifest.get("run_manifest_sha256"),
            error_prefix="NATIVE_ACTIVE_RUN_MANIFEST",
        )
        result.errors.extend(evidence_errors)
        run_manifest, evidence_errors = _read_evidence_object(
            run_manifest_path,
            error_prefix="NATIVE_ACTIVE_RUN_MANIFEST",
        )
        result.errors.extend(evidence_errors)
        run_cell = None
        run_request: Mapping[str, Any] = {}
        if isinstance(run_manifest, Mapping):
            run_request = (
                run_manifest.get("request")
                if isinstance(run_manifest.get("request"), Mapping)
                else {}
            )
            for candidate in run_manifest.get("results") or []:
                if not isinstance(candidate, Mapping):
                    continue
                if str(candidate.get("symbol") or "") == canonical_symbol and str(
                    candidate.get("timeframe") or ""
                ) == tf:
                    if run_cell is not None:
                        result.errors.append("NATIVE_ACTIVE_RUN_DUPLICATE_CELL")
                    run_cell = candidate
        try:
            contract_minimum = float(universe.get("minimum_session_coverage"))
        except (TypeError, ValueError, OverflowError):
            contract_minimum = float("nan")
        if not math.isfinite(contract_minimum):
            result.errors.append("NATIVE_ACTIVE_SESSION_COVERAGE_POLICY_INVALID")
        else:
            result.errors.extend(
                _verify_activation_cell_evidence(
                    native_root,
                    symbol=canonical_symbol,
                    timeframe=tf,
                    entry=entry,
                    run_cell=run_cell,
                    audited_cell=None,
                    expected_symbol_status="eligible",
                    activation_id=str(result.activation_id or ""),
                    run_id=str(manifest.get("run_id") or ""),
                    run_manifest_relative_path=manifest.get(
                        "run_manifest_relative_path"
                    ),
                    run_manifest_sha256=manifest.get("run_manifest_sha256"),
                    run_request=run_request,
                    contract_minimum=contract_minimum,
                )
            )
        if result.errors:
            return result
        evidence_verified = True

    data_path = _contained_path(native_root, entry.get("bars_relative_path"))
    sidecar_path = _contained_path(native_root, entry.get("sidecar_relative_path"))
    if data_path is None:
        result.errors.append("NATIVE_ACTIVE_BARS_PATH_UNSAFE")
    if sidecar_path is None:
        result.errors.append("NATIVE_ACTIVE_SIDECAR_PATH_UNSAFE")
    if data_path is None or sidecar_path is None:
        return result
    result.data_path = data_path
    result.sidecar_path = sidecar_path
    if not data_path.exists():
        result.errors.append("NATIVE_ACTIVE_BARS_MISSING")
    if not sidecar_path.exists():
        result.errors.append("NATIVE_ACTIVE_SIDECAR_MISSING")
    if result.errors:
        return result
    if not evidence_verified:
        if sha256_file(data_path).lower() != str(entry.get("bars_sha256") or "").lower():
            result.errors.append("NATIVE_ACTIVE_BARS_SHA256_MISMATCH")
        if sha256_file(sidecar_path).lower() != str(entry.get("sidecar_sha256") or "").lower():
            result.errors.append("NATIVE_ACTIVE_SIDECAR_SHA256_MISMATCH")
    return result
