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
import os
import re
import secrets
import threading
from collections import OrderedDict
from contextvars import ContextVar, Token
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION
from pm_native_consensus import (
    CONSENSUS_POLICY,
    CONSENSUS_SCHEMA_VERSION,
    EVIDENCE_PASS,
    EVIDENCE_PENDING,
    EVIDENCE_SEMANTIC_FAIL,
    EVIDENCE_TERMINAL_UNAVAILABLE,
    INTEGRITY_ERROR,
    INTEGRITY_PASS,
    REQUIRED_AUDIT_WINDOWS,
    build_consensus_envelope,
    build_edge_report,
    canonical_json_bytes,
    expected_reference_timeframes,
    validate_consensus_envelope,
)
from pm_native_parity import (
    CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
    CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
    ParityThresholds,
    audit_normalized_native_cross_resolution,
    normalize_ohlc_frame,
)
from pm_native_resolution import (
    RESOLUTION_AUDIT_SCHEMA_VERSION,
    audit_native_resolution,
)
from pm_native_request_contract import (
    COMMON_REQUEST_KEYS,
    PRODUCTION_SYMBOL_ORDER,
    NativeRequestContractError,
    validate_v5_terminal_technical_cell,
)


ACTIVE_POINTER_SCHEMA = "fxpm-native-active-pointer-v1"
ACTIVATION_MANIFEST_SCHEMA = "fxpm-native-activation-v1"
TERMINAL_PROMOTION_RECEIPT_SCHEMA = "fxpm-native-terminal-promotion-v2"
TERMINAL_PROMOTION_TRANSITION = "failed_complete_terminal_grid_to_active_view"
_SAFE_ACTIVATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
ACTIVE_POINTER_FIELDS = frozenset(
    {
        "schema_version",
        "activation_id",
        "manifest_relative_path",
        "manifest_sha256",
        "activated_at_utc",
        "production_authoritative",
    }
)
TERMINAL_PROMOTION_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "activation_id",
        "run_id",
        "source_manifest_relative_path",
        "source_manifest_sha256",
        "promoted_manifest_relative_path",
        "promoted_manifest_sha256",
        "previous_active_pointer_sha256",
        "counts",
        "transition",
        "data_reacquired",
        "cell_evidence_replayed",
    }
)
NATIVE_RUN_MANIFEST_SCHEMA = "fxpm.native_history_run_manifest.v1"
NATIVE_BARS_SCHEMA = "fxpm-native-bars-v1"
NATIVE_PARITY_SCHEMA = "fxpm-native-m5-parity-v1"
NATIVE_CROSS_RESOLUTION_PARITY_SCHEMA = "fxpm-native-cross-resolution-parity-v2"
NATIVE_ACQUISITION_CONTRACT = "2026-07-14-native-calendar-acquisition-v5"
AUDITED_SIDECAR_CONTRACT = "2026-07-10-native-audited-sidecar-v1"
UNIVERSE_AUTHENTICITY_SCHEMA = "fxpm-native-universe-authenticity-v4"
UNIVERSE_ELIGIBILITY_SCHEMA = "fxpm-native-universe-eligibility-v3"
LEGACY_UNIVERSE_ELIGIBILITY_SCHEMA = "fxpm-native-universe-eligibility-v2"
NATIVE_AUTHENTICITY_DECISION_BASIS = (
    "retrieval_authenticity_cadence_and_availability_only"
)
NATIVE_SOURCE_EVIDENCE_DECISION_BASIS = (
    "source_availability_integrity_and_coverage_only"
)
LEGACY_SOURCE_AVAILABILITY_DECISION_BASIS = "source_availability_only"
NATIVE_MINIMUM_SESSION_COVERAGE = 0.95
NATIVE_EVIDENCE_CANDIDATE = "evidence_candidate"
NATIVE_SOURCE_UNAVAILABLE = "source_unavailable"
NATIVE_TECHNICAL_ERROR = "technical_error"
NATIVE_PENDING = "pending"
NATIVE_TERMINAL_AUTHENTICITY_STATES = frozenset(
    {
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
    }
)
NATIVE_ALL_AUTHENTICITY_STATES = frozenset(
    {
        *NATIVE_TERMINAL_AUTHENTICITY_STATES,
        NATIVE_TECHNICAL_ERROR,
        NATIVE_PENDING,
    }
)
LEGACY_NATIVE_SESSION_COVERAGE_METHOD = "train_only_robust_weekly_session_rate_v1"
SEGMENTED_NATIVE_SESSION_COVERAGE_METHOD = "timestamp_segmented_weekly_session_rate_v2"
NATIVE_SESSION_COVERAGE_METHOD = "broker_native_cross_resolution_partition_v1"
SUPPORTED_SOURCE_TIMESTAMP_COVERAGE_METHODS = frozenset(
    {
        LEGACY_NATIVE_SESSION_COVERAGE_METHOD,
        SEGMENTED_NATIVE_SESSION_COVERAGE_METHOD,
    }
)
SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS = frozenset(
    {
        *SUPPORTED_SOURCE_TIMESTAMP_COVERAGE_METHODS,
        NATIVE_SESSION_COVERAGE_METHOD,
    }
)
LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = (
    "fxpm.native_history.timeframe_request_map.v3"
)
FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = (
    "fxpm.native_history.timeframe_request_map.v4"
)
TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = "fxpm.native_history.timeframe_request_map.v5"
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
NATIVE_PRODUCTION_SYMBOLS: Tuple[str, ...] = tuple(PRODUCTION_SYMBOL_ORDER)
PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA = (
    "fxpm.production_native_authority_verification_scope.v1"
)
_MAX_ACTIVE_POINTER_BYTES = 64 * 1024
_MAX_V5_BARS_BYTES = 256 * 1024 * 1024
_MAX_V5_NORMALIZED_FRAME_CACHE_FRAMES = 6
_MAX_V5_NORMALIZED_FRAME_CACHE_BYTES = 512 * 1024 * 1024
NATIVE_AVAILABILITY_EXCLUSION_FAILURES = frozenset(
    {
        "partial_history",
        "no_history",
        "no_fully_closed_history",
    }
)
NATIVE_TIMEFRAME_SECONDS: Dict[str, int] = {
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}


def _expected_cross_resolution_reference_map(
    timeframes: Sequence[str],
) -> Dict[str, str]:
    ordered = tuple(
        sorted(
            (str(value).upper() for value in timeframes),
            key=lambda value: NATIVE_TIMEFRAME_SECONDS[value],
        )
    )
    if len(ordered) < 2:
        return {}
    return {
        timeframe: (ordered[1] if index == 0 else ordered[index - 1])
        for index, timeframe in enumerate(ordered)
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


def validate_native_session_coverage_diagnostics(
    coverage: Any,
) -> Tuple[List[str], Dict[str, Optional[float]]]:
    """Validate coverage telemetry without making a sufficiency decision.

    Activation v4 records these ratios so later power/horizon authorities can
    use them, but no fixed ratio is allowed to turn authentic market history
    into a trading verdict.  Missing, non-finite, or out-of-range telemetry is
    still an integrity error because a consumer must never receive invented or
    malformed diagnostics.
    """

    normalized: Dict[str, Optional[float]] = {}
    errors: List[str] = []
    if not isinstance(coverage, Mapping):
        return (
            ["SESSION_COVERAGE_DIAGNOSTIC_MAP_INVALID"],
            {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS},
        )
    if set(coverage) != set(NATIVE_EVIDENCE_PARTITIONS):
        errors.append("SESSION_COVERAGE_DIAGNOSTIC_PARTITIONS_INVALID")
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        raw = coverage.get(partition)
        if isinstance(raw, bool):
            normalized[partition] = None
            errors.append(f"SESSION_COVERAGE_DIAGNOSTIC_INVALID:{partition}")
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError, OverflowError):
            value = float("nan")
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            normalized[partition] = None
            errors.append(f"SESSION_COVERAGE_DIAGNOSTIC_INVALID:{partition}")
            continue
        normalized[partition] = value
    return list(dict.fromkeys(errors)), normalized


def validate_native_source_unavailable_proof(
    provenance: Any,
    *,
    symbol: str,
    timeframe: str,
) -> List[str]:
    """Authenticate a zero-history conclusion without treating it as NO_TRADE."""

    if not isinstance(provenance, Mapping):
        return ["SOURCE_UNAVAILABLE_PROVENANCE_INVALID"]
    proof = provenance.get("source_unavailable_proof")
    if not isinstance(proof, Mapping):
        return ["SOURCE_UNAVAILABLE_FIRST_DATE_PROOF_MISSING"]
    errors: List[str] = []
    if str(proof.get("schema_version") or "") != (
        "fxpm-native-source-unavailable-proof-v1"
    ):
        errors.append("SOURCE_UNAVAILABLE_PROOF_SCHEMA_INVALID")
    if str(proof.get("symbol") or "") != str(symbol):
        errors.append("SOURCE_UNAVAILABLE_PROOF_SYMBOL_MISMATCH")
    if str(proof.get("timeframe") or "").upper() != str(timeframe).upper():
        errors.append("SOURCE_UNAVAILABLE_PROOF_TIMEFRAME_MISMATCH")
    if str(proof.get("attestation_kind") or "") != (
        "broker_server_first_date_and_exhaustive_page_probe"
    ):
        errors.append("SOURCE_UNAVAILABLE_PROOF_KIND_INVALID")
    if str(proof.get("conclusion") or "") not in {
        "NO_SERVER_HISTORY_IN_REQUESTED_INTERVAL",
        "NO_FULLY_CLOSED_SERVER_BAR_IN_REQUESTED_INTERVAL",
    }:
        errors.append("SOURCE_UNAVAILABLE_PROOF_CONCLUSION_INVALID")
    for field_name in ("source_identity_sha256", "probe_ledger_sha256"):
        if not _is_sha256(proof.get(field_name)):
            errors.append(f"SOURCE_UNAVAILABLE_PROOF_{field_name.upper()}_INVALID")
    request = provenance.get("request")
    request_start = (
        _parse_aware_utc(request.get("native_fetch_start_utc"))
        if isinstance(request, Mapping)
        else None
    )
    request_end = (
        _parse_aware_utc(request.get("native_fetch_end_exclusive_utc"))
        if isinstance(request, Mapping)
        else None
    )
    proof_start = _parse_aware_utc(proof.get("requested_start_utc"))
    proof_end = _parse_aware_utc(proof.get("requested_end_exclusive_utc"))
    queried_at = _parse_aware_utc(proof.get("queried_at_utc"))
    if None in {request_start, request_end, proof_start, proof_end, queried_at}:
        errors.append("SOURCE_UNAVAILABLE_PROOF_BOUNDARY_INVALID")
    else:
        assert request_start is not None
        assert request_end is not None
        assert proof_start is not None
        assert proof_end is not None
        assert queried_at is not None
        if proof_start != request_start or proof_end != request_end:
            errors.append("SOURCE_UNAVAILABLE_PROOF_BOUNDARY_MISMATCH")
        if queried_at < request_end:
            errors.append("SOURCE_UNAVAILABLE_PROOF_QUERY_PRECEDES_INTERVAL_END")
        first_open_raw = proof.get("broker_server_first_open_utc")
        if first_open_raw is not None and first_open_raw != "":
            first_open = _parse_aware_utc(first_open_raw)
            if first_open is None:
                errors.append("SOURCE_UNAVAILABLE_PROOF_FIRST_DATE_INVALID")
            elif first_open < request_end:
                errors.append(
                    "SOURCE_UNAVAILABLE_PROOF_FIRST_DATE_CONTRADICTS_EMPTY_INTERVAL"
                )
    if proof.get("all_requested_pages_terminal_empty") is not True:
        errors.append("SOURCE_UNAVAILABLE_PROOF_PAGE_EXHAUSTION_MISSING")
    if proof.get("tradeable") is not False:
        errors.append("SOURCE_UNAVAILABLE_PROOF_TRADEABLE_FLAG_INVALID")
    return list(dict.fromkeys(errors))


def classify_native_availability_cell(
    status: Any,
    failure_status: Any,
    rows: Any,
    resume_disposition: Any = None,
) -> Tuple[str, Optional[str]]:
    """Classify one acquisition result using availability facts only.

    The acquisition manifest is the immutable universe input, so its compact
    result rows must be internally coherent.  In particular, an arbitrary
    failure label cannot be changed to ``no_history`` to hide a fetch, mapping,
    write, or data-quality fault from activation.

    ``success`` means that a hash-valid bar artifact exists and may proceed to
    the cell-specific evidence audit.  It does *not* mean the originally
    requested leading warmup span is sufficient. New acquisitions distinguish
    an auditable ``boundary_history_shortfall`` from legacy undifferentiated
    ``partial_history`` and from technical internal-page defects.
    """
    normalized_status = str(status or "").strip().lower()
    failure = str(failure_status or "").strip().lower()
    disposition = str(resume_disposition or "").strip().lower()
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

    if failure == "boundary_history_shortfall":
        if normalized_status != "partial":
            return "invalid", "BOUNDARY_HISTORY_SHORTFALL_STATUS_MISMATCH"
        if published_rows <= 0:
            return "invalid", "BOUNDARY_HISTORY_SHORTFALL_WITHOUT_PUBLISHED_ROWS"
        return "success", None

    if failure in {"no_history", "no_fully_closed_history"}:
        if normalized_status != "failed":
            return "invalid", f"{failure.upper()}_STATUS_MISMATCH"
        if published_rows != 0:
            return "invalid", f"{failure.upper()}_WITH_PUBLISHED_ROWS"
        return "unavailable", None

    if failure == "technical_history_gap" and disposition == "terminal_technical":
        if normalized_status != "failed":
            return "invalid", "TERMINAL_TECHNICAL_STATUS_MISMATCH"
        if published_rows != 0:
            return "invalid", "TERMINAL_TECHNICAL_WITH_PUBLISHED_ROWS"
        return "terminal_technical", None

    return (
        "invalid",
        f"NON_AVAILABILITY_FAILURE:{failure or normalized_status or 'unknown'}",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_file_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def validate_terminal_promotion_view(
    native_root: Path,
    *,
    activation_id: str,
    view_path: Path,
    view_sha256: str,
) -> List[str]:
    """Authenticate the sole immutable failed-to-active manifest view.

    This helper never grants authority from a receipt alone.  It requires the
    exact canonical source bytes, a canonical view that differs only by the
    ``status`` transition, and a canonical receipt binding both byte streams to
    the source manifest's predecessor-pointer commitment.
    """

    errors: List[str] = []
    root = Path(native_root).resolve()
    identity = str(activation_id or "").strip()
    if _SAFE_ACTIVATION_ID.fullmatch(identity) is None:
        return ["NATIVE_TERMINAL_PROMOTION_ACTIVATION_ID_INVALID"]
    source_relative = f"activations/{identity}.json"
    promoted_relative = f"activations/promoted/{identity}.json"
    source_path = _contained_path(root, source_relative)
    expected_view_path = _contained_path(root, promoted_relative)
    receipt_path = _contained_path(
        root,
        f"transition_receipts/{identity}.json",
    )
    try:
        supplied_view_path = Path(view_path).resolve()
    except (OSError, RuntimeError, ValueError):
        supplied_view_path = Path(view_path)
    if expected_view_path is None or supplied_view_path != expected_view_path:
        return ["NATIVE_TERMINAL_PROMOTION_VIEW_PATH_IDENTITY_MISMATCH"]
    if source_path is None or receipt_path is None:
        return ["NATIVE_TERMINAL_PROMOTION_ARTIFACT_MISSING_OR_UNSAFE"]
    if (
        not source_path.is_file()
        or source_path.is_symlink()
        or not expected_view_path.is_file()
        or expected_view_path.is_symlink()
        or not receipt_path.is_file()
        or receipt_path.is_symlink()
    ):
        return ["NATIVE_TERMINAL_PROMOTION_ARTIFACT_MISSING_OR_UNSAFE"]

    try:
        source_bytes = source_path.read_bytes()
        promoted_bytes = expected_view_path.read_bytes()
        receipt_bytes = receipt_path.read_bytes()
        source = json.loads(source_bytes.decode("utf-8"))
        receipt = json.loads(receipt_bytes.decode("utf-8"))
    except Exception:
        return ["NATIVE_TERMINAL_PROMOTION_ARTIFACT_INVALID_JSON"]
    if not isinstance(source, Mapping) or not isinstance(receipt, Mapping):
        return ["NATIVE_TERMINAL_PROMOTION_ARTIFACT_NOT_OBJECT"]
    source = dict(source)
    receipt = dict(receipt)
    try:
        canonical_source_bytes = _canonical_json_file_bytes(source)
        canonical_receipt_bytes = _canonical_json_file_bytes(receipt)
    except (TypeError, ValueError, OverflowError):
        return ["NATIVE_TERMINAL_PROMOTION_ARTIFACT_NOT_CANONICAL"]
    if source_bytes != canonical_source_bytes:
        errors.append("NATIVE_TERMINAL_PROMOTION_SOURCE_NOT_CANONICAL")
    if receipt_bytes != canonical_receipt_bytes:
        errors.append("NATIVE_TERMINAL_PROMOTION_RECEIPT_NOT_CANONICAL")
    if set(receipt) != set(TERMINAL_PROMOTION_RECEIPT_FIELDS):
        errors.append("NATIVE_TERMINAL_PROMOTION_RECEIPT_FIELDS_INVALID")
    if str(receipt.get("schema_version") or "") != TERMINAL_PROMOTION_RECEIPT_SCHEMA:
        errors.append("NATIVE_TERMINAL_PROMOTION_RECEIPT_SCHEMA_INVALID")
    if str(receipt.get("transition") or "") != TERMINAL_PROMOTION_TRANSITION:
        errors.append("NATIVE_TERMINAL_PROMOTION_TRANSITION_INVALID")

    observed_source_sha = hashlib.sha256(source_bytes).hexdigest()
    observed_promoted_sha = hashlib.sha256(promoted_bytes).hexdigest()
    declared_view_sha = str(view_sha256 or "").strip().lower()
    for value, label in (
        (receipt.get("source_manifest_sha256"), "SOURCE"),
        (receipt.get("promoted_manifest_sha256"), "PROMOTED"),
        (receipt.get("previous_active_pointer_sha256"), "PREDECESSOR_POINTER"),
        (declared_view_sha, "POINTER_PROMOTED"),
    ):
        if not _is_sha256(value):
            errors.append(f"NATIVE_TERMINAL_PROMOTION_{label}_SHA256_INVALID")
    if str(receipt.get("source_manifest_relative_path") or "") != source_relative:
        errors.append("NATIVE_TERMINAL_PROMOTION_SOURCE_PATH_MISMATCH")
    if str(receipt.get("promoted_manifest_relative_path") or "") != promoted_relative:
        errors.append("NATIVE_TERMINAL_PROMOTION_VIEW_PATH_MISMATCH")
    if str(receipt.get("source_manifest_sha256") or "").lower() != (
        observed_source_sha
    ):
        errors.append("NATIVE_TERMINAL_PROMOTION_SOURCE_SHA256_MISMATCH")
    if (
        str(receipt.get("promoted_manifest_sha256") or "").lower()
        != observed_promoted_sha
        or declared_view_sha != observed_promoted_sha
    ):
        errors.append("NATIVE_TERMINAL_PROMOTION_VIEW_SHA256_MISMATCH")

    if str(source.get("schema_version") or "") != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("NATIVE_TERMINAL_PROMOTION_SOURCE_SCHEMA_INVALID")
    if str(source.get("status") or "").lower() != "failed":
        errors.append("NATIVE_TERMINAL_PROMOTION_SOURCE_STATUS_INVALID")
    if str(source.get("activation_id") or "") != identity:
        errors.append("NATIVE_TERMINAL_PROMOTION_ACTIVATION_ID_MISMATCH")
    run_id = str(source.get("run_id") or "")
    if not run_id or str(receipt.get("run_id") or "") != run_id:
        errors.append("NATIVE_TERMINAL_PROMOTION_RUN_ID_MISMATCH")
    if str(receipt.get("activation_id") or "") != identity:
        errors.append("NATIVE_TERMINAL_PROMOTION_RECEIPT_ACTIVATION_ID_MISMATCH")
    if not isinstance(source.get("counts"), Mapping) or receipt.get("counts") != (
        source.get("counts")
    ):
        errors.append("NATIVE_TERMINAL_PROMOTION_COUNTS_MISMATCH")
    predecessor_sha = str(source.get("previous_active_pointer_sha256") or "").lower()
    if (
        not _is_sha256(predecessor_sha)
        or str(receipt.get("previous_active_pointer_sha256") or "").lower()
        != predecessor_sha
    ):
        errors.append("NATIVE_TERMINAL_PROMOTION_PREDECESSOR_POINTER_MISMATCH")
    if receipt.get("data_reacquired") is not False:
        errors.append("NATIVE_TERMINAL_PROMOTION_REACQUIRE_FLAG_INVALID")
    if receipt.get("cell_evidence_replayed") is not False:
        errors.append("NATIVE_TERMINAL_PROMOTION_REPLAY_FLAG_INVALID")

    promoted = dict(source)
    promoted["status"] = "active"
    try:
        expected_promoted_bytes = _canonical_json_file_bytes(promoted)
    except (TypeError, ValueError, OverflowError):
        errors.append("NATIVE_TERMINAL_PROMOTION_VIEW_NOT_CANONICAL")
    else:
        if promoted_bytes != expected_promoted_bytes:
            errors.append("NATIVE_TERMINAL_PROMOTION_VIEW_CONTENT_MISMATCH")
    return list(dict.fromkeys(errors))


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


def _parse_aware_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validated_partition_boundaries(
    raw_boundaries: Any,
    *,
    error_prefix: str,
    as_of_ts: Optional[datetime],
    requested_start_ts: Optional[datetime],
) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    errors: List[str] = []
    boundaries: Dict[str, Dict[str, str]] = {}
    if not isinstance(raw_boundaries, Mapping) or set(raw_boundaries) != set(
        NATIVE_EVIDENCE_PARTITIONS
    ):
        return {}, [f"{error_prefix}_BOUNDARIES_INVALID"]
    previous_end: Optional[datetime] = None
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        raw_window = raw_boundaries.get(partition)
        if not isinstance(raw_window, Mapping) or set(raw_window) != {
            "start_ts",
            "end_ts",
        }:
            errors.append(f"{error_prefix}_WINDOW_INVALID:{partition}")
            continue
        raw_window_start = str(raw_window.get("start_ts") or "").strip()
        raw_window_end = str(raw_window.get("end_ts") or "").strip()
        window_start = _parse_aware_utc(raw_window_start)
        window_end = _parse_aware_utc(raw_window_end)
        if (
            window_start is None
            or window_end is None
            or window_start >= window_end
            or (previous_end is not None and window_start < previous_end)
        ):
            errors.append(f"{error_prefix}_WINDOW_INVALID:{partition}")
            continue
        boundaries[partition] = {
            "start_ts": raw_window_start,
            "end_ts": raw_window_end,
        }
        previous_end = window_end
    final_end = (
        _parse_aware_utc(boundaries.get("final_lockbox", {}).get("end_ts"))
        if boundaries
        else None
    )
    if as_of_ts is not None and final_end != as_of_ts:
        errors.append(f"{error_prefix}_AS_OF_BOUNDARY_MISMATCH")
    optimizer_start = (
        _parse_aware_utc(boundaries.get("optimizer_train", {}).get("start_ts"))
        if boundaries
        else None
    )
    if (
        requested_start_ts is not None
        and optimizer_start is not None
        and requested_start_ts > optimizer_start
    ):
        errors.append(f"{error_prefix}_WARMUP_RANGE_INVALID")
    return boundaries, errors


def _validated_calendar_contract(
    request: Mapping[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    """Authenticate legacy flat or v3 timeframe-specific calendar windows."""
    errors: List[str] = []
    common_as_of = str(request.get("common_as_of_utc") or "").strip()
    common_start = str(request.get("common_requested_start_utc") or "").strip()
    as_of_ts = _parse_aware_utc(common_as_of)
    start_ts = _parse_aware_utc(common_start)
    if as_of_ts is None:
        errors.append("NATIVE_UNIVERSE_CALENDAR_AS_OF_INVALID")
    if start_ts is None:
        errors.append("NATIVE_UNIVERSE_CALENDAR_REQUESTED_START_INVALID")
    elif as_of_ts is not None and start_ts >= as_of_ts:
        errors.append("NATIVE_UNIVERSE_CALENDAR_REQUESTED_RANGE_INVALID")

    boundaries, boundary_errors = _validated_partition_boundaries(
        request.get("common_calendar_boundaries"),
        error_prefix="NATIVE_UNIVERSE_CALENDAR",
        as_of_ts=as_of_ts,
        requested_start_ts=start_ts,
    )
    errors.extend(boundary_errors)

    version = str(request.get("window_contract_version") or "").strip()
    timeframe_boundaries: Dict[str, Dict[str, Dict[str, str]]] = {}
    if version and version != TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        errors.append("NATIVE_UNIVERSE_TIMEFRAME_CALENDAR_VERSION_INVALID")
    elif version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        raw_timeframes = [
            str(value or "").strip()
            for value in (request.get("timeframes") or [])
            if str(value or "").strip()
        ]
        if (
            not raw_timeframes
            or raw_timeframes != [value.upper() for value in raw_timeframes]
            or len(raw_timeframes) != len(set(raw_timeframes))
        ):
            errors.append("NATIVE_UNIVERSE_TIMEFRAME_CALENDAR_ROSTER_INVALID")
        roster = tuple(raw_timeframes)
        raw_map = request.get("timeframe_calendar_boundaries")
        if not isinstance(raw_map, Mapping) or set(raw_map) != set(roster):
            errors.append("NATIVE_UNIVERSE_TIMEFRAME_CALENDAR_MAP_INVALID")
            raw_map = {}
        for timeframe in roster:
            normalized, cell_errors = _validated_partition_boundaries(
                raw_map.get(timeframe),
                error_prefix=f"NATIVE_UNIVERSE_TIMEFRAME_CALENDAR:{timeframe}",
                as_of_ts=as_of_ts,
                requested_start_ts=start_ts,
            )
            timeframe_boundaries[timeframe] = normalized
            errors.extend(cell_errors)

        request_map_version = str(
            request.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version not in {
            LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
        }:
            errors.append("NATIVE_UNIVERSE_TIMEFRAME_REQUEST_MAP_SCHEMA_INVALID")
        if str(request.get("timeframe_calendar_boundaries_sha256") or "") != (
            _canonical_json_sha256(raw_map)
        ):
            errors.append("NATIVE_UNIVERSE_TIMEFRAME_CALENDAR_HASH_INVALID")

        request_map_fields = {
            "schema_version": request.get("timeframe_request_map_schema_version"),
            "window_contract_version": version,
            "common_as_of_utc": common_as_of,
            "timeframe_window_years": request.get("timeframe_window_years"),
            "timeframe_calendar_boundaries": raw_map,
            "timeframe_required_evidence_lead_start_utc": request.get(
                "timeframe_required_evidence_lead_start_utc"
            ),
            "timeframe_source_request_intervals": request.get(
                "timeframe_source_request_intervals"
            ),
        }
        if request_map_version == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            request_map_fields.update(
                {
                    "timeframe_strategy_evidence_intervals": request.get(
                        "timeframe_strategy_evidence_intervals"
                    ),
                    "timeframe_audit_reference_map": request.get(
                        "timeframe_audit_reference_map"
                    ),
                    "timeframe_audit_only_prefix_intervals": request.get(
                        "timeframe_audit_only_prefix_intervals"
                    ),
                }
            )
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            request_map_fields.update(
                {
                    field_name: request.get(field_name)
                    for field_name in (
                        "timeframe_strategy_evidence_intervals",
                        "timeframe_audit_reference_map",
                        "timeframe_audit_consumer_map",
                        "timeframe_audit_windows",
                        "timeframe_audit_reference_intervals",
                        "timeframe_audit_only_intervals",
                        "timeframe_consensus_policy",
                    )
                }
            )
        if str(request.get("timeframe_request_map_sha256") or "") != (
            _canonical_json_sha256(request_map_fields)
        ):
            errors.append("NATIVE_UNIVERSE_TIMEFRAME_REQUEST_MAP_HASH_INVALID")

        for field_name in (
            "timeframe_window_years",
            "timeframe_required_evidence_lead_start_utc",
            "timeframe_source_request_intervals",
            *(
                (
                    "timeframe_strategy_evidence_intervals",
                    "timeframe_audit_reference_map",
                    "timeframe_audit_only_prefix_intervals",
                )
                if request_map_version == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                else (
                    "timeframe_strategy_evidence_intervals",
                    "timeframe_audit_reference_map",
                    "timeframe_audit_consumer_map",
                    "timeframe_audit_windows",
                    "timeframe_audit_reference_intervals",
                    "timeframe_audit_only_intervals",
                )
                if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                else ()
            ),
        ):
            raw_value = request.get(field_name)
            if not isinstance(raw_value, Mapping) or set(raw_value) != set(roster):
                errors.append(f"NATIVE_UNIVERSE_{field_name.upper()}_ROSTER_INVALID")
        years_map = request.get("timeframe_window_years")
        if isinstance(years_map, Mapping):
            for timeframe in roster:
                years = years_map.get(timeframe)
                if not isinstance(years, Mapping) or set(years) != set(
                    NATIVE_EVIDENCE_PARTITIONS
                ):
                    errors.append(
                        f"NATIVE_UNIVERSE_TIMEFRAME_WINDOW_YEARS_INVALID:{timeframe}"
                    )
                    continue
                for value in years.values():
                    if isinstance(value, bool):
                        errors.append(
                            f"NATIVE_UNIVERSE_TIMEFRAME_WINDOW_YEARS_INVALID:{timeframe}"
                        )
                        break
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError, OverflowError):
                        numeric = float("nan")
                    if not math.isfinite(numeric) or numeric <= 0.0:
                        errors.append(
                            f"NATIVE_UNIVERSE_TIMEFRAME_WINDOW_YEARS_INVALID:{timeframe}"
                        )
                        break

        leads = request.get("timeframe_required_evidence_lead_start_utc")
        intervals = request.get("timeframe_source_request_intervals")
        strategy_intervals = request.get("timeframe_strategy_evidence_intervals")
        audit_prefixes = request.get("timeframe_audit_only_prefix_intervals")
        audit_references = request.get("timeframe_audit_reference_map")
        for timeframe in roster:
            lead = leads.get(timeframe) if isinstance(leads, Mapping) else None
            if _parse_aware_utc(lead) is None:
                errors.append(
                    f"NATIVE_UNIVERSE_TIMEFRAME_EVIDENCE_LEAD_INVALID:{timeframe}"
                )
            interval = (
                intervals.get(timeframe) if isinstance(intervals, Mapping) else None
            )
            interval_start = (
                _parse_aware_utc(interval.get("start_utc"))
                if isinstance(interval, Mapping)
                else None
            )
            interval_end = (
                _parse_aware_utc(interval.get("end_exclusive_utc"))
                if isinstance(interval, Mapping)
                else None
            )
            if (
                not isinstance(interval, Mapping)
                or set(interval) != {"start_utc", "end_exclusive_utc"}
                or interval_start is None
                or interval_end is None
                or interval_start >= interval_end
            ):
                errors.append(
                    f"NATIVE_UNIVERSE_TIMEFRAME_SOURCE_INTERVAL_INVALID:{timeframe}"
                )
            if request_map_version in {
                FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            }:
                strategy_interval = (
                    strategy_intervals.get(timeframe)
                    if isinstance(strategy_intervals, Mapping)
                    else None
                )
                strategy_start = (
                    _parse_aware_utc(strategy_interval.get("start_utc"))
                    if isinstance(strategy_interval, Mapping)
                    else None
                )
                strategy_end = (
                    _parse_aware_utc(strategy_interval.get("end_exclusive_utc"))
                    if isinstance(strategy_interval, Mapping)
                    else None
                )
                if (
                    not isinstance(strategy_interval, Mapping)
                    or set(strategy_interval) != {"start_utc", "end_exclusive_utc"}
                    or strategy_start is None
                    or strategy_end is None
                    or strategy_start >= strategy_end
                    or interval_start is None
                    or interval_end is None
                    or interval_start > strategy_start
                    or interval_end != strategy_end
                ):
                    errors.append(
                        "NATIVE_UNIVERSE_TIMEFRAME_STRATEGY_INTERVAL_INVALID:"
                        f"{timeframe}"
                    )
                prefix = (
                    audit_prefixes.get(timeframe)
                    if isinstance(audit_prefixes, Mapping)
                    else None
                )
                if request_map_version == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
                    if prefix is None:
                        if interval_start != strategy_start:
                            errors.append(
                                "NATIVE_UNIVERSE_TIMEFRAME_AUDIT_PREFIX_INVALID:"
                                f"{timeframe}"
                            )
                    else:
                        prefix_start = (
                            _parse_aware_utc(prefix.get("start_utc"))
                            if isinstance(prefix, Mapping)
                            else None
                        )
                        prefix_end = (
                            _parse_aware_utc(prefix.get("end_exclusive_utc"))
                            if isinstance(prefix, Mapping)
                            else None
                        )
                        if (
                            not isinstance(prefix, Mapping)
                            or set(prefix) != {"start_utc", "end_exclusive_utc", "role"}
                            or prefix.get("role") != "audit_only_reference"
                            or prefix_start != interval_start
                            or prefix_end != strategy_start
                            or prefix_start is None
                            or prefix_end is None
                            or prefix_start >= prefix_end
                        ):
                            errors.append(
                                "NATIVE_UNIVERSE_TIMEFRAME_AUDIT_PREFIX_INVALID:"
                                f"{timeframe}"
                            )
                elif (
                    interval_start is not None
                    and strategy_start is not None
                    and (interval_start > strategy_start)
                ):
                    errors.append(
                        f"NATIVE_UNIVERSE_TIMEFRAME_SOURCE_UNION_INVALID:{timeframe}"
                    )

        if request_map_version == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            expected_references = _expected_cross_resolution_reference_map(roster)
            if audit_references != expected_references:
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_AUDIT_REFERENCE_MAP_INVALID")
            if isinstance(audit_references, Mapping):
                for target, reference in expected_references.items():
                    reference_interval = (
                        intervals.get(reference)
                        if isinstance(intervals, Mapping)
                        else None
                    )
                    reference_start = (
                        _parse_aware_utc(reference_interval.get("start_utc"))
                        if isinstance(reference_interval, Mapping)
                        else None
                    )
                    target_lead = (
                        _parse_aware_utc(leads.get(target))
                        if isinstance(leads, Mapping)
                        else None
                    )
                    if (
                        reference_start is None
                        or target_lead is None
                        or reference_start > target_lead
                    ):
                        errors.append(
                            "NATIVE_UNIVERSE_TIMEFRAME_AUDIT_DEPTH_INVALID:"
                            f"{target}->{reference}"
                        )
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            expected_references = {
                target: list(expected_reference_timeframes(target))
                for target in NATIVE_PRODUCTION_TIMEFRAMES
            }
            if roster != NATIVE_PRODUCTION_TIMEFRAMES:
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_V5_ROSTER_INVALID")
            if audit_references != expected_references:
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_AUDIT_REFERENCE_MAP_INVALID")
            expected_consumers = {
                timeframe: [] for timeframe in NATIVE_PRODUCTION_TIMEFRAMES
            }
            for target, references in expected_references.items():
                for reference in references:
                    expected_consumers[reference].append(target)
            if request.get("timeframe_audit_consumer_map") != expected_consumers:
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_AUDIT_CONSUMER_MAP_INVALID")
            if request.get("timeframe_consensus_policy") != (CONSENSUS_POLICY):
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_CONSENSUS_POLICY_INVALID")
            audit_windows = request.get("timeframe_audit_windows")
            edge_intervals = request.get("timeframe_audit_reference_intervals")
            audit_only_intervals = request.get("timeframe_audit_only_intervals")
            for field_name, value in (
                ("AUDIT_WINDOWS", audit_windows),
                ("AUDIT_REFERENCE_INTERVALS", edge_intervals),
                ("AUDIT_ONLY_INTERVALS", audit_only_intervals),
            ):
                if not isinstance(value, Mapping) or set(value) != set(roster):
                    errors.append(
                        f"NATIVE_UNIVERSE_TIMEFRAME_{field_name}_ROSTER_INVALID"
                    )
            if isinstance(audit_windows, Mapping) and isinstance(
                edge_intervals, Mapping
            ):
                for target in roster:
                    target_windows = audit_windows.get(target)
                    if not isinstance(target_windows, Mapping) or set(
                        target_windows
                    ) != set(REQUIRED_AUDIT_WINDOWS):
                        errors.append(
                            f"NATIVE_UNIVERSE_TIMEFRAME_AUDIT_WINDOWS_INVALID:{target}"
                        )
                    target_edges = edge_intervals.get(target)
                    if not isinstance(target_edges, Mapping) or set(
                        target_edges
                    ) != set(expected_references.get(target, ())):
                        errors.append(
                            f"NATIVE_UNIVERSE_TIMEFRAME_AUDIT_EDGES_INVALID:{target}"
                        )

        compatibility_timeframe = (
            "M5" if "M5" in roster else (roster[0] if roster else "")
        )
        if compatibility_timeframe:
            if boundaries != timeframe_boundaries.get(compatibility_timeframe):
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_COMMON_PROJECTION_MISMATCH")
            if isinstance(years_map, Mapping) and request.get("window_years") != (
                years_map.get(compatibility_timeframe)
            ):
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_WINDOW_PROJECTION_MISMATCH")

        reference_active: Optional[Dict[str, str]] = None
        for timeframe in roster:
            cell = timeframe_boundaries.get(timeframe) or {}
            active_projection = {
                partition: json.dumps(cell.get(partition), sort_keys=True)
                for partition in ("tuning_validation", "final_lockbox")
            }
            if reference_active is None:
                reference_active = active_projection
            elif active_projection != reference_active:
                errors.append("NATIVE_UNIVERSE_TIMEFRAME_ACTIVE_WINDOWS_NOT_CONCURRENT")
                break
    if errors:
        return {}, list(dict.fromkeys(errors))
    result = {
        "common_as_of_utc": common_as_of,
        "common_requested_start_utc": common_start,
        "common_calendar_boundaries": boundaries,
    }
    if version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        result.update(
            {
                "window_contract_version": version,
                "timeframe_calendar_boundaries": timeframe_boundaries,
                "timeframe_calendar_boundaries_sha256": request.get(
                    "timeframe_calendar_boundaries_sha256"
                ),
                "timeframe_request_map_sha256": request.get(
                    "timeframe_request_map_sha256"
                ),
                "timeframe_request_map_schema_version": request.get(
                    "timeframe_request_map_schema_version"
                ),
                "timeframe_strategy_evidence_intervals": request.get(
                    "timeframe_strategy_evidence_intervals"
                ),
                "timeframe_audit_reference_map": request.get(
                    "timeframe_audit_reference_map"
                ),
            }
        )
        request_map_version = str(
            request.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            result["timeframe_audit_only_prefix_intervals"] = request.get(
                "timeframe_audit_only_prefix_intervals"
            )
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            for field_name in (
                "timeframe_audit_consumer_map",
                "timeframe_audit_windows",
                "timeframe_audit_reference_intervals",
                "timeframe_audit_only_intervals",
                "timeframe_consensus_policy",
            ):
                result[field_name] = request.get(field_name)
    return result, []


def _v5_interval(value: Any) -> Optional[Tuple[datetime, datetime]]:
    if not isinstance(value, Mapping) or set(value) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        return None
    start = _parse_aware_utc(value.get("start_utc"))
    end = _parse_aware_utc(value.get("end_exclusive_utc"))
    if start is None or end is None or start >= end:
        return None
    return start, end


def _v5_scored_resolution_frames(
    provenance: Mapping[str, Any],
    frame: Optional[pd.DataFrame],
    daily_frame: Optional[pd.DataFrame],
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """Independently project cadence evidence onto scored partitions."""

    request = provenance.get("request")
    boundaries = (
        request.get("cell_calendar_boundaries")
        if isinstance(request, Mapping)
        else None
    )
    if not isinstance(boundaries, Mapping) or set(boundaries) != set(
        NATIVE_EVIDENCE_PARTITIONS
    ):
        return None, None, "SCORED_EVIDENCE_INTERVALS_INVALID"
    intervals: List[Tuple[datetime, datetime]] = []
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        boundary = boundaries.get(partition)
        interval = _v5_interval(
            {
                "start_utc": boundary.get("start_ts"),
                "end_exclusive_utc": boundary.get("end_ts"),
            }
            if isinstance(boundary, Mapping)
            else None
        )
        if interval is None:
            return None, None, "SCORED_EVIDENCE_INTERVALS_INVALID"
        intervals.append(interval)
    intervals.sort(key=lambda value: (value[0], value[1]))

    def _slice(value: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if value is None:
            return None
        pieces = []
        for start, end in intervals:
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
            left = int(value.index.searchsorted(start_ts, side="left"))
            right = int(value.index.searchsorted(end_ts, side="left"))
            pieces.append(value.iloc[left:right])
        return pd.concat(pieces, axis=0, copy=False)

    return _slice(frame), _slice(daily_frame), None


def _v5_pair_states(report: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    partitions = report.get("partitions")
    states: Dict[str, Dict[str, Any]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        detail = partitions.get(window) if isinstance(partitions, Mapping) else None
        status = str(
            detail.get("status") if isinstance(detail, Mapping) else ""
        ).upper()
        if status == "PASS":
            integrity, evidence = INTEGRITY_PASS, EVIDENCE_PASS
        elif status == "FAIL":
            integrity, evidence = INTEGRITY_PASS, EVIDENCE_SEMANTIC_FAIL
        elif status == "PENDING":
            integrity, evidence = INTEGRITY_PASS, EVIDENCE_PENDING
        else:
            integrity, evidence = INTEGRITY_ERROR, EVIDENCE_PENDING
        states[window] = {
            "integrity_state": integrity,
            "evidence_state": evidence,
            "reason_codes": list(
                detail.get("reason_codes") if isinstance(detail, Mapping) else [] or []
            ),
        }
    return states


def _v5_receipt_byte_exact(left: Any, right: Any) -> bool:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    try:
        return canonical_json_bytes(left) == canonical_json_bytes(right)
    except (TypeError, ValueError, OverflowError):
        return False


def _v5_legacy_edge_no_more_permissive(
    legacy_edge: Mapping[str, Any],
    current_edge: Mapping[str, Any],
) -> bool:
    """Enforce the frozen-receipt subset relation window by window.

    A legacy PASS must also PASS under current semantics.  Legacy PENDING may
    remain only when current semantics are PASS or PENDING.  A legacy FAIL is
    conservative against any current evidence state.  Integrity ERROR is also
    conservative, while legacy integrity PASS may not override a current ERROR.
    """

    legacy_windows = legacy_edge.get("windows")
    current_windows = current_edge.get("windows")
    if (
        not isinstance(legacy_windows, Mapping)
        or not isinstance(current_windows, Mapping)
        or set(legacy_windows) != set(REQUIRED_AUDIT_WINDOWS)
        or set(current_windows) != set(REQUIRED_AUDIT_WINDOWS)
    ):
        return False
    valid_integrity = {INTEGRITY_PASS, INTEGRITY_ERROR}
    valid_evidence = {
        EVIDENCE_PASS,
        EVIDENCE_PENDING,
        EVIDENCE_SEMANTIC_FAIL,
        EVIDENCE_TERMINAL_UNAVAILABLE,
    }
    for window in REQUIRED_AUDIT_WINDOWS:
        legacy = legacy_windows.get(window)
        current = current_windows.get(window)
        if not isinstance(legacy, Mapping) or not isinstance(current, Mapping):
            return False
        legacy_integrity = str(legacy.get("integrity_state") or "").upper()
        current_integrity = str(current.get("integrity_state") or "").upper()
        legacy_evidence = str(legacy.get("evidence_state") or "").upper()
        current_evidence = str(current.get("evidence_state") or "").upper()
        if (
            legacy_integrity not in valid_integrity
            or current_integrity not in valid_integrity
            or legacy_evidence not in valid_evidence
            or current_evidence not in valid_evidence
        ):
            return False
        if legacy_integrity == INTEGRITY_PASS and current_integrity != INTEGRITY_PASS:
            return False
        if legacy_evidence == EVIDENCE_PASS and current_evidence != EVIDENCE_PASS:
            return False
        if legacy_evidence == EVIDENCE_PENDING and current_evidence not in {
            EVIDENCE_PASS,
            EVIDENCE_PENDING,
        }:
            return False
    return True


def _v5_select_compatible_edge_receipt(
    stored_edge: Any,
    current_edge: Mapping[str, Any],
    *,
    frozen_pre_seq2307_edge: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[Mapping[str, Any]], Optional[str]]:
    """Select only an exactly reproduced, semantically admissible edge.

    Current receipts always take the current path.  A frozen receipt is
    accepted only when its canonical bytes reproduce exactly and every legacy
    window is no more permissive than the independently recomputed current
    window.  Returning ``None`` leaves the caller on the fail-closed mismatch
    path; this helper never rewrites stored evidence.
    """

    if _v5_receipt_byte_exact(stored_edge, current_edge):
        return current_edge, CROSS_RESOLUTION_COMPATIBILITY_CURRENT
    if (
        frozen_pre_seq2307_edge is not None
        and _v5_receipt_byte_exact(stored_edge, frozen_pre_seq2307_edge)
        and _v5_legacy_edge_no_more_permissive(
            frozen_pre_seq2307_edge,
            current_edge,
        )
    ):
        return (
            frozen_pre_seq2307_edge,
            CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
        )
    return None, None


def _v5_frame_extent(
    frame: pd.DataFrame,
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    if frame.empty or not isinstance(frame.index, pd.DatetimeIndex):
        return None
    first = pd.Timestamp(frame.index[0])
    if first.tzinfo is None or "_BAR_CLOSE_TS" not in frame.columns:
        return None
    closes = pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    if closes.isna().any():
        return None
    return first.tz_convert("UTC"), pd.Timestamp(closes.iloc[-1]).tz_convert("UTC")


def _scope_native_pair_window_states_from_extents(
    states: Mapping[str, Mapping[str, Any]],
    *,
    target_extent: Optional[Tuple[pd.Timestamp, pd.Timestamp]],
    reference_extent: Optional[Tuple[pd.Timestamp, pd.Timestamp]],
    partition_windows: Mapping[str, Tuple[Any, Any]],
    boundary_tolerance_seconds: float,
) -> Dict[str, Dict[str, Any]]:
    tolerance = max(0.0, float(boundary_tolerance_seconds))
    result: Dict[str, Dict[str, Any]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        state = dict(states.get(window) or {})
        reasons = [str(value) for value in (state.get("reason_codes") or [])]
        raw_window = partition_windows.get(window)
        pending: List[str] = []
        try:
            start = pd.Timestamp(raw_window[0])
            end = pd.Timestamp(raw_window[1])
            if start.tzinfo is None or end.tzinfo is None or start >= end:
                raise ValueError("invalid window")
            start = start.tz_convert("UTC")
            end = end.tz_convert("UTC")
        except Exception:
            pending.append(f"AUDIT_WINDOW_BOUNDARY_INVALID:{window}")
        else:
            for role, extent in (
                ("TARGET", target_extent),
                ("REFERENCE", reference_extent),
            ):
                if extent is None:
                    pending.append(f"{role}_AUDIT_EXTENT_INVALID:{window}")
                    continue
                if (extent[0] - start).total_seconds() > tolerance:
                    pending.append(f"{role}_AUDIT_PREFIX_PENDING:{window}")
                if (end - extent[1]).total_seconds() > tolerance:
                    pending.append(f"{role}_AUDIT_SUFFIX_PENDING:{window}")
        if pending and state.get("evidence_state") not in {
            EVIDENCE_SEMANTIC_FAIL,
            EVIDENCE_TERMINAL_UNAVAILABLE,
        }:
            state["evidence_state"] = EVIDENCE_PENDING
        state["reason_codes"] = list(dict.fromkeys([*reasons, *pending]))
        result[window] = state
    return result


def scope_native_pair_window_states(
    states: Mapping[str, Mapping[str, Any]],
    *,
    target_frame: pd.DataFrame,
    reference_frame: pd.DataFrame,
    partition_windows: Mapping[str, Tuple[Any, Any]],
    boundary_tolerance_seconds: float,
) -> Dict[str, Dict[str, Any]]:
    """Keep overlap-only parity from proving an uncovered audit window."""

    return _scope_native_pair_window_states_from_extents(
        states,
        target_extent=_v5_frame_extent(target_frame),
        reference_extent=_v5_frame_extent(reference_frame),
        partition_windows=partition_windows,
        boundary_tolerance_seconds=boundary_tolerance_seconds,
    )


def _v5_terminal_states(reasons: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    return {
        window: {
            "integrity_state": INTEGRITY_PASS,
            "evidence_state": EVIDENCE_TERMINAL_UNAVAILABLE,
            "reason_codes": list(reasons),
        }
        for window in REQUIRED_AUDIT_WINDOWS
    }


def _v5_pending_states(reasons: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    return {
        window: {
            "integrity_state": INTEGRITY_PASS,
            "evidence_state": EVIDENCE_PENDING,
            "reason_codes": list(reasons),
        }
        for window in REQUIRED_AUDIT_WINDOWS
    }


def _v5_authenticated_reference_resolution_audit(
    native_root: Path,
    *,
    activation_id: str,
    run_id: str,
    symbol: str,
    timeframe: str,
    reference_entry: Any,
    run_manifest_relative_path: Any,
    run_manifest_sha256: Any,
    bars_sha256: Any,
    provenance_sha256: Any,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Open the pinned reference receipts before using resolution as a fact.

    A stored target-edge reason is not authority for omitting raw pair evidence.
    The reference cell's canonical sidecar and parity bytes must both be pinned
    by the same activation entry and bound back to the authenticated run source.
    Callers additionally recompute any claimed ``FAIL`` from bars before using
    that fact to derive ``AUDIT_REFERENCE_RESOLUTION_EXCLUDED``.
    """

    cell_id = f"{symbol}|{timeframe}"
    prefix = f"NATIVE_V5_REFERENCE_AUDIT:{cell_id}"
    errors: List[str] = []
    if not isinstance(reference_entry, Mapping):
        return None, [f"{prefix}:ENTRY_MISSING"]

    expected_leaf = f"{symbol}/{timeframe}"
    expected_bars_relative = f"runs/{run_id}/{expected_leaf}/bars.csv"
    expected_provenance_relative = f"runs/{run_id}/{expected_leaf}/provenance.json"
    expected_sidecar_relative = f"audits/{activation_id}/{expected_leaf}/bars.meta.json"
    expected_parity_relative = f"audits/{activation_id}/{expected_leaf}/parity.json"
    for field_name, expected in (
        ("symbol", symbol),
        ("timeframe", timeframe),
        ("run_id", run_id),
        ("bars_relative_path", expected_bars_relative),
        ("sidecar_relative_path", expected_sidecar_relative),
        ("parity_relative_path", expected_parity_relative),
    ):
        if str(reference_entry.get(field_name) or "") != expected:
            errors.append(f"{prefix}:ENTRY_{field_name.upper()}_MISMATCH")
    if (
        str(reference_entry.get("bars_sha256") or "").lower()
        != str(bars_sha256 or "").lower()
    ):
        errors.append(f"{prefix}:ENTRY_BARS_SHA256_MISMATCH")
    if (
        str(reference_entry.get("run_manifest_sha256") or "").lower()
        != str(run_manifest_sha256 or "").lower()
    ):
        errors.append(f"{prefix}:ENTRY_RUN_MANIFEST_SHA256_MISMATCH")

    sidecar_path, file_errors = _verify_pinned_file(
        native_root,
        reference_entry.get("sidecar_relative_path"),
        reference_entry.get("sidecar_sha256"),
        error_prefix=f"{prefix}:SIDECAR",
    )
    errors.extend(file_errors)
    parity_path, file_errors = _verify_pinned_file(
        native_root,
        reference_entry.get("parity_relative_path"),
        reference_entry.get("parity_sha256"),
        error_prefix=f"{prefix}:PARITY",
    )
    errors.extend(file_errors)
    if errors:
        return None, list(dict.fromkeys(errors))

    sidecar, object_errors = _read_evidence_object(
        sidecar_path,
        error_prefix=f"{prefix}:SIDECAR",
    )
    errors.extend(object_errors)
    parity, object_errors = _read_evidence_object(
        parity_path,
        error_prefix=f"{prefix}:PARITY",
    )
    errors.extend(object_errors)
    if not isinstance(sidecar, Mapping) or not isinstance(parity, Mapping):
        if not errors:
            errors.append(f"{prefix}:RECEIPT_MISSING")
        return None, list(dict.fromkeys(errors))

    for payload, label, expected_schema in (
        (sidecar, "SIDECAR", NATIVE_BARS_SCHEMA),
        (parity, "PARITY", CONSENSUS_SCHEMA_VERSION),
    ):
        if str(payload.get("schema_version") or "") != expected_schema:
            errors.append(f"{prefix}:{label}_SCHEMA_INVALID")
        for field_name, expected in (
            ("activation_id", activation_id),
            ("run_id", run_id),
            ("symbol", symbol),
            ("timeframe", timeframe),
            ("run_manifest_relative_path", str(run_manifest_relative_path or "")),
        ):
            observed = str(payload.get(field_name) or "")
            mismatch = (
                observed.upper() != str(expected).upper()
                if field_name == "symbol"
                else observed != expected
            )
            if mismatch:
                errors.append(f"{prefix}:{label}_{field_name.upper()}_MISMATCH")
        if (
            str(payload.get("run_manifest_sha256") or "").lower()
            != str(run_manifest_sha256 or "").lower()
        ):
            errors.append(f"{prefix}:{label}_RUN_MANIFEST_SHA256_MISMATCH")

    expected_bars_sha = str(bars_sha256 or "").lower()
    expected_provenance_sha = str(provenance_sha256 or "").lower()
    if str(sidecar.get("audited_sidecar_contract") or "") != (AUDITED_SIDECAR_CONTRACT):
        errors.append(f"{prefix}:SIDECAR_CONTRACT_INVALID")
    if str(sidecar.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"{prefix}:SIDECAR_BARS_SHA256_MISMATCH")
    if str(sidecar.get("source_provenance_relative_path") or "") != (
        expected_provenance_relative
    ):
        errors.append(f"{prefix}:SIDECAR_PROVENANCE_PATH_MISMATCH")
    if str(sidecar.get("source_provenance_sha256") or "").lower() != (
        expected_provenance_sha
    ):
        errors.append(f"{prefix}:SIDECAR_PROVENANCE_SHA256_MISMATCH")

    expected_source_binding = {
        "availability_state": "AVAILABLE",
        "bars_sha256": bars_sha256,
        "provenance_sha256": provenance_sha256,
    }
    if str(parity.get("source_bars_sha256") or "").lower() != expected_bars_sha:
        errors.append(f"{prefix}:PARITY_BARS_SHA256_MISMATCH")
    if str(parity.get("source_provenance_sha256") or "").lower() != (
        expected_provenance_sha
    ):
        errors.append(f"{prefix}:PARITY_PROVENANCE_SHA256_MISMATCH")
    if parity.get("source_binding") != expected_source_binding:
        errors.append(f"{prefix}:PARITY_SOURCE_BINDING_MISMATCH")
    declared_consensus_sha = str(parity.get("consensus_sha256") or "").lower()
    expected_consensus_sha = _canonical_json_sha256(
        {key: value for key, value in parity.items() if key != "consensus_sha256"}
    )
    if not _is_sha256(declared_consensus_sha) or declared_consensus_sha != (
        expected_consensus_sha
    ):
        errors.append(f"{prefix}:PARITY_CONSENSUS_SHA256_MISMATCH")

    resolution = sidecar.get("resolution_audit")
    resolution_fields = {
        "schema_version",
        "symbol",
        "timeframe",
        "status",
        "reason_codes",
        "rows",
        "active_sessions",
        "median_bars_per_active_session",
        "nominal_or_faster_spacing_ratio",
        "daily_identity_overlap_rows",
        "daily_timestamp_identity_ratio",
        "daily_ohlc_identity_ratio",
        "yearly_diagnostics",
    }
    if not isinstance(resolution, Mapping) or set(resolution) != resolution_fields:
        errors.append(f"{prefix}:RESOLUTION_OBJECT_INVALID")
        return None, list(dict.fromkeys(errors))
    if str(resolution.get("schema_version") or "") != (RESOLUTION_AUDIT_SCHEMA_VERSION):
        errors.append(f"{prefix}:RESOLUTION_SCHEMA_INVALID")
    if str(resolution.get("symbol") or "").upper() != symbol.upper():
        errors.append(f"{prefix}:RESOLUTION_SYMBOL_MISMATCH")
    if str(resolution.get("timeframe") or "") != timeframe:
        errors.append(f"{prefix}:RESOLUTION_TIMEFRAME_MISMATCH")
    resolution_status = str(resolution.get("status") or "")
    if resolution_status not in {"PASS", "FAIL", "PENDING"}:
        errors.append(f"{prefix}:RESOLUTION_STATUS_INVALID")
    raw_reasons = resolution.get("reason_codes")
    if (
        not isinstance(raw_reasons, list)
        or any(not isinstance(value, str) or not value for value in raw_reasons)
        or len(raw_reasons) != len(set(raw_reasons))
    ):
        errors.append(f"{prefix}:RESOLUTION_REASONS_INVALID")
        resolution_reasons: List[str] = []
    else:
        resolution_reasons = list(raw_reasons)
    for field_name in ("rows", "active_sessions", "daily_identity_overlap_rows"):
        value = resolution.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            errors.append(f"{prefix}:RESOLUTION_{field_name.upper()}_INVALID")
    for field_name, upper_bound in (
        ("median_bars_per_active_session", None),
        ("nominal_or_faster_spacing_ratio", 1.0),
        ("daily_timestamp_identity_ratio", 1.0),
        ("daily_ohlc_identity_ratio", 1.0),
    ):
        value = resolution.get(field_name)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            numeric = float("nan")
        if (
            isinstance(value, bool)
            or not math.isfinite(numeric)
            or numeric < 0.0
            or (upper_bound is not None and numeric > upper_bound)
        ):
            errors.append(f"{prefix}:RESOLUTION_{field_name.upper()}_INVALID")
    if not isinstance(resolution.get("yearly_diagnostics"), Mapping):
        errors.append(f"{prefix}:RESOLUTION_YEARLY_DIAGNOSTICS_INVALID")
    if str(reference_entry.get("resolution_status") or "") != resolution_status:
        errors.append(f"{prefix}:ENTRY_RESOLUTION_STATUS_MISMATCH")
    if list(reference_entry.get("resolution_reason_codes") or []) != (
        resolution_reasons
    ):
        errors.append(f"{prefix}:ENTRY_RESOLUTION_REASONS_MISMATCH")
    expected_resolution_reasons = [
        f"RESOLUTION:{value}" for value in resolution_reasons
    ]
    for payload, label, reason_field in (
        (sidecar, "SIDECAR", "activation_reason_codes"),
        (parity, "PARITY", "cell_reason_codes"),
    ):
        raw_payload_reasons = payload.get(reason_field)
        if not isinstance(raw_payload_reasons, list):
            errors.append(f"{prefix}:{label}_CELL_REASONS_INVALID")
            continue
        recorded_resolution_reasons = [
            value
            for value in raw_payload_reasons
            if isinstance(value, str) and value.startswith("RESOLUTION:")
        ]
        if recorded_resolution_reasons != expected_resolution_reasons:
            errors.append(f"{prefix}:{label}_RESOLUTION_REASONS_MISMATCH")

    if errors:
        return None, list(dict.fromkeys(errors))
    return dict(resolution), []


def _v5_terminal_technical_binding(provenance_sha256: Any) -> Dict[str, Any]:
    """Bind a proven technical leaf without pretending that bars exist."""

    digest = str(provenance_sha256 or "").strip().lower()
    return {
        "availability_state": "TECHNICAL_TERMINAL",
        "bars_sha256": None,
        "provenance_sha256": digest,
    }


def _v5_tolerance(provenance: Mapping[str, Any]) -> Optional[float]:
    instrument = provenance.get("instrument")
    try:
        value = float(
            instrument.get("parity_tick_tolerance")
            if isinstance(instrument, Mapping)
            else None
        )
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0.0 else None


def _v5_read_frame(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path is None or not path.is_file():
        return None
    try:
        return normalize_ohlc_frame(pd.read_csv(path))
    except Exception:
        return None


def _v5_detached_frame_copy(frame: pd.DataFrame) -> pd.DataFrame:
    """Copy data and both axes so callers cannot mutate cached semantics."""

    detached = frame.copy(deep=True)
    detached.index = frame.index.copy(deep=True)
    detached.columns = frame.columns.copy(deep=True)
    return detached


@dataclass(frozen=True)
class _V5NormalizedFrameCacheEntry:
    expected_sha256: str
    file_identity: Tuple[int, int, int, int]
    frame: pd.DataFrame
    extent: Optional[Tuple[pd.Timestamp, pd.Timestamp]]
    memory_bytes: int


class _V5NormalizedFrameCache:
    """Bounded normalized-frame reuse within exactly one symbol replay.

    Cached frames are never byte authority. Every lookup still opens, streams
    and SHA-256 hashes the complete file before returning a deep copy. The
    cache only avoids repeated CSV parsing and OHLC normalization.
    """

    def __init__(
        self,
        symbol: str,
        *,
        max_frames: int = _MAX_V5_NORMALIZED_FRAME_CACHE_FRAMES,
        max_bytes: int = _MAX_V5_NORMALIZED_FRAME_CACHE_BYTES,
    ) -> None:
        canonical_symbol = str(symbol or "").strip()
        if not canonical_symbol:
            raise ValueError("NATIVE_V5_FRAME_CACHE_SYMBOL_INVALID")
        if (
            isinstance(max_frames, bool)
            or not isinstance(max_frames, int)
            or not 1 <= max_frames <= _MAX_V5_NORMALIZED_FRAME_CACHE_FRAMES
        ):
            raise ValueError("NATIVE_V5_FRAME_CACHE_FRAME_BOUND_INVALID")
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or not 1 <= max_bytes <= _MAX_V5_NORMALIZED_FRAME_CACHE_BYTES
        ):
            raise ValueError("NATIVE_V5_FRAME_CACHE_BYTE_BOUND_INVALID")
        self.symbol = canonical_symbol
        self.max_frames = max_frames
        self.max_bytes = max_bytes
        self._entries: OrderedDict[str, _V5NormalizedFrameCacheEntry] = OrderedDict()
        self._memory_bytes = 0

    @staticmethod
    def _key(path: Path) -> str:
        return os.path.normcase(os.path.abspath(os.fspath(path)))

    def _assert_symbol_path(self, path: Path) -> None:
        # V5 bars have the immutable .../<symbol>/<timeframe>/bars.csv geometry.
        if path.name != "bars.csv" or path.parent.parent.name != self.symbol:
            raise ValueError("NATIVE_V5_FRAME_CACHE_SYMBOL_SCOPE_MISMATCH")

    def _drop_key(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._memory_bytes -= entry.memory_bytes

    def evict(self, path: Path) -> None:
        self._drop_key(self._key(path))

    def lookup(
        self,
        path: Path,
        expected_sha256: str,
        file_identity: Tuple[int, int, int, int],
    ) -> Tuple[Optional[_V5NormalizedFrameCacheEntry], Optional[str]]:
        self._assert_symbol_path(path)
        key = self._key(path)
        entry = self._entries.get(key)
        if entry is None:
            return None, None
        if entry.expected_sha256 != expected_sha256:
            self._drop_key(key)
            return None, "NATIVE_V5_FRAME_CACHE_SHA256_IDENTITY_MISMATCH"
        if entry.file_identity != file_identity:
            self._drop_key(key)
            return None, "NATIVE_V5_FRAME_CACHE_FILE_IDENTITY_MISMATCH"
        self._entries.move_to_end(key)
        return entry, None

    def store(
        self,
        path: Path,
        expected_sha256: str,
        file_identity: Tuple[int, int, int, int],
        frame: pd.DataFrame,
    ) -> bool:
        self._assert_symbol_path(path)
        memory_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        if memory_bytes < 0:
            raise ValueError("NATIVE_V5_FRAME_CACHE_MEMORY_SIZE_INVALID")
        key = self._key(path)
        self._drop_key(key)
        if memory_bytes > self.max_bytes:
            return False
        cached_frame = _v5_detached_frame_copy(frame)
        extent = _v5_frame_extent(cached_frame)
        while self._entries and (
            len(self._entries) >= self.max_frames
            or self._memory_bytes + memory_bytes > self.max_bytes
        ):
            oldest_key = next(iter(self._entries))
            self._drop_key(oldest_key)
        entry = _V5NormalizedFrameCacheEntry(
            expected_sha256=expected_sha256,
            file_identity=file_identity,
            frame=cached_frame,
            extent=extent,
            memory_bytes=memory_bytes,
        )
        self._entries[key] = entry
        self._memory_bytes += memory_bytes
        return True

    def extent_for_verified_read(
        self,
        path: Path,
        observed_sha256: str,
    ) -> Tuple[bool, Optional[Tuple[pd.Timestamp, pd.Timestamp]]]:
        """Return metadata for the entry just authenticated by a full read."""

        self._assert_symbol_path(path)
        entry = self._entries.get(self._key(path))
        if entry is None or entry.expected_sha256 != observed_sha256:
            return False, None
        return True, entry.extent

    def clear(self) -> None:
        self._entries.clear()
        self._memory_bytes = 0

    def stats(self) -> Dict[str, int]:
        return {
            "frame_count": len(self._entries),
            "memory_bytes": self._memory_bytes,
            "max_frames": self.max_frames,
            "max_bytes": self.max_bytes,
        }


def _v5_file_identity(stat_result: os.stat_result) -> Tuple[int, int, int, int]:
    return (
        int(stat_result.st_dev),
        int(stat_result.st_ino),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
    )


def _v5_read_hashed_frame(
    path: Optional[Path],
    expected_sha256: Any,
    *,
    parse_on_hash_mismatch: bool,
    frame_cache: Optional[_V5NormalizedFrameCache] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Read one bounded CSV, verify its digest, then parse those exact bytes.

    At most one CSV byte buffer exists and is released before return.  No bytes
    reach pandas before the full payload matches its pinned SHA-256 unless a
    diagnostic caller explicitly requests the legacy mismatch parse behavior.
    """

    if path is None or not path.is_file():
        return None, None
    expected = str(expected_sha256 or "").strip().lower()
    digest = hashlib.sha256()
    raw_frame: Optional[pd.DataFrame] = None
    payload: Optional[BytesIO] = None
    cached_entry: Optional[_V5NormalizedFrameCacheEntry] = None
    cache_rejection: Optional[str] = None
    file_identity: Optional[Tuple[int, int, int, int]] = None
    try:
        with path.open("rb", buffering=0) as handle:
            initial_identity = _v5_file_identity(os.fstat(handle.fileno()))
            if initial_identity[2] > _MAX_V5_BARS_BYTES:
                if frame_cache is not None:
                    frame_cache.evict(path)
                raise ValueError("NATIVE_V5_BARS_SIZE_LIMIT_EXCEEDED")
            if frame_cache is not None and not parse_on_hash_mismatch:
                cached_entry, cache_rejection = frame_cache.lookup(
                    path,
                    expected,
                    initial_identity,
                )
            if cached_entry is None and cache_rejection is None:
                payload = BytesIO()
            total_bytes = 0
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                total_bytes += len(chunk)
                if total_bytes > _MAX_V5_BARS_BYTES:
                    if frame_cache is not None:
                        frame_cache.evict(path)
                    raise ValueError("NATIVE_V5_BARS_SIZE_LIMIT_EXCEEDED")
                digest.update(chunk)
                if payload is not None:
                    payload.write(chunk)
            final_identity = _v5_file_identity(os.fstat(handle.fileno()))
            file_identity = initial_identity
            if final_identity != initial_identity or total_bytes != initial_identity[2]:
                if frame_cache is not None:
                    frame_cache.evict(path)
                raise ValueError("NATIVE_V5_BARS_CHANGED_DURING_READ")
            observed_sha256 = digest.hexdigest()
            if observed_sha256 != expected and not parse_on_hash_mismatch:
                if frame_cache is not None:
                    frame_cache.evict(path)
                return None, observed_sha256
            if payload is not None:
                payload.seek(0)
                try:
                    raw_frame = pd.read_csv(payload)
                except Exception:
                    raw_frame = None
        try:
            path_identity_after = _v5_file_identity(path.stat())
        except OSError as exc:
            if frame_cache is not None:
                frame_cache.evict(path)
            raise ValueError("NATIVE_V5_BARS_PATH_IDENTITY_LOST") from exc
        if file_identity is None or path_identity_after != file_identity:
            if frame_cache is not None:
                frame_cache.evict(path)
            raise ValueError("NATIVE_V5_BARS_PATH_IDENTITY_CHANGED")
    finally:
        if payload is not None:
            payload.close()
    if cache_rejection is not None:
        raise ValueError(cache_rejection)
    if cached_entry is not None:
        return _v5_detached_frame_copy(cached_entry.frame), observed_sha256
    if raw_frame is None:
        return None, observed_sha256
    try:
        frame = normalize_ohlc_frame(raw_frame)
    except Exception:
        frame = None
    if frame is not None and frame_cache is not None and observed_sha256 == expected:
        if file_identity is None:
            if frame_cache is not None:
                frame_cache.evict(path)
            raise ValueError("NATIVE_V5_FRAME_CACHE_FILE_IDENTITY_MISSING")
        frame_cache.store(
            path,
            expected,
            file_identity,
            frame,
        )
        return frame, observed_sha256
    return frame, observed_sha256


def _v5_read_pinned_frame(
    root: Path,
    relative: Any,
    expected_sha256: Any,
    *,
    error_prefix: str,
    frame_cache: Optional[_V5NormalizedFrameCache] = None,
) -> Tuple[Optional[Path], Optional[pd.DataFrame], Optional[str], List[str]]:
    """Preserve pinned-file diagnostics while avoiding a second CSV read."""

    path = _contained_path(root, relative)
    if path is None:
        return None, None, None, [f"{error_prefix}_PATH_UNSAFE"]
    if not path.is_file() or path.is_symlink():
        return path, None, None, [f"{error_prefix}_MISSING_OR_UNSAFE"]
    expected = str(expected_sha256 or "").strip().lower()
    if not _is_sha256(expected):
        return path, None, None, [f"{error_prefix}_SHA256_INVALID"]
    frame, observed_sha256 = _v5_read_hashed_frame(
        path,
        expected,
        parse_on_hash_mismatch=False,
        frame_cache=frame_cache,
    )
    errors = [f"{error_prefix}_SHA256_MISMATCH"] if observed_sha256 != expected else []
    return path, frame, observed_sha256, errors


def _v5_extent_after_verified_read(
    *,
    frame_cache: Optional[_V5NormalizedFrameCache],
    path: Optional[Path],
    observed_sha256: Optional[str],
    frame: Optional[pd.DataFrame],
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Reuse exact-byte-bound extent metadata or compute the same fallback."""

    if frame is None:
        return None
    if frame_cache is not None and path is not None and observed_sha256 is not None:
        found, extent = frame_cache.extent_for_verified_read(path, observed_sha256)
        if found:
            return extent
    return _v5_frame_extent(frame)


def _v5_expected_coverage(
    envelope: Mapping[str, Any],
    pair_reports: Mapping[str, Optional[Mapping[str, Any]]],
) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {}
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        status = str(
            ((envelope.get("windows") or {}).get(partition) or {}).get("status") or ""
        ).upper()
        candidates: List[float] = []
        for report in pair_reports.values():
            detail = (
                (report.get("partitions") or {}).get(partition)
                if isinstance(report, Mapping)
                else None
            )
            if (
                not isinstance(detail, Mapping)
                or str(detail.get("status") or "").upper() != "PASS"
            ):
                continue
            raw = (
                detail.get("bidirectional_coverage")
                if isinstance(detail, Mapping)
                else None
            )
            try:
                value = float(raw)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(value) and 0.0 <= value <= 1.0:
                candidates.append(value)
        result[partition] = (
            max(candidates)
            if status == "PASS" and candidates
            else 0.0
            if status == "FAIL"
            else None
        )
    return result


def _v5_expected_disagreement(envelope: Mapping[str, Any]) -> Dict[str, Any]:
    per_window: Dict[str, Any] = {}
    count = 0
    for window in REQUIRED_AUDIT_WINDOWS:
        detail = (envelope.get("windows") or {}).get(window) or {}
        states = detail.get("edge_states") or {}
        outcomes = {
            str(reference): {
                "integrity_state": state.get("integrity_state"),
                "evidence_state": state.get("evidence_state"),
            }
            for reference, state in states.items()
            if isinstance(state, Mapping)
        }
        disagreement = (
            len(
                {
                    (value["integrity_state"], value["evidence_state"])
                    for value in outcomes.values()
                }
            )
            > 1
        )
        count += int(disagreement)
        per_window[window] = {
            "consensus_status": detail.get("status"),
            "disagreement": disagreement,
            "edge_outcomes": outcomes,
        }
    return {
        "window_disagreement_count": count,
        "windows_with_disagreement": [
            window
            for window in REQUIRED_AUDIT_WINDOWS
            if per_window[window]["disagreement"]
        ],
        "per_window": per_window,
    }


def _v5_has_unopposed_semantic_failure(envelope: Mapping[str, Any]) -> bool:
    """Match the V5 writer's unresolved-contradiction classification.

    A semantic failure can be isolated when another exact reference passes the
    same scored window.  A failure opposed only by pending or unavailable
    evidence has no passing witness, so the target remains a terminal technical
    cell even when the aggregate consensus is ``PENDING``.
    """

    windows = envelope.get("windows")
    if not isinstance(windows, Mapping):
        return False
    for window in REQUIRED_AUDIT_WINDOWS:
        detail = windows.get(window)
        edge_states = detail.get("edge_states") if isinstance(detail, Mapping) else None
        if not isinstance(edge_states, Mapping):
            continue
        evidence_states = {
            str(state.get("evidence_state") or "").strip().upper()
            for state in edge_states.values()
            if isinstance(state, Mapping)
        }
        if (
            EVIDENCE_SEMANTIC_FAIL in evidence_states
            and EVIDENCE_PASS not in evidence_states
        ):
            return True
    return False


def _v5_derive_recomputed_authenticity_state(
    envelope: Mapping[str, Any],
    *,
    resolution_status: str,
) -> str:
    """Derive a verified-success cell state with writer-identical semantics."""

    resolution = str(resolution_status or "PENDING").strip().upper()
    consensus = str(envelope.get("status") or "ERROR").strip().upper()
    if resolution == "PENDING":
        return NATIVE_PENDING
    if resolution != "PASS":
        return NATIVE_TECHNICAL_ERROR
    if consensus not in {"PASS", "PENDING"} or _v5_has_unopposed_semantic_failure(
        envelope
    ):
        return NATIVE_TECHNICAL_ERROR
    return NATIVE_EVIDENCE_CANDIDATE


def _verify_v5_activation_cell_evidence(
    native_root: Path,
    *,
    symbol: str,
    timeframe: str,
    entry: Any,
    run_cell: Any,
    audited_cell: Any,
    expected_cell_status: str,
    activation_id: str,
    run_id: str,
    run_manifest_relative_path: Any,
    run_manifest_sha256: Any,
    run_request: Mapping[str, Any],
    run_source_environment: Mapping[str, Any],
    run_acquisition_started_utc: Any,
    run_acquisition_completed_utc: Any,
    contract_minimum: float,
    parity_thresholds: Mapping[str, Any],
    symbol_run_cells: Mapping[str, Mapping[str, Any]],
    symbol_activation_entries: Mapping[str, Mapping[str, Any]],
    frame_cache: Optional[_V5NormalizedFrameCache] = None,
) -> List[str]:
    """Recompute V5 pair evidence from pinned CSVs; never trust stored PASS."""

    cell_id = f"{symbol}|{timeframe}"
    errors: List[str] = []
    if not isinstance(entry, Mapping) or not isinstance(run_cell, Mapping):
        return [f"NATIVE_V5_CELL_EVIDENCE_MISSING:{cell_id}"]
    expected_relative_leaf = f"{symbol}/{timeframe}"
    expected_bars_relative = f"runs/{run_id}/{expected_relative_leaf}/bars.csv"
    expected_sidecar_relative = (
        f"audits/{activation_id}/{expected_relative_leaf}/bars.meta.json"
    )
    expected_consensus_relative = (
        f"audits/{activation_id}/{expected_relative_leaf}/parity.json"
    )
    if (
        str(run_cell.get("symbol") or "") != symbol
        or str(run_cell.get("timeframe") or "") != timeframe
    ):
        errors.append(f"NATIVE_V5_RUN_CELL_IDENTITY_MISMATCH:{cell_id}")
    if (
        str(run_cell.get("relative_leaf") or "").replace("\\", "/")
        != expected_relative_leaf
    ):
        errors.append(f"NATIVE_V5_RUN_CELL_PATH_IDENTITY_MISMATCH:{cell_id}")
    allowed_fields = {
        "status",
        "audit_status",
        "universe_eligibility_status",
        "tradeable",
        "statistical_sufficiency_receipt_required",
        "evidence_metadata",
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
        "cross_resolution_reference_timeframes",
        "cross_resolution_reference_bindings",
        "consensus_status",
        "consensus_disagreement_count",
        "consensus_window_diagnostics",
        "resolution_status",
        "resolution_reason_codes",
        "expected_session_coverage_by_partition",
        "observed_session_bars_by_partition",
        "expected_session_bars_by_partition",
        "reason_codes",
    }
    if set(entry) - allowed_fields:
        errors.append(f"NATIVE_V5_ENTRY_FIELDS_INVALID:{cell_id}")
    if entry.get("tradeable") is not False:
        errors.append(f"NATIVE_V5_ENTRY_TRADEABLE_FLAG_INVALID:{cell_id}")
    if entry.get("statistical_sufficiency_receipt_required") is not True:
        errors.append(f"NATIVE_V5_ENTRY_SUFFICIENCY_GATE_INVALID:{cell_id}")
    if not isinstance(entry.get("evidence_metadata"), Mapping):
        errors.append(f"NATIVE_V5_ENTRY_EVIDENCE_METADATA_INVALID:{cell_id}")
    for field_name, expected in (
        ("symbol", symbol),
        ("timeframe", timeframe),
        ("run_id", run_id),
    ):
        if str(entry.get(field_name) or "") != expected:
            errors.append(f"NATIVE_V5_ENTRY_{field_name.upper()}_MISMATCH:{cell_id}")
    for field_name, expected in (
        ("bars_relative_path", expected_bars_relative),
        ("sidecar_relative_path", expected_sidecar_relative),
        ("parity_relative_path", expected_consensus_relative),
    ):
        if str(entry.get(field_name) or "").replace("\\", "/") != expected:
            errors.append(
                f"NATIVE_V5_ENTRY_{field_name.upper()}_IDENTITY_MISMATCH:{cell_id}"
            )
    if (
        str(entry.get("run_manifest_sha256") or "").lower()
        != str(run_manifest_sha256 or "").lower()
    ):
        errors.append(f"NATIVE_V5_ENTRY_RUN_MANIFEST_MISMATCH:{cell_id}")
    classification, coherence_error = classify_native_availability_cell(
        run_cell.get("status"),
        run_cell.get("failure_status"),
        run_cell.get("rows"),
        run_cell.get("resume_disposition"),
    )
    if classification == "invalid":
        errors.append(
            f"NATIVE_V5_RUN_AVAILABILITY_INVALID:{cell_id}:{coherence_error or 'unknown'}"
        )
    if (
        str(entry.get("bars_sha256") or "").lower()
        != str(run_cell.get("sha256") or "").lower()
    ):
        errors.append(f"NATIVE_V5_RUN_BARS_MISMATCH:{cell_id}")

    sidecar_path, file_errors = _verify_pinned_file(
        native_root,
        entry.get("sidecar_relative_path"),
        entry.get("sidecar_sha256"),
        error_prefix=f"NATIVE_V5_SIDECAR:{cell_id}",
    )
    errors.extend(file_errors)
    consensus_path, file_errors = _verify_pinned_file(
        native_root,
        entry.get("parity_relative_path"),
        entry.get("parity_sha256"),
        error_prefix=f"NATIVE_V5_CONSENSUS:{cell_id}",
    )
    errors.extend(file_errors)
    sidecar, object_errors = _read_evidence_object(
        sidecar_path,
        error_prefix=f"NATIVE_V5_SIDECAR:{cell_id}",
    )
    errors.extend(object_errors)
    consensus, object_errors = _read_evidence_object(
        consensus_path,
        error_prefix=f"NATIVE_V5_CONSENSUS:{cell_id}",
    )
    errors.extend(object_errors)
    if not isinstance(sidecar, Mapping) or not isinstance(consensus, Mapping):
        return list(dict.fromkeys(errors))

    provenance_relative = sidecar.get("source_provenance_relative_path")
    expected_provenance_relative = (
        f"runs/{run_id}/{expected_relative_leaf}/provenance.json"
    )
    if (
        str(provenance_relative or "").replace("\\", "/")
        != expected_provenance_relative
    ):
        errors.append(f"NATIVE_V5_PROVENANCE_PATH_IDENTITY_MISMATCH:{cell_id}")
    provenance_path, file_errors = _verify_pinned_file(
        native_root,
        provenance_relative,
        sidecar.get("source_provenance_sha256"),
        error_prefix=f"NATIVE_V5_PROVENANCE:{cell_id}",
    )
    errors.extend(file_errors)
    provenance, object_errors = _read_evidence_object(
        provenance_path,
        error_prefix=f"NATIVE_V5_PROVENANCE:{cell_id}",
    )
    errors.extend(object_errors)
    if not isinstance(provenance, Mapping):
        return list(dict.fromkeys(errors))
    if (
        str(provenance.get("sha256") or "").lower()
        != str(entry.get("bars_sha256") or "").lower()
    ):
        errors.append(f"NATIVE_V5_PROVENANCE_BARS_MISMATCH:{cell_id}")
    provenance_request = provenance.get("request")
    for field_name in (
        "window_contract_version",
        "timeframe_request_map_schema_version",
        "timeframe_window_years",
        "timeframe_calendar_boundaries",
        "timeframe_calendar_boundaries_sha256",
        "timeframe_required_evidence_lead_start_utc",
        "timeframe_source_request_intervals",
        "timeframe_strategy_evidence_intervals",
        "timeframe_audit_reference_map",
        "timeframe_audit_consumer_map",
        "timeframe_audit_windows",
        "timeframe_audit_reference_intervals",
        "timeframe_audit_only_intervals",
        "timeframe_consensus_policy",
        "timeframe_request_map_sha256",
    ):
        if not isinstance(provenance_request, Mapping) or provenance_request.get(
            field_name
        ) != run_request.get(field_name):
            errors.append(
                f"NATIVE_V5_PROVENANCE_REQUEST_MISMATCH:{cell_id}:{field_name}"
            )
    for leaf_field, map_field in (
        ("strategy_evidence_interval", "timeframe_strategy_evidence_intervals"),
        ("audit_reference_timeframes", "timeframe_audit_reference_map"),
        ("audit_consumer_targets", "timeframe_audit_consumer_map"),
        ("audit_windows", "timeframe_audit_windows"),
        ("audit_reference_intervals", "timeframe_audit_reference_intervals"),
        ("audit_only_intervals", "timeframe_audit_only_intervals"),
    ):
        source_map = run_request.get(map_field)
        expected = (
            source_map.get(timeframe) if isinstance(source_map, Mapping) else None
        )
        if (
            not isinstance(provenance_request, Mapping)
            or provenance_request.get(leaf_field) != expected
        ):
            errors.append(
                f"NATIVE_V5_PROVENANCE_LEAF_ROLE_MISMATCH:{cell_id}:{leaf_field}"
            )

    references = expected_reference_timeframes(timeframe)
    if list(entry.get("cross_resolution_reference_timeframes") or []) != list(
        references
    ):
        errors.append(f"NATIVE_V5_REFERENCE_ORDER_MISMATCH:{cell_id}")
    if list(consensus.get("reference_timeframes") or []) != list(references):
        errors.append(f"NATIVE_V5_CONSENSUS_REFERENCE_ORDER_MISMATCH:{cell_id}")
    for payload, label in ((sidecar, "SIDECAR"), (consensus, "CONSENSUS")):
        if str(payload.get("activation_id") or "") != activation_id:
            errors.append(f"NATIVE_V5_{label}_ACTIVATION_MISMATCH:{cell_id}")
        if str(payload.get("run_id") or "") != run_id:
            errors.append(f"NATIVE_V5_{label}_RUN_MISMATCH:{cell_id}")
        if str(payload.get("run_manifest_relative_path") or "").replace(
            "\\", "/"
        ) != str(run_manifest_relative_path or "").replace("\\", "/"):
            errors.append(f"NATIVE_V5_{label}_RUN_MANIFEST_PATH_MISMATCH:{cell_id}")
        if (
            str(payload.get("run_manifest_sha256") or "").lower()
            != str(run_manifest_sha256 or "").lower()
        ):
            errors.append(f"NATIVE_V5_{label}_RUN_MANIFEST_SHA_MISMATCH:{cell_id}")

    normalized_expected = str(expected_cell_status or "").lower()
    if normalized_expected == "eligible":
        normalized_expected = "active"
    if classification == "terminal_technical":
        try:
            validate_v5_terminal_technical_cell(
                provenance,
                run_cell,
                {
                    key: run_request[key]
                    for key in COMMON_REQUEST_KEYS
                    if key in run_request
                },
                symbol=symbol,
                timeframe=timeframe,
                run_id=run_id,
                manifest_source_environment=run_source_environment,
                expected_retries=run_request.get("retries"),
                manifest_acquisition_started_utc=run_acquisition_started_utc,
                manifest_acquisition_completed_utc=(run_acquisition_completed_utc),
            )
        except NativeRequestContractError as exc:
            errors.extend(
                f"NATIVE_V5_TERMINAL_TECHNICAL_INVALID:{cell_id}:{code}"
                for code in exc.errors
            )
        expected_source_binding = _v5_terminal_technical_binding(
            sidecar.get("source_provenance_sha256")
        )
        reference_bindings = consensus.get("reference_bindings")
        if not isinstance(reference_bindings, Mapping):
            reference_bindings = {}
            errors.append(
                f"NATIVE_V5_TERMINAL_TECHNICAL_REFERENCE_BINDINGS_INVALID:{cell_id}"
            )
        errors.extend(
            f"NATIVE_V5_{value}:{cell_id}"
            for value in validate_consensus_envelope(
                consensus,
                symbol=symbol,
                target_timeframe=timeframe,
                source_binding=expected_source_binding,
                reference_bindings=reference_bindings,
            )
        )
        edges = consensus.get("edges")
        if not isinstance(edges, Mapping) or set(edges) != set(references):
            errors.append(f"NATIVE_V5_TERMINAL_TECHNICAL_EDGE_GRID_INVALID:{cell_id}")
            edges = {}
        for reference in references:
            report = (
                (edges.get(reference) or {}).get("report")
                if isinstance(edges.get(reference), Mapping)
                else None
            )
            windows = report.get("windows") if isinstance(report, Mapping) else None
            for window in REQUIRED_AUDIT_WINDOWS:
                state = windows.get(window) if isinstance(windows, Mapping) else None
                if (
                    not isinstance(state, Mapping)
                    or state.get("integrity_state") != INTEGRITY_PASS
                    or state.get("evidence_state") != EVIDENCE_PENDING
                    or state.get("pair_report_sha256") is not None
                ):
                    errors.append(
                        "NATIVE_V5_TERMINAL_TECHNICAL_EDGE_STATE_INVALID:"
                        f"{cell_id}:{reference}:{window}"
                    )
        bars_path = _contained_path(native_root, entry.get("bars_relative_path"))
        if bars_path is None or bars_path.exists():
            errors.append(f"NATIVE_V5_TERMINAL_TECHNICAL_BARS_STATE_INVALID:{cell_id}")
        if (
            normalized_expected != NATIVE_TECHNICAL_ERROR
            or str(entry.get("status") or "").lower() != NATIVE_TECHNICAL_ERROR
            or str(entry.get("universe_eligibility_status") or "").lower()
            != NATIVE_TECHNICAL_ERROR
            or str(entry.get("audit_status") or "").upper() != "PASS"
            or entry.get("bars_sha256") is not None
            or str(consensus.get("status") or "").upper() != "PENDING"
            or str(sidecar.get("activation_status") or "").upper() != "PASS"
            or str(sidecar.get("data_state") or "").lower() != NATIVE_TECHNICAL_ERROR
            or str(sidecar.get("parity_status") or "").upper() != "PENDING"
            or str(entry.get("consensus_status") or "").upper() != "PENDING"
            or str(entry.get("resolution_status") or "").upper() != "PENDING"
            or str(entry.get("coverage_method") or "") != NATIVE_SESSION_COVERAGE_METHOD
            or entry.get("expected_session_coverage_by_partition")
            != {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS}
            or str(consensus.get("cell_activation_status") or "").upper() != "PASS"
            or consensus.get("source_bars_sha256") is not None
            or consensus.get("source_provenance_sha256")
            != sidecar.get("source_provenance_sha256")
            or consensus.get("expected_session_coverage_by_partition")
            != {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS}
            or str(consensus.get("coverage_method") or "")
            != NATIVE_SESSION_COVERAGE_METHOD
            or sidecar.get("expected_session_coverage_by_partition")
            != {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS}
            or str(sidecar.get("coverage_method") or "")
            != NATIVE_SESSION_COVERAGE_METHOD
            or sidecar.get("complete") is not True
            or sidecar.get("tradeable") is not False
            or sidecar.get("statistical_sufficiency_receipt_required") is not True
        ):
            errors.append(f"NATIVE_V5_TERMINAL_TECHNICAL_AUDIT_STATE_INVALID:{cell_id}")
        if entry.get("cross_resolution_reference_bindings") != reference_bindings:
            errors.append(
                f"NATIVE_V5_TERMINAL_TECHNICAL_REFERENCE_BINDING_MISMATCH:{cell_id}"
            )
        if entry.get("evidence_metadata") != sidecar.get("evidence_metadata"):
            errors.append(f"NATIVE_V5_TERMINAL_TECHNICAL_METADATA_MISMATCH:{cell_id}")
        return list(dict.fromkeys(errors))
    if classification == "unavailable":
        if normalized_expected != NATIVE_SOURCE_UNAVAILABLE:
            errors.append(f"NATIVE_V5_UNAVAILABLE_STATUS_MISMATCH:{cell_id}")
        if consensus.get("source_unavailable") is not True:
            errors.append(f"NATIVE_V5_UNAVAILABLE_CONSENSUS_INVALID:{cell_id}")
        errors.extend(
            f"NATIVE_V5_{value}:{cell_id}"
            for value in validate_native_source_unavailable_proof(
                provenance,
                symbol=symbol,
                timeframe=timeframe,
            )
        )
        if str(entry.get("status") or "").lower() != NATIVE_SOURCE_UNAVAILABLE:
            errors.append(f"NATIVE_V5_UNAVAILABLE_ENTRY_STATUS_INVALID:{cell_id}")
        if str(entry.get("universe_eligibility_status") or "").lower() != (
            NATIVE_SOURCE_UNAVAILABLE
        ):
            errors.append(f"NATIVE_V5_UNAVAILABLE_DATA_STATE_INVALID:{cell_id}")
        if str(entry.get("audit_status") or "").upper() != "PASS":
            errors.append(f"NATIVE_V5_UNAVAILABLE_AUDIT_STATUS_INVALID:{cell_id}")
        if (
            sidecar.get("tradeable") is not False
            or sidecar.get("statistical_sufficiency_receipt_required") is not True
        ):
            errors.append(f"NATIVE_V5_UNAVAILABLE_SIDECAR_GATE_INVALID:{cell_id}")
        if str(sidecar.get("data_state") or "").lower() != NATIVE_SOURCE_UNAVAILABLE:
            errors.append(f"NATIVE_V5_UNAVAILABLE_SIDECAR_STATE_INVALID:{cell_id}")
        return list(dict.fromkeys(errors))
    if classification != "success":
        return list(dict.fromkeys(errors))

    bars_path, target_frame, target_bars_observed_sha256, file_errors = (
        _v5_read_pinned_frame(
            native_root,
            entry.get("bars_relative_path"),
            entry.get("bars_sha256"),
            error_prefix=f"NATIVE_V5_BARS:{cell_id}",
            frame_cache=frame_cache,
        )
    )
    errors.extend(file_errors)
    tolerance = _v5_tolerance(provenance)
    if target_frame is None or tolerance is None:
        errors.append(f"NATIVE_V5_TARGET_FRAME_OR_TOLERANCE_INVALID:{cell_id}")
        return list(dict.fromkeys(errors))
    target_extent = _v5_extent_after_verified_read(
        frame_cache=frame_cache,
        path=bars_path,
        observed_sha256=target_bars_observed_sha256,
        frame=target_frame,
    )

    try:
        thresholds = ParityThresholds(
            min_membership_ratio=float(parity_thresholds["min_membership_ratio"]),
            max_ohlc_mismatch_ratio=float(parity_thresholds["max_ohlc_mismatch_ratio"]),
            max_unassigned_m5_ratio=float(parity_thresholds["max_unassigned_m5_ratio"]),
            min_compared_native_bars=int(parity_thresholds["min_compared_native_bars"]),
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        errors.append(f"NATIVE_V5_PARITY_THRESHOLDS_INVALID:{cell_id}")
        return list(dict.fromkeys(errors))
    raw_windows = (run_request.get("timeframe_audit_windows") or {}).get(timeframe)
    windows: Dict[str, Tuple[datetime, datetime]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        parsed = _v5_interval(
            raw_windows.get(window) if isinstance(raw_windows, Mapping) else None
        )
        if parsed is None:
            errors.append(f"NATIVE_V5_AUDIT_WINDOW_INVALID:{cell_id}:{window}")
        else:
            windows[window] = parsed
    if len(windows) != len(REQUIRED_AUDIT_WINDOWS):
        return list(dict.fromkeys(errors))

    source_binding = {
        "availability_state": "AVAILABLE",
        "bars_sha256": entry.get("bars_sha256"),
        "provenance_sha256": sidecar.get("source_provenance_sha256"),
    }
    expected_reference_bindings: Dict[str, Dict[str, Any]] = {}
    expected_edges: Dict[str, Dict[str, Any]] = {}
    pair_reports: Dict[str, Optional[Mapping[str, Any]]] = {}
    stored_edges = consensus.get("edges")
    d1_cell = symbol_run_cells.get("D1")
    d1_path = native_root / "runs" / run_id / symbol / "D1" / "bars.csv"
    d1_frame: Optional[pd.DataFrame] = None
    d1_classification, _d1_classification_error = classify_native_availability_cell(
        d1_cell.get("status") if isinstance(d1_cell, Mapping) else None,
        d1_cell.get("failure_status") if isinstance(d1_cell, Mapping) else None,
        d1_cell.get("rows") if isinstance(d1_cell, Mapping) else None,
        (d1_cell.get("resume_disposition") if isinstance(d1_cell, Mapping) else None),
    )
    if isinstance(d1_cell, Mapping) and d1_classification == "success":
        expected_d1_sha256 = str(d1_cell.get("sha256") or "").lower()
        if timeframe == "D1":
            if target_bars_observed_sha256 == expected_d1_sha256:
                d1_frame = target_frame
        elif d1_path.is_file():
            candidate_d1_frame, observed_d1_sha256 = _v5_read_hashed_frame(
                d1_path,
                expected_d1_sha256,
                parse_on_hash_mismatch=False,
                frame_cache=frame_cache,
            )
            if observed_d1_sha256 == expected_d1_sha256:
                d1_frame = candidate_d1_frame
    for reference in references:
        reference_resolution_status: Optional[str] = None
        reference_cell = symbol_run_cells.get(reference)
        reference_class, reference_error = classify_native_availability_cell(
            reference_cell.get("status")
            if isinstance(reference_cell, Mapping)
            else None,
            reference_cell.get("failure_status")
            if isinstance(reference_cell, Mapping)
            else None,
            reference_cell.get("rows") if isinstance(reference_cell, Mapping) else None,
            (
                reference_cell.get("resume_disposition")
                if isinstance(reference_cell, Mapping)
                else None
            ),
        )
        if reference_class == "unavailable":
            failure = str(reference_cell.get("failure_status") or "").upper()
            binding = {
                "availability_state": EVIDENCE_TERMINAL_UNAVAILABLE,
                "bars_sha256": None,
                "provenance_sha256": None,
            }
            states = _v5_terminal_states(
                [f"AUDIT_REFERENCE_UNAVAILABLE:{reference}:{failure or 'UNSPECIFIED'}"]
            )
            pair_reports[reference] = None
        elif reference_class == "terminal_technical":
            reference_provenance = (
                native_root / "runs" / run_id / symbol / reference / "provenance.json"
            )
            if not reference_provenance.is_file():
                errors.append(
                    f"NATIVE_V5_REFERENCE_PROVENANCE_MISSING:{cell_id}:{reference}"
                )
                continue
            binding = _v5_terminal_technical_binding(sha256_file(reference_provenance))
            states = _v5_pending_states(
                [f"AUDIT_REFERENCE_TERMINAL_TECHNICAL:{reference}"]
            )
            pair_reports[reference] = None
        elif reference_class == "success":
            reference_bars = (
                native_root / "runs" / run_id / symbol / reference / "bars.csv"
            )
            reference_provenance = (
                native_root / "runs" / run_id / symbol / reference / "provenance.json"
            )
            expected_reference_bars_sha256 = str(
                reference_cell.get("sha256") or ""
            ).lower()
            reference_frame, observed_reference_bars_sha256 = _v5_read_hashed_frame(
                reference_bars,
                expected_reference_bars_sha256,
                parse_on_hash_mismatch=False,
                frame_cache=frame_cache,
            )
            if observed_reference_bars_sha256 != expected_reference_bars_sha256:
                errors.append(f"NATIVE_V5_REFERENCE_BARS_INVALID:{cell_id}:{reference}")
                continue
            if not reference_provenance.is_file():
                errors.append(
                    f"NATIVE_V5_REFERENCE_PROVENANCE_MISSING:{cell_id}:{reference}"
                )
                continue
            reference_provenance_sha = sha256_file(reference_provenance)
            try:
                reference_provenance_payload = json.loads(
                    reference_provenance.read_text(encoding="utf-8")
                )
            except Exception:
                errors.append(
                    f"NATIVE_V5_REFERENCE_PROVENANCE_INVALID:{cell_id}:{reference}"
                )
                continue
            reference_tolerance = (
                _v5_tolerance(reference_provenance_payload)
                if isinstance(reference_provenance_payload, Mapping)
                else None
            )
            if reference_frame is None or reference_tolerance is None:
                errors.append(
                    f"NATIVE_V5_REFERENCE_FRAME_OR_TOLERANCE_INVALID:{cell_id}:{reference}"
                )
                continue
            reference_extent = _v5_extent_after_verified_read(
                frame_cache=frame_cache,
                path=reference_bars,
                observed_sha256=observed_reference_bars_sha256,
                frame=reference_frame,
            )
            binding = {
                "availability_state": "AVAILABLE",
                "bars_sha256": reference_cell.get("sha256"),
                "provenance_sha256": reference_provenance_sha,
            }
            reference_resolution, reference_resolution_errors = (
                _v5_authenticated_reference_resolution_audit(
                    native_root,
                    activation_id=activation_id,
                    run_id=run_id,
                    symbol=symbol,
                    timeframe=reference,
                    reference_entry=symbol_activation_entries.get(reference),
                    run_manifest_relative_path=run_manifest_relative_path,
                    run_manifest_sha256=run_manifest_sha256,
                    bars_sha256=reference_cell.get("sha256"),
                    provenance_sha256=reference_provenance_sha,
                )
            )
            errors.extend(reference_resolution_errors)
            if reference_resolution is None:
                continue
            reference_resolution_status = str(reference_resolution.get("status") or "")
            if reference_resolution_status == "FAIL":
                (
                    reference_resolution_frame,
                    reference_resolution_daily_frame,
                    reference_resolution_interval_error,
                ) = _v5_scored_resolution_frames(
                    reference_provenance_payload,
                    reference_frame,
                    d1_frame,
                )
                if reference_resolution_interval_error is not None:
                    errors.append(
                        "NATIVE_V5_REFERENCE_"
                        f"{reference_resolution_interval_error}:{cell_id}:{reference}"
                    )
                    continue
                recomputed_reference_resolution = audit_native_resolution(
                    reference_resolution_frame,
                    symbol=symbol,
                    timeframe=reference,
                    daily_frame=reference_resolution_daily_frame,
                    tick_tolerance=reference_tolerance,
                ).to_dict()
                recomputed_reference_resolution = json.loads(
                    json.dumps(
                        recomputed_reference_resolution,
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                )
                if recomputed_reference_resolution != reference_resolution:
                    errors.append(
                        "NATIVE_V5_REFERENCE_RESOLUTION_RECOMPUTATION_MISMATCH:"
                        f"{cell_id}:{reference}"
                    )
                    continue
                states = _v5_pending_states(
                    [f"AUDIT_REFERENCE_RESOLUTION_EXCLUDED:{reference}"]
                )
                pair_reports[reference] = None
            else:
                if (
                    NATIVE_TIMEFRAME_SECONDS[reference]
                    < NATIVE_TIMEFRAME_SECONDS[timeframe]
                ):
                    fine_tf, fine_frame = reference, reference_frame
                    coarse_tf, coarse_frame = timeframe, target_frame
                else:
                    fine_tf, fine_frame = timeframe, target_frame
                    coarse_tf, coarse_frame = reference, reference_frame
                raw_report = audit_normalized_native_cross_resolution(
                    fine_frame,
                    coarse_frame,
                    symbol=symbol,
                    fine_timeframe=fine_tf,
                    coarse_timeframe=coarse_tf,
                    activation_target_timeframe=timeframe,
                    tick_tolerance=min(tolerance, reference_tolerance),
                    partition_windows=windows,
                    thresholds=thresholds,
                )
                raw_report = dict(raw_report)
                raw_report["reference_timeframe"] = reference
                pair_reports[reference] = raw_report
                states = _scope_native_pair_window_states_from_extents(
                    _v5_pair_states(raw_report),
                    target_extent=target_extent,
                    reference_extent=reference_extent,
                    partition_windows=windows,
                    boundary_tolerance_seconds=float(
                        run_request.get("boundary_tolerance_days", 0.0)
                    )
                    * 86_400.0,
                )
        else:
            errors.append(
                f"NATIVE_V5_REFERENCE_RUN_CELL_INVALID:{cell_id}:{reference}:{reference_error or 'unknown'}"
            )
            continue
        expected_reference_bindings[reference] = binding
        current_edge = build_edge_report(
            symbol=symbol,
            target_timeframe=timeframe,
            reference_timeframe=reference,
            source_binding=source_binding,
            reference_binding=binding,
            window_states=states,
            pair_reports=(
                {window: pair_reports[reference] for window in REQUIRED_AUDIT_WINDOWS}
                if pair_reports[reference] is not None
                else None
            ),
        )
        stored_report = (
            (stored_edges.get(reference) or {}).get("report")
            if isinstance(stored_edges, Mapping)
            else None
        )
        selected_edge, selected_profile = _v5_select_compatible_edge_receipt(
            stored_report,
            current_edge,
        )
        if reference_class == "success":
            if selected_edge is None:
                if reference_resolution_status != "FAIL":
                    legacy_report = audit_normalized_native_cross_resolution(
                        fine_frame,
                        coarse_frame,
                        symbol=symbol,
                        fine_timeframe=fine_tf,
                        coarse_timeframe=coarse_tf,
                        activation_target_timeframe=timeframe,
                        tick_tolerance=min(tolerance, reference_tolerance),
                        partition_windows=windows,
                        thresholds=thresholds,
                        compatibility_profile=(
                            CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307
                        ),
                    )
                    legacy_report = dict(legacy_report)
                    legacy_report["reference_timeframe"] = reference
                    legacy_states = _scope_native_pair_window_states_from_extents(
                        _v5_pair_states(legacy_report),
                        target_extent=target_extent,
                        reference_extent=reference_extent,
                        partition_windows=windows,
                        boundary_tolerance_seconds=float(
                            run_request.get("boundary_tolerance_days", 0.0)
                        )
                        * 86_400.0,
                    )
                    legacy_edge = build_edge_report(
                        symbol=symbol,
                        target_timeframe=timeframe,
                        reference_timeframe=reference,
                        source_binding=source_binding,
                        reference_binding=binding,
                        window_states=legacy_states,
                        pair_reports={
                            window: legacy_report for window in REQUIRED_AUDIT_WINDOWS
                        },
                    )
                    selected_edge, selected_profile = (
                        _v5_select_compatible_edge_receipt(
                            stored_report,
                            current_edge,
                            frozen_pre_seq2307_edge=legacy_edge,
                        )
                    )
                    if (
                        selected_profile
                        == CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307
                    ):
                        pair_reports[reference] = legacy_report
            del reference_frame
        if selected_edge is None:
            selected_edge = current_edge
        expected_edges[reference] = selected_edge
    if set(expected_edges) != set(references):
        return list(dict.fromkeys(errors))
    expected_envelope = build_consensus_envelope(
        symbol=symbol,
        target_timeframe=timeframe,
        source_binding=source_binding,
        reference_bindings=expected_reference_bindings,
        edge_reports=expected_edges,
    )
    for payload, label in (
        (entry, "ENTRY"),
        (sidecar, "SIDECAR"),
        (audited_cell, "AUDIT"),
        (consensus, "CONSENSUS"),
    ):
        if not isinstance(payload, Mapping):
            continue
        if list(payload.get("cross_resolution_reference_timeframes") or []) != list(
            references
        ):
            errors.append(f"NATIVE_V5_{label}_REFERENCE_ORDER_MISMATCH:{cell_id}")
        if payload.get("cross_resolution_reference_bindings") != (
            expected_reference_bindings
        ):
            errors.append(f"NATIVE_V5_{label}_REFERENCE_BINDINGS_MISMATCH:{cell_id}")
    errors.extend(
        f"NATIVE_V5_{value}:{cell_id}"
        for value in validate_consensus_envelope(
            consensus,
            symbol=symbol,
            target_timeframe=timeframe,
            source_binding=source_binding,
            reference_bindings=expected_reference_bindings,
        )
    )
    for reference in references:
        stored_report = (
            (stored_edges.get(reference) or {}).get("report")
            if isinstance(stored_edges, Mapping)
            else None
        )
        if not _v5_receipt_byte_exact(stored_report, expected_edges[reference]):
            errors.append(
                f"NATIVE_V5_RAW_PAIR_RECOMPUTATION_MISMATCH:{cell_id}:{reference}"
            )
    for field_name in (
        "status",
        "reference_timeframes",
        "reference_graph_sha256",
        "policy",
        "policy_sha256",
        "source_binding",
        "reference_bindings",
        "windows",
    ):
        if consensus.get(field_name) != expected_envelope.get(field_name):
            errors.append(
                f"NATIVE_V5_CONSENSUS_DERIVATION_MISMATCH:{cell_id}:{field_name}"
            )

    expected_disagreement = _v5_expected_disagreement(expected_envelope)
    consensus_diagnostics = consensus.get("diagnostics")
    if (
        not isinstance(consensus_diagnostics, Mapping)
        or consensus_diagnostics.get("raw_pair_reports_recomputed_from_pinned_bars")
        is not True
    ):
        errors.append(f"NATIVE_V5_RAW_PAIR_RECOMPUTATION_MARKER_INVALID:{cell_id}")
    for field_name in (
        "window_disagreement_count",
        "windows_with_disagreement",
        "per_window",
    ):
        if (
            not isinstance(consensus_diagnostics, Mapping)
            or consensus_diagnostics.get(field_name)
            != expected_disagreement[field_name]
        ):
            errors.append(
                f"NATIVE_V5_CONSENSUS_DISAGREEMENT_MISMATCH:{cell_id}:{field_name}"
            )
    for payload, label in (
        (entry, "ENTRY"),
        (sidecar, "SIDECAR"),
        (audited_cell, "AUDIT"),
    ):
        if not isinstance(payload, Mapping):
            continue
        if (
            payload.get("consensus_disagreement_count")
            != expected_disagreement["window_disagreement_count"]
        ):
            errors.append(f"NATIVE_V5_{label}_DISAGREEMENT_COUNT_MISMATCH:{cell_id}")
        if (
            payload.get("consensus_window_diagnostics")
            != expected_disagreement["per_window"]
        ):
            errors.append(f"NATIVE_V5_{label}_WINDOW_DIAGNOSTICS_MISMATCH:{cell_id}")

    expected_coverage = _v5_expected_coverage(expected_envelope, pair_reports)
    if consensus.get("expected_session_coverage_by_partition") != expected_coverage:
        errors.append(f"NATIVE_V5_CONSENSUS_COVERAGE_MISMATCH:{cell_id}")
    for payload, label in (
        (entry, "ENTRY"),
        (sidecar, "SIDECAR"),
        (audited_cell, "AUDIT"),
    ):
        if (
            isinstance(payload, Mapping)
            and payload.get("expected_session_coverage_by_partition")
            != expected_coverage
        ):
            errors.append(f"NATIVE_V5_{label}_COVERAGE_MISMATCH:{cell_id}")

    # Resolution is independently recomputed before any consensus decision is
    # allowed to activate the cell. The authenticated D1 frame cached above is
    # shared with the narrow reference-FAIL check and released on return.
    resolution_frame, resolution_daily_frame, resolution_interval_error = (
        _v5_scored_resolution_frames(provenance, target_frame, d1_frame)
    )
    if resolution_interval_error is not None:
        errors.append(f"NATIVE_V5_{resolution_interval_error}:{cell_id}")
    resolution = audit_native_resolution(
        resolution_frame,
        symbol=symbol,
        timeframe=timeframe,
        daily_frame=resolution_daily_frame,
        tick_tolerance=tolerance,
    ).to_dict()
    serialized_resolution = json.loads(
        json.dumps(resolution, sort_keys=True, ensure_ascii=False)
    )
    if sidecar.get("resolution_audit") != serialized_resolution:
        errors.append(f"NATIVE_V5_RESOLUTION_RECOMPUTATION_MISMATCH:{cell_id}")
    resolution_status = str(resolution.get("status") or "PENDING").upper()
    consensus_status = str(expected_envelope.get("status") or "ERROR").upper()
    if consensus_status == "PASS":
        diagnostic_errors, _normalized = validate_native_session_coverage_diagnostics(
            expected_coverage
        )
        errors.extend(f"NATIVE_V5_{value}:{cell_id}" for value in diagnostic_errors)
    derived = _v5_derive_recomputed_authenticity_state(
        expected_envelope,
        resolution_status=resolution_status,
    )
    if derived != normalized_expected:
        errors.append(
            f"NATIVE_V5_UNIVERSE_STATUS_MISMATCH:{cell_id}:{derived}!={normalized_expected}"
        )
    expected_audit_status = (
        "PASS"
        if derived == NATIVE_EVIDENCE_CANDIDATE
        else "PENDING"
        if derived == NATIVE_PENDING
        else "FAIL"
    )
    expected_reasons = [
        f"RESOLUTION:{value}"
        for value in (resolution.get("reason_codes") or [])
        if str(value)
    ]
    if consensus_status == "FAIL":
        expected_reasons.append("CROSS_RESOLUTION_CONSENSUS_FAILED")
    elif consensus_status == "PENDING":
        expected_reasons.append("CROSS_RESOLUTION_CONSENSUS_PENDING")
    elif consensus_status == "ERROR":
        expected_reasons.append("CROSS_RESOLUTION_CONSENSUS_INTEGRITY_ERROR")
    expected_reasons.extend(
        f"CONSENSUS_DISAGREEMENT:{window}"
        for window in expected_disagreement["windows_with_disagreement"]
    )
    expected_reasons = list(dict.fromkeys(expected_reasons))
    if str(entry.get("audit_status") or "").upper() != expected_audit_status:
        errors.append(f"NATIVE_V5_ENTRY_AUDIT_STATUS_MISMATCH:{cell_id}")
    if list(entry.get("reason_codes") or []) != expected_reasons:
        errors.append(f"NATIVE_V5_ENTRY_REASON_DERIVATION_MISMATCH:{cell_id}")
    if str(sidecar.get("activation_status") or "").upper() != expected_audit_status:
        errors.append(f"NATIVE_V5_SIDECAR_AUDIT_STATUS_MISMATCH:{cell_id}")
    if list(sidecar.get("activation_reason_codes") or []) != expected_reasons:
        errors.append(f"NATIVE_V5_SIDECAR_REASON_DERIVATION_MISMATCH:{cell_id}")
    if str(consensus.get("cell_activation_status") or "").upper() != (
        expected_audit_status
    ):
        errors.append(f"NATIVE_V5_CONSENSUS_AUDIT_STATUS_MISMATCH:{cell_id}")
    if list(consensus.get("cell_reason_codes") or []) != expected_reasons:
        errors.append(f"NATIVE_V5_CONSENSUS_REASON_DERIVATION_MISMATCH:{cell_id}")
    if sidecar.get("complete") is not (expected_audit_status == "PASS"):
        errors.append(f"NATIVE_V5_SIDECAR_COMPLETE_MISMATCH:{cell_id}")
    if str(entry.get("consensus_status") or "").upper() != consensus_status:
        errors.append(f"NATIVE_V5_ENTRY_CONSENSUS_STATUS_MISMATCH:{cell_id}")
    if str(sidecar.get("parity_status") or "").upper() != consensus_status:
        errors.append(f"NATIVE_V5_SIDECAR_CONSENSUS_STATUS_MISMATCH:{cell_id}")
    if str(entry.get("status") or "").lower() != normalized_expected:
        errors.append(f"NATIVE_V5_ENTRY_STATUS_MISMATCH:{cell_id}")
    if str(entry.get("universe_eligibility_status") or "").lower() != (
        normalized_expected
    ):
        errors.append(f"NATIVE_V5_ENTRY_DATA_STATE_MISMATCH:{cell_id}")
    if (
        sidecar.get("tradeable") is not False
        or sidecar.get("statistical_sufficiency_receipt_required") is not True
    ):
        errors.append(f"NATIVE_V5_SIDECAR_SUFFICIENCY_GATE_INVALID:{cell_id}")
    if str(sidecar.get("data_state") or "").lower() != normalized_expected:
        errors.append(f"NATIVE_V5_SIDECAR_DATA_STATE_MISMATCH:{cell_id}")
    if sidecar.get("evidence_metadata") != entry.get("evidence_metadata"):
        errors.append(f"NATIVE_V5_EVIDENCE_METADATA_MISMATCH:{cell_id}")
    return list(dict.fromkeys(errors))


def _verify_activation_cell_evidence(
    native_root: Path,
    *,
    symbol: str,
    timeframe: str,
    entry: Any,
    run_cell: Any,
    audited_cell: Any,
    expected_cell_status: str,
    activation_id: str,
    run_id: str,
    run_manifest_relative_path: Any,
    run_manifest_sha256: Any,
    run_request: Mapping[str, Any],
    contract_minimum: float,
    expected_coverage_method: str,
    require_resolution_audit: bool,
    reference_run_cell: Any,
    run_source_environment: Optional[Mapping[str, Any]] = None,
    run_acquisition_started_utc: Any = None,
    run_acquisition_completed_utc: Any = None,
    parity_thresholds: Optional[Mapping[str, Any]] = None,
    symbol_run_cells: Optional[Mapping[str, Mapping[str, Any]]] = None,
    symbol_activation_entries: Optional[Mapping[str, Mapping[str, Any]]] = None,
    v5_frame_cache: Optional[_V5NormalizedFrameCache] = None,
) -> List[str]:
    """Bind one manifest decision to the immutable acquisition/audit bytes.

    Hash pinning alone is insufficient when an operator can edit an activation
    manifest and repin ``ACTIVE.json``.  This validator therefore derives the
    cell's admissible universe state from the authenticated run result,
    provenance, audited sidecar and parity report, and requires every copied
    manifest claim to agree with those records.
    """

    if str(run_request.get("timeframe_request_map_schema_version") or "") == (
        TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        return _verify_v5_activation_cell_evidence(
            native_root,
            symbol=symbol,
            timeframe=timeframe,
            entry=entry,
            run_cell=run_cell,
            audited_cell=audited_cell,
            expected_cell_status=expected_cell_status,
            activation_id=activation_id,
            run_id=run_id,
            run_manifest_relative_path=run_manifest_relative_path,
            run_manifest_sha256=run_manifest_sha256,
            run_request=run_request,
            run_source_environment=run_source_environment or {},
            run_acquisition_started_utc=run_acquisition_started_utc,
            run_acquisition_completed_utc=run_acquisition_completed_utc,
            contract_minimum=contract_minimum,
            parity_thresholds=parity_thresholds or {},
            symbol_run_cells=symbol_run_cells or {},
            symbol_activation_entries=symbol_activation_entries or {},
            frame_cache=v5_frame_cache,
        )

    cell_id = f"{symbol}|{timeframe}"
    legacy_symbol_excluded = (
        str(expected_cell_status or "").strip().lower() == "legacy_excluded_no_trade"
    )
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
        "cross_resolution_reference_timeframe",
        "cross_resolution_reference_bars_sha256",
        "cross_resolution_reference_provenance_sha256",
        "resolution_status",
        "resolution_reason_codes",
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
    if (
        str(entry.get("run_manifest_sha256") or "").lower()
        != str(run_manifest_sha256 or "").lower()
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_RUN_MANIFEST_MISMATCH:{cell_id}")

    classification, coherence_error = classify_native_availability_cell(
        run_cell.get("status"),
        run_cell.get("failure_status"),
        run_cell.get("rows"),
        run_cell.get("resume_disposition"),
    )
    if classification == "invalid":
        errors.append(
            f"NATIVE_CELL_EVIDENCE_RUN_AVAILABILITY_INVALID:{cell_id}:{coherence_error or 'unknown'}"
        )
    if (
        str(run_cell.get("symbol") or "") != symbol
        or str(run_cell.get("timeframe") or "") != timeframe
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_RUN_CELL_ID_MISMATCH:{cell_id}")
    if (
        str(entry.get("bars_sha256") or "").lower()
        != str(run_cell.get("sha256") or "").lower()
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_RUN_BARS_MISMATCH:{cell_id}")
    if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
        reference_map = run_request.get("timeframe_audit_reference_map")
        expected_reference = (
            str(reference_map.get(timeframe) or "").upper()
            if isinstance(reference_map, Mapping)
            else ""
        )
        if str(entry.get("cross_resolution_reference_timeframe") or "").upper() != (
            expected_reference
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_REFERENCE_TIMEFRAME_MISMATCH:{cell_id}"
            )
        if not isinstance(reference_run_cell, Mapping):
            errors.append(f"NATIVE_CELL_EVIDENCE_REFERENCE_RUN_CELL_MISSING:{cell_id}")
        elif classification == "success":
            expected_reference_bars = str(
                reference_run_cell.get("sha256") or ""
            ).lower()
            if (
                str(entry.get("cross_resolution_reference_bars_sha256") or "").lower()
                != expected_reference_bars
            ):
                errors.append(f"NATIVE_CELL_EVIDENCE_REFERENCE_BARS_MISMATCH:{cell_id}")
            reference_provenance_path = (
                native_root
                / "runs"
                / str(run_id)
                / symbol
                / expected_reference
                / "provenance.json"
            )
            reference_provenance_sha = (
                sha256_file(reference_provenance_path)
                if reference_provenance_path.is_file()
                else ""
            )
            if (
                str(
                    entry.get("cross_resolution_reference_provenance_sha256") or ""
                ).lower()
                != reference_provenance_sha
            ):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_REFERENCE_PROVENANCE_MISMATCH:{cell_id}"
                )
        else:
            for field_name in (
                "cross_resolution_reference_bars_sha256",
                "cross_resolution_reference_provenance_sha256",
            ):
                if entry.get(field_name) not in {None, ""}:
                    errors.append(
                        "NATIVE_CELL_EVIDENCE_UNAVAILABLE_REFERENCE_BINDING_INVALID:"
                        f"{cell_id}:{field_name}"
                    )

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
        (
            Path(str(entry.get("bars_relative_path") or "")).parent / "provenance.json"
        ).as_posix()
    )
    if (
        str(provenance_relative or "").replace("\\", "/")
        != expected_provenance_relative
    ):
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
    expected_parity_schema = (
        NATIVE_CROSS_RESOLUTION_PARITY_SCHEMA
        if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD
        else NATIVE_PARITY_SCHEMA
    )
    if str(parity.get("schema_version") or "") != expected_parity_schema:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_SCHEMA_INVALID:{cell_id}")
    if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
        if str(parity.get("activation_target_timeframe") or "").upper() != timeframe:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PARITY_TARGET_TIMEFRAME_MISMATCH:{cell_id}"
            )
        if str(parity.get("reference_timeframe") or "").upper() != (expected_reference):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PARITY_REFERENCE_TIMEFRAME_MISMATCH:{cell_id}"
            )
        if expected_reference in NATIVE_TIMEFRAME_SECONDS:
            if (
                NATIVE_TIMEFRAME_SECONDS[expected_reference]
                < NATIVE_TIMEFRAME_SECONDS[timeframe]
            ):
                expected_fine = expected_reference
                expected_coarse = timeframe
            else:
                expected_fine = timeframe
                expected_coarse = expected_reference
            if str(parity.get("fine_timeframe") or "").upper() != expected_fine:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_PARITY_FINE_TIMEFRAME_MISMATCH:{cell_id}"
                )
            if str(parity.get("coarse_timeframe") or "").upper() != expected_coarse:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_PARITY_COARSE_TIMEFRAME_MISMATCH:{cell_id}"
                )
    if str(provenance.get("schema_version") or "") != NATIVE_BARS_SCHEMA:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_SCHEMA_INVALID:{cell_id}")
    for payload, label in (
        (sidecar, "SIDECAR"),
        (parity, "PARITY"),
        (provenance, "PROVENANCE"),
    ):
        if (
            str(payload.get("symbol") or "") != symbol
            or str(payload.get("timeframe") or "") != timeframe
        ):
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_CELL_MISMATCH:{cell_id}")
        if str(payload.get("run_id") or "") != run_id:
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_RUN_ID_MISMATCH:{cell_id}")
    if str(sidecar.get("activation_id") or "") != activation_id:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_ACTIVATION_MISMATCH:{cell_id}")
    if str(parity.get("activation_id") or "") != activation_id:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_ACTIVATION_MISMATCH:{cell_id}")
    if provenance.get("immutable_record") is not True:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_NOT_IMMUTABLE:{cell_id}")
    if (
        str(provenance.get("acquisition_contract_version") or "")
        != NATIVE_ACQUISITION_CONTRACT
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_CONTRACT_INVALID:{cell_id}")
    if str(provenance.get("canonical_symbol") or "") != symbol:
        errors.append(
            f"NATIVE_CELL_EVIDENCE_PROVENANCE_CANONICAL_SYMBOL_MISMATCH:{cell_id}"
        )
    if provenance.get("timeframe_seconds") != NATIVE_TIMEFRAME_SECONDS.get(timeframe):
        errors.append(
            f"NATIVE_CELL_EVIDENCE_PROVENANCE_TIMEFRAME_SECONDS_MISMATCH:{cell_id}"
        )
    if (
        str(provenance.get("status") or "").lower()
        != str(run_cell.get("status") or "").lower()
        or str(provenance.get("failure_status") or "").lower() != failure
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_STATUS_MISMATCH:{cell_id}")
    if provenance.get("rows") != run_cell.get("rows"):
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_ROWS_MISMATCH:{cell_id}")

    for payload, label in ((sidecar, "SIDECAR"), (parity, "PARITY")):
        if str(payload.get("run_manifest_relative_path") or "").replace(
            "\\", "/"
        ) != str(run_manifest_relative_path or "").replace("\\", "/"):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_{label}_RUN_MANIFEST_PATH_MISMATCH:{cell_id}"
            )
        if (
            str(payload.get("run_manifest_sha256") or "").lower()
            != str(run_manifest_sha256 or "").lower()
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_{label}_RUN_MANIFEST_SHA_MISMATCH:{cell_id}"
            )

    expected_bars_sha = str(entry.get("bars_sha256") or "").lower()
    if str(sidecar.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_SIDECAR_BARS_MISMATCH:{cell_id}")
    if str(parity.get("source_bars_sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_BARS_MISMATCH:{cell_id}")
    if str(provenance.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"NATIVE_CELL_EVIDENCE_PROVENANCE_BARS_MISMATCH:{cell_id}")
    for payload, label in ((parity, "PARITY"), (audited_cell, "AUDIT")):
        if (
            isinstance(payload, Mapping)
            and str(payload.get("source_provenance_sha256") or "").lower()
            != str(provenance_sha or "").lower()
        ):
            errors.append(f"NATIVE_CELL_EVIDENCE_{label}_PROVENANCE_MISMATCH:{cell_id}")
    if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
        for payload, label in (
            (sidecar, "SIDECAR"),
            (parity, "PARITY"),
            (audited_cell, "AUDIT"),
        ):
            if not isinstance(payload, Mapping):
                continue
            for field_name in (
                "cross_resolution_reference_timeframe",
                "cross_resolution_reference_bars_sha256",
                "cross_resolution_reference_provenance_sha256",
            ):
                if payload.get(field_name) != entry.get(field_name):
                    errors.append(
                        f"NATIVE_CELL_EVIDENCE_{label}_REFERENCE_MISMATCH:"
                        f"{cell_id}:{field_name}"
                    )

    expected_coverage = sidecar.get("expected_session_coverage_by_partition")
    expected_observed = sidecar.get("observed_session_bars_by_partition")
    expected_counts = sidecar.get("expected_session_bars_by_partition")
    if (
        classification == "success"
        and expected_coverage_method != NATIVE_SESSION_COVERAGE_METHOD
    ):
        if (
            provenance.get("expected_session_coverage_by_partition")
            != expected_coverage
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_COVERAGE_MISMATCH:{cell_id}"
            )
        if provenance.get("observed_session_bars_by_partition") != expected_observed:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_OBSERVED_MISMATCH:{cell_id}"
            )
        if provenance.get("expected_session_bars_by_partition") != expected_counts:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_EXPECTED_MISMATCH:{cell_id}"
            )
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
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_{label}_OBSERVED_MISMATCH:{cell_id}"
                )
            if payload.get("expected_session_bars_by_partition") != expected_counts:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_{label}_EXPECTED_MISMATCH:{cell_id}"
                )

    provenance_request = (
        provenance.get("request")
        if isinstance(provenance.get("request"), Mapping)
        else {}
    )
    window_contract_version = str(run_request.get("window_contract_version") or "")
    expected_boundaries = run_request.get("common_calendar_boundaries")
    if window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        timeframe_boundaries = run_request.get("timeframe_calendar_boundaries")
        expected_boundaries = (
            timeframe_boundaries.get(timeframe)
            if isinstance(timeframe_boundaries, Mapping)
            else None
        )
        request_fields = [
            "window_contract_version",
            "timeframe_request_map_schema_version",
            "timeframe_window_years",
            "timeframe_calendar_boundaries",
            "timeframe_calendar_boundaries_sha256",
            "timeframe_required_evidence_lead_start_utc",
            "timeframe_source_request_intervals",
            "timeframe_request_map_sha256",
        ]
        if str(run_request.get("timeframe_request_map_schema_version") or "") == (
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        ):
            request_fields.extend(
                [
                    "timeframe_strategy_evidence_intervals",
                    "timeframe_audit_reference_map",
                    "timeframe_audit_only_prefix_intervals",
                ]
            )
        for field_name in request_fields:
            if provenance_request.get(field_name) != run_request.get(field_name):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_PROVENANCE_TIMEFRAME_REQUEST_MISMATCH:{cell_id}:{field_name}"
                )
        expected_years = (
            (run_request.get("timeframe_window_years") or {}).get(timeframe)
            if isinstance(run_request.get("timeframe_window_years"), Mapping)
            else None
        )
        if provenance_request.get("cell_timeframe") != timeframe:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_CELL_TIMEFRAME_MISMATCH:{cell_id}"
            )
        if provenance_request.get("cell_timeframe_window_years") != expected_years:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_CELL_WINDOW_MISMATCH:{cell_id}"
            )
        if provenance_request.get("window_years") != expected_years:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_WINDOW_PROJECTION_MISMATCH:{cell_id}"
            )
        if provenance_request.get("cell_calendar_boundaries") != expected_boundaries:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PROVENANCE_CELL_BOUNDARIES_MISMATCH:{cell_id}"
            )
        if str(run_request.get("timeframe_request_map_schema_version") or "") == (
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        ):
            for leaf_field, map_field in (
                ("strategy_evidence_interval", "timeframe_strategy_evidence_intervals"),
                ("audit_only_prefix_interval", "timeframe_audit_only_prefix_intervals"),
                ("audit_reference_timeframe", "timeframe_audit_reference_map"),
            ):
                source_map = run_request.get(map_field)
                expected_value = (
                    source_map.get(timeframe)
                    if isinstance(source_map, Mapping)
                    else None
                )
                if (
                    leaf_field not in provenance_request
                    or provenance_request.get(leaf_field) != expected_value
                ):
                    errors.append(
                        "NATIVE_CELL_EVIDENCE_PROVENANCE_AUDIT_ROLE_MISMATCH:"
                        f"{cell_id}:{leaf_field}"
                    )
        if sidecar.get("cell_calendar_boundaries") != expected_boundaries:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_SIDECAR_CELL_BOUNDARIES_MISMATCH:{cell_id}"
            )
        if str(sidecar.get("timeframe_calendar_boundaries_sha256") or "") != str(
            run_request.get("timeframe_calendar_boundaries_sha256") or ""
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_SIDECAR_TIMEFRAME_CALENDAR_HASH_MISMATCH:{cell_id}"
            )
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
    if classification == "success" and coverage_method not in (
        SUPPORTED_SOURCE_TIMESTAMP_COVERAGE_METHODS
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_COVERAGE_METHOD_INVALID:{cell_id}")
    if (
        classification == "success"
        and expected_coverage_method != NATIVE_SESSION_COVERAGE_METHOD
        and coverage_method != expected_coverage_method
    ):
        errors.append(
            f"NATIVE_CELL_EVIDENCE_COVERAGE_METHOD_CONTRACT_MISMATCH:{cell_id}"
        )
    if str(entry.get("coverage_method") or "") != expected_coverage_method:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_COVERAGE_METHOD_MISMATCH:{cell_id}")
    if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
        if str(sidecar.get("coverage_method") or "") != expected_coverage_method:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_SIDECAR_COVERAGE_METHOD_MISMATCH:{cell_id}"
            )
        if str(parity.get("coverage_method") or "") != expected_coverage_method:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PARITY_COVERAGE_METHOD_MISMATCH:{cell_id}"
            )
    if (
        isinstance(audited_cell, Mapping)
        and str(audited_cell.get("coverage_method") or "") != expected_coverage_method
    ):
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
    if (
        str(sidecar.get("parity_status") or "").upper()
        != str(parity.get("status") or "").upper()
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_SIDECAR_MISMATCH:{cell_id}")

    resolution_status = "PASS"
    resolution_reasons: List[str] = []
    if require_resolution_audit:
        resolution_audit = sidecar.get("resolution_audit")
        if not isinstance(resolution_audit, Mapping):
            errors.append(f"NATIVE_CELL_EVIDENCE_RESOLUTION_AUDIT_MISSING:{cell_id}")
            resolution_status = "INVALID"
        else:
            resolution_status = (
                str(resolution_audit.get("status") or "").strip().upper()
            )
            resolution_reasons = [
                str(value)
                for value in (resolution_audit.get("reason_codes") or [])
                if str(value)
            ]
            if str(resolution_audit.get("schema_version") or "") != (
                RESOLUTION_AUDIT_SCHEMA_VERSION
            ):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_RESOLUTION_SCHEMA_INVALID:{cell_id}"
                )
            if (
                str(resolution_audit.get("symbol") or "") != symbol
                or str(resolution_audit.get("timeframe") or "") != timeframe
            ):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_RESOLUTION_CELL_MISMATCH:{cell_id}"
                )
            if resolution_status not in {"PASS", "FAIL", "PENDING"}:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_RESOLUTION_STATUS_INVALID:{cell_id}"
                )
        if str(entry.get("resolution_status") or "").upper() != resolution_status:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_ENTRY_RESOLUTION_STATUS_MISMATCH:{cell_id}"
            )
        if list(entry.get("resolution_reason_codes") or []) != resolution_reasons:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_ENTRY_RESOLUTION_REASON_MISMATCH:{cell_id}"
            )
        if isinstance(audited_cell, Mapping):
            if str(audited_cell.get("resolution_status") or "").upper() != (
                resolution_status
            ):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_AUDIT_RESOLUTION_STATUS_MISMATCH:{cell_id}"
                )
            if list(audited_cell.get("resolution_reason_codes") or []) != (
                resolution_reasons
            ):
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_AUDIT_RESOLUTION_REASON_MISMATCH:{cell_id}"
                )
        recorded_resolution_reasons = {
            str(value)
            for value in entry_reasons
            if str(value).startswith("RESOLUTION:")
        }
        expected_resolution_reasons = {
            f"RESOLUTION:{value}" for value in resolution_reasons
        }
        if classification == "success" and (
            recorded_resolution_reasons != expected_resolution_reasons
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_RESOLUTION_REASON_BINDING_MISMATCH:{cell_id}"
            )

    coverage_diagnostic_errors, _normalized = (
        validate_native_session_coverage_diagnostics(expected_coverage)
    )
    errors.extend(
        f"NATIVE_CELL_EVIDENCE_{value}:{cell_id}"
        for value in coverage_diagnostic_errors
    )
    parity_status = str(parity.get("status") or "").upper()
    semantic_cross_resolution_failure = bool(
        expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD
        and parity_status == "FAIL"
    )
    derived_status = "invalid"
    if classification == "unavailable":
        derived_status = "excluded_no_trade"
    elif (
        classification == "success"
        and not coverage_diagnostic_errors
        and resolution_status == "PASS"
        and not semantic_cross_resolution_failure
    ):
        derived_status = "active"
    elif classification == "success" and (
        resolution_status in {"FAIL", "PENDING"} or semantic_cross_resolution_failure
    ):
        derived_status = "excluded_no_trade"
    normalized_expected = str(expected_cell_status or "").strip().lower()
    if legacy_symbol_excluded:
        normalized_expected = "excluded_no_trade"
    if normalized_expected == "eligible":  # v2 whole-symbol compatibility
        normalized_expected = "active"
    if legacy_symbol_excluded:
        if derived_status == "invalid":
            errors.append(
                f"NATIVE_CELL_EVIDENCE_UNIVERSE_STATUS_INVALID:{cell_id}:{derived_status}"
            )
    elif derived_status != normalized_expected:
        errors.append(
            f"NATIVE_CELL_EVIDENCE_UNIVERSE_STATUS_MISMATCH:{cell_id}:{derived_status}!={normalized_expected}"
        )
    if str(entry.get("status") or "").lower() != normalized_expected:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_STATUS_MISMATCH:{cell_id}")
    expected_universe_status = (
        "eligible" if normalized_expected == "active" else normalized_expected
    )
    if (
        str(entry.get("universe_eligibility_status") or "").lower()
        != expected_universe_status
    ):
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_UNIVERSE_STATUS_MISMATCH:{cell_id}")

    if classification == "success":
        if parity_status == "NOT_APPLICABLE":
            reference_classification, _reference_error = (
                classify_native_availability_cell(
                    reference_run_cell.get("status")
                    if isinstance(reference_run_cell, Mapping)
                    else None,
                    (
                        reference_run_cell.get("failure_status")
                        if isinstance(reference_run_cell, Mapping)
                        else None
                    ),
                    reference_run_cell.get("rows")
                    if isinstance(reference_run_cell, Mapping)
                    else None,
                    (
                        reference_run_cell.get("resume_disposition")
                        if isinstance(reference_run_cell, Mapping)
                        else None
                    ),
                )
            )
            reference_failure = (
                str(
                    (
                        reference_run_cell.get("failure_status")
                        if isinstance(reference_run_cell, Mapping)
                        else ""
                    )
                    or ""
                )
                .strip()
                .upper()
            )
            if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
                reference_timeframe = str(
                    entry.get("cross_resolution_reference_timeframe") or ""
                ).upper()
                expected_reason = (
                    f"AUDIT_REFERENCE_UNAVAILABLE:{reference_timeframe}:"
                    f"{reference_failure or 'UNSPECIFIED'}"
                )
            else:
                expected_reason = f"M5_ACQUISITION_UNAVAILABLE:{reference_failure}"
            if reference_classification != "unavailable" or list(
                parity.get("reason_codes") or []
            ) != [expected_reason]:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_PARITY_NOT_APPLICABLE_INVALID:{cell_id}"
                )
        elif (
            parity_status == "FAIL"
            and expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD
        ):
            pass
        elif parity_status != "PASS":
            errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_NOT_PASS:{cell_id}")
        expected_audit_status = (
            "PASS"
            if (
                not coverage_diagnostic_errors
                and resolution_status == "PASS"
                and not semantic_cross_resolution_failure
            )
            else "FAIL"
        )
        if entry_audit_status != expected_audit_status:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_STATUS_DERIVATION_MISMATCH:{cell_id}"
            )
    elif classification == "unavailable" and not legacy_symbol_excluded:
        expected_reason = f"ACQUISITION_UNAVAILABLE:{failure.upper()}"
        if entry_audit_status != "FAIL":
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_STATUS_DERIVATION_MISMATCH:{cell_id}"
            )
        if entry_reasons != [expected_reason]:
            errors.append(f"NATIVE_CELL_EVIDENCE_UNAVAILABLE_REASON_MISMATCH:{cell_id}")
    if isinstance(audited_cell, Mapping):
        if classification == "unavailable":
            expected_source = "ACQUISITION_UNAVAILABLE"
            expected_parity = "NOT_APPLICABLE"
            expected_coverage_status = "NOT_APPLICABLE"
            expected_coverage_reasons: List[str] = []
        else:
            expected_source = "PASS"
            expected_parity = str(parity.get("status") or "").upper()
            expected_coverage_status = "ERROR" if coverage_diagnostic_errors else "PASS"
            expected_coverage_reasons = coverage_diagnostic_errors
        if (
            str(audited_cell.get("source_validation_status") or "").upper()
            != expected_source
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_SOURCE_STATUS_MISMATCH:{cell_id}"
            )
        if str(audited_cell.get("parity_status") or "").upper() != expected_parity:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_PARITY_STATUS_MISMATCH:{cell_id}"
            )
        if (
            str(audited_cell.get("coverage_status") or "").upper()
            != expected_coverage_status
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_COVERAGE_STATUS_MISMATCH:{cell_id}"
            )
        if (
            list(audited_cell.get("coverage_reason_codes") or [])
            != expected_coverage_reasons
        ):
            errors.append(
                f"NATIVE_CELL_EVIDENCE_AUDIT_COVERAGE_REASON_MISMATCH:{cell_id}"
            )

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
        return (
            not self.errors
            and self.data_path is not None
            and self.sidecar_path is not None
        )


@dataclass
class ActiveNativeUniverse:
    """Hash-pinned cell eligibility decision from one activation.

    The authoritative decision is the exact ``symbol x timeframe`` partition
    in ``cell_statuses``.  ``eligible_symbols`` and ``excluded_symbols`` remain
    compatibility projections: eligible means at least one active timeframe,
    while excluded contains only symbols whose complete timeframe grid is
    excluded.  A sparse cell therefore cannot suppress a valid sibling.
    """

    activation_id: Optional[str] = None
    activation_manifest_path: Optional[Path] = None
    activation_manifest_sha256: Optional[str] = None
    requested_symbols: Tuple[str, ...] = ()
    required_timeframes: Tuple[str, ...] = ()
    eligible_symbols: Tuple[str, ...] = ()
    excluded_symbols: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    cell_statuses: Dict[str, Dict[str, str]] = field(default_factory=dict)
    symbols_with_active_cells: Tuple[str, ...] = ()
    fully_excluded_symbols: Tuple[str, ...] = ()
    calendar_contract: Dict[str, Any] = field(default_factory=dict)
    entries: Dict[str, Any] = field(default_factory=dict)
    eligibility_schema: str = ""
    decision_basis: str = ""
    explicit_contract: bool = False
    exact_symbol_identity: bool = False
    errors: List[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors and bool(self.activation_id)

    def _symbol_key(self, symbol: str) -> str:
        value = str(symbol or "").strip()
        return value if self.exact_symbol_identity else value.upper()

    def symbol_status(self, symbol: str) -> str:
        canonical = self._symbol_key(symbol)
        if (
            canonical in self.fully_excluded_symbols
            or canonical in self.excluded_symbols
        ):
            return "excluded_no_trade"
        if (
            canonical in self.symbols_with_active_cells
            or canonical in self.eligible_symbols
        ):
            return "eligible"
        statuses = set((self.cell_statuses.get(canonical) or {}).values())
        if statuses and statuses <= {NATIVE_SOURCE_UNAVAILABLE}:
            return NATIVE_SOURCE_UNAVAILABLE
        return "unbound"

    def cell_status(self, symbol: str, timeframe: str) -> str:
        canonical = self._symbol_key(symbol)
        tf = str(timeframe or "").strip().upper()
        return str((self.cell_statuses.get(canonical) or {}).get(tf) or "unbound")

    def active_timeframes(self, symbol: str) -> Tuple[str, ...]:
        canonical = self._symbol_key(symbol)
        statuses = self.cell_statuses.get(canonical) or {}
        return tuple(
            timeframe
            for timeframe in self.required_timeframes
            if statuses.get(timeframe) in {"active", NATIVE_EVIDENCE_CANDIDATE}
        )

    def source_unavailable_timeframes(self, symbol: str) -> Tuple[str, ...]:
        canonical = self._symbol_key(symbol)
        statuses = self.cell_statuses.get(canonical) or {}
        return tuple(
            timeframe
            for timeframe in self.required_timeframes
            if statuses.get(timeframe) == NATIVE_SOURCE_UNAVAILABLE
        )

    def excluded_timeframes(self, symbol: str) -> Tuple[str, ...]:
        canonical = self._symbol_key(symbol)
        statuses = self.cell_statuses.get(canonical) or {}
        return tuple(
            timeframe
            for timeframe in self.required_timeframes
            if statuses.get(timeframe) == "excluded_no_trade"
        )

    def cell_exclusion_binding(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """Return the immutable binding for one excluded specialist cell."""
        canonical = self._symbol_key(symbol)
        tf = str(timeframe or "").strip().upper()
        if self.cell_status(canonical, tf) != "excluded_no_trade":
            return {}
        entry = (
            (self.entries.get(canonical) or {}).get(tf)
            if isinstance(self.entries.get(canonical), Mapping)
            else None
        )
        if not isinstance(entry, Mapping):
            return {}
        return {
            "schema_version": self.eligibility_schema or UNIVERSE_ELIGIBILITY_SCHEMA,
            "decision_basis": (
                self.decision_basis or NATIVE_SOURCE_EVIDENCE_DECISION_BASIS
            ),
            "activation_id": self.activation_id,
            "activation_manifest_sha256": self.activation_manifest_sha256,
            "symbol": canonical,
            "timeframe": tf,
            "status": "excluded_no_trade",
            "reason_codes": list(entry.get("reason_codes") or []),
        }

    def exclusion_binding(self, symbol: str) -> Dict[str, Any]:
        """Return the exact marker payload a ledger NO_TRADE row must carry."""
        canonical = self._symbol_key(symbol)
        record = dict(self.excluded_symbols.get(canonical) or {})
        if not record:
            return {}
        return {
            "schema_version": self.eligibility_schema or UNIVERSE_ELIGIBILITY_SCHEMA,
            "decision_basis": str(
                record.get("decision_basis") or NATIVE_SOURCE_EVIDENCE_DECISION_BASIS
            ),
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
    pointer_activation_id = str(pointer.get("activation_id") or "")
    canonical_manifest_relative = (
        f"activations/{pointer_activation_id}.json" if pointer_activation_id else ""
    )
    promoted_manifest_relative = (
        f"activations/promoted/{pointer_activation_id}.json"
        if pointer_activation_id
        else ""
    )
    observed_manifest_relative = str(
        pointer.get("manifest_relative_path") or ""
    ).replace("\\", "/")
    promoted_view = observed_manifest_relative == promoted_manifest_relative
    if observed_manifest_relative not in {
        canonical_manifest_relative,
        promoted_manifest_relative,
    }:
        errors.append("NATIVE_ACTIVE_MANIFEST_PATH_IDENTITY_MISMATCH")
    if promoted_view and set(pointer) != set(ACTIVE_POINTER_FIELDS):
        errors.append("NATIVE_ACTIVE_POINTER_FIELDS_INVALID")
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
    if promoted_view:
        errors.extend(
            validate_terminal_promotion_view(
                native_root,
                activation_id=pointer_activation_id,
                view_path=manifest_path,
                view_sha256=observed_sha,
            )
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return (
            None,
            manifest_path,
            observed_sha,
            errors + ["NATIVE_ACTIVE_MANIFEST_INVALID_JSON"],
        )
    if not isinstance(manifest, Mapping):
        return (
            None,
            manifest_path,
            observed_sha,
            errors + ["NATIVE_ACTIVE_MANIFEST_NOT_OBJECT"],
        )
    manifest = dict(manifest)
    if str(manifest.get("schema_version")) != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("NATIVE_ACTIVE_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status", "")).lower() != "active":
        errors.append("NATIVE_ACTIVE_MANIFEST_NOT_ACTIVE")
    if pointer.get("production_authoritative") is not manifest.get(
        "production_authoritative"
    ):
        errors.append("NATIVE_ACTIVE_PRODUCTION_AUTHORITY_MARKER_MISMATCH")
    activation_id = str(manifest.get("activation_id") or "")
    if not activation_id or activation_id != str(pointer.get("activation_id") or ""):
        errors.append("NATIVE_ACTIVE_ACTIVATION_ID_MISMATCH")
    return manifest, manifest_path, observed_sha, list(dict.fromkeys(errors))


@dataclass(frozen=True)
class _ActiveAuthorityIdentity:
    data_dir: Path
    active_pointer_bytes: bytes
    active_pointer_sha256: str
    activation_manifest_sha256: str


def _read_active_authority_identity(data_dir: Path) -> _ActiveAuthorityIdentity:
    """Read the bounded pointer bytes and hash the manifest they name."""

    resolved_data_dir = Path(data_dir).resolve()
    native_root = (resolved_data_dir / "native").resolve()
    pointer_path = native_root / "ACTIVE.json"
    try:
        pointer_size = pointer_path.stat().st_size
    except (OSError, ValueError) as exc:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_READ_FAILED") from exc
    if pointer_size <= 0 or pointer_size > _MAX_ACTIVE_POINTER_BYTES:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_SIZE_INVALID")
    try:
        with pointer_path.open("rb") as handle:
            pointer_bytes = handle.read(_MAX_ACTIVE_POINTER_BYTES + 1)
    except (OSError, ValueError) as exc:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_READ_FAILED") from exc
    if (
        len(pointer_bytes) != pointer_size
        or len(pointer_bytes) > _MAX_ACTIVE_POINTER_BYTES
    ):
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_UNSTABLE")
    try:
        pointer = json.loads(pointer_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_INVALID") from exc
    if not isinstance(pointer, Mapping):
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_INVALID")
    manifest_path = _contained_path(native_root, pointer.get("manifest_relative_path"))
    declared_manifest_sha = str(pointer.get("manifest_sha256") or "").lower()
    if manifest_path is None or not _is_sha256(declared_manifest_sha):
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_INVALID")
    try:
        observed_manifest_sha = sha256_file(manifest_path).lower()
    except (OSError, ValueError) as exc:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_MANIFEST_READ_FAILED") from exc
    if observed_manifest_sha != declared_manifest_sha:
        raise RuntimeError("NATIVE_AUTHORITY_SCOPE_MANIFEST_SHA256_MISMATCH")
    return _ActiveAuthorityIdentity(
        data_dir=resolved_data_dir,
        active_pointer_bytes=pointer_bytes,
        active_pointer_sha256=hashlib.sha256(pointer_bytes).hexdigest(),
        activation_manifest_sha256=observed_manifest_sha,
    )


_PRODUCTION_NATIVE_AUTHORITY_SCOPE: ContextVar[
    Optional["ProductionNativeAuthorityVerificationScope"]
] = ContextVar("fxpm_production_native_authority_verification_scope", default=None)


class ProductionNativeAuthorityVerificationScope:
    """One lexical, process-local reuse boundary for a verified 100x6 authority."""

    def __init__(self) -> None:
        self._scope = secrets.token_hex(32)
        self._pid: Optional[int] = None
        self._thread_id: Optional[int] = None
        self._token: Optional[
            Token[Optional["ProductionNativeAuthorityVerificationScope"]]
        ] = None
        self._entered = False
        self._closed = False
        self._poisoned = False
        self._poison_reason = ""
        self._full_verification_count = 0
        self._identity_revalidation_attempt_count = 0
        self._identity_revalidation_count = 0
        self._identity_revalidation_failure_count = 0
        self._data_dir: Optional[Path] = None
        self._active_pointer_bytes: Optional[bytes] = None
        self._active_pointer_sha256 = ""
        self._activation_manifest_sha256 = ""
        self._universe: Optional[ActiveNativeUniverse] = None

    def __enter__(self) -> "ProductionNativeAuthorityVerificationScope":
        if self._entered or self._closed:
            raise RuntimeError("NATIVE_AUTHORITY_VERIFICATION_SCOPE_NOT_REUSABLE")
        if _PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is not None:
            raise RuntimeError("NATIVE_AUTHORITY_VERIFICATION_SCOPE_NESTED")
        self._pid = os.getpid()
        self._thread_id = threading.get_ident()
        self._token = _PRODUCTION_NATIVE_AUTHORITY_SCOPE.set(self)
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        if exc_type is not None:
            self._poison(f"SCOPE_BODY_EXCEPTION:{exc_type.__name__}")
        self.close()
        return False

    def _poison(self, reason: str) -> None:
        self._poisoned = True
        if not self._poison_reason:
            self._poison_reason = str(reason or "NATIVE_AUTHORITY_SCOPE_POISONED")

    def _assert_live_owner(self) -> None:
        if self._closed or not self._entered:
            self._poison("SCOPE_NOT_ACTIVE")
            raise RuntimeError("NATIVE_AUTHORITY_VERIFICATION_SCOPE_NOT_ACTIVE")
        if self._poisoned:
            raise RuntimeError(
                f"NATIVE_AUTHORITY_VERIFICATION_SCOPE_POISONED:{self._poison_reason}"
            )
        if (
            os.getpid() != self._pid
            or threading.get_ident() != self._thread_id
            or _PRODUCTION_NATIVE_AUTHORITY_SCOPE.get() is not self
        ):
            self._poison("SCOPE_EXECUTION_IDENTITY_MISMATCH")
            raise RuntimeError(
                "NATIVE_AUTHORITY_VERIFICATION_SCOPE_EXECUTION_IDENTITY_MISMATCH"
            )

    def _identity_failure(
        self, reason: str, exc: Optional[BaseException] = None
    ) -> None:
        self._identity_revalidation_failure_count += 1
        self._poison(reason)
        error = RuntimeError(f"NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED:{reason}")
        if exc is None:
            raise error
        raise error from exc

    def _same_sealed_identity(self, identity: _ActiveAuthorityIdentity) -> bool:
        return (
            identity.data_dir == self._data_dir
            and identity.active_pointer_bytes == self._active_pointer_bytes
            and identity.active_pointer_sha256 == self._active_pointer_sha256
            and identity.activation_manifest_sha256 == self._activation_manifest_sha256
        )

    def _resolve(
        self,
        data_dir: Path,
        configured_symbols: Optional[Iterable[str]],
        *,
        required_timeframes: Sequence[str],
        verify_exclusion_evidence: bool,
        production_contract_required: bool,
    ) -> ActiveNativeUniverse:
        try:
            self._assert_live_owner()
        except BaseException:
            if self._universe is not None:
                self._identity_revalidation_attempt_count += 1
                self._identity_revalidation_failure_count += 1
            raise
        if self._universe is None:
            try:
                identity_before = _read_active_authority_identity(data_dir)
            except BaseException as exc:
                self._poison("INITIAL_IDENTITY_READ_FAILED")
                raise RuntimeError(
                    "NATIVE_AUTHORITY_INITIAL_IDENTITY_READ_FAILED"
                ) from exc
            self._full_verification_count += 1
            try:
                universe = _resolve_active_native_universe_uncached(
                    data_dir,
                    configured_symbols,
                    required_timeframes=required_timeframes,
                    verify_exclusion_evidence=verify_exclusion_evidence,
                    production_contract_required=production_contract_required,
                )
            except BaseException:
                self._poison("FULL_VERIFICATION_EXCEPTION")
                raise
            try:
                identity_after = _read_active_authority_identity(data_dir)
            except BaseException as exc:
                self._poison("INITIAL_IDENTITY_REREAD_FAILED")
                raise RuntimeError(
                    "NATIVE_AUTHORITY_INITIAL_IDENTITY_REREAD_FAILED"
                ) from exc
            if identity_before != identity_after:
                self._poison("IDENTITY_CHANGED_DURING_FULL_VERIFICATION")
                raise RuntimeError(
                    "NATIVE_AUTHORITY_IDENTITY_CHANGED_DURING_FULL_VERIFICATION"
                )
            if (
                not universe.valid
                or str(universe.activation_manifest_sha256 or "").lower()
                != identity_after.activation_manifest_sha256
            ):
                self._poison("FULL_VERIFICATION_RESULT_IDENTITY_INVALID")
                return deepcopy(universe)
            try:
                sealed = deepcopy(universe)
                returned = deepcopy(sealed)
            except BaseException:
                self._poison("FULL_VERIFICATION_COPY_FAILED")
                raise
            self._data_dir = identity_after.data_dir
            self._active_pointer_bytes = bytes(identity_after.active_pointer_bytes)
            self._active_pointer_sha256 = identity_after.active_pointer_sha256
            self._activation_manifest_sha256 = identity_after.activation_manifest_sha256
            self._universe = sealed
            return returned

        self._identity_revalidation_attempt_count += 1
        try:
            identity_before = _read_active_authority_identity(data_dir)
            if not self._same_sealed_identity(identity_before):
                self._identity_failure("IDENTITY_BEFORE_MISMATCH")
            returned = deepcopy(self._universe)
            identity_after = _read_active_authority_identity(data_dir)
            if identity_before != identity_after or not self._same_sealed_identity(
                identity_after
            ):
                self._identity_failure("IDENTITY_AFTER_MISMATCH")
        except RuntimeError as exc:
            if str(exc).startswith("NATIVE_AUTHORITY_IDENTITY_REVALIDATION_FAILED:"):
                raise
            self._identity_failure("IDENTITY_READ_OR_COPY_FAILED", exc)
        except BaseException as exc:
            self._identity_failure("IDENTITY_READ_OR_COPY_FAILED", exc)
        self._identity_revalidation_count += 1
        return returned

    def close(self) -> None:
        """Close immediately; the surrounding context-manager exit is idempotent."""

        if self._closed:
            return
        token = self._token
        try:
            if token is not None:
                _PRODUCTION_NATIVE_AUTHORITY_SCOPE.reset(token)
        finally:
            self._token = None
            self._active_pointer_bytes = None
            self._universe = None
            self._data_dir = None
            self._closed = True

    def stats(self) -> Dict[str, Any]:
        """Return a non-authoritative copy of bounded reuse observability."""

        return {
            "schema_version": (PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA),
            "scope": self._scope,
            "full_verification_count": self._full_verification_count,
            "identity_revalidation_attempt_count": (
                self._identity_revalidation_attempt_count
            ),
            "identity_revalidation_count": self._identity_revalidation_count,
            "identity_revalidation_failure_count": (
                self._identity_revalidation_failure_count
            ),
            "active_pointer_sha256": self._active_pointer_sha256,
            "activation_manifest_sha256": self._activation_manifest_sha256,
            "pid": self._pid,
            "thread_id": self._thread_id,
            "closed": self._closed,
            "poisoned": self._poisoned,
            "poison_reason": self._poison_reason or None,
        }


def production_native_authority_verification_scope() -> (
    ProductionNativeAuthorityVerificationScope
):
    """Create an explicit lexical same-run authority verification scope."""

    return ProductionNativeAuthorityVerificationScope()


def _resolve_active_native_universe_uncached(
    data_dir: Path,
    configured_symbols: Optional[Iterable[str]] = None,
    *,
    required_timeframes: Sequence[str] = NATIVE_PRODUCTION_TIMEFRAMES,
    verify_exclusion_evidence: bool = True,
    production_contract_required: bool = False,
    _v5_frame_cache_enabled: bool = True,
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
    if (
        production_contract_required
        and manifest.get("production_authoritative") is not True
    ):
        result.errors.append("NATIVE_ACTIVE_PRODUCTION_AUTHORITY_REQUIRED")
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
        expected_run_manifest_relative = (
            f"runs/{str(manifest.get('run_id') or '')}/run_manifest.json"
        )
        if (
            str(manifest.get("run_manifest_relative_path") or "").replace("\\", "/")
            != expected_run_manifest_relative
        ):
            result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_PATH_IDENTITY_MISMATCH")
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
            if (
                str(run_manifest.get("schema_version") or "")
                != NATIVE_RUN_MANIFEST_SCHEMA
            ):
                result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_SCHEMA_INVALID")
            if str(run_manifest.get("run_id") or "") != str(
                manifest.get("run_id") or ""
            ):
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
            calendar_contract, calendar_errors = _validated_calendar_contract(
                verified_run_request
            )
            result.calendar_contract = calendar_contract
            result.errors.extend(calendar_errors)
    result.activation_id = str(manifest.get("activation_id") or "") or None
    entries = (
        manifest.get("entries") if isinstance(manifest.get("entries"), Mapping) else {}
    )
    result.entries = dict(entries)
    contract = manifest.get("universe_eligibility")
    declared_authenticity_contract = (
        isinstance(contract, Mapping)
        and str(contract.get("schema_version") or "") == UNIVERSE_AUTHENTICITY_SCHEMA
    )
    raw_entry_symbols = [str(value or "").strip() for value in entries]
    verified_request_is_v5 = (
        str(verified_run_request.get("timeframe_request_map_schema_version") or "")
        == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    )
    v5_symbol_identity = verified_request_is_v5 or declared_authenticity_contract
    if declared_authenticity_contract and not verified_request_is_v5:
        result.errors.append("NATIVE_AUTHENTICITY_V5_REQUEST_IDENTITY_REQUIRED")
    result.exact_symbol_identity = v5_symbol_identity
    if v5_symbol_identity:
        if any(not value for value in raw_entry_symbols) or len(
            raw_entry_symbols
        ) != len(set(raw_entry_symbols)):
            result.errors.append("NATIVE_UNIVERSE_ENTRY_SYMBOL_KEYS_NOT_EXACT_UNIQUE")
        if production_contract_required and set(raw_entry_symbols) != set(
            NATIVE_PRODUCTION_SYMBOLS
        ):
            result.errors.append(
                "NATIVE_UNIVERSE_PRODUCTION_SYMBOL_ORDER_IDENTITY_MISMATCH"
            )
    else:
        canonical_entry_symbols = [value.upper() for value in raw_entry_symbols]
        if any(
            raw != canonical
            for raw, canonical in zip(raw_entry_symbols, canonical_entry_symbols)
        ) or len(canonical_entry_symbols) != len(set(canonical_entry_symbols)):
            result.errors.append(
                "NATIVE_UNIVERSE_ENTRY_SYMBOL_KEYS_NOT_CANONICAL_UNIQUE"
            )
    required = tuple(
        dict.fromkeys(
            str(timeframe or "").strip().upper()
            for timeframe in required_timeframes
            if str(timeframe or "").strip()
        )
    )
    raw_configured = tuple(
        str(symbol or "").strip()
        for symbol in (configured_symbols or ())
        if str(symbol or "").strip()
    )
    configured = (
        raw_configured
        if v5_symbol_identity
        else tuple(sorted({value.upper() for value in raw_configured}))
    )
    if production_contract_required and not isinstance(contract, Mapping):
        result.errors.append("NATIVE_UNIVERSE_PRODUCTION_EXPLICIT_CONTRACT_REQUIRED")
    if isinstance(contract, Mapping):
        result.explicit_contract = True
        contract_schema = str(contract.get("schema_version") or "")
        result.eligibility_schema = contract_schema
        cell_contract = contract_schema == UNIVERSE_ELIGIBILITY_SCHEMA
        authenticity_contract = contract_schema == UNIVERSE_AUTHENTICITY_SCHEMA
        if contract_schema not in {
            UNIVERSE_AUTHENTICITY_SCHEMA,
            UNIVERSE_ELIGIBILITY_SCHEMA,
            LEGACY_UNIVERSE_ELIGIBILITY_SCHEMA,
        }:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_SCHEMA_INVALID")
        contract_decision_basis = str(contract.get("decision_basis") or "")
        result.decision_basis = contract_decision_basis
        if contract_decision_basis not in {
            NATIVE_AUTHENTICITY_DECISION_BASIS,
            NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
            LEGACY_SOURCE_AVAILABILITY_DECISION_BASIS,
        }:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_BASIS_INVALID")
        if contract.get("performance_inputs_used") is not False:
            result.errors.append(
                "NATIVE_UNIVERSE_ELIGIBILITY_PERFORMANCE_INPUTS_NOT_FORBIDDEN"
            )
        if authenticity_contract:
            contract_minimum = NATIVE_MINIMUM_SESSION_COVERAGE
            if contract.get("fixed_coverage_activation_threshold") is not None:
                result.errors.append(
                    "NATIVE_AUTHENTICITY_FIXED_COVERAGE_THRESHOLD_PRESENT"
                )
            if manifest.get("minimum_session_coverage") is not None:
                result.errors.append(
                    "NATIVE_AUTHENTICITY_MANIFEST_COVERAGE_THRESHOLD_PRESENT"
                )
            if manifest.get("coverage_activation_threshold_removed") is not True:
                result.errors.append(
                    "NATIVE_AUTHENTICITY_THRESHOLD_REMOVAL_MARKER_INVALID"
                )
        else:
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
        contract_method = str(contract.get("session_coverage_method") or "")
        if contract_method not in SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS or (
            not cell_contract
            and not authenticity_contract
            and contract_method != LEGACY_NATIVE_SESSION_COVERAGE_METHOD
        ):
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_METHOD_INVALID")
        if production_contract_required:
            if contract_schema != UNIVERSE_AUTHENTICITY_SCHEMA:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CURRENT_ELIGIBILITY_SCHEMA_REQUIRED"
                )
            if contract_method != NATIVE_SESSION_COVERAGE_METHOD:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CROSS_RESOLUTION_REQUIRED"
                )
            if (
                str(
                    verified_run_request.get("timeframe_request_map_schema_version")
                    or ""
                )
                != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
            ):
                result.errors.append("NATIVE_UNIVERSE_PRODUCTION_TIMEFRAME_V5_REQUIRED")
            if contract_decision_basis != NATIVE_AUTHENTICITY_DECISION_BASIS:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_SOURCE_EVIDENCE_BASIS_REQUIRED"
                )
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
            production_contract_required
            and declared_required != NATIVE_PRODUCTION_TIMEFRAMES
        ):
            result.errors.append(
                "NATIVE_UNIVERSE_PRODUCTION_SIX_TIMEFRAME_ROSTER_REQUIRED"
            )
        if list(declared_required) != raw_declared_required or len(
            declared_required
        ) != len(set(declared_required)):
            result.errors.append(
                "NATIVE_UNIVERSE_REQUIRED_TIMEFRAMES_NOT_CANONICAL_UNIQUE"
            )
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
        if v5_symbol_identity:
            if len(raw_requested) != len(set(raw_requested)):
                result.errors.append(
                    "NATIVE_UNIVERSE_REQUESTED_SYMBOLS_NOT_EXACT_UNIQUE"
                )
            if len(raw_eligible) != len(set(raw_eligible)):
                result.errors.append(
                    "NATIVE_UNIVERSE_ELIGIBLE_SYMBOLS_NOT_EXACT_UNIQUE"
                )
            requested = tuple(raw_requested)
            eligible = tuple(raw_eligible)
            if production_contract_required and requested != NATIVE_PRODUCTION_SYMBOLS:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_REQUESTED_SYMBOL_ORDER_MISMATCH"
                )
        else:
            requested_canonical = [value.upper() for value in raw_requested]
            eligible_canonical = [value.upper() for value in raw_eligible]
            if raw_requested != requested_canonical or len(requested_canonical) != len(
                set(requested_canonical)
            ):
                result.errors.append(
                    "NATIVE_UNIVERSE_REQUESTED_SYMBOLS_NOT_CANONICAL_UNIQUE"
                )
            if raw_eligible != eligible_canonical or len(eligible_canonical) != len(
                set(eligible_canonical)
            ):
                result.errors.append(
                    "NATIVE_UNIVERSE_ELIGIBLE_SYMBOLS_NOT_CANONICAL_UNIQUE"
                )
            requested = tuple(sorted(set(requested_canonical)))
            eligible = tuple(sorted(set(eligible_canonical)))
        raw_excluded = (
            contract.get("excluded_symbols")
            if isinstance(contract.get("excluded_symbols"), Mapping)
            else {}
        )
        raw_excluded_symbols = [str(value or "").strip() for value in raw_excluded]
        canonical_excluded_symbols = (
            raw_excluded_symbols
            if v5_symbol_identity
            else [value.upper() for value in raw_excluded_symbols]
        )
        if (
            not v5_symbol_identity
            and raw_excluded_symbols != canonical_excluded_symbols
        ) or len(canonical_excluded_symbols) != len(set(canonical_excluded_symbols)):
            result.errors.append(
                "NATIVE_UNIVERSE_EXCLUDED_SYMBOL_KEYS_NOT_CANONICAL_UNIQUE"
            )
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
            if v5_symbol_identity:
                if len(raw_run_symbols) != len(set(raw_run_symbols)):
                    result.errors.append("NATIVE_UNIVERSE_RUN_SYMBOLS_NOT_EXACT_UNIQUE")
                if tuple(raw_run_symbols) != requested:
                    result.errors.append(
                        "NATIVE_UNIVERSE_RUN_SYMBOL_ORDER_IDENTITY_MISMATCH"
                    )
                if production_contract_required and tuple(raw_run_symbols) != (
                    NATIVE_PRODUCTION_SYMBOLS
                ):
                    result.errors.append(
                        "NATIVE_UNIVERSE_PRODUCTION_RUN_SYMBOL_ORDER_MISMATCH"
                    )
            elif raw_run_symbols != [value.upper() for value in raw_run_symbols] or len(
                raw_run_symbols
            ) != len(set(raw_run_symbols)):
                result.errors.append("NATIVE_UNIVERSE_RUN_SYMBOLS_NOT_CANONICAL_UNIQUE")
            if raw_run_timeframes != [
                value.upper() for value in raw_run_timeframes
            ] or len(raw_run_timeframes) != len(set(raw_run_timeframes)):
                result.errors.append(
                    "NATIVE_UNIVERSE_RUN_TIMEFRAMES_NOT_CANONICAL_UNIQUE"
                )
            run_symbols = (
                tuple(raw_run_symbols)
                if v5_symbol_identity
                else {
                    str(value or "").strip().upper()
                    for value in (run_request.get("symbols") or [])
                    if str(value or "").strip()
                }
            )
            run_timeframes = tuple(
                str(value or "").strip().upper()
                for value in (run_request.get("timeframes") or [])
                if str(value or "").strip()
            )
            if (
                run_symbols != requested
                if v5_symbol_identity
                else run_symbols != set(requested)
            ):
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
                        (
                            str(item.get("symbol") or "").strip()
                            if v5_symbol_identity
                            else str(item.get("symbol") or "").strip().upper()
                        ),
                        str(item.get("timeframe") or "").strip().upper(),
                    )
                    if (
                        str(item.get("symbol") or "").strip() != key[0]
                        or str(item.get("timeframe") or "").strip() != key[1]
                    ):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_RUN_CELL_ID_NOT_CANONICAL:{key[0]}|{key[1]}"
                        )
                    if key in run_cells:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_RUN_DUPLICATE_CELL:{key[0]}|{key[1]}"
                        )
                    run_cells[key] = item
                    if (
                        v5_symbol_identity
                        and str(item.get("relative_leaf") or "").replace("\\", "/")
                        != f"{key[0]}/{key[1]}"
                    ):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_RUN_CELL_PATH_IDENTITY_MISMATCH:{key[0]}|{key[1]}"
                        )
                expected_run_cells = {
                    (symbol, timeframe)
                    for symbol in requested
                    for timeframe in tuple(declared_required or required)
                }
                if set(run_cells) != expected_run_cells:
                    result.errors.append("NATIVE_UNIVERSE_RUN_CELL_ROSTER_MISMATCH")
        if authenticity_contract:
            allowed_contract_fields = {
                "schema_version",
                "decision_basis",
                "performance_inputs_used",
                "trading_verdicts_emitted",
                "coverage_is_diagnostic_only",
                "fixed_coverage_activation_threshold",
                "session_coverage_method",
                "session_coverage_partitions",
                "requested_symbols",
                "required_timeframes",
                "cell_statuses",
                "terminal_states",
                "nonterminal_states",
                "evidence_candidate_timeframes_by_symbol",
                "source_unavailable_timeframes_by_symbol",
                "symbols_with_evidence_candidates",
                "eligible_symbols",
                "excluded_symbols",
                "statistical_sufficiency_authority",
            }
            if set(contract) != allowed_contract_fields:
                result.errors.append("NATIVE_AUTHENTICITY_CONTRACT_FIELDS_INVALID")
            if contract.get("trading_verdicts_emitted") is not False:
                result.errors.append("NATIVE_AUTHENTICITY_TRADING_VERDICT_FLAG_INVALID")
            if contract.get("coverage_is_diagnostic_only") is not True:
                result.errors.append("NATIVE_AUTHENTICITY_COVERAGE_ROLE_INVALID")
            declared_terminal_states = list(contract.get("terminal_states") or [])
            declared_nonterminal_states = list(contract.get("nonterminal_states") or [])
            current_terminal_states = [
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
                NATIVE_TECHNICAL_ERROR,
            ]
            legacy_terminal_states = [
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
            ]
            if tuple(declared_terminal_states) not in {
                tuple(current_terminal_states),
                tuple(legacy_terminal_states),
            }:
                result.errors.append("NATIVE_AUTHENTICITY_TERMINAL_STATES_INVALID")
            if (
                declared_terminal_states == current_terminal_states
                and declared_nonterminal_states != [NATIVE_PENDING]
            ) or (
                declared_terminal_states == legacy_terminal_states
                and declared_nonterminal_states
                != [NATIVE_TECHNICAL_ERROR, NATIVE_PENDING]
            ):
                result.errors.append("NATIVE_AUTHENTICITY_NONTERMINAL_STATES_INVALID")
            published_terminal_states = frozenset(declared_terminal_states)
            if str(contract.get("statistical_sufficiency_authority") or "") != (
                "authenticated_horizon_ess_trade_power_and_multiplicity_v1"
            ):
                result.errors.append(
                    "NATIVE_AUTHENTICITY_SUFFICIENCY_AUTHORITY_INVALID"
                )
            if raw_excluded:
                result.errors.append("NATIVE_AUTHENTICITY_EXCLUDED_SYMBOLS_FORBIDDEN")

            expected_timeframes = tuple(declared_required or required)
            requested_set = set(requested)
            if set(entries) != requested_set:
                result.errors.append("NATIVE_AUTHENTICITY_ENTRY_SYMBOL_GRID_INVALID")
            raw_cell_statuses = contract.get("cell_statuses")
            parsed_statuses: Dict[str, Dict[str, str]] = {}
            if (
                not isinstance(raw_cell_statuses, Mapping)
                or set(raw_cell_statuses) != requested_set
            ):
                result.errors.append("NATIVE_AUTHENTICITY_CELL_SYMBOL_GRID_INVALID")
                raw_cell_statuses = {}
            for symbol in requested:
                raw_grid = raw_cell_statuses.get(symbol)
                raw_grid_keys = (
                    [str(value or "").strip() for value in raw_grid]
                    if isinstance(raw_grid, Mapping)
                    else []
                )
                if (
                    not isinstance(raw_grid, Mapping)
                    or raw_grid_keys != [value.upper() for value in raw_grid_keys]
                    or len(raw_grid_keys) != len(set(raw_grid_keys))
                    or set(raw_grid_keys) != set(expected_timeframes)
                ):
                    result.errors.append(
                        f"NATIVE_AUTHENTICITY_CELL_TIMEFRAME_GRID_INVALID:{symbol}"
                    )
                    raw_grid = {}
                symbol_entries = (
                    entries.get(symbol)
                    if isinstance(entries.get(symbol), Mapping)
                    else {}
                )
                if set(symbol_entries) != set(expected_timeframes):
                    result.errors.append(
                        f"NATIVE_AUTHENTICITY_ENTRY_TIMEFRAME_GRID_INVALID:{symbol}"
                    )
                parsed_statuses[symbol] = {}
                for timeframe in expected_timeframes:
                    cell_id = f"{symbol}|{timeframe}"
                    state = str(raw_grid.get(timeframe) or "").strip().lower()
                    if state not in published_terminal_states:
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_NONTERMINAL_PUBLISHED:{cell_id}:{state or 'missing'}"
                        )
                    parsed_statuses[symbol][timeframe] = state
                    entry = (
                        symbol_entries.get(timeframe)
                        if isinstance(symbol_entries.get(timeframe), Mapping)
                        else None
                    )
                    if not isinstance(entry, Mapping):
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_ENTRY_MISSING:{cell_id}"
                        )
                        continue
                    if str(entry.get("status") or "").lower() != state:
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_ENTRY_STATUS_MISMATCH:{cell_id}"
                        )
                    if entry.get("tradeable") is not False:
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_ENTRY_TRADEABLE_FLAG_INVALID:{cell_id}"
                        )
                    if (
                        entry.get("statistical_sufficiency_receipt_required")
                        is not True
                    ):
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_ENTRY_SUFFICIENCY_GATE_INVALID:{cell_id}"
                        )
                    if (
                        str(entry.get("universe_eligibility_status") or "").lower()
                        != state
                    ):
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_ENTRY_DATA_STATE_MISMATCH:{cell_id}"
                        )

            def _projection(field_name: str, state: str) -> Dict[str, List[str]]:
                raw_projection = contract.get(field_name)
                if (
                    not isinstance(raw_projection, Mapping)
                    or set(raw_projection) != requested_set
                ):
                    result.errors.append(
                        f"NATIVE_AUTHENTICITY_{field_name.upper()}_GRID_INVALID"
                    )
                    raw_projection = {}
                derived: Dict[str, List[str]] = {}
                for symbol in requested:
                    expected_values = [
                        timeframe
                        for timeframe in expected_timeframes
                        if parsed_statuses.get(symbol, {}).get(timeframe) == state
                    ]
                    if list(raw_projection.get(symbol) or []) != expected_values:
                        result.errors.append(
                            f"NATIVE_AUTHENTICITY_{field_name.upper()}_MISMATCH:{symbol}"
                        )
                    derived[symbol] = expected_values
                return derived

            candidate_projection = _projection(
                "evidence_candidate_timeframes_by_symbol",
                NATIVE_EVIDENCE_CANDIDATE,
            )
            _projection(
                "source_unavailable_timeframes_by_symbol",
                NATIVE_SOURCE_UNAVAILABLE,
            )
            derived_candidate_symbols = tuple(
                symbol for symbol in requested if candidate_projection.get(symbol)
            )
            for field_name in (
                "symbols_with_evidence_candidates",
                "eligible_symbols",
            ):
                raw_values = [
                    str(value or "").strip()
                    for value in (contract.get(field_name) or [])
                    if str(value or "").strip()
                ]
                if tuple(raw_values) != derived_candidate_symbols:
                    result.errors.append(
                        f"NATIVE_AUTHENTICITY_{field_name.upper()}_MISMATCH"
                    )

            result.requested_symbols = requested
            result.required_timeframes = expected_timeframes
            result.cell_statuses = parsed_statuses
            result.symbols_with_active_cells = derived_candidate_symbols
            result.eligible_symbols = derived_candidate_symbols
            result.excluded_symbols = {}
            result.fully_excluded_symbols = ()

            if configured:
                if v5_symbol_identity and configured != requested:
                    result.errors.append(
                        "NATIVE_UNIVERSE_CONFIG_SYMBOL_ORDER_IDENTITY_MISMATCH"
                    )
                for symbol in sorted(set(configured) - requested_set):
                    result.errors.append(f"NATIVE_UNIVERSE_SYMBOL_UNBOUND:{symbol}")
                for symbol in sorted(requested_set - set(configured)):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_CONFIG_ROSTER_MISSING_SYMBOL:{symbol}"
                    )

            if verify_exclusion_evidence or production_contract_required:
                for symbol in requested:
                    symbol_entries = entries.get(symbol) or {}
                    symbol_run_cells = {
                        candidate_timeframe: candidate_cell
                        for (
                            candidate_symbol,
                            candidate_timeframe,
                        ), candidate_cell in run_cells.items()
                        if candidate_symbol == symbol
                    }
                    frame_cache = (
                        _V5NormalizedFrameCache(symbol)
                        if _v5_frame_cache_enabled
                        else None
                    )
                    try:
                        for timeframe in expected_timeframes:
                            result.errors.extend(
                                _verify_activation_cell_evidence(
                                    native_root,
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    entry=symbol_entries.get(timeframe),
                                    run_cell=run_cells.get((symbol, timeframe)),
                                    audited_cell=None,
                                    expected_cell_status=parsed_statuses[symbol][
                                        timeframe
                                    ],
                                    activation_id=str(result.activation_id or ""),
                                    run_id=str(manifest.get("run_id") or ""),
                                    run_manifest_relative_path=manifest.get(
                                        "run_manifest_relative_path"
                                    ),
                                    run_manifest_sha256=manifest.get(
                                        "run_manifest_sha256"
                                    ),
                                    run_request=verified_run_request,
                                    run_source_environment=(
                                        verified_run_manifest.get("source_environment")
                                        if isinstance(verified_run_manifest, Mapping)
                                        and isinstance(
                                            verified_run_manifest.get(
                                                "source_environment"
                                            ),
                                            Mapping,
                                        )
                                        else {}
                                    ),
                                    run_acquisition_started_utc=(
                                        verified_run_manifest.get(
                                            "acquisition_started_utc"
                                        )
                                        if isinstance(verified_run_manifest, Mapping)
                                        else None
                                    ),
                                    run_acquisition_completed_utc=(
                                        verified_run_manifest.get(
                                            "acquisition_completed_utc"
                                        )
                                        if isinstance(verified_run_manifest, Mapping)
                                        else None
                                    ),
                                    contract_minimum=NATIVE_MINIMUM_SESSION_COVERAGE,
                                    expected_coverage_method=contract_method,
                                    require_resolution_audit=True,
                                    reference_run_cell=None,
                                    parity_thresholds=(
                                        manifest.get("parity_thresholds")
                                        if isinstance(
                                            manifest.get("parity_thresholds"), Mapping
                                        )
                                        else {}
                                    ),
                                    symbol_run_cells=symbol_run_cells,
                                    symbol_activation_entries=(
                                        symbol_entries
                                        if isinstance(symbol_entries, Mapping)
                                        else {}
                                    ),
                                    v5_frame_cache=frame_cache,
                                )
                            )
                    finally:
                        if frame_cache is not None:
                            frame_cache.clear()

            raw_counts = manifest.get("counts")
            if not isinstance(raw_counts, Mapping):
                result.errors.append("NATIVE_AUTHENTICITY_COUNTS_MISSING")
            else:
                expected_counts = {
                    state: sum(
                        value == state
                        for grid in parsed_statuses.values()
                        for value in grid.values()
                    )
                    for state in (
                        NATIVE_EVIDENCE_CANDIDATE,
                        NATIVE_SOURCE_UNAVAILABLE,
                        NATIVE_TECHNICAL_ERROR,
                        NATIVE_PENDING,
                    )
                }
                expected_counts.update(
                    {
                        "total": len(requested) * len(expected_timeframes),
                        "terminal": sum(
                            value in published_terminal_states
                            for grid in parsed_statuses.values()
                            for value in grid.values()
                        ),
                        "symbols_with_evidence_candidates": len(
                            derived_candidate_symbols
                        ),
                    }
                )
                if dict(raw_counts) != expected_counts:
                    result.errors.append("NATIVE_AUTHENTICITY_COUNTS_MISMATCH")
                if expected_counts["terminal"] != expected_counts["total"]:
                    result.errors.append(
                        "NATIVE_AUTHENTICITY_PUBLISHED_TERMINAL_GRID_INCOMPLETE"
                    )
            return result
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
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_FIELDS_INVALID:{symbol}"
                )
            if str(record.get("status") or "") != "excluded_no_trade":
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_STATUS_INVALID:{symbol}"
                )
            if str(record.get("decision_basis") or "") != contract_decision_basis:
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_BASIS_INVALID:{symbol}"
                )
            reasons = [
                str(value) for value in (record.get("reason_codes") or []) if str(value)
            ]
            if not reasons:
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_REASONS_MISSING:{symbol}"
                )
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
            expected_evidence_reason_codes: List[str] = []
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
                    cell.get("resume_disposition"),
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
                else:
                    expected_evidence_reason_codes.extend(
                        f"{timeframe}:{reason}"
                        for reason in manifest_entry.get("reason_codes") or []
                        if str(reason)
                        and (
                            contract_method == NATIVE_SESSION_COVERAGE_METHOD
                            or str(reason).startswith(
                                ("SESSION_COVERAGE_", "RESOLUTION:")
                            )
                        )
                    )
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
                    "resolution_status",
                    "resolution_reason_codes",
                    "expected_session_coverage_by_partition",
                    "observed_session_bars_by_partition",
                    "expected_session_bars_by_partition",
                    "coverage_method",
                    "source_provenance_sha256",
                    "cross_resolution_reference_timeframe",
                    "cross_resolution_reference_bars_sha256",
                    "cross_resolution_reference_provenance_sha256",
                    "audited_sidecar_sha256",
                    "parity_sha256",
                }
                if (
                    str(
                        verified_run_request.get("timeframe_request_map_schema_version")
                        or ""
                    )
                    == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                ):
                    allowed_audit_fields.update(
                        {
                            "cross_resolution_reference_timeframes",
                            "cross_resolution_reference_bindings",
                            "consensus_disagreement_count",
                            "consensus_window_diagnostics",
                        }
                    )
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
                    unavailable_parity_status = (
                        "FAIL"
                        if str(
                            verified_run_request.get(
                                "timeframe_request_map_schema_version"
                            )
                            or ""
                        )
                        == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                        else "NOT_APPLICABLE"
                    )
                    if (
                        parity_status != unavailable_parity_status
                        or coverage_status != "NOT_APPLICABLE"
                    ):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_UNAVAILABLE_AUDIT_INVALID:{symbol}|{timeframe}"
                        )
                    continue
                if source_status != "PASS":
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_SOURCE_STATUS_INVALID:{symbol}|{timeframe}"
                    )
                history_status, history_reasons, _normalized = (
                    classify_native_session_history(
                        audit_cell.get("expected_session_coverage_by_partition"),
                        minimum=contract_minimum,
                    )
                )
                parity_status_valid = parity_status == "PASS"
                semantic_cross_resolution_failure = bool(
                    contract_method == NATIVE_SESSION_COVERAGE_METHOD
                    and parity_status == "FAIL"
                )
                if semantic_cross_resolution_failure:
                    parity_status_valid = True
                    unavailable_count += 1
                if parity_status == "NOT_APPLICABLE":
                    if contract_method == NATIVE_SESSION_COVERAGE_METHOD:
                        reference_timeframe = str(
                            audit_cell.get("cross_resolution_reference_timeframe") or ""
                        ).upper()
                    else:
                        reference_timeframe = "M5" if timeframe != "M5" else ""
                    reference_cell = run_cells.get((symbol, reference_timeframe))
                    reference_classification, _reference_error = (
                        classify_native_availability_cell(
                            reference_cell.get("status")
                            if isinstance(reference_cell, Mapping)
                            else None,
                            (
                                reference_cell.get("failure_status")
                                if isinstance(reference_cell, Mapping)
                                else None
                            ),
                            reference_cell.get("rows")
                            if isinstance(reference_cell, Mapping)
                            else None,
                            (
                                reference_cell.get("resume_disposition")
                                if isinstance(reference_cell, Mapping)
                                else None
                            ),
                        )
                    )
                    parity_status_valid = reference_classification == "unavailable"
                if not parity_status_valid:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_PARITY_STATUS_INVALID:{symbol}|{timeframe}"
                    )
                if str(audit_cell.get("coverage_method") or "") != contract_method:
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_COVERAGE_METHOD_INVALID:{symbol}|{timeframe}"
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
                if cell_contract:
                    resolution_status = str(
                        audit_cell.get("resolution_status") or ""
                    ).upper()
                    if resolution_status not in {"PASS", "FAIL", "PENDING"}:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_EXCLUSION_RESOLUTION_STATUS_INVALID:{symbol}|{timeframe}"
                        )
                    elif resolution_status != "PASS":
                        unavailable_count += 1
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
                if (
                    str(audit_cell.get("audited_sidecar_sha256") or "").lower()
                    != str(manifest_entry.get("sidecar_sha256") or "").lower()
                ):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_EXCLUSION_SIDECAR_BINDING_MISMATCH:{symbol}|{timeframe}"
                    )
                if (
                    str(audit_cell.get("parity_sha256") or "").lower()
                    != str(manifest_entry.get("parity_sha256") or "").lower()
                ):
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
                        *expected_evidence_reason_codes,
                    ]
                )
            )
            if reasons != expected_reason_codes:
                result.errors.append(
                    f"NATIVE_UNIVERSE_EXCLUSION_REASON_BINDING_MISMATCH:{symbol}"
                )
            excluded[symbol] = record

        requested_set = set(requested)
        if cell_contract:
            raw_cell_statuses = contract.get("cell_statuses")
            parsed_cell_statuses: Dict[str, Dict[str, str]] = {}
            if not isinstance(raw_cell_statuses, Mapping):
                result.errors.append("NATIVE_UNIVERSE_CELL_STATUSES_MISSING")
                raw_cell_statuses = {}
            raw_cell_symbols = [str(value or "").strip() for value in raw_cell_statuses]
            if (
                raw_cell_symbols != [value.upper() for value in raw_cell_symbols]
                or len(raw_cell_symbols) != len(set(raw_cell_symbols))
                or set(raw_cell_symbols) != requested_set
            ):
                result.errors.append("NATIVE_UNIVERSE_CELL_SYMBOL_GRID_INVALID")
            expected_timeframes = tuple(declared_required or required)
            for symbol in requested:
                raw_grid = raw_cell_statuses.get(symbol)
                if not isinstance(raw_grid, Mapping):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_CELL_TIMEFRAME_GRID_MISSING:{symbol}"
                    )
                    raw_grid = {}
                raw_keys = [str(value or "").strip() for value in raw_grid]
                if (
                    raw_keys != [value.upper() for value in raw_keys]
                    or len(raw_keys) != len(set(raw_keys))
                    or set(raw_keys) != set(expected_timeframes)
                ):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_CELL_TIMEFRAME_GRID_INVALID:{symbol}"
                    )
                grid: Dict[str, str] = {}
                symbol_entries = (
                    entries.get(symbol)
                    if isinstance(entries.get(symbol), Mapping)
                    else {}
                )
                for timeframe in expected_timeframes:
                    status = str(raw_grid.get(timeframe) or "").strip().lower()
                    if status not in {"active", "excluded_no_trade"}:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_CELL_STATUS_INVALID:{symbol}|{timeframe}"
                        )
                    grid[timeframe] = status
                    entry = (
                        symbol_entries.get(timeframe)
                        if isinstance(symbol_entries, Mapping)
                        else None
                    )
                    if (
                        not isinstance(entry, Mapping)
                        or str(entry.get("status") or "").strip().lower() != status
                    ):
                        result.errors.append(
                            f"NATIVE_UNIVERSE_CELL_ENTRY_STATUS_MISMATCH:{symbol}|{timeframe}"
                        )
                parsed_cell_statuses[symbol] = grid

            derived_active = tuple(
                sorted(
                    symbol
                    for symbol, grid in parsed_cell_statuses.items()
                    if any(status == "active" for status in grid.values())
                )
            )
            derived_fully_excluded = tuple(
                sorted(
                    symbol
                    for symbol, grid in parsed_cell_statuses.items()
                    if grid
                    and all(status == "excluded_no_trade" for status in grid.values())
                )
            )

            def _canonical_declared_list(field_name: str) -> Tuple[str, ...]:
                raw_values = [
                    str(value or "").strip()
                    for value in (contract.get(field_name) or [])
                    if str(value or "").strip()
                ]
                canonical = [value.upper() for value in raw_values]
                if raw_values != canonical or len(canonical) != len(set(canonical)):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_{field_name.upper()}_NOT_CANONICAL_UNIQUE"
                    )
                return tuple(sorted(set(canonical)))

            declared_active = _canonical_declared_list("symbols_with_active_cells")
            declared_fully_excluded = _canonical_declared_list("fully_excluded_symbols")
            if declared_active != derived_active:
                result.errors.append(
                    "NATIVE_UNIVERSE_SYMBOLS_WITH_ACTIVE_CELLS_DERIVATION_MISMATCH"
                )
            if declared_fully_excluded != derived_fully_excluded:
                result.errors.append(
                    "NATIVE_UNIVERSE_FULLY_EXCLUDED_SYMBOLS_DERIVATION_MISMATCH"
                )
            if tuple(sorted(set(eligible))) != derived_active:
                result.errors.append("NATIVE_UNIVERSE_ELIGIBLE_PROJECTION_MISMATCH")
            if set(excluded) != set(derived_fully_excluded):
                result.errors.append("NATIVE_UNIVERSE_EXCLUDED_PROJECTION_MISMATCH")

            for field_name, target_status in (
                ("active_timeframes_by_symbol", "active"),
                ("excluded_timeframes_by_symbol", "excluded_no_trade"),
            ):
                raw_projection = contract.get(field_name)
                if (
                    not isinstance(raw_projection, Mapping)
                    or set(raw_projection) != requested_set
                ):
                    result.errors.append(
                        f"NATIVE_UNIVERSE_{field_name.upper()}_GRID_INVALID"
                    )
                    raw_projection = {}
                for symbol in requested:
                    raw_values = [
                        str(value or "").strip()
                        for value in (raw_projection.get(symbol) or [])
                        if str(value or "").strip()
                    ]
                    expected_values = [
                        timeframe
                        for timeframe in expected_timeframes
                        if parsed_cell_statuses.get(symbol, {}).get(timeframe)
                        == target_status
                    ]
                    if raw_values != expected_values:
                        result.errors.append(
                            f"NATIVE_UNIVERSE_{field_name.upper()}_MISMATCH:{symbol}"
                        )
            eligible = derived_active
            result.cell_statuses = parsed_cell_statuses
            result.symbols_with_active_cells = derived_active
            result.fully_excluded_symbols = derived_fully_excluded

        eligible_set = set(eligible)
        excluded_set = set(excluded)
        if not requested_set:
            result.errors.append("NATIVE_UNIVERSE_REQUESTED_SYMBOLS_MISSING")
        if eligible_set & excluded_set:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBLE_EXCLUDED_OVERLAP")
        if eligible_set | excluded_set != requested_set:
            result.errors.append("NATIVE_UNIVERSE_PARTITION_INCOMPLETE")
        entry_symbols = {
            (
                str(value or "").strip()
                if v5_symbol_identity
                else str(value or "").strip().upper()
            )
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

    if not result.cell_statuses:
        # Legacy/manifest-only compatibility projection. New v3 activations
        # always authenticate an explicit exact cell grid above.
        projected: Dict[str, Dict[str, str]] = {}
        for symbol in result.requested_symbols:
            symbol_entries = (
                entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
            )
            projected[symbol] = {
                timeframe: str(
                    (
                        symbol_entries.get(timeframe)
                        if isinstance(symbol_entries.get(timeframe), Mapping)
                        else {}
                    ).get("status")
                    or ""
                )
                .strip()
                .lower()
                for timeframe in result.required_timeframes
            }
        result.cell_statuses = projected
        result.symbols_with_active_cells = tuple(
            sorted(
                symbol
                for symbol, grid in projected.items()
                if any(status == "active" for status in grid.values())
            )
        )
        result.fully_excluded_symbols = tuple(
            sorted(
                symbol
                for symbol, grid in projected.items()
                if grid
                and all(status == "excluded_no_trade" for status in grid.values())
            )
        )

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
        symbol_entries = (
            entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        )
        raw_timeframe_keys = [str(value or "").strip() for value in symbol_entries]
        canonical_timeframe_keys = [value.upper() for value in raw_timeframe_keys]
        if raw_timeframe_keys != canonical_timeframe_keys or len(
            canonical_timeframe_keys
        ) != len(set(canonical_timeframe_keys)):
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
            entry = (
                symbol_entries.get(timeframe)
                if isinstance(symbol_entries, Mapping)
                else None
            )
            cell_id = f"{symbol}|{timeframe}"
            expected_status = result.cell_status(symbol, timeframe)
            if result.explicit_contract and (
                not cell_contract or expected_status == "active"
            ):
                run_cell = run_cells.get((symbol, timeframe))
                run_classification, _run_coherence_error = (
                    classify_native_availability_cell(
                        run_cell.get("status")
                        if isinstance(run_cell, Mapping)
                        else None,
                        run_cell.get("failure_status")
                        if isinstance(run_cell, Mapping)
                        else None,
                        run_cell.get("rows") if isinstance(run_cell, Mapping) else None,
                        (
                            run_cell.get("resume_disposition")
                            if isinstance(run_cell, Mapping)
                            else None
                        ),
                    )
                )
                if run_classification != "success":
                    result.errors.append(
                        f"NATIVE_ACTIVE_RUN_CELL_NOT_SUCCESS:{cell_id}"
                    )
            if not isinstance(entry, Mapping):
                result.errors.append(f"NATIVE_ACTIVE_CELL_MISSING:{cell_id}")
            elif str(entry.get("status") or "").lower() != expected_status:
                result.errors.append(f"NATIVE_ACTIVE_CELL_STATUS_MISMATCH:{cell_id}")
            elif result.explicit_contract and (
                not cell_contract or expected_status == "active"
            ):
                if (
                    str(entry.get("bars_sha256") or "").lower()
                    != str(
                        run_cell.get("sha256") if isinstance(run_cell, Mapping) else ""
                    ).lower()
                ):
                    result.errors.append(
                        f"NATIVE_ACTIVE_RUN_BARS_BINDING_MISMATCH:{cell_id}"
                    )
                if str(entry.get("audit_status") or "").upper() != "PASS":
                    result.errors.append(f"NATIVE_ACTIVE_CELL_AUDIT_NOT_PASS:{cell_id}")
                if str(entry.get("coverage_method") or "") != contract_method:
                    result.errors.append(
                        f"NATIVE_ACTIVE_CELL_COVERAGE_METHOD_INVALID:{cell_id}"
                    )
                coverage_errors, _normalized = (
                    validate_native_session_coverage_diagnostics(
                        entry.get("expected_session_coverage_by_partition")
                    )
                )
                if coverage_errors:
                    result.errors.append(
                        f"NATIVE_ACTIVE_CELL_COVERAGE_DIAGNOSTIC_INVALID:{cell_id}"
                    )
    for symbol in result.excluded_symbols:
        symbol_entries = (
            entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        )
        raw_timeframe_keys = [str(value or "").strip() for value in symbol_entries]
        canonical_timeframe_keys = [value.upper() for value in raw_timeframe_keys]
        if raw_timeframe_keys != canonical_timeframe_keys or len(
            canonical_timeframe_keys
        ) != len(set(canonical_timeframe_keys)):
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
            entry = (
                symbol_entries.get(timeframe)
                if isinstance(symbol_entries, Mapping)
                else None
            )
            cell_id = f"{symbol}|{timeframe}"
            if not isinstance(entry, Mapping):
                result.errors.append(f"NATIVE_EXCLUDED_AUDIT_CELL_MISSING:{cell_id}")
            elif str(entry.get("status") or "").lower() != "excluded_no_trade":
                result.errors.append(
                    f"NATIVE_EXCLUDED_AUDIT_CELL_STATUS_INVALID:{cell_id}"
                )

    # Production resolution may never disable byte-level evidence replay. The
    # compatibility flag remains available only for non-production forensic
    # inspection of historical manifests.
    verify_cell_evidence = bool(
        verify_exclusion_evidence or production_contract_required
    )
    if result.explicit_contract and verify_cell_evidence:
        for symbol in result.requested_symbols:
            symbol_entries = (
                entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
            )
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
                        expected_cell_status=(
                            result.cell_status(symbol, timeframe)
                            if cell_contract
                            else (
                                "eligible"
                                if symbol in set(result.eligible_symbols)
                                else "legacy_excluded_no_trade"
                            )
                        ),
                        activation_id=str(result.activation_id or ""),
                        run_id=str(manifest.get("run_id") or ""),
                        run_manifest_relative_path=manifest.get(
                            "run_manifest_relative_path"
                        ),
                        run_manifest_sha256=manifest.get("run_manifest_sha256"),
                        run_request=verified_run_request,
                        run_source_environment=(
                            verified_run_manifest.get("source_environment")
                            if isinstance(verified_run_manifest, Mapping)
                            and isinstance(
                                verified_run_manifest.get("source_environment"),
                                Mapping,
                            )
                            else {}
                        ),
                        run_acquisition_started_utc=(
                            verified_run_manifest.get("acquisition_started_utc")
                            if isinstance(verified_run_manifest, Mapping)
                            else None
                        ),
                        run_acquisition_completed_utc=(
                            verified_run_manifest.get("acquisition_completed_utc")
                            if isinstance(verified_run_manifest, Mapping)
                            else None
                        ),
                        contract_minimum=contract_minimum,
                        expected_coverage_method=contract_method,
                        require_resolution_audit=cell_contract,
                        reference_run_cell=run_cells.get(
                            (
                                symbol,
                                str(
                                    (
                                        verified_run_request.get(
                                            "timeframe_audit_reference_map"
                                        )
                                        or {}
                                    ).get(timeframe)
                                    or "M5"
                                ).upper(),
                            )
                        ),
                        parity_thresholds=(
                            manifest.get("parity_thresholds")
                            if isinstance(manifest.get("parity_thresholds"), Mapping)
                            else {}
                        ),
                        symbol_run_cells={
                            candidate_timeframe: candidate_cell
                            for (
                                candidate_symbol,
                                candidate_timeframe,
                            ), candidate_cell in run_cells.items()
                            if candidate_symbol == symbol
                        },
                        symbol_activation_entries=(
                            symbol_entries
                            if isinstance(symbol_entries, Mapping)
                            else {}
                        ),
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
            if cell_contract:
                expected_counts.update(
                    {
                        "symbols_with_active_cells": len(
                            result.symbols_with_active_cells
                        ),
                        "fully_excluded_symbols": len(result.fully_excluded_symbols),
                    }
                )
            for field_name, expected_value in expected_counts.items():
                raw_value = raw_counts.get(field_name)
                if isinstance(raw_value, bool) or raw_value != expected_value:
                    result.errors.append(f"NATIVE_ACTIVE_COUNT_MISMATCH:{field_name}")
            if sum(observed_status_counts.values()) != expected_total:
                result.errors.append("NATIVE_ACTIVE_ENTRY_STATUS_PARTITION_INCOMPLETE")
            if observed_status_counts["pending"] or observed_status_counts["failed"]:
                result.errors.append("NATIVE_ACTIVE_MANIFEST_CONTAINS_NONFINAL_CELLS")
    return result


_UNSCOPED_UNIVERSE_MEMO: Dict[
    Tuple[Any, ...], Tuple["_ActiveAuthorityIdentity", ActiveNativeUniverse]
] = {}
_UNSCOPED_UNIVERSE_MEMO_LOCK = threading.Lock()
_UNSCOPED_UNIVERSE_MEMO_MAX = 8


def clear_unscoped_native_universe_memo() -> None:
    """Drop every memoized unscoped resolution (tests and long-lived processes)."""
    with _UNSCOPED_UNIVERSE_MEMO_LOCK:
        _UNSCOPED_UNIVERSE_MEMO.clear()


def _resolve_unscoped_memoized(
    data_dir: Path,
    configured_symbols: Optional[Iterable[str]],
    *,
    required_timeframes: Sequence[str],
    verify_exclusion_evidence: bool,
    production_contract_required: bool,
) -> ActiveNativeUniverse:
    """Resolve without a lexical scope, reusing results pinned to authority identity.

    Resolution re-reads, re-hashes and cross-resolution-audits every leaf in the
    active universe. Measured 2026-08-08: 12 GB across 567 ``bars.csv`` files,
    over eight minutes per call even with an empty symbol list. Callers outside a
    ``ProductionNativeAuthorityVerificationScope`` previously paid that on every
    invocation, which is why six live-readiness tests consumed 99.96% of a 3h24m
    suite.

    The result is a pure function of the authority identity - the ACTIVE.json
    pointer bytes and the sha256 of the manifest they name - together with the
    resolution parameters. Both are cheap to read, so this memo re-checks identity
    on every hit and recomputes the moment ACTIVE.json or the manifest changes.
    That is the ``activation-vs-resolver`` boundary applied as written: activation
    performs the one-time semantic evaluation, the resolver does hash checks only.

    Only valid universes whose manifest sha matches the identity are stored, so an
    invalid or mid-promotion state is never cached and never reused.
    """

    # Materialize the caller's symbols exactly once. configured_symbols is typed
    # Iterable, so a generator is legal - and building the memo key from it used
    # to exhaust it, leaving the resolver an empty iterator and silently
    # resolving an EMPTY symbol universe. Measured and reported by Codex in
    # CX-013. Everything below uses this tuple, never the original argument.
    symbols = None if configured_symbols is None else tuple(configured_symbols)
    frozen_timeframes = tuple(required_timeframes)

    try:
        identity_before = _read_active_authority_identity(data_dir)
    except BaseException:
        # No readable authority identity means nothing can be pinned; fall back to
        # the uncached path so the caller still sees the real failure mode.
        return _resolve_active_native_universe_uncached(
            data_dir,
            symbols,
            required_timeframes=frozen_timeframes,
            verify_exclusion_evidence=verify_exclusion_evidence,
            production_contract_required=production_contract_required,
        )

    key = (
        str(Path(data_dir).resolve()),
        symbols,
        frozen_timeframes,
        bool(verify_exclusion_evidence),
        bool(production_contract_required),
    )
    with _UNSCOPED_UNIVERSE_MEMO_LOCK:
        entry = _UNSCOPED_UNIVERSE_MEMO.get(key)
    if entry is not None and entry[0] == identity_before:
        returned = deepcopy(entry[1])
        # Post-copy identity re-read, mirroring the scoped path. Copying is not
        # instantaneous, and a warm hit that skipped this could hand back a
        # universe pinned to an authority that moved during the copy.
        try:
            identity_after_copy = _read_active_authority_identity(data_dir)
        except BaseException:
            identity_after_copy = None
        if identity_after_copy == identity_before:
            return returned
        with _UNSCOPED_UNIVERSE_MEMO_LOCK:
            _UNSCOPED_UNIVERSE_MEMO.pop(key, None)
        # fall through and re-verify against the authority as it now stands

    universe = _resolve_active_native_universe_uncached(
        data_dir,
        symbols,
        required_timeframes=frozen_timeframes,
        verify_exclusion_evidence=verify_exclusion_evidence,
        production_contract_required=production_contract_required,
    )

    try:
        identity_after = _read_active_authority_identity(data_dir)
    except BaseException:
        return universe
    if identity_after != identity_before:
        # The authority moved under us; return what was computed but never pin it.
        return universe
    if (
        not universe.valid
        or str(universe.activation_manifest_sha256 or "").lower()
        != identity_after.activation_manifest_sha256
    ):
        return universe

    try:
        sealed = deepcopy(universe)
        returned = deepcopy(sealed)
    except BaseException:
        return universe
    with _UNSCOPED_UNIVERSE_MEMO_LOCK:
        if len(_UNSCOPED_UNIVERSE_MEMO) >= _UNSCOPED_UNIVERSE_MEMO_MAX:
            _UNSCOPED_UNIVERSE_MEMO.pop(next(iter(_UNSCOPED_UNIVERSE_MEMO)), None)
        _UNSCOPED_UNIVERSE_MEMO[key] = (identity_after, sealed)
    return returned


def resolve_active_native_universe(
    data_dir: Path,
    configured_symbols: Optional[Iterable[str]] = None,
    *,
    required_timeframes: Sequence[str] = NATIVE_PRODUCTION_TIMEFRAMES,
    verify_exclusion_evidence: bool = True,
    production_contract_required: bool = False,
) -> ActiveNativeUniverse:
    """Resolve an active universe, reusing only one exact scoped production grid."""

    scope = _PRODUCTION_NATIVE_AUTHORITY_SCOPE.get()
    if scope is None:
        return _resolve_unscoped_memoized(
            data_dir,
            configured_symbols,
            required_timeframes=required_timeframes,
            verify_exclusion_evidence=verify_exclusion_evidence,
            production_contract_required=production_contract_required,
        )

    scoped_timeframes = tuple(required_timeframes)
    scoped_symbols = None if configured_symbols is None else tuple(configured_symbols)
    exact_production_grid = (
        production_contract_required is True
        and verify_exclusion_evidence is True
        and scoped_timeframes == NATIVE_PRODUCTION_TIMEFRAMES
        and (scoped_symbols is None or scoped_symbols == NATIVE_PRODUCTION_SYMBOLS)
    )
    if not exact_production_grid:
        return _resolve_active_native_universe_uncached(
            data_dir,
            scoped_symbols,
            required_timeframes=scoped_timeframes,
            verify_exclusion_evidence=verify_exclusion_evidence,
            production_contract_required=production_contract_required,
        )
    return scope._resolve(
        data_dir,
        scoped_symbols,
        required_timeframes=scoped_timeframes,
        verify_exclusion_evidence=verify_exclusion_evidence,
        production_contract_required=production_contract_required,
    )


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
    if dict(artifact.get("native_exclusion_binding") or {}) != dict(
        expected_binding or {}
    ):
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
        regimes = (
            grid.get(timeframe) if isinstance(grid.get(timeframe), Mapping) else {}
        )
        exact_regimes = tuple(
            str(value or "").strip().upper() for value in required_regimes
        )
        if set(regimes.keys()) != set(exact_regimes):
            errors.append(f"NATIVE_EXCLUDED_REGIME_GRID_NOT_EXACT:{symbol}|{timeframe}")
        elif any(str(key) != str(key).upper() for key in regimes):
            errors.append(
                f"NATIVE_EXCLUDED_REGIME_GRID_NOT_CANONICAL:{symbol}|{timeframe}"
            )
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


def validate_native_cell_exclusion_no_trade(
    timeframe_payload: Any,
    expected_binding: Mapping[str, Any],
    *,
    required_regimes: Sequence[str] = ("TREND", "RANGE", "BREAKOUT", "CHOP"),
) -> List[str]:
    """Verify one excluded timeframe is an exact four-regime NO_TRADE grid."""
    symbol = str((expected_binding or {}).get("symbol") or "").strip().upper()
    timeframe = str((expected_binding or {}).get("timeframe") or "").strip().upper()
    prefix = f"{symbol or 'UNKNOWN'}|{timeframe or 'UNKNOWN'}"
    if not isinstance(timeframe_payload, Mapping):
        return [f"NATIVE_EXCLUDED_CELL_TIMEFRAME_MISSING:{prefix}"]
    errors: List[str] = []
    regimes = tuple(str(value or "").strip().upper() for value in required_regimes)
    if set(timeframe_payload) != set(regimes) or any(
        str(key) != str(key).upper() for key in timeframe_payload
    ):
        errors.append(f"NATIVE_EXCLUDED_CELL_REGIME_GRID_NOT_EXACT:{prefix}")
        return errors
    for regime in regimes:
        cell_id = f"{prefix}|{regime}"
        cell = timeframe_payload.get(regime)
        if not isinstance(cell, Mapping):
            errors.append(f"NATIVE_EXCLUDED_CELL_MISSING:{cell_id}")
            continue
        if str(cell.get("strategy_name") or "").upper() != "NO_TRADE":
            errors.append(f"NATIVE_EXCLUDED_CELL_TRADEABLE:{cell_id}")
        if str(cell.get("promotion_status") or "").upper() != "NO_TRADE":
            errors.append(f"NATIVE_EXCLUDED_CELL_STATUS_INVALID:{cell_id}")
        artifact = (
            cell.get("artifact_meta")
            if isinstance(cell.get("artifact_meta"), Mapping)
            else {}
        )
        if dict(artifact.get("native_cell_exclusion_binding") or {}) != dict(
            expected_binding or {}
        ):
            errors.append(f"NATIVE_EXCLUDED_CELL_BINDING_MISMATCH:{cell_id}")
        validation = (
            cell.get("validation_evidence")
            if isinstance(cell.get("validation_evidence"), Mapping)
            else {}
        )
        if dict(validation.get("native_cell_exclusion_binding") or {}) != dict(
            expected_binding or {}
        ):
            errors.append(f"NATIVE_EXCLUDED_CELL_VALIDATION_BINDING_MISMATCH:{cell_id}")
    return errors


def resolve_active_native_artifact(
    data_dir: Path,
    symbol: str,
    timeframe: str,
    *,
    production_contract_required: bool = False,
) -> ActiveNativeResolution:
    """Resolve and verify one active native artifact without any fallback."""

    raw_symbol = str(symbol or "").strip()
    canonical_symbol = raw_symbol
    tf = str(timeframe or "").strip().upper()
    result = ActiveNativeResolution(symbol=canonical_symbol, timeframe=tf)
    evidence_verified = False
    native_root = (Path(data_dir) / "native").resolve()
    manifest, manifest_path, manifest_sha, manifest_errors = _load_active_manifest(
        data_dir
    )
    result.activation_manifest_path = manifest_path
    result.activation_manifest_sha256 = manifest_sha
    result.errors.extend(manifest_errors)
    if manifest is None:
        return result
    if (
        production_contract_required
        and manifest.get("production_authoritative") is not True
    ):
        result.errors.append("NATIVE_ACTIVE_PRODUCTION_AUTHORITY_REQUIRED")
    result.activation_id = str(manifest.get("activation_id") or "") or None

    universe = manifest.get("universe_eligibility")
    if isinstance(universe, Mapping):
        exact_symbol_identity = str(universe.get("schema_version") or "") == (
            UNIVERSE_AUTHENTICITY_SCHEMA
        )
        canonical_symbol = raw_symbol if exact_symbol_identity else raw_symbol.upper()
        result.symbol = canonical_symbol
        if (
            production_contract_required
            and exact_symbol_identity
            and (canonical_symbol not in NATIVE_PRODUCTION_SYMBOLS)
        ):
            result.errors.append("NATIVE_ACTIVE_SYMBOL_IDENTITY_INVALID")
            return result
        declared_required = tuple(
            str(value or "").strip().upper()
            for value in (universe.get("required_timeframes") or [])
            if str(value or "").strip()
        )
        semantic_universe = resolve_active_native_universe(
            data_dir,
            required_timeframes=declared_required or (tf,),
            verify_exclusion_evidence=False,
            # This cell resolver performs the expensive raw-bar replay for the
            # requested cell below. Asking the universe resolver to replay the
            # complete symbol grid first would multiply twelve-edge validation
            # by every artifact load without adding evidence.
            production_contract_required=False,
        )
        if semantic_universe.errors:
            result.errors.extend(semantic_universe.errors)
            return result
        if production_contract_required:
            if semantic_universe.eligibility_schema != UNIVERSE_AUTHENTICITY_SCHEMA:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CURRENT_ELIGIBILITY_SCHEMA_REQUIRED"
                )
            if str(universe.get("session_coverage_method") or "") != (
                NATIVE_SESSION_COVERAGE_METHOD
            ):
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CROSS_RESOLUTION_REQUIRED"
                )
            if (
                str(
                    semantic_universe.calendar_contract.get(
                        "timeframe_request_map_schema_version"
                    )
                    or ""
                )
                != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
            ):
                result.errors.append("NATIVE_UNIVERSE_PRODUCTION_TIMEFRAME_V5_REQUIRED")
            if semantic_universe.decision_basis != NATIVE_AUTHENTICITY_DECISION_BASIS:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_SOURCE_EVIDENCE_BASIS_REQUIRED"
                )
            if declared_required != NATIVE_PRODUCTION_TIMEFRAMES:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_SIX_TIMEFRAME_ROSTER_REQUIRED"
                )
            if result.errors:
                return result
        cell_data_state = semantic_universe.cell_status(canonical_symbol, tf)
        if cell_data_state == NATIVE_TECHNICAL_ERROR:
            result.errors.append("NATIVE_ACTIVE_CELL_TERMINAL_TECHNICAL")
            return result
        if canonical_symbol in semantic_universe.excluded_symbols:
            result.errors.append("NATIVE_ACTIVE_SYMBOL_EXCLUDED_NO_TRADE")
            return result
        if canonical_symbol not in set(semantic_universe.eligible_symbols):
            result.errors.append("NATIVE_ACTIVE_SYMBOL_NOT_ELIGIBLE")
            return result
        if semantic_universe.cell_status(canonical_symbol, tf) == "excluded_no_trade":
            result.errors.append("NATIVE_ACTIVE_CELL_EXCLUDED_NO_TRADE")
            return result
        if cell_data_state == NATIVE_SOURCE_UNAVAILABLE:
            result.errors.append("NATIVE_ACTIVE_CELL_SOURCE_UNAVAILABLE")
            return result
        expected_data_state = (
            NATIVE_EVIDENCE_CANDIDATE
            if semantic_universe.eligibility_schema == UNIVERSE_AUTHENTICITY_SCHEMA
            else "active"
        )
        if cell_data_state != expected_data_state:
            result.errors.append("NATIVE_ACTIVE_CELL_NOT_ELIGIBLE")
            return result
    elif production_contract_required:
        result.errors.append("NATIVE_UNIVERSE_PRODUCTION_EXPLICIT_CONTRACT_REQUIRED")
        return result
    else:
        canonical_symbol = raw_symbol.upper()
        result.symbol = canonical_symbol

    symbols = manifest.get("entries")
    symbol_entries = (
        symbols.get(canonical_symbol) if isinstance(symbols, Mapping) else None
    )
    entry = symbol_entries.get(tf) if isinstance(symbol_entries, Mapping) else None
    if not isinstance(entry, Mapping):
        result.errors.append("NATIVE_ACTIVE_CELL_MISSING")
        return result
    result.entry = dict(entry)
    expected_entry_state = (
        NATIVE_EVIDENCE_CANDIDATE
        if isinstance(universe, Mapping)
        and str(universe.get("schema_version") or "") == UNIVERSE_AUTHENTICITY_SCHEMA
        else "active"
    )
    if str(entry.get("status", "")).lower() != expected_entry_state:
        result.errors.append("NATIVE_ACTIVE_CELL_NOT_ACTIVE")
    if str(entry.get("symbol", "")) != canonical_symbol:
        result.errors.append("NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH")
    if str(entry.get("timeframe", "")).upper() != tf:
        result.errors.append("NATIVE_ACTIVE_CELL_TIMEFRAME_MISMATCH")

    if isinstance(universe, Mapping):
        expected_run_manifest_relative = (
            f"runs/{str(manifest.get('run_id') or '')}/run_manifest.json"
        )
        if (
            str(manifest.get("run_manifest_relative_path") or "").replace("\\", "/")
            != expected_run_manifest_relative
        ):
            result.errors.append("NATIVE_ACTIVE_RUN_MANIFEST_PATH_IDENTITY_MISMATCH")
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
        run_cells_for_symbol: Dict[str, Mapping[str, Any]] = {}
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
                if str(candidate.get("symbol") or "") != canonical_symbol:
                    continue
                candidate_timeframe = str(candidate.get("timeframe") or "").upper()
                if candidate_timeframe in run_cells_for_symbol:
                    result.errors.append(
                        f"NATIVE_ACTIVE_RUN_DUPLICATE_CELL:{candidate_timeframe}"
                    )
                run_cells_for_symbol[candidate_timeframe] = candidate
                if candidate_timeframe == tf:
                    if run_cell is not None:
                        result.errors.append("NATIVE_ACTIVE_RUN_DUPLICATE_CELL")
                    run_cell = candidate
        reference_map = run_request.get("timeframe_audit_reference_map")
        reference_timeframe = (
            str(reference_map.get(tf) or "M5").upper()
            if isinstance(reference_map, Mapping)
            else "M5"
        )
        reference_run_cell = run_cells_for_symbol.get(reference_timeframe)
        authenticity_contract = str(universe.get("schema_version") or "") == (
            UNIVERSE_AUTHENTICITY_SCHEMA
        )
        if authenticity_contract:
            contract_minimum = NATIVE_MINIMUM_SESSION_COVERAGE
        else:
            try:
                contract_minimum = float(universe.get("minimum_session_coverage"))
            except (TypeError, ValueError, OverflowError):
                contract_minimum = float("nan")
        if not authenticity_contract and not math.isfinite(contract_minimum):
            result.errors.append("NATIVE_ACTIVE_SESSION_COVERAGE_POLICY_INVALID")
        else:
            frame_cache = (
                _V5NormalizedFrameCache(canonical_symbol)
                if str(run_request.get("timeframe_request_map_schema_version") or "")
                == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
                else None
            )
            try:
                result.errors.extend(
                    _verify_activation_cell_evidence(
                        native_root,
                        symbol=canonical_symbol,
                        timeframe=tf,
                        entry=entry,
                        run_cell=run_cell,
                        audited_cell=None,
                        expected_cell_status=expected_entry_state,
                        activation_id=str(result.activation_id or ""),
                        run_id=str(manifest.get("run_id") or ""),
                        run_manifest_relative_path=manifest.get(
                            "run_manifest_relative_path"
                        ),
                        run_manifest_sha256=manifest.get("run_manifest_sha256"),
                        run_request=run_request,
                        run_source_environment=(
                            run_manifest.get("source_environment")
                            if isinstance(run_manifest, Mapping)
                            and isinstance(
                                run_manifest.get("source_environment"), Mapping
                            )
                            else {}
                        ),
                        run_acquisition_started_utc=(
                            run_manifest.get("acquisition_started_utc")
                            if isinstance(run_manifest, Mapping)
                            else None
                        ),
                        run_acquisition_completed_utc=(
                            run_manifest.get("acquisition_completed_utc")
                            if isinstance(run_manifest, Mapping)
                            else None
                        ),
                        contract_minimum=contract_minimum,
                        expected_coverage_method=str(
                            universe.get("session_coverage_method") or ""
                        ),
                        require_resolution_audit=(
                            str(universe.get("schema_version") or "")
                            in {
                                UNIVERSE_ELIGIBILITY_SCHEMA,
                                UNIVERSE_AUTHENTICITY_SCHEMA,
                            }
                        ),
                        reference_run_cell=reference_run_cell,
                        parity_thresholds=(
                            manifest.get("parity_thresholds")
                            if isinstance(manifest.get("parity_thresholds"), Mapping)
                            else {}
                        ),
                        symbol_run_cells=run_cells_for_symbol,
                        symbol_activation_entries=(
                            symbol_entries
                            if isinstance(symbol_entries, Mapping)
                            else {}
                        ),
                        v5_frame_cache=frame_cache,
                    )
                )
            finally:
                if frame_cache is not None:
                    frame_cache.clear()
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
        if (
            sha256_file(data_path).lower()
            != str(entry.get("bars_sha256") or "").lower()
        ):
            result.errors.append("NATIVE_ACTIVE_BARS_SHA256_MISMATCH")
        if (
            sha256_file(sidecar_path).lower()
            != str(entry.get("sidecar_sha256") or "").lower()
        ):
            result.errors.append("NATIVE_ACTIVE_SIDECAR_SHA256_MISMATCH")
    return result
