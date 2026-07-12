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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION
from pm_native_consensus import (
    CONSENSUS_POLICY,
    EVIDENCE_PASS,
    EVIDENCE_PENDING,
    EVIDENCE_SEMANTIC_FAIL,
    EVIDENCE_TERMINAL_UNAVAILABLE,
    INTEGRITY_ERROR,
    INTEGRITY_PASS,
    REQUIRED_AUDIT_WINDOWS,
    build_consensus_envelope,
    build_edge_report,
    expected_reference_timeframes,
    validate_consensus_envelope,
)
from pm_native_parity import (
    ParityThresholds,
    audit_native_cross_resolution,
    normalize_ohlc_frame,
)
from pm_native_resolution import RESOLUTION_AUDIT_SCHEMA_VERSION, audit_native_resolution


ACTIVE_POINTER_SCHEMA = "fxpm-native-active-pointer-v1"
ACTIVATION_MANIFEST_SCHEMA = "fxpm-native-activation-v1"
NATIVE_RUN_MANIFEST_SCHEMA = "fxpm.native_history_run_manifest.v1"
NATIVE_BARS_SCHEMA = "fxpm-native-bars-v1"
NATIVE_PARITY_SCHEMA = "fxpm-native-m5-parity-v1"
NATIVE_CROSS_RESOLUTION_PARITY_SCHEMA = (
    "fxpm-native-cross-resolution-parity-v2"
)
NATIVE_ACQUISITION_CONTRACT = "2026-07-12-native-calendar-acquisition-v3"
AUDITED_SIDECAR_CONTRACT = "2026-07-10-native-audited-sidecar-v1"
UNIVERSE_ELIGIBILITY_SCHEMA = "fxpm-native-universe-eligibility-v3"
LEGACY_UNIVERSE_ELIGIBILITY_SCHEMA = "fxpm-native-universe-eligibility-v2"
NATIVE_SOURCE_EVIDENCE_DECISION_BASIS = (
    "source_availability_integrity_and_coverage_only"
)
LEGACY_SOURCE_AVAILABILITY_DECISION_BASIS = "source_availability_only"
NATIVE_MINIMUM_SESSION_COVERAGE = 0.95
LEGACY_NATIVE_SESSION_COVERAGE_METHOD = "train_only_robust_weekly_session_rate_v1"
SEGMENTED_NATIVE_SESSION_COVERAGE_METHOD = (
    "timestamp_segmented_weekly_session_rate_v2"
)
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

    ``success`` means that a hash-valid bar artifact exists and may proceed to
    the cell-specific evidence audit.  It does *not* mean the originally
    requested leading warmup span is sufficient. New acquisitions distinguish
    an auditable ``boundary_history_shortfall`` from legacy undifferentiated
    ``partial_history`` and from technical internal-page defects.
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
                if request_map_version
                == FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
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
                errors.append(
                    f"NATIVE_UNIVERSE_{field_name.upper()}_ROSTER_INVALID"
                )
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
            interval = intervals.get(timeframe) if isinstance(intervals, Mapping) else None
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
                            or set(prefix)
                            != {"start_utc", "end_exclusive_utc", "role"}
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
                elif interval_start is not None and strategy_start is not None and (
                    interval_start > strategy_start
                ):
                    errors.append(
                        "NATIVE_UNIVERSE_TIMEFRAME_SOURCE_UNION_INVALID:"
                        f"{timeframe}"
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
            if request.get("timeframe_consensus_policy") != (
                CONSENSUS_POLICY
            ):
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
                errors.append(
                    "NATIVE_UNIVERSE_TIMEFRAME_COMMON_PROJECTION_MISMATCH"
                )
            if isinstance(years_map, Mapping) and request.get("window_years") != (
                years_map.get(compatibility_timeframe)
            ):
                errors.append(
                    "NATIVE_UNIVERSE_TIMEFRAME_WINDOW_PROJECTION_MISMATCH"
                )

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
                errors.append(
                    "NATIVE_UNIVERSE_TIMEFRAME_ACTIVE_WINDOWS_NOT_CONCURRENT"
                )
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
                detail.get("reason_codes") if isinstance(detail, Mapping) else []
                or []
            ),
        }
    return states


def _v5_terminal_states(reasons: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    return {
        window: {
            "integrity_state": INTEGRITY_PASS,
            "evidence_state": EVIDENCE_TERMINAL_UNAVAILABLE,
            "reason_codes": list(reasons),
        }
        for window in REQUIRED_AUDIT_WINDOWS
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


def _v5_expected_coverage(
    envelope: Mapping[str, Any],
    pair_reports: Mapping[str, Optional[Mapping[str, Any]]],
) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {}
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        status = str(
            ((envelope.get("windows") or {}).get(partition) or {}).get("status")
            or ""
        ).upper()
        candidates: List[float] = []
        for report in pair_reports.values():
            detail = (
                (report.get("partitions") or {}).get(partition)
                if isinstance(report, Mapping)
                else None
            )
            if not isinstance(detail, Mapping) or str(
                detail.get("status") or ""
            ).upper() != "PASS":
                continue
            raw = detail.get("bidirectional_coverage") if isinstance(detail, Mapping) else None
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
        disagreement = len(
            {
                (value["integrity_state"], value["evidence_state"])
                for value in outcomes.values()
            }
        ) > 1
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
    contract_minimum: float,
    parity_thresholds: Mapping[str, Any],
    symbol_run_cells: Mapping[str, Mapping[str, Any]],
) -> List[str]:
    """Recompute V5 pair evidence from pinned CSVs; never trust stored PASS."""

    cell_id = f"{symbol}|{timeframe}"
    errors: List[str] = []
    if not isinstance(entry, Mapping) or not isinstance(run_cell, Mapping):
        return [f"NATIVE_V5_CELL_EVIDENCE_MISSING:{cell_id}"]
    allowed_fields = {
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
    for field_name, expected in (
        ("symbol", symbol),
        ("timeframe", timeframe),
        ("run_id", run_id),
    ):
        if str(entry.get(field_name) or "") != expected:
            errors.append(f"NATIVE_V5_ENTRY_{field_name.upper()}_MISMATCH:{cell_id}")
    if str(entry.get("run_manifest_sha256") or "").lower() != str(
        run_manifest_sha256 or ""
    ).lower():
        errors.append(f"NATIVE_V5_ENTRY_RUN_MANIFEST_MISMATCH:{cell_id}")
    classification, coherence_error = classify_native_availability_cell(
        run_cell.get("status"),
        run_cell.get("failure_status"),
        run_cell.get("rows"),
    )
    if classification == "invalid":
        errors.append(
            f"NATIVE_V5_RUN_AVAILABILITY_INVALID:{cell_id}:{coherence_error or 'unknown'}"
        )
    if str(entry.get("bars_sha256") or "").lower() != str(
        run_cell.get("sha256") or ""
    ).lower():
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
    if str(provenance.get("sha256") or "").lower() != str(
        entry.get("bars_sha256") or ""
    ).lower():
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
        expected = source_map.get(timeframe) if isinstance(source_map, Mapping) else None
        if not isinstance(provenance_request, Mapping) or provenance_request.get(
            leaf_field
        ) != expected:
            errors.append(f"NATIVE_V5_PROVENANCE_LEAF_ROLE_MISMATCH:{cell_id}:{leaf_field}")

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
        if str(payload.get("run_manifest_sha256") or "").lower() != str(
            run_manifest_sha256 or ""
        ).lower():
            errors.append(f"NATIVE_V5_{label}_RUN_MANIFEST_SHA_MISMATCH:{cell_id}")

    normalized_expected = str(expected_cell_status or "").lower()
    if normalized_expected == "eligible":
        normalized_expected = "active"
    if classification == "unavailable":
        if normalized_expected != "excluded_no_trade":
            errors.append(f"NATIVE_V5_UNAVAILABLE_STATUS_MISMATCH:{cell_id}")
        if consensus.get("source_unavailable") is not True:
            errors.append(f"NATIVE_V5_UNAVAILABLE_CONSENSUS_INVALID:{cell_id}")
        return list(dict.fromkeys(errors))
    if classification != "success":
        return list(dict.fromkeys(errors))

    bars_path, file_errors = _verify_pinned_file(
        native_root,
        entry.get("bars_relative_path"),
        entry.get("bars_sha256"),
        error_prefix=f"NATIVE_V5_BARS:{cell_id}",
    )
    errors.extend(file_errors)
    target_frame = _v5_read_frame(bars_path)
    tolerance = _v5_tolerance(provenance)
    if target_frame is None or tolerance is None:
        errors.append(f"NATIVE_V5_TARGET_FRAME_OR_TOLERANCE_INVALID:{cell_id}")
        return list(dict.fromkeys(errors))

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
    for reference in references:
        reference_cell = symbol_run_cells.get(reference)
        reference_class, reference_error = classify_native_availability_cell(
            reference_cell.get("status") if isinstance(reference_cell, Mapping) else None,
            reference_cell.get("failure_status") if isinstance(reference_cell, Mapping) else None,
            reference_cell.get("rows") if isinstance(reference_cell, Mapping) else None,
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
        elif reference_class == "success":
            reference_bars = (
                native_root / "runs" / run_id / symbol / reference / "bars.csv"
            )
            reference_provenance = (
                native_root / "runs" / run_id / symbol / reference / "provenance.json"
            )
            if not reference_bars.is_file() or sha256_file(reference_bars).lower() != str(
                reference_cell.get("sha256") or ""
            ).lower():
                errors.append(f"NATIVE_V5_REFERENCE_BARS_INVALID:{cell_id}:{reference}")
                continue
            if not reference_provenance.is_file():
                errors.append(f"NATIVE_V5_REFERENCE_PROVENANCE_MISSING:{cell_id}:{reference}")
                continue
            reference_provenance_sha = sha256_file(reference_provenance)
            try:
                reference_provenance_payload = json.loads(
                    reference_provenance.read_text(encoding="utf-8")
                )
            except Exception:
                errors.append(f"NATIVE_V5_REFERENCE_PROVENANCE_INVALID:{cell_id}:{reference}")
                continue
            reference_tolerance = (
                _v5_tolerance(reference_provenance_payload)
                if isinstance(reference_provenance_payload, Mapping)
                else None
            )
            reference_frame = _v5_read_frame(reference_bars)
            if (
                reference_frame is None
                or reference_tolerance is None
            ):
                errors.append(
                    f"NATIVE_V5_REFERENCE_FRAME_OR_TOLERANCE_INVALID:{cell_id}:{reference}"
                )
                continue
            binding = {
                "availability_state": "AVAILABLE",
                "bars_sha256": reference_cell.get("sha256"),
                "provenance_sha256": reference_provenance_sha,
            }
            if NATIVE_TIMEFRAME_SECONDS[reference] < NATIVE_TIMEFRAME_SECONDS[timeframe]:
                fine_tf, fine_frame = reference, reference_frame
                coarse_tf, coarse_frame = timeframe, target_frame
            else:
                fine_tf, fine_frame = timeframe, target_frame
                coarse_tf, coarse_frame = reference, reference_frame
            raw_report = audit_native_cross_resolution(
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
            states = _v5_pair_states(raw_report)
            del reference_frame
        else:
            errors.append(
                f"NATIVE_V5_REFERENCE_RUN_CELL_INVALID:{cell_id}:{reference}:{reference_error or 'unknown'}"
            )
            continue
        expected_reference_bindings[reference] = binding
        expected_edges[reference] = build_edge_report(
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
    stored_edges = consensus.get("edges")
    for reference in references:
        stored_report = (
            (stored_edges.get(reference) or {}).get("report")
            if isinstance(stored_edges, Mapping)
            else None
        )
        if stored_report != expected_edges[reference]:
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
            errors.append(f"NATIVE_V5_CONSENSUS_DERIVATION_MISMATCH:{cell_id}:{field_name}")

    expected_disagreement = _v5_expected_disagreement(expected_envelope)
    consensus_diagnostics = consensus.get("diagnostics")
    if not isinstance(consensus_diagnostics, Mapping) or consensus_diagnostics.get(
        "raw_pair_reports_recomputed_from_pinned_bars"
    ) is not True:
        errors.append(f"NATIVE_V5_RAW_PAIR_RECOMPUTATION_MARKER_INVALID:{cell_id}")
    for field_name in (
        "window_disagreement_count",
        "windows_with_disagreement",
        "per_window",
    ):
        if not isinstance(consensus_diagnostics, Mapping) or consensus_diagnostics.get(
            field_name
        ) != expected_disagreement[field_name]:
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
        if payload.get("consensus_disagreement_count") != expected_disagreement[
            "window_disagreement_count"
        ]:
            errors.append(f"NATIVE_V5_{label}_DISAGREEMENT_COUNT_MISMATCH:{cell_id}")
        if payload.get("consensus_window_diagnostics") != expected_disagreement[
            "per_window"
        ]:
            errors.append(f"NATIVE_V5_{label}_WINDOW_DIAGNOSTICS_MISMATCH:{cell_id}")

    expected_coverage = _v5_expected_coverage(expected_envelope, pair_reports)
    if consensus.get("expected_session_coverage_by_partition") != expected_coverage:
        errors.append(f"NATIVE_V5_CONSENSUS_COVERAGE_MISMATCH:{cell_id}")
    for payload, label in (
        (entry, "ENTRY"),
        (sidecar, "SIDECAR"),
        (audited_cell, "AUDIT"),
    ):
        if isinstance(payload, Mapping) and payload.get(
            "expected_session_coverage_by_partition"
        ) != expected_coverage:
            errors.append(f"NATIVE_V5_{label}_COVERAGE_MISMATCH:{cell_id}")

    # Resolution is independently recomputed before any consensus decision is
    # allowed to activate the cell. D1 is loaded only for this calculation and
    # released before returning; pair references were processed sequentially.
    d1_cell = symbol_run_cells.get("D1")
    d1_path = native_root / "runs" / run_id / symbol / "D1" / "bars.csv"
    d1_frame: Optional[pd.DataFrame] = None
    if isinstance(d1_cell, Mapping) and str(d1_cell.get("status") or "").lower() == "success":
        if d1_path.is_file() and sha256_file(d1_path).lower() == str(
            d1_cell.get("sha256") or ""
        ).lower():
            d1_frame = target_frame if timeframe == "D1" else _v5_read_frame(d1_path)
    resolution = audit_native_resolution(
        target_frame,
        symbol=symbol,
        timeframe=timeframe,
        daily_frame=d1_frame,
        tick_tolerance=tolerance,
    ).to_dict()
    serialized_resolution = json.loads(
        json.dumps(resolution, sort_keys=True, ensure_ascii=False)
    )
    if sidecar.get("resolution_audit") != serialized_resolution:
        errors.append(f"NATIVE_V5_RESOLUTION_RECOMPUTATION_MISMATCH:{cell_id}")
    resolution_status = str(resolution.get("status") or "PENDING").upper()
    history_status, _history_reasons, _normalized = classify_native_session_history(
        expected_coverage,
        minimum=contract_minimum,
    )
    consensus_status = str(expected_envelope.get("status") or "ERROR").upper()
    derived = (
        "active"
        if consensus_status == "PASS"
        and history_status == "sufficient"
        and resolution_status == "PASS"
        else "excluded_no_trade"
        if consensus_status == "FAIL"
        or history_status == "insufficient"
        or resolution_status == "FAIL"
        else "invalid"
    )
    if derived != normalized_expected:
        errors.append(
            f"NATIVE_V5_UNIVERSE_STATUS_MISMATCH:{cell_id}:{derived}!={normalized_expected}"
        )
    expected_audit_status = "PASS" if derived == "active" else "FAIL"
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
    elif history_status != "sufficient":
        expected_reasons.extend(_history_reasons)
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
    parity_thresholds: Optional[Mapping[str, Any]] = None,
    symbol_run_cells: Optional[Mapping[str, Mapping[str, Any]]] = None,
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
            contract_minimum=contract_minimum,
            parity_thresholds=parity_thresholds or {},
            symbol_run_cells=symbol_run_cells or {},
        )

    cell_id = f"{symbol}|{timeframe}"
    legacy_symbol_excluded = (
        str(expected_cell_status or "").strip().lower()
        == "legacy_excluded_no_trade"
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
            if str(
                entry.get("cross_resolution_reference_bars_sha256") or ""
            ).lower() != expected_reference_bars:
                errors.append(
                    f"NATIVE_CELL_EVIDENCE_REFERENCE_BARS_MISMATCH:{cell_id}"
                )
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
            if str(
                entry.get("cross_resolution_reference_provenance_sha256") or ""
            ).lower() != reference_provenance_sha:
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
    expected_parity_schema = (
        NATIVE_CROSS_RESOLUTION_PARITY_SCHEMA
        if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD
        else NATIVE_PARITY_SCHEMA
    )
    if str(parity.get("schema_version") or "") != expected_parity_schema:
        errors.append(f"NATIVE_CELL_EVIDENCE_PARITY_SCHEMA_INVALID:{cell_id}")
    if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
        if str(
            parity.get("activation_target_timeframe") or ""
        ).upper() != timeframe:
            errors.append(
                f"NATIVE_CELL_EVIDENCE_PARITY_TARGET_TIMEFRAME_MISMATCH:{cell_id}"
            )
        if str(parity.get("reference_timeframe") or "").upper() != (
            expected_reference
        ):
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
    window_contract_version = str(
        run_request.get("window_contract_version") or ""
    )
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
    if isinstance(audited_cell, Mapping) and str(
        audited_cell.get("coverage_method") or ""
    ) != expected_coverage_method:
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

    resolution_status = "PASS"
    resolution_reasons: List[str] = []
    if require_resolution_audit:
        resolution_audit = sidecar.get("resolution_audit")
        if not isinstance(resolution_audit, Mapping):
            errors.append(f"NATIVE_CELL_EVIDENCE_RESOLUTION_AUDIT_MISSING:{cell_id}")
            resolution_status = "INVALID"
        else:
            resolution_status = str(
                resolution_audit.get("status") or ""
            ).strip().upper()
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
            if str(resolution_audit.get("symbol") or "") != symbol or str(
                resolution_audit.get("timeframe") or ""
            ) != timeframe:
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

    history_status, history_reasons, _normalized = classify_native_session_history(
        expected_coverage,
        minimum=contract_minimum,
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
        and history_status == "sufficient"
        and resolution_status == "PASS"
        and not semantic_cross_resolution_failure
    ):
        derived_status = "active"
    elif classification == "success" and (
        history_status == "insufficient"
        or resolution_status in {"FAIL", "PENDING"}
        or semantic_cross_resolution_failure
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
    if str(entry.get("universe_eligibility_status") or "").lower() != expected_universe_status:
        errors.append(f"NATIVE_CELL_EVIDENCE_ENTRY_UNIVERSE_STATUS_MISMATCH:{cell_id}")

    if classification == "success":
        if parity_status == "NOT_APPLICABLE":
            reference_classification, _reference_error = classify_native_availability_cell(
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
            )
            reference_failure = str(
                (
                    reference_run_cell.get("failure_status")
                    if isinstance(reference_run_cell, Mapping)
                    else ""
                )
                or ""
            ).strip().upper()
            if expected_coverage_method == NATIVE_SESSION_COVERAGE_METHOD:
                reference_timeframe = str(
                    entry.get("cross_resolution_reference_timeframe") or ""
                ).upper()
                expected_reason = (
                    f"AUDIT_REFERENCE_UNAVAILABLE:{reference_timeframe}:"
                    f"{reference_failure or 'UNSPECIFIED'}"
                )
            else:
                expected_reason = (
                    f"M5_ACQUISITION_UNAVAILABLE:{reference_failure}"
                )
            if (
                reference_classification != "unavailable"
                or list(parity.get("reason_codes") or []) != [expected_reason]
            ):
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
                history_status == "sufficient"
                and resolution_status == "PASS"
                and not semantic_cross_resolution_failure
            )
            else "FAIL"
        )
        if entry_audit_status != expected_audit_status:
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_STATUS_DERIVATION_MISMATCH:{cell_id}")
    elif classification == "unavailable" and not legacy_symbol_excluded:
        expected_reason = f"ACQUISITION_UNAVAILABLE:{failure.upper()}"
        if entry_audit_status != "FAIL":
            errors.append(f"NATIVE_CELL_EVIDENCE_AUDIT_STATUS_DERIVATION_MISMATCH:{cell_id}")
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
    errors: List[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors and bool(self.activation_id)

    def symbol_status(self, symbol: str) -> str:
        canonical = str(symbol or "").strip().upper()
        if canonical in self.fully_excluded_symbols or canonical in self.excluded_symbols:
            return "excluded_no_trade"
        if canonical in self.symbols_with_active_cells or canonical in self.eligible_symbols:
            return "eligible"
        return "unbound"

    def cell_status(self, symbol: str, timeframe: str) -> str:
        canonical = str(symbol or "").strip().upper()
        tf = str(timeframe or "").strip().upper()
        return str((self.cell_statuses.get(canonical) or {}).get(tf) or "unbound")

    def active_timeframes(self, symbol: str) -> Tuple[str, ...]:
        canonical = str(symbol or "").strip().upper()
        statuses = self.cell_statuses.get(canonical) or {}
        return tuple(
            timeframe
            for timeframe in self.required_timeframes
            if statuses.get(timeframe) == "active"
        )

    def excluded_timeframes(self, symbol: str) -> Tuple[str, ...]:
        canonical = str(symbol or "").strip().upper()
        statuses = self.cell_statuses.get(canonical) or {}
        return tuple(
            timeframe
            for timeframe in self.required_timeframes
            if statuses.get(timeframe) == "excluded_no_trade"
        )

    def cell_exclusion_binding(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """Return the immutable binding for one excluded specialist cell."""
        canonical = str(symbol or "").strip().upper()
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
        canonical = str(symbol or "").strip().upper()
        record = dict(self.excluded_symbols.get(canonical) or {})
        if not record:
            return {}
        return {
            "schema_version": self.eligibility_schema or UNIVERSE_ELIGIBILITY_SCHEMA,
            "decision_basis": str(
                record.get("decision_basis")
                or NATIVE_SOURCE_EVIDENCE_DECISION_BASIS
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
    production_contract_required: bool = False,
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
            calendar_contract, calendar_errors = _validated_calendar_contract(
                verified_run_request
            )
            result.calendar_contract = calendar_contract
            result.errors.extend(calendar_errors)
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
    if production_contract_required and not isinstance(contract, Mapping):
        result.errors.append(
            "NATIVE_UNIVERSE_PRODUCTION_EXPLICIT_CONTRACT_REQUIRED"
        )
    if isinstance(contract, Mapping):
        result.explicit_contract = True
        contract_schema = str(contract.get("schema_version") or "")
        result.eligibility_schema = contract_schema
        cell_contract = contract_schema == UNIVERSE_ELIGIBILITY_SCHEMA
        if contract_schema not in {
            UNIVERSE_ELIGIBILITY_SCHEMA,
            LEGACY_UNIVERSE_ELIGIBILITY_SCHEMA,
        }:
            result.errors.append("NATIVE_UNIVERSE_ELIGIBILITY_SCHEMA_INVALID")
        contract_decision_basis = str(contract.get("decision_basis") or "")
        result.decision_basis = contract_decision_basis
        if contract_decision_basis not in {
            NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
            LEGACY_SOURCE_AVAILABILITY_DECISION_BASIS,
        }:
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
        contract_method = str(contract.get("session_coverage_method") or "")
        if (
            contract_method not in SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS
            or (
                not cell_contract
                and contract_method != LEGACY_NATIVE_SESSION_COVERAGE_METHOD
            )
        ):
            result.errors.append("NATIVE_UNIVERSE_SESSION_COVERAGE_METHOD_INVALID")
        if production_contract_required:
            if contract_schema != UNIVERSE_ELIGIBILITY_SCHEMA:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CURRENT_ELIGIBILITY_SCHEMA_REQUIRED"
                )
            if contract_method != NATIVE_SESSION_COVERAGE_METHOD:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CROSS_RESOLUTION_REQUIRED"
                )
            if str(
                verified_run_request.get("timeframe_request_map_schema_version")
                or ""
            ) != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_TIMEFRAME_V5_REQUIRED"
                )
            if contract_decision_basis != NATIVE_SOURCE_EVIDENCE_DECISION_BASIS:
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
            if str(record.get("decision_basis") or "") != contract_decision_basis:
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
                if str(
                    verified_run_request.get(
                        "timeframe_request_map_schema_version"
                    )
                    or ""
                ) == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
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
                history_status, history_reasons, _normalized = classify_native_session_history(
                    audit_cell.get("expected_session_coverage_by_partition"),
                    minimum=contract_minimum,
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
                    reference_classification, _reference_error = classify_native_availability_cell(
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
                    if not isinstance(entry, Mapping) or str(
                        entry.get("status") or ""
                    ).strip().lower() != status:
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
            declared_fully_excluded = _canonical_declared_list(
                "fully_excluded_symbols"
            )
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
                if not isinstance(raw_projection, Mapping) or set(raw_projection) != requested_set:
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
                ).strip().lower()
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
                        run_cell.get("rows")
                        if isinstance(run_cell, Mapping)
                        else None,
                    )
                )
                if run_classification != "success":
                    result.errors.append(f"NATIVE_ACTIVE_RUN_CELL_NOT_SUCCESS:{cell_id}")
            if not isinstance(entry, Mapping):
                result.errors.append(f"NATIVE_ACTIVE_CELL_MISSING:{cell_id}")
            elif str(entry.get("status") or "").lower() != expected_status:
                result.errors.append(f"NATIVE_ACTIVE_CELL_STATUS_MISMATCH:{cell_id}")
            elif result.explicit_contract and (
                not cell_contract or expected_status == "active"
            ):
                if str(entry.get("bars_sha256") or "").lower() != str(
                    run_cell.get("sha256") if isinstance(run_cell, Mapping) else ""
                ).lower():
                    result.errors.append(f"NATIVE_ACTIVE_RUN_BARS_BINDING_MISMATCH:{cell_id}")
                if str(entry.get("audit_status") or "").upper() != "PASS":
                    result.errors.append(f"NATIVE_ACTIVE_CELL_AUDIT_NOT_PASS:{cell_id}")
                if str(entry.get("coverage_method") or "") != contract_method:
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

    # Production resolution may never disable byte-level evidence replay. The
    # compatibility flag remains available only for non-production forensic
    # inspection of historical manifests.
    verify_cell_evidence = bool(
        verify_exclusion_evidence or production_contract_required
    )
    if result.explicit_contract and verify_cell_evidence:
        for symbol in result.requested_symbols:
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
                            for (candidate_symbol, candidate_timeframe), candidate_cell in run_cells.items()
                            if candidate_symbol == symbol
                        },
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
                        "fully_excluded_symbols": len(
                            result.fully_excluded_symbols
                        ),
                    }
                )
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
            errors.append(
                f"NATIVE_EXCLUDED_CELL_VALIDATION_BINDING_MISMATCH:{cell_id}"
            )
    return errors


def resolve_active_native_artifact(
    data_dir: Path,
    symbol: str,
    timeframe: str,
    *,
    production_contract_required: bool = False,
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
            if semantic_universe.eligibility_schema != UNIVERSE_ELIGIBILITY_SCHEMA:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CURRENT_ELIGIBILITY_SCHEMA_REQUIRED"
                )
            if str(universe.get("session_coverage_method") or "") != (
                NATIVE_SESSION_COVERAGE_METHOD
            ):
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_CROSS_RESOLUTION_REQUIRED"
                )
            if str(
                semantic_universe.calendar_contract.get(
                    "timeframe_request_map_schema_version"
                )
                or ""
            ) != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_TIMEFRAME_V5_REQUIRED"
                )
            if semantic_universe.decision_basis != (
                NATIVE_SOURCE_EVIDENCE_DECISION_BASIS
            ):
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_SOURCE_EVIDENCE_BASIS_REQUIRED"
                )
            if declared_required != NATIVE_PRODUCTION_TIMEFRAMES:
                result.errors.append(
                    "NATIVE_UNIVERSE_PRODUCTION_SIX_TIMEFRAME_ROSTER_REQUIRED"
                )
            if result.errors:
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
        if semantic_universe.cell_status(canonical_symbol, tf) != "active":
            result.errors.append("NATIVE_ACTIVE_CELL_NOT_ELIGIBLE")
            return result
    elif production_contract_required:
        result.errors.append(
            "NATIVE_UNIVERSE_PRODUCTION_EXPLICIT_CONTRACT_REQUIRED"
        )
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
                    expected_cell_status="active",
                    activation_id=str(result.activation_id or ""),
                    run_id=str(manifest.get("run_id") or ""),
                    run_manifest_relative_path=manifest.get(
                        "run_manifest_relative_path"
                    ),
                    run_manifest_sha256=manifest.get("run_manifest_sha256"),
                    run_request=run_request,
                    contract_minimum=contract_minimum,
                    expected_coverage_method=str(
                        universe.get("session_coverage_method") or ""
                    ),
                    require_resolution_audit=(
                        str(universe.get("schema_version") or "")
                        == UNIVERSE_ELIGIBILITY_SCHEMA
                    ),
                    reference_run_cell=reference_run_cell,
                    parity_thresholds=(
                        manifest.get("parity_thresholds")
                        if isinstance(manifest.get("parity_thresholds"), Mapping)
                        else {}
                    ),
                    symbol_run_cells=run_cells_for_symbol,
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
