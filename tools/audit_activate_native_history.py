"""Audit and atomically activate one immutable native-history run.

This command is offline: it never imports or connects to MetaTrader.  It reads
an acquisition run, audits every requested cell, and freezes an availability-
only specialist-cell universe before model search. Each symbol/timeframe is
independently active or hash-bound NO_TRADE. A sparse cell cannot veto a valid
sibling, while fetch, mapping, write, parity, and invalid-coverage errors still
block activation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_native_activation import (  # noqa: E402
    ACTIVATION_MANIFEST_SCHEMA,
    ACTIVE_POINTER_SCHEMA,
    LEGACY_NATIVE_SESSION_COVERAGE_METHOD,
    NATIVE_EVIDENCE_PARTITIONS,
    NATIVE_MINIMUM_SESSION_COVERAGE,
    NATIVE_SESSION_COVERAGE_METHOD,
    SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS,
    UNIVERSE_ELIGIBILITY_SCHEMA,
    classify_native_availability_cell,
    classify_native_session_history,
    sha256_file,
)
from pm_native_parity import (  # noqa: E402
    PARITY_SCHEMA_VERSION,
    ParityThresholds,
    audit_native_vs_session_m5,
    normalize_ohlc_frame,
)
from pm_native_resolution import audit_native_resolution  # noqa: E402
from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION  # noqa: E402
from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    DEFAULT_TIMEFRAMES,
    MANIFEST_SCHEMA_VERSION,
    SCHEMA_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
    ceil_utc_boundary,
    expected_session_coverage_by_partition,
    floor_utc_boundary,
    segmented_session_coverage_by_partition_v2,
)


AUDITED_SIDECAR_CONTRACT = "2026-07-10-native-audited-sidecar-v1"
_SUCCESS_SOURCE_STATUS = "success"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_REQUEST_CONTRACT_FIELDS: Tuple[str, ...] = (
    "horizon_years",
    "requested_information_cutoff_utc",
    "requested_horizon_start_utc",
    "common_requested_start_utc",
    "required_evidence_lead_start_utc",
    "warmup_days",
    "safety_days",
    "closed_bar_slack_seconds",
    "common_as_of_utc",
    "boundary_tolerance_days",
    "window_years",
    "embargo_days",
    "common_calendar_boundaries",
)
_V3_REQUEST_CONTRACT_FIELDS: Tuple[str, ...] = (
    "window_contract_version",
    "timeframe_request_map_schema_version",
    "timeframe_window_years",
    "timeframe_calendar_boundaries",
    "timeframe_calendar_boundaries_sha256",
    "timeframe_required_evidence_lead_start_utc",
    "timeframe_source_request_intervals",
    "timeframe_request_map_sha256",
)
_SOURCE_IDENTITY_FIELDS: Mapping[str, Tuple[str, ...]] = {
    "account": ("login", "server", "company", "currency"),
    "configured_connection": (
        "server",
        "terminal_path",
        "expected_login",
        "expected_server",
        "expected_currency",
    ),
    "terminal": (
        "name",
        "company",
        "path",
        "data_path",
        "commondata_path",
        "build",
        "version",
        "max_bars",
    ),
}


def _iso_now(now: Optional[datetime] = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(path: Path) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _contained(root: Path, relative: str) -> Path:
    root = Path(root).resolve()
    candidate = (root / relative).resolve()
    candidate.relative_to(root)
    return candidate


def _safe_native_root(path: Path) -> Path:
    root = Path(path).resolve()
    if tuple(part.lower() for part in root.parts[-2:]) != ("data", "native"):
        raise ValueError("native root must resolve to a data/native directory")
    return root


def _safe_component(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not _SAFE_COMPONENT.fullmatch(text) or text in {".", ".."}:
        raise ValueError(f"unsafe {label}: {text!r}")
    return text


def _atomic_create_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"immutable record already exists: {path}")
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.staging")
    try:
        with temp.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        return path
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def _atomic_replace_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.staging")
    try:
        with temp.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        return path
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def _write_audit_leaf(
    audit_root: Path,
    symbol: str,
    timeframe: str,
    parity: Mapping[str, Any],
    sidecar: Mapping[str, Any],
) -> Tuple[Path, Path]:
    final_dir = audit_root / symbol / timeframe
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    if final_dir.exists():
        raise FileExistsError(f"immutable audit leaf already exists: {final_dir}")
    staging = final_dir.parent / f".{timeframe}.{uuid.uuid4().hex}.staging"
    staging.mkdir(parents=False, exist_ok=False)
    try:
        parity_path = staging / "parity.json"
        sidecar_path = staging / "bars.meta.json"
        for target, payload in ((parity_path, parity), (sidecar_path, sidecar)):
            with target.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(staging, final_dir)
        return final_dir / "parity.json", final_dir / "bars.meta.json"
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _read_bars(path: Path) -> pd.DataFrame:
    return normalize_ohlc_frame(pd.read_csv(path))


def _strict_int(value: Any) -> Optional[int]:
    return value if type(value) is int else None


def _canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request_contract(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    version = str(value.get("window_contract_version") or "")
    fields = _REQUEST_CONTRACT_FIELDS
    if version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        if str(value.get("timeframe_request_map_schema_version") or "") != (
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        ):
            return None
        fields = (*fields, *_V3_REQUEST_CONTRACT_FIELDS)
    elif version:
        return None
    if any(field_name not in value for field_name in fields):
        return None
    contract = {field_name: value[field_name] for field_name in fields}
    if version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        boundaries = contract.get("timeframe_calendar_boundaries")
        if not isinstance(boundaries, Mapping) or not boundaries:
            return None
        if str(contract.get("timeframe_calendar_boundaries_sha256") or "") != (
            _canonical_json_sha256(boundaries)
        ):
            return None
        request_map_payload = {
            "schema_version": contract.get("timeframe_request_map_schema_version"),
            "window_contract_version": contract.get("window_contract_version"),
            "common_as_of_utc": contract.get("common_as_of_utc"),
            "timeframe_window_years": contract.get("timeframe_window_years"),
            "timeframe_calendar_boundaries": boundaries,
            "timeframe_required_evidence_lead_start_utc": contract.get(
                "timeframe_required_evidence_lead_start_utc"
            ),
            "timeframe_source_request_intervals": contract.get(
                "timeframe_source_request_intervals"
            ),
        }
        if str(contract.get("timeframe_request_map_sha256") or "") != (
            _canonical_json_sha256(request_map_payload)
        ):
            return None
    return contract


def _source_identity(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    identity: Dict[str, Any] = {}
    for section, field_names in _SOURCE_IDENTITY_FIELDS.items():
        section_value = value.get(section)
        if not isinstance(section_value, Mapping):
            continue
        selected = {
            field_name: section_value[field_name]
            for field_name in field_names
            if field_name in section_value
        }
        if selected:
            identity[section] = selected
    return identity


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _pagination_contract_errors(
    provenance: Mapping[str, Any],
    *,
    timeframe: str,
    manifest_request: Mapping[str, Any],
    manifest_source_environment: Mapping[str, Any],
    availability_class: str,
    failure_status: str,
) -> list[str]:
    """Prove that this leaf attempted every page in the immutable horizon."""

    errors: list[str] = []
    request = provenance.get("request")
    pagination = provenance.get("pagination")
    if not isinstance(request, Mapping):
        return ["SOURCE_REQUEST_MISSING"]
    if not isinstance(pagination, Mapping):
        return ["SOURCE_PAGINATION_MISSING"]
    seconds = TIMEFRAME_SECONDS[timeframe]
    try:
        common_start = _utc_timestamp(manifest_request.get("common_requested_start_utc"))
        common_as_of = _utc_timestamp(manifest_request.get("common_as_of_utc"))
        fetch_start = _utc_timestamp(request.get("native_fetch_start_utc"))
        fetch_end = _utc_timestamp(request.get("native_fetch_end_exclusive_utc"))
    except Exception:
        return ["SOURCE_FETCH_BOUNDARY_INVALID"]
    if (
        str(manifest_request.get("window_contract_version") or "")
        == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    ):
        timeframe_intervals = manifest_request.get(
            "timeframe_source_request_intervals"
        )
        raw_interval = (
            timeframe_intervals.get(timeframe)
            if isinstance(timeframe_intervals, Mapping)
            else None
        )
        try:
            expected_start = _utc_timestamp(
                raw_interval.get("start_utc")
                if isinstance(raw_interval, Mapping)
                else None
            )
            expected_end = _utc_timestamp(
                raw_interval.get("end_exclusive_utc")
                if isinstance(raw_interval, Mapping)
                else None
            )
        except Exception:
            return ["SOURCE_TIMEFRAME_FETCH_INTERVAL_INVALID"]
        if request.get("source_request_interval") != raw_interval:
            errors.append("SOURCE_TIMEFRAME_FETCH_INTERVAL_PROJECTION_MISMATCH")
    elif timeframe in {"H4", "D1"}:
        expected_start = common_start - pd.Timedelta(seconds=seconds)
        expected_end = common_as_of + pd.Timedelta(seconds=1)
    else:
        expected_start = pd.Timestamp(
            ceil_utc_boundary(common_start.to_pydatetime(), seconds)
        )
        expected_end = pd.Timestamp(
            floor_utc_boundary(common_as_of.to_pydatetime(), seconds)
        )

    if timeframe in {"H4", "D1"}:
        expected_alignment = "broker_observed_successor_or_nominal_close_v1"
        expected_slack = 1
    else:
        expected_alignment = "fixed_utc_native_boundary_v1"
        expected_slack = 0
    if fetch_start != expected_start or fetch_end != expected_end or fetch_end <= fetch_start:
        errors.append("SOURCE_FETCH_BOUNDARY_MISMATCH")
    if str(request.get("alignment_mode") or "") != expected_alignment:
        errors.append("SOURCE_FETCH_ALIGNMENT_MODE_MISMATCH")
    if _strict_int(request.get("successor_sentinel_fetch_slack_seconds")) != expected_slack:
        errors.append("SOURCE_FETCH_SENTINEL_SLACK_MISMATCH")

    terminal = (
        manifest_source_environment.get("terminal")
        if isinstance(manifest_source_environment.get("terminal"), Mapping)
        else {}
    )
    terminal_max_bars_raw = terminal.get("max_bars", terminal.get("maxbars", 0))
    terminal_max_bars = _strict_int(terminal_max_bars_raw)
    if terminal_max_bars is None or terminal_max_bars < 0:
        errors.append("SOURCE_TERMINAL_MAX_BARS_INVALID")
        terminal_max_bars = 0
    configured_slots = max(1, int(DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds))
    if terminal_max_bars > 0:
        configured_slots = min(configured_slots, max(1, int(terminal_max_bars * 0.90)))
    expected_page_span_seconds = configured_slots * seconds
    page_span_seconds = _strict_int(pagination.get("page_span_seconds"))
    if page_span_seconds != expected_page_span_seconds:
        errors.append("SOURCE_PAGINATION_PAGE_SPAN_MISMATCH")

    expected_intervals: list[Tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = expected_start
    while cursor < expected_end:
        page_end = min(
            cursor + pd.Timedelta(seconds=expected_page_span_seconds),
            expected_end,
        )
        expected_intervals.append((cursor, page_end))
        cursor = page_end
    expected_interval_keys = {
        (int(start.value), int(end.value)) for start, end in expected_intervals
    }
    pages_requested = _strict_int(pagination.get("pages_requested"))
    pages_with_data = _strict_int(pagination.get("pages_with_data"))
    attempts_total = _strict_int(pagination.get("attempts_total"))
    retries_per_page = _strict_int(pagination.get("retries_per_page"))
    manifest_retries = _strict_int(manifest_request.get("retries"))
    empty_pages = pagination.get("empty_pages")
    failed_pages = pagination.get("failed_pages")
    if pages_requested != len(expected_intervals):
        errors.append("SOURCE_PAGINATION_PAGE_COUNT_MISMATCH")
    if pages_with_data is None or not 0 <= pages_with_data <= len(expected_intervals):
        errors.append("SOURCE_PAGINATION_DATA_PAGE_COUNT_INVALID")
    if not isinstance(empty_pages, list) or not isinstance(failed_pages, list):
        errors.append("SOURCE_PAGINATION_PAGE_LEDGER_INVALID")
        empty_pages = []
        failed_pages = []
    if failed_pages:
        errors.append("SOURCE_PAGINATION_FAILED_PAGE_PRESENT")
    if (
        pages_requested is not None
        and pages_with_data is not None
        and pages_with_data + len(empty_pages) + len(failed_pages) != pages_requested
    ):
        errors.append("SOURCE_PAGINATION_DISPOSITION_COUNT_MISMATCH")
    if manifest_retries is None or manifest_retries <= 0:
        errors.append("SOURCE_MANIFEST_RETRY_CONTRACT_INVALID")
    if retries_per_page != manifest_retries:
        errors.append("SOURCE_PAGINATION_RETRY_CONTRACT_MISMATCH")
    if (
        attempts_total is None
        or pages_requested is None
        or attempts_total < pages_requested
        or (
            manifest_retries is not None
            and manifest_retries > 0
            and attempts_total > pages_requested * manifest_retries
        )
    ):
        errors.append("SOURCE_PAGINATION_ATTEMPT_COUNT_INVALID")

    observed_page_keys: set[Tuple[int, int]] = set()
    for page in [*empty_pages, *failed_pages]:
        if not isinstance(page, Mapping):
            errors.append("SOURCE_PAGINATION_PAGE_RECORD_INVALID")
            continue
        try:
            page_start = _utc_timestamp(page.get("start_utc"))
            page_end = _utc_timestamp(page.get("end_exclusive_utc"))
            key = (int(page_start.value), int(page_end.value))
        except Exception:
            errors.append("SOURCE_PAGINATION_PAGE_BOUNDARY_INVALID")
            continue
        if key not in expected_interval_keys:
            errors.append("SOURCE_PAGINATION_PAGE_OUTSIDE_HORIZON")
        if key in observed_page_keys:
            errors.append("SOURCE_PAGINATION_DUPLICATE_PAGE_RECORD")
        observed_page_keys.add(key)
        attempts = _strict_int(page.get("attempts"))
        if (
            attempts is None
            or attempts <= 0
            or (manifest_retries is not None and attempts > manifest_retries)
        ):
            errors.append("SOURCE_PAGINATION_PAGE_ATTEMPTS_INVALID")

    if failure_status == "no_history":
        if pages_with_data != 0 or observed_page_keys != expected_interval_keys:
            errors.append("SOURCE_NO_HISTORY_PAGE_PROOF_INCOMPLETE")
    elif failure_status == "no_fully_closed_history":
        if pages_with_data is None or pages_with_data <= 0:
            errors.append("SOURCE_NO_FULLY_CLOSED_HISTORY_DATA_PROOF_MISSING")
    elif availability_class in {"success", "unavailable"}:
        if pages_with_data is None or pages_with_data <= 0:
            errors.append("SOURCE_PUBLISHED_HISTORY_DATA_PAGE_PROOF_MISSING")
    return list(dict.fromkeys(errors))


def _source_leaf(
    native_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    run_manifest_entry: Optional[Mapping[str, Any]] = None,
    run_manifest_errors: Sequence[str] = (),
    run_manifest_request: Optional[Mapping[str, Any]] = None,
    run_source_environment: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    leaf = native_root / "runs" / run_id / symbol / timeframe
    bars_path = leaf / "bars.csv"
    provenance_path = leaf / "provenance.json"
    errors: list[str] = list(run_manifest_errors)
    availability_class = "invalid"
    manifest_failure = ""
    manifest_rows = -1
    if not isinstance(run_manifest_entry, Mapping):
        errors.append("RUN_MANIFEST_CELL_MISSING")
    else:
        manifest_failure = str(
            run_manifest_entry.get("failure_status") or ""
        ).strip().lower()
        availability_class, coherence_error = classify_native_availability_cell(
            run_manifest_entry.get("status"),
            manifest_failure,
            run_manifest_entry.get("rows"),
        )
        if availability_class == "invalid":
            errors.append(
                f"RUN_MANIFEST_AVAILABILITY_INVALID:{coherence_error or 'unknown'}"
            )
        try:
            manifest_rows = int(run_manifest_entry.get("rows", -1))
        except (TypeError, ValueError, OverflowError):
            manifest_rows = -1

    bars_required = availability_class == "success" or (
        availability_class == "unavailable" and manifest_failure == "partial_history"
    )
    if bars_required and not bars_path.is_file():
        errors.append("SOURCE_BARS_MISSING")
    if not bars_required and bars_path.exists():
        errors.append("SOURCE_BARS_UNEXPECTED")
    if not provenance_path.exists():
        errors.append("SOURCE_PROVENANCE_MISSING")
    if not provenance_path.exists():
        return {
            "bars_path": bars_path,
            "provenance_path": provenance_path,
            "provenance": {},
            "frame": None,
            "availability_class": availability_class,
            "errors": errors,
        }
    try:
        provenance = _json(provenance_path)
    except Exception as exc:
        return {
            "bars_path": bars_path,
            "provenance_path": provenance_path,
            "provenance": {},
            "frame": None,
            "availability_class": availability_class,
            "errors": [f"SOURCE_PROVENANCE_INVALID:{type(exc).__name__}"],
        }
    frame: Optional[pd.DataFrame] = None
    observed_sha: Optional[str] = None
    if bars_path.is_file():
        try:
            frame = _read_bars(bars_path)
        except Exception as exc:
            errors.append(f"SOURCE_BARS_INVALID:{type(exc).__name__}")
        observed_sha = sha256_file(bars_path)
    if bars_required and observed_sha is not None:
        if str((run_manifest_entry or {}).get("sha256") or "").lower() != observed_sha.lower():
            errors.append("RUN_MANIFEST_BARS_SHA256_MISMATCH")
        if frame is not None and manifest_rows != len(frame):
            errors.append("RUN_MANIFEST_ROW_COUNT_MISMATCH")
    elif not bars_required and (run_manifest_entry or {}).get("sha256") is not None:
        errors.append("RUN_MANIFEST_UNAVAILABLE_SHA256_PRESENT")

    if str(provenance.get("schema_version")) != SCHEMA_VERSION:
        errors.append("SOURCE_SCHEMA_INVALID")
    if str(provenance.get("acquisition_contract_version") or "") != (
        ACQUISITION_CONTRACT_VERSION
    ):
        errors.append("SOURCE_ACQUISITION_CONTRACT_INVALID")
    if provenance.get("immutable_record") is not True:
        errors.append("SOURCE_NOT_IMMUTABLE")
    if str(provenance.get("run_id") or "") != run_id:
        errors.append("SOURCE_RUN_ID_MISMATCH")
    if str(provenance.get("symbol", "")).upper() != symbol:
        errors.append("SOURCE_SYMBOL_MISMATCH")
    if str(provenance.get("canonical_symbol", "")).upper() != symbol:
        errors.append("SOURCE_CANONICAL_SYMBOL_MISMATCH")
    if str(provenance.get("timeframe", "")).upper() != timeframe:
        errors.append("SOURCE_TIMEFRAME_MISMATCH")
    if _strict_int(provenance.get("timeframe_seconds")) != TIMEFRAME_SECONDS[timeframe]:
        errors.append("SOURCE_TIMEFRAME_SECONDS_MISMATCH")
    if str(provenance.get("source_kind", "")).lower() != "broker_native":
        errors.append("SOURCE_KIND_INVALID")
    if str(provenance.get("timezone", "")).upper() != "UTC":
        errors.append("SOURCE_TIMEZONE_INVALID")
    if str(provenance.get("timestamp_semantics", "")).lower() != "bar_open_utc":
        errors.append("SOURCE_TIMESTAMP_SEMANTICS_INVALID")
    if str(provenance.get("bar_close_column", "")) != "_BAR_CLOSE_TS":
        errors.append("SOURCE_BAR_CLOSE_SEMANTICS_INVALID")
    provenance_status = str(provenance.get("status") or "").strip().lower()
    provenance_failure = str(provenance.get("failure_status") or "").strip().lower()
    if provenance_status != str((run_manifest_entry or {}).get("status") or "").lower():
        errors.append("SOURCE_MANIFEST_STATUS_MISMATCH")
    if provenance_failure != manifest_failure:
        errors.append("SOURCE_MANIFEST_FAILURE_STATUS_MISMATCH")
    source_request_contract = _request_contract(provenance.get("request"))
    manifest_request_contract = _request_contract(run_manifest_request)
    if source_request_contract is None:
        errors.append("SOURCE_REQUEST_CONTRACT_INVALID")
    if manifest_request_contract is None:
        errors.append("RUN_MANIFEST_REQUEST_CONTRACT_INVALID")
    if source_request_contract is not None and manifest_request_contract is not None:
        source_contract_version = str(
            source_request_contract.get("window_contract_version") or ""
        )
        manifest_contract_version = str(
            manifest_request_contract.get("window_contract_version") or ""
        )
        if source_contract_version != manifest_contract_version:
            errors.append("SOURCE_MANIFEST_REQUEST_CONTRACT_MISMATCH")
        elif source_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            # V3 deliberately gives each timeframe its own train start.  The
            # run manifest's legacy compatibility projection is M5 (or the
            # first requested timeframe), whereas each leaf projects its own
            # exact cell window.  Compare the authenticated full request map
            # and invariant fields, then validate the leaf projection below;
            # never compare a D1/H4/H1 leaf to the M5 projection.
            projection_fields = {"window_years", "common_calendar_boundaries"}
            source_shared = {
                key: value
                for key, value in source_request_contract.items()
                if key not in projection_fields
            }
            manifest_shared = {
                key: value
                for key, value in manifest_request_contract.items()
                if key not in projection_fields
            }
            if source_shared != manifest_shared:
                errors.append("SOURCE_MANIFEST_REQUEST_CONTRACT_MISMATCH")

            timeframe_years = (
                source_request_contract.get("timeframe_window_years") or {}
            )
            timeframe_boundaries = (
                source_request_contract.get("timeframe_calendar_boundaries") or {}
            )
            expected_years = (
                timeframe_years.get(timeframe)
                if isinstance(timeframe_years, Mapping)
                else None
            )
            expected_boundaries = (
                timeframe_boundaries.get(timeframe)
                if isinstance(timeframe_boundaries, Mapping)
                else None
            )
            if (
                expected_years is None
                or source_request_contract.get("window_years") != expected_years
            ):
                errors.append(
                    "SOURCE_TIMEFRAME_REQUEST_PROJECTION_MISMATCH:window_years"
                )
            if (
                expected_boundaries is None
                or source_request_contract.get("common_calendar_boundaries")
                != expected_boundaries
            ):
                errors.append(
                    "SOURCE_TIMEFRAME_REQUEST_PROJECTION_MISMATCH:common_calendar_boundaries"
                )
        elif source_request_contract != manifest_request_contract:
            errors.append("SOURCE_MANIFEST_REQUEST_CONTRACT_MISMATCH")
    if (
        source_request_contract is not None
        and str(source_request_contract.get("window_contract_version") or "")
        == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    ):
        expected_projection = {
            "cell_timeframe": timeframe,
            "cell_timeframe_window_years": (
                source_request_contract.get("timeframe_window_years") or {}
            ).get(timeframe),
            "cell_calendar_boundaries": (
                source_request_contract.get("timeframe_calendar_boundaries") or {}
            ).get(timeframe),
            "cell_required_evidence_lead_start_utc": (
                source_request_contract.get(
                    "timeframe_required_evidence_lead_start_utc"
                )
                or {}
            ).get(timeframe),
            "source_request_interval": (
                source_request_contract.get("timeframe_source_request_intervals")
                or {}
            ).get(timeframe),
        }
        for field_name, expected_value in expected_projection.items():
            if expected_value is None or provenance.get("request", {}).get(
                field_name
            ) != expected_value:
                errors.append(
                    f"SOURCE_TIMEFRAME_REQUEST_PROJECTION_MISMATCH:{field_name}"
                )
    source_identity = _source_identity(provenance.get("source_environment"))
    manifest_source_identity = _source_identity(run_source_environment)
    if not source_identity:
        errors.append("SOURCE_IDENTITY_MISSING")
    if not manifest_source_identity:
        errors.append("RUN_MANIFEST_SOURCE_IDENTITY_MISSING")
    if (
        source_identity
        and manifest_source_identity
        and source_identity != manifest_source_identity
    ):
        errors.append("SOURCE_MANIFEST_IDENTITY_MISMATCH")
    try:
        provenance_rows = int(provenance.get("rows", -1))
    except (TypeError, ValueError, OverflowError):
        provenance_rows = -1
    if provenance_rows != manifest_rows:
        errors.append("SOURCE_MANIFEST_PUBLISHED_ROWS_MISMATCH")

    if availability_class == "success":
        if provenance_status != _SUCCESS_SOURCE_STATUS or provenance_failure:
            errors.append("SOURCE_ACQUISITION_SUCCESS_INVALID")
        if provenance.get("complete") is not True:
            errors.append("SOURCE_INCOMPLETE")
    elif availability_class == "unavailable":
        if provenance.get("complete") is not False:
            errors.append("SOURCE_UNAVAILABLE_MARKED_COMPLETE")
        if provenance.get("eligible_for_downstream_validation") is not False or provenance.get(
            "eligible_for_optimization"
        ) is not False:
            errors.append("SOURCE_UNAVAILABLE_MARKED_ELIGIBLE")
        pagination = provenance.get("pagination")
        if not isinstance(pagination, Mapping) or list(
            pagination.get("failed_pages") or []
        ):
            errors.append("SOURCE_UNAVAILABLE_PAGINATION_INVALID")
        if manifest_failure == "partial_history":
            if provenance_status != "partial" or frame is None:
                errors.append("SOURCE_PARTIAL_HISTORY_INVALID")
        elif manifest_failure in {"no_history", "no_fully_closed_history"}:
            if provenance_status != "failed" or frame is not None or provenance_rows != 0:
                errors.append("SOURCE_NO_HISTORY_INVALID")
            if provenance.get("artifact") is not None or provenance.get("sha256") is not None:
                errors.append("SOURCE_NO_HISTORY_ARTIFACT_PRESENT")
            if not isinstance(pagination, Mapping):
                errors.append("SOURCE_NO_HISTORY_PAGINATION_MISSING")
            else:
                for field_name in ("pages_requested", "pages_with_data"):
                    if isinstance(pagination.get(field_name), bool):
                        errors.append("SOURCE_NO_HISTORY_PAGINATION_INVALID")
                try:
                    pages_requested = int(pagination.get("pages_requested", -1))
                    pages_with_data = int(pagination.get("pages_with_data", -1))
                except (TypeError, ValueError, OverflowError):
                    pages_requested = pages_with_data = -1
                if pages_requested <= 0:
                    errors.append("SOURCE_NO_HISTORY_NO_PAGES_REQUESTED")
                if manifest_failure == "no_history" and pages_with_data != 0:
                    errors.append("SOURCE_NO_HISTORY_DATA_PAGE_PRESENT")
    else:
        errors.append("SOURCE_AVAILABILITY_CLASS_INVALID")

    if (
        availability_class in {"success", "unavailable"}
        and isinstance(run_manifest_request, Mapping)
        and isinstance(run_source_environment, Mapping)
    ):
        errors.extend(
            _pagination_contract_errors(
                provenance,
                timeframe=timeframe,
                manifest_request=run_manifest_request,
                manifest_source_environment=run_source_environment,
                availability_class=availability_class,
                failure_status=manifest_failure,
            )
        )

    if observed_sha is not None and str(provenance.get("sha256") or "").lower() != observed_sha.lower():
        errors.append("SOURCE_SHA256_MISMATCH")
    if bars_required:
        artifact = provenance.get("artifact")
        if not isinstance(artifact, Mapping):
            errors.append("SOURCE_ARTIFACT_MISSING")
        else:
            if str(artifact.get("relative_path") or "") != "bars.csv":
                errors.append("SOURCE_ARTIFACT_PATH_INVALID")
            if str(artifact.get("sha256") or "").lower() != str(
                observed_sha or ""
            ).lower():
                errors.append("SOURCE_ARTIFACT_SHA256_MISMATCH")
            for field_name, observed in (
                ("rows", len(frame) if frame is not None else -1),
                ("bytes", int(bars_path.stat().st_size) if bars_path.is_file() else -1),
            ):
                raw = artifact.get(field_name)
                if isinstance(raw, bool):
                    errors.append(f"SOURCE_ARTIFACT_{field_name.upper()}_INVALID")
                    continue
                try:
                    declared = int(raw)
                except (TypeError, ValueError, OverflowError):
                    declared = -1
                if declared != observed:
                    errors.append(f"SOURCE_ARTIFACT_{field_name.upper()}_MISMATCH")
    if frame is not None:
        if provenance_rows != len(frame):
            errors.append("SOURCE_ROW_COUNT_MISMATCH")
        if "_BAR_CLOSE_TS" not in frame.columns:
            errors.append("SOURCE_BAR_CLOSE_COLUMN_MISSING")
        try:
            declared_start = pd.Timestamp(provenance.get("start_ts")).tz_convert("UTC")
            declared_end = pd.Timestamp(provenance.get("end_ts")).tz_convert("UTC")
        except Exception:
            errors.append("SOURCE_TIMESTAMP_DECLARATION_INVALID")
        else:
            if declared_start != frame.index[0]:
                errors.append("SOURCE_START_TS_MISMATCH")
            if declared_end != frame.index[-1]:
                errors.append("SOURCE_END_TS_MISMATCH")
    return {
        "bars_path": bars_path,
        "provenance_path": provenance_path,
        "provenance": provenance,
        "provenance_sha256": sha256_file(provenance_path),
        "bars_sha256": observed_sha,
        "frame": frame,
        "availability_class": availability_class,
        "errors": list(dict.fromkeys(errors)),
    }


def _partition_windows(
    request: Mapping[str, Any],
    timeframe: Optional[str] = None,
) -> Tuple[Dict[str, Tuple[datetime, datetime]], list[str]]:
    raw = request.get("common_calendar_boundaries")
    if (
        str(request.get("window_contract_version") or "")
        == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    ):
        raw_map = request.get("timeframe_calendar_boundaries")
        raw = (
            raw_map.get(str(timeframe or "").upper())
            if isinstance(raw_map, Mapping)
            else None
        )
    windows: Dict[str, Tuple[datetime, datetime]] = {}
    errors: list[str] = []
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        item = raw.get(partition) if isinstance(raw, Mapping) else None
        if not isinstance(item, Mapping):
            errors.append(f"RUN_MANIFEST_BOUNDARY_MISSING:{partition}")
            continue
        try:
            start = pd.Timestamp(item.get("start_ts"))
            end = pd.Timestamp(item.get("end_ts"))
            if start.tzinfo is None or end.tzinfo is None:
                raise ValueError("timezone missing")
            start = start.tz_convert("UTC")
            end = end.tz_convert("UTC")
        except Exception:
            errors.append(f"RUN_MANIFEST_BOUNDARY_INVALID:{partition}")
            continue
        if start >= end:
            errors.append(f"RUN_MANIFEST_BOUNDARY_ORDER_INVALID:{partition}")
            continue
        windows[partition] = (start.to_pydatetime(), end.to_pydatetime())
    return windows, errors


def _strict_finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) else None


def _coverage_verdict(
    provenance: Mapping[str, Any],
    frame: Optional[pd.DataFrame],
    minimum: float,
    partition_windows: Mapping[str, Tuple[datetime, datetime]],
) -> Tuple[str, list[str], Dict[str, Optional[float]]]:
    if frame is None or set(partition_windows) != set(NATIVE_EVIDENCE_PARTITIONS):
        return (
            "ERROR",
            ["SESSION_COVERAGE_RECOMPUTE_INPUT_INVALID"],
            {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS},
        )
    details = provenance.get("expected_session_coverage_details_by_partition")
    declared_method = (
        str(details.get("method") or "") if isinstance(details, Mapping) else ""
    )
    if (
        not isinstance(details, Mapping)
        or declared_method not in SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS
        or str(details.get("status") or "") != "estimated"
    ):
        return (
            "ERROR",
            ["SESSION_COVERAGE_METHOD_OR_STATUS_INVALID"],
            {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS},
        )
    if declared_method == NATIVE_SESSION_COVERAGE_METHOD:
        recomputed, recomputed_details = segmented_session_coverage_by_partition_v2(
            frame,
            partition_windows,
        )
    else:
        recomputed, recomputed_details = expected_session_coverage_by_partition(
            frame,
            partition_windows,
        )
    if str(recomputed_details.get("status") or "") != "estimated":
        return (
            "ERROR",
            ["SESSION_COVERAGE_RECOMPUTE_NOT_ESTIMATED"],
            {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS},
        )
    declared_coverage = provenance.get("expected_session_coverage_by_partition")
    declared_observed = provenance.get("observed_session_bars_by_partition")
    declared_expected = provenance.get("expected_session_bars_by_partition")
    declared_partitions = details.get("partitions")
    provenance_request = provenance.get("request")
    provenance_boundaries = (
        provenance_request.get("cell_calendar_boundaries")
        or provenance_request.get("common_calendar_boundaries")
        if isinstance(provenance_request, Mapping)
        else None
    )
    errors: list[str] = []
    if declared_method == NATIVE_SESSION_COVERAGE_METHOD and dict(details) != dict(
        recomputed_details
    ):
        errors.append("SESSION_COVERAGE_SEGMENTED_DETAILS_MISMATCH")
    for mapping, code in (
        (declared_coverage, "SESSION_COVERAGE_MAP_INVALID"),
        (declared_observed, "SESSION_OBSERVED_MAP_INVALID"),
        (declared_expected, "SESSION_EXPECTED_MAP_INVALID"),
        (declared_partitions, "SESSION_COVERAGE_DETAILS_MAP_INVALID"),
        (provenance_boundaries, "SESSION_COVERAGE_BOUNDARIES_INVALID"),
    ):
        if not isinstance(mapping, Mapping) or set(mapping) != set(
            NATIVE_EVIDENCE_PARTITIONS
        ):
            errors.append(code)
    if declared_method == LEGACY_NATIVE_SESSION_COVERAGE_METHOD:
        for field_name in (
            "complete_train_weeks",
            "nominal_bars_per_week_q75",
        ):
            declared_value = _strict_finite_number(details.get(field_name))
            recomputed_value = _strict_finite_number(recomputed_details.get(field_name))
            if declared_value is None or recomputed_value is None or not math.isclose(
                declared_value,
                recomputed_value,
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                errors.append(f"SESSION_COVERAGE_{field_name.upper()}_MISMATCH")
        if str(details.get("schedule_source_partition") or "") != "optimizer_train":
            errors.append("SESSION_COVERAGE_SCHEDULE_SOURCE_INVALID")
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        recomputed_value = _strict_finite_number(recomputed.get(partition))
        declared_value = (
            _strict_finite_number(declared_coverage.get(partition))
            if isinstance(declared_coverage, Mapping)
            else None
        )
        detail = (
            declared_partitions.get(partition)
            if isinstance(declared_partitions, Mapping)
            else None
        )
        expected_detail = (recomputed_details.get("partitions") or {}).get(partition)
        if (
            recomputed_value is None
            or declared_value is None
            or not math.isclose(declared_value, recomputed_value, rel_tol=0.0, abs_tol=1e-12)
        ):
            errors.append(f"SESSION_COVERAGE_VALUE_MISMATCH:{partition}")
        if not isinstance(detail, Mapping) or not isinstance(expected_detail, Mapping):
            errors.append(f"SESSION_COVERAGE_DETAIL_INVALID:{partition}")
            continue
        for field_name in ("observed_rows", "expected_rows_from_train_session_rate", "coverage"):
            declared_number = _strict_finite_number(detail.get(field_name))
            expected_number = _strict_finite_number(expected_detail.get(field_name))
            if declared_number is None or expected_number is None or not math.isclose(
                declared_number,
                expected_number,
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                errors.append(
                    f"SESSION_COVERAGE_DETAIL_{field_name.upper()}_MISMATCH:{partition}"
                )
        for top_mapping, detail_field, code in (
            (declared_observed, "observed_rows", "SESSION_OBSERVED_COUNT_MISMATCH"),
            (
                declared_expected,
                "expected_rows_from_train_session_rate",
                "SESSION_EXPECTED_COUNT_MISMATCH",
            ),
        ):
            top_value = (
                _strict_finite_number(top_mapping.get(partition))
                if isinstance(top_mapping, Mapping)
                else None
            )
            detail_value = _strict_finite_number(detail.get(detail_field))
            if top_value is None or detail_value is None or not math.isclose(
                top_value,
                detail_value,
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                errors.append(f"{code}:{partition}")
        start, end = partition_windows[partition]
        boundary = (
            provenance_boundaries.get(partition)
            if isinstance(provenance_boundaries, Mapping)
            else None
        )
        for candidate, code in (
            (boundary, "SESSION_PROVENANCE_BOUNDARY_MISMATCH"),
            (detail, "SESSION_DETAIL_BOUNDARY_MISMATCH"),
        ):
            if not isinstance(candidate, Mapping):
                errors.append(f"{code}:{partition}")
                continue
            try:
                candidate_start = pd.Timestamp(candidate.get("start_ts")).tz_convert("UTC")
                candidate_end = pd.Timestamp(candidate.get("end_ts")).tz_convert("UTC")
            except Exception:
                errors.append(f"{code}:{partition}")
                continue
            if candidate_start != pd.Timestamp(start) or candidate_end != pd.Timestamp(end):
                errors.append(f"{code}:{partition}")
    if errors:
        return "ERROR", list(dict.fromkeys(errors)), dict(recomputed)
    if declared_method == NATIVE_SESSION_COVERAGE_METHOD:
        if recomputed_details.get("conservative_lower_bound_available") is not True:
            return (
                "ERROR",
                ["SESSION_COVERAGE_CONSERVATIVE_BOUND_UNAVAILABLE"],
                dict(recomputed),
            )
        for partition in NATIVE_EVIDENCE_PARTITIONS:
            detail = (recomputed_details.get("partitions") or {}).get(partition) or {}
            lower_bound = _strict_finite_number(
                detail.get("conservative_coverage_lower_bound")
            )
            point = _strict_finite_number(recomputed.get(partition))
            if (
                lower_bound is None
                or point is None
                or not 0.0 <= lower_bound <= point + 1e-12 <= 1.0 + 1e-12
            ):
                return (
                    "ERROR",
                    [f"SESSION_COVERAGE_LOWER_BOUND_INVALID:{partition}"],
                    dict(recomputed),
                )
            missing_weeks = int(detail.get("missing_calendar_weeks", 0) or 0)
            thin_weeks = int(
                detail.get("weeks_below_half_expected_schedule", 0) or 0
            )
            gap = detail.get("largest_unexplained_gap")
            if gap is not None:
                gap_seconds = (
                    _strict_finite_number(gap.get("seconds"))
                    if isinstance(gap, Mapping)
                    else None
                )
                if gap_seconds is None or gap_seconds < 0.0:
                    return (
                        "ERROR",
                        [f"SESSION_COVERAGE_GAP_DIAGNOSTIC_INVALID:{partition}"],
                        dict(recomputed),
                    )
            if (missing_weeks > 0 or thin_weeks > 0) and not isinstance(gap, Mapping):
                return (
                    "ERROR",
                    [f"SESSION_COVERAGE_GAP_DIAGNOSTIC_MISSING:{partition}"],
                    dict(recomputed),
                )
    history_status, reasons, normalized = classify_native_session_history(
        recomputed,
        minimum=minimum,
    )
    if history_status == "insufficient":
        return "FAIL", reasons, normalized
    if history_status == "invalid":
        return "ERROR", reasons, normalized
    return "PASS", [], normalized


def _merge_cell_status(
    source_errors: Sequence[str],
    parity: Mapping[str, Any],
    coverage_status: str,
    coverage_reasons: Sequence[str],
) -> Tuple[str, list[str]]:
    reasons = [f"SOURCE:{reason}" for reason in source_errors]
    reasons.extend(str(reason) for reason in parity.get("reason_codes", []) or [])
    reasons.extend(str(reason) for reason in coverage_reasons)
    parity_status = str(parity.get("status", "PENDING")).upper()
    normalized_coverage = str(coverage_status or "").strip().upper()
    if (
        source_errors
        or parity_status == "FAIL"
        or parity_status not in {"PASS", "FAIL", "PENDING", "NOT_APPLICABLE"}
        or normalized_coverage in {"FAIL", "ERROR"}
        or normalized_coverage not in {"PASS", "PENDING"}
    ):
        return "FAIL", list(dict.fromkeys(reasons))
    if parity_status == "PENDING" or normalized_coverage == "PENDING":
        return "PENDING", list(dict.fromkeys(reasons))
    return "PASS", list(dict.fromkeys(reasons))


def _symbol_availability_decision(
    symbol: str,
    timeframes: Sequence[str],
    run_manifest_cells: Mapping[Tuple[str, str], Mapping[str, Any]],
    run_manifest_errors: Sequence[str],
) -> Dict[str, Any]:
    """Partition a symbol using source availability facts, never performance.

    A transient fetch/write failure or a data-quality failure is activation
    blocking.  Only the small allow-list above may create an excluded NO_TRADE
    symbol.  This prevents a broken source (or a weak backtest) from shrinking
    the production universe unnoticed.
    """
    cells: Dict[str, Dict[str, Any]] = {}
    availability_reasons: list[str] = []
    blocking_reasons: list[str] = [str(reason) for reason in run_manifest_errors]
    for timeframe in timeframes:
        item = run_manifest_cells.get((symbol, timeframe))
        if not isinstance(item, Mapping):
            cells[timeframe] = {
                "status": "missing",
                "failure_status": "run_manifest_cell_missing",
                "rows": 0,
            }
            blocking_reasons.append(f"{timeframe}:RUN_MANIFEST_CELL_MISSING")
            continue
        status = str(item.get("status") or "").strip().lower()
        failure = str(item.get("failure_status") or "").strip().lower()
        try:
            rows = int(item.get("rows", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            rows = 0
        cells[timeframe] = {
            "status": status,
            "failure_status": failure or None,
            "rows": rows,
        }
        classification, coherence_error = classify_native_availability_cell(
            status,
            failure,
            rows,
        )
        if classification == "success":
            continue
        if classification == "unavailable":
            availability_reasons.append(f"{timeframe}:{failure.upper()}")
        else:
            blocking_reasons.append(
                f"{timeframe}:NON_AVAILABILITY_SOURCE_FAILURE:"
                f"{coherence_error or failure or status or 'unknown'}"
            )
    if blocking_reasons:
        status = "invalid"
    elif availability_reasons:
        status = "excluded_no_trade"
    else:
        status = "eligible"
    return {
        "symbol": symbol,
        "status": status,
        "decision_basis": "source_availability_only",
        "reason_codes": list(dict.fromkeys(availability_reasons or blocking_reasons)),
        "availability_cells": cells,
    }


def audit_and_activate(
    native_root: Path,
    *,
    run_id: str,
    symbols: Sequence[str],
    timeframes: Sequence[str] = DEFAULT_TIMEFRAMES,
    activation_id: Optional[str] = None,
    thresholds: Optional[ParityThresholds] = None,
    minimum_session_coverage: float = NATIVE_MINIMUM_SESSION_COVERAGE,
    tick_tolerance_overrides: Optional[Mapping[str, float]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    native_root = _safe_native_root(native_root)
    run_id = _safe_component(run_id, "run id")
    activation_id = activation_id or (
        (now or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    activation_id = _safe_component(activation_id, "activation id")
    canonical_symbols = tuple(
        dict.fromkeys(_safe_component(symbol, "symbol").upper() for symbol in symbols)
    )
    canonical_timeframes = tuple(
        dict.fromkeys(_safe_component(tf, "timeframe").upper() for tf in timeframes)
    )
    if not canonical_symbols or not canonical_timeframes:
        raise ValueError("symbols and timeframes are required")
    unsupported = sorted(set(canonical_timeframes) - set(DEFAULT_TIMEFRAMES))
    if unsupported:
        raise ValueError(f"unsupported native timeframe(s): {', '.join(unsupported)}")
    thresholds = thresholds or ParityThresholds()
    if not math.isclose(
        float(minimum_session_coverage),
        NATIVE_MINIMUM_SESSION_COVERAGE,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError(
            "production minimum_session_coverage is pinned at "
            f"{NATIVE_MINIMUM_SESSION_COVERAGE:.2f}"
        )
    if not 0.0 <= float(thresholds.min_membership_ratio) <= 1.0:
        raise ValueError("min_membership_ratio must be between 0 and 1")
    if not 0.0 <= float(thresholds.max_ohlc_mismatch_ratio) <= 1.0:
        raise ValueError("max_ohlc_mismatch_ratio must be between 0 and 1")
    if not 0.0 <= float(thresholds.max_unassigned_m5_ratio) <= 1.0:
        raise ValueError("max_unassigned_m5_ratio must be between 0 and 1")
    if int(thresholds.min_compared_native_bars) <= 0:
        raise ValueError("min_compared_native_bars must be positive")
    overrides = {str(key).upper(): float(value) for key, value in (tick_tolerance_overrides or {}).items()}
    started = _iso_now(now)
    audit_root = native_root / "audits" / activation_id
    if audit_root.exists():
        raise FileExistsError(f"immutable activation audit already exists: {audit_root}")
    audit_root.mkdir(parents=True, exist_ok=False)
    previous_pointer = native_root / "ACTIVE.json"
    previous_pointer_sha = sha256_file(previous_pointer) if previous_pointer.exists() else None
    run_manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    run_manifest_errors: list[str] = []
    run_manifest: Dict[str, Any] = {}
    run_request: Dict[str, Any] = {}
    run_source_environment: Dict[str, Any] = {}
    if not run_manifest_path.exists():
        run_manifest_errors.append("RUN_MANIFEST_MISSING")
    else:
        try:
            run_manifest = _json(run_manifest_path)
        except Exception as exc:
            run_manifest_errors.append(f"RUN_MANIFEST_INVALID:{type(exc).__name__}")
    if run_manifest:
        if str(run_manifest.get("schema_version")) != MANIFEST_SCHEMA_VERSION:
            run_manifest_errors.append("RUN_MANIFEST_SCHEMA_INVALID")
        if str(run_manifest.get("run_id", "")) != run_id:
            run_manifest_errors.append("RUN_MANIFEST_ID_MISMATCH")
        if run_manifest.get("immutable_record") is not True:
            run_manifest_errors.append("RUN_MANIFEST_NOT_IMMUTABLE")
        if run_manifest.get("complete") is not True:
            run_manifest_errors.append("RUN_MANIFEST_NOT_COMPLETE")
        request = (
            run_manifest.get("request")
            if isinstance(run_manifest.get("request"), Mapping)
            else {}
        )
        run_request = dict(request)
        if _request_contract(run_request) is None:
            run_manifest_errors.append("RUN_MANIFEST_REQUEST_CONTRACT_INVALID")
        if (
            str(run_request.get("window_contract_version") or "")
            == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        ):
            expected_timeframe_set = set(canonical_timeframes)
            timeframe_boundaries = run_request.get(
                "timeframe_calendar_boundaries"
            )
            timeframe_years = run_request.get("timeframe_window_years")
            timeframe_leads = run_request.get(
                "timeframe_required_evidence_lead_start_utc"
            )
            timeframe_intervals = run_request.get(
                "timeframe_source_request_intervals"
            )
            for field_name, value in (
                ("BOUNDARIES", timeframe_boundaries),
                ("WINDOW_YEARS", timeframe_years),
                ("EVIDENCE_LEADS", timeframe_leads),
                ("SOURCE_INTERVALS", timeframe_intervals),
            ):
                if not isinstance(value, Mapping) or set(value) != (
                    expected_timeframe_set
                ):
                    run_manifest_errors.append(
                        f"RUN_MANIFEST_TIMEFRAME_{field_name}_ROSTER_INVALID"
                    )
            compatibility_timeframe = (
                "M5" if "M5" in canonical_timeframes else canonical_timeframes[0]
            )
            if isinstance(timeframe_boundaries, Mapping) and (
                run_request.get("common_calendar_boundaries")
                != timeframe_boundaries.get(compatibility_timeframe)
            ):
                run_manifest_errors.append(
                    "RUN_MANIFEST_TIMEFRAME_COMMON_PROJECTION_MISMATCH"
                )
            if isinstance(timeframe_years, Mapping) and (
                run_request.get("window_years")
                != timeframe_years.get(compatibility_timeframe)
            ):
                run_manifest_errors.append(
                    "RUN_MANIFEST_TIMEFRAME_WINDOW_PROJECTION_MISMATCH"
                )
            if isinstance(timeframe_boundaries, Mapping):
                reference_active = None
                for timeframe in canonical_timeframes:
                    cell_windows = timeframe_boundaries.get(timeframe)
                    active_windows = (
                        {
                            partition: cell_windows.get(partition)
                            for partition in (
                                "tuning_validation",
                                "final_lockbox",
                            )
                        }
                        if isinstance(cell_windows, Mapping)
                        else None
                    )
                    if reference_active is None:
                        reference_active = active_windows
                    elif active_windows != reference_active:
                        run_manifest_errors.append(
                            "RUN_MANIFEST_TIMEFRAME_ACTIVE_WINDOWS_NOT_CONCURRENT"
                        )
                        break
        source_environment = run_manifest.get("source_environment")
        if isinstance(source_environment, Mapping):
            run_source_environment = dict(source_environment)
        if not _source_identity(run_source_environment):
            run_manifest_errors.append("RUN_MANIFEST_SOURCE_IDENTITY_MISSING")
        manifest_symbols = {
            str(value or "").strip().upper()
            for value in (request.get("symbols") or [])
            if str(value or "").strip()
        }
        manifest_timeframes = {
            str(value or "").strip().upper()
            for value in (request.get("timeframes") or [])
            if str(value or "").strip()
        }
        if manifest_symbols != set(canonical_symbols):
            run_manifest_errors.append("RUN_MANIFEST_SYMBOL_ROSTER_MISMATCH")
        if manifest_timeframes != set(canonical_timeframes):
            run_manifest_errors.append("RUN_MANIFEST_TIMEFRAME_ROSTER_MISMATCH")
    partition_windows_by_timeframe: Dict[
        str, Dict[str, Tuple[datetime, datetime]]
    ] = {}
    for timeframe in canonical_timeframes:
        timeframe_windows, partition_window_errors = _partition_windows(
            run_request,
            timeframe,
        )
        partition_windows_by_timeframe[timeframe] = timeframe_windows
        run_manifest_errors.extend(
            f"{timeframe}:{reason}" for reason in partition_window_errors
        )
    run_manifest_sha = sha256_file(run_manifest_path) if run_manifest_path.exists() else None
    run_manifest_cells: Dict[Tuple[str, str], Mapping[str, Any]] = {}
    results = run_manifest.get("results", []) if isinstance(run_manifest, Mapping) else []
    if not isinstance(results, list):
        run_manifest_errors.append("RUN_MANIFEST_RESULTS_INVALID")
    for item in results if isinstance(results, list) else []:
        if isinstance(item, Mapping):
            key = (
                str(item.get("symbol", "")).upper(),
                str(item.get("timeframe", "")).upper(),
            )
            if key in run_manifest_cells:
                run_manifest_errors.append(
                    f"RUN_MANIFEST_DUPLICATE_CELL:{key[0]}|{key[1]}"
                )
            run_manifest_cells[key] = item
        else:
            run_manifest_errors.append("RUN_MANIFEST_RESULT_NOT_OBJECT")

    entries: Dict[str, Dict[str, Any]] = {}
    activation_blocking_statuses: list[str] = []
    declared_coverage_methods: set[str] = set()
    audited_sufficiency_cells: Dict[str, Dict[str, Any]] = {}
    availability_decisions = {
        symbol: _symbol_availability_decision(
            symbol,
            canonical_timeframes,
            run_manifest_cells,
            run_manifest_errors,
        )
        for symbol in canonical_symbols
    }
    for symbol in canonical_symbols:
        entries[symbol] = {}
        audited_sufficiency_cells[symbol] = {}
        availability = availability_decisions[symbol]
        availability_status = str(availability.get("status") or "invalid")
        if availability_status == "invalid":
            activation_blocking_statuses.append("FAIL")
        sources = {
            timeframe: _source_leaf(
                native_root,
                run_id,
                symbol,
                timeframe,
                run_manifest_cells.get((symbol, timeframe)),
                run_manifest_errors,
                run_request,
                run_source_environment,
            )
            for timeframe in canonical_timeframes
        }
        m5_source = sources.get("M5")
        m5_frame = m5_source.get("frame") if isinstance(m5_source, Mapping) else None
        m5_errors = list(m5_source.get("errors", [])) if isinstance(m5_source, Mapping) else ["M5_SOURCE_MISSING"]
        d1_source = sources.get("D1")
        d1_frame = d1_source.get("frame") if isinstance(d1_source, Mapping) else None
        m5_availability_class = (
            str(m5_source.get("availability_class") or "")
            if isinstance(m5_source, Mapping)
            else ""
        )
        m5_failure = str(
            (
                (m5_source.get("provenance") or {}).get("failure_status")
                if isinstance(m5_source, Mapping)
                and isinstance(m5_source.get("provenance"), Mapping)
                else ""
            )
            or ""
        ).strip().upper()
        for timeframe in canonical_timeframes:
            source = sources[timeframe]
            provenance = source.get("provenance", {}) or {}
            frame = source.get("frame")
            source_errors = list(source.get("errors", []) or [])
            tolerance = overrides.get(symbol)
            if tolerance is None:
                instrument = provenance.get("instrument")
                if isinstance(instrument, Mapping):
                    try:
                        candidate = float(instrument.get("parity_tick_tolerance"))
                    except (TypeError, ValueError, OverflowError):
                        candidate = float("nan")
                    if math.isfinite(candidate) and candidate > 0.0:
                        tolerance = candidate
            resolution_audit = audit_native_resolution(
                frame,
                symbol=symbol,
                timeframe=timeframe,
                daily_frame=d1_frame,
                tick_tolerance=tolerance,
            ).to_dict()
            resolution_status = str(
                resolution_audit.get("status") or "PENDING"
            ).upper()
            resolution_reasons = [
                str(reason)
                for reason in resolution_audit.get("reason_codes") or []
                if str(reason)
            ]
            source_availability_class = str(
                source.get("availability_class") or ""
            )
            if source_availability_class == "unavailable" and not source_errors:
                parity = {
                    "schema_version": PARITY_SCHEMA_VERSION,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "NOT_APPLICABLE",
                    "reason_codes": ["CELL_ACQUISITION_UNAVAILABLE"],
                    "native_is_signal_authority": True,
                    "m5_role": "read_only_session_membership_audit_comparator",
                    "metrics": {},
                    "diagnostics": {},
                }
            elif (
                timeframe != "M5"
                and m5_availability_class == "unavailable"
                and not m5_errors
            ):
                parity = {
                    "schema_version": PARITY_SCHEMA_VERSION,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "NOT_APPLICABLE",
                    "reason_codes": [
                        "M5_ACQUISITION_UNAVAILABLE:"
                        f"{m5_failure or 'UNSPECIFIED'}"
                    ],
                    "native_is_signal_authority": True,
                    "m5_role": "read_only_session_membership_audit_comparator",
                    "metrics": {},
                    "diagnostics": {},
                }
            elif frame is None or m5_frame is None or m5_errors:
                parity = {
                    "schema_version": PARITY_SCHEMA_VERSION,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "PENDING",
                    "reason_codes": ["SOURCE_FRAME_UNAVAILABLE_FOR_PARITY"] + [
                        f"M5:{reason}" for reason in m5_errors
                    ],
                    "native_is_signal_authority": True,
                    "m5_role": "read_only_session_membership_audit_comparator",
                    "metrics": {},
                    "diagnostics": {},
                }
            else:
                parity = audit_native_vs_session_m5(
                    m5_frame,
                    frame,
                    symbol=symbol,
                    timeframe=timeframe,
                    tick_tolerance=tolerance,
                    thresholds=thresholds,
                )
            coverage_status, coverage_reasons, coverage = _coverage_verdict(
                provenance,
                frame,
                float(minimum_session_coverage),
                partition_windows_by_timeframe.get(timeframe, {}),
            )
            cell_status, reasons = _merge_cell_status(
                source_errors,
                parity,
                coverage_status,
                coverage_reasons,
            )
            availability_cell = (
                (availability.get("availability_cells") or {}).get(timeframe, {})
                if isinstance(availability.get("availability_cells"), Mapping)
                else {}
            )
            availability_class, availability_error = classify_native_availability_cell(
                availability_cell.get("status"),
                availability_cell.get("failure_status"),
                availability_cell.get("rows"),
            )
            final_cell_status = "failed"
            cell_universe_status = "invalid"
            if availability_class == "unavailable":
                failure = str(
                    availability_cell.get("failure_status") or ""
                ).strip().upper()
                cell_status = "FAIL"
                reasons = [f"ACQUISITION_UNAVAILABLE:{failure}"]
                if source_errors:
                    reasons = [f"SOURCE:{reason}" for reason in source_errors]
                    activation_blocking_statuses.append("FAIL")
                else:
                    final_cell_status = "excluded_no_trade"
                    cell_universe_status = "excluded_no_trade"
            elif availability_class == "success":
                # Low but valid coverage is a deterministic cell abstention.
                # Invalid coverage, source, and parity evidence still blocks
                # the entire activation rather than becoming NO_TRADE.
                if resolution_status != "PASS":
                    cell_status = "FAIL"
                    reasons = list(
                        dict.fromkeys(
                            [
                                *reasons,
                                *(
                                    f"RESOLUTION:{reason}"
                                    for reason in resolution_reasons
                                ),
                            ]
                        )
                    )
                technical_coverage_status = (
                    "FAIL"
                    if coverage_status == "ERROR"
                    else ("PENDING" if coverage_status == "PENDING" else "PASS")
                )
                technical_coverage_reasons = (
                    coverage_reasons
                    if coverage_status in {"ERROR", "PENDING"}
                    else []
                )
                technical_status, _technical_reasons = _merge_cell_status(
                    source_errors,
                    parity,
                    technical_coverage_status,
                    technical_coverage_reasons,
                )
                if technical_status in {"FAIL", "PENDING"}:
                    activation_blocking_statuses.append(technical_status)
                    final_cell_status = (
                        "failed" if technical_status == "FAIL" else "pending"
                    )
                elif coverage_status == "FAIL" or resolution_status != "PASS":
                    final_cell_status = "excluded_no_trade"
                    cell_universe_status = "excluded_no_trade"
                elif cell_status == "PASS":
                    final_cell_status = "active"
                    cell_universe_status = "eligible"
                else:
                    activation_blocking_statuses.append("FAIL")
            else:
                cell_status = "FAIL"
                reasons = [
                    "SOURCE_AVAILABILITY_INVALID:"
                    f"{availability_error or 'unknown'}"
                ]
                activation_blocking_statuses.append("FAIL")
            parity = dict(parity)
            parity.update(
                {
                    "activation_id": activation_id,
                    "run_id": run_id,
                    "cell_activation_status": cell_status,
                    "cell_reason_codes": reasons,
                    "expected_session_coverage_by_partition": coverage,
                    "minimum_session_coverage": float(minimum_session_coverage),
                    "source_bars_sha256": source.get("bars_sha256"),
                    "source_provenance_sha256": source.get("provenance_sha256"),
                    "run_manifest_relative_path": str(
                        run_manifest_path.relative_to(native_root)
                    ).replace("\\", "/"),
                    "run_manifest_sha256": run_manifest_sha,
                    "audited_at_utc": started,
                }
            )
            audited_sidecar = {
                "schema_version": SCHEMA_VERSION,
                "audited_sidecar_contract": AUDITED_SIDECAR_CONTRACT,
                "symbol": symbol,
                "timeframe": timeframe,
                "source_kind": "broker_native",
                "timezone": "UTC",
                "timestamp_semantics": "bar_open_utc",
                "bar_interval_semantics": "half_open_[bar_open,bar_close)",
                "bar_close_column": "_BAR_CLOSE_TS",
                "sha256": source.get("bars_sha256"),
                "rows": int(len(frame)) if frame is not None else 0,
                "start_ts": frame.index[0].isoformat() if frame is not None and len(frame) else None,
                "end_ts": frame.index[-1].isoformat() if frame is not None and len(frame) else None,
                "complete": cell_status == "PASS",
                "resolution_audit": resolution_audit,
                "expected_session_coverage_by_partition": coverage,
                "observed_session_bars_by_partition": provenance.get(
                    "observed_session_bars_by_partition"
                ),
                "expected_session_bars_by_partition": provenance.get(
                    "expected_session_bars_by_partition"
                ),
                "common_calendar_boundaries": (
                    (provenance.get("request") or {}).get(
                        "cell_calendar_boundaries"
                    )
                    or (provenance.get("request") or {}).get(
                        "common_calendar_boundaries"
                    )
                    if isinstance(provenance.get("request"), Mapping)
                    else None
                ),
                "cell_calendar_boundaries": (
                    (provenance.get("request") or {}).get(
                        "cell_calendar_boundaries"
                    )
                    or (provenance.get("request") or {}).get(
                        "common_calendar_boundaries"
                    )
                    if isinstance(provenance.get("request"), Mapping)
                    else None
                ),
                "timeframe_calendar_boundaries_sha256": (
                    (provenance.get("request") or {}).get(
                        "timeframe_calendar_boundaries_sha256"
                    )
                    if isinstance(provenance.get("request"), Mapping)
                    else None
                ),
                "run_id": run_id,
                "activation_id": activation_id,
                "activation_status": cell_status,
                "activation_reason_codes": reasons,
                "parity_status": parity.get("status"),
                "source_provenance_relative_path": str(
                    Path(source["provenance_path"]).resolve().relative_to(native_root)
                ).replace("\\", "/"),
                "source_provenance_sha256": source.get("provenance_sha256"),
                "run_manifest_relative_path": str(
                    run_manifest_path.relative_to(native_root)
                ).replace("\\", "/"),
                "run_manifest_sha256": run_manifest_sha,
                "audited_at_utc": started,
            }
            parity_path, sidecar_path = _write_audit_leaf(
                audit_root,
                symbol,
                timeframe,
                parity,
                audited_sidecar,
            )
            parity_sha = sha256_file(parity_path)
            sidecar_sha = sha256_file(sidecar_path)
            coverage_details = provenance.get(
                "expected_session_coverage_details_by_partition"
            )
            coverage_method = (
                str(coverage_details.get("method") or "")
                if isinstance(coverage_details, Mapping)
                else ""
            )
            if coverage_method:
                declared_coverage_methods.add(coverage_method)
            if availability_class == "unavailable":
                source_validation_status = (
                    "ACQUISITION_UNAVAILABLE" if not source_errors else "FAIL"
                )
                audited_parity_status = "NOT_APPLICABLE"
                audited_coverage_status = "NOT_APPLICABLE"
                audited_coverage_reasons: list[str] = []
            else:
                source_validation_status = "PASS" if not source_errors else "FAIL"
                audited_parity_status = str(parity.get("status") or "PENDING").upper()
                audited_coverage_status = (
                    "INSUFFICIENT" if coverage_status == "FAIL" else coverage_status
                )
                audited_coverage_reasons = list(coverage_reasons)
            audited_sufficiency_cells[symbol][timeframe] = {
                "source_validation_status": source_validation_status,
                "parity_status": audited_parity_status,
                "coverage_status": audited_coverage_status,
                "coverage_reason_codes": audited_coverage_reasons,
                "resolution_status": resolution_status,
                "resolution_reason_codes": resolution_reasons,
                "expected_session_coverage_by_partition": coverage,
                "observed_session_bars_by_partition": provenance.get(
                    "observed_session_bars_by_partition"
                ),
                "expected_session_bars_by_partition": provenance.get(
                    "expected_session_bars_by_partition"
                ),
                "coverage_method": coverage_method,
                "source_provenance_sha256": source.get("provenance_sha256"),
                "audited_sidecar_sha256": sidecar_sha,
                "parity_sha256": parity_sha,
            }
            entries[symbol][timeframe] = {
                "status": final_cell_status,
                "audit_status": cell_status,
                "universe_eligibility_status": cell_universe_status,
                "symbol": symbol,
                "timeframe": timeframe,
                "run_id": run_id,
                "bars_relative_path": str(
                    Path(source["bars_path"]).resolve().relative_to(native_root)
                ).replace("\\", "/"),
                "bars_sha256": source.get("bars_sha256"),
                "run_manifest_sha256": run_manifest_sha,
                "sidecar_relative_path": str(sidecar_path.relative_to(native_root)).replace("\\", "/"),
                "sidecar_sha256": sidecar_sha,
                "parity_relative_path": str(parity_path.relative_to(native_root)).replace("\\", "/"),
                "parity_sha256": parity_sha,
                "coverage_method": coverage_method,
                "resolution_status": resolution_status,
                "resolution_reason_codes": resolution_reasons,
                "expected_session_coverage_by_partition": coverage,
                "observed_session_bars_by_partition": provenance.get(
                    "observed_session_bars_by_partition"
                ),
                "expected_session_bars_by_partition": provenance.get(
                    "expected_session_bars_by_partition"
                ),
                "reason_codes": reasons,
            }

    cell_statuses = {
        symbol: {
            timeframe: str(entries[symbol][timeframe].get("status") or "")
            for timeframe in canonical_timeframes
        }
        for symbol in canonical_symbols
    }
    active_timeframes_by_symbol = {
        symbol: [
            timeframe
            for timeframe in canonical_timeframes
            if cell_statuses[symbol][timeframe] == "active"
        ]
        for symbol in canonical_symbols
    }
    excluded_timeframes_by_symbol = {
        symbol: [
            timeframe
            for timeframe in canonical_timeframes
            if cell_statuses[symbol][timeframe] == "excluded_no_trade"
        ]
        for symbol in canonical_symbols
    }
    symbols_with_active_cells = sorted(
        symbol for symbol in canonical_symbols if active_timeframes_by_symbol[symbol]
    )
    fully_excluded_symbols = sorted(
        symbol
        for symbol in canonical_symbols
        if len(excluded_timeframes_by_symbol[symbol]) == len(canonical_timeframes)
    )
    excluded_symbols: Dict[str, Dict[str, Any]] = {}
    for symbol in fully_excluded_symbols:
        availability = availability_decisions[symbol]
        availability_cells = dict(availability.get("availability_cells") or {})
        acquisition_reason_codes: list[str] = []
        coverage_reason_codes: list[str] = []
        for timeframe in canonical_timeframes:
            acquisition = availability_cells.get(timeframe) or {}
            classification, _coherence_error = classify_native_availability_cell(
                acquisition.get("status"),
                acquisition.get("failure_status"),
                acquisition.get("rows"),
            )
            if classification == "unavailable":
                acquisition_reason_codes.append(
                    f"{timeframe}:"
                    f"{str(acquisition.get('failure_status') or '').upper()}"
                )
            else:
                coverage_reason_codes.extend(
                    f"{timeframe}:{reason}"
                    for reason in entries[symbol][timeframe].get("reason_codes") or []
                    if str(reason).startswith(("SESSION_COVERAGE_", "RESOLUTION:"))
                )
        excluded_symbols[symbol] = {
            "status": "excluded_no_trade",
            "decision_basis": "source_availability_only",
            "reason_codes": list(
                dict.fromkeys(
                    [*acquisition_reason_codes, *coverage_reason_codes]
                )
            ),
            "availability_cells": availability_cells,
            "audited_sufficiency_cells": dict(
                audited_sufficiency_cells.get(symbol) or {}
            ),
        }

    if len(declared_coverage_methods) > 1:
        activation_blocking_statuses.append("FAIL")
    activation_coverage_method = (
        next(iter(declared_coverage_methods))
        if len(declared_coverage_methods) == 1
        else NATIVE_SESSION_COVERAGE_METHOD
    )
    if "FAIL" in activation_blocking_statuses:
        manifest_status = "failed"
    elif "PENDING" in activation_blocking_statuses:
        manifest_status = "pending"
    else:
        manifest_status = "active"
    universe_eligibility = {
        "schema_version": UNIVERSE_ELIGIBILITY_SCHEMA,
        "decision_basis": "source_availability_only",
        "performance_inputs_used": False,
        "minimum_session_coverage": NATIVE_MINIMUM_SESSION_COVERAGE,
        "session_coverage_method": activation_coverage_method,
        "session_coverage_partitions": list(NATIVE_EVIDENCE_PARTITIONS),
        "requested_symbols": list(canonical_symbols),
        "required_timeframes": list(canonical_timeframes),
        "cell_statuses": cell_statuses,
        "active_timeframes_by_symbol": active_timeframes_by_symbol,
        "excluded_timeframes_by_symbol": excluded_timeframes_by_symbol,
        "symbols_with_active_cells": symbols_with_active_cells,
        "fully_excluded_symbols": fully_excluded_symbols,
        # Compatibility projections. These are authenticated against the
        # exact cell grid by the resolver and are never independently trusted.
        "eligible_symbols": symbols_with_active_cells,
        "excluded_symbols": excluded_symbols,
        "invalid_symbols": {
            symbol: dict(record)
            for symbol, record in sorted(availability_decisions.items())
            if str(record.get("status") or "") == "invalid"
        },
    }
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "run_manifest_relative_path": str(run_manifest_path.relative_to(native_root)).replace(
            "\\", "/"
        ),
        "run_manifest_sha256": run_manifest_sha,
        "status": manifest_status,
        "created_at_utc": started,
        "native_is_signal_authority": True,
        "silent_fallback_allowed": False,
        "previous_active_pointer_sha256": previous_pointer_sha,
        "minimum_session_coverage": float(minimum_session_coverage),
        "parity_thresholds": {
            "min_membership_ratio": thresholds.min_membership_ratio,
            "max_ohlc_mismatch_ratio": thresholds.max_ohlc_mismatch_ratio,
            "max_unassigned_m5_ratio": thresholds.max_unassigned_m5_ratio,
            "min_compared_native_bars": thresholds.min_compared_native_bars,
        },
        "universe_eligibility": universe_eligibility,
        "entries": entries,
        "counts": {
            "active": sum(
                str(entry.get("status") or "") == "active"
                for symbol_entries in entries.values()
                for entry in symbol_entries.values()
            ),
            "excluded_no_trade": sum(
                str(entry.get("status") or "") == "excluded_no_trade"
                for symbol_entries in entries.values()
                for entry in symbol_entries.values()
            ),
            "pending": sum(
                str(entry.get("status") or "") == "pending"
                for symbol_entries in entries.values()
                for entry in symbol_entries.values()
            ),
            "failed": sum(
                str(entry.get("status") or "") == "failed"
                for symbol_entries in entries.values()
                for entry in symbol_entries.values()
            ),
            "total": len(canonical_symbols) * len(canonical_timeframes),
            "eligible_symbols": len(symbols_with_active_cells),
            "excluded_symbols": len(excluded_symbols),
            "symbols_with_active_cells": len(symbols_with_active_cells),
            "fully_excluded_symbols": len(fully_excluded_symbols),
        },
    }
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    _atomic_create_json(manifest_path, manifest)
    manifest_sha = sha256_file(manifest_path)
    pointer_updated = False
    if manifest_status == "active":
        pointer = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": str(manifest_path.relative_to(native_root)).replace("\\", "/"),
            "manifest_sha256": manifest_sha,
            "activated_at_utc": started,
        }
        _atomic_replace_json(native_root / "ACTIVE.json", pointer)
        pointer_updated = True
    return {
        **manifest,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "active_pointer_updated": pointer_updated,
    }


def _parse_tolerances(values: Sequence[str]) -> Dict[str, float]:
    result: Dict[str, float] = {}
    for item in values:
        if "=" not in item:
            raise ValueError("--tick-tolerance values must be SYMBOL=VALUE")
        symbol, raw = item.split("=", 1)
        value = float(raw)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(f"invalid tick tolerance: {item}")
        result[symbol.strip().upper()] = value
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Offline native parity/availability audit and atomic eligible-plus-NO_TRADE "
            "universe activation"
        )
    )
    parser.add_argument("--native-root", default="data/native")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--timeframes", nargs="+", default=list(DEFAULT_TIMEFRAMES))
    parser.add_argument("--activation-id")
    parser.add_argument(
        "--minimum-session-coverage",
        type=float,
        default=NATIVE_MINIMUM_SESSION_COVERAGE,
        help="pinned production policy; values other than 0.95 are rejected",
    )
    parser.add_argument("--min-membership-ratio", type=float, default=0.99)
    parser.add_argument("--max-ohlc-mismatch-ratio", type=float, default=0.005)
    parser.add_argument("--max-unassigned-m5-ratio", type=float, default=0.01)
    parser.add_argument("--min-compared-native-bars", type=int, default=20)
    parser.add_argument("--tick-tolerance", action="append", default=[])
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    native_root = _safe_native_root(Path(args.native_root))
    run_root = native_root / "runs" / args.run_id
    run_manifest_path = run_root / "run_manifest.json"
    run_manifest = _json(run_manifest_path) if run_manifest_path.exists() else {}
    request = run_manifest.get("request") if isinstance(run_manifest.get("request"), Mapping) else {}
    manifest_symbols = [
        str(value).strip().upper()
        for value in (request.get("symbols") or [])
        if str(value).strip()
    ]
    if not manifest_symbols:
        manifest_symbols = sorted({
            str(item.get("symbol") or "").strip().upper()
            for item in (run_manifest.get("results") or [])
            if isinstance(item, Mapping) and str(item.get("symbol") or "").strip()
        })
    symbols = [str(value).strip().upper() for value in (args.symbols or manifest_symbols)]
    if args.symbols and manifest_symbols and set(symbols) != set(manifest_symbols):
        raise ValueError(
            "--symbols must equal the immutable acquisition run's requested symbol roster; "
            "activation may not hide failed or short-history symbols"
        )
    manifest_timeframes = [
        str(value).strip().upper()
        for value in (request.get("timeframes") or [])
        if str(value).strip()
    ]
    if manifest_timeframes and set(args.timeframes) != set(manifest_timeframes):
        raise ValueError(
            "--timeframes must equal the immutable acquisition run's requested timeframe roster"
        )
    plan = {
        "mode": "execute" if args.execute else "plan_only",
        "native_root": str(native_root),
        "run_id": args.run_id,
        "symbols": symbols,
        "timeframes": args.timeframes,
        "activation_id": args.activation_id,
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not args.execute:
        return 0
    result = audit_and_activate(
        native_root,
        run_id=args.run_id,
        symbols=symbols,
        timeframes=args.timeframes,
        activation_id=args.activation_id,
        thresholds=ParityThresholds(
            min_membership_ratio=args.min_membership_ratio,
            max_ohlc_mismatch_ratio=args.max_ohlc_mismatch_ratio,
            max_unassigned_m5_ratio=args.max_unassigned_m5_ratio,
            min_compared_native_bars=args.min_compared_native_bars,
        ),
        minimum_session_coverage=args.minimum_session_coverage,
        tick_tolerance_overrides=_parse_tolerances(args.tick_tolerance),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "active" else (3 if result["status"] == "pending" else 4)


if __name__ == "__main__":
    raise SystemExit(main())
