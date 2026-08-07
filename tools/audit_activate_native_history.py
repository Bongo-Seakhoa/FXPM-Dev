"""Audit and atomically activate one immutable native-history run.

This command is offline: it never imports or connects to MetaTrader.  It reads
an acquisition run and authenticates every requested cell independently before
model search.  Activation v4 never emits a trading verdict: authentic nonempty
history becomes an evidence candidate, proven source nonexistence remains a
separate terminal data state, and technical faults stay technical.  Statistical
sufficiency and NO_TRADE decisions belong to downstream evidence authorities.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import shutil
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_native_activation import (  # noqa: E402
    ACTIVATION_MANIFEST_SCHEMA,
    ACTIVE_POINTER_FIELDS,
    ACTIVE_POINTER_SCHEMA,
    LEGACY_NATIVE_SESSION_COVERAGE_METHOD,
    NATIVE_AUTHENTICITY_DECISION_BASIS,
    NATIVE_BARS_SCHEMA,
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_EVIDENCE_PARTITIONS,
    NATIVE_MINIMUM_SESSION_COVERAGE,
    NATIVE_PENDING,
    NATIVE_SESSION_COVERAGE_METHOD,
    NATIVE_SOURCE_UNAVAILABLE,
    NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
    NATIVE_TECHNICAL_ERROR,
    SUPPORTED_NATIVE_SESSION_COVERAGE_METHODS,
    TERMINAL_PROMOTION_RECEIPT_SCHEMA,
    TERMINAL_PROMOTION_TRANSITION,
    UNIVERSE_AUTHENTICITY_SCHEMA,
    UNIVERSE_ELIGIBILITY_SCHEMA,
    classify_native_availability_cell,
    resolve_active_native_universe,
    scope_native_pair_window_states,
    sha256_file,
    validate_native_session_coverage_diagnostics,
    validate_terminal_promotion_view,
)
from pm_native_consensus import (  # noqa: E402
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
    canonical_sha256,
    expected_reference_timeframes,
    validate_consensus_envelope,
)
from pm_native_intervals import native_source_interval  # noqa: E402
from pm_native_parity import (  # noqa: E402
    CROSS_RESOLUTION_PARITY_SCHEMA_VERSION,
    PARITY_SCHEMA_VERSION,
    ParityThresholds,
    audit_native_cross_resolution,
    audit_native_vs_session_m5,
    normalize_ohlc_frame,
)
from pm_native_resolution import audit_native_resolution  # noqa: E402
from pm_native_csv_validation import (  # noqa: E402
    NativeCsvValidationError,
    scan_native_csv_stream,
)
from pm_native_request_contract import (  # noqa: E402
    COMMON_REQUEST_KEYS,
    PRODUCTION_SYMBOL_ORDER,
    NativeRequestContractError,
    build_v5_cell_validation_context,
    validate_v5_cell_status,
    validate_v5_terminal_technical_cell,
)
from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION  # noqa: E402
from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    DEFAULT_TIMEFRAMES,
    MANIFEST_SCHEMA_VERSION,
    PAGINATION_CONTRACT_VERSION,
    SCHEMA_VERSION,
    LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
    TWO_REFERENCE_CONSENSUS_POLICY,
    ceil_utc_boundary,
    expected_session_coverage_by_partition,
    floor_utc_boundary,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
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
_V4_REQUEST_CONTRACT_FIELDS: Tuple[str, ...] = (
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_only_prefix_intervals",
)
_V5_REQUEST_CONTRACT_FIELDS: Tuple[str, ...] = (
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_consumer_map",
    "timeframe_audit_windows",
    "timeframe_audit_reference_intervals",
    "timeframe_audit_only_intervals",
    "timeframe_consensus_policy",
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


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _atomic_replace_bytes(path: Path, payload: bytes) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.staging")
    try:
        with temp.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        return path
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def _atomic_replace_bytes_with_retry(
    path: Path,
    payload: bytes,
    *,
    attempts: int = 8,
    initial_delay_seconds: float = 0.025,
) -> Path:
    """Retry a bounded atomic byte restore across transient Windows share locks."""

    last_error: Optional[OSError] = None
    for attempt in range(max(1, int(attempts))):
        try:
            return _atomic_replace_bytes(path, payload)
        except OSError as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise
            time.sleep(initial_delay_seconds * (2**attempt))
    assert last_error is not None
    raise last_error


def _unlink_with_retry(
    path: Path,
    *,
    attempts: int = 8,
    initial_delay_seconds: float = 0.025,
) -> None:
    """Remove one transaction marker across bounded transient share locks."""

    for attempt in range(max(1, int(attempts))):
        try:
            Path(path).unlink()
            return
        except FileNotFoundError:
            return
        except OSError:
            if attempt + 1 >= attempts:
                raise
            time.sleep(initial_delay_seconds * (2**attempt))


@contextmanager
def _active_pointer_lock(
    native_root: Path,
    *,
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """Serialize ACTIVE publication within this activation tool."""

    lock_path = Path(native_root) / ".ACTIVE.json.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("timed out waiting for native ACTIVE lock") from exc
                    time.sleep(0.025)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("timed out waiting for native ACTIVE lock") from exc
                    time.sleep(0.025)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _replace_directory_with_retry(
    staging: Path,
    final_dir: Path,
    *,
    attempts: int = 8,
    initial_delay_seconds: float = 0.025,
    replace_fn: Any = None,
    sleep_fn: Any = None,
) -> None:
    """Publish one immutable directory despite transient Windows share locks.

    OneDrive, antivirus, and indexers can briefly open a newly fsynced staging
    child without delete sharing.  Windows then reports ``PermissionError``
    for the parent-directory rename even though neither this process nor a
    competing audit owns the destination.  Retrying the same atomic rename is
    safe; copying files as a fallback would not be atomic and is forbidden.

    A destination appearing at any point is an immutable-record collision,
    not a retryable sharing violation.  Exhaustion preserves the original OS
    error and lets the caller remove only its private staging directory.
    """

    staging = Path(staging)
    final_dir = Path(final_dir)
    if attempts < 1:
        raise ValueError("attempts must be at least one")
    if initial_delay_seconds < 0.0:
        raise ValueError("initial_delay_seconds cannot be negative")
    replace = replace_fn or os.replace
    sleep = sleep_fn or time.sleep
    for attempt in range(attempts):
        if final_dir.exists():
            raise FileExistsError(f"immutable audit leaf already exists: {final_dir}")
        try:
            replace(staging, final_dir)
            return
        except PermissionError:
            # A concurrent publisher must never be overwritten, even if
            # Windows surfaced its destination collision as access denied.
            if final_dir.exists():
                raise FileExistsError(
                    f"immutable audit leaf already exists: {final_dir}"
                ) from None
            if attempt + 1 >= attempts or not staging.is_dir():
                raise
            sleep(initial_delay_seconds * (2**attempt))


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
        _replace_directory_with_retry(staging, final_dir)
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
        request_map_version = str(
            value.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version not in {
            LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4,
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
        }:
            return None
        fields = (*fields, *_V3_REQUEST_CONTRACT_FIELDS)
        if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
            fields = (*fields, *_V4_REQUEST_CONTRACT_FIELDS)
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            fields = (*fields, *_V5_REQUEST_CONTRACT_FIELDS)
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
        request_map_version = str(
            contract.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
            request_map_payload.update(
                {
                    "timeframe_strategy_evidence_intervals": contract.get(
                        "timeframe_strategy_evidence_intervals"
                    ),
                    "timeframe_audit_reference_map": contract.get(
                        "timeframe_audit_reference_map"
                    ),
                    "timeframe_audit_only_prefix_intervals": contract.get(
                        "timeframe_audit_only_prefix_intervals"
                    ),
                }
            )
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            request_map_payload.update(
                {
                    field_name: contract.get(field_name)
                    for field_name in _V5_REQUEST_CONTRACT_FIELDS
                }
            )
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
    expected_strategy_start: Optional[pd.Timestamp] = None
    request = provenance.get("request")
    pagination = provenance.get("pagination")
    if not isinstance(request, Mapping):
        return ["SOURCE_REQUEST_MISSING"]
    if not isinstance(pagination, Mapping):
        return ["SOURCE_PAGINATION_MISSING"]
    seconds = TIMEFRAME_SECONDS[timeframe]
    try:
        common_start = _utc_timestamp(
            manifest_request.get("common_requested_start_utc")
        )
        common_as_of = _utc_timestamp(manifest_request.get("common_as_of_utc"))
        fetch_start = _utc_timestamp(request.get("native_fetch_start_utc"))
        fetch_end = _utc_timestamp(request.get("native_fetch_end_exclusive_utc"))
    except Exception:
        return ["SOURCE_FETCH_BOUNDARY_INVALID"]
    if (
        str(manifest_request.get("window_contract_version") or "")
        == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    ):
        timeframe_intervals = manifest_request.get("timeframe_source_request_intervals")
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
        strategy_intervals = manifest_request.get(
            "timeframe_strategy_evidence_intervals"
        )
        raw_strategy = (
            strategy_intervals.get(timeframe)
            if isinstance(strategy_intervals, Mapping)
            else None
        )
        try:
            expected_strategy_start = _utc_timestamp(
                raw_strategy.get("start_utc")
                if isinstance(raw_strategy, Mapping)
                else None
            )
        except Exception:
            errors.append("SOURCE_TIMEFRAME_STRATEGY_INTERVAL_INVALID")
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
    if (
        fetch_start != expected_start
        or fetch_end != expected_end
        or fetch_end <= fetch_start
    ):
        errors.append("SOURCE_FETCH_BOUNDARY_MISMATCH")
    if str(request.get("alignment_mode") or "") != expected_alignment:
        errors.append("SOURCE_FETCH_ALIGNMENT_MODE_MISMATCH")
    if (
        _strict_int(request.get("successor_sentinel_fetch_slack_seconds"))
        != expected_slack
    ):
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

    is_v5 = (
        str(manifest_request.get("timeframe_request_map_schema_version") or "")
        == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    )
    expected_intervals: list[Tuple[pd.Timestamp, pd.Timestamp]] = []
    if is_v5:
        cursor_end = expected_end
        while cursor_end > expected_start:
            page_start = max(
                expected_start,
                cursor_end - pd.Timedelta(seconds=expected_page_span_seconds),
            )
            expected_intervals.append((page_start, cursor_end))
            cursor_end = page_start
        expected_intervals.reverse()
    else:
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
    pages_planned = _strict_int(pagination.get("pages_planned"))
    pages_with_data = _strict_int(pagination.get("pages_with_data"))
    attempts_total = _strict_int(pagination.get("attempts_total"))
    copy_attempts_total = _strict_int(pagination.get("copy_rates_range_attempts_total"))
    page_attempts = pagination.get("page_attempts")
    retries_per_page = _strict_int(pagination.get("retries_per_page"))
    manifest_retries = _strict_int(manifest_request.get("retries"))
    empty_pages = pagination.get("empty_pages")
    failed_pages = pagination.get("failed_pages")
    terminal_prefix_pages = pagination.get("terminal_prefix_unresolved_pages", [])
    stop_reason = pagination.get("stop_reason")
    terminal_prefix_stopped = stop_reason == "terminal_prefix_unresolved"
    technical_prefix_stopped = stop_reason == "technical_prefix_unresolved"
    prefix_stopped = terminal_prefix_stopped or technical_prefix_stopped
    if stop_reason not in {
        None,
        "terminal_prefix_unresolved",
        "technical_prefix_unresolved",
    }:
        errors.append("SOURCE_PAGINATION_STOP_REASON_INVALID")
    if is_v5 and pages_planned != len(expected_intervals):
        errors.append("SOURCE_PAGINATION_PLANNED_COUNT_MISMATCH")
    if not isinstance(page_attempts, list) or not page_attempts:
        errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_LEDGER_INVALID")
        page_attempts = []
    expected_attempt_intervals = (
        expected_intervals[-len(page_attempts) :] if page_attempts else []
    )
    expected_attempt_keys = {
        (int(start.value), int(end.value)) for start, end in expected_attempt_intervals
    }
    if not prefix_stopped and len(page_attempts) != len(expected_intervals):
        errors.append("SOURCE_PAGINATION_PAGE_COUNT_MISMATCH")
    if pages_requested != len(page_attempts):
        errors.append("SOURCE_PAGINATION_PAGE_COUNT_MISMATCH")
    if pages_with_data is None or not 0 <= pages_with_data <= len(expected_intervals):
        errors.append("SOURCE_PAGINATION_DATA_PAGE_COUNT_INVALID")
    if not all(
        isinstance(value, list)
        for value in (empty_pages, failed_pages, terminal_prefix_pages)
    ):
        errors.append("SOURCE_PAGINATION_PAGE_LEDGER_INVALID")
        empty_pages = []
        failed_pages = []
        terminal_prefix_pages = []
    if (
        pages_requested is not None
        and pages_with_data is not None
        and pages_with_data
        + len(empty_pages)
        + len(failed_pages)
        + len(terminal_prefix_pages)
        != pages_requested
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

    require_copy_attempt_ledger = is_v5
    if require_copy_attempt_ledger:
        if str(pagination.get("contract_version") or "") != (
            PAGINATION_CONTRACT_VERSION
        ):
            errors.append("SOURCE_PAGINATION_CONTRACT_VERSION_INVALID")
        ledger_keys: set[Tuple[int, int]] = set()
        ledger_outer_total = 0
        ledger_copy_total = 0
        for page in page_attempts:
            if not isinstance(page, Mapping):
                errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_RECORD_INVALID")
                continue
            try:
                page_start = _utc_timestamp(page.get("start_utc"))
                page_end = _utc_timestamp(page.get("end_exclusive_utc"))
                key = (int(page_start.value), int(page_end.value))
            except Exception:
                errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_BOUNDARY_INVALID")
                continue
            if key not in expected_attempt_keys or key in ledger_keys:
                errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_INTERVAL_INVALID")
            ledger_keys.add(key)
            expected_sequence = len(page_attempts) - len(ledger_keys) + 1
            if _strict_int(page.get("request_sequence")) != expected_sequence:
                errors.append("SOURCE_PAGINATION_REQUEST_SEQUENCE_INVALID")
            outer_attempts = _strict_int(page.get("attempts"))
            copy_attempts = _strict_int(page.get("copy_rates_range_attempts"))
            if (
                outer_attempts is None
                or outer_attempts <= 0
                or copy_attempts is None
                or copy_attempts < outer_attempts
            ):
                errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_COUNT_INVALID")
                continue
            ledger_outer_total += outer_attempts
            ledger_copy_total += copy_attempts
        if ledger_keys != expected_attempt_keys:
            errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_COVERAGE_INVALID")
        if attempts_total != ledger_outer_total:
            errors.append("SOURCE_PAGINATION_OUTER_ATTEMPT_TOTAL_MISMATCH")
        if copy_attempts_total != ledger_copy_total:
            errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_TOTAL_MISMATCH")
        if copy_attempts_total is None or copy_attempts_total < int(
            attempts_total or 0
        ):
            errors.append("SOURCE_PAGINATION_COPY_ATTEMPT_TOTAL_INVALID")

    observed_page_keys: set[Tuple[int, int]] = set()
    for page in [*empty_pages, *failed_pages, *terminal_prefix_pages]:
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

    technical_prefix_valid = False
    if terminal_prefix_stopped:
        if len(terminal_prefix_pages) != 1 or not expected_attempt_intervals:
            errors.append("SOURCE_TERMINAL_PREFIX_EVIDENCE_INVALID")
        else:
            prefix_page = terminal_prefix_pages[0]
            try:
                prefix_key = (
                    int(_utc_timestamp(prefix_page.get("start_utc")).value),
                    int(_utc_timestamp(prefix_page.get("end_exclusive_utc")).value),
                )
                prefix_end = _utc_timestamp(prefix_page.get("end_exclusive_utc"))
            except Exception:
                prefix_key = None
                prefix_end = None
            expected_oldest = expected_attempt_intervals[0]
            expected_oldest_key = (
                int(expected_oldest[0].value),
                int(expected_oldest[1].value),
            )
            if prefix_key != expected_oldest_key:
                errors.append("SOURCE_TERMINAL_PREFIX_PAGE_NOT_OLDEST")
            if (
                expected_strategy_start is None
                or prefix_end is None
                or prefix_end > expected_strategy_start
            ) and not (
                availability_class == "success"
                and failure_status == "boundary_history_shortfall"
            ):
                # The released V5 cell validator consumes an authenticated
                # leading terminal prefix as failed/boundary-shortfall
                # evidence.  Its exact page, artifact, source, and provenance
                # gates run again below.  Missing requested history is then a
                # downstream horizon/power fact, not acquisition corruption.
                errors.append("SOURCE_TERMINAL_PREFIX_INTERSECTS_STRATEGY")
            prefix_before_attempt = pagination.get("unrequested_prefix")
            if expected_oldest[0] == expected_start:
                if prefix_before_attempt is not None:
                    errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
            else:
                try:
                    declared_start = _utc_timestamp(
                        prefix_before_attempt.get("start_utc")
                    )
                    declared_end = _utc_timestamp(
                        prefix_before_attempt.get("end_exclusive_utc")
                    )
                    declared_reason = prefix_before_attempt.get("reason")
                except Exception:
                    errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
                else:
                    if (
                        declared_start != expected_start
                        or declared_end != expected_oldest[0]
                        or declared_reason != "terminal_prefix_unresolved"
                    ):
                        errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
    elif technical_prefix_stopped:
        prefix_error_count = len(errors)
        technical_pages = [*empty_pages, *failed_pages]
        if (
            len(technical_pages) != 1
            or not expected_attempt_intervals
            or terminal_prefix_pages
        ):
            errors.append("SOURCE_TECHNICAL_PREFIX_EVIDENCE_INVALID")
        else:
            technical_page = technical_pages[0]
            try:
                technical_key = (
                    int(_utc_timestamp(technical_page.get("start_utc")).value),
                    int(_utc_timestamp(technical_page.get("end_exclusive_utc")).value),
                )
                technical_end = _utc_timestamp(technical_page.get("end_exclusive_utc"))
            except Exception:
                technical_key = None
                technical_end = None
            expected_oldest = expected_attempt_intervals[0]
            expected_oldest_key = (
                int(expected_oldest[0].value),
                int(expected_oldest[1].value),
            )
            if technical_key != expected_oldest_key:
                errors.append("SOURCE_TECHNICAL_PREFIX_PAGE_NOT_OLDEST")
            if (
                expected_strategy_start is None
                or technical_end is None
                or technical_end > expected_strategy_start
            ):
                errors.append("SOURCE_TECHNICAL_PREFIX_INTERSECTS_STRATEGY")
            prefix_before_attempt = pagination.get("unrequested_prefix")
            if expected_oldest[0] == expected_start:
                if prefix_before_attempt is not None:
                    errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
            else:
                try:
                    declared_start = _utc_timestamp(
                        prefix_before_attempt.get("start_utc")
                    )
                    declared_end = _utc_timestamp(
                        prefix_before_attempt.get("end_exclusive_utc")
                    )
                    declared_reason = prefix_before_attempt.get("reason")
                except Exception:
                    errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
                else:
                    if (
                        declared_start != expected_start
                        or declared_end != expected_oldest[0]
                        or declared_reason != "technical_prefix_unresolved"
                    ):
                        errors.append("SOURCE_UNREQUESTED_PREFIX_INVALID")
        technical_prefix_valid = len(errors) == prefix_error_count
    else:
        if terminal_prefix_pages:
            errors.append("SOURCE_TERMINAL_PREFIX_WITHOUT_STOP")
        if pagination.get("unrequested_prefix") is not None:
            errors.append("SOURCE_UNREQUESTED_PREFIX_WITHOUT_STOP")

    if failed_pages and not technical_prefix_valid:
        errors.append("SOURCE_PAGINATION_FAILED_PAGE_PRESENT")

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


def _boundary_shortfall_integrity_errors(
    provenance: Mapping[str, Any],
    frame: Optional[pd.DataFrame],
) -> list[str]:
    """Recompute that an auditable shortfall has no internal page defect."""

    if frame is None or frame.empty:
        return ["SOURCE_BOUNDARY_SHORTFALL_FRAME_MISSING"]
    pagination = provenance.get("pagination")
    coverage = provenance.get("coverage")
    request = provenance.get("request")
    if not all(isinstance(value, Mapping) for value in (pagination, coverage, request)):
        return ["SOURCE_BOUNDARY_SHORTFALL_DIAGNOSTICS_MISSING"]
    assert isinstance(pagination, Mapping)
    assert isinstance(coverage, Mapping)
    assert isinstance(request, Mapping)
    errors: list[str] = []
    failed_pages = pagination.get("failed_pages")
    empty_pages = pagination.get("empty_pages")
    truncated_pages = pagination.get("truncated_pages")
    terminal_prefix_pages = pagination.get("terminal_prefix_unresolved_pages", [])
    if not all(
        isinstance(value, list)
        for value in (
            failed_pages,
            empty_pages,
            truncated_pages,
            terminal_prefix_pages,
        )
    ):
        return ["SOURCE_BOUNDARY_SHORTFALL_PAGE_LEDGER_INVALID"]
    assert isinstance(failed_pages, list)
    assert isinstance(empty_pages, list)
    assert isinstance(truncated_pages, list)
    assert isinstance(terminal_prefix_pages, list)
    actual_first = pd.Timestamp(frame.index[0]).tz_convert("UTC")
    actual_last = pd.Timestamp(frame.index[-1]).tz_convert("UTC")
    stop_reason = pagination.get("stop_reason")
    technical_prefix_page: Optional[Mapping[str, Any]] = None
    if stop_reason == "technical_prefix_unresolved":
        candidates = [*empty_pages, *failed_pages]
        strategy_interval = request.get("strategy_evidence_interval")
        try:
            strategy_start = _utc_timestamp(
                strategy_interval.get("start_utc")
                if isinstance(strategy_interval, Mapping)
                else None
            )
            candidate_end = _utc_timestamp(candidates[0].get("end_exclusive_utc"))
        except Exception:
            strategy_start = None
            candidate_end = None
        if (
            len(candidates) == 1
            and not terminal_prefix_pages
            and strategy_start is not None
            and candidate_end is not None
            and candidate_end <= strategy_start
            and candidate_end <= actual_first
        ):
            technical_prefix_page = candidates[0]
        else:
            errors.append("SOURCE_BOUNDARY_SHORTFALL_PREFIX_STATE_INVALID")
    if failed_pages and technical_prefix_page not in failed_pages:
        errors.append("SOURCE_BOUNDARY_SHORTFALL_FAILED_PAGE_PRESENT")
    internal_empty = 0
    for page in empty_pages:
        if page is technical_prefix_page:
            continue
        if not isinstance(page, Mapping):
            errors.append("SOURCE_BOUNDARY_SHORTFALL_EMPTY_PAGE_INVALID")
            continue
        try:
            page_start = _utc_timestamp(page.get("start_utc"))
            page_end = _utc_timestamp(page.get("end_exclusive_utc"))
        except Exception:
            errors.append("SOURCE_BOUNDARY_SHORTFALL_EMPTY_PAGE_INVALID")
            continue
        if page_start >= actual_first and page_end <= actual_last:
            internal_empty += 1

    tolerance_days = _strict_finite_number(request.get("boundary_tolerance_days"))
    tolerance_seconds = max(0.0, float(tolerance_days or 0.0) * 86_400.0)
    internal_truncated = 0
    boundary_truncated = 0
    for page in truncated_pages:
        if not isinstance(page, Mapping):
            errors.append("SOURCE_BOUNDARY_SHORTFALL_TRUNCATED_PAGE_INVALID")
            internal_truncated += 1
            continue
        try:
            page_start = _utc_timestamp(page.get("start_utc"))
            page_end = _utc_timestamp(page.get("end_exclusive_utc"))
            start_delay = float(page.get("start_delay_seconds", 0.0) or 0.0)
            end_lag = float(page.get("end_lag_seconds", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            errors.append("SOURCE_BOUNDARY_SHORTFALL_TRUNCATED_PAGE_INVALID")
            internal_truncated += 1
            continue
        contains_first = page_start <= actual_first < page_end
        contains_last = page_start <= actual_last < page_end
        if (start_delay > tolerance_seconds and not contains_first) or (
            end_lag > tolerance_seconds and not contains_last
        ):
            internal_truncated += 1
        else:
            boundary_truncated += 1

    declared_counts = {
        "internal_empty_page_count": internal_empty,
        "internal_truncated_page_count": internal_truncated,
        "boundary_truncated_page_count": boundary_truncated,
        "failed_page_count": len(failed_pages),
    }
    if (
        "terminal_prefix_unresolved_pages" in pagination
        or "terminal_prefix_unresolved_page_count" in coverage
    ):
        declared_counts["terminal_prefix_unresolved_page_count"] = len(
            terminal_prefix_pages
        )
    for field_name, expected in declared_counts.items():
        if _strict_int(coverage.get(field_name)) != expected:
            errors.append(f"SOURCE_BOUNDARY_SHORTFALL_{field_name.upper()}_MISMATCH")
    unresolved_failed = bool(failed_pages and technical_prefix_page not in failed_pages)
    if internal_empty or internal_truncated or unresolved_failed:
        errors.append("SOURCE_BOUNDARY_SHORTFALL_INTERNAL_GAP_PRESENT")
    prefix_state_valid = bool(terminal_prefix_pages) == (
        stop_reason == "terminal_prefix_unresolved"
    ) and bool(technical_prefix_page) == (stop_reason == "technical_prefix_unresolved")
    if not prefix_state_valid:
        errors.append("SOURCE_BOUNDARY_SHORTFALL_PREFIX_STATE_INVALID")
    if str(coverage.get("technical_integrity_status") or "") != (
        "complete_page_ledger"
    ):
        errors.append("SOURCE_BOUNDARY_SHORTFALL_INTEGRITY_STATUS_INVALID")
    return list(dict.fromkeys(errors))


def _legacy_partial_boundary_integrity_errors(
    provenance: Mapping[str, Any],
    frame: Optional[pd.DataFrame],
) -> list[str]:
    """Forensically distinguish a V3 leading boundary from an internal hole.

    Historical V3 writers used the undifferentiated ``partial_history`` label,
    but persisted an exact page-disposition ledger.  We may upgrade that label
    only when every empty/truncated page is provably outside the observed span
    or contains its first/last bar.  This is an audit attestation; the immutable
    acquisition record itself is never rewritten.
    """

    if frame is None or frame.empty:
        return ["SOURCE_LEGACY_PARTIAL_FRAME_MISSING"]
    pagination = provenance.get("pagination")
    coverage = provenance.get("coverage")
    if not isinstance(pagination, Mapping) or not isinstance(coverage, Mapping):
        return ["SOURCE_LEGACY_PARTIAL_DIAGNOSTICS_MISSING"]
    empty_pages = pagination.get("empty_pages")
    failed_pages = pagination.get("failed_pages")
    truncated_pages = pagination.get("truncated_pages")
    if not all(
        isinstance(value, list)
        for value in (empty_pages, failed_pages, truncated_pages)
    ):
        return ["SOURCE_LEGACY_PARTIAL_PAGE_LEDGER_INVALID"]
    assert isinstance(empty_pages, list)
    assert isinstance(failed_pages, list)
    assert isinstance(truncated_pages, list)
    errors: list[str] = []
    if failed_pages:
        errors.append("SOURCE_LEGACY_PARTIAL_FAILED_PAGE_PRESENT")
    actual_first = pd.Timestamp(frame.index[0]).tz_convert("UTC")
    actual_last = pd.Timestamp(frame.index[-1]).tz_convert("UTC")
    declared_first = provenance.get("start_ts")
    declared_last = provenance.get("end_ts")
    try:
        if _utc_timestamp(declared_first) != actual_first:
            errors.append("SOURCE_LEGACY_PARTIAL_FIRST_BAR_MISMATCH")
        if _utc_timestamp(declared_last) != actual_last:
            errors.append("SOURCE_LEGACY_PARTIAL_LAST_BAR_MISMATCH")
    except Exception:
        errors.append("SOURCE_LEGACY_PARTIAL_BOUNDARY_DECLARATION_INVALID")

    internal_empty = 0
    for page in empty_pages:
        if not isinstance(page, Mapping):
            errors.append("SOURCE_LEGACY_PARTIAL_EMPTY_PAGE_INVALID")
            continue
        try:
            page_start = _utc_timestamp(page.get("start_utc"))
            page_end = _utc_timestamp(page.get("end_exclusive_utc"))
        except Exception:
            errors.append("SOURCE_LEGACY_PARTIAL_EMPTY_PAGE_INVALID")
            continue
        if page_start >= actual_first and page_end <= actual_last:
            internal_empty += 1

    internal_truncated = 0
    for page in truncated_pages:
        if not isinstance(page, Mapping):
            errors.append("SOURCE_LEGACY_PARTIAL_TRUNCATED_PAGE_INVALID")
            internal_truncated += 1
            continue
        try:
            page_start = _utc_timestamp(page.get("start_utc"))
            page_end = _utc_timestamp(page.get("end_exclusive_utc"))
            page_first = _utc_timestamp(page.get("actual_first_open_utc"))
            page_last = _utc_timestamp(page.get("actual_last_open_utc"))
        except Exception:
            errors.append("SOURCE_LEGACY_PARTIAL_TRUNCATED_PAGE_INVALID")
            internal_truncated += 1
            continue
        contains_global_first = (
            page_start <= actual_first < page_end and page_first == actual_first
        )
        contains_global_last = (
            page_start <= actual_last < page_end and page_last == actual_last
        )
        if not (contains_global_first or contains_global_last):
            internal_truncated += 1

    if _strict_int(coverage.get("internal_empty_page_count")) != internal_empty:
        errors.append("SOURCE_LEGACY_PARTIAL_INTERNAL_EMPTY_COUNT_MISMATCH")
    if _strict_int(coverage.get("truncated_page_count")) != len(truncated_pages):
        errors.append("SOURCE_LEGACY_PARTIAL_TRUNCATED_COUNT_MISMATCH")
    if str(coverage.get("status") or "") != "partial_requested_span":
        errors.append("SOURCE_LEGACY_PARTIAL_COVERAGE_STATUS_INVALID")
    if internal_empty:
        errors.append("SOURCE_LEGACY_PARTIAL_INTERNAL_EMPTY_PAGE_PRESENT")
    if internal_truncated:
        errors.append("SOURCE_LEGACY_PARTIAL_INTERNAL_TRUNCATED_PAGE_PRESENT")
    return list(dict.fromkeys(errors))


def _production_v5_source_errors(
    provenance: Mapping[str, Any],
    manifest_result: Mapping[str, Any],
    manifest_request: Mapping[str, Any],
    source_environment: Mapping[str, Any],
    bars_path: Path,
    *,
    symbol: str,
    timeframe: str,
) -> list[str]:
    if (
        manifest_request.get("timeframe_request_map_schema_version")
        != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        or not bars_path.is_file()
    ):
        return []
    common_request = {
        key: manifest_request[key]
        for key in COMMON_REQUEST_KEYS
        if key in manifest_request
    }
    retries = manifest_request.get("retries")
    try:
        context = build_v5_cell_validation_context(
            common_request,
            source_environment,
            retries,
        )
    except NativeRequestContractError as exc:
        return [f"SOURCE_PRODUCTION_CONTRACT_INVALID:{code}" for code in exc.errors]

    pagination = provenance.get("pagination")
    scan_request = common_request
    if isinstance(pagination, Mapping) and pagination.get("stop_reason") in {
        "terminal_prefix_unresolved",
        "technical_prefix_unresolved",
    }:
        attempts = pagination.get("page_attempts")
        intervals = common_request.get("timeframe_source_request_intervals")
        if (
            isinstance(attempts, list)
            and attempts
            and isinstance(attempts[0], Mapping)
            and isinstance(intervals, Mapping)
            and isinstance(intervals.get(timeframe), Mapping)
        ):
            bounded_intervals = dict(intervals)
            interval = dict(intervals[timeframe])
            interval["start_utc"] = attempts[0].get("start_utc")
            bounded_intervals[timeframe] = interval
            scan_request = {
                **common_request,
                "timeframe_source_request_intervals": bounded_intervals,
            }
    try:
        with bars_path.open("rb") as stream:
            scan = scan_native_csv_stream(
                stream,
                expected_bytes=int(bars_path.stat().st_size),
                timeframe=timeframe,
                common_request=scan_request,
                pagination=pagination,
            )
    except NativeCsvValidationError as exc:
        return [f"SOURCE_PRODUCTION_CSV_INVALID:{exc.code}"]
    except (OSError, TypeError, ValueError, OverflowError) as exc:
        return [f"SOURCE_PRODUCTION_CSV_INVALID:{type(exc).__name__}"]
    try:
        validate_v5_cell_status(
            provenance,
            manifest_result,
            common_request,
            symbol=symbol,
            timeframe=timeframe,
            manifest_source_environment=source_environment,
            artifact_observation=scan.artifact,
            expected_retries=retries,
            validation_context=context,
        )
    except NativeRequestContractError as exc:
        return [f"SOURCE_PRODUCTION_CONTRACT_INVALID:{code}" for code in exc.errors]
    return []


def _source_leaf(
    native_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    run_manifest_entry: Optional[Mapping[str, Any]] = None,
    run_manifest_errors: Sequence[str] = (),
    run_manifest_request: Optional[Mapping[str, Any]] = None,
    run_source_environment: Optional[Mapping[str, Any]] = None,
    run_acquisition_started_utc: Any = None,
    run_acquisition_completed_utc: Any = None,
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
        manifest_failure = (
            str(run_manifest_entry.get("failure_status") or "").strip().lower()
        )
        availability_class, coherence_error = classify_native_availability_cell(
            run_manifest_entry.get("status"),
            manifest_failure,
            run_manifest_entry.get("rows"),
            run_manifest_entry.get("resume_disposition"),
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
    if availability_class == "terminal_technical":
        if (
            not isinstance(run_manifest_entry, Mapping)
            or not isinstance(run_manifest_request, Mapping)
            or not isinstance(run_source_environment, Mapping)
        ):
            errors.append("SOURCE_TERMINAL_TECHNICAL_CONTEXT_MISSING")
        else:
            common_request = {
                key: run_manifest_request[key]
                for key in COMMON_REQUEST_KEYS
                if key in run_manifest_request
            }
            try:
                validate_v5_terminal_technical_cell(
                    provenance,
                    run_manifest_entry,
                    common_request,
                    symbol=symbol,
                    timeframe=timeframe,
                    run_id=run_id,
                    manifest_source_environment=run_source_environment,
                    expected_retries=run_manifest_request.get("retries"),
                    manifest_acquisition_started_utc=run_acquisition_started_utc,
                    manifest_acquisition_completed_utc=(run_acquisition_completed_utc),
                )
            except NativeRequestContractError as exc:
                errors.extend(
                    f"SOURCE_TERMINAL_TECHNICAL_INVALID:{code}" for code in exc.errors
                )
        return {
            "bars_path": bars_path,
            "provenance_path": provenance_path,
            "provenance": provenance,
            "provenance_sha256": sha256_file(provenance_path),
            "bars_sha256": None,
            "frame": None,
            "availability_class": availability_class,
            "availability_attestation": None,
            "errors": list(dict.fromkeys(errors)),
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
        if (
            str((run_manifest_entry or {}).get("sha256") or "").lower()
            != observed_sha.lower()
        ):
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
    if str(provenance.get("symbol", "")) != symbol:
        errors.append("SOURCE_SYMBOL_MISMATCH")
    if str(provenance.get("canonical_symbol", "")) != symbol:
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
                source_request_contract.get("timeframe_source_request_intervals") or {}
            ).get(timeframe),
        }
        request_map_version = str(
            source_request_contract.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
            expected_projection.update(
                {
                    "strategy_evidence_interval": (
                        source_request_contract.get(
                            "timeframe_strategy_evidence_intervals"
                        )
                        or {}
                    ).get(timeframe),
                    "audit_only_prefix_interval": (
                        source_request_contract.get(
                            "timeframe_audit_only_prefix_intervals"
                        )
                        or {}
                    ).get(timeframe),
                    "audit_reference_timeframe": (
                        source_request_contract.get("timeframe_audit_reference_map")
                        or {}
                    ).get(timeframe),
                }
            )
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            expected_projection.update(
                {
                    "strategy_evidence_interval": (
                        source_request_contract.get(
                            "timeframe_strategy_evidence_intervals"
                        )
                        or {}
                    ).get(timeframe),
                    "audit_reference_timeframes": (
                        source_request_contract.get("timeframe_audit_reference_map")
                        or {}
                    ).get(timeframe),
                    "audit_consumer_targets": (
                        source_request_contract.get("timeframe_audit_consumer_map")
                        or {}
                    ).get(timeframe),
                    "audit_windows": (
                        source_request_contract.get("timeframe_audit_windows") or {}
                    ).get(timeframe),
                    "audit_reference_intervals": (
                        source_request_contract.get(
                            "timeframe_audit_reference_intervals"
                        )
                        or {}
                    ).get(timeframe),
                    "audit_only_intervals": (
                        source_request_contract.get("timeframe_audit_only_intervals")
                        or {}
                    ).get(timeframe),
                }
            )
        for field_name, expected_value in expected_projection.items():
            if (
                field_name not in provenance.get("request", {})
                or provenance.get("request", {}).get(field_name) != expected_value
            ):
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
        if manifest_failure == "boundary_history_shortfall":
            # A leading/trailing requested-span shortfall is statistical
            # evidence, not source absence.  Preserve the writer's immutable
            # partial marker while allowing the exact timeframe cell to reach
            # the partition, bar-floor, resolution, and parity gates below.
            if (
                provenance_status != "partial"
                or provenance_failure != "boundary_history_shortfall"
                or frame is None
                or provenance.get("complete") is not False
            ):
                errors.append("SOURCE_BOUNDARY_HISTORY_SHORTFALL_INVALID")
            if (
                provenance.get("eligible_for_downstream_validation") is not False
                or provenance.get("eligible_for_optimization") is not False
            ):
                errors.append("SOURCE_BOUNDARY_HISTORY_SHORTFALL_SELF_PROMOTED")
            errors.extend(_boundary_shortfall_integrity_errors(provenance, frame))
        else:
            if provenance_status != _SUCCESS_SOURCE_STATUS or provenance_failure:
                errors.append("SOURCE_ACQUISITION_SUCCESS_INVALID")
            if provenance.get("complete") is not True:
                errors.append("SOURCE_INCOMPLETE")
    elif availability_class == "unavailable":
        if provenance.get("complete") is not False:
            errors.append("SOURCE_UNAVAILABLE_MARKED_COMPLETE")
        if (
            provenance.get("eligible_for_downstream_validation") is not False
            or provenance.get("eligible_for_optimization") is not False
        ):
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
            if (
                provenance_status != "failed"
                or frame is not None
                or provenance_rows != 0
            ):
                errors.append("SOURCE_NO_HISTORY_INVALID")
            if (
                provenance.get("artifact") is not None
                or provenance.get("sha256") is not None
            ):
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
        if isinstance(run_manifest_entry, Mapping):
            errors.extend(
                _production_v5_source_errors(
                    provenance,
                    run_manifest_entry,
                    run_manifest_request,
                    run_source_environment,
                    bars_path,
                    symbol=symbol,
                    timeframe=timeframe,
                )
            )

    availability_attestation: Optional[Dict[str, Any]] = None
    if manifest_failure == "partial_history":
        legacy_boundary_errors = _legacy_partial_boundary_integrity_errors(
            provenance,
            frame,
        )
        errors.extend(legacy_boundary_errors)
        # ``partial_history`` was historically ambiguous.  It is never a
        # SOURCE_UNAVAILABLE proof.  A clean page ledger upgrades it only in
        # this derived audit; otherwise it is a technical integrity failure.
        availability_class = "invalid"
        if not errors:
            availability_class = "success"
            availability_attestation = {
                "schema_version": "fxpm-native-boundary-shortfall-attestation-v1",
                "original_failure_status": "partial_history",
                "derived_availability_state": NATIVE_EVIDENCE_CANDIDATE,
                "run_id": run_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "observed_interval": {
                    "start_utc": pd.Timestamp(frame.index[0])
                    .tz_convert("UTC")
                    .isoformat(),
                    "end_open_utc": pd.Timestamp(frame.index[-1])
                    .tz_convert("UTC")
                    .isoformat(),
                },
                "requested_interval": {
                    "start_utc": (provenance.get("request") or {}).get(
                        "native_fetch_start_utc"
                    ),
                    "end_exclusive_utc": (provenance.get("request") or {}).get(
                        "native_fetch_end_exclusive_utc"
                    ),
                },
                "failed_page_count": 0,
                "internal_empty_page_count": 0,
                "internal_truncated_page_count": 0,
                "tradeable": False,
            }
            availability_attestation["attestation_sha256"] = _canonical_json_sha256(
                availability_attestation
            )

    if (
        observed_sha is not None
        and str(provenance.get("sha256") or "").lower() != observed_sha.lower()
    ):
        errors.append("SOURCE_SHA256_MISMATCH")
    if bars_required:
        artifact = provenance.get("artifact")
        if not isinstance(artifact, Mapping):
            errors.append("SOURCE_ARTIFACT_MISSING")
        else:
            if str(artifact.get("relative_path") or "") != "bars.csv":
                errors.append("SOURCE_ARTIFACT_PATH_INVALID")
            if (
                str(artifact.get("sha256") or "").lower()
                != str(observed_sha or "").lower()
            ):
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
        "availability_attestation": availability_attestation,
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
    if declared_method == SEGMENTED_SESSION_COVERAGE_METHOD_V2:
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
    if declared_method == SEGMENTED_SESSION_COVERAGE_METHOD_V2 and dict(
        details
    ) != dict(recomputed_details):
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
            if (
                declared_value is None
                or recomputed_value is None
                or not math.isclose(
                    declared_value,
                    recomputed_value,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
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
            or not math.isclose(
                declared_value, recomputed_value, rel_tol=0.0, abs_tol=1e-12
            )
        ):
            errors.append(f"SESSION_COVERAGE_VALUE_MISMATCH:{partition}")
        if not isinstance(detail, Mapping) or not isinstance(expected_detail, Mapping):
            errors.append(f"SESSION_COVERAGE_DETAIL_INVALID:{partition}")
            continue
        for field_name in (
            "observed_rows",
            "expected_rows_from_train_session_rate",
            "coverage",
        ):
            declared_number = _strict_finite_number(detail.get(field_name))
            expected_number = _strict_finite_number(expected_detail.get(field_name))
            if (
                declared_number is None
                or expected_number is None
                or not math.isclose(
                    declared_number,
                    expected_number,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
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
            if (
                top_value is None
                or detail_value is None
                or not math.isclose(
                    top_value,
                    detail_value,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
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
                candidate_start = pd.Timestamp(candidate.get("start_ts")).tz_convert(
                    "UTC"
                )
                candidate_end = pd.Timestamp(candidate.get("end_ts")).tz_convert("UTC")
            except Exception:
                errors.append(f"{code}:{partition}")
                continue
            if candidate_start != pd.Timestamp(start) or candidate_end != pd.Timestamp(
                end
            ):
                errors.append(f"{code}:{partition}")
    if errors:
        return "ERROR", list(dict.fromkeys(errors)), dict(recomputed)
    if declared_method in {
        SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    }:
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
            thin_weeks = int(detail.get("weeks_below_half_expected_schedule", 0) or 0)
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
    diagnostic_errors, normalized = validate_native_session_coverage_diagnostics(
        recomputed
    )
    if diagnostic_errors:
        return "ERROR", diagnostic_errors, normalized
    return "PASS", [], normalized


def _cross_resolution_coverage_verdict(
    report: Mapping[str, Any],
    minimum: float,
) -> Tuple[str, list[str], Dict[str, Optional[float]]]:
    """Classify an independently reconciled native timeframe pair.

    Unlike the legacy timestamp diagnostic, this map is computed from another
    broker-native resolution and is evaluated separately for train, tuning,
    and final partitions. Deterministic source inconsistencies are a cell-level
    insufficiency; malformed or incomplete audit evidence is technical.
    """

    empty = {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS}
    if not isinstance(report, Mapping) or str(report.get("schema_version") or "") != (
        CROSS_RESOLUTION_PARITY_SCHEMA_VERSION
    ):
        return "ERROR", ["CROSS_RESOLUTION_REPORT_INVALID"], empty
    partitions = report.get("partitions")
    if not isinstance(partitions, Mapping) or set(partitions) != set(
        NATIVE_EVIDENCE_PARTITIONS
    ):
        return "ERROR", ["CROSS_RESOLUTION_PARTITION_MAP_INVALID"], empty

    coverage: Dict[str, Optional[float]] = {}
    errors: list[str] = []
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        detail = partitions.get(partition)
        if not isinstance(detail, Mapping):
            coverage[partition] = None
            errors.append(f"CROSS_RESOLUTION_PARTITION_INVALID:{partition}")
            continue
        partition_status = str(detail.get("status") or "").upper()
        value = _strict_finite_number(detail.get("bidirectional_coverage"))
        try:
            compared_coarse = int(detail.get("coarse_bars", 0) or 0)
            compared_fine = int(detail.get("fine_rows", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            compared_coarse = compared_fine = -1
        no_comparable_evidence = compared_coarse == 0 or compared_fine == 0
        if value is None and (
            partition_status == "FAIL"
            or (partition_status == "PENDING" and no_comparable_evidence)
        ):
            # A completed comparison with no usable overlap is decisive zero
            # evidence, not a malformed ratio. This keeps genuinely short
            # histories as immutable cell exclusions instead of turning the
            # entire activation into a technical failure.
            value = 0.0
        coverage[partition] = value
        if value is None or not 0.0 <= value <= 1.0:
            errors.append(f"CROSS_RESOLUTION_COVERAGE_INVALID:{partition}")
        if partition_status not in {"PASS", "FAIL", "PENDING"}:
            errors.append(f"CROSS_RESOLUTION_PARTITION_STATUS_INVALID:{partition}")
    if errors:
        return "ERROR", list(dict.fromkeys(errors)), coverage

    report_status = str(report.get("status") or "").upper()
    if report_status == "PENDING":
        report_reasons = [
            f"CROSS_RESOLUTION:{reason}"
            for reason in (report.get("reason_codes") or [])
            if str(reason)
        ]
        return "PENDING", report_reasons or ["CROSS_RESOLUTION_PENDING"], coverage
    if report_status not in {"PASS", "FAIL"}:
        return "ERROR", ["CROSS_RESOLUTION_STATUS_INVALID"], coverage

    diagnostic_errors, normalized = validate_native_session_coverage_diagnostics(
        coverage
    )
    if diagnostic_errors:
        return "ERROR", diagnostic_errors, normalized
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


def _source_unavailable_proof_errors(
    provenance: Mapping[str, Any],
    *,
    symbol: str,
    timeframe: str,
) -> list[str]:
    """Validate the broker/server first-date proof for a zero-evidence cell."""

    proof = provenance.get("source_unavailable_proof")
    if not isinstance(proof, Mapping):
        return ["SOURCE_UNAVAILABLE_FIRST_DATE_PROOF_MISSING"]
    errors: list[str] = []
    if str(proof.get("schema_version") or "") != (
        "fxpm-native-source-unavailable-proof-v1"
    ):
        errors.append("SOURCE_UNAVAILABLE_PROOF_SCHEMA_INVALID")
    if str(proof.get("symbol") or "") != symbol:
        errors.append("SOURCE_UNAVAILABLE_PROOF_SYMBOL_MISMATCH")
    if str(proof.get("timeframe") or "").upper() != timeframe:
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
    for field_name in (
        "source_identity_sha256",
        "probe_ledger_sha256",
    ):
        value = str(proof.get(field_name) or "").lower()
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            errors.append(f"SOURCE_UNAVAILABLE_PROOF_{field_name.upper()}_INVALID")
    try:
        request = provenance.get("request")
        if not isinstance(request, Mapping):
            raise ValueError("request missing")
        request_start = _utc_timestamp(request.get("native_fetch_start_utc"))
        request_end = _utc_timestamp(request.get("native_fetch_end_exclusive_utc"))
        proof_start = _utc_timestamp(proof.get("requested_start_utc"))
        proof_end = _utc_timestamp(proof.get("requested_end_exclusive_utc"))
        queried_at = _utc_timestamp(proof.get("queried_at_utc"))
    except Exception:
        errors.append("SOURCE_UNAVAILABLE_PROOF_BOUNDARY_INVALID")
    else:
        if proof_start != request_start or proof_end != request_end:
            errors.append("SOURCE_UNAVAILABLE_PROOF_BOUNDARY_MISMATCH")
        if queried_at < request_end:
            errors.append("SOURCE_UNAVAILABLE_PROOF_QUERY_PRECEDES_INTERVAL_END")
    first_open = proof.get("broker_server_first_open_utc")
    if first_open is not None and first_open != "":
        try:
            first_ts = _utc_timestamp(first_open)
        except Exception:
            errors.append("SOURCE_UNAVAILABLE_PROOF_FIRST_DATE_INVALID")
        else:
            if "request_end" in locals() and first_ts < request_end:
                errors.append(
                    "SOURCE_UNAVAILABLE_PROOF_FIRST_DATE_CONTRADICTS_EMPTY_INTERVAL"
                )
    if proof.get("all_requested_pages_terminal_empty") is not True:
        errors.append("SOURCE_UNAVAILABLE_PROOF_PAGE_EXHAUSTION_MISSING")
    if proof.get("tradeable") is not False:
        errors.append("SOURCE_UNAVAILABLE_PROOF_TRADEABLE_FLAG_INVALID")
    return list(dict.fromkeys(errors))


def _has_unopposed_semantic_failure(envelope: Mapping[str, Any]) -> bool:
    """Return whether a contradiction exists without an exact passing witness.

    The consensus policy intentionally lets one exact PASS isolate one faulty
    sibling.  A SEMANTIC_FAIL paired only with PENDING/unavailable evidence is
    different: there is no authenticated witness that identifies which side is
    wrong, so activation must require reacquisition instead of silently
    promoting the target as an evidence candidate.
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


def _has_only_authenticated_terminal_pending(
    envelope: Mapping[str, Any],
) -> bool:
    """Recognize completed audits whose remaining evidence is prefix-short.

    ``PENDING`` is normally non-terminal.  The V5 pair scoper also uses it for
    narrower, fully evaluated facts: an authenticated target/reference does
    not reach a scored historical prefix, or an authenticated sibling has
    already terminalized as technical/resolution-excluded.  Those are
    cell-local insufficient evidence, not unfinished audit transactions.
    Unknown or synthetic pending reasons remain globally blocking.
    """

    diagnostics = envelope.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        return False
    if diagnostics.get("all_references_evaluated") is not True:
        return False
    if diagnostics.get("raw_pair_reports_recomputed_from_pinned_bars") is not True:
        return False
    edges = envelope.get("edges")
    windows = envelope.get("windows")
    if not isinstance(edges, Mapping) or not isinstance(windows, Mapping):
        return False
    saw_pending = False
    allowed_prefixes = (
        "TARGET_AUDIT_PREFIX_PENDING:",
        "REFERENCE_AUDIT_PREFIX_PENDING:",
        "AUDIT_REFERENCE_TERMINAL_TECHNICAL:",
        "AUDIT_REFERENCE_RESOLUTION_EXCLUDED:",
        "PARTITION_OVERLAP_TOO_SMALL:",
        "FINE_REFERENCE_EMPTY:",
    )
    for window in REQUIRED_AUDIT_WINDOWS:
        window_record = windows.get(window)
        edge_states = (
            window_record.get("edge_states")
            if isinstance(window_record, Mapping)
            else None
        )
        if not isinstance(edge_states, Mapping) or set(edge_states) != set(edges):
            return False
        for reference, state in edge_states.items():
            if not isinstance(state, Mapping):
                return False
            if str(state.get("integrity_state") or "").upper() != INTEGRITY_PASS:
                return False
            if str(state.get("evidence_state") or "").upper() != EVIDENCE_PENDING:
                continue
            saw_pending = True
            edge = edges.get(reference)
            report = edge.get("report") if isinstance(edge, Mapping) else None
            report_windows = (
                report.get("windows") if isinstance(report, Mapping) else None
            )
            detail = (
                report_windows.get(window)
                if isinstance(report_windows, Mapping)
                else None
            )
            reasons = (
                [str(value) for value in (detail.get("reason_codes") or ())]
                if isinstance(detail, Mapping)
                else []
            )
            if not reasons or any(
                not reason.startswith(allowed_prefixes) for reason in reasons
            ):
                return False
    return saw_pending


def _derive_authenticity_state(
    *,
    availability_class: str,
    source_errors: Sequence[str],
    resolution_status: str,
    consensus_status: str,
    source_unavailable_proven: bool,
    unopposed_semantic_failure: bool = False,
) -> str:
    """Derive a data state only; never infer a trading or profitability state."""

    availability = str(availability_class or "").strip().lower()
    resolution = str(resolution_status or "").strip().upper()
    consensus = str(consensus_status or "").strip().upper()
    if availability == "unavailable":
        return (
            NATIVE_SOURCE_UNAVAILABLE
            if source_unavailable_proven and not source_errors
            else NATIVE_TECHNICAL_ERROR
        )
    if availability == "terminal_technical":
        return NATIVE_TECHNICAL_ERROR
    if availability != "success" or source_errors:
        return NATIVE_TECHNICAL_ERROR
    if resolution == "PENDING":
        return NATIVE_PENDING
    if resolution != "PASS":
        return NATIVE_TECHNICAL_ERROR
    # Exact contradictions on authenticated overlap remain integrity faults.
    # A sparse/missing sibling yields PENDING and cannot veto the target cell.
    if consensus in {"FAIL", "ERROR"} or unopposed_semantic_failure:
        return NATIVE_TECHNICAL_ERROR
    if consensus not in {"PASS", "PENDING", "NOT_APPLICABLE"}:
        return NATIVE_TECHNICAL_ERROR
    return NATIVE_EVIDENCE_CANDIDATE


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
    technical_reasons: list[str] = []
    blocking_reasons: list[str] = [str(reason) for reason in run_manifest_errors]
    success_count = 0
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
        disposition = str(item.get("resume_disposition") or "").strip().lower()
        try:
            rows = int(item.get("rows", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            rows = 0
        cell = {
            "status": status,
            "failure_status": failure or None,
            "rows": rows,
        }
        if disposition == "terminal_technical":
            cell["resume_disposition"] = disposition
        cells[timeframe] = cell
        classification, coherence_error = classify_native_availability_cell(
            status,
            failure,
            rows,
            disposition,
        )
        if classification == "success":
            success_count += 1
            continue
        if classification == "unavailable":
            availability_reasons.append(f"{timeframe}:{failure.upper()}")
        elif classification == "terminal_technical":
            technical_reasons.append(
                f"{timeframe}:TERMINAL_TECHNICAL:{failure.upper()}"
            )
        else:
            blocking_reasons.append(
                f"{timeframe}:NON_AVAILABILITY_SOURCE_FAILURE:"
                f"{coherence_error or failure or status or 'unknown'}"
            )
    if blocking_reasons:
        status = "invalid"
    elif success_count:
        status = "eligible"
    elif technical_reasons:
        status = NATIVE_TECHNICAL_ERROR
    elif availability_reasons:
        status = "excluded_no_trade"
    else:
        status = "eligible"
    return {
        "symbol": symbol,
        "status": status,
        "decision_basis": NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
        "reason_codes": list(
            dict.fromkeys(blocking_reasons or technical_reasons or availability_reasons)
        ),
        "availability_cells": cells,
    }


def _legacy_cross_resolution_reference_map(
    timeframes: Sequence[str],
) -> Dict[str, str]:
    """Reconstruct the immutable V4 one-reference graph for forensics."""

    ordered = tuple(
        sorted(
            (str(value).upper() for value in timeframes),
            key=lambda value: TIMEFRAME_SECONDS[value],
        )
    )
    if len(ordered) < 2:
        return {}
    return {
        timeframe: (ordered[1] if index == 0 else ordered[index - 1])
        for index, timeframe in enumerate(ordered)
    }


def _v5_interval(value: Any) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    if not isinstance(value, Mapping) or set(value) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        return None
    try:
        start = pd.Timestamp(value.get("start_utc"))
        end = pd.Timestamp(value.get("end_exclusive_utc"))
        if start.tzinfo is None or end.tzinfo is None:
            return None
        start = start.tz_convert("UTC")
        end = end.tz_convert("UTC")
    except (TypeError, ValueError, OverflowError):
        return None
    return (start, end) if start < end else None


def _v5_request_errors(
    request: Mapping[str, Any],
    timeframes: Sequence[str],
) -> list[str]:
    """Validate V5 graph semantics in addition to its immutable hash."""

    errors: list[str] = []
    roster = tuple(str(value).upper() for value in timeframes)
    if roster != tuple(DEFAULT_TIMEFRAMES):
        errors.append("RUN_MANIFEST_V5_SIX_TIMEFRAME_ROSTER_REQUIRED")
        return errors
    expected_graph = {
        target: list(expected_reference_timeframes(target))
        for target in DEFAULT_TIMEFRAMES
    }
    graph = request.get("timeframe_audit_reference_map")
    if not isinstance(graph, Mapping) or dict(graph) != expected_graph:
        errors.append("RUN_MANIFEST_V5_REFERENCE_GRAPH_INVALID")
    expected_consumers = {timeframe: [] for timeframe in DEFAULT_TIMEFRAMES}
    for target in DEFAULT_TIMEFRAMES:
        for reference in expected_graph[target]:
            expected_consumers[reference].append(target)
    consumers = request.get("timeframe_audit_consumer_map")
    if not isinstance(consumers, Mapping) or dict(consumers) != expected_consumers:
        errors.append("RUN_MANIFEST_V5_CONSUMER_GRAPH_INVALID")
    if request.get("timeframe_consensus_policy") != TWO_REFERENCE_CONSENSUS_POLICY:
        errors.append("RUN_MANIFEST_V5_CONSENSUS_POLICY_INVALID")

    audit_windows = request.get("timeframe_audit_windows")
    reference_intervals = request.get("timeframe_audit_reference_intervals")
    source_intervals = request.get("timeframe_source_request_intervals")
    strategy_intervals = request.get("timeframe_strategy_evidence_intervals")
    boundaries = request.get("timeframe_calendar_boundaries")
    audit_only = request.get("timeframe_audit_only_intervals")
    for field_name, value in (
        ("AUDIT_WINDOWS", audit_windows),
        ("AUDIT_REFERENCE_INTERVALS", reference_intervals),
        ("SOURCE_INTERVALS", source_intervals),
        ("STRATEGY_INTERVALS", strategy_intervals),
        ("CALENDAR_BOUNDARIES", boundaries),
        ("AUDIT_ONLY_INTERVALS", audit_only),
    ):
        if not isinstance(value, Mapping) or set(value) != set(DEFAULT_TIMEFRAMES):
            errors.append(f"RUN_MANIFEST_V5_{field_name}_ROSTER_INVALID")

    if errors:
        return list(dict.fromkeys(errors))
    assert isinstance(audit_windows, Mapping)
    assert isinstance(reference_intervals, Mapping)
    assert isinstance(source_intervals, Mapping)
    assert isinstance(strategy_intervals, Mapping)
    assert isinstance(boundaries, Mapping)
    assert isinstance(audit_only, Mapping)
    for target in DEFAULT_TIMEFRAMES:
        target_windows = audit_windows.get(target)
        if not isinstance(target_windows, Mapping) or set(target_windows) != set(
            REQUIRED_AUDIT_WINDOWS
        ):
            errors.append(f"RUN_MANIFEST_V5_AUDIT_WINDOW_ROSTER_INVALID:{target}")
            continue
        parsed_windows: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}
        for window in REQUIRED_AUDIT_WINDOWS:
            parsed = _v5_interval(target_windows.get(window))
            if parsed is None:
                errors.append(f"RUN_MANIFEST_V5_AUDIT_WINDOW_INVALID:{target}:{window}")
            else:
                parsed_windows[window] = parsed
            if window != "strategy_full_context":
                declared_boundary = (boundaries.get(target) or {}).get(window)
                if target_windows.get(window) != (
                    {
                        "start_utc": declared_boundary.get("start_ts"),
                        "end_exclusive_utc": declared_boundary.get("end_ts"),
                    }
                    if isinstance(declared_boundary, Mapping)
                    else None
                ):
                    errors.append(
                        f"RUN_MANIFEST_V5_AUDIT_CALENDAR_MISMATCH:{target}:{window}"
                    )
        strategy = _v5_interval(strategy_intervals.get(target))
        source = _v5_interval(source_intervals.get(target))
        full_context = parsed_windows.get("strategy_full_context")
        expected_strategy: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
        if full_context is not None:
            try:
                transformed = native_source_interval(
                    target,
                    full_context[0].to_pydatetime(),
                    full_context[1].to_pydatetime(),
                )
                expected_strategy = (
                    pd.Timestamp(transformed[0]),
                    pd.Timestamp(transformed[1]),
                )
            except (TypeError, ValueError, OverflowError):
                expected_strategy = None
        if (
            strategy is None
            or source is None
            or expected_strategy is None
            or strategy != expected_strategy
            or source[0] > strategy[0]
            or source[1] < strategy[1]
        ):
            errors.append(f"RUN_MANIFEST_V5_STRATEGY_CONTEXT_INVALID:{target}")

        target_edges = reference_intervals.get(target)
        expected_references = expected_reference_timeframes(target)
        if not isinstance(target_edges, Mapping) or set(target_edges) != set(
            expected_references
        ):
            errors.append(f"RUN_MANIFEST_V5_EDGE_ROSTER_INVALID:{target}")
            continue
        for reference in expected_references:
            edge_windows = target_edges.get(reference)
            if not isinstance(edge_windows, Mapping) or set(edge_windows) != set(
                REQUIRED_AUDIT_WINDOWS
            ):
                errors.append(
                    f"RUN_MANIFEST_V5_EDGE_WINDOW_ROSTER_INVALID:{target}:{reference}"
                )
                continue
            reference_source = _v5_interval(source_intervals.get(reference))
            for window in REQUIRED_AUDIT_WINDOWS:
                edge_interval = _v5_interval(edge_windows.get(window))
                target_interval = parsed_windows.get(window)
                expected_edge: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
                if target_interval is not None:
                    try:
                        transformed = native_source_interval(
                            reference,
                            target_interval[0].to_pydatetime(),
                            target_interval[1].to_pydatetime(),
                        )
                        expected_edge = (
                            pd.Timestamp(transformed[0]),
                            pd.Timestamp(transformed[1]),
                        )
                    except (TypeError, ValueError, OverflowError):
                        expected_edge = None
                if (
                    edge_interval is None
                    or target_interval is None
                    or reference_source is None
                    or expected_edge is None
                    or edge_interval != expected_edge
                    or reference_source[0] > edge_interval[0]
                    or reference_source[1] < edge_interval[1]
                ):
                    errors.append(
                        "RUN_MANIFEST_V5_EDGE_INTERVAL_INVALID:"
                        f"{target}:{reference}:{window}"
                    )
        if not isinstance(audit_only.get(target), list):
            errors.append(f"RUN_MANIFEST_V5_AUDIT_ONLY_INVALID:{target}")
    return list(dict.fromkeys(errors))


def _v5_audit_windows(
    request: Mapping[str, Any],
    target: str,
) -> Dict[str, Tuple[datetime, datetime]]:
    raw = (request.get("timeframe_audit_windows") or {}).get(target) or {}
    result: Dict[str, Tuple[datetime, datetime]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        parsed = _v5_interval(raw.get(window))
        if parsed is not None:
            result[window] = (parsed[0].to_pydatetime(), parsed[1].to_pydatetime())
    return result


def _v5_source_binding(source: Mapping[str, Any]) -> Dict[str, Any]:
    def _digest(value: Any) -> str:
        text = str(value or "").lower()
        return (
            text
            if len(text) == 64
            and all(character in "0123456789abcdef" for character in text)
            else "0" * 64
        )

    terminal_technical = (
        str(source.get("availability_class") or "").strip().lower()
        == "terminal_technical"
    )
    return {
        "availability_state": (
            "TECHNICAL_TERMINAL" if terminal_technical else "AVAILABLE"
        ),
        # A terminal technical binding explicitly has no bar artifact. Other
        # invalid sources retain the legacy placeholder only so a failed audit
        # can be serialized; their integrity stays ERROR and blocks activation.
        "bars_sha256": (
            None if terminal_technical else _digest(source.get("bars_sha256"))
        ),
        "provenance_sha256": _digest(source.get("provenance_sha256")),
    }


def _v5_unavailable_binding() -> Dict[str, Any]:
    return {
        "availability_state": EVIDENCE_TERMINAL_UNAVAILABLE,
        "bars_sha256": None,
        "provenance_sha256": None,
    }


def _v5_pair_window_states(
    report: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    partitions = report.get("partitions")
    result: Dict[str, Dict[str, Any]] = {}
    for window in REQUIRED_AUDIT_WINDOWS:
        detail = partitions.get(window) if isinstance(partitions, Mapping) else None
        status = str(
            detail.get("status") if isinstance(detail, Mapping) else ""
        ).upper()
        if status == "PASS":
            evidence = EVIDENCE_PASS
            integrity = INTEGRITY_PASS
        elif status == "FAIL":
            evidence = EVIDENCE_SEMANTIC_FAIL
            integrity = INTEGRITY_PASS
        elif status == "PENDING":
            evidence = EVIDENCE_PENDING
            integrity = INTEGRITY_PASS
        else:
            evidence = EVIDENCE_PENDING
            integrity = INTEGRITY_ERROR
        result[window] = {
            "integrity_state": integrity,
            "evidence_state": evidence,
            "reason_codes": list(
                detail.get("reason_codes") if isinstance(detail, Mapping) else [] or []
            ),
        }
    return result


def _v5_constant_edge_states(
    *,
    integrity: str,
    evidence: str,
    reasons: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    return {
        window: {
            "integrity_state": integrity,
            "evidence_state": evidence,
            "reason_codes": list(reasons),
        }
        for window in REQUIRED_AUDIT_WINDOWS
    }


def _v5_consensus_diagnostics(
    envelope: Mapping[str, Any],
) -> Dict[str, Any]:
    per_window: Dict[str, Any] = {}
    disagreement_count = 0
    for window in REQUIRED_AUDIT_WINDOWS:
        detail = (envelope.get("windows") or {}).get(window) or {}
        edge_states = detail.get("edge_states") or {}
        outcomes = {
            str(reference): {
                "integrity_state": state.get("integrity_state"),
                "evidence_state": state.get("evidence_state"),
            }
            for reference, state in edge_states.items()
            if isinstance(state, Mapping)
        }
        distinct = {
            (value["integrity_state"], value["evidence_state"])
            for value in outcomes.values()
        }
        disagrees = len(distinct) > 1
        disagreement_count += int(disagrees)
        per_window[window] = {
            "consensus_status": detail.get("status"),
            "disagreement": disagrees,
            "edge_outcomes": outcomes,
        }
    return {
        "window_disagreement_count": disagreement_count,
        "windows_with_disagreement": [
            window
            for window in REQUIRED_AUDIT_WINDOWS
            if per_window[window]["disagreement"]
        ],
        "per_window": per_window,
    }


def _v5_coverage_from_pairs(
    envelope: Mapping[str, Any],
    pair_reports: Mapping[str, Optional[Mapping[str, Any]]],
) -> Dict[str, Optional[float]]:
    coverage: Dict[str, Optional[float]] = {}
    for partition in NATIVE_EVIDENCE_PARTITIONS:
        status = str(
            ((envelope.get("windows") or {}).get(partition) or {}).get("status") or ""
        ).upper()
        values: list[float] = []
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
            value = _strict_finite_number(detail.get("bidirectional_coverage"))
            if value is not None and 0.0 <= value <= 1.0:
                values.append(value)
        if status == "PASS" and values:
            coverage[partition] = max(values)
        elif status == "FAIL":
            coverage[partition] = 0.0
        else:
            coverage[partition] = None
    return coverage


def _v5_authenticated_tolerance(source: Mapping[str, Any]) -> Optional[float]:
    instrument = (source.get("provenance") or {}).get("instrument")
    try:
        value = float(
            instrument.get("parity_tick_tolerance")
            if isinstance(instrument, Mapping)
            else None
        )
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0.0 else None


def _v5_load_frame(source: Mapping[str, Any]) -> Optional[pd.DataFrame]:
    path = source.get("bars_path")
    if str(source.get("availability_class") or "") != "success":
        return None
    if not isinstance(path, Path) or not path.is_file():
        return None
    try:
        return _read_bars(path)
    except Exception:
        return None


def _v5_scored_resolution_frames(
    source: Mapping[str, Any],
    frame: Optional[pd.DataFrame],
    daily_frame: Optional[pd.DataFrame],
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """Project cadence evidence onto authenticated, scored partitions."""

    provenance = source.get("provenance")
    request = provenance.get("request") if isinstance(provenance, Mapping) else None
    boundaries = (
        request.get("cell_calendar_boundaries")
        if isinstance(request, Mapping)
        else None
    )
    if not isinstance(boundaries, Mapping) or set(boundaries) != set(
        NATIVE_EVIDENCE_PARTITIONS
    ):
        return None, None, "SOURCE_SCORED_EVIDENCE_INTERVALS_INVALID"
    intervals: list[Tuple[pd.Timestamp, pd.Timestamp]] = []
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
            return None, None, "SOURCE_SCORED_EVIDENCE_INTERVALS_INVALID"
        intervals.append(interval)
    intervals.sort(key=lambda value: (value[0], value[1]))

    def _slice(value: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if value is None:
            return None
        pieces = []
        for start, end in intervals:
            left = int(value.index.searchsorted(start, side="left"))
            right = int(value.index.searchsorted(end, side="left"))
            pieces.append(value.iloc[left:right])
        return pd.concat(pieces, axis=0, copy=False)

    return _slice(frame), _slice(daily_frame), None


def _v5_audit_symbol(
    native_root: Path,
    *,
    audit_root: Path,
    symbol: str,
    timeframes: Sequence[str],
    run_id: str,
    activation_id: str,
    run_manifest_cells: Mapping[Tuple[str, str], Mapping[str, Any]],
    run_manifest_errors: Sequence[str],
    run_request: Mapping[str, Any],
    run_source_environment: Mapping[str, Any],
    run_acquisition_started_utc: Any,
    run_acquisition_completed_utc: Any,
    run_manifest_path: Path,
    run_manifest_sha: Optional[str],
    availability: Mapping[str, Any],
    thresholds: ParityThresholds,
    overrides: Mapping[str, float],
    audited_at: str,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], list[str]]:
    """Audit one V5 symbol while retaining at most two bar frames at once."""

    sources: Dict[str, Dict[str, Any]] = {}
    for timeframe in timeframes:
        loaded = _source_leaf(
            native_root,
            run_id,
            symbol,
            timeframe,
            run_manifest_cells.get((symbol, timeframe)),
            run_manifest_errors,
            run_request,
            run_source_environment,
            run_acquisition_started_utc,
            run_acquisition_completed_utc,
        )
        # _source_leaf authenticates the bytes while the frame is resident.
        # Retain metadata only; resolution and pair audits reload exactly the
        # target/reference pair needed for the current calculation.
        loaded.pop("frame", None)
        sources[timeframe] = loaded

    resolution_audits: Dict[str, Dict[str, Any]] = {}
    d1_frame = _v5_load_frame(sources.get("D1", {}))
    for timeframe in timeframes:
        source = sources[timeframe]
        frame = d1_frame if timeframe == "D1" else _v5_load_frame(source)
        resolution_frame, resolution_daily_frame, interval_error = (
            _v5_scored_resolution_frames(source, frame, d1_frame)
        )
        tolerance = _v5_authenticated_tolerance(source)
        override = overrides.get(symbol)
        source_errors = list(source.get("errors") or [])
        if interval_error is not None:
            source_errors.append(interval_error)
        if override is not None:
            if tolerance is None or override > tolerance + 1e-12:
                source_errors.append(
                    "TICK_TOLERANCE_OVERRIDE_WEAKENS_AUTHENTICATED_BASE"
                )
            else:
                tolerance = override
        source["errors"] = list(dict.fromkeys(source_errors))
        resolution_audits[timeframe] = audit_native_resolution(
            resolution_frame,
            symbol=symbol,
            timeframe=timeframe,
            daily_frame=resolution_daily_frame,
            tick_tolerance=tolerance,
        ).to_dict()
        if timeframe != "D1":
            del frame
    del d1_frame

    symbol_entries: Dict[str, Dict[str, Any]] = {}
    audited_cells: Dict[str, Dict[str, Any]] = {}
    blocking: list[str] = []
    availability_cells = (
        availability.get("availability_cells")
        if isinstance(availability.get("availability_cells"), Mapping)
        else {}
    )
    for timeframe in timeframes:
        source = sources[timeframe]
        provenance = source.get("provenance") or {}
        source_errors = list(source.get("errors") or [])
        manifest_classification, availability_error = classify_native_availability_cell(
            (availability_cells.get(timeframe) or {}).get("status"),
            (availability_cells.get(timeframe) or {}).get("failure_status"),
            (availability_cells.get(timeframe) or {}).get("rows"),
            (availability_cells.get(timeframe) or {}).get("resume_disposition"),
        )
        classification = (
            str(source.get("availability_class") or manifest_classification)
            .strip()
            .lower()
        )
        resolution = resolution_audits[timeframe]
        resolution_status = str(resolution.get("status") or "PENDING").upper()
        resolution_reasons = [
            str(value) for value in (resolution.get("reason_codes") or []) if str(value)
        ]
        references = expected_reference_timeframes(timeframe)
        source_binding = _v5_source_binding(source)
        pair_reports: Dict[str, Optional[Mapping[str, Any]]] = {
            reference: None for reference in references
        }
        reference_bindings: Dict[str, Dict[str, Any]] = {}
        edge_reports: Dict[str, Dict[str, Any]] = {}
        target_frame = _v5_load_frame(source)
        tolerance = _v5_authenticated_tolerance(source)
        if overrides.get(symbol) is not None and tolerance is not None:
            tolerance = min(tolerance, float(overrides[symbol]))

        if classification == "success":
            for reference in references:
                reference_source = sources[reference]
                reference_errors = list(reference_source.get("errors") or [])
                reference_class = str(
                    reference_source.get("availability_class") or "invalid"
                )
                reference_failure = str(
                    ((reference_source.get("provenance") or {}).get("failure_status"))
                    or ""
                ).upper()
                reference_resolution_status = str(
                    (resolution_audits.get(reference) or {}).get("status")
                    or "PENDING"
                ).upper()
                if reference_class == "unavailable" and not reference_errors:
                    reference_binding = _v5_unavailable_binding()
                    states = _v5_constant_edge_states(
                        integrity=INTEGRITY_PASS,
                        evidence=EVIDENCE_TERMINAL_UNAVAILABLE,
                        reasons=[
                            f"AUDIT_REFERENCE_UNAVAILABLE:{reference}:"
                            f"{reference_failure or 'UNSPECIFIED'}"
                        ],
                    )
                elif reference_class == "terminal_technical" and not reference_errors:
                    reference_binding = _v5_source_binding(reference_source)
                    states = _v5_constant_edge_states(
                        integrity=INTEGRITY_PASS,
                        evidence=EVIDENCE_PENDING,
                        reasons=[f"AUDIT_REFERENCE_TERMINAL_TECHNICAL:{reference}"],
                    )
                elif (
                    reference_class == "success"
                    and not reference_errors
                    and reference_resolution_status == "FAIL"
                ):
                    # The sibling has already failed its own authenticated
                    # cadence-resolution audit and is terminally excluded from
                    # search. It cannot be reused as contradictory evidence
                    # against an otherwise valid target cell.
                    reference_binding = _v5_source_binding(reference_source)
                    states = _v5_constant_edge_states(
                        integrity=INTEGRITY_PASS,
                        evidence=EVIDENCE_PENDING,
                        reasons=[f"AUDIT_REFERENCE_RESOLUTION_EXCLUDED:{reference}"],
                    )
                elif source_errors or target_frame is None:
                    reference_binding = _v5_source_binding(reference_source)
                    states = _v5_constant_edge_states(
                        integrity=INTEGRITY_ERROR,
                        evidence=EVIDENCE_PENDING,
                        reasons=[
                            "TARGET_SOURCE_INTEGRITY_ERROR",
                            *(f"TARGET:{reason}" for reason in source_errors),
                        ],
                    )
                elif reference_errors:
                    reference_binding = _v5_source_binding(reference_source)
                    states = _v5_constant_edge_states(
                        integrity=INTEGRITY_ERROR,
                        evidence=EVIDENCE_PENDING,
                        reasons=[
                            f"REFERENCE:{reference}:{reason}"
                            for reason in reference_errors
                        ],
                    )
                else:
                    reference_binding = _v5_source_binding(reference_source)
                    reference_tolerance = _v5_authenticated_tolerance(reference_source)
                    reference_frame = _v5_load_frame(reference_source)
                    if (
                        reference_frame is None
                        or tolerance is None
                        or reference_tolerance is None
                    ):
                        states = _v5_constant_edge_states(
                            integrity=INTEGRITY_ERROR,
                            evidence=EVIDENCE_PENDING,
                            reasons=[
                                f"REFERENCE_FRAME_OR_TOLERANCE_INVALID:{reference}"
                            ],
                        )
                    else:
                        if TIMEFRAME_SECONDS[reference] < TIMEFRAME_SECONDS[timeframe]:
                            fine_tf, fine_frame = reference, reference_frame
                            coarse_tf, coarse_frame = timeframe, target_frame
                        else:
                            fine_tf, fine_frame = timeframe, target_frame
                            coarse_tf, coarse_frame = reference, reference_frame
                        audit_windows = _v5_audit_windows(run_request, timeframe)
                        raw_report = audit_native_cross_resolution(
                            fine_frame,
                            coarse_frame,
                            symbol=symbol,
                            fine_timeframe=fine_tf,
                            coarse_timeframe=coarse_tf,
                            activation_target_timeframe=timeframe,
                            # Cross-source metadata can disagree. The pair is
                            # evaluated at the stricter authenticated precision;
                            # it is never permitted to inherit the looser value.
                            tick_tolerance=min(tolerance, reference_tolerance),
                            partition_windows=audit_windows,
                            thresholds=thresholds,
                        )
                        raw_report = dict(raw_report)
                        raw_report["reference_timeframe"] = reference
                        pair_reports[reference] = raw_report
                        states = scope_native_pair_window_states(
                            _v5_pair_window_states(raw_report),
                            target_frame=target_frame,
                            reference_frame=reference_frame,
                            partition_windows=audit_windows,
                            boundary_tolerance_seconds=float(
                                run_request.get("boundary_tolerance_days", 0.0)
                            )
                            * 86_400.0,
                        )
                    del reference_frame
                reference_bindings[reference] = reference_binding
                edge_reports[reference] = build_edge_report(
                    symbol=symbol,
                    target_timeframe=timeframe,
                    reference_timeframe=reference,
                    source_binding=source_binding,
                    reference_binding=reference_binding,
                    window_states=states,
                    pair_reports=(
                        {
                            window: pair_reports[reference]
                            for window in REQUIRED_AUDIT_WINDOWS
                        }
                        if pair_reports[reference] is not None
                        else None
                    ),
                )
            envelope = build_consensus_envelope(
                symbol=symbol,
                target_timeframe=timeframe,
                source_binding=source_binding,
                reference_bindings=reference_bindings,
                edge_reports=edge_reports,
            )
            diagnostics = _v5_consensus_diagnostics(envelope)
            envelope["diagnostics"] = {
                **dict(envelope.get("diagnostics") or {}),
                **diagnostics,
                "raw_pair_reports_recomputed_from_pinned_bars": True,
            }
            coverage = _v5_coverage_from_pairs(envelope, pair_reports)
            consensus_status = str(envelope.get("status") or "ERROR").upper()
        elif classification == "terminal_technical":
            for reference in references:
                reference_source = sources[reference]
                reference_class = str(
                    reference_source.get("availability_class") or "invalid"
                )
                reference_binding = (
                    _v5_unavailable_binding()
                    if reference_class == "unavailable"
                    else _v5_source_binding(reference_source)
                )
                states = _v5_constant_edge_states(
                    integrity=INTEGRITY_PASS,
                    evidence=EVIDENCE_PENDING,
                    reasons=["TARGET_TERMINAL_TECHNICAL_NO_BAR_EVIDENCE"],
                )
                reference_bindings[reference] = reference_binding
                edge_reports[reference] = build_edge_report(
                    symbol=symbol,
                    target_timeframe=timeframe,
                    reference_timeframe=reference,
                    source_binding=source_binding,
                    reference_binding=reference_binding,
                    window_states=states,
                    pair_reports=None,
                )
            envelope = build_consensus_envelope(
                symbol=symbol,
                target_timeframe=timeframe,
                source_binding=source_binding,
                reference_bindings=reference_bindings,
                edge_reports=edge_reports,
            )
            diagnostics = _v5_consensus_diagnostics(envelope)
            envelope["diagnostics"] = {
                **dict(envelope.get("diagnostics") or {}),
                **diagnostics,
                "raw_pair_reports_recomputed_from_pinned_bars": False,
            }
            coverage = {partition: None for partition in NATIVE_EVIDENCE_PARTITIONS}
            consensus_status = str(envelope.get("status") or "ERROR").upper()
        else:
            # A missing target has no mathematically meaningful pair. Its
            # acquisition proof is terminal and the cell is excluded without
            # pretending that a consensus calculation occurred.
            consensus_status = "FAIL" if classification == "unavailable" else "ERROR"
            coverage = {partition: 0.0 for partition in NATIVE_EVIDENCE_PARTITIONS}
            diagnostics = {
                "window_disagreement_count": 0,
                "windows_with_disagreement": [],
                "per_window": {},
            }
            envelope = {
                "schema_version": CONSENSUS_SCHEMA_VERSION,
                "symbol": symbol,
                "timeframe": timeframe,
                "activation_target_timeframe": timeframe,
                "status": consensus_status,
                "reference_timeframes": list(references),
                "source_unavailable": classification == "unavailable",
                "diagnostics": {
                    **diagnostics,
                    "raw_pair_reports_recomputed_from_pinned_bars": False,
                },
            }
        del target_frame

        if classification == "terminal_technical":
            coverage_status = "NOT_APPLICABLE"
            coverage_reasons = ["TERMINAL_TECHNICAL_NO_BAR_EVIDENCE"]
        elif consensus_status == "PASS":
            coverage_reasons, normalized_coverage = (
                validate_native_session_coverage_diagnostics(coverage)
            )
            coverage = normalized_coverage
            coverage_status = "ERROR" if coverage_reasons else "PASS"
        elif consensus_status == "FAIL":
            coverage_status = "NOT_COMPARABLE"
            coverage_reasons = ["CROSS_RESOLUTION_CONSENSUS_FAILED"]
        elif consensus_status == "PENDING":
            coverage_status = "NOT_COMPARABLE"
            coverage_reasons = ["CROSS_RESOLUTION_CONSENSUS_PENDING"]
        else:
            coverage_status = "ERROR"
            coverage_reasons = ["CROSS_RESOLUTION_CONSENSUS_INTEGRITY_ERROR"]

        reasons: list[str] = []
        reasons.extend(f"SOURCE:{reason}" for reason in source_errors)
        reasons.extend(f"RESOLUTION:{reason}" for reason in resolution_reasons)
        reasons.extend(coverage_reasons)
        for window, detail in diagnostics.get("per_window", {}).items():
            if detail.get("disagreement"):
                reasons.append(f"CONSENSUS_DISAGREEMENT:{window}")
        reasons = list(dict.fromkeys(reasons))

        unavailable_proof_errors: list[str] = []
        if classification == "unavailable":
            unavailable_proof_errors = _source_unavailable_proof_errors(
                provenance,
                symbol=symbol,
                timeframe=timeframe,
            )
            source_errors.extend(unavailable_proof_errors)
            reasons.extend(f"SOURCE:{reason}" for reason in unavailable_proof_errors)
        unopposed_semantic_failure = _has_unopposed_semantic_failure(envelope)
        final_status = _derive_authenticity_state(
            availability_class=classification,
            source_errors=source_errors,
            resolution_status=resolution_status,
            consensus_status=consensus_status,
            source_unavailable_proven=(
                classification == "unavailable" and not unavailable_proof_errors
            ),
            unopposed_semantic_failure=unopposed_semantic_failure,
        )
        if (
            final_status == NATIVE_EVIDENCE_CANDIDATE
            and consensus_status == "PENDING"
            and not _has_only_authenticated_terminal_pending(envelope)
        ):
            # A sparse, fully evaluated reference prefix is an authenticated
            # cell-local fact.  Any other PENDING consensus is unfinished and
            # must block activation instead of becoming a search candidate.
            final_status = NATIVE_PENDING
        universe_status = final_status
        audit_status = (
            "PASS"
            if (
                final_status
                in {
                    NATIVE_EVIDENCE_CANDIDATE,
                    NATIVE_SOURCE_UNAVAILABLE,
                }
                or (
                    final_status == NATIVE_TECHNICAL_ERROR
                    and classification == "terminal_technical"
                    and not source_errors
                )
            )
            else "PENDING"
            if final_status == NATIVE_PENDING
            else "FAIL"
        )
        if final_status == NATIVE_SOURCE_UNAVAILABLE:
            failure = str(
                (availability_cells.get(timeframe) or {}).get("failure_status") or ""
            ).upper()
            reasons = [f"SOURCE_UNAVAILABLE:{failure}"]
        elif final_status == NATIVE_TECHNICAL_ERROR:
            if classification == "terminal_technical" and not source_errors:
                reasons = list(
                    dict.fromkeys(
                        ["SOURCE_TERMINAL_TECHNICAL:TECHNICAL_HISTORY_GAP", *reasons]
                    )
                )
            elif classification != "success" and not reasons:
                reasons = [
                    f"SOURCE_AVAILABILITY_INVALID:{availability_error or 'unknown'}"
                ]
                blocking.append("FAIL")
            elif source_errors:
                blocking.append("FAIL")
            elif resolution_status == "FAIL":
                # The target's own authenticated cadence exclusion is already
                # a terminal cell-local technical state.
                pass
            elif consensus_status == "ERROR":
                # Malformed or internally inconsistent consensus evidence is
                # not a cell-local market/data outcome.  It remains a global
                # activation failure so no sibling can hide broken provenance.
                blocking.append("FAIL")
            elif consensus_status == "PENDING" and not (
                _has_only_authenticated_terminal_pending(envelope)
            ):
                # An unfinished/unknown pending audit remains global.  Only
                # exact prefix-short or already-terminal sibling evidence from
                # a completed authenticated audit is terminalized cell-locally.
                blocking.append("FAIL")
            # Authenticated semantic FAIL/PENDING evidence is already carried
            # as a terminal ``technical_error`` for this exact cell.  It must
            # not erase independently authenticated sibling cells or veto the
            # whole universe.  The cell remains unavailable to search, and the
            # published terminal-state contract preserves the reason/evidence.
        elif final_status == NATIVE_PENDING:
            blocking.append("PENDING")
        reasons = list(dict.fromkeys(reasons))

        reference_binding_projection = {
            reference: dict(reference_bindings.get(reference) or {})
            for reference in references
        }
        envelope.update(
            {
                "activation_id": activation_id,
                "run_id": run_id,
                "cell_activation_status": audit_status,
                "cell_reason_codes": reasons,
                "expected_session_coverage_by_partition": coverage,
                "coverage_method": NATIVE_SESSION_COVERAGE_METHOD,
                "source_bars_sha256": source.get("bars_sha256"),
                "source_provenance_sha256": source.get("provenance_sha256"),
                "cross_resolution_reference_timeframes": list(references),
                "cross_resolution_reference_bindings": reference_binding_projection,
                "run_manifest_relative_path": str(
                    run_manifest_path.relative_to(native_root)
                ).replace("\\", "/"),
                "run_manifest_sha256": run_manifest_sha,
                "audited_at_utc": audited_at,
            }
        )
        envelope["consensus_sha256"] = canonical_sha256(
            {key: value for key, value in envelope.items() if key != "consensus_sha256"}
        )
        if classification in {"success", "terminal_technical"}:
            consensus_errors = validate_consensus_envelope(
                envelope,
                symbol=symbol,
                target_timeframe=timeframe,
                source_binding=source_binding,
                reference_bindings=reference_bindings,
            )
            if consensus_errors:
                final_status = "failed"
                universe_status = "invalid"
                audit_status = "FAIL"
                reasons = list(
                    dict.fromkeys(
                        [*reasons, *(f"CONSENSUS:{e}" for e in consensus_errors)]
                    )
                )
                envelope["cell_activation_status"] = audit_status
                envelope["cell_reason_codes"] = reasons
                envelope["consensus_sha256"] = canonical_sha256(
                    {
                        key: value
                        for key, value in envelope.items()
                        if key != "consensus_sha256"
                    }
                )
                blocking.append("FAIL")

        observed = provenance.get("observed_session_bars_by_partition")
        expected = provenance.get("expected_session_bars_by_partition")
        source_request = (
            provenance.get("request")
            if isinstance(provenance.get("request"), Mapping)
            else {}
        )
        evidence_metadata = {
            "observed_interval": {
                "start_open_utc": provenance.get("start_ts"),
                "end_open_utc": provenance.get("end_ts"),
                "rows": int((availability_cells.get(timeframe) or {}).get("rows") or 0),
            },
            "requested_interval": {
                "start_utc": source_request.get("native_fetch_start_utc"),
                "end_exclusive_utc": source_request.get(
                    "native_fetch_end_exclusive_utc"
                ),
            },
            "page_gap_diagnostics": provenance.get("coverage"),
            "pagination_sha256": _canonical_json_sha256(provenance.get("pagination")),
            "cadence_resolution_audit": resolution,
            "source_availability_attestation": source.get("availability_attestation"),
            "coverage_is_diagnostic_only": True,
        }
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
            "rows": int((availability_cells.get(timeframe) or {}).get("rows") or 0),
            "start_ts": provenance.get("start_ts"),
            "end_ts": provenance.get("end_ts"),
            "complete": audit_status == "PASS",
            "resolution_audit": resolution,
            "coverage_method": NATIVE_SESSION_COVERAGE_METHOD,
            "expected_session_coverage_by_partition": coverage,
            "source_timestamp_coverage_method": str(
                (
                    (
                        provenance.get("expected_session_coverage_details_by_partition")
                        or {}
                    ).get("method")
                )
                or ""
            ),
            "source_timestamp_coverage_by_partition": provenance.get(
                "expected_session_coverage_by_partition"
            ),
            "observed_session_bars_by_partition": observed,
            "expected_session_bars_by_partition": expected,
            "common_calendar_boundaries": (
                (provenance.get("request") or {}).get("cell_calendar_boundaries")
            ),
            "cell_calendar_boundaries": (
                (provenance.get("request") or {}).get("cell_calendar_boundaries")
            ),
            "timeframe_calendar_boundaries_sha256": (
                (provenance.get("request") or {}).get(
                    "timeframe_calendar_boundaries_sha256"
                )
            ),
            "run_id": run_id,
            "activation_id": activation_id,
            "activation_status": audit_status,
            "activation_reason_codes": reasons,
            "data_state": final_status,
            "tradeable": False,
            "statistical_sufficiency_receipt_required": True,
            "evidence_metadata": evidence_metadata,
            "parity_status": consensus_status,
            "consensus_disagreement_count": diagnostics.get(
                "window_disagreement_count", 0
            ),
            "consensus_window_diagnostics": diagnostics.get("per_window", {}),
            "source_provenance_relative_path": str(
                Path(source["provenance_path"]).resolve().relative_to(native_root)
            ).replace("\\", "/"),
            "source_provenance_sha256": source.get("provenance_sha256"),
            "cross_resolution_reference_timeframes": list(references),
            "cross_resolution_reference_bindings": reference_binding_projection,
            "run_manifest_relative_path": str(
                run_manifest_path.relative_to(native_root)
            ).replace("\\", "/"),
            "run_manifest_sha256": run_manifest_sha,
            "audited_at_utc": audited_at,
        }
        parity_path, sidecar_path = _write_audit_leaf(
            audit_root,
            symbol,
            timeframe,
            envelope,
            audited_sidecar,
        )
        parity_sha = sha256_file(parity_path)
        sidecar_sha = sha256_file(sidecar_path)
        audited_cells[timeframe] = {
            "source_validation_status": (
                "ACQUISITION_UNAVAILABLE"
                if classification == "unavailable" and not source_errors
                else "PASS"
                if not source_errors
                else "FAIL"
            ),
            "parity_status": consensus_status,
            "coverage_status": (
                "NOT_APPLICABLE"
                if classification == "unavailable"
                else "INSUFFICIENT"
                if coverage_status == "FAIL"
                else coverage_status
            ),
            "coverage_reason_codes": (
                [] if classification == "unavailable" else list(coverage_reasons)
            ),
            "coverage_is_diagnostic_only": True,
            "data_state": final_status,
            "tradeable": False,
            "statistical_sufficiency_receipt_required": True,
            "evidence_metadata": evidence_metadata,
            "resolution_status": resolution_status,
            "resolution_reason_codes": resolution_reasons,
            "expected_session_coverage_by_partition": coverage,
            "observed_session_bars_by_partition": observed,
            "expected_session_bars_by_partition": expected,
            "coverage_method": NATIVE_SESSION_COVERAGE_METHOD,
            "source_provenance_sha256": source.get("provenance_sha256"),
            "cross_resolution_reference_timeframes": list(references),
            "cross_resolution_reference_bindings": reference_binding_projection,
            "consensus_disagreement_count": diagnostics.get(
                "window_disagreement_count", 0
            ),
            "consensus_window_diagnostics": diagnostics.get("per_window", {}),
            "audited_sidecar_sha256": sidecar_sha,
            "parity_sha256": parity_sha,
        }
        symbol_entries[timeframe] = {
            "status": final_status,
            "audit_status": audit_status,
            "universe_eligibility_status": universe_status,
            "tradeable": False,
            "statistical_sufficiency_receipt_required": True,
            "evidence_metadata": evidence_metadata,
            "symbol": symbol,
            "timeframe": timeframe,
            "run_id": run_id,
            "bars_relative_path": str(
                Path(source["bars_path"]).resolve().relative_to(native_root)
            ).replace("\\", "/"),
            "bars_sha256": source.get("bars_sha256"),
            "run_manifest_sha256": run_manifest_sha,
            "sidecar_relative_path": str(sidecar_path.relative_to(native_root)).replace(
                "\\", "/"
            ),
            "sidecar_sha256": sidecar_sha,
            "parity_relative_path": str(parity_path.relative_to(native_root)).replace(
                "\\", "/"
            ),
            "parity_sha256": parity_sha,
            "coverage_method": NATIVE_SESSION_COVERAGE_METHOD,
            "cross_resolution_reference_timeframes": list(references),
            "cross_resolution_reference_bindings": reference_binding_projection,
            "consensus_status": consensus_status,
            "consensus_disagreement_count": diagnostics.get(
                "window_disagreement_count", 0
            ),
            "consensus_window_diagnostics": diagnostics.get("per_window", {}),
            "resolution_status": resolution_status,
            "resolution_reason_codes": resolution_reasons,
            "expected_session_coverage_by_partition": coverage,
            "observed_session_bars_by_partition": observed,
            "expected_session_bars_by_partition": expected,
            "reason_codes": reasons,
        }
    return symbol_entries, audited_cells, list(dict.fromkeys(blocking))


def audit_and_activate(
    native_root: Path,
    *,
    run_id: str,
    symbols: Sequence[str],
    timeframes: Sequence[str] = DEFAULT_TIMEFRAMES,
    activation_id: Optional[str] = None,
    thresholds: Optional[ParityThresholds] = None,
    tick_tolerance_overrides: Optional[Mapping[str, float]] = None,
    now: Optional[datetime] = None,
    update_active_pointer: bool = True,
) -> Dict[str, Any]:
    native_root = _safe_native_root(native_root)
    run_id = _safe_component(run_id, "run id")
    activation_id = activation_id or (
        (now or datetime.now(timezone.utc))
        .astimezone(timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    activation_id = _safe_component(activation_id, "activation id")
    canonical_symbols = tuple(
        dict.fromkeys(_safe_component(symbol, "symbol") for symbol in symbols)
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
    if not 0.0 <= float(thresholds.min_membership_ratio) <= 1.0:
        raise ValueError("min_membership_ratio must be between 0 and 1")
    if not math.isclose(
        float(thresholds.max_ohlc_mismatch_ratio),
        0.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise ValueError(
            "production max_ohlc_mismatch_ratio is pinned at 0.0; "
            "tick tolerance already handles legitimate price precision"
        )
    if not 0.0 <= float(thresholds.max_unassigned_m5_ratio) <= 1.0:
        raise ValueError("max_unassigned_m5_ratio must be between 0 and 1")
    if int(thresholds.min_compared_native_bars) <= 0:
        raise ValueError("min_compared_native_bars must be positive")
    overrides = {
        _safe_component(key, "tick-tolerance symbol"): float(value)
        for key, value in (tick_tolerance_overrides or {}).items()
    }
    started = _iso_now(now)
    audit_root = native_root / "audits" / activation_id
    if audit_root.exists():
        raise FileExistsError(
            f"immutable activation audit already exists: {audit_root}"
        )
    audit_root.mkdir(parents=True, exist_ok=False)
    previous_pointer = native_root / "ACTIVE.json"
    previous_pointer_sha = (
        sha256_file(previous_pointer) if previous_pointer.exists() else None
    )
    run_manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    run_manifest_errors: list[str] = []
    run_manifest: Dict[str, Any] = {}
    run_request: Dict[str, Any] = {}
    run_source_environment: Dict[str, Any] = {}
    run_acquisition_started_utc: Any = None
    run_acquisition_completed_utc: Any = None
    manifest_symbols_ordered: Tuple[str, ...] = ()
    manifest_timeframes_ordered: Tuple[str, ...] = ()
    if not run_manifest_path.exists():
        run_manifest_errors.append("RUN_MANIFEST_MISSING")
    else:
        try:
            run_manifest = _json(run_manifest_path)
        except Exception as exc:
            run_manifest_errors.append(f"RUN_MANIFEST_INVALID:{type(exc).__name__}")
    if run_manifest:
        run_acquisition_started_utc = run_manifest.get("acquisition_started_utc")
        run_acquisition_completed_utc = run_manifest.get("acquisition_completed_utc")
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
            timeframe_boundaries = run_request.get("timeframe_calendar_boundaries")
            timeframe_years = run_request.get("timeframe_window_years")
            timeframe_leads = run_request.get(
                "timeframe_required_evidence_lead_start_utc"
            )
            timeframe_intervals = run_request.get("timeframe_source_request_intervals")
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
        manifest_symbols_ordered = tuple(
            str(value or "").strip()
            for value in (request.get("symbols") or [])
            if str(value or "").strip()
        )
        manifest_timeframes_ordered = tuple(
            str(value or "").strip()
            for value in (request.get("timeframes") or [])
            if str(value or "").strip()
        )
        if {value.upper() for value in manifest_timeframes_ordered} != set(
            canonical_timeframes
        ):
            run_manifest_errors.append("RUN_MANIFEST_TIMEFRAME_ROSTER_MISMATCH")
    partition_windows_by_timeframe: Dict[str, Dict[str, Tuple[datetime, datetime]]] = {}
    for timeframe in canonical_timeframes:
        timeframe_windows, partition_window_errors = _partition_windows(
            run_request,
            timeframe,
        )
        partition_windows_by_timeframe[timeframe] = timeframe_windows
        run_manifest_errors.extend(
            f"{timeframe}:{reason}" for reason in partition_window_errors
        )
    run_manifest_sha = (
        sha256_file(run_manifest_path) if run_manifest_path.exists() else None
    )
    run_manifest_cells: Dict[Tuple[str, str], Mapping[str, Any]] = {}
    results = (
        run_manifest.get("results", []) if isinstance(run_manifest, Mapping) else []
    )
    if not isinstance(results, list):
        run_manifest_errors.append("RUN_MANIFEST_RESULTS_INVALID")
    for item in results if isinstance(results, list) else []:
        if isinstance(item, Mapping):
            key = (
                str(item.get("symbol", "")).strip(),
                str(item.get("timeframe", "")).upper(),
            )
            if key in run_manifest_cells:
                run_manifest_errors.append(
                    f"RUN_MANIFEST_DUPLICATE_CELL:{key[0]}|{key[1]}"
                )
            run_manifest_cells[key] = item
        else:
            run_manifest_errors.append("RUN_MANIFEST_RESULT_NOT_OBJECT")

    request_map_version = str(
        run_request.get("timeframe_request_map_schema_version") or ""
    )
    v5_consensus_mode = request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    v4_cross_resolution_mode = (
        request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4
    )
    cross_resolution_mode = v5_consensus_mode or v4_cross_resolution_mode
    if v5_consensus_mode:
        if manifest_timeframes_ordered != canonical_timeframes:
            run_manifest_errors.append("RUN_MANIFEST_TIMEFRAME_ORDER_IDENTITY_MISMATCH")
        if manifest_symbols_ordered != canonical_symbols:
            run_manifest_errors.append("RUN_MANIFEST_SYMBOL_ORDER_IDENTITY_MISMATCH")
        if bool(update_active_pointer) and canonical_symbols != tuple(
            PRODUCTION_SYMBOL_ORDER
        ):
            run_manifest_errors.append(
                "RUN_MANIFEST_PRODUCTION_SYMBOL_ORDER_IDENTITY_MISMATCH"
            )
        if bool(update_active_pointer) and len(run_manifest_cells) != (
            len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES)
        ):
            run_manifest_errors.append("RUN_MANIFEST_PRODUCTION_600_CELL_GRID_REQUIRED")
        for (cell_symbol, cell_timeframe), cell in run_manifest_cells.items():
            if (
                str(cell.get("symbol") or "") != cell_symbol
                or str(cell.get("timeframe") or "") != cell_timeframe
            ):
                run_manifest_errors.append(
                    f"RUN_MANIFEST_CELL_IDENTITY_MISMATCH:{cell_symbol}|{cell_timeframe}"
                )
            if (
                str(cell.get("relative_leaf") or "").replace("\\", "/")
                != f"{cell_symbol}/{cell_timeframe}"
            ):
                run_manifest_errors.append(
                    f"RUN_MANIFEST_CELL_PATH_IDENTITY_MISMATCH:{cell_symbol}|{cell_timeframe}"
                )
        if any(key not in set(PRODUCTION_SYMBOL_ORDER) for key in overrides):
            run_manifest_errors.append("TICK_TOLERANCE_SYMBOL_IDENTITY_INVALID")
    else:
        if {value.upper() for value in manifest_symbols_ordered} != {
            value.upper() for value in canonical_symbols
        }:
            run_manifest_errors.append("RUN_MANIFEST_SYMBOL_ROSTER_MISMATCH")
        overrides = {key.upper(): value for key, value in overrides.items()}

    production_authoritative_contract = bool(
        v5_consensus_mode
        and canonical_symbols == tuple(PRODUCTION_SYMBOL_ORDER)
        and canonical_timeframes == tuple(DEFAULT_TIMEFRAMES)
        and len(run_manifest_cells)
        == len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES)
    )
    audit_reference_map: Dict[str, str] = {}
    if v5_consensus_mode:
        run_manifest_errors.extend(
            _v5_request_errors(run_request, canonical_timeframes)
        )
        declared_references = run_request.get("timeframe_audit_reference_map")
        expected_references = {
            target: list(expected_reference_timeframes(target))
            for target in DEFAULT_TIMEFRAMES
        }
        if (
            not isinstance(declared_references, Mapping)
            or dict(declared_references) != expected_references
        ):
            run_manifest_errors.append("RUN_MANIFEST_AUDIT_REFERENCE_MAP_INVALID")
    elif v4_cross_resolution_mode:
        declared_references = run_request.get("timeframe_audit_reference_map")
        expected_references = _legacy_cross_resolution_reference_map(
            canonical_timeframes
        )
        if (
            not isinstance(declared_references, Mapping)
            or dict(declared_references) != expected_references
        ):
            run_manifest_errors.append("RUN_MANIFEST_AUDIT_REFERENCE_MAP_INVALID")
        else:
            audit_reference_map = dict(expected_references)

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
        if v5_consensus_mode:
            (
                entries[symbol],
                audited_sufficiency_cells[symbol],
                v5_blocking,
            ) = _v5_audit_symbol(
                native_root,
                audit_root=audit_root,
                symbol=symbol,
                timeframes=canonical_timeframes,
                run_id=run_id,
                activation_id=activation_id,
                run_manifest_cells=run_manifest_cells,
                run_manifest_errors=run_manifest_errors,
                run_request=run_request,
                run_source_environment=run_source_environment,
                run_acquisition_started_utc=run_acquisition_started_utc,
                run_acquisition_completed_utc=run_acquisition_completed_utc,
                run_manifest_path=run_manifest_path,
                run_manifest_sha=run_manifest_sha,
                availability=availability,
                thresholds=thresholds,
                overrides=overrides,
                audited_at=started,
            )
            activation_blocking_statuses.extend(v5_blocking)
            declared_coverage_methods.add(NATIVE_SESSION_COVERAGE_METHOD)
            continue
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
                run_acquisition_started_utc,
                run_acquisition_completed_utc,
            )
            for timeframe in canonical_timeframes
        }
        m5_source = sources.get("M5")
        m5_frame = m5_source.get("frame") if isinstance(m5_source, Mapping) else None
        m5_errors = (
            list(m5_source.get("errors", []))
            if isinstance(m5_source, Mapping)
            else ["M5_SOURCE_MISSING"]
        )
        d1_source = sources.get("D1")
        d1_frame = d1_source.get("frame") if isinstance(d1_source, Mapping) else None
        m5_availability_class = (
            str(m5_source.get("availability_class") or "")
            if isinstance(m5_source, Mapping)
            else ""
        )
        m5_failure = (
            str(
                (
                    (m5_source.get("provenance") or {}).get("failure_status")
                    if isinstance(m5_source, Mapping)
                    and isinstance(m5_source.get("provenance"), Mapping)
                    else ""
                )
                or ""
            )
            .strip()
            .upper()
        )
        for timeframe in canonical_timeframes:
            source = sources[timeframe]
            provenance = source.get("provenance", {}) or {}
            frame = source.get("frame")
            source_errors = list(source.get("errors", []) or [])
            authenticated_tolerance: Optional[float] = None
            instrument = provenance.get("instrument")
            if isinstance(instrument, Mapping):
                try:
                    candidate = float(instrument.get("parity_tick_tolerance"))
                except (TypeError, ValueError, OverflowError):
                    candidate = float("nan")
                if math.isfinite(candidate) and candidate > 0.0:
                    authenticated_tolerance = candidate
            tolerance = authenticated_tolerance
            override_tolerance = overrides.get(symbol)
            if override_tolerance is not None:
                if authenticated_tolerance is None:
                    if cross_resolution_mode:
                        source_errors.append(
                            "TICK_TOLERANCE_OVERRIDE_WITHOUT_AUTHENTICATED_BASE"
                        )
                    else:
                        tolerance = override_tolerance
                elif override_tolerance > authenticated_tolerance + 1e-12:
                    source_errors.append(
                        "TICK_TOLERANCE_OVERRIDE_WEAKENS_AUTHENTICATED_BASE"
                    )
                else:
                    tolerance = override_tolerance
            resolution_audit = audit_native_resolution(
                frame,
                symbol=symbol,
                timeframe=timeframe,
                daily_frame=d1_frame,
                tick_tolerance=tolerance,
            ).to_dict()
            resolution_status = str(resolution_audit.get("status") or "PENDING").upper()
            resolution_reasons = [
                str(reason)
                for reason in resolution_audit.get("reason_codes") or []
                if str(reason)
            ]
            source_availability_class = str(source.get("availability_class") or "")
            reference_source: Mapping[str, Any] = {}
            reference_timeframe = audit_reference_map.get(timeframe)
            if source_availability_class == "unavailable" and not source_errors:
                parity = {
                    "schema_version": (
                        CROSS_RESOLUTION_PARITY_SCHEMA_VERSION
                        if cross_resolution_mode
                        else PARITY_SCHEMA_VERSION
                    ),
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "NOT_APPLICABLE",
                    "reason_codes": ["CELL_ACQUISITION_UNAVAILABLE"],
                    "native_is_signal_authority": True,
                    "metrics": {},
                    "partitions": {},
                    "diagnostics": {},
                }
                coverage_status = "FAIL"
                coverage_reasons = ["CELL_ACQUISITION_UNAVAILABLE"]
                coverage = {partition: 0.0 for partition in NATIVE_EVIDENCE_PARTITIONS}
            elif cross_resolution_mode:
                reference_source = sources.get(reference_timeframe, {})
                reference_frame = (
                    reference_source.get("frame")
                    if isinstance(reference_source, Mapping)
                    else None
                )
                reference_errors = (
                    list(reference_source.get("errors", []) or [])
                    if isinstance(reference_source, Mapping)
                    else ["REFERENCE_SOURCE_MISSING"]
                )
                reference_class = (
                    str(reference_source.get("availability_class") or "")
                    if isinstance(reference_source, Mapping)
                    else ""
                )
                reference_failure = (
                    str(
                        (
                            (reference_source.get("provenance") or {}).get(
                                "failure_status"
                            )
                            if isinstance(reference_source, Mapping)
                            and isinstance(reference_source.get("provenance"), Mapping)
                            else ""
                        )
                        or ""
                    )
                    .strip()
                    .upper()
                )
                if not reference_timeframe:
                    parity = {
                        "schema_version": CROSS_RESOLUTION_PARITY_SCHEMA_VERSION,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "status": "PENDING",
                        "reason_codes": ["AUDIT_REFERENCE_TIMEFRAME_MISSING"],
                        "native_is_signal_authority": True,
                        "metrics": {},
                        "partitions": {},
                        "diagnostics": {},
                    }
                    coverage_status = "ERROR"
                    coverage_reasons = ["AUDIT_REFERENCE_TIMEFRAME_MISSING"]
                    coverage = dict.fromkeys(NATIVE_EVIDENCE_PARTITIONS)
                elif reference_class == "unavailable" and not reference_errors:
                    reason = (
                        f"AUDIT_REFERENCE_UNAVAILABLE:{reference_timeframe}:"
                        f"{reference_failure or 'UNSPECIFIED'}"
                    )
                    parity = {
                        "schema_version": CROSS_RESOLUTION_PARITY_SCHEMA_VERSION,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "status": "NOT_APPLICABLE",
                        "reason_codes": [reason],
                        "native_is_signal_authority": True,
                        "reference_timeframe": reference_timeframe,
                        "metrics": {},
                        "partitions": {},
                        "diagnostics": {},
                    }
                    coverage = {
                        partition: 0.0 for partition in NATIVE_EVIDENCE_PARTITIONS
                    }
                    # The target cell has authentic native history, but its
                    # separately acquired audit reference does not.  That is
                    # an unavailable *diagnostic*, not negative evidence about
                    # the target cell.  Statistical sufficiency is decided by
                    # the downstream evidence policy, so a sparse sibling may
                    # not veto an otherwise valid cell.
                    coverage_reasons = []
                    coverage_status = "PASS"
                elif frame is None or reference_frame is None or reference_errors:
                    reasons_pending = ["SOURCE_FRAME_UNAVAILABLE_FOR_CROSS_RESOLUTION"]
                    reasons_pending.extend(
                        f"{reference_timeframe}:{reason}" for reason in reference_errors
                    )
                    parity = {
                        "schema_version": CROSS_RESOLUTION_PARITY_SCHEMA_VERSION,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "status": "PENDING",
                        "reason_codes": reasons_pending,
                        "native_is_signal_authority": True,
                        "reference_timeframe": reference_timeframe,
                        "metrics": {},
                        "partitions": {},
                        "diagnostics": {},
                    }
                    coverage_status = "PENDING"
                    coverage_reasons = reasons_pending
                    coverage = dict.fromkeys(NATIVE_EVIDENCE_PARTITIONS)
                else:
                    if (
                        TIMEFRAME_SECONDS[reference_timeframe]
                        < TIMEFRAME_SECONDS[timeframe]
                    ):
                        fine_timeframe, fine_frame = (
                            reference_timeframe,
                            reference_frame,
                        )
                        coarse_timeframe, coarse_frame = timeframe, frame
                    else:
                        fine_timeframe, fine_frame = timeframe, frame
                        coarse_timeframe, coarse_frame = (
                            reference_timeframe,
                            reference_frame,
                        )
                    parity = audit_native_cross_resolution(
                        fine_frame,
                        coarse_frame,
                        symbol=symbol,
                        fine_timeframe=fine_timeframe,
                        coarse_timeframe=coarse_timeframe,
                        activation_target_timeframe=timeframe,
                        tick_tolerance=tolerance,
                        partition_windows=partition_windows_by_timeframe.get(
                            timeframe,
                            {},
                        ),
                        thresholds=thresholds,
                    )
                    parity = dict(parity)
                    parity.update(
                        {
                            "timeframe": timeframe,
                            "activation_target_timeframe": timeframe,
                            "reference_timeframe": reference_timeframe,
                            "reference_bars_sha256": reference_source.get(
                                "bars_sha256"
                            ),
                            "reference_provenance_sha256": reference_source.get(
                                "provenance_sha256"
                            ),
                        }
                    )
                    coverage_status, coverage_reasons, coverage = (
                        _cross_resolution_coverage_verdict(
                            parity,
                            NATIVE_MINIMUM_SESSION_COVERAGE,
                        )
                    )
            else:
                if (
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
                            f"M5_ACQUISITION_UNAVAILABLE:{m5_failure or 'UNSPECIFIED'}"
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
                        "reason_codes": ["SOURCE_FRAME_UNAVAILABLE_FOR_PARITY"]
                        + [f"M5:{reason}" for reason in m5_errors],
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
                    NATIVE_MINIMUM_SESSION_COVERAGE,
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
                availability_cell.get("resume_disposition"),
            )
            final_cell_status = "failed"
            cell_universe_status = "invalid"
            if availability_class == "unavailable":
                failure = (
                    str(availability_cell.get("failure_status") or "").strip().upper()
                )
                cell_status = "FAIL"
                reasons = [f"ACQUISITION_UNAVAILABLE:{failure}"]
                if source_errors:
                    reasons = [f"SOURCE:{reason}" for reason in source_errors]
                    activation_blocking_statuses.append("FAIL")
                else:
                    final_cell_status = "excluded_no_trade"
                    cell_universe_status = "excluded_no_trade"
            elif availability_class == "success":
                # Deterministic cross-resolution inconsistency is a cell-level
                # source abstention. Malformed/hash/pagination evidence and an
                # unfinished audit remain activation-blocking technical states.
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
                    coverage_reasons if coverage_status in {"ERROR", "PENDING"} else []
                )
                semantic_cross_resolution_failure = bool(
                    cross_resolution_mode
                    and str(parity.get("schema_version") or "")
                    == CROSS_RESOLUTION_PARITY_SCHEMA_VERSION
                    and str(parity.get("status") or "").upper() == "FAIL"
                )
                if semantic_cross_resolution_failure and coverage_status == "ERROR":
                    # A deterministic parity FAIL can make the derived coverage
                    # ratio unavailable (for example, too little overlap). That
                    # is a cell-level evidence abstention, not a broken audit
                    # transaction. Malformed source/hash/pagination evidence is
                    # already carried independently in ``source_errors``.
                    technical_coverage_status = "PASS"
                    technical_coverage_reasons = []
                technical_parity = dict(parity)
                if semantic_cross_resolution_failure:
                    technical_parity["status"] = "PASS"
                    technical_parity["reason_codes"] = []
                technical_status, _technical_reasons = _merge_cell_status(
                    source_errors,
                    technical_parity,
                    technical_coverage_status,
                    technical_coverage_reasons,
                )
                if technical_status in {"FAIL", "PENDING"}:
                    activation_blocking_statuses.append(technical_status)
                    final_cell_status = (
                        "failed" if technical_status == "FAIL" else "pending"
                    )
                elif (
                    coverage_status == "FAIL"
                    or resolution_status != "PASS"
                    or semantic_cross_resolution_failure
                ):
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
                    f"SOURCE_AVAILABILITY_INVALID:{availability_error or 'unknown'}"
                ]
                activation_blocking_statuses.append("FAIL")
            coverage_details = provenance.get(
                "expected_session_coverage_details_by_partition"
            )
            source_timestamp_coverage_method = (
                str(coverage_details.get("method") or "")
                if isinstance(coverage_details, Mapping)
                else ""
            )
            coverage_method = (
                NATIVE_SESSION_COVERAGE_METHOD
                if cross_resolution_mode
                else (
                    source_timestamp_coverage_method
                    or LEGACY_NATIVE_SESSION_COVERAGE_METHOD
                )
            )
            if coverage_method:
                declared_coverage_methods.add(coverage_method)
            parity = dict(parity)
            if cross_resolution_mode and reference_timeframe:
                if (
                    TIMEFRAME_SECONDS[reference_timeframe]
                    < TIMEFRAME_SECONDS[timeframe]
                ):
                    bound_fine_timeframe = reference_timeframe
                    bound_coarse_timeframe = timeframe
                else:
                    bound_fine_timeframe = timeframe
                    bound_coarse_timeframe = reference_timeframe
            else:
                bound_fine_timeframe = None
                bound_coarse_timeframe = None
            parity.update(
                {
                    "activation_id": activation_id,
                    "run_id": run_id,
                    "activation_target_timeframe": (
                        timeframe if cross_resolution_mode else None
                    ),
                    "fine_timeframe": bound_fine_timeframe,
                    "coarse_timeframe": bound_coarse_timeframe,
                    "reference_timeframe": (
                        reference_timeframe if cross_resolution_mode else None
                    ),
                    "cell_activation_status": cell_status,
                    "cell_reason_codes": reasons,
                    "expected_session_coverage_by_partition": coverage,
                    "coverage_method": coverage_method,
                    "source_timestamp_coverage_method": (
                        source_timestamp_coverage_method
                    ),
                    "minimum_session_coverage": (
                        None if v5_consensus_mode else NATIVE_MINIMUM_SESSION_COVERAGE
                    ),
                    "coverage_activation_threshold_removed": v5_consensus_mode,
                    "source_bars_sha256": source.get("bars_sha256"),
                    "source_provenance_sha256": source.get("provenance_sha256"),
                    "cross_resolution_reference_timeframe": (
                        reference_timeframe if cross_resolution_mode else None
                    ),
                    "cross_resolution_reference_bars_sha256": (
                        reference_source.get("bars_sha256")
                        if isinstance(reference_source, Mapping)
                        else None
                    ),
                    "cross_resolution_reference_provenance_sha256": (
                        reference_source.get("provenance_sha256")
                        if isinstance(reference_source, Mapping)
                        else None
                    ),
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
                "start_ts": frame.index[0].isoformat()
                if frame is not None and len(frame)
                else None,
                "end_ts": frame.index[-1].isoformat()
                if frame is not None and len(frame)
                else None,
                "complete": cell_status == "PASS",
                "resolution_audit": resolution_audit,
                "coverage_method": coverage_method,
                "expected_session_coverage_by_partition": coverage,
                "source_timestamp_coverage_method": (source_timestamp_coverage_method),
                "source_timestamp_coverage_by_partition": provenance.get(
                    "expected_session_coverage_by_partition"
                ),
                "observed_session_bars_by_partition": provenance.get(
                    "observed_session_bars_by_partition"
                ),
                "expected_session_bars_by_partition": provenance.get(
                    "expected_session_bars_by_partition"
                ),
                "common_calendar_boundaries": (
                    (provenance.get("request") or {}).get("cell_calendar_boundaries")
                    or (provenance.get("request") or {}).get(
                        "common_calendar_boundaries"
                    )
                    if isinstance(provenance.get("request"), Mapping)
                    else None
                ),
                "cell_calendar_boundaries": (
                    (provenance.get("request") or {}).get("cell_calendar_boundaries")
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
                "cross_resolution_reference_timeframe": (
                    reference_timeframe if cross_resolution_mode else None
                ),
                "cross_resolution_reference_bars_sha256": (
                    reference_source.get("bars_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
                "cross_resolution_reference_provenance_sha256": (
                    reference_source.get("provenance_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
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
                "cross_resolution_reference_timeframe": (
                    reference_timeframe if cross_resolution_mode else None
                ),
                "cross_resolution_reference_bars_sha256": (
                    reference_source.get("bars_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
                "cross_resolution_reference_provenance_sha256": (
                    reference_source.get("provenance_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
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
                "sidecar_relative_path": str(
                    sidecar_path.relative_to(native_root)
                ).replace("\\", "/"),
                "sidecar_sha256": sidecar_sha,
                "parity_relative_path": str(
                    parity_path.relative_to(native_root)
                ).replace("\\", "/"),
                "parity_sha256": parity_sha,
                "coverage_method": coverage_method,
                "cross_resolution_reference_timeframe": (
                    reference_timeframe if cross_resolution_mode else None
                ),
                "cross_resolution_reference_bars_sha256": (
                    reference_source.get("bars_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
                "cross_resolution_reference_provenance_sha256": (
                    reference_source.get("provenance_sha256")
                    if isinstance(reference_source, Mapping)
                    else None
                ),
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
    if len(declared_coverage_methods) > 1:
        activation_blocking_statuses.append("FAIL")
    activation_coverage_method = (
        next(iter(declared_coverage_methods))
        if len(declared_coverage_methods) == 1
        else (
            NATIVE_SESSION_COVERAGE_METHOD
            if cross_resolution_mode
            else LEGACY_NATIVE_SESSION_COVERAGE_METHOD
        )
    )
    if "FAIL" in activation_blocking_statuses:
        manifest_status = "failed"
    elif "PENDING" in activation_blocking_statuses:
        manifest_status = "pending"
    else:
        manifest_status = "active"
    if v5_consensus_mode:
        evidence_candidate_timeframes_by_symbol = {
            symbol: [
                timeframe
                for timeframe in canonical_timeframes
                if cell_statuses[symbol][timeframe] == NATIVE_EVIDENCE_CANDIDATE
            ]
            for symbol in canonical_symbols
        }
        source_unavailable_timeframes_by_symbol = {
            symbol: [
                timeframe
                for timeframe in canonical_timeframes
                if cell_statuses[symbol][timeframe] == NATIVE_SOURCE_UNAVAILABLE
            ]
            for symbol in canonical_symbols
        }
        symbols_with_evidence_candidates = [
            symbol
            for symbol in canonical_symbols
            if evidence_candidate_timeframes_by_symbol[symbol]
        ]
        terminal_count = sum(
            status
            in {
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
                NATIVE_TECHNICAL_ERROR,
            }
            for grid in cell_statuses.values()
            for status in grid.values()
        )
        total_cells = len(canonical_symbols) * len(canonical_timeframes)
        if manifest_status == "active" and terminal_count != total_cells:
            manifest_status = "failed"
            activation_blocking_statuses.append("FAIL")
        universe_eligibility = {
            "schema_version": UNIVERSE_AUTHENTICITY_SCHEMA,
            "decision_basis": NATIVE_AUTHENTICITY_DECISION_BASIS,
            "performance_inputs_used": False,
            "trading_verdicts_emitted": False,
            "coverage_is_diagnostic_only": True,
            "fixed_coverage_activation_threshold": None,
            "session_coverage_method": activation_coverage_method,
            "session_coverage_partitions": list(NATIVE_EVIDENCE_PARTITIONS),
            "requested_symbols": list(canonical_symbols),
            "required_timeframes": list(canonical_timeframes),
            "cell_statuses": cell_statuses,
            "terminal_states": [
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
                NATIVE_TECHNICAL_ERROR,
            ],
            "nonterminal_states": [NATIVE_PENDING],
            "evidence_candidate_timeframes_by_symbol": (
                evidence_candidate_timeframes_by_symbol
            ),
            "source_unavailable_timeframes_by_symbol": (
                source_unavailable_timeframes_by_symbol
            ),
            "symbols_with_evidence_candidates": symbols_with_evidence_candidates,
            # Compatibility projection: this means data-loadable, never
            # tradeable. Every entry carries tradeable=false as a tripwire.
            "eligible_symbols": symbols_with_evidence_candidates,
            "excluded_symbols": {},
            "statistical_sufficiency_authority": (
                "authenticated_horizon_ess_trade_power_and_multiplicity_v1"
            ),
        }
        symbols_with_active_cells = symbols_with_evidence_candidates
        fully_excluded_symbols: list[str] = []
        excluded_symbols: Dict[str, Dict[str, Any]] = {}
    else:
        # Historical forensic activations retain the v3 contract. Production
        # requires V5 and therefore always takes the v4 authenticity branch.
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
            symbol
            for symbol in canonical_symbols
            if active_timeframes_by_symbol[symbol]
        )
        fully_excluded_symbols = sorted(
            symbol
            for symbol in canonical_symbols
            if len(excluded_timeframes_by_symbol[symbol]) == len(canonical_timeframes)
        )
        excluded_symbols = {}
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
                    acquisition.get("resume_disposition"),
                )
                if classification == "unavailable":
                    acquisition_reason_codes.append(
                        f"{timeframe}:"
                        f"{str(acquisition.get('failure_status') or '').upper()}"
                    )
                else:
                    coverage_reason_codes.extend(
                        f"{timeframe}:{reason}"
                        for reason in (
                            entries[symbol][timeframe].get("reason_codes") or []
                        )
                        if str(reason)
                        and (
                            cross_resolution_mode
                            or str(reason).startswith(
                                ("SESSION_COVERAGE_", "RESOLUTION:")
                            )
                        )
                    )
            excluded_symbols[symbol] = {
                "status": "excluded_no_trade",
                "decision_basis": NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
                "reason_codes": list(
                    dict.fromkeys([*acquisition_reason_codes, *coverage_reason_codes])
                ),
                "availability_cells": availability_cells,
                "audited_sufficiency_cells": dict(
                    audited_sufficiency_cells.get(symbol) or {}
                ),
            }
        universe_eligibility = {
            "schema_version": UNIVERSE_ELIGIBILITY_SCHEMA,
            "decision_basis": NATIVE_SOURCE_EVIDENCE_DECISION_BASIS,
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
            "eligible_symbols": symbols_with_active_cells,
            "excluded_symbols": excluded_symbols,
            "invalid_symbols": {
                symbol: dict(record)
                for symbol, record in sorted(availability_decisions.items())
                if str(record.get("status") or "") == "invalid"
            },
        }
    if v5_consensus_mode:
        counts = {
            state: sum(
                str(entry.get("status") or "") == state
                for symbol_entries in entries.values()
                for entry in symbol_entries.values()
            )
            for state in (
                NATIVE_EVIDENCE_CANDIDATE,
                NATIVE_SOURCE_UNAVAILABLE,
                NATIVE_TECHNICAL_ERROR,
                NATIVE_PENDING,
            )
        }
        counts.update(
            {
                "total": len(canonical_symbols) * len(canonical_timeframes),
                "terminal": counts[NATIVE_EVIDENCE_CANDIDATE]
                + counts[NATIVE_SOURCE_UNAVAILABLE]
                + counts[NATIVE_TECHNICAL_ERROR],
                "symbols_with_evidence_candidates": len(symbols_with_active_cells),
            }
        )
    else:
        counts = {
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
        }
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "run_manifest_relative_path": str(
            run_manifest_path.relative_to(native_root)
        ).replace("\\", "/"),
        "run_manifest_sha256": run_manifest_sha,
        "status": manifest_status,
        "created_at_utc": started,
        "native_is_signal_authority": True,
        "silent_fallback_allowed": False,
        # Legacy/forensic activations may still be materialized for backwards
        # compatibility, but only the exact V5 100 x 6 contract can carry
        # production authority. Production resolvers verify this marker.
        "production_authoritative": production_authoritative_contract,
        "previous_active_pointer_sha256": previous_pointer_sha,
        "minimum_session_coverage": (
            None if v5_consensus_mode else NATIVE_MINIMUM_SESSION_COVERAGE
        ),
        "coverage_activation_threshold_removed": v5_consensus_mode,
        "parity_thresholds": {
            "min_membership_ratio": thresholds.min_membership_ratio,
            "max_ohlc_mismatch_ratio": thresholds.max_ohlc_mismatch_ratio,
            "max_unassigned_m5_ratio": thresholds.max_unassigned_m5_ratio,
            "min_compared_native_bars": thresholds.min_compared_native_bars,
        },
        "universe_eligibility": universe_eligibility,
        "entries": entries,
        "counts": counts,
    }
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    _atomic_create_json(manifest_path, manifest)
    manifest_sha = sha256_file(manifest_path)
    pointer_updated = False
    if manifest_status == "active" and bool(update_active_pointer):
        pointer = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": str(
                manifest_path.relative_to(native_root)
            ).replace("\\", "/"),
            "manifest_sha256": manifest_sha,
            "activated_at_utc": started,
            "production_authoritative": production_authoritative_contract,
        }
        active_pointer_path = native_root / "ACTIVE.json"
        with _active_pointer_lock(native_root):
            current_pointer_sha = (
                sha256_file(active_pointer_path)
                if active_pointer_path.is_file()
                else None
            )
            if current_pointer_sha != previous_pointer_sha:
                raise RuntimeError("ACTIVE_POINTER_CHANGED_DURING_AUDIT")
            _atomic_replace_json(active_pointer_path, pointer)
        pointer_updated = True
    return {
        **manifest,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "active_pointer_updated": pointer_updated,
        "audit_only": not bool(update_active_pointer),
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
        result[_safe_component(symbol.strip(), "tick-tolerance symbol")] = value
    return result


_TERMINAL_PROMOTION_RECEIPT_SCHEMA = TERMINAL_PROMOTION_RECEIPT_SCHEMA
_TERMINAL_PROMOTION_RECOVERY_SCHEMA = "fxpm-native-terminal-promotion-recovery-v1"
_TERMINAL_PROMOTION_RECOVERY_FILENAME = ".ACTIVE.promotion-recovery.json"
_TERMINAL_PROMOTION_POINTER_MAX_BYTES = 64 * 1024
_TERMINAL_PROMOTION_RECOVERY_MAX_BYTES = 256 * 1024
_TERMINAL_PROMOTION_RECOVERY_FIELDS = frozenset(
    {
        "schema_version",
        "activation_id",
        "run_id",
        "source_manifest_relative_path",
        "source_manifest_sha256",
        "promoted_manifest_relative_path",
        "promoted_manifest_sha256",
        "transition_receipt_relative_path",
        "transition_receipt_sha256",
        "previous_active_pointer_bytes_base64",
        "previous_active_pointer_sha256",
        "candidate_active_pointer_bytes_base64",
        "candidate_active_pointer_sha256",
    }
)


def _terminal_promotion_decode_pointer(
    recovery: Mapping[str, Any],
    *,
    label: str,
) -> tuple[Optional[bytes], Optional[Mapping[str, Any]], list[str]]:
    """Decode one bounded, byte-exact ACTIVE pointer from a recovery record."""

    errors: list[str] = []
    field_prefix = (
        "previous_active_pointer" if label == "PREVIOUS" else "candidate_active_pointer"
    )
    encoded = recovery.get(f"{field_prefix}_bytes_base64")
    declared_sha = str(recovery.get(f"{field_prefix}_sha256") or "").lower()
    if not isinstance(encoded, str) or len(encoded) > (
        4 * ((_TERMINAL_PROMOTION_POINTER_MAX_BYTES + 2) // 3)
    ):
        return None, None, [f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_SIZE_INVALID"]
    try:
        pointer_bytes = base64.b64decode(encoded.encode("ascii"), validate=True)
    except Exception:
        return None, None, [f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_BASE64_INVALID"]
    if (
        not pointer_bytes
        or len(pointer_bytes) > _TERMINAL_PROMOTION_POINTER_MAX_BYTES
        or base64.b64encode(pointer_bytes).decode("ascii") != encoded
    ):
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_SIZE_INVALID")
    if not re.fullmatch(r"[0-9a-f]{64}", declared_sha):
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_SHA256_INVALID")
    elif hashlib.sha256(pointer_bytes).hexdigest() != declared_sha:
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_SHA256_MISMATCH")
    try:
        pointer = json.loads(pointer_bytes.decode("utf-8"))
    except Exception:
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_JSON_INVALID")
        return pointer_bytes, None, errors
    if not isinstance(pointer, Mapping):
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_OBJECT_INVALID")
        return pointer_bytes, None, errors
    pointer = dict(pointer)
    if pointer_bytes != _canonical_json_bytes(pointer):
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_NOT_CANONICAL")
    return pointer_bytes, pointer, errors


def _terminal_promotion_pointer_identity_errors(
    pointer: Mapping[str, Any],
    *,
    label: str,
    activation_id: str,
    promoted_relative: str,
    promoted_sha256: str,
) -> list[str]:
    errors: list[str] = []
    current_fields = set(ACTIVE_POINTER_FIELDS)
    legacy_fields = current_fields - {"production_authoritative"}
    observed_fields = set(pointer)
    if label == "CANDIDATE":
        if observed_fields != current_fields:
            errors.append("TERMINAL_PROMOTION_RECOVERY_CANDIDATE_POINTER_FIELDS_INVALID")
    elif frozenset(observed_fields) not in {
        frozenset(legacy_fields),
        frozenset(current_fields),
    }:
        errors.append("TERMINAL_PROMOTION_RECOVERY_PREVIOUS_POINTER_FIELDS_INVALID")
    if str(pointer.get("schema_version") or "") != ACTIVE_POINTER_SCHEMA:
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_POINTER_SCHEMA_INVALID")
    pointer_activation_id = str(pointer.get("activation_id") or "")
    pointer_relative = str(pointer.get("manifest_relative_path") or "")
    pointer_sha = str(pointer.get("manifest_sha256") or "").lower()
    if label == "CANDIDATE":
        if pointer_activation_id != activation_id:
            errors.append("TERMINAL_PROMOTION_RECOVERY_CANDIDATE_ACTIVATION_ID_MISMATCH")
        if pointer_relative != promoted_relative:
            errors.append("TERMINAL_PROMOTION_RECOVERY_CANDIDATE_PATH_MISMATCH")
        if pointer_sha != promoted_sha256:
            errors.append("TERMINAL_PROMOTION_RECOVERY_CANDIDATE_MANIFEST_SHA_MISMATCH")
        if pointer.get("production_authoritative") is not True:
            errors.append("TERMINAL_PROMOTION_RECOVERY_CANDIDATE_AUTHORITY_INVALID")
    else:
        canonical_previous = f"activations/{pointer_activation_id}.json"
        promoted_previous = f"activations/promoted/{pointer_activation_id}.json"
        if not pointer_activation_id or pointer_relative not in {
            canonical_previous,
            promoted_previous,
        }:
            errors.append("TERMINAL_PROMOTION_RECOVERY_PREVIOUS_POINTER_IDENTITY_INVALID")
        if "production_authoritative" in pointer and (
            pointer.get("production_authoritative") is not True
        ):
            errors.append("TERMINAL_PROMOTION_RECOVERY_PREVIOUS_AUTHORITY_INVALID")
    if not re.fullmatch(r"[0-9a-f]{64}", pointer_sha):
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_MANIFEST_SHA_INVALID")
    activated_at = str(pointer.get("activated_at_utc") or "")
    try:
        parsed_activated_at = datetime.fromisoformat(
            activated_at[:-1] + "+00:00" if activated_at.endswith("Z") else activated_at
        )
    except ValueError:
        parsed_activated_at = None
    if parsed_activated_at is None or parsed_activated_at.tzinfo is None:
        errors.append(f"TERMINAL_PROMOTION_RECOVERY_{label}_ACTIVATED_AT_INVALID")
    return errors


def _terminal_promotion_recovery_record(
    *,
    activation_id: str,
    run_id: str,
    source_relative: str,
    source_sha256: str,
    promoted_relative: str,
    promoted_sha256: str,
    receipt_relative: str,
    receipt_sha256: str,
    previous_pointer_bytes: bytes,
    candidate_pointer_bytes: bytes,
) -> Dict[str, Any]:
    return {
        "schema_version": _TERMINAL_PROMOTION_RECOVERY_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "source_manifest_relative_path": source_relative,
        "source_manifest_sha256": source_sha256,
        "promoted_manifest_relative_path": promoted_relative,
        "promoted_manifest_sha256": promoted_sha256,
        "transition_receipt_relative_path": receipt_relative,
        "transition_receipt_sha256": receipt_sha256,
        "previous_active_pointer_bytes_base64": base64.b64encode(
            previous_pointer_bytes
        ).decode("ascii"),
        "previous_active_pointer_sha256": hashlib.sha256(
            previous_pointer_bytes
        ).hexdigest(),
        "candidate_active_pointer_bytes_base64": base64.b64encode(
            candidate_pointer_bytes
        ).decode("ascii"),
        "candidate_active_pointer_sha256": hashlib.sha256(
            candidate_pointer_bytes
        ).hexdigest(),
    }


def _terminal_promotion_read_recovery(
    native_root: Path,
    *,
    activation_id: str,
    run_id: str,
    expected_source_sha256: str,
) -> tuple[Optional[Dict[str, Any]], list[str]]:
    """Reopen the one non-authority promotion transaction, if present."""

    recovery_path = native_root / _TERMINAL_PROMOTION_RECOVERY_FILENAME
    if not recovery_path.exists():
        return None, []
    if not recovery_path.is_file() or recovery_path.is_symlink():
        return None, ["TERMINAL_PROMOTION_RECOVERY_PATH_UNSAFE"]
    recovery_bytes = recovery_path.read_bytes()
    if not recovery_bytes or len(recovery_bytes) > _TERMINAL_PROMOTION_RECOVERY_MAX_BYTES:
        return None, ["TERMINAL_PROMOTION_RECOVERY_SIZE_INVALID"]
    try:
        raw_recovery = json.loads(recovery_bytes.decode("utf-8"))
    except Exception:
        return None, ["TERMINAL_PROMOTION_RECOVERY_JSON_INVALID"]
    if not isinstance(raw_recovery, Mapping):
        return None, ["TERMINAL_PROMOTION_RECOVERY_OBJECT_INVALID"]
    recovery = dict(raw_recovery)
    errors: list[str] = []
    if recovery_bytes != _canonical_json_bytes(recovery):
        errors.append("TERMINAL_PROMOTION_RECOVERY_NOT_CANONICAL")
    if set(recovery) != set(_TERMINAL_PROMOTION_RECOVERY_FIELDS):
        errors.append("TERMINAL_PROMOTION_RECOVERY_FIELDS_INVALID")
    if str(recovery.get("schema_version") or "") != (
        _TERMINAL_PROMOTION_RECOVERY_SCHEMA
    ):
        errors.append("TERMINAL_PROMOTION_RECOVERY_SCHEMA_INVALID")
    if str(recovery.get("activation_id") or "") != activation_id or str(
        recovery.get("run_id") or ""
    ) != run_id:
        errors.append("TERMINAL_PROMOTION_RECOVERY_DIFFERENT_PROMOTION_PENDING")
    source_relative = f"activations/{activation_id}.json"
    promoted_relative = f"activations/promoted/{activation_id}.json"
    receipt_relative = f"transition_receipts/{activation_id}.json"
    if (
        str(recovery.get("source_manifest_relative_path") or "") != source_relative
        or str(recovery.get("source_manifest_sha256") or "").lower()
        != expected_source_sha256
    ):
        errors.append("TERMINAL_PROMOTION_RECOVERY_SOURCE_IDENTITY_MISMATCH")
    if str(recovery.get("promoted_manifest_relative_path") or "") != promoted_relative:
        errors.append("TERMINAL_PROMOTION_RECOVERY_VIEW_PATH_MISMATCH")
    if str(recovery.get("transition_receipt_relative_path") or "") != receipt_relative:
        errors.append("TERMINAL_PROMOTION_RECOVERY_RECEIPT_PATH_MISMATCH")
    promoted_sha = str(recovery.get("promoted_manifest_sha256") or "").lower()
    receipt_sha = str(recovery.get("transition_receipt_sha256") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", promoted_sha):
        errors.append("TERMINAL_PROMOTION_RECOVERY_VIEW_SHA256_INVALID")
    if not re.fullmatch(r"[0-9a-f]{64}", receipt_sha):
        errors.append("TERMINAL_PROMOTION_RECOVERY_RECEIPT_SHA256_INVALID")
    previous_bytes, previous_pointer, pointer_errors = (
        _terminal_promotion_decode_pointer(recovery, label="PREVIOUS")
    )
    errors.extend(pointer_errors)
    candidate_bytes, candidate_pointer, pointer_errors = (
        _terminal_promotion_decode_pointer(recovery, label="CANDIDATE")
    )
    errors.extend(pointer_errors)
    if isinstance(previous_pointer, Mapping):
        errors.extend(
            _terminal_promotion_pointer_identity_errors(
                previous_pointer,
                label="PREVIOUS",
                activation_id=activation_id,
                promoted_relative=promoted_relative,
                promoted_sha256=promoted_sha,
            )
        )
    if isinstance(candidate_pointer, Mapping):
        errors.extend(
            _terminal_promotion_pointer_identity_errors(
                candidate_pointer,
                label="CANDIDATE",
                activation_id=activation_id,
                promoted_relative=promoted_relative,
                promoted_sha256=promoted_sha,
            )
        )
    if errors:
        return None, list(dict.fromkeys(errors))
    assert previous_bytes is not None and candidate_bytes is not None
    return {
        "path": recovery_path,
        "bytes": recovery_bytes,
        "record": recovery,
        "previous_pointer_bytes": previous_bytes,
        "candidate_pointer_bytes": candidate_bytes,
    }, []


def _terminal_promotion_restore_and_clear_recovery(
    native_root: Path,
    recovery: Mapping[str, Any],
) -> list[str]:
    """Restore the exact predecessor and clear only this exact transaction."""

    pointer_path = native_root / "ACTIVE.json"
    recovery_path = Path(recovery["path"])
    recovery_bytes = bytes(recovery["bytes"])
    previous_bytes = bytes(recovery["previous_pointer_bytes"])
    candidate_bytes = bytes(recovery["candidate_pointer_bytes"])
    previous_sha = str(
        (recovery.get("record") or {}).get("previous_active_pointer_sha256") or ""
    ).lower()
    errors: list[str] = []
    try:
        with _active_pointer_lock(native_root):
            if (
                not recovery_path.is_file()
                or recovery_path.read_bytes() != recovery_bytes
            ):
                raise RuntimeError("recovery record changed; refusing guessed rollback")
            if (
                not re.fullmatch(r"[0-9a-f]{64}", previous_sha)
                or hashlib.sha256(previous_bytes).hexdigest() != previous_sha
            ):
                raise RuntimeError("predecessor pointer recovery hash mismatch")
            current_bytes = pointer_path.read_bytes() if pointer_path.is_file() else b""
            if current_bytes == candidate_bytes:
                _atomic_replace_bytes_with_retry(pointer_path, previous_bytes)
            elif current_bytes != previous_bytes:
                raise RuntimeError("ACTIVE pointer was superseded; not clobbering it")
            restored_bytes = pointer_path.read_bytes() if pointer_path.is_file() else b""
            if (
                restored_bytes != previous_bytes
                or hashlib.sha256(restored_bytes).hexdigest() != previous_sha
            ):
                raise RuntimeError("restored pointer bytes/hash do not match")
            _unlink_with_retry(recovery_path)
            if recovery_path.exists():
                raise RuntimeError("recovery record was not cleared")
    except BaseException as exc:  # pragma: no cover - OS-specific share locks
        errors.append(f"pointer:{type(exc).__name__}:{exc}")
    return errors


def _terminal_promotion_read_pinned_json(
    native_root: Path,
    *,
    relative_path: Any,
    expected_sha256: Any,
    label: str,
    cell_id: str,
) -> tuple[Optional[Mapping[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        path = _contained(native_root, str(relative_path or ""))
    except Exception:
        return None, [f"TERMINAL_PROMOTION_{label}_PATH_INVALID:{cell_id}"]
    if not path.is_file():
        return None, [f"TERMINAL_PROMOTION_{label}_MISSING:{cell_id}"]
    expected_sha = str(expected_sha256 or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        errors.append(f"TERMINAL_PROMOTION_{label}_SHA256_INVALID:{cell_id}")
    elif sha256_file(path) != expected_sha:
        errors.append(f"TERMINAL_PROMOTION_{label}_SHA256_MISMATCH:{cell_id}")
    try:
        payload = _json(path)
    except Exception:
        errors.append(f"TERMINAL_PROMOTION_{label}_JSON_INVALID:{cell_id}")
        return None, errors
    if not isinstance(payload, Mapping):
        errors.append(f"TERMINAL_PROMOTION_{label}_OBJECT_INVALID:{cell_id}")
        return None, errors
    return payload, errors


def _terminal_promotion_evidence_errors(
    entry: Mapping[str, Any],
    *,
    native_root: Path,
    run_id: str,
    activation_id: str,
    symbol: str,
    timeframe: str,
    run_manifest_relative_path: str,
    run_manifest_sha256: str,
) -> list[str]:
    """Reopen compact cell evidence without replaying or hashing bar payloads."""

    cell_id = f"{symbol}|{timeframe}"
    errors: list[str] = []
    sidecar, pinned_errors = _terminal_promotion_read_pinned_json(
        native_root,
        relative_path=entry.get("sidecar_relative_path"),
        expected_sha256=entry.get("sidecar_sha256"),
        label="SIDECAR",
        cell_id=cell_id,
    )
    errors.extend(pinned_errors)
    parity, pinned_errors = _terminal_promotion_read_pinned_json(
        native_root,
        relative_path=entry.get("parity_relative_path"),
        expected_sha256=entry.get("parity_sha256"),
        label="PARITY",
        cell_id=cell_id,
    )
    errors.extend(pinned_errors)
    if not isinstance(sidecar, Mapping) or not isinstance(parity, Mapping):
        return list(dict.fromkeys(errors))

    if str(sidecar.get("schema_version") or "") != NATIVE_BARS_SCHEMA:
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_SCHEMA_INVALID:{cell_id}")
    if str(sidecar.get("audited_sidecar_contract") or "") != (
        AUDITED_SIDECAR_CONTRACT
    ):
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_CONTRACT_INVALID:{cell_id}")
    for payload, label in ((sidecar, "SIDECAR"), (parity, "PARITY")):
        observed_symbol = str(payload.get("symbol") or "")
        symbol_matches = (
            observed_symbol == symbol
            if label == "SIDECAR"
            else observed_symbol.upper() == symbol.upper()
        )
        if not symbol_matches or str(payload.get("timeframe") or "").upper() != timeframe:
            errors.append(f"TERMINAL_PROMOTION_{label}_CELL_MISMATCH:{cell_id}")
        if str(payload.get("run_id") or "") != run_id:
            errors.append(f"TERMINAL_PROMOTION_{label}_RUN_ID_MISMATCH:{cell_id}")
        if str(payload.get("activation_id") or "") != activation_id:
            errors.append(f"TERMINAL_PROMOTION_{label}_ACTIVATION_MISMATCH:{cell_id}")
        if str(payload.get("run_manifest_relative_path") or "").replace(
            "\\", "/"
        ) != run_manifest_relative_path:
            errors.append(
                f"TERMINAL_PROMOTION_{label}_RUN_MANIFEST_PATH_MISMATCH:{cell_id}"
            )
        if str(payload.get("run_manifest_sha256") or "").lower() != (
            run_manifest_sha256
        ):
            errors.append(
                f"TERMINAL_PROMOTION_{label}_RUN_MANIFEST_SHA256_MISMATCH:{cell_id}"
            )

    audit_status = str(entry.get("audit_status") or "").upper()
    consensus_status = str(entry.get("consensus_status") or "").upper()
    entry_reasons = [str(value) for value in (entry.get("reason_codes") or ())]
    if str(sidecar.get("activation_status") or "").upper() != audit_status:
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_STATUS_MISMATCH:{cell_id}")
    if str(parity.get("cell_activation_status") or "").upper() != audit_status:
        errors.append(f"TERMINAL_PROMOTION_PARITY_STATUS_MISMATCH:{cell_id}")
    if list(sidecar.get("activation_reason_codes") or ()) != entry_reasons:
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_REASON_MISMATCH:{cell_id}")
    if list(parity.get("cell_reason_codes") or ()) != entry_reasons:
        errors.append(f"TERMINAL_PROMOTION_PARITY_REASON_MISMATCH:{cell_id}")
    if str(sidecar.get("parity_status") or "").upper() != consensus_status:
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_CONSENSUS_MISMATCH:{cell_id}")
    if str(parity.get("status") or "").upper() != consensus_status:
        errors.append(f"TERMINAL_PROMOTION_PARITY_CONSENSUS_MISMATCH:{cell_id}")

    expected_bars_sha = str(entry.get("bars_sha256") or "").lower()
    provenance_sha = str(sidecar.get("source_provenance_sha256") or "").lower()
    if str(sidecar.get("sha256") or "").lower() != expected_bars_sha:
        errors.append(f"TERMINAL_PROMOTION_SIDECAR_BARS_MISMATCH:{cell_id}")
    if str(parity.get("source_bars_sha256") or "").lower() != expected_bars_sha:
        errors.append(f"TERMINAL_PROMOTION_PARITY_BARS_MISMATCH:{cell_id}")
    if str(parity.get("source_provenance_sha256") or "").lower() != provenance_sha:
        errors.append(f"TERMINAL_PROMOTION_PARITY_PROVENANCE_MISMATCH:{cell_id}")
    expected_provenance_relative = str(
        (Path(str(entry.get("bars_relative_path") or "")).parent / "provenance.json").as_posix()
    )
    provenance_relative = str(
        sidecar.get("source_provenance_relative_path") or ""
    ).replace("\\", "/")
    if provenance_relative != expected_provenance_relative:
        errors.append(f"TERMINAL_PROMOTION_PROVENANCE_PATH_MISMATCH:{cell_id}")
    provenance: Optional[Mapping[str, Any]] = None
    try:
        provenance_path = _contained(native_root, provenance_relative)
    except Exception:
        errors.append(f"TERMINAL_PROMOTION_PROVENANCE_PATH_INVALID:{cell_id}")
    else:
        if not provenance_path.is_file():
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_MISSING:{cell_id}")
        elif not re.fullmatch(r"[0-9a-f]{64}", provenance_sha):
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_SHA256_INVALID:{cell_id}")
        elif sha256_file(provenance_path) != provenance_sha:
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_SHA256_MISMATCH:{cell_id}")
        else:
            try:
                raw_provenance = _json(provenance_path)
            except Exception:
                errors.append(f"TERMINAL_PROMOTION_PROVENANCE_JSON_INVALID:{cell_id}")
            else:
                if not isinstance(raw_provenance, Mapping):
                    errors.append(
                        f"TERMINAL_PROMOTION_PROVENANCE_OBJECT_INVALID:{cell_id}"
                    )
                else:
                    provenance = raw_provenance
    if isinstance(provenance, Mapping):
        if str(provenance.get("schema_version") or "") != SCHEMA_VERSION:
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_SCHEMA_INVALID:{cell_id}")
        if str(provenance.get("acquisition_contract_version") or "") != (
            ACQUISITION_CONTRACT_VERSION
        ):
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_CONTRACT_INVALID:{cell_id}")
        if provenance.get("immutable_record") is not True:
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_NOT_IMMUTABLE:{cell_id}")
        if str(provenance.get("run_id") or "") != run_id:
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_RUN_ID_MISMATCH:{cell_id}")
        if (
            str(provenance.get("symbol") or "") != symbol
            or str(provenance.get("canonical_symbol") or "") != symbol
            or str(provenance.get("timeframe") or "").upper() != timeframe
        ):
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_CELL_MISMATCH:{cell_id}")
        if str(provenance.get("sha256") or "").lower() != expected_bars_sha:
            errors.append(f"TERMINAL_PROMOTION_PROVENANCE_BARS_MISMATCH:{cell_id}")

    source_binding = parity.get("source_binding")
    reference_bindings = parity.get("reference_bindings")
    if not isinstance(source_binding, Mapping) or str(
        source_binding.get("bars_sha256") or ""
    ).lower() != expected_bars_sha or str(
        source_binding.get("provenance_sha256") or ""
    ).lower() != provenance_sha:
        errors.append(f"TERMINAL_PROMOTION_SOURCE_BINDING_MISMATCH:{cell_id}")
    expected_reference_bindings = entry.get("cross_resolution_reference_bindings")
    if reference_bindings != expected_reference_bindings:
        errors.append(f"TERMINAL_PROMOTION_REFERENCE_BINDING_MISMATCH:{cell_id}")
    for payload, label in ((sidecar, "SIDECAR"), (parity, "PARITY")):
        if payload.get("cross_resolution_reference_bindings") != (
            expected_reference_bindings
        ):
            errors.append(
                f"TERMINAL_PROMOTION_{label}_REFERENCE_BINDING_MISMATCH:{cell_id}"
            )
    if isinstance(source_binding, Mapping) and isinstance(reference_bindings, Mapping):
        errors.extend(
            f"TERMINAL_PROMOTION_CONSENSUS:{cell_id}:{value}"
            for value in validate_consensus_envelope(
                parity,
                symbol=symbol,
                target_timeframe=timeframe,
                source_binding=source_binding,
                reference_bindings=reference_bindings,
            )
        )

    if str(entry.get("status") or "").lower() == NATIVE_EVIDENCE_CANDIDATE:
        if not isinstance(source_binding, Mapping) or str(
            source_binding.get("availability_state") or ""
        ).upper() != "AVAILABLE":
            errors.append(
                f"TERMINAL_PROMOTION_CANDIDATE_SOURCE_BINDING_INVALID:{cell_id}"
            )
        if audit_status != "PASS":
            errors.append(f"TERMINAL_PROMOTION_CANDIDATE_AUDIT_INVALID:{cell_id}")
        if str(entry.get("resolution_status") or "").upper() != "PASS":
            errors.append(f"TERMINAL_PROMOTION_CANDIDATE_RESOLUTION_INVALID:{cell_id}")
        if consensus_status not in {"PASS", "PENDING", "NOT_APPLICABLE"}:
            errors.append(f"TERMINAL_PROMOTION_CANDIDATE_CONSENSUS_INVALID:{cell_id}")
        if consensus_status == "PENDING" and not (
            _has_only_authenticated_terminal_pending(parity)
        ):
            errors.append(
                f"TERMINAL_PROMOTION_CANDIDATE_PENDING_UNAUTHENTICATED:{cell_id}"
            )
        if any(
            reason.startswith(
                (
                    "SOURCE:",
                    "CONSENSUS:",
                    "SOURCE_AVAILABILITY_INVALID:",
                    "CROSS_RESOLUTION_CONSENSUS_INTEGRITY_ERROR",
                )
            )
            for reason in entry_reasons
        ):
            errors.append(f"TERMINAL_PROMOTION_CANDIDATE_INTEGRITY_ERROR:{cell_id}")
    return list(dict.fromkeys(errors))


def _completed_terminal_promotion_errors(
    manifest: Mapping[str, Any],
    *,
    native_root: Path,
    run_id: str,
    activation_id: str,
) -> list[str]:
    """Validate a completed non-active V5 artifact without replaying bar data."""

    errors: list[str] = []
    if str(manifest.get("schema_version") or "") != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("TERMINAL_PROMOTION_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("activation_id") or "") != activation_id:
        errors.append("TERMINAL_PROMOTION_ACTIVATION_ID_MISMATCH")
    if str(manifest.get("run_id") or "") != run_id:
        errors.append("TERMINAL_PROMOTION_RUN_ID_MISMATCH")
    if str(manifest.get("status") or "") != "failed":
        errors.append("TERMINAL_PROMOTION_SOURCE_STATUS_NOT_FAILED")
    if manifest.get("production_authoritative") is not True:
        errors.append("TERMINAL_PROMOTION_PRODUCTION_AUTHORITY_MISSING")
    if manifest.get("native_is_signal_authority") is not True:
        errors.append("TERMINAL_PROMOTION_SIGNAL_AUTHORITY_INVALID")
    if manifest.get("silent_fallback_allowed") is not False:
        errors.append("TERMINAL_PROMOTION_SILENT_FALLBACK_INVALID")

    run_manifest_relative_path = str(
        manifest.get("run_manifest_relative_path") or ""
    ).replace("\\", "/")
    expected_run_manifest_relative_path = f"runs/{run_id}/run_manifest.json"
    run_manifest_sha256 = str(manifest.get("run_manifest_sha256") or "").lower()
    if run_manifest_relative_path != expected_run_manifest_relative_path:
        errors.append("TERMINAL_PROMOTION_RUN_MANIFEST_PATH_INVALID")
    try:
        run_manifest_path = _contained(native_root, run_manifest_relative_path)
    except Exception:
        errors.append("TERMINAL_PROMOTION_RUN_MANIFEST_PATH_UNSAFE")
    else:
        if not run_manifest_path.is_file():
            errors.append("TERMINAL_PROMOTION_RUN_MANIFEST_MISSING")
        elif not re.fullmatch(r"[0-9a-f]{64}", run_manifest_sha256):
            errors.append("TERMINAL_PROMOTION_RUN_MANIFEST_SHA256_INVALID")
        elif sha256_file(run_manifest_path) != run_manifest_sha256:
            errors.append("TERMINAL_PROMOTION_RUN_MANIFEST_SHA256_MISMATCH")

    contract = manifest.get("universe_eligibility")
    if not isinstance(contract, Mapping):
        return [*errors, "TERMINAL_PROMOTION_UNIVERSE_CONTRACT_MISSING"]
    if str(contract.get("schema_version") or "") != UNIVERSE_AUTHENTICITY_SCHEMA:
        errors.append("TERMINAL_PROMOTION_UNIVERSE_SCHEMA_INVALID")
    if tuple(contract.get("requested_symbols") or ()) != tuple(PRODUCTION_SYMBOL_ORDER):
        errors.append("TERMINAL_PROMOTION_SYMBOL_ROSTER_INVALID")
    if tuple(contract.get("required_timeframes") or ()) != tuple(DEFAULT_TIMEFRAMES):
        errors.append("TERMINAL_PROMOTION_TIMEFRAME_ROSTER_INVALID")
    expected_terminal_states = (
        NATIVE_EVIDENCE_CANDIDATE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_TECHNICAL_ERROR,
    )
    if tuple(contract.get("terminal_states") or ()) != expected_terminal_states:
        errors.append("TERMINAL_PROMOTION_TERMINAL_STATES_INVALID")
    if tuple(contract.get("nonterminal_states") or ()) != (NATIVE_PENDING,):
        errors.append("TERMINAL_PROMOTION_NONTERMINAL_STATES_INVALID")

    entries = manifest.get("entries")
    statuses = contract.get("cell_statuses")
    if not isinstance(entries, Mapping) or set(entries) != set(PRODUCTION_SYMBOL_ORDER):
        errors.append("TERMINAL_PROMOTION_ENTRY_SYMBOL_GRID_INVALID")
        entries = {}
    if not isinstance(statuses, Mapping) or set(statuses) != set(
        PRODUCTION_SYMBOL_ORDER
    ):
        errors.append("TERMINAL_PROMOTION_STATUS_SYMBOL_GRID_INVALID")
        statuses = {}

    observed_counts = {state: 0 for state in (*expected_terminal_states, NATIVE_PENDING)}
    candidate_symbols = 0
    integrity_reason_prefixes = (
        "SOURCE:",
        "CONSENSUS:",
        "SOURCE_AVAILABILITY_INVALID:",
        "CROSS_RESOLUTION_CONSENSUS_INTEGRITY_ERROR",
    )
    for symbol in PRODUCTION_SYMBOL_ORDER:
        entry_grid = entries.get(symbol)
        status_grid = statuses.get(symbol)
        if not isinstance(entry_grid, Mapping) or set(entry_grid) != set(
            DEFAULT_TIMEFRAMES
        ):
            errors.append(f"TERMINAL_PROMOTION_ENTRY_TIMEFRAME_GRID_INVALID:{symbol}")
            entry_grid = {}
        if not isinstance(status_grid, Mapping) or set(status_grid) != set(
            DEFAULT_TIMEFRAMES
        ):
            errors.append(f"TERMINAL_PROMOTION_STATUS_TIMEFRAME_GRID_INVALID:{symbol}")
            status_grid = {}
        symbol_has_candidate = False
        for timeframe in DEFAULT_TIMEFRAMES:
            cell_id = f"{symbol}|{timeframe}"
            state = str(status_grid.get(timeframe) or "").strip().lower()
            entry = entry_grid.get(timeframe)
            if state not in expected_terminal_states:
                errors.append(
                    f"TERMINAL_PROMOTION_CELL_NOT_TERMINAL:{cell_id}:{state or 'missing'}"
                )
                continue
            observed_counts[state] += 1
            symbol_has_candidate = symbol_has_candidate or state == NATIVE_EVIDENCE_CANDIDATE
            if not isinstance(entry, Mapping):
                errors.append(f"TERMINAL_PROMOTION_ENTRY_MISSING:{cell_id}")
                continue
            if str(entry.get("status") or "").strip().lower() != state:
                errors.append(f"TERMINAL_PROMOTION_ENTRY_STATUS_MISMATCH:{cell_id}")
            errors.extend(
                _terminal_promotion_evidence_errors(
                    entry,
                    native_root=native_root,
                    run_id=run_id,
                    activation_id=activation_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    run_manifest_relative_path=run_manifest_relative_path,
                    run_manifest_sha256=run_manifest_sha256,
                )
            )
            if state == NATIVE_EVIDENCE_CANDIDATE and str(
                entry.get("audit_status") or ""
            ).upper() != "PASS":
                errors.append(f"TERMINAL_PROMOTION_CANDIDATE_AUDIT_INVALID:{cell_id}")
            if state == NATIVE_TECHNICAL_ERROR:
                consensus_status = str(entry.get("consensus_status") or "").upper()
                resolution_status = str(entry.get("resolution_status") or "").upper()
                reasons = [str(value) for value in (entry.get("reason_codes") or ())]
                if consensus_status == "ERROR" or any(
                    reason.startswith(integrity_reason_prefixes) for reason in reasons
                ):
                    errors.append(
                        f"TERMINAL_PROMOTION_INTEGRITY_ERROR_PRESENT:{cell_id}"
                    )
                source_terminal = any(
                    reason.startswith("SOURCE_TERMINAL_TECHNICAL:")
                    for reason in reasons
                )
                if (
                    consensus_status == "PENDING"
                    and resolution_status != "FAIL"
                    and not source_terminal
                ):
                    parity_relative = str(entry.get("parity_relative_path") or "")
                    try:
                        parity_path = (
                            _contained(native_root, parity_relative)
                            if native_root is not None
                            else None
                        )
                    except Exception:
                        parity_path = None
                    if (
                        parity_path is None
                        or not parity_path.is_file()
                        or sha256_file(parity_path)
                        != str(entry.get("parity_sha256") or "").lower()
                    ):
                        errors.append(
                            f"TERMINAL_PROMOTION_PENDING_PARITY_INVALID:{cell_id}"
                        )
                    else:
                        try:
                            parity = _json(parity_path)
                        except Exception:
                            parity = {}
                        if not _has_only_authenticated_terminal_pending(parity):
                            errors.append(
                                f"TERMINAL_PROMOTION_UNRESOLVED_PENDING_PRESENT:{cell_id}"
                            )
        candidate_symbols += int(symbol_has_candidate)

    total = len(PRODUCTION_SYMBOL_ORDER) * len(DEFAULT_TIMEFRAMES)
    observed_counts[NATIVE_PENDING] = 0
    expected_counts = {
        **observed_counts,
        "total": total,
        "terminal": sum(observed_counts[state] for state in expected_terminal_states),
        "symbols_with_evidence_candidates": candidate_symbols,
    }
    if dict(manifest.get("counts") or {}) != expected_counts:
        errors.append("TERMINAL_PROMOTION_COUNTS_MISMATCH")
    if expected_counts["terminal"] != total or candidate_symbols != len(
        PRODUCTION_SYMBOL_ORDER
    ):
        errors.append("TERMINAL_PROMOTION_COMPLETE_USABLE_GRID_REQUIRED")
    return list(dict.fromkeys(errors))


def _terminal_promotion_postcheck(
    native_root: Path,
    *,
    activation_id: str,
    promoted_manifest_path: Path,
    promoted_manifest_sha256: str,
) -> Dict[str, Any]:
    """Run every production consumer gate against the exact promoted view."""

    resolved = resolve_active_native_universe(
        native_root.parent,
        PRODUCTION_SYMBOL_ORDER,
        required_timeframes=DEFAULT_TIMEFRAMES,
        verify_exclusion_evidence=True,
        production_contract_required=True,
    )
    errors = list(resolved.errors)
    if resolved.activation_id != activation_id:
        errors.append("TERMINAL_PROMOTION_POST_ACTIVATION_ID_MISMATCH")
    if (
        not isinstance(resolved.activation_manifest_path, Path)
        or resolved.activation_manifest_path.resolve() != promoted_manifest_path.resolve()
    ):
        errors.append("TERMINAL_PROMOTION_POST_MANIFEST_PATH_MISMATCH")
    if resolved.activation_manifest_sha256 != promoted_manifest_sha256:
        errors.append("TERMINAL_PROMOTION_POST_MANIFEST_SHA256_MISMATCH")
    if tuple(resolved.requested_symbols) != tuple(PRODUCTION_SYMBOL_ORDER):
        errors.append("TERMINAL_PROMOTION_POST_SYMBOL_ROSTER_MISMATCH")
    if tuple(resolved.required_timeframes) != tuple(DEFAULT_TIMEFRAMES):
        errors.append("TERMINAL_PROMOTION_POST_TIMEFRAME_ROSTER_MISMATCH")
    if errors:
        raise RuntimeError(";".join(list(dict.fromkeys(errors))))

    from pm_portfolio_replay import active_native_portfolio_contract
    from pm_production_anchor_plan import (
        derive_production_anchor_plan,
        verify_production_anchor_plan_receipt,
    )

    anchor_receipt = derive_production_anchor_plan(native_root.parent)
    anchor_body = verify_production_anchor_plan_receipt(
        native_root.parent,
        anchor_receipt,
    )
    if (
        anchor_body.get("activation_id") != activation_id
        or anchor_body.get("activation_manifest_sha256")
        != promoted_manifest_sha256
    ):
        raise RuntimeError("TERMINAL_PROMOTION_POST_ANCHOR_IDENTITY_MISMATCH")
    portfolio_contract, portfolio_errors = active_native_portfolio_contract(
        native_root.parent,
        PRODUCTION_SYMBOL_ORDER,
        timeframes=DEFAULT_TIMEFRAMES,
        # The exact full-grid evidence replay already passed above in this
        # control flow; avoid only the duplicate raw-bar pass.
        verify_exclusion_evidence=False,
    )
    if portfolio_errors:
        raise RuntimeError(
            "TERMINAL_PROMOTION_POST_PORTFOLIO_INVALID:"
            + ";".join(portfolio_errors)
        )
    if (
        portfolio_contract.get("activation_id") != activation_id
        or portfolio_contract.get("activation_manifest_sha256")
        != promoted_manifest_sha256
        or tuple(portfolio_contract.get("symbols") or ())
        != tuple(PRODUCTION_SYMBOL_ORDER)
        or tuple(portfolio_contract.get("timeframes") or ())
        != tuple(DEFAULT_TIMEFRAMES)
    ):
        raise RuntimeError("TERMINAL_PROMOTION_POST_PORTFOLIO_IDENTITY_MISMATCH")
    return {
        "anchor_plan_receipt_sha256": anchor_body.get("receipt_sha256"),
        "portfolio_activation_roster_sha256": portfolio_contract.get(
            "activation_roster_sha256"
        ),
    }


def _promote_completed_terminal_activation_transaction(
    native_root: Path,
    *,
    run_id: str,
    activation_id: str,
    expected_manifest_sha256: str,
    recovery: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Promote one byte-pinned terminal grid through one atomic pointer swap.

    The failed source manifest remains immutable.  A separately persisted view
    differs only by ``status=active``; an exact transition receipt binds the
    source, view, and predecessor pointer.  Orphan views/receipts authorize
    nothing because every consumer starts from ``ACTIVE.json``.
    """

    native_root = _safe_native_root(native_root)
    run_id = _safe_component(run_id, "run id")
    activation_id = _safe_component(activation_id, "activation id")
    expected_sha = str(expected_manifest_sha256 or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError("expected manifest sha256 must be 64 lowercase hex characters")
    source_manifest_path = native_root / "activations" / f"{activation_id}.json"
    if not source_manifest_path.is_file():
        raise FileNotFoundError(
            f"activation manifest missing: {source_manifest_path}"
        )
    source_manifest_bytes = source_manifest_path.read_bytes()
    observed_sha = hashlib.sha256(source_manifest_bytes).hexdigest()
    if observed_sha != expected_sha:
        raise ValueError("TERMINAL_PROMOTION_MANIFEST_SHA256_MISMATCH")
    manifest = _json(source_manifest_path)
    errors = _completed_terminal_promotion_errors(
        manifest,
        native_root=native_root,
        run_id=run_id,
        activation_id=activation_id,
    )
    promoted = dict(manifest)
    promoted["status"] = "active"
    promoted_bytes = _canonical_json_bytes(promoted)
    promoted_sha = hashlib.sha256(promoted_bytes).hexdigest()
    source_relative = str(source_manifest_path.relative_to(native_root)).replace(
        "\\", "/"
    )
    promoted_manifest_path = (
        native_root / "activations" / "promoted" / f"{activation_id}.json"
    )
    promoted_relative = str(promoted_manifest_path.relative_to(native_root)).replace(
        "\\", "/"
    )
    previous_pointer_sha = str(
        manifest.get("previous_active_pointer_sha256") or ""
    ).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", previous_pointer_sha):
        errors.append("TERMINAL_PROMOTION_PREVIOUS_POINTER_SHA256_INVALID")

    receipt = {
        "schema_version": _TERMINAL_PROMOTION_RECEIPT_SCHEMA,
        "activation_id": activation_id,
        "run_id": run_id,
        "source_manifest_relative_path": source_relative,
        "source_manifest_sha256": observed_sha,
        "promoted_manifest_relative_path": promoted_relative,
        "promoted_manifest_sha256": promoted_sha,
        "previous_active_pointer_sha256": previous_pointer_sha,
        "counts": dict(promoted["counts"]),
        "transition": TERMINAL_PROMOTION_TRANSITION,
        "data_reacquired": False,
        "cell_evidence_replayed": False,
    }
    receipt_path = native_root / "transition_receipts" / f"{activation_id}.json"
    receipt_relative = str(receipt_path.relative_to(native_root)).replace("\\", "/")
    receipt_bytes = _canonical_json_bytes(receipt)
    receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()

    pointer_path = native_root / "ACTIVE.json"
    current_pointer_bytes = pointer_path.read_bytes() if pointer_path.is_file() else b""
    original_pointer_bytes = current_pointer_bytes
    pointer_bytes = b""
    pointer_was_already_active = False
    committed_reentry = False
    current_is_previous = False
    if recovery is not None:
        original_pointer_bytes = bytes(recovery["previous_pointer_bytes"])
        pointer_bytes = bytes(recovery["candidate_pointer_bytes"])
        recovery_record = recovery.get("record") or {}
        if hashlib.sha256(original_pointer_bytes).hexdigest() != previous_pointer_sha:
            errors.append("TERMINAL_PROMOTION_RECOVERY_PREVIOUS_POINTER_MISMATCH")
        if (
            str(recovery_record.get("promoted_manifest_sha256") or "").lower()
            != promoted_sha
            or str(recovery_record.get("transition_receipt_sha256") or "").lower()
            != receipt_sha
        ):
            errors.append("TERMINAL_PROMOTION_RECOVERY_ARTIFACT_IDENTITY_MISMATCH")
        if current_pointer_bytes == original_pointer_bytes:
            current_is_previous = True
        elif current_pointer_bytes == pointer_bytes:
            pointer_was_already_active = True
        else:
            errors.append("TERMINAL_PROMOTION_RECOVERY_ACTIVE_POINTER_DIVERGED")
    else:
        observed_pointer_sha = (
            hashlib.sha256(current_pointer_bytes).hexdigest()
            if current_pointer_bytes
            else None
        )
        if observed_pointer_sha == previous_pointer_sha:
            current_is_previous = True
        elif current_pointer_bytes:
            try:
                current_pointer = json.loads(current_pointer_bytes.decode("utf-8"))
            except Exception:
                current_pointer = None
            candidate_errors = (
                _terminal_promotion_pointer_identity_errors(
                    current_pointer,
                    label="CANDIDATE",
                    activation_id=activation_id,
                    promoted_relative=promoted_relative,
                    promoted_sha256=promoted_sha,
                )
                if isinstance(current_pointer, Mapping)
                else ["invalid"]
            )
            if isinstance(current_pointer, Mapping) and current_pointer_bytes != (
                _canonical_json_bytes(current_pointer)
            ):
                candidate_errors.append("not canonical")
            if candidate_errors:
                errors.append("TERMINAL_PROMOTION_PREVIOUS_POINTER_SHA256_MISMATCH")
            else:
                pointer_bytes = current_pointer_bytes
                pointer_was_already_active = True
                committed_reentry = True
        else:
            errors.append("TERMINAL_PROMOTION_PREVIOUS_POINTER_MISSING")
    if current_is_previous:
        prior = resolve_active_native_universe(
            native_root.parent,
            PRODUCTION_SYMBOL_ORDER,
            required_timeframes=DEFAULT_TIMEFRAMES,
            verify_exclusion_evidence=False,
            production_contract_required=False,
        )
        if prior.errors:
            errors.append("TERMINAL_PROMOTION_PREVIOUS_POINTER_INVALID")
    if errors:
        raise ValueError(";".join(list(dict.fromkeys(errors))))

    for path, payload, collision in (
        (
            promoted_manifest_path,
            promoted_bytes,
            "terminal promoted manifest collision",
        ),
        (receipt_path, receipt_bytes, "terminal promotion receipt collision"),
    ):
        if path.exists():
            if path.read_bytes() != payload:
                raise FileExistsError(collision)
        elif committed_reentry or recovery is not None:
            raise FileNotFoundError(
                f"TERMINAL_PROMOTION_RECONCILIATION_ARTIFACT_MISSING:{path}"
            )
        elif path == promoted_manifest_path:
            _atomic_create_json(path, promoted)
        else:
            _atomic_create_json(path, receipt)

    staged_view_errors = validate_terminal_promotion_view(
        native_root,
        activation_id=activation_id,
        view_path=promoted_manifest_path,
        view_sha256=promoted_sha,
    )
    if staged_view_errors:
        raise ValueError(
            "TERMINAL_PROMOTION_STAGED_VIEW_INVALID:"
            + ";".join(staged_view_errors)
        )

    transaction_recovery: Optional[Dict[str, Any]] = (
        dict(recovery) if recovery is not None else None
    )
    recovery_record_established = recovery is not None
    recovery_path = native_root / _TERMINAL_PROMOTION_RECOVERY_FILENAME
    if not pointer_bytes:
        pointer = {
            "schema_version": ACTIVE_POINTER_SCHEMA,
            "activation_id": activation_id,
            "manifest_relative_path": promoted_relative,
            "manifest_sha256": promoted_sha,
            "activated_at_utc": _iso_now(),
            "production_authoritative": True,
        }
        pointer_bytes = _canonical_json_bytes(pointer)
        recovery_record = _terminal_promotion_recovery_record(
            activation_id=activation_id,
            run_id=run_id,
            source_relative=source_relative,
            source_sha256=observed_sha,
            promoted_relative=promoted_relative,
            promoted_sha256=promoted_sha,
            receipt_relative=receipt_relative,
            receipt_sha256=receipt_sha,
            previous_pointer_bytes=original_pointer_bytes,
            candidate_pointer_bytes=pointer_bytes,
        )
        recovery_bytes = _canonical_json_bytes(recovery_record)
        if len(recovery_bytes) > _TERMINAL_PROMOTION_RECOVERY_MAX_BYTES:
            raise ValueError("TERMINAL_PROMOTION_RECOVERY_SIZE_INVALID")
        constructed_recovery_errors: list[str] = []
        for label in ("PREVIOUS", "CANDIDATE"):
            _, recovered_pointer, pointer_errors = (
                _terminal_promotion_decode_pointer(
                    recovery_record,
                    label=label,
                )
            )
            constructed_recovery_errors.extend(pointer_errors)
            if isinstance(recovered_pointer, Mapping):
                constructed_recovery_errors.extend(
                    _terminal_promotion_pointer_identity_errors(
                        recovered_pointer,
                        label=label,
                        activation_id=activation_id,
                        promoted_relative=promoted_relative,
                        promoted_sha256=promoted_sha,
                    )
                )
        if constructed_recovery_errors:
            raise ValueError(";".join(list(dict.fromkeys(constructed_recovery_errors))))
        transaction_recovery = {
            "path": recovery_path,
            "bytes": recovery_bytes,
            "record": recovery_record,
            "previous_pointer_bytes": original_pointer_bytes,
            "candidate_pointer_bytes": pointer_bytes,
        }

    try:
        with _active_pointer_lock(native_root):
            if source_manifest_path.read_bytes() != source_manifest_bytes:
                raise RuntimeError("TERMINAL_PROMOTION_SOURCE_CHANGED_DURING_PRECHECK")
            expected_current_pointer = (
                pointer_bytes if pointer_was_already_active else original_pointer_bytes
            )
            if (
                not pointer_path.is_file()
                or pointer_path.read_bytes() != expected_current_pointer
            ):
                raise RuntimeError("TERMINAL_PROMOTION_POINTER_CAS_MISMATCH")
            if (
                promoted_manifest_path.read_bytes() != promoted_bytes
                or receipt_path.read_bytes() != receipt_bytes
            ):
                raise RuntimeError("TERMINAL_PROMOTION_STAGED_VIEW_CHANGED")
            if committed_reentry:
                if recovery_path.exists():
                    raise RuntimeError("TERMINAL_PROMOTION_UNEXPECTED_RECOVERY_RECORD")
            else:
                assert transaction_recovery is not None
                expected_recovery_bytes = bytes(transaction_recovery["bytes"])
                if recovery is None:
                    if recovery_path.exists():
                        raise RuntimeError(
                            "TERMINAL_PROMOTION_DIFFERENT_RECOVERY_RECORD_PENDING"
                        )
                    # This immutable, fsynced record is created under the same
                    # lock immediately before the sole authority mutation.
                    _atomic_create_json(
                        recovery_path,
                        transaction_recovery["record"],
                    )
                    recovery_record_established = True
                if (
                    not recovery_path.is_file()
                    or recovery_path.read_bytes() != expected_recovery_bytes
                ):
                    raise RuntimeError("TERMINAL_PROMOTION_RECOVERY_RECORD_CHANGED")
            if not pointer_was_already_active:
                _atomic_replace_bytes_with_retry(pointer_path, pointer_bytes)

        postcheck = _terminal_promotion_postcheck(
            native_root,
            activation_id=activation_id,
            promoted_manifest_path=promoted_manifest_path,
            promoted_manifest_sha256=promoted_sha,
        )
        # Linearize success after every consumer has validated the candidate.
        # A cooperating publisher can supersede ACTIVE while the expensive
        # postcheck runs; never report this promotion as active in that case.
        with _active_pointer_lock(native_root):
            if source_manifest_path.read_bytes() != source_manifest_bytes:
                raise RuntimeError("TERMINAL_PROMOTION_SOURCE_CHANGED_DURING_POSTCHECK")
            if (
                promoted_manifest_path.read_bytes() != promoted_bytes
                or receipt_path.read_bytes() != receipt_bytes
            ):
                raise RuntimeError("TERMINAL_PROMOTION_STAGED_VIEW_CHANGED_DURING_POSTCHECK")
            if not pointer_path.is_file() or pointer_path.read_bytes() != pointer_bytes:
                raise RuntimeError("TERMINAL_PROMOTION_POINTER_CHANGED_DURING_POSTCHECK")
            if transaction_recovery is not None:
                expected_recovery_bytes = bytes(transaction_recovery["bytes"])
                if (
                    not recovery_path.is_file()
                    or recovery_path.read_bytes() != expected_recovery_bytes
                ):
                    raise RuntimeError(
                        "TERMINAL_PROMOTION_RECOVERY_RECORD_CHANGED_DURING_POSTCHECK"
                    )
                _unlink_with_retry(recovery_path)
            if recovery_path.exists():
                raise RuntimeError("TERMINAL_PROMOTION_RECOVERY_RECORD_NOT_CLEARED")
    except BaseException as promotion_error:
        rollback_errors: list[str] = []
        if transaction_recovery is not None and recovery_record_established:
            rollback_errors.extend(
                _terminal_promotion_restore_and_clear_recovery(
                    native_root,
                    transaction_recovery,
                )
            )
        if source_manifest_path.read_bytes() != source_manifest_bytes:
            rollback_errors.append("source_manifest:immutable_bytes_changed")
        if rollback_errors:
            raise RuntimeError(
                "TERMINAL_PROMOTION_ROLLBACK_FAILED:" + "|".join(rollback_errors)
            ) from promotion_error
        raise
    result = {
        "status": "active",
        "activation_id": activation_id,
        "run_id": run_id,
        "manifest_path": str(promoted_manifest_path),
        "manifest_sha256": promoted_sha,
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_sha256": observed_sha,
        "active_pointer_updated": not pointer_was_already_active,
        "reconciled_recovery": recovery is not None,
        "idempotent_committed_reentry": committed_reentry,
        "recovery_record_cleared": not recovery_path.exists(),
        "counts": dict(promoted["counts"]),
        "transition_receipt_path": str(receipt_path),
        "data_reacquired": False,
        "cell_evidence_replayed": False,
        **postcheck,
    }
    if recovery_path.exists():
        raise RuntimeError("TERMINAL_PROMOTION_RECOVERY_RECORD_NOT_CLEARED")
    return result


def promote_completed_terminal_activation(
    native_root: Path,
    *,
    run_id: str,
    activation_id: str,
    expected_manifest_sha256: str,
) -> Dict[str, Any]:
    """Promote or reconcile one durable pointer-only activation transaction."""

    native_root = _safe_native_root(native_root)
    run_id = _safe_component(run_id, "run id")
    activation_id = _safe_component(activation_id, "activation id")
    expected_sha = str(expected_manifest_sha256 or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError("expected manifest sha256 must be 64 lowercase hex characters")
    recovery, recovery_errors = _terminal_promotion_read_recovery(
        native_root,
        activation_id=activation_id,
        run_id=run_id,
        expected_source_sha256=expected_sha,
    )
    if recovery_errors:
        raise ValueError(";".join(recovery_errors))
    if recovery is not None:
        pointer_path = native_root / "ACTIVE.json"
        current_pointer_bytes = (
            pointer_path.read_bytes() if pointer_path.is_file() else b""
        )
        if current_pointer_bytes not in {
            bytes(recovery["previous_pointer_bytes"]),
            bytes(recovery["candidate_pointer_bytes"]),
        }:
            raise RuntimeError("TERMINAL_PROMOTION_RECOVERY_ACTIVE_POINTER_DIVERGED")
    try:
        return _promote_completed_terminal_activation_transaction(
            native_root,
            run_id=run_id,
            activation_id=activation_id,
            expected_manifest_sha256=expected_sha,
            recovery=recovery,
        )
    except BaseException as promotion_error:
        if recovery is not None:
            recovery_path = Path(recovery["path"])
            pointer_path = native_root / "ACTIVE.json"
            current_pointer_bytes = (
                pointer_path.read_bytes() if pointer_path.is_file() else b""
            )
            previous_bytes = bytes(recovery["previous_pointer_bytes"])
            candidate_bytes = bytes(recovery["candidate_pointer_bytes"])
            # Missing record + exact candidate is the commit point after full
            # postcheck.  A retry will take the idempotent committed path.
            if recovery_path.exists():
                rollback_errors = _terminal_promotion_restore_and_clear_recovery(
                    native_root,
                    recovery,
                )
                if rollback_errors:
                    raise RuntimeError(
                        "TERMINAL_PROMOTION_ROLLBACK_FAILED:"
                        + "|".join(rollback_errors)
                    ) from promotion_error
            elif current_pointer_bytes == candidate_bytes:
                raise RuntimeError(
                    "TERMINAL_PROMOTION_ROLLBACK_FAILED:"
                    "pointer:recovery record missing; refusing guessed rollback"
                ) from promotion_error
            elif current_pointer_bytes != previous_bytes:
                raise RuntimeError(
                    "TERMINAL_PROMOTION_ROLLBACK_FAILED:"
                    "pointer:ACTIVE pointer was superseded; not clobbering it"
                ) from promotion_error
        raise


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
        "--promote-completed-activation",
        metavar="ACTIVATION_ID",
        help=(
            "atomically promote an already-audited complete terminal V5 grid "
            "without reacquisition or evidence replay"
        ),
    )
    parser.add_argument("--expected-activation-manifest-sha256")
    parser.add_argument("--tick-tolerance", action="append", default=[])
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help=(
            "execute the full immutable audit and write its manifest, but never "
            "replace data/native/ACTIVE.json"
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    native_root = _safe_native_root(Path(args.native_root))
    if args.promote_completed_activation:
        if args.execute or args.audit_only or args.activation_id or args.symbols:
            raise ValueError(
                "--promote-completed-activation cannot be combined with audit execution options"
            )
        if args.tick_tolerance:
            raise ValueError("terminal promotion forbids tick-tolerance overrides")
        if not args.expected_activation_manifest_sha256:
            raise ValueError(
                "--expected-activation-manifest-sha256 is required for terminal promotion"
            )
        result = promote_completed_terminal_activation(
            native_root,
            run_id=args.run_id,
            activation_id=args.promote_completed_activation,
            expected_manifest_sha256=args.expected_activation_manifest_sha256,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    run_root = native_root / "runs" / args.run_id
    run_manifest_path = run_root / "run_manifest.json"
    run_manifest = _json(run_manifest_path) if run_manifest_path.exists() else {}
    request = (
        run_manifest.get("request")
        if isinstance(run_manifest.get("request"), Mapping)
        else {}
    )
    manifest_symbols = [
        str(value).strip()
        for value in (request.get("symbols") or [])
        if str(value).strip()
    ]
    if not manifest_symbols:
        manifest_symbols = sorted(
            {
                str(item.get("symbol") or "").strip()
                for item in (run_manifest.get("results") or [])
                if isinstance(item, Mapping) and str(item.get("symbol") or "").strip()
            }
        )
    symbols = [str(value).strip() for value in (args.symbols or manifest_symbols)]
    request_map_version = str(request.get("timeframe_request_map_schema_version") or "")
    v5_identity = request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    symbols_match_manifest = (
        tuple(symbols) == tuple(manifest_symbols)
        if v5_identity
        else {value.upper() for value in symbols}
        == {value.upper() for value in manifest_symbols}
    )
    if args.symbols and manifest_symbols and not symbols_match_manifest:
        raise ValueError(
            "--symbols must equal the immutable acquisition run's requested symbol roster; "
            "activation may not hide failed or short-history symbols"
        )
    manifest_timeframes = [
        str(value).strip()
        for value in (request.get("timeframes") or [])
        if str(value).strip()
    ]
    v5_timeframes_match = tuple(args.timeframes) == tuple(manifest_timeframes)
    legacy_timeframes_match = {str(value).upper() for value in args.timeframes} == {
        value.upper() for value in manifest_timeframes
    }
    if manifest_timeframes and not (
        v5_timeframes_match if v5_identity else legacy_timeframes_match
    ):
        raise ValueError(
            "--timeframes must equal the immutable acquisition run's requested timeframe roster"
        )
    production_contract = bool(
        v5_identity
        and tuple(manifest_timeframes) == tuple(DEFAULT_TIMEFRAMES)
        and tuple(manifest_symbols) == tuple(PRODUCTION_SYMBOL_ORDER)
        and tuple(symbols) == tuple(PRODUCTION_SYMBOL_ORDER)
    )
    if args.execute and not args.audit_only and not production_contract:
        raise ValueError(
            "production activation requires the V5 request map, exact "
            "PRODUCTION_SYMBOL_ORDER, and exact six-timeframe roster; "
            "legacy/subset runs are audit-only"
        )
    if args.execute and not args.audit_only and args.tick_tolerance:
        raise ValueError(
            "production activation forbids tick-tolerance overrides; the immutable "
            "instrument point/tick size is authoritative"
        )
    plan = {
        "mode": (
            "audit_only"
            if args.execute and args.audit_only
            else ("execute" if args.execute else "plan_only")
        ),
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
        thresholds=ParityThresholds(),
        tick_tolerance_overrides=_parse_tolerances(args.tick_tolerance),
        update_active_pointer=not bool(args.audit_only),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return (
        0
        if result["status"] == "active"
        else (3 if result["status"] == "pending" else 4)
    )


if __name__ == "__main__":
    raise SystemExit(main())
