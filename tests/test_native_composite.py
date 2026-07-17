from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pytest

from pm_native_composite import (
    AcquisitionShortfall,
    BrokerSourceIdentity,
    CompositeCapacityError,
    CompositeCell,
    CompositeIntegrityError,
    CompositeLimits,
    CompositePathError,
    CompositeRunManifest,
    CompositeSchemaError,
    CompositeSegment,
    CURRENT_ACQUISITION_CONTRACT_VERSION,
    CURRENT_PAGINATION_CONTRACT_VERSION,
    CURRENT_PROVENANCE_SCHEMA_VERSION,
    CURRENT_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    CURRENT_WINDOW_CONTRACT_VERSION,
    LEGACY_ABSENT_PAGINATION_CONTRACT,
    LEGACY_MISSING_ATTEMPT_LEDGER,
    LegacyAdoptionAttestation,
    NativeCompositeError,
    NativeCompositeResolver,
)
from pm_native_consensus import CONSENSUS_POLICY, REFERENCE_GRAPH
from pm_native_lifecycle import LifecycleLimits, NativeLifecycleAuthority


CSV_HEADER = (
    "time,Open,High,Low,Close,Volume,Spread,RealVolume,"
    "_SOURCE_FIRST_TS,_SOURCE_LAST_TS,_BAR_CLOSE_TS"
)
PROVENANCE_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Spread",
    "RealVolume",
    "_SOURCE_FIRST_TS",
    "_SOURCE_LAST_TS",
    "_BAR_CLOSE_TS",
]
RUN_SCHEMA = "fxpm.native_history_run_manifest.v1"
LEGACY_ACQUISITION = "2026-07-10-native-calendar-acquisition-v1"
TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
TIMEFRAME_SECONDS = {
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
    "H1": 3_600,
    "H4": 14_400,
    "D1": 86_400,
}
PAGE_DAYS = {"M5": 30, "M15": 90, "M30": 180, "H1": 365, "H4": 1460, "D1": 3650}
AUDIT_WINDOWS = (
    "strategy_full_context",
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
)
LIFECYCLE_AUTH_KEY = b"fxpm-composite-publication-test!!"
LIFECYCLE_PROVISIONER = "pytest-native-composite-v1"


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _csv_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")


def _bar_line(
    opened: datetime,
    *,
    duration_seconds: int | None = None,
    source_last_delta_ns: int = 0,
    newline: bytes = b"\n",
) -> bytes:
    closed = opened + timedelta(seconds=duration_seconds)
    source_last_second = closed - timedelta(seconds=1)
    final_ns = 999_999_999 + source_last_delta_ns
    if not 0 <= final_ns <= 999_999_999:
        raise ValueError("test source-last adjustment leaves its second")
    source_last = (
        source_last_second.strftime("%Y-%m-%d %H:%M:%S")
        + f".{final_ns:09d}+00:00"
    )
    values = (
        _csv_time(opened),
        "1.10",
        "1.20",
        "1.00",
        "1.10",
        "100",
        "2",
        "0",
        _csv_time(opened),
        source_last,
        _csv_time(closed),
    )
    return ",".join(values).encode("ascii") + newline


def _source_environment() -> dict[str, object]:
    return {
        "account": {
            "company": "Test Broker Pty Ltd",
            "server": "TestBroker-Demo",
            "login": "123456",
        },
        "terminal": {"build": 6000},
    }


def _identity() -> BrokerSourceIdentity:
    environment = _source_environment()
    return BrokerSourceIdentity(
        broker="Test Broker Pty Ltd",
        server="TestBroker-Demo",
        account_id="123456",
        source_kind="broker_native",
        source_environment_sha256=_sha(_canonical_bytes(environment)),
    )


def _request_map_hash_payload(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": request.get("timeframe_request_map_schema_version"),
        "window_contract_version": request.get("window_contract_version"),
        "common_as_of_utc": request.get("common_as_of_utc"),
        "timeframe_window_years": request.get("timeframe_window_years"),
        "timeframe_calendar_boundaries": request.get(
            "timeframe_calendar_boundaries"
        ),
        "timeframe_required_evidence_lead_start_utc": request.get(
            "timeframe_required_evidence_lead_start_utc"
        ),
        "timeframe_source_request_intervals": request.get(
            "timeframe_source_request_intervals"
        ),
        "timeframe_strategy_evidence_intervals": request.get(
            "timeframe_strategy_evidence_intervals"
        ),
        "timeframe_audit_reference_map": request.get(
            "timeframe_audit_reference_map"
        ),
        "timeframe_audit_consumer_map": request.get(
            "timeframe_audit_consumer_map"
        ),
        "timeframe_audit_windows": request.get("timeframe_audit_windows"),
        "timeframe_audit_reference_intervals": request.get(
            "timeframe_audit_reference_intervals"
        ),
        "timeframe_audit_only_intervals": request.get(
            "timeframe_audit_only_intervals"
        ),
        "timeframe_consensus_policy": request.get("timeframe_consensus_policy"),
    }


def _rehash_request_map(request: dict[str, Any]) -> None:
    request["timeframe_calendar_boundaries_sha256"] = _sha(
        _canonical_bytes(request["timeframe_calendar_boundaries"])
    )
    request["timeframe_request_map_sha256"] = _sha(
        _canonical_bytes(_request_map_hash_payload(request))
    )


def _v5_common_request(start: datetime, end: datetime) -> dict[str, object]:
    interval = {"start_utc": _z(start), "end_exclusive_utc": _z(end)}
    boundary = {"start_ts": _z(start), "end_ts": _z(end)}
    years = {
        timeframe: {
            "optimizer_train": 1,
            "tuning_validation": 1,
            "final_lockbox": 1,
        }
        for timeframe in TIMEFRAMES
    }
    boundaries = {
        timeframe: {
            "optimizer_train": dict(boundary),
            "tuning_validation": dict(boundary),
            "final_lockbox": dict(boundary),
        }
        for timeframe in TIMEFRAMES
    }
    references = {
        timeframe: list(REFERENCE_GRAPH[timeframe]) for timeframe in TIMEFRAMES
    }
    consumers = {
        timeframe: [
            target for target in TIMEFRAMES if timeframe in REFERENCE_GRAPH[target]
        ]
        for timeframe in TIMEFRAMES
    }
    audit_windows = {
        timeframe: {name: dict(interval) for name in AUDIT_WINDOWS}
        for timeframe in TIMEFRAMES
    }
    reference_intervals = {
        timeframe: {
            reference: {name: dict(interval) for name in AUDIT_WINDOWS}
            for reference in REFERENCE_GRAPH[timeframe]
        }
        for timeframe in TIMEFRAMES
    }
    common: dict[str, object] = {
        "window_contract_version": CURRENT_WINDOW_CONTRACT_VERSION,
        "timeframe_request_map_schema_version": (
            CURRENT_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        ),
        "horizon_years": 1,
        "requested_information_cutoff_utc": _z(end),
        "requested_horizon_start_utc": _z(start),
        "common_requested_start_utc": _z(start),
        "required_evidence_lead_start_utc": _z(start),
        "warmup_days": 1.0,
        "safety_days": 1.0,
        "closed_bar_slack_seconds": 86_400,
        "common_as_of_utc": _z(end),
        "boundary_tolerance_days": 7.0,
        "embargo_days": 7.0,
        "timeframe_window_years": years,
        "timeframe_calendar_boundaries": boundaries,
        "timeframe_calendar_boundaries_sha256": _sha(
            _canonical_bytes(boundaries)
        ),
        "timeframe_required_evidence_lead_start_utc": {
            timeframe: _z(start) for timeframe in TIMEFRAMES
        },
        "timeframe_source_request_intervals": {
            timeframe: dict(interval) for timeframe in TIMEFRAMES
        },
        "timeframe_strategy_evidence_intervals": {
            timeframe: dict(interval) for timeframe in TIMEFRAMES
        },
        "timeframe_audit_reference_map": references,
        "timeframe_audit_consumer_map": consumers,
        "timeframe_audit_windows": audit_windows,
        "timeframe_audit_reference_intervals": reference_intervals,
        "timeframe_audit_only_intervals": {
            timeframe: [] for timeframe in TIMEFRAMES
        },
        "timeframe_consensus_policy": dict(CONSENSUS_POLICY),
    }
    _rehash_request_map(common)
    return common


@dataclass(frozen=True)
class BuiltSegment:
    segment: CompositeSegment
    bars_path: Path
    provenance_path: Path
    manifest_path: Path


def _rewrite_json(
    path: Path, mutator: Callable[[dict[str, Any]], None]
) -> bytes:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    mutator(payload)
    raw = _canonical_bytes(payload)
    path.write_bytes(raw)
    return raw


def _rewrite_provenance(
    built: BuiltSegment,
    mutator: Callable[[dict[str, Any]], None],
) -> CompositeSegment:
    raw = _rewrite_json(built.provenance_path, mutator)
    return replace(
        built.segment,
        provenance_sha256=_sha(raw),
        provenance_bytes=len(raw),
    )


def _rewrite_manifest(
    segment: CompositeSegment,
    built: BuiltSegment,
    mutator: Callable[[dict[str, Any]], None],
) -> CompositeSegment:
    raw = _rewrite_json(built.manifest_path, mutator)
    return replace(segment, origin_manifest_sha256=_sha(raw))


def _build_segment(
    root: Path,
    *,
    run_id: str,
    role: str,
    interval_start: datetime,
    interval_end: datetime,
    row_times: list[datetime],
    identity: BrokerSourceIdentity,
    legacy: bool = False,
    timeframe: str = "M5",
    newline: bytes = b"\n",
    duration_seconds: int = 300,
    row_duration_seconds: list[int] | None = None,
    source_last_delta_ns: int = 0,
    status: str = "success",
    original_request_start: datetime | None = None,
    original_request_end: datetime | None = None,
    declared_shortfall: tuple[datetime, datetime, str] | None = None,
) -> BuiltSegment:
    timeframe = timeframe.upper()
    nominal_seconds = TIMEFRAME_SECONDS[timeframe]
    durations = row_duration_seconds or [
        duration_seconds or nominal_seconds
    ] * len(row_times)
    if len(durations) != len(row_times):
        raise ValueError("one test duration is required for every row")
    leaf = root / "runs" / run_id / "EURUSD" / timeframe
    leaf.mkdir(parents=True)
    bars_path = leaf / "bars.csv"
    bars_raw = CSV_HEADER.encode("ascii") + newline + b"".join(
        _bar_line(
            value,
            duration_seconds=row_duration,
            source_last_delta_ns=source_last_delta_ns,
            newline=newline,
        )
        for value, row_duration in zip(row_times, durations)
    )
    bars_path.write_bytes(bars_raw)
    bars_sha = _sha(bars_raw)
    acquisition_contract = (
        LEGACY_ACQUISITION if legacy else CURRENT_ACQUISITION_CONTRACT_VERSION
    )
    pagination_contract = (
        LEGACY_ABSENT_PAGINATION_CONTRACT
        if legacy
        else CURRENT_PAGINATION_CONTRACT_VERSION
    )
    pagination: dict[str, object]
    request_start = original_request_start or interval_start
    request_end = original_request_end or interval_end
    if legacy:
        pagination = {
            "attempts_total": 1,
            "empty_pages": [],
            "failed_pages": [],
            "truncated_pages": [],
        }
        request = {
            "native_fetch_start_utc": _z(request_start),
            "native_fetch_end_exclusive_utc": _z(request_end),
        }
    else:
        page = {
            "start_utc": _z(request_start),
            "end_exclusive_utc": _z(request_end),
            "attempts": 1,
            "copy_rates_range_attempts": 2,
        }
        truncated_pages: list[dict[str, object]] = []
        if status == "partial":
            expected_last = request_end - timedelta(seconds=nominal_seconds)
            truncated_pages.append(
                {
                    **page,
                    "actual_first_open_utc": _z(row_times[0]),
                    "actual_last_open_utc": _z(row_times[-1]),
                    "start_delay_seconds": max(
                        0.0, (row_times[0] - request_start).total_seconds()
                    ),
                    "end_lag_seconds": max(
                        0.0, (expected_last - row_times[-1]).total_seconds()
                    ),
                }
            )
        pagination = {
            "contract_version": CURRENT_PAGINATION_CONTRACT_VERSION,
            "page_attempts": [page],
            "page_span_seconds": PAGE_DAYS[timeframe] * 86_400,
            "attempts_total": 1,
            "copy_rates_range_attempts_total": 2,
            "pages_requested": 1,
            "pages_with_data": 1,
            "retries_per_page": 3,
            "empty_pages": [],
            "failed_pages": [],
            "truncated_pages": truncated_pages,
        }
        common = _v5_common_request(request_start, request_end)
        request = {
            **common,
            "window_years": dict(common["timeframe_window_years"][timeframe]),
            "common_calendar_boundaries": dict(
                common["timeframe_calendar_boundaries"][timeframe]
            ),
            "cell_timeframe": timeframe,
            "cell_timeframe_window_years": dict(
                common["timeframe_window_years"][timeframe]
            ),
            "cell_calendar_boundaries": dict(
                common["timeframe_calendar_boundaries"][timeframe]
            ),
            "cell_required_evidence_lead_start_utc": common[
                "timeframe_required_evidence_lead_start_utc"
            ][timeframe],
            "source_request_interval": {
                "start_utc": _z(request_start),
                "end_exclusive_utc": _z(request_end),
            },
            "strategy_evidence_interval": dict(
                common["timeframe_strategy_evidence_intervals"][timeframe]
            ),
            "audit_reference_timeframes": list(
                common["timeframe_audit_reference_map"][timeframe]
            ),
            "audit_consumer_targets": list(
                common["timeframe_audit_consumer_map"][timeframe]
            ),
            "audit_windows": dict(common["timeframe_audit_windows"][timeframe]),
            "audit_reference_intervals": dict(
                common["timeframe_audit_reference_intervals"][timeframe]
            ),
            "audit_only_intervals": list(
                common["timeframe_audit_only_intervals"][timeframe]
            ),
            "native_fetch_start_utc": _z(request_start),
            "native_fetch_end_exclusive_utc": _z(request_end),
            "last_fully_closed_bar_open_utc": _z(row_times[-1]),
            "alignment_mode": (
                "broker_observed_successor_or_nominal_close_v1"
                if timeframe in {"H4", "D1"}
                else "fixed_utc_native_boundary_v1"
            ),
            "successor_sentinel_fetch_slack_seconds": (
                1 if timeframe in {"H4", "D1"} else 0
            ),
        }
    failure_status = None if status == "success" else "partial_history"
    provenance = {
        "schema_version": CURRENT_PROVENANCE_SCHEMA_VERSION,
        "run_id": run_id,
        "symbol": "EURUSD",
        "canonical_symbol": "EURUSD",
        "broker_symbol": "EURUSD",
        "timeframe": timeframe,
        "source_kind": "broker_native",
        "source_environment": _source_environment(),
        "acquisition_contract_version": acquisition_contract,
        "immutable_record": True,
        "complete": status == "success",
        "status": status,
        "failure_status": failure_status,
        "rows": len(row_times),
        "sha256": bars_sha,
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": bars_sha,
            "bytes": len(bars_raw),
            "rows": len(row_times),
        },
        "columns": PROVENANCE_COLUMNS,
        "bar_close_column": "_BAR_CLOSE_TS",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "timestamp_semantics": "bar_open_utc",
        "timezone": "UTC",
        "timeframe_seconds": nominal_seconds,
        "start_ts": _z(row_times[0]),
        "end_ts": _z(row_times[-1]),
        "request": request,
        "pagination": pagination,
    }
    provenance_raw = _canonical_bytes(provenance)
    provenance_path = leaf / "provenance.json"
    provenance_path.write_bytes(provenance_raw)
    provenance_sha = _sha(provenance_raw)
    result = {
        "symbol": "EURUSD",
        "timeframe": timeframe,
        "status": status,
        "failure_status": failure_status,
        "relative_leaf": f"EURUSD/{timeframe}",
        "rows": len(row_times),
        "sha256": bars_sha,
        "resume_disposition": "newly_acquired",
    }
    results = [result]
    if not legacy:
        results.extend(
            {
                "symbol": "EURUSD",
                "timeframe": other_timeframe,
                "status": "failed",
                "failure_status": "synthetic_unavailable",
                "relative_leaf": f"EURUSD/{other_timeframe}",
                "rows": 0,
                "sha256": None,
                "resume_disposition": "newly_acquired",
            }
            for other_timeframe in TIMEFRAMES
            if other_timeframe != timeframe
        )
    counts = {
        "success": int(status == "success"),
        "partial": int(status == "partial"),
        "failed": 0 if legacy else 5,
        "total": 1 if legacy else 6,
    }
    origin_manifest = {
        "schema_version": RUN_SCHEMA,
        "run_id": run_id,
        "complete": True,
        "immutable_record": True,
        "source_environment": _source_environment(),
        "counts": counts,
        "results": results,
    }
    if not legacy:
        origin_manifest["request"] = {
            **common,
            "symbols": ["EURUSD"],
            "timeframes": list(TIMEFRAMES),
            "retries": 3,
            "window_years": dict(common["timeframe_window_years"][timeframe]),
            "common_calendar_boundaries": dict(
                common["timeframe_calendar_boundaries"][timeframe]
            ),
        }
        origin_manifest["resume"] = {
            "enabled": False,
            "newly_acquired_cells": 6,
            "reused_cells": 0,
            "stale_staging_removed": 0,
        }
    manifest_raw = _canonical_bytes(origin_manifest)
    manifest_path = root / "runs" / run_id / "run_manifest.json"
    manifest_path.write_bytes(manifest_raw)
    manifest_sha = _sha(manifest_raw)
    adoption = None
    if legacy:
        adoption = LegacyAdoptionAttestation(
            origin_manifest_sha256=manifest_sha,
            bars_sha256=bars_sha,
            provenance_sha256=provenance_sha,
            bars_bytes=len(bars_raw),
            provenance_bytes=len(provenance_raw),
            rows=len(row_times),
            original_provenance_schema_version=CURRENT_PROVENANCE_SCHEMA_VERSION,
            original_acquisition_contract_version=acquisition_contract,
            original_pagination_contract_version=pagination_contract,
            origin_manifest_declared_immutable=True,
        )
    preceding_shortfall = None
    if declared_shortfall is not None:
        shortfall_start, shortfall_end, reason = declared_shortfall
        preceding_shortfall = AcquisitionShortfall(
            start_utc=_z(shortfall_start),
            end_exclusive_utc=_z(shortfall_end),
            reason_code=reason,
            evidence_sha256=provenance_sha,
        )
    segment = CompositeSegment(
        segment_id=f"{run_id}-segment",
        role=role,
        origin_contract_kind="LEGACY_ADOPTED" if legacy else "V5_NATIVE",
        origin_run_id=run_id,
        origin_manifest_schema_version=RUN_SCHEMA,
        origin_manifest_relative_path=f"runs/{run_id}/run_manifest.json",
        origin_manifest_sha256=manifest_sha,
        bars_relative_path=f"runs/{run_id}/EURUSD/{timeframe}/bars.csv",
        bars_sha256=bars_sha,
        bars_bytes=len(bars_raw),
        rows=len(row_times),
        provenance_relative_path=f"runs/{run_id}/EURUSD/{timeframe}/provenance.json",
        provenance_sha256=provenance_sha,
        provenance_bytes=len(provenance_raw),
        origin_provenance_schema_version=CURRENT_PROVENANCE_SCHEMA_VERSION,
        origin_acquisition_contract_version=acquisition_contract,
        origin_pagination_contract_version=pagination_contract,
        interval_start_utc=_z(interval_start),
        interval_end_exclusive_utc=_z(interval_end),
        first_timestamp_utc=_z(row_times[0]),
        last_timestamp_utc=_z(row_times[-1]),
        first_close=1.1,
        last_close=1.1,
        symbol="EURUSD",
        timeframe=timeframe,
        source_identity=identity,
        preceding_shortfall=preceding_shortfall,
        legacy_adoption=adoption,
    )
    return BuiltSegment(segment, bars_path, provenance_path, manifest_path)


def _resolver(
    root: Path, identity: BrokerSourceIdentity, *segments: CompositeSegment
) -> NativeCompositeResolver:
    timeframe = segments[0].timeframe
    if any(item.timeframe != timeframe for item in segments):
        raise ValueError("test resolver cells require one timeframe")
    manifest = CompositeRunManifest(
        composite_run_id="composite-test-v1",
        created_at_utc="2026-07-13T00:00:00Z",
        source_identity=identity,
        cells=(CompositeCell("EURUSD", timeframe, tuple(segments)),),
    )
    return NativeCompositeResolver(root, manifest)


def _lifecycle_limits() -> LifecycleLimits:
    return LifecycleLimits(
        max_hot_bytes=16 * 1024 * 1024,
        max_cold_bytes=16 * 1024 * 1024,
        min_volume_free_bytes=1,
        max_hot_files=10_000,
        max_cold_files=10_000,
        max_complete_hot_runs=20,
        max_inflight_runs=4,
        max_operations=200,
        max_active_operations=16,
        max_payload_bytes=64 * 1024,
        max_files_per_operation=2_000,
        max_pins=32,
        max_pin_payload_bytes=64 * 1024,
        max_pointer_bytes=64 * 1024,
        max_scan_entries=20_000,
        max_db_bytes=2 * 1024 * 1024,
        page_size=4096,
        journal_size_limit=64 * 1024,
        db_busy_deadline_seconds=2.0,
        lock_deadline_seconds=4.0,
        lease_seconds=30.0,
    )


def _pin_composite(
    resolver: NativeCompositeResolver,
    *,
    native_root: Path,
    lifecycle_root: Path,
    pin_id: str = "composite-publication-v1",
) -> NativeLifecycleAuthority:
    cold_root = lifecycle_root / "cold"
    cold_root.mkdir(parents=True)
    authority = NativeLifecycleAuthority(
        native_root=native_root,
        cold_root=cold_root,
        state_root=lifecycle_root / "state",
        limits=_lifecycle_limits(),
        auth_key=LIFECYCLE_AUTH_KEY,
        provision=True,
        provisioning_identity=LIFECYCLE_PROVISIONER,
    )
    roots = [
        native_root / Path(*item.relative_path.split("/"))
        for item in resolver.protection_graph.roots
    ]
    with authority.lease("composite-publication-test") as lease:
        lease.pin_evidence(pin_id, roots)
    resolver.authorize_publication(authority, pin_id)
    return authority


def test_v5_composite_is_deterministic_bounded_and_byte_deduplicated(
    tmp_path: Path,
) -> None:
    native_root = tmp_path / "native"
    native_root.mkdir()
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    boundary = start + timedelta(minutes=10)
    end = start + timedelta(minutes=20)
    origin = _build_segment(
        native_root,
        run_id="origin-v5",
        role="ORIGIN",
        interval_start=start,
        interval_end=boundary,
        row_times=[start, start + timedelta(minutes=5), boundary],
        identity=identity,
    )
    delta = _build_segment(
        native_root,
        run_id="delta-v5",
        role="DELTA",
        interval_start=boundary,
        interval_end=end,
        row_times=[boundary, boundary + timedelta(minutes=5)],
        identity=identity,
    )
    resolver = _resolver(native_root, identity, origin.segment, delta.segment)

    receipt = resolver.verify()
    assert receipt == resolver.verify()
    assert receipt.total_segments == 2
    assert receipt.total_physical_rows == 5
    assert receipt.total_stitched_rows == 4
    assert receipt.v5_native_segments == 2
    assert receipt.legacy_adopted_segments == 0
    assert receipt.limitation_codes == ()
    assert len(resolver.protection_graph.roots) == 6
    assert resolver.protection_graph.graph_sha256 == resolver.protection_graph.graph_sha256

    with pytest.raises(CompositeIntegrityError, match="lifecycle pin"):
        list(resolver.iter_cell_rows("eurusd", "m5"))
    authority = _pin_composite(
        resolver,
        native_root=native_root,
        lifecycle_root=tmp_path / "lifecycle",
    )
    rows = list(resolver.iter_cell_rows("eurusd", "m5"))
    assert [row.timestamp_utc for row in rows] == [
        "2024-01-01T00:00:00.000000Z",
        "2024-01-01T00:05:00.000000Z",
        "2024-01-01T00:10:00.000000Z",
        "2024-01-01T00:15:00.000000Z",
    ]
    assert rows[-1].mapping["Close"] == "1.10"
    assert resolver.publication_receipt.pin_id == "composite-publication-v1"

    authority.retire_pin("composite-publication-v1")
    with pytest.raises(CompositeIntegrityError, match="lifecycle pin"):
        list(resolver.iter_cell_rows("eurusd", "m5"))


def test_legacy_adoption_is_visible_and_can_never_masquerade_as_v5(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    legacy = _build_segment(
        tmp_path,
        run_id="origin-legacy",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
        legacy=True,
    )
    receipt = _resolver(tmp_path, identity, legacy.segment).verify()
    assert receipt.v5_native_segments == 0
    assert receipt.legacy_adopted_segments == 1
    assert receipt.limitation_codes == (LEGACY_MISSING_ATTEMPT_LEDGER,)

    assert legacy.segment.legacy_adoption is not None
    with pytest.raises(CompositeSchemaError):
        replace(legacy.segment.legacy_adoption, eligible_as_v5=True)
    with pytest.raises(CompositeSchemaError):
        replace(
            legacy.segment.legacy_adoption,
            original_attempt_ledger_present=True,
        )
    with pytest.raises(CompositeSchemaError):
        replace(legacy.segment, origin_contract_kind="V5_NATIVE")


def test_self_consistent_v3_request_cannot_masquerade_as_current_v5(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="v3-masquerade",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )

    def downgrade_request(payload: dict[str, Any]) -> None:
        request = payload["request"]
        assert isinstance(request, dict)
        request["timeframe_request_map_schema_version"] = (
            "fxpm.native_history.timeframe_request_map.v3"
        )
        _rehash_request_map(request)

    segment = _rewrite_provenance(built, downgrade_request)
    segment = _rewrite_manifest(segment, built, downgrade_request)
    with pytest.raises(CompositeIntegrityError, match="exact current V5"):
        _resolver(tmp_path, identity, segment).verify()


def test_leaf_request_projection_is_cross_bound_to_its_run_manifest(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="leaf-cross-binding",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )

    def falsify_leaf(payload: dict[str, Any]) -> None:
        request = payload["request"]
        assert isinstance(request, dict)
        years = request["cell_timeframe_window_years"]
        assert isinstance(years, dict)
        years["optimizer_train"] = 2

    segment = _rewrite_provenance(built, falsify_leaf)
    with pytest.raises(CompositeIntegrityError, match="leaf projections"):
        _resolver(tmp_path, identity, segment).verify()


def test_partial_legacy_artifact_may_truthfully_trim_only_missing_edge_history(
    tmp_path: Path,
) -> None:
    identity = _identity()
    requested_start = datetime(2023, 12, 1, tzinfo=timezone.utc)
    first_available = datetime(2024, 1, 1, tzinfo=timezone.utc)
    requested_end = first_available + timedelta(minutes=10)
    partial = _build_segment(
        tmp_path,
        run_id="partial-legacy",
        role="ORIGIN",
        interval_start=first_available,
        interval_end=requested_end,
        row_times=[first_available, first_available + timedelta(minutes=5)],
        identity=identity,
        legacy=True,
        status="partial",
        original_request_start=requested_start,
        original_request_end=requested_end,
    )
    assert _resolver(tmp_path, identity, partial.segment).verify().total_stitched_rows == 2

    dishonest_success = _build_segment(
        tmp_path,
        run_id="dishonest-success-trim",
        role="ORIGIN",
        interval_start=first_available,
        interval_end=requested_end,
        row_times=[first_available, first_available + timedelta(minutes=5)],
        identity=identity,
        legacy=True,
        status="success",
        original_request_start=requested_start,
        original_request_end=requested_end,
    )
    with pytest.raises(CompositeIntegrityError, match="partial artifact"):
        _resolver(tmp_path, identity, dishonest_success.segment).verify()


@pytest.mark.parametrize("artifact", ["bars", "provenance", "manifest"])
def test_any_origin_artifact_tamper_fails_closed(
    tmp_path: Path, artifact: str
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id=f"tamper-{artifact}",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    path = {
        "bars": built.bars_path,
        "provenance": built.provenance_path,
        "manifest": built.manifest_path,
    }[artifact]
    path.write_bytes(path.read_bytes() + b" ")

    with pytest.raises(NativeCompositeError):
        _resolver(tmp_path, identity, built.segment).verify()


@pytest.mark.parametrize(
    ("start_offset", "duration", "source_delta"),
    [(timedelta(minutes=1), 300, 0), (timedelta(0), 360, 0), (timedelta(0), 300, -1)],
)
def test_native_alignment_duration_and_source_interval_are_exact(
    tmp_path: Path,
    start_offset: timedelta,
    duration: int,
    source_delta: int,
) -> None:
    identity = _identity()
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    opened = base + start_offset
    built = _build_segment(
        tmp_path,
        run_id=f"bad-bar-{duration}-{int(start_offset.total_seconds())}-{source_delta}",
        role="ORIGIN",
        interval_start=base,
        interval_end=base + timedelta(minutes=20),
        row_times=[opened],
        identity=identity,
        duration_seconds=duration,
        source_last_delta_ns=source_delta,
    )
    with pytest.raises(CompositeIntegrityError):
        _resolver(tmp_path, identity, built.segment).verify()


def test_pagination_authenticates_nested_copy_retries_not_reversed_counts(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="pagination-retry-inequality",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )

    def reverse_retry_counts(payload: dict[str, Any]) -> None:
        pagination = payload["pagination"]
        assert isinstance(pagination, dict)
        pages = pagination["page_attempts"]
        assert isinstance(pages, list) and isinstance(pages[0], dict)
        pages[0]["attempts"] = 2
        pages[0]["copy_rates_range_attempts"] = 1
        pagination["attempts_total"] = 2
        pagination["copy_rates_range_attempts_total"] = 1

    segment = _rewrite_provenance(built, reverse_retry_counts)
    with pytest.raises(CompositeIntegrityError, match="retry counts are impossible"):
        _resolver(tmp_path, identity, segment).verify()


def test_pagination_pages_must_exactly_partition_the_source_request(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=10)
    built = _build_segment(
        tmp_path,
        run_id="pagination-partition",
        role="ORIGIN",
        interval_start=start,
        interval_end=end,
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )

    def shorten_page(payload: dict[str, Any]) -> None:
        pagination = payload["pagination"]
        assert isinstance(pagination, dict)
        pages = pagination["page_attempts"]
        assert isinstance(pages, list) and isinstance(pages[0], dict)
        pages[0]["end_exclusive_utc"] = _z(end - timedelta(minutes=5))

    segment = _rewrite_provenance(built, shorten_page)
    with pytest.raises(CompositeIntegrityError, match="source bounds"):
        _resolver(tmp_path, identity, segment).verify()


def test_pagination_rejects_conflicting_duplicate_page_dispositions(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="pagination-dispositions",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
        status="partial",
    )

    def duplicate_disposition(payload: dict[str, Any]) -> None:
        pagination = payload["pagination"]
        assert isinstance(pagination, dict)
        pages = pagination["page_attempts"]
        assert isinstance(pages, list) and isinstance(pages[0], dict)
        pagination["empty_pages"] = [dict(pages[0])]
        pagination["pages_with_data"] = 0

    segment = _rewrite_provenance(built, duplicate_disposition)
    with pytest.raises(CompositeIntegrityError, match="conflicting"):
        _resolver(tmp_path, identity, segment).verify()


@pytest.mark.parametrize(
    ("timeframe", "start", "successor_delta", "nominal"),
    [
        (
            "H4",
            datetime(2024, 3, 31, 21, tzinfo=timezone.utc),
            timedelta(hours=5),
            timedelta(hours=4),
        ),
        (
            "D1",
            datetime(2024, 3, 30, 22, tzinfo=timezone.utc),
            timedelta(hours=23),
            timedelta(days=1),
        ),
    ],
)
def test_h4_d1_close_uses_authenticated_broker_successor_across_session_shifts(
    tmp_path: Path,
    timeframe: str,
    start: datetime,
    successor_delta: timedelta,
    nominal: timedelta,
) -> None:
    identity = _identity()
    successor = start + successor_delta
    end = successor + nominal
    valid = _build_segment(
        tmp_path,
        run_id=f"session-valid-{timeframe.lower()}",
        role="ORIGIN",
        interval_start=start,
        interval_end=end,
        row_times=[start, successor],
        row_duration_seconds=[
            int(successor_delta.total_seconds()),
            int(nominal.total_seconds()),
        ],
        identity=identity,
        timeframe=timeframe,
    )
    assert _resolver(tmp_path, identity, valid.segment).verify().total_stitched_rows == 2

    invalid = _build_segment(
        tmp_path,
        run_id=f"session-invalid-{timeframe.lower()}",
        role="ORIGIN",
        interval_start=start,
        interval_end=end,
        row_times=[start, successor],
        row_duration_seconds=[
            int(nominal.total_seconds()),
            int(nominal.total_seconds()),
        ],
        identity=identity,
        timeframe=timeframe,
    )
    with pytest.raises(CompositeIntegrityError, match="observed broker successor"):
        _resolver(tmp_path, identity, invalid.segment).verify()


def test_boundary_duplicate_must_be_byte_identical(tmp_path: Path) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    boundary = start + timedelta(minutes=10)
    origin = _build_segment(
        tmp_path,
        run_id="origin-crlf",
        role="ORIGIN",
        interval_start=start,
        interval_end=boundary,
        row_times=[start, start + timedelta(minutes=5), boundary],
        identity=identity,
        newline=b"\r\n",
    )
    delta = _build_segment(
        tmp_path,
        run_id="delta-lf",
        role="DELTA",
        interval_start=boundary,
        interval_end=boundary + timedelta(minutes=10),
        row_times=[boundary, boundary + timedelta(minutes=5)],
        identity=identity,
        newline=b"\n",
    )
    with pytest.raises(CompositeIntegrityError, match="byte-identical successor"):
        _resolver(tmp_path, identity, origin.segment, delta.segment).verify()


def test_terminal_segment_cannot_publish_a_successor_sentinel(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=10)
    terminal = _build_segment(
        tmp_path,
        run_id="terminal-sentinel",
        role="ORIGIN",
        interval_start=start,
        interval_end=end,
        row_times=[start, start + timedelta(minutes=5), end],
        identity=identity,
    )
    with pytest.raises(CompositeIntegrityError, match="terminal segment"):
        _resolver(tmp_path, identity, terminal.segment).verify()


def test_caps_reject_declared_work_before_scanning(tmp_path: Path) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="over-cap",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    manifest = CompositeRunManifest(
        "over-cap-composite",
        "2026-07-13T00:00:00Z",
        identity,
        (CompositeCell("EURUSD", "M5", (built.segment,)),),
    )
    with pytest.raises(CompositeCapacityError, match="row cap"):
        NativeCompositeResolver(
            tmp_path,
            manifest,
            limits=CompositeLimits(max_rows_per_segment=1),
        )


def test_structural_segment_cap_fails_before_manifest_serialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    boundary = start + timedelta(minutes=10)
    first = _build_segment(
        tmp_path,
        run_id="pre-serialize-origin",
        role="ORIGIN",
        interval_start=start,
        interval_end=boundary,
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    second = _build_segment(
        tmp_path,
        run_id="pre-serialize-delta",
        role="DELTA",
        interval_start=boundary,
        interval_end=boundary + timedelta(minutes=10),
        row_times=[boundary, boundary + timedelta(minutes=5)],
        identity=identity,
    )
    manifest = CompositeRunManifest(
        "pre-serialize-cap",
        "2026-07-13T00:00:00Z",
        identity,
        (CompositeCell("EURUSD", "M5", (first.segment, second.segment)),),
    )

    def forbid_serialization(_manifest: CompositeRunManifest) -> bytes:
        raise AssertionError("serialization happened before the structural cap")

    monkeypatch.setattr(CompositeRunManifest, "to_bytes", forbid_serialization)
    with pytest.raises(CompositeCapacityError, match="segment cap"):
        NativeCompositeResolver(
            tmp_path,
            manifest,
            limits=CompositeLimits(max_segments_per_cell=1),
        )


def test_hardlink_and_symlink_aliases_are_rejected(tmp_path: Path) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="path-alias",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    alias = built.bars_path.with_name("bars-alias.csv")
    os.link(built.bars_path, alias)
    with pytest.raises(CompositePathError, match="hardlinked"):
        _resolver(tmp_path, identity, built.segment).verify()
    alias.unlink()

    target = built.bars_path.with_name("bars-target.csv")
    built.bars_path.replace(target)
    try:
        built.bars_path.symlink_to(target.name)
    except OSError:
        pytest.skip("host does not permit creating a test symlink")
    with pytest.raises(CompositePathError, match="symlink|reparse"):
        _resolver(tmp_path, identity, built.segment).verify()


@pytest.mark.parametrize(
    "unsafe",
    [
        "../outside.csv",
        "runs/run/../../outside.csv",
        "runs/run/file.csv:stream",
        "runs/run/trailing./file.csv",
        "C:/outside.csv",
        "runs\\run\\file.csv",
    ],
)
def test_unsafe_relative_paths_are_rejected(
    tmp_path: Path, unsafe: str
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="unsafe-path",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    with pytest.raises(CompositeSchemaError):
        replace(built.segment, bars_relative_path=unsafe)


def test_source_identity_and_unattested_interval_gaps_are_rejected(
    tmp_path: Path,
) -> None:
    identity = _identity()
    other_identity = replace(identity, account_id="999999")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    first = _build_segment(
        tmp_path,
        run_id="source-first",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    second = _build_segment(
        tmp_path,
        run_id="source-second",
        role="DELTA",
        interval_start=start + timedelta(minutes=15),
        interval_end=start + timedelta(minutes=25),
        row_times=[start + timedelta(minutes=15), start + timedelta(minutes=20)],
        identity=identity,
    )
    with pytest.raises(CompositeSchemaError, match="shortfall"):
        CompositeCell("EURUSD", "M5", (first.segment, second.segment))
    with pytest.raises(CompositeSchemaError, match="source identity"):
        CompositeCell(
            "EURUSD",
            "M5",
            (first.segment, replace(second.segment, source_identity=other_identity)),
        )


def test_hashed_acquisition_shortfall_can_authorize_one_exact_interval_gap(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    first_end = start + timedelta(minutes=10)
    second_start = start + timedelta(minutes=15)
    second_end = start + timedelta(minutes=25)
    first = _build_segment(
        tmp_path,
        run_id="shortfall-origin",
        role="ORIGIN",
        interval_start=start,
        interval_end=first_end,
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    second = _build_segment(
        tmp_path,
        run_id="shortfall-delta",
        role="DELTA",
        interval_start=second_start,
        interval_end=second_end,
        row_times=[second_start, second_start + timedelta(minutes=5)],
        identity=identity,
        status="partial",
        original_request_start=first_end,
        original_request_end=second_end,
        declared_shortfall=(first_end, second_start, "BROKER_HISTORY_ABSENT"),
    )
    receipt = _resolver(tmp_path, identity, first.segment, second.segment).verify()
    assert receipt.total_segments == 2
    assert receipt.total_stitched_rows == 4

    assert second.segment.preceding_shortfall is not None
    wrong_evidence = replace(
        second.segment.preceding_shortfall,
        evidence_sha256="0" * 64,
    )
    with pytest.raises(CompositeSchemaError, match="shortfall"):
        CompositeCell(
            "EURUSD",
            "M5",
            (first.segment, replace(second.segment, preceding_shortfall=wrong_evidence)),
        )

    incompatible_reason = replace(
        second.segment.preceding_shortfall,
        reason_code="BROKER_PAGE_EMPTY",
    )
    with pytest.raises(CompositeIntegrityError, match="compatible page-level"):
        _resolver(
            tmp_path,
            identity,
            first.segment,
            replace(second.segment, preceding_shortfall=incompatible_reason),
        ).verify()


def test_shortfall_cannot_authorize_history_that_was_never_requested(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    first_end = start + timedelta(minutes=10)
    second_start = start + timedelta(minutes=15)
    second_end = start + timedelta(minutes=25)
    first = _build_segment(
        tmp_path,
        run_id="unrequested-shortfall-origin",
        role="ORIGIN",
        interval_start=start,
        interval_end=first_end,
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    second = _build_segment(
        tmp_path,
        run_id="unrequested-shortfall-delta",
        role="DELTA",
        interval_start=second_start,
        interval_end=second_end,
        row_times=[second_start, second_start + timedelta(minutes=5)],
        identity=identity,
        status="partial",
        original_request_start=second_start,
        original_request_end=second_end,
        declared_shortfall=(first_end, second_start, "BROKER_HISTORY_ABSENT"),
    )
    with pytest.raises(CompositeIntegrityError, match="requested, partial V5"):
        _resolver(tmp_path, identity, first.segment, second.segment).verify()


def test_strict_json_rejects_duplicate_keys_even_when_raw_hash_is_bound(
    tmp_path: Path,
) -> None:
    identity = _identity()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    built = _build_segment(
        tmp_path,
        run_id="duplicate-json",
        role="ORIGIN",
        interval_start=start,
        interval_end=start + timedelta(minutes=10),
        row_times=[start, start + timedelta(minutes=5)],
        identity=identity,
    )
    raw = built.provenance_path.read_bytes()
    hostile = raw[:-1] + b',"status":"success"}'
    built.provenance_path.write_bytes(hostile)
    hostile_segment = replace(
        built.segment,
        provenance_sha256=_sha(hostile),
        provenance_bytes=len(hostile),
    )
    with pytest.raises(CompositeIntegrityError, match="strict bounded"):
        _resolver(tmp_path, identity, hostile_segment).verify()
