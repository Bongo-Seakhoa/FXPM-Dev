"""Reusable exact V5 native-history fixtures for production-boundary tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from pm_native_activation import sha256_file
from pm_native_parity import ParityThresholds
from pm_native_request_contract import (
    OUTPUT_MAX_CELL_BYTES,
    OUTPUT_MIN_FREE_BYTES,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_SYMBOL_ORDER,
    _fixture_mt5_v4_empty_diagnostics,
    _fixture_mt5_v4_page_diagnostics,
    build_v5_manifest_request,
    build_v5_leaf_request,
    build_v5_request_contract,
)
from tools.acquire_native_history import (
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    DEFAULT_TIMEFRAMES,
    PAGINATION_CONTRACT_VERSION,
    SCHEMA_VERSION,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    TIMEFRAME_SECONDS,
)
from tools.audit_activate_native_history import audit_and_activate


UTC = timezone.utc
SYMBOL = "EURUSD"
SOURCE_ENVIRONMENT = {
    "metadata_status": "available",
    "account": {
        "login": 42,
        "server": "TestBroker-Demo",
        "company": "TestBroker",
        "currency": "USD",
    },
    "configured_connection": {
        "server": "TestBroker-Demo",
        "terminal_path": r"C:\TestBroker\terminal64.exe",
        "expected_login": 42,
        "expected_server": "TestBroker-Demo",
        "expected_currency": "USD",
    },
    "terminal": {
        "name": "Test Terminal",
        "company": "TestBroker",
        "path": r"C:\TestBroker",
        "data_path": r"C:\TestBroker\Data",
        "commondata_path": r"C:\ProgramData\TestBroker",
        "build": 5432,
        "version": [5, 5432, "13 Jul 2026"],
        "max_bars": 10000,
    },
}
VALID_RESOURCE_SNAPSHOT = {
    "status": "within_bounds",
    "data_path": r"C:\TestBroker\Data",
    "disk_total_bytes": 100 * 1024**3,
    "disk_free_bytes": 50 * 1024**3,
    "minimum_free_bytes": 4 * 1024**3,
    "hcc_files_inspected": 1,
    "hcc_entries_inspected": 1,
    "largest_hcc_bytes": 1_000_000,
    "largest_hcc_relative_path": r"bases\TestBroker-Demo\history\EURUSD\2026.hcc",
    "maximum_hcc_file_bytes": 2 * 1024**3,
    "scan_scope": "symbol",
    "scan_error": None,
    "violations": [],
}


def _interval(start: Any, end: Any) -> dict[str, str]:
    return {
        "start_utc": pd.Timestamp(start).isoformat(),
        "end_exclusive_utc": pd.Timestamp(end).isoformat(),
    }


def _iso(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    timestamp = (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )
    return timestamp.isoformat().replace("+00:00", "Z")


def _retried_page_diagnostics(
    *, symbol: str, timeframe: str, start: Any, end: Any, frame: pd.DataFrame
) -> dict[str, Any]:
    empty = _fixture_mt5_v4_empty_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=pd.Timestamp(start).to_pydatetime(),
        end=pd.Timestamp(end).to_pydatetime(),
    )
    data = _fixture_mt5_v4_page_diagnostics(
        symbol=symbol,
        timeframe=timeframe,
        start=pd.Timestamp(start).to_pydatetime(),
        end=pd.Timestamp(end).to_pydatetime(),
        first_open=frame.index[0].to_pydatetime(),
        last_open=frame.index[-1].to_pydatetime(),
        rows=len(frame),
    )
    retry = dict(data["call_ledger"][0])
    retry.update(sequence=3, call_type="copy_rates_range_retry")
    empty["call_ledger"][2] = retry
    for field in (
        "sync_status",
        "raw_relation",
        "raw_count",
        "raw_extent",
        "admitted_count",
        "admitted_extent",
        "boundary_complete",
        "best_extent",
        "last_error",
    ):
        empty[field] = data[field]
    empty["confirmation_raw_relation"] = data["raw_relation"]
    return empty


def _v5_contract(_origin: pd.Timestamp, end: pd.Timestamp):
    """Build a semantically valid production V5 request with tiny bar leaves.

    The bars intentionally cover only the recent tail. Empty exact pages are
    represented as technical history gaps; the fixture never upgrades terminal
    cache behavior into broker-absence or listing-boundary authority.
    """

    common = build_v5_request_contract(
        requested_information_cutoff_utc=end.to_pydatetime()
    )
    leaves = {}
    partitions = {}
    for timeframe in DEFAULT_TIMEFRAMES:
        # The fixture frame is built below.  Seed the strict leaf constructor
        # with the latest nominally closed bar; _pagination replaces this with
        # the exact final bar in the generated artifact before validation.
        leaf = build_v5_leaf_request(
            common,
            timeframe,
            last_fully_closed_bar_open_utc=(
                end - pd.Timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
            ).to_pydatetime(),
        )
        leaves[timeframe] = leaf
        partitions[timeframe] = {
            partition: (
                pd.Timestamp(boundary["start_ts"]),
                pd.Timestamp(boundary["end_ts"]),
            )
            for partition, boundary in leaf["cell_calendar_boundaries"].items()
        }
    return common, leaves, partitions


def _bars(origin: pd.Timestamp, *, days: int = 100, price_offset: float = 0.0):
    index = pd.date_range(origin, periods=days * 24 * 12, freq="5min")
    # Piecewise constant within M15 prevents floating reduction-order noise.
    value = (
        100.0
        + float(price_offset)
        + (np.arange(len(index)) // 3).astype(float) * 0.0001
    )
    return pd.DataFrame(
        {
            "Open": value,
            "High": value + 0.0002,
            "Low": value - 0.0002,
            "Close": value + 0.00005,
            "Volume": np.ones(len(index), dtype=int),
        },
        index=index,
    )


def _bars_for_index(
    index: pd.DatetimeIndex,
    *,
    price_offset: float = 0.0,
) -> pd.DataFrame:
    """Build associative OHLCV values for a sparse, UTC-native test clock."""

    index = pd.DatetimeIndex(index).sort_values().unique()
    if index.tz is None:
        index = index.tz_localize("UTC")
    else:
        index = index.tz_convert("UTC")
    # A time-derived M15 bucket makes OHLC aggregation associative across the
    # independently materialized native timeframes.  Sparse rows therefore
    # exercise page/authenticity mechanics without fabricating dense history.
    bucket_ns = pd.Timedelta(minutes=15).value
    buckets = index.asi8 // bucket_ns
    value = 100.0 + float(price_offset) + (buckets - buckets.min()) * 0.000001
    return pd.DataFrame(
        {
            "Open": value,
            "High": value + 0.0002,
            "Low": value - 0.0002,
            "Close": value + 0.00005,
            "Volume": np.ones(len(index), dtype=int),
        },
        index=index,
    )


def _resample(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (
        frame.resample(rule)
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna()
    )


def _full_span_sparse_frames(
    leaves: dict[str, dict[str, Any]],
    partitions_by_timeframe: dict[str, dict[str, tuple[pd.Timestamp, pd.Timestamp]]],
    *,
    price_offset: float,
    include_dense_comparison_band: bool = True,
) -> dict[str, pd.DataFrame]:
    """Return compact native frames with authenticated data on every page.

    These fixtures are intentionally statistically sparse outside a bounded
    dense comparison band.  Unlike the retired fixture, however, they do not
    turn generic empty MT5 pages into a listing/broker boundary assertion: each
    requested page contains observed rows close enough to both boundaries for
    the released connector receipt to authenticate it.
    """

    rules = {
        "M5": "5min",
        "M15": "15min",
        "M30": "30min",
        "H1": "1h",
        "H4": "4h",
        "D1": "1D",
    }
    points: set[pd.Timestamp] = set()
    common_as_of = pd.Timestamp(leaves["M5"]["common_as_of_utc"])
    for timeframe, leaf in leaves.items():
        seconds = TIMEFRAME_SECONDS[timeframe]
        frequency = pd.Timedelta(seconds=seconds)
        start = pd.Timestamp(leaf["source_request_interval"]["start_utc"])
        end = pd.Timestamp(leaf["source_request_interval"]["end_exclusive_utc"])
        slots = min(
            max(1, DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds),
            int(SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90),
        )
        page_span = pd.Timedelta(seconds=slots * seconds)
        cursor_end = end
        last_closed_open = common_as_of - frequency
        while cursor_end > start:
            page_start = max(start, cursor_end - page_span)
            first_open = page_start.ceil(rules[timeframe])
            last_open = min(
                (cursor_end - pd.Timedelta(nanoseconds=1)).floor(rules[timeframe]),
                last_closed_open,
            )
            if first_open <= last_open:
                points.add(first_open)
                points.add(last_open)
            cursor_end = page_start

    # Preserve a bounded, high-density common band only where corruption tests
    # need it. The other 99 production-grid symbols remain byte-backed but use
    # only the page-boundary rows needed for source-integrity verification.
    if include_dense_comparison_band:
        dense_origin = partitions_by_timeframe["M5"]["final_lockbox"][0] - pd.Timedelta(
            days=10
        )
        points.update(pd.date_range(dense_origin, periods=100 * 24 * 12, freq="5min"))
    base = _bars_for_index(
        pd.DatetimeIndex(sorted(points)),
        price_offset=price_offset,
    )

    frames: dict[str, pd.DataFrame] = {}
    for timeframe, leaf in leaves.items():
        seconds = TIMEFRAME_SECONDS[timeframe]
        start = pd.Timestamp(leaf["source_request_interval"]["start_utc"])
        end = pd.Timestamp(leaf["source_request_interval"]["end_exclusive_utc"])
        frame = _resample(base, rules[timeframe])
        frame = frame.loc[
            (frame.index >= start)
            & (frame.index < end)
            & (frame.index + pd.Timedelta(seconds=seconds) <= common_as_of)
        ]
        assert not frame.empty
        frames[timeframe] = frame
    return frames


def _pagination(
    symbol: str,
    timeframe: str,
    leaf_contract: dict[str, Any],
    frame: pd.DataFrame,
):
    start = pd.Timestamp(leaf_contract["source_request_interval"]["start_utc"])
    end = pd.Timestamp(leaf_contract["source_request_interval"]["end_exclusive_utc"])
    seconds = TIMEFRAME_SECONDS[timeframe]
    slots = min(
        max(1, DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds),
        int(SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90),
    )
    page_seconds = slots * seconds
    attempts = []
    empty_pages = []
    pages_with_data = 0
    cursor_end = end
    request_sequence = 0
    while cursor_end > start:
        page_start = max(start, cursor_end - pd.Timedelta(seconds=page_seconds))
        request_sequence += 1
        page = {
            "start_utc": _iso(page_start),
            "end_exclusive_utc": _iso(cursor_end),
            "attempts": 1,
            "copy_rates_range_attempts": 1,
            "request_sequence": request_sequence,
            "terminal_cache_resource_snapshot": dict(VALID_RESOURCE_SNAPSHOT),
        }
        page_frame = frame.loc[(frame.index >= page_start) & (frame.index < cursor_end)]
        has_data = not page_frame.empty
        if has_data:
            pages_with_data += 1
            initial = _fixture_mt5_v4_page_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=page_start.to_pydatetime(),
                end=cursor_end.to_pydatetime(),
                first_open=page_frame.index[0].to_pydatetime(),
                last_open=page_frame.index[-1].to_pydatetime(),
                rows=len(page_frame),
            )
            page["connector_diagnostics"] = (
                initial
                if initial["boundary_complete"]
                else _retried_page_diagnostics(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=page_start,
                    end=cursor_end,
                    frame=page_frame,
                )
            )
            page["copy_rates_range_attempts"] = page["connector_diagnostics"][
                "copy_rates_range_attempts"
            ]
        else:
            page["connector_diagnostics"] = _fixture_mt5_v4_empty_diagnostics(
                symbol=symbol,
                timeframe=timeframe,
                start=page_start.to_pydatetime(),
                end=cursor_end.to_pydatetime(),
            )
            page["copy_rates_range_attempts"] = 2
            empty_pages.append(dict(page))
        attempts.append(page)
        cursor_end = page_start
    attempts.sort(key=lambda item: item["start_utc"])
    empty_pages.sort(key=lambda item: item["start_utc"])
    request = {
        **leaf_contract,
        "native_fetch_start_utc": _iso(start),
        "native_fetch_end_exclusive_utc": _iso(end),
        "last_fully_closed_bar_open_utc": _iso(frame.index[-1]),
        "alignment_mode": (
            "broker_observed_successor_or_nominal_close_v1"
            if timeframe in {"H4", "D1"}
            else "fixed_utc_native_boundary_v1"
        ),
        "successor_sentinel_fetch_slack_seconds": (
            1 if timeframe in {"H4", "D1"} else 0
        ),
    }
    return request, {
        "contract_version": PAGINATION_CONTRACT_VERSION,
        "request_order": "newest_to_oldest",
        "page_span_seconds": page_seconds,
        "pages_planned": len(attempts),
        "pages_requested": len(attempts),
        "pages_with_data": pages_with_data,
        "empty_pages": empty_pages,
        "failed_pages": [],
        "truncated_pages": [],
        "terminal_prefix_unresolved_pages": [],
        "stop_reason": None,
        "unrequested_prefix": None,
        "initial_terminal_cache_resource_snapshot": dict(VALID_RESOURCE_SNAPSHOT),
        "attempts_total": len(attempts),
        "copy_rates_range_attempts_total": sum(
            page["copy_rates_range_attempts"] for page in attempts
        ),
        "page_attempts": attempts,
        "retries_per_page": 1,
    }


def _write_leaf(
    native_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    frame: pd.DataFrame,
    partitions: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
    leaf_contract: dict[str, Any],
):
    leaf = native_root / "runs" / run_id / symbol / timeframe
    leaf.mkdir(parents=True, exist_ok=False)
    seconds = TIMEFRAME_SECONDS[timeframe]
    frame = frame.copy()
    frame["Spread"] = 0
    frame["RealVolume"] = frame["Volume"]
    frame["_SOURCE_FIRST_TS"] = frame.index
    frame["_SOURCE_LAST_TS"] = (
        frame.index + pd.Timedelta(seconds=seconds) - pd.Timedelta(nanoseconds=1)
    )
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(seconds=seconds)
    frame = frame.loc[:, PRODUCTION_ARTIFACT_COLUMNS]
    bars_path = leaf / "bars.csv"
    frame.to_csv(bars_path, index_label="time", lineterminator="\n")
    digest = sha256_file(bars_path)
    observed_counts = {
        name: int(
            (
                (frame.index >= start)
                & (frame.index + pd.Timedelta(seconds=seconds) <= end)
            ).sum()
        )
        for name, (start, end) in partitions.items()
    }
    request, pagination = _pagination(symbol, timeframe, leaf_contract, frame)
    status = "partial" if pagination["empty_pages"] else "success"
    # An empty exact MT5 page is a technical observation, never proof that the
    # broker has no history at a leading or trailing boundary.  Synthetic V5
    # fixtures must preserve that distinction or tests can accidentally bless
    # the same 35-symbol eligibility veto we are removing from production.
    failure_status = "technical_history_gap" if status == "partial" else None
    internal_empty = len(pagination["empty_pages"])
    first_open = pd.Timestamp(frame.index[0])
    last_open = pd.Timestamp(frame.index[-1])
    last_close = pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1])
    source_start = pd.Timestamp(request["source_request_interval"]["start_utc"])
    source_end = pd.Timestamp(request["source_request_interval"]["end_exclusive_utc"])
    coverage_start = source_start
    coverage_end = source_end
    if timeframe in {"H4", "D1"}:
        coverage_end = pd.Timestamp(request["common_as_of_utc"])
    start_delay_seconds = max(0.0, float((first_open - coverage_start).total_seconds()))
    end_lag_seconds = max(0.0, float((coverage_end - last_close).total_seconds()))
    coverage_span_seconds = max(
        1.0, float((coverage_end - coverage_start).total_seconds())
    )
    observed_span_seconds = max(
        0.0,
        float(
            (
                min(coverage_end, last_close) - max(coverage_start, first_open)
            ).total_seconds()
        ),
    )
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "symbol": symbol,
        "canonical_symbol": symbol,
        "broker_symbol": f"{symbol}.a",
        "timeframe": timeframe,
        "timeframe_seconds": seconds,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "status": status,
        "failure_status": failure_status,
        "eligible_for_optimization": False,
        "eligible_for_downstream_validation": status == "success",
        "statistical_sufficiency_assessed": False,
        "complete": status == "success",
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": digest,
            "bytes": bars_path.stat().st_size,
            "rows": len(frame),
        },
        "sha256": digest,
        "rows": len(frame),
        "start_ts": _iso(first_open),
        "end_ts": _iso(last_open),
        "columns": list(frame.columns),
        "instrument": {"parity_tick_tolerance": 0.0001},
        "source_environment": SOURCE_ENVIRONMENT,
        "output_storage_resource_snapshot": {
            "status": "within_bounds",
            "output_path": str(native_root / "runs" / run_id),
            "disk_total_bytes": 100 * 1024**3,
            "disk_free_bytes": 50 * 1024**3,
            "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
            "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
            "estimated_cell_bytes": bars_path.stat().st_size,
            "sample_rows": min(2048, len(frame)),
            "violations": [],
        },
        "coverage": {
            "status": (
                "partial_requested_span"
                if status == "partial"
                else "full_requested_span"
            ),
            "actual_first_open_utc": _iso(first_open),
            "actual_last_open_utc": _iso(last_open),
            "actual_last_close_utc": _iso(last_close),
            "start_delay_seconds": float(start_delay_seconds),
            "end_lag_seconds": float(end_lag_seconds),
            "calendar_span_coverage_ratio": float(
                min(1.0, observed_span_seconds / coverage_span_seconds)
            ),
            "internal_empty_page_count": internal_empty,
            "truncated_page_count": 0,
            "internal_truncated_page_count": 0,
            "boundary_truncated_page_count": 0,
            "failed_page_count": 0,
            "terminal_prefix_unresolved_page_count": 0,
            "technical_integrity_status": (
                "internal_gap_detected" if internal_empty else "complete_page_ledger"
            ),
            "market_calendar_completeness_assessed": False,
        },
        "expected_session_coverage_by_partition": dict.fromkeys(partitions, 1.0),
        "expected_session_coverage_details_by_partition": {
            "method": SEGMENTED_SESSION_COVERAGE_METHOD_V2,
            "status": "estimated",
            "performance_inputs_used": False,
            "timestamp_only": True,
        },
        "observed_session_bars_by_partition": dict(observed_counts),
        "expected_session_bars_by_partition": {
            name: float(value) for name, value in observed_counts.items()
        },
        "request": request,
        "pagination": pagination,
    }
    (leaf / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "relative_leaf": f"{symbol}/{timeframe}",
        "resume_disposition": "newly_acquired",
        "status": status,
        "failure_status": failure_status,
        "rows": len(frame),
        "sha256": digest,
        "eligible_for_optimization": False,
        "eligible_for_downstream_validation": status == "success",
    }


def write_v5_run(
    native_root: Path,
    run_id: str,
    *,
    corrupt_timeframe: Optional[str] = None,
    context_only: bool = False,
    price_offset: float = 0.0,
    production_grid: bool = False,
) -> None:
    """Write a bounded V5 run.

    Most consensus tests intentionally isolate one symbol and therefore use a
    non-authoritative subset manifest.  ``production_grid=True`` writes the
    exact 100 x 6 roster. Every production-grid cell has a physical sparse bar
    artifact and a complete page ledger; no caller-asserted absence is used to
    manufacture production authority. The production request authority itself
    never accepts the subset form.
    """

    cutoff = pd.Timestamp("2025-04-11", tz="UTC")
    contract, leaves, partitions_by_timeframe = _v5_contract(
        cutoff - pd.Timedelta(days=100), cutoff
    )
    frames = _full_span_sparse_frames(
        leaves,
        partitions_by_timeframe,
        price_offset=price_offset,
    )
    if corrupt_timeframe:
        frame = frames[corrupt_timeframe].copy()
        partitions = partitions_by_timeframe[corrupt_timeframe]
        if context_only:
            strategy = leaves[corrupt_timeframe]["strategy_evidence_interval"]
            candidates = pd.Series(
                (frame.index >= pd.Timestamp(strategy["start_utc"]))
                & (frame.index < pd.Timestamp(strategy["end_exclusive_utc"])),
                index=frame.index,
            )
            for start, finish in partitions.values():
                candidates &= ~((frame.index >= start) & (frame.index < finish))
            locations = frame.index[candidates.to_numpy()]
            assert len(locations) > 2
            location = locations[2]
        else:
            lockbox_start, lockbox_end = partitions["final_lockbox"]
            locations = frame.index[
                (frame.index >= lockbox_start) & (frame.index < lockbox_end)
            ]
            assert len(locations) > 2
            location = locations[2]
        frame.loc[location, ["Open", "High", "Low", "Close"]] += 5.0
        frames[corrupt_timeframe] = frame
    results = [
        _write_leaf(
            native_root,
            run_id,
            SYMBOL,
            timeframe,
            frames[timeframe],
            partitions_by_timeframe[timeframe],
            leaves[timeframe],
        )
        for timeframe in DEFAULT_TIMEFRAMES
    ]
    if production_grid:
        assert SYMBOL in PRODUCTION_SYMBOL_ORDER
        sparse_frames = _full_span_sparse_frames(
            leaves,
            partitions_by_timeframe,
            price_offset=price_offset,
            include_dense_comparison_band=False,
        )
        results.extend(
            _write_leaf(
                native_root,
                run_id,
                symbol,
                timeframe,
                sparse_frames[timeframe],
                partitions_by_timeframe[timeframe],
                leaves[timeframe],
            )
            for symbol in PRODUCTION_SYMBOL_ORDER
            if symbol != SYMBOL
            for timeframe in DEFAULT_TIMEFRAMES
        )
        manifest_request = build_v5_manifest_request(
            contract,
            symbols=PRODUCTION_SYMBOL_ORDER,
            retries=1,
        )
    else:
        # Fixture-only projection for fast, isolated consensus unit tests.
        # build_v5_manifest_request deliberately rejects this shape.
        manifest_request = {
            "symbols": [SYMBOL],
            "timeframes": list(DEFAULT_TIMEFRAMES),
            "retries": 1,
            "window_years": dict(contract["timeframe_window_years"]["M5"]),
            "common_calendar_boundaries": dict(
                contract["timeframe_calendar_boundaries"]["M5"]
            ),
            **contract,
        }
    manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "immutable_record": True,
        "complete": True,
        "run_id": run_id,
        "request": manifest_request,
        "source_environment": SOURCE_ENVIRONMENT,
        "results": results,
        "counts": {
            "success": sum(item["status"] == "success" for item in results),
            "partial": sum(item["status"] == "partial" for item in results),
            "failed": sum(item["status"] == "failed" for item in results),
            "total": len(results),
        },
    }
    (native_root / "runs" / run_id / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def rewrite_no_history(native_root: Path, run_id: str, timeframe: str) -> None:
    leaf = native_root / "runs" / run_id / SYMBOL / timeframe
    (leaf / "bars.csv").unlink()
    provenance_path = leaf / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    page_attempts = [
        {
            **page,
            "attempts": 1,
            "copy_rates_range_attempts": 2,
            "connector_diagnostics": _fixture_mt5_v4_empty_diagnostics(
                symbol=SYMBOL,
                timeframe=timeframe,
                start=pd.Timestamp(page["start_utc"]).to_pydatetime(),
                end=pd.Timestamp(page["end_exclusive_utc"]).to_pydatetime(),
            ),
        }
        for page in provenance["pagination"]["page_attempts"]
    ]
    provenance.update(
        {
            "status": "failed",
            "failure_status": "terminal_history_empty_unresolved",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "complete": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
            "columns": [],
        }
    )
    provenance.pop("source_unavailable_proof", None)
    provenance["pagination"].update(
        {
            "pages_with_data": 0,
            "empty_pages": page_attempts,
            "failed_pages": [],
            "truncated_pages": [],
            "attempts_total": len(page_attempts),
            "copy_rates_range_attempts_total": 2 * len(page_attempts),
            "page_attempts": page_attempts,
        }
    )
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    result = next(
        item for item in manifest["results"] if item["timeframe"] == timeframe
    )
    result.update(
        {
            "status": "failed",
            "failure_status": "terminal_history_empty_unresolved",
            "rows": 0,
            "sha256": None,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
        }
    )
    manifest["counts"] = {
        "success": sum(item["status"] == "success" for item in manifest["results"]),
        "partial": sum(item["status"] == "partial" for item in manifest["results"]),
        "failed": sum(item["status"] == "failed" for item in manifest["results"]),
        "total": len(manifest["results"]),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def activate_v5_run(
    native_root: Path,
    run_id: str,
    activation_id: str,
    *,
    production_only_pointer: bool = False,
):
    manifest = json.loads(
        (native_root / "runs" / run_id / "run_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    symbols = tuple(manifest["request"]["symbols"])
    production_authoritative = symbols == tuple(PRODUCTION_SYMBOL_ORDER)
    return audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=symbols,
        timeframes=DEFAULT_TIMEFRAMES,
        activation_id=activation_id,
        thresholds=ParityThresholds(min_compared_native_bars=20),
        now=datetime(2026, 1, 1, tzinfo=UTC),
        # This is a test helper, not production authorization. Existing
        # cross-suite fixtures use its default pointer publication to build a
        # later contract under test. Production-like consensus tests opt into
        # the stricter exact-roster publication rule explicitly.
        update_active_pointer=(
            production_authoritative if production_only_pointer else True
        ),
    )


__all__ = [
    "SYMBOL",
    "activate_v5_run",
    "rewrite_no_history",
    "write_v5_run",
]
