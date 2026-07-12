"""Reusable exact V5 native-history fixtures for production-boundary tests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from pm_native_activation import sha256_file
from pm_native_intervals import native_source_interval
from pm_native_parity import ParityThresholds
from tools.acquire_native_history import (
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    DEFAULT_TIMEFRAMES,
    PAGINATION_CONTRACT_VERSION,
    SCHEMA_VERSION,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
    TWO_REFERENCE_CONSENSUS_POLICY,
    cross_resolution_reference_map,
)
from tools.audit_activate_native_history import audit_and_activate


UTC = timezone.utc
SYMBOL = "EURUSD"
SOURCE_ENVIRONMENT = {
    "account": {
        "login": 42,
        "server": "TestBroker-Demo",
        "company": "TestBroker",
        "currency": "USD",
    },
    "terminal": {
        "name": "Test Terminal",
        "company": "TestBroker",
        "build": 5432,
        "max_bars": 10000,
    },
}


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _interval(start: Any, end: Any) -> dict[str, str]:
    return {
        "start_utc": pd.Timestamp(start).isoformat(),
        "end_exclusive_utc": pd.Timestamp(end).isoformat(),
    }


def _v5_contract(origin: pd.Timestamp, end: pd.Timestamp):
    partitions = {
        "optimizer_train": (
            origin + pd.Timedelta(days=10),
            origin + pd.Timedelta(days=40),
        ),
        "tuning_validation": (
            origin + pd.Timedelta(days=40),
            origin + pd.Timedelta(days=70),
        ),
        "final_lockbox": (
            origin + pd.Timedelta(days=70),
            origin + pd.Timedelta(days=100),
        ),
    }
    boundaries = {
        timeframe: {
            partition: {
                "start_ts": start.isoformat(),
                "end_ts": finish.isoformat(),
            }
            for partition, (start, finish) in partitions.items()
        }
        for timeframe in DEFAULT_TIMEFRAMES
    }
    years = {
        timeframe: dict.fromkeys(
            ("optimizer_train", "tuning_validation", "final_lockbox"),
            1,
        )
        for timeframe in DEFAULT_TIMEFRAMES
    }
    graph = cross_resolution_reference_map(DEFAULT_TIMEFRAMES)
    consumers = {timeframe: [] for timeframe in DEFAULT_TIMEFRAMES}
    for target, references in graph.items():
        for reference in references:
            consumers[reference].append(target)
    windows = {
        timeframe: {
            "strategy_full_context": _interval(origin, end),
            **{
                partition: _interval(start, finish)
                for partition, (start, finish) in partitions.items()
            },
        }
        for timeframe in DEFAULT_TIMEFRAMES
    }
    strategy_ranges = {
        timeframe: native_source_interval(
            timeframe,
            origin.to_pydatetime(),
            end.to_pydatetime(),
        )
        for timeframe in DEFAULT_TIMEFRAMES
    }
    strategy = {
        timeframe: _interval(*strategy_ranges[timeframe])
        for timeframe in DEFAULT_TIMEFRAMES
    }
    reference_ranges = {
        target: {
            reference: {
                window: native_source_interval(
                    reference,
                    pd.Timestamp(interval["start_utc"]).to_pydatetime(),
                    pd.Timestamp(interval["end_exclusive_utc"]).to_pydatetime(),
                )
                for window, interval in windows[target].items()
            }
            for reference in graph[target]
        }
        for target in DEFAULT_TIMEFRAMES
    }
    reference_intervals = {
        target: {
            reference: {
                window: _interval(*interval)
                for window, interval in reference_ranges[target][reference].items()
            }
            for reference in graph[target]
        }
        for target in DEFAULT_TIMEFRAMES
    }
    source_parts = {
        timeframe: [strategy_ranges[timeframe]] for timeframe in DEFAULT_TIMEFRAMES
    }
    for target, references in reference_ranges.items():
        for reference, intervals in references.items():
            source_parts[reference].extend(intervals.values())
    source = {
        timeframe: _interval(
            min(interval[0] for interval in intervals),
            max(interval[1] for interval in intervals),
        )
        for timeframe, intervals in source_parts.items()
    }
    audit_only = {timeframe: [] for timeframe in DEFAULT_TIMEFRAMES}
    leads = {timeframe: origin.isoformat() for timeframe in DEFAULT_TIMEFRAMES}
    common = {
        "horizon_years": 1,
        "requested_information_cutoff_utc": end.isoformat(),
        "requested_horizon_start_utc": origin.isoformat(),
        "common_requested_start_utc": origin.isoformat(),
        "required_evidence_lead_start_utc": origin.isoformat(),
        "warmup_days": 0.0,
        "safety_days": 0.0,
        "closed_bar_slack_seconds": TIMEFRAME_SECONDS["D1"],
        "common_as_of_utc": end.isoformat(),
        "boundary_tolerance_days": 3.0,
        "window_years": years["M5"],
        "embargo_days": 0.0,
        "common_calendar_boundaries": boundaries["M5"],
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "timeframe_request_map_schema_version": TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
        "timeframe_window_years": years,
        "timeframe_calendar_boundaries": boundaries,
        "timeframe_calendar_boundaries_sha256": _digest(boundaries),
        "timeframe_required_evidence_lead_start_utc": leads,
        "timeframe_source_request_intervals": source,
        "timeframe_strategy_evidence_intervals": strategy,
        "timeframe_audit_reference_map": graph,
        "timeframe_audit_consumer_map": consumers,
        "timeframe_audit_windows": windows,
        "timeframe_audit_reference_intervals": reference_intervals,
        "timeframe_audit_only_intervals": audit_only,
        "timeframe_consensus_policy": dict(TWO_REFERENCE_CONSENSUS_POLICY),
    }
    hash_payload = {
        "schema_version": TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "common_as_of_utc": end.isoformat(),
        **{
            field: common[field]
            for field in (
                "timeframe_window_years",
                "timeframe_calendar_boundaries",
                "timeframe_required_evidence_lead_start_utc",
                "timeframe_source_request_intervals",
                "timeframe_strategy_evidence_intervals",
                "timeframe_audit_reference_map",
                "timeframe_audit_consumer_map",
                "timeframe_audit_windows",
                "timeframe_audit_reference_intervals",
                "timeframe_audit_only_intervals",
                "timeframe_consensus_policy",
            )
        },
    }
    common["timeframe_request_map_sha256"] = _digest(hash_payload)
    leaves = {
        timeframe: {
            **common,
            "window_years": years[timeframe],
            "common_calendar_boundaries": boundaries[timeframe],
            "cell_timeframe": timeframe,
            "cell_timeframe_window_years": years[timeframe],
            "cell_calendar_boundaries": boundaries[timeframe],
            "cell_required_evidence_lead_start_utc": leads[timeframe],
            "source_request_interval": source[timeframe],
            "strategy_evidence_interval": strategy[timeframe],
            "audit_reference_timeframes": graph[timeframe],
            "audit_consumer_targets": consumers[timeframe],
            "audit_windows": windows[timeframe],
            "audit_reference_intervals": reference_intervals[timeframe],
            "audit_only_intervals": audit_only[timeframe],
        }
        for timeframe in DEFAULT_TIMEFRAMES
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


def _resample(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    return frame.resample(rule).agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    ).dropna()


def _pagination(timeframe: str, leaf_contract: dict[str, Any]):
    start = pd.Timestamp(leaf_contract["source_request_interval"]["start_utc"])
    end = pd.Timestamp(leaf_contract["source_request_interval"]["end_exclusive_utc"])
    seconds = TIMEFRAME_SECONDS[timeframe]
    slots = min(
        max(1, DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds),
        int(SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90),
    )
    page_seconds = slots * seconds
    attempts = []
    cursor = start
    while cursor < end:
        page_end = min(cursor + pd.Timedelta(seconds=page_seconds), end)
        attempts.append(
            {
                "start_utc": cursor.isoformat(),
                "end_exclusive_utc": page_end.isoformat(),
                "attempts": 1,
                "copy_rates_range_attempts": 1,
            }
        )
        cursor = page_end
    request = {
        **leaf_contract,
        "native_fetch_start_utc": start.isoformat(),
        "native_fetch_end_exclusive_utc": end.isoformat(),
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
        "page_span_seconds": page_seconds,
        "pages_requested": len(attempts),
        "pages_with_data": len(attempts),
        "empty_pages": [],
        "failed_pages": [],
        "truncated_pages": [],
        "attempts_total": len(attempts),
        "copy_rates_range_attempts_total": len(attempts),
        "page_attempts": attempts,
        "retries_per_page": 3,
    }


def _write_leaf(
    native_root: Path,
    run_id: str,
    timeframe: str,
    frame: pd.DataFrame,
    partitions: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
    leaf_contract: dict[str, Any],
):
    leaf = native_root / "runs" / run_id / SYMBOL / timeframe
    leaf.mkdir(parents=True, exist_ok=False)
    seconds = TIMEFRAME_SECONDS[timeframe]
    frame = frame.copy()
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(seconds=seconds)
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
    request, pagination = _pagination(timeframe, leaf_contract)
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "symbol": SYMBOL,
        "canonical_symbol": SYMBOL,
        "timeframe": timeframe,
        "timeframe_seconds": seconds,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "status": "success",
        "failure_status": None,
        "eligible_for_optimization": False,
        "eligible_for_downstream_validation": True,
        "complete": True,
        "artifact": {
            "relative_path": "bars.csv",
            "sha256": digest,
            "bytes": bars_path.stat().st_size,
            "rows": len(frame),
        },
        "sha256": digest,
        "rows": len(frame),
        "start_ts": frame.index[0].isoformat(),
        "end_ts": frame.index[-1].isoformat(),
        "instrument": {"parity_tick_tolerance": 0.0001},
        "source_environment": SOURCE_ENVIRONMENT,
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
        "symbol": SYMBOL,
        "timeframe": timeframe,
        "status": "success",
        "failure_status": None,
        "rows": len(frame),
        "sha256": digest,
    }


def write_v5_run(
    native_root: Path,
    run_id: str,
    *,
    corrupt_timeframe: Optional[str] = None,
    context_only: bool = False,
    price_offset: float = 0.0,
) -> None:
    """Write one exact bounded 100-day, six-timeframe V5 run."""

    origin = pd.Timestamp("2025-01-01", tz="UTC")
    end = origin + pd.Timedelta(days=100)
    contract, leaves, partitions = _v5_contract(origin, end)
    m5 = _bars(origin, price_offset=price_offset)
    frames = {
        "M5": m5,
        "M15": _resample(m5, "15min"),
        "M30": _resample(m5, "30min"),
        "H1": _resample(m5, "1h"),
        "H4": _resample(m5, "4h"),
        "D1": _resample(m5, "1D"),
    }
    if corrupt_timeframe:
        frame = frames[corrupt_timeframe].copy()
        if context_only:
            candidates = frame.index < partitions["optimizer_train"][0]
            location = frame.index[candidates][2]
        else:
            location = frame.index[
                frame.index >= partitions["optimizer_train"][0]
            ][2]
        frame.loc[location, ["Open", "High", "Low", "Close"]] += 5.0
        frames[corrupt_timeframe] = frame
    results = [
        _write_leaf(
            native_root,
            run_id,
            timeframe,
            frames[timeframe],
            partitions,
            leaves[timeframe],
        )
        for timeframe in DEFAULT_TIMEFRAMES
    ]
    manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "immutable_record": True,
        "complete": True,
        "run_id": run_id,
        "request": {
            "symbols": [SYMBOL],
            "timeframes": list(DEFAULT_TIMEFRAMES),
            "retries": 3,
            **contract,
        },
        "source_environment": SOURCE_ENVIRONMENT,
        "results": results,
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
            "attempts": 3,
            "copy_rates_range_attempts": 3,
        }
        for page in provenance["pagination"]["page_attempts"]
    ]
    provenance.update(
        {
            "status": "failed",
            "failure_status": "no_history",
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "complete": False,
            "artifact": None,
            "sha256": None,
            "rows": 0,
            "start_ts": None,
            "end_ts": None,
        }
    )
    provenance["pagination"].update(
        {
            "pages_with_data": 0,
            "empty_pages": page_attempts,
            "failed_pages": [],
            "truncated_pages": [],
            "attempts_total": len(page_attempts) * 3,
            "copy_rates_range_attempts_total": len(page_attempts) * 3,
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
            "failure_status": "no_history",
            "rows": 0,
            "sha256": None,
        }
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def activate_v5_run(native_root: Path, run_id: str, activation_id: str):
    return audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=[SYMBOL],
        timeframes=DEFAULT_TIMEFRAMES,
        activation_id=activation_id,
        thresholds=ParityThresholds(min_compared_native_bars=20),
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )


__all__ = [
    "SYMBOL",
    "activate_v5_run",
    "rewrite_no_history",
    "write_v5_run",
]
