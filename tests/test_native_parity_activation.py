import json
import hashlib
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


PATCH_ROOT = Path(__file__).resolve().parents[1]
if str(PATCH_ROOT) not in sys.path:
    sys.path.insert(0, str(PATCH_ROOT))

from pm_native_activation import (  # noqa: E402
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_parity import (  # noqa: E402
    ParityThresholds,
    audit_native_vs_session_m5,
)
from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    SCHEMA_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
    expected_session_coverage_by_partition,
    segmented_session_coverage_by_partition_v2,
)
from tools.audit_activate_native_history import audit_and_activate  # noqa: E402


UTC = timezone.utc
_SOURCE_ENVIRONMENT = {
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


def _request_contract(partition_windows, timeframes):
    common_start = pd.Timestamp(partition_windows["optimizer_train"][0])
    common_as_of = pd.Timestamp(partition_windows["final_lockbox"][1])
    return {
        "horizon_years": 1,
        "requested_information_cutoff_utc": common_as_of.isoformat(),
        "requested_horizon_start_utc": common_start.isoformat(),
        "common_requested_start_utc": common_start.isoformat(),
        "required_evidence_lead_start_utc": common_start.isoformat(),
        "warmup_days": 0.0,
        "safety_days": 0.0,
        "closed_bar_slack_seconds": max(TIMEFRAME_SECONDS[tf] for tf in timeframes),
        "common_as_of_utc": common_as_of.isoformat(),
        "boundary_tolerance_days": 3.0,
        "window_years": {
            "optimizer_train": 1,
            "tuning_validation": 1,
            "final_lockbox": 1,
        },
        "embargo_days": 0.0,
        "common_calendar_boundaries": {
            partition: {
                "start_ts": pd.Timestamp(start).isoformat(),
                "end_ts": pd.Timestamp(end).isoformat(),
            }
            for partition, (start, end) in partition_windows.items()
        },
    }


def _canonical_json_sha256(value):
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _timeframe_request_contract(timeframe_windows, timeframes):
    serialized = {
        timeframe: {
            partition: {
                "start_ts": pd.Timestamp(start).isoformat(),
                "end_ts": pd.Timestamp(end).isoformat(),
            }
            for partition, (start, end) in timeframe_windows[timeframe].items()
        }
        for timeframe in timeframes
    }
    common_as_of = pd.Timestamp(
        timeframe_windows[timeframes[0]]["final_lockbox"][1]
    )
    common_start = min(
        pd.Timestamp(timeframe_windows[timeframe]["optimizer_train"][0])
        for timeframe in timeframes
    )
    timeframe_years = {
        timeframe: {
            "optimizer_train": 5 + index,
            "tuning_validation": 2,
            "final_lockbox": 2,
        }
        for index, timeframe in enumerate(timeframes)
    }
    leads = {
        timeframe: pd.Timestamp(
            timeframe_windows[timeframe]["optimizer_train"][0]
        ).isoformat()
        for timeframe in timeframes
    }
    intervals = {
        timeframe: {
            "start_utc": pd.Timestamp(
                timeframe_windows[timeframe]["optimizer_train"][0]
            ).isoformat(),
            "end_exclusive_utc": common_as_of.isoformat(),
        }
        for timeframe in timeframes
    }
    compatibility_timeframe = "M5" if "M5" in timeframes else timeframes[0]
    contract = {
        "horizon_years": 9,
        "requested_information_cutoff_utc": common_as_of.isoformat(),
        "requested_horizon_start_utc": common_start.isoformat(),
        "common_requested_start_utc": common_start.isoformat(),
        "required_evidence_lead_start_utc": common_start.isoformat(),
        "warmup_days": 0.0,
        "safety_days": 0.0,
        "closed_bar_slack_seconds": max(
            TIMEFRAME_SECONDS[timeframe] for timeframe in timeframes
        ),
        "common_as_of_utc": common_as_of.isoformat(),
        "boundary_tolerance_days": 3.0,
        "window_years": timeframe_years[compatibility_timeframe],
        "embargo_days": 0.0,
        "common_calendar_boundaries": serialized[compatibility_timeframe],
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "timeframe_request_map_schema_version": (
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        ),
        "timeframe_window_years": timeframe_years,
        "timeframe_calendar_boundaries": serialized,
        "timeframe_calendar_boundaries_sha256": _canonical_json_sha256(serialized),
        "timeframe_required_evidence_lead_start_utc": leads,
        "timeframe_source_request_intervals": intervals,
    }
    request_map_payload = {
        "schema_version": TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "common_as_of_utc": common_as_of.isoformat(),
        "timeframe_window_years": timeframe_years,
        "timeframe_calendar_boundaries": serialized,
        "timeframe_required_evidence_lead_start_utc": leads,
        "timeframe_source_request_intervals": intervals,
    }
    contract["timeframe_request_map_sha256"] = _canonical_json_sha256(
        request_map_payload
    )
    leaves = {
        timeframe: {
            **contract,
            "window_years": timeframe_years[timeframe],
            "common_calendar_boundaries": serialized[timeframe],
            "cell_timeframe": timeframe,
            "cell_timeframe_window_years": timeframe_years[timeframe],
            "cell_calendar_boundaries": serialized[timeframe],
            "cell_required_evidence_lead_start_utc": leads[timeframe],
            "source_request_interval": intervals[timeframe],
        }
        for timeframe in timeframes
    }
    return contract, leaves


def _pagination_contract(timeframe, request_contract, source_interval=None):
    seconds = TIMEFRAME_SECONDS[timeframe]
    common_start = pd.Timestamp(request_contract["common_requested_start_utc"])
    common_as_of = pd.Timestamp(request_contract["common_as_of_utc"])
    if source_interval is not None:
        fetch_start = pd.Timestamp(source_interval["start_utc"])
        fetch_end = pd.Timestamp(source_interval["end_exclusive_utc"])
    elif timeframe in {"H4", "D1"}:
        fetch_start = common_start - pd.Timedelta(seconds=seconds)
        fetch_end = common_as_of + pd.Timedelta(seconds=1)
    else:
        fetch_start = common_start.ceil(f"{seconds}s")
        fetch_end = common_as_of.floor(f"{seconds}s")
    if timeframe in {"H4", "D1"}:
        alignment_mode = "broker_observed_successor_or_nominal_close_v1"
        sentinel_slack = 1
    else:
        alignment_mode = "fixed_utc_native_boundary_v1"
        sentinel_slack = 0
    configured_slots = max(1, DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds)
    configured_slots = min(configured_slots, int(_SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90))
    page_span_seconds = configured_slots * seconds
    pages_requested = int(
        np.ceil((fetch_end - fetch_start).total_seconds() / page_span_seconds)
    )
    request = {
        **request_contract,
        "native_fetch_start_utc": fetch_start.isoformat(),
        "native_fetch_end_exclusive_utc": fetch_end.isoformat(),
        "alignment_mode": alignment_mode,
        "successor_sentinel_fetch_slack_seconds": sentinel_slack,
    }
    pagination = {
        "page_span_seconds": page_span_seconds,
        "pages_requested": pages_requested,
        "pages_with_data": pages_requested,
        "empty_pages": [],
        "failed_pages": [],
        "truncated_pages": [],
        "attempts_total": pages_requested,
        "retries_per_page": 3,
    }
    return request, pagination


def _m5_history(start="2025-01-06", days=30):
    index = pd.date_range(start, periods=days * 24 * 12, freq="5min", tz="UTC")
    close = 100.0 + np.arange(len(index), dtype=float) * 0.0001
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 0.0002,
            "Low": close - 0.0002,
            "Close": close + 0.00005,
            "Volume": 1,
        },
        index=index,
    )


def _weekly_m5_history(start="2025-01-06", weeks=50, *, omitted_weeks=()):
    frames = []
    origin = pd.Timestamp(start, tz="UTC")
    for week in range(weeks):
        if week in set(omitted_weeks):
            continue
        index = pd.date_range(
            origin + pd.Timedelta(weeks=week),
            periods=24 * 12,
            freq="5min",
        )
        close = 100.0 + np.arange(len(index), dtype=float) * 0.0001 + week
        frames.append(
            pd.DataFrame(
                {
                    "Open": close,
                    "High": close + 0.0002,
                    "Low": close - 0.0002,
                    "Close": close + 0.00005,
                    "Volume": 1,
                },
                index=index,
            )
        )
    return pd.concat(frames).sort_index()


def _resample(frame, rule):
    return frame.resample(rule).agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    ).dropna()


def _write_source(
    native_root,
    run_id,
    symbol,
    timeframe,
    frame,
    partition_windows,
    request_contract,
    *,
    tolerance=0.0001,
):
    leaf = native_root / "runs" / run_id / symbol / timeframe
    leaf.mkdir(parents=True, exist_ok=False)
    bars = leaf / "bars.csv"
    frame = frame.copy()
    duration = {
        "M5": pd.Timedelta(minutes=5),
        "H1": pd.Timedelta(hours=1),
        "D1": pd.Timedelta(days=1),
    }[timeframe]
    frame["_BAR_CLOSE_TS"] = frame.index + duration
    frame.to_csv(bars, index_label="time", lineterminator="\n")
    digest = sha256_file(bars)
    coverage, coverage_details = expected_session_coverage_by_partition(
        frame,
        partition_windows,
    )
    source_interval = (
        request_contract.get("source_request_interval")
        if isinstance(request_contract, dict)
        else None
    )
    provenance_request, pagination = _pagination_contract(
        timeframe,
        request_contract,
        source_interval=source_interval,
    )
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "symbol": symbol,
        "canonical_symbol": symbol,
        "timeframe": timeframe,
        "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
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
            "bytes": bars.stat().st_size,
            "rows": len(frame),
        },
        "sha256": digest,
        "rows": len(frame),
        "start_ts": frame.index[0].isoformat(),
        "end_ts": frame.index[-1].isoformat(),
        "instrument": {"parity_tick_tolerance": tolerance},
        "source_environment": _SOURCE_ENVIRONMENT,
        "expected_session_coverage_by_partition": coverage,
        "expected_session_coverage_details_by_partition": coverage_details,
        "observed_session_bars_by_partition": {
            partition: int(coverage_details["partitions"][partition]["observed_rows"])
            for partition in coverage_details["partitions"]
        },
        "expected_session_bars_by_partition": {
            partition: float(
                coverage_details["partitions"][partition][
                    "expected_rows_from_train_session_rate"
                ]
            )
            for partition in coverage_details["partitions"]
        },
        "request": provenance_request,
        "pagination": pagination,
    }
    (leaf / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "status": "success",
        "rows": len(frame),
        "sha256": digest,
    }


def _write_run(
    native_root,
    run_id,
    *,
    mismatch=False,
    tolerance=0.0001,
    coverage_mode="pass",
):
    origin = pd.Timestamp("2025-01-06", tz="UTC")
    partition_windows = {
        "optimizer_train": (
            origin.to_pydatetime(),
            (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
        ),
        "tuning_validation": (
            (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
            (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
        ),
        "final_lockbox": (
            (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
            (origin + pd.Timedelta(weeks=50)).to_pydatetime(),
        ),
    }
    request_contract = _request_contract(partition_windows, ("M5", "H1"))
    omitted_weeks = {
        "pass": (),
        "boundary": (30,),
        "low": (30, 31),
    }[coverage_mode]
    m5 = _weekly_m5_history(weeks=50, omitted_weeks=omitted_weeks)
    h1 = _resample(m5, "1h")
    if mismatch:
        h1.loc[h1.index[5], "Close"] += 1.0
    results = [
        _write_source(
            native_root,
            run_id,
            "EURUSD",
            "M5",
            m5,
            partition_windows,
            request_contract,
            tolerance=tolerance,
        ),
        _write_source(
            native_root,
            run_id,
            "EURUSD",
            "H1",
            h1,
            partition_windows,
            request_contract,
            tolerance=tolerance,
        ),
    ]
    run_manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "immutable_record": True,
        "complete": True,
        "run_id": run_id,
        "request": {
            "symbols": ["EURUSD"],
            "timeframes": ["M5", "H1"],
            "retries": 3,
            **request_contract,
        },
        "source_environment": _SOURCE_ENVIRONMENT,
        "results": results,
    }
    (native_root / "runs" / run_id / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return m5, h1


def _write_timeframe_v3_run(native_root, run_id):
    origin = pd.Timestamp("2025-01-06", tz="UTC")
    active_windows = {
        "tuning_validation": (
            (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
            (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
        ),
        "final_lockbox": (
            (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
            (origin + pd.Timedelta(weeks=50)).to_pydatetime(),
        ),
    }
    timeframe_windows = {
        "M5": {
            "optimizer_train": (
                origin.to_pydatetime(),
                (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
            ),
            **active_windows,
        },
        "H1": {
            "optimizer_train": (
                (origin - pd.Timedelta(weeks=10)).to_pydatetime(),
                (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
            ),
            **active_windows,
        },
    }
    run_contract, leaf_contracts = _timeframe_request_contract(
        timeframe_windows,
        ("M5", "H1"),
    )
    m5 = _weekly_m5_history(start=origin.strftime("%Y-%m-%d"), weeks=50)
    h1_early_source = _weekly_m5_history(
        start=(origin - pd.Timedelta(weeks=10)).strftime("%Y-%m-%d"),
        weeks=10,
    )
    h1 = pd.concat(
        [_resample(h1_early_source, "1h"), _resample(m5, "1h")]
    ).sort_index()
    frames = {"M5": m5, "H1": h1}
    results = []
    for timeframe in ("M5", "H1"):
        result = _write_source(
            native_root,
            run_id,
            "EURUSD",
            timeframe,
            frames[timeframe],
            timeframe_windows[timeframe],
            leaf_contracts[timeframe],
        )
        results.append(result)
        provenance_path = (
            native_root
            / "runs"
            / run_id
            / "EURUSD"
            / timeframe
            / "provenance.json"
        )
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        coverage, details = segmented_session_coverage_by_partition_v2(
            frames[timeframe],
            timeframe_windows[timeframe],
        )
        provenance.update(
            {
                "expected_session_coverage_by_partition": coverage,
                "expected_session_coverage_details_by_partition": details,
                "observed_session_bars_by_partition": {
                    partition: int(detail["observed_rows"])
                    for partition, detail in details["partitions"].items()
                },
                "expected_session_bars_by_partition": {
                    partition: float(
                        detail["expected_rows_from_segmented_session_schedule"]
                    )
                    for partition, detail in details["partitions"].items()
                },
            }
        )
        provenance_path.write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    run_manifest = {
        "schema_version": "fxpm.native_history_run_manifest.v1",
        "immutable_record": True,
        "complete": True,
        "run_id": run_id,
        "request": {
            "symbols": ["EURUSD"],
            "timeframes": ["M5", "H1"],
            "retries": 3,
            **run_contract,
        },
        "source_environment": _SOURCE_ENVIRONMENT,
        "results": results,
    }
    (native_root / "runs" / run_id / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_m5_as_no_history(native_root, run_id):
    leaf = native_root / "runs" / run_id / "EURUSD" / "M5"
    bars_path = leaf / "bars.csv"
    bars_path.unlink()
    provenance_path = leaf / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    request = provenance["request"]
    pagination = provenance["pagination"]
    start = pd.Timestamp(request["native_fetch_start_utc"])
    end = pd.Timestamp(request["native_fetch_end_exclusive_utc"])
    span = pd.Timedelta(seconds=int(pagination["page_span_seconds"]))
    empty_pages = []
    cursor = start
    while cursor < end:
        page_end = min(cursor + span, end)
        empty_pages.append(
            {
                "start_utc": cursor.isoformat(),
                "end_exclusive_utc": page_end.isoformat(),
                "attempts": 3,
            }
        )
        cursor = page_end
    pagination.update(
        {
            "pages_requested": len(empty_pages),
            "pages_with_data": 0,
            "empty_pages": empty_pages,
            "failed_pages": [],
            "truncated_pages": [],
            "attempts_total": len(empty_pages) * 3,
            "retries_per_page": 3,
        }
    )
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
            "pagination": pagination,
        }
    )
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    m5_cell = next(
        item for item in manifest["results"] if item["timeframe"] == "M5"
    )
    m5_cell.update(
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


def _set_session_coverage(
    native_root,
    run_id,
    timeframe,
    values,
    *,
    remove_partition=None,
):
    path = native_root / "runs" / run_id / "EURUSD" / timeframe / "provenance.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["expected_session_coverage_by_partition"].update(values)
    if remove_partition is not None:
        payload["expected_session_coverage_by_partition"].pop(remove_partition, None)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_source_frame(
    native_root,
    run_id,
    timeframe,
    frame,
    *,
    status,
    failure_status,
):
    bars_path = native_root / "runs" / run_id / "EURUSD" / timeframe / "bars.csv"
    provenance_path = bars_path.with_name("provenance.json")
    frame.to_csv(bars_path, index_label="time", lineterminator="\n")
    digest = sha256_file(bars_path)
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    raw_boundaries = provenance["request"]["common_calendar_boundaries"]
    windows = {
        partition: (
            pd.Timestamp(boundary["start_ts"]).to_pydatetime(),
            pd.Timestamp(boundary["end_ts"]).to_pydatetime(),
        )
        for partition, boundary in raw_boundaries.items()
    }
    coverage, details = expected_session_coverage_by_partition(frame, windows)
    detail_partitions = details.get("partitions", {})
    provenance.update(
        {
            "status": status,
            "failure_status": failure_status,
            "eligible_for_downstream_validation": status == "success",
            "complete": status == "success",
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
            "expected_session_coverage_by_partition": coverage,
            "expected_session_coverage_details_by_partition": details,
            "observed_session_bars_by_partition": {
                partition: (
                    int(detail_partitions[partition]["observed_rows"])
                    if partition in detail_partitions
                    else None
                )
                for partition in ("optimizer_train", "tuning_validation", "final_lockbox")
            },
            "expected_session_bars_by_partition": {
                partition: (
                    float(
                        detail_partitions[partition][
                            "expected_rows_from_train_session_rate"
                        ]
                    )
                    if partition in detail_partitions
                    else None
                )
                for partition in ("optimizer_train", "tuning_validation", "final_lockbox")
            },
        }
    )
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    run_manifest_path = native_root / "runs" / run_id / "run_manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_cell = next(
        item
        for item in run_manifest["results"]
        if item["symbol"] == "EURUSD" and item["timeframe"] == timeframe
    )
    run_cell.update(
        {
            "status": status,
            "failure_status": failure_status,
            "eligible_for_downstream_validation": status == "success",
            "rows": len(frame),
            "sha256": digest,
        }
    )
    run_manifest_path.write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_exact_native_m5_parity_passes_with_membership_and_ohlc():
    m5 = _m5_history(days=3)
    h1 = _resample(m5, "1h")

    report = audit_native_vs_session_m5(
        m5,
        h1,
        symbol="EURUSD",
        timeframe="H1",
        tick_tolerance=0.0001,
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "PASS", report["reason_codes"]
    assert report["metrics"]["bar_membership_ratio"] == 1.0
    assert report["metrics"]["ohlc_mismatch_bars"] == 0
    assert report["metrics"]["unassigned_m5_rows"] == 0


def test_missing_tick_tolerance_is_explicitly_pending():
    m5 = _m5_history(days=3)
    h1 = _resample(m5, "1h")

    report = audit_native_vs_session_m5(
        m5,
        h1,
        symbol="EURUSD",
        timeframe="H1",
        tick_tolerance=None,
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "PENDING"
    assert "TICK_TOLERANCE_UNAVAILABLE" in report["reason_codes"]


def test_parity_report_labels_dst_and_holiday_candidate_weeks():
    m5 = _m5_history(start="2025-03-01", days=35)
    opens = list(pd.date_range("2025-03-01T22:00:00Z", periods=31, freq="24h"))
    # Shift broker daily open one UTC hour at DST, then remove one session to
    # create a holiday/short-week candidate.
    opens = [ts if i < 15 else ts - pd.Timedelta(hours=1) for i, ts in enumerate(opens)]
    del opens[17]
    rows = []
    kept_opens = []
    for i, start in enumerate(opens[:-1]):
        following = opens[i + 1]
        delta = following - start
        end = following if pd.Timedelta(hours=12) <= delta <= pd.Timedelta(hours=36) else start + pd.Timedelta(days=1)
        group = m5[(m5.index >= start) & (m5.index < end)]
        if group.empty:
            continue
        kept_opens.append(start)
        rows.append(
            {
                "Open": group["Open"].iloc[0],
                "High": group["High"].max(),
                "Low": group["Low"].min(),
                "Close": group["Close"].iloc[-1],
                "Volume": group["Volume"].sum(),
            }
        )
    native = pd.DataFrame(rows, index=pd.DatetimeIndex(kept_opens))

    report = audit_native_vs_session_m5(
        m5,
        native,
        symbol="EURUSD",
        timeframe="D1",
        tick_tolerance=0.0001,
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["diagnostics"]["dst_transition_weeks"]
    assert report["diagnostics"]["holiday_or_short_session_candidate_weeks"]
    assert "weekly" in report["diagnostics"]


def test_successful_activation_pins_exact_hashes_and_resolver_uses_no_flat_fallback():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-good")

        result = audit_and_activate(
            native_root,
            run_id="run-good",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-good",
            thresholds=ParityThresholds(min_compared_native_bars=20),
            now=datetime(2026, 1, 1, tzinfo=UTC),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        assert result["run_manifest_sha256"] == sha256_file(
            native_root / "runs" / "run-good" / "run_manifest.json"
        )
        resolution = resolve_active_native_artifact(data_dir, "EURUSD", "H1")
        assert resolution.valid, resolution.errors
        assert resolution.data_path == native_root / "runs" / "run-good" / "EURUSD" / "H1" / "bars.csv"
        assert "audits/activation-good" in str(resolution.sidecar_path).replace("\\", "/")

        # A tempting flat file is ignored; the hash-pinned run remains the only authority.
        flat = native_root / "EURUSD_H1.csv"
        flat.write_text("malicious fallback", encoding="utf-8")
        again = resolve_active_native_artifact(data_dir, "EURUSD", "H1")
        assert again.valid
        assert again.data_path == resolution.data_path


def test_segmented_v2_coverage_is_recomputed_and_authenticated():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-segmented-v2"
        _write_run(native_root, run_id)
        run_manifest_path = native_root / "runs" / run_id / "run_manifest.json"
        run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
        raw_boundaries = run_manifest["request"]["common_calendar_boundaries"]
        windows = {
            partition: (
                pd.Timestamp(boundary["start_ts"]).to_pydatetime(),
                pd.Timestamp(boundary["end_ts"]).to_pydatetime(),
            )
            for partition, boundary in raw_boundaries.items()
        }
        for timeframe in ("M5", "H1"):
            leaf = native_root / "runs" / run_id / "EURUSD" / timeframe
            frame = pd.read_csv(leaf / "bars.csv")
            frame.index = pd.to_datetime(frame.pop("time"), utc=True)
            coverage, details = segmented_session_coverage_by_partition_v2(
                frame,
                windows,
            )
            provenance_path = leaf / "provenance.json"
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
            provenance.update(
                {
                    "expected_session_coverage_by_partition": coverage,
                    "expected_session_coverage_details_by_partition": details,
                    "observed_session_bars_by_partition": {
                        partition: int(detail["observed_rows"])
                        for partition, detail in details["partitions"].items()
                    },
                    "expected_session_bars_by_partition": {
                        partition: float(
                            detail["expected_rows_from_segmented_session_schedule"]
                        )
                        for partition, detail in details["partitions"].items()
                    },
                }
            )
            provenance_path.write_text(
                json.dumps(provenance, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-segmented-v2",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["universe_eligibility"]["session_coverage_method"] == (
            SEGMENTED_SESSION_COVERAGE_METHOD_V2
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors


def test_timeframe_v3_cell_windows_activate_and_resolve_independently():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_timeframe_v3_run(native_root, "run-timeframe-v3")

        result = audit_and_activate(
            native_root,
            run_id="run-timeframe-v3",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v3",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        calendar = universe.calendar_contract
        assert calendar["window_contract_version"] == (
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        )
        assert calendar["timeframe_calendar_boundaries"]["M5"] != (
            calendar["timeframe_calendar_boundaries"]["H1"]
        )
        assert resolve_active_native_artifact(data_dir, "EURUSD", "H1").valid


def test_timeframe_v3_request_map_tamper_blocks_activation():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_timeframe_v3_run(native_root, "run-timeframe-v3-tamper")
        provenance_path = (
            native_root
            / "runs"
            / "run-timeframe-v3-tamper"
            / "EURUSD"
            / "H1"
            / "provenance.json"
        )
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        provenance["request"]["timeframe_calendar_boundaries"]["H1"][
            "optimizer_train"
        ]["start_ts"] = "2024-01-01T00:00:00+00:00"
        provenance_path.write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        result = audit_and_activate(
            native_root,
            run_id="run-timeframe-v3-tamper",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v3-tamper",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["entries"]["EURUSD"]["H1"]["status"] == "failed"
        assert any(
            "SOURCE_REQUEST_CONTRACT_INVALID" in reason
            for reason in result["entries"]["EURUSD"]["H1"]["reason_codes"]
        )


def test_proven_no_history_m5_excludes_only_m5_and_does_not_veto_h1():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-m5-unavailable")
        _rewrite_m5_as_no_history(native_root, "run-m5-unavailable")

        result = audit_and_activate(
            native_root,
            run_id="run-m5-unavailable",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-m5-unavailable",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["universe_eligibility"]["cell_statuses"]["EURUSD"] == {
            "M5": "excluded_no_trade",
            "H1": "active",
        }
        h1_entry = result["entries"]["EURUSD"]["H1"]
        parity = json.loads(
            (native_root / h1_entry["parity_relative_path"]).read_text(
                encoding="utf-8"
            )
        )
        assert parity["status"] == "NOT_APPLICABLE"
        assert parity["reason_codes"] == [
            "M5_ACQUISITION_UNAVAILABLE:NO_HISTORY"
        ]
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ("H1",)
        assert universe.excluded_timeframes("EURUSD") == ("M5",)
        assert resolve_active_native_artifact(data_dir, "EURUSD", "H1").valid


def test_m5_technical_failure_still_blocks_sibling_instead_of_becoming_no_trade():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-m5-technical")
        _rewrite_m5_as_no_history(native_root, "run-m5-technical")
        provenance_path = (
            native_root
            / "runs"
            / "run-m5-technical"
            / "EURUSD"
            / "M5"
            / "provenance.json"
        )
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        provenance["failure_status"] = "fetch_failed"
        provenance_path.write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = (
            native_root / "runs" / "run-m5-technical" / "run_manifest.json"
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        next(
            item for item in manifest["results"] if item["timeframe"] == "M5"
        )["failure_status"] = "fetch_failed"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        result = audit_and_activate(
            native_root,
            run_id="run-m5-technical",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-m5-technical",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["entries"]["EURUSD"]["H1"]["status"] == "pending"
        assert result["entries"]["EURUSD"]["H1"]["audit_status"] == "PENDING"


def test_exact_session_coverage_policy_boundary_remains_eligible():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-boundary", coverage_mode="boundary")

        result = audit_and_activate(
            native_root,
            run_id="run-boundary",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-boundary",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["universe_eligibility"]["eligible_symbols"] == ["EURUSD"]
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.symbol_status("EURUSD") == "eligible"


def test_low_session_coverage_is_whole_symbol_hash_bound_no_trade_exclusion():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-low-coverage", coverage_mode="low")

        result = audit_and_activate(
            native_root,
            run_id="run-low-coverage",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-low-coverage",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        eligibility = result["universe_eligibility"]
        assert eligibility["eligible_symbols"] == []
        exclusion = eligibility["excluded_symbols"]["EURUSD"]
        assert set(exclusion["reason_codes"]) == {
            "M5:SESSION_COVERAGE_LOW:final_lockbox:0.900000<0.950000",
            "H1:SESSION_COVERAGE_LOW:final_lockbox:0.900000<0.950000",
        }
        assert all(
            entry["status"] == "excluded_no_trade"
            for entry in result["entries"]["EURUSD"].values()
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.symbol_status("EURUSD") == "excluded_no_trade"


def test_one_partial_cell_excludes_only_that_cell_and_preserves_sibling():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-mixed-partial", coverage_mode="pass")
        bars_path = (
            native_root / "runs" / "run-mixed-partial" / "EURUSD" / "H1" / "bars.csv"
        )
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        frame = frame.loc[
            frame.index >= pd.Timestamp("2025-01-06T00:00:00Z") + pd.Timedelta(weeks=10)
        ]
        _rewrite_source_frame(
            native_root,
            "run-mixed-partial",
            "H1",
            frame,
            status="partial",
            failure_status="partial_history",
        )

        result = audit_and_activate(
            native_root,
            run_id="run-mixed-partial",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-mixed-partial",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["universe_eligibility"]["eligible_symbols"] == ["EURUSD"]
        assert result["universe_eligibility"]["excluded_symbols"] == {}
        assert result["entries"]["EURUSD"]["M5"]["status"] == "active"
        assert result["entries"]["EURUSD"]["H1"]["status"] == "excluded_no_trade"
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ("M5",)
        assert universe.excluded_timeframes("EURUSD") == ("H1",)
        assert universe.cell_exclusion_binding("EURUSD", "H1")["timeframe"] == "H1"
        assert resolve_active_native_artifact(data_dir, "EURUSD", "M5").valid
        excluded = resolve_active_native_artifact(data_dir, "EURUSD", "H1")
        assert "NATIVE_ACTIVE_CELL_EXCLUDED_NO_TRADE" in excluded.errors


def test_one_low_coverage_cell_excludes_only_that_cell_with_sufficient_sibling():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-mixed-coverage", coverage_mode="pass")
        bars_path = (
            native_root / "runs" / "run-mixed-coverage" / "EURUSD" / "H1" / "bars.csv"
        )
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        origin = pd.Timestamp("2025-01-06T00:00:00Z")
        remove = pd.Series(False, index=frame.index)
        for week in (35, 36):
            start = origin + pd.Timedelta(weeks=week)
            remove |= (frame.index >= start) & (frame.index < start + pd.Timedelta(days=1))
        frame = frame.loc[~remove.to_numpy()]
        _rewrite_source_frame(
            native_root,
            "run-mixed-coverage",
            "H1",
            frame,
            status="success",
            failure_status=None,
        )

        result = audit_and_activate(
            native_root,
            run_id="run-mixed-coverage",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-mixed-coverage",
            thresholds=ParityThresholds(
                min_compared_native_bars=20,
                max_unassigned_m5_ratio=1.0,
            ),
        )

        assert result["status"] == "active", result
        eligibility = result["universe_eligibility"]
        assert eligibility["eligible_symbols"] == ["EURUSD"]
        assert eligibility["excluded_symbols"] == {}
        assert eligibility["cell_statuses"]["EURUSD"] == {
            "M5": "active",
            "H1": "excluded_no_trade",
        }
        assert result["entries"]["EURUSD"]["M5"][
            "expected_session_coverage_by_partition"
        ]["final_lockbox"] >= 0.95
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ("M5",)
        assert universe.excluded_timeframes("EURUSD") == ("H1",)
        assert universe.calendar_contract["common_as_of_utc"]
        assert set(universe.calendar_contract["common_calendar_boundaries"]) == {
            "optimizer_train",
            "tuning_validation",
            "final_lockbox",
        }


def test_daily_like_m5_is_excluded_without_suppressing_valid_d1():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-daily-like-m5"
        origin = pd.Timestamp("2025-01-06", tz="UTC")
        partition_windows = {
            "optimizer_train": (
                origin.to_pydatetime(),
                (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
            ),
            "tuning_validation": (
                (origin + pd.Timedelta(weeks=28)).to_pydatetime(),
                (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
            ),
            "final_lockbox": (
                (origin + pd.Timedelta(weeks=30)).to_pydatetime(),
                (origin + pd.Timedelta(weeks=50)).to_pydatetime(),
            ),
        }
        request_contract = _request_contract(partition_windows, ("M5", "D1"))
        index = pd.date_range(origin, periods=50 * 7, freq="D")
        close = 100.0 + np.arange(len(index), dtype=float) * 0.01
        daily = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.02,
                "Low": close - 0.02,
                "Close": close + 0.01,
                "Volume": 1,
            },
            index=index,
        )
        results = [
            _write_source(
                native_root,
                run_id,
                "AU200",
                timeframe,
                daily,
                partition_windows,
                request_contract,
            )
            for timeframe in ("M5", "D1")
        ]
        run_manifest = {
            "schema_version": "fxpm.native_history_run_manifest.v1",
            "immutable_record": True,
            "complete": True,
            "run_id": run_id,
            "request": {
                "symbols": ["AU200"],
                "timeframes": ["M5", "D1"],
                "retries": 3,
                **request_contract,
            },
            "source_environment": _SOURCE_ENVIRONMENT,
            "results": results,
        }
        (native_root / "runs" / run_id / "run_manifest.json").write_text(
            json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["AU200"],
            timeframes=["M5", "D1"],
            activation_id="activation-daily-like-m5",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["entries"]["AU200"]["M5"]["status"] == "excluded_no_trade"
        assert result["entries"]["AU200"]["D1"]["status"] == "active"
        assert any(
            reason.startswith("RESOLUTION:INTRADAY_")
            for reason in result["entries"]["AU200"]["M5"]["reason_codes"]
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["AU200"],
            required_timeframes=("M5", "D1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("AU200") == ("D1",)
        assert universe.excluded_timeframes("AU200") == ("M5",)


def test_unknown_session_coverage_fails_instead_of_excluding():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-unknown-coverage")
        _set_session_coverage(
            native_root,
            "run-unknown-coverage",
            "H1",
            {},
            remove_partition="final_lockbox",
        )

        result = audit_and_activate(
            native_root,
            run_id="run-unknown-coverage",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-unknown-coverage",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["universe_eligibility"]["excluded_symbols"] == {}


@pytest.mark.parametrize(
    "tamper_kind",
    ["ratio", "observed_count", "boundary", "method_status"],
)
def test_coverage_provenance_tamper_is_activation_failure(tamper_kind):
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        run_id = f"run-coverage-{tamper_kind}"
        _write_run(native_root, run_id)
        path = native_root / "runs" / run_id / "EURUSD" / "H1" / "provenance.json"
        provenance = json.loads(path.read_text(encoding="utf-8"))
        if tamper_kind == "ratio":
            provenance["expected_session_coverage_by_partition"]["final_lockbox"] = 0.5
        elif tamper_kind == "observed_count":
            provenance["observed_session_bars_by_partition"]["final_lockbox"] -= 1
        elif tamper_kind == "boundary":
            provenance["request"]["common_calendar_boundaries"]["final_lockbox"][
                "start_ts"
            ] = "2025-01-07T00:00:00Z"
        else:
            provenance["expected_session_coverage_details_by_partition"][
                "status"
            ] = "untrusted"
        path.write_text(json.dumps(provenance), encoding="utf-8")

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id=f"activation-coverage-{tamper_kind}",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["universe_eligibility"]["excluded_symbols"] == {}
        if tamper_kind == "method_status":
            assert result["entries"]["EURUSD"]["H1"]["audit_status"] == "FAIL"
            assert result["entries"]["EURUSD"]["H1"]["status"] == "failed"


def test_low_coverage_cannot_hide_parity_failure():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(
            native_root,
            "run-low-and-bad",
            mismatch=True,
            coverage_mode="low",
        )

        result = audit_and_activate(
            native_root,
            run_id="run-low-and-bad",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-low-and-bad",
            thresholds=ParityThresholds(
                min_compared_native_bars=20,
                max_ohlc_mismatch_ratio=0.0,
            ),
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["universe_eligibility"]["excluded_symbols"] == {}
        assert any(
            "OHLC_MISMATCH_RATIO_HIGH" in reason
            for reason in result["entries"]["EURUSD"]["H1"]["reason_codes"]
        )


def test_production_session_coverage_threshold_cannot_be_weakened():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-threshold")

        with pytest.raises(ValueError, match="pinned at 0.95"):
            audit_and_activate(
                native_root,
                run_id="run-threshold",
                symbols=["EURUSD"],
                timeframes=["M5", "H1"],
                activation_id="activation-threshold",
                minimum_session_coverage=0.0,
            )


def test_coverage_exclusion_ratio_tamper_invalidates_active_universe():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-coverage-tamper", coverage_mode="low")
        result = audit_and_activate(
            native_root,
            run_id="run-coverage-tamper",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-coverage-tamper",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        manifest_path = Path(result["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        audited = manifest["universe_eligibility"]["excluded_symbols"]["EURUSD"][
            "audited_sufficiency_cells"
        ]["H1"]
        audited["expected_session_coverage_by_partition"]["final_lockbox"] = 0.99
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        pointer_path = native_root / "ACTIVE.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["manifest_sha256"] = sha256_file(manifest_path)
        pointer_path.write_text(json.dumps(pointer), encoding="utf-8")

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )

        assert not universe.valid
        assert any("COVERAGE_REASON_MISMATCH" in error for error in universe.errors)


@pytest.mark.parametrize("evidence_kind", ["bars", "provenance", "sidecar", "parity"])
def test_excluded_universe_rehashes_every_authority_file(evidence_kind):
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(
            native_root,
            f"run-rehash-{evidence_kind}",
            coverage_mode="low",
        )
        result = audit_and_activate(
            native_root,
            run_id=f"run-rehash-{evidence_kind}",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id=f"activation-rehash-{evidence_kind}",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        entry = result["entries"]["EURUSD"]["H1"]
        paths = {
            "bars": native_root / entry["bars_relative_path"],
            "provenance": native_root
            / Path(entry["bars_relative_path"]).parent
            / "provenance.json",
            "sidecar": native_root / entry["sidecar_relative_path"],
            "parity": native_root / entry["parity_relative_path"],
        }
        with paths[evidence_kind].open("a", encoding="utf-8") as handle:
            handle.write("tamper\n")

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )

        assert not universe.valid
        assert any("SHA256_MISMATCH" in error for error in universe.errors)


def test_activation_session_coverage_policy_tamper_invalidates_universe():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-policy-tamper")
        result = audit_and_activate(
            native_root,
            run_id="run-policy-tamper",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-policy-tamper",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        manifest_path = Path(result["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["minimum_session_coverage"] = 0.94
        manifest["universe_eligibility"]["minimum_session_coverage"] = 0.94
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        pointer_path = native_root / "ACTIVE.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["manifest_sha256"] = sha256_file(manifest_path)
        pointer_path.write_text(json.dumps(pointer), encoding="utf-8")

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )

        assert not universe.valid
        assert "NATIVE_UNIVERSE_SESSION_COVERAGE_POLICY_INVALID" in universe.errors
        assert "NATIVE_ACTIVE_SESSION_COVERAGE_POLICY_INVALID" in universe.errors


def test_material_mismatch_writes_failed_manifest_without_advancing_existing_pointer():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-good")
        audit_and_activate(
            native_root,
            run_id="run-good",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-good",
            thresholds=ParityThresholds(min_compared_native_bars=20),
            now=datetime(2026, 1, 1, tzinfo=UTC),
        )
        pointer = native_root / "ACTIVE.json"
        pointer_before = sha256_file(pointer)
        _write_run(native_root, "run-bad", mismatch=True)

        failed = audit_and_activate(
            native_root,
            run_id="run-bad",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-bad",
            thresholds=ParityThresholds(
                min_compared_native_bars=20,
                max_ohlc_mismatch_ratio=0.0,
            ),
            now=datetime(2026, 1, 2, tzinfo=UTC),
        )

        assert failed["status"] == "failed"
        assert not failed["active_pointer_updated"]
        assert sha256_file(pointer) == pointer_before
        bad_entry = failed["entries"]["EURUSD"]["H1"]
        assert bad_entry["status"] == "failed"
        assert any("OHLC_MISMATCH_RATIO_HIGH" in reason for reason in bad_entry["reason_codes"])


def test_unresolved_parity_is_immutable_pending_and_does_not_create_active_pointer():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-pending", tolerance=None)

        pending = audit_and_activate(
            native_root,
            run_id="run-pending",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-pending",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert pending["status"] == "pending"
        assert not pending["active_pointer_updated"]
        assert not (native_root / "ACTIVE.json").exists()
        assert (native_root / "activations" / "activation-pending.json").exists()
        h1_entry = pending["entries"]["EURUSD"]["H1"]
        assert h1_entry["status"] == "pending"
        assert "TICK_TOLERANCE_UNAVAILABLE" in h1_entry["reason_codes"]


def test_hash_tamper_invalidates_active_resolution():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-good")
        audit_and_activate(
            native_root,
            run_id="run-good",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-good",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        bars = native_root / "runs" / "run-good" / "EURUSD" / "H1" / "bars.csv"
        with bars.open("a", encoding="utf-8") as handle:
            handle.write("tamper\n")

        resolution = resolve_active_native_artifact(data_dir, "EURUSD", "H1")

        assert not resolution.valid
        assert any(
            "BARS" in error and "SHA256_MISMATCH" in error
            for error in resolution.errors
        )


def _rewrite_activation_and_repin(native_root, activated, manifest):
    manifest_path = Path(activated["manifest_path"])
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = sha256_file(manifest_path)
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_repin_cannot_promote_low_coverage_evidence_to_eligible():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-reverse-promote", coverage_mode="low")
        activated = audit_and_activate(
            native_root,
            run_id="run-reverse-promote",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-reverse-promote",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        manifest = json.loads(Path(activated["manifest_path"]).read_text(encoding="utf-8"))
        universe = manifest["universe_eligibility"]
        universe["eligible_symbols"] = ["EURUSD"]
        universe["excluded_symbols"] = {}
        for entry in manifest["entries"]["EURUSD"].values():
            entry["status"] = "active"
            entry["universe_eligibility_status"] = "eligible"
            entry["audit_status"] = "PASS"
            entry["reason_codes"] = []
            entry["expected_session_coverage_by_partition"]["final_lockbox"] = 0.99
        manifest["counts"].update(
            active=2,
            excluded_no_trade=0,
            eligible_symbols=1,
            excluded_symbols=0,
        )
        _rewrite_activation_and_repin(native_root, activated, manifest)

        resolved = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        artifact = resolve_active_native_artifact(data_dir, "EURUSD", "H1")

        assert not resolved.valid
        assert any("NATIVE_CELL_EVIDENCE" in error for error in resolved.errors)
        assert not artifact.valid
        assert any(
            error.startswith(("NATIVE_CELL_EVIDENCE", "NATIVE_UNIVERSE_"))
            for error in artifact.errors
        )


def test_repin_cannot_demote_sufficient_evidence_to_coverage_exclusion():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-reverse-exclude")
        activated = audit_and_activate(
            native_root,
            run_id="run-reverse-exclude",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-reverse-exclude",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        manifest = json.loads(Path(activated["manifest_path"]).read_text(encoding="utf-8"))
        run_manifest = json.loads(
            (native_root / "runs" / "run-reverse-exclude" / "run_manifest.json").read_text(
                encoding="utf-8"
            )
        )
        run_cells = {
            item["timeframe"]: item for item in run_manifest["results"]
        }
        availability_cells = {}
        audited_cells = {}
        contract_reasons = []
        for timeframe, entry in manifest["entries"]["EURUSD"].items():
            reason = "SESSION_COVERAGE_LOW:final_lockbox:0.900000<0.950000"
            coverage = dict(entry["expected_session_coverage_by_partition"])
            coverage["final_lockbox"] = 0.9
            sidecar = json.loads(
                (native_root / entry["sidecar_relative_path"]).read_text(encoding="utf-8")
            )
            availability_cells[timeframe] = {
                "status": run_cells[timeframe]["status"],
                "failure_status": run_cells[timeframe].get("failure_status"),
                "rows": run_cells[timeframe]["rows"],
            }
            audited_cells[timeframe] = {
                "source_validation_status": "PASS",
                "parity_status": "PASS",
                "coverage_status": "INSUFFICIENT",
                "coverage_reason_codes": [reason],
                "expected_session_coverage_by_partition": coverage,
                "observed_session_bars_by_partition": entry[
                    "observed_session_bars_by_partition"
                ],
                "expected_session_bars_by_partition": entry[
                    "expected_session_bars_by_partition"
                ],
                "coverage_method": entry["coverage_method"],
                "source_provenance_sha256": sidecar[
                    "source_provenance_sha256"
                ],
                "audited_sidecar_sha256": entry["sidecar_sha256"],
                "parity_sha256": entry["parity_sha256"],
            }
            contract_reasons.append(f"{timeframe}:{reason}")
            entry["status"] = "excluded_no_trade"
            entry["universe_eligibility_status"] = "excluded_no_trade"
            entry["audit_status"] = "FAIL"
            entry["reason_codes"] = [reason]
            entry["expected_session_coverage_by_partition"] = coverage
        universe = manifest["universe_eligibility"]
        universe["eligible_symbols"] = []
        universe["excluded_symbols"] = {
            "EURUSD": {
                "status": "excluded_no_trade",
                "decision_basis": "source_availability_only",
                "reason_codes": contract_reasons,
                "availability_cells": availability_cells,
                "audited_sufficiency_cells": audited_cells,
            }
        }
        manifest["counts"].update(
            active=0,
            excluded_no_trade=2,
            eligible_symbols=0,
            excluded_symbols=1,
        )
        _rewrite_activation_and_repin(native_root, activated, manifest)

        resolved = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )

        assert not resolved.valid
        assert any("NATIVE_CELL_EVIDENCE" in error for error in resolved.errors)
