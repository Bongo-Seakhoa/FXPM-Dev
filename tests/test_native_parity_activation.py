import json
import hashlib
import inspect
import sys
import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


PATCH_ROOT = Path(__file__).resolve().parents[1]
if str(PATCH_ROOT) not in sys.path:
    sys.path.insert(0, str(PATCH_ROOT))

import tools.audit_activate_native_history as native_activation_audit  # noqa: E402

from pm_native_activation import (  # noqa: E402
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_SOURCE_UNAVAILABLE,
    NATIVE_TECHNICAL_ERROR,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    scope_native_pair_window_states,
    sha256_file,
    validate_native_session_coverage_diagnostics,
)
from pm_native_consensus import (  # noqa: E402
    EVIDENCE_PASS,
    EVIDENCE_PENDING,
    EVIDENCE_SEMANTIC_FAIL,
    INTEGRITY_PASS,
    REQUIRED_AUDIT_WINDOWS,
)
from pm_native_parity import (  # noqa: E402
    ParityThresholds,
    audit_native_cross_resolution,
    audit_native_vs_session_m5,
)
from pm_native_request_contract import (  # noqa: E402
    build_valid_v5_contract_fixture,
)
from tools.acquire_native_history import (  # noqa: E402
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    PAGINATION_CONTRACT_VERSION,
    SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    SCHEMA_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4,
    TIMEFRAME_SECONDS,
    expected_session_coverage_by_partition,
    segmented_session_coverage_by_partition_v2,
)
from tools.audit_activate_native_history import (  # noqa: E402
    _derive_authenticity_state,
    _parser,
    _source_leaf,
    audit_and_activate,
)
from tests.native_v5_support import write_v5_run as _write_v5_run  # noqa: E402


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


def test_partial_reference_extent_keeps_uncovered_audit_windows_pending():
    start = pd.Timestamp("2020-01-01T00:00:00Z")
    end = pd.Timestamp("2021-01-01T00:00:00Z")

    def _frame(first: pd.Timestamp) -> pd.DataFrame:
        index = pd.DatetimeIndex([first, end - pd.Timedelta(hours=1)])
        return pd.DataFrame(
            {"_BAR_CLOSE_TS": [first + pd.Timedelta(hours=1), end]},
            index=index,
        )

    states = {
        window: {
            "integrity_state": INTEGRITY_PASS,
            "evidence_state": EVIDENCE_PASS,
            "reason_codes": [],
        }
        for window in REQUIRED_AUDIT_WINDOWS
    }
    windows = {window: (start, end) for window in REQUIRED_AUDIT_WINDOWS}
    scoped = scope_native_pair_window_states(
        states,
        target_frame=_frame(start),
        reference_frame=_frame(start + pd.Timedelta(days=180)),
        partition_windows=windows,
        boundary_tolerance_seconds=3 * 86_400,
    )
    assert all(
        state["evidence_state"] == EVIDENCE_PENDING for state in scoped.values()
    )
    assert all(
        f"REFERENCE_AUDIT_PREFIX_PENDING:{window}" in scoped[window]["reason_codes"]
        for window in REQUIRED_AUDIT_WINDOWS
    )

    failed = {window: dict(state) for window, state in states.items()}
    failed[REQUIRED_AUDIT_WINDOWS[0]]["evidence_state"] = EVIDENCE_SEMANTIC_FAIL
    scoped_failed = scope_native_pair_window_states(
        failed,
        target_frame=_frame(start),
        reference_frame=_frame(start + pd.Timedelta(days=180)),
        partition_windows=windows,
        boundary_tolerance_seconds=3 * 86_400,
    )
    assert (
        scoped_failed[REQUIRED_AUDIT_WINDOWS[0]]["evidence_state"]
        == EVIDENCE_SEMANTIC_FAIL
    )


def test_source_leaf_invokes_exact_v5_connector_contract(monkeypatch, tmp_path):
    fixture = build_valid_v5_contract_fixture()
    provenance = deepcopy(fixture["provenance"])
    diagnostics = provenance["pagination"]["page_attempts"][0][
        "connector_diagnostics"
    ]
    diagnostics.update(
        sync_status="hard_mt5_failure",
        hard_failure=True,
        hard_failure_call="copy_rates_range_initial",
    )
    run_id = provenance["run_id"]
    leaf = tmp_path / "runs" / run_id / "EURUSD" / "M5"
    leaf.mkdir(parents=True)
    (leaf / "provenance.json").write_text(
        json.dumps(provenance),
        encoding="utf-8",
    )
    first = pd.Timestamp(fixture["artifact_observation"].first_open_utc)
    frame = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [1.1],
            "Low": [0.9],
            "Close": [1.0],
            "Volume": [1],
            "_BAR_CLOSE_TS": [first + pd.Timedelta(minutes=5)],
        },
        index=pd.DatetimeIndex([first]),
    )
    frame.to_csv(leaf / "bars.csv", index_label="time", lineterminator="\n")
    monkeypatch.setattr(
        native_activation_audit,
        "scan_native_csv_stream",
        lambda *_args, **_kwargs: SimpleNamespace(
            artifact=fixture["artifact_observation"]
        ),
    )

    loaded = _source_leaf(
        tmp_path,
        run_id,
        "EURUSD",
        "M5",
        fixture["manifest_result"],
        run_manifest_request=fixture["manifest_request"],
        run_source_environment=fixture["source_environment"],
    )

    assert (
        "SOURCE_PRODUCTION_CONTRACT_INVALID:V5_PAGE_MT5_V4_DIAGNOSTICS_INVALID"
        in loaded["errors"]
    )


def test_activation_v4_coverage_is_diagnostic_not_a_trading_veto():
    errors, normalized = validate_native_session_coverage_diagnostics(
        {
            "optimizer_train": 0.01,
            "tuning_validation": 0.25,
            "final_lockbox": 0.0,
        }
    )
    assert errors == []
    assert normalized == {
        "optimizer_train": 0.01,
        "tuning_validation": 0.25,
        "final_lockbox": 0.0,
    }


def test_activation_v4_sparse_sibling_cannot_veto_authentic_target():
    assert (
        _derive_authenticity_state(
            availability_class="success",
            source_errors=[],
            resolution_status="PASS",
            consensus_status="PENDING",
            source_unavailable_proven=False,
        )
        == NATIVE_EVIDENCE_CANDIDATE
    )


def test_activation_v4_exact_overlap_contradiction_is_technical_not_no_trade():
    assert (
        _derive_authenticity_state(
            availability_class="success",
            source_errors=[],
            resolution_status="PASS",
            consensus_status="FAIL",
            source_unavailable_proven=False,
        )
        == NATIVE_TECHNICAL_ERROR
    )


def test_activation_v4_unavailable_requires_first_date_proof():
    assert (
        _derive_authenticity_state(
            availability_class="unavailable",
            source_errors=[],
            resolution_status="PENDING",
            consensus_status="FAIL",
            source_unavailable_proven=False,
        )
        == NATIVE_TECHNICAL_ERROR
    )
    assert (
        _derive_authenticity_state(
            availability_class="unavailable",
            source_errors=[],
            resolution_status="PENDING",
            consensus_status="FAIL",
            source_unavailable_proven=True,
        )
        == NATIVE_SOURCE_UNAVAILABLE
    )


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
    common_as_of = pd.Timestamp(timeframe_windows[timeframes[0]]["final_lockbox"][1])
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
    strategy_intervals = {
        timeframe: {
            "start_utc": pd.Timestamp(
                timeframe_windows[timeframe]["optimizer_train"][0]
            ).isoformat(),
            "end_exclusive_utc": common_as_of.isoformat(),
        }
        for timeframe in timeframes
    }
    ordered = tuple(sorted(timeframes, key=lambda value: TIMEFRAME_SECONDS[value]))
    references = {
        timeframe: (ordered[1] if index == 0 else ordered[index - 1])
        for index, timeframe in enumerate(ordered)
    }
    intervals = {
        timeframe: dict(strategy_intervals[timeframe]) for timeframe in timeframes
    }
    for target, reference in references.items():
        intervals[reference]["start_utc"] = min(
            pd.Timestamp(intervals[reference]["start_utc"]),
            pd.Timestamp(strategy_intervals[target]["start_utc"]),
        ).isoformat()
    audit_prefixes = {
        timeframe: (
            {
                "start_utc": intervals[timeframe]["start_utc"],
                "end_exclusive_utc": strategy_intervals[timeframe]["start_utc"],
                "role": "audit_only_reference",
            }
            if intervals[timeframe]["start_utc"]
            != strategy_intervals[timeframe]["start_utc"]
            else None
        )
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
            TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4
        ),
        "timeframe_window_years": timeframe_years,
        "timeframe_calendar_boundaries": serialized,
        "timeframe_calendar_boundaries_sha256": _canonical_json_sha256(serialized),
        "timeframe_required_evidence_lead_start_utc": leads,
        "timeframe_source_request_intervals": intervals,
        "timeframe_strategy_evidence_intervals": strategy_intervals,
        "timeframe_audit_reference_map": references,
        "timeframe_audit_only_prefix_intervals": audit_prefixes,
    }
    request_map_payload = {
        "schema_version": TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4,
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "common_as_of_utc": common_as_of.isoformat(),
        "timeframe_window_years": timeframe_years,
        "timeframe_calendar_boundaries": serialized,
        "timeframe_required_evidence_lead_start_utc": leads,
        "timeframe_source_request_intervals": intervals,
        "timeframe_strategy_evidence_intervals": strategy_intervals,
        "timeframe_audit_reference_map": references,
        "timeframe_audit_only_prefix_intervals": audit_prefixes,
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
            "strategy_evidence_interval": strategy_intervals[timeframe],
            "audit_only_prefix_interval": audit_prefixes[timeframe],
            "audit_reference_timeframe": references.get(timeframe),
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
    configured_slots = min(
        configured_slots, int(_SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90)
    )
    page_span_seconds = configured_slots * seconds
    pages_requested = int(
        np.ceil((fetch_end - fetch_start).total_seconds() / page_span_seconds)
    )
    page_attempts = []
    cursor = fetch_start
    while cursor < fetch_end:
        page_end = min(
            cursor + pd.Timedelta(seconds=page_span_seconds),
            fetch_end,
        )
        page_attempts.append(
            {
                "start_utc": cursor.isoformat(),
                "end_exclusive_utc": page_end.isoformat(),
                "attempts": 1,
                "copy_rates_range_attempts": 1,
            }
        )
        cursor = page_end
    request = {
        **request_contract,
        "native_fetch_start_utc": fetch_start.isoformat(),
        "native_fetch_end_exclusive_utc": fetch_end.isoformat(),
        "alignment_mode": alignment_mode,
        "successor_sentinel_fetch_slack_seconds": sentinel_slack,
    }
    pagination = {
        "contract_version": PAGINATION_CONTRACT_VERSION,
        "page_span_seconds": page_span_seconds,
        "pages_requested": pages_requested,
        "pages_with_data": pages_requested,
        "empty_pages": [],
        "failed_pages": [],
        "truncated_pages": [],
        "attempts_total": pages_requested,
        "copy_rates_range_attempts_total": pages_requested,
        "page_attempts": page_attempts,
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
        "M15": pd.Timedelta(minutes=15),
        "M30": pd.Timedelta(minutes=30),
        "H1": pd.Timedelta(hours=1),
        "H4": pd.Timedelta(hours=4),
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


def _write_timeframe_v4_run(native_root, run_id):
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
    m5_strategy = _weekly_m5_history(start=origin.strftime("%Y-%m-%d"), weeks=50)
    m5_audit_prefix = _weekly_m5_history(
        start=(origin - pd.Timedelta(weeks=10)).strftime("%Y-%m-%d"),
        weeks=10,
    )
    m5 = pd.concat([m5_audit_prefix, m5_strategy]).sort_index()
    h1 = _resample(m5, "1h")
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
            native_root / "runs" / run_id / "EURUSD" / timeframe / "provenance.json"
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


def test_semantic_receipt_creation_and_validation_share_structural_gate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    events = []
    real_build = native_activation_audit.build_native_semantic_receipt
    real_validate = (
        native_activation_audit.validate_staged_receipt_backed_native_authority
    )

    def _tracked_build(manifest):
        events.append(
            (
                "create",
                manifest["activation_id"],
                manifest["production_authoritative"],
            )
        )
        return real_build(manifest)

    def _tracked_validate(data_dir, pointer):
        events.append(
            (
                "validate",
                pointer["activation_id"],
                pointer["production_authoritative"],
            )
        )
        return real_validate(data_dir, pointer)

    def _bounded_v5_symbol(*_args, **kwargs):
        symbol = kwargs["symbol"]
        run_id = kwargs["run_id"]
        activation_id = kwargs["activation_id"]
        coverage = {
            partition: 1.0
            for partition in native_activation_audit.NATIVE_EVIDENCE_PARTITIONS
        }
        entries = {}
        for timeframe in kwargs["timeframes"]:
            cell = kwargs["run_manifest_cells"][(symbol, timeframe)]
            entries[timeframe] = {
                "status": NATIVE_EVIDENCE_CANDIDATE,
                "audit_status": "PASS",
                "universe_eligibility_status": NATIVE_EVIDENCE_CANDIDATE,
                "tradeable": False,
                "statistical_sufficiency_receipt_required": True,
                "symbol": symbol,
                "timeframe": timeframe,
                "run_id": run_id,
                "bars_relative_path": (
                    f"runs/{run_id}/{symbol}/{timeframe}/bars.csv"
                ),
                "bars_sha256": cell["sha256"],
                "sidecar_relative_path": (
                    f"audits/{activation_id}/{symbol}/{timeframe}/bars.meta.json"
                ),
                "sidecar_sha256": "7" * 64,
                "parity_relative_path": (
                    f"audits/{activation_id}/{symbol}/{timeframe}/parity.json"
                ),
                "parity_sha256": "8" * 64,
                "coverage_method": (
                    native_activation_audit.NATIVE_SESSION_COVERAGE_METHOD
                ),
                "expected_session_coverage_by_partition": coverage,
            }
        return entries, {}, []

    monkeypatch.setattr(
        native_activation_audit,
        "build_native_semantic_receipt",
        _tracked_build,
    )
    monkeypatch.setattr(
        native_activation_audit,
        "validate_staged_receipt_backed_native_authority",
        _tracked_validate,
    )
    monkeypatch.setattr(
        native_activation_audit,
        "_v5_audit_symbol",
        _bounded_v5_symbol,
    )

    forensic_root = tmp_path / "forensic" / "data" / "native"
    forensic_root.mkdir(parents=True)
    _write_run(forensic_root, "run-forensic-symmetry")
    forensic = audit_and_activate(
        forensic_root,
        run_id="run-forensic-symmetry",
        symbols=["EURUSD"],
        timeframes=["M5", "H1"],
        activation_id="activation-forensic-symmetry",
        thresholds=ParityThresholds(min_compared_native_bars=20),
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert forensic["status"] == "active", forensic
    assert forensic["production_authoritative"] is False
    assert forensic["active_pointer_updated"] is True
    assert forensic["semantic_receipt_path"] is None
    assert forensic["semantic_receipt_sha256"] is None
    forensic_pointer = json.loads(
        (forensic_root / "ACTIVE.json").read_text(encoding="utf-8")
    )
    assert forensic_pointer["production_authoritative"] is False
    assert events == []

    production_root = tmp_path / "production" / "data" / "native"
    production_root.mkdir(parents=True)
    _write_v5_run(
        production_root,
        "run-production-symmetry",
        production_grid=True,
    )
    production = audit_and_activate(
        production_root,
        run_id="run-production-symmetry",
        symbols=native_activation_audit.PRODUCTION_SYMBOL_ORDER,
        timeframes=native_activation_audit.DEFAULT_TIMEFRAMES,
        activation_id="activation-production-symmetry",
        thresholds=ParityThresholds(min_compared_native_bars=20),
        now=datetime(2026, 1, 1, tzinfo=UTC),
        semantic_verification_mode=(
            native_activation_audit.NATIVE_SEMANTIC_VERIFICATION_FORCE_RAW
        ),
    )
    assert production["status"] == "active", production
    assert production["production_authoritative"] is True
    assert production["active_pointer_updated"] is True
    receipt_path = Path(production["semantic_receipt_path"])
    assert receipt_path.is_file()
    assert production["semantic_receipt_sha256"] == sha256_file(receipt_path)
    assert events == [
        ("create", "activation-production-symmetry", True),
        ("validate", "activation-production-symmetry", True),
    ]


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
                "copy_rates_range_attempts": 3,
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
            "copy_rates_range_attempts_total": len(empty_pages) * 3,
            "page_attempts": [dict(page) for page in empty_pages],
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
    m5_cell = next(item for item in manifest["results"] if item["timeframe"] == "M5")
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
                for partition in (
                    "optimizer_train",
                    "tuning_validation",
                    "final_lockbox",
                )
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
                for partition in (
                    "optimizer_train",
                    "tuning_validation",
                    "final_lockbox",
                )
            },
        }
    )
    if failure_status == "boundary_history_shortfall":
        requested_start = pd.Timestamp(provenance["request"]["native_fetch_start_utc"])
        requested_end = pd.Timestamp(
            provenance["request"]["native_fetch_end_exclusive_utc"]
        )
        actual_last_close = pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1])
        provenance["coverage"] = {
            "status": "partial_requested_span",
            "actual_first_open_utc": frame.index[0].isoformat(),
            "actual_last_open_utc": frame.index[-1].isoformat(),
            "actual_last_close_utc": actual_last_close.isoformat(),
            "start_delay_seconds": max(
                0.0,
                float((frame.index[0] - requested_start).total_seconds()),
            ),
            "end_lag_seconds": max(
                0.0,
                float((requested_end - actual_last_close).total_seconds()),
            ),
            "calendar_span_coverage_ratio": float(
                (actual_last_close - frame.index[0]).total_seconds()
                / (requested_end - requested_start).total_seconds()
            ),
            "internal_empty_page_count": 0,
            "truncated_page_count": 0,
            "internal_truncated_page_count": 0,
            "boundary_truncated_page_count": 0,
            "failed_page_count": 0,
            "technical_integrity_status": "complete_page_ledger",
            "market_calendar_completeness_assessed": False,
        }
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


def _three_partition_windows(start):
    start = pd.Timestamp(start)
    return {
        "optimizer_train": (start, start + pd.Timedelta(days=2)),
        "tuning_validation": (
            start + pd.Timedelta(days=2),
            start + pd.Timedelta(days=4),
        ),
        "final_lockbox": (
            start + pd.Timedelta(days=4),
            start + pd.Timedelta(days=6),
        ),
    }


def _resample_with_real_volume(frame, rule="15min"):
    return (
        frame.resample(rule)
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "RealVolume": "sum",
            }
        )
        .dropna()
    )


def test_cross_resolution_reconciliation_is_exact_and_partition_scoped():
    fine = _m5_history(days=6)
    fine["RealVolume"] = 2
    coarse = _resample_with_real_volume(fine)
    windows = _three_partition_windows(fine.index[0])

    report = audit_native_cross_resolution(
        fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M15",
        tick_tolerance=0.0001,
        partition_windows=windows,
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "PASS", report["reason_codes"]
    assert report["metrics"]["partitions_passed"] == 3
    for partition in windows:
        evidence = report["partitions"][partition]
        assert evidence["status"] == "PASS"
        assert evidence["bidirectional_coverage"] == 1.0
        assert evidence["tick_volume_mismatch_coarse_bars"] == 0
        assert evidence["real_volume_mismatch_coarse_bars"] == 0


def test_cross_resolution_missing_fine_bar_is_caught_by_exact_volume():
    complete_fine = _m5_history(days=6)
    complete_fine["RealVolume"] = 2
    coarse = _resample_with_real_volume(complete_fine)
    # The middle member does not determine the M15 open/close/high/low in this
    # monotonic fixture. Exact volume must still expose the missing M5 bar.
    missing_ts = complete_fine.index[1]
    incomplete_fine = complete_fine.drop(index=missing_ts)

    report = audit_native_cross_resolution(
        incomplete_fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M15",
        tick_tolerance=0.0001,
        partition_windows=_three_partition_windows(complete_fine.index[0]),
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "FAIL"
    assert any(
        reason.startswith("TICK_VOLUME_MISMATCH:optimizer_train:")
        for reason in report["reason_codes"]
    )
    assert any(
        reason.startswith("REAL_VOLUME_MISMATCH:optimizer_train:")
        for reason in report["reason_codes"]
    )
    assert report["partitions"]["tuning_validation"]["status"] == "PASS"
    assert report["partitions"]["final_lockbox"]["status"] == "PASS"


def test_cross_resolution_missing_coarse_bar_leaves_fine_rows_unassigned():
    fine = _m5_history(days=6)
    fine["RealVolume"] = 0
    coarse = _resample_with_real_volume(fine)
    coarse = coarse.drop(index=coarse.index[4])

    report = audit_native_cross_resolution(
        fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M15",
        tick_tolerance=0.0001,
        partition_windows=_three_partition_windows(fine.index[0]),
        thresholds=ParityThresholds(
            max_unassigned_m5_ratio=0.0,
            min_compared_native_bars=20,
        ),
    )

    assert report["status"] == "FAIL"
    train = report["partitions"]["optimizer_train"]
    assert train["unassigned_fine_rows"] == 3
    assert any(
        reason.startswith("UNASSIGNED_FINE_RATIO_HIGH:optimizer_train:")
        for reason in report["reason_codes"]
    )


def test_cross_resolution_final_defect_is_not_diluted_by_training_history():
    complete_fine = _m5_history(days=6)
    complete_fine["RealVolume"] = 0
    coarse = _resample_with_real_volume(complete_fine)
    final_start = complete_fine.index[0] + pd.Timedelta(days=4)
    incomplete_fine = complete_fine.drop(index=final_start + pd.Timedelta(minutes=5))

    report = audit_native_cross_resolution(
        incomplete_fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M15",
        tick_tolerance=0.0001,
        partition_windows=_three_partition_windows(complete_fine.index[0]),
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "FAIL"
    assert report["partitions"]["optimizer_train"]["status"] == "PASS"
    assert report["partitions"]["tuning_validation"]["status"] == "PASS"
    assert report["partitions"]["final_lockbox"]["status"] == "FAIL"
    boundary = report["partitions"]["final_lockbox"]["boundaries"]
    assert boundary["requested_start_utc"] == final_start.isoformat()


def test_cross_resolution_nonaligned_boundaries_follow_target_bar_containment():
    fine = _m5_history(days=1).iloc[:73].copy()
    coarse = _resample(fine, "15min")
    origin = fine.index[0]
    windows = {
        "optimizer_train": (
            origin + pd.Timedelta(minutes=7),
            origin + pd.Timedelta(hours=2, minutes=7),
        ),
        "tuning_validation": (
            origin + pd.Timedelta(hours=2, minutes=7),
            origin + pd.Timedelta(hours=4, minutes=7),
        ),
        "final_lockbox": (
            origin + pd.Timedelta(hours=4, minutes=7),
            origin + pd.Timedelta(hours=6, minutes=7),
        ),
    }

    corrupt_fine = fine.copy()
    corrupt_fine.loc[
        origin + pd.Timedelta(minutes=10),
        ["Open", "High", "Low", "Close"],
    ] += 10.0
    fine_target = audit_native_cross_resolution(
        corrupt_fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M5",
        tick_tolerance=0.0001,
        partition_windows=windows,
        thresholds=ParityThresholds(min_compared_native_bars=1),
    )
    assert fine_target["status"] == "FAIL"
    assert any(
        reason.startswith("OHLC_MISMATCH_RATIO_HIGH:optimizer_train:")
        for reason in fine_target["reason_codes"]
    )

    corrupt_coarse = coarse.copy()
    corrupt_coarse.loc[
        origin + pd.Timedelta(hours=2),
        ["Open", "High", "Low", "Close"],
    ] += 10.0
    coarse_target = audit_native_cross_resolution(
        fine,
        corrupt_coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M15",
        tick_tolerance=0.0001,
        partition_windows=windows,
        thresholds=ParityThresholds(min_compared_native_bars=1),
    )
    assert coarse_target["status"] == "PASS", coarse_target["reason_codes"]


def test_fine_target_defers_only_the_trailing_incomplete_coarse_interval():
    origin = pd.Timestamp("2025-01-01T00:00:00Z")
    fine = _m5_history(start=str(origin), days=2).iloc[:289].copy()
    cutoff = origin + pd.Timedelta(days=1, minutes=5)
    # The final target M5 bar [00:00, 00:05) is closed at the common cutoff,
    # but its native M15 reference [00:00, 00:15) cannot be closed yet.
    coarse = _resample(fine.iloc[:-1], "15min")

    report = audit_native_cross_resolution(
        fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M5",
        tick_tolerance=0.0001,
        partition_windows={"final_lockbox": (origin, cutoff)},
        thresholds=ParityThresholds(min_compared_native_bars=1),
    )

    assert report["status"] == "PASS", report["reason_codes"]
    final = report["partitions"]["final_lockbox"]
    assert final["fine_rows"] == 288
    assert final["assigned_fine_rows"] == 288
    assert final["unassigned_fine_rows"] == 0
    assert final["trailing_incomplete_coarse_fine_rows_deferred"] == 1
    assert final["trailing_incomplete_coarse_interval"] == {
        "open_utc": "2025-01-02T00:00:00+00:00",
        "close_exclusive_utc": "2025-01-02T00:15:00+00:00",
        "evidence_cutoff_utc": "2025-01-02T00:05:00+00:00",
    }


def test_fine_target_still_fails_a_missing_coarse_interval_closed_at_cutoff():
    origin = pd.Timestamp("2025-01-01T00:00:00Z")
    fine = _m5_history(start=str(origin), days=2).iloc[:291].copy()
    cutoff = origin + pd.Timedelta(days=1, minutes=15)
    # The M15 interval ending exactly at the cutoff is no longer partial.  If
    # its native bar is absent, all three M5 members must remain hard failures.
    coarse = _resample(fine.iloc[:288], "15min")

    report = audit_native_cross_resolution(
        fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M5",
        tick_tolerance=0.0001,
        partition_windows={"final_lockbox": (origin, cutoff)},
        thresholds=ParityThresholds(min_compared_native_bars=1),
    )

    assert report["status"] == "FAIL"
    final = report["partitions"]["final_lockbox"]
    assert final["trailing_incomplete_coarse_fine_rows_deferred"] == 0
    assert final["trailing_incomplete_coarse_interval"] is None
    assert final["unassigned_fine_rows"] == 3
    assert "UNASSIGNED_FINE_ROWS:final_lockbox:3" in final["reason_codes"]


def test_fine_target_still_fails_an_internal_missing_coarse_interval():
    fine = _m5_history(days=1)
    coarse = _resample(fine, "15min").drop(index=fine.index[12])

    report = audit_native_cross_resolution(
        fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M5",
        tick_tolerance=0.0001,
        partition_windows={
            "optimizer_train": (fine.index[0], fine.index[-1] + pd.Timedelta(minutes=5))
        },
        thresholds=ParityThresholds(min_compared_native_bars=1),
    )

    assert report["status"] == "FAIL"
    train = report["partitions"]["optimizer_train"]
    assert train["trailing_incomplete_coarse_fine_rows_deferred"] == 0
    assert train["unassigned_fine_rows"] == 3
    assert "UNASSIGNED_FINE_ROWS:optimizer_train:3" in train["reason_codes"]


def test_cross_resolution_fine_target_detects_wholly_missing_fine_group():
    complete_fine = _m5_history(days=6)
    coarse = _resample(complete_fine, "15min")
    missing_open = complete_fine.index[0] + pd.Timedelta(hours=3)
    incomplete_fine = complete_fine.drop(
        index=pd.date_range(missing_open, periods=3, freq="5min")
    )

    report = audit_native_cross_resolution(
        incomplete_fine,
        coarse,
        symbol="EURUSD",
        fine_timeframe="M5",
        coarse_timeframe="M15",
        activation_target_timeframe="M5",
        tick_tolerance=0.0001,
        partition_windows=_three_partition_windows(complete_fine.index[0]),
        thresholds=ParityThresholds(min_compared_native_bars=20),
    )

    assert report["status"] == "FAIL"
    train = report["partitions"]["optimizer_train"]
    assert train["missing_coarse_membership_bars"] >= 1
    assert any(
        reason.startswith("MISSING_COARSE_MEMBERSHIP:optimizer_train:")
        for reason in report["reason_codes"]
    )


def test_cross_resolution_fixed_coarse_target_detects_missing_edge_bars():
    fine = _m5_history(days=1)
    complete_coarse = _resample(fine, "1h")
    origin = fine.index[0]
    windows = {
        "optimizer_train": (origin, origin + pd.Timedelta(hours=8)),
        "tuning_validation": (
            origin + pd.Timedelta(hours=8),
            origin + pd.Timedelta(hours=16),
        ),
        "final_lockbox": (
            origin + pd.Timedelta(hours=16),
            origin + pd.Timedelta(hours=24),
        ),
    }

    for missing_open in (origin, origin + pd.Timedelta(hours=7)):
        report = audit_native_cross_resolution(
            fine,
            complete_coarse.drop(index=missing_open),
            symbol="EURUSD",
            fine_timeframe="M5",
            coarse_timeframe="H1",
            activation_target_timeframe="H1",
            tick_tolerance=0.0001,
            partition_windows=windows,
            thresholds=ParityThresholds(min_compared_native_bars=1),
        )
        assert report["status"] == "FAIL"
        assert report["partitions"]["optimizer_train"]["unassigned_fine_rows"] >= 12


def test_cross_resolution_broker_aligned_h4_detects_bracketed_edge_gaps():
    m5 = _m5_history(start="2024-12-31", days=5)
    fine = _resample(m5, "1h")
    coarse = (
        fine.resample(
            "4h",
            origin="epoch",
            offset="2h",
            label="left",
            closed="left",
        )
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
    origin = pd.Timestamp("2025-01-01T00:00:00Z")
    windows = {
        "optimizer_train": (origin, origin + pd.Timedelta(days=1)),
        "tuning_validation": (
            origin + pd.Timedelta(days=1),
            origin + pd.Timedelta(days=2),
        ),
        "final_lockbox": (
            origin + pd.Timedelta(days=2),
            origin + pd.Timedelta(days=3),
        ),
    }

    for missing_open in (
        origin + pd.Timedelta(hours=2),
        origin + pd.Timedelta(hours=18),
    ):
        report = audit_native_cross_resolution(
            fine,
            coarse.drop(index=missing_open),
            symbol="EURUSD",
            fine_timeframe="H1",
            coarse_timeframe="H4",
            activation_target_timeframe="H4",
            tick_tolerance=0.0001,
            partition_windows=windows,
            thresholds=ParityThresholds(min_compared_native_bars=1),
        )
        assert report["status"] == "FAIL"
        assert report["partitions"]["optimizer_train"]["unassigned_fine_rows"] >= 4


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
        end = (
            following
            if pd.Timedelta(hours=12) <= delta <= pd.Timedelta(hours=36)
            else start + pd.Timedelta(days=1)
        )
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
        assert (
            resolution.data_path
            == native_root / "runs" / "run-good" / "EURUSD" / "H1" / "bars.csv"
        )
        assert "audits/activation-good" in str(resolution.sidecar_path).replace(
            "\\", "/"
        )

        # A tempting flat file is ignored; the hash-pinned run remains the only authority.
        flat = native_root / "EURUSD_H1.csv"
        flat.write_text("malicious fallback", encoding="utf-8")
        again = resolve_active_native_artifact(data_dir, "EURUSD", "H1")
        assert again.valid
        assert again.data_path == resolution.data_path


def test_audit_only_writes_evidence_without_advancing_active_pointer():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_run(native_root, "run-audit-only")
        pointer_path = native_root / "ACTIVE.json"
        pointer_path.write_text('{"existing": "pointer"}', encoding="utf-8")
        before = pointer_path.read_bytes()

        result = audit_and_activate(
            native_root,
            run_id="run-audit-only",
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-audit-only",
            thresholds=ParityThresholds(min_compared_native_bars=20),
            now=datetime(2026, 1, 1, tzinfo=UTC),
            update_active_pointer=False,
        )

        assert result["status"] == "active", result
        assert result["audit_only"] is True
        assert result["active_pointer_updated"] is False
        assert pointer_path.read_bytes() == before
        assert Path(result["manifest_path"]).is_file()


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
        production = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
            production_contract_required=True,
        )
        assert not production.valid
        assert production.errors == [
            "NATIVE_SEMANTIC_PRODUCTION_AUTHORITY_REQUIRED"
        ]


def test_timeframe_v4_cell_windows_activate_and_resolve_independently():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        _write_timeframe_v4_run(native_root, "run-timeframe-v3")

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
        production = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
            production_contract_required=True,
        )
        assert not production.valid
        assert production.errors == [
            "NATIVE_SEMANTIC_PRODUCTION_AUTHORITY_REQUIRED"
        ]
        calendar = universe.calendar_contract
        assert calendar["window_contract_version"] == (
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        )
        assert (
            calendar["timeframe_calendar_boundaries"]["M5"]
            != (calendar["timeframe_calendar_boundaries"]["H1"])
        )
        assert resolve_active_native_artifact(data_dir, "EURUSD", "H1").valid


def test_timeframe_v4_high_coverage_integrity_failure_is_resolvable_exclusion():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-integrity-fail"
        _write_timeframe_v4_run(native_root, run_id)

        bars_path = native_root / "runs" / run_id / "EURUSD" / "M5" / "bars.csv"
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        audit_prefix = frame.index < pd.Timestamp("2025-01-06T00:00:00Z")
        changed_timestamp = frame.index[audit_prefix][0]
        frame.loc[changed_timestamp, "Volume"] += 1
        _rewrite_source_frame(
            native_root,
            run_id,
            "M5",
            frame,
            status="success",
            failure_status=None,
        )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-integrity-fail",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        assert result["entries"]["EURUSD"]["M5"]["status"] == "active"
        h1_entry = result["entries"]["EURUSD"]["H1"]
        assert h1_entry["status"] == "excluded_no_trade"
        assert all(
            value >= 0.95
            for value in h1_entry["expected_session_coverage_by_partition"].values()
        )
        assert any("VOLUME" in reason for reason in h1_entry["reason_codes"])

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ("M5",)
        assert universe.excluded_timeframes("EURUSD") == ("H1",)


def test_timeframe_v4_all_semantic_failures_form_valid_full_exclusion():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-all-integrity-fail"
        _write_timeframe_v4_run(native_root, run_id)

        bars_path = native_root / "runs" / run_id / "EURUSD" / "M5" / "bars.csv"
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        shared_strategy = frame.index >= pd.Timestamp("2025-01-06T00:00:00Z")
        changed_timestamp = frame.index[shared_strategy][0]
        frame.loc[changed_timestamp, "Volume"] += 1
        _rewrite_source_frame(
            native_root,
            run_id,
            "M5",
            frame,
            status="success",
            failure_status=None,
        )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-all-integrity-fail",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        assert result["universe_eligibility"]["eligible_symbols"] == []
        exclusion = result["universe_eligibility"]["excluded_symbols"]["EURUSD"]
        assert exclusion["reason_codes"]
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
        assert universe.active_timeframes("EURUSD") == ()


def test_timeframe_v4_single_ohlc_corruption_is_not_diluted_by_horizon():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-ohlc-corruption"
        _write_timeframe_v4_run(native_root, run_id)

        bars_path = native_root / "runs" / run_id / "EURUSD" / "H1" / "bars.csv"
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        strategy_rows = frame.index >= pd.Timestamp("2025-01-06T00:00:00Z")
        changed_timestamp = frame.index[strategy_rows][0]
        frame.loc[
            changed_timestamp,
            ["Open", "High", "Low", "Close"],
        ] += 10.0
        _rewrite_source_frame(
            native_root,
            run_id,
            "H1",
            frame,
            status="success",
            failure_status=None,
        )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-ohlc-corruption",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        assert all(
            entry["status"] == "excluded_no_trade"
            for entry in result["entries"]["EURUSD"].values()
        )
        assert all(
            any(
                "OHLC_MISMATCH_RATIO_HIGH" in reason for reason in entry["reason_codes"]
            )
            for entry in result["entries"]["EURUSD"].values()
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ()


def test_timeframe_v4_tick_tolerance_override_cannot_weaken_source_contract():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-tolerance-override"
        _write_timeframe_v4_run(native_root, run_id)

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-tolerance-override",
            thresholds=ParityThresholds(min_compared_native_bars=20),
            tick_tolerance_overrides={"EURUSD": 1.0},
        )

        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert all(
            any(
                "TICK_TOLERANCE_OVERRIDE_WEAKENS_AUTHENTICATED_BASE" in reason
                for reason in entry["reason_codes"]
            )
            for entry in result["entries"]["EURUSD"].values()
        )


def test_timeframe_v4_unavailable_reference_does_not_veto_valid_target_cell():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-reference-unavailable"
        _write_timeframe_v4_run(native_root, run_id)
        _rewrite_m5_as_no_history(native_root, run_id)

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-reference-unavailable",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["active_pointer_updated"]
        assert result["entries"]["EURUSD"]["M5"]["status"] == ("excluded_no_trade")
        assert result["entries"]["EURUSD"]["H1"]["status"] == "active"
        assert result["universe_eligibility"]["cell_statuses"]["EURUSD"] == {
            "M5": "excluded_no_trade",
            "H1": "active",
        }
        h1_entry = result["entries"]["EURUSD"]["H1"]
        parity = json.loads(
            (native_root / h1_entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert parity["status"] == "NOT_APPLICABLE"
        assert parity["reason_codes"] == ["AUDIT_REFERENCE_UNAVAILABLE:M5:NO_HISTORY"]

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.symbol_status("EURUSD") == "eligible"
        assert universe.active_timeframes("EURUSD") == ("H1",)
        assert universe.excluded_timeframes("EURUSD") == ("M5",)


def test_timeframe_v4_request_map_tamper_blocks_activation():
    with tempfile.TemporaryDirectory() as tmp:
        native_root = Path(tmp) / "data" / "native"
        native_root.mkdir(parents=True)
        _write_timeframe_v4_run(native_root, "run-timeframe-v3-tamper")
        provenance_path = (
            native_root
            / "runs"
            / "run-timeframe-v3-tamper"
            / "EURUSD"
            / "H1"
            / "provenance.json"
        )
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        provenance["request"]["timeframe_calendar_boundaries"]["H1"]["optimizer_train"][
            "start_ts"
        ] = "2024-01-01T00:00:00+00:00"
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


def test_timeframe_v4_repin_cannot_flip_parity_target_orientation():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-timeframe-v4-orientation-repin"
        _write_timeframe_v4_run(native_root, run_id)
        activated = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-timeframe-v4-orientation-repin",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )
        manifest_path = Path(activated["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entry = manifest["entries"]["EURUSD"]["H1"]
        parity_path = native_root / entry["parity_relative_path"]
        parity = json.loads(parity_path.read_text(encoding="utf-8"))
        parity["activation_target_timeframe"] = "M5"
        parity_path.write_text(
            json.dumps(parity, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        entry["parity_sha256"] = sha256_file(parity_path)
        _rewrite_activation_and_repin(native_root, activated, manifest)

        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )

        assert not universe.valid
        assert any(
            "PARITY_TARGET_TIMEFRAME_MISMATCH" in error for error in universe.errors
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
            (native_root / h1_entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert parity["status"] == "NOT_APPLICABLE"
        assert parity["reason_codes"] == ["M5_ACQUISITION_UNAVAILABLE:NO_HISTORY"]
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
        manifest_path = native_root / "runs" / "run-m5-technical" / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        next(item for item in manifest["results"] if item["timeframe"] == "M5")[
            "failure_status"
        ] = "fetch_failed"
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


def test_low_session_coverage_is_diagnostic_and_does_not_emit_no_trade():
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
        assert eligibility["eligible_symbols"] == ["EURUSD"]
        assert eligibility["excluded_symbols"] == {}
        assert all(
            entry["status"] == "active"
            for entry in result["entries"]["EURUSD"].values()
        )
        assert all(
            entry["expected_session_coverage_by_partition"]["final_lockbox"]
            == pytest.approx(0.9)
            for entry in result["entries"]["EURUSD"].values()
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.symbol_status("EURUSD") == "eligible"


def test_ambiguous_legacy_partial_cell_blocks_activation_but_preserves_sibling_evidence():
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

        assert result["status"] == "failed", result
        assert result["active_pointer_updated"] is False
        assert result["entries"]["EURUSD"]["M5"]["status"] == "active"
        assert result["entries"]["EURUSD"]["H1"]["status"] == "failed"
        assert any(
            "SOURCE_LEGACY_PARTIAL" in reason
            for reason in result["entries"]["EURUSD"]["H1"]["reason_codes"]
        )
        assert not (native_root / "ACTIVE.json").exists()


def test_v4_leading_audit_prefix_shortfall_does_not_veto_covered_target_cell():
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp) / "data"
        native_root = data_dir / "native"
        native_root.mkdir(parents=True)
        run_id = "run-v4-leading-shortfall"
        _write_timeframe_v4_run(native_root, run_id)
        bars_path = native_root / "runs" / run_id / "EURUSD" / "M5" / "bars.csv"
        frame = pd.read_csv(bars_path)
        frame.index = pd.to_datetime(frame.pop("time"), utc=True)
        own_train_start = pd.Timestamp("2025-01-06T00:00:00Z")
        # Remove only the oldest week of the H1-reference prefix. M5's own
        # train/tuning/lockbox strategy envelope remains byte-for-byte covered.
        frame = frame.loc[frame.index >= own_train_start - pd.Timedelta(weeks=9)]
        _rewrite_source_frame(
            native_root,
            run_id,
            "M5",
            frame,
            status="partial",
            failure_status="boundary_history_shortfall",
        )

        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["EURUSD"],
            timeframes=["M5", "H1"],
            activation_id="activation-v4-leading-shortfall",
            thresholds=ParityThresholds(min_compared_native_bars=20),
        )

        assert result["status"] == "active", result
        assert result["entries"]["EURUSD"]["M5"]["status"] == "active"
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert "M5" in universe.active_timeframes("EURUSD")
        assert resolve_active_native_artifact(data_dir, "EURUSD", "M5").valid


def test_one_low_coverage_cell_remains_data_eligible_for_downstream_power_test():
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
            remove |= (frame.index >= start) & (
                frame.index < start + pd.Timedelta(days=1)
            )
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
            "H1": "active",
        }
        assert (
            result["entries"]["EURUSD"]["M5"]["expected_session_coverage_by_partition"][
                "final_lockbox"
            ]
            >= 0.95
        )
        universe = resolve_active_native_universe(
            data_dir,
            ["EURUSD"],
            required_timeframes=("M5", "H1"),
        )
        assert universe.valid, universe.errors
        assert universe.active_timeframes("EURUSD") == ("M5", "H1")
        assert universe.excluded_timeframes("EURUSD") == ()
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
            provenance["expected_session_coverage_details_by_partition"]["status"] = (
                "untrusted"
            )
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


def test_production_session_coverage_threshold_has_no_public_override():
    assert (
        "minimum_session_coverage"
        not in inspect.signature(audit_and_activate).parameters
    )

    with pytest.raises(SystemExit):
        _parser().parse_args(
            [
                "--run-id",
                "run-threshold",
                "--minimum-session-coverage",
                "0.0",
            ]
        )


def test_coverage_diagnostic_tamper_invalidates_active_universe():
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
        manifest["entries"]["EURUSD"]["H1"]["expected_session_coverage_by_partition"][
            "final_lockbox"
        ] = 0.99
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
        assert any("COVERAGE_MISMATCH" in error for error in universe.errors)


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
        assert any(
            "OHLC_MISMATCH_RATIO_HIGH" in reason for reason in bad_entry["reason_codes"]
        )


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
        manifest = json.loads(
            Path(activated["manifest_path"]).read_text(encoding="utf-8")
        )
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
        manifest = json.loads(
            Path(activated["manifest_path"]).read_text(encoding="utf-8")
        )
        run_manifest = json.loads(
            (
                native_root / "runs" / "run-reverse-exclude" / "run_manifest.json"
            ).read_text(encoding="utf-8")
        )
        run_cells = {item["timeframe"]: item for item in run_manifest["results"]}
        availability_cells = {}
        audited_cells = {}
        contract_reasons = []
        for timeframe, entry in manifest["entries"]["EURUSD"].items():
            reason = "SESSION_COVERAGE_LOW:final_lockbox:0.900000<0.950000"
            coverage = dict(entry["expected_session_coverage_by_partition"])
            coverage["final_lockbox"] = 0.9
            sidecar = json.loads(
                (native_root / entry["sidecar_relative_path"]).read_text(
                    encoding="utf-8"
                )
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
                "source_provenance_sha256": sidecar["source_provenance_sha256"],
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
