import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pm_native_activation import (
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)
from pm_native_consensus import canonical_sha256
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
import tools.audit_activate_native_history as activation_tool


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


def _digest(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _interval(start, end):
    return {
        "start_utc": pd.Timestamp(start).isoformat(),
        "end_exclusive_utc": pd.Timestamp(end).isoformat(),
    }


def _v5_contract(origin, end):
    partitions = {
        "optimizer_train": (origin + pd.Timedelta(days=10), origin + pd.Timedelta(days=40)),
        "tuning_validation": (origin + pd.Timedelta(days=40), origin + pd.Timedelta(days=70)),
        "final_lockbox": (origin + pd.Timedelta(days=70), origin + pd.Timedelta(days=100)),
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
    strategy = {
        timeframe: _interval(origin, end) for timeframe in DEFAULT_TIMEFRAMES
    }
    source = {
        timeframe: dict(strategy[timeframe]) for timeframe in DEFAULT_TIMEFRAMES
    }
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
    reference_intervals = {
        target: {
            reference: {
                window: dict(interval)
                for window, interval in windows[target].items()
            }
            for reference in graph[target]
        }
        for target in DEFAULT_TIMEFRAMES
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


def _bars(origin, days=100):
    index = pd.date_range(origin, periods=days * 24 * 12, freq="5min")
    # Piecewise constant within M15 prevents floating reduction-order noise
    # while retaining distinct bars at every resolution.
    value = 100.0 + (np.arange(len(index)) // 3).astype(float) * 0.0001
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


def _resample(frame, rule):
    return frame.resample(rule).agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    ).dropna()


def _pagination(timeframe, leaf_contract):
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


def _write_leaf(native_root, run_id, timeframe, frame, partitions, leaf_contract):
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
    coverage = dict.fromkeys(partitions, 1.0)
    details = {
        "method": SEGMENTED_SESSION_COVERAGE_METHOD_V2,
        "status": "estimated",
        "performance_inputs_used": False,
        "timestamp_only": True,
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
        "expected_session_coverage_by_partition": coverage,
        "expected_session_coverage_details_by_partition": details,
        "observed_session_bars_by_partition": {
            name: value for name, value in observed_counts.items()
        },
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


def _write_v5_run(native_root, run_id, *, corrupt_timeframe=None, context_only=False):
    origin = pd.Timestamp("2025-01-01", tz="UTC")
    end = origin + pd.Timedelta(days=100)
    contract, leaves, partitions = _v5_contract(origin, end)
    m5 = _bars(origin)
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
            location = frame.index[frame.index >= partitions["optimizer_train"][0]][2]
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


def _rewrite_no_history(native_root, run_id, timeframe):
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


def _activate(native_root, run_id, activation_id):
    return audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=[SYMBOL],
        timeframes=DEFAULT_TIMEFRAMES,
        activation_id=activation_id,
        thresholds=ParityThresholds(min_compared_native_bars=20),
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_v5_all_twelve_edges_survive_sorted_json_and_production_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-clean")

    activated = _activate(native_root, "run-clean", "activation-clean")

    assert activated["status"] == "active", activated
    assert activated["counts"]["active"] == 6
    for timeframe in DEFAULT_TIMEFRAMES:
        entry = activated["entries"][SYMBOL][timeframe]
        report = json.loads(
            (native_root / entry["parity_relative_path"]).read_text(encoding="utf-8")
        )
        assert report["reference_timeframes"] == cross_resolution_reference_map(
            DEFAULT_TIMEFRAMES
        )[timeframe]
        assert set(report["edges"]) == set(report["reference_timeframes"])
        assert all(
            detail["status"] == "PASS" for detail in report["windows"].values()
        )
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )
    assert universe.valid, universe.errors
    artifact = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "D1",
        production_contract_required=True,
    )
    assert artifact.valid, artifact.errors


@pytest.mark.parametrize("target", DEFAULT_TIMEFRAMES)
def test_v5_single_target_corruption_isolates_only_that_specialist(tmp_path, target):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, f"run-corrupt-{target}", corrupt_timeframe=target)

    activated = _activate(
        native_root,
        f"run-corrupt-{target}",
        f"activation-corrupt-{target}",
    )

    assert activated["status"] == "active", activated
    statuses = {
        timeframe: entry["status"]
        for timeframe, entry in activated["entries"][SYMBOL].items()
    }
    assert statuses[target] == "excluded_no_trade"
    assert {key for key, value in statuses.items() if value != "active"} == {target}
    assert any(
        entry["consensus_disagreement_count"] > 0
        for timeframe, entry in activated["entries"][SYMBOL].items()
        if timeframe != target
    )
    universe = resolve_active_native_universe(
        tmp_path / "data",
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors


def test_v5_strategy_context_corruption_cannot_hide_outside_optimizer_partitions(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(
        native_root,
        "run-context",
        corrupt_timeframe="H1",
        context_only=True,
    )

    activated = _activate(native_root, "run-context", "activation-context")

    h1 = activated["entries"][SYMBOL]["H1"]
    report = json.loads(
        (native_root / h1["parity_relative_path"]).read_text(encoding="utf-8")
    )
    assert h1["status"] == "excluded_no_trade"
    assert report["windows"]["strategy_full_context"]["status"] == "FAIL"
    assert all(
        report["windows"][partition]["status"] == "PASS"
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox")
    )


def test_v5_one_terminal_reference_plus_one_exact_pass_remains_active(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-one-unavailable")
    _rewrite_no_history(native_root, "run-one-unavailable", "M15")

    activated = _activate(
        native_root,
        "run-one-unavailable",
        "activation-one-unavailable",
    )

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M15"]["status"] == "excluded_no_trade"
    assert activated["entries"][SYMBOL]["M5"]["status"] == "active"
    assert activated["entries"][SYMBOL]["M5"]["consensus_disagreement_count"] == 4
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors


def test_v5_both_terminal_references_form_semantic_cell_exclusion(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-both-unavailable")
    _rewrite_no_history(native_root, "run-both-unavailable", "M15")
    _rewrite_no_history(native_root, "run-both-unavailable", "M30")

    activated = _activate(
        native_root,
        "run-both-unavailable",
        "activation-both-unavailable",
    )

    assert activated["status"] == "active", activated
    assert activated["entries"][SYMBOL]["M5"]["status"] == "excluded_no_trade"
    report_path = (
        native_root
        / activated["entries"][SYMBOL]["M5"]["parity_relative_path"]
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "FAIL"
    assert all(detail["status"] == "FAIL" for detail in report["windows"].values())


def test_v5_authenticated_all_unavailable_symbol_resolves_as_no_trade(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-all-unavailable")
    for timeframe in DEFAULT_TIMEFRAMES:
        _rewrite_no_history(
            native_root,
            "run-all-unavailable",
            timeframe,
        )

    activated = _activate(
        native_root,
        "run-all-unavailable",
        "activation-all-unavailable",
    )

    assert activated["status"] == "active", activated
    assert activated["counts"]["active"] == 0
    assert activated["counts"]["excluded_no_trade"] == 6
    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        production_contract_required=True,
    )
    assert universe.valid, universe.errors
    assert universe.symbol_status(SYMBOL) == "excluded_no_trade"
    assert universe.active_timeframes(SYMBOL) == ()
    assert universe.excluded_timeframes(SYMBOL) == DEFAULT_TIMEFRAMES


def test_v5_repinning_fabricated_consensus_does_not_defeat_raw_recomputation(tmp_path):
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-repin")
    activated = _activate(native_root, "run-repin", "activation-repin")
    manifest_path = Path(activated["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["entries"][SYMBOL]["D1"]
    consensus_path = native_root / entry["parity_relative_path"]
    consensus = json.loads(consensus_path.read_text(encoding="utf-8"))
    reference = consensus["reference_timeframes"][0]
    edge = consensus["edges"][reference]
    edge["report"]["windows"]["optimizer_train"]["pair_report_sha256"] = "0" * 64
    edge["report_sha256"] = canonical_sha256(edge["report"])
    consensus["consensus_sha256"] = canonical_sha256(
        {key: value for key, value in consensus.items() if key != "consensus_sha256"}
    )
    consensus_path.write_text(
        json.dumps(consensus, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry["parity_sha256"] = sha256_file(consensus_path)
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

    universe = resolve_active_native_universe(
        data_dir,
        [SYMBOL],
        verify_exclusion_evidence=False,
        production_contract_required=True,
    )

    assert not universe.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error
        or "EDGE_PAIR_HASH_INVALID" in error
        for error in universe.errors
    )
    artifact = resolve_active_native_artifact(
        data_dir,
        SYMBOL,
        "D1",
        production_contract_required=True,
    )
    assert not artifact.valid
    assert any(
        "RAW_PAIR_RECOMPUTATION_MISMATCH" in error
        for error in artifact.errors
    )


def test_v5_failed_audit_never_advances_existing_active_pointer(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-first")
    first = _activate(native_root, "run-first", "activation-first")
    pointer_path = native_root / "ACTIVE.json"
    first_pointer = pointer_path.read_bytes()

    _write_v5_run(native_root, "run-bad-contract")
    run_path = native_root / "runs" / "run-bad-contract" / "run_manifest.json"
    run = json.loads(run_path.read_text(encoding="utf-8"))
    run["request"]["timeframe_consensus_policy"]["evaluate_all_references"] = False
    run_path.write_text(
        json.dumps(run, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    failed = _activate(native_root, "run-bad-contract", "activation-bad-contract")

    assert first["status"] == "active"
    assert failed["status"] == "failed"
    assert not failed["active_pointer_updated"]
    assert pointer_path.read_bytes() == first_pointer


def test_v5_missing_pinned_source_is_integrity_error_not_semantic_exclusion(tmp_path):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-missing-bars")
    (
        native_root
        / "runs"
        / "run-missing-bars"
        / SYMBOL
        / "H1"
        / "bars.csv"
    ).unlink()

    result = _activate(
        native_root,
        "run-missing-bars",
        "activation-missing-bars",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["entries"][SYMBOL]["H1"]["status"] == "failed"
    assert any(
        "SOURCE_BARS_MISSING" in reason
        for reason in result["entries"][SYMBOL]["H1"]["reason_codes"]
    )


def test_v5_fail_plus_pending_consensus_stays_pending_and_pointer_is_unchanged(
    tmp_path,
    monkeypatch,
):
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_v5_run(native_root, "run-pending-base")
    base = _activate(
        native_root,
        "run-pending-base",
        "activation-pending-base",
    )
    pointer_path = native_root / "ACTIVE.json"
    original_pointer = pointer_path.read_bytes()
    _write_v5_run(native_root, "run-pending-candidate")
    original_audit = activation_tool.audit_native_cross_resolution

    def _mixed_edge_states(*args, **kwargs):
        report = original_audit(*args, **kwargs)
        if kwargs.get("activation_target_timeframe") != "H1":
            return report
        report = json.loads(json.dumps(report))
        window = report["partitions"]["optimizer_train"]
        if kwargs.get("fine_timeframe") == "M30":
            window["status"] = "FAIL"
            window["reason_codes"] = ["SYNTHETIC_SEMANTIC_FAIL"]
            report["status"] = "FAIL"
        elif kwargs.get("coarse_timeframe") == "H4":
            window["status"] = "PENDING"
            window["reason_codes"] = ["SYNTHETIC_PENDING"]
            report["status"] = "PENDING"
        return report

    monkeypatch.setattr(
        activation_tool,
        "audit_native_cross_resolution",
        _mixed_edge_states,
    )
    candidate = _activate(
        native_root,
        "run-pending-candidate",
        "activation-pending-candidate",
    )

    assert base["status"] == "active"
    assert candidate["status"] == "pending"
    assert candidate["entries"][SYMBOL]["H1"]["status"] == "pending"
    assert not candidate["active_pointer_updated"]
    assert pointer_path.read_bytes() == original_pointer
