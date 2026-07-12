import json
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from pm_core import PipelineConfig
from pm_native_activation import (
    ACTIVATION_MANIFEST_SCHEMA,
    ACTIVE_POINTER_SCHEMA,
    NATIVE_PRODUCTION_TIMEFRAMES,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    validate_native_exclusion_no_trade_grid,
)
from pm_pipeline import OptimizationPipeline, PipelineResult, PortfolioManager
from pm_portfolio_replay import active_native_portfolio_contract
from tools.acquire_native_history import (
    ACQUISITION_CONTRACT_VERSION,
    DEFAULT_PAGE_DAYS,
    MANIFEST_SCHEMA_VERSION,
    SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
)
from tools.audit_activate_native_history import audit_and_activate


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


def _availability_request_contract(boundaries):
    common_start = pd.Timestamp(boundaries["optimizer_train"]["start_ts"])
    common_as_of = pd.Timestamp(boundaries["final_lockbox"]["end_ts"])
    return {
        "horizon_years": 9,
        "requested_information_cutoff_utc": common_as_of.isoformat(),
        "requested_horizon_start_utc": common_start.isoformat(),
        "common_requested_start_utc": common_start.isoformat(),
        "required_evidence_lead_start_utc": common_start.isoformat(),
        "warmup_days": 0.0,
        "safety_days": 0.0,
        "closed_bar_slack_seconds": max(TIMEFRAME_SECONDS.values()),
        "common_as_of_utc": common_as_of.isoformat(),
        "boundary_tolerance_days": 3.0,
        "window_years": {
            "optimizer_train": 5,
            "tuning_validation": 2,
            "final_lockbox": 2,
        },
        "embargo_days": 7.0,
        "common_calendar_boundaries": boundaries,
    }


def _availability_pagination(timeframe, request_contract, *, has_data):
    seconds = TIMEFRAME_SECONDS[timeframe]
    common_start = pd.Timestamp(request_contract["common_requested_start_utc"])
    common_as_of = pd.Timestamp(request_contract["common_as_of_utc"])
    if timeframe in {"H4", "D1"}:
        fetch_start = common_start - pd.Timedelta(seconds=seconds)
        fetch_end = common_as_of + pd.Timedelta(seconds=1)
        alignment_mode = "broker_observed_successor_or_nominal_close_v1"
        sentinel_slack = 1
    else:
        fetch_start = common_start.ceil(f"{seconds}s")
        fetch_end = common_as_of.floor(f"{seconds}s")
        alignment_mode = "fixed_utc_native_boundary_v1"
        sentinel_slack = 0
    configured_slots = max(1, DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds)
    configured_slots = min(configured_slots, int(_SOURCE_ENVIRONMENT["terminal"]["max_bars"] * 0.90))
    page_span_seconds = configured_slots * seconds
    intervals = []
    cursor = fetch_start
    while cursor < fetch_end:
        page_end = min(cursor + pd.Timedelta(seconds=page_span_seconds), fetch_end)
        intervals.append((cursor, page_end))
        cursor = page_end
    data_timestamp = pd.Timestamp("2025-01-01T00:00:00Z")
    data_index = next(
        (
            index
            for index, (start, end) in enumerate(intervals)
            if has_data and start <= data_timestamp < end
        ),
        None,
    )
    empty_pages = [
        {
            "start_utc": start.isoformat(),
            "end_exclusive_utc": end.isoformat(),
            "attempts": 3,
        }
        for index, (start, end) in enumerate(intervals)
        if index != data_index
    ]
    pages_with_data = 1 if data_index is not None else 0
    request = {
        **request_contract,
        "native_fetch_start_utc": fetch_start.isoformat(),
        "native_fetch_end_exclusive_utc": fetch_end.isoformat(),
        "alignment_mode": alignment_mode,
        "successor_sentinel_fetch_slack_seconds": sentinel_slack,
    }
    pagination = {
        "page_span_seconds": page_span_seconds,
        "pages_requested": len(intervals),
        "pages_with_data": pages_with_data,
        "empty_pages": empty_pages,
        "failed_pages": [],
        "truncated_pages": [],
        "attempts_total": len(empty_pages) * 3 + pages_with_data,
        "retries_per_page": 3,
    }
    return request, pagination


def _write_unavailable_run(
    native_root: Path,
    run_id: str,
    *,
    symbol: str = "SOLUSD",
    failure_status: str = "no_history",
) -> None:
    run_root = native_root / "runs" / run_id
    run_root.mkdir(parents=True)
    boundaries = {
        "optimizer_train": {
            "start_ts": "2017-01-01T00:00:00Z",
            "end_ts": "2022-01-01T00:00:00Z",
        },
        "tuning_validation": {
            "start_ts": "2022-01-08T00:00:00Z",
            "end_ts": "2024-01-08T00:00:00Z",
        },
        "final_lockbox": {
            "start_ts": "2024-01-15T00:00:00Z",
            "end_ts": "2026-01-15T00:00:00Z",
        },
    }
    request_contract = _availability_request_contract(boundaries)
    seconds = {
        "M5": 300,
        "M15": 900,
        "M30": 1800,
        "H1": 3600,
        "H4": 14400,
        "D1": 86400,
    }
    results = []
    for timeframe in NATIVE_PRODUCTION_TIMEFRAMES:
        leaf = run_root / symbol / timeframe
        leaf.mkdir(parents=True)
        status = "partial" if failure_status == "partial_history" else "failed"
        provenance_request, pagination = _availability_pagination(
            timeframe,
            request_contract,
            has_data=failure_status in {"partial_history", "no_fully_closed_history"},
        )
        provenance = {
            "schema_version": SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "immutable_record": True,
            "run_id": run_id,
            "status": status,
            "failure_status": failure_status,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "canonical_symbol": symbol,
            "symbol": symbol,
            "broker_symbol": symbol,
            "timeframe": timeframe,
            "timeframe_seconds": seconds[timeframe],
            "source_kind": "broker_native",
            "timezone": "UTC",
            "timestamp_semantics": "bar_open_utc",
            "bar_interval_semantics": "half_open_[bar_open,bar_close)",
            "bar_close_column": "_BAR_CLOSE_TS",
            "complete": False,
            "request": provenance_request,
            "source_environment": _SOURCE_ENVIRONMENT,
            "pagination": pagination,
        }
        if failure_status == "partial_history":
            index = pd.date_range(
                "2025-01-01",
                periods=10,
                freq=f"{seconds[timeframe]}s",
                tz="UTC",
            )
            frame = pd.DataFrame(
                {
                    "Open": range(10),
                    "High": [value + 1 for value in range(10)],
                    "Low": [value - 1 for value in range(10)],
                    "Close": [value + 0.5 for value in range(10)],
                    "Volume": 1,
                    "_BAR_CLOSE_TS": index + pd.to_timedelta(seconds[timeframe], unit="s"),
                },
                index=index,
            )
            bars = leaf / "bars.csv"
            frame.to_csv(bars, index_label="time", lineterminator="\n")
            digest = hashlib.sha256(bars.read_bytes()).hexdigest()
            provenance.update(
                {
                    "artifact": {
                        "relative_path": "bars.csv",
                        "sha256": digest,
                        "bytes": bars.stat().st_size,
                        "rows": len(frame),
                    },
                    "sha256": digest,
                    "rows": len(frame),
                    "start_ts": index[0].isoformat(),
                    "end_ts": index[-1].isoformat(),
                }
            )
        else:
            digest = None
            provenance.update(
                {
                    "artifact": None,
                    "sha256": None,
                    "rows": 0,
                    "start_ts": None,
                    "end_ts": None,
                }
            )
        (leaf / "provenance.json").write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        results.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "status": status,
                "failure_status": failure_status,
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
                "rows": 10 if failure_status == "partial_history" else 0,
                "sha256": digest,
            }
        )
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "immutable_record": True,
        "complete": True,
        "run_id": run_id,
        "request": {
            "symbols": [symbol],
            "timeframes": list(NATIVE_PRODUCTION_TIMEFRAMES),
            "retries": 3,
            **request_contract,
        },
        "source_environment": _SOURCE_ENVIRONMENT,
        "results": results,
    }
    (run_root / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_short_history_symbol_is_immutable_excluded_no_trade_not_activation_failure(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(
        native_root,
        "short-sol",
        failure_status="partial_history",
    )

    result = audit_and_activate(
        native_root,
        run_id="short-sol",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="availability-only-sol",
    )

    assert result["status"] == "active"
    assert result["active_pointer_updated"]
    eligibility = result["universe_eligibility"]
    assert eligibility["performance_inputs_used"] is False
    assert eligibility["eligible_symbols"] == []
    assert list(eligibility["excluded_symbols"]) == ["SOLUSD"]
    assert all(
        entry["status"] == "excluded_no_trade"
        for entry in result["entries"]["SOLUSD"].values()
    )

    universe = resolve_active_native_universe(data_dir, ["SOLUSD"])
    assert universe.valid, universe.errors
    assert universe.symbol_status("SOLUSD") == "excluded_no_trade"
    resolution = resolve_active_native_artifact(data_dir, "SOLUSD", "D1")
    assert not resolution.valid
    assert "NATIVE_ACTIVE_SYMBOL_EXCLUDED_NO_TRADE" in resolution.errors

    contract, errors = active_native_portfolio_contract(data_dir, ["SOLUSD"])
    assert errors == []
    assert contract["eligible_symbols"] == []
    assert contract["excluded_symbols"] == ["SOLUSD"]
    assert contract["bars_sha256_by_source"] == {}


def test_manifest_only_availability_exclusion_is_activation_failure(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(
        native_root,
        "missing-proof",
        failure_status="partial_history",
    )
    (native_root / "runs" / "missing-proof" / "SOLUSD" / "M5" / "provenance.json").unlink()

    result = audit_and_activate(
        native_root,
        run_id="missing-proof",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="missing-proof-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert not (native_root / "ACTIVE.json").exists()


def test_partial_history_corrupt_bars_are_activation_failure(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(
        native_root,
        "corrupt-partial",
        failure_status="partial_history",
    )
    bars = native_root / "runs" / "corrupt-partial" / "SOLUSD" / "H1" / "bars.csv"
    with bars.open("a", encoding="utf-8") as handle:
        handle.write("tamper\n")

    result = audit_and_activate(
        native_root,
        run_id="corrupt-partial",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="corrupt-partial-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]


def test_no_history_provenance_failure_status_must_match_manifest(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "false-no-history", failure_status="no_history")
    provenance_path = (
        native_root
        / "runs"
        / "false-no-history"
        / "SOLUSD"
        / "D1"
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["failure_status"] = "fetch_failed"
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")

    result = audit_and_activate(
        native_root,
        run_id="false-no-history",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="false-no-history-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]


def test_no_history_one_page_cannot_claim_nine_year_horizon(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "truncated-no-history", failure_status="no_history")
    provenance_path = (
        native_root
        / "runs"
        / "truncated-no-history"
        / "SOLUSD"
        / "M5"
        / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    first_page = provenance["pagination"]["empty_pages"][0]
    provenance["pagination"].update(
        {
            "pages_requested": 1,
            "pages_with_data": 0,
            "empty_pages": [first_page],
            "attempts_total": int(first_page["attempts"]),
        }
    )
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")

    result = audit_and_activate(
        native_root,
        run_id="truncated-no-history",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="truncated-no-history-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert any(
        "SOURCE_PAGINATION_PAGE_COUNT_MISMATCH" in reason
        for reason in result["entries"]["SOLUSD"]["M5"]["reason_codes"]
    )


def test_foreign_run_provenance_cannot_enter_activation(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "bound-run", failure_status="partial_history")
    provenance_path = (
        native_root / "runs" / "bound-run" / "SOLUSD" / "H1" / "provenance.json"
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["run_id"] = "foreign-run"
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")

    result = audit_and_activate(
        native_root,
        run_id="bound-run",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="foreign-run-provenance-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert any(
        "SOURCE_RUN_ID_MISMATCH" in reason
        for reason in result["entries"]["SOLUSD"]["H1"]["reason_codes"]
    )


@pytest.mark.parametrize(
    "tamper_kind",
    [
        "acquisition_contract",
        "canonical_symbol",
        "timeframe_seconds",
        "request_contract",
        "source_identity",
    ],
)
def test_availability_provenance_lineage_contract_is_exact(
    tmp_path: Path,
    tamper_kind: str,
) -> None:
    native_root = tmp_path / tamper_kind / "data" / "native"
    native_root.mkdir(parents=True)
    run_id = f"lineage-{tamper_kind}"
    _write_unavailable_run(native_root, run_id, failure_status="partial_history")
    provenance_path = native_root / "runs" / run_id / "SOLUSD" / "H4" / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    if tamper_kind == "acquisition_contract":
        provenance["acquisition_contract_version"] = "foreign-contract"
    elif tamper_kind == "canonical_symbol":
        provenance["canonical_symbol"] = "OTHER"
    elif tamper_kind == "timeframe_seconds":
        provenance["timeframe_seconds"] += 1
    elif tamper_kind == "request_contract":
        provenance["request"]["horizon_years"] += 1
    else:
        provenance["source_environment"]["account"]["login"] += 1
    provenance_path.write_text(json.dumps(provenance), encoding="utf-8")

    result = audit_and_activate(
        native_root,
        run_id=run_id,
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id=f"activation-{tamper_kind}",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]


def test_fetch_failure_cannot_masquerade_as_availability_exclusion(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "fetch-broken", failure_status="fetch_failed")

    result = audit_and_activate(
        native_root,
        run_id="fetch-broken",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="fetch-broken-activation",
    )

    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["universe_eligibility"]["excluded_symbols"] == {}
    assert "SOLUSD" in result["universe_eligibility"]["invalid_symbols"]


def test_symbol_resolution_failure_blocks_instead_of_hiding_mapping_defect(
    tmp_path: Path,
) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(
        native_root,
        "unresolved-alias",
        failure_status="symbol_resolution_failed",
    )

    result = audit_and_activate(
        native_root,
        run_id="unresolved-alias",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="unresolved-alias-activation",
    )

    assert result["status"] == "failed"
    assert result["universe_eligibility"]["excluded_symbols"] == {}
    reasons = result["universe_eligibility"]["invalid_symbols"]["SOLUSD"]["reason_codes"]
    assert any("symbol_resolution_failed" in reason for reason in reasons)


def test_write_and_quality_failures_are_activation_blockers(tmp_path: Path) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    for failure_status in ("write_failed", "data_quality_failed"):
        run_id = f"run-{failure_status}"
        _write_unavailable_run(
            native_root,
            run_id,
            failure_status=failure_status,
        )
        result = audit_and_activate(
            native_root,
            run_id=run_id,
            symbols=["SOLUSD"],
            timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
            activation_id=f"activation-{failure_status}",
        )
        assert result["status"] == "failed"
        assert not result["active_pointer_updated"]
        assert result["universe_eligibility"]["excluded_symbols"] == {}


def test_availability_exclusion_cannot_mask_broken_success_sibling(
    tmp_path: Path,
) -> None:
    native_root = tmp_path / "data" / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(
        native_root,
        "mixed-source",
        failure_status="partial_history",
    )
    manifest_path = native_root / "runs" / "mixed-source" / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    h1 = next(
        item for item in manifest["results"] if item["timeframe"] == "H1"
    )
    h1.update(
        {
            "status": "success",
            "failure_status": None,
            "rows": 10,
            "sha256": "a" * 64,
        }
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    result = audit_and_activate(
        native_root,
        run_id="mixed-source",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="mixed-source-activation",
    )

    assert result["universe_eligibility"]["excluded_symbols"]["SOLUSD"]
    assert result["status"] == "failed"
    assert not result["active_pointer_updated"]
    assert result["entries"]["SOLUSD"]["H1"]["audit_status"] == "FAIL"


def test_portfolio_manager_materializes_bound_6x4_no_trade_grid_before_search(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "new-ton", symbol="TONUSD")
    audit_and_activate(
        native_root,
        run_id="new-ton",
        symbols=["TONUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="availability-only-ton",
    )
    config = PipelineConfig(
        data_dir=data_dir,
        output_dir=tmp_path / "outputs",
        log_dir=tmp_path / "logs",
        timeframes=list(NATIVE_PRODUCTION_TIMEFRAMES),
    )
    manager = PortfolioManager(
        config,
        ["TONUSD"],
        config_file=str(tmp_path / "pm_configs.json"),
    )
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: (_ for _ in ()).throw(AssertionError("excluded symbol was optimized")),
    )
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})

    assert manager.initial_optimization() == {}
    payload = json.loads((tmp_path / "pm_configs.json").read_text(encoding="utf-8"))["TONUSD"]
    universe = resolve_active_native_universe(data_dir, ["TONUSD"])
    binding = universe.exclusion_binding("TONUSD")
    assert validate_native_exclusion_no_trade_grid(payload, binding) == []
    assert len(payload["regime_configs"]) == 6
    assert sum(len(regimes) for regimes in payload["regime_configs"].values()) == 24

    extra_timeframe = json.loads(json.dumps(payload))
    extra_timeframe["regime_configs"]["M1"] = {
        "TREND": extra_timeframe["regime_configs"]["M5"]["TREND"]
    }
    assert any(
        "TIMEFRAME_GRID_NOT_EXACT" in error
        for error in validate_native_exclusion_no_trade_grid(extra_timeframe, binding)
    )

    extra_regime = json.loads(json.dumps(payload))
    extra_regime["regime_configs"]["M5"]["SECRET"] = extra_regime[
        "regime_configs"
    ]["M5"]["TREND"]
    assert any(
        "REGIME_GRID_NOT_EXACT" in error
        for error in validate_native_exclusion_no_trade_grid(extra_regime, binding)
    )

    missing_default = json.loads(json.dumps(payload))
    missing_default.pop("default_config")
    assert any(
        "DEFAULT_CONFIG_MISSING" in error
        for error in validate_native_exclusion_no_trade_grid(missing_default, binding)
    )

    promoted_default = json.loads(json.dumps(payload))
    promoted_default["default_config"]["promotion_status"] = "PROMOTED"
    assert any(
        "DEFAULT_CONFIG_STATUS_INVALID" in error
        for error in validate_native_exclusion_no_trade_grid(promoted_default, binding)
    )

    # A strategy-name marker with a promoted cell is not an exact NO_TRADE
    # grid.  Reconciliation must repair the persisted ledger rather than trust
    # the looser in-memory marker predicate.
    payload["regime_configs"]["M5"]["TREND"]["promotion_status"] = "PROMOTED"
    (tmp_path / "pm_configs.json").write_text(
        json.dumps({"TONUSD": payload}, indent=2),
        encoding="utf-8",
    )
    manager._load_configs()
    assert manager.initial_optimization() == {}
    repaired = json.loads(
        (tmp_path / "pm_configs.json").read_text(encoding="utf-8")
    )["TONUSD"]
    assert validate_native_exclusion_no_trade_grid(repaired, binding) == []


def test_calendar_loader_fails_before_legacy_m5_or_resampling(tmp_path: Path, monkeypatch) -> None:
    config = PipelineConfig(
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "outputs",
        log_dir=tmp_path / "logs",
        timeframes=list(NATIVE_PRODUCTION_TIMEFRAMES),
        production_split_mode="calendar_lockbox_v2",
    )
    pipeline = OptimizationPipeline(config)
    monkeypatch.setattr(
        pipeline.data_loader,
        "load_native_timeframe",
        lambda symbol, timeframe: (
            None,
            {
                "source_kind": "missing",
                "symbol": symbol,
                "timeframe": timeframe,
                "provenance_valid": False,
                "provenance_errors": ["TEST_NATIVE_MISSING"],
            },
        ),
    )
    monkeypatch.setattr(
        pipeline.data_loader,
        "get_data",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("calendar production must not read legacy M5")
        ),
    )
    monkeypatch.setattr(
        pipeline.data_loader,
        "resample",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("calendar production must not resample M5")
        ),
    )

    frames, metadata = pipeline._load_symbol_timeframe_data("EURUSD")

    assert frames == {}
    assert metadata["native_source_gate"]["strict_no_fallback"] is True
    assert set(metadata["native_source_gate"]["errors_by_timeframe"]) == set(
        NATIVE_PRODUCTION_TIMEFRAMES
    )


def _activate_manifest_only_eligible_symbol(
    native_root: Path,
    symbol: str,
    activation_id: str,
) -> None:
    entries = {
        symbol: {
            timeframe: {
                "status": "active",
                "audit_status": "PASS",
                "symbol": symbol,
                "timeframe": timeframe,
                "coverage_method": "train_only_robust_weekly_session_rate_v1",
                "expected_session_coverage_by_partition": {
                    "optimizer_train": 0.99,
                    "tuning_validation": 0.99,
                    "final_lockbox": 0.99,
                },
            }
            for timeframe in NATIVE_PRODUCTION_TIMEFRAMES
        }
    }
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "activation_id": activation_id,
        "status": "active",
        "entries": entries,
    }
    manifest_path = native_root / "activations" / f"{activation_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pointer = {
        "schema_version": ACTIVE_POINTER_SCHEMA,
        "activation_id": activation_id,
        "manifest_relative_path": str(manifest_path.relative_to(native_root)).replace(
            "\\", "/"
        ),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    }
    (native_root / "ACTIVE.json").write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_later_eligibility_forces_old_exclusion_back_into_retraining(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "old-unavailable", symbol="TONUSD")
    audit_and_activate(
        native_root,
        run_id="old-unavailable",
        symbols=["TONUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="old-exclusion",
    )
    config = PipelineConfig(
        data_dir=data_dir,
        output_dir=tmp_path / "outputs",
        log_dir=tmp_path / "logs",
        timeframes=list(NATIVE_PRODUCTION_TIMEFRAMES),
    )
    manager = PortfolioManager(
        config,
        ["TONUSD"],
        config_file=str(tmp_path / "pm_configs.json"),
    )
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    assert manager.initial_optimization() == {}

    _activate_manifest_only_eligible_symbol(native_root, "TONUSD", "now-eligible")
    calls = []

    def run_for_symbol(symbol: str) -> PipelineResult:
        calls.append(symbol)
        return PipelineResult(symbol=symbol, success=False, error_message="test stop")

    monkeypatch.setattr(manager.pipeline, "run_for_symbol", run_for_symbol)
    assert manager.retrain_symbol("TONUSD") is False
    assert calls == ["TONUSD"]


def test_performance_tamper_invalidates_universe_contract(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    native_root = data_dir / "native"
    native_root.mkdir(parents=True)
    _write_unavailable_run(native_root, "no-history")
    activated = audit_and_activate(
        native_root,
        run_id="no-history",
        symbols=["SOLUSD"],
        timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        activation_id="availability-only",
    )
    manifest_path = Path(activated["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["universe_eligibility"]["performance_inputs_used"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    # Keep the pointer hash internally consistent to prove semantic validation,
    # not only byte-tamper detection, rejects performance-based eligibility.
    import hashlib

    pointer_path = native_root / "ACTIVE.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")

    universe = resolve_active_native_universe(data_dir, ["SOLUSD"])
    assert "NATIVE_UNIVERSE_ELIGIBILITY_PERFORMANCE_INPUTS_NOT_FORBIDDEN" in universe.errors
