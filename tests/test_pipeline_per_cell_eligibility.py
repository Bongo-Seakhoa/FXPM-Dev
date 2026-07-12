from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import pm_pipeline
from pm_core import PipelineConfig
from pm_evidence import (
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_timeframe_calendar_boundaries,
)
from pm_pipeline import (
    OptimizationPipeline,
    PipelineResult,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
    build_artifact_meta,
)
from pm_memory_governor import TaskOutcome


def _install_fake_governor(
    monkeypatch: pytest.MonkeyPatch,
    run_symbol,
) -> None:
    """Drive the parent integration contract without bypassing its worker seam."""

    class FakeMemoryGovernor:
        def __init__(self, **_kwargs):
            pass

        def iter_completed(self, worker, tasks):
            assert worker is pm_pipeline._governed_optimize_symbol_worker
            for task in tasks:
                (
                    _config_dict,
                    _runtime_bindings,
                    symbol,
                    generation_id,
                    receipt_value,
                ) = task.args
                result = run_symbol(symbol)
                if not result.success or result.config is None:
                    detail = str(result.error_message or "injected technical failure")
                    memory = pm_pipeline.is_memory_failure(detail)
                    yield TaskOutcome(
                        task_key=symbol,
                        status="FAILED_MEMORY" if memory else "FAILED_TECHNICAL",
                        error_type="InjectedMemoryError" if memory else "InjectedError",
                        error_message=detail,
                        technical_failure=True,
                        memory_failure=memory,
                    )
                    continue
                receipt = {
                    "schema_version": pm_pipeline.OPTIMIZER_RECEIPT_SCHEMA,
                    "generation_id": generation_id,
                    "symbol": symbol,
                    "config": result.config.to_dict(),
                    "counters": {
                        "duration_seconds": float(result.duration_seconds),
                        "strategies_tested": int(result.strategies_tested),
                        "timeframes_tested": int(result.timeframes_tested),
                        "param_combos_tested": int(result.param_combos_tested),
                        "regime_winners": int(result.regime_winners),
                    },
                }
                receipt_path = Path(receipt_value)
                digest, byte_count = pm_pipeline._atomic_write_optimizer_receipt(
                    receipt_path,
                    receipt,
                )
                yield TaskOutcome(
                    task_key=symbol,
                    status="COMPLETED",
                    value={
                        "schema_version": pm_pipeline.OPTIMIZER_RECEIPT_POINTER_SCHEMA,
                        "generation_id": generation_id,
                        "symbol": symbol,
                        "path": str(receipt_path.resolve()),
                        "sha256": digest,
                        "bytes": byte_count,
                    },
                )

    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", FakeMemoryGovernor)


class _MixedUniverse:
    explicit_contract = True
    errors = []
    activation_id = "activation-cell-test"
    activation_manifest_sha256 = "a" * 64
    eligible_symbols = ("EURUSD",)
    excluded_symbols = {}
    symbols_with_active_cells = ("EURUSD",)
    fully_excluded_symbols = ()
    calendar_contract = {
        "common_as_of_utc": "2026-01-01T00:00:00Z",
        "common_requested_start_utc": "2016-01-01T00:00:00Z",
        "common_calendar_boundaries": {
            "optimizer_train": {
                "start_ts": "2017-01-01T00:00:00Z",
                "end_ts": "2022-01-01T00:00:00Z",
            },
            "tuning_validation": {
                "start_ts": "2022-01-08T00:00:00Z",
                "end_ts": "2024-01-01T00:00:00Z",
            },
            "final_lockbox": {
                "start_ts": "2024-01-08T00:00:00Z",
                "end_ts": "2026-01-01T00:00:00Z",
            },
        },
    }

    @staticmethod
    def active_timeframes(_symbol: str):
        return ("H1",)

    @staticmethod
    def excluded_timeframes(_symbol: str):
        return ("M5",)

    @staticmethod
    def cell_status(_symbol: str, timeframe: str):
        return "active" if timeframe == "H1" else "excluded_no_trade"

    @staticmethod
    def cell_exclusion_binding(symbol: str, timeframe: str):
        if timeframe != "M5":
            return {}
        return {
            "schema_version": "fxpm-native-cell-eligibility-v1",
            "activation_id": "activation-cell-test",
            "activation_manifest_sha256": "a" * 64,
            "symbol": symbol,
            "timeframe": timeframe,
            "status": "excluded_no_trade",
            "reason_codes": ["SESSION_COVERAGE_LOW:optimizer_train"],
        }


def _native_frame() -> pd.DataFrame:
    index = pd.date_range("2016-01-01T00:00:00Z", "2025-12-31T23:00:00Z", freq="12h")
    values = 100.0 + np.linspace(0.0, 5.0, len(index))
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.2,
            "Low": values - 0.2,
            "Close": values,
            "Volume": 1,
            "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
        },
        index=index,
    )


def test_calendar_loader_uses_active_h1_when_m5_is_manifest_excluded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    (data_dir / "native").mkdir(parents=True)
    (data_dir / "native" / "ACTIVE.json").write_text("{}", encoding="utf-8")
    cfg = PipelineConfig(
        data_dir=data_dir,
        output_dir=tmp_path / "out",
        timeframes=["M5", "H1"],
        production_split_mode="calendar_lockbox_v2",
        feature_disk_cache_enabled=False,
    )
    pipeline = OptimizationPipeline(cfg)
    frame = _native_frame()
    load_calls: list[str] = []

    def resolve_universe(_data_dir, *configured_symbols, **_kwargs):
        # A worker validates the immutable full activation. Passing its single
        # symbol as `configured_symbols` would falsely reject the other 99
        # activation members as config-roster omissions.
        assert configured_symbols == ()
        return _MixedUniverse()

    monkeypatch.setattr(
        "pm_pipeline.resolve_active_native_universe",
        resolve_universe,
    )

    def load_native(_symbol: str, timeframe: str):
        load_calls.append(timeframe)
        assert timeframe == "H1", "excluded M5 must never be loaded"
        return frame, {
            "source_kind": "broker_native",
            "symbol": "EURUSD",
            "timeframe": "H1",
            "provenance_valid": True,
            "provenance_errors": [],
            "sha256": "b" * 64,
            "rows": len(frame),
        }

    monkeypatch.setattr(pipeline.data_loader, "load_native_timeframe", load_native)
    monkeypatch.setattr(
        pipeline.data_loader,
        "resample",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("calendar cell eligibility must never resample")
        ),
    )

    frames, metadata = pipeline._load_symbol_timeframe_data("EURUSD")

    assert load_calls == ["H1"]
    assert set(frames) == {"H1"}
    assert metadata["base_timeframe"] == "H1"
    assert metadata["as_of_utc"] == "2026-01-01T00:00:00+00:00"
    assert metadata["windows"]["final_lockbox"]["start_ts"] == (
        "2024-01-08T00:00:00+00:00"
    )
    eligibility = metadata["native_cell_eligibility"]
    assert eligibility["active_timeframes"] == ["H1"]
    assert eligibility["excluded_timeframes"] == ["M5"]
    assert eligibility["excluded_bindings"]["M5"]["timeframe"] == "M5"


def test_v3_training_starts_are_per_timeframe_but_active_windows_are_concurrent(
    tmp_path: Path,
) -> None:
    cfg = PipelineConfig(
        data_dir=tmp_path,
        output_dir=tmp_path / "out",
        timeframes=["M5", "H1"],
        production_split_mode="calendar_lockbox_v2",
        evidence_purge_days=7.0,
    )
    pipeline = OptimizationPipeline(cfg)
    as_of = pd.Timestamp("2026-01-01T00:00:00Z")
    boundaries = {
        timeframe: build_timeframe_calendar_boundaries(
            as_of,
            timeframe,
            timeframe_window_years=cfg.evidence_timeframe_window_years,
            embargo=pd.Timedelta(days=7),
        ).to_dict()["windows"]
        for timeframe in ("M5", "H1")
    }
    contract = {
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "common_as_of_utc": as_of.isoformat(),
        "timeframe_window_years": cfg.evidence_timeframe_window_years,
        "timeframe_calendar_boundaries": boundaries,
        "common_calendar_boundaries": boundaries["M5"],
    }
    frame = _native_frame()

    m5_meta = pipeline._build_calendar_workflow_meta(
        frame,
        as_of_utc=as_of,
        base_timeframe="M5",
        calendar_contract=contract,
    )
    h1_meta = pipeline._build_calendar_workflow_meta(
        frame,
        as_of_utc=as_of,
        base_timeframe="H1",
        calendar_contract=contract,
    )

    assert m5_meta["split_contract_version"] == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    assert h1_meta["split_contract_version"] == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    assert (
        m5_meta["windows"]["optimizer_train"]["start_ts"]
        != h1_meta["windows"]["optimizer_train"]["start_ts"]
    )
    for partition in ("tuning_validation", "final_lockbox"):
        assert m5_meta["windows"][partition]["start_ts"] == h1_meta["windows"][partition]["start_ts"]
        assert m5_meta["windows"][partition]["end_ts"] == h1_meta["windows"][partition]["end_ts"]


def test_native_exclusion_markers_bind_all_four_regimes_to_exact_cell(tmp_path: Path) -> None:
    pipeline = OptimizationPipeline(
        PipelineConfig(
            data_dir=tmp_path,
            output_dir=tmp_path / "out",
            timeframes=["M5"],
            feature_disk_cache_enabled=False,
        )
    )
    binding = _MixedUniverse.cell_exclusion_binding("EURUSD", "M5")

    markers = pipeline._native_cell_exclusion_markers(binding)

    assert set(markers) == {"TREND", "RANGE", "BREAKOUT", "CHOP"}
    for marker in markers.values():
        assert marker.is_no_trade_marker()
        assert marker.promotion_status == "NO_TRADE"
        assert marker.artifact_meta["native_cell_exclusion_binding"] == binding
        assert marker.validation_evidence["native_cell_exclusion_binding"] == binding


def test_mixed_symbol_produces_complete_grid_without_searching_excluded_timeframe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = PipelineConfig(
        data_dir=tmp_path,
        output_dir=tmp_path / "out",
        timeframes=["M5", "H1"],
        production_split_mode="calendar_lockbox_v2",
        evidence_sufficiency_enabled=True,
        feature_disk_cache_enabled=False,
    )
    pipeline = OptimizationPipeline(cfg)
    frame = _native_frame()
    metadata = pipeline._build_calendar_workflow_meta(
        frame,
        as_of_utc="2026-01-01T00:00:00Z",
        base_timeframe="H1",
        calendar_contract=_MixedUniverse.calendar_contract,
    )
    # Deliberately omit the audited session-count fields so H1 completes as an
    # evidence NO_TRADE result before feature/search. M5 is activation-excluded.
    metadata["timeframe_sources"] = {
        "H1": {
            "source_kind": "broker_native",
            "provenance_valid": True,
            "provenance_errors": [],
        }
    }
    metadata["native_cell_eligibility"] = {
        "active_timeframes": ["H1"],
        "excluded_timeframes": ["M5"],
        "excluded_bindings": {
            "M5": _MixedUniverse.cell_exclusion_binding("EURUSD", "M5")
        },
    }
    monkeypatch.setattr(
        pipeline,
        "_load_symbol_timeframe_data",
        lambda _symbol: ({"H1": frame}, metadata),
    )
    monkeypatch.setattr(
        "pm_pipeline.validate_regime_params_provenance",
        lambda *_args, **_kwargs: {
            "valid": True,
            "reason": "test",
            "provenance_path": "test",
        },
    )
    monkeypatch.setattr(
        "pm_pipeline.FeatureComputer.compute_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("evidence-failed active H1 must not reach features")
        ),
    )
    monkeypatch.setattr(
        pipeline.regime_optimizer,
        "optimize_symbol",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("excluded/evidence-failed cells must not reach search")
        ),
    )

    result = pipeline.run_for_symbol("EURUSD")

    assert result.success
    assert result.config is not None
    assert set(result.config.regime_configs) == {"M5", "H1"}
    assert all(
        set(regimes) == {"TREND", "RANGE", "BREAKOUT", "CHOP"}
        for regimes in result.config.regime_configs.values()
    )
    assert all(
        "native_cell_exclusion_binding" in marker.artifact_meta
        for marker in result.config.regime_configs["M5"].values()
    )
    assert all(
        "native_cell_exclusion_binding" not in marker.artifact_meta
        for marker in result.config.regime_configs["H1"].values()
    )


def test_optimizer_infrastructure_failure_does_not_mutate_existing_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_path = tmp_path / "pm_configs.json"
    cfg = PipelineConfig(
        research_mode=True,
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        timeframes=["M5"],
        production_split_mode="bleeding_edge",
        optimization_max_workers=1,
        feature_disk_cache_enabled=False,
    )
    manager = PortfolioManager(cfg, ["EURUSD"], config_file=str(ledger_path))
    winner = RegimeConfig(
        strategy_name="TestStrategy",
        parameters={"period": 10},
        quality_score=0.5,
        promotion_status="PROMOTED",
        trained_at=datetime.now(),
        valid_until=datetime.now() + timedelta(days=7),
    )
    old_config = SymbolConfig(
        symbol="EURUSD",
        regime_configs={"M5": {"TREND": winner}},
        default_config=winner,
        strategy_name="TestStrategy",
        timeframe="M5",
        parameters={"period": 10},
        is_validated=True,
        optimized_at=datetime.now(),
        valid_until=datetime.now() + timedelta(days=7),
    )
    assert manager.ledger.update_symbol("EURUSD", old_config)
    manager._load_configs()
    before = ledger_path.read_bytes()

    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("parent pipeline.run_for_symbol bypassed governor"),
    )
    _install_fake_governor(
        monkeypatch,
        lambda symbol: PipelineResult(
            symbol=symbol,
            success=False,
            error_message="simulated ArrayMemoryError",
        ),
    )

    results = manager.initial_optimization(overwrite=True)

    assert not results["EURUSD"].success
    assert ledger_path.read_bytes() == before


def test_successful_symbol_is_not_persisted_when_sibling_fails_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_path = tmp_path / "pm_configs.json"
    cfg = PipelineConfig(
        research_mode=True,
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        timeframes=["M5"],
        production_split_mode="bleeding_edge",
        optimization_max_workers=1,
        feature_disk_cache_enabled=False,
    )
    manager = PortfolioManager(
        cfg, ["EURUSD", "GBPUSD"], config_file=str(ledger_path)
    )

    def make_config(symbol: str, strategy: str) -> SymbolConfig:
        winner = RegimeConfig(
            strategy_name=strategy,
            parameters={"period": 10},
            quality_score=0.5,
            promotion_status="PROMOTED",
            trained_at=datetime.now(),
            valid_until=datetime.now() + timedelta(days=7),
        )
        return SymbolConfig(
            symbol=symbol,
            regime_configs={"M5": {"TREND": winner}},
            default_config=winner,
            strategy_name=strategy,
            timeframe="M5",
            parameters={"period": 10},
            is_validated=True,
            artifact_meta=build_artifact_meta(cfg),
            optimized_at=datetime.now(),
            valid_until=datetime.now() + timedelta(days=7),
        )

    assert manager.ledger.update_symbol("EURUSD", make_config("EURUSD", "OLD_EUR"))
    assert manager.ledger.update_symbol("GBPUSD", make_config("GBPUSD", "OLD_GBP"))
    manager._load_configs()
    before = ledger_path.read_bytes()

    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})

    def run(symbol: str) -> PipelineResult:
        if symbol == "EURUSD":
            return PipelineResult(
                symbol=symbol,
                success=True,
                config=make_config(symbol, "NEW_EUR"),
            )
        return PipelineResult(
            symbol=symbol,
            success=False,
            error_message="simulated worker OOM",
        )

    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("parent pipeline.run_for_symbol bypassed governor"),
    )
    _install_fake_governor(monkeypatch, run)
    results = manager.initial_optimization(overwrite=True)

    assert results["EURUSD"].success
    assert not results["GBPUSD"].success
    assert ledger_path.read_bytes() == before
    stage_path, stage_manifest, _active_manifest = manager._optimization_generation_paths()
    assert stage_path.exists()
    staged_payload = json.loads(stage_path.read_text(encoding="utf-8"))
    assert staged_payload["EURUSD"]["strategy_name"] == "NEW_EUR"
    assert json.loads(stage_manifest.read_text(encoding="utf-8"))[
        "generation_status"
    ] == "FAILED_TECHNICAL"


def test_complete_generation_promotes_whole_ledger_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger_path = tmp_path / "pm_configs.json"
    cfg = PipelineConfig(
        research_mode=True,
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        timeframes=["M5"],
        production_split_mode="bleeding_edge",
        optimization_max_workers=1,
        feature_disk_cache_enabled=False,
    )
    manager = PortfolioManager(
        cfg, ["EURUSD", "GBPUSD"], config_file=str(ledger_path)
    )

    def make_config(symbol: str) -> SymbolConfig:
        strategy = f"NEW_{symbol}"
        winner = RegimeConfig(
            strategy_name=strategy,
            parameters={},
            quality_score=0.6,
            promotion_status="PROMOTED",
            trained_at=datetime.now(),
            valid_until=datetime.now() + timedelta(days=7),
        )
        return SymbolConfig(
            symbol=symbol,
            regime_configs={"M5": {"TREND": winner}},
            default_config=winner,
            strategy_name=strategy,
            timeframe="M5",
            parameters={},
            is_validated=True,
            artifact_meta=build_artifact_meta(cfg),
            optimized_at=datetime.now(),
            valid_until=datetime.now() + timedelta(days=7),
        )

    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("parent pipeline.run_for_symbol bypassed governor"),
    )
    _install_fake_governor(
        monkeypatch,
        lambda symbol: PipelineResult(
            symbol=symbol,
            success=True,
            config=make_config(symbol),
        ),
    )

    results = manager.initial_optimization(overwrite=True)

    assert all(result.success for result in results.values())
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert set(payload) == {"EURUSD", "GBPUSD"}
    assert payload["EURUSD"]["strategy_name"] == "NEW_EURUSD"
    assert payload["GBPUSD"]["strategy_name"] == "NEW_GBPUSD"
    generation_ids = {
        value["artifact_meta"]["optimization_generation"]["generation_id"]
        for value in payload.values()
    }
    assert len(generation_ids) == 1
    stage_path, stage_manifest, active_manifest = manager._optimization_generation_paths()
    assert not stage_path.exists()
    assert not stage_manifest.exists()
    assert json.loads(active_manifest.read_text(encoding="utf-8"))[
        "generation_status"
    ] == "PROMOTED"


def test_regime_preflight_checks_active_cells_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = PipelineConfig(
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        timeframes=["M5", "H1"],
        production_split_mode="calendar_lockbox_v2",
        regime_params_file=str(tmp_path / "regime_params.json"),
    )
    manager = PortfolioManager(cfg, ["EURUSD"], config_file=str(tmp_path / "ledger.json"))
    calls: list[tuple[str, str]] = []

    def validate(symbol: str, timeframe: str, *_args, **_kwargs):
        calls.append((symbol, timeframe))
        return {"valid": False, "reason": "test missing provenance"}

    monkeypatch.setattr("pm_pipeline.validate_regime_params_provenance", validate)

    with pytest.raises(RuntimeError, match="ACTIVE_REGIME_PROVENANCE_PREFLIGHT_FAILED"):
        manager._preflight_active_regime_provenance(_MixedUniverse())

    assert calls == [("EURUSD", "H1")]
