from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pytest

import pm_pipeline
from pm_core import PipelineConfig
from pm_memory_governor import TaskOutcome
from pm_pipeline import (
    FEATURE_PRECOMPUTE_RESULT_SCHEMA,
    OPTIMIZER_RECEIPT_POINTER_SCHEMA,
    OPTIMIZER_RECEIPT_SCHEMA,
    PipelineResult,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
    _atomic_write_optimizer_receipt,
    _consume_optimizer_receipt,
    _governed_optimize_symbol_worker,
    _optimizer_peak_estimate_bytes,
    _optimizer_receipt_path,
    _ordered_optimizer_memory_work,
    _pipeline_config_to_dict,
    _pipeline_runtime_bindings,
    _restore_pipeline_runtime_bindings,
)


def _pipeline_config(tmp_path: Path, *, workers: int = 1) -> PipelineConfig:
    return PipelineConfig(
        research_mode=True,
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        timeframes=["M5"],
        production_split_mode="bleeding_edge",
        optimization_max_workers=workers,
        feature_disk_cache_enabled=False,
        oos_evidence_export_enabled=False,
    )


def _no_trade_config(symbol: str, cfg: PipelineConfig) -> SymbolConfig:
    now = datetime.now()
    regimes = {}
    for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP"):
        regimes[regime] = RegimeConfig(
            strategy_name="NO_TRADE",
            parameters={},
            quality_score=0.0,
            trained_at=now,
            valid_until=now + timedelta(days=7),
            artifact_meta={"no_trade": True, "reason": "complete search abstained"},
            promotion_status="NO_TRADE",
        )
    default = regimes["TREND"]
    return SymbolConfig(
        symbol=symbol,
        regime_configs={"M5": regimes},
        default_config=default,
        strategy_name="NO_TRADE",
        timeframe="M5",
        parameters={},
        is_validated=False,
        validation_reason="complete semantic all-NO_TRADE result",
        artifact_meta=pm_pipeline.build_artifact_meta(cfg),
        optimized_at=now,
        valid_until=now + timedelta(days=7),
    )


def _receipt_pointer(
    path: Path,
    *,
    generation_id: str,
    symbol: str,
    config: SymbolConfig,
) -> dict[str, Any]:
    payload = {
        "schema_version": OPTIMIZER_RECEIPT_SCHEMA,
        "generation_id": generation_id,
        "symbol": symbol,
        "config": config.to_dict(),
        "counters": {
            "duration_seconds": 1.25,
            "strategies_tested": 3,
            "timeframes_tested": 1,
            "param_combos_tested": 17,
            "regime_winners": 0,
        },
    }
    digest, byte_count = _atomic_write_optimizer_receipt(path, payload)
    return {
        "schema_version": OPTIMIZER_RECEIPT_POINTER_SCHEMA,
        "generation_id": generation_id,
        "symbol": symbol,
        "path": str(path.resolve()),
        "sha256": digest,
        "bytes": byte_count,
    }


class _Universe:
    explicit_contract = True

    def __init__(self, entries: Mapping[str, Any]):
        self.entries = dict(entries)

    def active_timeframes(self, symbol: str):
        return tuple(
            timeframe
            for timeframe, entry in self.entries[symbol].items()
            if entry["status"] == "active"
        )


def test_estimates_are_monotone_input_derived_and_heaviest_first(tmp_path: Path) -> None:
    cfg = _pipeline_config(tmp_path)
    native = cfg.data_dir / "native"
    small = native / "runs" / "r1" / "SMALL" / "M5" / "bars.csv"
    large = native / "runs" / "r1" / "LARGE" / "M5" / "bars.csv"
    small.parent.mkdir(parents=True)
    large.parent.mkdir(parents=True)
    small.write_bytes(b"x" * 1024)
    large.write_bytes(b"x" * 8192)
    universe = _Universe(
        {
            "SMALL": {"M5": {"status": "active", "bars_relative_path": str(small.relative_to(native))}},
            "LARGE": {"M5": {"status": "active", "bars_relative_path": str(large.relative_to(native))}},
        }
    )

    small_estimate = _optimizer_peak_estimate_bytes(cfg, "SMALL", universe)
    large_estimate = _optimizer_peak_estimate_bytes(cfg, "LARGE", universe)

    assert large_estimate > small_estimate
    assert _ordered_optimizer_memory_work(cfg, ["SMALL", "LARGE"], universe) == [
        ("LARGE", large_estimate),
        ("SMALL", small_estimate),
    ]


def test_spawn_runtime_bindings_preserve_deployment_artifact_contract(
    tmp_path: Path,
) -> None:
    broker_specs = tmp_path / "broker_specs.json"
    broker_specs.write_text('{"EURUSD":{"pip_size":0.0001}}', encoding="utf-8")
    deployment = tmp_path / "config.json"
    deployment.write_text(
        json.dumps(
            {
                "_meta": {"scope": "universal"},
                "symbols": ["EURUSD"],
                "position": {"risk_per_trade_pct": 0.25},
                "broker_specs_path": broker_specs.name,
            }
        ),
        encoding="utf-8",
    )
    parent = _pipeline_config(tmp_path)
    parent._deployment_config_path = str(deployment)
    parent._funded_live_execution = True
    child = PipelineConfig(**_pipeline_config_to_dict(parent))
    _restore_pipeline_runtime_bindings(child, _pipeline_runtime_bindings(parent))

    assert child._funded_live_execution is True
    assert (
        pm_pipeline.build_artifact_meta(child)["deployment_contract"]
        == pm_pipeline.build_artifact_meta(parent)["deployment_contract"]
    )


def test_receipt_roundtrip_is_bounded_verified_and_unlinked(tmp_path: Path) -> None:
    cfg = _pipeline_config(tmp_path)
    symbol = "EURUSD"
    generation_id = "generation-test"
    path = tmp_path / "receipt.json"
    original = _no_trade_config(symbol, cfg)
    pointer = _receipt_pointer(
        path,
        generation_id=generation_id,
        symbol=symbol,
        config=original,
    )

    assert len(json.dumps(pointer).encode("utf-8")) < 1024
    result = _consume_optimizer_receipt(
        pointer,
        expected_generation_id=generation_id,
        expected_symbol=symbol,
        expected_path=path,
    )

    assert result.success
    assert result.config is not None
    assert result.config.to_dict() == original.to_dict()
    assert result.regime_winners == 0
    assert all(
        cell.is_no_trade_marker()
        for cells in result.config.regime_configs.values()
        for cell in cells.values()
    )
    assert not path.exists()


def test_receipts_are_generation_scoped(tmp_path: Path) -> None:
    ledger = tmp_path / "pm_configs.json"
    first = _optimizer_receipt_path(ledger, "generation-a", "EURUSD")
    second = _optimizer_receipt_path(ledger, "generation-b", "EURUSD")

    assert first != second
    assert first.parent != second.parent
    assert first.parent.parent == second.parent.parent


def test_invalid_receipt_hash_is_technical_and_unlinked(tmp_path: Path) -> None:
    cfg = _pipeline_config(tmp_path)
    path = tmp_path / "receipt.json"
    pointer = _receipt_pointer(
        path,
        generation_id="g",
        symbol="EURUSD",
        config=_no_trade_config("EURUSD", cfg),
    )
    pointer["sha256"] = "0" * 64

    with pytest.raises(RuntimeError, match="HASH_MISMATCH"):
        _consume_optimizer_receipt(
            pointer,
            expected_generation_id="g",
            expected_symbol="EURUSD",
            expected_path=path,
        )
    assert not path.exists()


@pytest.mark.parametrize(
    ("message", "error_type"),
    [
        ("Unable to allocate 17.4 MiB for an array", MemoryError),
        ("regime provenance unavailable", RuntimeError),
    ],
)
def test_worker_reraises_unsuccessful_results_as_infrastructure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    message: str,
    error_type: type[BaseException],
) -> None:
    cfg = _pipeline_config(tmp_path)

    class FakePipeline:
        def __init__(self, _config):
            pass

        def run_for_symbol(self, symbol):
            return PipelineResult(symbol=symbol, success=False, error_message=message)

    monkeypatch.setattr(pm_pipeline, "OptimizationPipeline", FakePipeline)
    with pytest.raises(error_type):
        _governed_optimize_symbol_worker(
            pm_pipeline._pipeline_config_to_dict(cfg),
            pm_pipeline._pipeline_runtime_bindings(cfg),
            "EURUSD",
            "g",
            str(tmp_path / "receipt.json"),
        )


class _FakeGovernor:
    instances: list["_FakeGovernor"] = []
    mode = "success"
    config: SymbolConfig | None = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.tasks = []
        self.worker = None
        type(self).instances.append(self)

    def iter_completed(self, worker, tasks):
        self.worker = worker
        for task in tasks:
            self.tasks.append(task)
            assert not any(isinstance(value, pd.DataFrame) for value in task.args)
            config_dict, runtime_bindings, symbol, generation_id, receipt_value = task.args
            del config_dict, runtime_bindings
            if self.mode == "success":
                pointer = _receipt_pointer(
                    Path(receipt_value),
                    generation_id=generation_id,
                    symbol=symbol,
                    config=self.config,
                )
                yield TaskOutcome(task_key=symbol, status="COMPLETED", value=pointer)
            elif self.mode == "invalid_receipt":
                yield TaskOutcome(
                    task_key=symbol,
                    status="COMPLETED",
                    value={"schema_version": "wrong"},
                )
            else:
                yield TaskOutcome(
                    task_key=symbol,
                    status=self.mode,
                    error_type="InjectedFailure",
                    error_message="injected scheduler failure",
                    technical_failure=True,
                    memory_failure=self.mode in {"FAILED_MEMORY", "FAILED_MEMORY_BUDGET"},
                )


def _run_manager_with_fake_governor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
) -> tuple[PortfolioManager, dict[str, PipelineResult], bytes]:
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    before = ledger.read_bytes()
    cfg = _pipeline_config(tmp_path, workers=1)
    manager = PortfolioManager(cfg, ["EURUSD"], config_file=str(ledger))
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("same-process optimizer path was used"),
    )
    _FakeGovernor.instances = []
    _FakeGovernor.mode = mode
    _FakeGovernor.config = _no_trade_config("EURUSD", cfg)
    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", _FakeGovernor)
    results = manager.initial_optimization(overwrite=True)
    return manager, results, before


def test_workers_one_still_uses_governed_spawn_worker_and_semantic_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, results, _before = _run_manager_with_fake_governor(
        tmp_path,
        monkeypatch,
        mode="success",
    )

    assert results["EURUSD"].success
    assert len(_FakeGovernor.instances) == 1
    instance = _FakeGovernor.instances[0]
    assert instance.kwargs["requested_workers"] == 1
    assert instance.worker is pm_pipeline._governed_optimize_symbol_worker
    assert len(instance.tasks) == 1
    receipt_base = manager.config_file.resolve().with_name(
        f".{manager.config_file.name}.optimizer-receipts"
    )
    assert not receipt_base.exists()


@pytest.mark.parametrize(
    "mode",
    [
        "FAILED_MEMORY",
        "FAILED_TECHNICAL",
        "FAILED_TIMEOUT_TASK",
        "FAILED_MEMORY_BUDGET",
        "invalid_receipt",
    ],
)
def test_scheduler_and_receipt_failures_are_technical_and_preserve_live_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    manager, results, before = _run_manager_with_fake_governor(
        tmp_path,
        monkeypatch,
        mode=mode,
    )

    assert not results["EURUSD"].success
    assert manager.config_file.read_bytes() == before
    _stage, stage_manifest, _active = manager._optimization_generation_paths()
    manifest = json.loads(stage_manifest.read_text(encoding="utf-8"))
    assert manifest["generation_status"] == "FAILED_TECHNICAL"
    assert "EURUSD" in manifest["technical_failures"]


def test_first_technical_failure_closes_stream_before_second_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "pm_configs.json"
    ledger.write_text("{}", encoding="utf-8")
    before = ledger.read_bytes()
    cfg = _pipeline_config(tmp_path, workers=1)
    manager = PortfolioManager(
        cfg,
        ["AUDUSD", "EURUSD"],
        config_file=str(ledger),
    )
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("same-process optimizer path was used"),
    )
    consumed: list[str] = []

    class FailFastGovernor:
        def __init__(self, **_kwargs):
            pass

        def iter_completed(self, worker, tasks):
            assert worker is pm_pipeline._governed_optimize_symbol_worker
            task_iter = iter(tasks)
            first = next(task_iter)
            consumed.append(first.key)
            yield TaskOutcome(
                task_key=first.key,
                status="FAILED_TECHNICAL",
                error_type="InjectedFailure",
                error_message="first task failed",
                technical_failure=True,
            )
            second = next(task_iter)
            consumed.append(second.key)
            pytest.fail("governor stream continued after terminal generation failure")

    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", FailFastGovernor)
    results = manager.initial_optimization(overwrite=True)

    assert consumed == ["AUDUSD"]
    assert not results["AUDUSD"].success
    assert not results["EURUSD"].success
    assert results["EURUSD"].error_message.startswith("technical_not_run:")
    assert ledger.read_bytes() == before


def test_feature_precompute_uses_governor_even_for_one_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _pipeline_config(tmp_path)
    cfg.feature_disk_cache_precompute_workers = 1
    manager = PortfolioManager(
        cfg,
        ["EURUSD"],
        config_file=str(tmp_path / "pm_configs.json"),
    )

    class FeatureGovernor(_FakeGovernor):
        def iter_completed(self, worker, tasks):
            self.worker = worker
            for task in tasks:
                self.tasks.append(task)
                yield TaskOutcome(
                    task_key=task.key,
                    status="COMPLETED",
                    value={
                        "schema_version": FEATURE_PRECOMPUTE_RESULT_SCHEMA,
                        "symbol": task.key,
                        "symbols_completed": 1,
                        "timeframes_computed_or_retrieved": 1,
                        "feature_cache": {},
                    },
                )

    FeatureGovernor.instances = []
    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", FeatureGovernor)
    summary = manager._precompute_feature_cache_for_symbols(["EURUSD"])

    assert summary["symbols_completed"] == 1
    assert FeatureGovernor.instances[0].worker is pm_pipeline._governed_precompute_feature_cache_worker


def test_feature_precompute_stops_after_first_technical_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _pipeline_config(tmp_path)
    cfg.feature_disk_cache_precompute_workers = 1
    manager = PortfolioManager(
        cfg,
        ["AUDUSD", "EURUSD"],
        config_file=str(tmp_path / "pm_configs.json"),
    )
    monkeypatch.setattr(
        manager.pipeline,
        "precompute_feature_cache",
        lambda _symbols: pytest.fail("same-process feature precompute path was used"),
    )
    consumed: list[str] = []

    class FailFastFeatureGovernor:
        def __init__(self, **_kwargs):
            pass

        def iter_completed(self, worker, tasks):
            assert worker is pm_pipeline._governed_precompute_feature_cache_worker
            task_iter = iter(tasks)
            first = next(task_iter)
            consumed.append(first.key)
            yield TaskOutcome(
                task_key=first.key,
                status="FAILED_MEMORY",
                error_type="MemoryError",
                error_message="injected precompute OOM",
                technical_failure=True,
                memory_failure=True,
            )
            second = next(task_iter)
            consumed.append(second.key)
            pytest.fail("precompute stream continued after first technical failure")

    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", FailFastFeatureGovernor)
    summary = manager._precompute_feature_cache_for_symbols(["AUDUSD", "EURUSD"])

    assert consumed == ["AUDUSD"]
    assert summary["symbols_completed"] == 0
    assert summary["symbols_failed"] == 2
    assert summary["errors"]["AUDUSD"].startswith("FAILED_MEMORY:")
    assert summary["errors"]["EURUSD"].startswith("technical_not_run:")
