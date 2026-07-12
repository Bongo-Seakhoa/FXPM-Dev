from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_pipeline
from pm_core import PipelineConfig
from pm_memory_governor import TaskOutcome
from pm_pipeline import (
    OPTIMIZER_RECEIPT_POINTER_SCHEMA,
    OPTIMIZER_RECEIPT_SCHEMA,
    OptimizationGenerationRequest,
    OptimizationPipeline,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
    _atomic_write_optimizer_receipt,
    build_artifact_meta,
)


def _config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        research_mode=True,
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        timeframes=["M5"],
        production_split_mode="bleeding_edge",
        optimization_max_workers=1,
        feature_disk_cache_enabled=False,
        oos_evidence_export_enabled=False,
    )


def _winner(symbol: str, cfg: PipelineConfig, strategy: str, *, expired: bool) -> SymbolConfig:
    now = datetime.now()
    valid_until = now - timedelta(days=1) if expired else now + timedelta(days=7)
    cell = RegimeConfig(
        strategy_name=strategy,
        parameters={"period": 10},
        quality_score=0.5,
        promotion_status="PROMOTED",
        trained_at=now,
        valid_until=valid_until,
    )
    return SymbolConfig(
        symbol=symbol,
        regime_configs={"M5": {"TREND": cell}},
        default_config=cell,
        strategy_name=strategy,
        timeframe="M5",
        parameters={"period": 10},
        is_validated=True,
        artifact_meta=build_artifact_meta(cfg),
        optimized_at=now,
        valid_until=valid_until,
    )


class _Governor:
    configs: dict[str, SymbolConfig] = {}
    failures: dict[str, str] = {}
    instances: list["_Governor"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.tasks = []
        type(self).instances.append(self)

    def iter_completed(self, worker, tasks):
        assert worker is pm_pipeline._governed_optimize_symbol_worker
        for task in tasks:
            self.tasks.append(task)
            _cfg, _bindings, symbol, generation_id, receipt_value = task.args
            if symbol in self.failures:
                yield TaskOutcome(
                    task_key=symbol,
                    status="FAILED_MEMORY",
                    error_type="MemoryError",
                    error_message=self.failures[symbol],
                    technical_failure=True,
                    memory_failure=True,
                )
                return
            receipt = {
                "schema_version": OPTIMIZER_RECEIPT_SCHEMA,
                "generation_id": generation_id,
                "symbol": symbol,
                "config": self.configs[symbol].to_dict(),
                "counters": {
                    "duration_seconds": 1.0,
                    "strategies_tested": 1,
                    "timeframes_tested": 1,
                    "param_combos_tested": 1,
                    "regime_winners": 1,
                },
            }
            path = Path(receipt_value)
            digest, byte_count = _atomic_write_optimizer_receipt(path, receipt)
            yield TaskOutcome(
                task_key=symbol,
                status="COMPLETED",
                value={
                    "schema_version": OPTIMIZER_RECEIPT_POINTER_SCHEMA,
                    "generation_id": generation_id,
                    "symbol": symbol,
                    "path": str(path.resolve()),
                    "sha256": digest,
                    "bytes": byte_count,
                },
            )


def _manager(tmp_path: Path, monkeypatch, symbols=("AUDUSD", "EURUSD")):
    cfg = _config(tmp_path)
    ledger = tmp_path / "pm_configs.json"
    manager = PortfolioManager(cfg, list(symbols), config_file=str(ledger))
    for symbol in symbols:
        assert manager.ledger.update_symbol(
            symbol,
            _winner(symbol, cfg, f"OLD_{symbol}", expired=True),
        )
    manager._load_configs()
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: None)
    monkeypatch.setattr(manager, "_run_production_replay_audit", lambda: {})
    monkeypatch.setattr(
        manager.pipeline,
        "run_for_symbol",
        lambda _symbol: pytest.fail("parent-process optimizer bypassed governor"),
    )
    _Governor.instances = []
    _Governor.failures = {}
    _Governor.configs = {
        symbol: _winner(symbol, cfg, f"NEW_{symbol}", expired=False)
        for symbol in symbols
    }
    monkeypatch.setattr(pm_pipeline, "MemoryGovernor", _Governor)
    return manager, ledger, cfg


def test_scheduled_due_batch_is_atomic_when_later_symbol_ooms(tmp_path, monkeypatch):
    manager, ledger, _cfg = _manager(tmp_path, monkeypatch)
    before = ledger.read_bytes()
    _Governor.failures = {"EURUSD": "Unable to allocate optimizer array"}

    results = manager.retrain_all_needed()

    assert results == {"AUDUSD": False, "EURUSD": False}
    assert ledger.read_bytes() == before
    assert manager.last_optimization_generation.status == "FAILED_TECHNICAL"
    assert _Governor.instances[0].kwargs["requested_workers"] == 1
    assert [task.key for task in _Governor.instances[0].tasks] == ["AUDUSD", "EURUSD"]


def test_scheduled_success_promotes_due_batch_once_and_retains_full_roster(
    tmp_path,
    monkeypatch,
):
    manager, ledger, cfg = _manager(
        tmp_path,
        monkeypatch,
        symbols=("AUDUSD", "EURUSD", "GBPUSD"),
    )
    assert manager.ledger.update_symbol(
        "GBPUSD",
        _winner("GBPUSD", cfg, "OLD_GBPUSD", expired=False),
    )
    manager._load_configs()
    promote_calls = []
    original_promote = manager._promote_staged_generation

    def promote_once(*args, **kwargs):
        promote_calls.append(args[0])
        return original_promote(*args, **kwargs)

    monkeypatch.setattr(manager, "_promote_staged_generation", promote_once)

    results = manager.retrain_all_needed()

    assert results == {"AUDUSD": True, "EURUSD": True}
    assert len(promote_calls) == 1
    payload = json.loads(ledger.read_text(encoding="utf-8"))
    assert set(payload) == {"AUDUSD", "EURUSD", "GBPUSD"}
    assert payload["AUDUSD"]["strategy_name"] == "NEW_AUDUSD"
    assert payload["EURUSD"]["strategy_name"] == "NEW_EURUSD"
    assert payload["GBPUSD"]["strategy_name"] == "OLD_GBPUSD"
    assert [task.key for task in _Governor.instances[0].tasks] == [
        "AUDUSD",
        "EURUSD",
    ]


def test_manual_single_target_uses_generation_and_retains_sibling(tmp_path, monkeypatch):
    manager, ledger, _cfg = _manager(tmp_path, monkeypatch)

    assert manager.retrain_symbol("EURUSD", force=True)

    payload = json.loads(ledger.read_text(encoding="utf-8"))
    assert payload["EURUSD"]["strategy_name"] == "NEW_EURUSD"
    assert payload["AUDUSD"]["strategy_name"] == "OLD_AUDUSD"
    assert [task.key for task in _Governor.instances[0].tasks] == ["EURUSD"]
    assert manager.last_optimization_generation.promoted
    assert manager.last_optimization_generation.ledger_changed


def test_successful_generation_invalidates_cached_promotion_authorization(
    tmp_path,
    monkeypatch,
):
    manager, _ledger, _cfg = _manager(tmp_path, monkeypatch)
    cached = SimpleNamespace(
        valid=True,
        authorization_id="authorization-for-old-ledger",
        authorizes=lambda *_args: True,
        concise_reason=lambda: "valid",
    )
    manager._last_promotion_authorization = cached

    assert manager.retrain_symbol("EURUSD", force=True)

    assert manager._last_promotion_authorization is None


def test_full_staged_roster_is_replayed_before_live_ledger_cas(tmp_path, monkeypatch):
    manager, ledger, _cfg = _manager(tmp_path, monkeypatch)
    before = ledger.read_bytes()
    observed = []

    def replay():
        assert ledger.read_bytes() == before
        stage = Path(manager._staged_replay_ledger_override)
        staged = json.loads(stage.read_text(encoding="utf-8"))
        assert set(staged) == {"AUDUSD", "EURUSD"}
        assert staged["EURUSD"]["strategy_name"] == "NEW_EURUSD"
        observed.append(True)
        return {"enabled": False, "reason": "test"}

    monkeypatch.setattr(manager, "_run_production_replay_audit", replay)
    assert manager.retrain_symbol("EURUSD", force=True)
    assert observed == [True]
    assert ledger.read_bytes() != before


def test_scheduled_same_activation_blocks_before_governor(tmp_path, monkeypatch):
    manager, ledger, cfg = _manager(tmp_path, monkeypatch, symbols=("EURUSD",))
    cfg.production_split_mode = "calendar_lockbox_v2"
    cfg._deployment_config_path = "config.json"
    existing = manager.symbol_configs["EURUSD"]
    existing.data_window_meta = {
        "native_cell_eligibility": {"activation_id": "same-activation"}
    }
    assert manager.ledger.update_symbol("EURUSD", existing)
    manager._load_configs()
    universe = SimpleNamespace(
        explicit_contract=True,
        activation_id="same-activation",
        activation_manifest_sha256="a" * 64,
        symbols_with_active_cells=("EURUSD",),
        eligible_symbols=("EURUSD",),
        fully_excluded_symbols=(),
        excluded_symbols=(),
    )
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: universe)
    monkeypatch.setattr(
        manager,
        "_preflight_active_regime_provenance",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(manager, "_has_exact_native_cell_grid", lambda *_args: True)

    outcome = manager._run_governed_symbol_generation(
        OptimizationGenerationRequest(
            kind="SCHEDULED_RETRAIN",
            target_symbols=("EURUSD",),
        )
    )

    assert outcome.status == "BLOCKED_STALE_SOURCE"
    assert not outcome.ledger_changed
    assert _Governor.instances == []
    assert json.loads(ledger.read_text(encoding="utf-8"))["EURUSD"][
        "strategy_name"
    ] == "OLD_EURUSD"


def test_new_native_activation_makes_current_ledger_symbol_due(tmp_path, monkeypatch):
    manager, _ledger, cfg = _manager(
        tmp_path,
        monkeypatch,
        symbols=("EURUSD",),
    )
    current = _winner("EURUSD", cfg, "OLD_EURUSD", expired=False)
    assert manager.ledger.update_symbol("EURUSD", current)
    manager._load_configs()
    universe = SimpleNamespace(explicit_contract=True)
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: universe)
    monkeypatch.setattr(manager, "_has_exact_native_cell_grid", lambda *_args: False)

    assert manager.get_symbols_needing_retrain() == ["EURUSD"]


def test_new_activation_expands_effective_targets_before_provenance_preflight(
    tmp_path,
    monkeypatch,
):
    manager, _ledger, cfg = _manager(tmp_path, monkeypatch)
    for symbol in ("AUDUSD", "EURUSD"):
        assert manager.ledger.update_symbol(
            symbol,
            _winner(symbol, cfg, f"OLD_{symbol}", expired=False),
        )
    manager._load_configs()
    universe = SimpleNamespace(
        explicit_contract=True,
        activation_id="new-activation",
        activation_manifest_sha256="c" * 64,
        symbols_with_active_cells=("AUDUSD", "EURUSD"),
        eligible_symbols=("AUDUSD", "EURUSD"),
        fully_excluded_symbols=(),
        excluded_symbols=(),
    )
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: universe)
    monkeypatch.setattr(
        manager,
        "_has_exact_native_cell_grid",
        lambda _symbol, config, _universe: bool(
            config is not None
            and str(config.strategy_name).startswith("NEW_")
        ),
    )
    preflight_targets = []
    monkeypatch.setattr(
        manager,
        "_preflight_active_regime_provenance",
        lambda _universe, symbols: preflight_targets.append(tuple(symbols)),
    )

    assert manager.retrain_symbol("EURUSD", force=True)

    assert preflight_targets == [("EURUSD", "AUDUSD")]
    assert manager.last_optimization_generation.effective_symbols == (
        "EURUSD",
        "AUDUSD",
    )
    assert {task.key for task in _Governor.instances[0].tasks} == {
        "EURUSD",
        "AUDUSD",
    }


def test_promotion_failure_preserves_external_ledger_and_manager_memory(
    tmp_path,
    monkeypatch,
):
    manager, ledger, _cfg = _manager(tmp_path, monkeypatch)
    before_memory = {
        symbol: config.to_dict()
        for symbol, config in manager.symbol_configs.items()
    }
    external_payload = json.loads(ledger.read_text(encoding="utf-8"))
    external_payload["EURUSD"]["validation_reason"] = "external concurrent update"
    external_bytes = json.dumps(external_payload, indent=2).encode("utf-8")
    original_promote = manager._promote_staged_generation

    def concurrent_change_before_cas(*args, **kwargs):
        ledger.write_bytes(external_bytes)
        return original_promote(*args, **kwargs)

    monkeypatch.setattr(
        manager,
        "_promote_staged_generation",
        concurrent_change_before_cas,
    )

    outcome = manager._run_governed_symbol_generation(
        OptimizationGenerationRequest(
            kind="MANUAL_RETRAIN",
            target_symbols=("EURUSD",),
            overwrite_targets=True,
        )
    )

    assert outcome.status == "FAILED_PROMOTION_TECHNICAL"
    assert not outcome.promoted
    assert not outcome.ledger_changed
    assert outcome.technical_failures["__PROMOTION__"].startswith(
        "PROMOTION_TECHNICAL:RuntimeError:"
    )
    assert ledger.read_bytes() == external_bytes
    assert {
        symbol: config.to_dict()
        for symbol, config in manager.symbol_configs.items()
    } == before_memory
    manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
    evidence = manifest["promotion_failure_evidence"]
    assert evidence["external_ledger_change_preserved"] is True
    assert evidence["cas_applied"] is False


def test_post_cas_manifest_failure_is_truthful_and_never_rolls_back(
    tmp_path,
    monkeypatch,
):
    manager, ledger, _cfg = _manager(tmp_path, monkeypatch)
    before = ledger.read_bytes()
    manager._last_promotion_authorization = SimpleNamespace(valid=True)
    _stage, _stage_manifest, active_manifest = (
        manager._optimization_generation_paths()
    )
    original_atomic_write = manager._atomic_write_json

    def fail_active_manifest(path, payload):
        if Path(path).resolve() == active_manifest.resolve():
            raise OSError("simulated active-manifest failure after ledger CAS")
        return original_atomic_write(path, payload)

    monkeypatch.setattr(manager, "_atomic_write_json", fail_active_manifest)

    outcome = manager._run_governed_symbol_generation(
        OptimizationGenerationRequest(
            kind="MANUAL_RETRAIN",
            target_symbols=("EURUSD",),
            overwrite_targets=True,
        )
    )

    assert outcome.status == "PROMOTED_RECOVERY_REQUIRED"
    assert outcome.promoted
    assert outcome.ledger_changed
    assert ledger.read_bytes() != before
    payload = json.loads(ledger.read_text(encoding="utf-8"))
    assert payload["EURUSD"]["strategy_name"] == "NEW_EURUSD"
    assert manager.symbol_configs["EURUSD"].strategy_name == "NEW_EURUSD"
    assert manager._last_promotion_authorization is None
    manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
    evidence = manifest["promotion_failure_evidence"]
    assert evidence["cas_applied"] is True
    assert evidence["manager_reconciled"] is True


def test_generation_request_policy_is_code_owned(tmp_path, monkeypatch):
    manager, _ledger, _cfg = _manager(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="KIND_UNKNOWN"):
        manager._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="AD_HOC",
                target_symbols=("EURUSD",),
            )
        )
    with pytest.raises(ValueError, match="SCHEDULED_RETRAIN_OVERWRITE_FORBIDDEN"):
        manager._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="SCHEDULED_RETRAIN",
                target_symbols=("EURUSD",),
                overwrite_targets=True,
            )
        )
    with pytest.raises(ValueError, match="ONLY_APPLIES_TO_SCHEDULED_RETRAIN"):
        manager._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="MANUAL_RETRAIN",
                target_symbols=("EURUSD",),
                require_fresh_native_activation=True,
            )
        )


def test_unsafe_batch_and_direct_save_apis_fail_closed(tmp_path):
    pipeline = OptimizationPipeline(_config(tmp_path))
    with pytest.raises(RuntimeError, match="UNSAFE_UNGOVERNED_BATCH_API_DISABLED"):
        pipeline.run_for_all(["EURUSD"])
    with pytest.raises(RuntimeError, match="UNSAFE_DIRECT_CONFIG_SAVE_DISABLED"):
        pipeline.save_configs({}, str(tmp_path / "pm_configs.json"))
