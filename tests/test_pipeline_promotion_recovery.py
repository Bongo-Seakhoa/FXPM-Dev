from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_pipeline import PortfolioManager


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _manager(ledger_path: Path) -> PortfolioManager:
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config_file = ledger_path
    manager.config = SimpleNamespace(_deployment_config_path="")
    manager._last_promotion_authorization = object()
    return manager


def _recovery_manager(tmp_path: Path) -> tuple[PortfolioManager, Path]:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(b'{"book":"last-good"}\n')
    manager = _manager(ledger)
    manager.config = SimpleNamespace(
        _deployment_config_path=str(config_path),
    )
    return manager, ledger


def _abandoned_paths(
    tmp_path: Path,
    generation_id: str,
) -> tuple[Path, Path]:
    digest = hashlib.sha256(generation_id.encode("utf-8")).hexdigest()
    runs = tmp_path / "fxpm-production-runs"
    runs.mkdir(exist_ok=True)
    run_root = runs / f"run-{digest}"
    assembly = tmp_path / f"fxpm-authority-{digest}"
    run_root.mkdir()
    assembly.mkdir()
    return run_root, assembly


def _journal(
    manager: PortfolioManager,
    *,
    base: bytes,
    target: bytes,
) -> tuple[Path, Path, dict]:
    _stage, stage_manifest, active_manifest = (
        manager._optimization_generation_paths()
    )
    stage_manifest.write_text("{}", encoding="utf-8")
    promoted = {
        "generation_id": "generation-1",
        "generation_status": "PROMOTED",
        "ledger_sha256": _sha(target),
    }
    journal = manager._write_promotion_journal(
        base_ledger_sha256=_sha(base),
        target_ledger_sha256=_sha(target),
        stage_manifest_path=stage_manifest,
        promoted_manifest=promoted,
    )
    return journal, active_manifest, promoted


def test_interrupted_post_cas_publication_is_completed_from_bound_journal(
    tmp_path: Path,
) -> None:
    base = b'{"book":"last-good"}\n'
    target = b'{"book":"new"}\n'
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(base)
    manager = _manager(ledger)
    journal, active_manifest, promoted = _journal(
        manager, base=base, target=target
    )

    ledger.write_bytes(target)  # exact state after atomic CAS, before manifest write
    assert manager._reconcile_interrupted_promotion() == "COMPLETED"

    assert ledger.read_bytes() == target
    assert json.loads(active_manifest.read_text(encoding="utf-8")) == promoted
    assert not journal.exists()
    assert manager._last_promotion_authorization is None


def test_reconciliation_runs_retention_prune(
    tmp_path: Path,
) -> None:
    base = b'{"book":"last-good"}\n'
    target = b'{"book":"new"}\n'
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(base)
    manager = _manager(ledger)
    _journal_path, _active_manifest, _promoted = _journal(
        manager,
        base=base,
        target=target,
    )
    ledger.write_bytes(target)
    observed: list[bool] = []
    manager._prune_production_generation_storage_locked = (  # type: ignore[method-assign]  # noqa: SLF001
        lambda _manifest: observed.append(True) or {}
    )

    assert (
        manager._reconcile_interrupted_promotion(  # noqa: SLF001
            lock_timeout_seconds=0.0,
        )
        == "COMPLETED"
    )
    assert observed == [True]


def test_interrupted_pre_cas_publication_preserves_last_good_book(
    tmp_path: Path,
) -> None:
    base = b'{"book":"last-good"}\n'
    target = b'{"book":"new"}\n'
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(base)
    manager = _manager(ledger)
    journal, active_manifest, _promoted = _journal(
        manager, base=base, target=target
    )

    assert manager._reconcile_interrupted_promotion() == "ABORTED_BEFORE_CAS"
    assert ledger.read_bytes() == base
    assert not active_manifest.exists()
    assert not journal.exists()


def test_abandoned_supervised_generation_removes_only_exact_owned_roots(
    tmp_path: Path,
) -> None:
    generation_id = "a" * 64
    manager, _ledger = _recovery_manager(tmp_path)
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    unrelated_run = run_root.parent / f"run-{'b' * 64}"
    unrelated_assembly = tmp_path / f"fxpm-authority-{'b' * 64}"
    unrelated_run.mkdir()
    unrelated_assembly.mkdir()

    status = manager._recover_abandoned_supervised_generation(  # noqa: SLF001
        generation_id,
        lock_timeout_seconds=0.0,
    )

    assert status.startswith("REMOVED:ASSEMBLY,RUN_ROOT")
    assert not run_root.exists()
    assert not assembly.exists()
    assert unrelated_run.is_dir()
    assert unrelated_assembly.is_dir()


def test_abandoned_supervised_generation_reconciles_pre_cas_journal_first(
    tmp_path: Path,
) -> None:
    generation_id = "c" * 64
    manager, ledger = _recovery_manager(tmp_path)
    base = ledger.read_bytes()
    target = b'{"book":"candidate"}\n'
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    _stage, stage_manifest, _active = manager._optimization_generation_paths()
    stage_manifest.write_text("{}", encoding="utf-8")
    journal = manager._write_promotion_journal(
        base_ledger_sha256=_sha(base),
        target_ledger_sha256=_sha(target),
        stage_manifest_path=stage_manifest,
        promoted_manifest={
            "generation_id": generation_id,
            "generation_status": "PROMOTED",
            "ledger_sha256": _sha(target),
        },
    )
    stage_manifest.unlink()

    status = manager._recover_abandoned_supervised_generation(  # noqa: SLF001
        generation_id,
        lock_timeout_seconds=0.0,
    )

    assert "journal=ABORTED_BEFORE_CAS" in status
    assert not journal.exists()
    assert not run_root.exists()
    assert not assembly.exists()
    assert ledger.read_bytes() == base


def test_abandoned_supervised_generation_preserves_exact_active_reference(
    tmp_path: Path,
) -> None:
    generation_id = "d" * 64
    manager, ledger = _recovery_manager(tmp_path)
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    staged_source = run_root / "staged-ledger.json"
    live = ledger.read_bytes()
    staged_source.write_bytes(live)
    ledger_sha = _sha(live)
    _stage, _stage_manifest, active_path = (
        manager._optimization_generation_paths()
    )
    active = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": generation_id,
        "generation_status": "PROMOTED",
        "ledger_sha256": ledger_sha,
        "ledger_status": {
            "complete_current_contract_universe": True,
        },
        "authority_chain_receipt": {
            "generation_id": generation_id,
            "ledger_sha256": ledger_sha,
        },
        "production_authority_run_root": str(run_root),
        "authority_assembly_path": str(assembly),
        "production_staged_ledger_source_path": str(staged_source),
    }
    active_path.write_text(json.dumps(active), encoding="utf-8")

    status = manager._recover_abandoned_supervised_generation(  # noqa: SLF001
        generation_id,
        lock_timeout_seconds=0.0,
    )

    assert status.startswith("REFERENCED_ACTIVE")
    assert run_root.is_dir()
    assert assembly.is_dir()
    assert active_path.is_file()


def test_abandoned_supervised_generation_rejects_ambiguous_reference(
    tmp_path: Path,
) -> None:
    generation_id = "e" * 64
    manager, _ledger = _recovery_manager(tmp_path)
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    _stage, _stage_manifest, active_path = (
        manager._optimization_generation_paths()
    )
    active_path.write_text('{"generation_status":"PROMOTED"}', encoding="utf-8")

    with pytest.raises(
        RuntimeError,
        match="SUPERVISED_GENERATION_RECOVERY_REFERENCE_AMBIGUOUS",
    ):
        manager._recover_abandoned_supervised_generation(  # noqa: SLF001
            generation_id,
            lock_timeout_seconds=0.0,
        )

    assert run_root.is_dir()
    assert assembly.is_dir()


def test_abandoned_supervised_generation_discards_only_exact_fixed_stage(
    tmp_path: Path,
) -> None:
    generation_id = "f" * 64
    manager, ledger = _recovery_manager(tmp_path)
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    stage, stage_manifest, _active = manager._optimization_generation_paths()
    target = b'{"book":"candidate"}\n'
    stage.write_bytes(target)
    stage_manifest.write_text(
        json.dumps(
            {
                "generation_id": generation_id,
                "ledger_sha256": _sha(target),
                "ledger_staging_path": str(stage),
            }
        ),
        encoding="utf-8",
    )
    assert _sha(ledger.read_bytes()) != _sha(target)

    status = manager._recover_abandoned_supervised_generation(  # noqa: SLF001
        generation_id,
        lock_timeout_seconds=0.0,
    )

    assert "stage=DISCARDED" in status
    assert not stage.exists()
    assert not stage_manifest.exists()
    assert not run_root.exists()
    assert not assembly.exists()


def test_abandoned_supervised_generation_recovers_manifest_only_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "1" * 64
    manager, ledger = _recovery_manager(tmp_path)
    run_root, assembly = _abandoned_paths(tmp_path, generation_id)
    stage, stage_manifest, _active = manager._optimization_generation_paths()
    target = b'{"book":"candidate"}\n'
    stage.write_bytes(target)
    stage_manifest.write_text(
        json.dumps(
            {
                "generation_id": generation_id,
                "ledger_sha256": _sha(target),
                "ledger_staging_path": str(stage),
            }
        ),
        encoding="utf-8",
    )
    assert _sha(ledger.read_bytes()) != _sha(target)
    original_unlink = Path.unlink
    interrupt_manifest_unlink = True

    def interrupted_unlink(path: Path, *args, **kwargs):
        nonlocal interrupt_manifest_unlink
        if path == stage_manifest and interrupt_manifest_unlink:
            interrupt_manifest_unlink = False
            raise OSError("injected manifest unlink interruption")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted_unlink)
    with pytest.raises(
        RuntimeError,
        match="SUPERVISED_GENERATION_RECOVERY_STAGE_AMBIGUOUS",
    ):
        manager._recover_abandoned_supervised_generation(  # noqa: SLF001
            generation_id,
            lock_timeout_seconds=0.0,
        )

    assert not stage.exists()
    assert stage_manifest.is_file()
    assert run_root.is_dir()
    assert assembly.is_dir()

    status = manager._recover_abandoned_supervised_generation(  # noqa: SLF001
        generation_id,
        lock_timeout_seconds=0.0,
    )

    assert "stage=DISCARDED_MANIFEST_ONLY" in status
    assert not stage_manifest.exists()
    assert not run_root.exists()
    assert not assembly.exists()


def test_promotion_recovery_fails_closed_on_unbound_external_ledger_change(
    tmp_path: Path,
) -> None:
    base = b'{"book":"last-good"}\n'
    target = b'{"book":"new"}\n'
    external = b'{"book":"external"}\n'
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(base)
    manager = _manager(ledger)
    journal, _active_manifest, _promoted = _journal(
        manager, base=base, target=target
    )
    ledger.write_bytes(external)

    with pytest.raises(
        RuntimeError, match="OPTIMIZATION_PROMOTION_EXTERNAL_LEDGER_CHANGE"
    ):
        manager._reconcile_interrupted_promotion()

    assert ledger.read_bytes() == external
    assert journal.exists()


def test_normal_promotion_clears_journal_only_after_ledger_and_manifest(
    tmp_path: Path,
) -> None:
    base = b'{"book":"last-good"}\n'
    target = b'{"book":"new"}\n'
    ledger = tmp_path / "pm_configs.json"
    ledger.write_bytes(base)
    manager = _manager(ledger)
    stage, stage_manifest, active_manifest = manager._optimization_generation_paths()
    stage.write_bytes(target)
    stage_manifest.write_text("{}", encoding="utf-8")
    manager._verify_staged_authority_before_cas = lambda *args, **kwargs: None

    class _Ledger:
        def __init__(self) -> None:
            self.loaded = False

        def load(self) -> None:
            self.loaded = True

        def get_all_configs(self):
            return {"loaded": True}

    manager.ledger = _Ledger()
    manifest = {
        "generation_id": "generation-1",
        "base_ledger_sha256": _sha(base),
        "ledger_sha256": _sha(target),
    }

    manager._promote_staged_generation(
        stage, stage_manifest, manifest, universe=None
    )

    assert ledger.read_bytes() == target
    assert json.loads(active_manifest.read_text(encoding="utf-8"))[
        "generation_status"
    ] == "PROMOTED"
    assert manager.ledger.loaded is True
    assert manager.symbol_configs == {"loaded": True}
    assert not manager._optimization_promotion_journal_path().exists()
