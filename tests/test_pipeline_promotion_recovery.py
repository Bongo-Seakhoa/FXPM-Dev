from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pm_pipeline import PortfolioManager


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _manager(ledger_path: Path) -> PortfolioManager:
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config_file = ledger_path
    manager._last_promotion_authorization = object()
    return manager


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
