import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_pipeline import CALENDAR_SPLIT_MODE, PortfolioManager


def test_production_generation_cannot_cas_without_raw_authority(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"symbols": ["EURUSD"], "pipeline": {"data_dir": "data"}}),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "pm_configs.json"
    prior = b'{"EURUSD":{"winner":"prior"}}'
    ledger_path.write_bytes(prior)
    stage_path = tmp_path / "pm_configs.staged.json"
    staged = b'{"EURUSD":{"winner":"candidate"}}'
    stage_path.write_bytes(staged)
    stage_manifest = tmp_path / "pm_configs.staged.manifest.json"
    stage_manifest.write_text("{}", encoding="utf-8")

    pipeline = PortfolioManager.__new__(PortfolioManager)
    pipeline.config = SimpleNamespace(
        _deployment_config_path=str(config_path),
        production_split_mode=CALENDAR_SPLIT_MODE,
    )
    pipeline.config_file = str(ledger_path)
    pipeline.symbols = ["EURUSD"]
    manifest = {
        "generation_id": "generation-test",
        "ledger_sha256": hashlib.sha256(staged).hexdigest(),
        "base_ledger_sha256": hashlib.sha256(prior).hexdigest(),
    }

    with pytest.raises(RuntimeError, match="OPTIMIZATION_AUTHORITY_PRE_CAS_BLOCKED"):
        pipeline._promote_staged_generation(
            stage_path,
            stage_manifest,
            manifest,
            SimpleNamespace(),
        )

    assert ledger_path.read_bytes() == prior
    assert stage_path.read_bytes() == staged
