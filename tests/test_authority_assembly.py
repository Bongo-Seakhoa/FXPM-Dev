from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_assembly as assembly
import pm_optimization_authority as authority


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, allow_nan=False, sort_keys=True), encoding="utf-8")


def _raw(root: Path, *, detector_rows=()) -> assembly.RawAuthorityInputs:
    raw_manifest = root / "refit" / "raw-manifest.json"
    _write(raw_manifest, {})
    replay_root = root / "capital-replay"
    replay_root.mkdir(exist_ok=True)
    return assembly.RawAuthorityInputs(
        detector_ledger_records=list(detector_rows),
        historical_process_decision={},
        causal_precommit_payload={},
        causal_evidence_payload={},
        causal_reconciliation_payload={},
        historical_book_decision={},
        accepted_book_generation_receipt={},
        current_book_decision={},
        current_refit_receipt={},
        current_refit_raw_manifest_path=raw_manifest,
        current_refit_artifact_root=raw_manifest.parent,
        portfolio_replay_evidence={},
        portfolio_expected_lineage={},
        portfolio_expected_policies={},
        portfolio_validation_policy={},
        capital_artifact={},
        capital_expected_lineage={},
        capital_constraints={},
        capital_expected_selected_policy_point_id=None,
        capital_replay_evidence_root=replay_root,
        capital_trusted_authority_public_keys={},
        capital_trusted_replay_public_keys={},
        capital_trusted_precommitment_public_keys={},
    )


def _call(root: Path, raw: assembly.RawAuthorityInputs):
    config_payload = {
        "symbols": ["EURUSD"],
        "pipeline": {"data_dir": "data"},
    }
    ledger = {}
    config_path = root / "config.json"
    ledger_path = root / "pm_configs.staged.json"
    _write(config_path, config_payload)
    _write(ledger_path, ledger)
    native = SimpleNamespace(
        activation_id="activation-test",
        activation_manifest_sha256="a" * 64,
        entries={},
        cell_status=lambda *_: "source_unavailable",
    )
    return assembly.assemble_authority_directory(
        deployment_root=root,
        target_name="authority-generation-test",
        generation_id="generation-test",
        authorization_scope="DEMO_PROBATION",
        active_config_path=config_path,
        config=SimpleNamespace(data_dir="data"),
        config_payload=config_payload,
        staged_ledger_path=ledger_path,
        ledger=ledger,
        requested_symbols=("EURUSD",),
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
        raw=raw,
    )


def test_index_builder_preserves_sparse_empirical_regimes(tmp_path: Path) -> None:
    row = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "train_regime_support": {
            "observed_regimes": ["TREND"],
            "unobserved_regimes": ["RANGE", "BREAKOUT", "CHOP"],
        },
        "regime_detector_ledger_record": {},
    }
    native = SimpleNamespace(
        activation_id="activation-test",
        activation_manifest_sha256="a" * 64,
    )

    indexes = assembly._build_indexes(
        deployment_root=tmp_path.resolve(),
        native_universe=native,
        raw=_raw(tmp_path, detector_rows=[row]),
    )

    assert tuple(indexes) == authority.ARTIFACTS
    assert indexes["regime_support"]["detector_ledger_records"] == [row]
    assert len(indexes["regime_support"]["detector_ledger_records"]) == 1
    for name, value in indexes.items():
        assert value["schema_version"] == authority.INDEX_SCHEMAS[name]


def test_missing_raw_dependency_fails_before_staging(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    raw = assembly.RawAuthorityInputs(
        **{
            **raw.__dict__,
            "current_refit_raw_manifest_path": tmp_path / "missing.json",
        }
    )

    with pytest.raises(FileNotFoundError):
        _call(tmp_path, raw)

    assert not (tmp_path / "authority-generation-test").exists()
    assert not list(tmp_path.glob(".authority-generation-test.*"))


def test_unverified_raw_closure_never_publishes(tmp_path: Path) -> None:
    with pytest.raises(assembly.AuthorityAssemblyBlocked) as raised:
        _call(tmp_path, _raw(tmp_path))

    assert "CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED" in raised.value.errors
    assert any("PORTFOLIO" in value for value in raised.value.errors)
    assert not (tmp_path / "authority-generation-test").exists()
    assert not list(tmp_path.glob(".authority-generation-test.*"))


def test_canonical_writer_rejects_nonfinite_values() -> None:
    with pytest.raises(ValueError):
        assembly._canonical_bytes({"x": float("nan")})
