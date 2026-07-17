import hashlib
import inspect
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_live_readiness as readiness


SHA = "a" * 64


def _config():
    return SimpleNamespace(
        max_total_open_risk_pct=8.0,
        max_factor_group_open_risk_pct=2.0,
        max_combined_risk_pct=1.0,
        margin_entry_block_level=200.0,
    )


def _binding():
    return {name: SHA for name in readiness._CURRENT_REFIT_BINDING_FIELDS}


def _winner_ledger():
    return {
        "EURUSD": {
            "symbol": "EURUSD",
            "regime_configs": {
                "H1": {
                    "TREND": {
                        "strategy_name": "ExpertA",
                        "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
                        "artifact_meta": {"current_refit_binding": _binding()},
                    }
                }
            },
        }
    }


def _write_authority_manifest(root: Path, ledger_path: Path):
    artifacts = {}
    for name in readiness.SPARSE_AUTHORITY_ARTIFACTS:
        path = root / f"{name}.json"
        raw = json.dumps({"component": name}).encode()
        path.write_bytes(raw)
        artifacts[name] = {
            "relative_path": path.name,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
            "schema_version": readiness.SPARSE_AUTHORITY_ARTIFACT_SCHEMAS[name],
        }
    manifest = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_status": "PROMOTED",
        "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        "technical_failures": {},
        "authority_chain_receipt": {"type": "checksum-not-authority"},
        "authority_artifacts": artifacts,
    }
    path = root / f"{ledger_path.stem}.generation.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, manifest


def _authority_result(selected):
    possible = {
        f"EURUSD|{timeframe}|{regime}"
        for timeframe in ("M5", "M15", "M30", "H1", "H4", "D1")
        for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
    }
    unobserved = {"EURUSD|H1|CHOP"}
    return {
        "schema_version": readiness.SPARSE_AUTHORITY_RESULT_SCHEMA,
        "status": "FUNDED_AUTHORIZED",
        "errors": [],
        "raw_reverification": {
            name: True for name in readiness.SPARSE_AUTHORITY_RAW_REVERIFICATION
        },
        "selected_cell_ids": sorted(selected),
        "possible_slot_count": 24,
        "source_unavailable_cell_ids": [],
        "regime_unobserved_cell_ids": sorted(unobserved),
        "no_local_winner_cell_ids": sorted(possible - set(selected) - unobserved),
        "technical_cell_ids": [],
        "unsearched_cell_ids": [],
        "missing_observed_cell_ids": [],
        "native_activation_id": "activation-1",
        "native_activation_manifest_sha256": SHA,
        "ledger_sha256": None,
        "selected_capital_envelope": {
            "max_total_stop_risk_pct": 8.0,
            "max_factor_stop_risk_pct": 2.0,
            "max_same_symbol_stop_risk_pct": 1.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 20.0,
            "gap_stress_multiple": 2.0,
        },
        "capital_config_matches": True,
        "capital_execution_enforcement_reverified": True,
        "funded_live_authorized": True,
    }


def _verify_authority_result(tmp_path, monkeypatch, result):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    result["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(
            verify_live_readiness_authority_chain=lambda **_kwargs: result
        ),
    )
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    return readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells={"EURUSD|H1|TREND"},
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )


def test_sparse_physical_ledger_does_not_force_all_regimes_or_symbols():
    cells, errors = readiness._sparse_current_refit_projection(
        _winner_ledger(), ["EURUSD", "GBPUSD"]
    )
    assert cells == {"EURUSD|H1|TREND"}
    assert errors == []


def test_production_readiness_has_no_legacy_prelockbox_dependency():
    source = inspect.getsource(readiness.validate_live_readiness)
    assert "pm_two_phase_lockbox" not in source
    assert "prelockbox" not in source.lower()
    assert "independent_cell_ledger_incomplete" not in source


def test_retired_validator_does_not_force_physical_four_regime_grids():
    source = inspect.getsource(readiness._validate_legacy_live_readiness)
    assert "validate_native_exclusion_no_trade_grid" not in source
    assert "independent_cell_ledger_incomplete" not in source


def test_physical_ledger_rejects_no_trade_accounting_placeholders():
    ledger = _winner_ledger()
    ledger["EURUSD"]["regime_configs"]["H1"]["CHOP"] = {
        "strategy_name": "NO_TRADE",
        "promotion_status": "NO_TRADE",
        "artifact_meta": {"no_trade": True},
    }
    _cells, errors = readiness._sparse_current_refit_projection(
        ledger, ["EURUSD"]
    )
    assert "LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:EURUSD|H1|CHOP" in errors


@pytest.mark.parametrize(
    "sentinel",
    [
        "NO_LOCAL_WINNER",
        "NO_WINNER",
        "REGIME_UNOBSERVED",
        "SOURCE_UNAVAILABLE",
        "TECHNICAL_FAILURE",
        "UNSEARCHED",
    ],
)
def test_physical_ledger_never_promotes_accounting_sentinels(sentinel):
    ledger = _winner_ledger()
    ledger["EURUSD"]["regime_configs"]["H1"]["CHOP"] = {
        "strategy_name": sentinel,
        "promotion_status": "PROCESS_VALIDATED_CURRENT_REFIT",
        "artifact_meta": {"current_refit_binding": _binding()},
    }
    cells, errors = readiness._sparse_current_refit_projection(
        ledger, ["EURUSD"]
    )
    assert cells == {"EURUSD|H1|TREND"}
    assert "LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:EURUSD|H1|CHOP" in errors


def test_authority_index_rejects_parent_traversal(tmp_path):
    manifest = {
        "authority_artifacts": {
            name: {
                "relative_path": "../escape.json",
                "sha256": SHA,
                "size_bytes": 1,
                "schema_version": "x",
            }
            for name in readiness.SPARSE_AUTHORITY_ARTIFACTS
        }
    }
    paths, errors = readiness._resolve_sparse_authority_indexes(
        manifest, tmp_path / "pm_configs.generation.json"
    )
    assert paths == {}
    assert all(error.startswith("AUTHORITY_ARTIFACT_PATH_INVALID") for error in errors)


def test_missing_raw_current_refit_verifier_is_explicit_blocker(
    tmp_path, monkeypatch
):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    monkeypatch.setitem(sys.modules, "pm_optimization_authority", None)
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    result, errors = readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells={"EURUSD|H1|TREND"},
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert result is None
    assert "CURRENT_REFIT_RAW_VERIFIER_UNAVAILABLE" in errors


def test_self_asserted_receipts_cannot_replace_raw_reverification(
    tmp_path, monkeypatch
):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    selected = {"EURUSD|H1|TREND"}
    forged = _authority_result(selected)
    forged["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    forged["raw_reverification"]["current_refit_execution"] = False
    module = SimpleNamespace(
        verify_live_readiness_authority_chain=lambda **_kwargs: forged
    )
    monkeypatch.setitem(sys.modules, "pm_optimization_authority", module)
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    _result, errors = readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells=selected,
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert "LIVE_AUTHORITY_RAW_REVERIFICATION_INCOMPLETE" in errors
    assert "CURRENT_REFIT_RAW_EXECUTION_UNVERIFIED" in errors


def test_exact_raw_authority_result_can_clear_successor_gate(tmp_path, monkeypatch):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    selected = {"EURUSD|H1|TREND"}
    verified = _authority_result(selected)
    verified["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    module = SimpleNamespace(
        verify_live_readiness_authority_chain=lambda **_kwargs: verified
    )
    monkeypatch.setitem(sys.modules, "pm_optimization_authority", module)
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    result, errors = readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells=selected,
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert errors == []
    assert result["funded_live_authorized"] is True


def test_statistical_no_local_winner_cells_are_non_blocking(tmp_path, monkeypatch):
    result = _authority_result({"EURUSD|H1|TREND"})
    _verified, errors = _verify_authority_result(tmp_path, monkeypatch, result)
    assert result["no_local_winner_cell_ids"]
    assert errors == []


def test_sparse_slot_partition_rejects_omitted_cell(tmp_path, monkeypatch):
    result = _authority_result({"EURUSD|H1|TREND"})
    result["no_local_winner_cell_ids"].pop()
    _verified, errors = _verify_authority_result(tmp_path, monkeypatch, result)
    assert "LIVE_AUTHORITY_CELL_PARTITION_INCOMPLETE" in errors


def test_missing_observed_leaf_blocks_sparse_authority(tmp_path, monkeypatch):
    result = _authority_result({"EURUSD|H1|TREND"})
    missing = result["no_local_winner_cell_ids"].pop()
    result["missing_observed_cell_ids"] = [missing]
    _verified, errors = _verify_authority_result(tmp_path, monkeypatch, result)
    assert (
        "LIVE_AUTHORITY_BLOCKING_CELLS_PRESENT:missing_observed_cell_ids" in errors
    )


def test_sparse_slot_partition_rejects_overlap(tmp_path, monkeypatch):
    result = _authority_result({"EURUSD|H1|TREND"})
    result["no_local_winner_cell_ids"].append("EURUSD|H1|CHOP")
    result["no_local_winner_cell_ids"].sort()
    _verified, errors = _verify_authority_result(tmp_path, monkeypatch, result)
    assert any(
        error.startswith("LIVE_AUTHORITY_CELL_PARTITION_OVERLAP")
        for error in errors
    )


def test_sparse_slot_partition_rejects_forged_cell(tmp_path, monkeypatch):
    result = _authority_result({"EURUSD|H1|TREND"})
    result["no_local_winner_cell_ids"][-1] = "FAKE|D1|TREND"
    result["no_local_winner_cell_ids"].sort()
    _verified, errors = _verify_authority_result(tmp_path, monkeypatch, result)
    assert (
        "LIVE_AUTHORITY_CELL_OUTSIDE_POSSIBLE_BOOK:no_local_winner_cell_ids"
        in errors
    )
    assert "LIVE_AUTHORITY_CELL_PARTITION_INCOMPLETE" in errors


def test_blocking_book_states_cannot_be_hidden_by_a_pass_status(
    tmp_path, monkeypatch
):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    selected = {"EURUSD|H1|TREND"}
    forged = _authority_result(selected)
    forged["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    forged["technical_cell_ids"] = ["EURUSD|H1|RANGE"]
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(
            verify_live_readiness_authority_chain=lambda **_kwargs: forged
        ),
    )
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    _result, errors = readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells=selected,
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert "LIVE_AUTHORITY_BLOCKING_CELLS_PRESENT:technical_cell_ids" in errors


def test_selected_cap_cannot_pass_without_live_enforcement(tmp_path, monkeypatch):
    ledger = _winner_ledger()
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    _write_authority_manifest(tmp_path, ledger_path)
    selected = {"EURUSD|H1|TREND"}
    forged = _authority_result(selected)
    forged["ledger_sha256"] = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    forged["capital_execution_enforcement_reverified"] = False
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(
            verify_live_readiness_authority_chain=lambda **_kwargs: forged
        ),
    )
    native = SimpleNamespace(
        activation_id="activation-1", activation_manifest_sha256=SHA
    )
    _result, errors = readiness._verify_sparse_live_authority(
        config=_config(),
        config_path=str(tmp_path / "config.json"),
        config_payload={},
        ledger=ledger,
        ledger_path=ledger_path,
        requested_symbols=["EURUSD"],
        selected_cells=selected,
        native_universe=native,
        validation_time_utc="2026-07-14T00:00:00Z",
    )
    assert "LIVE_AUTHORITY_CAPITAL_EXECUTION_ENFORCEMENT_UNVERIFIED" in errors


def test_transient_runtime_findings_are_separate_from_evidence():
    report = readiness.ReadinessReport(
        False,
        [
            readiness.ReadinessFinding("error", "mt5_account_identity_unverified", "x", "runtime"),
            readiness.ReadinessFinding("error", "historical_book_invalid", "x", "evidence"),
        ],
    )
    assert [item.code for item in report.runtime_errors] == [
        "mt5_account_identity_unverified"
    ]
    assert [item.code for item in report.evidence_errors] == [
        "historical_book_invalid"
    ]
