import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from pm_core import PipelineConfig
from pm_native_activation import sha256_file
from pm_live_readiness import validate_live_readiness
from pm_main import LiveTrader
from pm_pipeline import PortfolioManager, RegimeConfig, SymbolConfig
from pm_promotion_authorization import (
    AUTHORIZATION_SCHEMA,
    authorization_payload_sha256,
    build_cell_contract,
    build_review_scope,
    canonical_sha256,
    cell_contract_sha256,
    verify_promotion_authorization,
)
from tests.native_v5_support import activate_v5_run, write_v5_run


def _activate_native(data_dir: Path, *, activation_id: str, close_value: float) -> dict:
    native_root = data_dir / "native"
    run_id = f"{activation_id}-run"
    write_v5_run(
        native_root,
        run_id,
        price_offset=float(close_value),
        production_grid=True,
    )
    activated = activate_v5_run(native_root, run_id, activation_id)
    assert activated["status"] == "active", activated
    assert activated["active_pointer_updated"]
    entry = activated["entries"]["EURUSD"]["H1"]
    return {
        "activation_id": activation_id,
        "activation_manifest_sha256": activated["manifest_sha256"],
        "bars_sha256": entry["bars_sha256"],
        "sidecar_sha256": entry["sidecar_sha256"],
        "entry": entry,
    }


def _winner(native: dict, now: datetime) -> tuple[RegimeConfig, SymbolConfig]:
    artifact_meta = {
        "artifact_version": "test-artifact-v1",
        "deployment_contract": {
            "contract": "funded_live_deployment_v1",
            "status": "PASS",
            "sha1": "a" * 40,
        },
        "strategy_surface_hash": "surface-v1",
    }
    cell = RegimeConfig(
        strategy_name="EMACrossoverStrategy",
        parameters={"fast_period": 12, "slow_period": 48},
        quality_score=0.8,
        promotion_status="PROMOTED",
        artifact_meta=artifact_meta,
        validation_evidence={"statistics": {"dsr": 0.99, "pbo": 0.1}},
        holdout_metrics={"policy_status": "PASS", "total_trades": 80},
        governance_policy={"policy_name": "control_fixed"},
        trained_at=now - timedelta(days=2),
        valid_until=now + timedelta(days=20),
    )
    symbol = SymbolConfig(
        symbol="EURUSD",
        regime_configs={"H1": {"TREND": cell}},
        is_validated=True,
        optimized_at=now - timedelta(days=2),
        valid_until=now + timedelta(days=20),
        artifact_meta=artifact_meta,
        data_window_meta={
            "evidence_contract_version": "calendar-lockbox-v2",
            "data_window_fingerprint": "b" * 40,
            "timeframe_sources": {
                "H1": {
                    "source_kind": "broker_native",
                    "provenance_valid": True,
                    "sha256": native["bars_sha256"],
                    "activation_id": native["activation_id"],
                    "activation_manifest_sha256": native["activation_manifest_sha256"],
                    "activation_entry": native["entry"],
                }
            },
            "timeframe_evidence": {
                "H1": {
                    "passed": True,
                    "contract_version": "calendar-lockbox-v2",
                    "source_kind": "broker_native",
                    "source_sha256": native["bars_sha256"],
                }
            },
        },
    )
    return cell, symbol


def _write_ledger(path: Path, symbol: SymbolConfig) -> str:
    path.write_text(
        json.dumps({"EURUSD": symbol.to_dict()}, indent=2), encoding="utf-8"
    )
    return sha256_file(path)


def _write_authorization(
    path: Path,
    *,
    ledger_sha: str,
    contract: dict,
    now: datetime,
    review_overrides: dict | None = None,
) -> str:
    scope = build_review_scope(ledger_sha, [contract])
    review = {
        "status": "APPROVED",
        "method": "HUMAN",
        "prepared_by": "promotion-preparer-01",
        "reviewer_id": "risk-committee-reviewer-01",
        "reviewed_at": (now - timedelta(minutes=10)).isoformat(),
        "decision_reference": "RC-2026-001",
        "scope_sha256": canonical_sha256(scope),
    }
    review.update(review_overrides or {})
    payload = {
        "schema_version": AUTHORIZATION_SCHEMA,
        "authorization_id": "AUTH-2026-001",
        "authorization_status": "APPROVED",
        "issued_at": (now - timedelta(minutes=5)).isoformat(),
        "expires_at": (now + timedelta(days=7)).isoformat(),
        "winner_ledger_sha256": ledger_sha,
        "review": review,
        "cells": [
            {
                "cell_id": contract["cell_id"],
                "decision": "APPROVE",
                "contract_sha256": cell_contract_sha256(contract),
                "contract": contract,
            }
        ],
    }
    payload["payload_sha256"] = authorization_payload_sha256(payload)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return sha256_file(path)


def _authorized_fixture(tmp_path: Path):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    data_dir = tmp_path / "data"
    native = _activate_native(data_dir, activation_id="activation-a", close_value=1.1)
    cell, symbol = _winner(native, now)
    ledger = tmp_path / "pm_configs.json"
    ledger_sha = _write_ledger(ledger, symbol)
    contract = build_cell_contract("EURUSD", "H1", "TREND", cell, symbol)
    authorization = tmp_path / "promotion_authorization.json"
    authorization_sha = _write_authorization(
        authorization,
        ledger_sha=ledger_sha,
        contract=contract,
        now=now,
    )
    config = PipelineConfig(
        data_dir=data_dir,
        production_require_promotion_gate=True,
        production_promotion_gate_mode="immutable_exact",
        production_promotion_authorization_path=str(authorization),
        production_promotion_authorization_sha256=authorization_sha,
        production_promotion_authorization_max_age_days=31,
        live_allowed_promotion_statuses=["PROMOTED"],
    )
    return now, config, ledger, cell, symbol, authorization


def test_exact_immutable_authorization_accepts_matching_cell(tmp_path):
    now, config, ledger, _cell, symbol, _authorization = _authorized_fixture(tmp_path)

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert result.valid, result.errors
    assert result.authorizes("eurusd", "h1", "trend")
    assert not result.authorizes("EURUSD", "H1", "CHOP")


def test_relative_native_data_dir_resolves_from_selected_config(tmp_path, monkeypatch):
    now, config, ledger, _cell, symbol, _authorization = _authorized_fixture(tmp_path)
    config.data_dir = Path("data")
    elsewhere = tmp_path / "unrelated-working-directory"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        config_path=tmp_path / "config.json",
        now=now,
    )

    assert result.valid, result.errors


def test_strategy_parameter_drift_blocks_authorized_cell(tmp_path):
    now, config, ledger, cell, symbol, _authorization = _authorized_fixture(tmp_path)
    cell.parameters["fast_period"] = 13

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert not result.valid
    assert any(
        "CELL_CONTRACT_DRIFT:EURUSD|H1|TREND" in reason for reason in result.errors
    )


def test_authorization_file_tampering_breaks_configured_hash_pin(tmp_path):
    now, config, ledger, _cell, symbol, authorization = _authorized_fixture(tmp_path)
    payload = json.loads(authorization.read_text(encoding="utf-8"))
    payload["authorization_id"] = "AUTH-TAMPERED"
    authorization.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert not result.valid
    assert "PROMOTION_AUTHORIZATION_FILE_SHA256_MISMATCH" in result.errors
    assert "PROMOTION_AUTHORIZATION_PAYLOAD_SHA256_MISMATCH" in result.errors


def test_winner_ledger_byte_drift_blocks_authorization(tmp_path):
    now, config, ledger, _cell, symbol, _authorization = _authorized_fixture(tmp_path)
    ledger.write_text(ledger.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert not result.valid
    assert "PROMOTION_WINNER_LEDGER_SHA256_MISMATCH" in result.errors
    assert "PROMOTION_REVIEW_SCOPE_SHA256_MISMATCH" in result.errors


def test_active_native_activation_drift_blocks_authorization(tmp_path):
    now, config, ledger, _cell, symbol, _authorization = _authorized_fixture(tmp_path)
    _activate_native(
        Path(config.data_dir), activation_id="activation-b", close_value=1.2
    )

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert not result.valid
    assert any(
        "ACTIVE_NATIVE_DRIFT:EURUSD|H1|TREND" in reason for reason in result.errors
    )


def test_missing_or_nonapproved_human_review_blocks(tmp_path):
    now, config, ledger, cell, symbol, authorization = _authorized_fixture(tmp_path)
    ledger_sha = sha256_file(ledger)
    contract = build_cell_contract("EURUSD", "H1", "TREND", cell, symbol)
    config.production_promotion_authorization_sha256 = _write_authorization(
        authorization,
        ledger_sha=ledger_sha,
        contract=contract,
        now=now,
        review_overrides={"status": "PENDING", "reviewer_id": ""},
    )

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now,
    )

    assert not result.valid
    assert "PROMOTION_REVIEW_STATUS_NOT_APPROVED" in result.errors
    assert "PROMOTION_REVIEWER_ID_MISSING" in result.errors


def test_expired_authorization_blocks(tmp_path):
    now, config, ledger, _cell, symbol, _authorization = _authorized_fixture(tmp_path)

    result = verify_promotion_authorization(
        config,
        {"EURUSD": symbol},
        ledger_path=ledger,
        now=now + timedelta(days=8),
    )

    assert not result.valid
    assert "PROMOTION_AUTHORIZATION_EXPIRED" in result.errors


def test_portfolio_manager_rechecks_exact_cell_authorization(tmp_path):
    _now, config, ledger, _cell, _symbol, authorization = _authorized_fixture(tmp_path)
    manager = PortfolioManager(config, ["EURUSD"], config_file=str(ledger))

    authorized, reason = manager.is_exact_cell_authorized_for_live(
        "EURUSD", "H1", "TREND", refresh=True
    )
    assert authorized, reason

    authorization.write_text(
        authorization.read_text(encoding="utf-8") + "\n", encoding="utf-8"
    )
    authorized, reason = manager.is_exact_cell_authorized_for_live(
        "EURUSD", "H1", "TREND", refresh=True
    )
    assert not authorized
    assert "FILE_SHA256_MISMATCH" in reason


def test_enabled_runtime_rejects_mutated_gate_mode_and_disabled_research_bypasses():
    config = SimpleNamespace(
        production_require_promotion_gate=True,
        production_promotion_gate_mode="ledger_status",
        _funded_live_execution=False,
    )
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config = config

    authorized, reason = manager.is_exact_cell_authorized_for_live(
        "EURUSD", "H1", "TREND", refresh=True
    )

    assert not authorized
    assert "requires literal immutable_exact" in reason

    config.production_require_promotion_gate = False
    authorized, reason = manager.is_exact_cell_authorized_for_live(
        "EURUSD", "H1", "TREND", refresh=True
    )
    assert authorized
    assert reason == "promotion gate disabled for research"


def test_funded_startup_eligibility_cannot_disable_promotion_gate():
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config = SimpleNamespace(
        production_require_promotion_gate=False,
        production_promotion_gate_mode="immutable_exact",
        _funded_live_execution=True,
        live_artifact_drift_policy="block",
        live_config_expiry_grace_minutes=0,
    )
    manager.ledger = SimpleNamespace(
        get_all_configs=lambda: {"EURUSD": object()}
    )

    assert manager.get_live_eligible_configs(current_artifact_meta={}) == {}
    assert manager._last_live_eligibility_rejections == {
        "EURUSD": "funded execution cannot disable the promotion gate"
    }


def test_final_send_boundary_rechecks_exact_cell_with_refresh():
    trader = LiveTrader.__new__(LiveTrader)
    trader.enable_trading = True
    trader.pipeline_config = SimpleNamespace(
        production_require_promotion_gate=True,
        production_promotion_gate_mode="immutable_exact",
        _funded_live_execution=True,
    )
    checker = MagicMock(return_value=(True, "authorized"))
    trader.pm = SimpleNamespace(is_exact_cell_authorized_for_live=checker)

    reason = trader._promotion_authorization_block_reason("EURUSD", "H1", "TREND")

    assert reason is None
    checker.assert_called_once_with("EURUSD", "H1", "TREND", refresh=True)


def test_final_send_boundary_fails_closed_when_checker_is_missing():
    trader = LiveTrader.__new__(LiveTrader)
    trader.enable_trading = True
    trader.pipeline_config = SimpleNamespace(
        production_require_promotion_gate=True,
        production_promotion_gate_mode="immutable_exact",
        _funded_live_execution=True,
    )
    trader.pm = SimpleNamespace()

    reason = trader._promotion_authorization_block_reason("EURUSD", "H1", "TREND")

    assert "checker unavailable" in reason


def test_funded_readiness_reports_missing_hash_pinned_authorization(tmp_path):
    _now, config, ledger, _cell, _symbol, _authorization = _authorized_fixture(tmp_path)
    config.production_promotion_authorization_path = ""
    config.production_promotion_authorization_sha256 = ""
    config.winner_ledger_path = str(ledger)
    config.regime_params_file = ""

    report = validate_live_readiness(
        config,
        symbols=["EURUSD"],
        config_path=str(tmp_path / "config.json"),
    )

    assert not report.ok
    assert any(
        finding.code == "production_promotion_authorization_invalid"
        and "PROMOTION_AUTHORIZATION_PATH_MISSING" in finding.message
        for finding in report.errors
    )
