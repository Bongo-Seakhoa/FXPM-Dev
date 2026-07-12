import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_core import PipelineConfig
from pm_portfolio_replay import seal_portfolio_replay_evidence
from pm_promotion_authorization import (
    build_cell_contract,
    sha256_file,
    verify_promotion_authorization,
)
from pm_two_phase_lockbox import (
    REQUIRED_REGIMES,
    REQUIRED_TIMEFRAMES,
    build_prelockbox_roster_artifact,
    materialize_joint_pass_ledger,
    validate_released_ledger_against_prelockbox,
)
from tools.issue_promotion_authorization import (
    AuthoringFailure,
    VerifiedAuthoringContext,
    _tradeable_contracts,
    _validate_final_pass_shape,
    approve_authorization_draft,
    build_authorization_draft,
)


def _symbol_and_contract(now: datetime):
    artifact = {
        "artifact_version": "artifact-v1",
        "deployment_contract": {
            "contract": "funded_live_deployment_v1",
            "status": "PASS",
            "sha1": "a" * 40,
        },
    }
    cell = {
        "strategy_name": "Momentum",
        "parameters": {"period": 12},
        "promotion_status": "PROMOTED",
        "artifact_meta": artifact,
        "validation_evidence": {"statistics": {"dsr": 0.99}},
        "holdout_metrics": {"policy_status": "PASS"},
        "governance_policy": {"name": "fixed"},
        "trained_at": (now - timedelta(days=2)).isoformat(),
        "valid_until": (now + timedelta(days=40)).isoformat(),
    }
    source = {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "sha256": "b" * 64,
        "activation_id": "activation-1",
        "activation_manifest_sha256": "c" * 64,
        "activation_entry": {"sidecar_sha256": "d" * 64},
    }
    symbol = {
        "symbol": "EURUSD",
        "artifact_meta": artifact,
        "valid_until": (now + timedelta(days=40)).isoformat(),
        "data_window_meta": {
            "data_window_fingerprint": "e" * 40,
            "evidence_contract_version": "calendar-lockbox-v2",
            "timeframe_sources": {"H1": source},
            "timeframe_evidence": {
                "H1": {
                    "passed": True,
                    "source_sha256": "b" * 64,
                }
            },
        },
        "regime_configs": {"H1": {"TREND": cell}},
    }
    contract = build_cell_contract("EURUSD", "H1", "TREND", cell, symbol)
    return symbol, contract


def _context(tmp_path: Path, now: datetime, *, max_age_days: int = 31):
    symbol, contract = _symbol_and_contract(now)
    ledger = tmp_path / "released.json"
    ledger.write_text(json.dumps({"EURUSD": symbol}, indent=2), encoding="utf-8")
    return VerifiedAuthoringContext(
        config_path=tmp_path / "config.json",
        config_payload={"symbols": ["EURUSD"], "pipeline": {}},
        config=PipelineConfig(),
        ledger_path=ledger,
        ledger={"EURUSD": symbol},
        ledger_sha256=sha256_file(ledger),
        contracts=(contract,),
        authoring_chain={
            "schema_version": "fxpm-funded-promotion-authoring-chain-v1",
            "final_portfolio_evidence_file_sha256": "f" * 64,
            "release_scope": "ALL_FROZEN_TRADEABLE_CELLS",
        },
        max_age_days=max_age_days,
        earliest_cell_expiry_utc=now + timedelta(days=40),
    )


def test_controlled_approval_round_trip_is_runtime_authoritative(tmp_path):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now)
    draft = build_authorization_draft(
        context, prepared_by="promotion-preparer-01", now=now
    )

    approved = approve_authorization_draft(
        draft,
        context,
        review_method="CONTROLLED",
        reviewer_id="promotion-preparer-01",
        decision_reference="AUTO-CONTROL-2026-0711",
        control_id="fxpm-joint-pass-control",
        control_version="v1.0.0",
        expires_at=(now + timedelta(days=7)).isoformat(),
        now=now,
    )
    authorization = tmp_path / "authorization.json"
    authorization.write_text(json.dumps(approved, indent=2), encoding="utf-8")
    config = SimpleNamespace(
        production_promotion_authorization_path=str(authorization),
        production_promotion_authorization_sha256=sha256_file(authorization),
        production_promotion_authorization_max_age_days=31,
        live_allowed_promotion_statuses=["PROMOTED"],
        data_dir=tmp_path / "data",
    )

    result = verify_promotion_authorization(
        config,
        context.ledger,
        ledger_path=context.ledger_path,
        now=now,
        verify_active_native=False,
    )

    assert result.valid, result.errors
    assert result.authorizes("EURUSD", "H1", "TREND")
    assert approved["review"]["control_id"] == "fxpm-joint-pass-control"


def test_runtime_rejects_approval_validity_longer_than_configured_max_age(tmp_path):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now, max_age_days=90)
    draft = build_authorization_draft(context, prepared_by="preparer", now=now)
    approved = approve_authorization_draft(
        draft,
        context,
        review_method="HUMAN",
        reviewer_id="independent-reviewer",
        decision_reference="RC-LONG-1",
        expires_at=(now + timedelta(days=32)).isoformat(),
        now=now,
    )
    authorization = tmp_path / "long-authorization.json"
    authorization.write_text(json.dumps(approved, indent=2), encoding="utf-8")
    config = SimpleNamespace(
        production_promotion_authorization_path=str(authorization),
        production_promotion_authorization_sha256=sha256_file(authorization),
        production_promotion_authorization_max_age_days=31,
        live_allowed_promotion_statuses=["PROMOTED"],
        data_dir=tmp_path / "data",
    )

    result = verify_promotion_authorization(
        config,
        context.ledger,
        ledger_path=context.ledger_path,
        now=now,
        verify_active_native=False,
    )

    assert not result.valid
    assert "PROMOTION_AUTHORIZATION_EXPIRY_EXCEEDS_MAX_AGE" in result.errors


def test_tampered_draft_cannot_be_approved(tmp_path):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now)
    draft = build_authorization_draft(context, prepared_by="preparer", now=now)
    draft["cells"][0]["contract"]["strategy_name"] = "Tampered"

    with pytest.raises(AuthoringFailure) as caught:
        approve_authorization_draft(
            draft,
            context,
            review_method="CONTROLLED",
            reviewer_id="control-runner",
            decision_reference="CONTROL-1",
            control_id="control",
            control_version="v1",
            expires_at=(now + timedelta(days=1)).isoformat(),
            now=now,
        )

    assert "PROMOTION_DRAFT_SEAL_INVALID" in caught.value.errors
    assert "PROMOTION_DRAFT_CELLS_STALE_OR_TAMPERED" in caught.value.errors


def test_missing_final_pass_is_rejected_even_when_evidence_is_sealed():
    evidence = seal_portfolio_replay_evidence(
        {
            "verdict": "FAIL",
            "failures": ["drawdown"],
            "replay": {
                "window": {"partition": "final_lockbox"},
                "partial_run": False,
                "allow_cell_errors": False,
            },
        }
    )

    errors = _validate_final_pass_shape(evidence)

    assert "FINAL_PORTFOLIO_VERDICT_NOT_PASS" in errors
    assert "FINAL_PORTFOLIO_FAILURES_PRESENT" in errors


def test_draft_file_is_rejected_by_runtime_authorization_verifier(tmp_path):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now)
    draft = build_authorization_draft(context, prepared_by="preparer", now=now)
    draft_path = tmp_path / "draft.json"
    draft_path.write_text(json.dumps(draft, indent=2), encoding="utf-8")
    config = SimpleNamespace(
        production_promotion_authorization_path=str(draft_path),
        production_promotion_authorization_sha256=sha256_file(draft_path),
        production_promotion_authorization_max_age_days=31,
        live_allowed_promotion_statuses=["PROMOTED"],
        data_dir=tmp_path / "data",
    )

    result = verify_promotion_authorization(
        config,
        context.ledger,
        ledger_path=context.ledger_path,
        now=now,
        verify_active_native=False,
    )

    assert not result.valid
    assert "PROMOTION_AUTHORIZATION_SCHEMA_INVALID" in result.errors
    assert "PROMOTION_AUTHORIZATION_STATUS_NOT_APPROVED" in result.errors


def test_human_approval_requires_case_insensitive_separation_of_duties(tmp_path):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now)
    draft = build_authorization_draft(context, prepared_by="Risk.User", now=now)

    with pytest.raises(AuthoringFailure) as caught:
        approve_authorization_draft(
            draft,
            context,
            review_method="HUMAN",
            reviewer_id="risk.user",
            decision_reference="RC-1",
            expires_at=(now + timedelta(days=1)).isoformat(),
            now=now,
        )

    assert caught.value.errors == (
        "PROMOTION_REVIEW_SEPARATION_OF_DUTIES_FAILED",
    )


@pytest.mark.parametrize(
    ("context_max_age", "expiry", "reason"),
    [
        (31, "2026-07-12T12:00:00", "PROMOTION_EXPIRES_AT_INVALID"),
        (7, "2026-07-19T12:00:00Z", "PROMOTION_AUTHORIZATION_EXPIRY_EXCEEDS_MAX_AGE"),
        (90, "2026-08-21T12:00:00Z", "PROMOTION_AUTHORIZATION_OUTLIVES_CELL"),
    ],
)
def test_approval_expiry_is_aware_bounded_and_no_later_than_cell(
    tmp_path, context_max_age, expiry, reason
):
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    context = _context(tmp_path, now, max_age_days=context_max_age)
    draft = build_authorization_draft(context, prepared_by="preparer", now=now)

    with pytest.raises(AuthoringFailure) as caught:
        approve_authorization_draft(
            draft,
            context,
            review_method="HUMAN",
            reviewer_id="independent-reviewer",
            decision_reference="RC-2",
            expires_at=expiry,
            now=now,
        )

    assert reason in caught.value.errors


def _frozen_symbol(symbol: str, *, selected: bool) -> dict:
    grid = {}
    for timeframe in REQUIRED_TIMEFRAMES:
        grid[timeframe] = {}
        for regime in REQUIRED_REGIMES:
            winner = selected and timeframe == "H1" and regime == "TREND"
            grid[timeframe][regime] = {
                "strategy_name": "Momentum" if winner else "NO_TRADE",
                "parameters": {"period": 12} if winner else {},
                "promotion_status": "FROZEN_PRELOCKBOX" if winner else "NO_TRADE",
                "promotion_reasons": [],
                "holdout_metrics": {},
                "validation_evidence": (
                    {
                        "temporal_lineage": {
                            "parameter_selection_cutoff_utc": "2024-01-01T00:00:00Z",
                            "performance_evidenced_through_utc": "2024-01-01T00:00:00Z",
                        }
                    }
                    if winner
                    else {}
                ),
                "artifact_meta": {"no_trade": not winner},
            }
    payload = {
        "symbol": symbol,
        "holdout_metrics": {},
        "edge_challenger_book": {"regime_configs": {}},
        "data_window_meta": {
            "windows": {
                "final_lockbox": {
                    "start_ts": "2024-01-08T00:00:00Z",
                    "end_ts": "2026-01-08T00:00:00Z",
                }
            }
        },
        "regime_configs": grid,
    }
    if not selected:
        payload["artifact_meta"] = {
            "native_exclusion_binding": {
                "status": "SOURCE_EXCLUDED_NO_TRADE",
                "reason_codes": ["no_history"],
            }
        }
    return payload


def test_excluded_no_trade_symbol_is_preserved_but_partial_release_is_rejected():
    frozen = {
        "EURUSD": _frozen_symbol("EURUSD", selected=True),
        "USDJPY": _frozen_symbol("USDJPY", selected=False),
    }
    artifact = build_prelockbox_roster_artifact(
        ledger=frozen,
        symbols=["EURUSD", "USDJPY"],
        lineage={},
    )
    released = materialize_joint_pass_ledger(
        artifact, portfolio_evidence_sha256="f" * 64
    )

    assert (
        validate_released_ledger_against_prelockbox(
            released,
            artifact,
            expected_portfolio_evidence_sha256="f" * 64,
        )
        == []
    )
    contracts = _tradeable_contracts(released, ["PROMOTED"])
    assert [row["cell_id"] for row in contracts] == ["EURUSD|H1|TREND"]
    assert all(
        cell["promotion_status"] == "NO_TRADE"
        for regimes in released["USDJPY"]["regime_configs"].values()
        for cell in regimes.values()
    )

    partial = copy.deepcopy(released)
    del partial["USDJPY"]
    errors = validate_released_ledger_against_prelockbox(
        partial,
        artifact,
        expected_portfolio_evidence_sha256="f" * 64,
    )
    assert "RELEASED_SYMBOL_MISSING:USDJPY" in errors
