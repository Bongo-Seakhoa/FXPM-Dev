"""Synthetic negative integration witnesses; no market or qualification authority.

The real default runner and real pipeline handler must refuse the unadopted
interval policy before search, and must preserve an existing ledger generation.
Only upstream configuration/native-universe labels are invented.
"""

from contextlib import contextmanager

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_book_multiplicity as book
import pm_live_readiness as readiness
import pm_pipeline as pipeline
import pm_production_capital_bootstrap as capital
import pm_production_optimization_runner as runner
import test_continuous_retraining_generation as generation_fixture
import test_production_optimization_runner as runner_fixture

for module in (runner, readiness):
    assert Path(module.__file__).resolve() == PRODUCT_ROOT / Path(module.__file__).name
assert Path(pipeline.__file__).resolve() == REPO / "pm_pipeline.py"
assert runner_fixture.runner is runner
assert generation_fixture.pm_pipeline is pipeline


@contextmanager
def _forbid_real_mt5_context():
    try:
        import MetaTrader5 as mt5
    except ImportError:
        yield
        return
    with pytest.MonkeyPatch.context() as patch:
        guards = []
        for name in ("initialize", "login", "order_send", "positions_get", "account_info",
                     "copy_rates_from_pos", "copy_rates_range", "history_deals_get"):
            guard = Mock(side_effect=AssertionError(f"Real MT5 call forbidden: {name}"))
            patch.setattr(mt5, name, guard, raising=False)
            guards.append(guard)
        yield
        for guard in guards:
            guard.assert_not_called()


@pytest.fixture(scope="module", autouse=True)
def forbid_real_mt5():
    with _forbid_real_mt5_context():
        yield


@pytest.fixture(autouse=True)
def _forbid_real_mt5_per_test(block_real_mt5_terminal):
    with _forbid_real_mt5_context():
        yield


def _request(tmp_path, *, scope=capital.DEMO_PROBATION_SCOPE, generation="synthetic-interval-default-refusal"):
    request = runner_fixture._request(tmp_path, generation_id=generation)
    request.config._funded_live_execution = scope == capital.FUNDED_LIVE_SCOPE
    return replace(request, authorization_scope=scope)


def _forbid_later_stages(monkeypatch):
    guards = []
    for owner, name in (
        (runner, "_effective_spool_limits"),
        (runner.coordinator, "derive_production_environment_authority"),
        (runner.coordinator, "coordinate_production_authority"),
        (runner.capital_bootstrap, "bootstrap_production_capital_authority"),
    ):
        guard = Mock(side_effect=AssertionError(f"Retired later stage reached: {name}"))
        monkeypatch.setattr(owner, name, guard)
        guards.append(guard)
    return guards


def _assert_refusal(error, scope):
    code, message = readiness.interval_cell_qualification_readiness_finding()
    assert error.code == "INTERVAL_CELL_QUALIFICATION_POLICY_UNADOPTED"
    assert error.details == (code, message, str(scope))
    assert error.interval_disposition is None


@pytest.mark.parametrize("scope", [capital.DEMO_PROBATION_SCOPE, capital.FUNDED_LIVE_SCOPE])
def test_default_production_refuses_before_search_or_owned_path_creation(tmp_path, monkeypatch, scope):
    request = _request(tmp_path, scope=scope)
    config_before = request.active_config_path.read_bytes()
    guards = _forbid_later_stages(monkeypatch)
    with pytest.raises(runner.ProductionOptimizationNotPromotable) as caught:
        runner.run_production_optimization(request)
    _assert_refusal(caught.value, scope)
    for guard in guards:
        guard.assert_not_called()
    digest = hashlib.sha256(request.generation_id.encode("utf-8")).hexdigest()
    assert not (tmp_path / "fxpm-production-runs").exists()
    assert not (tmp_path / f"fxpm-authority-{digest}").exists()
    assert request.active_config_path.read_bytes() == config_before
    assert sorted(path.name for path in tmp_path.iterdir()) == ["config.json", "data"]


def test_default_policy_refusal_precedes_an_existing_target_and_never_cleans_it(tmp_path, monkeypatch):
    request = _request(tmp_path)
    digest = hashlib.sha256(request.generation_id.encode("utf-8")).hexdigest()
    target = tmp_path / f"fxpm-authority-{digest}"
    target.mkdir()
    marker = target / "operator-owned-existing-proof.txt"
    marker.write_text("SYNTHETIC PRIOR TARGET; NOT AUTHORITY", encoding="utf-8")
    before = marker.read_bytes()
    guards = _forbid_later_stages(monkeypatch)
    with pytest.raises(runner.ProductionOptimizationNotPromotable) as caught:
        runner.run_production_optimization(request)
    _assert_refusal(caught.value, request.authorization_scope)
    assert marker.read_bytes() == before
    assert not (tmp_path / "fxpm-production-runs").exists()
    for guard in guards:
        guard.assert_not_called()


def test_malformed_request_remains_technical_before_the_policy_marker(monkeypatch):
    marker = Mock(side_effect=AssertionError("Policy marker must not mask malformed request"))
    monkeypatch.setattr(readiness, "interval_cell_qualification_readiness_finding", marker)
    with pytest.raises(runner.ProductionOptimizationRunnerError) as caught:
        runner.run_production_optimization(object())
    assert not isinstance(caught.value, runner.ProductionOptimizationNotPromotable)
    assert caught.value.code == "PRODUCTION_OPTIMIZATION_REQUEST_INVALID"
    marker.assert_not_called()


def test_declared_account_dispatch_keeps_technical_precedence_over_default_marker(tmp_path, monkeypatch):
    request = replace(_request(tmp_path), declared_account_inputs=object())
    marker = Mock(side_effect=AssertionError("Default marker must not shadow typed account dispatch"))
    monkeypatch.setattr(readiness, "interval_cell_qualification_readiness_finding", marker)
    with pytest.raises(runner.ProductionOptimizationRunnerError) as caught:
        runner.run_production_optimization(request)
    assert not isinstance(caught.value, runner.ProductionOptimizationNotPromotable)
    assert caught.value.code == "DECLARED_CELL_ADMISSION_INPUTS_INVALID"
    marker.assert_not_called()
    assert not (tmp_path / "fxpm-production-runs").exists()


def test_real_pipeline_persists_typed_refusal_without_replacing_last_good_generation(tmp_path, monkeypatch):
    config = generation_fixture._config(tmp_path)
    config.production_split_mode = "calendar_lockbox_v2"
    config.timeframes = list(book.TIMEFRAME_ORDER)
    config._funded_live_execution = False
    ledger = tmp_path / "pm_configs.json"
    manager = pipeline.PortfolioManager(config, list(book.PINNED_SYMBOL_ORDER), config_file=str(ledger))
    first_symbol = book.PINNED_SYMBOL_ORDER[0]
    assert manager.ledger.update_symbol(first_symbol, generation_fixture._winner(
        first_symbol, config, "SYNTHETIC_OLD_NOT_AUTHORITY", expired=True))
    manager._load_configs()
    governor = Mock(side_effect=AssertionError("Legacy search governor must not start"))
    monkeypatch.setattr(pipeline, "MemoryGovernor", governor)
    replay = Mock(side_effect=AssertionError("No replay before policy adoption"))
    monkeypatch.setattr(manager, "_run_production_replay_audit", replay)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "pipeline": {"timeframes": list(book.TIMEFRAME_ORDER)},
    }), encoding="utf-8")
    config._deployment_config_path = str(config_path)
    universe = SimpleNamespace(
        activation_id="SYNTHETIC-NATIVE-LABEL-NOT-AUTHORITY",
        activation_manifest_sha256="b" * 64,
    )
    monkeypatch.setattr(manager, "_native_universe_for_optimization", lambda: universe)
    stage_path, stage_manifest, active_manifest = manager._optimization_generation_paths()
    active_manifest.write_text(json.dumps({
        "schema_version": "fxpm.optimization-generation.v2",
        "fixture_kind": "SYNTHETIC LAST-GOOD BYTE PRESERVATION ONLY; NOT AUTHORITY",
        "generation_id": "synthetic-prior-generation",
        "generation_status": "PROMOTED",
        "authority_chain_receipt": {"native_activation_id": "synthetic-prior"},
    }), encoding="utf-8")
    before_ledger, before_manifest = ledger.read_bytes(), active_manifest.read_bytes()
    before_configs = manager.symbol_configs.copy()
    actual_runner, calls = runner.run_production_optimization, []

    def observe_actual_runner(request):
        calls.append(request)
        return actual_runner(request)

    monkeypatch.setattr(runner, "run_production_optimization", observe_actual_runner)
    guards = _forbid_later_stages(monkeypatch)
    generation = hashlib.sha256(b"SYNTHETIC-INTERVAL-PRESEARCH-PIPELINE").hexdigest()
    result = manager._run_production_authority_generation(pipeline.OptimizationGenerationRequest(
        kind="INITIAL", target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        overwrite_targets=True, supervised_generation_id=generation,
    ))
    assert len(calls) == 1 and type(calls[0]) is runner.ProductionOptimizationRequest
    assert calls[0].declared_account_inputs is None and calls[0].generation_id == generation
    assert result.status == "NOT_PROMOTABLE" and result.technical_failures == {}
    assert not result.promoted and not result.ledger_changed
    assert result.base_ledger_sha256 == hashlib.sha256(before_ledger).hexdigest()
    assert result.promoted_ledger_sha256 == ""
    assert result.authority_chain_receipt == {} and result.authority_artifacts == {}
    assert ledger.read_bytes() == before_ledger and active_manifest.read_bytes() == before_manifest
    assert manager.symbol_configs == before_configs
    assert not stage_path.exists() and not stage_manifest.exists()
    failure_path = manager._optimization_failure_manifest_path()
    assert Path(result.manifest_path) == failure_path
    diagnostic = json.loads(failure_path.read_text(encoding="utf-8"))
    code, message = readiness.interval_cell_qualification_readiness_finding()
    expected = {"code": "INTERVAL_CELL_QUALIFICATION_POLICY_UNADOPTED",
                "details": [code, message, capital.DEMO_PROBATION_SCOPE],
                "preserved_last_good_ledger": True,
                "confers_publication_or_trading_authority": False}
    assert diagnostic["generation_status"] == "NOT_PROMOTABLE"
    assert diagnostic["non_promotable"] == result.non_promotable == expected
    assert diagnostic["base_ledger_sha256"] == hashlib.sha256(before_ledger).hexdigest()
    assert diagnostic["base_active_manifest_sha256"] == hashlib.sha256(before_manifest).hexdigest()
    assert diagnostic["technical_failures"] == {}
    assert not (tmp_path / "fxpm-production-runs").exists()
    digest = hashlib.sha256(generation.encode("utf-8")).hexdigest()
    assert not (tmp_path / f"fxpm-authority-{digest}").exists()
    for guard in guards:
        guard.assert_not_called()
    governor.assert_not_called()
    replay.assert_not_called()
