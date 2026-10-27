"""Selected-source interval admission on one synthetic account and real readers.

The reused fixture invents finite raw/native/spec authentication and sparse :01
bars. It proves engineering joins, not authentic OWNER evidence or profitability.
"""

from contextlib import contextmanager

from dataclasses import FrozenInstanceError, replace
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

import pm_causal_evidence_producer as causal
import pm_forward_oos_replay_producer as forward
import pm_production_authority_coordinator as coordinator
import pm_production_optimization_runner as runner


for module in (causal, coordinator, runner):
    assert Path(module.__file__).resolve() == PRODUCT_ROOT / Path(module.__file__).name
assert coordinator.ProductionAuthorityNotQualified.__module__ == coordinator.__name__
assert runner.causal_evidence is causal and runner.coordinator is coordinator

import prototype_support as carrier
assert carrier.causal is causal and carrier.forward is forward


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


@pytest.fixture(scope="module")
def complete_account(tmp_path_factory, forbid_real_mt5):
    with pytest.MonkeyPatch.context() as patch:
        env = carrier._synthetic_reopened_declared_prefix(
            tmp_path_factory.mktemp("synthetic-interval-complete"), patch, "v16"
        )
        real_run, calls = forward._run_declared_cell_account, []

        def one_synthetic_account(**kwargs):
            assert not calls, "An interval disposition must not run another account"
            calls.append(kwargs)
            return real_run(**kwargs)

        patch.setattr(forward, "_run_declared_cell_account", one_synthetic_account)
        artifact = forward.produce_declared_cell_account_artifact(
            env.context, precommit=env.precommit, process=env.process,
            frozen_pipeline_config=env.frozen, execution_product=env.products["H1"],
            spread_source_product=env.products["M5"], frozen_profile_path=env.profile_path,
        )
        projection = causal.read_declared_cell_account_artifact(
            env.context, artifact, process=env.process, frozen_pipeline_config=env.frozen,
        )
        assert projection.progress_status == "COMPLETE" and projection.account is not None
        assert len(calls) == 1
        bundle = causal.DeclaredCellAccountInputs(env.context, artifact, env.process, env.frozen)
        yield SimpleNamespace(env=env, bundle=bundle, projection=projection, engine_calls=calls)
        assert len(calls) == 1


def _legacy_traps(monkeypatch):
    targets = (
        (coordinator, "_run_historical"), (coordinator, "_run_current"),
        (coordinator, "_run_capital"), (runner, "_validate_request"),
        (runner, "_effective_spool_limits"), (runner._FailedRunCleanup, "arm"),
        (coordinator, "derive_production_environment_authority"),
        (runner.authority_spool, "freeze_generation_context"),
        (runner.anchor_plan, "derive_production_anchor_plan"),
        (runner.config_binding, "freeze_production_pipeline_config"),
        (runner.search_planner, "freeze_production_search_process"),
        (runner.capital_bootstrap, "bootstrap_production_capital_authority"),
        (coordinator.authority_assembly, "assemble_authority_directory"),
    )
    guards = []
    for owner, name in targets:
        guard = Mock(side_effect=AssertionError(f"Legacy preparation forbidden: {name}"))
        monkeypatch.setattr(owner, name, guard)
        guards.append(guard)
    return guards


def _assert_unreached(guards):
    for guard in guards:
        guard.assert_not_called()


def _request(bundle, tmp_path):
    # Deliberately invalid legacy inputs must remain unread on the declared path.
    return runner.ProductionOptimizationRequest(
        active_config_path=tmp_path / "must-not-create" / "config.json",
        config=SimpleNamespace(classification="SYNTHETIC_UNREAD_LEGACY_CONFIG"),
        config_payload={}, requested_symbols=(), native_universe=object(),
        generation_id="MUST-NOT-PLAN-A-LEGACY-RUN", declared_account_inputs=bundle,
    )


def test_complete_account_produces_only_bound_canonical_nonqualification(complete_account):
    fixture = complete_account
    disposition = causal.evaluate_declared_cell_interval_admission(fixture.bundle)
    assert type(disposition) is causal.DeclaredCellIntervalDisposition
    body = disposition.to_dict()
    assert set(body) == {
        "schema_version", "status", "reason_code", "cell_id",
        "raw_account_descriptor_sha256", "generation_context_sha256",
        "declared_precommit_sha256", "search_process_receipt_sha256",
        "config_sha256", "effective_config_sha256", "interval_policy_adopted",
        "book_admissible", "qualification_authority", "capital_authority",
        "live_authority", "order_authority",
    }
    assert body["schema_version"] == "fxpm.declared-cell-interval-disposition.v1"
    assert disposition.status == body["status"] == "NOT_QUALIFIED"
    assert disposition.reason_code == body["reason_code"] == "NO_ADOPTED_INTERVAL_POLICY"
    assert body["cell_id"] == fixture.projection.cell_id
    assert body["raw_account_descriptor_sha256"] == fixture.bundle.artifact.descriptor["sha256"]
    assert body["generation_context_sha256"] == fixture.bundle.context.context_sha256
    assert body["declared_precommit_sha256"] == fixture.env.precommit.receipt_sha256
    assert body["search_process_receipt_sha256"] == fixture.bundle.process.receipt_sha256
    assert body["config_sha256"] == fixture.bundle.frozen_pipeline_config.config_sha256
    assert body["effective_config_sha256"] == fixture.bundle.frozen_pipeline_config.effective_config_sha256
    for name in ("interval_policy_adopted", "book_admissible", "qualification_authority",
                 "capital_authority", "live_authority", "order_authority"):
        assert body[name] is False
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    assert disposition._body_json == raw and len(raw) <= 4096
    assert str(fixture.bundle.context.root).encode() not in raw
    assert str(fixture.env.config_path).encode() not in raw
    assert str(fixture.bundle.context.root) not in repr(fixture.bundle)
    body["status"] = "FORGED"
    assert disposition.to_dict()["status"] == "NOT_QUALIFIED"
    with pytest.raises(FrozenInstanceError):
        disposition._body_json = b"{}"


def test_coordinator_returns_bound_nonqualification_without_legacy_fallback(
    complete_account, monkeypatch,
):
    guards = _legacy_traps(monkeypatch)
    with pytest.raises(coordinator.ProductionAuthorityNotQualified) as caught:
        coordinator.coordinate_production_authority(declared_account_inputs=complete_account.bundle)
    assert caught.value.code == "NOT_QUALIFIED"
    assert caught.value.details == ("NO_ADOPTED_INTERVAL_POLICY",)
    assert type(caught.value.disposition) is causal.DeclaredCellIntervalDisposition
    assert caught.value.disposition.to_dict()["raw_account_descriptor_sha256"] == (
        complete_account.bundle.artifact.descriptor["sha256"]
    )
    _assert_unreached(guards)


def test_runner_stops_before_request_work_native_context_plan_or_assembly(
    complete_account, monkeypatch, tmp_path,
):
    guards = _legacy_traps(monkeypatch)
    with pytest.raises(runner.ProductionOptimizationNotPromotable) as caught:
        runner.run_production_optimization(_request(complete_account.bundle, tmp_path))
    assert caught.value.code == "DECLARED_CELL_INTERVAL_NOT_QUALIFIED"
    assert caught.value.details == ("NO_ADOPTED_INTERVAL_POLICY",)
    assert caught.value.interval_disposition.to_dict() == (
        causal.evaluate_declared_cell_interval_admission(complete_account.bundle).to_dict()
    )
    assert not list(tmp_path.iterdir())
    _assert_unreached(guards)


def test_caller_projection_and_corrupt_descriptor_cannot_become_dispositions(complete_account):
    for caller_value in (complete_account.projection, complete_account.projection.verified_body):
        with pytest.raises(causal.CausalEvidenceProductionError) as caught:
            causal.evaluate_declared_cell_interval_admission(caller_value)
        assert caught.value.code == "DECLARED_CELL_ADMISSION_INPUTS_INVALID"
    descriptor = dict(complete_account.bundle.artifact.descriptor)
    descriptor["sha256"] = "0" * 64
    changed = replace(
        complete_account.bundle,
        artifact=causal.DeclaredCellRawAccountArtifact(descriptor),
    )
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        causal.evaluate_declared_cell_interval_admission(changed)
    assert caught.value.code.startswith("DECLARED_CELL_RAW_ACCOUNT")


def test_mixed_legacy_and_declared_inputs_are_refused_before_evaluation(
    complete_account, monkeypatch,
):
    guards = _legacy_traps(monkeypatch)
    evaluate = Mock(side_effect=AssertionError("Ambiguous inputs reached interval evaluation"))
    monkeypatch.setattr(causal, "evaluate_declared_cell_interval_admission", evaluate)
    with pytest.raises(coordinator.ProductionAuthorityBlocked) as caught:
        coordinator.coordinate_production_authority(
            declared_account_inputs=complete_account.bundle,
            historical_inputs=object(), current_refit_factory=object(),
            capital_factory=object(), assembly_inputs=object(),
        )
    assert caught.value.code == "DECLARED_CELL_AUTHORITY_INPUTS_AMBIGUOUS"
    evaluate.assert_not_called()
    _assert_unreached(guards)


def _assert_technical_at_all_consumers(bundle, code, tmp_path):
    with pytest.raises(causal.CausalEvidenceProductionError) as producer_error:
        causal.evaluate_declared_cell_interval_admission(bundle)
    assert producer_error.value.code == code
    with pytest.raises(coordinator.ProductionAuthorityBlocked) as coordinator_error:
        coordinator.coordinate_production_authority(declared_account_inputs=bundle)
    assert coordinator_error.value.code == code
    with pytest.raises(runner.ProductionOptimizationRunnerError) as runner_error:
        runner.run_production_optimization(_request(bundle, tmp_path))
    assert runner_error.value.code == code


def test_real_missing_only_prefix_remains_technical_without_an_account(
    complete_account, monkeypatch, tmp_path,
):
    env = carrier._synthetic_reopened_declared_prefix(tmp_path, monkeypatch, "v16", missing_only=True)
    no_engine = Mock(side_effect=AssertionError("INCOMPLETE attempted an account run"))
    monkeypatch.setattr(forward, "_run_declared_cell_account", no_engine)
    artifact = forward.produce_declared_cell_account_artifact(
        env.context, precommit=env.precommit, process=env.process, frozen_pipeline_config=env.frozen,
    )
    bundle = causal.DeclaredCellAccountInputs(env.context, artifact, env.process, env.frozen)
    projection = causal.read_declared_cell_account_artifact(
        env.context, artifact, process=env.process, frozen_pipeline_config=env.frozen,
    )
    assert projection.progress_status == "INCOMPLETE" and projection.account is None
    assert len(env.driver_calls) == 13 and env.native_calls == []
    guards = _legacy_traps(monkeypatch)
    _assert_technical_at_all_consumers(bundle, "DECLARED_CELL_ACCOUNT_INCOMPLETE", tmp_path)
    no_engine.assert_not_called()
    _assert_unreached(guards)


def test_mocked_reader_stop_stays_technical_at_new_consumers(
    complete_account, monkeypatch, tmp_path,
):
    """Propagation only: this is not an authenticated STOP witness or relabelled body."""
    reader = Mock(side_effect=causal.CausalEvidenceProductionError("DECLARED_CELL_ACCOUNT_STOP"))
    monkeypatch.setattr(causal, "read_declared_cell_account_artifact", reader)
    guards = _legacy_traps(monkeypatch)
    _assert_technical_at_all_consumers(
        complete_account.bundle, "DECLARED_CELL_ACCOUNT_STOP", tmp_path,
    )
    assert reader.call_count == 3
    assert not list(tmp_path.iterdir())
    _assert_unreached(guards)
