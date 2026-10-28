import hashlib
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_main
import pm_native_activation
from pm_main import FXPortfolioManagerApp, _governed_generation_cli_verdict


def _write_promoted_generation(tmp_path: Path, ledger: bytes = b"{}"):
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_bytes(ledger)
    ledger_sha = hashlib.sha256(ledger).hexdigest()
    generation_id = "generation-1"
    manifest_path = tmp_path / "pm_configs.generation.active.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "fxpm.optimization-generation.v2",
                "generation_id": generation_id,
                "generation_status": "PROMOTED",
                "ledger_sha256": ledger_sha,
                "ledger_status": {"complete_current_contract_universe": True},
                "technical_failures": {},
            }
        ),
        encoding="utf-8",
    )
    outcome = SimpleNamespace(
        generation_id=generation_id,
        status="PROMOTED",
        promoted=True,
        ledger_changed=True,
        technical_failures={},
        promoted_ledger_sha256=ledger_sha,
        manifest_path=str(manifest_path),
    )
    return ledger_path, manifest_path, outcome


def test_promoted_empty_ledger_without_authority_chain_is_rejected(tmp_path: Path):
    ledger_path, _manifest_path, outcome = _write_promoted_generation(tmp_path)

    accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)

    assert not accepted
    assert "OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_MISSING" in errors


def test_authority_chain_must_be_reverified_not_self_asserted(
    tmp_path: Path,
    monkeypatch,
):
    ledger_path, manifest_path, outcome = _write_promoted_generation(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = {"receipt_type": "fxpm.optimization-authority-chain"}
    manifest["authority_chain_receipt"] = receipt
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    outcome.authority_chain_receipt = receipt
    monkeypatch.setitem(
        sys.modules,
        "pm_optimization_authority",
        SimpleNamespace(
            verify_optimization_authority_chain=lambda **_kwargs: [
                "CAPITAL_AUTHORITY_BLOCKED"
            ]
        ),
    )

    accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)

    assert not accepted
    assert "CAPITAL_AUTHORITY_BLOCKED" in errors


def test_outcome_and_manifest_authority_receipts_must_match(tmp_path: Path):
    ledger_path, manifest_path, outcome = _write_promoted_generation(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["authority_chain_receipt"] = {"payload_sha256": "a" * 64}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    outcome.authority_chain_receipt = {"payload_sha256": "b" * 64}

    accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)

    assert not accepted
    assert "OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_MISMATCH" in errors


def test_manifest_path_must_match_the_active_generation(tmp_path: Path):
    ledger_path, _manifest_path, outcome = _write_promoted_generation(tmp_path)
    expected = tmp_path / "pm_configs.generation.json"

    accepted, errors = _governed_generation_cli_verdict(
        outcome,
        ledger_path,
        expected_manifest_path=expected,
    )

    assert not accepted
    assert "OPTIMIZATION_GENERATION_MANIFEST_PATH_MISMATCH" in errors


def test_non_object_manifest_cannot_bypass_authority(tmp_path: Path):
    ledger_path, manifest_path, outcome = _write_promoted_generation(tmp_path)
    manifest_path.write_text("[]", encoding="utf-8")

    accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)

    assert not accepted
    assert "OPTIMIZATION_GENERATION_MANIFEST_NOT_OBJECT" in errors


def test_failed_generation_cannot_inherit_stale_promoted_ledger(tmp_path: Path):
    ledger_path, _manifest_path, outcome = _write_promoted_generation(tmp_path)
    outcome.status = "FAILED_TECHNICAL"
    outcome.promoted = False
    outcome.ledger_changed = False
    outcome.technical_failures = {"AUDUSD": "worker failed"}

    accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)

    assert not accepted
    assert "OPTIMIZATION_GENERATION_STATUS_NOT_PROMOTED:FAILED_TECHNICAL" in errors
    assert "OPTIMIZATION_GENERATION_TECHNICAL_FAILURES_PRESENT" in errors


def test_recovery_and_noop_are_not_cli_success_without_exact_verification(
    tmp_path: Path,
):
    ledger_path, _manifest_path, outcome = _write_promoted_generation(tmp_path)
    for status in ("PROMOTED_RECOVERY_REQUIRED", "NOOP"):
        outcome.status = status
        accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)
        assert not accepted
        assert f"OPTIMIZATION_GENERATION_STATUS_NOT_PROMOTED:{status}" in errors


class _Logger:
    def info(self, *_args, **_kwargs):
        pass

    def warning(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        pass


class _Storage:
    def __init__(self):
        self.completions = 0
        self.shutdowns = 0

    def on_optimization_complete(self):
        self.completions += 1

    def on_shutdown(self):
        self.shutdowns += 1


def test_supervisor_parent_shutdown_skips_duplicate_storage_tree_scan() -> None:
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.trader = None
    app.storage_manager = _Storage()
    app.mt5 = None
    app._skip_storage_shutdown_housekeeping = True

    app.shutdown()

    assert app.storage_manager.shutdowns == 0


class _FailedManager:
    def __init__(self, outcome):
        self.symbols = ["AUDUSD"]
        self.last_optimization_generation = outcome

    def initial_optimization(self, overwrite=False):
        del overwrite
        return {
            "AUDUSD": SimpleNamespace(
                success=False,
                config=None,
                error_message="MT5 pagination failed",
            )
        }

    def print_status(self):
        pass


class _MutatingRejectedManager(_FailedManager):
    def __init__(self, outcome, ledger_path: Path, manifest_path: Path, target: bytes):
        super().__init__(outcome)
        self.ledger_path = ledger_path
        self.manifest_path = manifest_path
        self.target = target

    def initial_optimization(self, overwrite=False):
        del overwrite
        self.ledger_path.write_bytes(self.target)
        target_sha = hashlib.sha256(self.target).hexdigest()
        self.manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": "fxpm.optimization-generation.v2",
                    "generation_id": self.last_optimization_generation.generation_id,
                    "generation_status": "PROMOTED",
                    "ledger_sha256": target_sha,
                    "ledger_status": {"complete_current_contract_universe": True},
                    "technical_failures": {},
                }
            ),
            encoding="utf-8",
        )
        return {}


def test_failed_run_preserves_last_success_summary_and_storage_hook(
    tmp_path: Path,
):
    ledger_path, _manifest_path, outcome = _write_promoted_generation(tmp_path)
    outcome.status = "FAILED_TECHNICAL"
    outcome.promoted = False
    outcome.ledger_changed = False
    outcome.technical_failures = {"AUDUSD": "MT5 pagination failed"}
    summary_path = tmp_path / "optimization_summary.csv"
    prior_summary = b"last,successful,summary\n"
    summary_path.write_bytes(prior_summary)

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["AUDUSD"]
    app.portfolio_manager = _FailedManager(outcome)
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert summary_path.read_bytes() == prior_summary
    assert app.storage_manager.completions == 0
    attempt = pd.read_csv(tmp_path / "optimization_attempt_summary.csv")
    assert len(attempt) == 2
    assert attempt.iloc[0]["generation_status"] == "FAILED_TECHNICAL"
    assert "MT5 pagination failed" in attempt.iloc[1]["error"]
    assert not list(tmp_path.glob(".optimization_attempt_summary.csv.*.tmp"))


def test_rejected_unjournaled_mutation_is_preserved_for_forensics(tmp_path: Path):
    ledger_path = tmp_path / "pm_configs.json"
    manifest_path = tmp_path / "pm_configs.generation.json"
    prior_ledger = b'{"EURUSD":{"winner":"old"}}'
    prior_manifest = b'{"generation_status":"PRIOR"}'
    target_ledger = b'{"EURUSD":{"winner":"legacy-unverified"}}'
    ledger_path.write_bytes(prior_ledger)
    manifest_path.write_bytes(prior_manifest)
    outcome = SimpleNamespace(
        generation_id="generation-rejected",
        status="PROMOTED",
        promoted=True,
        ledger_changed=True,
        technical_failures={},
        promoted_ledger_sha256=hashlib.sha256(target_ledger).hexdigest(),
        manifest_path=str(manifest_path),
    )

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MutatingRejectedManager(
        outcome,
        ledger_path,
        manifest_path,
        target_ledger,
    )
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == target_ledger
    assert manifest_path.read_bytes() != prior_manifest
    assert app.storage_manager.completions == 0
    assert not list(tmp_path.glob(".*.optimize-snapshot*"))


def test_rejected_partial_external_write_is_never_overwritten_from_snapshot(
    tmp_path: Path,
):
    ledger_path = tmp_path / "pm_configs.json"
    manifest_path = tmp_path / "pm_configs.generation.json"
    prior_ledger = b'{"winner":"prior"}'
    prior_manifest = b'{"generation_status":"PRIOR"}'
    partial_ledger = b'{"winner":"partial"}'
    ledger_path.write_bytes(prior_ledger)
    manifest_path.write_bytes(prior_manifest)
    outcome = SimpleNamespace(
        generation_id="generation-partial",
        status="FAILED_TECHNICAL",
        promoted=False,
        ledger_changed=False,
        technical_failures={"__GENERATION__": "crashed"},
        promoted_ledger_sha256="f" * 64,
        manifest_path=str(manifest_path),
    )

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MutatingRejectedManager(
        outcome,
        ledger_path,
        manifest_path,
        partial_ledger,
    )
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == partial_ledger
    assert manifest_path.read_bytes() != prior_manifest
    assert not list(tmp_path.glob(".*.optimize-snapshot*"))


def test_rejected_manifest_only_external_write_is_preserved_for_forensics(tmp_path: Path):
    ledger_path = tmp_path / "pm_configs.json"
    manifest_path = tmp_path / "pm_configs.generation.json"
    prior_ledger = b'{"winner":"prior"}'
    prior_manifest = b'{"generation_status":"PRIOR"}'
    ledger_path.write_bytes(prior_ledger)
    manifest_path.write_bytes(prior_manifest)
    outcome = SimpleNamespace(
        generation_id="generation-manifest-only",
        status="FAILED_TECHNICAL",
        promoted=False,
        ledger_changed=False,
        technical_failures={"__GENERATION__": "manifest-only"},
        promoted_ledger_sha256="",
        manifest_path=str(manifest_path),
    )

    class _ManifestOnlyManager(_FailedManager):
        def initial_optimization(self, overwrite=False):
            del overwrite
            manifest_path.write_bytes(b'{"generation_status":"FAILED"}')
            return {}

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _ManifestOnlyManager(outcome)
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == prior_ledger
    assert manifest_path.read_bytes() == b'{"generation_status":"FAILED"}'
    assert not list(tmp_path.glob(".*.optimize-snapshot*"))


def test_post_cas_diagnostic_failure_cannot_roll_back_live_target(tmp_path: Path):
    ledger_path = tmp_path / "pm_configs.json"
    manifest_path = tmp_path / "pm_configs.generation.json"
    prior_ledger = b'{"winner":"prior"}'
    prior_manifest = b'{"generation_status":"PRIOR"}'
    target = b'{"winner":"unverified"}'
    ledger_path.write_bytes(prior_ledger)
    manifest_path.write_bytes(prior_manifest)
    outcome = SimpleNamespace(
        generation_id="generation-diagnostic-failure",
        status="PROMOTED",
        promoted=True,
        ledger_changed=True,
        technical_failures={},
        promoted_ledger_sha256=hashlib.sha256(target).hexdigest(),
        manifest_path=str(manifest_path),
    )
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MutatingRejectedManager(
        outcome, ledger_path, manifest_path, target
    )
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()
    app._save_results = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OSError("diagnostic disk failure")
    )

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == target
    assert manifest_path.read_bytes() != prior_manifest
    assert not list(tmp_path.glob(".*.optimize-snapshot*"))


def test_post_cas_verdict_exception_cannot_roll_back_live_target(
    tmp_path: Path,
    monkeypatch,
):
    ledger_path = tmp_path / "pm_configs.json"
    manifest_path = tmp_path / "pm_configs.generation.json"
    prior_ledger = b'{"winner":"prior"}'
    prior_manifest = b'{"generation_status":"PRIOR"}'
    target = b'{"winner":"unverified"}'
    ledger_path.write_bytes(prior_ledger)
    manifest_path.write_bytes(prior_manifest)
    outcome = SimpleNamespace(
        generation_id="generation-verdict-failure",
        status="PROMOTED",
        promoted=True,
        ledger_changed=True,
        technical_failures={},
        promoted_ledger_sha256=hashlib.sha256(target).hexdigest(),
        manifest_path=str(manifest_path),
    )
    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MutatingRejectedManager(
        outcome, ledger_path, manifest_path, target
    )
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()
    monkeypatch.setattr(
        pm_main,
        "_governed_generation_cli_verdict",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("verdict")),
    )

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == target
    assert manifest_path.read_bytes() != prior_manifest
    assert not list(tmp_path.glob(".*.optimize-snapshot*"))


def test_stale_recovery_snapshot_blocks_a_new_optimizer_run(tmp_path: Path):
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_bytes(b'{"winner":"prior"}')
    stale = tmp_path / ".pm_configs.json.optimize-snapshot"
    stale.write_bytes(b"operator recovery evidence")
    outcome = SimpleNamespace(status="MISSING")

    class _MustNotRun(_FailedManager):
        def initial_optimization(self, overwrite=False):
            raise AssertionError("optimizer ran despite stale recovery evidence")

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MustNotRun(outcome)
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert ledger_path.read_bytes() == b'{"winner":"prior"}'
    assert stale.read_bytes() == b"operator recovery evidence"


def test_stale_recovery_snapshot_blocks_when_target_is_absent(tmp_path: Path):
    ledger_path = tmp_path / "pm_configs.json"
    stale = tmp_path / ".pm_configs.json.optimize-snapshot"
    stale.write_bytes(b"operator recovery evidence")
    outcome = SimpleNamespace(status="MISSING")

    class _MustNotRun(_FailedManager):
        def initial_optimization(self, overwrite=False):
            raise AssertionError("optimizer ran despite stale recovery evidence")

    app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
    app.logger = _Logger()
    app.pipeline_config = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    app.mt5 = None
    app.symbols = ["EURUSD"]
    app.portfolio_manager = _MustNotRun(outcome)
    app.config_file = str(ledger_path)
    app.output_dir = tmp_path
    app.storage_manager = _Storage()

    assert not app.run_optimization(overwrite=True)
    assert not ledger_path.exists()
    assert stale.read_bytes() == b"operator recovery evidence"


def _production_scope_stats(**overrides):
    stats = {
        "schema_version": "fxpm.production_native_authority_verification_scope.v1",
        "scope": "optimization",
        "full_verification_count": 1,
        "identity_revalidation_attempt_count": 13,
        "identity_revalidation_count": 13,
        "identity_revalidation_failure_count": 0,
        "active_pointer_sha256": "a" * 64,
        "activation_manifest_sha256": "b" * 64,
        "closed": False,
        "poisoned": False,
        "pid": 1234,
        "thread_id": 5678,
        "poison_reason": None,
    }
    stats.update(overrides)
    return stats


class _FakeProductionAuthorityScope:
    def __init__(self, stats, events):
        self._stats = stats
        self._events = events
        self.active = False

    def __enter__(self):
        assert not self.active
        self.active = True
        self._events.append("scope_enter")
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        assert self.active
        self.active = False
        self._stats["closed"] = True
        self._events.append("scope_exit")
        return False

    def stats(self):
        assert not self.active
        self._events.append("scope_stats")
        return dict(self._stats)


def _install_optimization_main_harness(
    tmp_path,
    monkeypatch,
    *,
    production_mode=True,
    optimize_result=True,
    optimize_exception=None,
    trade=False,
    stats=None,
):
    events = []
    observed = {}
    scope = _FakeProductionAuthorityScope(
        stats if stats is not None else _production_scope_stats(),
        events,
    )

    class _App:
        def __init__(self, **kwargs):
            observed["init"] = kwargs
            self.mt5 = None
            self.config_file = kwargs["config_file"]
            self.portfolio_manager = SimpleNamespace(
                _reconcile_interrupted_promotion=lambda **_kwargs: (
                    events.append("reconcile") or "NONE"
                ),
                _recover_abandoned_supervised_generation=(
                    lambda _generation_id, **_kwargs: (
                        events.append("recover") or "RECOVERED"
                    )
                ),
            )

        def initialize(self):
            events.append("initialize")
            return True

        def run_optimization(self, *, overwrite=False):
            events.append("optimize")
            observed["overwrite"] = overwrite
            assert scope.active is production_mode
            if optimize_exception is not None:
                raise optimize_exception
            return optimize_result

        def run_trading(self, **kwargs):
            assert not scope.active
            events.append("trade")
            observed["trade"] = kwargs
            return True

        def shutdown(self):
            assert not scope.active
            events.append("shutdown")

    def _scope_factory():
        events.append("scope_factory")
        return scope

    mode = "calendar_lockbox_v2" if production_mode else "research_clean_holdout"
    config_path = tmp_path / "config.json"
    monkeypatch.setattr(
        pm_main,
        "load_config_json",
        lambda _path: {
            "symbols": ["EURUSD"],
            "pipeline": {
                "production_split_mode": mode,
                "research_mode": not production_mode,
                "data_dir": str(tmp_path / "data"),
                "output_dir": str(tmp_path / "out"),
                "log_dir": str(tmp_path / "logs"),
                "winner_ledger_path": str(tmp_path / "pm_configs.json"),
            },
        },
    )
    monkeypatch.setattr(pm_main, "setup_logging", lambda **_kwargs: None)
    monkeypatch.setattr(pm_main, "log_resolved_config_summary", lambda *_args: None)
    monkeypatch.setattr(pm_main.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(pm_main, "set_instrument_specs", lambda **_kwargs: None)
    monkeypatch.setattr(
        pm_main,
        "_production_hard_supervision_required",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(pm_main, "FXPortfolioManagerApp", _App)
    monkeypatch.setattr(
        pm_native_activation,
        "production_native_authority_verification_scope",
        _scope_factory,
        raising=False,
    )
    argv = [
        "pm_main.py",
        "--config",
        str(config_path),
        "--optimize",
        "--overwrite",
    ]
    if trade:
        argv.extend(["--trade", "--mt5-demo"])
    monkeypatch.setattr(sys, "argv", argv)
    return events, observed, scope


def test_calendar_optimize_requires_exact_closed_scope_and_trades_after_exit(
    tmp_path,
    monkeypatch,
    caplog,
):
    stats = _production_scope_stats()
    events, observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
        stats=stats,
        trade=True,
    )
    caplog.set_level(logging.INFO, logger=pm_main.__name__)

    assert pm_main.main() == 0

    assert observed["overwrite"] is True
    assert events.index("scope_enter") < events.index("optimize")
    assert events.index("optimize") < events.index("scope_exit")
    assert events.index("scope_exit") < events.index("scope_stats")
    assert events.index("scope_stats") < events.index("trade")
    assert events.index("trade") < events.index("shutdown")
    assert observed["trade"]["enable_trading"] is True
    assert observed["trade"]["execution_book"] == pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK
    assert observed["init"]["pipeline_config"]._funded_live_execution is False
    expected = json.dumps(
        {**stats, "closed": True},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    assert (
        "PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_STATS " + expected
        in caplog.messages
    )


@pytest.mark.parametrize("mt5_demo", [True, False])
def test_trade_cli_destination_preserves_book_authority_and_funded_bit(
    tmp_path, monkeypatch, mt5_demo,
):
    """Route through real main; application and evidence authority are stubbed."""
    events, observed, _scope = _install_optimization_main_harness(tmp_path, monkeypatch)
    arguments = ["--config", str(tmp_path / "config.json"), "--trade"]
    if mt5_demo:
        arguments.append("--mt5-demo")
    checks = []

    def funded_schema(config):
        checks.append(("funded_schema", config))

    def funded_readiness(*args, **kwargs):
        checks.append(("funded_readiness", args, kwargs))
        return SimpleNamespace(ok=True, errors=[])

    monkeypatch.setattr(pm_main, "_validate_funded_config_schema", funded_schema)
    monkeypatch.setattr(pm_main, "validate_live_readiness", funded_readiness)
    monkeypatch.setattr(pm_main, "log_readiness_report", lambda *_args: None)
    assert pm_main.main(arguments) == 0
    pipeline = observed["init"]["pipeline_config"]
    assert pipeline._funded_live_execution is (not mt5_demo)
    assert observed["trade"] == {
        "enable_trading": True, "close_on_opposite_signal": False,
        "execution_book": (pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK
                           if mt5_demo else pm_main.CHAMPION_EXECUTION_BOOK),
        "startup_entry_block_reason": "",
    }
    assert events == ["initialize", "trade", "shutdown"]
    assert [check[0] for check in checks] == (
        [] if mt5_demo else ["funded_schema", "funded_readiness"])


@pytest.mark.parametrize("mt5_demo,hard_supervision", [(False, True), (True, False)])
def test_production_optimize_supervision_distinguishes_mt5_demo_destination(
    mt5_demo, hard_supervision,
):
    args = SimpleNamespace(
        optimize=True, trade=False, paper=False, mt5_demo=mt5_demo,
        preflight=False, status=False,
    )
    pipeline = SimpleNamespace(production_split_mode="calendar_lockbox_v2")
    assert pm_main._production_hard_supervision_required(
        args, pipeline, supervised_generation_id=None,
    ) is hard_supervision


@pytest.mark.parametrize("arguments", [
    ["--mt5-demo"],
    ["--optimize", "--mt5-demo"],
    ["--status", "--mt5-demo"],
])
def test_mt5_demo_without_action_refuses_before_configuration_application_or_broker(
    monkeypatch, capsys, arguments,
):
    def forbidden(*_args, **_kwargs):
        pytest.fail("A missing DEMO action must refuse before any runtime access")

    for name in ("load_config_json", "FXPortfolioManagerApp", "MT5Connector"):
        monkeypatch.setattr(pm_main, name, forbidden)
    monkeypatch.setattr(pm_main, "MT5_AVAILABLE", True)
    assert pm_main.main(arguments) == 2
    message = capsys.readouterr().out
    assert "Refusing --mt5-demo" in message
    assert "--trade or --preflight" in message


def test_optimize_only_calendar_initialization_never_connects_unused_mt5(
    tmp_path,
    monkeypatch,
) -> None:
    config = pm_main.PipelineConfig(
        production_split_mode="calendar_lockbox_v2",
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        winner_ledger_path=str(tmp_path / "pm_configs.json"),
    )
    config._optimization_only_entrypoint = True
    app = pm_main.FXPortfolioManagerApp(
        symbols=["EURUSD"],
        pipeline_config=config,
        config_file=str(tmp_path / "pm_configs.json"),
    )
    monkeypatch.setattr(pm_main, "MT5_AVAILABLE", True)
    monkeypatch.setattr(
        pm_main,
        "MT5Connector",
        lambda *_args, **_kwargs: pytest.fail("MT5 must not be constructed"),
    )
    observed: dict[str, object] = {}

    def portfolio_manager(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(symbol_configs={})

    monkeypatch.setattr(
        pm_main,
        "PortfolioManager",
        portfolio_manager,
    )

    assert app.initialize() is True
    assert app.mt5 is None


def test_successful_calendar_optimize_fails_on_scope_counter_mismatch(
    tmp_path,
    monkeypatch,
    caplog,
):
    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
        stats=_production_scope_stats(identity_revalidation_count=12),
    )
    caplog.set_level(logging.INFO, logger=pm_main.__name__)

    assert pm_main.main() == 2

    assert "scope_exit" in events
    assert "trade" not in events
    assert any(
        "PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_CONTRACT_REJECTED" in message
        and "identity_revalidation_count:expected=13:actual=12" in message
        for message in caplog.messages
    )


def test_optimize_only_calendar_uses_hard_supervisor_without_run_limit(
    tmp_path,
    monkeypatch,
) -> None:
    import pm_production_generation_entrypoint as generation_entrypoint

    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
    )
    captured = {}
    lease = object()

    class _LeaseContext:
        def __enter__(self):
            events.append("run_lease_enter")
            return lease

        def __exit__(self, _exc_type, _exc, _traceback):
            events.append("run_lease_exit")
            return False

    def fake_supervised(**kwargs):
        events.append("supervised")
        captured.update(kwargs)
        recovery = kwargs["recovery_callback"]("a" * 64)
        return SimpleNamespace(
            generation_id="a" * 64,
            succeeded=True,
            safe_to_reconcile=True,
            cli_exit_code=0,
            generation_recovery_status=recovery,
            stale_recovery_status=None,
            request_directory_retained=False,
            supervisor=SimpleNamespace(
                status="SUCCEEDED",
                succeeded=True,
                result_path=tmp_path / "supervisor-result.json",
            ),
        )

    monkeypatch.setattr(
        pm_main,
        "_production_hard_supervision_required",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        generation_entrypoint,
        "production_optimization_run_lease",
        lambda _path: _LeaseContext(),
    )
    monkeypatch.setattr(
        generation_entrypoint,
        "run_supervised_optimization_cli",
        fake_supervised,
    )

    assert pm_main.main() == 0

    assert captured["argv"] == sys.argv[1:]
    assert captured["run_lease"] is lease
    assert events.index("run_lease_enter") < events.index("initialize")
    assert events.index("initialize") < events.index("supervised")
    assert events.index("shutdown") < events.index("run_lease_exit")
    assert "optimize" not in events
    assert "scope_factory" not in events
    assert events[-1] == "run_lease_exit"


def test_supervised_failure_reconciles_only_after_tree_empty(
    tmp_path,
    monkeypatch,
) -> None:
    import pm_production_generation_entrypoint as generation_entrypoint

    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
    )
    monkeypatch.setattr(
        pm_main,
        "_production_hard_supervision_required",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        generation_entrypoint,
        "run_supervised_optimization_cli",
        lambda **kwargs: SimpleNamespace(
            generation_id="b" * 64,
            succeeded=False,
            safe_to_reconcile=True,
            cli_exit_code=None,
            generation_recovery_status=kwargs["recovery_callback"](
                "b" * 64
            ),
            stale_recovery_status=None,
            request_directory_retained=False,
            supervisor=SimpleNamespace(
                status="FAILED",
                succeeded=False,
                result_path=tmp_path / "failed-result.json",
            ),
        ),
    )

    assert pm_main.main() == 2

    assert "recover" in events
    assert "optimize" not in events
    assert "scope_factory" not in events


def test_supervised_unproved_tree_shutdown_never_reconciles(
    tmp_path,
    monkeypatch,
) -> None:
    import pm_production_generation_entrypoint as generation_entrypoint

    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
    )
    monkeypatch.setattr(
        pm_main,
        "_production_hard_supervision_required",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        generation_entrypoint,
        "run_supervised_optimization_cli",
        lambda **_kwargs: SimpleNamespace(
            generation_id="c" * 64,
            succeeded=False,
            safe_to_reconcile=False,
            cli_exit_code=None,
            generation_recovery_status=None,
            stale_recovery_status=None,
            request_directory_retained=True,
            supervisor=SimpleNamespace(
                status="TERMINATION_FAILED",
                succeeded=False,
                result_path=tmp_path / "unsafe-result.json",
            ),
        ),
    )

    assert pm_main.main() == 2

    assert "reconcile" not in events
    assert "optimize" not in events


def test_failed_calendar_optimize_logs_partial_scope_before_existing_failure(
    tmp_path,
    monkeypatch,
    caplog,
):
    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
        optimize_result=False,
        stats=_production_scope_stats(
            full_verification_count=1,
            identity_revalidation_attempt_count=4,
            identity_revalidation_count=3,
            identity_revalidation_failure_count=1,
            poisoned=True,
            poison_reason="ACTIVE_POINTER_IDENTITY_MISMATCH",
        ),
    )
    caplog.set_level(logging.INFO, logger=pm_main.__name__)

    assert pm_main.main() == 2

    assert events.index("scope_exit") < events.index("scope_stats")
    assert any(
        message.startswith("PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_STATS ")
        and '"identity_revalidation_attempt_count":4' in message
        for message in caplog.messages
    )
    assert any(
        "Optimization generation did not complete and promote" in message
        for message in caplog.messages
    )


@pytest.mark.parametrize("exception_type", [RuntimeError, SystemExit])
def test_calendar_optimize_exception_still_closes_and_reports_scope(
    tmp_path,
    monkeypatch,
    caplog,
    exception_type,
):
    events, _observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
        optimize_exception=exception_type("optimizer stopped"),
        stats=_production_scope_stats(
            full_verification_count=0,
            identity_revalidation_attempt_count=2,
            identity_revalidation_count=2,
        ),
    )
    caplog.set_level(logging.INFO, logger=pm_main.__name__)

    if exception_type is SystemExit:
        with pytest.raises(SystemExit, match="optimizer stopped"):
            pm_main.main()
    else:
        assert pm_main.main() == 1

    assert events.index("optimize") < events.index("scope_exit")
    assert events.index("scope_exit") < events.index("scope_stats")
    assert events.index("scope_stats") < events.index("shutdown")
    assert any(
        message.startswith("PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_STATS ")
        for message in caplog.messages
    )


def test_non_calendar_optimize_does_not_create_production_authority_scope(
    tmp_path,
    monkeypatch,
):
    events, observed, _scope = _install_optimization_main_harness(
        tmp_path,
        monkeypatch,
        production_mode=False,
    )

    assert pm_main.main() == 0

    assert observed["overwrite"] is True
    assert "scope_factory" not in events
    assert events.index("optimize") < events.index("shutdown")
