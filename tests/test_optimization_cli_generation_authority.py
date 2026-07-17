import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import pm_main
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

    def on_optimization_complete(self):
        self.completions += 1


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
