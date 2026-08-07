from __future__ import annotations

import hashlib
import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_audit.performance.scripts import run_promoted_cell_replay as replay_tools
import pm_book_multiplicity as book
import pm_production_optimization_runner as production_runner
from pm_core import PipelineConfig
from pm_pipeline import (
    CALENDAR_SPLIT_MODE,
    ConfigLedger,
    OptimizationGenerationRequest,
    PortfolioManager,
    RegimeConfig,
    SymbolConfig,
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _deployment_manager(
    tmp_path: Path,
    *,
    split_mode: str,
    research_mode: bool,
    funded_execution: bool = False,
) -> tuple[PortfolioManager, Path, Path, Path, dict[str, str], bytes, bytes]:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"symbols": ["EURUSD"], "pipeline": {"data_dir": "data"}}),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "pm_configs.json"
    prior = b'{"EURUSD":{"winner":"last-good"}}\n'
    candidate = b'{"EURUSD":{"winner":"candidate"}}\n'
    ledger_path.write_bytes(prior)
    stage_path = tmp_path / ".pm_configs.json.generation.staging"
    stage_path.write_bytes(candidate)
    stage_manifest_path = tmp_path / ".pm_configs.json.generation.staging.manifest.json"
    stage_manifest_path.write_bytes(b'{"status":"staged"}\n')

    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config = SimpleNamespace(
        _deployment_config_path=str(config_path),
        _funded_live_execution=funded_execution,
        production_split_mode=split_mode,
        research_mode=research_mode,
    )
    manager.config_file = str(ledger_path)
    manager.symbols = ["EURUSD"]
    manifest = {
        "generation_id": "generation-authority-guard",
        "ledger_sha256": _sha256(candidate),
        "base_ledger_sha256": _sha256(prior),
    }
    return (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        prior,
        candidate,
    )


@pytest.mark.parametrize(
    "split_mode",
    ("chronological_lockbox_v1", "bleeding_edge", "legacy-production-spelling"),
)
def test_non_calendar_deployment_cannot_cas_without_raw_authority(
    tmp_path: Path,
    split_mode: str,
) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        prior,
        candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode=split_mode,
        research_mode=False,
    )
    stage_manifest_before = stage_manifest_path.read_bytes()
    active_manifest_path = manager._optimization_generation_paths()[2]
    active_manifest_before = b'{"generation_id":"last-good"}\n'
    active_manifest_path.write_bytes(active_manifest_before)

    with pytest.raises(RuntimeError, match="OPTIMIZATION_AUTHORITY_PRE_CAS_BLOCKED"):
        manager._promote_staged_generation(
            stage_path,
            stage_manifest_path,
            manifest,
            universe=SimpleNamespace(),
        )

    assert ledger_path.read_bytes() == prior
    assert stage_path.read_bytes() == candidate
    assert stage_manifest_path.read_bytes() == stage_manifest_before
    assert active_manifest_path.read_bytes() == active_manifest_before
    assert not manager._optimization_promotion_journal_path().exists()


def test_explicit_non_funded_research_generation_keeps_legacy_cas_behavior(
    tmp_path: Path,
) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        _prior,
        candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="bleeding_edge",
        research_mode=True,
    )

    class _Ledger:
        def load(self) -> None:
            return None

        def get_all_configs(self) -> dict[str, str]:
            return {"EURUSD": "loaded"}

    manager.ledger = _Ledger()
    manager._last_promotion_authorization = object()

    manager._promote_staged_generation(
        stage_path,
        stage_manifest_path,
        manifest,
        universe=None,
    )

    assert ledger_path.read_bytes() == candidate
    assert manager.symbol_configs == {"EURUSD": "loaded"}
    assert manager._last_promotion_authorization is None
    assert not manager._optimization_promotion_journal_path().exists()


def test_promotion_rejects_live_change_during_raw_verification(tmp_path: Path) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        _prior,
        _candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="production",
        research_mode=False,
    )
    external = b'{"EXTERNAL":{"newer":true}}\n'

    def mutate_live(*args, **kwargs):
        del args, kwargs
        ledger_path.write_bytes(external)

    manager._verify_staged_authority_before_cas = mutate_live  # noqa: SLF001

    with pytest.raises(
        RuntimeError,
        match="OPTIMIZATION_GENERATION_LIVE_LEDGER_CHANGED_DURING_VERIFY",
    ):
        manager._promote_staged_generation(  # noqa: SLF001
            stage_path,
            stage_manifest_path,
            manifest,
            universe=SimpleNamespace(),
        )

    assert ledger_path.read_bytes() == external
    assert stage_path.exists()
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_promotion_rejects_stage_substitution_during_raw_verification(
    tmp_path: Path,
) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        prior,
        _candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="production",
        research_mode=False,
    )
    substitution = b'{"SUBSTITUTED":{"winner":"attacker"}}\n'

    def mutate_stage(*args, **kwargs):
        del args, kwargs
        stage_path.write_bytes(substitution)

    manager._verify_staged_authority_before_cas = mutate_stage  # noqa: SLF001

    with pytest.raises(
        RuntimeError,
        match="OPTIMIZATION_GENERATION_STAGE_CHANGED_DURING_VERIFY",
    ):
        manager._promote_staged_generation(  # noqa: SLF001
            stage_path,
            stage_manifest_path,
            manifest,
            universe=SimpleNamespace(),
        )

    assert ledger_path.read_bytes() == prior
    assert stage_path.read_bytes() == substitution
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_internal_symbol_writer_cannot_follow_and_invalidate_governed_cas(
    tmp_path: Path,
) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        prior,
        _candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="research",
        research_mode=True,
    )
    candidate = _sparse_current_ledger()
    stage_path.write_bytes(candidate)
    generation_id = "governed-concurrent-generation"
    manifest = {
        **manifest,
        "generation_id": generation_id,
        "ledger_sha256": _sha256(candidate),
        "base_ledger_sha256": _sha256(prior),
            "authority_chain_receipt": {
                "generation_id": generation_id,
                "ledger_sha256": _sha256(candidate),
            },
        "ledger_status": {"complete_current_contract_universe": True},
    }
    manager.ledger = ConfigLedger(str(ledger_path))
    manager._last_promotion_authorization = None
    verifier_entered = threading.Event()
    release_verifier = threading.Event()
    writer_done = threading.Event()
    writer_ledger = ConfigLedger(str(ledger_path))
    replacement = SymbolConfig(symbol=book.PINNED_SYMBOL_ORDER[0], is_validated=True)

    def verifier(*args, **kwargs):
        del args, kwargs
        verifier_entered.set()
        assert release_verifier.wait(timeout=5)

    manager._verify_staged_authority_before_cas = verifier  # noqa: SLF001

    def promote() -> None:
        manager._promote_staged_generation(  # noqa: SLF001
            stage_path,
            stage_manifest_path,
            manifest,
            universe=None,
        )

    def write_symbol() -> bool:
        try:
            return writer_ledger.update_symbol(
                book.PINNED_SYMBOL_ORDER[0], replacement
            )
        finally:
            writer_done.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        promote_future = pool.submit(promote)
        assert verifier_entered.wait(timeout=5)
        writer_future = pool.submit(write_symbol)
        assert not writer_done.wait(timeout=0.1)
        release_verifier.set()
        promote_future.result(timeout=5)
        assert writer_future.result(timeout=5) is False

    assert ledger_path.read_bytes() == candidate
    active = json.loads(
        manager._optimization_generation_paths()[2].read_text(encoding="utf-8")  # noqa: SLF001
    )
    assert active["generation_id"] == generation_id


def test_two_concurrent_promoters_cannot_overwrite_each_other(tmp_path: Path) -> None:
    (
        first,
        ledger_path,
        _default_stage,
        stage_manifest_path,
        _manifest,
        prior,
        _candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="research",
        research_mode=True,
    )
    second = PortfolioManager.__new__(PortfolioManager)
    second.config = first.config
    second.config_file = first.config_file
    second.symbols = list(first.symbols)

    class _Ledger:
        def load(self) -> None:
            return None

        def get_all_configs(self) -> dict[str, str]:
            return {"EURUSD": "loaded"}

    first.ledger = _Ledger()
    second.ledger = _Ledger()
    first._last_promotion_authorization = None
    second._last_promotion_authorization = None
    candidate_one = b'{"EURUSD":{"winner":"one"}}\n'
    candidate_two = b'{"EURUSD":{"winner":"two"}}\n'
    stage_one = tmp_path / "candidate-one.json"
    stage_two = tmp_path / "candidate-two.json"
    stage_one.write_bytes(candidate_one)
    stage_two.write_bytes(candidate_two)
    manifest_one = {
        "generation_id": "concurrent-one",
        "ledger_sha256": _sha256(candidate_one),
        "base_ledger_sha256": _sha256(prior),
    }
    manifest_two = {
        "generation_id": "concurrent-two",
        "ledger_sha256": _sha256(candidate_two),
        "base_ledger_sha256": _sha256(prior),
    }
    start = threading.Barrier(2)

    def promote(manager, stage, manifest):
        start.wait(timeout=5)
        try:
            manager._promote_staged_generation(  # noqa: SLF001
                stage,
                stage_manifest_path,
                manifest,
                universe=None,
            )
            return "PROMOTED"
        except RuntimeError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = tuple(
            pool.map(
                lambda args: promote(*args),
                (
                    (first, stage_one, manifest_one),
                    (second, stage_two, manifest_two),
                ),
            )
        )

    assert outcomes.count("PROMOTED") == 1
    assert sum("LIVE_LEDGER_CHANGED_DURING_RUN" in item for item in outcomes) == 1
    assert ledger_path.read_bytes() in {candidate_one, candidate_two}
    assert not first._optimization_promotion_journal_path().exists()  # noqa: SLF001


def _production_cutover_manager(tmp_path: Path) -> tuple[PortfolioManager, Path]:
    config_path = tmp_path / "config.json"
    config_payload = {
        "_meta": {"scope": "funded_live_v1"},
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "pipeline": {
            "data_dir": str(tmp_path / "data"),
            "timeframes": list(book.TIMEFRAME_ORDER),
        },
        "position": {},
        "storage": {},
    }
    config_path.write_text(json.dumps(config_payload), encoding="utf-8")
    (tmp_path / "data").mkdir()
    config = PipelineConfig(
        data_dir=tmp_path / "data",
        timeframes=list(book.TIMEFRAME_ORDER),
        production_split_mode=CALENDAR_SPLIT_MODE,
    )
    config._deployment_config_path = str(config_path)  # noqa: SLF001
    config._funded_live_execution = False  # noqa: SLF001
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.config = config
    manager.symbols = list(book.PINNED_SYMBOL_ORDER)
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_bytes(b'{"LAST_GOOD":{"value":1}}')
    manager.config_file = str(ledger_path)
    manager.last_optimization_generation = None
    manager._native_universe_for_optimization = lambda: SimpleNamespace(  # noqa: SLF001
        activation_id="activation-current",
        activation_manifest_sha256="a" * 64,
    )
    return manager, ledger_path


def _sparse_current_ledger(
    period: int = 10,
    *,
    symbol: str | None = None,
) -> bytes:
    symbol = symbol or book.PINNED_SYMBOL_ORDER[0]
    schedule = PipelineConfig()
    # Keep promotion fixtures well clear of a live retrain boundary.  Tests that
    # exercise the boundary itself pass an explicit validation clock below.
    optimized_at = (
        datetime.now(timezone.utc).replace(microsecond=0) + schedule.retrain_interval
    )
    valid_until = schedule.get_next_retrain_at_utc(optimized_at)
    candidate = RegimeConfig(
        strategy_name="TestStrategy",
        parameters={"period": period},
        quality_score=0.0,
        trained_at=optimized_at,
        valid_until=valid_until,
        promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        artifact_meta={"book_role": "PROCESS_VALIDATED_CURRENT_REFIT"},
    )
    config = SymbolConfig(
        symbol=symbol,
        is_validated=True,
        optimized_at=optimized_at,
        valid_until=valid_until,
        regime_configs={book.TIMEFRAME_ORDER[0]: {book.REGIME_ORDER[0]: candidate}},
    )
    return json.dumps(
        {symbol: config.to_dict()},
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("ascii")


def test_sparse_current_ledger_requires_exact_future_utc_expiry(tmp_path: Path) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    payload = json.loads(raw)
    symbol = book.PINNED_SYMBOL_ORDER[0]
    timeframe = book.TIMEFRAME_ORDER[0]
    regime = book.REGIME_ORDER[0]
    deadline = datetime.fromisoformat(payload[symbol]["valid_until"])

    parsed, count = manager._validated_sparse_current_ledger(  # noqa: SLF001
        raw,
        validation_time_utc=deadline - timedelta(seconds=1),
    )
    assert set(parsed) == {symbol}
    assert count == 1

    def encoded(value):
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")

    missing_cell_deadline = json.loads(raw)
    missing_cell_deadline[symbol]["regime_configs"][timeframe][regime][
        "valid_until"
    ] = None
    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_AUTHORITY_LEDGER_CELL_VALID_UNTIL_MISMATCH",
    ):
        manager._validated_sparse_current_ledger(  # noqa: SLF001
            encoded(missing_cell_deadline),
            validation_time_utc=deadline - timedelta(seconds=1),
        )

    wrong_slot = json.loads(raw)
    wrong_slot[symbol]["valid_until"] = (
        deadline + manager.config.retrain_interval
    ).isoformat()
    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_AUTHORITY_LEDGER_VALID_UNTIL_MISMATCH",
    ):
        manager._validated_sparse_current_ledger(  # noqa: SLF001
            encoded(wrong_slot),
            validation_time_utc=deadline - timedelta(seconds=1),
        )

    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_AUTHORITY_LEDGER_VALID_UNTIL_EXPIRED",
    ):
        manager._validated_sparse_current_ledger(  # noqa: SLF001
            raw,
            validation_time_utc=deadline,
        )


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_sparse_current_ledger_preserves_exact_mt5_symbol(
    tmp_path: Path,
    symbol: str,
) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger(symbol=symbol)
    deadline = datetime.fromisoformat(json.loads(raw)[symbol]["valid_until"])

    parsed, count = manager._validated_sparse_current_ledger(  # noqa: SLF001
        raw,
        validation_time_utc=deadline - timedelta(seconds=1),
    )

    assert set(parsed) == {symbol}
    assert parsed[symbol].symbol == symbol
    assert count == 1


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_sparse_current_ledger_rejects_wrong_case_mt5_symbol(
    tmp_path: Path,
    wrong_case: str,
) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    payload = json.loads(_sparse_current_ledger(symbol="Platinum"))
    payload[wrong_case] = payload.pop("Platinum")
    payload[wrong_case]["symbol"] = wrong_case

    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_AUTHORITY_LEDGER_SYMBOL_OUTSIDE_UNIVERSE",
    ):
        manager._validated_sparse_current_ledger(  # noqa: SLF001
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii"),
            validation_time_utc=datetime.fromisoformat(
                payload[wrong_case]["valid_until"]
            )
            - timedelta(seconds=1),
        )


@pytest.mark.parametrize("wrong_case", ("PLATINUM", "platinum"))
def test_production_generation_rejects_wrong_case_mt5_target(
    tmp_path: Path,
    wrong_case: str,
) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    manager._reconcile_interrupted_promotion = lambda **_kwargs: None  # noqa: SLF001

    with pytest.raises(
        ValueError,
        match=f"OPTIMIZATION_GENERATION_TARGET_UNKNOWN:{wrong_case}",
    ):
        manager._run_production_authority_generation(  # noqa: SLF001
            OptimizationGenerationRequest(
                kind="INITIAL",
                target_symbols=(wrong_case,),
            )
        )


def test_promotion_revalidates_expiry_after_raw_verify_before_publication(
    tmp_path: Path,
) -> None:
    (
        manager,
        ledger_path,
        stage_path,
        stage_manifest_path,
        manifest,
        prior,
        candidate,
    ) = _deployment_manager(
        tmp_path,
        split_mode="production",
        research_mode=False,
    )
    events: list[str] = []

    def raw_verify(*args, **kwargs) -> None:
        del args, kwargs
        events.append("raw")

    def expiry_verify(raw: bytes, *, validation_time_utc: datetime):
        assert raw == candidate
        assert validation_time_utc.tzinfo is not None
        assert validation_time_utc.utcoffset() == timedelta(0)
        events.append("expiry")
        raise RuntimeError("TEST_EXPIRY_REJECTED")

    manager._verify_staged_authority_before_cas = raw_verify  # noqa: SLF001
    manager._validated_sparse_current_ledger = expiry_verify  # noqa: SLF001

    with pytest.raises(RuntimeError, match="TEST_EXPIRY_REJECTED"):
        manager._promote_staged_generation(  # noqa: SLF001
            stage_path,
            stage_manifest_path,
            manifest,
            universe=SimpleNamespace(),
        )

    assert events == ["raw", "expiry"]
    assert ledger_path.read_bytes() == prior
    assert stage_path.read_bytes() == candidate
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def _fake_production_result(request, raw: bytes):
    digest = hashlib.sha256(request.generation_id.encode("utf-8")).hexdigest()
    deployment_root = request.active_config_path.parent
    run_root = deployment_root / "fxpm-production-runs" / f"run-{digest}"
    run_root.mkdir(parents=True, exist_ok=True)
    source_path = run_root / "current-refit" / "physical-ledger.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(raw)
    assembly_path = deployment_root / f"fxpm-authority-{digest}"
    assembly_path.mkdir(exist_ok=True)
    return SimpleNamespace(
        generation_id=request.generation_id,
        staged_ledger_bytes=raw,
        staged_ledger_content_sha256=_sha256(raw),
        physical_ledger_sha256="b" * 64,
        capital_decision_sha256="c" * 64,
        selected_policy_point_id="policy-selected",
        run_root=run_root,
        staged_ledger_source_path=source_path,
        manifest_fields=lambda: {
            "authority_chain_receipt": {
                "generation_id": request.generation_id,
                "ledger_sha256": _sha256(raw),
            },
            "authority_artifacts": {"raw": {"sha256": "d" * 64}},
            "authority_assembly_path": str(assembly_path),
            "historical_run_receipt_sha256": "e" * 64,
            "historical_process_decision_sha256": "f" * 64,
            "current_roster_sha256": "1" * 64,
            "capital_decision_sha256": "c" * 64,
            "selected_policy_point_id": "policy-selected",
            "production_authority_runner_schema": (production_runner.RUNNER_SCHEMA),
        },
    )


def _promoted_manifest_for_fake_result(result, raw: bytes) -> dict:
    fields = result.manifest_fields()
    return {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": result.generation_id,
        "generation_status": "PROMOTED",
        "ledger_sha256": _sha256(raw),
        "authority_chain_receipt": fields["authority_chain_receipt"],
        "ledger_status": {"complete_current_contract_universe": True},
        "production_authority_run_root": str(result.run_root.resolve()),
        "authority_assembly_path": fields["authority_assembly_path"],
        "production_staged_ledger_source_path": str(
            result.staged_ledger_source_path.resolve()
        ),
        "production_retention_policy": {
            "schema_version": "fxpm.production-retention-policy.v1",
            "maximum_complete_generations": 2,
            "keep_current_and_previous": True,
        },
    }


def _prepare_retention_generation(
    manager: PortfolioManager,
    *,
    generation_id: str,
    raw: bytes,
) -> tuple[Path, Path, dict, Path, Path]:
    ledger_path = Path(manager.config_file)
    deployment_root = Path(manager.config._deployment_config_path).parent  # noqa: SLF001
    digest = hashlib.sha256(generation_id.encode("utf-8")).hexdigest()
    run_root = deployment_root / "fxpm-production-runs" / f"run-{digest}"
    run_root.mkdir(parents=True)
    source_path = run_root / "current-refit" / "physical-ledger.json"
    source_path.parent.mkdir()
    source_path.write_bytes(raw)
    assembly_path = deployment_root / f"fxpm-authority-{digest}"
    assembly_path.mkdir()
    stage_path, stage_manifest_path, active_path = (  # noqa: SLF001
        manager._optimization_generation_paths()
    )
    stage_path.write_bytes(raw)
    stage_manifest_path.write_text("{}", encoding="utf-8")
    active_raw = active_path.read_bytes() if active_path.exists() else b""
    active = json.loads(active_raw) if active_raw else {}
    previous = manager._production_generation_retention_descriptor(  # noqa: SLF001
        active,
        deployment_root=deployment_root,
    )
    manifest = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": generation_id,
        "generation_status": "COMPLETE",
        "base_ledger_sha256": _sha256(ledger_path.read_bytes()),
        "base_active_manifest_sha256": _sha256(active_raw),
        "ledger_sha256": _sha256(raw),
        "authority_chain_receipt": {
            "generation_id": generation_id,
            "ledger_sha256": _sha256(raw),
        },
        "ledger_status": {"complete_current_contract_universe": True},
        "production_authority_run_root": str(run_root.resolve()),
        "authority_assembly_path": str(assembly_path.resolve()),
        "production_staged_ledger_source_path": str(source_path.resolve()),
        "production_retention_policy": {
            "schema_version": "fxpm.production-retention-policy.v1",
            "maximum_complete_generations": 2,
            "keep_current_and_previous": True,
        },
    }
    if previous is not None:
        snapshot_sha = _sha256(active_raw)
        snapshot = run_root / "rollback" / f"manifest-{snapshot_sha}.json"
        snapshot.parent.mkdir()
        snapshot.write_bytes(active_raw)
        manifest["previous_production_generation"] = {
            **previous,
            "previous_active_manifest_snapshot_path": str(snapshot.resolve()),
            "previous_active_manifest_snapshot_sha256": snapshot_sha,
        }
    if "previous_production_generation" in active:
        obsolete = manager._production_generation_retention_descriptor(  # noqa: SLF001
            active["previous_production_generation"],
            deployment_root=deployment_root,
        )
        assert obsolete is not None
        manifest["obsolete_production_generation"] = obsolete
    return stage_path, stage_manifest_path, manifest, run_root, assembly_path


def _promote_retention_generation(
    manager: PortfolioManager,
    *,
    generation_id: str,
    raw: bytes,
) -> tuple[Path, Path]:
    stage, stage_manifest, manifest, run_root, assembly = (
        _prepare_retention_generation(
            manager,
            generation_id=generation_id,
            raw=raw,
        )
    )
    manager._promote_staged_generation(  # noqa: SLF001
        stage,
        stage_manifest,
        manifest,
        universe=None,
    )
    return run_root, assembly


def test_production_cutover_runs_once_stages_exact_sparse_bytes_and_promotes(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    prior = ledger_path.read_bytes()
    raw = _sparse_current_ledger()
    calls = []

    def run(request):
        calls.append(request)
        return _fake_production_result(request, raw)

    promoted = []

    def promote(*, stage_path, stage_manifest_path, staged_bytes, manifest, universe):
        del stage_manifest_path
        promoted.append((staged_bytes, dict(manifest), universe))
        assert ledger_path.read_bytes() == prior
        ledger_path.write_bytes(staged_bytes)

    monkeypatch.setattr(production_runner, "run_production_optimization", run)
    manager._stage_and_promote_exact_production_generation = promote  # noqa: SLF001

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=(book.PINNED_SYMBOL_ORDER[0],),
        )
    )

    assert len(calls) == 1
    assert tuple(calls[0].requested_symbols) == tuple(book.PINNED_SYMBOL_ORDER)
    assert calls[0].authorization_scope == "DEMO_PROBATION"
    assert promoted[0][0] == raw
    assert promoted[0][1]["ledger_status"] == {
        "schema_version": "fxpm.production-ledger-status.v2",
        "complete_current_contract_universe": True,
        "coverage_authority": "RAW_REVERIFIED_COMPLETE_100x6x4_BOOK",
        "configured_symbol_count": 100,
        "configured_cell_count": 2400,
        "physical_winner_symbol_count": 1,
        "physical_winner_cell_count": 1,
        "sparse_physical_winner_book": True,
    }
    assert ledger_path.read_bytes() == raw
    assert outcome.status == "PROMOTED"
    assert outcome.promoted is True
    assert set(outcome.results) == {book.PINNED_SYMBOL_ORDER[0]}


def test_retention_keeps_current_and_previous_without_deleting_inflight_work(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.ledger = ConfigLedger(str(ledger_path))
    manager._last_promotion_authorization = None
    raw = _sparse_current_ledger()

    run_one, assembly_one = _promote_retention_generation(
        manager, generation_id="retention-one", raw=raw
    )
    run_two, assembly_two = _promote_retention_generation(
        manager, generation_id="retention-two", raw=raw
    )
    inflight_digest = "c" * 64
    inflight_run = (
        tmp_path / "fxpm-production-runs" / f"run-{inflight_digest}"
    )
    inflight_assembly = tmp_path / f"fxpm-authority-{inflight_digest}"
    inflight_run.mkdir()
    inflight_assembly.mkdir()
    run_three, assembly_three = _promote_retention_generation(
        manager, generation_id="retention-three", raw=raw
    )

    assert not run_one.exists()
    assert not assembly_one.exists()
    assert run_two.is_dir()
    assert assembly_two.is_dir()
    assert run_three.is_dir()
    assert assembly_three.is_dir()
    assert inflight_run.is_dir()
    assert inflight_assembly.is_dir()
    active_path = manager._optimization_generation_paths()[2]  # noqa: SLF001
    active = json.loads(active_path.read_text(encoding="utf-8"))
    previous = active["previous_production_generation"]
    assert previous["generation_id"] == "retention-two"
    snapshot = Path(previous["previous_active_manifest_snapshot_path"])
    assert _sha256(snapshot.read_bytes()) == previous[
        "previous_active_manifest_snapshot_sha256"
    ]
    snap_manifest = json.loads(snapshot.read_text(encoding="utf-8"))
    assert snap_manifest["generation_id"] == "retention-two"
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_recovery_discharge_prunes_exact_obsolete_generation(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.ledger = ConfigLedger(str(ledger_path))
    manager._last_promotion_authorization = None
    raw = _sparse_current_ledger()
    run_one, assembly_one = _promote_retention_generation(
        manager, generation_id="recovery-one", raw=raw
    )
    _promote_retention_generation(
        manager, generation_id="recovery-two", raw=raw
    )
    stage, stage_manifest, manifest, _run_three, _assembly_three = (
        _prepare_retention_generation(
            manager,
            generation_id="recovery-three",
            raw=raw,
        )
    )
    active_path = manager._optimization_generation_paths()[2]  # noqa: SLF001
    original_write = manager._atomic_write_json  # noqa: SLF001

    def fail_active_once(path, payload):
        if Path(path) == active_path and payload.get("generation_status") == "PROMOTED":
            raise OSError("simulated active-manifest crash")
        original_write(path, payload)

    manager._atomic_write_json = fail_active_once  # noqa: SLF001
    with pytest.raises(OSError, match="simulated active-manifest crash"):
        manager._promote_staged_generation(  # noqa: SLF001
            stage,
            stage_manifest,
            manifest,
            universe=None,
        )
    assert manager._optimization_promotion_journal_path().exists()  # noqa: SLF001
    assert run_one.exists()
    assert assembly_one.exists()

    manager._atomic_write_json = original_write  # noqa: SLF001
    assert manager._reconcile_interrupted_promotion() == "COMPLETED"  # noqa: SLF001
    assert not run_one.exists()
    assert not assembly_one.exists()
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001
    active = json.loads(active_path.read_text(encoding="utf-8"))
    assert active["generation_id"] == "recovery-three"


def test_retention_rejects_unrelated_direct_child_before_deletion(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.ledger = ConfigLedger(str(ledger_path))
    manager._last_promotion_authorization = None
    raw = _sparse_current_ledger()
    _promote_retention_generation(
        manager, generation_id="unrelated-one", raw=raw
    )
    _promote_retention_generation(
        manager, generation_id="unrelated-two", raw=raw
    )
    stage, stage_manifest, manifest, _run, _assembly = (
        _prepare_retention_generation(
            manager,
            generation_id="unrelated-three",
            raw=raw,
        )
    )
    unrelated = tmp_path / "important-unrelated-data"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("keep", encoding="utf-8")
    manifest["obsolete_production_generation"] = {
        **manifest["obsolete_production_generation"],
        "authority_assembly_path": str(unrelated.resolve()),
    }

    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_RETENTION_OBSOLETE_SNAPSHOT_BINDING_MISMATCH",
    ):
        manager._promote_staged_generation(  # noqa: SLF001
            stage,
            stage_manifest,
            manifest,
            universe=None,
        )

    assert unrelated.is_dir()
    assert (unrelated / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_retention_recovery_is_idempotent_after_partial_cleanup(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.ledger = ConfigLedger(str(ledger_path))
    manager._last_promotion_authorization = None
    raw = _sparse_current_ledger()
    run_one, assembly_one = _promote_retention_generation(
        manager, generation_id="partial-one", raw=raw
    )
    _promote_retention_generation(
        manager, generation_id="partial-two", raw=raw
    )
    stage, stage_manifest, manifest, _run, _assembly = (
        _prepare_retention_generation(
            manager,
            generation_id="partial-three",
            raw=raw,
        )
    )
    original_remove = manager._remove_owned_generation_directory  # noqa: SLF001

    def fail_after_first(path, parent):
        if Path(path) == assembly_one:
            original_remove(path, parent)
            return
        if Path(path) == run_one:
            raise OSError("simulated cleanup crash")
        original_remove(path, parent)

    manager._remove_owned_generation_directory = fail_after_first  # noqa: SLF001
    with pytest.raises(OSError, match="simulated cleanup crash"):
        manager._promote_staged_generation(  # noqa: SLF001
            stage,
            stage_manifest,
            manifest,
            universe=None,
        )
    assert not assembly_one.exists()
    assert run_one.exists()
    assert manager._optimization_promotion_journal_path().exists()  # noqa: SLF001

    manager._remove_owned_generation_directory = original_remove  # noqa: SLF001
    assert manager._reconcile_interrupted_promotion() == "COMPLETED"  # noqa: SLF001
    assert not assembly_one.exists()
    assert not run_one.exists()
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_verified_rollback_c_to_b_then_d_preserves_bounded_recovery_chain(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.ledger = ConfigLedger(str(ledger_path))
    manager.symbol_configs = manager.ledger.get_all_configs()
    manager._last_promotion_authorization = None
    raw_a = _sparse_current_ledger(10)
    raw_b = _sparse_current_ledger(20)
    raw_c = _sparse_current_ledger(30)
    raw_d = _sparse_current_ledger(40)

    run_a, assembly_a = _promote_retention_generation(
        manager, generation_id="rollback-a", raw=raw_a
    )
    run_b, assembly_b = _promote_retention_generation(
        manager, generation_id="rollback-b", raw=raw_b
    )
    run_c, assembly_c = _promote_retention_generation(
        manager, generation_id="rollback-c", raw=raw_c
    )
    assert not run_a.exists()
    assert not assembly_a.exists()

    result = manager.rollback_to_previous_production_generation()

    assert result["status"] == "ROLLED_BACK_TO_PREVIOUS"
    assert result["rolled_back_from_generation_id"] == "rollback-c"
    assert result["active_generation_id"] == "rollback-b"
    assert result["ledger_changed"] is True
    assert ledger_path.read_bytes() == raw_b
    active_path = manager._optimization_generation_paths()[2]  # noqa: SLF001
    rolled_back = json.loads(active_path.read_text(encoding="utf-8"))
    assert rolled_back["generation_id"] == "rollback-b"
    assert rolled_back["previous_production_generation"]["generation_id"] == (
        "rollback-c"
    )
    assert run_b.is_dir() and assembly_b.is_dir()
    assert run_c.is_dir() and assembly_c.is_dir()

    run_d, assembly_d = _promote_retention_generation(
        manager, generation_id="rollback-d", raw=raw_d
    )

    assert ledger_path.read_bytes() == raw_d
    final = json.loads(active_path.read_text(encoding="utf-8"))
    assert final["generation_id"] == "rollback-d"
    assert final["previous_production_generation"]["generation_id"] == "rollback-b"
    assert run_d.is_dir() and assembly_d.is_dir()
    assert run_b.is_dir() and assembly_b.is_dir()
    assert not run_c.exists()
    assert not assembly_c.exists()
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_production_cutover_failure_preserves_last_good(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    prior = ledger_path.read_bytes()
    promoted = []
    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: (_ for _ in ()).throw(RuntimeError("anchor failed")),
    )
    manager._promote_staged_generation = lambda *args: promoted.append(args)  # noqa: SLF001

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_TECHNICAL"
    assert promoted == []
    assert ledger_path.read_bytes() == prior
    assert (
        "PRODUCTION_AUTHORITY_RUN_FAILED"
        in outcome.technical_failures["__GENERATION__"]
    )


def test_production_inner_recovery_uses_fail_fast_lock(
    tmp_path: Path,
) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    observed: dict[str, object] = {}

    def stop_after_recovery(**kwargs):
        observed.update(kwargs)
        raise RuntimeError("stop-after-recovery")

    manager._reconcile_interrupted_promotion = stop_after_recovery  # type: ignore[method-assign]  # noqa: SLF001

    with pytest.raises(RuntimeError, match="stop-after-recovery"):
        manager._run_production_authority_generation(  # noqa: SLF001
            OptimizationGenerationRequest(
                kind="INITIAL",
                target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
            )
        )

    assert observed == {"lock_timeout_seconds": 0.0}


def test_supervisor_generation_identity_reaches_exact_runner_request(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manager, _ledger_path = _production_cutover_manager(tmp_path)
    generation_id = "9" * 64
    observed = {}

    def stop_after_capture(request):
        observed["generation_id"] = request.generation_id
        raise production_runner.ProductionOptimizationNotPromotable(
            "CURRENT_REFIT_NOT_PROMOTABLE",
            ("TEST_CAPTURE_ONLY",),
        )

    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        stop_after_capture,
    )
    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
            supervised_generation_id=generation_id,
        )
    )

    assert observed["generation_id"] == generation_id
    assert outcome.generation_id == generation_id
    assert outcome.status == "NOT_PROMOTABLE"


def test_production_non_promotable_is_not_a_technical_failure(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    prior = ledger_path.read_bytes()
    active_manifest_path = manager._optimization_generation_paths()[2]  # noqa: SLF001
    prior_active = (
        active_manifest_path.read_bytes() if active_manifest_path.exists() else b""
    )
    promoted = []
    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: (_ for _ in ()).throw(
            production_runner.ProductionOptimizationNotPromotable(
                "CURRENT_REFIT_NOT_PROMOTABLE",
                (
                    "CURRENT_BOOK_HAS_NO_ELIGIBLE_WINNERS",
                    "NO_LOCAL_WINNERS",
                ),
            )
        ),
    )
    manager._promote_staged_generation = lambda *args: promoted.append(args)  # noqa: SLF001

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "NOT_PROMOTABLE"
    assert outcome.technical_failures == {}
    assert outcome.non_promotable == {
        "code": "CURRENT_REFIT_NOT_PROMOTABLE",
        "details": [
            "CURRENT_BOOK_HAS_NO_ELIGIBLE_WINNERS",
            "NO_LOCAL_WINNERS",
        ],
        "preserved_last_good_ledger": True,
        "confers_publication_or_trading_authority": False,
    }
    assert promoted == []
    assert outcome.promoted is False
    assert outcome.ledger_changed is False
    assert ledger_path.read_bytes() == prior
    assert (
        active_manifest_path.read_bytes() if active_manifest_path.exists() else b""
    ) == prior_active
    manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
    assert manifest["generation_status"] == "NOT_PROMOTABLE"
    assert manifest["technical_failures"] == {}
    assert manifest["non_promotable"] == outcome.non_promotable


def test_post_run_verification_failure_is_typed_and_discards_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    prior = ledger_path.read_bytes()
    raw = _sparse_current_ledger()
    discarded = []

    def run(request):
        result = _fake_production_result(request, raw)
        result.generation_id = "wrong-generation"
        return result

    monkeypatch.setattr(production_runner, "run_production_optimization", run)
    monkeypatch.setattr(
        production_runner,
        "discard_unpublished_production_result",
        lambda result: discarded.append(result) or True,
    )

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_POST_RUN_TECHNICAL"
    assert outcome.promoted is False
    assert outcome.ledger_changed is False
    assert ledger_path.read_bytes() == prior
    assert len(discarded) == 1
    manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
    assert manifest["generation_status"] == "FAILED_POST_RUN_TECHNICAL"
    assert manifest["unpublished_artifact_cleanup"] == "DISCARDED"
    assert "GENERATION_ID_MISMATCH" in manifest["technical_failures"]["__POST_RUN__"]
    assert manager.last_optimization_generation == outcome


def test_failed_concurrent_generation_cannot_delete_valid_promoter_stage(
    tmp_path: Path, monkeypatch
) -> None:
    first, ledger_path = _production_cutover_manager(tmp_path)
    second = PortfolioManager.__new__(PortfolioManager)
    second.config = first.config
    second.symbols = list(first.symbols)
    second.config_file = first.config_file
    second.last_optimization_generation = None
    second._native_universe_for_optimization = (  # noqa: SLF001
        first._native_universe_for_optimization
    )
    for manager in (first, second):
        manager.ledger = ConfigLedger(str(ledger_path))
        manager.symbol_configs = manager.ledger.get_all_configs()
        manager._last_promotion_authorization = None

    raw = _sparse_current_ledger()
    runner_lock = threading.Lock()
    runner_calls = 0
    first_in_runner = threading.Event()
    second_in_runner = threading.Event()
    first_in_verifier = threading.Event()
    second_discarded = threading.Event()
    release_first = threading.Event()

    def run(request):
        nonlocal runner_calls
        with runner_lock:
            call_index = runner_calls
            runner_calls += 1
        if call_index == 0:
            first_in_runner.set()
            assert second_in_runner.wait(timeout=10)
            return _fake_production_result(request, raw)
        second_in_runner.set()
        assert first_in_verifier.wait(timeout=10)
        result = _fake_production_result(request, raw)
        result.generation_id = "wrong-concurrent-generation"
        return result

    def verifier(*args, **kwargs):
        del args, kwargs
        first_in_verifier.set()
        assert release_first.wait(timeout=10)

    def discard(_result):
        second_discarded.set()
        return True

    monkeypatch.setattr(production_runner, "run_production_optimization", run)
    monkeypatch.setattr(
        production_runner, "discard_unpublished_production_result", discard
    )
    first._verify_staged_authority_before_cas = verifier  # noqa: SLF001
    request = OptimizationGenerationRequest(
        kind="INITIAL",
        target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            first._run_governed_symbol_generation,  # noqa: SLF001
            request,
        )
        assert first_in_runner.wait(timeout=10)
        second_future = executor.submit(
            second._run_governed_symbol_generation,  # noqa: SLF001
            request,
        )
        assert first_in_verifier.wait(timeout=10)
        assert second_discarded.wait(timeout=10)
        stage_path = first._optimization_generation_paths()[0]  # noqa: SLF001
        assert stage_path.read_bytes() == raw
        release_first.set()
        first_outcome = first_future.result(timeout=20)
        second_outcome = second_future.result(timeout=20)

    assert first_outcome.status == "PROMOTED"
    assert first_outcome.promoted is True
    assert second_outcome.status == "FAILED_POST_RUN_SUPERSEDED"
    assert second_outcome.promoted is False
    assert ledger_path.read_bytes() == raw
    active = json.loads(
        first._optimization_generation_paths()[2].read_text(encoding="utf-8")  # noqa: SLF001
    )
    assert active["generation_id"] == first_outcome.generation_id
    failure = json.loads(Path(second_outcome.manifest_path).read_text(encoding="utf-8"))
    assert failure["generation_status"] == "FAILED_POST_RUN_SUPERSEDED"
    assert failure["superseded_by_ledger_sha256"] == _sha256(raw)


def test_byte_identical_pre_cas_failure_is_not_misreported_as_promotion(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    ledger_path.write_bytes(raw)

    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: _fake_production_result(request, raw),
    )
    manager._stage_and_promote_exact_production_generation = (  # noqa: SLF001
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("pre-CAS raw gate failed")
        )
    )

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_PROMOTION_TECHNICAL"
    assert outcome.promoted is False
    assert outcome.ledger_changed is False
    assert outcome.promoted_ledger_sha256 == ""
    assert ledger_path.read_bytes() == raw
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_byte_identical_authorized_generation_reports_unchanged_book(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    ledger_path.write_bytes(raw)

    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: _fake_production_result(request, raw),
    )

    def promote(*, stage_path, stage_manifest_path, staged_bytes, manifest, universe):
        del stage_path, stage_manifest_path, manifest, universe
        assert staged_bytes == raw

    manager._stage_and_promote_exact_production_generation = promote  # noqa: SLF001

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "PROMOTED"
    assert outcome.promoted is True
    assert outcome.ledger_changed is False
    assert outcome.promoted_ledger_sha256 == _sha256(raw)
    assert ledger_path.read_bytes() == raw


def test_stale_same_hash_journal_cannot_authorize_new_generation(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    ledger_path.write_bytes(raw)

    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: _fake_production_result(request, raw),
    )

    def stale_journal(
        *, stage_path, stage_manifest_path, staged_bytes, manifest, universe
    ):
        del stage_path, staged_bytes, universe
        stale = {
            **manifest,
            "generation_id": "prior-generation",
            "generation_status": "PROMOTED",
            "authority_chain_receipt": {
                **manifest["authority_chain_receipt"],
                "generation_id": "prior-generation",
            },
        }
        manager._write_promotion_journal(  # noqa: SLF001
            base_ledger_sha256=manifest["base_ledger_sha256"],
            target_ledger_sha256=manifest["ledger_sha256"],
            stage_manifest_path=stage_manifest_path,
            promoted_manifest=stale,
        )
        raise RuntimeError("stale journal injected")

    manager._stage_and_promote_exact_production_generation = (  # noqa: SLF001
        stale_journal
    )

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_PROMOTION_TECHNICAL"
    assert outcome.promoted is False
    assert outcome.ledger_changed is False
    assert ledger_path.read_bytes() == raw
    assert manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_unrelated_journal_must_be_coherent_and_disjoint_before_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    unrelated_request = SimpleNamespace(
        generation_id="coherent-unrelated-a",
        active_config_path=Path(manager.config._deployment_config_path),  # noqa: SLF001
    )
    unrelated = _fake_production_result(unrelated_request, raw)
    unrelated_manifest = _promoted_manifest_for_fake_result(unrelated, raw)
    candidate = []

    def run(request):
        result = _fake_production_result(request, raw)
        candidate.append(result)
        return result

    def inject_unrelated_journal(
        *, stage_path, stage_manifest_path, staged_bytes, manifest, universe
    ):
        del stage_path, staged_bytes, universe
        manager._write_promotion_journal(  # noqa: SLF001
            base_ledger_sha256=manifest["base_ledger_sha256"],
            target_ledger_sha256=_sha256(raw),
            stage_manifest_path=stage_manifest_path,
            promoted_manifest=unrelated_manifest,
        )
        raise RuntimeError("coherent unrelated journal injected")

    discarded = []

    def discard(result):
        discarded.append(result.generation_id)
        shutil.rmtree(result.run_root)
        shutil.rmtree(Path(result.manifest_fields()["authority_assembly_path"]))
        return True

    monkeypatch.setattr(production_runner, "run_production_optimization", run)
    monkeypatch.setattr(
        production_runner, "discard_unpublished_production_result", discard
    )
    manager._stage_and_promote_exact_production_generation = (  # noqa: SLF001
        inject_unrelated_journal
    )

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="INITIAL",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_PROMOTION_TECHNICAL"
    assert discarded == [candidate[0].generation_id]
    assert not candidate[0].run_root.exists()
    assert unrelated.run_root.is_dir()
    assert Path(unrelated_manifest["authority_assembly_path"]).is_dir()
    assert ledger_path.read_bytes() == b'{"LAST_GOOD":{"value":1}}'
    assert manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_unrelated_journal_aliasing_candidate_paths_is_ambiguous(
    tmp_path: Path,
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    raw = _sparse_current_ledger()
    candidate_request = SimpleNamespace(
        generation_id="journal-candidate-b",
        active_config_path=Path(manager.config._deployment_config_path),  # noqa: SLF001
    )
    candidate = _fake_production_result(candidate_request, raw)
    alias_manifest = {
        **_promoted_manifest_for_fake_result(candidate, raw),
        "generation_id": "journal-unrelated-a",
        "authority_chain_receipt": {
            "generation_id": "journal-unrelated-a",
            "ledger_sha256": _sha256(raw),
        },
    }
    _stage, stage_manifest, _active = manager._optimization_generation_paths()  # noqa: SLF001
    manager._write_promotion_journal(  # noqa: SLF001
        base_ledger_sha256=_sha256(ledger_path.read_bytes()),
        target_ledger_sha256=_sha256(raw),
        stage_manifest_path=stage_manifest,
        promoted_manifest=alias_manifest,
    )

    classification = manager._promotion_journal_reference_locked(  # noqa: SLF001
        generation_id=candidate.generation_id,
        ledger_sha256=_sha256(raw),
        run_root=candidate.run_root,
        authority_assembly_path=Path(
            candidate.manifest_fields()["authority_assembly_path"]
        ),
        staged_ledger_source_path=candidate.staged_ledger_source_path,
    )

    assert classification == "AMBIGUOUS"
    assert candidate.run_root.is_dir()
    assert Path(candidate.manifest_fields()["authority_assembly_path"]).is_dir()


def test_new_generation_reconciles_existing_promotion_journal_first(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    base = ledger_path.read_bytes()
    promoted = _sparse_current_ledger()
    _stage, stage_manifest, active_manifest = (  # noqa: SLF001
        manager._optimization_generation_paths()
    )
    stage_manifest.write_text("{}", encoding="utf-8")
    prior_generation = "interrupted-prior-generation"
    prior_manifest = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": prior_generation,
        "generation_status": "PROMOTED",
        "base_ledger_sha256": _sha256(base),
        "ledger_sha256": _sha256(promoted),
    }
    manager._write_promotion_journal(  # noqa: SLF001
        base_ledger_sha256=_sha256(base),
        target_ledger_sha256=_sha256(promoted),
        stage_manifest_path=stage_manifest,
        promoted_manifest=prior_manifest,
    )
    ledger_path.write_bytes(promoted)

    monkeypatch.setattr(
        production_runner,
        "run_production_optimization",
        lambda request: (_ for _ in ()).throw(RuntimeError("stop after reconcile")),
    )

    outcome = manager._run_governed_symbol_generation(  # noqa: SLF001
        OptimizationGenerationRequest(
            kind="SCHEDULED_RETRAIN",
            target_symbols=tuple(book.PINNED_SYMBOL_ORDER),
        )
    )

    assert outcome.status == "FAILED_TECHNICAL"
    assert outcome.base_ledger_sha256 == _sha256(promoted)
    assert ledger_path.read_bytes() == promoted
    assert json.loads(active_manifest.read_text(encoding="utf-8"))[
        "generation_id"
    ] == prior_generation
    assert not manager._optimization_promotion_journal_path().exists()  # noqa: SLF001


def test_existing_promotion_journal_is_never_overwritten(tmp_path: Path) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    _stage, stage_manifest, _active = manager._optimization_generation_paths()  # noqa: SLF001
    first = {
        "generation_id": "first",
        "generation_status": "PROMOTED",
        "ledger_sha256": _sha256(ledger_path.read_bytes()),
    }
    journal = manager._write_promotion_journal(  # noqa: SLF001
        base_ledger_sha256=_sha256(ledger_path.read_bytes()),
        target_ledger_sha256=_sha256(ledger_path.read_bytes()),
        stage_manifest_path=stage_manifest,
        promoted_manifest=first,
    )
    frozen = journal.read_bytes()

    with pytest.raises(
        RuntimeError, match="OPTIMIZATION_PROMOTION_JOURNAL_ALREADY_EXISTS"
    ):
        manager._write_promotion_journal(  # noqa: SLF001
            base_ledger_sha256=_sha256(ledger_path.read_bytes()),
            target_ledger_sha256=_sha256(ledger_path.read_bytes()),
            stage_manifest_path=stage_manifest,
            promoted_manifest={**first, "generation_id": "second"},
        )

    assert journal.read_bytes() == frozen


def test_replay_apply_cannot_bypass_governed_ledger_with_explicit_override(
    tmp_path: Path, monkeypatch
) -> None:
    manager, ledger_path = _production_cutover_manager(tmp_path)
    manager.config.production_split_mode = "research"
    manager.config.research_mode = True
    manager.config.production_replay_audit_enabled = True
    manager.config.production_replay_audit_mode = "apply"
    manager.config.production_replay_audit_out_dir = str(tmp_path / "audit")
    ledger_payload = {
        "FORD": {
            "symbol": "FORD",
            "timeframe": "H4",
            "strategy_name": "DoubleEMATrendStrategy",
            "promotion_status": "PROMOTED",
            "regime_configs": {
                "H4": {
                    "CHOP": {
                        "strategy_name": "DoubleEMATrendStrategy",
                        "parameters": {},
                        "promotion_status": "PROMOTED",
                    }
                }
            },
        }
    }
    ledger_path.write_text(json.dumps(ledger_payload), encoding="utf-8")
    manager.ledger = ConfigLedger(str(ledger_path))
    active_path = manager._optimization_generation_paths()[2]  # noqa: SLF001
    active_payload = {
        "schema_version": "fxpm.optimization-generation.v2",
        "generation_id": "governed-replay",
        "generation_status": "PROMOTED",
        "ledger_sha256": _sha256(ledger_path.read_bytes()),
        "authority_chain_receipt": {"generation_id": "governed-replay"},
        "ledger_status": {"complete_current_contract_universe": True},
    }
    active_path.write_text(json.dumps(active_payload), encoding="utf-8")
    before_ledger = ledger_path.read_bytes()
    before_manifest = active_path.read_bytes()
    monkeypatch.setattr(
        replay_tools,
        "replay_cell",
        lambda *args, **kwargs: {
            "error": "",
            "starting_balance": 10000.0,
            "net_profit": -100.0,
            "return_pct": -1.0,
            "profit_factor": "0.5",
            "max_drawdown_pct": 5.0,
            "total_trades": 50,
            "expectancy_pips": -1.0,
            "avg_mae_r": 1.0,
        },
    )

    with pytest.raises(
        RuntimeError,
        match="PRODUCTION_REPLAY_AUDIT_APPLY_FORBIDDEN_GOVERNED_LEDGER",
    ):
        manager._run_production_replay_audit(  # noqa: SLF001
            ledger_override=ledger_path,
            mutate_manager=False,
        )

    assert ledger_path.read_bytes() == before_ledger
    assert active_path.read_bytes() == before_manifest


def test_production_cutover_classification_preserves_explicit_research(
    tmp_path: Path,
) -> None:
    manager, _ = _production_cutover_manager(tmp_path)
    assert manager._requires_production_authority_runner() is True  # noqa: SLF001
    manager.config.production_split_mode = "bleeding_edge"
    manager.config.research_mode = True
    assert manager._requires_production_authority_runner() is False  # noqa: SLF001
    manager.config._funded_live_execution = True  # noqa: SLF001
    assert manager._requires_production_authority_runner() is True  # noqa: SLF001
