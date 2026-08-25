import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_current_refit_production as production
import pm_pipeline as pipeline
import pm_portfolio_replay as replay
from pm_current_refit_artifacts import spool_current_refit_search_attempt_v2
from pm_core import PipelineConfig
from pm_strategies import StrategyRegistry
from tests.test_pipeline_current_refit_executor import _production_fixture


def _sha(character: str) -> str:
    return character * 64


def _candidate() -> replay.CandidateTrade:
    return replay.CandidateTrade(
        candidate_id="candidate-1",
        cell_id="EURUSD|H1|TREND",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="EMACrossoverStrategy",
        promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        asset_class="fx",
        risk_groups=(),
        entry_time="2025-01-01T00:00:00Z",
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=1_000.0,
        volume_min=0.01,
        volume_max=10.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(
            replay.ExitLeg(
                exit_time="2025-01-02T00:00:00Z",
                fraction=1.0,
                net_pnl_per_entry_lot=200.0,
                price_pnl_per_entry_lot=200.0,
                swap_per_entry_lot=0.0,
                exit_reason="target",
            ),
        ),
        marks=(
            replay.MarkPoint(
                time="2025-01-01T23:59:00Z",
                pnl_per_current_lot=80.0,
                observation_time="2025-01-01T23:59:00Z",
            ),
        ),
        source_schema=replay.PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
        margin_model="broker_snapshot",
    ).normalized()


def _captured_from_raw_manifest(
    manifest: dict,
) -> tuple[production.CapturedArtifact, ...]:
    by_format = {
        pipeline.CURRENT_PROCESS_BOOK_ARTIFACT_VERSION: production.ART_ACCEPTED_BOOK,
        pipeline.CURRENT_REFIT_SOURCE_BUNDLE_VERSION: production.ART_SOURCE_BUNDLE,
        production.execution.CURRENT_REGIME_PARAMS_BUNDLE_VERSION: production.ART_REGIME_BUNDLE,
        pipeline.CURRENT_REFIT_HORIZON_BUNDLE_VERSION: production.ART_HORIZON_BUNDLE,
        pipeline.CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION: production.ART_SOURCE,
        pipeline.CURRENT_REFIT_BAR_FILE_VERSION: production.ART_BARS,
        pipeline.CURRENT_REFIT_PROCESS_VERSION: production.ART_WINNER_PROCESS,
        pipeline.CURRENT_REFIT_HORIZON_VERSION: production.ART_HORIZON_PROCESS,
        pipeline.CURRENT_REFIT_TRIAL_VERSION: production.ART_TRIAL,
        pipeline.CURRENT_REFIT_RECIPE_VERSION: production.ART_RECIPE,
        pipeline.CURRENT_REFIT_BOOK_ENTRY_VERSION: production.ART_BOOK_ENTRY,
    }
    return tuple(
        production.CapturedArtifact(
            artifact_id=descriptor["artifact_id"],
            artifact_type=by_format[descriptor["format_version"]],
            relative_path=descriptor["relative_path"],
            size_bytes=descriptor["size_bytes"],
            content_sha256=descriptor["content_sha256"],
            spool_format=(
                "csv"
                if descriptor["format_version"]
                == pipeline.CURRENT_REFIT_BAR_FILE_VERSION
                else spool.CANONICAL_JSON_FORMAT
            ),
            semantic_sha256=descriptor["semantic_sha256"],
            format_version=descriptor["format_version"],
        )
        for descriptor in manifest["artifact_descriptors"]
    )


def test_raw_adapter_reassembles_and_reverifies_real_executor_inputs(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "raw"
    receipt, source_manifest = _production_fixture(artifact_root)

    path = production.assemble_raw_current_refit_manifest(
        current_refit_receipt=receipt,
        artifacts=_captured_from_raw_manifest(source_manifest),
        artifact_root=artifact_root,
        output_root=tmp_path / "manifests",
    )

    verification = production.execution.verify_raw_current_refit_execution(
        receipt,
        path,
        artifact_root=artifact_root,
    )
    assert verification["verified"] is True, verification["errors"]
    assert verification["errors"] == []


def _physical_fixture(tmp_path: Path) -> tuple[dict, dict, dict, Path, Path]:
    artifact_root = tmp_path / "physical-raw"
    receipt, manifest, details = _production_fixture(artifact_root, return_details=True)
    manifest_path = tmp_path / "raw-current-manifest.json"
    manifest_path.write_bytes(
        json.dumps(
            manifest,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    accepted = details["accepted_book_receipt"]
    accepted_payload, _ = production.open_typed_receipt(
        accepted,
        expected_receipt_type=production.CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    )
    book_body = {
        "receipt_sha256": accepted_payload["book_multiplicity_receipt_sha256"],
        "cells": [
            dict(details["book_entries"][timeframe]) for timeframe in ("M5", "D1")
        ],
    }
    return book_body, accepted, receipt, manifest_path, artifact_root


def _scheduled_deadline(receipt: dict) -> datetime:
    payload, _ = production.open_typed_receipt(
        receipt,
        expected_receipt_type=production.CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    completed_at = datetime.fromisoformat(
        str(payload["execution"]["completed_at_utc"]).replace("Z", "+00:00")
    )
    return PipelineConfig().get_next_retrain_at_utc(completed_at)


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_selected_book_cells_preserve_exact_mt5_symbol(symbol: str) -> None:
    selected = production._selected_book_cells(  # noqa: SLF001
        {
            "cells": [
                {
                    "symbol": symbol,
                    "timeframe": "H4",
                    "regime": "TREND",
                    "historical_state": "WINNER",
                }
            ]
        }
    )

    assert set(selected) == {f"{symbol}|H4|TREND"}
    assert selected[f"{symbol}|H4|TREND"]["symbol"] == symbol


def test_physical_current_ledger_is_derived_from_raw_selected_entries_only(
    tmp_path: Path, monkeypatch
) -> None:
    book_body, accepted, receipt, manifest_path, artifact_root = _physical_fixture(
        tmp_path
    )
    monkeypatch.setattr(production, "_owned_book", lambda _value: book_body)

    derived = production.derive_physical_current_refit_ledger(
        current_book_decision=book_body,
        accepted_book_generation_receipt=accepted,
        current_refit_receipt=receipt,
        raw_manifest_path=manifest_path,
        artifact_root=artifact_root,
        output_root=tmp_path / "derived",
        required_valid_until_utc=_scheduled_deadline(receipt),
    )

    assert derived.path.name == (
        f"physical_current_refit_ledger_{derived.content_sha256}.json"
    )
    assert hashlib.sha256(derived.path.read_bytes()).hexdigest() == (
        derived.content_sha256
    )
    assert replay.canonical_json_sha256(derived.payload) == (
        derived.physical_ledger_sha256
    )
    symbol_payload = derived.payload["EURUSD"]
    expected_deadline = _scheduled_deadline(receipt).isoformat()
    assert symbol_payload["valid_until"] == expected_deadline
    assert symbol_payload["artifact_meta"]["valid_until_basis"] == (
        production.SCHEDULE_DEFAULT_VALIDITY_BASIS
    )
    assert (
        symbol_payload["artifact_meta"]["evidence_led_validity_horizon_claimed"]
        is False
    )
    cells = symbol_payload["regime_configs"]
    assert set(cells) == {"M5", "D1"}
    assert {next(iter(regimes)) for regimes in cells.values()} == {"TREND"}
    for regimes in cells.values():
        cell = regimes["TREND"]
        assert cell["promotion_status"] == "PROCESS_VALIDATED_CURRENT_REFIT"
        assert cell["strategy_name"] == "EMACrossoverStrategy"
        assert (
            cell["parameters"]
            == StrategyRegistry.get("EMACrossoverStrategy").get_params()
        )
        binding = cell["artifact_meta"]["current_refit_binding"]
        assert set(binding) == set(production._CURRENT_REFIT_BINDING_FIELDS)
        assert cell["artifact_meta"]["winner_process_candidate_id"]
        assert cell["valid_until"] == expected_deadline
        assert cell["artifact_meta"]["valid_until_basis"] == (
            production.SCHEDULE_DEFAULT_VALIDITY_BASIS
        )
        assert cell["artifact_meta"]["evidence_led_validity_horizon_claimed"] is False
        assert cell["validation_evidence"]["authority_scope"] == ("DEMO_PROBATION_ONLY")


@pytest.mark.parametrize("attack", ["book_entry", "extra_winner"])
def test_physical_current_ledger_rejects_selected_roster_attacks(
    tmp_path: Path, monkeypatch, attack: str
) -> None:
    book_body, accepted, receipt, manifest_path, artifact_root = _physical_fixture(
        tmp_path
    )
    hostile = deepcopy(book_body)
    if attack == "book_entry":
        hostile["cells"][0]["historical_book_entry_sha256"] = _sha("0")
        expected = "PHYSICAL_LEDGER_SELECTED_BOOK_ENTRY_MISMATCH"
    else:
        extra = dict(hostile["cells"][0])
        extra.update(symbol="GBPUSD", cell_id=_sha("1"))
        hostile["cells"].append(extra)
        expected = "PHYSICAL_LEDGER_SELECTED_ROSTER_MISMATCH"
    monkeypatch.setattr(production, "_owned_book", lambda _value: hostile)

    with pytest.raises(production.CurrentRefitProductionCaptureError, match=expected):
        production.derive_physical_current_refit_ledger(
            current_book_decision=hostile,
            accepted_book_generation_receipt=accepted,
            current_refit_receipt=receipt,
            raw_manifest_path=manifest_path,
            artifact_root=artifact_root,
            output_root=tmp_path / "derived-attack",
            required_valid_until_utc=_scheduled_deadline(receipt),
        )


@pytest.mark.parametrize(
    "deadline",
    [datetime(2099, 1, 1), datetime(2000, 1, 1, tzinfo=timezone.utc)],
)
def test_physical_current_ledger_rejects_invalid_required_deadline(
    tmp_path: Path, monkeypatch, deadline: datetime
) -> None:
    book_body, accepted, receipt, manifest_path, artifact_root = _physical_fixture(
        tmp_path
    )
    monkeypatch.setattr(production, "_owned_book", lambda _value: book_body)

    with pytest.raises(
        production.CurrentRefitProductionCaptureError,
        match=(
            "PHYSICAL_LEDGER_VALID_UNTIL_NOT_AWARE_UTC"
            if deadline.tzinfo is None
            else "PHYSICAL_LEDGER_VALID_UNTIL_NOT_AFTER_REFIT"
        ),
    ):
        production.derive_physical_current_refit_ledger(
            current_book_decision=book_body,
            accepted_book_generation_receipt=accepted,
            current_refit_receipt=receipt,
            raw_manifest_path=manifest_path,
            artifact_root=artifact_root,
            output_root=tmp_path / "derived-invalid-deadline",
            required_valid_until_utc=deadline,
        )


def test_current_search_spool_is_explicitly_insufficient_for_production(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    context = spool.freeze_generation_context(
        root,
        generation_id="generation-1",
        native_universe_sha256=_sha("a"),
        config_sha256=_sha("b"),
    )
    sink = spool.open_symbol_sink(
        context,
        "EURUSD",
        manifest_domain=spool.CURRENT_REFIT_MANIFEST_DOMAIN,
    )
    strategy = StrategyRegistry.get("EMACrossoverStrategy")
    spool_current_refit_search_attempt_v2(
        sink,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_id=strategy.name,
        parameters=strategy.get_params(),
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1_000,
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z",
        latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256=_sha("c"),
        source_bar_count=1_000,
        seed=20260714,
    )
    receipt = sink.seal()
    adapter = production.CurrentRefitProductionAdapter(
        production.ProductionCaptureInputs(
            spool_root=root,
            compact_symbol_receipts={"EURUSD": receipt},
            output_root=tmp_path / "out",
            generation_id="generation-1",
            native_universe_sha256=_sha("a"),
            config_sha256=_sha("b"),
            required_valid_until_utc=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
    )
    with pytest.raises(production.CurrentRefitProductionCaptureError) as captured:
        adapter(
            SimpleNamespace(
                to_dict=lambda: {
                    "schema_version": "test",
                    "historical_run_receipt_sha256": _sha("c"),
                    "historical_process_decision_sha256": _sha("d"),
                    "historical_book_receipt_sha256": _sha("e"),
                    "authority_claimed": False,
                }
            )
        )

    assert captured.value.code == "CAPTURE_ARTIFACT_TYPES_MISSING"
    assert production.ART_BARS in captured.value.details
    assert production.ART_CURRENT_BOOK in captured.value.details
    assert production.ART_PORTFOLIO_SOURCE_LEDGER in captured.value.details


def test_current_capture_rejects_a_general_domain_symbol_receipt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    context = spool.freeze_generation_context(
        root,
        generation_id="generation-domain-splice",
        native_universe_sha256=_sha("a"),
        config_sha256=_sha("b"),
    )
    general_sink = spool.open_symbol_sink(context, "EURUSD")
    general_sink.put_json("cell", "unrelated", {"domain": "GENERAL"})
    receipt = general_sink.seal()
    inputs = production.ProductionCaptureInputs(
        spool_root=root,
        compact_symbol_receipts={"EURUSD": receipt},
        output_root=tmp_path / "out",
        generation_id="generation-domain-splice",
        native_universe_sha256=_sha("a"),
        config_sha256=_sha("b"),
        required_valid_until_utc=datetime(2099, 1, 1, tzinfo=timezone.utc),
    )

    with pytest.raises(
        production.CurrentRefitProductionCaptureError,
        match="CAPTURE_SYMBOL_RECEIPT_INVALID",
    ) as captured:
        production._verified_artifacts(inputs)  # noqa: SLF001

    assert captured.value.details == (
        "EURUSD",
        "COMPACT_RECEIPT_MANIFEST_DOMAIN_MISMATCH",
    )


def test_adapter_construction_is_lazy_and_does_not_access_current_evidence(
    tmp_path: Path,
) -> None:
    adapter = production.CurrentRefitProductionAdapter(
        production.ProductionCaptureInputs(
            spool_root=tmp_path / "not-created",
            compact_symbol_receipts={"EURUSD": {"not": "opened"}},
            output_root=tmp_path / "out",
            generation_id="generation-lazy",
            native_universe_sha256=_sha("a"),
            config_sha256=_sha("b"),
            required_valid_until_utc=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
    )

    assert type(adapter) is production.CurrentRefitProductionAdapter
    assert not (tmp_path / "not-created").exists()


def test_candidate_and_portfolio_source_ledgers_are_real_and_request_bound(
    tmp_path: Path,
) -> None:
    staged = tmp_path / "staged.json"
    staged.write_text('{"EURUSD":{"symbol":"EURUSD"}}', encoding="utf-8")
    staged_raw = staged.read_bytes()
    staged_sha = hashlib.sha256(staged_raw).hexdigest()
    physical_sha = replay.canonical_json_sha256(json.loads(staged_raw))
    root = tmp_path / "spool"
    context = spool.freeze_generation_context(
        root,
        generation_id="generation-2",
        native_universe_sha256=_sha("a"),
        config_sha256=_sha("b"),
    )
    sink = spool.open_symbol_sink(context, "__GENERATION__")
    cell_id = "EURUSD|H1|TREND"
    entries = production.spool_current_refit_replay_capture(
        sink,
        staged_ledger_content_sha256=staged_sha,
        physical_ledger_sha256=physical_sha,
        accepted_book_generation_receipt_sha256=_sha("c"),
        current_refit_receipt_sha256=_sha("d"),
        current_refit_execution_sha256=_sha("e"),
        current_cutoff_utc="2026-07-14T00:00:00Z",
        candidates=(_candidate(),),
        replayed_cell_ids=(cell_id,),
        current_refit_execution_access_id="access-1",
        window={
            "mode": "common_intersection",
            "partition": "current_refit_management_replay",
            "evaluation_start": "2024-01-01T00:00:00Z",
            "evaluation_end": "2026-07-14T00:00:00Z",
        },
        native_sources={cell_id: {"source_kind": "broker_native"}},
        valuation_sources={"EURUSD": {"source_kind": "broker_native"}},
    )
    sink.seal()

    def artifact_json(name: str) -> dict:
        descriptor = entries[name]["descriptor"]
        return json.loads((root / descriptor["relative_path"]).read_text("utf-8"))

    candidate_ledger = artifact_json("candidate_ledger")
    source_ledger = artifact_json("portfolio_source_ledger")
    assert production.CAPTURE_CONTRACT_VERSION == (
        "fxpm.current-refit-production-capture.v2"
    )
    assert candidate_ledger["schema_version"] == (
        "fxpm.current-refit-candidate-source-ledger.v2"
    )
    assert source_ledger["schema_version"] == (
        "fxpm.current-refit-portfolio-source-ledger.v2"
    )
    frozen = production._FrozenFile(staged, len(staged_raw), staged_sha)
    producer = production.SealedPortfolioSourceLedgerProducer(
        source_ledger,
        candidate_ledger,
        frozen,
    )
    record = replay.current_refit_candidate_record(_candidate())
    current_lineage = {
        "accepted_book_generation_receipt_sha256": _sha("c"),
        "current_refit_receipt_sha256": _sha("d"),
        "current_refit_execution_sha256": _sha("e"),
    }
    roster = {
        "physical_ledger_sha256": physical_sha,
        "tradeable_cell_ids": [cell_id],
        "roster_sha256": _sha("f"),
        "current_refit_lineage": current_lineage,
    }
    successor = {
        "candidate_records": [record],
        "candidate_records_sha256": candidate_ledger["candidate_records_sha256"],
    }
    request_body = {
        "schema_version": "test",
        "roster": roster,
        "current_refit_successor": successor,
        "scenarios": [],
        "expected_lineage": {"physical_ledger_sha256": physical_sha},
        "expected_cost_contract": {
            "use_spread": True,
            "swap_model_enabled": True,
        },
        "expected_policies": {},
        "authority_claimed": False,
    }
    evidence = producer(SimpleNamespace(to_dict=lambda: request_body))

    assert replay.portfolio_replay_evidence_seal_valid(evidence)
    assert evidence["replay"]["current_refit_successor"] == successor
    assert evidence["replay"]["native_sources"] == source_ledger["native_sources"]
    changed = json.loads(json.dumps(request_body))
    changed["current_refit_successor"]["candidate_records_sha256"] = _sha("0")
    with pytest.raises(
        production.CurrentRefitProductionCaptureError,
        match="PORTFOLIO_SOURCE_CANDIDATE_RECORD_MISMATCH",
    ):
        producer(SimpleNamespace(to_dict=lambda: changed))


def test_staged_ledger_change_invalidates_source_producer(tmp_path: Path) -> None:
    staged = tmp_path / "staged.json"
    staged.write_text('{"EURUSD":{}}', encoding="utf-8")
    raw = staged.read_bytes()
    frozen = production._FrozenFile(
        staged,
        len(raw),
        hashlib.sha256(raw).hexdigest(),
    )
    candidate = _candidate()
    descriptor = {"sha256": _sha("1"), "size_bytes": 10}
    ledger = production.build_candidate_source_ledger(
        generation_id="generation",
        staged_ledger_content_sha256=frozen.sha256,
        physical_ledger_sha256=_sha("2"),
        accepted_book_generation_receipt_sha256=_sha("3"),
        current_refit_receipt_sha256=_sha("4"),
        current_refit_execution_sha256=_sha("5"),
        current_cutoff_utc="2026-07-14T00:00:00Z",
        records_descriptor=descriptor,
        candidates=(candidate,),
        replayed_cell_ids=(candidate.cell_id,),
    )
    source = production.build_portfolio_source_ledger(
        generation_id="generation",
        candidate_ledger=ledger,
        current_refit_execution_access_id="access",
        window={},
        native_sources={},
        valuation_sources={},
    )
    producer = production.SealedPortfolioSourceLedgerProducer(source, ledger, frozen)
    staged.write_text('{"EURUSD":{"changed":true}}', encoding="utf-8")

    with pytest.raises(
        production.CurrentRefitProductionCaptureError,
        match="STAGED_LEDGER_CHANGED_AFTER_CAPTURE",
    ):
        producer(SimpleNamespace(to_dict=lambda: {}))
