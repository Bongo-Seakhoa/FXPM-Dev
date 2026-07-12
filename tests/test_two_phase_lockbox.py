from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pm_cap_calibration import cap_neutral_config_sha256
from pm_evidence import CALENDAR_SPLIT_MODE
from pm_pipeline import OptimizationPipeline, _stable_hash
from pm_main import _phase_a_optimization_complete
from pm_model_lifecycle import (
    EDGE_CHALLENGER_ROLE,
    LOCKBOX_CHAMPION_ROLE,
    PRELOCKBOX_CHAMPION_ROLE,
    build_edge_challenger_lineage,
    temporal_role_block_reason,
)
from pm_core import PipelineConfig
from pm_statistics import LockboxAccessRegistry
from pm_two_phase_lockbox import (
    CRITICAL_MODULE_PATHS,
    build_frozen_roster,
    build_prelockbox_roster_artifact,
    canonical_json_sha256,
    consume_global_final_lockbox_once,
    critical_module_code_contract,
    load_pinned_prelockbox_roster,
    materialize_joint_pass_ledger,
    sha256_file,
    validate_released_ledger_against_prelockbox,
    write_immutable_prelockbox_roster,
)
from tools.seal_prelockbox_roster import _nonoverlapping_registry_contract
from tools.build_postlockbox_edge_challenger import (
    attach_edge_book_preserving_champions,
    source_excluded_edge_book,
)


TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")


def _ledger() -> dict:
    grid = {}
    for timeframe in TIMEFRAMES:
        grid[timeframe] = {}
        for regime in REGIMES:
            selected = timeframe == "H1" and regime == "TREND"
            grid[timeframe][regime] = {
                "strategy_name": "Momentum" if selected else "NO_TRADE",
                "parameters": {"period": 12} if selected else {},
                "quality_score": 0.8 if selected else 0.0,
                "promotion_status": (
                    "FROZEN_PRELOCKBOX" if selected else "NO_TRADE"
                ),
                "promotion_reasons": [],
                "train_metrics": {"total_trades": 30} if selected else {},
                "val_metrics": {"total_trades": 20} if selected else {},
                "holdout_metrics": {},
                "validation_evidence": (
                    {
                        "selection_score_raw": 0.8123,
                        "temporal_lineage": {
                            "parameter_selection_cutoff_utc": "2024-01-01T00:00:00Z",
                            "performance_evidenced_through_utc": "2024-01-01T00:00:00Z",
                        },
                    }
                    if selected
                    else {}
                ),
                "artifact_meta": {"no_trade": not selected},
            }
    return {
        "EURUSD": {
            "symbol": "EURUSD",
            "holdout_metrics": {},
            "edge_challenger_book": {"regime_configs": {}},
            "data_window_meta": {
                "windows": {
                    "tuning_validation": {
                        "start_ts": "2022-01-01T00:00:00Z",
                        "end_ts": "2024-01-01T00:00:00Z",
                    },
                    "final_lockbox": {
                        "start_ts": "2024-01-08T00:00:00Z",
                        "end_ts": "2026-01-08T00:00:00Z",
                    },
                }
            },
            "regime_configs": grid,
        }
    }


def test_no_trade_strategy_is_recognized_when_artifact_meta_is_not_mapping() -> None:
    ledger = _ledger()
    cell = ledger["EURUSD"]["regime_configs"]["M5"]["CHOP"]
    cell["artifact_meta"] = None
    roster, errors = build_frozen_roster(ledger, ["EURUSD"])
    assert errors == []
    row = next(row for row in roster["rows"] if row["cell_id"] == "EURUSD|M5|CHOP")
    assert row["no_trade"] is True
    assert row["frozen_candidate"] is False


def test_phase_a_complete_is_successful_but_has_no_live_status() -> None:
    ledger = _ledger()
    complete, errors = _phase_a_optimization_complete(ledger, ["EURUSD"])
    assert complete is True
    assert errors == []
    statuses = {
        cell["promotion_status"]
        for regimes in ledger["EURUSD"]["regime_configs"].values()
        for cell in regimes.values()
    }
    assert "PROMOTED" not in statuses
    del ledger["EURUSD"]["regime_configs"]["D1"]["CHOP"]
    complete, errors = _phase_a_optimization_complete(ledger, ["EURUSD"])
    assert complete is False
    assert "PRELOCKBOX_CELL_MISSING:EURUSD|D1|CHOP" in errors


def test_phase_a_freeze_requires_a_final_interval_for_every_symbol() -> None:
    ledger = _ledger()
    del ledger["EURUSD"]["data_window_meta"]["windows"]["final_lockbox"]
    with pytest.raises(ValueError, match="PRELOCKBOX_GLOBAL_FINAL_WINDOW_INVALID"):
        build_prelockbox_roster_artifact(
            ledger=ledger,
            symbols=["EURUSD"],
            lineage={},
        )


def test_prelockbox_temporal_role_unblocks_only_with_all_or_none_release_marker() -> None:
    evidence = {
        "temporal_lineage": {"book_role": PRELOCKBOX_CHAMPION_ROLE},
    }
    assert temporal_role_block_reason(evidence)
    evidence["two_phase_release"] = {
        "prelockbox_roster_sha256": "a" * 64,
        "portfolio_evidence_sha256": "b" * 64,
        "release_scope": "ALL_FROZEN_TRADEABLE_CELLS",
        "survivor_pruning_performed": False,
        "detached_authorization_required": True,
    }
    assert temporal_role_block_reason(evidence) is None


def test_calendar_phase_a_feature_input_is_invariant_to_future_price_mutation() -> None:
    index = pd.date_range("2023-12-30", periods=6, freq="D", tz="UTC")
    frame = pd.DataFrame(
        {
            "Open": [1, 2, 3, 4, 5, 6],
            "High": [2, 3, 4, 5, 6, 7],
            "Low": [0, 1, 2, 3, 4, 5],
            "Close": [1, 2, 3, 4, 5, 6],
            "_SOURCE_FIRST_TS": index,
            "_SOURCE_LAST_TS": index,
        },
        index=index,
    )
    lockbox_start = pd.Timestamp("2024-01-02T00:00:00Z")
    before = OptimizationPipeline._selection_feature_input(
        frame,
        split_mode=CALENDAR_SPLIT_MODE,
        final_lockbox_start=lockbox_start,
    )
    mutated = frame.copy()
    mutated.loc[mutated.index >= lockbox_start, ["Open", "High", "Low", "Close"]] = 999999
    after = OptimizationPipeline._selection_feature_input(
        mutated,
        split_mode=CALENDAR_SPLIT_MODE,
        final_lockbox_start=lockbox_start,
    )
    pd.testing.assert_frame_equal(before, after)


def test_pinned_prelockbox_round_trip_does_not_create_config_self_reference(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.json"
    payload = {
        "pipeline": {
            "production_prelockbox_roster_path": "",
            "production_prelockbox_roster_sha256": "",
            "max_total_open_risk_pct": 8.0,
        },
        "symbols": ["EURUSD"],
    }
    artifact = build_prelockbox_roster_artifact(
        ledger=_ledger(),
        symbols=["EURUSD"],
        lineage={"cap_neutral_config_sha256": cap_neutral_config_sha256(payload)},
    )
    path = write_immutable_prelockbox_roster(tmp_path / "rosters", artifact)
    payload["pipeline"]["production_prelockbox_roster_path"] = str(path)
    payload["pipeline"]["production_prelockbox_roster_sha256"] = sha256_file(path)
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    assert cap_neutral_config_sha256(payload) == artifact["lineage"][
        "cap_neutral_config_sha256"
    ]
    loaded, loaded_path, errors = load_pinned_prelockbox_roster(
        config_payload=payload,
        config_path=config_path,
        expected_symbols=["EURUSD"],
        expected_lineage={
            "cap_neutral_config_sha256": cap_neutral_config_sha256(payload)
        },
    )
    assert errors == []
    assert loaded == artifact
    assert loaded_path == path


def test_critical_module_contract_invalidates_on_one_byte_drift(tmp_path: Path) -> None:
    for relative in CRITICAL_MODULE_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"stable:{relative}".encode("utf-8"))
    before = critical_module_code_contract(tmp_path)
    target = tmp_path / CRITICAL_MODULE_PATHS[0]
    target.write_bytes(target.read_bytes() + b"x")
    after = critical_module_code_contract(tmp_path)
    assert before["missing"] == []
    assert after["missing"] == []
    assert before["aggregate_sha256"] != after["aggregate_sha256"]
    assert before["files"][CRITICAL_MODULE_PATHS[0]] != after["files"][
        CRITICAL_MODULE_PATHS[0]
    ]


def test_frozen_roster_records_the_exact_effective_replay_priority() -> None:
    roster, errors = build_frozen_roster(_ledger(), ["EURUSD"])
    assert errors == []
    row = next(
        value
        for value in roster["rows"]
        if value["cell_id"] == "EURUSD|H1|TREND"
    )
    assert row["effective_replay_priority"] == {
        "source": "quality_score",
        "value": 0.8,
    }


def test_all_or_none_release_protects_no_trade_slots_and_symbol_bytes() -> None:
    artifact = build_prelockbox_roster_artifact(
        ledger=_ledger(), symbols=["EURUSD"], lineage={}
    )
    released = materialize_joint_pass_ledger(
        artifact, portfolio_evidence_sha256="e" * 64
    )
    assert validate_released_ledger_against_prelockbox(released, artifact) == []
    wrong_evidence = validate_released_ledger_against_prelockbox(
        released,
        artifact,
        expected_portfolio_evidence_sha256="f" * 64,
    )
    assert "RELEASED_CELL_EVIDENCE_MISMATCH:EURUSD|H1|TREND" in wrong_evidence

    released["EURUSD"]["regime_configs"]["M5"]["CHOP"]["quality_score"] = 1.0
    errors = validate_released_ledger_against_prelockbox(released, artifact)
    assert "RELEASED_NO_TRADE_CELL_MUTATED:EURUSD|M5|CHOP" in errors

    restored = materialize_joint_pass_ledger(
        artifact, portfolio_evidence_sha256="e" * 64
    )
    restored["EURUSD"]["validation_reason"] = "changed after release"
    errors = validate_released_ledger_against_prelockbox(restored, artifact)
    assert "RELEASED_SYMBOL_MUTATED:EURUSD" in errors


def test_global_final_access_allows_future_disjoint_cycle_and_rejects_overlap(
    tmp_path: Path,
) -> None:
    cfg = PipelineConfig(lockbox_access_registry_path=str(tmp_path / "registry.json"))
    first = _ledger()
    first_id = consume_global_final_lockbox_once(
        config=cfg,
        config_path=tmp_path / "config.json",
        frozen_ledger=first,
        roster_sha256="a" * 64,
        prelockbox_file_sha256="b" * 64,
    )
    assert first_id

    future = _ledger()
    future_window = future["EURUSD"]["data_window_meta"]["windows"]["final_lockbox"]
    future_window.update(
        {
            "start_ts": "2026-01-08T00:00:00Z",
            "end_ts": "2028-01-08T00:00:00Z",
        }
    )
    future_id = consume_global_final_lockbox_once(
        config=cfg,
        config_path=tmp_path / "config.json",
        frozen_ledger=future,
        roster_sha256="c" * 64,
        prelockbox_file_sha256="d" * 64,
    )
    assert future_id and future_id != first_id

    overlap = _ledger()
    overlap_window = overlap["EURUSD"]["data_window_meta"]["windows"]["final_lockbox"]
    overlap_window.update(
        {
            "start_ts": "2025-01-01T00:00:00Z",
            "end_ts": "2027-01-01T00:00:00Z",
        }
    )
    with pytest.raises(RuntimeError, match="OVERLAPPING"):
        consume_global_final_lockbox_once(
            config=cfg,
            config_path=tmp_path / "config.json",
            frozen_ledger=overlap,
            roster_sha256="f" * 64,
            prelockbox_file_sha256="0" * 64,
        )


def test_roster_freeze_allows_disjoint_old_cycle_but_rejects_overlap(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "registry.json"
    cfg = PipelineConfig(lockbox_access_registry_path=str(registry_path))
    registry = LockboxAccessRegistry(registry_path)
    old = registry.consume(
        cell={"symbol": "__GLOBAL_PORTFOLIO__", "timeframe": "ALL", "regime": "ALL"},
        lineage={
            "source_sha256": "a" * 64,
            "window_start": "2020-01-01T00:00:00Z",
            "window_end": "2022-01-01T00:00:00Z",
        },
    )
    assert old.allowed
    contract = _nonoverlapping_registry_contract(
        cfg, tmp_path / "config.json", _ledger(), ["EURUSD"]
    )
    assert contract["historical_access_count_at_freeze"] == 1
    assert contract["no_overlapping_access_at_freeze"] is True

    overlapping_path = tmp_path / "overlapping.json"
    overlap_cfg = PipelineConfig(lockbox_access_registry_path=str(overlapping_path))
    overlap = LockboxAccessRegistry(overlapping_path).consume(
        cell={"symbol": "__GLOBAL_PORTFOLIO__", "timeframe": "ALL", "regime": "ALL"},
        lineage={
            "source_sha256": "b" * 64,
            "window_start": "2025-01-01T00:00:00Z",
            "window_end": "2025-06-01T00:00:00Z",
        },
    )
    assert overlap.allowed
    with pytest.raises(SystemExit, match="OVERLAPPING_FINAL_ACCESS"):
        _nonoverlapping_registry_contract(
            overlap_cfg, tmp_path / "config.json", _ledger(), ["EURUSD"]
        )


def test_postlockbox_edge_book_is_physically_separate_from_champions() -> None:
    ledger = _ledger()
    before = json.dumps(ledger["EURUSD"]["regime_configs"], sort_keys=True)
    temporal = build_edge_challenger_lineage(
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-08T00:00:00Z",
        parent_champion={"book_role": LOCKBOX_CHAMPION_ROLE},
    )
    parameters = {"period": 12}
    candidate_id = _stable_hash(
        {
            "book_role": EDGE_CHALLENGER_ROLE,
            "symbol": "EURUSD",
            "timeframe": "H1",
            "regime": "TREND",
            "strategy_name": "Momentum",
            "parameters": parameters,
            "fit_cutoff_utc": temporal["fit_cutoff_utc"],
        }
    )
    output = attach_edge_book_preserving_champions(
        ledger,
        {
            "EURUSD": {
                "book_role": "LATEST_DATA_EDGE_CHALLENGER",
                "paper_only": True,
                "funded_role_eligible": False,
                "regime_configs": {
                    "H1": {
                        "TREND": {
                            "strategy_name": "Momentum",
                            "parameters": parameters,
                            "promotion_status": "PAPER_ONLY",
                            "artifact_meta": {
                                "edge_challenger": True,
                                "funded_role_eligible": False,
                                "candidate_id": candidate_id,
                            },
                            "validation_evidence": {
                                "temporal_lineage": temporal,
                            },
                        }
                    }
                },
            }
        },
    )
    after = json.dumps(output["EURUSD"]["regime_configs"], sort_keys=True)
    assert before == after
    assert output["EURUSD"]["edge_challenger_book"]["paper_only"] is True


def test_postlockbox_edge_attachment_rejects_any_funded_claim() -> None:
    with pytest.raises(ValueError, match="EDGE_BOOK_CLAIMS_FUNDED_ELIGIBILITY"):
        attach_edge_book_preserving_champions(
            _ledger(),
            {
                "EURUSD": {
                    "book_role": EDGE_CHALLENGER_ROLE,
                    "paper_only": True,
                    "funded_role_eligible": True,
                    "regime_configs": {},
                }
            },
        )


def test_source_excluded_symbol_gets_empty_manifest_bound_edge_book() -> None:
    excluded = _ledger()["EURUSD"]
    binding = {
        "status": "excluded_no_trade",
        "reason_codes": ["partial_history"],
        "activation_id": "activation-1",
    }
    excluded.setdefault("artifact_meta", {})["native_exclusion_binding"] = binding
    excluded["data_window_meta"]["native_exclusion_binding"] = binding

    book = source_excluded_edge_book(
        excluded,
        portfolio_evidence_sha256="a" * 64,
        prelockbox_roster_sha256="b" * 64,
    )
    output = attach_edge_book_preserving_champions(
        {"EURUSD": excluded},
        {"EURUSD": book},
    )

    attached = output["EURUSD"]["edge_challenger_book"]
    assert attached["generation_status"] == "SOURCE_EXCLUDED_NO_TRADE"
    assert attached["regime_configs"] == {}
    assert attached["generated_cells"] == 0
    assert attached["paper_only"] is True
    assert attached["funded_role_eligible"] is False
    assert attached["native_exclusion_binding"] == binding
    assert attached["native_exclusion_binding_sha256"] == canonical_json_sha256(binding)
