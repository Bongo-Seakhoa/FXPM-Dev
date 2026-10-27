from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import asdict, fields, replace
from decimal import Decimal
import json
import hashlib
import shutil
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal_producer
import pm_forward_oos_replay_producer as producer
import pm_native_prefix_production as native_prefix
import pm_portfolio_replay as portfolio
import pm_winner_raw_evidence as raw_winner
from pm_core import BACKTEST_FROZEN_PATH_COST_LADDER_SCHEMA_VERSION, Backtester, InstrumentSpec, PipelineConfig
from pm_spread_profile import FrozenSpreadProfile, align_prior_completed_spreads
from tests import test_backtester_winner_daily_r as backtester_fixture
from tests import test_book_multiplicity as book_fixture
from tests import test_native_prefix_production as native_fixture


class _Lane127WitnessUnavailable(AssertionError):
    """A named retained-evidence gap, never an arbitrary test failure."""


def _lane127_blocked(reason):
    return pytest.mark.xfail(raises=_Lane127WitnessUnavailable, strict=True, reason=reason)


def _is_current_owner_forward_migration_block(error):
    cause = error.__cause__
    return (type(error) is producer.ForwardOOSReplayProductionError
        and error.code == "SELECTED_ATTEMPT_RAW_V2_REVERIFY_FAILED"
        and error.details == ("b4e65e7a067617d3187cfc5e0b2104315d8e23a618bb04eaa1df665802b8d345", "CurrentRefitArtifactError")
        and type(cause) is producer.current_refit.CurrentRefitArtifactError
        and cause.args == ("TRIAL_DEFINITION_SEMANTICS_INVALID",))


STRICT_CURRENT_OWNER_FORWARD_MIGRATION = pytest.mark.xfail(
    raises=pytest.RaisesExc(producer.ForwardOOSReplayProductionError,
                           check=_is_current_owner_forward_migration_block),
    strict=True,
    reason="BLOCKED_CURRENT_OWNER_FORWARD_MIGRATION: next T2 integration, E1284/E1286",
)


_P75_LEVEL_ID = "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75"


def _specialist(
    *, source_count_receipt: dict | None = None
) -> producer._VerifiedSpecialist:
    cell_id = book.compute_cell_id("AUDUSD", "D1", "TREND")
    assert cell_id is not None
    return producer._VerifiedSpecialist(
        anchor_id="anchor-forward-oos-1",
        cell_id=cell_id,
        symbol="AUDUSD",
        timeframe="D1",
        regime="TREND",
        strategy_id="winner-daily-r-hold",
        strategy_class="_AtrHoldStrategy",
        strategy_module="tests.test_backtester_winner_daily_r",
        parameters={"sl_atr_mult": 10.0, "tp_atr_mult": 10.0},
        process_candidate_id="candidate-forward-oos-1",
        process_sha256="a" * 64,
        priority_score=1.0,
        source_count_receipt=source_count_receipt or {},
        detector_parameters={"minimum_history": 200},
        detector_parameters_sha256="b" * 64,
        detector_record={"record_sha256": "c" * 64},
    )


def _synthetic_archived_position() -> _PositionEvidence:
    """Invented legacy-format payload for archive integrity tests, not a replay."""
    cell_id = _specialist().cell_id
    entry = pd.Timestamp("2026-01-05T00:00:00Z")
    exit_time = pd.Timestamp("2026-01-06T12:00:00Z")
    candidate = portfolio.CandidateTrade(
        candidate_id="synthetic-archived-position",
        cell_id=cell_id, symbol="AUDUSD", timeframe="D1", regime="TREND",
        strategy_name="synthetic-archive-format", promotion_status="HISTORICAL_OOS_REPLAY",
        asset_class="fx", risk_groups=(), entry_time=entry, direction=1,
        stop_risk_per_lot=1010.0, margin_per_lot=100010.0,
        volume_min=0.01, volume_max=100.0, volume_step=0.01, priority_score=1.0,
        legs=(portfolio.ExitLeg(exit_time, 1.0, 180.0, price_pnl_per_entry_lot=180.0),),
        marks=(portfolio.MarkPoint(pd.Timestamp("2026-01-05T23:55:00Z"), 80.0),),
        source_schema=producer.POSITION_SOURCE_SCHEMA, margin_model=producer.MARGIN_MODEL,
    ).normalized()
    marks = tuple(
        _DailyMark(
            date=day, marked_at_utc=stamp, mid_price=Decimal(mid),
            bid_price=Decimal(mid) - Decimal("0.0001"),
            ask_price=Decimal(mid) + Decimal("0.0001"),
            base_financing_per_lot=Decimal(0), p75_financing_per_lot=Decimal(0),
            candidate_net_pnl_per_lot=pnl,
        )
        for day, stamp, mid, pnl in (
            ("2026-01-05", "2026-01-05T23:55:00Z", "1.001", 80.0),
            ("2026-01-06", "2026-01-06T12:00:00Z", "1.002", 180.0),
        )
    )
    return _PositionEvidence(
        candidate=candidate, position_id=candidate.candidate_id, cell_id=cell_id,
        symbol="AUDUSD", timeframe="D1", regime="TREND", side="LONG",
        contract_multiplier=Decimal(100000), stop_price=Decimal("0.99"),
        entry_mid=Decimal(1), entry_bid=Decimal("0.9999"), entry_ask=Decimal("1.0001"),
        base_entry_fill=Decimal("1.0001"), p75_entry_fill=Decimal("1.0002"),
        exit_mid=Decimal("1.002"), exit_bid=Decimal("1.0019"), exit_ask=Decimal("1.0021"),
        base_exit_fill=Decimal("1.0019"), p75_exit_fill=Decimal("1.0018"),
        margin_per_lot=Decimal(100010), marks=marks,
    )


def _anchor_for(position: _PositionEvidence) -> dict[str, object]:
    return {
        "anchor_id": "anchor-forward-oos-1",
        "selection_cutoff_utc": "2026-01-04T00:00:00Z",
        "forward_start_date": position.marks[0].date,
        "forward_end_date": position.marks[-1].date,
    }


def _spooled_ledgers(tmp_path, replay: _ArchivedAnchorRawReplay):
    context = authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="forward-oos-ledger-test-v1",
        native_universe_sha256="d" * 64,
        config_sha256="e" * 64,
    )
    bodies = _legacy_archive_ledger_bodies(replay, starting_balance=Decimal("10000"))
    bodies["EVENT_LEDGER"].update(
        {
            "evidence_access_started_at_utc": "2026-07-15T10:00:00Z",
            "execution_started_at_utc": "2026-07-15T10:00:01Z",
            "execution_completed_at_utc": "2026-07-15T10:00:02Z",
            "reconciled_at_utc": "2026-07-15T10:00:03Z",
        }
    )
    descriptors = {
        role: producer._write_raw_ledger(context, body, role=role)
        for role, body in bodies.items()
    }
    descriptor_set = causal_producer.AnchorReplayArtifactDescriptors(
        event_ledger=descriptors["EVENT_LEDGER"],
        fill_ledger=descriptors["FILL_LEDGER"],
        mtm_ledger=descriptors["MTM_LEDGER"],
        cost_ledger=descriptors["COST_LEDGER"],
        exposure_ledger=descriptors["EXPOSURE_LEDGER"],
        margin_ledger=descriptors["MARGIN_LEDGER"],
    )
    ledgers, hashes = causal_producer._load_replay_ledgers(
        context, descriptor_set, replay.anchor
    )
    return ledgers, hashes


def _bound_native_config(root: Path, data_dir: Path) -> PipelineConfig:
    config_path = native_fixture._execution_config(root)
    body = json.loads(config_path.read_text(encoding="utf-8"))
    body["pipeline"] = {"initial_capital": 10_000.0}
    body["position"] = {}
    native_fixture._write_json(config_path, body)
    config = PipelineConfig(
        data_dir=data_dir,
        initial_capital=10_000.0,
        research_mode=True,
        backtest_spread_mode="historical_bar",
    )
    config._deployment_config_path = str(config_path)
    return config


def _sealed_identity_only_product(
    *, symbol: str, timeframe: str, cutoff_utc: str
) -> native_prefix.NativePrefixProduct:
    def descriptor(format_name: str, seed: str, size: int = 10) -> dict:
        return {
            "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": f"objects/{seed}",
            "sha256": producer._sha({"seed": seed}),
            "size_bytes": size,
            "format": format_name,
        }

    content = descriptor("csv", "prefix-content", 100)
    index = {
        "schema_version": native_prefix.PREFIX_INDEX_SCHEMA,
        "authority_scope": native_prefix.PREFIX_AUTHORITY_SCOPE,
        "source_state": native_prefix.AVAILABLE,
        "source_reason": None,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "source_accessed_at_utc": "2026-02-01T00:00:01Z",
        "activation": {
            "activation_id": "identity-only-activation",
            "manifest_relative_path": "activations/identity-only.json",
            "manifest_sha256": "1" * 64,
            "entry_sha256": "2" * 64,
            "run_manifest_relative_path": "runs/identity-only/run.json",
            "run_manifest_sha256": "3" * 64,
        },
        "source": {
            "bars_relative_path": "runs/identity-only/bars.csv",
            "bars_sha256": "4" * 64,
            "bars_bytes": 100,
            "bars_rows": 2,
            "audit_sidecar_relative_path": "audits/identity-only.json",
            "audit_sidecar_sha256": "5" * 64,
            "audit_sidecar_bytes": 10,
            "provenance_relative_path": "runs/identity-only/provenance.json",
            "provenance_sha256": "6" * 64,
            "provenance_bytes": 10,
            "request_map_sha256": "7" * 64,
            "source_identity_sha256": "8" * 64,
            "source_identity": {"account": "frozen"},
            "instrument_snapshot": {},
            "request_start_utc": "2026-01-01T00:00:00Z",
            "request_end_exclusive_utc": cutoff_utc,
        },
        "prefix": {
            "schema": descriptor("json", "prefix-schema"),
            "content": content,
            "segments": descriptor("jsonl", "prefix-segments"),
            "prefix_rows": 2,
            "prefix_start_utc": "2026-01-01T00:00:00Z",
            "prefix_end_exclusive_utc": cutoff_utc,
            "prefix_first_open_utc": "2026-01-01T00:00:00Z",
            "prefix_last_open_utc": "2026-01-31T23:55:00Z",
            "prefix_last_close_utc": cutoff_utc,
        },
        "continuity": {
            "policy": descriptor("json", "continuity-policy"),
            "receipt": descriptor("json", "continuity-receipt"),
            "geometry_sha256": "9" * 64,
            "internal_status": "CONTINUOUS",
            "boundary_status": "COMPLETE",
            "unexplained_discontinuity_count": 0,
            "segment_count": 1,
            "gap_count": 0,
            "requires_preserved_segment_ledger": True,
        },
        "terminal_receipt": None,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    raw = producer._canonical_bytes(index)
    index_sha = producer.hashlib.sha256(raw).hexdigest()
    index_descriptor = {
        "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": "objects/prefix-index",
        "sha256": index_sha,
        "size_bytes": len(raw),
        "format": authority_spool.CANONICAL_JSON_FORMAT,
    }
    return native_prefix.NativePrefixProduct(
        source_state=native_prefix.AVAILABLE,
        index=index,
        index_descriptor=index_descriptor,
        index_sha256=index_sha,
        native_prefix=native_prefix._native_prefix_from_index(index, index_sha),
        instrument_snapshot={},
        prefix_path=None,
        prefix_descriptor=content,
        artifact_descriptors=(index_descriptor, content),
        _seal=native_prefix._PRODUCT_SEAL,
    )


def test_synthetic_archived_position_spools_and_reopens_as_causal_activity(
    tmp_path,
) -> None:
    position = _synthetic_archived_position()
    replay = _ArchivedAnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={position.position_id: Decimal("0.01")},
    )
    ledgers, hashes = _spooled_ledgers(tmp_path, replay)
    raw_book_cell = {"cell_id": position.cell_id, "regime": position.regime}
    projected_cell = {
        "status": "WINNER",
        "timeframe": position.timeframe,
        "cell_decision_sha256": "f" * 64,
    }

    activity, gross, base_cost, p75_cost = causal_producer._assemble_activity(
        anchor=replay.anchor,
        ledgers=ledgers,
        descriptor_hashes=hashes,
        projected_cells=[projected_cell],
        book_cells=[raw_book_cell],
    )

    assert len(activity["days"]) == len(position.marks)
    assert sum(len(day["entries"]) for day in activity["days"]) == 1
    assert sum(len(day["exits"]) for day in activity["days"]) == 1
    assert sum(len(day["position_marks"]) for day in activity["days"]) == len(
        position.marks
    )
    assert len(gross) == len(base_cost) == len(p75_cost) == len(position.marks)
    assert sum(p75_cost) < sum(base_cost) < Decimal(0)


def test_portfolio_blocked_position_is_not_laundered_into_raw_fill(
    tmp_path,
) -> None:
    position = _synthetic_archived_position()
    replay = _ArchivedAnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={},
    )
    ledgers, hashes = _spooled_ledgers(tmp_path, replay)

    activity, gross, base_cost, p75_cost = causal_producer._assemble_activity(
        anchor=replay.anchor,
        ledgers=ledgers,
        descriptor_hashes=hashes,
        projected_cells=[
            {
                "status": "WINNER",
                "timeframe": position.timeframe,
                "cell_decision_sha256": "f" * 64,
            }
        ],
        book_cells=[{"cell_id": position.cell_id, "regime": position.regime}],
    )

    assert all(not day["entries"] and not day["exits"] for day in activity["days"])
    assert gross == base_cost == p75_cost == [Decimal(0)] * len(position.marks)


def test_archived_forward_ledgers_preserve_legacy_p75_fields() -> None:
    position = _synthetic_archived_position()
    replay = _ArchivedAnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={position.position_id: Decimal("0.01")},
    )

    ledgers = _legacy_archive_ledger_bodies(replay, starting_balance=Decimal("10000"))
    opening = next(
        entry
        for day in ledgers["FILL_LEDGER"]["days"]
        for entry in day["entries"]
    )
    mark = next(
        item
        for day in ledgers["COST_LEDGER"]["days"]
        for item in day["marks"]
    )

    assert opening["p75_fill_price"] == producer._decimal_text(
        position.p75_entry_fill
    )
    assert "p75_financing_cash" in mark
    assert "stress" not in json.dumps(ledgers, sort_keys=True).lower()


def test_forward_p75_ladder_selector_rejects_full_or_reordered_levels() -> None:
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="BACKTEST_COST_LADDER_LEVEL_ROSTER_INVALID",
    ):
        _p75_ladder_result(
            {
                "schema_version": BACKTEST_FROZEN_PATH_COST_LADDER_SCHEMA_VERSION,
                "levels": [
                    {"level_id": "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P95"},
                    {"level_id": _P75_LEVEL_ID},
                ],
            },
            cell_id=_specialist().cell_id,
        )


def test_candidate_mark_budget_fails_before_unbounded_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(producer, "_MAX_CANDIDATE_MARKS_PER_ANCHOR", 2)
    budget = producer._ResourceBudget()
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_MARK_COUNT_BOUND_EXCEEDED",
    ):
        budget.reserve_marks(3, "anchor-forward-oos-1")
    assert budget.candidate_marks == 0


def test_deployment_and_broker_files_are_immutably_bound_and_rechecked(
    tmp_path,
) -> None:
    broker_path = tmp_path / "broker_specs.json"
    broker_path.write_text('{"AUDUSD":{"symbol":"AUDUSD"}}', encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"_meta":{"scope":"test"},"broker_specs_path":"broker_specs.json",'
        '"pipeline":{"initial_capital":10000.0},"position":{}}',
        encoding="utf-8",
    )
    config = PipelineConfig(initial_capital=10_000.0)
    config._deployment_config_path = str(config_path)
    binding = producer._deployment_config(config)
    producer._assert_deployment_config_unchanged(binding, config)

    broker_path.write_text(
        '{"AUDUSD":{"symbol":"AUDUSD","tampered":true}}', encoding="utf-8"
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="DEPLOYMENT_OR_BROKER_CONFIG_CHANGED_DURING_REPLAY",
    ):
        producer._assert_deployment_config_unchanged(binding, config)


def test_later_native_frame_must_contain_byte_exact_selection_prefix() -> None:
    frame = backtester_fixture._frame()
    selected = frame.iloc[:3].copy(deep=True)
    receipt = {
        "fit_start_utc": selected.index[0].isoformat(),
        "fit_end_exclusive_utc": selected["_BAR_CLOSE_TS"].iloc[-1].isoformat(),
        "source_bar_count": len(selected),
        "fit_frame_sha256": producer._frame_sha256(selected),
        "latest_closed_bar_utc": selected["_BAR_CLOSE_TS"].iloc[-1].isoformat(),
    }
    specialist = _specialist(source_count_receipt=receipt)
    producer._selection_prefix_match(frame, specialist)

    tampered = frame.copy(deep=True)
    tampered.loc[tampered.index[1], "Close"] += 0.0001
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="LATER_NATIVE_PRODUCT_DOES_NOT_EXTEND_EXACT_SELECTION_PREFIX",
    ):
        producer._selection_prefix_match(tampered, specialist)


@pytest.mark.parametrize(
    "symbol",
    (book.PINNED_SYMBOL_ORDER[17], "Platinum", "Palladium"),
)
def test_nonfirst_symbol_uses_local_adapter_timeframe_group_and_exact_book_copy(
    symbol: str,
) -> None:
    timeframe = "H4"
    regime = "BREAKOUT"
    group_ordinal = book.PINNED_SYMBOL_ORDER.index(symbol) * len(
        book.TIMEFRAME_ORDER
    ) + book.TIMEFRAME_ORDER.index(timeframe)
    ordinal = group_ordinal * len(book.REGIME_ORDER) + book.REGIME_ORDER.index(regime)
    cell = {
        "ordinal": ordinal,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
    }
    cells = [{} for _ in range(book.FAMILY_SIZE)]
    cells[ordinal] = cell
    sources = [{"payload": {"source": index}, "receipt_sha256": f"{index:064x}"}
               for index in range(len(book.PINNED_SYMBOL_ORDER) * 6)]
    fits = [{"payload": {"fit": index}, "receipt_sha256": f"{index + 600:064x}"}
            for index in range(len(sources))]
    local_sources = [{"source": -1} for _ in book.TIMEFRAME_ORDER]
    local_fits = [{"fit": -1} for _ in book.TIMEFRAME_ORDER]
    local_ordinal = book.TIMEFRAME_ORDER.index(timeframe)
    local_sources[local_ordinal] = {**copy.deepcopy(sources[group_ordinal]["payload"]),
                                    "receipt_sha256": sources[group_ordinal]["receipt_sha256"]}
    local_fits[local_ordinal] = {**copy.deepcopy(fits[group_ordinal]["payload"]),
                                 "receipt_sha256": fits[group_ordinal]["receipt_sha256"]}
    book_body = {"cells": cells, "source_groups": sources, "regime_fits": fits}
    index = {"source_groups": local_sources, "regime_fits": local_fits}

    source, fit = producer._selected_group_records(
        book_body=book_body,
        book_cell=cell,
        index=index,
        symbol=symbol,
        timeframe=timeframe,
        cell_id="1" * 64,
    )
    assert source == sources[group_ordinal]
    assert fit == fits[group_ordinal]

    if symbol in {"Platinum", "Palladium"}:
        with pytest.raises(
            producer.ForwardOOSReplayProductionError,
            match="SELECTED_SYMBOL_INVALID",
        ):
            producer._selected_group_records(
                book_body=book_body,
                book_cell=cell,
                index=index,
                symbol=symbol.upper(),
                timeframe=timeframe,
                cell_id="1" * 64,
            )

    index["regime_fits"][local_ordinal] = {"fit": "post-hoc"}
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="SELECTED_ADAPTER_BOOK_GROUP_SPLICE",
    ):
        producer._selected_group_records(
            book_body=book_body,
            book_cell=cell,
            index=index,
            symbol=symbol,
            timeframe=timeframe,
            cell_id="1" * 64,
        )


def test_winner_v14_request_and_decision_are_standalone_replayed() -> None:
    cell = book_fixture._verified_moderate_winner("AUDUSD", "M5", "TREND")
    request = cell.winner_request
    decision = cell.winner_decision
    assert request is not None and decision is not None

    reopened_request = producer._winner_request(
        request.to_dict(), request.request_sha256
    )
    reopened_decision = producer._winner_decision(
        decision.to_dict(), reopened_request, decision.receipt_sha256
    )
    assert reopened_request.to_dict() == request.to_dict()
    assert reopened_decision.to_dict() == decision.to_dict()

    tampered = copy.deepcopy(request.to_dict())
    tampered["attempt_ledger"]["payload"]["attempts"][0]["strategy_id"] = (
        "post-hoc-strategy"
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="WINNER_REQUEST_PAYLOAD_HASH_INVALID",
    ):
        producer._winner_request(tampered, request.request_sha256)


@pytest.mark.parametrize(
    ("symbol", "timeframe"),
    [
        ("NOT_A_SYMBOL", "M5"),
        ("AUDUSD", "H2"),
        ("PLATINUM", "H4"),
        ("platinum", "H4"),
    ],
)
def test_native_product_key_rejects_out_of_contract_cells(
    symbol: str, timeframe: str
) -> None:
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="NATIVE_PRODUCT_KEY_INVALID",
    ):
        producer.native_product_key(symbol, timeframe)


def test_native_product_key_preserves_exact_mt5_symbol_for_full_roster() -> None:
    for symbol in book.PINNED_SYMBOL_ORDER:
        assert producer.native_product_key(symbol, "H4") == f"{symbol}|H4"


@pytest.mark.parametrize("symbol", ("Platinum", "Palladium"))
def test_forward_native_identity_preserves_exact_mt5_symbol(
    symbol: str,
) -> None:
    cutoff = "2026-02-01T00:00:00Z"
    product = _sealed_identity_only_product(
        symbol=symbol,
        timeframe="H4",
        cutoff_utc=cutoff,
    )

    identity = producer._native_product_identity(
        product,
        expected_symbol=symbol,
        expected_timeframe="H4",
        expected_cutoff_utc=cutoff,
        role=f"EXECUTION:test:{symbol}|H4",
    )

    assert identity["symbol"] == symbol
    assert producer.native_product_key(
        identity["symbol"], identity["timeframe"]
    ) == f"{symbol}|H4"


def test_m5_specialist_cannot_implicitly_self_supply_valuation_roster() -> None:
    plan = {
        "anchor_id": "anchor-forward-oos-1",
        "forward_end_date": "2026-01-31",
    }
    required = {
        "anchor-forward-oos-1": {
            "execution_product_keys": ["AUDUSD|M5"],
            "valuation_m5_product_keys": ["AUDUSD|M5"],
        }
    }
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_M5_VALUATION_PRODUCT_ROSTER_MISMATCH",
    ):
            producer._product_roster_identities(
                plans=[plan],
                capital_phase_windows_by_anchor={
                    "anchor-forward-oos-1": {
                        producer.CAPITAL_INNER_SELECTION: {
                            "start_date": "2025-01-01",
                            "end_date": "2026-01-01",
                        },
                        producer.CAPITAL_OUTER_EVALUATION: {
                            "start_date": "2026-01-01",
                            "end_date": "2026-02-01",
                        },
                    }
                },
            required=required,
            execution_products_by_anchor={
                "anchor-forward-oos-1": {"AUDUSD|M5": object()}
            },
            valuation_products_by_anchor={"anchor-forward-oos-1": {}},
        )


def test_seventh_producer_receipt_is_content_addressed_reopened_and_non_authorizing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert producer.PRODUCTION_CONTRACT_VERSION == (
        "fxpm.forward-oos-replay-producer.v5"
    )
    assert producer.REPLAY_PRECOMMIT_SCHEMA == "fxpm.forward-oos-replay-precommit.v5"
    assert producer.PRODUCER_RECEIPT_SCHEMA == (
        "fxpm.forward-oos-replay-producer-receipt.v5"
    )
    assert producer.CAPITAL_CANDIDATE_CHUNK_SCHEMA == (
        "fxpm.forward-oos-capital-candidate-chunk.v2"
    )
    assert producer.POSITION_SOURCE_SCHEMA == "fxpm.forward-oos-backtester-position.v2"
    assert producer.VALUATION_CONTRACT == (
        "AUTHENTICATED_NATIVE_M5_OPEN_EXECUTABLE_BID_ASK_PLUS_SWAP_V2"
    )
    context = authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="forward-oos-product-test",
        native_universe_sha256="e" * 64,
        config_sha256="f" * 64,
    )
    position = _synthetic_archived_position()
    replay = _ArchivedAnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={},
    )
    bodies = _legacy_archive_ledger_bodies(replay, starting_balance=Decimal("10000"))
    bodies["EVENT_LEDGER"].update(
        {
            "evidence_access_started_at_utc": "2026-07-15T10:00:00Z",
            "execution_started_at_utc": "2026-07-15T10:00:01Z",
            "execution_completed_at_utc": "2026-07-15T10:00:02Z",
            "reconciled_at_utc": "2026-07-15T10:00:03Z",
        }
    )
    rows = {
        role: producer._write_raw_ledger(context, body, role=role)
        for role, body in bodies.items()
    }
    descriptors = causal_producer.AnchorReplayArtifactDescriptors(
        event_ledger=rows["EVENT_LEDGER"],
        fill_ledger=rows["FILL_LEDGER"],
        mtm_ledger=rows["MTM_LEDGER"],
        cost_ledger=rows["COST_LEDGER"],
        exposure_ledger=rows["EXPOSURE_LEDGER"],
        margin_ledger=rows["MARGIN_LEDGER"],
    )
    anchor_id = replay.anchor["anchor_id"]
    component_manifest = {
        "schema": "fxpm.causal-component-source-roster.v1",
        "component_identity_key": "synchronized_replay_code_sha256",
        "files": [
            producer._source_file_identity("pm_portfolio_replay.py"),
            producer._source_file_identity("pm_causal_evidence_producer.py"),
            producer._source_file_identity("pm_forward_oos_replay_producer.py"),
        ],
    }
    causal_payload = {
        "process_definition": {
            "component_identities": {
                "synchronized_replay_code_sha256": producer._sha(
                    component_manifest
                )
            }
        }
    }
    code_identity = producer._replay_code_identity(causal_payload)
    pipeline_identity = {
        "schema_version": "fxpm.forward-oos-pipeline-config-identity.v1",
        "deployment_config_path": "config.json",
        "full_dataclass_surface": {},
        "backtester_cost_surface": {},
    }
    deployment = {
        "deployment_config_path": "config.json",
        "deployment_config_sha256": context.config_sha256,
        "deployment_config_size_bytes": 1,
        "broker_specs_path": "broker_specs.json",
        "broker_specs_relative_path": "broker_specs.json",
        "broker_specs_sha256": "5" * 64,
        "broker_specs_size_bytes": 1,
        "pipeline_config_identity": pipeline_identity,
        "pipeline_config_identity_sha256": producer._sha(pipeline_identity),
    }
    outer_window = {
        "start_date": replay.anchor["forward_start_date"],
        "end_date": (
            pd.Timestamp(replay.anchor["forward_end_date"]) + pd.Timedelta(days=1)
        ).date().isoformat(),
    }
    capital_windows = {
        anchor_id: {
            producer.CAPITAL_INNER_SELECTION: dict(outer_window),
            producer.CAPITAL_OUTER_EVALUATION: dict(outer_window),
        }
    }
    capital_binding = {
        "anchor_plan": [
            {
                "anchor_id": anchor_id,
                "inner_window": dict(outer_window),
                "outer_window": dict(outer_window),
            }
        ],
        "bootstrap_identity_sha256": "6" * 64,
        "capital_precommitment_body_sha256": "7" * 64,
        "contract_lineage": {"test": "contract"},
        "margin_terms_sha256": "8" * 64,
        "broker_terms_sha256": "9" * 64,
    }
    monkeypatch.setattr(
        producer.capital_bootstrap,
        "verify_capital_forward_phase_binding_body",
        lambda observed_context, _value: capital_binding
        if observed_context is context
        else pytest.fail("generation context substituted"),
    )
    native_identity_d1 = {
        "symbol": "AUDUSD",
        "timeframe": "D1",
        "activation_id": "activation-test",
        "activation_manifest_sha256": "a" * 64,
        "source_bars_sha256": "b" * 64,
        "source_audit_sidecar_sha256": "c" * 64,
        "index_sha256": "d" * 64,
    }
    native_identity_m5 = {
        **native_identity_d1,
        "timeframe": "M5",
        "source_bars_sha256": "e" * 64,
        "source_audit_sidecar_sha256": "f" * 64,
        "index_sha256": "0" * 64,
    }
    product_identities = [
        {
            "anchor_id": anchor_id,
            "forward_end_exclusive_utc": outer_window["end_date"]
            + "T00:00:00Z",
            "execution_products": [native_identity_d1],
            "valuation_m5_products": [native_identity_m5],
            "execution_product_roster_sha256": producer._sha(
                [native_identity_d1]
            ),
            "valuation_m5_product_roster_sha256": producer._sha(
                [native_identity_m5]
            ),
            "valuation_products_supplied_separately": True,
            "capital_phase_windows": capital_windows[anchor_id],
        }
    ]
    precommit_body = {
        "schema_version": producer.REPLAY_PRECOMMIT_SCHEMA,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": "1" * 64,
        "historical_run_receipt_sha256": "2" * 64,
        "historical_run_payload_sha256": "3" * 64,
        "causal_precommit_payload_sha256": "4" * 64,
        "created_at_utc": "2026-07-15T09:59:59.000000Z",
        "created_before_first_forward_oos_frame_access": True,
        "all_native_source_access_times_not_after_precommit": True,
        "required_rosters": {anchor_id: {}},
        "anchor_product_identities": product_identities,
        "anchor_product_identities_sha256": producer._sha(product_identities),
        "capital_phase_windows_by_anchor": capital_windows,
        "capital_phase_windows_sha256": producer._sha(capital_windows),
        "capital_forward_phase_binding": capital_binding,
        "capital_forward_phase_binding_sha256": producer._sha(capital_binding),
        "capital_bootstrap_identity_sha256": "6" * 64,
        "capital_precommitment_body_sha256": "7" * 64,
        "capital_contract_lineage_sha256": producer._sha({"test": "contract"}),
        "capital_margin_terms_sha256": "8" * 64,
        "capital_broker_terms_sha256": "9" * 64,
        "deployment_binding": deployment,
        "code_identity": code_identity,
        "valuation_contract": producer.VALUATION_CONTRACT,
        "product_cutoff_contract": "EXACT_FORWARD_END_EXCLUSIVE_NO_LATER_ROWS",
        "confers_causal_current_refit_capital_publication_or_trading_authority": False,
    }
    precommit_descriptor = producer._write_raw_ledger(
        context, precommit_body, role="FORWARD_PRECOMMIT"
    )
    replay_precommit = SimpleNamespace(
        receipt_sha256=precommit_descriptor["sha256"]
    )
    run_anchor = {
        "anchor_id": anchor_id,
        "book_decision_sha256": "1" * 64,
        "book_generation_sha256": "2" * 64,
        "producer_request_sha256": "3" * 64,
        "symbol_output_roster_sha256": "4" * 64,
    }
    phase_manifests = {}
    for phase in producer.CAPITAL_PHASES:
        phase_candidate = replace(
            position.candidate,
            candidate_id=f"{position.candidate.candidate_id}:{phase}",
        ).normalized()
        phase_position = replace(position, candidate=phase_candidate)
        phase_replay = replace(
            replay,
            positions=(phase_position,),
            production_metadata={
                "capital_phase_id": producer._capital_phase_id(anchor_id, phase),
                "capital_phase": phase,
                "capital_window": dict(outer_window),
            },
        )
        manifest_descriptor, _ = _persist_capital_phase_evidence(
            context=context,
            replay=phase_replay,
            run_anchor=run_anchor,
            replay_precommit=replay_precommit,
            replay_precommit_body=precommit_body,
            precommit_anchor=product_identities[0],
        )
        phase_manifests[producer._capital_phase_id(anchor_id, phase)] = (
            manifest_descriptor
        )
    receipt = {
        "schema_version": producer.PRODUCER_RECEIPT_SCHEMA,
        "production_contract_version": producer.PRODUCTION_CONTRACT_VERSION,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": "1" * 64,
        "historical_run_receipt_sha256": "2" * 64,
        "causal_precommit_payload_sha256": "4" * 64,
        "forward_replay_precommit_receipt_sha256": precommit_descriptor["sha256"],
        "forward_replay_precommit_descriptor": precommit_descriptor,
        "capital_bootstrap_identity_sha256": "6" * 64,
        "capital_precommitment_body_sha256": "7" * 64,
        "capital_contract_lineage_sha256": producer._sha({"test": "contract"}),
        "capital_margin_terms_sha256": "8" * 64,
        "capital_broker_terms_sha256": "9" * 64,
        "evidence_access_started_at_utc": "2026-07-15T10:00:00Z",
        "execution_started_at_utc": "2026-07-15T10:00:01Z",
        "execution_completed_at_utc": "2026-07-15T10:00:02Z",
        "reconciled_at_utc": "2026-07-15T10:00:03Z",
        "deployment_binding": deployment,
        "code_identity": code_identity,
        "anchor_ledger_descriptors": {
            anchor_id: {
                role: dict(value) for role, value in descriptors.descriptors()
            }
        },
        "anchor_production_metadata": {anchor_id: {"anchor_id": anchor_id}},
        "ordered_anchor_ids": [anchor_id],
        "capital_phase_manifest_descriptors": phase_manifests,
        "ordered_capital_phase_ids": list(phase_manifests),
        "capital_candidate_records_complete_pre_policy": True,
        "capital_phase_window_contract": (
            "SIGNED_BOOTSTRAP_INNER_AND_CAUSAL_OUTER_HALF_OPEN_V1"
        ),
        "valuation_contract": producer.VALUATION_CONTRACT,
        "valuation_event_order": "close_existing_then_mark_existing_then_admit",
        "candidate_marks_thinned_or_sampled": False,
        "causal_consumer_reopened_and_verified": False,
        "causal_binding_followup_required": True,
        "confers_causal_process_current_refit_capital_readiness_publication_"
        "or_trading_authority": False,
    }
    receipt_descriptor = producer._write_raw_ledger(
        context, receipt, role="PRODUCER_RECEIPT"
    )
    raw = producer._canonical_bytes(receipt)
    anchor_rows = ((anchor_id, descriptors),)
    unissued = producer.ForwardOOSReplayProduct(
        anchor_rows,
        raw,
        receipt_descriptor,
        "0" * 64,
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_NOT_ISSUED",
    ):
        producer.verify_forward_oos_replay_product(context, unissued)
    issuer = producer.produce_forward_oos_replay_artifacts.__kwdefaults__[
        "_issue_product"
    ]
    product = replace(
        unissued,
        _issuance_tag=issuer(anchor_rows, raw, receipt_descriptor),
    )
    assert producer.verify_forward_oos_replay_product(context, product) == receipt
    _, capital_candidates, capital_lineages = (
        producer.reopen_forward_oos_capital_phase_evidence(context, product)
    )
    assert set(capital_candidates) == set(phase_manifests)
    assert all(len(values) == 1 for values in capital_candidates.values())
    assert set(capital_lineages) == set(phase_manifests)
    assert product.producer_receipt_sha256 == receipt_descriptor["sha256"]
    assert product.causal_binding_followup_required is True

    hostile = replace(
        product,
        producer_receipt_descriptor={
            **receipt_descriptor,
            "sha256": "0" * 64,
        },
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_NOT_ISSUED",
    ):
        producer.verify_forward_oos_replay_product(context, hostile)

    spliced_descriptors = replace(
        descriptors,
        fill_ledger=descriptors.event_ledger,
    )
    spliced = replace(
        product,
        _anchor_rows=((anchor_id, spliced_descriptors),),
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_NOT_ISSUED",
    ):
        producer.verify_forward_oos_replay_product(context, spliced)

    fill_path = Path(context.root) / rows["FILL_LEDGER"]["relative_path"]
    original_fill = fill_path.read_bytes()
    fill_path.write_bytes(b"{}")
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_FILL_LEDGER_INTEGRITY_MISMATCH",
    ):
        producer.verify_forward_oos_replay_product(context, product)

    fill_path.write_bytes(original_fill)
    assert producer.verify_forward_oos_replay_product(context, product) == receipt
    outer_manifest = producer._read_descriptor_json(
        context,
        phase_manifests[
            producer._capital_phase_id(
                anchor_id, producer.CAPITAL_OUTER_EVALUATION
            )
        ],
        code="TEST_CAPITAL_MANIFEST",
    )
    chunk_descriptor = outer_manifest["candidate_chunk_descriptors"][0]
    chunk_path = Path(context.root) / chunk_descriptor["relative_path"]
    original_chunk = chunk_path.read_bytes()
    chunk_path.write_bytes(b"{}")
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="CAPITAL_CANDIDATE_CHUNK_INTEGRITY_MISMATCH",
    ):
        producer.verify_forward_oos_replay_product(context, product)
    chunk_path.write_bytes(original_chunk)
    assert producer.verify_forward_oos_replay_product(context, product) == receipt
    fill_path.unlink()
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_OOS_REPLAY_PRODUCT_FILL_LEDGER_UNAVAILABLE",
    ):
        producer.verify_forward_oos_replay_product(context, product)


def test_code_identity_exposes_exact_causal_binding_followup() -> None:
    files = [
        producer._source_file_identity("pm_portfolio_replay.py"),
        producer._source_file_identity("pm_causal_evidence_producer.py"),
        producer._source_file_identity("pm_forward_oos_replay_producer.py"),
    ]
    body = {
        "schema": "fxpm.causal-component-source-roster.v1",
        "component_identity_key": "synchronized_replay_code_sha256",
        "files": files,
    }
    payload = {
        "process_definition": {
            "component_identities": {
                "synchronized_replay_code_sha256": producer._sha(body)
            }
        }
    }
    identity = producer._replay_code_identity(payload)
    assert identity["causal_synchronized_replay_component_identity_sha256"] == (
        producer._sha(body)
    )
    assert identity["forward_producer_in_causal_component_manifest"] is True
    assert identity["causal_binding_followup_required"] is True
    assert identity["forward_oos_replay_producer_source_identity_sha256"]


def test_separate_d1_and_m5_native_products_preserve_identity_and_raw_reopen(
    tmp_path,
) -> None:
    d1_data, d1_facts, d1_context, d1_product = native_fixture._produce(
        tmp_path / "d1-native", "D1"
    )
    m5_data, m5_facts, m5_context, m5_product = native_fixture._produce(
        tmp_path / "m5-native", "M5"
    )
    d1_identity = producer._native_product_identity(
        d1_product,
        expected_symbol=native_fixture.SYMBOL,
        expected_timeframe="D1",
        expected_cutoff_utc=d1_facts["cutoff"],
        role="EXECUTION:canary:EURUSD|D1",
    )
    m5_identity = producer._native_product_identity(
        m5_product,
        expected_symbol=native_fixture.SYMBOL,
        expected_timeframe="M5",
        expected_cutoff_utc=m5_facts["cutoff"],
        role="VALUATION_M5:canary:EURUSD|M5",
    )
    for identity in (d1_identity, m5_identity):
        assert identity["activation_manifest_sha256"]
        assert identity["source_provenance_sha256"]
        assert identity["source_request_map_sha256"]
        assert identity["source_identity_sha256"]
        assert identity["prefix_content_sha256"]
        assert identity["continuity_receipt_sha256"]
        assert identity["continuity_geometry_sha256"]

    d1_config = _bound_native_config(tmp_path / "d1-config", d1_data)
    m5_config = _bound_native_config(tmp_path / "m5-config", m5_data)
    d1_reopened = producer._reopen_bound_native_product(
        context=d1_context,
        product=d1_product,
        expected_identity=d1_identity,
        config=d1_config,
        production_contract_required=False,
    )
    assert d1_reopened.index_sha256 == d1_product.index_sha256

    m5_reopened = producer._reopen_bound_native_product(
        context=m5_context,
        product=m5_product,
        expected_identity=m5_identity,
        config=m5_config,
        production_contract_required=False,
    )
    assert m5_reopened.index_sha256 == m5_product.index_sha256


def test_reopen_rejects_post_precommit_native_source_tamper(tmp_path) -> None:
    data_dir, facts, context, product = native_fixture._produce(
        tmp_path / "native", "M5"
    )
    identity = producer._native_product_identity(
        product,
        expected_symbol=native_fixture.SYMBOL,
        expected_timeframe="M5",
        expected_cutoff_utc=facts["cutoff"],
        role="VALUATION_M5:canary:EURUSD|M5",
    )
    config = _bound_native_config(tmp_path / "config", data_dir)
    bars = Path(facts["bars_path"])
    bars.write_bytes(bars.read_bytes() + b"\n")
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_NATIVE_PRODUCT_RAW_REVERIFY_FAILED",
    ):
        producer._reopen_bound_native_product(
            context=context,
            product=product,
            expected_identity=identity,
            config=config,
            production_contract_required=False,
        )


@pytest.mark.parametrize("name,arguments", [
    ("precommit_forward_oos_replay_products",
     ["context", "historical_precommit", "historical_run_receipt", "causal_precommit_payload",
      "execution_products_by_anchor", "valuation_products_by_anchor", "capital_phase_binding", "config"]),
    ("produce_forward_oos_replay_artifacts",
     ["context", "historical_precommit", "historical_run_receipt", "causal_precommit_payload",
      "replay_precommit", "native_products_by_anchor", "valuation_products_by_anchor", "config"]),
])
def test_retired_forward_producers_fail_before_touching_inputs(name, arguments):
    class UnavailableInput:
        def __getattribute__(self, attribute):
            pytest.fail(f"retired producer accessed input attribute {attribute}")

    with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
        getattr(producer, name)(**dict.fromkeys(arguments, UnavailableInput()))
    assert caught.value.code == "RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED"


class _FrozenRawStrategy:
    name = "SyntheticFrozenRawConformance"
    params = {}

    def calculate_stops(self, features, direction, symbol, **kwargs):
        return 100, 100


def _synthetic_profile(tmp_path, *, symbol="EURUSD", timeframe="H1", source_timeframe="M5", identity=None):
    """Invented thresholds for execution conformance, not market estimates."""
    identity = identity or {"source_identity_sha256": "1" * 64, "source_bars_sha256": "2" * 64,
        "source_audit_sidecar_sha256": "3" * 64, "source_provenance_sha256": "4" * 64,
        "activation_id": "synthetic-cost-conformance", "activation_manifest_sha256": "5" * 64}
    body = {"schema_version": "fxpm.frozen-spread-profile.v1",
        "status": "COMPLETE_CALIBRATION_WITH_EXPLICIT_UNAVAILABLE_GROUPS",
        "calculation_policy": "ONE_TIME_OFFLINE_NO_RUNTIME_RECALCULATION",
        "source_convention": "FINEST_ACTIVATED_CANDIDATE_FIXED_PER_SYMBOL_EXACT_PRIOR_CLOSE_AT_TARGET_OPEN",
        "production_authority": False, "protected_cutoff_utc": "2026-02-01T00:00:00Z",
        "activation": {"id": identity["activation_id"], "manifest_sha256": identity["activation_manifest_sha256"]},
        "cells": [{"symbol": symbol, "timeframe": timeframe, "status": "CALCULATED", "point_price": 0.00001,
            "source_timeframe": source_timeframe, "source_identity": {"identity_sha256": identity["source_identity_sha256"]},
            "source_binding": {"bars_relative_path": "synthetic/bars.csv", "bars_sha256": identity["source_bars_sha256"],
                "sidecar_sha256": identity["source_audit_sidecar_sha256"], "provenance_sha256": identity["source_provenance_sha256"],
                "target_bars_relative_path": "synthetic/bars.csv", "target_bars_sha256": identity["source_bars_sha256"]},
            "tiers": {"P75": {"spread_points": 9, "full_spread_price": 9 * 0.00001,
                              "half_spread_price": 9 * 0.00001 / 2, "usability": "POSITIVE_QUANTILE"},
                      "P95": {"spread_points": 16, "full_spread_price": 16 * 0.00001,
                              "half_spread_price": 16 * 0.00001 / 2, "usability": "POSITIVE_QUANTILE"}}}]}
    path = tmp_path / "synthetic_frozen_profile.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body), encoding="utf-8")
    profile = FrozenSpreadProfile(path)
    return {level: profile.resolve(symbol, timeframe, level, point=0.00001,
            source_identity_sha256=identity["source_identity_sha256"]) for level in ("P75", "P95")}


def _real_frozen_engine_account(tmp_path):
    """Actual engine output on invented bars, including a midnight swap."""
    tiers = _synthetic_profile(tmp_path)
    index = pd.date_range("2026-01-05T21:00:00Z", periods=5, freq="h")
    frame = pd.DataFrame({"Open": 1.0, "High": 1.0002, "Low": 0.9998,
        "Close": 1.0001, "Spread": 5.0, "ATR_14": 0.001,
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    config = PipelineConfig(use_spread=True, backtest_spread_mode="conservative_max",
        initial_capital=10_000, swap_model_enabled=True)
    spec = InstrumentSpec("EURUSD", 4, pip_value=10, point=0.00001, tick_size=0.00001,
        tick_value=1, min_lot=0.01, max_lot=100, volume_step=0.01, asset_class="fx", swap_long=-2.0)
    strategy = _FrozenRawStrategy()
    signals = pd.Series([1, 0, 0, 0, 0], index=index)
    observations = pd.Series(5.0, index=index)
    result = Backtester(config).run(frame, signals, "EURUSD", strategy, spec=spec, timeframe="H1",
        warmup_bars=1, spread_tier=tiers["P75"], entry_spread_observations=observations)
    m5_index = pd.date_range(index[0], index[-1] + pd.Timedelta(minutes=55), freq="5min")
    m5 = pd.DataFrame({"Open": 1.0001, "Spread": 1.0}, index=m5_index)
    half, _, meta = Backtester(config)._build_half_spread_arrays(m5, spec, spread_tier=tiers["P75"], timeframe="H1")
    tape = producer._M5ValuationTape("EURUSD", m5, m5_index, half, {"spread_model": meta},
        {"instrument_spec": asdict(spec)}, {"symbol": "EURUSD", "timeframe": "M5"})
    specialist = replace(_specialist(), symbol="EURUSD", timeframe="H1",
        cell_id=book.compute_cell_id("EURUSD", "H1", "TREND"))
    account = producer._position_evidence(specialist=specialist, strategy=strategy, features=frame,
        spec=spec, config=config, result=result, tier=tiers["P75"], warmup_bars=1, valuation_tape=tape)
    assert account["trades"] and account["derived"]["transactions"]
    return account, result, frame, spec, config, strategy, specialist, tape, tiers


def _mechanical_candidate_from_account(account, owner):
    """Test-built consumer input; production account-to-candidate wiring is absent."""
    assert len(account["derived"]["positions"]) == 1
    position = account["derived"]["positions"][0]
    lots = position["lots"]
    realized = next(row for row in account["derived"]["transactions"]
                    if row["kind"] == "REALIZED_GROSS" and row["position_id"] == position["position_id"])
    price_per_lot = realized["cash"] / lots
    swap_per_lot = sum(row["cash"] for row in position["financing_events"]) / lots
    marks = tuple(portfolio.MarkPoint(row["effective_at_utc"], (row["equity"] - row["cash"]) / lots,
                                     observation_time=row["effective_at_utc"])
                  for row in account["m5_marks"] if row["state"] == "AVAILABLE"
                  and any(item["position_id"] == position["position_id"] for item in row["inventory"]))
    spec = account["instrument_spec"]
    return portfolio.CandidateTrade(
        candidate_id=position["position_id"], cell_id=account["cell_id"], symbol=account["symbol"],
        timeframe=account["timeframe"], regime=owner.regime,
        strategy_name="SYNTHETIC_E891_MECHANICAL_CONSUMER_ONLY", promotion_status="HISTORICAL_OOS_REPLAY",
        asset_class=spec["asset_class"], risk_groups=(),
        entry_time=account["source_rows"][position["entry_bar"]]["open_utc"],
        direction=1 if position["side"] == "LONG" else -1,
        stop_risk_per_lot=position["gross_stop_risk_per_lot"], margin_per_lot=position["margin_per_lot"],
        volume_min=spec["min_lot"], volume_max=spec["max_lot"], volume_step=spec["volume_step"],
        priority_score=1.0, legs=(portfolio.ExitLeg(realized["effective_at_utc"], 1.0,
            price_per_lot + swap_per_lot, price_pnl_per_entry_lot=price_per_lot,
            swap_per_entry_lot=swap_per_lot, exit_reason="SYNTHETIC_MECHANICAL_TERMINAL_CLOSE"),),
        marks=marks, source_schema=producer.POSITION_SOURCE_SCHEMA,
        margin_model=position["margin_model"]).normalized()


def test_position_events_v2_is_rebuilt_and_exact_compared(tmp_path):
    """Exact raw evidence: reject both PnL and risk changes to genuine events."""
    account, result, frame, spec, config, strategy, owner, tape, tiers = _real_frozen_engine_account(tmp_path)
    for field in ("pnl_dollars", "risk_amount"):
        changed = copy.deepcopy(result)
        changed["position_events"][0][field] += 1
        with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
            producer._position_evidence(specialist=owner, strategy=strategy, features=frame, spec=spec, config=config,
                result=changed, tier=tiers["P75"], warmup_bars=1, valuation_tape=tape)
        assert caught.value.code == "BACKTEST_POSITION_EVENTS_V2_RAW_REVERIFY_FAILED"
    assert producer._reconcile_tier_account(account) == account["derived"]


def test_partial_position_fails_typed_until_raw_ledger_has_partial_quantity(tmp_path):
    """Exact raw evidence: the producer refuses an actual two-leg governance close."""
    frame = backtester_fixture._frame().assign(Spread=5.0)
    spec = backtester_fixture._spec(symbol="AUDUSD", point=0.00001, tick_size=0.00001, tick_value=1.0)
    config = PipelineConfig(research_mode=True, use_spread=True, backtest_spread_mode="conservative_max",
        swap_model_enabled=False, discovery_risk_per_trade_pct=1.0)
    tiers = _synthetic_profile(tmp_path, symbol="AUDUSD", timeframe="D1")
    strategy = backtester_fixture._AtrHoldStrategy()
    backtester = Backtester(config)
    result = backtester.run(frame, backtester_fixture._signals(frame), "AUDUSD", strategy, spec=spec,
        timeframe="D1", warmup_bars=1, spread_tier=tiers["P75"],
        entry_spread_observations=pd.Series(5.0, index=frame.index),
        governance_policy={"name": "partial_tp_runner", "partial_at_r": 0.1,
            "partial_close_pct": 33.0, "trail_atr_mult": 100.0, "release_tp_after_r": 100.0})
    assert len(result["trades"]) == 2 and len(result["position_events"]) == 1
    assert result["trades"][0]["exit_reason"] == "governance_partial"
    assert result["trades"][0] != result["trades"][1]
    index = pd.date_range(frame.index[0], frame["_BAR_CLOSE_TS"].iloc[-1] - pd.Timedelta(minutes=5), freq="5min")
    valuation = pd.DataFrame({"Open": 1.001, "Spread": 5.0}, index=index)
    half, _, meta = backtester._build_half_spread_arrays(valuation, spec, spread_tier=tiers["P75"], timeframe="D1")
    tape = producer._M5ValuationTape("AUDUSD", valuation, index, half, {"spread_model": meta},
        {"instrument_spec": asdict(spec)}, {"symbol": "AUDUSD", "timeframe": "M5"})
    with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
        producer._position_evidence(specialist=_specialist(), strategy=strategy, features=frame,
            spec=spec, config=config, result=result, tier=tiers["P75"], warmup_bars=1, valuation_tape=tape)
    assert caught.value.code == "PARTIAL_OR_MULTI_LEG_POSITION_NOT_REPRESENTABLE"


def test_conservative_margin_model_and_amount_are_candidate_roster_bound(tmp_path):
    """Exact raw formula plus mechanical candidate binding; producer wiring is absent."""
    account, _, _, spec, _, _, owner, *_ = _real_frozen_engine_account(tmp_path)
    position = account["derived"]["positions"][0]
    assert position["margin_model"] == producer.MARGIN_MODEL
    assert position["margin_per_lot"] == abs(position["entry_fill"]) * (spec.tick_value / spec.tick_size)
    assert position["margin_amount"] == position["margin_per_lot"] * position["lots"]
    changed = copy.deepcopy(account)
    changed["derived"]["positions"][0]["margin_amount"] *= 0.01
    assert producer._reconcile_tier_account(changed) != changed["derived"]
    candidate = _mechanical_candidate_from_account(account, owner)
    record = portfolio.current_refit_candidate_record(candidate)
    assert portfolio.candidate_trade_from_record(record) == candidate
    assert candidate.margin_model == record["margin_model"] == producer.MARGIN_MODEL
    assert candidate.margin_per_lot == record["margin_per_lot"] == position["margin_per_lot"]
    original = portfolio.portfolio_replay_candidate_roster_receipt([candidate])
    for mutation in (replace(candidate, margin_model="caller-leverage-500"),
                     replace(candidate, margin_per_lot=candidate.margin_per_lot * 0.01)):
        changed_receipt = portfolio.portfolio_replay_candidate_roster_receipt([mutation.normalized()])
        assert changed_receipt["candidate_roster_sha256"] != original["candidate_roster_sha256"]


def test_common_m5_open_marks_use_executable_bid_and_swap_only(tmp_path):
    """Exact raw marks and a real mechanical consumer; production wiring is absent."""
    account, _, _, spec, _, _, owner, tape, _ = _real_frozen_engine_account(tmp_path)
    position = account["derived"]["positions"][0]
    holding = [mark for mark in account["m5_marks"] if mark["state"] == "AVAILABLE" and mark["inventory"]]
    first = holding[0]
    entry = pd.Timestamp(account["source_rows"][position["entry_bar"]]["open_utc"])
    assert pd.Timestamp(first["effective_at_utc"]) > entry
    index = tape.opens.get_loc(pd.Timestamp(first["effective_at_utc"]))
    mid = Decimal(str(tape.frame["Open"].iloc[index]))
    half = Decimal(str(tape.entry_half_spread[index]))
    assert first["mid"] == float(mid)
    assert first["bid"] == float(mid - half)
    candidate = _mechanical_candidate_from_account(account, owner)
    assert candidate.marks and candidate.marks[0].time > candidate.entry_time
    expected = spec.calculate_tick_profit(position["entry_fill"], float(mid - half), 1.0, 1)
    assert candidate.marks[0].pnl_per_current_lot == pytest.approx(expected)
    mid_only = spec.calculate_tick_profit(position["entry_fill"], float(mid), 1.0, 1)
    assert candidate.marks[0].pnl_per_current_lot != pytest.approx(mid_only)
    changed_frame = tape.frame.copy(deep=True)
    changed_frame.iloc[index, changed_frame.columns.get_loc("Open")] += 0.0001
    changed_marks = producer._tier_m5_marks(account, replace(tape, frame=changed_frame))
    changed_first = next(mark for mark in changed_marks if mark["effective_at_utc"] == first["effective_at_utc"])
    assert changed_first["bid"] != first["bid"] and changed_first["equity"] != first["equity"]
    assert changed_first["cash"] == first["cash"]
    mark = next(mark for mark in holding if mark["cash"] != account["initial_cash"])
    assert mark["bid"] == float(Decimal(str(mark["mid"])) - Decimal(str(account["tier"]["full_spread_price"] / 2)))
    assert mark["equity"] == mark["cash"] + spec.calculate_tick_profit(
        position["entry_fill"], mark["bid"], position["lots"], 1)
    assert mark["cash"] == account["initial_cash"] + sum(
        event["cash"] for event in position["financing_events"]
        if pd.Timestamp(event["effective_at_utc"]) <= pd.Timestamp(mark["effective_at_utc"]))
    policy = portfolio.PortfolioPolicy(allowed_promotion_statuses=("HISTORICAL_OOS_REPLAY",),
        max_open_risk_pct=10.0, max_same_symbol_open_risk_pct=10.0,
        max_factor_group_open_risk_pct=10.0, max_margin_utilization_pct=90.0)
    # Explicitly synthetic consumer perturbations, never observed account PnL.
    mark_time = candidate.marks[0].time
    adverse_per_lot = -300_000.0
    marked = replace(candidate, marks=(portfolio.MarkPoint(mark_time, adverse_per_lot),))
    unmarked = replace(marked, marks=())
    newcomer = replace(candidate, candidate_id="synthetic-e891-concurrent", cell_id="synthetic-e891-concurrent-cell",
        symbol="XAUUSD", asset_class="metal", strategy_name="SYNTHETIC_E891_CONCURRENT_CONSUMER_ONLY",
        entry_time=mark_time, stop_risk_per_lot=500.0, margin_per_lot=1000.0, marks=(),
        legs=(portfolio.ExitLeg(mark_time + pd.Timedelta(minutes=5), 1.0, 0.0),)).normalized()
    admission_policy = replace(policy, target_risk_per_trade_pct=0.25, max_risk_per_trade_pct=0.25,
        max_open_risk_pct=0.45, max_same_symbol_open_risk_pct=0.45,
        max_factor_group_open_risk_pct=0.45, min_margin_level_pct=1.0, min_free_margin_pct=0.0)
    stale = portfolio.SynchronizedPortfolioSimulator(admission_policy).run([unmarked, newcomer])
    causal = portfolio.SynchronizedPortfolioSimulator(admission_policy).run([marked, newcomer])
    assert stale["summary"]["accepted_positions"] == len(stale["closed_positions"]) == 2
    assert stale["summary"]["blocked_positions"] == len(stale["blocked_positions"]) == 0
    assert stale["summary"]["mark_updates"] == 0
    assert causal["summary"]["accepted_positions"] == len(causal["closed_positions"]) == 1
    assert causal["summary"]["blocked_positions"] == len(causal["blocked_positions"]) == 1
    assert causal["summary"]["blocked_by_reason"] == {"EXISTING_TOTAL_STOP_RISK_CAP": 1}
    assert causal["blocked_positions"][0]["candidate_id"] == newcomer.candidate_id
    assert causal["summary"]["mark_updates"] == 1
    volume = next(row["volume"] for row in stale["closed_positions"] if row["candidate_id"] == candidate.candidate_id)
    marked_row = next(row for row in causal["path"] if pd.Timestamp(row["timestamp"]) == mark_time)
    stale_row = next(row for row in stale["path"] if pd.Timestamp(row["timestamp"]) == mark_time)
    assert marked_row["balance"] == admission_policy.starting_balance
    assert marked_row["unrealized_pnl"] == pytest.approx(volume * adverse_per_lot)
    assert marked_row["mtm_equity"] == pytest.approx(admission_policy.starting_balance + volume * adverse_per_lot)
    assert marked_row["admission_events"] == 0 and marked_row["open_positions"] == 1
    assert stale_row["admission_events"] == 1 and stale_row["open_positions"] == 2
    assert stale_row["unrealized_pnl"] == 0.0
    assert causal["summary"]["valuation_event_order"] == "close_existing_then_mark_existing_then_admit"

    close_time = candidate.legs[-1].exit_time
    replacement = replace(candidate, candidate_id="synthetic-e891-same-cell-replacement", entry_time=close_time,
        marks=(), legs=(portfolio.ExitLeg(close_time + pd.Timedelta(minutes=5), 1.0, 0.0),)).normalized()
    closed_first = portfolio.SynchronizedPortfolioSimulator(policy).run([candidate, replacement])
    assert closed_first["summary"]["accepted_positions"] == len(closed_first["closed_positions"]) == 2
    assert closed_first["summary"]["blocked_positions"] == len(closed_first["blocked_positions"]) == 0
    assert closed_first["summary"]["mark_updates"] == len(candidate.marks) > 0
    close_row = next(row for row in closed_first["path"] if pd.Timestamp(row["timestamp"]) == close_time)
    first_closed = next(row for row in closed_first["closed_positions"] if row["candidate_id"] == candidate.candidate_id)
    next_closed = next(row for row in closed_first["closed_positions"] if row["candidate_id"] == replacement.candidate_id)
    assert first_closed["exit_time"] == next_closed["entry_time"] == close_time.isoformat()
    assert close_row["realized_pnl_at_timestamp"] == pytest.approx(first_closed["net_pnl"])
    assert close_row["admission_events"] == 1 and close_row["open_positions"] == 1
    assert closed_first["summary"]["valuation_event_order"] == "close_existing_then_mark_existing_then_admit"


def test_stop_risk_uses_gross_broker_price_loss(tmp_path):
    """Exact raw risk plus mechanical candidate binding; producer wiring is absent."""
    account, _, _, spec, _, _, owner, *_ = _real_frozen_engine_account(tmp_path)
    position = account["derived"]["positions"][0]
    expected = abs(position["entry_fill"] - position["stop_price"]) * (spec.tick_value / spec.tick_size)
    assert position["gross_stop_risk_per_lot"] == expected
    assert any(event["cash"] != 0 for event in position["financing_events"])
    candidate = _mechanical_candidate_from_account(account, owner)
    record = portfolio.current_refit_candidate_record(candidate)
    assert candidate.stop_risk_per_lot == record["stop_risk_per_lot"] == expected
    assert portfolio.candidate_trade_from_record(record) == candidate
    original = portfolio.portfolio_replay_candidate_roster_receipt([candidate])
    changed = replace(candidate, stop_risk_per_lot=candidate.stop_risk_per_lot + 1.0).normalized()
    assert portfolio.portfolio_replay_candidate_roster_receipt([changed])["candidate_roster_sha256"] != original["candidate_roster_sha256"]



def test_intrabar_exit_keeps_m5_time_unavailable_and_terminal_facts_explicit(tmp_path):
    account, _, frame, spec, config, strategy, owner, tape, tiers = _real_frozen_engine_account(tmp_path)
    changed = frame.copy(deep=True)
    changed.iloc[2, changed.columns.get_loc("Low")] = 0.98
    result = Backtester(config).run(changed, pd.Series([1, 0, 0, 0, 0], index=changed.index),
        "EURUSD", strategy, spec=spec, timeframe="H1", warmup_bars=1,
        spread_tier=tiers["P75"], entry_spread_observations=pd.Series(5.0, index=changed.index))
    stopped = producer._position_evidence(specialist=owner, strategy=strategy, features=changed,
        spec=spec, config=config, result=result, tier=tiers["P75"], warmup_bars=1, valuation_tape=tape)
    assert stopped["derived"]["positions"][0]["exit_phase"] == "INTRABAR_ORDER_ONLY"
    missing = [mark for mark in stopped["m5_marks"] if mark["state"] == "UNAVAILABLE_INTRABAR_EXIT_TIME"]
    assert missing and all(mark["cash"] is None and mark["inventory"] is None and mark["equity"] is None for mark in missing)
    assert account["derived"]["terminal_inventory"] == []
    assert account["derived"]["terminal_simulation"] == "END_OF_DATA_LIQUIDATED"
    assert account["derived"]["cash_inventory"][-1]["engine_equity"] == account["derived"]["cash_inventory"][-1]["cash"]


_RETAINED_RAW_GRAPHS = {
    "v1": (
        "frozen_tier_raw_account_verification/attempt_009_pytest_tmp/frozen-tier-canonical0/spool",
        "77ec9522a0dc84d7b321bfb21f98fb6ae303957107124f29ddcc42a11a89512b",
    ),
    "v2": (
        "frozen_spread_missing_observation_verification/attempt_001_pytest_tmp/frozen-tier-canonical0/spool",
        "80683fe9d18e9455478de203124bfc9edadfeeab4630d9a837dc5bf5e4fdc7ba",
    ),
}


def load_retained_raw_transport(tmp_path, *, version="v2", return_inputs=False):
    """Reopen original engineering bytes; native/config paths stay read only.

    These deliberately nonportable archives fail if their original dependencies
    are missing. Copying nodes gives each tamper test its own writable spool.
    """
    import pm_production_config_binding as binding

    relative, digest = _RETAINED_RAW_GRAPHS[version]
    original = Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007" / relative
    packet_relative = f"objects/sha256/{digest[:2]}/{digest}.canonical-json"
    packet = original / packet_relative
    original_bytes = packet.read_bytes()
    assert hashlib.sha256(original_bytes).hexdigest() == digest, "retained original packet changed"
    destination = tmp_path / "spool"
    assert destination.resolve().is_relative_to(tmp_path.resolve())
    shutil.copytree(original, destination)
    facts = json.loads((original / "generation-context.json").read_text(encoding="utf-8"))
    context = authority_spool.reopen_generation_context(destination,
        expected_generation_id=facts["generation_id"],
        expected_native_universe_sha256=facts["native_universe_sha256"],
        expected_config_sha256=facts["config_sha256"])
    descriptor = {"schema_version": "fxpm.authority-spool-artifact-descriptor.v1",
        "format": "canonical-json", "relative_path": packet_relative,
        "sha256": digest, "size_bytes": len(original_bytes)}
    body = producer._read_descriptor_json(context, descriptor, code="RETAINED_ORIGINAL_RAW_PACKET")
    assert body["schema_version"] == f"fxpm.frozen-tier-raw-accounts.{version}"
    result = context, body["anchor"], body, causal_producer.FrozenTierRawAccountsArtifact(descriptor)
    if not return_inputs:
        return result
    assert version == "v2", "v1 has strict archive read semantics only"
    precommit = body["original_precommit_anchor"]
    global_body = producer._read_descriptor_json(context, body["original_global_precommit_descriptor"],
        code="RETAINED_ORIGINAL_GLOBAL_INPUT")
    config, deployment, _ = producer._reopen_retained_deployment(context=context,
        descriptors=body["deployment_source_descriptors"], original_receipt=global_body["deployment_binding"],
        config_record=body["pipeline_config_binding"])
    full_book = producer._read_descriptor_json(context, body["full_book_descriptor"], code="RETAINED_ORIGINAL_BOOK")
    owner_source = body["owner_source"]
    owner = producer._reopen_owner_capture(context=context, anchor=body["anchor"], book_body=full_book,
        cell_id=body["cell_projection"]["cell_id"], symbol_receipt=owner_source["symbol_receipt"],
        adapter_index_descriptor=owner_source["adapter_index_descriptor"])
    _, execution = causal_producer._reopen_frozen_transport_source(context, body["execution_source"], config)
    _, fine = causal_producer._reopen_frozen_transport_source(context, body["spread_source"], config)
    payload, original_config = binding.load_effective_pipeline_config(Path(body["pipeline_config_binding"]["active_config_path"]))
    frozen_config = binding.freeze_production_pipeline_config(context,
        active_config_path=Path(body["pipeline_config_binding"]["active_config_path"]),
        config_payload=payload, supplied_config=original_config)
    assert producer._pipeline_config_identity(original_config) == producer._pipeline_config_identity(config)
    profile_descriptor = body["frozen_profile_descriptor"]
    native_prefix._read_spool_descriptor(context, profile_descriptor, expected_format="json-profile", collect=True)
    arguments = dict(context=context, anchor=body["anchor"], specialists=[owner],
        native_products={f"{owner.symbol}|{owner.timeframe}": execution},
        valuation_products={f"{owner.symbol}|M5": fine}, precommit_anchor=precommit,
        capital_phase=body["capital_phase"], capital_window=body["capital_window"], config=config,
        deployment=deployment, account_genesis=body["genesis"], cell_projection=body["cell_projection"],
        spread_source_products={f"{owner.symbol}|M5": fine},
        frozen_profile_path=context.root / profile_descriptor["relative_path"],
        frozen_pipeline_config=frozen_config, owner_symbol_receipt=owner_source["symbol_receipt"],
        original_adapter_index_descriptor=owner_source["adapter_index_descriptor"],
        original_global_precommit_descriptor=body["original_global_precommit_descriptor"])
    return (*result, arguments)


def test_original_retained_owner_refuses_current_code_semantic_rebinding(tmp_path):
    # Original archived identity is valid; current-code owner migration is not
    # implemented. The real forward reopener must preserve that distinction.
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="SELECTED_ATTEMPT_RAW_V2_REVERIFY_FAILED") as observed:
        load_retained_raw_transport(tmp_path, return_inputs=True)
    cause = observed.value.__cause__
    assert isinstance(cause, producer.current_refit.CurrentRefitArtifactError)
    assert cause.args == ("TRIAL_DEFINITION_SEMANTICS_INVALID",)


def test_mixed_current_trial_original_recipe_cannot_launder_archived_code_pins(tmp_path):
    """Hostile mixed-generation engineering input, never an original archive."""
    context, _, packet, _ = load_retained_raw_transport(tmp_path)
    symbol = packet["owner_source"]["symbol_receipt"]["symbol"]
    _, entries = producer._verified_manifest(context, packet["owner_source"]["symbol_receipt"], symbol)
    prefix = "historical-anchor/historical-run-001/anchor-01/winner-search/D1/TREND/frozen/attempt-0000/"
    original = {name: producer._manifest_body(context, entries, prefix + name,
        code="ORIGINAL_ARCHIVED_" + name.upper()) for name in (
            "winner_process", "horizon_process", "trial_definition", "refit_recipe")}
    process, horizon, trial, recipe = (original[name] for name in (
        "winner_process", "horizon_process", "trial_definition", "refit_recipe"))
    original_book = producer._read_descriptor_json(context, packet["full_book_descriptor"], code="ORIGINAL_ARCHIVED_BOOK")
    cell = next(value for value in original_book["cells"] if value["cell_id"] == packet["cell_projection"]["cell_id"])
    arguments = dict(symbol=cell["symbol"], timeframe=cell["timeframe"], regime=cell["regime"],
        strategy_id=process["strategy_id"], lookback_kind=horizon["lookback_kind"],
        lookback_value=horizon["lookback_value"], fit_start_utc=horizon["fit_start_utc"],
        fit_end_utc=horizon["fit_end_utc"], latest_closed_bar_utc=horizon["latest_closed_bar_utc"],
        source_snapshot_sha256=horizon["source_snapshot_sha256"], source_bar_count=horizon["source_bar_count"],
        seed=recipe["seed"])
    current = producer.current_refit.build_current_refit_search_artifacts_v2(parameters=trial["parameters"], **arguments)
    assert process["strategy_code_sha256"] == producer.current_refit.RETAINED_STRATEGY_SOURCE_V1_SHA256
    assert current.winner_process == {
        **process,
        "strategy_code_sha256": producer.current_refit.RETAINED_STRATEGY_V1_SUCCESSOR_SHA256,
    }
    assert current.horizon_process == horizon
    assert current.trial_definition != trial
    assert current.refit_recipe["version_pins"]["pm_core_sha256"] != recipe["version_pins"]["pm_core_sha256"]
    assert producer.current_refit.verify_current_refit_search_artifacts_bytes_v2(
        winner_process_bytes=producer._canonical_bytes(current.winner_process),
        horizon_process_bytes=producer._canonical_bytes(current.horizon_process),
        trial_definition_bytes=producer._canonical_bytes(current.trial_definition),
        refit_recipe_bytes=producer._canonical_bytes(current.refit_recipe), **arguments,
    ) == current
    # The original artifact bodies and receipts stay untouched. Only this hostile
    # input combines a coherent current chain with the untouched original recipe.
    with pytest.raises(producer.current_refit.CurrentRefitArtifactError,
                       match="^REFIT_RECIPE_SEMANTICS_INVALID$"):
        producer.current_refit.verify_current_refit_search_artifacts_bytes_v2(
            winner_process_bytes=producer._canonical_bytes(current.winner_process),
            horizon_process_bytes=producer._canonical_bytes(current.horizon_process),
            trial_definition_bytes=producer._canonical_bytes(current.trial_definition),
            refit_recipe_bytes=producer._canonical_bytes(recipe), **arguments)


def _frozen_raw_transport(tmp_path, *, return_inputs=False):
    """Run current v2 transport using the genuine retained original owner graph."""
    context, anchor, _, _, arguments = load_retained_raw_transport(tmp_path, return_inputs=True)
    replay = producer._execute_anchor_replay(**arguments)
    bodies = producer._ledger_bodies(replay, starting_balance=Decimal(str(arguments["config"].initial_capital)))
    descriptor = producer._write_raw_ledger(context, bodies["FROZEN_TIER_RAW_ACCOUNTS"], role="FROZEN_TIER_RAW_ACCOUNTS")
    body = producer._read_descriptor_json(context, descriptor, code="CURRENT_RAW_REOPEN")
    result = context, anchor, body, causal_producer.FrozenTierRawAccountsArtifact(descriptor)
    return (*result, arguments) if return_inputs else result


_LANE127_QA = Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007/cell_interval_integration"
_LANE127_PACKET = {"sha256": "53ee6e3c859ae78cd26a6e1496ed3553739f459520a9ae46dfc2a97a9a20a2c4", "bytes": 3192452}
_LANE127_STATES = {}
_LANE127_ACTIVE = None
_LANE127_AUDIT_INSTALLED = False


def _lane127_audit(event, arguments):
    state = _LANE127_ACTIVE
    if state is None:
        return
    if event == "open":
        mode, flags = arguments[1:3]
        writing = ((isinstance(mode, str) and any(c in mode for c in "wax+"))
            or (isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)))
        targets = arguments[:1]
    elif event in {"os.mkdir", "os.remove", "os.rmdir", "os.chmod", "os.utime", "os.truncate"}:
        writing, targets = True, arguments[:1]
    elif event in {"os.rename", "os.link", "os.symlink"}:
        writing, targets = True, arguments[:2]
    else:
        return
    for target in targets:
        if not isinstance(target, (str, bytes, os.PathLike)):
            continue
        path = Path(os.fsdecode(target)).resolve()
        if event == "open" and path.is_relative_to(state["project"] / "data"):
            state["forbidden"].append("REAL_DATA_OPEN:" + str(path))
            pytest.fail("lane127 opened real data")
        if not writing or (event == "os.mkdir" and path.is_dir()):
            continue
        own = path.is_relative_to(state["root"])
        lock = (event == "open" and path in state["locks"] and isinstance(flags, int)
            and bool(flags & os.O_APPEND) and not bool(flags & os.O_TRUNC))
        cache = path.parent == state["project"] / "__pycache__" and (
            path.name.startswith("pm_core.") and any(token in path.name for token in (".nbi", ".nbc")))
        cache_probe = (state.get("cache_probe_depth", 0) > 0 and path.parent == state["project"] / "__pycache__"
            and path.name.startswith("tmp") and event in {"open", "os.remove"})
        if not (own or lock or cache or cache_probe):
            state["forbidden"].append("WRITE:" + event + ":" + str(path))
            pytest.fail("lane127 write outside its sole malformed clone: " + str(path))


@contextmanager
def _lane127_runtime_guards(transport):
    """Guard only this dependent test, leaving unrelated suite work alone."""
    global _LANE127_ACTIVE, _LANE127_AUDIT_INSTALLED
    import pm_core as core
    from numba.core import caching as numba_caching
    from tools import build_spread_profile as profile_builder
    state = _LANE127_STATES[Path(transport[0].root).resolve()]
    assert _current_raw_file_identity(state["packet_path"]) == _LANE127_PACKET
    assert _LANE127_ACTIVE is None
    if not _LANE127_AUDIT_INSTALLED:
        sys.addaudithook(_lane127_audit)
        _LANE127_AUDIT_INSTALLED = True

    def denied(label):
        def refuse(*_args, **_kwargs):
            state["forbidden"].append(label)
            pytest.fail("lane127 forbidden financial/generation call: " + label)
        return refuse

    original_writer = producer._write_raw_ledger
    original_ensure_cache = numba_caching._CacheLocator.ensure_cache_path
    def ensure_cache(locator):
        exact = (Path(getattr(locator, "_py_file", "")).resolve() == state["project"] / "pm_core.py"
            and Path(locator.get_cache_path()).resolve() == state["project"] / "__pycache__")
        if exact:
            state["cache_probe_depth"] = state.get("cache_probe_depth", 0) + 1
        try:
            return original_ensure_cache(locator)
        finally:
            if exact:
                state["cache_probe_depth"] -= 1
    def malformed_writer(context, body, *, role):
        assert Path(context.root).resolve() == state["root"]
        raw = producer._canonical_bytes(body, 64 * 1024 * 1024)
        assert hashlib.sha256(raw).hexdigest() != _LANE127_PACKET["sha256"], "valid packet reissue forbidden"
        descriptor = original_writer(context, body, role=role)
        state["malformed_descriptors"].append(dict(descriptor))
        return descriptor

    guards = [(Backtester, "run"), (Backtester, "_run_python_loop"), (core, "_backtest_loop_numba"),
        (producer, "_execute_anchor_replay"), (producer, "produce_forward_oos_replay_artifacts"),
        (Backtester, "build_evidence_source_seed_result"), (profile_builder, "build_native_profile"),
        (sys.modules[__name__], "_prepare_current_raw_inputs"),
        (sys.modules[__name__], "_frozen_raw_transport"),
        (sys.modules[__name__], "load_retained_raw_transport"),
        (sys.modules[__name__], "_current_raw_transport_session"),
        (sys.modules[__name__], "_clone_current_raw_transport_for_tamper"),
        (causal_producer.BoundedCausalEvidenceFactory, "build_after_historical_run"),
        (causal_producer.BoundedCausalEvidenceProducer, "__call__")]
    guards += [(producer.search_execution, name) for name in ("execute_current_observed_cell",
        "execute_historical_observed_cell", "fit_native_timeframe_for_search", "fit_terminal_native_timeframe",
        "_run_tier_partition", "_build_planned_attempt_evidence")]
    guards += [(producer.native_prefix, name) for name in ("produce_activated_native_prefix",
        "produce_activated_native_prefixes", "_produce_activated_native_prefix_single",
        "_produce_terminal_prefix", "_produce_cutoff_unavailable_prefix")]
    try:
        with pytest.MonkeyPatch.context() as patcher:
            for target, name in guards:
                patcher.setattr(target, name, denied(target.__name__ + "." + name))
            patcher.setattr(producer, "_write_raw_ledger", malformed_writer)
            patcher.setattr(numba_caching._CacheLocator, "ensure_cache_path", ensure_cache)
            _LANE127_ACTIVE = state
            yield
    finally:
        _LANE127_ACTIVE = None
        assert _current_raw_file_identity(state["packet_path"]) == _LANE127_PACKET
        assert _current_raw_file_identity(state["root"] / state["descriptor"]["relative_path"]) == _LANE127_PACKET
        assert not state["forbidden"], state["forbidden"]


def _lane127_validate_only(transport, arguments, case, expected):
    """E2031 permits only these five early, writer/feature/engine-free refusals."""
    allowed = {"missing_fine": "FROZEN_TIER_CAPTURED_FINE_TIMEFRAME_UNAVAILABLE",
        "frozen_pipeline_config": "FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_UNAVAILABLE",
        "owner_symbol_receipt": "FROZEN_TIER_ORIGINAL_OWNER_CAPTURE_UNAVAILABLE",
        "original_adapter_index_descriptor": "FROZEN_TIER_ORIGINAL_OWNER_CAPTURE_UNAVAILABLE",
        "original_global_precommit_descriptor": "FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT_UNAVAILABLE"}
    assert allowed.get(case) == expected
    state = _LANE127_STATES[Path(transport[0].root).resolve()]
    def denied(*_args, **_kwargs):
        state["forbidden"].append("VALIDATION_REACHED_FEATURE_OR_WRITER:" + case)
        pytest.fail("validation-only entry reached a feature or writer")
    writers = ("freeze_generation_context", "write_canonical_json_artifact", "write_opaque_artifact",
        "reserve_transient_publication", "open_symbol_sink", "_publish_exact_json", "_publish_temp_exclusive",
        "_write_streamed_object", "_ensure_directory", "_open_exclusive")
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(producer.FeatureComputer, "compute_all", denied)
        patcher.setattr(producer, "_features_and_prefix_parity", denied)
        patcher.setattr(producer, "_selection_prefix_match", denied)
        for name in ("_write_raw_ledger", "_persist_frozen_profile", "_persist_deployment_sources"):
            patcher.setattr(producer, name, denied)
        for name in writers:
            patcher.setattr(authority_spool, name, denied)
        with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
            state["validation_entry"](**arguments)
    assert caught.value.code == expected
    assert not state["forbidden"]
    state["validation_calls"].append({"case": case, "code": caught.value.code})


@pytest.fixture(scope="session")
def completed_raw_transport(tmp_path_factory, pytestconfig):
    """Seed the completed 53ee transport; never execute its financial writer."""
    attribute = "_fxpm_lane127_completed_transport"
    if hasattr(pytestconfig, attribute):
        yield getattr(pytestconfig, attribute)["transport"]
        return
    project = Path(__file__).resolve().parents[1]
    basetemp = tmp_path_factory.getbasetemp().resolve(strict=True)
    runtime = _LANE127_QA / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    session_root = (basetemp if basetemp.is_relative_to(runtime) else
        Path(tempfile.mkdtemp(prefix="lane127-session-", dir=runtime)).resolve(strict=True))
    original = _LANE127_QA.parent / "descriptor_raw_transport_verification"
    receipt_path = original / "receipt.json"
    assert _current_raw_file_identity(receipt_path) == {
        "sha256": "3f6be72ce6850f881864939c3b9fdbe9ab8711a8522720f5cb54c8d0d9574aa4", "bytes": 20965}
    closed_receipt = json.loads(receipt_path.read_bytes())
    launch_path = original / "results/current_transport_001/result.json"
    recovery_path = original / "results/current_reader_recovery_001/result.json"
    launch, recovery = (json.loads(path.read_bytes()) for path in (launch_path, recovery_path))
    assert _current_raw_file_identity(launch_path) == {"sha256": "ece8dbb23c7cdaf54ae342966a4804712b4ab245ddbddfee0b4997b7411cbf74", "bytes": 50985}
    assert _current_raw_file_identity(recovery_path) == {"sha256": "efa1aa67e94802706ab7f833df946815877c293245f5b0cecaaa2d470e75d811", "bytes": 62799}
    assert [(call["tier"], call["status"]) for call in launch["engine_calls"]] == [("P75", "COMPLETED"), ("P95", "COMPLETED")]
    assert all(call["loop"] == "NUMBA" and call["status"] == "COMPLETED" for call in launch["kernel_calls"]) and len(launch["kernel_calls"]) == 2
    assert recovery["status"] == "PASS" and recovery["exit_code"] == 0 and recovery["financial_execution_count"] == 0
    assert recovery["custody_unchanged"] is True and recovery["forbidden_calls"] == []
    retained_path = original / "prepared_input_receipt.json"
    retained = json.loads(retained_path.read_bytes())
    metadata_path = Path(retained["metadata_path"])
    assert _current_raw_file_identity(metadata_path) == retained["metadata_identity"]
    packet_receipt = json.loads((original / "results/current_transport_001/packet-receipt.json").read_bytes())
    source = Path(packet_receipt["spool_root"]).resolve(strict=True)
    descriptor = packet_receipt["descriptor"]
    packet_path = source / descriptor["relative_path"]
    assert _current_raw_file_identity(packet_path) == packet_receipt["identity"] == _LANE127_PACKET
    assert recovery["positive_reader"]["descriptor_sha256"] == descriptor["sha256"]
    binding = json.loads((original / "proposed_current_transport_binding.json").read_bytes())
    source_bindings = {name: value for name, value in closed_receipt["sources"].items() if not name.startswith("tests/")}
    source_bindings.update(json.loads(metadata_path.read_bytes())["source_bindings"])
    assert all(_current_raw_file_identity(project / name) == value for name, value in source_bindings.items())
    protected = [receipt_path, launch_path, recovery_path, retained_path, metadata_path,
        original / "security_obligation_map.json", packet_path, source / "generation-context.json",
        *map(Path, binding["protected_input_files"]), *(project / name for name in source_bindings)]
    custody = {str(path): _current_raw_file_identity(path) for path in protected}
    scratch = session_root / "lane127-completed-53ee"
    scratch.mkdir(exist_ok=False)
    assert scratch.resolve(strict=True).is_relative_to(runtime) and not scratch.is_relative_to(original)
    root = scratch / "spool"
    shutil.copytree(source, root)  # The sole necessary clone in this pytest session.
    native_root = Path(binding["synthetic_native_root"])
    state = {"root": root.resolve(), "project": project,
        "packet_path": packet_path, "descriptor": descriptor, "forbidden": [], "malformed_descriptors": [],
        "validation_entry": producer._execute_anchor_replay, "validation_calls": [],
        "runtime_guard": _lane127_runtime_guards, "validate_only": _lane127_validate_only,
        "locks": {(native_root / name).resolve(strict=True) for name in (".ACTIVE.json.lock", ".TERMS_ACTIVE.json.lock")}}
    _LANE127_STATES[state["root"]] = state
    locks_before = {str(path): _current_raw_file_identity(path) for path in state["locks"]}
    native_before = {name: {"bytes": Path(name).stat().st_size, "mtime_ns": Path(name).stat().st_mtime_ns}
        for name in binding["native_source_stats"]}
    def cache_identity():
        return {str(path): _current_raw_file_identity(path) for path in (project / "__pycache__").glob("pm_core.*")
            if path.is_file() and any(token in path.name for token in (".nbi", ".nbc"))}
    cache_before = cache_identity()
    assert native_before == binding["native_source_stats"]
    try:
        with _lane127_runtime_guards((SimpleNamespace(root=root),)):
            prepared = _reopen_current_raw_inputs(metadata_path, root)
            inputs = prepared["execute_arguments"]
            context = inputs["context"]
            assert context.context_sha256 == retained["generation_context_sha256"]
            body = producer._read_descriptor_json(context, descriptor, code="LANE127_COMPLETED_RAW")
            assert body["anchor"] == inputs["anchor"] and body["schema_version"] == producer.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
            for level in ("P75", "P95"):
                account = body["accounts"][level]
                observed = recovery["positive_reader"]["accounts"][level]
                assert len(account["trades"]) == observed["trade_count"]
                assert len(account["engine_equity"]) == observed["full_axis_rows"] == 34908
                assert account["warmup_bars"] == observed["warmup_bars"] == 32772
            transport = context, body["anchor"], body, causal_producer.FrozenTierRawAccountsArtifact(descriptor), inputs
            state["transport"] = transport
            setattr(pytestconfig, attribute, state)
        yield transport
    finally:
        after = {str(path): _current_raw_file_identity(path) for path in protected}
        locks_after = {str(path): _current_raw_file_identity(path) for path in state["locks"]}
        native_after = {name: {"bytes": Path(name).stat().st_size, "mtime_ns": Path(name).stat().st_mtime_ns}
            for name in binding["native_source_stats"]}
        cache_after = cache_identity()
        same = (custody == after and native_before == native_after
            and locks_before == locks_after and cache_before == cache_after)
        record = {"classification": "SYNTHETIC ENGINEERING", "source_packet": _LANE127_PACKET,
            "source_custody_before": custody, "source_custody_after": after, "source_custody_unchanged": same,
            "clone_count": 1, "clone_path": str(root), "malformed_descriptors": state["malformed_descriptors"],
            "forbidden_calls": state["forbidden"], "financial_execution_count": 0,
            "financial_execution_count_basis": "Guard-enforced prohibition of financial entrypoints, not transparent loop-entry instrumentation",
            "native_stats_before": native_before, "native_stats_after": native_after,
            "locks_before": locks_before, "locks_after": locks_after,
            "pm_core_cache_before": cache_before, "pm_core_cache_after": cache_after,
            "validation_calls": state["validation_calls"],
            "clone_retention": "RETAINED_UNTIL_BOTH_PMS_RECORD_PURPOSE_ENDED_E2033"}
        (session_root / "lane127-fixture-custody.json").write_text(json.dumps(record, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        _LANE127_STATES.pop(state["root"])
        assert same and not state["forbidden"], "lane127 source custody or forbidden-call violation"


@pytest.fixture
def canonical_raw_transport(completed_raw_transport, pytestconfig):
    state = getattr(pytestconfig, "_fxpm_lane127_completed_transport")
    assert state["transport"] is completed_raw_transport
    with state["runtime_guard"](completed_raw_transport):
        yield completed_raw_transport


@_lane127_blocked("LANE127_PARTIAL_BASELINE: saved75/89 trades do not supply the original one-trade, financing and in-test engine baseline")
def test_independent_frozen_accounts_use_real_engine_writer_and_reopen(canonical_raw_transport):
    raise _Lane127WitnessUnavailable("Original one-trade/financing/in-test engine baseline remains unavailable")
    context, anchor, body, artifact, _ = canonical_raw_transport
    projection = causal_producer._read_frozen_tier_raw_accounts(context, artifact, anchor)
    assert projection.qualification_authority is False
    assert projection.account_tiers == ("P75", "P95")
    assert projection.descriptor_sha256 == artifact.descriptor["sha256"]
    assert projection.accounts["P75"] is not projection.accounts["P95"]
    assert all(account["derived"]["reconciliation"] == "EXACT_FLOAT64_AT_EVERY_AVAILABLE_ENGINE_CLOSE"
               for account in projection.accounts.values())
    assert projection.accounts["P95"]["engine_spread_admission"]["input_provenance_authenticated"] is False
    assert body["release_authority_status"] == "UNAVAILABLE_AUTHORITY_MIGRATION_NOT_IMPLEMENTED"
    assert body["precommit_binding_scope"] == "REOPENED_INPUT_DESCRIPTOR_IDENTITY_NOT_ISSUED_RELEASE_AUTHORITY"
    assert body["full_book_descriptor"] == body["original_precommit_anchor"]["full_book_descriptor"]
    p75, p95 = projection.accounts["P75"], projection.accounts["P95"]
    assert body["schema_version"] == "fxpm.frozen-tier-raw-accounts.v3"
    assert not {"source_observations", "spread_source_rows"}.intersection(body)
    for level, account in body["accounts"].items():
        assert not {"source_rows", "valuation_rows", "m5_marks"}.intersection(account)
        assert len(account["engine_equity"]) == len(projection.accounts[level]["source_rows"])
    assert all(account["engine_spread_admission"]["schema_version"] == "fxpm.spread-entry-admission.v2"
               and account["engine_spread_admission"]["entry_admission_policy"] == "EXACT_PRIOR_FINE_CLOSE_CAP_ADMIT_ONLY_V2"
               and account["engine_spread_admission"]["missing_executed_legs"] == 0
               for account in (p75, p95))
    assert p75["trades"] == [] and p75["derived"]["positions"] == []
    assert p75["engine_spread_admission"]["known_cap_refused_opportunities"] == 1
    assert p95["engine_spread_admission"]["known_cap_pass_opportunities"] == 1
    assert len(p95["trades"]) == len(p95["position_events"]) == len(p95["derived"]["positions"]) == 1
    trade, event, position = p95["trades"][0], p95["position_events"][0], p95["derived"]["positions"][0]
    assert trade["direction"] == "LONG" and position["side"] == "LONG"
    assert event["position_id"] == position["position_id"]
    assert event["risk_amount"] == trade["risk_amount"]
    assert position["lots"] == trade["position_size"] >= p95["instrument_spec"]["min_lot"]
    holding = [mark for mark in p95["m5_marks"] if mark["state"] == "AVAILABLE" and mark["inventory"]]
    assert holding and all(mark["inventory"] == [{"position_id": position["position_id"], "lots": position["lots"]}]
                           for mark in holding)
    assert position["financing_events"]
    assert {row["kind"] for row in p95["derived"]["transactions"]} == {"SWAP", "REALIZED_GROSS"}
    with pytest.raises(causal_producer.CausalEvidenceProductionError, match="INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"):
        causal_producer._load_replay_ledgers(context, artifact, anchor)


def test_retained_execution_spec_equals_live_wrapper_for_original_sources(canonical_raw_transport):
    context, _, body, _, inputs = canonical_raw_transport
    source_bytes = {}
    for role in ("config", "broker"):
        _, source_bytes[role] = producer.native_prefix._read_spool_descriptor(context,
            body["deployment_source_descriptors"][role], expected_format="json-" + role, collect=True)
    for field in ("native_products", "spread_source_products"):
        product = next(iter(inputs[field].values()))
        live_spec, live_receipt = producer._freeze_execution_spec(product=product,
            config=inputs["config"], deployment=inputs["deployment"])
        retained_spec, retained_receipt = producer._derive_retained_execution_spec(product=product,
            deployment=inputs["deployment"], source_bytes=source_bytes)
        assert asdict(retained_spec) == asdict(live_spec)
        assert retained_receipt == live_receipt


@_lane127_blocked("LANE127_MISSING_GAP_EOD: current outer has zero prior-fine gaps and zero terminal EOD trades")
def test_raw_v2_warmup_and_holding_gap_preserve_actual_source_and_management(canonical_raw_transport):
    raise _Lane127WitnessUnavailable("Actual holding-gap/SWAP/EOD witness remains unavailable")
    context, anchor, body, artifact, _ = canonical_raw_transport
    projection = causal_producer._read_frozen_tier_raw_accounts(context, artifact, anchor)
    rows = body["source_observations"]
    warmup = projection.accounts["P95"]["warmup_bars"]
    assert all(row["points"] is None and row["prior_fine_close_utc"] is None for row in rows[:warmup])
    gap = next(row for row in rows if pd.Timestamp(row["target_open_utc"]) == pd.Timestamp("2020-02-06T00:00:00Z"))
    assert gap["expected_prior_fine_close_utc"] == gap["target_open_utc"]
    assert gap["prior_fine_close_utc"] is None and gap["points"] is None
    account = projection.accounts["P95"]
    assert len(account["trades"]) == 1 and account["trades"][0]["entry_spread_admission"] == "ADMIT"
    assert account["engine_spread_admission"]["signal_opportunities"] == 1
    assert account["engine_spread_admission"]["missing_refused_opportunities"] == 0
    assert account["engine_spread_admission"]["missing_executed_legs"] == 0
    assert account["derived"]["cash_inventory"][0]["inventory"]
    assert any(posting["kind"] == "SWAP" for posting in account["derived"]["transactions"])
    assert account["derived"]["terminal_inventory"] == []
    assert account["derived"]["terminal_simulation"] == "END_OF_DATA_LIQUIDATED"
    omitted_open = pd.Timestamp("2020-02-05T23:55:00Z")
    assert all(pd.Timestamp(row["open_utc"]) != omitted_open for row in account["valuation_rows"])
    assert all(pd.Timestamp(mark["effective_at_utc"]) != omitted_open for mark in account["m5_marks"])


def test_source_observations_distinguish_missing_row_from_matched_missing_points():
    target = pd.date_range("2026-01-05T00:00:00Z", periods=3, freq="D")
    fine = pd.DataFrame({"Spread": [float("nan")], "_BAR_CLOSE_TS": [target[1]]}, index=target[:1])
    points, matched = align_prior_completed_spreads(target, fine, "D1", include_matched_closes=True)
    rows = producer._spread_source_observations(points, matched)
    assert all(row["points"] is None for row in rows)
    assert [row["prior_fine_close_utc"] for row in rows] == [None, target[1].isoformat(), None]
    assert [row["expected_prior_fine_close_utc"] for row in rows] == [stamp.isoformat() for stamp in target]
    assert [row["target_open_utc"] for row in rows] == [stamp.isoformat() for stamp in target]


@pytest.mark.parametrize("cutoff,equivalent", [
    ("2020-01-01T00:00:00+00:00", True),
    ("2020-01-01T00:00:00.000001+00:00", False),
])
def test_original_owner_cutoff_uses_exact_utc_instant(canonical_raw_transport, cutoff, equivalent):
    context, anchor, body, _, inputs = canonical_raw_transport
    # Keep the original parameter IDs; apply their zero/+1us offset to the
    # authenticated current owner cutoff instead of the retired 2020 fixture.
    offset = pd.Timestamp(cutoff) - pd.Timestamp("2020-01-01T00:00:00+00:00")
    cutoff = (pd.Timestamp(anchor["selection_cutoff_utc"]) + offset).isoformat()
    book_body = producer._read_descriptor_json(context, body["full_book_descriptor"], code="SYNTHETIC_ORIGINAL_BOOK")
    arguments = dict(context=context, anchor={**anchor, "selection_cutoff_utc": cutoff},
        book_body=book_body, cell_id=body["cell_projection"]["cell_id"],
        symbol_receipt=inputs["owner_symbol_receipt"],
        adapter_index_descriptor=inputs["original_adapter_index_descriptor"])
    if equivalent:
        owner = producer._reopen_owner_capture(**arguments)
        assert owner.process_sha256 == body["cell_projection"]["process_sha256"]
    else:
        with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
            producer._reopen_owner_capture(**arguments)
        assert caught.value.code == "CURRENT_OWNER_INDEX_BINDING_MISMATCH"


@pytest.mark.parametrize("group_field", ["source_groups", "regime_fits"])
def test_original_owner_rejects_metadata_only_group_transport_tamper(canonical_raw_transport, group_field):
    context, anchor, body, _, _ = canonical_raw_transport
    original_book = producer._read_descriptor_json(context, body["full_book_descriptor"], code="SYNTHETIC_ORIGINAL_BOOK")
    original_book = producer._owned_book(original_book, original_book["receipt_sha256"])
    selected = next(cell for cell in original_book["cells"] if cell["cell_id"] == body["cell_projection"]["cell_id"])
    _, entries = producer._verified_manifest(context, body["owner_source"]["symbol_receipt"], selected["symbol"])
    original_index = producer._manifest_body(context, entries, f"{anchor['anchor_id']}/adapter/production-index",
        code="SYNTHETIC_ORIGINAL_INDEX", expected_descriptor=body["owner_source"]["adapter_index_descriptor"])
    # The genuine original graph works before one transported metadata value
    # changes. Neither original Book/index receipts nor their source bytes are
    # reissued to disguise the change.
    producer._selected_artifacts(context=context, entries=entries, anchor=anchor,
        book_body=original_book, book_cell=selected, index=original_index)
    changed_index = copy.deepcopy(original_index)
    local_ordinal = book.TIMEFRAME_ORDER.index(selected["timeframe"])
    changed_index[group_field][local_ordinal]["reason_code"] += "_SUBSTITUTED"
    assert changed_index[group_field][local_ordinal]["receipt_sha256"] == original_index[group_field][local_ordinal]["receipt_sha256"]
    group = changed_index[group_field][local_ordinal]
    assert producer._sha({key: value for key, value in group.items() if key != "receipt_sha256"}) != group["receipt_sha256"]
    with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
        producer._selected_artifacts(context=context, entries=entries, anchor=anchor,
            book_body=original_book, book_cell=selected, index=changed_index)
    assert caught.value.code == "CURRENT_OWNER_GROUP_BINDING_INVALID"


@pytest.mark.parametrize("genesis,owners,projection,expected", [
    (None, [_specialist()], None, "GENESIS_OR_CARRY_UNAVAILABLE"),
    ({"mode": producer.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0,
      "initial_inventory": [{"lots": 1}], "owner_changes": []}, [_specialist()], None, "GENESIS_OR_CARRY_UNAVAILABLE"),
    ({"mode": producer.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0,
      "initial_inventory": [], "owner_changes": [{"bar": 5}]}, [_specialist()], None, "GENESIS_OR_CARRY_UNAVAILABLE"),
    ({"mode": producer.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0,
      "initial_inventory": [], "owner_changes": []}, [_specialist(), _specialist()], {}, "SINGLE_CELL_PROJECTION_REQUIRED"),
    ({"mode": producer.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0,
      "initial_inventory": [], "owner_changes": []}, [_specialist()],
     {"cell_id": _specialist().cell_id, "process_sha256": _specialist().process_sha256}, "PROFILE_SOURCE_UNAVAILABLE"),
])
def test_raw_replay_requires_supported_genesis_and_source_contract_before_access(genesis, owners, projection, expected):
    class UnavailableInput:
        def __getattribute__(self, name):
            pytest.fail(f"unsupported raw replay accessed {name}")
    unavailable = UnavailableInput()
    with pytest.raises(producer.ForwardOOSReplayProductionError, match=expected):
        producer._execute_anchor_replay(context=unavailable,
            anchor={"anchor_id": _specialist().anchor_id}, specialists=owners,
            native_products=unavailable, valuation_products=unavailable, precommit_anchor=unavailable,
            capital_phase=producer.CAPITAL_OUTER_EVALUATION, capital_window=unavailable,
            config=PipelineConfig(initial_capital=10000, use_spread=True, backtest_spread_mode="conservative_max"),
            deployment=unavailable, account_genesis=genesis, cell_projection=projection)


def test_raw_replay_missing_captured_fine_timeframe_refuses_before_engine(canonical_raw_transport, monkeypatch, pytestconfig):
    context, _, body, _, inputs = canonical_raw_transport
    precommit = {**body["original_precommit_anchor"], "spread_source_products": [],
                 "spread_source_product_roster_sha256": producer._sha([])}
    original = producer._read_descriptor_json(context, inputs["original_global_precommit_descriptor"], code="SYNTHETIC_ORIGINAL")
    original["anchor_product_identities"] = [precommit]
    original["anchor_product_identities_sha256"] = producer._sha([precommit])
    descriptor = producer._write_raw_ledger(context, original, role="SYNTHETIC_MISSING_FINE_PRECOMMIT")
    state = getattr(pytestconfig, "_fxpm_lane127_completed_transport")
    state["validate_only"](canonical_raw_transport,
        {**inputs, "precommit_anchor": precommit, "spread_source_products": {},
         "original_global_precommit_descriptor": descriptor}, "missing_fine",
        "FROZEN_TIER_CAPTURED_FINE_TIMEFRAME_UNAVAILABLE")


# The original missing-entry run passed QA80 attempt001. Its function scratch
# was not retained; current v15 tests now verify source/mask/entry reconstruction.
# Do not invent an archived gap graph or regenerate a retired Winner capture.


@pytest.mark.parametrize("carrier,code", [
    ("frozen_pipeline_config", "FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_UNAVAILABLE"),
    ("owner_symbol_receipt", "FROZEN_TIER_ORIGINAL_OWNER_CAPTURE_UNAVAILABLE"),
    ("original_adapter_index_descriptor", "FROZEN_TIER_ORIGINAL_OWNER_CAPTURE_UNAVAILABLE"),
    ("original_global_precommit_descriptor", "FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT_UNAVAILABLE"),
])
def test_raw_replay_requires_original_owner_and_config_carriers(canonical_raw_transport, carrier, code, pytestconfig):
    *_, inputs = canonical_raw_transport
    changed = {**inputs, carrier: None}
    embedded = {"owner_symbol_receipt": "origin_symbol_receipt",
        "original_adapter_index_descriptor": "origin_adapter_index_descriptor"}.get(carrier)
    if embedded is not None:
        changed["specialists"] = [replace(inputs["specialists"][0], **{embedded: None})]
    state = getattr(pytestconfig, "_fxpm_lane127_completed_transport")
    state["validate_only"](canonical_raw_transport, changed, carrier, code)


def test_independent_tier_engines_can_have_different_admitted_paths(tmp_path):
    _, _, frame, spec, config, strategy, owner, tape, tiers = _real_frozen_engine_account(tmp_path)
    results = Backtester(config).run_frozen_spread_tiers(frame,
        pd.Series([1, 0, 0, 0, 0], index=frame.index), owner.symbol, strategy, spec=spec, timeframe="H1",
        p75=tiers["P75"], p95=tiers["P95"], warmup_bars=1,
        entry_spread_observations=pd.Series(12.0, index=frame.index))
    assert not results["accounts"]["P75"]["trades"]
    assert results["accounts"]["P95"]["trades"]
    assert list(results["accounts"]["P75"]["equity_curve"]) != list(results["accounts"]["P95"]["equity_curve"])


# Synthetic legacy-format construction retained only for strict archive-reader tests.

@producer.dataclass(frozen=True, slots=True)
class _DailyMark:
    date: str
    marked_at_utc: str
    mid_price: producer.Decimal
    bid_price: producer.Decimal
    ask_price: producer.Decimal
    base_financing_per_lot: producer.Decimal
    p75_financing_per_lot: producer.Decimal
    candidate_net_pnl_per_lot: float


@producer.dataclass(frozen=True, slots=True)
class _PositionEvidence:
    candidate: producer.portfolio.CandidateTrade
    position_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    side: str
    contract_multiplier: producer.Decimal
    stop_price: producer.Decimal
    entry_mid: producer.Decimal
    entry_bid: producer.Decimal
    entry_ask: producer.Decimal
    base_entry_fill: producer.Decimal
    p75_entry_fill: producer.Decimal
    exit_mid: producer.Decimal
    exit_bid: producer.Decimal
    exit_ask: producer.Decimal
    base_exit_fill: producer.Decimal
    p75_exit_fill: producer.Decimal
    margin_per_lot: producer.Decimal
    marks: tuple[_DailyMark, ...]


@producer.dataclass(frozen=True, slots=True)
class _ArchivedAnchorRawReplay:
    anchor: producer.Mapping[str, producer.Any]
    specialists: tuple[producer._VerifiedSpecialist, ...]
    positions: tuple[_PositionEvidence, ...]
    accepted_volume_by_position_id: producer.Mapping[str, producer.Decimal]
    production_metadata: producer.Mapping[str, producer.Any] | None = None


def _p75_ladder_result(
    cost_ladder_result: producer.Mapping[str, producer.Any],
    *,
    cell_id: str,
) -> producer.Mapping[str, producer.Any]:
    """Select the fixed forward-adversity lane from one verified ladder replay."""

    if (
        type(cost_ladder_result) is not dict
        or cost_ladder_result.get("schema_version")
        != BACKTEST_FROZEN_PATH_COST_LADDER_SCHEMA_VERSION
    ):
        producer._fail("BACKTEST_COST_LADDER_RESULT_INVALID", cell_id)
    levels = cost_ladder_result.get("levels")
    receipt = cost_ladder_result.get("cost_ladder_frozen_path_receipt")
    receipt_levels = receipt.get("levels") if type(receipt) is dict else None
    if (
        type(levels) is not list
        or len(levels) != 1
        or tuple(
            level.get("level_id") if type(level) is dict else None
            for level in levels
        )
        != (_P75_LEVEL_ID,)
        or type(receipt_levels) is not list
        or len(receipt_levels) != 1
        or tuple(
            level.get("level_id") if type(level) is dict else None
            for level in receipt_levels
        )
        != (_P75_LEVEL_ID,)
    ):
        producer._fail("BACKTEST_COST_LADDER_LEVEL_ROSTER_INVALID", cell_id)
    return levels[0]


def _event_decisions(
    *,
    event_id: str,
    position_id: str,
    cell_id: str,
    regime: str,
    decided_at_utc: str,
    action: str,
) -> dict[str, producer.Any]:
    return {
        "event_id": event_id,
        "position_id": position_id,
        "cell_id": cell_id,
        "controller": {
            "schema_version": producer.causal_producer.CONTROLLER_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "regime": regime,
            "decided_at_utc": decided_at_utc,
        },
        "arbitration": {
            "schema_version": producer.causal_producer.ARBITRATION_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "decision": "ADMIT" if action == "OPEN" else "EXIT",
            "decided_at_utc": decided_at_utc,
        },
        "order": {
            "schema_version": producer.causal_producer.ORDER_EVENT_SCHEMA,
            "event_id": event_id,
            "position_id": position_id,
            "action": action,
            "decided_at_utc": decided_at_utc,
        },
    }


def _write_capital_json_artifact(
    context: producer.authority_spool.GenerationSpoolContext,
    body: producer.Mapping[str, producer.Any],
    *,
    role: str,
    maximum: int,
) -> dict[str, producer.Any]:
    raw = producer._canonical_bytes(body, maximum)
    try:
        descriptor = producer.authority_spool.write_canonical_json_artifact(context, body)
    except Exception as exc:
        raise producer.ForwardOOSReplayProductionError(
            "CAPITAL_EVIDENCE_SPOOL_FAILED", (role, type(exc).__name__)
        ) from exc
    normalized = producer._descriptor(descriptor, "CAPITAL_EVIDENCE_DESCRIPTOR_INVALID")
    if (
        normalized["format"] != producer.authority_spool.CANONICAL_JSON_FORMAT
        or normalized["size_bytes"] != len(raw)
        or normalized["sha256"] != producer.hashlib.sha256(raw).hexdigest()
        or normalized["size_bytes"] > maximum
    ):
        producer._fail("CAPITAL_EVIDENCE_SPOOL_IDENTITY_MISMATCH", role)
    return normalized


def _persist_capital_phase_evidence(
    *,
    context: producer.authority_spool.GenerationSpoolContext,
    replay: _ArchivedAnchorRawReplay,
    run_anchor: producer.Mapping[str, producer.Any],
    replay_precommit: producer.ForwardOOSReplayPrecommit,
    replay_precommit_body: producer.Mapping[str, producer.Any],
    precommit_anchor: producer.Mapping[str, producer.Any],
) -> tuple[dict[str, producer.Any], dict[str, producer.Any]]:
    phase_id = str(replay.production_metadata.get("capital_phase_id"))
    phase = str(replay.production_metadata.get("capital_phase"))
    window = replay.production_metadata.get("capital_window")
    expected_phase_id = producer._capital_phase_id(str(replay.anchor["anchor_id"]), phase)
    if phase_id != expected_phase_id or type(window) is not dict:
        producer._fail("CAPITAL_PHASE_REPLAY_METADATA_INVALID", phase_id)
    ordered_candidates = sorted(
        (position.candidate for position in replay.positions),
        key=lambda value: value.candidate_id,
    )
    if not ordered_candidates:
        producer._fail("CAPITAL_PHASE_CANDIDATE_ROSTER_EMPTY", phase_id)
    if len(ordered_candidates) > producer.portfolio.MAX_PORTFOLIO_REPLAY_CANDIDATES:
        producer._fail("CAPITAL_PHASE_CANDIDATE_COUNT_BOUND_EXCEEDED", phase_id)
    identifiers = [value.candidate_id for value in ordered_candidates]
    if len(set(identifiers)) != len(identifiers):
        producer._fail("CAPITAL_PHASE_CANDIDATE_ID_REUSED", phase_id)
    specialist_roster = producer._capital_specialist_roster(replay.specialists)
    selected_ids = frozenset(value["cell_id"] for value in specialist_roster)
    records: list[tuple[dict[str, producer.Any], str]] = [
        producer._capital_candidate_record(
            candidate,
            phase_id=phase_id,
            window=window,
            selected_cell_ids=selected_ids,
        )
        for candidate in ordered_candidates
    ]
    descriptors: list[dict[str, producer.Any]] = []
    all_record_hashes: list[str] = []
    pending_records: list[dict[str, producer.Any]] = []
    pending_hashes: list[str] = []
    pending_bytes = 0

    def flush() -> None:
        nonlocal pending_records, pending_hashes, pending_bytes
        if not pending_records:
            return
        chunk_index = len(descriptors)
        if chunk_index >= producer._MAX_CAPITAL_PHASE_CHUNKS:
            producer._fail("CAPITAL_PHASE_CHUNK_COUNT_BOUND_EXCEEDED", phase_id)
        chunk = {
            "schema_version": producer.CAPITAL_CANDIDATE_CHUNK_SCHEMA,
            "phase_id": phase_id,
            "chunk_index": chunk_index,
            "candidate_count": len(pending_records),
            "candidate_records": pending_records,
            "candidate_record_sha256s": pending_hashes,
            "candidate_record_roster_sha256": producer._sha(
                [
                    {
                        "candidate_id": record["candidate_id"],
                        "candidate_record_sha256": record_sha,
                    }
                    for record, record_sha in zip(
                        pending_records, pending_hashes, strict=True
                    )
                ]
            ),
            "complete_records_not_policy_filtered": True,
        }
        descriptors.append(
            _write_capital_json_artifact(
                context,
                chunk,
                role=f"{phase_id}:CANDIDATE_CHUNK:{chunk_index}",
                maximum=producer._MAX_CAPITAL_CANDIDATE_CHUNK_BYTES,
            )
        )
        pending_records = []
        pending_hashes = []
        pending_bytes = 0

    for record, record_sha in records:
        record_bytes = len(
            producer._canonical_bytes(record, producer._MAX_CAPITAL_CANDIDATE_RECORD_BYTES)
        )
        if pending_records and (
            pending_bytes + record_bytes + 64 * 1024
            > producer._MAX_CAPITAL_CANDIDATE_CHUNK_BYTES
        ):
            flush()
        pending_records.append(record)
        pending_hashes.append(record_sha)
        pending_bytes += record_bytes
        all_record_hashes.append(record_sha)
    flush()
    total_bytes = sum(value["size_bytes"] for value in descriptors)
    if total_bytes > producer._MAX_CAPITAL_PHASE_RECORD_BYTES:
        producer._fail("CAPITAL_PHASE_RECORD_BYTE_BOUND_EXCEEDED", phase_id, total_bytes)
    lineage = producer._capital_operational_lineage(
        run_anchor=run_anchor,
        specialists=replay.specialists,
        precommit_anchor=precommit_anchor,
        replay_precommit_body=replay_precommit_body,
    )
    roster_receipt = producer.portfolio.portfolio_replay_candidate_roster_receipt(
        ordered_candidates
    )
    manifest = {
        "schema_version": producer.CAPITAL_PHASE_MANIFEST_SCHEMA,
        "production_contract_version": producer.PRODUCTION_CONTRACT_VERSION,
        "phase_id": phase_id,
        "anchor_id": replay.anchor["anchor_id"],
        "phase": phase,
        "window": dict(window),
        "historical_run_receipt_sha256": replay_precommit_body[
            "historical_run_receipt_sha256"
        ],
        "forward_replay_precommit_receipt_sha256": replay_precommit.receipt_sha256,
        "capital_bootstrap_identity_sha256": replay_precommit_body[
            "capital_bootstrap_identity_sha256"
        ],
        "selected_specialist_roster_sha256": producer._sha(specialist_roster),
        "selected_cell_ids": sorted(selected_ids),
        "candidate_chunk_descriptors": descriptors,
        "candidate_chunk_descriptor_roster_sha256": producer._sha(descriptors),
        "candidate_count": len(ordered_candidates),
        "candidate_record_sha256s_sha256": producer._sha(all_record_hashes),
        "candidate_roster_receipt": roster_receipt,
        "operational_lineage": lineage,
        "complete_pre_policy_candidate_roster": True,
        "candidate_marks_thinned_or_sampled": False,
        "confers_capital_publication_or_trading_authority": False,
    }
    descriptor = _write_capital_json_artifact(
        context,
        manifest,
        role=f"{phase_id}:CAPITAL_PHASE_MANIFEST",
        maximum=producer._MAX_JSON_BYTES,
    )
    return descriptor, lineage


def _raw_event_id(anchor_id: str, position_id: str, lane: str, day: str = "") -> str:
    raw = f"{anchor_id}|{position_id}|{lane}|{day}".encode("utf-8")
    return "evt:" + producer.hashlib.sha256(raw).hexdigest()


def _broker_fill_id(anchor_id: str, position_id: str, lane: str) -> str:
    raw = f"{anchor_id}|{position_id}|{lane}".encode("utf-8")
    return "fill:" + producer.hashlib.sha256(raw).hexdigest()


def _legacy_archive_ledger_bodies(
    replay: _ArchivedAnchorRawReplay,
    *,
    starting_balance: producer.Decimal,
) -> dict[str, dict[str, producer.Any]]:
    anchor = replay.anchor
    dates = [
        (
            producer._date(anchor["forward_start_date"], "FORWARD_START_INVALID")
            + producer.timedelta(days=offset)
        ).isoformat()
        for offset in range(
            (
                producer._date(anchor["forward_end_date"], "FORWARD_END_INVALID")
                - producer._date(anchor["forward_start_date"], "FORWARD_START_INVALID")
            ).days
            + 1
        )
    ]
    accepted = [
        (position, replay.accepted_volume_by_position_id[position.position_id])
        for position in replay.positions
        if position.position_id in replay.accepted_volume_by_position_id
    ]
    if len(accepted) > producer._MAX_CANDIDATE_POSITIONS_PER_ANCHOR:
        producer._fail("ACCEPTED_POSITION_BOUND_EXCEEDED", anchor["anchor_id"])
    by_entry: dict[str, list[tuple[_PositionEvidence, producer.Decimal]]] = {}
    by_exit: dict[str, list[tuple[_PositionEvidence, producer.Decimal]]] = {}
    for position, volume in accepted:
        entry_day = (
            producer._timestamp(position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID")
            .date()
            .isoformat()
        )
        exit_day = (
            producer._timestamp(
                position.candidate.legs[-1].exit_time, "POSITION_EXIT_TIME_INVALID"
            )
            .date()
            .isoformat()
        )
        by_entry.setdefault(entry_day, []).append((position, volume))
        by_exit.setdefault(exit_day, []).append((position, volume))

    event_days: list[dict[str, producer.Any]] = []
    fill_days: list[dict[str, producer.Any]] = []
    mtm_days: list[dict[str, producer.Any]] = []
    cost_days: list[dict[str, producer.Any]] = []
    exposure_days: list[dict[str, producer.Any]] = []
    margin_days: list[dict[str, producer.Any]] = []
    open_states: dict[str, tuple[_PositionEvidence, producer.Decimal, producer.Decimal]] = {}
    equity = starting_balance
    cumulative_base_cost = producer.Decimal(0)
    cumulative_p75_cost = producer.Decimal(0)
    if not equity.is_finite() or equity <= 0:
        producer._fail("REPLAY_STARTING_BALANCE_INVALID")
    for day_text in dates:
        entries_event: list[dict[str, producer.Any]] = []
        entries_fill: list[dict[str, producer.Any]] = []
        for position, volume in sorted(
            by_entry.get(day_text, []), key=lambda item: item[0].position_id
        ):
            if position.position_id in open_states:
                producer._fail("ACCEPTED_POSITION_REOPENED", position.position_id)
            opened_at = producer._utc_seconds(
                producer._timestamp(
                    position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID"
                ).isoformat(),
                "POSITION_ENTRY_TIME_INVALID",
            )
            event_id = _raw_event_id(anchor["anchor_id"], position.position_id, "OPEN")
            quantity = producer._decimal_text(volume)
            entries_event.append(
                _event_decisions(
                    event_id=event_id,
                    position_id=position.position_id,
                    cell_id=position.cell_id,
                    regime=position.regime,
                    decided_at_utc=opened_at,
                    action="OPEN",
                )
            )
            entries_fill.append(
                {
                    "event_id": event_id,
                    "position_id": position.position_id,
                    "cell_id": position.cell_id,
                    "opened_at_utc": opened_at,
                    "side": position.side,
                    "quantity": quantity,
                    "contract_multiplier": producer._decimal_text(position.contract_multiplier),
                    "stop_price": producer._decimal_text(position.stop_price),
                    "broker_fill_id": _broker_fill_id(
                        anchor["anchor_id"], position.position_id, "OPEN"
                    ),
                    "reference_mid_price": producer._decimal_text(position.entry_mid),
                    "reference_bid_price": producer._decimal_text(position.entry_bid),
                    "reference_ask_price": producer._decimal_text(position.entry_ask),
                    "base_fill_price": producer._decimal_text(position.base_entry_fill),
                    "p75_fill_price": producer._decimal_text(position.p75_entry_fill),
                }
            )
            open_states[position.position_id] = (
                position,
                volume,
                position.entry_mid,
            )

        mark_events: list[dict[str, producer.Any]] = []
        raw_marks: list[dict[str, producer.Any]] = []
        mark_costs: list[dict[str, producer.Any]] = []
        exposure_positions: list[dict[str, producer.Any]] = []
        daily_gross = producer.Decimal(0)
        daily_base_cost = producer.Decimal(0)
        daily_p75_cost = producer.Decimal(0)
        for position_id in sorted(open_states):
            position, volume, last_mark = open_states[position_id]
            match = [mark for mark in position.marks if mark.date == day_text]
            if len(match) != 1:
                producer._fail("ACCEPTED_POSITION_DAILY_MARK_INCOMPLETE", position_id, day_text)
            mark = match[0]
            event_id = _raw_event_id(anchor["anchor_id"], position_id, "MARK", day_text)
            mark_events.append({"event_id": event_id, "position_id": position_id})
            raw_marks.append(
                {
                    "event_id": event_id,
                    "position_id": position_id,
                    "marked_at_utc": mark.marked_at_utc,
                    "mark_price": producer._decimal_text(mark.mid_price),
                    "mark_bid_price": producer._decimal_text(mark.bid_price),
                    "mark_ask_price": producer._decimal_text(mark.ask_price),
                }
            )
            base_finance = mark.base_financing_per_lot * volume
            p75_finance = mark.p75_financing_per_lot * volume
            mark_costs.append(
                {
                    "event_id": event_id,
                    "base_financing_cash": producer._decimal_text(base_finance),
                    "p75_financing_cash": producer._decimal_text(p75_finance),
                }
            )
            sign = producer.Decimal(1) if position.side == "LONG" else producer.Decimal(-1)
            unit = volume * position.contract_multiplier
            daily_gross += sign * unit * (mark.mid_price - last_mark)
            daily_base_cost += base_finance
            daily_p75_cost += p75_finance
            if (
                day_text
                == producer._timestamp(
                    position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID"
                )
                .date()
                .isoformat()
            ):
                executable = (
                    position.entry_ask
                    if position.side == "LONG"
                    else position.entry_bid
                )
                spread = sign * (executable - position.entry_mid) * unit
                base_fill_degradation = (
                    sign * (position.base_entry_fill - executable) * unit
                )
                p75_fill_degradation = (
                    sign * (position.p75_entry_fill - executable) * unit
                )
                daily_base_cost += max(
                    producer.Decimal(0), spread + base_fill_degradation
                )
                daily_p75_cost += max(
                    producer.Decimal(0), spread + p75_fill_degradation
                )
            required_margin = position.margin_per_lot * volume
            exposure_positions.append(
                {
                    "position_id": position_id,
                    "quantity": producer._decimal_text(volume),
                    "contract_multiplier": producer._decimal_text(position.contract_multiplier),
                    "stop_price": producer._decimal_text(position.stop_price),
                    "required_margin_cash": producer._decimal_text(required_margin),
                }
            )
            open_states[position_id] = (position, volume, mark.mid_price)

        exits_event: list[dict[str, producer.Any]] = []
        exits_fill: list[dict[str, producer.Any]] = []
        for position, volume in sorted(
            by_exit.get(day_text, []), key=lambda item: item[0].position_id
        ):
            state = open_states.get(position.position_id)
            if state is None:
                producer._fail("ACCEPTED_POSITION_EXIT_NOT_OPEN", position.position_id)
            closed_at = producer._utc_seconds(
                producer._timestamp(
                    position.candidate.legs[-1].exit_time,
                    "POSITION_EXIT_TIME_INVALID",
                ).isoformat(),
                "POSITION_EXIT_TIME_INVALID",
            )
            event_id = _raw_event_id(anchor["anchor_id"], position.position_id, "CLOSE")
            exits_event.append(
                _event_decisions(
                    event_id=event_id,
                    position_id=position.position_id,
                    cell_id=position.cell_id,
                    regime=position.regime,
                    decided_at_utc=closed_at,
                    action="CLOSE",
                )
            )
            exits_fill.append(
                {
                    "event_id": event_id,
                    "position_id": position.position_id,
                    "closed_at_utc": closed_at,
                    "broker_fill_id": _broker_fill_id(
                        anchor["anchor_id"], position.position_id, "CLOSE"
                    ),
                    "reference_mid_price": producer._decimal_text(position.exit_mid),
                    "reference_bid_price": producer._decimal_text(position.exit_bid),
                    "reference_ask_price": producer._decimal_text(position.exit_ask),
                    "base_fill_price": producer._decimal_text(position.base_exit_fill),
                    "p75_fill_price": producer._decimal_text(position.p75_exit_fill),
                }
            )
            sign = producer.Decimal(1) if position.side == "LONG" else producer.Decimal(-1)
            unit = volume * position.contract_multiplier
            # The daily mark is an independently acquired M5 Open, while the
            # close is the exact specialist execution fill.  Realize the gross
            # mid-price move from that last common valuation to the execution
            # reference before charging the executable spread/fill degradation.
            daily_gross += sign * unit * (position.exit_mid - state[2])
            executable = (
                position.exit_bid if position.side == "LONG" else position.exit_ask
            )
            spread = sign * (position.exit_mid - executable) * unit
            base_fill_degradation = (
                sign * (executable - position.base_exit_fill) * unit
            )
            p75_fill_degradation = (
                sign * (executable - position.p75_exit_fill) * unit
            )
            daily_base_cost += max(producer.Decimal(0), spread + base_fill_degradation)
            daily_p75_cost += max(
                producer.Decimal(0), spread + p75_fill_degradation
            )
            del open_states[position.position_id]

        used_margin = sum(
            (
                producer._decimal(item["required_margin_cash"], "POSITION_MARGIN_INVALID")
                for item in exposure_positions
            ),
            producer.Decimal(0),
        )
        free_margin = equity - used_margin
        if free_margin < 0:
            producer._fail("FORWARD_REPLAY_UNMODELED_MARGIN_CALL", anchor["anchor_id"], day_text)
        basis_text = producer._decimal_text(equity)
        event_days.append(
            {
                "date": day_text,
                "entries": entries_event,
                "marks": mark_events,
                "exits": exits_event,
            }
        )
        fill_days.append(
            {
                "date": day_text,
                "entries": entries_fill,
                "exits": exits_fill,
            }
        )
        mtm_days.append(
            {
                "date": day_text,
                "equity_basis_cash": basis_text,
                "position_marks": raw_marks,
            }
        )
        cost_days.append(
            {
                "date": day_text,
                "marks": mark_costs,
            }
        )
        exposure_days.append({"date": day_text, "positions": exposure_positions})
        margin_days.append(
            {
                "date": day_text,
                "equity_basis_cash": basis_text,
                "used_margin_cash": producer._decimal_text(used_margin),
                "free_margin_cash": producer._decimal_text(free_margin),
            }
        )
        equity += daily_gross - daily_base_cost
        if not equity.is_finite() or equity <= 0:
            producer._fail("FORWARD_REPLAY_EQUITY_RUIN", anchor["anchor_id"], day_text)
        cumulative_base_cost += daily_base_cost
        cumulative_p75_cost += daily_p75_cost
        # MT5 swap mode 6 can make one P75 SHORT rollover day less costly
        # because its rate depends on the worse opening notional.  Protect the
        # shared gross event stream and still-open positions by comparing every
        # cumulative cost prefix instead of each daily first difference.
        if cumulative_p75_cost + producer.Decimal("1e-12") < cumulative_base_cost:
            producer._fail("FORWARD_REPLAY_P75_COST_IMPROVES", anchor["anchor_id"], day_text)
    if open_states:
        producer._fail("FORWARD_REPLAY_ENDS_WITH_OPEN_POSITIONS", anchor["anchor_id"])

    common = {
        "anchor_id": anchor["anchor_id"],
        "forward_start_date": anchor["forward_start_date"],
        "forward_end_date": anchor["forward_end_date"],
    }
    specialists_body = [
        {
            "schema_version": producer.causal_producer.SPECIALIST_SCHEMA,
            "cell_id": value.cell_id,
            "strategy_module": value.strategy_module,
            "strategy_class": value.strategy_class,
            "parameters": dict(value.parameters),
            "refit_market_data_end_utc": anchor["selection_cutoff_utc"],
        }
        for value in replay.specialists
    ]
    return {
        "EVENT_LEDGER": {
            "schema_version": producer.causal_producer.EVENT_LEDGER_SCHEMA,
            **common,
            "selection_cutoff_utc": anchor["selection_cutoff_utc"],
            "specialists": specialists_body,
            "initial_positions": [],
            "days": event_days,
        },
        "FILL_LEDGER": {
            "schema_version": producer.causal_producer.FILL_LEDGER_SCHEMA,
            **common,
            "initial_positions": [],
            "days": fill_days,
        },
        "MTM_LEDGER": {
            "schema_version": producer.causal_producer.MTM_LEDGER_SCHEMA,
            **common,
            "days": mtm_days,
        },
        "COST_LEDGER": {
            "schema_version": producer.causal_producer.COST_LEDGER_SCHEMA,
            **common,
            "days": cost_days,
        },
        "EXPOSURE_LEDGER": {
            "schema_version": producer.causal_producer.EXPOSURE_LEDGER_SCHEMA,
            **common,
            "days": exposure_days,
        },
        "MARGIN_LEDGER": {
            "schema_version": producer.causal_producer.MARGIN_LEDGER_SCHEMA,
            **common,
            "days": margin_days,
        },
    }


_RETAINED_CURRENT_FREEZE_ROOT = (
    "selected_owner_geometry_control/runtime/attempt_002_pytest_tmp/"
    "test_declared_native_two_trial0/spool"
)
_RETAINED_CURRENT_MANIFEST = (
    "symbols/af1c64037373198aa9f2805a63b3d1ae4cfddc9a70b971e23425e06658c2ed6f/"
    "domains/b15f584799e6d595cd91f88524a58fb6d0b4ea073d79cc381048d1f3ba660225/manifest.json"
)
_RETAINED_CURRENT_MANIFEST_SHA256 = "dfb6f8d125c80aedb094eae93b9e2f0782b4e3d6c24af7ca1c75a81823566a82"
_RETAINED_CURRENT_FROZEN_RECEIPT_SHA256 = "39839c26bbd9345f14f658728a03df39666fa5769b68a627b18d4962b71d5ba0"
_RETAINED_CURRENT_CROSS_SHA256 = "6f62e751af758236390c6a70a33ad803446ab8dfd1c5fada11396f09ab88302f"
_RESTORE_ARTIFACT_NAMES = ("winner_process", "horizon_process", "trial_definition", "refit_recipe")


def _frozen_restore_transport(context, entries, *, symbol, timeframe, regime, namespace):
    prefix = f"{namespace}/winner-search/{timeframe}/{regime}/frozen/"
    selected = [copy.deepcopy(entry) for name, entry in sorted(entries.items())
                if name.startswith(prefix)]
    encoded = [producer._read_descriptor_bytes(context, entry["descriptor"],
        code="TEST_CURRENT_FROZEN_BODY") for entry in selected]
    by_name = {entry["artifact_id"][len(prefix):].replace("/", "-", 1):
               json.loads(body.decode("ascii")) for entry, body in zip(selected, encoded, strict=True)}
    return {
        "context_identity": {
            "generation_id": context.generation_id,
            "generation_context_sha256": context.context_sha256,
            "native_universe_sha256": context.native_universe_sha256,
            "config_sha256": context.config_sha256,
        },
        "symbol": symbol, "timeframe": timeframe, "regime": regime,
        "artifact_namespace": namespace, "manifest_entries": selected,
        "body_bytes": encoded,
    }, by_name


@pytest.fixture(scope="module")
def retained_current_owner_metadata(tmp_path_factory):
    """Genuine sealed synthetic presearch metadata; no financial replay or result reads."""
    original = (Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007"
                / _RETAINED_CURRENT_FREEZE_ROOT)
    manifest_bytes = (original / _RETAINED_CURRENT_MANIFEST).read_bytes()
    assert hashlib.sha256(manifest_bytes).hexdigest() == _RETAINED_CURRENT_MANIFEST_SHA256, (
        "retained QA97 attempt002 sealed manifest changed")
    manifest = json.loads(manifest_bytes)
    destination = tmp_path_factory.mktemp("readonly-current-owner") / "spool"
    shutil.copytree(original, destination)
    context = authority_spool.reopen_generation_context(destination,
        expected_generation_id=manifest["generation_id"],
        expected_native_universe_sha256=manifest["native_universe_sha256"],
        expected_config_sha256=manifest["config_sha256"])
    receipt = {name: manifest[name] for name in (
        "generation_id", "symbol", "manifest_domain", "native_universe_sha256",
        "config_sha256", "generation_context_sha256")}
    receipt.update({"schema_version": authority_spool.COMPACT_RECEIPT_SCHEMA,
        "symbol_manifest": {
            "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": _RETAINED_CURRENT_MANIFEST,
            "sha256": _RETAINED_CURRENT_MANIFEST_SHA256,
            "size_bytes": len(manifest_bytes), "format": authority_spool.CANONICAL_JSON_FORMAT}})
    reopened_manifest, entries = producer._verified_manifest(context, receipt, manifest["symbol"])
    assert reopened_manifest == manifest and manifest["artifact_count"] == 196
    frozen_entries = [entry for entry in entries.values()
                      if entry["artifact_type"] == "winner_frozen_search_plan_receipt"]
    frozen_entry = next(entry for entry in frozen_entries
                        if entry["descriptor"]["sha256"] == _RETAINED_CURRENT_FROZEN_RECEIPT_SHA256)
    frozen_receipt_bytes = producer._read_descriptor_bytes(context, frozen_entry["descriptor"],
        code="TEST_RETAINED_FROZEN_RECEIPT")
    frozen_receipt = json.loads(frozen_receipt_bytes)
    namespace = frozen_receipt["artifact_namespace"]
    cross_entry = entries[f"unissued-selected-owner-anchor-64d-control/adapter/H1/TREND/horizon-winner-cross-binding"]
    assert cross_entry["descriptor"]["sha256"] == _RETAINED_CURRENT_CROSS_SHA256
    cross = producer._manifest_body(context, entries, cross_entry["artifact_id"], code="TEST_RETAINED_CROSS")
    arguments, bodies = _frozen_restore_transport(context, entries,
        symbol=frozen_receipt["symbol"], timeframe=frozen_receipt["timeframe"],
        regime=frozen_receipt["regime"], namespace=namespace)
    assert len(arguments["manifest_entries"]) == 18
    assert bodies["receipt"] == frozen_receipt
    assert len(bodies["attempt_ledger"]["attempts"]) == 2
    for ordinal in (0, 1):
        for name in _RESTORE_ARTIFACT_NAMES:
            assert f"attempt-{ordinal:04d}-{name}" in bodies
    yield {"context": context, "entries": entries, "arguments": arguments,
           "bodies": bodies, "cross": cross, "frozen_receipt_bytes": frozen_receipt_bytes}
    assert (original / _RETAINED_CURRENT_MANIFEST).read_bytes() == manifest_bytes


@pytest.fixture
def forbid_current_owner_economics(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("read-only owner restoration reached selected-owner or financial execution")
    monkeypatch.setattr(producer, "_selected_artifacts", forbidden)
    monkeypatch.setattr(producer.Backtester, "run", forbidden)


def _restore_current_binding(case, *, cross=None):
    arguments = case["arguments"]
    return producer._restore_current_owner_frozen_binding(
        context=case["context"], entries=case["entries"], symbol=arguments["symbol"],
        timeframe=arguments["timeframe"], regime=arguments["regime"],
        namespace=arguments["artifact_namespace"], cross=case["cross"] if cross is None else cross)


def _current_owner_capture_arguments(case, *, frozen, cross):
    context, entries = case["context"], case["entries"]
    index = producer._manifest_body(context, entries,
        "unissued-selected-owner-anchor-64d-control/adapter/production-index",
        code="TEST_RETAINED_PRODUCTION_INDEX")
    records = [row for row in index["cell_records"]
               if (row["symbol"], row["timeframe"], row["regime"])
               == (frozen.symbol, frozen.timeframe, frozen.regime)]
    assert len(records) == 1
    record = records[0]
    assert record["raw_artifact_namespace"] == frozen.artifact_namespace
    prefix = f"{frozen.artifact_namespace}/winner-search/{frozen.timeframe}/{frozen.regime}/authority"
    request_body = producer._manifest_body(context, entries, f"{prefix}/winner-request",
        code="TEST_RETAINED_WINNER_REQUEST", maximum=producer.winner.MAX_REQUEST_BYTES)
    request = producer._winner_request(request_body, record["winner_request_sha256"])
    decision_body = producer._manifest_body(context, entries, f"{prefix}/winner-decision",
        code="TEST_RETAINED_WINNER_DECISION", maximum=producer.winner.MAX_DECISION_BYTES)
    owned_decision = copy.deepcopy(decision_body)
    decision_sha256 = owned_decision.pop("receipt_sha256")
    assert decision_sha256 == record["winner_decision_sha256"] == producer._sha(owned_decision)
    # Own the sealed result without rerunning the selection policy.
    decision = producer.winner.WinnerSelectionDecision(
        producer._canonical_bytes(owned_decision, producer.winner.MAX_DECISION_BYTES), decision_sha256)
    assert request.to_dict() == request_body and decision.to_dict() == decision_body
    return {"context": context, "entries": entries, "anchor": index["anchor"],
            "index": index, "record": record, "request": request, "decision": decision,
            "frozen": frozen, "cross": cross, "cell_id": frozen.precommitment["cell_id"]}


@pytest.mark.parametrize("generation_id", [
    "synthetic-readonly-freeze-oracle",
    "g" * 300,
    "g" * 512,
    "\u00e9" * 256,
], ids=["short", "ascii-300-bytes", "ascii-512-bytes", "utf8-512-bytes"])
def test_readonly_current_frozen_restore_matches_every_real_freeze_field(
    tmp_path, retained_current_owner_metadata, forbid_current_owner_economics, generation_id,
):
    retained = retained_current_owner_metadata
    bodies = retained["bodies"]
    precommitment = bodies["precommitment"]
    ledger = bodies["attempt_ledger"]
    context = authority_spool.freeze_generation_context(tmp_path / "fresh-presearch-spool",
        generation_id=generation_id,
        native_universe_sha256="a" * 64, config_sha256="b" * 64)
    assert context.generation_id == generation_id
    sink = authority_spool.open_symbol_sink(context, retained["arguments"]["symbol"])
    fresh = raw_winner.freeze_winner_search_plan_v4(sink,
        symbol=retained["arguments"]["symbol"], timeframe=retained["arguments"]["timeframe"],
        regime=retained["arguments"]["regime"],
        precommitted_at_utc=precommitment["precommitted_at_utc"],
        attempts_frozen_at_utc=ledger["frozen_at_utc"],
        fold_plan=[{name: fold[name] for name in (
            "anchor_as_of_utc", "oos_start_date_utc", "oos_end_date_utc")}
            for fold in precommitment["causal_fold_plan"]],
        attempt_artifacts=[{name: bodies[f"attempt-{ordinal:04d}-{name}"]
                            for name in _RESTORE_ARTIFACT_NAMES} for ordinal in (0, 1)],
        precommitted_evidence_roles=[attempt["precommitted_evidence_role"] for attempt in ledger["attempts"]],
        horizon_execution_contracts=bodies["horizon_execution_contracts"],
        source_contract_seeds=bodies["source_contract_seeds"],
        frozen_tier_pair_contract=bodies["frozen_tier_pair_contract"],
        risk_unit_contract=bodies["risk_unit_contract"],
        partition_cost_precommitments=bodies["partition_cost_precommitments"],
        artifact_namespace="historical-anchor/readonly-oracle/fresh-freeze")
    sealed = sink.seal()
    _, entries = producer._verified_manifest(context, sealed, fresh.symbol)
    arguments, _ = _frozen_restore_transport(context, entries, symbol=fresh.symbol,
        timeframe=fresh.timeframe, regime=fresh.regime, namespace=fresh.artifact_namespace)
    restored = raw_winner.restore_frozen_winner_search_plan_v4(**arguments)
    assert type(restored) is type(fresh) is raw_winner.FrozenWinnerSearchPlan
    for field in fields(fresh):
        assert getattr(restored, field.name) == getattr(fresh, field.name), field.name
    for name in ("source_contract_seed_bytes", "cost_ladder_contract_bytes", "cost_ladder_adversity_plan_bytes"):
        assert getattr(fresh, name) == getattr(restored, name) == b""


@pytest.mark.parametrize("generation_id", [
    "g" * 513, "\u00e9" * 256 + "g",
], ids=["ascii-513-bytes", "utf8-513-bytes"])
def test_current_generation_factory_refuses_ids_over_512_utf8_bytes(tmp_path, generation_id):
    assert len(generation_id.encode("utf-8")) == 513
    with pytest.raises(authority_spool.AuthoritySpoolError, match="GENERATION_ID_INVALID"):
        authority_spool.freeze_generation_context(tmp_path / "oversized-generation",
            generation_id=generation_id, native_universe_sha256="a" * 64, config_sha256="b" * 64)


def test_retained_two_attempt_frozen_receipt_and_cross_restore_exactly(
    retained_current_owner_metadata, forbid_current_owner_economics, monkeypatch,
):
    case = retained_current_owner_metadata
    arguments, bodies = case["arguments"], case["bodies"]
    prefix = (f"{arguments['artifact_namespace']}/winner-search/"
              f"{arguments['timeframe']}/{arguments['regime']}/frozen/")
    artifact_entries = {entry["artifact_id"][len(prefix):].replace("/", "-", 1): entry
                        for entry in arguments["manifest_entries"]
                        if entry["artifact_id"] != prefix + "receipt"}
    assert len(artifact_entries) == 17
    for name in _RESTORE_ARTIFACT_NAMES:
        assert artifact_entries[f"attempt-0001-{name}"]["descriptor"] == bodies["receipt"]["artifact_roster"][f"attempt-0001-{name}"]
    def no_writer(*_args, **_kwargs):
        pytest.fail("read-only restore used a writer/sink")
    monkeypatch.setattr(raw_winner, "_put_ascii_json", no_writer)
    rebuilt = raw_winner.build_frozen_winner_search_plan_v4_receipt(
        context_identity=arguments["context_identity"], symbol=arguments["symbol"],
        timeframe=arguments["timeframe"], regime=arguments["regime"],
        artifact_namespace=arguments["artifact_namespace"], precommitment=bodies["precommitment"],
        attempt_ledger=bodies["attempt_ledger"], horizon_execution_contracts=bodies["horizon_execution_contracts"],
        partition_cost_precommitments=bodies["partition_cost_precommitments"], artifact_entries=artifact_entries)
    assert producer._canonical_bytes(rebuilt) == case["frozen_receipt_bytes"]
    assert hashlib.sha256(case["frozen_receipt_bytes"]).hexdigest() == _RETAINED_CURRENT_FROZEN_RECEIPT_SHA256
    frozen, cross, verified_attempts, binding_context = _restore_current_binding(case)
    assert frozen.frozen_receipt_bytes == case["frozen_receipt_bytes"]
    assert cross == case["cross"]
    assert len(verified_attempts) == 2
    assert binding_context["mode"] == raw_winner.HISTORICAL_CROSS_BINDING_MODE
    assert {row["ordinal"] for row in cross["mapping"]} == {0, 1}
    assert producer._verify_current_owner_capture_binding(
        **_current_owner_capture_arguments(case, frozen=frozen, cross=cross)) is None


@pytest.mark.parametrize(("mutation", "expected"), [
    ("index", "CURRENT_OWNER_INDEX_BINDING_MISMATCH"),
    ("horizon", "CURRENT_OWNER_HORIZON_BINDING_SPLICE"),
    ("group", "CURRENT_OWNER_GROUP_BINDING_SPLICE"),
    ("native_index", "CURRENT_OWNER_NATIVE_PREFIX_INDEX_SPLICE"),
    ("second_attempt_count", "CURRENT_OWNER_SOURCE_COUNT_SPLICE"),
    ("frozen_receipt", "CURRENT_OWNER_FROZEN_RECEIPT_SPLICE"),
    ("raw_production", "CURRENT_OWNER_RAW_PRODUCTION_SPLICE"),
    ("raw_named_roster", "CURRENT_OWNER_RAW_ROSTER_SPLICE"),
])
def test_retained_current_owner_capture_rejects_each_binding_splice(
    tmp_path, retained_current_owner_metadata, forbid_current_owner_economics, mutation, expected,
):
    case = retained_current_owner_metadata
    frozen, cross, _, _ = _restore_current_binding(case)
    arguments = _current_owner_capture_arguments(case, frozen=frozen, cross=cross)
    for name in ("index", "record", "cross"):
        arguments[name] = copy.deepcopy(arguments[name])
    if mutation == "index":
        arguments["index"]["producer_request_sha256"] = "0" * 64
    elif mutation == "horizon":
        binding = next(row for row in arguments["index"]["horizon_bindings"]
                       if row["cell"] == {"symbol": frozen.symbol, "timeframe": frozen.timeframe,
                                          "regime": frozen.regime})
        binding["plan_receipt_sha256"] = "0" * 64
    elif mutation == "group":
        arguments["cross"]["source_group_decision_sha256"] = "0" * 64
    elif mutation == "native_index":
        arguments["cross"]["native_prefix_index_sha256"] = "0" * 64
    elif mutation == "second_attempt_count":
        arguments["cross"]["mapping"][1]["source_count_receipt"]["horizon_plan_receipt_sha256"] = "0" * 64
    elif mutation == "frozen_receipt":
        arguments["record"]["frozen_plan_receipt"] = arguments["record"]["raw_production_receipt"]
    else:
        prefix = f"{frozen.artifact_namespace}/winner-search/{frozen.timeframe}/{frozen.regime}/authority"
        production_id = f"{prefix}/production-receipt"
        production = producer._manifest_body(case["context"], case["entries"], production_id,
            code="TEST_RETAINED_RAW_PRODUCTION", maximum=producer.winner.MAX_REQUEST_BYTES)
        if mutation == "raw_production":
            production["request_sha256"] = "0" * 64
        else:
            production["artifact_roster"]["winner-request"] = copy.deepcopy(
                case["entries"][f"{prefix}/winner-decision"]["descriptor"])
        encoded = producer._canonical_bytes(production)
        digest = hashlib.sha256(encoded).hexdigest()
        arguments["entries"] = copy.deepcopy(case["entries"])
        descriptor = arguments["entries"][production_id]["descriptor"]
        descriptor.update({"sha256": digest, "size_bytes": len(encoded),
            "relative_path": f"objects/sha256/{digest[:2]}/{digest}.{descriptor['format']}"})
        arguments["record"]["raw_production_receipt"] = copy.deepcopy(descriptor)
        # Isolate the hostile receipt leaf; the sealed source remains immutable.
        arguments["context"] = replace(case["context"], root=tmp_path / "hostile-receipt")
        target = arguments["context"].root / descriptor["relative_path"]
        target.parent.mkdir(parents=True)
        target.write_bytes(encoded)
    with pytest.raises(producer.ForwardOOSReplayProductionError, match=expected):
        producer._verify_current_owner_capture_binding(**arguments)


@pytest.mark.parametrize(("mutation", "expected"), [
    ("body_digest", "FROZEN_PLAN_RESTORE_BODY_BINDING_INVALID"),
    ("coherent_descriptor_substitution", "FROZEN_PLAN_RECEIPT_MISMATCH"),
    ("missing_second_attempt_entry", "FROZEN_PLAN_RESTORE_ENTRY_MISSING"),
])
def test_readonly_frozen_restore_rejects_body_roster_and_missing_entry(
    retained_current_owner_metadata, forbid_current_owner_economics, mutation, expected,
):
    arguments = copy.deepcopy(retained_current_owner_metadata["arguments"])
    entries = arguments["manifest_entries"]
    bodies = arguments["body_bytes"]
    target = next(index for index, entry in enumerate(entries)
                  if entry["artifact_id"].endswith("/attempt-0001/trial_definition"))
    if mutation == "body_digest":
        changed = json.loads(bodies[target])
        changed["strategy_id"] = "SYNTHETIC_HOSTILE_TRIAL"
        bodies[target] = producer._canonical_bytes(changed)
    elif mutation == "coherent_descriptor_substitution":
        source = next(index for index, entry in enumerate(entries)
                      if entry["artifact_id"].endswith("/attempt-0000/trial_definition"))
        assert entries[source]["descriptor"] != entries[target]["descriptor"]
        entries[target]["descriptor"] = copy.deepcopy(entries[source]["descriptor"])
        bodies[target] = bodies[source]
    else:
        entries.pop(target)
        bodies.pop(target)
    with pytest.raises(raw_winner.WinnerRawEvidenceError, match=expected):
        raw_winner.restore_frozen_winner_search_plan_v4(**arguments)


@pytest.mark.parametrize(("mutation", "cause_type"), [
    ("stale_contract", ValueError), ("missing_registry_field", KeyError),
])
def test_readonly_frozen_restore_types_coherently_rehashed_malformed_ledger(
    retained_current_owner_metadata, forbid_current_owner_economics, mutation, cause_type,
):
    arguments = copy.deepcopy(retained_current_owner_metadata["arguments"])
    entries, bodies = arguments["manifest_entries"], arguments["body_bytes"]
    ordinal = next(index for index, entry in enumerate(entries)
                   if entry["artifact_id"].endswith("/frozen/attempt_ledger"))
    ledger = json.loads(bodies[ordinal])
    if mutation == "stale_contract":
        ledger["contract_version"] = "synthetic-invalid-ledger-contract"
    else:
        ledger["attempts"][0].pop("strategy_id")
        ledger["attempts_sha256"] = producer._sha(ledger["attempts"])
    encoded = producer._canonical_bytes(ledger)
    digest = hashlib.sha256(encoded).hexdigest()
    descriptor = entries[ordinal]["descriptor"]
    descriptor.update({"sha256": digest, "size_bytes": len(encoded),
        "relative_path": f"objects/sha256/{digest[:2]}/{digest}.{descriptor['format']}"})
    bodies[ordinal] = encoded
    with pytest.raises(raw_winner.WinnerRawEvidenceError,
                       match="FROZEN_PLAN_RESTORE_RECEIPT_INVALID") as caught:
        raw_winner.restore_frozen_winner_search_plan_v4(**arguments)
    assert caught.value.code == "FROZEN_PLAN_RESTORE_RECEIPT_INVALID"
    assert type(caught.value.__cause__) is cause_type


@pytest.mark.parametrize("schema", [
    raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA,
    raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4,
])
def test_current_mode_cross_schemas_refuse_before_canonical_delegation(
    retained_current_owner_metadata, forbid_current_owner_economics, monkeypatch, schema,
):
    calls = []
    def forbidden(*_args, **_kwargs):
        calls.append("canonical")
        pytest.fail("CURRENT-mode cross reached canonical historical delegation")
    monkeypatch.setattr(raw_winner, "restore_frozen_winner_search_plan_v4", forbidden)
    monkeypatch.setattr(raw_winner, "verify_historical_frozen_winner_cross_binding", forbidden)
    cross = {**retained_current_owner_metadata["cross"], "schema_version": schema, "mode": "CURRENT"}
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="CURRENT_OWNER_CROSS_BINDING_MODE_FORBIDDEN"):
        _restore_current_binding(retained_current_owner_metadata, cross=cross)
    assert calls == []


@pytest.mark.parametrize("schema", [
    raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA,
    raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4,
])
def test_raw_current_mode_cross_schemas_refuse_before_canonical_delegation(
    retained_current_owner_metadata, forbid_current_owner_economics, monkeypatch, schema,
):
    case = retained_current_owner_metadata
    frozen = raw_winner.restore_frozen_winner_search_plan_v4(**case["arguments"])
    calls = []
    def forbidden(*_args, **_kwargs):
        calls.append("canonical")
        pytest.fail("CURRENT-mode cross reached canonical historical delegation")
    monkeypatch.setattr(raw_winner, "_verified_frozen_attempt_bundle", forbidden)
    monkeypatch.setattr(raw_winner, "_validated_horizon_winner_cross_binding", forbidden)
    cross = {**case["cross"], "schema_version": schema, "mode": "CURRENT"}
    with pytest.raises(raw_winner.WinnerRawEvidenceError,
                       match="HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN"):
        raw_winner.verify_historical_frozen_winner_cross_binding(
            producer._canonical_bytes(cross), frozen=frozen)
    assert calls == []


def test_readonly_current_owner_rejects_coherently_rehashed_mapping_splice(
    retained_current_owner_metadata, forbid_current_owner_economics,
):
    cross = copy.deepcopy(retained_current_owner_metadata["cross"])
    cross["mapping"][1]["winner_process_candidate_id"] = cross["mapping"][0]["winner_process_candidate_id"]
    cross["mapping_sha256"] = producer._sha(cross["mapping"])
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="CURRENT_OWNER_CROSS_BINDING_REVERIFY_FAILED") as caught:
        _restore_current_binding(retained_current_owner_metadata, cross=cross)
    assert caught.value.details == ("PROVENANCE_CROSS_BINDING_ATTEMPT_SPLICE",)
    assert type(caught.value.__cause__) is raw_winner.WinnerRawEvidenceError


def test_current_owner_rejects_missing_pair_before_cross_delegation(
    retained_current_owner_metadata, forbid_current_owner_economics, monkeypatch,
):
    frozen = raw_winner.restore_frozen_winner_search_plan_v4(**retained_current_owner_metadata["arguments"])
    calls = []
    def without_pair(**_kwargs):
        calls.append("restore")
        return replace(frozen, frozen_tier_pair_contract_bytes=b"")
    def forbidden(*_args, **_kwargs):
        pytest.fail("pair-less current plan reached canonical cross delegation")
    monkeypatch.setattr(raw_winner, "restore_frozen_winner_search_plan_v4", without_pair)
    monkeypatch.setattr(raw_winner, "verify_historical_frozen_winner_cross_binding", forbidden)
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="CURRENT_OWNER_FROZEN_PAIR_GENERATION_MISMATCH"):
        _restore_current_binding(retained_current_owner_metadata)
    assert calls == ["restore"]


def _owner_generation_tuple(generation):
    winner_generation = generation.winner_generation
    return ({"contract_version": generation.version},
            {"contract_version": winner_generation.request_version,
             "precommitment": {"payload": {"contract_version": winner_generation.precommitment_version}},
             "evidence_ledger": {"payload": {"contract_version": winner_generation.evidence_version}}},
            {"contract_version": winner_generation.selection_version})


@pytest.mark.parametrize("generation", [book.ARCHIVED_GENERATION, book.CURRENT_GENERATION, book.V16_GENERATION])
def test_owner_generation_tuple_accepts_only_homogeneous_central_generations(generation):
    assert producer._selected_owner_generation(*_owner_generation_tuple(generation)) is generation


@pytest.mark.parametrize("arm", ["book", "request", "precommitment", "evidence", "decision"])
@pytest.mark.parametrize("generation", [book.CURRENT_GENERATION, book.V16_GENERATION])
def test_owner_generation_tuple_rejects_each_mixed_generation_arm(arm, generation):
    current_book, current_request, current_decision = _owner_generation_tuple(generation)
    mismatch = book.ARCHIVED_GENERATION if generation is book.CURRENT_GENERATION else book.CURRENT_GENERATION
    old_book, old_request, old_decision = _owner_generation_tuple(mismatch)
    if arm == "book":
        current_book = old_book
    elif arm == "request":
        current_request["contract_version"] = old_request["contract_version"]
    elif arm == "precommitment":
        current_request["precommitment"] = old_request["precommitment"]
    elif arm == "evidence":
        current_request["evidence_ledger"] = old_request["evidence_ledger"]
    else:
        current_decision = old_decision
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="SELECTED_OWNER_GENERATION_TUPLE_INVALID"):
        producer._selected_owner_generation(current_book, current_request, current_decision)


@pytest.mark.parametrize("wrong_restored_generation", [False, True])
def test_transport_owner_restore_dispatch_keeps_canonical_v5_boundary(monkeypatch, wrong_restored_generation):
    """Dispatch-only spies; this supplies no authenticated owner evidence."""
    context = SimpleNamespace(generation_id="SYNTHETIC_TRANSPORT_DISPATCH",
        context_sha256="a" * 64, native_universe_sha256="b" * 64, config_sha256="c" * 64)
    generation = book.V16_GENERATION
    frozen = SimpleNamespace(frozen_tier_pair_contract_bytes=b"synthetic",
        selection_generation=(book.CURRENT_GENERATION.winner_generation
                              if wrong_restored_generation else generation.winner_generation))
    cross = {"schema_version": generation.cross_binding_schema}
    calls = []

    def legacy_forbidden(**_kwargs):
        pytest.fail("transport owner selected the legacy frozen reader")

    def restore(**kwargs):
        calls.append("v5")
        assert kwargs["context_identity"]["generation_context_sha256"] == context.context_sha256
        assert kwargs["manifest_entries"] == [] and kwargs["body_bytes"] == []
        return frozen

    def verify(encoded, *, frozen):
        calls.append("cross")
        assert json.loads(encoded) == cross
        assert frozen.selection_generation is generation.winner_generation
        return cross, (), {"synthetic_dispatch_only": True}

    monkeypatch.setattr(raw_winner, "restore_frozen_winner_search_plan_v4", legacy_forbidden)
    monkeypatch.setattr(raw_winner, "restore_frozen_winner_search_plan_v5", restore)
    monkeypatch.setattr(raw_winner, "verify_historical_frozen_winner_cross_binding", verify)
    args = dict(context=context, entries={}, symbol="SYNTHETIC", timeframe="H1", regime="TREND",
                namespace="synthetic", cross=cross, _generation=generation)
    if wrong_restored_generation:
        with pytest.raises(producer.ForwardOOSReplayProductionError,
                           match="CURRENT_OWNER_FROZEN_PAIR_GENERATION_MISMATCH"):
            producer._restore_current_owner_frozen_binding(**args)
        assert calls == ["v5"]
    else:
        assert producer._restore_current_owner_frozen_binding(**args) == (
            frozen, cross, (), {"synthetic_dispatch_only": True})
        assert calls == ["v5", "cross"]


@pytest.mark.parametrize("reason,expected", [
    ("V16_INPUT_STRUCTURE_LIMIT_EXCEEDED", "V16_INPUT_STRUCTURE_LIMIT_EXCEEDED"),
    ("PAYLOAD_ROOT_TYPE_INVALID", "WINNER_REQUEST_RAW_REPLAY_FAILED"),
], ids=["capacity", "payload"])
def test_transport_request_capacity_refusal_keeps_distinct_code(monkeypatch, reason, expected):
    generation = book.V16_GENERATION.winner_generation
    payloads = [{"contract_version": generation.precommitment_version},
                {"contract_version": "synthetic-dispatch-only"},
                {"contract_version": generation.evidence_version}]
    body = {"contract_version": generation.request_version}
    for name, payload in zip(("precommitment", "attempt_ledger", "evidence_ledger"), payloads):
        body[name] = {"payload": payload, "payload_sha256": producer._sha(payload)}

    def refuse(*_args):
        raise ValueError(reason)

    monkeypatch.setattr(producer.winner, "create_winner_selection_request", refuse)
    with pytest.raises(producer.ForwardOOSReplayProductionError) as caught:
        producer._winner_request(body, "f" * 64)
    assert caught.value.code == expected


@pytest.mark.parametrize("mutation", ["hash", "byte_bound"])
def test_readonly_frozen_list_reader_enforces_actual_file_identity_and_bound(
    tmp_path, retained_current_owner_metadata, forbid_current_owner_economics, mutation,
):
    arguments = retained_current_owner_metadata["arguments"]
    ordinal = next(index for index, entry in enumerate(arguments["manifest_entries"])
                   if entry["artifact_id"].endswith("/frozen/horizon_execution_contracts"))
    descriptor = arguments["manifest_entries"][ordinal]["descriptor"]
    encoded = arguments["body_bytes"][ordinal]
    assert type(json.loads(encoded)) is list
    # Only the already-authenticated metadata leaf is copied for this reader check.
    context = replace(retained_current_owner_metadata["context"], root=tmp_path / "reader-root")
    target = context.root / descriptor["relative_path"]
    target.parent.mkdir(parents=True)
    target.write_bytes(encoded)
    assert producer._read_descriptor_bytes(context, descriptor, code="READONLY_LIST") == encoded
    if mutation == "hash":
        target.write_bytes(encoded[:-1] + b" ")
        expected = "READONLY_LIST_INTEGRITY_MISMATCH"
        maximum = len(encoded)
    else:
        expected = "READONLY_LIST_FORMAT_OR_BOUND_INVALID"
        maximum = len(encoded) - 1
    with pytest.raises(producer.ForwardOOSReplayProductionError, match=expected):
        producer._read_descriptor_bytes(context, descriptor, code="READONLY_LIST", maximum=maximum)


_RETAINED_SELECTED_OWNER_004_ROOT = (
    "selected_owner_flat_control/runtime/attempt_004_pytest_tmp/test_declared_native_two_trial0"
)
_RETAINED_SELECTED_OWNER_004_RESULT_SHA256 = "126a983aa8e925e3ef64a566c2d2e3b40430596f1cef85f52465547d1314f11a"
_RETAINED_SELECTED_OWNER_004_EVIDENCE_SHA256 = "9b18e7ea394c976a3ef41a129a8c6d21e220ed436bc75814e1dfe42e7a34e2c2"
_RETAINED_SELECTED_OWNER_004_MANIFEST_SHA256 = "6a2591966545bd014f876e1d7f0c56f719bad5ca3932bf739f8190558a964854"


@pytest.fixture(scope="module")
def retained_selected_owner_004(tmp_path_factory):
    """Genuine saved engineering outcome; missing or changed custody is a failure."""
    original = (Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007"
                / _RETAINED_SELECTED_OWNER_004_ROOT)
    result_bytes = (original / "selected-owner-result.json").read_bytes()
    assert len(result_bytes) == 6274192
    assert hashlib.sha256(result_bytes).hexdigest() == _RETAINED_SELECTED_OWNER_004_RESULT_SHA256
    result = json.loads(result_bytes)
    evidence_path = original / "actual-attempt-evidence.json"
    assert evidence_path.stat().st_size == 23590511
    with evidence_path.open("rb") as handle:
        assert hashlib.file_digest(handle, "sha256").hexdigest() == _RETAINED_SELECTED_OWNER_004_EVIDENCE_SHA256
    assert result["actual_attempt_evidence"]["sha256"] == _RETAINED_SELECTED_OWNER_004_EVIDENCE_SHA256
    assert result["actual_attempt_evidence"]["bytes"] == 23590511
    manifest_bytes = (original / "spool" / _RETAINED_CURRENT_MANIFEST).read_bytes()
    assert len(manifest_bytes) == 105194
    assert hashlib.sha256(manifest_bytes).hexdigest() == _RETAINED_SELECTED_OWNER_004_MANIFEST_SHA256
    receipt = result["symbol_receipt"]
    assert receipt["symbol_manifest"]["sha256"] == _RETAINED_SELECTED_OWNER_004_MANIFEST_SHA256
    destination = tmp_path_factory.mktemp("saved-owner-cutoff") / "spool"
    shutil.copytree(original / "spool", destination)
    context = authority_spool.reopen_generation_context(destination,
        expected_generation_id=receipt["generation_id"],
        expected_native_universe_sha256=receipt["native_universe_sha256"],
        expected_config_sha256=receipt["config_sha256"])
    manifest, _ = producer._verified_manifest(context, receipt, receipt["symbol"])
    assert manifest["artifact_count"] == 196
    book_body = producer._owned_book(result["book_decision"], result["book_decision"]["receipt_sha256"])
    yield context, result, book_body
    assert (original / "spool" / _RETAINED_CURRENT_MANIFEST).read_bytes() == manifest_bytes


def test_saved_current_owner_reopens_exact_prefix_before_selection_without_execution(
    retained_selected_owner_004, monkeypatch,
):
    context, result, book_body = retained_selected_owner_004
    def forbidden(*_args, **_kwargs):
        pytest.fail("saved owner reopen attempted fitting, signals or financial execution")
    monkeypatch.setattr(producer.Backtester, "run", forbidden)
    monkeypatch.setattr(producer.search_execution, "fit_native_timeframe_for_search", forbidden)
    monkeypatch.setattr(producer.pm_regime.MarketRegimeDetector, "compute_regime_scores", forbidden)
    target = result["target_book_cell"]
    assert target["historical_state"] == "WINNER"
    assert target == next(cell for cell in book_body["cells"] if cell["cell_id"] == target["cell_id"])
    assert result["owner_reopen"]["status"] == "EXPECTED_MIGRATION_BLOCK"
    owner = producer._reopen_owner_capture(
        context=context, anchor=result["adapter_anchor"], book_body=book_body,
        cell_id=target["cell_id"], symbol_receipt=result["symbol_receipt"],
        adapter_index_descriptor=result["adapter_index_descriptor"])
    assert (owner.cell_id, owner.symbol, owner.timeframe, owner.regime, owner.anchor_id) == (
        target["cell_id"], target["symbol"], target["timeframe"], target["regime"],
        result["adapter_anchor"]["anchor_id"])
    assert owner.strategy_id == owner.strategy_class == "EMACrossoverStrategy"
    assert owner.process_candidate_id == target["local_process_candidate_id"]
    assert owner.process_sha256 == target["local_process_sha256"]
    assert owner.detector_record == result["owner_cutoff_proof"]["detector_record"]
    assert owner.source_count_receipt["native_prefix_index_sha256"] == result["native_product_index_sha256"]
    assert producer._utc(owner.detector_record["fit_cutoff_utc"], "TEST_DETECTOR_CUTOFF") == (
        producer._utc(owner.source_count_receipt["fit_end_exclusive_utc"], "TEST_PREFIX_END")
    ) == pd.Timestamp("2024-07-14T08:00:00Z").to_pydatetime()
    assert producer._utc(result["adapter_anchor"]["selection_cutoff_utc"], "TEST_SELECTION_CUTOFF") == (
        pd.Timestamp("2024-07-14T08:05:00Z").to_pydatetime()
    )


@pytest.mark.parametrize(("detector", "own_end", "selection", "expected"), [
    ("2024-07-14T00:00:00Z", "2024-07-14T00:00:00Z", "2024-07-14T00:00:00Z", True),
    ("2024-07-14T08:00:00Z", "2024-07-14T08:00:00Z", "2024-07-14T08:05:00Z", True),
    ("2024-07-14T10:00:00+02:00", "2024-07-14T08:00:00Z", "2024-07-14T08:05:00Z", True),
    ("2024-07-14T07:00:00Z", "2024-07-14T08:00:00Z", "2024-07-14T08:05:00Z", False),
    ("2024-07-14T08:01:00Z", "2024-07-14T08:00:00Z", "2024-07-14T08:05:00Z", False),
    ("2024-07-14T08:05:00Z", "2024-07-14T08:00:00Z", "2024-07-14T08:05:00Z", False),
    ("2024-07-14T08:10:00Z", "2024-07-14T08:10:00Z", "2024-07-14T08:05:00Z", False),
], ids=["aligned", "between-bars", "aware-offset", "older-than-own-prefix", "later-than-own-prefix",
        "detector-equals-selection-only", "future-own-prefix"])
def test_selected_regime_cutoff_requires_exact_own_closed_prefix(detector, own_end, selection, expected):
    assert producer._selected_regime_cutoff_matches_source(
        detector_cutoff_utc=detector, source_prefix_end_utc=own_end,
        selection_cutoff_utc=selection) is expected


@pytest.mark.parametrize(("field", "expected"), [
    ("detector_cutoff_utc", "SELECTED_REGIME_CUTOFF_INVALID"),
    ("source_prefix_end_utc", "SELECTED_SOURCE_PREFIX_END_INVALID"),
    ("selection_cutoff_utc", "SELECTION_CUTOFF_INVALID"),
])
def test_selected_regime_cutoff_rejects_naive_timestamps(field, expected):
    arguments = {"detector_cutoff_utc": "2024-07-14T08:00:00Z",
                 "source_prefix_end_utc": "2024-07-14T08:00:00Z",
                 "selection_cutoff_utc": "2024-07-14T08:05:00Z"}
    arguments[field] = arguments[field].removesuffix("Z")
    with pytest.raises(producer.ForwardOOSReplayProductionError, match=expected) as caught:
        producer._selected_regime_cutoff_matches_source(**arguments)
    assert caught.value.code == expected


def _mechanical_compact_rich_account():
    """Invented schema control only; no native, engine, or qualification evidence."""
    return {"symbol": "AUDUSD", "timeframe": "H1", "cell_id": "0" * 64,
        "tier": {}, "initial_cash": 10000.0, "warmup_bars": 1,
        "instrument_spec": {}, "position_namespace": "MECHANICAL_SCHEMA_CONTROL",
        "trades": [], "position_events": [], "engine_equity_growth": {},
        "engine_spread_admission": {}, "engine_spread_model": {},
        "execution_cost_timing_contract": {}, "numerical_execution_cost_attestation": {},
        "financing_model": {}, "derived": {"mechanical_control": True},
        "valuation_source": {}, "valuation_rows": [], "m5_marks": [],
        "source_rows": [
            {"open_utc": "2026-01-05T00:00:00+00:00", "close_utc": "2026-01-05T01:00:00+00:00",
             "open": 1.0, "high": 1.02, "low": 0.99, "close": 1.01,
             "recorded_entry_points": None, "recorded_intrabar_points": 3.0,
             "entry_half_spread": 0.0001, "intrabar_half_spread": 0.0002, "engine_equity": 10000},
            {"open_utc": "2026-01-05T01:00:00+00:00", "close_utc": "2026-01-05T02:00:00+00:00",
             "open": 1.01, "high": 1.03, "low": 1.0, "close": 1.02,
             "recorded_entry_points": 0.0, "recorded_intrabar_points": 4.0,
             "entry_half_spread": 0.0002, "intrabar_half_spread": 0.0003,
             "engine_equity": 9999.123456789}]}


def test_mechanical_compact_account_retains_full_unrounded_engine_vector():
    """Writer-shape control, not a positive native transport or engine replay."""
    rich = _mechanical_compact_rich_account()
    before = copy.deepcopy(rich)
    compact = producer._compact_tier_account(rich)
    assert rich == before and compact is not rich
    assert set(compact) == (set(rich) - {"source_rows", "valuation_rows", "m5_marks"}) | {"engine_equity"}
    assert compact["engine_equity"] == [10000, 9999.123456789]
    assert type(compact["engine_equity"][0]) is int
    assert {key: value for key, value in compact.items() if key != "engine_equity"} == {
        key: value for key, value in rich.items() if key not in {"source_rows", "valuation_rows", "m5_marks"}}
    compact["engine_equity"].pop()
    assert rich == before


@pytest.mark.parametrize("mutation", ["existing_vector", "empty_source", "missing_valuation"])
def test_mechanical_compact_account_rejects_ambiguous_or_incomplete_views(mutation):
    rich = _mechanical_compact_rich_account()
    if mutation == "existing_vector":
        rich["engine_equity"] = [10000.0, 10000.0]
    elif mutation == "empty_source":
        rich["source_rows"] = []
    else:
        rich.pop("valuation_rows")
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="^FROZEN_TIER_COMPACT_ACCOUNT_INPUT_INVALID$"):
        producer._compact_tier_account(rich)


@pytest.mark.parametrize("value", [None, True, float("nan"), float("inf"), "10000"],
    ids=["null", "bool", "nan", "infinite", "string"])
def test_mechanical_compact_account_cannot_invent_engine_equity(value):
    rich = _mechanical_compact_rich_account()
    rich["source_rows"][0]["engine_equity"] = value
    with pytest.raises(producer.ForwardOOSReplayProductionError,
                       match="^FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE$"):
        producer._compact_tier_account(rich)


_CURRENT_RAW_002_RESULT_RELATIVE = (
    "bound_midnight_selected_owner_capture/results/attempt_002/bound-selected-owner-result.json"
)
_CURRENT_RAW_002_RESULT_SHA256 = "4b9fcfbb675f97094c4bfb8dab035e7813b02d81667a71bb5073ac2169a2d8e4"
_CURRENT_RAW_002_RESULT_BYTES = 9_238_679
_CURRENT_RAW_CONFIG_SHA256 = "c03525f2ecddcb36271feccbe5daa8e0944d3666ec0f21c41c3a288f01cb84a9"
_CURRENT_RAW_EFFECTIVE_SHA256 = "20bba367caa29509b68b7b9105db84adc3cd622b9f26a3a56acc11c85a21ef4f"
_CURRENT_RAW_DYNAMIC_PATHS = (
    "trades", "position_events", "engine_equity", "engine_equity_growth",
    "engine_spread_admission", "financing_model/pre_open_units",
    "financing_model/close_units", "financing_model/long_per_lot",
    "financing_model/short_per_lot", "derived/positions",
    "derived/transactions", "derived/cash_inventory",
)


def _current_raw_file_identity(path):
    path = Path(path).resolve(strict=True)
    before = path.stat()
    assert path.is_file(), "CURRENT_RAW_INPUT_NOT_FILE"
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), (
        "CURRENT_RAW_INPUT_CHANGED_DURING_READ"
    )
    return {"sha256": digest, "bytes": after.st_size}


def _prepare_current_raw_inputs(tmp_path):
    """Prepare genuine unissued inputs and a labelled capacity format; never replay."""
    import pm_production_config_binding as binding

    project = Path(__file__).resolve().parents[1]
    audit = project / "audit_reports/fxpm_workshop_20261007"
    result_path = (audit / _CURRENT_RAW_002_RESULT_RELATIVE).resolve(strict=True)
    assert _current_raw_file_identity(result_path) == {
        "sha256": _CURRENT_RAW_002_RESULT_SHA256, "bytes": _CURRENT_RAW_002_RESULT_BYTES,
    }, "CURRENT_RAW_002_RESULT_CHANGED"
    result = json.loads(result_path.read_bytes())
    assert result["status"] == "PASS", "CURRENT_RAW_002_NOT_ACCEPTED_PASS"
    assert result["owner_reopen"]["status"] == "VERIFIED", "CURRENT_RAW_002_OWNER_NOT_VERIFIED"
    evidence_path = Path(result["actual_attempt_evidence"]["path"]).resolve(strict=True)
    assert evidence_path.parent == result_path.parent, "CURRENT_RAW_002_EVIDENCE_PATH_CHANGED"
    assert _current_raw_file_identity(evidence_path) == {
        key: result["actual_attempt_evidence"][key] for key in ("sha256", "bytes")
    }, "CURRENT_RAW_002_EVIDENCE_CHANGED"

    original = Path(result["spool_root"]).resolve(strict=True)
    original_source = Path(result["original_spool_root"]).resolve(strict=True)
    receipt = copy.deepcopy(result["symbol_receipt"])
    manifest_descriptor = receipt["symbol_manifest"]
    original_manifest = original / manifest_descriptor["relative_path"]
    manifest_identity = {"sha256": manifest_descriptor["sha256"],
                         "bytes": manifest_descriptor["size_bytes"]}
    assert _current_raw_file_identity(original_manifest) == manifest_identity, (
        "CURRENT_RAW_002_MANIFEST_CHANGED"
    )
    retained_manifest = Path(result["manifest"]["retained_path"]).resolve(strict=True)
    assert retained_manifest.parent == result_path.parent, "CURRENT_RAW_MANIFEST_SNAPSHOT_PATH_CHANGED"
    assert _current_raw_file_identity(retained_manifest) == manifest_identity, (
        "CURRENT_RAW_MANIFEST_SNAPSHOT_CHANGED"
    )

    config_path = (audit / "selected_owner_bound_context_preflight/runtime/attempt_001_pytest_tmp"
                   / "test_bound_midnight_native_inp0/config.json").resolve(strict=True)
    assert Path(result["config"]["active_config_path"]).resolve(strict=True) == config_path, (
        "CURRENT_RAW_109_CONFIG_PATH_CHANGED"
    )
    payload, loaded_config = binding.load_effective_pipeline_config(config_path)
    native_root = (Path(loaded_config.data_dir) / "native").resolve(strict=True)
    profile_path = (native_root / "terms/spread_profile.json").resolve(strict=True)
    axis_receipt = (audit / "current_owner_raw_capacity_preflight/results/attempt_002"
                    / "capacity-result.json").resolve(strict=True)

    scratch = Path(tmp_path).resolve()
    protected = (original, original_source, result_path.parent, config_path.parent,
                 native_root, axis_receipt.parent)
    assert scratch.is_relative_to(project), "CURRENT_RAW_SCRATCH_OUTSIDE_WORKSPACE"
    assert all(not scratch.is_relative_to(path) and not path.is_relative_to(scratch)
               for path in protected), "CURRENT_RAW_SCRATCH_OVERLAPS_RETAINED_INPUT"
    destination = scratch / "spool"
    assert not destination.exists(), "CURRENT_RAW_SCRATCH_SPOOL_ALREADY_EXISTS"
    scratch.mkdir(parents=True, exist_ok=True)
    shutil.copytree(original, destination)

    expected_context = {
        "expected_generation_id": receipt["generation_id"],
        "expected_native_universe_sha256": receipt["native_universe_sha256"],
        "expected_config_sha256": receipt["config_sha256"],
    }
    context = authority_spool.reopen_generation_context(destination, **expected_context)
    source_context = authority_spool.reopen_generation_context(original_source, **expected_context)
    assert context.context_sha256 == source_context.context_sha256 == receipt["generation_context_sha256"], (
        "CURRENT_RAW_002_CONTEXT_CHANGED"
    )
    producer._verified_manifest(context, receipt, receipt["symbol"])
    frozen = binding.freeze_production_pipeline_config(
        context, active_config_path=config_path, config_payload=payload, supplied_config=loaded_config)
    assert frozen.config_sha256 == context.config_sha256 == _CURRENT_RAW_CONFIG_SHA256, (
        "CURRENT_RAW_109_CONFIG_CHANGED"
    )
    assert frozen.effective_config_sha256 == result["effective_config_sha256"] == (
        _CURRENT_RAW_EFFECTIVE_SHA256
    ), "CURRENT_RAW_109_EFFECTIVE_CONFIG_CHANGED"
    config = frozen.materialize(context)
    deployment = producer._deployment_config(config)

    anchor = copy.deepcopy(result["adapter_anchor"])
    assert anchor["selection_cutoff_utc"] == "2024-07-14T00:00:00Z", (
        "CURRENT_RAW_SELECTION_CUTOFF_CHANGED"
    )
    assert (anchor["forward_start_date"], anchor["forward_end_date"]) == (
        "2024-07-15", "2024-10-11"
    ), "CURRENT_RAW_OUTER_ANCHOR_CHANGED"
    book_body = producer._owned_book(result["book_decision"],
                                     result["book_decision"]["receipt_sha256"])
    assert book_body["contract_version"] == book.CURRENT_GENERATION.version, (
        "CURRENT_RAW_BOOK_GENERATION_CHANGED"
    )
    target_id = result["owner_reopen"]["cell_id"]
    target = next(cell for cell in book_body["cells"] if cell["cell_id"] == target_id)
    assert (target["symbol"], target["timeframe"], target["regime"], target["historical_state"]) == (
        "AUDUSD", "H1", "TREND", "WINNER"
    ), "CURRENT_RAW_SELECTED_CELL_CHANGED"
    owner = producer._reopen_owner_capture(
        context=context, anchor=anchor, book_body=book_body, cell_id=target_id,
        symbol_receipt=receipt, adapter_index_descriptor=result["adapter_index_descriptor"])
    assert owner.process_sha256 == result["owner_reopen"]["process_sha256"], (
        "CURRENT_RAW_SELECTED_PROCESS_CHANGED"
    )
    full_book_descriptor = producer._write_raw_ledger(
        context, book_body, role="UNISSUED_CURRENT_RAW_FULL_BOOK_INPUT")

    windows = producer._capital_phase_windows([anchor], {"anchor_plan": [{
        "anchor_id": anchor["anchor_id"],
        "inner_window": {"start_date": "2023-07-15", "end_date": "2024-07-14"},
        "outer_window": {"start_date": "2024-07-15", "end_date": "2024-10-12"},
    }]})
    outer_window = windows[anchor["anchor_id"]][producer.CAPITAL_OUTER_EVALUATION]
    start, end = producer._capital_window_datetimes(
        outer_window, phase_id=producer._capital_phase_id(
            anchor["anchor_id"], producer.CAPITAL_OUTER_EVALUATION))
    cutoff = "2024-10-12T00:00:00Z"
    accessed_at = pd.Timestamp.now(tz="UTC").floor("s").isoformat().replace("+00:00", "Z")
    products = {}
    for timeframe in ("H1", "M5"):
        products[timeframe] = native_prefix.produce_activated_native_prefix(
            data_dir=Path(config.data_dir), context=context, symbol=owner.symbol,
            timeframe=timeframe, cutoff_utc=cutoff, source_accessed_at_utc=accessed_at,
            production_contract_required=not config.research_mode)
    execution_products = {f"{owner.symbol}|H1": products["H1"]}
    fine_products = {f"{owner.symbol}|M5": products["M5"]}
    required = producer._required_forward_product_rosters([anchor], {anchor["anchor_id"]: book_body})
    assert required[anchor["anchor_id"]] == {
        "execution_product_keys": [f"{owner.symbol}|H1"],
        "valuation_m5_product_keys": [f"{owner.symbol}|M5"],
    }, "CURRENT_RAW_FULL_BOOK_REQUIRES_DIFFERENT_PRODUCT_ROSTER"
    precommit = producer._product_roster_identities(
        plans=[anchor], capital_phase_windows_by_anchor=windows, required=required,
        execution_products_by_anchor={anchor["anchor_id"]: execution_products},
        valuation_products_by_anchor={anchor["anchor_id"]: fine_products})[0]
    fine_identity = producer._native_product_identity(
        products["M5"], expected_symbol=owner.symbol, expected_timeframe="M5",
        expected_cutoff_utc=cutoff, role=f"SPREAD_SOURCE:{anchor['anchor_id']}:{owner.symbol}|M5")
    precommit.update({
        "full_book_descriptor": full_book_descriptor,
        "full_book_sha256": full_book_descriptor["sha256"],
        "spread_source_products": [fine_identity],
        "spread_source_product_roster_sha256": producer._sha([fine_identity]),
    })
    all_identities = (*precommit["execution_products"], *precommit["valuation_m5_products"],
                      *precommit["spread_source_products"])
    assert sum(int(item["prefix_rows"]) for item in all_identities) <= (
        producer._MAX_NATIVE_ROWS_PER_ANCHOR
    ), "CURRENT_RAW_NATIVE_ROW_BOUND_EXCEEDED"
    assert sum(int(item["prefix_bytes"]) for item in all_identities) <= (
        producer._MAX_NATIVE_BYTES_PER_ANCHOR
    ), "CURRENT_RAW_NATIVE_BYTE_BOUND_EXCEEDED"
    global_input = {
        "schema_version": producer.REPLAY_PRECOMMIT_SCHEMA,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "deployment_binding": deployment.receipt_body(),
        "anchor_product_identities": [precommit],
        "anchor_product_identities_sha256": producer._sha([precommit]),
        "fixture_provenance": "UNISSUED_INPUT_CARRIER_NOT_RELEASE_AUTHORITY",
        "qualification_authority": False,
    }
    global_descriptor = producer._write_raw_ledger(
        context, global_input, role="UNISSUED_CURRENT_RAW_GLOBAL_INPUT")
    producer._reopen_original_global_precommit(
        context=context, descriptor=global_descriptor, precommit_anchor=precommit,
        deployment=deployment)

    spec, execution_spec_receipt = producer._freeze_execution_spec(
        product=products["H1"], config=config, deployment=deployment)
    fine_spec, fine_spec_receipt = producer._freeze_execution_spec(
        product=products["M5"], config=config, deployment=deployment)
    assert asdict(fine_spec) == asdict(spec), "CURRENT_RAW_FINE_TERMS_CHANGED"
    profile = FrozenSpreadProfile(profile_path)
    pair = target["validated_cost_ladder_tolerance"]["frozen_tier_pair_contract"]
    expected_profile_hashes = {
        item["frozen_spread_tier"]["profile_sha256"] for item in pair["tiers"]
    }
    assert expected_profile_hashes == {profile.sha256}, "CURRENT_RAW_ORIGINAL_PROFILE_CHANGED"
    tiers = {
        level: profile.resolve(owner.symbol, owner.timeframe, level, point=spec.point,
                               source_identity_sha256=precommit["execution_products"][0]["source_identity_sha256"])
        for level in ("P75", "P95")
    }
    assert all(tier.source_timeframe == "M5" for tier in tiers.values()), (
        "CURRENT_RAW_FINE_TIMEFRAME_CHANGED"
    )
    tier_descriptor = tiers["P75"].descriptor()
    profile_binding = tier_descriptor["source_binding"]
    execution_identity = precommit["execution_products"][0]
    assert (
        fine_identity["source_bars_sha256"] == profile_binding["bars_sha256"]
        and fine_identity["source_audit_sidecar_sha256"] == profile_binding["sidecar_sha256"]
        and fine_identity["source_provenance_sha256"] == profile_binding["provenance_sha256"]
        and execution_identity["source_bars_sha256"] == profile_binding["target_bars_sha256"]
        and fine_identity["activation_id"] == tier_descriptor["activation"]["id"]
        and fine_identity["activation_manifest_sha256"] == tier_descriptor["activation"]["manifest_sha256"]
        and fine_identity["source_identity_sha256"] == execution_identity["source_identity_sha256"]
    ), "CURRENT_RAW_PROFILE_SOURCE_BINDING_CHANGED"
    profile_descriptor = producer._persist_frozen_profile(context, profile_path, profile.sha256)
    deployment_descriptors = producer._persist_deployment_sources(context, deployment)
    opened = native_prefix.open_verified_prefix_frame(products["H1"])
    _, closes = producer._frame_interval(opened)
    frame = opened.loc[closes <= end].copy(deep=True)
    producer._selection_prefix_match(frame, owner)
    fine_frame = native_prefix.open_verified_prefix_frame(products["M5"])
    observations, _ = producer.spread_profile.align_prior_completed_spreads(
        frame.index, fine_frame, "M5", include_matched_closes=True)
    warmup = int(frame.index.searchsorted(start, side="left"))
    geometry = {
        "native_axis_rows": len(frame), "warmup_bars": warmup,
        "scored_bars": len(frame) - warmup, "fine_native_rows": len(fine_frame),
        "calendar_days": (end - start).days, "governance_policy": None,
    }
    assert geometry == {
        "native_axis_rows": 34908, "warmup_bars": 32772, "scored_bars": 2136,
        "fine_native_rows": 417651, "calendar_days": 89, "governance_policy": None,
    }, "CURRENT_RAW_SOURCE_GEOMETRY_CHANGED"

    genesis = {"mode": producer.NEW_SIMULATED_FLAT_GENESIS,
               "initial_cash": float(config.initial_capital),
               "initial_inventory": [], "owner_changes": []}
    projection = {"cell_id": owner.cell_id, "process_sha256": owner.process_sha256}
    arguments = dict(
        context=context, anchor=anchor, specialists=[owner],
        native_products=execution_products, valuation_products=fine_products,
        precommit_anchor=precommit, capital_phase=producer.CAPITAL_OUTER_EVALUATION,
        capital_window=outer_window, config=config, deployment=deployment,
        account_genesis=genesis, cell_projection=projection,
        spread_source_products=fine_products, frozen_profile_path=profile_path,
        full_book_body=book_body, frozen_pipeline_config=frozen,
        owner_symbol_receipt=receipt,
        original_adapter_index_descriptor=copy.deepcopy(result["adapter_index_descriptor"]),
        original_global_precommit_descriptor=global_descriptor,
    )
    arguments_by_phase = {
        phase: {**arguments, "capital_phase": phase, "capital_window": copy.deepcopy(window)}
        for phase, window in windows[anchor["anchor_id"]].items()
    }
    for phase, phase_arguments in arguments_by_phase.items():
        assert producer._original_capital_window(anchor, precommit, phase) == (
            phase_arguments["capital_window"]
        ), "CURRENT_RAW_PHASE_WINDOW_CHANGED"

    accounts = {}
    strategy = producer._strategy(owner)
    for level, tier in tiers.items():
        source_probe = Backtester(config)
        seed = source_probe.build_evidence_source_seed_result(
            frame, owner.symbol, spec=spec, timeframe=owner.timeframe,
            spread_tier=tier, entry_spread_observations=observations)
        assert seed["source_seed_has_strategy_or_position_evidence"] is False, (
            "CURRENT_RAW_STATIC_PROBE_CONTAINS_POSITION_EVIDENCE"
        )
        _, _, _, _, enabled, swap_contract = source_probe._build_causal_swap_inputs(
            frame, spec, producer.np.full(len(frame), producer.np.nan, dtype=producer.np.float64),
            timeframe=owner.timeframe)
        assert producer._raw_json(swap_contract) == seed["swap_accrual_contract"], (
            "CURRENT_RAW_STATIC_FINANCING_CONTRACT_CHANGED"
        )
        tape, valuation_spec = producer._open_m5_valuation_tape(
            context=context, product=products["M5"],
            expected_identity=precommit["valuation_m5_products"][0],
            config=config, deployment=deployment, forward_start=start,
            forward_end_exclusive=end, spread_tier=tier)
        assert asdict(valuation_spec) == asdict(spec), "CURRENT_RAW_VALUATION_TERMS_CHANGED"
        accounts[level] = {
            "symbol": owner.symbol, "timeframe": owner.timeframe, "cell_id": owner.cell_id,
            "tier": tier.descriptor(), "initial_cash": float(config.initial_capital),
            "warmup_bars": warmup, "instrument_spec": asdict(spec),
            "position_namespace": "|".join((owner.symbol, owner.timeframe,
                                            str(getattr(strategy, "name", type(strategy).__name__)))),
            "trades": None, "position_events": None, "engine_equity": None,
            "engine_equity_growth": None, "engine_spread_admission": None,
            "engine_spread_model": seed["spread_model"],
            "execution_cost_timing_contract": seed["execution_cost_timing_contract"],
            "numerical_execution_cost_attestation": seed["numerical_execution_cost_attestation"],
            "financing_model": {
                "pre_open_units": None, "close_units": None,
                "long_per_lot": None, "short_per_lot": None,
                "enabled": bool(enabled), "contract": producer._raw_json(swap_contract),
            },
            "derived": {
                "positions": None, "transactions": None, "cash_inventory": None,
                "terminal_inventory": [], "terminal_simulation": "END_OF_DATA_LIQUIDATED",
                "reconciliation": "EXACT_FLOAT64_AT_EVERY_AVAILABLE_ENGINE_CLOSE",
            },
            "valuation_source": {
                "native_product_identity": dict(tape.native_product_identity),
                "execution_spec_receipt": dict(tape.execution_spec_receipt),
                "spread_and_cost_surface": dict(tape.spread_meta),
            },
        }
        for dynamic_path in _CURRENT_RAW_DYNAMIC_PATHS:
            value = accounts[level]
            for key in dynamic_path.split("/"):
                value = value[key]
            assert value is None, "CURRENT_RAW_DYNAMIC_FORMAT_PLACEHOLDER_CHANGED"
    body_template = {
        "schema_version": producer.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
        "generation_id": context.generation_id, "anchor": copy.deepcopy(anchor),
        "capital_phase": producer.CAPITAL_OUTER_EVALUATION,
        "capital_window": copy.deepcopy(outer_window), "genesis": copy.deepcopy(genesis),
        "full_book_descriptor": full_book_descriptor, "original_precommit_anchor": precommit,
        "cell_projection": projection, "owner_provenance": producer._owner_provenance(owner),
        "owner_source": {"symbol_receipt": receipt,
                         "adapter_index_descriptor": copy.deepcopy(result["adapter_index_descriptor"])},
        "original_global_precommit_descriptor": global_descriptor,
        "pipeline_config_binding": {
            "active_config_path": str(frozen.active_config_path),
            "config_sha256": frozen.config_sha256,
            "effective_config_sha256": frozen.effective_config_sha256,
        },
        "deployment_source_descriptors": deployment_descriptors,
        "execution_source": precommit["execution_products"][0],
        "execution_spec_receipt": execution_spec_receipt,
        "frozen_profile_descriptor": profile_descriptor, "spread_source": fine_identity,
        "spread_source_execution_spec_receipt": fine_spec_receipt,
        "accounts": accounts, "qualification_authority": False,
        "precommit_binding_scope": "REOPENED_INPUT_DESCRIPTOR_IDENTITY_NOT_ISSUED_RELEASE_AUTHORITY",
        "release_authority_status": "UNAVAILABLE_AUTHORITY_MIGRATION_NOT_IMPLEMENTED",
        "coverage_contract": "ACTUAL_CLOSE_AND_AVAILABLE_M5_MARKS_NOT_COMPLETE_DAILY_VALUATION",
        "selection_policy": "P75_REQUIRED_P95_PREFERRED_AMONG_P75_PASSES",
    }
    source_files = ("pm_forward_oos_replay_producer.py", "pm_core.py", "pm_metrics.py",
                    "pm_trade_power.py", "pm_spread_profile.py")
    metadata = {
        "schema_version": "fxpm.raw-v3-capacity-metadata.v1",
        "provenance": "GENUINE_ENGINE_FREE_STATIC_METADATA_NOT_ENGINE_OUTPUT",
        "geometry": geometry,
        "clock_disclosure": {
            "selection_cutoff_utc": anchor["selection_cutoff_utc"],
            "profile_calibration_cutoff_utc": result["profile_calibration_cutoff_utc"],
            "source_acquisition_cutoff_utc": result["source_acquisition_cutoff_utc"],
            "native_prefix_accessed_at_utc": accessed_at,
            "profile_scope": result["profile_scope"],
        },
        "source_bindings": {name: _current_raw_file_identity(project / name) for name in source_files},
        "input_bindings": {str(path): _current_raw_file_identity(path)
                           for path in (result_path, config_path, axis_receipt)},
        "payload_template": body_template,
        "format_placeholders": {
            "null_means": "UNOBSERVED_ENGINE_OUTPUT_NOT_ZERO_OR_MISSING_MARK",
            "paths_per_account": list(_CURRENT_RAW_DYNAMIC_PATHS),
        },
    }
    assert _current_raw_file_identity(original_manifest) == manifest_identity, (
        "CURRENT_RAW_ORIGINAL_MANIFEST_CHANGED_DURING_PREPARATION"
    )
    assert _current_raw_file_identity(result_path) == {
        "sha256": _CURRENT_RAW_002_RESULT_SHA256, "bytes": _CURRENT_RAW_002_RESULT_BYTES,
    }, "CURRENT_RAW_ORIGINAL_RESULT_CHANGED_DURING_PREPARATION"
    return {
        "execute_arguments": arguments,
        "execute_arguments_by_phase": arguments_by_phase,
        "capacity_metadata": metadata,
        "original_result_path": result_path,
        "cache_owner_root": scratch,
    }


def _current_raw_transport_session(
        tmp_path_factory, pytestconfig, *, metadata_path=None, spool_root=None):
    """Share one preparation or explicit disk continuation and one later replay."""
    assert (metadata_path is None) == (spool_root is None), (
        "CURRENT_RAW_DISK_CONTINUATION_PAIR_REQUIRED"
    )
    disk_source = (None if metadata_path is None else (
        str(Path(metadata_path).resolve(strict=True)), str(Path(spool_root).resolve(strict=True))))
    attribute = "_fxpm_current_raw_transport"
    if not hasattr(pytestconfig, attribute):
        if disk_source is None:
            prepared = _prepare_current_raw_inputs(tmp_path_factory.mktemp("current-raw-inputs"))
        else:
            project = Path(__file__).resolve().parents[1]
            source = Path(disk_source[1])
            scratch = Path(tmp_path_factory.mktemp("current-raw-disk-inputs")).resolve(strict=True)
            assert scratch.is_relative_to(project), "CURRENT_RAW_SESSION_CLONE_OUTSIDE_WORKSPACE"
            assert not scratch.is_relative_to(source) and not source.is_relative_to(scratch), (
                "CURRENT_RAW_SESSION_CLONE_OVERLAPS_SAVED_INPUT"
            )
            destination = scratch / "spool"
            assert not destination.exists() and not destination.is_symlink(), (
                "CURRENT_RAW_SESSION_CLONE_SPOOL_ALREADY_EXISTS"
            )
            shutil.copytree(source, destination)
            prepared = _reopen_current_raw_inputs(disk_source[0], destination)
        setattr(pytestconfig, attribute, {
            "preparation_version": "QA113002_DESCRIPTOR_V3",
            "input_source": disk_source,
            "prepared": prepared,
            "transport": None,
        })
    state = getattr(pytestconfig, attribute)
    assert state["preparation_version"] == "QA113002_DESCRIPTOR_V3", (
        "CURRENT_RAW_SESSION_CACHE_VERSION_CHANGED"
    )
    assert state["input_source"] == disk_source, "CURRENT_RAW_SESSION_INPUT_SOURCE_CHANGED"
    return state


def _clone_current_raw_transport_for_tamper(state, tmp_path):
    """Clone a completed cached transport; never modify its retained/canonical spool."""
    assert state["transport"] is not None, "CURRENT_RAW_TRANSPORT_NOT_EXECUTED"
    context, anchor, _, artifact, original_arguments = state["transport"]
    scratch = Path(tmp_path).resolve()
    project = Path(__file__).resolve().parents[1]
    canonical = Path(context.root).resolve(strict=True)
    assert scratch.is_relative_to(project), "CURRENT_RAW_TAMPER_SCRATCH_OUTSIDE_WORKSPACE"
    assert not scratch.is_relative_to(canonical) and not canonical.is_relative_to(scratch), (
        "CURRENT_RAW_TAMPER_SCRATCH_OVERLAPS_CANONICAL"
    )
    destination = scratch / "spool"
    assert not destination.exists(), "CURRENT_RAW_TAMPER_SPOOL_ALREADY_EXISTS"
    scratch.mkdir(parents=True, exist_ok=True)
    shutil.copytree(canonical, destination)
    cloned = authority_spool.reopen_generation_context(
        destination, expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256)
    body = producer._read_descriptor_json(cloned, artifact.descriptor, code="CURRENT_RAW_TAMPER_BASE")
    full_book = producer._read_descriptor_json(
        cloned, body["full_book_descriptor"], code="CURRENT_RAW_TAMPER_BOOK")
    owner_source = body["owner_source"]
    owner = producer._reopen_owner_capture(
        context=cloned, anchor=anchor, book_body=full_book,
        cell_id=body["cell_projection"]["cell_id"],
        symbol_receipt=owner_source["symbol_receipt"],
        adapter_index_descriptor=owner_source["adapter_index_descriptor"])
    frozen = original_arguments["frozen_pipeline_config"]
    config = frozen.materialize(cloned)
    global_body = producer._read_descriptor_json(
        cloned, body["original_global_precommit_descriptor"], code="CURRENT_RAW_TAMPER_GLOBAL")
    _, deployment, _ = producer._reopen_retained_deployment(
        context=cloned, descriptors=body["deployment_source_descriptors"],
        original_receipt=global_body["deployment_binding"], config_record=body["pipeline_config_binding"])
    _, execution = causal_producer._reopen_frozen_transport_source(cloned, body["execution_source"], config)
    _, fine = causal_producer._reopen_frozen_transport_source(cloned, body["spread_source"], config)
    arguments = {
        **original_arguments, "context": cloned, "anchor": copy.deepcopy(anchor),
        "specialists": [owner], "config": config, "deployment": deployment,
        "native_products": {f"{owner.symbol}|{owner.timeframe}": execution},
        "valuation_products": {f"{owner.symbol}|M5": fine},
        "spread_source_products": {f"{owner.symbol}|M5": fine},
        "frozen_profile_path": cloned.root / body["frozen_profile_descriptor"]["relative_path"],
        "full_book_body": full_book,
    }
    return cloned, copy.deepcopy(anchor), body, causal_producer.FrozenTierRawAccountsArtifact(
        copy.deepcopy(artifact.descriptor)), arguments


def _reopen_current_raw_inputs(metadata_path, spool_root):
    """Continue owned preparation; caller first verifies its retained metadata digest."""
    import pm_production_config_binding as binding

    project = Path(__file__).resolve().parents[1]
    audit = project / "audit_reports/fxpm_workshop_20261007"
    metadata_path = Path(metadata_path).resolve(strict=True)
    spool_root = Path(spool_root).resolve(strict=True)
    assert metadata_path.is_relative_to(project) and spool_root.is_relative_to(project), (
        "CURRENT_RAW_DISK_CONTINUATION_OUTSIDE_WORKSPACE"
    )
    metadata_identity = _current_raw_file_identity(metadata_path)
    metadata = json.loads(metadata_path.read_bytes())
    assert _current_raw_file_identity(metadata_path) == metadata_identity, (
        "CURRENT_RAW_METADATA_CHANGED_DURING_READ"
    )
    assert metadata["schema_version"] == "fxpm.raw-v3-capacity-metadata.v1", (
        "CURRENT_RAW_METADATA_SCHEMA_CHANGED"
    )
    assert metadata["provenance"] == "GENUINE_ENGINE_FREE_STATIC_METADATA_NOT_ENGINE_OUTPUT", (
        "CURRENT_RAW_METADATA_PROVENANCE_CHANGED"
    )
    source_files = ("pm_forward_oos_replay_producer.py", "pm_core.py", "pm_metrics.py",
                    "pm_trade_power.py", "pm_spread_profile.py")
    assert metadata["source_bindings"] == {
        name: _current_raw_file_identity(project / name) for name in source_files
    }, "CURRENT_RAW_METADATA_SOURCE_BINDINGS_CHANGED"
    result_path = (audit / _CURRENT_RAW_002_RESULT_RELATIVE).resolve(strict=True)
    config_path = (audit / "selected_owner_bound_context_preflight/runtime/attempt_001_pytest_tmp"
                   / "test_bound_midnight_native_inp0/config.json").resolve(strict=True)
    axis_receipt = (audit / "current_owner_raw_capacity_preflight/results/attempt_002"
                    / "capacity-result.json").resolve(strict=True)
    assert metadata["input_bindings"] == {
        str(path): _current_raw_file_identity(path) for path in (result_path, config_path, axis_receipt)
    }, "CURRENT_RAW_METADATA_INPUT_BINDINGS_CHANGED"
    assert metadata["input_bindings"][str(result_path)] == {
        "sha256": _CURRENT_RAW_002_RESULT_SHA256, "bytes": _CURRENT_RAW_002_RESULT_BYTES,
    }, "CURRENT_RAW_002_RESULT_CHANGED"
    result = json.loads(result_path.read_bytes())
    assert result["status"] == "PASS" and result["owner_reopen"]["status"] == "VERIFIED", (
        "CURRENT_RAW_002_NOT_VERIFIED"
    )
    protected = (
        Path(result["spool_root"]).resolve(strict=True),
        Path(result["original_spool_root"]).resolve(strict=True),
        result_path.parent, config_path.parent, axis_receipt.parent,
    )
    assert all(not spool_root.is_relative_to(path) and not path.is_relative_to(spool_root)
               for path in protected), "CURRENT_RAW_DISK_SPOOL_OVERLAPS_RETAINED_INPUT"

    body = metadata["payload_template"]
    assert set(body) == {
        "schema_version", "generation_id", "anchor", "capital_phase", "capital_window", "genesis",
        "full_book_descriptor", "original_precommit_anchor", "cell_projection", "owner_provenance",
        "owner_source", "original_global_precommit_descriptor", "pipeline_config_binding",
        "deployment_source_descriptors", "execution_source", "execution_spec_receipt",
        "frozen_profile_descriptor", "spread_source", "spread_source_execution_spec_receipt", "accounts",
        "qualification_authority", "precommit_binding_scope", "release_authority_status",
        "coverage_contract", "selection_policy",
    }, "CURRENT_RAW_METADATA_BODY_FIELDS_CHANGED"
    assert body["schema_version"] == producer.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA, (
        "CURRENT_RAW_METADATA_RAW_SCHEMA_CHANGED"
    )
    assert (
        body["qualification_authority"] is False
        and body["precommit_binding_scope"] == "REOPENED_INPUT_DESCRIPTOR_IDENTITY_NOT_ISSUED_RELEASE_AUTHORITY"
        and body["release_authority_status"] == "UNAVAILABLE_AUTHORITY_MIGRATION_NOT_IMPLEMENTED"
        and body["coverage_contract"] == "ACTUAL_CLOSE_AND_AVAILABLE_M5_MARKS_NOT_COMPLETE_DAILY_VALUATION"
        and body["selection_policy"] == "P75_REQUIRED_P95_PREFERRED_AMONG_P75_PASSES"
    ), "CURRENT_RAW_METADATA_AUTHORITY_CHANGED"
    receipt = result["symbol_receipt"]
    assert body["owner_source"] == {
        "symbol_receipt": receipt, "adapter_index_descriptor": result["adapter_index_descriptor"],
    }, "CURRENT_RAW_METADATA_OWNER_CARRIER_CHANGED"
    assert body["generation_id"] == receipt["generation_id"], "CURRENT_RAW_METADATA_GENERATION_CHANGED"
    assert body["anchor"] == result["adapter_anchor"], "CURRENT_RAW_METADATA_ANCHOR_CHANGED"
    context = authority_spool.reopen_generation_context(
        spool_root, expected_generation_id=receipt["generation_id"],
        expected_native_universe_sha256=receipt["native_universe_sha256"],
        expected_config_sha256=receipt["config_sha256"])
    assert context.context_sha256 == receipt["generation_context_sha256"], (
        "CURRENT_RAW_METADATA_CONTEXT_CHANGED"
    )
    producer._verified_manifest(context, receipt, receipt["symbol"])
    original_global = producer._read_descriptor_json(
        context, body["original_global_precommit_descriptor"], code="CURRENT_RAW_RESUME_GLOBAL")
    retained_config, deployment, deployment_bytes = producer._reopen_retained_deployment(
        context=context, descriptors=body["deployment_source_descriptors"],
        original_receipt=original_global["deployment_binding"],
        config_record=body["pipeline_config_binding"])
    assert Path(body["pipeline_config_binding"]["active_config_path"]).resolve(strict=True) == config_path, (
        "CURRENT_RAW_RESUME_109_CONFIG_PATH_CHANGED"
    )
    payload, loaded_config = binding.load_effective_pipeline_config(config_path)
    frozen = binding.freeze_production_pipeline_config(
        context, active_config_path=config_path, config_payload=payload, supplied_config=loaded_config)
    config = frozen.materialize(context)
    assert frozen.config_sha256 == _CURRENT_RAW_CONFIG_SHA256, "CURRENT_RAW_RESUME_CONFIG_CHANGED"
    assert frozen.effective_config_sha256 == _CURRENT_RAW_EFFECTIVE_SHA256, (
        "CURRENT_RAW_RESUME_EFFECTIVE_CONFIG_CHANGED"
    )
    assert producer._pipeline_config_identity(config) == producer._pipeline_config_identity(retained_config), (
        "CURRENT_RAW_RESUME_RETAINED_CONFIG_CHANGED"
    )
    producer._assert_deployment_config_unchanged(deployment, config)
    native_root = (Path(config.data_dir) / "native").resolve(strict=True)
    assert not spool_root.is_relative_to(native_root) and not native_root.is_relative_to(spool_root), (
        "CURRENT_RAW_DISK_SPOOL_OVERLAPS_NATIVE_INPUT"
    )
    precommit = body["original_precommit_anchor"]
    producer._reopen_original_global_precommit(
        context=context, descriptor=body["original_global_precommit_descriptor"],
        precommit_anchor=precommit, deployment=deployment)
    full_book = producer._read_descriptor_json(
        context, body["full_book_descriptor"], code="CURRENT_RAW_RESUME_FULL_BOOK")
    full_book = producer._owned_book(full_book, result["book_decision"]["receipt_sha256"])
    assert full_book == result["book_decision"], "CURRENT_RAW_RESUME_FULL_BOOK_CHANGED"
    assert (
        precommit["full_book_descriptor"] == body["full_book_descriptor"]
        and precommit["full_book_sha256"] == body["full_book_descriptor"]["sha256"]
    ), "CURRENT_RAW_RESUME_BOOK_DESCRIPTOR_CHANGED"
    owner = producer._reopen_owner_capture(
        context=context, anchor=body["anchor"], book_body=full_book,
        cell_id=result["owner_reopen"]["cell_id"], symbol_receipt=receipt,
        adapter_index_descriptor=result["adapter_index_descriptor"])
    assert body["cell_projection"] == {
        "cell_id": owner.cell_id, "process_sha256": owner.process_sha256,
    }, "CURRENT_RAW_RESUME_CELL_PROJECTION_CHANGED"
    assert body["owner_provenance"] == producer._owner_provenance(owner), (
        "CURRENT_RAW_RESUME_OWNER_PROVENANCE_CHANGED"
    )
    assert body["genesis"] == {
        "mode": producer.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": float(config.initial_capital),
        "initial_inventory": [], "owner_changes": [],
    }, "CURRENT_RAW_RESUME_GENESIS_CHANGED"
    anchor = body["anchor"]
    windows = producer._capital_phase_windows([anchor], {"anchor_plan": [{
        "anchor_id": anchor["anchor_id"],
        "inner_window": {"start_date": "2023-07-15", "end_date": "2024-07-14"},
        "outer_window": {"start_date": "2024-07-15", "end_date": "2024-10-12"},
    }]})
    assert precommit["capital_phase_windows"] == windows[anchor["anchor_id"]], (
        "CURRENT_RAW_RESUME_PHASE_WINDOWS_CHANGED"
    )
    assert body["capital_phase"] == producer.CAPITAL_OUTER_EVALUATION, (
        "CURRENT_RAW_RESUME_CAPACITY_PHASE_CHANGED"
    )
    assert body["capital_window"] == windows[anchor["anchor_id"]][producer.CAPITAL_OUTER_EVALUATION], (
        "CURRENT_RAW_RESUME_CAPACITY_WINDOW_CHANGED"
    )
    accounts = body["accounts"]
    assert set(accounts) == {"P75", "P95"}, "CURRENT_RAW_RESUME_ACCOUNT_ROSTER_CHANGED"
    account_fields = {
        "symbol", "timeframe", "cell_id", "tier", "initial_cash", "warmup_bars", "instrument_spec",
        "position_namespace", "trades", "position_events", "engine_equity_growth",
        "engine_spread_admission", "engine_spread_model", "execution_cost_timing_contract",
        "numerical_execution_cost_attestation", "financing_model", "derived", "valuation_source",
        "engine_equity",
    }
    for level, account in accounts.items():
        assert set(account) == account_fields, "CURRENT_RAW_RESUME_ACCOUNT_FIELDS_CHANGED"
        assert (
            account["symbol"] == owner.symbol and account["timeframe"] == owner.timeframe
            and account["cell_id"] == owner.cell_id and account["tier"]["tier"] == level
            and account["tier"]["qualification_authority"] is False
            and account["initial_cash"] == float(config.initial_capital)
        ), "CURRENT_RAW_RESUME_ACCOUNT_IDENTITY_CHANGED"
        for dynamic_path in _CURRENT_RAW_DYNAMIC_PATHS:
            value = account
            for key in dynamic_path.split("/"):
                value = value[key]
            assert value is None, "CURRENT_RAW_RESUME_ENGINE_OUTPUT_NOT_FORMAT_NULL"
    execution_frame, execution = causal_producer._reopen_frozen_transport_source(
        context, body["execution_source"], config)
    fine_frame, fine = causal_producer._reopen_frozen_transport_source(
        context, body["spread_source"], config)
    valuation_identity = accounts["P75"]["valuation_source"]["native_product_identity"]
    assert accounts["P95"]["valuation_source"]["native_product_identity"] == valuation_identity, (
        "CURRENT_RAW_RESUME_VALUATION_PAIR_CHANGED"
    )
    assert producer._native_product_identity(
        fine, expected_symbol=owner.symbol, expected_timeframe="M5",
        expected_cutoff_utc="2024-10-12T00:00:00Z", role=valuation_identity["role"]
    ) == valuation_identity, "CURRENT_RAW_RESUME_VALUATION_SOURCE_CHANGED"
    for field, digest_field, expected in (
        ("execution_products", "execution_product_roster_sha256", [body["execution_source"]]),
        ("spread_source_products", "spread_source_product_roster_sha256", [body["spread_source"]]),
        ("valuation_m5_products", "valuation_m5_product_roster_sha256", [valuation_identity]),
    ):
        assert precommit[field] == expected and precommit[digest_field] == producer._sha(expected), (
            "CURRENT_RAW_RESUME_PRODUCT_ROSTER_CHANGED"
        )
    _, spec_receipt = producer._derive_retained_execution_spec(
        product=execution, deployment=deployment, source_bytes=deployment_bytes)
    _, fine_spec_receipt = producer._derive_retained_execution_spec(
        product=fine, deployment=deployment, source_bytes=deployment_bytes)
    assert spec_receipt == body["execution_spec_receipt"], "CURRENT_RAW_RESUME_EXECUTION_SPEC_CHANGED"
    assert fine_spec_receipt == body["spread_source_execution_spec_receipt"], (
        "CURRENT_RAW_RESUME_FINE_SPEC_CHANGED"
    )
    for account in accounts.values():
        assert account["instrument_spec"] == spec_receipt["instrument_spec"], (
            "CURRENT_RAW_RESUME_ACCOUNT_SPEC_CHANGED"
        )
        assert account["valuation_source"]["execution_spec_receipt"] == fine_spec_receipt, (
            "CURRENT_RAW_RESUME_VALUATION_SPEC_CHANGED"
        )
    profile_descriptor, _ = native_prefix._read_spool_descriptor(
        context, body["frozen_profile_descriptor"], expected_format="json-profile", collect=False)
    target = next(cell for cell in full_book["cells"] if cell["cell_id"] == owner.cell_id)
    pair = target["validated_cost_ladder_tolerance"]["frozen_tier_pair_contract"]
    assert {item["frozen_spread_tier"]["profile_sha256"] for item in pair["tiers"]} == {
        profile_descriptor["sha256"]
    }, "CURRENT_RAW_RESUME_PROFILE_DESCRIPTOR_CHANGED"
    start, end = producer._capital_window_datetimes(
        body["capital_window"], phase_id=producer._capital_phase_id(
            anchor["anchor_id"], producer.CAPITAL_OUTER_EVALUATION))
    _, closes = producer._frame_interval(execution_frame)
    retained = execution_frame.loc[closes <= end]
    producer._selection_prefix_match(retained, owner)
    warmup = int(retained.index.searchsorted(start, side="left"))
    assert metadata["geometry"] == {
        "native_axis_rows": len(retained), "warmup_bars": warmup,
        "scored_bars": len(retained) - warmup, "fine_native_rows": len(fine_frame),
        "calendar_days": (end - start).days, "governance_policy": None,
    } == {
        "native_axis_rows": 34908, "warmup_bars": 32772, "scored_bars": 2136,
        "fine_native_rows": 417651, "calendar_days": 89, "governance_policy": None,
    }, "CURRENT_RAW_RESUME_GEOMETRY_CHANGED"
    assert all(account["warmup_bars"] == warmup for account in accounts.values()), (
        "CURRENT_RAW_RESUME_ACCOUNT_WARMUP_CHANGED"
    )
    arguments = dict(
        context=context, anchor=anchor, specialists=[owner],
        native_products={f"{owner.symbol}|H1": execution},
        valuation_products={f"{owner.symbol}|M5": fine}, precommit_anchor=precommit,
        capital_phase=body["capital_phase"], capital_window=body["capital_window"],
        config=config, deployment=deployment, account_genesis=body["genesis"],
        cell_projection=body["cell_projection"],
        spread_source_products={f"{owner.symbol}|M5": fine},
        frozen_profile_path=context.root / profile_descriptor["relative_path"],
        full_book_body=full_book, frozen_pipeline_config=frozen,
        owner_symbol_receipt=receipt,
        original_adapter_index_descriptor=result["adapter_index_descriptor"],
        original_global_precommit_descriptor=body["original_global_precommit_descriptor"],
    )
    arguments_by_phase = {
        phase: {**arguments, "capital_phase": phase, "capital_window": copy.deepcopy(window)}
        for phase, window in windows[anchor["anchor_id"]].items()
    }
    assert _current_raw_file_identity(metadata_path) == metadata_identity, (
        "CURRENT_RAW_METADATA_CHANGED_DURING_REOPEN"
    )
    return {
        "execute_arguments": arguments, "execute_arguments_by_phase": arguments_by_phase,
        "capacity_metadata": metadata, "original_result_path": result_path,
        "cache_owner_root": spool_root.parent,
        "metadata_path": metadata_path, "metadata_identity": metadata_identity,
    }


def test_synthetic_current_raw_disk_session_clones_once_and_owns_writes(tmp_path, monkeypatch):
    """Invented tiny files exercise cache custody only, never native or engine data."""
    project = tmp_path / "synthetic-current-raw-workspace"
    saved = project / "retained" / "spool"
    saved.mkdir(parents=True)
    original = saved / "synthetic-input.txt"
    original.write_text("SYNTHETIC_CACHE_OWNERSHIP_INPUT", encoding="utf-8")
    metadata = saved.parent / "synthetic-metadata.json"
    metadata.write_text('{"provenance":"SYNTHETIC_CACHE_OWNERSHIP_UNIT_ONLY"}', encoding="utf-8")
    owned = project / "owned-session"
    factory_calls, reopen_calls = [], []

    def make_owned(name):
        factory_calls.append(name)
        owned.mkdir()
        return owned

    def reopen_owned(metadata_path, spool_root):
        reopen_calls.append((Path(metadata_path), Path(spool_root)))
        assert Path(metadata_path) == metadata.resolve()
        assert Path(spool_root) == owned / "spool"
        assert (Path(spool_root) / original.name).read_bytes() == original.read_bytes()
        return {"cache_owner_root": Path(spool_root).parent, "synthetic_preparation": True}

    monkeypatch.setitem(globals(), "__file__", str(project / "tests" / "synthetic_fixture.py"))
    monkeypatch.setitem(globals(), "_reopen_current_raw_inputs", reopen_owned)
    factory = SimpleNamespace(mktemp=make_owned)
    pytestconfig = SimpleNamespace()
    state = _current_raw_transport_session(
        factory, pytestconfig, metadata_path=metadata, spool_root=saved)
    assert _current_raw_transport_session(
        factory, pytestconfig, metadata_path=metadata, spool_root=saved) is state
    assert factory_calls == ["current-raw-disk-inputs"]
    assert reopen_calls == [(metadata.resolve(), owned / "spool")]
    assert state["input_source"] == (str(metadata.resolve()), str(saved.resolve()))
    assert state["prepared"]["cache_owner_root"] == owned
    assert state["transport"] is None
    (owned / "spool" / original.name).write_text("SYNTHETIC_OWNED_WRITE", encoding="utf-8")
    assert original.read_text(encoding="utf-8") == "SYNTHETIC_CACHE_OWNERSHIP_INPUT"


@pytest.mark.parametrize("destination_kind,expected", [
    ("outside_workspace", "CURRENT_RAW_SESSION_CLONE_OUTSIDE_WORKSPACE"),
    ("inside_saved_spool", "CURRENT_RAW_SESSION_CLONE_OVERLAPS_SAVED_INPUT"),
    ("contains_saved_spool", "CURRENT_RAW_SESSION_CLONE_OVERLAPS_SAVED_INPUT"),
])
def test_synthetic_current_raw_disk_session_refuses_unsafe_clone_before_copy(
        tmp_path, monkeypatch, destination_kind, expected):
    """Synthetic destination refusals do not copy or reopen a retained spool."""
    project = tmp_path / "synthetic-current-raw-workspace"
    saved = project / "retained" / "spool"
    saved.mkdir(parents=True)
    (saved / "synthetic-input.txt").write_text("SYNTHETIC_INPUT", encoding="utf-8")
    metadata = saved.parent / "synthetic-metadata.json"
    metadata.write_text("{}", encoding="utf-8")
    destination = {
        "outside_workspace": tmp_path / "outside-workspace",
        "inside_saved_spool": saved / "nested-destination",
        "contains_saved_spool": project,
    }[destination_kind]
    destination.mkdir(parents=True, exist_ok=True)

    def forbidden(*args, **kwargs):
        pytest.fail("unsafe synthetic destination reached copying or reopening")

    monkeypatch.setitem(globals(), "__file__", str(project / "tests" / "synthetic_fixture.py"))
    monkeypatch.setitem(globals(), "_reopen_current_raw_inputs", forbidden)
    monkeypatch.setattr(shutil, "copytree", forbidden)
    pytestconfig = SimpleNamespace()
    with pytest.raises(AssertionError, match=expected):
        _current_raw_transport_session(
            SimpleNamespace(mktemp=lambda name: destination), pytestconfig,
            metadata_path=metadata, spool_root=saved)
    assert not hasattr(pytestconfig, "_fxpm_current_raw_transport")
