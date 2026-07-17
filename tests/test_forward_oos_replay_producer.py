from __future__ import annotations

import copy
from dataclasses import asdict, replace
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal_producer
import pm_causal_process as causal
import pm_forward_oos_replay_producer as producer
import pm_native_prefix_production as native_prefix
import pm_portfolio_replay as portfolio
from pm_core import Backtester, PipelineConfig
from tests import test_backtester_winner_daily_r as backtester_fixture
from tests import test_book_multiplicity as book_fixture
from tests import test_native_prefix_production as native_fixture


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


def _genuine_position_evidence(
    *, use_slippage: bool = False, return_tape: bool = False
):
    frame = backtester_fixture._frame().assign(
        SpreadOpen=[1, 1, 1, 1, 1],
        SpreadMax=[5, 5, 5, 5, 5],
        SpreadP95=[4, 4, 4, 4, 4],
    )
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        use_slippage=use_slippage,
        slippage_pips=0.5,
        use_commission=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
        oos_evidence_execution_stress_mode="stress_p95",
        discovery_risk_per_trade_pct=1.0,
    )
    spec = backtester_fixture._spec(commission_per_lot=7.0)
    strategy = backtester_fixture._AtrHoldStrategy()
    signals = backtester_fixture._signals(frame)
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "AUDUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )
    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "AUDUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )
    valuation_index = pd.date_range(
        frame.index[0],
        frame["_BAR_CLOSE_TS"].iloc[-1] - pd.Timedelta(minutes=5),
        freq="5min",
        tz="UTC",
    )
    valuation_frame = pd.DataFrame(
        {
            "Open": np.linspace(1.0, 1.0031, len(valuation_index)),
            "High": np.linspace(1.0001, 1.0032, len(valuation_index)),
            "Low": np.linspace(0.9999, 1.0030, len(valuation_index)),
            "Close": np.linspace(1.0, 1.0031, len(valuation_index)),
            "Volume": np.full(len(valuation_index), 100.0),
            "Spread": np.full(len(valuation_index), 1.0),
            "SpreadOpen": np.full(len(valuation_index), 1.0),
            "SpreadMax": np.full(len(valuation_index), 5.0),
            "SpreadP95": np.full(len(valuation_index), 4.0),
            "RealVolume": np.full(len(valuation_index), 80.0),
            "_BAR_CLOSE_TS": valuation_index + pd.Timedelta(minutes=5),
        },
        index=valuation_index,
    )
    valuation_entry_half, _, valuation_spread_meta = (
        backtester._build_half_spread_arrays(valuation_frame, spec)
    )
    valuation_tape = producer._M5ValuationTape(
        symbol="AUDUSD",
        frame=valuation_frame,
        opens=valuation_index,
        entry_half_spread=valuation_entry_half,
        spread_meta=valuation_spread_meta,
        execution_spec_receipt={"instrument_spec": asdict(spec)},
        native_product_identity={"index_sha256": "1" * 64},
    )
    positions = producer._position_evidence(
        specialist=_specialist(),
        capital_phase=producer.CAPITAL_OUTER_EVALUATION,
        strategy=strategy,
        features=frame,
        spec=spec,
        config=config,
        base_result=base,
        stress_result=stress,
        valuation_tape=valuation_tape,
        resource_budget=producer._ResourceBudget(),
    )
    assert len(positions) == 1
    result = (frame, config, spec, strategy, base, stress, positions[0])
    return (*result, valuation_tape) if return_tape else result


def _anchor_for(position: producer._PositionEvidence) -> dict[str, object]:
    return {
        "anchor_id": "anchor-forward-oos-1",
        "selection_cutoff_utc": "2026-01-04T00:00:00Z",
        "forward_start_date": position.marks[0].date,
        "forward_end_date": position.marks[-1].date,
    }


def _spooled_ledgers(tmp_path, replay: producer._AnchorRawReplay):
    context = authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="forward-oos-ledger-test-v1",
        native_universe_sha256="d" * 64,
        config_sha256="e" * 64,
    )
    bodies = producer._ledger_bodies(replay, starting_balance=Decimal("10000"))
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


def test_genuine_backtester_position_spools_and_reopens_as_causal_activity(
    tmp_path,
) -> None:
    _, _, _, _, _, _, position = _genuine_position_evidence()
    replay = producer._AnchorRawReplay(
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

    activity, gross, base_cost, stress_cost = causal_producer._assemble_activity(
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
    assert len(gross) == len(base_cost) == len(stress_cost) == len(position.marks)
    assert sum(stress_cost) < sum(base_cost) < Decimal(0)


def test_portfolio_blocked_position_is_not_laundered_into_raw_fill(
    tmp_path,
) -> None:
    _, _, _, _, _, _, position = _genuine_position_evidence()
    replay = producer._AnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={},
    )
    ledgers, hashes = _spooled_ledgers(tmp_path, replay)

    activity, gross, base_cost, stress_cost = causal_producer._assemble_activity(
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
    assert gross == base_cost == stress_cost == [Decimal(0)] * len(position.marks)


def test_stress_position_roster_mismatch_fails_typed() -> None:
    frame, config, spec, strategy, base, stress, _ = _genuine_position_evidence()
    tampered = copy.deepcopy(stress)
    tampered["position_events"] = []

    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="BACKTEST_STRESS_POSITION_ROSTER_MISMATCH",
    ):
        producer._position_evidence(
            specialist=_specialist(),
            capital_phase=producer.CAPITAL_OUTER_EVALUATION,
            strategy=strategy,
            features=frame,
            spec=spec,
            config=config,
            base_result=base,
            stress_result=tampered,
            valuation_tape=producer._M5ValuationTape(
                symbol="AUDUSD",
                frame=pd.DataFrame(),
                opens=pd.DatetimeIndex([]),
                entry_half_spread=np.array([], dtype=float),
                spread_meta={},
                execution_spec_receipt={"instrument_spec": asdict(spec)},
                native_product_identity={},
            ),
            resource_budget=producer._ResourceBudget(),
        )


def test_position_events_v2_is_rebuilt_and_exact_compared() -> None:
    frame, config, spec, strategy, base, stress, _, tape = _genuine_position_evidence(
        return_tape=True
    )
    tampered = copy.deepcopy(base)
    tampered["position_events"][0]["pnl_dollars"] += 1.0
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="BACKTEST_POSITION_EVENTS_V2_RAW_REVERIFY_FAILED",
    ):
        producer._position_evidence(
            specialist=_specialist(),
            capital_phase=producer.CAPITAL_OUTER_EVALUATION,
            strategy=strategy,
            features=frame,
            spec=spec,
            config=config,
            base_result=tampered,
            stress_result=stress,
            valuation_tape=tape,
            resource_budget=producer._ResourceBudget(),
        )


def test_partial_position_fails_typed_until_raw_ledger_has_partial_quantity() -> None:
    frame, config, spec, strategy, _, _, _, tape = _genuine_position_evidence(
        return_tape=True
    )
    signals = backtester_fixture._signals(frame)
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "AUDUSD",
        strategy,
        spec=spec,
        timeframe="D1",
        governance_policy={
            "name": "partial_tp_runner",
            "partial_at_r": 0.1,
            "partial_close_pct": 33.0,
            "trail_atr_mult": 100.0,
            "release_tp_after_r": 100.0,
        },
    )
    # Frozen-path replay intentionally reconstructs the plain strategy path,
    # not a governance overlay.  An exact copy is sufficient for this negative
    # test because the producer must reject multi-leg raw-ledger geometry before
    # consuming either lane's economics.
    stress = copy.deepcopy(base)
    assert len(base["position_events"]) == 1 and len(base["trades"]) == 2
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="PARTIAL_OR_MULTI_LEG_POSITION_NOT_REPRESENTABLE",
    ):
        producer._position_evidence(
            specialist=_specialist(),
            capital_phase=producer.CAPITAL_OUTER_EVALUATION,
            strategy=strategy,
            features=frame,
            spec=spec,
            config=config,
            base_result=base,
            stress_result=stress,
            valuation_tape=tape,
            resource_budget=producer._ResourceBudget(),
        )


def test_conservative_margin_model_and_amount_are_candidate_roster_bound() -> None:
    _, _, _, _, _, _, position = _genuine_position_evidence()
    original = portfolio.portfolio_replay_candidate_roster_receipt([position.candidate])
    assert position.candidate.margin_model == producer.MARGIN_MODEL
    assert position.candidate.margin_per_lot == pytest.approx(
        float(position.base_entry_fill * position.contract_multiplier)
    )

    relabelled = portfolio.portfolio_replay_candidate_roster_receipt(
        [replace(position.candidate, margin_model="caller-leverage-500").normalized()]
    )
    reduced = portfolio.portfolio_replay_candidate_roster_receipt(
        [replace(position.candidate, margin_per_lot=1.0).normalized()]
    )
    assert relabelled["candidate_roster_sha256"] != original["candidate_roster_sha256"]
    assert reduced["candidate_roster_sha256"] != original["candidate_roster_sha256"]


def test_common_m5_open_marks_use_executable_bid_and_full_round_trip_commission() -> (
    None
):
    _, _, spec, _, _, _, position, tape = _genuine_position_evidence(return_tape=True)
    assert position.candidate.marks
    first = position.candidate.marks[0]
    assert first.time > position.candidate.entry_time
    index = tape.opens.get_loc(first.time.tz_localize("UTC"))
    mid = Decimal(str(tape.frame["Open"].iloc[index]))
    half = Decimal(str(tape.entry_half_spread[index]))
    executable_bid = mid - half
    expected = (
        executable_bid - position.base_entry_fill
    ) * position.contract_multiplier - position.base_commission_per_lot
    assert first.pnl_per_current_lot == pytest.approx(float(expected))
    assert position.marks[0].mid_price == Decimal(
        str(
            tape.frame["Open"].iloc[
                tape.opens.get_loc(pd.Timestamp(position.marks[0].marked_at_utc))
            ]
        )
    )
    policy = portfolio.PortfolioPolicy(
        allowed_promotion_statuses=("HISTORICAL_OOS_REPLAY",),
        max_open_risk_pct=10.0,
        max_same_symbol_open_risk_pct=10.0,
        max_factor_group_open_risk_pct=10.0,
        max_margin_utilization_pct=90.0,
    )
    simulation = portfolio.SynchronizedPortfolioSimulator(policy).run(
        [position.candidate]
    )
    assert simulation["summary"]["valuation_event_order"] == (
        "close_existing_then_mark_existing_then_admit"
    )


def test_stop_risk_includes_adverse_configured_stop_slippage() -> None:
    _, config, spec, _, _, _, position = _genuine_position_evidence(use_slippage=True)
    slippage = Decimal(str(spec.pips_to_price(config.slippage_pips)))
    adverse_stop_fill = position.stop_price - slippage
    expected = (
        abs(position.base_entry_fill - adverse_stop_fill) * position.contract_multiplier
        + position.base_commission_per_lot
    )
    assert position.candidate.stop_risk_per_lot == pytest.approx(float(expected))


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
        '{"AUDUSD":{"symbol":"AUDUSD","commission":7}}', encoding="utf-8"
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


def test_nonfirst_symbol_uses_local_adapter_timeframe_group_and_exact_book_copy() -> (
    None
):
    symbol = book.PINNED_SYMBOL_ORDER[17]
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
    sources = [{"source": index} for index in range(len(book.PINNED_SYMBOL_ORDER) * 6)]
    fits = [{"fit": index} for index in range(len(sources))]
    local_sources = [{"source": -1} for _ in book.TIMEFRAME_ORDER]
    local_fits = [{"fit": -1} for _ in book.TIMEFRAME_ORDER]
    local_ordinal = book.TIMEFRAME_ORDER.index(timeframe)
    local_sources[local_ordinal] = copy.deepcopy(sources[group_ordinal])
    local_fits[local_ordinal] = copy.deepcopy(fits[group_ordinal])
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


def test_winner_v11_request_and_decision_are_standalone_replayed() -> None:
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
    [("NOT_A_SYMBOL", "M5"), ("AUDUSD", "H2")],
)
def test_native_product_key_rejects_out_of_contract_cells(
    symbol: str, timeframe: str
) -> None:
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="NATIVE_PRODUCT_KEY_INVALID",
    ):
        producer.native_product_key(symbol, timeframe)


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


def test_public_precommit_spools_separate_execution_and_m5_rosters_without_opening(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    anchor_id = "anchor-forward-oos-1"
    plan = {
        "anchor_id": anchor_id,
        "selection_cutoff_utc": "2026-01-01T00:00:00Z",
        "forward_start_date": "2026-01-02",
        "forward_end_date": "2026-01-31",
        "purge_calendar_days": 1,
    }
    cells = [{} for _ in range(book.FAMILY_SIZE)]
    cells[0] = {
        "symbol": "EURUSD",
        "timeframe": "M5",
        "regime": "TREND",
        "historical_state": "WINNER",
    }
    run_body = {"schema_version": "identity-only-run"}
    historical_precommit = SimpleNamespace(
        receipt_sha256="a" * 64,
        to_dict=lambda: {"anchors": [plan]},
    )
    historical_run_receipt = SimpleNamespace(receipt_sha256="b" * 64)
    causal_files = [
        producer._source_file_identity("pm_portfolio_replay.py"),
        producer._source_file_identity("pm_causal_evidence_producer.py"),
        producer._source_file_identity("pm_forward_oos_replay_producer.py"),
    ]
    causal_component = {
        "schema": "fxpm.causal-component-source-roster.v1",
        "component_identity_key": "synchronized_replay_code_sha256",
        "files": causal_files,
    }
    causal_payload = {
        "process_definition": {
            "component_identities": {
                "synchronized_replay_code_sha256": producer._sha(causal_component)
            }
        }
    }
    monkeypatch.setattr(
        producer,
        "_reopen_books",
        lambda *_args: (
            run_body,
            [plan],
            {anchor_id: {"cells": cells}},
            [],
        ),
    )
    deadline = producer.datetime.now(producer.UTC) + producer.timedelta(hours=1)
    monkeypatch.setattr(
        producer,
        "_causal_precommit_geometry",
        lambda *_args: (
            causal_payload,
            producer.datetime.now(producer.UTC),
            deadline,
        ),
    )
    monkeypatch.setattr(
        native_prefix,
        "open_verified_prefix_frame",
        lambda *_args, **_kwargs: pytest.fail("precommit opened OOS frame"),
    )
    context = authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="forward-oos-precommit-test",
        native_universe_sha256="c" * 64,
        config_sha256="d" * 64,
    )
    config = _bound_native_config(tmp_path / "config", tmp_path / "data")
    product = _sealed_identity_only_product(
        symbol="EURUSD", timeframe="M5", cutoff_utc="2026-02-01T00:00:00Z"
    )
    capital_phase_binding = object()
    capital_binding_body = {
        "anchor_plan": [
            {
                "anchor_id": anchor_id,
                "inner_window": {
                    "start_date": "2025-01-01",
                    "end_date": "2026-01-01",
                },
                "outer_window": {
                    "start_date": "2026-01-02",
                    "end_date": "2026-02-01",
                },
            }
        ],
        "bootstrap_identity": {
            "process_precommitment_sha256": causal.canonical_sha256(causal_payload),
            "process_definition_sha256": causal_payload.get(
                "process_definition_sha256"
            ),
        },
        "bootstrap_identity_sha256": "1" * 64,
        "capital_precommitment_body_sha256": "2" * 64,
        "contract_lineage": {"test": "contract"},
        "margin_terms_sha256": "3" * 64,
        "broker_terms_sha256": "4" * 64,
    }
    monkeypatch.setattr(
        producer.capital_bootstrap,
        "verify_capital_forward_phase_binding",
        lambda observed_context, _observed_binding: capital_binding_body
        if observed_context is context
        else pytest.fail("capital phase binding was substituted"),
    )
    precommit = producer.precommit_forward_oos_replay_products(
        context=context,
        historical_precommit=historical_precommit,
        historical_run_receipt=historical_run_receipt,
        causal_precommit_payload=causal_payload,
        execution_products_by_anchor={anchor_id: {"EURUSD|M5": product}},
        valuation_products_by_anchor={anchor_id: {"EURUSD|M5": product}},
        capital_phase_binding=capital_phase_binding,
        config=config,
    )
    body = precommit.to_dict()
    row = body["anchor_product_identities"][0]
    assert row["valuation_products_supplied_separately"] is True
    assert row["execution_products"][0]["role"].startswith("EXECUTION:")
    assert row["valuation_m5_products"][0]["role"].startswith("VALUATION_M5:")
    assert precommit.receipt_sha256 == precommit.descriptor["sha256"]
    assert (
        body["confers_causal_current_refit_capital_publication_or_trading_authority"]
        is False
    )
    producer._verify_forward_replay_precommit(
        context=context,
        value=precommit,
        historical_precommit=historical_precommit,
        historical_run_receipt=historical_run_receipt,
        causal_precommit_payload=causal_payload,
        deployment=producer._deployment_config(config),
    )
    forged_body = dict(body)
    forged_body["created_at_utc"] = "2000-01-01T00:00:00.000000Z"
    forged_raw = producer._canonical_bytes(forged_body)
    forged_descriptor = producer._write_raw_ledger(
        context, forged_body, role="FORGED_FORWARD_PRECOMMIT"
    )
    forged = producer.ForwardOOSReplayPrecommit(
        forged_raw,
        forged_descriptor,
        precommit._issuance_tag,
    )
    with pytest.raises(
        producer.ForwardOOSReplayProductionError,
        match="FORWARD_REPLAY_PRECOMMIT_NOT_ISSUED",
    ):
        producer._verify_forward_replay_precommit(
            context=context,
            value=forged,
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run_receipt,
            causal_precommit_payload=causal_payload,
            deployment=producer._deployment_config(config),
        )


def test_seventh_producer_receipt_is_content_addressed_reopened_and_non_authorizing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = authority_spool.freeze_generation_context(
        tmp_path / "spool",
        generation_id="forward-oos-product-test",
        native_universe_sha256="e" * 64,
        config_sha256="f" * 64,
    )
    _, _, _, _, _, _, position = _genuine_position_evidence()
    replay = producer._AnchorRawReplay(
        anchor=_anchor_for(position),
        specialists=(_specialist(),),
        positions=(position,),
        accepted_volume_by_position_id={},
    )
    bodies = producer._ledger_bodies(replay, starting_balance=Decimal("10000"))
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
        manifest_descriptor, _ = producer._persist_capital_phase_evidence(
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
        "full_round_trip_commission_in_every_candidate_mark": True,
        "stop_risk_includes_adverse_configured_stop_slippage": True,
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


def test_genuine_public_d1_execution_and_separate_m5_valuation_products_reopen(
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

    m5_deployment = producer._deployment_config(m5_config)
    m5_start = pd.Timestamp(m5_product.index["prefix"]["prefix_first_open_utc"])
    m5_end = pd.Timestamp(m5_facts["cutoff"])
    tape, spec = producer._open_m5_valuation_tape(
        context=m5_context,
        product=m5_product,
        expected_identity=m5_identity,
        config=m5_config,
        deployment=m5_deployment,
        forward_start=m5_start,
        forward_end_exclusive=m5_end,
    )
    assert tape.symbol == native_fixture.SYMBOL
    assert tape.frame.index.equals(tape.opens)
    assert len(tape.entry_half_spread) == len(tape.frame) == 3
    assert spec.symbol == native_fixture.SYMBOL
    assert tape.spread_meta["valuation_price_field"] == "Open"
    assert tape.spread_meta["round_trip_commission_deducted_at_every_mark"] is True


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
