from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from pm_core import PipelineConfig, get_instrument_spec
from pm_main import (
    CHAMPION_EXECUTION_BOOK,
    EDGE_DEMO_EXECUTION_BOOK,
    DecisionThrottle,
    LiveTrader,
)
from pm_live_outcomes import LiveOutcomeLedger
from pm_model_lifecycle import (
    EDGE_CHALLENGER_ROLE,
    LOCKBOX_CHAMPION_ROLE,
    build_edge_challenger_lineage,
)
from pm_pipeline import PortfolioManager, RegimeConfig, SymbolConfig, _stable_hash
from pm_position import PositionConfig, TradeTagEncoder


def _edge_cell(candidate_id: str = "") -> RegimeConfig:
    lineage = build_edge_challenger_lineage(
        fit_start_utc="2021-01-01T00:00:00+00:00",
        fit_end_utc="2026-01-01T00:00:00+00:00",
        parent_champion={
            "book_role": LOCKBOX_CHAMPION_ROLE,
            "parameter_selection_cutoff_utc": "2024-01-01T00:00:00+00:00",
            "performance_evidenced_through_utc": "2026-01-01T00:00:00+00:00",
        },
    )
    parameters = {"lookback": 20}
    if not candidate_id:
        candidate_id = _stable_hash({
            "book_role": EDGE_CHALLENGER_ROLE,
            "symbol": "EURUSD",
            "timeframe": "H1",
            "regime": "TREND",
            "strategy_name": "DemoStrategy",
            "parameters": parameters,
            "fit_cutoff_utc": lineage["fit_cutoff_utc"],
        })
    return RegimeConfig(
        strategy_name="DemoStrategy",
        parameters=parameters,
        quality_score=0.75,
        train_metrics={"total_trades": 42, "profit_factor": 1.2},
        val_metrics={},
        holdout_metrics={},
        regime_train_trades=42,
        trained_at=datetime.now(),
        valid_until=None,
        artifact_meta={
            "edge_challenger": True,
            "funded_role_eligible": False,
            "candidate_id": candidate_id,
        },
        validation_evidence={
            "temporal_lineage": lineage,
            "edge_refit": {
                "candidate_id": candidate_id,
                "forward_evidence_status": "NOT_STARTED",
            },
        },
        promotion_status="PAPER_ONLY",
        promotion_reasons=["requires future forward evidence"],
    )


def _champion_cell() -> RegimeConfig:
    return RegimeConfig(
        strategy_name="ChampionStrategy",
        parameters={},
        quality_score=0.8,
        train_metrics={"total_trades": 100},
        val_metrics={
            "total_trades": 40,
            "profit_factor": 1.4,
            "total_return_pct": 10.0,
            "max_drawdown_pct": 5.0,
        },
        promotion_status="PROMOTED",
    )


def _symbol_book() -> SymbolConfig:
    edge = _edge_cell()
    return SymbolConfig(
        symbol="EURUSD",
        regime_configs={"H1": {"TREND": _champion_cell()}},
        is_validated=True,
        edge_challenger_book={
            "book_role": EDGE_CHALLENGER_ROLE,
            "generation_status": "COMPLETE",
            "paper_only": True,
            "funded_role_eligible": False,
            "regime_configs": {"H1": {"TREND": edge.to_dict()}},
        },
    )


def _candidate(cell: RegimeConfig):
    strategy = Mock()
    strategy.build_trade_intent.return_value = Mock()
    features = pd.DataFrame(
        {
            "Open": [1.0, 1.0, 1.0],
            "High": [1.1, 1.1, 1.1],
            "Low": [0.9, 0.9, 0.9],
            "Close": [1.0, 1.0, 1.0],
        },
        index=pd.date_range("2026-01-01", periods=3, freq="h"),
    )
    return {
        "strategy_name": cell.strategy_name,
        "timeframe": "H1",
        "regime": "TREND",
        "signal": 1,
        "selection_score": 0.75,
        "quality_score": 0.75,
        "freshness": 1.0,
        "regime_strength": 1.0,
        "features": features,
        "strategy": strategy,
        "regime_config": cell,
        "bar_time": str(features.index[-2]),
    }


def test_edge_selector_builds_a_paper_only_view_without_copying_champions():
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.ledger = SimpleNamespace(get_all_configs=lambda: {"EURUSD": _symbol_book()})

    selected = manager.get_edge_challenger_configs()

    assert set(selected) == {"EURUSD"}
    edge = selected["EURUSD"].regime_configs["H1"]["TREND"]
    assert edge.strategy_name == "DemoStrategy"
    assert edge.promotion_status == "PAPER_ONLY"
    assert edge.artifact_meta["candidate_id"]
    assert selected["EURUSD"].artifact_meta["funded_role_eligible"] is False
    assert selected["EURUSD"].regime_configs["H1"]["TREND"].strategy_name != (
        _symbol_book().regime_configs["H1"]["TREND"].strategy_name
    )


def test_edge_selector_rejects_tampered_candidate_identity():
    book = _symbol_book()
    book.edge_challenger_book["regime_configs"]["H1"]["TREND"]["artifact_meta"][
        "candidate_id"
    ] = "tampered"
    manager = PortfolioManager.__new__(PortfolioManager)
    manager.ledger = SimpleNamespace(get_all_configs=lambda: {"EURUSD": book})

    assert manager.get_edge_challenger_configs() == {}
    assert "candidate id" in manager._last_edge_challenger_rejections["EURUSD"]


def test_execution_book_selector_never_falls_back_across_books():
    champion = {"EURUSD": _symbol_book()}
    edge = {"EURUSD": SymbolConfig(symbol="EURUSD", regime_configs={"H1": {"TREND": _edge_cell()}})}
    manager = Mock()
    manager.get_live_eligible_configs.return_value = champion
    manager.get_edge_challenger_configs.return_value = edge

    trader = LiveTrader.__new__(LiveTrader)
    trader.pm = manager
    trader._is_edge_challenger_demo = False
    assert trader._get_live_configs() is champion
    manager.get_edge_challenger_configs.assert_not_called()

    trader._is_edge_challenger_demo = True
    assert trader._get_live_configs() is edge
    manager.get_edge_challenger_configs.assert_called_once()


def test_funded_selector_failure_never_falls_through_to_management_configs():
    manager = Mock()
    manager.get_live_eligible_configs.side_effect = TypeError("authorization verifier failed")
    manager.get_validated_configs.return_value = {"EURUSD": _symbol_book()}

    trader = LiveTrader.__new__(LiveTrader)
    trader.pm = manager
    trader.logger = Mock()
    trader._is_edge_challenger_demo = False

    assert trader._get_live_configs() == {}
    manager.get_validated_configs.assert_not_called()

    manager.get_live_eligible_configs.side_effect = None
    manager.get_live_eligible_configs.return_value = ["malformed"]
    assert trader._get_live_configs() == {}
    manager.get_validated_configs.assert_not_called()


def test_demo_edge_cell_is_executable_but_funded_mode_rejects_paper_only():
    edge = _edge_cell()
    trader = LiveTrader.__new__(LiveTrader)
    trader.enable_trading = True
    trader._is_edge_challenger_demo = True

    assert trader._promotion_status_is_entry_eligible("PAPER_ONLY")
    assert not trader._promotion_status_is_entry_eligible("PROMOTED")
    assert trader._regime_config_is_entry_eligible(
        edge,
        min_pf=1.0,
        min_return=5.0,
        max_dd=35.0,
        min_return_dd_ratio=1.0,
    )

    trader._is_edge_challenger_demo = False
    trader.pipeline_config = SimpleNamespace(live_allowed_promotion_statuses=["PROMOTED"])
    assert not trader._promotion_status_is_entry_eligible("PAPER_ONLY")
    assert trader._promotion_status_is_entry_eligible("PROMOTED")


def test_edge_process_reaches_order_path_only_with_demo_account(tmp_path):
    mt5 = Mock()
    mt5.is_connected.return_value = False
    mt5.find_broker_symbol.return_value = "EURUSD"
    mt5.get_symbol_info.return_value = Mock(
        to_instrument_spec=Mock(return_value=get_instrument_spec("EURUSD"))
    )
    config = SymbolConfig(
        symbol="EURUSD",
        regime_configs={"H1": {"TREND": _edge_cell()}},
        edge_challenger_book={"book_role": EDGE_CHALLENGER_ROLE, "paper_only": True},
    )
    pipeline = SimpleNamespace(
        output_dir=tmp_path,
        allow_d1_plus_lower_tf=False,
        event_calendar_enabled=False,
        local_governance_live_mode="off",
        institutional_supervisor_mode="off",
    )
    portfolio_manager = Mock(symbols=["EURUSD"])
    trader = LiveTrader(
        mt5_connector=mt5,
        portfolio_manager=portfolio_manager,
        position_config=PositionConfig(),
        enable_trading=True,
        pipeline_config=pipeline,
        execution_book=EDGE_DEMO_EXECUTION_BOOK,
    )
    trader._decision_throttle = DecisionThrottle(log_path=str(tmp_path / "edge-decisions.json"))
    candidate = _candidate(config.regime_configs["H1"]["TREND"])
    trader._evaluate_regime_candidates = Mock(
        return_value=([candidate], {"timeframes_evaluated": 1, "timeframes_total": 1})
    )
    trader._execute_entry = Mock()

    real_account = SimpleNamespace(trade_mode=2, trade_allowed=True, trade_expert=True)
    trader._process_symbol("EURUSD", config, positions_snapshot=[], account_info=real_account)
    trader._execute_entry.assert_not_called()

    demo_account = SimpleNamespace(trade_mode=0, trade_allowed=True, trade_expert=True)
    trader._process_symbol("EURUSD", config, positions_snapshot=[], account_info=demo_account)
    trader._execute_entry.assert_called_once()
    call = trader._execute_entry.call_args.kwargs
    assert call["best_candidate"]["regime_config"].promotion_status == "PAPER_ONLY"
    assert call["magic"] == trader._encode_specialist_magic("EURUSD", "H1", "TREND")
    assert call["magic"] != TradeTagEncoder.encode_magic("EURUSD", "H1", "TREND")


def test_fresh_account_gate_rejects_real_contest_unknown_and_accepts_demo():
    trader = LiveTrader.__new__(LiveTrader)
    trader._is_edge_challenger_demo = True
    trader.pipeline_config = SimpleNamespace(
        live_min_account_equity=0.0,
        live_min_account_equity_currency="",
    )
    trader.mt5 = Mock()
    trader.mt5.config = SimpleNamespace(
        expected_login=7,
        expected_server="Broker-Demo",
        expected_currency="USD",
    )

    def account(mode):
        return SimpleNamespace(
            trade_mode=mode,
            equity=10_000.0,
            trade_allowed=True,
            trade_expert=True,
            login=7,
            server="Broker-Demo",
            currency="USD",
        )

    for mode in (1, 2, None):
        value = account(mode) if mode is not None else SimpleNamespace(
            equity=10_000.0,
            trade_allowed=True,
            trade_expert=True,
            login=7,
            server="Broker-Demo",
            currency="USD",
        )
        trader.mt5.get_account_info.return_value = value
        fresh, reason = trader._fresh_entry_account_validation(value)
        assert fresh is None
        assert reason

    demo = account(0)
    trader.mt5.get_account_info.return_value = demo
    fresh, reason = trader._fresh_entry_account_validation(demo)
    assert fresh is demo
    assert reason is None


def test_edge_magic_comment_and_runtime_evidence_state_are_separate(tmp_path):
    config = PipelineConfig(output_dir=tmp_path / "outputs", log_dir=tmp_path / "logs")
    mt5 = Mock()
    mt5.is_connected.return_value = False
    manager = Mock(symbols=[])
    trader = LiveTrader(
        mt5_connector=mt5,
        portfolio_manager=manager,
        position_config=PositionConfig(),
        enable_trading=True,
        pipeline_config=config,
        execution_book=EDGE_DEMO_EXECUTION_BOOK,
    )

    comment = TradeTagEncoder.encode_comment(
        "EURUSD",
        "H1",
        "DemoStrategy",
        "LONG",
        risk_pct=0.25,
        book_role=EDGE_CHALLENGER_ROLE,
    )
    decoded = TradeTagEncoder.decode_comment(comment)
    assert comment.startswith("PME:")
    assert decoded["book_role"] == EDGE_CHALLENGER_ROLE
    assert trader._live_outcome_ledger.ledger_path == (
        tmp_path / "outputs" / "edge_challenger_demo" / "closed_positions.jsonl"
    )
    assert trader._live_outcome_ledger.state_path == (
        tmp_path / "outputs" / "edge_challenger_demo" / "closed_positions.state.json"
    )
    assert trader._institutional_supervisor.state_path.parent.name == "edge_challenger_demo"
    assert trader._cycle_drift_monitor.state_path.parent.name == "edge_challenger_demo"
    assert trader.execution_book == EDGE_DEMO_EXECUTION_BOOK
    assert CHAMPION_EXECUTION_BOOK not in str(trader._live_outcome_ledger.ledger_path)


def test_closed_demo_order_persists_exact_edge_candidate_attribution(tmp_path):
    config = PipelineConfig(output_dir=tmp_path)
    ledger = LiveOutcomeLedger(
        config,
        ledger_path=tmp_path / "edge.jsonl",
        state_path=tmp_path / "edge.state.json",
    )
    cell = _edge_cell()
    candidate_id = cell.artifact_meta["candidate_id"]
    comment = TradeTagEncoder.encode_comment(
        "EURUSD",
        "H1",
        cell.strategy_name,
        "LONG",
        risk_pct=0.25,
        book_role=EDGE_CHALLENGER_ROLE,
    )
    ledger.record_entry_intent(
        position_id=77,
        opening_deal_ticket=1,
        opening_order_ticket=11,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name=cell.strategy_name,
        magic=123,
        entry_price=1.10,
        volume=1.0,
        initial_stop=1.09,
        risk_cash=100.0,
        book_role=EDGE_CHALLENGER_ROLE,
        candidate_id=candidate_id,
    )
    history = {
        "history_complete": True,
        "deals": [
            {
                "ticket": 1,
                "order_ticket": 11,
                "position_id": 77,
                "symbol": "EURUSD",
                "entry_kind": "IN",
                "volume": 1.0,
                "price": 1.10,
                "profit": 0.0,
                "magic": 123,
                "comment": comment,
                "time": 1_750_000_000,
            },
            {
                "ticket": 2,
                "order_ticket": 12,
                "position_id": 77,
                "symbol": "EURUSD",
                "entry_kind": "OUT",
                "volume": 1.0,
                "price": 1.11,
                "profit": 50.0,
                "magic": 123,
                "comment": comment,
                "time": 1_750_003_600,
            },
        ],
        "orders": [{"ticket": 11, "position_id": 77, "sl": 1.09}],
    }
    result = ledger.sync(
        closing_deals=[history["deals"][1]],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: history,
        resolve_cell=lambda _context: {
            "symbol": "EURUSD",
            "timeframe": "H1",
            "regime": "TREND",
            "strategy_name": cell.strategy_name,
            "book_role": EDGE_CHALLENGER_ROLE,
            "candidate_id": candidate_id,
            "cell_match_status": "EXACT",
            "cell_match_reason": "edge test",
        },
    )

    record = result.emitted_records[0]
    assert record["book_role"] == EDGE_CHALLENGER_ROLE
    assert record["candidate_id"] == candidate_id
    assert record["cell_match_status"] == "EXACT"
    assert record["eligible_for_auto_promotion"] is False
