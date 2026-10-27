"""Actual main control paths with synthetic dependencies, not trading authority.

Account/quotes and upstream eligibility are invented. Runtime storage, writer
lease, PAPER ledger and owner restoration are real. The app loop uses an already
stopped test trader; it cannot fetch source data or evaluate a market strategy.
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_main as main
import pm_mt5 as backend
import pm_live_outcomes as outcomes
import pm_storage as storage
from pm_position import TradeTagEncoder
from pm_regime import RegimeParams, regime_params_sha256
from pm_strategies import StrategyRegistry
import test_operational_execution as execution
from test_operational_execution import world
import test_required_signal_durability as signal_fixture


assert Path(main.__file__).resolve() == PRODUCT_ROOT / "pm_main.py"
assert Path(storage.__file__).resolve() == PRODUCT_ROOT / "pm_storage.py"
assert signal_fixture.main is main
LABEL = "SYNTHETIC_MAIN_INTEGRATION_NO_TRADING_AUTHORITY"


def _config():
    strategy = StrategyRegistry.get("EMACrossoverStrategy")
    params = strategy.get_default_params()
    return main.SymbolConfig.from_dict({"symbol": "EURUSD", "regime_configs": {
        tf: {"TREND": {"strategy_name": "EMACrossoverStrategy", "parameters": deepcopy(params),
                       "governance_policy": {}}} for tf in ("M5", "H1", "D1")}})


def _trader(account):
    trader = main.LiveTrader.__new__(main.LiveTrader)
    trader.mt5, trader.logger = account.paper, Mock()
    trader.execution_destination = "PAPER"
    trader._runtime_account_identity = account.identity
    trader._live_outcome_ledger = account.ledger
    trader._held_owner_contexts, trader._held_owner_errors = {}, {}
    trader.pipeline_config = SimpleNamespace(position_timeframe_overrides={})
    return trader


def _owner_snapshot(trader, config, timeframe):
    params = RegimeParams().to_dict()
    strategy = StrategyRegistry.get("EMACrossoverStrategy")
    cell = config.get_regime_config(timeframe, "TREND")
    candidate = {"owner_detector": {"parameters": params, "parameters_sha256": regime_params_sha256(params)}}
    return trader._paper_owner_snapshot("EURUSD", timeframe, "TREND", "EMACrossoverStrategy",
        strategy, config, cell, candidate, "LOCKBOX_VALIDATED_CHAMPION", "synthetic-original-owner")


def _held(account, trader, config, timeframe="H1", *, bind=True):
    magic = TradeTagEncoder.encode_magic("EURUSD", timeframe, "TREND")
    snapshot = _owner_snapshot(trader, config, timeframe)
    reservation = account.ledger.reserve_pre_send_intent(symbol="EURUSD", timeframe=timeframe, regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=magic, entry_price=1.1002, volume=1.0,
        initial_stop=1.09, risk_cash=1020.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
        candidate_id="synthetic-original-owner", reservation_id="1" * 32, owner_snapshot=snapshot)
    result = account.paper.send_market_order("EURUSD", backend.OrderType.BUY, 1.0, sl=1.09, tp=1.12,
        magic=magic, reservation_id=reservation["reservation_id"], comment=TradeTagEncoder.encode_comment(
            "EURUSD", timeframe, "EMACrossoverStrategy", "LONG", risk_pct=0.25))
    assert result.success
    if bind:
        account.ledger.bind_pre_send_intent(reservation["reservation_id"], position_id=result.position_id)
    return account.paper.get_position_by_ticket(result.position_id), snapshot


def _slot(trader, config, positions, timeframe):
    return trader._same_symbol_specialist_position_block("EURUSD", config, positions,
        SimpleNamespace(margin_mode=2, hedge_allowed=True),
        candidate_magic=TradeTagEncoder.encode_magic("EURUSD", timeframe, "TREND"), candidate_timeframe=timeframe)


@pytest.mark.parametrize("held_tf,candidate_tf,expected", [
    ("H1", "M5", "BLOCKED_LIVE_SLOT_OCCUPIED"), ("H1", "D1", None),
    ("D1", "H1", None), ("D1", "D1", "BLOCKED_LIVE_SLOT_OCCUPIED"),
])
def test_actual_shared_slot_counts_d1_and_lower_from_original_owner(world, held_tf, candidate_tf, expected):
    account, config = world.open(), _config()
    trader = _trader(account)
    position, _ = _held(account, trader, config, held_tf)
    assert trader._refresh_held_owner_contexts([position])
    result = _slot(trader, config, [position], candidate_tf)
    assert (None if result is None else result[0]) == expected


@pytest.mark.parametrize("pending_tf,candidate_tf,expected", [
    ("M5", "H1", "BLOCKED_LIVE_SLOT_OCCUPIED"), ("D1", "H1", None),
])
def test_pending_intent_consumes_actual_shared_slot(world, pending_tf, candidate_tf, expected):
    account, config = world.open(), _config()
    trader = _trader(account)
    execution._reserve(account, timeframe=pending_tf, magic=456)
    result = _slot(trader, config, [], candidate_tf)
    assert (None if result is None else result[0]) == expected
    assert len(account.ledger.unresolved_pre_send_intents()) == 1


def test_conflicting_slots_unknown_owner_and_unavailable_snapshot_fail_closed(world):
    account, config = world.open(), _config()
    trader = _trader(account)
    position, _ = _held(account, trader, config)
    trader._refresh_held_owner_contexts([position])
    execution._reserve(account, reservation_id="2" * 32, timeframe="M5", magic=456)
    assert _slot(trader, config, [position], "D1")[0] == "BLOCKED_LIVE_SLOT_CONFLICT"
    assert _slot(trader, config, None, "D1")[0] == "BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE"
    unknown = deepcopy(position)
    unknown.ticket = unknown.identifier = 999
    unknown.comment = ""
    assert _slot(trader, config, [unknown], "D1")[0] == "BLOCKED_POSITION_OWNER_UNKNOWN"
    foreign = deepcopy(position)
    foreign.destination = "BROKER"
    assert _slot(trader, config, [foreign], "D1")[0] == "BLOCKED_POSITION_OWNER_UNKNOWN"


def test_real_cold_reconcile_and_held_owner_ignore_current_candidate_changes(world):
    account, config = world.open(), _config()
    trader = _trader(account)
    position, original = _held(account, trader, config, bind=False)
    account.ledger.close()
    account.ledger = world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    trader = _trader(account)
    trader.pm = SimpleNamespace(get_validated_configs=Mock(side_effect=AssertionError("Do not replace an original held owner")))
    config.get_regime_config("H1", "TREND").parameters["fast_period"] = 99
    trader._reconcile_runtime_reservations(account.paper.get_positions())
    assert account.ledger.unresolved_pre_send_intents() == []
    recovered = trader._refresh_held_owner_contexts(account.paper.get_positions())
    owner = trader._held_owner_contexts[position.ticket]
    assert recovered["EURUSD"].get_regime_config("H1", "TREND").parameters == original["parameters"]
    assert owner["cell"].parameters != config.get_regime_config("H1", "TREND").parameters
    assert all(owner["permissions"][name] is False for name in ("open_new_positions", "increase_positions", "widen_stops"))
    assert owner["permissions"]["tighten_stops"] and owner["permissions"]["reduce_positions"]
    trader.pm.get_validated_configs.assert_not_called()
    changed = deepcopy(owner["intent"])
    changed["owner_snapshot"]["detector"]["detector_code_sha256"] = "0" * 64
    with pytest.raises(outcomes.LiveOutcomeError, match="detector is unavailable or changed"):
        trader._owner_context_for_intent(changed)
    legacy = {**owner["intent"], "book_role": main.PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
              "management_capsule_id": "", "management_generation_id": ""}
    legacy.pop("owner_snapshot", None)
    with pytest.raises(outcomes.LiveOutcomeError, match="held-owner capsule binding is incomplete"):
        trader._owner_context_for_intent(legacy)


@pytest.mark.parametrize("late_pending", [False, True])
def test_actual_final_snapshot_and_reservation_boundary_see_late_pending_slot(world, tmp_path, monkeypatch, late_pending):
    account, config = world.open(), _config()
    helpers = signal_fixture._helpers
    trader = helpers.paper_trader(signal_fixture._synthetic_tier(), [])
    trader.mt5 = account.paper
    trader.execution_destination, trader._runtime_account_identity = "PAPER", account.identity
    trader._held_owner_contexts, trader._held_owner_errors = {}, {}
    trader.enable_trading = True
    trader._live_outcome_ledger = account.ledger
    trader._live_outcome_feedback_initialized = True
    trader._live_outcome_feedback_error = trader._startup_entry_block_reason = ""
    trader._is_current_refit_demo = False
    trader.storage_manager = storage.StorageManager(trader.pipeline_config,
        data_dir=tmp_path / "signals/data", output_dir=tmp_path / "signals/output",
        log_dir=tmp_path / "signals/logs", logger=Mock(), runtime_identity=account.identity)
    trader._same_symbol_specialist_position_block = main.LiveTrader._same_symbol_specialist_position_block.__get__(trader)
    trader._realized_loss_hard_block_reasons = Mock(return_value=[])
    world.fake.set_quote(account.login, 1.0, 1.00005)
    monkeypatch.setattr(account.paper, "get_bars", Mock(return_value=helpers.prior_bars(9)))
    baseline = account.paper.get_risk_account_info()
    events = []
    real_fresh = trader._fresh_entry_account_validation
    real_positions = account.paper.get_positions
    real_reserve = account.ledger.reserve_pre_send_intent

    def authorization(*_args):
        events.append("authorization")
        if late_pending:
            execution._reserve(account, timeframe="M5", magic=456)
        return None

    def fresh(value):
        events.append("fresh-account")
        return real_fresh(value)

    def positions(*args, **kwargs):
        events.append("positions")
        return real_positions(*args, **kwargs)

    def reserve(**kwargs):
        events.append("reserve")
        return real_reserve(**kwargs)

    def no_fill(**kwargs):
        events.append("send")
        assert len(kwargs["reservation_id"]) == 32
        assert account.ledger.paper_fill_for_reservation(kwargs["reservation_id"]) is None
        return backend.MT5OrderResult(False, 10013, "SYNTHETIC_DEFINITIVE_NO_FILL", destination="PAPER")

    trader._promotion_authorization_block_reason = Mock(side_effect=authorization)
    trader._fresh_entry_account_validation = Mock(side_effect=fresh)
    monkeypatch.setattr(account.paper, "get_positions", Mock(side_effect=positions))
    monkeypatch.setattr(account.ledger, "reserve_pre_send_intent", Mock(side_effect=reserve))
    monkeypatch.setattr(account.paper, "send_market_order", Mock(side_effect=no_fill))
    detector = RegimeParams().to_dict()
    candidate = {"timeframe": "H1", "regime": "TREND", "strategy_name": "EMACrossoverStrategy",
        "regime_config": config.get_regime_config("H1", "TREND"),
        "owner_detector": {"parameters": detector, "parameters_sha256": regime_params_sha256(detector)}}
    trader._execute_entry(symbol="EURUSD", broker_symbol="EURUSD", signal=1,
        strategy=StrategyRegistry.get("EMACrossoverStrategy"),
        features=pd.DataFrame({"ATR_14": [0.01, 0.01]}, index=[helpers.OPEN - pd.Timedelta(hours=1), helpers.OPEN]),
        spec=SimpleNamespace(pip_size=0.0001, point=0.00001, spread_avg=0.9),
        magic=TradeTagEncoder.encode_magic("EURUSD", "H1", "TREND"), config=config,
        decision_key=LABEL, bar_time_iso=str(helpers.OPEN - pd.Timedelta(hours=1)), best_candidate=candidate,
        trade_intent=SimpleNamespace(stop_loss_pips=100, take_profit_pips=200),
        positions_snapshot=[], account_info=baseline)
    payload = trader._actionable_log.record.call_args.args[1]
    assert events.index("authorization") < events.index("fresh-account")
    assert "positions" in events[events.index("fresh-account") + 1:]
    if late_pending:
        assert payload["action"] == "BLOCKED_LIVE_SLOT_OCCUPIED"
        account.paper.send_market_order.assert_not_called()
        assert events.count("reserve") == 1  # The injected M5 intent, never the new H1 order.
        assert [r["timeframe"] for r in account.ledger.unresolved_pre_send_intents()] == ["M5"]
    else:
        assert payload["action"] == "FAILED_10013"
        assert events.index("fresh-account") < events.index("reserve") < events.index("send")
        assert account.ledger.unresolved_pre_send_intents() == []
        account.paper.send_market_order.assert_called_once()


def _app_config(root):
    return SimpleNamespace(data_dir=root / "data", output_dir=root / "output", log_dir=root / "logs",
        winner_ledger_path=str(root / "invented-winner-ledger.json"), production_retrain_mode="off",
        describe_retrain_schedule=lambda: LABEL, live_outcome_feedback_enabled=True,
        live_outcome_feed_cycle_drift=False, storage_signal_ledger_enabled=True)


@pytest.mark.parametrize("state", ["flat-no-winner", "held-no-winner", "technical-error", "clean-stop", "trader-error"])
def test_actual_app_binding_and_lifecycle_distinguish_no_winner_from_startup_failure(world, tmp_path, monkeypatch, request, state):
    config = _app_config(tmp_path / state)
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5 = world.native(900001)
    app._skip_storage_shutdown_housekeeping = True
    monkeypatch.setattr(main, "setup_logging", Mock())
    selected = {} if "no-winner" in state else {"EURUSD": _config()}
    eligibility = Mock(side_effect=RuntimeError("SYNTHETIC_TECHNICAL_STARTUP_ERROR")) if state == "technical-error" else Mock(return_value=selected)
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=eligibility)
    acquired, constructed = [], []
    real_ledger = main.LiveOutcomeLedger

    def ledger(*args, **kwargs):
        result = real_ledger(*args, **kwargs)
        request.addfinalizer(result.close)
        acquired.append(result)
        if state == "held-no-winner":
            # Existing local exposure, not a qualified new entry. Main must start management.
            identity = kwargs["runtime_identity"]
            result.reserve_pre_send_intent(symbol="EURUSD", timeframe="H1", regime="TREND",
                strategy_name="EMACrossoverStrategy", magic=123, entry_price=1.1002, volume=1.0,
                initial_stop=1.09, risk_cash=1020.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
                candidate_id="synthetic-owner-not-qualified", reservation_id="1" * 32)
            result.paper_open_position("1" * 32, {"price": 1.1002, "volume": 1.0, "type": 0,
                "time_msc": int(world.fake.when.timestamp() * 1000), "risk_cash": 1020.0,
                "funding_terms": {"schema_version": "fxpm.paper-funding-terms.v1", "swap_mode": 0,
                                  "swap_long": 0.0, "swap_short": 0.0}})
            assert identity.destination == "PAPER"
        return result

    def stopped_loop(**kwargs):
        constructed.append(kwargs)
        if state == "trader-error":
            raise RuntimeError("SYNTHETIC_TRADER_CONSTRUCTION_ERROR")
        stopped = threading.Event()
        stopped.set()
        return SimpleNamespace(_shutdown_event=stopped, _runtime_account_identity=kwargs["runtime_identity"],
            _runtime_state_dir=kwargs["storage_manager"].output_dir, stop=Mock(), save_trade_log=Mock())

    monkeypatch.setattr(main, "LiveOutcomeLedger", ledger)
    monkeypatch.setattr(main, "LiveTrader", stopped_loop)
    if state == "technical-error":
        with pytest.raises(RuntimeError, match="SYNTHETIC_TECHNICAL_STARTUP_ERROR"):
            app.run_trading(execution_destination="PAPER")
    else:
        result = app.run_trading(execution_destination="PAPER")
        if state in {"flat-no-winner", "trader-error"}:
            assert result is False
    assert len(acquired) == 1 and app._runtime_outcome_ledger is None
    identity = acquired[0].runtime_identity
    assert app.storage_manager.runtime_identity == identity
    assert identity.namespace in app.storage_manager.output_dir.parts
    assert app._runtime_writer_thread == threading.get_ident()
    with pytest.raises(outcomes.LiveOutcomeError, match="closed"):
        acquired[0].paper_positions()
    expected = state not in {"flat-no-winner", "technical-error"}
    assert bool(constructed) is expected
    if constructed:
        assert constructed[0]["runtime_identity"] == identity
        assert constructed[0]["outcome_ledger"] is acquired[0]
    if state in {"clean-stop", "held-no-winner"}:
        app.trader.stop.assert_called_once()
    # A fresh writer proves every path releases the actual account-keyed lease.
    replacement = world.ledger(identity, config)
    assert replacement.runtime_identity == identity


def test_late_direct_destination_refuses_preexisting_research_storage_without_migration(world, tmp_path):
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=_app_config(tmp_path))
    original = app.storage_manager
    assert original is not None and original.runtime_identity is None
    app.mt5 = world.native(900001)
    assert app.run_trading(execution_destination="PAPER") is False
    assert app.storage_manager is original and app._runtime_outcome_ledger is None


def test_foreign_thread_shutdown_stops_without_closing_owner_thread_writer(world):
    account = world.open()
    app = main.FXPortfolioManagerApp.__new__(main.FXPortfolioManagerApp)
    app.logger, app.trader, app.storage_manager, app.mt5 = Mock(), SimpleNamespace(stop=Mock()), Mock(), Mock()
    app._runtime_outcome_ledger, app._runtime_writer_thread = account.ledger, threading.get_ident()
    errors = []

    def request_stop():
        try:
            app.shutdown()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=request_stop)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive() and errors == []
    app.trader.stop.assert_called_once()
    app.storage_manager.on_shutdown.assert_not_called()
    app.mt5.disconnect.assert_not_called()
    assert account.ledger.paper_positions() == []
    app._close_runtime_outcome_ledger()
    assert app._runtime_outcome_ledger is None
