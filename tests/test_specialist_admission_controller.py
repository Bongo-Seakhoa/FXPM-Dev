from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from pm_core import get_instrument_spec
from pm_main import DecisionThrottle, LiveTrader
from pm_position import PositionConfig, TradeTagEncoder


def _candidate(timeframe: str, score: float, strategy_name: str):
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
        "strategy_name": strategy_name,
        "timeframe": timeframe,
        "regime": "TREND",
        "signal": 1,
        "selection_score": score,
        "quality_score": score,
        "freshness": 1.0,
        "regime_strength": 1.0,
        "features": features,
        "strategy": strategy,
        "regime_config": None,
        "bar_time": str(features.index[-2]),
    }


def _bare_trader():
    trader = LiveTrader.__new__(LiveTrader)
    return trader


def test_symbol_controller_admits_best_specialist_per_horizon_when_flat():
    trader = _bare_trader()
    candidates = [
        _candidate("M5", 0.90, "M5Best"),
        _candidate("H1", 0.60, "H1RunnerUp"),
        _candidate("D1", 0.80, "D1Best"),
    ]

    admitted = trader._plan_symbol_specialist_admissions(
        candidates,
        allow_d1_plus_lower=True,
        open_position_count=0,
    )

    assert [(item["timeframe"], item["strategy_name"]) for item in admitted] == [
        ("M5", "M5Best"),
        ("D1", "D1Best"),
    ]


def test_symbol_controller_uses_one_remaining_slot_when_position_open():
    trader = _bare_trader()
    candidates = [
        _candidate("M5", 0.90, "M5Best"),
        _candidate("D1", 0.80, "D1Best"),
    ]

    admitted = trader._plan_symbol_specialist_admissions(
        candidates,
        allow_d1_plus_lower=True,
        open_position_count=1,
    )

    assert [item["strategy_name"] for item in admitted] == ["M5Best"]


def test_rate_limit_identity_is_exact_specialist_not_symbol_wide():
    first = LiveTrader._specialist_rate_limit_key("EURUSD", 101)
    second = LiveTrader._specialist_rate_limit_key("EURUSD", 202)

    assert first == "EURUSD|101"
    assert second == "EURUSD|202"
    assert first != second


def test_second_symbol_position_requires_explicit_mt5_hedging_mode():
    assert LiveTrader._multi_position_account_block_reason(
        SimpleNamespace(margin_mode=2, hedge_allowed=True)
    ) is None
    assert "netting/exchange" in LiveTrader._multi_position_account_block_reason(
        SimpleNamespace(margin_mode=0, hedge_allowed=False)
    )
    assert "unavailable" in LiveTrader._multi_position_account_block_reason(
        SimpleNamespace()
    )
    assert "false" in LiveTrader._multi_position_account_block_reason(
        SimpleNamespace(margin_mode=2, hedge_allowed=False)
    )


def test_process_symbol_defers_other_horizon_instead_of_consuming_signal(tmp_path):
    mt5 = Mock()
    mt5.is_connected.return_value = False
    mt5.find_broker_symbol.return_value = "EURUSD"
    mt5.get_symbol_info.return_value = Mock(
        to_instrument_spec=Mock(return_value=get_instrument_spec("EURUSD"))
    )
    config = Mock()
    config.has_regime_configs.return_value = True
    config.get_available_timeframes.return_value = ["M5", "D1"]
    config.get_regimes_for_timeframe.return_value = ["TREND"]
    pipeline = SimpleNamespace(
        allow_d1_plus_lower_tf=True,
        event_calendar_enabled=False,
        local_governance_live_mode="off",
        institutional_supervisor_mode="off",
    )
    portfolio_manager = Mock()
    portfolio_manager.symbols = ["EURUSD"]
    trader = LiveTrader(
        mt5_connector=mt5,
        portfolio_manager=portfolio_manager,
        position_config=PositionConfig(),
        enable_trading=False,
        pipeline_config=pipeline,
    )
    trader._decision_throttle = DecisionThrottle(
        log_path=str(tmp_path / "decisions.json")
    )
    d1 = _candidate("D1", 0.90, "D1Best")
    m5 = _candidate("M5", 0.80, "M5Best")
    trader._evaluate_regime_candidates = Mock(
        return_value=([d1, m5], {"timeframes_evaluated": 2, "timeframes_total": 2})
    )
    trader._execute_entry = Mock()
    trader._last_bar_times = {
        "EURUSD_D1": pd.Timestamp("2026-01-01"),
        "EURUSD_M5": pd.Timestamp("2026-01-01"),
    }

    trader._process_symbol(
        "EURUSD",
        config,
        positions_snapshot=[],
        account_info=SimpleNamespace(trade_allowed=True, trade_expert=True),
    )

    trader._execute_entry.assert_called_once()
    assert trader._execute_entry.call_args.kwargs["best_candidate"]["timeframe"] == "D1"
    assert "EURUSD_D1" in trader._last_bar_times
    assert "EURUSD_M5" not in trader._last_bar_times
    assert trader._pending_specialist_timeframes["EURUSD"] == {"M5"}

    # The bar scheduler must surface the queued horizon even though no new M5
    # bar arrived after the first specialist consumed the shared observation.
    mt5.get_bars.return_value = None
    changed = trader.get_symbols_with_new_bars({"EURUSD": config})
    assert changed["EURUSD"] == {"M5"}

    trader._evaluate_regime_candidates.return_value = (
        [m5],
        {"timeframes_evaluated": 1, "timeframes_total": 2},
    )
    open_d1 = SimpleNamespace(
        symbol="EURUSD",
        type=0,
        ticket=77,
        magic=TradeTagEncoder.encode_magic("EURUSD", "D1", "TREND"),
        comment=TradeTagEncoder.encode_comment(
            "EURUSD", "D1", "D1Best", "LONG", risk_pct=0.25
        ),
    )
    trader._process_symbol(
        "EURUSD",
        config,
        positions_snapshot=[open_d1],
        account_info=SimpleNamespace(
            trade_allowed=True,
            trade_expert=True,
            margin_mode=2,
            hedge_allowed=True,
        ),
        timeframes_filter={"M5"},
    )

    assert trader._execute_entry.call_count == 2
    assert trader._execute_entry.call_args.kwargs["best_candidate"]["timeframe"] == "M5"
    assert "EURUSD" not in trader._pending_specialist_timeframes
