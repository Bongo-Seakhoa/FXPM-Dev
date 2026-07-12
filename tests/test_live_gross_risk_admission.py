from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pm_main import LiveTrader


def bare_trader() -> LiveTrader:
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = MagicMock()
    trader.logger = MagicMock()
    trader.pipeline_config = SimpleNamespace(use_commission=False)
    trader._get_management_configs = MagicMock(return_value={})
    trader._resolve_symbol_from_broker = MagicMock(return_value=None)
    trader.mt5.order_calc_profit.return_value = -25.0
    trader.mt5.get_symbol_info.return_value = SimpleNamespace(asset_class="fx")
    return trader


def position(**overrides):
    payload = {
        "ticket": 42,
        "symbol": "EURUSD",
        "comment": "PM3:EURUSD:H1:abc12:L:2",
        "price_open": 1.1000,
        "sl": 1.0950,
        "volume": 0.10,
        "type": 0,
        "price_current": 1.1000,
        "profit": 0.0,
        "swap": 0.0,
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def test_live_open_position_risk_uses_fresh_geometry_and_equity():
    trader = bare_trader()
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position()],
        SimpleNamespace(balance=10_000.0, equity=10_000.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert error == ""
    assert risk_capital == pytest.approx(9_975.0)
    assert len(exposures) == 1
    assert exposures[0].symbol == "EURUSD"
    assert exposures[0].stop_risk_pct == pytest.approx(25.0 / 9_975.0 * 100.0)
    # BUY type 0 must remain BUY; `0 or 1` would silently invert it.
    assert trader.mt5.order_calc_profit.call_args.args[0] == 0


def test_live_locked_profit_stop_raises_stop_equity_and_uses_no_gross_exposure():
    trader = bare_trader()
    trader.mt5.order_calc_profit.return_value = 50.0
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position(sl=1.1050, price_current=1.1100, profit=100.0)],
        SimpleNamespace(balance=10_000.0, equity=10_100.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert error == ""
    assert risk_capital == pytest.approx(10_050.0)
    assert exposures == []


def test_live_mixed_stops_preserve_locked_profit_and_count_only_capital_loss():
    trader = bare_trader()
    trader.mt5.order_calc_profit.side_effect = [50.0, -100.0]
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [
            position(ticket=1, profit=100.0, sl=1.1050),
            position(
                ticket=2,
                symbol="GBPUSD",
                comment="PM3:GBPUSD:H1:abc12:L:2",
                price_open=1.2500,
                price_current=1.2450,
                sl=1.2400,
                profit=-50.0,
            ),
        ],
        SimpleNamespace(balance=10_000.0, equity=10_050.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert error == ""
    assert risk_capital == pytest.approx(9_950.0)
    assert len(exposures) == 1
    assert exposures[0].symbol == "GBPUSD"
    assert exposures[0].stop_risk_pct == pytest.approx(100.0 / 9_950.0 * 100.0)


def test_live_missing_current_mtm_fails_closed():
    trader = bare_trader()
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position(profit=None)],
        SimpleNamespace(balance=10_000.0, equity=10_000.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert exposures == []
    assert risk_capital is None
    assert "current MTM profit/swap is unavailable" in error


def test_signed_stop_pnl_unavailable_after_all_paths_fails_closed():
    trader = bare_trader()
    trader.mt5.order_calc_profit.return_value = None
    empty_math = SimpleNamespace(
        trade_tick_size=0.0,
        trade_tick_value=0.0,
    )
    pnl, path = trader._estimate_stop_pnl_amount(
        0,
        "",
        1.0,
        1.1000,
        1.0900,
        accrued_swap=0.0,
        spec=SimpleNamespace(pip_size=0.0, pip_value=0.0),
        symbol_info=empty_math,
    )
    assert pnl is None
    assert path is None

    trader = bare_trader()
    trader._estimate_stop_pnl_amount = MagicMock(return_value=(None, None))
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position()],
        SimpleNamespace(balance=10_000.0, equity=10_000.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert exposures == []
    assert risk_capital is None
    assert "signed stop P&L cannot be measured" in error


def test_signed_stop_pnl_fallback_preserves_profit_direction_and_costs():
    trader = bare_trader()
    trader.pipeline_config = SimpleNamespace(use_commission=True)
    trader.mt5.order_calc_profit.return_value = None
    spec = SimpleNamespace(
        pip_size=0.0001,
        pip_value=10.0,
        commission_per_lot=8.0,
    )
    info = SimpleNamespace(trade_tick_size=0.0001, trade_tick_value=10.0)
    pnl, path = trader._estimate_stop_pnl_amount(
        0,
        "EURUSD",
        1.0,
        1.1000,
        1.1010,
        accrued_swap=5.0,
        spec=spec,
        symbol_info=info,
    )
    assert path == "tick_value"
    assert pnl == pytest.approx(101.0)  # +100 price +5 swap -4 exit commission


def test_live_open_position_without_stop_fails_closed():
    trader = bare_trader()
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position(sl=0.0)],
        SimpleNamespace(balance=10_000.0, equity=10_000.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert exposures == []
    assert risk_capital is None
    assert "lacks positive entry, stop or volume" in error


def test_unattributed_external_position_blocks_new_shared_risk():
    trader = bare_trader()
    exposures, risk_capital, error = trader._build_live_stop_risk_exposures(
        [position(symbol="UNMAPPED.a", comment="")],
        SimpleNamespace(balance=10_000.0, equity=10_000.0),
        candidate_symbol="EURUSD",
        candidate_broker_symbol="EURUSD",
    )
    assert exposures == []
    assert risk_capital is None
    assert "cannot resolve canonical symbol" in error


def test_trade_tag_must_match_broker_symbol():
    trader = bare_trader()
    trader.mt5.find_broker_symbol.return_value = "EURUSD.a"
    canonical, reason = trader._resolve_live_position_canonical_symbol(
        position(symbol="GBPUSD", comment="PM3:EURUSD:H1:abc12:L:2"),
        candidate_symbol="USDJPY",
        candidate_broker_symbol="USDJPY",
    )
    assert canonical is None
    assert "does not match broker symbol" in reason
