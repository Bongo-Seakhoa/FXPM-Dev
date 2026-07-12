from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pm_main import LiveTrader


def bare_trader() -> LiveTrader:
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = MagicMock()
    trader.logger = MagicMock()
    trader._get_management_configs = MagicMock(return_value={})
    trader._resolve_symbol_from_broker = MagicMock(return_value=None)
    trader._estimate_stop_loss_amount = MagicMock(return_value=(25.0, "mt5"))
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
    assert trader._estimate_stop_loss_amount.call_args.args[0] == 0


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
