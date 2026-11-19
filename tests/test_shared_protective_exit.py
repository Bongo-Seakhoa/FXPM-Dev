"""Synthetic fill oracles for the shared Core protective/session-exit decision."""
import numpy as np
import pandas as pd
import pytest

import pm_core
from pm_core import Backtester, PipelineConfig, EXIT_NONE, EXIT_SL, EXIT_TP, EXIT_SESSION
from test_backtester_cost_timing import _ScheduledAtrStrategy, _fx_spec


@pytest.fixture(params=["python", "numba"])
def exit_primitive(request):
    helper = pm_core._protective_session_exit
    if request.param == "numba":
        if not pm_core.NUMBA_AVAILABLE:
            pytest.skip("Numba is unavailable")
        return helper
    return getattr(helper, "py_func", helper)


@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
@pytest.mark.parametrize("opened,low,high,session_exit,price,reason,at_open", [
    (100., 95., 105., False, 0., EXIT_NONE, False),
    (100., 90., 105., False, 90., EXIT_SL, False),
    (100., 95., 110., False, 110., EXIT_TP, False),
    (100., 89., 111., False, 90., EXIT_SL, False),
    (85., 80., 112., False, 85., EXIT_SL, True),
    (90., 89., 111., False, 90., EXIT_SL, True),
    (115., 112., 118., False, 110., EXIT_TP, False),
    (110., 105., 112., False, 110., EXIT_TP, False),
    (115., 89., 118., False, 90., EXIT_SL, False),
    (100., 95., 105., True, 100., EXIT_SESSION, True),
    (85., 80., 120., True, 85., EXIT_SESSION, True),
], ids=["quiet", "stop-touch", "target-touch", "both-stop-first", "gap-stop",
        "open-on-stop", "gap-target-level", "open-on-target", "target-open-then-stop",
        "session-quiet", "session-overrides-protection"])
def test_exit_primitive_fill_oracle(
    exit_primitive, direction, opened, low, high, session_exit, price, reason, at_open,
):
    stop, target = 90., 110.
    if direction == -1:
        opened, low, high = 200. - opened, 200. - high, 200. - low
        stop, target = 200. - stop, 200. - target
        if reason != EXIT_NONE:
            price = 200. - price
    assert exit_primitive(direction, opened, low, high, stop, target, session_exit) == (
        price, reason, at_open,
    )


def test_exit_primitive_default_preserves_protective_behavior(exit_primitive):
    assert exit_primitive(1, 85., 80., 112., 90., 110.) == (85., EXIT_SL, True)


@pytest.fixture
def account_frame():
    index = pd.date_range("2026-05-11", periods=4, freq="h", tz="UTC")
    return pd.DataFrame({
        "Open": np.ones(4), "High": np.full(4, 1.0002),
        "Low": np.full(4, .9998), "Close": np.ones(4),
        "ATR_14": np.full(4, .001),
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
    }, index=index)


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
@pytest.mark.parametrize("exit_bar", [1, 2], ids=["entry-bar", "held-bar"])
def test_account_stop_first_oracle_keeps_entry_open_ownership(
    monkeypatch, account_frame, compiled, direction, exit_bar,
):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba is unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    frame = account_frame
    frame.loc[frame.index[exit_bar], ["High", "Low"]] = [1.003, .997]
    strategy = _ScheduledAtrStrategy([direction, -direction, 0, 0])
    result = Backtester(PipelineConfig(
        research_mode=True, use_spread=False, swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.,
    )).run(frame, strategy.generate_signals(frame, "ZZZUSD"), "ZZZUSD", strategy,
           spec=_fx_spec(), timeframe="H1")
    trades = result["trades"]
    incumbent = trades[0]
    assert incumbent["entry_bar"] == 1
    assert incumbent["exit_bar"] == exit_bar
    assert incumbent["exit_reason"] == "closed_sl"
    assert incumbent["exit_price"] == incumbent["stop_loss"]
    assert incumbent["exit_price"] == pytest.approx(1. - direction * .002)
    # Only the entry-bar exit leaves the following bar's fresh intent eligible.
    assert [trade["entry_bar"] for trade in trades] == ([1, 2] if exit_bar == 1 else [1])


@pytest.mark.parametrize("compiled", [False, True], ids=["python", "numba"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
def test_account_session_open_precedes_stop_and_target(
    monkeypatch, account_frame, compiled, direction,
):
    if compiled and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba is unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    frame = account_frame
    frame.loc[frame.index[2], ["High", "Low"]] = [1.003, .997]
    strategy = _ScheduledAtrStrategy([direction, -direction, 0, 0])
    strategy.session_spec = lambda symbol: (0, 120)
    result = Backtester(PipelineConfig(
        research_mode=True, use_spread=False, swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.,
    )).run(frame, strategy.generate_signals(frame, "ZZZUSD"), "ZZZUSD", strategy,
           spec=_fx_spec(), timeframe="H1")
    assert len(result["trades"]) == 1
    trade = result["trades"][0]
    assert (trade["entry_bar"], trade["exit_bar"]) == (1, 2)
    assert trade["exit_reason"] == "closed_session"
    assert trade["exit_price"] == frame["Open"].iat[2]
    assert trade["mae_pips"] == trade["mfe_pips"] == 2.
