"""Invented independent-R economic oracles; no market programme or qualification."""
from dataclasses import replace
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_core
import pm_signal_opportunities as opportunities
from pm_core import Backtester, PipelineConfig
from test_signal_opportunities import FixedStops, _frame, _spec, _provenance


def _config(**changes):
    return PipelineConfig(research_mode=True, initial_capital=10000., use_spread=True,
        backtest_spread_mode="fixed_spec", swap_model_enabled=False, **changes)


def _evaluate(frame, values, *, strategy=None, spec=None, pricing=None, **kwargs):
    strategy = strategy or FixedStops()
    spec = spec or _spec()
    pricing = pricing or Backtester(_config())
    if "REGIME" not in frame:
        frame = frame.copy(deep=True)
        frame["REGIME"] = "TREND"
    arguments = dict(strategy=strategy, spec=spec, symbol=spec.symbol, timeframe="H1",
        cutoff_utc=frame["_BAR_CLOSE_TS"].iloc[-1], pricing=pricing,
        entry_half_spreads=np.full(len(frame), spec.get_half_spread_price()),
        exit_half_spreads=np.full(len(frame), spec.get_half_spread_price()),
        entry_admission=["ADMIT"] * len(frame), provenance=_provenance(strategy))
    arguments.update(kwargs)
    return opportunities.evaluate_independent_signal_r(frame, pd.Series(values, index=frame.index), **arguments)


@pytest.mark.parametrize("compiled", (False, True), ids=("python", "numba"))
@pytest.mark.parametrize("direction", (1, -1))
@pytest.mark.parametrize("scenario", ("target", "stop", "gap"))
def test_independent_r_matches_single_core_trade_actual_entry_stop_and_costs(monkeypatch, compiled, direction, scenario):
    assert pm_core.NUMBA_AVAILABLE or not compiled, "This scoped oracle requires installed Numba"
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    frame = _frame()
    if scenario == "target":
        frame.loc[frame.index[2], "High" if direction == 1 else "Low"] = 1.001 if direction == 1 else .999
    elif scenario == "stop":
        frame.loc[frame.index[2], "Low" if direction == 1 else "High"] = .999 if direction == 1 else 1.001
    else:
        value = .998 if direction == 1 else 1.002
        frame.loc[frame.index[2], ["Open", "High", "Low", "Close"]] = [value, value + .0001, value - .0001, value]
    signals = [direction, 0, 0, 0, 0, 0]
    strategy, spec, config = FixedStops(), _spec(), _config()
    trade = Backtester(config).run(frame, pd.Series(signals, index=frame.index), spec.symbol,
        strategy, spec=spec, timeframe="H1")["trades"][0]
    batch = _evaluate(frame, signals, strategy=strategy, spec=spec, pricing=Backtester(config))
    record = batch.records[0]
    risk = abs(spec.calculate_tick_profit(trade["entry_price"], trade["stop_loss"], 1., direction))
    unit_pnl = trade["pnl_dollars"] / trade["position_size"]
    assert (record.entry_bar, record.exit_bar) == (trade["entry_bar"], trade["exit_bar"])
    assert record.entry_price == trade["entry_price"] and record.exit_price == trade["exit_price"]
    assert record.initial_risk_per_unit == risk
    assert record.modeled_net_r == pytest.approx(unit_pnl / risk)
    assert batch.cost_model["commission"] == "NOT_MODELED"
    assert batch.qualification_authority is False and batch.complete_cost_net_profit_available is False


def test_independent_r_keeps_overlapping_persistent_signals_without_occupancy():
    frame = _frame()
    frame.loc[frame.index[2:], "High"] = 1.001
    batch = _evaluate(frame, [1, 1, 1, 1, 0, 0])
    assert [row.entry_bar for row in batch.records] == [1, 2, 3, 4]
    assert batch.records[0].exit_bar == batch.records[1].exit_bar == 2
    assert all(row.eligible and row.status == "CLOSED" for row in batch.records)


def test_independent_r_has_no_balance_or_lot_sizing_dependency():
    frame = _frame()
    frame.loc[frame.index[2], "High"] = 1.001
    spec = _spec()
    first = _evaluate(frame, [1, 0, 0, 0, 0, 0], spec=spec).records[0]
    config = _config()
    config.initial_capital = .01
    other = replace(spec, min_lot=1000., max_lot=1000., volume_step=1000.)
    second = _evaluate(frame, [1, 0, 0, 0, 0, 0], spec=other, pricing=Backtester(config)).records[0]
    assert first.modeled_net_r == second.modeled_net_r
    assert first.initial_risk_per_unit == second.initial_risk_per_unit


def test_independent_r_uses_entry_live_label_and_censors_unfinished_bar():
    frame = _frame()
    frame["REGIME"] = ["RANGE", "TREND", "BREAKOUT", "CHOP", "CHOP", "CHOP"]
    frame["REGIME_LIVE"] = ["CHOP", "RANGE", "TREND", "BREAKOUT", "CHOP", "CHOP"]
    frame.loc[frame.index[1:], ["High", "Low", "Close"]] = np.nan
    batch = _evaluate(frame, [1, 1, 1, 1, 1, 1], cutoff_utc=frame.index[1] + pd.Timedelta(minutes=30))
    record = batch.records[0]
    assert record.entry_regime == "RANGE" and record.signal_regime == "CHOP"
    assert record.status == "RIGHT_CENSORED" and record.entry_bar == 1
    assert record.exit_price is None and record.modeled_net_r is None
    assert batch.episode_diagnostics["RANGE"]["resolved_episode_count"] == 0


def test_independent_r_episodes_precede_slicing_and_unresolved_share_is_explicit():
    frame = _frame(count=8)
    frame["REGIME"] = ["TREND", "TREND", "TREND", "RANGE", "RANGE", "TREND", "TREND", "TREND"]
    frame.loc[frame.index[2], "High"] = 1.001
    episodes = opportunities.independent_entry_episodes(frame, symbol="SYNTHETIC", timeframe="H1",
        cutoff_utc=frame["_BAR_CLOSE_TS"].iloc[-1])
    assert episodes.iat[1] == episodes.iat[3] and episodes.iat[1] != episodes.iat[6]
    batch = _evaluate(frame, [1, 0, 0, 0, 0, 1, 0, 0], episode_ids=episodes)
    stats = batch.episode_diagnostics["TREND"]
    assert stats == {"traded_episode_count": 2, "resolved_episode_count": 1,
        "profitable_episode_count": 1, "profitable_episode_share": 1., "unresolved_episode_count": 1}
    sliced = frame.iloc[2:]
    part = _evaluate(sliced, [1, 0, 0, 0, 0, 0], episode_ids=episodes.loc[sliced.index])
    assert part.records[0].episode_id == episodes.iat[1]


def test_independent_r_prepares_swap_once_and_keeps_core_phase_inclusivity(monkeypatch):
    frame = _frame(count=6)
    frame.index = pd.date_range("2026-01-05", periods=6, freq="D", tz="UTC")
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(days=1)
    frame.loc[frame.index[4], "High"] = 1.001
    spec = replace(_spec(), swap_long=-2., swap_short=1., swap_mode=6, contract_size=100000.)
    config = _config()
    config.swap_model_enabled = True
    signals = [1, 0, 0, 0, 0, 0]
    exact = {}
    Backtester(config).run(frame, pd.Series(signals, index=frame.index), spec.symbol,
        FixedStops(), spec=spec, timeframe="D1", _exact_trade_facts_out=exact)
    account = exact["trades"][0]
    pricing, calls = Backtester(config), []
    original = pricing._build_causal_swap_inputs
    def prepare(*args, **kwargs):
        calls.append(len(args[0]))
        return original(*args, **kwargs)
    monkeypatch.setattr(pricing, "_build_causal_swap_inputs", prepare)
    record = _evaluate(frame, signals, spec=spec, pricing=pricing, timeframe="D1").records[0]
    risk = abs(spec.calculate_tick_profit(account["entry_price"], account["initial_stop_price"], 1., 1))
    unit_swap = account["swap_dollars_exact"] / account["executed_volume"]
    unit_price = spec.calculate_tick_profit(account["entry_price"], account["exit_price"], 1., 1)
    assert calls == [len(frame)]
    assert record.swap_per_unit == pytest.approx(unit_swap)
    assert record.modeled_net_r == pytest.approx((unit_price + unit_swap) / risk)


@pytest.mark.parametrize("admission", ("REFUSE", "MISSING", "UNAVAILABLE"))
def test_independent_r_keeps_ineligible_emission_without_inventing_trade(admission):
    batch = _evaluate(_frame(), [1, 0, 0, 0, 0, 0], entry_admission=["ADMIT", admission, "ADMIT", "ADMIT", "ADMIT", "ADMIT"])
    record = batch.records[0]
    assert batch.window_nonzero_count == 1 and record.status == "NOT_ELIGIBLE"
    assert record.entry_price is None and record.initial_risk_per_unit is None and record.modeled_net_r is None


def test_independent_r_respects_declared_strategy_session_exit_before_protection():
    class SessionStops(FixedStops):
        def session_spec(self, symbol): return (0, 120)
    frame = _frame()
    frame.loc[frame.index[2], ["High", "Low"]] = [1.001, .999]
    record = _evaluate(frame, [1, 0, 0, 0, 0, 0], strategy=SessionStops()).records[0]
    assert (record.entry_bar, record.exit_bar, record.exit_reason, record.exit_fill) == (1, 2, "SESSION", "SESSION_OPEN")
    assert record.exit_observed_at_utc == frame.index[2].isoformat().replace("+00:00", "Z")


@pytest.mark.parametrize("has_signal", (False, True), ids=("empty", "refused"))
def test_independent_r_skips_unused_financing_preparation(monkeypatch, has_signal):
    pricing = Backtester(_config())
    monkeypatch.setattr(pricing, "_build_causal_swap_inputs", lambda *args, **kwargs:
        pytest.fail("No eligible entry needs financing preparation"))
    batch = _evaluate(_frame(), [int(has_signal), 0, 0, 0, 0, 0], pricing=pricing,
        entry_admission=["REFUSE"] * 6)
    assert batch.window_nonzero_count == int(has_signal)
    assert batch.cost_model["swap_contract"]["status"] == "NOT_REQUIRED_NO_ELIGIBLE_ENTRY"
