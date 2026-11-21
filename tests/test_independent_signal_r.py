"""Invented independent-R economic oracles; no market programme or qualification."""
from dataclasses import asdict, replace
from types import SimpleNamespace
import json

import numpy as np
import pandas as pd
import pytest

import pm_core
import pm_signal_opportunities as opportunities
from pm_core import Backtester, PipelineConfig
from test_signal_opportunities import FixedStops, PublicStructuralStops, _frame, _spec, _provenance


def _config(**changes):
    return PipelineConfig(research_mode=True, initial_capital=10000., use_spread=True,
        backtest_spread_mode="fixed_spec", swap_model_enabled=False, **changes)


def _evaluate(frame, values, *, strategy=None, spec=None, pricing=None, **kwargs):
    strategy = strategy or FixedStops()
    spec = spec or _spec()
    pricing = pricing or Backtester(_config())
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


def test_independent_r_uses_closed_signal_live_label_and_censors_unfinished_bar():
    frame = _frame()
    frame["REGIME"] = ["RANGE", "TREND", "BREAKOUT", "CHOP", "CHOP", "CHOP"]
    frame["REGIME_LIVE"] = ["CHOP", "RANGE", "TREND", "BREAKOUT", "CHOP", "CHOP"]
    frame.loc[frame.index[1:], ["High", "Low", "Close"]] = np.nan
    stop_inputs = []
    strategy = FixedStops()

    def build_trade_intent(_features, *, regime, bar_index, **_):
        stop_inputs.append((regime, bar_index))
        return SimpleNamespace(stop_loss_pips=4., take_profit_pips=6.)

    strategy.build_trade_intent = build_trade_intent
    batch = _evaluate(frame, [1, 1, 1, 1, 1, 1], strategy=strategy,
        cutoff_utc=frame.index[1] + pd.Timedelta(minutes=30))
    record = batch.records[0]
    assert record.entry_regime == record.signal_regime == "CHOP"
    assert stop_inputs == [("CHOP", 0)]
    assert record.status == "RIGHT_CENSORED" and record.entry_bar == 1
    assert record.exit_price is None and record.modeled_net_r is None
    assert batch.episode_diagnostics["CHOP"]["resolved_episode_count"] == 0


def test_independent_r_episodes_precede_slicing_and_unresolved_share_is_explicit():
    frame = _frame(count=8)
    frame["REGIME_LIVE"] = ["TREND", "TREND", "TREND", "RANGE", "RANGE", "TREND", "TREND", "TREND"]
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


def _trace_base_atr_scalar_stops(monkeypatch):
    calls = []
    scalar = opportunities._stops

    def trace(strategy, features, bar, *args, **kwargs):
        calls.append(bar)
        return scalar(strategy, features, bar, *args, **kwargs)

    monkeypatch.setattr(opportunities, "_stops", trace)
    return calls


def test_base_atr_batch_matches_complete_scalar_v19(monkeypatch):
    class ScalarStops(PublicStructuralStops):
        def calculate_stops(self, *args, **kwargs):
            return super().calculate_stops(*args, **kwargs)

    frame = _frame()
    frame.loc[frame.index[2], "High"] = 1.001
    frame["ATR_14"] = [0.0006, 0.0005, np.nan, np.inf, np.inf, 0.0005]
    frame["REGIME_LIVE"] = ["TREND", "TREND", "RANGE", "CHOP", "BREAKOUT", "TREND"]
    signals = [1, 0, -1, 1, 1, 0]
    params = dict(use_structural_sl=False, sl_atr_mult=0.0, tp_atr_mult=1.25)
    bounds = dict(cutoff_utc=frame.index[-1],
        entry_admission=["ADMIT", "ADMIT", "ADMIT", "ADMIT", "REFUSE", "ADMIT"],
        exit_half_spreads=[0.00002, 0.00002, 0.00003, 0.00002, 0.00002, 0.00002])

    scalar_calls = _trace_base_atr_scalar_stops(monkeypatch)
    batch = _evaluate(frame, signals, strategy=PublicStructuralStops(**params), **bounds)
    assert scalar_calls == []
    scalar = _evaluate(frame, signals, strategy=ScalarStops(**params), **bounds)
    assert scalar_calls == [0, 2]
    assert asdict(batch) == asdict(scalar)
    assert [record.status for record in batch.records] == [
        "CLOSED", "RIGHT_CENSORED", "NOT_ELIGIBLE", "NO_NEXT_OBSERVATION",
    ]
    first, second = batch.records[:2]
    assert first.stop_loss_pips == 5.0 and first.take_profit_pips == pytest.approx(7.5)
    assert first.spread_r < 0.0 and first.swap_r == 0.0
    assert second.stop_loss_pips == second.take_profit_pips == 5.0


@pytest.mark.parametrize("fallback", (
    "stop_override", "intent_override", "price_to_pips_override", "structural",
    "session", "missing_atr", "nonexact_spec", "atr_float32", "bool_multiplier",
))
def test_base_atr_fallback_keeps_scalar_behavior(monkeypatch, fallback):
    class SessionStops(PublicStructuralStops):
        def session_spec(self, symbol):
            return (0, 120)

    class AlternateSpec(pm_core.InstrumentSpec):
        pass

    frame = _frame()
    frame.loc[frame.index[2], "High"] = 1.001
    strategy_type = SessionStops if fallback == "session" else PublicStructuralStops
    strategy = strategy_type(use_structural_sl=fallback == "structural")
    spec = _spec()
    expected_stops = None
    if fallback == "stop_override":
        monkeypatch.setattr(strategy, "calculate_stops", lambda *args, **kwargs: (7.0, 8.0))
        expected_stops = (7.0, 8.0)
    elif fallback == "intent_override":
        monkeypatch.setattr(strategy, "build_trade_intent", lambda *args, **kwargs:
            SimpleNamespace(stop_loss_pips=7.0, take_profit_pips=8.0))
        expected_stops = (7.0, 8.0)
    elif fallback == "price_to_pips_override":
        monkeypatch.setattr(spec, "price_to_pips", lambda price: 9.0)
        expected_stops = (9.0, 9.0)
    elif fallback == "missing_atr":
        frame = frame.drop(columns="ATR_14")
        expected_stops = (5.0, 5.0)
    elif fallback == "nonexact_spec":
        spec = AlternateSpec(**asdict(spec))
    elif fallback == "atr_float32":
        frame["ATR_14"] = frame["ATR_14"].astype(np.float32)
    elif fallback == "bool_multiplier":
        strategy.params["sl_atr_mult"] = True

    scalar_calls = _trace_base_atr_scalar_stops(monkeypatch)
    record = _evaluate(frame, [1, 0, 0, 0, 0, 0], strategy=strategy, spec=spec).records[0]
    assert scalar_calls == [0]
    assert record.status == "CLOSED"
    if expected_stops is not None:
        assert (record.stop_loss_pips, record.take_profit_pips) == expected_stops
    if fallback == "session":
        assert record.exit_fill == "SESSION_OPEN"
        assert record.exit_observed_at_utc == frame.index[2].isoformat().replace("+00:00", "Z")


def test_base_atr_refuses_invalid_pip_geometry_before_stop_work(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Invalid pip geometry must be refused before stop preparation")

    monkeypatch.setattr(opportunities, "_prepare_base_atr_stop_pairs", unexpected)
    monkeypatch.setattr(opportunities, "_stops", unexpected)
    spec = _spec()
    spec.pip_position = 400  # Exact InstrumentSpec, with pip_size underflow to zero.
    strategy = PublicStructuralStops(use_structural_sl=False)
    with pytest.raises(opportunities.OpportunityLabelError, match="^INSTRUMENT_PIP_GEOMETRY_INVALID$"):
        _evaluate(_frame(), [1, 0, 0, 0, 0, 0], strategy=strategy, spec=spec)


@pytest.mark.parametrize("fail_earlier_risk", (False, True), ids=("middle-inf", "earlier-risk"))
def test_base_atr_preserves_scalar_first_error_order(monkeypatch, fail_earlier_risk):
    frame = _frame()
    frame.loc[frame.index[2], "ATR_14"] = np.inf
    spec = _spec()
    if fail_earlier_risk:
        # The first eligible trade must reach its initial-risk check before bar 2's stop failure.
        monkeypatch.setattr(spec, "calculate_tick_profit", lambda *args, **kwargs: 0.0)
    strategy = PublicStructuralStops(use_structural_sl=False)
    scalar_calls = _trace_base_atr_scalar_stops(monkeypatch)
    error = "INITIAL_RISK_INVALID" if fail_earlier_risk else "STOPS_INVALID"
    with pytest.raises(opportunities.OpportunityLabelError, match=f"^{error}$"):
        _evaluate(frame, [1, 0, -1, 0, 1, 0], strategy=strategy, spec=spec)
    assert scalar_calls == ([] if fail_earlier_risk else [2])
