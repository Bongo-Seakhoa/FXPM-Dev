"""Execution-cost timing contracts used by winner statistics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import pm_core
from pm_core import Backtester, InstrumentSpec, PipelineConfig, StrategyCategory
from pm_strategies import BaseStrategy


class _OneShotAtrStrategy(BaseStrategy):
    def __init__(self, direction: int, **params):
        self._direction = int(direction)
        super().__init__(**params)

    @property
    def name(self) -> str:
        return "OneShotAtrStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND

    def get_default_params(self):
        return {
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.0,
            "use_structural_sl": False,
        }

    def generate_signals(self, features, symbol):
        values = np.zeros(len(features), dtype=np.int32)
        values[0] = self._direction
        return pd.Series(values, index=features.index)


class _ScheduledAtrStrategy(_OneShotAtrStrategy):
    def __init__(self, signal_values, **params):
        self._signal_values = np.asarray(signal_values, dtype=np.int32)
        super().__init__(1, **params)

    @property
    def name(self) -> str:
        return "ScheduledAtrStrategy"

    def generate_signals(self, features, symbol):
        assert len(features) == len(self._signal_values)
        return pd.Series(self._signal_values, index=features.index)


def _flat_daily_frame(rows: int = 3) -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=rows, freq="1D", tz="UTC")
    return pd.DataFrame(
        {
            "Open": np.ones(rows),
            "High": np.full(rows, 1.0002),
            "Low": np.full(rows, 0.9998),
            "Close": np.ones(rows),
            "ATR_14": np.full(rows, 0.0010),
            "_BAR_CLOSE_TS": index + pd.Timedelta(days=1),
        },
        index=index,
    )


def _gap_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [
            "2026-04-08 19:00:00+00:00",
            "2026-04-08 20:00:00+00:00",
            "2026-04-09 02:00:00+00:00",
        ]
    )
    return pd.DataFrame(
        {
            "Open": np.ones(3),
            "High": np.full(3, 1.0002),
            "Low": np.full(3, 0.9998),
            "Close": np.ones(3),
            "ATR_14": np.full(3, 0.0010),
            "_BAR_CLOSE_TS": pd.DatetimeIndex(
                [
                    "2026-04-08 20:00:00+00:00",
                    "2026-04-08 21:00:00+00:00",
                    "2026-04-09 03:00:00+00:00",
                ]
            ),
        },
        index=index,
    )


def _fx_spec(**overrides) -> InstrumentSpec:
    values = {
        "symbol": "ZZZUSD",
        "pip_position": 4,
        "pip_value": 10.0,
        "spread_avg": 0.0,
        "min_lot": 0.01,
        "max_lot": 100.0,
        "commission_per_lot": 0.0,
        "tick_size": 0.0001,
        "tick_value": 10.0,
        "point": 0.0001,
        "swap_long": 0.0,
        "swap_short": 0.0,
    }
    values.update(overrides)
    return InstrumentSpec(**values)


def test_volume_rounding_corrects_only_grid_boundary_float_noise():
    spec = _fx_spec(min_lot=0.001, volume_step=0.001)

    assert spec.round_volume(0.49999999999999956) == pytest.approx(0.500)
    assert pm_core._round_volume_numba(
        0.49999999999999956,
        spec.min_lot,
        spec.max_lot,
        spec.volume_step,
    ) == pytest.approx(0.500)
    assert spec.round_volume(0.499999) == pytest.approx(0.499)
    assert pm_core._round_volume_numba(
        0.499999,
        spec.min_lot,
        spec.max_lot,
        spec.volume_step,
    ) == pytest.approx(0.499)
    assert spec.round_volume(0.000999) == 0.0
    assert pm_core._round_volume_numba(
        0.000999,
        spec.min_lot,
        spec.max_lot,
        spec.volume_step,
    ) == 0.0


def test_sub_minimum_risk_size_fails_closed_in_python_and_numba(monkeypatch):
    frame = _flat_daily_frame(rows=3)
    frame["ATR_14"] = 0.25  # $50,000 stop loss per lot; raw size = 0.002
    strategy = _OneShotAtrStrategy(1)
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(min_lot=0.01, volume_step=0.01)
    original_numba = bool(pm_core.NUMBA_AVAILABLE)

    for use_numba in ([False, True] if original_numba else [False]):
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="D1",
        )
        assert result["trades"] == []
        assert result["final_equity"] == pytest.approx(config.initial_capital)
        assert result["equity_curve"]
        assert all(
            value == pytest.approx(config.initial_capital)
            for value in result["equity_curve"]
        )


def test_empty_frame_returns_attested_result_without_entering_kernel(monkeypatch):
    frame = _flat_daily_frame(rows=0)
    signals = pd.Series([], index=frame.index, dtype=np.int32)
    strategy = _OneShotAtrStrategy(1)
    config = PipelineConfig(research_mode=True, swap_model_enabled=True)
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    results = []

    for use_numba in ([False, True] if original_numba else [False]):
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            signals,
            "ZZZUSD",
            strategy,
            spec=_fx_spec(swap_long=-10.0, swap_mode=-1),
            timeframe="D1",
        )
        assert result["trades"] == []
        assert result["total_trades"] == 0
        assert result["final_equity"] == pytest.approx(config.initial_capital)
        assert result["equity_curve"] == [config.initial_capital]
        assert result["execution_cost_timing_contract"] == {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        }
        assert result["swap_accrual_contract"]["status"] == "UNAVAILABLE"
        assert result["swap_accrual_contract"]["execution_state_compounding"] is False
        results.append(result)

    if original_numba:
        assert results[0] == results[1]


def test_governance_partial_close_uses_broker_executable_volumes(monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=5)
    frame.loc[frame.index[1]:, "Close"] = 1.0011
    frame.loc[frame.index[1]:, "High"] = 1.0012
    frame.loc[frame.index[1]:, "Low"] = 0.9998
    strategy = _OneShotAtrStrategy(1, tp_atr_mult=10.0)
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(min_lot=0.01, volume_step=0.01)
    result = Backtester(config).run(
        frame,
        strategy.generate_signals(frame, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
        governance_policy={
            "name": "partial_tp_runner",
            "partial_at_r": 0.5,
            "partial_close_pct": 33.0,
            "trail_atr_mult": 100.0,
            "release_tp_after_r": 100.0,
        },
    )

    assert len(result["trades"]) == 2
    partial, remainder = result["trades"]
    assert partial["exit_reason"] == "governance_partial"
    # Initial 0.50 lots * 33% = 0.165, which live execution floors to 0.16.
    assert partial["position_size"] == pytest.approx(0.16)
    assert remainder["position_size"] == pytest.approx(0.34)
    assert partial["position_size"] + remainder["position_size"] == pytest.approx(
        0.50
    )
    assert partial["risk_amount"] == pytest.approx(32.0)
    assert remainder["risk_amount"] == pytest.approx(68.0)


@pytest.mark.parametrize("direction,current_open", [(1, 1.0050), (-1, 0.9950)])
def test_governance_partial_executes_at_current_side_open_not_prior_close(
    monkeypatch,
    direction,
    current_open,
):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _gap_frame()
    if direction > 0:
        frame.loc[frame.index[1], ["High", "Close"]] = [1.0012, 1.0011]
    else:
        frame.loc[frame.index[1], ["Low", "Close"]] = [0.9988, 0.9989]
    frame.loc[frame.index[2], "Open"] = current_open
    # These deliberately extreme post-open values must not determine the
    # market execution price of a prior-close governance decision.
    frame.loc[frame.index[2], ["High", "Low", "Close"]] = [1.5000, 0.5000, 1.2500]
    strategy = _OneShotAtrStrategy(direction, tp_atr_mult=20.0)
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=False,
    )
    result = Backtester(config).run(
        frame,
        strategy.generate_signals(frame, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=_fx_spec(),
        timeframe="H1",
        governance_policy={
            "name": "partial_tp_runner",
            "partial_at_r": 0.5,
            "partial_close_pct": 50.0,
            "trail_atr_mult": 100.0,
            "release_tp_after_r": 100.0,
        },
    )

    partial = next(
        trade
        for trade in result["trades"]
        if trade["exit_reason"] == "governance_partial"
    )
    assert partial["exit_bar"] == 2
    assert partial["exit_time"] == frame.index[2]
    assert partial["exit_price"] == pytest.approx(current_open)
    assert partial["exit_price"] != pytest.approx(frame["Close"].iat[1])
    assert partial["mfe_pips"] == pytest.approx(50.0)
    assert partial["time_to_mfe_bars"] == 1


@pytest.mark.parametrize("direction,current_open", [(1, 0.9950), (-1, 1.0050)])
def test_governance_exit_now_executes_at_current_side_open_not_prior_close(
    monkeypatch,
    direction,
    current_open,
):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _gap_frame()
    frame.loc[frame.index[2], "Open"] = current_open
    frame.loc[frame.index[2], ["High", "Low", "Close"]] = [1.5000, 0.5000, 1.2500]
    strategy = _OneShotAtrStrategy(direction, tp_atr_mult=20.0)
    result = Backtester(
        PipelineConfig(
            research_mode=True,
            use_spread=False,
            use_slippage=False,
            use_commission=False,
            swap_model_enabled=False,
        )
    ).run(
        frame,
        strategy.generate_signals(frame, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=_fx_spec(),
        timeframe="H1",
        governance_policy={
            "name": "time_decay_exit",
            "max_bars_held": 1,
            "time_decay_min_favorable_r": 0.25,
        },
    )

    assert len(result["trades"]) == 1
    trade = result["trades"][0]
    assert trade["exit_reason"] == "governance_exit"
    assert trade["exit_bar"] == 2
    assert trade["exit_time"] == frame.index[2]
    assert trade["exit_price"] == pytest.approx(current_open)
    assert trade["exit_price"] != pytest.approx(frame["Close"].iat[1])
    assert trade["mae_pips"] == pytest.approx(50.0)
    assert trade["time_to_mae_bars"] == 1


@pytest.mark.parametrize("direction", [1, -1])
def test_entry_slippage_matches_fast_slow_and_python_numba(monkeypatch, direction):
    frame = _flat_daily_frame()
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=True,
        slippage_pips=1.0,
        use_commission=False,
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec()
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    engine_modes = [False, True] if original_numba else [False]
    results = {}

    for use_numba in engine_modes:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        for structural in (False, True):
            strategy = _OneShotAtrStrategy(
                direction,
                use_structural_sl=structural,
            )
            result = Backtester(config).run(
                frame,
                strategy.generate_signals(frame, "ZZZUSD"),
                "ZZZUSD",
                strategy,
                spec=spec,
                timeframe="D1",
            )
            trade = result["trades"][0]
            expected_entry = 1.0001 if direction > 0 else 0.9999
            assert trade["entry_price"] == pytest.approx(expected_entry)
            assert trade["pnl_pips"] == pytest.approx(-1.0)
            results[(use_numba, structural)] = result

        fast = results[(use_numba, False)]
        slow = results[(use_numba, True)]
        assert fast["trades"] == slow["trades"]
        assert fast["equity_curve"] == slow["equity_curve"]
        assert fast["final_equity"] == slow["final_equity"]

    if original_numba:
        for structural in (False, True):
            python = results[(False, structural)]
            numba = results[(True, structural)]
            assert python["equity_curve"] == numba["equity_curve"]
            assert python["final_equity"] == numba["final_equity"]
            for field in (
                "direction",
                "entry_price",
                "exit_price",
                "stop_loss",
                "take_profit",
                "position_size",
                "pnl_pips",
                "pnl_dollars",
                "risk_amount",
                "r_multiple",
            ):
                if field == "direction":
                    assert python["trades"][0][field] == numba["trades"][0][field]
                else:
                    assert python["trades"][0][field] == pytest.approx(
                        numba["trades"][0][field]
                    )


def test_swap_accrues_on_observable_rollover_closes_and_preserves_terminal():
    config = PipelineConfig(
        research_mode=True,
        swap_model_enabled=True,
        swap_model_triple_rollover_day="friday",  # broker metadata wins
        swap_model_min_holding_hours=0.0,
    )
    backtester = Backtester(config)
    features = _flat_daily_frame(rows=5)
    features.index = pd.date_range(
        "2026-01-05", periods=5, freq="1D", tz="UTC"
    )
    features["_BAR_CLOSE_TS"] = features.index + pd.Timedelta(days=1)
    trade = {
        "entry_time": features.index[1],  # Tuesday
        "exit_time": features.index[4],   # Friday
        "entry_bar": 1,
        "exit_bar": 4,
        "entry_price": 1.0,
        "direction": "LONG",
        "position_size": 1.0,
        "pnl_dollars": 0.0,
        "risk_amount": 100.0,
    }
    spec = _fx_spec(
        swap_long=-10.0,
        swap_short=2.0,
        swap_mode=-1,
        swap_rollover3days=3,  # MT5 Wednesday
    )

    adjusted, final_equity, max_drawdown, curve = backtester._apply_swap_model(
        [trade],
        final_equity=10000.0,
        equity_curve=[10000.0] * 5,
        max_drawdown=0.0,
        features=features,
        spec=spec,
        timeframe="D1",
    )

    # The Tuesday->Wednesday rollover is one unit; MT5's Wednesday multiplier
    # applies to Wednesday->Thursday and is therefore visible at Thursday's
    # boundary. Thursday->Friday is one more unit. The Friday-close terminal
    # retains the exact same five total units and terminal P&L.
    assert adjusted[0]["swap_rollover_units"] == 5
    assert adjusted[0]["swap_dollars"] == pytest.approx(-50.0)
    assert adjusted[0]["pnl_dollars"] == pytest.approx(-50.0)
    assert curve == pytest.approx([10000.0, 9990.0, 9960.0, 9950.0, 9950.0])
    assert final_equity == pytest.approx(9950.0)
    assert curve[-1] == pytest.approx(final_equity)
    assert max_drawdown == pytest.approx(0.5)


def test_rollover_event_is_classified_as_pre_open_across_a_session_gap():
    index = pd.DatetimeIndex(
        ["2026-04-08 20:00:00+00:00", "2026-04-09 02:00:00+00:00"]
    )
    features = pd.DataFrame(
        {
            "Open": [1.0, 1.0],
            "High": [1.0, 1.0],
            "Low": [1.0, 1.0],
            "Close": [1.0, 1.0],
            "_BAR_CLOSE_TS": pd.DatetimeIndex(
                ["2026-04-08 21:00:00+00:00", "2026-04-09 03:00:00+00:00"]
            ),
        },
        index=index,
    )
    spec = _fx_spec(
        swap_long=-10.0,
        swap_short=2.0,
        swap_mode=-1,
        swap_rollover3days=3,
    )
    pre_open_units, close_units, _long, _short, causal, contract = Backtester(
        PipelineConfig(research_mode=True, swap_model_enabled=True)
    )._build_causal_swap_inputs(
        features,
        spec,
        np.array([np.nan, 1.0], dtype=np.float64),
    )

    # The Wednesday->Thursday event is at Thursday 00:00.  The next observed
    # bar starts at 02:00, so it cannot inherit a pre-entry rollover merely
    # because its close is the first close after midnight.  The event is kept
    # separately as a pre-open cashflow for any position held across the gap.
    assert causal is True
    assert contract["status"] == "UNAVAILABLE"
    assert contract["mode"] == "legacy_calendar_diagnostic_only"
    assert pre_open_units.tolist() == [1, 3]
    assert close_units.tolist() == [0, 0]


def test_gap_carry_precedes_exit_and_replacement_sizing_in_both_engines(
    monkeypatch,
):
    frame = _gap_frame()
    frame.loc[frame.index[2], "High"] = 1.0030
    strategy = _ScheduledAtrStrategy([1, 1, 0])
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=True,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(
        min_lot=0.001,
        volume_step=0.001,
        swap_long=-10.0,
        swap_short=2.0,
        swap_mode=-1,
        swap_rollover3days=3,
    )
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    results = {}

    for use_numba in ([False, True] if original_numba else [False]):
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="H1",
        )
        assert len(result["trades"]) == 2
        carried, replacement = result["trades"]
        assert carried["position_size"] == pytest.approx(0.500)
        assert carried["swap_rollover_units"] == 3
        assert carried["swap_dollars"] == pytest.approx(-15.0)
        assert carried["gross_pnl_dollars"] == pytest.approx(100.0)
        # Carry is booked before the old trade exits: 10,000 - 15 + 100.
        # The same-bar replacement therefore risks 1% of 10,085, not 10,100.
        assert replacement["risk_amount"] == pytest.approx(100.85)
        assert replacement["position_size"] == pytest.approx(0.504)
        assert replacement["swap_rollover_units"] == 0
        results[use_numba] = result

    if original_numba:
        assert results[False]["trades"] == results[True]["trades"]
        assert results[False]["equity_curve"] == pytest.approx(
            results[True]["equity_curve"]
        )


def test_activation_bound_stress_projection_is_promotion_admissible(monkeypatch):
    from pm_broker_terms import SwapProjection

    frame = _flat_daily_frame(rows=4)
    projection = SwapProjection(
        pre_open_units=np.array([0, 3, 0, 0], dtype=np.int8),
        at_close_units=np.array([0, 0, 3, 0], dtype=np.int8),
        digest="a" * 64,
        event_count=2,
    )
    resolved = {
        "schedule_provenance_status": "STRESS_APPROVED",
        "source_provenance_authenticated": False,
        "stress_approved": True,
        "stress_policy_immutable": True,
        "stress_policy_sha256": "9" * 64,
        "price_activation_id": "price-a1",
        "price_activation_manifest_sha256": "b" * 64,
        "terms_activation_id": "terms-a1",
        "terms_activation_manifest_sha256": "c" * 64,
        "terms_snapshot_sha256": "d" * 64,
        "schedule_sha256": "e" * 64,
        "target_bars_sha256": "f" * 64,
        "target_sidecar_sha256": "1" * 64,
        "projection_contract": "fxpm-swap-preopen-close-projection-v1",
        "terms": {
            "swap_mode": -1,
            "swap_long": -10.0,
            "swap_short": -5.0,
        },
    }
    backtester = Backtester(
        PipelineConfig(research_mode=True, swap_model_enabled=True)
    )
    monkeypatch.setattr(
        backtester,
        "_resolve_broker_swap_projection",
        lambda *_args, **_kwargs: (projection, resolved),
    )

    pre, close, long_rate, short_rate, causal, contract = (
        backtester._build_causal_swap_inputs(
            frame,
            _fx_spec(swap_mode=-1),
            np.array([np.nan, 1.0, np.nan, np.nan], dtype=np.float64),
            timeframe="D1",
        )
    )

    assert causal is True
    assert pre.tolist() == [0, 3, 0, 0]
    assert close.tolist() == [0, 0, 3, 0]
    assert long_rate[1] == pytest.approx(-10.0)
    assert short_rate[1] == pytest.approx(-5.0)
    assert contract["status"] == "AVAILABLE"
    assert contract["provenance_status"] == "STRESS_APPROVED"
    assert contract["stress_policy_explicitly_approved"] is True
    assert contract["stress_policy_immutable"] is True
    assert contract["historical_rates_authenticated"] is False
    assert contract["projection_sha256"] == "a" * 64


def test_new_post_gap_entry_does_not_pay_pre_entry_carry_in_both_engines(
    monkeypatch,
):
    frame = _gap_frame()
    strategy = _ScheduledAtrStrategy([0, 1, 0])
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=True,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(
        swap_long=-10.0,
        swap_short=2.0,
        swap_mode=-1,
        swap_rollover3days=3,
    )
    original_numba = bool(pm_core.NUMBA_AVAILABLE)

    for use_numba in ([False, True] if original_numba else [False]):
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="H1",
        )
        assert len(result["trades"]) == 1
        trade = result["trades"][0]
        assert trade["entry_bar"] == 2
        assert trade["swap_rollover_units"] == 0
        assert trade["swap_dollars"] == 0.0
        assert result["final_equity"] == pytest.approx(config.initial_capital)


def test_missing_close_axis_is_explicitly_noncausal_and_unavailable(monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=3).drop(columns=["_BAR_CLOSE_TS"])
    strategy = _OneShotAtrStrategy(1)
    result = Backtester(
        PipelineConfig(
            research_mode=True,
            use_spread=False,
            use_slippage=False,
            use_commission=False,
            swap_model_enabled=True,
            swap_model_min_holding_hours=0.0,
        )
    ).run(
        frame,
        strategy.generate_signals(frame, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=_fx_spec(swap_long=-10.0, swap_mode=-1),
        timeframe="D1",
    )

    assert result["execution_cost_timing_contract"] == {
        "entry_slippage": "entry_slippage_parity_v1",
        "swap_accrual": "causal_swap_accrual_v1",
    }
    assert result["swap_accrual_contract"]["status"] == "UNAVAILABLE"
    assert result["swap_accrual_contract"]["mode"] == (
        "noncausal_post_loop_fallback"
    )
    assert result["swap_accrual_contract"]["timestamp_source"] is None
    assert result["swap_accrual_contract"]["execution_state_compounding"] is False


def test_causal_swap_changes_the_next_trade_risk_budget_and_size(monkeypatch):
    """Carry must enter live equity before a later trade is sized."""
    frame = _flat_daily_frame(rows=5)
    frame.index = pd.date_range("2026-01-05", periods=5, freq="1D", tz="UTC")
    frame["_BAR_CLOSE_TS"] = frame.index + pd.Timedelta(days=1)
    frame.loc[frame.index[3], "High"] = 1.0030  # close trade one at its TP
    strategy = _ScheduledAtrStrategy([1, 0, 0, 1, 0])
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=False,
        swap_model_enabled=True,
        swap_model_min_holding_hours=0.0,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(
        min_lot=0.001,
        volume_step=0.001,
        swap_long=-100.0,
        swap_short=100.0,
        swap_mode=-1,
        swap_rollover3days=3,  # MT5 Wednesday
    )
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    engine_modes = [False, True] if original_numba else [False]
    results = {}

    for use_numba in engine_modes:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="D1",
        )
        assert result["swap_accrual_contract"]["status"] == "UNAVAILABLE"
        assert result["swap_accrual_contract"]["mode"] == (
            "legacy_calendar_diagnostic_only"
        )
        assert result["swap_accrual_contract"][
            "source_provenance_authenticated"
        ] is False
        assert result["swap_accrual_contract"]["execution_state_compounding"] is True
        assert result["execution_cost_timing_contract"] == {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        }
        assert result["swap_accrual_contract"]["timestamp_source"] == (
            "_BAR_CLOSE_TS"
        )
        assert result["swap_accrual_contract"]["timestamp_evidence_level"] == (
            "observable_validated_column"
        )
        assert len(result["trades"]) == 2
        first, second = result["trades"]

        # Trade one risks $100 at 20 pips, so opens 0.500 lots.  It earns
        # $100 at TP but pays four rollover units * -$100 * 0.5 = -$200.
        assert first["position_size"] == pytest.approx(0.500)
        assert first["gross_pnl_dollars"] == pytest.approx(100.0)
        assert first["swap_rollover_units"] == 4
        assert first["swap_dollars"] == pytest.approx(-200.0)
        assert first["pnl_dollars"] == pytest.approx(-100.0)

        # Its carry-adjusted closing equity is $9,900, so trade two must risk
        # $99 and size at 0.495 lots.  Exit-only swap booking would incorrectly
        # size this trade from $10,100 (or otherwise omit the carry state).
        assert second["risk_amount"] == pytest.approx(99.0)
        assert second["position_size"] == pytest.approx(0.495)
        assert sum(trade["pnl_dollars"] for trade in result["trades"]) == pytest.approx(
            result["final_equity"] - config.initial_capital
        )
        assert result["equity_curve"][-1] == pytest.approx(result["final_equity"])
        results[use_numba] = result

    if original_numba:
        assert results[False]["trades"] == results[True]["trades"]
        assert results[False]["equity_curve"] == pytest.approx(
            results[True]["equity_curve"]
        )
        assert results[False]["final_equity"] == pytest.approx(
            results[True]["final_equity"]
        )


@pytest.mark.parametrize("direction", [1, -1])
def test_round_trip_commission_is_included_in_stop_risk_sizing(
    monkeypatch,
    direction,
):
    frame = _flat_daily_frame(rows=3)
    frame["ATR_14"] = 0.0025  # 50-pip stop = $500 per lot
    if direction > 0:
        frame.loc[frame.index[1], "Low"] = 0.9940
    else:
        frame.loc[frame.index[1], "High"] = 1.0060
    strategy = _OneShotAtrStrategy(direction)
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        use_slippage=False,
        use_commission=True,
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec(commission_per_lot=7.0)
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    engine_modes = [False, True] if original_numba else [False]
    results = {}

    for use_numba in engine_modes:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
        result = Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="D1",
        )
        trade = result["trades"][0]
        # $100 / ($500 stop + $7 round-trip commission) = 0.1972 lots,
        # rounded down to the broker's 0.01 step.
        assert trade["risk_amount"] == pytest.approx(100.0)
        assert trade["position_size"] == pytest.approx(0.19)
        assert trade["gross_pnl_dollars"] == pytest.approx(-96.33)
        assert abs(trade["pnl_dollars"]) <= trade["risk_amount"]
        results[use_numba] = result

    if original_numba:
        assert results[False]["trades"] == results[True]["trades"]
        assert results[False]["equity_curve"] == pytest.approx(
            results[True]["equity_curve"]
        )
