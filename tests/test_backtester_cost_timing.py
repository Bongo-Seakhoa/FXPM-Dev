"""Execution-cost timing contracts used by winner statistics."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

import pm_core
from pm_core import Backtester, InstrumentSpec, PipelineConfig, StrategyCategory
from pm_strategies import BaseStrategy
from pm_trade_power import build_numerical_execution_cost_attestation
from pm_spread_profile import load_spread_profile


def _frozen_daily_tier():
    # Persisted cost values on synthetic frames; no price or profit evidence.
    return load_spread_profile().resolve(
        "EURUSD", "D1", "P75", point=0.00001,
        source_identity_sha256="0be6d0831806c1a62ab723193a87a362772a14d8bf4ccbe2d3a8774962eefe09",
    )


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


@pytest.mark.parametrize(
    "mutated_mode",
    ["stress_p90", "stress_p95", "stress_max", "wishful"],
)
def test_runtime_mutated_spread_mode_fails_before_spread_early_returns(mutated_mode):
    config = PipelineConfig(use_spread=False)
    config.backtest_spread_mode = mutated_mode

    with pytest.raises(ValueError, match="invalid backtest_spread_mode at execution"):
        Backtester(config)._build_half_spread_arrays(
            pd.DataFrame(),
            _fx_spec(),
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
            "swap_accrual": "causal_swap_accrual_v1",
        }
        assert result["swap_accrual_contract"]["status"] == "UNAVAILABLE"
        assert result["swap_accrual_contract"]["execution_state_compounding"] is False
        attestation = result["numerical_execution_cost_attestation"]
        assert attestation["schema_version"] == (
            "fxpm-numerical-execution-cost-v3"
        )
        assert len(attestation["policy_sha256"]) == 64
        assert len(attestation["attestation_sha256"]) == 64
        assert attestation["entry_half_spread_values_sha256"] == (
            Backtester._spread_array_sha256(
                "entry_half_spread", np.empty(0, dtype=np.float64)
            )
        )
        assert attestation["intrabar_half_spread_values_sha256"] == (
            Backtester._spread_array_sha256(
                "intrabar_half_spread", np.empty(0, dtype=np.float64)
            )
        )
        results.append(result)

    if original_numba:
        assert results[0] == results[1]


def test_backtester_seals_exact_spread_arrays_and_cost_inputs(monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=4)
    frame["SpreadOpen"] = [22.0, 23.0, 24.0, 25.0]
    frame["SpreadMax"] = [24.0, 25.0, 26.0, 27.0]
    strategy = _OneShotAtrStrategy(1)
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        backtest_spread_mode="conservative_max",
        swap_model_enabled=False,
    )
    spec = _fx_spec(
        symbol="EURUSD",
        spread_avg=1.0,
        point=0.00001,
    )
    backtester = Backtester(config)
    result = backtester.run(
        frame,
        strategy.generate_signals(frame, "EURUSD"),
        "EURUSD",
        strategy,
        spec=spec,
        timeframe="D1",
        spread_tier=_frozen_daily_tier(),
    )
    entry_half_spread, intrabar_half_spread, _ = (
        backtester._build_half_spread_arrays(
            frame, spec, spread_tier=_frozen_daily_tier(), timeframe="D1"
        )
    )
    attestation = result["numerical_execution_cost_attestation"]

    assert attestation["use_spread"] is True
    assert set(attestation) == {
        "schema_version",
        "use_spread",
        "spread_mode",
        "entry_half_spread_values_sha256",
        "intrabar_half_spread_values_sha256",
        "instrument_spec_sha256",
        "policy_sha256",
        "attestation_sha256",
    }
    assert attestation["entry_half_spread_values_sha256"] == (
        Backtester._spread_array_sha256(
            "entry_half_spread", entry_half_spread
        )
    )
    assert attestation["intrabar_half_spread_values_sha256"] == (
        Backtester._spread_array_sha256(
            "intrabar_half_spread", intrabar_half_spread
        )
    )
    spread_policy = json.loads(attestation["spread_mode"])
    assert spread_policy["mode"] == "conservative_max"
    assert spread_policy["entry_column"] == "SpreadOpen"
    assert spread_policy["bar_column"] == "SpreadMax"
    expected = build_numerical_execution_cost_attestation(
        use_spread=True,
        spread_mode=attestation["spread_mode"],
        entry_half_spread_values_sha256=attestation[
            "entry_half_spread_values_sha256"
        ],
        intrabar_half_spread_values_sha256=attestation[
            "intrabar_half_spread_values_sha256"
        ],
        instrument_spec_sha256=attestation["instrument_spec_sha256"],
    )
    assert attestation == expected


def test_cost_policy_is_stable_but_exact_spread_attestation_is_slice_local(
    monkeypatch,
):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=8)
    frame["SpreadOpen"] = np.arange(22.0, 30.0)
    frame["SpreadMax"] = np.arange(23.0, 31.0)
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        backtest_spread_mode="conservative_max",
        swap_model_enabled=False,
    )
    spec = _fx_spec(symbol="EURUSD", spread_avg=1.0, point=0.00001)
    attestations = []
    for subset in (frame.iloc[:4].copy(), frame.iloc[4:].copy()):
        strategy = _OneShotAtrStrategy(1)
        result = Backtester(config).run(
            subset,
            strategy.generate_signals(subset, "EURUSD"),
            "EURUSD",
            strategy,
            spec=spec,
            timeframe="D1",
            spread_tier=_frozen_daily_tier(),
        )
        attestations.append(result["numerical_execution_cost_attestation"])

    first, second = attestations
    assert first["policy_sha256"] == second["policy_sha256"]
    assert first["entry_half_spread_values_sha256"] != second[
        "entry_half_spread_values_sha256"
    ]
    assert first["intrabar_half_spread_values_sha256"] != second[
        "intrabar_half_spread_values_sha256"
    ]
    assert first["attestation_sha256"] != second["attestation_sha256"]


def test_cost_policy_binds_instrument_spec(monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=3)
    strategy = _OneShotAtrStrategy(1)

    def attestation(*, tick_value):
        config = PipelineConfig(
            research_mode=True,
            use_spread=True,
            swap_model_enabled=False,
        )
        spec = _fx_spec(
            spread_avg=1.0,
            tick_value=tick_value,
        )
        return Backtester(config).run(
            frame,
            strategy.generate_signals(frame, "ZZZUSD"),
            "ZZZUSD",
            strategy,
            spec=spec,
            timeframe="D1",
        )["numerical_execution_cost_attestation"]

    baseline = attestation(tick_value=10.0)
    changed_spec = attestation(tick_value=11.0)

    assert baseline["policy_sha256"] != changed_spec["policy_sha256"]


def test_disabled_research_costs_are_attested_honestly(monkeypatch):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _flat_daily_frame(rows=3)
    strategy = _OneShotAtrStrategy(1)
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        swap_model_enabled=False,
    )
    result = Backtester(config).run(
        frame,
        strategy.generate_signals(frame, "ZZZUSD"),
        "ZZZUSD",
        strategy,
        spec=_fx_spec(),
        timeframe="D1",
    )
    attestation = result["numerical_execution_cost_attestation"]

    assert attestation["use_spread"] is False


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
def test_entry_price_has_no_configured_slippage_in_python_and_numba(
    monkeypatch,
    direction,
):
    frame = _flat_daily_frame()
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
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
            assert trade["entry_price"] == pytest.approx(1.0)
            assert trade["pnl_pips"] == pytest.approx(0.0)
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


@pytest.mark.parametrize("use_numba", [False, True])
@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("exit_kind", ["stop", "target"])
def test_intrabar_incumbent_exit_cannot_admit_earlier_open_replacement(
    monkeypatch, use_numba, direction, exit_kind,
):
    if use_numba and not pm_core.NUMBA_AVAILABLE:
        pytest.skip("Numba unavailable")
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", use_numba)
    index = pd.date_range("2026-05-11", periods=4, freq="1h", tz="UTC")
    frame = pd.DataFrame(
        {
            "Open": np.ones(4), "High": np.full(4, 1.0002),
            "Low": np.full(4, 0.9998), "Close": np.ones(4),
            "ATR_14": np.full(4, 0.001),
            "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
        }, index=index,
    )
    if (direction == 1) == (exit_kind == "stop"):
        frame.loc[index[2], "Low"] = 0.9970
    else:
        frame.loc[index[2], "High"] = 1.0030
    strategy = _ScheduledAtrStrategy([direction, -direction, direction, 0])
    result = Backtester(PipelineConfig(
        research_mode=True, use_spread=True, swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )).run(frame, strategy.generate_signals(frame, "ZZZUSD"), "ZZZUSD",
           strategy, spec=_fx_spec(spread_avg=2.0), timeframe="H1")
    assert [trade["entry_bar"] for trade in result["trades"]] == [1, 3]
    incumbent, replacement = result["trades"]
    assert incumbent["exit_bar"] == 2
    assert incumbent["exit_reason"] == ("closed_sl" if exit_kind == "stop" else "closed_tp")
    assert replacement["signal_bar"] == 2
    assert replacement["risk_amount"] == pytest.approx(
        (10_000.0 + incumbent["pnl_dollars"]) * 0.01,
    )


def test_gap_carry_precedes_exit_and_replacement_sizing_in_both_engines(
    monkeypatch,
):
    frame = _gap_frame()
    frame.loc[frame.index[2], ["Open", "High", "Low", "Close"]] = [
        0.9970, 0.9972, 0.9968, 0.9970,
    ]
    strategy = _ScheduledAtrStrategy([1, 1, 0])
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
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
        assert carried["exit_reason"] == "closed_sl"
        assert carried["exit_price"] == pytest.approx(frame["Open"].iat[2])
        assert carried["gross_pnl_dollars"] == pytest.approx(-150.0)
        # The known gap-open stop settles after carry: 10,000 - 15 - 150.
        assert replacement["entry_bar"] == carried["exit_bar"] == 2
        assert replacement["entry_price"] == pytest.approx(frame["Open"].iat[2])
        assert replacement["risk_amount"] == pytest.approx(98.35)
        assert replacement["position_size"] == pytest.approx(0.491)
        assert replacement["swap_rollover_units"] == 0
        results[use_numba] = result

    if original_numba:
        assert results[False]["trades"] == results[True]["trades"]
        assert results[False]["equity_curve"] == pytest.approx(
            results[True]["equity_curve"]
        )


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("scenario", ["gap_stop", "non_deferral", "tp_gap", "partial_then_stop"])
def test_open_entry_ownership_and_exact_backend_path(monkeypatch, direction, scenario):
    """Pin short gap eligibility (core1586/9997) and intrabar intent ownership."""
    index = pd.date_range("2026-05-11", periods=4, freq="1h", tz="UTC")
    frame = pd.DataFrame({
        "Open": np.ones(4), "High": np.full(4, 1.0002),
        "Low": np.full(4, 0.9998), "Close": np.ones(4),
        "ATR_14": np.full(4, 0.001),
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
    }, index=index)
    policy = None
    if scenario == "gap_stop":
        price = 1.0 - direction * 0.003
        frame.loc[index[2], ["Open", "High", "Low", "Close"]] = [price, price + .0002, price - .0002, price]
    elif scenario == "tp_gap":
        price = 1.0 + direction * .003
        frame.loc[index[2], ["Open", "High", "Low", "Close"]] = [price, price + .0002, price - .0002, price]
    else:
        frame.loc[index[2], "Low" if direction == 1 else "High"] = 1.0 - direction * .003
        if scenario == "partial_then_stop":
            # Prior-close profit causes an Open partial; the remainder stops later.
            frame.loc[index[1], "Close"] = 1.0 + direction * .0011
            frame.loc[index[1], "High" if direction == 1 else "Low"] = 1.0 + direction * .0012
            frame.loc[index[2], "Open"] = 1.0 + direction * .001
            policy = {"name": "partial_tp_runner", "partial_at_r": .5,
                      "partial_close_pct": 50., "trail_atr_mult": 100.,
                      "release_tp_after_r": 100.}
    # No fresh signal after the suppressed intent: it must never be deferred.
    strategy = _ScheduledAtrStrategy([direction, -direction, 0, 0])
    config = PipelineConfig(research_mode=True, use_spread=False,
                            swap_model_enabled=False, discovery_risk_per_trade_pct=1.)
    modes = [False, True] if pm_core.NUMBA_AVAILABLE else [False]
    results = []
    for mode in modes:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", mode)
        result = Backtester(config).run(
            frame, strategy.generate_signals(frame, "ZZZUSD"), "ZZZUSD", strategy,
            spec=_fx_spec(), timeframe="H1", governance_policy=policy)
        trades = result["trades"]
        if scenario == "gap_stop":
            assert [trade["entry_bar"] for trade in trades] == [1, 2]
            assert trades[0]["exit_price"] == frame["Open"].iat[2]
        elif scenario == "partial_then_stop":
            assert [trade["entry_bar"] for trade in trades] == [1, 1]
            assert [trade["exit_reason"] for trade in trades] == ["governance_partial", "closed_sl"]
            assert all(trade["exit_bar"] == 2 for trade in trades)
        else:
            assert len(trades) == 1
            assert trades[0]["entry_bar"] == 1
            assert trades[0]["exit_bar"] == 2
            assert trades[0]["exit_reason"] == ("closed_tp" if scenario == "tp_gap" else "closed_sl")
            if scenario == "tp_gap":
                assert trades[0]["exit_price"] == pytest.approx(1.0 + direction * .002)
        results.append(result)
    if len(results) == 2:
        assert results[0]["trades"] == results[1]["trades"]
        assert results[0]["equity_curve"] == results[1]["equity_curve"]
        assert results[0]["equity_growth"] == results[1]["equity_growth"]


@pytest.mark.parametrize("direction", [1, -1])
def test_prior_bar_governance_open_exit_can_admit_open_replacement(
    monkeypatch, direction,
):
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    index = pd.date_range("2026-05-11", periods=4, freq="1h", tz="UTC")
    frame = pd.DataFrame({
        "Open": [1.0, 1.0, 1.0 + direction * 0.0005, 1.0],
        "High": [1.0002, 1.0002, 1.0008, 1.0002],
        "Low": [0.9998, 0.9998, 0.9992, 0.9998],
        "Close": np.ones(4), "ATR_14": np.full(4, 0.001),
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
    }, index=index)
    strategy = _ScheduledAtrStrategy([direction, direction, 0, 0])
    result = Backtester(PipelineConfig(
        research_mode=True, use_spread=True, swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )).run(frame, strategy.generate_signals(frame, "ZZZUSD"), "ZZZUSD",
           strategy, spec=_fx_spec(spread_avg=2.0), timeframe="H1",
           governance_policy={
               "name": "time_decay_exit", "max_bars_held": 1,
               "time_decay_min_favorable_r": 0.25,
           })
    assert [trade["entry_bar"] for trade in result["trades"]] == [1, 2]
    incumbent, replacement = result["trades"]
    assert incumbent["exit_reason"] == "governance_exit"
    assert incumbent["exit_bar"] == replacement["entry_bar"] == 2
    assert incumbent["exit_price"] == pytest.approx(1.0 + direction * 0.0004)
    assert replacement["signal_bar"] == 1
    assert replacement["risk_amount"] == pytest.approx(
        (10_000.0 + incumbent["pnl_dollars"]) * 0.01,
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
    assert contract["price_activation_id"] == "price-a1"
    assert contract["price_activation_manifest_sha256"] == "b" * 64
    assert contract["terms_activation_id"] == "terms-a1"
    assert contract["terms_activation_manifest_sha256"] == "c" * 64
    assert contract["terms_snapshot_sha256"] == "d" * 64
    assert contract["schedule_sha256"] == "e" * 64
    assert contract["target_bars_sha256"] == "f" * 64
    assert contract["target_sidecar_sha256"] == "1" * 64
    assert contract["projection_sha256"] == "a" * 64


def test_broker_terms_pointer_race_rejects_before_cache(tmp_path, monkeypatch):
    frame = _flat_daily_frame(rows=2)
    frame.attrs[pm_core.NATIVE_SOURCE_BINDING_ATTR] = {
        "symbol": "ZZZUSD",
        "timeframe": "D1",
        "activation_id": "price-a1",
        "activation_manifest_sha256": "c" * 64,
        "bars_sha256": "d" * 64,
        "sidecar_sha256": "e" * 64,
    }
    pointer_path = tmp_path / "native" / "terms" / "ACTIVE.json"
    pointer_path.parent.mkdir(parents=True)
    pointer_path.write_text("{}", encoding="ascii")
    observed_pointer_sha = "a" * 64
    resolved_pointer_sha = "b" * 64
    monkeypatch.setattr(
        pm_core,
        "broker_terms_sha256_file",
        lambda _path: observed_pointer_sha,
    )
    monkeypatch.setattr(
        pm_core,
        "resolve_active_swap_contract",
        lambda *_args: {
            "terms_active_pointer_sha256": resolved_pointer_sha,
        },
    )
    backtester = Backtester(
        PipelineConfig(data_dir=tmp_path, research_mode=True)
    )

    with pytest.raises(
        pm_core.BrokerTermsError,
        match="changed during swap contract resolution",
    ):
        backtester._resolve_broker_swap_projection(
            frame,
            _fx_spec(),
            "D1",
        )

    assert backtester._swap_contract_cache == {}


def test_new_post_gap_entry_does_not_pay_pre_entry_carry_in_both_engines(
    monkeypatch,
):
    frame = _gap_frame()
    strategy = _ScheduledAtrStrategy([0, 1, 0])
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
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
def test_stop_risk_sizing_is_price_only(
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
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    spec = _fx_spec()
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
        # $100 / $500 stop loss per lot = 0.20 lots exactly. Research sizing
        # does not invent a configured round-trip commission.
        assert trade["risk_amount"] == pytest.approx(100.0)
        assert trade["position_size"] == pytest.approx(0.20)
        assert trade["gross_pnl_dollars"] == pytest.approx(-100.0)
        assert abs(trade["pnl_dollars"]) <= trade["risk_amount"]
        results[use_numba] = result

    if original_numba:
        assert results[False]["trades"] == results[True]["trades"]
        assert results[False]["equity_curve"] == pytest.approx(
            results[True]["equity_curve"]
        )


def _continuous_timing_semantic_bits(value):
    if isinstance(value, (float, np.floating)):
        return ("float64", float(value).hex())
    if isinstance(value, np.ndarray):
        return (
            "array", value.dtype.str, value.shape,
            tuple(_continuous_timing_semantic_bits(x) for x in value.flat),
        )
    if isinstance(value, dict):
        return tuple(
            (key, _continuous_timing_semantic_bits(value[key]))
            for key in sorted(value)
        )
    if isinstance(value, (list, tuple)):
        return tuple(_continuous_timing_semantic_bits(x) for x in value)
    return value


@pytest.mark.parametrize("case_id", [
    "N16_PREOPEN_SWAP_THEN_OLD_GAP_EXIT_THEN_NEW_SIZE",
    "N17_CAUSAL_SWAP_OWNER_CHANGE_AND_WEEKEND",
    "N18_NEW_BOUNDARY_ENTRY_NOT_CHARGED_PREOPEN_HISTORY",
])
def test_continuous_account_causal_carry_goldens(monkeypatch, case_id):
    from test_frozen_spread_execution import (
        _assert_continuous_expected,
        _continuous_case,
    )

    assert pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    results = []
    for compiled in (False, True):
        backtester, kwargs, expected = _continuous_case(case_id, compiled=compiled)
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
        result = backtester.run(**kwargs)
        _assert_continuous_expected(result, expected)
        terminal = result["terminal"]
        assert float(result["final_equity"]).hex() == float(
            terminal["cash"] + terminal["unrealized_gross"]
        ).hex()
        assert float(result["equity_curve"][-1]).hex() == float(
            result["final_equity"]
        ).hex()
        position = terminal["open_position"]
        assert position is not None
        if case_id.startswith("N16_"):
            assert position["lots"] == 1.5
            assert position["posted_swap_cash"] == 0.0
            assert position["posted_swap_units"] == 0
        elif case_id.startswith("N17_"):
            assert position["posted_swap_cash"] == -5.0
            assert position["posted_swap_units"] == 2
            assert position["latched_rate_per_lot"] == -1.0
        else:
            assert position["entry_bar"] == 2
            assert position["posted_swap_cash"] == 0.0
            assert position["posted_swap_units"] == 0
        results.append(result)
    assert _continuous_timing_semantic_bits(results[0]) == (
        _continuous_timing_semantic_bits(results[1])
    )


def _continuous_flat_consumer(kind):
    unit_spec = _fx_spec(
        pip_position=0, pip_value=1.0, point=1.0, tick_size=1.0,
        tick_value=1.0, min_lot=0.5, volume_step=0.5,
    )
    empty = np.zeros(0, dtype=np.float64)
    if kind == "equity_growth":
        return lambda **kwargs: pm_core.build_backtest_equity_growth(
            pd.DataFrame(), [], initial_equity=10000.0, **kwargs,
        )
    if kind == "daily_r":
        return lambda **kwargs: pm_core.build_backtest_winner_daily_r_surface(
            pd.DataFrame(), [], [], namespace="synthetic-continuous-refusal",
            spec=unit_spec, half_spread_arr=empty, use_spread=True,
            swap_model_enabled=False, causal_swap_enabled=False,
            pre_open_rollover_units_arr=empty, rollover_units_arr=empty,
            swap_long_per_lot_arr=empty, swap_short_per_lot_arr=empty,
            **kwargs,
        )
    if kind == "position_events":
        return lambda **kwargs: pm_core.build_backtest_position_events(
            [], namespace="synthetic-continuous-refusal", **kwargs,
        )
    assert kind == "cost_attestation"
    backtester = Backtester(PipelineConfig(
        initial_capital=10000.0, research_mode=True,
        use_spread=True, swap_model_enabled=False,
    ))
    return lambda **kwargs: backtester._numerical_execution_cost_attestation(
        entry_half_spread=empty, intrabar_half_spread=empty,
        spread_meta={"mode": "fixed_spec", "source": "synthetic"},
        spec=unit_spec, **kwargs,
    )


@pytest.mark.parametrize("kind", [
    "equity_growth", "daily_r", "position_events", "cost_attestation",
])
def test_continuous_account_flat_consumers_refuse_mark_open_and_keep_defaults(kind):
    consume = _continuous_flat_consumer(kind)
    omitted = consume()
    explicit = consume(terminal_policy="LIQUIDATE")
    assert json.dumps(omitted, sort_keys=True, allow_nan=False).encode("ascii") == (
        json.dumps(explicit, sort_keys=True, allow_nan=False).encode("ascii")
    )
    with pytest.raises(pm_core.ContinuousAccountExecutionError) as caught:
        consume(terminal_policy="MARK_OPEN")
    assert caught.value.code == "CONTINUOUS_ACCOUNT_TERMINAL_SCHEMA_UNSUPPORTED"


@pytest.mark.parametrize("compiled", [False, True])
def test_continuous_account_never_calls_flat_only_builders(monkeypatch, compiled):
    from test_frozen_spread_execution import (
        _assert_continuous_expected,
        _continuous_case,
    )

    assert pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    backtester, kwargs, expected = _continuous_case(
        "N01_P75_LONG_OPEN_ONLY", compiled=compiled,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("MARK_OPEN called a flat-to-flat evidence builder")

    for name in (
        "build_backtest_equity_growth",
        "build_backtest_winner_daily_r_surface",
        "build_backtest_position_events",
        "build_numerical_execution_cost_attestation",
    ):
        monkeypatch.setattr(pm_core, name, forbidden)
    monkeypatch.setattr(Backtester, "_numerical_execution_cost_attestation", forbidden)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
    result = backtester.run(**kwargs)
    _assert_continuous_expected(result, expected)
    for name in (
        "equity_growth", "winner_daily_r_surface", "position_events",
        "numerical_execution_cost_attestation",
    ):
        assert name not in result


def test_continuous_R01_closed_tp_then_open_matches_legacy_path(monkeypatch):
    from test_frozen_spread_execution import _continuous_case

    assert pm_core.NUMBA_AVAILABLE, "Both execution backends are required"
    common_results = []
    for compiled in (False, True):
        template, kwargs, _ = _continuous_case("N01_P75_LONG_OPEN_ONLY")
        features = kwargs["features"].copy()
        features.loc[features.index[2], "High"] = 112.0
        signals = pd.Series([1, 0, 1, 0, 0], index=features.index, dtype=np.int32)
        original = kwargs["entry_schedule"].owners[0]
        owner = pm_core.ContinuousEntryOwner(
            "R01-owner", original.strategy, features.copy(), signals,
            original.spread_tier,
        )
        slot = pm_core.ContinuousEntrySlot(
            "R01-slot", features.index[1], features["_BAR_CLOSE_TS"].iat[-1],
            owner.owner_id,
        )
        kwargs.update(
            features=features,
            entry_schedule=pm_core.ContinuousEntrySchedule((owner,), (slot,)),
        )
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
        scheduled = Backtester(template.config).run(**kwargs)
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.update(
            signals=signals, strategy=owner.strategy,
            spread_tier=owner.spread_tier,
        )
        legacy_kwargs.pop("entry_schedule")
        legacy_kwargs.pop("terminal_policy")
        legacy = Backtester(template.config).run(**legacy_kwargs)

        expected_curve = [10000.0, 9995.0, 10025.0, 10020.0, 10020.0]
        assert _continuous_timing_semantic_bits(scheduled["equity_curve"]) == (
            _continuous_timing_semantic_bits(expected_curve)
        )
        assert _continuous_timing_semantic_bits(scheduled["equity_curve"]) == (
            _continuous_timing_semantic_bits(legacy["equity_curve"])
        )
        assert len(legacy["trades"]) == 2
        assert legacy["trades"][0]["exit_reason"] == pm_core.TradeStatus.CLOSED_TP.value
        eod = legacy["trades"][-1]
        assert eod["exit_reason"] == pm_core.TradeStatus.CLOSED_EOD.value
        assert eod["exit_bar"] == len(features) - 1
        assert len(scheduled["trades"]) == 1
        legacy_trade = legacy["trades"][0]
        scheduled_trade = scheduled["trades"][0]
        common_trade_fields = {
            "signal_bar", "entry_bar", "exit_bar", "signal_time", "entry_time",
            "exit_time", "direction", "entry_price", "exit_price", "stop_loss",
            "take_profit", "position_size", "pnl_pips", "gross_pnl_dollars",
            "swap_rollover_units", "swap_dollars", "pnl_dollars", "exit_reason",
            "risk_amount", "r_multiple", "trade_record_schema_version",
            "mae_pips", "mfe_pips", "mae_r", "mfe_r", "time_to_mae_bars",
            "time_to_mfe_bars", "time_underwater_bars", "entry_to_stop_pips",
            "entry_to_target_pips", "governance_policy", "tp_released",
        }
        # E2148 leaves admission provenance to v4; these schemas differ only
        # by the legacy admission stamp and the continuous owner/slot/tier.
        assert set(legacy_trade) == common_trade_fields | {"entry_spread_admission"}
        assert set(scheduled_trade) == common_trade_fields | {
            "owner_id", "slot_id", "frozen_spread_tier",
        }
        assert legacy_trade["entry_spread_admission"] == "ADMIT"
        assert scheduled_trade["owner_id"] == owner.owner_id
        assert scheduled_trade["slot_id"] == slot.slot_id
        assert scheduled_trade["frozen_spread_tier"] == owner.spread_tier.descriptor()
        for key in sorted(common_trade_fields):
            assert _continuous_timing_semantic_bits(scheduled_trade[key]) == (
                _continuous_timing_semantic_bits(legacy_trade[key])
            )
        terminal = scheduled["terminal"]
        position = terminal["open_position"]
        assert position is not None and position["direction"] == 1
        assert position["entry_bar"] == 3 and position["lots"] == 2.5
        assert position["entry_price"] == 101.0
        assert terminal["bid"] == 99.0 and terminal["ask"] == 101.0
        assert eod["entry_bar"] == position["entry_bar"]
        assert eod["position_size"] == position["lots"]
        assert eod["entry_price"] == position["entry_price"]
        assert eod["exit_price"] == terminal["bid"]
        assert eod["gross_pnl_dollars"] == -5.0
        assert terminal["cash"] == 10025.0
        assert terminal["unrealized_gross"] == -5.0
        assert scheduled["final_equity"] == legacy["final_equity"] == 10020.0
        assert legacy["final_equity"] - eod["pnl_dollars"] == terminal["cash"]
        assert terminal["cash"] + terminal["unrealized_gross"] == scheduled["final_equity"]
        assert not any(
            trade["exit_reason"] == pm_core.TradeStatus.CLOSED_EOD.value
            for trade in scheduled["trades"]
        )
        common_results.append(scheduled)
    assert _continuous_timing_semantic_bits(common_results[0]) == (
        _continuous_timing_semantic_bits(common_results[1])
    )


def test_continuous_real_calendar_carry_precedes_gap_and_new_entry(monkeypatch):
    from dataclasses import replace
    from test_frozen_spread_execution import _continuous_case

    assert pm_core.NUMBA_AVAILABLE, "Both execution backends are required"

    def native_terms_forbidden(*args, **kwargs):
        raise AssertionError("Synthetic calendar case accessed native financing")

    monkeypatch.setattr(pm_core, "resolve_active_swap_contract", native_terms_forbidden)
    monkeypatch.setattr(pm_core, "broker_terms_sha256_file", native_terms_forbidden)
    results = []
    for compiled in (False, True):
        template, kwargs, _ = _continuous_case(
            "N16_PREOPEN_SWAP_THEN_OLD_GAP_EXIT_THEN_NEW_SIZE",
        )
        index = pd.DatetimeIndex([
            pd.Timestamp("2026-04-08T19:00:00Z"),
            pd.Timestamp("2026-04-08T20:00:00Z"),
            pd.Timestamp("2026-04-09T02:00:00Z"),
            pd.Timestamp("2026-04-09T03:00:00Z"),
            pd.Timestamp("2026-04-09T04:00:00Z"),
        ])
        features = kwargs["features"].copy()
        features.index = index
        features["_BAR_CLOSE_TS"] = index + pd.Timedelta(hours=1)
        assert features.attrs == {}
        owners = tuple(
            pm_core.ContinuousEntryOwner(
                owner.owner_id, owner.strategy, features.copy(),
                pd.Series(owner.signals.to_numpy(copy=True), index=index, dtype=np.int32),
                owner.spread_tier,
            )
            for owner in kwargs["entry_schedule"].owners
        )
        slots = (
            pm_core.ContinuousEntrySlot("calendar-S1", index[1], index[2], "A"),
            pm_core.ContinuousEntrySlot(
                "calendar-S2", index[2], features["_BAR_CLOSE_TS"].iat[-1], "B",
            ),
        )
        kwargs.update(
            features=features,
            entry_schedule=pm_core.ContinuousEntrySchedule(owners, slots),
            entry_spread_observations=pd.Series(2.0, index=index),
            spec=replace(
                kwargs["spec"], swap_long=-1.0, swap_short=-1.0,
                swap_mode=-1, swap_rollover3days=3,
            ),
        )
        config = replace(template.config, swap_model_min_holding_hours=0.0)
        # Rebuilding drops the factory's instance-only synthetic projection.
        backtester = Backtester(config)
        assert backtester._build_causal_swap_inputs.__func__ is Backtester._build_causal_swap_inputs
        assert backtester._research_swap_context is None
        assert backtester._swap_contract_cache == {}
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", compiled)
        result = backtester.run(**kwargs)
        contract = result["swap_accrual_contract"]
        assert contract["status"] == "UNAVAILABLE"
        assert contract["mode"] == "legacy_calendar_diagnostic_only"
        assert contract["provenance_status"] == "LEGACY_UNTRUSTED"
        assert contract["source_provenance_authenticated"] is False
        assert contract["execution_state_compounding"] is True
        assert contract["pre_open_rollover_event_bars"] == 2
        assert contract["close_rollover_event_bars"] == 0
        assert contract["rollover_units_on_axis"] == 4
        assert backtester._swap_contract_cache == {}

        # Warmup's Tuesday unit is unheld; Wednesday's three units charge A
        # before the gap stop, then B sizes from 10000-7.5-30=9962.5.
        assert len(result["trades"]) == 1
        closed = result["trades"][0]
        assert closed["swap_rollover_units"] == 3
        assert closed["swap_dollars"] == -7.5
        assert closed["gross_pnl_dollars"] == -30.0
        assert closed["pnl_dollars"] == -37.5
        assert closed["exit_price"] == 89.0
        terminal = result["terminal"]
        position = terminal["open_position"]
        assert position is not None and position["owner_id"] == "B"
        assert position["direction"] == -1
        assert position["entry_bar"] == closed["exit_bar"] == 2
        assert position["entry_price"] == 88.0
        assert position["requested_risk_amount"] == 24.90625
        assert position["lots"] == 1.5
        assert position["posted_swap_cash"] == 0.0
        assert position["posted_swap_units"] == 0
        assert position["latched_rate_per_lot"] == -1.0
        assert terminal["cash"] == 9962.5
        assert terminal["unrealized_gross"] == -6.0
        assert result["final_equity"] == 9956.5
        assert terminal["cash"] + terminal["unrealized_gross"] == result["final_equity"]
        assert _continuous_timing_semantic_bits(result["equity_curve"]) == (
            _continuous_timing_semantic_bits([
                10000.0, 9995.0, 9956.5, 9956.5, 9956.5,
            ])
        )
        assert result["qualification_authority"] is False
        results.append(result)
    assert _continuous_timing_semantic_bits(results[0]) == (
        _continuous_timing_semantic_bits(results[1])
    )
