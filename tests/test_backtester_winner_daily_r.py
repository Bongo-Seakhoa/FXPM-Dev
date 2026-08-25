from __future__ import annotations

import copy
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import pm_core
from pm_core import (
    Backtester,
    InstrumentSpec,
    PipelineConfig,
    build_backtest_winner_daily_r_surface,
    canonical_backtest_winner_r,
)


class _AtrHoldStrategy:
    name = "winner-daily-r-hold"
    params = {
        "sl_atr_mult": 10.0,
        "tp_atr_mult": 10.0,
        "use_structural_sl": False,
    }


def _frame() -> pd.DataFrame:
    index = pd.date_range("2026-01-05", periods=5, freq="1D", tz="UTC")
    return pd.DataFrame(
        {
            "Open": [1.0, 1.0, 1.0010, 1.0020, 1.0030],
            "High": [1.0002, 1.0002, 1.0012, 1.0022, 1.0032],
            "Low": [0.9998, 0.9998, 1.0008, 1.0018, 1.0028],
            "Close": [1.0, 1.0001, 1.0011, 1.0021, 1.0031],
            "ATR_14": np.full(5, 0.0010),
            "_BAR_CLOSE_TS": index + pd.Timedelta(days=1),
        },
        index=index,
    )


def _spec(**overrides) -> InstrumentSpec:
    values = {
        "symbol": "ZZZUSD",
        "pip_position": 4,
        "pip_value": 10.0,
        "spread_avg": 0.0,
        "tick_size": 0.0001,
        "tick_value": 10.0,
        "point": 0.0001,
        "min_lot": 0.01,
        "volume_step": 0.01,
    }
    values.update(overrides)
    return InstrumentSpec(**values)


def _signals(frame: pd.DataFrame) -> pd.Series:
    return pd.Series([1, 0, 0, 0, 0], index=frame.index, dtype=np.int32)


def _run(
    frame: pd.DataFrame,
    *,
    config: PipelineConfig,
    spec: InstrumentSpec,
    governance_policy=None,
):
    strategy = _AtrHoldStrategy()
    return Backtester(config).run(
        frame,
        _signals(frame),
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
        governance_policy=governance_policy,
    )


def _assert_accounting(result) -> None:
    surface = result["winner_daily_r_surface"]
    assert surface["status"] == "AVAILABLE"
    daily_total = sum(
        (Decimal(str(value)) for value in surface["daily_net_mtm_r"]),
        Decimal(0),
    )
    trade_total = sum(
        (
            Decimal(
                str(
                    canonical_backtest_winner_r(
                        event["pnl_dollars"], event["risk_amount"]
                    )
                )
            )
            for event in result["position_events"]
        ),
        Decimal(0),
    )
    assert daily_total == trade_total


def test_overnight_full_path_matches_python_and_numba(monkeypatch) -> None:
    frame = _frame()
    config = PipelineConfig(
        research_mode=True,
        use_spread=False,
        swap_model_enabled=False,
        discovery_risk_per_trade_pct=1.0,
    )
    original_numba = bool(pm_core.NUMBA_AVAILABLE)
    results = []
    for enabled in ([False, True] if original_numba else [False]):
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", enabled)
        result = _run(frame, config=config, spec=_spec())
        _assert_accounting(result)
        results.append(result)

    surface = results[0]["winner_daily_r_surface"]
    assert surface["schema_version"] == "fxpm.backtester-winner-daily-r.v2"
    assert surface["risk_unit_contract"] == {
        "schema_version": "fxpm.precommitted-cell-risk-unit.v2",
        "unit": "PRECOMMITTED_CELL_RISK_UNIT",
        "position_denominator": "ENTRY_BROKER_STOP_PRICE_LOSS_DOLLARS",
        "daily_surface": "SEARCH_TIME_FULL_PATH_POSITION_NORMALIZED_NET_MTM",
        "inventory_boundary": "FLAT_TO_FLAT_LIQUIDATED_POST_COST",
        "accounting_identity": (
            "EXACT_DECIMAL_SUM_DAILY_NET_MTM_EQUALS_COMPLETED_TRADE_NET_MTM_"
            "IN_SAME_PRECOMMITTED_CELL_RISK_UNIT"
        ),
        "r_decimal_places": 12,
        "r_rounding": "ROUND_HALF_EVEN",
    }
    assert surface["daily_dates_utc"] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
    ]
    assert any(value != 0.0 for value in surface["daily_net_mtm_r"][1:-1])
    if original_numba:
        assert results[0]["winner_daily_r_surface"] == results[1][
            "winner_daily_r_surface"
        ]


def test_partial_close_reconciles_one_flat_to_flat_position(monkeypatch) -> None:
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    frame = _frame()
    result = _run(
        frame,
        config=PipelineConfig(
            research_mode=True,
            use_spread=False,
            swap_model_enabled=False,
            discovery_risk_per_trade_pct=1.0,
        ),
        spec=_spec(),
        governance_policy={
            "name": "partial_tp_runner",
            "partial_at_r": 0.1,
            "partial_close_pct": 33.0,
            "trail_atr_mult": 100.0,
            "release_tp_after_r": 100.0,
        },
    )

    assert len(result["trades"]) == 2
    assert len(result["position_events"]) == 1
    assert result["trades"][0]["exit_reason"] == "governance_partial"
    assert result["position_events"][0]["risk_amount"] == pytest.approx(100.0)
    _assert_accounting(result)


def test_spread_and_carry_are_in_daily_r() -> None:
    frame = _frame()
    common = {
        "research_mode": True,
        "swap_model_enabled": True,
        "swap_model_min_holding_hours": 0.0,
        "discovery_risk_per_trade_pct": 1.0,
    }
    base = _run(
        frame,
        config=PipelineConfig(
            **common,
            use_spread=False,
        ),
        spec=_spec(swap_long=-1.0),
    )
    stress = _run(
        frame,
        config=PipelineConfig(
            **common,
            use_spread=True,
        ),
        spec=_spec(
            spread_avg=1.0,
            swap_long=-1.0,
        ),
    )

    _assert_accounting(base)
    _assert_accounting(stress)
    assert base["trades"][0]["swap_dollars"] < 0.0
    assert stress["position_events"][0]["position_id"] == base[
        "position_events"
    ][0]["position_id"]
    assert stress["position_events"][0]["pnl_dollars"] < base[
        "position_events"
    ][0]["pnl_dollars"]
    assert sum(stress["winner_daily_r_surface"]["daily_net_mtm_r"]) < sum(
        base["winner_daily_r_surface"]["daily_net_mtm_r"]
    )


def test_frozen_path_cost_stress_uses_genuine_arrays_and_preserves_positions() -> None:
    frame = _frame().assign(
        SpreadOpen=[1, 1, 1, 1, 1],
        SpreadMax=[5, 5, 5, 5, 5],
        SpreadP95=[4, 4, 4, 4, 4],
    )
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=1.0,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec()
    backtester = Backtester(config)
    signals = _signals(frame)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    assert stress["position_events"]
    assert [
        (
            event["position_id"],
            event["entry_time"],
            event["exit_time"],
            event["risk_amount"],
        )
        for event in stress["position_events"]
    ] == [
        (
            event["position_id"],
            event["entry_time"],
            event["exit_time"],
            event["risk_amount"],
        )
        for event in base["position_events"]
    ]
    assert all(
        stressed["pnl_dollars"] <= baseline["pnl_dollars"]
        for baseline, stressed in zip(
            base["position_events"], stress["position_events"], strict=True
        )
    )
    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["path_identity_preserved"] is True
    assert receipt["stress_never_improves"] is True
    assert (
        stress["numerical_execution_cost_attestation"]["spread_values_sha256"]
        != base["numerical_execution_cost_attestation"]["spread_values_sha256"]
    )
    _assert_accounting(stress)

    tampered_event = copy.deepcopy(base)
    tampered_event["position_events"][0]["pnl_dollars"] += 0.01
    with pytest.raises(ValueError, match="position reconciliation"):
        backtester.replay_frozen_path_cost_stress(
            frame,
            signals,
            "ZZZUSD",
            strategy,
            tampered_event,
            stress_mode="stress_p95",
            spec=spec,
            timeframe="D1",
        )

    tampered_cost = copy.deepcopy(base)
    tampered_cost["numerical_execution_cost_attestation"][
        "spread_values_sha256"
    ] = "0" * 64
    with pytest.raises(ValueError, match="numerical cost attestation drift"):
        backtester.replay_frozen_path_cost_stress(
            frame,
            signals,
            "ZZZUSD",
            strategy,
            tampered_cost,
            stress_mode="stress_p95",
            spec=spec,
            timeframe="D1",
        )


def test_swap_contract_cache_is_shared_only_with_explicit_sibling() -> None:
    config = PipelineConfig(research_mode=True)
    parent = Backtester(config)
    sibling = Backtester(config)
    unrelated = Backtester(config)
    contract = {"terms_active_pointer_sha256": "a" * 64}
    parent._swap_contract_cache[("a" * 64, "EURUSD", "H1")] = contract
    parent._swap_terms_pointer_pin = "a" * 64

    parent._share_immutable_swap_cache_with(sibling)

    assert sibling._swap_contract_cache is parent._swap_contract_cache
    assert sibling._swap_terms_pointer_pin == parent._swap_terms_pointer_pin
    assert unrelated._swap_contract_cache == {}
    assert unrelated._swap_contract_cache is not parent._swap_contract_cache

    different_root = Backtester(
        PipelineConfig(research_mode=True, data_dir=Path("different-data-root"))
    )
    with pytest.raises(ValueError, match="one native root"):
        parent._share_immutable_swap_cache_with(different_root)


def test_frozen_path_cost_stress_rejects_base_from_other_signals() -> None:
    frame = _frame().assign(
        SpreadOpen=[1, 1, 1, 1, 1],
        SpreadMax=[5, 5, 5, 5, 5],
        SpreadP95=[4, 4, 4, 4, 4],
    )
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=1.0,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec()
    backtester = Backtester(config)
    supplied_signals = _signals(frame)
    other_signals = pd.Series(
        [-1, 0, 0, 0, 0],
        index=frame.index,
        dtype=np.int32,
    )
    fabricated_but_self_consistent = backtester.run(
        frame,
        other_signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    with pytest.raises(pm_core.FrozenPathCostStressError) as error:
        backtester.replay_frozen_path_cost_stress(
            frame,
            supplied_signals,
            "ZZZUSD",
            strategy,
            fabricated_but_self_consistent,
            stress_mode="stress_p95",
            spec=spec,
            timeframe="D1",
        )

    assert error.value.code == "BASE_REPLAY_MISMATCH"


def test_spread_only_frozen_path_stress_uses_closed_partition_p95() -> None:
    frame = _frame().assign(Spread=[8, 1, 1, 1, 1])
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=1.0,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec()
    backtester = Backtester(config)
    signals = _signals(frame)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    spread = stress["spread_model"]
    assert spread["stress_derivation_schema"] == (
        pm_core.BACKTEST_CLOSED_PARTITION_SPREAD_STRESS_SCHEMA_VERSION
    )
    assert spread["stress_distribution_scope"] == (
        "EXACT_CLOSED_BACKTEST_PARTITION"
    )
    assert spread["stress_distribution_statistic"] == (
        "EMPIRICAL_P95_LINEAR_ORDER_STATISTIC"
    )
    assert spread["stress_distribution_observation_count"] == len(frame)
    assert spread["stress_distribution_value_points"] == pytest.approx(6.6)
    assert stress["position_events"][0]["pnl_dollars"] < (
        base["position_events"][0]["pnl_dollars"]
    )


def test_trade_bearing_equal_cost_vectors_are_explicitly_non_adverse() -> None:
    frame = _frame().assign(Spread=[2, 2, 2, 2, 2])
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=1.0,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec()
    backtester = Backtester(config)
    signals = _signals(frame)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["schema_version"] == (
        pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION
    )
    assert receipt["realized_adversity_status"] == (
        pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    assert (
        receipt["base_spread_values_sha256"]
        == receipt["stress_spread_values_sha256"]
    )
    assert receipt["strictly_worse_trade_count"] == 0
    assert receipt["strictly_worse_daily_observation_count"] == 0
    assert receipt["stress_never_improves"] is True
    assert stress["position_events"] == base["position_events"]
    assert stress["winner_daily_r_surface"] == base["winner_daily_r_surface"]


def test_equal_swap_inputs_preserve_already_settled_base_cents() -> None:
    frame = _frame().iloc[:3].copy().assign(Spread=[2, 2, 2])
    signals = pd.Series([1, 0, 0], index=frame.index, dtype=np.int32)
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=True,
        swap_model_min_holding_hours=0.0,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=2.5,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec(swap_long=-4.095, swap_short=-4.095, swap_mode=-1)
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    assert base["trades"][0]["position_size"] == pytest.approx(0.25)
    assert base["trades"][0]["swap_rollover_units"] == 4
    assert base["trades"][0]["swap_dollars"] == -4.10

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    assert stress["trades"][0]["swap_dollars"] == -4.10
    assert stress["position_events"] == base["position_events"]
    assert stress["winner_daily_r_surface"] == base["winner_daily_r_surface"]
    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["strictly_worse_trade_count"] == 0
    assert receipt["strictly_worse_daily_observation_count"] == 0


def test_frozen_path_swap_delta_uses_settled_base_and_rejects_nonfinite() -> None:
    settle = Backtester._settle_frozen_path_stress_swap

    # This is the exact preserved Gate-2 boundary: the base path has already
    # settled NumPy's representation of -5.46 * .25 * 3 to -4.10.
    assert settle(
        base_swap=-4.10,
        rollover_units=3,
        position_size=0.25,
        base_rate=-5.46,
        stress_rate=-5.46,
    ) == -4.10
    assert settle(
        base_swap=-4.10,
        rollover_units=3,
        position_size=0.25,
        base_rate=-5.46,
        stress_rate=-5.50,
    ) == -4.13
    assert settle(
        base_swap=-4.10,
        rollover_units=3,
        position_size=0.25,
        base_rate=-5.46,
        stress_rate=-5.42,
    ) == -4.07

    for field, value in (
        ("base_swap", float("nan")),
        ("base_rate", float("inf")),
        ("stress_rate", float("-inf")),
    ):
        values = {
            "base_swap": -4.10,
            "rollover_units": 3,
            "position_size": 0.25,
            "base_rate": -5.46,
            "stress_rate": -5.50,
        }
        values[field] = value
        with pytest.raises(ValueError, match="swap counterfactual is invalid"):
            settle(**values)


@pytest.mark.parametrize(
    ("direction", "raw_swap", "must_reject_improvement"),
    (
        ("LONG", 36000.0, True),
        ("LONG", -36000.0, False),
        ("SHORT", 36000.0, False),
        ("SHORT", -36000.0, True),
    ),
)
def test_mode6_entry_notional_delta_remains_economic_and_fail_closed(
    direction: str,
    raw_swap: float,
    must_reject_improvement: bool,
) -> None:
    frame = _frame().iloc[:3].copy().assign(
        SpreadOpen=[1, 1, 1],
        SpreadMax=[5, 5, 5],
        SpreadP95=[4, 4, 4],
    )
    signals = pd.Series(
        [1 if direction == "LONG" else -1, 0, 0],
        index=frame.index,
        dtype=np.int32,
    )
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=True,
        swap_model_min_holding_hours=0.0,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=2.5,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec(
        swap_long=raw_swap,
        swap_short=raw_swap,
        swap_mode=6,
        contract_size=100000.0,
    )
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    if must_reject_improvement:
        with pytest.raises(ValueError, match="improves under stress"):
            backtester.replay_frozen_path_cost_stress(
                frame,
                signals,
                "ZZZUSD",
                strategy,
                base,
                stress_mode="stress_p95",
                spec=spec,
                timeframe="D1",
            )
        return

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )
    assert stress["trades"][0]["swap_dollars"] < base["trades"][0][
        "swap_dollars"
    ]
    assert stress["trades"][0]["pnl_dollars"] < base["trades"][0][
        "pnl_dollars"
    ]


def test_mode6_rollover_rebound_passes_cumulative_stress_rule() -> None:
    frame = _frame().iloc[:3].copy().assign(
        SpreadOpen=[1, 1, 1],
        SpreadMax=[5, 5, 5],
        SpreadP95=[4, 4, 4],
    )
    signals = pd.Series([-1, 0, 0], index=frame.index, dtype=np.int32)
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=True,
        swap_model_min_holding_hours=0.0,
        backtest_spread_mode="conservative_max",
        discovery_risk_per_trade_pct=2.5,
    )
    strategy = _AtrHoldStrategy()
    spec = _spec(
        swap_long=-36.0,
        swap_short=-36.0,
        swap_mode=6,
        contract_size=100000.0,
    )
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    base_trade = base["trades"][0]
    stress_trade = stress["trades"][0]
    assert stress_trade["entry_price"] < base_trade["entry_price"]
    assert stress_trade["swap_dollars"] > base_trade["swap_dollars"]
    assert stress_trade["pnl_dollars"] < base_trade["pnl_dollars"]

    base_daily = base["winner_daily_r_surface"]["daily_net_mtm_r"]
    stress_daily = stress["winner_daily_r_surface"]["daily_net_mtm_r"]
    improvement_count = sum(
        Decimal(str(stress_value)) > Decimal(str(base_value))
        for base_value, stress_value in zip(base_daily, stress_daily, strict=True)
    )
    assert improvement_count > 0  # The retired componentwise rule rejected this path.

    base_cumulative = Decimal(0)
    stress_cumulative = Decimal(0)
    for base_value, stress_value in zip(base_daily, stress_daily, strict=True):
        base_cumulative += Decimal(str(base_value))
        stress_cumulative += Decimal(str(stress_value))
        assert stress_cumulative <= base_cumulative + Decimal("1e-12")

    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["stress_daily_improvement_observation_count"] == improvement_count
    assert receipt["realized_adversity_status"] == (
        pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
    )


def test_changed_spread_without_realized_degradation_is_insufficient() -> None:
    frame = _frame().assign(
        SpreadOpen=[1, 1, 1, 1, 1],
        SpreadMax=[5, 5, 5, 5, 5],
        SpreadP95=[4, 4, 4, 4, 4],
    )
    signals = pd.Series(np.zeros(len(frame), dtype=np.int32), index=frame.index)
    config = PipelineConfig(
        research_mode=True,
        use_spread=True,
        swap_model_enabled=False,
        backtest_spread_mode="conservative_max",
    )
    strategy = _AtrHoldStrategy()
    spec = _spec()
    backtester = Backtester(config)
    base = backtester.run(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        spec=spec,
        timeframe="D1",
    )

    stress = backtester.replay_frozen_path_cost_stress(
        frame,
        signals,
        "ZZZUSD",
        strategy,
        base,
        stress_mode="stress_p95",
        spec=spec,
        timeframe="D1",
    )

    receipt = stress["cost_stress_frozen_path_receipt"]
    assert receipt["base_spread_values_sha256"] != receipt[
        "stress_spread_values_sha256"
    ]
    assert receipt["strictly_worse_trade_count"] == 0
    assert receipt["strictly_worse_daily_observation_count"] == 0
    assert receipt["stress_daily_improvement_observation_count"] == 0
    assert receipt["realized_adversity_status"] == (
        pm_core.BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )


def test_noncausal_swap_never_claims_full_path_daily_r() -> None:
    frame = _frame().drop(columns=["_BAR_CLOSE_TS"])
    result = _run(
        frame,
        config=PipelineConfig(research_mode=True, swap_model_enabled=True),
        spec=_spec(swap_long=-1.0),
    )

    assert result["winner_daily_r_surface"]["status"] == "UNAVAILABLE"
    assert result["winner_daily_r_surface"]["daily_net_mtm_r"] == []


def test_tampered_position_event_fails_reconciliation(monkeypatch) -> None:
    frame = _frame()
    spec = _spec()
    result = _run(
        frame,
        config=PipelineConfig(
            research_mode=True,
            use_spread=False,
            swap_model_enabled=False,
        ),
        spec=spec,
    )
    events = [dict(event) for event in result["position_events"]]
    events[0]["pnl_dollars"] += 0.01

    with pytest.raises(ValueError, match="position-event reconciliation"):
        build_backtest_winner_daily_r_surface(
            frame,
            result["trades"],
            events,
            namespace="ZZZUSD|D1|winner-daily-r-hold",
            spec=spec,
            half_spread_arr=np.zeros(len(frame)),
            use_spread=False,
            swap_model_enabled=False,
            causal_swap_enabled=False,
            pre_open_rollover_units_arr=np.zeros(len(frame)),
            rollover_units_arr=np.zeros(len(frame)),
            swap_long_per_lot_arr=np.zeros(len(frame)),
            swap_short_per_lot_arr=np.zeros(len(frame)),
        )


def test_mark_operation_bound_fails_without_approximation(monkeypatch) -> None:
    monkeypatch.setattr(pm_core, "BACKTEST_WINNER_DAILY_R_MAX_MARKS", 1)
    with pytest.raises(ValueError, match="mark-operation bound"):
        _run(
            _frame(),
            config=PipelineConfig(
                research_mode=True,
                use_spread=False,
                swap_model_enabled=False,
            ),
            spec=_spec(),
        )
