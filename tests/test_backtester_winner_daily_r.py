from __future__ import annotations

import copy
import json
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

EXPECTED_RISK_UNIT_CONTRACT = {
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


def _signals(frame: pd.DataFrame, direction: int = 1) -> pd.Series:
    values = np.zeros(len(frame), dtype=np.int32)
    values[0] = direction
    return pd.Series(values, index=frame.index, dtype=np.int32)


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
    position_total = sum(
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
    assert daily_total == position_total


def test_canonical_winner_r_normalizes_quantized_zero_sign() -> None:
    risk_contract = pm_core.build_backtest_winner_risk_unit_contract()
    assert risk_contract == EXPECTED_RISK_UNIT_CONTRACT
    assert pm_core._BACKTEST_WINNER_R_QUANTUM == Decimal(1).scaleb(
        -risk_contract["r_decimal_places"]
    )
    result = canonical_backtest_winner_r("-0.0000000000004", "1")

    assert result == 0.0
    assert not np.signbit(result)
    assert json.dumps(result, allow_nan=False) == "0.0"


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
    assert surface["risk_unit_contract"] == EXPECTED_RISK_UNIT_CONTRACT
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


@pytest.mark.parametrize("warmup_bars", [0, 2])
def test_no_signal_backtest_exports_full_actual_equity_axis(monkeypatch, warmup_bars) -> None:
    """Synthetic timing fixture: no entries still produce every engine mark."""
    frame = _frame()
    config = PipelineConfig(
        initial_capital=12_345.67,
        research_mode=True,
        use_spread=False,
        swap_model_enabled=True,
    )
    modes = [False, True] if pm_core.NUMBA_AVAILABLE else [False]
    results = []
    for mode in modes:
        monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", mode)
        result = Backtester(config).run(
            frame,
            pd.Series(0, index=frame.index, dtype=np.int32),
            "ZZZUSD",
            _AtrHoldStrategy(),
            spec=_spec(swap_long=-1.0),
            timeframe="D1",
            warmup_bars=warmup_bars,
        )
        assert result["total_trades"] == 0
        assert result["trades"] == result["position_events"] == []
        assert result["position_event_count"] == 0
        assert type(result["equity_curve"]) is list
        assert result["equity_curve"] == [config.initial_capital] * len(frame)
        assert result["final_equity"] == config.initial_capital
        assert result["equity_growth"]["raw_mark_count"] == len(frame) - warmup_bars
        axis = result["trade_power_partition_axis"]
        assert axis["start_utc"] == frame.index[warmup_bars].isoformat()
        assert axis["end_utc"] == frame["_BAR_CLOSE_TS"].iloc[-1].isoformat()
        assert axis["scored_bars"] == len(frame) - warmup_bars
        assert axis["warmup_bars"] == warmup_bars
        assert result["swap_accrual_contract"]["execution_state_compounding"] is True
        assert result["swap_accrual_contract"]["source_provenance_authenticated"] is False
        results.append(result)
    if len(results) == 2:
        assert results[0]["equity_curve"] == results[1]["equity_curve"]
        assert results[0]["equity_growth"] == results[1]["equity_growth"]


def test_no_trade_metrics_preserve_supplied_curve_without_recreating_marks() -> None:
    backtester = Backtester(PipelineConfig(initial_capital=10_000.0))
    # Invented export-only values distinguish preservation from a flat reset.
    supplied_curve = [10_000.0, 9_997.5, 10_001.25]
    result = backtester._calculate_metrics([], 10_001.25, supplied_curve, 0.025)
    assert result["equity_curve"] == supplied_curve
    defaults = backtester._empty_result()
    assert {key: value for key, value in result.items() if key != "equity_curve"} == {
        key: value for key, value in defaults.items() if key != "equity_curve"
    }


@pytest.mark.parametrize("curve", [None, []])
def test_no_trade_metrics_without_engine_curve_keep_empty_schema(curve) -> None:
    backtester = Backtester(PipelineConfig(initial_capital=12_345.67))
    assert backtester._calculate_metrics([], 12_345.67, curve, 0.0) == backtester._empty_result()


@pytest.mark.parametrize(
    ("warmup_bars", "min_periods"),
    [(0, 8), (2, 8), (2, 10)],
    ids=["0", "2", "2-min10"],
)
def test_no_trade_oos_record_isolates_full_curve_content_changes(warmup_bars, min_periods) -> None:
    """Measure legacy export versus actual marks through the real OOS builder."""
    import pm_pipeline

    index = pd.date_range("2026-01-05", periods=12, freq="1D", tz="UTC")
    frame = pd.DataFrame(
        {
            "Open": 1.0,
            "High": 1.0002,
            "Low": 0.9998,
            "Close": 1.0,
            "ATR_14": 0.001,
            "_BAR_CLOSE_TS": index + pd.Timedelta(days=1),
        },
        index=index,
    )
    signals = pd.Series(0, index=index, dtype=np.int32)
    config = PipelineConfig(
        initial_capital=12_345.67,
        research_mode=True,
        use_spread=False,
        swap_model_enabled=False,
        oos_evidence_periods=64,
        oos_evidence_min_periods=min_periods,
    )
    result = Backtester(config).run(
        frame,
        signals,
        "ZZZUSD",
        _AtrHoldStrategy(),
        spec=_spec(),
        timeframe="D1",
        warmup_bars=warmup_bars,
    )
    assert result["total_trades"] == 0 and result["trades"] == []
    assert result["equity_curve"] == [config.initial_capital] * len(frame)
    result["_baseline_gated_signals_sha256"] = pm_pipeline._signal_vector_sha256(signals)
    # The measured pre-repair export discarded this path and kept one seed.
    legacy_result = {**result, "equity_curve": [config.initial_capital]}
    candidate = {
        "strategy_name": _AtrHoldStrategy.name,
        "params": dict(_AtrHoldStrategy.params),
        "evaluation_regime": "TREND",
    }
    optimizer = pm_pipeline.RegimeOptimizer(config)
    old_record = optimizer._build_oos_evidence_record(
        "ZZZUSD", "D1", copy.deepcopy(candidate), legacy_result, frame, warmup_bars
    )
    new_record = optimizer._build_oos_evidence_record(
        "ZZZUSD", "D1", copy.deepcopy(candidate), result, frame, warmup_bars
    )
    if min_periods == 10:
        # The threshold stays ten: nine legacy fallback periods are insufficient.
        assert config.oos_evidence_min_periods == 10
        assert old_record is None
        assert new_record is not None
        assert new_record["period_count"] == 10
        assert new_record["period_returns"] == [0.0] * 10
        assert new_record["candidate_id"] == optimizer._exact_cell_candidate_id(
            "ZZZUSD", "D1", "TREND", candidate
        )
        assert new_record["metrics"] == {
            "total_trades": 0,
            "profit_factor": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate": 0.0,
        }
        assert new_record["window"] == {
            "start_ts": index[0].isoformat(),
            "end_ts": index[-1].isoformat(),
            "bars": 12,
            "warmup_bars": 2,
        }
        assert new_record["equity_path_evidence"]["status"] == "available"
        assert new_record["equity_path_evidence"]["stored_points"] == 11
        assert new_record["equity_path_evidence"]["return_pct_blocks"] == [0.0] * 11
        assert new_record["baseline_evidence_sha256"] == pm_pipeline._oos_baseline_evidence_sha256(new_record)
        return
    assert old_record is not None and new_record is not None
    old_periods = 11 if warmup_bars == 0 else 9
    new_periods = 11 if warmup_bars == 0 else 10
    assert old_record["period_count"] == old_periods
    assert old_record["period_returns"] == [0.0] * old_periods
    assert new_record["period_count"] == new_periods
    assert new_record["period_returns"] == [0.0] * new_periods
    assert old_record["equity_path_evidence"] == {
        "status": "unavailable",
        "stored_points": 0,
        "return_pct_blocks": [],
        "capital_preservation_blocks": [],
    }
    stored_points = 12 if warmup_bars == 0 else 11
    assert new_record["equity_path_evidence"] == {
        "status": "available",
        "stored_points": stored_points,
        "truncated": False,
        "return_pct_blocks": [0.0] * stored_points,
        "drawdown_pct_blocks": [0.0] * stored_points,
        "capital_preservation_blocks": [1.0] * stored_points,
        "max_drawdown_pct": 0.0,
        "time_underwater_fraction": 0.0,
    }
    changed = {"equity_path_evidence", "baseline_evidence_sha256"}
    if warmup_bars:
        changed |= {"period_count", "period_returns"}
    assert old_record.keys() == new_record.keys()
    assert {key for key in old_record if old_record[key] != new_record[key]} == changed
    for key in pm_pipeline._OOS_BASELINE_IMMUTABLE_KEYS:
        if key not in changed:
            assert old_record[key] == new_record[key]
    for record in (old_record, new_record):
        assert record["baseline_evidence_sha256"] == pm_pipeline._oos_baseline_evidence_sha256(record)


@pytest.mark.parametrize("bar_count", [0, 1])
def test_empty_or_single_bar_backtest_keeps_no_execution_schema(bar_count) -> None:
    frame = _frame().iloc[:bar_count]
    config = PipelineConfig(
        initial_capital=12_345.67,
        research_mode=True,
        use_spread=False,
        swap_model_enabled=False,
    )
    backtester = Backtester(config)
    result = backtester.run(
        frame,
        pd.Series(0, index=frame.index, dtype=np.int32),
        "ZZZUSD",
        _AtrHoldStrategy(),
        spec=_spec(),
        timeframe="D1",
    )
    assert {key: result[key] for key in backtester._empty_result()} == backtester._empty_result()
    assert result["equity_curve"] == [config.initial_capital]
    assert result["numerical_execution_cost_attestation"]
    if bar_count == 0:
        assert result["winner_daily_r_surface"]["status"] == "UNAVAILABLE"
        assert result["equity_growth"]["reason"] == "EMPTY_EVALUATION_WINDOW"
    else:
        assert result["winner_daily_r_surface"]["status"] == "AVAILABLE"
        assert result["equity_growth"]["raw_mark_count"] == 1


def test_partial_close_reconciles_one_flat_to_flat_position(monkeypatch) -> None:
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    result = _run(
        _frame(),
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
        config=PipelineConfig(**common, use_spread=False),
        spec=_spec(swap_long=-1.0),
    )
    costed = _run(
        frame,
        config=PipelineConfig(**common, use_spread=True),
        spec=_spec(spread_avg=1.0, swap_long=-1.0),
    )

    _assert_accounting(base)
    _assert_accounting(costed)
    assert base["trades"][0]["swap_dollars"] < 0.0
    assert costed["position_events"][0]["position_id"] == base[
        "position_events"
    ][0]["position_id"]
    assert costed["position_events"][0]["pnl_dollars"] < base[
        "position_events"
    ][0]["pnl_dollars"]


@pytest.mark.parametrize("maximum_level", [
    "BASE", "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
    "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P95",
])
def test_retired_frozen_ladder_cannot_create_new_receipts(monkeypatch, maximum_level):
    from pm_spread_profile import RETIRED_COST_POLICY

    def execution_must_not_start(*args, **kwargs):
        pytest.fail("retired frozen-path replay reached execution")

    monkeypatch.setattr(Backtester, "run", execution_must_not_start)
    with pytest.raises(ValueError, match=RETIRED_COST_POLICY):
        Backtester(PipelineConfig(research_mode=True)).replay_frozen_path_cost_ladder(
            None, None, "ZZZUSD", _AtrHoldStrategy(), {},
            spec=_spec(), timeframe="D1", maximum_level_id=maximum_level,
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


def test_ladder_swap_delta_uses_settled_base_and_rejects_nonfinite() -> None:
    settle = Backtester._settle_frozen_path_ladder_swap
    assert settle(
        base_swap=-4.10,
        rollover_units=3,
        position_size=0.25,
        base_rate=-5.46,
        ladder_rate=-5.46,
    ) == -4.10
    assert settle(
        base_swap=-4.10,
        rollover_units=3,
        position_size=0.25,
        base_rate=-5.46,
        ladder_rate=-5.50,
    ) == -4.13

    for field, value in (
        ("base_swap", float("nan")),
        ("base_rate", float("inf")),
        ("ladder_rate", float("-inf")),
    ):
        values = {
            "base_swap": -4.10,
            "rollover_units": 3,
            "position_size": 0.25,
            "base_rate": -5.46,
            "ladder_rate": -5.50,
        }
        values[field] = value
        with pytest.raises(ValueError, match="swap counterfactual is invalid"):
            settle(**values)


def test_position_cumulative_prefix_improvement_fails_closed() -> None:
    frame = _frame()
    spec = _spec()
    base = _run(
        frame,
        config=PipelineConfig(
            research_mode=True,
            use_spread=False,
            swap_model_enabled=False,
        ),
        spec=spec,
    )
    changed_trades = copy.deepcopy(base["trades"])
    changed_trades[0]["entry_price"] -= 0.0001
    changed_trades[0]["pnl_dollars"] -= 1.0
    changed_events = pm_core.build_backtest_position_events(
        changed_trades,
        namespace="ZZZUSD|D1|winner-daily-r-hold",
    )
    zeros = np.zeros(len(frame), dtype=np.float64)
    units = np.zeros(len(frame), dtype=np.int64)

    with pytest.raises(pm_core.FrozenPathCostLadderError) as error:
        pm_core._build_backtest_winner_daily_r_lanes(
            frame,
            (
                pm_core._BacktestWinnerDailyRLane(
                    name="BASE",
                    trades=base["trades"],
                    position_events=base["position_events"],
                    half_spread_arr=zeros,
                    use_spread=False,
                    swap_model_enabled=False,
                    causal_swap_enabled=False,
                    pre_open_rollover_units_arr=units,
                    rollover_units_arr=units,
                    swap_long_per_lot_arr=zeros,
                    swap_short_per_lot_arr=zeros,
                ),
                pm_core._BacktestWinnerDailyRLane(
                    name="CHANGED",
                    trades=changed_trades,
                    position_events=changed_events,
                    half_spread_arr=zeros,
                    use_spread=False,
                    swap_model_enabled=False,
                    causal_swap_enabled=False,
                    pre_open_rollover_units_arr=units,
                    rollover_units_arr=units,
                    swap_long_per_lot_arr=zeros,
                    swap_short_per_lot_arr=zeros,
                ),
            ),
            namespace="ZZZUSD|D1|winner-daily-r-hold",
            spec=spec,
        )
    assert error.value.code == "LADDER_POSITION_CUMULATIVE_PREFIX_IMPROVEMENT"


def test_noncausal_swap_never_claims_full_path_daily_r() -> None:
    frame = _frame().drop(columns=["_BAR_CLOSE_TS"])
    result = _run(
        frame,
        config=PipelineConfig(research_mode=True, swap_model_enabled=True),
        spec=_spec(swap_long=-1.0),
    )
    assert result["winner_daily_r_surface"]["status"] == "UNAVAILABLE"
    assert result["winner_daily_r_surface"]["daily_net_mtm_r"] == []


def test_tampered_position_event_fails_reconciliation() -> None:
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
