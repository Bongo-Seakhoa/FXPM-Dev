from unittest.mock import patch

import numpy as np
import pandas as pd

from pm_core import PipelineConfig
from pm_pipeline import OptimizationPipeline, RegimeConfig, RegimeOptimizer
from pm_trade_power import build_numerical_execution_cost_attestation


def _authenticated_result(seed: int = 41, observations: int = 500):
    rng = np.random.default_rng(seed)
    net_r = rng.normal(loc=0.30, scale=0.70, size=observations)
    start = pd.Timestamp("2020-01-01", tz="UTC")
    trades = []
    for index, value in enumerate(net_r):
        entry = start + pd.Timedelta(hours=3 * index)
        trades.append({
            "pnl_dollars": float(value * 100.0),
            "risk_amount": 100.0,
            "entry_time": entry,
            "exit_time": entry + pd.Timedelta(hours=1),
            "trade_record_schema_version": "2026-06-18-mae-mfe-v1",
        })
    return {
        "total_trades": len(trades),
        "trades": trades,
        "execution_cost_timing_contract": {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "numerical_execution_cost_attestation": (
            build_numerical_execution_cost_attestation(
                use_spread=True,
                use_slippage=True,
                use_commission=True,
                spread_mode="conservative_max",
                spread_values_sha256="7" * 64,
                commission_inputs_sha256="8" * 64,
                instrument_spec_sha256="9" * 64,
                slippage_pips=0.5,
                slippage_price=0.00005,
            )
        ),
        "swap_accrual_contract": {
            "contract_version": "causal_swap_accrual_v1",
            "status": "AVAILABLE",
            "provenance_status": "STRESS_APPROVED",
            "execution_state_compounding": True,
            "historical_rates_authenticated": False,
            "stress_policy_explicitly_approved": True,
            "stress_policy_immutable": True,
            "stress_policy_sha256": "a" * 64,
            "timestamp_source": "_BAR_CLOSE_TS",
            "price_activation_manifest_sha256": "1" * 64,
            "terms_activation_manifest_sha256": "2" * 64,
            "terms_snapshot_sha256": "3" * 64,
            "schedule_sha256": "4" * 64,
            "target_bars_sha256": "5" * 64,
            "target_sidecar_sha256": "6" * 64,
            "projection_contract": "fxpm-swap-preopen-close-projection-v1",
            "projection_sha256": "a" * 64,
        },
    }


class _Strategy:
    name = "PowerIntegratedFamily"


def test_frozen_validation_consumes_train_requirement_and_drops_transient_state():
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    train = _authenticated_result(seed=51)
    train_decision = optimizer._calibrate_train_power_result(
        train,
        effective_trials=1,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family=_Strategy.name,
        parameter_hash="b" * 64,
    )
    assert train_decision.passed
    assert train_decision.frozen_requirement is not None

    candidate = {
        "strategy_name": _Strategy.name,
        "params": {},
        "evaluation_regime": "TREND",
        "trade_power_contract_enforced": True,
        "trade_power_train": train_decision.to_dict(),
        "_trade_power_requirement": train_decision.frozen_requirement,
    }
    records = [{
        "strategy_family": _Strategy.name,
        "regime": "TREND",
    }]
    validation = _authenticated_result(seed=52)
    compact = {
        "total_trades": validation["total_trades"],
        "_trade_power_raw_result": validation,
    }
    optimizer._run_one_exact_regime = lambda **_kwargs: (dict(compact), True)
    features = pd.DataFrame(index=pd.RangeIndex(80))

    with patch("pm_pipeline.StrategyRegistry.get", return_value=_Strategy()):
        optimizer._attach_frozen_probe_validation(
            symbol="EURUSD",
            timeframe="H1",
            val_features=features,
            val_warmup_bars=0,
            candidates=[candidate],
            cell_records=records,
        )

    assert candidate["trade_power_validation"]["status"] == "PASS"
    assert candidate["trade_power_validation"]["requirement_recalibrated"] is False
    assert "_trade_power_requirement" not in candidate
    assert "_trade_power_raw_result" not in candidate
    assert "trades" not in candidate["trade_power_validation"]
    assert records[0]["tuning_validation_evaluations"] == 1


def test_trade_power_artifacts_are_compact_and_expose_required_ess():
    optimizer = RegimeOptimizer.__new__(RegimeOptimizer)
    decision = optimizer._calibrate_train_power_result(
        _authenticated_result(seed=61),
        effective_trials=4,
        symbol="EURUSD",
        timeframe="D1",
        regime="RANGE",
        strategy_family=_Strategy.name,
        parameter_hash="c" * 64,
    )
    payload = decision.to_dict()

    assert payload["status"] == "PASS"
    assert payload["observed_effective_trades"] > 0
    assert payload["required_effective_trades"] > 0
    assert payload["sample_evidence"]["raw_trade_records"] == 500
    assert "trades" not in payload
    assert "net_r_values" not in payload["sample_evidence"]


def test_final_lockbox_consumes_frozen_train_ess_requirement():
    optimizer = RegimeOptimizer(PipelineConfig())
    train_decision = optimizer._calibrate_train_power_result(
        _authenticated_result(seed=71, observations=500),
        effective_trials=4,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_family=_Strategy.name,
        parameter_hash="d" * 64,
    )
    assert train_decision.passed

    final_result = _authenticated_result(seed=72, observations=12)
    optimizer.backtester = type(
        "_Backtester",
        (),
        {"run": lambda _self, *_args, **_kwargs: final_result},
    )()
    optimizer._build_daily_net_mtm_surface = lambda **_kwargs: {}

    pipeline = OptimizationPipeline.__new__(OptimizationPipeline)
    pipeline.config = PipelineConfig(lockbox_one_shot_registry_enabled=False)
    pipeline.regime_optimizer = optimizer
    winner = RegimeConfig(
        strategy_name=_Strategy.name,
        parameters={},
        quality_score=1.0,
        validation_evidence={
            "trade_power": {"train": train_decision.to_dict()}
        },
    )
    index = pd.date_range("2026-01-01", periods=20, freq="h", tz="UTC")
    features = pd.DataFrame(
        {
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.0,
            "REGIME_LIVE": "TREND",
        },
        index=index,
    )
    strategy = _Strategy()
    strategy.generate_signals = lambda frame, _symbol: pd.Series(
        1, index=frame.index, dtype=np.int8
    )

    with patch("pm_pipeline.StrategyRegistry.get", return_value=strategy), patch(
        "pm_pipeline.block_bootstrap_evidence",
        return_value={"status": "PASS", "passed": True},
    ):
        pipeline._attach_holdout_metrics(
            "EURUSD",
            {"H1": {"TREND": winner}},
            {"H1": features},
            {"H1": 0},
        )

    final_power = winner.holdout_metrics["trade_power_final"]
    assert final_power["partition"] == "final_lockbox"
    assert final_power["requirement_recalibrated"] is False
    assert final_power["status"] == "EXPECTED_INELIGIBLE"
    assert winner.holdout_metrics["policy_status"] == "FAIL"
