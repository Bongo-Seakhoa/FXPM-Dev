from unittest.mock import patch

import numpy as np
import pandas as pd

from pm_core import PipelineConfig
from pm_pipeline import OptimizationPipeline, RegimeConfig, RegimeOptimizer
from tests.test_trade_power import _source_contract, _trade_records


class _Strategy:
    name = "LineageStrategy"

    def generate_signals(self, frame, _symbol):
        return pd.Series(1, index=frame.index, dtype=np.int8)


def _result(*, base, start, end, axis, projection, seed):
    source = _source_contract(projection_sha256=projection)
    swap = dict(source["swap_accrual_contract"])
    swap.update(source["source_binding"])
    values = np.random.default_rng(seed).normal(0.30, 0.70, 500)
    return {
        "total_trades": len(values),
        "position_events": _trade_records(values, base=base),
        "execution_cost_timing_contract": source[
            "execution_cost_timing_contract"
        ],
        "numerical_execution_cost_attestation": source[
            "numerical_execution_cost_attestation"
        ],
        "swap_accrual_contract": swap,
        "trade_power_partition_axis": {
            "start_utc": start,
            "end_utc": end,
            "horizon_receipt_sha256": axis * 64,
            "event_axis_sha256": axis * 64,
        },
    }


def test_persisted_tuning_validation_is_consumed_by_final_lockbox():
    optimizer = RegimeOptimizer(PipelineConfig())
    identity = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_family": _Strategy.name,
        "parameter_hash": "d" * 64,
    }
    train = _result(
        base="2001-01-01",
        start="2000-01-01T00:00:00+00:00",
        end="2010-01-01T00:00:00+00:00",
        axis="a",
        projection="7" * 64,
        seed=1,
    )
    tuning = _result(
        base="2031-01-01",
        start="2030-01-01T00:00:00+00:00",
        end="2040-01-01T00:00:00+00:00",
        axis="b",
        projection="8" * 64,
        seed=2,
    )
    final = _result(
        base="2061-01-01",
        start="2060-01-01T00:00:00+00:00",
        end="2070-01-01T00:00:00+00:00",
        axis="c",
        projection="9" * 64,
        seed=3,
    )

    train_decision = optimizer._calibrate_train_power_result(
        train, effective_trials=4, **identity
    )
    tuning_decision = optimizer._evaluate_partition_power_result(
        tuning,
        frozen_requirement=train_decision.frozen_requirement,
        **identity,
    )
    assert train_decision.passed
    assert tuning_decision.passed

    optimizer.backtester = type(
        "_Backtester",
        (),
        {"run": lambda _self, *_args, **_kwargs: final},
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
            "trade_power": {
                "train": train_decision.to_dict(),
                "tuning_validation": tuning_decision.to_dict(),
            }
        },
    )
    index = pd.date_range("2060-01-01", periods=20, freq="h", tz="UTC")
    features = pd.DataFrame({"REGIME_LIVE": "TREND"}, index=index)

    with patch("pm_pipeline.StrategyRegistry.get", return_value=_Strategy()), patch(
        "pm_pipeline.block_bootstrap_evidence",
        return_value={"status": "PASS", "passed": True},
    ):
        pipeline._attach_holdout_metrics(
            "EURUSD",
            {"H1": {"TREND": winner}},
            {"H1": features},
            {"H1": 0},
        )

    assert "error" not in winner.holdout_metrics
    assert winner.holdout_metrics["trade_power_final"]["status"] == "PASS"
    assert winner.holdout_metrics["policy_status"] == "PASS"
