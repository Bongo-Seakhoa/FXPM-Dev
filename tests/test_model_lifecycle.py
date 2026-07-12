from pm_core import PipelineConfig
from pm_model_lifecycle import (
    EDGE_CHALLENGER_ROLE,
    LOCKBOX_CHAMPION_ROLE,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
    temporal_role_block_reason,
)
from unittest.mock import Mock, patch

import pandas as pd

from pm_pipeline import (
    OptimizationPipeline,
    RegimeConfig,
    SymbolConfig,
    classify_promotion_status,
)


def _window_meta():
    return {
        "as_of_utc": "2026-01-01T00:00:00Z",
        "windows": {
            "tuning_validation": {
                "start_ts": "2022-12-25T00:00:00Z",
                "end_ts": "2023-12-25T00:00:00Z",
            },
            "final_lockbox": {
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2026-01-01T00:00:00Z",
            },
        },
    }


def _profitable_config(lineage):
    return RegimeConfig(
        strategy_name="DemoStrategy",
        parameters={"period": 20},
        quality_score=0.8,
        val_metrics={
            "profit_factor": 1.4,
            "total_return_pct": 15.0,
            "max_drawdown_pct": 5.0,
        },
        validation_evidence={"temporal_lineage": lineage},
    )


def test_champion_lineage_exposes_selection_cutoff_and_latest_evidence_age():
    lineage = build_lockbox_champion_lineage(_window_meta())

    assert lineage["contract_status"] == "PASS"
    assert lineage["book_role"] == LOCKBOX_CHAMPION_ROLE
    assert lineage["parameter_selection_cutoff_utc"].startswith("2023-12-25")
    assert lineage["performance_evidenced_through_utc"].startswith("2026-01-01")
    assert lineage["parameter_selection_age_days_at_asof"] > 700
    assert lineage["evidence_lag_days_at_asof"] == 0.0
    assert temporal_role_block_reason({"temporal_lineage": lineage}) is None


def test_champion_lineage_fails_if_selection_touches_lockbox():
    meta = _window_meta()
    meta["windows"]["tuning_validation"]["end_ts"] = "2024-02-01T00:00:00Z"

    lineage = build_lockbox_champion_lineage(meta)

    assert lineage["contract_status"] == "FAIL"
    assert not lineage["funded_role_eligible"]
    assert "overlaps final lockbox" in " ".join(lineage["contract_errors"])


def test_edge_challenger_is_separate_paper_only_book_and_never_live_valid():
    book = build_empty_edge_challenger_book(_window_meta())
    challenger_lineage = {
        "schema_version": book["schema_version"],
        "book_role": EDGE_CHALLENGER_ROLE,
        "contract_status": "PASS",
        "parameters_frozen_during_final_lockbox": False,
    }
    challenger = _profitable_config(challenger_lineage)

    status, reasons = classify_promotion_status(challenger, PipelineConfig())

    assert book["paper_only"]
    assert book["regime_configs"] == {}
    assert status == "PAPER_ONLY"
    assert "future non-overlapping forward evidence" in reasons[0]
    assert not challenger.is_valid_for_live()


def test_symbol_config_roundtrip_preserves_separate_challenger_book():
    lineage = build_lockbox_champion_lineage(_window_meta())
    champion = _profitable_config(lineage)
    book = build_empty_edge_challenger_book(_window_meta())
    config = SymbolConfig(
        symbol="EURUSD",
        regime_configs={"H1": {"TREND": champion}},
        default_config=champion,
        edge_challenger_book=book,
    )

    restored = SymbolConfig.from_dict(config.to_dict())

    assert restored.edge_challenger_book["book_role"] == EDGE_CHALLENGER_ROLE
    assert restored.edge_challenger_book["funded_role_eligible"] is False
    assert restored.get_regime_config("H1", "TREND").is_valid_for_live()


def test_latest_data_refit_writes_only_paper_challenger_book():
    pipeline = OptimizationPipeline.__new__(OptimizationPipeline)
    pipeline.config = PipelineConfig(
        false_discovery_control_enabled=False,
        false_discovery_holdout_policy="off",
        walk_forward_promotion_policy="off",
    )
    pipeline.regime_optimizer = Mock()
    pipeline.regime_optimizer.min_train_trades = 2
    pipeline.regime_optimizer._tune_strategy_params.return_value = [
        {
            "strategy_name": "DemoStrategy",
            "params": {"period": 11},
            "train_regime_metrics": {
                "TREND": {
                    "total_trades": 40,
                    "profit_factor": 1.3,
                    "total_return_pct": 12.0,
                    "max_drawdown_pct": 6.0,
                }
            },
            "tuned_for_regime": "TREND",
            "search_trials": 25,
            "search_phase": "edge_refit_test",
        }
    ]
    pipeline.regime_optimizer._compute_train_regime_score.return_value = 2.0
    pipeline.regime_optimizer._normalize_score.return_value = 0.75

    champion_lineage = build_lockbox_champion_lineage(_window_meta())
    champion = _profitable_config(champion_lineage)
    champion.parameters = {"period": 20}
    index = pd.date_range("2020-01-01", "2026-01-01", freq="D", tz="UTC")
    full_features = pd.DataFrame(
        {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.0},
        index=index,
    )
    strategy = Mock()
    strategy.get_param_grid.return_value = {"period": [10, 11, 12]}

    with patch("pm_pipeline.StrategyRegistry.get", return_value=strategy):
        challengers, failures = pipeline._build_edge_challengers_for_timeframe(
            symbol="EURUSD",
            timeframe="H1",
            champion_configs={"TREND": champion},
            full_features=full_features,
            fit_start=pd.Timestamp("2021-01-01T00:00:00Z"),
            fit_end=pd.Timestamp("2026-01-01T00:00:00Z"),
            champion_temporal_lineage=champion_lineage,
        )

    assert failures == []
    challenger = challengers["TREND"]
    assert challenger["parameters"] == {"period": 11}
    assert challenger["promotion_status"] == "PAPER_ONLY"
    temporal = challenger["validation_evidence"]["temporal_lineage"]
    assert temporal["book_role"] == EDGE_CHALLENGER_ROLE
    assert temporal["fit_cutoff_utc"].startswith("2026-01-01")
    assert temporal["performance_evidenced_through_utc"] is None
    assert not temporal["champion_lockbox_verdict_inherited"]
    # The funded champion remains immutable and physically separate.
    assert champion.parameters == {"period": 20}
