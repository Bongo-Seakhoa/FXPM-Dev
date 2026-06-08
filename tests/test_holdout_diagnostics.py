from tools.holdout_diagnostics import build_diagnostics


def test_holdout_diagnostics_counts_threshold_sensitivity_and_mismatches():
    promotion = {
        "engine_commit": "abc123",
        "rows": [
            {
                "symbol": "EURUSD",
                "timeframe": "H4",
                "regime": "TREND",
                "strategy_name": "A",
                "val_trades": 20,
                "holdout_status": "INSUFFICIENT",
                "holdout_trades": 4,
                "holdout_return_pct": 2.0,
                "holdout_pf": 1.4,
                "stress": "SURVIVE",
                "historical_status": "PASS",
                "pbo_timeframe": 0.2,
                "provenance": "fx_historical_spread",
                "promotion_status": "PROMOTED",
                "final_verdict": "REVIEW",
                "why": "holdout insufficient",
            },
            {
                "symbol": "EURUSD",
                "timeframe": "H4",
                "regime": "CHOP",
                "strategy_name": "B",
                "val_trades": 18,
                "holdout_status": "PASS",
                "holdout_trades": 5,
                "holdout_return_pct": 1.0,
                "holdout_pf": 1.2,
                "stress": "SURVIVE",
                "historical_status": "PASS",
                "pbo_timeframe": 0.2,
                "provenance": "fx_historical_spread",
                "promotion_status": "PROMOTED",
                "final_verdict": "PROMOTE",
                "why": "all gates clean",
            },
            {
                "symbol": "EURUSD",
                "timeframe": "H1",
                "regime": "RANGE",
                "strategy_name": "C",
                "val_trades": 15,
                "holdout_status": "FAIL",
                "holdout_trades": 6,
                "holdout_return_pct": -1.0,
                "holdout_pf": 0.7,
                "stress": "SURVIVE",
                "historical_status": "PASS",
                "pbo_timeframe": 0.2,
                "provenance": "fx_historical_spread",
                "promotion_status": "BLOCKED",
                "final_verdict": "REJECT",
                "why": "negative/sub-1 holdout",
            },
            {
                "symbol": "EURUSD",
                "timeframe": "M30",
                "regime": "CHOP",
                "strategy_name": "D",
                "val_trades": 15,
                "holdout_status": "INSUFFICIENT",
                "holdout_trades": 4,
                "holdout_return_pct": 3.0,
                "holdout_pf": 1.7,
                "stress": "SURVIVE",
                "historical_status": "PASS",
                "historical_trades": 12,
                "historical_return_pct": -0.2,
                "historical_pf": 0.98,
                "pbo_timeframe": 0.2,
                "provenance": "fx_historical_spread",
                "promotion_status": "BLOCKED",
                "gate_failure_codes": ["promotion_blocked", "historical_pf_lt_1"],
                "final_verdict": "REJECT",
                "why": "historical PF < 1",
            },
        ],
    }
    ledger = {
        "EURUSD": {
            "data_window_meta": {
                "windows": {
                    "stage2_training_selection": {"rows": 1000},
                    "clean_holdout": {"rows": 100},
                }
            }
        }
    }

    diagnostics = build_diagnostics(promotion, ledger, max_threshold=5)

    assert diagnostics["holdout_status_counts"] == {
        "FAIL": 1,
        "INSUFFICIENT": 2,
        "PASS": 1,
    }
    assert diagnostics["promotion_status_mismatches"]["promoted_but_holdout_insufficient"] == 1
    assert diagnostics["positive_but_insufficient_holdout"]["count"] == 2
    assert diagnostics["threshold_sensitivity"][3]["min_trades"] == 4
    assert diagnostics["threshold_sensitivity"][3]["gate_clean_except_holdout_promotable"] == 2
    assert diagnostics["threshold_sensitivity"][4]["min_trades"] == 5
    assert diagnostics["threshold_sensitivity"][4]["gate_clean_except_holdout_promotable"] == 1
    assert diagnostics["trade_rate_rows"][0]["holdout_trades_per_10k_m5"] == 400.0

    projection = diagnostics["construction_projection"]
    assert projection["projection_scope"] == "sufficiency_only_not_profitability"
    assert projection["rate_context"]["median_clean_holdout_to_training_bar_ratio"] == 0.1
    assert projection["rate_context"]["median_clean_holdout_to_stage2_bar_ratio"] == 0.090909
    assert projection["headroom_estimate_if_positive_thin_rows_hold_up"][
        "trade_count_anchor_ceiling"
    ] == 2

    anchor = projection["trade_count_anchor"]
    assert anchor["gate_clean_except_holdout_feasible"] == 1
    assert anchor["gate_clean_positive_thin_feasible"] == 1
    assert anchor["median_extra_training_loss_pct_if_done_as_flat_tail"] == 2.5

    kfold = projection["purged_kfold"]
    assert kfold["folds"] == 5
    assert kfold["gate_clean_except_holdout_sufficient"] == 1
    assert kfold["gate_clean_positive_thin_sufficient"] == 1
    assert kfold["permanent_train_data_loss_pct"] == 0.0
