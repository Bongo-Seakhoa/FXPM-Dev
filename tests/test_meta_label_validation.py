import json
from pathlib import Path

import pytest

from tools.meta_label_validation import DEFAULT_FEATURES, evaluate, feature_functions, rank_auc


def test_rank_auc_handles_perfect_reversed_and_ties():
    labels = [False, False, True, True]

    assert rank_auc(labels, [0.1, 0.2, 0.8, 0.9]) == 1.0
    assert rank_auc(labels, [0.9, 0.8, 0.2, 0.1]) == 0.0
    assert rank_auc(labels, [1.0, 1.0, 1.0, 1.0]) == 0.5


def test_evaluate_excludes_skipped_non_specialists(tmp_path: Path):
    labels = {
        "rows": [
            {
                "candidate_id": "skip",
                "status": "skipped",
                "carrier_label": False,
                "symbol": "EURUSD",
                "timeframe": "H1",
                "label_regime": "",
                "oos_sharpe": -999.0,
            },
            {
                "candidate_id": "bad",
                "status": "ok",
                "carrier_label": False,
                "symbol": "EURUSD",
                "timeframe": "H1",
                "label_regime": "TREND",
                "strategy_name": "Bad",
                "oos_sharpe": 0.1,
            },
            {
                "candidate_id": "good",
                "status": "ok",
                "carrier_label": True,
                "symbol": "EURUSD",
                "timeframe": "H1",
                "label_regime": "TREND",
                "strategy_name": "Good",
                "oos_sharpe": 0.9,
            },
        ]
    }
    labels_path = tmp_path / "labels.json"
    labels_path.write_text(json.dumps(labels), encoding="utf-8")

    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    manifest = {
        "records": [
            {"candidate_id": "bad", "val_regime_metrics": {"TREND": {"mean_r": -0.1}}},
            {"candidate_id": "good", "val_regime_metrics": {"TREND": {"mean_r": 0.2}}},
        ]
    }
    (evidence_dir / "EURUSD_oos_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = evaluate(labels_path, evidence_dir, "carrier_label", ["all_oos_sharpe", "val_reg_mean_r"])

    assert result["row_counts"]["all_rows"] == 3
    assert result["row_counts"]["ok_specialists"] == 2
    assert result["row_counts"]["skipped_non_specialists"] == 1
    assert result["feature_auc"]["all_oos_sharpe"]["pooled_auc"] == 1.0
    assert result["feature_cell_summaries"]["all_oos_sharpe"]["picked_carrier"] == 1


def test_circular_full_history_outcome_fields_are_not_predictors(tmp_path: Path):
    forbidden = {
        "full_history_return_pct",
        "profit_factor",
        "sharpe",
        "max_dd_pct",
        "trades",
    }

    available = set(DEFAULT_FEATURES) | set(feature_functions())
    assert forbidden.isdisjoint(available)

    labels = {
        "rows": [
            {
                "candidate_id": "lottery",
                "status": "ok",
                "carrier_label": True,
                "symbol": "XAUUSD",
                "timeframe": "H1",
                "label_regime": "TREND",
                "strategy_name": "Lottery",
                "oos_sharpe": -1.0,
                "full_history_return_pct": 460.0,
                "profit_factor": 5.0,
                "sharpe": 99.0,
                "max_dd_pct": 12.0,
                "trades": 1446,
            },
            {
                "candidate_id": "decay",
                "status": "ok",
                "carrier_label": False,
                "symbol": "GBPUSD",
                "timeframe": "H1",
                "label_regime": "CHOP",
                "strategy_name": "Decay",
                "oos_sharpe": 1.0,
                "full_history_return_pct": -50.0,
                "profit_factor": 0.55,
                "sharpe": -5.0,
                "max_dd_pct": 45.0,
                "trades": 1132,
            },
        ]
    }
    labels_path = tmp_path / "labels.json"
    labels_path.write_text(json.dumps(labels), encoding="utf-8")
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    for feature in sorted(forbidden):
        with pytest.raises(SystemExit):
            evaluate(labels_path, evidence_dir, "carrier_label", [feature])
