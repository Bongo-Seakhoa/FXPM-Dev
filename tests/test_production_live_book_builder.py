import pytest

from tools.build_production_live_book import _assert_marker, build_live_ledger


def test_builder_rejects_non_review_ready_completion_marker():
    with pytest.raises(SystemExit, match="review_ready"):
        _assert_marker(
            {
                "verdict": "CANDIDATES_PROMOTABLE_AFTER_GATES_CELL_SAFE",
                "pid_alive": False,
                "stale": False,
            }
        )


def test_builder_stamps_deployment_validity_on_symbol_and_cells():
    source = {
        "EURUSD": {
            "symbol": "EURUSD",
            "retrain_days": 14,
            "valid_until": "2026-06-07T00:01:00",
            "artifact_meta": {"artifact_version": "x"},
            "regime_configs": {
                "H4": {
                    "TREND": {
                        "strategy_name": "EMACrossoverStrategy",
                        "parameters": {},
                        "promotion_status": "PROMOTED",
                        "valid_until": "2026-06-07T00:01:00",
                    }
                }
            },
        }
    }

    ledger = build_live_ledger(
        source,
        {
            "EURUSD|H4|TREND": {
                "symbol": "EURUSD",
                "timeframe": "H4",
                "regime": "TREND",
                "strategy_name": "EMACrossoverStrategy",
                "final_verdict": "PROMOTE",
            }
        },
        valid_until="2026-07-05T00:01:00",
        retrain_days=28,
    )

    symbol_cfg = ledger["EURUSD"]
    cell_cfg = symbol_cfg["regime_configs"]["H4"]["TREND"]
    assert symbol_cfg["retrain_days"] == 28
    assert symbol_cfg["valid_until"] == "2026-07-05T00:01:00"
    assert cell_cfg["valid_until"] == "2026-07-05T00:01:00"
    assert symbol_cfg["strategy_name"] == "NO_TRADE"
