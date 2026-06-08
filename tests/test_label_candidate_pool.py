import math

import numpy as np
import pandas as pd

from tools.label_candidate_pool import (
    _filter_trades_for_regime,
    _label_payload,
    _period_cvar_loss,
    _period_sharpe,
    _period_sortino,
    _predictor_payload,
)


def test_period_risk_features_are_finite_and_directional():
    returns = np.array([0.03, -0.01, 0.02, -0.04, 0.01], dtype=float)

    assert math.isfinite(_period_sharpe(returns))
    assert math.isfinite(_period_sortino(returns))
    assert _period_cvar_loss(returns, 0.2) == 0.04


def test_predictor_payload_exposes_honest_cost_oos_features():
    payload = _predictor_payload(
        np.array([0.01, -0.02, 0.03, 0.02], dtype=float),
        {
            "trade_path_evidence": {"late_gain_share": 0.3},
            "generalization_metrics": {"return_retention": 1.2},
            "equity_path_evidence": {
                "max_drawdown_pct": 4.5,
                "time_underwater_fraction": 0.25,
            },
        },
    )

    assert payload["oos_period_count"] == 4
    assert payload["oos_sharpe"] != 0.0
    assert payload["oos_cvar5_loss"] == 0.02
    assert payload["oos_path_max_dd_pct"] == 4.5
    assert payload["oos_path_time_underwater_fraction"] == 0.25


def test_regime_filter_uses_signal_bar_before_entry_bar():
    features = pd.DataFrame({"REGIME_LIVE": ["TREND", "RANGE"]})
    trades = [{"signal_bar": 0, "entry_bar": 1, "pnl_dollars": 10.0}]

    assert _filter_trades_for_regime(trades, features, "TREND") == trades
    assert _filter_trades_for_regime(trades, features, "RANGE") == []


def test_label_payload_accepts_full_history_return_alias():
    labels = _label_payload(
        {
            "full_history_return_pct": 2.0,
            "trades": 5,
            "profit_factor": 1.2,
            "max_dd_pct": 12.0,
        },
        min_label_trades=5,
    )

    assert labels["label_trade_sufficient"] is True
    assert labels["holder_label"] is True
    assert labels["carrier_label"] is True
    assert labels["strict_carrier_label"] is True


def test_label_payload_respects_minimum_trade_floor():
    labels = _label_payload(
        {
            "full_history_return_pct": 20.0,
            "trades": 4,
            "profit_factor": 9.0,
            "max_dd_pct": 1.0,
        },
        min_label_trades=5,
    )

    assert labels["label_trade_sufficient"] is False
    assert labels["holder_label"] is False
    assert labels["carrier_label"] is False
    assert labels["strict_carrier_label"] is False
