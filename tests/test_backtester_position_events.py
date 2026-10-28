from __future__ import annotations

from copy import deepcopy

import pytest

from pm_core import (
    BACKTEST_POSITION_EVENT_SCHEMA_VERSION,
    build_backtest_position_events,
)
from pm_pipeline import RegimeOptimizer
from pm_trade_power import REQUIRED_TRADE_RECORD_SCHEMA


def _leg(**overrides):
    row = {
        "signal_bar": 9,
        "entry_bar": 10,
        "exit_bar": 14,
        "entry_time": "2026-01-05T10:00:00+00:00",
        "exit_time": "2026-01-05T10:20:00+00:00",
        "direction": "LONG",
        "pnl_dollars": 12.5,
        "risk_amount": 40.0,
    }
    row.update(overrides)
    return row


def test_partial_legs_become_one_flat_to_flat_position() -> None:
    rows = [
        _leg(
            exit_bar=12,
            exit_time="2026-01-05T10:10:00+00:00",
            pnl_dollars=5.0,
            risk_amount=20.0,
        ),
        _leg(pnl_dollars=7.5, risk_amount=20.0),
    ]

    events = build_backtest_position_events(rows, namespace="EURUSD|M5|alpha")

    assert len(events) == 1
    event = events[0]
    assert event["trade_record_schema_version"] == REQUIRED_TRADE_RECORD_SCHEMA
    assert BACKTEST_POSITION_EVENT_SCHEMA_VERSION == REQUIRED_TRADE_RECORD_SCHEMA
    assert event["pnl_dollars"] == 12.5
    assert event["risk_amount"] == 40.0
    assert event["exit_time"] == "2026-01-05T10:20:00+00:00"
    assert event["entry_event_order"] < event["exit_event_order"]


def test_base_and_cost_ladder_keep_position_identity() -> None:
    base = [_leg()]
    ladder = [deepcopy(base[0])]
    ladder[0]["pnl_dollars"] = 9.0

    base_event = build_backtest_position_events(
        base, namespace="EURUSD|M5|alpha"
    )[0]
    ladder_event = build_backtest_position_events(
        ladder, namespace="EURUSD|M5|alpha"
    )[0]

    assert base_event["position_id"] == ladder_event["position_id"]
    assert base_event["pnl_dollars"] != ladder_event["pnl_dollars"]


def test_events_are_in_canonical_close_order() -> None:
    later = _leg()
    earlier = _leg(
        signal_bar=2,
        entry_bar=3,
        exit_bar=5,
        entry_time="2026-01-05T09:00:00+00:00",
        exit_time="2026-01-05T09:10:00+00:00",
    )

    events = build_backtest_position_events(
        [later, earlier], namespace="EURUSD|M5|alpha"
    )

    assert [event["exit_time"] for event in events] == [
        "2026-01-05T09:10:00+00:00",
        "2026-01-05T10:20:00+00:00",
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"risk_amount": 0.0},
        {"pnl_dollars": float("nan")},
        {"exit_bar": 9},
        {"exit_time": "2026-01-05T09:59:00+00:00"},
        {"direction": ""},
    ],
)
def test_malformed_position_evidence_fails_closed(changes) -> None:
    with pytest.raises(ValueError):
        build_backtest_position_events(
            [_leg(**changes)], namespace="EURUSD|M5|alpha"
        )


def test_compaction_does_not_retain_position_vectors() -> None:
    compact = RegimeOptimizer._compact_backtest_result(
        {
            "total_trades": 1,
            "trades": [_leg()],
            "position_events": build_backtest_position_events(
                [_leg()], namespace="EURUSD|M5|alpha"
            ),
            "equity_curve": [10000.0, 10012.5],
        }
    )

    assert compact == {"total_trades": 1}
