"""
Test that no-actionable signal logging is properly suppressed within the same bar.

This test validates that the decision throttle prevents duplicate log spam
when the same no-actionable decision occurs multiple times within the same bar.
"""
import pytest
from unittest.mock import MagicMock, patch
from pm_main import DecisionThrottle


def test_no_actionable_signal_suppression_same_bar():
    """
    Test that duplicate no-actionable signals within the same bar are suppressed.

    Expected behavior:
    - First call: should log and record
    - Second call with same bar_time: should be suppressed (no log, no record)
    - Third call with different bar_time: should log and record again
    """
    throttle = DecisionThrottle()

    symbol = "EURUSD"
    strategy = "RSI_TREND"
    timeframe = "H1"
    regime = "TRENDING"
    direction = 0  # No signal
    bar_time_1 = "2026-02-08T10:00:00"
    bar_time_2 = "2026-02-08T11:00:00"

    # Build decision key
    dk = DecisionThrottle.make_decision_key(
        symbol, strategy, timeframe, regime, direction, bar_time_1
    )

    # First call - should NOT be suppressed
    should_suppress_1 = throttle.should_suppress(symbol, dk, bar_time_1)
    assert should_suppress_1 is False, "First call should not be suppressed"

    # Record the decision
    throttle.record_decision(
        symbol=symbol,
        decision_key=dk,
        bar_time_iso=bar_time_1,
        timeframe=timeframe,
        regime=regime,
        strategy_name=strategy,
        direction=direction,
        action="NO_ACTIONABLE_WINNER_SIGNAL"
    )

    # Second call - SAME bar_time - should be suppressed
    should_suppress_2 = throttle.should_suppress(symbol, dk, bar_time_1)
    assert should_suppress_2 is True, "Second call with same bar_time should be suppressed"

    # Third call - DIFFERENT bar_time - should NOT be suppressed
    dk_new_bar = DecisionThrottle.make_decision_key(
        symbol, strategy, timeframe, regime, direction, bar_time_2
    )
    should_suppress_3 = throttle.should_suppress(symbol, dk_new_bar, bar_time_2)
    assert should_suppress_3 is False, "Call with new bar_time should not be suppressed"


def test_no_actionable_signal_different_strategies_not_suppressed():
    """
    Test that different strategies within same bar are NOT suppressed.

    This ensures we only suppress identical decisions, not different ones.
    """
    throttle = DecisionThrottle()

    symbol = "GBPUSD"
    timeframe = "M15"
    regime = "RANGING"
    direction = 0
    bar_time = "2026-02-08T12:00:00"

    # Record first strategy
    dk1 = DecisionThrottle.make_decision_key(
        symbol, "MACD_CROSS", timeframe, regime, direction, bar_time
    )
    throttle.record_decision(
        symbol=symbol,
        decision_key=dk1,
        bar_time_iso=bar_time,
        timeframe=timeframe,
        regime=regime,
        strategy_name="MACD_CROSS",
        direction=direction,
        action="NO_ACTIONABLE_WINNER_SIGNAL"
    )

    # Try different strategy - should NOT be suppressed
    dk2 = DecisionThrottle.make_decision_key(
        symbol, "BOLLINGER_BOUNCE", timeframe, regime, direction, bar_time
    )
    should_suppress = throttle.should_suppress(symbol, dk2, bar_time)
    assert should_suppress is False, "Different strategy should not be suppressed"


def test_no_actionable_signal_different_symbols_not_suppressed():
    """
    Test that different symbols are tracked independently.
    """
    throttle = DecisionThrottle()

    strategy = "RSI_TREND"
    timeframe = "H4"
    regime = "TRENDING"
    direction = 0
    bar_time = "2026-02-08T14:00:00"

    # Record for EURUSD
    dk1 = DecisionThrottle.make_decision_key(
        "EURUSD", strategy, timeframe, regime, direction, bar_time
    )
    throttle.record_decision(
        symbol="EURUSD",
        decision_key=dk1,
        bar_time_iso=bar_time,
        timeframe=timeframe,
        regime=regime,
        strategy_name=strategy,
        direction=direction,
        action="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN"
    )

    # Try GBPUSD - should NOT be suppressed
    dk2 = DecisionThrottle.make_decision_key(
        "GBPUSD", strategy, timeframe, regime, direction, bar_time
    )
    should_suppress = throttle.should_suppress("GBPUSD", dk2, bar_time)
    assert should_suppress is False, "Different symbol should not be suppressed"


def test_actionable_signals_not_affected():
    """
    Test that actionable signals (direction != 0) are not affected by
    no-actionable signal suppression.
    """
    throttle = DecisionThrottle()

    symbol = "USDJPY"
    strategy = "MACD_CROSS"
    timeframe = "H1"
    regime = "TRENDING"
    bar_time = "2026-02-08T15:00:00"

    # Record no-actionable signal (direction=0)
    dk_no_action = DecisionThrottle.make_decision_key(
        symbol, strategy, timeframe, regime, 0, bar_time
    )
    throttle.record_decision(
        symbol=symbol,
        decision_key=dk_no_action,
        bar_time_iso=bar_time,
        timeframe=timeframe,
        regime=regime,
        strategy_name=strategy,
        direction=0,
        action="NO_ACTIONABLE_WINNER_SIGNAL"
    )

    # Try actionable signal (direction=1) - should NOT be suppressed
    dk_long = DecisionThrottle.make_decision_key(
        symbol, strategy, timeframe, regime, 1, bar_time
    )
    should_suppress = throttle.should_suppress(symbol, dk_long, bar_time)
    assert should_suppress is False, "Actionable signal should not be suppressed by no-actionable"


def test_log_reduction_scenario():
    """
    Simulate a high-frequency scenario where multiple ticks arrive within
    the same bar with no actionable signals.

    Expected: Only the first tick should log, subsequent ticks suppressed.
    """
    import os
    # Use a temporary file to avoid state contamination
    tmp_dir = os.path.join(os.getcwd(), "_test_tmp", "no_actionable_suppression")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_file = os.path.join(tmp_dir, "last_trade_log.json")
    throttle = DecisionThrottle(log_path=tmp_file)

    symbol = "EURJPY"
    strategy = "RSI_TREND"
    timeframe = "M5"
    regime = "RANGING"
    direction = 0
    bar_time = "2026-02-08T16:05:00"

    # Build decision key
    dk = DecisionThrottle.make_decision_key(
        symbol, strategy, timeframe, regime, direction, bar_time
    )

    # Simulate 10 ticks within same bar
    log_count = 0

    for tick in range(10):
        if not throttle.should_suppress(symbol, dk, bar_time):
            # Would log here
            log_count += 1
            throttle.record_decision(
                symbol=symbol,
                decision_key=dk,
                bar_time_iso=bar_time,
                timeframe=timeframe,
                regime=regime,
                strategy_name=strategy,
                direction=direction,
                action="NO_ACTIONABLE_WINNER_SIGNAL"
            )

    # Clean up
    try:
        os.unlink(tmp_file)
    except:
        pass

    # Should only log once (first tick)
    assert log_count == 1, f"Expected 1 log, got {log_count} (90% reduction achieved)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
