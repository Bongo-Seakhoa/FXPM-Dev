# Decision Throttle Fix Summary

## Objective
Prevent duplicate log spam when no actionable signals exist by checking `should_suppress()` before recording "no actionable signal" decisions.

## Problem
Before this fix, the system would log "NO_ACTIONABLE_WINNER_SIGNAL" and "NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN" messages on every tick, even when multiple ticks occurred within the same bar. This caused log spam in high-frequency scenarios.

Example (before fix):
```
[DEBUG] [EURUSD] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)
[DEBUG] [EURUSD] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)
[DEBUG] [EURUSD] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)
... (repeated 20 times for same bar)
```

## Solution
Added suppression check before logging and recording no-actionable decisions:

1. Created helper method `_log_no_actionable_signal()` in `LiveTrader` class
2. Updated two code paths to use the helper:
   - NO_ACTIONABLE_WINNER_SIGNAL (no actionable candidates)
   - NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN (candidates below margin threshold)
3. Helper checks `should_suppress()` before logging to prevent duplicates

## Changes Made

### File: `pm_main.py`

#### 1. Added Helper Method (lines 709-756)
```python
def _log_no_actionable_signal(self, symbol: str, message: str,
                               best_candidate: Dict, bar_time_iso: str,
                               action_type: str) -> None:
    """
    Log no actionable signal with throttle suppression to prevent duplicate logs.

    This helper prevents log spam when the same no-signal decision occurs
    multiple times within the same bar (common in high-frequency tick scenarios).
    """
    # Build decision key with direction=0 for no-signal cases
    dk = DecisionThrottle.make_decision_key(
        symbol,
        best_candidate.get('strategy_name', 'UNKNOWN'),
        best_candidate.get('timeframe', '?'),
        best_candidate.get('regime', '?'),
        0,  # direction=0 is correct for no-signal
        bar_time_iso
    )

    # Check if we should suppress this log (already logged in this bar)
    if self._decision_throttle.should_suppress(symbol, dk, bar_time_iso):
        return  # Silent return - already logged this decision for this bar

    # Log the message
    self.logger.debug(f"[{symbol}] {message}")

    # Record the decision in throttle
    self._decision_throttle.record_decision(
        symbol=symbol,
        decision_key=dk,
        bar_time_iso=bar_time_iso,
        timeframe=best_candidate.get('timeframe', '?'),
        regime=best_candidate.get('regime', '?'),
        strategy_name=best_candidate.get('strategy_name', 'UNKNOWN'),
        direction=0,
        action=action_type,
    )
```

#### 2. Updated NO_ACTIONABLE_WINNER_SIGNAL Path (lines 1086-1101)
**Before:**
```python
if not actionable:
    bar_time_iso = str(best_overall.get('bar_time', ''))
    dk = DecisionThrottle.make_decision_key(
        symbol, best_overall['strategy_name'], best_overall['timeframe'],
        best_overall['regime'], 0, bar_time_iso
    )
    self._decision_throttle.record_decision(
        symbol=symbol, decision_key=dk,
        bar_time_iso=bar_time_iso,
        timeframe=best_overall['timeframe'], regime=best_overall['regime'],
        strategy_name=best_overall['strategy_name'], direction=0,
        action="NO_ACTIONABLE_WINNER_SIGNAL",
    )
    self.logger.debug(f"[{symbol}] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)")
    return
```

**After:**
```python
if not actionable:
    # No actionable winner signals on any timeframe for this bar.
    # Use helper to prevent duplicate log spam within same bar.
    bar_time_iso = str(best_overall.get('bar_time', ''))

    self._log_no_actionable_signal(
        symbol=symbol,
        message="NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)",
        best_candidate=best_overall,
        bar_time_iso=bar_time_iso,
        action_type="NO_ACTIONABLE_WINNER_SIGNAL"
    )
    return
```

#### 3. Updated NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN Path (lines 1108-1123)
**Before:**
```python
if not eligible:
    bar_time_iso = str(best_overall.get('bar_time', ''))
    dk = DecisionThrottle.make_decision_key(
        symbol, best_overall['strategy_name'], best_overall['timeframe'],
        best_overall['regime'], 0, bar_time_iso
    )
    self._decision_throttle.record_decision(
        symbol=symbol, decision_key=dk,
        bar_time_iso=bar_time_iso,
        timeframe=best_overall['timeframe'], regime=best_overall['regime'],
        strategy_name=best_overall['strategy_name'], direction=0,
        action="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN",
    )
    self.logger.debug(f"[{symbol}] NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN (actionable below margin)")
    return
```

**After:**
```python
if not eligible:
    # There were signals, but none were close enough to the best overall score.
    # Use helper to prevent duplicate log spam within same bar.
    bar_time_iso = str(best_overall.get('bar_time', ''))

    self._log_no_actionable_signal(
        symbol=symbol,
        message="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN (actionable below margin)",
        best_candidate=best_overall,
        bar_time_iso=bar_time_iso,
        action_type="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN"
    )
    return
```

## Validation Results

### Test Suite
Created comprehensive test suite in `tests/test_no_actionable_suppression.py`:

```
PASSED test_no_actionable_signal_suppression_same_bar
PASSED test_no_actionable_signal_different_strategies_not_suppressed
PASSED test_no_actionable_signal_different_symbols_not_suppressed
PASSED test_actionable_signals_not_affected
PASSED test_log_reduction_scenario
```

All 7 throttle-related tests pass (5 new + 2 existing).

### High-Frequency Scenario Validation

**Scenario:** 20 ticks arrive within same bar with no actionable signals

**Results:**
```
Metric                          Without Fix    With Fix    Improvement
---------------------------------------------------------------------------
Logs per 20 ticks:              20             1           -
Log spam reduction:             0%             95%         95%
```

**Bar Transition Test:**
```
Bar 1 (2026-02-08T10:00:00):
  Tick 1: LOGGED (first occurrence)      ✓
  Tick 2: SUPPRESSED (same bar)          ✓

Bar 2 (2026-02-08T11:00:00):
  Tick 1: LOGGED (new bar, reset)        ✓
  Tick 2: SUPPRESSED (same bar)          ✓
```

## Key Features

### 1. Suppression Within Same Bar
- First occurrence: LOGS
- Subsequent ticks (same bar): SUPPRESSED
- Log reduction: ~95% in high-frequency scenarios

### 2. Bar Transition Handling
- Suppression resets on new bar
- Ensures logs appear for each bar (no cross-bar suppression)
- Maintains bar-by-bar visibility

### 3. Independent Tracking
- Different symbols: tracked independently
- Different strategies: tracked independently
- Different timeframes: tracked independently
- Actionable signals (direction != 0): not affected

### 4. Correct Semantics
- Uses `direction=0` for no-signal cases (correct)
- Maintains decision key structure
- Preserves throttle state across restarts

## Quality Checks Performed

1. **Repeated ticks within same bar** → Only one log ✓
2. **New bar arrival** → Log appears again (not suppressed) ✓
3. **Log count reduction** → 95% reduction in high-frequency scenarios ✓
4. **Decision throttle state** → Grows reasonably, persists correctly ✓
5. **Different strategies/symbols** → Tracked independently ✓
6. **Actionable signals** → Not affected by no-signal suppression ✓

## Core Objectives Alignment

### Reliability
- **Clean logs improve debugging** ✓
  - Reduced noise in logs
  - Easier to spot actual trading decisions
  - Bar-by-bar visibility maintained

### Trade Frequency
- **No impact on actual trading** ✓
  - Only affects logging behavior
  - Trading logic unchanged
  - All signals still evaluated

## Files Modified

1. `pm_main.py` - Added helper method and updated two no-actionable branches
2. `tests/test_no_actionable_suppression.py` - Created comprehensive test suite
3. `validate_throttle_fix.py` - Created validation script

## Backwards Compatibility

- Fully backwards compatible
- Uses existing `DecisionThrottle` infrastructure
- No changes to decision key format
- No changes to state file structure

## Conclusion

The fix successfully prevents duplicate log spam for no-actionable signals while:
- Maintaining bar-by-bar visibility
- Preserving independent tracking per symbol/strategy
- Not affecting actual trading decisions
- Achieving 95% log reduction in high-frequency scenarios

**Status:** FIX VALIDATED SUCCESSFULLY ✓
