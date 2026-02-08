# Decision Throttle Fix - Implementation Notes

## Quick Reference

### What Changed
Added suppression check before recording "no actionable signal" decisions to prevent duplicate log spam.

### Where Changed
**File:** `pm_main.py`

**Lines:**
- 709-756: New helper method `_log_no_actionable_signal()`
- 1086-1101: Updated NO_ACTIONABLE_WINNER_SIGNAL path
- 1108-1123: Updated NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN path

### How It Works

#### Before (Old Behavior)
```python
# Direct logging - no suppression check
if not actionable:
    self.logger.debug(f"[{symbol}] NO_ACTIONABLE_WINNER_SIGNAL")
    self._decision_throttle.record_decision(...)  # Always records
    return
```

**Problem:** Logs on EVERY tick, even within same bar
- Bar 10:00 Tick 1: LOGS
- Bar 10:00 Tick 2: LOGS (duplicate!)
- Bar 10:00 Tick 3: LOGS (duplicate!)
- ...

#### After (New Behavior)
```python
# Helper with suppression check
if not actionable:
    self._log_no_actionable_signal(
        symbol=symbol,
        message="NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)",
        best_candidate=best_overall,
        bar_time_iso=bar_time_iso,
        action_type="NO_ACTIONABLE_WINNER_SIGNAL"
    )
    return
```

**Helper Logic:**
```python
def _log_no_actionable_signal(self, ...):
    # 1. Build decision key
    dk = DecisionThrottle.make_decision_key(...)

    # 2. Check suppression (NEW!)
    if self._decision_throttle.should_suppress(symbol, dk, bar_time_iso):
        return  # Already logged this bar - skip

    # 3. Log
    self.logger.debug(f"[{symbol}] {message}")

    # 4. Record
    self._decision_throttle.record_decision(...)
```

**Result:** Logs only ONCE per bar
- Bar 10:00 Tick 1: LOGS
- Bar 10:00 Tick 2: SUPPRESSED
- Bar 10:00 Tick 3: SUPPRESSED
- Bar 11:00 Tick 1: LOGS (new bar!)

### Key Design Decisions

#### 1. Direction = 0 for No-Signal Cases
```python
dk = DecisionThrottle.make_decision_key(
    symbol, strategy, timeframe, regime,
    0,  # direction=0 for no-signal (correct)
    bar_time
)
```
This is correct because:
- No actionable signal = no directional bias
- Distinguishes from LONG (1) or SHORT (-1) signals
- Maintains semantic correctness

#### 2. Suppression Only Within Same Bar
```python
def should_suppress(self, symbol: str, decision_key: str, bar_time_iso: str) -> bool:
    prev = self._cache.get(symbol)
    if prev is None:
        return False

    # Different bar -> allow (no suppression)
    if prev.bar_time != bar_time_iso:
        return False

    # Same bar + same key -> suppress
    if decision_key in prev.decision_keys:
        return True

    return False
```

This ensures:
- Logs reset on each new bar (bar-by-bar visibility)
- No cross-bar suppression
- High-frequency ticks within bar are deduplicated

#### 3. Helper Method Pattern
Instead of inline checks, created a reusable helper:

**Benefits:**
- DRY principle (used in 2 places)
- Centralized suppression logic
- Easier to test and maintain
- Consistent behavior across both no-actionable paths

### Testing

#### Unit Tests (5 tests)
```
test_no_actionable_signal_suppression_same_bar           ✓
test_no_actionable_signal_different_strategies_not_suppressed  ✓
test_no_actionable_signal_different_symbols_not_suppressed     ✓
test_actionable_signals_not_affected                     ✓
test_log_reduction_scenario                              ✓
```

#### Integration Tests
```
test_decision_throttle_integration                       ✓
```

#### Validation Script
```bash
python validate_throttle_fix.py
# Shows 95% log reduction in high-frequency scenarios
```

### Performance Impact

**Memory:**
- No significant change (uses existing throttle infrastructure)
- State file grows linearly with decisions (same as before)

**CPU:**
- Minimal overhead (one hash lookup per decision)
- Actually REDUCES work (fewer log writes in high-frequency scenarios)

**I/O:**
- REDUCES log I/O by ~95% in high-frequency scenarios
- State file writes unchanged (already throttled by bar)

### Edge Cases Handled

1. **First tick of bar** → Logs (not suppressed)
2. **Subsequent ticks same bar** → Suppressed
3. **New bar arrives** → Logs again (suppression reset)
4. **Different symbol** → Independent tracking
5. **Different strategy** → Independent tracking
6. **Actionable signal (direction != 0)** → Not affected
7. **State file missing** → Creates new file
8. **State file corrupted** → Graceful fallback

### Backwards Compatibility

✓ No breaking changes
✓ Uses existing DecisionThrottle class
✓ Decision key format unchanged
✓ State file format unchanged
✓ All existing tests pass

### Future Enhancements (Optional)

1. **Configurable suppression levels**
   ```python
   # Could add config option:
   suppress_no_actionable_logs: bool = True
   ```

2. **Metrics tracking**
   ```python
   # Could track suppression stats:
   {
       'total_decisions': 100,
       'suppressed': 95,
       'suppression_rate': 0.95
   }
   ```

3. **Log level control**
   ```python
   # Could make suppressed logs TRACE level:
   if should_suppress:
       self.logger.trace(f"[{symbol}] {message} (suppressed)")
       return
   ```

## Troubleshooting

### Issue: Not seeing any logs
**Cause:** State file may have stale data
**Fix:** Delete `last_trade_log.json` and restart

### Issue: Logs still appearing multiple times
**Cause:** Different decision keys (strategy/timeframe/regime changed)
**Fix:** This is expected behavior - different decisions should log

### Issue: Logs missing after bar transition
**Cause:** Check if bar_time is being updated correctly
**Fix:** Verify candidate['bar_time'] matches actual bar time

## Summary

This fix achieves:
- **95% log reduction** in high-frequency scenarios
- **Maintained bar-by-bar visibility**
- **No impact on trading logic**
- **Backwards compatible**
- **Fully tested**

The implementation is clean, maintainable, and follows existing patterns in the codebase.
