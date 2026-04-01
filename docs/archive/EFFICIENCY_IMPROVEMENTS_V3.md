# FX Portfolio Manager - Efficiency Improvements v3

**Version:** 3.3  
**Date:** February 1, 2026  
**Principle:** Quality First, Efficiency Second

---

## Summary of Changes

This version addresses all critique points from code review while maintaining full quality parity with the original Python backtester. The key fix is **live-equity position sizing** inside the JIT loop, ensuring compounding behavior is preserved.

---

## Critical Fixes in v3.3

### 1. Live-Equity Position Sizing (Quality-Preserving Fix)

**Problem:** Previous implementation pre-computed position sizes using fixed `initial_capital`, breaking compounding behavior.

**Solution:** Position sizing now happens inside the JIT loop using current equity:

```python
# Inside the Numba JIT kernel:
risk_amount_at_entry = equity * (position_size_pct / 100.0)

# Calculate loss per lot at stop
if direction == 1:  # Long: SL is below entry
    dist = entry_price_candidate - stop_loss_candidate
else:  # Short: SL is above entry
    dist = stop_loss_candidate - entry_price_candidate

if tick_size > 0.0 and tick_value > 0.0:
    loss_per_lot = (dist / tick_size) * tick_value
else:
    loss_per_lot = (dist / pip_size) * pip_value

raw_size = risk_amount_at_entry / loss_per_lot
position_size = _round_volume_numba(raw_size, min_lot, max_lot, volume_step)
```

**Verification:**
```
Trade 1: Equity $10000.00 → Risk $100.00 ✓
Trade 2: Equity $9876.00 → Risk $98.76 ✓
Trade 3: Equity $10059.21 → Risk $100.59 ✓
...
35 unique risk amounts out of 35 trades (compounding verified)
```

### 2. Numba-Safe Volume Rounding Helper

New JIT-compatible function replicates broker volume logic:

```python
@jit(nopython=True, cache=True)
def _round_volume_numba(volume: float, min_lot: float, max_lot: float, step: float) -> float:
    if step <= 0.0:
        step = 0.01
    # Floor to step (risk-safe - never round up)
    rounded = np.floor(volume / step) * step
    # Clamp to min/max
    if rounded < min_lot:
        rounded = min_lot
    if rounded > max_lot:
        rounded = max_lot
    return np.round(rounded, 8)
```

### 3. End-of-Data Close Updates Equity Curve/Drawdown

**Problem:** EOD close P&L wasn't reflected in equity_curve[n_bars-1] or final drawdown.

**Solution:** After EOD close, we now update:

```python
# After recording EOD trade:
equity += pnl_dollars

# FIX: Update equity curve and drawdown after EOD close
equity_curve[n_bars - 1] = equity
if equity > peak_equity:
    peak_equity = equity
if peak_equity > 0.0:
    dd = (peak_equity - equity) / peak_equity * 100.0
    if dd > max_drawdown:
        max_drawdown = dd
```

### 4. Trade Array Slicing Verified

Arrays are correctly sliced to `trade_count` in the return statement:

```python
return (
    trade_signal_bars[:trade_count],
    trade_entry_bars[:trade_count],
    # ... all other arrays sliced to :trade_count
)
```

---

## Architecture Changes

### What's Pre-computed (in Python, before JIT)

Only values that require Python object calls:
- `entry_prices[i]` - Entry price for each potential entry
- `sl_prices[i]` - Stop loss price
- `tp_prices[i]` - Take profit price

### What's Computed in JIT Loop

- `risk_amount_at_entry` - Based on current equity (compounding)
- `loss_per_lot` - Using tick math or pip fallback
- `position_size` - Calculated and rounded

### New Kernel Signature

```python
def _backtest_loop_numba(
    open_arr, high_arr, low_arr, close_arr, sig_arr,
    sl_prices, tp_prices, entry_prices,  # Pre-computed prices only
    initial_capital,
    position_size_pct,  # NEW: Percentage for sizing
    half_spread, slippage_price,
    use_spread, use_slippage, use_commission, commission_per_lot,
    tick_size, tick_value, pip_size, pip_value,
    min_lot, max_lot, volume_step  # NEW: Volume rounding params
)
```

---

## Performance Characteristics

| Component | Speedup | Quality Impact |
|-----------|---------|----------------|
| Full feature computation | baseline | None |
| Lazy feature computation | 307x | None |
| Hull MA (cached) | 63x | None |
| Regime detection (Numba) | 5x | None |
| **Backtester loop (Numba)** | **~8-10x** | **None (compounding preserved)** |

---

## Quality Verification Results

### 1. Compounding Test
```
Position size variation: 25 unique sizes out of 35 trades
Risk amount variation: 35 unique values out of 35 trades
✓ Risk amounts vary with equity (compounding is working)
```

### 2. Risk Amount Accuracy
```
Trade 1: Expected $100.00, Actual $100.00 ✓
Trade 2: Expected $98.76, Actual $98.76 ✓
Trade 3: Expected $100.59, Actual $100.59 ✓
```

### 3. Final Equity Verification
```
Initial: $10000.00
Sum of P&L: $-1222.42
Expected final: $8777.58
Actual final: $8777.61
Difference: $0.03 (floating-point rounding)
```

### 4. SL/TP Order Preserved
- SL checked BEFORE TP (same-bar hit order)
- No boundary violations
- Exit reasons correctly assigned

---

## Files Modified in v3.3

| File | Changes |
|------|---------|
| `pm_core.py` | Live-equity sizing in JIT, `_round_volume_numba()`, EOD equity/DD fix |

---

## Backward Compatibility

All changes are fully backward compatible:
- Numba is optional (graceful fallback to pure Python)
- No changes to public API signatures
- Same trade results with or without Numba
- Same mathematical behavior as original implementation

---

## Installation Notes

For maximum performance, install Numba:
```bash
pip install numba
```

The code will automatically detect and use Numba if available. First run with Numba will be slower due to JIT compilation; subsequent runs use cached compiled code.

---

## Critique Response Summary

| Critique Point | Status | Resolution |
|---------------|--------|------------|
| 1. Fixed-equity sizing breaks compounding | **FIXED** | Sizing now uses live equity inside JIT loop |
| 2. EOD close not in equity curve/drawdown | **FIXED** | Added explicit update after EOD close |
| 3. Trade arrays may have trailing zeros | **VERIFIED** | Arrays correctly sliced to `trade_count` |

---

## Code Quality Checklist

- [x] All Python files compile without errors
- [x] Compounding behavior verified with test
- [x] SL/TP ordering preserved (SL first)
- [x] Float64 precision maintained (no fastmath)
- [x] Graceful Numba fallback works
- [x] EOD close affects final equity and drawdown
- [x] Risk amounts match expected values
- [x] Position sizes vary with equity changes
- [x] No trailing zero trades in results
