# Regime Scoring Gap Fixes - v3.4

**Date:** February 1, 2026

---

## Summary

Four critical gaps in the regime-level scoring and validation system have been identified and fixed:

1. **Gap A**: Regime bucket drawdown not computed
2. **Gap B**: Drawdown validation not enforced  
3. **Gap C**: Trade count not rewarded in scoring
4. **Gap D**: Drawdown early rejection NOT in optimization loop (wasted compute)

---

## Gap A: Regime Bucket Drawdown Not Computed

### Problem
`_compute_bucket_metrics()` did NOT compute `max_drawdown_pct`. It was completely missing, and `_bucket_to_full_metrics()` defaulted it to 0 (i.e., "perfect drawdown").

### Fix
- Compute proper drawdown from regime bucket equity curve
- Compute proper `total_return_pct = (total_pnl / initial_capital) * 100`
- Default to 100% (worst-case), not 0%

---

## Gap B: Drawdown Validation Not Enforced

### Problem
`_validate_regime_winner()` claimed it checked drawdown, but didn't. High-DD strategies could become winners.

### Fix
- Check `val_drawdown <= fx_val_max_drawdown`
- Check `train_drawdown <= threshold * 1.25`
- Early rejection in `_select_best_for_regime()` before scoring

---

## Gap C: Trade Count Not Rewarded

### Problem
Once minimum thresholds met, trade count wasn't rewarded. A strategy with 25 trades could beat one with 100 trades.

### Fix
- Log-scaled stability factor: `log1p(trades) / log1p(target)`
- More trades → higher score (up to 1.0x at 2× minimum)

---

## Gap D: Early Rejection NOT in Optimization Loop (CRITICAL)

### Problem
Drawdown validation was only a **post-hoc filter** in `_select_best_for_regime()`, running AFTER all Optuna optimization completed. This meant:
- Full backtests ran on high-DD parameter combinations (wasted compute)
- Validation backtests ran even when training DD already exceeded threshold
- Optuna TPE couldn't learn to avoid high-DD parameter regions

### Fix
Added drawdown-based early rejection directly in the Optuna objective:

```python
def objective(trial) -> float:
    # ... run training backtest ...
    
    # EARLY REJECTION: Training drawdown - SKIP VALIDATION if too high
    if train_dd > max_drawdown_pct * 1.25:
        return -500.0  # Skip validation entirely
    
    # ... run validation backtest ...
    
    # EARLY REJECTION: Validation drawdown
    if val_dd > max_drawdown_pct:
        return -500.0
    
    # ... per-regime checks ...
    if regime_dd > max_drawdown_pct:
        continue  # Skip this regime
```

**Key**: Returns -500 (not -1000) so TPE can learn "high DD is bad".

---

## Early Rejection Flow

```
OPTUNA OPTIMIZATION LOOP:
  
  Trial N:
    1. Run TRAINING backtest
    2. Check: train_trades < min? ──YES──► return -1000
    3. Check: train_dd > threshold*1.25? ──YES──► return -500 (SKIP VAL)
       │
       NO
       ▼
    4. Run VALIDATION backtest
    5. Check: val_dd > threshold? ──YES──► return -500
       │
       NO
       ▼
    6. Bucket by regime
    7. For each regime:
       - Check regime_dd > threshold? ──YES──► skip regime
       - Compute score, update best
    8. Return max(regime_scores)
```

---

## Files Modified

### pm_pipeline.py
- `_compute_bucket_metrics()` - Added drawdown computation
- `_bucket_to_full_metrics()` - Fixed default to worst-case
- `_validate_regime_winner()` - Added drawdown validation
- `_select_best_for_regime()` - Added early DD rejection
- `_compute_regime_score()` - Added trade count stability factor
- `_tune_strategy_params()` - Pass max_drawdown_pct to optimizer

### pm_optuna.py
- `optimize()` - Added max_drawdown_pct parameter and early rejection
- `optimize_for_regimes()` - Added max_drawdown_pct and 4-level early rejection
- `_fallback_random_search()` - Added early rejection
- `_fallback_regime_search()` - Added early rejection

---

## Verification Results

```
✓ GAP A: Drawdown properly computed from equity curve
✓ GAP B: High drawdown correctly rejected (22% > 20% threshold)
✓ GAP C: More trades → higher score (44.85 > 43.68)
✓ GAP D: Optuna objective has 4 levels of early rejection
✓ Fallback grid search also has early rejection
✓ max_drawdown_pct passed from RegimeOptimizer to Optuna
```

---

## Compute Savings

With early rejection in the optimization loop:
- High-DD training backtests → validation SKIPPED
- High-DD parameter regions → TPE learns to avoid them
- Per-regime filtering → only valid regimes tracked

This ensures no wasted compute on strategies that would be rejected anyway.
