# Optuna TPE Optimization Implementation - Change Document

## Overview

This implementation replaces the random/grid search hyperparameter optimization with Optuna's Tree-structured Parzen Estimator (TPE) sampler throughout the FX Portfolio Manager codebase.

**Key Benefits:**
- **5-10x fewer evaluations** for equivalent quality results
- **Intelligent search** that learns from previous trials
- **Handles constraints** (e.g., fast_period < slow_period) natively
- **Robust fallback** to random search when Optuna unavailable

---

## Version 1.2 Fixes (January 2026)

### Issue 1: Multi-Regime Objective Bias (FIXED)
**Problem:** The original `optimize_for_regimes()` used `max(regime_scores)` as the objective, which biased TPE toward optimizing only the easiest/highest-scoring regime while ignoring underperforming regimes.

**Solution:** Changed to balanced scoring:
```python
objective_value = mean_weight * mean(scores) + min_weight * min(scores)
```
- Default weights: mean=0.6, min=0.4
- Configurable via `optuna_regime_mean_weight` and `optuna_regime_min_weight`
- Ensures TPE improves overall quality while not ignoring any regime

### Issue 2: Configuration Propagation (FIXED)
**Problem:** `OptunaConfig.from_pipeline_config()` only checked `regime_hyperparam_max_combos` and used a hardcoded seed.

**Solution:**
- Added fallback chain: `regime_hyperparam_max_combos` → `max_param_combos` → 100
- Seed now configurable via `optuna_seed` in config
- Objective weights configurable via `optuna_regime_mean_weight` / `optuna_regime_min_weight`

### New Configuration Fields in `PipelineConfig`:
```python
optuna_seed: int = 42                      # Random seed for reproducibility
optuna_regime_mean_weight: float = 0.6     # Weight for mean regime score
optuna_regime_min_weight: float = 0.4      # Weight for min regime score
```

---

## Files Created/Modified

### 1. NEW: `pm_optuna.py` (Core Optuna TPE Module)

**Purpose:** Self-contained Optuna TPE optimization implementation.

**Key Components:**

| Class/Function | Description |
|----------------|-------------|
| `OptunaConfig` | Configuration dataclass for optimization settings |
| `ParameterSpace` | Handles strategy param grids with constraint enforcement |
| `OptunaTPEOptimizer` | Core optimizer with single and multi-regime optimization |
| `OptimizationResult` | Result dataclass with params, metrics, and statistics |
| `OptimizationStats` | Trial statistics (completed, pruned, timing) |

**Key Features:**
- TPE sampler with configurable startup trials and multivariate mode
- Parameter constraint handling (fast < slow, tenkan < kijun, etc.)
- Multi-regime optimization in single optimization run
- Graceful fallback to random search when Optuna unavailable
- Comprehensive logging with trial statistics

**Usage:**
```python
from pm_optuna import create_optimizer, is_optuna_available

optimizer = create_optimizer(config, backtester, scorer, StrategyRegistry)
result = optimizer.optimize(symbol, strategy_name, param_grid, train_data, val_data, scoring_fn)
```

---

### 2. MODIFIED: `pm_pipeline.py`

**Changes:**

#### a) Imports (Lines 1-50)
```python
# Added Optuna imports with graceful fallback
from pm_optuna import (
    OptunaTPEOptimizer, OptunaConfig, OptimizationResult,
    is_optuna_available, get_optimization_method, create_optimizer, OPTUNA_AVAILABLE
)
```

#### b) `HyperparameterOptimizer` Class (Lines 458-720)

**Before:** Grid search with random sampling when combinations exceeded limit

**After:** 
- Initializes `OptunaTPEOptimizer` in `__init__`
- Primary path uses `_optimize_with_optuna()` when Optuna available
- Fallback path `_optimize_grid_search()` preserves original behavior
- Preserves all validation guardrails and scoring modes

```python
class HyperparameterOptimizer:
    def __init__(self, config):
        # ... existing setup ...
        if OPTUNA_AVAILABLE:
            self.optuna_optimizer = create_optimizer(config, self.backtester, self.scorer, StrategyRegistry)
        else:
            self.optuna_optimizer = None
    
    def optimize(self, symbol, strategy_name, train_features, val_features):
        if self.optuna_optimizer:
            return self._optimize_with_optuna(...)
        return self._optimize_grid_search(...)
```

#### c) `RegimeOptimizer` Class (Lines 949-1020)

**Before:** Grid search for each strategy's param tuning

**After:**
- Initializes `OptunaTPEOptimizer` in `__init__`
- `_tune_strategy_params()` now calls `optimize_for_regimes()` for efficient multi-regime search
- Fallback to `_tune_strategy_params_grid()` when Optuna unavailable

**Key Change:** Multi-regime optimization in single pass
```python
# Instead of N separate backtests per param combo:
regime_results = self.optuna_optimizer.optimize_for_regimes(
    symbol, timeframe, strategy_name, param_grid,
    train_features, val_features, self.REGIMES,
    bucket_trades_fn=self._bucket_trades_by_regime,
    compute_score_fn=self._compute_regime_score
)
```

#### d) Exports (Lines 2100-2145)
Added Optuna utilities to `__all__`:
```python
__all__ = [
    # ... existing exports ...
    'OPTUNA_AVAILABLE',
    'is_optuna_available', 
    'get_optimization_method',
]
```

---

### 3. MODIFIED: `config.json`

**Changes:**
| Parameter | Old Value | New Value | Reason |
|-----------|-----------|-----------|--------|
| `max_param_combos` | 500 | 100 | TPE finds equivalent quality in fewer trials |
| `regime_hyperparam_max_combos` | 250 | 100 | TPE efficiency allows reduction |
| `timeframes` | Included M5 | Removed M5 | M5 noise often unhelpful, reduces runtime |
| `retrain_periods` | 7 values | 5 values | Simplified for efficiency |

---

## Integration Points

### Entry Points Updated

| Entry Point | Integration Method |
|-------------|-------------------|
| `HyperparameterOptimizer.optimize()` | Primary optimization for single strategy |
| `RegimeOptimizer._tune_strategy_params()` | Multi-regime tuning with TPE |

### Scoring Functions Preserved

All existing scoring logic is unchanged:
- `fx_generalization_score()` for validation-aware scoring
- `calculate_fx_opt_score()` for optimization scoring
- `calculate_fx_selection_score()` for selection scoring
- Gap penalty, robustness boost, and all validation guardrails

---

## Runtime Improvements

### Expected Performance

| Scenario | Before (Random) | After (TPE) | Speedup |
|----------|-----------------|-------------|---------|
| Single strategy optimization | 500 trials | 100 trials | 5x |
| Regime tuning per strategy | 250 trials × N strategies | 100 trials × N strategies | 2.5x |
| Full symbol optimization | ~2 hours | ~30-45 min | 3-4x |

### Why TPE is More Efficient

1. **Learns from history:** Each trial informs the next
2. **Models P(x|y):** Focuses on promising parameter regions
3. **Handles discrete/categorical:** Native support for period lengths, multipliers
4. **Respects constraints:** Built-in handling for fast < slow, etc.

---

## Logging Output

### New Log Messages

```
INFO  [EURUSD] Optuna TPE optimization for EMACrossoverStrategy: search_space=4400, trials=100
INFO  [EURUSD] Optimization complete: Trials: 100 (completed=95, pruned=0, failed=5), best_score=45.23, time=12.3s
DEBUG [EURUSD] [H1] EMACrossoverStrategy: 3 tuned variants (Optuna TPE)
```

### Statistics Tracked

- `n_trials`: Total trials run
- `n_completed`: Successfully evaluated trials
- `n_pruned`: Early-stopped trials (when pruning enabled)
- `n_failed`: Failed trials (exceptions)
- `optimization_time_sec`: Total optimization time

---

## Fallback Behavior

When Optuna is not installed:

1. `OPTUNA_AVAILABLE = False` is set at import
2. `is_optuna_available()` returns `False`
3. `get_optimization_method()` returns "Grid Search (fallback)"
4. All optimization paths use original grid/random search logic
5. **No code changes required** - automatic fallback

To install Optuna:
```bash
pip install optuna
```

---

## Testing Checklist

- [ ] `python -m py_compile pm_optuna.py` passes
- [ ] `python -m py_compile pm_pipeline.py` passes
- [ ] Single symbol optimization completes successfully
- [ ] Multi-symbol batch optimization works
- [ ] Regime optimization produces valid winners
- [ ] Fallback works when Optuna not installed
- [ ] Logging shows trial statistics
- [ ] Results quality matches or exceeds grid search

---

## Rollback Instructions

If issues occur:

1. Replace `pm_pipeline.py` with original version
2. Delete `pm_optuna.py`
3. Restore original `config.json` values:
   - `max_param_combos: 500`
   - `regime_hyperparam_max_combos: 250`
   - Add M5 back to timeframes

---

## Future Enhancements

1. **Pruning:** Enable `MedianPruner` for early stopping (requires intermediate score reporting)
2. **Parallel optimization:** Use Optuna's distributed mode for multi-process search
3. **Study persistence:** Save/load optimization history for warm-starting
4. **Hyperband:** Implement successive halving for even faster convergence
