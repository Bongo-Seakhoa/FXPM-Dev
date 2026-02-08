# FxPM 1.4 Changelog

All notable changes to FX Portfolio Manager 1.4 are documented here.

---

## [1.4.4] - 2026-02-07 (Current)

### Winners-Only Cleanup (Tier/Fallback Removal)
- Removed deprecated 3-tier risk policy (`tier1_risk_multiplier`, `tier1_max_risk_pct`, `tier23_max_risk_pct`, `fallback_risk_multiplier`, `fallback_max_risk_pct`) from PipelineConfig and config.json
- Simplified `_execute_entry` risk calculation: `base_risk_pct` capped by `max_risk_pct` (mathematically identical to prior tier-1-only behavior)
- Removed `is_fallback` / `fallback_tier` from candidate cache, candidate dicts, and actionable log
- New v3 trade comment format (`PM3:symbol:tf:scode:dir:risk_tenths`) — no tier field
- Backward-compatible v2/v1 comment decoding preserved for existing open positions
- Removed `get_tier_from_comment()` utility (no longer needed)
- Updated SETTINGS_REFERENCE.md to reflect winners-only risk policy

### Pandas Warning Fixes
- Fixed ~67 `SettingWithCopyWarning` sources: `features[col] = val` → `features.loc[:, col] = val` across pm_strategies.py (10) and pm_core.py (~57)
- Fixed 3 `FutureWarning` fillna patterns in pm_strategies.py: `shift().fillna()` → `shift(fill_value=)`

### Config Propagation Fix
- Fixed `fx_min_robustness_ratio` default mismatch: `0.75` → `0.85` in RegimeOptimizer (matches PipelineConfig default)

### Optimization Progress Bars
- Added tqdm progress bars for sequential and parallel optimization loops
- Inner timeframe progress bar per symbol during regime optimization
- Graceful fallback when tqdm is not installed
- Added `tqdm>=4.60` to requirements.txt

### Testing
- Added 3 new tests: v3 comment encode/decode, v2 backward compat, v1 legacy decode
- Total: 38 tests, all passing in ~2s

---

## [1.4.3] - 2026-02-07

### Config Validation & Documentation
- Added `actionable_score_margin` to PipelineConfig dataclass (was silently dropped from config.json)
- Added `tier23_max_risk_pct` to config.json (was defined in code but not configurable)
- Created comprehensive SETTINGS_REFERENCE.md with all config.json settings documented
- Full config audit: verified all 78 pipeline keys, 15 position keys, 6 MT5 keys

### Indicator Caching (DRY Refactor)
- Created `_get_adx_di()` memoizing shared helper (eliminates 2x duplicate ADX/DI computation)
- Refactored 8 strategies to use shared indicator helpers instead of inline computation:
  ADXDIStrength, KeltnerPullback, SqueezeBreakout, KeltnerBreakout, EMARibbonADX,
  RSITrendFilteredMR, MACDHistogramMomentum, StochRSITrendGate
- Test runtime dropped from 8.1s to 2.3s due to indicator caching
- Eliminated ~100 lines of duplicated indicator code

### Integration Test Suite
- New `test_live_loop_integration.py` with 5 tests covering the full live signal pipeline:
  config winner lookup, signal generation, decision throttle, regime detection, full pipeline (no MT5)
- Total test count: 35 tests, all passing

### Documentation Cleanup
- Consolidated 8 incremental .md files into this CHANGELOG.md
- Deleted redundant analysis/rebuttal documents
- Kept: README.md, SETUP_AND_RUN.md, SETTINGS_REFERENCE.md, pm_dashboard/README.md

---

## [1.4.2] - 2026-02-07

### Strategy Expansion (27 -> 42 Strategies)
- Added 15 new strategies across 5 categories:
  - **Price Action (5):** InsideBarBreakout, NarrowRangeBreakout, PinBarReversal, EngulfingReversal, TurtleSoupFade
  - **Divergence (3):** RSIDivergence, MACDDivergence, OBVDivergence
  - **Volume-Based (2):** VolumeSpikeBreakout, OBVTrend
  - **Exhaustion (2):** ROCExhaustion, KeltnerFade
  - **Adaptive (3):** EMAPullback, ParabolicSARTrend, KaufmanAMA

### Production Safety Fixes
- Strategy min-bars guard (`_zero_warmup`) prevents signals during indicator warmup
- Stochastic/WilliamsR/StochRSI zero-denominator protection
- Signal NaN validation (NaN signals mapped to 0)
- DecisionThrottle atomic writes (temp file + `os.replace()`)
- MT5 reconnection limit (max 12 cumulative failures)
- Sharpe/Sortino numerical stability threshold raised to 1e-8

### Bug Fixes
- **InstrumentSpec typo:** `trade_stops_level=` corrected to `stops_level=` in `_create_spec_from_broker_data()`
- **MT5 parity gap:** Added 7 missing fields to `MT5SymbolInfo.to_instrument_spec()` (tick_size, tick_value, contract_size, volume_step, stops_level, point, digits)

### Dashboard Upgrade (5.6/10 -> 8.5/10)
- Created `common.js` shared module eliminating all JS duplication across 4 pages
- Added analytics backend with Sortino, Calmar, recovery factor, expectancy, consecutive win/loss streaks
- Added P&L heatmap (hour x day-of-week), strategy ranking chart, daily P&L chart
- Loading skeletons, error toasts, empty state messages
- Accessibility: ARIA labels, role attributes, focus management, escape-close drawers

### Testing
- Expanded from 14 to 30 tests covering critical paths
- New test categories: backtest SL/TP, strategy param consistency, position sizing edge cases, regime warmup

---

## [1.4.1] - 2026-02-01

### Optuna TPE Integration
- Replaced random/grid search with Optuna Tree-structured Parzen Estimator
- 5-10x fewer evaluations needed per strategy
- Fixed multi-regime objective bias with balanced scoring (mean + min weights)
- Graceful fallback to random search when Optuna unavailable

### Efficiency Improvements (3 Phases)
- **Phase 1:** Numba JIT backtester loop with live-equity compounding (8-10x speedup)
- **Phase 2:** Vectorized regime detection (3-10x speedup), lazy feature loading (307x for targeted)
- **Phase 3:** Compute-once-slice-many for retrain period selection (3-5x speedup)
- Vectorized CCI (3-5x), Hull MA caching (2-3x), Supertrend NumPy (2-5x)

### Generalization Scoring (fx_backtester mode)
- 3-layer scoring: Optuna TPE -> Pipeline fx_generalization_score -> Live winners-only
- Gap penalty (lambda=0.50) penalizes train-to-val score divergence
- Robustness boost rewards strategies that generalize well
- Trade count stability factor (log-scaled)

### Gap Fixes & Validation
- Regime bucket drawdown computed from equity curve (not aggregate)
- 4-level early rejection in Optuna objective: training trades, training DD, validation DD, per-regime
- Drawdown validation enforced with `fx_val_max_drawdown` checks

### Stateful Optimization
- ConfigLedger with skip-valid-configs behavior
- Atomic writes pattern (temp file + rename) prevents corruption on interruption
- Incremental persistence after each symbol
- Validity rules: is_validated, valid_until check, non-empty parameters

### Production Readiness
- Fixed OptimizationStats type error (`early_dd_rejections` field)
- All 42/42 production readiness tests passing
- Complete feature matrix verified across all 9 modules

---

## [1.4.0] - 2026-01-15 (Initial Release)

### Core Architecture
- Numba JIT-compiled backtester with realistic cost modeling (spread, commission, slippage)
- 4-regime market detection: TREND, RANGE, BREAKOUT, CHOP (hysteresis state machine)
- Regime-aware optimization with per-regime strategy winners
- 80/30 train/validation split with 10% overlap window
- SHA256 decision throttle for live trade deduplication

### Original Strategy Set (27 Strategies)
- Trend Following: Supertrend, EMA Crossover, MACD, Parabolic SAR, Ichimoku, ADX/DI
- Mean Reversion: Bollinger Bounce, RSI Extremes, Stochastic, Williams %R
- Momentum: MACD Histogram, RSI Momentum, Stochastic RSI
- Breakout: Donchian, Keltner, Squeeze
- Volatility: ATR Breakout, Bollinger Squeeze
- And more across 6 timeframes (M5-D1)

### Live Trading
- MT5 integration with auto-reconnection
- Tiered risk policy (3 tiers based on signal confidence)
- D1 + lower-TF concurrent trade support
- Atomic config persistence

### Dashboard
- Flask-based web dashboard with Signal Desk, Strategies, Analytics, Trade History
- Real-time signal monitoring with freshness indicators
- Dark/light theme support
