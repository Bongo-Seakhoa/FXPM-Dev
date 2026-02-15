# FX Portfolio Manager v3.3

A production-ready automated trading system featuring regime-aware strategy selection, stateful optimization with incremental persistence, Numba-accelerated backtesting, and live execution via MetaTrader 5.

---

## Documentation Notes

- Primary run/setup guide: `SETUP_AND_RUN.md`
- Dashboard guide: `pm_dashboard/README.md`
- Margin protection policy: `README.md` (Risk Management -> Margin Protection)
- Patch notes: `PATCH_NOTES.md`
- Archived historical analysis notes: `docs/archive/`

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [Trading Strategies](#trading-strategies)
- [Regime Detection](#regime-detection)
- [Risk Management](#risk-management)
- [Stateful Optimization](#stateful-optimization)
- [Performance Optimizations](#performance-optimizations)
- [Output Files](#output-files)
- [Performance Metrics](#performance-metrics)
- [Repository Hygiene](#repository-hygiene)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Version History](#version-history)

---

## Overview

The FX Portfolio Manager is a fully automated trading pipeline that:

1. **Detects Market Regimes** - Classifies markets as TREND, RANGE, BREAKOUT, or CHOP
2. **Selects Strategies** - Tests 50 strategies across 6 timeframes per regime
3. **Optimizes Parameters** - Grid/Optuna search with validation-aware scoring
4. **Validates Robustness** - Gap penalty and robustness ratio enforcement
5. **Persists Incrementally** - Never loses optimization progress (atomic saves)
6. **Executes Trades** - Live MT5 execution with broker-accurate risk management
7. **Adapts Continuously** - Auto-retraining when configurations expire

### What Makes It Different

| Feature | Traditional Systems | This System |
|---------|---------------------|-------------|
| Strategy Selection | Single strategy | 50 strategies compete per regime |
| Market Adaptation | Static | Regime-aware (TREND/RANGE/BREAKOUT/CHOP) |
| Parameter Tuning | Manual or random | Systematic grid/Optuna with validation |
| Overfitting Prevention | None/minimal | Gap penalty + robustness ratio |
| Risk Calculation | Pip-based (inaccurate) | MT5 tick-based math (broker-accurate) |
| Execution Timing | Often has lookahead bias | Signal bar -> next bar entry (verified) |
| Optimization State | Lost on interrupt | **Stateful ledger with atomic saves** |
| Position Sizing | Fixed or simple | **Live-equity compounding** |
| Backtesting Speed | Pure Python | **Numba JIT (3-10x faster)** |

---

## Repository Hygiene

The repository now treats runtime artifacts as non-source outputs.

- Ignored by default (`.gitignore`): caches, logs, generated trade outputs, and downloaded market data files.
- Already tracked artifacts were removed from the git index with `git rm --cached` (local files remain on disk).

If a clone still tracks runtime files, run:

```bash
git rm -r --cached -f --ignore-unmatch __pycache__ pm_dashboard/__pycache__ tests/__pycache__ logs pm_outputs data/.cache data/*.csv last_trade_log.json last_actionable_log.json
```

Optional local cleanup of ignored artifacts (destructive for ignored files only):

```bash
git clean -fdX
```

---

## Key Features

### Stateful Optimization Ledger (NEW in v3.3)
- **Skip Valid Configs**: Re-running optimization only processes symbols that need it
- **Incremental Persistence**: Saves after each symbol (never loses progress)
- **Atomic Writes**: Config file is never corrupted, even on interruption
- **Explicit Overwrite**: Use `--overwrite` flag to force re-optimization

### Numba-Accelerated Backtesting (NEW in v3.3)
- **3-10x Speedup**: JIT-compiled main loop
- **Live-Equity Sizing**: Position sizes compound with equity changes
- **Quality Preserved**: Same SL/TP ordering, float64 precision, no fastmath

### Regime-Aware Strategy Selection
- **4 Market Regimes**: TREND, RANGE, BREAKOUT, CHOP
- **Per-Regime Winners**: Best strategy selected for each (timeframe, regime) combination
- **Winners-Only Live Execution**: Live trading only uses validated winners for the exact (timeframe, regime); no fallback configs
- **Hysteresis State Machine**: Prevents rapid regime flipping
- **CHOP Protection**: Optional hard no-trade in choppy markets

### Broker-Accurate Risk Engine
- **MT5 Contract Math**: Uses `order_calc_profit()` for precise loss-at-SL calculations
- **Multi-Asset Support**: Works correctly for forex, indices, metals, crypto
- **Volume Normalization**: Respects broker min/max/step constraints
- **Hard Safety Cap**: Configurable maximum risk per trade (default 5%)

### Generalization-Focused Validation
- **Gap Penalty**: Penalizes train->validation performance degradation
- **Robustness Ratio**: Validates val_score/train_score ratio
- **Minimum Trade Thresholds**: Per-regime trade count requirements
- **Top-K Validation**: Only validates promising candidates

### Production-Ready Execution
- **Decision Throttling**: Prevents duplicate signals, persists across restarts
- **Feature Caching**: Skips recomputation when no new bar
- **Connection Recovery**: Auto-reconnect with retry logic
- **Comprehensive Logging**: Full audit trail of all decisions

---

## Architecture

### Module Overview

```
FX_Portfolio_Manager/
├── pm_core.py           # Configuration, data loading, backtesting, scoring
├── pm_strategies.py     # 50 trading strategies with param grids
├── pm_pipeline.py       # Optimization pipeline, ConfigLedger, PortfolioManager
├── pm_main.py           # Application entry, live trading loop
├── pm_mt5.py            # MetaTrader 5 integration
├── pm_position.py       # Position management and sizing
├── pm_regime.py         # Market regime detection (Numba-accelerated)
├── pm_regime_tuner.py   # Regime parameter optimization
├── pm_optuna.py         # Optuna TPE optimizer (optional)
├── config.json          # Runtime configuration
├── pm_configs.json      # Saved strategy configurations (auto-generated)
├── regime_params.json   # Tuned regime parameters (optional)
├── last_trade_log.json      # Decision throttle state (auto-generated)
├── last_actionable_log.json # Last actionable decision feed (auto-generated)
├── data/                # Historical data cache
└── pm_outputs/          # Logs and reports
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    STATEFUL OPTIMIZATION PHASE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  pm_configs.json ◄── ConfigLedger ──► PortfolioManager              │
│        │                                      │                      │
│        ▼                                      │                      │
│  Check: Valid?  ──YES──► SKIP (log reason)   │                      │
│        │                                      │                      │
│        NO                                     │                      │
│        │                                      ▼                      │
│        └────────────────────► OptimizationPipeline                  │
│                                      │                               │
│                                      ▼                               │
│                              RegimeOptimizer                         │
│                              ├─ Screen strategies                    │
│                              ├─ Tune top-K per regime                │
│                              └─ Validate winners                     │
│                                      │                               │
│                                      ▼                               │
│                         SAVE (atomic) ──► pm_configs.json            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         LIVE TRADING PHASE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  pm_configs.json ──► LiveTrader                                      │
│                           │                                          │
│       ┌───────────────────┼───────────────────┐                     │
│       ▼                   ▼                   ▼                     │
│   [Symbol 1]          [Symbol 2]          [Symbol N]                │
│       │                   │                   │                     │
│       ▼                   ▼                   ▼                     │
│  ┌─────────┐         ┌─────────┐         ┌─────────┐               │
│  │Get Bars │         │Get Bars │         │Get Bars │               │
│  │Compute  │         │Compute  │         │Compute  │               │
│  │Features │         │Features │         │Features │               │
│  │+ Regime │         │+ Regime │         │+ Regime │               │
│  └────┬────┘         └────┬────┘         └────┬────┘               │
│       │                   │                   │                     │
│       ▼                   ▼                   ▼                     │
│  Select best         Select best         Select best               │
│  (tf, regime)        (tf, regime)        (tf, regime)              │
│  candidate           candidate           candidate                  │
│       │                   │                   │                     │
│       └───────────────────┴───────────────────┘                     │
│                           │                                          │
│                           ▼                                          │
│                    Risk Management                                   │
│                    ├─ MT5 contract math                             │
│                    ├─ Live-equity sizing                            │
│                    ├─ Volume normalization                          │
│                    └─ Hard cap check                                │
│                           │                                          │
│                           ▼                                          │
│                    MT5 Order Execution                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Installation

### Prerequisites

- **Python 3.8+** (3.10+ recommended)
- **MetaTrader 5** terminal installed and logged in
- **Windows OS** (MT5 Python API requires Windows)

### Step 1: Install Python Dependencies

```bash
# Required packages
pip install pandas numpy MetaTrader5

# Optional (for better performance)
pip install numba    # 3-10x faster backtesting
pip install optuna   # Bayesian hyperparameter optimization
```

Or with a virtual environment (recommended):

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Install dependencies
pip install pandas numpy MetaTrader5 numba optuna
```

### Step 2: Download Files

Place all files in a single folder:

```
FX_Portfolio_Manager/
├── pm_core.py
├── pm_strategies.py
├── pm_pipeline.py
├── pm_main.py
├── pm_mt5.py
├── pm_position.py
├── pm_regime.py
├── pm_regime_tuner.py
├── pm_optuna.py
├── config.json
└── data/              (create this folder)
```

### Step 3: Configure MetaTrader 5

1. Open MetaTrader 5 and log in to your account
2. Enable **AutoTrading** (Ctrl+E or click the AutoTrading button)
3. Ensure the terminal stays open while the script runs

### Step 4: Verify Installation

```bash
python -c "from pm_main import FXPortfolioManagerApp; print('Installation verified!')"
```

---

## Quick Start

```bash
# 1. Run optimization (required first time)
#    - Skips symbols with valid configs
#    - Saves progress after each symbol
python pm_main.py --optimize

# 2. Force re-optimization (if needed)
python pm_main.py --optimize --overwrite

# 3. Paper trade to verify (run for a few days)
python pm_main.py --trade --paper

# 4. Go live when confident
python pm_main.py --trade

# 5. Full autonomous mode (auto-retrains when configs expire)
python pm_main.py --trade --auto-retrain
```

---

## Configuration

### config.json Structure

```json
{
  "pipeline": {
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    "max_bars": 500000,
    
    "train_pct": 80.0,
    "val_pct": 30.0,
    "overlap_pct": 10.0,
    
    "initial_capital": 10000.0,
    "risk_per_trade_pct": 1.0,
    
    "use_spread": true,
    "use_commission": true,
    "use_slippage": true,
    "slippage_pips": 0.5,
    
    "max_param_combos": 150,
    "optimization_max_workers": 1,
    "min_trades": 25,
    "min_robustness": 0.20,
    
    "optimization_valid_days": 14,
    
    "scoring_mode": "fx_backtester",
    
    "fx_opt_min_trades": 15,
    "fx_val_min_trades": 15,
    "fx_val_max_drawdown": 18.0,
    "fx_gap_penalty_lambda": 0.70,
    "fx_min_robustness_ratio": 0.80,
    
    "timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
    "retrain_periods": [14, 30, 60, 90, 120],
    
    "use_regime_optimization": true,
    "regime_min_train_trades": 25,
    "regime_min_val_trades": 15,
    "regime_chop_no_trade": true
  },

  "position": {
    "risk_per_trade_pct": 1.0,
    "risk_basis": "balance",
    "max_risk_pct": 5.0,
    "auto_widen_sl": true
  },

  "mt5": {
    "login": 0,
    "password": "",
    "server": "",
    "path": ""
  },

  "broker_specs_path": "broker_specs.json",
  "instrument_spec_defaults": {
    "commission_per_lot": 7.0
  },
  "instrument_specs": {
    "AUDCAD": { "inherit": "USDCAD" },
    "AUDCHF": { "inherit": "EURCHF" },
    "CHFJPY": { "inherit": "USDJPY" }
  },

  "symbols": [
    "EURUSD", "GBPUSD", "USDJPY", "XAUUSD"
  ]
}
```

Note: `val_pct` is informational only. Actual validation size is controlled by
`train_pct` and `overlap_pct`.
Note: Live trading is **winners-only** and non-tiered. `default_config` is not used for live entries. Use `live_risk_multiplier` and `live_max_risk_pct` for live risk sizing.
Note: Current production `config.json` symbol universe is **77 symbols**.

`instrument_specs` entries can use `"inherit": "SYMBOL"` to clone an existing
spec as a starting point (handy for new symbols before you export broker specs).

### Key Configuration Options

| Setting | Description | Default |
|---------|-------------|---------|
| `optimization_valid_days` | Days before config expires | 14 |
| `risk_per_trade_pct` | Risk per trade as % of equity | 1.0 |
| `max_risk_pct` | Hard cap on risk (safety) | 5.0 |
| `fx_min_robustness_ratio` | Minimum val/train score ratio | 0.80 |
| `regime_chop_no_trade` | Block trades in CHOP regime | true |
| `broker_specs_path` | Path to MT5-exported broker specs (optional) | broker_specs.json |
| `instrument_specs` | Per-symbol instrument overrides (supports `inherit`) | {} |
| `optimization_max_workers` | Max parallel workers for optimization (1 = sequential) | 1 |
| `margin_entry_block_level` | Block new entries below this margin level % | 100.0 |
| `margin_recovery_start_level` | Start forced closures below this % | 80.0 |
| `margin_panic_level` | Aggressive forced closures below this % | 65.0 |

### Scoring Audit Controls

These controls were added for scoring-audit implementation and are all configurable in `pipeline`.

| Setting | Description | Default |
|---------|-------------|---------|
| `regime_validation_top_k` | Max ranked regime candidates to validate in descent order | 5 |
| `scoring_use_continuous_dd` | Use smooth DD penalty instead of legacy DD buckets | true |
| `scoring_use_sortino_blend` | Blend Sortino into risk-adjusted scoring | true |
| `scoring_use_tail_risk` | Penalize severe left-tail outcomes (`worst_5pct_r`) | true |
| `scoring_use_consistency` | Penalize excessive losing streaks | true |
| `scoring_use_trade_frequency_bonus` | Apply conservative trade-count confidence bonus | true |
| `optuna_objective_blend_enabled` | Enable bounded train/val blend in Optuna objective | true |
| `optuna_objective_train_weight` | Train weight in Optuna objective blend | 0.80 |
| `optuna_objective_val_weight` | Validation weight in Optuna objective blend | 0.20 |

Data split contract:
- Keep `train_pct=80.0`, `val_pct=30.0`, `overlap_pct=10.0` unless doing a dedicated split-policy redesign.
- Optuna objective blending does **not** change split windows; it only changes objective weighting.

---

## Usage

### Command Line Interface

```bash
# Optimization
python pm_main.py --optimize              # Skip valid configs
python pm_main.py --optimize --overwrite  # Force re-optimize all

# Trading
python pm_main.py --trade                 # Live trading
python pm_main.py --trade --paper         # Paper trading (no real orders)
python pm_main.py --trade --auto-retrain  # With automatic retraining

# Status
python pm_main.py --status                # Show current portfolio status

# Options
python pm_main.py --symbols EURUSD GBPUSD # Specific symbols only
python pm_main.py --log-level DEBUG       # Verbose logging
python pm_main.py --config myconfig.json  # Custom config file
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--optimize` | Run optimization |
| `--overwrite` | Force re-optimization (ignore validity) |
| `--trade` | Start live trading loop |
| `--paper` | Paper trading mode (no real orders) |
| `--auto-retrain` | Auto-retrain when configs expire |
| `--status` | Print portfolio status |
| `--symbols` | Specific symbols to process |
| `--config` | Path to config JSON file |
| `--data-dir` | Data directory path |
| `--output-dir` | Output directory path |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) |

---

## Live Readiness Checklist

1. Run a paper session for at least 1-2 weeks on the same symbols and timeframes.
2. Confirm MT5 has full history for each symbol/timeframe (open chart, scroll back, or use History Center).
3. Verify winners-only behavior in logs: only validated winners produce `Selected` lines.
4. Ensure risk caps and stop rules behave as expected (no unexpected `SKIPPED_RISK_CAP` spikes).
5. Check that `pm_configs.json` is up to date and not expired for your symbols.
6. Keep MT5 terminal open, logged in, and AutoTrading enabled.

---

## Trading Strategies

### Available Strategies (50)

| Category | Strategies |
|----------|------------|
| **Trend Following (18)** | EMACrossoverStrategy, SupertrendStrategy, MACDTrendStrategy, ADXTrendStrategy, IchimokuStrategy, HullMATrendStrategy, EMARibbonADXStrategy, AroonTrendStrategy, ADXDIStrengthStrategy, KeltnerPullbackStrategy, OBVDivergenceStrategy, EMAPullbackContinuationStrategy, ParabolicSARTrendStrategy, KaufmanAMATrendStrategy, VortexTrendStrategy, ElderRayBullBearStrategy, MarketStructureBOSPullbackStrategy, FibonacciRetracementPullbackStrategy |
| **Mean Reversion (18)** | RSIExtremesStrategy, BollingerBounceStrategy, ZScoreMRStrategy, StochasticReversalStrategy, CCIReversalStrategy, WilliamsRStrategy, RSITrendFilteredMRStrategy, StochRSITrendGateStrategy, FisherTransformMRStrategy, ZScoreVWAPReversionStrategy, TurtleSoupReversalStrategy, PinBarReversalStrategy, EngulfingPatternStrategy, RSIDivergenceStrategy, MACDDivergenceStrategy, KeltnerFadeStrategy, ROCExhaustionReversalStrategy, LiquiditySweepReversalStrategy |
| **Breakout/Momentum (14)** | DonchianBreakoutStrategy, VolatilityBreakoutStrategy, MomentumBurstStrategy, SqueezeBreakoutStrategy, KeltnerBreakoutStrategy, PivotBreakoutStrategy, MACDHistogramMomentumStrategy, InsideBarBreakoutStrategy, NarrowRangeBreakoutStrategy, VolumeSpikeMomentumStrategy, ATRPercentileBreakoutStrategy, TRIXMomentumStrategy, FractalSRZoneBreakRetestStrategy, SupplyDemandImpulseRetestStrategy |

### Strategy Selection Process

1. **Screen all strategies** on training data
2. **Select top-K** per (timeframe, regime) combination
3. **Hyperparameter tune** the top candidates
4. **Validate** on out-of-sample data
5. **Rank by quality score** (robustness-adjusted)

---

## Regime Detection

### Market Regimes

| Regime | Description | Typical Strategies |
|--------|-------------|-------------------|
| **TREND** | Clear directional move | Supertrend, EMA Crossover, MACD |
| **RANGE** | Sideways, mean-reverting | RSI Reversal, Bollinger Bounce |
| **BREAKOUT** | Volatility expansion | Squeeze Breakout, Donchian |
| **CHOP** | Noisy, no clear direction | No trade (optional) |

### Regime Scoring Components

- **Trend Score**: ADX, EMA slope alignment
- **Range Score**: Bollinger bandwidth compression
- **Breakout Score**: Squeeze release, structure breaks
- **Chop Score**: Whipsaw frequency, direction flips

### Hysteresis State Machine

Prevents rapid regime switching:
- `k_confirm`: Bars to confirm regime switch
- `gap_min`: Minimum score gap to switch
- `k_hold`: Minimum bars to hold regime

---

## Risk Management

### Position Sizing (Live-Equity Compounding)

```
risk_amount = current_equity × (risk_per_trade_pct / 100)
loss_per_lot = distance_to_stop × tick_value
position_size = floor(risk_amount / loss_per_lot, volume_step)
```

The system uses **live equity** for sizing, meaning:
- Winning streaks -> larger positions (compounding)
- Losing streaks -> smaller positions (risk reduction)

### Safety Features

| Feature | Description |
|---------|-------------|
| **Hard Cap** | Skips trade if risk > max_risk_pct |
| **Auto-Widen SL** | Widens SL to meet broker minimum stop distance |
| **Volume Floor** | Uses floor() not round() to avoid exceeding target |
| **Position Check** | Verifies no existing position before entry |
| **Rate Limiting** | Prevents rapid order submission |
| **Margin Guard** | Blocks entries and force-closes losers under margin stress |

### Margin Protection (Black Swan Guard)

Cycle-based margin protection integrated into the live trading loop.
Uses MT5-native `margin_level` for broker-accurate gating:

| State | Margin Level | Behavior |
|---|---|---|
| NORMAL | >= 100% | Full operation, no restrictions |
| BLOCKED | 80-99% | New entries blocked, existing positions untouched |
| RECOVERY | 65-79% | Entries blocked + 1 worst-loser closed per cycle |
| PANIC | < 65% | Entries blocked + up to 3 closures per cycle |

Configured via the `config.json` pipeline section. Policy summary is in this section and in `SETUP_AND_RUN.md`.

---

## Stateful Optimization

### How It Works

1. **Load existing configs** from `pm_configs.json`
2. **Check validity** for each symbol:
   - Valid (not expired, validated) -> **SKIP**
   - Expired/missing/invalid -> **OPTIMIZE**
3. **Save incrementally** after each symbol (atomic write)
4. **Never lose progress** even on interruption

### CLI Behavior

```bash
# Default: Skip valid configs
python pm_main.py --optimize
# Output:
# SKIP EURUSD: valid until 2026-02-14 (13 days remaining)
# OPTIMIZE USDJPY: expired 3 days ago
# OPTIMIZE AUDUSD: missing

# Force re-optimization
python pm_main.py --optimize --overwrite
# Output:
# OVERWRITE MODE: ignoring validity checks
# OPTIMIZE EURUSD: overwrite enabled
# OPTIMIZE USDJPY: overwrite enabled
```

### Atomic Write Pattern

Configs are saved using temp file + rename to prevent corruption:
```
1. Write to pm_configs.json.tmp
2. fsync for durability
3. Atomic rename to pm_configs.json
```

---

## Performance Optimizations

### Numba JIT Compilation

The backtester main loop is JIT-compiled for 3-10x speedup:

```bash
# Automatic when numba is installed
pip install numba
```

Features:
- **Live-equity sizing** inside JIT loop (compounding preserved)
- **SL/TP ordering preserved** (SL checked first)
- **Float64 precision** (no fastmath)
- **Graceful fallback** to pure Python if numba unavailable

### Lazy Feature Loading

Only computes features needed by each strategy:
```python
# Instead of computing all 66 features:
features = FeatureComputer.compute_all(df)  # ~2.1s

# Compute only what's needed:
features = FeatureComputer.compute_required(df, required_features)  # ~0.007s
```

### Performance Comparison

| Component | Before v3.3 | After v3.3 | Speedup |
|-----------|-------------|------------|---------|
| Full feature computation | 2.15s | 2.15s | baseline |
| Lazy feature computation | 2.15s | 0.007s | **307x** |
| Backtester loop (Numba) | ~0.5s | ~0.05s | **~10x** |
| Regime detection (Numba) | ~9s | ~1.8s | **5x** |

---

## Output Files

### Directory Structure

```
FX_Portfolio_Manager/
├── pm_configs.json              # Strategy configurations (IMPORTANT!)
├── regime_params.json           # Tuned regime parameters (optional)
├── last_trade_log.json          # Decision throttle state
├── last_actionable_log.json     # Last actionable decision feed
├── data/
│   ├── EURUSD_M5.csv           # Historical data cache
│   ├── EURUSD_H1.csv
│   └── ...
└── pm_outputs/
    ├── optimization_summary.csv # Summary of all results
    └── logs/
        └── pm_YYYYMMDD.log      # Daily log files
```

### pm_configs.json Structure

```json
{
  "EURUSD": {
    "symbol": "EURUSD",
    "strategy_name": "SupertrendStrategy",
    "timeframe": "H1",
    "parameters": {"atr_period": 10, "multiplier": 3.0},
    "is_validated": true,
    "validation_reason": "passed all checks",
    "optimized_at": "2026-02-01T10:30:00",
    "valid_until": "2026-02-15T10:30:00",
    "composite_score": 75.5,
    "robustness_ratio": 0.85,
    "regime_configs": {
      "H1": {
        "TREND": {"strategy_name": "SupertrendStrategy", ...},
        "RANGE": {"strategy_name": "BollingerBounceStrategy", ...}
      }
    }
  }
}
```

---

## Performance Metrics

### Backtest Metrics

| Metric | Description |
|--------|-------------|
| **Sharpe Ratio** | Risk-adjusted return (equity curve-based, annualized) |
| **Sortino Ratio** | Downside risk-adjusted return |
| **Profit Factor** | Gross profit / Gross loss |
| **Win Rate** | Percentage of winning trades |
| **Max Drawdown** | Maximum peak-to-trough decline |
| **Calmar Ratio** | Annual return / Max drawdown |
| **Expectancy** | Average expected profit per trade |

### R-Multiple Statistics

| Metric | Description |
|--------|-------------|
| **Mean R** | Average R-multiple per trade |
| **Median R** | Median R-multiple |
| **% Positive R** | Percentage of trades with R > 0 |
| **Worst 5% R** | Average R of worst 5% of trades |

---

## Troubleshooting

### Common Issues

#### "Failed to connect to MT5"
1. Ensure MetaTrader 5 is running
2. Ensure you're logged into your account
3. Enable AutoTrading (Ctrl+E)
4. Try restarting MT5 and the script

#### "Symbol not found: EURUSD"
Your broker uses different symbol names. Check exact names:
1. Open MT5 Market Watch
2. Find the exact symbol name (e.g., `EURUSD.a`, `EURUSDm`)
3. Use that name in your config

#### "No valid strategy found"
- Lower `min_trades` (try 15-20)
- Lower `fx_min_robustness_ratio` (try 0.70)
- Ensure sufficient historical data exists

#### "SKIP: min lot would exceed max_risk_pct"
Broker minimum lot size exceeds your risk budget:
- Increase `risk_per_trade_pct`
- Increase `max_risk_pct` (with caution)
- Remove that symbol

#### "Corrupted JSON in pm_configs.json"
Config file was corrupted (rare). Fix or remove:
```bash
# Backup and restart
mv pm_configs.json pm_configs.json.bak
python pm_main.py --optimize
```

### Debug Mode

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

---

## Development

### Adding a New Strategy

1. Create class in `pm_strategies.py`:

```python
class MyNewStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "MyNewStrategy"
    
    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING
    
    def get_default_params(self) -> Dict[str, Any]:
        return {
            'period': 20,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 3.0
        }
    
    def get_required_features(self) -> Set[str]:
        return {f'EMA_{self.params.get("period", 20)}', 'ATR_14'}
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        signals = pd.Series(0, index=features.index)
        # Your signal logic here
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 25, 30],
            'sl_atr_mult': [1.5, 2.0, 2.5],
            'tp_atr_mult': [2.0, 3.0, 4.0]
        }
```

2. Register in `StrategyRegistry._strategies`:

```python
'MyNewStrategy': MyNewStrategy,
```

### Running Tests

```bash
# Syntax check all modules
python -m py_compile pm_core.py pm_strategies.py pm_pipeline.py pm_main.py

# Test imports
python -c "from pm_main import FXPortfolioManagerApp; print('OK')"

# Test single symbol
python pm_main.py --optimize --symbols EURUSD --log-level DEBUG
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| **3.3** | Feb 2026 | Stateful optimization ledger, Numba JIT backtester, live-equity sizing, atomic saves |
| 3.2 | Feb 2026 | Efficiency improvements v2, lazy feature loading |
| 3.1 | Jan 2026 | Optuna TPE optimization, enhanced validation |
| 3.0 | Jan 2026 | Regime-aware optimization, hyperparameter tuning |
| 2.0 | 2025 | fx_backtester scoring mode, generalization controls |
| 1.0 | 2024 | Initial release |

---

## License

MIT License - Use at your own risk.

---

## Disclaimer

**IMPORTANT RISK WARNING**

- Trading forex, CFDs, and derivatives involves substantial risk of loss
- Past performance is not indicative of future results
- This software is for educational and research purposes
- Always test thoroughly in paper mode before live trading
- Never risk more than you can afford to lose
- The authors assume no liability for trading losses

---

*FX Portfolio Manager v3.3 - Quality First, Efficiency Second*
