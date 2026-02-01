# FX Portfolio Manager v3.0

A production-ready automated trading system featuring regime-aware strategy selection, hyperparameter optimization, and live execution via MetaTrader 5.

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
- [Output Files](#output-files)
- [Performance Metrics](#performance-metrics)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

---

## Overview

The FX Portfolio Manager is a fully automated trading pipeline that:

1. **Detects Market Regimes** - Classifies markets as TREND, RANGE, BREAKOUT, or CHOP
2. **Selects Strategies** - Tests 28 strategies across 6 timeframes per regime
3. **Optimizes Parameters** - Grid search with validation-aware scoring
4. **Validates Robustness** - Gap penalty and robustness ratio enforcement
5. **Executes Trades** - Live MT5 execution with broker-accurate risk management
6. **Adapts Continuously** - Auto-retraining when configurations expire

### What Makes It Different

| Feature | Traditional Systems | This System |
|---------|---------------------|-------------|
| Strategy Selection | Single strategy | 28 strategies compete per regime |
| Market Adaptation | Static | Regime-aware (TREND/RANGE/BREAKOUT/CHOP) |
| Parameter Tuning | Manual or random | Systematic grid search with validation |
| Overfitting Prevention | None/minimal | Gap penalty + robustness ratio |
| Risk Calculation | Pip-based (inaccurate for CFDs) | MT5 contract math (broker-accurate) |
| Execution Timing | Often has lookahead bias | Signal bar → next bar entry (verified) |

---

## Key Features

### Regime-Aware Strategy Selection
- **4 Market Regimes**: TREND, RANGE, BREAKOUT, CHOP
- **Per-Regime Winners**: Best strategy selected for each (timeframe, regime) combination
- **Hysteresis State Machine**: Prevents rapid regime flipping
- **CHOP Protection**: Optional hard no-trade in choppy markets

### Broker-Accurate Risk Engine
- **MT5 Contract Math**: Uses `order_calc_profit()` for precise loss-at-SL calculations
- **Multi-Asset Support**: Works correctly for forex, indices, metals, crypto
- **Volume Normalization**: Respects broker min/max/step constraints
- **Hard Safety Cap**: Configurable maximum risk per trade (default 5%)

### Generalization-Focused Validation
- **Gap Penalty**: Penalizes train→validation performance degradation
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
├── pm_core.py           # Configuration, data loading, backtesting, scoring (2,142 lines)
├── pm_strategies.py     # 28 trading strategies with param grids (2,279 lines)
├── pm_pipeline.py       # Optimization pipeline, regime optimizer (1,975 lines)
├── pm_main.py           # Application entry, live trading loop (1,535 lines)
├── pm_mt5.py            # MetaTrader 5 integration (1,117 lines)
├── pm_position.py       # Position management and sizing (795 lines)
├── pm_regime.py         # Market regime detection (964 lines)
├── pm_regime_tuner.py   # Regime parameter optimization (490 lines)
├── config.json          # Runtime configuration
├── pm_configs.json      # Saved strategy configurations (auto-generated)
├── regime_params.json   # Tuned regime parameters (optional)
├── last_trade_log.json  # Decision throttle state (auto-generated)
├── data/                # Historical data cache
└── pm_outputs/          # Logs and reports
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        OPTIMIZATION PHASE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  MT5 Data ──► DataLoader ──► FeatureComputer ──► RegimeDetector     │
│                                     │                                │
│                                     ▼                                │
│              ┌─────────────────────────────────────┐                │
│              │      RegimeOptimizer                 │                │
│              │  ┌─────────────────────────────┐    │                │
│              │  │ Phase 1: Screen all strategies │    │                │
│              │  │ Phase 2: Tune top-K per regime │    │                │
│              │  │ Phase 3: Validate winners      │    │                │
│              │  └─────────────────────────────┘    │                │
│              └─────────────────────────────────────┘                │
│                                     │                                │
│                                     ▼                                │
│                            pm_configs.json                           │
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
pip install pandas numpy MetaTrader5
```

Or with a virtual environment (recommended):

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Install dependencies
pip install pandas numpy MetaTrader5
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
├── config.json
└── data/              ← Create this folder
```

### Step 3: Configure MetaTrader 5

1. Open MetaTrader 5 and log in to your account
2. Enable **AutoTrading** (Ctrl+E or click the AutoTrading button)
3. Ensure the terminal stays open while the script runs

---

## Quick Start

```bash
# 1. Run optimization (required first time, takes 10-30 minutes)
python pm_main.py --optimize

# 2. Paper trade to verify (run for a few days)
python pm_main.py --trade --paper

# 3. Go live when confident
python pm_main.py --trade

# 4. Full autonomous mode (auto-retrains when configs expire)
python pm_main.py --trade --auto-retrain
```

---

## Configuration

### config.json Structure

```json
{
  "pipeline": {
    // Data settings
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    "max_bars": 500000,           // ~5 years of M5 data
    
    // Train/validation split
    "train_pct": 80.0,
    "val_pct": 30.0,
    "overlap_pct": 10.0,
    
    // Backtest settings
    "initial_capital": 10000.0,
    "risk_per_trade_pct": 1.0,
    "use_spread": true,
    "use_commission": true,
    "use_slippage": true,
    "slippage_pips": 0.5,
    
    // Scoring mode: "pm_weighted" or "fx_backtester"
    "scoring_mode": "fx_backtester",
    
    // Generalization controls (fx_backtester mode)
    "fx_gap_penalty_lambda": 0.70,     // Penalty for train→val gap
    "fx_min_robustness_ratio": 0.80,   // Min val/train score ratio
    "fx_val_min_trades": 15,           // Min validation trades
    "fx_val_max_drawdown": 20.0,       // Max validation drawdown %
    
    // Regime optimization
    "use_regime_optimization": true,
    "regime_min_train_trades": 25,
    "regime_min_val_trades": 15,
    "regime_enable_hyperparam_tuning": true,
    "regime_hyperparam_top_k": 3,      // Top K strategies to tune
    "regime_hyperparam_max_combos": 500,
    
    // Timeframes and retrain periods
    "timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
    "retrain_periods": [7, 14, 30, 60, 90, 120, 180]
  },
  
  "position": {
    "risk_per_trade_pct": 1.0,
    "risk_basis": "balance",           // "balance" or "equity"
    "max_risk_pct": 5.0,               // Hard safety cap
    "auto_widen_sl": true,             // Widen SL for broker minimums
    "min_position_size": 0.01,
    "max_position_size": 0.0           // 0 = use broker max
  },
  
  "mt5": {
    "login": 0,                        // 0 = use existing session
    "password": "",
    "server": "",
    "path": ""
  },
  
  "symbols": [
    "EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "US30"
    // ... add your symbols
  ]
}
```

### Key Configuration Options Explained

| Option | Description | Recommended |
|--------|-------------|-------------|
| `scoring_mode` | `fx_backtester` penalizes overfitting | `fx_backtester` |
| `fx_gap_penalty_lambda` | Higher = more penalty for train/val gap | 0.5-0.8 |
| `fx_min_robustness_ratio` | Minimum val_score/train_score | 0.75-0.85 |
| `risk_per_trade_pct` | Target risk per trade | 0.5-2.0% |
| `max_risk_pct` | Hard cap (skips trade if exceeded) | 5.0% |
| `regime_hyperparam_max_combos` | Param combinations to test | 100-500 |

---

## Usage

### Command Reference

| Command | Description |
|---------|-------------|
| `--optimize` | Run full optimization pipeline |
| `--trade` | Start live trading |
| `--trade --paper` | Paper trading (no real orders) |
| `--trade --auto-retrain` | Live trading with auto-retraining |
| `--status` | Show current configuration status |

### Optional Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config FILE` | Configuration file path | `config.json` |
| `--symbols SYM1 SYM2` | Trade specific symbols only | All configured |
| `--data-dir PATH` | Data directory | `./data` |
| `--output-dir PATH` | Output directory | `./pm_outputs` |
| `--log-level LEVEL` | DEBUG, INFO, WARNING, ERROR | `INFO` |

### Examples

```bash
# Optimize specific symbols only
python pm_main.py --optimize --symbols EURUSD GBPUSD XAUUSD

# Paper trade with debug logging
python pm_main.py --trade --paper --log-level DEBUG

# Use custom config file
python pm_main.py --trade --config my_settings.json

# Check status of existing configurations
python pm_main.py --status
```

---

## Trading Strategies

### 28 Strategies Across 3 Categories

#### Trend Following (10 strategies)

| Strategy | Description |
|----------|-------------|
| EMACrossoverStrategy | Fast/slow EMA crossover |
| SupertrendStrategy | ATR-based trend bands |
| MACDTrendStrategy | MACD line/signal crossover |
| ADXTrendStrategy | ADX strength + DI direction |
| IchimokuStrategy | Cloud-based trend following |
| HullMATrendStrategy | Hull Moving Average direction |
| EMARibbonADXStrategy | EMA ribbon with ADX filter |
| AroonTrendStrategy | Aroon oscillator signals |
| ADXDIStrengthStrategy | ADX + DI strength confluence |
| KeltnerPullbackStrategy | Keltner channel pullbacks |

#### Mean Reversion (10 strategies)

| Strategy | Description |
|----------|-------------|
| RSIExtremesStrategy | RSI overbought/oversold |
| BollingerBounceStrategy | Bollinger Band mean reversion |
| ZScoreMRStrategy | Statistical Z-score extremes |
| StochasticReversalStrategy | Stochastic %K/%D crossover |
| CCIReversalStrategy | CCI extreme reversals |
| WilliamsRStrategy | Williams %R extremes |
| RSITrendFilteredMRStrategy | RSI MR with trend filter |
| StochRSITrendGateStrategy | Stochastic RSI with trend gate |
| VWAPDeviationReversionStrategy | VWAP deviation mean reversion |
| FisherTransformMRStrategy | Fisher transform reversals |
| ZScoreVWAPReversionStrategy | Z-score of VWAP deviation |

#### Breakout/Momentum (8 strategies)

| Strategy | Description |
|----------|-------------|
| DonchianBreakoutStrategy | Donchian channel breakouts |
| VolatilityBreakoutStrategy | ATR-based volatility breakouts |
| MomentumBurstStrategy | ROC momentum bursts |
| SqueezeBreakoutStrategy | Bollinger/Keltner squeeze breakouts |
| KeltnerBreakoutStrategy | Keltner channel breakouts |
| PivotBreakoutStrategy | Pivot point breakouts |
| MACDHistogramMomentumStrategy | MACD histogram momentum |

### Standardized Stop Loss / Take Profit

All strategies use standardized ATR-based SL/TP grids:

```python
SL_ATR_MULTIPLIER = [1.5, 2.0, 2.5, 3.0]
TP_ATR_MULTIPLIER = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]
```

---

## Regime Detection

### Four Market Regimes

| Regime | Characteristics | Typical Strategies |
|--------|-----------------|-------------------|
| **TREND** | Strong directional movement, high ADX | Trend following |
| **RANGE** | Bounded price action, low ADX | Mean reversion |
| **BREAKOUT** | Volatility expansion, structure breaks | Breakout/momentum |
| **CHOP** | No clear direction, high noise | No trade (optional) |

### Regime Scoring Components

Each regime is scored 0-1 based on multiple factors:

**TREND Score:**
- ADX strength (40%)
- Directional efficiency (35%)
- Price slope (25%)

**RANGE Score:**
- Low ADX (35%)
- Low directional efficiency (30%)
- Price containment (35%)

**BREAKOUT Score:**
- Bollinger squeeze (35%)
- ATR expansion (30%)
- Structure break (35%)

**CHOP Score:**
- Low ADX (30%)
- Low efficiency (30%)
- High whipsaw rate (40%)

### Hysteresis State Machine

Prevents rapid regime flipping:

```
Parameters:
- k_confirm: Bars to confirm switch (default: 3)
- gap_min: Minimum score gap to switch (default: 0.10)
- k_hold: Minimum bars to hold regime (default: 5)

Switch occurs only when:
1. New regime leads for k_confirm consecutive bars
2. Score gap >= gap_min
3. Current regime held for >= k_hold bars
```

### REGIME_LIVE Parity

Ensures backtest and live trading use identical information:

```python
# Backtest: uses REGIME_LIVE (shifted by 1)
trade_regime = features['REGIME_LIVE'].iloc[entry_bar]

# Live: uses REGIME from last closed bar (index -2)
current_regime = features['REGIME'].iloc[-2]

# These are equivalent: REGIME_LIVE[i] = REGIME[i-1]
```

---

## Risk Management

### Position Sizing Flow

```
1. Target Risk Calculation
   target_risk = balance × (risk_per_trade_pct / 100)

2. Loss-per-Lot Calculation (MT5 contract math)
   loss_per_lot = mt5.order_calc_profit(SELL/BUY, symbol, 1.0, entry, sl)
   
   Fallback chain:
   a) MT5 order_calc_profit (preferred)
   b) Tick-based: (entry - sl) / tick_size × tick_value
   c) Pip-based: sl_pips × pip_value (last resort, with warning)

3. Raw Volume
   volume_raw = target_risk / loss_per_lot

4. Normalization
   volume = floor(volume_raw / volume_step) × volume_step
   volume = clamp(volume, volume_min, volume_max)

5. Hard Cap Check
   actual_risk = loss_per_lot × volume
   actual_risk_pct = actual_risk / balance × 100
   
   if actual_risk_pct > max_risk_pct:
       SKIP TRADE
```

### Safety Features

| Feature | Description |
|---------|-------------|
| **Hard Cap** | Skips trade if risk > max_risk_pct (default 5%) |
| **Auto-Widen SL** | Widens SL to meet broker minimum stop distance |
| **Volume Floor** | Uses floor() not round() to avoid exceeding target |
| **Position Check** | Verifies no existing position before entry |
| **Rate Limiting** | Prevents rapid order submission |

### Risk Audit Logging

Every trade logs complete risk details:

```
[EURUSD] BUY | basis=10000.00 (balance) | target_risk=1.00% ($100.00) | 
actual_risk=0.98% ($98.00) | vol_raw=0.1523 | vol=0.15 | 
entry=1.08520 | sl=1.08020 | tp=1.09520
```

---

## Output Files

### After Optimization

```
pm_configs.json              # Strategy configurations (IMPORTANT!)
pm_outputs/
├── optimization_summary.csv # Summary of all results
└── logs/
    └── pm_YYYYMMDD.log      # Daily log files
```

### pm_configs.json Structure

```json
{
  "EURUSD": {
    "symbol": "EURUSD",
    "regime_configs": {
      "H1": {
        "TREND": {
          "strategy_name": "SupertrendStrategy",
          "parameters": {"atr_period": 10, "multiplier": 3.0, ...},
          "quality_score": 0.75,
          "regime_train_trades": 145,
          "regime_val_trades": 52
        },
        "RANGE": {...},
        "BREAKOUT": {...}
      },
      "H4": {...}
    },
    "default_config": {...},
    "is_validated": true,
    "validation_reason": "12 validated winners (3 rejected) across 4 timeframes"
  }
}
```

### After Trading

```
pm_outputs/
├── trades_YYYYMMDD_HHMMSS.json  # Trade log (on stop)
└── last_trade_log.json          # Decision throttle state
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

### Scoring Modes

**pm_weighted** (Original):
```
score = Σ(weight_i × normalized_metric_i)
```

**fx_backtester** (Recommended):
```
train_score = f(sharpe, return, drawdown)
val_score = f(sharpe, return, drawdown)
gap = max(0, train_score - val_score)
final_score = val_score - λ × gap
final_score *= robustness_boost(val_score / train_score)
```

---

## Troubleshooting

### Common Issues

#### "Failed to connect to MT5"
1. Ensure MetaTrader 5 is running
2. Ensure you're logged into your account
3. Enable AutoTrading (Ctrl+E)
4. Try restarting MT5 and the script

#### "Symbol not found: EURUSD"
Your broker uses different symbol names. The system tries variants automatically, but if it fails:
1. Open MT5 Market Watch
2. Find the exact symbol name (e.g., `EURUSD.a`, `EURUSDm`)
3. Use that name in your config

#### "No valid strategy found"
- Lower `min_trades` (try 15-20)
- Lower `fx_min_robustness_ratio` (try 0.70)
- Ensure sufficient historical data exists

#### "SKIP: min lot would exceed max_risk_pct"
Broker minimum lot size exceeds your risk budget. Options:
- Increase `risk_per_trade_pct`
- Increase `max_risk_pct` (with caution)
- Remove that symbol

#### High validation rejection rate
If many strategies fail validation:
- Reduce `fx_min_robustness_ratio` (try 0.70-0.75)
- Reduce `regime_min_val_trades` (try 10)
- Check if data quality issues exist

### Debug Mode

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

This shows:
- Feature computation details
- Regime detection scores
- Cache hit/miss statistics
- Full risk calculation audit

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
            'threshold': 0.5,
            'sl_atr_mult': 2.0,
            'tp_atr_mult': 3.0
        }
    
    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        signals = pd.Series(0, index=features.index)
        # Your signal logic here
        # signals[condition_long] = 1
        # signals[condition_short] = -1
        return signals
    
    def get_param_grid(self) -> Dict[str, List]:
        return {
            'period': [10, 15, 20, 25, 30],
            'threshold': [0.3, 0.5, 0.7],
            'sl_atr_mult': _GLOBAL_SL_GRID,
            'tp_atr_mult': _GLOBAL_TP_GRID
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
python -c "from pm_main import FXPortfolioManager; print('OK')"

# Dry run optimization (single symbol)
python pm_main.py --optimize --symbols EURUSD --log-level DEBUG
```

---

## License

MIT License - Use at your own risk.

---

## Disclaimer

⚠️ **IMPORTANT RISK WARNING**

- Trading forex, CFDs, and derivatives involves substantial risk of loss
- Past performance is not indicative of future results
- This software is for educational and research purposes
- Always test thoroughly in paper mode before live trading
- Never risk more than you can afford to lose
- The authors assume no liability for trading losses

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 3.0 | Jan 2026 | Regime-aware optimization, hyperparameter tuning, validation enforcement |
| 2.0 | 2025 | fx_backtester scoring mode, generalization controls |
| 1.0 | 2024 | Initial release |

~Bongo