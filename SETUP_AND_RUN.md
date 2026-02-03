# FX Portfolio Manager v3.3 - Complete Setup and Run Guide

This guide walks you through every step from installation to live trading.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Configuration](#3-configuration)
4. [Running Optimization](#4-running-optimization)
5. [Paper Trading](#5-paper-trading)
6. [Live Trading](#6-live-trading)
7. [Monitoring and Maintenance](#7-monitoring-and-maintenance)
8. [Advanced Configuration](#8-advanced-configuration)
9. [Troubleshooting](#9-troubleshooting)
10. [Quick Reference](#10-quick-reference)

---

## 1. Prerequisites

### 1.1 System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| OS | Windows 10 | Windows 10/11 |
| Python | 3.8 | 3.10+ |
| RAM | 4 GB | 8+ GB |
| Disk | 2 GB free | 10+ GB free |
| Network | Stable internet | Low-latency connection |

> **Note:** MetaTrader 5 Python API only works on Windows.

### 1.2 Software Requirements

1. **Python 3.8+**
   - Download from [python.org](https://www.python.org/downloads/)
   - During installation, check **"Add Python to PATH"**
   - Verify: `python --version`

2. **MetaTrader 5**
   - Download from your broker or [metatrader5.com](https://www.metatrader5.com/)
   - Install and log in to your trading account
   - Keep MT5 running during script execution

### 1.3 Trading Account Requirements

- Demo or live account with your broker
- Sufficient margin for your intended symbols
- AutoTrading enabled in MT5

---

## 2. Installation

### 2.1 Create Project Folder

```bash
mkdir FX_Portfolio_Manager
cd FX_Portfolio_Manager
```

### 2.2 Set Up Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows Command Prompt)
.venv\Scripts\activate.bat

# Activate (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate (Git Bash)
source .venv/Scripts/activate
```

### 2.3 Install Dependencies

```bash
# Required packages
pip install pandas numpy MetaTrader5

# Optional (recommended for better performance)
pip install numba    # 3-10x faster backtesting
pip install optuna   # Bayesian hyperparameter optimization
```

Verify installation:
```bash
python -c "import pandas, numpy, MetaTrader5; print('All packages installed successfully')"
```

Check Numba availability:
```bash
python -c "import numba; print(f'Numba {numba.__version__} installed')"
```

### 2.4 Download and Place Files

Place all files in your project folder:

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
├── data/                  ← Create this folder
└── .venv/                 ← Created by venv
```

Create the data folder:
```bash
mkdir data
```

### 2.5 Verify Installation

```bash
python -c "from pm_main import FXPortfolioManagerApp; print('Installation verified!')"
```

Check all components:
```bash
python -c "
from pm_core import NUMBA_AVAILABLE
from pm_pipeline import ConfigLedger
from pm_optuna import OPTUNA_AVAILABLE
print(f'Numba JIT: {\"enabled\" if NUMBA_AVAILABLE else \"disabled (install numba for 3-10x speedup)\"}')
print(f'Optuna TPE: {\"enabled\" if OPTUNA_AVAILABLE else \"disabled (install optuna for Bayesian optimization)\"}')
print(f'ConfigLedger: available')
print('All components loaded successfully!')
"
```

---

## 3. Configuration

### 3.1 Edit config.json

Open `config.json` in a text editor and configure:

#### Minimal Configuration (Quick Start)

```json
{
  "pipeline": {
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    "initial_capital": 10000.0,
    "risk_per_trade_pct": 1.0,
    "scoring_mode": "fx_backtester"
  },
  "position": {
    "risk_per_trade_pct": 1.0,
    "risk_basis": "balance",
    "max_risk_pct": 5.0
  },
  "mt5": {
    "login": 0,
    "password": "",
    "server": "",
    "path": ""
  },
  "symbols": [
    "EURUSD", "GBPUSD", "USDJPY", "XAUUSD"
  ]
}
```

> **Note:** `mt5.login = 0` means use the currently logged-in MT5 session.
>
> Optional instrument specs (recommended for accuracy):
> ```json
> {
>   "broker_specs_path": "broker_specs.json",
>   "instrument_spec_defaults": { "commission_per_lot": 7.0 },
>   "instrument_specs": { "AUDCAD": { "inherit": "USDCAD" } }
> }
> ```

#### Full Configuration (Production)

```json
{
  "pipeline": {
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    
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
    "min_trades": 25,
    "min_robustness": 0.20,
    
    "min_win_rate": 45.0,
    "min_profit_factor": 1.2,
    "min_sharpe": 0.5,
    "max_drawdown": 15.0,
    
    "scoring_mode": "fx_backtester",
    
    "fx_opt_min_trades": 15,
    "fx_val_min_trades": 15,
    "fx_val_max_drawdown": 20.0,
    "fx_val_sharpe_override": 0.3,
    "fx_selection_top_k": 5,
    "fx_opt_top_k": 5,
    "fx_gap_penalty_lambda": 0.70,
    "fx_robustness_boost": 0.15,
    "fx_min_robustness_ratio": 0.80,
    
    "timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
    "retrain_periods": [14, 30, 60, 90, 120],
    
    "max_bars": 500000,
    
    "optimization_valid_days": 14,
    
    "use_regime_optimization": true,
    "regime_min_train_trades": 25,
    "regime_min_val_trades": 15,
    "regime_freshness_decay": 0.85,
    "regime_chop_no_trade": true,
    "regime_params_file": "regime_params.json",
    
    "regime_enable_hyperparam_tuning": true,
    "regime_hyperparam_top_k": 3,
    "regime_hyperparam_max_combos": 150,
    
    "score_weights": {
      "sharpe": 0.25,
      "profit_factor": 0.20,
      "win_rate": 0.15,
      "total_return": 0.15,
      "max_drawdown": 0.15,
      "trade_count": 0.10
    }
  },

  "position": {
    "risk_per_trade_pct": 1.0,
    "max_position_size": 0.0,
    "min_position_size": 0.01,
    
    "risk_basis": "balance",
    "max_risk_pct": 5.0,
    "risk_tolerance_pct": 2.0,
    "auto_widen_sl": true,
    
    "use_trailing_stop": false,
    "trailing_stop_pips": 0.0,
    "trailing_activation_pips": 0.0,
    
    "use_breakeven_stop": false,
    "breakeven_trigger_pips": 0.0,
    "breakeven_offset_pips": 1.0,
    
    "allow_scaling": false,
    "max_scale_ins": 3,
    "scale_in_pct": 50.0,
    
    "max_trade_duration_bars": 0,
    
    "use_spread": true,
    "use_slippage": true,
    "slippage_pips": 0.5
  },

  "mt5": {
    "login": 0,
    "password": "",
    "server": "",
    "path": "",
    "timeout": 60000,
    "portable": false
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
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "XAUUSD", "US30"
  ]
}
```

Note: `val_pct` is informational only. Actual validation size is controlled by
`train_pct` and `overlap_pct`.
Note: `optimization_max_workers` (default 1) enables parallel optimization when set > 1.

### 3.2 MT5 Connection Options

#### Option A: Use Existing Session (Recommended)
```json
"mt5": {
  "login": 0,
  "password": "",
  "server": "",
  "path": ""
}
```
Just keep MT5 open and logged in.

#### Option B: Auto-Login
```json
"mt5": {
  "login": 12345678,
  "password": "your_password",
  "server": "Your-Broker-Server",
  "path": "C:/Program Files/MetaTrader 5/terminal64.exe"
}
```

### 3.3 Symbol Configuration

Find your broker's exact symbol names:
1. Open MT5
2. Press Ctrl+U (Symbols)
3. Note exact names (may be `EURUSD`, `EURUSD.a`, `EURUSDm`, etc.)

```json
"symbols": [
  "EURUSD",      // Standard forex
  "XAUUSD",      // Gold
  "US30",        // Dow Jones index
  "BTCUSD"       // Bitcoin (if available)
]
```

---

## 4. Running Optimization

### 4.1 Prepare MetaTrader 5

1. Open MetaTrader 5
2. Log in to your account
3. Enable AutoTrading (Ctrl+E or click button)
4. Ensure Market Watch shows your symbols

### 4.2 Run Optimization

```bash
# First time or after deleting pm_configs.json
python pm_main.py --optimize
```

### 4.3 Stateful Optimization (NEW in v3.3)

The optimization is now **stateful** - it remembers progress and skips valid configs:

```bash
# Default: Skip symbols with valid configs
python pm_main.py --optimize
# Output:
# SKIP EURUSD: valid until 2026-02-14 (13 days remaining)
# SKIP GBPUSD: valid until 2026-02-12 (11 days remaining)
# OPTIMIZE USDJPY: expired 3 days ago
# OPTIMIZE AUDUSD: missing

# Force re-optimization of everything
python pm_main.py --optimize --overwrite
# Output:
# OVERWRITE MODE: ignoring validity checks
# OPTIMIZE EURUSD: overwrite enabled
# OPTIMIZE GBPUSD: overwrite enabled
# ...
```

### 4.4 What Happens During Optimization

```
============================================================
FX PORTFOLIO MANAGER v3.3
============================================================
Connecting to MetaTrader 5...
✓ Connected to MT5
✓ Portfolio Manager initialized
  Symbols: 12
  Strategies: 28
  Existing configs: 4

============================================================
RUNNING OPTIMIZATION
MODE: INCREMENTAL (skipping valid configs)
============================================================
SKIP EURUSD: valid until 2026-02-14 (13 days remaining)
SKIP GBPUSD: valid until 2026-02-12 (11 days remaining)
OPTIMIZE USDJPY: expired 3 days ago

Progress: 1/10
============================================================
OPTIMIZING: USDJPY (Regime-Aware)
============================================================
[USDJPY] Regime Optimization: 28 strategies x 6 timeframes x 4 regimes
[USDJPY] [H1] Hyperparameter tuning 3 strategies
[USDJPY] [H1] [TREND] Winner: SupertrendStrategy [TUNED] (quality=0.723)
[USDJPY] [H1] [RANGE] Winner: BollingerBounceStrategy (quality=0.681)
SAVED USDJPY to pm_configs.json (atomic)
...

============================================================
OPTIMIZATION COMPLETE
============================================================
Total time: 342.5s
Optimized: 8/10
Skipped: 2 (already valid)
Total validated: 10/12
```

### 4.5 Interruption Recovery

If optimization is interrupted (Ctrl+C, crash, power loss):

```bash
# Simply re-run - it will resume where it left off
python pm_main.py --optimize
# Output:
# Loaded 8 existing configs from pm_configs.json
# SKIP EURUSD: valid until 2026-02-14
# SKIP USDJPY: valid until 2026-02-15 (just optimized)
# OPTIMIZE NZDUSD: missing (next in queue)
```

---

## 5. Paper Trading

### 5.1 Start Paper Trading

```bash
python pm_main.py --trade --paper
```

### 5.2 What Paper Trading Does

- Receives real market data
- Generates real signals
- Calculates real position sizes
- **Does NOT execute real orders**
- Logs what WOULD have happened

### 5.3 Monitor Paper Trading

Watch the console output:
```
[EURUSD] Signal: LONG | Regime: TREND | Strategy: SupertrendStrategy
[EURUSD] PAPER: Would BUY 0.15 lots @ 1.08520, SL: 1.08020, TP: 1.09520
[EURUSD] Risk: $100.00 (1.00% of $10000.00)
```

### 5.4 Paper Trading Duration

Recommended paper trading period:
- **Minimum**: 1 week
- **Recommended**: 2-4 weeks
- **Conservative**: 1-3 months

Verify:
- Signals match expected regime behavior
- Position sizes are appropriate
- No unexpected errors
- System runs stably

---

## 6. Live Trading

### 6.1 Pre-Live Checklist

Before going live, verify:

- [ ] Paper trading ran for sufficient period
- [ ] No unexpected errors in logs
- [ ] Position sizes look reasonable
- [ ] MT5 account has sufficient margin
- [ ] Risk settings are conservative enough
- [ ] You understand the risks involved

### 6.2 Start Live Trading

```bash
# Basic live trading
python pm_main.py --trade

# With automatic retraining (recommended for long-term)
python pm_main.py --trade --auto-retrain
```

### 6.3 Live Trading Output

```
============================================================
STARTING LIVE TRADING
============================================================
Trading enabled: True
Auto-retrain: True

[EURUSD] New bar detected: 2026-02-01 14:00:00
[EURUSD] Regime: TREND (score: 0.72)
[EURUSD] Strategy: SupertrendStrategy
[EURUSD] Signal: LONG

[EURUSD] EXECUTING ORDER
  Direction: BUY
  Volume: 0.15
  Entry: 1.08520
  Stop Loss: 1.08020
  Take Profit: 1.09520
  Risk: $100.00 (1.00%)

[EURUSD] ORDER FILLED
  Ticket: 12345678
  Fill Price: 1.08521
  Commission: $3.50
```

### 6.4 Stopping Live Trading

Press **Ctrl+C** to gracefully stop:

```
^C
Shutdown signal received
Saving state...
Closing connections...
Trade log saved to pm_outputs/trades_20260201_143052.json
Shutdown complete
```

### 6.5 Running as a Background Service (Advanced)

For 24/7 operation, consider:

**Windows Task Scheduler:**
1. Create a batch file `run_trading.bat`:
   ```batch
   @echo off
   cd /d C:\FX_Portfolio_Manager
   call .venv\Scripts\activate.bat
   python pm_main.py --trade --auto-retrain
   ```
2. Schedule via Task Scheduler to restart on failure

**Or use a process manager like PM2 or NSSM**

---

## 7. Monitoring and Maintenance

### 7.1 Daily Checks

1. **Verify script is running**
   - Check console or logs for recent activity

2. **Review trade log**
   - Check `pm_outputs/logs/pm_YYYYMMDD.log`

3. **Check MT5 positions**
   - Verify positions match expected signals

### 7.2 Weekly Maintenance

1. **Check config validity**
   ```bash
   python pm_main.py --status
   ```
   Output shows expiry dates:
   ```
   OK EURUSD   | SupertrendStrategy       | H1  | Score: 75.5 | Expires: 2026-02-14 (13d)
   EX USDJPY   | BollingerBounceStrategy  | H4  | Score: 68.2 | EXPIRED 2d ago
   ```

2. **Re-optimize expired configs**
   ```bash
   python pm_main.py --optimize
   ```

3. **Review logs for errors**
   ```bash
   grep -i "error\|warning\|failed" pm_outputs/logs/pm_*.log
   ```

### 7.3 Monthly Maintenance

1. **Full re-optimization** (optional)
   ```bash
   python pm_main.py --optimize --overwrite
   ```

2. **Tune regime parameters** (optional)
   ```bash
   python pm_regime_tuner.py --data-dir ./data --output regime_params.json
   ```

3. **Review overall performance**
   - Compare actual vs backtested results
   - Adjust risk settings if needed

### 7.4 Cache Statistics

Get cache performance:
```python
# In Python shell
from pm_main import FXPortfolioManagerApp
app = FXPortfolioManagerApp()
app.initialize()
# After running for a while:
print(app.trader.get_cache_stats())
# {'cache_hits': 1250, 'cache_misses': 45, 'hit_rate_pct': 96.5, 'cache_size': 24}
```

---

## 8. Advanced Configuration

### 8.1 Tuning Regime Parameters

For better regime detection per symbol/timeframe:

```bash
python pm_regime_tuner.py --data-dir ./data --output regime_params.json
```

This creates optimized parameters for:
- `k_confirm`: Bars to confirm regime switch
- `gap_min`: Minimum score gap to switch
- `k_hold`: Minimum bars to hold regime

### 8.2 Scoring Mode Comparison

| Mode | Best For | Characteristics |
|------|----------|-----------------|
| `pm_weighted` | Stable markets | Strict criteria, less adaptive |
| `fx_backtester` | Most cases | Penalizes overfitting, more robust |

### 8.3 Adjusting Generalization Controls

If too many strategies fail validation:

```json
"fx_gap_penalty_lambda": 0.50,      // Lower = less penalty (0.3-0.7)
"fx_min_robustness_ratio": 0.70,    // Lower = more permissive (0.6-0.85)
"fx_val_min_trades": 10             // Lower = accept less data (5-15)
```

If strategies are overfitting:

```json
"fx_gap_penalty_lambda": 0.80,      // Higher = more penalty
"fx_min_robustness_ratio": 0.90,    // Higher = stricter
"regime_min_val_trades": 20         // Higher = more validation data
```

### 8.4 Timeframe Selection

For faster optimization, reduce timeframes:

```json
"timeframes": ["H1", "H4", "D1"]     // Skip lower timeframes
```

For more granular signals:

```json
"timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"]
```

### 8.5 Config Validity Duration

Control how long configs remain valid before expiring:

```json
"optimization_valid_days": 14    // Default: 14 days
```

Shorter periods = more frequent re-optimization (more adaptive)
Longer periods = less compute, more stable

### 8.6 Multiple Configurations

Create different configs for different purposes:

```bash
# Conservative config
python pm_main.py --trade --config config_conservative.json

# Aggressive config
python pm_main.py --trade --config config_aggressive.json
```

---

## 9. Troubleshooting

### 9.1 Connection Issues

#### "Failed to connect to MT5"

1. **Check MT5 is running**
   - Open MetaTrader 5
   - Ensure you're logged in (green icon in bottom right)

2. **Enable AutoTrading**
   - Press Ctrl+E or click AutoTrading button
   - Icon should show green

3. **Check Python MT5 package**
   ```bash
   pip install --upgrade MetaTrader5
   ```

4. **Try explicit connection**
   ```python
   import MetaTrader5 as mt5
   print(mt5.initialize())
   print(mt5.last_error())
   ```

#### "Terminal not found"

Specify the path explicitly:
```json
"mt5": {
  "path": "C:/Program Files/MetaTrader 5/terminal64.exe"
}
```

### 9.2 Symbol Issues

#### "Symbol not found: EURUSD"

1. **Check exact name in MT5**
   - Open Market Watch (Ctrl+M)
   - Right-click → Symbols (Ctrl+U)
   - Find exact name (e.g., `EURUSD`, `EURUSD.a`, `EURUSDm`)

2. **Update config.json**
   ```json
   "symbols": ["EURUSD.a", "GBPUSD.a"]
   ```

3. **Enable symbol in Market Watch**
   - Right-click Market Watch → Symbols
   - Find symbol → Show

### 9.3 Risk/Position Issues

#### "SKIP: min lot would exceed max_risk_pct"

The broker's minimum lot size exceeds your risk budget.

**Solutions:**
1. Increase risk settings:
   ```json
   "risk_per_trade_pct": 2.0,
   "max_risk_pct": 10.0
   ```

2. Remove problematic symbol
3. Use a broker with smaller minimums

### 9.4 Optimization Issues

#### "No valid strategy found"

All strategies failed minimum criteria.

**Solutions:**
1. Lower thresholds:
   ```json
   "min_trades": 15,
   "min_profit_factor": 1.1,
   "fx_min_robustness_ratio": 0.65
   ```

2. Check data quality
3. Try different timeframes

#### Optimization is very slow

1. Install Numba for 3-10x speedup:
   ```bash
   pip install numba
   ```

2. Reduce symbols, param combinations, or timeframes

### 9.5 Config File Issues

#### "Corrupted JSON in pm_configs.json"

Config file was corrupted (very rare due to atomic writes).

**Fix:**
```bash
# Backup corrupted file
mv pm_configs.json pm_configs.json.corrupt

# Start fresh
python pm_main.py --optimize
```

### 9.6 Debug Mode

For detailed troubleshooting:

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

This shows:
- Every feature computation
- All regime scores
- Cache hit/miss details
- Full risk calculations
- Order request/response details

---

## 10. Quick Reference

### Command Cheat Sheet

```bash
# First-time setup
pip install pandas numpy MetaTrader5 numba optuna
python pm_main.py --optimize

# Daily usage
python pm_main.py --trade --paper          # Test mode
python pm_main.py --trade                   # Live mode
python pm_main.py --trade --auto-retrain   # Autonomous mode

# Maintenance
python pm_main.py --status                  # Check configs
python pm_main.py --optimize                # Re-optimize expired only
python pm_main.py --optimize --overwrite    # Force re-optimize all
python pm_main.py --optimize --symbols EURUSD  # Re-optimize one symbol

# Debugging
python pm_main.py --trade --paper --log-level DEBUG
```

### File Locations

| File | Purpose |
|------|---------|
| `config.json` | Your settings |
| `pm_configs.json` | Strategy configurations (auto-generated) |
| `regime_params.json` | Tuned regime params (optional) |
| `last_trade_log.json` | Decision throttle state |
| `data/*.csv` | Historical data cache |
| `pm_outputs/logs/*.log` | Daily log files |
| `pm_outputs/trades_*.json` | Trade records |

### Key Settings Quick Reference

| Setting | Conservative | Normal | Aggressive |
|---------|--------------|--------|------------|
| `risk_per_trade_pct` | 0.5% | 1.0% | 2.0% |
| `max_risk_pct` | 2.5% | 5.0% | 10.0% |
| `fx_min_robustness_ratio` | 0.85 | 0.80 | 0.70 |
| `fx_gap_penalty_lambda` | 0.80 | 0.70 | 0.50 |
| `regime_min_val_trades` | 20 | 15 | 10 |
| `optimization_valid_days` | 7 | 14 | 30 |

### CLI Arguments Reference

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
| `--log-level` | DEBUG/INFO/WARNING/ERROR |

### Emergency Procedures

**Stop all trading immediately:**
```
Ctrl+C in terminal
```

**Close all positions manually:**
1. Open MetaTrader 5
2. Go to Trade tab
3. Right-click each position → Close

**Reset decision throttle:**
```bash
del last_trade_log.json
```

**Reset all configurations:**
```bash
del pm_configs.json
python pm_main.py --optimize
```

**Check system health:**
```bash
python pm_main.py --status
```

---

## Support

For issues:
1. Check logs in `pm_outputs/logs/`
2. Run with `--log-level DEBUG`
3. Verify MT5 connection and symbol names
4. Review this troubleshooting guide

---

## Disclaimer

⚠️ **IMPORTANT RISK WARNING**

- Trading forex and CFDs involves substantial risk of loss
- Past performance is not indicative of future results
- This software is for educational purposes
- Always test thoroughly in paper mode first
- Start with conservative risk settings
- Never risk more than you can afford to lose
- The authors assume no liability for trading losses

---

*FX Portfolio Manager v3.3 - Complete Setup and Run Guide*
