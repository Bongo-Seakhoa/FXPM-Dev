# FX Portfolio Manager - Complete Setup and Run Guide

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
pip install pandas numpy MetaTrader5
```

Verify installation:
```bash
python -c "import pandas, numpy, MetaTrader5; print('All packages installed successfully')"
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
python -c "from pm_main import FXPortfolioManager; print('Installation verified!')"
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
    
    "max_param_combos": 500,
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
    "retrain_periods": [7, 14, 30, 60, 90, 120, 180],
    
    "max_bars": 500000,
    
    "use_regime_optimization": true,
    "regime_min_train_trades": 25,
    "regime_min_val_trades": 15,
    "regime_freshness_decay": 0.85,
    "regime_chop_no_trade": true,
    "regime_params_file": "regime_params.json",
    
    "regime_enable_hyperparam_tuning": true,
    "regime_hyperparam_top_k": 3,
    "regime_hyperparam_max_combos": 500,
    
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

  "symbols": [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "XAUUSD", "US30"
  ]
}
```

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
python pm_main.py --optimize
```

### 4.3 What Happens During Optimization

```
============================================================
FX PORTFOLIO MANAGER v3.0
============================================================
Connecting to MetaTrader 5...
✓ Connected to MT5
✓ Portfolio Manager initialized
  Symbols: 12
  Strategies: 28
  Existing configs: 0

============================================================
RUNNING OPTIMIZATION
============================================================
Fetching historical data for 12 symbols...
  EURUSD: 450000 bars saved
  GBPUSD: 450000 bars saved
  ...

============================================================
OPTIMIZING: EURUSD (Regime-Aware)
============================================================
[EURUSD] Regime Optimization: 28 strategies x 6 timeframes x 4 regimes
[EURUSD] [H1] Hyperparameter tuning 3 strategies
[EURUSD] [H1] [TREND] Winner: SupertrendStrategy [TUNED] (quality=0.723, train=89, val=31) [VALIDATED: robustness=0.87]
[EURUSD] [H1] [RANGE] Winner: BollingerBounceStrategy (quality=0.681, train=72, val=28) [VALIDATED: robustness=0.82]
[EURUSD] [H1] [BREAKOUT] Winner: SqueezeBreakoutStrategy (quality=0.695, train=45, val=18) [VALIDATED: robustness=0.79]
[EURUSD] [H1] [CHOP] Candidate RSIExtremesStrategy FAILED validation: Low robustness 0.62 < 0.80
...

[EURUSD] OPTIMIZATION COMPLETE
  Timeframes:  4
  Validated:   12
  Rejected:    4
  Best:        SupertrendStrategy @ H1/TREND
  Retrain:     Every 60 days
  Duration:    45.2s
```

### 4.4 Optimization Duration

| Symbols | Estimated Time |
|---------|---------------|
| 5 | 5-10 minutes |
| 15 | 15-30 minutes |
| 40+ | 45-90 minutes |

### 4.5 Verify Results

Check that `pm_configs.json` was created:

```bash
# Windows
type pm_configs.json

# Or open in text editor
```

Look for:
- Each symbol has `regime_configs` populated
- `is_validated: true` for symbols you want to trade
- Reasonable `quality_score` values (0.5-0.9)

---

## 5. Paper Trading

### 5.1 Start Paper Trading

```bash
python pm_main.py --trade --paper
```

### 5.2 What to Expect

```
============================================================
STARTING LIVE TRADING
Trading enabled: False
Auto-retrain: False
============================================================
Starting live trading loop...
Trading enabled: False
Symbols: 12
Validated configs: 10

[EURUSD] Selected: SupertrendStrategy @ H1/TREND (strength=0.82, quality=0.72, freshness=1.00, score=0.591)
[EURUSD] BUY | basis=10000.00 (balance) | target_risk=1.00% ($100.00) | actual_risk=0.98% ($98.00) | vol_raw=0.1523 | vol=0.15 | entry=1.08520 | sl=1.08020 | tp=1.09520
[EURUSD] [PAPER] Would execute LONG: 0.15 lots @ 1.08520 | SL=1.08020 | TP=1.09520
```

### 5.3 Paper Trading Checklist

Monitor for 1-2 weeks and verify:

- [ ] Signals generated at expected frequency
- [ ] Risk calculations match expectations
- [ ] No errors in logs
- [ ] Regime detection seems reasonable
- [ ] No duplicate signals (throttle working)

### 5.4 Review Logs

```bash
# View today's log
type pm_outputs\logs\pm_20260131.log

# Or use: tail -f (Git Bash)
tail -f pm_outputs/logs/pm_*.log
```

### 5.5 Stop Paper Trading

Press `Ctrl+C` to stop gracefully.

---

## 6. Live Trading

### 6.1 Pre-Live Checklist

Before going live, ensure:

- [ ] Completed 1-2 weeks of paper trading
- [ ] Reviewed all paper trade logs for issues
- [ ] Account has sufficient margin
- [ ] Risk settings are conservative
- [ ] You understand the strategies being used
- [ ] You're prepared for potential losses

### 6.2 Conservative Start Settings

For your first week of live trading:

```json
"position": {
  "risk_per_trade_pct": 0.5,    // Half of normal
  "max_risk_pct": 2.5           // Half of normal
}
```

### 6.3 Start Live Trading

```bash
python pm_main.py --trade
```

### 6.4 What to Expect

```
============================================================
STARTING LIVE TRADING
Trading enabled: True
Auto-retrain: False
============================================================

[EURUSD] Selected: SupertrendStrategy @ H1/TREND (strength=0.82, quality=0.72, freshness=1.00, score=0.591)
[EURUSD] BUY | basis=10000.00 (balance) | target_risk=0.50% ($50.00) | actual_risk=0.49% ($49.00) | vol_raw=0.0761 | vol=0.07 | entry=1.08520 | sl=1.08020 | tp=1.09520
✓ [EURUSD] LONG executed: 0.07 lots @ 1.08523
```

### 6.5 Autonomous Mode (Auto-Retrain)

For fully autonomous operation:

```bash
python pm_main.py --trade --auto-retrain
```

This will:
- Trade based on current configurations
- Check hourly if any symbol needs retraining
- Automatically re-run optimization when retrain period expires
- Update configurations and continue trading

### 6.6 Running as Background Service

#### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task → Name: "FX Portfolio Manager"
3. Trigger: "When the computer starts"
4. Action: Start a program
   - Program: `C:\path\to\.venv\Scripts\python.exe`
   - Arguments: `pm_main.py --trade --auto-retrain`
   - Start in: `C:\path\to\FX_Portfolio_Manager`
5. Check "Run whether user is logged on or not"

#### Keep-Alive Script

Create `run_forever.bat`:
```batch
@echo off
:loop
python pm_main.py --trade --auto-retrain
echo Restarting in 10 seconds...
timeout /t 10
goto loop
```

---

## 7. Monitoring and Maintenance

### 7.1 Daily Monitoring

Check logs daily for:

```bash
# View errors only
findstr /i "error\|fail\|exception" pm_outputs\logs\pm_*.log

# View trade executions
findstr /i "executed\|LONG\|SHORT" pm_outputs\logs\pm_*.log
```

### 7.2 Key Metrics to Watch

| Metric | Warning Sign | Action |
|--------|--------------|--------|
| Validation rejection rate | > 50% | Lower fx_min_robustness_ratio |
| Cache hit rate | < 80% | Check for issues |
| Risk cap skips | Frequent | Review symbol/risk settings |
| MT5 disconnections | Multiple/day | Check network/MT5 stability |

### 7.3 Weekly Maintenance

1. **Review trade logs**
   ```bash
   type pm_outputs\trades_*.json
   ```

2. **Check configuration expiry**
   ```bash
   python pm_main.py --status
   ```

3. **Review regime distribution**
   - Are strategies being selected for expected regimes?
   - Is CHOP blocking trades appropriately?

### 7.4 Monthly Maintenance

1. **Full re-optimization** (optional)
   ```bash
   python pm_main.py --optimize
   ```

2. **Tune regime parameters** (optional)
   ```bash
   python pm_regime_tuner.py --data-dir ./data --output regime_params.json
   ```

3. **Review overall performance**
   - Compare actual vs backtested results
   - Adjust risk settings if needed

### 7.5 Cache Statistics

Get cache performance:
```python
# In Python shell
from pm_main import FXPortfolioManager
app = FXPortfolioManager()
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

### 8.5 Multiple Configurations

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

#### "Symbol not available for trading"

Check if symbol is tradeable:
- Market may be closed
- Symbol may be view-only
- Insufficient margin

### 9.3 Risk/Position Issues

#### "SKIP: min lot would exceed max_risk_pct"

The broker's minimum lot size exceeds your risk budget.

**Solutions:**
1. Increase risk settings:
   ```json
   "risk_per_trade_pct": 2.0,
   "max_risk_pct": 10.0
   ```

2. Remove problematic symbol:
   ```json
   "symbols": ["EURUSD", "GBPUSD"]  // Remove US30
   ```

3. Use a broker with smaller minimums

#### "Could not compute loss_per_lot"

MT5 failed to calculate loss. Usually temporary.

**Check:**
- Market is open
- Symbol is tradeable
- Spread is reasonable

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

2. Check data quality:
   ```bash
   python -c "import pandas as pd; df = pd.read_csv('data/EURUSD_M5.csv'); print(len(df), df.isnull().sum().sum())"
   ```

3. Try different timeframes

#### Optimization is very slow

1. Reduce symbols:
   ```json
   "symbols": ["EURUSD", "GBPUSD"]
   ```

2. Reduce param combinations:
   ```json
   "regime_hyperparam_max_combos": 100
   ```

3. Reduce timeframes:
   ```json
   "timeframes": ["H1", "H4"]
   ```

### 9.5 Runtime Errors

#### "RuntimeWarning: Degrees of freedom <= 0"

Too few trades for statistical calculations. Not an error - metrics default to 0.

#### Memory errors

Too much data loaded.

**Solutions:**
1. Reduce max_bars:
   ```json
   "max_bars": 200000
   ```

2. Process fewer symbols at once:
   ```bash
   python pm_main.py --optimize --symbols EURUSD GBPUSD
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
pip install pandas numpy MetaTrader5
python pm_main.py --optimize

# Daily usage
python pm_main.py --trade --paper          # Test mode
python pm_main.py --trade                   # Live mode
python pm_main.py --trade --auto-retrain   # Autonomous mode

# Maintenance
python pm_main.py --status                  # Check configs
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
