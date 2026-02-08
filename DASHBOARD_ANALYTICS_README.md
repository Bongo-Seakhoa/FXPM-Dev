# PM Dashboard Analytics - Historical Data & Trade Reconstruction

## Overview

The PM Dashboard Analytics system now includes **historical data download** and **trade outcome reconstruction** capabilities. This allows you to:

1. Download OHLC historical data from MT5 automatically
2. Reconstruct trade outcomes from signal entries using actual market data
3. Simulate different capital amounts and return metrics (dollar/pip/trade basis)
4. Analyze account-agnostic signal performance

## Architecture

### Components

```
pm_dashboard/
├── jobs.py                    # NEW: Historical data download scheduler
├── analytics.py               # UPDATED: Trade reconstruction logic
├── app.py                     # UPDATED: Simulation API endpoints
├── templates/analytics.html   # UPDATED: Simulation UI
└── static/analytics.js        # UPDATED: Simulation + per-unit logic
```

### Data Flow

```
Signal Entry → Historical Bars → Bar-by-Bar Simulation → Exit Outcome
     ↓                ↓                    ↓                    ↓
  timestamp      OHLC data         SL/TP hit check         pnl_pips
  entry_price                                              exit_price
  sl/tp                                                    close_reason
  direction                                                duration
```

## Features

### 1. Historical Data Download (`pm_dashboard/jobs.py`)

#### Automatic Daily Downloads

```python
from pm_dashboard.jobs import initialize_data_jobs
from pm_mt5 import MT5Connector

# Connect to MT5
mt5 = MT5Connector()
mt5.connect()

# Initialize downloader and scheduler
downloader, scheduler = initialize_data_jobs(
    pm_root='path/to/pm',
    mt5_connector=mt5,
    enable_scheduler=True  # Run daily at 00:05
)
```

#### Manual Download

```python
# Download yesterday's data
downloader.download_all_symbols()

# Download specific date
from datetime import datetime
downloader.download_all_symbols(date=datetime(2026, 2, 1))
```

#### Data Storage

Historical data is stored in:
```
pm_outputs/historical_data/
├── EURUSD_M5_20260201.csv
├── EURUSD_H1_20260201.csv
├── EURUSD_D1_20260201.csv
├── GBPUSD_M5_20260201.csv
...
```

Format: `{symbol}_{timeframe}_{YYYYMMDD}.csv`

### 2. Trade Outcome Reconstruction (`pm_dashboard/analytics.py`)

#### Core Algorithm

The `reconstruct_trade_outcome()` function simulates trade execution:

```python
def reconstruct_trade_outcome(trade_entry, historical_bars):
    """
    Walk through bars after entry to find SL/TP hit.

    For LONG trades:
        - Check Low <= SL first (conservative)
        - Then check High >= TP

    For SHORT trades:
        - Check High >= SL first (conservative)
        - Then check Low <= TP

    Returns:
        {
            'exit_timestamp': datetime,
            'exit_price': float,
            'close_reason': 'SL_HIT' | 'TP_HIT' | 'TIMEOUT',
            'pnl_pips': float,
            'duration_minutes': float
        }
    """
```

#### Example Usage

```python
from pm_dashboard.analytics import reconstruct_trade_outcome
import pandas as pd

# Trade entry
trade = {
    '_parsed_timestamp': datetime(2026, 2, 1, 10, 0),
    'symbol': 'EURUSD',
    'entry_price': 1.1000,
    'sl': 1.0950,  # 50 pips
    'tp': 1.1100,  # 100 pips
    'direction': 'LONG'
}

# Load historical bars (from CSV or MT5)
bars = pd.read_csv('EURUSD_H1_20260201.csv', index_col=0, parse_dates=True)

# Reconstruct
outcome = reconstruct_trade_outcome(trade, bars)

print(f"Closed: {outcome['close_reason']}")  # TP_HIT or SL_HIT
print(f"PnL: {outcome['pnl_pips']:.1f} pips")
print(f"Duration: {outcome['duration_minutes']:.0f} min")
```

#### Pip Size Calculation

Automatic pip size detection:

| Symbol Type | Pip Size | Examples |
|------------|----------|----------|
| FX (non-JPY) | 0.0001 | EURUSD, GBPUSD |
| FX (JPY) | 0.01 | USDJPY, EURJPY |
| Metals/Indices | 0.1 | XAUUSD, US30 |
| Crypto | 1.0 | BTCUSD, ETHUSD |

### 3. Simulation API (`pm_dashboard/app.py`)

#### Endpoint: `/api/simulate`

**Request:**
```json
POST /api/simulate
{
    "initial_capital": 10000,
    "start_date": "2026-01-01T00:00:00",
    "end_date": "2026-02-08T00:00:00",
    "return_basis": "dollar",  // "dollar", "pip", or "trade"
    "max_trades": 1000
}
```

**Response:**
```json
{
    "success": true,
    "simulated": true,
    "message": "Reconstructed 156 trades",
    "total_trades": 156,
    "return_basis": "dollar",
    "metrics": {
        "total_trades": 156,
        "win_rate": 65.4,
        "profit_factor": 2.1,
        "sharpe_ratio": 1.8,
        "max_drawdown_pct": 8.2,
        ...
    },
    "equity_curve": [...],
    "drawdown_curve": [...],
    "trades": [...]  // First 50 trades
}
```

#### Return Basis Options

1. **Dollar (`"dollar"`)**: Standard P&L in account currency
   - `pnl = pnl_pips * pip_value`
   - Default: $10/pip for FX

2. **Pip (`"pip"`)**: Pure pip-based performance
   - `pnl = pnl_pips`
   - Platform/capital agnostic

3. **Trade (`"trade"`)**: Binary win/loss
   - `pnl = +1` (win), `-1` (loss), `0` (neutral)
   - Focus on win rate vs capital

#### Endpoint: `/api/download_historical_data`

Trigger manual data download:

```json
POST /api/download_historical_data
{}
```

Response:
```json
{
    "success": true,
    "message": "Historical data download started"
}
```

### 4. Dashboard UI (`pm_dashboard/templates/analytics.html`)

#### Simulation Controls

The Analytics page now includes:

- **Initial Capital**: Adjust starting balance for simulation
- **Date Range**: Filter trades by date
- **Return Basis**: Toggle between dollar/pip/trade metrics
- **Simulate Button**: Run reconstruction
- **Download Data Button**: Fetch historical data from MT5

#### Per-Unit Return Display

Toggle between:
- **Per Dollar**: Traditional P&L ($)
- **Per Pip**: Broker-agnostic performance (pips)
- **Per Trade**: Win/loss binary (+1/-1)

All metrics (Sharpe, drawdown, equity curve) recalculate based on selected basis.

## Testing

### Unit Tests

Run the test suite:

```bash
cd "c:\Users\Bongo\OneDrive\Desktop\FxPM 1.4 - Obtimized"
python test_reconstruction.py
```

**Test Coverage:**
- ✓ LONG trade hits TP (100 pips profit)
- ✓ LONG trade hits SL (50 pips loss)
- ✓ SHORT trade hits TP (100 pips profit)
- ✓ SHORT trade hits SL (50 pips loss)
- ✓ Trade timeout (no SL/TP hit)
- ✓ Pip size calculation (EURUSD, USDJPY, XAUUSD, etc.)

### Integration Test

1. **Start Dashboard**:
   ```bash
   python -m pm_dashboard.app --pm-root "c:\Users\Bongo\OneDrive\Desktop\FxPM 1.4 - Obtimized"
   ```

2. **Navigate to Analytics** (`http://127.0.0.1:8000/analytics`)

3. **Download Historical Data**:
   - Click "Download Historical Data"
   - Wait for completion (check logs)

4. **Run Simulation**:
   - Set initial capital: $10,000
   - Select date range (e.g., last 30 days)
   - Choose return basis: "Per Pip"
   - Click "Run Simulation"

5. **Verify Results**:
   - Check equity curve updates
   - Verify metrics are in pips (not dollars)
   - Inspect recent trades for `close_reason`, `exit_price`, `pnl_pips`

## Error Handling

### Missing Historical Data

If historical data is unavailable:
- Simulation falls back to existing trade PnL (if present)
- Warning message: "Using existing trade data (MT5 not available)"
- Reconstructed trades will show `close_reason: 'NO_DATA'`

### MT5 Connection Issues

If MT5 is not connected:
- Data download features are disabled
- Simulation uses cached CSV files
- Dashboard shows: "MT5 or jobs not available - simulation features will be limited"

### Invalid Trade Entries

Trades with missing fields are skipped:
- Missing `timestamp` → `close_reason: 'INVALID_ENTRY_TIME'`
- Missing `entry_price`/`sl`/`tp` → `close_reason: 'MISSING_PRICES'`
- Invalid `direction` → `close_reason: 'INVALID_DIRECTION'`

## Performance Optimization

### Caching Strategy

- Historical data files are cached locally (CSV)
- Subsequent loads read from disk (fast)
- Re-download only if file missing or empty

### Parallel Downloads

Default configuration:
- Sleep 0.1s between symbols (avoid MT5 throttling)
- Downloads all timeframes for symbol before moving to next

Optimize for speed:
```python
# In jobs.py, reduce sleep time
time.sleep(0.05)  # Faster but riskier
```

### Reconstruction Limits

- Default: Max 1000 trades per simulation
- Increase via API: `"max_trades": 5000`
- Each trade processes up to 1000 bars (configurable via `timeout_bars`)

## Quality Checks

### Validation Steps

1. **Pip Calculation Accuracy**:
   - Test: EURUSD 100 pip move = 0.0100 price change
   - Test: USDJPY 100 pip move = 1.00 price change

2. **SL/TP Hit Detection**:
   - Conservative approach: Check SL first (realistic slippage)
   - Intra-bar order: Low then High (LONG), High then Low (SHORT)

3. **Exit Timestamp**:
   - Uses bar timestamp (open time)
   - Duration = exit_bar_time - entry_time

4. **PnL Consistency**:
   - TP hit: Positive PnL
   - SL hit: Negative PnL
   - Magnitude matches pip distance

### Known Limitations

1. **Intra-Bar Precision**: Cannot determine exact order of High/Low within bar
   - Assumption: SL hit before TP if both possible in same bar (conservative)

2. **Spread**: Not factored into reconstruction
   - Entry price assumed to be filled at exact signal price
   - Add buffer to SL/TP if needed: `sl = sl + spread`

3. **Slippage**: Not simulated
   - Exit price = exact SL or TP
   - Real execution may differ by 0-2 pips

4. **Partial Fills**: Not supported
   - Assumes full position filled at entry
   - Assumes full position closed at exit

## Core Objectives Alignment

### How This Supports PM Goals

| Objective | Feature Benefit |
|-----------|-----------------|
| **High Return** | Accurate PnL reconstruction validates signal profitability |
| **High Win Rate** | Realistic SL/TP hit detection shows true win percentage |
| **Low Drawdown** | Reconstructed equity curve reveals actual drawdown path |
| **Reliability** | Historical validation proves consistency across time |
| **Trade Frequency** | All signals tracked, measurable signal generation rate |

### Account-Agnostic Analysis

Using **per-pip** basis removes:
- Account size dependency
- Broker spread variations
- Capital allocation differences

Perfect for:
- Comparing strategies across platforms
- Validating signal quality independently
- Backtesting before live deployment

## Troubleshooting

### Issue: "No historical data available"

**Solution**:
1. Ensure MT5 is connected
2. Run manual download: `POST /api/download_historical_data`
3. Check logs for download errors
4. Verify symbols in `config.json` match MT5 symbols

### Issue: All trades show "TIMEOUT"

**Cause**: Historical bars don't extend far enough

**Solution**:
- Increase `timeout_bars` in reconstruction call
- Download more historical data (longer date range)
- Check if symbol data is available for that period

### Issue: PnL doesn't match expected

**Debug Steps**:
1. Check pip size: `get_pip_size('SYMBOL')`
2. Verify entry/SL/TP values in trade entry
3. Inspect historical bars around entry time
4. Confirm direction is correct ('LONG' vs 'SHORT')

### Issue: Dashboard shows simulation disabled

**Cause**: MT5 connector failed to initialize

**Solution**:
1. Check MT5 is installed and running
2. Verify credentials in `config.json` (if needed)
3. Test MT5 connection: `python -c "from pm_mt5 import MT5Connector; c = MT5Connector(); print(c.connect())"`

## Future Enhancements

### Planned Features

1. **Multi-Position Sizing**:
   - Simulate different lot sizes per trade
   - Kelly criterion position sizing

2. **Spread Integration**:
   - Load broker spread from historical data
   - Adjust entry/exit prices accordingly

3. **Commission Calculation**:
   - Per-lot commission from config
   - Realistic net PnL after fees

4. **Partial Exits**:
   - Simulate scaling out at TP levels
   - Trailing stop reconstruction

5. **Correlation Analysis**:
   - Concurrent trade overlap detection
   - Drawdown correlation by regime

## Files Modified/Created

### New Files
- `pm_dashboard/jobs.py` (429 lines) - Data download scheduler
- `test_reconstruction.py` (291 lines) - Reconstruction tests
- `DASHBOARD_ANALYTICS_README.md` (This file)

### Updated Files
- `pm_dashboard/analytics.py` - Added reconstruction functions
- `pm_dashboard/app.py` - Added simulation endpoints
- `pm_dashboard/templates/analytics.html` - Added simulation UI
- `pm_dashboard/static/analytics.js` - Added simulation logic

## Support

For issues or questions:
1. Check logs: `logs/pm_*.log`
2. Run tests: `python test_reconstruction.py`
3. Review dashboard console (browser F12)

---

**Version**: 1.0
**Date**: 2026-02-08
**Status**: Production Ready ✓
