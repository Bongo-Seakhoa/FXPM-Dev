# PM Dashboard

A self-contained, read-only companion to the Forex Portfolio Manager (PM). It scans PM outputs (signals, recommendations, trades, logs) and surfaces actionable entries for manual or semi-manual execution. The dashboard measures **PM signal performance** and is account-agnostic -- it evaluates the quality of PM decisions, not MT5 account P&L.

## Pages

### Signal Desk (`/`)
Live signal monitor with filtering, entry details, and position sizing.
- Real-time signal cards with symbol, direction, entry/SL/TP, strength, regime
- One-click copy buttons for quick trade entry
- Built-in position sizing calculator using PM `instrument_specs`
- Desktop notifications + sound on new or changed signals
- Filters by symbol, timeframe, regime, direction, strategy, and free-text search
- Sort options (recent/strength/symbol) and latest-per-symbol-timeframe toggle
- Staleness/status banner for watcher errors or no-source conditions
- Persistent sizing/view preferences via localStorage

### Strategies (`/strategies`)
Browse all 47 PM strategies with their configuration and regime assignments.
- Strategy cards grouped by category (Momentum, Trend, Breakout, etc.)
- Shows tuned parameters, assigned regimes, and timeframes per symbol
- Filter by symbol/timeframe/regime/strategy plus free-text search
- Selected-row drill-down state persists while filtering/paging
- Loaded from `pm_configs.json`

### Analytics (`/analytics`)
Performance analytics with equity curves, drawdown analysis, and trade simulation.
- Equity curve and drawdown charts
- Key metrics: total return, win rate, profit factor, max drawdown, Sharpe ratio
- Extended risk metrics: drawdown duration, recovery time, and ulcer index
- Trade simulation with historical OHLC reconstruction (bar-by-bar SL/TP walk)
- Configurable initial capital, date range, and return basis (dollar/pip/trade)
- Symbol-aware PnL calculation (FX, gold, crypto, indices)
- Graceful fallback to metrics/tables when Chart.js is unavailable

### Trade History (`/trades`)
Searchable trade log with filtering and export.
- Paginated table of all recorded PM trades
- Columns: timestamp, symbol, direction, volume, price, SL, TP, PnL, status, strategy
- Filter by symbol, direction, status, timeframe, regime, and free-text search
- Sortable columns
- Row selection highlighting with detail drawer and copy helper

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/entries` | Current signal snapshot with config metadata |
| GET/POST | `/api/config` | Read or update dashboard configuration |
| GET | `/api/strategies` | Strategy list (optional `?include_invalid=true`) |
| GET | `/api/analytics` | Performance metrics (optional `?initial_capital=N`) |
| GET | `/api/trades` | Trade history (optional `?limit=N`, default 200) |
| POST | `/api/simulate` | Trade outcome reconstruction with historical data |
| POST | `/api/download_historical_data` | Trigger manual root `data/` M5 maintenance via MT5 |

## Post-Redesign Notes (2026-02-08)

- Frontend UI system was rebuilt for consistent layout, spacing, typography, and responsive behavior across all dashboard pages.
- Signal Desk now supports richer filtering/searching/sorting and better operational feedback (last update age + warning banner).
- Backend routes for `/api/analytics`, `/api/trades`, and `/api/simulate` now read the **current** dashboard config at request time, so `pm_root` updates from the UI apply immediately.
- CSV export paths now escape values safely to prevent malformed exports on special characters.

## Data Loading Reliability Updates (2026-02-09)

- `pm_root` is now normalized at startup and on config save. Invalid values automatically fall back to the detected PM project root instead of silently returning empty datasets.
- The watcher now performs an initial synchronous poll before the background thread starts, so Signal Desk, Strategies, Analytics, and Trade History are populated immediately on first load.
- When `last_actionable_log.json` is the primary source, the watcher now also keeps latest `EXECUTED` events from PM logs for symbols whose latest actionable decision is non-executed (for example, `SKIPPED_RISK_CAP`). This prevents fresh executions from being hidden.
- Trade history enrichment now infers missing `timeframe`, `regime`, and `strategy` from `pm_configs.json` and trade `magic` values, so Analytics/Trades/Simulation remain populated even when `trades_*.json` records are minimal.

## Read-Only Guarantee

- The dashboard **never writes to PM files or folders**.
- It only reads PM artifacts (config, logs, outputs).
- All dashboard files live under `pm_dashboard/`.
- Dashboard config is stored separately in `pm_dashboard/dashboard_config.json`.

## Setup

```
python -m venv .venv
.venv\Scripts\activate
pip install -r pm_dashboard\requirements.txt
```

Dependencies: `Flask>=2.3,<3.0`, `plyer>=2.1.0` (for desktop notifications).

The main PM dependencies (`pandas`, `numpy`) are assumed to be already installed.

## Run

```
python -m pm_dashboard.app --pm-root "C:\path\to\FxPM 1.4 - Obtimized"
```

Defaults:
- Host: `127.0.0.1`
- Port: `8000`
- URL: http://127.0.0.1:8000

To change host/port:
```
python -m pm_dashboard.app --pm-root "C:\path\to\FxPM" --host 0.0.0.0 --port 9000
```

## Configuration

Primary config lives in `pm_dashboard/dashboard_config.json`.

Key settings:
- `pm_root` -- PM project path (also settable via `--pm-root`)
  Relative paths (for example `"."`) are resolved from the PM dashboard config location.
- `file_patterns` -- glob patterns for PM artifacts (signals, recommendations, trades, logs)
- `primary_sources` -- primary files used as source of truth (default: `last_actionable_log.json`, fallback `last_trade_log.json`)
- `log_sources` -- log file patterns to detect EXECUTED trades when trade files lag
- `log_max_files` -- number of most recent logs to scan
- `trade_files_pattern` -- trade files used to enrich EXECUTED entries with entry/SL/TP
- `trade_map_max_age_minutes` -- discard stale trade-map enrichment (prevents mismatched prices)
- `pm_configs_path` -- PM strategy config file for timeframe/regime/strategy enrichment
- `explicit_files` -- exact file paths to pin specific outputs
- `min_strength` -- filter for valid entries
- `max_signal_age_minutes` -- ignore stale signals
- `valid_actions` / `valid_action_prefixes` -- define which PM actions are treated as valid-now signals (default includes `EXECUTED`, `SKIPPED_RISK_CAP`, `BLOCKED_RISK_CAP`)
- `alert` -- desktop notification rules
- `field_aliases` -- schema drift handling (maps unknown fields to normalized model)

The Settings section in the UI updates common values and saves them back to `dashboard_config.json`.

## Trade Simulation

The Analytics page includes a trade simulation feature that reconstructs outcomes from historical OHLC data:

1. Loads PM trade records (entry price, SL, TP, direction, volume)
2. Enriches missing trade metadata (`timeframe`/`regime`/`strategy`) from `pm_configs.json` + magic-number mapping when needed
3. Loads local root historical bars from `data/*_M5.csv` and resamples to the required timeframe
4. Walks bars forward from entry to determine SL hit, TP hit, or open
5. Computes PnL in pips and dollars with symbol-aware pip values
6. Builds equity curve and drawdown series from reconstructed outcomes

This runs without needing an active MT5 connection when root `data/*_M5.csv` files are already present.

## Data Maintenance

The dashboard includes a background data scheduler (`jobs.py`) for root market-data maintenance:

- Automatic daily maintenance at configurable time (default `00:00`)
- Manual trigger via the `/api/download_historical_data` endpoint
- Refreshes `data/<SYMBOL>_M5.csv` using the same MT5 call path as PM main app (`get_bars(..., "M5", count=max_bars)`)
- Uses `config.json` symbols and `pipeline.max_bars` for maintenance depth
- Analytics simulation resamples M5 locally to M15/M30/H1/H4/D1 as needed

Requires MT5 connection only for refresh operations. Simulation itself reads local root data.

## File Structure

```
pm_dashboard/
  app.py                 # Flask application, routes, startup
  analytics.py           # Performance metrics, trade reconstruction, equity curves
  watcher.py             # File watcher for PM output monitoring
  utils.py               # Shared utilities (config I/O, parsing, normalization)
  jobs.py                # Root M5 data maintenance scheduler/loader
  dashboard_config.json  # Dashboard configuration
  requirements.txt       # Python dependencies
  templates/
    index.html           # Signal Desk page
    strategies.html      # Strategies page
    analytics.html       # Analytics page
    trades.html          # Trade History page
  static/
    common.js            # Shared JS (theme toggle, toast, utilities)
    app.js               # Signal Desk logic
    strategies.js        # Strategies page logic
    analytics.js         # Analytics charts and simulation
    trades.js            # Trade History table and filters
    styles_enhanced.css  # Dashboard styling with dark/light theme
```

## Notes

- Desktop notifications may require OS permissions.
- If a file is partially written while being read, the dashboard skips it and retries on the next poll.
- Position sizing uses PM `instrument_specs` from `config.json`.
- If present, `last_actionable_log.json` provides the latest EXECUTED / risk-cap decisions and is treated as the primary source.
- PM execution logs are also scanned to retain recent `EXECUTED` records that may be replaced in `last_actionable_log.json` by later non-executed decisions for the same symbol.
- Dark/light theme toggle persists across sessions via localStorage.
