# PM Dashboard

The dashboard is the PM's companion web interface for signal review, strategy inspection, analytics, and trade history.

It is read-mostly, not fully read-only:

- it reads PM outputs, config, logs, and historical data
- it writes its own `pm_dashboard/dashboard_config.json`
- it can trigger root `data/` refresh jobs through the dashboard API

The dashboard measures PM signal and trade behavior. It is not a broker-account P&L replacement.

---

## Pages

### Signal Desk (`/`)

- live signal list with filtering and sorting
- entry, stop, target, regime, strategy, and strength visibility
- notification and alert support
- staleness and watcher-state feedback
- quick sizing tools using PM instrument specs

### Strategies (`/strategies`)

- browse current PM strategies from `pm_configs.json`
- filter by symbol, timeframe, regime, and strategy
- inspect assigned winners and tuned parameters

### Analytics (`/analytics`)

- equity and drawdown views
- expectancy, profit factor, Sharpe, drawdown duration, recovery time, ulcer index
- trade reconstruction and simulation from historical OHLC data
- graceful fallback when chart libraries are unavailable

### Trade History (`/trades`)

- paginated trade table
- filtering and sorting
- detail drawer and export helpers

---

## API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/entries` | Current signal snapshot and metadata |
| `GET/POST` | `/api/config` | Read or update dashboard configuration |
| `GET` | `/api/strategies` | Current PM strategies and winners |
| `GET` | `/api/analytics` | Analytics summary and metrics |
| `GET` | `/api/trades` | Trade history |
| `POST` | `/api/simulate` | Historical trade reconstruction |
| `POST` | `/api/download_historical_data` | Trigger root `data/` maintenance via MT5 |

---

## Current Behavior Notes

### Source and config handling

- Dashboard config lives in `pm_dashboard/dashboard_config.json`
- PM root can be set via `--pm-root` or dashboard config
- The dashboard reloads current config at request time for key routes
- Asset version hashes are generated dynamically from the static directory

### Watcher behavior

- watcher performs an initial synchronous poll so pages are populated quickly
- alert keys are stabilized to reduce duplicate notifications
- notification helpers run asynchronously
- recent `EXECUTED` events can be retained even when later decisions overwrite the latest actionable state

### Analytics behavior

- expectancy math uses corrected loss handling
- pip values are resolved from PM specs when possible, with fallbacks for common asset classes
- trade ordering is stabilized for equity and recent-trade views
- cache keys are hardened to reduce stale analytics reuse

### Trade simulation

- simulation walks bars forward from the trade entry
- gap-through stop/target handling follows the PM backtest semantics
- simulation uses local root `data/*_M5.csv` and resamples as required

### Frontend behavior

- loading states are explicit
- Chart.js has fallback handling
- dark-theme charts are styled intentionally rather than relying on browser defaults

---

## Setup

From the PM repo root:

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r pm_dashboard\requirements.txt
```

The dashboard assumes the main PM dependencies such as `pandas` and `numpy` are already installed.

---

## Run

```bash
python -m pm_dashboard.app --pm-root "."
```

Defaults:

- host: `127.0.0.1`
- port: `8000`

Custom host/port:

```bash
python -m pm_dashboard.app --pm-root "." --host 0.0.0.0 --port 9000
```

---

## Configuration

Primary dashboard config file:

```text
pm_dashboard/dashboard_config.json
```

Important keys include:

- `pm_root`
- `file_patterns`
- `primary_sources`
- `log_sources`
- `log_max_files`
- `trade_files_pattern`
- `trade_map_max_age_minutes`
- `pm_configs_path`
- `explicit_files`
- `min_strength`
- `max_signal_age_minutes`
- `valid_actions`
- `valid_action_prefixes`
- `alert`
- `field_aliases`

The Settings UI updates common dashboard values and persists them back to `dashboard_config.json`.

---

## Data Maintenance

The dashboard includes `jobs.py` for root historical-data maintenance.

This can:

- run scheduled refreshes
- trigger manual refreshes through `/api/download_historical_data`
- refresh root `data/<SYMBOL>_M5.csv` files via MT5

This is why the dashboard is not documented as fully read-only.

---

## Limitations

- The dashboard is only as fresh as the PM outputs and available local data
- Trade simulation depends on local CSV coverage and cannot recreate missing market history
- The dashboard assumes a recognizable PM root with `config.json`, `data/`, `logs/`, and `pm_outputs/`

---

## File Structure

```text
pm_dashboard/
|-- app.py
|-- analytics.py
|-- watcher.py
|-- jobs.py
|-- parsers.py
|-- utils.py
|-- models.py
|-- dashboard_config.json
|-- requirements.txt
|-- templates/
`-- static/
```
