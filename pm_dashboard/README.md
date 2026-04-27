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
- live command panel for ledger coverage, readiness, signal/action health, and delivery status
- entry, stop, target, regime, strategy, and strength visibility
- notification and alert support
- optional Telegram publishing for valid signals when explicitly enabled
- staleness and watcher-state feedback
- quick sizing tools using PM instrument specs

### Strategies (`/strategies`)

- browse current PM strategies from the active winner ledger
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
| `GET` | `/api/live-command` | Operator readiness summary for active ledger, signals, trade events, and Telegram status |
| `GET/POST` | `/api/config` | Read or update dashboard configuration. Writes require loopback access or a write token. |
| `GET` | `/api/strategies` | Current PM strategies and winners |
| `GET` | `/api/analytics` | Analytics summary and metrics |
| `GET` | `/api/trades` | Trade history |
| `POST` | `/api/simulate` | Historical trade reconstruction |
| `POST` | `/api/download_historical_data` | Trigger root `data/` maintenance via MT5. Writes require loopback access or a write token. |

---

## Current Behavior Notes

### Source and config handling

- Dashboard config lives in `pm_dashboard/dashboard_config.json`
- PM root can be set via `--pm-root` or dashboard config
- The dashboard reloads current config at request time for key routes
- By default, `pm_configs_path = "auto"` follows `pipeline.winner_ledger_path` from the PM root `config.json`
- Asset version hashes are generated dynamically from the static directory

### Watcher behavior

- watcher performs an initial synchronous poll so pages are populated quickly
- alert keys are stabilized to reduce duplicate notifications
- notification helpers run asynchronously
- recent `EXECUTED` events can be retained even when later decisions overwrite the latest actionable state
- Telegram delivery is disabled by default, uses a bot token from environment, and dedupes signal keys in memory during the active dashboard session

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

When binding beyond loopback, set a write token before exposing the dashboard:

```bash
$env:PM_DASHBOARD_WRITE_TOKEN = "choose-a-long-random-token"
python -m pm_dashboard.app --pm-root "." --host 0.0.0.0 --port 9000
```

Write APIs accept the token in `X-PM-Dashboard-Token` or `Authorization: Bearer <token>`.
Without a token, remote requests can read dashboard pages/API data but cannot update dashboard config or start root data refreshes. Local loopback writes remain available for the default single-user workflow, with cross-origin browser writes rejected.

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
- `pm_configs_path` (`"auto"` follows `pipeline.winner_ledger_path`; set a file path only for an explicit dashboard override)
- `write_api_token_env`
- `explicit_files`
- `min_strength`
- `max_signal_age_minutes`
- `valid_actions`
- `valid_action_prefixes`
- `alert`
- `telegram`
- `field_aliases`

The Settings UI updates common dashboard values and persists them back to `dashboard_config.json`.

### Telegram Signal Publishing

Telegram publishing is opt-in. The dashboard never stores a bot token in `dashboard_config.json`; it stores the environment variable name and reads the token at runtime.

Recommended setup:

```powershell
$env:PM_DASHBOARD_TELEGRAM_BOT_TOKEN = "123456:bot-token"
python -m pm_dashboard.app --pm-root "."
```

Then set the Telegram chat/group ID in the dashboard settings and enable Telegram Signals. By default, the Telegram message includes symbol, direction, action, timeframe/regime, entry, SL, TP, R:R, strength, and timestamp. Strategy names are hidden unless `telegram.include_strategy` is enabled.

---

## Data Maintenance

The dashboard includes `jobs.py` for root historical-data maintenance.

This can:

- run scheduled refreshes
- trigger manual refreshes through `/api/download_historical_data`
- refresh root `data/<SYMBOL>_M5.csv` files via MT5

Refresh writes are protected by an in-process lock, merge returned MT5 bars with existing local bars, and publish the CSV with an atomic replace so readers do not observe partial files.

The scheduler now uses the same due-time comparator style as the PM runtime instead of a fixed sleep loop, so it stays responsive without carrying an extra polling cadence of its own.

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
