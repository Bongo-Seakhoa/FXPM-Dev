# PM Dashboard (Read-Only)

This dashboard is a self-contained, read-only companion to the Forex Portfolio Manager (PM). It scans PM outputs (signals, recommendations, trades, logs) and surfaces actionable entries for manual or semi-manual execution on a second account.

## What it does
- Monitors PM output files with configurable glob patterns
- Normalizes entries into a common signal model (symbol, timeframe, regime, direction, entry, SL, TP, strength, timestamp)
- Shows a live local web dashboard with filters and entry details
- Sends desktop notifications + sound when the valid entry set changes or a strongest signal changes
- Provides one-click copy buttons and a position sizing calculator

## Read-only guarantee
- The dashboard **never writes to PM files or folders**.
- It only reads PM artifacts (config, logs, outputs).
- All dashboard files live under `pm_dashboard/`.

## Setup
```
python -m venv .venv
.venv\\Scripts\\activate
pip install -r pm_dashboard\\requirements.txt
```

## Run
```
python -m pm_dashboard.app --pm-root "C:\\Users\\Bongo\\OneDrive\\Desktop\\FxPM 1.4 - Obtimized"
```

Defaults:
- Host: `127.0.0.1`
- Port: `8000`
- URL: `http://127.0.0.1:8000`

Strategies page:
- `http://127.0.0.1:8000/strategies`

To change host/port:
```
python -m pm_dashboard.app --pm-root "C:\\path\\to\\pm" --host 127.0.0.1 --port 8000
```

## Configuration
Primary config lives in `pm_dashboard/dashboard_config.json`.

Key settings:
- `pm_root`: PM project path (can also be passed via `--pm-root`)
- `file_patterns`: glob patterns for PM artifacts (signals, recommendations, trades, logs)
- `primary_sources`: primary files used as the source of truth (default: `last_actionable_log.json`, fallback `last_trade_log.json`)
- `log_sources`: log file patterns used to detect EXECUTED trades when trade files lag
- `log_max_files`: number of most recent logs to scan
- `trade_files_pattern`: trade files used to enrich EXECUTED entries with entry/SL/TP
- `trade_map_max_age_minutes`: discard stale trade-map enrichment (prevents mismatched prices)
- `pm_configs_path`: PM strategy config file used to enrich timeframe/regime/strategy
- `explicit_files`: exact file paths if you want to pin specific outputs
- `min_strength`: filter for valid entries
- `max_signal_age_minutes`: ignore stale signals
- `alert`: desktop notification rules
- `field_aliases`: schema drift handling (maps unknown fields to the normalized model)

The Settings section in the UI updates common values and saves them back to `dashboard_config.json`.

## Mapping unknown PM output files
If your PM outputs use different file names or schemas:
1. Add or adjust patterns in `file_patterns`
2. Or set `explicit_files` to exact paths
3. Add or extend `field_aliases` to map custom fields

## Notes
- Desktop notifications may require OS permissions.
- If a file is partially written while being read, the dashboard skips it and retries on the next poll.
- Position sizing uses PM `instrument_specs` when available (from the PM `config.json`).
- If present, `last_actionable_log.json` provides the latest EXECUTED / risk-cap decisions and is treated as the primary source.
