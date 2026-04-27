# FX Portfolio Manager v3.1 - Setup and Run Guide

This guide covers installation, configuration, optimization, paper trading, live trading, monitoring, and maintenance for the current PM.

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
9. [Dashboard](#9-dashboard)
10. [Troubleshooting](#10-troubleshooting)
11. [Quick Reference](#11-quick-reference)

---

## 1. Prerequisites

### System requirements

| Requirement | Minimum | Recommended |
|---|---:|---:|
| OS | Windows 10 | Windows 10/11 |
| Python | 3.8 | 3.10+ |
| RAM | 4 GB | 8+ GB |
| Disk | 2 GB free | 10+ GB free |

MetaTrader 5 Python integration requires Windows.

### Required software

1. Python 3.8+
2. MetaTrader 5 installed and logged in
3. AutoTrading enabled in MT5

---

## 2. Installation

### Create and activate a virtual environment

```bash
python -m venv .venv
```

Windows PowerShell:

```bash
.venv\Scripts\Activate.ps1
```

Git Bash:

```bash
source .venv/Scripts/activate
```

### Install packages

```bash
pip install pandas numpy MetaTrader5
pip install numba optuna
```

### Verify imports

```bash
python -c "import pandas, numpy, MetaTrader5; print('Core packages OK')"
python -c "from pm_main import FXPortfolioManagerApp; print('PM import OK')"
```

### Expected root layout

```text
FxPM 1.4 - Obtimized/
|-- pm_core.py
|-- pm_strategies.py
|-- pm_pipeline.py
|-- pm_main.py
|-- pm_mt5.py
|-- pm_position.py
|-- pm_regime.py
|-- pm_regime_tuner.py
|-- pm_optuna.py
|-- config.json
|-- data/
|-- logs/
`-- pm_outputs/
```

If `data/`, `logs/`, or `pm_outputs/` do not exist yet, they will be created as the PM runs.

---

## 3. Configuration

`config.json` is the primary PM configuration file. The committed file currently reflects a high-risk low-balance live profile with a standard research/backtest baseline. Review it before deployment.

### Current important settings

```json
{
  "pipeline": {
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    "log_dir": "./logs",
    "initial_capital": 10000.0,
    "risk_per_trade_pct": 1.0,
    "scoring_mode": "fx_backtester",
    "timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
    "data_workflow_mode": "active_recent_m5",
    "max_bars": 300000,
    "historical_stress_audit_bars": 50000,
    "active_universe_bars": 250000,
    "active_stage2_pct": 50.0,
    "max_param_combos": 200,
    "optimization_max_workers": 4,
    "regime_hyperparam_top_k": 5,
    "regime_hyperparam_max_combos": 200,
    "risk_management_optimization_enabled": true,
    "risk_management_selection_stage": "stage3",
    "production_retrain_mode": "notify",
    "production_retrain_interval_weeks": 2,
    "production_retrain_weekday": "sunday",
    "production_retrain_time": "00:01",
    "production_retrain_anchor_date": "2026-03-29",
    "storage_enabled": true,
    "storage_observe_only": true,
    "storage_signal_ledger_enabled": true,
    "storage_warn_free_gb": 15.0,
    "storage_critical_free_gb": 10.0,
    "storage_local_data_first_enabled": true,
    "storage_live_sync_bars": 3000,
    "storage_live_sync_overlap_bars": 100,
    "storage_live_cache_max_age_days": 7,
    "live_loop_trigger_mode": "bar",
    "live_bar_poll_seconds": 0.25,
    "live_bar_settle_seconds": 5,
    "live_stale_retry_seconds": 15,
    "live_risk_scalars_enabled": false,
    "live_risk_scalars_mode": "shadow",
    "local_governance_tournament_enabled": true,
    "local_governance_live_mode": "shadow",
    "winner_ledger_path": "pm_configs_high_risk.json",
    "target_annual_vol": 0.10,
    "execution_spread_filter_enabled": true,
    "execution_spread_min_edge_mult": 1.25,
    "execution_spread_spike_mult": 3.0,
    "execution_spread_penalty_start_mult": 0.75,
    "min_trade_risk_pct": 0.05
  },
  "position": {
    "risk_per_trade_pct": 2.0,
    "risk_basis": "balance",
    "max_risk_pct": 3.0,
    "auto_widen_sl": true
  },
  "mt5": {
    "login": 0,
    "password": "",
    "server": "",
    "path": "",
    "timeout": 60000,
    "portable": false
  }
}
```

### Configuration notes

- `mt5.login = 0` means use the currently logged-in MT5 session.
- Runtime logs go to `logs/`, not `pm_outputs/`.
- `pipeline.output_dir` controls optimization summaries and trade artifacts.
- Stage 1/Stage 2 currently use the active recent M5 workflow: latest 300,000 M5 bars, with the oldest 50,000 as `historical_stress_audit` and the newest 250,000 as the active universe. Stage 1 baseline eligibility runs across the full active universe; Stage 2 uses the newest active half as the fresh optimization/risk-selection surface.
- The checked-in Stage 1/Stage 2 presets are the current recommended production baseline. They favor quality, profitability, and live relevance over simply admitting more candidates.
- `risk_management_selection_stage = "stage3"` means per-winner governance/risk policy selection runs on the newest fresh selection surface after the strategy/timeframe/regime winner is chosen.
- `production_retrain_mode`:
  - `auto`: run the fixed schedule automatically
  - `notify`: tell the operator the PM is due, but do not auto-run optimization
  - `off`: disable schedule checks
- `execution_spread_spike_mult` is the live spread-spike blocker threshold.
- `position.risk_per_trade_pct` is the authoritative live target risk. `pipeline.risk_per_trade_pct` is the research/backtest value and only backfills live sizing if the `position` value is absent.
- `live_risk_scalars_mode = "shadow"` records the would-be risk-scalar effect without changing live position sizing.
- `local_governance_live_mode = "shadow"` keeps governance policy selection observable before broker-side stop management is made authoritative.
- storage governance is now config-backed through `pipeline.storage_*`.
  - `storage_observe_only = true` is the recommended first rollout mode because it records state, manifests, and cleanup candidates without deleting PM-owned files yet.
  - `storage_local_data_first_enabled = true` keeps live analysis local-first, but live sweeps now use bounded timeframe-specific `.live/<symbol>_<TF>.csv` caches instead of rereading the full canonical `*_M5.csv` files every cycle.
  - `storage_live_sync_overlap_bars = 100` controls the safety overlap when the PM delta-refreshes a bounded live cache from recent M5 bars.
  - If the local gap grows beyond that overlap window, the PM stops stitching a tiny delta across the gap and re-seeds the affected live cache from MT5 instead.
  - `storage_live_sync_bars = 3000` is the direct-MT5 M5 reseed floor for that repair path.
  - `storage_live_cache_max_age_days = 7` controls how long stale bounded live caches may stay on disk before housekeeping can prune them.
  - `live_loop_trigger_mode = "bar"` is the default live runtime mode: changed MT5 bar timestamps trigger live checks for the affected symbol/timeframe branches. `live_bar_poll_seconds` is only a CPU-idle polling interval for the Python MT5 API, not a signal timing rule.
  - `live_bar_settle_seconds = 5` and `live_stale_retry_seconds = 15` apply to `live_loop_trigger_mode = "scheduled"`, the legacy due-time fallback.
  - actionable live outcomes are also persisted to `pm_outputs/signal_ledger_YYYYMM.jsonl`.

### Instrument specs

The PM can read broker metadata live through MT5, but it still uses:

- `broker_specs_path`
- `instrument_spec_defaults`
- `instrument_specs`

for backtesting fallbacks and symbol-specific modeling when needed.

---

## 4. Running Optimization

### Standard optimization

```bash
python pm_main.py --optimize
```

### Force full re-optimization

```bash
python pm_main.py --optimize --overwrite
```

### Optimize specific symbols only

```bash
python pm_main.py --optimize --symbols EURUSD,GBPUSD
```

For expensive validation, prefer a one-symbol subset first. A full optimization run can take a long time and should be reserved for deliberate retraining or final verification.

### Current preset posture

- Stage 1 eligibility uses the full active universe, strict enough to remove weak candidates while preserving competitive diversity.
- Any Stage 1 "Top-K-like" wording refers only to the survivor pool produced by eligibility gates; it is not equivalent to the Stage 2 Top-K optimizer.
- Stage 2 optimization/risk selection uses the newest half of the active universe with warmup context, so execution selection stays close to the live market.
- Regime hyperparameter search currently uses Top-K `5` and `200` regime combinations in the checked-in profile.
- Family-size-aware Optuna budgeting is enabled in the checked-in profile so large strategy grids are not unfairly starved by a flat trial cap.
- Risk-management optimization is mandatory for selected winners and defaults to the Stage 3 fresh selection surface.
- The older `historical_stress_audit` window reports fragility; it does not overrule the live-relevance objective by acting as a forward holdout.

### What to expect

Example startup summary:

```text
FX PORTFOLIO MANAGER v3.1
Connected to MT5
Portfolio Manager initialized
  Symbols: 62
  Strategies: 47
```

Example optimization flow:

```text
RUNNING OPTIMIZATION
MODE: OVERWRITE
[EURUSD] Regime Optimization: 47 strategies x 6 timeframes x 4 regimes
[EURUSD] [H1] [TREND] Winner: SupertrendStrategy
SAVED EURUSD to <configured winner ledger>
```

### Typical status flow

Useful status check:

```bash
python pm_main.py --status
```

Typical output themes:

- how many symbols are loaded
- current strategy count
- whether configs are due or current
- retrain schedule summary

### Resume behavior

The optimization ledger is stateful and atomic. Its path comes from `pipeline.winner_ledger_path` unless overridden by `--winner-ledger`. If the process is interrupted, rerun the same command and the PM resumes from existing saved state unless `--overwrite` is used.

---

## 5. Paper Trading

### Start paper mode

```bash
python pm_main.py --trade --paper
```

Paper mode runs the live decision loop without sending real orders.

### Useful paper-mode checks

- confirm symbols and bars load correctly
- confirm MT5 connection is stable
- inspect `last_actionable_log.json`
- inspect `logs/pm_YYYYMMDD.log`

### Recommended paper period

- minimum: a few days
- better: 1-2 weeks
- best before serious live use: enough time to see multiple market conditions across your symbol set

---

## 6. Live Trading

### Start live mode

```bash
python pm_main.py --trade
```

### Live execution behavior

- The PM polls small MT5 bar probes and refreshes the full configured bar window only for symbol/timeframe branches whose broker bar timestamp changed
- Signals remain candle-bar signals; bar changes wake the live cycle and do not turn the strategy layer into tick-by-tick signal generation
- It computes features and the active regime
- It looks up the exact validated winner for `(timeframe, regime)`
- It applies risk sizing, broker constraints, spread-quality checks, and same-symbol exposure checks based on actual open-position geometry
- It sends orders through MT5 if a trade passes all gates

### Live safety notes

- The PM is candle-signal driven, so trades occur on the next actionable broker state after the signal decision.
- The spread-quality overlay can block trades when spread conditions are too poor.
- `position.risk_basis` controls whether live sizing uses account balance or equity.

### Pre-live checklist

- the configured winner ledger has been regenerated or confirmed current
- MT5 is open and logged in
- AutoTrading is enabled
- broker symbol naming matches the configured symbol list
- recent paper-mode logs look normal
- risk settings in `config.json` match the intended live account profile

### Graceful stop

Use `Ctrl+C` in the terminal. The PM is designed to stop cleanly and preserve the main decision/config state.

---

## 7. Monitoring and Maintenance

### Daily checks

1. Verify the process is running
2. Review `logs/pm_YYYYMMDD.log`
3. Check MT5 positions against recent PM decisions
4. Inspect `last_actionable_log.json` if you need the latest actionable state quickly

### Weekly checks

1. Review status:

```bash
python pm_main.py --status
```

2. Review warnings/errors:

```bash
Select-String -Path logs\\pm_*.log -Pattern "error|warning|failed" -CaseSensitive:$false
```

3. Confirm whether the PM is due under the configured production retrain schedule

### Retrain schedule

The PM no longer uses rolling `optimization_valid_days` or per-strategy retrain-period selection. Production retraining is controlled by the fixed calendar schedule in `config.json`:

- `production_retrain_interval_weeks`
- `production_retrain_weekday`
- `production_retrain_time`
- `production_retrain_anchor_date`
- `production_retrain_mode`

### Manual retrain when due

```bash
python pm_main.py --optimize
```

### Full refresh

```bash
python pm_main.py --optimize --overwrite
```

### Monthly review ideas

- compare live results against the recent optimization profile
- review symbols that repeatedly fail validation or tradability checks
- review spread filter behavior if too many otherwise-valid entries are blocked
- refresh regime parameters if the detector behavior appears stale

### Useful files during maintenance

- `logs/pm_YYYYMMDD.log`
- the active winner ledger from `pipeline.winner_ledger_path`
- `pm_outputs/optimization_summary.csv`
- `last_actionable_log.json`

---

## 8. Advanced Configuration

### Generalization controls

If too many candidates fail validation:

```json
"fx_gap_penalty_lambda": 0.50,
"fx_min_robustness_ratio": 0.75,
"fx_val_min_trades": 10
```

If the optimizer is still admitting fragile winners:

```json
"fx_gap_penalty_lambda": 0.80,
"fx_min_robustness_ratio": 0.90,
"regime_min_val_trades": 20
```

### Timeframe selection

Fewer timeframes for faster runs:

```json
"timeframes": ["H1", "H4", "D1"]
```

Broader set for more signal coverage:

```json
"timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"]
```

### Spread filter tuning

The live spread-quality overlay is driven by:

```json
"execution_spread_filter_enabled": true,
"execution_spread_min_edge_mult": 1.5,
"execution_spread_spike_mult": 3.0,
"execution_spread_penalty_start_mult": 0.5
```

Practical reading:

- lower `execution_spread_spike_mult` = stricter spike blocking
- higher `execution_spread_spike_mult` = looser spike blocking
- higher `execution_spread_min_edge_mult` = stricter ATR-vs-spread edge requirement

### Schedule modes

```json
"production_retrain_mode": "auto"
```

Options:

- `auto`: run the fixed schedule automatically
- `notify`: tell the operator when due
- `off`: do not schedule production retrains

### Multiple config files

You can run different PM profiles with separate config files:

```bash
python pm_main.py --trade --config config_conservative.json
python pm_main.py --trade --config config_aggressive.json
```

---

## 9. Dashboard

Run the dashboard from the repo root:

```bash
python -m pm_dashboard.app --pm-root "."
```

Defaults:

- host: `127.0.0.1`
- port: `8000`

The dashboard:

- reads PM outputs and config
- follows `pipeline.winner_ledger_path` when `pm_configs_path` is left at `auto`
- writes its own `pm_dashboard/dashboard_config.json`
- can optionally trigger root `data/` maintenance jobs through the API using locked atomic CSV refreshes

Keep the default `127.0.0.1` bind for local use. If you bind to `0.0.0.0`, set `PM_DASHBOARD_WRITE_TOKEN`; remote write APIs are blocked without it.

See [pm_dashboard/README.md](pm_dashboard/README.md) for the full dashboard guide.

---

## 10. Troubleshooting

### MT5 connection failure

1. Ensure MT5 is open
2. Ensure you are logged in
3. Ensure AutoTrading is enabled
4. If needed, set `mt5.path` explicitly

### Symbol not found or blocked

1. Confirm the exact broker symbol name in MT5
2. Ensure the symbol is visible in Market Watch
3. Ensure the symbol is tradable for the connected account

### No valid winners found

- verify historical data coverage
- inspect thresholds in `config.json`
- inspect `logs/pm_YYYYMMDD.log`

### Position skipped for risk reasons

- review `position.risk_per_trade_pct`
- review `position.max_risk_pct`
- review `pipeline.min_trade_risk_pct`
- review `pipeline.max_combined_risk_pct` only if you already have open trades on the same symbol; it is not a portfolio-wide cap
- review symbol-specific minimum lot / stop distance constraints

### Leftover MT5 position after restart

- The live loop now tries to recover an open position timeframe in this order:
  - `pipeline.position_timeframe_overrides`
  - live MT5 comment decode, including truncated `PM2` / `PM3` comments
  - legacy `PM_<tag>` strategy-tag match
  - encoded magic lookup
  - MT5 opening order/deal metadata via position identifier
- If the timeframe still cannot be resolved, the PM stays fail-closed and blocks the secondary trade on that symbol.
- Unknown leftovers are warning-throttled to one warning per open position per session.
- If your broker strips too much metadata, add a manual override in `config.json`:

```json
"pipeline": {
  "position_timeframe_overrides": {
    "ticket:123456789": "D1",
    "magic:987654321": "H4"
  }
}
```

### Debug mode

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

DEBUG mode is useful for:

- feature path issues
- regime selection issues
- sizing and stop-distance issues
- MT5 order request/response diagnosis
- symbol resolution and tradability checks

---

## 11. Quick Reference

### Common commands

```bash
pip install pandas numpy MetaTrader5 numba optuna
python pm_main.py --optimize
python pm_main.py --optimize --overwrite
python pm_main.py --status
python pm_main.py --trade --paper
python pm_main.py --trade
python -m pm_dashboard.app --pm-root "."
```

### Important files

| File | Purpose |
|---|---|
| `config.json` | Primary PM configuration |
| `pm_configs*.json` | Saved validated winners; active path comes from `pipeline.winner_ledger_path` |
| `regime_params.json` | Tuned regime parameters |
| `last_trade_log.json` | Decision throttle state |
| `last_actionable_log.json` | Latest actionable decision feed |
| `data/*.csv` | Historical bar cache |
| `logs/pm_*.log` | Runtime logs |
| `pm_outputs/optimization_summary.csv` | Optimization summary |
| `pm_outputs/trades_*.json` | Trade records and related artifacts |

### Next docs

- [README.md](README.md)
- [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md)
- [CHANGELOG.md](CHANGELOG.md)

### Emergency actions

Stop the PM:

```text
Ctrl+C
```

Reset decision-state files if needed:

```bash
del last_trade_log.json
del last_actionable_log.json
```

Force a clean reselection run:

```bash
del <configured-winner-ledger>
python pm_main.py --optimize
```

---

## Disclaimer

Trading involves substantial risk of loss. Always verify setup, data coverage, broker symbol mapping, and risk settings in paper mode before live deployment.
