# FX Portfolio Manager v3.1

FX Portfolio Manager is a production-focused automated trading system with regime-aware strategy selection, stateful optimization, Numba-accelerated backtesting, MetaTrader 5 execution, and a companion dashboard.

Archived audit and planning documents live in `documentation_archive/`.

---

## Table of Contents

- [Overview](#overview)
- [Current State](#current-state)
- [What Makes It Different](#what-makes-it-different)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Strategy Pool](#strategy-pool)
- [Regime Detection](#regime-detection)
- [Risk and Execution](#risk-and-execution)
- [Live Readiness](#live-readiness)
- [Performance Notes](#performance-notes)
- [Performance Metrics](#performance-metrics)
- [Output Files](#output-files)
- [Dashboard](#dashboard)
- [Troubleshooting](#troubleshooting)
- [Additional Docs](#additional-docs)
- [Versioning](#versioning)

---

## Overview

The current PM:

1. Detects one of 4 active market regimes: `TREND`, `RANGE`, `BREAKOUT`, `CHOP`
2. Evaluates a 47-strategy pool across configured timeframes
3. Tunes shortlisted candidates with grid search and optional Optuna
4. Applies validation and generalization checks before storing winners
5. Persists optimization state incrementally and atomically
6. Trades only validated winners for the exact `(timeframe, regime)` live context
7. Uses MT5 contract math, broker metadata, and spread-quality filters in live execution

The checked-in `config.json` is deployment-specific and currently reflects a lower-risk challenge profile. Treat it as the active repo configuration, not a universal recommendation.

---

## Current State

### Core facts

- Runtime banner: `FX PORTFOLIO MANAGER v3.1`
- Strategy pool: `47` registered strategies
- Active regime engine: score-based 4-regime detector in `pm_regime.py`
- Live retrain model: calendar-anchored schedule from `config.json`
- Primary PM config source of truth: `config.json`
- Root log path: `logs/`
- Optimization/trade artifact path: `pm_outputs/`

### What is implemented

- Stateful optimization ledger with atomic config persistence
- Numba backtest path with same-bar gap-through-stop handling
- Regime-aware validation and winners-only live selection
- MT5 symbol-resolution and order-preflight hardening
- Spread-aware live execution overlay driven by config
- 5 added strategies:
  - `VortexTrendStrategy`
  - `TRIXSignalStrategy`
  - `RelativeVigorIndexStrategy`
  - `VIDYABandTrendStrategy`
  - `ChoppinessCompressionBreakoutStrategy`
- Dashboard alignment pass across analytics, trades, watcher, jobs, parsers, templates, and static assets

### Important non-features

- The active regime detector is not HMM-based
- The full enhancement seam pack is not the default active backtest/live exit engine
- The PM still operates on candle-close decision timing, not true tick-by-tick execution

---

## What Makes It Different

| Dimension | Typical Retail PM | Current FxPM |
|---|---|---|
| Strategy selection | one or a few fixed systems | 47-strategy competition across timeframes and regimes |
| Market adaptation | static logic | regime-aware winner selection |
| Optimization persistence | restart from scratch on interruption | atomic stateful ledger |
| Live risk math | rough pip assumptions | MT5 contract-aware loss-at-stop sizing |
| Live execution quality | basic entry gating | broker checks plus spread-quality filtering |
| Retrain workflow | ad hoc or rolling expiry only | calendar-anchored schedule in config |
| Monitoring | logs only | logs plus dashboard and actionable feed |

---

## Key Features

### Stateful optimization

- incremental config persistence after each symbol
- atomic writes to avoid partial or corrupt config saves
- overwrite mode when a full reselection run is required

### Regime-aware selection

- 4 active regimes: `TREND`, `RANGE`, `BREAKOUT`, `CHOP`
- validated winners stored per `(timeframe, regime)`
- live trading restricted to validated winners only

### Broker-aware live execution

- MT5 symbol resolution and tradability checks
- `order_check()` / broker-preflight aware flow
- lot normalization against broker min/max/step constraints
- stop widening support where broker stop distance requires it

### Execution-quality controls

- spread spike blocker
- ATR-vs-spread minimum edge filter
- same-symbol dual-trade controls for `D1 + lower timeframe`
- decision throttling and actionable logging

### Research-path hardening

- same-bar gap-through-stop parity in Python and Numba backtest paths
- deterministic seeded optimization paths
- improved validation and regime-local rescue behavior

---

## Architecture

### Module overview

```text
FX_Portfolio_Manager/
|-- pm_core.py                # Configuration, feature computation, backtesting, scoring
|-- pm_strategies.py          # Strategy classes and shared indicator helpers
|-- pm_pipeline.py            # Optimization pipeline, ledger, portfolio orchestration
|-- pm_main.py                # CLI entrypoint and live trading loop
|-- pm_mt5.py                 # MT5 connection, broker metadata, order handling
|-- pm_position.py            # Position sizing and order-management helpers
|-- pm_regime.py              # Active score-based regime detector
|-- pm_regime_tuner.py        # Regime parameter optimization
|-- pm_optuna.py              # Optuna integration
|-- config.json               # Primary PM configuration
|-- pm_configs.json           # Saved validated winners (auto-generated)
|-- regime_params.json        # Tuned regime params (optional)
|-- last_trade_log.json       # Decision throttle state (auto-generated)
|-- last_actionable_log.json  # Last actionable decision feed (auto-generated)
|-- data/                     # Historical data cache
|-- logs/                     # Runtime logs
`-- pm_outputs/               # Optimization summaries and trade artifacts
```

### Optimization flow

1. `config.json` defines symbols, thresholds, schedule, and paths.
2. `pm_pipeline.py` loads historical data and runs the optimization pipeline.
3. `pm_regime.py` supplies causal regime labels; `pm_regime_tuner.py` can tune regime parameters.
4. `pm_strategies.py` evaluates the 47-strategy pool across configured timeframes.
5. `pm_optuna.py` optionally tunes shortlisted candidates.
6. Validation and generalization checks are applied before results are written atomically to `pm_configs.json`.

### Live trading flow

1. `pm_main.py` loads `config.json`, `pm_configs.json`, and MT5 broker metadata.
2. The live loop refreshes bars, computes features, and derives the active regime.
3. Only validated winners for the exact `(timeframe, regime)` combination are considered.
4. Position sizing uses the configured `risk_basis` (`balance` or `equity`), broker contract math, spread-quality filters, and broker lot constraints.
5. Orders are placed through `pm_mt5.py`, while decisions and outcomes are written to `logs/` and `pm_outputs/`.

---

## Quick Start

### 1. Install dependencies

```bash
pip install pandas numpy MetaTrader5
pip install numba optuna
```

### 2. Review `config.json`

Minimum keys to understand before first run:

- `pipeline.data_dir`
- `pipeline.output_dir`
- `pipeline.risk_per_trade_pct`
- `pipeline.production_retrain_*`
- `pipeline.execution_spread_*`
- `position.risk_per_trade_pct`
- `position.risk_basis`
- `position.max_risk_pct`
- `symbols`

### 3. Optimize

```bash
python pm_main.py --optimize
```

Force a full re-optimization:

```bash
python pm_main.py --optimize --overwrite
```

### 4. Check status

```bash
python pm_main.py --status
```

### 5. Paper trade

```bash
python pm_main.py --trade --paper
```

### 6. Live trade

```bash
python pm_main.py --trade
```

---

## Configuration

`config.json` is the primary PM source of truth. The code still contains defensive defaults, but the intended operating model is config-first propagation.

### Key pipeline controls

```json
{
  "pipeline": {
    "data_dir": "./data",
    "output_dir": "./pm_outputs",
    "initial_capital": 10000.0,
    "risk_per_trade_pct": 0.50,
    "scoring_mode": "fx_backtester",
    "timeframes": ["M5", "M15", "M30", "H1", "H4", "D1"],
    "production_retrain_mode": "notify",
    "production_retrain_interval_weeks": 2,
    "production_retrain_weekday": "sunday",
    "production_retrain_time": "00:01",
    "production_retrain_anchor_date": "2026-03-29",
    "execution_spread_filter_enabled": true,
    "execution_spread_min_edge_mult": 1.5,
    "execution_spread_spike_mult": 3.0,
    "execution_spread_penalty_start_mult": 0.5
  },
  "position": {
    "risk_per_trade_pct": 0.50,
    "risk_basis": "equity",
    "max_risk_pct": 1.0,
    "auto_widen_sl": true
  }
}
```

### Notes

- The committed config currently uses a challenge-style lower-risk profile.
- `logs/` is not controlled by `pipeline.output_dir`; runtime logs always go to the root `logs/` folder.
- `pipeline.production_retrain_mode` supports `auto`, `notify`, and `off`.
- `pipeline.execution_spread_spike_mult` is the live spread-spike blocker threshold.

For full key-by-key details, see [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md).

---

## Strategy Pool

The registry currently groups the 47 strategies into 3 operational families:

### Trend (`18`)

`EMACrossoverStrategy`, `SupertrendStrategy`, `MACDTrendStrategy`, `ADXTrendStrategy`, `IchimokuStrategy`, `HullMATrendStrategy`, `EMARibbonADXStrategy`, `AroonTrendStrategy`, `ADXDIStrengthStrategy`, `KeltnerPullbackStrategy`, `OBVDivergenceStrategy`, `EMAPullbackContinuationStrategy`, `ParabolicSARTrendStrategy`, `KaufmanAMATrendStrategy`, `VortexTrendStrategy`, `TRIXSignalStrategy`, `RelativeVigorIndexStrategy`, `VIDYABandTrendStrategy`

### Mean Reversion (`17`)

`RSIExtremesStrategy`, `BollingerBounceStrategy`, `ZScoreMRStrategy`, `StochasticReversalStrategy`, `CCIReversalStrategy`, `WilliamsRStrategy`, `RSITrendFilteredMRStrategy`, `StochRSITrendGateStrategy`, `FisherTransformMRStrategy`, `ZScoreVWAPReversionStrategy`, `TurtleSoupReversalStrategy`, `PinBarReversalStrategy`, `EngulfingPatternStrategy`, `RSIDivergenceStrategy`, `MACDDivergenceStrategy`, `KeltnerFadeStrategy`, `ROCExhaustionReversalStrategy`

### Breakout (`12`)

`DonchianBreakoutStrategy`, `VolatilityBreakoutStrategy`, `MomentumBurstStrategy`, `SqueezeBreakoutStrategy`, `KeltnerBreakoutStrategy`, `PivotBreakoutStrategy`, `MACDHistogramMomentumStrategy`, `InsideBarBreakoutStrategy`, `NarrowRangeBreakoutStrategy`, `VolumeSpikeMomentumStrategy`, `ATRPercentileBreakoutStrategy`, `ChoppinessCompressionBreakoutStrategy`

Live trading uses validated winners only. There is no production fallback to an unvalidated "best available" strategy.

---

## Regime Detection

### Active regimes

- `TREND`
- `RANGE`
- `BREAKOUT`
- `CHOP`

### Active methodology

- Score-based regime detection in `pm_regime.py`
- Hysteresis and causal shifting for live-decision parity
- Tunable thresholds via `regime_params.json`
- No HMM in the active live code path

### Regime tuning

Optional regime parameter optimization:

```bash
python pm_regime_tuner.py --data-dir ./data --output regime_params.json
```

---

## Risk and Execution

### Position sizing

Live sizing uses either balance or equity, depending on `position.risk_basis`:

```text
risk_source = balance if risk_basis == "balance" else equity
risk_amount = risk_source * (risk_per_trade_pct / 100)
loss_per_lot = MT5 order_calc_profit() at stop-loss distance
volume = normalized broker-valid lot size
```

### Live protections

- Broker-aware min/max/step lot normalization
- Auto-widen stop support for broker stop-distance constraints
- Spread-quality overlay:
  - weak edge rejection via `execution_spread_min_edge_mult`
  - spike blocking via `execution_spread_spike_mult`
  - soft penalty via `execution_spread_penalty_start_mult`
- Same-symbol dual-trade controls for `D1 + lower timeframe`

### Important execution caveat

This PM still operates on candle data. Signals are generated on the signal bar and executed on the next actionable bar, so live trading will always have some inter-candle execution gap relative to idealized historical evaluation.

---

## Live Readiness

Before deploying live:

1. Run a paper session on the same symbols and timeframes you intend to trade.
2. Confirm MT5 resolves and enables every intended broker symbol.
3. Verify `pm_configs.json` is current and due-state is understood via `python pm_main.py --status`.
4. Review recent runtime logs in `logs/`.
5. Confirm spread filter thresholds and risk settings match the intended operating profile.
6. Confirm the production retrain mode in `config.json` is what you actually want: `auto`, `notify`, or `off`.

---

## Performance Notes

### Backtest path

- Numba remains the accelerated primary path when available.
- The upgraded backtester preserves same-bar gap-through-stop behavior and ordering semantics.
- The PM still favors correctness first and performance second when the two conflict.

### Feature computation

- `FeatureComputer.compute_required(...)` exists for targeted feature computation.
- The helper layer has been hardened to reduce pandas fragmentation and noisy warning behavior.
- The codebase now contains both full-feature and targeted-feature pathways; use the targeted path where parity is preserved.

### Optimization profile

- the effective search path is bounded by `max_param_combos`, shortlist gates, and top-K validation
- `optimization_max_workers` controls concurrency
- `scoring_mode = "fx_backtester"` remains the primary intended mode

---

## Performance Metrics

The PM and dashboard primarily work with:

- Sharpe ratio
- Sortino ratio
- Profit factor
- Win rate
- Total return
- Max drawdown
- Calmar ratio
- Expectancy
- Drawdown duration
- Recovery time
- Ulcer index

Validation and ranking also incorporate robustness and train-to-validation gap behavior.

---

## Output Files

### Core artifacts

- `config.json`: primary PM configuration
- `pm_configs.json`: validated winner configurations
- `regime_params.json`: optional tuned regime parameters
- `last_trade_log.json`: throttle state
- `last_actionable_log.json`: current actionable decision feed
- `data/*.csv`: historical bar cache
- `logs/pm_YYYYMMDD.log`: runtime logs
- `pm_outputs/optimization_summary.csv`: optimization summary
- `pm_outputs/trades_*.json`: trade records and related artifacts

### Directory structure

```text
FX_Portfolio_Manager/
|-- config.json
|-- pm_configs.json
|-- regime_params.json
|-- last_trade_log.json
|-- last_actionable_log.json
|-- data/
|-- logs/
|   `-- pm_YYYYMMDD.log
`-- pm_outputs/
    |-- optimization_summary.csv
    `-- trades_*.json
```

---

## Dashboard

The companion dashboard lives under `pm_dashboard/`.

It provides:

- Signal Desk
- Strategy browser
- Analytics and simulation
- Trade history
- Optional root `data/` refresh jobs through the dashboard API

It is read-mostly, not fully read-only:

- it writes its own `pm_dashboard/dashboard_config.json`
- it can trigger historical-data refreshes that write to the PM root `data/` folder

See [pm_dashboard/README.md](pm_dashboard/README.md) for details.

---

## Troubleshooting

### MT5 connection issues

- Ensure MT5 is running and logged in
- Ensure AutoTrading is enabled
- If needed, set an explicit `mt5.path`

### Symbol resolution issues

- Use the exact broker symbol if your broker suffixes names
- Ensure the symbol is visible and tradable in MT5

### No valid winners

- Check data coverage in `data/`
- Review validation thresholds in `config.json`
- Inspect `logs/pm_YYYYMMDD.log`

### Logging and diagnostics

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

Useful places to inspect:

- `logs/pm_YYYYMMDD.log`
- `last_actionable_log.json`
- `pm_configs.json`
- `pm_outputs/optimization_summary.csv`

---

## Additional Docs

- [SETUP_AND_RUN.md](SETUP_AND_RUN.md)
- [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md)
- [CHANGELOG.md](CHANGELOG.md)
- [pm_dashboard/README.md](pm_dashboard/README.md)

---

## Versioning

- Runtime/application banner: `v3.1`
- Repository release log: `CHANGELOG.md`
- Current repository changelog entry: `1.4.5`

---

## Disclaimer

Trading forex, CFDs, crypto, commodities, and indices involves substantial risk of loss. Past performance is not indicative of future results. Test thoroughly in paper mode before any live deployment.
