# FX Portfolio Manager v3.1

FX Portfolio Manager is a production-focused automated trading system with regime-aware strategy selection, stateful optimization, Numba-accelerated backtesting, MetaTrader 5 execution, and a companion dashboard.

Archived audit and planning documents live in `documentation_archive/`.

---

## Documentation Notes

- Primary run/setup guide: `SETUP_AND_RUN.md`
- Settings reference: `SETTINGS_REFERENCE.md`
- Change history: `CHANGELOG.md` and `PATCH_NOTES.md`
- Dashboard guide: `pm_dashboard/README.md`
- Margin protection policy: `README.md` (Risk Management -> Margin Protection)
- Archived audit/planning docs: `documentation_archive/`
- Older historical analysis notes: `docs/archive/`

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
4. Uses the active recent M5 workflow for eligibility, optimization, and production selection
5. Persists optimization state incrementally and atomically
6. Trades only validated winners for the exact `(timeframe, regime)` live context
7. Stores historical-stress audit, no-trade, and risk-management evidence on artifacts
8. Uses MT5 contract math, broker metadata, margin protection, and spread-quality filters in live execution

The checked-in `config.json` is deployment-specific and currently reflects a high-risk low-balance profile. Treat it as the active repo configuration, not a universal recommendation.

---

## Current State

### Core facts

- Runtime banner: `FX PORTFOLIO MANAGER v3.1`
- Strategy pool: `47` registered strategies
- Active regime engine: score-based 4-regime detector in `pm_regime.py`
- Active data workflow: latest `300,000` M5 bars, with oldest `50,000` as `historical_stress_audit` and newest `250,000` as the active universe
- Live retrain model: calendar-anchored schedule from `config.json`
- Primary PM config source of truth: `config.json`
- Root log path: `logs/`
- Optimization/trade artifact path: `pm_outputs/`

### What is implemented

- Stateful optimization ledger with atomic config persistence
- Numba backtest path with same-bar gap-through-stop handling
- Regime-aware validation and winners-only live selection
- Validation-aware regime Optuna scoring and signal-bar regime bucketing
- Explicit `NO_TRADE` markers for evaluated no-winner regime slots
- Historical stress audit metrics on winner artifacts
- Mandatory per-winner risk-management/governance policy evidence
- Live margin-protection pass before governance and new entries
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
- The PM generates candle-close signals and the live loop is gated by MT5 bar availability rather than a wall-clock bar timer by default

---

## What Makes It Different

| Dimension | Typical Retail PM | Current FxPM |
|---|---|---|
| Strategy selection | one or a few fixed systems | 47-strategy competition across timeframes and regimes |
| Market adaptation | static logic | regime-aware winner selection |
| Data relevance | broad/stale history can dominate | newest 250k M5 active universe drives selection |
| Optimization persistence | restart from scratch on interruption | atomic stateful ledger |
| Live risk math | rough pip assumptions | MT5 contract-aware loss-at-stop sizing |
| Live execution quality | basic entry gating | broker checks plus spread-quality filtering |
| No-winner handling | implicit omission | explicit `NO_TRADE` artifact markers |
| Retrain workflow | ad hoc or rolling expiry only | calendar-anchored schedule in config |
| Monitoring | logs only | logs plus dashboard and actionable feed |

---

## Repository Hygiene

The repository now treats runtime artifacts as non-source outputs.

- Ignored by default (`.gitignore`): caches, logs, generated trade outputs, and downloaded market data files.
- Already tracked artifacts were removed from the git index with `git rm --cached` (local files remain on disk).

If a clone still tracks runtime files, run:

```bash
git rm -r --cached -f --ignore-unmatch __pycache__ pm_dashboard/__pycache__ tests/__pycache__ logs pm_outputs data/.cache data/*.csv last_trade_log.json last_actionable_log.json
```

Optional local cleanup of ignored artifacts (destructive for ignored files only):

```bash
git clean -fdX
```

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
- restart-safe open-position timeframe recovery using comments, magic, and MT5 opening metadata
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
|-- pm_configs*.json          # Saved validated winners; active path comes from pipeline.winner_ledger_path
|-- regime_params.json        # Tuned regime params (optional)
|-- last_trade_log.json       # Decision throttle state (auto-generated)
|-- last_actionable_log.json  # Last actionable decision feed (auto-generated)
|-- data/                     # Historical data cache
|-- logs/                     # Runtime logs
`-- pm_outputs/               # Optimization summaries and trade artifacts
```

### Optimization flow

1. `config.json` defines symbols, thresholds, schedule, and paths.
2. `pm_pipeline.py` loads the latest base M5 workflow window and derives higher timeframes from those timestamp boundaries.
3. The oldest 50,000 M5 bars are reserved for `historical_stress_audit`; the newest 250,000 M5 bars form the active universe.
4. Stage 1 baseline eligibility runs across the full active 250,000 M5-bar universe. Stage 2 optimization/risk selection uses the newest half of that active universe with warmup context retained.
5. `pm_regime.py` supplies causal regime labels; `pm_regime_tuner.py` can tune regime parameters.
6. `pm_strategies.py` evaluates the 47-strategy pool across configured timeframes.
7. `pm_optuna.py` tunes shortlisted candidates; regime search uses validation bucket evidence where available.
8. Validated winners, `NO_TRADE` markers, historical audit metrics, validation evidence, ledger-completion status, and governance/risk policy evidence are written atomically to the configured winner ledger (`pipeline.winner_ledger_path`; the shipped high-risk profile uses `pm_configs_high_risk.json`).

### Live trading flow

1. `pm_main.py` loads `config.json`, the configured winner ledger, and MT5 broker metadata.
2. The live loop is triggered by changed MT5 bar timestamps by default (`pipeline.live_loop_trigger_mode = "bar"`), then refreshes bars, computes features, and derives the active regime only when the broker/live data shows a new candle is available.
   It now does this through bounded timeframe-specific `.live/<symbol>_<TF>.csv` caches rather than rereading full canonical datasets on every sweep. If the local seed is too stale to bridge safely with a small delta, the PM re-seeds that cache from MT5 instead of carrying a discontinuous series forward.
3. Only validated winners for the exact `(timeframe, regime)` combination are considered.
4. Position sizing uses the configured `risk_basis` (`balance` or `equity`), broker contract math, spread-quality filters, broker lot constraints, and optional live risk scalars only when they are explicitly enabled in config.
5. Existing MT5 positions are reclassified through comment, magic, and opening-metadata recovery before same-symbol secondary-trade rules are applied.
6. Same-symbol exposure checks are based on live open-position geometry (`entry`, `SL`, `volume`) first and use comment metadata only as a fallback.
7. Orders are placed through `pm_mt5.py`, while decisions and outcomes are written to `logs/` and `pm_outputs/`.

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
- `pipeline.live_risk_scalars_enabled`
- `pipeline.target_annual_vol`
- `pipeline.storage_*`
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
    "optuna_family_size_aware_budget": true,
    "risk_management_optimization_enabled": true,
    "risk_management_selection_stage": "stage3",
    "production_retrain_mode": "notify",
    "production_retrain_interval_weeks": 2,
    "production_retrain_weekday": "sunday",
    "production_retrain_time": "00:01",
    "production_retrain_anchor_date": "2026-03-29",
    "winner_ledger_path": "pm_configs_high_risk.json",
    "local_governance_tournament_enabled": true,
    "local_governance_live_mode": "shadow",
    "live_risk_scalars_mode": "shadow",
    "target_annual_vol": 0.10,
    "execution_spread_filter_enabled": true,
    "execution_spread_min_edge_mult": 1.25,
    "execution_spread_spike_mult": 3.0,
    "execution_spread_penalty_start_mult": 0.75
  },
  "position": {
    "risk_per_trade_pct": 2.0,
    "risk_basis": "balance",
    "max_risk_pct": 3.0,
    "auto_widen_sl": true
  }
}
```

### Notes

- The committed config currently uses a high-risk low-balance profile.
- Stage 1/Stage 2 use the active recent M5 workflow: latest 300k M5 bars, oldest 50k as `historical_stress_audit`, newest 250k as the active optimization universe. Stage 1 runs baseline eligibility across the full active universe; Stage 2 uses the newest half as the fresh optimization/risk-selection surface.
- The current presets are the recommended baseline for this workflow. They are designed to favor quality, profitability, and live relevance; do not loosen them without comparing regenerated artifacts and live rejection/outcome data.
- Stage 2 freshness is intentional: the newest half of the active universe is used as the fresh selection surface, with warmup context retained, rather than adding a stale walk-forward layer between selection and live implementation.
- `risk_management_selection_stage = "stage3"` means governance/risk policies are selected on the newest Stage 2 selection surface. Setting it to `"stage2"` evaluates policies on the active optimization universe instead.
- `logs/` is not controlled by `pipeline.output_dir`; runtime logs always go to the root `logs/` folder.
- `position.risk_per_trade_pct` is the authoritative live target risk. `pipeline.risk_per_trade_pct` is the research/backtest value and only backfills the position value when the `position` key is absent.
- `live_risk_scalars_mode = "shadow"` records would-be risk-scalar changes but does not alter live sizing.
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
- Optimizer bucketing, backtest trade intent, regime TP multipliers, and live winner lookup prefer the decision-time `REGIME_LIVE` / `REGIME_STRENGTH_LIVE` surface; `REGIME` remains a legacy fallback only when shifted columns are unavailable.
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

The intended live precedence is:

- `position.risk_per_trade_pct` = live target risk percentage
- `position.max_risk_pct` = per-trade hard cap after sizing
- `pipeline.max_combined_risk_pct` = same-symbol combined cap, not a portfolio-wide cap, and the live runtime estimates existing open-position exposure from actual geometry before falling back to comment tags

`pipeline.risk_per_trade_pct` remains important for research and as a compatibility backfill, but it is not meant to silently override the live `position` section.

### Live protections

- Broker-aware min/max/step lot normalization
- Auto-widen stop support for broker stop-distance constraints
- Spread-quality overlay:
  - weak edge rejection via `execution_spread_min_edge_mult`
  - spike blocking via `execution_spread_spike_mult`
  - soft penalty via `execution_spread_penalty_start_mult`
- Margin protection:
  - immediate entry blocking below `margin_entry_block_level`
  - recovery/panic forced-close bands from `margin_recovery_start_level` and `margin_panic_level`
  - stateful reopening only after `margin_reopen_level` once margin stress or a forced close has occurred
  - missing/unparseable `margin_level` is neutral only when no margin is in use; otherwise new entries fail closed until account data is trustworthy
- Same-symbol dual-trade controls for `D1 + lower timeframe`
- Final duplicate-position protection uses the sweep snapshot first, then performs a fresh broker-side position read immediately before order send.

### Important execution caveat

This PM operates on candle data. The live loop polls small MT5 bar probes as the market-data availability gate, and signals are generated from candle bars and executed on the next actionable broker state, so live trading will always have some inter-candle execution gap relative to idealized historical evaluation.

---

## Live Readiness

Before deploying live:

1. Run a paper session on the same symbols and timeframes you intend to trade.
2. Confirm MT5 resolves and enables every intended broker symbol.
3. Verify the configured winner ledger is current and due-state is understood via `python pm_main.py --status`.
   The status view now separates raw validated configs from live-eligible configs; expired configs and artifact-drifted configs are blocked when `live_artifact_drift_policy = "block"`.
4. Review recent runtime logs in `logs/`.
5. Confirm spread filter thresholds and risk settings match the intended operating profile.
6. Confirm `pipeline.live_loop_trigger_mode` is `bar` for MT5 bar-gated live operation, or intentionally set to `scheduled` only if you want the legacy due-time fallback.
7. Confirm the production retrain mode in `config.json` is what you actually want: `auto`, `notify`, or `off`.

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

- the effective search path is bounded by `max_param_combos`, eligibility gates, regime candidate descent, and validation-aware regime Optuna scoring
- Stage 1 is config-based baseline eligibility over the full active universe; "Top-K-like" language here only means many candidates can survive the gate, not a Stage 2-style Top-K optimizer
- Stage 2 is the actual Top-K and algorithmic optimization layer
- risk-management/governance policy selection is attached to optimized winners, defaulting to the Stage 3 selection surface
- the historical stress audit is an older-window severe-fragility report, not the primary selector
- `optimization_max_workers` controls concurrency
- `scoring_mode = "fx_backtester"` remains the primary intended mode
- artifact invalidation compares only the semantic optimizer/backtester contract, so volatile ledger completion metadata does not make a freshly optimized config appear stale
- resample-cache telemetry records memory hits, disk hits, misses, invalidations, bytes, and read/write seconds so cache sizing can be judged by measured benefit. The active high-risk profile keeps a 4 GB cache quota; larger 5-10 GB quotas can be justified later if telemetry proves a quality-preserving efficiency gain.

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

Validation and ranking also incorporate robustness and train-to-validation gap behavior. Winner artifacts now persist compact `validation_evidence`, including selected robustness, return/drawdown efficiency, validation trade counts, validation reason, and optimizer gate/cache telemetry where available.

---

## Output Files

### Core artifacts

- `config.json`: primary PM configuration
- `pm_configs.json` / `pm_configs_high_risk.json`: validated winner configurations; the active file is selected by `pipeline.winner_ledger_path` and now carries validation evidence plus ledger-completion metadata on saved artifacts
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
|-- pm_configs*.json
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

- it follows `pipeline.winner_ledger_path` when the dashboard ledger path is left at `auto`
- it writes its own `pm_dashboard/dashboard_config.json`
- it can trigger historical-data refreshes that write to the PM root `data/` folder via locked atomic CSV refreshes

Keep the default loopback bind for normal use. If the dashboard is bound to `0.0.0.0`, configure `PM_DASHBOARD_WRITE_TOKEN`; remote write APIs are denied without a token.

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

### Leftover position after restart

- The PM now attempts timeframe recovery from:
  - `pipeline.position_timeframe_overrides`
  - live/truncated trade comments
  - magic lookup
  - MT5 opening order/deal metadata
- If recovery still fails, the symbol stays fail-closed and secondary trades remain blocked.
- Use `position_timeframe_overrides` with `ticket:<n>` or `magic:<n>` keys for broker-specific edge cases.

### Logging and diagnostics

```bash
python pm_main.py --trade --paper --log-level DEBUG
```

Useful places to inspect:

- `logs/pm_YYYYMMDD.log`
- `last_actionable_log.json`
- the active winner ledger from `pipeline.winner_ledger_path`
- `pm_outputs/optimization_summary.csv`

---

## Additional Docs

- [SETUP_AND_RUN.md](SETUP_AND_RUN.md)
- [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md)
- [CHANGELOG.md](CHANGELOG.md)
- [PATCH_NOTES.md](PATCH_NOTES.md)
- [pm_dashboard/README.md](pm_dashboard/README.md)
- [documentation_archive/README.md](documentation_archive/README.md)

---

## Versioning

- Runtime/application banner: `v3.1`
- Repository release log: `CHANGELOG.md`
- Current repository changelog entry: `1.4.8`

---

## Disclaimer

Trading forex, CFDs, crypto, commodities, and indices involves substantial risk of loss. Past performance is not indicative of future results. Test thoroughly in paper mode before any live deployment.
