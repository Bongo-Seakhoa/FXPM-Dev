# FxPM Practical Workflow Operating Model

Snapshot date: 2026-04-29

This document explains how the current FxPM system functions as an operating workflow. It is not a full audit and it is not a setup guide. Its purpose is to make the practical machine visible:

- what enters the system
- what transforms those inputs
- what outputs are produced
- what outcomes the system is meant to create
- where the actual live/research behavior can deviate from the expected narrative

Use this as a working reference when asking: "Is FxPM doing what I think it is doing?"

---

## 1. Operating Thesis

FxPM is a regime-aware strategy discovery and live execution system. It does not train one fixed model. It runs a structured competition between many strategy candidates, selects validated winners per market context, stores those winners in a stateful ledger, and then trades only the winner that matches the current live symbol, timeframe, and regime.

The practical goal is:

> Select and execute the strategy package most likely to be profitable, robust, current-market relevant, broker-executable, and operationally understandable.

The hard measurable outcomes include profitability, drawdown control, trade count, robustness, live eligibility, broker feasibility, and actual live decisions. The internal quality outcomes include fairness of the candidate tournament, explainability of why trades did or did not happen, controlled retrain cadence, and operator confidence that live behavior matches the intended research surface.

The current system is best understood as four coupled loops:

```text
1. Discovery loop
   config + M5 data + strategies + regimes
   -> features/backtests/scoring/validation
   -> winner ledger

2. Live execution loop
   winner ledger + MT5 bars + broker/account state
   -> live candidate selection + risk/execution gates
   -> order, paper action, or no-action reason

3. State and retrain loop
   ledger expiry + artifact contract + retrain schedule
   -> optimize/skip/warn/block decisions
   -> refreshed or stale winner universe

4. Observability loop
   logs + status + signal ledger + dashboard + storage state
   -> operator understanding
   -> deviation detection
```

Primary code anchors:

- CLI and app startup: `pm_main.py:5380`, `pm_main.py:5461`
- optimization pipeline: `pm_pipeline.py:2907`, `pm_pipeline.py:3426`
- active data split: `pm_core.py:3647`
- backtester and scoring: `pm_core.py:3737`, `pm_core.py:4796`
- live loop: `pm_main.py:5083`, `pm_main.py:5146`, `pm_main.py:5202`
- live eligibility: `pm_pipeline.py:4064`, `pm_pipeline.py:4099`
- storage and signal ledger: `pm_storage.py:133`, `pm_storage.py:528`

---

## 2. Current Local Profile Snapshot

These values are read from the local `config.json` at the time this document was created. They matter because the live runtime posture can differ from older documentation or from the pushed baseline.

| Area | Current local value | Meaning |
|---|---:|---|
| Symbols | `62` | Configured trade universe. |
| Data workflow | `active_recent_m5` | Production uses the active recent M5 workflow. |
| Scoring mode | `fx_backtester` | Active scoring uses the FX backtester-oriented scorer, not the legacy weighted scorer. |
| Max base bars | `300000` | Latest M5 bars loaded per symbol. |
| Historical audit | `50000` | Oldest slice inside the workflow window, used as stress/audit rather than primary selector. |
| Active universe | `250000` | Newest M5 bars that drive active strategy selection. |
| Stage 2 percentage | `50.0` | Newest half of active universe is used for fresh Stage 2/selection surface. |
| Winner ledger | `pm_configs_high_risk.json` | Active validated-winner ledger. |
| Retrain mode | `notify` | Due retrain logs a warning but does not auto-run optimization. |
| Retrain schedule | every 2 weeks Sunday `00:01` | Calendar-anchored production cycle. |
| Live artifact drift | `warn` | Drift warns in live eligibility but does not block live trading under current local profile. |
| Live loop trigger | `bar` | MT5 bar timestamp changes trigger live checks. |
| Governance live mode | `on` | Selected local governance can actively modify open-trade SL/TP. |
| Risk-management selection stage | `stage3` | Governance/risk policy selected after strategy winner discovery. |
| Walk-forward audit | `false` | Walk-forward audit is not an active promotion gate. |
| Regime TP multipliers | all `1.0` | Live overlay defaults neutral; discovery disables the overlay. |
| Live risk target | `position.risk_per_trade_pct = 0.5` | Position config is the authoritative live risk source. |
| Live hard risk cap | `position.max_risk_pct = 0.8` | Entries above cap block. |
| Risk tolerance warning | `position.risk_tolerance_pct = 8.0` | Lot-normalization drift tolerance before warning/block classification. |

Deviation note: some root docs still describe `live_artifact_drift_policy = block` and `local_governance_live_mode = shadow` as the active profile. The local config currently says `warn` and `on` (`config.json:79`, `config.json:114`). Treat this as a real narrative-vs-runtime checkpoint.

---

## 3. End Outcome Definition

FxPM has two kinds of outcomes.

### Hard Outcomes

These are physically measurable:

| Outcome | Measured by | Where to inspect |
|---|---|---|
| Profitability | `total_return_pct`, `total_pnl`, `profit_factor`, `expectancy_pips` | backtest metrics in winner ledger and `pm_core.py:4538` |
| Drawdown control | `max_drawdown_pct`, `return_dd_ratio`, `calmar_ratio` | validation evidence and backtest metrics |
| Risk-adjusted quality | `sharpe_ratio`, `sortino_ratio`, `mean_r`, `worst_5pct_r` | backtest metrics and scorer |
| Robustness | `robustness_ratio`, train/validation gap, stability penalty | `validation_evidence`, `pm_core.py:4934` |
| Live tradability | `Live eligible`, exact regime/timeframe winner, not expired, not `NO_TRADE` | `python pm_main.py --status`, `pm_pipeline.py:4099` |
| Broker feasibility | symbol visible/tradeable, valid tick, valid stops, margin, min lot, order check | live logs and actionable ledger |
| Execution action | order placed, paper action, skipped, blocked, no actionable setup | `last_actionable_log.json`, `pm_outputs/signal_ledger_YYYYMM.jsonl` |

### Internal Quality Outcomes

These are not just financial, but they decide whether the system is trustworthy:

| Objective | Practical meaning |
|---|---|
| Fair discovery | Stage 1 starts from neutral baseline defaults. Live-only TP overlays are not part of winner discovery. |
| Current-market relevance | Newest active universe drives selection; older data is audit/stress, not the main selector. |
| Explainable no-trade | No action should have a reason: no winner, no signal, spread, margin, risk, symbol, stale bar, expiry, etc. |
| Statefulness | Optimization can resume without losing prior symbols; configs are saved atomically. |
| Fail-closed safety | Missing account/position/tick/margin information blocks or skips entries rather than trading blind. |
| Operator control | Retrain mode, artifact drift policy, governance mode, storage policy, and risk posture are config-driven. |
| Live/research alignment | The live strategy, timeframe, regime, stops, and risk surfaces should trace back to the selected artifact. |

---

## 4. Whole System Input -> Transform -> Output Map

```text
Input Layer
  config.json
  instrument specs / broker specs
  data/{SYMBOL}_M5.csv
  regime_params.json or defaults
  strategy registry
  MT5 account, broker symbols, ticks, positions, bars
  winner ledger from previous optimization

Discovery Transform
  load latest bounded M5
  resample configured timeframes
  compute features and causal regimes
  Stage 1 baseline screening
  Stage 2 parameter optimization
  Stage 3 governance/risk policy selection
  validation and artifact fingerprinting

Discovery Output
  pm_configs_high_risk.json
  validation evidence
  historical audit evidence
  NO_TRADE markers
  governance policy evidence
  artifact metadata and expiry

Live Transform
  load live-eligible configs
  poll MT5 changed bars
  compute current live regime and signal on closed bar
  require exact winner for symbol/timeframe/regime
  build TradeIntent
  apply live gates: spread, risk, margin, broker stops, same-symbol exposure
  send order or record no-action/block reason

Live Output
  MT5 order / paper order / no action
  last_trade_log.json
  last_actionable_log.json
  pm_outputs/signal_ledger_YYYYMM.jsonl
  logs/pm_YYYYMMDD.log
  dashboard/status surfaces
```

The key operational rule is that a validated research winner is not automatically a live trade. It must survive live eligibility, exact live context matching, signal generation, risk gates, broker gates, spread gates, margin gates, and final quote refresh.

---

## 5. Pipeline A: Configuration And Startup

### Inputs

- `config.json`
- optional CLI overrides:
  - `--config`
  - `--symbols`
  - `--data-dir`
  - `--output-dir`
  - `--winner-ledger`
  - `--optimize`
  - `--trade`
  - `--paper`
  - `--status`
- instrument specs and broker specs
- MT5 login/session settings

### Transform

`pm_main.py` parses CLI arguments, loads JSON, normalizes the storage section into `PipelineConfig`, builds `PipelineConfig`, `PositionConfig`, and `MT5Config`, applies position-risk precedence, loads broker/instrument specs, and creates `FXPortfolioManagerApp`.

Important anchors:

- CLI parse: `pm_main.py:5380`
- config load and dataclass construction: `pm_main.py:5461`, `pm_main.py:5481`
- position risk backfill from pipeline only if position risk is absent: `pm_main.py:5486`
- winner ledger override: `pm_main.py:5497`
- app creation: `pm_main.py:5529`

### Outputs

- resolved runtime config summary in logs
- initialized app
- initialized `PortfolioManager`
- connected MT5 session if running live/trade path

### Deviation Checkpoints

| Expected narrative | Actual checkpoint |
|---|---|
| "Pipeline risk controls live risk." | Live risk is primarily `PositionConfig`; pipeline risk only backfills if position risk is missing. |
| "Docs describe active profile." | Local config can drift from docs. Check `config.json` directly. |
| "Changing config always changes live immediately." | Some changes only matter after optimization because they affect artifact contract/winner discovery. |
| "MT5 credentials are in config." | `login=0` can use current MT5 session. Do not assume credentials are embedded. |

---

## 6. Pipeline B: Market Data, Features, And Regime State

### Inputs

- canonical `data/{SYMBOL}_M5.csv`
- live MT5 bars
- data cache under `data/.cache`
- live cache under `data/.live/{SYMBOL}_{TF}.csv`
- configured timeframes
- regime params file or defaults

### Discovery Transform

Optimization loads the latest bounded M5 data and derives all configured higher timeframes from the same source. This matters because the M5 source creates one coherent calendar window instead of independently sampling each timeframe.

Active data workflow:

```text
latest 300000 M5 bars
  oldest 50000 -> historical_stress_audit
  newest 250000 -> active_universe
      newest 50% -> Stage 2 / fresh selection surface
      overlap -> warmup only, not scored
```

Code anchors:

- data loading: `pm_core.py:2274`
- resampling/cache: `pm_core.py:2430`, `pm_core.py:2478`
- active workflow split: `pm_core.py:3647`
- workflow metadata: `pm_pipeline.py:2962`
- symbol/timeframe data load: `pm_pipeline.py:3005`

### Regime Transform

The regime engine writes causal regime columns and shifted live-decision columns:

- `REGIME`
- `REGIME_STRENGTH`
- `REGIME_LIVE`
- `REGIME_STRENGTH_LIVE`

The live path prefers `REGIME_LIVE` so it does not choose using information from the current unfinished bar.

Code anchors:

- regime feature generation: `pm_regime.py:547`
- shifted live-decision columns: `pm_regime.py:743`
- hysteresis behavior: `pm_regime.py:1252`
- feature computation: `pm_core.py:3543`

### Live Transform

Live trading uses bounded local-first caches where possible, repairs gaps with MT5 reseed, and falls back to direct MT5 bars when local data is insufficient.

Code anchors:

- live cache path: `pm_main.py:888`
- local-to-live cache sync: `pm_main.py:968`
- MT5 reseed/repair path: `pm_main.py:1081`
- final bar fetch: `pm_main.py:1205`

### Outputs

- feature-enriched dataframes for discovery
- cached resampled data
- bounded live cache files
- regime fallback logs when tuned regime params are missing

### Deviation Checkpoints

| Expected narrative | Actual checkpoint |
|---|---|
| "All timeframes have independent datasets." | Higher timeframes are derived from the bounded M5 source in active workflow. |
| "Old data participates equally in selection." | Oldest 50k M5 bars are audit/stress; newest active universe drives selection. |
| "Live evaluates every symbol every timer tick." | Bar mode evaluates only changed symbol/timeframe branches. |
| "Regime params are always tuned." | Missing `(symbol,timeframe)` regime params fall back to timeframe/default params and log a fallback. |

---

## 7. Pipeline C: Discovery And Optimization

### Purpose

Discovery answers:

> Which strategy package should be eligible to trade each symbol, timeframe, and regime?

The output is not a trade. The output is a validated winner artifact.

### Inputs

- resolved `PipelineConfig`
- latest M5-derived timeframe data
- 47-strategy registry
- neutral Stage 1 defaults
- global parameter grids and optional Optuna
- regime labels
- instrument specs
- existing winner ledger for skip/resume

### High-Level Sequence

```text
PortfolioManager.initial_optimization()
  -> decide optimize vs skip using ledger validity and artifact contract
  -> OptimizationPipeline.run_for_symbol(symbol)
      -> load M5 and resample timeframes
      -> compute features and regimes
      -> create active/audit/stage2 windows
      -> RegimeOptimizer.optimize_symbol()
          -> Stage 1 baseline screening
          -> Stage 2 parameter optimization
          -> final validation
          -> NO_TRADE markers where no candidate survives
      -> attach historical stress audit
      -> attach governance/risk policy evidence
      -> build SymbolConfig
  -> save each symbol atomically to winner ledger
```

Code anchors:

- `PortfolioManager.initial_optimization()`: `pm_pipeline.py:3927`
- optimize/skip partition: `pm_pipeline.py:3947`
- pipeline symbol run: `pm_pipeline.py:3426`
- regime optimizer entry: `pm_pipeline.py:1698`
- atomic save: `pm_pipeline.py:354`, `pm_pipeline.py:3844`

### Stage 1: Baseline Eligibility

Stage 1 is a viability filter, not the final judge. It uses default strategy parameters over the full active universe to decide which baseline strategies are worth deeper search.

Recent correction: Stage 1 exit defaults are neutral `2.0 / 2.0`, and the minimum stop/target floor is symmetric `5.0 / 5.0`. That means Stage 1 is no longer advantaging a wider default TP or a hidden larger TP floor.

Code anchors:

- Stage 1 screening: `pm_pipeline.py:1939`, `pm_pipeline.py:1987`
- regime-local rescue: `pm_pipeline.py:1825`
- neutral exit defaults: `pm_strategies.py:577`
- symmetric floor: `pm_strategies.py:579`

Stage 1 answers:

- did the strategy generate enough trades?
- was drawdown survivable?
- did it show enough baseline viability?
- should it enter the expensive search?

Stage 1 does not answer:

- is this the final best parameter set?
- is this the final live strategy package?
- is this ready to trade without Stage 2/validation?

### Stage 2: Parameter Search And Winner Discovery

Stage 2 searches the strategy parameter surface for candidates that are stronger than the baseline. The active config uses Top-K per regime, a max combination budget, and Optuna family-size-aware budgeting.

Code anchors:

- Stage 2 tuning: `pm_pipeline.py:2004`, `pm_pipeline.py:2084`
- Optuna objective: `pm_optuna.py:607`
- global SL/TP grids and strategy registry: `pm_strategies.py:91`, `pm_strategies.py:3832`

Stage 2 answers:

- which parameterized strategy is strongest for this symbol/timeframe/regime?
- does the optimized candidate pass validation gates?
- should the regime receive a tradeable winner or an explicit `NO_TRADE` marker?

### Stage 3: Governance / Risk-Management Selection

After a strategy winner exists, the pipeline can select a local order-governance/risk-management policy. With `risk_management_selection_stage = "stage3"`, the current active workflow uses the freshest tail surface copied into `stage3_features_by_tf`.

This is a deviation checkpoint: Stage 3 is a post-winner governance selection step, but in active mode it is not a fully independent third holdout universe.

Code anchors:

- Stage 3 feature surface: `pm_pipeline.py:3501`
- governance selection stage routing: `pm_pipeline.py:3548`
- governance policy implementation: `pm_order_governance.py:184`

### Backtest And Scoring Semantics

Backtester behavior:

- previous closed-bar signal
- next-bar open entry
- spread/slippage/commission modeling
- broker-aware tick/pip sizing
- Numba-compatible semantics
- live-only regime TP overlay disabled during discovery

Code anchors:

- backtester entry: `pm_core.py:3737`
- signal validation and backtest loop: `pm_core.py:3773`
- `build_trade_intent(..., apply_regime_tp_multiplier=False)`: `pm_core.py:3891`
- metrics output: `pm_core.py:4538`
- FX scorer: `pm_core.py:4796`, `pm_core.py:4934`, `pm_core.py:4941`

Primary metrics:

- `total_pnl`
- `total_return_pct`
- `profit_factor`
- `max_drawdown_pct`
- `expectancy_pips`
- `sharpe_ratio`
- `sortino_ratio`
- `calmar_ratio`
- `mean_r`
- `worst_5pct_r`
- streak and duration metrics

### Validation Gates

The active regime winner path applies gates around:

- minimum train trades
- minimum validation trades
- validation drawdown
- train drawdown
- return/drawdown ratio
- profit factor
- return floor
- DSR-adjusted Sharpe scoring
- robustness ratio
- weak-train exceptional validation rescue

Code anchors:

- candidate gates: `pm_pipeline.py:2420`, `pm_pipeline.py:2438`, `pm_pipeline.py:2452`, `pm_pipeline.py:2460`, `pm_pipeline.py:2493`
- final validation: `pm_pipeline.py:2598`, `pm_pipeline.py:2640`, `pm_pipeline.py:2655`, `pm_pipeline.py:2698`, `pm_pipeline.py:2708`, `pm_pipeline.py:2719`
- persisted validation evidence: `pm_pipeline.py:2737`

### Discovery Outputs

Each saved winner artifact can include:

- symbol-level config
- regime configs per timeframe
- default winner
- explicit `NO_TRADE` markers
- metrics
- validation evidence
- data window metadata
- historical stress audit
- governance policy
- live observability
- artifact contract metadata
- expiry / `valid_until`

Primary file:

- `pm_configs_high_risk.json` unless overridden by `winner_ledger_path`

Additional output:

- `pm_outputs/optimization_summary.csv`

### Discovery Deviation Checkpoints

| Expected narrative | Actual checkpoint |
|---|---|
| "Stage 1 is Top-K." | Stage 1 is baseline eligibility; Top-K belongs primarily to Stage 2. |
| "Stage 2 is a classical independent validation split." | Active workflow is freshness-weighted; Stage 2 uses newest subset of the active universe. |
| "Stage 3 is an independent final holdout." | In active mode, Stage 3 governance uses the fresh selection surface copy, not a separate holdout. |
| "use_regime_optimization can disable regime path." | Production always instantiates `RegimeOptimizer`; legacy selector classes remain but are not the production path. |
| "A configured symbol means tradeable." | A symbol may be validated but contain only `NO_TRADE` markers or no tradeable regime winner. |
| "Discovery uses live regime TP multipliers." | Discovery disables live-only regime TP overlay. |
| "Historical stress audit selects winners." | It attaches audit evidence; it is not the primary selector. |

---

## 8. Pipeline D: Winner Ledger, Artifact Drift, And Retrain Lifecycle

### Purpose

This pipeline decides whether saved winners are still valid enough to use, whether optimization should run again, and whether live trading is allowed to use a saved artifact.

### Inputs

- active winner ledger
- current artifact contract
- `valid_until`
- production retrain schedule
- current config and regime param file metadata

### Validity Rules

A raw config is valid only if:

- it exists in the ledger
- `is_validated = true`
- `valid_until` exists
- current time is before `valid_until`

Code anchor:

- `has_valid_config()`: `pm_pipeline.py:423`

Optimization is needed when:

- overwrite is enabled
- config is missing
- config is invalid
- config is expired
- artifact fingerprint changed
- regime parameter version changed

Code anchor:

- `should_optimize()`: `pm_pipeline.py:455`

### Artifact Contract

`build_artifact_meta()` fingerprints the semantic contract behind winners. This includes:

- pipeline artifact version
- feature cache version
- split contract
- workflow mode and bar counts
- scoring mode
- retrain schedule
- governance/risk settings
- risk-management selection stage
- exit-surface contract
- regime params fingerprint
- strategy surface hash

Code anchors:

- artifact meta: `pm_pipeline.py:136`
- contract match ignoring volatile metadata: `pm_pipeline.py:184`

Practical meaning:

> If the system's research/execution contract changes, old winners may be due for optimization even if their date has not expired.

### Retrain Schedule

The current profile is `production_retrain_mode = notify`. This means:

- due retrain logs a warning
- it does not block the PM by itself
- it does not auto-run optimization
- it tells the operator to run `python pm_main.py --optimize --config config.json`

If mode is `auto`, the app fetches data, retrains needed symbols, invalidates runtime caches, and marks storage optimization completion.

Code anchors:

- schedule math: `pm_core.py:1191`
- retrain check: `pm_main.py:4964`
- notify branch: `pm_main.py:4980`
- auto branch: `pm_main.py:4991`

### Live Eligibility

Live eligibility is stricter than raw validation. It blocks or excludes configs when:

- not validated
- missing expiry
- expired
- no tradeable regime winners
- default/symbol config is `NO_TRADE`
- artifact drift exists and policy is `block`

Code anchors:

- live eligibility reason: `pm_pipeline.py:4064`
- live eligibility collector: `pm_pipeline.py:4099`

Policy behavior:

| Policy | Live behavior |
|---|---|
| `block` | Artifact drift blocks live eligibility. |
| `warn` | Artifact drift logs a live warning but allows trading. |
| `ignore` | Artifact drift live gate is suppressed. |

Current local profile is `warn`, so artifact drift is advisory for live eligibility, but it still makes symbols due for optimization.

### Advisory Versus Blocking

Advisory examples:

- retrain due when `production_retrain_mode = notify`
- artifact drift when `live_artifact_drift_policy = warn`
- storage pressure unless `storage_pause_entries_below_free_gb` is set
- storage cleanup when `storage_observe_only = true`
- daily/session loss advisory logs
- live drift monitor warnings
- lot-normalization drift that stays under hard cap

Blocking examples:

- expired or invalid config
- no live-eligible configs and no open positions
- artifact drift when policy is `block`
- missing account/position snapshot
- margin block/reopen cooldown
- spread filter
- invalid tick/stops/loss calculation
- actual risk above hard cap
- insufficient free margin
- broker order preflight failure

### State Outputs

- `pm_configs_high_risk.json`
- `pm_outputs/storage_state.json`
- `pm_outputs/storage_manifest.jsonl`
- `last_trade_log.json`
- `last_actionable_log.json`
- `pm_outputs/signal_ledger_YYYYMM.jsonl`

### Deviation Checkpoints

| Expected narrative | Actual checkpoint |
|---|---|
| "Retrain due blocks PM." | In `notify` mode it logs only; expired configs/live eligibility can still block new entries separately. |
| "Raw validated configs are live configs." | Live eligibility is stricter than raw validation. |
| "Artifact drift always blocks." | It depends on `live_artifact_drift_policy`; current local profile is `warn`. |
| "Ledger complete means all symbols trade." | Ledger can contain `NO_TRADE`, expired, drifted, or no-winner configs. |
| "Status only reports optimization." | Status separates valid, live eligible, expired, invalid, and due. |

---

## 9. Pipeline E: Live Trading Runtime

### Purpose

The live runtime answers:

> Given the current broker state and the current closed bar, is there an exact validated winner that produces an actionable signal, and can it be executed safely now?

### Startup Inputs

- active winner ledger
- live-eligible configs
- MT5 connection
- broker symbol metadata
- account info
- open positions
- position config
- storage manager

Code anchors:

- `run_trading()`: `pm_main.py:5083`
- live eligibility at startup: `pm_main.py:5118`
- management-only mode if open positions exist but no entry configs: `pm_main.py:5120`
- live trader creation: `pm_main.py:5135`

### Bar-Gated Runtime Loop

Default live mode is `bar`. The loop polls MT5 for changed bar timestamps and processes only affected symbol/timeframe branches.

Code anchors:

- trigger normalization: `pm_main.py:5146`
- main loop: `pm_main.py:5202`
- changed-bar probe: `pm_main.py:1318`
- commit probe times only after successful processing: `pm_main.py:1383`

Practical result:

- no changed bar means no new signal evaluation
- margin/governance can still run as runtime-only safety work
- poll interval is CPU idle behavior, not signal timing logic

### Per-Sweep Safety

Each sweep:

- takes position snapshot
- takes account snapshot
- fails closed if snapshots are unavailable
- blocks non-tradable account states
- runs margin protection
- runs local order governance
- then evaluates entry signals

Code anchor:

- `process_all_symbols()`: `pm_main.py:1450`

### Candidate Selection

For each symbol:

1. Resolve broker symbol.
2. Recover/infer existing position timeframe and regime context.
3. Enforce same-symbol position rules.
4. Load live bars.
5. Use the last closed bar.
6. Read `REGIME_LIVE` and `REGIME_STRENGTH_LIVE`.
7. Require exact validated winner for `(timeframe, regime)`.
8. Apply live quality gates.
9. Generate signal from the selected strategy.
10. Rank candidates by live context quality.

Code anchors:

- symbol processing: `pm_main.py:2745`
- candidate evaluation: `pm_main.py:3160`
- exact winner requirement: `pm_main.py:3311`
- live gates: `pm_main.py:3349`
- candidate rank: `pm_main.py:3379`

### Trade Intent

The selected candidate builds a typed `TradeIntent` with:

- symbol
- timeframe
- regime
- signal
- SL pips
- TP pips
- score context
- governance hint
- bar metadata

Code anchors:

- trade intent build in live path: `pm_main.py:3077`
- `BaseStrategy.build_trade_intent()`: `pm_strategies.py:597`
- optional live TP overlay: `pm_strategies.py:622`

Important current contract:

- discovery disables live-only regime TP overlay
- live can apply the overlay only if non-neutral values are configured
- current multipliers are all `1.0`

### Entry Gates

Before an order is sent, `_execute_entry()` checks:

- storage pressure
- order rate limit
- fresh duplicate/same-symbol position state
- symbol visibility/trade mode
- account and margin state
- margin reopen/cooldown
- tick availability
- spread filter
- stop distances
- risk scalar mode
- secondary risk adjustments
- minimum risk
- broker volume min/max/step
- MT5/tick/pip stop-loss math
- lot-normalization drift
- same-symbol combined risk
- required margin versus free margin
- final quote refresh
- final risk and margin recheck
- MT5 `order_check`

Code anchors:

- entry start/action logging: `pm_main.py:3542`
- position state and symbol checks: `pm_main.py:3680`, `pm_main.py:3698`
- margin gates: `pm_main.py:3869`
- spread overlay: `pm_main.py:3949`
- sizing/risk: `pm_main.py:4017`
- actual risk hard cap: `pm_main.py:4281`
- required margin: `pm_main.py:4334`
- final quote refresh: `pm_main.py:4425`
- order send: `pm_main.py:4633`
- MT5 order preflight: `pm_mt5.py:706`

### Position Management

Live runtime also manages open positions:

- optional close-on-opposite-signal behavior
- margin protection states: `NORMAL`, `BLOCKED`, `RECOVERY`, `PANIC`
- forced loser closes during margin stress
- margin reopen cooldown
- local governance policies such as breakeven and ATR trails
- shadow or active governance depending on config

Code anchors:

- close-on-opposite: `pm_main.py:3120`, `pm_main.py:4727`
- margin protection: `pm_main.py:2577`, `pm_main.py:2622`
- governance loop: `pm_main.py:1697`, `pm_main.py:1733`
- governance policy code: `pm_order_governance.py:184`

### Live Outputs

- real MT5 order or paper order log
- no-action/no-signal reason
- blocked/skipped reason
- decision throttle state
- actionable state
- monthly signal ledger
- PM log
- optional shutdown trade JSON

### Live Deviation Checkpoints

| Expected narrative | Actual checkpoint |
|---|---|
| "Validated winner means trade." | Winner must match live symbol/timeframe/regime and produce signal on last closed bar. |
| "Spread filter is only informational." | Spread filter can block entries. |
| "Broker universe misses are fatal." | Missing broker symbols fail that symbol path but do not remove it from config. Recent log noise is debug-only for bar fetch misses. |
| "Paper/live use same decision path." | Paper skips broker order send but still goes through most selection and risk logic. |
| "Final price is the same as signal price." | Final quote refresh re-anchors price, stops, risk, and margin before submit. |
| "Governance is shadow." | Current local config says governance live mode is `on`. |
| "Daily/session loss advisory stops entries." | It is advisory unless implemented as a blocking policy. |

---

## 10. Pipeline F: Observability And Human Review

### Primary Surfaces

| Surface | Purpose |
|---|---|
| `logs/pm_YYYYMMDD.log` | Full runtime history and warnings. |
| `python pm_main.py --status` | Valid/live/due/blocked status summary. |
| `pm_configs_high_risk.json` | Winner artifacts, validation evidence, expiry, ledger status. |
| `last_actionable_log.json` | Latest actionable/no-action decision state. |
| `last_trade_log.json` | Decision throttle state to prevent duplicate actions. |
| `pm_outputs/signal_ledger_YYYYMM.jsonl` | Monthly actionable decision ledger. |
| `pm_outputs/storage_state.json` | Storage sweep/housekeeping freshness and pressure. |
| dashboard | Operator overview, readiness, strategies, trades, analytics. |

Code anchors:

- actionable logging: `pm_main.py:3634`
- throttle state: `pm_main.py:351`
- actionable cache: `pm_main.py:580`
- signal ledger: `pm_storage.py:133`, `pm_storage.py:205`
- storage freshness: `pm_storage.py:528`
- PM status: `pm_pipeline.py:4280`
- dashboard readiness: `pm_dashboard/app.py:536`

### What To Watch Daily

- no live-eligible configs
- retrain due warnings
- artifact drift warnings/blocks
- repeated `NO_ACTIONABLE` by symbol/regime
- repeated spread blocks
- repeated margin blocks
- repeated broker symbol misses
- skipped risk due to min lot or hard cap
- stale storage state
- open-position management-only mode

### What To Watch Weekly

- live eligibility count versus configured symbol count
- due symbols count
- missing symbols in `ledger_status`
- validation evidence quality
- live-vs-validation drift
- repeated no-winner regimes
- historical audit fragility
- governance policy performance and mode
- config values that have drifted from docs or intention

---

## 11. Practical Success Matrix

| Goal | Input required | Transform | Desired output | Deviation signal |
|---|---|---|---|---|
| Current-market relevance | latest M5 data | active recent workflow | winners based on recent active universe | old data dominating selection, stale source metadata |
| Fair baseline competition | neutral strategy defaults | Stage 1 baseline screen | no strategy advantaged by default SL/TP asymmetry | non-neutral defaults, hidden floors, live overlay in discovery |
| Robust strategy selection | enough active data and regimes | Stage 2 tuning and validation | validated winner per tradeable regime | low trades, high DD, weak PF, poor robustness |
| Broker-executable live trading | MT5 symbols, specs, account, ticks | live gates and preflight | order accepted or clear block reason | symbol missing, invalid stops, min-lot risk drift, margin failure |
| Risk containment | position config and broker account | sizing, hard cap, margin checks | risk within target/cap | `SKIPPED_RISK_CAP`, `BLOCKED_MIN_LOT_EXCEEDS_CAP`, margin blocks |
| Explainable no-trade | decision and logging surfaces | throttled action records | clear reason for no action | silence, missing actionable record, repeated vague logs |
| Operational continuity | winner ledger and retrain schedule | validity/drift checks | live-eligible configs or retrain instruction | expired configs, artifact drift, incomplete ledger |
| Human confidence | status, dashboard, logs, evidence | observability loop | operator can explain PM state | docs/config mismatch, hidden mode changes, stale dashboards |

---

## 12. Critical Deviation Register

This section is the practical bridge between planned narrative and live reality.

| Deviation | Why it matters | How to detect | Current posture |
|---|---|---|---|
| Docs say drift blocking, local config says drift warning | Stale artifacts may still trade if policy is `warn` | check `config.json:79`, live logs for `LIVE WARN` | local `warn` |
| Docs say governance shadow, local config says on | Broker SL/TP may be actively modified | check `config.json:114`, governance logs | local `on` |
| Stage 3 is named like a separate stage but uses fresh-tail surface copy | May be mistaken for independent holdout validation | inspect `pm_pipeline.py:3501`, `pm_pipeline.py:3548` | active workflow behavior |
| `use_regime_optimization` exists but production always uses `RegimeOptimizer` | Config knob can create false expectation | inspect `pm_pipeline.py:2925`, `pm_pipeline.py:2930` | regime path active |
| Legacy selector/validator classes still exist | Readers can confuse dead/offline paths with production | production pipeline comments and instantiation | production bypasses legacy |
| `NO_TRADE` artifacts can be present in a valid ledger | Configured does not mean tradeable | status live-eligible count, no-trade metadata | expected behavior |
| Retrain warning does not block in `notify` mode | Operator may assume PM stopped | check logs and mode | advisory only |
| Artifact drift can still trigger optimization even if live warns only | Optimize due and live eligibility are related but not identical | `should_optimize()` vs `get_live_eligible_configs()` | due can exist while live trades |
| Live bar gate evaluates changed bars only | No signal checks on unchanged bars | bar trigger logs, changed bar probes | expected behavior |
| Broker symbol universe mismatch | Symbols may remain in config but not trade at current broker | MT5 spec sync failures, debug bar misses | expected if broker lacks symbols |
| Position risk config overrides pipeline risk | Risk may not match research value | resolved config summary, `position` section | position authoritative |

---

## 13. How To Use This Document

When the system behaves differently than expected, work through this sequence:

1. Identify the observed output.
   Example: no trade, retrain warning, spread block, no live-eligible configs, or order rejection.

2. Locate the pipeline that owns that output.
   Discovery, live execution, state/retrain, or observability.

3. Check the relevant input.
   Config value, data freshness, winner ledger, MT5 symbol, account state, open positions, or broker tick.

4. Check the transform stage.
   Stage 1, Stage 2, live eligibility, bar gate, candidate selection, risk gates, or order preflight.

5. Compare intended narrative with actual code/config.
   Use the deviation register above.

6. Decide whether the behavior is:
   - correct and expected
   - correct but poorly documented
   - config drift
   - code drift
   - broker/environment constraint
   - a real implementation bug

---

## 14. Minimal Operator Review Checklist

Before live trading:

```text
python pm_main.py --status --config config.json
```

Review:

- `Valid` count
- `Live eligible` count
- `Need optimization`
- `Live artifact policy`
- expired symbols
- blocked symbols
- missing symbols
- `NO_TRADE` regimes

Then inspect:

- `config.json` for drift policy, governance mode, risk, and retrain mode
- `pm_configs_high_risk.json` for `valid_until`, `artifact_meta`, `validation_evidence`
- `last_actionable_log.json` for latest decision reasons
- `pm_outputs/signal_ledger_YYYYMM.jsonl` for repeated blocks
- `logs/pm_YYYYMMDD.log` for broker/spec/margin/spread warnings

If retrain is due:

```text
python pm_main.py --optimize --config config.json
```

If the research/execution contract changed substantially:

```text
python pm_main.py --optimize --overwrite --config config.json
```

Use overwrite with care because it intentionally ignores existing validity.

---

## 15. Source Anchor Index

Core architecture:

- `pm_main.py:5380` - CLI and main entrypoint
- `pm_main.py:4805` - app class
- `pm_pipeline.py:3762` - `PortfolioManager`
- `pm_pipeline.py:2907` - `OptimizationPipeline`
- `pm_core.py:480` - `PipelineConfig`
- `pm_position.py:253` - `PositionConfig`

Discovery:

- `pm_pipeline.py:3005` - load M5-derived timeframe data
- `pm_core.py:3647` - active recent M5 workflow indices
- `pm_pipeline.py:1698` - regime optimizer
- `pm_pipeline.py:1939` - Stage 1 screen
- `pm_pipeline.py:2004` - Stage 2 tuning
- `pm_pipeline.py:2598` - final validation
- `pm_pipeline.py:3081` - historical stress audit
- `pm_pipeline.py:3148` - walk-forward audit attachment

Scoring and exits:

- `pm_core.py:3737` - backtester
- `pm_core.py:3891` - discovery disables live-only TP overlay
- `pm_core.py:4538` - metrics
- `pm_core.py:4796` - FX selection score
- `pm_strategies.py:577` - neutral default TP fallback
- `pm_strategies.py:579` - symmetric minimum exit floor
- `pm_strategies.py:597` - trade intent

State and live eligibility:

- `pm_pipeline.py:136` - artifact metadata
- `pm_pipeline.py:184` - artifact contract match
- `pm_pipeline.py:423` - raw validity
- `pm_pipeline.py:455` - should optimize
- `pm_pipeline.py:4064` - live eligibility reason
- `pm_pipeline.py:4099` - live eligible configs
- `pm_main.py:4964` - production retrain check

Live:

- `pm_main.py:5083` - run trading
- `pm_main.py:5146` - live trigger mode
- `pm_main.py:5202` - live loop
- `pm_main.py:1450` - process all symbols
- `pm_main.py:2745` - process symbol
- `pm_main.py:3160` - candidate evaluation
- `pm_main.py:3311` - exact winner requirement
- `pm_main.py:3542` - entry execution
- `pm_mt5.py:706` - MT5 order preflight/send

Observability:

- `pm_main.py:351` - decision throttle persistence
- `pm_main.py:580` - actionable cache
- `pm_storage.py:133` - signal ledger
- `pm_storage.py:528` - storage freshness
- `pm_pipeline.py:4280` - status report
- `pm_dashboard/app.py:536` - dashboard readiness

