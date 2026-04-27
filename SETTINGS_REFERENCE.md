# FxPM 1.4 Settings Reference

This file documents the current `config.json` schema and the way the PM uses those settings today.

Use this as the reference for the live codebase. The `Default` column documents code defaults unless a row says otherwise. The checked-in `config.json` is the active repo profile and may intentionally override those defaults.

---

## 1. Root-Level Settings

| Key | Type | Description |
|---|---|---|
| `pipeline` | object | Optimization, backtest, live-loop, and production schedule controls |
| `position` | object | Live position sizing and optional order-management helpers |
| `mt5` | object | MetaTrader 5 connection settings |
| `broker_specs_path` | string | Path to broker spec cache JSON |
| `instrument_spec_defaults` | object | Fallback defaults for symbols missing explicit specs |
| `instrument_specs` | object | Per-symbol static fallback specs used mainly outside live broker resolution |
| `symbols` | list | Instruments to optimize and trade |

---

## 2. Pipeline Settings

### 2.1 Paths and dataset controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `data_dir` | string | `"./data"` | Historical data directory. Expects root M5 CSV files such as `EURUSD_M5.csv`. |
| `output_dir` | string | `"./pm_outputs"` | Optimization summaries and trade artifact directory. Runtime logs are not stored here. |
| `log_dir` | string | `"./logs"` | Runtime log directory used by `setup_logging()`. |
| `max_bars` | int | `300000` | Max base M5 bars loaded per symbol for the active workflow. |

### 2.1a Active recent M5 workflow

The active repo profile uses `data_workflow_mode = "active_recent_m5"`. The bar counts below refer to base `M5` bars; higher timeframes are derived from the same timestamp-bounded M5 source so the tournament uses one coherent calendar window.

| Key | Type | Default | Active profile | Description |
|---|---|---:|---:|---|
| `data_workflow_mode` | enum | `"active_recent_m5"` | `"active_recent_m5"` | Production data workflow. `"active_recent_m5"` uses explicit M5 windows; `"legacy_percentage"` keeps the older percentage split behavior. |
| `historical_stress_audit_bars` | int | `50000` | `50000` | Oldest bars inside the latest 300k M5 window. This is an older out-of-selection stress audit, not a forward-looking holdout selector. |
| `active_universe_bars` | int | `250000` | `250000` | Newest M5 bars that drive production strategy discovery and Stage 1 baseline eligibility. |
| `active_stage2_pct` | float | `50.0` | `50.0` | Newest percentage of the active universe used for Stage 2 optimization / production-selection surfaces, with overlap warmup retained as context. |

### 2.1b Storage governance

These controls are part of `pipeline` because the live runtime reads them through `PipelineConfig`.

| Key | Type | Default | Description |
|---|---|---:|---|
| `storage_enabled` | bool | `true` | Master switch for the PM-native storage subsystem. |
| `storage_observe_only` | bool | `true` | If `true`, the PM measures and records cleanup candidates without deleting them. Recommended initial rollout mode. |
| `storage_signal_ledger_enabled` | bool | `true` | Enables the monthly actionable signal ledger under `pm_outputs/signal_ledger_YYYYMM.jsonl`. |
| `storage_warn_free_gb` | float | `15.0` | Warning threshold for free disk space. |
| `storage_critical_free_gb` | float | `10.0` | Critical free-space threshold that forces immediate PM-owned housekeeping. |
| `storage_pause_entries_below_free_gb` | float or `null` | `null` | Optional emergency floor for pausing new entries. Disabled by default. Protective closes are not meant to depend on this. |
| `storage_measure_interval_seconds` | int | `300` | Due-time interval for periodic storage measurement. |
| `storage_housekeeping_interval_seconds` | int | `900` | Due-time interval for PM-owned housekeeping. |
| `storage_metaquotes_review_interval_seconds` | int | `21600` | Due-time interval for MetaQuotes review. |
| `storage_write_protect_minutes` | int | `5` | Minimum quiet window before any cleanup action may touch a file or directory. |
| `storage_local_data_first_enabled` | bool | `true` | Prefer PM-local canonical history for live analysis. Live sweeps maintain bounded `.live/<symbol>_<TF>.csv` caches, and when the canonical seed is too stale to bridge safely they re-seed from MT5 instead of stitching a tiny delta across a long gap. |
| `storage_live_sync_bars` | int | `3000` | Floor for direct MT5 M5 reseeds when local continuity cannot be safely bridged. `PipelineConfig` normalizes this to at least `100`. |
| `storage_live_sync_overlap_bars` | int | `100` | Safety overlap used when delta-refreshing a bounded live cache from recent M5 bars. If the local gap grows beyond this overlap window, the PM stops delta-stitching and re-seeds from MT5. |
| `storage_live_cache_max_age_days` | int | `7` | Age-based retention for bounded `.live` timeframe caches when a symbol is inactive or the cache has gone stale. |
| `storage_delta_sync_overlap_minutes` | int | `1440` | Overlap used when historical M5 data is refreshed by merge-in-place delta sync. |
| `storage_resample_cache_max_gb` | float | `1.0` | Size quota for `data/.cache`. The active high-risk profile uses `4.0`; larger 5-10 GB quotas are acceptable when telemetry proves a quality-preserving runtime benefit and storage remains manageable. |
| `storage_resample_cache_max_age_days` | int | `7` | Age-based retention for resample cache entries. The active high-risk profile uses `14`. |
| `storage_logs_keep_days` | int | `14` | Retention window for log files in `log_dir`. |
| `storage_pm_outputs_keep_days` | int | `14` | Retention window for legacy `trades_*.json` snapshots once destructive mode is enabled. |
| `storage_pm_outputs_keep_count` | int | `30` | Count-based retention floor for legacy `trades_*.json` snapshots. |
| `storage_metaquotes_cleanup_enabled` | bool | `false` | Enables destructive MetaQuotes cleanup. Keep off until review-only output has been validated. |
| `storage_metaquotes_root` | string | `""` | Optional explicit root for MetaQuotes data. Empty means `%APPDATA%\\MetaQuotes` on Windows. |
| `storage_metaquotes_active_root_allowlist` | list | `[]` | Protected active MT5 roots or server names. |
| `storage_metaquotes_demo_servers` | list | `["FBS-Demo","MetaQuotes-Demo"]` | Demo server names that may be reviewed as reclaim candidates when inactive. |
| `storage_metaquotes_stale_tester_days` | int | `14` | Age threshold for stale tester-base discovery. |

Resample-cache telemetry now records memory hits, disk hits, misses, invalidations, read/write bytes, and read/write seconds. Use that evidence before reducing or increasing cache size; cache policy should support PM quality and throughput, not pursue smallness for its own sake.

### 2.2 Train/validation split

| Key | Type | Default | Description |
|---|---|---:|---|
| `train_pct` | float | `80.0` | Percentage of bars used for training. |
| `val_pct` | float | `10.0` | Legacy percentage validation field. In `active_recent_m5`, the explicit active-window controls above are the production selectors. |
| `overlap_pct` | float | `10.0` | Shared overlap between train and validation windows. |

Legacy percentage split behavior:

- Training: start to `train_pct`
- Validation: `(train_pct - overlap_pct)` to end
- The overlap is intentional in the legacy methodology

Active workflow behavior:

- The latest `300000` M5 bars are loaded per symbol by the checked-in profile.
- The oldest `50000` M5 bars are `historical_stress_audit`, used to detect catastrophic older-window fragility after winners are selected.
- The newest `250000` M5 bars are the active universe for Stage 1/Stage 2 discovery.
- Stage 1 baseline eligibility runs across the full active universe.
- The newest `50%` of that active universe is used as the fresh Stage 2 optimization/risk-management surface when `risk_management_selection_stage = "stage3"`, with warmup retained as context.

### 2.3 Capital and base risk

| Key | Type | Default | Description |
|---|---|---:|---|
| `initial_capital` | float | `10000.0` | Starting capital for backtests and analytics. |
| `risk_per_trade_pct` | float | `1.0` | Base backtest/optimization risk percentage. It only backfills live sizing when `position.risk_per_trade_pct` is absent. |

### 2.4 Cost modeling

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_spread` | bool | `true` | Apply spread cost in backtests. |
| `use_commission` | bool | `true` | Apply commission per lot in backtests. |
| `use_slippage` | bool | `true` | Apply slippage modeling for stop-based exits. |
| `slippage_pips` | float | `0.5` | Slippage in pips for modeled stop exits. |

### 2.5 Optimization controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `max_param_combos` | int | `150` | Code default max parameter combinations per strategy/timeframe in the optimization path. The active profile uses `200`. |
| `optimization_max_workers` | int | `1` | Parallel worker count for optimization. The active profile uses `4`. |
| `timeframes` | list | `["M5","M15","M30","H1","H4","D1"]` | Timeframes evaluated during optimization. |
| `optuna_family_size_aware_budget` | bool | `false` | Scales per-strategy Optuna trial budgets by parameter-grid size. The active profile enables it. |
| `optuna_min_trials_per_strategy` | int | `50` | Lower bound for family-size-aware strategy trials. The active profile uses `200`. |
| `optuna_max_trials_per_strategy` | int | `500` | Upper bound for family-size-aware strategy trials. The active profile uses `1000`. |
| `optuna_target_coverage_pct` | float | `0.10` | Target percentage of a strategy grid to cover before clamping by the min/max trial bounds. The active profile uses `0.15`. |

The active Stage 1/Stage 2 presets are currently treated as the recommended production baseline. Stage 1 is an eligibility gate over the full active universe; any "Top-K-like" Stage 1 wording means a survivor pool from pass/fail gates, not a Stage 2-style Top-K optimizer. The presets are intentionally quality-first and live-relevance-first; changing them should be evidence-led from regenerated artifacts, one-symbol experiments, or paper/live outcome data.

### 2.6 Legacy weighted-score thresholds

These mainly matter when `scoring_mode = "pm_weighted"`.

| Key | Type | Default | Description |
|---|---|---:|---|
| `min_trades` | int | `25` | Minimum trades for consideration. |
| `min_robustness` | float | `0.2` | Minimum robustness threshold in legacy weighted mode. |
| `min_win_rate` | float | `40.0` | Minimum win rate in legacy weighted mode. |
| `min_profit_factor` | float | `1.1` | Minimum profit factor in legacy weighted mode. |
| `min_sharpe` | float | `0.5` | Minimum Sharpe ratio in legacy weighted mode. |
| `max_drawdown` | float | `18.0` | Maximum drawdown in legacy weighted mode. |

### 2.7 Primary scoring and FX validation controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `scoring_mode` | string | `"fx_backtester"` | Primary scoring methodology. Recommended active mode. |
| `fx_opt_min_trades` | int | `15` | Minimum training trades during parameter search. |
| `fx_val_min_trades` | int | `15` | Minimum validation trades required. |
| `fx_val_max_drawdown` | float | `12.0` | Maximum allowed validation drawdown. |
| `fx_val_sharpe_override` | float | `0.3` | Validation Sharpe threshold that can override weaker robustness in some paths. |
| `fx_selection_top_k` | int | `5` | Top strategy/timeframe candidates retained for validation. |
| `fx_opt_top_k` | int | `5` | Top parameter sets retained per strategy before validation. |
| `fx_gap_penalty_lambda` | float | `0.7` | Train-to-validation gap penalty strength. |
| `fx_robustness_boost` | float | `0.15` | Reward weight for strategies that generalize better. |
| `fx_min_robustness_ratio` | float | `0.80` | Minimum validation-to-training robustness ratio in code defaults. |

### 2.8 Score weights

| Key | Type | Default | Description |
|---|---|---:|---|
| `score_weights` | object | see code | Composite weights for metrics such as Sharpe, PF, return, drawdown, and trade count. |

Additional score-shaping flags:

| Key | Type | Default | Description |
|---|---|---:|---|
| `scoring_use_continuous_dd` | bool | `true` | Apply a continuous drawdown penalty instead of only threshold-style effects. |
| `scoring_use_sortino_blend` | bool | `true` | Blend Sortino-style downside sensitivity into the composite score. |
| `scoring_use_tail_risk` | bool | `true` | Penalize weaker left-tail behavior in score calculations. |
| `scoring_use_consistency` | bool | `true` | Reward steadier strategy behavior instead of only peak aggregate metrics. |
| `scoring_use_trade_frequency_bonus` | bool | `true` | Apply a confidence bonus to candidates with healthier trade counts. |

### 2.9 Regime optimization

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_regime_optimization` | bool | `true` | Enable regime-aware optimization. |
| `regime_min_train_trades` | int | `25` | Minimum training trades per regime bucket. |
| `regime_min_val_trades` | int | `15` | Minimum validation trades per regime bucket. |
| `regime_freshness_decay` | float | `0.85` | Penalty/decay for stale signals. |
| `regime_chop_no_trade` | bool | `false` | If true, CHOP can force no-trade behavior. |
| `regime_params_file` | string | `"regime_params.json"` | Tuned regime parameter file path. Entries are keyed `{symbol: {timeframe: {field: value, ...}}}` and deserialize into `RegimeParams`. **E3**: if an entry omits `adx_trend_threshold`, the asset-class default (`fx=25`, `metal=22`, `crypto=30`, `index=25`) is applied at resolution time; entries that pin `adx_trend_threshold` keep the pinned value. All other fields fall back to `DEFAULT_PARAMS_BY_TIMEFRAME[timeframe]` and then to the `RegimeParams()` defaults. |
| `regime_enable_hyperparam_tuning` | bool | `true` | Enable regime-aware hyperparameter tuning. |
| `regime_hyperparam_top_k` | int | `3` | Code default Top-K strategies to tune per regime. The active profile uses `5`. |
| `regime_hyperparam_max_combos` | int | `150` | Code default max parameter combinations per regime-tuned strategy. The active profile uses `200`. |

Optimization, backtest trade intent, regime TP multipliers, and live winner lookup all prefer the decision-time `REGIME_LIVE` / `REGIME_STRENGTH_LIVE` surface. `REGIME` / `REGIME_STRENGTH` are legacy fallbacks when shifted live-decision columns are unavailable.

### 2.10 Regime winner validation gates

| Key | Type | Default | Description |
|---|---|---:|---|
| `regime_validation_top_k` | int | `5` | Number of ranked regime candidates attempted in descent order before declaring no winner. |
| `regime_min_val_profit_factor` | float | `1.05` | Minimum validation PF for a regime winner to be stored. |
| `regime_min_val_return_pct` | float | `5.0` | Minimum validation return percentage for a regime winner. |
| `regime_min_val_return_dd_ratio` | float | `1.0` | Minimum validation return-to-drawdown efficiency ratio for a regime winner. |
| `regime_allow_losing_winners` | bool | `false` | If enabled, allows PF-below-1 winners to survive the validation gate. |
| `regime_no_winner_marker` | string | `"NO_TRADE"` | Strategy marker written when no candidate passes regime validation. |

### 2.11 Pre-tuning eligibility gates

| Key | Type | Default | Description |
|---|---|---:|---|
| `train_min_profit_factor` | float | `0.5` | Lenient pre-tuning training PF screen. The active profile uses `0.80`. |
| `train_min_return_pct` | float | `-30.0` | Lenient pre-tuning training return screen. The active profile uses `-15.0`. |
| `train_max_drawdown` | float | `60.0` | Lenient pre-tuning training drawdown ceiling. The active profile uses `20.0`. |

These gates act before tuning and should remain materially looser than the final winner gates. The checked-in active profile is stricter than the code defaults because the current goal is higher-quality discovery rather than maximum candidate admission.

### 2.12 Exceptional validation overrides

| Key | Type | Default | Description |
|---|---|---:|---|
| `exceptional_val_profit_factor` | float | `1.5` | Validation PF needed to rescue a weak-train candidate. |
| `exceptional_val_return_pct` | float | `10.0` | Validation return needed to rescue a weak-train candidate. |

### 2.13 Live bars and signal gating

| Key | Type | Default | Description |
|---|---|---:|---|
| `live_bars_count` | int | `1500` | Target bars loaded per evaluation timeframe during live trading. The PM satisfies this from bounded timeframe-specific `.live` caches rather than full canonical reloads. |
| `live_min_bars` | int | `300` | Minimum bars required to evaluate a timeframe live. |
| `live_loop_trigger_mode` | enum | `"bar"` | Live-cycle trigger mode. `"bar"` polls small MT5 bar probes and processes only symbol/timeframe branches whose bar timestamp advanced; `"scheduled"` keeps the legacy due-time fallback. Aliases such as `"quote"` and `"tick"` normalize to `"bar"` for backward compatibility. |
| `live_bar_poll_seconds` | float | `0.25` | CPU-idle poll interval used only to avoid a busy loop while checking MT5 bar availability. It does not decide whether a signal is due. |
| `live_bar_settle_seconds` | int | `5` | Scheduled-mode post-close settle buffer before a bar is treated as due. In bar mode, freshness is determined from live data timestamps instead of this wall-clock gate. |
| `live_stale_retry_seconds` | int | `15` | Scheduled-mode retry fallback when the PM cannot yet compute a trustworthy next due-time or a broker feed has not advanced after the expected close. |
| `live_artifact_drift_policy` | enum | `"block"` | Live eligibility policy when a validated config's semantic artifact contract no longer matches the current optimizer/backtester contract. `"block"` prevents live trading on stale contracts, `"warn"` allows trading but records warnings, and `"ignore"` suppresses the artifact-drift live gate. |
| `live_config_expiry_grace_minutes` | int | `0` | Optional grace period after `valid_until` before live trading blocks an otherwise validated config. The active profile uses `0` so expiry is strict. |
| `actionable_score_margin` | float | `0.92` | Relative margin used when comparing actionable signal quality. |
| `min_trade_risk_pct` | float | `0.1` | Minimum non-zero risk required for a trade to be placed. The active high-risk low-balance profile uses `0.05`. |

### 2.13a Optional live risk scalars

| Key | Type | Default | Description |
|---|---|---:|---|
| `live_risk_scalars_enabled` | bool | `false` | **Legacy** boolean form of `live_risk_scalars_mode`. `true` is interpreted as `"on"` when `live_risk_scalars_mode` is `"off"` (the default). Prefer setting `live_risk_scalars_mode` directly on new configs. |
| `live_risk_scalars_mode` | enum | `"off"` | Tri-state for the risk-scalar overlays: `"off"` (no overlays installed), `"shadow"` (overlays installed; computed scalar logged at INFO but **not applied** to live sizing), `"on"` (overlays installed and applied authoritatively). Use `"shadow"` to measure the would-be sizing delta on real trades before flipping to `"on"`. Invalid values fall back to `"off"` with a one-shot WARN. |
| `target_annual_vol` | float | `0.10` | Annualized volatility target used by the volatility risk scalar when `live_risk_scalars_mode` is `"shadow"` or `"on"`. |

### 2.13b Optional market-driven exit pack

| Key | Type | Default | Description |
|---|---|---:|---|
| `market_driven_exit_pack_mode` | enum | `"off"` | Exit-pack runtime mode. `"off"` disables the pack, `"paper"` records would-be actions without modifying live trades, and `"on"` allows the pack to act. This is separate from the local governance tournament and should be treated as a downstream execution layer. |

### 2.14 Live spread-quality overlay

| Key | Type | Default | Description |
|---|---|---:|---|
| `execution_spread_filter_enabled` | bool | `true` | Enable live spread-quality gating. |
| `execution_spread_min_edge_mult` | float | `1.5` | Block when `ATR < min_edge_mult x spread`. The active high-risk low-balance profile uses `1.25`. |
| `execution_spread_spike_mult` | float | `2.0` | Block when spread exceeds this multiple of rolling median spread. The active high-risk low-balance profile uses `3.0`. |
| `execution_spread_penalty_start_mult` | float | `0.5` | Start soft penalties when `spread / ATR` exceeds this ratio. The active high-risk low-balance profile uses `0.75`. |

`execution_spread_spike_mult` is the spread-spike blocker threshold.

### 2.14a Live margin protection

These keys control live account-level margin safety. `margin_entry_block_level` is the immediate entry block threshold; `margin_reopen_level` is the recovery threshold after stress or a forced close.

| Key | Type | Default | Active profile | Description |
|---|---|---:|---:|---|
| `margin_entry_block_level` | float | `100.0` | `80.0` | Block new entries when valid margin level is below this percentage. |
| `margin_recovery_start_level` | float | `80.0` | `65.0` | Begin forced margin recovery closes below this percentage. |
| `margin_panic_level` | float | `65.0` | `50.0` | More aggressive forced-close band. |
| `margin_reopen_level` | float | `100.0` | `90.0` | Once margin stress or a forced close has occurred, new entries stay blocked until margin level recovers to at least this percentage. |
| `margin_reopen_cooldown_minutes` | float | `15.0` | `5.0` | Additional time-based cooldown after a forced close. |

Missing or unparseable `margin_level` is neutral only when no margin is in use. If margin exposure exists and the level is unavailable, new entries fail closed until the account snapshot becomes trustworthy.

### 2.15 Dual-trade same-symbol controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `allow_d1_plus_lower_tf` | bool | `true` | Allow one D1 trade plus one lower-timeframe trade on the same symbol. |
| `d1_secondary_risk_multiplier` | float | `1.0` | Risk multiplier for the secondary trade. |
| `secondary_trade_max_risk_pct` | float | `0.9` | Hard risk cap for the secondary trade. |
| `max_combined_risk_pct` | float | `3.0` | Combined same-symbol cap for the D1 + lower-timeframe pair. The live check now estimates open-position exposure from actual entry/SL/volume geometry first and uses comment metadata only as a fallback. This is not a portfolio-wide cross-symbol cap. |
| `position_timeframe_overrides` | object | `{}` | Manual open-position timeframe overrides used during restart recovery. Keys are `ticket:<n>` or `magic:<n>`, values are timeframe strings such as `D1` or `H4`. |

### 2.15a Weekly-review observability surfaces

These keys do not add a mid-cycle shutdown layer. They support the weekly review surface that sits inside the fixed two-week retraining cadence.

| Key | Type | Default | Description |
|---|---|---:|---|
| `portfolio_observatory_enabled` | bool | `false` | Enables the `PortfolioObservatory` reporter. It surfaces concurrent exposure and cluster snapshots for review, but it does not block entries or change sizing. |
| `daily_loss_advisory_pct` | float | `0.0` | Advisory daily realized-loss threshold. `0.0` disables it. Crossing the threshold emits INFO only; it does not halt trading or force closes. |
| `session_loss_advisory_pct` | float | `0.0` | Advisory session realized-loss threshold with the same INFO-only behavior as `daily_loss_advisory_pct`. |

Related artifact surfaces:

- `live_eligibility_report(symbol, timeframe, regime)` reports `config_present`, artifact age, cycle position, retrain timestamps, and a freshness band for weekly review.
- Winner artifacts persist a `live_observability` block (`last_seen_drift`, `recent_consecutive_losses`, `last_review_ts`, `operator_note`, `operator_state`) for operator review without auto-derating live sizing.

### 2.15b Walk-forward audit and local governance tournament

These keys are post-selection research and execution-governance controls. They do not narrow the strategy tournament before winner discovery.

| Key | Type | Default | Description |
|---|---|---:|---|
| `walk_forward_audit_enabled` | bool | `false` | Runs walk-forward analysis as an offline audit inside the normal retraining window. It records telemetry on winner artifacts but does not delay validation-to-live promotion. |
| `walk_forward_audit_windows` | int | `3` | Number of audit windows used when `walk_forward_audit_enabled` is on. `PipelineConfig` normalizes this to at least `1`. |
| `local_governance_tournament_enabled` | bool | `false` | Enables the per-context governance sweep that compares candidate order-management policies after a `(symbol, timeframe, regime, strategy)` winner already exists. The active profile enables it. |
| `local_governance_live_mode` | enum | `"off"` | Live runtime mode for the selected governance policy: `"off"` disables the layer, `"shadow"` logs would-be stop updates without modifying broker orders, `"on"` applies the chosen policy to open trades in that exact local context. Invalid values fall back to `"off"` with a WARN. The active profile uses `"shadow"`. |
| `local_governance_candidate_policies` | list[string] | `["control_fixed","breakeven_1r","atr_trail_capped","pure_atr"]` | Candidate governance policies normalized by `PipelineConfig`. Aliases such as `"control"` and `"pure_atr_runner"` are mapped to canonical names. Unknown values are dropped. |
| `winner_ledger_path` | string | `"pm_configs.json"` | Path to the validated-winner ledger used by `PortfolioManager`. The active high-risk profile uses `pm_configs_high_risk.json`. |

### 2.15c Risk-management optimization and historical audit

These settings make risk-management selection explicit after a strategy/timeframe/regime winner exists. They do not replace strategy discovery.

| Key | Type | Default | Active profile | Description |
|---|---|---:|---:|---|
| `risk_management_optimization_enabled` | bool | `true` | `true` | Enables per-winner governance/risk policy evaluation. |
| `risk_management_selection_stage` | enum | `"stage3"` | `"stage3"` | `"stage3"` evaluates policies on the newest fresh selection surface; `"stage2"` evaluates on the full active universe. |
| `historical_audit_min_trades` | int | `5` | `5` | Minimum trades used when classifying historical stress audit evidence. |
| `historical_audit_max_drawdown` | float | `35.0` | `35.0` | Drawdown threshold for older-window stress audit flags. |
| `historical_audit_min_profit_factor` | float | `0.60` | `0.60` | Profit-factor threshold for older-window stress audit flags. |

### 2.16 Production retrain schedule

These fields replaced the old rolling retrain-validity approach for production scheduling.

| Key | Type | Default | Description |
|---|---|---:|---|
| `production_retrain_mode` | string | `"auto"` | `auto`, `notify`, or `off`. |
| `production_retrain_interval_weeks` | int | `2` | Interval between scheduled retrain dates. |
| `production_retrain_weekday` | string | `"sunday"` | Scheduled retrain weekday. |
| `production_retrain_time` | string | `"00:01"` | Scheduled retrain time in `HH:MM`. |
| `production_retrain_anchor_date` | string | `"2026-03-29"` | Anchor date for the calendar schedule. Must match the configured weekday. |
| `production_retrain_poll_seconds` | int | `60` | Loop poll interval for schedule checks. |

Important:

- The PM no longer documents `retrain_periods` as an active production control.
- Production readiness is now governed by the fixed calendar schedule plus artifact/config invalidation.
- The two-week retrain cadence is the freshness mechanism; weekly-review surfaces do not add an automatic mid-cycle shutdown path.

---

## 3. Position Settings

These settings live under `"position"` and primarily affect live trading.

### 3.1 Core sizing

| Key | Type | Default | Description |
|---|---|---:|---|
| `risk_per_trade_pct` | float | `1.0` | Authoritative live target risk percentage. This overrides `pipeline.risk_per_trade_pct` when both are present. |
| `max_position_size` | float | `0.0` | Hard lot-size ceiling. `0.0` means no extra cap beyond broker max. |
| `min_position_size` | float | `0.01` | Hard lot-size floor. |
| `risk_basis` | string | `"balance"` | Live sizing basis. Valid active options are `balance` and `equity`. |
| `max_risk_pct` | float | `2.0` | Hard per-trade risk cap after overlays and sizing logic. The bot may target less risk after scalars/overlays, but it only blocks when actual post-sizing risk exceeds this cap. |
| `risk_tolerance_pct` | float | `2.0` | Advisory tolerance field. Currently not a major live decision driver. |
| `auto_widen_sl` | bool | `true` | Auto-widen stop loss to meet broker minimum stop distance when needed. |
| `allow_min_lot_risk_clamp` | bool | `false` | Optional seam for the standalone position-sizing helper. When `true`, a raw below-min-lot size may clamp up to `min_lot` only if the resulting actual risk still fits `max_risk_pct`; otherwise the trade is skipped. |

### 3.2 Optional trailing helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_trailing_stop` | bool | `false` | Enable trailing-stop helper. |
| `trailing_stop_pips` | float | `0.0` | Trailing distance. |
| `trailing_activation_pips` | float | `0.0` | Profit threshold to start trailing. |

### 3.3 Optional breakeven helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_breakeven_stop` | bool | `false` | Enable breakeven helper. |
| `breakeven_trigger_pips` | float | `0.0` | Profit threshold to move stop to breakeven. |
| `breakeven_offset_pips` | float | `1.0` | Offset above entry once breakeven is triggered. |

### 3.4 Optional scaling helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `allow_scaling` | bool | `false` | Allow scale-ins. |
| `max_scale_ins` | int | `3` | Max number of additions. |
| `scale_in_pct` | float | `50.0` | Size of each scale-in as percentage of the original trade. |

### 3.5 Optional duration and mirrored cost fields

| Key | Type | Default | Description |
|---|---|---:|---|
| `max_trade_duration_bars` | int | `0` | Force-close trade after N bars. `0` disables time-based close. |
| `use_spread` | bool | `true` | Mirrors spread modeling preference into live helper code paths. |
| `use_slippage` | bool | `true` | Mirrors slippage modeling preference into live helper code paths. |
| `slippage_pips` | float | `0.5` | Mirrored slippage setting. |
| `regime_tp_multipliers` | dict[str,float] | `{"TREND":1.25,"BREAKOUT":1.15,"RANGE":0.85,"CHOP":0.75}` | Regime-aware TP multipliers applied when `BaseStrategy.build_trade_intent()` prepares a live trade after winner selection. Empty dict resets to the module default; unknown labels / non-positive values are silently dropped. The base strategy tournament remains unchanged, and the 10-pip TP floor is preserved. |

Important:

- These trailing/breakeven/scaling settings exist, but they are not the default primary exit engine of the PM.
- The active PM still centers on strategy entry logic plus SL/TP and live execution safeguards.
- The local governance tournament is the path for testing context-specific order management after winner selection; it is not a global replacement for strategy discovery.

---

## 4. MT5 Settings

| Key | Type | Default | Description |
|---|---|---:|---|
| `login` | int | `0` | MT5 account number. `0` means use the current logged-in session. |
| `password` | string | `""` | MT5 password. |
| `server` | string | `""` | MT5 server name. |
| `path` | string | `""` | Explicit terminal path if auto-detection is insufficient. |
| `timeout` | int | `60000` | Connection timeout in milliseconds. |
| `portable` | bool | `false` | Launch terminal in portable mode. |

---

## 5. Instrument Specifications

### 5.1 Shared defaults

| Key | Type | Default | Description |
|---|---|---:|---|
| `broker_specs_path` | string | `"broker_specs.json"` | Cached broker specs file path. |
| `instrument_spec_defaults.commission_per_lot` | float | `7.0` | Default commission fallback. |

### 5.2 Per-symbol fields

| Field | Type | Description |
|---|---|---|
| `pip_position` | int | Pip decimal position for the symbol. |
| `pip_value` | float | Pip value per standard lot. |
| `spread_avg` | float | Average spread used mainly in backtests/fallback modeling. |
| `min_lot` | float | Minimum lot size. |
| `max_lot` | float | Maximum lot size. |
| `commission_per_lot` | float | Commission per standard lot. |
| `swap_long` | float | Informational long swap field. |
| `swap_short` | float | Informational short swap field. |

Live trading prefers broker metadata from MT5 when available, with these static values used as fallbacks.

---

## 6. Symbols

The checked-in repo currently configures `62` symbols across 7 asset groups:

- Major FX: `7`
- Cross FX: `21`
- Exotic FX: `8`
- Metals: `2`
- Energy: `3`
- Indices: `11`
- Crypto: `10`

To add a symbol:

1. Add it to `symbols`
2. Add or inherit a fallback spec in `instrument_specs` if needed
3. Ensure MT5 can resolve the broker symbol
4. Ensure historical data exists or can be fetched

---

## 7. Practical Notes

- Runtime logs are written to `logs/`, not `output_dir`.
- `config.json` is the intended PM source of truth, but some defensive code defaults still exist for robustness.
- Live risk precedence is `position.risk_per_trade_pct` -> optional explicitly enabled overlays -> `position.max_risk_pct`.
- The active live sizing basis is either `balance` or `equity`; `free_margin` is not an active documented option.
- `risk_tolerance_pct` is currently much less operationally important than:
  - `pipeline.risk_per_trade_pct`
  - `position.risk_per_trade_pct`
  - `position.risk_basis`
  - `position.max_risk_pct`
  - `pipeline.min_trade_risk_pct`
  - `pipeline.execution_spread_*`
