# FxPM 1.4 Changelog

All notable repository-level changes are documented here.

Notes:

- The application/runtime banner remains `v3.1`
- The repository changelog uses the `1.4.x` track

---

## [1.4.8] - 2026-04-25 (Current)

### Live eligibility and optimize-readiness hardening

- added a strict live eligibility surface separate from raw validated-ledger reads:
  - live trading now blocks expired configs, no-expiry configs, no-winner configs, and artifact-drifted configs when `live_artifact_drift_policy = "block"`
  - `python pm_main.py --status` now reports `Live eligible` and marks live-blocked configs with `BL`
  - `live_artifact_drift_policy` supports `block`, `warn`, and `ignore`; the active profile uses `block`
  - `live_config_expiry_grace_minutes` defaults to `0` for strict expiry
- fixed optimize-linked artifact invalidation so volatile saved metadata such as `ledger_status` no longer makes a freshly optimized config appear due on the next status/retrain check
- hardened live margin gates against non-finite broker/account values:
  - `NaN`/`inf` margin level is treated as unavailable
  - non-finite required margin or free margin blocks with `SKIPPED_MARGIN_UNAVAILABLE`
- propagated the fresh final broker-side symbol position read into the same-symbol combined-risk cap, closing the remaining stale-snapshot race for different-magic same-symbol positions
- surfaced low-balance and margin feasibility events in the dashboard defaults, including `SKIPPED_MARGIN_*` and `BLOCKED_MIN_LOT_EXCEEDS_CAP`
- added storage-state freshness labels (`state_updated_at` and `freshness`) so stale live-sweep fields are distinguishable from current housekeeping state
- changed the default live-loop trigger to MT5 bar timestamps (`live_loop_trigger_mode = "bar"`), so signal checks run from broker bar availability rather than a wall-clock bar scheduler
- kept the legacy due-time scheduler available as `live_loop_trigger_mode = "scheduled"` for intentional fallback/testing use
- kept signal discovery bar-gated while preserving runtime-only management cycles between changed signal bars, so margin protection and open-order governance are not starved while waiting for the next strategy candle
- refreshed executable quote, SL/TP geometry, actual risk, same-symbol risk, and margin immediately before order submit, while keeping strategy signal generation on the closed bar surface

### Audit implementation sweep

- aligned live winner selection with the optimized decision surface by preferring `REGIME_LIVE` / `REGIME_STRENGTH_LIVE`, with legacy fallback to `REGIME` only when shifted live-decision columns are unavailable
- implemented stateful margin reopen hysteresis:
  - stressed margin states and forced margin closes set a reopen-required latch
  - new entries resume only after `margin_reopen_level` and the configured cooldown are satisfied
  - missing or unparseable `margin_level` fails closed only when margin exposure exists
- hardened the final live duplicate-position guard so `_execute_entry()` keeps the sweep snapshot as a cheap first pass but refreshes exact/symbol broker positions immediately before order send
- improved artifact truthfulness:
  - `NO_TRADE` marker reasons survive metadata propagation
  - regime and symbol artifacts persist compact `validation_evidence`
  - top-level selected robustness is populated from validation evidence when available
  - workflow metadata carries data-window fingerprints
  - saved symbol artifacts include active-ledger completion status, missing symbols, and artifact-contract counts
- added optimizer/storage observability:
  - pre-tuning eligibility gates record compact rejection/rescue counters
  - resample-cache telemetry now tracks memory hits, disk hits, misses, invalidations, bytes, and read/write seconds
  - active cache sizing remains benefit-driven; the high-risk profile keeps its 4 GB quota rather than shrinking cache capacity for its own sake
- updated `README.md`, `SETTINGS_REFERENCE.md`, `PATCH_NOTES.md`, `audit.md`, `audit.html`, and `trading_implementation.html` to reflect the implemented behavior
- focused verification:
  - `python -m pytest tests/test_resample_cache.py tests/test_margin_protection.py tests/test_secondary_position_inference.py tests/test_live_loop_integration.py tests/test_pipeline_artifacts.py -q`: `78 passed, 11 subtests passed`
  - `python -m pytest tests/test_config_source_of_truth.py tests/test_position_sizing_edge_cases.py tests/test_portfolio_risk_cap.py tests/test_storage_manager.py tests/test_storage_live_data.py tests/test_feature_cache.py -q`: `66 passed`
  - `python -m py_compile pm_core.py pm_main.py pm_pipeline.py`
- full-suite verification after the latest implementation sweep: `python -m pytest -q`: `559 passed, 1 skipped, 350 subtests passed`

### Active recent workflow and risk-management alignment

- implemented and documented the active recent M5 workflow as the production baseline:
  - latest `300000` M5 bars per symbol
  - oldest `50000` M5 bars as `historical_stress_audit`
  - newest `250000` M5 bars as the active strategy-selection universe
  - Stage 1 baseline eligibility over the full active universe
  - newest `50%` of the active universe as the fresh Stage 2 optimization/risk-management surface with warmup context
- clarified that `historical_stress_audit` is an older-window fragility check, not a forward-looking holdout selector
- hardened the workflow splitter so raising `max_bars` cannot silently expand the configured 50k M5 historical audit window
- passed timeframe context into regime-bucket metrics so Sharpe/Sortino annualization and stability penalties use the intended timeframe instead of an H1 fallback
- aligned risk-management/governance policy selection with selected winners through `risk_management_optimization_enabled` and `risk_management_selection_stage`
- clarified Stage 1 terminology: Stage 1 is a full-active-universe eligibility gate and any "Top-K-like" wording refers to the survivor pool, not the Stage 2 Top-K optimizer
- kept the checked-in Stage 1/Stage 2 presets unchanged as the current recommended baseline:
  - `max_param_combos = 200`
  - `optimization_max_workers = 4`
  - `regime_hyperparam_top_k = 5`
  - `regime_hyperparam_max_combos = 200`
  - family-size-aware Optuna budgeting enabled
- synchronized operator documentation so `README.md`, `SETUP_AND_RUN.md`, and `SETTINGS_REFERENCE.md` reflect the active high-risk low-balance profile and current winner ledger path
- recorded the active profile's governance/risk runtime posture: high-risk live sizing under `position`, `live_risk_scalars_mode = "shadow"`, and `local_governance_live_mode = "shadow"`

### Documentation cleanup

- archived completed audit, analysis, and implementation-planning artifacts under `documentation_archive/2026-04-25-active-workflow-audit/`
- kept active operator guidance at the root while moving historical audit/planning documents out of the main doc surface
- updated the root README and `documentation_archive/README.md` so current docs and archive docs are clearly separated

### Verification

- current implementation verification: `python -m pytest -q`: `559 passed, 1 skipped, 350 subtests passed`
- documentation sync verified with targeted stale-reference checks and `git diff --check`

---

## [1.4.7] - 2026-04-22 (Previous)

Driven by the findings / technical-direction rollout now archived under `documentation_archive/2026-04-25-active-workflow-audit/`. All changes ship with paired tests and zero suite regressions.

### Dashboard hardening audit - 2026-04-24

- dashboard write APIs now allow default local loopback use but block remote writes unless `PM_DASHBOARD_WRITE_TOKEN` is configured and supplied via `X-PM-Dashboard-Token` or `Authorization: Bearer`
- dashboard strategy, trade, and analytics reads now follow `pipeline.winner_ledger_path` from root `config.json` when `pm_configs_path` is left at `auto`, so the high-risk profile reads `pm_configs_high_risk.json`
- dashboard root-data refresh now merges overlapping MT5 bars under an in-process lock and publishes `data/<SYMBOL>_M5.csv` through atomic replace
- README, setup, dashboard, implementation-tracking, and trading-implementation docs now describe the remote-bind security posture and active-ledger behavior

### Phase A — Validation gate retightening

- `regime_min_val_return_pct`: `0.0` -> `5.0`
- `exceptional_val_profit_factor`: `1.15` -> `1.50`
- `exceptional_val_return_pct`: `5.0` -> `10.0`
- `fx_min_robustness_ratio`: `0.75` -> `0.80`
- `actionable_score_margin`: `0.90` -> `0.92`
- `execution_spread_spike_mult`: `3.0` -> `2.0`
- `min_trade_risk_pct`: `0.05` -> `0.10`

Note: the later checked-in small-account high-risk profile intentionally loosens `execution_spread_spike_mult` back to `3.0` and `min_trade_risk_pct` back to `0.05`; `config.json` is the active profile source of truth.

### Phase B — Live-loop hardening

- secondary-trade risk inference now emits a reason-coded `WARN` when both broker geometry and comment decoding fail to recover risk
- spread-spike penalty rewritten with a 100-bar window, anchored to `max(rolling_median, spec.spread_avg)`, with a non-saturating floor (`max(0.25, 1 - 0.4·(ratio - start))`) so spread quality is throttled rather than nuked
- `margin_reopen_cooldown_minutes` (default `15`) added to `PipelineConfig` + `config.json`; engaged after every successful margin-cycle close; entry gate refuses with `SKIPPED_MARGIN_COOLDOWN`
- `DecisionThrottle._load()` now purges entries older than `max_age_hours` (default `24`, `0` disables); rewrites the on-disk file when anything was dropped; one INFO log per load
- `_execute_entry` now `WARN`s when post-normalization actual risk drift exceeds `position.risk_tolerance_pct` (still in-cap), keeps INFO for in-tolerance drift, and blocks unchanged for over-cap

### Phase C — Selection quality

- DD penalty slope tightened from `exp(-0.03·dd)` to `exp(-0.05·dd)` in both `calculate_fx_selection_score` and `calculate_fx_opt_score` so the TPE sampler matches the live selector
- weak-train rescue now also requires `val_sharpe >= 0.5`, `val_win_rate > 50%`, and `val_trades >= max(2·min_val_trades, 50)`; rescue reason names which gate(s) failed
- `fx_val_max_drawdown` tightened `15.0` -> `12.0` (config + dataclass default)
- `fx_stability_penalty_k` (default `2.0`) added to `PipelineConfig` + `config.json`; backtester now emits `std_weekly_return`; `StrategyScorer.fx_generalization_score` applies multiplicative `exp(-k·std_weekly_return)` (disabled when `k=0`)
- `load_regime_params()` records each fallback once per `(symbol, timeframe)` and emits an INFO log; `LiveTrader.run_trading` startup banner surfaces the snapshot
- `OptimizationPipeline._log_regime_coverage` emits a one-line regime distribution per `(symbol, tf)` after the per-TF winner summary (silent when `REGIME` column absent)
- `_process_symbol` refuses to open a trade when `best_overall_score <= 0`, emitting `NO_ACTIONABLE_BEST_SCORE_NONPOSITIVE` through the throttle so the actionable-margin threshold cannot collapse/invert with non-positive leader scores
- removed dead Optuna objective-blend knobs from `PipelineConfig`, `OptunaConfig`, the trial manifest, the startup banner, and `config.json` / `Normal config (Full Equity).json`:
  - `optuna_use_val_in_objective`
  - `optuna_objective_blend_enabled`
  - `optuna_objective_train_weight`
  - `optuna_objective_val_weight`
  These fields populated dataclasses but were never read anywhere (no `.use_validation_in_objective` / `.objective_blend_enabled` / `.objective_train_weight` / `.objective_val_weight` consumer survived in code). The actual train/val lever is `fx_gap_penalty_lambda`. Guard tests in `test_scoring_audit.py` prevent silent reintroduction.
- helper short-circuits in `pm_strategies.py` (`_get_keltner`, `_get_bb`, `_get_adx_di`, `_get_stochastic`, `_get_macd`) now gate on a per-DataFrame precomputed-params tag (`features.attrs['_fxpm_precomputed_params']`) in addition to the exact-default-params check, not on column presence alone. `FeatureComputer` stamps the tag at both precompute paths after writing default-params indicators

### Phase E — Optional uplift (in progress)

- **E4**: `DEFAULT_PARAMS_BY_TIMEFRAME` rows now override `bb_squeeze_lookback` per timeframe — M5=`50`, M15=`80`, H1=`50`, H4=`50`, D1=`60` — so squeeze detection is no longer dense on intraday and stale on daily. M30 intentionally keeps the dataclass default (`200`) because the archived findings did not specify a value for it. The dataclass default stays at `200` so any unknown TF still gets the conservative baseline. Locked by `BBSqueezeLookbackTimeframeTests` in `test_regime_fallback_log.py`.
- **E5**: `DEFAULT_PARAMS_BY_TIMEFRAME['D1'].k_hold` lifted `2` → `3` (`pm_regime.py:332`) so an overnight-holding D1 winner cannot flip its held regime on a single-bar spike before the next evaluation. All other timeframes and the D1 `k_confirm` / `gap_min` rows are untouched. Locked by `DefaultParamsByTimeframeTests` in `test_regime_fallback_log.py`.
- **E1**: tri-state `live_risk_scalars_mode: "off" | "shadow" | "on"` added to `PipelineConfig` + `config.json`. `"shadow"` installs the full overlay stack but `RiskScalarStack.apply()` returns the input `risk_pct` unchanged; the live entry path then calls `RiskScalarStack.compute()` and emits an `INFO` line `Risk scalar SHADOW: would size X% -> Y% (delta ±Δ%); live target unchanged` so operators can measure the would-be sizing delta on real trades before flipping to authoritative `"on"`. The legacy `live_risk_scalars_enabled` boolean still works (`true` falls back to `"on"` when `live_risk_scalars_mode` is left at default `"off"`); invalid mode strings WARN once and degrade to `"off"`. Startup banner now surfaces the active mode (`risk_scalars=off|shadow|on`) instead of a bare boolean.
- **E2**: `BaseStrategy.build_trade_intent()` now scales the base TP by a regime-aware multiplier table (`TREND=1.25`, `BREAKOUT=1.15`, `RANGE=0.85`, `CHOP=0.75`) only after `calculate_stops()` has produced the tournament-neutral SL/TP pair. The base `tp_atr_mult` discovered by Optuna per (symbol, TF, regime) remains the primary lever — these multipliers are downstream governance applied on top of, not instead of, discovery. Reads the look-ahead-safe `REGIME_LIVE` column (falls back to `REGIME` for legacy fixtures); silent no-op when the column is missing, the label is unknown, or the value is NaN. Operators override via `PositionConfig.regime_tp_multipliers`; `FXPortfolioManagerApp.__init__` installs the dict at startup through `pm_strategies.set_regime_tp_multipliers()`. The 10-pip TP floor is preserved. Locked by `tests/test_regime_tp_multipliers.py` (19 tests).
- **E3**: `RegimeParams.adx_trend_threshold` (new field, default `25.0`) + `DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS` re-anchor the regime-detector's ADX normalization to the instrument's natural trend floor. `fx`/`index` keep the classic Wilder `25`; metals use `22` (slower buildup); crypto uses `30` (higher natural floor). `_infer_asset_class()` classifies by symbol prefix (XAU/XAG/XPT/XPD + GOLD/SILVER → metal; BTC/ETH/LTC/XRP/SOL/ADA/DOT/DOGE/BCH → crypto; SP500/NAS100/US30/DAX/FTSE/... → index; all else → fx). `load_regime_params()` applies the asset-class default during fallback **and** to JSON-cached entries unless the JSON explicitly pinned `adx_trend_threshold`. `_normalize_adx(_vectorized)` and `_normalize_adx_mid(_vectorized)` now take a `threshold` parameter; knees are expressed as multiples of the threshold (`0.8·T` / `1.6·T` / `3.2·T` for TREND, `0.6·T` / `0.9·T` / `1.2·T` / `1.6·T` for CHOP) so the curves collapse to byte-identical legacy behavior at `T=25`. `compute_regime_scores` forwards `p.adx_trend_threshold` into both curves and uses it as the NaN seed when the ADX column has gaps. Locked by `tests/test_adx_trend_threshold.py` (22 tests).

### Section 13 - Final implementation sweep

- added `pm_order_governance.py` as the shared post-selection order-governance layer with canonical policy normalization and causal policy evaluation for `control_fixed`, `breakeven_1r`, `atr_trail_capped`, and `pure_atr`
- extended `Backtester.run()` and the Python execution loop so a winner can be replayed under a selected governance policy without changing entry logic, costs, or the fixed-SL/TP control path
- added `TradeIntent` to `pm_strategies.py` and wired the live executor to consume the typed strategy-to-execution contract while preserving backward compatibility for legacy callers
- added the Section 13 weekly-review surfaces:
  - `live_eligibility_report(symbol, timeframe, regime)` for exact-context artifact age / cycle reporting
  - per-winner `live_observability` persistence and live-loop updates
  - `PortfolioObservatory` as an INFO/report-only exposure surface
  - daily/session loss advisories that log at INFO without blocking entries
- added walk-forward audit attachment inside the retraining pipeline; it records artifact telemetry but does not insert a promotion delay between validation and live deployment
- added local governance tournament plumbing in the pipeline and live runtime:
  - offline per-context policy selection and artifact persistence
  - stateful live governance dispatch with `local_governance_live_mode = off|shadow|on`
  - order-governance state persistence in `pm_storage.py`
- synced `config.json`, `Normal config (Full Equity).json`, `SETTINGS_REFERENCE.md`, and the then-active implementation-tracking journal to the shipped keys and behavior
- cleaned remaining patch-history style comments in the touched runtime modules so comments explain function, not implementation history
- full verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`429` tests, `OK`)

### Small-account proof-mode implementation

- `config.json` now ships a dedicated small-account high-risk live profile:
  - `position.risk_per_trade_pct = 2.0`
  - `position.max_risk_pct = 3.0`
  - `position.risk_basis = "balance"`
  - looser spread, same-symbol, and margin thresholds suited to the temporary proof phase
  - full symbol universe remains active
- `pipeline.risk_per_trade_pct` now stays at `1.0` so the tournament-facing baseline remains standard while the temporary live aggression sits downstream in `position`
- added `PipelineConfig.winner_ledger_path` plus `--winner-ledger`; the shipped high-risk profile isolates winners in `pm_configs_high_risk.json`
- live risk estimation now uses a shared cost-aware stop-loss helper so commission is included in:
  - pre-entry sizing and hard-cap checks
  - open-position risk estimation
  - post-fill executable-risk telemetry
- `MT5Connector.calc_margin_required()` added and wired into `_execute_entry()` so the PM blocks locally when required margin exceeds free margin before `send_market_order()`
- added a reason-coded min-lot hard-cap outcome (`BLOCKED_MIN_LOT_EXCEEDS_CAP`) for clearer tiny-account observability
- `PositionConfig.allow_min_lot_risk_clamp` added so the standalone sizing helper can opt into the live clamp-and-cap philosophy only when the resulting min-lot risk still fits `max_risk_pct`
- high-value auxiliary settings are now enabled in the shipped profile without contaminating discovery:
  - `local_governance_tournament_enabled = true`
  - `local_governance_live_mode = "shadow"` in the current active profile
  - `portfolio_observatory_enabled = true`
  - `live_risk_scalars_mode = "shadow"`
  - `daily_loss_advisory_pct = 8.0`
  - `session_loss_advisory_pct = 15.0`
  - `storage_observe_only = true`
  - `storage_metaquotes_cleanup_enabled = false`
- added focused regressions for the new behavior in:
  - `tests/test_config_source_of_truth.py`
  - `tests/test_position_sizing_edge_cases.py`

Note: the controlled live A/B promotion harness for the local governance tournament was tracked as in-progress in the archived implementation journal; the shipped code covers offline selection, artifact persistence, and live shadow/on dispatch.

### Documentation sync

- `SETTINGS_REFERENCE.md`: updated for the new Section 13 config surfaces (`portfolio_observatory_enabled`, walk-forward audit, local governance tournament, advisory loss thresholds) and the checked-in `regime_tp_multipliers` defaults
- the then-active implementation-tracking journal was updated with the final Section 13 sweep and accurate completion states, and has since been archived with the completed audit package
- `config.json` and `Normal config (Full Equity).json`: synced to the newly shipped pipeline and position keys with safe defaults
- expanded regression coverage now includes `tests/test_pipeline_config_fields.py`, `tests/test_enhancement_seams.py`, `tests/test_portfolio_risk_cap.py`, and `tests/test_section13_surfaces.py` in addition to the earlier Phase A-E tests
- repo-wide verification: `python -m unittest discover -s tests -p "test*.py"` passes (`429` tests, `OK`)

---

## [1.4.6] - 2026-04-02

### Restart recovery hardening

- fixed the live-loop `NameError` triggered when an existing position had an unknown timeframe after an ungraceful shutdown
- consolidated open-position timeframe inference into one helper with this priority order:
  - manual `position_timeframe_overrides`
  - live comment decoding
  - legacy `PM_<tag>` strategy-tag matching
  - deterministic magic lookup
  - MT5 opening order/deal metadata recovery
- added support for truncated `PM2`/`PM3` comments so broker-shortened comments can still recover symbol/timeframe
- cached MT5 history-based timeframe recovery per position identifier for the session lifetime
- throttled unknown-timeframe warnings to one warning per open position per session instead of repeated log spam

### Regression cleanup

- restored the code default for `pipeline.actionable_score_margin` to `0.92`
- restored the missing `PipelineConfig` scoring and validation fields used by the current regime-selection baseline
- aligned weak-train rejection messaging and candidate-descent handling with the active scoring-audit contract
- hardened enhancement-seam config coercion so partial or mocked pipeline configs fall back to safe numeric defaults
- fixed dashboard execution-log parsing for `[SECONDARY] Selected:` lines
- fixed dashboard temp-data startup and refresh-path behavior so route tests no longer depend on a pre-created `data/` directory
- completed the live margin-protection state-classification and deleveraging helper path used by the current tests
- refreshed stale test imports and the strategy-expansion baseline to the current `47`-strategy roster
- aligned live risk enforcement with the intended model:
  - `position.max_risk_pct` remains the per-trade hard cap
  - post-scalar `target_risk_pct` remains a sizing target, not the maximum allowed risk
  - same-symbol `max_combined_risk_pct` remains symbol-scoped, not portfolio-wide
- normalized risk fallback values to the active dataclass defaults so config stays authoritative and code backups stay consistent when fields are absent or mocked
- improved live observability:
  - startup config summary now reports the active same-symbol and per-trade risk controls
  - min-lot / volume-normalization drift above target risk is logged explicitly
  - market-order success logs no longer show `0.00000` when MT5 omits the returned fill price
  - throttled `NO_ACTIONABLE_*` live decisions are now surfaced at `INFO` so quiet sessions still show that the PM is evaluating bars
  - each completed live sweep now emits a heartbeat with symbol count, open-position count, equity, and sweep duration
- added regression coverage for config precedence and the live hard-cap-vs-target-risk path
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`237` tests, `OK`)

### Documentation sync

- updated `README.md`, `SETUP_AND_RUN.md`, and `SETTINGS_REFERENCE.md` to document restart recovery, restored scoring defaults, `position_timeframe_overrides`, and the clarified live risk model
- refreshed patch notes and version references for the `1.4.6` repository state

### Storage governance foundation

- added config-backed storage controls under `pipeline.*` for PM-owned retention, disk-pressure thresholds, write-protection windows, MetaQuotes review cadence, and signal-ledger behavior
- added a PM-native storage manager that:
  - records storage state and cleanup manifests in `pm_outputs`
  - runs on sweep/shutdown/optimization hooks without adding a sleep-based janitor loop
  - keeps cleanup comparator-driven via due-time checks
  - supports observe-only rollout by default
- added a monthly JSONL actionable signal ledger under `pm_outputs/signal_ledger_YYYYMM.jsonl`
- wired actionable live outcomes to both `last_actionable_log.json` and the new signal ledger
- hardened `save_trade_log()` to use atomic replace writes
- moved runtime logging setup onto config-driven `pipeline.log_dir`
- upgraded historical M5 refresh to merge, deduplicate, and cap in place instead of blindly overwriting full files
- corrected the local-data-first live path so `live_bars_count` is respected at the I/O layer:
  - live sweeps no longer full-read `*_M5.csv` for every symbol before trimming
  - the trader now maintains bounded timeframe-specific `.live/<symbol>_<TF>.csv` source caches
  - live refresh now delta-merges from the last stored timestamp with a config-backed overlap window via `storage_live_sync_overlap_bars`
- added guarded MetaQuotes discovery for demo roots and stale tester bases, including the post-optimization tester-review path
- cleaned duplicate `pipeline` keys in `config.json` so storage and risk values remain unambiguous
- added regression coverage for storage defaults, PM-owned retention, write-protection windows, signal-ledger writes, and MetaQuotes candidate discovery
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`237` tests, `OK`)

### Final hardening pass

- corrected the local-data-first live path for mixed timeframes:
  - timeframe-specific `.live/<symbol>_<TF>.csv` caches now remain authoritative for live reads
  - undersized local caches fall back cleanly to MT5 for the missing depth instead of silently degrading the requested bar target
  - bounded live-cache writes remain atomic
- completed a second live hardening pass focused on quality-first runtime behavior:
  - bounded live caches now stop delta-stitching across long local gaps and re-seed from MT5 instead of carrying discontinuous series forward
  - `storage_live_sync_bars` now participates in real M5 reseed behavior rather than existing only as a logged config knob
  - the outer live scheduler now derives the next wake-up from per-symbol/per-timeframe due state first, using the stale-retry cadence only as a recovery fallback
  - same-symbol exposure checks now estimate open-position risk from live entry/SL/volume geometry first and use comment metadata only as a fallback
  - secondary-trade snapshot failures no longer touch unassigned runtime values in actionable logging
  - live winner scoring now preserves legitimate `0.0` quality scores and uses an explicit deterministic tie-break instead of implicit config-order bias
- restored strict candle-close live evaluation semantics for the regime-aware path:
  - stale symbol/timeframe branches are skipped before bar loading once that timeframe is not due
  - freshness decay is no longer used to keep stale higher-timeframe candidates alive between bars
  - the main winners-only path now matches the intended no-new-bar, no-re-evaluation behavior
- added `storage_live_cache_max_age_days` so PM-owned `.live` caches are governed separately from `data/.cache`
- normalized nested root-level `storage` config sections into `PipelineConfig` keys so natural config shapes no longer get silently dropped
- made `config.json` more trustworthy as the operator source of truth:
  - added explicit `live_risk_scalars_enabled` and `target_annual_vol` entries
  - removed duplicate instrument-spec keys that were previously being shadowed by later JSON entries
  - added regression coverage to fail if duplicate JSON object keys reappear
- removed dead internal wrappers from the PM/dashboard runtime to reduce maintenance surface without changing behavior
- replaced the dashboard data-maintenance scheduler's fixed sleep loop with the same due-time comparator pattern used by the PM runtime and removed per-symbol sleep pacing from the root-data refresh path
- updated `README.md`, `SETUP_AND_RUN.md`, `SETTINGS_REFERENCE.md`, and dashboard docs so the documented live flow now matches the real runtime:
  - `position.risk_per_trade_pct` is the authoritative live target risk
  - risk scalars are opt-in
  - local-data-first uses timeframe-specific bounded live caches rather than full canonical reloads
- repo-wide verification now passes with `python -m unittest discover -s tests -p "test*.py"` (`253` tests, `OK`)

---

## [1.4.5] - 2026-04-01

### Core PM upgrade completion

- completed the major PM implementation pass driven by the archived audit and upgrade specs
- kept the active live regime engine as the score-based 4-regime detector
- expanded the live strategy registry from `42` to `47`
- hardened backtest/live parity around same-bar gap-through-stop handling
- strengthened validation, regime selection, and optimizer reproducibility

### Strategy roster

Added:

- `VortexTrendStrategy`
- `TRIXSignalStrategy`
- `RelativeVigorIndexStrategy`
- `VIDYABandTrendStrategy`
- `ChoppinessCompressionBreakoutStrategy`

### Live trading and MT5 hardening

- removed the legacy `--auto-retrain` path
- moved production retraining to the calendar schedule controlled by:
  - `production_retrain_mode`
  - `production_retrain_interval_weeks`
  - `production_retrain_weekday`
  - `production_retrain_time`
  - `production_retrain_anchor_date`
- hardened MT5 symbol resolution, tradability checks, order preflight, and partial-fill handling
- wired the spread-quality overlay into config with:
  - `execution_spread_filter_enabled`
  - `execution_spread_min_edge_mult`
  - `execution_spread_spike_mult`
  - `execution_spread_penalty_start_mult`

### Dashboard alignment

- fixed analytics expectancy and pip-value handling
- hardened watcher, jobs, parsers, and utils behavior
- aligned templates/static assets with current dashboard behavior
- clarified the dashboard as read-mostly rather than fully read-only

### Documentation sync

- rewrote `README.md` to match the current PM architecture and runtime behavior
- rewrote `SETUP_AND_RUN.md` for the fixed retrain schedule, root `logs/`, and current live flow
- rewrote `SETTINGS_REFERENCE.md` against the current config and code semantics
- rewrote `pm_dashboard/README.md` for current dashboard capabilities and write behavior
- refreshed this changelog so the repo state and docs align

---

## [1.4.4] - 2026-02-07

### Winners-only cleanup

- removed deprecated fallback/tier risk artifacts from the main live path
- simplified `_execute_entry` around a winners-only risk model
- updated trade comment formats while preserving backward decoding compatibility

### Warning and config cleanup

- fixed a broad batch of pandas warning sources
- aligned `fx_min_robustness_ratio` propagation across code paths
- added optimization progress visibility

---

## [1.4.3] - 2026-02-07

### Config and indicator cleanup

- expanded config documentation coverage
- introduced shared indicator helper caching
- added live-loop integration tests

---

## [1.4.2] - 2026-02-07

### Strategy expansion

- expanded the roster from `27` to `42` strategies

### Safety and dashboard upgrades

- improved warmup protection, numeric guards, and MT5 parity
- added a major dashboard capability upgrade

---

## [1.4.1] - 2026-02-01

### Optimization and efficiency

- integrated Optuna TPE
- introduced major speed improvements around backtesting and regime work
- added stateful optimization persistence

---

## [1.4.0] - 2026-01-15

### Initial release

- introduced the core regime-aware PM architecture
- shipped the initial 27-strategy version
- added MT5 live trading and the original dashboard
