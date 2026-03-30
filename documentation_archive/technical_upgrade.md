# FxPM 1.4 - Technical Upgrade Specification

This document converts the finalized `suggestions.md` / `suggestions.html` audit into a concrete implementation plan. It is not another audit. It is the engineering specification for how to turn the current repo into a materially stronger PM codebase without regressing correctness, live parity, or trading usefulness.

> Archive note (2026-03-30): this specification has now been implemented and audited. It is retained as the canonical upgrade design record. Final completion status and post-audit amendments are tracked in `technical_upgrade_checklist.md` and `UPGRADE_PROGRESS.md` in the same archive folder.

The working assumption is:

- invasive change is acceptable when it produces a better PM,
- no change is acceptable if it knowingly makes research, live trading, or monitoring less truthful,
- the upgrade must improve the codebase itself, not just layer new features on top of unresolved defects.

---

## 1. Frozen Constraints

These are not to be reopened during implementation unless live evidence later proves they are wrong:

- Candle-based next-bar execution remains the architecture.
- Production retraining remains **fixed biweekly Sunday**, not operational walk-forward.
- Validation freshness is preserved, but **scored overlap is removed**.
- `Time-based exit` is **not** a core path.
- Strategy concentration is acceptable **if it survives clean methodology**.
- Trade frequency and volume are to be preserved wherever possible.
- Correctness first, accuracy second, efficiency third.
- Where two implementations are equivalent in output quality, the more efficient and lower-ambiguity one wins.

---

## 2. Target End State

The upgraded PM should have all of the following properties at the same time:

1. Backtest and live logic agree on fills, stops, sizing, and candidate selection rules.
2. Validation is recent but honest: no scored overlap, no repeated holdout peeking, no fake winner approval.
3. Strategy optimization is performed on real parameter surfaces, not dead or duplicated search dimensions.
4. Live trading fails closed on missing state, not open.
5. Dashboard analytics reflect realized outcomes, not order attempts or mismatched enrichments.
6. Cached or persisted artifacts become invalid automatically when the underlying model or data assumptions change.
7. Performance work reduces compute without changing outputs.
8. Post-stabilization enhancements improve profit quality without materially sacrificing trade count.

---

## 3. Workstream Summary

| Package | Theme | Primary Modules | Dependency Rule |
|---|---|---|---|
| A | Research Kernel Correctness | `pm_core.py`, `pm_regime.py` | Must ship before any new full optimization run |
| B | Validation, Search, and Artifact Integrity | `pm_core.py`, `pm_pipeline.py`, `pm_optuna.py`, `pm_regime_tuner.py` | Must ship before trusting any new winners |
| C | Strategy Layer Remediation | `pm_strategies.py`, `pm_core.py` | Must ship before strategy re-ranking |
| D | Live Runtime Hardening | `pm_main.py`, `pm_mt5.py`, `pm_position.py` | Must ship before extended live/paper soak |
| E | Dashboard and Analytics Truthfulness | `pm_dashboard/*` | Must ship before trusting monitoring |
| F | Performance and Output-Preserving Optimization | `pm_core.py`, `pm_strategies.py`, `pm_pipeline.py` | Ships only after parity harness is green for parity-preserving changes |
| G | Quant Enhancement Layer | sizing, exits, portfolio, regimes, execution overlays | Default setting decided only after corrected validation/holdout proves the improvement |

---

## 4. Single Integrated Upgrade Rule

This document is the **single implementation scope** for the upgrade. The packages below are **not optional phases** in the sense of "do some now and maybe do the rest later."

They are:

- one integrated upgrade program,
- one document of record,
- one complete implementation scope.

The sequencing only exists to manage dependency order, regression control, and rerun points inside the same overall upgrade effort.

Nothing in `suggestions.md` or `suggestions.html` is intended to sit outside this document as a future "phase 2" roadmap.

---

## 5. Suggestion Coverage Matrix

This matrix makes the scope explicit.

| Finalized Suggestion Scope | Covered In Technical Upgrade |
|---|---|
| Priority 0 measurement corrections | Package A |
| Priority 0 validation protocol | Package B |
| Risk, execution, and live parity | Package D + Package G1 where sizing overlays are involved |
| Search-objective and selection fixes | Package B + Package C |
| Position sizing and drawdown control | Package G1 |
| Exit upgrades | Package D + Package G2 |
| Portfolio construction | Package G3 |
| Regime detection enhancement | Package G4 |
| Execution quality overlays | Package D + Package G5 |
| Dashboard, config, and tests | Package E + Package B/F where infrastructure overlaps |
| Recovery factor and strategy rotation | Package G1 + Package G2 + Package G7 |
| Strategy robustness / trust-table implications | Package C acceptance gate + post-fix rerank |
| Scoring and config-threshold tuning | Package B |
| Validation freshness / Appendix C conclusions | Package B + Frozen Constraints |
| Additional verified code findings / Appendices D, J, K, L, M, N | Packages A-F by domain |
| Dashboard addendum / Appendix E | Package E |
| Option-pricing adaptations / Appendix H | Package G6 |
| Recommended new strategy additions / Appendix I | Package G7 |
| Final residual issues / Appendix T | Packages A-E by domain |

In other words: the implementation document is now intended to cover the entire finalized suggestion set, not just the high-level categories.

---

## 6. Global Implementation Rules

- Any change to metric math, fill semantics, validation slicing, or search objective requires **full config invalidation** because previously stored winners are no longer comparable on the same scoring basis.
- Strategy breadth is intentional. Strategy-family regression tests exist to verify that each family still behaves correctly after a logic or search-space change; they are **not** a request to compress the roster, merge overlapping families, or stop a naturally dominant winner from winning.
- Any change to strategy signal semantics or parameter surfaces requires **strategy-family correctness tests** before re-optimization, after which the affected family goes back into the same best-per-`(symbol, timeframe, regime)` selection race.
- Any live-trading safety change must **fail closed** when account, position, or symbol state is unavailable.
- Pure performance refactors are not allowed to merge without a **golden-fixture parity harness** against the pre-refactor output. If a change is meant to improve outputs rather than merely compute them faster or more cleanly, it is not a pure refactor and should be judged on corrected PM objective metrics instead.
- New defaults should be the **best validated settings for this PM**, not legacy-safe placeholders. Compatibility or rollout toggles should only remain when needed for controlled comparison, rollback, or broker variance; bug fixes and proven improvements should become the new default.
- The comparison baseline is **corrected, objective-aligned validation/holdout performance**, weighted by PM priorities: profitability first, then drawdown/reliability quality, while preserving trade count and volume.

---

## 6A. Single-Phase Technical Change Map

This section is the developer-facing implementation map for the **single integrated upgrade**. It is intentionally focused on:

- **where** each change lands in the code,
- **why** the change is necessary,
- which downstream modules and tests are coupled to that seam.

The developer can choose the concrete implementation technique, but these are the entry points and technical reasons that define the scope.

### Change Block A: Research Kernel and Data Contract

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Exact source-file selection and raw-bar quarantine | `pm_core.py -> DataLoader.load_symbol`, `DataLoader.get_data`, `DataLoader._validate_data`, `DataLoader.resample` | `load_symbol()` still falls back to wildcard-first file selection and `_validate_data()` only warns on invalid OHLC relationships. That allows the wrong CSV or corrupted bars to flow into every higher-timeframe resample and every downstream backtest. | All pipeline/live data loads; `tests/test_resample_cache.py`; new exact-source and invalid-OHLC tests |
| Feature cache identity and feature-contract invalidation | `pm_core.py -> FeatureComputer._make_cache_key`, `_cache_get`, `_cache_put`, `compute_all`, `compute_required` | Cache identity currently keys mostly on symbol/timeframe/index bounds plus regime params filename. It does not encode feature-version or semantic-contract changes, so stale derived columns can survive math or strategy-surface changes. | `OptimizationPipeline.run_for_symbol`, live `FeatureComputer.compute_all`, `tests/test_feature_cache.py` |
| Regime feature computation must stay on one canonical path | `pm_core.py -> FeatureComputer.compute_all`; `pm_regime.py -> load_regime_params`, `clear_regime_params_cache`, `MarketRegimeDetector.compute_regime_scores`, `compute_regime_for_features` | Regime columns are part of the feature contract. Parameter/cache invalidation, warmup flags, and `REGIME_LIVE` parity all have to be derived from the same detector path or the research/live split drifts silently. | `tests/test_regime_warmup_exclusion.py`, `tests/test_live_loop_integration.py` |
| Broken signal/stop contracts must fail explicitly | `pm_core.py -> Backtester.run` signal coercion/reindex/fill block; stop precompute loop that calls `strategy.calculate_stops()` | `Backtester.run()` currently repairs misaligned signals by reindexing/filling flat and skips invalid precomputed stop rows by turning them into non-entries. That hides broken strategies and turns structural defects into misleading weak metrics. | All strategy evaluations; `tests/test_backtester.py`; strategy regression fixtures |
| Python/Numba fill, slippage, and sizing semantics must be identical | `pm_core.py -> _backtest_loop_numba`, `Backtester._run_python_loop`, `Backtester.run` precomputed `entry_prices` / `sl_prices` / `tp_prices` block | Both execution paths feed selection and validation. Any difference in entry-bar exit handling, gap-stop fills, slippage symmetry, commission/swap handling, or unsafe stop-based sizing changes the search surface rather than just the runtime cost. | All optimizer/scorer paths; `tests/test_backtester.py`; `tests/test_backtest_sl_exit.py` |
| Metric math must be driven by corrected equity and net PnL | `pm_core.py -> Backtester._calculate_metrics`, `_max_consecutive`, `_empty_result` | `_calculate_metrics()` still classifies wins/losses from `pnl_pips`, caps PF, and uses a proxy Sortino path. The corrected metric layer must classify from net dollar outcome and operate on the corrected equity path used for drawdown. | `StrategyScorer`, `RegimeOptimizer`, dashboard analytics, all score-based ranking |
| Regime-bucket metrics must inherit the same corrected definitions as the kernel | `pm_pipeline.py -> RegimeOptimizer._compute_bucket_metrics`, `_bucket_to_full_metrics`, `_compute_regime_score` | Regime bucket scoring currently reconstructs a parallel metric stack. If the kernel math is corrected but bucket math is not, regime selection still optimizes against stale definitions. | `RegimeOptimizer.optimize_symbol`, `pm_optuna.optimize_for_regimes`, regime validation gates |

### Change Block B: Validation, Search, and Artifact Integrity

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Split contract must expose explicit train/warmup/validation/holdout regions | `pm_core.py -> DataSplitter.split`, `DataSplitter.get_split_indices`; `pm_pipeline.py -> OptimizationPipeline.run_for_symbol` | The current splitter still returns only `train` and `val`, with validation starting inside the trained region. The upgrade needs an explicit split contract so freshness can be preserved without scoring overlapped bars. | `tests/test_data_splitter.py`; all pipeline slicing; holdout reporting |
| Holdout isolation and no fallback-to-train approval | `pm_pipeline.py -> OptimizationPipeline.run_for_symbol`, `RegimeOptimizer.optimize_symbol`, `SymbolConfig.default_config`, legacy `StrategySelector` / `Validator` surfaces | Winner approval must not fall back to `default_config` or training-only behavior when validation is weak. The current compatibility fields and legacy selector surfaces make that failure mode easier to reintroduce. | `tests/test_winners_only.py`; live candidate selection; config serialization |
| Optuna objective leakage and regime-max bias | `pm_optuna.py -> OptunaConfig.use_validation_in_objective`, `OptunaTPEOptimizer.optimize.objective`, `OptunaTPEOptimizer.optimize_for_regimes.objective`, `_empty_val_metrics` | Validation-aware tuning and `max(regime_scores)` turn one spiking regime or reused validation slice into search bias. `_empty_val_metrics()` also fabricates a zeroed validation object that can mask "no validation happened". | `RegimeOptimizer`, parameter selection, future optimizer comparisons |
| Artifact fingerprinting must cover full model semantics, not only regime params | `pm_pipeline.py -> ConfigLedger.LEDGER_VERSION`, `ConfigLedger.should_optimize`, `SymbolConfig.to_dict/from_dict`, `RegimeConfig.to_dict/from_dict`, `OptimizationPipeline.run_for_symbol` | Stored configs only carry timestamps plus optional `regime_detection_version`. They do not fingerprint split logic, metric math, strategy schema, scoring mode, or feature contract version, so stale winners can remain "valid" after semantic changes. | `pm_configs.json`, retrain-skipping behavior, live config loading |
| Fixed biweekly retrain must replace research-only retrain window selection in production | `pm_core.py -> PipelineConfig.retrain_periods`; `pm_pipeline.py -> RetrainPeriodSelector`, `OptimizationPipeline.__init__`, `OptimizationPipeline.run_for_symbol` | The code still instantiates and uses `RetrainPeriodSelector`, while the operational decision is fixed biweekly Sunday retraining. Keeping the selector in the production path adds another optimization branch and more stale-window risk. | `run_for_symbol`, config expiry logic, production retrain cadence |
| Regime parameter tuning must be invalidation-safe and clearly research-only | `pm_regime_tuner.py -> RegimeParamTuner.tune_symbol_timeframe`, `tune_all`; `pm_regime.py -> save_regime_params`, `clear_regime_params_cache`; `pm_pipeline.py -> ConfigLedger.should_optimize` | Regime parameter changes are only partially tracked today. If regime params are tuned or replaced, cached regime features and stored winners need deterministic invalidation across research and production. | `regime_params.json`, `FeatureComputer.compute_all`, config reuse |
| Expiry semantics must be centralized and not hard-coded per class | `pm_core.py -> PipelineConfig.optimization_valid_days`; `pm_pipeline.py -> RegimeOptimizer._select_regime_winner` (`valid_until=now + timedelta(days=60)`), `OptimizationPipeline.run_for_symbol` (`valid_until=now + timedelta(days=retrain_days)`) | Validity windows are currently partly config-driven and partly hard-coded. The upgrade should anchor expiry to the single operational retrain model so stale configs are handled consistently. | `ConfigLedger.has_valid_config`, `PortfolioManager.needs_retraining`, live reload behavior |

### Change Block C: Strategy Layer Remediation

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Shared helper/precompute shortcuts must obey the real parameter surface | `pm_strategies.py -> _get_adx_di`, `_get_bb`, `_get_keltner`, `_detect_swing_points`; `pm_core.py -> FeatureComputer.adx`, `plus_di`, `minus_di`, `compute_all` | These helpers decide whether strategies see precomputed features or fresh calculations. If they ignore `std`, `period`, or DI construction differences, parameter dimensions become fake and strategy behavior stops matching the search surface. | Strategy signal generation; `tests/test_strategy_param_grid_consistency.py`; `tests/test_feature_cache.py`; new helper-parity tests |
| Broken or inconsistent trend families | `pm_strategies.py -> SupertrendStrategy.generate_signals`, `ADXTrendStrategy.generate_signals`, `ADXDIStrengthStrategy.generate_signals`, `EMARibbonADXStrategy.generate_signals`, `AroonTrendStrategy._aroon` / `generate_signals` | These families have confirmed issues around first-transition handling, DI/ADX source consistency, suspect cached DI use, and tied-extreme handling. They directly affect trend winner selection and family-to-family comparability. | Trend strategy rankings; new `tests/test_strategy_regressions.py`; pipeline integration tests |
| Broken or misleading breakout/reversal families | `pm_strategies.py -> InsideBarBreakoutStrategy.generate_signals`, `PinBarReversalStrategy.generate_signals`, `EngulfingPatternStrategy.get_default_params` / `generate_signals` / `get_param_grid`, `FisherTransformMRStrategy.get_default_params` / `generate_signals` / `get_param_grid`, `SqueezeBreakoutStrategy.generate_signals`, `KeltnerPullbackStrategy.generate_signals` / `get_param_grid`, `ParabolicSARTrendStrategy._compute_psar` / `generate_signals` | Several strategies either never fire correctly, add an unintended extra delay, expose dead parameters, or consume helper outputs that do not vary with the tuned surface. Those defects change both ranking quality and optimization efficiency. | `tests/test_pipeline_integration.py`; `tests/test_strategy_param_grid_consistency.py`; new strategy regression fixtures |
| Event-vs-level emission cleanup and conditional dead dimensions | `pm_strategies.py -> TurtleSoupReversalStrategy.generate_signals`, `ZScoreVWAPReversionStrategy.generate_signals` / `get_param_grid`, `MACDHistogramMomentumStrategy.generate_signals` / `get_param_grid`, `StochRSITrendGateStrategy._stoch_rsi_from_rsi` / `generate_signals` | These strategies can keep emitting inside a state window or expose tuned dimensions that go dead when filters are off. That inflates action opportunities, wastes optimizer budget, and can create fake signal freedom. | Backtest trade counts; validation filters; feature-cache and param-grid tests |
| Registry/schema integrity for the roster | `pm_strategies.py -> StrategyRegistry`, `_STRATEGY_MIGRATION`, every affected `get_param_grid()` / `get_required_features()` implementation | The registry is the production contract for strategy names, migrations, and param grids. Any broken grid dimension, stale migration entry, or missing feature contract contaminates both optimization and config loading. | `pm_pipeline.py`, `pm_configs.json`, `tests/test_strategy_param_grid_consistency.py`, new `tests/test_strategy_regressions.py` |

### Change Block D: Live Runtime and Order Path

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Canonical decision identity vs broker symbol identity | `pm_main.py -> DecisionThrottle.make_decision_key`, `DecisionThrottle.should_suppress`, `DecisionThrottle.record_decision`, `LiveTrader._process_symbol`, `LiveTrader._evaluate_regime_candidates`; `pm_position.py -> TradeTagEncoder.encode_comment/decode_comment`; `pm_mt5.py -> find_broker_symbol` | Throttle suppression, action logging, risk accounting, and trade tags must anchor to canonical PM identity while MT5 I/O uses broker symbols. If those identities drift, suffixed symbols can bypass same-bar suppression or poison risk aggregation. | `tests/test_decision_throttle.py`; `tests/test_live_loop_integration.py`; dashboard action-log consumers |
| One authoritative live loop | `pm_main.py -> LiveTrader.start`, `LiveTrader._process_all_symbols`, `LiveTrader._reconnect`, `FXPortfolioManagerApp.run_trading` | The repo currently has two loop owners: `LiveTrader.start()` and the app-level `run_trading()` loop. Reconnect policy, shutdown, retrain checks, and cache invalidation should have one runtime owner so live behavior is deterministic. | Live soak behavior; reconnect handling; runtime integration tests |
| Fail-closed tradability and position-state gates | `pm_mt5.py -> MT5Connector.get_positions`, `get_account_info`, `get_symbol_info`, `_get_filling_type`; `pm_main.py -> LiveTrader._process_symbol`, `_check_portfolio_risk_cap`, `_execute_entry` | Missing positions, account data, or symbol metadata must resolve to "state unavailable", not "safe to trade". The current runtime reads open positions in multiple places and treats failed fetches too similarly to true empty state. | `tests/test_live_loop_integration.py`; `tests/test_portfolio_risk_cap.py`; new fail-closed state tests |
| Entry-stop parity and sizing/risk entry point | `pm_main.py -> LiveTrader._process_symbol` closed-bar signal read, `LiveTrader._execute_entry`; `pm_position.py -> PositionCalculator.calculate_stop_prices`, `TradeTagEncoder.get_risk_pct_from_comment`; `pm_mt5.py -> calc_loss_amount`, `normalize_volume` | `_execute_entry()` is the live seam where closed-bar signal timing, stop placement, broker min-stop widening, loss-per-lot math, and final volume/risk normalization all meet. This is the correct technical landing zone for parity fixes and the later scalar risk stack. | Live order placement; secondary-trade logic; symbol-level risk-cap tests |
| Dormant live exit surfaces must either be wired or removed | `pm_main.py -> LiveTrader.close_on_opposite_signal`, `_close_position_on_signal`; `pm_position.py -> PositionConfig`, `PositionManager.check_exit_conditions`, `apply_trailing_stop`, `apply_breakeven_stop` | Exit-management surfaces exist in config and helper code, but the live runtime does not consistently route open-position management through them. Leaving them half-wired creates false capability and invalid expectations around live exit behavior. | Live runtime behavior; future exit-pack enablement; exit-parity tests |
| Runtime cache and symbol-spec invalidation | `pm_main.py -> LiveTrader._candidate_cache`, `_last_bar_times`, `_prune_cache`, `_reconnect`; `pm_mt5.py -> MT5Connector._symbol_cache`, `get_symbol_info`, `to_instrument_spec` | Retrain events, reconnects, and symbol-spec changes must invalidate cached candidates, bar state, and broker specs or the runtime can trade on stale assumptions after state transitions. | Long-running live session correctness; live cache/spec refresh tests |
| Explicit fill policy and retcode handling | `pm_mt5.py -> OrderFillingType`, `MT5Connector._get_filling_type`, `send_market_order`, `close_position`; `pm_main.py -> LiveTrader._execute_entry` result handling | Fill mode, stop-level validation, and retcode-to-success policy are centralized in the MT5 order path. This is the seam for broker-tested fill-policy control that preserves frequency better than a hidden FOK/IOC bias and makes partial-fill behavior explicit. | Live execution quality; broker compatibility; order-path integration tests |

### Change Block E: Dashboard and Analytics Truthfulness

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Realized trades must be separate from actionable/signal feeds | `pm_dashboard/analytics.py -> load_trade_history`, `compute_performance_metrics`, `build_analytics_payload`; `pm_dashboard/app.py -> /api/analytics`, `/api/trades`, `/api/simulate`; `pm_dashboard/watcher.py -> _load_primary_entries`, `merge_actionable_with_log_executions` | The dashboard currently loads generic trade JSONs and operational logs into analytics as if they were realized closed trades, with missing `pnl/profit` often degrading to `0.0`. Monitoring cannot be trusted until realized outcomes and order-attempt feeds are separate products. | `tests/test_dashboard_signals.py`; `tests/test_dashboard_trade_enrichment.py`; frontend analytics/trades views |
| Trade enrichment identity must be exact | `pm_dashboard/analytics.py -> _build_magic_lookup`, `_enrich_trade_metadata`; `pm_dashboard/watcher.py -> _load_trade_map`, `trade_map_is_fresh`, `merge_actionable_with_log_executions`; `pm_dashboard/parsers.py` execution-log regex path | Symbol-only or loose recency enrichment can attach the wrong timeframe/regime/strategy to an executed trade, especially when multiple contexts exist for one symbol. Matching must key off magic/ticket plus forward time ordering and accept broker-suffixed symbols. | Strategy breakdowns; trade tables; simulation inputs; enrichment tests |
| Analytics metric consistency and simulation correctness | `pm_dashboard/analytics.py -> compute_equity_curve`, `compute_drawdown_curve`, `compute_performance_metrics`, `compute_breakdown_by_field`, `compute_monthly_performance`, `reconstruct_trade_outcomes`; `pm_dashboard/app.py -> api_simulate`, `/api/analytics` | Initial capital, end-date filtering, reconstruction PnL assumptions, and metric aggregation must be internally consistent or the dashboard tells a different PnL story than the research/live core. | Dashboard analytics pages; API consumers; simulation tests |
| Config persistence must be atomic and transparent on failure | `pm_dashboard/utils.py -> load_dashboard_config`, `save_dashboard_config`; `pm_dashboard/app.py -> api_config` | Dashboard config writes currently use a direct overwrite path and config load falls back quietly on parse errors. This is the seam for atomic temp-file replace and explicit failure surfacing instead of silent reset behavior. | UI settings; config corruption handling |
| Watcher lifecycle, freshness, and maintenance jobs must be deterministic | `pm_dashboard/watcher.py -> DashboardWatcher.start`, `poll_once`, `update_config`, freshness helpers, PM-config/trade-map loaders; `pm_dashboard/jobs.py -> initialize_data_jobs`, `Scheduler.start` | The dashboard test surface and recency logic depend on watcher startup behavior, timezone handling, and time-based freshness checks. Those need deterministic control or stale actionable signals can remain "valid now" for the wrong reasons. | `tests/test_dashboard_data_jobs.py`; date-sensitive dashboard tests |

### Change Block F: Performance and Output-Preserving Optimization

| Issue | Exact entry points | Why it must change | Coupled consumers/tests |
|---|---|---|---|
| Retire dead or duplicate production paths | `pm_pipeline.py -> StrategySelector`, `HyperparameterOptimizer`, `Validator`, `RetrainPeriodSelector`, `OptimizationPipeline.__init__` | The regime-aware path is the real production path. Keeping older selector/validator/retrain surfaces live in the codebase expands the maintenance surface and makes future regressions harder to spot. | Pipeline reasoning; future refactors; parity harness |
| Dashboard analytics ingress should be cached by file identity | `pm_dashboard/analytics.py -> load_trade_history`, `build_analytics_payload`; `pm_dashboard/app.py -> /api/analytics`, `/api/trades`, `/api/simulate` | The dashboard repeatedly reloads and re-enriches the same trade files and config state across routes. An mtime-keyed parse cache is an output-preserving optimization seam that reduces cost without changing analytics semantics. | Dashboard responsiveness; enrichment tests |
| Watcher parsing should be incremental rather than full re-read | `pm_dashboard/watcher.py -> poll_once`, `_load_primary_entries`, `_load_trade_map`, execution-log load path | `poll_once()` currently re-globs and re-parses the same files every refresh cycle. The correct output-preserving optimization is a per-file mtime/content cache inside the watcher. | Dashboard refresh latency; watcher tests |
| Lazy feature requests need one parameter-aware entry point | `pm_core.py -> FeatureComputer.compute_required`, `FeatureComputer.compute_all`; `pm_strategies.py -> BaseStrategy.get_required_features`; pipeline/live feature call sites | Output-preserving optimization depends on asking for only the required columns without changing feature semantics. The requested-feature contract must be parameter-aware and shared. | `tests/test_feature_cache.py`; new helper-parity tests |
| Trade reconstruction should batch by symbol/timeframe window | `pm_dashboard/analytics.py -> reconstruct_trade_outcomes`; `pm_dashboard/jobs.py -> HistoricalDataDownloader.load_historical_data` | Reconstruction currently loads historical bars trade-by-trade. Grouping requests by `(symbol, timeframe, window)` is the correct output-preserving seam to cut repeated I/O without altering reconstructed outcomes. | `/api/simulate`; dashboard reconstruction tests |
| Verified hotspots for vectorization / low-level optimization | `pm_regime.py -> _compute_atr_percentile`, `_compute_directional_efficiency`, `_compute_direction_flips`, `_compute_whipsaw`, `_compute_structure_break`; `pm_strategies.py -> _detect_swing_points`, `AroonTrendStrategy._aroon`; `pm_core.py -> CCI/rolling indicator paths` | These are the actual loop-heavy hotspots where performance can improve without changing output semantics. The document should point optimization work here, not at broad rewrites. | Benchmark harness; parity tests |
| Remove dead work and stale helper branches | `pm_pipeline.py -> RetrainPeriodSelector` internal feature copies; `pm_core.py -> dead cached ATR/Keltner branches`; `pm_strategies.py -> stale helper fallbacks` | Several branches compute or carry state that is either unused or semantically dead. Removing only those branches lowers ambiguity without broad behavioral risk. | Code maintainability; parity harness |
| Central parity harness ownership | `pm_core.py -> Backtester.validate_execution_timing`; mixed-basket benchmark/test harness files under `tests/` | Output-preserving optimization needs an owned parity gate, not ad hoc spot checks. This is the seam where pre/post fixtures and benchmark cases need to live. | `tests/test_backtester.py`; new parity smoke suite |

### Change Block G: Quant Enhancement Insertion Points

| Enhancement area | Exact entry points | Why this is the right seam | Coupled consumers/tests |
|---|---|---|---|
| Risk scalar stack | `pm_main.py -> LiveTrader._execute_entry`; `pm_position.py -> PositionCalculator.calculate_position_size`, `TradeTagEncoder.get_risk_pct_from_comment`; portfolio state in `MT5Connector.get_positions` | Risk scaling belongs at the live entry-sizing seam because it changes dollar exposure, not signal generation. That preserves trade count while making portfolio-aware risk transparent. | Live risk-cap logic; position sizing tests |
| Market-driven exit pack | `pm_position.py -> PositionConfig`, `PositionManager.check_exit_conditions`, `apply_trailing_stop`, `apply_breakeven_stop`; `pm_main.py -> _close_position_on_signal` and the live position-management loop | Exit upgrades should land where stops and trade lifecycle are already managed, not inside strategy entry logic. That keeps exits market-driven and portable across families. | Future exit-pack tests; live/backtest exit parity |
| Portfolio construction / exposure redistribution | Candidate ranking in `pm_main.py -> LiveTrader._evaluate_regime_candidates` and entry sizing in `_execute_entry`; supporting risk helpers in `pm_position.py` | Portfolio overlays should scale or reprioritize candidates before order placement rather than veto the book upstream. | Portfolio risk tests; live candidate selection |
| Regime model upgrades (HMM/GARCH) | `pm_regime.py -> MarketRegimeDetector.compute_regime_scores`, `compute_regime_for_features`; `pm_core.py -> FeatureComputer.compute_all` | Regime upgrades belong at the regime-column generation seam so downstream selection still consumes a unified regime interface. | Regime warmup/live parity tests; pipeline re-optimization |
| Regime percentile self-reference bias | `pm_regime.py -> _compute_bb_squeeze_score` (lines 870-871), `_compute_atr_percentile` (line 916) | BB squeeze and ATR percentile features use in-sample percentile self-reference, making scores dependent on the window being analyzed rather than stable reference points. Use rolling or expanding-window percentiles anchored to historical distribution. | Regime detection correctness; regime warmup tests |
| Random seed reuse across optimizer paths | `pm_optuna.py -> optimize()` (line 813), `optimize_for_regimes()` (line 934); `pm_pipeline.py -> run_for_symbol()` (line 974), `_select_best_for_regime()` (line 1785) | Multiple Optuna and pipeline paths reset `np.random.seed(42)` mid-execution, creating correlated search paths. Each optimizer run should use a deterministic but unique seed derived from `(symbol, timeframe, regime, run_id)`. | Optimizer diversity; reproducibility tests |
| Version string inconsistency | `pm_main.py` line 27 ("Version: 3.1") vs line 2114 ("FX PORTFOLIO MANAGER v3.0") | Inconsistent version strings create confusion about which build is running. Centralize to one `__version__` constant. | Logging/debugging clarity |
| Execution quality overlays | `pm_main.py -> LiveTrader._evaluate_regime_candidates`, `_execute_entry`; `pm_mt5.py -> get_symbol_info`, spread/fill data access | Spread/fill overlays should adjust ranking or risk close to order-time reality, not mutate strategy semantics. | Live execution filters; trade-count preservation evidence |
| Options-model adaptations | `pm_core.py -> FeatureComputer.compute_all`, `compute_required`; optional new feature helper module | Expected-move and volatility features should enter as derived features, not as a separate execution engine, so they remain lightweight and causal. | Strategy features; benchmark/parity checks |
| New strategy additions | `pm_strategies.py -> new strategy classes`, `StrategyRegistry`, `get_required_features`, `get_param_grid`; `tests/test_pipeline_integration.py`, `tests/test_strategy_param_grid_consistency.py` | New strategies should join the existing strategy contract cleanly: registered, parameterized, feature-scoped, and regression-tested like the current roster. | Full re-optimization; strategy smoke/parity tests |

---

## 7. Package A: Research Kernel Correctness

### Objective

Make the research kernel mathematically and operationally honest. This is the foundation for every later decision.

### Primary code touchpoints

- `pm_core.py`
  - `DataLoader`
  - `FeatureComputer`
  - `Backtester.run`
  - Python and Numba backtest loops
  - `_calculate_metrics()`
- `pm_regime.py`
  - ATR consumers used in regime detection

### Required changes

1. **Unify bar-level equity and drawdown**
   - Both Python and Numba paths must produce per-bar equity, not per-trade equity.
   - Drawdown must be mark-to-market while a trade is open.
   - Sharpe, Sortino, Calmar, and recovery metrics must operate on the corrected equity path.

2. **Unify fill semantics between Python and Numba**
   - Entry-bar SL/TP recheck must exist in both paths.
   - Gap-through-stop must fill at worst tradable bar price, not capped stop.
   - Entry slippage must be applied symmetrically, not only on stop exits.
   - Unsafe stop-based sizing must resolve the same way in both paths: **skip the trade** if reliable loss-per-lot cannot be computed.

3. **Fix cost and classification truthfulness**
   - Win/loss classification must use **net dollar PnL after all costs**.
   - Swap accrual must be applied where holding periods justify it.
   - Metric outputs must clearly separate gross and net values where useful.

4. **Stop hiding broken contracts**
   - Misaligned signal indexes must no longer be silently reindexed and flattened.
   - Unexpected `calculate_stops()` failures must invalidate the candidate rather than silently turning into "weak" results.
   - Only known warm-up / NaN conditions should be tolerated and skipped.

5. **Fix raw data and cache correctness**
   - Invalid OHLC bars must be quarantined or dropped, not only logged.
   - `DataLoader` must require an exact base-timeframe source instead of wildcard-first matching.
   - `FeatureComputer` cache keys must include a source-data fingerprint and regime-parameter fingerprint or timestamp.

6. **Standardize indicator primitives used across layers**
   - ATR must use one consistent definition repo-wide.
   - DI / ADX precompute vs strategy helper logic must be made mathematically identical before those cached values are trusted.

### Implementation notes

- Prefer extracting shared fill/exit logic into one internal semantic layer used by both execution paths instead of fixing Python and Numba independently.
- When a metric-model version changes, stamp it into config fingerprints so stale winners cannot survive.
- This package is expected to reduce some headline metrics. That is a correction, not a regression.

### Required test work

- Extend `tests/test_backtester.py`
- Extend `tests/test_backtest_sl_exit.py`
- Add `tests/test_backtester_contracts.py`
- Add `tests/test_metric_engine.py`
- Extend `tests/test_feature_cache.py`
- Extend `tests/test_resample_cache.py`

### Acceptance gate

- Python and Numba produce identical trades and summary metrics on deterministic fixtures.
- Same-bar stop/target fixtures pass.
- Gap-through-stop fixtures pass.
- Misaligned signals fail loudly during strategy evaluation.
- Invalid OHLC bars cannot silently contaminate backtests.
- Feature caches invalidate when data or regime parameters change.

### Artifact impact

- Bump metric-model version.
- Bump cost-model version.
- Bump feature-cache version.
- Invalidate all persisted configs after merge.

---

## 8. Package B: Validation, Search, and Artifact Integrity

### Objective

Keep the PM fresh while making winner selection statistically honest and artifact invalidation automatic.

### Primary code touchpoints

- `pm_core.py`
  - `DataSplitter`
- `pm_pipeline.py`
  - `OptimizationPipeline`
  - `RegimeOptimizer`
  - `ConfigLedger`
  - `SymbolConfig` / `PipelineResult`
- `pm_optuna.py`
- `pm_regime_tuner.py`
- `config.json`

### Required changes

1. **Redesign the split contract**
   - `DataSplitter.get_split_indices()` should return explicit `train`, `warmup`, `validation`, and optional `holdout` regions.
   - Overlap remains allowed only as a **warm-up buffer**, never as a scored region.
   - `OptimizationPipeline.run_for_symbol()` must slice full-history features accordingly.

2. **Stop reusing the same recent bars for everything**
   - Validation bucket: strategy/parameter selection.
   - Holdout bucket: final accept/reject only.
   - No part of holdout may influence tuning, shortlist ranking, or retrain cadence.

3. **Remove all fallback-to-train approval behavior**
   - If validation fails, the result is `no trade`, not "best train candidate".
   - This applies to both legacy selector paths and regime-aware selection paths.

4. **Fix optimizer objective leakage**
   - Remove `max(regime_scores)` bias in regime tuning.
   - Track per-regime best candidates from the same evaluated trial instead of allowing one regime spike to dominate unrelated regimes.
   - Ensure robustness scoring respects the requested optimization purpose end-to-end.

5. **Demote retrain-period optimization out of production**
   - Production uses `retrain_days = 14`.
   - `RetrainPeriodSelector` becomes research-only or is retired entirely from the live pipeline.
   - `valid_until` must align with the fixed biweekly cadence.

6. **Make invalidation real, not nominal**
   - Persist a fingerprint containing at least:
     - scorer version,
     - cost-model version,
     - split/validation version,
     - strategy-code version,
     - regime-parameter version,
     - data-source version.
   - Thread the current fingerprint through `ConfigLedger.get_symbols_to_optimize()`.
   - Do not let stale winners survive regime-parameter changes.

7. **Fix stale retrain behavior**
   - Clear data/feature caches before retrain.
   - Ensure download path and optimization read path are the same configured directory.

8. **Fix regime tuner leakage**
   - `pm_regime_tuner.py` must evaluate parameter choices on a recent holdout, not only on the same data used to choose them.

9. **Optimize the pipeline surface**
   - The regime-aware path is the real production path.
   - Legacy `StrategySelector`, `HyperparameterOptimizer`, and `Validator` code should either be explicitly retained as offline/research-only or removed from the production path to reduce ambiguity.

10. **Add Deflated Sharpe Ratio (DSR) multiple-testing control**
    - Entry point: `pm_pipeline.py -> RegimeOptimizer._select_best_for_regime()` (line 1980).
    - At the time of the original spec, the system evaluated ~42 strategies x 40+ symbols x 30 Optuna trials with no explicit correction for selection bias. The current upgraded roster is 47, so the multiple-testing argument remains at least as important.
    - Implement DSR per Bailey & Lopez de Prado (2014):
      ```
      SE_SR = sqrt((1 + 0.5*SR^2 - skew*SR + (kurt-3)/4*SR^2) / T)
      DSR = Phi((SR - SR_benchmark) / SE_SR)
      ```
    - Track the full number of trials attempted per `(symbol, timeframe, regime)`. Compute DSR on finalists inside `_select_best_for_regime()`.
    - Use DSR as a **confidence-adjusted lower-bound** on Sharpe, not a hard veto — this preserves trade frequency while penalizing unreliable edges.
    - Optionally run PBO/CSCV on shortlisted candidates (not the full universe) to keep runtime manageable.

11. **Move pre-tuning eligibility gates to regime-local metrics**
    - Entry point: `pm_pipeline.py -> RegimeOptimizer._apply_training_eligibility_gates()` (line 1490).
    - Currently filters on full-sample training metrics before per-regime tuning. A strategy that is strong in RANGE but poor in TREND gets rejected before it ever gets tuned for RANGE.
    - Fix: evaluate gates on regime-local training metrics, or use a blended test that keeps a strategy alive if it is strong in at least one regime and not catastrophic elsewhere.
    - This directly preserves strategy breadth and regime specialization.

12. **Adopt recommended scoring weights and config thresholds**
    - Entry points: `pm_core.py -> PipelineConfig.score_weights` (line 621), `fx_generalization_score()` (line 3348); `config.json` threshold fields.
    - Current score weights: `{"sharpe": 0.25, "profit_factor": 0.20, "win_rate": 0.15, "total_return": 0.15, "max_drawdown": 0.15, "trade_count": 0.10}`.
    - Recommended weights (after Sharpe correction): `{"sharpe": 0.20, "profit_factor": 0.25, "max_drawdown": 0.25, "total_return": 0.10, "win_rate": 0.10, "trade_count": 0.10}`.
    - Rationale: Profit factor and max drawdown are the most robust predictors of live performance. Win rate is the weakest predictor.
    - Config threshold adjustments:
      | Parameter | Current | Recommended | Rationale |
      |---|---|---|---|
      | `regime_min_val_profit_factor` | 1.05 | 1.15 | PF 1.05 is net-negative after real costs |
      | `train_min_profit_factor` | 0.50 | 0.80 | PF 0.50 wastes Optuna budget on hopeless candidates |
      | `fx_gap_penalty_lambda` | 0.50 | 0.70 | Current penalty insufficient for train-optimized params |
      | `fx_val_min_trades` | 5 | 15 | 5 trades = zero statistical power (graduate to 25 as data grows) |
      | `regime_min_val_trades` | 10 | 15 | Paired with confidence-adjusted scoring (not hard cutoff) |
    - These are best-validated defaults, not legacy-safe placeholders. Apply them as the new defaults after corrected validation is in place.

### Required test work

- Update `tests/test_data_splitter.py`
- Extend `tests/test_pipeline_integration.py`
- Extend `tests/test_winners_only.py`
- Add `tests/test_pipeline_validation_contract.py`
- Add `tests/test_config_fingerprint_invalidation.py`
- Add `tests/test_regime_tuner_holdout.py`

### Acceptance gate

- No scored bar has been seen in fitting.
- No validation-failed candidate becomes a stored winner.
- Regime-parameter changes trigger re-optimization automatically.
- Production configs expire on the fixed biweekly schedule.
- Auto-retrain reads newly fetched data, not stale cached data.

### Artifact impact

- Invalidate `pm_configs.json` and any stored winner sets.
- Re-run a smoke basket before full-universe optimization.

---

## 9. Package C: Strategy Layer Remediation

### Objective

Remove structurally broken strategies, fake parameter surfaces, and misleading helper shortcuts before the next ranking cycle.

This package is about making each strategy family real, distinct, and trustworthy. It is not about reducing the number of strategies or forcing them to become more similar to one another.

### Primary code touchpoints

- `pm_strategies.py`
- `pm_core.py` helper/precompute functions used by strategies

### Tier 0 fixes: must ship before re-optimization

| Strategy / Area | Change |
|---|---|
| `InsideBarBreakoutStrategy` | Fix impossible signal conditions |
| `SupertrendStrategy` | Fix missed first transition / first-trade bug |
| `ADXTrendStrategy` | Fix DI/ADX source correctness and slice warm-up behavior |
| `ADXDIStrengthStrategy` | Stop consuming bad precomputed DI values |
| `EMARibbonADXStrategy` | Stop consuming bad precomputed DI values |
| `PinBarReversalStrategy` | Remove double delay |
| `ParabolicSARTrendStrategy` | Remove boundary-artifact first-bar short |
| `EngulfingPatternStrategy` | Remove dead `lookback_level` or implement it properly |
| `FisherTransformMRStrategy` | Remove dead `signal_period` or make it part of entries |
| `SqueezeBreakoutStrategy` | Make `bb_std` real by fixing BB cache behavior |
| `KeltnerPullbackStrategy` | Remove or truly use `kc_mult` |
| `AroonTrendStrategy` | Use last tied extreme, not first tied extreme |

### Tier 1 fixes: important before trusting search quality

- Convert level-like strategies to one-shot event signals where the intended behavior is event-driven.
- Make `TurtleSoupReversalStrategy` reclaim logic one-shot instead of repeat-emitting inside the reclaim window.
- Remove or canonicalize conditional dead grid dimensions in:
  - `ZScoreVWAPReversionStrategy`
  - `MACDHistogramMomentumStrategy`
- Remove the future fake-parameter risk from `StochRSITrendGateStrategy` precomputed shortcuts unless they are keyed by the full parameter set.

### Helper / precompute cleanup

- `_get_bb()` cache key must include `std`.
- `_get_bb()` should memoize computed series.
- `_get_keltner()` should memoize or stop relying on dead precompute.
- Stochastic cache behavior should be parameter-aware.
- Strategies should reuse precomputed CCI/ATR/BB values where outputs are equivalent.
- Dead swing-point computations in divergence strategies should be removed.

### Required test work

- Extend `tests/test_strategy_param_grid_consistency.py`
- Add `tests/test_strategy_regressions.py`
- Add `tests/test_strategy_signal_smoke.py`
- Extend `tests/test_pipeline_integration.py`

### Acceptance gate

- Every tuned parameter either changes signal behavior or stop behavior.
- No strategy in the "fix-before-trust" bucket remains unresolved.
- Smoke fixtures produce deterministic, non-degenerate signals for covered strategies.
- Re-optimization no longer repeats fake/duplicated search dimensions.

### Artifact impact

- Full strategy re-optimization required after this package.

---

## 10. Package D: Live Runtime Hardening

### Objective

Make live trading safer, more deterministic, and more faithful to the research assumptions.

### Primary code touchpoints

- `pm_main.py`
  - `DecisionThrottle`
  - `LiveTrader`
  - runtime orchestration
- `pm_mt5.py`
  - `MT5Connector`
- `pm_position.py`
  - tagging / comment decoding as needed

### Required changes

1. **Collapse to one authoritative trading loop**
   - Remove the dead dual-loop arrangement.
   - The cleanest shape is:
     - app owns outer loop, retrain schedule, and reconnect policy;
     - trader owns one `run_iteration()` / `process_all_symbols()` unit of work.

2. **Separate canonical symbol identity from broker symbol identity**
   - Canonical symbol must be used for:
     - decision throttle keys,
     - action logs,
     - risk accounting,
     - trade intent identity.
   - Broker symbol should be used only for MT5 I/O.

3. **Make position snapshots fail closed**
   - `MT5Connector.get_positions()` must distinguish `None` / fetch failure from true empty state.
   - If the snapshot is unavailable, skip new entries for that iteration.
   - Hoist one positions snapshot per sweep and reuse it.

4. **Create a stricter tradability gate**
   - Terminal connected is not enough.
   - Trading requires:
     - terminal alive,
     - account info available,
     - `trade_allowed`,
     - `trade_expert`,
     - symbol selected and tradable.

5. **Fix stop placement parity**
   - Live stop placement must use the closed signal bar, not the forming bar.

6. **Preserve validated R-multiple when SL is widened**
   - Entry point: `pm_main.py -> LiveTrader._execute_entry()` (line 1530), specifically the broker min-stop widening block at lines 1687-1693.
   - When SL is auto-widened to satisfy `trade_stops_level`, TP is currently left unchanged. This silently degrades the validated R-multiple (reward:risk ratio) that the optimizer selected.
   - Fix: when SL is widened for broker or sizing reasons, recalculate TP to preserve the validated target R-multiple unless explicitly configured otherwise. Formula: `new_tp_dist = original_R * new_sl_dist`.

7. **Resolve dormant live-exit features**
   - Either wire `close_on_opposite_signal`, trailing, and breakeven logic into the live path, or remove the misleading surface until implemented.

8. **Invalidate runtime caches when state changes**
   - Clear candidate caches on retrain and reconnect.
   - Clear MT5 symbol metadata cache on reconnect / disconnect.
   - Refresh symbol specs on a timed basis or session boundary.

9. **Make filling policy explicit**
   - Defaulting blindly to `FOK` is too strict for trade-count preservation.
   - Make fill policy configurable and broker-tested.

10. **Promote drift monitoring**
   - `DriftMonitor` should be instantiated and fed realized-vs-validated deltas.
   - This is monitoring, not a new circuit breaker.

### Required test work

- Extend `tests/test_live_loop_integration.py`
- Extend `tests/test_decision_throttle.py`
- Extend `tests/test_portfolio_risk_cap.py`
- Add `tests/test_live_tradability_gate.py`
- Add `tests/test_symbol_suffix_throttle.py`
- Add `tests/test_live_state_fail_closed.py`

### Acceptance gate

- No same-bar duplicate order attempts occur on broker-suffixed symbols.
- No new entry is attempted when positions or tradability state is unavailable.
- Live stop distances match the closed signal bar.
- Only one authoritative runtime loop remains in production code.

---

## 11. Package E: Dashboard and Analytics Truthfulness

### Objective

Make the dashboard report what actually happened, not what the PM attempted or what a loose enrichment heuristic guessed.

### Primary code touchpoints

- `pm_dashboard/app.py`
- `pm_dashboard/watcher.py`
- `pm_dashboard/analytics.py`
- `pm_dashboard/utils.py`
- dashboard tests

### Required changes

1. **Separate realized trades from order-attempt logs**
   - Dashboard performance must be computed from realized trade outcomes only.
   - Actionable/recommendation logs remain useful, but as a separate operational feed.

2. **Fix enrichment identity**
   - Stop matching executed trades to signals by symbol alone.
   - Prefer `magic`, ticket, or `symbol + timeframe + regime`.
   - Require `trade_ts >= signal_ts`, not absolute time distance.

3. **Make analytics internally consistent**
   - Equity curve, drawdown, return, Sharpe, and Sortino must operate on a consistent realized-trade set.
   - Trades with unparseable timestamps must be either excluded everywhere or ordered consistently everywhere.
   - Risk-adjusted metrics should use daily equity returns or be explicitly labeled non-annualized if still trade-based.

4. **Fix simulation and API input handling**
   - `/api/simulate` must honor `end_date` independently.
   - Invalid JSON should return a handled validation error.
   - Initial capital must be propagated consistently across breakdowns and monthly panels.

5. **Make config persistence crash-safe**
   - Use atomic temp-file replace for dashboard config writes.
   - Surface corrupt-config fallback in logs/API rather than silently resetting to defaults.

6. **Fix lifecycle and recency behavior**
   - Background watcher/scheduler startup should be controllable for tests.
   - Timezone and freshness logic must be deterministic.
   - Date-sensitive dashboard tests should use relative timestamps or clock control.

7. **Add missing analytics metrics**
   - Entry point: `pm_dashboard/analytics.py -> compute_performance_metrics()` (line 283).
   - Currently missing:
     | Metric | Formula | Purpose |
     |---|---|---|
     | Drawdown Duration | Max bars spent in drawdown | Patience assessment |
     | Recovery Time | Bars to recover from max DD | Capital efficiency |
     | Ulcer Index | `sqrt(mean(DD^2))` | Better risk metric than StdDev |
   - These metrics should be added to the same `compute_performance_metrics()` function and surfaced in the analytics API and frontend.

### Required test work

- Extend `tests/test_dashboard_trade_enrichment.py`
- Extend `tests/test_dashboard_signals.py`
- Extend `tests/test_dashboard_data_jobs.py`
- Add `tests/test_dashboard_realized_metrics.py`
- Add `tests/test_dashboard_config_persistence.py`

### Acceptance gate

- Dashboard headline metrics reconcile with realized trade history.
- Enrichment no longer cross-links the wrong same-symbol trade.
- Config corruption cannot silently reset the dashboard without visibility.
- Dashboard tests are deterministic and do not depend on stale absolute dates.
- Drawdown Duration, Recovery Time, and Ulcer Index are computed and displayed.

---

## 12. Package F: Performance and Output-Preserving Optimization

### Objective

Reduce runtime and code ambiguity only after correctness is locked in.

Package F is limited to parity-preserving cleanup. If a proposed "performance" change also changes signals, fills, ranking, or metrics because it is expected to make the PM better, then it belongs in the owning correctness or enhancement package and must be evaluated on corrected PM objectives rather than parity alone.

### Primary code touchpoints

- `pm_core.py`
- `pm_strategies.py`
- `pm_pipeline.py`
- related tests/benchmarks

### Required changes

1. **Retire dead production paths**
   - Remove or quarantine legacy selector/optimizer/validator surfaces if the regime-aware path is the only supported production path.
   - Remove production dependence on `RetrainPeriodSelector`.

2. **Activate lazy features where outputs are unchanged**
   - Introduce a parameter-aware feature request builder for single-strategy evaluation contexts.
   - Do not blindly replace `compute_all()` everywhere; only use lazy computation where parity is proven.

3. **Remove known dead work**
   - dead `train_features` copy in retrain selector if it is retained for research,
   - dead Keltner precompute,
   - unused `atr_20_cached`,
   - repeated indicator recomputation where exact cached equivalents exist.

4. **Vectorize verified hotspots**
   - CCI rolling MAD path,
   - Aroon loop,
   - Kaufman AMA inner loop,
   - regime raw-score loop,
   - divergence swing-point helpers where parity can be maintained.

5. **Measure before and after**
   - Add a lightweight optimization benchmark on a mixed symbol basket.
   - Runtime wins only count if signal/trade outputs remain unchanged.

### Required test work

- Extend `tests/test_feature_cache.py`
- Add `tests/test_strategy_helper_parity.py`
- Add `tests/test_pipeline_perf_smoke.py`

### Acceptance gate

- Golden-fixture outputs are unchanged.
- Mixed-basket optimization runtime improves materially.
- The production path is easier to reason about after the cleanup, not harder.

---

## 13. Package G: Quant Enhancement Layer

This package is **not** allowed to outrun Packages A-F. It sits on top of a corrected, honest baseline.

### G1. Risk Scalar Stack

Implement a layered position-risk scalar:

`base risk x volatility target x exposure/correlation scalar x fractional Kelly cap`

Requirements:

- signal count unchanged,
- sizing changes only dollar risk,
- portfolio and exposure awareness implemented as scalars before hard blocks.
- include drawdown-based position scaling as part of the same scalar stack rather than as an unrelated hard stop.

Primary modules:

- `pm_main.py`
- `pm_position.py`
- portfolio/risk helpers as needed

Acceptance for default enablement:

- trade count remains at least **95%** of corrected baseline,
- profit factor improves,
- drawdown does not worsen,
- live risk accounting remains transparent.

### G2. Market-Driven Exit Upgrade Pack

Allowed exits:

- ATR trailing by strategy family,
- partial profit taking,
- opposite-signal / structure exits where justified,
- breakeven logic where expectancy supports it.

Not included as default:

- time-based exit.

Acceptance for default enablement:

- no material trade-count collapse,
- win rate / profit factor / recovery factor improve on clean holdout,
- live parity path exists before research surface is widened.

### G3. Portfolio Construction Layer

Introduce shrinkage covariance + HRP for active-position risk redistribution.

Requirements:

- allocation layer must scale risk, not suppress large parts of the book by default,
- exposure buckets remain interpretable,
- live implementation must use the same risk worldview as research.

### G4. Regime Model Upgrade Track

Research-first branch:

- HMM regime detection,
- GARCH volatility overlay,
- optional stochastic-volatility overlays if kept lightweight,
- regime upgrades must preserve production freshness and runtime realism.

Nothing in this track becomes default until the current regime layer is already correct and invalidation-safe.

### G5. Execution Quality Overlay

Entry points: `pm_main.py -> LiveTrader._evaluate_regime_candidates()` (line 1266), `LiveTrader._execute_entry()` (line 1530); `pm_mt5.py -> get_symbol_info()` for spread data access.

Soft overlays first:

- **Spread-aware signal filter** (Almgren & Chriss, 2001):
  ```
  min_edge = 1.5 * current_spread
  if ATR(14) < min_edge:
      skip_trade("ATR < 1.5x spread: insufficient edge")
  ```
  This only filters trades where the expected move is smaller than costs — trades that would have been net losers anyway. For wide-spread instruments (XAUUSD, DE30, exotics), this alone improves net profitability by 10-20%.

- **Spread spike detection**:
  ```
  if current_spread > 2.0 * rolling_spread_median[symbol]:
      skip_trade("Spread spike detected")
  ```
  Track rolling spread median per symbol. Skip trades when current spread exceeds 2x median.

- fill-quality penalties,
- marketability checks.

These should first enter as **ranking or sizing penalties**, not hard trade vetoes, unless hard evidence later supports a stricter filter.

### G6. Options-Model Adaptation Track

These are research overlays, not core blockers:

- Garman-Kohlhagen / Black-Scholes expected-move distance features,
- CRR barrier-feasibility features,
- Heston-lite stochastic-volatility regime modifiers.

Implementation rule:

- keep them indicator-like and lightweight,
- no heavy options engine on the live critical path,
- no default rollout without clean incremental value over the corrected baseline.

---

### G7. Strategy Expansion, Recovery, and Rotation Controls

This package also includes the finalized non-core but approved expansion items:

- `VortexTrendStrategy`
- `TRIXSignalStrategy`
- `RelativeVigorIndexStrategy`
- `VIDYABandTrendStrategy`
- `ChoppinessCompressionBreakoutStrategy`

Implementation rules:

- these strategies are added only after Package C has cleaned the existing roster,
- they must be implemented as lightweight indicator-based strategies,
- they enter the same clean validation/search framework as the rest of the book,
- they are not allowed to bypass the corrected trust/ranking process just because they are new.

This package also includes:

- equity curve trading overlays,
- strategy rotation as a sizing-based overlay,
- recovery-factor improvement logic that scales capital allocation rather than suppressing valid signal generation.

Acceptance for default enablement:

- trade frequency remains materially preserved,
- clean-holdout profit factor and recovery factor improve,
- no hidden diversification-forcing logic is introduced.

---

## 14. Dependency Constraints Inside The Single Upgrade Program

These are **dependency constraints**, not implementation phases. The entire document remains one implementation scope.

- establish version/fingerprint constants and repair obviously date-fragile tests before using them as gates,
- complete Change Blocks A-C before trusting any new winner set,
- execute the first clean re-optimization only after A-C are in place,
- complete Change Blocks D-E before trusting live runtime behavior or dashboard monitoring,
- keep Change Block F parity-preserving and bounded by the parity harness,
- enable Change Block G items only after the corrected baseline exists, but still inside the same overall upgrade effort.

---

## 15. Mandatory Re-Optimization and Migration Events

The following events require full artifact invalidation and re-optimization:

- Package A merge
- Package B merge
- Package C merge
- any change to strategy signal semantics
- any change to metric math
- any change to scored split logic
- any change to cost model or fill semantics
- any change to regime-parameter fingerprinting

The following events require at least dashboard rebuild / monitoring refresh:

- Package D merge
- Package E merge

---

## 16. Definition of Done

The technical upgrade is only complete when all of the following are true:

- backtest metrics are computed on corrected equity and drawdown paths,
- validation is fresh and clean,
- stored winners are invalidated automatically when assumptions change,
- no known fake search dimensions remain in production strategies,
- live trading fails closed on missing state,
- monitoring reflects realized outcomes,
- performance refactors are parity-proven,
- enhancement layers are measured against the corrected baseline rather than the legacy one.

If a change improves old metrics by relying on old distortions, it is not done.

---

## 17. Developer Tickbox Order

This is the intended **single-program checklist order** for the developer, not a multi-phase roadmap:

1. lock version/fingerprint constants, split-contract constants, and corrected metric definitions
2. implement Change Block A at its listed code seams
3. implement Change Block B at its listed code seams
4. implement Change Block C at its listed code seams
5. run the first full corrected re-optimization and artifact rebuild
6. implement Change Block D at its listed code seams
7. implement Change Block E at its listed code seams
8. implement Change Block F under the parity harness
9. implement approved Change Block G overlays at their listed seams and validate them against the corrected baseline

The point of the order is dependency control, not deferral. All listed blocks remain in scope for the same upgrade program.

---

## 18. Required Evidence Pack Per Change Block

Every change block should ship with an evidence bundle stored alongside the branch or release notes.

### For Change Blocks A-C

Capture, at minimum:

- smoke-basket metrics before and after,
- trade count,
- win rate,
- profit factor,
- max drawdown,
- total return,
- Sharpe / Sortino on corrected math,
- winner composition changes by strategy, timeframe, and regime,
- config fingerprint/version values used for the run.

### For Change Block D

Capture, at minimum:

- paper/live-dry logs showing no duplicate same-bar entries,
- paper/live-dry logs showing no entries when positions snapshot is unavailable,
- paper/live-dry logs showing no entries when tradability is false,
- paper/live-dry logs showing correct signal-bar stop placement,
- reconnect behavior,
- symbol-spec refresh behavior,
- throttle behavior on suffixed broker symbols.

### For Change Block E

Capture, at minimum:

- a reconciliation sample between realized trade history and dashboard analytics,
- evidence that actionable logs and realized trades are now distinct feeds,
- one enrichment sample proving the correct trade is matched to the correct signal.

### For Change Blocks F-G

Capture, at minimum:

- parity comparison against the corrected baseline,
- runtime benchmark on the mixed basket,
- holdout comparison for each enhancement package enabled,
- trade-count preservation statistics.

---

## 19. Suggested Review Boundaries

These are **review boundaries only**, not separate upgrade phases and not optional scope reductions:

1. `Kernel semantics`: Change Block A
2. `Validation contract`: Change Block B
3. `Strategy truthfulness`: Change Block C
4. `Runtime hardening`: Change Block D
5. `Dashboard truth model`: Change Block E
6. `Output-preserving optimization`: Change Block F
7. `Enhancement seams`: Change Block G

If the developer chooses to deliver in smaller reviewable slices, the slices should follow these boundaries so each seam remains testable and rerunnable on its own.
