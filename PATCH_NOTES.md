# FxPM Patch Notes
Scope: Comprehensive change history for this repository.
Status: Canonical patch note file (single source of truth).

## 2026-04-25 - Live Eligibility and Optimize-Readiness Hardening
- Added `PortfolioManager.get_live_eligible_configs()` and moved live trading surfaces to it instead of raw `get_validated_configs()`.
- Added `live_artifact_drift_policy` (`block`, `warn`, `ignore`) and `live_config_expiry_grace_minutes`; the active profile blocks drifted artifacts and has no expiry grace.
- Fixed artifact-contract comparison so operational metadata such as `ledger_status` does not make a freshly optimized config immediately due.
- Updated `--status` to report `Live eligible` and show live-blocked configs as `BL`.
- Treated non-finite margin values (`NaN`/`inf`) as unavailable and blocked unsafe entries with `SKIPPED_MARGIN_UNAVAILABLE`.
- Passed the final fresh symbol-position read into the same-symbol combined-risk cap.
- Added a fresh same-symbol D1/lower-timeframe pairing check inside `_execute_entry()` so a candidate selected from an older sweep snapshot is blocked if the broker position state changes before sizing.
- Refreshed the executable quote immediately before submit and rechecked shifted SL/TP geometry, actual risk, same-symbol risk, and margin against that final quote.
- Updated dashboard defaults so margin and minimum-lot feasibility skips are visible and valid in the dashboard feed.
- Added storage-state freshness metadata so stale sweep values are not confused with current housekeeping.
- Added MT5 bar-timestamp live gating via `live_loop_trigger_mode = "bar"` and `live_bar_poll_seconds`; changed broker bars now wake signal checks for the affected symbol/timeframe branches, while the older due-time loop remains available as `scheduled`.
- Preserved runtime-only management cycles between changed signal bars so margin protection and open-position governance continue running while signal discovery remains bar-gated.
- Added focused regression tests for live eligibility, artifact-contract volatility, margin finite checks, fresh symbol risk snapshots, dashboard visibility, and storage freshness.
- Full optimizer was not run.

## 2026-04-25 - Intent Audit Implementation Sweep
### Live/optimizer alignment
- Live regime winner lookup now prefers the same decision-time surface used by optimization and backtesting: `REGIME_LIVE` / `REGIME_STRENGTH_LIVE`, with legacy fallback to `REGIME` only when shifted columns are unavailable.
- Added regression coverage where `REGIME` and `REGIME_LIVE` intentionally differ so live selection must use the intended surface.

### Live safety and account protection
- Implemented stateful margin reopen hysteresis:
  - margin stress or a forced close sets a reopen-required latch
  - new entries resume only after `margin_reopen_level` and the configured cooldown are satisfied
  - missing/unparseable `margin_level` blocks new entries only when margin exposure exists
- Hardened the final duplicate-position guard so `_execute_entry()` uses the sweep snapshot first, then refreshes exact/symbol broker positions immediately before order send.

### Artifact and optimizer truthfulness
- Preserved `NO_TRADE` marker reasons during artifact metadata propagation.
- Added compact `validation_evidence` to regime and symbol artifacts.
- Populated selected top-level robustness evidence where available.
- Added per-symbol/timeframe data-window fingerprints and ledger-completion status.
- Added pre-tuning eligibility gate telemetry for rejection/rescue visibility.

### Cache/storage observability
- Added resample-cache telemetry for memory hits, disk hits, misses, invalidations, bytes, and read/write seconds.
- Kept the active high-risk profile's 4 GB resample-cache quota. Future 5-10 GB cache sizing is acceptable when telemetry proves a quality-preserving efficiency benefit and storage remains manageable.

### Documentation and verification
- Updated `README.md`, `SETTINGS_REFERENCE.md`, `CHANGELOG.md`, `audit.md`, `audit.html`, and `trading_implementation.html`.
- Full optimizer was not run.
- Verification: `python -m pytest -q` -> `559 passed, 1 skipped, 350 subtests passed`.
- Hygiene: `git diff --check` passed for the touched tracked files.

## 2026-04-25 - Active Workflow Documentation and Preset Propagation
### Stage 1 / Stage 2 preset posture
- Reviewed the checked-in backtester and optimizer presets against the active `config.json`.
- Kept the current Stage 1/Stage 2 values unchanged as the recommended baseline:
  - `max_param_combos = 200`
  - `optimization_max_workers = 4`
  - `regime_hyperparam_top_k = 5`
  - `regime_hyperparam_max_combos = 200`
  - `optuna_family_size_aware_budget = true`
- Documented that these presets are a strong current baseline, not a guaranteed global optimum; proof still comes from regenerated ledgers, paper/live outcomes, and targeted one-symbol experiments.

### Workflow documentation
- Updated operator docs to describe the active recent M5 workflow:
  - latest `300000` M5 bars per symbol
  - oldest `50000` as `historical_stress_audit`
  - newest `250000` as the active universe
  - Stage 1 baseline eligibility over the full active universe
  - newest `50%` of the active universe as the fresh Stage 2 optimization/risk-management surface with warmup context
- Clarified that the older audit window detects severe fragility and does not act as a forward-looking holdout.
- Hardened the workflow splitter so raising `max_bars` cannot silently expand the configured 50k M5 historical audit window.
- Propagated timeframe-aware regime-bucket metrics.
- Propagated the high-risk low-balance profile into README/setup/settings docs, including `pm_configs_high_risk.json`, live sizing, spread profile, risk scalars in shadow mode, and governance in shadow mode.

### Verification
- Documentation-only sync; no full optimization run was performed.
- Current implementation verification: `python -m pytest -q`: `559 passed, 1 skipped, 350 subtests passed`.
- Ran targeted stale-reference checks and `git diff --check` after the documentation patch.

### Documentation cleanup
- Archived completed audit, analysis, and implementation-planning artifacts under `documentation_archive/2026-04-25-active-workflow-audit/`.
- Moved stale root-level files such as `audit.md`, `audit.html`, `analysis.md`, `analysis.html`, `IMPLEMENTATION_TRACKING.md`, `findings.html`, and the active-workflow proposal/implementation documents out of the main operator doc surface.
- Added an archive index and updated the root README/documentation archive README so the active docs remain `README.md`, `SETUP_AND_RUN.md`, `SETTINGS_REFERENCE.md`, `CHANGELOG.md`, and `PATCH_NOTES.md`.

## 2026-04-02 - Restart Recovery Completion and Regression Cleanup
### Live restart hardening
- Fixed the live-loop crash caused by referencing `has_unknown_position_timeframe` instead of the actual `has_unknown_position` state.
- Finished the unified open-position timeframe inference path in `pm_main.py`:
  - `position_timeframe_overrides`
  - live comment decode
  - legacy `PM_<tag>` strategy-tag match
  - magic lookup
  - MT5 opening order/deal metadata recovery
- Kept fail-closed behavior intact: if timeframe still cannot be resolved, secondary trades remain blocked.
- Added per-position warning throttling so unknown-timeframe leftovers after an ungraceful shutdown emit one warning per session instead of spamming every cycle.

### MT5 metadata recovery
- Added session-cached timeframe recovery keyed by `POSITION_IDENTIFIER`.
- Preserved best-effort fallback behavior: history lookups never override a confident live comment or magic decode.
- Continued using truncated `PM2`/`PM3` comment decode so broker-shortened comments still recover symbol and timeframe when possible.

### Regression fixes
- Restored `PipelineConfig.actionable_score_margin` default to `0.92` to match the repo test contract and prior baseline notes.
- Restored the missing `PipelineConfig` scoring and validation fields expected by the active scoring path:
  - `regime_validation_top_k`
  - `regime_min_val_return_dd_ratio`
  - `scoring_use_continuous_dd`
  - `scoring_use_sortino_blend`
  - `scoring_use_tail_risk`
  - `scoring_use_consistency`
  - `scoring_use_trade_frequency_bonus`
- Restored the stricter weak-train exceptional defaults:
  - `exceptional_val_profit_factor = 1.5`
  - `exceptional_val_return_pct = 10.0`
- Fixed regime-candidate descent bookkeeping and weak-train rejection messaging so the live code matches the scoring-audit expectations already encoded in tests.
- Hardened `create_default_enhancement_seams(...)` against partial or mocked config objects by safely coercing bool/float fields.
- Fixed dashboard execution-log parsing for `[SECONDARY] Selected:` lines by using the current regex group name.
- Fixed dashboard app startup and refresh-job writes for temp-data tests:
  - `create_app(..., start_background_workers=False)` now stays read-mostly as intended
  - refresh jobs create missing `data/` directories before writing CSVs
- Completed the current live margin-protection helper path in `pm_main.py`:
  - margin-state classification
  - RECOVERY / PANIC close-cycle handling
  - post-close re-checks and transition logging
- Refreshed stale tests to the current repo surface:
  - repaired missing imports in feature-cache and instrument-spec tests
  - moved the resample-cache temp path into the repo runtime artifact area
  - replaced the stale 42/50 strategy expansion test with a baseline aligned to the shipped `47`-strategy roster

### Documentation updates
- Updated `CHANGELOG.md` for repository version `1.4.6`.
- Updated `SETTINGS_REFERENCE.md` with `position_timeframe_overrides`, restored validation/scoring defaults, and corrected live defaults.
- Updated `SETUP_AND_RUN.md` and `README.md` troubleshooting notes for restart/orphan-position recovery.

### Verification
- Broad baseline verification now passes:
  - `python -m unittest discover -s tests -p "test*.py"`
  - Result: `Ran 208 tests ... OK`

## 2026-02-15 - Audit Pass Hardening and Quality Cleanup
### Configuration integrity
- Restored `pipeline.regime_min_val_return_pct` in `config.json` to `5.0` (from `4.0`) to preserve the validated-winner floor policy.

### Live-gate observability
- Enhanced regime live-gate rejection logging in `pm_main.py` to include computed `ret/dd` ratio alongside PF/return/DD values.
- Outcome: faster diagnosis when a candidate fails due to return-to-drawdown inefficiency.

### Code hygiene
- Normalized one non-ASCII cache-section comment in `pm_main.py` to ASCII-safe text.
- Normalized two non-ASCII dash characters in `tests/test_return_dd_ratio.py` comments.

### Verification
- Full regression suite rerun after cleanup:
  - `232 passed, 2 skipped, 11 subtests passed`

## 2026-02-15 - DD/Return Validation Hard Gate
### Validation quality upgrade
- Added hard validation efficiency gate for regime winners:
  - `val_return_dd_ratio = val_return_pct / max(val_drawdown_pct, 0.5)`
  - required: `val_return_dd_ratio >= regime_min_val_return_dd_ratio` (default `1.0`)
- Keeps the `regime_min_val_return_pct = 5.0` floor as policy baseline.
- Gate is applied in both:
  - early candidate rejection in `_select_best_for_regime` (before scoring)
  - final validation in `_validate_regime_winner` (before robustness)
- Weak-train exceptional path now also requires ratio compliance.

### Config and hardening
- Added `pipeline.regime_min_val_return_dd_ratio` to `config.json` with default `1.0`.
- Added `PipelineConfig` field in `pm_core.py`.
- Added config hardening clamp in `PipelineConfig.__post_init__`:
  - invalid/non-finite/non-positive values fallback to `1.0`
- Added finite-safe read path in `RegimeOptimizer` initialization.

### Quality and clarity cleanups
- Updated validation docstring and gate comments in `pm_pipeline.py` to reflect unconditional ratio enforcement.
- Improved candidate rejection summary wording:
  - now reports `trade/drawdown/ratio/profitability` gate coverage
  - rejection count text no longer implies profitability-only rejection.

### Tests and verification
- Added `tests/test_return_dd_ratio.py` (14 tests), covering:
  - 5% return floor interaction
  - ratio gate pass/fail
  - unconditional behavior when `regime_allow_losing_winners=True`
  - weak-train exception ratio enforcement
  - candidate descent behavior when rank-1 fails ratio
  - epsilon stability for near-zero DD
  - config/default hardening behaviors
- Targeted verification:
  - `tests/test_return_dd_ratio.py`: `14 passed`
  - `tests/test_scoring_audit.py::TestConfigHardening` + `tests/test_winners_only.py`: `3 passed`
- Full suite verification:
  - `232 passed, 2 skipped, 11 subtests passed`

## 2026-02-15 - Validation Return Floor Tightening
### Validation gate hardening
- Raised minimum validation return floor for regime winners from `0.0%` to `5.0%`.
- Propagated across:
  - runtime config baseline (`config.json`)
  - pipeline defaults (`pm_core.py`, `pm_pipeline.py`)
  - live-gate fallback thresholds (`pm_main.py`)
- Intent: prevent low-return, high-drawdown candidates from being accepted as validated winners.

## 2026-02-15 - Repository Hygiene and Documentation Polish
### Repository hygiene baseline
- Added root `.gitignore` to prevent generated/runtime artifacts from polluting commits:
  - Python caches (`__pycache__`, `*.pyc`, Numba cache artifacts)
  - virtual environments (`.venv`, `venv`, `env`)
  - test/tool caches (`.pytest_cache`, coverage outputs, static-analysis caches)
  - local workspace folders (`.claude`, `.codex_tmp`, IDE folders)
  - project runtime outputs (`logs/`, `pm_outputs/`, `data/.cache/`, `data/*.csv`)
  - transient PM state logs (`last_trade_log.json`, `last_actionable_log.json`)
- Removed already-tracked runtime artifacts from git index via `git rm --cached` (local files preserved).

### Documentation cleanup
- Normalized ambiguous unicode arrows/emoji to ASCII-safe equivalents in:
  - `README.md`
  - `SETUP_AND_RUN.md`
- Added `Repository Hygiene` section to `README.md` with standard cleanup commands.
- Kept content/meaning unchanged; this is a readability and portability cleanup only.

## 2026-02-15 - Scoring Audit Adoption
Source scope: full adoption of scoring-audit workstreams (A-F).

### Workstream A: Winner Descent Validation (`pm_pipeline.py`)
- Fixed regime winner selection to validate ranked candidates in descent order (top-K) instead of validating only rank-1.
- Added per-rank pass/fail telemetry with validation reasons.
- Added `regime_validation_top_k` wiring from config.
- Outcome: no-winner is emitted only when all attempted candidates fail validation.

### Workstream B: Weak-Train Exception Tightening (`pm_pipeline.py`, `pm_core.py`, `config.json`)
- Tightened weak-train exception gates for candidates with poor train metrics but strong validation:
  - `exceptional_val_profit_factor`: `1.15` -> `1.50`
  - `exceptional_val_return_pct`: raised to `10.0`
  - Added strict gates: `val_drawdown < 0.75 * max` and `win_rate > 50%`.

### Workstream C: Scoring Calibration Extensions (`pm_core.py`)
- Added feature-flagged scoring terms:
  - continuous DD penalty (`scoring_use_continuous_dd`)
  - Sortino/Sharpe blend (`scoring_use_sortino_blend`)
  - tail-risk penalty (`scoring_use_tail_risk`)
  - consistency penalty (`scoring_use_consistency`)
  - trade-frequency confidence bonus (`scoring_use_trade_frequency_bonus`)
- Applied to selection and optimization scoring paths with backward compatibility via flags.

### Workstream C2: Sigmoid Recalibration (`pm_pipeline.py`)
- Recalibrated `_normalize_score` sigmoid from center/scale `80/40` to `45/30` for the new score distribution.

### Workstream D: Regime Bucket Metric Expansion (`pm_pipeline.py`)
- Expanded bucket metrics and mapping to full metrics:
  - `max_consecutive_losses`
  - `calmar_ratio`
  - `mean_r`
  - `worst_5pct_r`
  - `sortino_approx`

### Workstream E: Optuna Objective Alignment (`pm_optuna.py`)
- Added bounded train/validation blend objective support (default `80/20`):
  - `objective_blend_enabled`
  - `objective_train_weight`
  - `objective_val_weight`
- Preserved split-policy windows (`train_pct`, `val_pct`, `overlap_pct` unchanged).

### Workstream F: Telemetry, Tests, Docs
- Added reason-coded optimization summaries and descent path logging.
- Test coverage (`tests/test_scoring_audit.py`): 29 tests.
- Validation results after adoption:
  - scoring-audit tests: `29 passed`
  - full suite: `218 passed, 2 skipped, 11 subtests passed`
- Documentation updates:
  - `README.md`
  - `SETUP_AND_RUN.md`

### Config changes (`config.json`, `pipeline`)
- Added:
  - `regime_validation_top_k`: `5`
  - `scoring_use_continuous_dd`: `true`
  - `scoring_use_sortino_blend`: `true`
  - `scoring_use_tail_risk`: `true`
  - `scoring_use_consistency`: `true`
  - `scoring_use_trade_frequency_bonus`: `true`
  - `optuna_objective_blend_enabled`: `true`
  - `optuna_objective_train_weight`: `0.80`
  - `optuna_objective_val_weight`: `0.20`
- Updated:
  - `exceptional_val_profit_factor`: `1.10` -> `1.50`
  - `exceptional_val_return_pct`: `5.0` -> `10.0`

### Hardening follow-ups
- Added config hardening in `PipelineConfig`:
  - clamp `regime_validation_top_k >= 1`
  - normalize Optuna objective weights to sum to `1.0` (safe fallback `0.8/0.2`)
- Added tests to cover hardening and candidate descent behavior.

## 2026-02-15 - Margin Protection Finalization
### Live guardrail finalization
- Margin protection integrated as cycle-based live guardrail (no margin sleep/cooldown path).
- Config-controlled thresholds:
  - `margin_entry_block_level`
  - `margin_recovery_start_level`
  - `margin_panic_level`
  - `margin_reopen_level`
  - `margin_recovery_closes_per_cycle`
  - `margin_panic_closes_per_cycle`
- Runtime behavior:
  - entry blocking below configured threshold
  - forced deleveraging in RECOVERY/PANIC with loser-first ordering
  - panic fallback to largest-volume closures if no losers exist
- Documentation policy cleanup:
  - policy consolidated into `README.md` and `SETUP_AND_RUN.md`
  - standalone `margin_management.md` removed (de-duplication)
- Validation at finalization:
  - targeted margin tests: `28 passed, 11 subtests passed`
  - full suite: `189 passed, 2 skipped, 11 subtests passed`

## 2026-02-14 - Post-Merge Addendum
### Tier system removal from active live execution
- Live risk execution path made single-path (non-tiered) in `_execute_entry(...)`.
- `fallback_tier` removed from active evaluation/risk sizing/actionable payload writes.
- Trade comments default to non-tier `PM3` encoding for new writes; legacy decoding retained.

### Symbol universe expansion (64 -> 77)
- Added 13 symbols:
  - `GBXUSD`, `BTCXAU`
  - `XAUEUR`, `XAUGBP`, `XAUAUD`, `XAGEUR`, `XRX`
  - `EURNOK`, `EURSEK`, `EURDKK`, `GBPNOK`, `GBPSEK`, `EURTRY`
- Propagated in:
  - `config.json` (`symbols`, `instrument_specs`)
  - `pm_main.py` (`DEFAULT_SYMBOLS`)
  - `pm_dashboard/jobs.py` (`DEFAULT_SYMBOLS`)
  - `pm_core.py` (`INSTRUMENT_SPECS` fallback defaults)
  - `pm_dashboard/analytics.py` (classification/pip handling)

### Strategy set expansion (42 -> 50)
- Added:
  - `VortexTrendStrategy`
  - `ElderRayBullBearStrategy`
  - `TRIXMomentumStrategy`
  - `MarketStructureBOSPullbackStrategy`
  - `LiquiditySweepReversalStrategy`
  - `FibonacciRetracementPullbackStrategy`
  - `FractalSRZoneBreakRetestStrategy`
  - `SupplyDemandImpulseRetestStrategy`
- Added/updated validation coverage:
  - `tests/test_new_strategies_42_50.py`
  - `tests/test_strategy_registry_compat.py`

### Authoritative config baseline (as of 2026-02-14)
- `pipeline.live_risk_multiplier`: `1.0`
- `pipeline.live_max_risk_pct`: `2.0`
- `pipeline.min_trade_risk_pct`: `0.1`
- `pipeline.secondary_trade_max_risk_pct`: `1.0`
- `pipeline.max_combined_risk_pct`: `3.0`
- `pipeline.regime_min_val_profit_factor`: `1.05`
- `pipeline.train_min_profit_factor`: `0.75`
- `pipeline.train_min_return_pct`: `-20.0`
- `pipeline.train_max_drawdown`: `25.0`
- `pipeline.exceptional_val_profit_factor`: `1.10`
- `pipeline.exceptional_val_return_pct`: `5.0`
- `pipeline.actionable_score_margin`: `0.92`
- `position.max_risk_pct`: `2.0`

### Validation snapshot
- Full suite snapshot: `160 passed, 2 skipped`

## 2026-02-12 - Selective Non-Regressive Merge
Target repo: `FxPM 1.4 - backup`  
Source merged: `FxPM 1.4 - Obtimized` (selective merge)

### Summary
- Preserved backup cache copy-safety and legacy compatibility paths.
- Merged live risk realism controls.
- Merged optimization/regime discipline improvements.
- Merged observability hardening while preserving backup dashboard visibility behavior.
- Tuned defaults for return/win-rate/drawdown/reliability/trade volume balance.

### Phase 1: Live Risk Realism
- Added MT5 instrument spec synchronization (`pm_core.py`, `pm_main.py`, `pm_mt5.py`).
- Added symbol-level portfolio risk cap in `_execute_entry(...)` (`pm_main.py`).
- Added min-lot viability protection in `PositionCalculator.calculate_position_size(...)` (`pm_position.py`).

### Phase 2: Optimizer and Regime Discipline
- Added regime warmup labeling (`pm_regime.py`, regime outputs).
- Added regime config invalidation/versioning path (`pm_pipeline.py` ledger checks).
- Added training eligibility gates before expensive tuning.
- Standardized no-validated-winner behavior as no-trade.
- Added weak-train exceptional-validation handling (early version; later tightened in 2026-02-15 scoring audit).
- Added progressive Optuna rejection knobs (`pm_optuna.py`) and applied across objective/fallback paths.

### Phase 3: Observability and Reliability
- Hardened decision throttle persistence with atomic writes.
- Added richer decision context telemetry in execution records.
- Added dashboard parser support for `BLOCKED_SYMBOL_RISK_CAP`.

### Initial config tuning in merge
- Tightened drawdown controls (`max_drawdown`, `fx_val_max_drawdown` to `18.0`).
- Rebalanced risk controls (`tier1_risk_multiplier`, `tier1_max_risk_pct`, `secondary_trade_max_risk_pct`).
- Added pre-tuning gate knobs and weak-train exception knobs.
- Tightened `actionable_score_margin`.
- Aligned dataclass defaults with baseline behavior.

### Merge-era tests
- Added:
  - `tests/test_portfolio_risk_cap.py`
  - `tests/test_position_sizing_edge_cases.py`
  - `tests/test_regime_warmup_exclusion.py`
- Updated:
  - `tests/test_instrument_specs.py`
  - `tests/test_pipeline_config_fields.py`

### Merge-era validation snapshot
- Targeted merge tests: `19 passed`
- Full suite snapshot: `51 passed`
