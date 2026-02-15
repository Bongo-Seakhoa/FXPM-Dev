# FxPM Patch Notes
Scope: Comprehensive change history for this repository.
Status: Canonical patch note file (single source of truth).

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
