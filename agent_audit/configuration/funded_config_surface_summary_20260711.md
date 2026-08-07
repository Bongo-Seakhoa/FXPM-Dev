# FxPM funded configuration and complexity audit

Scope: `FxPM-Dev` only. The academic repository was not read from or written to
by this audit. No broker connection, order placement, or optimization run was
performed.

## Result

- Funded `PipelineConfig` JSON surface: **156 allowed keys** out of **314**
  research-compatible dataclass fields. This rejects 158 fields (50.3%) from
  funded JSON unless they receive an explicit contract review.
- Shipped pipeline JSON: **194 -> 156 keys** net. The final scoring/feature
  audit removed 13 additional dead or inactive overrides after the earlier
  calendar/config cleanup; reviewed risk, native-roster, and sealed
  cap-calibration evidence fields were intentionally added.
- Funded `PositionConfig` JSON surface: **6 allowed keys** out of **19** fields.
  Trailing-stop, breakeven, time-exit, and duplicate cost overrides remain
  available to research objects but are rejected in a funded profile.
- Nested `storage` can no longer bypass the funded schema by supplying an
  arbitrary pipeline field. It accepts only 25 storage settings (prefixed or
  natural aliases) and three storage filenames.
- The full current consumer/effect inventory is
  `agent_audit/configuration/funded_config_consumer_map_20260711.csv` (270
  rows, including all shipped section keys, every instrument row, and grouped
  instrument-spec leaf fields).

## Removed funded JSON keys

Research-only fixed-row split inputs, inactive under the native calendar
contract (dataclass compatibility retained):

- `historical_stress_audit_bars`
- `active_universe_bars`
- `active_stage2_pct`
- `overlap_pct`
- `walk_forward_validation_bars`
- `clean_holdout_pct`
- `max_bars`
- `research_mode`
- `tuning_validation_bars`
- `final_lockbox_bars`
- `split_embargo_bars`
- `oos_kfold_splits`
- `oos_embargo_bars`
- `oos_aggregate_policy`

The last six fields belong to legacy fixed-row or purged-k-fold paths. Funded
`calendar_lockbox_v2` uses native timestamp windows, calendar-day purges, and a
fixed one-shot lockbox; retaining row-count controls in its JSON was misleading
and could not improve the native evidence geometry.

Fixed cost and selection invariants, whose safe defaults are now tested:

- `use_spread` (fixed `true`)
- `use_commission` (fixed `true`)
- `use_slippage` (fixed `true`)
- `scoring_use_trade_frequency_bonus` (fixed `false`)
- `local_governance_tournament_enabled` (fixed `false`)
- `local_governance_live_mode` (fixed `off`)
- `risk_management_optimization_enabled` (fixed `false`)

Dead funded selector controls removed in the final pass (the uninstantiated
legacy `StrategySelector`/`HyperparameterOptimizer` classes and research
dataclass fields remain available for historical tests):

- `max_param_combos`
- `min_trades`
- `fx_opt_min_trades`
- `fx_val_min_trades`
- `fx_selection_top_k`
- `fx_opt_top_k`

Inactive model/policy overrides removed from the shipped funded profile:

- `legacy_confidence_bonus_cap`
- `threadspark_selection_surface`
- `threadspark_use_historical_cost_evidence`
- `threadspark_historical_cost_scope`
- `threadspark_historical_chunk_count`
- `promote_small_risk_multiplier`
- `oos_min_trades`

`regime_scoring_model=legacy` is now a funded schema/runtime invariant because
the retained experiment record shows the alternate spoke, asymmetric, v2,
ThreadSpark, edge-trend, and meta-label selectors failed the honest per-cell
acceptance bar. `oos_validation_mode=fixed_holdout` is likewise fixed for the
native two-phase lockbox. Both alternatives remain research-constructible.

Disabled operational branches that do not belong in the funded profile:

- `feature_disk_cache_precompute_enabled`
- `feature_disk_cache_precompute_workers`

Reviewed no-op or report-only knobs:

- `walk_forward_audit_mode`: normalized but never read by audit construction,
  promotion, readiness, or live execution.
- `walk_forward_do_not_displace_latest_surface`: wrote a descriptive evidence
  boolean but did not change selection or promotion.
- `false_discovery_scope_power`: fingerprinted but not used by DSR, PBO,
  bootstrap, scoring, or promotion.
- `false_discovery_symbol_count`: only populated a reported scope multiplier;
  the runtime already derives the actual symbol count when absent, while the
  statistical gates use actual attempted trials and candidates.

Mandatory funded walk-forward policy, fixed to tested defaults:

- `walk_forward_audit_enabled=true`
- `walk_forward_promotion_policy=promotion_gate`
- `walk_forward_live_blocking_enabled=true`

Readiness now independently rejects weakened walk-forward objects even if one
is constructed outside the strict funded JSON loader.

Mandatory funded position geometry, also removed from JSON and fixed to tested
defaults:

- `auto_widen_sl=false`
- `allow_min_lot_risk_clamp=false`
- all `regime_tp_multipliers=1.0`

## Decision-path corrections

1. Candidate descent no longer stops after
   `regime_validation_top_k`. Every candidate in that list has already been
   evaluated and scored on tuning validation, so the cap saved no expensive
   work and could return `NO_TRADE` even when rank 6 was the first candidate to
   pass final generalization gates. A regression proves ranks 1-5 can fail and
   rank 6 is selected. The field remains research-compatible only for
   ThreadSpark's explicit L1 shortlist and is rejected by funded JSON.
2. ThreadSpark evidence attachment calls are made only when ThreadSpark is the
   configured selector. The helper already had an internal early return, so
   this is a code-path clarity reduction rather than a claimed backtest-speed
   improvement.
3. Funded live position spread/slippage inputs are synchronized from the
   resolved backtest cost contract. Research profiles retain independent
   PositionConfig behavior.
4. Non-finite candidate scores and malformed core metric surfaces now fail
   before sorting; a NaN score can no longer retain insertion-order rank or be
   normalized into a maximum quality score.
5. OOS and execution-cost evidence is now replayed with the candidate's exact
   regime entry gate, and candidate identity includes regime. Same-parameter
   specialists in different regime cells can no longer overwrite one another.
6. Exact-cell PBO now compares the complete dated return axis, counts unique
   effective candidate IDs, and fails closed on corrupt/ruin observations.

## Remaining maintainability risk

The safety-preserving cleanup deliberately did not attempt a last-minute
rewrite of the large execution and optimization monoliths. Static structure at
audit time includes:

- `pm_main.py::_execute_entry`: approximately 2,100 lines.
- `pm_core.py::PipelineConfig.__post_init__`: approximately 1,100 lines.
- `pm_pipeline.py::run_for_symbol`: approximately 594 lines.
- `pm_core.py`: 7,521 lines; `pm_main.py`: 8,460 lines; `pm_pipeline.py`:
  10,195 lines.

These are real maintenance risks, but decomposing them without a full
behavioral characterization suite would add production risk rather than
improve mathematical accuracy. The strict funded profile, readiness checks,
and focused regression tests reduce their reachable production state space in
this release. A later refactor should first freeze golden traces for signal
selection, position intent, pre-send checks, and ledger persistence, then
extract pure staged functions without changing policy.
