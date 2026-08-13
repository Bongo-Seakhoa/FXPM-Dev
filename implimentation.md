# FxPM Split Implementation Audit

## Verdict

The production split was materially misaligned with the intended FxPM operating model. The prior clean-holdout implementation protected an academic OOS surface, but it did so by withholding the freshest market-adjacent bars from production optimization. For an FX system whose optimized strategies are expected to decay over time, that is the wrong production tradeoff.

The intended production logic is sound:

- Load the latest 300,000 M5 bars.
- Reserve the oldest 50,000 of that loaded window for `historical_stress_audit`.
- Treat the newest 250,000 as the active production universe.
- Use the full active 250,000 for broad/base strategy eligibility.
- Optimize/final-select on the newest active half, 125,000 bars, ending at the bleeding edge.
- Use the immediately prior 50,000 bars as adjacent walk-forward validation.
- Keep production `clean_holdout` empty/disabled.

The caveat is terminology: the prior 50,000-bar validation window is not pristine forward OOS if Stage 1 eligibility can see the full active universe. It is still useful as an adjacent resilience audit, but it must not be sold as a clean unbiased forward estimate.

## Independent Audit Summary

Three read-only sub-audits independently agreed on the core direction.

- Methodology audit: production should not withhold the freshest 15% as hard clean holdout in a decaying M5 FX strategy stack. The correct posture is bleeding-edge optimization plus adjacent resilience validation and live monitoring.
- Code-impact audit: this is not config-only. `DataSplitter`, pipeline slicing, and optimizer candidate collection all had to change because the old code could not express "full active eligibility plus newest-half optimization."
- Test/documentation audit: docs and tests were split-brained. Some docs described the practical active workflow, while config/tests enforced `2026-06-04-disjoint-stage2-validation`. Artifacts must be invalidated through a split contract bump.

## Current-State Misalignment

Before this fix, active `config.json` encoded:

- `active_stage2_pct = 58.823529`
- `clean_holdout_pct = 15.0`
- `threadspark_selection_surface = "clean_holdout"`
- `false_discovery_holdout_policy = "block"`

For a 300,000-row M5 source, that produced approximately:

| Window | Rows | Role |
|---|---:|---|
| `historical_stress_audit` | 50,000 | Older stress audit |
| `active_universe` | 250,000 | Claimed active universe |
| `optimization_train` | 102,641 | Older active segment, not bleeding edge |
| `stage2_training_selection` | 124,700 | Validation/training-selection segment |
| `clean_holdout` | 22,059 | Freshest bars withheld from production optimization |

That meant the optimizer was not actually using the newest market-adjacent slice as the final production training surface. The clean holdout improved research honesty but degraded production freshness.

## Implemented Production Contract

The production contract is now `production_split_mode = "bleeding_edge"`.

For exactly 300,000 M5 rows with `oos_embargo_bars = 300`, the intended geometry is:

| Window | Index range | Rows | Role |
|---|---:|---:|---|
| `historical_stress_audit` | `0..50000` | 50,000 | Older severe-fragility audit |
| `active_universe` | `50000..300000` | 250,000 | Production active universe |
| `base_selection` | `50000..300000` | 250,000 | Broad/base strategy eligibility |
| `walk_forward_validation` | `124700..174700` | 50,000 | Adjacent resilience validation |
| embargo gap | `174700..175000` | 300 | Leakage buffer |
| `optimization_train` | `175000..300000` | 125,000 | Bleeding-edge optimization/final selection |
| `clean_holdout` | `300000..300000` | 0 | Disabled for production |

The old clean-holdout design is retained only as explicit `production_split_mode = "research_clean_holdout"` behavior.

## Code Map

- `config.json`: production split is explicit with `production_split_mode="bleeding_edge"`, `walk_forward_validation_bars=50000`, `active_stage2_pct=50.0`, `clean_holdout_pct=0.0`, and `false_discovery_holdout_policy="off"`. Scoring is now a fixed canonical code path rather than a configurable ThreadSpark surface.
- `pm_core.py`: `PipelineConfig` normalizes the new split fields. `DataSplitter.SPLIT_CONTRACT_VERSION` was bumped to invalidate stale ledgers/artifacts, and `DataSplitter` now emits `base_selection`, `walk_forward_validation`, `optimization_train`, and empty production `clean_holdout`.
- `pm_pipeline.py`: artifact metadata includes the split mode and walk-forward validation bar count. Pipeline slicing now passes separate full-active eligibility features, latest-half optimization features, and prior-window validation features.
- `RegimeOptimizer`: candidate collection now uses the broad eligibility surface for screening/gates and top-K strategy discovery, while stored train metrics and tuning remain on the bleeding-edge optimization surface.
- Promotion logic: clean holdout is replaced by WFV as the production promotion evidence surface. Clean WFV promotes, weak or thin WFV derates to `PROMOTE_SMALL`, and severe WFV blocks promotion; live performance and drift governance can then derate or pause contexts as real trade evidence accumulates.
- Tests/docs: splitter, config, artifact, promotion, optimizer, README, settings, and research docs were updated to lock the new production contract and demote clean holdout to research mode.

## Recommendation

Use the bleeding-edge split as the production default. It best matches the stated objective: live strategies should be optimized on the freshest available market context, with validation focused on detecting severe fragility rather than withholding the exact data most relevant to the next live trades.

Keep clean holdout only for research comparisons. It is useful for academic honesty and model-development diagnostics, but it should not be the default production gate for a live FX optimizer unless future evidence shows that overfit risk dominates recency decay.

Reject the previous clean-holdout split as production default. It was not just a naming issue; it changed the data geometry and made the deployed optimizer stale relative to the live market.

## Acceptance Criteria

- `DataSplitter` emits the exact 300k bleeding-edge geometry listed above.
- `config.json` no longer reserves production clean holdout.
- Optimizer screening can use full active 250k eligibility while train metrics remain on the newest 125k.
- Production promotion uses WFV evidence instead of production clean-holdout evidence.
- Artifact metadata rejects old `2026-06-04-disjoint-stage2-validation` outputs.
- Targeted tests pass for splitter, config, artifacts, optimizer gating, promotion, and holdout diagnostics.

## Operational Note

The prior 50,000-bar validation window should be described as adjacent walk-forward validation evidence. It participates in candidate validation and can identify obvious brittleness, but it is not a pristine forward holdout because the broader Stage 1 eligibility process is allowed to inspect the full active universe.
