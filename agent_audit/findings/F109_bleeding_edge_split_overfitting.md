# F109: Production split favors freshness (no pristine clean holdout) — elevated regime-decay/overfitting risk

## Category
Optimization methodology (red-team)

## Severity
Medium

## Confidence
Medium

## Evidence
- `config.json` — `production_split_mode: "bleeding_edge"`, `clean_holdout_pct: 0.0`,
  `false_discovery_holdout_policy: "off"`, `threadspark_selection_surface: "validation"`,
  `walk_forward_validation_bars: 50000` (adjacent WFV with a 300-bar embargo), `active_stage2_pct: 50`.
- README §"Current State"/§Configuration documents this as intentional: the optimizer uses the freshest
  bars and validates on the adjacent slice rather than a held-out future window.
- Mitigations present: deflated Sharpe (`pm_pipeline.py:3812`), fold profit factor (`:3599`), PBO tooling
  (`tools/pbo.py`), OOS manifests (`pm_outputs/production_live/oos_evidence/`), promotion gate + statuses.

## Explanation
The production profile deliberately trades pristine out-of-sample rigor for live relevance: there is no
newest-tail clean holdout, and selection/validation both sit on recent, adjacent data. This maximizes
freshness but means winners are chosen on the *same recent regime* they will trade. If the regime shifts,
selected winners can decay quickly, and adjacent-slice validation will not have caught it. This is a
defensible design choice for a fast-adapting system, but it raises overfitting/curve-fit risk and makes the
4-week `notify` retrain cadence load-bearing.

## Trading Impact
- Higher sensitivity to regime change between retrains; live edge may decay faster than backtest suggests.
- Combined with F108, demo performance is doubly optimistic (fresh-regime selection + large-notional sizing).

## Suggested Fix or Experiment
1. Keep bleeding-edge for selection but add a **periodic pristine-OOS audit** (a genuinely held-out future
   window, even if not used as a gate) to measure decay between retrains — the PBO/OOS scaffolding exists.
2. Track live-vs-validation metric drift per winner cell (`cycle_drift` already logs cells) and alert when
   live PF/Sharpe diverges from the validation evidence stored in `pm_configs.json`.

## Estimated Effort
Medium

## Risk of Change
Low (adds measurement, does not change selection)

## Validation Plan
Codex: from `oos_manifest_summary.csv`, check whether any pristine-OOS slice exists and summarize
validation-vs-OOS metric gaps for current winners.

## Partner Review
Pending (Codex). Methodology lens — Codex to ground in the OOS/PBO artifacts.
