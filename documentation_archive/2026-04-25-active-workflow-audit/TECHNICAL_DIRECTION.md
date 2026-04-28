# FxPM 1.4 â€” Technical Direction & Governing Philosophy

This document is the companion to [findings.html](findings.html). The findings document tells you **what is currently wrong or weak**; this document tells you **how to decide** when you face a change that the findings document did not anticipate.

If the two conflict, the findings are tactical and time-bound; this document is strategic and should change slowly. When a future decision is not covered by either, default to the decision rules in Â§1 and the review checklist in Â§9.

*Revision note (2026-04-21):* this document has been updated in response to a second-pass reviewer pass on two points that matter for decision-making: decision rule Â§1.5 now scopes "risk-budgeted-forward" only to the risk caps that are actually enforced (per-trade and same-symbol); architectural invariant Â§3.5 now describes `pm_configs.json` as an atomic-write ledger updated in place, not an append-only log. Long-horizon direction Â§11 has been expanded with the holistic review's structural recommendations (items 6â€“10), notably the `OrderGovernanceEngine` subsystem. The `OrderGovernanceEngine` direction was further narrowed to a measured first cohort after a direct audit of `fxpm_order_management_poc.ipynb` (control-arm parity verified against `Backtester.run`; per-arm metrics re-read from the notebook results table, not taken from intermediate documents). This revision also corrects a deeper philosophical drift: FXPM is a broad competitive discovery engine, not a curated shortlist, and any pool-pruning or search-space-management recommendation must prove that exact `(symbol, TF, regime)` tournament fairness is preserved.

## 0. Foundational identity (corrected)

FXPM is a **broad competitive discovery engine**. Its job is to discover, through a wide and fair tournament, the best available strategy and parameter expression for each exact `(symbol, TF, regime)` combination.

This means:

- There is **no single symbol winner**. A winner exists only at exact local granularity: symbol x timeframe x regime.
- Strategy diversity is a **discovery asset**, not a nuisance to be reduced by default.
- Search-space breadth is **not inherently a defect**. Breadth becomes a problem only when compute policy distorts competitive fairness or produces scientific invalidity.
- Efficiency is **subordinate to truthful discovery**. We optimize compute only after preserving tournament integrity.
- Governance layers that act **after winner discovery** are acceptable: holdout gating, live allocation, order governance, and other downstream controls.
- Governance layers that leak backward into the training tournament are unacceptable unless they can show they preserve competitive fairness.
- Fewer, tidier, or more maintainable strategies are **not** assumed to be better by default. Pool reduction, family merging, or hard parameter restrictions require evidence that exact local winner outcomes remain materially unchanged.

The earlier drift corrected by this section is explicit:

- FXPM is **not** moving from "47 strategies toward 35 high-quality strategies" as an inherent good.
- New strategies are **not** required to fill a regime-coverage hole before they are allowed to compete.
- A strategy does **not** need to win across many symbols or regimes to justify its existence.
- Correlation or structural similarity does **not** automatically justify merging or pruning.
- Large parameter spaces such as EMARibbonADX are **not** to be hard-preconditioned by default; fair-search governance comes first.
- Strategy-family merging and strategy-count reduction are **not** objectives unless fairness-preserving evidence supports them.

---

## 1. Decision rules (the ten)

These are not aspirations. They are tests that every proposed change â€” code, config, strategy, deployment â€” must pass. A change that fails any of them gets reworked or rejected.

1. **Correctness is a precondition, not a feature.**
   Any code path that privileges speed, elegance, or convenience over deterministic causal behavior is rejected even if faster or cleaner. Numba-accelerated paths must match their Python reference to floating-point parity; live paths must match backtest to the same tolerance after accounting for real-spread and slippage.

2. **Config is a contract between operator and strategy.**
   If code defaults tighten (safer), committed config must catch up before the next live session or the looser config is treated as a defect (see [findings.html Â§3](findings.html#config-drift)). The inverse â€” code default loosens while config stays strict â€” is acceptable and does not require immediate alignment.

3. **Winners-only means no fallback, ever.**
   "No trade" is a first-class outcome with dedicated logging, metrics, throttle behavior, and observability. Any code path that substitutes a "best-available" candidate for a validated winner is rejected, even in edge cases ("the winner is stale," "no winner exists for this regime today"). The correct response to "no winner" is `NO_ACTIONABLE_*`.

4. **Regime is a hard filter, not a hint.**
   A strategy validated as a winner in TREND does not trade in CHOP even if its signal happens to fire. Regime transitions are explicit state changes, hysteresis-governed, and observable in the live log. Soft-blending, probability-weighted, or ensemble-of-regimes selection is out of scope for this system.

5. **Risk is budgeted forward, not reconciled backward.**
   Risk caps that actually exist today â€” per-trade (`position.max_risk_pct`) and same-symbol combined (`max_combined_risk_pct`) â€” are checked at decision time against geometry-derived exposure (entry, SL, volume). Comment-tag metadata is informational, not authoritative â€” if decode fails, fall back to geometry, never to "assume it was fine." Portfolio-wide and daily/session caps are not currently enforced (see [findings Â§13.3](findings.html#strategic) for the direction); until they exist, the rule applies only at the scopes that do.

6. **Backtest and live must be indistinguishable to the strategy.**
   Any divergence in spread handling, slippage model, bar-close timing, same-bar gap-through-stop, or signal evaluation is a debt. Debts are paid off, not rationalized. The only acceptable live-vs-backtest drift is what is modeled (spread overlay, broker lot step) or logged (actual spread at fill).

7. **Signals fire on closed bars.**
   Anything else is look-ahead. `live_bar_settle_seconds` is a feature, not a delay â€” it guards the causal boundary between bar-close and decision. Re-evaluating the same bar on a later tick is a bug.

8. **Generalization beats optimization.**
   The robustness ratio gate, the train/val blended objective, the weak-train rescue thresholds, and the variance penalty all answer the same question: *does this survive the next regime?* If a proposed change lifts train Sharpe by 0.3 but loses 0.1 on val, reject it.

9. **Selection is a fair tournament at exact local granularity.**
   Winners are decided per `(symbol, TF, regime)`, stored in `pm_configs.json`, and retrained on a calendar anchor. There is no global symbol winner, no preferred family, and no justification for narrowing the field before discovery unless fairness is shown to be preserved. The tournament runs at retrain time; the live loop is an executor.

10. **Observability is execution's insurance.**
    Every blocked entry has a reason string. Every margin-state transition has a log line. Every deviation from modeled behavior (widened SL, min-lot drift, stale cache, fallback pip-value) is surfaced at INFO or WARN, not swallowed at DEBUG.

---

## 2. What the system is (and is not)

### It is

- A **winners-only, regime-gated, calendar-retrained** FX portfolio manager.
- A **config-driven** system where `config.json` is the authoritative operator interface.
- A **deterministic** system: seed-stable Optuna, atomic ledger writes, frozen regime labels at bar-close.
- A **broad strategy pool across 4 regimes** where diversity is a discovery asset and similar-looking competitors are allowed to coexist unless evidence shows they do not change exact local winners under fair search.

### It is not

- **Not** a machine-learning system. Strategies are rule-based with tunable parameters; Optuna is a hyper-parameter optimizer, not a model trainer.
- **Not** a high-frequency system. The minimum decision granularity is the closed bar of the lowest live timeframe (M5 on the data layer, M15+ on the signal layer).
- **Not** an ensemble system. Regime selects one winner per (symbol, TF, regime); winners do not vote. A secondary strategy only exists per the explicit secondary-trade plumbing and is risk-budgeted separately.
- **Not** a hand-curated shortlist. New strategies are not required to fill a regime-coverage hole or to win broadly across symbols to justify their existence. Experimental strategies may be flag-gated operationally, but admission and retirement decisions must preserve tournament fairness rather than optimize for tidiness.

---

## 3. Architectural invariants

These are structural guarantees the codebase must preserve. Violating one of these should require a conscious decision and a test-suite update, not a drive-by refactor.

1. **Three scoring layers, three distinct questions.**
   - Optuna TPE â†’ "Given a parameter space, what are promising points?"
   - Pipeline `fx_generalization_score` â†’ "Given a candidate set, which survive the trainâ†’val gap?"
   - Live winners-only â†’ "Of the validated winners, which fires on this bar right now?"

   These must stay separated. Folding Optuna into live selection (or live gating into Optuna) is rejected.

2. **Regime labels are frozen at bar-close and shifted for decisions.**
   `REGIME_LIVE = REGIME.shift(1)` is non-negotiable. Any code path that reads the current bar's label for the current bar's decision is look-ahead.

3. **Data splits have causal ordering.**
   Train â†’ Val â†’ (Test). Overlap on train/val is a soft look-ahead for indicator warmup only; never extend overlap to improve val scores.

4. **`PipelineConfig` is the single source of truth for runtime parameters.**
   `_filter_dataclass_kwargs` strips unknown keys silently â€” this is deliberate (it protects against stale config), but it means every new config key requires a matching dataclass field. No "quietly added via getattr" parameters.

5. **`pm_configs.json` is an atomic-write ledger at the retrain boundary.**
   Per-`(symbol, TF, regime)` entries are updated in place via atomic replace â€” the file is not append-only, and stale or losing winners are overwritten rather than retained as history. Interrupted runs must leave the file in its prior state. The ledger is the ground truth for "who the current winners are." Historical winner lineage, if needed, lives in `pm_outputs/` artifacts (training manifests, signal ledger), not in `pm_configs.json` itself.

6. **MT5 is a boundary, not a dependency.**
   All MT5 calls must have a fallback path that returns a well-typed result or an explicit error. No MT5 primitive return value should crash a live loop.

---

## 4. Quality gates â€” what "shippable to live" means

A change is shippable to live when all of the following are true. These are ordered; failing an earlier gate short-circuits the later ones.

1. **Tests pass.**
   `python -m unittest discover -s tests -p "test*.py"` reports `OK` with zero failures, zero errors.

2. **No config-vs-code drift.**
   For every key in `config.json`, the value is at least as strict as the `PipelineConfig` default. See [findings.html Â§3](findings.html#config-drift) for the current audit.

3. **No orphaned outputs.**
   If the change writes a file, at least one reader exists on the consuming path and is exercised by tests. `regime_params.json` is the cautionary tale â€” written by the tuner, not read by the pipeline until explicitly wired.

4. **Backtest parity holds.**
   On the winners set, live behavior must match backtest to within the documented spread/slippage envelope. Diverging behavior is investigated before ship.

5. **Observability budget met.**
   New code paths have log lines at WARN or INFO for every material decision point. No silent fallbacks, no swallowed exceptions.

6. **Paper-run week.**
   For changes that touch the live loop, position sizing, risk, or regime selection, one full trading week of paper-run observation is the default before flipping to live.

---

## 5. How to propose a new strategy

Strategies are discovery competitors, not slots in a tidy catalogue. Carry cost is real, but it is subordinate to truthful local discovery.

A new strategy is accepted into the pool when:

1. **It has a clear, implementable thesis that can compete at exact `(symbol, TF, regime)` resolution.** Broad coverage is welcome, but not required.

2. **Its search budget can be integrated without hidden bias.** If compute is tight, solve with staged search, family-level budget allocation, matched-budget experiments, or justified larger compute budgets rather than arbitrary exclusion.

3. **It composes with the execution and risk contract, or clearly declares the new seam required.** The R-multiple, widen-on-viability, regime-aware TP, and any governance hooks must remain technically sound.

4. **It has a written thesis.** One paragraph, in the strategy's docstring: what edge, what regime, what failure mode. No anonymous strategies.

5. **It does not need to win broadly to justify existence.** One repeatable local edge can be sufficient if it survives fair retraining.

Similarity and pruning rules:

- Structural similarity is an investigation trigger, not an eviction rule.
- Similar-looking strategies may still produce different exact local winners.
- Merge or prune only after an invariance audit shows the change does not materially alter exact local winner outcomes under fair search.

A strategy is evicted from the pool when:

- It wins on zero exact `(symbol, TF, regime)` combinations after repeated fair retrains with adequate search budget and data coverage.
- An invariance audit shows it is non-contributory: removing or merging it does not materially change exact local winners under matched search conditions.
- Its code has unresolved numerical warnings (division-by-zero, NaN-propagation) that cannot be fixed in < 50 lines.

---

## 6. How to propose a config change

1. **State the code default.** If your proposed value is looser than the code default, you are weakening the system; the bar for acceptance is high.

2. **State the expected PnL impact.** Bucket it: "winner quality," "expectancy," "drawdown tail," "signal frequency." Vague expected impact â†’ rejected.

3. **Identify the side channel.** Every config change has at least one unintended consequence. `regime_min_val_return_pct: 5.0 â†’ 0.0` widens the winner pool (intended) but also lets break-even regime winners through (side channel). Name it.

4. **Couple with a paper week.** Config changes do not go to live on the same day they are committed.

5. **Prefer two strict changes to five loose ones.** The challenge-profile temptation is to knob-twist ten things by 10 %; the disciplined alternative is to move two things decisively.

---

## 7. What we owe the operator (and what we don't)

### We owe

- A single, authoritative configuration interface (`config.json`) with documented defaults ([SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md)).
- A single, authoritative live log with reason strings on every decision.
- Deterministic optimizer runs: same seed, same data, same winners.
- Atomic ledger state: an interrupted session does not corrupt `pm_configs.json`.
- A dashboard that reads current state and displays it honestly, including when state is degraded (stale winners, MT5 disconnected, margin recovery active).

### We do not owe

- **A single-click answer.** Operators are expected to read the reason strings and act on them, not to receive a "trade/don't-trade" boolean.
- **Arbitrary config flexibility.** If a key is not in `PipelineConfig`, it is not a knob. Ad-hoc environment variables are not a config surface.
- **Retroactive explanations.** The system tells the operator *why* it blocked at the moment of blocking. It does not reconstruct "why didn't we trade on Tuesday" from archival state.

---

## 8. Change-control categories

Every change falls in one of five buckets. The category determines the review bar.

| Category | Examples | Review bar | Requires paper week |
|---|---|---|---|
| **Tightening** | Stricter config default, narrower gate, added validation | Code review + test pass | No |
| **Loosening** | Wider config, relaxed gate, new fallback | Code review + test pass + paper week + documented side channel | Yes |
| **New feature** | New strategy, new enhancement seam, new regime dimension | Code review + tests + flag-gated rollout + paper week | Yes |
| **Structural refactor** | Module split, dataclass change, parameter rename | Code review + tests + migration path + backtest-live parity check | Depends |
| **Correctness fix** | Bug fix, parity fix, causal-leak fix | Code review + regression test + documented in CHANGELOG | No (deploy immediately) |

"Correctness fixes" bypass the paper-week requirement because holding them back is itself a safety cost. "Loosening" requires a paper week because its failure mode is quiet PnL degradation, not a stack trace.

---

## 9. Pre-merge review checklist

Before any change to the live path, the reviewer confirms:

- [ ] Passes decision rules Â§1.
- [ ] Tests pass with `python -m unittest discover -s tests -p "test*.py"`.
- [ ] No tournament contamination: any search-space narrowing, family-level budget rule, or pruning change is either backed by evidence that exact local winner outcomes are preserved or is explicitly marked as an experiment.
- [ ] No new orphaned outputs (every written file has a reader on a live path).
- [ ] No new silent fallbacks (no bare `except`, no swallowed `None`, no DEBUG-level fallback logs where INFO is correct).
- [ ] No new config-vs-code drift (every new `PipelineConfig` field has a committed `config.json` entry â‰¤ code default, or is opt-in).
- [ ] Documentation sync: if behavior visible to the operator changed, [README.md](README.md), [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md), or [SETUP_AND_RUN.md](SETUP_AND_RUN.md) is updated in the same commit.
- [ ] [CHANGELOG.md](CHANGELOG.md) entry under the current 1.4.x bullet set.

---

## 10. Known anti-patterns (do not reintroduce)

These patterns existed in earlier versions of the codebase and were explicitly removed. Reintroducing them â€” in any form â€” is a regression.

1. **"Best-train fallback."** Deprecated in 1.4.4. If validation fails, there is no trade. Resist the urge to re-add a "tier 2" or "backup winner" concept. [findings Â§3 rule 3.]

2. **`--auto-retrain` rolling path.** Deprecated in 1.4.5. Retraining is calendar-anchored; a live loop does not retrain itself. Do not reintroduce retrain-on-drift, retrain-on-schedule-from-live, or any path where the live loop modifies `pm_configs.json`.

3. **Freshness-decay carry-over.** Closed in 1.4.6. Stale higher-TF winners must not be kept alive across bars by decay weighting. Winners live or die on "is this bar closed and due?"

4. **Feature-flag cruft.** A flag is either rolled out or rolled back; indefinitely half-enabled flags become a correctness hazard. Schedule flag removal at the PR that adds the flag.

5. **"Quiet config shadowing."** Duplicate JSON object keys, nested `storage.*` sections not normalized to `PipelineConfig` keys, or ad-hoc `os.environ` reads. All are correctness hazards because they create a gap between "what the operator wrote" and "what the system sees."

6. **`order_calc_profit` fallback silence.** When MT5 returns `None` for pip-value, the system must log WARN, not DEBUG. Silent under-sizing is worse than an explicit fallback.

---

## 11. Long-horizon direction

These are directions the system is moving, not decisions that have been made. They are listed here so incremental changes stay aligned. Items 1-5 are the original intra-module directions; items 6-10 reflect the later holistic review and the corrected operating philosophy summarized in [findings.html Section 13](findings.html#strategic).

1. **From per-trade risk to portfolio-vol targeting.** The risk-scalar stack exists and is correct; the direction is to enable it (shadow â†’ live, with floor 0.5 and ceiling 1.0) and let volatility-target become the primary sizing regulator.

2. **From regime-blind TP to regime-conditional TP.** The same R-target is currently applied across TREND, RANGE, BREAKOUT, CHOP. The direction is per-regime TP multipliers, composing with the existing regime-aware strategy selection.

3. **From spread-overlay as an afterthought to spread-overlay as a first-class regime dimension.** The execution-quality overlay is a penalty today; the direction is to treat spread as a covariate of the regime system (high-spread regimes have different winners, not just the same winners rejected more often).

4. **From blunt pool compression to fairness-preserving search governance.** The direction is not to shrink `47` toward a tidier number by default. The direction is to keep broad competition while governing compute more intelligently: staged search, family-level budget controls, matched-budget ablations, and larger search budgets where justified. Pruning or family merging is acceptable only after evidence shows exact local winner outcomes are preserved and tournament fairness is not degraded.

5. **From single-account to multi-account governance.** Not a near-term goal, but the config and ledger structure should stay compatible with a future where `pm_configs.json` is namespaced per account.

6. **From retraining cadence alone to retraining cadence plus structured review.** The fixed two-week retraining cycle remains the intended freshness anchor. The direction is not automatic mid-cycle fail-closed shutdown for presumed decay; it is stronger pre-cycle hygiene, weekly in-cycle review, and end-of-cycle refresh so the cadence is managed deliberately without overriding the active live window.

7. **From holdout-as-report to holdout-as-structured-review-context.** Pipeline artifacts carry holdout metrics today, but they are not surfaced cleanly into review and retrain prioritization. The direction is to persist per-winner holdout context, review flags, and retrain priority so generalization evidence informs the next cycle without becoming a hidden mid-cycle kill switch.

8. **From thin portfolio visibility to light-touch orchestration.** The live loop today evaluates symbols one at a time and enforces per-trade and same-symbol risk only. The direction is first better visibility and sequencing when many local winners fire together, not a heavy umbrella allocator that routinely suppresses productive capital. Any account-level restriction should remain exceptional rather than the default governance model.

9. **From unmanaged trades to a local `OrderGovernanceEngine` as a second tournament after discovery.** Today, once a trade is filled, broker-side SL/TP own most of its lifecycle. Trailing, breakeven, and time-exit logic exist in `pm_position.py` and `MarketDrivenExitPack` but are not bound into a live supervisor. The direction is a dedicated governance layer with explicit arms (`G0_CONTROL` -> `G6_PARTIAL_PLUS_TRAIL`), deterministic per-fill arm assignment, durable per-position state keyed by MT5 ticket, and a staged validation ladder (offline replay -> shadow live -> controlled local A/B). Strategy selection stays untouched; governance begins *only after* a trade is filled and is selected for the exact local winner context rather than imposed globally. This is the single largest structural addition on the roadmap.

   *Empirical narrowing (`fxpm_order_management_poc.ipynb`, audited 2026-04-21):* the notebook's `control_fixed` arm asserts byte-for-byte parity against `Backtester.run` and ran 14 governance arms on `EURUSD` and `NZDJPY` H4 (KaufmanAMATrendStrategy). The strongest arm on both sampled local contexts was `atr_trail_capped` (EURUSD PF 1.74 -> 2.22, return 15.64% -> 20.51%, DD 6.43% -> 3.58%; NZDJPY PF 1.16 -> 1.65, return 4.01% -> 15.99%). `breakeven_1r` was the credible second; `r_ladder_lock` was numerically identical to `breakeven_1r` on this sample; `volatility_shock_exit` was numerically identical to control. The correct reading is local narrowing, not a PM-wide governance decree. The first production-test cohort is therefore `atr_trail_capped` then `breakeven_1r` on sampled contexts; broader contexts must prove themselves locally. Random-entry sanity controls remain required before any price-path overlay is promoted to live (see [findings Section 13.6](findings.html#strategic)).

10. **From opaque strategy contract to `TradeIntent`.** A longer-horizon step that only becomes valuable once items 8 and 9 are live: strategies emit a typed `TradeIntent` carrying side, confidence, expected edge after cost, holding horizon, stop/target geometry, sizing bounds, and governance hints. The goal is not to centralize policy under one umbrella brain; it is to help the PM backbone carry many specialized local winners more cleanly. Adopt via an adapter so existing strategies continue to work.

---

## 12. Definitions (terms that mean something specific here)

- **Winner**: A strategy-parameter tuple that passes all three scoring layers (Optuna, pipeline generalization, regime-aware validation) for a specific (symbol, timeframe, regime) as of the last retrain. Stored in `pm_configs.json`.

- **Regime**: One of {TREND, RANGE, BREAKOUT, CHOP}, computed per bar by `MarketRegimeDetector.compute_regime_scores()` with hysteresis. Labels are frozen at bar-close; decisions use `.shift(1)`.

- **Actionable**: A live entry signal that has passed the winners-only gate, the regime gate, the spread-quality gate, the margin gate, and the risk-budget gate. Actionable signals are logged to both `last_actionable_log.json` and `pm_outputs/signal_ledger_YYYYMM.jsonl`.

- **Challenge profile**: A `config.json` tuned for a prop-firm / evaluation account. The repo's committed `config.json` is a challenge profile. Challenge profiles should be *tighter* than code defaults, not looser.

- **Enhancement seam**: A code point (in `pm_enhancement_seams.py`) where optional scalars, overlays, or exit models can be plugged in without changing the live loop. Seams are disabled by default and rolled in behind explicit flags.

- **Viability**: The condition `lot_at_risk â‰¥ broker_min_lot AND risk_usd â‰¥ min_trade_risk_usd`. Non-viable trades are either widened (see [findings Â§8.1](findings.html#position)) or rejected.

- **Parity**: The invariant that backtest and live produce identical entry/exit decisions for the same bar, same spread, same slippage model, same regime label.

---

*This document, like the system, is intended to change slowly. Edit it when a decision rule is violated often enough that the rule needs revisiting â€” not when a new feature makes the rules inconvenient.*

*Companion file: [findings.html](findings.html).*

