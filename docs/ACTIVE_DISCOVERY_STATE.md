# Active Discovery State

> 2026-06-09 production split note: current production uses `production_split_mode="bleeding_edge"`.
> The newest active half is the optimization surface, the immediately prior 50k bars are adjacent
> WFV evidence, and production clean holdout is empty. Use `docs/PRODUCTION_SPLIT_AND_PROCESS.md`
> and `docs/audit/production_split_process_20260609.html` as the canonical split/process references.
> Older clean-holdout/disjoint material below is historical or research-only.

> 2026-06-06 current note: scoring discovery is closed. Production scorer is `legacy`
> with trade-frequency bonus off; see `docs/PROGRAM_PLAN.md` for the canonical state.
> First-pass spent report/debug tools are archived under
> `tools/archive/scoring_discovery_20260606/`.

> 2026-06-05 note: the active PM tracker for the current candidate-pool edge-persistence
> test is `docs/PROGRAM_PLAN.md`. This file still preserves earlier run-state context,
> but if it conflicts with `PROGRAM_PLAN.md`, use `PROGRAM_PLAN.md` plus `AGENT_COMMS.md`
> as the live coordination source until this document is fully refreshed.

Last updated: 2026-06-04 (asym VERDICT recorded — run complete + REJECTED)

> 2026-06-07 guardrail note: the generated W5 completion report for
> `production_book_legacy_freqoff_20260606_resume_w5` is stale if read as "6 clean/promotable"
> production-book cells. The post-fix readiness result is `PROMOTE=4`, `REVIEW=10`, `REJECT=293`;
> unsafe `PROMOTE` cells: `0`.

> 2026-06-07 production-readiness note: a production live book has been generated from the corrected
> W5 gatefix artifacts and consolidated into the universal runtime files. Current production config:
> `config.json`; production ledger: `pm_configs.json`.

Current correction: v2 r3 has been operator-stopped and rejected as implemented; the active lane is assessment reset, engine/OOS repair, and true ThreadSpark evidence-geometry design.

This is the compact source of truth for the current discovery run and scoring alignment. If this file conflicts with older audit notes, preview HTML, archived comms, or long roadmap sections, treat this file plus the current code/tests as authoritative and update the stale file before acting on it.

## W5 Production-Book Readiness Correction (2026-06-07)

The production-book run `production_book_legacy_freqoff_20260606_resume_w5` completed, but its
generated completion report still says `CANDIDATES_PROMOTABLE_AFTER_GATES` because the original
promotion review counted `6` final `PROMOTE` rows. That report is a historical/generated artifact,
not the current readiness source.

Corrected readiness after the promotion-readiness gate fix:

- `PROMOTE=4`, `REVIEW=10`, `REJECT=293`.
- Unsafe `PROMOTE` cells: `0`.
- FAIL cells not rejected: `0`.
- Final clean promotes: `AUDUSD|H4|CHOP`, `EURUSD|H4|TREND`, `USDCAD|H4|TREND`,
  `USDJPY|H4|TREND`.

Canonical corrected artifacts:

- `pm_outputs/experiments/production_book_legacy_freqoff_20260606_resume_w5_promotion_gatefix.json`
- `pm_outputs/experiments/production_book_legacy_freqoff_20260606/benchmarks/production_book_legacy_freqoff_20260606_per_cell_triage_gatefix.md`
- `pm_outputs/experiments/production_book_legacy_freqoff_20260606/benchmarks/production_book_legacy_freqoff_20260606_per_cell_triage_gatefix.json`

Guardrail implemented: completion/reporting automation now cross-checks promotion `PROMOTE` rows
against the per-cell audit and fails or downgrades the headline if any promoted cell is
`FAIL`, `INSUFFICIENT_EVIDENCE`, missing from the cell audit, or otherwise contradicted by
the promotion/cell safety summary.

Additional trust-boundary hardening implemented: automated promotion review now requires an
explicit `review_ready` completion marker with `pid_alive=false`, no memory-pressure stop,
ledger path/count consistency, and OOS artifact coverage. The stale W5 machine-readable done
marker is marked `STALE_SUPERSEDED_BY_PROMOTION_GATEFIX`; the corrected marker is
`logs/discovery_watch/production_book_legacy_freqoff_20260606_resume_w5_completion_done_gatefix.json`.
Regression verification after the hardening pass: full suite `1156 passed, 5 skipped`
on 2026-06-07.

## Production Live Book & Safety Gate (2026-06-07)

Codex generated a production-scoped live book from the corrected W5 evidence and consolidated the
runtime path to the root `config.json` / `pm_configs.json` pair. The default CLI path now uses these
universal files and no longer targets stale high-risk artifacts.

Production artifacts:

- Config: `config.json`
- Ledger: `pm_configs.json`
- Per-cell audit: `pm_outputs/production_live/production_live_w5_specialist_cells.json`
- Promotion/cell safety: `pm_outputs/production_live/production_live_w5_promotion_cell_safety.json`
- Live watchdog status/events: `logs/live_silent_watchdog_status.json`,
  `logs/live_silent_watchdog_events.jsonl`

Tradeable production specialists:

- `AUDUSD|H4|CHOP` - `LiquiditySweepReversalStrategy`, `PASS_STRICT`
- `EURUSD|H4|TREND` - `LiquiditySweepReversalStrategy`, `PASS_THIN`
- `USDCAD|H4|TREND` - `VIDYABandTrendStrategy`, `PASS_STRICT`
- `USDJPY|H4|TREND` - `DoubleEMATrendStrategy`, `PASS_THIN`

Per-cell audit result: 96 specialist cells inspected individually; 92 `NO_TRADE_ABSTAINED`, 2
`PASS_STRICT`, 2 `PASS_THIN`, unsafe promotes `0`.

Readiness gate result after account pinning: **live-ready for the pinned FBS-Demo account**.
Warning: `regime_params_missing`, accepted because the selected artifact contract expected no
`regime_params.json`. The book is built and cell-safe, and `--trade` remains tied to the pinned
non-secret MT5 account identity (`expected_login`, `expected_server`, `expected_currency`).

Live safety additions implemented in this pass:

- manual new-entry kill switch;
- realized-loss hard block fails closed when broker deal history is unavailable;
- market-entry `order_send(None)` is marked ambiguous for reconcile/fail-closed handling;
- production promotion-gate parity checks in live readiness;
- independent `tools/live_silent_watchdog.py` with nonzero warning/critical exit states.

Verification after this pass: targeted production/live-safety tests `76 passed`; full suite
`1169 passed, 5 skipped, 2 warnings, 575 subtests`.

## CURRENT ACTIVE LANE (2026-06-04): assessment reset after `specialist_spokes_v2` rejection

`specialist_spokes_asym` remains rejected. `specialist_spokes_v2` is also rejected as implemented.
The v2 r3 run was operator-stopped after 4 completed symbols because the partial honest-cost replay was
already decisively bad:

- `GBPUSD`: `-61.65%`, PF `0.77`, DD `66.61%`, `527` trades.
- `EURAUD`: `-6.77%`, PF `0.82`, DD `11.38%`, `146` trades.
- `EURUSD`: `-11.94%`, PF `0.77`, DD `15.94%`, `198` trades.
- `USDJPY`: `-13.15%`, PF `0.73`, DD `16.49%`, `204` trades.
- Pooled partial v2 r3: mean return `-23.38%`, median return `-12.54%`, `0/4` positive.

Operator-stop marker:

- `logs/discovery_watch/specialist_spokes_v2_11_r3_20260604_operator_stop.json`

Current conclusion:

- Do not resume v2 r3.
- Do not build more v2 veto/fallback machinery by momentum.
- Do not treat v2 failure as a rejection of Bongo's true ThreadSpark / 3D spoke evidence-geometry concept.
- The implementation drifted into scalar score plus veto/fallback approximation rather than a real
  `candidate x evidence_step x spoke` evidence object.

The active work is back in assessment and repair:

1. Fix engine metric integrity issues before trusting new scorer comparisons. DONE locally on 2026-06-04:
   invalid-entry equity bookkeeping is fixed in both Numba and Python paths; warmup-prefixed equity is trimmed
   for Sharpe/Sortino/std-weekly and OOS period/equity-path evidence. Verification: full suite
   `1105 passed, 5 skipped, 575 subtests`.
2. Redesign the train/validation/OOS split so scoring has genuinely disjoint evidence.
3. Re-center the next scorer on the true path-dependent evidence geometry, preserving 8-10 spokes.
4. Re-test against legacy and old `specialist_spokes` using the robust comparator.

## VERDICT (2026-06-04): `specialist_spokes_asym` REJECTED on honest-cost like-for-like

The asym run is COMPLETE (8/11 winners + US30/US500/DOGUSD NO_TRADE; no OOM recovery needed). No run is active. Honest-cost (conservative_max) benchmark on the matched 8 symbols (records under `pm_outputs/experiments/scoring_like_for_like_20260602/asym_postrun_analysis/`):

- **asym: mean -35.2% / median -36.0%, EVERY symbol negative (-2% to -73%), mean DD 44%, time-underwater 98%.**
- legacy: mean +6.2% / median +4.9%.  old-spoke: mean -0.8% / median +8.7%.
- Tenet-5 `--compare` = REGRESSION vs BOTH legacy and old-spoke; uniform (NOT outlier-dominated). Robust gate REJECTS decisively.

**Diagnosed defect (Goodhart / cost-blind):** the asym scorer gave high composite (83-87) to HIGH-FREQUENCY churn strategies that looked profitable on the SHORT train/val window (val +16% to +24%) but traded 334-2548x over full history and bled on honest cost (e.g. XAUUSD InsideBarBreakout val +17%/51tr -> benchmark -61%/2548tr; AUDJPY VolatilityBreakout val +24%/48tr -> -73%/938tr). `execution_cost_survival` did NOT protect. Validating axes on the OLD W4 pool did not transfer once the scorer drove its OWN search into a high-churn region.

**DECISION: default stays `legacy`. `specialist_spokes_asym` is REJECTED.** A future asym v2 must add real cost/frequency-awareness at SELECTION (functional `execution_cost_survival`, a trade-frequency x cost penalty, or a cost-stressed selection window) and re-validate before any re-run. The pre-registered honest-cost gate caught a catastrophic scorer before any default change — the discipline worked.

## Historical Discovery Nuance: old-spoke evidence

`legacy` is the current default because it remains the safest proven default under the existing Tenet-5 gate.
That is not the same as proving legacy is the strongest robust scorer.

At the 2026-06 decision point, the old `specialist_spokes` evidence looked
stronger on several robust summaries despite its failed mean. It is no longer
an executable candidate; the following figures are retained to explain why it
was investigated before the branch was retired:

- old-spoke median return `8.74%` vs legacy `5.03%`
- old-spoke trimmed mean return `5.78%` vs legacy `2.39%`
- old-spoke median PF `1.285` vs legacy `1.135`
- old-spoke median max DD `8.98%` vs legacy `13.13%`
- both have `5/8` positive matched symbols

The old-spoke raw mean is damaged mainly by one catastrophic miss:
`AUDJPY TriangleCompressionBreakoutStrategy H1` at `-63.92%`, PF `0.51`, max DD `64.22%`,
ulcer `40.691`, and time underwater `99.38%`. Legacy's mean is helped heavily by one monster positive:
`USDJPY DayOfWeekBiasStrategy H1` at `+64.68%`.

Next discovery emphasis: harden old-spoke rather than abandon it. The immediate route is an
`old_spoke_v2` outlier-containment / fallback-selection experiment: use existing completed artifacts where
possible to test whether a full-history conservative replay, catastrophe veto, cost/frequency guard, and
top-N fallback can remove the AUDJPY blowup while preserving old-spoke's median/PF/risk advantages.

## Context Hierarchy

1. `docs/ACTIVE_DISCOVERY_STATE.md` - current state, active run, accepted guardrails, next actions.
2. `AGENT_COMMS.md` - short live coordination channel only.
3. `CHANGELOG.md` - durable project history after a finding is closed.
4. `DISCOVERY_ROADMAP.md` and `docs/discovery_execution_roadmap.md` - historical rationale and route map; useful, but not live state if stale.
5. `docs/archive/` - audit trail only; do not re-litigate archived conclusions unless new evidence requires it.
6. `docs/audit/` - active unresolved audits only.

## Completed Run

Run label: `specialist_spokes_asym_11_20260604`

Purpose: like-for-like discovery run for the new asym specialist-spokes scorer on the 11-symbol ex-BTC subset. This was not a production/live trading run.

Paths:

- Config: `pm_outputs/experiments/scoring_like_for_like_20260602/config.specialist_spokes_asym_11.json`
- Output: `pm_outputs/experiments/scoring_like_for_like_20260602/specialist_spokes_asym_11/`
- Ledger: `pm_outputs/experiments/scoring_like_for_like_20260602/specialist_spokes_asym_11/pm_configs.json`
- App log: `logs/experiments/scoring_like_for_like_20260602/specialist_spokes_asym_11/pm_20260604.log`
- Status marker: `logs/discovery_watch/specialist_spokes_asym_11_20260604_completion_status.json`
- Done marker: `logs/discovery_watch/specialist_spokes_asym_11_20260604_completion_done.json`

Completion state at 2026-06-04 06:15:

- Optimizer completed cleanly and exited.
- Ledger count: 8 symbol-level winners.
- OOS evidence: 11 manifests and 11 matrices.
- The post-run report is `pm_outputs/experiments/scoring_like_for_like_20260602/asym_postrun_analysis/POSTRUN_VERDICT.md`.
- `regime_scoring_model=specialist_spokes_asym`, 11 symbols, 100 strategies, 6 timeframes, and `optimization_max_workers=4`.

Silent-failure watchdog:

- Watchdog PID `19540`.
- Status: `logs/discovery_watch/specialist_spokes_asym_11_20260604_silent_watchdog_status.json`
- Events: `logs/discovery_watch/specialist_spokes_asym_11_20260604_silent_watchdog_events.jsonl`
- Alert criteria: optimizer dead, completion monitor dead, worker count below 4, or stale progress plus no optimizer-tree CPU movement.
- Second heartbeat at 2026-06-04 01:19 was `OK`; total optimizer-tree CPU advanced by `212.53125s` over the previous poll, so the run was active even though the app log had not emitted new symbol-level lines. After completion, `optimizer_pid_dead` from the watchdog is expected and is not a failure.

Post-run analysis watcher:

- Watcher PID `16444`.
- Script archived: `tools/archive/scoring_discovery_20260606/scoring_like_for_like_postrun.py`
- Status: `pm_outputs/experiments/scoring_like_for_like_20260602/asym_postrun_analysis/postrun_status.json`
- Report target: `pm_outputs/experiments/scoring_like_for_like_20260602/asym_postrun_analysis/POSTRUN_VERDICT.md`
- Purpose: when the completion marker and asym ledger exist, rerun legacy, old-spoke, and asym benchmarks
  under the current engine, compare them, run asym spoke-selector validation if evidence exists, write a
  report, and append a Claude audit request to `AGENT_COMMS.md`.
- Final state: `DONE`; post-run report and JSON were written.

## Current Scorer State

Executable regime scorer:

- `legacy` - the single fixed canonical implementation, recorded as literal artifact provenance rather than a setting.

Historical-only comparators:

- `specialist_spokes` - rejected 8-spoke implementation, archived for research reconstruction.
- `specialist_spokes_asym` - rejected v1 implementation, archived for forensic comparison.

The `specialist_spokes_asym` scorer failed the like-for-like benchmark and has been removed from the executable system. Its evidence remains in the research record and archive; it must not be promoted unchanged.

Primary axes:

- `risk_adjusted_edge`
- `trade_quality`
- `gain_pain_efficiency`
- `capital_preservation`
- `tail_risk_control`
- `generalization_survival`
- `execution_cost_survival`
- `parameter_stability`

Diagnostics only:

- `conditional_upside_capture`
- `path_persistence`

Important implementation facts:

- The old geomean veto is replaced by a vote-like asymmetric aggregate.
- Outward extensions can help; inward dents are penalized more heavily.
- Capital and tail fragility remain smooth safety pressure, not arbitrary portfolio balance.
- The tail-axis fix is implemented: backtest/bucket metrics emit `r_downside_deviation`, `r_block_downside_deviation`, and `r_block_cvar`; the asym tail axis uses those before falling back to old scalar tail fields. `r_block_cvar` is the current R-normalized evidence-block CVaR implementation. A literal per-bar equity `period_cvar` is not the active scorer input and should be treated as a future diagnostic unless separately calibrated.
- Execution-cost evidence is copied to `selection_execution_cost_survival`; the scorer must not read nested holdout/forward/live artifacts.

## Leakage Boundary

Allowed inside scoring objective:

- Train metrics.
- Validation/selection metrics.
- Candidate parameters and Optuna/search metadata generated before final selection.
- Selection-window execution cost evidence when explicitly exported for that run.

Forbidden inside scoring objective:

- Clean holdout results.
- Promotion-review verdicts.
- Forward/live results.
- Benchmark comparison outputs.
- Any post-selection report used to judge the winner.

If any future metric needs forbidden data, it belongs in promotion review or diagnostics, not the Optuna/regime objective.

## Evaluation Gate

Do not decide from raw pooled mean alone. Prior runs were outlier-dominated.

The like-for-like verdict must compare:

- `specialist_spokes_asym`
- `legacy`
- old `specialist_spokes`
- a trivial high-signal baseline such as rank-by-Calmar where available

Pre-registered decision emphasis:

- median/trimmed forward return
- percent profitable
- drawdown and time-underwater
- per-symbol blowups
- holdout/PBO/provenance/stress review
- whether any improvement is due to thin evidence or a single lucky outlier

Default remains `legacy`. The asym scorer did not prove improvement or a justified trade-off.

## Cleanup Rules

- Keep active run artifacts until the verdict is complete.
- Keep cross-run comparisons and final reports needed for decisions.
- Archive or delete spent audit/metafile docs after durable facts are moved into code/tests/config/`CHANGELOG.md`.
- `AGENT_COMMS.md` should stay short. Archive it when it becomes too long, then reset it with a pointer to the archive and this file.
- Do not add new long-lived design docs unless they replace stale ones or become the current source of truth.

## Next Actions

1. Keep the fixed `legacy` scorer until a separately precommitted approach proves superior on untouched evidence.
2. Do not route historical spoke variants through production config; the retired keys fail closed.
3. Preserve the rejected-run evidence as learning, not as executable production machinery.
