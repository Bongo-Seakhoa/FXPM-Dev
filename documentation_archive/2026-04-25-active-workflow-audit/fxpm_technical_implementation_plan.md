# FXPM Technical Implementation Plan

## Purpose

This document consolidates the current audit, analysis, and data workflow restructure into an implementation plan.

The goal is not to maximize the number of changes. The goal is to make the best implementation possible: changes that leave the PM codebase more correct, more profitable, more accurate, more actionable, and more reliable than it was found.

Priority order:

1. Quality and correctness.
2. Profitability and live relevance.
3. Signal accuracy and actionability.
4. Reliability and observability.
5. Efficiency.

Efficiency matters, but only as a support mechanism. It should not compromise quality, profitability, signal accuracy, or live execution realism.

## Implementation Status As Of 2026-04-25

The first implementation pass has been completed.

Completed:

- active recent M5 workflow fields and splitter contract
- M5 timestamp-boundary slicing for higher timeframes
- historical stress audit metrics on regime and symbol artifacts
- mandatory risk-management/governance policy selection metadata
- `risk_management_selection_stage` propagation to the actual evaluation surface
- live margin-protection wiring before governance and new entries
- robust margin-level handling and snapshot reuse in the live sweep
- validation-aware regime Optuna scoring where validation buckets exist
- signal-bar regime bucketing for per-regime metrics
- explicit `NO_TRADE` markers for evaluated no-winner slots
- nested regime validity aligned with the production retrain schedule
- broker-spec cache invalidation when the configured file is missing or malformed
- implementation changelog in `FXPM_IMPLEMENTATION_CHANGELOG.md`

Validation:

- `python -m py_compile pm_core.py pm_pipeline.py pm_main.py pm_optuna.py`
- `python -m pytest -q`
- Full result after the latest implementation pass: `531 passed, 1 skipped, 350 subtests passed`.

Not completed in this pass:

- no full `python pm_main.py --optimize` run was executed
- `broker_specs.json` still needs to be captured from the target MT5 terminal
- `pm_configs_high_risk.json` still needs regeneration under the latest artifact contract
- dashboard refresh job status remains future work
- generated runtime/cache/log/output cleanup remains an operational hygiene task

## Current System Position

The PM is already materially stronger than the earlier baseline:

- Backtesting and live execution now share the typed `TradeIntent` exit surface.
- Regime TP multipliers are included in the optimized/backtested trade path.
- Artifact metadata now records the exit-surface contract and regime TP multipliers.
- Live execution has strong layered rejection gates.
- Dashboard writes are hardened.
- Storage and signal-ledger observability exist.
- The full non-optimization test suite passed during the audit.

The main remaining problems are not a broken PM thesis. They are loose spots in runtime safety, artifact freshness, data workflow design, optimization selection relevance, and operational reproducibility.

## Non-Negotiable Implementation Principles

1. Do not run full `python pm_main.py --optimize` casually. A one-currency or small-subset run is acceptable when necessary.
2. Do not add walk-forward evaluation as an intermediary gate between Stage 2, Stage 3 if separated, and live implementation.
3. Do not let older data dominate current strategy selection.
4. Do not let efficiency shortcuts change which strategies win or how live trades exit.
5. Do not optimize one exit/risk surface and trade another.
6. Do not silently expand the optimization surface without artifact metadata, tests, and enough-trade safeguards.

Mandatory risk-management contract: risk-management optimization is required for winning optimized symbol/timeframe/regime strategies. The implementation choice is whether to optimize it inside Stage 2 or as a separate Stage 3.

## Target Data Workflow

All 300,000 / 50,000 / 250,000 quantities refer to base M5 bars.

Target layout:

```text
latest 300,000 M5 bars

oldest 50,000 M5 bars:
historical_stress_audit / historical_audit_window

newest 250,000 M5 bars:
active processing universe
```

Higher timeframe slices must use timestamp boundaries derived from M5. Do not independently take 50,000 H1, H4, or D1 candles.

The historical audit window is not a current-code `holdout`. In the current codebase, `holdout` means the freshest scored out-of-sample tail. The historical audit window is older than the active universe and exists to flag severe fragility.

## Target Stage Model

### Stage 1: Config-Based Baseline Eligibility

Purpose: preserve viable strategy candidates.

Stage 1 should:

- run baseline strategy configurations across the active 250,000 M5 bar universe
- apply config-defined pass/fail criteria
- avoid unnecessary A/B/C chronological segmentation
- avoid acting as the primary Top-K optimizer
- output eligible strategy families for Stage 2

Stage 1 should not try to prove final robustness. It should be permissive enough to avoid beam exclusion risk.

### Stage 2: Top-K Optimization And Production Selection

Purpose: perform the expensive competitive selection.

Stage 2 should:

- receive Stage 1 survivors
- run parameter search and strategy refinement
- apply Top-K logic inside the optimization process
- select the best production-ready candidate per symbol, timeframe, and regime
- use the same exit/risk assumptions that live execution can actually trade
- either include risk-management optimization directly or hand the selected winner to Stage 3 for mandatory risk-management optimization

### Required Risk-Management Optimization

Risk-management optimization is required for the winning optimized symbol/timeframe/regime strategies. The PM should not select only the best entry strategy and then treat risk management as a fixed afterthought if risk-management behavior can materially affect profitability, actionability, or live execution.

The design choice is placement:

- Path A: optimize risk management inside Stage 2.
- Path B: optimize risk management after Stage 2 as Stage 3.

### Risk Management Path A: Inside Stage 2

Risk-management policy selection can be included inside Stage 2 if the surface is controlled. Under this path, the Stage 2 winner is already a complete strategy plus risk-management package.

Requirements:

- curated policy set
- enough-trade safeguards
- baseline-improvement requirement
- artifact metadata
- live/backtest parity tests

### Risk Management Path B: Stage 3 After Stage 2

Risk management can alternatively run after Stage 2 as Stage 3. Under this path, Stage 3 is not optional. It is the required risk-management optimization stage for the winning optimized Stage 2 strategies.

Stage 3 should:

- receive the selected Stage 2 winner
- evaluate curated risk-management/governance policies
- use the same Stage 2 slice where appropriate, including a half-of-active-universe slice if that is the chosen Stage 2 contract
- require enough trades and baseline improvement
- attach selected policy evidence to artifacts
- remain separate from walk-forward gating

The optional choice is not whether to optimize risk management. The choice is whether risk-management optimization is integrated into Stage 2 or run as Stage 3 after the Stage 2 winner is selected.

### Historical Stress Audit

Purpose: detect severe fragility on older market data.

The oldest 50,000 M5 bars should be used after selection to identify:

- catastrophic collapse
- unacceptable drawdown
- execution incompatibility
- insufficient trade viability
- structurally unsafe risk-management behavior
- warmup or state requirements that should be explicit

Weaker historical-audit performance should not automatically reject a strategy. Severe failure should trigger review, rejection, risk adjustment, or reduced confidence.

### Non-Gating Stale-Winner Walk-Forward Report

Do not add walk-forward as a gate between optimized winners and live implementation.

Future low-priority reporting can replay stale winners from the previous production cycle over the prior two-week retrain period. This report should measure drift, regime mismatch, expected-versus-realized behavior, edge decay, and trade-count deviation.

This report is observational only unless a future operator policy explicitly changes that.

## Implementation Priorities

### Priority 0: Runtime Safety And Contract Correctness

These are the highest urgency correctness and live-safety changes.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| Governance TP release can raise `NameError` | Add missing `math` import in `pm_main.py`; add focused regression for `pure_atr` TP release through `_run_order_governance_cycle()` | Governance cycle does not crash when TP is released to infinity |
| Margin protection forced-close cycle appears unwired | Wire `_run_margin_protection_cycle()` into the live sweep before new entries, or explicitly disable it with a documented config flag | Recovery/panic margin states can actively reduce exposure if enabled |
| Active high-risk ledger is stale | Regenerate `pm_configs_high_risk.json` after runtime fixes using a controlled subset first, then broader run when ready | New artifact metadata includes current exit-surface contract and regime TP multipliers |
| Missing `broker_specs.json` | Capture/version broker specs from MT5 or explicit config before authoritative optimization | Backtest/live sizing uses reproducible contract size, tick value, tick size, volume step, min/max lot, stops level, spread, and commission |
| Worktree reproducibility is poor | Separate source/docs/config from generated logs, caches, ledgers, storage state, and signal ledgers | A clean checkout can reproduce source behavior and locate the active trading artifact |

### Priority 1: Data Workflow And Live-Relevance Restructure

These changes should improve profitability and actionability by reducing stale-data dominance.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| Beam exclusion risk from older candidate selection | Add explicit active-universe slicing: newest 250,000 M5 bars drive Stage 1 and Stage 2 | Candidate pool is formed from live-relevant data, not the oldest 80% of a 500k window |
| Historical audit terminology conflict | Rename oldest 50k concept to `historical_stress_audit` / `historical_audit_window` | No implementation or report labels confuse oldest audit data with current freshest-tail holdout |
| Higher timeframe slicing ambiguity | Derive all timeframe boundaries from M5 timestamps | M15/M30/H1/H4/D1 windows align with the same calendar period as the M5 active universe |
| Stage 1 overgovernance risk | Keep Stage 1 config-based and permissive; avoid nonessential chronological segmentation | Promising untuned strategies are not rejected before Stage 2 can optimize them |
| Current validation evidence too thin | Replace default `80/10/10` scored split behavior for production restructure with active-universe selection semantics | The freshest active data is not silently carved away into a pseudo-holdout before strategy selection |

### Priority 2: Optimization And Selection Quality

These changes improve candidate quality and reduce train-overfit behavior.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| Multi-regime Optuna train-only objective | Feed validation or active-ranking metrics into the Optuna objective where available, with bounded influence | TPE search is less likely to spend trial budget on train-only overfit regions |
| Risk management is currently not fully part of selection | Choose Path A or Path B and optimize curated risk/governance policy selection for winning optimized strategies | Artifacts identify the selected complete trade-management package, not only entry strategy |
| TP multiplier regime and metric bucket regime can differ by one bar | Store signal-bar regime on trade records and use it for regime-bucket metrics where evaluating signal context | Per-regime metrics reflect the regime that created the trade intent |
| No-winner regimes are omitted | Store explicit `NO_TRADE` marker or explicit no-winner metadata for evaluated regimes | Reports distinguish "evaluated and no winner" from "not evaluated" |
| `REGIME_WARMUP` not preserved | Preserve regime warmup marker in feature output if useful for audits | Warmup behavior can be inspected in reports and artifacts |
| Cache fingerprint samples rows | Decide whether stronger file hashing is needed for production runs | Interior-only CSV changes cannot silently reuse stale cache in authoritative mode |

### Priority 3: Live Execution And Risk Reliability

These are live-safety and actionability improvements.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| Duplicate/race protection can use stale sweep snapshot | Refresh duplicate/position state immediately before order send or add a final MT5 check | Manual/external order after sweep snapshot cannot bypass duplicate prevention |
| Partial fill retcode handling is incomplete | Treat MT5 partial fill retcode as partial success with residual handling/reporting | Operator can see partial fills and no residual exposure is hidden |
| `margin_reopen_level` configured but not clearly used | Wire or document margin reopen behavior alongside margin protection cycle | Configured margin recovery settings have visible runtime meaning |
| Broker min-lot risk clamps can reject many low-balance trades | Keep rejection strict, but improve reporting on why each trade is rejected | Low-balance rejection is understandable and actionable |
| Local governance is shadow | Keep shadow until runtime bugs, enough evidence, and artifact metadata are clean | Governance can be promoted deliberately, not accidentally |

### Priority 4: Dashboard, Docs, And Operator Clarity

These reduce operator mistakes and improve actionability.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| Dashboard manual refresh is async without status | Add job-state endpoint with running flag, last run time, updated/failed counts, last error | Operator can distinguish "started" from "finished" |
| `actionable_score_margin` naming drift | Rename or document as relative score band, e.g. `actionable_score_band_ratio` | Operators tune it as a relative band, not an absolute score threshold |
| Docs describe stale lower-risk profile | Update setup/implementation docs for high-risk micro-account profile and governance shadow mode | Operator docs match current config |
| Dashboard reads are unauthenticated | Keep acceptable for loopback; warn or add optional read auth when bound externally | Sensitive data is not exposed on remote bind by accident |
| CSV alias duplication | Clean or normalize duplicated `Spread`/`spread` and `RealVolume`/`real_volume` columns later | Data files are easier to reason about without breaking loader compatibility |

### Priority 5: Artifact And Reporting Health

These improve reproducibility and future audit quality.

| Issue | Implementation | Acceptance Criteria |
|---|---|---|
| `pm_outputs/optimization_summary.csv` is stale | Regenerate or label stale summaries clearly | Reports do not present old summaries as current truth |
| Active runtime artifacts are untracked | Decide tracking/ignore/archive policy for ledgers, manifests, storage state, signal ledgers | Active artifact state is reproducible or intentionally runtime-only |
| Full ledger is expired and not active | Label `pm_configs.json` as historical/fallback unless regenerated | Operators do not confuse full stale ledger with active high-risk ledger |
| Historical audit results need reporting | Add report fields for `historical_stress_audit` metrics and severe-failure flags | Historical fragility is visible but not confused with live selection |
| Stale-winner report is future work | Add independent report generator later for prior two-week stale winners | Drift analysis exists without blocking new live winners |
| MT5 session dependency is not fully reproducible | Document or configure MT5 login/server/path expectations separately from secrets | Live behavior does not depend on unexplained terminal state |
| Cache and storage growth can obscure current state | Add cache/storage review rules and keep observe-only cleanup explicit | Disk pressure and stale cache state are visible before they affect operation |

## Recommended Implementation Order

1. Patch governance `math` import and add the focused regression.
2. Wire or explicitly gate margin protection forced-close behavior.
3. Capture broker specs and add artifact metadata for broker spec source.
4. Implement data workflow terminology and config fields: active universe and historical audit window.
5. Implement M5 timestamp-boundary slicing and tests.
6. Adapt Stage 1 to use active-universe baseline eligibility.
7. Adapt Stage 2 to optimize/rank within active-universe semantics.
8. Choose risk-management Path A or Path B and implement mandatory risk-management optimization with curated policies only.
9. Add historical stress audit evaluation and reporting.
10. Regenerate the high-risk ledger with a one-currency or small subset test first.
11. Re-audit the new ledger for validation, historical audit, live feasibility, and artifact metadata.
12. Add dashboard refresh status and clean operator documentation.
13. Add non-gating stale-winner walk-forward report as a later diagnostic tool.

## Data Workflow Acceptance Tests

Add tests that prove:

1. A 300,000-row M5 dataset splits into 50,000 historical audit rows and 250,000 active rows.
2. The historical audit window is oldest, not newest.
3. Active universe is newest, not oldest.
4. Higher timeframe boundaries are timestamp-based and align with M5 boundaries.
5. Stage 1 does not score historical audit rows as active selection data.
6. Stage 2 does not optimize on historical audit rows unless explicitly running audit mode.
7. Historical audit metrics are attached as audit metadata, not as primary selection score.
8. No walk-forward report can gate new winners by default.

## Live Alignment Acceptance Tests

Add or preserve tests that prove:

1. Backtester uses `build_trade_intent()` and regime TP multipliers.
2. Live execution uses the same TP/SL/risk-management assumptions stored in artifacts.
3. Governance TP release does not crash.
4. Margin protection runs at the intended live-loop point when enabled.
5. Risk-management policy selected in Stage 2 or Stage 3 is the policy live execution sees.
6. Signal-bar regime and trade metric regime are explicitly recorded.

## Optimization Acceptance Tests

Add or preserve tests that prove:

1. Stage 1 is eligibility-based, not Top-K ranked selection.
2. Stage 1 thresholds come from config.
3. Stage 2 owns Top-K ranking and expensive optimization.
4. Optuna objective can use validation/active-ranking evidence without leaking historical audit data.
5. No-winner regimes are represented explicitly.
6. Artifact metadata records data workflow version, M5 boundary timestamps, exit-surface contract, regime TP multipliers, broker specs source, and risk-management policy selection method.

## Documentation Acceptance Criteria

Documentation should consistently use:

```text
historical_stress_audit / historical_audit_window
active_universe
Stage 1: config-based baseline eligibility
Stage 2: Top-K optimization and production selection
Stage 3, if separated: mandatory risk-management/governance policy optimization for Stage 2 winners
freshest-tail holdout: existing code term, only if retained
stale-winner walk-forward report: non-gating prior-cycle drift report
```

Documentation should avoid:

- calling the oldest 50,000 M5 bars a holdout
- calling Stage 1 a Top-K optimization layer
- calling a tuning/ranking slice validation
- implying walk-forward is a live implementation gate
- implying risk-management optimization is optional for winning optimized strategies
- implying low-balance rejected trades mean the strategy choice is wrong

## Definition Of Done

The implementation is successful when:

1. Known live-runtime hazards are patched and covered by focused tests.
2. The PM uses a recent active universe for strategy eligibility and optimization.
3. The oldest 50,000 M5 bars are used only as historical stress/audit unless explicitly configured otherwise.
4. Stage 1, Stage 2, Stage 3 if separated, and historical audit have distinct names, code paths, metadata, and reports.
5. Risk-management behavior is optimized either inside Stage 2 or in Stage 3 and is explicitly attached afterward with evidence.
6. Optimized behavior and live behavior match for exits, regime TP multipliers, sizing assumptions, and governance policy.
7. A regenerated high-risk ledger records current artifact metadata and can be explained from source.
8. Reports make live feasibility and low-balance rejection reasons visible.
9. The codebase is cleaner, safer, more accurate, and more live-relevant than it was before the work began.
