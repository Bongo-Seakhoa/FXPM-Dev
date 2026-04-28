# FXPM Current-System Analysis

**Analysis date:** 2026-04-25

**Purpose:** Explain how the current PM works after the latest implementation pass, how to interpret the active workflow, and what should materially differ in the next generated high-risk ledger.

**Validation note:** Full pytest passed after the latest patches: `531 passed, 1 skipped, 350 subtests passed`. No full `pm_main.py --optimize` run was executed.

## System Thesis

FXPM is best understood as a regime-aware strategy and trade-management selection engine, not a classical supervised ML model.

The optimizer searches for profitable, actionable local winners by symbol, timeframe, and regime. Live execution then decides whether a winner is actually tradable under current account, margin, spread, broker, volume, risk, and duplicate-position constraints.

That separation is intentional. A good winner can still be rejected live on a low balance if the broker minimum lot or margin requirement makes the trade unsafe.

## Data Workflow

The production workflow now uses the active recent M5 contract:

```text
latest 300,000 M5 bars

oldest 50,000 M5 bars:
historical_stress_audit / historical_audit_window

newest 250,000 M5 bars:
active universe for eligibility, optimization, ranking, and production selection
```

All higher timeframes are derived from the same base M5 window and aligned by M5 timestamp boundaries.

The historical audit window is older than the active universe. It is not a forward-looking holdout and it should not dominate current strategy choice. Its job is to expose severe fragility.

## Stage Model

### Stage 1: Baseline Eligibility

Stage 1 is broad and config-governed. It asks whether a baseline strategy is viable enough to deserve deeper optimization.

It should preserve promising candidates and avoid unnecessary chronological over-segmentation.

### Stage 2: Top-K Optimization And Production Selection

Stage 2 is where expensive parameter search, ranking, and winner selection happen.

The latest code reduces train-only search drift by feeding validation bucket metrics into the regime Optuna objective when that evidence exists.

### Stage 3: Risk-Management/Governance Selection

Risk-management selection is mandatory for winning optimized symbol/timeframe/regime strategies.

Current default config uses `risk_management_selection_stage = "stage3"`, which evaluates curated governance policies on the newest Stage 2 selection slice. If configured as `stage2`, the policy tournament evaluates on the active optimization universe.

Artifacts now record the selected policy, candidate policy metrics, baseline metrics, selected metrics, selection stage, and selection surface.

## Exit And Regime Alignment

The important live/backtest alignment issue is now corrected.

Backtest, optimization, governance evaluation, and live execution use the typed trade-intent exit surface. Regime TP multipliers are part of that surface.

Per-regime metrics now bucket trades by the signal-bar regime. That is the regime the live PM knows when it selects the winner and builds the trade intent. This avoids one-bar drift between the regime that created the TP and the regime used for evaluation.

## No-Trade Semantics

The optimizer now stores explicit no-trade markers for evaluated timeframe/regime slots where no validated winner exists.

This matters because absence can mean several different things:

- the timeframe was never evaluated
- data was insufficient
- no strategy passed validation
- the regime was evaluated and should fail closed

`NO_TRADE` markers make the last case explicit. They are not counted as winners and live evaluation treats them as no-winner states.

## Live Execution

Live execution remains intentionally stricter than optimization.

The live path can reject a validated signal because of:

- storage pressure
- duplicate open position
- symbol visibility or trade-mode failure
- missing account or position snapshot
- account trading permission disabled
- margin entry block
- margin recovery or panic state
- margin reopen cooldown after forced close
- spread quality filter
- stop-distance constraints
- tiny risk below minimum trade risk
- broker minimum lot exceeding the hard risk cap
- same-symbol combined risk cap
- insufficient free margin
- MT5 order preflight or send failure

For a high-risk low-balance profile, more live rejections are expected. That does not automatically mean the optimized strategy choice is wrong. It usually means the account cannot safely express every winner until balance or margin headroom improves.

## Margin Protection

The margin-protection cycle is now wired into the live sweep before governance and new entries.

It uses the existing account and position snapshots for initial classification, closes limited exposure in `RECOVERY` or `PANIC`, refreshes snapshots only after a successful forced close, and skips new entries if margin remains stressed.

Missing or non-numeric `margin_level` no longer crashes the sweep.

## Broker Specs

The code now avoids stale broker-spec cache if the configured broker spec path is missing or malformed.

However, the workspace still lacks an actual `broker_specs.json`. Before authoritative optimization, broker specs should be captured from the target terminal so optimization and live execution agree on tick value, tick size, contract size, volume step, stops level, spread, and commission.

## How The Next High-Risk JSON Should Differ

A new `pm_configs_high_risk.json` generated from the current code should materially differ from the old one in metadata and possibly in winners.

Expected differences:

- artifact version reflects the active recent M5 workflow and risk-management contract
- data window metadata includes M5 boundaries
- historical audit metrics are attached
- governance/risk policy evidence is attached per winner
- explicit `NO_TRADE` slots appear where evaluated regimes have no winner
- signal-bar regime bucketing can shift regime metrics near transitions
- Optuna may choose different parameters because validation bucket metrics now influence regime search
- nested regime validity aligns with the production retrain schedule

The low-balance profile should not directly change the optimized strategic edge. It should mainly affect live feasibility: rejected trades, min-lot risk clamp decisions, margin blocks, and order preflight outcomes.

## Current Config Interpretation

The checked-in config is a high-risk micro-account profile:

- active winner ledger path: `pm_configs_high_risk.json`
- position risk basis: `balance`
- position risk per trade: `2.0%`
- hard per-trade risk cap: `3.0%`
- local governance: `shadow`
- live risk scalars: `shadow`
- market-driven exit pack: `off`
- walk-forward audit: disabled
- active workflow: latest 300k M5, with 50k historical audit and 250k active universe

## Operating Guidance

Use the active recent universe as the production selection authority.

Use historical audit as a severe-fragility warning layer.

Do not add walk-forward as a gate between Stage 2/Stage 3 and live implementation. A future stale-winner report can be useful, but it should remain separate and observational unless deliberately promoted later.

Do not treat low-balance trade rejections as proof that the strategy is bad. Treat them as feasibility data.

## Immediate Next Actions

1. Capture broker specs from MT5.
2. Run a one-symbol optimization first if a live check is needed.
3. Inspect the generated symbol artifact for workflow metadata, historical audit, governance policy, and no-trade slots.
4. Regenerate the high-risk ledger when ready.
5. Re-audit the generated ledger before expanding live authority.

## Bottom Line

The PM is now more coherent and live-aligned. The most important logic points in the right direction: recent data drives selection, older data audits fragility, exits are shared between optimization and live, risk management is attached to winners, and no-winner states fail closed explicitly.

The next big move is controlled artifact regeneration, not a full optimizer run from this audit session.
