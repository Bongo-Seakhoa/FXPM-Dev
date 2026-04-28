# FXPM Codebase Audit

**Audit date:** 2026-04-25

**Workspace:** `c:\Users\Bongo\OneDrive\Desktop\FxPM 1.4 - Obtimized`

**Scope:** Current local codebase after the active workflow, live-risk, optimizer-alignment, and documentation patches.

**Validation run:** `python -m py_compile pm_core.py pm_pipeline.py pm_main.py pm_optuna.py` passed. `python -m pytest -q` passed with `531 passed, 1 skipped, 350 subtests passed`. No full `python pm_main.py --optimize` run was executed.

## Executive Verdict

The PM codebase is in a materially better state than the previous audit snapshot.

The highest-impact alignment issues have been corrected:

- live and backtest exits share the typed trade-intent surface
- regime TP multipliers remain in the optimized/backtested path
- active data workflow now prioritizes the newest M5 universe
- historical older data is separated into `historical_stress_audit`
- risk-management/governance policy selection is attached to winner artifacts
- Optuna regime search now scores with validation bucket evidence where available
- regime metrics now use the signal-bar regime, matching live winner selection
- explicit `NO_TRADE` markers distinguish evaluated no-winner slots from missing data
- margin protection is wired into the live sweep without breaking snapshot reuse

The main remaining risks are operational rather than architectural: the active high-risk ledger still needs regeneration under the latest artifact contract, broker specs should be captured from the live terminal before authoritative optimization, and the local worktree remains very dirty.

## Current System Snapshot

| Area | Current State |
|---|---|
| Production optimizer | Regime-aware `OptimizationPipeline` |
| Data workflow | `active_recent_m5` |
| Base data window | latest `300,000` M5 bars |
| Historical audit | oldest `50,000` M5 bars of the workflow window |
| Active universe | newest `250,000` M5 bars |
| Risk-management selection | mandatory, default `stage3` surface |
| Active winner ledger path | `pm_configs_high_risk.json` |
| Live governance | `shadow` mode in config |
| Live risk scalars | `shadow` mode in config |
| Walk-forward gating | disabled; future report should be non-gating only |
| Storage | enabled, observe-only, signal ledger enabled |
| Dashboard | read-mostly; remote writes require token |
| Test health | full suite green after patches |

## High-Impact Corrections Implemented

### 1. Active M5 Workflow

The pipeline now has explicit workflow fields:

- `data_workflow_mode = active_recent_m5`
- `max_bars = 300000`
- `historical_stress_audit_bars = 50000`
- `active_universe_bars = 250000`
- `active_stage2_pct = 50.0`

`DataSplitter.get_workflow_indices()` defines the latest-300k M5 contract. Higher timeframe data is derived from the same base M5 source and sliced by M5 timestamp boundaries, avoiding independent stale candle counts per timeframe.

### 2. Historical Stress Audit

`RegimeConfig` and `SymbolConfig` now store `historical_audit_metrics`.

The historical audit is not treated as a forward holdout or primary selector. It flags severe fragility, low trade count, excessive drawdown, or poor historical profit factor on the older audit window.

### 3. Risk-Management Optimization Propagation

Risk-management optimization is now a first-class artifact concern:

- `risk_management_optimization_enabled`
- `risk_management_selection_stage`
- selected governance/risk policy
- selection surface
- baseline metrics
- selected metrics
- candidate policy metrics

Default config uses `stage3`, which evaluates on the newest Stage 2 selection slice. If set to `stage2`, governance policy selection evaluates on the active optimization universe.

### 4. Optimizer/Live Alignment

The multi-regime Optuna objective and fallback regime search now pass validation bucket metrics into the scoring function when validation data is available. This reduces train-only trial-budget drift.

Regime bucketing now uses `signal_bar` before falling back to `entry_bar`, which matches the live moment when the PM chooses the regime winner and builds the trade intent.

### 5. Explicit No-Trade Slots

Evaluated timeframe/regime slots with no validated winner now receive explicit `NO_TRADE` markers. These markers:

- are not counted as tradeable winners
- are skipped by holdout, historical audit, walk-forward audit, and governance selection
- are treated as no-winner states in live evaluation
- preserve why a slot was fail-closed

### 6. Live Margin Protection

`_run_margin_protection_cycle()` is now called from the live sweep before governance and new entries.

The implementation reuses the sweep account and position snapshots for initial classification, refreshes only after an actual forced close, handles missing/non-numeric `margin_level` safely, and skips new entries if the PM remains in `RECOVERY` or `PANIC`.

### 7. Broker-Spec Cache Safety

`load_broker_specs()` now clears stale broker-spec cache when the configured file is missing or malformed. This prevents old broker economics from leaking into a later run after a path change.

## Remaining High-Priority Risks

### 1. Active Ledger Must Be Regenerated

`pm_configs_high_risk.json` remains a transitional artifact until regenerated by the latest code. It should not be treated as authoritative current-code output.

Do not compare a new optimized ledger against the old one as if they used the same artifact contract.

### 2. Broker Specs Still Need Capture

`config.json` points to `broker_specs.json`, but the file is absent in the current workspace. The cache safety bug is fixed, but true optimization/live reproducibility still requires a captured broker-spec file from the target MT5 terminal.

This matters for tick value, tick size, contract size, lot step, min/max lot, stops level, spread, and commission.

### 3. Worktree Reproducibility Is Still Poor

The workspace contains many modified, deleted, and untracked runtime/data/cache/output paths. Source changes, generated data, cached resamples, ledgers, logs, and storage state are mixed together.

This should be cleaned or policy-separated before treating the repository as a reproducible production baseline.

### 4. Dashboard Refresh Status Is Still Weak

Historical data refresh can start a background job without an operator-facing completion/status endpoint. This is not a trading-core bug, but it can create operator confusion.

### 5. Broker Specs And Regenerated Artifacts Need Metadata Discipline

The artifact metadata is now stronger, but the next regenerated ledger should be inspected for:

- data workflow version
- M5 window boundaries
- risk-management selection surface
- governance candidate metrics
- historical audit metrics
- exit-surface contract
- regime TP multiplier table
- broker spec source

## Low And Informational Findings

### Cache Fingerprints

Feature cache fingerprinting samples rows for efficiency. This is acceptable for normal development but can miss interior-only CSV edits inside one process. Authoritative optimization should use fresh processes or stronger cache invalidation if data was edited.

### Dashboard Reads

Dashboard writes are protected, but reads remain open to anyone who can reach the bound host. This is fine for loopback use and risky for public binds.

### Runtime Artifacts

`pm_outputs`, signal ledgers, storage state, logs, and cache files need an explicit tracking/ignore/archive policy.

## Current Ledger Interpretation

The current `pm_configs_high_risk.json` is useful for inspection but should be considered stale relative to the latest code. A fresh optimization should produce materially different metadata and may produce different winners because:

- the data workflow now uses the active recent M5 contract
- risk-management selection is mandatory and attached
- Optuna regime scoring uses validation evidence
- signal-bar regime bucketing can alter per-regime metrics
- no-winner slots are explicit
- nested validity timestamps align with the production retrain schedule

The strategy choice should still be driven by symbol/timeframe/regime edge, not by account balance. Low balance should mainly appear as live trade rejection, min-lot risk clamp pressure, or margin feasibility rejection.

## Recommended Next Steps

1. Capture `broker_specs.json` from the target live/demo MT5 terminal.
2. Run a controlled one-symbol optimization first, not the full optimizer.
3. Inspect the generated artifact metadata and historical audit output.
4. Then regenerate `pm_configs_high_risk.json` when ready.
5. Re-audit winners for trade count, validation return/DD, historical severe-failure flags, selected governance policy, and live feasibility.
6. Add dashboard refresh status when trading-core work is quiet.
7. Clean generated caches/logs/runtime artifacts from source status.

## Final Assessment

The core PM thesis is intact and stronger: recent live-relevant data drives selection, older data audits fragility, live execution remains strict, and artifacts now describe more of the behavior that will actually be traded.

The codebase is safer, more accurate, more live-aligned, and easier to reason about than it was before these patches. The next decisive step is not more broad refactoring; it is a controlled regeneration and audit of the high-risk ledger under the current contract.
