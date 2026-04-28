# FXPM Implementation Changelog

## 2026-04-25 Active Workflow And Risk-Management Propagation

Implementation focus: improve PM quality, profitability, accuracy, actionability, and live reliability by converting the documented workflow into code.

### Runtime Safety

- Added the missing `math` import in `pm_main.py` so order-governance TP release paths using `math.isinf()` cannot raise `NameError`.
- Wired `_run_margin_protection_cycle()` into `LiveTrader.process_all_symbols()` before governance checks and new entries.
- Refreshed account and position snapshots after the margin protection pass so downstream governance and entry checks do not use stale pre-close state.
- Skips new entries for the sweep if margin remains in `RECOVERY` or `PANIC` after the protection pass.
- Reused the live sweep's existing account/position snapshots for the initial margin-protection classification and refreshed only after an actual forced close, preserving the single-snapshot live-loop contract.
- Treated missing or non-numeric margin-level values as unavailable instead of crashing the live sweep.
- Cleared cached broker specs when a configured broker-spec file is missing or malformed, preventing stale broker economics from leaking across path changes.

### Data Workflow

- Added explicit active-recent M5 workflow configuration:
  - `data_workflow_mode = active_recent_m5`
  - `max_bars = 300000`
  - `historical_stress_audit_bars = 50000`
  - `active_universe_bars = 250000`
  - `active_stage2_pct = 50.0`
- Added `DataSplitter.get_workflow_indices()` for the latest-300k M5 contract.
- Updated the optimization pipeline to load one base M5 window and derive higher timeframes from that same timestamp-bounded source.
- Stage 1/Stage 2 now operate from the active universe rather than the oldest 80% of a 500k window when `active_recent_m5` mode is enabled.

### Historical Stress Audit

- Added `historical_audit_metrics` to `RegimeConfig` and `SymbolConfig`.
- Added historical stress audit evaluation against the oldest 50,000 M5-bar audit window.
- Historical audit stores metrics, severe-failure status, and failure reasons without becoming the primary selection surface.

### Risk-Management Optimization Propagation

- Added mandatory risk-management optimization configuration:
  - `risk_management_optimization_enabled`
  - `risk_management_selection_stage`
- Reworked governance/risk policy selection so winner artifacts record:
  - selected policy
  - optimization stage
  - selection surface
  - baseline metrics
  - selected metrics
  - candidate policy metrics
- In active workflow mode, risk-management policy selection uses the Stage 3 selection surface, which is aligned with the Stage 2 live-relevant slice rather than the old freshest-tail holdout.
- `risk_management_selection_stage = stage2` now evaluates governance/risk policies on the active Stage 2 optimization universe; the default `stage3` path evaluates on the newest Stage 2 selection slice.

### Optimization/Live Alignment

- Changed regime trade bucketing to use the trade `signal_bar` instead of the next-bar `entry_bar`, matching the live PM decision point where the regime winner is selected before execution.
- Changed Optuna regime tuning and its fallback regime search to score candidates with validation bucket metrics when validation is available, rather than optimizing only against the training bucket after already computing validation results.
- Added explicit `NO_TRADE` regime markers for timeframe/regime slots where no validated winner exists, preserving a fail-closed artifact contract without counting those markers as tradeable winners.
- Updated live winners-only evaluation to recognize explicit `NO_TRADE` markers as no-winner states instead of treating them as ordinary failed winners.
- Ensured holdout, historical audit, walk-forward audit, governance policy selection, and pipeline winner counts skip explicit no-trade markers.
- Aligned nested `RegimeConfig.valid_until` timestamps with the production retrain schedule instead of leaving one winner path on a fixed 60-day timestamp.

### Artifact Contract

- Bumped the pipeline artifact version to `2026-04-25-active-recent-risk-mgmt`.
- Added data workflow and risk-management selection fields to artifact metadata so stale ledgers are invalidated when these contracts change.
- Added `data_window_meta` to symbol artifacts so generated ledgers explain the M5 window and timestamp boundaries used for optimization.

### Tests

- Added active-recent M5 workflow index tests.
- Added PipelineConfig coverage for workflow and mandatory risk-management defaults.
- Added artifact metadata assertions for workflow/risk-management fields.
- Added live-loop margin protection wiring coverage.
- Added optimizer hardening coverage for signal-bar regime bucketing, validation-aware regime fallback scoring, and `NO_TRADE` winner counts.
- Added broker-spec cache invalidation coverage.
