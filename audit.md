# FXPM Final Comprehensive Audit

Date: 2026-04-25
Scope: current FXPM codebase, active high-risk configuration, optimizer/backtester/live propagation, generated artifacts, storage/cache behavior, dashboard visibility, documentation state, and intent alignment.
Constraint honored: full-universe `python pm_main.py --optimize` was not run.

---

## Executive Summary

The FXPM codebase is materially stronger than the older audit trail. The current architecture now has a coherent active-recent M5 workflow, regime-aware winner selection, Stage 3 risk/governance selection, shared strategy trade-intent exits, storage governance, and a much better live safety envelope than the previous versions.

The core methodology is broadly aligned with the stated PM intent: maximize quality, profitability, accuracy, actionability, and only then efficiency. The audit does not treat known low-balance broker rejections as PM failures. A $25 high-risk live account will naturally reject otherwise valid symbols when minimum lot, margin, spread, stop-distance, or broker feasibility makes the trade impossible. That is an expected execution constraint, not a strategy-selection defect.

Post-audit implementation update: the code-level findings F1, F4, F5, F6, and F7 have now been patched. F2 remains intentionally dependent on the operator's full `python pm_main.py --optimize --overwrite` regeneration run. F3 remains a source-control/deployment boundary decision because staging or committing files is outside a code patch.

At the time of the audit snapshot, the strongest remaining issues were propagation and live-readiness issues:

1. Live config eligibility used `is_validated` only, while retrain/status logic used expiry and artifact fingerprints. This has been corrected with a separate live eligibility surface.
2. The active high-risk ledger is partial and mixed-version: 14 of 62 symbols are configured, 59 symbols are due for optimization, and most persisted artifacts predate the latest contract.
3. Several core runtime files are untracked by git, so a git-based deploy would not reproduce the working local system.
4. Two live safety edges remained: non-finite margin values and stale same-symbol combined-risk input. Both have been patched.
5. Dashboard defaults hid several margin/low-balance skip actions. The dashboard defaults now surface those actions.

No evidence was found that the active data split philosophy itself is broken. The active recent-M5 workflow, the `historical_stress_audit` naming, and the rejection of walk-forward as a mandatory intermediary promotion layer remain appropriate for the PM's stated live-trading objective.

---

## Verification Performed

- Ran `python -m pytest -q`.
  - Result: `541 passed, 1 skipped, 350 subtests passed`.
- Ran `python pm_main.py --status`.
  - Result: active ledger `pm_configs_high_risk.json`, 62 managed symbols, 14 configured, 14 valid, 0 expired, 0 invalid, 59 needing optimization.
- Inspected `config.json`, `pm_configs_high_risk.json`, `pm_outputs/storage_state.json`, core runtime modules, optimizer/backtester modules, storage manager, dashboard config/parsers, docs, and tests.
- Inspected active storage state.
  - Storage pressure: `normal`.
  - `data/.cache`: approximately 667 MB in the latest storage state.
  - `data` directory: approximately 1.93 GB in the latest storage state.
  - Cache quota: 4 GB, 14-day retention in active config.
- Did not run full optimization.

---

## Current System State

### Active Configuration

The active runtime profile is `config.json` with:

- `pipeline.winner_ledger_path = "pm_configs_high_risk.json"`
- `pipeline.data_workflow_mode = "active_recent_m5"`
- `pipeline.max_bars = 300000`
- `pipeline.historical_stress_audit_bars = 50000`
- `pipeline.active_universe_bars = 250000`
- `pipeline.risk_management_optimization_enabled = true`
- `pipeline.risk_management_selection_stage = "stage3"`
- `pipeline.production_retrain_mode = "notify"`
- `storage_resample_cache_max_gb = 4.0`
- `storage_resample_cache_max_age_days = 14`
- `position.risk_per_trade_pct = 2.0`
- `position.max_risk_pct = 3.0`

This is an aggressive high-risk proof profile. It is not intended to simulate a conservative institutional account.

### Active High-Risk Ledger

`pm_configs_high_risk.json` currently contains:

- 14 configured symbols out of 62.
- 108 regime/timeframe slots.
- 69 tradeable slots.
- 39 explicit no-trade slots.
- 11 symbols on artifact contract `2026-03-29b / 2026-03-29a`.
- 3 symbols on artifact contract `2026-04-25-active-recent-risk-mgmt / 2026-04-25-active-recent-m5`.
- 0 symbols with compact `validation_evidence`.
- 0 symbols with `ledger_status`.
- 0 symbols with `data_window_fingerprint`.
- 0 symbols with non-zero top-level `robustness_ratio`.
- Current `valid_until` values are `2026-04-26T00:01:00`.

This ledger should be treated as an in-progress operational artifact, not the final optimized high-risk universe.

### Code/Test State

The unit test suite is healthy. The current tests cover a wide surface, including strategy execution, pipeline artifacts, storage governance, dashboard behavior, live loop behavior, and config source-of-truth rules.

The main test gaps are targeted rather than broad:

- Live eligibility should be tested against expired configs and artifact-due configs.
- Margin gates should be tested with `NaN`, `inf`, and broker-null values.
- Same-symbol combined risk should be tested against a fresh broker read that differs from the sweep snapshot.
- Dashboard defaults should be tested for new margin and low-balance skip actions.

---

## Methodology Alignment

### Data Workflow

The active data workflow is aligned with the PM intent:

- The latest 300,000 M5 bars define the workflow window.
- The oldest 50,000 M5 bars inside that window are `historical_stress_audit`.
- The newest 250,000 M5 bars form the active universe.
- Stage 2 selection works on the active recent universe.
- Stage 3 risk/governance selection uses the freshest Stage 2 selection surface.

The older 50,000 M5 bars are correctly not a forward-looking holdout. They are older out-of-selection context for catastrophic fragility checks.

### Walk-Forward Position

Walk-forward evaluation should remain an offline audit report, not a mandatory promotion gate between Stage 2/Stage 3 and live implementation. Adding it as an intermediary selection layer would reduce freshness and conflict with the current live-relevance objective.

### Backtest Balance Versus Live Balance

Using a larger backtest balance for strategy discovery is acceptable. The backtest is primarily selecting strategies, parameters, risk-management behavior, symbol/timeframe/regime winners, and order-management behavior. Live feasibility is handled by sizing, margin, broker constraints, minimum lot rules, and signal-ledger visibility.

Low-balance rejections are expected. They only become deeper PM failures if symbols continue failing after the account is large enough for the required minimum lot, margin, and broker constraints.

### Regime Methodology

The live/optimization regime alignment is now conceptually coherent:

- Optimization buckets trades by `REGIME_LIVE` at the signal bar.
- Live winner lookup prefers `REGIME_LIVE` / `REGIME_STRENGTH_LIVE` on the last closed bar.
- `REGIME` is a fallback, not the primary live decision surface.

This is the right compromise between backtest parity and live feasibility. Parallel regime code paths are not automatically wrong; the important condition is that they share the same decision-time contract, and they now largely do.

### Exit Surface And TP Behavior

Backtester and live execution now share the strategy `build_trade_intent()` surface. The backtester computes stops and take-profits at the signal bar and installs configured regime TP multipliers before strategy evaluation. The artifact contract also records the exit-surface contract and regime TP multipliers.

No current evidence suggests that optimization is selecting on one TP surface while live trades another materially different one.

---

## Findings

### F1 - Live Eligibility Does Not Enforce Expiry Or Artifact Due Status

Severity: High
Status: Resolved in code after this audit.
Impact: Profitability, safety, selection freshness, live/live-status consistency
Files: `pm_pipeline.py`, `pm_main.py`

`ConfigLedger.has_valid_config()` correctly rejects missing, invalid, no-expiry, and expired configs. `ConfigLedger.should_optimize()` also detects artifact fingerprint drift. But `PortfolioManager.get_validated_configs()` currently returns every config with `is_validated = true`.

A direct probe confirmed the mismatch:

- `ledger.has_valid_config("EURUSD")` returned expired.
- `portfolio_manager.get_validated_configs()` still returned `EURUSD`.

Live trading uses `get_validated_configs()`, so an expired config can remain live-tradeable. Artifact-due configs are also still tradeable even though `--status` marks them `DU`.

Implementation:

- `get_live_eligible_configs()` now separates live trading eligibility from raw validated-ledger reads.
- Live trading uses the live-eligible surface.
- Expired, no-expiry, no-winner, and artifact-drifted configs are blocked under `live_artifact_drift_policy = "block"`.
- `--status` now reports live eligibility and marks blocked configs with `BL`.
- Artifact invalidation now compares only semantic contract keys so volatile ledger metadata does not make fresh optimized configs immediately stale.
- Focused regression tests cover expiry and artifact-drift behavior.

This is the most important code-level finding in the audit.

### F2 - Active High-Risk Ledger Is Not Final Or Fully Propagated

Severity: High
Impact: Production readiness, ranking truthfulness, operator interpretation
Files: `pm_configs_high_risk.json`, `pm_pipeline.py`, `config.json`

The active high-risk ledger is incomplete:

- 14 configured symbols out of 62.
- 59 symbols currently need optimization according to `python pm_main.py --status`.
- 11 of the 14 configured symbols are due because the artifact fingerprint changed.
- The current ledger lacks the newer validation evidence, ledger status, data-window fingerprint, and non-zero robustness evidence fields.

This does not prove that the optimizer is wrong. It means the generated ledger is not yet the final optimized production artifact.

Recommendation:

- Do not evaluate the final PM quality from the current high-risk ledger.
- After the next deliberate optimization, verify that the new ledger includes:
  - expected symbol coverage
  - artifact contract consistency
  - validation evidence
  - data-window fingerprint
  - ledger completion status
  - non-zero robustness evidence where applicable
- Keep `--status` as the first operational readiness check before live deployment.

### F3 - Git/Deployment Boundary Is Not Clean

Severity: High
Impact: Reproducibility, deployment safety, handoff reliability
Files: workspace/git state

Several core files that the local system now depends on are untracked by git, including:

- `pm_storage.py`
- `pm_order_governance.py`
- `pm_dashboard/ledger.py`
- `pm_configs_high_risk.json`
- `audit.html`
- several tests, including storage/config-source tests

There are also tracked deletions such as:

- `Analysis.md`
- `Normal config (Full Equity).json`

The local workspace can run because those files exist on disk. A git-based deployment or backup from tracked files only would not reproduce the working PM.

Recommendation:

- Before any live deployment or remote handoff, decide which generated artifacts should remain untracked and which source/runtime modules must be tracked.
- At minimum, source modules and tests should be tracked.
- If `pm_configs_high_risk.json` is the active production ledger, either track it deliberately or document the deployment process that provisions it.

### F4 - Margin Guards Do Not Normalize Non-Finite Values

Severity: Medium-High
Status: Resolved in code after this audit.
Impact: Live safety, broker edge-case handling
File: `pm_main.py`

`_safe_account_margin_level()` converts values with `float(raw_level)` but does not reject `NaN` or `inf`. `float("nan")` succeeds. In the entry path, this can avoid both the missing-margin fail-closed branch and the normal margin-level comparisons.

The margin protection cycle is mostly conservative when classification sees `NaN`, but the entry gate should not depend on that downstream behavior.

Implementation:

- Non-finite margin level, free margin, and required margin are treated as unavailable.
- Entries fail closed with `SKIPPED_MARGIN_UNAVAILABLE` where broker/account margin data is unsafe.
- Regression tests cover `NaN`/`inf` margin cases.

### F5 - Same-Symbol Combined Risk Still Uses Stale Sweep Snapshot

Severity: Medium-High
Status: Resolved in code after this audit.
Impact: Live risk accuracy, duplicate/exposure protection
File: `pm_main.py`

The exact duplicate-position guard now performs a fresh broker-side read immediately before sending an order. That is a meaningful improvement.

However, the same-symbol combined-risk check later still receives the original `positions_snapshot`, not the fresh symbol-level positions already fetched during the duplicate check. If another same-symbol/different-magic position appears after the sweep snapshot, the exact duplicate guard can pass while the combined symbol-risk cap evaluates stale exposure.

Implementation:

- The fresh symbol-level broker positions read during the final duplicate guard are now passed into `_check_symbol_combined_risk_cap()`.
- A regression test verifies the combined-risk cap receives the fresh broker-side symbol snapshot.

### F6 - Dashboard Defaults Hide New Margin/Low-Balance Skip Actions

Severity: Medium
Status: Resolved in code after this audit.
Impact: Actionability, low-balance proof observability
Files: `pm_dashboard/utils.py`, `pm_dashboard/dashboard_config.json`, `pm_dashboard/watcher.py`

The live system records useful skip actions such as:

- `SKIPPED_MARGIN_UNAVAILABLE`
- `SKIPPED_MARGIN_BLOCKED`
- `SKIPPED_MARGIN_REOPEN_WAIT`
- `SKIPPED_MARGIN_COOLDOWN`
- `SKIPPED_MARGIN_REQUIRED`
- `BLOCKED_MIN_LOT_EXCEEDS_CAP`

Before the patch, the dashboard display filter had a non-empty `display_actions` list and no `display_action_prefixes`, so important small-account feasibility telemetry could be written to ledgers/logs but hidden from the operator dashboard.

Implementation:

- Dashboard defaults now include margin and minimum-lot feasibility actions and prefixes.
- Regression tests verify those entries display and validate correctly.
- Noisy `NO_ACTIONABLE_*` entries remain excluded.

### F7 - Storage/Cache Implementation Is Healthy, But State Semantics Need Tightening

Severity: Medium-Low
Status: Resolved in code after this audit.
Impact: Long-run operations, interpretation clarity
Files: `pm_storage.py`, `pm_core.py`, `pm_outputs/storage_state.json`

The cache/storage design is in a good zone:

- Active cache quota is 4 GB, not overly conservative.
- Current cache use is well below quota.
- Housekeeping is observe-only by default.
- PM-owned cleanup candidates are zero in the latest state.
- Resample-cache telemetry exists for memory hits, disk hits, misses, invalidations, bytes, and timing.
- Tests cover cache invalidation, pruning, and storage cleanup.

The main issue is interpretability. `storage_state.json` has a current housekeeping timestamp, but `last_sweep` and several `next_*` timestamps can remain stale from an older live session. That can mislead an operator if the state file is read as one current snapshot.

Implementation:

- Storage state now includes `state_updated_at` and a `freshness` block with last-sweep and housekeeping ages/freshness flags.
- The 4 GB cache cap remains unchanged.
- MetaQuotes/external cleanup remains observe-only unless deliberately configured otherwise.

### F8 - Production Retrain Mode Is Notify, Not Auto

Severity: Medium-Low
Impact: Operational freshness
File: `config.json`

The active config uses `production_retrain_mode = "notify"`. This is valid if the operator wants manual control, but it means due symbols are announced rather than automatically refreshed.

Given that the current status reports 59 symbols needing optimization, this setting should be treated as an operational decision, not a background automation guarantee.

Recommendation:

- Keep `notify` if manual retrain control is desired.
- Use `auto` only when the machine, broker connection, storage, and time budget are ready for unattended retraining.
- Do not assume `notify` will keep the ledger fresh by itself.

---

## Non-Issues Confirmed

These areas were reviewed and should not be treated as defects without new evidence:

- The active-recent M5 workflow is not broken by not being a traditional academic train/validation/holdout design.
- `historical_stress_audit` is the correct name for the oldest 50,000 M5 bars.
- Walk-forward should remain an offline audit, not a mandatory live-promotion gate.
- Larger backtest notional balance is acceptable for strategy discovery.
- Low-balance live rejections are expected and should not be over-interpreted.
- Regime live/optimization logic is now aligned around `REGIME_LIVE` at decision time.
- Backtester and live execution now share the strategy trade-intent exit surface.
- The 4 GB cache setting is reasonable; there is no evidence that shrinking cache would improve PM quality.

---

## Patch Order

1. Completed: live eligibility semantics now prevent live use of expired/no-winner/artifact-drifted configs under the active strict policy.
2. Remaining operator/deployment task: clean the git/deployment boundary so source modules, tests, and active runtime expectations are reproducible.
3. Completed: non-finite margin handling and tests.
4. Completed: fresh symbol positions now feed the same-symbol combined-risk cap.
5. Completed: dashboard action filters now surface margin and low-balance feasibility events.
6. Remaining F2 task: regenerate the high-risk ledger deliberately, then verify contract coverage and evidence propagation.
7. Completed: storage-state freshness labels were added without reducing useful cache capacity.

---

## Final Assessment

FXPM is directionally in a much better state than it was before the recent patches. The methodology is sound enough to continue with the active recent-M5, Stage 3 risk-management workflow. The code-level safety and actionability issues from this audit have now been handled.

The codebase should not be considered production-clean until the high-risk ledger is regenerated and the git/deployment boundary is cleaned up, but the remaining work is now narrower and operational rather than architectural.
