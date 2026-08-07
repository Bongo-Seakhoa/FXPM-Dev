# Audit Tasks

## Completed By Codex

- [DONE] Bootstrap shared coordination files and agent profile.
- [DONE] Build and run read-only evidence collector under `agent_audit/scripts/`.
- [DONE] Mine current config, winner ledger, live logs, signal ledger, trade logs, OOS manifests, drift/supervisor state, and storage state.
- [DONE] Audit metric/accounting formulas and selection gates for profitability, drawdown, Sharpe, Sortino, Calmar, expectancy, recovery, and custom metrics.
- [DONE] Audit live risk, recovery, MT5 execution, account identity, order safety, min-lot behavior, and spread/spec handling.
- [DONE] Audit optimization methodology, split geometry, OOS/PBO/false-discovery evidence, and parameter robustness.
- [DONE] Run bounded targeted tests; no full optimization and no live trade command.
- [DONE] Create Codex findings F001-F005.
- [DONE] Cross-review Claude findings F101-F113.
- [DONE] Add `Deep Internal Consistency Review` to both technical and strategic reports.
- [DONE] Add `Bleeding Edge Optimization Methodology Review` to strategic report.
- [DONE] Add `Calculation Correctness Audit` to technical report.
- [DONE] Add `High-Drawdown Trade Root Cause Review` to technical report.
- [DONE] Regenerate `TECHNICAL_AUDIT.html` and `STRATEGIC_AUDIT.html`.

## Completed By Claude Code

- [DONE] Repository cartography in `agent_audit/README.md`.
- [DONE] Position-sizing review at small balance: F101, F103.
- [DONE] Institutional-supervisor and risk-cap review: F102, F104.
- [DONE] Metric/accounting review: F105, F106.
- [DONE] High-drawdown analysis framework and MAE/MFE data-gap: F107.
- [DONE] Bleeding-edge/holdout and backtest-notional findings: F108, F109.
- [DONE] Deep corrective findings F110-F113.
- [DONE] Cross-review Codex F001-F004.

## Review Queue

- [DONE by Claude Code] Codex F005 reviewed and agreed.
- [OPTIONAL REVIEW by Claude Code] Revised `TECHNICAL_AUDIT.md` and `STRATEGIC_AUDIT.md`.

## Recommended Next Implementation Tasks

- [OPEN] Add live-account replay mode with equity, min-lot, hard-cap, blocked-trade, margin, and spread/commission/swap semantics.
- [DONE by Codex 2026-06-18] Add per-trade MAE/MFE to backtest records.
- [DONE by Codex 2026-06-18] Add expected-vs-fill slippage attribution to live/actionable logs.
- [DONE by Codex 2026-06-18] Canonicalize metric definitions across engine, dashboard, pipeline bucket metrics, benchmark tools, and reports: shared `pm_metrics.py` contract added, Calmar basis exposed, benchmark recovery factor corrected.
- [OPEN] Add account-size regimes: micro-account growth, intermediate, full safety, capital preservation, and drawdown recovery.
- [PARTIAL by Codex 2026-06-18] Add balance-tier symbol/timeframe tradeability flags: live-readiness min-lot tradeability warnings/errors and `--preflight` implemented; automatic suppression remains evidence-gated.
- [DONE by Codex 2026-06-18] Add non-gating post-selection audit or selection-bias penalty for validation-driven winners: shadow-selection audit script implemented; objective mutation remains open.

## Performance-First Implementation Pass By Codex

- [DONE] Read and executed `prompt.md` guardrails in performance-first mode.
- [DONE] Created `agent_audit/performance/` artifacts and read-only evidence builder.
- [DONE] Added MAE/MFE and underwater timing instrumentation to Python and Numba backtest paths.
- [DONE] Added live actionable expected-vs-fill entry slippage attribution.
- [DONE] Fixed dashboard all-winner profit factor sentinel.
- [DONE] Generated 42 strategy-family anatomy files from the historical 157-row ledger snapshot. These files are not the current live-book count after the 2026-06-18 `pm_configs.json` rewrite.
- [DONE] Ran bounded validation: 247 focused tests passed plus 11 subtests, compile checks passed, JSON validation passed.
- [DONE by Claude Code] Claude Code review of PERF-FIX-001/002/003.
- [DONE by Codex] Implemented PERF-FIX-004 schema-version metadata after Claude challenge.
- [DONE by Claude Code] Claude Code re-review of PERF-FIX-004 accepted schema-version metadata.

## Performance Redo / Comms Correction By Codex

- [DONE] Checked `agent_audit/` communications and found stale partner-review status.
- [DONE] Added `agent_audit/performance/COMMUNICATIONS_CHECK.md`.
- [DONE] Rewrote `FINAL_PERFORMANCE_REPORT.md` to match the required section 1-15 structure.
- [DONE] Regenerated `PERFORMANCE_COMPARISONS.csv` with required columns.
- [DONE] Regenerated all 42 strategy anatomy files with required prompt sections and strategy code anchors.
- [DONE] Updated `RECOMMENDATION_TRIAGE.md`, `OPTUNA_NUMBA_COMPATIBILITY.md`, `FIX_REGISTER.md`, `ACCEPTED_FIXES.md`, `OPEN_QUESTIONS.md`, and `PARTNER_REVIEW.md`.
- [DONE] Ran redo validation: 247 focused tests plus 11 subtests passed.

## Aligned Fix Implementation By Codex

- [DONE] Implemented PERF-FIX-005 canonical metric contract and Calmar/recovery basis fields.
- [DONE] Implemented PERF-FIX-006 live-readiness min-lot tradeability checks and `--preflight`.
- [DONE] Implemented PERF-FIX-007 bounded replay and shadow-selection audit tooling.
- [DONE] Ran USD 10,000 replay for the current logical promoted live-book cell and wrote `agent_audit/performance/data/promoted_cell_replay_10000.csv`.
- [DONE] Ran shadow-selection audit and wrote `agent_audit/performance/data/shadow_selection_audit.csv`.
- [DONE] Ran focused validation: 170 pytest tests passed across metric, dashboard, backtester, readiness, benchmark, scoring, and pipeline surfaces.
- [DONE by Claude Code] Claude Code review of PERF-FIX-005/006 accepted canonical metrics and live-readiness preflight.
- [DONE by Claude Code + Codex] Claude Code accepted PERF-FIX-007 per-cell replay but found the symbol-level enumeration defect; Codex fixed it and added regression tests.

## Decisive Current-Ledger Correction By Codex

- [DONE] Verified Claude's current-ledger tally directly from `pm_configs.json`: 96 symbols `NO_TRADE`, 3 `BLOCKED`, 1 `PROMOTED`; 2387 regime leaves `NO_TRADE`, 12 `BLOCKED`, 1 `PROMOTED`.
- [DONE] Fixed `run_promoted_cell_replay.iter_promoted_cells` to include symbol-level promoted winners, include regime-leaf winners, de-duplicate symbol+leaf duplicates, and label `promotion_source`.
- [DONE] Fixed `shadow_selection_audit.iter_cells` to use the same promotion enumeration semantics.
- [DONE] Updated `claude_replay_all_promoted.py` to reuse the canonical fixed enumerator.
- [DONE] Added `tests/test_performance_replay_tools.py` covering symbol-level enumeration and symbol+leaf de-duplication.
- [DONE] Regenerated current-ledger evidence: `current_ledger_inventory.json`, `current_promoted_cells.csv`, `promoted_cell_replay_10000.csv`, `promoted_winner_replay_all_10000.csv`, and `shadow_selection_audit.csv`.
- [DONE] Marked the old 157-cell evidence as historical/stale for current live-book decisions.
- [OPEN] Determine whether the current one-winner `pm_configs.json` is final or mid-reoptimization; do not run live on it before this is resolved.

## Post-Optimization Current-Book Tasks

- [DONE by operator] Optimization completed on 2026-06-19.
- [DONE by Codex] Reran corrected promoted-book list: 117 logical promoted cells.
- [DONE by Codex] Reran corrected all-promoted USD 10,000 replay: 117 rows.
- [DONE by Codex] Reran shadow-selection audit: 117 rows.
- [DONE by Codex] Refreshed current-ledger inventory and performance evidence manifest.
- [DONE by Codex 2026-07-07] Add/verify `PALLADIUM` broker instrument spec and replay that row; real-spec replay still fails gates (`net=-1516.64`, PF `0.89`, DD `20.89%`), so it remains blocked.
- [OPEN] Decide whether `PROMOTE_SMALL` cells are live-eligible, small-size-only, or paper-only.
- [DONE by Claude Code + Codex 2026-07-07] Quarantine or review 38 replay-negative cells with PF below 1.0 before live use; replay audit blocked 55 failed/unacceptable cells, and Codex refreshed the 62-cell survivor replay with PASS 62 / FAIL 0 / UNVERIFIED 0.
- [PARTIAL by Codex 2026-07-07] Run live readiness/preflight against the completed book; preflight remains blocked by account equity `5.14 < 20.00` and stale winner-ledger freshness, so do not trade live.

## July 2026 Replay-Gated Pipeline Hardening By Codex

- [DONE] Set up and used append-only comms/watcher workflow for the multi-agent pass.
- [DONE] Implemented case/alias-insensitive broker-spec lookup in `pm_core.py`.
- [DONE] Added production replay-audit config fields to `PipelineConfig`.
- [DONE] Wired the production replay audit gate into `PortfolioManager.initial_optimization()` using Claude's audit tool without editing that tool or `pm_configs.json`.
- [DONE] Added opt-in timeframe-aware validation floors in `RegimeOptimizer`.
- [DONE] Added regression tests for broker-spec alias lookup, replay-audit apply integration, and timeframe floor behavior.
- [DONE] Refreshed read-only current survivor replay and report-only audit evidence for the 62-cell audited book.
- [OPEN] Resolve root config-integrity cleanup: move/remove root `pm_configs.backup_replay_audit_*.json` artifacts and decide whether canonical `config.json` should restore `risk_per_trade_pct=1.5` or update the test expectation.
- [OPEN] Continue CC-AB-4: replace thin validation-slice objective evidence with longer-window/full-replay correlated evidence.
