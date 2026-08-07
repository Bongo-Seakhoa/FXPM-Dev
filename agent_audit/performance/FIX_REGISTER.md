# Fix Register

## FIX-001: Per-Trade MAE/MFE Instrumentation

## Status
Accepted / Implemented

## Category
Metric / Numba / Reporting

## Direct profitability mechanism
Identifies whether high drawdown is profitable pain or bad risk, enabling future exit/filter/objective changes to remove unpaid drawdown without destroying profitable trade flow.

## Evidence
F107: no per-trade MAE/MFE fields. Trade records in `pm_core.py` contain final PnL/R but not adverse/favorable excursion.

## Exact code locations
`pm_core._backtest_loop_numba`; `pm_core.Backtester._run_python_loop`; Numba result conversion in `pm_core.Backtester.run`; `tests/test_backtester.py`.

## Proposed implementation
Track per-open-trade adverse/favorable excursion in pips, R, time-to-MAE bars, time-to-MFE bars, and time-underwater bars. Add fields to trade records. Do not change entries/exits.

## Optuna compatibility
- Search-space impact: none.
- Objective impact: none in this pass.
- Trial metadata impact: trade records can include additional fields.
- Serialization impact: additive fields only.
- Resume compatibility: existing studies unaffected.
- Winner-loading impact: none.

## Numba compatibility
- Hot-path impact: yes.
- Types: float64 and int64 arrays.
- Python reference path: same field names.
- Compiled path: nopython scalar/array tracking.
- Parity test: deterministic fixture comparing Python and Numba fields.
- Compile/runtime implications: low overhead.

## Expected trading impact
Measurement-only initially; protects net profit by enabling evidence-based drawdown/exit improvements.

## Risk of unintended consequences
Could slow hot path slightly. Must not change trade decisions.

## Minimum bounded experiment
Focused backtester tests with known MAE/MFE and Python/Numba parity.

## Compute budget
Unit tests only.

## Results
Implemented in `pm_core.py`; validated by `tests/test_backtester.py::test_trade_excursion_fields_match_python_and_numba` and the focused backtester suite.

## Verdict
Measurement Only accepted.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted as measurement-only, no trading-decision impact.

## FIX-002: Live Entry Slippage Attribution

## Status
Accepted / Implemented

## Category
Execution / Cost / Reporting

## Direct profitability mechanism
Quantifies live fill leakage so cost-sensitive strategies and sessions can be corrected or stressed.

## Evidence
F005: result price is logged, expected-vs-fill slippage is not persisted.

## Exact code locations
`pm_main.ProductionLiveTrader._execute_entry`.

## Proposed implementation
Add optional expected/fill/slippage fields to actionable payload for executed and reconciled orders.

## Optuna compatibility
No direct change. Future objective stress can consume these logs.

## Numba compatibility
None.

## Expected trading impact
Measurement-only initially; protects net profit by exposing execution leakage.

## Risk of unintended consequences
Low; additive log fields.

## Minimum bounded experiment
Existing live-loop/logging tests should continue to pass; inspect payload construction.

## Compute budget
Unit tests only.

## Results
Implemented in `pm_main.py`; validated by compile checks plus `tests/test_live_loop_integration.py` and `tests/test_config_source_of_truth.py`.

## Verdict
Measurement Only accepted.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted as measurement-only, no trading-decision impact. Claude noted that exit slippage attribution remains a next measurement step.

## FIX-003: Dashboard Profit Factor No-Loss Sentinel

## Status
Accepted / Implemented

## Category
Metric / Reporting

## Direct profitability mechanism
Prevents profitable no-loss samples from being displayed as PF=0.0, which can cause incorrect operator/selection interpretation.

## Evidence
F112: engine uses `inf`; dashboard uses `0.0` when gross loss is zero.

## Exact code locations
`pm_dashboard/analytics.py`.

## Proposed implementation
Return `inf` for profitable no-loss samples, `0.0` only when no gross profit exists.

## Optuna compatibility
None.

## Numba compatibility
None.

## Expected trading impact
Measurement/reporting correction; prevents false rejection of profitable no-loss samples.

## Risk of unintended consequences
Downstream UI must tolerate infinity; existing engine already emits it.

## Minimum bounded experiment
Focused dashboard metric test.

## Compute budget
Unit tests only.

## Results
Implemented in `pm_dashboard/analytics.py`; validated by `tests/test_dashboard_trade_enrichment.py`.

## Verdict
Measurement/reporting correction accepted.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted. Claude verified dashboard JSON safety for non-finite PF conversion.

## FIX-004: Measurement Schema Versioning

## Status
Accepted / Implemented

## Category
Metric / Execution / Reporting / Data

## Direct profitability mechanism
Prevents old artifacts that lack MAE/MFE or entry-slippage fields from being read as clean zero-MAE or zero-slippage evidence. This protects future profit attribution and winner-selection experiments from false positives.

## Evidence
Claude Code accepted PERF-FIX-001/002/003 but explicitly challenged the missing result-schema version required by `prompt.md` section 17.

## Exact code locations
`pm_core.Backtester.run`; `pm_core.Backtester._run_python_loop`; `pm_main.LiveTrader._execute_entry`; focused tests.

## Proposed implementation
Add explicit schema-version constants to emitted backtest trade records and live actionable payloads:

- `trade_record_schema_version = "2026-06-18-mae-mfe-v1"`
- `actionable_schema_version = "2026-06-18-entry-slippage-v1"`

## Optuna compatibility
- Search-space impact: none.
- Objective impact: none.
- Trial metadata impact: additive metadata available to future exports.
- Serialization impact: additive string fields.
- Resume compatibility: existing studies unaffected.
- Winner-loading impact: none.

## Numba compatibility
- Hot-path impact: none.
- Types: Python dict metadata only.
- Python reference path: same emitted field.
- Compiled path: set at reconstruction after Numba returns arrays.
- Parity test: focused assertion in backtester test.
- Compile/runtime implications: negligible.

## Expected trading impact
Measurement correctness only; no trading decision impact.

## Risk of unintended consequences
Very low. Consumers that strictly reject unknown keys could need adjustment, but current code handles additive trade/actionable dictionaries.

## Minimum bounded experiment
Focused tests and compile checks.

## Compute budget
Unit tests only.

## Results
Implemented in `pm_core.py` and `pm_main.py`; validated by focused compile checks and `python -m pytest tests/test_backtester.py tests/test_config_source_of_truth.py -q` -> 37 passed.

## Verdict
Measurement Only; accepted.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted. Follow-up remains: add consumer-side guards so missing schema is treated as pre-instrumentation, not as zero excursion/slippage.

## FIX-005: Canonical Metric Contract And Calmar Basis

## Status
Accepted / Implemented

## Category
Metric / Reporting / Benchmark

## Direct profitability mechanism
Prevents same-named metrics from silently meaning different things across engine, dashboard, pipeline buckets, and benchmark tooling. This protects winner interpretation, replay comparison, and operator decisions from false positives/false rejections.

## Evidence
F105 and F112 agreed that Calmar was actually cumulative return divided by max drawdown, and dashboard/engine metric definitions diverged. Claude/Codex aligned on canonical metric/reporting correctness as a safe implementation lane.

## Exact code locations
`pm_metrics.py`; `pm_core.Backtester._calculate_metrics`; `pm_core.Backtester._empty_result`; `pm_dashboard.analytics.compute_performance_metrics`; `pm_pipeline.RegimeOptimizer._compute_bucket_metrics`; `tools/benchmark.py`; `tests/test_metric_contract.py`.

## Proposed implementation
Add a shared metric helper module. Keep legacy `calmar_ratio` for compatibility, but emit `return_drawdown_ratio`, `calmar_ratio_annualized`, `annualized_return_pct`, `calmar_ratio_basis`, and `metric_contract_version`. Use shared profit-factor sentinel semantics and correct benchmark `recovery_factor` to net PnL / dollar max drawdown.

## Optuna compatibility
No search-space or objective mutation. Existing keys remain available.

## Numba compatibility
No Numba kernel change.

## Expected trading impact
Measurement/reporting correctness; no entry/exit/risk behavior change.

## Risk of unintended consequences
Low. Additive fields plus shared helper usage. Dashboard long/short PF now also uses the profitable no-loss `inf` sentinel.

## Minimum bounded experiment
Focused metric/dashboard/backtester tests.

## Results
Implemented and validated by `tests/test_metric_contract.py`, `tests/test_dashboard_trade_enrichment.py`, and `tests/test_backtester.py`.

## Verdict
Accepted implementation lane.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted. `calmar_ratio` remains a backward-compatible alias; explicit basis fields prevent metric-name drift.

## FIX-006: Live Readiness Min-Lot Tradeability And Preflight

## Status
Accepted / Implemented

## Category
Live Readiness / Execution Safety / Tradeability

## Direct profitability mechanism
Surfaces cells whose broker minimum lot makes intended target risk impossible at current equity, before the live loop starts. This prevents hidden blocked trades, unexpected oversized min-lot trades, and false confidence about deployable winner cells.

## Evidence
F101, F103, F108, F110, and F113 converged on live/backtest sizing divergence and min-lot tradeability gaps. The agreed framing is scale-aware: micro-account aggression is intentional, but hidden execution infeasibility is not acceptable.

## Exact code locations
`pm_live_readiness.validate_live_readiness`; helper functions in `pm_live_readiness.py`; `pm_main.py --preflight`; `tests/test_profitability_readiness_blueprint.py`.

## Proposed implementation
Estimate min-lot risk from broker specs plus static stop pips or validation average-loss pips. Emit warning when min-lot risk exceeds target but remains below hard cap; emit error when it exceeds hard cap. Add `python pm_main.py --preflight` to run readiness checks and exit without entering the trading loop.

## Optuna compatibility
No optimization change.

## Numba compatibility
Not applicable.

## Expected trading impact
No direct trade decisions changed. Live startup/readiness becomes more explicit about actual deployability.

## Risk of unintended consequences
Low-medium. Cells lacking static stop/avg-loss evidence emit warnings, not blocks. Hard-cap excesses are errors because live execution would reject or violate the configured cap.

## Minimum bounded experiment
Focused readiness tests with a USD 22 equity fixture and broker min-lot geometry.

## Results
Implemented and validated by `tests/test_profitability_readiness_blueprint.py`.

## Verdict
Accepted implementation lane.

## Partner review
Reviewed by Claude Code on 2026-06-18 and accepted.

## FIX-007: Bounded Replay And Shadow Selection Audit Tooling

## Status
Accepted With Correction / Implemented

## Category
Quant Methodology / Replay Evidence / High-Drawdown Root Cause

## Direct profitability mechanism
Turns the agreed "needs evidence before strategy mutation" items into executable bounded tools. The replay emits MAE/MFE summaries from the real backtest path; the shadow audit flags sparse validation, extreme Sharpe without PBO, DayOfWeekBias concentration, and stability-penalty sensitivity without changing selection.

## Evidence
Claude partner review CC-AB-1 through CC-AB-4; F002, F106, F107, F111; `agent_audit/performance/data/current_ledger_inventory.json`; `agent_audit/performance/data/current_promoted_cells.csv`; `agent_audit/performance/data/promoted_cell_replay_10000.csv`; `agent_audit/performance/data/promoted_winner_replay_all_10000.csv`; `agent_audit/performance/data/shadow_selection_audit.csv`.

## Exact code locations
`agent_audit/performance/scripts/run_promoted_cell_replay.py`; `agent_audit/performance/scripts/shadow_selection_audit.py`; `agent_audit/performance/scripts/claude_replay_all_promoted.py`; `tests/test_performance_replay_tools.py`; `agent_audit/performance/data/promoted_cell_replay_10000.csv`; `agent_audit/performance/data/promoted_winner_replay_all_10000.csv`; `agent_audit/performance/data/shadow_selection_audit.csv`.

## Proposed implementation
Add bounded read-only replay and shadow-selection scripts. Default replay cap is small; scripts write under `agent_audit/performance/data/` and do not call live trade APIs or full optimization. After Claude review, enumerate both symbol-level promoted winners and regime-leaf promoted winners, de-duplicate symbol+leaf duplicates, and label `promotion_source`.

## Optuna compatibility
No objective/search mutation. Produces evidence for future shadow-ranking/objective tests.

## Numba compatibility
Uses existing backtester path and records emitted MAE/MFE schema version.

## Expected trading impact
Evidence-only now. Prevents premature TP/trailing/objective changes without proof.

## Risk of unintended consequences
Low. Read-only scripts only.

## Minimum bounded experiment
One-cell USD 10,000 promoted-cell replay plus shadow audit over current tradeable ledger cells.

## Results
Corrected current-ledger inventory found one logical promoted winner: `FORD H4 CHOP DoubleEMATrendStrategy`, `promotion_source=symbol_and_regime`. The $10k replay produced 166 trades, net PnL `-2317.84`, return `-23.18%`, max DD `24.88%`, PF `0.66`, avg MAE `0.7394R`, schema `2026-06-18-mae-mfe-v1`. Shadow audit found 1 row and flagged extreme validation Sharpe without PBO evidence. The old "157 tradeable cells" evidence is historical and stale for the current `pm_configs.json`.

## Verdict
Evidence tooling accepted with enumerator correction applied. The current ledger should not be run live unless it is known to be an incomplete/mid-reoptimization artifact.

## Partner review
Claude Code accepted `replay_cell` and found the symbol-level enumeration defect. Codex fixed the enumerator and added regression tests; partner re-check of the fixed evidence is requested.
