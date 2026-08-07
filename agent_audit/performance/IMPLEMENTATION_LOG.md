# Performance Implementation Log

## Performance Relevance - PERF-FIX-001 MAE/MFE Trade Excursion Instrumentation

### Direct profitability mechanism
MAE/MFE exposes whether drawdown is paid for. It allows future candidate exits/filters/objectives to retain profitable pain while removing trades that consume risk and costs without recovering.

### Evidence
Prior findings F107 and F005 confirmed no per-trade MAE/MFE and incomplete execution path attribution. `pm_core.py` trade records currently include PnL, R, exit reason, SL, and TP, but no excursion fields.

### Code-level location
`pm_core._backtest_loop_numba`, `pm_core.Backtester._run_python_loop`, Numba result reconstruction in `Backtester.run`, and `tests/test_backtester.py`.

### Production decision affected
Future winner selection, exit/filter candidate validation, and high-drawdown trade classification. This change is measurement-only and must not alter trade decisions.

### Minimum useful experiment
Run deterministic Python and Numba backtest fixtures and assert identical trades plus equivalent MAE/MFE fields.

### Optuna impact
No search-space change. Trial outputs gain additional fields for downstream objective/analysis use. Current objective behavior remains unchanged.

### Numba impact
Touches compiled hot path. Add scalar tracking and fixed-size arrays using float64/int64. Maintain nopython mode and Python/Numba parity.

### Compute cost
Low.

### Disposition
Implemented and accepted as measurement-only instrumentation.

### Validation result
`tests/test_backtester.py` passed, including deterministic Python/Numba MAE/MFE parity. The change did not alter entry/exit decisions.

## Performance Relevance - PERF-FIX-002 Live Entry Slippage Attribution

### Direct profitability mechanism
Expected-versus-filled entry slippage identifies execution leakage that can turn apparently profitable strategies into low-edge churn after costs.

### Evidence
F005 confirmed live logs record broker result price but do not persist expected-vs-fill slippage fields. Backtester applies slippage only to stop exits.

### Code-level location
`pm_main.ProductionLiveTrader._execute_entry`, nested `_record_actionable`.

### Production decision affected
Execution monitoring, future cost stress, and strategy/session filtering. Measurement-only; no trade decisions change.

### Minimum useful experiment
Unit/static validation that actionable payload can include expected/fill/slippage fields and existing callers remain compatible.

### Optuna impact
No immediate Optuna change. Provides future cost data for objective/candidate stress.

### Numba impact
None; live logging path only.

### Compute cost
Low.

### Disposition
Implemented and accepted as measurement-only live/actionable logging.

### Validation result
`tests/test_live_loop_integration.py` and `tests/test_config_source_of_truth.py` passed after the payload extension.

## Performance Relevance - PERF-FIX-003 Metric Formula Isolation

### Direct profitability mechanism
Fixing no-loss PF and naming metric bases prevents the dashboard/tools from misclassifying profitable candidates or misleading operator decisions.

### Evidence
F112 confirmed dashboard profit factor returns `0.0` when gross loss is zero, while engine uses `inf` for profitable no-loss samples.

### Code-level location
`pm_dashboard/analytics.py`, `tests/test_dashboard_trade_enrichment.py` or new focused tests.

### Production decision affected
Reporting and future selection interpretation. No trading decision changes directly.

### Minimum useful experiment
No-loss profitable sample returns infinite/explicit PF rather than zero.

### Optuna impact
None unless dashboard metrics are later fed into selection.

### Numba impact
None.

### Compute cost
Low.

### Disposition
Implemented and accepted as reporting correctness fix.

### Validation result
`tests/test_dashboard_trade_enrichment.py` passed, including an all-winner sample that now returns infinite profit factor.

## Performance Relevance - PERF-FIX-004 Measurement Schema Versioning

### Direct profitability mechanism
Schema versioning prevents old trade/actionable artifacts that lack MAE/MFE or slippage fields from being interpreted as zero-excursion or zero-slippage evidence. That protects future winner selection and drawdown/root-cause work from false profit conclusions.

### Evidence
Claude Code review accepted PERF-FIX-001/002/003 but challenged the missing result-schema version required by `prompt.md` section 17. Current new fields are additive but not labeled with a schema version.

### Code-level location
`pm_core.Backtester.run`, `pm_core.Backtester._run_python_loop`, and `pm_main.LiveTrader._execute_entry` nested `_record_actionable`.

### Production decision affected
Future replay/live-log parsers can distinguish pre-instrumentation records from post-instrumentation records before using MAE/MFE or slippage data in promotion, exit, or objective experiments.

### Minimum useful experiment
Focused backtester and live-loop tests asserting the schema version field is emitted with the new measurement fields.

### Optuna impact
No search-space or objective change. Additive metadata only; future trial/export consumers can gate on schema version.

### Numba impact
No compiled kernel change; schema version is added at Python trade-dict reconstruction and Python fallback trade records.

### Compute cost
Low.

### Disposition
Proceed.

## Performance Relevance - PERF-FIX-005 Canonical Metric Contract

### Direct profitability mechanism
Prevents metric-name drift from causing bad promotion, reporting, or operator decisions. Specifically separates cumulative return/DD from annualized Calmar and fixes benchmark recovery-factor semantics.

### Evidence
F105/F112 and partner review aligned on metric/reporting correctness as a safe fix lane.

### Code-level location
`pm_metrics.py`, `pm_core.py`, `pm_dashboard/analytics.py`, `pm_pipeline.py`, `tools/benchmark.py`, `tests/test_metric_contract.py`.

### Production decision affected
Reporting and future shadow objective ranking. No trading behavior changed.

### Validation result
`tests/test_metric_contract.py tests/test_dashboard_trade_enrichment.py tests/test_backtester.py -q` -> 28 passed.

## Performance Relevance - PERF-FIX-006 Live Readiness Tradeability

### Direct profitability mechanism
Surfaces broker min-lot risk geometry before live startup so a micro-account winner is not assumed deployable at target risk when it can only trade oversized or be blocked.

### Evidence
F101/F103/F108/F110/F113 converged on min-lot/backtest/live transfer gaps.

### Code-level location
`pm_live_readiness.py`, `pm_main.py`, `tests/test_profitability_readiness_blueprint.py`.

### Production decision affected
Preflight/readiness only. Live entries are not placed by this change.

### Validation result
`tests/test_profitability_readiness_blueprint.py tests/test_config_source_of_truth.py -q` -> 72 passed.

## Performance Relevance - PERF-FIX-007 Bounded Replay And Shadow Audit

### Direct profitability mechanism
Produces the evidence needed before changing TP/trailing filters or objective scoring. It exposes paid/unpaid drawdown and validation-surface fragility without altering strategy behavior.

### Evidence
Claude CC-AB-1 through CC-AB-4 and F002/F106/F107/F111.

### Code-level location
`agent_audit/performance/scripts/run_promoted_cell_replay.py`, `agent_audit/performance/scripts/shadow_selection_audit.py`, `agent_audit/performance/data/promoted_cell_replay_10000.csv`, `agent_audit/performance/data/shadow_selection_audit.csv`.

### Production decision affected
Evidence only. Strategy/objective mutations remain partner-review gated.

### Validation result
Compile passed. One-cell replay completed for `FORD H4 CHOP DoubleEMATrendStrategy`; shadow audit completed and flagged extreme validation Sharpe without PBO.

## 2026-06-18 Decisive Replay Correction

Claude Code found that the current `pm_configs.json` live book is not the earlier 157-cell ledger. Codex independently verified the current tally:

- 96 symbol-level `NO_TRADE`, 3 `BLOCKED`, 1 `PROMOTED`.
- 2387 regime leaves `NO_TRADE`, 12 `BLOCKED`, 1 `PROMOTED`.
- One logical promoted winner: `FORD H4 CHOP DoubleEMATrendStrategy`.

Claude also found that the original Codex replay enumerator could miss symbol-level promoted winners. Codex fixed `run_promoted_cell_replay.iter_promoted_cells`, mirrored the fix in `shadow_selection_audit.iter_cells`, updated `claude_replay_all_promoted.py` to reuse the canonical enumerator, added `tests/test_performance_replay_tools.py`, and regenerated current-ledger evidence.

Corrected $10k replay result: `FORD H4 CHOP DoubleEMATrendStrategy` loses `-2317.84` (`-23.18%`), PF `0.66`, 166 trades, max DD `24.88%`.
