# Accepted Fixes

| Fix | Files Changed | Validation | Status |
|---|---|---|---|
| PERF-FIX-001: per-trade MAE/MFE instrumentation | `pm_core.py`, `tests/test_backtester.py` | Backtester Python/Numba parity test; focused backtester suite passed. | Accepted by Claude |
| PERF-FIX-002: live entry slippage attribution | `pm_main.py` | Compile check; `test_live_loop_integration.py`; `test_config_source_of_truth.py`. | Accepted by Claude |
| PERF-FIX-003: dashboard PF no-loss sentinel | `pm_dashboard/analytics.py`, `tests/test_dashboard_trade_enrichment.py` | Dashboard focused tests passed. | Accepted by Claude |
| PERF-FIX-004: measurement schema versioning | `pm_core.py`, `pm_main.py`, `tests/test_backtester.py`, `tests/test_config_source_of_truth.py` | Compile check; 37 focused tests passed. | Accepted by Claude |
| PERF-FIX-005: canonical metric contract and Calmar basis | `pm_metrics.py`, `pm_core.py`, `pm_dashboard/analytics.py`, `pm_pipeline.py`, `tools/benchmark.py`, `tests/test_metric_contract.py` | Focused metric/dashboard/backtester tests passed. | Accepted by Claude |
| PERF-FIX-006: live readiness min-lot tradeability and preflight | `pm_live_readiness.py`, `pm_main.py`, `tests/test_profitability_readiness_blueprint.py` | Readiness tests passed. | Accepted by Claude |
| PERF-FIX-007: bounded replay and shadow selection tooling | `agent_audit/performance/scripts/run_promoted_cell_replay.py`, `agent_audit/performance/scripts/shadow_selection_audit.py`, `agent_audit/performance/scripts/claude_replay_all_promoted.py`, `tests/test_performance_replay_tools.py` | Compile passed; list-only replay, all-promoted replay, shadow audit, and enumerator regression tests passed. | Accepted with enumerator correction applied |

## Accepted Comparison Rows

See `agent_audit/performance/PERFORMANCE_COMPARISONS.csv`.

## Partner Review

Claude Code accepted PERF-FIX-001 through PERF-FIX-006 on 2026-06-18. Claude accepted PERF-FIX-007's per-cell replay logic but found that the original enumerator under-enumerated symbol-level promoted winners. Codex fixed the enumerator, added regression tests, and regenerated current-ledger evidence.

Current live-book finding agreed by Claude and Codex: the rewritten `pm_configs.json` contains one logical promoted winner, `FORD H4 CHOP DoubleEMATrendStrategy`, and the corrected $10k replay loses `-23.18%` with PF `0.66`.
