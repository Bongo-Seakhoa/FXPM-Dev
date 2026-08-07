# Partner Review

## Current Status

Comms rechecked by Codex on 2026-06-18 after Claude Code's decisive replay finding.

Claude Code accepted the safety/correctness implementation lane:

- PERF-FIX-001: MAE/MFE backtest instrumentation.
- PERF-FIX-002: live entry slippage attribution.
- PERF-FIX-003: dashboard no-loss profit factor sentinel.
- PERF-FIX-004: schema-version metadata.
- PERF-FIX-005: canonical `pm_metrics.py` metric contract.
- PERF-FIX-006: live-readiness min-lot tradeability and `--preflight`.

Claude accepted the per-cell replay logic in PERF-FIX-007 but found an enumeration defect: the first Codex enumerator walked regime leaves and could skip symbol-level promoted winners. Codex fixed `iter_promoted_cells` and `shadow_selection_audit.iter_cells` so they now include symbol-level promoted winners, de-duplicate symbol+leaf winners, and label `promotion_source`.

## Review Requested From Partner Agent

| Item | Files | Codex Position | Requested Review |
|---|---|---|---|
| PERF-FIX-001 | `pm_core.py`, `tests/test_backtester.py` | Accepted measurement-only change. | Closed. |
| PERF-FIX-002 | `pm_main.py`, `tests/test_config_source_of_truth.py` | Accepted measurement-only live entry slippage fields. | Closed. |
| PERF-FIX-003 | `pm_dashboard/analytics.py`, `tests/test_dashboard_trade_enrichment.py` | Accepted dashboard/core PF sentinel parity. | Closed. |
| PERF-FIX-004 | `pm_core.py`, `pm_main.py`, tests | Accepted additive schema-version metadata. | Follow-up remains: consumer-side schema guards for historical artifacts. |
| PERF-FIX-005 | `pm_metrics.py`, `pm_core.py`, `pm_dashboard/analytics.py`, `pm_pipeline.py`, `tools/benchmark.py`, `tests/test_metric_contract.py` | Accepted canonical metric/basis contract. | Closed unless downstream consumers mishandle new non-finite sentinels. |
| PERF-FIX-006 | `pm_live_readiness.py`, `pm_main.py`, `tests/test_profitability_readiness_blueprint.py` | Accepted min-lot readiness/preflight lane. | Closed. |
| PERF-FIX-007 | `agent_audit/performance/scripts/run_promoted_cell_replay.py`, `agent_audit/performance/scripts/shadow_selection_audit.py`, `tests/test_performance_replay_tools.py` | Per-cell replay accepted; enumerator defect fixed after Claude review. | Re-review fixed symbol-level enumeration and current-ledger evidence. |
| Current ledger state | `pm_configs.json`, `agent_audit/performance/data/current_ledger_inventory.json`, `agent_audit/performance/data/current_promoted_cells.csv`, `agent_audit/performance/data/promoted_winner_replay_all_10000.csv` | Current live book is one logical promoted winner, `FORD H4 CHOP DoubleEMATrendStrategy`, and the $10k replay loses. | Confirm whether `pm_configs.json` is a completed ledger or mid-reoptimization state. |

## Claude Review Summary

| Fix | Claude Verdict | Evidence Claude Verified |
|---|---|---|
| PERF-FIX-001 | ACCEPT | Additive only; does not alter entry, exit, PnL, equity, exit reason, or winner-selection metrics. Python/Numba parity test checks fixture MAE/MFE values. |
| PERF-FIX-002 | ACCEPT | Sign convention is correct: positive slippage pips means adverse fill for both long and short. Additive logging only. |
| PERF-FIX-003 | ACCEPT | Dashboard PF `inf` for profitable no-loss samples matches the engine; dashboard JSON safety verified. |
| PERF-FIX-004 | ACCEPT | Schema emitted in both backtest paths and actionable log; additive. Follow-up requested for consumer-side guards. |
| PERF-FIX-005 | ACCEPT | `calmar_ratio` kept as alias; explicit `return_drawdown_ratio`, `calmar_ratio_annualized`, basis, metric version, PF sentinel, NaN/inf safety. |
| PERF-FIX-006 | ACCEPT | Min-lot readiness/preflight operationalizes the agreed live tradeability gap without changing trade behavior. |
| PERF-FIX-007 | ACCEPT WITH FIX APPLIED | `replay_cell` accepted. Claude found `iter_promoted_cells` under-enumerated symbol-level winners; Codex fixed the enumerator and added regression tests. |

## Claude Challenges Preserved

1. Measurement does not yet prove profit improvement. Any net-profit claim must cite a baseline-versus-candidate table.
2. Objective scoring still consumes validation-surface metrics; validation floors, sparse-trade penalties, and false-discovery handling remain open strategy-selection work.
3. Schema-version metadata exists now, but consumers still need explicit guards so pre-MAE artifacts are treated as "not instrumented," not as zero excursion.
4. Bar-resolution MAE/MFE is accepted as the sanctioned aggregate approximation, but it is not tick-path MAE/MFE.

## Bounded A/B Specs From Claude

| ID | Hypothesis | Exact Change | Bounded Test | Accept If |
|---|---|---|---|---|
| CC-AB-1 | EMAPullbackContinuation may cap MFE at 1:1 R:R | Widen `tp_atr_mult` grid and test optional trailing | EURUSD H1/H4 EMAPullback, equal budget | Net profit and MFE retained improve without expectancy/trade-count collapse |
| CC-AB-2 | DayOfWeekBias winners may be validation-surface noise | Measurement first; re-score current winners on post-selection slice | Current DayOfWeekBias cells after a real winner book exists | Edge persistence proves or rejects weekday/mode choices |
| CC-AB-3 | `stability_mult` may demote rewarded aggression | Shadow-compute objective with and without stability multiplier | Representative winners | No-penalty ranking yields higher net-dollar winners with acceptable DD |
| CC-AB-4 | Current objective consumes biased Sharpe | Feed canonical per-trade or in-market Sharpe plus candidate-count deflation into shadow score | Bounded study, same budget | Ranking aligns better with net dollars than current composite |

## Decisive Correction From Claude

See `agent_audit/performance/CLAUDE_DECISIVE_REPLAY_FINDING.md`.

- The "157 tradeable cells / 45 symbols" figure is stale for the current live book. It describes the previous `agent_audit/data/ledger_tradeable_cells.csv` state before the 2026-06-18 `pm_configs.json` rewrite.
- Current direct ledger tally: 96 symbols `NO_TRADE`, 3 `BLOCKED`, 1 `PROMOTED`; regime leaves: 2387 `NO_TRADE`, 12 `BLOCKED`, 1 `PROMOTED`.
- The sole logical live-eligible winner is `FORD H4 CHOP DoubleEMATrendStrategy`, promoted from 16 validation trades with PF 1.77.
- The corrected $10k replay over 166 trades loses `-2317.84` (`-23.18%`), PF `0.66`, max DD `24.88%`, avg MAE `0.7394R`.
- Action: do not run live on the current ledger. If the ledger is mid-reoptimization, let it finish, then re-run corrected inventory, shadow audit, and all-promoted-winner replay.

## Current Partner Review State

- PERF-FIX-001/002/003: accepted and closed.
- PERF-FIX-004: accepted; consumer schema guard remains open.
- PERF-FIX-005: accepted and closed.
- PERF-FIX-006: accepted and closed.
- PERF-FIX-007: accepted after Codex's enumerator correction; partner re-check requested on the fixed enumerator evidence.
- P1/P2 strategy/objective mutations (CC-AB-1 through CC-AB-4): open, now demonstrably high-value because the current sole promoted winner is a sparse-validation selection artifact.
