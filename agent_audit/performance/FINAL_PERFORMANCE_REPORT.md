# Final Performance Implementation Report

## Post-Optimization Update: 2026-06-19

The operator reported that optimization is complete. Codex reran the corrected current-book inventory, shadow-selection audit, and all-promoted replay.

The current `pm_configs.json` now contains 117 logical promoted cells, not the prior one-cell interim ledger. The all-promoted USD 10,000 replay completed 117 rows:

- 79 positive rows and 38 negative rows.
- 38 rows have PF below 1.0.
- Aggregate net replay sum: `4,529,727.86`.
- Aggregate net replay sum excluding `PALLADIUM`: `2,443,636.25`.
- Median PF: `1.10`.
- Worst confirmed rows include `ETHUSD M5 TREND DayOfWeekBiasStrategy` at `-94.86%`, `EURJPY M30 RANGE ZScoreVWAPReversionStrategy` at `-91.58%`, and `USDJPY M30 TREND KeltnerFadeStrategy` at `-67.84%`.
- `PALLADIUM H4 StochasticReversalStrategy` used a default instrument spec and must be replayed with broker specs before its large positive result is trusted.

Deployment verdict: the completed book is materially better than the one-winner interim state, but it is still not cleanly live-ready. Before live use, verify missing instrument specs, decide whether `PROMOTE_SMALL` cells are eligible for live execution, quarantine replay-negative cells with PF below 1.0/severe drawdown, and run live readiness/preflight on the completed book.

Detailed status: `agent_audit/performance/POST_OPTIMIZATION_STATUS.md`.

## 1. Executive Outcome

### What was implemented

- PERF-FIX-001: per-trade MAE/MFE instrumentation in both `pm_core._backtest_loop_numba` and `pm_core.Backtester._run_python_loop`.
- PERF-FIX-002: expected-versus-filled live entry slippage attribution in `pm_main.LiveTrader._execute_entry`.
- PERF-FIX-003: dashboard no-loss profit factor sentinel fixed in `pm_dashboard.analytics.compute_performance_metrics`.
- PERF-FIX-004: schema-version metadata added to new backtest trade records and live actionable records.
- PERF-FIX-005: shared metric contract added for profit factor, return/drawdown, Calmar basis, annualized Calmar companion, and benchmark recovery factor.
- PERF-FIX-006: live-readiness min-lot tradeability checks and `--preflight` mode added.
- PERF-FIX-007: bounded promoted-cell replay and shadow-selection audit tooling added.
- PERF-FIX-007 correction: Claude found the replay enumerator could skip symbol-level promoted winners; Codex fixed it and added regression tests.

### Superseded 2026-06-18 production-ledger conclusion

This section described the 2026-06-18 interim ledger before the operator reported optimization completion. It is preserved as audit history, not as the current book. The current book is summarized in the 2026-06-19 post-optimization update above.

At that earlier point, `pm_configs.json` was not the same ledger that produced the "157 tradeable cells" evidence. Claude's decisive review and Codex's direct tally agreed:

- Symbol-level statuses: 96 `NO_TRADE`, 3 `BLOCKED`, 1 `PROMOTED`.
- Regime-leaf statuses: 2387 `NO_TRADE`, 12 `BLOCKED`, 1 `PROMOTED`.
- Logical promoted live book: exactly one winner, `FORD H4 CHOP DoubleEMATrendStrategy`.
- Corrected $10k replay: 166 trades, net `-2317.84`, return `-23.18%`, max DD `24.88%`, PF `0.66`, avg MAE `0.7394R`.

Historical recommendation at that point: do not run live on that interim ledger. That condition has now been superseded by the completed 117-cell post-optimization replay.

### What improved

- The system can now distinguish high-MAE/high-recovery trades from high-MAE/bad-risk trades after replay.
- Live/actionable logs can now identify adverse entry slippage in pips and R.
- Profitable no-loss dashboard samples no longer display as PF `0.0`.
- New artifacts are versioned so old missing fields are not misread as zero MAE/MFE or zero slippage.
- Calmar-like reporting now states its basis instead of silently presenting cumulative return/DD as annualized Calmar.
- Live readiness can now warn when broker min-lot geometry exceeds target risk and block when it exceeds hard cap.
- Replay/shadow tooling now enumerates symbol-level and regime-leaf promoted winners consistently.

### What did not improve yet

- No strategy entry/exit behavior was changed.
- No optimizer objective was changed.
- No full optimization or balance sweep was run.
- No live trade was placed.
- Net profit was not improved by the implemented fixes; these are correctness, readiness, and evidence-generation changes that expose the current selection problem.

## 2. Canonical Baseline

| Field | Value |
|---|---|
| Starting balance | USD 10,000 |
| Balance sweep | Not run |
| Full optimizer | Not run |
| Live trade | Not placed |
| Current ledger inventory | `agent_audit/performance/data/current_ledger_inventory.json` |
| Current promoted cells | `agent_audit/performance/data/current_promoted_cells.csv` |
| Config hash | See `agent_audit/performance/baseline_10000_summary.json` |
| Costs | Current config: spread on, slippage on, commission on, `backtest_spread_mode=conservative_max` |
| Risk | Current config fingerprints recorded; no risk lowering was performed |
| Current live book | 1 logical promoted winner, not 157 |
| Historical anatomy source | `agent_audit/data/ledger_tradeable_cells.csv`, 157 rows from the previous ledger state |
| Optuna settings | No Optuna run; no search-space or objective change |
| Numba settings | Compiled path preserved; legacy return indices 0-15 retained; MAE/MFE arrays appended 16-24 |

Machine-readable baseline: `agent_audit/performance/baseline_10000_summary.json`.

## 3. Existing Recommendation Triage

Full triage is in `agent_audit/performance/RECOMMENDATION_TRIAGE.md`.

| Decision | Examples |
|---|---|
| ALREADY IMPLEMENTED | MAE/MFE capture; expected-vs-fill entry slippage; dashboard no-loss PF; canonical metric contract; live min-lot tradeability preflight; bounded replay/shadow tooling |
| KEEP NOW | Corrected current-ledger replay; validation trade-count floor study; objective de-biasing study |
| KEEP BUT REFRAME | High-risk/account-size guidance as performance/correctness evidence, not risk reduction for its own sake |
| DEFER | Account-size regimes, balance-tiered symbol suppression, broad capital-preservation mode |
| REJECT | Balance sweep, full optimization, live trading, production credential edits in this phase |
| NEEDS EVIDENCE | MAE/MFE gates, trailing exits, stability multiplier removal, strategy family TP/exit changes |

## 4. Strategy Profit Anatomy

Generated files live under `agent_audit/performance/strategy_anatomy/`.

Important caveat: the 42 strategy-family anatomy files are generated from `agent_audit/data/ledger_tradeable_cells.csv`, which is historical evidence from the previous ledger state. They remain useful for code/methodology review, but they are not the current production live book.

The current production live book is represented by:

- `agent_audit/performance/data/current_ledger_inventory.json`
- `agent_audit/performance/data/current_promoted_cells.csv`
- `agent_audit/performance/data/promoted_winner_replay_all_10000.csv`

Current promoted winner:

| Symbol | TF | Regime | Strategy | Promotion Source | Val Trades | Val PF | Replay Trades | Replay PF | Replay Return |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| FORD | H4 | CHOP | DoubleEMATrendStrategy | symbol_and_regime | 16 | 1.77 | 166 | 0.66 | -23.18% |

## 5. High-Drawdown Trade Findings

| Bucket | Current Evidence | Status |
|---|---|---|
| Profitable pain | Now measurable via `mae_r`, `mfe_r`, final R, and time underwater | Tooling implemented; broader clean-book replay needed |
| Bad risk | Now measurable via high `mae_r`, low `mfe_r`, poor final R | Current FORD replay is bad-risk evidence, not profitable pain |
| Poor entry timing | First-bar/time-to-MAE available in new records | Needs per-trade export review |
| Poor exit management | MFE retained/given back can now be measured | FORD avg MFE giveback is roughly `0.80R`; needs trade-level cause review |
| Execution problems | Entry slippage pips/R now logged for executed/reconciled live entries | Needs live fill sample; no live trades were placed |
| Regime mismatch | FORD is a trend strategy promoted in `CHOP` | Confirmed current mismatch; needs selection rule fix/test |
| Reporting artifact | Schema versions prevent old missing fields being treated as zero | Implemented |
| Selection artifact | FORD promoted on 16 validation trades at PF 1.77 but full replay PF is 0.66 | Confirmed by Claude and Codex |

## 6. Calculation And Metric Corrections

| Item | Old Formula/Behavior | New Formula/Behavior | Trading/Selection Impact |
|---|---|---|---|
| Dashboard profit factor | `gross_profit / gross_loss if gross_loss > 0 else 0.0` | `gross_profit / gross_loss if gross_loss > 0 else inf if gross_profit > 0 else 0.0` | Prevents profitable no-loss samples from being misreported as PF zero |
| Backtest trade record schema | No schema label for MAE/MFE additions | `trade_record_schema_version = 2026-06-18-mae-mfe-v1` | Prevents old missing fields from being interpreted as zero excursion |
| Live actionable schema | No schema label for slippage additions | `actionable_schema_version = 2026-06-18-entry-slippage-v1` | Prevents old missing fields from being interpreted as zero slippage |
| MAE/MFE | Not present | Per-trade pips, R, timing, underwater bars, stop/target distance | Enables paid/unpaid drawdown analysis |
| Entry slippage | Not persisted | Expected/fill price, adverse pips, adverse R, direction | Enables execution leakage analysis |
| Calmar basis | Same `calmar_ratio` key implied annualized Calmar in some reports | Legacy key kept; `return_drawdown_ratio`, `calmar_ratio_annualized`, `annualized_return_pct`, and `calmar_ratio_basis` now emitted | Prevents misreading cumulative return/DD as annualized Calmar |
| Benchmark recovery factor | Return/DD ratio under `recovery_factor` name | `recovery_factor = net_pnl / max_drawdown_abs`; return/DD emitted separately | Aligns benchmark with dashboard/accounting definition |

## 7. Implemented Changes

| Fix ID | Files | Mechanism | Optuna Impact | Numba Impact | Result | Verdict |
|---|---|---|---|---|---|---|
| PERF-FIX-001 | `pm_core.py`, `tests/test_backtester.py` | Add MAE/MFE and underwater timing to backtest trade records | No objective/search change | Kernel arrays appended; Python parity path added | Focused tests passed | Accepted by Claude |
| PERF-FIX-002 | `pm_main.py`, `tests/test_config_source_of_truth.py` | Add expected-vs-fill entry slippage fields | None | None | Live/actionable payload compatibility tested | Accepted by Claude |
| PERF-FIX-003 | `pm_dashboard/analytics.py`, `tests/test_dashboard_trade_enrichment.py` | Correct PF no-loss sentinel | None | None | Dashboard focused tests passed | Accepted by Claude |
| PERF-FIX-004 | `pm_core.py`, `pm_main.py`, tests | Add schema-version metadata | None | No kernel change | Focused tests passed | Accepted by Claude |
| PERF-FIX-005 | `pm_metrics.py`, `pm_core.py`, `pm_dashboard/analytics.py`, `pm_pipeline.py`, `tools/benchmark.py`, tests | Canonical metric helpers and explicit Calmar/recovery basis | No objective/search change | None | Focused metric/dashboard/backtester tests passed | Accepted by Claude |
| PERF-FIX-006 | `pm_live_readiness.py`, `pm_main.py`, tests | Min-lot tradeability readiness and `--preflight` | None | None | Focused readiness tests passed | Accepted by Claude |
| PERF-FIX-007 | `agent_audit/performance/scripts/*`, `tests/test_performance_replay_tools.py` | Bounded replay and shadow-selection tooling with fixed promoted-winner enumeration | Shadow evidence only | Existing backtester path | Current live book replayed as one losing winner | Accepted with enumerator fix applied |

## 8. Performance Comparisons

Full machine-readable table: `agent_audit/performance/PERFORMANCE_COMPARISONS.csv`.

This pass does not claim higher net profit from code changes. `PERFORMANCE_COMPARISONS.csv` includes FIX-005 through FIX-007. The decisive replay row shows the current logical live winner, `FORD H4 CHOP DoubleEMATrendStrategy`, losing `-2317.84` net PnL, `-23.18%` return, `24.88%` max DD, PF `0.66`, over 166 trades.

## 9. Optimization Quality

- Current objective and winner selection were inspected but not changed.
- Shadow-selection tooling was added and run over the current logical promoted book.
- No Optuna parameter, sampler, pruner, trial budget, study storage, fallback random-search path, or winner export schema was changed.
- The current FORD result is concrete evidence that the existing selection surface can promote a sparse validation winner that inverts on fuller replay.
- Next optimization work should enforce or shadow-test validation trade-count floors, candidate-count deflation, regime/strategy compatibility checks, and canonical metric variants before changing objective weights.

## 10. Numba Quality And Performance

| Area | Result |
|---|---|
| Python/Numba parity | Deterministic MAE/MFE fixture compares Python and Numba outputs |
| Compilation mode | `pm_core._backtest_loop_numba` remains nopython-compatible |
| Runtime | Unit-test scope only; no benchmark claim |
| Memory | Adds fixed per-trade arrays, not full path storage |
| Recompilation behavior | No new dynamic object types in kernel |
| Remaining bottlenecks | Not profiled in this pass; efficiency is P3 after profit/correctness |

## 11. Accepted Fixes

See `agent_audit/performance/ACCEPTED_FIXES.md`.

Accepted by Claude Code:

- PERF-FIX-001
- PERF-FIX-002
- PERF-FIX-003
- PERF-FIX-004
- PERF-FIX-005
- PERF-FIX-006

Accepted with correction:

- PERF-FIX-007: per-cell replay accepted; enumeration bug fixed by Codex; re-check requested on fixed current-ledger evidence.

## 12. Rejected Fixes

See `agent_audit/performance/REJECTED_FIXES.md`.

Rejected/deferred in this pass:

- balance sweep;
- full optimization;
- live trading;
- production credential/config edits;
- immediate MAE/MFE gating before replay evidence;
- risk reduction merely because the current posture is aggressive.

## 13. Next Highest-Value Experiments

| ID | Experiment | Why It Matters |
|---|---|---|
| PEXP-000 | Confirm whether current `pm_configs.json` is complete or mid-reoptimization | A one-winner losing book is not deployable; the next action depends on whether the ledger is final or interim |
| PEXP-001 | Re-run corrected inventory and all-promoted-winner replay after clean optimization | Prevents stale or sparse winners from reaching live mode |
| CC-AB-4 | Canonical Sharpe/expectancy plus candidate-count deflation in shadow objective | Current FORD result proves sparse validation optimism can invert |
| Q4-FLOOR | Strategy/timeframe-aware validation trade-count floors | FORD was promoted on only 16 validation trades |
| REGIME-COMPAT | Penalize or block structural strategy/regime mismatches | Current promoted trend strategy sits in `CHOP` |
| CC-AB-1 | EMAPullbackContinuation wider TP/trailing replay | Tests whether trend winners are capped and MFE is given back, after a real winner book exists |
| PEXP-003 | Entry and exit slippage by family/session/symbol | Detects execution leakage before scaling |

## 14. Partner Review

Claude Code reviewed the 2026-06-18 implementation. The current state is:

- PERF-FIX-001/002/003: accepted.
- PERF-FIX-004: accepted; consumer-side schema guard remains open.
- PERF-FIX-005: accepted.
- PERF-FIX-006: accepted.
- PERF-FIX-007: accepted after Codex fixed the symbol-level enumeration defect identified by Claude.
- Decisive current-ledger conclusion: Claude and Codex agree the current ledger is one promoted winner and the corrected $10k replay loses.

## 15. Exact Commands, Files, Configs, And Artifacts

### Source files changed

- `pm_core.py`
- `pm_metrics.py`
- `pm_main.py`
- `pm_live_readiness.py`
- `pm_pipeline.py`
- `pm_dashboard/analytics.py`
- `tools/benchmark.py`
- `tests/test_backtester.py`
- `tests/test_dashboard_trade_enrichment.py`
- `tests/test_config_source_of_truth.py`
- `tests/test_metric_contract.py`
- `tests/test_profitability_readiness_blueprint.py`
- `tests/test_performance_replay_tools.py`

### Audit artifacts changed

- `agent_audit/performance/*`
- `agent_audit/performance/scripts/*`
- `agent_audit/performance/data/*`
- `agent_audit/performance/strategy_anatomy/*`
- `agent_audit/scripts/render_audit_html.py`
- `agent_audit/STATUS.md`
- `agent_audit/TASKS.md`
- `agent_audit/COMMAND_LOG.md`
- `agent_audit/LOCKS.md`

### Validation commands

| Command | Result |
|---|---|
| `python -m py_compile pm_core.py pm_main.py tests/test_backtester.py tests/test_config_source_of_truth.py` | Passed |
| `python -m pytest tests/test_backtester.py tests/test_config_source_of_truth.py -q` | 37 passed |
| `python -m pytest tests/test_backtester.py tests/test_config_source_of_truth.py tests/test_dashboard_trade_enrichment.py -q` | 47 passed |
| `python -m pytest tests/test_live_loop_integration.py tests/test_position_sizing_edge_cases.py tests/test_mt5_connector.py -q` | 51 passed |
| `python -m pytest tests/test_scoring_audit.py tests/test_pipeline_artifacts.py tests/test_margin_protection.py -q` | 102 passed, 11 subtests passed |
| `python -m pytest tests/test_profitability_readiness_blueprint.py -q` | 47 passed |
| `python -m py_compile pm_metrics.py pm_core.py pm_dashboard/analytics.py pm_pipeline.py pm_live_readiness.py pm_main.py tools/benchmark.py agent_audit/performance/scripts/run_promoted_cell_replay.py agent_audit/performance/scripts/shadow_selection_audit.py` | Passed |
| `python -m pytest tests/test_metric_contract.py tests/test_dashboard_trade_enrichment.py tests/test_backtester.py -q` | 28 passed |
| `python -m pytest tests/test_profitability_readiness_blueprint.py tests/test_config_source_of_truth.py -q` | 72 passed |
| `python -m pytest tests/test_benchmark_gate.py tests/test_scoring_audit.py tests/test_pipeline_artifacts.py -q` | 70 passed |
| `python -m py_compile agent_audit/performance/scripts/run_promoted_cell_replay.py agent_audit/performance/scripts/shadow_selection_audit.py agent_audit/performance/scripts/claude_replay_all_promoted.py agent_audit/performance/scripts/build_performance_evidence.py tests/test_performance_replay_tools.py` | Passed |
| `python -m pytest tests/test_performance_replay_tools.py -q` | 2 passed |
| `python agent_audit/performance/scripts/run_promoted_cell_replay.py --max-cells 10 --list-only --out agent_audit/performance/data/promoted_cell_replay_list.json` | Listed 1 logical promoted cell |
| `python agent_audit/performance/scripts/run_promoted_cell_replay.py --max-cells 10 --out agent_audit/performance/data/promoted_cell_replay_10000.csv` | Replayed 1 logical promoted cell |
| `python agent_audit/performance/scripts/claude_replay_all_promoted.py` | Replayed all current logical promoted winners: 1 losing FORD cell |
| `python agent_audit/performance/scripts/shadow_selection_audit.py` | Wrote 1-row shadow audit and summary |
| `python agent_audit/performance/scripts/build_performance_evidence.py` | Regenerated manifest, current-ledger inventory, comparisons, and anatomy files |
| `python agent_audit/scripts/render_audit_html.py` | Regenerated HTML reports |

### Important artifacts

- `agent_audit/performance/data/current_ledger_inventory.json`
- `agent_audit/performance/data/current_promoted_cells.csv`
- `agent_audit/performance/data/promoted_cell_replay_10000.csv`
- `agent_audit/performance/data/promoted_winner_replay_all_10000.csv`
- `agent_audit/performance/data/shadow_selection_audit.csv`
- `agent_audit/performance/PERFORMANCE_COMPARISONS.csv`
- `agent_audit/performance/CLAUDE_DECISIVE_REPLAY_FINDING.md`
- `agent_audit/performance/PARTNER_REVIEW.md`
- `agent_audit/performance/FIX_REGISTER.md`
- `agent_audit/performance/evidence_manifest.json`
- `agent_audit/performance/baseline_10000_summary.json`
