# Strategic Audit - Corrective Deep Pass

Date: 2026-06-17  
Agent: Codex  
Partner agent: Claude Code  
Scope: profitability, drawdown/recovery, methodology, scaling, execution realism, and internal consistency

## Executive Verdict

The system is not disqualified because it is aggressive at approximately USD 22. That risk profile is intentional micro-account growth mode. The strategic issue is whether the aggression is measured, rewarded, repeatable, and capable of scaling toward a larger account.

The codebase contains several strong pieces: account identity pinning, explicit split geometry, broker-aware tick PnL math, live min-lot hard-cap blocking, margin protection, cycle-drift/supervisor overlays, OOS artifact export, and false-discovery scaffolding. The deeper audit also found no off-by-one bug in the main bleeding-edge split. The latest-half and 50k pre-cutoff validation windows are placed as intended.

The weaknesses are more subtle and more important than generic "high risk" warnings:

1. Winner selection is not tested under the same sizing regime as current live execution. Discovery uses USD 10k / 1% / no cap-block semantics; live uses approximately USD 22 / 0.5% target / 25% cap / min-lot floor / blocking.
2. The 50k validation window is geometrically correct, but it is also a model-selection surface. Its metrics are not clean post-selection evidence.
3. The report/dashboard/engine metric layer is internally inconsistent, especially PF no-loss handling, Sharpe basis, expectancy units, recovery factor, and realized-vs-floating drawdown.
4. High-drawdown trade root cause cannot be answered honestly yet because per-trade MAE/MFE and expected-vs-fill slippage are missing.
5. The "aggressive now, conservative later" scaling thesis is not implemented as explicit account-size regimes. It currently depends on manual retuning.

The strategic direction should be: keep the bleeding-edge philosophy and the micro-account growth intent, but add measurement and replay layers that prove when risk is paid for and when it is just noise, execution leakage, or untradeable min-lot exposure.

## Evidence Base

### Exact Files Inspected

- `config.json`
- `pm_configs.json`
- `pm_core.py`
- `pm_pipeline.py`
- `pm_optuna.py`
- `pm_main.py`
- `pm_position.py`
- `pm_mt5.py`
- `pm_live_readiness.py`
- `pm_dashboard/analytics.py`
- `tools/benchmark.py`
- `tests/test_data_splitter.py`
- `tests/test_optimizer_hardening.py`
- `tests/test_backtester.py`
- `tests/test_position_sizing_edge_cases.py`
- `tests/test_margin_protection.py`
- `tests/test_dashboard_trade_enrichment.py`
- `tests/test_profitability_readiness_blueprint.py`
- `agent_audit/findings/F001-F005`
- `agent_audit/findings/F101-F113`
- `agent_audit/agents/claude_code.md`
- `agent_audit/agents/Codex.md`

### Exact Functions and Classes Inspected

- `pm_core.DataSplitter.get_workflow_indices`
- `pm_core.Backtester.run`
- `pm_core.Backtester._run_python_loop`
- `pm_core.Backtester._calculate_metrics`
- `pm_core._round_volume_numba`
- `pm_core.InstrumentSpec.calculate_tick_profit`
- `pm_core.InstrumentSpec.calculate_loss_at_stop`
- `pm_core.InstrumentSpec.round_volume`
- `pm_pipeline.ProductionPipeline.run_for_symbol`
- `pm_pipeline.RegimeOptimizer.optimize_symbol`
- `pm_pipeline.RegimeOptimizer._compute_bucket_metrics`
- `pm_pipeline.RegimeOptimizer._aggregate_purged_kfold_metrics`
- `pm_pipeline.RegimeOptimizer._deflated_sharpe_ratio`
- `pm_pipeline.RegimeOptimizer._build_false_discovery_evidence`
- `pm_optuna.HyperparameterOptimizer.optimize`
- `pm_main.ProductionLiveTrader._execute_entry`
- `pm_main.ProductionLiveTrader._estimate_stop_loss_amount`
- `pm_main.ProductionLiveTrader._estimate_position_risk_pct`
- `pm_main.ProductionLiveTrader._estimate_open_drawdown_pct`
- `pm_main.ProductionLiveTrader._estimate_realized_loss_pct`
- `pm_position.PositionCalculator.calculate_position_size`
- `pm_dashboard.analytics.compute_equity_curve`
- `pm_dashboard.analytics.compute_drawdown_curve`
- `pm_dashboard.analytics.compute_performance_metrics`
- `tools.benchmark.path_dependent_dd`
- `tools.benchmark.evaluate_winner`

### Historical Result Files and Logs Reviewed

- `pm_outputs/production_live/optimization_summary.csv`
- `pm_outputs/production_live/signal_ledger_202606.jsonl`
- `pm_outputs/production_live/oos_evidence/*.json`
- `pm_outputs/production_live/storage_state.json`
- `last_trade_log.json`
- `last_actionable_log.json`
- `logs/production_live/cycle_drift_state.json`
- `logs/institutional_supervisor_state.json`
- `logs/production_live/pm_20260607.log`
- `logs/production_live/pm_20260608.log`
- `logs/production_live/pm_20260609.log`
- `logs/production_live/pm_20260610.log`
- `logs/production_live/pm_20260611.log`
- `logs/production_live/pm_20260614.log`
- `logs/production_live/pm_20260617.log`
- `logs/production_live/live_trade_20260608_*.out.log`
- `logs/production_live/live_trade_20260608_*.err.log`
- `logs/production_live/bleeding_edge_retrain_20260609_*.out.log`
- `logs/production_live/bleeding_edge_retrain_20260609_*.err.log`

### Commands Run

- `python agent_audit\scripts\collect_audit_evidence.py`
- `python agent_audit\scripts\deep_consistency_audit.py`
- targeted JSON validation for audited config/ledger artifacts
- targeted `rg` and PowerShell line reads across the code files above
- `python -m pytest tests\test_backtester.py ... tests\test_profitability_readiness_blueprint.py -q` -> 245 passed, 11 subtests passed
- `python -m pytest tests\test_data_splitter.py tests\test_position_sizing_edge_cases.py tests\test_dashboard_trade_enrichment.py -q` -> 35 passed
- `python -m pytest tests\test_optimizer_hardening.py -q` -> 75 passed, 2 Optuna experimental warnings

Full command log: `agent_audit/COMMAND_LOG.md`.

## Deep Internal Consistency Review

| Area | Intended Behavior | Actual Code Behavior | Evidence | Consistent? | Impact | Required Action |
|---|---|---|---|---|---|---|
| Data loading | One base-M5 workflow should define the calendar boundary for derived timeframes | Pipeline loads base workflow and derives higher timeframes | `pm_pipeline.py:6465-6468` | Mostly yes | Reduces timeframe boundary leakage | Keep and expose fingerprint per cell |
| Data splitting | Latest half optimization plus 50k validation before cutoff | Implemented: at 400k bars WFV `224700-274700`, embargo `274700-275000`, optimization `275000-400000` | `deep_split_geometry.csv` | Yes | Methodology geometry matches theory | Preserve tests |
| Strategy design | Strategy candidates train on recent train segment only | Optuna trains on `train_features`; validation evaluated separately | `pm_optuna.py:514-560` | Mostly yes | No direct future leak found in objective training | Keep |
| Optimization | Optimize for fresh market behavior | Uses latest-half train features | `pm_pipeline.py:6560`, `6574` | Yes | Matches bleeding-edge philosophy | Keep |
| Validation | 50k validation should check reliability without being training data | It is non-overlapping but used to select/rank winners | `pm_pipeline.py:6593-6602`; `pm_optuna.py:545-567` | Partially | Stored validation stats are selection-biased | Add post-selection audit or penalty |
| Backtesting | Selected performance should reflect live account mode | Backtest uses USD 10k/1% discovery risk/no live block semantics | `config.json:20-22`; `pm_core.py:80-103`; F110 | No | Current live tradeability and drawdown path are not represented | Add live-account replay |
| Demo/live execution | Fail closed on wrong account; safe order result handling | Account pins and ambiguous-order reconciliation exist | `pm_live_readiness.py`; `pm_main.py:5428-5564` | Mostly yes | Live switch is safer than credential swap alone | Keep and verify terminal account |
| Risk sizing | Position size should mean the same across discovery, replay, and live | Discovery risk, pipeline risk, and live risk are separate; backtest clamps min lot, live can block | `config.json:20-22`, `281-285`; F110/F113 | No | Results can be interpreted under wrong risk basis | Formalize regimes |
| Trade filtering | Filter trades that are untradeable or low edge at current balance | Live blocks many min-lot cap breaches; promoted universe still includes them | `deep_consistency_summary.json` | Partially | Noise and missed opportunity-cost analysis | Add tradeability flags |
| Metric calculation | Same metric names should share definitions | Engine/dashboard/tools differ on PF, Sharpe, expectancy, recovery | F112; `deep_metric_definition_mismatches.csv` | No | Operator can compare incompatible numbers | Canonical metric contract |
| Reporting | Reports should state whether metrics are train, validation, OOS, live, or dashboard | Previous reports mixed surfaces; revised reports separate them | Audit workspace | Partially | Prior confidence was too generic | Keep source labels |
| Logging | Logs should support root-cause diagnosis | Action/risk logs exist; MAE/MFE and fill slippage absent | F005/F107 | Partially | Cannot classify high-drawdown trades | Add fields |
| Parameter selection | Multiple testing and selection bias should be controlled | DSR scaffolding exists; PBO unavailable for current cells; validation selection drives winners | `ledger_tradeable_cells.csv`; F002/F111 | Partially | False confidence risk | Enforce floors and audit slices |
| Symbol/timeframe handling | Symbol set should be coherent with active balance tier | 45 symbols/157 cells; high-notional symbols dominate min-lot blocks | `deep_risk_symbol_actions.csv` | No at USD 22 | Current account cannot act on full book evenly | Add equity-tiered universe |
| Balance/equity accounting | Balance and equity drawdown should be distinct | Backtest floating equity, live open DD, dashboard closed curve all coexist | `pm_core.py:5803-5816`; `pm_main.py:2824-2831`; `pm_dashboard/analytics.py:433-504` | Partially | Drawdown/recovery narratives can conflict | Report all three explicitly |
| Drawdown accounting | High-drawdown trades should be classifiable by root cause | Account DD exists; per-trade MAE/MFE absent | F107 | No | Profitable pain vs bad risk cannot be proven | Instrument MAE/MFE |
| Recovery accounting | Recovery should measure path after drawdown consistently | Recovery factor differs between dashboard/tools and missing in engine | `pm_dashboard/analytics.py:660-661`; `tools/benchmark.py:61-87` | No | Recovery-factor comparisons unreliable | Add canonical engine recovery |

## Bleeding Edge Optimization Methodology Review

### 1. My Understanding of the Intended Theory

The intended theory is coherent in outline:

- Use the full available dataset to define the symbol/strategy universe and the broad strategy context.
- Focus optimization on the most recent half of the active data because live edge decays and recent market behavior matters more than old regimes.
- Use approximately 50,000 bars before the latest-half cutoff as a reliability check so the optimizer is not blindly fitting only the newest data.
- Keep the production book fresh with retraining rather than pretending old long-history validation is always relevant.
- Accept that this is adaptive and aggressive, but require enough evidence that recent edge is not just noise.

This is a legitimate philosophy for a fast-adapting trading system. The question is whether the implementation preserves the distinction between recency, validation, selection, and proof.

### 2. Actual Implementation in the Code

- `pm_core.DataSplitter.get_workflow_indices` implements `production_split_mode="bleeding_edge"`.
- Current config uses `max_bars=300000`, `active_universe_bars=250000`, `historical_stress_audit_bars=50000`, `active_stage2_pct=50`, `walk_forward_validation_bars=50000`, `oos_embargo_bars=300`, and `clean_holdout_pct=0`.
- `pm_pipeline.ProductionPipeline.run_for_symbol` slices `active_features` from the optimization train window and `stage2_features` from WFV.
- `pm_optuna.HyperparameterOptimizer.optimize` evaluates train metrics on train features and validation metrics on validation features, then scores via `scoring_fn(train_metrics, val_metrics)`.
- `threadspark_selection_surface="validation"` causes the validation surface to feed final winner selection.

### 3. Data Window Verification

Verified with code and `agent_audit/data/deep_split_geometry.csv`.

For 400,000 bars:

| Window | Start | End | Rows |
|---|---:|---:|---:|
| workflow_window | 100000 | 400000 | 300000 |
| historical_stress_audit | 100000 | 150000 | 50000 |
| active_universe | 150000 | 400000 | 250000 |
| walk_forward_validation | 224700 | 274700 | 50000 |
| embargo gap | 274700 | 275000 | 300 |
| optimization_train | 275000 | 400000 | 125000 |
| clean_holdout | 400000 | 400000 | 0 |

The latest-half selection is correct: 50% of 250k active bars = 125k optimization train bars. The 50k validation window is before the optimization cutoff and separated by the configured 300-bar embargo. No off-by-one or forward overlap was found in the splitter tests.

### 4. Leakage and Overfitting Risks

There is no evidence that Optuna trains directly on future validation bars. The leakage risk is subtler: validation is used for selection. Once the final winner is chosen using the validation surface, the stored validation metrics are no longer an unbiased out-of-sample estimate. They are the best-looking validation result after a search process.

This matters because current evidence shows:

- 157 tradeable cells.
- 75/157 have fewer than 20 validation trades.
- 147/157 have validation Sharpe above 10.
- all 157 show `pbo_status=not_available`.
- OOS manifests exist for 93 symbols, but current promoted cells do not have active PBO status in the ledger evidence.

That pattern is not proof that the system is bad. It is proof that the reported validation evidence is too optimistic to stand alone.

### 5. Statistical Strengths

- Recency focus is appropriate if the strategy edge decays over time.
- The split geometry is explicit and tested.
- The validation window is before, not after, the optimized latest-half segment, matching the user's intended theory.
- The 300-bar embargo reduces immediate boundary contamination.
- False-discovery and DSR scaffolding exist.
- OOS evidence export exists across a broad candidate surface.
- Historical stress audit scaffolding exists.

### 6. Statistical Weaknesses

- The validation window is a selection surface, not pristine OOS.
- Validation samples are often small, so high Sharpe/PF can be noisy.
- `clean_holdout_pct=0.0` leaves no untouched post-selection measurement in production split mode.
- PBO status is unavailable for the current tradeable cells.
- The USD 10k discovery basis does not model the current USD 22 live path.
- Sharpe/Sortino are vulnerable to sparse-trade annualization artifacts.
- The method does not yet quantify parameter decay directly; it assumes decay and retrains.
- The system does not yet test whether high-drawdown trades are compensated by future return because MAE/MFE is missing.

### 7. Better Version of This Methodology

Keep the bleeding-edge philosophy, but make the evidence hierarchy explicit:

1. Use latest-half optimization for candidate adaptation.
2. Use the 50k pre-cutoff validation window as a selection/ranking surface, clearly labeled as such.
3. Add a post-selection audit that is never used to choose the winner. It can be small and non-gating at first; its purpose is to measure decay.
4. Add live-account replay at multiple balances: USD 22, USD 200, USD 1k, USD 5k, USD 10k.
5. Add selection-bias penalties: validation trade-count floor, deflated Sharpe with candidate count, and PBO or purged folds where feasible.
6. Add MAE/MFE and slippage attribution so drawdown and execution cost can be evaluated as paid or unpaid risk.
7. Track live-vs-validation drift by symbol/timeframe/regime, not only aggregate account performance.

This keeps freshness while reducing false confidence.

### 8. Code-Level Changes Needed

- Add a `live_account_replay` or `account_regime_replay` mode around `Backtester` or a companion replay engine that applies `risk_per_trade_pct`, `max_risk_pct`, min-lot block rules, margin checks, and blocked-trade records.
- Add `mae_pips`, `mfe_pips`, `mae_r`, and `mfe_r` to backtest trade records in `pm_core.Backtester._run_python_loop` and the Numba result conversion path.
- Add `expected_entry_price`, `filled_entry_price`, `entry_slippage_pips`, and `entry_slippage_r` to live/actionable logs in `pm_main._execute_entry`.
- Add canonical metric functions shared by `pm_core`, `pm_dashboard/analytics.py`, `pm_pipeline.py`, and `tools/benchmark.py`.
- Fix dashboard profit factor for no-loss samples.
- Rename/label validation metrics as `selection_validation_*` when `threadspark_selection_surface="validation"`.
- Add `post_selection_audit_metrics` beside `val_metrics` and `holdout_metrics`.
- Add explicit account-size regimes to config and startup logging.

### 9. Validation Plan

1. Recompute current promoted cells under USD 22 replay with min-lot/cap/block semantics. Compare trade count, blocked count, PF, return/DD, Sharpe, Sortino, recovery factor, and max DD against USD 10k discovery results.
2. Repeat replay at USD 200, USD 1k, USD 5k, and USD 10k to identify transition points where the current static risk profile stops being appropriate.
3. Add MAE/MFE instrumentation and bucket trades into profitable pain, bad risk, poor entry timing, poor exit management, and regime mismatch.
4. Apply validation trade-count floors such as 20 and 30 to current 157 cells and measure retained trade flow.
5. Add a non-gating post-selection audit slice for one bounded symbol set first, then compare live/demo decay against it.
6. Stress current winners with observed entry slippage once fill attribution exists.
7. Only after the replay/audit layer agrees with live/demo behavior should these gates be allowed to change production selection.

## Account-Size Regime Assessment

The code has static risk settings:

- Pipeline `initial_capital=10000`
- Pipeline `risk_per_trade_pct=1.5`
- Discovery `discovery_risk_per_trade_pct=1.0`
- Live position `risk_per_trade_pct=0.5`
- Live hard cap `max_risk_pct=25.0`
- Total open risk cap `75.0`
- Daily/session/weekly/open-DD hard blocks `50/40/60/50`
- `live_min_account_equity=20`

This is not an explicit regime system. At USD 22 the aggression is intentional. At USD 1k or USD 10k, the same caps are no longer obviously intentional. The code should support:

| Regime | Example Trigger | Purpose | Risk Posture |
|---|---|---|---|
| Micro-account growth mode | Equity < USD 200 | Accept high percentage risk for account growth | Aggressive, but measured |
| Intermediate account mode | USD 200-2k | Reduce min-lot distortion and widen symbol access carefully | Moderate |
| Full safety mode | USD 2k-10k+ | Use conventional caps and portfolio controls | Conservative relative to micro mode |
| Capital-preservation mode | New equity high or larger capital | Protect accumulated gains | Lower risk and tighter DD |
| Drawdown recovery mode | Equity below peak by threshold | Preserve optionality and avoid revenge-churn | Lower frequency, tighter filters |

The strategic value is not simply lowering risk. It is making the growth thesis executable without relying on a manual retune at the exact moment the account scales.

## Value Leakage Map

| Leakage Source | Evidence | Profitability Impact | Fix |
|---|---|---|---|
| Selection metrics not modeled at live balance | F110; `deep_consistency_summary.json` | Winners may be valid at USD 10k but untradeable/noisy at USD 22 | Live-account replay |
| Validation is selection-biased | F111; 147/157 Sharpe >10; PBO unavailable | Overstates edge and durability | Post-selection audit/penalty |
| Entry/TP slippage not attributed | F005 | Thin-edge cells may leak profit live | Fill-slippage logs and stress |
| Metric definition mismatch | F112 | Bad operational decisions from inconsistent reports | Canonical metrics |
| No MAE/MFE | F107 | Cannot remove bad drawdown without killing good pain | Instrument excursions |
| Static risk profile | F113 | Micro-account settings can persist into larger capital | Account-size regimes |
| High-notional symbol tradeability | F003/F103/F110; XAUGBP/US100/DE30 blocks | Opportunity cost and noisy action logs | Balance-tier symbol filters |

## Prioritized Recommendations

| Priority | Recommendation | Why It Comes First | Evidence |
|---|---|---|---|
| P0 | Build live-account replay at USD 22/USD 200/USD 1k/USD 5k/USD 10k | Directly tests whether aggressive scaling is rewarded under actual live constraints | F110, F003 |
| P0 | Add MAE/MFE and expected-vs-fill slippage instrumentation | Required to answer high-drawdown root cause and execution leakage | F005, F107 |
| P1 | Canonicalize metrics across engine/dashboard/pipeline/tools | Current reports can contradict selection engine | F112 |
| P1 | Add post-selection audit or selection-bias penalty | Keeps bleeding edge while reducing false confidence | F002, F111 |
| P1 | Add explicit account-size regimes | Makes the USD 22-to-USD 10k scaling plan real in code | F113 |
| P2 | Add equity-tiered symbol/timeframe filters | Removes currently untradeable cells without rejecting the full strategy | F003, F103 |
| P2 | Add broker-spec parity reports for tick/pip/margin | Protects PnL/risk math across CFDs, metals, indices, crypto | Technical audit |

## Collaboration Status

| Finding | Found By | Partner Review | Final Position |
|---|---|---|---|
| F001-F004 | Codex | Claude Code agreed | Confirmed |
| F005 | Codex | Claude Code agreed | Confirmed; execution-cost sibling of F110/F107 |
| F101-F109 | Claude Code | Codex agreed | Confirmed with scale-aware reframing |
| F110 | Claude Code | Codex agreed | Confirmed |
| F111 | Claude Code | Codex agreed | Confirmed |
| F112 | Claude Code | Codex agreed | Confirmed |
| F113 | Claude Code | Codex agreed | Confirmed |

The strongest partner finding was F111: the windowing math is correct, but validation is a selection surface. Codex agrees and treats that as the central methodology correction. The strongest Codex extension is F005, and Claude Code agreed with it: execution slippage attribution is not yet strong enough to separate strategy decay from fill-quality leakage.

## Assumptions

- The approximately USD 22 account risk profile is intentional.
- `config.json` and `pm_configs.json` dirty state predated this audit and was not reverted.
- The current work product is audit documentation and read-only evidence scripts, not production behavior changes.
- Historical demo success is relevant evidence, but not enough to prove USD 22 live behavior unless replayed with live sizing/cap semantics.
- No live trades were placed during the audit.

## Resolved Operator Decisions

The open strategy questions were answered in `agent_audit/QUESTIONS.md`. The strategic defaults are now:

- Keep the USD 22 account in aggressive micro-account growth mode.
- Keep most of the account working; do not implement account-size regimes as default trade suppressors.
- Keep the 25% live hard cap in micro-account mode.
- Keep all 100 valid-parameter symbols eligible, including high-notional symbols that may frequently block on min-lot/cap.
- Treat FBS demo specs as matching real live specs unless future read-only parity checks show drift.
- Use the latest completed demo/evaluation run before the current run as the primary historical performance source for risk-bucket analysis.
- Use timeframe-aware validation trade-count floors; D1 cells can have lower trade counts if supported by stronger stability/R evidence.
- Use a composite high-drawdown promotion control once MAE/MFE exists, so profitable pain is not removed just because it is uncomfortable.
