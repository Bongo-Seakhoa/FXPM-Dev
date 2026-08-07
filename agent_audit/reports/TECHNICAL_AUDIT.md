# Technical Audit - Corrective Deep Pass

Date: 2026-06-17  
Agent: Codex  
Partner agent: Claude Code  
Mode: bounded read-only audit, no live trades, no full optimization

## Scale-Aware Risk Framing

The live account is approximately USD 22. The audit treats the current aggressive profile as an intentional micro-account growth mode, not as an automatic defect. High percentage risk is acceptable only if it is implemented correctly, measured correctly, and compensated by expected upside. The audit still flags anything that distorts backtest/live results, hides ruin or margin risk, misreports drawdown/recovery, creates execution leakage, or prevents a clean transition toward larger-account controls.

The core technical conclusion is not "the bot is aggressive." The core conclusion is that several important code paths do not measure the same reality: winner selection is tested at USD 10k / 1% discovery sizing without live cap/block semantics, while current live execution is USD 22 / 0.5% target / 25% hard cap / min-lot dominated. That is the main internal-consistency problem.

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

- `pm_core.PipelineConfig`
- `pm_core.DataSplitter.get_workflow_indices`
- `pm_core.Backtester.run`
- `pm_core.Backtester._run_python_loop`
- `pm_core.Backtester._calculate_metrics`
- `pm_core.Backtester._empty_result`
- `pm_core._round_volume_numba`
- `pm_core.InstrumentSpec.__post_init__`
- `pm_core.InstrumentSpec.calculate_tick_profit`
- `pm_core.InstrumentSpec.calculate_loss_at_stop`
- `pm_core.InstrumentSpec.round_volume`
- `pm_pipeline.RegimeOptimizer.optimize_symbol`
- `pm_pipeline.RegimeOptimizer._compute_bucket_metrics`
- `pm_pipeline.RegimeOptimizer._build_purged_kfold_ranges`
- `pm_pipeline.RegimeOptimizer._aggregate_purged_kfold_metrics`
- `pm_pipeline.RegimeOptimizer._deflated_sharpe_ratio`
- `pm_pipeline.RegimeOptimizer._build_false_discovery_evidence`
- `pm_pipeline.ProductionPipeline.run_for_symbol`
- `pm_optuna.HyperparameterOptimizer.optimize`
- `pm_optuna.HyperparameterOptimizer._fallback_random_search`
- `pm_main.ProductionLiveTrader._execute_entry`
- `pm_main.ProductionLiveTrader._estimate_stop_loss_amount`
- `pm_main.ProductionLiveTrader._estimate_position_risk_pct`
- `pm_main.ProductionLiveTrader._check_symbol_combined_risk_cap`
- `pm_main.ProductionLiveTrader._estimate_open_drawdown_pct`
- `pm_main.ProductionLiveTrader._estimate_realized_loss_pct`
- `pm_position.PositionCalculator.calculate_position_size`
- `pm_mt5.MT5Connector.order_calc_profit`
- `pm_mt5.MT5Connector.calc_loss_amount`
- `pm_mt5.MT5Connector.order_calc_margin`
- `pm_mt5.MT5Connector.send_market_order`
- `pm_dashboard.analytics.compute_equity_curve`
- `pm_dashboard.analytics.compute_drawdown_curve`
- `pm_dashboard.analytics.compute_performance_metrics`
- `tools.benchmark.path_dependent_dd`
- `tools.benchmark.evaluate_winner`

### Historical Result Files and Logs Reviewed

- `pm_outputs/production_live/optimization_summary.csv`
- `pm_outputs/production_live/signal_ledger_202606.jsonl`
- `pm_outputs/production_live/oos_evidence/*.json` via `agent_audit/data/oos_manifest_summary.csv` and direct script inventory
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

### Derived Evidence Files

- `agent_audit/data/audit_evidence_summary.json`
- `agent_audit/data/ledger_tradeable_cells.csv`
- `agent_audit/data/ledger_symbol_summary.csv`
- `agent_audit/data/signal_risk_events.csv`
- `agent_audit/data/signal_risk_action_summary.csv`
- `agent_audit/data/signal_risk_symbol_action_summary.csv`
- `agent_audit/data/risk_cap_sensitivity.csv`
- `agent_audit/data/oos_manifest_summary.csv`
- `agent_audit/data/log_notable_events.csv`
- `agent_audit/data/deep_consistency_summary.json`
- `agent_audit/data/deep_split_geometry.csv`
- `agent_audit/data/deep_metric_definition_mismatches.csv`
- `agent_audit/data/deep_risk_symbol_actions.csv`

### Commands Run

Full command history is in `agent_audit/COMMAND_LOG.md`. The key validation commands were:

- `python agent_audit\scripts\collect_audit_evidence.py`
- `python agent_audit\scripts\deep_consistency_audit.py`
- JSON parsing checks for `config.json`, `pm_configs.json`, and `pm_outputs/production_live/signal_ledger_202606.jsonl`
- `python -m pytest tests\test_backtester.py ... tests\test_profitability_readiness_blueprint.py -q` -> 245 passed, 11 subtests passed
- `python -m pytest tests\test_data_splitter.py tests\test_position_sizing_edge_cases.py tests\test_dashboard_trade_enrichment.py -q` -> 35 passed
- `python -m pytest tests\test_optimizer_hardening.py -q` -> 75 passed, 2 Optuna experimental warnings

No full optimization sweep was run. No `pm_main.py --trade` command was run. No live trades were placed.

## Confirmed Findings vs Hypotheses

| Type | Finding | Evidence | Status |
|---|---|---|---|
| Confirmed | Live account switch pins expected account identity, not credentials | `config.json` MT5 expected login/server/currency; readiness checks in `pm_live_readiness.py`; F001 | Confirmed |
| Confirmed | Current promoted validation evidence is fragile | 157 tradeable cells, 75 with <20 validation trades, 147 with validation Sharpe >10, all `pbo_status=not_available`; F002/F111 | Confirmed |
| Confirmed | Bleeding-edge split geometry is implemented correctly | `DataSplitter.get_workflow_indices`; `tests/test_data_splitter.py`; `deep_split_geometry.csv` | Confirmed |
| Confirmed | 50k validation is used as a selection surface, not a pristine post-selection holdout | `pm_pipeline.py:6593-6602`, `pm_optuna.py:545-567`, `config.json:111`; F111 | Confirmed |
| Confirmed | Backtest/live sizing semantics diverge materially at USD 22 | `pm_core._round_volume_numba`, `InstrumentSpec.round_volume`, `pm_position.PositionCalculator`, `pm_main._execute_entry`; F110 | Confirmed |
| Confirmed | Dashboard and engine same-name metrics use different formulas | `pm_core._calculate_metrics`, `pm_dashboard.analytics.compute_performance_metrics`; F112 | Confirmed |
| Confirmed | No per-trade MAE/MFE exists | `rg mae/mfe/excursion` and trade record fields in `pm_core.py:5789-5800`; F107 | Confirmed |
| Confirmed | Execution slippage attribution is incomplete | `pm_core.py:4776`, `pm_core.py:276-295`, `pm_main.py:5428-5476`; F005 | Confirmed |
| Hypothesis | Some high-drawdown trades are profitable pain worth keeping | Cannot classify without MAE/MFE and per-trade profit contribution | Needs instrumentation |
| Hypothesis | Some symbols leak value via poor entry timing/session spread | Supported by slippage/spread modeling gap, but not proven per trade | Needs slippage/session attribution |
| Hypothesis | Account-size regimes improve scaling without killing edge | Supported by static config and micro-account block rates, but requires replay | Needs bounded replay |

## Deep Internal Consistency Review

| Area | Intended Behavior | Actual Code Behavior | Evidence | Consistent? | Impact | Required Action |
|---|---|---|---|---|---|---|
| Data loading | Load one recent base-M5 workflow and derive higher timeframes from the same boundary | `run_for_symbol` calls `_load_symbol_timeframe_data` and derives timeframes from the shared workflow | `pm_pipeline.py:6465-6468` | Mostly yes | Good calendar consistency across timeframes | Keep; add window fingerprint to every promoted cell |
| Data splitting | Use newest active/audit slice; latest half for optimization; 50k bars before cutoff for validation | Implemented as active 250k, optimization last 125k, WFV 50k before 300-bar embargo, audit oldest 50k | `pm_core.py:4677-4721`; `deep_split_geometry.csv` | Yes for geometry | No off-by-one found in split math | Preserve tests; add report badge that geometry is verified |
| Strategy design | Candidate strategy designs should not see future bars | Optuna objective trains on optimization train and evaluates validation separately | `pm_optuna.py:514-560` | Mostly yes | No direct forward leakage into trial training found | Keep; document validation as selection evidence |
| Optimization | Select winners close to recent market conditions | Train features are latest-half optimization window | `pm_pipeline.py:6560`, `train_features_by_tf` at `6574` | Yes | Matches bleeding-edge philosophy | Keep recency design |
| Validation | 50k pre-cutoff section should mitigate blind latest-half fitting | WFV is non-overlapping and embargoed, but it is also selection/validation evidence | `pm_pipeline.py:6593-6602`, `pm_optuna.py:545-567` | Partially | Stored validation metrics are selection-biased | Add post-selection audit or deflated validation gate |
| Backtesting | Backtest should represent the risk/execution regime being selected for | Discovery backtest uses USD 10k and 1% discovery risk; min lot clamps without live max-risk block | `config.json:20-22`; `pm_core.py:80-103`, `5704`; F110 | No for USD 22 live | Winner metrics do not model current live tradeability | Add min-lot/cap-aware replay mode |
| Demo/live execution | Live should fail closed on wrong account and log order results | Expected account pins exist; market order result and ambiguous retcodes handled | `config.json:318`; `pm_live_readiness.py`; `pm_main.py:5428-5564` | Mostly yes | Safer than a blind live switch | Keep; still require terminal account verification before trading |
| Risk sizing | Risk target, actual risk, min-lot clamp, and cap should be coherent | Live path computes target/actual risk and blocks above hard cap; backtest path always clamps to min | `pm_position.py:439-467`; `pm_main.py:4933-4987`; `pm_core.py:101-102` | No across modules | Current live behavior differs from selected backtest behavior | Centralize sizing semantics or add explicit backtest mode |
| Trade filtering | Low-quality or untradeable contexts should be excluded or derated | Live blocks 83 min-lot-cap events and one symbol-risk cap in risk events; cycle drift/supervisor also present in ledger evidence | `deep_consistency_summary.json`; `signal_ledger_202606.jsonl` | Partially | Some untradeable symbols remain in promoted universe | Add current-balance tradeability flags and symbol tiers |
| Metric calculation | Same metric names should mean same formula | Engine, dashboard, pipeline buckets, and tools differ on PF, Sharpe, expectancy, drawdown, recovery | `pm_core.py:5924-5999`; `pm_dashboard/analytics.py:626-665`; F112 | No | Operator/reporting decisions can compare unlike numbers | Create canonical metric contract |
| Reporting | Reports should reflect selection-engine reality and live reality | Existing reports mix validation metrics, dashboard metrics, OOS summaries, and live action logs | `agent_audit/data/*`; F112 | Partially | Can overstate confidence or misclassify risk | Label metric source and basis in every report |
| Logging | Live logs should allow root-cause analysis | Actionable logs capture action/risk, but no MAE/MFE and no expected-vs-fill slippage fields | `last_actionable_log.json`; `pm_main.py:5468-5476`; F005/F107 | Partially | Cannot fully explain high-drawdown trades | Add MAE/MFE and fill-slippage fields |
| Parameter selection | Final selected parameters should be defensible after multiple testing | DSR code exists, but current cells show `pbo_status=not_available`; validation surface drives selection | `pm_pipeline.py:3812-3892`; `ledger_tradeable_cells.csv` | Partially | Elevated false-discovery risk | Enforce trade-count/DSR/PBO or post-selection audit floors |
| Symbol/timeframe handling | Only trade symbols/timeframes that are feasible for the active account | 45 symbols and 157 tradeable cells promoted; live min-lot blocks cluster in XAUGBP, US100, DE30 | `deep_risk_symbol_actions.csv` | No at USD 22 | Actionable universe includes many currently impractical cells | Add equity-tiered universe gates |
| Balance/equity accounting | Backtest/live should distinguish balance and equity consistently | Backtest uses floating equity at bar close; live open DD uses balance-equity; dashboard uses closed-trade curve | `pm_core.py:5803-5816`; `pm_main.py:2824-2831`; `pm_dashboard/analytics.py:433-504` | Partially | Drawdown/recovery numbers can differ by context | Label balance vs equity drawdown separately |
| Drawdown accounting | Drawdown should capture account and per-trade pain | Account-level backtest DD is floating-aware; per-trade MAE/MFE absent; dashboard is realized-only | `pm_core.py:5803-5816`; F107/F112 | Partially | Cannot separate profitable pain from bad risk | Add MAE/MFE and intratrade path exports |
| Recovery accounting | Recovery should be measured after drawdown and across regimes | Dashboard/tools compute recovery factors differently; engine lacks canonical recovery factor | `pm_dashboard/analytics.py:660-661`; `tools/benchmark.py:61-87` | No | Recovery comparisons are inconsistent | Add canonical recovery factor and recovery duration definitions |

## Calculation Correctness Audit

| Calculation | File/Function | Formula Used | Expected Formula | Correct? | Impact If Wrong | Evidence | Recommendation |
|---|---|---|---|---|---|---|---|
| Net profit | `pm_core.Backtester._calculate_metrics` | `sum(pnl_dollars)` | Sum of closed trade net PnL after spread, commission, swap/slippage model | Mostly | Profitability overstated/understated if costs missing | `pm_core.py:5916`; swap applied separately | Keep, but label cost assumptions |
| Gross profit/loss | `pm_core.Backtester._calculate_metrics` | Sum positive PnL; absolute sum negative PnL | Same | Yes | Profit factor wrong if signs mishandled | `pm_core.py:5917-5918` | Keep |
| Win rate | `pm_core.Backtester._calculate_metrics` | Wins / total trades * 100; breakevens remain in denominator | Acceptable if convention documented | Yes | Inflated if breakevens excluded silently | `pm_core.py:5910-5921` | Document convention |
| Profit factor | Engine vs dashboard | Engine: `gross_profit/gross_loss`, `inf` if no losses and gross profit; dashboard: `0.0` if no losses | Engine formula is correct; dashboard should not report 0 for no-loss profitable sample | Engine yes, dashboard no | A no-loss system can look worst in dashboard | `pm_core.py:5924`; `pm_dashboard/analytics.py:626`; F112 | Fix dashboard sentinel to `inf`/`None` and label capped PF where used |
| Expectancy | Engine vs dashboard | Engine: expected pips per trade; dashboard: average dollars per trade | Store pips, dollars, and R expectancy under distinct names | Inconsistent | Cross-symbol expectancy comparison is misleading | `pm_core.py:5935-5939`; `pm_dashboard/analytics.py:663-665` | Add `expectancy_pips`, `expectancy_dollars`, `expectancy_r` |
| Sharpe | Engine vs dashboard | Engine: per-bar equity returns annualized by timeframe; dashboard: per-trade PnL * sqrt(252) | A single canonical basis or explicit separate labels | Partially | Extreme Sharpe can inflate confidence | `pm_core.py:5950-5981`; `pm_dashboard/analytics.py:641-644`; F106/F112 | Label `sharpe_equity_bar` vs `sharpe_trade`; require min trades |
| Sortino | Engine vs dashboard | Engine: per-bar downside returns; dashboard: per-trade downside PnL | Same basis issue as Sharpe | Partially | Downside risk can be misread | `pm_core.py:5983-5999`; `pm_dashboard/analytics.py:648-653` | Label and canonicalize |
| Recovery factor | Dashboard/tools | Dashboard: `total_pnl/max_dd_abs`; tools: `total_return_pct/max_dd_pct`; engine main metrics absent | Choose one canonical metric, usually net profit / max drawdown in same currency, plus return/DD ratio separately | No | Recovery comparisons are not portable | `pm_dashboard/analytics.py:660-661`; `tools/benchmark.py:61-87`; engine metrics omit | Add canonical `recovery_factor` to engine |
| Max drawdown | Engine backtester | Floating equity drawdown at bar close | Peak-to-trough equity drawdown; intrabar if available | Mostly | DD understated if intrabar lows matter | `pm_core.py:5803-5816` | Keep; add per-trade MAE and optional intrabar stress |
| Relative drawdown | Engine | `max_drawdown_pct` from equity curve | Percent drawdown from peak equity | Mostly | Reporting ambiguity with balance DD | `pm_core.py:5814-5816` | Rename/report `max_equity_drawdown_pct` |
| Equity drawdown | Live | `(balance - equity) / balance * 100` when equity below balance | Acceptable live open-DD proxy | Partially | Not same as peak-to-trough equity DD | `pm_main.py:2824-2831` | Store both open DD and peak equity DD |
| Balance drawdown | Live/dashboard | Realized loss percent over windows; dashboard closed-trade equity curve | Balance peak-to-trough should be explicit | Partial | Balance recovery can be misread | `pm_main.py:2833-2884`; `pm_dashboard/analytics.py:433-504` | Add balance curve and realized DD fields |
| Trade frequency | Engine/ledger | Raw trade count; OOS manifests contain total trades | Trades per scored bar/day and quality-adjusted trade rate | Partial | High-frequency low-edge churn can hide | `pm_core.py:5910`; `oos_manifest_summary.csv` | Add trades/year and edge-per-trade filters |
| Lot size | Backtester/live | Backtester floors then clamps to min; live can clamp then block if hard cap exceeded | Same semantics when testing live account | No for current live | Backtest can take trades live blocks | `pm_core.py:80-103`, `2131-2149`; `pm_main.py:4933-4987`; F110 | Add cap-aware replay/backtest mode |
| Pip value | `InstrumentSpec`, live MT5 | Static pip value with tick-value derivation; live prefers MT5 `order_calc_profit` for risk | MT5/account-currency tick/profit math, fallback visibly warned | Mostly | Non-FX or non-USD fallback can mis-size | `pm_core.py:1990-2043`; `pm_mt5.py:1633-1648` | Add broker-spec parity report |
| Tick value | `InstrumentSpec.calculate_tick_profit` | `(exit-entry)*direction/tick_size*tick_value*volume` | MT5 tick-value math in deposit currency | Yes if tick values current | Wrong tick specs distort all PnL/risk | `pm_core.py:2082-2112` | Keep syncing broker specs |
| Spread cost | Backtester | Half-spread entry/exit model; historical modes supported | Bid/ask-aware spread by symbol/session | Mostly | Thin edges overstate profit if spread stale | `pm_core.InstrumentSpec.get_entry_price/get_exit_price`; benchmark spread fields | Keep; validate provenance per symbol |
| Commission | Backtester | Deducts `commission_per_lot * volume` once per closed trade | Match broker round-trip or two one-way deals exactly | Partial | PnL and risk can be off by commission convention | `pm_core.py:2007`, exit code deducts once | Confirm broker commission convention for FBS real |
| Swap | Backtester | Swap model applied after run | Account-currency swap matching broker rollover rules | Mostly | Longer holds can misstate net PnL | `pm_core._apply_swap_model`; tests in readiness blueprint | Reconcile with live deal swap |
| Slippage | Backtester/live | Backtest slippage only on stop exits; live logs result price but no slippage metric | Entry, stop, TP, and market close slippage attribution | No | Profit leakage hidden; TP/entry optimism | `pm_core.py:4776`, `276-295`; `pm_main.py:5428-5476`; F005 | Add expected-vs-fill fields and observed slippage stress |
| Risk per trade | Config/live/backtest | Pipeline risk 1.5%, discovery risk 1.0%, live target 0.5%, hard cap 25% | Explicit named regimes and consistent replay basis | No | Results can be interpreted under wrong risk basis | `config.json:20-22`, `281-285`; F110/F113 | Add account-size regime config |
| Exposure | Live | Same-symbol cap plus institutional total-open-risk logic | Portfolio exposure by symbol/correlation/account regime | Partial | Correlated positions can stack risk | `config.json:149-161`; `pm_main._check_symbol_combined_risk_cap` | Add portfolio replay and correlation caps |
| Margin | Live/backtest | Live margin-level gates and `order_calc_margin`; backtest no margin/stopout simulation | Backtest/replay should simulate margin availability and stopout risk for micro-account mode | Partial | USD 22 path may be unvalidated under margin stress | `pm_mt5.py:1656-1666`; `pm_main` margin protection | Add margin-aware replay for USD 22/USD 1k/USD 10k |
| Currency conversion | MT5/account | Tick value and `order_calc_profit` return deposit currency; account currency pinned USD | Explicit currency conversion or validated broker tick values | Mostly for USD account, partial offline | Non-USD account or stale broker specs can mis-size | `pm_core.py:2015`; `pm_live_readiness.py:306-339`; `pm_mt5.py:1633-1648` | Keep USD pin; warn loudly on fallback |

## High-Drawdown Trade Root Cause Review

The current data is not sufficient to label individual high-drawdown trades with confidence because per-trade MAE/MFE is absent. This is itself a finding, not an excuse. Account drawdown is floating-aware in the backtester, but the trade record does not retain each trade's worst adverse excursion.

| Bucket | Description | Evidence Found | Profit Contribution | Drawdown Contribution | Keep / Modify / Remove / Needs Test |
|---|---|---|---|---|---|
| Profitable pain worth keeping | Trades that go deeply underwater but recover and produce the edge | Not directly measurable; no `mae_pips`, `mae_r`, `mfe_r` fields | Unknown | Unknown | Needs Test |
| Bad risk worth filtering | Trades with high adverse excursion and poor expectancy | Min-lot cap blocks show very high theoretical risk; no paired outcome data for blocked trades | Unknown | Live blocked-risk median 37.86%, max 362.06% in `deep_consistency_summary.json` | Needs Test |
| Oversized but valid signal | Valid signal where min-lot floor makes actual risk much larger than target | 67 executed live risk events; median actual risk 5.35%, p95 20.93%, max 24.67% | Unknown until closed-trade EV by risk bucket is computed | Potentially large, especially at USD 22 | Modify |
| Poor entry timing | Entries that immediately suffer adverse movement | No expected-vs-fill slippage or MAE fields | Unknown | Unknown | Needs Test |
| Poor exit management | Winners that give back MFE or losers held beyond useful signal | Exit reason exists; MFE not captured | Unknown | Unknown | Needs Test |
| Regime mismatch | Strategy traded in a market regime where its edge decays | Cycle drift and supervisor block actions exist in ledger/log evidence, but not paired to MAE/MFE | Unknown | Possible | Needs Test |
| Spread/slippage/session problem | Losses caused by high spread, fill slippage, or session liquidity | F005 confirms entry/TP slippage attribution gap; spread model exists | Unknown | Possible | Needs Test |
| Correlation/exposure stacking problem | Multiple positions share risk factor and deepen DD together | Same-symbol and total-risk gates exist live; backtest lacks portfolio exposure path | Unknown | Possible | Modify |
| Data or reporting artifact | Drawdown appears different because reports use different equity definitions | Dashboard uses realized-trade curve; engine uses floating bar-close equity | Can misstate | Can hide open pain | Modify |
| Insufficient evidence | Root cause cannot be classified honestly | No MAE/MFE, no fill slippage metric, no canonical recovery metric | Unknown | Unknown | Needs Test |

### Proposed Drawdown Controls

| Control | What It Does | Why It Might Help | What It Might Break | How To Test | Priority |
|---|---|---|---|---|---|
| Add MAE/MFE capture | Stores `mae_pips`, `mae_r`, `mfe_pips`, `mfe_r` per trade | Separates profitable pain from bad risk | Larger logs; minor runtime cost | Backtest EURUSD/US100 sample and bucket trades by MAE vs final PnL | P0 |
| Add expected-vs-fill slippage fields | Logs expected and filled price at entry/exit | Identifies execution leakage and session issues | Requires deal reconciliation | Compare live/actionable fills to quote at decision time | P0 |
| Add live-account replay mode | Replays promoted signals with equity, min-lot, hard cap, and blocked trades | Tests current USD 22 reality without trading | May reject cells that are good at USD 10k | Replay ledger at USD 22, USD 1k, USD 5k, USD 10k | P0 |
| Add equity-tier universe filter | Suppresses symbols whose min-lot risk is structurally above tier cap | Avoids untradeable high-notional churn | Could delay profitable high-beta symbols | Compare blocked-rate and opportunity cost by symbol | P1 |
| Add canonical recovery factor | Computes net profit / max equity DD plus recovery duration | Makes recovery comparable across reports | Requires migration of dashboards | Unit tests with known equity curves | P1 |
| Add validation trade-count floor | Requires enough validation trades before promotion/selection | Reduces noisy false positives | May reduce trade frequency | Re-score current 157 cells with >=20/30 trade floors | P1 |
| Add post-selection audit slice | Measures selected winner after selection without using it to choose | Quantifies decay and selection bias | Less fresh data available if used as hard gate | Run as non-gating report first | P1 |
| Add account-size regimes | Micro, intermediate, full safety, preservation, recovery modes | Preserves intentional USD 22 aggression while automating scaling | Needs careful transition testing | Replay signal ledger across tier balances | P1 |
| Add observed slippage stress | Re-scores cells under p50/p90/p95 live slippage | Finds thin-edge execution-sensitive cells | Could over-penalize if sparse data | Apply to EURUSD, XAUUSD, US100, DE30 sample | P2 |

## Partner-Agent Review Status

| Finding | Found By | Codex Review | Partner Review | Final Position |
|---|---|---|---|---|
| F001 | Codex | Confirmed | Agreed by Claude | Confirmed |
| F002 | Codex | Confirmed | Agreed by Claude | Confirmed |
| F003 | Codex | Confirmed | Agreed by Claude | Confirmed, but scale-aware |
| F004 | Codex | Confirmed | Agreed by Claude | Confirmed |
| F005 | Codex | Confirmed | Agreed by Claude | Confirmed; Claude agrees — execution-cost sibling of F110/F107 |
| F101-F109 | Claude Code | Agreed by Codex | Authored by Claude | Confirmed with scale-aware reframing |
| F110 | Claude Code | Agreed by Codex | Authored by Claude | Confirmed |
| F111 | Claude Code | Agreed by Codex | Authored by Claude | Confirmed |
| F112 | Claude Code | Agreed by Codex | Authored by Claude | Confirmed |
| F113 | Claude Code | Agreed by Codex | Authored by Claude | Confirmed |

## Prioritized Recommendations

1. Add a live-account replay mode that models equity, min lot, hard cap, blocked trades, spread, commission, swap, and margin. This is the highest-value correction because it answers whether USD 22 aggression is implemented profitably rather than merely aggressively.
2. Add per-trade MAE/MFE and expected-vs-fill slippage fields. Without these, high-drawdown root cause analysis remains mostly hypothetical.
3. Canonicalize metric definitions across engine, dashboard, pipeline bucket metrics, tools, and reports. Fix dashboard profit factor no-loss behavior immediately.
4. Keep the bleeding-edge split, but label the 50k window as selection validation, not pristine OOS. Add a post-selection audit measurement or selection-bias penalty.
5. Add explicit account-size regimes: micro-account growth, intermediate account, full safety, capital preservation, and drawdown recovery. Do not manually rely on remembering to retune at USD 1k/USD 10k.
6. Add symbol/timeframe tradeability flags by balance tier. At USD 22, many high-notional cells are not currently tradeable under the live cap even if they are valid at USD 10k.
7. Add broker-spec parity checks comparing offline tick/pip calculations against MT5 `order_calc_profit` and `order_calc_margin` in read-only mode.

## Assumptions

- `config.json` and `pm_configs.json` were already modified before Codex touched the audit workspace; they were not reverted.
- The USD 22 account profile is intentionally aggressive.
- Current task scope is audit documentation plus read-only evidence scripts, not production behavior changes.
- The MT5 terminal was not intentionally connected or used for live trading during this audit.
- Historical demo performance may be real, but it must not be assumed to transfer to the USD 22 live account until replayed with live sizing/cap semantics.

## Resolved Operator Decisions

The open audit questions were answered in `agent_audit/QUESTIONS.md`. Implementation should now assume:

- The USD 22 account remains in aggressive micro-account growth mode.
- The live hard cap stays at 25%.
- All 100 configured symbols remain eligible if they have valid trading parameters.
- FBS demo specs are treated as matching real live specs unless later read-only parity checks show drift.
- Use the latest completed demo/evaluation run before the current run as the main performance source for risk-bucket analysis.
- Use timeframe-aware validation trade-count floors rather than one universal number.
- Use a composite drawdown promotion control after MAE/MFE exists: MAE/R plus recovery/time-underwater plus expectancy/PF/R-multiple.
- Add post-selection audit as a hard gate only after replay proves that it improves actual trading outcomes.
