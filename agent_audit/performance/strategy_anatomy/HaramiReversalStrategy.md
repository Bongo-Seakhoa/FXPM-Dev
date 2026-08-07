# Strategy Profit Anatomy: HaramiReversalStrategy

## Intended edge
Strategy-specific technical edge selected by the promoted ledger; exact signal logic is in `pm_strategies.py`.

## Exact implementation
- Files: `pm_strategies.py` (class line 5008), `pm_core.py` backtest engine, `pm_pipeline.py` selection/export, `pm_main.py` live execution.
- Classes: `HaramiReversalStrategy` plus `BaseStrategy` for shared stop/intent behavior.
- Functions: `generate_signals`:5038, `get_default_params`:5019.
- Parameters: `{"atr_period": 14, "child_body_ratio": 0.5, "min_parent_atr": 0.4, "require_opposite": true, "sl_atr_mult": 2.0, "tp_atr_mult": 2.0, "trend_ema_period": 0}`.
- Indicators: inferred from `generate_signals` source and required feature columns; exact source anchor is `pm_strategies.py:5038`.
- Entry rules:
  - `def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:`
  - `signals = pd.Series(0, index=features.index, dtype=int)`
  - `signals[bullish] = 1`
  - `signals[bearish] = -1`
  - `return self._zero_warmup(signals, max(atr_period + 2, ema_period))`
- Exit rules: signal creates direction only; stop/target exits are executed by `pm_core._backtest_loop_numba` and `Backtester._run_python_loop` using SL-before-TP ordering.
- Stop logic: strategy stop/target pips come from `calculate_stops` or `BaseStrategy.build_trade_intent`; default ATR multipliers are propagated to precomputed SL/TP arrays before the Numba loop.
- Take-profit logic: TP price is precomputed from strategy TP pips and then checked in the backtest/live execution path.
- Trailing logic: no strategy-specific trailing was confirmed from this static anatomy; order governance overlays are separate and must be tested independently.
- Break-even logic: no strategy-specific break-even path was confirmed from this static anatomy; governance overlays may adjust stops outside signal generation.
- Session logic: present only if strategy source uses time/session columns; ledger-level session attribution is not present in existing `ledger_tradeable_cells.csv`.
- Regime logic: winner selection is regime-specific in `pm_configs.json`; the strategy class itself generally emits raw signals independent of the final promoted regime cell.

## Profit attribution
- Source: `agent_audit/data/ledger_tradeable_cells.csv`; cells reviewed: 1.
- Symbols: IBM.
- Timeframes: H4.
- Regimes: CHOP.
- Promotion statuses: `{"PROMOTED": 1}`.

| Symbol | TF | Regime | Status | Selection | Quality | Val PF | Val Return % | Val DD % | Val Trades |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| IBM | H4 | CHOP | PROMOTED | 73.16549327426316 | 0.7188672589476806 | 3.35 | 12.95 | 1.02 | 18.0 |

| Attribution dimension | Evidence status | Required next data |
|---|---|---|
| Long/short direction | Not present in ledger summary | Replay/trade records with direction and final R |
| Session | Not present in ledger summary | Add/session-enrich replay output |
| Holding duration | Not present in ledger summary | Trade records after replay |
| Exit reason | Not present in ledger summary | New MAE/MFE trade records include exit reason |
| Volatility level | Not present in ledger summary | Join trades to ATR/regime features |
| Spread level | Not present in ledger summary | Historical spread columns or live actionable logs |
| Parameter region | Partially present in winner artifacts, not in this CSV | Join to `pm_configs.json` winner params |
| Strategy combination | Portfolio-level interaction not present | Replay concurrent promoted book |

## Loss attribution
- Immediate adverse entries: now measurable through `mae_pips`, `mae_r`, and `time_to_mae_bars`; existing historical summaries cannot classify it.
- Repeated stop-out structures: requires replay with `exit_reason`, `mae_r`, and symbol/regime grouping.
- Poor payoff structures: compare `mfe_r` versus final realized R after replay.
- High-cost low-edge trades: requires spread/commission/swap/slippage fields joined to trades.
- Entries during unsuitable regimes: compare MAE/final R by promoted regime versus live/computed regime.
- Entries with late confirmation: use time-to-MAE/time-to-MFE and first-bar MAE from the new records.
- MFE given back: requires replay; the new fields make this testable but do not backfill old summaries.
- Excessive holding time: not available in current ledger summary; add holding bars/time in replay export.
- Repeated correlated exposure: not strategy-local; portfolio replay required.
- Unprofitable parameter regions: join winner parameter metadata and replay performance.

## Entry analysis
- Static code anchor: `pm_strategies.py:5038`.
- Backtester entry timing uses previous completed signal bar (`sig_arr[i - 1]`) and enters at the next bar open, which prevents same-bar signal/entry lookahead in the engine.
- Parameter exposure is controlled by `get_default_params`/search-space generation; any new entry parameter must propagate through Optuna, fallback random search, strategy construction, winner export, reload, and live execution.
- Redundant/delaying/low-quality entry conditions cannot be proven from the current summary CSV; use the bounded replay to compare first-bar MAE and final R by condition bucket.

## Exit analysis
- SL/TP exits are modeled in the trade-path implementation; Numba and Python paths now emit equivalent MAE/MFE fields.
- MFE retained/given back is now measurable after replay but not derivable from old ledger summaries.
- Stop-outs that later recover require post-exit feature/path analysis; current records do not include post-exit recovery.
- Slippage is currently strongest for entry attribution; exit/TP fill slippage remains a queued measurement extension.

## Strategy interaction analysis
- Risk and position sizing: handled by `pm_core.Backtester` in research and `pm_position.PositionCalculator`/`pm_main.LiveTrader` in live.
- Regime filters: regime-specific winners come from `pm_configs.json`; validation is a selection surface, not pristine post-selection OOS.
- Portfolio filters: live position/risk gates can block otherwise promoted cells; replay needs portfolio context to measure missed positive expectancy.
- Execution handling: live actionable logs now include entry slippage schema/version fields.
- Cost model: backtest spread/commission/swap/slippage assumptions must be matched in USD 10,000 A/B replay before claiming profit improvement.
- Symbol specifications: broker tick/pip/volume specs are part of `InstrumentSpec`/MT5 spec sync and must stay unchanged across baseline/candidate.
- Parameter-selection process: current winner metrics can be sparse; use trade count and post-selection decay when comparing candidates.

## Exact fix points
- Run a USD 10,000 promoted-cell replay and bucket by `mae_r`, `mfe_r`, final R, and exit reason.
- Stress entry slippage by symbol/timeframe/session before changing trade eligibility.
- Do not add a gate until the new measurement fields prove it removes unpaid drawdown rather than profitable pain.

## Optuna integration
- No new optimizable parameter was added by the current measurement fixes.
- If an entry/exit change is tested, add it to config schema, default params, Optuna suggestion, fallback random search, strategy construction, result export, winner reload, live execution, and search-space version.

## Numba integration
- Strategy signal generation remains Python/pandas; the trade path is Numba/Python dual-path.
- Any exit-path change must be implemented in `_backtest_loop_numba` and `_run_python_loop` with parity tests.
- Current MAE/MFE fields preserve legacy Numba return indices 0-15 and append new arrays at 16-24.

## Bounded validation
- Canonical baseline: USD 10,000, same data, same costs, same seed, same trial/replay budget.
- Minimum test: replay top promoted `HaramiReversalStrategy` cells, then compare net profit, PF, expectancy, drawdown, `mae_r`, `mfe_r`, time underwater, and trade count.
- Verdict standard: accept only if the candidate increases/protects net value or removes unpaid drawdown without destroying profitable flow.
