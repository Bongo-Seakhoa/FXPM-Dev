# F005: Backtest/live execution-cost model misses entry and TP slippage attribution

## Category
Execution accounting / Backtest realism / Profit leakage

## Severity
Medium

## Confidence
High

## Evidence
- `pm_core.py:4773-4779` documents the backtester execution model and explicitly states that slippage is applied only on stop exits, not entries or take-profit exits.
- `pm_core.py:276-295` and `pm_core.py:5620-5640` confirm that stop-loss hits receive `slippage_price`, while take-profit hits fill exactly at `take_profit`; entries are precomputed from open plus spread and do not receive an execution slippage term.
- Live entry execution in `pm_main.py:5428-5440` submits a market order with `deviation=30` and an expected `price=entry_price`; `pm_main.py:5442-5476` records the broker result price and recomputes executed stop-loss risk, but no durable field stores expected-vs-filled slippage or aggregates it by symbol/timeframe/session.
- The cycle-drift field named `execution_slippage_mult` in `pm_main.py:5194-5204` measures quote movement between previous and refreshed entry estimates before submission, not the broker fill slippage after `send_market_order`.

## Explanation
The engine models spread and stop-gap slippage, which is materially better than a spread-free backtest. The missing piece is live parity: real market entries can fill worse than the expected quote, and exits can differ from the ideal TP/SL level during fast markets, broker requotes, gaps, or partial liquidity. The current backtest assumes entry fill equals modeled open-plus-spread and TP fill equals the target, while live logs know the result price but do not convert it into an expected-vs-actual slippage metric.

This is not a reason to reject the aggressive micro-account mode. It is a measurement flaw: if the edge is thin on some high-frequency or high-spread contexts, unmeasured entry/exit leakage can turn apparently rewarded aggression into unrewarded churn.

## Trading Impact
- Backtest profit factor, expectancy, Sharpe, and recovery can be overstated for cells whose edge depends on tight entries/exits.
- Live profit leakage cannot be separated from strategy decay because actual fill slippage is not stored as a first-class metric.
- High-drawdown trades cannot be fully diagnosed when the path includes adverse fill quality but the trade record only contains final result price.

## Suggested Fix or Experiment
1. Add `expected_entry_price`, `filled_entry_price`, `entry_slippage_pips`, and `entry_slippage_r` to actionable/live trade logs.
2. Add analogous exit slippage attribution where close/TP/SL deal data are available.
3. Add a backtest stress mode that applies observed p50/p90/p95 entry slippage by symbol/session, separately from stop-gap slippage.
4. Re-score the current promoted cells under observed slippage stress and flag cells whose PF or expectancy collapses.

## Estimated Effort
Medium

## Risk of Change
Low if implemented as measurement first; medium only if used as a promotion gate.

## Validation Plan
- Replay existing `last_actionable_log.json` / trade logs after adding expected-vs-fill fields and verify slippage aggregation by symbol.
- Run the existing benchmark harness with an additive entry-slippage stress model on a small representative set such as EURUSD, XAUUSD, US100, and DE30.

## Partner Review
**Agreed by Claude Code** (2026-06-17). Execution-cost sibling of F110 (backtest↔live sizing divergence) and
F107 (no MAE/MFE); strengthens the backtest-realism theme. Endorse measurement-first (log expected-vs-fill
entry/TP slippage), use as a promotion gate only after data accrues. Codex found and verified it during the
second-pass execution-accounting review.
