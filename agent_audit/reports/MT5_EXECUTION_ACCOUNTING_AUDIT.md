# MT5 Execution and Accounting Audit

## Scope

This was a static and local-test audit. No live broker connection was intentionally opened and no orders were placed, modified, or closed.

## Account Routing

The live account switch is implemented by non-secret identity pinning:

- `config.json` now expects `28829971 / FBS-Real / USD`.
- MT5 `login`, `password`, and `server` remain blank.
- `pm_mt5.py:376-399` logs in only when credentials are supplied.
- `pm_live_readiness.py:304-334` checks actual account identity against expected identity.

Verdict: safe fail-closed design, with one operational caveat. The terminal must already be logged into the FBS Real account before live trading. The config does not switch the MT5 terminal session by itself.

## Order Send Handling

Static review found no immediate order-send red flag:

- `pm_mt5.py:1160-1175` calls `mt5.order_send`, maps retcodes, and treats `10008`, `10009`, and `10010` as success.
- `pm_main.py:5429-5562` records failed retcodes into the action ledger and has ambiguous-result reconciliation paths.
- `tests/test_mt5_connector.py` covers order-send request construction, ambiguous `None` result handling, and volume normalization.

Limitations:

- Real broker requotes, partial fills, symbol-specific filling modes, and live slippage were not empirically tested.
- One ledger row shows `FAILED_10018`; this should remain in the operational monitoring queue.

## Position Sizing and Broker Constraints

The implementation correctly recomputes actual risk after volume normalization, but the configured hard cap is high:

- `pm_position.py:436-516` allows min-lot clamp if actual risk is within `max_risk_pct`.
- `pm_main.py:4933-4982` recomputes actual risk and blocks if it exceeds the hard cap.
- `config.json` sets the hard cap to 25%.

Accounting correctness is not the issue; the risk policy is.

## Metric and Cost Accounting

Verified from static review:

- Profit factor uses dollar PnL.
- Backtest equity/drawdown is floating-aware at bar close.
- Commission/spread/slippage and swap models exist in the backtest path.
- Calmar is a return-to-drawdown ratio, not annualized Calmar.
- Per-trade MAE/MFE is not captured.

Recommended accounting improvements:

1. Rename `calmar_ratio` or annualize it.
2. Add MAE/MFE fields to backtest and live trade records.
3. Add a small broker-spec parity report comparing expected tick/pip value to MT5 `order_calc_profit` in a read-only/dry mode.

## Validation

Focused tests passed: 245 tests and 11 subtests across the requested modules. JSON validation passed for the audited config and ledger artifacts.

