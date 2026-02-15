# Current Signal Flow and Multi-Account Expansion

## Scope
This describes how the current live system behaves at runtime, including gates that suppress trades.
It is behavior-focused and intentionally avoids deep code-level references.

## Current Signal Flow (End to End)

### 1. Startup and Runtime State
1. Runtime config is loaded from `config.json`.
2. The symbol universe is taken from config (or CLI override).
3. MT5 connection is created.
4. Existing optimized configs are loaded from `pm_configs.json`.
5. Live trading runs only if there is at least one validated config.

Important behavior:
- Live processing iterates over validated configs, not the full symbol list.
- If a symbol is in `config.json` but missing a validated config entry, it is not processed for signals.

### 2. Symbol Eligibility Before Signal Evaluation
For each validated symbol:
1. Resolve broker symbol mapping (handles suffixes/prefix variants).
2. Pull currently open positions for that broker symbol.
3. Apply position-count and timeframe constraints:
   - 0 positions: new entry allowed.
   - 1 position:
     - If D1+lower mode is enabled and the existing position is identified as D1, allow one non-D1 secondary.
     - If D1+lower mode is enabled and existing position is non-D1, allow one D1 secondary.
     - If timeframe cannot be inferred for that open position, secondary trade is blocked.
     - If D1+lower mode is disabled, second trade is blocked.
   - 2+ positions: blocked (hard cap).

Position timeframe inference priority:
1. Parse trade comment metadata.
2. If comment parsing fails, try matching magic number against known configs.
3. If still unknown, treat as unknown timeframe.

### 3. Candidate Generation (Winners-Only Policy)
If symbol passes position gate:
1. Evaluate each available timeframe from the symbol's regime config.
2. Fetch recent bars for that timeframe from MT5.
3. Skip timeframe if bars are below live minimum.
4. Compute features and current regime (using the most recent closed bar).
5. For that exact `(timeframe, regime)`:
   - If no validated winner exists: skip timeframe.
   - If winner exists but fails live quality gate (PF/return/DD thresholds): skip timeframe.
   - Else generate signal from that winner strategy.
6. Build candidate score:
   - `selection_score = regime_strength * quality_score * freshness`.
   - Freshness is `1.0` on new bar, otherwise decayed constant.

Key consequence:
- There is no live fallback ladder for missing winners. If no exact winner for current regime/timeframe, that timeframe does not trade.

### 4. Actionable Selection Policy
After candidate list is built:
1. Find best overall candidate by `selection_score` (even if signal is flat).
2. Filter actionable candidates where signal is BUY/SELL (non-zero).
3. Apply margin rule:
   - Candidate is eligible only if `selection_score >= best_overall_score * actionable_score_margin`.
4. Pick best eligible actionable candidate.

If no actionable candidate:
- Decision recorded as no-actionable winner signal.

If actionable exists but below margin:
- Decision recorded as no actionable within margin.

### 5. Decision Throttle and Re-Entry Suppression
The system uses a per-symbol decision identity including symbol, strategy, timeframe, regime, direction, and bar time.

Behavior:
- Same decision on same bar is suppressed.
- New bar or changed decision identity allows a new attempt.
- State persists to disk, so restart does not immediately replay the same decision.

### 6. Execution Pre-Checks
Before order send:
1. Per-symbol order rate limit check.
2. Duplicate-position check for same symbol+magic (race/duplication protection).
3. Get symbol info, account info, and live tick.
4. Compute stop prices and auto-widen stop if broker min stop distance requires it.

### 7. Risk, Sizing, and Hard Blocks
Risk pipeline:
1. Start from base risk and tier policy.
2. Apply secondary-trade adjustments if this is D1+lower secondary.
3. Enforce per-symbol combined risk cap for secondary.
4. Block if computed risk falls below minimum trade risk.
5. Compute volume from stop-distance loss model.
6. Clamp to broker/config min/max volume.
7. Recompute actual risk after rounding.
8. Hard block if actual risk exceeds max risk cap.

If blocked, decision outcome is persisted with reason.

### 8. Order Dispatch and Post-Outcome
1. Build order with side, volume, SL, TP, magic, and metadata comment.
2. Send order to MT5.
3. Persist outcome:
   - Executed
   - Paper-mode simulated
   - Failed (retcode)
   - Risk/position/rate-limit skip
4. Update action logs used by dashboard.

### 9. What Is Persisted
- `pm_configs.json`: optimized/validated symbol configs.
- `last_trade_log.json`: latest decision state per symbol (including no-trade reasons).
- `last_actionable_log.json`: actionable outcomes for dashboard (not overwritten by no-action outcomes).
- `logs/*.log`: runtime logs (debug-level detail depends on configured log level).

## Edge Cases and Non-Obvious Behavior

1. Expiry vs validation:
- Live symbol filtering uses validated flag.
- Config expiry is not part of live filter.
- Expired validated configs can continue trading unless retraining refreshes them.

2. Unknown timeframe on existing position:
- If one position exists and timeframe cannot be inferred, secondary trade is blocked.
- This can silently reduce trade frequency on affected symbols.

3. Strict winners-only:
- Missing exact winner for current regime/timeframe means no candidate for that timeframe.
- This is a major source of "few signals."

4. Flat top candidate:
- Highest score candidate can legally be flat.
- Actionable margin helps pick near-top actionable alternatives, but only if close enough.

5. No close-on-opposite enforcement in live loop:
- Positions are primarily exited by SL/TP/manual broker-side closure behavior in current runtime path.
- Opposite-signal close option is present as a setting but not actively driving main live exits.

6. Secondary-risk behavior:
- Secondary can be allowed but then blocked by combined cap or max-risk cap.
- For high-volatility/high-value instruments, minimum lot can force risk too high, causing skips.

7. Log visibility:
- Many gating reasons are debug-level. At info-level logs, system can appear "quiet" without obvious reason text.

## Why You Can See Only a Few Symbols Trading (Current State)

The current behavior strongly suggests this is mostly gating, not a single hard pipeline failure:

1. Symbol coverage gate:
- Current config symbol list has 64 symbols.
- Current validated configs are 48.
- So 16 symbols are not eligible for live signal processing yet.
- Missing validated symbols currently are:
  `GBPZAR`, `USDCNH`, `XTIUSD`, `XBRUSD`, `XNGUSD`, `US500`, `FR40`, `ES35`,
  `HK50`, `AU200`, `BTCUSD`, `LTCUSD`, `SOLUSD`, `BCHUSD`, `DOGUSD`, `TRXUSD`.

2. Decision outcome mix from latest trade log snapshot:
- Total symbols in latest decision snapshot: 46.
- `NO_ACTIONABLE_WINNER_SIGNAL`: 33
- `NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN`: 3
- `EXECUTED`: 6
- `SKIPPED_RISK_CAP`: 3
- `SKIPPED_POSITION_EXISTS`: 1

3. Repeated unknown-position-timeframe observations:
- Several symbols repeatedly report open positions where timeframe metadata cannot be inferred.
- This blocks secondary entries on those symbols.
- Recent log sample showed this repeatedly on:
  `NZDUSD`, `AUDNZD`, `EURJPY`, `EURCAD`, `GBPAUD`, `GBPCHF`.

4. Winners-only + strict live gates:
- Exact regime winner required.
- Strategy can output flat signal even when selected.
- Actionable margin further reduces final eligible actions.

## Secondary Request: Supporting 2+ MT5 Accounts with Different Balances

## Reality Constraint
For robust concurrent trading across multiple MT5 accounts, use one MT5 terminal instance per account (or one worker process per terminal/account). Treat each account as an isolated execution engine.

## Recommended Target Architecture

### Option A (Fastest to ship): Account-Isolated Traders
Run one trader worker per account:
- Separate MT5 login/session.
- Separate risk basis from that account's balance/equity.
- Separate decision throttle and actionable logs per account.
- Shared strategy configs can be reused.

Pros:
- Minimal redesign.
- Strong fault isolation.

Cons:
- Duplicate signal computation across accounts.

### Option B (Better long-term): Shared Signal Engine + Account Executors
1. Compute candidate selection once per symbol/timeframe.
2. Publish normalized trade intent.
3. Fan out to N account executors.
4. Each executor applies account-specific risk, symbol mapping, and broker constraints.

Pros:
- Consistent decisions across accounts.
- Less duplicated compute.

Cons:
- More engineering effort (intent bus, idempotency, orchestration).

## Required Changes (Either Option)

1. Config model
- Add `accounts` list in config.
- Per-account fields: login/server/path, enabled flag, risk overrides, symbol whitelist, optional multiplier.

2. Runtime orchestration
- Introduce account manager that starts/stops workers and handles reconnect independently per account.

3. State separation
- Decision throttle and actionable logs must be account-scoped.
- Trade history files should include account id.

4. Execution context
- Pass account context into execution path so sizing uses that account's balance/equity.
- Keep broker symbol resolution and symbol specs account-scoped.

5. Idempotency and safety
- Decision key should include account id to avoid cross-account suppression collisions.
- Preserve duplicate-order protection per account and symbol/magic.

6. Observability
- Add account dimension to all logs and dashboard records.
- Add per-account health and order error metrics.

7. Testing
- Simulate mixed account balances and verify per-account volume differences.
- Simulate one account disconnected while others continue.
- Verify one account order rejection does not block other accounts.

## Suggested Implementation Path

1. Introduce account-aware config schema and validation.
2. Refactor current single-account trader into a reusable worker class.
3. Add multi-account orchestrator (start, supervise, stop workers).
4. Make throttle/actionable logs account-scoped.
5. Add account-aware dashboard/report fields.
6. Add integration tests for dual-account execution and risk divergence.

This approach gives you independent execution per account while preserving current strategy behavior.
