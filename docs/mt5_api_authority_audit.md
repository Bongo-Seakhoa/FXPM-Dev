# MT5 API Authority Audit

Principle (human directive, 2026-05-30): MT5 is the source of truth for anything it already
computes accurately. Don't re-derive it less accurately. Audit of where we comply vs where
remnants of manual replication remain. Read-only; no code changed.

## Already MT5-authoritative (good — keep)
- Account state: equity, margin, free margin, margin_level pulled from `account_info()`
  (`pm_mt5.py:423`, used `pm_main.py:2528/2671/2846/2851`). The old manual margin/equity calc
  is gone. ✓
- Authoritative calc wrappers exist: `order_calc_profit`, `order_calc_margin`
  (`pm_mt5.py:1315-1348`).
- Backtest/live PnL & sizing are tick-value based, not pip-approximated:
  `_calc_trade_pnl` uses `ticks * tick_value * size` when tick economics present
  (`pm_core.py:112-121`); sizing uses `tick_value` loss-per-lot (`pm_core.py:335-337`,
  `pm_main.py:2459-2470`).
- Live spec refresh on init: `sync_instrument_spec_from_mt5` updates tick_value, tick_size,
  volume_step, min/max lot, spread_avg (`pm_core.py:1755-1783`, called `pm_main.py:847-859`).

## Verified NOT gaps (live layer is fully MT5-authoritative — credit where due)
After full read of `sync_instrument_spec_from_mt5` (`pm_core.py:1755-1804`) and the live
entry path:
- Spec sync is COMPLETE: tick_value, tick_size, volume_step, min/max lot, spread,
  contract_size, **point, digits**, stops_level, swap_long/short, swap_mode, rollover3days.
- Swap uses MT5 rates: `_apply_swap_model` applies `spec.swap_long/short` with rollover-day
  timing (`pm_core.py:4413`) — broker rates + standard timing, not a guessed rate.
- Pre-trade + final margin checks use MT5 `calc_margin_required` vs free margin
  (`pm_main.py:4821-4876`, `5136-5188`) — not a manual leverage estimate.
- Account equity/margin/level from `account_info()`; PnL/sizing tick-value based.
So the LIVE contract is broker-truth end to end. (My initial P1/P4/P5 suspicions were wrong —
corrected after full read.)

## The one real remnant (high value)

### R1 — Backtest uses hand-maintained offline specs, not a captured MT5 snapshot
`INSTRUMENT_SPECS` (`pm_core.py:1808+`) is a hand-maintained default table "used when MT5
data is not available (e.g., backtesting from CSV)." This is the ONLY error-prone spec layer
— it is where the `digits=5`-for-all bug lived, and where index/crypto economics are guessed.
Live overrides it from MT5; backtest does not.

**Fix (the directive, applied):** persist one authoritative `symbol_info()` snapshot per
symbol to `broker_specs.json` (from the FBS account), load it for BOTH backtest and live, and
treat the hand-maintained table only as a last-resort fallback. This removes the entire
class of per-asset approximation, fixes non-FX cost economics, and lets historical spread be
floored/validated against the broker's real `symbol_info().spread`. Codex's
`c81696c "Prioritize broker specs for execution contract"` is moving here — coordinate so
the snapshot covers digits/point/contract_size/tick_value/spread/swap and the full 100-symbol
universe (incl. equities) before any full-universe promotion.

### R2 — Index/crypto historical spread reliability (subsumed by R1)
The frozen `Spread` column is a MetaQuotes-DEMO feed; for indices/crypto its values look
unreliable. The R1 broker snapshot is the fix (floor/validate demo spread against FBS
`symbol_info().spread`). Until then, non-FX honest-spread results are not trustworthy.

## Note
This audit is cheaper than it first looked: the live layer is already correct, so the work
collapses to R1 — make backtest consume the same broker truth live already uses.
