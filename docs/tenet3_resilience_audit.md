# Tenet 3 — Engineering Resilience Audit (live loop)

Read-only audit of the live trading loop's non-blocking fail-safes. Scope: `pm_main.py`
trading loop + `process_all_symbols`. Verdict: **already production-grade at the loop level.**

## Verified strengths
- **No bare `except:`** anywhere in `pm_main.py`/`pm_mt5.py`; broad handlers are explicit.
- **Outer loop is crash-resistant** (`run_trading`, pm_main.py:5770-5824): `while not
  shutdown_event`; per-cycle work in try/except that logs and continues; `KeyboardInterrupt`
  -> clean `finally: shutdown()`.
- **Reconnect is non-blocking with backoff** (pm_main.py:5612-5649, 5773-5780): disconnect ->
  config-driven attempts/interval/cooldown -> `continue`. Trading resumes when the link
  recovers; it does not block or busy-spin.
- **Fail-safe skips on missing/bad state** (process_all_symbols, pm_main.py:1495-1542):
  no position snapshot / no account / not `trade_allowed` / margin-refresh failure -> log +
  `return False` (skip iteration), never trade on stale/unknown state.
- **Capital protection runs first**: `_run_margin_protection_cycle` before any entries
  (pm_main.py:1517); RECOVERY/PANIC -> skip NEW entries (margin cycle handles reductions).
- **Per-symbol isolation** (pm_main.py:1560-1574): each `_process_symbol` in its own
  try/except -> one symbol's exception cannot abort the sweep for others.
- **Ancillary hooks individually guarded**: storage, recent deals, drift sync, observatory,
  governance each in their own try/except -> non-critical failures never break the cycle.
- Account state, margin checks, PnL all sourced from MT5 (see mt5_api_authority_audit.md).

## Follow-ups (not yet audited; lower priority)
1. **Execution-layer order results**: confirm failed/partial order sends are handled
   idempotently (retry vs abort) and never leave local state inconsistent with broker.
   (Trace `_process_symbol` -> order send path.)
2. **Management under PANIC**: entries are skipped under margin PANIC; verify open-position
   exits/SL still execute via the margin cycle (appears yes) and aren't starved.
3. **Watchdog/heartbeat**: outer loop relies on `is_connected()`; consider a stale-data
   watchdog (last successful bar age) to fail safe if the feed silently goes stale while
   nominally "connected".

## Conclusion
Tenet 3 at the live-loop level is met: non-blocking, isolated, fail-safe, capital-first. No
remediation required to proceed to a test-environment live run. The three follow-ups are
hardening, not blockers.
