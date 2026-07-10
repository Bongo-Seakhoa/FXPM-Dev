"""
Diagnose why MT5 returns no/partial history for specific symbols.

Run against your RUNNING, logged-in MT5 terminal. Places no orders. For each
symbol it probes every timeframe with a small and a large count, prints the bar
count / date span / last_error, and searches the broker universe for the real
name, so the cause (no history at all vs M5-only gap vs name mismatch) is
unambiguous.

    python tools/diagnose_symbol_history.py
    python tools/diagnose_symbol_history.py Platinum Palladium APPLE
    python tools/diagnose_symbol_history.py --count 300000 --wait 3
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone, timedelta

try:
    import MetaTrader5 as mt5
except Exception as exc:
    print(f"MetaTrader5 package not importable: {exc}")
    sys.exit(1)

DEFAULT_SYMBOLS = [
    "Platinum", "Palladium", "USDTHB",
    "XAGEUR", "XAUAUD", "XAUEUR", "XAUGBP",
    "APPLE", "TESLA", "GOOGLE",
    "XAUUSD", "XAGUSD", "EURUSD",
]


def _len(arr) -> int:
    return 0 if arr is None else len(arr)


def _timeframes():
    return [
        ("M5", mt5.TIMEFRAME_M5), ("M15", mt5.TIMEFRAME_M15),
        ("M30", mt5.TIMEFRAME_M30), ("H1", mt5.TIMEFRAME_H1),
        ("H4", mt5.TIMEFRAME_H4), ("D1", mt5.TIMEFRAME_D1),
    ]


def _span(rates):
    if _len(rates) == 0:
        return ""
    try:
        a = datetime.fromtimestamp(int(rates[0]["time"]), tz=timezone.utc).date()
        b = datetime.fromtimestamp(int(rates[-1]["time"]), tz=timezone.utc).date()
        return f"{a}..{b}"
    except Exception:
        return ""


def _candidates(all_symbols, base):
    key = base.upper()
    key = key[:-3] if key.endswith("USD") else key
    key = key[:4]
    hits = [getattr(s, "name", "") for s in all_symbols
            if key and key in getattr(s, "name", "").upper()]
    return hits[:10]


def paged_depth(symbol, count, maxbars=0, page=50000):
    """Walk history backwards by date like the FxPM loader; report total M5 bars."""
    tf = mt5.TIMEFRAME_M5
    if maxbars > 0:
        page = min(page, max(1000, maxbars - 1))
    total, oldest_seen, newest_seen, prev_oldest = 0, None, None, None
    anchor = datetime.now(timezone.utc)
    while total < count:
        n = min(page, count - total)
        chunk = mt5.copy_rates_from(symbol, tf, anchor, n)
        if chunk is None or len(chunk) == 0:
            if mt5.last_error()[0] == -2 and n > 500:
                page = max(500, n // 2)
                continue
            break
        times = [int(r["time"]) for r in chunk]
        total += len(chunk)
        if newest_seen is None:
            newest_seen = max(times)
        oldest = min(times)
        oldest_seen = oldest
        if prev_oldest is not None and oldest >= prev_oldest:
            break
        prev_oldest = oldest
        anchor = datetime.fromtimestamp(oldest, tz=timezone.utc) - timedelta(seconds=1)
    span = ""
    if oldest_seen is not None:
        a = datetime.fromtimestamp(oldest_seen, tz=timezone.utc).date()
        b = datetime.fromtimestamp(newest_seen, tz=timezone.utc).date()
        span = f"{a}..{b}"
    return total, span


def diagnose(symbol, count, wait, all_symbols, maxbars=0):
    print(f"\n=== {symbol} ===")
    info = mt5.symbol_info(symbol)
    if info is None:
        print(f"  symbol_info=None -> '{symbol}' is not a broker name as-is.")
        print(f"  candidate broker names: {_candidates(all_symbols, symbol) or 'none'}")
        return

    mt5.symbol_select(symbol, True)
    try:
        mt5.symbol_info_tick(symbol)
    except Exception:
        pass
    if wait > 0:
        time.sleep(wait)

    print(f"  visible={getattr(info,'visible','?')} select={getattr(info,'select','?')} "
          f"trade_mode={getattr(info,'trade_mode','?')} digits={getattr(info,'digits','?')} "
          f"start={getattr(info,'start_time','?')}")

    any_data = 0
    for name, tf in _timeframes():
        small = mt5.copy_rates_from_pos(symbol, tf, 0, 100)
        err_small = mt5.last_error()
        big = mt5.copy_rates_from_pos(symbol, tf, 0, count)
        err_big = mt5.last_error()
        any_data = max(any_data, _len(small), _len(big))
        print(f"  {name:>3}: small(100)={_len(small):>6} {err_small}  "
              f"big({count})={_len(big):>7} {err_big}  {_span(big) or _span(small)}")

    depth, span = paged_depth(symbol, count, maxbars=maxbars)
    print(f"  PAGED M5 depth (date-walk): {depth} bars {span}")
    # Only a cap problem when the setting itself is below the target.
    stuck_at_cap = 0 < maxbars < count and depth <= maxbars * 1.5
    if any_data == 0:
        print("  VERDICT: NO history on ANY timeframe -> broker not serving this symbol here.")
    elif depth >= count:
        print(f"  VERDICT: date-walk retrieves the full {count} M5 bars. Fixed.")
    elif stuck_at_cap:
        print(f"  VERDICT: stuck at 'Max bars in chart'={maxbars} (< {count}); set it to Unlimited")
        print("           (Tools>Options>Charts), RESTART MT5, re-run.")
    else:
        print(f"  VERDICT: {depth} M5 bars is the broker's genuine depth (< {count}); "
              "needs min-bars relief.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbols", nargs="*")
    ap.add_argument("--count", type=int, default=300000)
    ap.add_argument("--wait", type=float, default=2.0)
    args = ap.parse_args()

    if not mt5.initialize():
        print(f"mt5.initialize() failed: {mt5.last_error()} (terminal running and logged in?)")
        return 1
    try:
        term, acct = mt5.terminal_info(), mt5.account_info()
        print(f"Terminal: build={getattr(term,'build','?')} maxbars={getattr(term,'maxbars','?')} "
              f"connected={getattr(term,'connected','?')}")
        print(f"Account:  login={getattr(acct,'login','?')} server={getattr(acct,'server','?')}")
        all_symbols = mt5.symbols_get() or []
        maxbars = int(getattr(term, "maxbars", 0) or 0)
        print(f"Broker exposes {len(all_symbols)} symbols total.")
        if 0 < maxbars < args.count:
            print(f"WARNING: terminal 'Max bars in chart'={maxbars} < {args.count}; fresh symbols "
                  f"will cap at {maxbars}. Set it to Unlimited and restart MT5.")
        for sym in (args.symbols or DEFAULT_SYMBOLS):
            diagnose(sym, args.count, args.wait, all_symbols, maxbars=maxbars)
    finally:
        mt5.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
