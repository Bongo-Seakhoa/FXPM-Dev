"""
Analytics module for computing performance metrics, equity curves, and statistics.
"""
from __future__ import annotations

import copy
import json
import logging
import os
import zlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from .utils import (
    load_instrument_specs,
    normalize_symbol,
    normalize_timeframe,
    normalize_regime,
    load_pm_configs,
    parse_timestamp,
    pip_size_from_spec,
)

logger = logging.getLogger(__name__)

_TRADE_HISTORY_CACHE: Dict[Tuple[str, int, Tuple[Tuple[str, int, int], ...], Tuple[str, int, int]], List[Dict[str, Any]]] = {}
_TRADE_HISTORY_CACHE_VERSION = "v2"

_REALIZED_STATUSES = {
    "CLOSED",
    "CLOSE",
    "CLOSED_TP",
    "CLOSED_SL",
    "TP_HIT",
    "SL_HIT",
    "EXITED",
    "FILLED",
    "DONE",
    "SETTLED",
}

_OPEN_STATUSES = {
    "OPEN",
    "PENDING",
    "PLACED",
    "ORDER_SENT",
    "SUBMITTED",
}


def _coerce_magic(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _encode_magic(symbol: str, timeframe: str, regime: str) -> int:
    key = f"{normalize_symbol(symbol)}|{normalize_timeframe(timeframe)}|{normalize_regime(regime)}"
    return zlib.crc32(key.encode("utf-8")) & 0x7FFFFFFF


def _strategy_from_pm_config(pm_cfg: Dict[str, Any], timeframe: str, regime: str) -> str:
    tf = normalize_timeframe(timeframe)
    rg = normalize_regime(regime)
    regime_configs = pm_cfg.get("regime_configs", {})
    if tf and rg and isinstance(regime_configs, dict):
        tf_cfg = regime_configs.get(tf, {})
        if isinstance(tf_cfg, dict):
            reg_cfg = tf_cfg.get(rg, {})
            if isinstance(reg_cfg, dict):
                strategy_name = str(reg_cfg.get("strategy_name") or "").strip()
                if strategy_name:
                    return strategy_name
    strategy_name = str(pm_cfg.get("strategy_name") or "").strip()
    if strategy_name:
        return strategy_name
    default_cfg = pm_cfg.get("default_config", {})
    if isinstance(default_cfg, dict):
        return str(default_cfg.get("strategy_name") or "").strip()
    return ""


def _build_magic_lookup(pm_configs: Dict[str, Any]) -> Dict[str, Dict[int, Dict[str, str]]]:
    """
    Build mapping:
      symbol -> magic -> {timeframe, regime, strategy}
    """
    lookup: Dict[str, Dict[int, Dict[str, str]]] = {}

    for symbol_key, cfg in pm_configs.items():
        if not isinstance(cfg, dict):
            continue
        symbol = normalize_symbol(cfg.get("symbol") or symbol_key)
        if not symbol:
            continue
        symbol_map = lookup.setdefault(symbol, {})

        regime_configs = cfg.get("regime_configs", {})
        has_regime_rows = False
        if isinstance(regime_configs, dict):
            for tf_key, regimes in regime_configs.items():
                tf = normalize_timeframe(tf_key)
                if not tf or not isinstance(regimes, dict):
                    continue
                for regime_key, reg_cfg in regimes.items():
                    if not isinstance(reg_cfg, dict):
                        continue
                    regime = normalize_regime(regime_key)
                    if not regime:
                        continue
                    has_regime_rows = True
                    magic = _encode_magic(symbol, tf, regime)
                    symbol_map[magic] = {
                        "timeframe": tf,
                        "regime": regime,
                        "strategy": str(reg_cfg.get("strategy_name") or "").strip(),
                    }

        # Legacy single-timeframe config fallback.
        if not has_regime_rows:
            tf = normalize_timeframe(cfg.get("timeframe"))
            if tf:
                magic = _encode_magic(symbol, tf, "LEGACY")
                symbol_map[magic] = {
                    "timeframe": tf,
                    "regime": "LEGACY",
                    "strategy": _strategy_from_pm_config(cfg, tf, "LEGACY"),
                }

    return lookup


def _enrich_trade_metadata(
    trade: Dict[str, Any],
    pm_configs: Dict[str, Any],
    magic_lookup: Dict[str, Dict[int, Dict[str, str]]],
) -> None:
    symbol = normalize_symbol(trade.get("symbol"))
    if symbol:
        trade["symbol"] = symbol
    if not symbol:
        return

    timeframe = normalize_timeframe(trade.get("timeframe"))
    regime = normalize_regime(trade.get("regime"))
    strategy = str(trade.get("strategy") or trade.get("strategy_name") or "").strip()

    magic = _coerce_magic(trade.get("magic"))
    if magic is not None:
        by_magic = magic_lookup.get(symbol, {})
        mapped = by_magic.get(magic)
        if mapped:
            timeframe = timeframe or mapped.get("timeframe", "")
            regime = regime or mapped.get("regime", "")
            if not strategy:
                strategy = mapped.get("strategy", "")

    pm_cfg = pm_configs.get(symbol)
    if isinstance(pm_cfg, dict):
        if not timeframe:
            timeframe = normalize_timeframe(pm_cfg.get("timeframe"))

        if timeframe and not regime:
            regime_configs = pm_cfg.get("regime_configs", {})
            if isinstance(regime_configs, dict):
                tf_cfg = regime_configs.get(timeframe, {})
                if isinstance(tf_cfg, dict) and len(tf_cfg) == 1:
                    regime = normalize_regime(next(iter(tf_cfg.keys())))

        if not strategy:
            strategy = _strategy_from_pm_config(pm_cfg, timeframe, regime)

    if timeframe:
        trade["timeframe"] = timeframe
    if regime:
        trade["regime"] = regime
    if strategy:
        trade["strategy"] = strategy
        trade["strategy_name"] = strategy


def _path_signature(path: str) -> Tuple[str, int, int]:
    stat = os.stat(path)
    return (path, int(stat.st_mtime_ns), int(stat.st_size))


def _trade_file_signature(pm_root: str, max_files: int) -> Tuple[Tuple[str, int, int], ...]:
    trades_dir = os.path.join(pm_root, "pm_outputs")
    if not os.path.isdir(trades_dir):
        return tuple()

    trade_files: List[Tuple[str, float]] = []
    for filename in os.listdir(trades_dir):
        if filename.startswith("trades_") and filename.endswith(".json"):
            filepath = os.path.join(trades_dir, filename)
            if os.path.isfile(filepath):
                try:
                    trade_files.append((filepath, os.path.getmtime(filepath)))
                except OSError:
                    continue

    trade_files.sort(key=lambda item: item[1], reverse=True)
    selected = [path for path, _ in trade_files[:max_files]]

    signature: List[Tuple[str, int, int]] = []
    for path in selected:
        try:
            signature.append(_path_signature(path))
        except OSError:
            continue
    return tuple(signature)


def _pm_config_signature(pm_root: str) -> Tuple[str, int, int]:
    path = os.path.join(pm_root, "pm_configs.json")
    if not os.path.isfile(path):
        return (path, 0, 0)
    try:
        return _path_signature(path)
    except OSError:
        return (path, 0, 0)


def _root_config_signature(pm_root: str) -> Tuple[str, int, int]:
    path = os.path.join(pm_root, "config.json")
    if not os.path.isfile(path):
        return (path, 0, 0)
    try:
        return _path_signature(path)
    except OSError:
        return (path, 0, 0)


def _normalize_cache_root(pm_root: str) -> str:
    return os.path.normcase(os.path.abspath(pm_root))


def _trade_entry_timestamp(trade: Dict[str, Any]) -> Optional[datetime]:
    cached = trade.get("_parsed_timestamp")
    if isinstance(cached, datetime):
        return cached
    return parse_timestamp(trade.get("timestamp"))


def _trade_sort_timestamp(trade: Dict[str, Any]) -> Optional[datetime]:
    cached = trade.get("_sort_timestamp")
    if isinstance(cached, datetime):
        return cached

    for key in (
        "exit_timestamp",
        "close_timestamp",
        "closed_at",
        "close_time",
        "exit_time",
        "updated_at",
        "filled_at",
    ):
        ts = parse_timestamp(trade.get(key))
        if ts is not None:
            return ts
    return _trade_entry_timestamp(trade)


def _trade_pnl_value(trade: Dict[str, Any]) -> float:
    for key in ("pnl", "profit", "net_profit", "realized_pnl"):
        value = trade.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _is_realized_trade(trade: Dict[str, Any]) -> bool:
    if not isinstance(trade, dict):
        return False

    explicit = trade.get("realized")
    if explicit is True:
        return True
    if explicit is False:
        return False

    explicit_flag = trade.get("is_realized")
    if explicit_flag is True:
        return True
    if explicit_flag is False:
        return False

    if trade.get("exit_price") is not None or trade.get("exit_timestamp") is not None or trade.get("close_reason"):
        return True

    for key in ("pnl", "profit", "net_profit", "realized_pnl", "pnl_pips"):
        value = trade.get(key)
        if value is None:
            continue
        try:
            if abs(float(value)) > 1e-12:
                return True
        except (TypeError, ValueError):
            continue

    status = str(trade.get("status") or trade.get("state") or "").strip().upper()
    action = str(trade.get("action") or trade.get("reason") or "").strip().upper()
    if status in _OPEN_STATUSES:
        return False
    if status in _REALIZED_STATUSES:
        return True
    if action in _REALIZED_STATUSES:
        return True

    return False


def _realized_trades(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [trade for trade in trades if _is_realized_trade(trade)]


def load_trade_history(pm_root: str, max_files: int = 100) -> List[Dict[str, Any]]:
    """Load trade history from pm_outputs/trades_*.json files."""
    if not pm_root:
        return []

    trades_dir = os.path.join(pm_root, "pm_outputs")
    if not os.path.isdir(trades_dir):
        return []

    trade_signature = _trade_file_signature(pm_root, max_files)
    config_signature = _pm_config_signature(pm_root)
    root_config_signature = _root_config_signature(pm_root)
    cache_key = (
        _TRADE_HISTORY_CACHE_VERSION,
        _normalize_cache_root(pm_root),
        max_files,
        trade_signature,
        config_signature,
        root_config_signature,
    )

    cached = _TRADE_HISTORY_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    all_trades: List[Dict[str, Any]] = []
    for filepath, _, _ in trade_signature:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                all_trades.extend(data)
        except Exception:
            continue

    pm_configs = load_pm_configs(pm_root)
    magic_lookup = _build_magic_lookup(pm_configs)

    for trade in all_trades:
        if not isinstance(trade, dict):
            continue
        _enrich_trade_metadata(trade, pm_configs, magic_lookup)
        if "symbol" in trade:
            trade["symbol"] = normalize_symbol(trade["symbol"])
        trade["realized"] = _is_realized_trade(trade)
        trade["pnl"] = _trade_pnl_value(trade) if trade["realized"] else 0.0
        entry_ts = _trade_entry_timestamp(trade)
        if entry_ts:
            trade["_parsed_timestamp"] = entry_ts
        sort_ts = _trade_sort_timestamp(trade)
        if sort_ts:
            trade["_sort_timestamp"] = sort_ts

    all_trades.sort(
        key=lambda t: (t.get("_sort_timestamp") or t.get("_parsed_timestamp") or datetime.min, t.get("symbol", ""), t.get("entry_id", "")),
        reverse=True,
    )
    _TRADE_HISTORY_CACHE[cache_key] = copy.deepcopy(all_trades)
    return all_trades


def compute_equity_curve(trades: List[Dict[str, Any]], initial_capital: float = 10000.0) -> List[Dict[str, Any]]:
    """
    Compute equity curve from trade history.
    Returns list of {timestamp, equity, pnl, cumulative_pnl} points.
    """
    if not trades:
        return []

    realized_trades = _realized_trades(trades)
    sorted_trades = sorted(
        [t for t in realized_trades if _trade_sort_timestamp(t)],
        key=lambda t: _trade_sort_timestamp(t) or datetime.min,
    )

    if not sorted_trades:
        return []

    equity = initial_capital
    cumulative_pnl = 0.0
    curve: List[Dict[str, Any]] = []

    curve.append({
        "timestamp": (_trade_sort_timestamp(sorted_trades[0]) or datetime.now()).isoformat() if sorted_trades else datetime.now().isoformat(),
        "equity": equity,
        "pnl": 0.0,
        "cumulative_pnl": 0.0
    })

    for trade in sorted_trades:
        pnl = _trade_pnl_value(trade)
        equity += pnl
        cumulative_pnl += pnl
        trade_ts = _trade_sort_timestamp(trade) or datetime.now()

        curve.append({
            "timestamp": trade_ts.isoformat(),
            "equity": equity,
            "pnl": pnl,
            "cumulative_pnl": cumulative_pnl
        })

    return curve


def compute_drawdown_curve(equity_curve: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compute drawdown curve from equity curve.
    Returns list of {timestamp, drawdown_pct, drawdown_abs, peak_equity} points.
    """
    if not equity_curve:
        return []

    drawdown_curve: List[Dict[str, Any]] = []
    peak = equity_curve[0]["equity"]

    for point in equity_curve:
        equity = point["equity"]
        if equity > peak:
            peak = equity

        drawdown_abs = peak - equity
        drawdown_pct = (drawdown_abs / peak * 100.0) if peak > 0 else 0.0

        drawdown_curve.append({
            "timestamp": point["timestamp"],
            "drawdown_pct": drawdown_pct,
            "drawdown_abs": drawdown_abs,
            "peak_equity": peak,
            "equity": equity
        })

    return drawdown_curve


# ---------------------------------------------------------------------------
# Extended risk metrics: drawdown duration, recovery time, ulcer index
# ---------------------------------------------------------------------------

def _compute_drawdown_duration(equity_curve: List[float]) -> int:
    """Max consecutive trades spent in drawdown (equity below prior peak)."""
    if len(equity_curve) < 2:
        return 0
    peak = equity_curve[0]
    current_dd_len = 0
    max_dd_len = 0
    for eq in equity_curve[1:]:
        if eq >= peak:
            peak = eq
            current_dd_len = 0
        else:
            current_dd_len += 1
            max_dd_len = max(max_dd_len, current_dd_len)
    return max_dd_len


def _compute_recovery_time(equity_curve: List[float]) -> int:
    """Trades taken to recover from the maximum drawdown trough back to peak."""
    if len(equity_curve) < 2:
        return 0
    peak = equity_curve[0]
    max_dd = 0.0
    trough_idx = 0
    for idx, eq in enumerate(equity_curve):
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
            trough_idx = idx
    # Count trades from trough until equity recovers to or exceeds trough-era peak
    recovery_peak = max(equity_curve[:trough_idx + 1]) if trough_idx > 0 else equity_curve[0]
    for i in range(trough_idx, len(equity_curve)):
        if equity_curve[i] >= recovery_peak:
            return i - trough_idx
    # Not yet recovered
    return len(equity_curve) - 1 - trough_idx


def _compute_ulcer_index(equity_curve: List[float]) -> float:
    """Ulcer Index = sqrt(mean(drawdown_pct^2)).  Better risk metric than StdDev."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    dd_sq_sum = 0.0
    count = 0
    for eq in equity_curve[1:]:
        if eq > peak:
            peak = eq
        dd_pct = ((peak - eq) / peak * 100.0) if peak > 0 else 0.0
        dd_sq_sum += dd_pct ** 2
        count += 1
    return (dd_sq_sum / count) ** 0.5 if count > 0 else 0.0


def compute_performance_metrics(trades: List[Dict[str, Any]], initial_capital: float = 10000.0) -> Dict[str, Any]:
    """Compute comprehensive performance metrics from trade history."""
    realized_trades = _realized_trades(trades)
    if not realized_trades:
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "max_drawdown_abs": 0.0,
            "sharpe_ratio": 0.0,
            "sortino_ratio": 0.0,
            "calmar_ratio": 0.0,
            "recovery_factor": 0.0,
            "expectancy": 0.0,
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,
            "long_profit_factor": 0.0,
            "short_profit_factor": 0.0,
            "total_return_pct": 0.0,
            "avg_trade_pnl": 0.0,
            "best_trade": 0.0,
            "worst_trade": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "drawdown_duration": 0,
            "recovery_time": 0,
            "ulcer_index": 0.0,
        }

    pnls = []
    wins = []
    losses = []

    for trade in realized_trades:
        pnl = _trade_pnl_value(trade)
        pnls.append(pnl)

        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(pnl)

    total_trades = len(realized_trades)
    winning_trades = len(wins)
    losing_trades = len(losses)
    win_rate = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0

    total_pnl = sum(pnls)
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0

    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

    equity_curve = compute_equity_curve(realized_trades, initial_capital)
    drawdown_curve = compute_drawdown_curve(equity_curve)

    max_dd_pct = max((p["drawdown_pct"] for p in drawdown_curve), default=0.0)
    max_dd_abs = max((p["drawdown_abs"] for p in drawdown_curve), default=0.0)

    final_equity = equity_curve[-1]["equity"] if equity_curve else initial_capital
    total_return_pct = ((final_equity - initial_capital) / initial_capital * 100.0) if initial_capital > 0 else 0.0

    avg_trade_pnl = (total_pnl / total_trades) if total_trades > 0 else 0.0

    import math

    if len(pnls) > 1:
        pnl_mean = sum(pnls) / len(pnls)
        pnl_std = math.sqrt(sum((p - pnl_mean) ** 2 for p in pnls) / len(pnls))
        sharpe_ratio = (pnl_mean / pnl_std * math.sqrt(252)) if pnl_std > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    # Sortino ratio (downside deviation only)
    if len(pnls) > 1:
        pnl_mean = sum(pnls) / len(pnls)
        downside = [min(0.0, p - pnl_mean) ** 2 for p in pnls]
        downside_dev = math.sqrt(sum(downside) / len(downside))
        sortino_ratio = (pnl_mean / downside_dev * math.sqrt(252)) if downside_dev > 1e-8 else 0.0
    else:
        sortino_ratio = 0.0

    # Calmar ratio (annualized return / max drawdown)
    calmar_ratio = (total_return_pct / max_dd_pct) if max_dd_pct > 0 else 0.0

    # Recovery factor (net profit / max drawdown $)
    recovery_factor = (total_pnl / max_dd_abs) if max_dd_abs > 0 else 0.0

    # Expectancy (avg_win * win_rate - |avg_loss| * loss_rate)
    loss_rate = (losing_trades / total_trades) if total_trades > 0 else 0.0
    expectancy = avg_trade_pnl

    # Consecutive wins/losses
    max_consec_wins = 0
    max_consec_losses = 0
    cur_wins = 0
    cur_losses = 0
    for p in pnls:
        if p > 0:
            cur_wins += 1
            cur_losses = 0
            max_consec_wins = max(max_consec_wins, cur_wins)
        elif p < 0:
            cur_losses += 1
            cur_wins = 0
            max_consec_losses = max(max_consec_losses, cur_losses)
        else:
            cur_wins = 0
            cur_losses = 0

    # Long/Short profit factor
    def _is_long(t):
        d = (t.get("direction") or "").upper()
        return d in ("LONG", "BUY", "1")

    def _is_short(t):
        d = (t.get("direction") or "").upper()
        return d in ("SHORT", "SELL", "-1")

    def _get_pnl(t):
        return _trade_pnl_value(t)

    long_wins = sum(_get_pnl(t) for t in realized_trades if _is_long(t) and _get_pnl(t) > 0)
    long_losses = abs(sum(_get_pnl(t) for t in realized_trades if _is_long(t) and _get_pnl(t) < 0))
    short_wins = sum(_get_pnl(t) for t in realized_trades if _is_short(t) and _get_pnl(t) > 0)
    short_losses = abs(sum(_get_pnl(t) for t in realized_trades if _is_short(t) and _get_pnl(t) < 0))
    long_pf = (long_wins / long_losses) if long_losses > 0 else 0.0
    short_pf = (short_wins / short_losses) if short_losses > 0 else 0.0

    # Extract equity as plain floats for extended risk metrics
    _equity_floats = [p["equity"] for p in equity_curve] if equity_curve else [initial_capital]

    return {
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "max_drawdown_abs": round(max_dd_abs, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "sortino_ratio": round(sortino_ratio, 2),
        "calmar_ratio": round(calmar_ratio, 2),
        "recovery_factor": round(recovery_factor, 2),
        "expectancy": round(expectancy, 2),
        "max_consecutive_wins": max_consec_wins,
        "max_consecutive_losses": max_consec_losses,
        "long_profit_factor": round(long_pf, 2),
        "short_profit_factor": round(short_pf, 2),
        "total_return_pct": round(total_return_pct, 2),
        "avg_trade_pnl": round(avg_trade_pnl, 2),
        "best_trade": round(max(pnls, default=0.0), 2),
        "worst_trade": round(min(pnls, default=0.0), 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        # Extended risk metrics
        "drawdown_duration": _compute_drawdown_duration(_equity_floats),
        "recovery_time": _compute_recovery_time(_equity_floats),
        "ulcer_index": round(_compute_ulcer_index(_equity_floats), 4),
    }


def compute_breakdown_by_field(
    trades: List[Dict[str, Any]],
    field: str,
    initial_capital: float = 10000.0,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute performance breakdown by a specific field (symbol, strategy, timeframe, regime).
    Returns dict mapping field_value -> performance metrics.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for trade in _realized_trades(trades):
        value = trade.get(field, "N/A")
        if value:
            grouped[str(value)].append(trade)

    breakdown = {}
    for value, group_trades in grouped.items():
        breakdown[value] = compute_performance_metrics(group_trades, initial_capital=initial_capital)

    return breakdown


def compute_monthly_performance(
    trades: List[Dict[str, Any]],
    initial_capital: float = 10000.0,
) -> Dict[str, Dict[str, Any]]:
    """
    Compute monthly performance breakdown.
    Returns dict mapping "YYYY-MM" -> performance metrics.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for trade in _realized_trades(trades):
        ts = _trade_sort_timestamp(trade)
        if ts:
            month_key = ts.strftime("%Y-%m")
            grouped[month_key].append(trade)

    monthly = {}
    for month, group_trades in grouped.items():
        monthly[month] = compute_performance_metrics(group_trades, initial_capital=initial_capital)

    return dict(sorted(monthly.items(), reverse=True))


def compute_daily_pnl(trades: List[Dict[str, Any]], days: int = 30) -> List[Dict[str, Any]]:
    """
    Compute daily PnL for the last N days.
    Returns list of {date, pnl, num_trades} sorted by date descending.
    """
    if not trades:
        return []

    daily: Dict[str, List[float]] = defaultdict(list)

    for trade in _realized_trades(trades):
        ts = _trade_sort_timestamp(trade)
        if ts:
            date_key = ts.strftime("%Y-%m-%d")
            pnl = _trade_pnl_value(trade)
            daily[date_key].append(pnl)

    result = []
    for date_key, pnls in daily.items():
        result.append({
            "date": date_key,
            "pnl": round(sum(pnls), 2),
            "num_trades": len(pnls),
            "win_rate": round(sum(1 for p in pnls if p > 0) / len(pnls) * 100, 2) if pnls else 0.0
        })

    result.sort(key=lambda x: x["date"], reverse=True)
    return result[:days]


def compute_hour_day_heatmap(trades: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Compute P&L heatmap by hour x day-of-week.
    Returns dict mapping day_name -> {hour_str -> total_pnl}.
    """
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    heatmap: Dict[str, Dict[str, float]] = {d: {} for d in days}

    for trade in _realized_trades(trades):
        ts = _trade_sort_timestamp(trade)
        if not ts:
            continue
        pnl = _trade_pnl_value(trade)
        day_name = days[ts.weekday()]
        hour_str = str(ts.hour)
        heatmap[day_name][hour_str] = round(heatmap[day_name].get(hour_str, 0.0) + pnl, 2)

    return heatmap


def compute_strategy_ranking(trades: List[Dict[str, Any]], top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Rank strategies by total P&L.
    Returns list of {strategy, total_pnl, trades, win_rate} sorted desc by P&L.
    """
    grouped: Dict[str, List[float]] = defaultdict(list)
    for trade in _realized_trades(trades):
        strat = trade.get("strategy") or trade.get("strategy_name") or "Unknown"
        pnl = _trade_pnl_value(trade)
        grouped[strat].append(pnl)

    ranking = []
    for strat, pnls in grouped.items():
        wins = sum(1 for p in pnls if p > 0)
        wr = (wins / len(pnls) * 100.0) if pnls else 0.0
        ranking.append({
            "strategy": strat,
            "total_pnl": round(sum(pnls), 2),
            "trades": len(pnls),
            "win_rate": round(wr, 2)
        })

    ranking.sort(key=lambda x: x["total_pnl"], reverse=True)
    return ranking[:top_n]


def get_pip_size(symbol: str, instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None) -> float:
    """
    Get pip size for a symbol.

    Tries pm_core.get_instrument_spec first, then falls back to heuristic.

    Args:
        symbol: Symbol name

    Returns:
        Pip size (default: 0.0001 for most FX pairs)
    """
    symbol_norm = normalize_symbol(symbol)
    if instrument_specs:
        spec = instrument_specs.get(symbol_norm)
        pip_size = pip_size_from_spec(spec)
        if pip_size and pip_size > 0:
            return float(pip_size)

    try:
        from pm_core import get_instrument_spec
        spec = get_instrument_spec(symbol_norm)
        if spec and spec.pip_size > 0:
            return spec.pip_size
    except Exception:
        pass

    # Heuristic fallback
    sym = symbol_norm.upper()
    if 'JPY' in sym:
        return 0.01
    if sym in ('XAUUSD',):
        return 0.01  # Gold: 2-digit pip
    if sym in ('XAGUSD',):
        return 0.001
    if sym in ('US30', 'US100', 'US500', 'EU50', 'UK100', 'DE30', 'JP225',
               'FR40', 'ES35', 'AU200', 'HK50'):
        return 0.1
    if sym in ('BTCUSD', 'ETHUSD', 'XRPUSD', 'TONUSD', 'BTCETH', 'BCHUSD',
               'LTCUSD', 'SOLUSD', 'DOGUSD', 'TRXUSD'):
        return 1.0
    if sym in ('XBRUSD', 'XTIUSD', 'XNGUSD'):
        return 0.01
    return 0.0001


def _get_pip_value_per_lot(symbol: str, instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None) -> float:
    """Get pip value per standard lot from instrument specs, with fallback."""
    symbol_norm = normalize_symbol(symbol)
    if instrument_specs:
        spec = instrument_specs.get(symbol_norm)
        if isinstance(spec, dict):
            try:
                pip_value = float(spec.get("pip_value", 0.0) or 0.0)
                if pip_value > 0:
                    return pip_value
            except (TypeError, ValueError):
                pass
    try:
        from pm_core import get_instrument_spec
        spec = get_instrument_spec(symbol_norm)
        if spec and spec.pip_value > 0:
            return spec.pip_value
    except Exception:
        pass
    # Heuristic fallback
    sym = symbol_norm.upper()
    if sym in ('XAUUSD', 'XAGUSD'):
        return 1.0
    if sym in ('BTCUSD', 'ETHUSD', 'XRPUSD', 'TONUSD', 'BTCETH', 'BCHUSD',
               'LTCUSD', 'SOLUSD', 'DOGUSD', 'TRXUSD'):
        return 1.0
    if sym in ('US30', 'US100', 'US500', 'EU50', 'UK100', 'DE30', 'JP225',
               'FR40', 'ES35', 'AU200', 'HK50'):
        return 1.0
    if sym in ('XBRUSD', 'XTIUSD', 'XNGUSD'):
        return 1.0
    return 10.0  # Standard FX pip value per lot


def reconstruct_trade_outcome(
    trade_entry: Dict[str, Any],
    historical_bars: pd.DataFrame,
    timeout_bars: int = 1000,
    instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Simulate trade execution using historical bars.

    Walks through bars after entry to determine when SL or TP was hit.

    Args:
        trade_entry: Trade entry dict with timestamp, price, sl, tp, direction
        historical_bars: DataFrame with OHLC data (index = datetime)
        timeout_bars: Maximum bars to simulate before timeout

    Returns:
        Dict with exit_price, exit_timestamp, pnl_pips, close_reason, duration_minutes
    """
    try:
        # Extract trade details
        entry_time = trade_entry.get('_parsed_timestamp')
        if not entry_time:
            entry_time = parse_timestamp(trade_entry.get('timestamp'))
        if not entry_time:
            return {
                'exit_timestamp': None,
                'exit_price': None,
                'close_reason': 'INVALID_ENTRY_TIME',
                'pnl_pips': 0,
                'duration_minutes': None
            }

        entry_price = trade_entry.get('entry_price') or trade_entry.get('price')
        sl = trade_entry.get('sl') or trade_entry.get('stop_loss_price')
        tp = trade_entry.get('tp') or trade_entry.get('take_profit_price')
        direction_str = trade_entry.get('direction', '').upper()
        symbol = trade_entry.get('symbol', '')

        # Map direction
        if direction_str in ('LONG', 'BUY', '1'):
            direction = 'LONG'
        elif direction_str in ('SHORT', 'SELL', '-1'):
            direction = 'SHORT'
        else:
            return {
                'exit_timestamp': None,
                'exit_price': None,
                'close_reason': 'INVALID_DIRECTION',
                'pnl_pips': 0,
                'duration_minutes': None
            }

        if not entry_price or not sl or not tp:
            return {
                'exit_timestamp': None,
                'exit_price': None,
                'close_reason': 'MISSING_PRICES',
                'pnl_pips': 0,
                'duration_minutes': None
            }

        pip_size = get_pip_size(symbol, instrument_specs=instrument_specs)

        # Filter bars after entry
        bars_after_entry = historical_bars[historical_bars.index > entry_time].head(timeout_bars)

        if len(bars_after_entry) == 0:
            return {
                'exit_timestamp': None,
                'exit_price': None,
                'close_reason': 'NO_DATA',
                'pnl_pips': 0,
                'duration_minutes': None
            }

        # Walk through bars to find SL/TP hit
        # Mirrors pm_core A2 gap-through-stop semantics:
        # check Open first (gap-through), then intra-bar Low/High.
        for idx, bar in bars_after_entry.iterrows():
            bar_open = bar['Open']
            duration = (idx - entry_time).total_seconds() / 60

            if direction == 'LONG':
                # Gap-through SL at open
                if bar_open <= sl:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': bar_open,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': (bar_open - entry_price) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Intra-bar SL
                if bar['Low'] <= sl:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': sl,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': (sl - entry_price) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Gap-through TP at open
                if bar_open >= tp:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': bar_open,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': (bar_open - entry_price) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Intra-bar TP
                if bar['High'] >= tp:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': tp,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': (tp - entry_price) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }

            else:  # SHORT
                # Gap-through SL at open
                if bar_open >= sl:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': bar_open,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': (entry_price - bar_open) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Intra-bar SL
                if bar['High'] >= sl:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': sl,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': (entry_price - sl) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Gap-through TP at open
                if bar_open <= tp:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': bar_open,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': (entry_price - bar_open) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }
                # Intra-bar TP
                if bar['Low'] <= tp:
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': tp,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': (entry_price - tp) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration,
                    }

        # Timeout: no SL/TP hit within available data
        return {
            'exit_timestamp': None,
            'exit_price': None,
            'close_reason': 'TIMEOUT',
            'pnl_pips': 0,
            'duration_minutes': None
        }

    except Exception as e:
        logger.error(f"Error reconstructing trade outcome: {e}")
        return {
            'exit_timestamp': None,
            'exit_price': None,
            'close_reason': 'ERROR',
            'pnl_pips': 0,
            'duration_minutes': None
        }


def reconstruct_trade_outcomes(
    trades: List[Dict[str, Any]],
    data_loader_func,
    max_trades: int = 1000,
    instrument_specs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Reconstruct outcomes for multiple trades.

    Args:
        trades: List of trade entries
        data_loader_func: Function(symbol, timeframe, start_date, end_date) -> DataFrame
        max_trades: Maximum number of trades to process

    Returns:
        List of trades with reconstructed outcomes
    """
    grouped: Dict[Tuple[str, str], List[Tuple[int, Dict[str, Any], datetime]]] = defaultdict(list)
    for idx, trade in enumerate(trades[:max_trades]):
        symbol = normalize_symbol(trade.get('symbol'))
        timestamp = trade.get('_parsed_timestamp') or parse_timestamp(trade.get('timestamp'))
        if not symbol or not timestamp:
            continue
        timeframe = normalize_timeframe(trade.get('timeframe')) or "M5"
        grouped[(symbol, timeframe)].append((idx, trade, timestamp))

    reconstructed_by_index: Dict[int, Dict[str, Any]] = {}

    for (symbol, timeframe), batch in grouped.items():
        try:
            start_date = min(item[2] for item in batch)
            end_date = max(item[2] for item in batch) + timedelta(days=30)
            historical_bars = data_loader_func(symbol, timeframe, start_date, end_date)
        except Exception as exc:
            logger.error("Failed to load historical data batch for %s %s: %s", symbol, timeframe, exc)
            continue

        if historical_bars is None or len(historical_bars) == 0:
            logger.warning("No historical data for %s %s between %s and %s", symbol, timeframe, start_date, end_date)
            continue

        for idx, trade, _ in batch:
            try:
                outcome = reconstruct_trade_outcome(
                    trade,
                    historical_bars,
                    instrument_specs=instrument_specs,
                )
                trade_with_outcome = dict(trade)
                trade_with_outcome.update(outcome)

                if outcome['pnl_pips'] != 0:
                    volume = float(trade.get('volume', 0.0) or 1.0)
                    trade_symbol = normalize_symbol(trade.get('symbol') or '')
                    pip_value_per_lot = _get_pip_value_per_lot(trade_symbol, instrument_specs=instrument_specs)
                    trade_with_outcome['pnl'] = outcome['pnl_pips'] * pip_value_per_lot * volume
                else:
                    trade_with_outcome['pnl'] = 0.0

                reconstructed_by_index[idx] = trade_with_outcome
            except Exception as exc:
                logger.error("Failed to reconstruct trade: %s", exc)

    return [reconstructed_by_index[idx] for idx in sorted(reconstructed_by_index)]


def build_analytics_payload(pm_root: str, initial_capital: float = 10000.0) -> Dict[str, Any]:
    """Build complete analytics payload for the frontend."""
    trades = load_trade_history(pm_root, max_files=100)
    realized_trades = _realized_trades(trades)

    if not trades:
        return {
            "has_data": False,
            "metrics": compute_performance_metrics([]),
            "equity_curve": [],
            "drawdown_curve": [],
            "by_symbol": {},
            "by_timeframe": {},
            "by_regime": {},
            "monthly": {},
            "daily_pnl": [],
            "recent_trades": [],
            "heatmap": {},
            "strategy_ranking": []
        }

    equity_curve = compute_equity_curve(realized_trades, initial_capital)
    drawdown_curve = compute_drawdown_curve(equity_curve)
    metrics = compute_performance_metrics(realized_trades, initial_capital)

    by_symbol = compute_breakdown_by_field(realized_trades, "symbol", initial_capital=initial_capital)
    by_timeframe = compute_breakdown_by_field(realized_trades, "timeframe", initial_capital=initial_capital)
    by_regime = compute_breakdown_by_field(realized_trades, "regime", initial_capital=initial_capital)

    monthly = compute_monthly_performance(realized_trades, initial_capital=initial_capital)
    daily_pnl = compute_daily_pnl(realized_trades, days=30)
    heatmap = compute_hour_day_heatmap(realized_trades)
    strategy_ranking = compute_strategy_ranking(realized_trades, top_n=10)

    sorted_recent_trades = sorted(
        realized_trades,
        key=lambda trade: _trade_sort_timestamp(trade) or datetime.min,
        reverse=True,
    )
    recent_trades = []
    for trade in sorted_recent_trades[:50]:
        event_ts = _trade_sort_timestamp(trade) or _trade_entry_timestamp(trade)
        recent_trades.append({
            "timestamp": event_ts.isoformat() if event_ts else trade.get("timestamp"),
            "symbol": trade.get("symbol"),
            "direction": trade.get("direction"),
            "volume": trade.get("volume"),
            "price": trade.get("price"),
            "sl": trade.get("sl"),
            "tp": trade.get("tp"),
            "pnl": _trade_pnl_value(trade),
            "status": trade.get("status"),
            "timeframe": trade.get("timeframe"),
            "regime": trade.get("regime"),
            "strategy": trade.get("strategy") or trade.get("strategy_name"),
            "close_reason": trade.get("close_reason"),
            "exit_price": trade.get("exit_price"),
            "pnl_pips": trade.get("pnl_pips"),
            "realized": True,
        })

    return {
        "has_data": True,
        "metrics": metrics,
        "equity_curve": equity_curve,
        "drawdown_curve": drawdown_curve,
        "by_symbol": by_symbol,
        "by_timeframe": by_timeframe,
        "by_regime": by_regime,
        "monthly": monthly,
        "daily_pnl": daily_pnl,
        "recent_trades": recent_trades,
        "total_trades_loaded": len(trades),
        "realized_trades_loaded": len(realized_trades),
        "heatmap": heatmap,
        "strategy_ranking": strategy_ranking
    }
