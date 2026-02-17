"""
Analytics module for computing performance metrics, equity curves, and statistics.
"""
from __future__ import annotations

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
    parse_timestamp,
    normalize_symbol,
    normalize_timeframe,
    normalize_regime,
    load_pm_configs,
)

logger = logging.getLogger(__name__)

INDEX_SYMBOLS = {
    "US30", "US100", "DE30", "EU50", "UK100", "JP225",
    "US500", "FR40", "ES35", "HK50", "AU200",
}
CRYPTO_SYMBOLS = {
    "BTCUSD", "ETHUSD", "LTCUSD", "SOLUSD", "BCHUSD",
    "DOGUSD", "TRXUSD", "XRPUSD", "TONUSD", "BTCETH", "BTCXAU",
}
ENERGY_SYMBOLS = {"XTIUSD", "XBRUSD", "XNGUSD"}
METAL_SYMBOLS = {"XRX", "PLATINUM", "PALLADIUM"}


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


def load_trade_history(pm_root: str, max_files: int = 100) -> List[Dict[str, Any]]:
    """Load trade history from pm_outputs/trades_*.json files."""
    if not pm_root:
        return []

    trades_dir = os.path.join(pm_root, "pm_outputs")
    if not os.path.isdir(trades_dir):
        return []

    all_trades: List[Dict[str, Any]] = []
    trade_files = []

    for filename in os.listdir(trades_dir):
        if filename.startswith("trades_") and filename.endswith(".json"):
            filepath = os.path.join(trades_dir, filename)
            if os.path.isfile(filepath):
                trade_files.append((filepath, os.path.getmtime(filepath)))

    trade_files.sort(key=lambda x: x[1], reverse=True)
    trade_files = trade_files[:max_files]

    for filepath, _ in trade_files:
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
        if "timestamp" in trade:
            ts = parse_timestamp(trade["timestamp"])
            if ts:
                trade["_parsed_timestamp"] = ts

    all_trades.sort(key=lambda t: t.get("_parsed_timestamp") or datetime.min, reverse=True)

    return all_trades


def compute_equity_curve(trades: List[Dict[str, Any]], initial_capital: float = 10000.0) -> List[Dict[str, Any]]:
    """
    Compute equity curve from trade history.
    Returns list of {timestamp, equity, pnl, cumulative_pnl} points.
    """
    if not trades:
        return []

    sorted_trades = sorted(
        [t for t in trades if t.get("_parsed_timestamp")],
        key=lambda t: t["_parsed_timestamp"]
    )

    if not sorted_trades:
        return []

    equity = initial_capital
    cumulative_pnl = 0.0
    curve: List[Dict[str, Any]] = []

    curve.append({
        "timestamp": sorted_trades[0]["_parsed_timestamp"].isoformat() if sorted_trades else datetime.now().isoformat(),
        "equity": equity,
        "pnl": 0.0,
        "cumulative_pnl": 0.0
    })

    for trade in sorted_trades:
        pnl = trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0
        equity += pnl
        cumulative_pnl += pnl

        curve.append({
            "timestamp": trade["_parsed_timestamp"].isoformat(),
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


def compute_performance_metrics(trades: List[Dict[str, Any]], initial_capital: float = 10000.0) -> Dict[str, Any]:
    """Compute comprehensive performance metrics from trade history."""
    if not trades:
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
            "total_return_pct": 0.0,
            "avg_trade_pnl": 0.0,
            "best_trade": 0.0,
            "worst_trade": 0.0
        }

    pnls = []
    wins = []
    losses = []

    for trade in trades:
        pnl = trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0
        pnls.append(pnl)

        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(pnl)

    total_trades = len(trades)
    winning_trades = len(wins)
    losing_trades = len(losses)
    win_rate = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0

    total_pnl = sum(pnls)
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0

    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

    equity_curve = compute_equity_curve(trades, initial_capital)
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

    # Expectancy (avg_win * win_rate - avg_loss * loss_rate)
    loss_rate = (losing_trades / total_trades) if total_trades > 0 else 0.0
    expectancy = avg_win * (win_rate / 100.0) + avg_loss * loss_rate

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
        v = t.get("pnl")
        if v is None:
            v = t.get("profit", 0.0)
        return float(v or 0.0)

    long_wins = sum(_get_pnl(t) for t in trades if _is_long(t) and _get_pnl(t) > 0)
    long_losses = abs(sum(_get_pnl(t) for t in trades if _is_long(t) and _get_pnl(t) < 0))
    short_wins = sum(_get_pnl(t) for t in trades if _is_short(t) and _get_pnl(t) > 0)
    short_losses = abs(sum(_get_pnl(t) for t in trades if _is_short(t) and _get_pnl(t) < 0))
    long_pf = (long_wins / long_losses) if long_losses > 0 else 0.0
    short_pf = (short_wins / short_losses) if short_losses > 0 else 0.0

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
        "gross_loss": round(gross_loss, 2)
    }


def compute_breakdown_by_field(trades: List[Dict[str, Any]], field: str) -> Dict[str, Dict[str, Any]]:
    """
    Compute performance breakdown by a specific field (symbol, strategy, timeframe, regime).
    Returns dict mapping field_value -> performance metrics.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for trade in trades:
        value = trade.get(field, "N/A")
        if value:
            grouped[str(value)].append(trade)

    breakdown = {}
    for value, group_trades in grouped.items():
        breakdown[value] = compute_performance_metrics(group_trades, initial_capital=10000.0)

    return breakdown


def compute_monthly_performance(trades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Compute monthly performance breakdown.
    Returns dict mapping "YYYY-MM" -> performance metrics.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for trade in trades:
        ts = trade.get("_parsed_timestamp")
        if ts:
            month_key = ts.strftime("%Y-%m")
            grouped[month_key].append(trade)

    monthly = {}
    for month, group_trades in grouped.items():
        monthly[month] = compute_performance_metrics(group_trades, initial_capital=10000.0)

    return dict(sorted(monthly.items(), reverse=True))


def compute_daily_pnl(trades: List[Dict[str, Any]], days: int = 30) -> List[Dict[str, Any]]:
    """
    Compute daily PnL for the last N days.
    Returns list of {date, pnl, num_trades} sorted by date descending.
    """
    if not trades:
        return []

    daily: Dict[str, List[float]] = defaultdict(list)

    for trade in trades:
        ts = trade.get("_parsed_timestamp")
        if ts:
            date_key = ts.strftime("%Y-%m-%d")
            pnl = trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0
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

    for trade in trades:
        ts = trade.get("_parsed_timestamp")
        if not ts:
            continue
        pnl = trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0
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
    for trade in trades:
        strat = trade.get("strategy") or trade.get("strategy_name") or "Unknown"
        pnl = trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0
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


def get_pip_size(symbol: str) -> float:
    """
    Get pip size for a symbol.

    Args:
        symbol: Symbol name

    Returns:
        Pip size (default: 0.0001 for most FX pairs)
    """
    upper = symbol.upper()

    # JPY pairs use 0.01
    if 'JPY' in upper:
        return 0.01
    # Metals
    if upper.startswith("XAU"):
        return 0.1
    if upper.startswith("XAG"):
        return 0.01
    if upper in METAL_SYMBOLS:
        return 0.01
    # Indices
    if upper in INDEX_SYMBOLS:
        return 0.1
    # Energy
    if upper in ENERGY_SYMBOLS:
        return 0.001 if upper == "XNGUSD" else 0.01
    # Crypto
    if upper in CRYPTO_SYMBOLS:
        return 1.0
    # Default FX
    return 0.0001


def reconstruct_trade_outcome(trade_entry: Dict[str, Any],
                              historical_bars: pd.DataFrame,
                              timeout_bars: int = 1000) -> Dict[str, Any]:
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

        pip_size = get_pip_size(symbol)

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
        for idx, bar in bars_after_entry.iterrows():
            if direction == 'LONG':
                # Check SL first (conservative)
                if bar['Low'] <= sl:
                    pnl_pips = (sl - entry_price) / pip_size if pip_size > 0 else 0
                    duration = (idx - entry_time).total_seconds() / 60
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': sl,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': pnl_pips,
                        'duration_minutes': duration
                    }
                # Check TP
                elif bar['High'] >= tp:
                    pnl_pips = (tp - entry_price) / pip_size if pip_size > 0 else 0
                    duration = (idx - entry_time).total_seconds() / 60
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': tp,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': pnl_pips,
                        'duration_minutes': duration
                    }

            else:  # SHORT
                # Check SL first (conservative)
                if bar['High'] >= sl:
                    pnl_pips = (entry_price - sl) / pip_size if pip_size > 0 else 0
                    duration = (idx - entry_time).total_seconds() / 60
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': sl,
                        'close_reason': 'SL_HIT',
                        'pnl_pips': (entry_price - sl) / pip_size if pip_size > 0 else 0,
                        'duration_minutes': duration
                    }
                # Check TP
                elif bar['Low'] <= tp:
                    pnl_pips = (entry_price - tp) / pip_size if pip_size > 0 else 0
                    duration = (idx - entry_time).total_seconds() / 60
                    return {
                        'exit_timestamp': idx.isoformat(),
                        'exit_price': tp,
                        'close_reason': 'TP_HIT',
                        'pnl_pips': pnl_pips,
                        'duration_minutes': duration
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


def reconstruct_trade_outcomes(trades: List[Dict[str, Any]],
                               data_loader_func,
                               max_trades: int = 1000) -> List[Dict[str, Any]]:
    """
    Reconstruct outcomes for multiple trades.

    Args:
        trades: List of trade entries
        data_loader_func: Function(symbol, timeframe, start_date, end_date) -> DataFrame
        max_trades: Maximum number of trades to process

    Returns:
        List of trades with reconstructed outcomes
    """
    reconstructed = []

    for trade in trades[:max_trades]:
        try:
            symbol = trade.get('symbol')
            timeframe = trade.get('timeframe')
            timestamp = trade.get('_parsed_timestamp')

            if not timestamp:
                timestamp = parse_timestamp(trade.get('timestamp'))

            if not symbol or not timestamp:
                continue
            if not timeframe:
                timeframe = "M5"

            # Load historical data (30 days should be enough for most trades)
            end_date = timestamp + timedelta(days=30)
            historical_bars = data_loader_func(symbol, timeframe, timestamp, end_date)

            if historical_bars is None or len(historical_bars) == 0:
                logger.warning(f"No historical data for {symbol} {timeframe} at {timestamp}")
                continue

            # Reconstruct outcome
            outcome = reconstruct_trade_outcome(trade, historical_bars)

            # Merge outcome into trade
            trade_with_outcome = dict(trade)
            trade_with_outcome.update(outcome)

            # Calculate pnl in dollars using volume and instrument-aware pip value
            if outcome['pnl_pips'] != 0:
                volume = float(trade.get('volume', 0.0) or 1.0)
                symbol = trade.get('symbol', '')
                upper = symbol.upper()
                # Approximate pip value per lot (conservative default)
                # Standard FX = $10/pip/lot, JPY pairs ~ $7-10, crypto/indices vary
                pip_value_per_lot = 10.0  # Reasonable default for FX
                if upper.startswith("XAU") or upper.startswith("XAG") or upper in METAL_SYMBOLS or upper in ENERGY_SYMBOLS:
                    pip_value_per_lot = 1.0  # Metals/Energy: conservative $1/pip/lot
                elif upper in CRYPTO_SYMBOLS:
                    pip_value_per_lot = 1.0  # Crypto: $1/pip/lot (1.0 pip_size)
                elif upper in INDEX_SYMBOLS:
                    pip_value_per_lot = 1.0  # Indices: $1/pip/lot (0.1 pip_size)
                trade_with_outcome['pnl'] = outcome['pnl_pips'] * pip_value_per_lot * volume
            else:
                trade_with_outcome['pnl'] = 0.0

            reconstructed.append(trade_with_outcome)

        except Exception as e:
            logger.error(f"Failed to reconstruct trade: {e}")
            continue

    return reconstructed


def build_analytics_payload(pm_root: str, initial_capital: float = 10000.0) -> Dict[str, Any]:
    """Build complete analytics payload for the frontend."""
    trades = load_trade_history(pm_root, max_files=100)

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

    equity_curve = compute_equity_curve(trades, initial_capital)
    drawdown_curve = compute_drawdown_curve(equity_curve)
    metrics = compute_performance_metrics(trades, initial_capital)

    by_symbol = compute_breakdown_by_field(trades, "symbol")
    by_timeframe = compute_breakdown_by_field(trades, "timeframe")
    by_regime = compute_breakdown_by_field(trades, "regime")

    monthly = compute_monthly_performance(trades)
    daily_pnl = compute_daily_pnl(trades, days=30)
    heatmap = compute_hour_day_heatmap(trades)
    strategy_ranking = compute_strategy_ranking(trades, top_n=10)

    recent_trades = []
    for trade in trades[:50]:
        recent_trades.append({
            "timestamp": trade.get("timestamp"),
            "symbol": trade.get("symbol"),
            "direction": trade.get("direction"),
            "volume": trade.get("volume"),
            "price": trade.get("price"),
            "sl": trade.get("sl"),
            "tp": trade.get("tp"),
            "pnl": trade.get("pnl", 0.0) or trade.get("profit", 0.0) or 0.0,
            "status": trade.get("status"),
            "timeframe": trade.get("timeframe"),
            "regime": trade.get("regime"),
            "strategy": trade.get("strategy") or trade.get("strategy_name"),
            "close_reason": trade.get("close_reason"),
            "exit_price": trade.get("exit_price"),
            "pnl_pips": trade.get("pnl_pips")
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
        "heatmap": heatmap,
        "strategy_ranking": strategy_ranking
    }
