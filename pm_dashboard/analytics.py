"""
Analytics module for computing performance metrics, equity curves, and statistics.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .utils import parse_timestamp, normalize_symbol


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

    for trade in all_trades:
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
    long_wins = sum(t.get("pnl", 0.0) or 0.0 for t in trades if (t.get("direction", "").upper() == "LONG" or t.get("direction", "").lower() == "buy") and (t.get("pnl", 0.0) or 0.0) > 0)
    long_losses = abs(sum(t.get("pnl", 0.0) or 0.0 for t in trades if (t.get("direction", "").upper() == "LONG" or t.get("direction", "").lower() == "buy") and (t.get("pnl", 0.0) or 0.0) < 0))
    short_wins = sum(t.get("pnl", 0.0) or 0.0 for t in trades if (t.get("direction", "").upper() == "SHORT" or t.get("direction", "").lower() == "sell") and (t.get("pnl", 0.0) or 0.0) > 0)
    short_losses = abs(sum(t.get("pnl", 0.0) or 0.0 for t in trades if (t.get("direction", "").upper() == "SHORT" or t.get("direction", "").lower() == "sell") and (t.get("pnl", 0.0) or 0.0) < 0))
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
        strat = trade.get("strategy") or "Unknown"
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
            "strategy": trade.get("strategy")
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
