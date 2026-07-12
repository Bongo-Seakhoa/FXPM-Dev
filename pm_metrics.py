"""Shared performance metric helpers for FXPM.

The helpers in this module are intentionally small and dependency-light so the
backtester, dashboard, benchmark tools, and audit scripts can share metric
semantics without importing each other.
"""

from __future__ import annotations

import math
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np


METRIC_CONTRACT_VERSION = "2026-06-18-canonical-v1"
CALMAR_LEGACY_BASIS = "cumulative_return_pct_over_max_drawdown_pct"


def safe_float(value: object, default: float = 0.0, *, allow_inf: bool = False) -> float:
    """Convert a value to float while rejecting NaN and, by default, infinity."""
    try:
        out = float(value)
    except Exception:
        return float(default)
    if math.isnan(out):
        return float(default)
    if math.isinf(out) and not allow_inf:
        return float(default)
    return out


def round_metric(value: float, digits: int = 2) -> float:
    """Round finite metrics and preserve non-finite sentinels such as PF=inf."""
    value = float(value)
    return round(value, digits) if math.isfinite(value) else value


def gross_profit_loss(pnls: Iterable[float]) -> Tuple[float, float]:
    """Return gross profit and absolute gross loss from signed PnL values."""
    gross_profit = 0.0
    gross_loss = 0.0
    for pnl in pnls:
        value = safe_float(pnl)
        if value > 0.0:
            gross_profit += value
        elif value < 0.0:
            gross_loss += abs(value)
    return gross_profit, gross_loss


def profit_factor_from_gross(gross_profit: float, gross_loss: float) -> float:
    """Canonical profit-factor sentinel: positive no-loss samples are infinite."""
    gp = max(0.0, safe_float(gross_profit, allow_inf=True))
    gl = max(0.0, safe_float(gross_loss, allow_inf=True))
    if gl > 0.0:
        return gp / gl
    return float("inf") if gp > 0.0 else 0.0


def profit_factor_from_pnls(pnls: Iterable[float]) -> float:
    gross_profit, gross_loss = gross_profit_loss(pnls)
    return profit_factor_from_gross(gross_profit, gross_loss)


def max_drawdown_from_equity(equity_curve: Sequence[float]) -> Tuple[float, float]:
    """Return max drawdown as (percent, absolute currency units)."""
    values = [safe_float(value) for value in equity_curve]
    if not values:
        return 0.0, 0.0
    peak = values[0]
    max_dd_pct = 0.0
    max_dd_abs = 0.0
    for equity in values:
        if equity > peak:
            peak = equity
        drawdown_abs = max(0.0, peak - equity)
        drawdown_pct = (drawdown_abs / peak * 100.0) if peak > 0.0 else 0.0
        max_dd_abs = max(max_dd_abs, drawdown_abs)
        max_dd_pct = max(max_dd_pct, drawdown_pct)
    return max_dd_pct, max_dd_abs


def annualized_return_pct(
    initial_equity: float,
    final_equity: float,
    periods: int,
    periods_per_year: float,
) -> Optional[float]:
    """Compound annualized return from equally spaced return periods."""
    initial = safe_float(initial_equity)
    final = safe_float(final_equity)
    periods = int(periods or 0)
    bpy = safe_float(periods_per_year)
    if initial <= 0.0 or final <= 0.0 or periods <= 0 or bpy <= 0.0:
        return None
    try:
        return ((final / initial) ** (bpy / periods) - 1.0) * 100.0
    except Exception:
        return None


def return_drawdown_metrics(
    *,
    initial_equity: float,
    final_equity: float,
    max_drawdown_pct: float,
    periods: int = 0,
    periods_per_year: float = 0.0,
) -> dict:
    """Return explicit drawdown-ratio and annualized-Calmar fields.

    `calmar_ratio` remains a backward-compatible alias for the historical FXPM
    cumulative-return/DD value. The explicit `return_drawdown_ratio` and
    `calmar_ratio_basis` fields prevent it being mistaken for annualized Calmar.
    """
    initial = safe_float(initial_equity)
    final = safe_float(final_equity)
    max_dd = max(0.0, safe_float(max_drawdown_pct))
    total_return_pct = ((final - initial) / initial * 100.0) if initial > 0.0 else 0.0
    return_dd_ratio = (total_return_pct / max_dd) if max_dd > 0.0 else 0.0
    annualized = annualized_return_pct(initial, final, periods, periods_per_year)
    annualized_calmar = (annualized / max_dd) if annualized is not None and max_dd > 0.0 else 0.0
    return {
        "total_return_pct": total_return_pct,
        "return_drawdown_ratio": return_dd_ratio,
        "calmar_ratio": return_dd_ratio,
        "calmar_ratio_annualized": annualized_calmar,
        "annualized_return_pct": annualized if annualized is not None else 0.0,
        "calmar_ratio_basis": CALMAR_LEGACY_BASIS,
        "metric_contract_version": METRIC_CONTRACT_VERSION,
    }


def sharpe_sortino_from_returns(
    returns: Sequence[float],
    periods_per_year: float,
) -> Tuple[float, float]:
    """Annualized Sharpe/Sortino from an equally spaced return series."""
    arr = np.asarray([safe_float(value) for value in returns], dtype=np.float64)
    if arr.size < 2:
        return 0.0, 0.0
    bpy = safe_float(periods_per_year)
    if bpy <= 0.0:
        bpy = 252.0
    mean_return = float(np.mean(arr))
    std_return = float(np.std(arr, ddof=1)) if arr.size >= 2 else 0.0
    sharpe = (mean_return / std_return) * math.sqrt(bpy) if std_return > 1e-8 else 0.0
    negative = arr[arr < 0.0]
    if negative.size >= 2:
        downside_std = float(np.std(negative, ddof=1))
    elif negative.size == 1:
        downside_std = float(abs(negative[0]))
    else:
        downside_std = 0.0
    if downside_std > 1e-8:
        sortino = (mean_return / downside_std) * math.sqrt(bpy)
    elif mean_return > 0.0 and negative.size == 0:
        sortino = float("inf")
    else:
        sortino = 0.0
    return sharpe, sortino


def trade_sharpe_sortino_from_pnl(pnls: Sequence[float], periods_per_year: float = 252.0) -> Tuple[float, float]:
    """Dashboard-compatible trade-PnL Sharpe/Sortino using shared sentinels."""
    return sharpe_sortino_from_returns([safe_float(value) for value in pnls], periods_per_year)


def expectancy_per_trade(pnls: Sequence[float]) -> float:
    values: List[float] = [safe_float(value) for value in pnls]
    return float(sum(values) / len(values)) if values else 0.0
