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


def safe_float_array(values: Iterable[object]) -> np.ndarray:
    """safe_float() over a sequence without a Python call per element.

    Equivalent to ``[safe_float(v) for v in values]`` - NaN and infinity map to
    0.0 - but as one conversion plus one nan_to_num. Anything numpy cannot
    convert wholesale falls back to the exact element-wise path, so mixed junk
    still behaves identically. This ran 106,697 times per backtest on an H1
    frame, once per bar.
    """
    try:
        arr = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError):
        return np.asarray([safe_float(v) for v in values], dtype=np.float64)
    return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)


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
    values = safe_float_array(equity_curve)
    if values.size == 0:
        return 0.0, 0.0
    # maximum.accumulate is the running peak seeded at values[0], matching the
    # sequential loop exactly - max is order-independent, unlike a sum.
    peaks = np.maximum.accumulate(values)
    # Python's float arithmetic overflows to inf silently; numpy warns. The
    # results agree either way, so silence the warning rather than let an
    # extreme equity curve spray RuntimeWarnings through production logs.
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        drawdown_abs = np.maximum(0.0, peaks - values)
        drawdown_pct = np.where(peaks > 0.0, drawdown_abs / peaks * 100.0, 0.0)
    return float(drawdown_pct.max()), float(drawdown_abs.max())


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
    arr = safe_float_array(returns)
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
