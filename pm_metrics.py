"""Shared performance metric helpers for FXPM.

The helpers in this module are intentionally small and dependency-light so the
backtester, dashboard, benchmark tools, and audit scripts can share metric
semantics without importing each other.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np


METRIC_CONTRACT_VERSION = "2026-06-18-canonical-v1"
CALMAR_LEGACY_BASIS = "cumulative_return_pct_over_max_drawdown_pct"
EQUITY_REDUCER_CONTRACT_VERSION = "fxpm.equity-reducer.v1-reconstruction"


class EquityReductionError(ValueError):
    """A named accounting or valuation-contract failure, never a filtered row."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class EquityMark:
    timestamp: object
    equity: float


@dataclass(frozen=True)
class ExternalEquityFlow:
    timestamp: object
    kind: str
    amount: float


@dataclass(frozen=True)
class KnownFlatInterval:
    """Caller-supplied proof of unchanged equity except declared external flows."""

    start: object
    end: object


@dataclass(frozen=True)
class EquityPathReduction:
    contract_version: str
    evaluation_start: str
    evaluation_end: str
    initial_equity: float
    terminal_equity: float
    elapsed_calendar_days: float
    cumulative_log_growth: float
    time_weighted_return: float
    growth_rate_per_calendar_day: float
    calendar_days: Tuple[str, ...]
    observed_daily_log_totals: Tuple[Optional[float], ...]
    inference_days: Tuple[str, ...]
    inference_daily_log_returns: Tuple[float, ...]
    daily_coverage: Tuple[str, ...]
    missing_days: Tuple[str, ...]
    partial_days: Tuple[str, ...]
    carried_days: Tuple[str, ...]
    coverage_status: str
    coverage_reason: str


def _equity_timestamp(value: object) -> datetime:
    try:
        stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(stamp, datetime):
            raise TypeError("timestamp must be an aware datetime or ISO string")
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise EquityReductionError("NAIVE_TIMESTAMP", "timezone-aware timestamps are required")
        return stamp.astimezone(timezone.utc)
    except EquityReductionError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise EquityReductionError("INVALID_TIMESTAMP", "invalid equity-path timestamp") from exc


def _equity_number(value: object, *, initial: bool = False) -> float:
    code = "INVALID_INITIAL_EQUITY" if initial else "INVALID_EQUITY"
    try:
        if isinstance(value, (bool, np.bool_)):
            raise TypeError("boolean equity")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite equity")
    except (TypeError, ValueError, OverflowError) as exc:
        raise EquityReductionError(code, "equity must be finite and numeric") from exc
    if number <= 0:
        raise EquityReductionError(code if initial else "EQUITY_DEPLETED", "equity is nonpositive")
    return number


def _equity_mark(value: object) -> Tuple[datetime, float]:
    if isinstance(value, EquityMark):
        timestamp, equity = value.timestamp, value.equity
    elif isinstance(value, (tuple, list)) and len(value) == 2:
        timestamp, equity = value
    else:
        raise EquityReductionError("INVALID_EQUITY_MARK", "marks require a timestamp and equity")
    return _equity_timestamp(timestamp), _equity_number(equity)


def _equity_log_increment(equity: float, denominator: float) -> float:
    relative = (equity - denominator) / denominator
    if math.isfinite(relative) and relative > -1:
        return math.log1p(relative)
    # Positive extreme ratios can overflow or round a loss to exactly -1.
    return math.log(equity) - math.log(denominator)


def _equity_daily_chain(points, initial, start, flows):
    totals = {}
    increments = []
    denominator = initial
    if start in flows and (not points or points[0][0] != start):
        denominator = _equity_number(denominator + flows[start])
    for timestamp, equity in points:
        increment = _equity_log_increment(equity, denominator)
        increments.append(increment)
        day = timestamp.date()
        if timestamp > start and timestamp.time() == datetime.min.time():
            day -= timedelta(days=1)
        totals.setdefault(day.isoformat(), []).append(increment)
        denominator = _equity_number(equity + flows.get(timestamp, 0.0))
    return math.fsum(increments), {day: math.fsum(values) for day, values in totals.items()}


def reduce_equity_path(
    *,
    initial_equity: float,
    marks: Iterable[object],
    evaluation_start: object,
    evaluation_end: object,
    terminal_mark: object,
    external_flows: Iterable[ExternalEquityFlow] = (),
    complete_valuation_context: bool = False,
    known_flat_intervals: Iterable[KnownFlatInterval] = (),
) -> EquityPathReduction:
    """Reduce exact account equity under the accepted September metric direction.

    This is a reconstruction, not the recovered prototype. Initial equity is
    immediately before the first event at evaluation_start. The explicit
    terminal mark is at evaluation_end and is excluded from marks. Every flow
    follows a same-timestamp preflow mark; the initial baseline can serve at
    evaluation_start. Commission, swap and trading P&L remain in equity.

    Log increments use their right endpoint: a midnight mark closes the previous
    UTC day, except an event at the evaluation start belongs to its starting day.
    Observed totals group those increments; across unvalued boundaries they are
    diagnostics, not complete daily returns. Unknown days remain None. Flat
    proofs may carry a known valuation, never an unproved open-position mark.
    KNOWN_FLAT_BOUNDARY means a boundary was carried; that day may have P&L.

    Inference requires an explicit complete-context declaration and known day
    boundaries. Partial first/last days are always excluded. This declaration
    does not certify the producer, intraday risk coverage or trading authority.
    """
    initial = _equity_number(initial_equity, initial=True)
    start, end = _equity_timestamp(evaluation_start), _equity_timestamp(evaluation_end)
    if end <= start:
        raise EquityReductionError("INVALID_EVALUATION_WINDOW", "evaluation_end must follow evaluation_start")
    if type(complete_valuation_context) is not bool:
        raise EquityReductionError("INVALID_VALUATION_CONTEXT", "complete context must be an explicit bool")
    if terminal_mark is None:
        raise EquityReductionError("TERMINAL_MARK_MISSING", "an explicit terminal valuation is required")
    terminal = _equity_mark(terminal_mark)
    if terminal[0] != end:
        raise EquityReductionError("TERMINAL_MARK_GAP", "terminal mark must equal evaluation_end")
    points = [_equity_mark(mark) for mark in marks]
    points.append(terminal)
    previous_time = None
    for timestamp, _equity in points:
        if not start <= timestamp <= end:
            raise EquityReductionError("POINT_OUTSIDE_EVALUATION", "mark lies outside evaluation bounds")
        if previous_time is not None and timestamp <= previous_time:
            code = "DUPLICATE_MARK_TIMESTAMP" if timestamp == previous_time else "MARK_TIMESTAMP_ORDER"
            raise EquityReductionError(code, "marks must be strictly increasing after UTC normalization")
        previous_time = timestamp
    point_values = dict(points)
    flow_values = {}
    previous_time = None
    for flow in external_flows:
        if not isinstance(flow, ExternalEquityFlow) or flow.kind not in ("DEPOSIT", "WITHDRAWAL"):
            raise EquityReductionError("INVALID_EXTERNAL_FLOW", "typed DEPOSIT or WITHDRAWAL flows are required")
        timestamp = _equity_timestamp(flow.timestamp)
        try:
            if isinstance(flow.amount, (bool, np.bool_)):
                raise TypeError("boolean amount")
            amount = float(flow.amount)
            if not math.isfinite(amount) or amount <= 0:
                raise ValueError("nonpositive or nonfinite flow")
        except (TypeError, ValueError, OverflowError) as exc:
            raise EquityReductionError("INVALID_EXTERNAL_FLOW", "flow amount must be finite and positive") from exc
        if not start <= timestamp <= end:
            raise EquityReductionError("FLOW_OUTSIDE_EVALUATION", "flow lies outside evaluation bounds")
        if previous_time is not None and timestamp <= previous_time:
            code = "DUPLICATE_FLOW_TIMESTAMP" if timestamp == previous_time else "FLOW_TIMESTAMP_ORDER"
            raise EquityReductionError(code, "external flows must be strictly increasing")
        if timestamp != start and timestamp not in point_values:
            raise EquityReductionError("FLOW_PREFLOW_MARK_MISSING", "flow requires a same-timestamp preflow mark")
        flow_values[timestamp] = amount if flow.kind == "DEPOSIT" else -amount
        previous_time = timestamp
    growth, observed_totals = _equity_daily_chain(points, initial, start, flow_values)

    flat_intervals = []
    last_end = None
    for proof in known_flat_intervals:
        if not isinstance(proof, KnownFlatInterval):
            raise EquityReductionError("INVALID_FLAT_PROOF", "flat coverage requires typed intervals")
        proof_start, proof_end = _equity_timestamp(proof.start), _equity_timestamp(proof.end)
        if not start <= proof_start < proof_end <= end or (last_end is not None and proof_start < last_end):
            raise EquityReductionError("INVALID_FLAT_PROOF", "flat intervals must be ordered, nonoverlapping and within bounds")
        if flat_intervals and proof_start == flat_intervals[-1][1]:
            flat_intervals[-1] = (flat_intervals[-1][0], proof_end)
        else:
            flat_intervals.append((proof_start, proof_end))
        last_end = proof_end
    flat_anchors = []
    for proof_start, proof_end in flat_intervals:
        if proof_start == start:
            anchor = point_values.get(start, initial) + flow_values.get(start, 0.0)
        elif proof_start in point_values:
            anchor = point_values[proof_start] + flow_values.get(proof_start, 0.0)
        else:
            raise EquityReductionError("FLAT_PROOF_UNANCHORED", "flat proof requires a known starting valuation")
        anchor = _equity_number(anchor)
        expected = anchor
        for timestamp, equity in points:
            if proof_start < timestamp <= proof_end:
                if equity != expected:
                    raise EquityReductionError("FLAT_PROOF_CONTRADICTED", "observed economic change contradicts flat proof")
                expected = _equity_number(expected + flow_values.get(timestamp, 0.0))
        flat_anchors.append((proof_start, proof_end, anchor))

    calendar = []
    day_start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while day_start < end:
        calendar.append((day_start, day_start + timedelta(days=1)))
        day_start += timedelta(days=1)
    boundaries = {start: initial, **point_values}
    carried_boundaries = set()
    for day_start, day_end in calendar:
        for boundary in (day_start, day_end):
            if not start <= boundary <= end or boundary in boundaries:
                continue
            for proof_start, proof_end, anchor in flat_anchors:
                if proof_start < boundary <= proof_end:
                    value = anchor
                    for timestamp, _equity in points:
                        if proof_start < timestamp < boundary:
                            value += flow_values.get(timestamp, 0.0)
                    boundaries[boundary] = _equity_number(value)
                    carried_boundaries.add(boundary)
                    break
    coverage, missing, partial, carried, full_days = [], [], [], [], []
    for day_start, day_end in calendar:
        day = day_start.date().isoformat()
        if day_start < start or day_end > end:
            coverage.append("PARTIAL")
            partial.append(day)
        elif day_start not in boundaries or day_end not in boundaries:
            coverage.append("MISSING")
            missing.append(day)
        else:
            is_carried = day_start in carried_boundaries or day_end in carried_boundaries
            coverage.append("KNOWN_FLAT_BOUNDARY" if is_carried else "MARKED")
            if is_carried:
                carried.append(day)
            full_days.append(day)
    if complete_valuation_context and missing:
        raise EquityReductionError("DAILY_VALUATION_COVERAGE_UNPROVEN", f"unvalued UTC days: {', '.join(missing)}")
    daily_totals = observed_totals
    if carried_boundaries:
        augmented = sorted(points + [(timestamp, boundaries[timestamp]) for timestamp in carried_boundaries])
        _growth, daily_totals = _equity_daily_chain(augmented, initial, start, flow_values)
    elapsed = (end - start).total_seconds() / 86400.0
    try:
        twr = math.expm1(growth)
    except OverflowError as exc:
        raise EquityReductionError("GROWTH_OVERFLOW", "linked return cannot be represented") from exc
    inference_days = tuple(full_days) if complete_valuation_context else ()
    return EquityPathReduction(
        contract_version=EQUITY_REDUCER_CONTRACT_VERSION,
        evaluation_start=start.isoformat(), evaluation_end=end.isoformat(),
        initial_equity=initial, terminal_equity=terminal[1],
        elapsed_calendar_days=elapsed, cumulative_log_growth=growth,
        time_weighted_return=twr, growth_rate_per_calendar_day=growth / elapsed,
        calendar_days=tuple(left.date().isoformat() for left, _right in calendar),
        observed_daily_log_totals=tuple(daily_totals.get(left.date().isoformat()) for left, _right in calendar),
        inference_days=inference_days,
        inference_daily_log_returns=tuple(daily_totals[day] for day in inference_days),
        daily_coverage=tuple(coverage), missing_days=tuple(missing),
        partial_days=tuple(partial), carried_days=tuple(carried),
        coverage_status="COMPLETE_VALUATION" if complete_valuation_context else "EVENT_BOUNDED",
        coverage_reason=("COMPLETE_UTC_DAY_COVERAGE" if inference_days else "NO_COMPLETE_UTC_DAYS") if complete_valuation_context else "VALUATION_CONTEXT_UNPROVEN",
    )


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
