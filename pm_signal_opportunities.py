"""Descriptive, cash-free labels for an already prepared causal signal surface.

Every emitted nonzero signal remains a separate opportunity. This module does
not generate features, size positions, simulate an account or qualify a cell.
Stop APIs must honour ``bar_index``, as they do in the canonical backtester.
Only fully closed bars supply exit OHLC; an observed next open may start a
right-censored opportunity without consuming that bar's unfinished OHLC.
The caller authenticates the supplied provenance and supplies a causally
prepared, immutable surface; typed digests alone do not authenticate its data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
from typing import Any, Literal, Sequence

import numpy as np
import pandas as pd


SCHEMA_VERSION = "fxpm.signal-opportunity-labels.v1"
CORE_PRICING_REFERENCE_SHA256 = "e52c24e09d8b9de6da041a3bda817e0a5b1abddeb03ae9a20019a7cf0801331a"
_REGIMES = frozenset({"TREND", "RANGE", "CHOP", "BREAKOUT"})
_ADMISSION = frozenset({"ADMIT", "REFUSE", "MISSING", "UNAVAILABLE"})


class OpportunityLabelError(ValueError):
    """A technical input or pricing failure; never a losing/absent signal."""


@dataclass(frozen=True, slots=True)
class OpportunityProvenance:
    strategy_id: str
    configuration_sha256: str
    source_surface_sha256: str
    spread_tier: Literal["P75", "P95"]
    spread_model_sha256: str
    admission_policy_sha256: str


@dataclass(frozen=True, slots=True)
class SignalOpportunity:
    opportunity_id: str
    signal_bar: int
    signal_open_utc: str
    signal_close_utc: str
    direction: int
    run_start: bool
    entry_regime: str | None
    regime_status: Literal["AVAILABLE", "UNAVAILABLE"]
    raw_regime: str | None
    stop_loss_pips: float
    take_profit_pips: float
    status: Literal["CLOSED", "RIGHT_CENSORED", "NO_NEXT_OBSERVATION"]
    entry_bar: int | None = None
    entry_open_utc: str | None = None
    entry_admission: str | None = None
    entry_price: float | None = None
    stop_loss_price: float | None = None
    take_profit_price: float | None = None
    exit_bar: int | None = None
    exit_bar_open_utc: str | None = None
    exit_observed_at_utc: str | None = None
    exit_price: float | None = None
    exit_reason: Literal["SL", "TP"] | None = None
    exit_fill: Literal["GAP_OPEN", "STOP_LEVEL", "TARGET_LEVEL"] | None = None
    signed_price_delta: float | None = None
    signed_pips: float | None = None


@dataclass(frozen=True, slots=True)
class OpportunityBatch:
    provenance: OpportunityProvenance
    symbol: str
    timeframe: str
    pip_size: float
    cutoff_utc: str
    signal_start_utc: str | None
    raw_nonzero_count: int
    excluded_before_start_count: int
    window_nonzero_count: int
    unavailable_regime_count: int
    records: tuple[SignalOpportunity, ...]
    schema_version: str = field(default=SCHEMA_VERSION, init=False)
    quoted_spread_included: bool = field(default=True, init=False)
    swap_status: str = field(default="UNAVAILABLE_PRICE_UNIT_CONVERSION", init=False)
    complete_cost_net_profit_available: bool = field(default=False, init=False)
    qualification_authority: bool = field(default=False, init=False)


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OpportunityLabelError(name) from exc
    if pd.isna(stamp) or str(stamp.tz) != "UTC":
        raise OpportunityLabelError(name)
    return stamp


def _text(stamp: pd.Timestamp) -> str:
    return stamp.isoformat().replace("+00:00", "Z")


def _axis(values: Any, name: str) -> pd.DatetimeIndex:
    try:
        axis = pd.DatetimeIndex(values)
    except (TypeError, ValueError) as exc:
        raise OpportunityLabelError(name) from exc
    if axis.hasnans or str(axis.tz) != "UTC" or not axis.is_monotonic_increasing or not axis.is_unique:
        raise OpportunityLabelError(name)
    return axis


def _numeric_prefix(values: Any, count: int, total: int, index: pd.Index, name: str) -> np.ndarray:
    if isinstance(values, pd.Series) and not values.index.equals(index):
        raise OpportunityLabelError(name + "_AXIS_INVALID")
    try:
        if len(values) != total:
            raise OpportunityLabelError(name + "_LENGTH_INVALID")
        prefix = values.iloc[:count] if isinstance(values, pd.Series) else values[:count]
        result = np.asarray(prefix, dtype=np.float64)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OpportunityLabelError(name + "_INVALID") from exc
    if result.ndim != 1 or not np.isfinite(result).all():
        raise OpportunityLabelError(name + "_INVALID")
    return result


def _provenance(value: OpportunityProvenance) -> None:
    if type(value) is not OpportunityProvenance or not isinstance(value.strategy_id, str) or not value.strategy_id:
        raise OpportunityLabelError("PROVENANCE_INVALID")
    if value.spread_tier not in {"P75", "P95"}:
        raise OpportunityLabelError("SPREAD_TIER_INVALID")
    for digest in (value.configuration_sha256, value.source_surface_sha256,
                   value.spread_model_sha256, value.admission_policy_sha256):
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise OpportunityLabelError("PROVENANCE_DIGEST_INVALID")


def _stops(strategy: Any, features: pd.DataFrame, bar: int, direction: int,
           symbol: str, timeframe: str, regime: str | None, spec: Any) -> tuple[float, float]:
    try:
        intent_builder = getattr(strategy, "build_trade_intent", None)
        stop_builder = getattr(strategy, "calculate_stops", None)
        if callable(intent_builder):
            intent = intent_builder(features, symbol=symbol, timeframe=timeframe,
                regime=regime or "", signal=direction, spec=spec, bar_index=bar,
                apply_regime_tp_multiplier=False)
            raw = (intent.stop_loss_pips, intent.take_profit_pips)
        elif callable(stop_builder):
            raw = stop_builder(features, direction, symbol, spec=spec, bar_index=bar)
        else:
            raise OpportunityLabelError("STOP_API_REQUIRED")
        if len(raw) != 2 or any(isinstance(value, (bool, np.bool_)) for value in raw):
            raise OpportunityLabelError("STOPS_INVALID")
        stop, target = (float(value) for value in raw)
    except OpportunityLabelError:
        raise
    except Exception as exc:
        raise OpportunityLabelError("STOP_CALCULATION_FAILED") from exc
    if not math.isfinite(stop) or not math.isfinite(target) or stop <= 0 or target <= 0:
        raise OpportunityLabelError("STOPS_INVALID")
    return stop, target


class _FirstTouchIndex:
    """Linear storage/build; earliest suffix touch in O(log n) per label."""

    def __init__(self, low: np.ndarray, high: np.ndarray):
        self.count = len(low)
        self.size = 1 << max(0, (self.count - 1).bit_length())
        self.minimum = np.full(2 * self.size, np.inf, dtype=np.float64)
        self.maximum = np.full(2 * self.size, -np.inf, dtype=np.float64)
        self.minimum[self.size:self.size + self.count] = low
        self.maximum[self.size:self.size + self.count] = high
        for node in range(self.size - 1, 0, -1):
            self.minimum[node] = min(self.minimum[2 * node], self.minimum[2 * node + 1])
            self.maximum[node] = max(self.maximum[2 * node], self.maximum[2 * node + 1])

    def first(self, start: int, lower: float, upper: float) -> int | None:
        if start >= self.count:
            return None

        def visit(node: int, left: int, right: int) -> int | None:
            if right <= start or left >= self.count:
                return None
            if self.minimum[node] > lower and self.maximum[node] < upper:
                return None
            if right - left == 1:
                return left
            middle = (left + right) // 2
            found = visit(2 * node, left, middle)
            return found if found is not None else visit(2 * node + 1, middle, right)

        return visit(1, 0, self.size)


def _fixed_exit(direction: int, opened: float, low: float, high: float,
                stop: float, target: float) -> tuple[float, str, str]:
    if (direction == 1 and opened <= stop) or (direction == -1 and opened >= stop):
        return opened, "SL", "GAP_OPEN"
    if (direction == 1 and low <= stop) or (direction == -1 and high >= stop):
        return stop, "SL", "STOP_LEVEL"
    if (direction == 1 and high >= target) or (direction == -1 and low <= target):
        return target, "TP", "TARGET_LEVEL"
    raise OpportunityLabelError("TOUCH_INDEX_INCONSISTENT")


def label_signal_opportunities(
    features: pd.DataFrame, raw_signals: pd.Series, *, strategy: Any, spec: Any,
    symbol: str, timeframe: str, cutoff_utc: Any,
    entry_half_spreads: Sequence[float] | pd.Series,
    exit_half_spreads: Sequence[float] | pd.Series,
    entry_admission: Sequence[str] | pd.Series,
    provenance: OpportunityProvenance, signal_start_utc: Any | None = None,
) -> OpportunityBatch:
    """Label each nonzero closed-bar emission without an account or occupancy.

    The optional start is a signal-close boundary; pre-start emissions remain in
    the denominator. Refused/missing spread admission never removes a label.
    Caller-provided spreads are already modeled half-spreads in price units.
    No extra spread charge or cash-based financing estimate is added.
    Inputs must remain immutable during this call. Public stop implementations
    must honour bar_index; no feature generation or source authentication runs
    here. DataFrame attrs are not input features and are isolated for local
    stop-API scratch caches. End-of-data account liquidation is not a label exit.
    """
    _provenance(provenance)
    if not isinstance(features, pd.DataFrame) or not isinstance(raw_signals, pd.Series):
        raise OpportunityLabelError("SURFACE_TYPE_INVALID")
    required = {"Open", "High", "Low", "Close", "_BAR_CLOSE_TS", "REGIME_LIVE"}
    if not required.issubset(features.columns) or not features.columns.is_unique:
        raise OpportunityLabelError("SURFACE_COLUMNS_INVALID")
    if not raw_signals.index.equals(features.index):
        raise OpportunityLabelError("SIGNAL_AXIS_INVALID")
    opens = _axis(features.index, "OPEN_AXIS_INVALID")
    closes = _axis(features["_BAR_CLOSE_TS"], "CLOSE_AXIS_INVALID")
    if (closes <= opens).any() or (closes[:-1] > opens[1:]).any():
        raise OpportunityLabelError("BAR_AVAILABILITY_AXIS_INVALID")
    cutoff = _utc(cutoff_utc, "CUTOFF_UTC_INVALID")
    start = None if signal_start_utc is None else _utc(signal_start_utc, "SIGNAL_START_UTC_INVALID")
    if start is not None and start > cutoff:
        raise OpportunityLabelError("SIGNAL_WINDOW_INVALID")
    if not isinstance(symbol, str) or not symbol or not isinstance(timeframe, str) or not timeframe:
        raise OpportunityLabelError("CELL_IDENTITY_INVALID")
    try:
        pip_size = float(spec.pip_size)
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise OpportunityLabelError("INSTRUMENT_PIP_GEOMETRY_INVALID") from exc
    if getattr(spec, "symbol", None) != symbol or not math.isfinite(pip_size) or pip_size <= 0:
        raise OpportunityLabelError("INSTRUMENT_PIP_GEOMETRY_INVALID")
    if getattr(strategy, "name", None) != provenance.strategy_id:
        raise OpportunityLabelError("STRATEGY_IDENTITY_MISMATCH")
    total = len(features)
    visible = int(closes.searchsorted(cutoff, side="right"))
    observed_opens = int(opens.searchsorted(cutoff, side="left"))
    signals = _numeric_prefix(raw_signals, visible, total, features.index, "SIGNALS")
    if not np.isin(signals, (-1, 0, 1)).all():
        raise OpportunityLabelError("SIGNALS_INVALID")
    open_values = _numeric_prefix(features["Open"], observed_opens, total, features.index, "OPEN")
    high = _numeric_prefix(features["High"], visible, total, features.index, "HIGH")
    low = _numeric_prefix(features["Low"], visible, total, features.index, "LOW")
    close = _numeric_prefix(features["Close"], visible, total, features.index, "CLOSE")
    if np.any(high < low) or np.any(high < open_values[:visible]) or np.any(low > open_values[:visible]) or np.any(high < close) or np.any(low > close):
        raise OpportunityLabelError("OHLC_GEOMETRY_INVALID")
    entry_spreads = _numeric_prefix(entry_half_spreads, observed_opens, total, features.index, "ENTRY_SPREAD")
    exit_spreads = _numeric_prefix(exit_half_spreads, visible, total, features.index, "EXIT_SPREAD")
    if np.any(entry_spreads < 0) or np.any(exit_spreads < 0):
        raise OpportunityLabelError("SPREAD_PRICE_INVALID")
    if isinstance(entry_admission, pd.Series) and not entry_admission.index.equals(features.index):
        raise OpportunityLabelError("ADMISSION_AXIS_INVALID")
    if len(entry_admission) != total:
        raise OpportunityLabelError("ADMISSION_LENGTH_INVALID")
    admission = entry_admission.iloc[:observed_opens] if isinstance(entry_admission, pd.Series) else entry_admission[:observed_opens]
    if any(not isinstance(value, str) or value not in _ADMISSION for value in admission):
        raise OpportunityLabelError("ADMISSION_STATE_INVALID")
    emissions = np.flatnonzero(signals != 0)
    selected = [int(bar) for bar in emissions if start is None or closes[int(bar)] >= start]
    prepared = features.iloc[:visible]
    # A caller's copied frame may carry a length-keyed scratch cache from a
    # different surface. Give the public API one fresh local scratch namespace;
    # share the same view for all indices without copying a frame per label.
    prepared.attrs = {}
    indexes: dict[int, _FirstTouchIndex] = {}
    rows = []
    unknown_regimes = 0
    for bar in selected:
        direction = int(signals[bar])
        raw_regime = features["REGIME_LIVE"].iat[bar]
        raw_regime = raw_regime if isinstance(raw_regime, str) else None
        regime = raw_regime if raw_regime in _REGIMES else None
        unknown_regimes += int(regime is None)
        stop_pips, target_pips = _stops(strategy, prepared, bar, direction, symbol, timeframe, regime, spec)
        identity = (SCHEMA_VERSION, provenance.strategy_id, provenance.configuration_sha256,
                    provenance.source_surface_sha256, provenance.spread_tier,
                    provenance.spread_model_sha256, provenance.admission_policy_sha256,
                    symbol, timeframe, "pip_size_binary64", pip_size.hex(),
                    _text(opens[bar]), _text(closes[bar]), direction)
        identifier = hashlib.sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=True).encode("ascii")).hexdigest()
        common = dict(opportunity_id=identifier, signal_bar=bar, signal_open_utc=_text(opens[bar]),
            signal_close_utc=_text(closes[bar]), direction=direction,
            run_start=bool(bar == 0 or signals[bar - 1] != direction),
            entry_regime=regime, regime_status="AVAILABLE" if regime else "UNAVAILABLE",
            raw_regime=raw_regime, stop_loss_pips=stop_pips, take_profit_pips=target_pips)
        entry = bar + 1
        if entry >= observed_opens:
            rows.append(SignalOpportunity(**common, status="NO_NEXT_OBSERVATION"))
            continue
        entry_price = float(open_values[entry] + direction * entry_spreads[entry])
        stop = entry_price - direction * stop_pips * pip_size
        target = entry_price + direction * target_pips * pip_size
        if not all(math.isfinite(value) for value in (entry_price, stop, target)) or direction * (entry_price - stop) <= 0 or direction * (target - entry_price) <= 0:
            raise OpportunityLabelError("STOP_PRICE_GEOMETRY_INVALID")
        common.update(entry_bar=entry, entry_open_utc=_text(opens[entry]),
            entry_admission=admission.iloc[entry] if isinstance(admission, pd.Series) else admission[entry],
            entry_price=entry_price, stop_loss_price=stop, take_profit_price=target)
        if direction not in indexes:
            indexes[direction] = _FirstTouchIndex(low - direction * exit_spreads, high - direction * exit_spreads)
        touched = indexes[direction].first(entry, stop if direction == 1 else target,
                                          target if direction == 1 else stop)
        if touched is None:
            rows.append(SignalOpportunity(**common, status="RIGHT_CENSORED"))
            continue
        quote_spread = direction * exit_spreads[touched]
        exit_price, reason, fill = _fixed_exit(direction, float(open_values[touched] - quote_spread),
            float(low[touched] - quote_spread), float(high[touched] - quote_spread), stop, target)
        delta = direction * (exit_price - entry_price)
        rows.append(SignalOpportunity(**common, status="CLOSED", exit_bar=touched,
            exit_bar_open_utc=_text(opens[touched]), exit_observed_at_utc=_text(closes[touched]),
            exit_price=float(exit_price), exit_reason=reason, exit_fill=fill,
            signed_price_delta=float(delta), signed_pips=float(delta / pip_size)))
    return OpportunityBatch(provenance=provenance, symbol=symbol, timeframe=timeframe,
        pip_size=pip_size, cutoff_utc=_text(cutoff),
        signal_start_utc=None if start is None else _text(start),
        raw_nonzero_count=len(emissions), excluded_before_start_count=len(emissions) - len(selected),
        window_nonzero_count=len(selected), unavailable_regime_count=unknown_regimes, records=tuple(rows))
