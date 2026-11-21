"""Isolated V19 projection for views of an exactly compatible partition.

The caller owns one projector for one invented unit. Exec still binds and
verifies each view's source, cost precommitments, indexes and provenance.
"""

from dataclasses import replace

import numpy as np
import pandas as pd

import pm_book_multiplicity as book
import pm_signal_opportunities as opportunities
from pm_core import Backtester, InstrumentSpec
from pm_end_grid import canonical_bytes
from pm_strategies import BaseStrategy


_BASE_METHODS = ("build_trade_intent", "calculate_stops", "session_spec")


def _uses_base_stops_without_session(strategy, spec):
    cls = type(strategy)
    return (isinstance(strategy, BaseStrategy)
        and cls.__getattribute__ is object.__getattribute__
        and all(getattr(cls, name) is getattr(BaseStrategy, name) for name in _BASE_METHODS)
        and all(name not in vars(strategy) for name in _BASE_METHODS)
        and type(strategy.params) is dict
        and strategy.params.get("use_structural_sl", False) is False
        and type(spec) is InstrumentSpec)


def _compatibility_key(partition, tier, parameters, source_reference, cost_probe, inputs):
    # V19 provenance pins the full source surface, effective configuration,
    # spread arrays and admission states. Carry additionally needs the actual
    # swap binding. Keep the entire source reference when comparing views.
    strategy = inputs["strategy"]
    return (partition.geometry_receipt["receipt_sha256"], inputs["symbol"], inputs["timeframe"],
        inputs["cutoff_utc"], inputs["signal_start_utc"], tier, inputs["provenance"],
        type(strategy), canonical_bytes(strategy.params), canonical_bytes(parameters),
        canonical_bytes(source_reference), canonical_bytes(cost_probe["swap_accrual_contract"]))


def _project_batch(batch, features, masked_signals, regime, *, cutoff, start):
    closes = pd.DatetimeIndex(features["_BAR_CLOSE_TS"])
    visible = int(closes.searchsorted(cutoff, side="right"))
    signals = masked_signals.iloc[:visible].to_numpy(copy=False)
    emissions = np.flatnonzero(signals)
    selected = emissions[closes[emissions] >= start]
    rows = tuple(row for row in batch.records if row.signal_regime == regime)
    if tuple(row.signal_bar for row in rows) != tuple(selected):
        return None
    rows = tuple(replace(row, run_start=bool(row.signal_bar == 0
        or signals[row.signal_bar - 1] != row.direction)) for row in rows)
    costs = batch.cost_model
    if not any(row.eligible for row in rows):
        costs = {**costs, "swap_contract": {"status": "NOT_REQUIRED_NO_ELIGIBLE_ENTRY"}}
    return replace(batch, raw_nonzero_count=len(emissions),
        excluded_before_start_count=len(emissions) - len(selected), window_nonzero_count=len(rows),
        unavailable_regime_count=0, records=rows, cost_model=costs,
        episode_diagnostics=opportunities.independent_episode_diagnostics(rows))


class HistoricalUnitBatchProjection:
    """Build one direct batch per compatible geometry and tier, then project its views."""

    def __init__(self):
        self._batches = {}
        self.unit_evaluations = 0
        self.projected_partitions = 0
        self.direct_partitions = 0

    def project(self, *, evaluate, features, raw_signals, masked_signals, regime, partition, tier,
            parameters, view_slot, source_reference, cost_probe, evaluation_kwargs):
        inputs = evaluation_kwargs
        if (evaluate is not opportunities.evaluate_independent_signal_r
                or type(inputs["pricing"]) is not Backtester
                or not _uses_base_stops_without_session(inputs["strategy"], inputs["spec"])
                or "ATR_14" not in features or features["ATR_14"].dtype != np.dtype("float64")):
            self.direct_partitions += 1
            return None
        try:
            key = _compatibility_key(partition, tier, parameters, source_reference, cost_probe, inputs)
        except (TypeError, ValueError):
            self.direct_partitions += 1
            return None
        previous = self._batches.get(view_slot)
        if previous is not None:
            previous_key, batch = previous
            if previous_key != key:
                self.direct_partitions += 1
                return None
        else:
            supported = features["REGIME_LIVE"].isin(book.REGIME_ORDER)
            union_signals = raw_signals.where(supported, 0).astype(np.int32)
            self.unit_evaluations += 1
            try:
                batch = evaluate(features, union_signals, **inputs)
            except opportunities.OpportunityLabelError:
                # A sibling's inputs cannot change which errors the view observes.
                self.direct_partitions += 1
                return None
            self._batches[view_slot] = (key, batch)
        result = _project_batch(batch, features, masked_signals, regime,
            cutoff=inputs["cutoff_utc"], start=inputs["signal_start_utc"])
        if result is None:
            self.direct_partitions += 1
        else:
            self.projected_partitions += 1
        return result
