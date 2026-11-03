"""Non-overlapping opportunity chains: the trade list a gated single-position taker makes.

``pm_signal_opportunities`` labels every nonzero signal bar as an independent,
cash-free opportunity.  The account kernel in ``pm_core`` takes one position per
cell at a time.  This module bridges the two without an account: per regime it
walks the opportunities in bar order and keeps exactly those the kernel would
take under its own rule.

The kernel rule, from ``_backtest_loop_numba``: at bar ``i`` a new entry from
the signal at ``i - 1`` is allowed only when no position is open at the open of
``i``.  A position that exits during bar ``i`` at the stop level or the target
level blocks an entry at that same open; a position stopped out by a gap at the
open of ``i`` frees the slot for an entry at that open.  A position that never
exits inside the window holds to the end and blocks everything after it.

Chain statistics are descriptive.  The lower confidence bound uses a stated
normal approximation and is a ranking aid, never a qualification verdict.  The
all-opportunity expectancy is reported beside the chain so that an edge that
only appears when every overlapping signal is counted shows as a gap, not as
strength.  Nothing here confers selector, qualification, capital or live
authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from typing import Any, Final

from pm_signal_opportunities import OpportunityBatch, SignalOpportunity


CHAIN_SCHEMA_VERSION: Final = "fxpm.signal-chain.v1"
LCB_METHOD: Final = "NORMAL_APPROX_ONE_SIDED_Z"
STATISTICS_SCOPE: Final = "DESCRIPTIVE_DIAGNOSTIC_ONLY_NO_SELECTOR_OR_QUALIFICATION_AUTHORITY"
# The kernel's default terminal policy liquidates an open position on the last
# bar as END_OF_DATA; MARK_OPEN suppresses that close.  A chain never fabricates
# a close: a RIGHT_CENSORED trade stays open and is disclosed as such.
TERMINAL_POLICY_DISCLOSURE: Final = "RIGHT_CENSORED_LEFT_OPEN_KERNEL_LIQUIDATE_PAIRS_WITH_END_OF_DATA"
DEFAULT_Z: Final = 1.6448536269514722  # one-sided 95 percent
REGIMES: Final = ("TREND", "RANGE", "CHOP", "BREAKOUT")
SKIP_REASONS: Final = ("BLOCKED_BY_OPEN_TRADE", "NO_NEXT_OBSERVATION", "REGIME_UNAVAILABLE")


class SignalChainError(ValueError):
    """Malformed input; never a trading outcome."""


def _fail(code: str) -> None:
    raise SignalChainError(code)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


@dataclass(frozen=True, slots=True)
class ChainTrade:
    opportunity_id: str
    signal_bar: int
    entry_bar: int
    direction: int
    entry_regime: str
    run_start: bool
    status: str
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    stop_loss_pips: float
    take_profit_pips: float
    exit_bar: int | None
    exit_price: float | None
    exit_reason: str | None
    exit_fill: str | None
    signed_pips: float | None
    r_multiple: float | None


@dataclass(frozen=True, slots=True)
class SkippedOpportunity:
    opportunity_id: str
    signal_bar: int
    entry_bar: int | None
    reason: str


@dataclass(frozen=True, slots=True)
class RegimeChain:
    symbol: str
    timeframe: str
    regime: str | None
    strategy_id: str
    configuration_sha256: str
    spread_tier: str
    cutoff_utc: str
    taken: tuple[ChainTrade, ...]
    skipped: tuple[SkippedOpportunity, ...]
    chain_statistics: dict[str, Any]
    all_opportunity_statistics: dict[str, Any]
    lcb_method: str = LCB_METHOD
    statistics_scope: str = STATISTICS_SCOPE
    terminal_policy_disclosure: str = TERMINAL_POLICY_DISCLOSURE
    schema_version: str = CHAIN_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_bytes(self) -> bytes:
        return canonical_bytes(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    def trade_list(self) -> tuple[dict[str, Any], ...]:
        """The fields a kernel trade list must match, in kernel order."""
        return tuple({
            "signal_bar": trade.signal_bar, "entry_bar": trade.entry_bar,
            "exit_bar": trade.exit_bar, "direction": trade.direction,
            "entry_price": trade.entry_price, "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason, "status": trade.status,
        } for trade in self.taken)


def _check_batch(batch: Any) -> None:
    if type(batch) is not OpportunityBatch:
        _fail("BATCH_TYPE_INVALID")
    seen: set[int] = set()
    for record in batch.records:
        if type(record) is not SignalOpportunity:
            _fail("RECORD_TYPE_INVALID")
        if record.signal_bar in seen:
            _fail("DUPLICATE_SIGNAL_BAR")
        seen.add(record.signal_bar)
        if record.status == "CLOSED" and (record.exit_bar is None or record.exit_fill is None
                                          or record.signed_pips is None):
            _fail("CLOSED_RECORD_INCOMPLETE")
        if record.status in ("CLOSED", "RIGHT_CENSORED") and record.entry_bar is None:
            _fail("ENTERED_RECORD_WITHOUT_ENTRY_BAR")
        if not (record.stop_loss_pips > 0):
            _fail("STOP_PIPS_INVALID")


def _r(record: SignalOpportunity) -> float | None:
    if record.status != "CLOSED":
        return None
    return float(record.signed_pips) / float(record.stop_loss_pips)


def _trade(record: SignalOpportunity) -> ChainTrade:
    return ChainTrade(
        opportunity_id=record.opportunity_id, signal_bar=int(record.signal_bar),
        entry_bar=int(record.entry_bar), direction=int(record.direction),
        entry_regime=str(record.entry_regime), run_start=bool(record.run_start),
        status=record.status, entry_price=float(record.entry_price),
        stop_loss_price=float(record.stop_loss_price),
        take_profit_price=float(record.take_profit_price),
        stop_loss_pips=float(record.stop_loss_pips),
        take_profit_pips=float(record.take_profit_pips),
        exit_bar=None if record.exit_bar is None else int(record.exit_bar),
        exit_price=None if record.exit_price is None else float(record.exit_price),
        exit_reason=record.exit_reason, exit_fill=record.exit_fill,
        signed_pips=None if record.signed_pips is None else float(record.signed_pips),
        r_multiple=_r(record),
    )


def _r_statistics(values: list[float], z: float) -> dict[str, Any]:
    n = len(values)
    total = math.fsum(values)
    mean = total / n if n else None
    sd = None
    if n >= 2:
        sd = math.sqrt(math.fsum((v - mean) ** 2 for v in values) / (n - 1))
    lcb = None if sd is None else total - z * sd * math.sqrt(n)
    wins = sum(1 for v in values if v > 0)
    peak = running = 0.0
    worst = 0.0
    for v in values:
        running += v
        peak = max(peak, running)
        worst = min(worst, running - peak)
    return {
        "n_closed": n, "total_r": total, "mean_r": mean, "sd_r": sd,
        "lcb_total_r": lcb, "z": z, "hit_rate": (wins / n) if n else None,
        "worst_drawdown_r": worst,
        "gross_positive_r": math.fsum(v for v in values if v > 0),
        "gross_negative_r": math.fsum(v for v in values if v < 0),
    }


def build_regime_chain(batch: OpportunityBatch, regime: str | None, *,
                       z: float = DEFAULT_Z) -> RegimeChain:
    """Walk one regime's opportunities in bar order under the kernel's occupancy rule.

    ``regime=None`` chains every opportunity with an available regime label,
    which is what the ungated kernel run takes.
    """
    _check_batch(batch)
    if regime is not None and regime not in REGIMES:
        _fail("REGIME_INVALID")
    if not (isinstance(z, float) and math.isfinite(z) and z >= 0.0):
        _fail("Z_INVALID")
    records = sorted(batch.records, key=lambda item: item.signal_bar)
    taken: list[ChainTrade] = []
    skipped: list[SkippedOpportunity] = []
    all_r: list[float] = []
    all_run_start_r: list[float] = []
    open_exit_bar: int | None = None
    open_exit_fill: str | None = None
    open_censored = False
    for record in records:
        if record.regime_status != "AVAILABLE" or record.entry_regime is None:
            skipped.append(SkippedOpportunity(record.opportunity_id, record.signal_bar,
                                              record.entry_bar, "REGIME_UNAVAILABLE"))
            continue
        if regime is not None and record.entry_regime != regime:
            continue
        r = _r(record)
        if r is not None:
            all_r.append(r)
            if record.run_start:
                all_run_start_r.append(r)
        if record.status == "NO_NEXT_OBSERVATION":
            skipped.append(SkippedOpportunity(record.opportunity_id, record.signal_bar,
                                              record.entry_bar, "NO_NEXT_OBSERVATION"))
            continue
        entry = int(record.entry_bar)
        blocked = open_censored or (
            open_exit_bar is not None
            and (entry < open_exit_bar or (entry == open_exit_bar and open_exit_fill != "GAP_OPEN")))
        if blocked:
            skipped.append(SkippedOpportunity(record.opportunity_id, record.signal_bar,
                                              entry, "BLOCKED_BY_OPEN_TRADE"))
            continue
        taken.append(_trade(record))
        if record.status == "RIGHT_CENSORED":
            open_censored = True
            open_exit_bar, open_exit_fill = None, None
        else:
            open_exit_bar, open_exit_fill = int(record.exit_bar), record.exit_fill
    chain_r = [trade.r_multiple for trade in taken if trade.r_multiple is not None]
    chain_statistics = _r_statistics(chain_r, z)
    chain_statistics.update({
        "n_taken": len(taken),
        "n_censored": sum(1 for trade in taken if trade.status == "RIGHT_CENSORED"),
        "n_run_start_taken": sum(1 for trade in taken if trade.run_start),
        "n_skipped_blocked": sum(1 for item in skipped if item.reason == "BLOCKED_BY_OPEN_TRADE"),
        "n_skipped_no_next_observation": sum(1 for item in skipped
                                             if item.reason == "NO_NEXT_OBSERVATION"),
        "n_regime_unavailable": sum(1 for item in skipped if item.reason == "REGIME_UNAVAILABLE"),
    })
    all_statistics = _r_statistics(all_r, z)
    run_start_statistics = _r_statistics(all_run_start_r, z)
    all_statistics.update({
        "n_run_start_closed": run_start_statistics["n_closed"],
        "run_start_mean_r": run_start_statistics["mean_r"],
        "masking_gap_mean_r": (
            None if all_statistics["mean_r"] is None or chain_statistics["mean_r"] is None
            else all_statistics["mean_r"] - chain_statistics["mean_r"]),
    })
    provenance = batch.provenance
    return RegimeChain(
        symbol=batch.symbol, timeframe=batch.timeframe, regime=regime,
        strategy_id=provenance.strategy_id,
        configuration_sha256=provenance.configuration_sha256,
        spread_tier=provenance.spread_tier, cutoff_utc=batch.cutoff_utc,
        taken=tuple(taken), skipped=tuple(skipped),
        chain_statistics=chain_statistics, all_opportunity_statistics=all_statistics,
    )


def build_all_regime_chains(batch: OpportunityBatch, *, z: float = DEFAULT_Z) -> dict[str, RegimeChain]:
    """One chain per regime from one labelled batch: four cells from one pass."""
    return {regime: build_regime_chain(batch, regime, z=z) for regime in REGIMES}


__all__ = [
    "CHAIN_SCHEMA_VERSION", "LCB_METHOD", "DEFAULT_Z", "REGIMES", "SKIP_REASONS",
    "SignalChainError", "ChainTrade", "SkippedOpportunity", "RegimeChain",
    "build_regime_chain", "build_all_regime_chains", "canonical_bytes",
]
