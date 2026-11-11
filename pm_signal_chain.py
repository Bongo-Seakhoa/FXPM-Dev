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


def reconcile_chain_account(chain: RegimeChain, *, backtester_result: Any,
                            expected_source_contract_sha256: str,
                            source_window_binding: Any) -> dict[str, Any]:
    """Describe same-window P75 trade correspondence without selecting a candidate.

    Candidate, source-surface and chain-start provenance remain caller-bound:
    the chain does not independently retain its starting axis or source digest.
    """
    from collections.abc import Mapping
    from datetime import datetime, timezone
    from numbers import Real

    def refuse(suffix):
        _fail("CHAIN_ACCOUNT_" + suffix)

    def digest(value):
        return (type(value) is str and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    def utc(value, suffix, *, allow_datetime=False):
        if allow_datetime and isinstance(value, datetime):
            stamp = value
        elif type(value) is str:
            if (len(value) not in {20, 25}
                    or value[4] != "-" or value[7] != "-" or value[10] != "T"
                    or value[13] != ":" or value[16] != ":"
                    or value[19:] not in {"Z", "+00:00"}
                    or not all(value[index].isascii() and value[index].isdigit()
                               for index in (0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18))):
                refuse(suffix)
            try:
                stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise SignalChainError("CHAIN_ACCOUNT_" + suffix) from exc
        else:
            refuse(suffix)
        if (stamp.tzinfo is None or str(stamp.tzinfo) != "UTC"
                or stamp.utcoffset() != timezone.utc.utcoffset(stamp)
                or stamp.microsecond != 0 or getattr(stamp, "nanosecond", 0) != 0):
            refuse(suffix)
        return stamp

    if type(chain) is not RegimeChain or not isinstance(backtester_result, Mapping):
        refuse("INPUT_INVALID")
    if (any(type(getattr(chain, field)) is not str or not getattr(chain, field)
            for field in ("symbol", "timeframe", "strategy_id", "configuration_sha256", "spread_tier"))
            or (chain.regime is not None
                and (type(chain.regime) is not str or chain.regime not in REGIMES))):
        refuse("BINDING_INVALID")
    keys = {"symbol", "timeframe", "strategy_id", "configuration_sha256", "regime",
            "spread_tier", "source_surface_sha256", "partition_source_contract_sha256",
            "partition_axis", "comparison_scope", "partition_role"}
    if not isinstance(source_window_binding, Mapping) or set(source_window_binding) != keys:
        refuse("BINDING_INVALID")
    binding = dict(source_window_binding)
    if (not digest(expected_source_contract_sha256)
            or not digest(binding["configuration_sha256"])
            or not digest(binding["source_surface_sha256"])
            or not digest(binding["partition_source_contract_sha256"])):
        refuse("BINDING_INVALID")
    if binding["partition_role"] == "final_lockbox":
        refuse("LOCKBOX_REFUSED")
    if (type(binding["partition_role"]) is not str
            or binding["partition_role"] not in {"optimizer_train", "tuning_validation"}):
        refuse("BINDING_INVALID")
    scopes = {"SAME_WINDOW_TRADE_RECONCILIATION", "TRAIN_HELDOUT_CANDIDATE_COMPARISON"}
    if type(binding["comparison_scope"]) is not str or binding["comparison_scope"] not in scopes:
        refuse("BINDING_INVALID")
    protected = datetime(2024, 7, 14, 8, 5, tzinfo=timezone.utc)
    cutoff = utc(chain.cutoff_utc, "BINDING_INVALID")
    if cutoff > protected:
        refuse("PROTECTED_CUTOFF_EXCEEDED")

    def axis(value):
        fields = {"schema_version", "timeframe", "start_utc", "end_utc", "scored_bars",
                  "warmup_bars", "event_axis_sha256", "horizon_receipt_sha256"}
        if not isinstance(value, Mapping) or set(value) != fields:
            refuse("WINDOW_MISMATCH")
        body = dict(value)
        start = utc(body["start_utc"], "WINDOW_MISMATCH")
        end = utc(body["end_utc"], "WINDOW_MISMATCH")
        if end > protected:
            refuse("PROTECTED_CUTOFF_EXCEEDED")
        if (body["schema_version"] != "fxpm.backtest-partition-axis.v1"
                or body["timeframe"] != chain.timeframe or start >= end
                or type(body["scored_bars"]) is not int or body["scored_bars"] <= 0
                or type(body["warmup_bars"]) is not int or body["warmup_bars"] < 0
                or not digest(body["event_axis_sha256"])
                or not digest(body["horizon_receipt_sha256"])):
            refuse("WINDOW_MISMATCH")
        receipt = body.pop("horizon_receipt_sha256")
        if hashlib.sha256(canonical_bytes(body)).hexdigest() != receipt:
            refuse("WINDOW_MISMATCH")
        body["horizon_receipt_sha256"] = receipt
        return body, start, end

    expected_axis, _, _ = axis(binding["partition_axis"])
    observed_axis, start, end = axis(backtester_result.get("trade_power_partition_axis"))
    if observed_axis != expected_axis:
        refuse("WINDOW_MISMATCH")
    same_window = binding["comparison_scope"] == "SAME_WINDOW_TRADE_RECONCILIATION"
    if same_window and cutoff != end:
        refuse("WINDOW_MISMATCH")
    for field in ("symbol", "timeframe", "strategy_id", "configuration_sha256", "regime", "spread_tier"):
        if binding[field] != getattr(chain, field):
            refuse("CANDIDATE_MISMATCH")
    if (any(type(binding[field]) is not str or not binding[field]
            for field in ("symbol", "timeframe", "strategy_id", "spread_tier"))
            or (chain.regime is not None and chain.regime not in REGIMES)):
        refuse("BINDING_INVALID")
    try:
        saved_binding = json.loads(canonical_bytes(binding))
    except (TypeError, ValueError) as exc:
        raise SignalChainError("CHAIN_ACCOUNT_BINDING_INVALID") from exc

    from pm_core import (build_backtest_spread_mode,
                         build_backtest_trade_identity_pnl_evidence,
                         canonical_backtest_winner_r)
    from pm_trade_power import build_trade_sample_evidence
    from pm_winner_raw_evidence import (PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
                                       build_precommitted_risk_unit_contract,
                                       build_trade_source_contract_from_backtester_result)

    spread_model = backtester_result.get("spread_model")
    attestation = backtester_result.get("numerical_execution_cost_attestation")
    if not isinstance(spread_model, Mapping) or not isinstance(attestation, Mapping):
        refuse("SOURCE_CONTRACT_INVALID")
    try:
        contract = build_trade_source_contract_from_backtester_result(backtester_result)
        mode = build_backtest_spread_mode(spread_model)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SignalChainError("CHAIN_ACCOUNT_SOURCE_CONTRACT_INVALID") from exc
    if attestation.get("spread_mode") != mode:
        refuse("SOURCE_CONTRACT_INVALID")
    sample = build_trade_sample_evidence([], source_contract=contract,
        minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS)
    identity = sample.source_contract
    if identity is None:
        refuse("SOURCE_CONTRACT_INVALID")
    if identity.source_contract_sha256 != expected_source_contract_sha256:
        refuse("SOURCE_IDENTITY_MISMATCH")
    if identity.partition_source_contract_sha256 != binding["partition_source_contract_sha256"]:
        refuse("PARTITION_IDENTITY_MISMATCH")
    tier_descriptor = spread_model.get("frozen_spread_tier")
    actual_tier = tier_descriptor.get("tier") if isinstance(tier_descriptor, Mapping) else None
    if actual_tier is not None and type(actual_tier) is not str:
        refuse("SOURCE_CONTRACT_INVALID")
    report = {"schema_version": "fxpm.chain-account-reconciliation.v1",
        "scope": "DESCRIPTIVE_DIAGNOSTIC_ONLY",
        "provenance_scope": "CALLER_BOUND_INPUTS_NOT_SOURCE_AUTHENTICATION",
        "provenance_limitations": "Expected digests and candidate/surface identities are caller assertions; the chain retains no independent starting axis or source-surface digest.",
        "spread_tier": actual_tier,
        "source_contract_sha256": identity.source_contract_sha256,
        "partition_source_contract_sha256": identity.partition_source_contract_sha256,
        "source_window_binding": saved_binding,
        "risk_unit_contract": build_precommitted_risk_unit_contract(),
        "counts": None, "closed_pairs": [], "censored_pairs": [],
        "stress_cost": {"status": "UNAVAILABLE", "reason_code": "STRESS_ON_BASE_COST_EVIDENCE_UNAVAILABLE"},
        "status": "UNAVAILABLE", "reason_code": None,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}
    if not same_window:
        report["reason_code"] = "TRAIN_HELDOUT_COMPARISON_UNAVAILABLE"
        return report
    if actual_tier != "P75" or chain.spread_tier != "P75":
        report["reason_code"] = "BASE_TIER_REQUIRED"
        return report

    raw_trades = backtester_result.get("trades")
    try:
        trades = build_backtest_trade_identity_pnl_evidence(raw_trades)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SignalChainError("CHAIN_ACCOUNT_TRADE_INVALID") from exc
    directions = {"LONG": 1, "SHORT": -1}
    reasons = {"closed_sl": "SL", "closed_tp": "TP", "end_of_data": "EOD"}
    seen = set()
    for raw, trade in zip(raw_trades, trades):
        if raw["direction"] not in directions:
            refuse("TRADE_INVALID")
        if raw["exit_reason"] not in reasons:
            refuse("EXIT_REASON_UNSUPPORTED")
        key = (trade["signal_bar"], trade["entry_bar"], directions[trade["direction"]])
        if key in seen:
            refuse("DUPLICATE_TRADE")
        seen.add(key)
        opened = utc(raw["entry_time"], "TRADE_INVALID", allow_datetime=True)
        closed = utc(raw["exit_time"], "TRADE_INVALID", allow_datetime=True)
        if not (start <= opened <= closed < end):
            refuse("WINDOW_MISMATCH")
        size = observed_axis["warmup_bars"] + observed_axis["scored_bars"]
        if (trade["entry_bar"] < observed_axis["warmup_bars"]
                or trade["signal_bar"] >= trade["entry_bar"] or trade["exit_bar"] >= size):
            refuse("TRADE_INVALID")
    if type(chain.taken) is not tuple or len(chain.taken) != len(trades):
        refuse("TRADE_SEQUENCE_MISMATCH")

    def price(value):
        if isinstance(value, bool) or not isinstance(value, Real):
            refuse("PRICE_MISMATCH")
        value = float(value)
        if not math.isfinite(value):
            refuse("PRICE_MISMATCH")
        return value

    chain_seen = set()
    previous_signal = -1
    for item, raw, trade in zip(chain.taken, raw_trades, trades):
        if (type(item) is not ChainTrade or type(item.status) is not str
                or item.status not in {"CLOSED", "RIGHT_CENSORED"}):
            refuse("TRADE_INVALID")
        if (type(item.signal_bar) is not int or type(item.entry_bar) is not int
                or type(item.direction) is not int or item.direction not in {-1, 1}):
            refuse("TRADE_INVALID")
        key = (item.signal_bar, item.entry_bar, item.direction)
        if key in chain_seen:
            refuse("DUPLICATE_TRADE")
        chain_seen.add(key)
        if item.signal_bar <= previous_signal:
            refuse("TRADE_SEQUENCE_MISMATCH")
        previous_signal = item.signal_bar
        if (key != (trade["signal_bar"], trade["entry_bar"], directions[trade["direction"]])
                or item.entry_regime not in REGIMES
                or (chain.regime is not None and item.entry_regime != chain.regime)):
            refuse("TRADE_SEQUENCE_MISMATCH")
        for field, expected in (("entry_price", item.entry_price),
                                ("stop_loss", item.stop_loss_price),
                                ("take_profit", item.take_profit_price)):
            if price(raw.get(field)) != price(expected):
                refuse("PRICE_MISMATCH")
        reason = reasons[trade["exit_reason"]]
        if item.status == "CLOSED":
            if (type(item.exit_bar) is not int or item.exit_bar != trade["exit_bar"]
                    or type(item.exit_reason) is not str
                    or item.exit_reason not in {"SL", "TP"} or item.exit_reason != reason):
                refuse("TRADE_SEQUENCE_MISMATCH")
            if price(raw.get("exit_price")) != price(item.exit_price):
                refuse("PRICE_MISMATCH")
            if isinstance(item.r_multiple, bool) or not isinstance(item.r_multiple, Real) or not math.isfinite(item.r_multiple):
                refuse("R_INVALID")
        elif (reason != "EOD" or item.exit_bar is not None or item.exit_price is not None
                or item.exit_reason is not None or item.exit_fill is not None or item.r_multiple is not None):
            refuse("EXIT_REASON_UNSUPPORTED")
        else:
            price(raw.get("exit_price"))
        try:
            account_r = canonical_backtest_winner_r(trade["pnl_dollars"], trade["risk_amount"])
        except ValueError as exc:
            raise SignalChainError("CHAIN_ACCOUNT_R_INVALID") from exc
        pair = {"opportunity_id": item.opportunity_id, "signal_bar": item.signal_bar,
            "entry_bar": item.entry_bar, "exit_bar": trade["exit_bar"], "direction": item.direction,
            "entry_time": trade["entry_time"], "exit_time": trade["exit_time"],
            "pnl_dollars": trade["pnl_dollars"], "risk_amount": trade["risk_amount"],
            "chain_price_r": None if item.status == "RIGHT_CENSORED" else float(item.r_multiple),
            "account_net_r": account_r}
        if item.status == "RIGHT_CENSORED":
            pair.update(chain_status="RIGHT_CENSORED", account_status="END_OF_DATA_LIQUIDATION")
            report["censored_pairs"].append(pair)
        else:
            report["closed_pairs"].append(pair)
    closed_count, censored_count = len(report["closed_pairs"]), len(report["censored_pairs"])
    report["counts"] = {"chain_closed": closed_count, "chain_censored": censored_count,
        "account_trades": len(trades), "closed_pairs": closed_count, "censored_pairs": censored_count}
    report["status"] = "RECONCILED" if closed_count else "UNAVAILABLE"
    report["reason_code"] = None if closed_count else "NO_CLOSED_TRADE_PAIRS"
    return report


__all__.append("reconcile_chain_account")
