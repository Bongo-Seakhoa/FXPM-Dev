"""Synchronized, shared-capital portfolio replay primitives.

The single-strategy backtester remains responsible for producing causal entry,
exit and cost outcomes.  This module consumes those outcomes as *candidates* and
replays their admission and cash flows through one account.  It deliberately
does not sum standalone strategy returns.

The engine is broker-independent and has no live-trading imports.  It cannot
place an order.  Its conservative stop-equity curve is balance less all open
initial-stop risks; its mark-to-market curve uses only mark observations carried
by accepted candidates.  Missing margin inputs fail closed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from pm_risk_admission import (
    GrossRiskLimits,
    StopRiskExposure,
    conservative_risk_capital,
    evaluate_gross_stop_risk,
    evaluate_projected_margin,
    infer_factor_groups,
)


PORTFOLIO_REPLAY_SCHEMA_VERSION = "2026-07-11-synchronized-shared-capital-v6"
PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION = "2026-07-11-funded-portfolio-evidence-v6"
PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM = "sha256-canonical-json-v1"
PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION = (
    "2026-07-11-native-final-lockbox-exact-regime-position-v4"
)
PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
PORTFOLIO_REPLAY_REQUIRED_REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS = 500
MTM_MARK_COST_CONTRACT_VERSION = "causal-calendar-rollover-accrual-v1"
CorrelationLookup = Callable[[str, str, pd.Timestamp], Optional[float]]


def canonical_json_sha256(payload: Any) -> str:
    """Hash a JSON contract deterministically (paths/mtimes are not normalized)."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def seal_portfolio_replay_evidence(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a content-sealed evidence record.

    The seal is intentionally not a signature.  Its purpose is to make partial
    edits and accidental overwrites detectable while exact input hashes make a
    previously valid replay stale as soon as the live deployment changes.
    """
    record = json.loads(json.dumps(dict(payload), default=str))
    record.pop("content_seal", None)
    record["schema_version"] = PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION
    digest = canonical_json_sha256(record)
    record["content_seal"] = {
        "algorithm": PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM,
        "sha256": digest,
    }
    return record


def portfolio_replay_evidence_seal_valid(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    seal = payload.get("content_seal")
    if not isinstance(seal, Mapping):
        return False
    if str(seal.get("algorithm") or "") != PORTFOLIO_REPLAY_EVIDENCE_SEAL_ALGORITHM:
        return False
    unsigned = dict(payload)
    unsigned.pop("content_seal", None)
    expected = canonical_json_sha256(unsigned)
    return str(seal.get("sha256") or "").lower() == expected.lower()


def write_immutable_portfolio_replay_evidence(
    output_dir: Path,
    payload: Mapping[str, Any],
) -> Path:
    """Write one content-addressed evidence file without replacing old proof."""
    sealed = seal_portfolio_replay_evidence(payload)
    digest = str(sealed["content_seal"]["sha256"])
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"portfolio_replay_evidence_{digest}.json"
    serialized = json.dumps(sealed, indent=2, sort_keys=True, ensure_ascii=False)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.write("\n")
    except FileExistsError:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != sealed:
            raise RuntimeError(f"immutable evidence path already contains different bytes: {path}")
    return path


def build_ledger_cell_roster(
    ledger: Any,
    symbols: Iterable[str],
    *,
    allowed_promotion_statuses: Iterable[str] = ("PROMOTED",),
) -> Tuple[Dict[str, Any], List[str]]:
    """Fingerprint the complete independent cell book, including NO_TRADE cells."""
    requested = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    allowed = {str(status).strip().upper() for status in allowed_promotion_statuses}
    payload_by_symbol: Dict[str, Mapping[str, Any]] = {}
    if isinstance(ledger, Mapping):
        for raw_symbol, raw_payload in ledger.items():
            if not isinstance(raw_payload, Mapping):
                continue
            symbol = str(raw_payload.get("symbol") or raw_symbol).strip().upper()
            if symbol:
                payload_by_symbol[symbol] = raw_payload

    errors: List[str] = []
    rows: List[Dict[str, Any]] = []
    expected_ids: List[str] = []
    tradeable_ids: List[str] = []
    for symbol in requested:
        symbol_payload = payload_by_symbol.get(symbol)
        if not isinstance(symbol_payload, Mapping):
            errors.append(f"LEDGER_SYMBOL_MISSING:{symbol}")
            continue
        grid = symbol_payload.get("regime_configs")
        if not isinstance(grid, Mapping):
            errors.append(f"LEDGER_CELL_GRID_MISSING:{symbol}")
            grid = {}
        for timeframe in PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES:
            timeframe_payload = grid.get(timeframe)
            if not isinstance(timeframe_payload, Mapping):
                timeframe_payload = {}
            for regime in PORTFOLIO_REPLAY_REQUIRED_REGIMES:
                cell_id = f"{symbol}|{timeframe}|{regime}"
                expected_ids.append(cell_id)
                cell = timeframe_payload.get(regime)
                if not isinstance(cell, Mapping):
                    errors.append(f"LEDGER_CELL_MISSING:{cell_id}")
                    continue
                strategy = str(cell.get("strategy_name") or "").strip()
                status = str(cell.get("promotion_status") or "BLOCKED").strip().upper()
                artifact = cell.get("artifact_meta") if isinstance(cell.get("artifact_meta"), Mapping) else {}
                no_trade = bool(artifact.get("no_trade")) or strategy.upper() == "NO_TRADE"
                tradeable = bool(strategy and not no_trade and status in allowed)
                if strategy and not no_trade and not str(cell.get("promotion_status") or "").strip():
                    errors.append(f"LEDGER_PROMOTION_STATUS_MISSING:{cell_id}")
                if tradeable:
                    tradeable_ids.append(cell_id)
                rows.append({
                    "cell_id": cell_id,
                    "strategy_name": strategy,
                    "promotion_status": status,
                    "no_trade": no_trade,
                    "tradeable": tradeable,
                    "artifact_sha256": canonical_json_sha256(artifact),
                    "cell_sha256": canonical_json_sha256(cell),
                })

    extra_symbols = sorted(set(payload_by_symbol) - set(requested))
    roster = {
        "symbols": requested,
        "required_timeframes": list(PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES),
        "required_regimes": list(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "expected_cell_count": len(requested)
        * len(PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES)
        * len(PORTFOLIO_REPLAY_REQUIRED_REGIMES),
        "expected_cell_ids": sorted(expected_ids),
        "tradeable_cell_ids": sorted(tradeable_ids),
        "rows": sorted(rows, key=lambda row: row["cell_id"]),
        "extra_ledger_symbols": extra_symbols,
    }
    roster["roster_sha256"] = canonical_json_sha256(roster["rows"])
    if len(rows) != roster["expected_cell_count"]:
        errors.append(
            f"LEDGER_CELL_COUNT_MISMATCH:{len(rows)}!={roster['expected_cell_count']}"
        )
    return roster, list(dict.fromkeys(errors))


def portfolio_cost_contract(config_payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Resolve the replay cost contract exactly as ``PipelineConfig`` does.

    Funded JSON intentionally omits several code-owned invariants.  An omitted
    key therefore means the production dataclass default, not ``False``.  Using
    permissive mapping defaults here previously disabled spread, commission and
    slippage in the synchronized replay even though both optimization and live
    execution had them enabled, making every production evidence run fail (and
    any diagnostic contract mathematically inconsistent).
    """
    from dataclasses import MISSING, fields

    from pm_core import PipelineConfig

    pipeline = config_payload.get("pipeline") if isinstance(config_payload.get("pipeline"), Mapping) else {}

    defaults: Dict[str, Any] = {}
    for item in fields(PipelineConfig):
        if item.default is not MISSING:
            defaults[item.name] = item.default

    def resolved(name: str) -> Any:
        return pipeline[name] if name in pipeline else defaults.get(name)

    return {
        "use_spread": bool(resolved("use_spread")),
        "backtest_spread_mode": str(resolved("backtest_spread_mode") or ""),
        "use_commission": bool(resolved("use_commission")),
        "use_slippage": bool(resolved("use_slippage")),
        "slippage_pips": _finite(resolved("slippage_pips"), 0.0),
        "swap_model_enabled": bool(resolved("swap_model_enabled")),
        "swap_model_min_holding_hours": _finite(
            resolved("swap_model_min_holding_hours"), 0.0
        ),
        "swap_model_triple_rollover_day": str(
            resolved("swap_model_triple_rollover_day") or ""
        ).lower(),
    }


def portfolio_policy_from_config_payload(
    config_payload: Mapping[str, Any],
    starting_balance: float,
) -> "PortfolioPolicy":
    """Build the sole funded replay policy from the live risk configuration."""
    pipeline = config_payload.get("pipeline") if isinstance(config_payload.get("pipeline"), Mapping) else {}
    position = config_payload.get("position") if isinstance(config_payload.get("position"), Mapping) else {}
    target_risk = _finite(position.get("risk_per_trade_pct"), 0.25)
    max_trade_risk = _finite(position.get("max_risk_pct"), 0.50)
    open_entry_threshold = _finite(
        pipeline.get("max_new_entries_when_total_open_risk_above_pct"), 2.0
    )
    configured_open_cap = _finite(pipeline.get("max_total_open_risk_pct"), 3.0)
    open_risk_action = str(
        pipeline.get("open_risk_action") or "pause_new_entries"
    ).strip().lower()
    if open_risk_action == "pause_new_entries":
        effective_open_cap = (
            min(configured_open_cap, open_entry_threshold)
            if open_entry_threshold > 0.0
            else configured_open_cap
        )
    elif open_risk_action == "observe":
        effective_open_cap = configured_open_cap
    elif open_risk_action == "derate":
        raise ValueError(
            "funded synchronized replay does not support open_risk_action='derate'; "
            "use a frozen hard cap or implement exact chronological derating parity"
        )
    else:
        raise ValueError(f"unsupported open_risk_action={open_risk_action!r}")
    factor_group_cap = _finite(
        pipeline.get(
            "max_factor_group_open_risk_pct",
            pipeline.get("max_combined_risk_pct"),
        ),
        0.0,
    )
    margin_entry_block_level = _finite(
        pipeline.get("margin_entry_block_level"), 200.0
    )
    if margin_entry_block_level <= 100.0:
        raise ValueError(
            "margin_entry_block_level must be greater than 100% to retain "
            "positive free-margin headroom"
        )
    max_margin_utilization_pct = 10_000.0 / margin_entry_block_level
    if not (0.0 < max_margin_utilization_pct < 100.0):
        raise ValueError("derived margin utilization must be in (0, 100)")
    allow_d1_plus_lower = bool(pipeline.get("allow_d1_plus_lower_tf", False))
    raw_symbols = config_payload.get("symbols")
    configured_symbols = {
        str(symbol).strip().upper()
        for symbol in (raw_symbols if isinstance(raw_symbols, (list, tuple, set)) else ())
        if str(symbol).strip()
    }
    try:
        target_symbol_count = max(
            0, int(pipeline.get("production_symbol_pool_target_count", 0) or 0)
        )
    except (TypeError, ValueError, OverflowError):
        target_symbol_count = 0
    # The count is only a structural impossibility bound.  It must not be a
    # hidden capital-allocation decision: live admission is governed by stop
    # risk, factor concentration, correlation, and margin.  One slot exists per
    # symbol, or two only for the explicit D1 + one-lower contract.
    structural_symbol_count = max(1, len(configured_symbols), target_symbol_count)
    structural_position_ceiling = structural_symbol_count * (
        2 if allow_d1_plus_lower else 1
    )
    return PortfolioPolicy(
        starting_balance=float(starting_balance),
        target_risk_per_trade_pct=target_risk,
        max_risk_per_trade_pct=max_trade_risk,
        max_open_risk_pct=effective_open_cap,
        max_concurrent_positions=structural_position_ceiling,
        allow_d1_plus_lower_tf=allow_d1_plus_lower,
        max_same_symbol_open_risk_pct=_finite(
            pipeline.get("max_combined_risk_pct"), 1.0
        ),
        max_factor_group_open_risk_pct=factor_group_cap,
        # Funded live has one deterministic shared factor taxonomy in
        # evaluate_gross_stop_risk(). A replay-only trailing-correlation gate
        # would reject orders that live admits and invalidate parity.
        enforce_pair_correlation=False,
        correlation_threshold=0.75,
        max_correlated_cluster_risk_pct=factor_group_cap,
        correlation_min_observations=30,
        block_unknown_correlation=False,
        max_margin_utilization_pct=max_margin_utilization_pct,
        min_margin_level_pct=margin_entry_block_level,
        min_free_margin_pct=100.0 - max_margin_utilization_pct,
        ruin_equity_pct=50.0,
        allowed_promotion_statuses=tuple(
            str(status).upper()
            for status in (pipeline.get("live_allowed_promotion_statuses") or ["PROMOTED"])
        ),
    )


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _required_finite(value: Any, field: str) -> float:
    """Parse a required evidence number without neutralizing corruption."""
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def _timestamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError(f"invalid timestamp: {value!r}")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def floor_volume(volume: float, step: float, maximum: float) -> float:
    """Floor a requested volume without ever clamping it up to broker minimum."""
    volume = _finite(volume)
    step = _finite(step, 0.01)
    maximum = _finite(maximum, 0.0)
    if volume <= 0.0 or step <= 0.0 or maximum <= 0.0:
        return 0.0
    units = math.floor((volume + (step * 1e-10)) / step)
    return round(min(maximum, units * step), 10)


@dataclass(frozen=True)
class ExitLeg:
    """One partial or final close, normalized to one original entry lot.

    ``net_pnl_per_entry_lot`` already includes the leg fraction.  For example,
    a half close that makes USD 40 per closed lot contributes USD 20 per
    original entry lot.
    """

    exit_time: Any
    fraction: float
    net_pnl_per_entry_lot: float
    price_pnl_per_entry_lot: float = 0.0
    commission_per_entry_lot: float = 0.0
    swap_per_entry_lot: float = 0.0
    exit_reason: str = ""

    def normalized(self) -> "ExitLeg":
        return ExitLeg(
            exit_time=_timestamp(self.exit_time),
            fraction=_required_finite(self.fraction, "exit leg fraction"),
            net_pnl_per_entry_lot=_required_finite(
                self.net_pnl_per_entry_lot, "exit leg net P&L"
            ),
            price_pnl_per_entry_lot=_required_finite(
                self.price_pnl_per_entry_lot, "exit leg price P&L"
            ),
            commission_per_entry_lot=_required_finite(
                self.commission_per_entry_lot, "exit leg commission"
            ),
            swap_per_entry_lot=_required_finite(
                self.swap_per_entry_lot, "exit leg swap"
            ),
            exit_reason=str(self.exit_reason or ""),
        )


@dataclass(frozen=True)
class MarkPoint:
    """Unrealized net P&L for one currently-open lot at a bar close."""

    time: Any
    pnl_per_current_lot: float

    def normalized(self) -> "MarkPoint":
        return MarkPoint(
            time=_timestamp(self.time),
            pnl_per_current_lot=_required_finite(
                self.pnl_per_current_lot, "mark P&L"
            ),
        )


@dataclass(frozen=True)
class CandidateTrade:
    candidate_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    strategy_name: str
    promotion_status: str
    asset_class: str
    risk_groups: Tuple[str, ...]
    entry_time: Any
    direction: int
    stop_risk_per_lot: float
    margin_per_lot: float
    volume_min: float
    volume_max: float
    volume_step: float
    priority_score: float
    legs: Tuple[ExitLeg, ...]
    marks: Tuple[MarkPoint, ...] = ()
    source_schema: str = ""
    margin_model: str = ""

    def normalized(self) -> "CandidateTrade":
        direction = 1 if int(self.direction) > 0 else -1
        legs = tuple(sorted((leg.normalized() for leg in self.legs), key=lambda leg: leg.exit_time))
        marks = tuple(sorted((mark.normalized() for mark in self.marks), key=lambda mark: mark.time))
        entry = _timestamp(self.entry_time)
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id is required")
        if not legs:
            raise ValueError(f"candidate {self.candidate_id} has no exit legs")
        if any(leg.exit_time < entry for leg in legs):
            raise ValueError(f"candidate {self.candidate_id} exits before entry")
        fraction_sum = sum(leg.fraction for leg in legs)
        if abs(fraction_sum - 1.0) > 1e-5:
            raise ValueError(
                f"candidate {self.candidate_id} exit fractions sum to {fraction_sum:.8f}, expected 1"
            )
        if any(leg.fraction <= 0.0 for leg in legs):
            raise ValueError(f"candidate {self.candidate_id} has a non-positive exit fraction")
        stop_risk = _required_finite(
            self.stop_risk_per_lot, "candidate stop risk per lot"
        )
        if stop_risk <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has no positive stop risk")
        margin_per_lot = _required_finite(
            self.margin_per_lot, "candidate margin per lot"
        )
        volume_min = _required_finite(self.volume_min, "candidate minimum volume")
        volume_max = _required_finite(self.volume_max, "candidate maximum volume")
        volume_step = _required_finite(self.volume_step, "candidate volume step")
        priority_score = _required_finite(
            self.priority_score, "candidate priority score"
        )
        if margin_per_lot <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has no positive margin")
        if volume_min <= 0.0 or volume_max < volume_min or volume_step <= 0.0:
            raise ValueError(f"candidate {self.candidate_id} has invalid volume bounds")
        return CandidateTrade(
            candidate_id=str(self.candidate_id),
            cell_id=str(self.cell_id),
            symbol=str(self.symbol).upper(),
            timeframe=str(self.timeframe).upper(),
            regime=str(self.regime).upper(),
            strategy_name=str(self.strategy_name),
            promotion_status=str(self.promotion_status).upper(),
            asset_class=str(self.asset_class or "unknown").lower(),
            risk_groups=infer_factor_groups(self.symbol, self.asset_class),
            entry_time=entry,
            direction=direction,
            stop_risk_per_lot=stop_risk,
            margin_per_lot=margin_per_lot,
            volume_min=volume_min,
            volume_max=volume_max,
            volume_step=volume_step,
            priority_score=priority_score,
            legs=legs,
            marks=tuple(mark for mark in marks if entry <= mark.time <= legs[-1].exit_time),
            source_schema=str(self.source_schema or ""),
            margin_model=str(self.margin_model or ""),
        )


@dataclass(frozen=True)
class PortfolioPolicy:
    starting_balance: float = 10_000.0
    target_risk_per_trade_pct: float = 0.25
    max_risk_per_trade_pct: float = 0.50
    max_open_risk_pct: float = 2.0
    max_concurrent_positions: int = 6
    # Same-symbol concurrency mirrors the funded live contract exactly: either
    # one position, or (when explicitly enabled) one D1 and one lower-TF
    # position.  It never permits two lower-TF positions or pyramiding.
    allow_d1_plus_lower_tf: bool = False
    max_same_symbol_open_risk_pct: float = 1.0
    max_factor_group_open_risk_pct: float = 1.0
    # Research-only overlay. The funded policy builder fixes this false because
    # live admission uses the shared deterministic factor-group contract.
    enforce_pair_correlation: bool = False
    correlation_threshold: float = 0.75
    max_correlated_cluster_risk_pct: float = 1.0
    correlation_min_observations: int = 30
    block_unknown_correlation: bool = False
    max_margin_utilization_pct: float = 30.0
    min_margin_level_pct: float = 300.0
    min_free_margin_pct: float = 65.0
    ruin_equity_pct: float = 50.0
    allowed_promotion_statuses: Tuple[str, ...] = ("PROMOTED",)

    def validate(self) -> None:
        if _finite(self.starting_balance) <= 0.0:
            raise ValueError("starting_balance must be positive")
        if not (0.0 < self.target_risk_per_trade_pct <= self.max_risk_per_trade_pct):
            raise ValueError("target risk must be positive and no greater than per-trade cap")
        if self.max_open_risk_pct < self.max_risk_per_trade_pct:
            raise ValueError("max open risk cannot be less than max per-trade risk")
        if self.max_concurrent_positions < 1:
            raise ValueError("max_concurrent_positions must be positive")
        if not (0.0 < self.max_same_symbol_open_risk_pct <= self.max_open_risk_pct):
            raise ValueError(
                "same-symbol open-risk cap must be positive and no greater than total open-risk cap"
            )
        if not (0.0 < self.max_factor_group_open_risk_pct <= self.max_open_risk_pct):
            raise ValueError(
                "factor-group open-risk cap must be positive and no greater than total open-risk cap"
            )
        if self.enforce_pair_correlation and not (
            0.0 < self.correlation_threshold <= 1.0
        ):
            raise ValueError("correlation_threshold must be in (0, 1]")
        if not (0.0 < self.ruin_equity_pct < 100.0):
            raise ValueError("ruin_equity_pct must be in (0, 100)")


def active_native_portfolio_contract(
    data_dir: Path,
    symbols: Iterable[str],
    *,
    timeframes: Iterable[str] = PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES,
    verify_exclusion_evidence: bool = True,
) -> Tuple[Dict[str, Any], List[str]]:
    """Validate the active manifest's complete symbol/timeframe roster.

    By default the universe resolver also rehashes authority files for every
    excluded symbol. A caller that already performed that exact verification
    in the same control flow may disable only the duplicate pass.
    """
    from pm_native_activation import (
        ACTIVE_POINTER_SCHEMA,
        ACTIVATION_MANIFEST_SCHEMA,
        resolve_active_native_universe,
        sha256_file,
    )

    requested_symbols = sorted(
        {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    )
    requested_timeframes = [
        str(timeframe).strip().upper()
        for timeframe in timeframes
        if str(timeframe).strip()
    ]
    errors: List[str] = []
    native_root = (Path(data_dir) / "native").resolve()
    pointer_path = native_root / "ACTIVE.json"
    contract: Dict[str, Any] = {
        "pointer_schema": ACTIVE_POINTER_SCHEMA,
        "manifest_schema": ACTIVATION_MANIFEST_SCHEMA,
        "symbols": requested_symbols,
        "timeframes": requested_timeframes,
    }
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return contract, [f"NATIVE_ACTIVE_POINTER_UNREADABLE:{type(exc).__name__}"]
    if not isinstance(pointer, Mapping):
        return contract, ["NATIVE_ACTIVE_POINTER_NOT_OBJECT"]
    if str(pointer.get("schema_version") or "") != ACTIVE_POINTER_SCHEMA:
        errors.append("NATIVE_ACTIVE_POINTER_SCHEMA_INVALID")
    relative = str(pointer.get("manifest_relative_path") or "").strip()
    manifest_path = (native_root / relative).resolve() if relative else native_root
    try:
        manifest_path.relative_to(native_root)
    except ValueError:
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_PATH_UNSAFE"]
    if manifest_path == native_root or not manifest_path.is_file():
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_MISSING"]
    observed_sha = sha256_file(manifest_path)
    declared_sha = str(pointer.get("manifest_sha256") or "").lower()
    if observed_sha.lower() != declared_sha:
        errors.append("NATIVE_ACTIVE_MANIFEST_SHA256_MISMATCH")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return contract, errors + [f"NATIVE_ACTIVE_MANIFEST_UNREADABLE:{type(exc).__name__}"]
    if not isinstance(manifest, Mapping):
        return contract, errors + ["NATIVE_ACTIVE_MANIFEST_NOT_OBJECT"]
    if str(manifest.get("schema_version") or "") != ACTIVATION_MANIFEST_SCHEMA:
        errors.append("NATIVE_ACTIVE_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status") or "").lower() != "active":
        errors.append("NATIVE_ACTIVE_MANIFEST_NOT_ACTIVE")
    activation_id = str(manifest.get("activation_id") or "")
    if not activation_id or activation_id != str(pointer.get("activation_id") or ""):
        errors.append("NATIVE_ACTIVE_ACTIVATION_ID_MISMATCH")
    entries = manifest.get("entries") if isinstance(manifest.get("entries"), Mapping) else {}
    universe = resolve_active_native_universe(
        Path(data_dir),
        requested_symbols,
        required_timeframes=requested_timeframes,
        verify_exclusion_evidence=verify_exclusion_evidence,
        production_contract_required=True,
    )
    errors.extend(universe.errors)
    eligible_symbols = set(universe.eligible_symbols)
    excluded_symbols = dict(universe.excluded_symbols)
    entry_contract: List[Dict[str, Any]] = []
    bars_sha_by_source: Dict[str, str] = {}
    sidecar_sha_by_source: Dict[str, str] = {}
    for symbol in requested_symbols:
        if symbol in excluded_symbols:
            exclusion = excluded_symbols[symbol]
            entry_contract.append({
                "cell_id": f"{symbol}|EXCLUDED_NO_TRADE",
                "status": "excluded_no_trade",
                "decision_basis": "source_availability_only",
                "reason_codes": list(exclusion.get("reason_codes") or []),
            })
            continue
        if symbol not in eligible_symbols:
            errors.append(f"NATIVE_UNIVERSE_SYMBOL_NOT_ELIGIBLE_OR_EXCLUDED:{symbol}")
            continue
        symbol_entries = entries.get(symbol) if isinstance(entries.get(symbol), Mapping) else {}
        for timeframe in requested_timeframes:
            cell_id = f"{symbol}|{timeframe}"
            entry = symbol_entries.get(timeframe)
            if not isinstance(entry, Mapping):
                errors.append(f"NATIVE_ACTIVE_CELL_MISSING:{cell_id}")
                continue
            status = str(entry.get("status") or "").lower()
            bars_sha = str(entry.get("bars_sha256") or "").lower()
            sidecar_sha = str(entry.get("sidecar_sha256") or "").lower()
            if status != "active":
                errors.append(f"NATIVE_ACTIVE_CELL_NOT_ACTIVE:{cell_id}")
            if str(entry.get("symbol") or "").upper() != symbol:
                errors.append(f"NATIVE_ACTIVE_CELL_SYMBOL_MISMATCH:{cell_id}")
            if str(entry.get("timeframe") or "").upper() != timeframe:
                errors.append(f"NATIVE_ACTIVE_CELL_TIMEFRAME_MISMATCH:{cell_id}")
            if len(bars_sha) != 64 or len(sidecar_sha) != 64:
                errors.append(f"NATIVE_ACTIVE_CELL_HASH_MISSING:{cell_id}")
            entry_contract.append({
                "cell_id": cell_id,
                "status": status,
                "bars_sha256": bars_sha,
                "sidecar_sha256": sidecar_sha,
            })
            bars_sha_by_source[cell_id] = bars_sha
            sidecar_sha_by_source[cell_id] = sidecar_sha
    contract.update({
        "activation_id": activation_id,
        "activation_manifest_sha256": observed_sha,
        "activation_entry_count": len(entry_contract),
        "eligible_symbols": sorted(eligible_symbols & set(requested_symbols)),
        "excluded_symbols": sorted(set(excluded_symbols) & set(requested_symbols)),
        "universe_eligibility_explicit": bool(universe.explicit_contract),
        "activation_roster_sha256": canonical_json_sha256(
            sorted(entry_contract, key=lambda row: row["cell_id"])
        ),
        "bars_sha256_by_source": dict(sorted(bars_sha_by_source.items())),
        "sidecar_sha256_by_source": dict(sorted(sidecar_sha_by_source.items())),
    })
    return contract, list(dict.fromkeys(errors))


def validate_funded_portfolio_replay_evidence(
    evidence: Any,
    *,
    expected_lineage: Mapping[str, Any],
    expected_roster: Mapping[str, Any],
    expected_cost_contract: Mapping[str, Any],
    expected_policies: Mapping[float, Mapping[str, Any]],
    minimum_calendar_days: float,
    minimum_accepted_positions: int,
    minimum_profit_factor: float,
    minimum_return_pct: float,
    maximum_drawdown_pct: float,
    require_mtm: bool = True,
    maximum_bootstrap_ruin_upper_95pct: float = 1.0,
    maximum_bootstrap_p95_drawdown_pct: Optional[float] = None,
    minimum_bootstrap_daily_return_observations: int = (
        MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS
    ),
) -> List[str]:
    """Re-evaluate a sealed PASS record against the current funded deployment."""
    errors: List[str] = []
    if not isinstance(evidence, Mapping):
        return ["PORTFOLIO_EVIDENCE_NOT_OBJECT"]
    if str(evidence.get("schema_version") or "") != PORTFOLIO_REPLAY_EVIDENCE_SCHEMA_VERSION:
        errors.append("PORTFOLIO_EVIDENCE_SCHEMA_INVALID")
    if not portfolio_replay_evidence_seal_valid(evidence):
        errors.append("PORTFOLIO_EVIDENCE_SEAL_INVALID")
    if str(evidence.get("verdict") or "").upper() != "PASS":
        errors.append("PORTFOLIO_EVIDENCE_VERDICT_NOT_PASS")
    if evidence.get("failures"):
        errors.append("PORTFOLIO_EVIDENCE_FAILURES_PRESENT")
    bootstrap_ruin_limit = _finite(
        maximum_bootstrap_ruin_upper_95pct, float("inf")
    )
    if not (0.0 <= bootstrap_ruin_limit <= 1.0):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_RUIN_POLICY_INVALID")
    minimum_bootstrap_observations = _finite(
        minimum_bootstrap_daily_return_observations, float("nan")
    )
    if not (
        math.isfinite(minimum_bootstrap_observations)
        and minimum_bootstrap_observations.is_integer()
        and minimum_bootstrap_observations
        >= MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS
    ):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_DEPTH_POLICY_INVALID")
    bootstrap_drawdown_limit = _finite(
        maximum_bootstrap_p95_drawdown_pct
        if maximum_bootstrap_p95_drawdown_pct is not None
        else maximum_drawdown_pct,
        float("inf"),
    )
    if not (
        0.0 < bootstrap_drawdown_limit <= float(maximum_drawdown_pct) <= 20.0
    ):
        errors.append("PORTFOLIO_EVIDENCE_BOOTSTRAP_DRAWDOWN_POLICY_INVALID")

    lineage = evidence.get("lineage") if isinstance(evidence.get("lineage"), Mapping) else {}
    for key, expected in expected_lineage.items():
        if lineage.get(key) != expected:
            errors.append(f"PORTFOLIO_EVIDENCE_LINEAGE_MISMATCH:{key}")

    roster = evidence.get("roster") if isinstance(evidence.get("roster"), Mapping) else {}
    for key in (
        "symbols",
        "required_timeframes",
        "required_regimes",
        "expected_cell_count",
        "expected_cell_ids",
        "tradeable_cell_ids",
        "roster_sha256",
    ):
        if roster.get(key) != expected_roster.get(key):
            errors.append(f"PORTFOLIO_EVIDENCE_ROSTER_MISMATCH:{key}")

    replay = evidence.get("replay") if isinstance(evidence.get("replay"), Mapping) else {}
    if bool(replay.get("partial_run")):
        errors.append("PORTFOLIO_EVIDENCE_PARTIAL_RUN")
    if bool(replay.get("allow_cell_errors")):
        errors.append("PORTFOLIO_EVIDENCE_ALLOWED_CELL_ERRORS")
    if replay.get("cell_generation_errors"):
        errors.append("PORTFOLIO_EVIDENCE_CELL_ERRORS_PRESENT")
    if str(replay.get("prelockbox_source_status") or "").upper() != "FROZEN_PRELOCKBOX":
        errors.append("PORTFOLIO_EVIDENCE_PRELOCKBOX_STATUS_INVALID")
    if list(replay.get("evidence_only_status_allowlist") or []) != [
        "FROZEN_PRELOCKBOX"
    ]:
        errors.append("PORTFOLIO_EVIDENCE_STATUS_ALLOWLIST_INVALID")
    if not str(replay.get("global_lockbox_access_id") or "").strip():
        errors.append("PORTFOLIO_EVIDENCE_GLOBAL_LOCKBOX_ACCESS_MISSING")
    if sorted(replay.get("replayed_cell_ids") or []) != sorted(
        expected_roster.get("tradeable_cell_ids") or []
    ):
        errors.append("PORTFOLIO_EVIDENCE_REPLAYED_CELL_ROSTER_MISMATCH")
    if str(replay.get("candidate_schema_version") or "") != PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION:
        errors.append("PORTFOLIO_EVIDENCE_CANDIDATE_SCHEMA_INVALID")
    if str(replay.get("portfolio_schema_version") or "") != PORTFOLIO_REPLAY_SCHEMA_VERSION:
        errors.append("PORTFOLIO_EVIDENCE_PORTFOLIO_SCHEMA_INVALID")
    if replay.get("cost_contract") != dict(expected_cost_contract):
        errors.append("PORTFOLIO_EVIDENCE_COST_CONTRACT_MISMATCH")
    if not all(
        bool(expected_cost_contract.get(key))
        for key in ("use_spread", "use_commission", "use_slippage", "swap_model_enabled")
    ):
        errors.append("PORTFOLIO_EVIDENCE_CURRENT_COST_CONTRACT_DISABLED")
    expected_mark_cost_contract = {
        "spread_at_mark": bool(expected_cost_contract.get("use_spread")),
        "round_trip_commission_at_mark": bool(
            expected_cost_contract.get("use_commission")
        ),
        "swap_accrued_through_mark": bool(
            expected_cost_contract.get("swap_model_enabled")
        ),
        "swap_accrual_model": MTM_MARK_COST_CONTRACT_VERSION,
    }
    if replay.get("mark_cost_contract") != expected_mark_cost_contract:
        errors.append("PORTFOLIO_EVIDENCE_MARK_COST_CONTRACT_MISMATCH")

    window = replay.get("window") if isinstance(replay.get("window"), Mapping) else {}
    if str(window.get("mode") or "") != "common_intersection":
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_NOT_COMMON")
    if str(window.get("partition") or "") != "final_lockbox":
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_NOT_FINAL_LOCKBOX")
    try:
        start = _timestamp(window.get("evaluation_start"))
        end = _timestamp(window.get("evaluation_end"))
        span_days = (end - start).total_seconds() / 86400.0
        if end <= start or span_days + 1e-9 < float(minimum_calendar_days):
            errors.append("PORTFOLIO_EVIDENCE_CALENDAR_HORIZON_TOO_SHORT")
    except Exception:
        errors.append("PORTFOLIO_EVIDENCE_WINDOW_INVALID")

    native_sources = replay.get("native_sources")
    if not isinstance(native_sources, Mapping):
        native_sources = {}
    for cell_id in expected_roster.get("tradeable_cell_ids") or []:
        source = native_sources.get(cell_id)
        if not isinstance(source, Mapping):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SOURCE_MISSING:{cell_id}")
            continue
        if str(source.get("source_kind") or "").lower() != "broker_native":
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SOURCE_INVALID:{cell_id}")
        if not bool(source.get("provenance_valid")):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_PROVENANCE_INVALID:{cell_id}")
        if str(source.get("timestamp_semantics") or "").lower() != "bar_open_utc":
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_TIMESTAMP_INVALID:{cell_id}")
        if source.get("activation_id") != expected_lineage.get("native_activation_id"):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_ACTIVATION_MISMATCH:{cell_id}")
        if source.get("activation_manifest_sha256") != expected_lineage.get(
            "native_activation_manifest_sha256"
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_MANIFEST_MISMATCH:{cell_id}")
        parts = str(cell_id).split("|")
        source_id = "|".join(parts[:2]) if len(parts) >= 2 else ""
        bars_by_source = expected_lineage.get("native_bars_sha256_by_source")
        sidecars_by_source = expected_lineage.get("native_sidecar_sha256_by_source")
        expected_bars_sha = (
            bars_by_source.get(source_id) if isinstance(bars_by_source, Mapping) else None
        )
        expected_sidecar_sha = (
            sidecars_by_source.get(source_id)
            if isinstance(sidecars_by_source, Mapping)
            else None
        )
        if source.get("source_sha256") != expected_bars_sha:
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_BARS_HASH_MISMATCH:{cell_id}")
        if source.get("sidecar_sha256") != expected_sidecar_sha:
            errors.append(f"PORTFOLIO_EVIDENCE_NATIVE_SIDECAR_HASH_MISMATCH:{cell_id}")

    scenarios = replay.get("scenarios") if isinstance(replay.get("scenarios"), list) else []
    scenario_balances = [
        _finite(row.get("starting_balance"), float("nan"))
        for row in scenarios
        if isinstance(row, Mapping)
    ]
    if (
        len(scenarios) != len(expected_policies)
        or len(scenario_balances) != len(scenarios)
        or any(not math.isfinite(value) for value in scenario_balances)
        or len(set(scenario_balances)) != len(scenario_balances)
    ):
        errors.append("PORTFOLIO_EVIDENCE_SCENARIO_ROSTER_INVALID")
    for required_balance, expected_policy in expected_policies.items():
        if tuple(expected_policy.get("allowed_promotion_statuses") or ()) != (
            "FROZEN_PRELOCKBOX",
        ):
            errors.append("PORTFOLIO_EVIDENCE_POLICY_NOT_PRELOCKBOX_ONLY")
        scenario = next(
            (
                row
                for row in scenarios
                if isinstance(row, Mapping)
                and abs(_finite(row.get("starting_balance"), -1.0) - float(required_balance)) <= 1e-6
            ),
            None,
        )
        label = f"{float(required_balance):.2f}"
        if not isinstance(scenario, Mapping):
            errors.append(f"PORTFOLIO_EVIDENCE_SCENARIO_MISSING:{label}")
            continue
        if canonical_json_sha256(scenario.get("policy")) != canonical_json_sha256(
            dict(expected_policy)
        ):
            errors.append(f"PORTFOLIO_EVIDENCE_POLICY_MISMATCH:{label}")
        summary = scenario.get("summary") if isinstance(scenario.get("summary"), Mapping) else {}
        if str(summary.get("schema_version") or "") != PORTFOLIO_REPLAY_SCHEMA_VERSION:
            errors.append(f"PORTFOLIO_EVIDENCE_SUMMARY_SCHEMA_INVALID:{label}")
        if abs(
            _finite(summary.get("starting_balance"), float("inf"))
            - float(required_balance)
        ) > 1e-6:
            errors.append(
                f"PORTFOLIO_EVIDENCE_SUMMARY_BALANCE_MISMATCH:{label}"
            )
        if int(_finite(summary.get("accepted_positions"), 0.0)) < int(minimum_accepted_positions):
            errors.append(f"PORTFOLIO_EVIDENCE_TOO_FEW_POSITIONS:{label}")
        raw_pf = summary.get("profit_factor")
        profit_factor = float("inf") if str(raw_pf).lower() == "inf" else _finite(raw_pf, 0.0)
        if profit_factor + 1e-12 < float(minimum_profit_factor):
            errors.append(f"PORTFOLIO_EVIDENCE_PROFIT_FACTOR_TOO_LOW:{label}")
        if _finite(summary.get("return_pct"), -float("inf")) + 1e-12 < float(minimum_return_pct):
            errors.append(f"PORTFOLIO_EVIDENCE_RETURN_TOO_LOW:{label}")
        if _finite(summary.get("max_mark_to_market_drawdown_pct"), float("inf")) > float(
            maximum_drawdown_pct
        ) + 1e-12:
            errors.append(f"PORTFOLIO_EVIDENCE_DRAWDOWN_TOO_HIGH:{label}")
        if _finite(
            summary.get("max_initial_stop_stress_drawdown_pct"),
            float("inf"),
        ) > float(maximum_drawdown_pct) + 1e-12:
            errors.append(
                f"PORTFOLIO_EVIDENCE_STOP_STRESS_DRAWDOWN_TOO_HIGH:{label}"
            )
        if bool(summary.get("observed_ruin")):
            errors.append(f"PORTFOLIO_EVIDENCE_OBSERVED_RUIN:{label}")
        bootstrap = summary.get("exploratory_sequence_bootstrap")
        if not isinstance(bootstrap, Mapping):
            errors.append(
                f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}"
            )
        else:
            daily_observations = _finite(
                bootstrap.get("daily_return_observations"), float("nan")
            )
            if not (
                math.isfinite(daily_observations)
                and daily_observations.is_integer()
            ):
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DAILY_OBSERVATIONS_UNAVAILABLE:{label}"
                )
            elif daily_observations < minimum_bootstrap_observations:
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DAILY_OBSERVATIONS_TOO_LOW:{label}"
                )
            ruin_interval = bootstrap.get("ruin_probability_wilson_95pct")
            ruin_lower = (
                _finite(ruin_interval[0], float("nan"))
                if isinstance(ruin_interval, (list, tuple))
                and len(ruin_interval) == 2
                else float("nan")
            )
            ruin_upper = (
                _finite(ruin_interval[1], float("nan"))
                if isinstance(ruin_interval, (list, tuple))
                and len(ruin_interval) == 2
                else float("nan")
            )
            if not (
                math.isfinite(ruin_lower)
                and math.isfinite(ruin_upper)
                and 0.0 <= ruin_lower <= ruin_upper <= 100.0
            ):
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}"
                )
            elif ruin_upper > bootstrap_ruin_limit + 1e-12:
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_RUIN_TOO_HIGH:{label}"
                )
            bootstrap_drawdown = _finite(
                bootstrap.get("p95_max_drawdown_pct"), float("nan")
            )
            if not (
                math.isfinite(bootstrap_drawdown)
                and 0.0 <= bootstrap_drawdown <= 100.0
            ):
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_UNAVAILABLE:{label}"
                )
            elif bootstrap_drawdown > bootstrap_drawdown_limit + 1e-12:
                errors.append(
                    f"PORTFOLIO_EVIDENCE_BOOTSTRAP_DRAWDOWN_TOO_HIGH:{label}"
                )
        if require_mtm and _finite(summary.get("mtm_candidate_coverage_pct"), 0.0) < 100.0 - 1e-9:
            errors.append(f"PORTFOLIO_EVIDENCE_MTM_INCOMPLETE:{label}")
        policy = expected_policy
        if _finite(
            summary.get("max_admission_total_stop_risk_pct"),
            float("inf"),
        ) > _finite(
            policy.get("max_open_risk_pct"), 0.0
        ) + 1e-9:
            errors.append(
                f"PORTFOLIO_EVIDENCE_ADMISSION_RISK_CAP_BREACH:{label}"
            )
        if _finite(
            summary.get("max_admission_factor_group_stop_risk_pct"),
            float("inf"),
        ) > _finite(
            policy.get("max_factor_group_open_risk_pct"), 0.0
        ) + 1e-9:
            errors.append(
                f"PORTFOLIO_EVIDENCE_FACTOR_RISK_CAP_BREACH:{label}"
            )
        if _finite(summary.get("max_margin_utilization_pct"), float("inf")) > _finite(
            policy.get("max_margin_utilization_pct"), 0.0
        ) + 1e-9:
            errors.append(f"PORTFOLIO_EVIDENCE_MARGIN_CAP_BREACH:{label}")
    return list(dict.fromkeys(errors))


@dataclass
class _OpenPosition:
    candidate: CandidateTrade
    volume: float
    initial_risk: float
    initial_margin: float
    remaining_fraction: float = 1.0
    last_mark_per_current_lot: float = 0.0
    realized_pnl: float = 0.0
    price_pnl: float = 0.0
    commission: float = 0.0
    swap: float = 0.0
    closed_legs: int = 0

    @property
    def open_risk(self) -> float:
        return max(0.0, self.initial_risk * self.remaining_fraction)

    @property
    def open_margin(self) -> float:
        return max(0.0, self.initial_margin * self.remaining_fraction)

    @property
    def unrealized_pnl(self) -> float:
        return self.volume * self.remaining_fraction * self.last_mark_per_current_lot


class TrailingCorrelationBook:
    """Point-in-time daily correlation lookup with no future observations."""

    def __init__(self, returns_by_symbol: Mapping[str, pd.Series], *, lookback: int = 60,
                 min_observations: int = 30):
        self.lookback = max(2, int(lookback))
        self.min_observations = max(2, int(min_observations))
        self.returns: Dict[str, pd.Series] = {}
        for symbol, raw in returns_by_symbol.items():
            series = pd.to_numeric(raw, errors="coerce").dropna().sort_index()
            index = pd.DatetimeIndex([_timestamp(value) for value in series.index])
            self.returns[str(symbol).upper()] = pd.Series(series.to_numpy(dtype=float), index=index)

    def __call__(self, left: str, right: str, asof: pd.Timestamp) -> Optional[float]:
        left_series = self.returns.get(str(left).upper())
        right_series = self.returns.get(str(right).upper())
        if left_series is None or right_series is None:
            return None
        cutoff = _timestamp(asof).normalize()
        joined = pd.concat(
            [left_series[left_series.index < cutoff], right_series[right_series.index < cutoff]],
            axis=1,
            join="inner",
        ).dropna().tail(self.lookback)
        if len(joined) < self.min_observations:
            return None
        correlation = float(joined.iloc[:, 0].corr(joined.iloc[:, 1]))
        return correlation if math.isfinite(correlation) else None


class SynchronizedPortfolioSimulator:
    """Chronological admission and cash-flow simulator for one shared account."""

    def __init__(self, policy: PortfolioPolicy,
                 correlation_lookup: Optional[CorrelationLookup] = None):
        policy.validate()
        self.policy = policy
        self.correlation_lookup = correlation_lookup

    def run(self, raw_candidates: Iterable[CandidateTrade]) -> Dict[str, Any]:
        candidates = [candidate.normalized() for candidate in raw_candidates]
        identifiers = [candidate.candidate_id for candidate in candidates]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("candidate_id values must be unique")

        allowed = {str(status).upper() for status in self.policy.allowed_promotion_statuses}
        entry_events: Dict[pd.Timestamp, List[CandidateTrade]] = defaultdict(list)
        leg_events: Dict[pd.Timestamp, List[Tuple[str, int]]] = defaultdict(list)
        mark_events: Dict[pd.Timestamp, List[Tuple[str, float]]] = defaultdict(list)
        blocked: List[Dict[str, Any]] = []
        for candidate in candidates:
            if candidate.promotion_status not in allowed:
                blocked.append(self._block(candidate, candidate.entry_time, "PROMOTION_STATUS_NOT_ALLOWED"))
                continue
            entry_events[candidate.entry_time].append(candidate)
            for leg_index, leg in enumerate(candidate.legs):
                leg_events[leg.exit_time].append((candidate.candidate_id, leg_index))
            for mark in candidate.marks:
                mark_events[mark.time].append((candidate.candidate_id, mark.pnl_per_current_lot))

        timeline = sorted(set(entry_events) | set(leg_events) | set(mark_events))
        open_positions: Dict[str, _OpenPosition] = {}
        accepted_ids: set[str] = set()
        closed_positions: List[Dict[str, Any]] = []
        path: List[Dict[str, Any]] = []
        balance = float(self.policy.starting_balance)
        peak_balance = balance
        peak_equity = balance
        peak_stop_equity = balance
        max_balance_dd = 0.0
        max_equity_dd = 0.0
        max_stop_equity_dd = 0.0
        max_open_risk_pct = 0.0
        max_conservative_open_risk_pct = 0.0
        max_admission_total_stop_risk_pct = 0.0
        max_admission_factor_group_stop_risk_pct = 0.0
        max_margin_utilization_pct = 0.0
        max_concurrent = 0
        unknown_correlation_checks = 0
        known_correlation_checks = 0
        admission_tie_batches = 0
        mark_updates = 0
        realized_at_time: Dict[pd.Timestamp, float] = defaultdict(float)

        def totals() -> Tuple[float, float, float]:
            open_risk = sum(position.open_risk for position in open_positions.values())
            open_margin = sum(position.open_margin for position in open_positions.values())
            unrealized = sum(position.unrealized_pnl for position in open_positions.values())
            return open_risk, open_margin, unrealized

        def close_leg(candidate_id: str, leg_index: int, stamp: pd.Timestamp) -> None:
            nonlocal balance
            position = open_positions.get(candidate_id)
            if position is None:
                return
            leg = position.candidate.legs[leg_index]
            cashflow = position.volume * leg.net_pnl_per_entry_lot
            position.realized_pnl += cashflow
            position.price_pnl += position.volume * leg.price_pnl_per_entry_lot
            position.commission += position.volume * leg.commission_per_entry_lot
            position.swap += position.volume * leg.swap_per_entry_lot
            position.remaining_fraction = max(0.0, position.remaining_fraction - leg.fraction)
            position.closed_legs += 1
            balance += cashflow
            realized_at_time[stamp] += cashflow
            if position.remaining_fraction <= 1e-7 or position.closed_legs >= len(position.candidate.legs):
                closed_positions.append({
                    "candidate_id": candidate_id,
                    "cell_id": position.candidate.cell_id,
                    "symbol": position.candidate.symbol,
                    "timeframe": position.candidate.timeframe,
                    "regime": position.candidate.regime,
                    "strategy_name": position.candidate.strategy_name,
                    "entry_time": position.candidate.entry_time.isoformat(),
                    "exit_time": stamp.isoformat(),
                    "direction": position.candidate.direction,
                    "volume": position.volume,
                    "actual_risk": position.initial_risk,
                    "initial_margin": position.initial_margin,
                    "net_pnl": position.realized_pnl,
                    "price_pnl": position.price_pnl,
                    "commission": position.commission,
                    "swap": position.swap,
                    "exit_legs": position.closed_legs,
                    "priority_score": position.candidate.priority_score,
                })
                del open_positions[candidate_id]

        for stamp in timeline:
            # Existing positions close before new entries at the same timestamp.
            for candidate_id, leg_index in leg_events.get(stamp, []):
                if candidate_id in open_positions and open_positions[candidate_id].candidate.entry_time < stamp:
                    close_leg(candidate_id, leg_index, stamp)

            batch = sorted(
                entry_events.get(stamp, []),
                key=lambda candidate: (-candidate.priority_score, candidate.cell_id, candidate.candidate_id),
            )
            if len(batch) > 1 and len({candidate.priority_score for candidate in batch}) < len(batch):
                admission_tie_batches += 1
            for candidate in batch:
                reason, details, correlation_counts = self._admission_reason(
                    candidate,
                    stamp,
                    balance,
                    open_positions,
                )
                known_correlation_checks += correlation_counts[0]
                unknown_correlation_checks += correlation_counts[1]
                if reason:
                    blocked.append(self._block(candidate, stamp, reason, details))
                    continue
                gross_admission = details.get("gross_stop_risk_admission")
                if isinstance(gross_admission, Mapping):
                    max_admission_total_stop_risk_pct = max(
                        max_admission_total_stop_risk_pct,
                        _finite(
                            gross_admission.get(
                                "prospective_total_stop_risk_pct"
                            )
                        ),
                    )
                    prospective_groups = gross_admission.get(
                        "prospective_candidate_group_stop_risk_pct"
                    )
                    if isinstance(prospective_groups, Mapping):
                        max_admission_factor_group_stop_risk_pct = max(
                            max_admission_factor_group_stop_risk_pct,
                            max(
                                (
                                    _finite(value)
                                    for value in prospective_groups.values()
                                ),
                                default=0.0,
                            ),
                        )
                open_risk, open_margin, unrealized = totals()
                mtm_equity = balance + unrealized
                stop_equity = balance - open_risk
                risk_capital = max(0.0, min(mtm_equity, stop_equity))
                target_risk = risk_capital * (self.policy.target_risk_per_trade_pct / 100.0)
                raw_volume = target_risk / candidate.stop_risk_per_lot
                volume = floor_volume(raw_volume, candidate.volume_step, candidate.volume_max)
                # _admission_reason already validates volume; keep this fail-closed invariant.
                if volume < candidate.volume_min or volume <= 0.0:
                    blocked.append(self._block(candidate, stamp, "MIN_LOT_EXCEEDS_RISK_BUDGET"))
                    continue
                actual_risk = candidate.stop_risk_per_lot * volume
                margin = candidate.margin_per_lot * volume
                open_positions[candidate.candidate_id] = _OpenPosition(
                    candidate=candidate,
                    volume=volume,
                    initial_risk=actual_risk,
                    initial_margin=margin,
                )
                accepted_ids.add(candidate.candidate_id)

            # Marks at the entry bar close are applied after admission.
            for candidate_id, pnl_per_lot in mark_events.get(stamp, []):
                position = open_positions.get(candidate_id)
                if position is not None:
                    position.last_mark_per_current_lot = pnl_per_lot
                    mark_updates += 1

            # Same-bar exits are admitted and then settled at the same timestamp.
            for candidate_id, leg_index in leg_events.get(stamp, []):
                position = open_positions.get(candidate_id)
                if position is not None and position.candidate.entry_time == stamp:
                    close_leg(candidate_id, leg_index, stamp)

            open_risk, open_margin, unrealized = totals()
            equity = balance + unrealized
            stop_equity = balance - open_risk
            peak_balance = max(peak_balance, balance)
            peak_equity = max(peak_equity, equity)
            peak_stop_equity = max(peak_stop_equity, stop_equity)
            max_balance_dd = max(max_balance_dd, self._drawdown(peak_balance, balance))
            max_equity_dd = max(max_equity_dd, self._drawdown(peak_equity, equity))
            max_stop_equity_dd = max(max_stop_equity_dd, self._drawdown(peak_stop_equity, stop_equity))
            open_risk_pct = open_risk / max(1e-12, equity) * 100.0
            conservative_open_risk_pct = open_risk / max(1e-12, min(equity, stop_equity)) * 100.0
            margin_utilization = open_margin / max(1e-12, equity) * 100.0
            max_open_risk_pct = max(max_open_risk_pct, open_risk_pct)
            max_conservative_open_risk_pct = max(
                max_conservative_open_risk_pct, conservative_open_risk_pct
            )
            max_margin_utilization_pct = max(max_margin_utilization_pct, margin_utilization)
            max_concurrent = max(max_concurrent, len(open_positions))
            path.append({
                "timestamp": stamp.isoformat(),
                "balance": round(balance, 8),
                "unrealized_pnl": round(unrealized, 8),
                "mtm_equity": round(equity, 8),
                "stop_equity": round(stop_equity, 8),
                "open_risk": round(open_risk, 8),
                "open_risk_pct": round(open_risk_pct, 8),
                "conservative_open_risk_pct": round(conservative_open_risk_pct, 8),
                "margin_used": round(open_margin, 8),
                "margin_utilization_pct": round(margin_utilization, 8),
                "margin_level_pct": (
                    round(equity / open_margin * 100.0, 8) if open_margin > 0.0 else None
                ),
                "free_margin": round(equity - open_margin, 8),
                "open_positions": len(open_positions),
                "realized_pnl_at_timestamp": round(realized_at_time.get(stamp, 0.0), 8),
            })

        if open_positions:
            raise RuntimeError("portfolio replay ended with open positions; every candidate must have a final exit")

        daily_returns = self._daily_equity_returns(path) if closed_positions else []
        ruin = bootstrap_ruin_probability(
            daily_returns,
            starting_balance=self.policy.starting_balance,
            ruin_equity_pct=self.policy.ruin_equity_pct,
        )
        wins = sum(1 for trade in closed_positions if trade["net_pnl"] > 0.0)
        gross_profit = sum(max(0.0, trade["net_pnl"]) for trade in closed_positions)
        gross_loss = -sum(min(0.0, trade["net_pnl"]) for trade in closed_positions)
        final_equity = path[-1]["mtm_equity"] if path else balance
        min_equity = min((row["mtm_equity"] for row in path), default=balance)
        min_stop_equity = min((row["stop_equity"] for row in path), default=balance)
        block_counts = Counter(item["reason"] for item in blocked)
        mtm_candidates = sum(1 for candidate in candidates if candidate.marks)

        summary = {
            "schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
            "capital_model": "one_shared_compounding_account",
            "portfolio_return_aggregation": "chronological_cashflows_not_summed_cell_returns",
            "starting_balance": self.policy.starting_balance,
            "final_balance": round(balance, 8),
            "final_mtm_equity": round(float(final_equity), 8),
            "net_profit": round(balance - self.policy.starting_balance, 8),
            "return_pct": round((balance / self.policy.starting_balance - 1.0) * 100.0, 8),
            "candidate_positions": len(candidates),
            "accepted_positions": len(closed_positions),
            "blocked_positions": len(blocked),
            "blocked_by_reason": dict(sorted(block_counts.items())),
            "win_rate_pct": round(wins / len(closed_positions) * 100.0, 8) if closed_positions else 0.0,
            "profit_factor": (
                round(gross_profit / gross_loss, 8) if gross_loss > 0.0
                else ("inf" if gross_profit > 0.0 else 0.0)
            ),
            "gross_profit": round(gross_profit, 8),
            "gross_loss": round(gross_loss, 8),
            "max_realized_balance_drawdown_pct": round(max_balance_dd, 8),
            "max_mark_to_market_drawdown_pct": round(max_equity_dd, 8),
            "max_initial_stop_stress_drawdown_pct": round(max_stop_equity_dd, 8),
            "minimum_mtm_equity": round(float(min_equity), 8),
            "minimum_initial_stop_equity": round(float(min_stop_equity), 8),
            "max_open_risk_pct": round(max_open_risk_pct, 8),
            "max_conservative_open_risk_pct": round(max_conservative_open_risk_pct, 8),
            "max_admission_total_stop_risk_pct": round(
                max_admission_total_stop_risk_pct, 8
            ),
            "max_admission_factor_group_stop_risk_pct": round(
                max_admission_factor_group_stop_risk_pct, 8
            ),
            "max_margin_utilization_pct": round(max_margin_utilization_pct, 8),
            "max_concurrent_positions": max_concurrent,
            "total_price_pnl": round(sum(trade["price_pnl"] for trade in closed_positions), 8),
            "total_commission": round(sum(trade["commission"] for trade in closed_positions), 8),
            "total_swap": round(sum(trade["swap"] for trade in closed_positions), 8),
            "known_correlation_checks": known_correlation_checks,
            "unknown_correlation_checks": unknown_correlation_checks,
            "admission_tie_batches": admission_tie_batches,
            "mark_updates": mark_updates,
            "candidates_with_mtm_marks": mtm_candidates,
            "mtm_candidate_coverage_pct": round(mtm_candidates / len(candidates) * 100.0, 8) if candidates else 0.0,
            "observed_mtm_ruin": bool(
                min_equity <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "observed_initial_stop_ruin": bool(
                min_stop_equity <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "observed_ruin": bool(
                min(min_equity, min_stop_equity)
                <= self.policy.starting_balance * self.policy.ruin_equity_pct / 100.0
            ),
            "exploratory_sequence_bootstrap": ruin,
            "limitations": [
                "candidate strategies and parameters remain selection-contaminated unless generated solely from an untouched lockbox",
                "offline margin is an approximation unless broker historical margin-per-lot is supplied",
                "mark-to-market uses candidate timeframe bar closes; intrabar portfolio equity can be worse",
                "initial-stop stress retains original risk until a leg closes and is conservative for trailing-stop policies",
                "spread and slippage are embedded in price P&L and are not separately identifiable from the current trade schema",
                "bootstrap ruin is a conditional sequence stress test, not a forecast or profitability guarantee",
            ],
        }
        return {
            "summary": summary,
            "policy": asdict(self.policy),
            "closed_positions": closed_positions,
            "blocked_positions": blocked,
            "path": path,
        }

    def _admission_reason(
        self,
        candidate: CandidateTrade,
        stamp: pd.Timestamp,
        balance: float,
        open_positions: Mapping[str, _OpenPosition],
    ) -> Tuple[str, Dict[str, Any], Tuple[int, int]]:
        details: Dict[str, Any] = {}
        same_symbol_positions = [
            position
            for position in open_positions.values()
            if position.candidate.symbol == candidate.symbol
        ]
        if same_symbol_positions:
            if not self.policy.allow_d1_plus_lower_tf:
                return "SYMBOL_ALREADY_OPEN", details, (0, 0)
            # The live rule permits exactly one D1 plus exactly one lower-TF
            # specialist.  A third position, two D1 positions, or two lower-TF
            # positions remain blocked even when dual-timeframe mode is on.
            if len(same_symbol_positions) >= 2:
                return "SYMBOL_POSITION_LIMIT", details, (0, 0)
            existing_is_d1 = same_symbol_positions[0].candidate.timeframe == "D1"
            candidate_is_d1 = candidate.timeframe == "D1"
            if existing_is_d1 == candidate_is_d1:
                return "SYMBOL_TIMEFRAME_PAIR_INVALID", details, (0, 0)
        if len(open_positions) >= self.policy.max_concurrent_positions:
            return "MAX_CONCURRENT_POSITIONS", details, (0, 0)

        open_risk = sum(position.open_risk for position in open_positions.values())
        open_margin = sum(position.open_margin for position in open_positions.values())
        unrealized = sum(position.unrealized_pnl for position in open_positions.values())
        equity = balance + unrealized
        risk_capital_decision = conservative_risk_capital(
            balance=balance,
            equity=equity,
            open_stop_risk_amount=open_risk,
        )
        risk_capital = risk_capital_decision.risk_capital
        if not risk_capital_decision.allow:
            return "NO_POSITIVE_RISK_CAPITAL", details, (0, 0)

        target_risk = risk_capital * self.policy.target_risk_per_trade_pct / 100.0
        raw_volume = target_risk / candidate.stop_risk_per_lot
        volume = floor_volume(raw_volume, candidate.volume_step, candidate.volume_max)
        details.update({"raw_volume": raw_volume, "floored_volume": volume, "risk_capital": risk_capital})
        if volume <= 0.0 or volume < candidate.volume_min:
            return "MIN_LOT_EXCEEDS_RISK_BUDGET", details, (0, 0)
        actual_risk = candidate.stop_risk_per_lot * volume
        actual_risk_pct = actual_risk / risk_capital * 100.0
        details.update({"actual_risk": actual_risk, "actual_risk_pct": actual_risk_pct})
        if actual_risk_pct > self.policy.max_risk_per_trade_pct + 1e-10:
            return "PER_TRADE_RISK_CAP", details, (0, 0)

        shared_risk_decision = evaluate_gross_stop_risk(
            [
                StopRiskExposure(
                    symbol=position.candidate.symbol,
                    stop_risk_pct=(position.open_risk / risk_capital * 100.0),
                    asset_class=position.candidate.asset_class,
                    exposure_id=position.candidate.candidate_id,
                )
                for position in open_positions.values()
            ],
            StopRiskExposure(
                symbol=candidate.symbol,
                stop_risk_pct=actual_risk_pct,
                asset_class=candidate.asset_class,
                exposure_id=candidate.candidate_id,
            ),
            GrossRiskLimits(
                max_total_stop_risk_pct=self.policy.max_open_risk_pct,
                max_factor_group_stop_risk_pct=self.policy.max_factor_group_open_risk_pct,
            ),
        )
        details["gross_stop_risk_admission"] = asdict(shared_risk_decision)
        if not shared_risk_decision.allow:
            return shared_risk_decision.code, details, (0, 0)

        same_symbol_risk = sum(position.open_risk for position in same_symbol_positions)
        if (
            (same_symbol_risk + actual_risk) / risk_capital * 100.0
            > self.policy.max_same_symbol_open_risk_pct + 1e-10
        ):
            details["same_symbol_open_risk"] = same_symbol_risk
            return "SAME_SYMBOL_OPEN_RISK_CAP", details, (0, 0)

        known_checks = 0
        unknown_checks = 0
        if self.policy.enforce_pair_correlation:
            correlated_risk = 0.0
            for position in open_positions.values():
                raw_correlation = None
                if self.correlation_lookup is not None:
                    raw_correlation = self.correlation_lookup(
                        candidate.symbol,
                        position.candidate.symbol,
                        stamp,
                    )
                if raw_correlation is None or not math.isfinite(
                    _finite(raw_correlation, float("nan"))
                ):
                    unknown_checks += 1
                    if self.policy.block_unknown_correlation:
                        details["correlation_symbol"] = position.candidate.symbol
                        return (
                            "CORRELATION_UNAVAILABLE",
                            details,
                            (known_checks, unknown_checks),
                        )
                    continue
                known_checks += 1
                signed_correlation = (
                    float(raw_correlation)
                    * candidate.direction
                    * position.candidate.direction
                )
                if signed_correlation >= self.policy.correlation_threshold:
                    correlated_risk += position.open_risk
            if (
                (correlated_risk + actual_risk) / risk_capital * 100.0
                > self.policy.max_correlated_cluster_risk_pct + 1e-10
            ):
                details["correlated_open_risk"] = correlated_risk
                return (
                    "CORRELATED_CLUSTER_RISK_CAP",
                    details,
                    (known_checks, unknown_checks),
                )

        margin = candidate.margin_per_lot * volume
        margin_decision = evaluate_projected_margin(
            equity=equity,
            current_used_margin=open_margin,
            candidate_required_margin=margin,
            max_margin_utilization_pct=self.policy.max_margin_utilization_pct,
        )
        details["projected_margin_admission"] = asdict(margin_decision)
        if not margin_decision.allow:
            return margin_decision.code, details, (known_checks, unknown_checks)
        return "", details, (known_checks, unknown_checks)

    @staticmethod
    def _block(candidate: CandidateTrade, stamp: pd.Timestamp, reason: str,
               details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return {
            "candidate_id": candidate.candidate_id,
            "cell_id": candidate.cell_id,
            "symbol": candidate.symbol,
            "timeframe": candidate.timeframe,
            "regime": candidate.regime,
            "strategy_name": candidate.strategy_name,
            "entry_time": _timestamp(stamp).isoformat(),
            "reason": reason,
            "details": details or {},
        }

    @staticmethod
    def _drawdown(peak: float, value: float) -> float:
        return max(0.0, (peak - value) / peak * 100.0) if peak > 0.0 else 100.0

    @staticmethod
    def _daily_equity_returns(path: Sequence[Mapping[str, Any]]) -> List[float]:
        if len(path) < 2:
            return []
        frame = pd.DataFrame(path)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame["mtm_equity"] = pd.to_numeric(frame["mtm_equity"], errors="coerce")
        frame = frame.dropna(subset=["timestamp", "mtm_equity"]).set_index("timestamp").sort_index()
        if len(frame) < 2:
            return []
        daily = frame["mtm_equity"].resample("1D").last().ffill().pct_change().dropna()
        return [float(value) for value in daily if math.isfinite(float(value))]


def bootstrap_ruin_probability(
    returns: Sequence[float],
    *,
    starting_balance: float,
    ruin_equity_pct: float = 50.0,
    simulations: int = 2_000,
    block_size: int = 5,
    seed: int = 1729,
) -> Dict[str, Any]:
    """Moving-block sequence stress; conditional diagnostic, never a forecast."""
    clean = np.asarray([_finite(value, float("nan")) for value in returns], dtype=float)
    invalid = ~np.isfinite(clean) | (clean < -1.0)
    if invalid.any():
        return {
            "status": "invalid_daily_returns",
            "daily_return_observations": int(clean.size),
            "invalid_daily_return_observations": int(invalid.sum()),
            "simulations": 0,
            "ruin_probability_pct": None,
            "ruin_probability_wilson_95pct": [None, None],
            "median_max_drawdown_pct": None,
            "p95_max_drawdown_pct": None,
        }
    if clean.size < 2 or simulations <= 0:
        return {
            "status": "insufficient_daily_returns",
            "daily_return_observations": int(clean.size),
            "simulations": 0,
            "ruin_probability_pct": None,
            "ruin_probability_wilson_95pct": [None, None],
            "median_max_drawdown_pct": None,
            "p95_max_drawdown_pct": None,
        }
    simulations = int(simulations)
    block_size = max(1, min(int(block_size), int(clean.size)))
    rng = np.random.default_rng(int(seed))
    ruin_threshold = float(starting_balance) * float(ruin_equity_pct) / 100.0
    ruin_count = 0
    max_drawdowns = np.empty(simulations, dtype=float)
    max_start = max(1, int(clean.size) - block_size + 1)
    for simulation in range(simulations):
        sampled: List[float] = []
        while len(sampled) < clean.size:
            start = int(rng.integers(0, max_start))
            sampled.extend(clean[start:start + block_size].tolist())
        equity = float(starting_balance)
        peak = equity
        max_drawdown = 0.0
        ruined = False
        for value in sampled[:clean.size]:
            equity *= max(0.0, 1.0 + float(value))
            peak = max(peak, equity)
            if peak > 0.0:
                max_drawdown = max(max_drawdown, (peak - equity) / peak * 100.0)
            if equity <= ruin_threshold:
                ruined = True
        ruin_count += int(ruined)
        max_drawdowns[simulation] = max_drawdown
    probability = ruin_count / simulations
    z = 1.959963984540054
    denominator = 1.0 + z * z / simulations
    centre = (probability + z * z / (2.0 * simulations)) / denominator
    half_width = z * math.sqrt(
        probability * (1.0 - probability) / simulations + z * z / (4.0 * simulations * simulations)
    ) / denominator
    return {
        "status": "exploratory_conditional_sequence_stress",
        "daily_return_observations": int(clean.size),
        "simulations": simulations,
        "block_size_days": block_size,
        "seed": int(seed),
        "ruin_threshold_pct_of_start": float(ruin_equity_pct),
        "ruin_probability_pct": round(probability * 100.0, 6),
        "ruin_probability_wilson_95pct": [
            round(max(0.0, centre - half_width) * 100.0, 6),
            round(min(1.0, centre + half_width) * 100.0, 6),
        ],
        "median_max_drawdown_pct": round(float(np.quantile(max_drawdowns, 0.50)), 6),
        "p95_max_drawdown_pct": round(float(np.quantile(max_drawdowns, 0.95)), 6),
    }


def infer_risk_groups(symbol: str, asset_class: str) -> Tuple[str, ...]:
    """Compatibility alias for the shared funded/live factor taxonomy."""
    return infer_factor_groups(symbol, asset_class)
