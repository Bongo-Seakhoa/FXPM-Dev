"""Institutional synchronized replay of promoted FXPM cells.

This command is offline and read-only with respect to production inputs.  It
regenerates exact-regime candidate positions through the canonical backtester,
then re-sizes and admits those candidates through one shared account in strict
timestamp order.  It never imports MetaTrader and cannot place trades.

Unlike the legacy promoted-cell report, this tool does not add independently
compounded cell P&L.  It applies one equity balance, broker minimum volumes,
portfolio risk/concentration/correlation gates, approximate offline margin, and
the selected live governance mode.  Outputs are evidence artifacts, not a
profitability guarantee.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

PATCH_ROOT = Path(__file__).resolve().parents[3]
REPO = Path(os.environ.get("FXPM_REPO", PATCH_ROOT)).resolve()
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(PATCH_ROOT))

from pm_core import (  # noqa: E402
    Backtester,
    DataLoader,
    FeatureComputer,
    PipelineConfig,
    _create_spec_from_broker_data,
    strict_pipeline_config_kwargs,
)
from pm_portfolio_replay import (  # noqa: E402
    MTM_MARK_COST_CONTRACT_VERSION,
    PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
    PORTFOLIO_REPLAY_SCHEMA_VERSION,
    CandidateTrade,
    ExitLeg,
    MarkPoint,
    SynchronizedPortfolioSimulator,
    active_native_portfolio_contract,
    build_ledger_cell_roster,
    canonical_json_sha256,
    portfolio_cost_contract,
    portfolio_mark_cost_contract,
    seal_portfolio_replay_evidence,
    validate_funded_portfolio_replay_evidence,
    write_immutable_portfolio_replay_evidence,
)
from pm_risk_admission import infer_factor_groups  # noqa: E402
from pm_cap_calibration import (  # noqa: E402
    CapCalibrationConstraints,
    calibrate_total_stop_risk_cap,
    cap_neutral_config_sha256,
    cap_neutral_policy_contract_sha256,
    validate_cap_calibration_artifact,
    write_immutable_cap_calibration,
)
from pm_strategies import StrategyRegistry  # noqa: E402
from pm_two_phase_lockbox import (  # noqa: E402
    consume_global_final_lockbox_once,
    critical_module_code_contract,
    effective_replay_priority_contract,
    iter_frozen_tradeable_cells,
    load_pinned_prelockbox_roster,
    materialize_joint_pass_ledger,
    prelockbox_evidence_policy_from_config_payload,
    sha256_file as prelockbox_sha256_file,
    validate_prelockbox_roster_artifact,
    write_immutable_released_ledger,
)
from agent_audit.performance.scripts.run_promoted_cell_replay import gate_signals_to_cell_regime  # noqa: E402


CANDIDATE_SCHEMA_VERSION = PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION
OFFLINE_MARGIN_MODEL = "offline_notional_asset_rate_buffered_v1"
DEFAULT_MARGIN_RATES = {
    "fx": 0.05,
    "index": 0.10,
    "metal": 0.10,
    "commodity": 0.10,
    "equity": 0.20,
    "crypto": 0.50,
    "unknown": 1.00,
}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _required_trade_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _stamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError(f"invalid timestamp {value!r}")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pipeline_config(config_path: Path, starting_balance: float = 10_000.0) -> Tuple[PipelineConfig, Dict[str, Any]]:
    payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    pipe = dict(payload.get("pipeline") or {})
    kwargs = strict_pipeline_config_kwargs(pipe)
    kwargs["data_dir"] = str((config_path.parent / str(kwargs.get("data_dir", "data"))).resolve())
    regime_path = Path(str(kwargs.get("regime_params_file", "regime_params.json")))
    if not regime_path.is_absolute():
        regime_path = config_path.parent / regime_path
    kwargs["regime_params_file"] = str(regime_path.resolve())
    config = PipelineConfig(**kwargs)
    config.initial_capital = float(starting_balance)
    config._deployment_config_path = str(config_path.resolve())
    return config, payload


def resolve_instrument_spec(symbol: str, broker_specs: Mapping[str, Any]) -> Tuple[Any, str]:
    """Resolve broker specs case-insensitively and reject silent generic fallbacks."""
    target = str(symbol).upper()
    matches = [key for key in broker_specs if str(key).upper() == target]
    if len(matches) != 1:
        raise ValueError(f"broker_spec_not_unique_or_missing:{symbol}:matches={matches}")
    key = str(matches[0])
    spec = _create_spec_from_broker_data(key, dict(broker_specs[matches[0]]))
    required = {
        "tick_size": _finite(getattr(spec, "tick_size", 0.0)),
        "tick_value": _finite(getattr(spec, "tick_value", 0.0)),
        "volume_min": _finite(getattr(spec, "min_lot", 0.0)),
        "volume_step": _finite(getattr(spec, "volume_step", 0.0)),
        "volume_max": _finite(getattr(spec, "max_lot", 0.0)),
        "contract_size": _finite(getattr(spec, "contract_size", 0.0)),
    }
    invalid = [name for name, value in required.items() if value <= 0.0]
    if invalid:
        raise ValueError(f"broker_spec_incomplete:{symbol}:{','.join(invalid)}")
    return spec, key


def margin_at_entry_per_lot(symbol: str, entry_price: float, spec: Any) -> Tuple[float, Dict[str, Any]]:
    asset_class = str(getattr(spec, "asset_class", "unknown") or "unknown").lower()
    contract_size = _finite(getattr(spec, "contract_size", 0.0))
    entry_price = abs(_finite(entry_price))
    rate = DEFAULT_MARGIN_RATES.get(asset_class, DEFAULT_MARGIN_RATES["unknown"])
    if contract_size <= 0.0 or entry_price <= 0.0:
        return float("nan"), {"status": "unavailable", "reason": "notional_inputs"}
    symbol_upper = str(symbol).upper()
    conversion_assumption = "quote_currency_treated_as_usd_with_1.25_buffer"
    if asset_class == "fx" and len(symbol_upper) >= 6:
        base, quote = symbol_upper[:3], symbol_upper[3:6]
        if quote == "USD":
            notional_usd = contract_size * entry_price
            conversion_assumption = "usd_quote_exact_price_notional"
        elif base == "USD":
            notional_usd = contract_size
            conversion_assumption = "usd_base_contract_notional"
        else:
            notional_usd = contract_size * 1.50
            conversion_assumption = "fx_cross_base_usd_proxy_1.50"
    else:
        notional_usd = contract_size * entry_price * 1.25
    margin = notional_usd * rate
    return margin, {
        "status": "approximate_offline",
        "model": OFFLINE_MARGIN_MODEL,
        "asset_class": asset_class,
        "notional_usd_proxy": notional_usd,
        "margin_rate": rate,
        "conversion_assumption": conversion_assumption,
    }


def _priority_score(cell: Mapping[str, Any]) -> float:
    return float(effective_replay_priority_contract(cell)["value"])


def _candidate_groups_from_trades(
    trades: Sequence[Mapping[str, Any]],
) -> List[List[Mapping[str, Any]]]:
    grouped: Dict[Tuple[Any, ...], List[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (
            int(trade.get("entry_bar", -1)),
            int(trade.get("signal_bar", -1)),
            _stamp(trade.get("entry_time")),
            str(trade.get("direction", "")).upper(),
            round(_finite(trade.get("entry_price")), 10),
        )
        grouped[key].append(trade)
    return [sorted(group, key=lambda trade: _stamp(trade.get("exit_time"))) for _, group in sorted(
        grouped.items(), key=lambda item: (item[0][2], item[0][0], item[0][1])
    )]


def _marks_for_position(
    features: pd.DataFrame,
    half_spread: np.ndarray,
    spec: Any,
    entry_time: pd.Timestamp,
    final_exit_time: pd.Timestamp,
    entry_price: float,
    direction: int,
    backtester: Backtester,
    timeframe: str,
    *,
    valuation_price_field: str = "Close",
) -> Tuple[MarkPoint, ...]:
    mask = (features.index >= entry_time) & (features.index <= final_exit_time)
    indices = np.flatnonzero(np.asarray(mask))
    marks: List[MarkPoint] = []
    price_field = str(valuation_price_field or "").strip()
    if price_field not in features.columns:
        raise ValueError(
            f"missing {price_field or 'valuation'} price in required MTM mark stream"
        )
    observed_price = pd.to_numeric(
        features[price_field], errors="coerce"
    ).to_numpy(dtype=float)
    for index in indices:
        if not math.isfinite(observed_price[index]):
            raise ValueError("nonfinite price in required MTM mark stream")
        spread = float(half_spread[index]) if index < len(half_spread) else 0.0
        mark_exit = (
            observed_price[index] - spread
            if direction > 0
            else observed_price[index] + spread
        )
        pnl = spec.calculate_tick_profit(entry_price, mark_exit, 1.0, direction)
        if bool(getattr(backtester.config, "swap_model_enabled", True)):
            mark_time = features.index[index]
            rollover_units = backtester._swap_rollover_units(
                {
                    "entry_time": entry_time,
                    "exit_time": mark_time,
                    "direction": "LONG" if direction > 0 else "SHORT",
                },
                features,
                timeframe,
                spec,
            )
            raw_swap = (
                getattr(spec, "swap_long", 0.0)
                if direction > 0
                else getattr(spec, "swap_short", 0.0)
            )
            pnl += (
                backtester._swap_value_per_lot_dollars(
                    spec, raw_swap, entry_price=entry_price
                )
                * rollover_units
            )
        try:
            pnl_value = float(pnl)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("nonfinite P&L in required MTM mark stream") from exc
        if not math.isfinite(pnl_value):
            raise ValueError("nonfinite P&L in required MTM mark stream")
        effective_time = features.index[index]
        marks.append(
            MarkPoint(
                effective_time,
                pnl_value,
                observation_time=effective_time,
            )
        )
    return tuple(marks)


def _half_open_evaluation_frame(
    frame: pd.DataFrame,
    evaluation_end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """Return only bars inside the exclusive evaluation end boundary."""
    if evaluation_end is None:
        return frame.copy()
    return frame.loc[frame.index < evaluation_end].copy()


def replay_cell_candidates(
    cfg: PipelineConfig,
    cell: Mapping[str, Any],
    native_frame: pd.DataFrame,
    spec: Any,
    spec_key: str,
    *,
    evaluation_start: Optional[pd.Timestamp],
    evaluation_end: Optional[pd.Timestamp],
    apply_governance: bool,
    include_marks: bool,
    valuation_frame: Optional[pd.DataFrame] = None,
) -> Tuple[List[CandidateTrade], Dict[str, Any]]:
    symbol = str(cell["symbol"]).upper()
    timeframe = str(cell["timeframe"]).upper()
    regime = str(cell.get("regime") or "SYMBOL_LEVEL").upper()
    strategy_name = str(cell.get("strategy_name") or "")
    frame = native_frame.copy()
    if frame is None or len(frame) < 200:
        return [], {"cell": f"{symbol}|{timeframe}|{regime}", "error": "insufficient_bars"}
    frame = _half_open_evaluation_frame(frame, evaluation_end)
    if len(frame) < 200:
        return [], {"cell": f"{symbol}|{timeframe}|{regime}", "error": "insufficient_window_bars"}
    strategy = StrategyRegistry.get(strategy_name, **dict(cell.get("parameters") or {}))
    features = FeatureComputer.compute_all(
        frame,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=cfg.regime_params_file,
    )
    signals = strategy.generate_signals(features, symbol)
    signals, regime_column, matching_bars = gate_signals_to_cell_regime(signals, features, regime)
    if evaluation_start is not None:
        signals.loc[signals.index < evaluation_start] = 0
    if evaluation_end is not None:
        signals.loc[signals.index >= evaluation_end] = 0
    if int(signals.abs().sum()) == 0:
        return [], {
            "cell": f"{symbol}|{timeframe}|{regime}",
            "error": "no_signals_in_evaluation_window",
            "regime_column": regime_column,
            "regime_matching_bars": matching_bars,
        }

    backtester = Backtester(cfg)
    governance = cell.get("governance_policy") if apply_governance else None
    result = backtester.run(
        features,
        signals,
        symbol,
        strategy,
        spec=spec,
        timeframe=timeframe,
        governance_policy=governance,
    )
    trades = result.get("trades") or []
    if not trades:
        return [], {
            "cell": f"{symbol}|{timeframe}|{regime}",
            "error": "no_closed_candidate_positions",
            "regime_column": regime_column,
            "regime_matching_bars": matching_bars,
        }
    _, _strategy_half_spread, spread_meta = backtester._build_half_spread_arrays(
        features, spec
    )
    mark_frame: Optional[pd.DataFrame] = None
    mark_half_spread: Optional[np.ndarray] = None
    mark_spread_meta: Dict[str, Any] = {}
    if include_marks:
        if valuation_frame is None or valuation_frame.empty:
            raise ValueError(
                "causal M5 valuation frame is required when MTM marks are enabled"
            )
        mark_frame = _half_open_evaluation_frame(
            valuation_frame, evaluation_end
        )
        if "Open" not in mark_frame.columns:
            raise ValueError("causal M5 valuation frame has no observable Open")
        if not mark_frame.index.is_monotonic_increasing:
            raise ValueError("causal M5 valuation frame is not time ordered")
        _, mark_half_spread, mark_spread_meta = (
            backtester._build_half_spread_arrays(mark_frame, spec)
        )
    cell_id = f"{symbol}|{timeframe}|{regime}|{strategy_name}"
    candidates: List[CandidateTrade] = []
    group_errors: List[str] = []
    for group_index, group in enumerate(_candidate_groups_from_trades(trades)):
        try:
            for trade in group:
                for field in (
                    "position_size",
                    "entry_price",
                    "exit_price",
                    "pnl_dollars",
                    "entry_to_stop_pips",
                ):
                    _required_trade_number(trade.get(field), field)
                if trade.get("swap_dollars") not in (None, ""):
                    _required_trade_number(
                        trade.get("swap_dollars"), "swap_dollars"
                    )
        except ValueError:
            group_errors.append(f"group_{group_index}:nonfinite_trade_evidence")
            continue
        total_source_volume = sum(_finite(trade.get("position_size")) for trade in group)
        if total_source_volume <= 0.0:
            group_errors.append(f"group_{group_index}:no_source_volume")
            continue
        first = group[0]
        entry_time = _stamp(first.get("entry_time"))
        final_exit = max(_stamp(trade.get("exit_time")) for trade in group)
        if evaluation_start is not None and entry_time < evaluation_start:
            group_errors.append(f"group_{group_index}:entry_before_evaluation_start")
            continue
        if evaluation_end is not None and (
            entry_time >= evaluation_end or final_exit >= evaluation_end
        ):
            group_errors.append(f"group_{group_index}:outside_half_open_evaluation_window")
            continue
        entry_price = _finite(first.get("entry_price"))
        direction = 1 if str(first.get("direction", "")).upper() in {"LONG", "BUY", "1"} else -1
        entry_to_stop_pips = max(_finite(trade.get("entry_to_stop_pips")) for trade in group)
        if entry_to_stop_pips <= 0.0:
            group_errors.append(f"group_{group_index}:missing_initial_stop_distance")
            continue
        stop_distance = entry_to_stop_pips * _finite(getattr(spec, "pip_size", 0.0))
        synthetic_stop = entry_price - stop_distance if direction > 0 else entry_price + stop_distance
        stop_risk_per_lot = _finite(
            spec.calculate_loss_at_stop(
                entry_price,
                synthetic_stop,
                1.0,
                direction,
            ),
            float("nan"),
        )
        if not math.isfinite(_finite(stop_risk_per_lot, float("nan"))) or stop_risk_per_lot <= 0.0:
            group_errors.append(f"group_{group_index}:invalid_stop_risk")
            continue
        margin_per_lot, margin_meta = margin_at_entry_per_lot(symbol, entry_price, spec)
        legs: List[ExitLeg] = []
        for trade in group:
            leg_volume = _finite(trade.get("position_size"))
            fraction = leg_volume / total_source_volume
            price_pnl = spec.calculate_tick_profit(
                _finite(trade.get("entry_price")),
                _finite(trade.get("exit_price")),
                leg_volume,
                direction,
            )
            try:
                price_pnl = _required_trade_number(
                    price_pnl, "price_pnl_per_entry_lot"
                )
            except ValueError:
                group_errors.append(
                    f"group_{group_index}:nonfinite_trade_evidence"
                )
                legs = []
                break
            swap = _finite(trade.get("swap_dollars"))
            legs.append(ExitLeg(
                exit_time=_stamp(trade.get("exit_time")),
                fraction=fraction,
                net_pnl_per_entry_lot=_finite(trade.get("pnl_dollars")) / total_source_volume,
                price_pnl_per_entry_lot=price_pnl / total_source_volume,
                swap_per_entry_lot=swap / total_source_volume,
                exit_reason=str(trade.get("exit_reason") or ""),
            ))
        if not legs:
            continue
        marks = (
            _marks_for_position(
                mark_frame,
                mark_half_spread,
                spec,
                entry_time,
                final_exit,
                entry_price,
                direction,
                backtester,
                "M5",
                valuation_price_field="Open",
            )
            if include_marks else ()
        )
        candidates.append(CandidateTrade(
            candidate_id=f"{cell_id}|{group_index:06d}|{entry_time.isoformat()}",
            cell_id=cell_id,
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_name=strategy_name,
            promotion_status=str(cell.get("promotion_status") or "BLOCKED").upper(),
            asset_class=str(getattr(spec, "asset_class", "unknown") or "unknown").lower(),
            risk_groups=infer_factor_groups(
                symbol, getattr(spec, "asset_class", "unknown")
            ),
            entry_time=entry_time,
            direction=direction,
            stop_risk_per_lot=stop_risk_per_lot,
            margin_per_lot=margin_per_lot,
            volume_min=_finite(getattr(spec, "min_lot", 0.01), 0.01),
            volume_max=_finite(getattr(spec, "max_lot", 0.0)),
            volume_step=_finite(getattr(spec, "volume_step", 0.01), 0.01),
            priority_score=_priority_score(cell),
            legs=tuple(legs),
            marks=marks,
            source_schema=CANDIDATE_SCHEMA_VERSION,
            margin_model=OFFLINE_MARGIN_MODEL,
        ))
    return candidates, {
        "cell": cell_id,
        "error": "" if candidates else "all_candidate_groups_invalid",
        "group_errors": group_errors,
        "candidate_positions": len(candidates),
        "backtest_trade_legs": len(trades),
        "governance_applied": apply_governance,
        "selected_governance_policy": (cell.get("governance_policy") or {}).get("selected_policy")
        if isinstance(cell.get("governance_policy"), Mapping) else None,
        "regime_column": regime_column,
        "valuation_tape_contract": (
            MTM_MARK_COST_CONTRACT_VERSION if include_marks else "disabled"
        ),
        "valuation_timeframe": "M5" if include_marks else "",
        "valuation_price_field": "Open" if include_marks else "",
        "valuation_spread_meta": mark_spread_meta,
        "regime_matching_bars": matching_bars,
        "broker_spec_key": spec_key,
        "spread_model": spread_meta,
        "margin_model_example": margin_meta if candidates else {},
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields_order: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields_order:
                fields_order.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields_order, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return _stamp(value).isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _native_replay_frame(
    loader: DataLoader,
    symbol: str,
    timeframe: str,
    *,
    partition: str = "tuning_validation",
    global_lockbox_access_id: Optional[str] = None,
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], str]:
    """Load one exact active native source and retain its evidence lineage."""
    if (
        str(partition or "").strip().lower() == "final_lockbox"
        and not str(global_lockbox_access_id or "").strip()
    ):
        # Keep the burn-before-read rule local to the only helper that opens a
        # price-bearing native source.  A future caller cannot accidentally
        # bypass the ordering established in ``main``.
        return None, {}, "global_final_lockbox_access_not_registered"
    frame, raw_meta = loader.load_native_timeframe(symbol, timeframe)
    meta = raw_meta if isinstance(raw_meta, Mapping) else {}
    activation_entry = (
        meta.get("activation_entry")
        if isinstance(meta.get("activation_entry"), Mapping)
        else {}
    )
    report = {
        "symbol": str(meta.get("symbol") or symbol).upper(),
        "timeframe": str(meta.get("timeframe") or timeframe).upper(),
        "source_kind": str(meta.get("source_kind") or ""),
        "source_sha256": str(meta.get("sha256") or ""),
        "sidecar_sha256": str(activation_entry.get("sidecar_sha256") or ""),
        "provenance_valid": bool(meta.get("provenance_valid", False)),
        "provenance_errors": [str(value) for value in (meta.get("provenance_errors") or [])],
        "timestamp_semantics": str(meta.get("timestamp_semantics") or ""),
        "activation_id": str(meta.get("activation_id") or ""),
        "activation_manifest_sha256": str(meta.get("activation_manifest_sha256") or ""),
        "common_calendar_boundaries": meta.get("common_calendar_boundaries"),
        "rows": int(meta.get("rows") or 0),
    }
    if frame is None or len(frame) == 0:
        return None, report, "native_activation_unavailable"
    if report["source_kind"].lower() != "broker_native":
        return None, report, "native_source_not_broker_native"
    if str(meta.get("symbol") or "").upper() != str(symbol).upper():
        return None, report, "native_source_symbol_mismatch"
    if str(meta.get("timeframe") or "").upper() != str(timeframe).upper():
        return None, report, "native_source_timeframe_mismatch"
    if not report["provenance_valid"]:
        return None, report, "native_source_provenance_invalid"
    if report["timestamp_semantics"].lower() != "bar_open_utc":
        return None, report, "native_timestamp_semantics_invalid"
    if (
        not report["activation_id"]
        or len(report["activation_manifest_sha256"]) != 64
        or len(report["source_sha256"]) != 64
        or len(report["sidecar_sha256"]) != 64
        or str(activation_entry.get("status") or "").lower() != "active"
        or str(activation_entry.get("bars_sha256") or "").lower()
        != report["source_sha256"].lower()
    ):
        return None, report, "native_activation_unpinned"
    ordered = frame.copy()
    ordered.index = pd.DatetimeIndex([_stamp(value) for value in ordered.index])
    return ordered.sort_index(), report, ""


def _partition_pair(
    source_report: Mapping[str, Any],
    partition: str,
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    boundaries = source_report.get("common_calendar_boundaries")
    if not isinstance(boundaries, Mapping):
        return None
    raw = boundaries.get(str(partition))
    if isinstance(raw, Mapping):
        start_raw, end_raw = raw.get("start_ts"), raw.get("end_ts")
    elif isinstance(raw, (list, tuple)) and len(raw) == 2:
        start_raw, end_raw = raw
    else:
        return None
    try:
        start, end = _stamp(start_raw), _stamp(end_raw)
    except Exception:
        return None
    return (start, end) if end > start else None


def build_policy(config_payload: Mapping[str, Any], starting_balance: float) -> Any:
    """Build the frozen-roster evidence policy (never a live status policy)."""
    return prelockbox_evidence_policy_from_config_payload(
        config_payload, starting_balance
    )


def resolved_live_governance_mode(config: PipelineConfig) -> str:
    """Use the same resolved code-owned governance default as funded live."""
    return str(getattr(config, "local_governance_live_mode", "off") or "off").lower()


def main() -> int:
    # Production evidence accepts only explicit option names.  Argparse's
    # default prefix matching would otherwise resurrect removed switches (for
    # example ``--start`` as an abbreviation for ``--starting-balances``).
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", default=str(REPO / "config.json"))
    parser.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    parser.add_argument("--broker-specs", default=str(REPO / "broker_specs.json"))
    parser.add_argument("--out-dir", default=str(REPO / "agent_audit" / "performance" / "data" / "synchronized_portfolio_replay"))
    parser.add_argument("--starting-balances", default="1000,10000")
    parser.add_argument(
        "--partition",
        choices=("final_lockbox", "tuning_validation"),
        default="final_lockbox",
        help="tuning_validation calibrates and freezes the cap; final_lockbox only validates it",
    )
    args = parser.parse_args()

    raise SystemExit(
        "CAPITAL_AUTHORITY_UNAVAILABLE: legacy V7 replay cannot produce V8 capital "
        "authority; use pm_cap_replay_producer with signed precommitment inputs"
    )

    config_path = Path(args.config).resolve()
    ledger_path = Path(args.ledger).resolve()
    broker_path = Path(args.broker_specs).resolve()
    output_dir = Path(args.out_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg, config_payload = _pipeline_config(config_path)
    broker_specs = json.loads(broker_path.read_text(encoding="utf-8-sig"))
    pipeline_payload = config_payload.get("pipeline") or {}
    cost_contract = portfolio_cost_contract(config_payload)
    balances = sorted({
        float(item.strip())
        for item in args.starting_balances.split(",")
        if item.strip() and _finite(item.strip()) > 0.0
    })
    required_balances = sorted({
        float(value)
        for value in (
            pipeline_payload.get(
                "production_portfolio_replay_required_starting_balances",
                [1000.0, 10000.0],
            )
            or []
        )
        if _finite(value) > 0.0
    })
    if not balances or balances != required_balances:
        raise SystemExit(
            "--starting-balances must exactly match "
            "pipeline.production_portfolio_replay_required_starting_balances: "
            f"{required_balances}"
        )
    configured_symbols = [
        str(symbol).strip().upper()
        for symbol in (config_payload.get("symbols") or [])
        if str(symbol).strip()
    ]
    prelockbox_artifact, prelockbox_path, prelockbox_errors = (
        load_pinned_prelockbox_roster(
            config_payload=config_payload,
            config_path=config_path,
            expected_symbols=configured_symbols,
        )
    )
    if prelockbox_errors or not isinstance(prelockbox_artifact, Mapping):
        raise SystemExit(
            "PRELOCKBOX_ARTIFACT_INVALID: "
            + ", ".join(sorted(set(prelockbox_errors or ["UNREADABLE"])))
        )
    if prelockbox_path is None:
        raise SystemExit("PRELOCKBOX_ARTIFACT_PATH_UNRESOLVED")
    frozen_ledger = prelockbox_artifact.get("frozen_ledger")
    if not isinstance(frozen_ledger, Mapping):
        raise SystemExit("PRELOCKBOX_FROZEN_LEDGER_MISSING")
    source_ledger_sha = str(
        (prelockbox_artifact.get("lineage") or {}).get(
            "source_ledger_file_sha256"
        )
        or ""
    ).lower()
    if source_ledger_sha and _sha256(ledger_path).lower() != source_ledger_sha:
        raise SystemExit(
            "SOURCE_LEDGER_CHANGED_AFTER_PRELOCKBOX_FREEZE: use the ledger embedded "
            "in the pinned roster; do not reselect or edit Phase-A cells"
        )
    ledger = dict(frozen_ledger)
    roster, roster_errors = build_ledger_cell_roster(
        ledger,
        configured_symbols,
        allowed_promotion_statuses=("FROZEN_PRELOCKBOX",),
    )
    required_tradeable_ids = set(roster.get("tradeable_cell_ids") or [])
    frozen_roster = prelockbox_artifact.get("roster") or {}
    required_replay_ids = set(frozen_roster.get("tradeable_cell_ids") or [])
    if required_tradeable_ids != required_replay_ids:
        roster_errors.append("PRELOCKBOX_STANDARD_ROSTER_MISMATCH")

    native_contract, native_contract_errors = active_native_portfolio_contract(
        cfg.data_dir,
        configured_symbols,
    )
    from pm_pipeline import build_artifact_meta

    artifact_meta = build_artifact_meta(cfg)
    deployment_contract = dict(artifact_meta.get("deployment_contract", {}) or {})
    code_contract = critical_module_code_contract(REPO)
    if code_contract.get("missing"):
        raise SystemExit(
            "CRITICAL_MODULE_CODE_CONTRACT_INCOMPLETE: "
            + ", ".join(code_contract["missing"])
        )
    expected_prelockbox_lineage = {
        "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment_contract),
        "broker_specs_sha256": _sha256(broker_path),
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "critical_module_code_contract": code_contract,
        "critical_module_code_contract_sha256": code_contract.get(
            "aggregate_sha256"
        ),
    }
    prelockbox_errors = validate_prelockbox_roster_artifact(
        prelockbox_artifact,
        expected_symbols=configured_symbols,
        expected_lineage=expected_prelockbox_lineage,
    )
    if prelockbox_errors:
        raise SystemExit(
            "PRELOCKBOX_ARTIFACT_STALE_OR_TAMPERED: "
            + ", ".join(sorted(set(prelockbox_errors)))
        )
    prelockbox_file_sha = prelockbox_sha256_file(prelockbox_path)
    prelockbox_seal_sha = str(
        (prelockbox_artifact.get("content_seal") or {}).get("sha256") or ""
    )
    base_policies = {
        balance: build_policy(config_payload, balance) for balance in required_balances
    }
    calibration_lineage = {
        "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
        "prelockbox_roster_sha256": frozen_roster.get("roster_sha256"),
        "prelockbox_artifact_file_sha256": prelockbox_file_sha,
        "prelockbox_artifact_seal_sha256": prelockbox_seal_sha,
        "critical_module_code_contract_sha256": code_contract.get(
            "aggregate_sha256"
        ),
        "broker_specs_sha256": _sha256(broker_path),
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "base_policy_contract_sha256": cap_neutral_policy_contract_sha256(
            base_policies
        ),
        "required_starting_balances": required_balances,
    }
    configured_final_evidence = str(
        pipeline_payload.get("production_portfolio_replay_evidence_path") or ""
    ).strip()
    cap_calibration_artifact: Optional[Dict[str, Any]] = None
    cap_calibration_path: Optional[Path] = None
    cap_calibration_file_sha: Optional[str] = None
    if args.partition == "tuning_validation":
        if configured_final_evidence:
            raise SystemExit(
                "Refusing to recalibrate after final portfolio evidence has been pinned; "
                "the final lockbox is one-shot and cannot select the cap"
            )
    else:
        if configured_final_evidence:
            raise SystemExit(
                "Final synchronized replay evidence is already configured; refusing a "
                "second final-lockbox run"
            )
        cap_path_raw = str(
            pipeline_payload.get("production_cap_calibration_path") or ""
        ).strip()
        configured_cap_sha = str(
            pipeline_payload.get("production_cap_calibration_sha256") or ""
        ).strip().lower()
        if not cap_path_raw or len(configured_cap_sha) != 64:
            raise SystemExit(
                "Freeze and pin a tuning_validation cap-calibration artifact before "
                "accessing the final lockbox"
            )
        cap_calibration_path = Path(cap_path_raw)
        if not cap_calibration_path.is_absolute():
            cap_calibration_path = config_path.parent / cap_calibration_path
        try:
            cap_calibration_file_sha = _sha256(cap_calibration_path)
            cap_calibration_artifact = json.loads(
                cap_calibration_path.read_text(encoding="utf-8-sig")
            )
        except Exception as exc:
            raise SystemExit(
                f"Cap-calibration artifact is unreadable: {type(exc).__name__}: {exc}"
            ) from exc
        if cap_calibration_file_sha.lower() != configured_cap_sha:
            raise SystemExit("Cap-calibration artifact file hash does not match config")
        cap_errors = validate_cap_calibration_artifact(
            cap_calibration_artifact,
            expected_lineage=calibration_lineage,
            expected_selected_cap_pct=_finite(
                pipeline_payload.get("max_total_open_risk_pct"), 0.0
            ),
        )
        pre_lockbox_errors = list(roster_errors) + list(native_contract_errors) + cap_errors
        for key in ("use_spread", "swap_model_enabled"):
            if not bool(cost_contract.get(key)):
                pre_lockbox_errors.append(f"COST_COMPONENT_DISABLED:{key}")
        if pre_lockbox_errors:
            raise SystemExit(
                "Refusing final-lockbox access because frozen preconditions failed: "
                + ", ".join(sorted(set(pre_lockbox_errors)))
            )
    all_cells = []
    enumerated_ids: set[str] = set()
    for cell in iter_frozen_tradeable_cells(prelockbox_artifact):
        cell_id = (
            f"{str(cell.get('symbol') or '').upper()}|"
            f"{str(cell.get('timeframe') or '').upper()}|"
            f"{str(cell.get('regime') or '').upper()}"
        )
        if cell_id in required_replay_ids and cell_id not in enumerated_ids:
            all_cells.append(cell)
            enumerated_ids.add(cell_id)
    for missing_cell in sorted(required_replay_ids - enumerated_ids):
        roster_errors.append(f"TRADEABLE_CELL_NOT_ENUMERATED:{missing_cell}")

    # Production evidence has no partial/custom execution surface. Diagnostics
    # belong in a separate tool so they cannot be mistaken for a sealable run.
    cells = list(all_cells)
    partial_run = False
    if not cells:
        raise SystemExit("No frozen pre-lockbox candidate cells matched")

    global_lockbox_access_id: Optional[str] = None
    if args.partition == "final_lockbox":
        # This write occurs before DataLoader opens a final-partition source.
        # A crash or failed verdict still burns the holdout, which is the only
        # defensible one-shot behavior.
        try:
            global_lockbox_access_id = consume_global_final_lockbox_once(
                config=cfg,
                config_path=config_path,
                frozen_ledger=ledger,
                roster_sha256=str(frozen_roster.get("roster_sha256") or ""),
                prelockbox_file_sha256=prelockbox_file_sha,
            )
        except Exception as exc:
            raise SystemExit(str(exc)) from exc

    # Evidence generation must not mutate the canonical data directory or its
    # resample cache; all durable writes are confined to ``--out-dir``.
    loader = DataLoader(cfg.data_dir, cache_resampled=False)
    native_frames: Dict[Tuple[str, str], pd.DataFrame] = {}
    native_sources: Dict[str, Dict[str, Any]] = {}
    valuation_frames: Dict[str, pd.DataFrame] = {}
    valuation_sources: Dict[str, Dict[str, Any]] = {}
    load_errors: List[Dict[str, Any]] = []
    cells_by_source: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for cell in cells:
        symbol = str(cell["symbol"]).upper()
        timeframe = str(cell["timeframe"]).upper()
        cell_id = f"{symbol}|{timeframe}|{str(cell.get('regime') or '').upper()}"
        cells_by_source[(symbol, timeframe)].append(cell_id)
    for (symbol, timeframe), cell_ids in sorted(cells_by_source.items()):
        frame, source_report, error = _native_replay_frame(
            loader,
            symbol,
            timeframe,
            partition=args.partition,
            global_lockbox_access_id=global_lockbox_access_id,
        )
        for cell_id in cell_ids:
            native_sources[cell_id] = dict(source_report)
        if error or frame is None:
            load_errors.append({
                "source": f"{symbol}|{timeframe}",
                "cells": sorted(cell_ids),
                "error": error or "native_data_missing",
            })
            continue
        native_frames[(symbol, timeframe)] = frame

    # Valuation is deliberately independent of every specialist timeframe.
    # Load exactly one authenticated native M5 frame per symbol, even where an
    # M5 specialist frame was already loaded above, so the common equity clock
    # cannot silently inherit a strategy source or a higher-timeframe cadence.
    for symbol in sorted({str(cell["symbol"]).upper() for cell in cells}):
        frame, source_report, error = _native_replay_frame(
            loader,
            symbol,
            "M5",
            partition=args.partition,
            global_lockbox_access_id=global_lockbox_access_id,
        )
        valuation_sources[symbol] = dict(source_report)
        if error or frame is None:
            load_errors.append({
                "source": f"{symbol}|M5|VALUATION",
                "cells": [
                    cell_id
                    for (cell_symbol, _timeframe), cell_ids in cells_by_source.items()
                    if cell_symbol == symbol
                    for cell_id in cell_ids
                ],
                "error": error or "native_m5_valuation_data_missing",
            })
            continue
        valuation_frames[symbol] = frame

    source_partition_pairs = [
        _partition_pair(report, args.partition)
        for report in [*native_sources.values(), *valuation_sources.values()]
    ]
    partition_pairs = {pair for pair in source_partition_pairs if pair is not None}
    every_source_has_partition = bool(native_sources) and all(
        pair is not None for pair in source_partition_pairs
    )
    common_partition = (
        next(iter(partition_pairs))
        if every_source_has_partition and len(partition_pairs) == 1
        else None
    )
    if common_partition is None:
        raise SystemExit(
            f"{args.partition} sources do not prove one exact common calendar partition"
        )
    evaluation_start, evaluation_end = common_partition
    if (
        evaluation_start is None
        or evaluation_end is None
        or evaluation_start >= evaluation_end
    ):
        raise SystemExit(
            f"Empty or unprovable common evaluation window: {evaluation_start} >= {evaluation_end}"
        )
    window_is_exact_partition = bool(
        evaluation_start == common_partition[0]
        and evaluation_end == common_partition[1]
    )

    live_governance_mode = resolved_live_governance_mode(cfg)
    apply_governance = live_governance_mode in {"enforce", "live", "enabled"}
    candidates: List[CandidateTrade] = []
    cell_reports: List[Dict[str, Any]] = []
    replayed_cell_ids: set[str] = set()
    for index, cell in enumerate(cells, start=1):
        symbol = str(cell["symbol"]).upper()
        timeframe = str(cell.get("timeframe") or "").upper()
        regime = str(cell.get("regime") or "").upper()
        ledger_cell_id = f"{symbol}|{timeframe}|{regime}"
        native_frame = native_frames.get((symbol, timeframe))
        if native_frame is None:
            cell_reports.append({"cell": ledger_cell_id, "error": "no_native_timeframe_data"})
            continue
        try:
            spec, spec_key = resolve_instrument_spec(symbol, broker_specs)
            cell_candidates, report = replay_cell_candidates(
                cfg,
                cell,
                native_frame,
                spec,
                spec_key,
                evaluation_start=evaluation_start,
                evaluation_end=evaluation_end,
                apply_governance=apply_governance,
                include_marks=True,
                valuation_frame=valuation_frames.get(symbol),
            )
            candidates.extend(cell_candidates)
            cell_reports.append(report)
            if not report.get("error"):
                replayed_cell_ids.add(ledger_cell_id)
        except Exception as exc:
            cell_reports.append({
                "cell": f"{symbol}|{cell.get('timeframe')}|{cell.get('regime')}",
                "error": f"{type(exc).__name__}:{exc}",
            })
        print(f"[{index}/{len(cells)}] {cell_reports[-1]['cell']}: {cell_reports[-1].get('candidate_positions', 0)} candidates")

    fatal_reports = [report for report in cell_reports if report.get("error")]
    if fatal_reports or load_errors:
        (output_dir / "cell_generation_failures.json").write_text(
            json.dumps({"load_errors": load_errors, "cell_errors": fatal_reports}, indent=2, default=_json_default),
            encoding="utf-8",
        )
        raise SystemExit(
            f"Fail-closed: {len(load_errors)} symbol loads and {len(fatal_reports)} cells lacked complete candidates. "
            "Inspect cell_generation_failures.json; production evidence cannot omit failed cells."
        )
    if not candidates:
        raise SystemExit("No candidate positions were generated")

    # Funded live admission and funded replay both use the shared deterministic
    # factor taxonomy. Pair-correlation remains a research primitive only.
    correlation_book = None
    if args.partition == "tuning_validation":
        calibration_failures = list(roster_errors) + list(native_contract_errors)
        if not every_source_has_partition or len(partition_pairs) != 1:
            calibration_failures.append("TUNING_VALIDATION_BOUNDARIES_NOT_COMMON")
        if not window_is_exact_partition:
            calibration_failures.append("CALIBRATION_WINDOW_NOT_EXACT_TUNING_VALIDATION")
        if partial_run:
            calibration_failures.append("PARTIAL_CALIBRATION_RUN")
        if sorted(replayed_cell_ids) != sorted(required_replay_ids):
            calibration_failures.append("CALIBRATION_REPLAYED_CELL_ROSTER_MISMATCH")
        for key in ("use_spread", "swap_model_enabled"):
            if not bool(cost_contract.get(key)):
                calibration_failures.append(f"COST_COMPONENT_DISABLED:{key}")
        for cell_id, source in native_sources.items():
            if source.get("activation_id") != native_contract.get("activation_id"):
                calibration_failures.append(f"NATIVE_ACTIVATION_MISMATCH:{cell_id}")
            if source.get("activation_manifest_sha256") != native_contract.get(
                "activation_manifest_sha256"
            ):
                calibration_failures.append(f"NATIVE_MANIFEST_MISMATCH:{cell_id}")
        for symbol, source in valuation_sources.items():
            if source.get("activation_id") != native_contract.get("activation_id"):
                calibration_failures.append(
                    f"VALUATION_ACTIVATION_MISMATCH:{symbol}"
                )
            if source.get("activation_manifest_sha256") != native_contract.get(
                "activation_manifest_sha256"
            ):
                calibration_failures.append(
                    f"VALUATION_MANIFEST_MISMATCH:{symbol}"
                )
        if calibration_failures:
            raise SystemExit(
                "Tuning cap calibration failed closed before selection: "
                + ", ".join(sorted(set(calibration_failures)))
            )

        configured_max_dd = min(
            20.0,
            _finite(
                pipeline_payload.get("production_portfolio_replay_max_drawdown_pct"),
                20.0,
            ),
        )
        constraints = CapCalibrationConstraints(
            minimum_calendar_days=int(
                _finite(
                    pipeline_payload.get("production_portfolio_replay_min_calendar_days"),
                    730.0,
                )
            ),
            minimum_accepted_positions=int(
                _finite(
                    pipeline_payload.get("production_portfolio_replay_min_positions"),
                    100.0,
                )
            ),
            minimum_profit_factor=_finite(
                pipeline_payload.get("production_portfolio_replay_min_profit_factor"),
                1.05,
            ),
            minimum_return_pct=_finite(
                pipeline_payload.get("production_portfolio_replay_min_return_pct"),
                0.0,
            ),
            maximum_mtm_drawdown_pct=configured_max_dd,
            maximum_initial_stop_stress_drawdown_pct=configured_max_dd,
            maximum_bootstrap_ruin_upper_95pct=1.0,
            maximum_bootstrap_p95_drawdown_pct=configured_max_dd,
            minimum_mtm_coverage_pct=100.0,
            minimum_cap_utilization_fraction=0.80,
            initial_stop_gap_multiple=2.5,
            maximum_gap_stress_loss_pct=configured_max_dd,
        )
        calibration = calibrate_total_stop_risk_cap(
            candidates=candidates,
            base_policies=base_policies,
            evaluation_start=evaluation_start,
            evaluation_end=evaluation_end,
            lineage=calibration_lineage,
            constraints=constraints,
            correlation_lookup=correlation_book,
        )
        calibration_path = write_immutable_cap_calibration(output_dir, calibration)
        selected_cap = (calibration.get("selection") or {}).get(
            "selected_max_total_stop_risk_pct"
        )
        calibration_manifest = {
            "offline_only": True,
            "places_trades": False,
            "partition": "tuning_validation",
            "window": {
                "evaluation_start": evaluation_start.isoformat(),
                "evaluation_end": evaluation_end.isoformat(),
            },
            "inputs": {
                "config": {"path": str(config_path), "sha256": _sha256(config_path)},
                "ledger": {"path": str(ledger_path), "sha256": _sha256(ledger_path)},
                "broker_specs": {"path": str(broker_path), "sha256": _sha256(broker_path)},
                "prelockbox_roster": {
                    "path": str(prelockbox_path),
                    "file_sha256": prelockbox_file_sha,
                    "content_seal_sha256": prelockbox_seal_sha,
                    "roster_sha256": frozen_roster.get("roster_sha256"),
                },
            },
            "candidate_cells": len(cells),
            "candidate_positions": len(candidates),
            "cell_generation_reports": cell_reports,
            "cap_calibration": {
                "path": str(calibration_path),
                "sha256": _sha256(calibration_path),
                "status": (calibration.get("selection") or {}).get("status"),
                "selected_max_total_stop_risk_pct": selected_cap,
            },
        }
        (output_dir / "cap_calibration_manifest.json").write_text(
            json.dumps(
                calibration_manifest,
                indent=2,
                sort_keys=True,
                default=_json_default,
            ),
            encoding="utf-8",
        )
        _write_csv(output_dir / "cell_generation_report.csv", cell_reports)
        print(json.dumps({
            "out_dir": str(output_dir),
            "cap_calibration_path": str(calibration_path),
            "cap_calibration_sha256": _sha256(calibration_path),
            "status": (calibration.get("selection") or {}).get("status"),
            "selected_max_total_stop_risk_pct": selected_cap,
            "next_step": (
                "Set max_total_open_risk_pct plus production_cap_calibration_path/sha256 "
                "to these frozen values before the one-shot --partition final_lockbox run"
            ),
        }, indent=2, default=_json_default))
        return 0 if selected_cap is not None else 2

    scenario_summaries: List[Dict[str, Any]] = []
    scenario_records: List[Dict[str, Any]] = []
    for starting_balance in balances:
        policy = build_policy(config_payload, starting_balance)
        result = SynchronizedPortfolioSimulator(policy, correlation_book).run(candidates)
        scenario_name = f"balance_{starting_balance:.2f}".replace(".", "_")
        scenario_dir = output_dir / scenario_name
        scenario_dir.mkdir(parents=True, exist_ok=True)
        (scenario_dir / "summary.json").write_text(
            json.dumps(result["summary"], indent=2, sort_keys=True, default=_json_default), encoding="utf-8"
        )
        _write_csv(scenario_dir / "portfolio_path.csv", result["path"])
        _write_csv(scenario_dir / "closed_positions.csv", result["closed_positions"])
        _write_csv(scenario_dir / "blocked_positions.csv", result["blocked_positions"])
        scenario_summaries.append(result["summary"])
        scenario_records.append({
            "starting_balance": float(starting_balance),
            "policy": result["policy"],
            "summary": result["summary"],
        })

    lineage = {
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment_contract),
        "prelockbox_roster_sha256": frozen_roster.get("roster_sha256"),
        "prelockbox_artifact_file_sha256": prelockbox_file_sha,
        "prelockbox_artifact_seal_sha256": prelockbox_seal_sha,
        "critical_module_code_contract_sha256": code_contract.get(
            "aggregate_sha256"
        ),
        "broker_specs_sha256": _sha256(broker_path),
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "cap_calibration_sha256": cap_calibration_file_sha,
        "cap_calibration_selected_max_total_stop_risk_pct": (
            (cap_calibration_artifact.get("selection") or {}).get(
                "selected_max_total_stop_risk_pct"
            )
            if isinstance(cap_calibration_artifact, Mapping)
            else None
        ),
    }
    expected_policies = {
        balance: build_policy(config_payload, balance).__dict__
        for balance in required_balances
    }
    cell_generation_errors = [
        str(report.get("error")) for report in fatal_reports if report.get("error")
    ] + [str(report.get("error")) for report in load_errors if report.get("error")]
    replay_contract = {
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "portfolio_schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
        "window": {
            "mode": "common_intersection",
            "partition": "final_lockbox",
            "evaluation_start": evaluation_start.isoformat(),
            "evaluation_end": evaluation_end.isoformat(),
        },
        "partial_run": partial_run,
        "allow_cell_errors": False,
        "include_mtm": True,
        "prelockbox_source_status": "FROZEN_PRELOCKBOX",
        "evidence_only_status_allowlist": ["FROZEN_PRELOCKBOX"],
        "global_lockbox_access_id": global_lockbox_access_id,
        "replayed_cell_ids": sorted(replayed_cell_ids),
        "cell_generation_errors": cell_generation_errors,
        "native_sources": native_sources,
        "valuation_sources": valuation_sources,
        "cost_contract": cost_contract,
        "mark_cost_contract": portfolio_mark_cost_contract(cost_contract),
        "governance": {
            "configured_live_mode": live_governance_mode,
            "applied_in_candidate_generation": apply_governance,
        },
        "margin": {
            "model": OFFLINE_MARGIN_MODEL,
            "rates": DEFAULT_MARGIN_RATES,
            "broker_exact": False,
        },
        "candidate_positions": len(candidates),
        "scenarios": scenario_records,
    }
    evidence_payload: Dict[str, Any] = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "verdict": "PASS",
        "failures": [],
        "offline_only": True,
        "places_trades": False,
        "profitability_guarantee": False,
        "lineage": lineage,
        "roster": roster,
        "replay": replay_contract,
    }
    manual_failures = list(roster_errors) + list(native_contract_errors)
    if not every_source_has_partition or len(partition_pairs) != 1:
        manual_failures.append("FINAL_LOCKBOX_BOUNDARIES_NOT_COMMON")
    if not window_is_exact_partition:
        manual_failures.append("REPLAY_WINDOW_NOT_EXACT_FINAL_LOCKBOX")
    if partial_run:
        manual_failures.append("PARTIAL_REPLAY_RUN")
    for key in ("use_spread", "swap_model_enabled"):
        if not bool(cost_contract.get(key)):
            manual_failures.append(f"COST_COMPONENT_DISABLED:{key}")
    for cell_id, source in native_sources.items():
        if source.get("activation_id") != native_contract.get("activation_id"):
            manual_failures.append(f"NATIVE_ACTIVATION_MISMATCH:{cell_id}")
        if source.get("activation_manifest_sha256") != native_contract.get(
            "activation_manifest_sha256"
        ):
            manual_failures.append(f"NATIVE_MANIFEST_MISMATCH:{cell_id}")
    for symbol, source in valuation_sources.items():
        if source.get("activation_id") != native_contract.get("activation_id"):
            manual_failures.append(f"VALUATION_ACTIVATION_MISMATCH:{symbol}")
        if source.get("activation_manifest_sha256") != native_contract.get(
            "activation_manifest_sha256"
        ):
            manual_failures.append(f"VALUATION_MANIFEST_MISMATCH:{symbol}")
    provisional = seal_portfolio_replay_evidence(evidence_payload)
    frozen_cap_constraints = (
        cap_calibration_artifact.get("constraints")
        if isinstance(cap_calibration_artifact, Mapping)
        and isinstance(cap_calibration_artifact.get("constraints"), Mapping)
        else {}
    )
    contract_failures = validate_funded_portfolio_replay_evidence(
        provisional,
        expected_lineage=lineage,
        expected_roster=roster,
        expected_cost_contract=cost_contract,
        expected_policies=expected_policies,
        minimum_calendar_days=_finite(
            pipeline_payload.get("production_portfolio_replay_min_calendar_days"),
            730.0,
        ),
        minimum_accepted_positions=int(
            _finite(
                pipeline_payload.get("production_portfolio_replay_min_positions"),
                100.0,
            )
        ),
        minimum_profit_factor=_finite(
            pipeline_payload.get("production_portfolio_replay_min_profit_factor"),
            1.05,
        ),
        minimum_return_pct=_finite(
            pipeline_payload.get("production_portfolio_replay_min_return_pct"),
            0.0,
        ),
        maximum_drawdown_pct=_finite(
            pipeline_payload.get("production_portfolio_replay_max_drawdown_pct"),
            20.0,
        ),
        require_mtm=bool(
            pipeline_payload.get("production_portfolio_replay_require_mtm", True)
        ),
        maximum_bootstrap_ruin_upper_95pct=_finite(
            frozen_cap_constraints.get(
                "maximum_bootstrap_ruin_upper_95pct"
            ),
            float("nan"),
        ),
        maximum_bootstrap_p95_drawdown_pct=_finite(
            frozen_cap_constraints.get(
                "maximum_bootstrap_p95_drawdown_pct"
            ),
            float("nan"),
        ),
        minimum_bootstrap_daily_return_observations=_finite(
            frozen_cap_constraints.get(
                "minimum_bootstrap_daily_return_observations"
            ),
            float("nan"),
        ),
    )
    all_failures = sorted(set(manual_failures + contract_failures))
    evidence_payload["verdict"] = "FAIL" if all_failures else "PASS"
    evidence_payload["failures"] = all_failures
    evidence_path = write_immutable_portfolio_replay_evidence(output_dir, evidence_payload)
    released_ledger_path: Optional[Path] = None
    if evidence_payload["verdict"] == "PASS":
        released_ledger = materialize_joint_pass_ledger(
            prelockbox_artifact,
            portfolio_evidence_sha256=_sha256(evidence_path),
        )
        released_ledger_path = write_immutable_released_ledger(
            output_dir, released_ledger
        )

    manifest = {
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "offline_only": True,
        "places_trades": False,
        "repo": str(REPO),
        "inputs": {
            "config": {"path": str(config_path), "sha256": _sha256(config_path)},
            "ledger": {"path": str(ledger_path), "sha256": _sha256(ledger_path)},
            "prelockbox_roster": {
                "path": str(prelockbox_path),
                "sha256": prelockbox_file_sha,
                "roster_sha256": frozen_roster.get("roster_sha256"),
            },
            "broker_specs": {"path": str(broker_path), "sha256": _sha256(broker_path)},
        },
        "window": {
            "mode": "common_intersection",
            "evaluation_start": evaluation_start.isoformat() if evaluation_start is not None else None,
            "evaluation_end": evaluation_end.isoformat() if evaluation_end is not None else None,
        },
        "governance": {
            "configured_live_mode": live_governance_mode,
            "applied_in_candidate_generation": apply_governance,
        },
        "margin": {
            "model": OFFLINE_MARGIN_MODEL,
            "rates": DEFAULT_MARGIN_RATES,
            "broker_exact": False,
        },
        "candidate_cells": len(cells),
        "candidate_positions": len(candidates),
        "cell_generation_reports": cell_reports,
        "load_errors": load_errors,
        "scenarios": scenario_summaries,
        "portfolio_evidence": {
            "path": str(evidence_path),
            "sha256": _sha256(evidence_path),
            "verdict": evidence_payload["verdict"],
            "failures": all_failures,
        },
        "released_funded_ledger": (
            {
                "path": str(released_ledger_path),
                "sha256": _sha256(released_ledger_path),
                "release_scope": "ALL_FROZEN_TRADEABLE_CELLS",
                "detached_authorization_required": True,
            }
            if released_ledger_path is not None
            else None
        ),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=_json_default), encoding="utf-8"
    )
    _write_csv(output_dir / "cell_generation_report.csv", cell_reports)
    print(json.dumps({
        "out_dir": str(output_dir),
        "evidence_path": str(evidence_path),
        "verdict": evidence_payload["verdict"],
        "failures": all_failures,
        "released_funded_ledger_path": (
            str(released_ledger_path) if released_ledger_path is not None else None
        ),
        "released_funded_ledger_sha256": (
            _sha256(released_ledger_path) if released_ledger_path is not None else None
        ),
        "next_step": (
            "Pin the released funded ledger and portfolio evidence, then create the "
            "detached exact-cell promotion authorization; a PASS is not itself live authority."
            if released_ledger_path is not None
            else "The final lockbox is consumed and the frozen roster is rejected; do not replay survivors."
        ),
        "scenarios": scenario_summaries,
    }, indent=2, default=_json_default))
    return 0 if evidence_payload["verdict"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
