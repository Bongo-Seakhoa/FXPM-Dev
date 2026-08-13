"""Bounded promoted-cell replay for MAE/MFE and trade-path evidence.

This script is read-only against production inputs. It does not place trades,
does not call live MT5 order APIs, and writes only under
`agent_audit/performance/data/` unless an explicit output path is supplied.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Mapping, Tuple

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from pm_core import (
    Backtester,
    DataLoader,
    FeatureComputer,
    PipelineConfig,
    get_instrument_spec,
    strict_pipeline_config_kwargs,
)
from pm_strategies import StrategyRegistry


REPLAY_SCHEMA_VERSION = "2026-07-10-native-activation-regime-entry-filter-v3"
SUPPORTED_NATIVE_REPLAY_TIMEFRAMES = frozenset(
    {"M5", "M15", "M30", "H1", "H4", "D1"}
)


def load_activated_native_replay_frame(
    loader: DataLoader,
    symbol: str,
    timeframe: str,
) -> Tuple[Any, Dict[str, Any], str]:
    """Load one promoted cell's hash-pinned native timeframe or fail closed.

    Production replay must evaluate the same broker-native candle authority as
    discovery/live operation.  Loading legacy M5 and resampling it here would
    silently answer a different experiment for every higher timeframe.
    """
    canonical_symbol = str(symbol or "").strip().upper()
    tf = str(timeframe or "").strip().upper()
    if tf not in SUPPORTED_NATIVE_REPLAY_TIMEFRAMES:
        return None, {"native_timeframe": tf}, "unsupported_native_timeframe"
    try:
        frame, raw_meta = loader.load_native_timeframe(canonical_symbol, tf)
    except Exception as exc:
        return None, {
            "native_timeframe": tf,
            "native_source_errors": f"NATIVE_REPLAY_LOAD_FAILED:{type(exc).__name__}",
        }, "native_activation_load_failed"

    meta: Mapping[str, Any] = raw_meta if isinstance(raw_meta, Mapping) else {}
    errors = [str(value) for value in (meta.get("provenance_errors") or [])]
    evidence = {
        "native_timeframe": tf,
        "native_source_kind": str(meta.get("source_kind") or "unknown"),
        "native_source_sha256": str(meta.get("sha256") or ""),
        "native_activation_id": str(meta.get("activation_id") or ""),
        "native_activation_manifest_sha256": str(
            meta.get("activation_manifest_sha256") or ""
        ),
        "native_timestamp_semantics": str(meta.get("timestamp_semantics") or ""),
        "native_source_errors": ";".join(errors),
    }
    if frame is None or len(frame) == 0:
        return None, evidence, "native_activation_unavailable"
    if str(meta.get("source_kind") or "").strip().lower() != "broker_native":
        return None, evidence, "native_source_not_broker_native"
    if str(meta.get("symbol") or "").strip().upper() != canonical_symbol:
        return None, evidence, "native_source_symbol_mismatch"
    if str(meta.get("timeframe") or "").strip().upper() != tf:
        return None, evidence, "native_source_timeframe_mismatch"
    if not bool(meta.get("provenance_valid", False)):
        return None, evidence, "native_source_provenance_invalid"
    if str(meta.get("timestamp_semantics") or "").strip().lower() != "bar_open_utc":
        return None, evidence, "native_timestamp_semantics_invalid"
    activation_id = str(meta.get("activation_id") or "").strip()
    manifest_sha = str(meta.get("activation_manifest_sha256") or "").strip().lower()
    activation_entry = meta.get("activation_entry")
    if (
        not activation_id
        or len(manifest_sha) != 64
        or not isinstance(activation_entry, Mapping)
        or str(activation_entry.get("status") or "").strip().lower() != "active"
    ):
        return None, evidence, "native_activation_unpinned"
    return frame, evidence, ""


def gate_signals_to_cell_regime(signals: Any,
                                features: Any,
                                regime: str) -> tuple[Any, str, int]:
    """Restrict replay entries to the regime represented by a live ledger cell.

    A promoted regime leaf is only eligible to open a position while
    ``REGIME_LIVE`` equals that leaf's regime.  Replaying the underlying strategy
    unconditionally answers a different question and can make a losing live cell
    appear profitable because signals from other regimes are included.

    Symbol-level legacy winners intentionally remain unconditional.  The returned
    tuple is ``(filtered_signals, regime_column, matching_bar_count)``.
    """
    target = str(regime or "SYMBOL_LEVEL").strip().upper()
    if target in {"", "SYMBOL_LEVEL", "ALL", "ANY"}:
        return signals.copy(), "unconditional", int(len(features))

    regime_column = "REGIME_LIVE"
    if regime_column not in features.columns:
        raise ValueError("regime_cell_replay_requires_REGIME_LIVE")
    if not hasattr(signals, "index"):
        raise TypeError("regime_cell_replay_requires_indexed_signals")

    labels = features[regime_column].reindex(signals.index)
    mask = labels.astype(str).str.upper().eq(target).fillna(False)
    filtered = signals.copy()
    filtered.loc[~mask] = 0
    return filtered, regime_column, int(mask.sum())


def _pipeline_config(config_path: Path, starting_balance: float) -> PipelineConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    pipe = dict(payload.get("pipeline") or {})
    kwargs = strict_pipeline_config_kwargs(pipe)
    kwargs["data_dir"] = str((config_path.parent / str(kwargs.get("data_dir", "data"))).resolve())
    regime_file = kwargs.get("regime_params_file", "regime_params.json")
    kwargs["regime_params_file"] = str((config_path.parent / str(regime_file)).resolve())
    cfg = PipelineConfig(**kwargs)
    cfg.initial_capital = float(starting_balance)
    return cfg


def _tradeable(payload: Dict[str, Any]) -> bool:
    name = str(payload.get("strategy_name", "") or "").upper()
    meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    status = str(payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
    return bool(
        name
        and name != "NO_TRADE"
        and not meta.get("no_trade")
        and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
    )


def _cell_row(symbol: str, timeframe: str, regime: str, payload: Dict[str, Any], source: str) -> Dict[str, Any]:
    return {
        "symbol": str(payload.get("symbol") or symbol).upper(),
        "timeframe": str(payload.get("timeframe") or timeframe or "H1").upper(),
        "regime": str(payload.get("regime") or regime or "SYMBOL_LEVEL").upper(),
        "strategy_name": payload.get("strategy_name"),
        "parameters": payload.get("parameters") or {},
        "promotion_status": str(payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper(),
        "promotion_source": source,
        "val_metrics": payload.get("val_metrics") if isinstance(payload.get("val_metrics"), dict) else {},
        "train_metrics": payload.get("train_metrics") if isinstance(payload.get("train_metrics"), dict) else {},
    }


def iter_promoted_cells(ledger: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for symbol, symbol_payload in sorted(ledger.items()):
        if not isinstance(symbol_payload, dict):
            continue
        symbol_is_promoted = _tradeable(symbol_payload)
        promoted_leaves: List[Dict[str, Any]] = []
        regimes = symbol_payload.get("regime_configs")
        if isinstance(regimes, dict):
            for timeframe, regime_payload in sorted(regimes.items()):
                if not isinstance(regime_payload, dict):
                    continue
                for regime, cell in sorted(regime_payload.items()):
                    if isinstance(cell, dict) and _tradeable(cell):
                        source = "symbol_and_regime" if symbol_is_promoted else "regime_leaf"
                        row = _cell_row(
                            str(symbol_payload.get("symbol") or symbol),
                            str(timeframe),
                            str(regime),
                            cell,
                            source,
                        )
                        row["symbol"] = str(symbol_payload.get("symbol") or symbol).upper()
                        promoted_leaves.append(row)
        if promoted_leaves:
            yielded = set()
            for row in promoted_leaves:
                key = (
                    row.get("symbol"),
                    row.get("timeframe"),
                    row.get("regime"),
                    row.get("strategy_name"),
                )
                if key in yielded:
                    continue
                yielded.add(key)
                yield row
        elif symbol_is_promoted:
            yield _cell_row(
                str(symbol_payload.get("symbol") or symbol),
                str(symbol_payload.get("timeframe") or "H1"),
                str(symbol_payload.get("regime") or "SYMBOL_LEVEL"),
                symbol_payload,
                "symbol_level",
            )


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _summary(values: List[float]) -> Dict[str, float]:
    clean = [_finite(value) for value in values if math.isfinite(_finite(value))]
    if not clean:
        return {"mean": 0.0, "median": 0.0, "max": 0.0}
    return {"mean": mean(clean), "median": median(clean), "max": max(clean)}


def replay_cell(cfg: PipelineConfig, loader: DataLoader, cell: Dict[str, Any]) -> Dict[str, Any]:
    symbol = cell["symbol"]
    timeframe = cell["timeframe"]
    strategy_name = cell["strategy_name"]
    frame, native_evidence, native_error = load_activated_native_replay_frame(
        loader,
        symbol,
        timeframe,
    )
    if native_error:
        return {**cell, **native_evidence, "error": native_error}
    if frame is None or len(frame) < 200:
        return {**cell, **native_evidence, "error": "insufficient_native_bars"}
    strategy = StrategyRegistry.get(strategy_name, **(cell.get("parameters") or {}))
    # The replay is a live-cell parity check, so regime state is mandatory even
    # when the strategy itself does not request regime features.  The prior
    # compute_for_strategy path omitted REGIME_LIVE for most strategies and made
    # it impossible to replay the actual promoted regime leaf.
    features = FeatureComputer.compute_all(
        frame,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=cfg.regime_params_file,
    )
    signals = strategy.generate_signals(features, symbol)
    try:
        signals, regime_column, regime_matching_bars = gate_signals_to_cell_regime(
            signals,
            features,
            str(cell.get("regime") or "SYMBOL_LEVEL"),
        )
    except (TypeError, ValueError) as exc:
        return {**cell, **native_evidence, "error": str(exc)}
    if hasattr(signals, "abs") and int(signals.abs().sum()) == 0:
        return {
            **cell,
            **native_evidence,
            "error": "no_signals_in_cell_regime",
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "regime_filter_column": regime_column,
            "regime_matching_bars": regime_matching_bars,
        }
    result = Backtester(cfg).run(
        features,
        signals,
        symbol,
        strategy,
        spec=get_instrument_spec(symbol),
        timeframe=timeframe,
    )
    trades = result.get("trades") or []
    mae_r = [_finite(trade.get("mae_r")) for trade in trades if trade.get("mae_r") is not None]
    mfe_r = [_finite(trade.get("mfe_r")) for trade in trades if trade.get("mfe_r") is not None]
    r_mult = [_finite(trade.get("r_multiple")) for trade in trades if trade.get("r_multiple") is not None]
    mae = _summary(mae_r)
    mfe = _summary(mfe_r)
    giveback = _summary([mfe_r[idx] - r_mult[idx] for idx in range(min(len(mfe_r), len(r_mult)))])
    return {
        **cell,
        **native_evidence,
        "error": "",
        "replay_schema_version": REPLAY_SCHEMA_VERSION,
        "regime_filter_column": regime_column,
        "regime_matching_bars": regime_matching_bars,
        "starting_balance": cfg.initial_capital,
        "total_trades": int(result.get("total_trades", 0) or 0),
        "net_profit": round(_finite(result.get("total_pnl")), 2),
        "return_pct": round(_finite(result.get("total_return_pct")), 4),
        "max_drawdown_pct": round(_finite(result.get("max_drawdown_pct")), 4),
        "profit_factor": result.get("profit_factor", 0.0),
        "expectancy_pips": round(_finite(result.get("expectancy_pips")), 4),
        "return_drawdown_ratio": round(_finite(result.get("return_drawdown_ratio")), 4),
        "calmar_ratio_annualized": round(_finite(result.get("calmar_ratio_annualized")), 4),
        "avg_mae_r": round(mae["mean"], 4),
        "median_mae_r": round(mae["median"], 4),
        "max_mae_r": round(mae["max"], 4),
        "avg_mfe_r": round(mfe["mean"], 4),
        "median_mfe_r": round(mfe["median"], 4),
        "max_mfe_r": round(mfe["max"], 4),
        "avg_mfe_giveback_r": round(giveback["mean"], 4),
        "schema_version": trades[0].get("trade_record_schema_version", "") if trades else "",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(REPO / "config.json"))
    parser.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    parser.add_argument("--starting-balance", type=float, default=10000.0)
    parser.add_argument("--max-cells", type=int, default=12)
    parser.add_argument("--strategy", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--list-only", action="store_true")
    parser.add_argument(
        "--out",
        default=str(REPO / "agent_audit" / "performance" / "data" / "promoted_cell_replay_10000.csv"),
    )
    args = parser.parse_args()

    ledger = json.loads(Path(args.ledger).read_text(encoding="utf-8-sig"))
    cells = list(iter_promoted_cells(ledger))
    if args.strategy:
        cells = [cell for cell in cells if str(cell.get("strategy_name")) == args.strategy]
    if args.symbol:
        cells = [cell for cell in cells if str(cell.get("symbol")).upper() == args.symbol.upper()]
    cells = cells[: max(0, int(args.max_cells))]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.list_only:
        out_path.write_text(json.dumps({"selected_cells": cells}, indent=2), encoding="utf-8")
        print(f"listed {len(cells)} cell(s) -> {out_path}")
        return 0

    cfg = _pipeline_config(Path(args.config), args.starting_balance)
    loader = DataLoader(cfg.data_dir)
    rows = [replay_cell(cfg, loader, cell) for cell in cells]
    fieldnames = sorted({key for row in rows for key in row})
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"replayed {len(rows)} cell(s) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
