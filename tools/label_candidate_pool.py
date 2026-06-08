"""Replay OOS-manifest candidates over full history to create holder/fall-off labels.

This is a fixed-parameter replay tool, not an optimizer. It consumes the emitted
`oos_candidate_matrix_v1` manifests, replays each candidate with the same
Backtester contract used by `tools/benchmark.py`, and writes resumable per-symbol
JSONL plus a combined JSON summary for downstream selector validation.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

REPO = Path(__file__).resolve().parent.parent

import sys

sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchmark import (  # noqa: E402
    Backtester,
    DataLoader,
    FeatureComputer,
    StrategyRegistry,
    _finite_float,
    _git_head,
    _sanitize_profit_factor,
    build_config,
    get_instrument_spec,
    path_dependent_dd,
)


REGIME_VALUES = {"TREND", "RANGE", "BREAKOUT", "CHOP"}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _slope(values: np.ndarray) -> float:
    y = np.asarray(values, dtype=np.float64)
    mask = np.isfinite(y)
    y = y[mask]
    if y.size < 2:
        return 0.0
    x = np.arange(y.size, dtype=np.float64)
    return float(np.polyfit(x, y, 1)[0])


def _period_sharpe(values: np.ndarray) -> float:
    y = np.asarray(values, dtype=np.float64)
    y = y[np.isfinite(y)]
    if y.size < 2:
        return 0.0
    std = float(np.std(y, ddof=1))
    if std <= 1e-12:
        return 0.0
    return float(np.mean(y) / std)


def _period_sortino(values: np.ndarray) -> float:
    y = np.asarray(values, dtype=np.float64)
    y = y[np.isfinite(y)]
    if y.size < 2:
        return 0.0
    downside = y[y < 0.0]
    if downside.size >= 2:
        denom = float(np.std(downside, ddof=1))
    elif downside.size == 1:
        denom = abs(float(downside[0]))
    else:
        return 10.0 if float(np.mean(y)) > 0.0 else 0.0
    return float(np.mean(y) / denom) if denom > 1e-12 else 0.0


def _period_cvar_loss(values: np.ndarray, q: float = 0.05) -> float:
    y = np.asarray(values, dtype=np.float64)
    y = y[np.isfinite(y)]
    if y.size == 0:
        return 0.0
    tail_n = max(1, int(math.ceil(y.size * max(0.0, min(1.0, q)))))
    return float(abs(np.mean(np.sort(y)[:tail_n])))


def _predictor_payload(period_returns: np.ndarray, record: dict[str, Any]) -> dict[str, Any]:
    returns = np.asarray(period_returns, dtype=np.float64)
    mean = float(np.nanmean(returns)) if returns.size else 0.0
    slope = _slope(returns)
    first32 = _slope(returns[:32]) if returns.size >= 32 else 0.0
    last32 = _slope(returns[-32:]) if returns.size >= 32 else 0.0
    first_half = returns[: returns.size // 2]
    second_half = returns[returns.size // 2 :]
    first16 = returns[:16]
    last16 = returns[-16:]
    trade_path = record.get("trade_path_evidence") or {}
    generalization = record.get("generalization_metrics") or {}
    equity_path = record.get("equity_path_evidence") or {}
    return {
        "oos_period_count": int(returns.size),
        "oos_mean_return": round(mean, 10),
        "oos_std_return": round(float(np.nanstd(returns, ddof=1)), 10) if returns.size >= 2 else 0.0,
        "oos_sharpe": round(_period_sharpe(returns), 10),
        "oos_sortino": round(_period_sortino(returns), 10),
        "oos_cvar5_loss": round(_period_cvar_loss(returns, 0.05), 10),
        "oos_positive_period_rate": round(
            float(np.mean(returns > 0.0)),
            10,
        ) if returns.size else 0.0,
        "oos_slope": round(slope, 12),
        "oos_slope_x1e4": round(slope * 1e4, 6),
        "oos_relative_drift": round(float(slope / mean), 10) if abs(mean) > 1e-12 else 0.0,
        "oos_first32_slope_x1e4": round(first32 * 1e4, 6),
        "oos_last32_slope_x1e4": round(last32 * 1e4, 6),
        "oos_second_half_minus_first_half": round(
            float(np.nanmean(second_half) - np.nanmean(first_half)),
            10,
        ) if first_half.size and second_half.size else 0.0,
        "oos_last16_minus_first16": round(
            float(np.nanmean(last16) - np.nanmean(first16)),
            10,
        ) if first16.size and last16.size else 0.0,
        "late_gain_share": _safe_float(trade_path.get("late_gain_share"), 0.0),
        "return_retention": _safe_float(generalization.get("return_retention"), 0.0),
        "profit_factor_retention": _safe_float(generalization.get("profit_factor_retention"), 0.0),
        "oos_path_max_dd_pct": _safe_float(equity_path.get("max_drawdown_pct"), 0.0),
        "oos_path_time_underwater_fraction": _safe_float(equity_path.get("time_underwater_fraction"), 0.0),
    }


def collect_tasks(evidence_dir: Path, symbols: set[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    tasks_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for manifest_path in sorted(evidence_dir.glob("*_oos_manifest.json")):
        manifest = _load_json(manifest_path)
        symbol = str(manifest.get("symbol") or manifest_path.name.split("_", 1)[0])
        if symbols and symbol not in symbols:
            continue
        matrix_path = manifest_path.parent / str(manifest.get("matrix_path", "")).replace("\\", os.sep)
        if not matrix_path.exists():
            matrix_path = manifest_path.parent / Path(str(manifest.get("matrix_path", ""))).name
        matrix = np.load(matrix_path, allow_pickle=True)
        returns = np.asarray(matrix["returns"], dtype=np.float64)
        matrix_contract = manifest.get("matrix_contract") or {}
        artifact_meta = manifest.get("artifact_meta") or {}
        predictor_regime_scope = str(matrix_contract.get("regime_scope") or "unknown")
        predictor_cost_basis = str(artifact_meta.get("backtest_spread_mode") or "unknown")
        candidate_ids = [str(value) for value in matrix["candidate_ids"]]
        id_to_col = {candidate_id: idx for idx, candidate_id in enumerate(candidate_ids)}
        symbol_tasks: list[dict[str, Any]] = []
        for record in manifest.get("records", []):
            candidate_id = str(record.get("candidate_id", ""))
            col = int(record.get("matrix_column", id_to_col.get(candidate_id, -1)))
            if col < 0 or col >= returns.shape[1]:
                continue
            symbol_tasks.append({
                "candidate_id": candidate_id,
                "symbol": symbol,
                "timeframe": str(record.get("timeframe", "")),
                "strategy_name": str(record.get("strategy_name", "")),
                "parameters": record.get("parameters") or {},
                "tuned_for_regime": record.get("tuned_for_regime"),
                "matrix_column": col,
                "search_trials": int(record.get("search_trials", 0) or 0),
                "local_search_trials_total": int(record.get("local_search_trials_total", 0) or 0),
                "predictor_regime_scope": predictor_regime_scope,
                "predictor_cost_basis": predictor_cost_basis,
                "predictors": _predictor_payload(returns[:, col], record),
            })
        tasks_by_symbol[symbol] = symbol_tasks
    return tasks_by_symbol


def _trade_regime(trade: dict[str, Any], features: Any) -> str:
    signal_bar = trade.get("signal_bar", trade.get("entry_bar", None))
    try:
        idx = int(signal_bar)
    except (TypeError, ValueError):
        return ""
    if features is None or "REGIME_LIVE" not in getattr(features, "columns", []):
        return ""
    if not (0 <= idx < len(features)):
        return ""
    regime = features["REGIME_LIVE"].iloc[idx]
    return str(regime) if regime is not None and not (isinstance(regime, float) and math.isnan(regime)) else ""


def _filter_trades_for_regime(trades: list[dict[str, Any]], features: Any, regime: str) -> list[dict[str, Any]]:
    target = str(regime or "").upper()
    return [dict(trade) for trade in trades if _trade_regime(trade, features).upper() == target]


def _ensure_regime_features(features: Any, symbol: str, timeframe: str, regime_params_file: str) -> Any:
    if features is None or "REGIME_LIVE" in getattr(features, "columns", []):
        return features
    FeatureComputer._append_regime_features(features, symbol, timeframe, regime_params_file)
    return features


def _metrics_from_trades(
    cfg: Any,
    backtester: Backtester,
    trades: list[dict[str, Any]],
    features: Any,
    timeframe: str,
) -> dict[str, Any]:
    if not trades:
        result = backtester._empty_result()
        pdd = path_dependent_dd(result.get("equity_curve", []))
        return {
            "total_return_pct": 0.0,
            "trades": 0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "max_dd_pct": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "pct_positive_r": 0.0,
            "max_consec_losses": 0,
            "expectancy_pips": 0.0,
            **pdd,
        }

    initial_capital = float(getattr(cfg, "initial_capital", 10000.0) or 10000.0)
    sorted_trades = sorted(trades, key=lambda t: t.get("entry_bar", t.get("signal_bar", 0)))
    equity = initial_capital
    equity_curve = [equity]
    for trade in sorted_trades:
        equity += _safe_float(trade.get("pnl_dollars"), 0.0)
        equity_curve.append(equity)
    peak = initial_capital
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        if peak > 0.0:
            max_dd = max(max_dd, ((peak - value) / peak) * 100.0)
    metrics = backtester._calculate_metrics(
        sorted_trades,
        equity,
        equity_curve,
        max_dd,
        features=features,
        timeframe=timeframe,
    )
    pdd = path_dependent_dd(equity_curve)
    return {
        "total_return_pct": _finite_float(metrics.get("total_return_pct", 0.0)),
        "trades": int(metrics.get("total_trades", 0) or 0),
        "profit_factor": _sanitize_profit_factor(metrics.get("profit_factor", 0.0)),
        "win_rate": _finite_float(metrics.get("win_rate", 0.0)),
        "max_dd_pct": _finite_float(metrics.get("max_drawdown_pct", 0.0)),
        "sharpe": _finite_float(metrics.get("sharpe_ratio", 0.0)),
        "sortino": _finite_float(metrics.get("sortino_ratio", 0.0)),
        "pct_positive_r": _finite_float(metrics.get("pct_positive_r", 0.0)),
        "max_consec_losses": int(metrics.get("max_consecutive_losses", 0) or 0),
        "expectancy_pips": _finite_float(metrics.get("expectancy_pips", 0.0)),
        "mean_r": _finite_float(metrics.get("mean_r", 0.0)),
        "std_r": _finite_float(metrics.get("std_r", 0.0)),
        "se_r": _finite_float(metrics.get("se_r", 0.0)),
        "edge_t_stat_r": _finite_float(metrics.get("edge_t_stat_r", 0.0)),
        "r_block_cvar": _finite_float(metrics.get("r_block_cvar", 0.0)),
        **pdd,
    }


def _label_payload(metrics: dict[str, Any], min_label_trades: int) -> dict[str, Any]:
    trades = int(metrics.get("trades", 0) or 0)
    ret = _safe_float(metrics.get("total_return_pct", metrics.get("full_history_return_pct")), 0.0)
    pf = _sanitize_profit_factor(metrics.get("profit_factor", 0.0))
    dd = _safe_float(metrics.get("max_dd_pct"), 0.0)
    sufficient = trades >= int(min_label_trades)
    return {
        "label_min_trades": int(min_label_trades),
        "label_trade_sufficient": bool(sufficient),
        "holder_label": bool(sufficient and ret > 0.0),
        "carrier_label": bool(sufficient and ret > 0.0 and pf >= 1.0 and dd <= 35.0),
        "strict_carrier_label": bool(sufficient and ret > 0.0 and pf >= 1.15 and dd <= 35.0),
    }


def _load_done_ids(path: Path) -> set[str]:
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except Exception:
                continue
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id and str(row.get("status") or "").lower() != "error":
                done.add(candidate_id)
    return done


def _evaluate_candidate(
    cfg: Any,
    loader: DataLoader,
    task: dict[str, Any],
    resampled_cache: dict[str, Any],
    label_scope: str,
    min_label_trades: int,
) -> dict[str, Any]:
    symbol = str(task["symbol"])
    timeframe = str(task["timeframe"])
    strategy_name = str(task["strategy_name"])
    params = task.get("parameters") or {}
    base = loader.load_symbol(symbol)
    if base is None or len(base) == 0:
        raise RuntimeError("no_data")
    if timeframe not in resampled_cache:
        df = loader.resample(base, timeframe)
        if df is None or len(df) < 200:
            raise RuntimeError("insufficient_bars")
        resampled_cache[timeframe] = df
    df = resampled_cache[timeframe]
    strategy = StrategyRegistry.get(strategy_name, **params)
    features = FeatureComputer.compute_for_strategy(
        df,
        strategy,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=cfg.regime_params_file,
    )
    features = _ensure_regime_features(features, symbol, timeframe, cfg.regime_params_file)
    signals = strategy.generate_signals(features, symbol)
    backtester = Backtester(cfg)
    result = backtester.run(
        features,
        signals,
        symbol,
        strategy,
        spec=get_instrument_spec(symbol),
        timeframe=timeframe,
    )
    all_metrics = _metrics_from_trades(cfg, backtester, result.get("trades", []), features, timeframe)
    label_regime = ""
    label_metrics = all_metrics
    label_scope = str(label_scope or "all_regimes").lower()
    if label_scope == "regime_gated":
        label_regime = str(task.get("tuned_for_regime") or "").upper()
        if label_regime not in REGIME_VALUES:
            raise RuntimeError("not_regime_tuned")
        if "REGIME_LIVE" not in features.columns:
            raise RuntimeError("missing_REGIME_LIVE")
        regime_trades = _filter_trades_for_regime(result.get("trades", []), features, label_regime)
        label_metrics = _metrics_from_trades(cfg, backtester, regime_trades, features, timeframe)
    else:
        label_scope = "all_regimes"
    all_regime_pdd = path_dependent_dd(result.get("equity_curve", []))
    spread_model = dict(result.get("spread_model", {}) or {})
    payload = {
        "label_scope": label_scope,
        "label_regime": label_regime,
        "label_cost_basis": str(getattr(cfg, "backtest_spread_mode", "")),
        "full_history_return_pct": _finite_float(label_metrics.get("total_return_pct", 0.0)),
        "trades": int(label_metrics.get("trades", 0) or 0),
        "profit_factor": _sanitize_profit_factor(label_metrics.get("profit_factor", 0.0)),
        "win_rate": _finite_float(label_metrics.get("win_rate", 0.0)),
        "max_dd_pct": _finite_float(label_metrics.get("max_dd_pct", 0.0)),
        "sharpe": _finite_float(label_metrics.get("sharpe", 0.0)),
        "sortino": _finite_float(label_metrics.get("sortino", 0.0)),
        "pct_positive_r": _finite_float(label_metrics.get("pct_positive_r", 0.0)),
        "max_consec_losses": int(label_metrics.get("max_consec_losses", 0) or 0),
        "expectancy_pips": _finite_float(label_metrics.get("expectancy_pips", 0.0)),
        "mean_r": _finite_float(label_metrics.get("mean_r", 0.0)),
        "std_r": _finite_float(label_metrics.get("std_r", 0.0)),
        "se_r": _finite_float(label_metrics.get("se_r", 0.0)),
        "edge_t_stat_r": _finite_float(label_metrics.get("edge_t_stat_r", 0.0)),
        "r_block_cvar": _finite_float(label_metrics.get("r_block_cvar", 0.0)),
        "all_regime_return_pct": _finite_float(all_metrics.get("total_return_pct", 0.0)),
        "all_regime_trades": int(all_metrics.get("trades", 0) or 0),
        "all_regime_profit_factor": _sanitize_profit_factor(all_metrics.get("profit_factor", 0.0)),
        "all_regime_max_dd_pct": _finite_float(all_metrics.get("max_dd_pct", 0.0)),
        "all_regime_ulcer_index": _finite_float(all_regime_pdd.get("ulcer_index", 0.0)),
        "all_regime_time_underwater_pct": _finite_float(all_regime_pdd.get("time_underwater_pct", 0.0)),
        "asset_class": spread_model.get("asset_class", ""),
        "spread_provenance_status": spread_model.get("spread_provenance_status", ""),
        "ulcer_index": _finite_float(label_metrics.get("ulcer_index", 0.0)),
        "time_underwater_pct": _finite_float(label_metrics.get("time_underwater_pct", 0.0)),
        "avg_dd_duration": _finite_float(label_metrics.get("avg_dd_duration", 0.0)),
        "max_dd_duration": _finite_float(label_metrics.get("max_dd_duration", 0.0)),
        "dd_recovery_ratio": _finite_float(label_metrics.get("dd_recovery_ratio", 0.0)),
        "pain_index": _finite_float(label_metrics.get("pain_index", 0.0)),
        "tail_cvar": _finite_float(label_metrics.get("tail_cvar", 0.0)),
    }
    payload.update(_label_payload(payload, min_label_trades))
    return payload


def label_symbol(args: tuple[str, list[dict[str, Any]], str, str, int, str, int]) -> dict[str, Any]:
    symbol, tasks, out_dir_text, spread_mode, clear_cache_every, label_scope, min_label_trades = args
    out_dir = Path(out_dir_text)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"candidate_pool_labels_{symbol}.jsonl"
    done = _load_done_ids(out_path)
    cfg = build_config(spread_mode)
    loader = DataLoader(cfg.data_dir)
    resampled_cache: dict[str, Any] = {}
    wrote = 0
    errors = 0
    skipped = 0
    started = time.time()
    with out_path.open("a", encoding="utf-8") as fh:
        for idx, task in enumerate(tasks, start=1):
            candidate_id = str(task.get("candidate_id") or "")
            if candidate_id in done:
                continue
            row = {
                "candidate_id": candidate_id,
                "symbol": symbol,
                "strategy_name": task.get("strategy_name"),
                "timeframe": task.get("timeframe"),
                "parameters": task.get("parameters") or {},
                "tuned_for_regime": task.get("tuned_for_regime"),
                "matrix_column": task.get("matrix_column"),
                "search_trials": task.get("search_trials"),
                "local_search_trials_total": task.get("local_search_trials_total"),
                "predictor_regime_scope": task.get("predictor_regime_scope"),
                "predictor_cost_basis": task.get("predictor_cost_basis"),
                **(task.get("predictors") or {}),
            }
            tuned_regime = str(task.get("tuned_for_regime") or "").upper()
            if str(label_scope or "").lower() == "regime_gated" and tuned_regime not in REGIME_VALUES:
                skipped += 1
                row.update({
                    "status": "skipped",
                    "skip_reason": "candidate has no tuned_for_regime; not a per-regime specialist label",
                    "label_scope": str(label_scope or ""),
                    "label_regime": "",
                    "full_history_return_pct": 0.0,
                    "holder_label": False,
                    "carrier_label": False,
                    "strict_carrier_label": False,
                    "trades": 0,
                    "profit_factor": 0.0,
                    "max_dd_pct": 0.0,
                })
                fh.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
                fh.flush()
                wrote += 1
                done.add(candidate_id)
                continue
            try:
                row.update(_evaluate_candidate(
                    cfg,
                    loader,
                    task,
                    resampled_cache,
                    label_scope,
                    min_label_trades,
                ))
                row["status"] = "ok"
            except Exception as exc:
                if str(exc) == "not_regime_tuned":
                    skipped += 1
                    row.update({
                        "status": "skipped",
                        "skip_reason": "candidate has no tuned_for_regime; not a per-regime specialist label",
                        "label_scope": str(label_scope or ""),
                        "full_history_return_pct": 0.0,
                        "holder_label": False,
                        "carrier_label": False,
                        "strict_carrier_label": False,
                        "trades": 0,
                        "profit_factor": 0.0,
                        "max_dd_pct": 0.0,
                    })
                else:
                    errors += 1
                    row.update({
                        "status": "error",
                        "error": str(exc),
                        "label_scope": str(label_scope or ""),
                        "full_history_return_pct": 0.0,
                        "holder_label": False,
                        "carrier_label": False,
                        "strict_carrier_label": False,
                        "trades": 0,
                        "profit_factor": 0.0,
                        "max_dd_pct": 0.0,
                    })
            fh.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
            fh.flush()
            wrote += 1
            done.add(candidate_id)
            if clear_cache_every > 0 and idx % clear_cache_every == 0:
                try:
                    FeatureComputer.clear_cache()
                except Exception:
                    pass
    return {
        "symbol": symbol,
        "requested": len(tasks),
        "existing_at_start": len(_load_done_ids(out_path)) - wrote,
        "written": wrote,
        "errors": errors,
        "skipped": skipped,
        "output": str(out_path),
        "elapsed_sec": round(time.time() - started, 2),
    }


def count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    ids: set[str] = set()
    anonymous = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except Exception:
                anonymous += 1
                continue
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id:
                ids.add(candidate_id)
            else:
                anonymous += 1
    return len(ids) + anonymous


def write_status(status_path: Path, payload: dict[str, Any]) -> None:
    tmp = status_path.with_suffix(status_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(status_path)


def combine_outputs(out_dir: Path, tasks_by_symbol: dict[str, list[dict[str, Any]]]) -> Path:
    rows_by_id: dict[str, dict[str, Any]] = {}
    anonymous_rows: list[dict[str, Any]] = []
    for symbol in sorted(tasks_by_symbol):
        path = out_dir / f"candidate_pool_labels_{symbol}.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                candidate_id = str(row.get("candidate_id") or "")
                if candidate_id:
                    rows_by_id[candidate_id] = row
                else:
                    anonymous_rows.append(row)
    rows = list(rows_by_id.values()) + anonymous_rows
    summary = {
        "created_at": datetime.now().isoformat(),
        "engine_commit": _git_head(),
        "row_count": len(rows),
        "ok_count": sum(1 for row in rows if row.get("status") == "ok"),
        "skipped_count": sum(1 for row in rows if row.get("status") == "skipped"),
        "error_count": sum(1 for row in rows if row.get("status") == "error"),
        "holder_count": sum(1 for row in rows if row.get("status") == "ok" and row.get("holder_label")),
        "carrier_count": sum(1 for row in rows if row.get("status") == "ok" and row.get("carrier_label")),
        "strict_carrier_count": sum(1 for row in rows if row.get("status") == "ok" and row.get("strict_carrier_label")),
        "falloff_count": sum(1 for row in rows if row.get("status") == "ok" and not row.get("holder_label")),
        "symbols": sorted(tasks_by_symbol.keys()),
    }
    payload = {"summary": summary, "rows": rows}
    out = out_dir / "candidate_pool_labels_combined.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(out)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Label OOS-manifest candidate pools via full-history replay.")
    parser.add_argument("--experiment", required=True, type=Path)
    parser.add_argument("--arm", default="specialist_spokes_threadspark")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--spread-mode", default="conservative_max")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--limit-per-symbol", type=int, default=0)
    parser.add_argument("--clear-cache-every", type=int, default=20)
    parser.add_argument("--poll-sec", type=float, default=15.0)
    parser.add_argument(
        "--label-scope",
        choices=("all_regimes", "regime_gated"),
        default="all_regimes",
        help="all_regimes reproduces prior research labels; regime_gated labels only tuned_for_regime trades.",
    )
    parser.add_argument("--min-label-trades", type=int, default=5)
    args = parser.parse_args()

    arm_dir = args.experiment / args.arm
    evidence_dir = arm_dir / "oos_evidence"
    out_dir = args.out_dir or (
        args.experiment / ("candidate_pool_regime_labels" if args.label_scope == "regime_gated" else "candidate_pool_labels")
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    status_path = out_dir / "candidate_pool_label_status.json"
    symbols = set(args.symbols) if args.symbols else None
    tasks_by_symbol = collect_tasks(evidence_dir, symbols=symbols)
    if args.limit_per_symbol > 0:
        tasks_by_symbol = {symbol: tasks[: args.limit_per_symbol] for symbol, tasks in tasks_by_symbol.items()}
    totals = {symbol: len(tasks) for symbol, tasks in tasks_by_symbol.items()}
    total = sum(totals.values())
    write_status(status_path, {
        "status": "starting",
        "created_at": datetime.now().isoformat(),
        "experiment": str(args.experiment),
        "arm": args.arm,
        "spread_mode": args.spread_mode,
        "workers": args.workers,
        "label_scope": args.label_scope,
        "min_label_trades": int(args.min_label_trades),
        "total_candidates": total,
        "per_symbol_total": totals,
        "per_symbol_done": {},
    })
    futures = {}
    started = time.time()
    with ProcessPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        for symbol, tasks in sorted(tasks_by_symbol.items()):
            futures[pool.submit(
                label_symbol,
                (
                    symbol,
                    tasks,
                    str(out_dir),
                    args.spread_mode,
                    int(args.clear_cache_every),
                    str(args.label_scope),
                    int(args.min_label_trades),
                ),
            )] = symbol
        while futures:
            done_futures = [future for future in futures if future.done()]
            for future in done_futures:
                symbol = futures.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"symbol": symbol, "error": str(exc)}
                result_path = out_dir / f"candidate_pool_result_{symbol}.json"
                result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
            per_done = {
                symbol: count_jsonl(out_dir / f"candidate_pool_labels_{symbol}.jsonl")
                for symbol in tasks_by_symbol
            }
            done_total = sum(per_done.values())
            write_status(status_path, {
                "status": "running" if futures else "combining",
                "updated_at": datetime.now().isoformat(),
                "experiment": str(args.experiment),
                "arm": args.arm,
                "spread_mode": args.spread_mode,
                "workers": args.workers,
                "label_scope": args.label_scope,
                "min_label_trades": int(args.min_label_trades),
                "elapsed_sec": round(time.time() - started, 2),
                "total_candidates": total,
                "done_candidates": done_total,
                "remaining_candidates": max(0, total - done_total),
                "per_symbol_total": totals,
                "per_symbol_done": per_done,
                "active_symbols": sorted(futures.values()),
            })
            if futures:
                time.sleep(max(1.0, float(args.poll_sec)))
    combined = combine_outputs(out_dir, tasks_by_symbol)
    per_done = {
        symbol: count_jsonl(out_dir / f"candidate_pool_labels_{symbol}.jsonl")
        for symbol in tasks_by_symbol
    }
    write_status(status_path, {
        "status": "complete",
        "updated_at": datetime.now().isoformat(),
        "experiment": str(args.experiment),
        "arm": args.arm,
        "spread_mode": args.spread_mode,
        "workers": args.workers,
        "label_scope": args.label_scope,
        "min_label_trades": int(args.min_label_trades),
        "elapsed_sec": round(time.time() - started, 2),
        "total_candidates": total,
        "done_candidates": sum(per_done.values()),
        "per_symbol_total": totals,
        "per_symbol_done": per_done,
        "combined_output": str(combined),
    })
    print(combined)
    print(status_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
