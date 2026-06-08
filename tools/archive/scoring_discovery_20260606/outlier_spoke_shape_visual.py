"""Generate the W4 outlier spoke-shape comparison HTML.

This is a read-only discovery artifact generator. It does not re-optimize, backtest,
or mutate PM outputs. It maps the completed W4 legacy and specialist-spokes winners
through the same integrated spoke lens so the two outlier symbols can be inspected
like-for-like.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

REPO = Path(__file__).resolve().parent.parent
if str(REPO / "tools") not in sys.path:
    sys.path.insert(0, str(REPO / "tools"))

from pbo import load_oos_manifest_matrix  # noqa: E402

EXPERIMENT_DIR = REPO / "pm_outputs" / "experiments" / "scoring_like_for_like_20260602"
BENCHMARK_DIR = EXPERIMENT_DIR / "benchmark_w4_completion"
OUTPUT_HTML = BENCHMARK_DIR / "outlier_spoke_shape_comparison.html"

SYMBOLS = ("AUDJPY", "USDJPY")
ARMS = {
    "legacy": {
        "label": "Legacy Winner",
        "ledger": EXPERIMENT_DIR / "legacy" / "pm_configs.json",
        "evidence": EXPERIMENT_DIR / "legacy" / "oos_evidence",
        "benchmark": BENCHMARK_DIR / "legacy_11.json",
        "accent": "#315f9e",
    },
    "spokes": {
        "label": "Specialist-Spokes Winner",
        "ledger": EXPERIMENT_DIR / "specialist_spokes_11" / "pm_configs.json",
        "evidence": EXPERIMENT_DIR / "specialist_spokes_11" / "oos_evidence",
        "benchmark": BENCHMARK_DIR / "specialist_spokes_11.json",
        "accent": "#b04b35",
    },
}

SPOKES = (
    "edge_survival",
    "profit_structure_quality",
    "capital_preservation",
    "tail_risk_control",
    "temporal_consistency",
    "generalization_survival",
    "execution_cost_survival",
    "parameter_stability",
)

SPOKE_LABELS = {
    "edge_survival": "Edge",
    "profit_structure_quality": "Profit structure",
    "capital_preservation": "Capital preservation",
    "tail_risk_control": "Tail control",
    "temporal_consistency": "Temporal consistency",
    "generalization_survival": "Generalization",
    "execution_cost_survival": "Cost survival",
    "parameter_stability": "Parameter stability",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        value = float(value)
        if math.isnan(value):
            return default
        return value
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return int(value)
    except Exception:
        return default


def _finite_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _finite_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_for_json(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_finite_for_json(v) for v in value.tolist()]
    if isinstance(value, (np.floating, float)):
        v = float(value)
        if math.isnan(v):
            return None
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return round(v, 6)
    if isinstance(value, (np.integer, int)):
        return int(value)
    return value


def _geo_mean(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    arr = np.clip(arr, 1e-9, 1.0)
    return float(np.exp(np.mean(np.log(arr))))


def _face_balance(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    arr = np.clip(arr, 1e-9, 1.0)
    mean = float(np.mean(arr))
    return float(_geo_mean(arr) / mean) if mean > 1e-12 else 0.0


def _execution_cost_axis(metrics: dict[str, Any]) -> tuple[float, str]:
    ret = _safe_float(metrics.get("total_return_pct", metrics.get("return_pct")), 0.0)
    gross_profit_present = "gross_profit" in metrics
    total_pnl_present = "total_pnl" in metrics
    gross_profit = max(0.0, _safe_float(metrics.get("gross_profit"), 0.0))
    total_pnl = _safe_float(metrics.get("total_pnl"), 0.0)
    if gross_profit > 1e-9:
        return max(0.0, min(1.0, total_pnl / gross_profit)), "net_to_gross_available"
    if not gross_profit_present and not total_pnl_present and ret > 0.0:
        return 1.0, "not_available_positive_return_proxy"
    return 0.0, "not_available_zero"


def _profit_factor(metrics: dict[str, Any]) -> float:
    raw = metrics.get("profit_factor", 0.0)
    if raw == "Infinity":
        return float("inf")
    return _safe_float(raw, 0.0)


def _metric(metrics: dict[str, Any], primary: str, fallback: str | None = None, default: float = 0.0) -> float:
    if primary in metrics:
        return _safe_float(metrics.get(primary), default)
    if fallback and fallback in metrics:
        return _safe_float(metrics.get(fallback), default)
    return default


def _surface_spoke_axes(
    metrics: dict[str, Any],
    *,
    drawdown_limit: float,
    train_metrics: dict[str, Any] | None = None,
    parameter_stability: float = 1.0,
) -> tuple[dict[str, float], dict[str, str]]:
    trades = max(0, _safe_int(metrics.get("total_trades", metrics.get("trades")), 0))
    ret = _metric(metrics, "total_return_pct", "return_pct")
    dd = max(0.0, _metric(metrics, "max_drawdown_pct", "max_dd_pct", 100.0))
    pf = max(0.0, _profit_factor(metrics))
    worst_5 = _safe_float(metrics.get("worst_5pct_r"), 0.0)
    max_consec = max(0, _safe_int(metrics.get("max_consecutive_losses", metrics.get("max_consec_losses")), 0))

    edge = ret / (ret + dd + 1.0) if ret > 0.0 else 0.0
    profit_structure = (1.0 - (1.0 / pf)) if pf > 1.0 else 0.0
    capital_preservation = 1.0 - (dd / max(1e-9, drawdown_limit))
    tail_risk_control = 1.0 / (1.0 + max(0.0, -worst_5))
    temporal_consistency = (
        1.0 / (1.0 + (float(max_consec) / math.sqrt(float(trades))))
        if trades > 0
        else 0.0
    )
    execution_cost_survival, cost_status = _execution_cost_axis(metrics)
    if train_metrics is None:
        generalization_survival = 1.0 if ret > 0.0 else 0.0
    else:
        train_ret = _metric(train_metrics, "total_return_pct", "return_pct")
        generalization_survival = max(0.0, min(1.0, ret / train_ret)) if train_ret > 0.0 and ret > 0.0 else 0.0

    axes = {
        "edge_survival": max(0.0, min(1.0, edge)),
        "profit_structure_quality": max(0.0, min(1.0, profit_structure)),
        "capital_preservation": max(0.0, min(1.0, capital_preservation)),
        "tail_risk_control": max(0.0, min(1.0, tail_risk_control)),
        "temporal_consistency": max(0.0, min(1.0, temporal_consistency)),
        "generalization_survival": max(0.0, min(1.0, generalization_survival)),
        "execution_cost_survival": max(0.0, min(1.0, execution_cost_survival)),
        "parameter_stability": max(0.0, min(1.0, parameter_stability)),
    }
    status = {
        "execution_cost_survival": cost_status,
        "parameter_stability": "manifest_neighborhood_available",
    }
    return axes, status


def _dent_health(train_face: np.ndarray, final_face: np.ndarray) -> float:
    ratios: list[float] = []
    for before, after in zip(train_face, final_face):
        if before > 1e-9:
            ratios.append(max(0.0, min(1.0, float(after / before))))
        else:
            ratios.append(1.0 if after <= 1e-9 else 0.0)
    return float(np.mean(ratios)) if ratios else 0.0


def _manifest_match(ledger_rec: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any] | None:
    best: tuple[int, dict[str, Any]] | None = None
    ledger_params = ledger_rec.get("parameters") or {}
    for rec in manifest.get("records", []):
        if rec.get("strategy_name") != ledger_rec.get("strategy_name"):
            continue
        if rec.get("timeframe") != ledger_rec.get("timeframe"):
            continue
        params = rec.get("parameters") or {}
        equal_count = sum(1 for key, val in ledger_params.items() if str(params.get(key)) == str(val))
        exact = all(str(params.get(key)) == str(val) for key, val in ledger_params.items())
        score = equal_count + (1000 if exact else 0)
        if best is None or score > best[0]:
            best = (score, rec)
    return best[1] if best else None


def _selected_regime(ledger_rec: dict[str, Any]) -> str:
    tf = ledger_rec.get("timeframe")
    strategy = ledger_rec.get("strategy_name")
    params = ledger_rec.get("parameters") or {}
    configs = ledger_rec.get("regime_configs") or {}
    if not isinstance(configs, dict):
        return "UNKNOWN"
    for regime, cfg in (configs.get(tf) or {}).items():
        if cfg.get("strategy_name") != strategy:
            continue
        cfg_params = cfg.get("parameters") or {}
        if all(str(cfg_params.get(key)) == str(val) for key, val in params.items()):
            return str(regime)
    return "UNKNOWN"


def _parameter_stability(manifest_rec: dict[str, Any] | None) -> tuple[float, dict[str, Any]]:
    evidence = (manifest_rec or {}).get("parameter_stability_evidence") or {}
    if isinstance(evidence, dict) and evidence.get("status") == "available":
        return max(0.0, min(1.0, _safe_float(evidence.get("nearest_neighbor_median_score_ratio"), 1.0))), evidence
    return 1.0, {"status": "not_available_neutral"}


def _historical_metrics(ledger_rec: dict[str, Any]) -> dict[str, Any]:
    hist = ledger_rec.get("historical_audit_metrics") or {}
    metrics = hist.get("metrics") if isinstance(hist, dict) else None
    return metrics if isinstance(metrics, dict) else {}


def _benchmark_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_trades": row.get("trades"),
        "profit_factor": row.get("profit_factor"),
        "total_return_pct": row.get("return_pct"),
        "max_drawdown_pct": row.get("max_dd_pct"),
        "win_rate": row.get("win_rate"),
        "pct_positive_r": row.get("pct_positive_r"),
        "max_consecutive_losses": row.get("max_consec_losses"),
        "std_weekly_return": row.get("std_weekly_return"),
    }


def _shape_features(train_axes: dict[str, float], final_axes: dict[str, float], val_trades: int, min_val_trades: int) -> dict[str, Any]:
    train_face = np.array([train_axes[name] for name in SPOKES], dtype=np.float64)
    final_face = np.array([final_axes[name] for name in SPOKES], dtype=np.float64)
    final_quality = _geo_mean(final_face)
    train_quality = _geo_mean(train_face)
    path_quality = 0.5 * (train_quality + final_quality)
    balance = _face_balance(final_face)
    dent = _dent_health(train_face, final_face)
    evidence_confidence = min(1.0, math.sqrt(max(0, val_trades) / max(1, min_val_trades * 2)))
    score = 100.0 * evidence_confidence * final_quality * path_quality * balance * dent
    ratios = []
    positive_expansions = []
    for name in SPOKES:
        before = train_axes[name]
        after = final_axes[name]
        ratio = (after / before) if before > 1e-9 else (float("inf") if after > 1e-9 else 1.0)
        ratios.append({"spoke": name, "ratio": ratio, "before": before, "after": after})
        positive_expansions.append(max(0.0, ratio - 1.0) if math.isfinite(ratio) else 1.0)
    mean_face = float(np.mean(final_face))
    max_face = float(np.max(final_face))
    min_face = float(np.min(final_face))
    return {
        "final_face_quality": final_quality,
        "train_face_quality": train_quality,
        "path_average_quality": path_quality,
        "cross_axis_balance": balance,
        "dent_health": dent,
        "evidence_confidence": evidence_confidence,
        "hypothetical_spoke_score": score,
        "max_axis_value": max_face,
        "mean_axis_value": mean_face,
        "min_axis_value": min_face,
        "positive_outpush": max(0.0, max_face - mean_face),
        "weakest_axis_gap": max(0.0, mean_face - min_face),
        "unrewarded_positive_expansion_mean": float(np.mean(positive_expansions)),
        "axis_ratios": ratios,
    }


def _compound_return_pct(block_returns: np.ndarray) -> float:
    arr = np.asarray(block_returns, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if not np.any(np.abs(arr) > 1e-12):
        return 0.0
    return float((np.cumprod(1.0 + arr)[-1] - 1.0) * 100.0)


def _drawdown_pct(block_returns: np.ndarray) -> float:
    arr = np.asarray(block_returns, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if not np.any(np.abs(arr) > 1e-12):
        return 0.0
    eq = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(eq)
    dd = np.where(peak > 0, (peak - eq) / peak, 0.0) * 100.0
    return float(np.max(dd))


def _block_profit_factor(block_returns: np.ndarray) -> float:
    arr = np.asarray(block_returns, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    gains = float(arr[arr > 0].sum())
    losses = float(-arr[arr < 0].sum())
    if losses > 1e-12:
        return gains / losses
    return float("inf") if gains > 0 else 0.0


def _oos_trace(evidence_dir: Path, symbol: str, manifest_rec: dict[str, Any] | None) -> dict[str, Any]:
    manifest_path = evidence_dir / f"{symbol}_oos_manifest.json"
    if not manifest_path.exists() or not manifest_rec:
        return {"status": "not_available"}
    matrix, manifest, candidate_ids = load_oos_manifest_matrix(manifest_path)
    candidate_id = manifest_rec.get("candidate_id")
    try:
        column = candidate_ids.index(candidate_id)
    except ValueError:
        column = _safe_int(manifest_rec.get("matrix_column"), -1)
    if column < 0 or column >= matrix.shape[1]:
        return {"status": "column_not_found", "candidate_id": candidate_id}
    series = np.asarray(matrix[:, column], dtype=np.float64)
    n = len(series)
    steps = []
    for idx in np.linspace(1, n, min(16, n), dtype=int):
        prefix = series[:idx]
        active = int((np.abs(prefix) > 1e-12).sum())
        ret = _compound_return_pct(prefix)
        dd = _drawdown_pct(prefix)
        pf = _block_profit_factor(prefix)
        pos = float((prefix[np.abs(prefix) > 1e-12] > 0).mean()) if active else 0.0
        steps.append(
            {
                "x": idx / n,
                "block": int(idx),
                "active_blocks": active,
                "return_pct": ret,
                "drawdown_pct": dd,
                "capital_preservation": max(0.0, min(1.0, 1.0 - (dd / 12.0))),
                "profit_factor": min(pf, 10.0) if math.isfinite(pf) else 10.0,
                "positive_block_fraction": pos,
            }
        )
    trade_evidence = manifest_rec.get("trade_path_evidence") or {}
    equity_evidence = manifest_rec.get("equity_path_evidence") or {}
    return {
        "status": "available",
        "candidate_id": candidate_id,
        "manifest_alignment": (manifest.get("matrix_contract") or {}).get("alignment"),
        "period_count": int(matrix.shape[0]),
        "record_metrics": manifest_rec.get("metrics") or {},
        "generalization_metrics": manifest_rec.get("generalization_metrics") or {},
        "trade_path_summary": {
            key: trade_evidence.get(key)
            for key in (
                "status",
                "trade_count",
                "stored_points",
                "mean_r",
                "median_r",
                "positive_trade_fraction",
                "worst_trade_r",
                "worst_cluster_r",
                "late_gain_share",
            )
        },
        "equity_path_summary": {
            key: equity_evidence.get(key)
            for key in ("status", "stored_points", "max_drawdown_pct", "time_underwater_fraction")
        },
        "steps": steps,
    }


def _load_benchmark_rows(path: Path) -> dict[str, dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {row["symbol"]: row for row in data.get("rows", []) if isinstance(row, dict) and row.get("symbol")}


def _stage_faces(
    ledger_rec: dict[str, Any],
    benchmark_row: dict[str, Any],
    stability: float,
    min_val_trades: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    train_metrics = ledger_rec.get("train_metrics") or {}
    stages = [
        ("Train", "selection", train_metrics, 20.0, None),
        ("Validation", "selection", ledger_rec.get("val_metrics") or {}, 12.0, train_metrics),
        ("Holdout", "later", ledger_rec.get("holdout_metrics") or {}, 12.0, train_metrics),
        ("Historical audit", "later", _historical_metrics(ledger_rec), 12.0, train_metrics),
        ("Benchmark replay", "later", _benchmark_metrics(benchmark_row), 12.0, train_metrics),
    ]
    faces = []
    for name, group, metrics, boundary, train_ref in stages:
        axes, statuses = _surface_spoke_axes(
            metrics,
            drawdown_limit=boundary,
            train_metrics=train_ref,
            parameter_stability=stability,
        )
        faces.append(
            {
                "name": name,
                "group": group,
                "drawdown_limit": boundary,
                "spokes": axes,
                "statuses": statuses,
                "metrics": {
                    "trades": metrics.get("total_trades", metrics.get("trades")),
                    "return_pct": metrics.get("total_return_pct", metrics.get("return_pct")),
                    "profit_factor": metrics.get("profit_factor"),
                    "max_drawdown_pct": metrics.get("max_drawdown_pct", metrics.get("max_dd_pct")),
                    "win_rate": metrics.get("win_rate"),
                    "gross_profit": metrics.get("gross_profit"),
                    "gross_loss": metrics.get("gross_loss"),
                    "total_pnl": metrics.get("total_pnl"),
                },
            }
        )
    selection = _shape_features(
        faces[0]["spokes"],
        faces[1]["spokes"],
        _safe_int((ledger_rec.get("val_metrics") or {}).get("total_trades")),
        min_val_trades,
    )
    return faces, selection


def _build_payload() -> dict[str, Any]:
    config = json.loads((EXPERIMENT_DIR / "config.specialist_spokes_11.json").read_text(encoding="utf-8"))
    pipeline = config.get("pipeline") or {}
    min_val_trades = _safe_int(pipeline.get("regime_min_val_trades"), 15)

    arm_ledgers = {
        arm: json.loads(meta["ledger"].read_text(encoding="utf-8"))
        for arm, meta in ARMS.items()
    }
    arm_benchmarks = {
        arm: _load_benchmark_rows(meta["benchmark"])
        for arm, meta in ARMS.items()
    }

    symbols: dict[str, Any] = {}
    for symbol in SYMBOLS:
        arms: dict[str, Any] = {}
        for arm, meta in ARMS.items():
            ledger_rec = arm_ledgers[arm][symbol]
            benchmark_row = arm_benchmarks[arm][symbol]
            manifest_path = meta["evidence"] / f"{symbol}_oos_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_rec = _manifest_match(ledger_rec, manifest)
            stability, stability_evidence = _parameter_stability(manifest_rec)
            faces, selection = _stage_faces(ledger_rec, benchmark_row, stability, min_val_trades)
            hist = ledger_rec.get("historical_audit_metrics") or {}
            arms[arm] = {
                "label": meta["label"],
                "accent": meta["accent"],
                "symbol": symbol,
                "strategy_name": ledger_rec.get("strategy_name"),
                "timeframe": ledger_rec.get("timeframe"),
                "regime": _selected_regime(ledger_rec),
                "parameters": ledger_rec.get("parameters") or {},
                "quality_score_recorded": ledger_rec.get("composite_score"),
                "validation_reason": ledger_rec.get("validation_reason"),
                "manifest_candidate_id": (manifest_rec or {}).get("candidate_id"),
                "manifest_surface": (manifest_rec or {}).get("oos_surface"),
                "parameter_stability_evidence": stability_evidence,
                "historical_severe_failure": hist.get("severe_failure"),
                "historical_failure_reasons": hist.get("failure_reasons") or [],
                "benchmark": benchmark_row,
                "faces": faces,
                "selection_shape": selection,
                "oos_trace": _oos_trace(meta["evidence"], symbol, manifest_rec),
            }
        symbols[symbol] = {"arms": arms}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "experiment_dir": str(EXPERIMENT_DIR.relative_to(REPO)),
            "benchmark_dir": str(BENCHMARK_DIR.relative_to(REPO)),
            "scorer_formula": "pm_pipeline.py::_compute_specialist_spoke_evidence",
            "note": "No optimization or backtest was rerun; this page uses completed W4 ledgers, OOS manifests, and benchmark summaries.",
        },
        "config": {
            "regime_min_val_trades": min_val_trades,
            "train_max_drawdown": _safe_float(pipeline.get("train_max_drawdown"), 20.0),
            "fx_val_max_drawdown": _safe_float(pipeline.get("fx_val_max_drawdown"), 12.0),
            "spokes": list(SPOKES),
            "spoke_labels": SPOKE_LABELS,
        },
        "symbols": symbols,
    }


def _html_template(payload: dict[str, Any]) -> str:
    data_json = json.dumps(_finite_for_json(payload), indent=2)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>W4 Outlier Spoke Shape Comparison</title>
  <style>
    :root {{
      --ink: #17202a;
      --muted: #5b6574;
      --panel: #ffffff;
      --line: #d7dde7;
      --soft: #f5f7fb;
      --legacy: #315f9e;
      --spokes: #b04b35;
      --good: #23745b;
      --bad: #a33232;
      --warn: #8a6400;
      --stage-train: #687385;
      --stage-val: #147d80;
      --stage-holdout: #c1841f;
      --stage-hist: #7b5aa6;
      --stage-bench: #b43f3f;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background: linear-gradient(180deg, #f8fafc 0%, #eef3f8 100%);
      letter-spacing: 0;
    }}
    main {{ max-width: 1440px; margin: 0 auto; padding: 28px; }}
    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.5fr) minmax(360px, 0.8fr);
      gap: 18px;
      align-items: stretch;
      margin-bottom: 18px;
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(28px, 3.2vw, 50px); line-height: 1.02; }}
    .hero p {{ margin: 0; max-width: 900px; color: var(--muted); font-size: 16px; line-height: 1.55; }}
    .panel {{
      background: rgba(255,255,255,.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 16px 35px rgba(23, 32, 42, .08);
    }}
    .hero .panel {{ padding: 18px; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 16px; }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      color: #344053;
      background: #fff;
      font-size: 13px;
      white-space: nowrap;
    }}
    .scoreboard {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .scorebox {{ padding: 12px; background: var(--soft); border: 1px solid var(--line); border-radius: 8px; }}
    .scorebox .label {{ color: var(--muted); font-size: 12px; }}
    .scorebox .value {{ font-size: 24px; font-weight: 780; margin-top: 2px; }}
    .scorebox .value.good {{ color: var(--good); }}
    .scorebox .value.bad {{ color: var(--bad); }}
    section.symbol {{ margin-top: 20px; }}
    .symbol-head {{
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 16px;
      margin: 26px 0 12px;
    }}
    .symbol-head h2 {{ margin: 0; font-size: 30px; }}
    .symbol-head p {{ margin: 4px 0 0; color: var(--muted); }}
    .comparison {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }}
    .arm-card {{ padding: 16px; overflow: hidden; }}
    .arm-title {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      margin-bottom: 14px;
    }}
    .arm-title h3 {{ margin: 0; font-size: 20px; }}
    .arm-title .meta {{ color: var(--muted); font-size: 13px; margin-top: 4px; line-height: 1.35; }}
    .badge {{ border-radius: 999px; padding: 5px 8px; color: #fff; font-weight: 700; font-size: 12px; white-space: nowrap; }}
    .badge.legacy {{ background: var(--legacy); }}
    .badge.spokes {{ background: var(--spokes); }}
    .metrics-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 14px; }}
    .metric {{ border: 1px solid var(--line); background: #fbfcfe; border-radius: 8px; padding: 9px; min-height: 64px; }}
    .metric .k {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .04em; }}
    .metric .v {{ font-size: 18px; font-weight: 760; margin-top: 4px; }}
    .charts {{
      display: grid;
      grid-template-columns: minmax(250px, .8fr) minmax(340px, 1.2fr);
      gap: 12px;
      align-items: stretch;
    }}
    .chart-panel {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fff;
      min-width: 0;
    }}
    .chart-panel h4 {{ margin: 0 0 8px; font-size: 14px; }}
    .chart-panel .note {{ color: var(--muted); font-size: 12px; line-height: 1.4; margin-top: 6px; }}
    .svg-wrap {{ width: 100%; overflow: hidden; }}
    svg {{ display: block; max-width: 100%; height: auto; }}
    .stage-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 6px; margin-top: 10px; }}
    .stage-cell {{ border: 1px solid var(--line); border-radius: 8px; padding: 7px; background: #fbfcfe; }}
    .stage-cell .stage {{ font-size: 11px; color: var(--muted); }}
    .stage-cell .ret {{ font-weight: 760; margin-top: 2px; }}
    .stage-cell .pf {{ color: var(--muted); font-size: 12px; }}
    .diagnostics {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 12px;
    }}
    .diag-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .diag-table th, .diag-table td {{ border-bottom: 1px solid var(--line); padding: 7px 6px; text-align: left; }}
    .diag-table th {{ color: var(--muted); font-weight: 650; }}
    .callout {{
      border-left: 4px solid var(--warn);
      background: #fffaf0;
      color: #3b2a00;
      padding: 10px 12px;
      border-radius: 8px;
      font-size: 13px;
      line-height: 1.45;
    }}
    .callout.bad {{ border-left-color: var(--bad); background: #fff5f5; color: #4a1515; }}
    .callout.good {{ border-left-color: var(--good); background: #f2fbf7; color: #103f30; }}
    .cross-note {{
      margin-top: 14px;
      padding: 14px;
      background: #17202a;
      color: #f2f5f8;
      border-radius: 8px;
      line-height: 1.5;
    }}
    .cross-note strong {{ color: #ffffff; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; color: var(--muted); font-size: 12px; }}
    .legend span {{ display: inline-flex; align-items: center; gap: 6px; }}
    .swatch {{ width: 10px; height: 10px; border-radius: 3px; display: inline-block; }}
    .footer {{
      margin: 28px 0 4px;
      padding: 14px;
      color: var(--muted);
      line-height: 1.5;
      font-size: 13px;
    }}
    .tooltip {{
      position: fixed;
      z-index: 20;
      pointer-events: none;
      background: rgba(23,32,42,.95);
      color: #fff;
      padding: 7px 9px;
      border-radius: 8px;
      font-size: 12px;
      max-width: 280px;
      box-shadow: 0 12px 28px rgba(0,0,0,.18);
      opacity: 0;
      transform: translate(-50%, -115%);
      transition: opacity .12s ease;
    }}
    @media (max-width: 1100px) {{
      main {{ padding: 18px; }}
      .hero, .comparison, .charts, .diagnostics {{ grid-template-columns: 1fr; }}
      .metrics-row {{ grid-template-columns: repeat(2, 1fr); }}
      .stage-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
    @media (max-width: 620px) {{
      main {{ padding: 12px; }}
      .hero h1 {{ font-size: 32px; }}
      .metrics-row, .scoreboard {{ grid-template-columns: 1fr; }}
      .symbol-head {{ display: block; }}
      .arm-title {{ display: block; }}
      .badge {{ display: inline-block; margin-top: 8px; }}
    }}
  </style>
</head>
<body>
<main>
  <div class="hero">
    <div class="panel">
      <h1>Outlier Spoke Shape Cross-Section</h1>
      <p>
        Legacy winners are mapped through the same W4 specialist-spoke lens and compared against the
        actual specialist-spokes winners for AUDJPY and USDJPY. The diagnostic separates the
        selection-time face from later holdout, historical, benchmark, and OOS-matrix evidence.
      </p>
      <div class="chips">
        <span class="chip">No re-optimization</span>
        <span class="chip">Existing W4 artifacts only</span>
        <span class="chip">8-spoke current scorer lens</span>
        <span class="chip">Capital preservation is higher-is-better</span>
      </div>
    </div>
    <div class="panel">
      <div class="scoreboard" id="topScoreboard"></div>
    </div>
  </div>
  <div id="content"></div>
  <div class="footer panel">
    Source caveat: selected-regime ledger evidence and manifest OOS matrix evidence are not the same
    surface. The stacked diamond uses ledger faces for Train/Validation/Holdout/Historical/Benchmark.
    The OOS path line uses the manifest matrix column for the matched candidate and is labelled separately.
    Benchmark replay is the completed W4 benchmark summary, not a fresh run.
  </div>
</main>
<div class="tooltip" id="tooltip"></div>
<script>
const DATA = {data_json};
const AXES = DATA.config.spokes;
const LABELS = DATA.config.spoke_labels;
const STAGE_COLORS = {{
  "Train": "#687385",
  "Validation": "#147d80",
  "Holdout": "#c1841f",
  "Historical audit": "#7b5aa6",
  "Benchmark replay": "#b43f3f"
}};

function val(x, digits=2) {{
  if (x === null || x === undefined) return "n/a";
  if (x === "Infinity") return "∞";
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return n.toFixed(digits);
}}
function pct(x, digits=2) {{
  if (x === null || x === undefined) return "n/a";
  if (x === "Infinity") return "∞";
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return `${{n.toFixed(digits)}}%`;
}}
function clsGoodBad(x) {{
  const n = Number(x);
  if (!Number.isFinite(n)) return "";
  return n >= 0 ? "good" : "bad";
}}
function spokeArray(face) {{ return AXES.map(a => Number(face.spokes[a] ?? 0)); }}
function polar(cx, cy, r, i, n) {{
  const angle = -Math.PI / 2 + (Math.PI * 2 * i / n);
  return [cx + Math.cos(angle) * r, cy + Math.sin(angle) * r];
}}
function polyPoints(values, cx, cy, r) {{
  return values.map((v, i) => {{
    const [x,y] = polar(cx, cy, r * Math.max(0, Math.min(1, Number(v)||0)), i, values.length);
    return `${{x.toFixed(2)}},${{y.toFixed(2)}}`;
  }}).join(" ");
}}
function showTip(evt, html) {{
  const tip = document.getElementById("tooltip");
  tip.innerHTML = html;
  tip.style.left = `${{evt.clientX}}px`;
  tip.style.top = `${{evt.clientY}}px`;
  tip.style.opacity = 1;
}}
function hideTip() {{ document.getElementById("tooltip").style.opacity = 0; }}

function radarSvg(series, size=300) {{
  const cx = size / 2, cy = size / 2, r = size * 0.35;
  const n = AXES.length;
  let out = `<svg viewBox="0 0 ${{size}} ${{size}}" role="img">`;
  [0.25,0.5,0.75,1].forEach(level => {{
    const pts = Array.from({{length:n}}, (_,i) => polar(cx,cy,r*level,i,n).map(z=>z.toFixed(2)).join(",")).join(" ");
    out += `<polygon points="${{pts}}" fill="none" stroke="#d7dde7" stroke-width="1"/>`;
  }});
  AXES.forEach((axis,i) => {{
    const [x,y] = polar(cx,cy,r,i,n);
    const [lx,ly] = polar(cx,cy,r+28,i,n);
    out += `<line x1="${{cx}}" y1="${{cy}}" x2="${{x}}" y2="${{y}}" stroke="#d7dde7" stroke-width="1"/>`;
    out += `<text x="${{lx}}" y="${{ly}}" text-anchor="middle" dominant-baseline="middle" fill="#4d5968" font-size="9">${{LABELS[axis]}}</text>`;
  }});
  series.forEach((s, idx) => {{
    const pts = polyPoints(s.values, cx, cy, r);
    const dash = s.dash ? `stroke-dasharray="${{s.dash}}"` : "";
    out += `<polygon points="${{pts}}" fill="${{s.color}}" fill-opacity="${{s.fill ?? 0.12}}" stroke="${{s.color}}" stroke-width="${{s.width ?? 2}}" ${{dash}}/>`;
  }});
  out += `</svg>`;
  return out;
}}

function stackedDiamondSvg(faces, accent) {{
  const w = 520, h = 420, cx = w/2, r = 42;
  const y0 = 54, gap = 78, n = AXES.length;
  let out = `<svg viewBox="0 0 ${{w}} ${{h}}" role="img">`;
  out += `<line x1="${{cx}}" y1="${{y0-28}}" x2="${{cx}}" y2="${{y0+gap*(faces.length-1)+34}}" stroke="#aeb7c4" stroke-width="2" stroke-dasharray="5 6"/>`;
  out += `<text x="${{cx+8}}" y="${{y0-32}}" fill="#5b6574" font-size="11">evidence progression</text>`;
  AXES.forEach((axis,i) => {{
    for (let j=0; j<faces.length-1; j++) {{
      const a = spokeArray(faces[j])[i], b = spokeArray(faces[j+1])[i];
      const [x1,y1] = polar(cx, y0 + gap*j, r*a, i, n);
      const [x2,y2] = polar(cx, y0 + gap*(j+1), r*b, i, n);
      out += `<line x1="${{x1.toFixed(1)}}" y1="${{y1.toFixed(1)}}" x2="${{x2.toFixed(1)}}" y2="${{y2.toFixed(1)}}" stroke="${{accent}}" stroke-opacity=".16" stroke-width="1"/>`;
    }}
  }});
  faces.forEach((face, j) => {{
    const cy = y0 + gap*j;
    const values = spokeArray(face);
    const pts = polyPoints(values, cx, cy, r);
    const color = STAGE_COLORS[face.name] || accent;
    out += `<polygon points="${{pts}}" fill="${{color}}" fill-opacity=".12" stroke="${{color}}" stroke-width="2"/>`;
    out += `<circle cx="${{cx}}" cy="${{cy}}" r="2.3" fill="#17202a"/>`;
    out += `<text x="16" y="${{cy-4}}" fill="#17202a" font-size="12" font-weight="700">${{face.name}}</text>`;
    out += `<text x="16" y="${{cy+12}}" fill="#5b6574" font-size="11">ret ${{pct(face.metrics.return_pct, 1)}} · PF ${{val(face.metrics.profit_factor, 2)}} · DD ${{pct(face.metrics.max_drawdown_pct, 1)}}</text>`;
  }});
  out += `</svg>`;
  return out;
}}

function lineChart(trace, accent) {{
  if (!trace || trace.status !== "available" || !trace.steps || !trace.steps.length) {{
    return `<div class="note">No manifest path trace available.</div>`;
  }}
  const w = 520, h = 220, ml = 42, mr = 14, mt = 18, mb = 30;
  const xs = trace.steps.map(d => Number(d.x));
  const returns = trace.steps.map(d => Number(d.return_pct));
  const caps = trace.steps.map(d => Number(d.capital_preservation) * 100);
  const minY = Math.min(...returns, 0, ...caps);
  const maxY = Math.max(...returns, 100, ...caps);
  const x = v => ml + v * (w - ml - mr);
  const y = v => mt + (maxY - v) / Math.max(1e-9, maxY - minY) * (h - mt - mb);
  const path = arr => trace.steps.map((d,i) => `${{i?'L':'M'}}${{x(d.x).toFixed(1)}} ${{y(arr[i]).toFixed(1)}}`).join(" ");
  let out = `<svg viewBox="0 0 ${{w}} ${{h}}" role="img">`;
  out += `<rect x="${{ml}}" y="${{mt}}" width="${{w-ml-mr}}" height="${{h-mt-mb}}" fill="#fbfcfe" stroke="#d7dde7"/>`;
  [0,50,100].forEach(t => {{
    if (t >= minY && t <= maxY) out += `<line x1="${{ml}}" x2="${{w-mr}}" y1="${{y(t)}}" y2="${{y(t)}}" stroke="#d7dde7"/><text x="8" y="${{y(t)+4}}" font-size="10" fill="#5b6574">${{t}}</text>`;
  }});
  if (minY < 0 && maxY > 0) out += `<line x1="${{ml}}" x2="${{w-mr}}" y1="${{y(0)}}" y2="${{y(0)}}" stroke="#a33232" stroke-dasharray="4 4"/>`;
  out += `<path d="${{path(returns)}}" fill="none" stroke="${{accent}}" stroke-width="2.5"/>`;
  out += `<path d="${{path(caps)}}" fill="none" stroke="#23745b" stroke-width="2" stroke-dasharray="5 5"/>`;
  trace.steps.forEach((d,i) => {{
    out += `<circle cx="${{x(d.x)}}" cy="${{y(returns[i])}}" r="3" fill="${{accent}}" data-tip="block ${{d.block}} · return ${{pct(d.return_pct,2)}} · active blocks ${{d.active_blocks}}"/>`;
  }});
  out += `<text x="${{ml}}" y="${{h-8}}" fill="#5b6574" font-size="11">OOS manifest progress →</text>`;
  out += `<text x="${{w-170}}" y="14" fill="${{accent}}" font-size="11">cumulative return %</text>`;
  out += `<text x="${{w-170}}" y="30" fill="#23745b" font-size="11">capital preservation %</text>`;
  out += `</svg>`;
  return out;
}}

function stageCells(faces) {{
  return faces.map(face => `
    <div class="stage-cell">
      <div class="stage">${{face.name}}</div>
      <div class="ret ${{clsGoodBad(face.metrics.return_pct)}}">${{pct(face.metrics.return_pct, 2)}}</div>
      <div class="pf">PF ${{val(face.metrics.profit_factor, 2)}} · DD ${{pct(face.metrics.max_drawdown_pct, 2)}}</div>
    </div>
  `).join("");
}}

function diagnosticsTable(arm) {{
  const s = arm.selection_shape;
  const rows = [
    ["Hypothetical spoke score", s.hypothetical_spoke_score],
    ["Final face quality", s.final_face_quality],
    ["Path average quality", s.path_average_quality],
    ["Cross-axis balance", s.cross_axis_balance],
    ["Dent health", s.dent_health],
    ["Positive outpush", s.positive_outpush],
    ["Unrewarded expansion avg", s.unrewarded_positive_expansion_mean],
    ["Parameter stability", arm.faces[1].spokes.parameter_stability],
  ];
  return `<table class="diag-table"><tbody>${{rows.map(r => `<tr><th>${{r[0]}}</th><td>${{val(r[1], 3)}}</td></tr>`).join("")}}</tbody></table>`;
}}

function axisRatioTable(arm) {{
  const rows = arm.selection_shape.axis_ratios
    .slice()
    .sort((a,b) => Number(a.ratio ?? 0) - Number(b.ratio ?? 0));
  return `<table class="diag-table"><thead><tr><th>Axis</th><th>Train</th><th>Val</th><th>Val/train</th></tr></thead><tbody>
    ${{rows.map(r => `<tr><th>${{LABELS[r.spoke]}}</th><td>${{val(r.before,3)}}</td><td>${{val(r.after,3)}}</td><td>${{r.ratio === "Infinity" ? "∞" : val(r.ratio,2)}}</td></tr>`).join("")}}
  </tbody></table>`;
}}

function armCard(symbol, key, arm) {{
  const bench = arm.benchmark;
  const faces = arm.faces;
  const selectionSeries = [
    {{label:"Train", values: spokeArray(faces[0]), color:"#687385", fill:.07, dash:"4 4", width:1.5}},
    {{label:"Validation", values: spokeArray(faces[1]), color: arm.accent, fill:.16, width:2.5}}
  ];
  const laterSeries = [
    {{label:"Validation", values: spokeArray(faces[1]), color:"#147d80", fill:.04, dash:"4 4", width:1.3}},
    {{label:"Holdout", values: spokeArray(faces[2]), color:"#c1841f", fill:.10, width:2}},
    {{label:"Historical", values: spokeArray(faces[3]), color:"#7b5aa6", fill:.10, width:2}},
    {{label:"Benchmark", values: spokeArray(faces[4]), color:"#b43f3f", fill:.12, width:2.5}},
  ];
  const severe = arm.historical_severe_failure ? `<div class="callout bad">Historical audit severe failure: ${{(arm.historical_failure_reasons || []).join("; ")}}</div>` : "";
  const armClass = key === "legacy" ? "legacy" : "spokes";
  return `
  <article class="arm-card panel">
    <div class="arm-title">
      <div>
        <h3>${{arm.strategy_name}}</h3>
        <div class="meta">${{arm.label}} · ${{arm.timeframe}} · regime ${{arm.regime}} · candidate ${{(arm.manifest_candidate_id || "").slice(0,10)}}…</div>
      </div>
      <span class="badge ${{armClass}}">${{key === "legacy" ? "Legacy mapped" : "Spokes actual"}}</span>
    </div>
    <div class="metrics-row">
      <div class="metric"><div class="k">Benchmark return</div><div class="v ${{clsGoodBad(bench.return_pct)}}">${{pct(bench.return_pct,2)}}</div></div>
      <div class="metric"><div class="k">Benchmark PF</div><div class="v">${{val(bench.profit_factor,2)}}</div></div>
      <div class="metric"><div class="k">Benchmark DD</div><div class="v ${{Number(bench.max_dd_pct) > 20 ? 'bad' : ''}}">${{pct(bench.max_dd_pct,2)}}</div></div>
      <div class="metric"><div class="k">Spoke score at selection</div><div class="v">${{val(arm.selection_shape.hypothetical_spoke_score,2)}}</div></div>
    </div>
    <div class="charts">
      <div class="chart-panel">
        <h4>Selection face: train → validation</h4>
        <div class="svg-wrap">${{radarSvg(selectionSeries, 330)}}</div>
        <div class="legend">
          <span><i class="swatch" style="background:#687385"></i>Train</span>
          <span><i class="swatch" style="background:${{arm.accent}}"></i>Validation</span>
        </div>
      </div>
      <div class="chart-panel">
        <h4>Evidence diamond stack</h4>
        <div class="svg-wrap">${{stackedDiamondSvg(faces, arm.accent)}}</div>
      </div>
      <div class="chart-panel">
        <h4>Later evidence overlay</h4>
        <div class="svg-wrap">${{radarSvg(laterSeries, 330)}}</div>
        <div class="legend">
          <span><i class="swatch" style="background:#147d80"></i>Validation</span>
          <span><i class="swatch" style="background:#c1841f"></i>Holdout</span>
          <span><i class="swatch" style="background:#7b5aa6"></i>Historical</span>
          <span><i class="swatch" style="background:#b43f3f"></i>Benchmark</span>
        </div>
      </div>
      <div class="chart-panel">
        <h4>OOS manifest path trace</h4>
        <div class="svg-wrap">${{lineChart(arm.oos_trace, arm.accent)}}</div>
        <div class="note">Matrix path is the matched candidate column from the OOS manifest; it is not the selected-regime ledger face.</div>
      </div>
    </div>
    <div class="stage-grid">${{stageCells(faces)}}</div>
    <div class="diagnostics">
      <div class="chart-panel"><h4>Selection score components</h4>${{diagnosticsTable(arm)}}</div>
      <div class="chart-panel"><h4>Axis expansion / dents</h4>${{axisRatioTable(arm)}}</div>
    </div>
    ${{severe}}
  </article>`;
}}

function symbolNarrative(symbol, obj) {{
  if (symbol === "USDJPY") {{
    return `<div class="cross-note"><strong>USDJPY diagnostic:</strong> this is the strongest evidence for Bongo's concern. Legacy has a lopsided/low-stability shape and would be heavily demoted by the spoke score, but benchmark replay is +64.68%. The spokes pick has a cleaner validation diamond and much higher parameter-stability evidence, but later evidence goes negative. This points to shape-conformity and missing tail/forward safeguards, not to the geometry idea being dead.</div>`;
  }}
  return `<div class="cross-note"><strong>AUDJPY diagnostic:</strong> the spokes pick wins the selection face but collapses after validation. Its holdout, historical, and benchmark faces expose the dent that the train→validation scorer did not see strongly enough. This is a later-evidence/tail-control failure rather than a simple balance-only issue.</div>`;
}}

function render() {{
  const top = document.getElementById("topScoreboard");
  const usd = DATA.symbols.USDJPY.arms;
  const aud = DATA.symbols.AUDJPY.arms;
  top.innerHTML = `
    <div class="scorebox"><div class="label">USDJPY benchmark spread</div><div class="value">+64.68 vs -7.40</div></div>
    <div class="scorebox"><div class="label">USDJPY spoke score</div><div class="value">26.89 vs 68.46</div></div>
    <div class="scorebox"><div class="label">AUDJPY benchmark spread</div><div class="value bad">-28.68 vs -63.92</div></div>
    <div class="scorebox"><div class="label">AUDJPY spoke score</div><div class="value">57.62 vs 68.14</div></div>
  `;
  const content = document.getElementById("content");
  content.innerHTML = Object.entries(DATA.symbols).map(([symbol, obj]) => `
    <section class="symbol">
      <div class="symbol-head">
        <div><h2>${{symbol}}</h2><p>Legacy winner mapped through current W4 spokes vs actual specialist-spokes winner.</p></div>
      </div>
      <div class="comparison">
        ${{armCard(symbol, "legacy", obj.arms.legacy)}}
        ${{armCard(symbol, "spokes", obj.arms.spokes)}}
      </div>
      ${{symbolNarrative(symbol, obj)}}
    </section>
  `).join("");
  document.querySelectorAll("[data-tip]").forEach(el => {{
    el.addEventListener("mousemove", e => showTip(e, el.getAttribute("data-tip")));
    el.addEventListener("mouseleave", hideTip);
  }});
}}
render();
</script>
</body>
</html>
"""


def main() -> int:
    payload = _build_payload()
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(_html_template(payload), encoding="utf-8")
    print(f"wrote {OUTPUT_HTML.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
