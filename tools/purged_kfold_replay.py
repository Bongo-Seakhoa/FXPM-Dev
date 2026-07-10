"""
Bounded purged/embargoed k-fold OOS replay for completed discovery specialists.

This is a representative post-run replay, not a full optimizer rerun. It evaluates
stored selected configs on the opt-in purged-kfold OOS surface and compares the
result to the fixed-holdout fields already present in a promotion review artifact.
"""

import argparse
import json
import sys
import time
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from benchmark import _git_head
from pm_core import FeatureComputer, PipelineConfig
from pm_pipeline import OptimizationPipeline, RegimeConfig


PROMOTE_ANCHORS = {
    ("EURAUD", "H4", "CHOP", "TimeOfDayMomentumStrategy"),
    ("USDJPY", "H4", "RANGE", "EMAPullbackContinuationStrategy"),
}


def _load_pipeline_config(config_path: Path, *, splits: int, embargo_bars: int, min_trades: int) -> PipelineConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    pipe = payload.get("pipeline", {}) if isinstance(payload, dict) else {}
    allowed = {field.name for field in fields(PipelineConfig)}
    kwargs = {key: value for key, value in pipe.items() if key in allowed}
    cfg = PipelineConfig(**kwargs)
    cfg.data_dir = REPO / str(getattr(cfg, "data_dir", "./data"))
    cfg.output_dir = REPO / "pm_outputs" / "experiments"
    cfg.log_dir = REPO / "logs"
    cfg.regime_params_file = str(REPO / str(getattr(cfg, "regime_params_file", "regime_params.json")))
    cfg.oos_validation_mode = "purged_kfold"
    cfg.clean_holdout_pct = 0.0
    cfg.oos_kfold_splits = int(splits)
    cfg.oos_embargo_bars = int(embargo_bars)
    cfg.oos_min_trades = int(min_trades)
    cfg.oos_aggregate_policy = "aggregate_with_worst_fold_floor"
    cfg.feature_disk_cache_enabled = False
    cfg.feature_cache_max_entries = 4
    return PipelineConfig(**{field.name: getattr(cfg, field.name) for field in fields(PipelineConfig)})


def _row_key(row: Dict[str, Any]) -> tuple:
    return (
        str(row.get("symbol", "") or ""),
        str(row.get("timeframe", "") or ""),
        str(row.get("regime", "") or ""),
        str(row.get("strategy_name", "") or ""),
    )


def _is_fx(row: Dict[str, Any]) -> bool:
    return str(row.get("asset_class", "") or "").lower() == "fx" or str(row.get("provenance", "")) == "fx_historical_spread"


def _as_float(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key))
    except (TypeError, ValueError):
        return default


def _as_int(row: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(float(row.get(key, default) or default))
    except (TypeError, ValueError):
        return default


def _has_float(row: Dict[str, Any], key: str) -> bool:
    try:
        float(row.get(key))
    except (TypeError, ValueError):
        return False
    return row.get(key) is not None


def _non_holdout_gates_clean(row: Dict[str, Any]) -> bool:
    codes = {str(code) for code in (row.get("gate_failure_codes") or []) if str(code)}
    if any(not code.startswith("holdout_") for code in codes):
        return False
    if str(row.get("promotion_status") or "").upper().startswith("BLOCK"):
        return False
    val_trades = _as_int(row, "val_trades")
    if val_trades >= 5 and (
        (_has_float(row, "val_pf") and _as_float(row, "val_pf") < 1.0)
        or (_has_float(row, "val_return_pct") and _as_float(row, "val_return_pct") < 0.0)
    ):
        return False
    historical_trades = _as_int(row, "historical_trades")
    if historical_trades >= 5 and (
        (_has_float(row, "historical_pf") and _as_float(row, "historical_pf") < 1.0)
        or (_has_float(row, "historical_return_pct") and _as_float(row, "historical_return_pct") < 0.0)
        or (_has_float(row, "historical_max_dd_pct") and _as_float(row, "historical_max_dd_pct") > 40.0)
    ):
        return False
    return (
        str(row.get("stress")) == "SURVIVE"
        and str(row.get("historical_status")) == "PASS"
        and row.get("pbo_timeframe") is not None
        and float(row.get("pbo_timeframe") or 0.0) <= 0.5
    )


def _is_clean_promote(row: Dict[str, Any]) -> bool:
    return str(row.get("final_verdict", "")) == "PROMOTE" and _non_holdout_gates_clean(row)


def _first_matching(rows: Iterable[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    for row in rows:
        try:
            if predicate(row):
                return row
        except Exception:
            continue
    return None


def select_representative_rows(rows: List[Dict[str, Any]], max_rows: int) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    selected_keys = set()

    def add(row: Optional[Dict[str, Any]]) -> None:
        if not row:
            return
        key = _row_key(row)
        if key in selected_keys:
            return
        selected.append(row)
        selected_keys.add(key)

    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("symbol", "") or ""),
            str(row.get("timeframe", "") or ""),
            str(row.get("regime", "") or ""),
            str(row.get("strategy_name", "") or ""),
        ),
    )

    for row in ordered:
        if _row_key(row) in PROMOTE_ANCHORS or _is_clean_promote(row):
            add(row)

    criteria = [
        lambda row: str(row.get("final_verdict", "")) == "REVIEW" and row.get("holdout_status") == "INSUFFICIENT" and _is_fx(row),
        lambda row: row.get("pbo_timeframe") is not None and float(row.get("pbo_timeframe") or 0.0) > 0.5 and str(row.get("stress")) == "SURVIVE",
        lambda row: str(row.get("timeframe")) in {"H4", "D1"} and row.get("holdout_status") == "INSUFFICIENT" and int(row.get("holdout_trades", 0) or 0) <= 2,
        lambda row: str(row.get("timeframe")) in {"M5", "M15", "M30"} and int(row.get("val_trades", 0) or 0) >= 30 and _is_fx(row),
        lambda row: not _is_fx(row) and row.get("holdout_status") in {"INSUFFICIENT", "FAIL"},
        lambda row: str(row.get("stress")) == "SURVIVE" and row.get("holdout_status") == "INSUFFICIENT",
    ]
    for predicate in criteria:
        add(_first_matching(ordered, predicate))
        if len(selected) >= max_rows:
            return selected[:max_rows]

    for row in ordered:
        if len(selected) >= max_rows:
            break
        add(row)
    return selected[:max_rows]


def _stage2_features_for_row(pipeline: OptimizationPipeline,
                             symbol: str,
                             timeframe: str) -> tuple[pd.DataFrame, int, Dict[str, Any]]:
    data_by_tf, workflow_meta = pipeline._load_symbol_timeframe_data(symbol)
    data = data_by_tf.get(timeframe)
    if data is None or len(data) < 200:
        return pd.DataFrame(), 0, workflow_meta
    features = FeatureComputer.compute_all(
        data,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=getattr(pipeline.config, "regime_params_file", "regime_params.json"),
    )
    windows = workflow_meta.get("windows", {}) if isinstance(workflow_meta, dict) else {}
    stage2 = windows.get("stage2_selection", {}) if isinstance(windows, dict) else {}
    stage2_warm = windows.get("stage2_selection_with_warmup", {}) if isinstance(windows, dict) else {}
    stage2_start = pd.Timestamp(stage2["start_ts"]) if stage2.get("start_ts") else None
    stage2_warm_start = pd.Timestamp(stage2_warm["start_ts"]) if stage2_warm.get("start_ts") else stage2_start
    stage2_features = pipeline._slice_time_window(features, stage2_warm_start, None)
    warmup_bars = int((stage2_features.index < stage2_start).sum()) if stage2_start is not None else 0
    return stage2_features, warmup_bars, workflow_meta


def _status_from_kfold(evidence: Optional[Dict[str, Any]], min_trades: int) -> str:
    if not evidence:
        return "ERROR"
    trades = int(evidence.get("total_trades", 0) or 0)
    if trades < int(min_trades):
        return "INSUFFICIENT"
    if str(evidence.get("policy_status", "") or "").upper() != "PASS":
        return "FAIL"
    return "PASS"


def replay_rows(rows: List[Dict[str, Any]], cfg: PipelineConfig) -> List[Dict[str, Any]]:
    pipeline = OptimizationPipeline(cfg)
    output_rows: List[Dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol", "") or "")
        timeframe = str(row.get("timeframe", "") or "")
        regime = str(row.get("regime", "") or "")
        started = time.time()
        try:
            stage2_features, warmup_bars, workflow_meta = _stage2_features_for_row(pipeline, symbol, timeframe)
            winner = RegimeConfig(
                strategy_name=str(row.get("strategy_name", "") or ""),
                parameters=dict(row.get("parameters", {}) or {}),
                quality_score=float(row.get("quality_score", 0.0) or 0.0),
            )
            evidence = pipeline.regime_optimizer.evaluate_purged_kfold_oos(
                symbol,
                timeframe,
                regime,
                winner,
                stage2_features,
                scored_start=warmup_bars,
            )
            error = None
        except Exception as exc:
            evidence = None
            workflow_meta = {}
            error = f"{type(exc).__name__}: {exc}"
        finally:
            FeatureComputer.clear_cache()
            pipeline.data_loader.clear_cache()

        compact = {
            key: value for key, value in (evidence or {}).items()
            if key != "folds"
        }
        output_rows.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "strategy_name": row.get("strategy_name", ""),
            "asset_class": row.get("asset_class", ""),
            "provenance": row.get("provenance", ""),
            "stress": row.get("stress", ""),
            "historical_status": row.get("historical_status", ""),
            "pbo_timeframe": row.get("pbo_timeframe"),
            "fixed_holdout": {
                "status": row.get("holdout_status", ""),
                "trades": int(row.get("holdout_trades", 0) or 0),
                "return_pct": row.get("holdout_return_pct"),
                "profit_factor": row.get("holdout_pf"),
            },
            "purged_kfold": {
                **compact,
                "status": _status_from_kfold(evidence, int(getattr(cfg, "oos_min_trades", 5) or 0)),
                "folds": (evidence or {}).get("folds", []),
            },
            "recovered_sufficiency": (
                str(row.get("holdout_status", "")) == "INSUFFICIENT"
                and evidence is not None
                and int(evidence.get("total_trades", 0) or 0) >= int(getattr(cfg, "oos_min_trades", 5) or 0)
            ),
            "baseline_final_verdict": row.get("final_verdict", ""),
            "baseline_why": row.get("why", ""),
            "workflow": {
                "oos_validation_mode": (workflow_meta or {}).get("oos_validation_mode"),
                "effective_clean_holdout_pct": (workflow_meta or {}).get("effective_clean_holdout_pct"),
            },
            "error": error,
            "duration_seconds": round(time.time() - started, 3),
        })
    return output_rows


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    evaluated = [row for row in rows if not row.get("error")]
    fixed_insufficient = [row for row in evaluated if row.get("fixed_holdout", {}).get("status") == "INSUFFICIENT"]
    recovered = [row for row in evaluated if row.get("recovered_sufficiency")]
    kfold_pass = [row for row in evaluated if row.get("purged_kfold", {}).get("status") == "PASS"]
    manual_candidates = [
        row for row in kfold_pass
        if row.get("baseline_final_verdict") != "PROMOTE"
        and _non_holdout_gates_clean(row)
    ]
    return {
        "rows_requested": len(rows),
        "rows_evaluated": len(evaluated),
        "errors": len(rows) - len(evaluated),
        "fixed_holdout_insufficient_rows": len(fixed_insufficient),
        "purged_kfold_sufficiency_recovered_rows": len(recovered),
        "purged_kfold_policy_pass_rows": len(kfold_pass),
        "manual_candidate_rows_after_existing_non_oos_gates": len(manual_candidates),
        "manual_candidate_keys": [
            f"{row['symbol']} {row['timeframe']} {row['regime']} / {row['strategy_name']}"
            for row in manual_candidates
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded purged-kfold OOS replay for W4-style promotion rows.")
    parser.add_argument(
        "--review",
        default=str(REPO / "pm_outputs" / "experiments" / "production_live_final_verdict_promotion.json"),
    )
    parser.add_argument("--config", default=str(REPO / "config.json"))
    parser.add_argument("--max-rows", type=int, default=12)
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--embargo-bars", type=int, default=1)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--label", default="production_live_purged_kfold_bounded_replay")
    args = parser.parse_args()

    review_path = Path(args.review)
    payload = json.loads(review_path.read_text(encoding="utf-8"))
    source_rows = list(payload.get("rows", []) or [])
    selected = select_representative_rows(source_rows, max(1, int(args.max_rows)))
    cfg = _load_pipeline_config(
        Path(args.config),
        splits=int(args.splits),
        embargo_bars=int(args.embargo_bars),
        min_trades=int(args.min_trades),
    )
    replayed = replay_rows(selected, cfg)
    summary = summarize(replayed)
    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "engine_commit": _git_head(),
        "created_at": pd.Timestamp.now().isoformat(),
        "source_review": str(review_path),
        "config": {
            "oos_validation_mode": cfg.oos_validation_mode,
            "clean_holdout_pct": cfg.clean_holdout_pct,
            "oos_kfold_splits": cfg.oos_kfold_splits,
            "oos_embargo_bars": cfg.oos_embargo_bars,
            "oos_min_trades": cfg.oos_min_trades,
            "oos_aggregate_policy": cfg.oos_aggregate_policy,
        },
        "summary": summary,
        "rows": replayed,
    }, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
