"""Audit live/validation regime detector parity for promoted winner cells."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pm_core import DataLoader, FeatureComputer, PipelineConfig
from pm_regime import (
    clear_regime_params_cache,
    materialize_regime_params_for_cells,
)


REGIME_COLUMNS = [
    "TREND_SCORE",
    "RANGE_SCORE",
    "BREAKOUT_SCORE",
    "CHOP_SCORE",
    "REGIME_RAW",
    "REGIME",
    "REGIME_STRENGTH",
    "REGIME_GAP",
    "REGIME_LIVE",
    "REGIME_STRENGTH_LIVE",
]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _tradeable(payload: Dict[str, Any]) -> bool:
    name = str(payload.get("strategy_name", "") or "").strip().upper()
    meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    status = str(payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
    return bool(
        name
        and name != "NO_TRADE"
        and not meta.get("no_trade")
        and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
    )


def _promoted_cells(ledger: Dict[str, Any]) -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []
    for symbol, symbol_payload in ledger.items():
        if not isinstance(symbol_payload, dict):
            continue
        regimes = symbol_payload.get("regime_configs")
        if not isinstance(regimes, dict):
            continue
        for timeframe, regime_payload in regimes.items():
            if not isinstance(regime_payload, dict):
                continue
            for regime, cfg in regime_payload.items():
                if isinstance(cfg, dict) and _tradeable(cfg):
                    artifact = cfg.get("artifact_meta")
                    if not isinstance(artifact, dict):
                        artifact = symbol_payload.get("artifact_meta") if isinstance(symbol_payload.get("artifact_meta"), dict) else {}
                    cells.append(
                        {
                            "symbol": str(symbol),
                            "timeframe": str(timeframe).upper(),
                            "regime": str(regime).upper(),
                            "strategy_name": cfg.get("strategy_name"),
                            "artifact_regime_params": dict((artifact or {}).get("regime_params") or {}),
                        }
                    )
    return cells


def _pipeline_config(config_path: Path) -> PipelineConfig:
    payload = _load_json(config_path)
    pipeline_payload = payload.get("pipeline", {}) if isinstance(payload, dict) else {}
    return PipelineConfig(**pipeline_payload)


def _load_timeframe_data(loader: DataLoader, symbol: str, timeframe: str, max_bars: int) -> pd.DataFrame:
    data = loader.get_data(symbol, timeframe)
    if data is None or data.empty:
        raise RuntimeError(f"no data for {symbol}/{timeframe}")
    data = data.sort_index()
    if max_bars > 0 and len(data) > max_bars:
        data = data.tail(max_bars).copy()
    return data


def _compute_regime_surface(data: pd.DataFrame, symbol: str, timeframe: str, regime_params_file: str) -> pd.DataFrame:
    FeatureComputer.clear_cache()
    clear_regime_params_cache()
    features = FeatureComputer.compute_all(
        data,
        symbol=symbol,
        timeframe=timeframe,
        regime_params_file=regime_params_file,
    )
    return features[[col for col in REGIME_COLUMNS if col in features.columns]].copy()


def _compare_surfaces(left: pd.DataFrame, right: pd.DataFrame, tolerance: float) -> Dict[str, Any]:
    common_index = left.index.intersection(right.index)
    columns = [col for col in REGIME_COLUMNS if col in left.columns and col in right.columns]
    mismatches: Dict[str, Any] = {}
    for col in columns:
        a = left.loc[common_index, col]
        b = right.loc[common_index, col]
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
            diff = (pd.to_numeric(a, errors="coerce") - pd.to_numeric(b, errors="coerce")).abs()
            max_abs = float(diff.max(skipna=True) or 0.0)
            if max_abs > tolerance:
                mismatches[col] = {"max_abs_diff": max_abs}
        else:
            neq = (a.astype(str).fillna("<NA>") != b.astype(str).fillna("<NA>"))
            count = int(neq.sum())
            if count:
                mismatches[col] = {"mismatched_rows": count}
    return {
        "rows_compared": int(len(common_index)),
        "columns_compared": columns,
        "mismatches": mismatches,
        "ok": not mismatches and bool(columns) and len(common_index) > 0,
    }


def run_audit(
    *,
    config_path: Path,
    ledger_path: Path,
    output_path: Path,
    materialize: bool,
    max_bars: int,
    tolerance: float,
) -> Dict[str, Any]:
    cfg = _pipeline_config(config_path)
    ledger = _load_json(ledger_path)
    cells = _promoted_cells(ledger if isinstance(ledger, dict) else {})
    unique_symbol_tfs: Set[Tuple[str, str]] = {
        (cell["symbol"], cell["timeframe"]) for cell in cells
    }

    regime_params_file = Path(str(getattr(cfg, "regime_params_file", "regime_params.json") or "regime_params.json"))
    if not regime_params_file.is_absolute():
        regime_params_file = config_path.resolve().parent / regime_params_file
    if materialize:
        materialize_regime_params_for_cells(list(unique_symbol_tfs), str(regime_params_file))

    data_dir = Path(getattr(cfg, "data_dir", "data"))
    if not data_dir.is_absolute():
        data_dir = config_path.resolve().parent / data_dir
    loader = DataLoader(data_dir, cache_resampled=False)

    with tempfile.TemporaryDirectory() as tmp:
        missing_regime_file = str(Path(tmp) / "missing_regime_params.json")
        cell_reports: List[Dict[str, Any]] = []
        for symbol, timeframe in sorted(unique_symbol_tfs):
            report: Dict[str, Any] = {
                "symbol": symbol,
                "timeframe": timeframe,
                "baseline": "validation_no_file_detector_surface",
            }
            try:
                data = _load_timeframe_data(loader, symbol, timeframe, max_bars=max_bars)
                baseline = _compute_regime_surface(data, symbol, timeframe, missing_regime_file)
                explicit = _compute_regime_surface(data, symbol, timeframe, str(regime_params_file))
                report.update(_compare_surfaces(baseline, explicit, tolerance=tolerance))
                report["data_rows"] = int(len(data))
            except Exception as exc:
                report.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
            cell_reports.append(report)
            FeatureComputer.clear_cache()
            clear_regime_params_cache()

    result = {
        "ok": all(item.get("ok") for item in cell_reports),
        "promoted_cells": len(cells),
        "unique_symbol_timeframes": len(unique_symbol_tfs),
        "regime_params_file": str(regime_params_file),
        "max_bars": int(max_bars),
        "tolerance": float(tolerance),
        "cells": cell_reports,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--ledger", default="pm_configs.json")
    parser.add_argument("--output", default="pm_outputs/production_live/regime_param_parity_audit.json")
    parser.add_argument("--no-materialize", action="store_true")
    parser.add_argument("--max-bars", type=int, default=0, help="0 means full available timeframe history")
    parser.add_argument("--tolerance", type=float, default=1e-12)
    args = parser.parse_args(list(argv) if argv is not None else None)

    result = run_audit(
        config_path=Path(args.config),
        ledger_path=Path(args.ledger),
        output_path=Path(args.output),
        materialize=not args.no_materialize,
        max_bars=max(0, int(args.max_bars)),
        tolerance=max(0.0, float(args.tolerance)),
    )
    print(json.dumps({k: result[k] for k in ("ok", "promoted_cells", "unique_symbol_timeframes", "regime_params_file")}, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
