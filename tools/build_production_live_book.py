"""Build a live-trading ledger/config from corrected promoted specialist cells.

This tool is intentionally conservative: it only copies cells whose final
promotion verdict is PROMOTE, verifies the promotion/cell-safety artifacts, and
emits a ledger with no broad symbol-level fallback winners.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Set

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm_core import PipelineConfig


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _cell_id(symbol: str, timeframe: str, regime: str) -> str:
    return f"{symbol.upper()}|{timeframe.upper()}|{regime.upper()}"


def _json_path(path: Path) -> str:
    return path.as_posix()


def _promotion_cells(review: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    cells: Dict[str, Dict[str, Any]] = {}
    for row in review.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("final_verdict", "") or "").upper() != "PROMOTE":
            continue
        symbol = str(row.get("symbol", "") or "").strip().upper()
        timeframe = str(row.get("timeframe", "") or "").strip().upper()
        regime = str(row.get("regime", "") or "").strip().upper()
        if symbol and timeframe and regime:
            cells[_cell_id(symbol, timeframe, regime)] = row
    return cells


def _assert_safety(safety: Dict[str, Any], promote_count: int) -> None:
    summary = safety.get("navigation_counts") if isinstance(safety.get("navigation_counts"), dict) else None
    if summary is None:
        summary = safety.get("summary") if isinstance(safety.get("summary"), dict) else safety

    def _count(name: str) -> int:
        value = summary.get(name, safety.get(name, 0))
        if isinstance(value, list):
            return len(value)
        return int(value or 0)

    unsafe = _count("unsafe_promotes")
    fail_not_reject = _count("fail_not_reject")
    final_promotes = _count("final_promotes")
    if unsafe or fail_not_reject:
        raise SystemExit(
            f"promotion safety failed: unsafe_promotes={unsafe}, fail_not_reject={fail_not_reject}"
        )
    if final_promotes != promote_count:
        raise SystemExit(
            f"promotion safety count mismatch: safety={final_promotes}, review={promote_count}"
        )


def _assert_marker(marker: Dict[str, Any]) -> None:
    verdict = str(marker.get("verdict", "") or "").upper()
    if not bool(marker.get("review_ready", False)):
        raise SystemExit("completion marker is not marked review_ready")
    if bool(marker.get("stale")):
        raise SystemExit("completion marker is stale")
    if bool(marker.get("pid_alive")):
        raise SystemExit("completion marker still reports pid_alive=true")
    if "CELL_SAFE" not in verdict:
        raise SystemExit(f"completion marker is not cell-safe: {marker.get('verdict')}")


def _no_trade_config(reason: str) -> Dict[str, Any]:
    return {
        "strategy_name": "NO_TRADE",
        "parameters": {},
        "quality_score": 0.0,
        "regime_train_trades": 0,
        "regime_val_trades": 0,
        "train_metrics": {},
        "val_metrics": {},
        "holdout_metrics": {},
        "historical_audit_metrics": {"enabled": True, "severe_failure": True, "failure_reasons": [reason]},
        "artifact_meta": {"no_trade": True, "reason": reason},
        "promotion_status": "NO_TRADE",
        "promotion_reasons": [reason],
    }


def _config_from_live_payload(payload: Dict[str, Any]) -> PipelineConfig:
    pipeline = payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {}
    allowed = {item.name for item in fields(PipelineConfig)}
    return PipelineConfig(**{key: value for key, value in pipeline.items() if key in allowed})


def _deployment_valid_until(payload: Dict[str, Any]) -> str:
    cfg = _config_from_live_payload(payload)
    return cfg.get_next_retrain_at(datetime.now()).isoformat()


def _anchor_from_marker(marker: Dict[str, Any]) -> datetime:
    for key in ("created_at", "corrected_at", "completed_at"):
        raw = str(marker.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            continue
    return datetime.now()


def build_live_ledger(source: Dict[str, Any],
                      promote_cells: Dict[str, Dict[str, Any]],
                      *,
                      valid_until: str,
                      retrain_days: int) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for cell, row in sorted(promote_cells.items()):
        symbol, timeframe, regime = cell.split("|")
        symbol_payload = source.get(symbol)
        if not isinstance(symbol_payload, dict):
            raise SystemExit(f"source ledger missing promoted symbol {symbol}")
        regimes = symbol_payload.get("regime_configs")
        if not isinstance(regimes, dict):
            raise SystemExit(f"source ledger missing regime_configs for {symbol}")
        cell_payload = (((regimes.get(timeframe) or {}).get(regime)) or None)
        if not isinstance(cell_payload, dict):
            raise SystemExit(f"source ledger missing promoted cell {cell}")

        row_strategy = str(row.get("strategy_name", "") or "")
        if row_strategy and row_strategy != str(cell_payload.get("strategy_name", "") or ""):
            raise SystemExit(
                f"strategy mismatch for {cell}: review={row_strategy}, ledger={cell_payload.get('strategy_name')}"
            )
        status = str(cell_payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
        if status not in {"PROMOTED", "PROMOTE"}:
            raise SystemExit(f"promoted cell {cell} has non-live ledger status {status}")

        base = copy.deepcopy(symbol_payload)
        if symbol not in output:
            output[symbol] = {
                "symbol": symbol,
                "strategy_name": "NO_TRADE",
                "timeframe": "",
                "parameters": {},
                "retrain_days": retrain_days,
                "composite_score": 0.0,
                "robustness_ratio": 0.0,
                "is_validated": True,
                "validation_reason": "production ledger: only corrected promoted specialist cells are tradeable",
                "validation_evidence": {
                    "production_gate": "corrected_promotion_review",
                    "promoted_cells_only": True,
                },
                "train_metrics": {},
                "val_metrics": {},
                "holdout_metrics": {},
                "historical_audit_metrics": {},
                "data_window_meta": copy.deepcopy(base.get("data_window_meta") or {}),
                "artifact_meta": copy.deepcopy(base.get("artifact_meta") or {}),
                "live_observability": copy.deepcopy(base.get("live_observability") or {}),
                "walk_forward_audit": copy.deepcopy(base.get("walk_forward_audit") or {}),
                "regime_detection_version": copy.deepcopy(base.get("regime_detection_version")),
                "regime_configs": {},
                "default_config": _no_trade_config("production ledger has no broad default fallback"),
                "optimized_at": base.get("optimized_at"),
                "valid_until": valid_until,
            }
        promoted_payload = copy.deepcopy(cell_payload)
        promoted_payload["valid_until"] = valid_until
        output[symbol]["regime_configs"].setdefault(timeframe, {})[regime] = promoted_payload
    return output


def _copy_config(base: Dict[str, Any],
                 *,
                 operator: Dict[str, Any],
                 output_config: Path,
                 output_ledger: Path,
                 symbols: Iterable[str],
                 promote_cells: Set[str],
                 promotion_review: Path,
                 promotion_safety: Path,
                 completion_marker: Path,
                 anchor_dt: datetime) -> Dict[str, Any]:
    cfg = copy.deepcopy(base)
    cfg.setdefault("_meta", {})
    cfg["_meta"].update(
        {
            "purpose": "Live production config generated from corrected promoted specialist cells",
            "scope": "live_trading_requires_operator_mt5_identity",
            "source_promotion_review": _json_path(promotion_review),
            "source_promotion_cell_safety": _json_path(promotion_safety),
            "source_completion_marker": _json_path(completion_marker),
        }
    )
    pipeline = cfg.setdefault("pipeline", {})
    pipeline["winner_ledger_path"] = _json_path(output_ledger)
    pipeline["output_dir"] = "./pm_outputs/production_live"
    pipeline["log_dir"] = "./logs/production_live"
    pipeline["production_require_promotion_gate"] = True
    pipeline["production_promotion_review_path"] = _json_path(promotion_review)
    pipeline["production_promotion_cell_safety_path"] = _json_path(promotion_safety)
    pipeline["production_completion_marker_path"] = _json_path(completion_marker)
    pipeline["production_expected_promote_cells"] = sorted(promote_cells)
    pipeline["walk_forward_promotion_policy"] = "observe_only"
    pipeline["production_retrain_mode"] = "notify"
    pipeline["production_retrain_interval_weeks"] = 4
    pipeline["production_retrain_weekday"] = anchor_dt.strftime("%A").lower()
    pipeline.setdefault("production_retrain_time", "00:01")
    pipeline["production_retrain_anchor_date"] = anchor_dt.strftime("%Y-%m-%d")
    pipeline["manual_new_entries_enabled"] = True
    pipeline["manual_kill_switch_path"] = "runtime/manual_new_entries_enabled.flag"
    pipeline["storage_pause_entries_below_free_gb"] = 10.0
    pipeline["daily_loss_hard_block_pct"] = 2.0
    pipeline["session_loss_hard_block_pct"] = 1.0
    pipeline["weekly_loss_hard_block_pct"] = 4.0
    pipeline["open_drawdown_hard_block_pct"] = 3.0
    cfg["symbols"] = sorted(set(symbols))
    for section in ("position", "mt5", "instrument_specs", "instrument_spec_defaults"):
        if section in operator:
            cfg[section] = copy.deepcopy(operator[section])
    if "broker_specs_path" in operator:
        cfg["broker_specs_path"] = operator["broker_specs_path"]
    cfg.setdefault("mt5", {})
    cfg["mt5"].setdefault("expected_login", 0)
    cfg["mt5"].setdefault("expected_server", "")
    cfg["mt5"].setdefault("expected_currency", "")
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ledger", required=True)
    parser.add_argument("--promotion-review", required=True)
    parser.add_argument("--promotion-safety", required=True)
    parser.add_argument("--completion-marker", required=True)
    parser.add_argument("--base-config", required=True)
    parser.add_argument("--operator-config", default="config.json")
    parser.add_argument("--output-ledger", default="pm_configs.json")
    parser.add_argument("--output-config", default="config.json")
    args = parser.parse_args()

    source_ledger = Path(args.source_ledger)
    promotion_review = Path(args.promotion_review)
    promotion_safety = Path(args.promotion_safety)
    completion_marker = Path(args.completion_marker)
    base_config = Path(args.base_config)
    operator_config = Path(args.operator_config)
    output_ledger = Path(args.output_ledger)
    output_config = Path(args.output_config)

    review = _load_json(promotion_review)
    safety = _load_json(promotion_safety)
    marker = _load_json(completion_marker)
    source = _load_json(source_ledger)
    base = _load_json(base_config)
    operator = _load_json(operator_config) if operator_config.exists() else {}

    promote_cells = _promotion_cells(review)
    if not promote_cells:
        raise SystemExit("promotion review contains no final PROMOTE cells")
    _assert_safety(safety, len(promote_cells))
    _assert_marker(marker)
    anchor_dt = _anchor_from_marker(marker)

    live_config = _copy_config(
        base,
        operator=operator,
        output_config=output_config,
        output_ledger=output_ledger,
        symbols={cell.split("|", 1)[0] for cell in promote_cells},
        promote_cells=set(promote_cells.keys()),
        promotion_review=promotion_review,
        promotion_safety=promotion_safety,
        completion_marker=completion_marker,
        anchor_dt=anchor_dt,
    )
    live_cfg = _config_from_live_payload(live_config)
    live_ledger = build_live_ledger(
        source,
        promote_cells,
        valid_until=_deployment_valid_until(live_config),
        retrain_days=int(live_cfg.production_retrain_interval_weeks * 7),
    )

    output_ledger.parent.mkdir(parents=True, exist_ok=True)
    output_config.parent.mkdir(parents=True, exist_ok=True)
    output_ledger.write_text(json.dumps(_json_safe(live_ledger), indent=2, allow_nan=False), encoding="utf-8")
    output_config.write_text(json.dumps(_json_safe(live_config), indent=2, allow_nan=False), encoding="utf-8")
    print(f"Wrote {len(live_ledger)} symbols / {len(promote_cells)} cells to {output_ledger}")
    print(f"Wrote production config to {output_config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
