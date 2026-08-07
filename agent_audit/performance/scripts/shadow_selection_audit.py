"""Read-only shadow audit for validation-surface and objective fragility.

The script does not change optimizer scores or winner selection. It extracts
current ledger metrics and writes shadow flags under `agent_audit/performance/data/`.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _tradeable(payload: Dict[str, Any]) -> bool:
    name = str(payload.get("strategy_name", "") or "").upper()
    meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
    status = str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper()
    return bool(
        name
        and name != "NO_TRADE"
        and not meta.get("no_trade")
        and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
    )


def _cell_row(symbol: str, timeframe: str, regime: str, payload: Dict[str, Any], source: str) -> Dict[str, Any]:
    row = dict(payload)
    row.update(
        symbol=str(payload.get("symbol") or symbol).upper(),
        timeframe=str(payload.get("timeframe") or timeframe or "H1").upper(),
        regime=str(payload.get("regime") or regime or "SYMBOL_LEVEL").upper(),
        promotion_source=source,
        promotion_status=str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper(),
    )
    return row


def iter_cells(ledger: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
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
                        promoted_leaves.append(
                            _cell_row(
                                str(symbol_payload.get("symbol") or symbol),
                                str(timeframe),
                                str(regime),
                                cell,
                                source,
                            )
                        )
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


def _metrics(row: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = row.get(key)
    return value if isinstance(value, dict) else {}


def audit_rows(cells: Iterable[Dict[str, Any]], *, min_val_trades: int, max_sharpe_without_pbo: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for cell in cells:
        val = _metrics(cell, "val_metrics")
        train = _metrics(cell, "train_metrics")
        false_discovery = cell.get("false_discovery") if isinstance(cell.get("false_discovery"), dict) else {}
        pbo_status = str(false_discovery.get("pbo_status", "") or "").lower()
        val_trades = int(_finite(val.get("total_trades", cell.get("regime_val_trades", 0)), 0.0))
        val_return = _finite(val.get("total_return_pct"))
        val_dd = _finite(val.get("max_drawdown_pct"))
        val_pf = _finite(val.get("profit_factor"), 0.0)
        val_sharpe = _finite(val.get("sharpe_approx", val.get("sharpe_ratio", 0.0)))
        train_return = _finite(train.get("total_return_pct"))
        stability = _finite(val.get("std_weekly_return"), 0.0)
        no_stability_shadow = val_return - val_dd + min(val_pf, 10.0)
        stability_penalized_shadow = no_stability_shadow * math.exp(-2.5 * max(0.0, stability))
        rows.append(
            {
                "symbol": cell.get("symbol"),
                "timeframe": cell.get("timeframe"),
                "regime": cell.get("regime"),
                "strategy_name": cell.get("strategy_name"),
                "promotion_status": cell.get("promotion_status"),
                "promotion_source": cell.get("promotion_source"),
                "val_trades": val_trades,
                "val_return_pct": round(val_return, 6),
                "train_return_pct": round(train_return, 6),
                "val_max_drawdown_pct": round(val_dd, 6),
                "val_profit_factor": round(val_pf, 6),
                "val_sharpe": round(val_sharpe, 6),
                "pbo_status": pbo_status or "not_recorded",
                "std_weekly_return": round(stability, 6),
                "shadow_score_no_stability": round(no_stability_shadow, 6),
                "shadow_score_with_stability": round(stability_penalized_shadow, 6),
                "sparse_validation_flag": val_trades < min_val_trades,
                "extreme_sharpe_without_pbo_flag": (
                    max_sharpe_without_pbo > 0.0
                    and val_sharpe > max_sharpe_without_pbo
                    and pbo_status in {"", "not_available", "unavailable", "not_recorded"}
                ),
                "day_of_week_bias_flag": str(cell.get("strategy_name")) == "DayOfWeekBiasStrategy",
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", default=str(REPO / "pm_configs.json"))
    parser.add_argument("--min-val-trades", type=int, default=15)
    parser.add_argument("--max-sharpe-without-pbo", type=float, default=5.0)
    parser.add_argument(
        "--out",
        default=str(REPO / "agent_audit" / "performance" / "data" / "shadow_selection_audit.csv"),
    )
    args = parser.parse_args()

    ledger = json.loads(Path(args.ledger).read_text(encoding="utf-8-sig"))
    rows = audit_rows(
        iter_cells(ledger),
        min_val_trades=max(0, int(args.min_val_trades)),
        max_sharpe_without_pbo=max(0.0, float(args.max_sharpe_without_pbo)),
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "rows": len(rows),
        "sparse_validation": sum(1 for row in rows if row.get("sparse_validation_flag")),
        "extreme_sharpe_without_pbo": sum(1 for row in rows if row.get("extreme_sharpe_without_pbo_flag")),
        "day_of_week_bias": sum(1 for row in rows if row.get("day_of_week_bias_flag")),
    }
    summary_path = out_path.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"wrote {len(rows)} row(s) -> {out_path}")
    print(f"wrote summary -> {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
