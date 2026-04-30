"""Regime-local profitability and boundary audit helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List

import pandas as pd


@dataclass
class RegimeAuditReport:
    enabled: bool = True
    severe_failure: bool = False
    warnings: List[str] = field(default_factory=list)
    metrics_by_regime: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "severe_failure": self.severe_failure,
            "warnings": list(self.warnings),
            "metrics_by_regime": dict(self.metrics_by_regime),
        }


def _float_metric(metrics: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(metrics.get(key, default) or default)
    except Exception:
        return default


def audit_regime_profitability(metrics_by_regime: Dict[str, Dict[str, Any]],
                               *,
                               features: Any = None,
                               trades: Iterable[Dict[str, Any]] = (),
                               min_trades: int = 1,
                               min_profit_factor: float = 1.0,
                               min_return_pct: float = 0.0,
                               low_gap_warn: float = 0.05,
                               include_boundary_audit: bool = True) -> RegimeAuditReport:
    warnings: List[str] = []
    severe = False
    compact: Dict[str, Dict[str, Any]] = {}
    for regime, metrics in (metrics_by_regime or {}).items():
        payload = dict(metrics or {})
        trade_count = int(payload.get("total_trades", 0) or 0)
        pf = _float_metric(payload, "profit_factor")
        ret = _float_metric(payload, "total_return_pct")
        compact[str(regime)] = {
            "total_trades": trade_count,
            "profit_factor": pf,
            "total_return_pct": ret,
            "max_drawdown_pct": _float_metric(payload, "max_drawdown_pct"),
        }
        if trade_count < int(min_trades):
            warnings.append(f"{regime}: trades {trade_count} < {int(min_trades)}")
            continue
        if pf < float(min_profit_factor) or ret < float(min_return_pct):
            severe = True
            warnings.append(
                f"{regime}: profitability failed (PF={pf:.2f}, return={ret:.2f}%)"
            )
    if include_boundary_audit and features is not None and len(features) > 0:
        frame = features
        regime_col = "REGIME_LIVE" if "REGIME_LIVE" in frame.columns else "REGIME" if "REGIME" in frame.columns else None
        if regime_col is not None:
            regimes = frame[regime_col].astype(str).str.upper()
            distribution = regimes.value_counts(dropna=False).to_dict()
            transitions = (regimes.shift(1).fillna(regimes) + "->" + regimes).iloc[1:]
            transition_counts = transitions.value_counts().to_dict()
            compact["_coverage"] = {
                "regime_distribution": {str(k): int(v) for k, v in distribution.items()},
                "transition_counts": {str(k): int(v) for k, v in transition_counts.items()},
                "winner_coverage_by_regime": {
                    str(regime): 0.0 if int(metrics.get("total_trades", 0) or 0) <= 0 else 1.0
                    for regime, metrics in (metrics_by_regime or {}).items()
                },
                "no_trade_frequency_proxy": {
                    str(regime): 1.0 if int(metrics.get("total_trades", 0) or 0) <= 0 else 0.0
                    for regime, metrics in (metrics_by_regime or {}).items()
                },
            }
        if "REGIME_GAP" in frame.columns:
            gaps = pd.to_numeric(frame["REGIME_GAP"], errors="coerce").dropna().abs()
            if not gaps.empty:
                compact.setdefault("_coverage", {})["regime_gap"] = {
                    "avg": float(gaps.mean()),
                    "min": float(gaps.min()),
                    "low_gap_bar_pct": float((gaps < float(low_gap_warn)).mean() * 100.0),
                }

    trade_rows = list(trades or [])
    if include_boundary_audit and trade_rows:
        boundary_pnl = 0.0
        low_gap_pnl = 0.0
        low_gap_count = 0
        frame = features if features is not None else None
        transition_pnl: Dict[str, Dict[str, Any]] = {}
        for trade in trade_rows:
            pnl = float(trade.get("pnl_dollars", 0.0) or 0.0)
            boundary_pnl += pnl
            signal_bar = None
            try:
                signal_bar = int(trade.get("signal_bar", trade.get("entry_bar", 0)) or 0)
            except Exception:
                signal_bar = None
            if frame is not None and signal_bar is not None:
                regime_col = "REGIME_LIVE" if "REGIME_LIVE" in frame.columns else "REGIME" if "REGIME" in frame.columns else None
                if regime_col is not None and 0 <= signal_bar < len(frame):
                    try:
                        current_regime = str(frame[regime_col].iloc[signal_bar]).upper()
                        previous_regime = (
                            str(frame[regime_col].iloc[max(0, signal_bar - 1)]).upper()
                            if signal_bar > 0 else current_regime
                        )
                        key = f"{previous_regime}->{current_regime}"
                        bucket = transition_pnl.setdefault(key, {"trades": 0, "pnl_dollars": 0.0})
                        bucket["trades"] += 1
                        bucket["pnl_dollars"] += pnl
                    except Exception:
                        pass
            if frame is not None and "REGIME_GAP" in frame.columns and signal_bar is not None:
                try:
                    gap = abs(float(frame["REGIME_GAP"].iloc[signal_bar]))
                    if gap < float(low_gap_warn):
                        low_gap_count += 1
                        low_gap_pnl += pnl
                except Exception:
                    pass
        compact.setdefault("_coverage", {})["boundary_outcomes"] = {
            "audited_trades": len(trade_rows),
            "total_pnl_dollars": boundary_pnl,
            "low_gap_trades": low_gap_count,
            "low_gap_pnl_dollars": low_gap_pnl,
        }
        if transition_pnl:
            compact.setdefault("_coverage", {})["pnl_by_regime_transition"] = {
                key: {
                    "trades": int(value.get("trades", 0) or 0),
                    "pnl_dollars": float(value.get("pnl_dollars", 0.0) or 0.0),
                }
                for key, value in transition_pnl.items()
            }

    return RegimeAuditReport(
        severe_failure=severe,
        warnings=warnings,
        metrics_by_regime=compact,
    )


def summarize_regime_boundary_gaps(gaps: Iterable[float], warn_gap: float = 0.05) -> Dict[str, Any]:
    values = [abs(float(value)) for value in gaps if value is not None]
    if not values:
        return {"observed": False, "min_gap": None, "warning": False}
    min_gap = min(values)
    return {
        "observed": True,
        "min_gap": min_gap,
        "warning": min_gap < float(warn_gap),
    }
