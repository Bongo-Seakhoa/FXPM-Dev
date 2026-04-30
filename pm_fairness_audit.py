"""Search-budget fairness telemetry for strategy selection evidence."""

from __future__ import annotations

from typing import Any, Dict, Iterable


def build_fairness_evidence(candidate: Dict[str, Any],
                            strategy: Any = None) -> Dict[str, Any]:
    trials = int(candidate.get("search_trials", 0) or 0)
    cartesian = 0
    if strategy is not None and hasattr(strategy, "cartesian_grid_size"):
        try:
            cartesian = int(strategy.cartesian_grid_size())
        except Exception:
            cartesian = 0
    coverage = (trials / cartesian) if cartesian > 0 and trials > 0 else 0.0
    return {
        "strategy_name": candidate.get("strategy_name", ""),
        "strategy_category": str(getattr(getattr(strategy, "category", ""), "value", getattr(strategy, "category", ""))),
        "search_trials": trials,
        "cartesian_grid_size": cartesian,
        "approx_search_coverage_pct": round(coverage * 100.0, 6),
        "family_size_aware": bool(candidate.get("family_size_aware", False)),
    }


def build_fairness_report(regime_configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    total_slots = 0
    no_trade_slots = 0
    strategy_counts: Dict[str, int] = {}
    regime_slots: Dict[str, int] = {}
    regime_no_trade: Dict[str, int] = {}
    category_counts: Dict[str, int] = {}
    category_coverage_totals: Dict[str, float] = {}
    category_coverage_counts: Dict[str, int] = {}
    category_val_trade_totals: Dict[str, int] = {}
    for _tf, regimes in (regime_configs or {}).items():
        for regime, cfg in (regimes or {}).items():
            total_slots += 1
            regime_key = str(regime)
            regime_slots[regime_key] = regime_slots.get(regime_key, 0) + 1
            strategy_name = str(getattr(cfg, "strategy_name", "") or "")
            is_no_trade = False
            if hasattr(cfg, "is_no_trade_marker"):
                is_no_trade = bool(cfg.is_no_trade_marker())
            else:
                is_no_trade = strategy_name.upper() in {"", "NO_TRADE"}
            if is_no_trade:
                no_trade_slots += 1
                regime_no_trade[regime_key] = regime_no_trade.get(regime_key, 0) + 1
                continue
            strategy_counts[strategy_name] = strategy_counts.get(strategy_name, 0) + 1
            evidence = dict(getattr(cfg, "validation_evidence", {}) or {})
            fairness = dict(evidence.get("fairness_evidence", {}) or {})
            category = str(fairness.get("strategy_category", "") or "unknown")
            category_counts[category] = category_counts.get(category, 0) + 1
            coverage = fairness.get("approx_search_coverage_pct")
            try:
                coverage_value = float(coverage)
            except (TypeError, ValueError):
                coverage_value = 0.0
            category_coverage_totals[category] = category_coverage_totals.get(category, 0.0) + coverage_value
            category_coverage_counts[category] = category_coverage_counts.get(category, 0) + 1
            category_val_trade_totals[category] = (
                category_val_trade_totals.get(category, 0)
                + int(getattr(cfg, "regime_val_trades", 0) or 0)
            )
    concentration = 0.0
    if strategy_counts and total_slots > 0:
        concentration = max(strategy_counts.values()) / total_slots * 100.0
    return {
        "total_slots": total_slots,
        "no_trade_slots": no_trade_slots,
        "no_trade_rate_pct": round((no_trade_slots / total_slots * 100.0) if total_slots else 0.0, 6),
        "winner_concentration_pct": round(concentration, 6),
        "strategy_winner_counts": strategy_counts,
        "category_winner_counts": category_counts,
        "winner_rate_by_category_pct": {
            category: round((count / total_slots * 100.0) if total_slots else 0.0, 6)
            for category, count in category_counts.items()
        },
        "avg_search_coverage_by_category_pct": {
            category: round(
                category_coverage_totals.get(category, 0.0)
                / max(1, category_coverage_counts.get(category, 0)),
                6,
            )
            for category in category_counts
        },
        "avg_validation_trades_by_category": {
            category: round(
                category_val_trade_totals.get(category, 0)
                / max(1, category_coverage_counts.get(category, 0)),
                6,
            )
            for category in category_counts
        },
        "no_trade_rate_by_regime_pct": {
            regime: round((regime_no_trade.get(regime, 0) / count * 100.0) if count else 0.0, 6)
            for regime, count in regime_slots.items()
        },
    }
