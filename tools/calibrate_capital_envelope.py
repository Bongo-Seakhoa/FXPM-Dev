"""Quantitative calibration of FxPM's capital and stop-risk envelope.

This is an offline sensitivity tool.  It deliberately keeps three quantities
separate:

* initial-stop risk (loss if every open trade exits at its initial stop),
* margin utilization (used margin / equity), and
* gross/notional market exposure.

The deterministic envelope chooses a *survivability ceiling*, not a target and
not a claim that the strategy has an edge.  The configured ceiling must still
pass the synchronized tuning replay and then the untouched final-lockbox replay.
The Monte Carlo section shows how strongly the answer depends on edge and
dependence assumptions; it must never be used to manufacture a promotion.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


DEFAULT_CAPS = (3.0, 5.0, 8.0, 10.0, 12.0, 20.0, 33.25, 50.0, 75.0, 90.0)


@dataclass(frozen=True)
class CapitalEnvelope:
    per_trade_initial_stop_risk_pct: float
    total_initial_stop_risk_ceiling_pct: float
    factor_group_initial_stop_risk_ceiling_pct: float
    same_symbol_initial_stop_risk_ceiling_pct: float
    margin_utilization_operating_floor_pct: float
    margin_utilization_ceiling_pct: float
    margin_level_reopen_pct: float
    margin_level_entry_block_pct: float
    portfolio_gap_stress_multiple: float
    factor_gap_stress_multiple: float
    portfolio_stressed_loss_budget_pct: float
    factor_stressed_loss_budget_pct: float
    full_size_position_capacity: int
    full_size_positions_per_factor: int


def margin_level_from_utilization(utilization_pct: float) -> float:
    """Return MT5 margin level (equity / margin * 100) from utilization."""
    value = float(utilization_pct)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("margin utilization must be finite and positive")
    return 10_000.0 / value


def floor_to_increment(value: float, increment: float) -> float:
    if not math.isfinite(value) or not math.isfinite(increment) or increment <= 0.0:
        raise ValueError("value and increment must be finite; increment must be positive")
    return math.floor((value + 1e-12) / increment) * increment


def derive_capital_envelope(
    *,
    per_trade_risk_pct: float = 0.25,
    portfolio_stressed_loss_budget_pct: float = 20.0,
    portfolio_gap_stress_multiple: float = 2.5,
    factor_stressed_loss_budget_pct: float = 5.0,
    factor_gap_stress_multiple: float = 2.5,
    same_symbol_risk_ceiling_pct: float = 1.0,
    margin_utilization_floor_pct: float = 33.3333333333,
    margin_utilization_ceiling_pct: float = 50.0,
) -> CapitalEnvelope:
    """Derive the largest increment-aligned hard ceilings within stress budgets.

    With the funded defaults this yields an 8% total initial-stop ceiling because
    8% x 2.5 = the 20% portfolio stressed-loss budget, plus a 2% gross factor
    ceiling because 2% x 2.5 = the 5% common-factor budget.
    """
    numeric = {
        "per_trade_risk_pct": per_trade_risk_pct,
        "portfolio_stressed_loss_budget_pct": portfolio_stressed_loss_budget_pct,
        "portfolio_gap_stress_multiple": portfolio_gap_stress_multiple,
        "factor_stressed_loss_budget_pct": factor_stressed_loss_budget_pct,
        "factor_gap_stress_multiple": factor_gap_stress_multiple,
        "same_symbol_risk_ceiling_pct": same_symbol_risk_ceiling_pct,
        "margin_utilization_floor_pct": margin_utilization_floor_pct,
        "margin_utilization_ceiling_pct": margin_utilization_ceiling_pct,
    }
    if any(not math.isfinite(float(value)) or float(value) <= 0.0 for value in numeric.values()):
        raise ValueError("all calibration inputs must be finite and positive")
    if margin_utilization_floor_pct > margin_utilization_ceiling_pct:
        raise ValueError("margin utilization floor cannot exceed ceiling")
    if margin_utilization_ceiling_pct >= 100.0:
        raise ValueError("margin utilization ceiling must be below 100%")

    raw_total = portfolio_stressed_loss_budget_pct / portfolio_gap_stress_multiple
    raw_factor = factor_stressed_loss_budget_pct / factor_gap_stress_multiple
    total_cap = floor_to_increment(raw_total, per_trade_risk_pct)
    factor_cap = floor_to_increment(min(raw_factor, total_cap), per_trade_risk_pct)
    same_symbol_cap = floor_to_increment(
        min(same_symbol_risk_ceiling_pct, factor_cap, total_cap),
        per_trade_risk_pct,
    )
    if min(total_cap, factor_cap, same_symbol_cap) < per_trade_risk_pct:
        raise ValueError("stress budgets cannot admit one funded full-risk trade")

    return CapitalEnvelope(
        per_trade_initial_stop_risk_pct=float(per_trade_risk_pct),
        total_initial_stop_risk_ceiling_pct=float(total_cap),
        factor_group_initial_stop_risk_ceiling_pct=float(factor_cap),
        same_symbol_initial_stop_risk_ceiling_pct=float(same_symbol_cap),
        margin_utilization_operating_floor_pct=float(margin_utilization_floor_pct),
        margin_utilization_ceiling_pct=float(margin_utilization_ceiling_pct),
        margin_level_reopen_pct=margin_level_from_utilization(margin_utilization_floor_pct),
        margin_level_entry_block_pct=margin_level_from_utilization(margin_utilization_ceiling_pct),
        portfolio_gap_stress_multiple=float(portfolio_gap_stress_multiple),
        factor_gap_stress_multiple=float(factor_gap_stress_multiple),
        portfolio_stressed_loss_budget_pct=float(portfolio_stressed_loss_budget_pct),
        factor_stressed_loss_budget_pct=float(factor_stressed_loss_budget_pct),
        full_size_position_capacity=int(math.floor(total_cap / per_trade_risk_pct + 1e-12)),
        full_size_positions_per_factor=int(math.floor(factor_cap / per_trade_risk_pct + 1e-12)),
    )


def exact_binary_trade_math(win_probability: float, payoff_r: float) -> Dict[str, float]:
    p = float(win_probability)
    b = float(payoff_r)
    if not (0.0 < p < 1.0) or not math.isfinite(b) or b <= 0.0:
        raise ValueError("win probability must be in (0,1) and payoff must be positive")
    q = 1.0 - p
    expectancy = p * b - q
    kelly = (b * p - q) / b
    return {
        "win_probability": p,
        "payoff_r": b,
        "break_even_win_probability": 1.0 / (b + 1.0),
        "gross_expectancy_r": expectancy,
        "binary_kelly_fraction": kelly,
    }


def deterministic_stress_table(caps: Iterable[float], multiples: Sequence[float]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for cap in caps:
        row: Dict[str, Any] = {"initial_stop_risk_pct": float(cap)}
        for multiple in multiples:
            row[f"loss_at_{multiple:g}x_pct"] = float(cap) * float(multiple)
        rows.append(row)
    return rows


def _simulate_scenario(
    *,
    caps: Sequence[float],
    paths: int,
    batches: int,
    per_trade_risk_pct: float,
    factor_cap_pct: float,
    win_probability: float,
    payoff_r: float,
    within_factor_correlation: float,
    factor_shock_probability: float,
    global_shock_probability: float,
    shock_loss_multiple: float,
    seed: int,
) -> List[Dict[str, Any]]:
    """Seeded beta-binomial common-factor sensitivity simulation.

    The beta-binomial latent probability produces positive dependence inside a
    factor group.  Groups are independent except for an explicit rare global
    shock.  It is intentionally a sensitivity model, not a fitted forecast.
    """
    import numpy as np

    if not (0.0 <= within_factor_correlation < 1.0):
        raise ValueError("within-factor correlation must be in [0,1)")
    rng = np.random.default_rng(seed)
    results: List[Dict[str, Any]] = []
    group_size = max(1, int(math.floor(factor_cap_pct / per_trade_risk_pct + 1e-12)))
    if within_factor_correlation > 0.0:
        concentration = 1.0 / within_factor_correlation - 1.0
        alpha = win_probability * concentration
        beta = (1.0 - win_probability) * concentration
    else:
        alpha = beta = 0.0

    for raw_cap in caps:
        position_count = max(1, int(math.floor(float(raw_cap) / per_trade_risk_pct + 1e-12)))
        sizes = [group_size] * (position_count // group_size)
        if position_count % group_size:
            sizes.append(position_count % group_size)
        equity = np.ones(paths, dtype=float)
        peak = np.ones(paths, dtype=float)
        max_drawdown = np.zeros(paths, dtype=float)
        for _ in range(batches):
            pnl_r = np.zeros(paths, dtype=float)
            for size in sizes:
                if within_factor_correlation > 0.0:
                    batch_p = rng.beta(alpha, beta, size=paths)
                else:
                    batch_p = np.full(paths, win_probability, dtype=float)
                wins = rng.binomial(size, batch_p)
                group_pnl_r = wins * payoff_r - (size - wins)
                factor_shock = rng.random(paths) < factor_shock_probability
                group_pnl_r[factor_shock] = -size * shock_loss_multiple
                pnl_r += group_pnl_r
            global_shock = rng.random(paths) < global_shock_probability
            pnl_r[global_shock] = -position_count * shock_loss_multiple
            period_return = pnl_r * per_trade_risk_pct / 100.0
            equity *= np.maximum(0.0, 1.0 + period_return)
            peak = np.maximum(peak, equity)
            max_drawdown = np.maximum(max_drawdown, 1.0 - np.divide(
                equity,
                peak,
                out=np.zeros_like(equity),
                where=peak > 0.0,
            ))
        results.append({
            "cap_pct": float(raw_cap),
            "full_size_positions": position_count,
            "factor_groups": len(sizes),
            "median_terminal_equity_multiple": float(np.median(equity)),
            "p05_terminal_equity_multiple": float(np.quantile(equity, 0.05)),
            "median_max_drawdown_pct": float(np.median(max_drawdown) * 100.0),
            "p95_max_drawdown_pct": float(np.quantile(max_drawdown, 0.95) * 100.0),
            "probability_max_drawdown_above_20_pct": float(np.mean(max_drawdown > 0.20)),
            "probability_max_drawdown_above_30_pct": float(np.mean(max_drawdown > 0.30)),
            "probability_terminal_equity_below_50_pct": float(np.mean(equity < 0.50)),
        })
    return results


def build_report(*, paths: int, batches: int, seed: int) -> Dict[str, Any]:
    envelope = derive_capital_envelope()
    trade_math = exact_binary_trade_math(0.35, 2.0)
    caps = list(DEFAULT_CAPS)
    scenarios = {
        "thin_edge_hierarchical": _simulate_scenario(
            caps=caps,
            paths=paths,
            batches=batches,
            per_trade_risk_pct=envelope.per_trade_initial_stop_risk_pct,
            factor_cap_pct=envelope.factor_group_initial_stop_risk_ceiling_pct,
            win_probability=0.35,
            payoff_r=2.0,
            within_factor_correlation=0.25,
            factor_shock_probability=0.001,
            global_shock_probability=0.0005,
            shock_loss_multiple=1.25,
            seed=seed,
        ),
        "stronger_edge_hierarchical_sensitivity": _simulate_scenario(
            caps=caps,
            paths=paths,
            batches=batches,
            per_trade_risk_pct=envelope.per_trade_initial_stop_risk_pct,
            factor_cap_pct=envelope.factor_group_initial_stop_risk_ceiling_pct,
            win_probability=0.40,
            payoff_r=2.0,
            within_factor_correlation=0.25,
            factor_shock_probability=0.001,
            global_shock_probability=0.0005,
            shock_loss_multiple=1.25,
            seed=seed + 1,
        ),
        "thin_edge_concentrated_sensitivity": _simulate_scenario(
            caps=caps,
            paths=paths,
            batches=batches,
            per_trade_risk_pct=envelope.per_trade_initial_stop_risk_pct,
            factor_cap_pct=90.0,
            win_probability=0.35,
            payoff_r=2.0,
            within_factor_correlation=0.50,
            factor_shock_probability=0.0025,
            global_shock_probability=0.001,
            shock_loss_multiple=1.25,
            seed=seed + 2,
        ),
    }
    return {
        "schema_version": "2026-07-11-capital-envelope-v1",
        "purpose": "offline sensitivity and provisional survivability calibration",
        "profitability_guarantee": False,
        "selection_rule": (
            "largest 0.25%-aligned total and factor ceilings whose deterministic "
            "2.5x gap losses do not exceed 20% portfolio and 5% factor budgets"
        ),
        "provisional_envelope": asdict(envelope),
        "thin_edge_trade_math": trade_math,
        "deterministic_stress": deterministic_stress_table(
            caps,
            (1.0, 1.5, 2.0, 2.5, 3.0),
        ),
        "simulation_contract": {
            "paths": paths,
            "batches": batches,
            "seed": seed,
            "note": (
                "Sensitivity only. Production cap must be frozen on tuning evidence and "
                "pass one-shot synchronized final-lockbox replay."
            ),
        },
        "scenarios": scenarios,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=int, default=5_000)
    parser.add_argument("--batches", type=int, default=130)
    parser.add_argument("--seed", type=int, default=20_260_711)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if args.paths < 100 or args.batches < 1:
        parser.error("--paths must be >=100 and --batches must be positive")
    report = build_report(paths=args.paths, batches=args.batches, seed=args.seed)
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
        print(path)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
