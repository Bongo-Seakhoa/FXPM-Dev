"""Offline sensitivity report for FxPM's capital-envelope axes.

This is an offline sensitivity tool.  It deliberately keeps three quantities
separate:

* initial-stop risk (loss if every open trade exits at its initial stop),
* margin utilization (used margin / equity), and
* gross/notional market exposure.

This tool does not choose a numeric ceiling.  A production recommendation can
only come from :mod:`pm_cap_calibration`: a fresh synchronized causal winner
book, a complete multi-axis Pareto frontier, and an external authorization over
one exact frontier member.  The Monte Carlo section is sensitivity analysis;
it must never be used to manufacture a cap or a promotion.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


DEFAULT_CAPS = (3.0, 5.0, 8.0, 10.0, 12.0, 20.0, 33.25, 50.0, 75.0, 90.0)
MAX_SENSITIVITY_PATHS = 100_000
MAX_SENSITIVITY_BATCHES = 10_000
MAX_SENSITIVITY_WORK = 25_000_000
MAX_SENSITIVITY_CAPS = 128
MAX_STRESS_MULTIPLES = 32
MAX_FULL_SIZE_POSITIONS = 10_000
MAX_FACTOR_GROUPS = 1_024
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_OUTPUT_PATH_CHARS = 1_024


def _is_link_or_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return path.is_symlink() or bool(getattr(info, "st_file_attributes", 0) & 0x400)


def _assert_no_link_ancestors(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    for component in reversed([absolute, *absolute.parents]):
        if component.exists() and _is_link_or_reparse(component):
            raise ValueError("output path cannot traverse a link or reparse point")


def _prepare_output_path(raw_path: str) -> Path:
    if not raw_path or len(raw_path) > MAX_OUTPUT_PATH_CHARS:
        raise ValueError("output path is empty or exceeds its length bound")
    path = Path(os.path.abspath(raw_path))
    existing = path.parent
    missing: list[Path] = []
    while not existing.exists():
        missing.append(existing)
        if existing.parent == existing:
            raise ValueError("output root does not exist")
        existing = existing.parent
    _assert_no_link_ancestors(existing)
    for directory in reversed(missing):
        directory.mkdir(exist_ok=False)
        if _is_link_or_reparse(directory):
            raise ValueError("output directory became a link or reparse point")
    _assert_no_link_ancestors(path.parent)
    if not path.parent.is_dir() or _is_link_or_reparse(path):
        raise ValueError("output path is not a safe regular-file target")
    return path


def _bounded_existing_bytes(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    chunks: list[bytes] = []
    try:
        before = os.fstat(descriptor)
        if before.st_nlink != 1 or before.st_size < 1 or before.st_size > MAX_REPORT_BYTES:
            raise ValueError("existing output is aliased or outside its byte bound")
        read = 0
        while True:
            block = os.read(
                descriptor, min(1024 * 1024, MAX_REPORT_BYTES + 1 - read)
            )
            if not block:
                break
            read += len(block)
            if read > MAX_REPORT_BYTES:
                raise ValueError("existing output exceeds its byte bound")
            chunks.append(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.stat()
    if (
        read != before.st_size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ctime_ns != before.st_ctime_ns
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
        or current.st_nlink != 1
    ):
        raise ValueError("existing output changed while reading")
    return b"".join(chunks)


@dataclass(frozen=True)
class CapitalEnvelope:
    per_trade_initial_stop_risk_pct: float
    recommendation_status: str
    total_initial_stop_risk_ceiling_pct: Optional[float]
    factor_group_initial_stop_risk_ceiling_pct: Optional[float]
    same_symbol_initial_stop_risk_ceiling_pct: Optional[float]
    margin_utilization_operating_floor_pct: Optional[float]
    margin_utilization_ceiling_pct: Optional[float]
    margin_level_reopen_pct: Optional[float]
    margin_level_entry_block_pct: Optional[float]
    gross_notional_to_equity_ceiling_pct: Optional[float]
    abs_net_notional_to_equity_ceiling_pct: Optional[float]
    full_size_position_capacity: Optional[int]
    full_size_positions_per_factor: Optional[int]
    evidence_required: tuple[str, ...]


def margin_level_from_utilization(utilization_pct: float) -> float:
    """Return MT5 margin level (equity / margin * 100) from utilization."""
    value = float(utilization_pct)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError("margin utilization must be finite and positive")
    return 10_000.0 / value


def floor_to_increment(value: float, increment: float) -> float:
    if not math.isfinite(value) or not math.isfinite(increment) or increment <= 0.0:
        raise ValueError(
            "value and increment must be finite; increment must be positive"
        )
    return math.floor((value + 1e-12) / increment) * increment


def derive_capital_envelope(
    *,
    per_trade_risk_pct: float = 0.25,
) -> CapitalEnvelope:
    """Return an explicitly withheld envelope for sensitivity reports.

    The old API accepted caller-chosen loss budgets and mechanically emitted
    8%.  Those knobs were a cap-selection oracle and have intentionally been
    removed.  This compatibility object contains no numeric recommendation.
    """

    if not 0.0 < float(per_trade_risk_pct) < 100.0 or not math.isfinite(
        float(per_trade_risk_pct)
    ):
        raise ValueError("per-trade risk must be finite and in (0,100)")

    return CapitalEnvelope(
        per_trade_initial_stop_risk_pct=float(per_trade_risk_pct),
        recommendation_status="WITHHELD_NO_FRESH_AUTHENTICATED_FRONTIER",
        total_initial_stop_risk_ceiling_pct=None,
        factor_group_initial_stop_risk_ceiling_pct=None,
        same_symbol_initial_stop_risk_ceiling_pct=None,
        margin_utilization_operating_floor_pct=None,
        margin_utilization_ceiling_pct=None,
        margin_level_reopen_pct=None,
        margin_level_entry_block_pct=None,
        gross_notional_to_equity_ceiling_pct=None,
        abs_net_notional_to_equity_ceiling_pct=None,
        full_size_position_capacity=None,
        full_size_positions_per_factor=None,
        evidence_required=(
            "fresh_authenticated_winner_book",
            "causal_outer_oos_portfolio_evidence",
            "synchronized_complete_calendar_daily_mtm",
            "precommitted_joint_and_factor_gap_stress",
            "gross_and_net_notional_paths",
            "margin_utilization_paths",
            "external_pareto_member_authorization",
        ),
    )


def exact_binary_trade_math(
    win_probability: float, payoff_r: float
) -> Dict[str, float]:
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


def deterministic_stress_table(
    caps: Iterable[float], multiples: Sequence[float]
) -> List[Dict[str, Any]]:
    bounded_caps = list(itertools.islice(caps, MAX_SENSITIVITY_CAPS + 1))
    if not bounded_caps or len(bounded_caps) > MAX_SENSITIVITY_CAPS:
        raise ValueError("stress cap roster is empty or exceeds its bound")
    if not multiples or len(multiples) > MAX_STRESS_MULTIPLES:
        raise ValueError("stress-multiple roster is empty or exceeds its bound")
    normalized_caps = [float(value) for value in bounded_caps]
    normalized_multiples = [float(value) for value in multiples]
    if any(not math.isfinite(value) or value <= 0.0 for value in normalized_caps):
        raise ValueError("stress caps must be finite and positive")
    if any(not math.isfinite(value) or value <= 0.0 for value in normalized_multiples):
        raise ValueError("stress multiples must be finite and positive")
    rows: List[Dict[str, Any]] = []
    for cap in normalized_caps:
        row: Dict[str, Any] = {"initial_stop_risk_pct": float(cap)}
        for multiple in normalized_multiples:
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

    if type(paths) is not int or not 100 <= paths <= MAX_SENSITIVITY_PATHS:
        raise ValueError("sensitivity paths are outside the bounded range")
    if type(batches) is not int or not 1 <= batches <= MAX_SENSITIVITY_BATCHES:
        raise ValueError("sensitivity batches are outside the bounded range")
    bounded_caps = list(itertools.islice(iter(caps), MAX_SENSITIVITY_CAPS + 1))
    if not 1 <= len(bounded_caps) <= MAX_SENSITIVITY_CAPS:
        raise ValueError("sensitivity cap roster is outside its bound")
    normalized_caps = [float(value) for value in bounded_caps]
    if any(
        not math.isfinite(value) or not 0.0 < value <= 100.0
        for value in normalized_caps
    ):
        raise ValueError("sensitivity caps must be finite and positive")
    if not math.isfinite(per_trade_risk_pct) or not 0.0 < per_trade_risk_pct <= 10.0:
        raise ValueError("per-trade sensitivity risk must be finite and positive")
    if not math.isfinite(factor_cap_pct) or not 0.0 < factor_cap_pct <= 100.0:
        raise ValueError("factor sensitivity cap must be finite and positive")
    if (
        not 0.0 < win_probability < 1.0
        or not math.isfinite(payoff_r)
        or payoff_r <= 0.0
    ):
        raise ValueError("sensitivity payoff assumptions are invalid")
    if not (0.0 <= within_factor_correlation < 1.0):
        raise ValueError("within-factor correlation must be in [0,1)")
    if not 0.0 <= factor_shock_probability <= 1.0:
        raise ValueError("factor shock probability must be in [0,1]")
    if not 0.0 <= global_shock_probability <= 1.0:
        raise ValueError("global shock probability must be in [0,1]")
    if not math.isfinite(shock_loss_multiple) or shock_loss_multiple <= 0.0:
        raise ValueError("shock loss multiple must be finite and positive")
    if type(seed) is not int or not 0 <= seed <= (2**63 - 1):
        raise ValueError("sensitivity seed is outside its bounded range")
    rng = np.random.default_rng(seed)
    results: List[Dict[str, Any]] = []
    group_size = max(1, int(math.floor(factor_cap_pct / per_trade_risk_pct + 1e-12)))
    if group_size > MAX_FULL_SIZE_POSITIONS:
        raise ValueError("factor-cap/per-trade ratio exceeds its position bound")
    scenario_geometry: list[tuple[float, int, int]] = []
    for raw_cap in normalized_caps:
        position_count = max(
            1, int(math.floor(raw_cap / per_trade_risk_pct + 1e-12))
        )
        if position_count > MAX_FULL_SIZE_POSITIONS:
            raise ValueError("cap/per-trade ratio exceeds its position bound")
        group_count = int(math.ceil(position_count / group_size))
        if group_count > MAX_FACTOR_GROUPS:
            raise ValueError("sensitivity factor-group count exceeds its bound")
        scenario_geometry.append((raw_cap, position_count, group_count))
    # Each factor group can allocate beta/full, binomial and factor-shock arrays;
    # each cap/batch also allocates global-shock/equity arrays.  Bound the actual
    # dominant vector work, not merely paths*batches.
    random_vector_draws = paths * batches * sum(
        3 * group_count + 2 for _, _, group_count in scenario_geometry
    )
    if random_vector_draws > MAX_SENSITIVITY_WORK:
        raise ValueError("sensitivity cap/scenario/factor work exceeds its bound")
    if within_factor_correlation > 0.0:
        concentration = 1.0 / within_factor_correlation - 1.0
        alpha = win_probability * concentration
        beta = (1.0 - win_probability) * concentration
    else:
        alpha = beta = 0.0

    for raw_cap, position_count, expected_group_count in scenario_geometry:
        sizes = [group_size] * (position_count // group_size)
        if position_count % group_size:
            sizes.append(position_count % group_size)
        if len(sizes) != expected_group_count or len(sizes) > MAX_FACTOR_GROUPS:
            raise ValueError("sensitivity factor geometry is inconsistent")
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
            max_drawdown = np.maximum(
                max_drawdown,
                1.0
                - np.divide(
                    equity,
                    peak,
                    out=np.zeros_like(equity),
                    where=peak > 0.0,
                ),
            )
        results.append(
            {
                "cap_pct": float(raw_cap),
                "full_size_positions": position_count,
                "factor_groups": len(sizes),
                "median_terminal_equity_multiple": float(np.median(equity)),
                "p05_terminal_equity_multiple": float(np.quantile(equity, 0.05)),
                "median_max_drawdown_pct": float(np.median(max_drawdown) * 100.0),
                "p95_max_drawdown_pct": float(np.quantile(max_drawdown, 0.95) * 100.0),
                "probability_max_drawdown_above_20_pct": float(
                    np.mean(max_drawdown > 0.20)
                ),
                "probability_max_drawdown_above_30_pct": float(
                    np.mean(max_drawdown > 0.30)
                ),
                "probability_terminal_equity_below_50_pct": float(
                    np.mean(equity < 0.50)
                ),
            }
        )
    return results


def build_report(*, paths: int, batches: int, seed: int) -> Dict[str, Any]:
    if type(paths) is not int or type(batches) is not int or type(seed) is not int:
        raise ValueError("sensitivity paths, batches and seed must be integers")
    if not 100 <= paths <= MAX_SENSITIVITY_PATHS:
        raise ValueError("sensitivity paths are outside the bounded range")
    if not 1 <= batches <= MAX_SENSITIVITY_BATCHES:
        raise ValueError("sensitivity batches are outside the bounded range")
    envelope = derive_capital_envelope()
    trade_math = exact_binary_trade_math(0.35, 2.0)
    caps = list(DEFAULT_CAPS)
    sensitivity_factor_cap_pct = 2.0
    scenarios = {
        "thin_edge_hierarchical": _simulate_scenario(
            caps=caps,
            paths=paths,
            batches=batches,
            per_trade_risk_pct=envelope.per_trade_initial_stop_risk_pct,
            factor_cap_pct=sensitivity_factor_cap_pct,
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
            factor_cap_pct=sensitivity_factor_cap_pct,
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
    report = {
        "schema_version": "fxpm.capital-envelope-sensitivity.v2",
        "purpose": "offline sensitivity only; no numeric recommendation",
        "profitability_guarantee": False,
        "selection_rule": "NONE_RECOMMENDATION_WITHHELD_PENDING_AUTHENTICATED_PARETO_FRONTIER",
        "capital_envelope": asdict(envelope),
        "thin_edge_trade_math": trade_math,
        "deterministic_stress": deterministic_stress_table(
            caps,
            (1.0, 1.5, 2.0, 2.5, 3.0),
        ),
        "simulation_contract": {
            "paths": paths,
            "batches": batches,
            "seed": seed,
            "factor_cap_pct_sensitivity_assumption": sensitivity_factor_cap_pct,
            "note": (
                "Sensitivity only. Production configuration requires a fresh accepted "
                "winner book, causal portfolio outer-OOS evidence, synchronized multi-axis "
                "stress paths and external authorization of one Pareto member."
            ),
        },
        "scenarios": scenarios,
    }
    if (
        len(json.dumps(report, sort_keys=True, allow_nan=False).encode("utf-8"))
        > MAX_REPORT_BYTES
    ):
        raise ValueError("sensitivity report exceeds its byte bound")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=int, default=500)
    parser.add_argument("--batches", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20_260_711)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if args.output and len(args.output) > MAX_OUTPUT_PATH_CHARS:
        parser.error("--output path exceeds its length bound")
    try:
        report = build_report(paths=args.paths, batches=args.batches, seed=args.seed)
    except ValueError as exc:
        parser.error(str(exc))
    payload = json.dumps(report, indent=2, sort_keys=True, allow_nan=False)
    encoded = (payload + "\n").encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        parser.error("sensitivity report exceeds its byte bound")
    if args.output:
        try:
            path = _prepare_output_path(args.output)
        except ValueError as exc:
            parser.error(str(exc))
        if path.exists():
            try:
                existing = _bounded_existing_bytes(path)
            except ValueError as exc:
                parser.error(str(exc))
            if not path.is_file() or existing != encoded:
                parser.error("--output already exists with different bytes")
        else:
            descriptor = os.open(
                path,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_BINARY", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        print(path)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
