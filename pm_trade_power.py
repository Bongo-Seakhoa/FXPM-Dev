"""Power-derived trade-count requirements for specialist evidence.

The optimizer must not treat a folklore count such as 15 trades as equivalent
evidence across every symbol, timeframe and regime.  This module derives the
required *effective* observations from a frozen economic edge, pilot
dispersion, multiplicity, precision and power contract.  It does not inspect
final-lockbox outcomes.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import NormalDist
from typing import Any, Iterable, Optional

import numpy as np


TRADE_POWER_SCHEMA_VERSION = "fxpm-cell-power-v1"
_NORMAL = NormalDist()


@dataclass(frozen=True)
class TradePowerPolicy:
    minimum_economic_edge_r: float
    maximum_ci_half_width_r: float
    target_power: float = 0.80
    alpha_train: float = 0.05
    alpha_tuning: float = 0.05
    alpha_final: float = 0.05
    multiple_testing_adjustment: str = "sidak_effective_trials"
    pilot_dispersion: str = "mad_then_sample_std_r"
    schema_version: str = TRADE_POWER_SCHEMA_VERSION

    def validate(self) -> tuple[str, ...]:
        reasons: list[str] = []
        for name, value in (
            ("minimum_economic_edge_r", self.minimum_economic_edge_r),
            ("maximum_ci_half_width_r", self.maximum_ci_half_width_r),
        ):
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError):
                number = float("nan")
            if not math.isfinite(number) or number <= 0.0:
                reasons.append(f"{name.upper()}_INVALID")
        try:
            power = float(self.target_power)
        except (TypeError, ValueError, OverflowError):
            power = float("nan")
        if not math.isfinite(power) or not 0.5 < power < 1.0:
            reasons.append("TARGET_POWER_INVALID")
        for name, value in (
            ("alpha_train", self.alpha_train),
            ("alpha_tuning", self.alpha_tuning),
            ("alpha_final", self.alpha_final),
        ):
            try:
                alpha = float(value)
            except (TypeError, ValueError, OverflowError):
                alpha = float("nan")
            if not math.isfinite(alpha) or not 0.0 < alpha < 0.5:
                reasons.append(f"{name.upper()}_INVALID")
        if self.multiple_testing_adjustment not in {
            "sidak_effective_trials",
            "bonferroni_effective_trials",
        }:
            reasons.append("MULTIPLE_TESTING_ADJUSTMENT_INVALID")
        return tuple(reasons)


@dataclass(frozen=True)
class TradePowerRequirement:
    status: str
    reason: str
    sigma_r_robust: Optional[float]
    minimum_economic_edge_r: float
    maximum_ci_half_width_r: float
    target_power: float
    effective_trials: float
    alpha_train_nominal: float
    alpha_train_adjusted: Optional[float]
    alpha_tuning: float
    alpha_final: float
    required_effective_train_trades: Optional[int]
    required_effective_tuning_trades: Optional[int]
    required_effective_final_trades: Optional[int]
    reason_codes: tuple[str, ...] = ()
    schema_version: str = TRADE_POWER_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.status == "CALIBRATED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def robust_r_dispersion(values: Iterable[Any]) -> Optional[float]:
    """Return a finite robust pilot sigma without silently dropping corruption."""
    try:
        vector = np.asarray(list(values), dtype=np.float64)
    except (TypeError, ValueError, OverflowError):
        return None
    if vector.ndim != 1 or vector.size < 2 or not np.isfinite(vector).all():
        return None
    median = float(np.median(vector))
    mad_sigma = 1.4826 * float(np.median(np.abs(vector - median)))
    if math.isfinite(mad_sigma) and mad_sigma > np.finfo(np.float64).eps:
        return mad_sigma
    sample = float(np.std(vector, ddof=1))
    return sample if math.isfinite(sample) and sample > np.finfo(np.float64).eps else None


def adjusted_alpha(
    nominal_alpha: float,
    effective_trials: float,
    *,
    method: str,
) -> float:
    alpha = float(nominal_alpha)
    trials = float(effective_trials)
    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise ValueError("nominal alpha must be in (0, 1)")
    if not math.isfinite(trials) or trials < 1.0:
        raise ValueError("effective trials must be finite and at least one")
    if method == "bonferroni_effective_trials":
        value = alpha / trials
    elif method == "sidak_effective_trials":
        value = -math.expm1(math.log1p(-alpha) / trials)
    else:
        raise ValueError(f"unsupported multiple-testing adjustment: {method}")
    return max(np.finfo(np.float64).tiny, min(alpha, float(value)))


def _power_count(*, sigma: float, edge: float, alpha: float, power: float) -> int:
    z_alpha = _NORMAL.inv_cdf(1.0 - alpha)
    z_power = _NORMAL.inv_cdf(power)
    return int(math.ceil(((z_alpha + z_power) * sigma / edge) ** 2))


def _precision_count(*, sigma: float, half_width: float, alpha: float) -> int:
    z = _NORMAL.inv_cdf(1.0 - alpha / 2.0)
    return int(math.ceil((z * sigma / half_width) ** 2))


def derive_trade_power_requirement(
    *,
    sigma_r_robust: Any,
    effective_trials: Any,
    policy: TradePowerPolicy,
) -> TradePowerRequirement:
    """Derive frozen train/tuning/final effective-trade requirements."""
    policy_errors = policy.validate()
    try:
        sigma = float(sigma_r_robust)
    except (TypeError, ValueError, OverflowError):
        sigma = float("nan")
    try:
        trials = float(effective_trials)
    except (TypeError, ValueError, OverflowError):
        trials = float("nan")
    reasons = list(policy_errors)
    if not math.isfinite(sigma) or sigma <= 0.0:
        reasons.append("PILOT_SIGMA_R_INVALID")
    if not math.isfinite(trials) or trials < 1.0:
        reasons.append("EFFECTIVE_TRIALS_INVALID")
    common = {
        "sigma_r_robust": sigma if math.isfinite(sigma) and sigma > 0.0 else None,
        "minimum_economic_edge_r": float(policy.minimum_economic_edge_r),
        "maximum_ci_half_width_r": float(policy.maximum_ci_half_width_r),
        "target_power": float(policy.target_power),
        "effective_trials": trials,
        "alpha_train_nominal": float(policy.alpha_train),
        "alpha_tuning": float(policy.alpha_tuning),
        "alpha_final": float(policy.alpha_final),
    }
    if reasons:
        return TradePowerRequirement(
            status="UNAVAILABLE",
            reason="trade-power inputs are invalid",
            alpha_train_adjusted=None,
            required_effective_train_trades=None,
            required_effective_tuning_trades=None,
            required_effective_final_trades=None,
            reason_codes=tuple(dict.fromkeys(reasons)),
            **common,
        )
    try:
        alpha_adjusted = adjusted_alpha(
            policy.alpha_train,
            trials,
            method=policy.multiple_testing_adjustment,
        )
        train = _power_count(
            sigma=sigma,
            edge=float(policy.minimum_economic_edge_r),
            alpha=alpha_adjusted,
            power=float(policy.target_power),
        )
        tuning = _precision_count(
            sigma=sigma,
            half_width=float(policy.maximum_ci_half_width_r),
            alpha=float(policy.alpha_tuning),
        )
        final = _precision_count(
            sigma=sigma,
            half_width=float(policy.maximum_ci_half_width_r),
            alpha=float(policy.alpha_final),
        )
    except (ArithmeticError, ValueError, OverflowError) as exc:
        return TradePowerRequirement(
            status="UNAVAILABLE",
            reason=f"trade-power calculation failed: {exc}",
            alpha_train_adjusted=None,
            required_effective_train_trades=None,
            required_effective_tuning_trades=None,
            required_effective_final_trades=None,
            reason_codes=("POWER_CALCULATION_FAILED",),
            **common,
        )
    return TradePowerRequirement(
        status="CALIBRATED",
        reason="effective-trade requirements calibrated before validation",
        alpha_train_adjusted=alpha_adjusted,
        required_effective_train_trades=max(2, train),
        required_effective_tuning_trades=max(2, tuning),
        required_effective_final_trades=max(2, final),
        **common,
    )


__all__ = [
    "TRADE_POWER_SCHEMA_VERSION",
    "TradePowerPolicy",
    "TradePowerRequirement",
    "adjusted_alpha",
    "derive_trade_power_requirement",
    "robust_r_dispersion",
]
