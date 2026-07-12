"""Shared gross stop-risk admission for funded live and synchronized replay.

This module is deliberately small and deterministic.  Local specialists decide
whether they have an entry; this layer only protects the shared balance sheet.
It never nets opposing directions because stress-period hedges and broker stops
can execute at different prices or times.  Margin utilization is a separate
constraint and is intentionally not represented as stop risk here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Tuple

from pm_symbol import FIAT_CODES, infer_asset_class, normalize_symbol


RISK_ADMISSION_SCHEMA_VERSION = "2026-07-13-signed-portfolio-stop-equity-v3"
_EPSILON = 1e-10


def infer_factor_groups(symbol: str, asset_class: str = "") -> Tuple[str, ...]:
    """Return deterministic gross factor buckets for one canonical symbol.

    Explicit asset class is preferred, with the common symbol classifier as a
    fallback.  Every resolvable symbol receives at least one bucket; unknown
    instruments share an intentionally conservative unclassified bucket.
    """
    normalized = normalize_symbol(symbol)
    if not normalized:
        return ()
    resolved_class = infer_asset_class(
        normalized,
        broker_data={"asset_class": asset_class},
        default="unknown",
    )
    groups: set[str] = set()

    if resolved_class == "fx" and len(normalized) == 6:
        base, quote = normalized[:3], normalized[3:6]
        if base in FIAT_CODES and quote in FIAT_CODES:
            groups.update({f"CURRENCY:{base}", f"CURRENCY:{quote}"})
    elif resolved_class == "index":
        groups.add("GLOBAL_EQUITY_BETA")
        if normalized in {"US30", "US100", "US500", "NAS100", "USTEC", "SP500", "SPX500"}:
            groups.add("US_EQUITY_BETA")
        elif normalized in {"DE30", "DE40", "EU50", "ES35", "FR40", "UK100"}:
            groups.add("EUROPE_EQUITY_BETA")
        elif normalized in {"AU200", "AUS200", "HK50", "JP225", "NIKKEI", "CHINA50"}:
            groups.add("ASIA_PACIFIC_EQUITY_BETA")
    elif resolved_class == "equity":
        groups.update({"GLOBAL_EQUITY_BETA", "SINGLE_STOCK_BETA"})
    elif resolved_class == "metal":
        groups.add("PRECIOUS_METALS")
    elif resolved_class == "crypto":
        groups.add("CRYPTO_BETA")
    elif resolved_class == "energy":
        groups.update({"COMMODITY_BETA", "ENERGY_BETA"})

    # Fiat quote exposure is common to metals and crypto quoted in account
    # currencies (for example XAUUSD, XAUEUR and BTCUSD).
    if resolved_class in {"metal", "crypto", "energy"} and len(normalized) >= 6:
        quote = normalized[-3:]
        if quote in FIAT_CODES:
            groups.add(f"CURRENCY:{quote}")

    if not groups:
        groups.add("UNCLASSIFIED_MARKET_FACTOR")
    return tuple(sorted(groups))


@dataclass(frozen=True)
class StopRiskExposure:
    """One position's loss-at-stop as percent of one shared risk-capital snapshot."""

    symbol: str
    stop_risk_pct: float
    asset_class: str = ""
    exposure_id: str = ""


@dataclass(frozen=True)
class GrossRiskLimits:
    """The only allocation limits used by this shared stop-risk layer."""

    max_total_stop_risk_pct: float
    max_factor_group_stop_risk_pct: float


@dataclass(frozen=True)
class ConservativeRiskCapitalDecision:
    """Live/replay denominator: min(MTM equity, explicit portfolio stop equity)."""

    allow: bool
    code: str
    reason: str
    balance: float = 0.0
    equity: float = 0.0
    open_stop_risk_amount: float = 0.0
    stop_equity: float = 0.0
    risk_capital: float = 0.0
    schema_version: str = RISK_ADMISSION_SCHEMA_VERSION


@dataclass(frozen=True)
class GrossRiskDecision:
    allow: bool
    code: str
    reason: str
    current_total_stop_risk_pct: float = 0.0
    prospective_total_stop_risk_pct: float = 0.0
    candidate_groups: Tuple[str, ...] = ()
    current_group_stop_risk_pct: Mapping[str, float] = field(default_factory=dict)
    prospective_candidate_group_stop_risk_pct: Mapping[str, float] = field(default_factory=dict)
    schema_version: str = RISK_ADMISSION_SCHEMA_VERSION


@dataclass(frozen=True)
class ProjectedMarginDecision:
    """Admission result for margin usage; never interpreted as stop risk."""

    allow: bool
    code: str
    reason: str
    projected_margin_utilization_pct: float = 0.0
    projected_margin_level_pct: float = 0.0
    projected_free_margin_pct: float = 0.0
    schema_version: str = RISK_ADMISSION_SCHEMA_VERSION


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number <= 0.0:
        return None
    return number


def _invalid(reason: str, *, code: str = "RISK_INPUT_INVALID") -> GrossRiskDecision:
    return GrossRiskDecision(False, code, reason)


def conservative_risk_capital(
    *,
    balance: Any,
    equity: Any,
    portfolio_stop_equity: Any,
    open_stop_risk_amount: Any,
) -> ConservativeRiskCapitalDecision:
    """Calculate conservative capital from an explicit stop-equity snapshot.

    ``portfolio_stop_equity`` is authoritative and must already incorporate the
    signed outcome of every open position at its effective stop.  Gross
    ``open_stop_risk_amount`` remains a non-negative diagnostic used by the
    allocation layer; it is deliberately not subtracted from balance here
    because doing so destroys locked-in profits from trailing stops.
    """
    balance_value = _finite_positive(balance)
    equity_value = _finite_positive(equity)
    try:
        stop_equity = float(portfolio_stop_equity)
    except (TypeError, ValueError, OverflowError):
        stop_equity = float("nan")
    try:
        open_risk = float(open_stop_risk_amount)
    except (TypeError, ValueError, OverflowError):
        open_risk = float("nan")
    if (
        balance_value is None
        or equity_value is None
        or not math.isfinite(stop_equity)
        or not math.isfinite(open_risk)
        or open_risk < 0.0
    ):
        return ConservativeRiskCapitalDecision(
            False,
            "RISK_CAPITAL_INPUT_INVALID",
            "balance, equity, portfolio stop equity and gross open stop risk must be finite and valid",
        )
    risk_capital = min(equity_value, stop_equity)
    common = {
        "balance": balance_value,
        "equity": equity_value,
        "open_stop_risk_amount": open_risk,
        "stop_equity": stop_equity,
        "risk_capital": risk_capital,
    }
    if risk_capital <= 0.0:
        return ConservativeRiskCapitalDecision(
            False,
            "NO_POSITIVE_CONSERVATIVE_RISK_CAPITAL",
            "min(equity, portfolio stop equity) is not positive",
            **common,
        )
    return ConservativeRiskCapitalDecision(
        True,
        "ALLOW",
        "conservative risk capital is positive",
        **common,
    )


def evaluate_gross_stop_risk(
    open_exposures: Iterable[StopRiskExposure],
    candidate: StopRiskExposure,
    limits: GrossRiskLimits,
) -> GrossRiskDecision:
    """Apply total and gross factor-group stop-risk limits without netting.

    Invalid or incomplete open-position data fails closed. All inputs must be
    percentages of the same conservative risk-capital snapshot produced by
    :func:`conservative_risk_capital`; mixing fresh-equity and conservative
    denominators is a contract violation at the caller.
    """
    total_cap = _finite_positive(limits.max_total_stop_risk_pct)
    factor_cap = _finite_positive(limits.max_factor_group_stop_risk_pct)
    if total_cap is None or factor_cap is None or factor_cap > total_cap + _EPSILON:
        return _invalid(
            "risk limits must be finite and satisfy 0 < factor_group_cap <= total_stop_cap",
            code="RISK_POLICY_INVALID",
        )

    normalized: list[tuple[StopRiskExposure, str, float, Tuple[str, ...]]] = []
    for index, exposure in enumerate(tuple(open_exposures)):
        symbol = normalize_symbol(exposure.symbol)
        risk = _finite_positive(exposure.stop_risk_pct)
        groups = infer_factor_groups(symbol, exposure.asset_class) if symbol else ()
        if not symbol:
            return _invalid(f"open exposure {index} has no resolvable symbol")
        if risk is None:
            label = exposure.exposure_id or str(index)
            return _invalid(f"open exposure {label} has no finite positive stop risk")
        if not groups:
            return _invalid(f"open exposure {symbol} has no factor groups")
        normalized.append((exposure, symbol, risk, groups))

    candidate_symbol = normalize_symbol(candidate.symbol)
    candidate_risk = _finite_positive(candidate.stop_risk_pct)
    candidate_groups = infer_factor_groups(candidate_symbol, candidate.asset_class) if candidate_symbol else ()
    if not candidate_symbol:
        return _invalid("candidate has no resolvable symbol")
    if candidate_risk is None:
        return _invalid("candidate has no finite positive stop risk")
    if not candidate_groups:
        return _invalid(f"candidate {candidate_symbol} has no factor groups")

    current_total = sum(item[2] for item in normalized)
    group_risk: Dict[str, float] = {}
    for _exposure, _symbol, risk, groups in normalized:
        for group in groups:
            group_risk[group] = group_risk.get(group, 0.0) + risk

    prospective_total = current_total + candidate_risk
    prospective_candidate_groups = {
        group: group_risk.get(group, 0.0) + candidate_risk
        for group in candidate_groups
    }
    common = {
        "current_total_stop_risk_pct": current_total,
        "prospective_total_stop_risk_pct": prospective_total,
        "candidate_groups": candidate_groups,
        "current_group_stop_risk_pct": dict(sorted(group_risk.items())),
        "prospective_candidate_group_stop_risk_pct": dict(
            sorted(prospective_candidate_groups.items())
        ),
    }

    if current_total > total_cap + _EPSILON:
        return GrossRiskDecision(
            False,
            "EXISTING_TOTAL_STOP_RISK_CAP",
            f"existing total stop risk {current_total:.4f}% exceeds {total_cap:.4f}%",
            **common,
        )
    if prospective_total > total_cap + _EPSILON:
        return GrossRiskDecision(
            False,
            "TOTAL_STOP_RISK_CAP",
            f"prospective total stop risk {prospective_total:.4f}% exceeds {total_cap:.4f}%",
            **common,
        )

    existing_breaches = {
        group: risk for group, risk in group_risk.items() if risk > factor_cap + _EPSILON
    }
    if existing_breaches:
        first_group = sorted(existing_breaches)[0]
        return GrossRiskDecision(
            False,
            "EXISTING_FACTOR_GROUP_STOP_RISK_CAP",
            f"existing factor {first_group} stop risk {existing_breaches[first_group]:.4f}% exceeds {factor_cap:.4f}%",
            **common,
        )

    candidate_breaches = {
        group: risk
        for group, risk in prospective_candidate_groups.items()
        if risk > factor_cap + _EPSILON
    }
    if candidate_breaches:
        first_group = sorted(candidate_breaches)[0]
        return GrossRiskDecision(
            False,
            "FACTOR_GROUP_STOP_RISK_CAP",
            f"prospective factor {first_group} stop risk {candidate_breaches[first_group]:.4f}% exceeds {factor_cap:.4f}%",
            **common,
        )

    return GrossRiskDecision(
        True,
        "ALLOW",
        "gross total and factor-group stop risk are within limits",
        **common,
    )


def evaluate_projected_margin(
    *,
    equity: Any,
    current_used_margin: Any,
    candidate_required_margin: Any,
    max_margin_utilization_pct: Any,
) -> ProjectedMarginDecision:
    """Evaluate projected used margin on one fresh account snapshot.

    A single utilization ceiling determines the equivalent margin-level and
    free-margin boundaries, avoiding three independently drifting settings.
    """
    equity_value = _finite_positive(equity)
    candidate_margin = _finite_positive(candidate_required_margin)
    utilization_cap = _finite_positive(max_margin_utilization_pct)
    try:
        used_margin = float(current_used_margin)
    except (TypeError, ValueError, OverflowError):
        used_margin = float("nan")
    if (
        equity_value is None
        or candidate_margin is None
        or utilization_cap is None
        or utilization_cap >= 100.0
        or not math.isfinite(used_margin)
        or used_margin < 0.0
    ):
        return ProjectedMarginDecision(
            False,
            "MARGIN_INPUT_INVALID",
            "equity, used margin, candidate margin and utilization cap must be finite and valid",
        )

    projected_margin = used_margin + candidate_margin
    utilization = projected_margin / equity_value * 100.0
    margin_level = equity_value / projected_margin * 100.0
    free_margin = (equity_value - projected_margin) / equity_value * 100.0
    common = {
        "projected_margin_utilization_pct": utilization,
        "projected_margin_level_pct": margin_level,
        "projected_free_margin_pct": free_margin,
    }
    if utilization > utilization_cap + _EPSILON:
        return ProjectedMarginDecision(
            False,
            "PROJECTED_MARGIN_UTILIZATION_CAP",
            f"projected margin utilization {utilization:.4f}% exceeds {utilization_cap:.4f}%",
            **common,
        )
    return ProjectedMarginDecision(
        True,
        "ALLOW",
        "projected margin utilization is within limit",
        **common,
    )
