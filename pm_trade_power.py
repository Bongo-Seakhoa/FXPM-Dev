"""Power-derived trade-count requirements for specialist evidence.

The optimizer must not treat a folklore count such as 15 trades as equivalent
evidence across every symbol, timeframe and regime.  This module derives the
required *effective* observations from a frozen economic edge, pilot
dispersion, multiplicity, precision and power contract.  It does not inspect
final-lockbox outcomes.
"""

from __future__ import annotations

import math
import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from statistics import NormalDist
from typing import Any, Iterable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from pm_sample_dependence import effective_sample_evidence
from pm_search_outcomes import SearchOutcomeKind


TRADE_POWER_SCHEMA_VERSION = "fxpm-cell-power-v3"
TRADE_SAMPLE_SCHEMA_VERSION = "fxpm-net-r-sample-v4"
TRADE_POWER_DECISION_SCHEMA_VERSION = "fxpm-trade-power-decision-v3"
TRADE_POWER_MULTIPLICITY_VERSION = "fxpm-trade-power-multiplicity-v1"
TRADE_POWER_PARTITION_VERSION = "fxpm-trade-power-partition-v1"
TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION = "fxpm-trade-evidence-source-v4"
NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION = "fxpm-numerical-execution-cost-v2"
REQUIRED_SWAP_ACCRUAL_CONTRACT = "causal_swap_accrual_v1"
REQUIRED_SWAP_PROJECTION_CONTRACT = "fxpm-swap-preopen-close-projection-v1"
REQUIRED_TRADE_RECORD_SCHEMA = "2026-07-14-position-events-v2"
_NORMAL = NormalDist()
_PRODUCTION_PARTITIONS = {
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
}
_EXPECTED_SAMPLE_UNAVAILABLE_CODES = {
    "INSUFFICIENT_RAW_OBSERVATIONS",
    "VARIANCE_UNAVAILABLE",
}


@dataclass(frozen=True)
class TradePowerPolicy:
    minimum_economic_edge_r: float
    maximum_ci_half_width_r: float
    target_power: float = 0.80
    alpha_train: float = 0.05
    alpha_tuning: float = 0.05
    alpha_final: float = 0.05
    multiple_testing_adjustment: str = "sidak_effective_trials"
    pilot_dispersion: str = "max_mad_sample_std_r"
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
        if self.pilot_dispersion != "max_mad_sample_std_r":
            reasons.append("PILOT_DISPERSION_METHOD_INVALID")
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
    multiple_testing_adjustment: str = ""
    pilot_dispersion_method: str = ""
    policy_schema_version: str = ""
    multiplicity_method: str = ""
    calibration_partition: str = "unspecified"
    calibration_sample_sha256: Optional[str] = None
    calibration_source_contract_sha256: Optional[str] = None
    calibration_partition_source_contract_sha256: Optional[str] = None
    source_contract_schema_version: str = ""
    target_bars_sha256: str = ""
    target_sidecar_sha256: str = ""
    financing_schedule_sha256: str = ""
    projection_contract_version: str = ""
    numerical_execution_cost_policy_sha256: str = ""
    calibration_numerical_execution_cost_attestation_sha256: str = ""
    swap_accrual_contract_version: str = ""
    trade_record_schema_version: str = ""
    swap_provenance_class: str = ""
    historical_financing_provenance: str = ""
    frozen: bool = False
    attempt_ledger_payload_sha256: str = ""
    attempts_sha256: str = ""
    attempted_trials_exact: int = 0
    multiplicity_receipt_sha256: str = ""
    calibration_partition_contract_sha256: str = ""
    calibration_start_utc: str = ""
    calibration_end_utc: str = ""
    calibration_horizon_receipt_sha256: str = ""
    calibration_event_axis_sha256: str = ""
    calibration_net_r_values_sha256: str = ""
    calibration_holding_intervals_sha256: str = ""
    schema_version: str = TRADE_POWER_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.status == "CALIBRATED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradeEvidenceSourceContract:
    """Compact attestation for the Backtester inputs underlying net R."""

    # ``source_contract_sha256`` intentionally excludes the partition-local
    # projection digest.  Train, tuning and final are distinct slices of the
    # same authenticated bars artifact, so their projected event axes differ
    # even though their immutable source and cost policy must not.
    source_contract_sha256: str
    partition_source_contract_sha256: str
    target_bars_sha256: str
    target_sidecar_sha256: str
    financing_schedule_sha256: str
    projection_contract_version: str
    projection_sha256: str
    numerical_execution_cost_policy_sha256: str
    numerical_execution_cost_attestation_sha256: str
    swap_accrual_contract_version: str
    trade_record_schema_version: str
    swap_status: str
    swap_provenance_class: str
    historical_financing_provenance: str
    execution_state_compounding: bool
    schema_version: str = TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradeSampleEvidence:
    """Compact dependence-aware evidence from strict post-cost trade records."""

    status: str
    outcome_kind: SearchOutcomeKind
    reason: str
    raw_trade_records: int
    usable_trades: int
    minimum_observations_required: int
    mean_net_r: Optional[float]
    sigma_r_robust: Optional[float]
    effective_trades: float
    net_r_min: Optional[float]
    net_r_max: Optional[float]
    net_r_values_sha256: str
    holding_intervals_sha256: str
    position_ids_sha256: str
    event_order_sha256: str
    event_start_utc: str
    event_end_utc: str
    sample_sha256: str
    source_contract: Optional[TradeEvidenceSourceContract] = None
    dependence_evidence: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()
    net_r_contract: str = "strict_post_cost_pnl_dollars_divided_by_entry_risk_v1"
    schema_version: str = TRADE_SAMPLE_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return (
            self.status == "MEASURED" and self.outcome_kind is SearchOutcomeKind.VALID
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "outcome_kind": self.outcome_kind.value,
            "reason": self.reason,
            "reason_codes": list(self.reason_codes),
            "raw_trade_records": int(self.raw_trade_records),
            "usable_trades": int(self.usable_trades),
            "minimum_observations_required": int(self.minimum_observations_required),
            "mean_net_r": self.mean_net_r,
            "sigma_r_robust": self.sigma_r_robust,
            "effective_trades": float(self.effective_trades),
            "net_r_min": self.net_r_min,
            "net_r_max": self.net_r_max,
            "net_r_values_sha256": self.net_r_values_sha256,
            "holding_intervals_sha256": self.holding_intervals_sha256,
            "position_ids_sha256": self.position_ids_sha256,
            "event_order_sha256": self.event_order_sha256,
            "event_start_utc": self.event_start_utc,
            "event_end_utc": self.event_end_utc,
            "sample_sha256": self.sample_sha256,
            "source_contract": (
                self.source_contract.to_dict()
                if self.source_contract is not None
                else None
            ),
            "net_r_contract": self.net_r_contract,
            "dependence_evidence": dict(self.dependence_evidence),
        }


@dataclass(frozen=True)
class TradePowerDecision:
    """One partition verdict against an immutable train-calibrated requirement."""

    status: str
    outcome_kind: SearchOutcomeKind
    partition: str
    reason: str
    reason_codes: tuple[str, ...]
    observed_effective_trades: float
    required_effective_trades: Optional[int]
    sample_evidence: Optional[TradeSampleEvidence]
    frozen_requirement: Optional[TradePowerRequirement]
    partition_contract: Optional[Mapping[str, Any]] = None
    requirement_recalibrated: bool = False
    requirement_sigma_source: str = "optimizer_train_frozen"
    schema_version: str = TRADE_POWER_DECISION_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.status == "PASS" and self.outcome_kind is SearchOutcomeKind.VALID

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "outcome_kind": self.outcome_kind.value,
            "partition": self.partition,
            "passed": self.passed,
            "reason": self.reason,
            "reason_codes": list(self.reason_codes),
            "observed_effective_trades": float(self.observed_effective_trades),
            "required_effective_trades": self.required_effective_trades,
            "requirement_recalibrated": self.requirement_recalibrated,
            "requirement_sigma_source": self.requirement_sigma_source,
            "sample_evidence": (
                self.sample_evidence.to_dict()
                if self.sample_evidence is not None
                else None
            ),
            "frozen_requirement": (
                self.frozen_requirement.to_dict()
                if self.frozen_requirement is not None
                else None
            ),
            "partition_contract": (
                dict(self.partition_contract)
                if self.partition_contract is not None
                else None
            ),
        }


def trade_power_requirement_sha256(value: Any) -> str:
    payload = value.to_dict() if isinstance(value, TradePowerRequirement) else value
    if not isinstance(payload, Mapping):
        raise TypeError("trade-power requirement must be a mapping")
    if payload.get("schema_version") != TRADE_POWER_SCHEMA_VERSION:
        raise ValueError("trade-power requirement schema is stale")
    return _sha256_json(dict(payload))


def trade_power_decision_sha256(value: Any) -> str:
    payload = value.to_dict() if isinstance(value, TradePowerDecision) else value
    if not isinstance(payload, Mapping):
        raise TypeError("trade-power decision must be a mapping")
    if payload.get("schema_version") != TRADE_POWER_DECISION_SCHEMA_VERSION:
        raise ValueError("trade-power decision schema is stale")
    return _sha256_json(dict(payload))


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
    sample = float(np.std(vector, ddof=1))
    candidates = [
        value
        for value in (mad_sigma, sample)
        if math.isfinite(value) and value > np.finfo(np.float64).eps
    ]
    return max(candidates) if candidates else None


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


def _finite_report_value(value: Any) -> float:
    """Return a JSON-safe reported number without creating a valid fallback."""

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) else 0.0


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
        "minimum_economic_edge_r": _finite_report_value(policy.minimum_economic_edge_r),
        "maximum_ci_half_width_r": _finite_report_value(policy.maximum_ci_half_width_r),
        "target_power": _finite_report_value(policy.target_power),
        "effective_trials": trials if math.isfinite(trials) else 0.0,
        "alpha_train_nominal": _finite_report_value(policy.alpha_train),
        "alpha_tuning": _finite_report_value(policy.alpha_tuning),
        "alpha_final": _finite_report_value(policy.alpha_final),
        "multiple_testing_adjustment": str(policy.multiple_testing_adjustment),
        "pilot_dispersion_method": str(policy.pilot_dispersion),
        "policy_schema_version": str(policy.schema_version),
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


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8", errors="strict")
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


_NUMERICAL_COST_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "use_spread",
        "spread_mode",
        "spread_values_sha256",
        "instrument_spec_sha256",
        "policy_sha256",
        "attestation_sha256",
    }
)


def build_numerical_execution_cost_attestation(
    *,
    use_spread: bool,
    spread_mode: str,
    spread_values_sha256: str,
    instrument_spec_sha256: str,
) -> dict[str, Any]:
    """Build the exact immutable numerical spread-cost payload required by v2.

    The caller remains responsible for deriving the two hashes from the
    actual arrays/model inputs used by the Backtester.  This helper only
    canonicalizes and seals those inputs; it cannot manufacture upstream
    execution evidence after the fact.
    """

    if not isinstance(use_spread, (bool, np.bool_)):
        raise ValueError("use_spread must be an explicit boolean")
    mode = str(spread_mode or "").strip()
    if not mode:
        raise ValueError("spread_mode is missing")
    hashes = {
        "spread_values_sha256": str(spread_values_sha256 or "").lower(),
        "instrument_spec_sha256": str(instrument_spec_sha256 or "").lower(),
    }
    if not all(_is_sha256(value) for value in hashes.values()):
        raise ValueError("numerical execution-cost hashes are malformed")
    policy: dict[str, Any] = {
        "schema_version": NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION,
        "use_spread": bool(use_spread),
        "spread_mode": mode,
        "instrument_spec_sha256": hashes["instrument_spec_sha256"],
    }
    policy_sha256 = _sha256_json(policy)
    canonical: dict[str, Any] = {
        **policy,
        "spread_values_sha256": hashes["spread_values_sha256"],
        "policy_sha256": policy_sha256,
    }
    return {
        **canonical,
        "attestation_sha256": _sha256_json(canonical),
    }


def _validate_numerical_execution_cost_attestation(
    value: Any,
) -> tuple[Optional[str], Optional[str], str, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        return (
            None,
            None,
            "numerical execution-cost attestation is missing",
            ("SOURCE_CONTRACT_NUMERICAL_COST_MISSING",),
        )
    if set(value) != _NUMERICAL_COST_ATTESTATION_FIELDS:
        return (
            None,
            None,
            "numerical execution-cost fields are incomplete or unbound",
            ("SOURCE_CONTRACT_NUMERICAL_COST_INVALID",),
        )
    if str(value.get("schema_version") or "") != (
        NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION
    ):
        return (
            None,
            None,
            "numerical execution-cost schema is missing or stale",
            ("SOURCE_CONTRACT_NUMERICAL_COST_SCHEMA_INVALID",),
        )
    if value.get("use_spread") is not True:
        return (
            None,
            None,
            "production spread cost is disabled or ambiguous",
            ("SOURCE_CONTRACT_NUMERICAL_COST_INVALID",),
        )
    mode = str(value.get("spread_mode") or "").strip()
    if not mode:
        return (
            None,
            None,
            "numerical spread mode is missing",
            ("SOURCE_CONTRACT_NUMERICAL_COST_INVALID",),
        )
    hashes = {
        name: str(value.get(name) or "").lower()
        for name in (
            "spread_values_sha256",
            "instrument_spec_sha256",
        )
    }
    if not all(_is_sha256(item) for item in hashes.values()):
        return (
            None,
            None,
            "numerical execution-cost hashes are malformed",
            ("SOURCE_CONTRACT_NUMERICAL_COST_INVALID",),
        )
    policy = {
        "schema_version": NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION,
        "use_spread": True,
        "spread_mode": mode,
        "instrument_spec_sha256": hashes["instrument_spec_sha256"],
    }
    expected_policy = _sha256_json(policy)
    observed_policy = str(value.get("policy_sha256") or "").lower()
    if not _is_sha256(observed_policy) or observed_policy != expected_policy:
        return (
            None,
            None,
            "numerical execution-cost policy hash does not verify",
            ("SOURCE_CONTRACT_NUMERICAL_COST_HASH_INVALID",),
        )
    canonical = {
        **policy,
        "spread_values_sha256": hashes["spread_values_sha256"],
        "policy_sha256": expected_policy,
    }
    expected = _sha256_json(canonical)
    observed = str(value.get("attestation_sha256") or "").lower()
    if not _is_sha256(observed) or observed != expected:
        return (
            None,
            None,
            "numerical execution-cost attestation hash does not verify",
            ("SOURCE_CONTRACT_NUMERICAL_COST_HASH_INVALID",),
        )
    return expected_policy, expected, "", ()


def _validate_trade_evidence_source_contract(
    value: Any,
) -> tuple[Optional[TradeEvidenceSourceContract], str, tuple[str, ...]]:
    """Validate the exact execution, swap, schema and provenance attestation."""

    if value is None:
        return (
            None,
            "trade evidence source contract is missing",
            ("SOURCE_CONTRACT_MISSING",),
        )
    if not isinstance(value, Mapping):
        return (
            None,
            "trade evidence source contract is not a mapping",
            ("SOURCE_CONTRACT_INVALID",),
        )
    if str(value.get("schema_version") or "") != (
        TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION
    ):
        return (
            None,
            "trade evidence source contract schema is missing or stale",
            ("SOURCE_CONTRACT_SCHEMA_INVALID",),
        )
    timing = value.get("execution_cost_timing_contract")
    swap = value.get("swap_accrual_contract")
    if not isinstance(timing, Mapping) or not isinstance(swap, Mapping):
        return (
            None,
            "execution timing or swap source contract is missing",
            ("SOURCE_CONTRACT_INVALID",),
        )
    timing_swap_version = str(timing.get("swap_accrual") or "")
    swap_version = str(swap.get("contract_version") or "")
    if (
        set(timing) != {"swap_accrual"}
        or timing_swap_version != REQUIRED_SWAP_ACCRUAL_CONTRACT
        or swap_version != REQUIRED_SWAP_ACCRUAL_CONTRACT
    ):
        return (
            None,
            "execution timing contract is legacy, missing, or inconsistent",
            ("SOURCE_CONTRACT_EXECUTION_TIMING_INVALID",),
        )
    trade_schema = str(value.get("trade_record_schema_version") or "")
    if trade_schema != REQUIRED_TRADE_RECORD_SCHEMA:
        return (
            None,
            "trade-record schema attestation is missing or stale",
            ("SOURCE_CONTRACT_TRADE_SCHEMA_INVALID",),
        )
    cost_policy_sha256, cost_attestation_sha256, cost_reason, cost_codes = (
        _validate_numerical_execution_cost_attestation(
            value.get("numerical_execution_cost_attestation")
        )
    )
    if cost_policy_sha256 is None or cost_attestation_sha256 is None:
        return None, cost_reason, cost_codes
    raw_binding = value.get("source_binding")
    if not isinstance(raw_binding, Mapping):
        return (
            None,
            "source binding evidence is missing or malformed",
            ("SOURCE_CONTRACT_BINDING_INVALID",),
        )
    required_common_bindings = (
        "price_activation_manifest_sha256",
        "terms_activation_manifest_sha256",
        "terms_snapshot_sha256",
        "schedule_sha256",
        "target_bars_sha256",
        "target_sidecar_sha256",
    )
    source_binding = {
        name: str(raw_binding.get(name) or "").lower()
        for name in required_common_bindings
    }
    if not all(_is_sha256(value) for value in source_binding.values()):
        return (
            None,
            "source binding evidence is incomplete or malformed",
            ("SOURCE_CONTRACT_BINDING_INVALID",),
        )
    status = str(swap.get("status") or "").upper()
    provenance = str(swap.get("provenance_status") or "").upper()
    compounding = swap.get("execution_state_compounding") is True
    source_authenticated = swap.get("source_provenance_authenticated") is True
    stress_approved = swap.get("stress_policy_explicitly_approved") is True
    stress_immutable = swap.get("stress_policy_immutable") is True
    stress_policy_sha256 = str(swap.get("stress_policy_sha256") or "").lower()
    broker_disabled = swap.get("broker_swap_disabled") is True
    historical_rates_authenticated = swap.get("historical_rates_authenticated") is True
    timestamp_source = str(swap.get("timestamp_source") or "")
    projection_contract = str(swap.get("projection_contract") or "")
    projection_sha256 = str(swap.get("projection_sha256") or "").lower()
    if projection_contract != REQUIRED_SWAP_PROJECTION_CONTRACT or not _is_sha256(
        projection_sha256
    ):
        return (
            None,
            "financing projection contract is missing or stale",
            ("SOURCE_CONTRACT_PROJECTION_INVALID",),
        )
    if not compounding:
        return (
            None,
            "swap cashflows are not compounded in execution state",
            ("SOURCE_CONTRACT_SWAP_COMPOUNDING_INVALID",),
        )
    if status == "AVAILABLE" and provenance == "AUTHENTICATED":
        if (
            not source_authenticated
            or not historical_rates_authenticated
            or timestamp_source != "_BAR_CLOSE_TS"
        ):
            return (
                None,
                "authenticated current swap terms lack historical-rate proof",
                ("SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",),
            )
        provenance_class = "AUTHENTICATED"
        historical_financing_provenance = "AUTHENTICATED_HISTORICAL_RATES"
        historical_rates_sha256 = str(
            raw_binding.get("historical_rates_sha256") or ""
        ).lower()
        if not _is_sha256(historical_rates_sha256):
            return (
                None,
                "historical swap-rate binding is missing",
                ("SOURCE_CONTRACT_BINDING_INVALID",),
            )
        source_binding["historical_rates_sha256"] = historical_rates_sha256
    elif status == "AVAILABLE" and provenance == "STRESS_APPROVED":
        if (
            not stress_approved
            or not stress_immutable
            or not _is_sha256(stress_policy_sha256)
            or timestamp_source != "_BAR_CLOSE_TS"
        ):
            return (
                None,
                "stress swap provenance lacks immutable explicit approval",
                ("SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",),
            )
        provenance_class = "STRESS_APPROVED"
        historical_financing_provenance = "IMMUTABLE_STRESS_APPROVED"
        source_binding["stress_policy_sha256"] = stress_policy_sha256
    elif status == "DISABLED" and provenance == "AUTHENTICATED":
        if not source_authenticated or not broker_disabled:
            return (
                None,
                "disabled swap is not authenticated as a broker term",
                ("SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",),
            )
        provenance_class = "AUTHENTICATED_BROKER_DISABLED"
        historical_financing_provenance = "AUTHENTICATED_BROKER_DISABLED"
        disabled_proof_sha256 = str(
            raw_binding.get("broker_disabled_proof_sha256") or ""
        ).lower()
        if not _is_sha256(disabled_proof_sha256):
            return (
                None,
                "authenticated broker-disabled proof is missing",
                ("SOURCE_CONTRACT_BINDING_INVALID",),
            )
        source_binding["broker_disabled_proof_sha256"] = disabled_proof_sha256
    else:
        return (
            None,
            "swap provenance is unavailable, legacy, or unapproved",
            ("SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",),
        )
    canonical = {
        "schema_version": TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
        "source_binding": source_binding,
        "numerical_execution_cost_policy_sha256": cost_policy_sha256,
        "swap_accrual_contract_version": swap_version,
        "projection_contract_version": projection_contract,
        "trade_record_schema_version": trade_schema,
        "swap_status": status,
        "swap_provenance_class": provenance_class,
        "historical_financing_provenance": historical_financing_provenance,
        "execution_state_compounding": True,
        "timestamp_source": timestamp_source if status == "AVAILABLE" else None,
        "stress_policy_sha256": (
            stress_policy_sha256 if provenance_class == "STRESS_APPROVED" else None
        ),
    }
    stable_source_sha256 = _sha256_json(canonical)
    partition_source_sha256 = _sha256_json(
        {
            "schema_version": TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
            "source_contract_sha256": stable_source_sha256,
            "projection_sha256": projection_sha256,
            "numerical_execution_cost_attestation_sha256": (cost_attestation_sha256),
        }
    )
    return (
        TradeEvidenceSourceContract(
            source_contract_sha256=stable_source_sha256,
            partition_source_contract_sha256=partition_source_sha256,
            target_bars_sha256=source_binding["target_bars_sha256"],
            target_sidecar_sha256=source_binding["target_sidecar_sha256"],
            financing_schedule_sha256=source_binding["schedule_sha256"],
            projection_contract_version=projection_contract,
            projection_sha256=projection_sha256,
            numerical_execution_cost_policy_sha256=cost_policy_sha256,
            numerical_execution_cost_attestation_sha256=(cost_attestation_sha256),
            swap_accrual_contract_version=swap_version,
            trade_record_schema_version=trade_schema,
            swap_status=status,
            swap_provenance_class=provenance_class,
            historical_financing_provenance=historical_financing_provenance,
            execution_state_compounding=True,
        ),
        "",
        (),
    )


_TRADE_SAMPLE_IDENTITY_KEYS = (
    "schema_version",
    "status",
    "outcome_kind",
    "reason",
    "reason_codes",
    "raw_trade_records",
    "usable_trades",
    "minimum_observations_required",
    "mean_net_r",
    "sigma_r_robust",
    "effective_trades",
    "net_r_min",
    "net_r_max",
    "net_r_values_sha256",
    "holding_intervals_sha256",
    "position_ids_sha256",
    "event_order_sha256",
    "event_start_utc",
    "event_end_utc",
    "source_contract",
    "net_r_contract",
    "dependence_evidence",
)


def trade_sample_identity_sha256(sample: Mapping[str, Any]) -> str:
    """Bind every decision-relevant persisted sample field."""
    if not isinstance(sample, Mapping):
        raise TypeError("trade sample identity requires a mapping")
    payload = {key: sample.get(key) for key in _TRADE_SAMPLE_IDENTITY_KEYS}
    if payload["schema_version"] != TRADE_SAMPLE_SCHEMA_VERSION:
        raise ValueError("trade sample schema is stale")
    return _sha256_json(payload)


def _unavailable_trade_sample(
    *,
    outcome_kind: SearchOutcomeKind,
    reason: str,
    reason_codes: Sequence[str],
    raw_trade_records: int,
    net_r_values_sha256: str = "",
    holding_intervals_sha256: str = "",
    position_ids_sha256: str = "",
    event_order_sha256: str = "",
    event_start_utc: str = "",
    event_end_utc: str = "",
    source_contract: Optional[TradeEvidenceSourceContract] = None,
    dependence_evidence: Optional[Mapping[str, Any]] = None,
    usable_trades: int = 0,
    minimum_observations_required: int = 8,
    mean_net_r: Optional[float] = None,
    sigma_r_robust: Optional[float] = None,
    net_r_min: Optional[float] = None,
    net_r_max: Optional[float] = None,
) -> TradeSampleEvidence:
    codes = tuple(dict.fromkeys(str(value) for value in reason_codes))
    status = (
        "EXPECTED_INELIGIBLE"
        if outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
        else "INFRA_FAILURE"
    )
    raw_count = max(0, int(raw_trade_records))
    usable_count = max(0, int(usable_trades))
    minimum_count = max(3, int(minimum_observations_required))
    dependence_payload = dict(dependence_evidence or {})
    identity_payload = {
        "schema_version": TRADE_SAMPLE_SCHEMA_VERSION,
        "status": status,
        "outcome_kind": outcome_kind.value,
        "reason": str(reason),
        "reason_codes": list(codes),
        "raw_trade_records": raw_count,
        "usable_trades": usable_count,
        "minimum_observations_required": minimum_count,
        "mean_net_r": mean_net_r,
        "sigma_r_robust": sigma_r_robust,
        "effective_trades": 0.0,
        "net_r_min": net_r_min,
        "net_r_max": net_r_max,
        "net_r_values_sha256": str(net_r_values_sha256),
        "holding_intervals_sha256": str(holding_intervals_sha256),
        "position_ids_sha256": str(position_ids_sha256),
        "event_order_sha256": str(event_order_sha256),
        "event_start_utc": str(event_start_utc),
        "event_end_utc": str(event_end_utc),
        "source_contract": (
            source_contract.to_dict() if source_contract is not None else None
        ),
        "net_r_contract": "strict_post_cost_pnl_dollars_divided_by_entry_risk_v1",
        "dependence_evidence": dependence_payload,
    }
    return TradeSampleEvidence(
        status=status,
        outcome_kind=outcome_kind,
        reason=str(reason),
        reason_codes=codes,
        raw_trade_records=raw_count,
        usable_trades=usable_count,
        minimum_observations_required=minimum_count,
        mean_net_r=mean_net_r,
        sigma_r_robust=sigma_r_robust,
        effective_trades=0.0,
        net_r_min=net_r_min,
        net_r_max=net_r_max,
        net_r_values_sha256=str(net_r_values_sha256),
        holding_intervals_sha256=str(holding_intervals_sha256),
        position_ids_sha256=str(position_ids_sha256),
        event_order_sha256=str(event_order_sha256),
        event_start_utc=str(event_start_utc),
        event_end_utc=str(event_end_utc),
        sample_sha256=trade_sample_identity_sha256(identity_payload),
        source_contract=source_contract,
        dependence_evidence=dependence_payload,
    )


def _finite_trade_number(value: Any, *, field_name: str) -> float:
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{field_name} must be a finite number, not boolean")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} is not numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} is not finite")
    return number


def _utc_interval_timestamp(value: Any, *, field_name: str) -> str:
    if value in (None, ""):
        raise ValueError(f"{field_name} is missing")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} is invalid") from exc
    if pd.isna(timestamp):
        raise ValueError(f"{field_name} is invalid")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


def _valid_utc_interval(start: Any, end: Any) -> bool:
    try:
        start_ts = pd.Timestamp(_utc_interval_timestamp(start, field_name="start"))
        end_ts = pd.Timestamp(_utc_interval_timestamp(end, field_name="end"))
    except ValueError:
        return False
    return start_ts < end_ts


def build_trade_power_multiplicity_receipt(
    attempt_ledger_payload: Any,
) -> dict[str, Any]:
    """Bind power multiplicity to the exact frozen attempt ledger."""

    if not isinstance(attempt_ledger_payload, Mapping):
        raise ValueError("attempt ledger must be a mapping")
    if str(attempt_ledger_payload.get("contract_version") or "") != (
        "fxpm.winner-attempt-ledger.v2"
    ):
        raise ValueError("attempt ledger contract is missing or stale")
    attempts = attempt_ledger_payload.get("attempts")
    count = attempt_ledger_payload.get("attempt_count")
    if type(attempts) is not list or type(count) is not int or count <= 0:
        raise ValueError("attempt ledger roster is invalid")
    if count != len(attempts):
        raise ValueError("attempt ledger count does not match roster")
    expected_attempts_sha = _sha256_json(attempts)
    if str(attempt_ledger_payload.get("attempts_sha256") or "").lower() != (
        expected_attempts_sha
    ):
        raise ValueError("attempt ledger roster hash does not verify")
    attempt_ids = [
        str(value.get("attempt_id") or "") if isinstance(value, Mapping) else ""
        for value in attempts
    ]
    if any(not value for value in attempt_ids) or len(set(attempt_ids)) != count:
        raise ValueError("attempt ledger identities are missing or duplicated")
    body = {
        "schema_version": TRADE_POWER_MULTIPLICITY_VERSION,
        "attempt_ledger_payload_sha256": _sha256_json(attempt_ledger_payload),
        "attempts_sha256": expected_attempts_sha,
        "attempt_ids_sha256": _sha256_json(attempt_ids),
        "attempted_trials_exact": count,
        "multiplicity_method": "raw_attempt_ledger_count_v1",
    }
    return {**body, "receipt_sha256": _sha256_json(body)}


def _validate_multiplicity_receipt(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    keys = {
        "schema_version",
        "attempt_ledger_payload_sha256",
        "attempts_sha256",
        "attempt_ids_sha256",
        "attempted_trials_exact",
        "multiplicity_method",
        "receipt_sha256",
    }
    if set(value) != keys:
        return None
    body = {key: value.get(key) for key in keys - {"receipt_sha256"}}
    if (
        body["schema_version"] != TRADE_POWER_MULTIPLICITY_VERSION
        or body["multiplicity_method"] != "raw_attempt_ledger_count_v1"
        or type(body["attempted_trials_exact"]) is not int
        or body["attempted_trials_exact"] <= 0
        or not all(
            _is_sha256(body[name])
            for name in (
                "attempt_ledger_payload_sha256",
                "attempts_sha256",
                "attempt_ids_sha256",
            )
        )
        or str(value.get("receipt_sha256") or "").lower() != _sha256_json(body)
    ):
        return None
    return {**body, "receipt_sha256": _sha256_json(body)}


def build_trade_power_partition_contract(
    *,
    partition: str,
    start_utc: Any,
    end_utc: Any,
    horizon_receipt_sha256: str,
    event_axis_sha256: str,
    multiplicity_receipt: Any,
    prior_partition_contract_sha256: str = "",
) -> dict[str, Any]:
    """Build one exact half-open partition and lineage receipt."""

    role = str(partition or "").strip().lower()
    if role not in _PRODUCTION_PARTITIONS:
        raise ValueError("partition is invalid")
    start = _utc_interval_timestamp(start_utc, field_name="start_utc")
    end = _utc_interval_timestamp(end_utc, field_name="end_utc")
    if pd.Timestamp(start) >= pd.Timestamp(end):
        raise ValueError("partition interval must be nonempty and half-open")
    receipt = _validate_multiplicity_receipt(multiplicity_receipt)
    if receipt is None:
        raise ValueError("multiplicity receipt is invalid")
    horizon = str(horizon_receipt_sha256 or "").lower()
    event_axis = str(event_axis_sha256 or "").lower()
    prior = str(prior_partition_contract_sha256 or "").lower()
    if not _is_sha256(horizon) or not _is_sha256(event_axis):
        raise ValueError("partition evidence hashes are malformed")
    if prior and not _is_sha256(prior):
        raise ValueError("prior partition hash is malformed")
    if role == "optimizer_train" and prior:
        raise ValueError("optimizer train cannot have a prior partition")
    if role != "optimizer_train" and not prior:
        raise ValueError("validation partition requires prior lineage")
    body = {
        "schema_version": TRADE_POWER_PARTITION_VERSION,
        "partition": role,
        "start_utc": start,
        "end_utc": end,
        "horizon_receipt_sha256": horizon,
        "event_axis_sha256": event_axis,
        "attempt_ledger_payload_sha256": receipt["attempt_ledger_payload_sha256"],
        "attempts_sha256": receipt["attempts_sha256"],
        "attempted_trials_exact": receipt["attempted_trials_exact"],
        "multiplicity_receipt_sha256": receipt["receipt_sha256"],
        "prior_partition_contract_sha256": prior,
    }
    return {**body, "contract_sha256": _sha256_json(body)}


def _validate_partition_contract(
    value: Any,
    *,
    expected_partition: str,
) -> Optional[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    keys = {
        "schema_version",
        "partition",
        "start_utc",
        "end_utc",
        "horizon_receipt_sha256",
        "event_axis_sha256",
        "attempt_ledger_payload_sha256",
        "attempts_sha256",
        "attempted_trials_exact",
        "multiplicity_receipt_sha256",
        "prior_partition_contract_sha256",
        "contract_sha256",
    }
    if set(value) != keys:
        return None
    body = {key: value.get(key) for key in keys - {"contract_sha256"}}
    try:
        start = _utc_interval_timestamp(body["start_utc"], field_name="start_utc")
        end = _utc_interval_timestamp(body["end_utc"], field_name="end_utc")
    except ValueError:
        return None
    role = str(body["partition"] or "").lower()
    prior = str(body["prior_partition_contract_sha256"] or "").lower()
    if (
        body["schema_version"] != TRADE_POWER_PARTITION_VERSION
        or role != expected_partition
        or pd.Timestamp(start) >= pd.Timestamp(end)
        or type(body["attempted_trials_exact"]) is not int
        or body["attempted_trials_exact"] <= 0
        or any(
            not _is_sha256(body[name])
            for name in (
                "horizon_receipt_sha256",
                "event_axis_sha256",
                "attempt_ledger_payload_sha256",
                "attempts_sha256",
                "multiplicity_receipt_sha256",
            )
        )
        or (role == "optimizer_train" and prior)
        or (role != "optimizer_train" and not _is_sha256(prior))
    ):
        return None
    body["start_utc"] = start
    body["end_utc"] = end
    body["partition"] = role
    body["prior_partition_contract_sha256"] = prior
    if str(value.get("contract_sha256") or "").lower() != _sha256_json(body):
        return None
    return {**body, "contract_sha256": _sha256_json(body)}


def build_trade_sample_evidence(
    trades: Any,
    *,
    source_contract: Any = None,
    minimum_observations: int = 8,
) -> TradeSampleEvidence:
    """Build strict post-cost net-R evidence without dropping corrupt records.

    A non-empty record set is all-or-nothing.  Silently omitting one malformed
    trade would change both the estimated edge and its effective sample size,
    so any such record is an infrastructure failure.
    """

    try:
        minimum = max(3, int(minimum_observations))
    except (TypeError, ValueError, OverflowError):
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            reason="minimum observations contract is invalid",
            reason_codes=("MINIMUM_OBSERVATIONS_INVALID",),
            raw_trade_records=0,
        )
    source, source_reason, source_codes = _validate_trade_evidence_source_contract(
        source_contract
    )
    if source is None:
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            reason=source_reason,
            reason_codes=source_codes,
            raw_trade_records=(len(trades) if isinstance(trades, (list, tuple)) else 0),
            minimum_observations_required=minimum,
        )
    if trades is None:
        rows: Sequence[Any] = ()
    elif isinstance(trades, (list, tuple)):
        rows = trades
    else:
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            reason="trade records must be a list or tuple",
            reason_codes=("TRADE_RECORDS_INVALID",),
            raw_trade_records=0,
            minimum_observations_required=minimum,
            source_contract=source,
        )
    raw_count = len(rows)
    if raw_count == 0:
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
            reason="strategy produced no trades",
            reason_codes=("NO_TRADES",),
            raw_trade_records=0,
            minimum_observations_required=minimum,
            source_contract=source,
        )

    net_r_values: list[float] = []
    intervals: list[dict[str, Any]] = []
    position_ids: list[str] = []
    event_order: list[tuple[str, int, str, int, str]] = []
    prior_order_key: Optional[tuple[pd.Timestamp, int, pd.Timestamp, int, str]] = None
    for index, trade in enumerate(rows):
        if not isinstance(trade, Mapping):
            return _unavailable_trade_sample(
                outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
                reason=f"trade record {index} is not a mapping",
                reason_codes=("TRADE_RECORD_INVALID",),
                raw_trade_records=raw_count,
                minimum_observations_required=minimum,
                source_contract=source,
            )
        if str(trade.get("trade_record_schema_version") or "") != (
            REQUIRED_TRADE_RECORD_SCHEMA
        ):
            return _unavailable_trade_sample(
                outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
                reason=f"trade record {index} has a stale or missing schema",
                reason_codes=("TRADE_RECORD_SCHEMA_MISMATCH",),
                raw_trade_records=raw_count,
                minimum_observations_required=minimum,
                source_contract=source,
            )
        try:
            pnl = _finite_trade_number(
                trade.get("pnl_dollars"), field_name="pnl_dollars"
            )
            risk = _finite_trade_number(
                trade.get("risk_amount"), field_name="risk_amount"
            )
            if risk <= 0.0:
                raise ValueError("risk_amount must be positive")
            net_r = pnl / risk
            if not math.isfinite(net_r):
                raise ValueError("derived net R is not finite")
            entry = _utc_interval_timestamp(
                trade.get("entry_time"), field_name="entry_time"
            )
            exit_ = _utc_interval_timestamp(
                trade.get("exit_time"), field_name="exit_time"
            )
            position_id = str(trade.get("position_id") or "").strip()
            entry_order = trade.get("entry_event_order")
            exit_order = trade.get("exit_event_order")
            if not position_id or len(position_id) > 256:
                raise ValueError("position_id is missing or oversized")
            if type(entry_order) is not int or entry_order < 0:
                raise ValueError("entry_event_order is invalid")
            if type(exit_order) is not int or exit_order < 0:
                raise ValueError("exit_event_order is invalid")
            entry_key = (pd.Timestamp(entry), entry_order)
            exit_key = (pd.Timestamp(exit_), exit_order)
            if exit_key <= entry_key:
                raise ValueError("exit event does not follow entry event")
            order_key = (
                exit_key[0],
                exit_key[1],
                entry_key[0],
                entry_key[1],
                position_id,
            )
            if prior_order_key is not None and order_key <= prior_order_key:
                raise ValueError("trade records are not in canonical event order")
            prior_order_key = order_key
        except (TypeError, ValueError, OverflowError) as exc:
            return _unavailable_trade_sample(
                outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
                reason=f"trade record {index} violates net-R contract: {exc}",
                reason_codes=("MALFORMED_POSITIVE_TRADE_EVIDENCE",),
                raw_trade_records=raw_count,
                minimum_observations_required=minimum,
                source_contract=source,
            )
        net_r_values.append(float(net_r))
        position_ids.append(position_id)
        event_order.append((entry, entry_order, exit_, exit_order, position_id))
        intervals.append(
            {
                "start": entry,
                "end": exit_,
                "start_event_order": entry_order,
                "end_event_order": exit_order,
            }
        )

    if len(set(position_ids)) != raw_count:
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            reason="position identities are duplicated or split across records",
            reason_codes=("POSITION_ID_DUPLICATED",),
            raw_trade_records=raw_count,
            minimum_observations_required=minimum,
            source_contract=source,
        )

    value_hash = _sha256_json([float(value).hex() for value in net_r_values])
    interval_hash = _sha256_json(intervals)
    position_hash = _sha256_json(position_ids)
    event_order_hash = _sha256_json(event_order)
    event_start = min(value[0] for value in event_order)
    event_end = max(value[2] for value in event_order)
    vector = np.asarray(net_r_values, dtype=np.float64)
    mean_net_r = float(vector.mean())
    minimum_net_r = float(vector.min())
    maximum_net_r = float(vector.max())
    sigma = robust_r_dispersion(net_r_values)
    dependence = effective_sample_evidence(
        net_r_values,
        holding_intervals=intervals,
        minimum_observations=minimum,
    )
    dependence_payload = dependence.to_dict()
    if not dependence.passed:
        codes = tuple(dependence.reason_codes)
        expected = bool(codes) and set(codes).issubset(
            _EXPECTED_SAMPLE_UNAVAILABLE_CODES
        )
        return _unavailable_trade_sample(
            outcome_kind=(
                SearchOutcomeKind.EXPECTED_INELIGIBLE
                if expected
                else SearchOutcomeKind.INFRA_FAILURE
            ),
            reason=dependence.reason,
            reason_codes=codes or ("TRADE_DEPENDENCE_EVIDENCE_UNAVAILABLE",),
            raw_trade_records=raw_count,
            usable_trades=raw_count,
            minimum_observations_required=minimum,
            mean_net_r=mean_net_r,
            sigma_r_robust=sigma,
            net_r_min=minimum_net_r,
            net_r_max=maximum_net_r,
            net_r_values_sha256=value_hash,
            holding_intervals_sha256=interval_hash,
            position_ids_sha256=position_hash,
            event_order_sha256=event_order_hash,
            event_start_utc=event_start,
            event_end_utc=event_end,
            source_contract=source,
            dependence_evidence=dependence_payload,
        )
    if sigma is None:
        return _unavailable_trade_sample(
            outcome_kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
            reason="robust net-R dispersion is unavailable",
            reason_codes=("PILOT_SIGMA_R_UNAVAILABLE",),
            raw_trade_records=raw_count,
            usable_trades=raw_count,
            minimum_observations_required=minimum,
            mean_net_r=mean_net_r,
            net_r_min=minimum_net_r,
            net_r_max=maximum_net_r,
            net_r_values_sha256=value_hash,
            holding_intervals_sha256=interval_hash,
            position_ids_sha256=position_hash,
            event_order_sha256=event_order_hash,
            event_start_utc=event_start,
            event_end_utc=event_end,
            source_contract=source,
            dependence_evidence=dependence_payload,
        )
    identity_payload = {
        "schema_version": TRADE_SAMPLE_SCHEMA_VERSION,
        "status": "MEASURED",
        "outcome_kind": SearchOutcomeKind.VALID.value,
        "reason": "strict post-cost net-R effective sample measured",
        "reason_codes": [],
        "raw_trade_records": raw_count,
        "usable_trades": raw_count,
        "minimum_observations_required": minimum,
        "mean_net_r": mean_net_r,
        "sigma_r_robust": float(sigma),
        "effective_trades": float(dependence.effective_observations),
        "net_r_min": minimum_net_r,
        "net_r_max": maximum_net_r,
        "net_r_values_sha256": value_hash,
        "holding_intervals_sha256": interval_hash,
        "position_ids_sha256": position_hash,
        "event_order_sha256": event_order_hash,
        "event_start_utc": event_start,
        "event_end_utc": event_end,
        "source_contract": source.to_dict(),
        "net_r_contract": "strict_post_cost_pnl_dollars_divided_by_entry_risk_v1",
        "dependence_evidence": dependence_payload,
    }
    return TradeSampleEvidence(
        status="MEASURED",
        outcome_kind=SearchOutcomeKind.VALID,
        reason="strict post-cost net-R effective sample measured",
        reason_codes=(),
        raw_trade_records=raw_count,
        usable_trades=raw_count,
        minimum_observations_required=minimum,
        mean_net_r=mean_net_r,
        sigma_r_robust=float(sigma),
        effective_trades=float(dependence.effective_observations),
        net_r_min=minimum_net_r,
        net_r_max=maximum_net_r,
        net_r_values_sha256=value_hash,
        holding_intervals_sha256=interval_hash,
        position_ids_sha256=position_hash,
        event_order_sha256=event_order_hash,
        event_start_utc=event_start,
        event_end_utc=event_end,
        sample_sha256=trade_sample_identity_sha256(identity_payload),
        source_contract=source,
        dependence_evidence=dependence_payload,
    )


def _required_for_partition(
    requirement: TradePowerRequirement,
    partition: str,
) -> Optional[int]:
    if partition == "optimizer_train":
        return requirement.required_effective_train_trades
    if partition == "tuning_validation":
        return requirement.required_effective_tuning_trades
    if partition == "final_lockbox":
        return requirement.required_effective_final_trades
    return None


def requirement_source_provenance_valid(
    requirement: TradePowerRequirement,
) -> bool:
    admissible = {
        ("AUTHENTICATED", "AUTHENTICATED_HISTORICAL_RATES"),
        ("STRESS_APPROVED", "IMMUTABLE_STRESS_APPROVED"),
        (
            "AUTHENTICATED_BROKER_DISABLED",
            "AUTHENTICATED_BROKER_DISABLED",
        ),
    }
    return (
        requirement.swap_provenance_class,
        requirement.historical_financing_provenance,
    ) in admissible


def _sample_terminal_decision(
    sample: TradeSampleEvidence,
    *,
    partition: str,
    requirement: Optional[TradePowerRequirement],
    partition_contract: Optional[Mapping[str, Any]] = None,
) -> TradePowerDecision:
    return TradePowerDecision(
        status=(
            "INFRA_FAILURE"
            if sample.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
            else "EXPECTED_INELIGIBLE"
        ),
        outcome_kind=sample.outcome_kind,
        partition=partition,
        reason=sample.reason,
        reason_codes=sample.reason_codes,
        observed_effective_trades=float(sample.effective_trades),
        required_effective_trades=(
            _required_for_partition(requirement, partition)
            if requirement is not None
            else None
        ),
        sample_evidence=sample,
        frozen_requirement=requirement,
        partition_contract=partition_contract,
    )


def calibrate_train_trade_power(
    trades: Any,
    *,
    policy: TradePowerPolicy,
    multiplicity_receipt: Any,
    partition_contract: Any,
    source_contract: Any = None,
    minimum_observations: int = 8,
) -> TradePowerDecision:
    """Freeze train-only dispersion and multiplicity, then judge train ESS."""

    receipt = _validate_multiplicity_receipt(multiplicity_receipt)
    partition_receipt = _validate_partition_contract(
        partition_contract,
        expected_partition="optimizer_train",
    )
    if (
        receipt is None
        or partition_receipt is None
        or partition_receipt["attempt_ledger_payload_sha256"]
        != receipt["attempt_ledger_payload_sha256"]
        or partition_receipt["attempts_sha256"] != receipt["attempts_sha256"]
        or partition_receipt["attempted_trials_exact"]
        != receipt["attempted_trials_exact"]
        or partition_receipt["multiplicity_receipt_sha256"] != receipt["receipt_sha256"]
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition="optimizer_train",
            reason="attempt multiplicity or train partition receipt is invalid",
            reason_codes=("TRAIN_POWER_LINEAGE_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=None,
            sample_evidence=None,
            frozen_requirement=None,
            partition_contract=partition_receipt,
        )
    sample = build_trade_sample_evidence(
        trades,
        source_contract=source_contract,
        minimum_observations=minimum_observations,
    )
    if not sample.passed:
        return _sample_terminal_decision(
            sample,
            partition="optimizer_train",
            requirement=None,
            partition_contract=partition_receipt,
        )
    if pd.Timestamp(sample.event_start_utc) < pd.Timestamp(
        partition_receipt["start_utc"]
    ) or pd.Timestamp(sample.event_end_utc) >= pd.Timestamp(
        partition_receipt["end_utc"]
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition="optimizer_train",
            reason="train trade events fall outside the bound partition",
            reason_codes=("TRADE_EVENTS_OUTSIDE_PARTITION",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=None,
            sample_evidence=sample,
            frozen_requirement=None,
            partition_contract=partition_receipt,
        )
    requirement = derive_trade_power_requirement(
        sigma_r_robust=sample.sigma_r_robust,
        effective_trials=receipt["attempted_trials_exact"],
        policy=policy,
    )
    if not requirement.passed:
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition="optimizer_train",
            reason=requirement.reason,
            reason_codes=requirement.reason_codes,
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=requirement.required_effective_train_trades,
            sample_evidence=sample,
            frozen_requirement=requirement,
            partition_contract=partition_receipt,
        )
    requirement = replace(
        requirement,
        multiplicity_method=receipt["multiplicity_method"],
        calibration_partition="optimizer_train",
        calibration_sample_sha256=sample.sample_sha256,
        calibration_source_contract_sha256=(
            sample.source_contract.source_contract_sha256
            if sample.source_contract is not None
            else None
        ),
        calibration_partition_source_contract_sha256=(
            sample.source_contract.partition_source_contract_sha256
            if sample.source_contract is not None
            else None
        ),
        source_contract_schema_version=(
            sample.source_contract.schema_version
            if sample.source_contract is not None
            else ""
        ),
        target_bars_sha256=(
            sample.source_contract.target_bars_sha256
            if sample.source_contract is not None
            else ""
        ),
        target_sidecar_sha256=(
            sample.source_contract.target_sidecar_sha256
            if sample.source_contract is not None
            else ""
        ),
        financing_schedule_sha256=(
            sample.source_contract.financing_schedule_sha256
            if sample.source_contract is not None
            else ""
        ),
        projection_contract_version=(
            sample.source_contract.projection_contract_version
            if sample.source_contract is not None
            else ""
        ),
        numerical_execution_cost_policy_sha256=(
            sample.source_contract.numerical_execution_cost_policy_sha256
            if sample.source_contract is not None
            else ""
        ),
        calibration_numerical_execution_cost_attestation_sha256=(
            sample.source_contract.numerical_execution_cost_attestation_sha256
            if sample.source_contract is not None
            else ""
        ),
        swap_accrual_contract_version=(
            sample.source_contract.swap_accrual_contract_version
            if sample.source_contract is not None
            else ""
        ),
        trade_record_schema_version=(
            sample.source_contract.trade_record_schema_version
            if sample.source_contract is not None
            else ""
        ),
        swap_provenance_class=(
            sample.source_contract.swap_provenance_class
            if sample.source_contract is not None
            else ""
        ),
        historical_financing_provenance=(
            sample.source_contract.historical_financing_provenance
            if sample.source_contract is not None
            else ""
        ),
        attempt_ledger_payload_sha256=receipt["attempt_ledger_payload_sha256"],
        attempts_sha256=receipt["attempts_sha256"],
        attempted_trials_exact=receipt["attempted_trials_exact"],
        multiplicity_receipt_sha256=receipt["receipt_sha256"],
        calibration_partition_contract_sha256=partition_receipt["contract_sha256"],
        calibration_start_utc=partition_receipt["start_utc"],
        calibration_end_utc=partition_receipt["end_utc"],
        calibration_horizon_receipt_sha256=partition_receipt["horizon_receipt_sha256"],
        calibration_event_axis_sha256=partition_receipt["event_axis_sha256"],
        calibration_net_r_values_sha256=sample.net_r_values_sha256,
        calibration_holding_intervals_sha256=(sample.holding_intervals_sha256),
        frozen=True,
    )
    required = requirement.required_effective_train_trades
    if required is None or sample.effective_trades < float(required):
        return TradePowerDecision(
            status="EXPECTED_INELIGIBLE",
            outcome_kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
            partition="optimizer_train",
            reason=(
                f"effective train trades {sample.effective_trades:.6g} < {required}"
            ),
            reason_codes=("TRAIN_EFFECTIVE_TRADES_UNDERPOWERED",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=required,
            sample_evidence=sample,
            frozen_requirement=requirement,
            partition_contract=partition_receipt,
        )
    return TradePowerDecision(
        status="PASS",
        outcome_kind=SearchOutcomeKind.VALID,
        partition="optimizer_train",
        reason="train effective trades meet the frozen power requirement",
        reason_codes=(),
        observed_effective_trades=float(sample.effective_trades),
        required_effective_trades=required,
        sample_evidence=sample,
        frozen_requirement=requirement,
        partition_contract=partition_receipt,
    )


def evaluate_trade_power_partition(
    trades: Any,
    *,
    frozen_requirement: TradePowerRequirement,
    partition: str,
    partition_contract: Any,
    prior_partition_contract: Any,
    source_contract: Any = None,
    minimum_observations: int = 8,
) -> TradePowerDecision:
    """Judge tuning/final ESS without recalibrating train-frozen thresholds."""

    partition_name = str(partition or "").strip().lower()
    if partition_name not in _PRODUCTION_PARTITIONS - {"optimizer_train"}:
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="partition must be tuning_validation or final_lockbox",
            reason_codes=("PARTITION_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=None,
            sample_evidence=None,
            frozen_requirement=(
                frozen_requirement
                if isinstance(frozen_requirement, TradePowerRequirement)
                else None
            ),
        )
    current_receipt = _validate_partition_contract(
        partition_contract,
        expected_partition=partition_name,
    )
    prior_role = (
        "optimizer_train"
        if partition_name == "tuning_validation"
        else "tuning_validation"
    )
    prior_receipt = _validate_partition_contract(
        prior_partition_contract,
        expected_partition=prior_role,
    )
    if current_receipt is None or prior_receipt is None:
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="partition lineage contract is invalid",
            reason_codes=("PARTITION_LINEAGE_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=None,
            sample_evidence=None,
            frozen_requirement=(
                frozen_requirement
                if isinstance(frozen_requirement, TradePowerRequirement)
                else None
            ),
            partition_contract=current_receipt,
        )
    if (
        not isinstance(frozen_requirement, TradePowerRequirement)
        or not frozen_requirement.passed
        or not frozen_requirement.frozen
        or frozen_requirement.calibration_partition != "optimizer_train"
        or not frozen_requirement.calibration_sample_sha256
        or not frozen_requirement.calibration_source_contract_sha256
        or not _is_sha256(frozen_requirement.calibration_sample_sha256)
        or not _is_sha256(frozen_requirement.calibration_source_contract_sha256)
        or not _is_sha256(
            frozen_requirement.calibration_partition_source_contract_sha256
        )
        or frozen_requirement.source_contract_schema_version
        != TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION
        or not _is_sha256(frozen_requirement.target_bars_sha256)
        or not _is_sha256(frozen_requirement.target_sidecar_sha256)
        or not _is_sha256(frozen_requirement.financing_schedule_sha256)
        or frozen_requirement.projection_contract_version
        != REQUIRED_SWAP_PROJECTION_CONTRACT
        or not _is_sha256(frozen_requirement.numerical_execution_cost_policy_sha256)
        or not _is_sha256(
            frozen_requirement.calibration_numerical_execution_cost_attestation_sha256
        )
        or frozen_requirement.swap_accrual_contract_version
        != REQUIRED_SWAP_ACCRUAL_CONTRACT
        or frozen_requirement.trade_record_schema_version
        != REQUIRED_TRADE_RECORD_SCHEMA
        or frozen_requirement.policy_schema_version != TRADE_POWER_SCHEMA_VERSION
        or frozen_requirement.pilot_dispersion_method != "max_mad_sample_std_r"
        or not _is_sha256(frozen_requirement.attempt_ledger_payload_sha256)
        or not _is_sha256(frozen_requirement.attempts_sha256)
        or type(frozen_requirement.attempted_trials_exact) is not int
        or frozen_requirement.attempted_trials_exact <= 0
        or frozen_requirement.effective_trials
        != float(frozen_requirement.attempted_trials_exact)
        or not _is_sha256(frozen_requirement.multiplicity_receipt_sha256)
        or not _is_sha256(frozen_requirement.calibration_partition_contract_sha256)
        or not _is_sha256(frozen_requirement.calibration_horizon_receipt_sha256)
        or not _is_sha256(frozen_requirement.calibration_event_axis_sha256)
        or not _is_sha256(frozen_requirement.calibration_net_r_values_sha256)
        or not _is_sha256(frozen_requirement.calibration_holding_intervals_sha256)
        or not _valid_utc_interval(
            frozen_requirement.calibration_start_utc,
            frozen_requirement.calibration_end_utc,
        )
        or not requirement_source_provenance_valid(frozen_requirement)
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="trade-power requirement is not an authenticated train-frozen artifact",
            reason_codes=("FROZEN_REQUIREMENT_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=None,
            sample_evidence=None,
            frozen_requirement=(
                frozen_requirement
                if isinstance(frozen_requirement, TradePowerRequirement)
                else None
            ),
            partition_contract=current_receipt,
        )
    lineage_common = (
        current_receipt["attempt_ledger_payload_sha256"]
        == frozen_requirement.attempt_ledger_payload_sha256
        and current_receipt["attempts_sha256"] == frozen_requirement.attempts_sha256
        and current_receipt["attempted_trials_exact"]
        == frozen_requirement.attempted_trials_exact
        and current_receipt["multiplicity_receipt_sha256"]
        == frozen_requirement.multiplicity_receipt_sha256
        and prior_receipt["attempt_ledger_payload_sha256"]
        == frozen_requirement.attempt_ledger_payload_sha256
        and prior_receipt["attempts_sha256"] == frozen_requirement.attempts_sha256
        and prior_receipt["attempted_trials_exact"]
        == frozen_requirement.attempted_trials_exact
        and prior_receipt["multiplicity_receipt_sha256"]
        == frozen_requirement.multiplicity_receipt_sha256
        and (
            partition_name != "tuning_validation"
            or (
                prior_receipt["start_utc"] == frozen_requirement.calibration_start_utc
                and prior_receipt["end_utc"] == frozen_requirement.calibration_end_utc
                and prior_receipt["horizon_receipt_sha256"]
                == frozen_requirement.calibration_horizon_receipt_sha256
                and prior_receipt["event_axis_sha256"]
                == frozen_requirement.calibration_event_axis_sha256
            )
        )
    )
    prior_chain_valid = (
        prior_receipt["contract_sha256"]
        == frozen_requirement.calibration_partition_contract_sha256
        if partition_name == "tuning_validation"
        else prior_receipt["prior_partition_contract_sha256"]
        == frozen_requirement.calibration_partition_contract_sha256
    )
    if (
        not lineage_common
        or not prior_chain_valid
        or current_receipt["prior_partition_contract_sha256"]
        != prior_receipt["contract_sha256"]
        or pd.Timestamp(current_receipt["start_utc"])
        < pd.Timestamp(prior_receipt["end_utc"])
        or (
            partition_name == "final_lockbox"
            and pd.Timestamp(prior_receipt["start_utc"])
            < pd.Timestamp(frozen_requirement.calibration_end_utc)
        )
        or (
            partition_name == "final_lockbox"
            and prior_receipt["event_axis_sha256"]
            == frozen_requirement.calibration_event_axis_sha256
        )
        or current_receipt["event_axis_sha256"]
        == frozen_requirement.calibration_event_axis_sha256
        or current_receipt["event_axis_sha256"] == prior_receipt["event_axis_sha256"]
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="partition lineage is overlapping, replayed, or ledger-unbound",
            reason_codes=("PARTITION_LINEAGE_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=None,
            sample_evidence=None,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    required = _required_for_partition(frozen_requirement, partition_name)
    if required is None or int(required) <= 0:
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="frozen partition requirement is missing or nonpositive",
            reason_codes=("FROZEN_PARTITION_REQUIREMENT_INVALID",),
            observed_effective_trades=0.0,
            required_effective_trades=required,
            sample_evidence=None,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    sample = build_trade_sample_evidence(
        trades,
        source_contract=source_contract,
        minimum_observations=minimum_observations,
    )
    if sample.source_contract is None:
        return _sample_terminal_decision(
            sample,
            partition=partition_name,
            requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    if (
        sample.source_contract.source_contract_sha256
        != frozen_requirement.calibration_source_contract_sha256
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="partition source contract differs from train calibration",
            reason_codes=("SOURCE_CONTRACT_DRIFT",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=required,
            sample_evidence=sample,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    if not sample.passed:
        return _sample_terminal_decision(
            sample,
            partition=partition_name,
            requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    if pd.Timestamp(sample.event_start_utc) < pd.Timestamp(
        current_receipt["start_utc"]
    ) or pd.Timestamp(sample.event_end_utc) >= pd.Timestamp(current_receipt["end_utc"]):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="trade events fall outside the bound partition",
            reason_codes=("TRADE_EVENTS_OUTSIDE_PARTITION",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=required,
            sample_evidence=sample,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    if (
        sample.sample_sha256 == frozen_requirement.calibration_sample_sha256
        or sample.source_contract.partition_source_contract_sha256
        == frozen_requirement.calibration_partition_source_contract_sha256
        or sample.net_r_values_sha256
        == frozen_requirement.calibration_net_r_values_sha256
        and sample.holding_intervals_sha256
        == frozen_requirement.calibration_holding_intervals_sha256
    ):
        return TradePowerDecision(
            status="INFRA_FAILURE",
            outcome_kind=SearchOutcomeKind.INFRA_FAILURE,
            partition=partition_name,
            reason="validation evidence replays optimizer-train observations",
            reason_codes=("TRAIN_SAMPLE_REPLAYED",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=required,
            sample_evidence=sample,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    if sample.effective_trades < float(required):
        return TradePowerDecision(
            status="EXPECTED_INELIGIBLE",
            outcome_kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
            partition=partition_name,
            reason=(
                f"effective {partition_name} trades "
                f"{sample.effective_trades:.6g} < {required}"
            ),
            reason_codes=("PARTITION_EFFECTIVE_TRADES_UNDERPOWERED",),
            observed_effective_trades=float(sample.effective_trades),
            required_effective_trades=required,
            sample_evidence=sample,
            frozen_requirement=frozen_requirement,
            partition_contract=current_receipt,
        )
    return TradePowerDecision(
        status="PASS",
        outcome_kind=SearchOutcomeKind.VALID,
        partition=partition_name,
        reason="partition effective trades meet the train-frozen requirement",
        reason_codes=(),
        observed_effective_trades=float(sample.effective_trades),
        required_effective_trades=required,
        sample_evidence=sample,
        frozen_requirement=frozen_requirement,
        partition_contract=current_receipt,
    )


__all__ = [
    "NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION",
    "REQUIRED_SWAP_ACCRUAL_CONTRACT",
    "REQUIRED_SWAP_PROJECTION_CONTRACT",
    "REQUIRED_TRADE_RECORD_SCHEMA",
    "TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION",
    "TRADE_POWER_SCHEMA_VERSION",
    "TRADE_POWER_DECISION_SCHEMA_VERSION",
    "TRADE_POWER_MULTIPLICITY_VERSION",
    "TRADE_POWER_PARTITION_VERSION",
    "TRADE_SAMPLE_SCHEMA_VERSION",
    "TradePowerDecision",
    "TradeEvidenceSourceContract",
    "TradePowerPolicy",
    "TradePowerRequirement",
    "TradeSampleEvidence",
    "adjusted_alpha",
    "build_numerical_execution_cost_attestation",
    "build_trade_power_multiplicity_receipt",
    "build_trade_power_partition_contract",
    "build_trade_sample_evidence",
    "calibrate_train_trade_power",
    "derive_trade_power_requirement",
    "evaluate_trade_power_partition",
    "robust_r_dispersion",
    "requirement_source_provenance_valid",
    "trade_sample_identity_sha256",
    "trade_power_decision_sha256",
    "trade_power_requirement_sha256",
]
