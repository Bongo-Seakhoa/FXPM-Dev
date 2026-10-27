"""Canonical specialist winner selection on one precommitted economic utility.

This module is deliberately standalone.  It does not optimize parameters, size
capital, mutate a winner book, or authorize live trading.  Its single job is to
turn a precommitted, complete within-cell search and exact causal-fold evidence
into a deterministic process-level winner (or an explicit no-winner result).

The result is an offline book-selection calculation, not a live-trading
authority.  The evaluator consumes one canonical request containing an exact
precommitment, immutable all-attempt ledger, and exact post-cost evidence
ledger.  Content hashes bind those ledgers to one another and make the request
replayable by the production pipeline without private keys, signer services, or
test-only trust objects.  Book multiplicity, historical process validation,
current refit, promotion, and capital controls remain separate downstream
gates.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import hashlib
from itertools import combinations
import json
import math
import re
from statistics import NormalDist
import struct
from types import MappingProxyType
from typing import Any, Final, Mapping

from pm_core import (
    build_backtest_validated_cost_ladder_tolerance,
    build_backtest_winner_risk_unit_contract,
)
from pm_sample_dependence import effective_sample_evidence
from pm_trade_power import (
    TradePowerPolicy,
    derive_trade_power_requirement,
    robust_r_dispersion,
)


WINNER_SELECTION_CONTRACT_VERSION: Final = "fxpm.winner-selection.v14"
WINNER_SELECTION_SCOPE: Final = "OFFLINE_BOOK_SELECTION"
WINNER_SELECTION_REQUEST_VERSION: Final = "fxpm.winner-selection-request.v2"
WINNER_BOOK_MARGINAL_CONTRACT_VERSION: Final = "fxpm.winner-book-marginal.v1"
PRECOMMITMENT_VERSION: Final = "fxpm.winner-precommitment.v3"
ATTEMPT_LEDGER_VERSION: Final = "fxpm.winner-attempt-ledger.v3"
EVIDENCE_LEDGER_VERSION: Final = "fxpm.winner-evidence-ledger.v6"
SHARED_OOS_PARTITION_VERSION: Final = "fxpm.winner-shared-oos-partition.v2"
TRADE_POWER_BINDING_VERSION: Final = "fxpm.winner-trade-power-binding.v2"

TIMEFRAMES: Final = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES: Final = ("TREND", "RANGE", "BREAKOUT", "CHOP")

MAX_REQUEST_BYTES: Final = 16 * 1024 * 1024
MAX_DECISION_BYTES: Final = 20 * 1024 * 1024
MAX_JSON_NODES: Final = 1_000_000
MAX_SUPPORTED_WINNER_REQUEST_BYTES: Final = 64 * 1024 * 1024
MAX_JSON_DEPTH: Final = 28
MAX_CONTAINER_ITEMS: Final = 4_096
MAX_STRING_CHARS: Final = 2 * 1024 * 1024
# Payload bound, not a statistical one - it sits with the JSON/container limits
# above for a reason. Statistical control over multiplicity lives in
# pm_statistics, which deflates by effective_independent_trials and therefore
# tightens automatically as the attempt count rises.
#
# It was 128, which forced the planner to spend 100 attempts on registry
# defaults and share 28 tuned points across the whole strategy pool. That is a
# ceiling on how much edge the search can find, imposed by a serialization
# limit. 300 carries 100 defaults plus 2 tuned points per strategy; a 300-attempt
# plan canonicalizes well under MAX_REQUEST_BYTES and MAX_CONTAINER_ITEMS.
MAX_ATTEMPTS: Final = 300
MAX_FOLDS: Final = 12
MAX_DAYS_PER_FOLD: Final = 1_024
# A fold's parallel trade identity/time/order vectors remain ordinary JSON
# lists, so their reachable canonical width is exactly the parser ceiling.
MAX_TRADES_PER_FOLD: Final = MAX_CONTAINER_ITEMS
MAX_TOTAL_DAILY_POINTS: Final = 262_144
MAX_TOTAL_COMPLETED_TRADES: Final = 65_536
MAX_DEPENDENCE_BLOCK_OPERATIONS: Final = 50_000_000
# Three ladder lanes, two block scales, 1,024 repetitions, at most 76
# resampled blocks, and the full 300-attempt frozen roster.  This is a work
# bound, not an evidence/admission threshold.
MAX_BOOTSTRAP_BLOCK_DRAWS: Final = 140_083_200
MAX_ABS_DAILY_R: Final = 50.0
MAX_ABS_TRADE_R: Final = 100.0
PRIMARY_CELL_RISK_FRACTION: Final = 0.0025
PRIMARY_LCB_CONFIDENCE: Final = 0.95
_PRIMARY_LCB_NORMAL_CRITICAL: Final = NormalDist().inv_cdf(PRIMARY_LCB_CONFIDENCE)
_LANE_FIELDS: Final = MappingProxyType(
    {
        "BASE": "daily_net_mtm_r",
        "P75": "daily_net_mtm_r_p75",
        "P95": "daily_net_mtm_r_p95",
    }
)
_TRADE_LANE_FIELDS: Final = MappingProxyType(
    {
        "BASE": "completed_trade_net_mtm_r",
        "P75": "completed_trade_net_mtm_r_p75",
        "P95": "completed_trade_net_mtm_r_p95",
    }
)
_SERIALIZED_DAILY_LANE_FIELDS: Final = MappingProxyType(
    {
        "BASE": "daily_net_mtm_r_e12_i64be_base64",
        "P75": "daily_net_mtm_r_p75_e12_i64be_base64",
        "P95": "daily_net_mtm_r_p95_e12_i64be_base64",
    }
)
_SERIALIZED_TRADE_LANE_FIELDS: Final = MappingProxyType(
    {
        "BASE": "completed_trade_net_mtm_r_e12_i64be_base64",
        "P75": "completed_trade_net_mtm_r_p75_e12_i64be_base64",
        "P95": "completed_trade_net_mtm_r_p95_e12_i64be_base64",
    }
)
_TIER_LANES: Final = MappingProxyType(
    {
        "P75_REQUIRED": ("BASE", "P75"),
        "P95_PREFERENCE": ("BASE", "P75", "P95"),
    }
)

ARCHIVED_SELECTION_POLICY: Final = MappingProxyType(
    {
        "policy_version": "fxpm.winner-statistical-policy.v11",
        "cost_ladder_lane_order": ("BASE", "P75", "P95"),
        "required_cost_ladder_tier": "P75_REQUIRED",
        "preference_cost_ladder_tier": "P95_PREFERENCE",
        "p75_required_lanes": ("BASE", "P75"),
        "p95_preference_lanes": ("BASE", "P75", "P95"),
        "p95_fallback_rule": (
            "FALL_BACK_TO_P75_ONLY_WHEN_P95_CANDIDATE_ADMITTED_COUNT_IS_ZERO_"
            "BEFORE_CELL_PBO_AND_PAIRWISE_RESOLUTION"
        ),
        "daily_axis": "COMPLETE_UTC_CALENDAR_DAY_AXIS",
        "daily_evidence": "POST_COST_NET_MTM",
        "daily_unit": "PRECOMMITTED_CELL_RISK_UNIT",
        "fold_axis": "PRECOMMITTED_NONOVERLAPPING_HALF_OPEN_UTC_INTERVALS",
        "anchor_as_of_semantics": "INFORMATION_STRICTLY_BEFORE_OOS_START",
        "inventory_boundary": "FLAT_TO_FLAT_LIQUIDATED_POST_COST",
        "accounting_identity": (
            "EXACT_DECIMAL_SUM_DAILY_NET_MTM_EQUALS_COMPLETED_TRADE_NET_MTM_"
            "IN_SAME_PRECOMMITTED_CELL_RISK_UNIT"
        ),
        "inference": "DEPENDENCE_ROBUST_HAC_DSR_WITH_PRECOMMITTED_BLOCK_LCB_V3",
        "dependence_memory_rule": (
            "MAX_OBSERVED_TRADE_HOLDING_SAMPLE_SIZE_HAC_FLOOR_AND_"
            "INITIAL_POSITIVE_SEQUENCE_AUTOCORRELATION_MEMORY"
        ),
        "autocorrelation_memory_detector": (
            "GEYER_INITIAL_POSITIVE_PAIRED_AUTOCORRELATION_SEQUENCE_"
            "CAPPED_AT_ONE_QUARTER_FOLD"
        ),
        "primary_hac_bandwidth_rule": "DEPENDENCE_MEMORY_DAYS_MINUS_ONE",
        "sensitivity_hac_bandwidth_rule": (
            "MIN_TWO_TIMES_PRIMARY_MEMORY_AND_FOLD_LENGTH"
        ),
        "block_inference_rule": (
            "PRIMARY_AND_TWO_TIMES_PRIMARY_PRECOMMITTED_MEMORY_SCALES_WITH_"
            "FIXED_START_MID_END_ALIGNMENTS"
        ),
        "attempt_aware_dsr_rule": (
            "MAX_PRIMARY_AND_SENSITIVITY_HAC_DSR_ON_ONE_DAILY_LOG_GROWTH_"
            "MOMENT_VECTOR_PER_LANE"
        ),
        "attempt_aware_dsr_reapplications_per_block_scale": 0,
        "block_student_t_p_value_role": "DIAGNOSTIC_ONLY_NON_BINDING",
        "least_favorable_dependence_p_value_binds": False,
        "dependence_operation_bound": (
            "TWO_BLOCK_SCALES_TIMES_THREE_ALIGNMENTS_PLUS_QUARTER_FOLD_"
            "INITIAL_POSITIVE_SEQUENCE_UPPER_BOUND"
        ),
        "pairwise_dependence_operation_limit": MAX_DEPENDENCE_BLOCK_OPERATIONS,
        "pbo_contract_version": "fxpm.winner-cscv-pbo.v3",
        "pbo_candidate_population": (
            "ALL_PRECOMMITTED_EVALUATION_REQUIRED_ATTEMPTS_WITH_COMPLETE_"
            "ALIGNED_DAILY_COLUMNS_IN_EXACT_FROZEN_ROSTER"
        ),
        "pbo_daily_unit": "FIXED_0.25_PERCENT_RISK_DAILY_LOG_GROWTH",
        "pbo_performance_measure": "DAILY_LOG_GROWTH_SHARPE",
        "pbo_lane_rule": (
            "COMPUTE_BASE_P75_P95_ONCE_THEN_CONJOIN_LANES_BY_COST_LADDER_TIER"
        ),
        "pbo_contiguous_block_count": 8,
        "pbo_complete_combinations": 70,
        "pbo_min_evaluation_candidates": 2,
        "pbo_min_observations": 252,
        "pbo_max_probability": 0.50,
        "pbo_overfit_rank_rule": "SELECTED_OOS_RELATIVE_RANK_STRICTLY_BELOW_MEDIAN",
        "moving_block_bootstrap_contract_version": (
            "fxpm.winner-moving-block-bootstrap.v2"
        ),
        "moving_block_bootstrap_daily_unit": (
            "FIXED_0.25_PERCENT_RISK_DAILY_LOG_GROWTH"
        ),
        "moving_block_bootstrap_lane_rule": (
            "COMPUTE_BASE_P75_P95_ONCE_THEN_CONJOIN_LANES_BY_COST_LADDER_TIER"
        ),
        "moving_block_bootstrap_repetitions": 1024,
        "moving_block_bootstrap_lower_tail_numerator": 5,
        "moving_block_bootstrap_lower_tail_denominator": 100,
        "moving_block_bootstrap_seed_domain": "FXPM_WINNER_MBB_V2",
        "moving_block_bootstrap_seed_rule": (
            "SHA256_POLICY_CANDIDATE_LANE_BLOCK_SIZE_THEN_SPLITMIX64"
        ),
        "moving_block_bootstrap_prng": "SPLITMIX64_REJECTION_BOUNDED_INDEX_V1",
        "moving_block_bootstrap_block_rule": (
            "MAX_FIVE_DAYS_CEIL_TOTAL_DAYS_OVER_64_AND_PRECOMMITTED_"
            "DEPENDENCE_MEMORY_SCALE"
        ),
        "moving_block_bootstrap_pool_rule": (
            "POOL_ALL_WITHIN_FOLD_MOVING_BLOCK_STARTS_WITHOUT_CROSSING_"
            "ANY_CAUSAL_FOLD_BOUNDARY"
        ),
        "moving_block_bootstrap_max_resampled_blocks_per_scale": 76,
        "moving_block_bootstrap_lower_bound_must_be_positive": True,
        "moving_block_bootstrap_operation_limit": MAX_BOOTSTRAP_BLOCK_DRAWS,
        # This is a technical clock-skew allowance, not an evidence-age or
        # staleness policy. Historical evidence may be arbitrarily old, but a
        # canonical ledger must not claim that access/evaluation completed in the
        # future relative to the verifier's real UTC clock.
        "trusted_clock_max_future_skew_seconds": 300,
        "within_cell_alpha": 0.05,
        "paired_alpha": 0.05,
        "min_causal_folds": 4,
        "min_days_per_fold": 63,
        "min_total_days": 252,
        "min_hac_effective_days": 126.0,
        "min_dependence_blocks": 4,
        "minimum_dependence_blocks_rule": "EQUALS_MINIMUM_CAUSAL_FOLDS",
        "min_completed_trades": 40,
        "min_completed_trades_per_fold": 5,
        "min_hac_effective_completed_trades": 20.0,
        "trade_power_minimum_economic_edge_r": 0.25,
        "trade_power_maximum_ci_half_width_r": 0.25,
        "trade_power_target_power": 0.80,
        "trade_power_alpha": 0.05,
        "trade_power_rule": (
            "MAX_BOUND_TRAIN_FROZEN_REQUIREMENT_AND_RECOMPUTED_"
            "BASE_P75_P95_POSITION_EFFECTIVE_REQUIREMENT"
        ),
        "tail_fraction": 0.05,
        "primary_utility": "MEAN_DAILY_LOG_GROWTH_AT_FIXED_CELL_RISK",
        "primary_cell_risk_fraction": PRIMARY_CELL_RISK_FRACTION,
        "primary_transform": "LOG1P(0.0025_TIMES_DAILY_NET_MTM_R)",
        "primary_lcb_confidence": PRIMARY_LCB_CONFIDENCE,
        "primary_lcb_dependence_rule": (
            "MIN_PRIMARY_HAC_SENSITIVITY_HAC_AND_TWO_PRECOMMITTED_MEMORY_"
            "SCALE_BLOCK_STUDENT_T"
        ),
        "block_lcb_participates_in_candidate_score": True,
        "primary_candidate_score": (
            "MIN_DEPENDENCE_ROBUST_LCB_MEAN_DAILY_LOG_GROWTH_ACROSS_TIER_LANES"
        ),
        "primary_pairwise_inference": (
            "PAIRED_DAILY_LOG_GROWTH_DIFFERENCES_LEAST_FAVORABLE_"
            "RAW_BASE_P75_P95_PAIR_DEPENDENCE_ONCE_THEN_SEPARATE_TIER_HOLM"
        ),
        "selection_dominance_fields": (
            "conservative_primary_utility_lcb_daily_log_growth",
        ),
        "overall_mean_must_be_positive": True,
        "stochastic_sampling_support_required": True,
        "worst_fold_log_growth_must_be_positive": True,
        "newest_fold_log_growth_must_be_positive": True,
        "ladder_lane_mean_must_be_positive": True,
        "ladder_lane_worst_fold_log_growth_must_be_positive": True,
        "ladder_lane_newest_fold_log_growth_must_be_positive": True,
        # A lower confidence bound for each individual fold is useful evidence
        # about instability, but requiring four simultaneous fold-level
        # significance tests destroys power and is not a multiple-testing
        # correction.  Overall attempt-aware DSR is the inferential gate; fold
        # means are the precommitted robustness gates.
        "per_fold_hac_lcb_gate": False,
        # Tail, drawdown, and individual-fold LCBs remain exact reported diagnostics,
        # but are neither admission vetoes nor selection-dominance dimensions. A
        # universal cell-level risk preference here would be an unauthenticated
        # portfolio-risk oracle.
        # Synchronized portfolio replay and the separate capital authority own the
        # eventual economic limit; this historical cell selector owns no sizing.
        "diagnostic_only_fields": (
            "worst_fold_hac_lcb_daily_r",
            "newest_fold_hac_lcb_daily_r",
            "p75_worst_fold_hac_lcb_daily_r",
            "p75_newest_fold_hac_lcb_daily_r",
            "p95_worst_fold_hac_lcb_daily_r",
            "p95_newest_fold_hac_lcb_daily_r",
            "tail_cvar_daily_r",
            "max_path_drawdown_r",
            "p75_tail_cvar_daily_r",
            "p75_max_path_drawdown_r",
            "p95_tail_cvar_daily_r",
            "p95_max_path_drawdown_r",
        ),
        "candidate_admissibility_gate_codes": (
            "INSUFFICIENT_DAILY_OBSERVATIONS",
            "INSUFFICIENT_HAC_EFFECTIVE_DAYS",
            "INSUFFICIENT_DEPENDENCE_BLOCKS",
            "DEPENDENCE_GEOMETRY_UNSUPPORTED",
            "INSUFFICIENT_COMPLETED_TRADES",
            "INSUFFICIENT_COMPLETED_TRADE_HAC_POWER",
            "NO_STOCHASTIC_SAMPLING_SUPPORT",
            "NONPOSITIVE_MEAN",
            "NONPOSITIVE_WORST_FOLD",
            "NONPOSITIVE_NEWEST_FOLD",
            "LADDER_LANE_NONPOSITIVE_MEAN",
            "LADDER_LANE_NONPOSITIVE_WORST_FOLD",
            "LADDER_LANE_NONPOSITIVE_NEWEST_FOLD",
            "MOVING_BLOCK_BOOTSTRAP_INSUFFICIENT_EVIDENCE",
            "MOVING_BLOCK_BOOTSTRAP_LOWER_BOUND_FAILED",
        ),
        "selection_admissibility_gate_codes": ("WITHIN_CELL_ATTEMPT_AWARE_DSR_FAILED",),
        "cell_admissibility_gate_codes": (
            "CELL_PBO_INSUFFICIENT_EVIDENCE",
            "CELL_PBO_EXCEEDS_PRECOMMITTED_LIMIT",
        ),
        "failed_or_rejected_attempt_p_value": 1.0,
        "attempt_multiplicity": "EXACT_ATTEMPT_COUNT_DEFLATED_SHARPE",
        "winner_rule": (
            "BOOTSTRAP_AND_PBO_ADMISSIBLE_THEN_SCALAR_PRIMARY_LOG_GROWTH_"
            "PAIRED_SUPERIORITY_"
            "THEN_UNRESOLVED_PRIMARY_DIFFERENCE_GUARD_THEN_"
            "DETERMINISTIC_AUTHENTICATED_IDENTITY"
        ),
        "parameter_evidence_inherited": False,
        "power_calibration_version": "fxpm.winner-power-calibration.v1",
        "power_calibration_distribution": (
            "PYTHON_MT19937_GAUSSIAN_STANDARDIZED_DAILY_LOG_GROWTH"
        ),
        "power_calibration_seed_panel": "INTEGER_SEEDS_1_THROUGH_32",
        "power_calibration_expectation_schema": (
            "days_per_fold",
            "daily_sharpe",
            "attempted_trials",
            "minimum_passes_out_of_32",
            "maximum_passes_out_of_32",
        ),
        "power_calibration_expectations": (
            (63, 0.10, 1, 16, 24),
            (63, 0.30, 100, 20, 32),
            (63, 0.30, 1000, 12, 32),
            (252, 0.10, 1, 27, 32),
            (252, 0.10, 100, 0, 8),
            (252, 0.10, 1000, 0, 3),
            (252, 0.20, 100, 28, 32),
            (252, 0.20, 1000, 27, 32),
            (504, 0.10, 1, 30, 32),
            (504, 0.10, 100, 18, 32),
            (504, 0.10, 1000, 9, 32),
            (504, 0.20, 100, 30, 32),
            (504, 0.20, 1000, 30, 32),
        ),
        "minimum_total_days_role": (
            "STRUCTURAL_ELIGIBILITY_FLOOR_NOT_A_POWER_GUARANTEE"
        ),
        "high_trial_count_power_warning": (
            "252_TOTAL_DAYS_DOES_NOT_IMPLY_USEFUL_POWER_AT_1000_ATTEMPTS"
        ),
        "serial_dependence_calibration_expectation": (
            "AR1",
            0.90,
            0.10,
            4,
            252,
            100,
            "FAIL_INSUFFICIENT_INDEPENDENT_EVIDENCE",
        ),
    }
)

_BOOK_P_VALUE_CONTRACT = {
    "evidence_scope": "PROCESS_LEVEL_ONLY",
    "marginal_p_value": "ONE_SIDED_SUPERIORITY",
    "method": (
        "FIXED_0.25_PERCENT_RISK_DAILY_LOG_GROWTH_"
        "LEAST_FAVORABLE_SELECTED_COST_LADDER_TIER_LANES_"
        "PRIMARY_AND_SENSITIVITY_HAC_DSR_ONE_SIDED_ATTEMPT_AWARE_"
        "CONJUNCTIVE_MOVING_BLOCK_BOOTSTRAP_AND_CELL_CSCV_PBO"
    ),
    "parameter_evidence_inherited": False,
    "within_cell_multiplicity_adjusted": True,
}

# The archived generation is a deterministic reader, never an execution mode.
CURRENT_SELECTION_POLICY: Final = MappingProxyType({
    **ARCHIVED_SELECTION_POLICY,
    "policy_version": "fxpm.winner-statistical-policy.v12",
    "cost_ladder_lane_order": ("P75", "P95"),
    "p75_required_lanes": ("P75",),
    "p95_preference_lanes": ("P95",),
    "p95_fallback_rule": "QUALIFIED_P75_WHEN_NO_QUALIFIED_P95_WINNER",
    "pbo_lane_rule": "INDEPENDENT_FROZEN_TIER_ACCOUNTS",
    "moving_block_bootstrap_lane_rule": "INDEPENDENT_FROZEN_TIER_ACCOUNTS",
    "trade_power_rule": "EACH_TIER_OWN_TRAIN_BOUND_AND_RECOMPUTED_POSITION_REQUIREMENT",
    "primary_pairwise_inference": "INDEPENDENT_P75_P95_PAIRED_DAILY_DIFFERENCES_SEPARATE_TIER_HOLM",
})
PINNED_SELECTION_POLICY: Final = ARCHIVED_SELECTION_POLICY

_HEX40 = re.compile(r"[0-9a-f]{40}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_BACKTEST_POSITION_ID = re.compile(r"btp:[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9_.:/-]{1,128}\Z")
_SAFE_SYMBOL = re.compile(r"[A-Za-z0-9_.-]{1,32}\Z")
_MAX_EVENT_ORDER: Final = 2_147_483_647
_POSITION_ID_DIGEST_BYTES: Final = 32


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def risk_unit_quantum(risk_unit_contract: Mapping[str, Any]) -> Decimal:
    """Derive the exact R quantum from one declared risk-unit contract."""

    if not isinstance(risk_unit_contract, Mapping):
        raise ValueError("risk-unit contract is invalid")
    decimal_places = risk_unit_contract.get("r_decimal_places")
    if (
        type(decimal_places) is not int
        or not 0 <= decimal_places <= 16
        or risk_unit_contract.get("r_rounding") != "ROUND_HALF_EVEN"
    ):
        raise ValueError("risk-unit contract precision is invalid")
    return Decimal(1).scaleb(-decimal_places)


PINNED_RISK_UNIT_CONTRACT: Final = MappingProxyType(
    build_backtest_winner_risk_unit_contract()
)
R_QUANTUM: Final = risk_unit_quantum(PINNED_RISK_UNIT_CONTRACT)
_R_DECIMAL_PLACES: Final = PINNED_RISK_UNIT_CONTRACT["r_decimal_places"]
if _R_DECIMAL_PLACES != 12:
    raise RuntimeError(
        "Winner-v14 e12 transport requires a 12-place risk-unit contract"
    )
_R_SCALE_DECIMAL: Final = Decimal(1) / R_QUANTUM
PINNED_RISK_UNIT_CONTRACT_SHA256: Final = _sha256_json(
    dict(PINNED_RISK_UNIT_CONTRACT)
)
PINNED_VALIDATED_COST_LADDER_TOLERANCE: Final = MappingProxyType(
    build_backtest_validated_cost_ladder_tolerance()
)
PINNED_SELECTION_POLICY_SHA256: Final = _sha256_json(dict(PINNED_SELECTION_POLICY))
CURRENT_SELECTION_POLICY_SHA256: Final = _sha256_json(dict(CURRENT_SELECTION_POLICY))
PINNED_BOOK_P_VALUE_CONTRACT_SHA256: Final = _sha256_json(_BOOK_P_VALUE_CONTRACT)
CURRENT_BOOK_P_VALUE_CONTRACT_SHA256: Final = _sha256_json({
    **_BOOK_P_VALUE_CONTRACT,
    "method": "INDEPENDENT_FROZEN_TIER_SELECTED_ACCOUNT_HAC_DSR_MBB_CSCV_PBO",
})


@dataclass(frozen=True, slots=True)
class WinnerTransportEnvelope:
    max_request_bytes: int
    max_json_nodes: int
    max_total_completed_trades: int
    error_prefix: str = ""


LEGACY_TRANSPORT_ENVELOPE: Final = WinnerTransportEnvelope(MAX_REQUEST_BYTES, MAX_JSON_NODES, MAX_TOTAL_COMPLETED_TRADES)
V16_TRANSPORT_ENVELOPE: Final = WinnerTransportEnvelope(MAX_SUPPORTED_WINNER_REQUEST_BYTES, 2_000_000, 262_144, "V16_")
V16_METADATA_ENVELOPE: Final = replace(LEGACY_TRANSPORT_ENVELOPE, error_prefix="V16_")


@dataclass(frozen=True, slots=True)
class _ContractGeneration:
    selection_version: str
    request_version: str
    precommitment_version: str
    evidence_version: str
    marginal_version: str
    shared_partition_version: str
    policy: Mapping[str, Any]
    policy_sha256: str
    lane_fields: Mapping[str, str]
    trade_lane_fields: Mapping[str, str]
    tier_lanes: Mapping[str, tuple[str, ...]]
    archived: bool
    transport: WinnerTransportEnvelope = LEGACY_TRANSPORT_ENVELOPE

    @property
    def pair_field(self) -> str:
        return (
            "cost_ladder_contract_sha256"
            if self.archived
            else "frozen_tier_pair_contract_sha256"
        )


ARCHIVED_GENERATION: Final = _ContractGeneration(
    "fxpm.winner-selection.v14", "fxpm.winner-selection-request.v2",
    "fxpm.winner-precommitment.v3", "fxpm.winner-evidence-ledger.v6",
    "fxpm.winner-book-marginal.v1", "fxpm.winner-shared-oos-partition.v2",
    ARCHIVED_SELECTION_POLICY, _sha256_json(dict(ARCHIVED_SELECTION_POLICY)),
    _LANE_FIELDS, _TRADE_LANE_FIELDS, _TIER_LANES, True,
)
CURRENT_GENERATION: Final = _ContractGeneration(
    "fxpm.winner-selection.v15", "fxpm.winner-selection-request.v3",
    "fxpm.winner-precommitment.v4", "fxpm.winner-evidence-ledger.v7",
    "fxpm.winner-book-marginal.v2", "fxpm.winner-shared-oos-partition.v3",
    CURRENT_SELECTION_POLICY, CURRENT_SELECTION_POLICY_SHA256,
    MappingProxyType({"P75": "daily_net_mtm_r_p75", "P95": "daily_net_mtm_r_p95"}),
    MappingProxyType({"P75": "completed_trade_net_mtm_r_p75", "P95": "completed_trade_net_mtm_r_p95"}),
    MappingProxyType({"P75_REQUIRED": ("P75",), "P95_PREFERENCE": ("P95",)}), False,
)
V16_GENERATION: Final = replace(
    CURRENT_GENERATION, selection_version="fxpm.winner-selection.v16",
    request_version="fxpm.winner-selection-request.v4", precommitment_version="fxpm.winner-precommitment.v5",
    evidence_version="fxpm.winner-evidence-ledger.v8", marginal_version="fxpm.winner-book-marginal.v3",
    shared_partition_version="fxpm.winner-shared-oos-partition.v4", transport=V16_TRANSPORT_ENVELOPE,
)
CURRENT_WINNER_SELECTION_CONTRACT_VERSION: Final = CURRENT_GENERATION.selection_version
CURRENT_WINNER_SELECTION_REQUEST_VERSION: Final = CURRENT_GENERATION.request_version
CURRENT_WINNER_BOOK_MARGINAL_CONTRACT_VERSION: Final = CURRENT_GENERATION.marginal_version
CURRENT_PRECOMMITMENT_VERSION: Final = CURRENT_GENERATION.precommitment_version
CURRENT_EVIDENCE_LEDGER_VERSION: Final = CURRENT_GENERATION.evidence_version
CURRENT_SHARED_OOS_PARTITION_VERSION: Final = CURRENT_GENERATION.shared_partition_version


def _generation_for_precommitment(value: Mapping[str, Any]) -> _ContractGeneration:
    version = value.get("contract_version")
    if version == V16_GENERATION.precommitment_version:
        return V16_GENERATION
    if version == CURRENT_GENERATION.precommitment_version:
        return CURRENT_GENERATION
    if version == ARCHIVED_GENERATION.precommitment_version:
        return ARCHIVED_GENERATION
    raise _ContractError("PRECOMMITMENT_VERSION_INVALID")


def winner_generation_for_precommitment(value: Mapping[str, Any]) -> _ContractGeneration:
    return _generation_for_precommitment(value)


def winner_request_byte_limit(contract_version: str) -> int:
    """Assembled Winner request bound; metadata artifacts keep their own bounds."""
    for generation in (ARCHIVED_GENERATION, CURRENT_GENERATION, V16_GENERATION):
        if contract_version in (generation.selection_version, generation.request_version,
                generation.precommitment_version, generation.evidence_version, generation.marginal_version):
            return generation.transport.max_request_bytes
    raise ValueError("WINNER_TRANSPORT_VERSION_INVALID")


def _payload_transport(generation, payload_type):
    if generation is V16_GENERATION and payload_type is not WinnerEvidenceLedgerPayload:
        return V16_METADATA_ENVELOPE
    return generation.transport


def check_completed_trade_transport_count(total: int, *, generation: _ContractGeneration = CURRENT_GENERATION) -> None:
    if not any(generation is known for known in (ARCHIVED_GENERATION, CURRENT_GENERATION, V16_GENERATION)):
        raise _ContractError("WINNER_TRANSPORT_GENERATION_INVALID")
    if type(total) is not int or total < 0:
        raise _ContractError("COMPLETED_TRADE_COUNT_INVALID")
    if total > generation.transport.max_total_completed_trades:
        raise _ContractError(generation.transport.error_prefix + "GLOBAL_COMPLETED_TRADE_LIMIT_EXCEEDED")


def _generation_keys(
    keys: frozenset[str], generation: _ContractGeneration
) -> frozenset[str]:
    if generation.archived:
        return keys
    result = (keys - {"cost_ladder_contract_sha256"}) | (
        {generation.pair_field} if "cost_ladder_contract_sha256" in keys else set()
    )
    if keys is _PRECOMMITMENT_KEYS:
        result |= {"frozen_tier_pair_contract", "partition_cost_precommitments_sha256"}
    elif keys is _ATTEMPT_LEDGER_KEYS or keys is _EVIDENCE_LEDGER_KEYS:
        result |= {"partition_cost_precommitments_sha256"}
    return frozenset(result)


class _ContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class WinnerPrecommitmentPayload:
    """Owned canonical precommitment bytes and their deterministic hash."""

    _payload_json: bytes
    payload_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._payload_json.decode("ascii"))


@dataclass(frozen=True, slots=True)
class WinnerAttemptLedgerPayload:
    """Owned canonical full-attempt-ledger bytes and deterministic hash."""

    _payload_json: bytes
    payload_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._payload_json.decode("ascii"))


@dataclass(frozen=True, slots=True)
class WinnerEvidenceLedgerPayload:
    """Owned canonical exact-evidence-ledger bytes and deterministic hash."""

    _payload_json: bytes
    payload_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._payload_json.decode("ascii"))


@dataclass(frozen=True, slots=True)
class WinnerSelectionRequest:
    """Exact immutable input to the historical Winner V14 evaluator."""

    precommitment: WinnerPrecommitmentPayload
    attempt_ledger: WinnerAttemptLedgerPayload
    evidence_ledger: WinnerEvidenceLedgerPayload
    request_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": _generation_for_precommitment(self.precommitment.to_dict()).request_version,
            "precommitment": {
                "payload": self.precommitment.to_dict(),
                "payload_sha256": self.precommitment.payload_sha256,
            },
            "attempt_ledger": {
                "payload": self.attempt_ledger.to_dict(),
                "payload_sha256": self.attempt_ledger.payload_sha256,
            },
            "evidence_ledger": {
                "payload": self.evidence_ledger.to_dict(),
                "payload_sha256": self.evidence_ledger.payload_sha256,
            },
        }


def _owned_json(value: Any, byte_limit: int = MAX_REQUEST_BYTES, *,
        _envelope: WinnerTransportEnvelope = LEGACY_TRANSPORT_ENVELOPE, _copy_owned: bool = True) -> Any:
    """Iteratively reject cycles, aliases, depth bombs, and oversized JSON."""

    seen: set[int] = set()
    nodes = 0
    chars = 0
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > _envelope.max_json_nodes:
            raise _ContractError(_envelope.error_prefix + "INPUT_STRUCTURE_LIMIT_EXCEEDED")
        if depth > MAX_JSON_DEPTH:
            raise _ContractError("INPUT_STRUCTURE_LIMIT_EXCEEDED")
        current_type = type(current)
        if current_type is dict:
            identity = id(current)
            if identity in seen:
                raise _ContractError("CYCLIC_OR_ALIASED_CONTAINER")
            seen.add(identity)
            if len(current) > MAX_CONTAINER_ITEMS:
                raise _ContractError("INPUT_CONTAINER_WIDTH_EXCEEDED")
            for key, item in current.items():
                if type(key) is not str:
                    raise _ContractError("NON_JSON_DICTIONARY_KEY")
                if len(key) > 256:
                    raise _ContractError("INPUT_STRING_LIMIT_EXCEEDED")
                chars += len(key)
                stack.append((item, depth + 1))
        elif current_type is list:
            identity = id(current)
            if identity in seen:
                raise _ContractError("CYCLIC_OR_ALIASED_CONTAINER")
            seen.add(identity)
            if len(current) > MAX_CONTAINER_ITEMS:
                raise _ContractError("INPUT_CONTAINER_WIDTH_EXCEEDED")
            stack.extend((item, depth + 1) for item in current)
        elif current_type is str:
            if len(current) > MAX_STRING_CHARS:
                raise _ContractError("INPUT_STRING_LIMIT_EXCEEDED")
            chars += len(current)
        elif current_type is int:
            if abs(current) > 9_007_199_254_740_991:
                raise _ContractError("INTEGER_OUT_OF_CANONICAL_RANGE")
        elif current_type is float:
            if not math.isfinite(current):
                raise _ContractError("NON_FINITE_JSON_NUMBER")
        elif current_type not in (bool, type(None)):
            raise _ContractError("NON_CANONICAL_JSON_TYPE")
        if chars > byte_limit:
            raise _ContractError(_envelope.error_prefix + "INPUT_STRING_BUDGET_EXCEEDED")
    try:
        encoded = _canonical_bytes(value)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _ContractError("INPUT_CANONICALIZATION_FAILED") from exc
    if len(encoded) > byte_limit:
        raise _ContractError(_envelope.error_prefix + "INPUT_BYTE_LIMIT_EXCEEDED")
    return json.loads(encoded.decode("ascii")) if _copy_owned else value


def _exact_dict(value: Any, keys: frozenset[str], code: str) -> dict[str, Any]:
    if type(value) is not dict or frozenset(value) != keys:
        raise _ContractError(code)
    return value


def _hex(value: Any, matcher: re.Pattern[str], code: str) -> str:
    if type(value) is not str or matcher.fullmatch(value) is None:
        raise _ContractError(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        raise _ContractError(code)
    return value


def _exact_int(value: Any, low: int, high: int, code: str) -> int:
    if type(value) is not int or not low <= value <= high:
        raise _ContractError(code)
    return value


def _finite_float(value: Any, low: float, high: float, code: str) -> float:
    if type(value) is not float or not math.isfinite(value) or not low <= value <= high:
        raise _ContractError(code)
    return value


def _encode_r_e12(value: Any, maximum_abs_r: float, code: str) -> int:
    """Encode one canonical Winner R float without approximation."""

    if type(value) is not float or not math.isfinite(value):
        raise _ContractError(code)
    try:
        decimal_value = Decimal(str(value))
        scaled = decimal_value * _R_SCALE_DECIMAL
        integral = scaled.to_integral_value()
    except InvalidOperation as exc:
        raise _ContractError(code) from exc
    maximum = int(Decimal(str(maximum_abs_r)) * _R_SCALE_DECIMAL)
    if (
        not decimal_value.is_finite()
        or scaled != integral
        or abs(integral) > maximum
    ):
        raise _ContractError(code)
    return int(integral)


def _decode_r_e12(value: Any, maximum_abs_r: float, code: str) -> float:
    """Decode one fixed-point Winner R scalar and prove exact float identity."""

    maximum = int(Decimal(str(maximum_abs_r)) * _R_SCALE_DECIMAL)
    scaled = _exact_int(value, -maximum, maximum, code)
    decimal_value = Decimal(scaled) / _R_SCALE_DECIMAL
    decoded = float(decimal_value)
    if not math.isfinite(decoded) or Decimal(str(decoded)) != decimal_value:
        raise _ContractError(code)
    return decoded


def _encode_r_e12_i64be_base64(
    values: Any,
    maximum_abs_r: float,
    code: str,
) -> str:
    """Pack exact e12 integers as canonical standard-base64 signed int64 BE."""

    if type(values) is not list or len(values) > MAX_CONTAINER_ITEMS:
        raise _ContractError(code)
    maximum = int(Decimal(str(maximum_abs_r)) * _R_SCALE_DECIMAL)
    packed = bytearray()
    for value in values:
        exact = _exact_int(value, -maximum, maximum, code)
        packed.extend(struct.pack(">q", exact))
    return base64.b64encode(packed).decode("ascii")


def _decode_r_e12_i64be_base64(
    value: Any,
    *,
    expected_count: int,
    maximum_abs_r: float,
    code: str,
) -> list[int]:
    """Decode one canonical fixed-width R vector without changing any value."""

    if (
        type(value) is not str
        or type(expected_count) is not int
        or not 0 <= expected_count <= MAX_CONTAINER_ITEMS
    ):
        raise _ContractError(code)
    expected_bytes = 8 * expected_count
    expected_chars = 4 * ((expected_bytes + 2) // 3)
    if len(value) != expected_chars:
        raise _ContractError(code)
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
        raise _ContractError(code) from exc
    if (
        len(decoded) != expected_bytes
        or base64.b64encode(decoded).decode("ascii") != value
    ):
        raise _ContractError(code)
    maximum = int(Decimal(str(maximum_abs_r)) * _R_SCALE_DECIMAL)
    values = [
        item[0] for item in struct.iter_unpack(">q", decoded)
    ]
    if any(not -maximum <= item <= maximum for item in values):
        raise _ContractError(code)
    return values


def _encode_utc_second_offsets(
    values: Any,
    *,
    fold_start_utc: datetime,
    fold_end_exclusive_utc: datetime,
    code: str,
) -> list[int]:
    if type(values) is not list:
        raise _ContractError(code)
    encoded: list[int] = []
    for value in values:
        parsed = _timestamp(value, code)
        if not fold_start_utc <= parsed < fold_end_exclusive_utc:
            raise _ContractError(code)
        delta = parsed - fold_start_utc
        if delta.microseconds != 0:
            raise _ContractError(code)
        encoded.append(delta.days * 86_400 + delta.seconds)
    return encoded


def _decode_utc_second_offsets(
    values: Any,
    *,
    fold_start_utc: datetime,
    fold_end_exclusive_utc: datetime,
    code: str,
) -> list[str]:
    if type(values) is not list:
        raise _ContractError(code)
    maximum = int((fold_end_exclusive_utc - fold_start_utc).total_seconds()) - 1
    decoded: list[str] = []
    for value in values:
        offset = _exact_int(value, 0, maximum, code)
        moment = fold_start_utc + timedelta(seconds=offset)
        decoded.append(moment.isoformat(timespec="seconds").replace("+00:00", "Z"))
    return decoded


def _encode_event_order_offsets(
    open_orders: Any,
    close_orders: Any,
    code: str,
) -> tuple[int, list[int], list[int]]:
    if type(open_orders) is not list or type(close_orders) is not list:
        raise _ContractError(code)
    if len(open_orders) != len(close_orders):
        raise _ContractError(code)
    validated_open = [
        _exact_int(value, 0, _MAX_EVENT_ORDER, code) for value in open_orders
    ]
    validated_close = [
        _exact_int(value, 0, _MAX_EVENT_ORDER, code) for value in close_orders
    ]
    if not validated_open:
        return 0, [], []
    anchor = min((*validated_open, *validated_close))
    return (
        anchor,
        [value - anchor for value in validated_open],
        [value - anchor for value in validated_close],
    )


def _decode_event_order_offsets(
    anchor_raw: Any,
    open_offsets: Any,
    close_offsets: Any,
    code: str,
) -> tuple[list[int], list[int]]:
    anchor = _exact_int(anchor_raw, 0, _MAX_EVENT_ORDER, code)
    if type(open_offsets) is not list or type(close_offsets) is not list:
        raise _ContractError(code)
    if len(open_offsets) != len(close_offsets):
        raise _ContractError(code)
    maximum_offset = _MAX_EVENT_ORDER - anchor
    validated_open = [
        _exact_int(value, 0, maximum_offset, code) for value in open_offsets
    ]
    validated_close = [
        _exact_int(value, 0, maximum_offset, code) for value in close_offsets
    ]
    if not validated_open:
        if anchor != 0:
            raise _ContractError(code)
        return [], []
    if min((*validated_open, *validated_close)) != 0:
        raise _ContractError(code)
    return (
        [anchor + value for value in validated_open],
        [anchor + value for value in validated_close],
    )


def _encode_position_id_digests(position_ids: Any, code: str) -> str:
    if type(position_ids) is not list:
        raise _ContractError(code)
    if any(
        type(value) is not str or _BACKTEST_POSITION_ID.fullmatch(value) is None
        for value in position_ids
    ):
        raise _ContractError(code)
    if len(set(position_ids)) != len(position_ids):
        raise _ContractError(code)
    raw = b"".join(bytes.fromhex(value[4:]) for value in position_ids)
    return base64.b64encode(raw).decode("ascii")


def _decode_position_id_digests(value: Any, trade_count: int, code: str) -> list[str]:
    if type(value) is not str:
        raise _ContractError(code)
    expected_bytes = _POSITION_ID_DIGEST_BYTES * trade_count
    expected_chars = 4 * ((expected_bytes + 2) // 3)
    if len(value) != expected_chars:
        raise _ContractError(code)
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
        raise _ContractError(code) from exc
    if (
        len(decoded) != expected_bytes
        or base64.b64encode(decoded).decode("ascii") != value
    ):
        raise _ContractError(code)
    position_ids = [
        "btp:" + decoded[offset : offset + _POSITION_ID_DIGEST_BYTES].hex()
        for offset in range(0, expected_bytes, _POSITION_ID_DIGEST_BYTES)
    ]
    return position_ids


def _timestamp(value: Any, code: str) -> datetime:
    if (
        type(value) is not str
        or len(value) < 20
        or len(value) > 32
        or not value.endswith("Z")
    ):
        raise _ContractError(code)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise _ContractError(code) from exc
    if parsed.tzinfo != timezone.utc or parsed.microsecond != 0:
        raise _ContractError(code)
    canonical = parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    if value != canonical:
        raise _ContractError(code)
    return parsed


def _day(value: Any, code: str) -> date:
    if type(value) is not str or len(value) != 10:
        raise _ContractError(code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise _ContractError(code) from exc
    if parsed.isoformat() != value:
        raise _ContractError(code)
    return parsed


def compute_cell_id(symbol: Any, timeframe: Any, regime: Any) -> str | None:
    try:
        if (
            type(symbol) is not str
            or _SAFE_SYMBOL.fullmatch(symbol) is None
            or type(timeframe) is not str
            or timeframe not in TIMEFRAMES
            or type(regime) is not str
            or regime not in REGIMES
        ):
            return None
        return _sha256_json(
            {"regime": regime, "symbol": symbol, "timeframe": timeframe}
        )
    except Exception:
        return None


def _make_payload(
    value: Any,
    payload_type: type[
        WinnerPrecommitmentPayload
        | WinnerAttemptLedgerPayload
        | WinnerEvidenceLedgerPayload
    ],
    *, _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> (
    WinnerPrecommitmentPayload
    | WinnerAttemptLedgerPayload
    | WinnerEvidenceLedgerPayload
):
    envelope = _payload_transport(_generation, payload_type)
    owned = _owned_json(value, envelope.max_request_bytes, _envelope=envelope)
    if type(owned) is not dict:
        raise _ContractError("PAYLOAD_ROOT_TYPE_INVALID")
    encoded = _canonical_bytes(owned)
    return payload_type(encoded, _sha256_bytes(encoded))


def _request_binding_body(
    precommitment_sha256: str,
    attempt_ledger_sha256: str,
    evidence_ledger_sha256: str,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    return {
        "contract_version": _generation.request_version,
        "precommitment_payload_sha256": precommitment_sha256,
        "attempt_ledger_payload_sha256": attempt_ledger_sha256,
        "evidence_ledger_payload_sha256": evidence_ledger_sha256,
    }


def _create_winner_selection_request(
    precommitment_payload: Any,
    attempt_ledger_payload: Any,
    evidence_ledger_payload: Any,
    *, _generation: _ContractGeneration,
) -> WinnerSelectionRequest:
    """Own and hash the three pipeline-native payloads as one exact request."""

    precommitment = _make_payload(precommitment_payload, WinnerPrecommitmentPayload, _generation=_generation)
    attempt_ledger = _make_payload(attempt_ledger_payload, WinnerAttemptLedgerPayload, _generation=_generation)
    evidence_ledger = _make_payload(evidence_ledger_payload, WinnerEvidenceLedgerPayload, _generation=_generation)
    request_sha256 = _sha256_json(
        _request_binding_body(
            precommitment.payload_sha256, attempt_ledger.payload_sha256, evidence_ledger.payload_sha256,
            _generation=_generation,
        )
    )
    request = WinnerSelectionRequest(precommitment, attempt_ledger, evidence_ledger, request_sha256)
    if _generation is V16_GENERATION:
        _owned_json(request.to_dict(), _generation.transport.max_request_bytes,
            _envelope=_generation.transport, _copy_owned=False)
    elif len(_canonical_bytes(request.to_dict())) > _generation.transport.max_request_bytes:
        raise _ContractError(_generation.transport.error_prefix + "REQUEST_BYTE_LIMIT_EXCEEDED")
    return request


def create_winner_selection_request(precommitment_payload: Any, attempt_ledger_payload: Any,
        evidence_ledger_payload: Any) -> WinnerSelectionRequest | None:
    try:
        generation = _generation_for_precommitment(precommitment_payload)
    except Exception:
        return None
    if generation is V16_GENERATION:
        return create_winner_selection_request_v16(precommitment_payload, attempt_ledger_payload, evidence_ledger_payload)
    try:
        return _create_winner_selection_request(precommitment_payload, attempt_ledger_payload, evidence_ledger_payload,
            _generation=generation)
    except Exception:
        return None


def create_winner_selection_request_v16(precommitment_payload: Any, attempt_ledger_payload: Any,
        evidence_ledger_payload: Any) -> WinnerSelectionRequest:
    """Build the explicit transport successor; preserve named capacity refusals."""
    if _generation_for_precommitment(precommitment_payload) is not V16_GENERATION:
        raise _ContractError("V16_PRECOMMITMENT_VERSION_REQUIRED")
    return _create_winner_selection_request(precommitment_payload, attempt_ledger_payload, evidence_ledger_payload,
        _generation=V16_GENERATION)


def _payload_from_wrapper(
    value: Any,
    expected_type: type[
        WinnerPrecommitmentPayload
        | WinnerAttemptLedgerPayload
        | WinnerEvidenceLedgerPayload
    ],
    code: str,
    *, _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[dict[str, Any], str]:
    if type(value) is not expected_type:
        raise _ContractError(code + "_TYPE_INVALID")
    envelope = _payload_transport(_generation, expected_type)
    encoded = value._payload_json
    if type(encoded) is not bytes or len(encoded) > envelope.max_request_bytes:
        raise _ContractError(envelope.error_prefix + code + "_BYTES_INVALID")
    if type(value.payload_sha256) is not str or value.payload_sha256 != _sha256_bytes(
        encoded
    ):
        raise _ContractError(code + "_HASH_INVALID")
    try:
        decoded = json.loads(encoded.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _ContractError(code + "_CANONICAL_JSON_INVALID") from exc
    owned = _owned_json(decoded, envelope.max_request_bytes, _envelope=envelope)
    if type(owned) is not dict or _canonical_bytes(owned) != encoded:
        raise _ContractError(code + "_CANONICAL_JSON_INVALID")
    return owned, value.payload_sha256


def _request_payloads(
    request: Any, *, _generation: _ContractGeneration = ARCHIVED_GENERATION
) -> tuple[dict[str, Any], str, dict[str, Any], str, dict[str, Any], str, str]:
    if type(request) is not WinnerSelectionRequest:
        raise _ContractError("REQUEST_TYPE_INVALID")
    policy, policy_sha = _payload_from_wrapper(
        request.precommitment,
        WinnerPrecommitmentPayload,
        "PRECOMMITMENT_PAYLOAD",
    )
    _generation = _generation_for_precommitment(policy)
    attempts, attempt_sha = _payload_from_wrapper(
        request.attempt_ledger,
        WinnerAttemptLedgerPayload,
        "ATTEMPT_LEDGER_PAYLOAD",
        _generation=_generation,
    )
    evidence, evidence_sha = _payload_from_wrapper(
        request.evidence_ledger,
        WinnerEvidenceLedgerPayload,
        "EVIDENCE_LEDGER_PAYLOAD",
        _generation=_generation,
    )
    expected_request_sha = _sha256_json(
        _request_binding_body(
            policy_sha, attempt_sha, evidence_sha, _generation=_generation
        )
    )
    if _generation is V16_GENERATION:
        _owned_json(request.to_dict(), _generation.transport.max_request_bytes,
            _envelope=_generation.transport, _copy_owned=False)
    if (
        type(request.request_sha256) is not str
        or request.request_sha256 != expected_request_sha
        or (_generation is not V16_GENERATION
            and len(_canonical_bytes(request.to_dict())) > _generation.transport.max_request_bytes)
    ):
        raise _ContractError("REQUEST_BINDING_INVALID")
    return (
        policy,
        policy_sha,
        attempts,
        attempt_sha,
        evidence,
        evidence_sha,
        expected_request_sha,
    )


_PRECOMMITMENT_KEYS = frozenset(
    {
        "contract_version",
        "run_id",
        "precommitted_at_utc",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "selection_policy_sha256",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
        "planned_attempt_count",
        "planned_fold_count",
        "causal_fold_plan_sha256",
        "causal_fold_plan",
        "latest_evidence_boundary_utc",
    }
)
_FOLD_PLAN_KEYS = frozenset(
    {
        "fold_ordinal",
        "fold_id",
        "anchor_as_of_utc",
        "oos_start_date_utc",
        "oos_end_date_utc",
    }
)
_ATTEMPT_LEDGER_KEYS = frozenset(
    {
        "contract_version",
        "run_id",
        "frozen_at_utc",
        "precommitment_payload_sha256",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
        "attempt_count",
        "attempts_sha256",
        "attempts",
    }
)
_ATTEMPT_KEYS = frozenset(
    {
        "ordinal",
        "attempt_id",
        "process_candidate_id",
        "strategy_family",
        "strategy_id",
        "parameter_count",
        "precommitted_evidence_role",
        "process_sha256",
        "horizon_process_sha256",
        "trial_definition_sha256",
        "refit_recipe_sha256",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
    }
)
_EVIDENCE_LEDGER_KEYS = frozenset(
    {
        "contract_version",
        "run_id",
        "generated_at_utc",
        "evidence_access_started_at_utc",
        "evidence_access_completed_at_utc",
        "latest_evidence_boundary_utc",
        "precommitment_payload_sha256",
        "attempt_ledger_payload_sha256",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
        "shared_fold_axis_count",
        "shared_fold_axis_sha256",
        "shared_fold_axis",
        "attempt_count",
        "entries_sha256",
        "entries",
        "evidence_scope",
        "parameter_evidence_inherited",
    }
)
_EVIDENCE_ENTRY_KEYS = frozenset(
    {
        "ordinal",
        "attempt_id",
        "process_candidate_id",
        "process_sha256",
        "horizon_process_sha256",
        "trial_definition_sha256",
        "refit_recipe_sha256",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
        "evidence_status",
        "status_reason",
        "fold_count",
        "folds_sha256",
        "folds",
    }
)
_SHARED_FOLD_AXIS_KEYS = frozenset(
    {
        "fold_ordinal",
        "fold_id",
        "anchor_as_of_utc",
        "oos_start_date_utc",
        "oos_end_date_utc",
        "daily_dates_utc",
        "shared_oos_partition",
    }
)
_SHARED_OOS_PARTITION_KEYS = frozenset(
    {
        "schema_version",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "causal_fold_plan_row_sha256",
        "native_prefix_index_sha256",
        "native_prefix_content_sha256",
        "source_contract_sha256",
        "native_source_sha256",
        "activation_manifest_sha256",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "execution_spec_receipt_sha256",
        "cost_contract_sha256",
        "cost_ladder_contract_sha256",
        "risk_unit_contract_sha256",
        "receipt_sha256",
    }
)
_TRADE_POWER_BINDING_KEYS = frozenset(
    {
        "schema_version",
        "attempt_ledger_payload_sha256",
        "attempted_trials_exact",
        "train_requirement_sha256",
        "train_calibration_sample_sha256",
        "train_partition_contract_sha256",
        "validation_decision_sha256",
        "validation_partition_contract_sha256",
        "required_effective_trades",
        "binding_sha256",
    }
)
_FOLD_KEYS = frozenset(
    {
        "fold_ordinal",
        "anchor_selection_receipt_sha256",
        "anchor_horizon_decision_sha256",
        "anchor_trial_ledger_sha256",
        "anchor_selected_trial_sha256",
        "anchor_refit_receipt_sha256",
        "attempt_replay_receipt_sha256",
        "attempt_oos_source_receipt_sha256",
        "position_activity_ledger_sha256",
        *_SERIALIZED_DAILY_LANE_FIELDS.values(),
        "daily_ladder_sha256",
        "completed_trade_open_offset_seconds",
        "completed_trade_close_offset_seconds",
        "completed_trade_position_id_digests_base64",
        "completed_trade_event_order_anchor",
        "completed_trade_open_event_order_offsets",
        "completed_trade_close_event_order_offsets",
        *_SERIALIZED_TRADE_LANE_FIELDS.values(),
        "trade_power_binding",
        "trade_vector_sha256",
    }
)


def _cell_fields(value: dict[str, Any], prefix: str) -> tuple[str, str, str, str]:
    symbol = value["symbol"]
    timeframe = value["timeframe"]
    regime = value["regime"]
    if type(symbol) is not str or _SAFE_SYMBOL.fullmatch(symbol) is None:
        raise _ContractError(prefix + "SYMBOL_INVALID")
    if type(timeframe) is not str or timeframe not in TIMEFRAMES:
        raise _ContractError(prefix + "TIMEFRAME_INVALID")
    if type(regime) is not str or regime not in REGIMES:
        raise _ContractError(prefix + "REGIME_INVALID")
    expected_cell = compute_cell_id(symbol, timeframe, regime)
    if type(value["cell_id"]) is not str or value["cell_id"] != expected_cell:
        raise _ContractError(prefix + "CELL_ID_MISMATCH")
    return value["cell_id"], symbol, timeframe, regime


def _require_same_cell(
    value: dict[str, Any], expected: tuple[str, str, str, str], prefix: str
) -> None:
    for field, wanted in zip(
        ("cell_id", "symbol", "timeframe", "regime"), expected, strict=True
    ):
        if type(value[field]) is not str or value[field] != wanted:
            raise _ContractError(prefix + field.upper() + "_MISMATCH")


def _validate_causal_fold_plan(
    raw_plan: Any,
    *,
    declared_count: Any,
    declared_sha256: Any,
    declared_latest_boundary: Any,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[list[dict[str, Any]], datetime]:
    if type(raw_plan) is not list:
        raise _ContractError("CAUSAL_FOLD_PLAN_INVALID")
    fold_count = _exact_int(
        declared_count,
        int(_generation.policy["min_causal_folds"]),
        MAX_FOLDS,
        "PLANNED_FOLD_COUNT_INVALID",
    )
    if len(raw_plan) != fold_count:
        raise _ContractError("CAUSAL_FOLD_PLAN_COUNT_MISMATCH")
    if declared_sha256 != _sha256_json(raw_plan):
        raise _ContractError("CAUSAL_FOLD_PLAN_HASH_MISMATCH")
    plan: list[dict[str, Any]] = []
    seen_fold_ids: set[str] = set()
    prior_end: date | None = None
    latest_boundary: datetime | None = None
    for ordinal, raw in enumerate(raw_plan):
        prefix = f"FOLD_PLAN_{ordinal:03d}_"
        fold = _exact_dict(raw, _FOLD_PLAN_KEYS, prefix + "SCHEMA_INVALID")
        if type(fold["fold_ordinal"]) is not int or fold["fold_ordinal"] != ordinal:
            raise _ContractError(prefix + "ORDINAL_INVALID")
        fold_id = _hex(fold["fold_id"], _HEX40, prefix + "ID_INVALID")
        if fold_id in seen_fold_ids:
            raise _ContractError(prefix + "DUPLICATE_IDENTITY")
        seen_fold_ids.add(fold_id)
        start = _day(fold["oos_start_date_utc"], prefix + "START_DATE_INVALID")
        end = _day(fold["oos_end_date_utc"], prefix + "END_DATE_INVALID")
        days = (end - start).days + 1
        if (
            not int(_generation.policy["min_days_per_fold"])
            <= days
            <= MAX_DAYS_PER_FOLD
        ):
            raise _ContractError(prefix + "DATE_SPAN_INVALID")
        if prior_end is not None and start <= prior_end:
            raise _ContractError("CAUSAL_FOLD_PLAN_INTERVALS_OVERLAP")
        start_utc = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        anchor = _timestamp(fold["anchor_as_of_utc"], prefix + "ANCHOR_TIME_INVALID")
        # "As of" is the half-open information boundary.  It is not the wall
        # clock at which a retrospective evaluator later accessed the bytes.
        if anchor != start_utc:
            raise _ContractError(prefix + "ANCHOR_NOT_EXACT_OOS_BOUNDARY")
        prior_end = end
        latest_boundary = datetime.combine(
            end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
        )
        plan.append(fold)
    if latest_boundary is None:
        raise _ContractError("CAUSAL_FOLD_PLAN_EMPTY")
    declared_boundary = _timestamp(
        declared_latest_boundary, "LATEST_EVIDENCE_BOUNDARY_INVALID"
    )
    if declared_boundary != latest_boundary:
        raise _ContractError("LATEST_EVIDENCE_BOUNDARY_MISMATCH")
    return plan, latest_boundary


def _validate_precommitment(
    payload: dict[str, Any], *, _generation: _ContractGeneration = ARCHIVED_GENERATION
) -> tuple[
    datetime,
    tuple[str, str, str, str],
    int,
    list[dict[str, Any]],
    datetime,
]:
    _generation = _generation_for_precommitment(payload)
    value = _exact_dict(
        payload,
        _generation_keys(_PRECOMMITMENT_KEYS, _generation),
        "PRECOMMITMENT_SCHEMA_INVALID",
    )
    if value["contract_version"] != _generation.precommitment_version:
        raise _ContractError("PRECOMMITMENT_VERSION_INVALID")
    _hex(value["run_id"], _HEX64, "RUN_ID_INVALID")
    precommitted_at = _timestamp(
        value["precommitted_at_utc"], "PRECOMMITMENT_TIME_INVALID"
    )
    cell = _cell_fields(value, "PRECOMMITMENT_")
    if value["selection_policy_sha256"] != _generation.policy_sha256:
        raise _ContractError("SELECTION_POLICY_MISMATCH")
    if not _generation.archived:
        from pm_core import validate_backtest_frozen_tier_pair_contract

        pair = validate_backtest_frozen_tier_pair_contract(
            value["frozen_tier_pair_contract"]
        )
        if pair is None or value[_generation.pair_field] != _sha256_json(
            value["frozen_tier_pair_contract"]
        ):
            raise _ContractError("FROZEN_TIER_PAIR_CONTRACT_INVALID")
        expected_cost = _sha256_json(
            {
                item["tier"]: _sha256_json(item["numerical_execution_cost_policy"])
                for item in pair["tiers"]
            }
        )
        if value["cost_contract_sha256"] != expected_cost:
            raise _ContractError("FROZEN_TIER_PAIR_COST_CONTRACT_MISMATCH")
        if any(
            item["frozen_spread_tier"]["symbol"] != value["symbol"]
            or item["frozen_spread_tier"]["timeframe"] != value["timeframe"]
            for item in pair["tiers"]
        ):
            raise _ContractError("FROZEN_TIER_PAIR_CELL_MISMATCH")
        _hex(
            value["partition_cost_precommitments_sha256"],
            _HEX64,
            "PARTITION_COST_PRECOMMITMENTS_INVALID",
        )
    for field in (
        "source_contract_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    ):
        _hex(value[field], _HEX64, field.upper() + "_INVALID")
    if value["risk_unit_contract_sha256"] != PINNED_RISK_UNIT_CONTRACT_SHA256:
        raise _ContractError("RISK_UNIT_CONTRACT_MISMATCH")
    attempts = _exact_int(
        value["planned_attempt_count"], 1, MAX_ATTEMPTS, "PLANNED_ATTEMPT_COUNT_INVALID"
    )
    fold_plan, latest_boundary = _validate_causal_fold_plan(
        value["causal_fold_plan"],
        declared_count=value["planned_fold_count"],
        declared_sha256=value["causal_fold_plan_sha256"],
        declared_latest_boundary=value["latest_evidence_boundary_utc"],
        _generation=_generation,
    )
    return precommitted_at, cell, attempts, fold_plan, latest_boundary


def _process_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha256_json(
        {
            "processes": [
                {
                    "attempt_id": value["attempt_id"],
                    "process_candidate_id": value["process_candidate_id"],
                    "process_sha256": value["process_sha256"],
                    "strategy_family": value["strategy_family"],
                    "strategy_id": value["strategy_id"],
                    "precommitted_evidence_role": value["precommitted_evidence_role"],
                }
                for value in attempts
            ]
        }
    )


def _horizon_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha256_json(
        {
            "horizons": [
                {
                    "attempt_id": value["attempt_id"],
                    "horizon_process_sha256": value["horizon_process_sha256"],
                    "refit_recipe_sha256": value["refit_recipe_sha256"],
                }
                for value in attempts
            ]
        }
    )


def _trial_registry(attempts: list[dict[str, Any]]) -> str:
    return _sha256_json(
        {
            "trials": [
                {
                    "attempt_id": value["attempt_id"],
                    "trial_definition_sha256": value["trial_definition_sha256"],
                    "parameter_count": value["parameter_count"],
                }
                for value in attempts
            ]
        }
    )


def _validate_attempt_ledger(
    payload: dict[str, Any],
    policy: dict[str, Any],
    policy_sha: str,
    policy_time: datetime,
    cell: tuple[str, str, str, str],
    planned_attempts: int,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[datetime, list[dict[str, Any]]]:
    value = _exact_dict(payload, _generation_keys(_ATTEMPT_LEDGER_KEYS, _generation), "ATTEMPT_LEDGER_SCHEMA_INVALID")
    if value["contract_version"] != ATTEMPT_LEDGER_VERSION:
        raise _ContractError("ATTEMPT_LEDGER_VERSION_INVALID")
    if value["run_id"] != policy["run_id"]:
        raise _ContractError("ATTEMPT_LEDGER_RUN_MISMATCH")
    frozen_at = _timestamp(value["frozen_at_utc"], "ATTEMPT_LEDGER_TIME_INVALID")
    if frozen_at <= policy_time:
        raise _ContractError("ATTEMPT_LEDGER_NOT_AFTER_PRECOMMITMENT")
    if value["precommitment_payload_sha256"] != policy_sha:
        raise _ContractError("ATTEMPT_LEDGER_PRECOMMITMENT_BINDING_INVALID")
    _require_same_cell(value, cell, "ATTEMPT_LEDGER_")
    if not _generation.archived and value["partition_cost_precommitments_sha256"] != policy["partition_cost_precommitments_sha256"]:
        raise _ContractError("ATTEMPT_LEDGER_PARTITION_COST_PRECOMMITMENTS_MISMATCH")
    for field in (
        "source_contract_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    ):
        if value[field] != policy[field]:
            raise _ContractError("ATTEMPT_LEDGER_" + field.upper() + "_MISMATCH")
    if (
        type(value["attempt_count"]) is not int
        or value["attempt_count"] != planned_attempts
    ):
        raise _ContractError("ATTEMPT_LEDGER_COUNT_MISMATCH")
    attempts = value["attempts"]
    if type(attempts) is not list or len(attempts) != planned_attempts:
        raise _ContractError("ATTEMPT_LEDGER_INCOMPLETE")
    if value["attempts_sha256"] != _sha256_json(attempts):
        raise _ContractError("ATTEMPT_LEDGER_HASH_MISMATCH")
    seen_attempts: set[str] = set()
    seen_candidates: set[str] = set()
    for ordinal, raw in enumerate(attempts):
        prefix = f"ATTEMPT_{ordinal:04d}_"
        attempt = _exact_dict(raw, _generation_keys(_ATTEMPT_KEYS, _generation), prefix + "SCHEMA_INVALID")
        if type(attempt["ordinal"]) is not int or attempt["ordinal"] != ordinal:
            raise _ContractError(prefix + "ORDINAL_INVALID")
        attempt_id = _hex(attempt["attempt_id"], _HEX40, prefix + "ID_INVALID")
        candidate_id = _hex(
            attempt["process_candidate_id"], _HEX40, prefix + "CANDIDATE_INVALID"
        )
        if attempt_id in seen_attempts or candidate_id in seen_candidates:
            raise _ContractError(prefix + "DUPLICATE_IDENTITY")
        seen_attempts.add(attempt_id)
        seen_candidates.add(candidate_id)
        _safe_id(attempt["strategy_family"], prefix + "STRATEGY_FAMILY_INVALID")
        _safe_id(attempt["strategy_id"], prefix + "STRATEGY_ID_INVALID")
        _exact_int(
            attempt["parameter_count"], 0, 100_000, prefix + "PARAMETER_COUNT_INVALID"
        )
        if attempt["precommitted_evidence_role"] not in {
            "EVALUATION_REQUIRED",
            "PRESEARCH_PROCESS_REJECTED",
        }:
            raise _ContractError(prefix + "PRECOMMITTED_EVIDENCE_ROLE_INVALID")
        for field in (
            "process_sha256",
            "horizon_process_sha256",
            "trial_definition_sha256",
            "refit_recipe_sha256",
            "source_contract_sha256",
            "cost_contract_sha256",
            _generation.pair_field,
            "risk_unit_contract_sha256",
        ):
            _hex(attempt[field], _HEX64, prefix + field.upper() + "_INVALID")
        for field in (
            "source_contract_sha256",
            "cost_contract_sha256",
            _generation.pair_field,
            "risk_unit_contract_sha256",
        ):
            if attempt[field] != policy[field]:
                raise _ContractError(prefix + field.upper() + "_MISMATCH")
    if _process_registry(attempts) != policy["process_registry_sha256"]:
        raise _ContractError("PROCESS_REGISTRY_RECOMPUTATION_MISMATCH")
    if _horizon_registry(attempts) != policy["horizon_registry_sha256"]:
        raise _ContractError("HORIZON_REGISTRY_RECOMPUTATION_MISMATCH")
    if _trial_registry(attempts) != policy["trial_registry_sha256"]:
        raise _ContractError("TRIAL_REGISTRY_RECOMPUTATION_MISMATCH")
    return frozen_at, attempts


def _same_attempt_identity(
    entry: dict[str, Any],
    attempt: dict[str, Any],
    prefix: str,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> None:
    for field in (
        "ordinal",
        "attempt_id",
        "process_candidate_id",
        "process_sha256",
        "horizon_process_sha256",
        "trial_definition_sha256",
        "refit_recipe_sha256",
        "source_contract_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
    ):
        if (
            type(entry[field]) is not type(attempt[field])
            or entry[field] != attempt[field]
        ):
            raise _ContractError(prefix + field.upper() + "_MISMATCH")


def _validate_shared_oos_partition(
    raw: Any,
    *,
    fold_plan_row: dict[str, Any],
    policy: dict[str, Any],
    prefix: str,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    value = _exact_dict(
        raw,
        _generation_keys(_SHARED_OOS_PARTITION_KEYS, _generation),
        prefix + "SHARED_OOS_PARTITION_SCHEMA_INVALID",
    )
    body = {key: value[key] for key in _generation_keys(_SHARED_OOS_PARTITION_KEYS, _generation) - {"receipt_sha256"}}
    if value["schema_version"] != _generation.shared_partition_version:
        raise _ContractError(prefix + "SHARED_OOS_PARTITION_VERSION_INVALID")
    for field in (
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_contract_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
    ):
        if value[field] != policy[field]:
            raise _ContractError(
                prefix + "SHARED_OOS_PARTITION_" + field.upper() + "_MISMATCH"
            )
    if value["causal_fold_plan_row_sha256"] != _sha256_json(fold_plan_row):
        raise _ContractError(
            prefix + "SHARED_OOS_PARTITION_CAUSAL_FOLD_PLAN_ROW_MISMATCH"
        )
    for field in (
        "causal_fold_plan_row_sha256",
        "native_prefix_index_sha256",
        "native_prefix_content_sha256",
        "source_contract_sha256",
        "native_source_sha256",
        "activation_manifest_sha256",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "execution_spec_receipt_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
        "receipt_sha256",
    ):
        _hex(value[field], _HEX64, prefix + field.upper() + "_INVALID")
    if value["receipt_sha256"] != _sha256_json(body):
        raise _ContractError(prefix + "SHARED_OOS_PARTITION_HASH_MISMATCH")
    return value


def _validate_shared_fold_axis(
    raw_axis: Any,
    *,
    declared_count: Any,
    declared_sha256: Any,
    fold_plan: list[dict[str, Any]],
    policy: dict[str, Any],
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> list[dict[str, Any]]:
    if type(raw_axis) is not list or len(raw_axis) != len(fold_plan):
        raise _ContractError("SHARED_FOLD_AXIS_ROSTER_INCOMPLETE")
    if (
        type(declared_count) is not int
        or declared_count != len(raw_axis)
        or declared_sha256 != _sha256_json(raw_axis)
    ):
        raise _ContractError("SHARED_FOLD_AXIS_BINDING_INVALID")
    validated: list[dict[str, Any]] = []
    seen_partition_receipts: set[str] = set()
    for ordinal, (raw, planned) in enumerate(zip(raw_axis, fold_plan, strict=True)):
        prefix = f"SHARED_FOLD_AXIS_{ordinal:03d}_"
        axis = _exact_dict(raw, _SHARED_FOLD_AXIS_KEYS, prefix + "SCHEMA_INVALID")
        for field in _FOLD_PLAN_KEYS:
            expected = ordinal if field == "fold_ordinal" else planned[field]
            if type(axis[field]) is not type(expected) or axis[field] != expected:
                raise _ContractError(prefix + "PRECOMMITTED_AXIS_MISMATCH")
        start = _day(axis["oos_start_date_utc"], prefix + "START_DATE_INVALID")
        end = _day(axis["oos_end_date_utc"], prefix + "END_DATE_INVALID")
        dates = axis["daily_dates_utc"]
        if (
            type(dates) is not list
            or len(dates) != (end - start).days + 1
            or not int(_generation.policy["min_days_per_fold"])
            <= len(dates)
            <= MAX_DAYS_PER_FOLD
        ):
            raise _ContractError(prefix + "DAILY_AXIS_NOT_COMPLETE")
        for index, raw_day in enumerate(dates):
            if _day(raw_day, prefix + "DAILY_DATE_INVALID") != start + timedelta(
                days=index
            ):
                raise _ContractError(prefix + "DAILY_AXIS_NOT_CONSECUTIVE")
        partition = _validate_shared_oos_partition(
            axis["shared_oos_partition"],
            fold_plan_row=planned,
            policy=policy,
            prefix=prefix,
            _generation=_generation,
        )
        receipt = partition["receipt_sha256"]
        if receipt in seen_partition_receipts:
            raise _ContractError("SHARED_OOS_PARTITION_REPLAYED_ACROSS_FOLDS")
        seen_partition_receipts.add(receipt)
        validated.append(axis)
    return validated


def _validate_trade_power_binding(
    raw: Any,
    *,
    prefix: str,
    attempt_payload_sha: str,
    attempted_trials: int,
) -> dict[str, Any]:
    binding = _exact_dict(
        raw,
        _TRADE_POWER_BINDING_KEYS,
        prefix + "TRADE_POWER_BINDING_SCHEMA_INVALID",
    )
    binding_body = {
        key: binding[key] for key in _TRADE_POWER_BINDING_KEYS - {"binding_sha256"}
    }
    for field in (
        "attempt_ledger_payload_sha256",
        "train_requirement_sha256",
        "train_calibration_sample_sha256",
        "train_partition_contract_sha256",
        "validation_decision_sha256",
        "validation_partition_contract_sha256",
        "binding_sha256",
    ):
        _hex(binding[field], _HEX64, prefix + field.upper() + "_INVALID")
    if (
        binding["schema_version"] != TRADE_POWER_BINDING_VERSION
        or binding["attempt_ledger_payload_sha256"] != attempt_payload_sha
        or type(binding["attempted_trials_exact"]) is not int
        or binding["attempted_trials_exact"] != attempted_trials
        or type(binding["required_effective_trades"]) is not int
        or not 2
        <= binding["required_effective_trades"]
        <= MAX_TOTAL_COMPLETED_TRADES
        or binding["binding_sha256"] != _sha256_json(binding_body)
    ):
        raise _ContractError(prefix + "TRADE_POWER_BINDING_INVALID")
    return binding


def _validate_archived_fold(
    raw: Any,
    *,
    ordinal: int,
    shared_axis: dict[str, Any],
    policy: dict[str, Any],
    attempt_payload_sha: str,
    attempted_trials: int,
    authenticated: bool,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    prefix = f"FOLD_{ordinal:03d}_"
    fold = _exact_dict(raw, _FOLD_KEYS, prefix + "SCHEMA_INVALID")
    if type(fold["fold_ordinal"]) is not int or fold["fold_ordinal"] != ordinal:
        raise _ContractError(prefix + "ORDINAL_INVALID")
    for field in (
        "anchor_selection_receipt_sha256",
        "anchor_horizon_decision_sha256",
        "anchor_trial_ledger_sha256",
        "anchor_selected_trial_sha256",
        "anchor_refit_receipt_sha256",
        "attempt_replay_receipt_sha256",
        "attempt_oos_source_receipt_sha256",
        "position_activity_ledger_sha256",
        "daily_ladder_sha256",
        "trade_vector_sha256",
    ):
        _hex(fold[field], _HEX64, prefix + field.upper() + "_INVALID")

    axis_sha256 = _sha256_json(shared_axis)
    dates = shared_axis["daily_dates_utc"]
    start = _day(shared_axis["oos_start_date_utc"], prefix + "START_DATE_INVALID")
    end = _day(shared_axis["oos_end_date_utc"], prefix + "END_DATE_INVALID")
    start_utc = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    end_exclusive_utc = datetime.combine(
        end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )

    lane_daily_e12 = {
        lane: _decode_r_e12_i64be_base64(
            fold[field],
            expected_count=len(dates),
            maximum_abs_r=MAX_ABS_DAILY_R,
            code=prefix + lane + "_DAILY_R_I64BE_BASE64_INVALID",
        )
        for lane, field in _SERIALIZED_DAILY_LANE_FIELDS.items()
    }
    lane_daily: dict[str, list[float]] = {lane: [] for lane in _generation.lane_fields}
    cumulative = {lane: 0 for lane in _generation.lane_fields}
    for index, values in enumerate(
        zip(*(lane_daily_e12[lane] for lane in _generation.lane_fields), strict=True)
    ):
        for lane, raw_value in zip(_generation.lane_fields, values, strict=True):
            decoded = _decode_r_e12(
                raw_value,
                MAX_ABS_DAILY_R,
                prefix + lane + "_R_E12_INVALID",
            )
            lane_daily[lane].append(decoded)
            cumulative[lane] += int(raw_value)
        if cumulative["P75"] > cumulative["BASE"] + 1:
            raise _ContractError(
                prefix + f"P75_CUMULATIVE_PREFIX_IMPROVES_RETURN_{index:04d}"
            )
        if cumulative["P95"] > cumulative["P75"] + 1:
            raise _ContractError(
                prefix + f"P95_CUMULATIVE_PREFIX_IMPROVES_RETURN_{index:04d}"
            )
    expected_daily_sha = _sha256_json(
        {
            "shared_fold_axis_row_sha256": axis_sha256,
            **{
                field: fold[field]
                for field in _SERIALIZED_DAILY_LANE_FIELDS.values()
            },
        }
    )
    if fold["daily_ladder_sha256"] != expected_daily_sha:
        raise _ContractError(prefix + "DAILY_LADDER_HASH_MISMATCH")

    identity_fields = (
        "completed_trade_open_offset_seconds",
        "completed_trade_close_offset_seconds",
        "completed_trade_position_id_digests_base64",
        "completed_trade_event_order_anchor",
        "completed_trade_open_event_order_offsets",
        "completed_trade_close_event_order_offsets",
    )
    trade_list_fields = (
        "completed_trade_open_offset_seconds",
        "completed_trade_close_offset_seconds",
        "completed_trade_open_event_order_offsets",
        "completed_trade_close_event_order_offsets",
    )
    vectors = [fold[field] for field in trade_list_fields]
    if any(type(values) is not list for values in vectors):
        raise _ContractError(prefix + "TRADE_VECTOR_LENGTH_INVALID")
    trade_count = len(vectors[0])
    if any(len(values) != trade_count for values in vectors):
        raise _ContractError(prefix + "TRADE_VECTOR_LENGTH_INVALID")
    minimum = (
        int(_generation.policy["min_completed_trades_per_fold"])
        if authenticated
        else 0
    )
    if not minimum <= trade_count <= MAX_TRADES_PER_FOLD:
        raise _ContractError(prefix + "TRADE_COUNT_INVALID")

    trade_open_times = _decode_utc_second_offsets(
        fold["completed_trade_open_offset_seconds"],
        fold_start_utc=start_utc,
        fold_end_exclusive_utc=end_exclusive_utc,
        code=prefix + "TRADE_OPEN_OFFSET_INVALID",
    )
    trade_close_times = _decode_utc_second_offsets(
        fold["completed_trade_close_offset_seconds"],
        fold_start_utc=start_utc,
        fold_end_exclusive_utc=end_exclusive_utc,
        code=prefix + "TRADE_CLOSE_OFFSET_INVALID",
    )
    position_ids = _decode_position_id_digests(
        fold["completed_trade_position_id_digests_base64"],
        trade_count,
        prefix + "POSITION_ID_DIGESTS_INVALID",
    )
    if len(set(position_ids)) != trade_count:
        raise _ContractError(prefix + "POSITION_ID_DUPLICATED")
    open_orders, close_orders = _decode_event_order_offsets(
        fold["completed_trade_event_order_anchor"],
        fold["completed_trade_open_event_order_offsets"],
        fold["completed_trade_close_event_order_offsets"],
        prefix + "EVENT_ORDER_OFFSETS_INVALID",
    )
    lane_trades_e12 = {
        lane: _decode_r_e12_i64be_base64(
            fold[field],
            expected_count=trade_count,
            maximum_abs_r=MAX_ABS_TRADE_R,
            code=prefix + lane + "_TRADE_R_I64BE_BASE64_INVALID",
        )
        for lane, field in _SERIALIZED_TRADE_LANE_FIELDS.items()
    }
    lane_trades = {
        lane: [
            _decode_r_e12(
                value,
                MAX_ABS_TRADE_R,
                prefix + lane + "_TRADE_R_E12_INVALID",
            )
            for value in lane_trades_e12[lane]
        ]
        for lane in _generation.trade_lane_fields
    }
    prior_order: tuple[datetime, int, datetime, int, str] | None = None
    for values in zip(
        position_ids,
        trade_open_times,
        trade_close_times,
        open_orders,
        close_orders,
        lane_trades["BASE"],
        lane_trades["P75"],
        lane_trades["P95"],
        strict=True,
    ):
        (
            position_id,
            open_time,
            close_time,
            open_event_order,
            close_event_order,
            base_value,
            p75_value,
            p95_value,
        ) = values
        opened = _timestamp(open_time, prefix + "TRADE_OPEN_TIME_INVALID")
        closed = _timestamp(close_time, prefix + "TRADE_CLOSE_TIME_INVALID")
        if _BACKTEST_POSITION_ID.fullmatch(position_id) is None:
            raise _ContractError(prefix + "POSITION_ID_INVALID")
        open_order = _exact_int(
            open_event_order, 0, _MAX_EVENT_ORDER, prefix + "OPEN_EVENT_ORDER_INVALID"
        )
        close_order = _exact_int(
            close_event_order, 0, _MAX_EVENT_ORDER, prefix + "CLOSE_EVENT_ORDER_INVALID"
        )
        order_key = (closed, close_order, opened, open_order, position_id)
        if (
            opened < start_utc
            or (opened, open_order) >= (closed, close_order)
            or closed >= end_exclusive_utc
            or (prior_order is not None and order_key <= prior_order)
        ):
            raise _ContractError(prefix + "TRADE_TIME_ORDER_INVALID")
        prior_order = order_key
        base_number = _encode_r_e12(
            base_value, MAX_ABS_TRADE_R, prefix + "BASE_TRADE_R_INVALID"
        )
        p75_number = _encode_r_e12(
            p75_value, MAX_ABS_TRADE_R, prefix + "P75_TRADE_R_INVALID"
        )
        p95_number = _encode_r_e12(
            p95_value, MAX_ABS_TRADE_R, prefix + "P95_TRADE_R_INVALID"
        )
        if p75_number > base_number + 1:
            raise _ContractError(prefix + "P75_IMPROVES_TRADE_RETURN")
        if p95_number > p75_number + 1:
            raise _ContractError(prefix + "P95_IMPROVES_TRADE_RETURN")

    binding = fold["trade_power_binding"]
    if authenticated:
        binding = _validate_trade_power_binding(
            binding,
            prefix=prefix,
            attempt_payload_sha=attempt_payload_sha,
            attempted_trials=attempted_trials,
        )
    elif binding is not None:
        raise _ContractError(prefix + "REJECTED_TRADE_POWER_BINDING_MUST_BE_NULL")

    expected_trade_sha = _sha256_json(
        {
            "shared_fold_axis_row_sha256": axis_sha256,
            "accounting_identity_kind": _generation.policy["accounting_identity"],
            "completed_trade_unit": _generation.policy["daily_unit"],
            "inventory_boundary_kind": _generation.policy["inventory_boundary"],
            **{field: fold[field] for field in identity_fields},
            **{
                field: fold[field]
                for field in _SERIALIZED_TRADE_LANE_FIELDS.values()
            },
            "position_activity_ledger_sha256": fold[
                "position_activity_ledger_sha256"
            ],
            "risk_unit_contract_sha256": policy["risk_unit_contract_sha256"],
            "trade_power_binding": binding,
        }
    )
    if fold["trade_vector_sha256"] != expected_trade_sha:
        raise _ContractError(prefix + "TRADE_VECTOR_HASH_MISMATCH")

    for lane in _generation.lane_fields:
        if sum(lane_daily_e12[lane]) != sum(lane_trades_e12[lane]):
            raise _ContractError(
                prefix + lane + "_FLAT_TO_FLAT_ACCOUNTING_MISMATCH"
            )

    compact_fields = frozenset(
        (*_SERIALIZED_DAILY_LANE_FIELDS.values(), *identity_fields, *_SERIALIZED_TRADE_LANE_FIELDS.values())
    )
    semantic_fold = {
        field: value for field, value in fold.items() if field not in compact_fields
    }
    semantic_fold.update(
        {
            **{
                field: lane_daily[lane] for lane, field in _generation.lane_fields.items()
            },
            "completed_trade_open_utc": trade_open_times,
            "completed_trade_close_utc": trade_close_times,
            "completed_trade_position_ids": position_ids,
            "completed_trade_open_event_order": open_orders,
            "completed_trade_close_event_order": close_orders,
            **{
                field: lane_trades[lane]
                for lane, field in _generation.trade_lane_fields.items()
            },
        }
    )
    return semantic_fold


_TIER_IDENTITY_FIELDS: Final = (
    "completed_trade_open_offset_seconds", "completed_trade_close_offset_seconds",
    "completed_trade_position_id_digests_base64", "completed_trade_event_order_anchor",
    "completed_trade_open_event_order_offsets", "completed_trade_close_event_order_offsets",
)
_TIER_LINEAGE_FIELDS: Final = (
    "spread_tier", "source_contract_sha256", "cost_contract_sha256",
    "entry_spread_admission_policy_sha256",
)
_CURRENT_TIER_FOLD_KEYS: Final = frozenset({
    *_TIER_IDENTITY_FIELDS, *_TIER_LINEAGE_FIELDS,
    "position_activity_ledger_sha256", "daily_net_mtm_r_e12_i64be_base64",
    "daily_surface_sha256", "completed_trade_net_mtm_r_e12_i64be_base64",
    "trade_power_binding", "trade_vector_sha256",
})
_COMMON_FOLD_KEYS: Final = _FOLD_KEYS - {
    *_TIER_IDENTITY_FIELDS, *_SERIALIZED_DAILY_LANE_FIELDS.values(),
    *_SERIALIZED_TRADE_LANE_FIELDS.values(), "position_activity_ledger_sha256",
    "daily_ladder_sha256", "trade_power_binding", "trade_vector_sha256",
}


def _validate_current_fold(
    raw: Any,
    *,
    ordinal: int,
    shared_axis: dict[str, Any],
    policy: dict[str, Any],
    attempt_payload_sha: str,
    attempted_trials: int,
    authenticated: bool,
    _generation: _ContractGeneration = CURRENT_GENERATION,
) -> dict[str, Any]:
    prefix = f"FOLD_{ordinal:03d}_"
    fold = _exact_dict(
        raw, _COMMON_FOLD_KEYS | {"tier_evidence"}, prefix + "SCHEMA_INVALID"
    )
    if type(fold["fold_ordinal"]) is not int or fold["fold_ordinal"] != ordinal:
        raise _ContractError(prefix + "ORDINAL_INVALID")
    for field in _COMMON_FOLD_KEYS - {"fold_ordinal"}:
        _hex(fold[field], _HEX64, prefix + field.upper() + "_INVALID")
    tiers = _exact_dict(
        fold["tier_evidence"], frozenset({"P75", "P95"}), prefix + "TIER_ROSTER_INVALID"
    )
    start = datetime.combine(
        _day(shared_axis["oos_start_date_utc"], prefix + "START_INVALID"),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    end = datetime.combine(
        _day(shared_axis["oos_end_date_utc"], prefix + "END_INVALID")
        + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    axis_sha = _sha256_json(shared_axis)
    result = {key: fold[key] for key in _COMMON_FOLD_KEYS}
    result["tier_evidence"] = {}
    for tier in ("P75", "P95"):
        code = prefix + tier + "_"
        row = _exact_dict(tiers[tier], _CURRENT_TIER_FOLD_KEYS, code + "SCHEMA_INVALID")
        if row["spread_tier"] != tier:
            raise _ContractError(code + "TIER_MISMATCH")
        for field in (
            *_TIER_LINEAGE_FIELDS[1:],
            "position_activity_ledger_sha256",
            "daily_surface_sha256",
            "trade_vector_sha256",
        ):
            _hex(row[field], _HEX64, code + field.upper() + "_INVALID")
        pair = policy["frozen_tier_pair_contract"]
        tier_contract = next(item for item in pair["tiers"] if item["tier"] == tier)
        if row["cost_contract_sha256"] != _sha256_json(
            tier_contract["numerical_execution_cost_policy"]
        ):
            raise _ContractError(code + "COST_CONTRACT_MISMATCH")
        expected_admission_sha = _sha256_json(
            {
                "schema_version": pair["admission_schema"],
                "entry_admission_policy": pair["admission_policy"],
            }
        )
        if row["entry_spread_admission_policy_sha256"] != expected_admission_sha:
            raise _ContractError(code + "ADMISSION_POLICY_MISMATCH")
        daily = _decode_r_e12_i64be_base64(
            row["daily_net_mtm_r_e12_i64be_base64"],
            expected_count=len(shared_axis["daily_dates_utc"]),
            maximum_abs_r=MAX_ABS_DAILY_R,
            code=code + "DAILY_R_INVALID",
        )
        daily_body = {
            "shared_fold_axis_row_sha256": axis_sha,
            **{field: row[field] for field in _TIER_LINEAGE_FIELDS},
            "daily_net_mtm_r_e12_i64be_base64": row["daily_net_mtm_r_e12_i64be_base64"],
        }
        if row["daily_surface_sha256"] != _sha256_json(daily_body):
            raise _ContractError(code + "DAILY_SURFACE_HASH_MISMATCH")
        vectors = [
            row[field]
            for field in (
                _TIER_IDENTITY_FIELDS[0],
                _TIER_IDENTITY_FIELDS[1],
                _TIER_IDENTITY_FIELDS[4],
                _TIER_IDENTITY_FIELDS[5],
            )
        ]
        if any(type(vector) is not list for vector in vectors):
            raise _ContractError(code + "TRADE_VECTOR_INVALID")
        count = len(vectors[0])
        if count > MAX_TRADES_PER_FOLD or any(
            len(vector) != count for vector in vectors
        ):
            raise _ContractError(code + "TRADE_VECTOR_LENGTH_INVALID")
        opens = _decode_utc_second_offsets(
            vectors[0],
            fold_start_utc=start,
            fold_end_exclusive_utc=end,
            code=code + "TRADE_OPEN_OFFSET_INVALID",
        )
        closes = _decode_utc_second_offsets(
            vectors[1],
            fold_start_utc=start,
            fold_end_exclusive_utc=end,
            code=code + "TRADE_CLOSE_OFFSET_INVALID",
        )
        positions = _decode_position_id_digests(
            row["completed_trade_position_id_digests_base64"],
            count,
            code + "POSITION_IDS_INVALID",
        )
        if len(set(positions)) != count:
            raise _ContractError(code + "POSITION_ID_DUPLICATED")
        open_orders, close_orders = _decode_event_order_offsets(
            row["completed_trade_event_order_anchor"],
            vectors[2],
            vectors[3],
            code + "EVENT_ORDER_INVALID",
        )
        previous = None
        for opened, closed, open_order, close_order, position in zip(
            opens, closes, open_orders, close_orders, positions, strict=True
        ):
            opening = _timestamp(opened, code + "TRADE_OPEN_INVALID")
            closing = _timestamp(closed, code + "TRADE_CLOSE_INVALID")
            key = (closing, close_order, opening, open_order, position)
            if (
                opening < start
                or (opening, open_order) >= (closing, close_order)
                or closing >= end
                or (previous is not None and key <= previous)
            ):
                raise _ContractError(code + "TRADE_TIME_ORDER_INVALID")
            previous = key
        trades = _decode_r_e12_i64be_base64(
            row["completed_trade_net_mtm_r_e12_i64be_base64"],
            expected_count=count,
            maximum_abs_r=MAX_ABS_TRADE_R,
            code=code + "TRADE_R_INVALID",
        )
        binding = row["trade_power_binding"]
        if binding is not None:
            if not authenticated:
                raise _ContractError(code + "REJECTED_TRADE_POWER_BINDING_MUST_BE_NULL")
            _validate_trade_power_binding(
                binding,
                prefix=code,
                attempt_payload_sha=attempt_payload_sha,
                attempted_trials=attempted_trials,
            )
        trade_body = {
            "shared_fold_axis_row_sha256": axis_sha,
            "accounting_identity_kind": _generation.policy["accounting_identity"],
            "completed_trade_unit": _generation.policy["daily_unit"],
            "inventory_boundary_kind": _generation.policy["inventory_boundary"],
            **{
                field: row[field]
                for field in (*_TIER_LINEAGE_FIELDS, *_TIER_IDENTITY_FIELDS)
            },
            "completed_trade_net_mtm_r_e12_i64be_base64": row[
                "completed_trade_net_mtm_r_e12_i64be_base64"
            ],
            "position_activity_ledger_sha256": row["position_activity_ledger_sha256"],
            "risk_unit_contract_sha256": policy["risk_unit_contract_sha256"],
            "trade_power_binding": binding,
        }
        if row["trade_vector_sha256"] != _sha256_json(trade_body):
            raise _ContractError(code + "TRADE_VECTOR_HASH_MISMATCH")
        if sum(daily) != sum(trades):
            raise _ContractError(code + "FLAT_TO_FLAT_ACCOUNTING_MISMATCH")
        result["tier_evidence"][tier] = {
            **row,
            "daily_net_mtm_r": [value / 10**12 for value in daily],
            "completed_trade_net_mtm_r": [value / 10**12 for value in trades],
            "completed_trade_open_utc": opens,
            "completed_trade_close_utc": closes,
            "completed_trade_position_ids": positions,
            "completed_trade_open_event_order": open_orders,
            "completed_trade_close_event_order": close_orders,
        }
        result[CURRENT_GENERATION.lane_fields[tier]] = result["tier_evidence"][tier][
            "daily_net_mtm_r"
        ]
    for field in ("source_contract_sha256", "cost_contract_sha256"):
        aggregate = _sha256_json({tier: tiers[tier][field] for tier in ("P75", "P95")})
        if aggregate != policy[field]:
            raise _ContractError(prefix + field.upper() + "_PAIR_MISMATCH")
    return result


def _validate_fold(raw: Any, **kwargs: Any) -> dict[str, Any]:
    generation = _generation_for_precommitment(kwargs["policy"])
    if generation.archived:
        return _validate_archived_fold(raw, _generation=generation, **kwargs)
    return _validate_current_fold(raw, **kwargs)


def _folds_for_lane(
    folds: list[dict[str, Any]], lane: str, generation: _ContractGeneration
) -> list[dict[str, Any]]:
    return (
        folds
        if generation.archived
        else [fold["tier_evidence"][lane] for fold in folds]
    )


def _maximum_supported_block_size(lengths: list[int], minimum_blocks: int) -> int:
    maximum = 0
    for block_size in range(1, max(lengths, default=0) + 1):
        if sum(length // block_size for length in lengths) < minimum_blocks:
            break
        maximum = block_size
    return maximum


def _dependence_bandwidth_plan(
    lengths: list[int], holding_days: list[int] | None = None
) -> tuple[list[int], list[int]]:
    """Return the two frozen HAC-memory plans without inspecting outcomes."""

    if not lengths or any(type(length) is not int or length <= 0 for length in lengths):
        return [], []
    holdings = [1 for _ in lengths] if holding_days is None else holding_days
    if len(holdings) != len(lengths) or any(
        type(holding) is not int or holding <= 0 for holding in holdings
    ):
        raise _ContractError("DEPENDENCE_HOLDING_ROSTER_INVALID")
    primary = [
        min(
            length - 1,
            max(_sample_size_hac_bandwidth(length), holding - 1),
        )
        for length, holding in zip(lengths, holdings, strict=True)
    ]
    sensitivity = [
        min(length - 1, max(bandwidth + 1, 2 * (bandwidth + 1) - 1))
        for length, bandwidth in zip(lengths, primary, strict=True)
    ]
    return primary, sensitivity


def _precommitted_block_sizes(
    primary_bandwidths: list[int], sensitivity_bandwidths: list[int]
) -> tuple[int, int]:
    if not primary_bandwidths or len(primary_bandwidths) != len(sensitivity_bandwidths):
        return 0, 0
    return (
        max(bandwidth + 1 for bandwidth in primary_bandwidths),
        max(bandwidth + 1 for bandwidth in sensitivity_bandwidths),
    )


def _dependence_operation_count(
    lengths: list[int],
    holding_days: list[int] | None = None,
) -> int:
    """Outcome-independent upper bound for two scales and three alignments."""

    if not lengths or any(type(length) is not int or length <= 0 for length in lengths):
        return 0
    # Validate the holding roster even though the bound itself is independent
    # of the eventual outcome-derived autocorrelation memory. Every streamed
    # scale/alignment emits no more than ``length`` block means. The second
    # term exactly bounds every covariance product the quarter-fold IPS
    # detector could inspect before its data-dependent early stop.
    _dependence_bandwidth_plan(lengths, holding_days)
    autocorrelation_products = 0
    for length in lengths:
        maximum_lag = length // 4
        paired_maximum_lag = maximum_lag - (maximum_lag % 2)
        autocorrelation_products += (
            paired_maximum_lag * length
            - paired_maximum_lag * (paired_maximum_lag + 1) // 2
        )
    return 6 * sum(lengths) + autocorrelation_products


def _block_operation_count(lengths: list[int], block_sizes: tuple[int, int]) -> int:
    """Count the exact fixed-scale/alignment block means before allocating them."""

    operations = 0
    for block_size in sorted(set(block_sizes)):
        if block_size <= 0:
            continue
        for length in lengths:
            for offset in (0, block_size // 2, length % block_size):
                if length - offset >= block_size:
                    operations += (length - offset) // block_size
    return operations


def _validate_evidence_ledger(
    payload: dict[str, Any],
    policy: dict[str, Any],
    policy_sha: str,
    attempt_payload_sha: str,
    frozen_at: datetime,
    cell: tuple[str, str, str, str],
    attempts: list[dict[str, Any]],
    fold_plan: list[dict[str, Any]],
    latest_evidence_boundary: datetime,
    verification_time: datetime,
    request_sha: str,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[datetime, list[dict[str, Any]], list[dict[str, Any]]]:
    value = _exact_dict(
        payload, _generation_keys(_EVIDENCE_LEDGER_KEYS, _generation), "EVIDENCE_LEDGER_SCHEMA_INVALID"
    )
    if value["contract_version"] != _generation.evidence_version:
        raise _ContractError("EVIDENCE_LEDGER_VERSION_INVALID")
    if value["run_id"] != policy["run_id"]:
        raise _ContractError("EVIDENCE_RUN_MISMATCH")
    generated_at = _timestamp(value["generated_at_utc"], "EVIDENCE_TIME_INVALID")
    access_started = _timestamp(
        value["evidence_access_started_at_utc"], "EVIDENCE_ACCESS_START_INVALID"
    )
    access_completed = _timestamp(
        value["evidence_access_completed_at_utc"], "EVIDENCE_ACCESS_END_INVALID"
    )
    declared_boundary = _timestamp(
        value["latest_evidence_boundary_utc"], "EVIDENCE_BOUNDARY_INVALID"
    )
    if declared_boundary != latest_evidence_boundary:
        raise _ContractError("EVIDENCE_BOUNDARY_PRECOMMITMENT_MISMATCH")
    if access_started <= frozen_at:
        raise _ContractError("EVIDENCE_ACCESS_NOT_AFTER_ATTEMPT_FREEZE")
    if access_started < latest_evidence_boundary:
        raise _ContractError("EVIDENCE_ACCESS_PRECEDES_LATEST_MARKET_BOUNDARY")
    if access_completed < access_started:
        raise _ContractError("EVIDENCE_ACCESS_CLOCK_ORDER_INVALID")
    if generated_at != access_completed:
        raise _ContractError("EVIDENCE_FREEZE_NOT_EXACT_ACCESS_COMPLETION")
    maximum_trusted_time = verification_time + timedelta(
        seconds=int(_generation.policy["trusted_clock_max_future_skew_seconds"])
    )
    if generated_at > maximum_trusted_time:
        raise _ContractError("EVIDENCE_TRUSTED_CLOCK_IN_FUTURE")
    if value["precommitment_payload_sha256"] != policy_sha:
        raise _ContractError("EVIDENCE_PRECOMMITMENT_BINDING_INVALID")
    if value["attempt_ledger_payload_sha256"] != attempt_payload_sha:
        raise _ContractError("EVIDENCE_ATTEMPT_BINDING_INVALID")
    _require_same_cell(value, cell, "EVIDENCE_")
    if not _generation.archived and value["partition_cost_precommitments_sha256"] != policy["partition_cost_precommitments_sha256"]:
        raise _ContractError("EVIDENCE_PARTITION_COST_PRECOMMITMENTS_MISMATCH")
    for field in (
        "source_contract_sha256",
        "cost_contract_sha256",
        _generation.pair_field,
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    ):
        if value[field] != policy[field]:
            raise _ContractError("EVIDENCE_" + field.upper() + "_MISMATCH")
    if (
        value["evidence_scope"] != "PROCESS_LEVEL_ONLY"
        or value["parameter_evidence_inherited"] is not False
    ):
        raise _ContractError("EVIDENCE_SCOPE_OR_PARAMETER_INHERITANCE_INVALID")

    shared_axis = _validate_shared_fold_axis(
        value["shared_fold_axis"],
        declared_count=value["shared_fold_axis_count"],
        declared_sha256=value["shared_fold_axis_sha256"],
        fold_plan=fold_plan,
        policy=policy,
        _generation=_generation,
    )
    if type(value["attempt_count"]) is not int or value["attempt_count"] != len(
        attempts
    ):
        raise _ContractError("EVIDENCE_ATTEMPT_COUNT_MISMATCH")
    entries = value["entries"]
    if type(entries) is not list or len(entries) != len(attempts):
        raise _ContractError("EVIDENCE_ATTEMPT_ROSTER_INCOMPLETE")
    if value["entries_sha256"] != _sha256_json(entries):
        raise _ContractError("EVIDENCE_ENTRIES_HASH_MISMATCH")

    evidence_ledger_sha = _sha256_json(value)
    root_identities = (
        policy_sha,
        attempt_payload_sha,
        evidence_ledger_sha,
        request_sha,
    )
    if len(set(root_identities)) != len(root_identities):
        raise _ContractError("ROOT_BINDINGS_REUSED_ACROSS_EVIDENCE_DOMAINS")
    evidence_identity_domains: dict[str, str] = {
        policy_sha: "PRECOMMITMENT_PAYLOAD",
        attempt_payload_sha: "ATTEMPT_LEDGER_PAYLOAD",
        evidence_ledger_sha: "EVIDENCE_LEDGER_PAYLOAD",
        request_sha: "WINNER_SELECTION_REQUEST",
    }
    for fold_ordinal, axis in enumerate(shared_axis):
        identity = axis["shared_oos_partition"]["receipt_sha256"]
        if identity in evidence_identity_domains:
            raise _ContractError("SHARED_OOS_PARTITION_REPLAYED_ACROSS_DOMAINS")
        evidence_identity_domains[identity] = (
            f"SHARED_OOS_PARTITION_FOLD_{fold_ordinal:03d}"
        )

    total_days = 0
    total_trades = 0
    dependence_operations = 0
    technical_failure = False
    for ordinal, (raw, attempt) in enumerate(zip(entries, attempts, strict=True)):
        prefix = f"EVIDENCE_{ordinal:04d}_"
        entry = _exact_dict(raw, _generation_keys(_EVIDENCE_ENTRY_KEYS, _generation), prefix + "SCHEMA_INVALID")
        _same_attempt_identity(entry, attempt, prefix, _generation=_generation)
        status = entry["evidence_status"]
        if status not in {"AUTHENTICATED", "PROCESS_REJECTED", "TECHNICAL_FAILURE"}:
            raise _ContractError(prefix + "STATUS_INVALID")
        folds = entry["folds"]
        if type(folds) is not list or len(folds) > MAX_FOLDS:
            raise _ContractError(prefix + "FOLDS_INVALID")
        if type(entry["fold_count"]) is not int or entry["fold_count"] != len(folds):
            raise _ContractError(prefix + "FOLD_COUNT_MISMATCH")
        if entry["folds_sha256"] != _sha256_json(folds):
            raise _ContractError(prefix + "FOLDS_HASH_MISMATCH")

        role = attempt["precommitted_evidence_role"]
        if role == "PRESEARCH_PROCESS_REJECTED":
            if (
                status != "PROCESS_REJECTED"
                or entry["status_reason"] != "PRESEARCH_PROCESS_REJECTED"
                or folds
            ):
                raise _ContractError(prefix + "PRESEARCH_REJECTION_EVIDENCE_INVALID")
            continue
        if role != "EVALUATION_REQUIRED":
            raise _ContractError(prefix + "PRECOMMITTED_EVIDENCE_ROLE_INVALID")
        if status == "TECHNICAL_FAILURE":
            if folds or entry["status_reason"] not in {
                "EVALUATOR_TECHNICAL_FAILURE",
                "SOURCE_TECHNICAL_FAILURE",
            }:
                raise _ContractError(prefix + "TECHNICAL_FAILURE_EVIDENCE_INVALID")
            technical_failure = True
            continue
        if len(folds) != len(shared_axis):
            raise _ContractError(prefix + "PRECOMMITTED_FOLD_ROSTER_MISMATCH")
        if status == "AUTHENTICATED":
            if entry["status_reason"] is not None:
                raise _ContractError(prefix + "AUTHENTICATED_REASON_MUST_BE_NULL")
        elif entry["status_reason"] != "EVALUATED_OOS_PROCESS_REJECTED":
            raise _ContractError(prefix + "STATUS_REASON_INVALID")

        seen_positions: set[str] = set()
        seen_replay_axes: set[tuple[str, ...]] = set()
        validated: list[dict[str, Any]] = []
        for fold_ordinal, fold_raw in enumerate(folds):
            fold = _validate_fold(
                fold_raw,
                ordinal=fold_ordinal,
                shared_axis=shared_axis[fold_ordinal],
                policy=policy,
                attempt_payload_sha=attempt_payload_sha,
                attempted_trials=len(attempts),
                authenticated=status == "AUTHENTICATED",
            )
            positions = set(fold["completed_trade_position_ids"]) if _generation.archived else {
                tier + ":" + position
                for tier, account in fold["tier_evidence"].items()
                for position in account["completed_trade_position_ids"]
            }
            if seen_positions.intersection(positions):
                raise _ContractError(prefix + "POSITION_ID_REUSED_ACROSS_FOLDS")
            seen_positions.update(positions)
            for field in (
                "anchor_selection_receipt_sha256",
                "anchor_horizon_decision_sha256",
                "anchor_trial_ledger_sha256",
                "anchor_selected_trial_sha256",
                "anchor_refit_receipt_sha256",
                "attempt_replay_receipt_sha256",
                "attempt_oos_source_receipt_sha256",
            ):
                identity = fold[field]
                if identity in evidence_identity_domains:
                    raise _ContractError(
                        prefix + field.upper() + "_REPLAYED_ACROSS_EVIDENCE_DOMAINS"
                    )
                evidence_identity_domains[identity] = (
                    f"CANDIDATE_{ordinal:04d}_FOLD_{fold_ordinal:03d}_{field}"
                )
            accounts = {"ARCHIVED": fold} if _generation.archived else fold["tier_evidence"]
            for tier, account in accounts.items():
                activity = account["position_activity_ledger_sha256"]
                if activity in evidence_identity_domains:
                    raise _ContractError(prefix + "POSITION_ACTIVITY_LEDGER_SHA256_REPLAYED_ACROSS_EVIDENCE_DOMAINS")
                evidence_identity_domains[activity] = f"CANDIDATE_{ordinal:04d}_FOLD_{fold_ordinal:03d}_{tier}_ACTIVITY"
                replay_axis = (
                    shared_axis[fold_ordinal]["fold_id"], tier, activity,
                    fold["attempt_replay_receipt_sha256"],
                    account["daily_ladder_sha256" if _generation.archived else "daily_surface_sha256"],
                    account["trade_vector_sha256"],
                )
                if replay_axis in seen_replay_axes:
                    raise _ContractError(prefix + "FOLD_REPLAY_AXIS_DUPLICATED")
                seen_replay_axes.add(replay_axis)
            validated.append(fold)
            total_days += len(shared_axis[fold_ordinal]["daily_dates_utc"])
            total_trades += sum(len(account["completed_trade_close_utc"]) for account in accounts.values())
            if total_days > MAX_TOTAL_DAILY_POINTS:
                raise _ContractError("GLOBAL_DAILY_POINT_LIMIT_EXCEEDED")
            check_completed_trade_transport_count(total_trades, generation=_generation)

        daily_lengths = [
            len(axis["daily_dates_utc"]) for axis in shared_axis
        ]
        for lane in _generation.lane_fields:
            lane_folds = _folds_for_lane(validated, lane, _generation)
            trade_lengths = [len(fold["completed_trade_close_utc"]) for fold in lane_folds]
            daily_holding_days = [_derived_holding_days(fold) for fold in lane_folds]
            dependence_operations += _dependence_operation_count(daily_lengths, daily_holding_days)
            # Archived trade geometry was shared and charged once.
            if (not _generation.archived or lane == "BASE") and trade_lengths and all(length > 0 for length in trade_lengths):
                dependence_operations += _dependence_operation_count(trade_lengths)
        if dependence_operations > MAX_DEPENDENCE_BLOCK_OPERATIONS:
            raise _ContractError("DEPENDENCE_OPERATION_LIMIT_EXCEEDED")
        entries[ordinal] = {**entry, "folds": validated}

    if technical_failure:
        raise _ContractError("ATTEMPT_EVIDENCE_INCOMPLETE_TECHNICAL_FAILURE")
    return generated_at, entries, shared_axis

def _mean(values: list[float]) -> float:
    return math.fsum(values) / len(values)


def _sample_size_hac_bandwidth(count: int) -> int:
    if count < 2:
        return 0
    return min(
        count - 1,
        max(1, int(math.ceil(4.0 * (count / 100.0) ** (2.0 / 9.0)))),
    )


def _initial_positive_autocorrelation_lag(values: list[float]) -> int:
    """Detect persistent positive memory with a frozen, bounded IPS rule."""

    count = len(values)
    if count < 4:
        return 0
    center = _mean(values)
    centered = [value - center for value in values]
    gamma0 = math.fsum(value * value for value in centered) / count
    if gamma0 <= 0.0 or not math.isfinite(gamma0):
        return 0
    maximum_lag = min(count - 1, count // 4)
    last_positive_lag = 0
    # Pair adjacent autocorrelations and stop at the first non-positive pair.
    # This avoids the multiple-look behaviour of searching for an isolated
    # significant lag while still detecting the long positive memory that a
    # short sample-size bandwidth misses.
    for first_lag in range(1, maximum_lag, 2):
        pair_sum = 0.0
        for lag in (first_lag, first_lag + 1):
            covariance = (
                math.fsum(
                    centered[index] * centered[index - lag]
                    for index in range(lag, count)
                )
                / count
            )
            pair_sum += covariance / gamma0
        if pair_sum <= 0.0 or not math.isfinite(pair_sum):
            break
        last_positive_lag = first_lag + 1
    return last_positive_lag


def _newey_west_lrv(
    values: list[float], lag_limit: int | None = None
) -> tuple[float, float]:
    """Conservative Bartlett LRV and ESS; negative dependence cannot boost ESS."""

    count = len(values)
    center = _mean(values)
    centered = [value - center for value in values]
    gamma0 = math.fsum(value * value for value in centered) / count
    if gamma0 <= 0.0 or not math.isfinite(gamma0):
        return 0.0, 0.0
    lag_limit = (
        _sample_size_hac_bandwidth(count)
        if lag_limit is None
        else min(count - 1, max(0, lag_limit))
    )
    lrv = gamma0
    for lag in range(1, min(lag_limit, count - 1) + 1):
        covariance = (
            math.fsum(
                centered[index] * centered[index - lag] for index in range(lag, count)
            )
            / count
        )
        weight = 1.0 - lag / (lag_limit + 1.0)
        lrv += 2.0 * weight * covariance
    lrv = max(
        gamma0, lrv
    )  # do not claim extra power from estimated negative autocorrelation.
    ess = min(float(count), max(1.0, count * gamma0 / lrv))
    return lrv, ess


def _fold_aware_inference(
    fold_values: list[list[float]], lag_limits: list[int] | None = None
) -> dict[str, float]:
    total = sum(len(values) for values in fold_values)
    all_values = [value for values in fold_values for value in values]
    mean_value = math.fsum(all_values) / total
    variance_sum = 0.0
    ess = 0.0
    if lag_limits is None:
        lag_limits = [_sample_size_hac_bandwidth(len(values)) for values in fold_values]
    if len(lag_limits) != len(fold_values):
        raise _ContractError("DEPENDENCE_BANDWIDTH_ROSTER_MISMATCH")
    for values, lag_limit in zip(fold_values, lag_limits, strict=True):
        lrv, fold_ess = _newey_west_lrv(values, lag_limit)
        if lrv <= 0.0:
            return {"mean": mean_value, "se": math.inf, "z": -math.inf, "ess": 0.0}
        variance_sum += len(values) * lrv
        ess += fold_ess
    se = math.sqrt(variance_sum) / total
    z_value = mean_value / se if se > 0.0 and math.isfinite(se) else -math.inf
    return {"mean": mean_value, "se": se, "z": z_value, "ess": min(float(total), ess)}


@dataclass(slots=True)
class _RunningMoments:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        delta_n = delta / self.count
        self.mean += delta_n
        self.m2 += delta * (value - self.mean)


def _continued_beta_fraction(a: float, b: float, x: float) -> float:
    maximum_iterations = 256
    epsilon = 3.0e-14
    tiny = 1.0e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    result = d
    for iteration in range(1, maximum_iterations + 1):
        doubled = 2 * iteration
        coefficient = (
            iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        )
        d = 1.0 + coefficient * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + coefficient / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        result *= d * c
        coefficient = -(
            (a + iteration) * (qab + iteration) * x / ((a + doubled) * (qap + doubled))
        )
        d = 1.0 + coefficient * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + coefficient / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        result *= delta
        if abs(delta - 1.0) <= epsilon:
            return result
    raise _ContractError("STUDENT_T_BETA_FRACTION_DID_NOT_CONVERGE")


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if not (a > 0.0 and b > 0.0 and 0.0 <= x <= 1.0):
        raise _ContractError("STUDENT_T_BETA_INPUT_INVALID")
    if x in (0.0, 1.0):
        return x
    front = math.exp(
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        result = front * _continued_beta_fraction(a, b, x) / a
    else:
        result = 1.0 - front * _continued_beta_fraction(b, a, 1.0 - x) / b
    return min(1.0, max(0.0, result))


def _student_t_mean_one_sided_p(moments: _RunningMoments) -> float:
    if moments.count < 2 or moments.m2 <= 0.0:
        return 1.0
    sample_variance = moments.m2 / (moments.count - 1)
    standard_error = math.sqrt(sample_variance / moments.count)
    if standard_error <= 0.0 or not math.isfinite(standard_error):
        return 1.0
    t_value = moments.mean / standard_error
    degrees_of_freedom = moments.count - 1
    return _student_t_one_sided_tail(t_value, degrees_of_freedom)


def _student_t_one_sided_tail(t_value: float, degrees_of_freedom: int) -> float:
    if (
        type(degrees_of_freedom) is not int
        or degrees_of_freedom < 1
        or not math.isfinite(t_value)
    ):
        return 1.0
    beta_x = degrees_of_freedom / (degrees_of_freedom + t_value * t_value)
    positive_tail = 0.5 * _regularized_incomplete_beta(
        degrees_of_freedom / 2.0, 0.5, beta_x
    )
    return positive_tail if t_value >= 0.0 else 1.0 - positive_tail


@lru_cache(maxsize=16_384)
def _student_t_one_sided_critical(degrees_of_freedom: int, confidence: float) -> float:
    """Deterministically invert the exact Student-t tail for one bounded LCB."""

    if (
        type(degrees_of_freedom) is not int
        or degrees_of_freedom < 1
        or type(confidence) is not float
        or not 0.5 < confidence < 1.0
    ):
        raise _ContractError("PRIMARY_LCB_STUDENT_T_DOMAIN_INVALID")
    target_tail = 1.0 - confidence
    lower = 0.0
    upper = 1.0
    while _student_t_one_sided_tail(upper, degrees_of_freedom) > target_tail:
        upper *= 2.0
        if upper > 1_048_576.0:
            raise _ContractError("PRIMARY_LCB_STUDENT_T_INVERSION_FAILED")
    for _ in range(80):
        midpoint = (lower + upper) / 2.0
        if _student_t_one_sided_tail(midpoint, degrees_of_freedom) > target_tail:
            lower = midpoint
        else:
            upper = midpoint
    return upper


def _student_t_mean_lcb(
    moments: _RunningMoments,
    confidence: float,
) -> float:
    if moments.count < 2 or moments.m2 <= 0.0:
        return -math.inf
    sample_variance = moments.m2 / (moments.count - 1)
    standard_error = math.sqrt(sample_variance / moments.count)
    if standard_error <= 0.0 or not math.isfinite(standard_error):
        return -math.inf
    degrees_of_freedom = moments.count - 1
    critical = _student_t_one_sided_critical(degrees_of_freedom, confidence)
    return moments.mean - critical * standard_error


def _derived_holding_days(fold: dict[str, Any]) -> int:
    maximum = 1
    for raw_open, raw_close in zip(
        fold["completed_trade_open_utc"],
        fold["completed_trade_close_utc"],
        strict=True,
    ):
        opened = _timestamp(raw_open, "TRADE_OPEN_TIME_INVALID")
        closed = _timestamp(raw_close, "TRADE_CLOSE_TIME_INVALID")
        maximum = max(maximum, (closed.date() - opened.date()).days + 1)
    return maximum


def _stream_block_moments(
    prefixes: list[list[float]],
    block_size: int,
    alignment: int,
) -> _RunningMoments:
    moments = _RunningMoments()
    for prefix in prefixes:
        length = len(prefix) - 1
        candidate_offsets = (0, block_size // 2, length % block_size)
        offset = candidate_offsets[alignment]
        if offset >= block_size or length - offset < block_size:
            continue
        for start in range(offset, length - block_size + 1, block_size):
            block_mean = (prefix[start + block_size] - prefix[start]) / block_size
            moments.add(block_mean)
    return moments


def _precommitted_block_inference(
    fold_values: list[list[float]],
    primary_bandwidths: list[int],
    sensitivity_bandwidths: list[int],
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, float]:
    """Evaluate two declared memory scales; never recharge search multiplicity.

    These Student-t summaries contribute a conservative LCB for the same mean
    daily-log-growth estimand used everywhere else.  Their p-value is reported
    only as a diagnostic: candidate admission is owned exclusively by the
    primary/sensitivity HAC DSR gate on the original daily evidence vector.
    """

    minimum_blocks = int(_generation.policy["min_dependence_blocks"])
    if minimum_blocks != int(_generation.policy["min_causal_folds"]):
        raise _ContractError("DEPENDENCE_BLOCK_FLOOR_POLICY_INVALID")
    lengths = [len(values) for values in fold_values]
    block_sizes = _precommitted_block_sizes(primary_bandwidths, sensitivity_bandwidths)
    operation_count = _block_operation_count(lengths, block_sizes)
    if operation_count > MAX_DEPENDENCE_BLOCK_OPERATIONS:
        raise _ContractError("DEPENDENCE_OPERATION_LIMIT_EXCEEDED")
    maximum_block_size = _maximum_supported_block_size(lengths, minimum_blocks)
    prefixes: list[list[float]] = []
    for values in fold_values:
        prefix = [0.0]
        running = 0.0
        for value in values:
            running += value
            prefix.append(running)
        prefixes.append(prefix)
    worst_p = 0.0
    worst_lcb = math.inf
    worst_lcb_size = 0
    worst_lcb_count = 0
    minimum_count = math.inf
    scale_counts: dict[int, int] = {}
    evaluated = 0
    for block_size in sorted(set(block_sizes)):
        if block_size <= 0 or block_size > maximum_block_size:
            scale_counts[block_size] = 0
            minimum_count = 0
            continue
        scale_minimum_count = math.inf
        for alignment in range(3):
            moments = _stream_block_moments(prefixes, block_size, alignment)
            scale_minimum_count = min(scale_minimum_count, moments.count)
            minimum_count = min(minimum_count, moments.count)
            if moments.count < 2:
                continue
            evaluated += 1
            candidate_p = _student_t_mean_one_sided_p(moments)
            candidate_lcb = _student_t_mean_lcb(
                moments,
                PRIMARY_LCB_CONFIDENCE,
            )
            if candidate_p > worst_p:
                worst_p = candidate_p
            if candidate_lcb < worst_lcb:
                worst_lcb = candidate_lcb
                worst_lcb_size = block_size
                worst_lcb_count = moments.count
        scale_counts[block_size] = (
            int(scale_minimum_count) if math.isfinite(scale_minimum_count) else 0
        )
    primary_block_size, sensitivity_block_size = block_sizes
    return {
        "diagnostic_p": worst_p if evaluated else 1.0,
        "minimum_block_count": (
            float(minimum_count) if math.isfinite(minimum_count) else 0.0
        ),
        "primary_block_days": float(primary_block_size),
        "primary_block_count": float(scale_counts.get(primary_block_size, 0)),
        "sensitivity_block_days": float(sensitivity_block_size),
        "sensitivity_block_count": float(scale_counts.get(sensitivity_block_size, 0)),
        "maximum_supported_block_days": float(maximum_block_size),
        "operation_count": float(operation_count),
        "lcb": worst_lcb if evaluated else -math.inf,
        "lcb_block_days": float(worst_lcb_size),
        "lcb_block_count": float(worst_lcb_count),
    }


def _dependence_robust_inference(
    fold_values: list[list[float]],
    folds: list[dict[str, Any]] | None,
    attempted_trials: int,
    *,
    holding_days_override: list[int] | None = None,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, float]:
    all_values = [value for values in fold_values for value in values]
    if holding_days_override is not None:
        holding_days = holding_days_override
    else:
        holding_days = (
            [_derived_holding_days(fold) for fold in folds]
            if folds is not None
            else [1 for _ in fold_values]
        )
    primary_bandwidths, sensitivity_bandwidths = _dependence_bandwidth_plan(
        [len(values) for values in fold_values], holding_days
    )
    detected_autocorrelation_lags = [
        _initial_positive_autocorrelation_lag(values) for values in fold_values
    ]
    primary_bandwidths = [
        max(bandwidth, detected)
        for bandwidth, detected in zip(
            primary_bandwidths, detected_autocorrelation_lags, strict=True
        )
    ]
    sensitivity_bandwidths = [
        min(len(values) - 1, 2 * (bandwidth + 1) - 1)
        for values, bandwidth in zip(fold_values, primary_bandwidths, strict=True)
    ]
    primary = _fold_aware_inference(fold_values, primary_bandwidths)
    sensitivity = _fold_aware_inference(fold_values, sensitivity_bandwidths)
    block = _precommitted_block_inference(
        fold_values, primary_bandwidths, sensitivity_bandwidths, _generation=_generation
    )
    operation_upper_bound = _dependence_operation_count(
        [len(values) for values in fold_values], holding_days
    )
    if operation_upper_bound > MAX_DEPENDENCE_BLOCK_OPERATIONS:
        raise _ContractError("DEPENDENCE_OPERATION_LIMIT_EXCEEDED")
    # Search multiplicity is charged exactly once to each of the two declared
    # HAC views of this lane.  The maximum is their precommitted sensitivity
    # conjunction.  Crucially, the same trial count is never re-applied to
    # progressively smaller block samples.
    primary_p = _one_sided_dsr_p(all_values, primary["ess"], attempted_trials)
    sensitivity_p = _one_sided_dsr_p(all_values, sensitivity["ess"], attempted_trials)
    maximum_supported = int(block["maximum_supported_block_days"])
    maximum_holding = max(holding_days, default=1)
    primary_lcb = (
        primary["mean"] - _PRIMARY_LCB_NORMAL_CRITICAL * primary["se"]
        if math.isfinite(primary["se"])
        else -math.inf
    )
    sensitivity_lcb = (
        sensitivity["mean"] - _PRIMARY_LCB_NORMAL_CRITICAL * sensitivity["se"]
        if math.isfinite(sensitivity["se"])
        else -math.inf
    )
    return {
        "mean": primary["mean"],
        "se": max(primary["se"], sensitivity["se"]),
        "z": min(primary["z"], sensitivity["z"]),
        "ess": min(primary["ess"], sensitivity["ess"]),
        "primary_hac_p": primary_p,
        "sensitivity_hac_p": sensitivity_p,
        "precommitted_block_diagnostic_p": block["diagnostic_p"],
        "p": max(primary_p, sensitivity_p),
        "precommitted_primary_block_days": block["primary_block_days"],
        "precommitted_primary_block_count": block["primary_block_count"],
        "precommitted_sensitivity_block_days": block["sensitivity_block_days"],
        "precommitted_sensitivity_block_count": block["sensitivity_block_count"],
        "precommitted_minimum_block_count": block["minimum_block_count"],
        "maximum_supported_block_days": block["maximum_supported_block_days"],
        "maximum_observed_holding_days": float(maximum_holding),
        "maximum_detected_autocorrelation_lag_days": float(
            max(detected_autocorrelation_lags, default=0)
        ),
        "dependence_geometry_supported": float(
            maximum_holding <= maximum_supported
            and block["primary_block_days"] <= maximum_supported
            and block["sensitivity_block_days"] <= maximum_supported
        ),
        "dependence_operation_count": float(operation_upper_bound),
        "precommitted_block_operation_count": block["operation_count"],
        "primary_hac_lcb": primary_lcb,
        "sensitivity_hac_lcb": sensitivity_lcb,
        "precommitted_block_lcb": block["lcb"],
        "lcb": min(primary_lcb, sensitivity_lcb, block["lcb"]),
        "lcb_block_days": block["lcb_block_days"],
        "lcb_block_count": block["lcb_block_count"],
    }


def _expected_max_standard_normal(trials: int) -> float:
    if trials <= 1:
        return 0.0
    gamma = 0.5772156649015329
    distribution = NormalDist()
    first = distribution.inv_cdf(1.0 - 1.0 / trials)
    second = distribution.inv_cdf(1.0 - 1.0 / (trials * math.e))
    return max(0.0, (1.0 - gamma) * first + gamma * second)


def _one_sided_dsr_p(
    values: list[float], effective_observations: float, trials: int
) -> float:
    """Dependence-adjusted Deflated Sharpe probability, returned as a p-value."""

    if len(values) < 4 or effective_observations <= 1.0:
        return 1.0
    center = _mean(values)
    centered = [value - center for value in values]
    second = math.fsum(value**2 for value in centered) / len(values)
    if second <= 0.0 or not math.isfinite(second):
        return 1.0
    standard_deviation = math.sqrt(second)
    sharpe = center / standard_deviation
    skew = (math.fsum(value**3 for value in centered) / len(values)) / (second**1.5)
    kurtosis = (math.fsum(value**4 for value in centered) / len(values)) / (second**2)
    denominator_squared = 1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe**2
    if denominator_squared <= 0.0 or not math.isfinite(denominator_squared):
        return 1.0
    root_effective = math.sqrt(effective_observations - 1.0)
    deflated_sharpe_threshold = _expected_max_standard_normal(trials) / root_effective
    deflated_z = (
        (sharpe - deflated_sharpe_threshold)
        * root_effective
        / math.sqrt(denominator_squared)
    )
    return min(1.0, max(0.0, 0.5 * math.erfc(deflated_z / math.sqrt(2.0))))


def _holm_adjust(raw_p_values: list[float]) -> list[float]:
    count = len(raw_p_values)
    order = sorted(range(count), key=lambda index: (raw_p_values[index], index))
    adjusted = [1.0] * count
    running = 0.0
    for rank, index in enumerate(order):
        candidate = min(1.0, (count - rank) * raw_p_values[index])
        running = max(running, candidate)
        adjusted[index] = running
    return adjusted


def _tail_cvar(values: list[float], fraction: float) -> float:
    tail_count = max(1, int(math.ceil(len(values) * fraction)))
    return math.fsum(sorted(values)[:tail_count]) / tail_count


def _max_drawdown(values: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def _daily_log_growth_from_r(
    value: float, *, _generation: _ContractGeneration = ARCHIVED_GENERATION
) -> float:
    """Map one post-cost cell-risk-unit outcome to fixed-risk equity log growth."""

    risk_fraction = _generation.policy["primary_cell_risk_fraction"]
    if (
        type(risk_fraction) is not float
        or risk_fraction != PRIMARY_CELL_RISK_FRACTION
        or not 0.0 < risk_fraction < 1.0
    ):
        raise _ContractError("PRIMARY_CELL_RISK_POLICY_INVALID")
    if type(value) is not float or not math.isfinite(value):
        raise _ContractError("PRIMARY_UTILITY_INPUT_INVALID")
    scaled_return = risk_fraction * value
    if scaled_return <= -1.0:
        raise _ContractError("PRIMARY_UTILITY_LOG_DOMAIN_INVALID")
    result = math.log1p(scaled_return)
    if not math.isfinite(result):
        raise _ContractError("PRIMARY_UTILITY_RESULT_INVALID")
    return result


_UINT64_MASK: Final = (1 << 64) - 1
_UINT64_RANGE: Final = 1 << 64


def _splitmix64_next(state: int) -> tuple[int, int]:
    """Return the next value from the locally pinned deterministic generator."""

    state = (state + 0x9E3779B97F4A7C15) & _UINT64_MASK
    value = state
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _UINT64_MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _UINT64_MASK
    value ^= value >> 31
    return state, value & _UINT64_MASK


def _bounded_random_index(state: int, bound: int) -> tuple[int, int]:
    """Draw an unbiased integer in ``range(bound)`` using rejection sampling."""

    if type(bound) is not int or bound <= 0:
        raise _ContractError("BOOTSTRAP_RANDOM_BOUND_INVALID")
    rejection_limit = _UINT64_RANGE - (_UINT64_RANGE % bound)
    while True:
        state, value = _splitmix64_next(state)
        if value < rejection_limit:
            return state, value % bound


def _bootstrap_seed(
    process_candidate_id: str,
    lane: str,
    scale_role: str,
    block_days: int,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[int, str]:
    seed_body = {
        "seed_domain": _generation.policy["moving_block_bootstrap_seed_domain"],
        "selection_policy_sha256": _generation.policy_sha256,
        "process_candidate_id": process_candidate_id,
        "lane": lane,
        "scale_role": scale_role,
        "block_days": block_days,
        "repetitions": _generation.policy["moving_block_bootstrap_repetitions"],
    }
    seed_sha256 = _sha256_json(seed_body)
    return int(seed_sha256[:16], 16), seed_sha256


def _moving_block_bootstrap_scale(
    fold_values: list[list[float]],
    *,
    process_candidate_id: str,
    lane: str,
    scale_role: str,
    block_days: int,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    repetitions = int(_generation.policy["moving_block_bootstrap_repetitions"])
    lower_tail_numerator = int(
        _generation.policy["moving_block_bootstrap_lower_tail_numerator"]
    )
    lower_tail_denominator = int(
        _generation.policy["moving_block_bootstrap_lower_tail_denominator"]
    )
    base = {
        "scale_role": scale_role,
        "block_days": block_days,
        "repetitions": repetitions,
        "lower_tail_numerator": lower_tail_numerator,
        "lower_tail_denominator": lower_tail_denominator,
    }
    if (
        type(block_days) is not int
        or block_days <= 0
        or not fold_values
        or any(not values or block_days > len(values) for values in fold_values)
    ):
        body = {
            **base,
            "status": "INSUFFICIENT_EVIDENCE",
            "passed": False,
            "reason_code": "MOVING_BLOCK_GEOMETRY_UNSUPPORTED",
            "observation_count": sum(len(values) for values in fold_values),
            "moving_block_start_count": 0,
            "resampled_block_draw_count": 0,
            "observed_mean_daily_log_growth": None,
            "lower_confidence_bound_daily_log_growth": None,
            "bootstrap_median_daily_log_growth": None,
            "positive_bootstrap_fraction": None,
            "seed_sha256": None,
        }
        return {**body, "evidence_sha256": _sha256_json(body)}
    block_pool: list[tuple[list[float], int]] = []
    total_observations = 0
    moving_starts = 0
    for values in fold_values:
        prefix = [0.0]
        running = 0.0
        for value in values:
            if type(value) is not float or not math.isfinite(value):
                raise _ContractError("BOOTSTRAP_DAILY_LOG_GROWTH_INVALID")
            running += value
            prefix.append(running)
        total_observations += len(values)
        moving_starts += len(values) - block_days + 1
        block_pool.extend(
            (prefix, start) for start in range(len(values) - block_days + 1)
        )
    blocks_per_repetition = math.ceil(total_observations / block_days)
    maximum_blocks = int(
        _generation.policy["moving_block_bootstrap_max_resampled_blocks_per_scale"]
    )
    operation_count = repetitions * blocks_per_repetition
    if blocks_per_repetition > maximum_blocks or operation_count > int(
        _generation.policy["moving_block_bootstrap_operation_limit"]
    ):
        body = {
            **base,
            "status": "INSUFFICIENT_EVIDENCE",
            "passed": False,
            "reason_code": "MOVING_BLOCK_OPERATION_BOUND_EXCEEDED",
            "observation_count": total_observations,
            "moving_block_start_count": moving_starts,
            "resampled_block_draw_count": operation_count,
            "observed_mean_daily_log_growth": None,
            "lower_confidence_bound_daily_log_growth": None,
            "bootstrap_median_daily_log_growth": None,
            "positive_bootstrap_fraction": None,
            "seed_sha256": None,
        }
        return {**body, "evidence_sha256": _sha256_json(body)}
    state, seed_sha256 = _bootstrap_seed(
        process_candidate_id, lane, scale_role, block_days, _generation=_generation
    )
    bootstrap_means: list[float] = []
    for _ in range(repetitions):
        sampled_sum = 0.0
        remaining = total_observations
        while remaining:
            width = min(block_days, remaining)
            state, block_index = _bounded_random_index(state, len(block_pool))
            prefix, start = block_pool[block_index]
            sampled_sum += prefix[start + width] - prefix[start]
            remaining -= width
        bootstrap_means.append(sampled_sum / total_observations)
    bootstrap_means.sort()
    lower_rank = max(
        0,
        (lower_tail_numerator * (repetitions + 1)) // lower_tail_denominator - 1,
    )
    lower_rank = min(lower_rank, repetitions - 1)
    lower_bound = bootstrap_means[lower_rank]
    median = bootstrap_means[(repetitions - 1) // 2]
    observed_mean = (
        math.fsum(value for values in fold_values for value in values)
        / total_observations
    )
    positive_fraction = sum(value > 0.0 for value in bootstrap_means) / repetitions
    passed = bool(lower_bound > 0.0)
    body = {
        **base,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "reason_code": (
            "LOWER_CONFIDENCE_BOUND_POSITIVE"
            if passed
            else "LOWER_CONFIDENCE_BOUND_NONPOSITIVE"
        ),
        "observation_count": total_observations,
        "moving_block_start_count": moving_starts,
        "resampled_block_draw_count": operation_count,
        "observed_mean_daily_log_growth": observed_mean,
        "lower_confidence_bound_daily_log_growth": lower_bound,
        "bootstrap_median_daily_log_growth": median,
        "positive_bootstrap_fraction": positive_fraction,
        "seed_sha256": seed_sha256,
    }
    return {**body, "evidence_sha256": _sha256_json(body)}


def _moving_block_bootstrap_evidence(
    *,
    process_candidate_id: str,
    lane_log_growth_folds: dict[str, list[list[float]]],
    lane_inferences: dict[str, dict[str, float]],
    attempted_trials: int,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    if tuple(lane_log_growth_folds) != tuple(_generation.lane_fields):
        raise _ContractError("BOOTSTRAP_LANE_ROSTER_MISMATCH")
    totals = {
        lane: sum(len(values) for values in folds)
        for lane, folds in lane_log_growth_folds.items()
    }
    if len(set(totals.values())) != 1:
        raise _ContractError("BOOTSTRAP_LANE_AXIS_MISMATCH")
    total_days = next(iter(totals.values()))
    adaptive_floor = max(5, math.ceil(total_days / 64))

    def lane_evidence(
        lane: str,
        folds: list[list[float]],
        inference: dict[str, float],
    ) -> dict[str, Any]:
        primary_days = max(
            adaptive_floor,
            int(inference["precommitted_primary_block_days"]),
        )
        sensitivity_days = max(
            2 * primary_days,
            adaptive_floor,
            int(inference["precommitted_sensitivity_block_days"]),
        )
        primary = _moving_block_bootstrap_scale(
            folds,
            process_candidate_id=process_candidate_id,
            lane=lane,
            scale_role="PRIMARY",
            block_days=primary_days,
            _generation=_generation,
        )
        sensitivity = _moving_block_bootstrap_scale(
            folds,
            process_candidate_id=process_candidate_id,
            lane=lane,
            scale_role="SENSITIVITY",
            block_days=sensitivity_days,
            _generation=_generation,
        )
        statuses = {primary["status"], sensitivity["status"]}
        if "INSUFFICIENT_EVIDENCE" in statuses:
            status = "INSUFFICIENT_EVIDENCE"
        elif primary["passed"] and sensitivity["passed"]:
            status = "PASS"
        else:
            status = "FAIL"
        bounds = [
            value["lower_confidence_bound_daily_log_growth"]
            for value in (primary, sensitivity)
            if type(value["lower_confidence_bound_daily_log_growth"]) is float
        ]
        body = {
            "lane": lane,
            "status": status,
            "passed": status == "PASS",
            "primary": primary,
            "sensitivity": sensitivity,
            "conservative_lower_confidence_bound_daily_log_growth": (
                min(bounds) if len(bounds) == 2 else None
            ),
            "resampled_block_draw_count": int(
                primary["resampled_block_draw_count"]
                + sensitivity["resampled_block_draw_count"]
            ),
        }
        return {**body, "evidence_sha256": _sha256_json(body)}

    lane_evidence_by_name = {
        lane: lane_evidence(
            lane,
            lane_log_growth_folds[lane],
            lane_inferences[lane],
        )
        for lane in _generation.lane_fields
    }
    total_operations = sum(
        int(evidence["resampled_block_draw_count"])
        for evidence in lane_evidence_by_name.values()
    )
    per_candidate_limit = int(
        _generation.policy["moving_block_bootstrap_operation_limit"]
    ) // max(1, attempted_trials)

    tier_evidence: dict[str, dict[str, Any]] = {}
    for tier, lanes in _generation.tier_lanes.items():
        members = [lane_evidence_by_name[lane] for lane in lanes]
        if total_operations > per_candidate_limit:
            status = "INSUFFICIENT_EVIDENCE"
            reason_code = "CELL_BOOTSTRAP_OPERATION_BUDGET_EXCEEDED"
        elif any(member["status"] == "INSUFFICIENT_EVIDENCE" for member in members):
            status = "INSUFFICIENT_EVIDENCE"
            reason_code = "BOOTSTRAP_LANE_EVIDENCE_INSUFFICIENT"
        elif all(member["passed"] for member in members):
            status = "PASS"
            reason_code = "ALL_TIER_LANE_LOWER_BOUNDS_POSITIVE"
        else:
            status = "FAIL"
            reason_code = "TIER_LANE_LOWER_BOUND_NONPOSITIVE"
        bounds = [
            member["conservative_lower_confidence_bound_daily_log_growth"]
            for member in members
        ]
        tier_body = {
            "tier": tier,
            "lanes": list(lanes),
            "status": status,
            "passed": status == "PASS",
            "reason_code": reason_code,
            "conservative_lower_confidence_bound_daily_log_growth": (
                min(float(value) for value in bounds)
                if all(type(value) is float for value in bounds)
                else None
            ),
        }
        tier_evidence[tier] = {
            **tier_body,
            "evidence_sha256": _sha256_json(tier_body),
        }

    body = {
        "contract_version": _generation.policy[
            "moving_block_bootstrap_contract_version"
        ],
        "daily_unit": _generation.policy["moving_block_bootstrap_daily_unit"],
        "primary_cell_risk_fraction": PRIMARY_CELL_RISK_FRACTION,
        "process_candidate_id": process_candidate_id,
        "attempted_trials_exact": attempted_trials,
        "block_rule": _generation.policy["moving_block_bootstrap_block_rule"],
        "pool_rule": _generation.policy["moving_block_bootstrap_pool_rule"],
        "seed_rule": _generation.policy["moving_block_bootstrap_seed_rule"],
        "prng": _generation.policy["moving_block_bootstrap_prng"],
        "lane_evidence": lane_evidence_by_name,
        "tier_evidence": tier_evidence,
        "resampled_block_draw_count": total_operations,
        "per_candidate_operation_limit": per_candidate_limit,
    }
    return {**body, "evidence_sha256": _sha256_json(body)}

def _pbo_performance(total: float, square_total: float, count: int) -> float:
    if count < 2:
        return -1e300
    mean = total / count
    variance = max(0.0, (square_total - total * total / count) / (count - 1))
    if variance <= 1e-30:
        if mean > 0.0:
            return 1e300
        if mean < 0.0:
            return -1e300
        return 0.0
    return mean / math.sqrt(variance)


def _pbo_lane_evidence(
    *,
    lane: str,
    candidate_ids: list[str],
    candidate_values: list[list[float]],
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    observations = len(candidate_values[0])
    block_count = int(_generation.policy["pbo_contiguous_block_count"])
    quotient, remainder = divmod(observations, block_count)
    block_ranges: list[tuple[int, int]] = []
    start = 0
    for ordinal in range(block_count):
        width = quotient + (1 if ordinal < remainder else 0)
        block_ranges.append((start, start + width))
        start += width
    block_moments: list[list[tuple[float, float, int]]] = []
    for values in candidate_values:
        candidate_blocks: list[tuple[float, float, int]] = []
        for lower, upper in block_ranges:
            segment = values[lower:upper]
            candidate_blocks.append(
                (
                    math.fsum(segment),
                    math.fsum(value * value for value in segment),
                    len(segment),
                )
            )
        block_moments.append(candidate_blocks)
    selections = list(combinations(range(block_count), block_count // 2))
    expected_combinations = int(_generation.policy["pbo_complete_combinations"])
    if len(selections) != expected_combinations:
        raise _ContractError("PBO_COMBINATION_POLICY_MISMATCH")
    relative_ranks: list[float] = []
    selected_ids: list[str] = []
    for selected_blocks in selections:
        inside = frozenset(selected_blocks)
        in_performance: list[float] = []
        out_performance: list[float] = []
        for candidate_blocks in block_moments:
            in_total = math.fsum(candidate_blocks[index][0] for index in inside)
            in_squares = math.fsum(candidate_blocks[index][1] for index in inside)
            in_count = sum(candidate_blocks[index][2] for index in inside)
            outside = [index for index in range(block_count) if index not in inside]
            out_total = math.fsum(candidate_blocks[index][0] for index in outside)
            out_squares = math.fsum(candidate_blocks[index][1] for index in outside)
            out_count = sum(candidate_blocks[index][2] for index in outside)
            in_performance.append(_pbo_performance(in_total, in_squares, in_count))
            out_performance.append(_pbo_performance(out_total, out_squares, out_count))
        selected_index = max(
            range(len(candidate_ids)),
            key=lambda index: (in_performance[index], -index),
        )
        selected_ids.append(candidate_ids[selected_index])
        selected_oos = out_performance[selected_index]
        lower = sum(value < selected_oos for value in out_performance)
        equal = sum(value == selected_oos for value in out_performance)
        relative_rank = (lower + 0.5 * (equal - 1)) / (len(candidate_ids) - 1)
        relative_ranks.append(relative_rank)
    pbo = sum(rank < 0.5 for rank in relative_ranks) / len(relative_ranks)
    threshold = float(_generation.policy["pbo_max_probability"])
    passed = bool(pbo <= threshold)
    body = {
        "lane": lane,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "pbo": pbo,
        "maximum_pbo": threshold,
        "observation_count": observations,
        "candidate_count": len(candidate_ids),
        "contiguous_block_count": block_count,
        "complete_combination_count": len(selections),
        "median_selected_oos_relative_rank": sorted(relative_ranks)[
            (len(relative_ranks) - 1) // 2
        ],
        "selected_candidate_id_counts": [
            {
                "process_candidate_id": candidate_id,
                "count": selected_ids.count(candidate_id),
            }
            for candidate_id in candidate_ids
            if candidate_id in selected_ids
        ],
        "matrix_sha256": _sha256_json(
            {
                "candidate_ids": candidate_ids,
                "daily_log_growth_by_candidate": candidate_values,
            }
        ),
    }
    return {**body, "evidence_sha256": _sha256_json(body)}


def _cell_pbo_evidence(
    attempts: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    shared_axis: list[dict[str, Any]],
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    attempt_roster = [
        {
            "ordinal": attempt["ordinal"],
            "attempt_id": attempt["attempt_id"],
            "process_candidate_id": attempt["process_candidate_id"],
            "precommitted_evidence_role": attempt["precommitted_evidence_role"],
            "evidence_status": entry["evidence_status"],
            "status_reason": entry["status_reason"],
        }
        for attempt, entry in zip(attempts, entries, strict=True)
    ]
    evaluation_required = sorted(
        (
            (attempt["process_candidate_id"], entry)
            for attempt, entry in zip(attempts, entries, strict=True)
            if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
        ),
        key=lambda item: item[0],
    )
    candidate_ids = [item[0] for item in evaluation_required]
    presearch_rejected = [
        record
        for record in attempt_roster
        if record["precommitted_evidence_role"] == "PRESEARCH_PROCESS_REJECTED"
    ]
    authenticated = [
        record
        for record in attempt_roster
        if record["evidence_status"] == "AUTHENTICATED"
    ]
    base_body = {
        "contract_version": _generation.policy["pbo_contract_version"],
        "daily_unit": _generation.policy["pbo_daily_unit"],
        "performance_measure": _generation.policy["pbo_performance_measure"],
        "candidate_population_rule": _generation.policy[
            "pbo_candidate_population"
        ],
        "attempted_trials_exact": len(attempts),
        "evaluation_required_candidate_count": len(evaluation_required),
        "authenticated_candidate_count": len(authenticated),
        "presearch_rejected_attempt_count": len(presearch_rejected),
        "attempt_roster": attempt_roster,
        "attempt_roster_sha256": _sha256_json(attempt_roster),
        "evaluation_required_candidate_ids": candidate_ids,
        "evaluation_required_candidate_roster_sha256": _sha256_json(candidate_ids),
        "presearch_rejected_attempts": presearch_rejected,
        "presearch_rejected_attempt_roster_sha256": _sha256_json(presearch_rejected),
        "daily_axis_sha256": _sha256_json(shared_axis),
    }

    def insufficient(reason_code: str) -> dict[str, Any]:
        tier_evidence = {
            tier: {
                "tier": tier,
                "lanes": list(lanes),
                "status": "INSUFFICIENT_EVIDENCE",
                "passed": False,
                "reason_code": reason_code,
                "conservative_pbo": None,
            }
            for tier, lanes in _generation.tier_lanes.items()
        }
        body = {
            **base_body,
            "status": "INSUFFICIENT_EVIDENCE",
            "passed": False,
            "reason_code": reason_code,
            "lane_evidence": {lane: None for lane in _generation.lane_fields},
            "tier_evidence": tier_evidence,
        }
        return {**body, "evidence_sha256": _sha256_json(body)}

    minimum_candidates = int(_generation.policy["pbo_min_evaluation_candidates"])
    if len(evaluation_required) < minimum_candidates:
        return insufficient("EVALUATION_REQUIRED_CANDIDATE_MATRIX_TOO_NARROW")

    lane_values: dict[str, list[list[float]]] = {lane: [] for lane in _generation.lane_fields}
    for _, entry in evaluation_required:
        for lane, field in _generation.lane_fields.items():
            lane_values[lane].append(
                [
                    _daily_log_growth_from_r(float(value), _generation=_generation)
                    for fold in entry["folds"]
                    for value in fold[field]
                ]
            )
    observations = len(next(iter(lane_values.values()))[0])
    if (
        observations < int(_generation.policy["pbo_min_observations"])
        or observations
        < 2 * int(_generation.policy["pbo_contiguous_block_count"])
        or any(
            len(values) != observations
            for candidates in lane_values.values()
            for values in candidates
        )
    ):
        return insufficient("ALIGNED_DAILY_MATRIX_TOO_SHORT")

    lane_evidence = {
        lane: _pbo_lane_evidence(
            lane=lane,
            candidate_ids=candidate_ids,
            candidate_values=lane_values[lane],
            _generation=_generation,
        )
        for lane in _generation.lane_fields
    }
    tier_evidence: dict[str, dict[str, Any]] = {}
    for tier, lanes in _generation.tier_lanes.items():
        members = [lane_evidence[lane] for lane in lanes]
        passed = all(member["passed"] for member in members)
        tier_body = {
            "tier": tier,
            "lanes": list(lanes),
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "reason_code": (
                "ALL_TIER_LANE_PBO_WITHIN_LIMIT"
                if passed
                else "TIER_LANE_PBO_EXCEEDS_LIMIT"
            ),
            "conservative_pbo": max(float(member["pbo"]) for member in members),
        }
        tier_evidence[tier] = {
            **tier_body,
            "evidence_sha256": _sha256_json(tier_body),
        }
    strongest = tier_evidence["P95_PREFERENCE"]
    body = {
        **base_body,
        "status": strongest["status"],
        "passed": strongest["passed"],
        "reason_code": strongest["reason_code"],
        "lane_evidence": lane_evidence,
        "tier_evidence": tier_evidence,
    }
    return {**body, "evidence_sha256": _sha256_json(body)}

def _validate_primary_policy_contract(
    *, _generation: _ContractGeneration = ARCHIVED_GENERATION
) -> None:
    expected = {
        "policy_version": "fxpm.winner-statistical-policy.v11",
        "cost_ladder_lane_order": tuple(_generation.lane_fields),
        "required_cost_ladder_tier": "P75_REQUIRED",
        "preference_cost_ladder_tier": "P95_PREFERENCE",
        "p75_required_lanes": _generation.tier_lanes["P75_REQUIRED"],
        "p95_preference_lanes": _generation.tier_lanes["P95_PREFERENCE"],
        "primary_utility": "MEAN_DAILY_LOG_GROWTH_AT_FIXED_CELL_RISK",
        "primary_cell_risk_fraction": PRIMARY_CELL_RISK_FRACTION,
        "primary_transform": "LOG1P(0.0025_TIMES_DAILY_NET_MTM_R)",
        "primary_lcb_confidence": PRIMARY_LCB_CONFIDENCE,
        "primary_lcb_dependence_rule": (
            "MIN_PRIMARY_HAC_SENSITIVITY_HAC_AND_TWO_PRECOMMITTED_MEMORY_"
            "SCALE_BLOCK_STUDENT_T"
        ),
        "block_lcb_participates_in_candidate_score": True,
        "least_favorable_dependence_p_value_binds": False,
        "attempt_aware_dsr_reapplications_per_block_scale": 0,
        "primary_candidate_score": (
            "MIN_DEPENDENCE_ROBUST_LCB_MEAN_DAILY_LOG_GROWTH_ACROSS_TIER_LANES"
        ),
        "primary_pairwise_inference": (
            "PAIRED_DAILY_LOG_GROWTH_DIFFERENCES_LEAST_FAVORABLE_"
            "RAW_BASE_P75_P95_PAIR_DEPENDENCE_ONCE_THEN_SEPARATE_TIER_HOLM"
        ),
        "selection_dominance_fields": (_PRIMARY_UTILITY_FIELD,),
        "pbo_lane_rule": (
            "COMPUTE_BASE_P75_P95_ONCE_THEN_CONJOIN_LANES_BY_COST_LADDER_TIER"
        ),
        "moving_block_bootstrap_pool_rule": (
            "POOL_ALL_WITHIN_FOLD_MOVING_BLOCK_STARTS_WITHOUT_CROSSING_"
            "ANY_CAUSAL_FOLD_BOUNDARY"
        ),
        "moving_block_bootstrap_lower_bound_must_be_positive": True,
    }
    if not _generation.archived:
        expected.update({
            "policy_version": "fxpm.winner-statistical-policy.v12",
            "primary_pairwise_inference": "INDEPENDENT_P75_P95_PAIRED_DAILY_DIFFERENCES_SEPARATE_TIER_HOLM",
            "pbo_lane_rule": "INDEPENDENT_FROZEN_TIER_ACCOUNTS",
        })
    if any(
        _generation.policy.get(name) != value for name, value in expected.items()
    ):
        raise _ContractError("PRIMARY_SELECTION_POLICY_IMPLEMENTATION_MISMATCH")
    if 1.0 - PRIMARY_CELL_RISK_FRACTION * MAX_ABS_DAILY_R <= 0.0:
        raise _ContractError("PRIMARY_UTILITY_ACCEPTED_DOMAIN_INVALID")

def _completed_trade_power_account(
    entry: dict[str, Any],
    attempted_trials: int,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    lane_values: dict[str, list[float]] = {lane: [] for lane in _generation.lane_fields}
    lane_ess: dict[str, float] = {lane: 0.0 for lane in _generation.lane_fields}
    overlap_components = 0
    bound_required = int(_generation.policy["min_hac_effective_completed_trades"])
    for fold in entry["folds"]:
        fold_values = {
            lane: [float(value) for value in fold[field]]
            for lane, field in _generation.trade_lane_fields.items()
        }
        for lane, values in fold_values.items():
            lane_values[lane].extend(values)
        binding = fold["trade_power_binding"]
        if type(binding) is not dict:
            raise _ContractError("AUTHENTICATED_TRADE_POWER_BINDING_MISSING")
        bound_required = max(
            bound_required,
            int(binding["required_effective_trades"]),
        )
        intervals = [
            {
                "start": opened,
                "end": closed,
                "start_event_order": open_order,
                "end_event_order": close_order,
            }
            for opened, closed, open_order, close_order in zip(
                fold["completed_trade_open_utc"],
                fold["completed_trade_close_utc"],
                fold["completed_trade_open_event_order"],
                fold["completed_trade_close_event_order"],
                strict=True,
            )
        ]
        fold_evidence = {
            lane: effective_sample_evidence(
                values,
                holding_intervals=intervals,
                minimum_observations=3,
            )
            for lane, values in fold_values.items()
        }
        for lane, evidence in fold_evidence.items():
            if evidence.passed:
                lane_ess[lane] += float(evidence.effective_observations)
        overlap_components += min(
            int(evidence.overlap_cluster_count or 0)
            for evidence in fold_evidence.values()
        )

    sigma_values = [
        robust_r_dispersion(values) for values in lane_values.values()
    ]
    valid_sigma = [value for value in sigma_values if value is not None]
    derived_required = MAX_TOTAL_COMPLETED_TRADES + 1
    if valid_sigma:
        power_policy = TradePowerPolicy(
            minimum_economic_edge_r=float(
                _generation.policy["trade_power_minimum_economic_edge_r"]
            ),
            maximum_ci_half_width_r=float(
                _generation.policy["trade_power_maximum_ci_half_width_r"]
            ),
            target_power=float(_generation.policy["trade_power_target_power"]),
            alpha_train=float(_generation.policy["trade_power_alpha"]),
            alpha_tuning=float(_generation.policy["trade_power_alpha"]),
            alpha_final=float(_generation.policy["trade_power_alpha"]),
        )
        requirement = derive_trade_power_requirement(
            sigma_r_robust=max(valid_sigma),
            effective_trials=attempted_trials,
            policy=power_policy,
        )
        if requirement.passed:
            derived_required = max(
                int(requirement.required_effective_tuning_trades or 0),
                int(requirement.required_effective_final_trades or 0),
            )
    required = max(bound_required, derived_required)
    return {
        "lane_effective_counts": lane_ess,
        "tier_effective_counts": {
            tier: min(lane_ess[lane] for lane in lanes)
            for tier, lanes in _generation.tier_lanes.items()
        },
        "overlap_components": overlap_components,
        "bound_required": bound_required,
        "derived_required": derived_required,
        "required": required,
    }


def _completed_trade_power(
    entry: dict[str, Any],
    attempted_trials: int,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    if _generation.archived:
        return _completed_trade_power_account(
            entry, attempted_trials, _generation=_generation
        )
    accounts = {}
    for tier, lanes in _generation.tier_lanes.items():
        lane = lanes[0]
        folds = _folds_for_lane(entry["folds"], lane, _generation)
        if any(fold["trade_power_binding"] is None for fold in folds):
            accounts[tier] = {
                "lane_effective_counts": {lane: 0.0},
                "overlap_components": 0,
                "bound_required": MAX_TOTAL_COMPLETED_TRADES + 1,
                "derived_required": MAX_TOTAL_COMPLETED_TRADES + 1,
                "required": MAX_TOTAL_COMPLETED_TRADES + 1,
            }
            continue
        account_generation = replace(
            _generation,
            lane_fields=MappingProxyType({lane: "daily_net_mtm_r"}),
            trade_lane_fields=MappingProxyType({lane: "completed_trade_net_mtm_r"}),
            tier_lanes=MappingProxyType({tier: (lane,)}),
        )
        accounts[tier] = _completed_trade_power_account(
            {**entry, "folds": folds}, attempted_trials, _generation=account_generation
        )
    return {
        "lane_effective_counts": {
            lane: accounts[tier]["lane_effective_counts"][lane]
            for tier, (lane,) in _generation.tier_lanes.items()
        },
        "tier_effective_counts": {
            tier: min(value["lane_effective_counts"].values())
            for tier, value in accounts.items()
        },
        "tier_required": {tier: value["required"] for tier, value in accounts.items()},
        "overlap_components": min(
            value["overlap_components"] for value in accounts.values()
        ),
        "bound_required": max(value["bound_required"] for value in accounts.values()),
        "derived_required": max(
            value["derived_required"] for value in accounts.values()
        ),
        "required": max(value["required"] for value in accounts.values()),
    }


def _candidate_statistics(
    attempt: dict[str, Any],
    entry: dict[str, Any],
    attempted_trials: int,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    identity = {
        "ordinal": attempt["ordinal"],
        "attempt_id": attempt["attempt_id"],
        "process_candidate_id": attempt["process_candidate_id"],
        "strategy_family": attempt["strategy_family"],
        "strategy_id": attempt["strategy_id"],
        "parameter_count": attempt["parameter_count"],
        "process_sha256": attempt["process_sha256"],
        "horizon_process_sha256": attempt["horizon_process_sha256"],
        "trial_definition_sha256": attempt["trial_definition_sha256"],
        "refit_recipe_sha256": attempt["refit_recipe_sha256"],
        "evidence_status": entry["evidence_status"],
    }
    unavailable_tiers = {
        tier: {
            "tier": tier,
            "lanes": list(lanes),
            "admissible": False,
            "admissibility_failures": ["PROCESS_NOT_AUTHENTICATED"],
            "attempt_aware_dsr_p_value": 1.0,
            "within_cell_adjusted_p_value": 1.0,
            "hac_effective_days": 0.0,
            "completed_trade_hac_effective_count": 0.0,
            "completed_trade_required_effective_count": 0,
            _PRIMARY_UTILITY_FIELD: None,
        }
        for tier, lanes in _generation.tier_lanes.items()
    }
    if entry["evidence_status"] != "AUTHENTICATED":
        return {
            **identity,
            "daily_count": 0,
            "completed_trade_count": 0,
            "completed_trade_overlap_component_count": 0,
            "completed_trade_bound_required_effective_count": 0,
            "completed_trade_derived_required_effective_count": 0,
            "lane_statistics": {},
            "tier_statistics": unavailable_tiers,
            "moving_block_bootstrap_evidence": None,
        }

    lane_folds = {
        lane: [
            [float(value) for value in fold[field]]
            for fold in entry["folds"]
        ]
        for lane, field in _generation.lane_fields.items()
    }
    lane_log_folds = {
        lane: [
            [
                _daily_log_growth_from_r(value, _generation=_generation)
                for value in values
            ]
            for values in folds
        ]
        for lane, folds in lane_folds.items()
    }
    lane_inferences = {
        lane: _dependence_robust_inference(
            lane_log_folds[lane],
            _folds_for_lane(entry["folds"], lane, _generation),
            attempted_trials,
            _generation=_generation,
        )
        for lane in _generation.lane_fields
    }
    bootstrap = _moving_block_bootstrap_evidence(
        process_candidate_id=attempt["process_candidate_id"],
        lane_log_growth_folds=lane_log_folds,
        lane_inferences=lane_inferences,
        attempted_trials=attempted_trials,
        _generation=_generation,
    )
    trade_power = _completed_trade_power(
        entry, attempted_trials, _generation=_generation
    )
    lane_trade_counts = {
        lane: sum(len(fold["completed_trade_close_utc"]) for fold in _folds_for_lane(entry["folds"], lane, _generation))
        for lane in _generation.lane_fields
    }
    completed_trades = min(lane_trade_counts.values())

    lane_statistics: dict[str, dict[str, Any]] = {}
    for lane in _generation.lane_fields:
        folds = lane_folds[lane]
        log_folds = lane_log_folds[lane]
        values = [value for fold in folds for value in fold]
        inference = lane_inferences[lane]
        fold_means = [_mean(fold) for fold in folds]
        fold_log_means = [_mean(fold) for fold in log_folds]
        fold_lcbs: list[float] = []
        for result in (_fold_aware_inference([fold]) for fold in folds):
            fold_lcbs.append(
                result["mean"] - 1.6448536269514722 * result["se"]
                if math.isfinite(result["se"])
                else -MAX_ABS_DAILY_R
            )
        lcb = float(inference["lcb"])
        lane_statistics[lane] = {
            "lane": lane,
            "daily_count": len(values),
            "completed_trade_count": lane_trade_counts[lane],
            "completed_trade_hac_effective_count": trade_power[
                "lane_effective_counts"
            ][lane],
            "hac_effective_days": float(inference["ess"]),
            "attempt_aware_dsr_p_value": float(inference["p"]),
            "primary_hac_dsr_p_value": float(inference["primary_hac_p"]),
            "sensitivity_hac_dsr_p_value": float(inference["sensitivity_hac_p"]),
            "precommitted_block_student_t_p_value": float(
                inference["precommitted_block_diagnostic_p"]
            ),
            "precommitted_primary_block_days": int(
                inference["precommitted_primary_block_days"]
            ),
            "precommitted_primary_block_count": int(
                inference["precommitted_primary_block_count"]
            ),
            "precommitted_sensitivity_block_days": int(
                inference["precommitted_sensitivity_block_days"]
            ),
            "precommitted_sensitivity_block_count": int(
                inference["precommitted_sensitivity_block_count"]
            ),
            "precommitted_minimum_block_count": int(
                inference["precommitted_minimum_block_count"]
            ),
            "precommitted_block_lcb_days": int(inference["lcb_block_days"]),
            "precommitted_block_lcb_count": int(inference["lcb_block_count"]),
            "maximum_supported_block_days": int(
                inference["maximum_supported_block_days"]
            ),
            "maximum_observed_holding_days": int(
                inference["maximum_observed_holding_days"]
            ),
            "maximum_detected_autocorrelation_lag_days": int(
                inference["maximum_detected_autocorrelation_lag_days"]
            ),
            "dependence_operation_count": int(
                inference["dependence_operation_count"]
            ),
            "precommitted_block_operation_count": int(
                inference["precommitted_block_operation_count"]
            ),
            "dependence_geometry_supported": bool(
                inference["dependence_geometry_supported"]
            ),
            "mean_daily_r": _mean(values),
            "worst_fold_mean_daily_r": min(fold_means),
            "newest_fold_mean_daily_r": fold_means[-1],
            "worst_fold_mean_daily_log_growth": min(fold_log_means),
            "newest_fold_mean_daily_log_growth": fold_log_means[-1],
            "worst_fold_hac_lcb_daily_r": min(fold_lcbs),
            "newest_fold_hac_lcb_daily_r": fold_lcbs[-1],
            "tail_cvar_daily_r": _tail_cvar(
                values, float(_generation.policy["tail_fraction"])
            ),
            "max_path_drawdown_r": _max_drawdown(values),
            "mean_daily_log_growth": float(inference["mean"]),
            "dependence_robust_lcb_daily_log_growth": (
                lcb if math.isfinite(lcb) else None
            ),
        }

    declared_candidate_gates = tuple(
        _generation.policy["candidate_admissibility_gate_codes"]
    )
    declared_selection_gates = tuple(
        _generation.policy["selection_admissibility_gate_codes"]
    )
    if declared_selection_gates != ("WITHIN_CELL_ATTEMPT_AWARE_DSR_FAILED",):
        raise _ContractError("SELECTION_GATE_POLICY_IMPLEMENTATION_MISMATCH")
    tier_statistics: dict[str, dict[str, Any]] = {}
    for tier, lanes in _generation.tier_lanes.items():
        lane_stats = [lane_statistics[lane] for lane in lanes]
        base_lane = lane_statistics["BASE"] if _generation.archived else lane_stats[0]
        stressed_stats = [lane_statistics[lane] for lane in lanes if lane != "BASE"] if _generation.archived else []
        required_trades = trade_power["required"] if _generation.archived else trade_power["tier_required"][tier]
        tier_trade_count = min(lane_trade_counts[lane] for lane in lanes)
        each_fold_trade_floor_met = all(
            len(fold["completed_trade_close_utc"]) >= int(_generation.policy["min_completed_trades_per_fold"])
            for lane in lanes for fold in _folds_for_lane(entry["folds"], lane, _generation)
        )
        bootstrap_tier = bootstrap["tier_evidence"][tier]
        raw_p = max(float(stat["attempt_aware_dsr_p_value"]) for stat in lane_stats)
        conservative_lcb_values = [
            stat["dependence_robust_lcb_daily_log_growth"] for stat in lane_stats
        ]
        conservative_lcb = (
            min(float(value) for value in conservative_lcb_values)
            if all(type(value) is float for value in conservative_lcb_values)
            else None
        )
        gate_results = {
            "INSUFFICIENT_DAILY_OBSERVATIONS": base_lane["daily_count"]
            < int(_generation.policy["min_total_days"]),
            "INSUFFICIENT_HAC_EFFECTIVE_DAYS": min(
                float(stat["hac_effective_days"]) for stat in lane_stats
            )
            < float(_generation.policy["min_hac_effective_days"]),
            "INSUFFICIENT_DEPENDENCE_BLOCKS": min(
                int(stat["precommitted_minimum_block_count"]) for stat in lane_stats
            )
            < int(_generation.policy["min_dependence_blocks"]),
            "DEPENDENCE_GEOMETRY_UNSUPPORTED": not all(
                bool(stat["dependence_geometry_supported"]) for stat in lane_stats
            ),
            "INSUFFICIENT_COMPLETED_TRADES": tier_trade_count
            < int(_generation.policy["min_completed_trades"]) or not each_fold_trade_floor_met,
            "INSUFFICIENT_COMPLETED_TRADE_HAC_POWER": trade_power[
                "tier_effective_counts"
            ][tier]
            < float(required_trades),
            "NO_STOCHASTIC_SAMPLING_SUPPORT": bool(
                _generation.policy["stochastic_sampling_support_required"]
            )
            and any(
                not math.isfinite(float(lane_inferences[lane]["se"]))
                or float(lane_inferences[lane]["se"]) <= 0.0
                for lane in lanes
            ),
            "NONPOSITIVE_MEAN": bool(
                _generation.policy["overall_mean_must_be_positive"]
            )
            and float(base_lane["mean_daily_log_growth"]) <= 0.0,
            "NONPOSITIVE_WORST_FOLD": bool(
                _generation.policy["worst_fold_log_growth_must_be_positive"]
            )
            and float(base_lane["worst_fold_mean_daily_log_growth"]) <= 0.0,
            "NONPOSITIVE_NEWEST_FOLD": bool(
                _generation.policy["newest_fold_log_growth_must_be_positive"]
            )
            and float(base_lane["newest_fold_mean_daily_log_growth"]) <= 0.0,
            "LADDER_LANE_NONPOSITIVE_MEAN": bool(
                _generation.policy["ladder_lane_mean_must_be_positive"]
            )
            and any(
                float(stat["mean_daily_log_growth"]) <= 0.0
                for stat in stressed_stats
            ),
            "LADDER_LANE_NONPOSITIVE_WORST_FOLD": bool(
                _generation.policy[
                    "ladder_lane_worst_fold_log_growth_must_be_positive"
                ]
            )
            and any(
                float(stat["worst_fold_mean_daily_log_growth"]) <= 0.0
                for stat in stressed_stats
            ),
            "LADDER_LANE_NONPOSITIVE_NEWEST_FOLD": bool(
                _generation.policy[
                    "ladder_lane_newest_fold_log_growth_must_be_positive"
                ]
            )
            and any(
                float(stat["newest_fold_mean_daily_log_growth"]) <= 0.0
                for stat in stressed_stats
            ),
            "MOVING_BLOCK_BOOTSTRAP_INSUFFICIENT_EVIDENCE": bootstrap_tier[
                "status"
            ]
            == "INSUFFICIENT_EVIDENCE",
            "MOVING_BLOCK_BOOTSTRAP_LOWER_BOUND_FAILED": bootstrap_tier["status"]
            == "FAIL",
        }
        if tuple(gate_results) != declared_candidate_gates:
            raise _ContractError("CANDIDATE_GATE_POLICY_IMPLEMENTATION_MISMATCH")
        failures = [code for code, failed in gate_results.items() if failed]
        if raw_p > float(_generation.policy["within_cell_alpha"]):
            failures.append(declared_selection_gates[0])
        failures = sorted(set(failures))
        tier_statistics[tier] = {
            "tier": tier,
            "lanes": list(lanes),
            "admissible": not failures,
            "admissibility_failures": failures,
            "attempt_aware_dsr_p_value": raw_p,
            "within_cell_adjusted_p_value": raw_p,
            "hac_effective_days": min(
                float(stat["hac_effective_days"]) for stat in lane_stats
            ),
            "completed_trade_hac_effective_count": trade_power[
                "tier_effective_counts"
            ][tier],
            "completed_trade_required_effective_count": required_trades,
            _PRIMARY_UTILITY_FIELD: conservative_lcb,
        }
        if tier_statistics[tier]["admissible"] and (
            type(conservative_lcb) is not float or not math.isfinite(conservative_lcb)
        ):
            raise _ContractError("ADMISSIBLE_PRIMARY_UTILITY_INVALID")

    if _generation.policy["per_fold_hac_lcb_gate"] is not False:
        raise _ContractError("PER_FOLD_HAC_LCB_POLICY_INVALID")
    return {
        **identity,
        "daily_count": next(iter(lane_statistics.values()))["daily_count"],
        "completed_trade_count": completed_trades,
        "completed_trade_overlap_component_count": trade_power[
            "overlap_components"
        ],
        "completed_trade_bound_required_effective_count": trade_power[
            "bound_required"
        ],
        "completed_trade_derived_required_effective_count": trade_power[
            "derived_required"
        ],
        "lane_statistics": lane_statistics,
        "tier_statistics": tier_statistics,
        "moving_block_bootstrap_evidence": bootstrap,
    }

def _raw_pair_inference(
    left: dict[str, Any],
    right: dict[str, Any],
    entries: list[dict[str, Any]],
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    left_folds = entries[left["ordinal"]]["folds"]
    right_folds = entries[right["ordinal"]]["folds"]
    lane_statistics: dict[str, dict[str, float]] = {}
    for lane, field in _generation.lane_fields.items():
        pair_holding_days = [
            max(_derived_holding_days(left_fold), _derived_holding_days(right_fold))
            for left_fold, right_fold in zip(
                _folds_for_lane(left_folds, lane, _generation),
                _folds_for_lane(right_folds, lane, _generation), strict=True)
        ]
        differences = [
            [
                _daily_log_growth_from_r(float(left_value), _generation=_generation)
                - _daily_log_growth_from_r(float(right_value), _generation=_generation)
                for left_value, right_value in zip(
                    left_fold[field], right_fold[field], strict=True
                )
            ]
            for left_fold, right_fold in zip(left_folds, right_folds, strict=True)
        ]
        left_inference = _dependence_robust_inference(
            differences,
            None,
            1,
            holding_days_override=pair_holding_days,
            _generation=_generation,
        )
        right_inference = _dependence_robust_inference(
            [[-value for value in values] for values in differences],
            None,
            1,
            holding_days_override=pair_holding_days,
            _generation=_generation,
        )
        two_sided = min(
            1.0,
            2.0 * min(float(left_inference["p"]), float(right_inference["p"])),
        )
        lane_statistics[lane] = {
            "paired_mean_difference_daily_log_growth": float(
                left_inference["mean"]
            ),
            "paired_hac_effective_days": min(
                float(left_inference["ess"]),
                float(right_inference["ess"]),
            ),
            "paired_precommitted_sensitivity_block_days": max(
                float(left_inference["precommitted_sensitivity_block_days"]),
                float(right_inference["precommitted_sensitivity_block_days"]),
            ),
            "raw_one_sided_left_superior_p": float(left_inference["p"]),
            "raw_one_sided_right_superior_p": float(right_inference["p"]),
            "raw_two_sided_difference_p": two_sided,
        }
    return {
        "primary_cell_risk_fraction": PRIMARY_CELL_RISK_FRACTION,
        "lane_statistics": lane_statistics,
    }


_PRIMARY_UTILITY_FIELD = "conservative_primary_utility_lcb_daily_log_growth"


def _tier_primary_utility_better(
    left: dict[str, Any],
    right: dict[str, Any],
    tier: str,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> bool:
    declared_fields = tuple(_generation.policy["selection_dominance_fields"])
    if declared_fields != (_PRIMARY_UTILITY_FIELD,):
        raise _ContractError("PRIMARY_SELECTION_DIMENSION_POLICY_MISMATCH")
    left_value = left["tier_statistics"][tier].get(_PRIMARY_UTILITY_FIELD)
    right_value = right["tier_statistics"][tier].get(_PRIMARY_UTILITY_FIELD)
    if (
        type(left_value) is not float
        or type(right_value) is not float
        or not math.isfinite(left_value)
        or not math.isfinite(right_value)
    ):
        raise _ContractError("PRIMARY_SELECTION_UTILITY_INVALID")
    return left_value > right_value


def _simplicity_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    return (
        candidate["strategy_family"],
        candidate["strategy_id"],
        candidate["process_candidate_id"],
    )


def _reporting_candidate(
    candidates: list[dict[str, Any]],
    tier: str,
) -> dict[str, Any]:
    return min(
        candidates,
        key=lambda value: (
            value["tier_statistics"][tier]["within_cell_adjusted_p_value"],
            _simplicity_key(value),
        ),
    )


def _tier_pair_view(
    raw: dict[str, Any],
    tier: str,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    lanes = _generation.tier_lanes[tier]
    members = [raw["lane_statistics"][lane] for lane in lanes]
    return {
        "tier": tier,
        "lanes": list(lanes),
        "lane_statistics": {lane: raw["lane_statistics"][lane] for lane in lanes},
        "paired_hac_effective_days": min(
            float(member["paired_hac_effective_days"]) for member in members
        ),
        "paired_precommitted_sensitivity_block_days": max(
            float(member["paired_precommitted_sensitivity_block_days"])
            for member in members
        ),
        "raw_one_sided_left_superior_p": max(
            float(member["raw_one_sided_left_superior_p"]) for member in members
        ),
        "raw_one_sided_right_superior_p": max(
            float(member["raw_one_sided_right_superior_p"]) for member in members
        ),
        "raw_two_sided_difference_p": max(
            float(member["raw_two_sided_difference_p"]) for member in members
        ),
    }


def _resolve_tier(
    *,
    tier: str,
    admissible: list[dict[str, Any]],
    raw_pairs: dict[tuple[str, str], dict[str, Any]],
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    reporting = _reporting_candidate(admissible, tier)
    unordered_pairs: list[
        tuple[dict[str, Any], dict[str, Any], dict[str, Any]]
    ] = []
    for left_index, left in enumerate(admissible):
        for right in admissible[left_index + 1 :]:
            key = (
                left["process_candidate_id"],
                right["process_candidate_id"],
            )
            unordered_pairs.append(
                (
                    left,
                    right,
                    _tier_pair_view(raw_pairs[key], tier, _generation=_generation),
                )
            )
    two_sided_adjusted = (
        _holm_adjust(
            [pair[2]["raw_two_sided_difference_p"] for pair in unordered_pairs]
        )
        if unordered_pairs
        else []
    )
    directional_raw = [
        value
        for _, _, pair in unordered_pairs
        for value in (
            pair["raw_one_sided_left_superior_p"],
            pair["raw_one_sided_right_superior_p"],
        )
    ]
    directional_adjusted = _holm_adjust(directional_raw) if directional_raw else []
    comparisons: list[dict[str, Any]] = []
    dominated: set[str] = set()
    cursor = 0
    for pair_index, (left, right, pair) in enumerate(unordered_pairs):
        left_p = directional_adjusted[cursor]
        right_p = directional_adjusted[cursor + 1]
        cursor += 2
        left_dominates = _tier_primary_utility_better(
            left, right, tier, _generation=_generation
        ) and left_p <= float(_generation.policy["paired_alpha"])
        right_dominates = _tier_primary_utility_better(
            right, left, tier, _generation=_generation
        ) and right_p <= float(_generation.policy["paired_alpha"])
        if left_dominates:
            dominated.add(right["process_candidate_id"])
        if right_dominates:
            dominated.add(left["process_candidate_id"])
        comparisons.append(
            {
                "left_process_candidate_id": left["process_candidate_id"],
                "right_process_candidate_id": right["process_candidate_id"],
                **pair,
                "holm_adjusted_two_sided_difference_p": two_sided_adjusted[pair_index],
                "holm_adjusted_left_superior_p": left_p,
                "holm_adjusted_right_superior_p": right_p,
                "left_primary_utility_dominates": left_dominates,
                "right_primary_utility_dominates": right_dominates,
            }
        )
    nondominated = [
        candidate
        for candidate in admissible
        if candidate["process_candidate_id"] not in dominated
    ]
    if len(nondominated) == 1:
        winner = nondominated[0]
        reporting = winner
        status = "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
        reasons = ["UNIQUE_SIGNIFICANT_PRIMARY_UTILITY_NONDOMINATED_PROCESS"]
    else:
        survivor_ids = {
            candidate["process_candidate_id"] for candidate in nondominated
        }
        unresolved_difference = any(
            comparison["left_process_candidate_id"] in survivor_ids
            and comparison["right_process_candidate_id"] in survivor_ids
            and comparison["holm_adjusted_two_sided_difference_p"]
            <= float(_generation.policy["paired_alpha"])
            for comparison in comparisons
        )
        if unresolved_difference:
            winner = None
            reporting = _reporting_candidate(nondominated, tier)
            status = "UNRESOLVED_NONDOMINATED_SET"
            reasons = ["STATISTICALLY_DIFFERENT_PRIMARY_UTILITY_ORDER_CONFLICT"]
        else:
            winner = min(nondominated, key=_simplicity_key)
            reporting = winner
            status = "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
            reasons = [
                "PRIMARY_UTILITY_DIFFERENCE_UNRESOLVED_DETERMINISTIC_IDENTITY_TIEBREAK"
            ]
    decision = {
        "status": status,
        "reason_codes": reasons,
        "admitted_candidate_count": len(admissible),
        "winner_process_candidate_id": (
            winner["process_candidate_id"] if winner is not None else None
        ),
        "reporting_process_candidate_id": reporting["process_candidate_id"],
        "paired_comparisons": comparisons,
    }
    return decision, winner, reporting


def _pair_operation_diagnostic(
    candidates: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, int]:
    pair_count = len(candidates) * (len(candidates) - 1) // 2
    if not candidates:
        per_direction = 0
    else:
        per_direction = max(
            _dependence_operation_count(
                [len(fold[field]) for fold in entries[candidate["ordinal"]]["folds"]],
                [_derived_holding_days(fold) for fold in _folds_for_lane(entries[candidate["ordinal"]]["folds"], lane, _generation)],
            )
            for candidate in (candidates[:1] if _generation.archived else candidates)
            for lane, field in _generation.lane_fields.items()
        )
    return {
        "admissible_candidate_count": len(candidates),
        "pair_count": pair_count,
        "per_direction_dependence_operations": per_direction,
        "expected_dependence_operations": 2 * len(_generation.lane_fields) * pair_count * per_direction,
        "operation_limit": int(
            _generation.policy["pairwise_dependence_operation_limit"]
        ),
    }


def _select_process(
    attempts: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    shared_axis: list[dict[str, Any]],
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> tuple[
    str,
    list[str],
    dict[str, Any] | None,
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    _validate_primary_policy_contract(_generation=_generation)
    pbo_evidence = _cell_pbo_evidence(
        attempts, entries, shared_axis, _generation=_generation
    )
    summaries = [
        _candidate_statistics(attempt, entry, len(attempts), _generation=_generation)
        for attempt, entry in zip(attempts, entries, strict=True)
    ]
    p75_tier = "P75_REQUIRED"
    p95_tier = "P95_PREFERENCE"
    p75_admitted = [
        summary
        for summary in summaries
        if summary["tier_statistics"][p75_tier]["admissible"]
    ]
    # Admission and resolution remain distinct facts in both generations.
    p95_admitted = [
        summary
        for summary in p75_admitted
        if summary["tier_statistics"][p95_tier]["admissible"]
    ]
    p95_admitted_count = len(p95_admitted)
    reporting_pool = [
        summary
        for summary in summaries
        if summary["evidence_status"] == "AUTHENTICATED"
    ] or summaries
    reporting = _reporting_candidate(reporting_pool, p75_tier)

    empty_p95 = {
        "status": "NOT_EVALUATED",
        "reason_codes": ["P75_REQUIRED_TIER_DID_NOT_RESOLVE"],
        "admitted_candidate_count": p95_admitted_count,
        "winner_process_candidate_id": None,
        "reporting_process_candidate_id": None,
        "paired_comparisons": [],
    }
    if not p75_admitted:
        p75_decision = {
            "status": "NO_PROCESS_WINNER",
            "reason_codes": ["NO_P75_ADMISSIBLE_PROCESS_CANDIDATE"],
            "admitted_candidate_count": 0,
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": reporting["process_candidate_id"],
            "paired_comparisons": [],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": empty_p95,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        return (
            p75_decision["status"],
            p75_decision["reason_codes"],
            None,
            reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    cell_gate_codes = tuple(_generation.policy["cell_admissibility_gate_codes"])
    if cell_gate_codes != (
        "CELL_PBO_INSUFFICIENT_EVIDENCE",
        "CELL_PBO_EXCEEDS_PRECOMMITTED_LIMIT",
    ):
        raise _ContractError("CELL_GATE_POLICY_IMPLEMENTATION_MISMATCH")
    p75_pbo = pbo_evidence["tier_evidence"][p75_tier]
    if not p75_pbo["passed"]:
        reason = (
            cell_gate_codes[0]
            if p75_pbo["status"] == "INSUFFICIENT_EVIDENCE"
            else cell_gate_codes[1]
        )
        p75_decision = {
            "status": "NO_PROCESS_WINNER",
            "reason_codes": [reason],
            "admitted_candidate_count": len(p75_admitted),
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": _reporting_candidate(
                p75_admitted, p75_tier
            )["process_candidate_id"],
            "paired_comparisons": [],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": empty_p95,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        return (
            p75_decision["status"],
            p75_decision["reason_codes"],
            None,
            _reporting_candidate(p75_admitted, p75_tier),
            summaries,
            ladder,
            pbo_evidence,
        )

    p75_operations = _pair_operation_diagnostic(
        p75_admitted, entries, _generation=_generation
    )
    if (
        p75_operations["expected_dependence_operations"]
        > p75_operations["operation_limit"]
    ):
        reporting = _reporting_candidate(p75_admitted, p75_tier)
        p75_decision = {
            "status": "UNRESOLVED_NONDOMINATED_SET",
            "reason_codes": [
                "P75_PAIRWISE_DEPENDENCE_PRODUCT_EXCEEDS_BOUNDED_EVALUATION"
            ],
            "admitted_candidate_count": len(p75_admitted),
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": reporting["process_candidate_id"],
            "paired_comparisons": [p75_operations],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": empty_p95,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        return (
            p75_decision["status"],
            p75_decision["reason_codes"],
            None,
            reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    raw_pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for left_index, left in enumerate(p75_admitted):
        for right in p75_admitted[left_index + 1 :]:
            key = (
                left["process_candidate_id"],
                right["process_candidate_id"],
            )
            raw_pairs[key] = _raw_pair_inference(
                left, right, entries, _generation=_generation
            )
    p75_decision, p75_winner, p75_reporting = _resolve_tier(
        tier=p75_tier,
        admissible=p75_admitted,
        raw_pairs=raw_pairs,
        _generation=_generation,
    )
    if p75_winner is None:
        p75_decision["reason_codes"] = [
            "P75_" + reason for reason in p75_decision["reason_codes"]
        ]
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": empty_p95,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        return (
            p75_decision["status"],
            p75_decision["reason_codes"],
            None,
            p75_reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    if p95_admitted_count == 0:
        p95_decision = {
            "status": "NOT_EVALUATED_ZERO_ADMITTED",
            "reason_codes": ["P95_ZERO_ADMITTED_P75_FALLBACK"],
            "admitted_candidate_count": 0,
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": None,
            "paired_comparisons": [],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": p95_decision,
            "selected_tier": p75_tier,
            "p95_admitted_candidate_count": 0,
            "p75_p95_winner_diverged": None,
        }
        return (
            p75_decision["status"],
            ["P95_ZERO_ADMITTED_P75_FALLBACK"],
            p75_winner,
            p75_reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    p95_pbo = pbo_evidence["tier_evidence"][p95_tier]
    if not p95_pbo["passed"]:
        reason = (
            "P95_CELL_PBO_INSUFFICIENT_EVIDENCE"
            if p95_pbo["status"] == "INSUFFICIENT_EVIDENCE"
            else "P95_CELL_PBO_EXCEEDS_PRECOMMITTED_LIMIT"
        )
        p95_reporting = _reporting_candidate(p95_admitted, p95_tier)
        p95_decision = {
            "status": "NO_PROCESS_WINNER",
            "reason_codes": [reason],
            "admitted_candidate_count": p95_admitted_count,
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": p95_reporting["process_candidate_id"],
            "paired_comparisons": [],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": p95_decision,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        if not _generation.archived:
            ladder["selected_tier"] = p75_tier
            return (
                p75_decision["status"],
                ["NO_QUALIFIED_P95_WINNER_P75_FALLBACK"],
                p75_winner,
                p75_reporting,
                summaries,
                ladder,
                pbo_evidence,
            )
        return (
            p95_decision["status"],
            p95_decision["reason_codes"],
            None,
            p95_reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    p95_operations = _pair_operation_diagnostic(
        p95_admitted, entries, _generation=_generation
    )
    if (
        p95_operations["expected_dependence_operations"]
        > p95_operations["operation_limit"]
    ):
        p95_reporting = _reporting_candidate(p95_admitted, p95_tier)
        p95_decision = {
            "status": "UNRESOLVED_NONDOMINATED_SET",
            "reason_codes": [
                "P95_PAIRWISE_DEPENDENCE_PRODUCT_EXCEEDS_BOUNDED_EVALUATION"
            ],
            "admitted_candidate_count": p95_admitted_count,
            "winner_process_candidate_id": None,
            "reporting_process_candidate_id": p95_reporting["process_candidate_id"],
            "paired_comparisons": [p95_operations],
        }
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": p95_decision,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        if not _generation.archived:
            ladder["selected_tier"] = p75_tier
            return (
                p75_decision["status"],
                ["NO_QUALIFIED_P95_WINNER_P75_FALLBACK"],
                p75_winner,
                p75_reporting,
                summaries,
                ladder,
                pbo_evidence,
            )
        return (
            p95_decision["status"],
            p95_decision["reason_codes"],
            None,
            p95_reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    if not _generation.archived:
        for left_index, left in enumerate(p95_admitted):
            for right in p95_admitted[left_index + 1 :]:
                key = (left["process_candidate_id"], right["process_candidate_id"])
                if key not in raw_pairs:
                    raw_pairs[key] = _raw_pair_inference(
                        left, right, entries, _generation=_generation
                    )

    p95_decision, p95_winner, p95_reporting = _resolve_tier(
        tier=p95_tier,
        admissible=p95_admitted,
        raw_pairs=raw_pairs,
        _generation=_generation,
    )
    if p95_winner is None:
        p95_decision["reason_codes"] = [
            "P95_" + reason for reason in p95_decision["reason_codes"]
        ]
        ladder = {
            "required_tier": p75_tier,
            "preference_tier": p95_tier,
            "p75_required_decision": p75_decision,
            "p95_preference_decision": p95_decision,
            "selected_tier": None,
            "p95_admitted_candidate_count": p95_admitted_count,
            "p75_p95_winner_diverged": None,
        }
        if not _generation.archived:
            ladder["selected_tier"] = p75_tier
            return (
                p75_decision["status"],
                ["NO_QUALIFIED_P95_WINNER_P75_FALLBACK"],
                p75_winner,
                p75_reporting,
                summaries,
                ladder,
                pbo_evidence,
            )
        return (
            p95_decision["status"],
            p95_decision["reason_codes"],
            None,
            p95_reporting,
            summaries,
            ladder,
            pbo_evidence,
        )

    diverged = p75_winner["process_candidate_id"] != p95_winner["process_candidate_id"]
    ladder = {
        "required_tier": p75_tier,
        "preference_tier": p95_tier,
        "p75_required_decision": p75_decision,
        "p95_preference_decision": p95_decision,
        "selected_tier": p95_tier,
        "p95_admitted_candidate_count": p95_admitted_count,
        "p75_p95_winner_diverged": diverged,
    }
    return (
        p95_decision["status"],
        p95_decision["reason_codes"],
        p95_winner,
        p95_reporting,
        summaries,
        ladder,
        pbo_evidence,
    )


@dataclass(frozen=True, slots=True)
class WinnerSelectionDecision:
    """Deterministic local result for offline Book selection.

    ``receipt_sha256`` is a reproducibility checksum, not a signature or a
    standalone promotion capability.  The frozen dataclass prevents accidental
    mutation in normal code; it is not claimed as an adversarial Python trust
    boundary.
    """

    _body_json: bytes
    _receipt_sha256: str

    def _body(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def status(self) -> str:
        return self._body()["status"]

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(self._body()["reason_codes"])

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    @property
    def selection_scope(self) -> str:
        return self._body()["selection_scope"]

    @property
    def winner_process_candidate_id(self) -> str | None:
        winner = self._body().get("winner")
        return winner.get("process_candidate_id") if type(winner) is dict else None

    def to_dict(self) -> dict[str, Any]:
        body = self._body()
        body["receipt_sha256"] = self._receipt_sha256
        return body


def _make_decision(body: dict[str, Any]) -> WinnerSelectionDecision:
    """Construct the canonical local result and reproducibility checksum."""

    encoded = _canonical_bytes(body)
    if len(encoded) > MAX_DECISION_BYTES:
        raise _ContractError("DECISION_BYTE_LIMIT_EXCEEDED")
    return WinnerSelectionDecision(encoded, _sha256_bytes(encoded))


def _rejected(
    reason: str,
    request_sha: str | None = None,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> WinnerSelectionDecision:
    safe_reason = (
        reason
        if type(reason) is str and _SAFE_ID.fullmatch(reason) is not None
        else "UNCLASSIFIED_REJECTION"
    )
    return _make_decision(
        {
            "contract_version": _generation.selection_version,
            "selection_scope": WINNER_SELECTION_SCOPE,
            "status": "REJECTED",
            "validated": False,
            "reason_codes": [safe_reason],
            "request_contract_version": _generation.request_version,
            "request_sha256": request_sha,
            "precommitment_payload_sha256": None,
            "attempt_ledger_payload_sha256": None,
            "evidence_ledger_payload_sha256": None,
            "historical_verdict_at_utc": None,
            "latest_evidence_boundary_utc": None,
            "evidence_access_started_at_utc": None,
            "evidence_access_completed_at_utc": None,
            "evidence_access_delay_seconds": None,
            "cell": None,
            "attempted_trials_exact": 0,
            "effective_trials": 0.0,
            "winner": None,
            "reporting_candidate": None,
            "candidate_summaries": [],
            "cost_ladder_selection": None,
            "cell_pbo_evidence": None,
            "historical_book_marginal_receipt": None,
            "evidence_scope": "PROCESS_LEVEL_ONLY",
            "parameter_evidence_inherited": False,
            "live_authorized": False,
            "capital_sizing_authorized": False,
            "profitability_claimed": False,
            "production_integrated": False,
        }
    )


def _evaluate_body(
    request: WinnerSelectionRequest,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> dict[str, Any]:
    _generation = _generation_for_precommitment(request.precommitment.to_dict())
    verification_time = datetime.now(timezone.utc)
    (
        policy,
        policy_sha,
        attempt_payload,
        attempt_sha,
        evidence_payload,
        evidence_sha,
        request_sha,
    ) = _request_payloads(request, _generation=_generation)
    (
        policy_time,
        cell,
        planned_attempts,
        fold_plan,
        latest_evidence_boundary,
    ) = _validate_precommitment(policy, _generation=_generation)
    frozen_at, attempts = _validate_attempt_ledger(
        attempt_payload,
        policy,
        policy_sha,
        policy_time,
        cell,
        planned_attempts,
        _generation=_generation,
    )
    evidence_time, entries, shared_axis = _validate_evidence_ledger(
        evidence_payload,
        policy,
        policy_sha,
        attempt_sha,
        frozen_at,
        cell,
        attempts,
        fold_plan,
        latest_evidence_boundary,
        verification_time,
        request_sha,
        _generation=_generation,
    )
    (
        status,
        reasons,
        winner,
        reporting,
        summaries,
        ladder_selection,
        pbo_evidence,
    ) = _select_process(attempts, entries, shared_axis, _generation=_generation)
    selected_tier = ladder_selection["selected_tier"]
    marginal_p = (
        float(
            winner["tier_statistics"][selected_tier][
                "within_cell_adjusted_p_value"
            ]
        )
        if winner is not None
        and selected_tier in _generation.tier_lanes
        and status == "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
        else 1.0
    )
    marginal = {
        "contract_version": _generation.marginal_version,
        "cell_id": cell[0],
        "p_value_contract_sha256": PINNED_BOOK_P_VALUE_CONTRACT_SHA256 if _generation.archived else CURRENT_BOOK_P_VALUE_CONTRACT_SHA256,
        "process_sha256": reporting["process_sha256"],
        "evidence_candidate_id": reporting["process_candidate_id"],
        "source_evidence_sha256": evidence_sha,
        "marginal_p_value": marginal_p,
        "p_value_kind": "ONE_SIDED_SUPERIORITY",
        "selected_cost_ladder_tier": selected_tier,
        "validated_cost_ladder_tolerance": (
            (dict(PINNED_VALIDATED_COST_LADDER_TOLERANCE) if _generation.archived else {
                "schema_version": "fxpm.winner-frozen-spread-qualification-cost.v1",
                "tier": "P75" if selected_tier == "P75_REQUIRED" else "P95",
                "frozen_tier_pair_contract": policy["frozen_tier_pair_contract"],
                "frozen_tier_pair_contract_sha256": policy["frozen_tier_pair_contract_sha256"],
            })
            if winner is not None
            else None
        ),
        "within_cell_multiplicity_adjusted": True,
        "within_cell_attempted_trials_exact": len(attempts),
        "effective_trials": float(len(attempts)),
        "evidence_scope": "PROCESS_LEVEL_ONLY",
        "parameter_evidence_inherited": False,
        "local_admissibility_passed": winner is not None,
    }
    return {
        "contract_version": _generation.selection_version,
        "selection_scope": WINNER_SELECTION_SCOPE,
        "status": status,
        "validated": True,
        "reason_codes": reasons,
        "request_contract_version": _generation.request_version,
        "request_sha256": request_sha,
        "precommitment_payload_sha256": policy_sha,
        "attempt_ledger_payload_sha256": attempt_sha,
        "evidence_ledger_payload_sha256": evidence_sha,
        "historical_verdict_at_utc": evidence_time.isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "latest_evidence_boundary_utc": latest_evidence_boundary.isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "evidence_access_started_at_utc": evidence_payload[
            "evidence_access_started_at_utc"
        ],
        "evidence_access_completed_at_utc": evidence_payload[
            "evidence_access_completed_at_utc"
        ],
        "evidence_access_delay_seconds": int(
            (
                _timestamp(
                    evidence_payload["evidence_access_started_at_utc"],
                    "EVIDENCE_ACCESS_START_INVALID",
                )
                - latest_evidence_boundary
            ).total_seconds()
        ),
        "cell": {
            "cell_id": cell[0],
            "symbol": cell[1],
            "timeframe": cell[2],
            "regime": cell[3],
        },
        "attempted_trials_exact": len(attempts),
        "effective_trials": float(len(attempts)),
        "winner": winner,
        "reporting_candidate": reporting,
        "candidate_summaries": summaries,
        "cost_ladder_selection": ladder_selection,
        "cell_pbo_evidence": pbo_evidence,
        "historical_book_marginal_receipt": marginal,
        "evidence_scope": "PROCESS_LEVEL_ONLY",
        "parameter_evidence_inherited": False,
        "live_authorized": False,
        "capital_sizing_authorized": False,
        "profitability_claimed": False,
        "production_integrated": False,
    }


def _verify_historical_decision_core(
    decision: Any,
    request: Any,
    *,
    _generation: _ContractGeneration = ARCHIVED_GENERATION,
) -> bool:
    try:
        _generation = _generation_for_precommitment(request.precommitment.to_dict())
        if type(decision) is not WinnerSelectionDecision:
            return False
        expected_body = _evaluate_body(request, _generation=_generation)
        body_bytes = decision._body_json
        if type(body_bytes) is not bytes or len(body_bytes) > MAX_DECISION_BYTES:
            return False
        if type(
            decision._receipt_sha256
        ) is not str or decision._receipt_sha256 != _sha256_bytes(body_bytes):
            return False
        body = _owned_json(json.loads(body_bytes.decode("ascii")), MAX_DECISION_BYTES)
        if body.get("status") == "REJECTED" or body.get("validated") is not True:
            return False
        if body.get("selection_scope") != WINNER_SELECTION_SCOPE:
            return False
        if any(
            body.get(field) is not False
            for field in (
                "live_authorized",
                "capital_sizing_authorized",
                "profitability_claimed",
                "production_integrated",
            )
        ):
            return False
        if body.get("request_contract_version") != _generation.request_version:
            return False
        if (
            body.get("request_sha256") != expected_body["request_sha256"]
            or body.get("precommitment_payload_sha256")
            != expected_body["precommitment_payload_sha256"]
            or body.get("attempt_ledger_payload_sha256")
            != expected_body["attempt_ledger_payload_sha256"]
            or body.get("evidence_ledger_payload_sha256")
            != expected_body["evidence_ledger_payload_sha256"]
        ):
            return False
        return _canonical_bytes(expected_body) == body_bytes
    except Exception:
        return False


def evaluate_historical_cell_winner(
    request: Any) -> WinnerSelectionDecision:
    """Calculate a historical cell result for offline Book selection only."""

    _generation = ARCHIVED_GENERATION
    request_sha = (
        request.request_sha256
        if type(request) is WinnerSelectionRequest
        and type(getattr(request, "request_sha256", None)) is str
        else None
    )
    try:
        _generation = _generation_for_precommitment(request.precommitment.to_dict())
        return _make_decision(_evaluate_body(request, _generation=_generation))
    except _ContractError as exc:
        return _rejected(str(exc), request_sha, _generation=_generation)
    except Exception:
        return _rejected(
            "EVALUATION_FAILED_CLOSED", request_sha, _generation=_generation
        )


def verify_historical_cell_winner_decision(decision: Any, request: Any) -> bool:
    """Replay a local result against the exact canonical input request."""

    return _verify_historical_decision_core(decision, request)


def historical_book_marginal_receipt(
    decision: Any, request: Any
) -> dict[str, Any] | None:
    """Adapt an exact replayed result to Book; grant no live/capital authority."""

    try:
        if not _verify_historical_decision_core(decision, request):
            return None
        body = _owned_json(
            json.loads(decision._body_json.decode("ascii")),
            MAX_DECISION_BYTES,
        )
        marginal = body["historical_book_marginal_receipt"]
        return {
            **marginal,
            "upstream_evidence_receipt_sha256": decision._receipt_sha256,
        }
    except Exception:
        return None


def pinned_policy_snapshot() -> dict[str, Any]:
    try:
        return json.loads(
            _canonical_bytes(dict(ARCHIVED_GENERATION.policy)).decode("ascii")
        )
    except Exception:
        return {}


__all__ = [
    "V16_GENERATION", "V16_TRANSPORT_ENVELOPE", "WinnerTransportEnvelope",
    "MAX_SUPPORTED_WINNER_REQUEST_BYTES", "winner_generation_for_precommitment", "winner_request_byte_limit",
    "check_completed_trade_transport_count", "create_winner_selection_request_v16",
    "ATTEMPT_LEDGER_VERSION",
    "CURRENT_WINNER_SELECTION_CONTRACT_VERSION",
    "CURRENT_WINNER_SELECTION_REQUEST_VERSION",
    "CURRENT_WINNER_BOOK_MARGINAL_CONTRACT_VERSION",
    "CURRENT_PRECOMMITMENT_VERSION",
    "CURRENT_EVIDENCE_LEDGER_VERSION",
    "CURRENT_SHARED_OOS_PARTITION_VERSION",
    "EVIDENCE_LEDGER_VERSION",
    "PINNED_BOOK_P_VALUE_CONTRACT_SHA256",
    "PINNED_SELECTION_POLICY_SHA256",
    "PINNED_VALIDATED_COST_LADDER_TOLERANCE",
    "PRECOMMITMENT_VERSION",
    "WinnerAttemptLedgerPayload",
    "WinnerEvidenceLedgerPayload",
    "WinnerPrecommitmentPayload",
    "WinnerSelectionDecision",
    "WinnerSelectionRequest",
    "WINNER_SELECTION_CONTRACT_VERSION",
    "WINNER_BOOK_MARGINAL_CONTRACT_VERSION",
    "WINNER_SELECTION_REQUEST_VERSION",
    "WINNER_SELECTION_SCOPE",
    "compute_cell_id",
    "create_winner_selection_request",
    "current_policy_snapshot",
    "evaluate_historical_cell_winner",
    "historical_book_marginal_receipt",
    "pinned_policy_snapshot",
    "verify_historical_cell_winner_decision",
]


def current_policy_snapshot() -> dict[str, Any]:
    """Declared independent-account candidate policy; no campaign authority."""
    return json.loads(_canonical_bytes(dict(CURRENT_SELECTION_POLICY)).decode("ascii"))
