"""Deterministic offline replay for historical, causal FXPM process validation.

This module deliberately stops at offline process-validation evidence.  It cannot
authorize a current refit, promote a model, allocate capital, assert profitability,
or enable funded/live trading.  It consumes three exact bounded canonical payloads:
the precommitment, executed historical evidence, and ledger reconciliation.

Decision-v8 retains its historical ``*_receipt_sha256`` output field names for
downstream compatibility.  Each value is the deterministic canonical payload
receipt (the SHA-256 identity of the exact payload), not a signature receipt.

The market clocks inside rolling anchors are intentionally separate from the
real authorization/access/execution clocks.  A process authorized and executed
today may evaluate older, precommitted market segments without pretending that
the code or decision existed at those historical dates.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import (
    Decimal,
    DecimalException,
    InvalidOperation,
    ROUND_HALF_EVEN,
    localcontext,
)
from typing import Any, Final, Mapping, Sequence

PROCESS_AUTHORITY_VERSION: Final = "fxpm-offline-process-validation-v8"
PRECOMMIT_SCHEMA: Final = "fxpm-historical-process-precommit-v8"
EVIDENCE_SCHEMA: Final = "fxpm-historical-process-evidence-v8"
RECONCILIATION_SCHEMA: Final = "fxpm-historical-process-reconciliation-v8"
DECISION_SCHEMA: Final = "fxpm-offline-process-validation-decision-v8"
FORWARD_OOS_REPLAY_LINEAGE_SCHEMA: Final = "fxpm.causal-forward-oos-replay-lineage.v1"
FORWARD_OOS_ANCHOR_LINEAGE_SCHEMA: Final = "fxpm.causal-forward-oos-anchor-lineage.v1"
PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION: Final = (
    "fxpm.forward-oos-replay-producer.v3"
)
PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA: Final = "fxpm.forward-oos-replay-precommit.v3"
PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA: Final = (
    "fxpm.forward-oos-replay-producer-receipt.v3"
)
PINNED_AUTHORITY_ARTIFACT_DESCRIPTOR_SCHEMA: Final = (
    "fxpm.authority-spool-artifact-descriptor.v1"
)
PINNED_AUTHORITY_CANONICAL_JSON_FORMAT: Final = "canonical-json"

SYMBOLS: Final = (
    "AUDUSD",
    "EURUSD",
    "USDCAD",
    "USDJPY",
    "AU200",
    "AUDCAD",
    "AUDCHF",
    "AUDJPY",
    "AUDNZD",
    "BCHUSD",
    "BTCETH",
    "BTCUSD",
    "CADCHF",
    "CADJPY",
    "CHFJPY",
    "DE30",
    "DOGUSD",
    "ES35",
    "ETHUSD",
    "EU50",
    "EURAUD",
    "EURCAD",
    "EURCHF",
    "EURGBP",
    "EURJPY",
    "EURNZD",
    "EURZAR",
    "FR40",
    "GBPAUD",
    "GBPCAD",
    "GBPCHF",
    "GBPJPY",
    "GBPNZD",
    "GBPUSD",
    "GBPZAR",
    "HK50",
    "JP225",
    "LTCUSD",
    "NZDCAD",
    "NZDCHF",
    "NZDJPY",
    "NZDUSD",
    "SOLUSD",
    "TONUSD",
    "TRXUSD",
    "UK100",
    "US100",
    "US30",
    "US500",
    "USDCHF",
    "USDMXN",
    "USDNOK",
    "USDPLN",
    "USDSEK",
    "USDSGD",
    "USDZAR",
    "XAGUSD",
    "XAUUSD",
    "XBRUSD",
    "XNGUSD",
    "XRPUSD",
    "XTIUSD",
    "Platinum",
    "USDTHB",
    "Palladium",
    "XAGEUR",
    "XAUAUD",
    "XAUEUR",
    "XAUGBP",
    "AIG",
    "ALCOA",
    "ALIBABA",
    "AMAZON",
    "APPLE",
    "ATT",
    "BAC",
    "CISCO",
    "CITIGROUP",
    "COCA-COLA",
    "DISNEY",
    "EBAY",
    "FORD",
    "GE",
    "GM",
    "GOOGLE",
    "HP",
    "IBM",
    "INTEL",
    "JPM",
    "MASTERCARD",
    "MCDONALDS",
    "MICROSOFT",
    "NETFLIX",
    "NIKE",
    "NVIDIA",
    "ORACLE",
    "PEPSICO",
    "PFIZER",
    "TESLA",
    "VISA",
)
TIMEFRAMES: Final = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES: Final = ("TREND", "RANGE", "BREAKOUT", "CHOP")
CELL_COUNT: Final = 100 * 6 * 4
REGIME_UNOBSERVED: Final = "REGIME_UNOBSERVED"
INSUFFICIENT_EVIDENCE: Final = "INSUFFICIENT_EVIDENCE"
NO_LOCAL_WINNER: Final = "NO_LOCAL_WINNER"
TERMINAL_TECHNICAL: Final = "TERMINAL_TECHNICAL"
TERMINAL_TECHNICAL_SOURCE_STATE: Final = "AUTHENTIC_TERMINAL_TECHNICAL"
TERMINAL_TECHNICAL_REASON: Final = "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION: Final = "fxpm.book-multiplicity.v13"
PINNED_WINNER_SELECTION_CONTRACT_VERSION: Final = "fxpm.winner-selection.v13"
PINNED_WINNER_EVIDENCE_LEDGER_VERSION: Final = "fxpm.winner-evidence-ledger.v4"
PINNED_REGIME_FIT_CONTRACT_VERSION: Final = "fxpm.historical-regime-fit.v4"
REGIME_DETECTOR_RECORD_SCHEMA_VERSION: Final = "fxpm.regime-detector-record/v1"
REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION: Final = (
    "fxpm.regime-detector-ledger-record/v1"
)
REGIME_TRAIN_SUPPORT_SCHEMA_VERSION: Final = "fxpm.regime-train-support/v1"
REGIME_TRAIN_SUPPORT_VECTOR_ENCODING: Final = "fxpm.regime-live-labels-nul/v1"
REGIME_TRAIN_SUPPORT_WARMUP_POLICY: Final = "CURRENT_AND_SOURCE_BAR_NOT_WARMUP"
INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION: Final = (
    "fxpm.historical-insufficient-evidence-binding.v2"
)
HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION: Final = (
    "fxpm.historical-horizon-winner-cross-binding.v1"
)
WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION: Final = (
    "fxpm.winner-raw-evidence-production.v1"
)
INSUFFICIENT_PRESEARCH_BOUNDARY_REASON: Final = "SOURCE_BOUNDARY_SHORTFALL"
INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON: Final = (
    "ALL_PRECOMMITTED_ATTEMPTS_PROCESS_REJECTED"
)
INSUFFICIENT_PRESEARCH_STAGE: Final = "PRE_SEARCH_BOUNDARY"
INSUFFICIENT_POSTSEARCH_STAGE: Final = "POST_SEARCH_ALL_PROCESS_REJECTED"
REGIME_DETECTOR_PARAMETER_KEYS: Final = (
    "k_confirm",
    "gap_min",
    "k_hold",
    "adx_period",
    "atr_period",
    "bb_period",
    "bb_std",
    "efficiency_period",
    "bb_squeeze_lookback",
    "bb_squeeze_percentile",
    "atr_lookback",
    "structure_period",
    "structure_atr_mult",
    "trend_adx_weight",
    "trend_efficiency_weight",
    "trend_slope_weight",
    "range_adx_weight",
    "range_efficiency_weight",
    "range_containment_weight",
    "breakout_squeeze_weight",
    "breakout_atr_weight",
    "breakout_structure_weight",
    "chop_adx_weight",
    "chop_efficiency_weight",
    "chop_whipsaw_weight",
    "adx_trend_threshold",
)
_REGIME_DETECTOR_INTEGER_PARAMETERS: Final = frozenset(
    {
        "k_confirm",
        "k_hold",
        "adx_period",
        "atr_period",
        "bb_period",
        "efficiency_period",
        "bb_squeeze_lookback",
        "atr_lookback",
        "structure_period",
    }
)
REGIME_DETECTOR_INTEGER_MAXIMA: Final = {
    "k_confirm": 64,
    "k_hold": 256,
    "adx_period": 512,
    "atr_period": 512,
    "bb_period": 512,
    "efficiency_period": 512,
    "bb_squeeze_lookback": 10_000,
    "atr_lookback": 10_000,
    "structure_period": 512,
}
REGIME_DETECTOR_WEIGHT_GROUPS: Final = {
    prefix: tuple(
        key
        for key in REGIME_DETECTOR_PARAMETER_KEYS
        if key.startswith(f"{prefix}_") and key.endswith("_weight")
    )
    for prefix in ("trend", "range", "breakout", "chop")
}

COMPONENT_IDENTITY_KEYS: Final = (
    "native_source_contract_code_sha256",
    "activation_contract_code_sha256",
    "continuity_validation_code_sha256",
    "regime_fit_code_sha256",
    "training_horizon_code_sha256",
    "trial_registry_code_sha256",
    "winner_selection_code_sha256",
    "book_multiplicity_code_sha256",
    "specialist_arbitration_code_sha256",
    "institution_state_code_sha256",
    "risk_policy_code_sha256",
    "cost_model_code_sha256",
    "synchronized_replay_code_sha256",
    "reconciliation_authority_code_sha256",
)
POLICY_IDENTITY_KEYS: Final = (
    "native_source_policy_sha256",
    "activation_contract_policy_sha256",
    "continuity_validation_policy_sha256",
    "regime_fit_policy_sha256",
    "training_horizon_policy_sha256",
    "trial_registry_policy_sha256",
    "winner_selection_policy_sha256",
    "book_multiplicity_policy_sha256",
    "specialist_arbitration_policy_sha256",
    "institution_state_policy_sha256",
    "risk_policy_sha256",
    "cost_model_policy_sha256",
    "synchronized_replay_policy_sha256",
    "reconciliation_authority_policy_sha256",
)
ANCHOR_OUTPUT_KEYS: Final = (
    "source_manifest_sha256",
    "activation_receipt_sha256",
    "continuity_receipt_sha256",
    "regime_parameters_sha256",
    "horizon_decisions_sha256",
    "full_attempt_ledger_sha256",
    "winner_decisions_sha256",
    "exact_2400_cell_book_sha256",
    "anchor_refit_execution_sha256",
    "arbitration_execution_sha256",
    "institution_state_execution_sha256",
    "risk_execution_sha256",
    "cost_execution_sha256",
    "synchronized_replay_sha256",
    "reconciliation_input_sha256",
)
LEDGER_IDENTITY_KEYS: Final = (
    "event_ledger_sha256",
    "fill_ledger_sha256",
    "cost_ledger_sha256",
    "exposure_ledger_sha256",
    "open_position_ledger_sha256",
    "calendar_completeness_sha256",
)
CUTOFF_LOCALITY_KEYS: Final = (
    "source_data_end_utc",
    "regime_fit_data_end_utc",
    "horizon_search_data_end_utc",
    "attempt_search_data_end_utc",
    "winner_selection_data_end_utc",
)

_MAX_INPUT_BYTES: Final = 64 * 1024 * 1024
_MAX_INPUT_NODES: Final = 2_000_000
_MAX_STRING_BYTES: Final = 4_096
_MAX_KEY_BYTES: Final = 128
_MAX_INPUT_DEPTH: Final = 64
_MAX_ANCHORS: Final = 16
_MAX_DAYS_PER_ANCHOR: Final = 366
_MAX_TOTAL_DAYS: Final = _MAX_ANCHORS * _MAX_DAYS_PER_ANCHOR
_MAX_VERIFICATION_SECONDS: Final = 60.0
_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_MAX_POSITIONS_PER_ANCHOR: Final = 10_000
_MAX_POSITION_EVENTS_PER_DAY: Final = 10_000

_FROZEN_HOLDING_DAYS: Final = {
    "M5": 2,
    "M15": 3,
    "M30": 4,
    "H1": 7,
    "H4": 14,
    "D1": 30,
}
_FROZEN_OVERLAP_DAYS: Final = 5
_DERIVED_MINIMUM_PURGE_DAYS: Final = (
    max(_FROZEN_HOLDING_DAYS.values()) + _FROZEN_OVERLAP_DAYS
)
_FROZEN_RISK_POLICY: Final = {
    "schema": "fxpm-historical-process-risk-policy-v1",
    "portfolio_return_basis": "SYNCHRONIZED_DAILY_NET_MTM_EQUITY_RETURN",
    "maximum_holding_calendar_days_by_timeframe": _FROZEN_HOLDING_DAYS,
    "execution_overlap_allowance_calendar_days": _FROZEN_OVERLAP_DAYS,
    "minimum_purge_calendar_days": _DERIVED_MINIMUM_PURGE_DAYS,
}
_FROZEN_COST_POLICY: Final = {
    "schema": "fxpm-historical-process-cost-policy-v2",
    "base_lane": "GROSS_MTM_PLUS_OBSERVED_EXECUTION_COST_DEBITS",
    "stress_lane": "SAME_EVENTS_WITH_COST_DEBITS_NOT_LESS_SEVERE_THAN_BASE",
    "require_same_gross_event_stream": True,
    "require_daily_gross_cost_decomposition": True,
}
_FROZEN_REGIME_FIT_POLICY: Final = {
    "schema": "fxpm-historical-regime-fit-policy-v3",
    "fit_unit": "ONE_DECISION_PER_ANCHOR_SYMBOL_TIMEFRAME",
    "regime_fit_contract_version": PINNED_REGIME_FIT_CONTRACT_VERSION,
    "detector_record_schema_version": REGIME_DETECTOR_RECORD_SCHEMA_VERSION,
    "detector_ledger_record_schema_version": (
        REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
    ),
    "exact_detector_parameters_embedded": True,
    "mutable_regime_parameter_sidecar_is_authority": False,
    "required_fit_decision_count_per_anchor": len(SYMBOLS) * len(TIMEFRAMES),
    "cell_reference_rule": ("ALL_FOUR_REGIME_CELLS_REFERENCE_ONE_REGIME_FIT_DECISION"),
    "terminal_source_semantics": "ONE_SHARED_TERMINAL_NO_FIT_DECISION",
    "detector_candidate_roster_rule": (
        "PRECOMMITTED_BEFORE_NESTED_INNER_VALIDATION_ACCESS"
    ),
    "selected_controller_components": "FAMILY_FEATURES_AND_THRESHOLD_POLICY",
    "latent_parameter_fit_inputs": "CAUSAL_MARKET_DATA_AND_MARKET_FEATURES_ONLY",
    "latent_parameter_fit_strategy_utility_permitted": False,
    "controller_identifiability_gate": (
        "MODEL_FAMILY_DERIVED_EFFECTIVE_STATE_OBSERVATIONS_TRANSITIONS_AND_POSTERIOR_SEPARATION"
    ),
    "fixed_state_occupancy_floor_permitted": False,
    "possible_regime_slots_are_forced_observations": False,
    "regime_observation_scope": "EXACT_REGIME_LIVE_TRAIN_SUPPORT_RECEIPT",
    "regime_unobserved_rule": "EXACT_ZERO_TRAIN_LABEL_COUNT_ONLY",
    "regime_unobserved_attempt_count": 0,
    "regime_unobserved_winner_request_permitted": False,
    "expert_holding_horizon_support_veto_permitted": False,
    "rare_regime_no_local_winner_semantics": (
        "POSITIVE_TRAIN_LABEL_COUNT_USES_CELL_LOCAL_NO_LOCAL_WINNER_WITHOUT_GROUP_VETO"
    ),
    "positive_regime_insufficient_evidence_semantics": (
        "AUTHENTICATED_PRECOMMITTED_GEOMETRY_SHORTFALL_IS_COMPLETE_NON_ECONOMIC_ABSTENTION"
    ),
    "controller_selection_scope": "PURGED_TIME_ORDERED_NESTED_INNER_VALIDATION_ONLY",
    "controller_selection_unit": "ONE_SHARED_CONTROLLER_ACROSS_FOUR_REGIME_EXPERTS",
    "controller_selection_utility": ("SYNCHRONIZED_FOUR_EXPERT_POST_COST_LOG_GROWTH"),
    "controller_selection_strategy_utility_permitted": True,
    "controller_selection_outer_oos_permitted": False,
    "controller_selection_calendar": (
        "FULL_SYNCHRONIZED_CALENDAR_INCLUDING_INACTIVE_DAYS_AND_ALL_FOUR_ROUTED_STATES"
    ),
    "collapsed_state_labels_permitted": False,
    "economic_unsupervised_utility_tie_break": "SIMPLER_BASELINE_CONTROLLER",
    "multiplicity_accounting_rule": (
        "ALL_DETECTOR_CANDIDATE_X_EXPERT_TRIAL_ATTEMPTS_IN_FOUR_FULL_ATTEMPT_LEDGERS"
    ),
    "outer_evidence_role": (
        "UNTOUCHED_JOINT_SHARED_CONTROLLER_AND_FOUR_EXPERTS_PROCESS_VALIDATION"
    ),
}
_ONE_SIDED_T_95_BY_DF: Final = {
    3: "2.3533634348018264",
    4: "2.131846786326649",
    5: "2.0150483733330233",
    6: "1.9431802805153022",
    7: "1.894578605061305",
    8: "1.8595480375228424",
    9: "1.8331129326536335",
    10: "1.8124611228107335",
    11: "1.7958848187036691",
    12: "1.782287555649159",
    13: "1.7709333959867988",
    14: "1.7613101357748562",
    15: "1.7530503556925547",
}
_DECIMAL_PATTERN: Final = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d{1,12})?$")
_DECIMAL_CONTEXT_PRECISION: Final = 128
_ECONOMIC_VALUE_BOUND: Final = Decimal("1000000000000000")
_RETURN_QUANTUM: Final = Decimal("0.000000000001")

_PINNED_POLICY: Final = {
    "schema": "fxpm-historical-process-policy-v8",
    "authority_scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
    "book_multiplicity_contract_version": (PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION),
    "winner_selection_contract_version": PINNED_WINNER_SELECTION_CONTRACT_VERSION,
    "winner_evidence_ledger_version": PINNED_WINNER_EVIDENCE_LEDGER_VERSION,
    "forward_oos_production_contract_version": (
        PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION
    ),
    "forward_oos_precommit_schema": PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA,
    "forward_oos_producer_receipt_schema": (PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA),
    "forward_oos_issuer_authenticated_products_required": True,
    "forward_oos_exact_six_role_descriptor_lineage_required": True,
    "regime_fit_contract_version": PINNED_REGIME_FIT_CONTRACT_VERSION,
    "regime_detector_record_schema_version": REGIME_DETECTOR_RECORD_SCHEMA_VERSION,
    "regime_detector_ledger_record_schema_version": (
        REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
    ),
    "validation_target": "FROZEN_FULL_SELECTION_REFIT_ARBITRATION_PROCESS",
    "outer_evidence_geometry": ("REPEATED_LATER_SYNCHRONIZED_AS_OF_FORWARD_ANCHORS"),
    "interior_holdout_permitted_use": ("INNER_SELECTION_ONLY_NEVER_FINAL_CAUSAL_PROOF"),
    "minimum_anchor_count": 4,
    "maximum_anchor_count": _MAX_ANCHORS,
    "minimum_purge_calendar_days": _DERIVED_MINIMUM_PURGE_DAYS,
    "minimum_days_per_anchor": 365,
    "maximum_days_per_anchor": _MAX_DAYS_PER_ANCHOR,
    "maximum_total_days": _MAX_TOTAL_DAYS,
    "completed_trade_vectors_are_activity_diagnostics_only": True,
    "portfolio_inference_unit": "SYNCHRONIZED_DAILY_NET_MTM_EQUITY_RETURN",
    "maximum_holding_calendar_days_by_timeframe": _FROZEN_HOLDING_DAYS,
    "execution_overlap_allowance_calendar_days": _FROZEN_OVERLAP_DAYS,
    "hac_kernel": "BARTLETT",
    "holding_overlap_hac_floor_rule": "MAX_FROZEN_HOLDING_PLUS_OVERLAP_MINUS_ONE",
    "holding_overlap_hac_floor_calendar_days": 34,
    "sample_size_hac_floor_rule": "CEIL_4_TIMES_N_OVER_100_POWER_2_OVER_9",
    "sample_size_hac_floor_reference": "NEWEY_WEST_BARTLETT_RULE_OF_THUMB_CONSERVATIVE_CEILING",
    "primary_hac_bandwidth_rule": "MAX_HOLDING_OVERLAP_FLOOR_AND_SAMPLE_SIZE_FLOOR",
    "sensitivity_hac_bandwidth_multiplier": 2,
    "sensitivity_hac_bandwidth_rule": "MIN_TWO_TIMES_PRIMARY_AND_SHORTEST_ANCHOR_MINUS_ONE",
    "primary_estimand": "SYNCHRONIZED_DAILY_STRESS_NET_EQUITY_LOG_GROWTH",
    "arithmetic_return_role": "DIAGNOSTIC_AND_RISK_ONLY_NEVER_PASS_AUTHORITY",
    "confirmatory_variance_rule": (
        "MAX_PRIMARY_HAC_SENSITIVITY_HAC_ANCHOR_CLUSTER_AND_DAILY_IPS_NEVER_SUM"
    ),
    "confirmatory_gate": "ONE_SIDED_STUDENT_T_STRESS_LOG_GROWTH_LCB_ABOVE_ZERO",
    "confirmatory_null_daily_stress_log_growth": "0",
    "confirmatory_null_units": "LOG_EQUITY_GROWTH_PER_SYNCHRONIZED_UTC_DAY",
    "confirmatory_null_rationale": (
        "POST_TRADING_COST_COMPOUNDING_BREAK_EVEN_NOT_PROFITABILITY_OR_FIXED_OPERATING_COST_SUFFICIENCY"
    ),
    "newest_anchor_gate": "STRESS_LOG_GROWTH_MEAN_ABOVE_ZERO",
    "fixed_hac_role": "VARIANCE_CANDIDATES_AND_DIAGNOSTICS_NOT_SEPARATE_TESTS",
    "daily_ips_rule": "GEYER_INITIAL_POSITIVE_MONOTONE_SEQUENCE_ANCHOR_CENTERED",
    "daily_ips_max_lag": 67,
    "daily_ips_unresolved_state": (
        "OFFLINE_PROCESS_VALIDATION_DEPENDENCE_INSUFFICIENT"
    ),
    "dependence_block_rule": "NONOVERLAPPING_BLOCKS_OF_SENSITIVITY_BANDWIDTH_PLUS_ONE",
    "dependence_block_counts_are_diagnostic_only": True,
    "most_influential_block_rule": "REMOVE_HIGHEST_RETURN_SLIDING_HOLDING_PLUS_LABEL_BLOCK",
    "most_influential_block_calendar_days": _DERIVED_MINIMUM_PURGE_DAYS,
    "most_influential_block_role": "DIAGNOSTIC_ONLY_NEVER_PASS_AUTHORITY",
    "small_cluster_reference": "STUDENT_T_ONE_SIDED_95_PERCENT",
    "one_sided_t_95_by_degrees_of_freedom": {
        str(key): value for key, value in _ONE_SIDED_T_95_BY_DF.items()
    },
    "minimum_distinct_anchor_means": 2,
    "require_positive_within_anchor_variance": True,
    "maximum_base_expected_shortfall_5pct": "0.20",
    "maximum_stress_expected_shortfall_5pct": "0.25",
    "maximum_base_drawdown": "0.50",
    "maximum_stress_drawdown": "0.60",
    "daily_return_absolute_bound": "1",
    "daily_return_maximum_decimal_places": 12,
    "economic_primitive_absolute_bound": "1000000000000000",
    "derived_economic_cash_absolute_bound": "1000000000000000",
    "trusted_clock_max_future_skew_seconds": 300,
    "maximum_input_depth": _MAX_INPUT_DEPTH,
    "maximum_positions_per_anchor": _MAX_POSITIONS_PER_ANCHOR,
    "maximum_position_events_per_day": _MAX_POSITION_EVENTS_PER_DAY,
    "per_anchor_hac_lcb_gate": False,
    "calendar_semantics": "EVERY_UTC_DATE_INCLUSIVE",
    "terminal_source_unavailable_is_economic_state": True,
    "regime_unobserved_is_complete_nontechnical_state": True,
    "regime_unobserved_has_no_specialist_or_refit_leaf": True,
    "insufficient_evidence_binding_schema_version": (
        INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION
    ),
    "horizon_winner_cross_binding_schema_version": (
        HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
    ),
    "postsearch_insufficiency_requires_pre_evidence_bijective_cross_binding": True,
    "postsearch_insufficiency_requires_raw_winner_production_receipt": True,
    "insufficient_evidence_is_complete_nontechnical_state": True,
    "insufficient_evidence_is_non_economic_abstention": True,
    "insufficient_evidence_has_no_specialist_or_refit_leaf": True,
    "insufficient_evidence_does_not_veto_sibling_or_anchor": True,
    "insufficient_presearch_attempt_count": 0,
    "insufficient_postsearch_attempt_count": (
        "EXACT_ALL_PROCESS_REJECTED_ATTEMPT_COUNT"
    ),
    "technical_or_partial_cell_blocks_process": True,
    "current_survivor_relabel_forbidden": True,
    "maximum_input_bytes": _MAX_INPUT_BYTES,
    "maximum_input_nodes": _MAX_INPUT_NODES,
    "maximum_verification_seconds": "60",
}


class ProcessVerificationError(ValueError):
    """Raised when canonical evidence is malformed, non-causal, or incomplete."""


class _EvidenceIdentityRegistry:
    """Single-use namespace for semantic evidence identities.

    Immutable content and equal numeric observations may repeat only in an
    explicitly compatible content domain.  Receipts, decisions, activity
    provenance, execution outputs, reconciliation bindings, and ledgers may not
    reuse one SHA-256 identity for two semantic events.  Canonical aliases of
    the *same* object (for example activity ledger == event ledger, or the
    anchor calendar == calendar-completeness ledger) are verified as equal by
    their owning validator and registered exactly once.
    """

    __slots__ = ("_domains",)

    def __init__(self) -> None:
        self._domains: dict[str, tuple[str, str | None]] = {}

    def register(
        self,
        value: Any,
        domain: str,
        *,
        reusable_content_domain: str | None = None,
    ) -> str:
        identity = _require_hex(value, domain)
        prior = self._domains.get(identity)
        if prior is not None:
            prior_domain, prior_reusable_domain = prior
            if (
                reusable_content_domain is not None
                and reusable_content_domain == prior_reusable_domain
            ):
                return identity
            raise ProcessVerificationError(
                "semantic evidence identity is reused across incompatible domains: "
                f"{prior_domain} -> {domain}"
            )
        self._domains[identity] = (domain, reusable_content_domain)
        return identity


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _require_exact_keys(
    value: Mapping[str, Any], keys: Sequence[str], label: str
) -> None:
    if set(value) != set(keys):
        missing = sorted(set(keys) - set(value))
        extra = sorted(set(value) - set(keys))
        raise ProcessVerificationError(
            f"{label} keys mismatch: missing={missing}, extra={extra}"
        )


def _encoded_json_string_size(value: str) -> int:
    """Return exact canonical JSON bytes for one already-bounded string."""

    return len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"))


def _snapshot_bounded(value: Any) -> Any:
    nodes = 0
    conservative_bytes = 0
    stack = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if depth > _MAX_INPUT_DEPTH:
            raise ProcessVerificationError("input exceeds JSON depth bound")
        if nodes > _MAX_INPUT_NODES:
            raise ProcessVerificationError("input exceeds JSON node bound")
        if type(current) is dict:
            conservative_bytes += 2 + max(0, len(current) - 1) + len(current)
            for key, item in current.items():
                if type(key) is not str:
                    raise ProcessVerificationError(
                        "JSON object keys must be exact strings"
                    )
                key_bytes = len(key.encode("utf-8"))
                if key_bytes > _MAX_KEY_BYTES:
                    raise ProcessVerificationError("JSON object key exceeds byte bound")
                conservative_bytes += _encoded_json_string_size(key)
                stack.append((item, depth + 1))
        elif type(current) is list:
            stack.extend((item, depth + 1) for item in current)
            conservative_bytes += 2 + max(0, len(current) - 1)
        elif type(current) is str:
            size = len(current.encode("utf-8"))
            if size > _MAX_STRING_BYTES:
                raise ProcessVerificationError("JSON string exceeds byte bound")
            conservative_bytes += _encoded_json_string_size(current)
        elif type(current) is int:
            if abs(current) > _MAX_SAFE_INTEGER:
                raise ProcessVerificationError("integer exceeds exact JSON bound")
            conservative_bytes += 24
        elif type(current) is bool or current is None:
            conservative_bytes += 5
        else:
            raise ProcessVerificationError(
                f"unsupported JSON value type: {type(current).__name__}"
            )
        if conservative_bytes > _MAX_INPUT_BYTES:
            raise ProcessVerificationError("input exceeds conservative byte bound")
    try:
        encoded = _canonical_bytes(value)
    except (TypeError, ValueError) as exc:
        raise ProcessVerificationError("input is not canonical JSON") from exc
    if len(encoded) > _MAX_INPUT_BYTES:
        raise ProcessVerificationError("input exceeds canonical byte bound")
    return json.loads(encoded.decode("utf-8"))


def canonical_sha256(value: Mapping[str, Any]) -> str:
    """Return the bounded canonical JSON identity of a mapping."""

    owned = _snapshot_bounded(value)
    if type(owned) is not dict:
        raise ProcessVerificationError("canonical identity requires a mapping")
    return _sha256(owned)


def pinned_process_policy() -> dict[str, Any]:
    """Return an owned copy of the immutable historical process policy."""

    return json.loads(_canonical_bytes(_PINNED_POLICY).decode("utf-8"))


def expected_cell_roster() -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (symbol, timeframe, regime)
        for symbol in SYMBOLS
        for timeframe in TIMEFRAMES
        for regime in REGIMES
    )


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or len(value) != 20 or not value.endswith("Z"):
        raise ProcessVerificationError(f"{label} must be YYYY-MM-DDTHH:MM:SSZ")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ProcessVerificationError(f"{label} is not a valid UTC timestamp") from exc
    return parsed


def _parse_date(value: Any, label: str) -> date:
    if type(value) is not str or len(value) != 10:
        raise ProcessVerificationError(f"{label} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ProcessVerificationError(f"{label} is not a valid date") from exc


def _require_hex(value: Any, label: str) -> str:
    if type(value) is not str or len(value) != 64:
        raise ProcessVerificationError(
            f"{label} must be a lowercase SHA-256 hex digest"
        )
    try:
        bytes.fromhex(value)
    except ValueError as exc:
        raise ProcessVerificationError(f"{label} is not hexadecimal") from exc
    if value != value.lower():
        raise ProcessVerificationError(f"{label} must be lowercase")
    return value


def _decimal(
    value: Any, label: str, *, absolute_bound: Decimal | None = None
) -> Decimal:
    if (
        type(value) is not str
        or not value
        or len(value) > 32
        or _DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise ProcessVerificationError(
            f"{label} must be bounded fixed-point decimal text"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ProcessVerificationError(f"{label} is not a decimal") from exc
    if not parsed.is_finite():
        raise ProcessVerificationError(f"{label} must be finite")
    if absolute_bound is not None and abs(parsed) > absolute_bound:
        raise ProcessVerificationError(f"{label} exceeds its absolute bound")
    if parsed <= Decimal("-1"):
        raise ProcessVerificationError(f"{label} must be greater than -1")
    return parsed


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _economic_decimal(value: Any, label: str, *, strictly_positive: bool) -> Decimal:
    parsed = _decimal(value, label, absolute_bound=_ECONOMIC_VALUE_BOUND)
    if parsed < 0 or (strictly_positive and parsed == 0):
        qualifier = "positive" if strictly_positive else "non-negative"
        raise ProcessVerificationError(f"{label} must be {qualifier}")
    return parsed


def _bounded_economic_result(value: Decimal, label: str) -> Decimal:
    if not value.is_finite() or abs(value) > _ECONOMIC_VALUE_BOUND:
        raise ProcessVerificationError(
            f"{label} exceeds the derived economic magnitude bound"
        )
    return value


def _bounded_economic_product(label: str, *values: Decimal) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            result = Decimal(1)
            for value in values:
                result *= value
    except DecimalException as exc:
        raise ProcessVerificationError(
            f"{label} cannot be represented at the pinned precision"
        ) from exc
    return _bounded_economic_result(result, label)


def _bounded_economic_sum(label: str, *values: Decimal) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            result = sum(values, start=Decimal(0))
    except DecimalException as exc:
        raise ProcessVerificationError(
            f"{label} cannot be represented at the pinned precision"
        ) from exc
    return _bounded_economic_result(result, label)


def _economic_ratio(numerator: Decimal, denominator: Decimal, label: str) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            result = numerator / denominator
    except DecimalException as exc:
        raise ProcessVerificationError(
            f"{label} cannot be derived at the pinned precision"
        ) from exc
    if not result.is_finite():
        raise ProcessVerificationError(f"{label} must be finite")
    return result


def _quantized_return(value: Decimal, label: str) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            context.rounding = ROUND_HALF_EVEN
            quantized = value.quantize(_RETURN_QUANTUM)
    except DecimalException as exc:
        raise ProcessVerificationError(
            f"{label} cannot be represented at the pinned precision"
        ) from exc
    bound = Decimal(_PINNED_POLICY["daily_return_absolute_bound"])
    if abs(quantized) > bound or quantized <= Decimal("-1"):
        raise ProcessVerificationError(f"{label} is outside the return bound")
    return quantized


def _quantized_fixed(value: Decimal) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            context.rounding = ROUND_HALF_EVEN
            return value.quantize(_RETURN_QUANTUM)
    except DecimalException as exc:
        raise ProcessVerificationError(
            "derived fixed-point value exceeds pinned numeric capacity"
        ) from exc


def _cash_return(cash: Decimal, equity: Decimal, label: str) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            value = cash / equity
    except DecimalException as exc:
        raise ProcessVerificationError(
            f"{label} cannot be derived from cash and equity"
        ) from exc
    return _quantized_return(value, label)


def _own_payload(
    value: Any,
    *,
    expected_schema: str,
    label: str,
) -> tuple[dict[str, Any], str]:
    """Own one exact built-in payload and return its canonical receipt identity."""

    if type(value) is not dict:
        raise ProcessVerificationError(f"{label} must be an exact built-in dict")
    payload = _snapshot_bounded(value)
    if type(payload) is not dict:  # Defensive: _snapshot_bounded preserves JSON shape.
        raise ProcessVerificationError(f"{label} must be an exact built-in dict")
    if payload.get("schema") != expected_schema:
        raise ProcessVerificationError(f"{label} schema mismatch")
    return payload, _sha256(payload)


def _validate_hash_map(value: Any, keys: Sequence[str], label: str) -> dict[str, str]:
    if type(value) is not dict:
        raise ProcessVerificationError(f"{label} must be an exact mapping")
    _require_exact_keys(value, keys, label)
    return {key: _require_hex(value[key], f"{label}.{key}") for key in keys}


def _validate_forward_oos_replay_lineage(
    value: Any,
    plans: Sequence[Mapping[str, Any]],
    precommit_payload_sha256: str,
) -> dict[str, Any]:
    """Validate the sealed forward producer/precommit identity carried by v8.

    The raw six-ledger objects are reopened by the production adapter before
    this standalone evaluator is called.  The evaluator nevertheless owns the
    exact producer/precommit identities and the ordered per-anchor descriptor
    roster so that disposing of the transient product object cannot erase its
    authority lineage.
    """

    if type(value) is not dict:
        raise ProcessVerificationError(
            "forward OOS replay lineage must be an exact mapping"
        )
    _require_exact_keys(
        value,
        (
            "schema",
            "production_contract_version",
            "producer_receipt_schema_version",
            "producer_receipt_sha256",
            "producer_receipt_descriptor_sha256",
            "forward_precommit_schema_version",
            "forward_precommit_receipt_sha256",
            "forward_precommit_descriptor_sha256",
            "generation_id",
            "generation_context_sha256",
            "native_universe_sha256",
            "config_sha256",
            "historical_precommit_receipt_sha256",
            "historical_run_receipt_sha256",
            "historical_run_payload_sha256",
            "causal_precommit_payload_sha256",
            "code_identity_sha256",
            "deployment_binding_sha256",
            "evidence_access_started_at_utc",
            "execution_started_at_utc",
            "execution_completed_at_utc",
            "reconciled_at_utc",
            "ordered_anchor_ids",
            "ordered_anchor_descriptor_roster_sha256",
            "anchor_descriptor_roster_sha256_by_id",
            "anchor_ledger_descriptors_by_id",
            "ordered_anchor_production_metadata_roster_sha256",
            "anchor_production_metadata_sha256_by_id",
            "anchor_production_metadata_by_id",
            "causal_binding_followup_completed",
            "confers_current_refit_capital_readiness_publication_or_trading_authority",
        ),
        "forward OOS replay lineage",
    )
    if (
        value["schema"] != FORWARD_OOS_REPLAY_LINEAGE_SCHEMA
        or value["production_contract_version"]
        != PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION
        or value["producer_receipt_schema_version"]
        != PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA
        or value["forward_precommit_schema_version"]
        != PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA
        or value["causal_binding_followup_completed"] is not True
        or value[
            "confers_current_refit_capital_readiness_publication_or_trading_authority"
        ]
        is not False
    ):
        raise ProcessVerificationError("forward OOS replay lineage semantics mismatch")
    generation_id = value["generation_id"]
    if type(generation_id) is not str or not generation_id or len(generation_id) > 256:
        raise ProcessVerificationError("forward OOS generation identity invalid")
    for key in (
        "producer_receipt_sha256",
        "producer_receipt_descriptor_sha256",
        "forward_precommit_receipt_sha256",
        "forward_precommit_descriptor_sha256",
        "generation_context_sha256",
        "native_universe_sha256",
        "config_sha256",
        "historical_precommit_receipt_sha256",
        "historical_run_receipt_sha256",
        "historical_run_payload_sha256",
        "causal_precommit_payload_sha256",
        "code_identity_sha256",
        "deployment_binding_sha256",
        "ordered_anchor_descriptor_roster_sha256",
        "ordered_anchor_production_metadata_roster_sha256",
    ):
        _require_hex(value[key], f"forward OOS replay lineage.{key}")
    if (
        value["producer_receipt_descriptor_sha256"] != value["producer_receipt_sha256"]
        or value["forward_precommit_descriptor_sha256"]
        != value["forward_precommit_receipt_sha256"]
        or value["causal_precommit_payload_sha256"] != precommit_payload_sha256
    ):
        raise ProcessVerificationError(
            "forward OOS replay lineage receipt binding mismatch"
        )
    forward_clocks = [
        _parse_utc(value[key], f"forward OOS replay lineage.{key}")
        for key in (
            "evidence_access_started_at_utc",
            "execution_started_at_utc",
            "execution_completed_at_utc",
            "reconciled_at_utc",
        )
    ]
    if forward_clocks != sorted(forward_clocks):
        raise ProcessVerificationError("forward OOS replay lineage clocks reversed")
    anchor_ids = [plan["anchor_id"] for plan in plans]
    if value["ordered_anchor_ids"] != anchor_ids:
        raise ProcessVerificationError("forward OOS replay anchor order mismatch")
    roster = value["anchor_descriptor_roster_sha256_by_id"]
    if type(roster) is not dict or set(roster) != set(anchor_ids):
        raise ProcessVerificationError(
            "forward OOS replay anchor descriptor roster mismatch"
        )
    for anchor_id in anchor_ids:
        _require_hex(
            roster[anchor_id],
            f"forward OOS replay anchor descriptor roster.{anchor_id}",
        )
    descriptor_rows = value["anchor_ledger_descriptors_by_id"]
    descriptor_roles = (
        "EVENT_LEDGER",
        "FILL_LEDGER",
        "MTM_LEDGER",
        "COST_LEDGER",
        "EXPOSURE_LEDGER",
        "MARGIN_LEDGER",
    )
    if type(descriptor_rows) is not dict or set(descriptor_rows) != set(anchor_ids):
        raise ProcessVerificationError(
            "forward OOS replay exact descriptor rows mismatch"
        )
    for ordinal, anchor_id in enumerate(anchor_ids):
        row = descriptor_rows[anchor_id]
        if type(row) is not dict or set(row) != set(descriptor_roles):
            raise ProcessVerificationError(
                "forward OOS replay exact descriptor role roster mismatch"
            )
        normalized: dict[str, Any] = {}
        for role in descriptor_roles:
            descriptor = row[role]
            if type(descriptor) is not dict:
                raise ProcessVerificationError(
                    "forward OOS replay descriptor must be an exact mapping"
                )
            _require_exact_keys(
                descriptor,
                ("schema_version", "relative_path", "sha256", "size_bytes", "format"),
                f"forward OOS replay descriptor {anchor_id}.{role}",
            )
            if (
                descriptor["schema_version"]
                != PINNED_AUTHORITY_ARTIFACT_DESCRIPTOR_SCHEMA
                or descriptor["format"] != PINNED_AUTHORITY_CANONICAL_JSON_FORMAT
                or type(descriptor["relative_path"]) is not str
                or not descriptor["relative_path"]
                or type(descriptor["size_bytes"]) is not int
                or type(descriptor["size_bytes"]) is bool
                or not 1 <= descriptor["size_bytes"] <= 64 * 1024 * 1024
            ):
                raise ProcessVerificationError(
                    "forward OOS replay descriptor semantics mismatch"
                )
            _require_hex(
                descriptor["sha256"],
                f"forward OOS replay descriptor {anchor_id}.{role}.sha256",
            )
            normalized[role] = descriptor
        expected_anchor_root = _sha256(
            {
                "schema": "fxpm.causal-forward-oos-anchor-descriptor-roster.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": ordinal,
                "ledger_descriptors": normalized,
            }
        )
        if roster[anchor_id] != expected_anchor_root:
            raise ProcessVerificationError(
                "forward OOS replay anchor descriptor root mismatch"
            )
    expected_descriptor_root = _sha256(
        [
            {
                "anchor_id": anchor_id,
                "anchor_descriptor_roster_sha256": roster[anchor_id],
            }
            for anchor_id in anchor_ids
        ]
    )
    if value["ordered_anchor_descriptor_roster_sha256"] != expected_descriptor_root:
        raise ProcessVerificationError(
            "forward OOS replay ordered descriptor root mismatch"
        )
    metadata = value["anchor_production_metadata_sha256_by_id"]
    if type(metadata) is not dict or set(metadata) != set(anchor_ids):
        raise ProcessVerificationError(
            "forward OOS replay anchor production metadata roster mismatch"
        )
    for anchor_id in anchor_ids:
        _require_hex(
            metadata[anchor_id],
            f"forward OOS replay anchor production metadata.{anchor_id}",
        )
    metadata_rows = value["anchor_production_metadata_by_id"]
    if type(metadata_rows) is not dict or set(metadata_rows) != set(anchor_ids):
        raise ProcessVerificationError(
            "forward OOS replay exact production metadata rows mismatch"
        )
    for ordinal, anchor_id in enumerate(anchor_ids):
        if type(metadata_rows[anchor_id]) is not dict:
            raise ProcessVerificationError(
                "forward OOS replay production metadata must be an exact mapping"
            )
        expected_metadata = _sha256(
            {
                "schema": "fxpm.causal-forward-oos-anchor-production-metadata.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": ordinal,
                "production_metadata": metadata_rows[anchor_id],
            }
        )
        if metadata[anchor_id] != expected_metadata:
            raise ProcessVerificationError(
                "forward OOS replay anchor production metadata root mismatch"
            )
    expected_metadata_root = _sha256(
        [
            {
                "anchor_id": anchor_id,
                "anchor_production_metadata_sha256": metadata[anchor_id],
            }
            for anchor_id in anchor_ids
        ]
    )
    if (
        value["ordered_anchor_production_metadata_roster_sha256"]
        != expected_metadata_root
    ):
        raise ProcessVerificationError(
            "forward OOS replay ordered production metadata root mismatch"
        )
    return _snapshot_bounded(value)


def _forward_oos_anchor_lineage_sha256(
    lineage: Mapping[str, Any], anchor_id: str
) -> str:
    return _sha256(
        {
            "schema": FORWARD_OOS_ANCHOR_LINEAGE_SCHEMA,
            "producer_receipt_sha256": lineage["producer_receipt_sha256"],
            "forward_precommit_receipt_sha256": lineage[
                "forward_precommit_receipt_sha256"
            ],
            "code_identity_sha256": lineage["code_identity_sha256"],
            "deployment_binding_sha256": lineage["deployment_binding_sha256"],
            "anchor_id": anchor_id,
            "anchor_descriptor_roster_sha256": lineage[
                "anchor_descriptor_roster_sha256_by_id"
            ][anchor_id],
            "anchor_production_metadata_sha256": lineage[
                "anchor_production_metadata_sha256_by_id"
            ][anchor_id],
        }
    )


def _validate_anchor_plan(anchors: Any) -> list[dict[str, Any]]:
    if type(anchors) is not list:
        raise ProcessVerificationError("anchor plan must be a list")
    minimum = _PINNED_POLICY["minimum_anchor_count"]
    if not minimum <= len(anchors) <= _MAX_ANCHORS:
        raise ProcessVerificationError("anchor count is outside pinned bounds")
    normalized: list[dict[str, Any]] = []
    prior_cutoff: datetime | None = None
    prior_end: date | None = None
    seen_ids: set[str] = set()
    for index, anchor in enumerate(anchors):
        if type(anchor) is not dict:
            raise ProcessVerificationError("anchor plan entry must be a mapping")
        _require_exact_keys(
            anchor,
            (
                "anchor_id",
                "selection_cutoff_utc",
                "forward_start_date",
                "forward_end_date",
                "purge_calendar_days",
            ),
            f"anchor plan {index}",
        )
        anchor_id = anchor["anchor_id"]
        if type(anchor_id) is not str or not anchor_id or len(anchor_id) > 128:
            raise ProcessVerificationError("anchor_id is invalid")
        if anchor_id in seen_ids:
            raise ProcessVerificationError("anchor_id is duplicated")
        seen_ids.add(anchor_id)
        cutoff = _parse_utc(anchor["selection_cutoff_utc"], "selection cutoff")
        start = _parse_date(anchor["forward_start_date"], "forward start")
        end = _parse_date(anchor["forward_end_date"], "forward end")
        purge = anchor["purge_calendar_days"]
        if type(purge) is not int or type(purge) is bool:
            raise ProcessVerificationError("purge_calendar_days must be an integer")
        if purge < _PINNED_POLICY["minimum_purge_calendar_days"] or purge > 366:
            raise ProcessVerificationError("anchor purge is outside pinned bounds")
        if start != cutoff.date() + timedelta(days=purge):
            raise ProcessVerificationError(
                "anchor forward segment does not match exact half-open purge geometry"
            )
        if end < start:
            raise ProcessVerificationError(
                "anchor forward segment ends before it starts"
            )
        days = (end - start).days + 1
        if (
            not _PINNED_POLICY["minimum_days_per_anchor"]
            <= days
            <= _MAX_DAYS_PER_ANCHOR
        ):
            raise ProcessVerificationError("anchor day count is outside pinned bounds")
        if prior_cutoff is not None and cutoff <= prior_cutoff:
            raise ProcessVerificationError(
                "anchor selection cutoffs are not strictly ordered"
            )
        if prior_end is not None:
            if start <= prior_end:
                raise ProcessVerificationError("anchor forward segments overlap")
            prior_end_exclusive = datetime.combine(
                prior_end + timedelta(days=1),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            if cutoff <= prior_end_exclusive:
                raise ProcessVerificationError(
                    "rolling anchor cutoff is not strictly after prior OOS end-exclusive"
                )
        prior_cutoff = cutoff
        prior_end = end
        normalized.append(anchor)
    return normalized


def _validate_process_definition(
    value: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if type(value) is not dict:
        raise ProcessVerificationError("process_definition must be a mapping")
    _require_exact_keys(
        value,
        (
            "schema",
            "symbols",
            "timeframes",
            "regimes",
            "cell_count",
            "book_multiplicity_contract_version",
            "winner_selection_contract_version",
            "winner_evidence_ledger_version",
            "regime_fit_contract_version",
            "regime_detector_record_schema_version",
            "regime_detector_ledger_record_schema_version",
            "component_identities",
            "policy_identities",
            "regime_fit_policy",
            "risk_policy",
            "cost_policy",
            "anchor_plan",
            "scope",
            "forbidden_authorities",
        ),
        "process_definition",
    )
    if value["schema"] != "fxpm-frozen-historical-institution-process-v8":
        raise ProcessVerificationError("process definition schema mismatch")
    if type(value["symbols"]) is not list or tuple(value["symbols"]) != SYMBOLS:
        raise ProcessVerificationError(
            "process symbol roster or mixed-case identity is not exact"
        )
    if (
        type(value["timeframes"]) is not list
        or tuple(value["timeframes"]) != TIMEFRAMES
    ):
        raise ProcessVerificationError("process timeframe roster is not exact")
    if type(value["regimes"]) is not list or tuple(value["regimes"]) != REGIMES:
        raise ProcessVerificationError("process regime roster is not exact")
    if type(value["cell_count"]) is not int or value["cell_count"] != CELL_COUNT:
        raise ProcessVerificationError("process cell_count must be exactly 2400")
    if (
        value["book_multiplicity_contract_version"]
        != PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION
        or value["winner_selection_contract_version"]
        != PINNED_WINNER_SELECTION_CONTRACT_VERSION
        or value["winner_evidence_ledger_version"]
        != PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        or value["regime_fit_contract_version"] != PINNED_REGIME_FIT_CONTRACT_VERSION
        or value["regime_detector_record_schema_version"]
        != REGIME_DETECTOR_RECORD_SCHEMA_VERSION
        or value["regime_detector_ledger_record_schema_version"]
        != REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
    ):
        raise ProcessVerificationError(
            "process Winner/Book/regime contract pin mismatch"
        )
    _validate_hash_map(
        value["component_identities"], COMPONENT_IDENTITY_KEYS, "components"
    )
    policies = _validate_hash_map(
        value["policy_identities"], POLICY_IDENTITY_KEYS, "policies"
    )
    if _canonical_bytes(value["regime_fit_policy"]) != _canonical_bytes(
        _FROZEN_REGIME_FIT_POLICY
    ):
        raise ProcessVerificationError(
            "process regime-fit policy is not the frozen nested-selection policy"
        )
    if _canonical_bytes(value["risk_policy"]) != _canonical_bytes(_FROZEN_RISK_POLICY):
        raise ProcessVerificationError("process risk policy is not the frozen policy")
    if _canonical_bytes(value["cost_policy"]) != _canonical_bytes(_FROZEN_COST_POLICY):
        raise ProcessVerificationError("process cost policy is not the frozen policy")
    if policies["risk_policy_sha256"] != _sha256(_FROZEN_RISK_POLICY):
        raise ProcessVerificationError(
            "risk policy identity does not bind the frozen risk policy body"
        )
    if policies["cost_model_policy_sha256"] != _sha256(_FROZEN_COST_POLICY):
        raise ProcessVerificationError(
            "cost policy identity does not bind the frozen cost policy body"
        )
    if policies["regime_fit_policy_sha256"] != _sha256(_FROZEN_REGIME_FIT_POLICY):
        raise ProcessVerificationError(
            "regime-fit policy identity does not bind the frozen nested-selection body"
        )
    if value["scope"] != "OFFLINE_PROCESS_VALIDATION_ONLY":
        raise ProcessVerificationError("process scope exceeds historical validation")
    forbidden = value["forbidden_authorities"]
    expected_forbidden = [
        "CURRENT_REFIT",
        "MODEL_PROMOTION",
        "CAPITAL_ALLOCATION",
        "FUNDED_LIVE_TRADING",
        "LIVE_TRADING",
        "PROFITABILITY_CLAIM",
    ]
    if type(forbidden) is not list or forbidden != expected_forbidden:
        raise ProcessVerificationError(
            "process forbidden-authority declaration is not exact"
        )
    plans = _validate_anchor_plan(value["anchor_plan"])
    return value, plans


def _validate_precommit_payload(
    payload: dict[str, Any],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    datetime,
    datetime,
    datetime,
]:
    _require_exact_keys(
        payload,
        (
            "schema",
            "process_id",
            "authorized_at_utc",
            "evidence_access_not_before_utc",
            "evidence_access_deadline_utc",
            "policy",
            "policy_sha256",
            "process_definition",
            "process_definition_sha256",
        ),
        "precommit payload",
    )
    if type(payload["process_id"]) is not str or not payload["process_id"]:
        raise ProcessVerificationError("process_id is invalid")
    authorized = _parse_utc(payload["authorized_at_utc"], "authorized_at_utc")
    access_not_before = _parse_utc(
        payload["evidence_access_not_before_utc"], "evidence_access_not_before_utc"
    )
    access_deadline = _parse_utc(
        payload["evidence_access_deadline_utc"], "evidence_access_deadline_utc"
    )
    if access_not_before < authorized or access_deadline <= access_not_before:
        raise ProcessVerificationError("precommitted access window is not causal")
    if _canonical_bytes(payload["policy"]) != _canonical_bytes(_PINNED_POLICY):
        raise ProcessVerificationError("precommitted policy is not the pinned policy")
    if payload["policy_sha256"] != _sha256(payload["policy"]) or payload[
        "policy_sha256"
    ] != _sha256(_PINNED_POLICY):
        raise ProcessVerificationError("precommitted policy hash mismatch")
    process, plans = _validate_process_definition(payload["process_definition"])
    if payload["process_definition_sha256"] != _sha256(process):
        raise ProcessVerificationError("precommitted process definition hash mismatch")
    return process, plans, authorized, access_not_before, access_deadline


def _optional_hex(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _require_hex(value, label)


_TRAIN_SUPPORT_KEYS: Final = (
    "schema_version",
    "symbol",
    "timeframe",
    "surface",
    "warmup_policy",
    "label_order",
    "eligible_rows",
    "counts",
    "observed_regimes",
    "unobserved_regimes",
    "support_state_by_regime",
    "first_ts",
    "last_ts",
    "vector_encoding",
    "vector_sha256",
    "bindings",
    "receipt_sha256",
)
_TRAIN_SUPPORT_BINDING_KEYS: Final = (
    "symbol",
    "timeframe",
    "activation_id",
    "activation_manifest_sha256",
    "source_sha256",
    "train_window_sha256",
    "params_sha256",
    "detector_code_sha256",
)
_REGIME_DETECTOR_RECORD_KEYS: Final = (
    "schema_version",
    "symbol",
    "timeframe",
    "source_group_id",
    "source_group_decision_sha256",
    "source_contract_sha256",
    "source_group_evidence_sha256",
    "activation_id",
    "activation_manifest_sha256",
    "native_source_sha256",
    "fit_cutoff_utc",
    "detector_version",
    "detector_code_sha256",
    "parameters_canonical_json",
    "parameters_sha256",
    "record_sha256",
)
_REGIME_DETECTOR_LEDGER_KEYS: Final = (
    "schema_version",
    "regime_fit_contract_version",
    "regime_fit_receipt_sha256",
    "detector_record",
    "detector_record_sha256",
    "ledger_record_sha256",
)


def _validate_regime_detector_parameters(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProcessVerificationError("regime detector parameters must be a mapping")
    _require_exact_keys(
        value, REGIME_DETECTOR_PARAMETER_KEYS, "regime detector parameters"
    )
    for key in REGIME_DETECTOR_PARAMETER_KEYS:
        item = value[key]
        if key in _REGIME_DETECTOR_INTEGER_PARAMETERS:
            if (
                type(item) is not int
                or not 1 <= item <= REGIME_DETECTOR_INTEGER_MAXIMA[key]
            ):
                raise ProcessVerificationError(
                    "regime detector integer parameter is invalid"
                )
        elif type(item) not in {int, float} or not math.isfinite(item):
            raise ProcessVerificationError("regime detector float parameter is invalid")
    unit_keys = (
        "gap_min",
        *REGIME_DETECTOR_WEIGHT_GROUPS["trend"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["range"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["breakout"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["chop"],
    )
    if any(not 0.0 <= float(value[key]) <= 1.0 for key in unit_keys):
        raise ProcessVerificationError("regime detector unit parameter is invalid")
    if any(
        not 0.0 <= float(value[key]) <= 100.0
        for key in ("bb_squeeze_percentile", "adx_trend_threshold")
    ):
        raise ProcessVerificationError("regime detector percent parameter is invalid")
    if not 0.0 < float(value["bb_std"]) <= 10.0:
        raise ProcessVerificationError("regime detector bb_std is invalid")
    if not 0.0 <= float(value["structure_atr_mult"]) <= 10.0:
        raise ProcessVerificationError(
            "regime detector structure multiplier is invalid"
        )
    if any(
        not math.isclose(
            sum(float(value[key]) for key in keys),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        for keys in REGIME_DETECTOR_WEIGHT_GROUPS.values()
    ):
        raise ProcessVerificationError("regime detector weights do not sum to one")
    return value


def _validate_regime_detector_ledger_record(
    value: Any,
    symbol: str,
    timeframe: str,
    cutoff_text: str,
    support: Mapping[str, Any],
) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProcessVerificationError(
            "regime detector ledger record must be a mapping"
        )
    _require_exact_keys(
        value, _REGIME_DETECTOR_LEDGER_KEYS, "regime detector ledger record"
    )
    if (
        value["schema_version"] != REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
        or value["regime_fit_contract_version"] != PINNED_REGIME_FIT_CONTRACT_VERSION
    ):
        raise ProcessVerificationError("regime detector ledger contract mismatch")
    _require_hex(value["regime_fit_receipt_sha256"], "regime-fit receipt")
    record = value["detector_record"]
    if type(record) is not dict:
        raise ProcessVerificationError("regime detector record must be a mapping")
    _require_exact_keys(record, _REGIME_DETECTOR_RECORD_KEYS, "regime detector record")
    if (
        record["schema_version"] != REGIME_DETECTOR_RECORD_SCHEMA_VERSION
        or record["symbol"] != symbol
        or record["timeframe"] != timeframe
        or record["source_group_id"]
        != _sha256(
            {
                "identity_contract": "FXPM_SYMBOL_TIMEFRAME_SOURCE_GROUP_V1",
                "symbol": symbol,
                "timeframe": timeframe,
            }
        )
    ):
        raise ProcessVerificationError("regime detector record identity mismatch")
    for key in (
        "source_group_decision_sha256",
        "source_contract_sha256",
        "source_group_evidence_sha256",
        "activation_manifest_sha256",
        "native_source_sha256",
        "detector_code_sha256",
        "parameters_sha256",
        "record_sha256",
    ):
        _require_hex(record[key], f"regime detector record {key}")
    if type(record["activation_id"]) is not str or not record["activation_id"]:
        raise ProcessVerificationError("regime detector activation identity is invalid")
    if type(record["detector_version"]) is not str or not record["detector_version"]:
        raise ProcessVerificationError("regime detector version is invalid")
    try:
        fit_cutoff = datetime.fromisoformat(record["fit_cutoff_utc"])
        if fit_cutoff.tzinfo is None or fit_cutoff.utcoffset() != timedelta(0):
            raise ValueError
        canonical_fit_cutoff = fit_cutoff.astimezone(timezone.utc).isoformat()
    except Exception as exc:
        raise ProcessVerificationError("regime detector fit cutoff is invalid") from exc
    if record["fit_cutoff_utc"] != canonical_fit_cutoff or fit_cutoff != _parse_utc(
        cutoff_text, "anchor cutoff"
    ):
        raise ProcessVerificationError("regime detector fit cutoff is not anchor-local")
    parameters_text = record["parameters_canonical_json"]
    if type(parameters_text) is not str or not parameters_text.isascii():
        raise ProcessVerificationError("regime detector parameter payload is invalid")
    try:
        parameters = _validate_regime_detector_parameters(json.loads(parameters_text))
    except Exception as exc:
        raise ProcessVerificationError(
            "regime detector parameter payload is invalid"
        ) from exc
    if _canonical_bytes(parameters).decode("utf-8") != parameters_text:
        raise ProcessVerificationError(
            "regime detector parameter payload is not canonical"
        )
    if (
        record["parameters_sha256"]
        != hashlib.sha256(parameters_text.encode("ascii")).hexdigest()
    ):
        raise ProcessVerificationError("regime detector parameter hash mismatch")
    record_body = dict(record)
    record_body.pop("record_sha256")
    if record["record_sha256"] != _sha256(record_body):
        raise ProcessVerificationError("regime detector record hash mismatch")
    bindings = support["bindings"]
    if (
        record["activation_id"] != bindings["activation_id"]
        or record["activation_manifest_sha256"]
        != bindings["activation_manifest_sha256"]
        or record["native_source_sha256"] != bindings["source_sha256"]
        or record["detector_code_sha256"] != bindings["detector_code_sha256"]
        or record["parameters_sha256"] != bindings["params_sha256"]
    ):
        raise ProcessVerificationError("regime detector source or support splice")
    if value["detector_record_sha256"] != record["record_sha256"]:
        raise ProcessVerificationError("regime detector record identity is spliced")
    ledger_body = dict(value)
    ledger_body.pop("ledger_record_sha256")
    if value["ledger_record_sha256"] != _sha256(ledger_body):
        raise ProcessVerificationError("regime detector ledger record hash mismatch")
    _require_hex(value["ledger_record_sha256"], "regime detector ledger record")
    return value


def _validate_train_regime_support(
    value: Any, symbol: str, timeframe: str
) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProcessVerificationError("train regime support must be a mapping")
    _require_exact_keys(value, _TRAIN_SUPPORT_KEYS, "train regime support")
    if (
        value["schema_version"] != REGIME_TRAIN_SUPPORT_SCHEMA_VERSION
        or value["symbol"] != symbol
        or value["timeframe"] != timeframe
        or value["surface"] != "REGIME_LIVE"
        or value["warmup_policy"] != REGIME_TRAIN_SUPPORT_WARMUP_POLICY
        or value["label_order"] != list(REGIMES)
        or value["vector_encoding"] != REGIME_TRAIN_SUPPORT_VECTOR_ENCODING
    ):
        raise ProcessVerificationError("train regime support semantics are invalid")
    raw_counts = value["counts"]
    if type(raw_counts) is not dict or set(raw_counts) != set(REGIMES):
        raise ProcessVerificationError("train regime support counts are invalid")
    counts: dict[str, int] = {}
    for regime in REGIMES:
        count = raw_counts[regime]
        if (
            type(count) is not int
            or type(count) is bool
            or not 0 <= count <= 10_000_000
        ):
            raise ProcessVerificationError("train regime support count is invalid")
        counts[regime] = count
    eligible = value["eligible_rows"]
    if (
        type(eligible) is not int
        or type(eligible) is bool
        or not 0 <= eligible <= 10_000_000
        or sum(counts.values()) != eligible
    ):
        raise ProcessVerificationError("train regime support rows do not reconcile")
    observed = [regime for regime in REGIMES if counts[regime] > 0]
    unobserved = [regime for regime in REGIMES if counts[regime] == 0]
    states = {
        regime: "EMPIRICALLY_OBSERVED" if counts[regime] > 0 else "UNOBSERVED"
        for regime in REGIMES
    }
    if (
        value["observed_regimes"] != observed
        or value["unobserved_regimes"] != unobserved
        or value["support_state_by_regime"] != states
    ):
        raise ProcessVerificationError("train regime support partition is invalid")
    if eligible == 0:
        if value["first_ts"] is not None or value["last_ts"] is not None:
            raise ProcessVerificationError(
                "empty train regime support timestamps are invalid"
            )
    else:
        try:
            first = datetime.fromisoformat(
                str(value["first_ts"]).replace("Z", "+00:00")
            )
            last = datetime.fromisoformat(
                str(value["last_ts"]).replace("Z", "+00:00")
            )
            if first > last:
                raise ValueError
        except Exception as exc:
            raise ProcessVerificationError(
                "train regime support timestamps are invalid"
            ) from exc
    _require_hex(value["vector_sha256"], "train regime support vector")
    bindings = value["bindings"]
    if type(bindings) is not dict:
        raise ProcessVerificationError("train regime support bindings are missing")
    _require_exact_keys(
        bindings, _TRAIN_SUPPORT_BINDING_KEYS, "train regime support bindings"
    )
    if (
        bindings["symbol"] != symbol
        or bindings["timeframe"] != timeframe
        or type(bindings["activation_id"]) is not str
        or not bindings["activation_id"]
    ):
        raise ProcessVerificationError("train regime support bindings are invalid")
    for key in (
        "activation_manifest_sha256",
        "source_sha256",
        "train_window_sha256",
        "params_sha256",
        "detector_code_sha256",
    ):
        _require_hex(bindings[key], f"train regime support {key}")
    receipt = _require_hex(value["receipt_sha256"], "train regime support receipt")
    body = dict(value)
    body.pop("receipt_sha256")
    if receipt != _sha256(body):
        raise ProcessVerificationError("train regime support receipt is invalid")
    return value


_INSUFFICIENT_BINDING_KEYS: Final = (
    "schema_version",
    "terminal_stage",
    "reason_code",
    "selection_cutoff_utc",
    "source_prefix_receipt_sha256",
    "source_prefix_content_sha256",
    "source_prefix_rows",
    "horizon_plan_receipt_sha256",
    "horizon_policy_release_sha256",
    "dependency_policy_receipt_sha256",
    "dependency_geometry_sha256",
    "candidate_registry_sha256",
    "trial_registry_sha256",
    "horizon_attempt_roster_sha256",
    "evidence_roster_sha256",
    "causal_fold_geometry_sha256",
    "causal_fold_count",
    "planned_attempt_count",
    "process_rejected_attempt_count",
    "process_rejection_request_sha256",
    "process_rejection_attempt_ledger_sha256",
    "process_rejection_evidence_ledger_sha256",
    "process_rejection_decision_sha256",
    "horizon_winner_cross_binding_schema_version",
    "horizon_winner_cross_binding_content_sha256",
    "horizon_winner_cross_binding_mapping_sha256",
    "winner_raw_production_schema_version",
    "winner_raw_production_receipt_sha256",
)


def _validate_insufficient_evidence_binding(
    value: Any,
    *,
    cutoff_text: str,
    attempt_count: int,
    horizon_receipt_sha256: Any,
    full_attempt_ledger_sha256: Any,
) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProcessVerificationError(
            "insufficient-evidence binding must be an exact mapping"
        )
    _require_exact_keys(value, _INSUFFICIENT_BINDING_KEYS, "insufficient binding")
    if (
        value["schema_version"] != INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION
        or value["selection_cutoff_utc"] != cutoff_text
        or value["horizon_plan_receipt_sha256"] != horizon_receipt_sha256
    ):
        raise ProcessVerificationError(
            "insufficient-evidence binding identity is inconsistent"
        )
    for key in (
        "source_prefix_receipt_sha256",
        "source_prefix_content_sha256",
        "horizon_plan_receipt_sha256",
        "horizon_policy_release_sha256",
        "dependency_policy_receipt_sha256",
        "dependency_geometry_sha256",
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "horizon_attempt_roster_sha256",
        "evidence_roster_sha256",
        "causal_fold_geometry_sha256",
    ):
        _require_hex(value[key], f"insufficient binding {key}")
    for key, minimum, maximum in (
        ("source_prefix_rows", 1, 10_000_000_000),
        ("causal_fold_count", 0, 256),
        ("planned_attempt_count", 1, 1_000_000),
        ("process_rejected_attempt_count", 0, 1_000_000),
    ):
        raw = value[key]
        if type(raw) is not int or type(raw) is bool or not minimum <= raw <= maximum:
            raise ProcessVerificationError(f"insufficient binding {key} is invalid")
    process_keys = (
        "process_rejection_request_sha256",
        "process_rejection_attempt_ledger_sha256",
        "process_rejection_evidence_ledger_sha256",
        "process_rejection_decision_sha256",
    )
    cross_binding_keys = (
        "horizon_winner_cross_binding_schema_version",
        "horizon_winner_cross_binding_content_sha256",
        "horizon_winner_cross_binding_mapping_sha256",
        "winner_raw_production_schema_version",
        "winner_raw_production_receipt_sha256",
    )
    if value["terminal_stage"] == INSUFFICIENT_PRESEARCH_STAGE:
        if (
            value["reason_code"] != INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
            or attempt_count != 0
            or value["causal_fold_count"] != 0
            or value["process_rejected_attempt_count"] != 0
            or full_attempt_ledger_sha256 is not None
            or any(value[key] is not None for key in process_keys)
            or any(value[key] is not None for key in cross_binding_keys)
        ):
            raise ProcessVerificationError(
                "pre-search insufficient-evidence semantics are invalid"
            )
    elif value["terminal_stage"] == INSUFFICIENT_POSTSEARCH_STAGE:
        if (
            value["reason_code"] != INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
            or value["causal_fold_count"] < 4
            or value["planned_attempt_count"] != attempt_count
            or value["process_rejected_attempt_count"] != attempt_count
            or full_attempt_ledger_sha256
            != value["process_rejection_attempt_ledger_sha256"]
            or value["horizon_winner_cross_binding_schema_version"]
            != HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
            or value["winner_raw_production_schema_version"]
            != WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION
        ):
            raise ProcessVerificationError(
                "post-search insufficient-evidence semantics are invalid"
            )
        for key in process_keys:
            _require_hex(value[key], f"insufficient binding {key}")
        for key in (
            "horizon_winner_cross_binding_content_sha256",
            "horizon_winner_cross_binding_mapping_sha256",
            "winner_raw_production_receipt_sha256",
        ):
            _require_hex(value[key], f"insufficient binding {key}")
    else:
        raise ProcessVerificationError(
            "insufficient-evidence terminal stage is invalid"
        )
    return value


def _validate_cell_book(
    cells: Any,
    support_roster: Any,
    detector_roster: Any,
    cutoff_text: str,
    anchor_id: str,
    identity_registry: _EvidenceIdentityRegistry,
) -> str:
    if type(cells) is not list or len(cells) != CELL_COUNT:
        raise ProcessVerificationError("anchor book must contain exactly 2400 cells")
    if type(support_roster) is not list or len(support_roster) != CELL_COUNT // len(
        REGIMES
    ):
        raise ProcessVerificationError(
            "anchor must contain exactly 600 train regime support slots"
        )
    if type(detector_roster) is not list or len(detector_roster) != CELL_COUNT // len(
        REGIMES
    ):
        raise ProcessVerificationError(
            "anchor must contain exactly 600 regime detector ledger slots"
        )
    supports: list[dict[str, Any] | None] = []
    for group_ordinal, raw_support in enumerate(support_roster):
        symbol_index, timeframe_index = divmod(group_ordinal, len(TIMEFRAMES))
        symbol = SYMBOLS[symbol_index]
        timeframe = TIMEFRAMES[timeframe_index]
        supports.append(
            None
            if raw_support is None
            else _validate_train_regime_support(raw_support, symbol, timeframe)
        )
    detector_records: list[dict[str, Any] | None] = []
    for group_ordinal, raw_record in enumerate(detector_roster):
        symbol_index, timeframe_index = divmod(group_ordinal, len(TIMEFRAMES))
        symbol = SYMBOLS[symbol_index]
        timeframe = TIMEFRAMES[timeframe_index]
        support = supports[group_ordinal]
        if raw_record is None:
            detector_records.append(None)
        elif support is None:
            raise ProcessVerificationError(
                "regime detector record exists without train support"
            )
        else:
            detector_records.append(
                _validate_regime_detector_ledger_record(
                    raw_record, symbol, timeframe, cutoff_text, support
                )
            )
    expected = expected_cell_roster()
    cutoff = _parse_utc(cutoff_text, "cell selection cutoff")
    cell_specific_receipt_keys = (
        "cell_decision_sha256",
        "horizon_receipt_sha256",
        "full_attempt_ledger_sha256",
        "state_decision_receipt_sha256",
        "winner_request_sha256",
        "winner_evidence_ledger_payload_sha256",
        "winner_decision_receipt_sha256",
        "anchor_refit_receipt_sha256",
    )
    for index, cell in enumerate(cells):
        if type(cell) is not dict:
            raise ProcessVerificationError("book cell must be a mapping")
        _require_exact_keys(
            cell,
            (
                "symbol",
                "timeframe",
                "regime",
                "status",
                "search_complete",
                "attempts_terminal",
                "technical_failure",
                "terminal_source_unavailable",
                "attempt_count",
                "selection_as_of_utc",
                "market_data_end_utc",
                "cell_decision",
                "cell_decision_sha256",
            ),
            f"book cell {index}",
        )
        if (cell["symbol"], cell["timeframe"], cell["regime"]) != expected[index]:
            raise ProcessVerificationError(
                "book roster is incomplete, duplicated, or out of order"
            )
        if cell["selection_as_of_utc"] != cutoff_text:
            raise ProcessVerificationError("cell is not an anchor-local as-of decision")
        market_data_end_text = cell["market_data_end_utc"]
        if market_data_end_text is not None and (
            _parse_utc(market_data_end_text, "cell market_data_end_utc") > cutoff
        ):
            raise ProcessVerificationError(
                "cell uses market data after its anchor cutoff"
            )
        if (
            cell["search_complete"] is not True
            or cell["attempts_terminal"] is not True
            or cell["technical_failure"] is not False
        ):
            raise ProcessVerificationError(
                "technical, unsearched, or partial cell blocks process"
            )
        count = cell["attempt_count"]
        if type(count) is not int or type(count) is bool or not 0 <= count <= 1_000_000:
            raise ProcessVerificationError("cell attempt_count is invalid")
        status = cell["status"]
        unavailable = cell["terminal_source_unavailable"]
        decision = cell["cell_decision"]
        if type(decision) is not dict:
            raise ProcessVerificationError("cell decision must be an exact mapping")
        _require_exact_keys(
            decision,
            (
                "schema",
                "anchor_id",
                "symbol",
                "timeframe",
                "regime",
                "selection_cutoff_utc",
                "status",
                "source_state",
                "source_terminal_reason",
                "source_receipt_sha256",
                "regime_receipt_sha256",
                "regime_detector_ledger_record_sha256",
                "train_regime_support_receipt_sha256",
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "state_decision_receipt_sha256",
                "insufficient_evidence_binding",
                "winner_selection_contract_version",
                "winner_evidence_ledger_contract_version",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "training_feature_end_utc",
                "training_label_end_utc",
                "refit_market_data_end_utc",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ),
            "anchor-local cell decision",
        )
        if decision["schema"] != "fxpm-anchor-local-cell-decision-v6":
            raise ProcessVerificationError("anchor-local cell decision schema mismatch")
        if (
            decision["anchor_id"] != anchor_id
            or decision["symbol"] != cell["symbol"]
            or decision["timeframe"] != cell["timeframe"]
            or decision["regime"] != cell["regime"]
            or decision["selection_cutoff_utc"] != cutoff_text
            or decision["status"] != status
        ):
            raise ProcessVerificationError(
                "cell decision is not bound to its anchor, cell, and cutoff"
            )
        decision_sha = _sha256(decision)
        if cell["cell_decision_sha256"] != decision_sha:
            raise ProcessVerificationError(
                "anchor-local cell decision identity mismatch"
            )
        _require_hex(decision["source_receipt_sha256"], "cell source receipt")
        feature_end_text = decision["training_feature_end_utc"]
        label_end_text = decision["training_label_end_utc"]
        refit_end_text = decision["refit_market_data_end_utc"]
        source_state = decision["source_state"]
        source_terminal_reason = decision["source_terminal_reason"]
        insufficient_binding = decision["insufficient_evidence_binding"]
        _require_hex(
            decision["state_decision_receipt_sha256"], "cell state decision receipt"
        )
        support = supports[index // len(REGIMES)]
        detector_record = detector_records[index // len(REGIMES)]
        if status in {"SOURCE_UNAVAILABLE", TERMINAL_TECHNICAL}:
            if (
                support is not None
                or detector_record is not None
                or decision["regime_detector_ledger_record_sha256"] is not None
                or decision["train_regime_support_receipt_sha256"] is not None
            ):
                raise ProcessVerificationError(
                    "source-unavailable cell carries train regime support"
                )
        else:
            if support is None:
                raise ProcessVerificationError(
                    "available cell has no train regime support"
                )
            if detector_record is None:
                raise ProcessVerificationError(
                    "available cell has no immutable regime detector record"
                )
            if (
                decision["train_regime_support_receipt_sha256"]
                != support["receipt_sha256"]
                or decision["regime_detector_ledger_record_sha256"]
                != detector_record["ledger_record_sha256"]
            ):
                raise ProcessVerificationError(
                    "cell train regime support receipt is spliced"
                )
        label_counts = None if support is None else support["counts"]
        regime_label_count = (
            None if label_counts is None else label_counts[cell["regime"]]
        )
        winner_contract = decision["winner_selection_contract_version"]
        winner_ledger_contract = decision["winner_evidence_ledger_contract_version"]
        if status == "WINNER":
            if (
                unavailable is not False
                or count < 1
                or regime_label_count is None
                or regime_label_count < 1
                or source_state != "AVAILABLE"
                or source_terminal_reason is not None
                or insufficient_binding is not None
                or market_data_end_text is None
            ):
                raise ProcessVerificationError(
                    "winner cell terminal state is inconsistent"
                )
            feature_end = _parse_utc(feature_end_text, "winner training feature end")
            label_end = _parse_utc(label_end_text, "winner training label end")
            refit_end = _parse_utc(refit_end_text, "winner refit market-data end")
            if not feature_end <= label_end <= refit_end <= cutoff:
                raise ProcessVerificationError(
                    "winner feature, label, or refit locality is non-causal"
                )
            if market_data_end_text != refit_end_text:
                raise ProcessVerificationError(
                    "winner market-data and refit boundaries differ"
                )
            for key in (
                "regime_receipt_sha256",
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                _require_hex(decision[key], f"winner decision {key}")
            if (
                winner_contract != PINNED_WINNER_SELECTION_CONTRACT_VERSION
                or winner_ledger_contract != PINNED_WINNER_EVIDENCE_LEDGER_VERSION
            ):
                raise ProcessVerificationError("winner contract pin mismatch")
        elif status == NO_LOCAL_WINNER:
            if (
                unavailable is not False
                or count < 1
                or regime_label_count is None
                or regime_label_count < 1
                or source_state != "AVAILABLE"
                or source_terminal_reason is not None
                or insufficient_binding is not None
                or market_data_end_text is None
                or refit_end_text is not None
            ):
                raise ProcessVerificationError(
                    "no-local-winner cell terminal state is inconsistent"
                )
            feature_end = _parse_utc(
                feature_end_text, "no-local-winner training feature end"
            )
            label_end = _parse_utc(label_end_text, "no-local-winner training label end")
            if not feature_end <= label_end <= cutoff:
                raise ProcessVerificationError(
                    "no-local-winner feature or label locality is non-causal"
                )
            if market_data_end_text != label_end_text:
                raise ProcessVerificationError(
                    "no-local-winner market-data and label boundaries differ"
                )
            for key in (
                "regime_receipt_sha256",
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
            ):
                _require_hex(decision[key], f"no-local-winner decision {key}")
            if (
                winner_contract != PINNED_WINNER_SELECTION_CONTRACT_VERSION
                or winner_ledger_contract != PINNED_WINNER_EVIDENCE_LEDGER_VERSION
            ):
                raise ProcessVerificationError("no-local-winner contract pin mismatch")
            for key in (
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "anchor_refit_receipt_sha256",
            ):
                if (
                    _optional_hex(decision[key], f"no-local-winner decision {key}")
                    is not None
                ):
                    raise ProcessVerificationError(
                        "no-local-winner cell carries a selected/refit identity"
                    )
        elif status == INSUFFICIENT_EVIDENCE:
            if (
                unavailable is not False
                or regime_label_count is None
                or regime_label_count < 1
                or source_state != "AVAILABLE"
                or source_terminal_reason is not None
                or market_data_end_text is None
                or refit_end_text is not None
            ):
                raise ProcessVerificationError(
                    "insufficient-evidence cell terminal state is inconsistent"
                )
            feature_end = _parse_utc(
                feature_end_text, "insufficient-evidence training feature end"
            )
            label_end = _parse_utc(
                label_end_text, "insufficient-evidence training label end"
            )
            if not feature_end <= label_end <= cutoff:
                raise ProcessVerificationError(
                    "insufficient-evidence feature or label locality is non-causal"
                )
            if market_data_end_text != label_end_text:
                raise ProcessVerificationError(
                    "insufficient-evidence market-data and label boundaries differ"
                )
            _require_hex(
                decision["regime_receipt_sha256"],
                "insufficient-evidence shared fit receipt",
            )
            _require_hex(
                decision["horizon_receipt_sha256"],
                "insufficient-evidence horizon receipt",
            )
            _validate_insufficient_evidence_binding(
                insufficient_binding,
                cutoff_text=cutoff_text,
                attempt_count=count,
                horizon_receipt_sha256=decision["horizon_receipt_sha256"],
                full_attempt_ledger_sha256=decision["full_attempt_ledger_sha256"],
            )
            for key in (
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                if (
                    _optional_hex(
                        decision[key], f"insufficient-evidence decision {key}"
                    )
                    is not None
                ):
                    raise ProcessVerificationError(
                        "insufficient-evidence cell carries a Winner or refit leaf"
                    )
            if winner_contract is not None or winner_ledger_contract is not None:
                raise ProcessVerificationError(
                    "insufficient-evidence cell carries a Winner contract"
                )
        elif status == REGIME_UNOBSERVED:
            if (
                unavailable is not False
                or count != 0
                or regime_label_count != 0
                or source_state != "AVAILABLE"
                or source_terminal_reason is not None
                or insufficient_binding is not None
                or market_data_end_text is None
                or refit_end_text is not None
            ):
                raise ProcessVerificationError(
                    "unobserved regime state is inconsistent"
                )
            feature_end = _parse_utc(
                feature_end_text, "unobserved-regime training feature end"
            )
            label_end = _parse_utc(
                label_end_text, "unobserved-regime training label end"
            )
            if not feature_end <= label_end <= cutoff:
                raise ProcessVerificationError(
                    "unobserved-regime feature or label locality is non-causal"
                )
            if market_data_end_text != label_end_text:
                raise ProcessVerificationError(
                    "unobserved-regime market-data and label boundaries differ"
                )
            _require_hex(
                decision["regime_receipt_sha256"],
                "unobserved-regime shared fit receipt",
            )
            for key in (
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                if (
                    _optional_hex(decision[key], f"unobserved-regime decision {key}")
                    is not None
                ):
                    raise ProcessVerificationError(
                        "unobserved regime carries search, winner, or refit identity"
                    )
            if winner_contract is not None or winner_ledger_contract is not None:
                raise ProcessVerificationError(
                    "unobserved regime carries a Winner contract"
                )
        elif status == "SOURCE_UNAVAILABLE":
            if (
                unavailable is not True
                or count != 0
                or source_state != "AUTHENTIC_TERMINAL_UNAVAILABLE"
                or source_terminal_reason
                not in (
                    "SYMBOL_NOT_LISTED_AT_ANCHOR",
                    "BROKER_HISTORY_TERMINAL_ABSENCE",
                    "TIMEFRAME_NOT_OFFERED_AT_ANCHOR",
                )
                or market_data_end_text is not None
                or feature_end_text is not None
                or label_end_text is not None
                or refit_end_text is not None
                or insufficient_binding is not None
            ):
                raise ProcessVerificationError(
                    "source-unavailable cell is not conclusively terminal"
                )
            _require_hex(
                decision["regime_receipt_sha256"],
                "source-unavailable shared regime-fit decision receipt",
            )
            for key in (
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                if (
                    _optional_hex(decision[key], f"source-unavailable decision {key}")
                    is not None
                ):
                    raise ProcessVerificationError(
                        "source-unavailable cell carries a downstream identity"
                    )
            if winner_contract is not None or winner_ledger_contract is not None:
                raise ProcessVerificationError(
                    "source-unavailable cell carries a Winner contract"
                )
        elif status == TERMINAL_TECHNICAL:
            if (
                unavailable is not False
                or count != 0
                or source_state != TERMINAL_TECHNICAL_SOURCE_STATE
                or source_terminal_reason != TERMINAL_TECHNICAL_REASON
                or market_data_end_text is not None
                or feature_end_text is not None
                or label_end_text is not None
                or refit_end_text is not None
                or insufficient_binding is not None
            ):
                raise ProcessVerificationError(
                    "terminal-technical cell is not an authenticated final abstention"
                )
            _require_hex(
                decision["regime_receipt_sha256"],
                "terminal-technical shared regime-fit decision receipt",
            )
            for key in (
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                if (
                    _optional_hex(decision[key], f"terminal-technical decision {key}")
                    is not None
                ):
                    raise ProcessVerificationError(
                        "terminal-technical cell carries a downstream identity"
                    )
            if winner_contract is not None or winner_ledger_contract is not None:
                raise ProcessVerificationError(
                    "terminal-technical cell carries a Winner contract"
                )
        else:
            raise ProcessVerificationError(
                "book cell has a non-terminal or technical status"
            )
        # Executable code, trial definitions, and parameter values may
        # legitimately be identical in more than one specialist cell.  They
        # share an immutable-content domain but can never alias an event.
        for key in cell_specific_receipt_keys:
            identity = (
                cell["cell_decision_sha256"]
                if key == "cell_decision_sha256"
                else decision[key]
            )
            if identity is None:
                continue
            identity_registry.register(
                identity,
                f"{anchor_id} {cell['symbol']} {cell['timeframe']} "
                f"{cell['regime']} {key}",
            )
        if status == INSUFFICIENT_EVIDENCE:
            if type(insufficient_binding) is not dict:
                raise ProcessVerificationError(
                    "insufficient-evidence binding vanished after validation"
                )
            for key in (
                "source_prefix_receipt_sha256",
                "process_rejection_request_sha256",
                "process_rejection_evidence_ledger_sha256",
                "process_rejection_decision_sha256",
            ):
                identity = insufficient_binding[key]
                if identity is None:
                    continue
                identity_registry.register(
                    identity,
                    f"{anchor_id} {cell['symbol']} {cell['timeframe']} "
                    f"{cell['regime']} insufficient binding {key}",
                )
        for key in (
            "strategy_code_sha256",
            "trial_definition_sha256",
            "parameters_sha256",
        ):
            identity = decision[key]
            if identity is not None:
                identity_registry.register(
                    identity,
                    f"{anchor_id} {cell['symbol']} {cell['timeframe']} "
                    f"{cell['regime']} {key}",
                    reusable_content_domain=f"specialist content {key}",
                )
    for offset in range(0, CELL_COUNT, len(REGIMES)):
        group = cells[offset : offset + len(REGIMES)]
        symbol = group[0]["symbol"]
        timeframe = group[0]["timeframe"]
        canonical = {
            (
                cell["cell_decision"]["source_state"],
                cell["cell_decision"]["source_terminal_reason"],
                cell["cell_decision"]["source_receipt_sha256"],
                cell["market_data_end_utc"],
            )
            for cell in group
        }
        if len(canonical) != 1:
            raise ProcessVerificationError(
                "source state is not canonical across a symbol-timeframe four-regime group"
            )
        source_state, _, source_receipt, _ = next(iter(canonical))
        identity_registry.register(
            source_receipt,
            f"{anchor_id} {symbol} {timeframe} canonical source receipt",
        )
        regime_fit_receipts = {
            cell["cell_decision"]["regime_receipt_sha256"] for cell in group
        }
        if len(regime_fit_receipts) != 1 or None in regime_fit_receipts:
            raise ProcessVerificationError(
                "four regime cells do not reference one shared RegimeFit decision"
            )
        regime_fit_receipt = next(iter(regime_fit_receipts))
        identity_registry.register(
            regime_fit_receipt,
            f"{anchor_id} {symbol} {timeframe} shared RegimeFit decision",
        )
        detector_record = detector_records[offset // len(REGIMES)]
        if source_state == "AVAILABLE":
            support = supports[offset // len(REGIMES)]
            if support is None or detector_record is None:
                raise ProcessVerificationError(
                    "available group has no train support or detector record"
                )
            inner_record = detector_record["detector_record"]
            if (
                detector_record["regime_fit_receipt_sha256"] != regime_fit_receipt
                or inner_record["source_group_decision_sha256"] != source_receipt
            ):
                raise ProcessVerificationError(
                    "shared regime detector source or fit receipt is spliced"
                )
            counts = support["counts"]
            if any(
                (cell["status"] == REGIME_UNOBSERVED)
                is not (counts[cell["regime"]] == 0)
                for cell in group
            ):
                raise ProcessVerificationError(
                    "regime observed/unobserved states contradict train labels"
                )
            identity_registry.register(
                support["receipt_sha256"],
                f"{anchor_id} {symbol} {timeframe} train regime support",
            )
            identity_registry.register(
                support["vector_sha256"],
                f"{anchor_id} {symbol} {timeframe} REGIME_LIVE train vector",
            )
            identity_registry.register(
                detector_record["ledger_record_sha256"],
                f"{anchor_id} {symbol} {timeframe} regime detector ledger record",
            )
            identity_registry.register(
                inner_record["record_sha256"],
                f"{anchor_id} {symbol} {timeframe} immutable detector record",
            )
            identity_registry.register(
                inner_record["parameters_sha256"],
                f"{anchor_id} {symbol} {timeframe} exact detector parameters",
                reusable_content_domain="regime detector parameters",
            )
        unavailable_statuses = {
            cell["status"] == "SOURCE_UNAVAILABLE" for cell in group
        }
        technical_terminal_statuses = {
            cell["status"] == TERMINAL_TECHNICAL for cell in group
        }
        if source_state == "AUTHENTIC_TERMINAL_UNAVAILABLE":
            if detector_record is not None:
                raise ProcessVerificationError(
                    "terminal source absence carries a detector record"
                )
            if unavailable_statuses != {True}:
                raise ProcessVerificationError(
                    "terminal source absence is inconsistent across four regimes"
                )
            if technical_terminal_statuses != {False}:
                raise ProcessVerificationError(
                    "terminal source absence contains a terminal-technical regime"
                )
        elif source_state == TERMINAL_TECHNICAL_SOURCE_STATE:
            if detector_record is not None:
                raise ProcessVerificationError(
                    "terminal-technical source carries a detector record"
                )
            if technical_terminal_statuses != {True} or unavailable_statuses != {False}:
                raise ProcessVerificationError(
                    "terminal-technical source is inconsistent across four regimes"
                )
        elif unavailable_statuses != {False} or technical_terminal_statuses != {False}:
            raise ProcessVerificationError(
                "available source group contains a terminal source regime"
            )
    return _sha256(
        {
            "cells": cells,
            "train_regime_supports": support_roster,
            "regime_detector_records": detector_roster,
        }
    )


def _lineage_root(
    anchor_id: str,
    cells: Sequence[Mapping[str, Any]],
    *,
    schema: str,
    outer_keys: Sequence[str] = (),
    decision_keys: Sequence[str] = (),
) -> str:
    entries: list[dict[str, Any]] = []
    for cell in cells:
        decision = cell["cell_decision"]
        entry = {
            "symbol": cell["symbol"],
            "timeframe": cell["timeframe"],
            "regime": cell["regime"],
        }
        entry.update({key: cell[key] for key in outer_keys})
        entry.update({key: decision[key] for key in decision_keys})
        entries.append(entry)
    return _sha256({"schema": schema, "anchor_id": anchor_id, "entries": entries})


def _expected_lineage_outputs(anchor: Mapping[str, Any]) -> dict[str, str]:
    anchor_id = anchor["anchor_id"]
    cells = anchor["book_cells"]
    return {
        "source_manifest_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-source-lineage-v1",
            decision_keys=(
                "source_state",
                "source_terminal_reason",
                "source_receipt_sha256",
            ),
        ),
        "activation_receipt_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-activation-lineage-v1",
            outer_keys=(
                "status",
                "search_complete",
                "attempts_terminal",
                "technical_failure",
                "terminal_source_unavailable",
            ),
        ),
        "continuity_receipt_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-continuity-lineage-v1",
            outer_keys=("market_data_end_utc",),
            decision_keys=(
                "training_feature_end_utc",
                "training_label_end_utc",
                "refit_market_data_end_utc",
            ),
        ),
        "regime_parameters_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-regime-lineage-v3",
            decision_keys=(
                "regime_receipt_sha256",
                "regime_detector_ledger_record_sha256",
                "train_regime_support_receipt_sha256",
            ),
        ),
        "horizon_decisions_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-horizon-lineage-v1",
            decision_keys=("horizon_receipt_sha256",),
        ),
        "full_attempt_ledger_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-attempt-lineage-v1",
            outer_keys=("attempt_count",),
            decision_keys=("full_attempt_ledger_sha256",),
        ),
        "winner_decisions_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-winner-lineage-v2",
            decision_keys=(
                "status",
                "winner_selection_contract_version",
                "winner_evidence_ledger_contract_version",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_decision_receipt_sha256",
            ),
        ),
        "anchor_refit_execution_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-refit-lineage-v1",
            decision_keys=(
                "refit_market_data_end_utc",
                "anchor_refit_receipt_sha256",
            ),
        ),
        "arbitration_execution_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-arbitration-lineage-v1",
            decision_keys=(
                "status",
                "strategy_code_sha256",
                "parameters_sha256",
                "winner_decision_receipt_sha256",
            ),
        ),
        "institution_state_execution_sha256": _lineage_root(
            anchor_id,
            cells,
            schema="fxpm-anchor-state-lineage-v1",
            decision_keys=("status", "state_decision_receipt_sha256"),
        ),
    }


def _calendar(start: date, end: date) -> list[str]:
    count = (end - start).days + 1
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _vector_hash(dates: Sequence[str], values: Sequence[str], lane: str) -> str:
    return _sha256({"lane": lane, "dates": list(dates), "daily_values": list(values)})


def _calendar_hash(dates: Sequence[str]) -> str:
    return _sha256(
        {"calendar_semantics": "EVERY_UTC_DATE_INCLUSIVE", "dates": list(dates)}
    )


def _validate_activity_ledger(
    anchor: Mapping[str, Any],
    dates: Sequence[str],
    gross: Sequence[Decimal],
    base_cost: Sequence[Decimal],
    stress_cost: Sequence[Decimal],
    identity_registry: _EvidenceIdentityRegistry | None = None,
    used_broker_fill_ids: set[str] | None = None,
) -> tuple[
    list[Decimal],
    dict[str, int],
    dict[str, str],
    dict[str, Any],
    str,
]:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            context.rounding = ROUND_HALF_EVEN
            registry = identity_registry or _EvidenceIdentityRegistry()
            return _validate_activity_ledger_in_context(
                anchor,
                dates,
                gross,
                base_cost,
                stress_cost,
                registry,
                used_broker_fill_ids,
            )
    except DecimalException as exc:
        raise ProcessVerificationError(
            "activity-ledger arithmetic exceeds pinned numeric capacity"
        ) from exc


def _validate_activity_ledger_in_context(
    anchor: Mapping[str, Any],
    dates: Sequence[str],
    gross: Sequence[Decimal],
    base_cost: Sequence[Decimal],
    stress_cost: Sequence[Decimal],
    identity_registry: _EvidenceIdentityRegistry,
    used_broker_fill_ids: set[str] | None = None,
) -> tuple[
    list[Decimal],
    dict[str, int],
    dict[str, str],
    dict[str, Any],
    str,
]:
    ledger = anchor["activity_ledger"]
    if type(ledger) is not dict:
        raise ProcessVerificationError("activity ledger must be an exact mapping")
    _require_exact_keys(
        ledger,
        ("schema", "initial_positions", "days"),
        "activity ledger",
    )
    if ledger["schema"] != "fxpm-anchor-economic-position-activity-ledger-v2":
        raise ProcessVerificationError("activity ledger schema mismatch")
    initial_positions = ledger["initial_positions"]
    day_records = ledger["days"]
    if type(initial_positions) is not list or type(day_records) is not list:
        raise ProcessVerificationError("activity positions and days must be lists")
    if len(initial_positions) > _MAX_POSITIONS_PER_ANCHOR:
        raise ProcessVerificationError("initial position count exceeds hard bound")
    if len(day_records) != len(dates):
        raise ProcessVerificationError("activity ledger day count is incomplete")

    winner_timeframes = {
        cell["cell_decision_sha256"]: cell["timeframe"]
        for cell in anchor["book_cells"]
        if cell["status"] == "WINNER"
    }
    opening_primitive_keys = (
        "position_id",
        "cell_decision_sha256",
        "timeframe",
        "opened_at_utc",
        "side",
        "quantity",
        "contract_multiplier",
        "stop_price",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    opening_identity_keys = (
        "opening_fill_payload_sha256",
        "opening_fill_receipt_sha256",
    )
    carried_mark_keys = (
        "forward_window_open_mark_price",
        "forward_window_open_mark_payload_sha256",
        "forward_window_open_mark_receipt_sha256",
    )
    closing_primitive_keys = (
        "position_id",
        "closed_at_utc",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    closing_identity_keys = (
        "closing_fill_payload_sha256",
        "closing_fill_receipt_sha256",
    )
    broker_fill_ids: set[str] = set()

    def record_provenance_identity(value: Any, label: str) -> str:
        return identity_registry.register(
            value, f"{anchor['anchor_id']} activity {label}"
        )

    def validate_opening_fill(
        value: Any, label: str, *, carried: bool
    ) -> dict[str, Any]:
        if type(value) is not dict:
            raise ProcessVerificationError(f"{label} must be an exact mapping")
        expected_keys = opening_primitive_keys + opening_identity_keys
        if carried:
            expected_keys += carried_mark_keys
        _require_exact_keys(value, expected_keys, label)
        position_id = value["position_id"]
        if (
            type(position_id) is not str
            or not position_id
            or len(position_id.encode("utf-8")) > 128
        ):
            raise ProcessVerificationError(f"{label} position_id is invalid")
        cell_sha = _require_hex(
            value["cell_decision_sha256"], f"{label} specialist cell"
        )
        timeframe = value["timeframe"]
        if timeframe not in TIMEFRAMES or winner_timeframes.get(cell_sha) != timeframe:
            raise ProcessVerificationError(
                f"{label} does not bind an anchor-local winning specialist"
            )
        opened_at = _parse_utc(value["opened_at_utc"], f"{label} opened_at_utc")
        side = value["side"]
        if side not in ("LONG", "SHORT"):
            raise ProcessVerificationError(f"{label} side must be LONG or SHORT")
        side_sign = Decimal(1) if side == "LONG" else Decimal(-1)
        quantity = _economic_decimal(
            value["quantity"], f"{label} quantity", strictly_positive=True
        )
        multiplier = _economic_decimal(
            value["contract_multiplier"],
            f"{label} contract multiplier",
            strictly_positive=True,
        )
        unit_scale = _bounded_economic_product(
            f"{label} position unit scale", quantity, multiplier
        )
        stop_price = _economic_decimal(
            value["stop_price"], f"{label} stop price", strictly_positive=True
        )
        reference_mid = _economic_decimal(
            value["reference_mid_price"],
            f"{label} reference mid price",
            strictly_positive=True,
        )
        reference_bid = _economic_decimal(
            value["reference_bid_price"],
            f"{label} reference bid price",
            strictly_positive=True,
        )
        reference_ask = _economic_decimal(
            value["reference_ask_price"],
            f"{label} reference ask price",
            strictly_positive=True,
        )
        if (
            reference_bid > reference_mid
            or reference_mid > reference_ask
            or reference_bid + reference_ask != Decimal(2) * reference_mid
        ):
            raise ProcessVerificationError(
                f"{label} bid/mid/ask quote is not internally consistent"
            )
        base_fill = _economic_decimal(
            value["base_fill_price"],
            f"{label} base fill price",
            strictly_positive=True,
        )
        stress_fill = _economic_decimal(
            value["stress_fill_price"],
            f"{label} stress fill price",
            strictly_positive=True,
        )
        base_commission = _economic_decimal(
            value["base_commission_cash"],
            f"{label} base commission",
            strictly_positive=False,
        )
        stress_commission = _economic_decimal(
            value["stress_commission_cash"],
            f"{label} stress commission",
            strictly_positive=False,
        )
        base_fee = _economic_decimal(
            value["base_fee_cash"],
            f"{label} base fee",
            strictly_positive=False,
        )
        stress_fee = _economic_decimal(
            value["stress_fee_cash"],
            f"{label} stress fee",
            strictly_positive=False,
        )
        if (side == "LONG" and stop_price >= reference_mid) or (
            side == "SHORT" and stop_price <= reference_mid
        ):
            raise ProcessVerificationError(
                f"{label} stop is not adverse to the opening market"
            )
        broker_fill_id = value["broker_fill_id"]
        if (
            type(broker_fill_id) is not str
            or not broker_fill_id
            or len(broker_fill_id.encode("utf-8")) > 128
            or broker_fill_id in broker_fill_ids
            or (
                used_broker_fill_ids is not None
                and broker_fill_id in used_broker_fill_ids
            )
        ):
            raise ProcessVerificationError(
                f"{label} broker fill identity is invalid or reused"
            )
        broker_fill_ids.add(broker_fill_id)
        if used_broker_fill_ids is not None:
            used_broker_fill_ids.add(broker_fill_id)
        payload_sha = _sha256(
            {
                "schema": "fxpm-position-opening-fill-payload-v2",
                "anchor_id": anchor["anchor_id"],
                "fill": {key: value[key] for key in opening_primitive_keys},
            }
        )
        if value["opening_fill_payload_sha256"] != payload_sha:
            raise ProcessVerificationError(
                f"{label} opening fill payload identity mismatch"
            )
        record_provenance_identity(payload_sha, f"{label} opening payload")
        record_provenance_identity(
            value["opening_fill_receipt_sha256"], f"{label} opening receipt"
        )
        executable_quote = reference_ask if side == "LONG" else reference_bid
        base_spread_cash = _bounded_economic_product(
            f"{label} base spread cash",
            side_sign,
            executable_quote - reference_mid,
            unit_scale,
        )
        base_slippage_cash = _bounded_economic_product(
            f"{label} base slippage cash",
            side_sign,
            base_fill - executable_quote,
            unit_scale,
        )
        stress_slippage_cash = _bounded_economic_product(
            f"{label} stress slippage cash",
            side_sign,
            stress_fill - executable_quote,
            unit_scale,
        )
        base_execution_cost = max(
            Decimal(0),
            _bounded_economic_sum(
                f"{label} base spread and slippage cash",
                base_spread_cash,
                base_slippage_cash,
            ),
        )
        stress_execution_cost = max(
            Decimal(0),
            _bounded_economic_sum(
                f"{label} stress spread and slippage cash",
                base_spread_cash,
                stress_slippage_cash,
            ),
        )
        base_cost_cash = _bounded_economic_sum(
            f"{label} base opening cost cash",
            base_execution_cost,
            base_commission,
            base_fee,
        )
        stress_cost_cash = _bounded_economic_sum(
            f"{label} stress opening cost cash",
            stress_execution_cost,
            stress_commission,
            stress_fee,
        )
        if stress_cost_cash < base_cost_cash:
            raise ProcessVerificationError(
                f"{label} stress execution cost is less severe than base"
            )
        last_mark = reference_mid
        if carried:
            last_mark = _economic_decimal(
                value["forward_window_open_mark_price"],
                f"{label} forward-window opening mark",
                strictly_positive=True,
            )
            expected_mark_payload = _sha256(
                {
                    "schema": "fxpm-forward-window-opening-mark-payload-v1",
                    "anchor_id": anchor["anchor_id"],
                    "position_id": position_id,
                    "forward_start_date": dates[0],
                    "mark_price": value["forward_window_open_mark_price"],
                }
            )
            if (
                value["forward_window_open_mark_payload_sha256"]
                != expected_mark_payload
            ):
                raise ProcessVerificationError(
                    f"{label} forward-window opening mark identity mismatch"
                )
            record_provenance_identity(
                expected_mark_payload, f"{label} forward opening mark payload"
            )
            record_provenance_identity(
                value["forward_window_open_mark_receipt_sha256"],
                f"{label} forward opening mark receipt",
            )
            if (side == "LONG" and stop_price >= last_mark) or (
                side == "SHORT" and stop_price <= last_mark
            ):
                raise ProcessVerificationError(
                    f"{label} carried stop is not adverse at the forward boundary"
                )
        return {
            "position_id": position_id,
            "cell_decision_sha256": value["cell_decision_sha256"],
            "timeframe": timeframe,
            "opened_at": opened_at,
            "opened_on": opened_at.date(),
            "side": side,
            "side_sign": side_sign,
            "quantity": quantity,
            "multiplier": multiplier,
            "unit_scale": unit_scale,
            "stop_price": stop_price,
            "last_mark": last_mark,
            "pending_base_cost_cash": Decimal(0) if carried else base_cost_cash,
            "pending_stress_cost_cash": (Decimal(0) if carried else stress_cost_cash),
            "base_contribution": Decimal(0),
        }

    open_positions: dict[str, dict[str, Any]] = {}
    seen_positions: set[str] = set()
    prior_initial_id = ""
    first_date = _parse_date(dates[0], "first activity date")
    for position in initial_positions:
        state = validate_opening_fill(position, "initial position", carried=True)
        position_id = state["position_id"]
        if position_id <= prior_initial_id:
            raise ProcessVerificationError(
                "initial positions must be unique and sorted by position_id"
            )
        prior_initial_id = position_id
        if state["opened_on"] >= first_date:
            raise ProcessVerificationError(
                "carried position must predate the forward calendar"
            )
        seen_positions.add(position_id)
        open_positions[position_id] = state

    observed_holding = {timeframe: 0 for timeframe in TIMEFRAMES}
    completed_outcomes: list[Decimal] = []
    event_days: list[dict[str, Any]] = []
    fill_days: list[dict[str, Any]] = []
    cost_days: list[dict[str, Any]] = []
    exposure_days: list[dict[str, Any]] = []
    closing_days: list[dict[str, Any]] = []
    maximum_open_risk = Decimal(0)
    maximum_gross_exposure = Decimal(0)
    maximum_concurrent_positions = 0
    for index, (record, date_text) in enumerate(zip(day_records, dates, strict=True)):
        if type(record) is not dict:
            raise ProcessVerificationError("activity day must be an exact mapping")
        _require_exact_keys(
            record,
            ("date", "equity_basis_cash", "entries", "position_marks", "exits"),
            f"activity day {index}",
        )
        if record["date"] != date_text:
            raise ProcessVerificationError(
                "activity day is omitted, duplicated, or reordered"
            )
        entries = record["entries"]
        mark_records = record["position_marks"]
        exits = record["exits"]
        if any(type(value) is not list for value in (entries, mark_records, exits)):
            raise ProcessVerificationError("activity day event lanes must be lists")
        if sum((len(entries), len(mark_records), len(exits))) > (
            _MAX_POSITION_EVENTS_PER_DAY
        ):
            raise ProcessVerificationError(
                "daily position event count exceeds hard bound"
            )
        current_date = _parse_date(date_text, "activity date")
        equity = _economic_decimal(
            record["equity_basis_cash"],
            "daily equity basis",
            strictly_positive=True,
        )
        prior_entry_id = ""
        for entry in entries:
            state = validate_opening_fill(entry, "position entry", carried=False)
            position_id = state["position_id"]
            if position_id <= prior_entry_id:
                raise ProcessVerificationError(
                    "position entries must be unique and sorted by position_id"
                )
            prior_entry_id = position_id
            if state["opened_on"] != current_date or position_id in seen_positions:
                raise ProcessVerificationError(
                    "position entry is duplicated or has the wrong opening date"
                )
            seen_positions.add(position_id)
            open_positions[position_id] = state
        if len(open_positions) > _MAX_POSITIONS_PER_ANCHOR:
            raise ProcessVerificationError("open position count exceeds hard bound")
        expected_open_ids = sorted(open_positions)
        mark_ids: list[str] = []
        gross_cash_sum = Decimal(0)
        base_cost_cash_sum = Decimal(0)
        stress_cost_cash_sum = Decimal(0)
        total_open_risk = Decimal(0)
        total_gross_exposure = Decimal(0)
        position_exposures: list[dict[str, Any]] = []
        mark_by_position: dict[str, tuple[Decimal, Decimal, Decimal, datetime]] = {}
        for mark in mark_records:
            if type(mark) is not dict:
                raise ProcessVerificationError("position mark must be an exact mapping")
            _require_exact_keys(
                mark,
                (
                    "position_id",
                    "marked_at_utc",
                    "mark_price",
                    "mark_bid_price",
                    "mark_ask_price",
                    "market_mark_payload_sha256",
                    "market_mark_receipt_sha256",
                    "base_financing_cash",
                    "stress_financing_cash",
                ),
                "position mark",
            )
            position_id = mark["position_id"]
            if type(position_id) is not str or position_id not in open_positions:
                raise ProcessVerificationError(
                    "position mark references a non-open position"
                )
            mark_ids.append(position_id)
            state = open_positions[position_id]
            marked_at = _parse_utc(mark["marked_at_utc"], "position marked_at_utc")
            if marked_at.date() != current_date or marked_at < state["opened_at"]:
                raise ProcessVerificationError(
                    "position market mark has an invalid causal timestamp"
                )
            mark_price = _economic_decimal(
                mark["mark_price"],
                "position market mark",
                strictly_positive=True,
            )
            mark_bid = _economic_decimal(
                mark["mark_bid_price"],
                "position market bid",
                strictly_positive=True,
            )
            mark_ask = _economic_decimal(
                mark["mark_ask_price"],
                "position market ask",
                strictly_positive=True,
            )
            if (
                mark_bid > mark_price
                or mark_price > mark_ask
                or mark_bid + mark_ask != Decimal(2) * mark_price
            ):
                raise ProcessVerificationError(
                    "position market bid/mid/ask is not internally consistent"
                )
            expected_mark_payload = _sha256(
                {
                    "schema": "fxpm-position-market-mark-payload-v2",
                    "anchor_id": anchor["anchor_id"],
                    "date": date_text,
                    "position_id": position_id,
                    "marked_at_utc": mark["marked_at_utc"],
                    "mark_price": mark["mark_price"],
                    "mark_bid_price": mark["mark_bid_price"],
                    "mark_ask_price": mark["mark_ask_price"],
                }
            )
            if mark["market_mark_payload_sha256"] != expected_mark_payload:
                raise ProcessVerificationError(
                    "position market mark payload identity mismatch"
                )
            record_provenance_identity(
                expected_mark_payload, "position market mark payload"
            )
            record_provenance_identity(
                mark["market_mark_receipt_sha256"],
                "position market mark receipt",
            )
            base_financing = _economic_decimal(
                mark["base_financing_cash"],
                "position base financing",
                strictly_positive=False,
            )
            stress_financing = _economic_decimal(
                mark["stress_financing_cash"],
                "position stress financing",
                strictly_positive=False,
            )
            if stress_financing < base_financing:
                raise ProcessVerificationError(
                    "stress financing is less severe than base financing"
                )
            if (state["side"] == "LONG" and state["stop_price"] >= mark_price) or (
                state["side"] == "SHORT" and state["stop_price"] <= mark_price
            ):
                raise ProcessVerificationError(
                    "open position crossed its proved protective stop before valuation"
                )
            gross_cash = _bounded_economic_product(
                "position gross cash P&L",
                state["side_sign"],
                state["unit_scale"],
                mark_price - state["last_mark"],
            )
            position_base_cost = _bounded_economic_sum(
                "position base cost cash",
                state["pending_base_cost_cash"],
                base_financing,
            )
            position_stress_cost = _bounded_economic_sum(
                "position stress cost cash",
                state["pending_stress_cost_cash"],
                stress_financing,
            )
            gross_cash_sum = _bounded_economic_sum(
                "aggregate daily gross cash", gross_cash_sum, gross_cash
            )
            base_cost_cash_sum = _bounded_economic_sum(
                "aggregate daily base cost cash",
                base_cost_cash_sum,
                position_base_cost,
            )
            stress_cost_cash_sum = _bounded_economic_sum(
                "aggregate daily stress cost cash",
                stress_cost_cash_sum,
                position_stress_cost,
            )
            net_base_cash = _bounded_economic_sum(
                "position net base cash", gross_cash, -position_base_cost
            )
            state["base_contribution"] += _economic_ratio(
                net_base_cash,
                equity,
                "position base contribution",
            )
            state["pending_base_cost_cash"] = Decimal(0)
            state["pending_stress_cost_cash"] = Decimal(0)
            state["last_mark"] = mark_price
            mark_by_position[position_id] = (
                mark_price,
                mark_bid,
                mark_ask,
                marked_at,
            )
            gross_notional_cash = _bounded_economic_product(
                "position gross notional cash", state["unit_scale"], mark_price
            )
            open_risk_cash = _bounded_economic_product(
                "position open risk cash",
                state["unit_scale"],
                abs(mark_price - state["stop_price"]),
            )
            gross_exposure = _economic_ratio(
                gross_notional_cash,
                equity,
                "position gross exposure equity multiple",
            )
            open_risk = _economic_ratio(
                open_risk_cash,
                equity,
                "position open risk equity fraction",
            )
            total_gross_exposure += gross_exposure
            total_open_risk += open_risk
            position_exposures.append(
                {
                    "position_id": position_id,
                    "mark_price": mark["mark_price"],
                    "gross_exposure_equity_multiple": _decimal_text(
                        _quantized_fixed(gross_exposure)
                    ),
                    "open_risk_equity_fraction": _decimal_text(
                        _quantized_fixed(open_risk)
                    ),
                }
            )
        if mark_ids != expected_open_ids:
            raise ProcessVerificationError(
                "daily mark coverage must equal the exact sorted open-position set"
            )
        for position_id, state in open_positions.items():
            holding_days = (current_date - state["opened_on"]).days + 1
            timeframe = state["timeframe"]
            if not 1 <= holding_days <= _FROZEN_HOLDING_DAYS[timeframe]:
                raise ProcessVerificationError(
                    f"event-derived {timeframe} holding exceeds the frozen risk policy"
                )
            observed_holding[timeframe] = max(observed_holding[timeframe], holding_days)
        maximum_open_risk = max(maximum_open_risk, total_open_risk)
        maximum_gross_exposure = max(maximum_gross_exposure, total_gross_exposure)
        maximum_concurrent_positions = max(
            maximum_concurrent_positions, len(open_positions)
        )
        if any(type(exit_fill) is not dict for exit_fill in exits):
            raise ProcessVerificationError("position exits must be exact fill mappings")
        exit_ids = [exit_fill.get("position_id") for exit_fill in exits]
        if any(
            type(position_id) is not str for position_id in exit_ids
        ) or exit_ids != sorted(set(exit_ids)):
            raise ProcessVerificationError(
                "position exits must have unique sorted position IDs"
            )
        for exit_fill, position_id in zip(exits, exit_ids, strict=True):
            _require_exact_keys(
                exit_fill,
                closing_primitive_keys + closing_identity_keys,
                "position closing fill",
            )
            state = open_positions.get(position_id)
            if state is None:
                raise ProcessVerificationError(
                    "position exit references a non-open position"
                )
            closed_at = _parse_utc(exit_fill["closed_at_utc"], "position closed_at_utc")
            if (
                closed_at.date() != current_date
                or closed_at < state["opened_at"]
                or closed_at < mark_by_position[position_id][3]
            ):
                raise ProcessVerificationError(
                    "position closing fill has an invalid causal timestamp"
                )
            broker_fill_id = exit_fill["broker_fill_id"]
            if (
                type(broker_fill_id) is not str
                or not broker_fill_id
                or len(broker_fill_id.encode("utf-8")) > 128
                or broker_fill_id in broker_fill_ids
                or (
                    used_broker_fill_ids is not None
                    and broker_fill_id in used_broker_fill_ids
                )
            ):
                raise ProcessVerificationError(
                    "closing broker fill identity is invalid or reused"
                )
            broker_fill_ids.add(broker_fill_id)
            if used_broker_fill_ids is not None:
                used_broker_fill_ids.add(broker_fill_id)
            reference_mid = _economic_decimal(
                exit_fill["reference_mid_price"],
                "closing reference mid price",
                strictly_positive=True,
            )
            reference_bid = _economic_decimal(
                exit_fill["reference_bid_price"],
                "closing reference bid price",
                strictly_positive=True,
            )
            reference_ask = _economic_decimal(
                exit_fill["reference_ask_price"],
                "closing reference ask price",
                strictly_positive=True,
            )
            if (
                reference_bid > reference_mid
                or reference_mid > reference_ask
                or reference_bid + reference_ask != Decimal(2) * reference_mid
            ):
                raise ProcessVerificationError(
                    "closing bid/mid/ask quote is not internally consistent"
                )
            if (reference_mid, reference_bid, reference_ask) != mark_by_position[
                position_id
            ][:3]:
                raise ProcessVerificationError(
                    "closing reference quote does not equal the proved daily mark"
                )
            base_fill = _economic_decimal(
                exit_fill["base_fill_price"],
                "closing base fill price",
                strictly_positive=True,
            )
            stress_fill = _economic_decimal(
                exit_fill["stress_fill_price"],
                "closing stress fill price",
                strictly_positive=True,
            )
            base_commission = _economic_decimal(
                exit_fill["base_commission_cash"],
                "closing base commission",
                strictly_positive=False,
            )
            stress_commission = _economic_decimal(
                exit_fill["stress_commission_cash"],
                "closing stress commission",
                strictly_positive=False,
            )
            base_fee = _economic_decimal(
                exit_fill["base_fee_cash"],
                "closing base fee",
                strictly_positive=False,
            )
            stress_fee = _economic_decimal(
                exit_fill["stress_fee_cash"],
                "closing stress fee",
                strictly_positive=False,
            )
            executable_quote = (
                reference_bid if state["side"] == "LONG" else reference_ask
            )
            exit_spread_cash = _bounded_economic_product(
                "closing spread cash",
                state["side_sign"],
                reference_mid - executable_quote,
                state["unit_scale"],
            )
            base_slippage_cash = _bounded_economic_product(
                "closing base slippage cash",
                state["side_sign"],
                executable_quote - base_fill,
                state["unit_scale"],
            )
            stress_slippage_cash = _bounded_economic_product(
                "closing stress slippage cash",
                state["side_sign"],
                executable_quote - stress_fill,
                state["unit_scale"],
            )
            base_execution_cost = max(
                Decimal(0),
                _bounded_economic_sum(
                    "closing base spread and slippage cash",
                    exit_spread_cash,
                    base_slippage_cash,
                ),
            )
            stress_execution_cost = max(
                Decimal(0),
                _bounded_economic_sum(
                    "closing stress spread and slippage cash",
                    exit_spread_cash,
                    stress_slippage_cash,
                ),
            )
            base_exit_cost = _bounded_economic_sum(
                "closing base cost cash",
                base_execution_cost,
                base_commission,
                base_fee,
            )
            stress_exit_cost = _bounded_economic_sum(
                "closing stress cost cash",
                stress_execution_cost,
                stress_commission,
                stress_fee,
            )
            if stress_exit_cost < base_exit_cost:
                raise ProcessVerificationError(
                    "closing stress execution cost is less severe than base"
                )
            expected_close_payload = _sha256(
                {
                    "schema": "fxpm-position-closing-fill-payload-v2",
                    "anchor_id": anchor["anchor_id"],
                    "fill": {key: exit_fill[key] for key in closing_primitive_keys},
                }
            )
            if exit_fill["closing_fill_payload_sha256"] != expected_close_payload:
                raise ProcessVerificationError(
                    "position closing fill payload identity mismatch"
                )
            record_provenance_identity(
                expected_close_payload, "position closing fill payload"
            )
            record_provenance_identity(
                exit_fill["closing_fill_receipt_sha256"],
                "position closing fill receipt",
            )
            base_cost_cash_sum = _bounded_economic_sum(
                "aggregate daily base cost cash",
                base_cost_cash_sum,
                base_exit_cost,
            )
            stress_cost_cash_sum = _bounded_economic_sum(
                "aggregate daily stress cost cash",
                stress_cost_cash_sum,
                stress_exit_cost,
            )
            state["base_contribution"] -= _economic_ratio(
                base_exit_cost,
                equity,
                "closing base cost contribution",
            )
            completed_outcomes.append(
                _quantized_return(
                    state["base_contribution"],
                    "completed position net contribution",
                )
            )
            del open_positions[position_id]
        derived_gross = _cash_return(
            gross_cash_sum, equity, "event-derived daily gross return"
        )
        derived_base_cost = _cash_return(
            -base_cost_cash_sum, equity, "event-derived daily base cost"
        )
        derived_stress_cost = _cash_return(
            -stress_cost_cash_sum, equity, "event-derived daily stress cost"
        )
        if (
            derived_gross != gross[index]
            or derived_base_cost != base_cost[index]
            or derived_stress_cost != stress_cost[index]
        ):
            raise ProcessVerificationError(
                "economic fill and mark replay does not reconcile daily gross and cost vectors"
            )
        event_days.append(
            {
                "date": date_text,
                "entry_position_ids": [entry["position_id"] for entry in entries],
                "exit_position_ids": exit_ids,
            }
        )
        fill_days.append(
            {
                "date": date_text,
                "opening_fills": entries,
                "closing_fills": exits,
            }
        )
        cost_days.append(
            {
                "date": date_text,
                "equity_basis_cash": record["equity_basis_cash"],
                "opening_fills": entries,
                "position_marks": mark_records,
                "closing_fills": exits,
                "derived_base_cost_return": _decimal_text(derived_base_cost),
                "derived_stress_cost_return": _decimal_text(derived_stress_cost),
            }
        )
        exposure_days.append(
            {
                "date": date_text,
                "positions": position_exposures,
                "total_gross_exposure_equity_multiple": _decimal_text(
                    _quantized_fixed(total_gross_exposure)
                ),
                "total_open_risk_equity_fraction": _decimal_text(
                    _quantized_fixed(total_open_risk)
                ),
            }
        )
        closing_days.append(
            {"date": date_text, "closing_position_ids": sorted(open_positions)}
        )

    final_positions = [
        {
            "position_id": position_id,
            "cell_decision_sha256": state["cell_decision_sha256"],
            "timeframe": state["timeframe"],
            "opened_at_utc": state["opened_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "side": state["side"],
            "quantity": _decimal_text(state["quantity"]),
            "contract_multiplier": _decimal_text(state["multiplier"]),
            "stop_price": _decimal_text(state["stop_price"]),
            "last_mark_price": _decimal_text(state["last_mark"]),
            "base_contribution_in_window": _decimal_text(
                _quantized_return(
                    state["base_contribution"],
                    "open position net contribution",
                )
            ),
        }
        for position_id, state in sorted(open_positions.items())
    ]
    activity_sha = _sha256(ledger)
    ledger_identities = {
        "event_ledger_sha256": activity_sha,
        "fill_ledger_sha256": _sha256(
            {
                "schema": "fxpm-anchor-entry-exit-fill-ledger-v2",
                "anchor_id": anchor["anchor_id"],
                "initial_positions": initial_positions,
                "days": fill_days,
            }
        ),
        "cost_ledger_sha256": _sha256(
            {
                "schema": "fxpm-anchor-economic-cost-ledger-v2",
                "anchor_id": anchor["anchor_id"],
                "days": cost_days,
            }
        ),
        "exposure_ledger_sha256": _sha256(
            {
                "schema": "fxpm-anchor-derived-exposure-ledger-v2",
                "anchor_id": anchor["anchor_id"],
                "days": exposure_days,
            }
        ),
        "open_position_ledger_sha256": _sha256(
            {
                "schema": "fxpm-anchor-open-position-state-v1",
                "anchor_id": anchor["anchor_id"],
                "days": closing_days,
                "final_positions": final_positions,
            }
        ),
        "calendar_completeness_sha256": _calendar_hash(dates),
    }
    risk_diagnostics = {
        "maximum_concurrent_open_positions": maximum_concurrent_positions,
        "maximum_gross_exposure_equity_multiple": _decimal_text(
            _quantized_fixed(maximum_gross_exposure)
        ),
        "maximum_open_risk_equity_fraction": _decimal_text(
            _quantized_fixed(maximum_open_risk)
        ),
    }
    return (
        completed_outcomes,
        observed_holding,
        ledger_identities,
        risk_diagnostics,
        activity_sha,
    )


def _validate_anchor_evidence(
    anchor: Any,
    plan: Mapping[str, Any],
    forward_oos_replay_lineage: Mapping[str, Any],
    identity_registry: _EvidenceIdentityRegistry,
    risk_policy_sha256: str,
    cost_policy_sha256: str,
    used_broker_fill_ids: set[str],
) -> tuple[dict[str, Any], list[Decimal], list[Decimal]]:
    if type(anchor) is not dict:
        raise ProcessVerificationError("anchor evidence must be a mapping")
    _require_exact_keys(
        anchor,
        (
            "anchor_id",
            "selection_cutoff_utc",
            "forward_start_date",
            "forward_end_date",
            "forward_oos_replay_anchor_lineage_sha256",
            "historical_selection_mode",
            "cutoff_locality",
            "component_outputs",
            "book_cells",
            "train_regime_supports",
            "regime_detector_records",
            "book_sha256",
            "calendar_dates",
            "calendar_sha256",
            "daily_gross_mtm_returns",
            "gross_vector_sha256",
            "daily_base_cost_returns",
            "base_cost_vector_sha256",
            "daily_stress_cost_returns",
            "stress_cost_vector_sha256",
            "daily_base_net_mtm_returns",
            "base_vector_sha256",
            "daily_stress_net_mtm_returns",
            "stress_vector_sha256",
            "activity_ledger",
            "activity_ledger_sha256",
            "completed_trade_count",
            "completed_trade_outcomes",
            "trade_vector_sha256",
            "dependence_diagnostics",
            "risk_diagnostics",
            "ledger_identities",
        ),
        "anchor evidence",
    )
    for key in (
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
    ):
        if anchor[key] != plan[key]:
            raise ProcessVerificationError(
                "anchor evidence does not match precommitted geometry"
            )
    expected_forward_lineage = _forward_oos_anchor_lineage_sha256(
        forward_oos_replay_lineage, anchor["anchor_id"]
    )
    if anchor["forward_oos_replay_anchor_lineage_sha256"] != expected_forward_lineage:
        raise ProcessVerificationError(
            "anchor evidence does not bind the sealed forward OOS product"
        )
    if anchor["historical_selection_mode"] != "ANCHOR_LOCAL_AS_OF_ONLY":
        raise ProcessVerificationError(
            "current-survivor relabel or nonlocal selection is forbidden"
        )
    cutoff = _parse_utc(anchor["selection_cutoff_utc"], "anchor cutoff")
    locality = anchor["cutoff_locality"]
    if type(locality) is not dict:
        raise ProcessVerificationError("cutoff locality must be a mapping")
    _require_exact_keys(locality, CUTOFF_LOCALITY_KEYS, "cutoff locality")
    for key in CUTOFF_LOCALITY_KEYS:
        if _parse_utc(locality[key], key) > cutoff:
            raise ProcessVerificationError(
                "anchor-local process component saw future market data"
            )
    outputs = _validate_hash_map(
        anchor["component_outputs"], ANCHOR_OUTPUT_KEYS, "anchor outputs"
    )
    book_sha = _validate_cell_book(
        anchor["book_cells"],
        anchor["train_regime_supports"],
        anchor["regime_detector_records"],
        anchor["selection_cutoff_utc"],
        anchor["anchor_id"],
        identity_registry,
    )
    if (
        anchor["book_sha256"] != book_sha
        or outputs["exact_2400_cell_book_sha256"] != book_sha
    ):
        raise ProcessVerificationError("anchor exact book identity mismatch")
    for key, expected_output in _expected_lineage_outputs(anchor).items():
        if outputs[key] != expected_output:
            raise ProcessVerificationError(
                f"anchor component output is not the canonical lineage root: {key}"
            )
    start = _parse_date(anchor["forward_start_date"], "anchor forward start")
    end = _parse_date(anchor["forward_end_date"], "anchor forward end")
    expected_dates = _calendar(start, end)
    dates = anchor["calendar_dates"]
    if type(dates) is not list or dates != expected_dates:
        raise ProcessVerificationError(
            "anchor omits, duplicates, or reorders a UTC calendar date"
        )
    if len(dates) > _MAX_DAYS_PER_ANCHOR:
        raise ProcessVerificationError("anchor calendar exceeds the day bound")
    if anchor["calendar_sha256"] != _calendar_hash(dates):
        raise ProcessVerificationError("anchor calendar identity mismatch")
    gross_values = anchor["daily_gross_mtm_returns"]
    base_cost_values = anchor["daily_base_cost_returns"]
    stress_cost_values = anchor["daily_stress_cost_returns"]
    base_values = anchor["daily_base_net_mtm_returns"]
    stress_values = anchor["daily_stress_net_mtm_returns"]
    daily_vectors = (
        gross_values,
        base_cost_values,
        stress_cost_values,
        base_values,
        stress_values,
    )
    if any(type(values) is not list for values in daily_vectors):
        raise ProcessVerificationError("daily MTM evidence must be vectors")
    if any(len(values) != len(dates) for values in daily_vectors):
        raise ProcessVerificationError(
            "daily MTM vector length does not match its calendar"
        )
    bound = Decimal(_PINNED_POLICY["daily_return_absolute_bound"])
    gross = [
        _decimal(value, "gross daily return", absolute_bound=bound)
        for value in gross_values
    ]
    base_cost = [
        _decimal(value, "base daily cost debit", absolute_bound=bound)
        for value in base_cost_values
    ]
    stress_cost = [
        _decimal(value, "stress daily cost debit", absolute_bound=bound)
        for value in stress_cost_values
    ]
    base = [
        _decimal(value, "base daily return", absolute_bound=bound)
        for value in base_values
    ]
    stress = [
        _decimal(value, "stress daily return", absolute_bound=bound)
        for value in stress_values
    ]
    for index, (
        gross_return,
        base_debit,
        stress_debit,
        base_net,
        stress_net,
    ) in enumerate(zip(gross, base_cost, stress_cost, base, stress, strict=True)):
        if base_debit > 0 or stress_debit > base_debit:
            raise ProcessVerificationError(
                f"cost stress is not at least as severe as base on day {index}"
            )
        if gross_return + base_debit != base_net:
            raise ProcessVerificationError(
                f"base return does not reconcile gross MTM and costs on day {index}"
            )
        if gross_return + stress_debit != stress_net:
            raise ProcessVerificationError(
                f"stress return does not reconcile the same gross MTM stream on day {index}"
            )
    if anchor["gross_vector_sha256"] != _vector_hash(dates, gross_values, "GROSS_MTM"):
        raise ProcessVerificationError("gross daily vector identity mismatch")
    if anchor["base_cost_vector_sha256"] != _vector_hash(
        dates, base_cost_values, "BASE_COST_DEBIT"
    ):
        raise ProcessVerificationError("base cost vector identity mismatch")
    if anchor["stress_cost_vector_sha256"] != _vector_hash(
        dates, stress_cost_values, "STRESS_COST_DEBIT"
    ):
        raise ProcessVerificationError("stress cost vector identity mismatch")
    if anchor["base_vector_sha256"] != _vector_hash(dates, base_values, "BASE"):
        raise ProcessVerificationError("base daily vector identity mismatch")
    if anchor["stress_vector_sha256"] != _vector_hash(dates, stress_values, "STRESS"):
        raise ProcessVerificationError("stress daily vector identity mismatch")
    (
        event_completed_outcomes,
        event_observed_holding_by_timeframe,
        event_ledger_identities,
        event_risk_diagnostics,
        event_activity_sha256,
    ) = _validate_activity_ledger(
        anchor,
        dates,
        gross,
        base_cost,
        stress_cost,
        identity_registry,
        used_broker_fill_ids,
    )
    if anchor["activity_ledger_sha256"] != event_activity_sha256:
        raise ProcessVerificationError("position activity ledger identity mismatch")
    trades = anchor["completed_trade_count"]
    if (
        type(trades) is not int
        or type(trades) is bool
        or not 0 <= trades <= 100_000_000
    ):
        raise ProcessVerificationError("completed_trade_count is invalid")
    trade_values = anchor["completed_trade_outcomes"]
    if type(trade_values) is not list or len(trade_values) != trades:
        raise ProcessVerificationError(
            "completed-trade vector is absent or count-inconsistent"
        )
    parsed_trade_values = [
        _decimal(value, "completed-trade return", absolute_bound=bound)
        for value in trade_values
    ]
    if trades != len(event_completed_outcomes) or parsed_trade_values != (
        event_completed_outcomes
    ):
        raise ProcessVerificationError(
            "completed-trade outcomes are not derived from the position activity ledger"
        )
    expected_trade_sha = _sha256(
        {
            "lane": "COMPLETED_POSITION_NET_EQUITY_CONTRIBUTIONS_IN_FORWARD_WINDOW",
            "completed_position_net_equity_contributions": trade_values,
        }
    )
    if anchor["trade_vector_sha256"] != expected_trade_sha:
        raise ProcessVerificationError(
            "completed-trade outcome vector identity mismatch"
        )
    if trades == 0 and any(
        value != 0 for vector in (gross, base, stress) for value in vector
    ):
        raise ProcessVerificationError(
            "nonzero MTM evidence requires at least one completed position"
        )
    winner_count = sum(cell["status"] == "WINNER" for cell in anchor["book_cells"])
    if winner_count == 0 and (
        trades != 0
        or any(value != 0 for value in gross)
        or any(value != 0 for value in base)
        or any(value != 0 for value in stress)
    ):
        raise ProcessVerificationError(
            "an anchor with no selected specialist cannot manufacture returns or trades"
        )
    dependence = anchor["dependence_diagnostics"]
    if type(dependence) is not dict:
        raise ProcessVerificationError(
            "dependence diagnostics must be an exact mapping"
        )
    _require_exact_keys(
        dependence,
        (
            "risk_policy_sha256",
            "inference_unit",
            "maximum_observed_holding_calendar_days_by_timeframe",
            "maximum_observed_holding_calendar_days",
            "execution_overlap_allowance_calendar_days",
            "holding_overlap_hac_floor_calendar_days",
            "sample_size_hac_floor_calendar_days",
            "primary_hac_bandwidth_calendar_days",
            "sensitivity_hac_bandwidth_calendar_days",
        ),
        "dependence diagnostics",
    )
    if dependence["risk_policy_sha256"] != risk_policy_sha256:
        raise ProcessVerificationError(
            "dependence diagnostics do not bind the frozen risk policy"
        )
    if dependence["inference_unit"] != _PINNED_POLICY["portfolio_inference_unit"]:
        raise ProcessVerificationError(
            "dependence diagnostics use the wrong inference unit"
        )
    observed_holding = dependence["maximum_observed_holding_calendar_days"]
    observed_holding_by_timeframe = dependence[
        "maximum_observed_holding_calendar_days_by_timeframe"
    ]
    if type(observed_holding_by_timeframe) is not dict:
        raise ProcessVerificationError(
            "observed holding by timeframe must be an exact mapping"
        )
    _require_exact_keys(
        observed_holding_by_timeframe,
        TIMEFRAMES,
        "observed holding by timeframe",
    )
    if observed_holding_by_timeframe != event_observed_holding_by_timeframe:
        raise ProcessVerificationError(
            "holding diagnostics are not derived from the position activity ledger"
        )
    for timeframe, observed in observed_holding_by_timeframe.items():
        if (
            type(observed) is not int
            or type(observed) is bool
            or not 0
            <= observed
            <= _FROZEN_RISK_POLICY["maximum_holding_calendar_days_by_timeframe"][
                timeframe
            ]
        ):
            raise ProcessVerificationError(
                f"observed {timeframe} holding exceeds the frozen risk policy"
            )
    if (
        type(observed_holding) is not int
        or type(observed_holding) is bool
        or not 0
        <= observed_holding
        <= max(_PINNED_POLICY["maximum_holding_calendar_days_by_timeframe"].values())
    ):
        raise ProcessVerificationError(
            "observed holding exceeds the frozen dependence bound"
        )
    if observed_holding != max(observed_holding_by_timeframe.values()):
        raise ProcessVerificationError(
            "aggregate observed holding does not match timeframe diagnostics"
        )
    if (
        dependence["execution_overlap_allowance_calendar_days"]
        != (_PINNED_POLICY["execution_overlap_allowance_calendar_days"])
    ):
        raise ProcessVerificationError(
            "dependence diagnostics do not bind the frozen overlap allowance"
        )
    expected_holding_floor = (
        max(_PINNED_POLICY["maximum_holding_calendar_days_by_timeframe"].values())
        + _PINNED_POLICY["execution_overlap_allowance_calendar_days"]
        - 1
    )
    sample_floor, primary_bandwidth, sensitivity_bandwidth = _pinned_hac_bandwidths(
        [base]
    )
    if (
        dependence["holding_overlap_hac_floor_calendar_days"] != expected_holding_floor
        or expected_holding_floor
        != _PINNED_POLICY["holding_overlap_hac_floor_calendar_days"]
        or dependence["sample_size_hac_floor_calendar_days"] != sample_floor
        or dependence["primary_hac_bandwidth_calendar_days"] != primary_bandwidth
        or dependence["sensitivity_hac_bandwidth_calendar_days"]
        != sensitivity_bandwidth
    ):
        raise ProcessVerificationError(
            "dependence bandwidth is not the pinned derivation"
        )
    ledgers = _validate_hash_map(
        anchor["ledger_identities"], LEDGER_IDENTITY_KEYS, "ledger identities"
    )
    if ledgers != event_ledger_identities:
        raise ProcessVerificationError(
            "ledger identities are not derived from the position activity ledger"
        )
    risk_diagnostics = anchor["risk_diagnostics"]
    if type(risk_diagnostics) is not dict:
        raise ProcessVerificationError("risk diagnostics must be an exact mapping")
    _require_exact_keys(
        risk_diagnostics,
        (
            "maximum_concurrent_open_positions",
            "maximum_gross_exposure_equity_multiple",
            "maximum_open_risk_equity_fraction",
        ),
        "risk diagnostics",
    )
    if risk_diagnostics != event_risk_diagnostics:
        raise ProcessVerificationError(
            "risk diagnostics are not derived from the position activity ledger"
        )
    expected_risk_output = _sha256(
        {
            "schema": "fxpm-anchor-risk-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "risk_policy_sha256": risk_policy_sha256,
            "exposure_ledger_sha256": ledgers["exposure_ledger_sha256"],
            "open_position_ledger_sha256": ledgers["open_position_ledger_sha256"],
            "risk_diagnostics": risk_diagnostics,
            "dependence_diagnostics": dependence,
        }
    )
    expected_cost_output = _sha256(
        {
            "schema": "fxpm-anchor-cost-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "cost_policy_sha256": cost_policy_sha256,
            "cost_ledger_sha256": ledgers["cost_ledger_sha256"],
            "gross_vector_sha256": anchor["gross_vector_sha256"],
            "base_cost_vector_sha256": anchor["base_cost_vector_sha256"],
            "stress_cost_vector_sha256": anchor["stress_cost_vector_sha256"],
            "base_vector_sha256": anchor["base_vector_sha256"],
            "stress_vector_sha256": anchor["stress_vector_sha256"],
        }
    )
    expected_replay_output = _sha256(
        {
            "schema": "fxpm-anchor-synchronized-replay-v1",
            "anchor_id": anchor["anchor_id"],
            "forward_oos_replay_anchor_lineage_sha256": expected_forward_lineage,
            "book_sha256": book_sha,
            "calendar_sha256": anchor["calendar_sha256"],
            "event_ledger_sha256": ledgers["event_ledger_sha256"],
            "fill_ledger_sha256": ledgers["fill_ledger_sha256"],
            "gross_vector_sha256": anchor["gross_vector_sha256"],
            "base_vector_sha256": anchor["base_vector_sha256"],
            "stress_vector_sha256": anchor["stress_vector_sha256"],
        }
    )
    lineage_outputs = _expected_lineage_outputs(anchor)
    expected_reconciliation_input = _sha256(
        {
            "schema": "fxpm-anchor-reconciliation-input-v1",
            "anchor_id": anchor["anchor_id"],
            "lineage_outputs": lineage_outputs,
            "risk_execution_sha256": expected_risk_output,
            "cost_execution_sha256": expected_cost_output,
            "synchronized_replay_sha256": expected_replay_output,
            "forward_oos_replay_anchor_lineage_sha256": expected_forward_lineage,
            "ledger_identities": ledgers,
        }
    )
    for key, expected_output in (
        ("risk_execution_sha256", expected_risk_output),
        ("cost_execution_sha256", expected_cost_output),
        ("synchronized_replay_sha256", expected_replay_output),
        ("reconciliation_input_sha256", expected_reconciliation_input),
    ):
        if outputs[key] != expected_output:
            raise ProcessVerificationError(
                f"anchor output is not bound to vectors and ledgers: {key}"
            )
    anchor_id = anchor["anchor_id"]
    identity_registry.register(book_sha, f"{anchor_id} exact cell book")
    for key in (
        "calendar_sha256",
        "gross_vector_sha256",
        "base_cost_vector_sha256",
        "stress_cost_vector_sha256",
        "base_vector_sha256",
        "stress_vector_sha256",
        "trade_vector_sha256",
    ):
        identity_registry.register(
            anchor[key],
            f"{anchor_id} {key}",
            reusable_content_domain=f"observation content {key}",
        )
    for key, identity in outputs.items():
        if key != "exact_2400_cell_book_sha256":
            identity_registry.register(identity, f"{anchor_id} component output {key}")
    identity_registry.register(event_activity_sha256, f"{anchor_id} activity ledger")
    for key, identity in ledgers.items():
        if key not in ("event_ledger_sha256", "calendar_completeness_sha256"):
            identity_registry.register(identity, f"{anchor_id} ledger {key}")
    return anchor, base, stress


def _validate_reconciliation_anchor(
    reconciliation: Any,
    evidence_anchor: Mapping[str, Any],
    identity_registry: _EvidenceIdentityRegistry,
) -> None:
    if type(reconciliation) is not dict:
        raise ProcessVerificationError("reconciliation anchor must be a mapping")
    _require_exact_keys(
        reconciliation,
        (
            "anchor_id",
            "evidence_anchor_sha256",
            "forward_oos_replay_anchor_lineage_sha256",
            "component_outputs",
            "ledger_identities",
            "calendar_dates",
            "daily_gross_mtm_returns",
            "daily_base_cost_returns",
            "daily_stress_cost_returns",
            "daily_base_net_mtm_returns",
            "daily_stress_net_mtm_returns",
            "activity_ledger",
            "activity_ledger_sha256",
            "calendar_sha256",
            "gross_vector_sha256",
            "base_cost_vector_sha256",
            "stress_cost_vector_sha256",
            "base_vector_sha256",
            "stress_vector_sha256",
            "completed_trade_count",
            "completed_trade_outcomes",
            "trade_vector_sha256",
            "dependence_diagnostics",
            "risk_diagnostics",
            "events_complete",
            "fills_complete",
            "costs_complete",
            "exposures_complete",
            "open_position_valuation_complete",
            "calendar_complete",
            "net_mtm_reconciled",
        ),
        "reconciliation anchor",
    )
    if reconciliation["anchor_id"] != evidence_anchor["anchor_id"]:
        raise ProcessVerificationError("reconciliation anchor ID mismatch")
    if reconciliation["evidence_anchor_sha256"] != _sha256(evidence_anchor):
        raise ProcessVerificationError(
            "reconciliation does not bind the exact evidence anchor"
        )
    for key in (
        "component_outputs",
        "forward_oos_replay_anchor_lineage_sha256",
        "ledger_identities",
        "calendar_dates",
        "daily_gross_mtm_returns",
        "daily_base_cost_returns",
        "daily_stress_cost_returns",
        "daily_base_net_mtm_returns",
        "daily_stress_net_mtm_returns",
        "activity_ledger",
        "activity_ledger_sha256",
        "calendar_sha256",
        "gross_vector_sha256",
        "base_cost_vector_sha256",
        "stress_cost_vector_sha256",
        "base_vector_sha256",
        "stress_vector_sha256",
        "completed_trade_count",
        "completed_trade_outcomes",
        "trade_vector_sha256",
        "dependence_diagnostics",
        "risk_diagnostics",
    ):
        if reconciliation[key] != evidence_anchor[key]:
            raise ProcessVerificationError(
                "independent reconciliation does not bind exact dates, values, ledgers, and outputs"
            )
    for key in (
        "events_complete",
        "fills_complete",
        "costs_complete",
        "exposures_complete",
        "open_position_valuation_complete",
        "calendar_complete",
        "net_mtm_reconciled",
    ):
        if reconciliation[key] is not True:
            raise ProcessVerificationError(
                f"reconciliation completeness gate failed: {key}"
            )
    identity_registry.register(
        reconciliation["evidence_anchor_sha256"],
        f"{evidence_anchor['anchor_id']} reconciliation evidence anchor",
    )


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _sample_variance(values: Sequence[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal(0)
    mean = _mean(values)
    return sum(((value - mean) ** 2 for value in values), Decimal(0)) / Decimal(
        len(values) - 1
    )


def _anchor_lrv(values: Sequence[Decimal], lag_limit: int) -> Decimal:
    count = len(values)
    mean = _mean(values)
    centered = [value - mean for value in values]
    gamma0 = sum((value * value for value in centered), Decimal(0)) / Decimal(count)
    lrv = gamma0
    limit = min(lag_limit, count - 1)
    for lag in range(1, limit + 1):
        covariance = sum(
            (centered[index] * centered[index - lag] for index in range(lag, count)),
            Decimal(0),
        ) / Decimal(count)
        weight = Decimal(1) - Decimal(lag) / Decimal(limit + 1)
        lrv += Decimal(2) * weight * covariance
    return max(Decimal(0), lrv)


def _sample_size_hac_floor(count: int) -> int:
    return math.ceil(4.0 * ((count / 100.0) ** (2.0 / 9.0)))


def _pinned_hac_bandwidths(
    vectors: Sequence[Sequence[Decimal]],
) -> tuple[int, int, int]:
    shortest = min(len(vector) for vector in vectors)
    sample_floor = max(_sample_size_hac_floor(len(vector)) for vector in vectors)
    primary = max(
        _PINNED_POLICY["holding_overlap_hac_floor_calendar_days"], sample_floor
    )
    sensitivity = min(
        primary * _PINNED_POLICY["sensitivity_hac_bandwidth_multiplier"], shortest - 1
    )
    if sensitivity <= primary:
        raise ProcessVerificationError(
            "anchor calendar cannot support HAC sensitivity bandwidth"
        )
    return sample_floor, primary, sensitivity


def _log_growth_vectors(
    vectors: Sequence[Sequence[Decimal]],
) -> list[list[Decimal]]:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_CONTEXT_PRECISION
            return [
                [(Decimal(1) + value).ln() for value in vector] for vector in vectors
            ]
    except DecimalException as exc:
        raise ProcessVerificationError(
            "daily net equity return cannot produce finite log growth"
        ) from exc


def _pooled_hac_variance(
    vectors: Sequence[Sequence[Decimal]], bandwidth: int
) -> tuple[Decimal, Decimal]:
    total = sum(len(vector) for vector in vectors)
    pooled_mean = sum(
        (sum(vector, Decimal(0)) for vector in vectors), Decimal(0)
    ) / Decimal(total)
    variance = sum(
        (Decimal(len(vector)) * _anchor_lrv(vector, bandwidth) for vector in vectors),
        Decimal(0),
    ) / (Decimal(total) * Decimal(total))
    return pooled_mean, variance


def _anchor_cluster_variance(
    vectors: Sequence[Sequence[Decimal]], pooled_mean: Decimal
) -> Decimal:
    total = sum(len(vector) for vector in vectors)
    cluster_count = len(vectors)
    return (
        Decimal(cluster_count)
        / Decimal(cluster_count - 1)
        * sum(
            (sum(vector, Decimal(0)) - Decimal(len(vector)) * pooled_mean) ** 2
            for vector in vectors
        )
        / (Decimal(total) * Decimal(total))
    )


def _student_t_lcb(mean: Decimal, variance: Decimal, cluster_count: int) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        standard_error = variance.sqrt()
    critical = Decimal(_ONE_SIDED_T_95_BY_DF[cluster_count - 1])
    return mean - critical * standard_error


def _daily_ips_variance(
    vectors: Sequence[Sequence[Decimal]], max_lag: int
) -> tuple[Decimal | None, Decimal | None, bool, int]:
    """Return exact anchor-centered Geyer IPS LRV and pooled-mean variance."""

    if max_lag != 67:
        raise ProcessVerificationError("daily IPS maximum lag is not pinned at 67")
    total = sum(len(vector) for vector in vectors)
    centered = [[value - _mean(vector) for value in vector] for vector in vectors]
    gammas: list[Decimal] = []
    for lag in range(max_lag + 1):
        covariance_sum = sum(
            (
                sum(
                    (
                        vector[index] * vector[index - lag]
                        for index in range(lag, len(vector))
                    ),
                    Decimal(0),
                )
                for vector in centered
            ),
            Decimal(0),
        )
        gammas.append(covariance_sum / Decimal(total))
    raw_pairs = [
        gammas[index] + gammas[index + 1] for index in range(0, max_lag + 1, 2)
    ]
    first_nonpositive = next(
        (index for index, value in enumerate(raw_pairs) if value <= 0), None
    )
    if first_nonpositive is None:
        return None, None, True, len(raw_pairs)
    monotone_pairs: list[Decimal] = []
    for raw_pair in raw_pairs[:first_nonpositive]:
        monotone_pairs.append(
            raw_pair if not monotone_pairs else min(monotone_pairs[-1], raw_pair)
        )
    lrv = max(
        Decimal(0),
        -gammas[0] + Decimal(2) * sum(monotone_pairs, Decimal(0)),
    )
    return lrv, lrv / Decimal(total), False, len(monotone_pairs)


def _anchor_group_t_lcb(
    vectors: Sequence[Sequence[Decimal]],
) -> tuple[Decimal, Decimal, Decimal]:
    means = [_mean(vector) for vector in vectors]
    group_mean = _mean(means)
    between_variance = _sample_variance(means)
    with localcontext() as context:
        context.prec = _DECIMAL_CONTEXT_PRECISION
        standard_error = (between_variance / Decimal(len(means))).sqrt()
    critical = Decimal(_ONE_SIDED_T_95_BY_DF[len(means) - 1])
    return group_mean, group_mean - critical * standard_error, between_variance


def _effective_dependence_blocks(
    values: Sequence[Decimal], sensitivity_bandwidth: int
) -> Decimal:
    block_size = sensitivity_bandwidth + 1
    return Decimal(len(values)) / Decimal(block_size)


def _leave_most_influential_block(
    values: Sequence[Decimal], block_size: int
) -> tuple[list[Decimal], int, Decimal]:
    if (
        type(block_size) is not int
        or type(block_size) is bool
        or block_size <= 0
        or len(values) <= block_size
    ):
        raise ProcessVerificationError(
            "influence block must leave an independently testable remainder"
        )
    window = sum(values[:block_size], Decimal(0))
    most_influential_sum = window
    most_influential_start = 0
    for start in range(1, len(values) - block_size + 1):
        window += values[start + block_size - 1] - values[start - 1]
        if window > most_influential_sum:
            most_influential_sum = window
            most_influential_start = start
    remaining = list(values[:most_influential_start]) + list(
        values[most_influential_start + block_size :]
    )
    return remaining, most_influential_start, most_influential_sum


def _expected_shortfall_5pct(vectors: Sequence[Sequence[Decimal]]) -> Decimal:
    values = sorted(value for vector in vectors for value in vector)
    tail_count = max(1, math.ceil(len(values) * 0.05))
    return max(Decimal(0), -_mean(values[:tail_count]))


def _maximum_drawdown(vectors: Sequence[Sequence[Decimal]]) -> Decimal:
    equity = Decimal(1)
    high = Decimal(1)
    drawdown = Decimal(0)
    for vector in vectors:
        for value in vector:
            equity *= Decimal(1) + value
            high = max(high, equity)
            drawdown = max(drawdown, (high - equity) / high)
    return drawdown


def _compute_decision_body(
    *,
    process_id: str,
    process_definition_sha256: str,
    precommit_receipt_sha256: str,
    evidence_receipt_sha256: str,
    reconciliation_receipt_sha256: str,
    reconciliation_time: datetime,
    anchors: Sequence[Mapping[str, Any]],
    base_vectors: Sequence[Sequence[Decimal]],
    stress_vectors: Sequence[Sequence[Decimal]],
) -> dict[str, Any]:
    base_log_vectors = _log_growth_vectors(base_vectors)
    stress_log_vectors = _log_growth_vectors(stress_vectors)
    base_arithmetic_means = [_mean(vector) for vector in base_vectors]
    stress_arithmetic_means = [_mean(vector) for vector in stress_vectors]
    base_log_means = [_mean(vector) for vector in base_log_vectors]
    stress_log_means = [_mean(vector) for vector in stress_log_vectors]
    sample_floor, primary_bandwidth, sensitivity_bandwidth = _pinned_hac_bandwidths(
        stress_log_vectors
    )

    pooled_base_log_mean, base_primary_variance = _pooled_hac_variance(
        base_log_vectors, primary_bandwidth
    )
    _, base_sensitivity_variance = _pooled_hac_variance(
        base_log_vectors, sensitivity_bandwidth
    )
    base_cluster_variance = _anchor_cluster_variance(
        base_log_vectors, pooled_base_log_mean
    )
    base_diagnostic_variance = max(
        base_primary_variance,
        base_sensitivity_variance,
        base_cluster_variance,
    )
    base_diagnostic_lcb = _student_t_lcb(
        pooled_base_log_mean, base_diagnostic_variance, len(base_log_vectors)
    )

    pooled_stress_log_mean, stress_primary_variance = _pooled_hac_variance(
        stress_log_vectors, primary_bandwidth
    )
    _, stress_sensitivity_variance = _pooled_hac_variance(
        stress_log_vectors, sensitivity_bandwidth
    )
    stress_cluster_variance = _anchor_cluster_variance(
        stress_log_vectors, pooled_stress_log_mean
    )
    ips_lrv, ips_variance, ips_unresolved, ips_valid_pair_count = _daily_ips_variance(
        stress_log_vectors,
        _PINNED_POLICY["daily_ips_max_lag"],
    )
    stress_variance_candidates = {
        "PRIMARY_HAC": stress_primary_variance,
        "SENSITIVITY_HAC": stress_sensitivity_variance,
        "ANCHOR_CLUSTER": stress_cluster_variance,
    }
    if ips_variance is not None:
        stress_variance_candidates["DAILY_IPS"] = ips_variance
    selected_stress_variance = max(stress_variance_candidates.values())
    selected_stress_variance_sources = sorted(
        key
        for key, value in stress_variance_candidates.items()
        if value == selected_stress_variance
    )
    if ips_unresolved:
        confirmatory_lcb = None
        confirmatory_status = "DEPENDENCE_INSUFFICIENT"
    else:
        confirmatory_lcb = _student_t_lcb(
            pooled_stress_log_mean,
            selected_stress_variance,
            len(stress_log_vectors),
        )
        confirmatory_status = "PASS" if confirmatory_lcb > 0 else "FAIL"

    influence_block_size = _PINNED_POLICY["most_influential_block_calendar_days"]
    base_influence_results = [
        _leave_most_influential_block(vector, influence_block_size)
        for vector in base_log_vectors
    ]
    stress_influence_results = [
        _leave_most_influential_block(vector, influence_block_size)
        for vector in stress_log_vectors
    ]
    base_without_influence = [result[0] for result in base_influence_results]
    stress_without_influence = [result[0] for result in stress_influence_results]
    pooled_base_without_influence_mean = sum(
        (sum(vector, Decimal(0)) for vector in base_without_influence), Decimal(0)
    ) / Decimal(sum(len(vector) for vector in base_without_influence))
    pooled_stress_without_influence_mean = sum(
        (sum(vector, Decimal(0)) for vector in stress_without_influence), Decimal(0)
    ) / Decimal(sum(len(vector) for vector in stress_without_influence))

    anchor_group_base_log_mean, anchor_group_base_log_lcb, _ = _anchor_group_t_lcb(
        base_log_vectors
    )
    anchor_group_stress_log_mean, anchor_group_stress_log_lcb, _ = _anchor_group_t_lcb(
        stress_log_vectors
    )
    base_between_log_variance = _sample_variance(base_log_means)
    stress_between_log_variance = _sample_variance(stress_log_means)
    base_within_log_variances = [
        _sample_variance(vector) for vector in base_log_vectors
    ]
    stress_within_log_variances = [
        _sample_variance(vector) for vector in stress_log_vectors
    ]
    anchor_blocks = [
        _effective_dependence_blocks(vector, sensitivity_bandwidth)
        for vector in stress_log_vectors
    ]
    trades = [anchor["completed_trade_count"] for anchor in anchors]
    base_es = _expected_shortfall_5pct(base_vectors)
    stress_es = _expected_shortfall_5pct(stress_vectors)
    base_dd = _maximum_drawdown(base_vectors)
    stress_dd = _maximum_drawdown(stress_vectors)

    gates = {
        "newest_anchor_stress_log_growth_mean_above_zero": stress_log_means[-1] > 0,
        "base_between_anchor_log_growth_variation": base_between_log_variance > 0,
        "stress_between_anchor_log_growth_variation": stress_between_log_variance > 0,
        "base_distinct_anchor_log_growth_means": len(set(base_log_means))
        >= _PINNED_POLICY["minimum_distinct_anchor_means"],
        "stress_distinct_anchor_log_growth_means": len(set(stress_log_means))
        >= _PINNED_POLICY["minimum_distinct_anchor_means"],
        "base_positive_within_anchor_log_growth_variance": min(
            base_within_log_variances
        )
        > 0,
        "stress_positive_within_anchor_log_growth_variance": min(
            stress_within_log_variances
        )
        > 0,
        "base_arithmetic_expected_shortfall_risk_limit": base_es
        <= Decimal(_PINNED_POLICY["maximum_base_expected_shortfall_5pct"]),
        "stress_arithmetic_expected_shortfall_risk_limit": stress_es
        <= Decimal(_PINNED_POLICY["maximum_stress_expected_shortfall_5pct"]),
        "base_arithmetic_drawdown_risk_limit": base_dd
        <= Decimal(_PINNED_POLICY["maximum_base_drawdown"]),
        "stress_arithmetic_drawdown_risk_limit": stress_dd
        <= Decimal(_PINNED_POLICY["maximum_stress_drawdown"]),
    }
    failure_reasons = sorted(key for key, value in gates.items() if not value)
    if confirmatory_status == "FAIL":
        failure_reasons.append("stress_log_growth_confirmatory_lcb_above_zero")
        failure_reasons.sort()
    if failure_reasons:
        decision = "OFFLINE_PROCESS_VALIDATION_FAIL"
        insufficiency_reasons: list[str] = []
    elif confirmatory_status == "DEPENDENCE_INSUFFICIENT":
        decision = "OFFLINE_PROCESS_VALIDATION_DEPENDENCE_INSUFFICIENT"
        insufficiency_reasons = [
            "stress_log_growth_ips_positive_sequence_unresolved_through_lag_67"
        ]
    else:
        decision = "OFFLINE_PROCESS_VALIDATION_PASS"
        insufficiency_reasons = []

    metrics = {
        "primary_estimand": _PINNED_POLICY["primary_estimand"],
        "arithmetic_return_role": _PINNED_POLICY["arithmetic_return_role"],
        "confirmatory_null_daily_stress_log_growth": "0",
        "confirmatory_null_units": _PINNED_POLICY["confirmatory_null_units"],
        "confirmatory_null_rationale": _PINNED_POLICY["confirmatory_null_rationale"],
        "anchor_count": len(anchors),
        "total_calendar_days": sum(len(vector) for vector in stress_log_vectors),
        "completed_trade_count": sum(trades),
        "completed_trade_counts_by_anchor_diagnostic_only": trades,
        "effective_dependence_blocks": _decimal_text(sum(anchor_blocks, Decimal(0))),
        "sample_size_hac_floor_calendar_days": sample_floor,
        "primary_hac_bandwidth_calendar_days": primary_bandwidth,
        "sensitivity_hac_bandwidth_calendar_days": sensitivity_bandwidth,
        "fixed_hac_role": _PINNED_POLICY["fixed_hac_role"],
        "small_cluster_degrees_of_freedom": len(anchors) - 1,
        "small_cluster_one_sided_critical_value": _ONE_SIDED_T_95_BY_DF[
            len(anchors) - 1
        ],
        "pooled_stress_log_growth_mean": _decimal_text(pooled_stress_log_mean),
        "stress_log_growth_confirmatory_lcb": (
            None if confirmatory_lcb is None else _decimal_text(confirmatory_lcb)
        ),
        "stress_log_growth_selected_variance": _decimal_text(selected_stress_variance),
        "stress_log_growth_selected_variance_sources": (
            selected_stress_variance_sources
        ),
        "stress_log_growth_primary_hac_variance": _decimal_text(
            stress_primary_variance
        ),
        "stress_log_growth_sensitivity_hac_variance": _decimal_text(
            stress_sensitivity_variance
        ),
        "stress_log_growth_anchor_cluster_variance": _decimal_text(
            stress_cluster_variance
        ),
        "stress_log_growth_daily_ips_lrv": (
            None if ips_lrv is None else _decimal_text(ips_lrv)
        ),
        "stress_log_growth_daily_ips_variance": (
            None if ips_variance is None else _decimal_text(ips_variance)
        ),
        "stress_log_growth_daily_ips_valid_pair_count": ips_valid_pair_count,
        "stress_log_growth_daily_ips_unresolved_through_lag_67": ips_unresolved,
        "pooled_base_log_growth_mean_diagnostic": _decimal_text(pooled_base_log_mean),
        "base_log_growth_max_variance_lcb_diagnostic": _decimal_text(
            base_diagnostic_lcb
        ),
        "base_log_growth_primary_hac_variance_diagnostic": _decimal_text(
            base_primary_variance
        ),
        "base_log_growth_sensitivity_hac_variance_diagnostic": _decimal_text(
            base_sensitivity_variance
        ),
        "base_log_growth_anchor_cluster_variance_diagnostic": _decimal_text(
            base_cluster_variance
        ),
        "anchor_group_base_log_growth_mean_diagnostic": _decimal_text(
            anchor_group_base_log_mean
        ),
        "anchor_group_base_log_growth_t_lcb_diagnostic": _decimal_text(
            anchor_group_base_log_lcb
        ),
        "anchor_group_stress_log_growth_mean_diagnostic": _decimal_text(
            anchor_group_stress_log_mean
        ),
        "anchor_group_stress_log_growth_t_lcb_diagnostic": _decimal_text(
            anchor_group_stress_log_lcb
        ),
        "anchor_base_log_growth_means_diagnostic": [
            _decimal_text(value) for value in base_log_means
        ],
        "anchor_stress_log_growth_means": [
            _decimal_text(value) for value in stress_log_means
        ],
        "worst_anchor_base_log_growth_mean_diagnostic": _decimal_text(
            min(base_log_means)
        ),
        "worst_anchor_stress_log_growth_mean_diagnostic": _decimal_text(
            min(stress_log_means)
        ),
        "newest_anchor_base_log_growth_mean_diagnostic": _decimal_text(
            base_log_means[-1]
        ),
        "newest_anchor_stress_log_growth_mean": _decimal_text(stress_log_means[-1]),
        "base_between_anchor_log_growth_variance": _decimal_text(
            base_between_log_variance
        ),
        "stress_between_anchor_log_growth_variance": _decimal_text(
            stress_between_log_variance
        ),
        "most_influential_block_calendar_days": influence_block_size,
        "most_influential_block_role": _PINNED_POLICY["most_influential_block_role"],
        "base_log_growth_most_influential_block_start_by_anchor_diagnostic": [
            result[1] for result in base_influence_results
        ],
        "stress_log_growth_most_influential_block_start_by_anchor_diagnostic": [
            result[1] for result in stress_influence_results
        ],
        "base_log_growth_most_influential_block_sum_by_anchor_diagnostic": [
            _decimal_text(result[2]) for result in base_influence_results
        ],
        "stress_log_growth_most_influential_block_sum_by_anchor_diagnostic": [
            _decimal_text(result[2]) for result in stress_influence_results
        ],
        "pooled_base_log_growth_mean_after_top_block_removal_diagnostic": (
            _decimal_text(pooled_base_without_influence_mean)
        ),
        "pooled_stress_log_growth_mean_after_top_block_removal_diagnostic": (
            _decimal_text(pooled_stress_without_influence_mean)
        ),
        "pooled_base_arithmetic_return_mean_diagnostic": _decimal_text(
            sum((sum(vector, Decimal(0)) for vector in base_vectors), Decimal(0))
            / Decimal(sum(len(vector) for vector in base_vectors))
        ),
        "pooled_stress_arithmetic_return_mean_diagnostic": _decimal_text(
            sum((sum(vector, Decimal(0)) for vector in stress_vectors), Decimal(0))
            / Decimal(sum(len(vector) for vector in stress_vectors))
        ),
        "anchor_base_arithmetic_return_means_diagnostic": [
            _decimal_text(value) for value in base_arithmetic_means
        ],
        "anchor_stress_arithmetic_return_means_diagnostic": [
            _decimal_text(value) for value in stress_arithmetic_means
        ],
        "base_arithmetic_expected_shortfall_5pct": _decimal_text(base_es),
        "stress_arithmetic_expected_shortfall_5pct": _decimal_text(stress_es),
        "base_arithmetic_maximum_drawdown": _decimal_text(base_dd),
        "stress_arithmetic_maximum_drawdown": _decimal_text(stress_dd),
    }
    return {
        "schema": DECISION_SCHEMA,
        "authority_version": PROCESS_AUTHORITY_VERSION,
        "authority_scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
        "permitted_use": "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT",
        "explicit_non_authorities": [
            "CURRENT_REFIT",
            "MODEL_PROMOTION",
            "CAPITAL_ALLOCATION",
            "FUNDED_LIVE_TRADING",
            "LIVE_TRADING",
            "PROFITABILITY_CLAIM",
        ],
        "process_id": process_id,
        "process_definition_sha256": process_definition_sha256,
        "policy_sha256": _sha256(_PINNED_POLICY),
        "precommit_receipt_sha256": precommit_receipt_sha256,
        "evidence_receipt_sha256": evidence_receipt_sha256,
        "reconciliation_receipt_sha256": reconciliation_receipt_sha256,
        "evaluated_at_utc": reconciliation_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics": metrics,
        "gates": gates,
        "confirmatory_inference_status": confirmatory_status,
        "decision": decision,
        "failure_reasons": failure_reasons,
        "insufficiency_reasons": insufficiency_reasons,
    }


@dataclass(frozen=True, slots=True)
class HistoricalProcessDecision:
    _body_json: bytes

    def __post_init__(self) -> None:
        if type(self._body_json) is not bytes or len(self._body_json) > 512 * 1024:
            raise ProcessVerificationError("decision requires bounded canonical bytes")
        try:
            body = json.loads(self._body_json.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProcessVerificationError("decision is not JSON") from exc
        if type(body) is not dict or _canonical_bytes(body) != self._body_json:
            raise ProcessVerificationError("decision bytes are not canonical")
        _require_exact_keys(
            body,
            (
                "schema",
                "authority_version",
                "authority_scope",
                "permitted_use",
                "explicit_non_authorities",
                "process_id",
                "process_definition_sha256",
                "policy_sha256",
                "precommit_receipt_sha256",
                "evidence_receipt_sha256",
                "reconciliation_receipt_sha256",
                "evaluated_at_utc",
                "metrics",
                "gates",
                "confirmatory_inference_status",
                "decision",
                "failure_reasons",
                "insufficiency_reasons",
            ),
            "historical process decision",
        )
        if body["schema"] != DECISION_SCHEMA:
            raise ProcessVerificationError("decision schema mismatch")
        if (
            body["authority_version"] != PROCESS_AUTHORITY_VERSION
            or body["authority_scope"] != "OFFLINE_PROCESS_VALIDATION_ONLY"
            or body["permitted_use"] != "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT"
            or body["explicit_non_authorities"]
            != [
                "CURRENT_REFIT",
                "MODEL_PROMOTION",
                "CAPITAL_ALLOCATION",
                "FUNDED_LIVE_TRADING",
                "LIVE_TRADING",
                "PROFITABILITY_CLAIM",
            ]
        ):
            raise ProcessVerificationError(
                "decision attempts to exceed historical scope"
            )
        for key in (
            "process_definition_sha256",
            "policy_sha256",
            "precommit_receipt_sha256",
            "evidence_receipt_sha256",
            "reconciliation_receipt_sha256",
        ):
            _require_hex(body[key], f"decision {key}")
        _parse_utc(body["evaluated_at_utc"], "decision evaluated_at_utc")
        if type(body["metrics"]) is not dict or type(body["gates"]) is not dict:
            raise ProcessVerificationError(
                "decision metrics and gates must be mappings"
            )
        if any(type(value) is not bool for value in body["gates"].values()):
            raise ProcessVerificationError("decision gates must be exact booleans")
        confirmatory_status = body["confirmatory_inference_status"]
        if confirmatory_status not in (
            "PASS",
            "FAIL",
            "DEPENDENCE_INSUFFICIENT",
        ):
            raise ProcessVerificationError(
                "decision confirmatory inference status is invalid"
            )
        expected_failures = sorted(
            key for key, value in body["gates"].items() if not value
        )
        if confirmatory_status == "FAIL":
            expected_failures.append("stress_log_growth_confirmatory_lcb_above_zero")
            expected_failures.sort()
        if expected_failures:
            expected_decision = "OFFLINE_PROCESS_VALIDATION_FAIL"
            expected_insufficiency: list[str] = []
        elif confirmatory_status == "DEPENDENCE_INSUFFICIENT":
            expected_decision = "OFFLINE_PROCESS_VALIDATION_DEPENDENCE_INSUFFICIENT"
            expected_insufficiency = [
                "stress_log_growth_ips_positive_sequence_unresolved_through_lag_67"
            ]
        else:
            expected_decision = "OFFLINE_PROCESS_VALIDATION_PASS"
            expected_insufficiency = []
        if body["failure_reasons"] != expected_failures:
            raise ProcessVerificationError(
                "decision verdict and gates are inconsistent"
            )
        if (
            body["decision"] != expected_decision
            or body["insufficiency_reasons"] != expected_insufficiency
        ):
            raise ProcessVerificationError(
                "decision pass/fail/insufficient state is inconsistent"
            )

    def body(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


def _evaluate_impl(
    precommit_payload: dict[str, Any],
    evidence_payload: dict[str, Any],
    reconciliation_payload: dict[str, Any],
    verification_time: datetime,
) -> HistoricalProcessDecision:
    started = time.monotonic()
    if (
        type(verification_time) is not datetime
        or verification_time.tzinfo is None
        or verification_time.utcoffset() != timedelta(0)
    ):
        raise ProcessVerificationError("verification clock must be timezone-aware UTC")
    precommit, precommit_sha = _own_payload(
        precommit_payload,
        expected_schema=PRECOMMIT_SCHEMA,
        label="precommit payload",
    )
    evidence, evidence_sha = _own_payload(
        evidence_payload,
        expected_schema=EVIDENCE_SCHEMA,
        label="evidence payload",
    )
    reconciliation, reconciliation_sha = _own_payload(
        reconciliation_payload,
        expected_schema=RECONCILIATION_SCHEMA,
        label="reconciliation payload",
    )
    identity_registry = _EvidenceIdentityRegistry()
    identity_registry.register(precommit_sha, "precommit canonical payload receipt")
    (
        process,
        plans,
        authorized_at,
        access_not_before,
        access_deadline,
    ) = _validate_precommit_payload(precommit)
    for identity, domain, content_domain in (
        (
            precommit["policy_sha256"],
            "immutable process policy",
            "process policy content",
        ),
        (
            precommit["process_definition_sha256"],
            "immutable process definition",
            "process definition content",
        ),
    ):
        identity_registry.register(
            identity, domain, reusable_content_domain=content_domain
        )
    for key, identity in process["component_identities"].items():
        identity_registry.register(
            identity,
            f"immutable component identity {key}",
            reusable_content_domain="component executable content",
        )
    for key, identity in process["policy_identities"].items():
        identity_registry.register(
            identity,
            f"immutable policy identity {key}",
            reusable_content_domain="component policy content",
        )
    maximum_trusted_time = verification_time + timedelta(
        seconds=_PINNED_POLICY["trusted_clock_max_future_skew_seconds"]
    )
    if authorized_at > maximum_trusted_time:
        raise ProcessVerificationError("precommit claims a future trusted timestamp")
    identity_registry.register(evidence_sha, "evidence canonical payload receipt")
    _require_exact_keys(
        evidence,
        (
            "schema",
            "process_id",
            "precommit_payload_sha256",
            "process_definition_sha256",
            "evidence_access_started_at_utc",
            "execution_started_at_utc",
            "execution_completed_at_utc",
            "forward_oos_replay_lineage",
            "anchors",
        ),
        "evidence payload",
    )
    if evidence["process_id"] != precommit["process_id"]:
        raise ProcessVerificationError("evidence process_id mismatch")
    if evidence["precommit_payload_sha256"] != precommit_sha:
        raise ProcessVerificationError(
            "evidence does not bind the exact precommit payload"
        )
    process_sha = precommit["process_definition_sha256"]
    if evidence["process_definition_sha256"] != process_sha:
        raise ProcessVerificationError("evidence process definition mismatch")
    forward_oos_replay_lineage = _validate_forward_oos_replay_lineage(
        evidence["forward_oos_replay_lineage"], plans, precommit_sha
    )
    for key in (
        "producer_receipt_sha256",
        "forward_precommit_receipt_sha256",
        "code_identity_sha256",
        "ordered_anchor_descriptor_roster_sha256",
    ):
        identity_registry.register(
            forward_oos_replay_lineage[key],
            f"forward OOS replay lineage {key}",
        )
    for anchor_id in forward_oos_replay_lineage["ordered_anchor_ids"]:
        identity_registry.register(
            forward_oos_replay_lineage["anchor_descriptor_roster_sha256_by_id"][
                anchor_id
            ],
            f"{anchor_id} forward OOS descriptor roster",
        )
        identity_registry.register(
            forward_oos_replay_lineage["anchor_production_metadata_sha256_by_id"][
                anchor_id
            ],
            f"{anchor_id} forward OOS production metadata",
        )
    access_started = _parse_utc(
        evidence["evidence_access_started_at_utc"], "evidence access started"
    )
    execution_started = _parse_utc(
        evidence["execution_started_at_utc"], "execution started"
    )
    execution_completed = _parse_utc(
        evidence["execution_completed_at_utc"], "execution completed"
    )
    if any(
        forward_oos_replay_lineage[key] != evidence[evidence_key]
        for key, evidence_key in (
            ("evidence_access_started_at_utc", "evidence_access_started_at_utc"),
            ("execution_started_at_utc", "execution_started_at_utc"),
            ("execution_completed_at_utc", "execution_completed_at_utc"),
        )
    ):
        raise ProcessVerificationError(
            "causal evidence clocks do not match the sealed forward OOS receipt"
        )
    if not (
        authorized_at
        <= access_not_before
        <= access_started
        <= execution_started
        <= execution_completed
    ):
        raise ProcessVerificationError(
            "authorization/access/execution phase clocks are reversed"
        )
    if access_started > access_deadline:
        raise ProcessVerificationError(
            "evidence access began after its precommitted deadline"
        )
    if execution_completed > access_deadline:
        raise ProcessVerificationError(
            "execution completion must not exceed the access deadline"
        )
    if any(
        value > maximum_trusted_time
        for value in (access_started, execution_started, execution_completed)
    ):
        raise ProcessVerificationError("evidence claims a future trusted timestamp")
    for plan in plans:
        cutoff = _parse_utc(plan["selection_cutoff_utc"], "anchor selection cutoff")
        forward_end = datetime.combine(
            _parse_date(plan["forward_end_date"], "anchor forward end"),
            datetime.max.time().replace(microsecond=0),
            tzinfo=timezone.utc,
        )
        if cutoff >= access_started or forward_end >= access_started:
            raise ProcessVerificationError(
                "historical anchor market cutoff/end must precede real evidence access"
            )
    anchors_value = evidence["anchors"]
    if type(anchors_value) is not list or len(anchors_value) != len(plans):
        raise ProcessVerificationError("evidence anchor count differs from precommit")
    if len(anchors_value) > _MAX_ANCHORS:
        raise ProcessVerificationError("evidence anchor count exceeds hard bound")
    validated_anchors: list[dict[str, Any]] = []
    base_vectors: list[list[Decimal]] = []
    stress_vectors: list[list[Decimal]] = []
    used_broker_fill_ids: set[str] = set()
    total_days = 0
    for anchor, plan in zip(anchors_value, plans, strict=True):
        validated, base, stress = _validate_anchor_evidence(
            anchor,
            plan,
            forward_oos_replay_lineage,
            identity_registry,
            process["policy_identities"]["risk_policy_sha256"],
            process["policy_identities"]["cost_model_policy_sha256"],
            used_broker_fill_ids,
        )
        validated_anchors.append(validated)
        base_vectors.append(base)
        stress_vectors.append(stress)
        total_days += len(base)
        if total_days > _MAX_TOTAL_DAYS:
            raise ProcessVerificationError("total evidence calendar exceeds hard bound")
        if time.monotonic() - started > _MAX_VERIFICATION_SECONDS:
            raise ProcessVerificationError(
                "historical process verification exceeded runtime bound"
            )
    identity_registry.register(
        reconciliation_sha, "reconciliation canonical payload receipt"
    )
    _require_exact_keys(
        reconciliation,
        (
            "schema",
            "process_id",
            "precommit_payload_sha256",
            "evidence_payload_sha256",
            "process_definition_sha256",
            "reconciled_at_utc",
            "forward_oos_replay_lineage",
            "anchors",
        ),
        "reconciliation payload",
    )
    if reconciliation["process_id"] != precommit["process_id"]:
        raise ProcessVerificationError("reconciliation process_id mismatch")
    if reconciliation["precommit_payload_sha256"] != precommit_sha:
        raise ProcessVerificationError(
            "reconciliation does not bind the exact precommit payload"
        )
    if reconciliation["evidence_payload_sha256"] != evidence_sha:
        raise ProcessVerificationError(
            "reconciliation does not bind the exact evidence payload"
        )
    if reconciliation["process_definition_sha256"] != process_sha:
        raise ProcessVerificationError("reconciliation process definition mismatch")
    if reconciliation["forward_oos_replay_lineage"] != forward_oos_replay_lineage:
        raise ProcessVerificationError(
            "reconciliation does not bind the exact forward OOS replay lineage"
        )
    reconciled_at = _parse_utc(reconciliation["reconciled_at_utc"], "reconciled_at_utc")
    if (
        forward_oos_replay_lineage["reconciled_at_utc"]
        != reconciliation["reconciled_at_utc"]
    ):
        raise ProcessVerificationError(
            "causal reconciliation clock does not match the sealed forward OOS receipt"
        )
    if execution_completed > reconciled_at:
        raise ProcessVerificationError(
            "evidence/reconciliation phase clocks are reversed"
        )
    if reconciled_at > maximum_trusted_time:
        raise ProcessVerificationError(
            "reconciliation claims a future trusted timestamp"
        )
    if reconciled_at > access_deadline:
        raise ProcessVerificationError(
            "reconciliation must not exceed the access deadline"
        )
    reconciled_anchors = reconciliation["anchors"]
    if type(reconciled_anchors) is not list or len(reconciled_anchors) != len(
        validated_anchors
    ):
        raise ProcessVerificationError("reconciliation anchor count mismatch")
    for reconciled, anchor in zip(reconciled_anchors, validated_anchors, strict=True):
        _validate_reconciliation_anchor(reconciled, anchor, identity_registry)
        if time.monotonic() - started > _MAX_VERIFICATION_SECONDS:
            raise ProcessVerificationError(
                "historical process verification exceeded runtime bound"
            )
    body = _compute_decision_body(
        process_id=precommit["process_id"],
        process_definition_sha256=process_sha,
        precommit_receipt_sha256=precommit_sha,
        evidence_receipt_sha256=evidence_sha,
        reconciliation_receipt_sha256=reconciliation_sha,
        reconciliation_time=reconciled_at,
        anchors=validated_anchors,
        base_vectors=base_vectors,
        stress_vectors=stress_vectors,
    )
    return HistoricalProcessDecision(_canonical_bytes(body))


def evaluate_historical_process(
    precommit_payload: dict[str, Any],
    evidence_payload: dict[str, Any],
    reconciliation_payload: dict[str, Any],
) -> HistoricalProcessDecision:
    """Evaluate three exact canonical payloads for offline validation only.

    Decision-v6 fields ending in ``*_receipt_sha256`` identify the canonical
    input payload bytes; they are deterministic content receipts, not signatures.
    """

    with localcontext() as context:
        context.prec = _DECIMAL_CONTEXT_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return _evaluate_impl(
            precommit_payload,
            evidence_payload,
            reconciliation_payload,
            datetime.now(timezone.utc),
        )


def verify_historical_process_decision(
    decision: HistoricalProcessDecision | Mapping[str, Any],
    precommit_payload: dict[str, Any],
    evidence_payload: dict[str, Any],
    reconciliation_payload: dict[str, Any],
) -> bool:
    """Recompute an offline decision from the three exact canonical payloads."""

    expected = evaluate_historical_process(
        precommit_payload,
        evidence_payload,
        reconciliation_payload,
    )
    if isinstance(decision, HistoricalProcessDecision):
        presented = decision.body()
    elif type(decision) is dict:
        presented = _snapshot_bounded(decision)
    else:
        raise ProcessVerificationError(
            "decision must be a decision object or exact mapping"
        )
    if _canonical_bytes(presented) != expected._body_json:
        raise ProcessVerificationError(
            "historical process decision was tampered or spliced"
        )
    return True


__all__ = [
    "ANCHOR_OUTPUT_KEYS",
    "CELL_COUNT",
    "COMPONENT_IDENTITY_KEYS",
    "DECISION_SCHEMA",
    "EVIDENCE_SCHEMA",
    "FORWARD_OOS_ANCHOR_LINEAGE_SCHEMA",
    "FORWARD_OOS_REPLAY_LINEAGE_SCHEMA",
    "HistoricalProcessDecision",
    "INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON",
    "INSUFFICIENT_EVIDENCE",
    "INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION",
    "WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION",
    "INSUFFICIENT_POSTSEARCH_STAGE",
    "INSUFFICIENT_PRESEARCH_BOUNDARY_REASON",
    "INSUFFICIENT_PRESEARCH_STAGE",
    "LEDGER_IDENTITY_KEYS",
    "NO_LOCAL_WINNER",
    "PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION",
    "PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA",
    "PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA",
    "PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION",
    "PINNED_REGIME_FIT_CONTRACT_VERSION",
    "PINNED_WINNER_EVIDENCE_LEDGER_VERSION",
    "PINNED_WINNER_SELECTION_CONTRACT_VERSION",
    "POLICY_IDENTITY_KEYS",
    "PRECOMMIT_SCHEMA",
    "PROCESS_AUTHORITY_VERSION",
    "ProcessVerificationError",
    "RECONCILIATION_SCHEMA",
    "REGIMES",
    "REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION",
    "REGIME_DETECTOR_INTEGER_MAXIMA",
    "REGIME_DETECTOR_PARAMETER_KEYS",
    "REGIME_DETECTOR_RECORD_SCHEMA_VERSION",
    "REGIME_DETECTOR_WEIGHT_GROUPS",
    "REGIME_UNOBSERVED",
    "TERMINAL_TECHNICAL",
    "TERMINAL_TECHNICAL_REASON",
    "TERMINAL_TECHNICAL_SOURCE_STATE",
    "REGIME_TRAIN_SUPPORT_SCHEMA_VERSION",
    "REGIME_TRAIN_SUPPORT_VECTOR_ENCODING",
    "REGIME_TRAIN_SUPPORT_WARMUP_POLICY",
    "SYMBOLS",
    "TIMEFRAMES",
    "canonical_sha256",
    "evaluate_historical_process",
    "expected_cell_roster",
    "pinned_process_policy",
    "verify_historical_process_decision",
]
