"""Lean historical Book construction for the complete FXPM specialist family.

The family is fixed at 100 symbols x six native timeframes x four regimes.
Winner V11 decides local process admissibility inside each cell.  This module
replays those exact Winner requests, represents non-economic process outcomes
with canonical typed decisions, and reports a flat Benjamini-Yekutieli
diagnostic across all 2,400 hypotheses.  BY is diagnostic only: a local Winner
is a historical candidate pending a separate repeated whole-process Causal
decision, never an individual family-wide FDR discovery.

Source availability and regime fitting are symbol/timeframe facts.  Exactly one
canonical source-group decision and one canonical regime-fit decision must be
shared by the four sibling regimes.  A technical or unsearched cell therefore
cannot be relabelled as terminal source absence with an opaque receipt.

The four regimes are possible slots, not forced empirical outcomes.  A fitted
train partition may bind an exact zero-label slot as ``REGIME_UNOBSERVED``;
that slot remains in the 2,400-hypothesis roster with p=1 and zero attempts.
An observed slot whose authenticated source cannot satisfy the precommitted
Horizon/Winner evidence geometry is instead ``INSUFFICIENT_EVIDENCE``.  That
state is a complete, non-economic abstention: it never becomes a no-edge
verdict, never carries a selected Winner/refit leaf, and never vetoes a
sibling cell that has adequate evidence.

This boundary is deterministic, bounded, historical-only, and grants no live,
order, capital, or profitability authority.  Its hashes are reproducibility
checksums, not signatures or an adversarial in-process trust boundary.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import math
import re
from typing import Any, Final

import pm_winner_selection as winner
import pm_training_horizon as horizon


BOOK_MULTIPLICITY_CONTRACT_VERSION: Final = "fxpm.book-multiplicity.v12"
BOOK_MULTIPLICITY_SCOPE: Final = "OFFLINE_HISTORICAL_BOOK_ONLY"
PINNED_WINNER_SELECTION_CONTRACT_VERSION: Final = "fxpm.winner-selection.v13"
PINNED_WINNER_SELECTION_REQUEST_VERSION: Final = "fxpm.winner-selection-request.v1"
PINNED_WINNER_ATTEMPT_LEDGER_VERSION: Final = "fxpm.winner-attempt-ledger.v2"
PINNED_WINNER_EVIDENCE_LEDGER_VERSION: Final = "fxpm.winner-evidence-ledger.v4"
PINNED_WINNER_TRADE_POWER_BINDING_VERSION: Final = "fxpm.winner-trade-power-binding.v1"
SOURCE_GROUP_CONTRACT_VERSION: Final = "fxpm.historical-source-group.v2"
REGIME_FIT_CONTRACT_VERSION: Final = "fxpm.historical-regime-fit.v3"
REGIME_DETECTOR_RECORD_SCHEMA_VERSION: Final = "fxpm.regime-detector-record/v1"
PROCESS_STATE_CONTRACT_VERSION: Final = "fxpm.historical-process-state.v4"
INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION: Final = (
    "fxpm.historical-insufficient-evidence-binding.v2"
)
HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION: Final = (
    "fxpm.historical-horizon-winner-cross-binding.v1"
)
WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION: Final = (
    "fxpm.winner-raw-evidence-production.v1"
)

TIMEFRAME_ORDER: Final = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIME_ORDER: Final = ("TREND", "RANGE", "BREAKOUT", "CHOP")
PINNED_SYMBOL_ORDER: Final = (
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

SOURCE_GROUP_SIZE: Final = 100 * 6
FAMILY_SIZE: Final = SOURCE_GROUP_SIZE * 4
MULTIPLICITY_METHOD: Final = "BY"
TARGET_FDR: Final = 0.10
SELECTION_RULE: Final = (
    "WINNER_V11_LOCAL_ADMISSIBILITY_PENDING_WHOLE_PROCESS_CAUSAL_VALIDATION"
)
PINNED_P_VALUE_CONTRACT_SHA256: Final = winner.PINNED_BOOK_P_VALUE_CONTRACT_SHA256

WINNER_EVIDENCE: Final = "WINNER_SELECTION_DECISION"
TERMINAL_SOURCE_ABSENCE: Final = "TERMINAL_SOURCE_ABSENCE"
TECHNICAL_FAILURE: Final = "TECHNICAL_FAILURE"
UNSEARCHED: Final = "UNSEARCHED"
REGIME_UNOBSERVED: Final = "REGIME_UNOBSERVED"
INSUFFICIENT_EVIDENCE: Final = "INSUFFICIENT_EVIDENCE"
INSUFFICIENT_PRESEARCH_BOUNDARY_REASON: Final = "SOURCE_BOUNDARY_SHORTFALL"
INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON: Final = (
    "ALL_PRECOMMITTED_ATTEMPTS_PROCESS_REJECTED"
)
INSUFFICIENT_PRESEARCH_STAGE: Final = "PRE_SEARCH_BOUNDARY"
INSUFFICIENT_POSTSEARCH_STAGE: Final = "POST_SEARCH_ALL_PROCESS_REJECTED"
AVAILABLE: Final = "AVAILABLE"
TERMINAL_UNAVAILABLE: Final = "TERMINAL_UNAVAILABLE"
REGIME_FITTED: Final = "FITTED"
REGIME_FIT_TECHNICAL_FAILURE: Final = "TECHNICAL_FAILURE"
REGIME_NOT_APPLICABLE: Final = "NOT_APPLICABLE_SOURCE_UNAVAILABLE"
REGIME_UNOBSERVED_REASON: Final = "ZERO_OBSERVED_OPTIMIZER_TRAIN_LABELS"
REGIME_TRAIN_SUPPORT_SCHEMA_VERSION: Final = "fxpm.regime-train-support/v1"
REGIME_TRAIN_SUPPORT_VECTOR_ENCODING: Final = "fxpm.regime-live-labels-nul/v1"
REGIME_TRAIN_SUPPORT_WARMUP_POLICY: Final = "CURRENT_AND_SOURCE_BAR_NOT_WARMUP"
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

_PROCESS_STATE_KINDS: Final = frozenset(
    {
        TERMINAL_SOURCE_ABSENCE,
        TECHNICAL_FAILURE,
        UNSEARCHED,
        REGIME_UNOBSERVED,
        INSUFFICIENT_EVIDENCE,
    }
)
_OUTPUT_STATES: Final = frozenset(
    {
        "WINNER",
        "NO_WINNER",
        "SOURCE_UNAVAILABLE",
        TECHNICAL_FAILURE,
        UNSEARCHED,
        REGIME_UNOBSERVED,
        INSUFFICIENT_EVIDENCE,
    }
)
_MAX_DECISION_BYTES: Final = 8 * 1024 * 1024
_MAX_REASON_BYTES: Final = 160
_MAX_ATTEMPT_COUNT: Final = 1_000_000
_MAX_TRAIN_LABEL_COUNT: Final = 10_000_000
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
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON = re.compile(r"^[A-Z0-9_.:-]{1,160}$")


class _BookError(ValueError):
    pass


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


def _exact_dict(value: Any, keys: frozenset[str], code: str) -> dict[str, Any]:
    if type(value) is not dict or frozenset(value) != keys:
        raise _BookError(code)
    return value


def _hex64(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        raise _BookError(code)
    return value


def _optional_hex64(value: Any, code: str) -> str | None:
    if value is None:
        return None
    return _hex64(value, code)


def _reason(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > _MAX_REASON_BYTES
        or _SAFE_REASON.fullmatch(value) is None
    ):
        raise _BookError(code)
    return value


def _bounded_id(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 256
        or len(value.encode("utf-8")) > 512
    ):
        raise _BookError(code)
    return value


def _exact_int(value: Any, minimum: int, maximum: int, code: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise _BookError(code)
    return value


def _regime_counts(value: Any, code: str) -> dict[str, int]:
    if type(value) is not dict or frozenset(value) != frozenset(REGIME_ORDER):
        raise _BookError(code)
    return {
        regime: _exact_int(value[regime], 0, _MAX_TRAIN_LABEL_COUNT, code)
        for regime in REGIME_ORDER
    }


def _exact_bool(value: Any, code: str) -> bool:
    if type(value) is not bool:
        raise _BookError(code)
    return value


def _canonical_utc(value: Any, code: str) -> str:
    text = _bounded_id(value, code)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        normalized = parsed.astimezone(timezone.utc).isoformat()
    except Exception as exc:
        raise _BookError(code) from exc
    if text != normalized:
        raise _BookError(code)
    return text


def _regime_detector_parameters(value: Any) -> dict[str, int | float]:
    parameters = _exact_dict(
        value,
        frozenset(REGIME_DETECTOR_PARAMETER_KEYS),
        "REGIME_DETECTOR_PARAMETERS_SCHEMA_INVALID",
    )
    normalized: dict[str, int | float] = {}
    for key in REGIME_DETECTOR_PARAMETER_KEYS:
        item = parameters[key]
        if key in _REGIME_DETECTOR_INTEGER_PARAMETERS:
            normalized[key] = _exact_int(
                item,
                1,
                REGIME_DETECTOR_INTEGER_MAXIMA[key],
                "REGIME_DETECTOR_INTEGER_PARAMETER_INVALID",
            )
        elif type(item) not in {int, float} or not math.isfinite(item):
            raise _BookError("REGIME_DETECTOR_FLOAT_PARAMETER_INVALID")
        else:
            normalized[key] = item
    for key in (
        "gap_min",
        *REGIME_DETECTOR_WEIGHT_GROUPS["trend"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["range"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["breakout"],
        *REGIME_DETECTOR_WEIGHT_GROUPS["chop"],
    ):
        if not 0.0 <= float(normalized[key]) <= 1.0:
            raise _BookError("REGIME_DETECTOR_UNIT_PARAMETER_INVALID")
    for key in ("bb_squeeze_percentile", "adx_trend_threshold"):
        if not 0.0 <= float(normalized[key]) <= 100.0:
            raise _BookError("REGIME_DETECTOR_PERCENT_PARAMETER_INVALID")
    if not 0.0 < float(normalized["bb_std"]) <= 10.0:
        raise _BookError("REGIME_DETECTOR_BB_STD_INVALID")
    if not 0.0 <= float(normalized["structure_atr_mult"]) <= 10.0:
        raise _BookError("REGIME_DETECTOR_STRUCTURE_MULTIPLIER_INVALID")
    for keys in REGIME_DETECTOR_WEIGHT_GROUPS.values():
        if not math.isclose(
            sum(float(normalized[key]) for key in keys),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise _BookError("REGIME_DETECTOR_WEIGHT_SUM_INVALID")
    return normalized


def _probability(value: Any, code: str) -> float:
    if type(value) is not float or not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise _BookError(code)
    return value


def _positive_float(value: Any, maximum: float, code: str) -> float:
    if (
        type(value) is not float
        or not math.isfinite(value)
        or not 0.0 < value <= maximum
    ):
        raise _BookError(code)
    return value


def _cell_tuple(ordinal: int) -> tuple[str, str, str]:
    symbol_block, regime_index = divmod(ordinal, len(REGIME_ORDER))
    symbol_index, timeframe_index = divmod(symbol_block, len(TIMEFRAME_ORDER))
    return (
        PINNED_SYMBOL_ORDER[symbol_index],
        TIMEFRAME_ORDER[timeframe_index],
        REGIME_ORDER[regime_index],
    )


def _group_tuple(group_ordinal: int) -> tuple[str, str]:
    symbol_index, timeframe_index = divmod(group_ordinal, len(TIMEFRAME_ORDER))
    return PINNED_SYMBOL_ORDER[symbol_index], TIMEFRAME_ORDER[timeframe_index]


def compute_cell_id(symbol: Any, timeframe: Any, regime: Any) -> str | None:
    if (
        type(symbol) is not str
        or symbol not in PINNED_SYMBOL_ORDER
        or type(timeframe) is not str
        or timeframe not in TIMEFRAME_ORDER
        or type(regime) is not str
        or regime not in REGIME_ORDER
    ):
        return None
    return winner.compute_cell_id(symbol, timeframe, regime)


def compute_source_group_id(symbol: Any, timeframe: Any) -> str | None:
    if (
        type(symbol) is not str
        or symbol not in PINNED_SYMBOL_ORDER
        or type(timeframe) is not str
        or timeframe not in TIMEFRAME_ORDER
    ):
        return None
    return _sha256_json(
        {
            "identity_contract": "FXPM_SYMBOL_TIMEFRAME_SOURCE_GROUP_V1",
            "symbol": symbol,
            "timeframe": timeframe,
        }
    )


def _family_precommitment() -> dict[str, Any]:
    return {
        "contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "family_size": FAMILY_SIZE,
        "source_group_size": SOURCE_GROUP_SIZE,
        "method": MULTIPLICITY_METHOD,
        "p_value_contract_sha256": PINNED_P_VALUE_CONTRACT_SHA256,
        "regimes": list(REGIME_ORDER),
        "selection_rule": SELECTION_RULE,
        "symbols": list(PINNED_SYMBOL_ORDER),
        "target_fdr": TARGET_FDR,
        "timeframes": list(TIMEFRAME_ORDER),
        "by_role": "DIAGNOSTIC_ONLY",
        "winner_selection_contract_version": (PINNED_WINNER_SELECTION_CONTRACT_VERSION),
        "winner_selection_request_version": PINNED_WINNER_SELECTION_REQUEST_VERSION,
        "winner_attempt_ledger_version": PINNED_WINNER_ATTEMPT_LEDGER_VERSION,
        "winner_evidence_ledger_version": PINNED_WINNER_EVIDENCE_LEDGER_VERSION,
        "winner_trade_power_binding_version": (
            PINNED_WINNER_TRADE_POWER_BINDING_VERSION
        ),
    }


PINNED_FAMILY_PRECOMMITMENT_SHA256: Final = _sha256_json(_family_precommitment())


@dataclass(frozen=True, slots=True)
class HistoricalSourceGroupDecision:
    """Canonical source availability for one symbol/timeframe group."""

    _body_json: bytes
    _receipt_sha256: str

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    def to_dict(self) -> dict[str, Any]:
        body = json.loads(self._body_json.decode("ascii"))
        body["receipt_sha256"] = self._receipt_sha256
        return body


@dataclass(frozen=True, slots=True)
class RegimeFitDecision:
    """Canonical shared regime-controller fit for one symbol/timeframe group."""

    _body_json: bytes
    _receipt_sha256: str

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    def to_dict(self) -> dict[str, Any]:
        body = json.loads(self._body_json.decode("ascii"))
        body["receipt_sha256"] = self._receipt_sha256
        return body


@dataclass(frozen=True, slots=True)
class HistoricalProcessStateDecision:
    """Canonical non-economic outcome for exactly one historical cell."""

    _body_json: bytes
    _receipt_sha256: str

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    @property
    def state_kind(self) -> str:
        try:
            value = json.loads(self._body_json.decode("ascii"))["state_kind"]
            return value if type(value) is str else "INVALID"
        except Exception:
            return "INVALID"

    def to_dict(self) -> dict[str, Any]:
        body = json.loads(self._body_json.decode("ascii"))
        body["receipt_sha256"] = self._receipt_sha256
        return body


_SOURCE_KEYS: Final = frozenset(
    {
        "contract_version",
        "source_group_id",
        "symbol",
        "timeframe",
        "availability",
        "reason_code",
        "source_contract_sha256",
        "source_group_evidence_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "native_source_sha256",
        "acquisition_attempts_exact",
        "terminal_proof_sha256",
        "terminal",
        "complete",
    }
)
_REGIME_FIT_KEYS: Final = frozenset(
    {
        "contract_version",
        "source_group_id",
        "source_group_decision_sha256",
        "symbol",
        "timeframe",
        "fit_state",
        "reason_code",
        "regime_fit_policy_sha256",
        "detector_process_sha256",
        "regime_fit_evidence_sha256",
        "four_regime_mapping_sha256",
        "regime_params_sha256",
        "regime_detector_record",
        "regime_detector_record_sha256",
        "train_regime_support",
        "complete",
    }
)
_REGIME_DETECTOR_RECORD_KEYS: Final = frozenset(
    {
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
    }
)
_PROCESS_STATE_KEYS: Final = frozenset(
    {
        "contract_version",
        "cell",
        "state_kind",
        "reason_code",
        "source_group_id",
        "source_group_decision_sha256",
        "source_contract_sha256",
        "regime_receipt_sha256",
        "train_regime_support_receipt_sha256",
        "insufficient_evidence_binding",
        "attempted_trials_exact",
        "terminal",
        "complete",
    }
)
_CELL_IDENTITY_KEYS: Final = frozenset({"cell_id", "symbol", "timeframe", "regime"})
_INSUFFICIENT_BINDING_KEYS: Final = frozenset(
    {
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
    }
)


def _horizon_plan_body(value: Any) -> dict[str, Any]:
    """Reopen the sealed Horizon plan through its own authority."""

    try:
        valid = horizon._valid_plan(value)
    except Exception as exc:
        raise _BookError("INSUFFICIENT_HORIZON_PLAN_INVALID") from exc
    if valid is not True:
        raise _BookError("INSUFFICIENT_HORIZON_PLAN_INVALID")
    body = value.to_dict()
    if (
        type(body) is not dict
        or _sha256_json(body) != value.receipt_sha256
        or _canonical_bytes(body) != value._body_json
    ):
        raise _BookError("INSUFFICIENT_HORIZON_PLAN_INVALID")
    return body


def _fold_geometry(plan_body: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    geometry: list[dict[str, Any]] = []
    reference_axes: tuple[tuple[str, str], ...] | None = None
    for candidate, registry in zip(
        plan_body["candidates"], plan_body["candidate_registry"], strict=True
    ):
        folds = [
            fold
            for fold in candidate["folds"]
            if fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"
        ]
        geometry.append(
            {
                "candidate_id": candidate["candidate_id"],
                "candidate_definition_sha256": candidate["candidate_definition_sha256"],
                "boundary_shortfall": registry["boundary_shortfall"],
                "folds": folds,
            }
        )
        if registry["boundary_shortfall"]:
            if folds:
                raise _BookError("INSUFFICIENT_BOUNDARY_FOLD_GEOMETRY_INVALID")
            continue
        axes = tuple(
            (
                fold["validation_start_utc"],
                fold["validation_end_exclusive_utc"],
            )
            for fold in folds
        )
        if not axes or (reference_axes is not None and axes != reference_axes):
            raise _BookError("INSUFFICIENT_CAUSAL_FOLD_GEOMETRY_INVALID")
        reference_axes = axes
    return geometry, 0 if reference_axes is None else len(reference_axes)


def _winner_fold_axes(value: Any) -> tuple[tuple[str, str], ...]:
    if type(value) is not list:
        raise _BookError("INSUFFICIENT_WINNER_FOLD_PLAN_INVALID")
    axes: list[tuple[str, str]] = []
    for fold in value:
        if type(fold) is not dict:
            raise _BookError("INSUFFICIENT_WINNER_FOLD_PLAN_INVALID")
        start_text = fold.get("anchor_as_of_utc")
        start_day = fold.get("oos_start_date_utc")
        end_day = fold.get("oos_end_date_utc")
        if (
            type(start_text) is not str
            or type(start_day) is not str
            or type(end_day) is not str
        ):
            raise _BookError("INSUFFICIENT_WINNER_FOLD_PLAN_INVALID")
        try:
            start = datetime.fromisoformat(start_text.replace("Z", "+00:00"))
            start_date = datetime.fromisoformat(start_day).date()
            end_date = datetime.fromisoformat(end_day).date()
        except Exception as exc:
            raise _BookError("INSUFFICIENT_WINNER_FOLD_PLAN_INVALID") from exc
        if start.tzinfo is None or start.date() != start_date or end_date < start_date:
            raise _BookError("INSUFFICIENT_WINNER_FOLD_PLAN_INVALID")
        end_exclusive = datetime.combine(
            end_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        axes.append(
            (
                start.astimezone(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
                end_exclusive.isoformat(timespec="seconds").replace("+00:00", "Z"),
            )
        )
    return tuple(axes)


def _insufficient_binding_body(value: Any) -> dict[str, Any]:
    body = _exact_dict(
        value,
        _INSUFFICIENT_BINDING_KEYS,
        "INSUFFICIENT_BINDING_SCHEMA_INVALID",
    )
    if body["schema_version"] != INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION:
        raise _BookError("INSUFFICIENT_BINDING_VERSION_INVALID")
    if body["terminal_stage"] not in {
        INSUFFICIENT_PRESEARCH_STAGE,
        INSUFFICIENT_POSTSEARCH_STAGE,
    }:
        raise _BookError("INSUFFICIENT_STAGE_INVALID")
    _reason(body["reason_code"], "INSUFFICIENT_REASON_INVALID")
    selection_cutoff = body["selection_cutoff_utc"]
    if type(selection_cutoff) is not str or not selection_cutoff.endswith("Z"):
        raise _BookError("INSUFFICIENT_CUTOFF_INVALID")
    try:
        parsed_cutoff = datetime.fromisoformat(selection_cutoff.replace("Z", "+00:00"))
    except Exception as exc:
        raise _BookError("INSUFFICIENT_CUTOFF_INVALID") from exc
    if parsed_cutoff.tzinfo is None:
        raise _BookError("INSUFFICIENT_CUTOFF_INVALID")
    canonical_cutoff = (
        parsed_cutoff.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    if selection_cutoff != canonical_cutoff:
        raise _BookError("INSUFFICIENT_CUTOFF_INVALID")
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
        _hex64(body[key], "INSUFFICIENT_BINDING_HASH_INVALID")
    _exact_int(
        body["source_prefix_rows"],
        1,
        10_000_000_000,
        "INSUFFICIENT_SOURCE_ROWS_INVALID",
    )
    fold_count = _exact_int(
        body["causal_fold_count"], 0, 256, "INSUFFICIENT_FOLD_COUNT_INVALID"
    )
    planned = _exact_int(
        body["planned_attempt_count"],
        1,
        _MAX_ATTEMPT_COUNT,
        "INSUFFICIENT_PLANNED_ATTEMPTS_INVALID",
    )
    rejected = _exact_int(
        body["process_rejected_attempt_count"],
        0,
        _MAX_ATTEMPT_COUNT,
        "INSUFFICIENT_REJECTED_ATTEMPTS_INVALID",
    )
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
    if body["terminal_stage"] == INSUFFICIENT_PRESEARCH_STAGE:
        if (
            body["reason_code"] != INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
            or fold_count != 0
            or rejected != 0
            or any(body[key] is not None for key in process_keys)
            or any(body[key] is not None for key in cross_binding_keys)
        ):
            raise _BookError("INSUFFICIENT_PRESEARCH_SEMANTICS_INVALID")
    else:
        if (
            body["reason_code"] != INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
            or fold_count < int(winner.PINNED_SELECTION_POLICY["min_causal_folds"])
            or rejected != planned
            or body["horizon_winner_cross_binding_schema_version"]
            != HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
            or body["winner_raw_production_schema_version"]
            != WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION
        ):
            raise _BookError("INSUFFICIENT_POSTSEARCH_SEMANTICS_INVALID")
        for key in process_keys:
            _hex64(body[key], "INSUFFICIENT_PROCESS_REJECTION_HASH_INVALID")
        for key in (
            "horizon_winner_cross_binding_content_sha256",
            "horizon_winner_cross_binding_mapping_sha256",
            "winner_raw_production_receipt_sha256",
        ):
            _hex64(body[key], "INSUFFICIENT_CROSS_BINDING_HASH_INVALID")
    return body


def _horizon_winner_cross_binding_identity(
    encoded: Any,
    *,
    plan_body: dict[str, Any],
    source_body: dict[str, Any],
    fit_body: dict[str, Any],
    process_rejection_request: winner.WinnerSelectionRequest,
    process_rejection_decision: winner.WinnerSelectionDecision,
    winner_raw_production_receipt_bytes: Any,
) -> tuple[str, str, str, str, str]:
    """Reopen the pre-evidence bijection between distinct Horizon/Winner schemas."""

    if (
        type(encoded) is not bytes
        or not encoded
        or len(encoded) > winner.MAX_REQUEST_BYTES
    ):
        raise _BookError("INSUFFICIENT_CROSS_BINDING_BYTES_INVALID")
    try:
        binding = json.loads(encoded.decode("ascii"))
    except Exception as exc:
        raise _BookError("INSUFFICIENT_CROSS_BINDING_BYTES_INVALID") from exc
    top_keys = frozenset(
        {
            "schema_version",
            "producer_request_sha256",
            "anchor_id",
            "selection_cutoff_utc",
            "process_precommit_receipt_sha256",
            "source_group_decision_sha256",
            "regime_fit_decision_sha256",
            "native_prefix_index_sha256",
            "symbol",
            "timeframe",
            "regime",
            "artifact_namespace",
            "horizon_roster",
            "winner_roster",
            "mapping_count",
            "mapping_sha256",
            "mapping",
            "bijective_coverage",
            "created_before_winner_evidence_access",
            "confers_causal_current_live_or_capital_authority",
        }
    )
    _exact_dict(binding, top_keys, "INSUFFICIENT_CROSS_BINDING_SCHEMA_INVALID")
    if _canonical_bytes(binding) != encoded:
        raise _BookError("INSUFFICIENT_CROSS_BINDING_NONCANONICAL")
    plan_cell = plan_body["cell"]
    plan_anchor = plan_body["anchor"]
    prefix = plan_body["native_prefix"]
    if (
        binding["schema_version"]
        != HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
        or binding["anchor_id"] != plan_anchor["anchor_id"]
        or binding["selection_cutoff_utc"]
        != plan_anchor["selection_cutoff_utc"]
        or binding["process_precommit_receipt_sha256"]
        != plan_anchor["process_precommit_receipt_sha256"]
        or binding["source_group_decision_sha256"] != _sha256_json(source_body)
        or binding["regime_fit_decision_sha256"] != _sha256_json(fit_body)
        or binding["native_prefix_index_sha256"]
        != prefix["prefix_index_sha256"]
        or {
            "symbol": binding["symbol"],
            "timeframe": binding["timeframe"],
            "regime": binding["regime"],
        }
        != plan_cell
        or binding["bijective_coverage"] is not True
        or binding["created_before_winner_evidence_access"] is not True
        or binding["confers_causal_current_live_or_capital_authority"] is not False
        or type(binding["artifact_namespace"]) is not str
        or not 1 <= len(binding["artifact_namespace"]) <= 512
    ):
        raise _BookError("INSUFFICIENT_CROSS_BINDING_IDENTITY_INVALID")
    _hex64(binding["producer_request_sha256"], "INSUFFICIENT_PRODUCER_REQUEST_INVALID")
    for key in (
        "process_precommit_receipt_sha256",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "native_prefix_index_sha256",
        "mapping_sha256",
    ):
        _hex64(binding[key], "INSUFFICIENT_CROSS_BINDING_HASH_INVALID")

    horizon_roster = _exact_dict(
        binding["horizon_roster"],
        frozenset(
            {
                "plan_receipt_sha256",
                "source_prefix_receipt_sha256",
                "candidate_registry_sha256",
                "trial_registry_sha256",
                "attempt_roster_sha256",
                "evidence_roster_sha256",
            }
        ),
        "INSUFFICIENT_CROSS_BINDING_HORIZON_ROSTER_INVALID",
    )
    expected_horizon_roster = {
        "plan_receipt_sha256": _sha256_json(plan_body),
        "source_prefix_receipt_sha256": plan_body["source_prefix_receipt_sha256"],
        "candidate_registry_sha256": plan_body["candidate_registry_sha256"],
        "trial_registry_sha256": plan_body["trial_registry_sha256"],
        "attempt_roster_sha256": plan_body["attempt_roster_sha256"],
        "evidence_roster_sha256": plan_body["evidence_roster_sha256"],
    }
    if horizon_roster != expected_horizon_roster:
        raise _BookError("INSUFFICIENT_CROSS_BINDING_HORIZON_SPLICE")

    precommitment = process_rejection_request.precommitment.to_dict()
    attempt_ledger = process_rejection_request.attempt_ledger.to_dict()
    winner_roster = _exact_dict(
        binding["winner_roster"],
        frozenset(
            {
                "precommitment_sha256",
                "attempt_ledger_sha256",
                "frozen_receipt_sha256",
                "process_registry_sha256",
                "horizon_registry_sha256",
                "trial_registry_sha256",
                "attempts_sha256",
                "causal_fold_plan_sha256",
                "source_contract_sha256",
                "cost_contract_sha256",
                "cost_stress_contract_sha256",
            }
        ),
        "INSUFFICIENT_CROSS_BINDING_WINNER_ROSTER_INVALID",
    )
    expected_winner_roster = {
        "precommitment_sha256": _sha256_json(precommitment),
        "attempt_ledger_sha256": _sha256_json(attempt_ledger),
        "process_registry_sha256": precommitment["process_registry_sha256"],
        "horizon_registry_sha256": precommitment["horizon_registry_sha256"],
        "trial_registry_sha256": precommitment["trial_registry_sha256"],
        "attempts_sha256": attempt_ledger["attempts_sha256"],
        "causal_fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
        "source_contract_sha256": precommitment["source_contract_sha256"],
        "cost_contract_sha256": precommitment["cost_contract_sha256"],
        "cost_stress_contract_sha256": precommitment[
            "cost_stress_contract_sha256"
        ],
    }
    _hex64(
        winner_roster["frozen_receipt_sha256"],
        "INSUFFICIENT_CROSS_BINDING_FROZEN_RECEIPT_INVALID",
    )
    if any(
        winner_roster[key] != value
        for key, value in expected_winner_roster.items()
    ):
        raise _BookError("INSUFFICIENT_CROSS_BINDING_WINNER_SPLICE")

    plan_attempts = plan_body["attempts"]
    winner_attempts = attempt_ledger["attempts"]
    mapping = binding["mapping"]
    if (
        type(mapping) is not list
        or type(binding["mapping_count"]) is not int
        or type(binding["mapping_count"]) is bool
        or binding["mapping_count"] != len(mapping)
        or len(mapping) != len(plan_attempts)
        or len(mapping) != len(winner_attempts)
        or binding["mapping_sha256"] != _sha256_json(mapping)
    ):
        raise _BookError("INSUFFICIENT_CROSS_BINDING_MAPPING_INVALID")
    candidates = {
        item["candidate_id"]: item for item in plan_body["candidates"]
    }
    trials = {item["trial_id"]: item for item in plan_body["trials"]}
    expected_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for item in plan_body["expected_evaluations"]:
        expected_by_attempt.setdefault(item["attempt_id"], []).append(item)
    mapping_keys = frozenset(
        {
            "ordinal",
            "horizon_attempt_id",
            "horizon_candidate_id",
            "horizon_candidate_definition_sha256",
            "horizon_trial_id",
            "horizon_trial_definition_sha256",
            "winner_attempt_id",
            "winner_process_candidate_id",
            "process_sha256",
            "horizon_process_sha256",
            "trial_definition_sha256",
            "refit_recipe_sha256",
            "horizon_execution_contract",
            "source_count_receipt",
            "fold_mapping",
            "fold_mapping_sha256",
        }
    )
    execution_keys = frozenset(
        {"lookback_kind", "lookback_value", "source_bar_count"}
    )
    source_count_keys = frozenset(
        {
            "schema_version",
            "anchor_id",
            "symbol",
            "timeframe",
            "regime",
            "horizon_plan_receipt_sha256",
            "native_prefix_index_sha256",
            "native_prefix_content_sha256",
            "horizon_attempt_id",
            "horizon_candidate_id",
            "horizon_candidate_definition_sha256",
            "lookback_kind",
            "lookback_value",
            "fit_start_utc",
            "fit_end_exclusive_utc",
            "latest_closed_bar_utc",
            "source_bar_count",
            "fit_frame_sha256",
            "receipt_sha256",
        }
    )
    fold_keys = frozenset(
        {
            "horizon_fold_id",
            "horizon_fold_definition_sha256",
            "horizon_context_sha256",
            "winner_fold_id",
            "validation_start_utc",
            "validation_end_exclusive_utc",
        }
    )
    for ordinal, (row, planned, attempted) in enumerate(
        zip(mapping, plan_attempts, winner_attempts, strict=True)
    ):
        _exact_dict(row, mapping_keys, "INSUFFICIENT_CROSS_BINDING_ROW_INVALID")
        candidate = candidates.get(planned["candidate_id"])
        trial = trials.get(planned["trial_id"])
        if candidate is None or trial is None:
            raise _BookError("INSUFFICIENT_CROSS_BINDING_HORIZON_ID_INVALID")
        expected_row = {
            "ordinal": ordinal,
            "horizon_attempt_id": planned["attempt_id"],
            "horizon_candidate_id": planned["candidate_id"],
            "horizon_candidate_definition_sha256": candidate[
                "candidate_definition_sha256"
            ],
            "horizon_trial_id": planned["trial_id"],
            "horizon_trial_definition_sha256": trial["trial_definition_sha256"],
            "winner_attempt_id": attempted["attempt_id"],
            "winner_process_candidate_id": attempted["process_candidate_id"],
            "process_sha256": attempted["process_sha256"],
            "horizon_process_sha256": attempted["horizon_process_sha256"],
            "trial_definition_sha256": attempted["trial_definition_sha256"],
            "refit_recipe_sha256": attempted["refit_recipe_sha256"],
        }
        if any(row[key] != value for key, value in expected_row.items()):
            raise _BookError("INSUFFICIENT_CROSS_BINDING_ATTEMPT_SPLICE")
        if trial["strategy_id"] != attempted["strategy_id"]:
            raise _BookError("INSUFFICIENT_CROSS_BINDING_STRATEGY_SPLICE")
        expected_evidence_role = {
            "PLANNED": "EVALUATION_REQUIRED",
            "PROCESS_REJECTED": "PRESEARCH_PROCESS_REJECTED",
        }.get(planned["status"])
        if attempted["precommitted_evidence_role"] != expected_evidence_role:
            raise _BookError("INSUFFICIENT_CROSS_BINDING_EVIDENCE_ROLE_SPLICE")
        execution = _exact_dict(
            row["horizon_execution_contract"],
            execution_keys,
            "INSUFFICIENT_CROSS_BINDING_EXECUTION_INVALID",
        )
        source_count = _exact_dict(
            row["source_count_receipt"],
            source_count_keys,
            "INSUFFICIENT_CROSS_BINDING_SOURCE_COUNT_INVALID",
        )
        source_count_body = dict(source_count)
        source_count_sha256 = source_count_body.pop("receipt_sha256")
        if (
            source_count_sha256 != _sha256_json(source_count_body)
            or source_count["schema_version"]
            != "fxpm.historical-attempt-source-count.v1"
            or source_count["anchor_id"] != binding["anchor_id"]
            or {
                "symbol": source_count["symbol"],
                "timeframe": source_count["timeframe"],
                "regime": source_count["regime"],
            }
            != plan_cell
            or source_count["horizon_plan_receipt_sha256"]
            != horizon_roster["plan_receipt_sha256"]
            or source_count["native_prefix_index_sha256"]
            != prefix["prefix_index_sha256"]
            or source_count["native_prefix_content_sha256"]
            != prefix["prefix_content_sha256"]
            or source_count["horizon_attempt_id"] != planned["attempt_id"]
            or source_count["horizon_candidate_id"] != planned["candidate_id"]
            or source_count["horizon_candidate_definition_sha256"]
            != candidate["candidate_definition_sha256"]
            or any(source_count[key] != value for key, value in execution.items())
            or execution["lookback_kind"] != candidate["lookback_kind"]
            or execution["lookback_value"] != candidate["lookback_value"]
            or source_count["fit_start_utc"] != candidate["realized_start_utc"]
            or source_count["fit_end_exclusive_utc"]
            != prefix["prefix_end_exclusive_utc"]
            or source_count["latest_closed_bar_utc"]
            != prefix["prefix_last_close_utc"]
            or type(execution["source_bar_count"]) is not int
            or type(execution["source_bar_count"]) is bool
            or execution["source_bar_count"] < 1
        ):
            raise _BookError("INSUFFICIENT_CROSS_BINDING_SOURCE_COUNT_SPLICE")
        _hex64(source_count["fit_frame_sha256"], "INSUFFICIENT_FIT_FRAME_INVALID")

        folds = row["fold_mapping"]
        if type(folds) is not list or row["fold_mapping_sha256"] != _sha256_json(
            folds
        ):
            raise _BookError("INSUFFICIENT_CROSS_BINDING_FOLD_MAPPING_INVALID")
        causal_expected = [
            value
            for value in expected_by_attempt.get(planned["attempt_id"], [])
            if value["downstream_causal_eligible"] is True
        ]
        winner_folds = precommitment["causal_fold_plan"]
        if planned["status"] == "PLANNED":
            if len(folds) != len(causal_expected) or len(folds) != len(winner_folds):
                raise _BookError(
                    "INSUFFICIENT_CROSS_BINDING_FOLD_CARDINALITY_INVALID"
                )
        elif planned["status"] == "PROCESS_REJECTED":
            if folds or causal_expected:
                raise _BookError("INSUFFICIENT_CROSS_BINDING_TERMINAL_FOLD_INVALID")
        else:
            raise _BookError("INSUFFICIENT_CROSS_BINDING_ATTEMPT_STATUS_INVALID")
        for fold, expected, winner_fold in zip(
            folds, causal_expected, winner_folds, strict=True
        ):
            _exact_dict(
                fold, fold_keys, "INSUFFICIENT_CROSS_BINDING_FOLD_SCHEMA_INVALID"
            )
            candidate_fold = next(
                (
                    value
                    for value in candidate["folds"]
                    if value["fold_id"] == expected["fold_id"]
                ),
                None,
            )
            if (
                candidate_fold is None
                or fold["horizon_fold_id"] != expected["fold_id"]
                or fold["horizon_fold_definition_sha256"]
                != candidate_fold["fold_definition_sha256"]
                or fold["horizon_context_sha256"] != expected["context_sha256"]
                or fold["winner_fold_id"] != winner_fold["fold_id"]
                or fold["validation_start_utc"]
                != expected["validation_start_utc"]
                or fold["validation_end_exclusive_utc"]
                != expected["validation_end_exclusive_utc"]
                or fold["validation_start_utc"]
                != winner_fold["anchor_as_of_utc"]
            ):
                raise _BookError("INSUFFICIENT_CROSS_BINDING_FOLD_SPLICE")
    if (
        type(winner_raw_production_receipt_bytes) is not bytes
        or not winner_raw_production_receipt_bytes
        or len(winner_raw_production_receipt_bytes) > winner.MAX_REQUEST_BYTES
    ):
        raise _BookError("INSUFFICIENT_RAW_PRODUCTION_RECEIPT_BYTES_INVALID")
    try:
        production = json.loads(winner_raw_production_receipt_bytes.decode("ascii"))
    except Exception as exc:
        raise _BookError(
            "INSUFFICIENT_RAW_PRODUCTION_RECEIPT_BYTES_INVALID"
        ) from exc
    production_keys = frozenset(
        {
            "schema_version",
            "generation_context_sha256",
            "run_id",
            "artifact_namespace",
            "symbol",
            "timeframe",
            "regime",
            "precommitment_sha256",
            "attempt_ledger_sha256",
            "provenance_cross_binding_sha256",
            "evidence_ledger_sha256",
            "request_sha256",
            "decision_receipt_sha256",
            "artifact_roster",
        }
    )
    _exact_dict(
        production,
        production_keys,
        "INSUFFICIENT_RAW_PRODUCTION_RECEIPT_SCHEMA_INVALID",
    )
    if _canonical_bytes(production) != winner_raw_production_receipt_bytes:
        raise _BookError("INSUFFICIENT_RAW_PRODUCTION_RECEIPT_NONCANONICAL")
    cross_binding_sha256 = _sha256_bytes(encoded)
    evidence_ledger = process_rejection_request.evidence_ledger.to_dict()
    if (
        production["schema_version"]
        != WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION
        or production["run_id"] != precommitment["run_id"]
        or production["artifact_namespace"] != binding["artifact_namespace"]
        or {
            "symbol": production["symbol"],
            "timeframe": production["timeframe"],
            "regime": production["regime"],
        }
        != plan_cell
        or production["precommitment_sha256"] != _sha256_json(precommitment)
        or production["attempt_ledger_sha256"] != _sha256_json(attempt_ledger)
        or production["provenance_cross_binding_sha256"]
        != cross_binding_sha256
        or production["evidence_ledger_sha256"] != _sha256_json(evidence_ledger)
        or production["request_sha256"]
        != process_rejection_request.request_sha256
        or production["decision_receipt_sha256"]
        != process_rejection_decision.receipt_sha256
    ):
        raise _BookError("INSUFFICIENT_RAW_PRODUCTION_RECEIPT_SPLICE")
    _hex64(
        production["generation_context_sha256"],
        "INSUFFICIENT_RAW_PRODUCTION_CONTEXT_INVALID",
    )
    roster = production["artifact_roster"]
    if type(roster) is not dict or not 3 <= len(roster) <= 1024:
        raise _BookError("INSUFFICIENT_RAW_PRODUCTION_ROSTER_INVALID")
    descriptor_keys = frozenset(
        {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
    )
    expected_spooled = {
        "evidence-ledger": evidence_ledger,
        "winner-request": process_rejection_request.to_dict(),
        "winner-decision": process_rejection_decision.to_dict(),
    }
    for name, payload in expected_spooled.items():
        descriptor = _exact_dict(
            roster.get(name),
            descriptor_keys,
            "INSUFFICIENT_RAW_PRODUCTION_DESCRIPTOR_INVALID",
        )
        payload_bytes = _canonical_bytes(payload)
        if (
            descriptor["schema_version"]
            != "fxpm.authority-spool-artifact-descriptor.v1"
            or type(descriptor["relative_path"]) is not str
            or not descriptor["relative_path"]
            or descriptor["sha256"] != _sha256_bytes(payload_bytes)
            or descriptor["size_bytes"] != len(payload_bytes)
            or descriptor["format"] != "fxpm-ascii-json-v1"
        ):
            raise _BookError("INSUFFICIENT_RAW_PRODUCTION_DESCRIPTOR_SPLICE")
    return (
        HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION,
        cross_binding_sha256,
        binding["mapping_sha256"],
        WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION,
        _sha256_bytes(winner_raw_production_receipt_bytes),
    )


def _build_insufficient_binding(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    source_body: dict[str, Any],
    fit_body: dict[str, Any],
    reason_code: str,
    attempted_trials_exact: int,
    horizon_plan: Any,
    process_rejection_request: Any,
    process_rejection_decision: Any,
    horizon_winner_cross_binding_bytes: Any = None,
    winner_raw_production_receipt_bytes: Any = None,
) -> dict[str, Any]:
    plan_body = _horizon_plan_body(horizon_plan)
    cell_id = compute_cell_id(symbol, timeframe, regime)
    if (
        plan_body.get("cell")
        != {"symbol": symbol, "timeframe": timeframe, "regime": regime}
        or plan_body["native_prefix"]["source_state"] != AVAILABLE
        or plan_body["native_prefix"]["internal_status"] == "UNEXPLAINED"
        or source_body["availability"] != AVAILABLE
        or fit_body["fit_state"] != REGIME_FITTED
        or fit_body["train_regime_support"]["counts"].get(regime, 0) < 1
        or cell_id is None
    ):
        raise _BookError("INSUFFICIENT_SOURCE_OR_REGIME_INVALID")
    prefix = plan_body["native_prefix"]
    if (
        prefix["activation_id"] != source_body["activation_id"]
        or prefix["activation_manifest_sha256"]
        != source_body["activation_manifest_sha256"]
        or prefix["artifact_sha256"] != source_body["native_source_sha256"]
    ):
        raise _BookError("INSUFFICIENT_SOURCE_SPLICE")
    detector_cutoff = datetime.fromisoformat(
        fit_body["regime_detector_record"]["fit_cutoff_utc"].replace("Z", "+00:00")
    )
    selection_cutoff = datetime.fromisoformat(
        plan_body["anchor"]["selection_cutoff_utc"].replace("Z", "+00:00")
    )
    if detector_cutoff > selection_cutoff:
        raise _BookError("INSUFFICIENT_REGIME_CUTOFF_LEAK")
    fold_geometry, causal_fold_count = _fold_geometry(plan_body)
    attempts = plan_body["attempts"]
    planned_count = len(attempts)
    process_request_sha: str | None = None
    process_attempt_sha: str | None = None
    process_evidence_sha: str | None = None
    process_decision_sha: str | None = None
    cross_binding_schema: str | None = None
    cross_binding_content_sha: str | None = None
    cross_binding_mapping_sha: str | None = None
    raw_production_schema: str | None = None
    raw_production_receipt_sha: str | None = None
    if reason_code == INSUFFICIENT_PRESEARCH_BOUNDARY_REASON:
        if (
            attempted_trials_exact != 0
            or plan_body["plan_status"] != "HISTORICAL_BOUNDARY_LIMITED"
            or any(
                attempt["status"] != "PROCESS_REJECTED"
                or attempt["reason_code"] != INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
                for attempt in attempts
            )
            or any(
                not item["boundary_shortfall"]
                for item in plan_body["candidate_registry"]
            )
            or process_rejection_request is not None
            or process_rejection_decision is not None
            or horizon_winner_cross_binding_bytes is not None
            or winner_raw_production_receipt_bytes is not None
        ):
            raise _BookError("INSUFFICIENT_PRESEARCH_EVIDENCE_INVALID")
        stage = INSUFFICIENT_PRESEARCH_STAGE
        rejected_count = 0
    elif reason_code == INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON:
        if (
            plan_body["plan_status"] != "READY_FOR_HISTORICAL_EVIDENCE"
            or type(process_rejection_request) is not winner.WinnerSelectionRequest
            or type(process_rejection_decision) is not winner.WinnerSelectionDecision
            or not winner.verify_historical_cell_winner_decision(
                process_rejection_decision, process_rejection_request
            )
        ):
            raise _BookError("INSUFFICIENT_PROCESS_REJECTION_EVIDENCE_INVALID")
        decision_body = process_rejection_decision.to_dict()
        precommitment = process_rejection_request.precommitment.to_dict()
        attempt_ledger = process_rejection_request.attempt_ledger.to_dict()
        evidence_ledger = process_rejection_request.evidence_ledger.to_dict()
        attempt_rows = attempt_ledger["attempts"]
        entries = evidence_ledger["entries"]
        pbo_fold_count = len(precommitment["causal_fold_plan"])

        def _matches_rejected_role(
            attempt: dict[str, Any], entry: dict[str, Any]
        ) -> bool:
            if (
                entry["evidence_status"] != "PROCESS_REJECTED"
                or entry["fold_count"] != 0
                or entry["folds"] != []
            ):
                return False
            role = attempt["precommitted_evidence_role"]
            if role == "EVALUATION_REQUIRED":
                return (
                    entry["status_reason"] == "EVALUATED_OOS_PROCESS_REJECTED"
                    and entry["pbo_evidence_source"]
                    == "COMPLETE_EVALUATION_REQUIRED_FOLDS"
                    and entry["pbo_fold_count"] == pbo_fold_count
                    and len(entry["pbo_folds"]) == pbo_fold_count
                )
            if role == "PRESEARCH_PROCESS_REJECTED":
                return (
                    entry["status_reason"] == "PRESEARCH_PROCESS_REJECTED"
                    and entry["pbo_evidence_source"]
                    == "NOT_APPLICABLE_PRESEARCH_REJECTION"
                    and entry["pbo_fold_count"] == 0
                    and entry["pbo_folds"] == []
                )
            return False

        if (
            decision_body["status"] != "NO_PROCESS_WINNER"
            or decision_body["winner"] is not None
            or decision_body["cell"]
            != {
                "cell_id": cell_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
            }
            or precommitment["source_contract_sha256"]
            != source_body["source_contract_sha256"]
            or len(attempt_rows) != planned_count
            or attempted_trials_exact != planned_count
            or len(entries) != planned_count
            or any(
                not _matches_rejected_role(attempt, entry)
                for attempt, entry in zip(attempt_rows, entries, strict=True)
            )
        ):
            raise _BookError("INSUFFICIENT_PROCESS_REJECTION_EVIDENCE_INVALID")
        (
            cross_binding_schema,
            cross_binding_content_sha,
            cross_binding_mapping_sha,
            raw_production_schema,
            raw_production_receipt_sha,
        ) = _horizon_winner_cross_binding_identity(
            horizon_winner_cross_binding_bytes,
            plan_body=plan_body,
            source_body=source_body,
            fit_body=fit_body,
            process_rejection_request=process_rejection_request,
            process_rejection_decision=process_rejection_decision,
            winner_raw_production_receipt_bytes=(
                winner_raw_production_receipt_bytes
            ),
        )
        planned_axes: tuple[tuple[str, str], ...] | None = None
        for candidate, registry in zip(
            plan_body["candidates"], plan_body["candidate_registry"], strict=True
        ):
            if registry["boundary_shortfall"]:
                continue
            axes = tuple(
                (
                    fold["validation_start_utc"],
                    fold["validation_end_exclusive_utc"],
                )
                for fold in candidate["folds"]
                if fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"
            )
            planned_axes = axes if planned_axes is None else planned_axes
            if axes != planned_axes:
                raise _BookError("INSUFFICIENT_CAUSAL_FOLD_GEOMETRY_INVALID")
        if (
            planned_axes is None
            or _winner_fold_axes(precommitment["causal_fold_plan"]) != planned_axes
        ):
            raise _BookError("INSUFFICIENT_WINNER_HORIZON_FOLD_SPLICE")
        stage = INSUFFICIENT_POSTSEARCH_STAGE
        rejected_count = len(entries)
        process_request_sha = process_rejection_request.request_sha256
        process_attempt_sha = process_rejection_request.attempt_ledger.payload_sha256
        process_evidence_sha = process_rejection_request.evidence_ledger.payload_sha256
        process_decision_sha = process_rejection_decision.receipt_sha256
    else:
        raise _BookError("INSUFFICIENT_REASON_INVALID")
    body = {
        "schema_version": INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION,
        "terminal_stage": stage,
        "reason_code": reason_code,
        "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
        "source_prefix_receipt_sha256": plan_body["source_prefix_receipt_sha256"],
        "source_prefix_content_sha256": prefix["prefix_content_sha256"],
        "source_prefix_rows": prefix["prefix_rows"],
        "horizon_plan_receipt_sha256": horizon_plan.receipt_sha256,
        "horizon_policy_release_sha256": plan_body["horizon_policy"][
            "policy_release_sha256"
        ],
        "dependency_policy_receipt_sha256": plan_body["dependency_policy"][
            "signed_policy_receipt_sha256"
        ],
        "dependency_geometry_sha256": _sha256_json(
            plan_body["derived_dependency_geometry"]
        ),
        "candidate_registry_sha256": plan_body["candidate_registry_sha256"],
        "trial_registry_sha256": plan_body["trial_registry_sha256"],
        "horizon_attempt_roster_sha256": plan_body["attempt_roster_sha256"],
        "evidence_roster_sha256": plan_body["evidence_roster_sha256"],
        "causal_fold_geometry_sha256": _sha256_json(fold_geometry),
        "causal_fold_count": causal_fold_count,
        "planned_attempt_count": planned_count,
        "process_rejected_attempt_count": rejected_count,
        "process_rejection_request_sha256": process_request_sha,
        "process_rejection_attempt_ledger_sha256": process_attempt_sha,
        "process_rejection_evidence_ledger_sha256": process_evidence_sha,
        "process_rejection_decision_sha256": process_decision_sha,
        "horizon_winner_cross_binding_schema_version": cross_binding_schema,
        "horizon_winner_cross_binding_content_sha256": cross_binding_content_sha,
        "horizon_winner_cross_binding_mapping_sha256": cross_binding_mapping_sha,
        "winner_raw_production_schema_version": raw_production_schema,
        "winner_raw_production_receipt_sha256": raw_production_receipt_sha,
    }
    return _insufficient_binding_body(body)


def _owned_decision(decision_type: type[Any], body: dict[str, Any]) -> Any:
    encoded = _canonical_bytes(body)
    return decision_type(encoded, _sha256_bytes(encoded))


def _source_body(decision: Any) -> dict[str, Any]:
    if type(decision) is not HistoricalSourceGroupDecision:
        raise _BookError("SOURCE_GROUP_DECISION_TYPE_INVALID")
    if (
        type(decision._body_json) is not bytes
        or type(decision._receipt_sha256) is not str
        or decision._receipt_sha256 != _sha256_bytes(decision._body_json)
    ):
        raise _BookError("SOURCE_GROUP_RECEIPT_INVALID")
    try:
        body = json.loads(decision._body_json.decode("ascii"))
    except Exception as exc:
        raise _BookError("SOURCE_GROUP_PAYLOAD_INVALID") from exc
    _exact_dict(body, _SOURCE_KEYS, "SOURCE_GROUP_SCHEMA_INVALID")
    if _canonical_bytes(body) != decision._body_json:
        raise _BookError("SOURCE_GROUP_PAYLOAD_NONCANONICAL")
    if body["contract_version"] != SOURCE_GROUP_CONTRACT_VERSION:
        raise _BookError("SOURCE_GROUP_CONTRACT_INVALID")
    symbol = body["symbol"]
    timeframe = body["timeframe"]
    expected_group_id = compute_source_group_id(symbol, timeframe)
    if expected_group_id is None or body["source_group_id"] != expected_group_id:
        raise _BookError("SOURCE_GROUP_IDENTITY_INVALID")
    _reason(body["reason_code"], "SOURCE_GROUP_REASON_INVALID")
    _hex64(body["source_contract_sha256"], "SOURCE_CONTRACT_INVALID")
    _hex64(body["source_group_evidence_sha256"], "SOURCE_GROUP_EVIDENCE_INVALID")
    activation_id = body["activation_id"]
    if activation_id is not None:
        _bounded_id(activation_id, "ACTIVATION_ID_INVALID")
    activation_manifest = _optional_hex64(
        body["activation_manifest_sha256"], "ACTIVATION_MANIFEST_INVALID"
    )
    native_source = _optional_hex64(
        body["native_source_sha256"], "NATIVE_SOURCE_INVALID"
    )
    _exact_int(
        body["acquisition_attempts_exact"],
        1,
        _MAX_ATTEMPT_COUNT,
        "SOURCE_ACQUISITION_ATTEMPTS_INVALID",
    )
    terminal = _exact_bool(body["terminal"], "SOURCE_TERMINAL_TYPE_INVALID")
    complete = _exact_bool(body["complete"], "SOURCE_COMPLETE_TYPE_INVALID")
    proof = _optional_hex64(body["terminal_proof_sha256"], "TERMINAL_PROOF_INVALID")
    if body["availability"] == AVAILABLE:
        if (
            terminal
            or not complete
            or proof is not None
            or activation_id is None
            or activation_manifest is None
            or native_source is None
        ):
            raise _BookError("AVAILABLE_SOURCE_SEMANTICS_INVALID")
    elif body["availability"] == TERMINAL_UNAVAILABLE:
        if (
            not terminal
            or not complete
            or proof is None
            or any(
                value is not None
                for value in (activation_id, activation_manifest, native_source)
            )
        ):
            raise _BookError("TERMINAL_SOURCE_SEMANTICS_INVALID")
    else:
        raise _BookError("SOURCE_AVAILABILITY_INVALID")
    return body


def create_historical_source_group_decision(
    *,
    symbol: Any,
    timeframe: Any,
    availability: Any,
    reason_code: Any,
    source_contract_sha256: Any,
    source_group_evidence_sha256: Any,
    acquisition_attempts_exact: Any,
    activation_id: Any = None,
    activation_manifest_sha256: Any = None,
    native_source_sha256: Any = None,
    terminal_proof_sha256: Any = None,
) -> HistoricalSourceGroupDecision | None:
    """Create an exact source-group fact; return ``None`` for hostile input."""

    try:
        group_id = compute_source_group_id(symbol, timeframe)
        if group_id is None or type(availability) is not str:
            raise _BookError("SOURCE_GROUP_INPUT_INVALID")
        body = {
            "contract_version": SOURCE_GROUP_CONTRACT_VERSION,
            "source_group_id": group_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "availability": availability,
            "reason_code": _reason(reason_code, "SOURCE_GROUP_REASON_INVALID"),
            "source_contract_sha256": _hex64(
                source_contract_sha256, "SOURCE_CONTRACT_INVALID"
            ),
            "source_group_evidence_sha256": _hex64(
                source_group_evidence_sha256, "SOURCE_GROUP_EVIDENCE_INVALID"
            ),
            "activation_id": (
                None
                if activation_id is None
                else _bounded_id(activation_id, "ACTIVATION_ID_INVALID")
            ),
            "activation_manifest_sha256": _optional_hex64(
                activation_manifest_sha256, "ACTIVATION_MANIFEST_INVALID"
            ),
            "native_source_sha256": _optional_hex64(
                native_source_sha256, "NATIVE_SOURCE_INVALID"
            ),
            "acquisition_attempts_exact": _exact_int(
                acquisition_attempts_exact,
                1,
                _MAX_ATTEMPT_COUNT,
                "SOURCE_ACQUISITION_ATTEMPTS_INVALID",
            ),
            "terminal_proof_sha256": _optional_hex64(
                terminal_proof_sha256, "TERMINAL_PROOF_INVALID"
            ),
            "terminal": availability == TERMINAL_UNAVAILABLE,
            "complete": True,
        }
        decision = _owned_decision(HistoricalSourceGroupDecision, body)
        _source_body(decision)
        return decision
    except Exception:
        return None


def _regime_detector_record_body(
    value: Any,
    source_decision: Any,
) -> dict[str, Any]:
    record = _exact_dict(
        value,
        _REGIME_DETECTOR_RECORD_KEYS,
        "REGIME_DETECTOR_RECORD_SCHEMA_INVALID",
    )
    source = _source_body(source_decision)
    if source["availability"] != AVAILABLE:
        raise _BookError("REGIME_DETECTOR_SOURCE_UNAVAILABLE")
    if record["schema_version"] != REGIME_DETECTOR_RECORD_SCHEMA_VERSION:
        raise _BookError("REGIME_DETECTOR_RECORD_VERSION_INVALID")
    expected_source = {
        "symbol": source["symbol"],
        "timeframe": source["timeframe"],
        "source_group_id": source["source_group_id"],
        "source_group_decision_sha256": source_decision.receipt_sha256,
        "source_contract_sha256": source["source_contract_sha256"],
        "source_group_evidence_sha256": source["source_group_evidence_sha256"],
        "activation_id": source["activation_id"],
        "activation_manifest_sha256": source["activation_manifest_sha256"],
        "native_source_sha256": source["native_source_sha256"],
    }
    if any(record[key] != expected for key, expected in expected_source.items()):
        raise _BookError("REGIME_DETECTOR_SOURCE_SPLICE")
    _canonical_utc(record["fit_cutoff_utc"], "REGIME_DETECTOR_CUTOFF_INVALID")
    _bounded_id(record["detector_version"], "REGIME_DETECTOR_VERSION_INVALID")
    _hex64(record["detector_code_sha256"], "REGIME_DETECTOR_CODE_INVALID")
    parameters_text = record["parameters_canonical_json"]
    if (
        type(parameters_text) is not str
        or not 1 <= len(parameters_text.encode("ascii", errors="ignore")) <= 8192
        or not parameters_text.isascii()
    ):
        raise _BookError("REGIME_DETECTOR_PARAMETERS_PAYLOAD_INVALID")
    try:
        parameters = _regime_detector_parameters(json.loads(parameters_text))
    except Exception as exc:
        raise _BookError("REGIME_DETECTOR_PARAMETERS_PAYLOAD_INVALID") from exc
    if _canonical_bytes(parameters).decode("ascii") != parameters_text:
        raise _BookError("REGIME_DETECTOR_PARAMETERS_PAYLOAD_NONCANONICAL")
    parameters_sha = _hex64(
        record["parameters_sha256"], "REGIME_DETECTOR_PARAMETERS_HASH_INVALID"
    )
    if parameters_sha != _sha256_bytes(parameters_text.encode("ascii")):
        raise _BookError("REGIME_DETECTOR_PARAMETERS_HASH_INVALID")
    record_sha = _hex64(record["record_sha256"], "REGIME_DETECTOR_RECORD_HASH_INVALID")
    body = dict(record)
    body.pop("record_sha256")
    if record_sha != _sha256_json(body):
        raise _BookError("REGIME_DETECTOR_RECORD_HASH_INVALID")
    return record


def create_regime_detector_record(
    *,
    source_group_decision: Any,
    fit_cutoff_utc: Any,
    detector_version: Any,
    detector_code_sha256: Any,
    parameters: Any,
) -> dict[str, Any] | None:
    """Create one immutable detector record for a symbol/timeframe."""

    try:
        source = _source_body(source_group_decision)
        if source["availability"] != AVAILABLE:
            raise _BookError("REGIME_DETECTOR_SOURCE_UNAVAILABLE")
        exact_parameters = _regime_detector_parameters(parameters)
        parameters_text = _canonical_bytes(exact_parameters).decode("ascii")
        body = {
            "schema_version": REGIME_DETECTOR_RECORD_SCHEMA_VERSION,
            "symbol": source["symbol"],
            "timeframe": source["timeframe"],
            "source_group_id": source["source_group_id"],
            "source_group_decision_sha256": source_group_decision.receipt_sha256,
            "source_contract_sha256": source["source_contract_sha256"],
            "source_group_evidence_sha256": source["source_group_evidence_sha256"],
            "activation_id": source["activation_id"],
            "activation_manifest_sha256": source["activation_manifest_sha256"],
            "native_source_sha256": source["native_source_sha256"],
            "fit_cutoff_utc": _canonical_utc(
                fit_cutoff_utc, "REGIME_DETECTOR_CUTOFF_INVALID"
            ),
            "detector_version": _bounded_id(
                detector_version, "REGIME_DETECTOR_VERSION_INVALID"
            ),
            "detector_code_sha256": _hex64(
                detector_code_sha256, "REGIME_DETECTOR_CODE_INVALID"
            ),
            "parameters_canonical_json": parameters_text,
            "parameters_sha256": _sha256_bytes(parameters_text.encode("ascii")),
        }
        record = {**body, "record_sha256": _sha256_json(body)}
        return _regime_detector_record_body(record, source_group_decision)
    except Exception:
        return None


_TRAIN_SUPPORT_KEYS: Final = frozenset(
    {
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
    }
)
_TRAIN_SUPPORT_BINDING_KEYS: Final = frozenset(
    {
        "symbol",
        "timeframe",
        "activation_id",
        "activation_manifest_sha256",
        "source_sha256",
        "train_window_sha256",
        "params_sha256",
        "detector_code_sha256",
    }
)


def _train_regime_support_body(
    value: Any,
    source_body: dict[str, Any],
    params_sha256: str,
    detector_sha256: str,
) -> dict[str, Any]:
    support = _exact_dict(value, _TRAIN_SUPPORT_KEYS, "TRAIN_SUPPORT_SCHEMA_INVALID")
    if (
        support["schema_version"] != REGIME_TRAIN_SUPPORT_SCHEMA_VERSION
        or support["symbol"] != source_body["symbol"]
        or support["timeframe"] != source_body["timeframe"]
        or support["surface"] != "REGIME_LIVE"
        or support["warmup_policy"] != REGIME_TRAIN_SUPPORT_WARMUP_POLICY
        or support["label_order"] != list(REGIME_ORDER)
        or support["vector_encoding"] != REGIME_TRAIN_SUPPORT_VECTOR_ENCODING
    ):
        raise _BookError("TRAIN_SUPPORT_SEMANTICS_INVALID")
    counts = _regime_counts(support["counts"], "TRAIN_SUPPORT_COUNTS_INVALID")
    eligible = _exact_int(
        support["eligible_rows"],
        1,
        _MAX_TRAIN_LABEL_COUNT,
        "TRAIN_SUPPORT_ELIGIBLE_ROWS_INVALID",
    )
    if sum(counts.values()) != eligible:
        raise _BookError("TRAIN_SUPPORT_ROW_ACCOUNTING_INVALID")
    observed = [regime for regime in REGIME_ORDER if counts[regime] > 0]
    unobserved = [regime for regime in REGIME_ORDER if counts[regime] == 0]
    states = {
        regime: "EMPIRICALLY_OBSERVED" if counts[regime] > 0 else "UNOBSERVED"
        for regime in REGIME_ORDER
    }
    if (
        support["observed_regimes"] != observed
        or support["unobserved_regimes"] != unobserved
        or support["support_state_by_regime"] != states
    ):
        raise _BookError("TRAIN_SUPPORT_PARTITION_INVALID")
    first_text = _bounded_id(support["first_ts"], "TRAIN_SUPPORT_FIRST_TS_INVALID")
    last_text = _bounded_id(support["last_ts"], "TRAIN_SUPPORT_LAST_TS_INVALID")
    try:
        first_ts = datetime.fromisoformat(first_text.replace("Z", "+00:00"))
        last_ts = datetime.fromisoformat(last_text.replace("Z", "+00:00"))
        if first_ts > last_ts:
            raise ValueError
    except Exception as exc:
        raise _BookError("TRAIN_SUPPORT_TIME_RANGE_INVALID") from exc
    _hex64(support["vector_sha256"], "TRAIN_SUPPORT_VECTOR_INVALID")
    bindings = _exact_dict(
        support["bindings"],
        _TRAIN_SUPPORT_BINDING_KEYS,
        "TRAIN_SUPPORT_BINDINGS_INVALID",
    )
    expected_bindings = {
        "symbol": source_body["symbol"],
        "timeframe": source_body["timeframe"],
        "activation_id": source_body["activation_id"],
        "activation_manifest_sha256": source_body["activation_manifest_sha256"],
        "source_sha256": source_body["native_source_sha256"],
        "train_window_sha256": _hex64(
            bindings["train_window_sha256"], "TRAIN_WINDOW_INVALID"
        ),
        "params_sha256": params_sha256,
        "detector_code_sha256": detector_sha256,
    }
    if bindings != expected_bindings:
        raise _BookError("TRAIN_SUPPORT_BINDING_SPLICE")
    receipt = _hex64(support["receipt_sha256"], "TRAIN_SUPPORT_RECEIPT_INVALID")
    body = dict(support)
    body.pop("receipt_sha256")
    if receipt != _sha256_json(body):
        raise _BookError("TRAIN_SUPPORT_RECEIPT_INVALID")
    return support


def _regime_fit_body(
    decision: Any,
    source_decision: Any | None = None,
) -> dict[str, Any]:
    if type(decision) is not RegimeFitDecision:
        raise _BookError("REGIME_FIT_DECISION_TYPE_INVALID")
    if (
        type(decision._body_json) is not bytes
        or type(decision._receipt_sha256) is not str
        or decision._receipt_sha256 != _sha256_bytes(decision._body_json)
    ):
        raise _BookError("REGIME_FIT_RECEIPT_INVALID")
    try:
        body = json.loads(decision._body_json.decode("ascii"))
    except Exception as exc:
        raise _BookError("REGIME_FIT_PAYLOAD_INVALID") from exc
    _exact_dict(body, _REGIME_FIT_KEYS, "REGIME_FIT_SCHEMA_INVALID")
    if _canonical_bytes(body) != decision._body_json:
        raise _BookError("REGIME_FIT_PAYLOAD_NONCANONICAL")
    if body["contract_version"] != REGIME_FIT_CONTRACT_VERSION:
        raise _BookError("REGIME_FIT_CONTRACT_INVALID")
    expected_group_id = compute_source_group_id(body["symbol"], body["timeframe"])
    if expected_group_id is None or body["source_group_id"] != expected_group_id:
        raise _BookError("REGIME_FIT_GROUP_IDENTITY_INVALID")
    _hex64(
        body["source_group_decision_sha256"],
        "REGIME_FIT_SOURCE_RECEIPT_INVALID",
    )
    _reason(body["reason_code"], "REGIME_FIT_REASON_INVALID")
    _hex64(body["regime_fit_policy_sha256"], "REGIME_FIT_POLICY_INVALID")
    detector = _optional_hex64(
        body["detector_process_sha256"], "DETECTOR_PROCESS_INVALID"
    )
    evidence = _optional_hex64(
        body["regime_fit_evidence_sha256"], "REGIME_FIT_EVIDENCE_INVALID"
    )
    mapping = _optional_hex64(
        body["four_regime_mapping_sha256"], "REGIME_MAPPING_INVALID"
    )
    params_sha = _optional_hex64(body["regime_params_sha256"], "REGIME_PARAMS_INVALID")
    detector_record_sha = _optional_hex64(
        body["regime_detector_record_sha256"],
        "REGIME_DETECTOR_RECORD_HASH_INVALID",
    )
    complete = _exact_bool(body["complete"], "REGIME_FIT_COMPLETE_TYPE_INVALID")
    if source_decision is None:
        raise _BookError("REGIME_FIT_SOURCE_REQUIRED")
    source_body = _source_body(source_decision)
    if (
        body["source_group_id"] != source_body["source_group_id"]
        or body["source_group_decision_sha256"] != source_decision.receipt_sha256
    ):
        raise _BookError("REGIME_FIT_SOURCE_SPLICE")
    if body["fit_state"] == REGIME_FITTED:
        if (
            not complete
            or None in {detector, evidence, mapping, params_sha, detector_record_sha}
            or type(body["regime_detector_record"]) is not dict
            or type(body["train_regime_support"]) is not dict
        ):
            raise _BookError("FITTED_REGIME_SEMANTICS_INVALID")
        if source_body["availability"] != AVAILABLE:
            raise _BookError("FITTED_REGIME_SOURCE_INVALID")
        record = _regime_detector_record_body(
            body["regime_detector_record"], source_decision
        )
        if (
            detector_record_sha != record["record_sha256"]
            or detector != record["detector_code_sha256"]
            or params_sha != record["parameters_sha256"]
        ):
            raise _BookError("REGIME_DETECTOR_FIT_SPLICE")
        support = _train_regime_support_body(
            body["train_regime_support"], source_body, params_sha, detector
        )
        if datetime.fromisoformat(support["last_ts"].replace("Z", "+00:00")) > (
            datetime.fromisoformat(record["fit_cutoff_utc"].replace("Z", "+00:00"))
        ):
            raise _BookError("REGIME_DETECTOR_CUTOFF_PRECEDES_TRAIN_SUPPORT")
    elif body["fit_state"] == REGIME_FIT_TECHNICAL_FAILURE:
        if (
            complete
            or detector is None
            or evidence is None
            or any(
                value is not None
                for value in (
                    mapping,
                    params_sha,
                    detector_record_sha,
                    body["regime_detector_record"],
                )
            )
            or body["train_regime_support"] is not None
        ):
            raise _BookError("FAILED_REGIME_FIT_SEMANTICS_INVALID")
        if source_body["availability"] != AVAILABLE:
            raise _BookError("FAILED_REGIME_FIT_SOURCE_INVALID")
    elif body["fit_state"] == REGIME_NOT_APPLICABLE:
        if not complete or any(
            value is not None
            for value in (
                detector,
                evidence,
                mapping,
                params_sha,
                detector_record_sha,
                body["regime_detector_record"],
                body["train_regime_support"],
            )
        ):
            raise _BookError("INAPPLICABLE_REGIME_FIT_SEMANTICS_INVALID")
        if source_body["availability"] != TERMINAL_UNAVAILABLE:
            raise _BookError("INAPPLICABLE_REGIME_SOURCE_INVALID")
    else:
        raise _BookError("REGIME_FIT_STATE_INVALID")
    return body


def create_regime_fit_decision(
    *,
    source_group_decision: Any,
    fit_state: Any,
    reason_code: Any,
    regime_fit_policy_sha256: Any,
    detector_process_sha256: Any = None,
    regime_fit_evidence_sha256: Any = None,
    four_regime_mapping_sha256: Any = None,
    regime_params_sha256: Any = None,
    regime_detector_record: Any = None,
    train_regime_support: Any = None,
) -> RegimeFitDecision | None:
    """Create the one shared regime-fit fact for four sibling regimes."""

    try:
        source_body = _source_body(source_group_decision)
        if type(fit_state) is not str:
            raise _BookError("REGIME_FIT_STATE_INVALID")
        body = {
            "contract_version": REGIME_FIT_CONTRACT_VERSION,
            "source_group_id": source_body["source_group_id"],
            "source_group_decision_sha256": source_group_decision.receipt_sha256,
            "symbol": source_body["symbol"],
            "timeframe": source_body["timeframe"],
            "fit_state": fit_state,
            "reason_code": _reason(reason_code, "REGIME_FIT_REASON_INVALID"),
            "regime_fit_policy_sha256": _hex64(
                regime_fit_policy_sha256, "REGIME_FIT_POLICY_INVALID"
            ),
            "detector_process_sha256": _optional_hex64(
                detector_process_sha256, "DETECTOR_PROCESS_INVALID"
            ),
            "regime_fit_evidence_sha256": _optional_hex64(
                regime_fit_evidence_sha256, "REGIME_FIT_EVIDENCE_INVALID"
            ),
            "four_regime_mapping_sha256": _optional_hex64(
                four_regime_mapping_sha256, "REGIME_MAPPING_INVALID"
            ),
            "regime_params_sha256": _optional_hex64(
                regime_params_sha256, "REGIME_PARAMS_INVALID"
            ),
            "regime_detector_record": (
                None
                if regime_detector_record is None
                else json.loads(
                    _canonical_bytes(regime_detector_record).decode("ascii")
                )
            ),
            "regime_detector_record_sha256": (
                None
                if regime_detector_record is None
                else _hex64(
                    regime_detector_record.get("record_sha256"),
                    "REGIME_DETECTOR_RECORD_HASH_INVALID",
                )
            ),
            "train_regime_support": (
                None
                if train_regime_support is None
                else json.loads(_canonical_bytes(train_regime_support).decode("ascii"))
            ),
            "complete": fit_state != REGIME_FIT_TECHNICAL_FAILURE,
        }
        decision = _owned_decision(RegimeFitDecision, body)
        _regime_fit_body(decision, source_group_decision)
        return decision
    except Exception:
        return None


def _process_state_body(
    decision: Any,
    source_decision: Any | None = None,
    regime_fit_decision: Any | None = None,
) -> dict[str, Any]:
    if type(decision) is not HistoricalProcessStateDecision:
        raise _BookError("PROCESS_STATE_DECISION_TYPE_INVALID")
    if (
        type(decision._body_json) is not bytes
        or type(decision._receipt_sha256) is not str
        or decision._receipt_sha256 != _sha256_bytes(decision._body_json)
    ):
        raise _BookError("PROCESS_STATE_RECEIPT_INVALID")
    try:
        body = json.loads(decision._body_json.decode("ascii"))
    except Exception as exc:
        raise _BookError("PROCESS_STATE_PAYLOAD_INVALID") from exc
    _exact_dict(body, _PROCESS_STATE_KEYS, "PROCESS_STATE_SCHEMA_INVALID")
    if _canonical_bytes(body) != decision._body_json:
        raise _BookError("PROCESS_STATE_PAYLOAD_NONCANONICAL")
    if body["contract_version"] != PROCESS_STATE_CONTRACT_VERSION:
        raise _BookError("PROCESS_STATE_CONTRACT_INVALID")
    cell = _exact_dict(body["cell"], _CELL_IDENTITY_KEYS, "PROCESS_CELL_INVALID")
    cell_id = compute_cell_id(cell["symbol"], cell["timeframe"], cell["regime"])
    if cell_id is None or cell["cell_id"] != cell_id:
        raise _BookError("PROCESS_CELL_IDENTITY_INVALID")
    group_id = compute_source_group_id(cell["symbol"], cell["timeframe"])
    if group_id is None or body["source_group_id"] != group_id:
        raise _BookError("PROCESS_SOURCE_GROUP_IDENTITY_INVALID")
    _hex64(body["source_group_decision_sha256"], "PROCESS_SOURCE_RECEIPT_INVALID")
    _hex64(body["source_contract_sha256"], "PROCESS_SOURCE_CONTRACT_INVALID")
    _hex64(body["regime_receipt_sha256"], "PROCESS_REGIME_FIT_INVALID")
    support_receipt = _optional_hex64(
        body["train_regime_support_receipt_sha256"],
        "PROCESS_TRAIN_SUPPORT_RECEIPT_INVALID",
    )
    _reason(body["reason_code"], "PROCESS_REASON_INVALID")
    insufficient_binding = (
        _insufficient_binding_body(body["insufficient_evidence_binding"])
        if body["state_kind"] == INSUFFICIENT_EVIDENCE
        else None
    )
    if (
        body["state_kind"] != INSUFFICIENT_EVIDENCE
        and body["insufficient_evidence_binding"] is not None
    ):
        raise _BookError("NONINSUFFICIENT_STATE_CARRIES_EVIDENCE_BINDING")
    attempts = _exact_int(
        body["attempted_trials_exact"],
        0,
        _MAX_ATTEMPT_COUNT,
        "PROCESS_ATTEMPTS_INVALID",
    )
    terminal = _exact_bool(body["terminal"], "PROCESS_TERMINAL_TYPE_INVALID")
    complete = _exact_bool(body["complete"], "PROCESS_COMPLETE_TYPE_INVALID")
    if body["state_kind"] == TERMINAL_SOURCE_ABSENCE:
        if attempts != 0 or not terminal or not complete:
            raise _BookError("TERMINAL_PROCESS_STATE_INVALID")
    elif body["state_kind"] == TECHNICAL_FAILURE:
        if terminal or complete:
            raise _BookError("TECHNICAL_PROCESS_STATE_INVALID")
    elif body["state_kind"] == UNSEARCHED:
        if attempts != 0 or terminal or complete:
            raise _BookError("UNSEARCHED_PROCESS_STATE_INVALID")
    elif body["state_kind"] == REGIME_UNOBSERVED:
        if (
            attempts != 0
            or not terminal
            or not complete
            or body["reason_code"] != REGIME_UNOBSERVED_REASON
        ):
            raise _BookError("REGIME_UNOBSERVED_STATE_INVALID")
    elif body["state_kind"] == INSUFFICIENT_EVIDENCE:
        if (
            insufficient_binding is None
            or not terminal
            or not complete
            or body["reason_code"] != insufficient_binding["reason_code"]
            or attempts != insufficient_binding["process_rejected_attempt_count"]
        ):
            raise _BookError("INSUFFICIENT_PROCESS_STATE_INVALID")
    else:
        raise _BookError("PROCESS_STATE_KIND_INVALID")
    if source_decision is not None and regime_fit_decision is not None:
        source_body = _source_body(source_decision)
        fit_body = _regime_fit_body(regime_fit_decision, source_decision)
        if (
            body["source_group_decision_sha256"] != source_decision.receipt_sha256
            or body["source_contract_sha256"] != source_body["source_contract_sha256"]
            or body["regime_receipt_sha256"] != regime_fit_decision.receipt_sha256
        ):
            raise _BookError("PROCESS_PROVENANCE_SPLICE")
        fit_support = fit_body["train_regime_support"]
        expected_support_receipt = (
            None if fit_support is None else fit_support["receipt_sha256"]
        )
        if support_receipt != expected_support_receipt:
            raise _BookError("PROCESS_TRAIN_SUPPORT_SPLICE")
        if body["state_kind"] == TERMINAL_SOURCE_ABSENCE:
            if (
                source_body["availability"] != TERMINAL_UNAVAILABLE
                or fit_body["fit_state"] != REGIME_NOT_APPLICABLE
            ):
                raise _BookError("TERMINAL_PROCESS_PROVENANCE_INVALID")
        else:
            if source_body["availability"] != AVAILABLE:
                raise _BookError("NONTERMINAL_PROCESS_PROVENANCE_INVALID")
            fit_state = fit_body["fit_state"]
            observed = (
                fit_support["counts"].get(cell["regime"])
                if fit_state == REGIME_FITTED
                else None
            )
            if body["state_kind"] == REGIME_UNOBSERVED:
                if fit_state != REGIME_FITTED or observed != 0:
                    raise _BookError("REGIME_UNOBSERVED_PROVENANCE_INVALID")
            elif body["state_kind"] == INSUFFICIENT_EVIDENCE:
                if fit_state != REGIME_FITTED or observed is None or observed < 1:
                    raise _BookError("INSUFFICIENT_REGIME_PROVENANCE_INVALID")
            elif body["state_kind"] == UNSEARCHED:
                if fit_state != REGIME_FITTED or observed == 0:
                    raise _BookError("UNSEARCHED_REGIME_FIT_INVALID")
            elif body["state_kind"] == TECHNICAL_FAILURE:
                if fit_state not in {
                    REGIME_FITTED,
                    REGIME_FIT_TECHNICAL_FAILURE,
                } or (fit_state == REGIME_FITTED and observed == 0):
                    raise _BookError("TECHNICAL_REGIME_FIT_INVALID")
    return body


def create_historical_process_state_decision(
    *,
    symbol: Any,
    timeframe: Any,
    regime: Any,
    state_kind: Any,
    reason_code: Any,
    source_group_decision: Any,
    regime_fit_decision: Any,
    attempted_trials_exact: Any,
    horizon_plan: Any = None,
    process_rejection_request: Any = None,
    process_rejection_decision: Any = None,
    horizon_winner_cross_binding_bytes: Any = None,
    winner_raw_production_receipt_bytes: Any = None,
) -> HistoricalProcessStateDecision | None:
    """Create a canonical cell state bound to source and shared regime facts."""

    try:
        cell_id = compute_cell_id(symbol, timeframe, regime)
        source_body = _source_body(source_group_decision)
        fit_body = _regime_fit_body(regime_fit_decision, source_group_decision)
        if (
            cell_id is None
            or source_body["symbol"] != symbol
            or source_body["timeframe"] != timeframe
            or fit_body["symbol"] != symbol
            or fit_body["timeframe"] != timeframe
            or type(state_kind) is not str
        ):
            raise _BookError("PROCESS_STATE_INPUT_INVALID")
        attempts = _exact_int(
            attempted_trials_exact,
            0,
            _MAX_ATTEMPT_COUNT,
            "PROCESS_ATTEMPTS_INVALID",
        )
        insufficient_binding = None
        if state_kind == INSUFFICIENT_EVIDENCE:
            insufficient_binding = _build_insufficient_binding(
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                source_body=source_body,
                fit_body=fit_body,
                reason_code=reason_code,
                attempted_trials_exact=attempts,
                horizon_plan=horizon_plan,
                process_rejection_request=process_rejection_request,
                process_rejection_decision=process_rejection_decision,
                horizon_winner_cross_binding_bytes=(
                    horizon_winner_cross_binding_bytes
                ),
                winner_raw_production_receipt_bytes=(
                    winner_raw_production_receipt_bytes
                ),
            )
        elif any(
            value is not None
            for value in (
                horizon_plan,
                process_rejection_request,
                process_rejection_decision,
                horizon_winner_cross_binding_bytes,
                winner_raw_production_receipt_bytes,
            )
        ):
            raise _BookError("NONINSUFFICIENT_STATE_CARRIES_RAW_EVIDENCE")
        body = {
            "contract_version": PROCESS_STATE_CONTRACT_VERSION,
            "cell": {
                "cell_id": cell_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
            },
            "state_kind": state_kind,
            "reason_code": _reason(reason_code, "PROCESS_REASON_INVALID"),
            "source_group_id": source_body["source_group_id"],
            "source_group_decision_sha256": source_group_decision.receipt_sha256,
            "source_contract_sha256": source_body["source_contract_sha256"],
            "regime_receipt_sha256": regime_fit_decision.receipt_sha256,
            "train_regime_support_receipt_sha256": (
                None
                if fit_body["train_regime_support"] is None
                else fit_body["train_regime_support"]["receipt_sha256"]
            ),
            "insufficient_evidence_binding": insufficient_binding,
            "attempted_trials_exact": attempts,
            "terminal": state_kind
            in {
                TERMINAL_SOURCE_ABSENCE,
                REGIME_UNOBSERVED,
                INSUFFICIENT_EVIDENCE,
            },
            "complete": state_kind
            in {
                TERMINAL_SOURCE_ABSENCE,
                REGIME_UNOBSERVED,
                INSUFFICIENT_EVIDENCE,
            },
        }
        decision = _owned_decision(HistoricalProcessStateDecision, body)
        _process_state_body(decision, source_group_decision, regime_fit_decision)
        return decision
    except Exception:
        return None


@dataclass(frozen=True, slots=True)
class HistoricalBookCellEvidence:
    """Exact input for one cell; winner and process state are mutually exclusive."""

    symbol: str
    timeframe: str
    regime: str
    source_group_decision: HistoricalSourceGroupDecision
    regime_fit_decision: RegimeFitDecision
    winner_decision: winner.WinnerSelectionDecision | None = None
    winner_request: winner.WinnerSelectionRequest | None = None
    process_state_decision: HistoricalProcessStateDecision | None = None
    insufficient_horizon_plan: Any = None
    insufficient_process_rejection_request: winner.WinnerSelectionRequest | None = None
    insufficient_process_rejection_decision: winner.WinnerSelectionDecision | None = (
        None
    )
    insufficient_horizon_winner_cross_binding_bytes: bytes | None = None
    insufficient_winner_raw_production_receipt_bytes: bytes | None = None


def winner_cell_evidence(
    decision: Any,
    request: Any,
    source_group_decision: Any,
    regime_fit_decision: Any,
) -> HistoricalBookCellEvidence | None:
    """Build one Winner-backed input after exact Winner V11 request replay."""

    try:
        if (
            type(decision) is not winner.WinnerSelectionDecision
            or type(request) is not winner.WinnerSelectionRequest
            or not winner.verify_historical_cell_winner_decision(decision, request)
            or winner.historical_book_marginal_receipt(decision, request) is None
        ):
            return None
        source_body = _source_body(source_group_decision)
        fit_body = _regime_fit_body(regime_fit_decision, source_group_decision)
        if (
            source_body["availability"] != AVAILABLE
            or fit_body["fit_state"] != REGIME_FITTED
        ):
            return None
        body = decision.to_dict()
        cell = _exact_dict(body["cell"], _CELL_IDENTITY_KEYS, "WINNER_CELL_INVALID")
        if fit_body["train_regime_support"]["counts"].get(cell["regime"]) == 0:
            return None
        precommitment = request.precommitment.to_dict()
        if (
            cell["symbol"] != source_body["symbol"]
            or cell["timeframe"] != source_body["timeframe"]
            or precommitment.get("source_contract_sha256")
            != source_body["source_contract_sha256"]
        ):
            return None
        return HistoricalBookCellEvidence(
            symbol=cell["symbol"],
            timeframe=cell["timeframe"],
            regime=cell["regime"],
            source_group_decision=source_group_decision,
            regime_fit_decision=regime_fit_decision,
            winner_decision=decision,
            winner_request=request,
        )
    except Exception:
        return None


def process_state_cell_evidence(
    *,
    process_state_decision: Any,
    source_group_decision: Any,
    regime_fit_decision: Any,
    horizon_plan: Any = None,
    process_rejection_request: Any = None,
    process_rejection_decision: Any = None,
    horizon_winner_cross_binding_bytes: Any = None,
    winner_raw_production_receipt_bytes: Any = None,
) -> HistoricalBookCellEvidence | None:
    """Build one process-state input from its exact typed canonical decision."""

    try:
        body = _process_state_body(
            process_state_decision,
            source_group_decision,
            regime_fit_decision,
        )
        if body["state_kind"] == INSUFFICIENT_EVIDENCE:
            expected_binding = _build_insufficient_binding(
                symbol=body["cell"]["symbol"],
                timeframe=body["cell"]["timeframe"],
                regime=body["cell"]["regime"],
                source_body=_source_body(source_group_decision),
                fit_body=_regime_fit_body(regime_fit_decision, source_group_decision),
                reason_code=body["reason_code"],
                attempted_trials_exact=body["attempted_trials_exact"],
                horizon_plan=horizon_plan,
                process_rejection_request=process_rejection_request,
                process_rejection_decision=process_rejection_decision,
                horizon_winner_cross_binding_bytes=(
                    horizon_winner_cross_binding_bytes
                ),
                winner_raw_production_receipt_bytes=(
                    winner_raw_production_receipt_bytes
                ),
            )
            if body["insufficient_evidence_binding"] != expected_binding:
                raise _BookError("INSUFFICIENT_BINDING_REPLAY_MISMATCH")
        elif any(
            value is not None
            for value in (
                horizon_plan,
                process_rejection_request,
                process_rejection_decision,
                horizon_winner_cross_binding_bytes,
                winner_raw_production_receipt_bytes,
            )
        ):
            raise _BookError("NONINSUFFICIENT_EVIDENCE_CARRIES_RAW_BINDING")
        cell = body["cell"]
        return HistoricalBookCellEvidence(
            symbol=cell["symbol"],
            timeframe=cell["timeframe"],
            regime=cell["regime"],
            source_group_decision=source_group_decision,
            regime_fit_decision=regime_fit_decision,
            process_state_decision=process_state_decision,
            insufficient_horizon_plan=horizon_plan,
            insufficient_process_rejection_request=process_rejection_request,
            insufficient_process_rejection_decision=process_rejection_decision,
            insufficient_horizon_winner_cross_binding_bytes=(
                horizon_winner_cross_binding_bytes
            ),
            insufficient_winner_raw_production_receipt_bytes=(
                winner_raw_production_receipt_bytes
            ),
        )
    except Exception:
        return None


def adjust_p_values_by(p_values: Any) -> tuple[float, ...] | None:
    """Return exact-order Benjamini-Yekutieli q-values for at most 2,400 tests."""

    try:
        if type(p_values) not in {list, tuple} or not 1 <= len(p_values) <= FAMILY_SIZE:
            return None
        values = [_probability(value, "P_VALUE_INVALID") for value in p_values]
        count = len(values)
        order = sorted(range(count), key=lambda index: (values[index], index))
        with localcontext() as context:
            context.prec = 60
            harmonic = sum(
                (Decimal(1) / Decimal(index) for index in range(1, count + 1)),
                Decimal(0),
            )
            adjusted = [Decimal(1)] * count
            running = Decimal(1)
            for reverse_index in range(count - 1, -1, -1):
                original_index = order[reverse_index]
                rank = Decimal(reverse_index + 1)
                candidate = (
                    Decimal(str(values[original_index]))
                    * Decimal(count)
                    * harmonic
                    / rank
                )
                running = min(running, candidate, Decimal(1))
                adjusted[original_index] = running
        return tuple(float(value) for value in adjusted)
    except Exception:
        return None


_MARGINAL_KEYS: Final = frozenset(
    {
        "cell_id",
        "effective_trials",
        "evidence_candidate_id",
        "evidence_scope",
        "local_admissibility_passed",
        "marginal_p_value",
        "p_value_contract_sha256",
        "p_value_kind",
        "parameter_evidence_inherited",
        "process_sha256",
        "source_evidence_sha256",
        "upstream_evidence_receipt_sha256",
        "within_cell_attempted_trials_exact",
        "within_cell_multiplicity_adjusted",
    }
)


def _winner_record(
    evidence: HistoricalBookCellEvidence,
    ordinal: int,
    expected_cell_id: str,
    source_body: dict[str, Any],
) -> dict[str, Any]:
    if (
        type(evidence.winner_decision) is not winner.WinnerSelectionDecision
        or type(evidence.winner_request) is not winner.WinnerSelectionRequest
        or evidence.process_state_decision is not None
        or evidence.insufficient_horizon_plan is not None
        or evidence.insufficient_process_rejection_request is not None
        or evidence.insufficient_process_rejection_decision is not None
        or evidence.insufficient_horizon_winner_cross_binding_bytes is not None
        or evidence.insufficient_winner_raw_production_receipt_bytes is not None
    ):
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_INPUT_INVALID")
    if (
        winner.WINNER_SELECTION_CONTRACT_VERSION
        != PINNED_WINNER_SELECTION_CONTRACT_VERSION
        or winner.WINNER_SELECTION_REQUEST_VERSION
        != PINNED_WINNER_SELECTION_REQUEST_VERSION
        or winner.EVIDENCE_LEDGER_VERSION != PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        or winner.ATTEMPT_LEDGER_VERSION != PINNED_WINNER_ATTEMPT_LEDGER_VERSION
        or winner.TRADE_POWER_BINDING_VERSION
        != PINNED_WINNER_TRADE_POWER_BINDING_VERSION
    ):
        raise _BookError("WINNER_CONTRACT_VERSION_DRIFT")
    marginal = winner.historical_book_marginal_receipt(
        evidence.winner_decision, evidence.winner_request
    )
    if type(marginal) is not dict or frozenset(marginal) != _MARGINAL_KEYS:
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_REPLAY_REJECTED")
    body = evidence.winner_decision.to_dict()
    cell = body.get("cell")
    if type(cell) is not dict or cell != {
        "cell_id": expected_cell_id,
        "symbol": evidence.symbol,
        "timeframe": evidence.timeframe,
        "regime": evidence.regime,
    }:
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_CELL_MISMATCH")
    request_sha = _hex64(
        evidence.winner_request.request_sha256,
        f"CELL_{ordinal:04d}_WINNER_REQUEST_INVALID",
    )
    if body.get("request_sha256") != request_sha:
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_REQUEST_SPLICE")
    attempt_body = evidence.winner_request.attempt_ledger.to_dict()
    evidence_body = evidence.winner_request.evidence_ledger.to_dict()
    if (
        attempt_body.get("contract_version") != PINNED_WINNER_ATTEMPT_LEDGER_VERSION
        or evidence_body.get("contract_version")
        != PINNED_WINNER_EVIDENCE_LEDGER_VERSION
    ):
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_LEDGER_VERSION_INVALID")
    attempt_sha = _hex64(
        evidence.winner_request.attempt_ledger.payload_sha256,
        f"CELL_{ordinal:04d}_WINNER_ATTEMPT_LEDGER_INVALID",
    )
    evidence_sha = _hex64(
        evidence.winner_request.evidence_ledger.payload_sha256,
        f"CELL_{ordinal:04d}_WINNER_EVIDENCE_INVALID",
    )
    if (
        evidence.winner_request.precommitment.to_dict().get("source_contract_sha256")
        != source_body["source_contract_sha256"]
    ):
        raise _BookError(f"CELL_{ordinal:04d}_SOURCE_CONTRACT_SPLICE")
    decision_receipt = _hex64(
        evidence.winner_decision.receipt_sha256,
        f"CELL_{ordinal:04d}_WINNER_RECEIPT_INVALID",
    )
    if marginal["upstream_evidence_receipt_sha256"] != decision_receipt:
        raise _BookError(f"CELL_{ordinal:04d}_MARGINAL_RECEIPT_MISMATCH")
    if (
        marginal["cell_id"] != expected_cell_id
        or marginal["p_value_contract_sha256"] != PINNED_P_VALUE_CONTRACT_SHA256
        or marginal["p_value_kind"] != "ONE_SIDED_SUPERIORITY"
        or marginal["within_cell_multiplicity_adjusted"] is not True
        or marginal["evidence_scope"] != "PROCESS_LEVEL_ONLY"
        or marginal["parameter_evidence_inherited"] is not False
        or type(marginal["local_admissibility_passed"]) is not bool
    ):
        raise _BookError(f"CELL_{ordinal:04d}_MARGINAL_SEMANTICS_INVALID")
    attempts = _exact_int(
        marginal["within_cell_attempted_trials_exact"],
        1,
        _MAX_ATTEMPT_COUNT,
        f"CELL_{ordinal:04d}_ATTEMPT_COUNT_INVALID",
    )
    effective = _positive_float(
        marginal["effective_trials"],
        float(attempts),
        f"CELL_{ordinal:04d}_EFFECTIVE_TRIALS_INVALID",
    )
    p_value = _probability(
        marginal["marginal_p_value"], f"CELL_{ordinal:04d}_MARGINAL_P_INVALID"
    )
    process_sha = _hex64(
        marginal["process_sha256"], f"CELL_{ordinal:04d}_PROCESS_SHA_INVALID"
    )
    marginal_evidence_sha = _hex64(
        marginal["source_evidence_sha256"],
        f"CELL_{ordinal:04d}_WINNER_EVIDENCE_INVALID",
    )
    if marginal_evidence_sha != evidence_sha:
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_EVIDENCE_SPLICE")
    candidate_id = marginal["evidence_candidate_id"]
    if type(candidate_id) is not str or not 1 <= len(candidate_id) <= 256:
        raise _BookError(f"CELL_{ordinal:04d}_CANDIDATE_ID_INVALID")
    admissible = marginal["local_admissibility_passed"]
    reporting = body.get("winner") if admissible else body.get("reporting_candidate")
    if (
        type(reporting) is not dict
        or reporting.get("process_candidate_id") != candidate_id
        or reporting.get("process_sha256") != process_sha
    ):
        raise _BookError(f"CELL_{ordinal:04d}_REPORTING_PROCESS_MISMATCH")
    state = "WINNER" if admissible else "NO_WINNER"
    if admissible and body.get("status") != "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY":
        raise _BookError(f"CELL_{ordinal:04d}_WINNER_STATUS_INVALID")
    if not admissible and p_value != 1.0:
        raise _BookError(f"CELL_{ordinal:04d}_NO_WINNER_MUST_BE_P_ONE")
    return {
        "historical_state": state,
        "state_reason": _reason(
            body.get("status"), f"CELL_{ordinal:04d}_STATUS_INVALID"
        ),
        "historical_outcome_receipt_sha256": decision_receipt,
        "winner_request_sha256": request_sha,
        "winner_attempt_ledger_payload_sha256": attempt_sha,
        "winner_decision_sha256": decision_receipt,
        "winner_evidence_ledger_payload_sha256": evidence_sha,
        "local_process_candidate_id": candidate_id,
        "local_process_sha256": process_sha,
        "process_state_payload": None,
        "process_state_receipt_sha256": None,
        "within_cell_attempted_trials_exact": attempts,
        "effective_trials": effective,
        "marginal_p_value": p_value,
    }


def _state_record(
    evidence: HistoricalBookCellEvidence,
    ordinal: int,
    source_decision: HistoricalSourceGroupDecision,
    regime_fit_decision: RegimeFitDecision,
) -> dict[str, Any]:
    if (
        evidence.winner_decision is not None
        or evidence.winner_request is not None
        or type(evidence.process_state_decision) is not HistoricalProcessStateDecision
    ):
        raise _BookError(f"CELL_{ordinal:04d}_PROCESS_STATE_INPUT_INVALID")
    body = _process_state_body(
        evidence.process_state_decision, source_decision, regime_fit_decision
    )
    raw_insufficient = (
        evidence.insufficient_horizon_plan,
        evidence.insufficient_process_rejection_request,
        evidence.insufficient_process_rejection_decision,
        evidence.insufficient_horizon_winner_cross_binding_bytes,
        evidence.insufficient_winner_raw_production_receipt_bytes,
    )
    if body["state_kind"] == INSUFFICIENT_EVIDENCE:
        replayed = _build_insufficient_binding(
            symbol=evidence.symbol,
            timeframe=evidence.timeframe,
            regime=evidence.regime,
            source_body=_source_body(source_decision),
            fit_body=_regime_fit_body(regime_fit_decision, source_decision),
            reason_code=body["reason_code"],
            attempted_trials_exact=body["attempted_trials_exact"],
            horizon_plan=evidence.insufficient_horizon_plan,
            process_rejection_request=(evidence.insufficient_process_rejection_request),
            process_rejection_decision=(
                evidence.insufficient_process_rejection_decision
            ),
            horizon_winner_cross_binding_bytes=(
                evidence.insufficient_horizon_winner_cross_binding_bytes
            ),
            winner_raw_production_receipt_bytes=(
                evidence.insufficient_winner_raw_production_receipt_bytes
            ),
        )
        if replayed != body["insufficient_evidence_binding"]:
            raise _BookError(f"CELL_{ordinal:04d}_INSUFFICIENT_BINDING_MISMATCH")
    elif any(value is not None for value in raw_insufficient):
        raise _BookError(f"CELL_{ordinal:04d}_RAW_BINDING_ON_OTHER_STATE")
    expected_cell = {
        "cell_id": compute_cell_id(
            evidence.symbol, evidence.timeframe, evidence.regime
        ),
        "symbol": evidence.symbol,
        "timeframe": evidence.timeframe,
        "regime": evidence.regime,
    }
    if body["cell"] != expected_cell:
        raise _BookError(f"CELL_{ordinal:04d}_PROCESS_CELL_MISMATCH")
    state = {
        TERMINAL_SOURCE_ABSENCE: "SOURCE_UNAVAILABLE",
        TECHNICAL_FAILURE: TECHNICAL_FAILURE,
        UNSEARCHED: UNSEARCHED,
        REGIME_UNOBSERVED: REGIME_UNOBSERVED,
        INSUFFICIENT_EVIDENCE: INSUFFICIENT_EVIDENCE,
    }[body["state_kind"]]
    return {
        "historical_state": state,
        "state_reason": body["reason_code"],
        "historical_outcome_receipt_sha256": (
            evidence.process_state_decision.receipt_sha256
        ),
        "winner_request_sha256": None,
        "winner_attempt_ledger_payload_sha256": None,
        "winner_decision_sha256": None,
        "winner_evidence_ledger_payload_sha256": None,
        "local_process_candidate_id": None,
        "local_process_sha256": None,
        "process_state_payload": body,
        "process_state_receipt_sha256": (
            evidence.process_state_decision.receipt_sha256
        ),
        "within_cell_attempted_trials_exact": body["attempted_trials_exact"],
        "effective_trials": 0.0,
        "marginal_p_value": 1.0,
    }


def _entry_identity(record: dict[str, Any]) -> str:
    return _sha256_json(
        {
            "book_contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION,
            **record,
        }
    )


_CELL_KEYS: Final = frozenset(
    {
        "ordinal",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_group_id",
        "source_group_decision_sha256",
        "regime_receipt_sha256",
        "regime_detector_record_sha256",
        "train_regime_support_receipt_sha256",
        "historical_state",
        "state_reason",
        "historical_outcome_receipt_sha256",
        "winner_request_sha256",
        "winner_attempt_ledger_payload_sha256",
        "winner_decision_sha256",
        "winner_evidence_ledger_payload_sha256",
        "local_process_candidate_id",
        "local_process_sha256",
        "process_state_payload",
        "process_state_receipt_sha256",
        "within_cell_attempted_trials_exact",
        "effective_trials",
        "marginal_p_value",
        "adjusted_q_value",
        "by_qualified",
        "historical_local_candidate_pending_causal",
        "historical_book_entry_sha256",
    }
)

_DECISION_KEYS: Final = frozenset(
    {
        "contract_version",
        "selection_scope",
        "status",
        "valid",
        "reason_codes",
        "family_size",
        "source_group_size",
        "symbols",
        "timeframes",
        "regimes",
        "family_precommitment_sha256",
        "p_value_contract_sha256",
        "method",
        "target_fdr",
        "selection_rule",
        "by_is_diagnostic_only",
        "individual_family_wide_fdr_claimed",
        "causal_process_validation_required",
        "causal_process_validated",
        "optimization_run_complete",
        "counts",
        "source_group_counts",
        "regime_fit_counts",
        "diagnostic_by_qualified_count",
        "historical_local_candidate_count",
        "historical_local_candidate_cell_ids",
        "book_generation_sha256",
        "source_groups",
        "regime_fits",
        "cells",
        "historical_only",
        "live_authorized",
        "order_authorized",
        "capital_sizing_authorized",
        "profitability_claimed",
        "production_integrated",
    }
)


@dataclass(frozen=True, slots=True)
class BookMultiplicityDecision:
    _body_json: bytes
    _receipt_sha256: str

    def _body(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def status(self) -> str:
        try:
            value = self._body().get("status")
            return value if type(value) is str else "REJECTED"
        except Exception:
            return "REJECTED"

    @property
    def reason_codes(self) -> tuple[str, ...]:
        try:
            value = self._body().get("reason_codes")
            return tuple(value) if type(value) is list else ("INVALID_DECISION_BODY",)
        except Exception:
            return ("INVALID_DECISION_BODY",)

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    @property
    def book_generation_sha256(self) -> str | None:
        try:
            value = self._body().get("book_generation_sha256")
            return value if type(value) is str else None
        except Exception:
            return None

    @property
    def historical_local_candidate_cell_ids(self) -> tuple[str, ...]:
        try:
            value = self._body().get("historical_local_candidate_cell_ids")
            return tuple(value) if type(value) is list else ()
        except Exception:
            return ()

    @property
    def valid(self) -> bool:
        try:
            return self._body().get("valid") is True and (
                verify_historical_book_multiplicity_decision(self)
            )
        except Exception:
            return False

    def to_dict(self) -> dict[str, Any]:
        body = self._body()
        body["receipt_sha256"] = self._receipt_sha256
        return body


def _make_decision(body: dict[str, Any]) -> BookMultiplicityDecision:
    encoded = _canonical_bytes(body)
    if len(encoded) > _MAX_DECISION_BYTES:
        raise _BookError("DECISION_BYTE_LIMIT_EXCEEDED")
    return BookMultiplicityDecision(encoded, _sha256_bytes(encoded))


def _rejected(reason: str) -> BookMultiplicityDecision:
    safe = (
        reason if type(reason) is str and _SAFE_REASON.fullmatch(reason) else "REJECTED"
    )
    return _make_decision(
        {
            "contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION,
            "selection_scope": BOOK_MULTIPLICITY_SCOPE,
            "status": "REJECTED",
            "valid": False,
            "reason_codes": [safe],
            "historical_only": True,
            "live_authorized": False,
            "order_authorized": False,
            "capital_sizing_authorized": False,
            "profitability_claimed": False,
            "production_integrated": False,
        }
    )


def _iter_exact_family(value: Any) -> Iterable[HistoricalBookCellEvidence]:
    if isinstance(value, (str, bytes, bytearray, dict)) or value is None:
        raise _BookError("FAMILY_INPUT_INVALID")
    try:
        iterator = iter(value)
    except TypeError as exc:
        raise _BookError("FAMILY_INPUT_INVALID") from exc
    for ordinal in range(FAMILY_SIZE):
        try:
            evidence = next(iterator)
        except StopIteration as exc:
            raise _BookError("FAMILY_INCOMPLETE") from exc
        if type(evidence) is not HistoricalBookCellEvidence:
            raise _BookError(f"CELL_{ordinal:04d}_INPUT_TYPE_INVALID")
        yield evidence
    try:
        next(iterator)
    except StopIteration:
        return
    raise _BookError("FAMILY_OVERSIZED")


def _serialized_owned(decision: Any) -> dict[str, Any]:
    body = json.loads(decision._body_json.decode("ascii"))
    return {"payload": body, "receipt_sha256": decision.receipt_sha256}


def evaluate_historical_book_multiplicity(
    evidence_family: Any,
) -> BookMultiplicityDecision:
    """Evaluate the exact historical 2,400-cell family and nothing else."""

    try:
        records: list[dict[str, Any]] = []
        source_groups: list[dict[str, Any]] = []
        regime_fits: list[dict[str, Any]] = []
        outcome_receipts: set[str] = set()
        active_source: HistoricalSourceGroupDecision | None = None
        active_fit: RegimeFitDecision | None = None
        active_source_body: dict[str, Any] | None = None
        active_fit_body: dict[str, Any] | None = None
        for ordinal, evidence in enumerate(_iter_exact_family(evidence_family)):
            symbol, timeframe, regime = _cell_tuple(ordinal)
            if (evidence.symbol, evidence.timeframe, evidence.regime) != (
                symbol,
                timeframe,
                regime,
            ):
                raise _BookError(f"CELL_{ordinal:04d}_ROSTER_MISMATCH")
            source_body = _source_body(evidence.source_group_decision)
            fit_body = _regime_fit_body(
                evidence.regime_fit_decision, evidence.source_group_decision
            )
            if source_body["symbol"] != symbol or source_body["timeframe"] != timeframe:
                raise _BookError(f"CELL_{ordinal:04d}_SOURCE_GROUP_MISMATCH")
            if fit_body["symbol"] != symbol or fit_body["timeframe"] != timeframe:
                raise _BookError(f"CELL_{ordinal:04d}_REGIME_FIT_MISMATCH")
            if regime == REGIME_ORDER[0]:
                active_source = evidence.source_group_decision
                active_fit = evidence.regime_fit_decision
                active_source_body = source_body
                active_fit_body = fit_body
                source_groups.append(_serialized_owned(active_source))
                regime_fits.append(_serialized_owned(active_fit))
            elif (
                active_source is None
                or active_fit is None
                or active_source_body is None
                or active_fit_body is None
                or evidence.source_group_decision.receipt_sha256
                != active_source.receipt_sha256
                or evidence.source_group_decision._body_json != active_source._body_json
            ):
                raise _BookError(f"CELL_{ordinal:04d}_CONTRADICTORY_SOURCE_GROUP")
            elif (
                evidence.regime_fit_decision.receipt_sha256 != active_fit.receipt_sha256
                or evidence.regime_fit_decision._body_json != active_fit._body_json
            ):
                raise _BookError(f"CELL_{ordinal:04d}_CONTRADICTORY_REGIME_FIT")
            cell_id = compute_cell_id(symbol, timeframe, regime)
            if cell_id is None:
                raise _BookError(f"CELL_{ordinal:04d}_IDENTITY_INVALID")
            if (
                evidence.winner_decision is not None
                or evidence.winner_request is not None
            ):
                specific = _winner_record(evidence, ordinal, cell_id, source_body)
            else:
                specific = _state_record(
                    evidence,
                    ordinal,
                    evidence.source_group_decision,
                    evidence.regime_fit_decision,
                )
            outcome = specific["historical_outcome_receipt_sha256"]
            if outcome in outcome_receipts:
                raise _BookError(f"CELL_{ordinal:04d}_OUTCOME_RECEIPT_REUSED")
            outcome_receipts.add(outcome)
            record = {
                "ordinal": ordinal,
                "cell_id": cell_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
                "source_group_id": source_body["source_group_id"],
                "source_group_decision_sha256": (
                    evidence.source_group_decision.receipt_sha256
                ),
                "regime_receipt_sha256": evidence.regime_fit_decision.receipt_sha256,
                "regime_detector_record_sha256": fit_body[
                    "regime_detector_record_sha256"
                ],
                "train_regime_support_receipt_sha256": (
                    None
                    if fit_body["train_regime_support"] is None
                    else fit_body["train_regime_support"]["receipt_sha256"]
                ),
                **specific,
            }
            records.append(record)

        for group_ordinal in range(SOURCE_GROUP_SIZE):
            group_records = records[
                group_ordinal * len(REGIME_ORDER) : (group_ordinal + 1)
                * len(REGIME_ORDER)
            ]
            source_payload = source_groups[group_ordinal]["payload"]
            fit_payload = regime_fits[group_ordinal]["payload"]
            states = {record["historical_state"] for record in group_records}
            if source_payload["availability"] == TERMINAL_UNAVAILABLE:
                if states != {"SOURCE_UNAVAILABLE"}:
                    raise _BookError(
                        f"GROUP_{group_ordinal:03d}_TERMINAL_SOURCE_CONTRADICTION"
                    )
            elif fit_payload["fit_state"] == REGIME_FIT_TECHNICAL_FAILURE:
                if states != {TECHNICAL_FAILURE}:
                    raise _BookError(
                        f"GROUP_{group_ordinal:03d}_FAILED_REGIME_FIT_CONTRADICTION"
                    )
            elif "SOURCE_UNAVAILABLE" in states:
                raise _BookError(
                    f"GROUP_{group_ordinal:03d}_AVAILABLE_SOURCE_CONTRADICTION"
                )
            else:
                counts = fit_payload["train_regime_support"]["counts"]
                for record in group_records:
                    is_unobserved = record["historical_state"] == REGIME_UNOBSERVED
                    if is_unobserved is not (counts[record["regime"]] == 0):
                        raise _BookError(
                            f"GROUP_{group_ordinal:03d}_REGIME_OBSERVATION_CONTRADICTION"
                        )

        q_values = adjust_p_values_by(
            [record["marginal_p_value"] for record in records]
        )
        if q_values is None or len(q_values) != FAMILY_SIZE:
            raise _BookError("BY_ADJUSTMENT_FAILED")
        incomplete = any(
            record["historical_state"] in {TECHNICAL_FAILURE, UNSEARCHED}
            for record in records
        )
        cells: list[dict[str, Any]] = []
        diagnostic_count = 0
        candidate_ids: list[str] = []
        for record, q_value in zip(records, q_values, strict=True):
            qualified = record["historical_state"] == "WINNER" and q_value <= TARGET_FDR
            candidate = record["historical_state"] == "WINNER" and not incomplete
            diagnostic_count += int(qualified)
            if candidate:
                candidate_ids.append(record["cell_id"])
            entry_sha = _entry_identity(record)
            cells.append(
                {
                    **record,
                    "adjusted_q_value": q_value,
                    "by_qualified": qualified,
                    "historical_local_candidate_pending_causal": candidate,
                    "historical_book_entry_sha256": entry_sha,
                }
            )
        counts = {
            state: sum(cell["historical_state"] == state for cell in cells)
            for state in sorted(_OUTPUT_STATES)
        }
        source_counts = {
            state: sum(
                item["payload"]["availability"] == state for item in source_groups
            )
            for state in (AVAILABLE, TERMINAL_UNAVAILABLE)
        }
        regime_counts = {
            state: sum(item["payload"]["fit_state"] == state for item in regime_fits)
            for state in (
                REGIME_FITTED,
                REGIME_FIT_TECHNICAL_FAILURE,
                REGIME_NOT_APPLICABLE,
            )
        }
        if incomplete:
            status = "INCOMPLETE_HISTORICAL_BOOK"
            reasons = ["TECHNICAL_OR_UNSEARCHED_CELL_PRESENT"]
        elif candidate_ids:
            status = "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
            reasons = ["LOCAL_WINNERS_REQUIRE_SEPARATE_CAUSAL_PROCESS_VALIDATION"]
        else:
            status = "NO_LOCAL_WINNERS"
            reasons = ["COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"]
        generation_sha = _sha256_json(
            {
                "family_precommitment_sha256": PINNED_FAMILY_PRECOMMITMENT_SHA256,
                "method": MULTIPLICITY_METHOD,
                "selection_rule": SELECTION_RULE,
                "target_fdr": TARGET_FDR,
                "source_group_receipts": [
                    item["receipt_sha256"] for item in source_groups
                ],
                "regime_fit_receipts": [item["receipt_sha256"] for item in regime_fits],
                "historical_book_entry_sha256": [
                    cell["historical_book_entry_sha256"] for cell in cells
                ],
            }
        )
        return _make_decision(
            {
                "contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION,
                "selection_scope": BOOK_MULTIPLICITY_SCOPE,
                "status": status,
                "valid": not incomplete,
                "reason_codes": reasons,
                "family_size": FAMILY_SIZE,
                "source_group_size": SOURCE_GROUP_SIZE,
                "symbols": list(PINNED_SYMBOL_ORDER),
                "timeframes": list(TIMEFRAME_ORDER),
                "regimes": list(REGIME_ORDER),
                "family_precommitment_sha256": PINNED_FAMILY_PRECOMMITMENT_SHA256,
                "p_value_contract_sha256": PINNED_P_VALUE_CONTRACT_SHA256,
                "method": MULTIPLICITY_METHOD,
                "target_fdr": TARGET_FDR,
                "selection_rule": SELECTION_RULE,
                "by_is_diagnostic_only": True,
                "individual_family_wide_fdr_claimed": False,
                "causal_process_validation_required": True,
                "causal_process_validated": False,
                "optimization_run_complete": not incomplete,
                "counts": counts,
                "source_group_counts": source_counts,
                "regime_fit_counts": regime_counts,
                "diagnostic_by_qualified_count": diagnostic_count,
                "historical_local_candidate_count": len(candidate_ids),
                "historical_local_candidate_cell_ids": candidate_ids,
                "book_generation_sha256": generation_sha,
                "source_groups": source_groups,
                "regime_fits": regime_fits,
                "cells": cells,
                "historical_only": True,
                "live_authorized": False,
                "order_authorized": False,
                "capital_sizing_authorized": False,
                "profitability_claimed": False,
                "production_integrated": False,
            }
        )
    except _BookError as exc:
        return _rejected(str(exc))
    except Exception:
        return _rejected("EVALUATION_FAILED_CLOSED")


def _owned_from_serialized(
    raw: Any,
    decision_type: type[Any],
    code: str,
) -> Any:
    item = _exact_dict(raw, frozenset({"payload", "receipt_sha256"}), code)
    if type(item["payload"]) is not dict:
        raise _BookError(code)
    encoded = _canonical_bytes(item["payload"])
    receipt = _hex64(item["receipt_sha256"], code)
    if receipt != _sha256_bytes(encoded):
        raise _BookError(code)
    return decision_type(encoded, receipt)


def _verify_complete_body(body: dict[str, Any]) -> bool:
    try:
        _exact_dict(body, _DECISION_KEYS, "DECISION_SCHEMA_INVALID")
        if (
            body["contract_version"] != BOOK_MULTIPLICITY_CONTRACT_VERSION
            or body["selection_scope"] != BOOK_MULTIPLICITY_SCOPE
            or type(body["status"]) is not str
            or body["status"]
            not in {
                "INCOMPLETE_HISTORICAL_BOOK",
                "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE",
                "NO_LOCAL_WINNERS",
            }
            or type(body["valid"]) is not bool
            or type(body["reason_codes"]) is not list
            or any(
                type(value) is not str or _SAFE_REASON.fullmatch(value) is None
                for value in body["reason_codes"]
            )
            or type(body["family_size"]) is not int
            or body["family_size"] != FAMILY_SIZE
            or type(body["source_group_size"]) is not int
            or body["source_group_size"] != SOURCE_GROUP_SIZE
            or body["symbols"] != list(PINNED_SYMBOL_ORDER)
            or body["timeframes"] != list(TIMEFRAME_ORDER)
            or body["regimes"] != list(REGIME_ORDER)
            or body["family_precommitment_sha256"] != PINNED_FAMILY_PRECOMMITMENT_SHA256
            or body["p_value_contract_sha256"] != PINNED_P_VALUE_CONTRACT_SHA256
            or body["method"] != MULTIPLICITY_METHOD
            or type(body["target_fdr"]) is not float
            or body["target_fdr"] != TARGET_FDR
            or body["selection_rule"] != SELECTION_RULE
            or body["by_is_diagnostic_only"] is not True
            or body["individual_family_wide_fdr_claimed"] is not False
            or body["causal_process_validation_required"] is not True
            or body["causal_process_validated"] is not False
            or type(body["optimization_run_complete"]) is not bool
            or type(body["historical_only"]) is not bool
            or body["historical_only"] is not True
            or body["live_authorized"] is not False
            or body["order_authorized"] is not False
            or body["capital_sizing_authorized"] is not False
            or body["profitability_claimed"] is not False
            or body["production_integrated"] is not False
            or type(body["source_groups"]) is not list
            or len(body["source_groups"]) != SOURCE_GROUP_SIZE
            or type(body["regime_fits"]) is not list
            or len(body["regime_fits"]) != SOURCE_GROUP_SIZE
            or type(body["cells"]) is not list
            or len(body["cells"]) != FAMILY_SIZE
        ):
            return False
        sources: list[HistoricalSourceGroupDecision] = []
        fits: list[RegimeFitDecision] = []
        for group_ordinal, raw_source in enumerate(body["source_groups"]):
            source = _owned_from_serialized(
                raw_source,
                HistoricalSourceGroupDecision,
                "SOURCE_GROUP_SERIALIZED_INVALID",
            )
            source_body = _source_body(source)
            symbol, timeframe = _group_tuple(group_ordinal)
            if (source_body["symbol"], source_body["timeframe"]) != (symbol, timeframe):
                return False
            raw_fit = body["regime_fits"][group_ordinal]
            fit = _owned_from_serialized(
                raw_fit, RegimeFitDecision, "REGIME_FIT_SERIALIZED_INVALID"
            )
            fit_body = _regime_fit_body(fit, source)
            if (fit_body["symbol"], fit_body["timeframe"]) != (symbol, timeframe):
                return False
            sources.append(source)
            fits.append(fit)
        p_values: list[float] = []
        states: list[str] = []
        entry_hashes: list[str] = []
        outcome_receipts: set[str] = set()
        for ordinal, cell in enumerate(body["cells"]):
            _exact_dict(cell, _CELL_KEYS, "CELL_SCHEMA_INVALID")
            symbol, timeframe, regime = _cell_tuple(ordinal)
            group_ordinal = ordinal // len(REGIME_ORDER)
            source = sources[group_ordinal]
            fit = fits[group_ordinal]
            source_body = _source_body(source)
            fit_body = _regime_fit_body(fit, source)
            cell_id = compute_cell_id(symbol, timeframe, regime)
            if (
                type(cell["ordinal"]) is not int
                or cell["ordinal"] != ordinal
                or cell["symbol"] != symbol
                or cell["timeframe"] != timeframe
                or cell["regime"] != regime
                or cell["cell_id"] != cell_id
                or cell["source_group_id"] != source_body["source_group_id"]
                or cell["source_group_decision_sha256"] != source.receipt_sha256
                or cell["regime_receipt_sha256"] != fit.receipt_sha256
                or cell["regime_detector_record_sha256"]
                != fit_body["regime_detector_record_sha256"]
                or cell["train_regime_support_receipt_sha256"]
                != (
                    None
                    if fit_body["train_regime_support"] is None
                    else fit_body["train_regime_support"]["receipt_sha256"]
                )
                or cell["historical_state"] not in _OUTPUT_STATES
                or type(cell["state_reason"]) is not str
                or _SAFE_REASON.fullmatch(cell["state_reason"]) is None
                or type(cell["by_qualified"]) is not bool
                or type(cell["historical_local_candidate_pending_causal"]) is not bool
            ):
                return False
            outcome = _hex64(
                cell["historical_outcome_receipt_sha256"], "OUTCOME_RECEIPT_INVALID"
            )
            if outcome in outcome_receipts:
                return False
            outcome_receipts.add(outcome)
            attempts = _exact_int(
                cell["within_cell_attempted_trials_exact"],
                0,
                _MAX_ATTEMPT_COUNT,
                "ATTEMPTS_INVALID",
            )
            p_value = _probability(cell["marginal_p_value"], "P_VALUE_INVALID")
            _probability(cell["adjusted_q_value"], "Q_VALUE_INVALID")
            state = cell["historical_state"]
            if state in {"WINNER", "NO_WINNER"}:
                if (
                    source_body["availability"] != AVAILABLE
                    or fit_body["fit_state"] != REGIME_FITTED
                    or cell["winner_decision_sha256"] != outcome
                    or _HEX64.fullmatch(cell["winner_request_sha256"] or "") is None
                    or _HEX64.fullmatch(
                        cell["winner_attempt_ledger_payload_sha256"] or ""
                    )
                    is None
                    or _HEX64.fullmatch(
                        cell["winner_evidence_ledger_payload_sha256"] or ""
                    )
                    is None
                    or _HEX64.fullmatch(cell["local_process_sha256"] or "") is None
                    or type(cell["local_process_candidate_id"]) is not str
                    or not 1 <= len(cell["local_process_candidate_id"]) <= 256
                    or cell["process_state_payload"] is not None
                    or cell["process_state_receipt_sha256"] is not None
                    or attempts < 1
                    or type(cell["effective_trials"]) is not float
                    or not 0.0 < cell["effective_trials"] <= float(attempts)
                    or (state == "NO_WINNER" and p_value != 1.0)
                ):
                    return False
            else:
                if (
                    cell["winner_request_sha256"] is not None
                    or cell["winner_attempt_ledger_payload_sha256"] is not None
                    or cell["winner_decision_sha256"] is not None
                    or cell["winner_evidence_ledger_payload_sha256"] is not None
                    or cell["local_process_candidate_id"] is not None
                    or cell["local_process_sha256"] is not None
                    or type(cell["process_state_payload"]) is not dict
                    or cell["process_state_receipt_sha256"] != outcome
                    or type(cell["effective_trials"]) is not float
                    or cell["effective_trials"] != 0.0
                    or p_value != 1.0
                ):
                    return False
                process = HistoricalProcessStateDecision(
                    _canonical_bytes(cell["process_state_payload"]), outcome
                )
                process_body = _process_state_body(process, source, fit)
                expected_state = {
                    TERMINAL_SOURCE_ABSENCE: "SOURCE_UNAVAILABLE",
                    TECHNICAL_FAILURE: TECHNICAL_FAILURE,
                    UNSEARCHED: UNSEARCHED,
                    REGIME_UNOBSERVED: REGIME_UNOBSERVED,
                    INSUFFICIENT_EVIDENCE: INSUFFICIENT_EVIDENCE,
                }[process_body["state_kind"]]
                if (
                    expected_state != state
                    or process_body["reason_code"] != cell["state_reason"]
                    or process_body["attempted_trials_exact"] != attempts
                ):
                    return False
            identity_record = {
                key: value
                for key, value in cell.items()
                if key
                not in {
                    "adjusted_q_value",
                    "by_qualified",
                    "historical_local_candidate_pending_causal",
                    "historical_book_entry_sha256",
                }
            }
            entry_sha = _entry_identity(identity_record)
            if cell["historical_book_entry_sha256"] != entry_sha:
                return False
            p_values.append(p_value)
            states.append(state)
            entry_hashes.append(entry_sha)
        for group_ordinal in range(SOURCE_GROUP_SIZE):
            group_states = set(
                states[
                    group_ordinal * len(REGIME_ORDER) : (group_ordinal + 1)
                    * len(REGIME_ORDER)
                ]
            )
            source_body = _source_body(sources[group_ordinal])
            fit_body = _regime_fit_body(fits[group_ordinal], sources[group_ordinal])
            if source_body["availability"] == TERMINAL_UNAVAILABLE:
                if group_states != {"SOURCE_UNAVAILABLE"}:
                    return False
            elif fit_body["fit_state"] == REGIME_FIT_TECHNICAL_FAILURE:
                if group_states != {TECHNICAL_FAILURE}:
                    return False
            elif "SOURCE_UNAVAILABLE" in group_states:
                return False
            else:
                counts = fit_body["train_regime_support"]["counts"]
                group_cells = body["cells"][
                    group_ordinal * len(REGIME_ORDER) : (group_ordinal + 1)
                    * len(REGIME_ORDER)
                ]
                if any(
                    (cell["historical_state"] == REGIME_UNOBSERVED)
                    is not (counts[cell["regime"]] == 0)
                    for cell in group_cells
                ):
                    return False
        q_values = adjust_p_values_by(p_values)
        if q_values is None:
            return False
        incomplete = any(state in {TECHNICAL_FAILURE, UNSEARCHED} for state in states)
        candidates: list[str] = []
        diagnostic_count = 0
        for cell, q_value in zip(body["cells"], q_values, strict=True):
            qualified = cell["historical_state"] == "WINNER" and q_value <= TARGET_FDR
            candidate = cell["historical_state"] == "WINNER" and not incomplete
            if (
                cell["adjusted_q_value"] != q_value
                or cell["by_qualified"] is not qualified
                or cell["historical_local_candidate_pending_causal"] is not candidate
            ):
                return False
            diagnostic_count += int(qualified)
            if candidate:
                candidates.append(cell["cell_id"])
        counts = {state: states.count(state) for state in sorted(_OUTPUT_STATES)}
        source_counts = {
            state: sum(
                _source_body(source)["availability"] == state for source in sources
            )
            for state in (AVAILABLE, TERMINAL_UNAVAILABLE)
        }
        regime_counts = {
            state: sum(
                _regime_fit_body(fit, source)["fit_state"] == state
                for fit, source in zip(fits, sources, strict=True)
            )
            for state in (
                REGIME_FITTED,
                REGIME_FIT_TECHNICAL_FAILURE,
                REGIME_NOT_APPLICABLE,
            )
        }
        for mapping, expected_keys, expected_total in (
            (body["counts"], sorted(_OUTPUT_STATES), FAMILY_SIZE),
            (
                body["source_group_counts"],
                [AVAILABLE, TERMINAL_UNAVAILABLE],
                SOURCE_GROUP_SIZE,
            ),
            (
                body["regime_fit_counts"],
                [REGIME_FITTED, REGIME_FIT_TECHNICAL_FAILURE, REGIME_NOT_APPLICABLE],
                SOURCE_GROUP_SIZE,
            ),
        ):
            if (
                type(mapping) is not dict
                or set(mapping) != set(expected_keys)
                or any(type(value) is not int for value in mapping.values())
                or sum(mapping.values()) != expected_total
            ):
                return False
        if incomplete:
            expected_status = "INCOMPLETE_HISTORICAL_BOOK"
            expected_reasons = ["TECHNICAL_OR_UNSEARCHED_CELL_PRESENT"]
        elif candidates:
            expected_status = "HISTORICAL_LOCAL_CANDIDATES_AVAILABLE"
            expected_reasons = [
                "LOCAL_WINNERS_REQUIRE_SEPARATE_CAUSAL_PROCESS_VALIDATION"
            ]
        else:
            expected_status = "NO_LOCAL_WINNERS"
            expected_reasons = ["COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"]
        generation_sha = _sha256_json(
            {
                "family_precommitment_sha256": PINNED_FAMILY_PRECOMMITMENT_SHA256,
                "method": MULTIPLICITY_METHOD,
                "selection_rule": SELECTION_RULE,
                "target_fdr": TARGET_FDR,
                "source_group_receipts": [source.receipt_sha256 for source in sources],
                "regime_fit_receipts": [fit.receipt_sha256 for fit in fits],
                "historical_book_entry_sha256": entry_hashes,
            }
        )
        if (
            type(body["diagnostic_by_qualified_count"]) is not int
            or type(body["historical_local_candidate_count"]) is not int
            or type(body["historical_local_candidate_cell_ids"]) is not list
        ):
            return False
        return bool(
            body["counts"] == counts
            and body["source_group_counts"] == source_counts
            and body["regime_fit_counts"] == regime_counts
            and body["optimization_run_complete"] is (not incomplete)
            and body["valid"] is (not incomplete)
            and body["status"] == expected_status
            and body["reason_codes"] == expected_reasons
            and body["diagnostic_by_qualified_count"] == diagnostic_count
            and body["historical_local_candidate_count"] == len(candidates)
            and body["historical_local_candidate_cell_ids"] == candidates
            and body["book_generation_sha256"] == generation_sha
        )
    except Exception:
        return False


def verify_historical_book_multiplicity_decision(
    decision: Any,
    evidence_family: Any | None = None,
) -> bool:
    """Verify structure and, when supplied, replay the exact 2,400 inputs."""

    try:
        if type(decision) is not BookMultiplicityDecision:
            return False
        if (
            type(decision._body_json) is not bytes
            or len(decision._body_json) > _MAX_DECISION_BYTES
            or type(decision._receipt_sha256) is not str
            or decision._receipt_sha256 != _sha256_bytes(decision._body_json)
        ):
            return False
        body = json.loads(decision._body_json.decode("ascii"))
        if type(body) is not dict or _canonical_bytes(body) != decision._body_json:
            return False
        if body.get("status") == "REJECTED" or not _verify_complete_body(body):
            return False
        if evidence_family is not None:
            replay = evaluate_historical_book_multiplicity(evidence_family)
            return replay._body_json == decision._body_json
        return True
    except Exception:
        return False


def pinned_policy_snapshot() -> dict[str, Any]:
    return {
        **_family_precommitment(),
        "book_scope": BOOK_MULTIPLICITY_SCOPE,
        "family_precommitment_sha256": PINNED_FAMILY_PRECOMMITMENT_SHA256,
        "winner_selection_contract_version": PINNED_WINNER_SELECTION_CONTRACT_VERSION,
        "winner_selection_request_version": PINNED_WINNER_SELECTION_REQUEST_VERSION,
        "winner_attempt_ledger_version": PINNED_WINNER_ATTEMPT_LEDGER_VERSION,
        "winner_evidence_ledger_version": PINNED_WINNER_EVIDENCE_LEDGER_VERSION,
        "winner_trade_power_binding_version": (
            PINNED_WINNER_TRADE_POWER_BINDING_VERSION
        ),
        "historical_states": sorted(_OUTPUT_STATES),
        "one_source_decision_per_symbol_timeframe": True,
        "one_regime_fit_decision_per_symbol_timeframe": True,
        "one_immutable_regime_detector_record_per_available_symbol_timeframe": True,
        "regime_detector_record_schema_version": (
            REGIME_DETECTOR_RECORD_SCHEMA_VERSION
        ),
        "regime_detector_exact_parameters_are_embedded": True,
        "all_sibling_cells_bind_shared_regime_detector_record": True,
        "possible_regime_slots_are_not_forced_observations": True,
        "regime_unobserved_requires_exact_zero_train_labels": True,
        "regime_unobserved_is_complete_p_one_zero_attempts": True,
        "regime_unobserved_requires_train_support_receipt": True,
        "insufficient_evidence_binding_schema_version": (
            INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION
        ),
        "horizon_winner_cross_binding_schema_version": (
            HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION
        ),
        "postsearch_insufficiency_requires_pre_evidence_bijective_cross_binding": True,
        "postsearch_insufficiency_requires_raw_winner_production_receipt": True,
        "insufficient_evidence_requires_positive_train_labels": True,
        "insufficient_evidence_is_complete_nontechnical_p_one": True,
        "insufficient_evidence_has_no_selected_winner_or_refit_leaf": True,
        "insufficient_evidence_does_not_veto_sibling_candidates": True,
        "presearch_boundary_shortfall_attempt_count": 0,
        "postsearch_insufficiency_attempt_count": (
            "EXACT_ALL_PROCESS_REJECTED_ATTEMPT_COUNT"
        ),
        "train_regime_support_schema_version": REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "positive_regime_count_has_no_occupancy_threshold": True,
        "technical_or_unsearched_invalidates_complete_process": True,
        "terminal_source_absence_is_p_one": True,
        "by_is_diagnostic_only": True,
        "individual_family_wide_fdr_claimed": False,
        "causal_process_validation_required": True,
        "live_authorized": False,
        "order_authorized": False,
        "capital_sizing_authorized": False,
        "profitability_claimed": False,
    }


__all__ = [
    "AVAILABLE",
    "BOOK_MULTIPLICITY_CONTRACT_VERSION",
    "BOOK_MULTIPLICITY_SCOPE",
    "BookMultiplicityDecision",
    "FAMILY_SIZE",
    "HistoricalBookCellEvidence",
    "HistoricalProcessStateDecision",
    "HistoricalSourceGroupDecision",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION",
    "WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION",
    "INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON",
    "INSUFFICIENT_EVIDENCE",
    "INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION",
    "INSUFFICIENT_POSTSEARCH_STAGE",
    "INSUFFICIENT_PRESEARCH_BOUNDARY_REASON",
    "INSUFFICIENT_PRESEARCH_STAGE",
    "MULTIPLICITY_METHOD",
    "PINNED_FAMILY_PRECOMMITMENT_SHA256",
    "PINNED_P_VALUE_CONTRACT_SHA256",
    "PINNED_SYMBOL_ORDER",
    "PINNED_WINNER_ATTEMPT_LEDGER_VERSION",
    "PINNED_WINNER_EVIDENCE_LEDGER_VERSION",
    "PINNED_WINNER_SELECTION_CONTRACT_VERSION",
    "PINNED_WINNER_SELECTION_REQUEST_VERSION",
    "PINNED_WINNER_TRADE_POWER_BINDING_VERSION",
    "PROCESS_STATE_CONTRACT_VERSION",
    "REGIME_FIT_CONTRACT_VERSION",
    "REGIME_FITTED",
    "REGIME_DETECTOR_PARAMETER_KEYS",
    "REGIME_DETECTOR_INTEGER_MAXIMA",
    "REGIME_DETECTOR_RECORD_SCHEMA_VERSION",
    "REGIME_DETECTOR_WEIGHT_GROUPS",
    "REGIME_FIT_TECHNICAL_FAILURE",
    "REGIME_NOT_APPLICABLE",
    "REGIME_ORDER",
    "REGIME_UNOBSERVED",
    "REGIME_UNOBSERVED_REASON",
    "REGIME_TRAIN_SUPPORT_SCHEMA_VERSION",
    "REGIME_TRAIN_SUPPORT_VECTOR_ENCODING",
    "REGIME_TRAIN_SUPPORT_WARMUP_POLICY",
    "RegimeFitDecision",
    "SELECTION_RULE",
    "SOURCE_GROUP_CONTRACT_VERSION",
    "SOURCE_GROUP_SIZE",
    "TARGET_FDR",
    "TECHNICAL_FAILURE",
    "TERMINAL_SOURCE_ABSENCE",
    "TERMINAL_UNAVAILABLE",
    "TIMEFRAME_ORDER",
    "UNSEARCHED",
    "WINNER_EVIDENCE",
    "adjust_p_values_by",
    "compute_cell_id",
    "compute_source_group_id",
    "create_historical_process_state_decision",
    "create_historical_source_group_decision",
    "create_regime_detector_record",
    "create_regime_fit_decision",
    "evaluate_historical_book_multiplicity",
    "pinned_policy_snapshot",
    "process_state_cell_evidence",
    "verify_historical_book_multiplicity_decision",
    "winner_cell_evidence",
]
