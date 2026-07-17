"""Causal current-state arbitration for process-validated FxPM specialists.

Historical anchors validate the *runtime decision process*.  They do not grant
post-selection evidence to a winner chosen later at the current cutoff.  This
module therefore never reconstructs a historical return series for a current
winner and never runs a new post-Causal tournament.  It applies the exact
anchor-fitted and current-rerun calibration policy to one canonical closed-bar
snapshot using the institutional hierarchy:

1. calibrated regime probability and conditional survival through the
   candidate expert's measured holding horizon;
2. actual signal presence;
3. conditional net-R utility in one common risk unit after current costs,
   exposure/correlation, and order-state opportunity cost; and
4. evidence strength only as an exact-utility tie-break.

The result is an advisory receipt only.  D1 and lower-timeframe specialists
remain separate structural slots.  This module cannot size capital, place or
manage orders, promote models, authorize funded trading, or claim profit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Final, Mapping

from pm_model_lifecycle import (
    ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
    CURRENT_REFIT_RECEIPT_VERSION,
    CURRENT_REFIT_REGIMES,
    CURRENT_REFIT_TIMEFRAME_ORDER,
    PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
    build_process_validated_current_refit_lineage,
    canonical_receipt_sha256,
    open_typed_receipt,
)


ARBITRATION_CONTRACT_VERSION: Final = "fxpm.specialist-arbitration.v6"
ARBITRATION_POLICY_VERSION: Final = "fxpm.specialist-arbitration-policy.v6"
SIGNAL_SNAPSHOT_RECEIPT_VERSION: Final = "fxpm.current-signal-snapshot.v4"
POLICY_RECEIPT_TYPE: Final = "fxpm.specialist-arbitration-current-policy"
SIGNAL_RECEIPT_TYPE: Final = "fxpm.specialist-arbitration-current-snapshot"
AUTHORITY_SCOPE: Final = "CURRENT_SIGNAL_ADVISORY_ONLY"
TIMEFRAME_ORDER: Final = CURRENT_REFIT_TIMEFRAME_ORDER
TIMEFRAMES: Final = frozenset(TIMEFRAME_ORDER)
REGIME_ORDER: Final = ("TREND", "RANGE", "BREAKOUT", "CHOP")
REGIMES: Final = CURRENT_REFIT_REGIMES

_DECISION_HIERARCHY = (
    "CALIBRATED_REGIME_VALIDITY_AND_CONDITIONAL_SURVIVAL",
    "ACTUAL_SIGNAL_PRESENCE",
    "COMMON_CONDITIONAL_NET_R_UTILITY",
    "EVIDENCE_STRENGTH_EXACT_UTILITY_TIE_ONLY",
)
_UTILITY_UNIT = "NET_R_PER_ONE_RISK_UNIT"
_SELECTION_METHOD = "MAX_NET_R_THEN_EVIDENCE_EXACT_TIES_ONLY"
_PROBABILITY_METHODS = frozenset(
    {"OOS_CALIBRATED_PROBABILITY", "OOS_EMPIRICAL_PERCENTILE"}
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_MAX_INPUT_BYTES = 16_777_216
_MAX_RESULT_BYTES = 4_194_304
_MAX_NODES = 200_000
_MAX_DEPTH = 20
_MAX_LIST = 8_192
_MAX_STRING_BYTES = 16_384
_MAX_DECIMAL_TEXT_BYTES = 128
_MAX_DURATION_SECONDS = 100 * 366 * 24 * 60 * 60
_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS = 300

_POLICY_FIELDS = frozenset(
    {
        "schema_version",
        "policy_id",
        "symbol",
        "historical_process_decision_sha256",
        "process_id",
        "process_definition_sha256",
        "accepted_book_generation_receipt_sha256",
        "accepted_book_generation_sha256",
        "current_refit_receipt_sha256",
        "causal_policy_sha256",
        "causal_precommit_receipt_sha256",
        "causal_evidence_receipt_sha256",
        "causal_reconciliation_receipt_sha256",
        "runtime_policy_definition_sha256",
        "runtime_calibration_bundle_sha256",
        "shared_regime_function_sha256",
        "shared_utility_function_sha256",
        "closed_bar_contract_sha256",
        "portfolio_state_contract_sha256",
        "decision_hierarchy",
        "utility_unit",
        "selection_method",
        "cell_calibration_policies",
        "process_validated_calibration_procedure",
        "current_calibration_rerun",
        "current_signal_only",
        "capital_authority",
        "order_authority",
        "funded_live_authority",
        "profitability_claim",
    }
)
_CELL_POLICY_FIELDS = frozenset(
    {
        "timeframe",
        "regime",
        "edge_decay_evidence_sha256",
        "calibration_receipt_sha256",
        "threshold_selection_receipt_sha256",
        "calibrated_at_utc",
        "calibration_fit_end_utc",
        "maximum_calibration_input_utc",
        "valid_from_utc",
        "valid_through_utc",
        "retrain_due_utc",
        "status",
        "regime_probability_method",
        "minimum_regime_probability",
        "minimum_conditional_survival_probability",
        "minimum_conditional_net_r_utility",
        "thresholds_selected_inside_historical_anchors",
        "rerun_in_current_process",
        "parameter_level_oos_evidence_claimed",
    }
)
_SIGNAL_FIELDS = frozenset(
    {
        "schema_version",
        "symbol",
        "signal_as_of_utc",
        "historical_process_decision_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_receipt_sha256",
        "market_snapshot_sha256",
        "market_data_freshness_evidence_sha256",
        "market_data_current",
        "portfolio_state_snapshot_sha256",
        "portfolio_state_current",
        "timeframe_states",
    }
)
_STATE_FIELDS = frozenset(
    {
        "timeframe",
        "proposal_status",
        "latest_closed_bar_utc",
        "state_calculated_at_utc",
        "regime",
        "current_refit_cell_id",
        "direction",
        "signal_evidence_sha256",
        "closed_bar_only",
        "partial_current_bar_used",
        "shared_runtime_path",
        "regime_state_receipt_sha256",
        "feature_contract_sha256",
        "source_snapshot_sha256",
        "regime_params_sha256",
        "shared_regime_function_sha256",
        "calibration_receipt_sha256",
        "runtime_calibration_bundle_sha256",
        "regime_probability_method",
        "calibrated_regime_probability",
        "current_regime_elapsed_seconds",
        "expert_holding_horizon_seconds",
        "holding_horizon_source_receipt_sha256",
        "survival_horizon_end_utc",
        "conditional_survival_probability",
        "shared_utility_function_sha256",
        "utility_calculation_receipt_sha256",
        "conditional_net_r_utility",
        "evidence_strength",
        "evidence_strength_receipt_sha256",
        "current_cost_snapshot_sha256",
        "exposure_correlation_snapshot_sha256",
        "order_state_opportunity_cost_snapshot_sha256",
        "portfolio_state_snapshot_sha256",
        "parameter_level_oos_evidence",
    }
)
_REQUEST_FIELDS = frozenset(
    {
        "contract_version",
        "current_cutoff_utc",
        "historical_process_decision",
        "accepted_book_generation_receipt",
        "current_refit_receipt",
        "policy_receipt",
        "signal_snapshot_receipt",
    }
)


class _ContractError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _preflight(value: Any) -> None:
    stack = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_NODES:
            raise _ContractError("INPUT_NODE_LIMIT_EXCEEDED")
        if depth > _MAX_DEPTH:
            raise _ContractError("INPUT_DEPTH_LIMIT_EXCEEDED")
        if current is None or isinstance(current, (bool, int)):
            continue
        if isinstance(current, float):
            raise _ContractError("FLOAT_INPUT_FORBIDDEN_USE_CANONICAL_DECIMAL_TEXT")
        if isinstance(current, str):
            if len(current.encode("utf-8")) > _MAX_STRING_BYTES:
                raise _ContractError("INPUT_STRING_LIMIT_EXCEEDED")
            continue
        if isinstance(current, Mapping):
            if len(current) > _MAX_LIST:
                raise _ContractError("INPUT_MAPPING_LIMIT_EXCEEDED")
            for key, item in current.items():
                if not isinstance(key, str) or not key:
                    raise _ContractError("INPUT_KEY_INVALID")
                stack.append((item, depth + 1))
            continue
        if isinstance(current, (list, tuple)):
            if len(current) > _MAX_LIST:
                raise _ContractError("INPUT_LIST_LIMIT_EXCEEDED")
            stack.extend((item, depth + 1) for item in current)
            continue
        raise _ContractError("NON_JSON_INPUT_FORBIDDEN")


def _canonical_bytes(value: Any, *, maximum: int = _MAX_INPUT_BYTES) -> bytes:
    _preflight(value)
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise _ContractError("CANONICAL_JSON_INVALID") from exc
    if len(encoded) > maximum:
        raise _ContractError("INPUT_BYTE_LIMIT_EXCEEDED")
    return encoded


def _owned(value: Any) -> Any:
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _exact(value: Any, fields: frozenset[str], code: str) -> Mapping[str, Any]:
    if type(value) is not dict or frozenset(value) != fields:
        raise _ContractError(code)
    return value


def _required_sha(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        raise _ContractError(code)
    return value


def _required_text(value: Any, code: str, *, maximum: int = 512) -> str:
    if type(value) is not str:
        raise _ContractError(code)
    normalized = value.strip()
    if not normalized or len(normalized.encode("utf-8")) > maximum:
        raise _ContractError(code)
    return normalized


def _required_int(value: Any, code: str, *, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise _ContractError(code)
    return value


def _decimal_text(
    value: Any,
    code: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> tuple[str, Decimal]:
    if (
        type(value) is not str
        or not value
        or len(value.encode("utf-8")) > _MAX_DECIMAL_TEXT_BYTES
    ):
        raise _ContractError(code)
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise _ContractError(code) from exc
    if not parsed.is_finite() or abs(parsed.adjusted()) > 100:
        raise _ContractError(code)
    canonical = str(parsed.normalize())
    if canonical == "-0":
        canonical = "0"
    if value != canonical:
        raise _ContractError(code)
    if minimum is not None and parsed < minimum:
        raise _ContractError(code)
    if maximum is not None and parsed > maximum:
        raise _ContractError(code)
    return canonical, parsed


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value.strip():
        raise _ContractError(code)
    try:
        raw = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise _ContractError(code) from exc
    if raw.tzinfo is None or raw.utcoffset() is None:
        raise _ContractError(code)
    return raw.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _open(receipt: Any, *, receipt_type: str, code: str) -> tuple[dict[str, Any], str]:
    try:
        return open_typed_receipt(
            receipt,
            expected_receipt_type=receipt_type,
        )
    except (TypeError, ValueError, OverflowError, MemoryError, RecursionError) as exc:
        raise _ContractError(code) from exc


@dataclass(frozen=True)
class _Candidate:
    cell: Mapping[str, Any]
    state: Mapping[str, Any]
    cell_policy: Mapping[str, Any]
    regime_probability: Decimal
    survival_probability: Decimal
    net_r_utility: Decimal
    evidence_strength: Decimal
    eligibility_reason: str

    @property
    def candidate_id(self) -> str:
        return str(self.cell["current_refit_cell_id"])

    @property
    def timeframe(self) -> str:
        return str(self.cell["timeframe"])

    @property
    def structural_slot(self) -> str:
        return "D1_STRUCTURAL" if self.timeframe == "D1" else "LOWER_STRUCTURAL"

    @property
    def eligible(self) -> bool:
        return self.eligibility_reason == "ELIGIBLE"


class ArbitrationDecision:
    """Immutable deterministic advisory; never an execution authority."""

    __slots__ = ("_body",)

    def __init__(self, body: Mapping[str, Any]) -> None:
        owned = _owned(body)
        if len(_canonical_bytes(owned, maximum=_MAX_RESULT_BYTES)) > _MAX_RESULT_BYTES:
            raise _ContractError("RESULT_BYTE_LIMIT_EXCEEDED")
        self._body = owned

    def to_dict(self) -> dict[str, Any]:
        return _owned(self._body)


def _parse_policy(
    payload: Mapping[str, Any],
    *,
    lineage: Mapping[str, Any],
    current_refit_receipt_sha256: str,
    verification_time: datetime,
) -> dict[str, Any]:
    policy = _exact(payload, _POLICY_FIELDS, "POLICY_SCHEMA_INVALID")
    if policy["schema_version"] != ARBITRATION_POLICY_VERSION:
        raise _ContractError("POLICY_VERSION_INVALID")
    normalized: dict[str, Any] = {
        "schema_version": ARBITRATION_POLICY_VERSION,
        "policy_id": _required_text(policy["policy_id"], "POLICY_ID_INVALID"),
        "symbol": _required_text(
            policy["symbol"], "POLICY_SYMBOL_INVALID", maximum=64
        ).upper(),
        "historical_process_decision_sha256": _required_sha(
            policy["historical_process_decision_sha256"],
            "POLICY_PROCESS_DECISION_INVALID",
        ),
        "process_id": _required_text(policy["process_id"], "POLICY_PROCESS_ID_INVALID"),
        "process_definition_sha256": _required_sha(
            policy["process_definition_sha256"], "POLICY_PROCESS_DEFINITION_INVALID"
        ),
        "accepted_book_generation_receipt_sha256": _required_sha(
            policy["accepted_book_generation_receipt_sha256"],
            "POLICY_BOOK_RECEIPT_INVALID",
        ),
        "accepted_book_generation_sha256": _required_sha(
            policy["accepted_book_generation_sha256"], "POLICY_BOOK_GENERATION_INVALID"
        ),
        "current_refit_receipt_sha256": _required_sha(
            policy["current_refit_receipt_sha256"], "POLICY_REFIT_RECEIPT_INVALID"
        ),
    }
    for name in (
        "causal_policy_sha256",
        "causal_precommit_receipt_sha256",
        "causal_evidence_receipt_sha256",
        "causal_reconciliation_receipt_sha256",
        "runtime_policy_definition_sha256",
        "runtime_calibration_bundle_sha256",
        "shared_regime_function_sha256",
        "shared_utility_function_sha256",
        "closed_bar_contract_sha256",
        "portfolio_state_contract_sha256",
    ):
        normalized[name] = _required_sha(policy[name], f"POLICY_{name.upper()}_INVALID")
    if (
        normalized["historical_process_decision_sha256"]
        != lineage["historical_process_decision_sha256"]
    ):
        raise _ContractError("POLICY_PROCESS_DECISION_MISMATCH")
    if (
        normalized["process_id"] != lineage["process_id"]
        or normalized["process_definition_sha256"]
        != lineage["process_definition_sha256"]
    ):
        raise _ContractError("POLICY_GLOBAL_PROCESS_MISMATCH")
    for policy_name, lineage_name in (
        ("causal_policy_sha256", "causal_policy_sha256"),
        ("causal_precommit_receipt_sha256", "causal_precommit_receipt_sha256"),
        ("causal_evidence_receipt_sha256", "causal_evidence_receipt_sha256"),
        (
            "causal_reconciliation_receipt_sha256",
            "causal_reconciliation_receipt_sha256",
        ),
    ):
        if normalized[policy_name] != lineage[lineage_name]:
            raise _ContractError("POLICY_CAUSAL_DECISION_IDENTITY_MISMATCH")
    if (
        normalized["accepted_book_generation_receipt_sha256"]
        != lineage["accepted_book_generation_receipt_sha256"]
        or normalized["accepted_book_generation_sha256"]
        != lineage["accepted_book_generation_sha256"]
    ):
        raise _ContractError("POLICY_CURRENT_BOOK_MISMATCH")
    if normalized["current_refit_receipt_sha256"] != current_refit_receipt_sha256:
        raise _ContractError("POLICY_REFIT_RECEIPT_MISMATCH")
    if policy["decision_hierarchy"] != list(_DECISION_HIERARCHY):
        raise _ContractError("POLICY_DECISION_HIERARCHY_INVALID")
    if policy["utility_unit"] != _UTILITY_UNIT:
        raise _ContractError("POLICY_UTILITY_UNIT_INVALID")
    if policy["selection_method"] != _SELECTION_METHOD:
        raise _ContractError("POLICY_SELECTION_METHOD_INVALID")
    if (
        policy["process_validated_calibration_procedure"] is not True
        or policy["current_calibration_rerun"] is not True
        or policy["current_signal_only"] is not True
        or policy["capital_authority"] is not False
        or policy["order_authority"] is not False
        or policy["funded_live_authority"] is not False
        or policy["profitability_claim"] is not False
    ):
        raise _ContractError("POLICY_AUTHORITY_OR_CALIBRATION_SCOPE_INVALID")
    raw_cells = policy["cell_calibration_policies"]
    expected_count = len(TIMEFRAME_ORDER) * len(REGIME_ORDER)
    if type(raw_cells) is not list or len(raw_cells) != expected_count:
        raise _ContractError("POLICY_CELL_CALIBRATION_ROSTER_INVALID")
    cutoff = _utc(lineage["current_cutoff_utc"], "LINEAGE_CURRENT_CUTOFF_INVALID")
    maximum_trusted = verification_time + timedelta(
        seconds=_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS
    )
    cell_policies: dict[tuple[str, str], dict[str, Any]] = {}
    seen_calibration_receipts: set[str] = set()
    seen_threshold_receipts: set[str] = set()
    ordinal = 0
    for timeframe in TIMEFRAME_ORDER:
        for regime in REGIME_ORDER:
            raw = _exact(
                raw_cells[ordinal],
                _CELL_POLICY_FIELDS,
                "POLICY_CELL_CALIBRATION_SCHEMA_INVALID",
            )
            ordinal += 1
            actual_timeframe = _required_text(
                raw["timeframe"], "POLICY_CELL_TIMEFRAME_INVALID"
            ).upper()
            actual_regime = _required_text(
                raw["regime"], "POLICY_CELL_REGIME_INVALID"
            ).upper()
            if (actual_timeframe, actual_regime) != (timeframe, regime):
                raise _ContractError("POLICY_CELL_CALIBRATION_ORDER_INVALID")
            calibrated = _utc(raw["calibrated_at_utc"], "POLICY_CALIBRATED_AT_INVALID")
            fit_end = _utc(raw["calibration_fit_end_utc"], "POLICY_FIT_END_INVALID")
            maximum_input = _utc(
                raw["maximum_calibration_input_utc"], "POLICY_MAXIMUM_INPUT_INVALID"
            )
            valid_from = _utc(raw["valid_from_utc"], "POLICY_VALID_FROM_INVALID")
            valid_through = _utc(
                raw["valid_through_utc"], "POLICY_VALID_THROUGH_INVALID"
            )
            retrain_due = _utc(raw["retrain_due_utc"], "POLICY_RETRAIN_DUE_INVALID")
            if (
                maximum_input > cutoff
                or fit_end != cutoff
                or not cutoff <= calibrated <= maximum_trusted
                or valid_from != cutoff
                or not valid_from < valid_through
                or retrain_due != valid_through
            ):
                raise _ContractError("POLICY_CELL_CALIBRATION_CHRONOLOGY_INVALID")
            if str(raw["status"] or "").upper() != "CALIBRATED":
                raise _ContractError("POLICY_CELL_NOT_CALIBRATED")
            probability_method = _required_text(
                raw["regime_probability_method"], "POLICY_PROBABILITY_METHOD_INVALID"
            ).upper()
            if probability_method not in _PROBABILITY_METHODS:
                raise _ContractError("POLICY_PROBABILITY_METHOD_INVALID")
            probability_text, _ = _decimal_text(
                raw["minimum_regime_probability"],
                "POLICY_MINIMUM_REGIME_PROBABILITY_INVALID",
                minimum=Decimal(0),
                maximum=Decimal(1),
            )
            survival_text, _ = _decimal_text(
                raw["minimum_conditional_survival_probability"],
                "POLICY_MINIMUM_SURVIVAL_INVALID",
                minimum=Decimal(0),
                maximum=Decimal(1),
            )
            utility_text, _ = _decimal_text(
                raw["minimum_conditional_net_r_utility"],
                "POLICY_MINIMUM_NET_R_INVALID",
            )
            if (
                raw["thresholds_selected_inside_historical_anchors"] is not True
                or raw["rerun_in_current_process"] is not True
                or raw["parameter_level_oos_evidence_claimed"] is not False
            ):
                raise _ContractError("POLICY_CELL_CAUSAL_SCOPE_INVALID")
            normalized_cell = {
                "timeframe": timeframe,
                "regime": regime,
                "edge_decay_evidence_sha256": _required_sha(
                    raw["edge_decay_evidence_sha256"], "POLICY_EDGE_DECAY_SHA_INVALID"
                ),
                "calibration_receipt_sha256": _required_sha(
                    raw["calibration_receipt_sha256"], "POLICY_CALIBRATION_SHA_INVALID"
                ),
                "threshold_selection_receipt_sha256": _required_sha(
                    raw["threshold_selection_receipt_sha256"],
                    "POLICY_THRESHOLD_SELECTION_SHA_INVALID",
                ),
                "calibrated_at_utc": _iso(calibrated),
                "calibration_fit_end_utc": _iso(fit_end),
                "maximum_calibration_input_utc": _iso(maximum_input),
                "valid_from_utc": _iso(valid_from),
                "valid_through_utc": _iso(valid_through),
                "retrain_due_utc": _iso(retrain_due),
                "status": "CALIBRATED",
                "regime_probability_method": probability_method,
                "minimum_regime_probability": probability_text,
                "minimum_conditional_survival_probability": survival_text,
                "minimum_conditional_net_r_utility": utility_text,
                "thresholds_selected_inside_historical_anchors": True,
                "rerun_in_current_process": True,
                "parameter_level_oos_evidence_claimed": False,
            }
            if (
                normalized_cell["calibration_receipt_sha256"]
                in seen_calibration_receipts
                or normalized_cell["threshold_selection_receipt_sha256"]
                in seen_threshold_receipts
            ):
                raise _ContractError("POLICY_CELL_CALIBRATION_RECEIPT_REUSED")
            seen_calibration_receipts.add(normalized_cell["calibration_receipt_sha256"])
            seen_threshold_receipts.add(
                normalized_cell["threshold_selection_receipt_sha256"]
            )
            cell_policies[(timeframe, regime)] = normalized_cell
    normalized.update(
        {
            "decision_hierarchy": list(_DECISION_HIERARCHY),
            "utility_unit": _UTILITY_UNIT,
            "selection_method": _SELECTION_METHOD,
            "cell_calibration_policies": cell_policies,
            "process_validated_calibration_procedure": True,
            "current_calibration_rerun": True,
            "current_signal_only": True,
            "capital_authority": False,
            "order_authority": False,
            "funded_live_authority": False,
            "profitability_claim": False,
        }
    )
    return normalized


def _optional_fields_are_none(state: Mapping[str, Any], names: tuple[str, ...]) -> bool:
    return all(state[name] is None for name in names)


def _parse_signal(
    payload: Mapping[str, Any],
    *,
    lineage: Mapping[str, Any],
    policy: Mapping[str, Any],
    current_refit_receipt_sha256: str,
    verification_time: datetime,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    signal = _exact(payload, _SIGNAL_FIELDS, "SIGNAL_SCHEMA_INVALID")
    if signal["schema_version"] != SIGNAL_SNAPSHOT_RECEIPT_VERSION:
        raise _ContractError("SIGNAL_VERSION_INVALID")
    symbol = _required_text(
        signal["symbol"], "SIGNAL_SYMBOL_INVALID", maximum=64
    ).upper()
    if symbol != policy["symbol"]:
        raise _ContractError("SIGNAL_POLICY_SYMBOL_MISMATCH")
    signal_as_of = _utc(signal["signal_as_of_utc"], "SIGNAL_AS_OF_INVALID")
    cutoff = _utc(lineage["current_cutoff_utc"], "LINEAGE_CURRENT_CUTOFF_INVALID")
    if signal_as_of < cutoff:
        raise _ContractError("SIGNAL_AS_OF_BEFORE_CURRENT_REFIT")
    if signal_as_of > verification_time + timedelta(
        seconds=_TRUSTED_CLOCK_MAX_FUTURE_SKEW_SECONDS
    ):
        raise _ContractError("SIGNAL_TRUSTED_CLOCK_IN_FUTURE")
    if (
        _required_sha(
            signal["historical_process_decision_sha256"],
            "SIGNAL_PROCESS_DECISION_INVALID",
        )
        != lineage["historical_process_decision_sha256"]
    ):
        raise _ContractError("SIGNAL_PROCESS_DECISION_MISMATCH")
    if (
        _required_sha(
            signal["accepted_book_generation_receipt_sha256"],
            "SIGNAL_BOOK_RECEIPT_INVALID",
        )
        != lineage["accepted_book_generation_receipt_sha256"]
    ):
        raise _ContractError("SIGNAL_BOOK_RECEIPT_MISMATCH")
    if (
        _required_sha(signal["current_refit_receipt_sha256"], "SIGNAL_REFIT_INVALID")
        != current_refit_receipt_sha256
    ):
        raise _ContractError("SIGNAL_REFIT_RECEIPT_MISMATCH")
    if signal["market_data_current"] is not True:
        raise _ContractError("MARKET_DATA_NOT_CURRENT")
    if signal["portfolio_state_current"] is not True:
        raise _ContractError("PORTFOLIO_STATE_NOT_CURRENT")
    portfolio_sha = _required_sha(
        signal["portfolio_state_snapshot_sha256"], "PORTFOLIO_STATE_SHA_INVALID"
    )
    normalized = {
        "schema_version": SIGNAL_SNAPSHOT_RECEIPT_VERSION,
        "symbol": symbol,
        "signal_as_of_utc": _iso(signal_as_of),
        "market_snapshot_sha256": _required_sha(
            signal["market_snapshot_sha256"], "MARKET_SNAPSHOT_SHA_INVALID"
        ),
        "market_data_freshness_evidence_sha256": _required_sha(
            signal["market_data_freshness_evidence_sha256"],
            "MARKET_FRESHNESS_SHA_INVALID",
        ),
        "market_data_current": True,
        "portfolio_state_snapshot_sha256": portfolio_sha,
        "portfolio_state_current": True,
    }
    raw_states = signal["timeframe_states"]
    if type(raw_states) is not list or len(raw_states) != len(TIMEFRAME_ORDER):
        raise _ContractError("SIGNAL_TIMEFRAME_ROSTER_INVALID")
    states: dict[str, dict[str, Any]] = {}
    optional_expert_fields = (
        "current_refit_cell_id",
        "direction",
        "signal_evidence_sha256",
        "expert_holding_horizon_seconds",
        "holding_horizon_source_receipt_sha256",
        "survival_horizon_end_utc",
        "conditional_survival_probability",
        "utility_calculation_receipt_sha256",
        "conditional_net_r_utility",
        "evidence_strength",
        "evidence_strength_receipt_sha256",
        "current_cost_snapshot_sha256",
        "exposure_correlation_snapshot_sha256",
        "order_state_opportunity_cost_snapshot_sha256",
    )
    utility_fields = (
        "utility_calculation_receipt_sha256",
        "conditional_net_r_utility",
        "evidence_strength",
        "evidence_strength_receipt_sha256",
        "current_cost_snapshot_sha256",
        "exposure_correlation_snapshot_sha256",
        "order_state_opportunity_cost_snapshot_sha256",
    )
    seen_regime_state_receipts: set[str] = set()
    seen_signal_receipts: set[str] = set()
    seen_utility_receipts: set[str] = set()
    seen_evidence_strength_receipts: set[str] = set()
    for expected_timeframe, raw in zip(TIMEFRAME_ORDER, raw_states):
        state = _exact(raw, _STATE_FIELDS, "SIGNAL_STATE_SCHEMA_INVALID")
        timeframe = _required_text(
            state["timeframe"], "SIGNAL_STATE_TIMEFRAME_INVALID"
        ).upper()
        if timeframe != expected_timeframe:
            raise _ContractError("SIGNAL_TIMEFRAME_ORDER_OR_MEMBERSHIP_INVALID")
        latest_bar = _utc(state["latest_closed_bar_utc"], "SIGNAL_LATEST_BAR_INVALID")
        calculated_at = _utc(
            state["state_calculated_at_utc"], "SIGNAL_STATE_CALCULATED_AT_INVALID"
        )
        if not latest_bar <= calculated_at <= signal_as_of:
            raise _ContractError("SIGNAL_CLOSED_BAR_CLOCK_ORDER_INVALID")
        regime = _required_text(state["regime"], "SIGNAL_REGIME_INVALID").upper()
        if regime not in REGIMES:
            raise _ContractError("SIGNAL_REGIME_INVALID")
        cell_policy = policy["cell_calibration_policies"][(timeframe, regime)]
        if signal_as_of > _utc(
            cell_policy["valid_through_utc"], "POLICY_VALID_THROUGH_INVALID"
        ):
            raise _ContractError("CURRENT_CALIBRATION_STALE_BY_EDGE_DECAY_EVIDENCE")
        if (
            state["closed_bar_only"] is not True
            or state["partial_current_bar_used"] is not False
            or state["shared_runtime_path"] is not True
        ):
            raise _ContractError("SIGNAL_RUNTIME_PARITY_OR_CLOSED_BAR_INVALID")
        if state["parameter_level_oos_evidence"] is not False:
            raise _ContractError("SIGNAL_FALSE_PARAMETER_OOS_CLAIM")
        if (
            _required_sha(
                state["shared_regime_function_sha256"], "SIGNAL_REGIME_FUNCTION_INVALID"
            )
            != policy["shared_regime_function_sha256"]
            or _required_sha(
                state["shared_utility_function_sha256"],
                "SIGNAL_UTILITY_FUNCTION_INVALID",
            )
            != policy["shared_utility_function_sha256"]
        ):
            raise _ContractError("SIGNAL_SHARED_RUNTIME_FUNCTION_MISMATCH")
        if (
            _required_sha(
                state["calibration_receipt_sha256"],
                "SIGNAL_CALIBRATION_RECEIPT_INVALID",
            )
            != cell_policy["calibration_receipt_sha256"]
            or _required_sha(
                state["runtime_calibration_bundle_sha256"],
                "SIGNAL_CALIBRATION_BUNDLE_INVALID",
            )
            != policy["runtime_calibration_bundle_sha256"]
        ):
            raise _ContractError("SIGNAL_CALIBRATION_IDENTITY_MISMATCH")
        method = _required_text(
            state["regime_probability_method"], "SIGNAL_PROBABILITY_METHOD_INVALID"
        ).upper()
        if method != cell_policy["regime_probability_method"]:
            raise _ContractError("SIGNAL_PROBABILITY_METHOD_MISMATCH")
        probability_text, _ = _decimal_text(
            state["calibrated_regime_probability"],
            "SIGNAL_REGIME_PROBABILITY_INVALID",
            minimum=Decimal(0),
            maximum=Decimal(1),
        )
        common = {
            "timeframe": timeframe,
            "proposal_status": str(state["proposal_status"] or "").upper(),
            "latest_closed_bar_utc": _iso(latest_bar),
            "state_calculated_at_utc": _iso(calculated_at),
            "regime": regime,
            "closed_bar_only": True,
            "partial_current_bar_used": False,
            "shared_runtime_path": True,
            "regime_state_receipt_sha256": _required_sha(
                state["regime_state_receipt_sha256"], "SIGNAL_REGIME_STATE_INVALID"
            ),
            "feature_contract_sha256": _required_sha(
                state["feature_contract_sha256"], "SIGNAL_FEATURE_CONTRACT_INVALID"
            ),
            "source_snapshot_sha256": _required_sha(
                state["source_snapshot_sha256"], "SIGNAL_SOURCE_SNAPSHOT_INVALID"
            ),
            "regime_params_sha256": _required_sha(
                state["regime_params_sha256"], "SIGNAL_REGIME_PARAMS_INVALID"
            ),
            "shared_regime_function_sha256": policy["shared_regime_function_sha256"],
            "calibration_receipt_sha256": cell_policy["calibration_receipt_sha256"],
            "runtime_calibration_bundle_sha256": policy[
                "runtime_calibration_bundle_sha256"
            ],
            "regime_probability_method": method,
            "calibrated_regime_probability": probability_text,
            "current_regime_elapsed_seconds": _required_int(
                state["current_regime_elapsed_seconds"],
                "SIGNAL_REGIME_ELAPSED_INVALID",
                minimum=0,
                maximum=_MAX_DURATION_SECONDS,
            ),
            "shared_utility_function_sha256": policy["shared_utility_function_sha256"],
            "portfolio_state_snapshot_sha256": _required_sha(
                state["portfolio_state_snapshot_sha256"],
                "SIGNAL_STATE_PORTFOLIO_SHA_INVALID",
            ),
            "parameter_level_oos_evidence": False,
        }
        if common["regime_params_sha256"] != lineage["current_regime_params_sha256"]:
            raise _ContractError("SIGNAL_REGIME_PARAMS_MISMATCH")
        if common["portfolio_state_snapshot_sha256"] != portfolio_sha:
            raise _ContractError("SIGNAL_STATE_PORTFOLIO_SNAPSHOT_MISMATCH")
        if common["regime_state_receipt_sha256"] in seen_regime_state_receipts:
            raise _ContractError("SIGNAL_REGIME_STATE_RECEIPT_REUSED")
        seen_regime_state_receipts.add(common["regime_state_receipt_sha256"])
        status = common["proposal_status"]
        if status == "NO_CURRENT_WINNER":
            if not _optional_fields_are_none(state, optional_expert_fields):
                raise _ContractError("NO_CURRENT_WINNER_HAS_EXPERT_FIELDS")
            normalized_state = {
                **common,
                **{name: None for name in optional_expert_fields},
            }
        elif status in {"NO_SIGNAL", "PROPOSED"}:
            cell_id = _required_sha(
                state["current_refit_cell_id"], "SIGNAL_REFIT_CELL_ID_INVALID"
            )
            signal_evidence = _required_sha(
                state["signal_evidence_sha256"], "SIGNAL_EVIDENCE_SHA_INVALID"
            )
            holding_seconds = _required_int(
                state["expert_holding_horizon_seconds"],
                "SIGNAL_HOLDING_HORIZON_INVALID",
                minimum=1,
                maximum=_MAX_DURATION_SECONDS,
            )
            horizon_end = _utc(
                state["survival_horizon_end_utc"], "SIGNAL_SURVIVAL_HORIZON_END_INVALID"
            )
            if horizon_end != signal_as_of + timedelta(seconds=holding_seconds):
                raise _ContractError("SIGNAL_SURVIVAL_HORIZON_MISMATCH")
            survival_text, _ = _decimal_text(
                state["conditional_survival_probability"],
                "SIGNAL_SURVIVAL_PROBABILITY_INVALID",
                minimum=Decimal(0),
                maximum=Decimal(1),
            )
            normalized_state = {
                **common,
                "current_refit_cell_id": cell_id,
                "signal_evidence_sha256": signal_evidence,
                "expert_holding_horizon_seconds": holding_seconds,
                "holding_horizon_source_receipt_sha256": _required_sha(
                    state["holding_horizon_source_receipt_sha256"],
                    "SIGNAL_HOLDING_HORIZON_SOURCE_INVALID",
                ),
                "survival_horizon_end_utc": _iso(horizon_end),
                "conditional_survival_probability": survival_text,
            }
            if status == "NO_SIGNAL":
                if state["direction"] is not None or not _optional_fields_are_none(
                    state, utility_fields
                ):
                    raise _ContractError("NO_SIGNAL_HAS_PROPOSAL_UTILITY_FIELDS")
                normalized_state.update(
                    {
                        "direction": None,
                        **{name: None for name in utility_fields},
                    }
                )
                if signal_evidence in seen_signal_receipts:
                    raise _ContractError("SIGNAL_EVIDENCE_RECEIPT_REUSED")
                seen_signal_receipts.add(signal_evidence)
            else:
                direction = str(state["direction"] or "").upper()
                if direction not in {"BUY", "SELL"}:
                    raise _ContractError("SIGNAL_DIRECTION_INVALID")
                utility_text, _ = _decimal_text(
                    state["conditional_net_r_utility"], "SIGNAL_NET_R_UTILITY_INVALID"
                )
                strength_text, _ = _decimal_text(
                    state["evidence_strength"],
                    "SIGNAL_EVIDENCE_STRENGTH_INVALID",
                    minimum=Decimal(0),
                    maximum=Decimal(1),
                )
                normalized_state.update(
                    {
                        "direction": direction,
                        "utility_calculation_receipt_sha256": _required_sha(
                            state["utility_calculation_receipt_sha256"],
                            "SIGNAL_UTILITY_RECEIPT_INVALID",
                        ),
                        "conditional_net_r_utility": utility_text,
                        "evidence_strength": strength_text,
                        "evidence_strength_receipt_sha256": _required_sha(
                            state["evidence_strength_receipt_sha256"],
                            "SIGNAL_EVIDENCE_STRENGTH_RECEIPT_INVALID",
                        ),
                        "current_cost_snapshot_sha256": _required_sha(
                            state["current_cost_snapshot_sha256"],
                            "SIGNAL_COST_SNAPSHOT_INVALID",
                        ),
                        "exposure_correlation_snapshot_sha256": _required_sha(
                            state["exposure_correlation_snapshot_sha256"],
                            "SIGNAL_EXPOSURE_SNAPSHOT_INVALID",
                        ),
                        "order_state_opportunity_cost_snapshot_sha256": _required_sha(
                            state["order_state_opportunity_cost_snapshot_sha256"],
                            "SIGNAL_ORDER_STATE_COST_INVALID",
                        ),
                    }
                )
                utility_receipt = normalized_state["utility_calculation_receipt_sha256"]
                strength_receipt = normalized_state["evidence_strength_receipt_sha256"]
                if signal_evidence in seen_signal_receipts:
                    raise _ContractError("SIGNAL_EVIDENCE_RECEIPT_REUSED")
                if utility_receipt in seen_utility_receipts:
                    raise _ContractError("SIGNAL_UTILITY_RECEIPT_REUSED")
                if strength_receipt in seen_evidence_strength_receipts:
                    raise _ContractError("SIGNAL_EVIDENCE_STRENGTH_RECEIPT_REUSED")
                seen_signal_receipts.add(signal_evidence)
                seen_utility_receipts.add(utility_receipt)
                seen_evidence_strength_receipts.add(strength_receipt)
        else:
            raise _ContractError("SIGNAL_PROPOSAL_STATUS_INVALID")
        states[timeframe] = normalized_state
    normalized["timeframe_states"] = [states[item] for item in TIMEFRAME_ORDER]
    return normalized, states


def _candidate_eligibility(
    state: Mapping[str, Any], cell_policy: Mapping[str, Any]
) -> tuple[str, Decimal, Decimal, Decimal, Decimal]:
    probability = Decimal(state["calibrated_regime_probability"])
    probability_floor = Decimal(cell_policy["minimum_regime_probability"])
    if state["proposal_status"] == "NO_CURRENT_WINNER":
        return (
            "NO_CURRENT_WINNER",
            probability,
            Decimal(0),
            Decimal(0),
            Decimal(0),
        )
    survival = Decimal(state["conditional_survival_probability"])
    if probability < probability_floor:
        reason = "REGIME_PROBABILITY_BELOW_PROCESS_VALIDATED_THRESHOLD"
    elif survival < Decimal(cell_policy["minimum_conditional_survival_probability"]):
        reason = "CONDITIONAL_SURVIVAL_BELOW_PROCESS_VALIDATED_THRESHOLD"
    elif state["proposal_status"] == "NO_SIGNAL":
        reason = "NO_ACTUAL_SIGNAL"
    else:
        utility = Decimal(state["conditional_net_r_utility"])
        strength = Decimal(state["evidence_strength"])
        if utility < Decimal(cell_policy["minimum_conditional_net_r_utility"]):
            reason = "NET_R_BELOW_PROCESS_VALIDATED_THRESHOLD"
        else:
            reason = "ELIGIBLE"
        return reason, probability, survival, utility, strength
    return reason, probability, survival, Decimal(0), Decimal(0)


def _candidate_summary(candidate: _Candidate) -> dict[str, Any]:
    cell = candidate.cell
    state = candidate.state
    policy = candidate.cell_policy
    return {
        "current_refit_cell_id": candidate.candidate_id,
        "symbol": cell["symbol"],
        "timeframe": candidate.timeframe,
        "regime": cell["regime"],
        "strategy_id": cell["strategy_id"],
        "direction": state["direction"],
        "structural_slot": candidate.structural_slot,
        "winner_process_candidate_id": cell["winner_process_candidate_id"],
        "winner_process_sha256": cell["winner_process_sha256"],
        "winner_horizon_process_sha256": cell["winner_horizon_process_sha256"],
        "winner_trial_definition_sha256": cell["winner_trial_definition_sha256"],
        "winner_refit_recipe_sha256": cell["winner_refit_recipe_sha256"],
        "winner_decision_sha256": cell["winner_decision_sha256"],
        "winner_book_entry_sha256": cell["winner_book_entry_sha256"],
        "signal_evidence_sha256": state["signal_evidence_sha256"],
        "regime_state_receipt_sha256": state["regime_state_receipt_sha256"],
        "utility_calculation_receipt_sha256": state[
            "utility_calculation_receipt_sha256"
        ],
        "regime_probability_method": state["regime_probability_method"],
        "calibrated_regime_probability": state["calibrated_regime_probability"],
        "minimum_regime_probability": policy["minimum_regime_probability"],
        "conditional_survival_probability": state["conditional_survival_probability"],
        "minimum_conditional_survival_probability": policy[
            "minimum_conditional_survival_probability"
        ],
        "expert_holding_horizon_seconds": state["expert_holding_horizon_seconds"],
        "holding_horizon_source_receipt_sha256": state[
            "holding_horizon_source_receipt_sha256"
        ],
        "current_regime_elapsed_seconds": state["current_regime_elapsed_seconds"],
        "conditional_net_r_utility": state["conditional_net_r_utility"],
        "minimum_conditional_net_r_utility": policy[
            "minimum_conditional_net_r_utility"
        ],
        "evidence_strength": state["evidence_strength"],
        "eligibility_reason": candidate.eligibility_reason,
        "eligible": candidate.eligible,
        "parameter_level_oos_evidence_inherited": False,
        "historical_current_winner_return_tournament_used": False,
    }


def _timeframe_evaluation(
    *,
    state: Mapping[str, Any],
    cell_policy: Mapping[str, Any],
    candidate: _Candidate | None,
) -> dict[str, Any]:
    return {
        "timeframe": state["timeframe"],
        "regime": state["regime"],
        "proposal_status": state["proposal_status"],
        "current_refit_cell_id": state["current_refit_cell_id"],
        "calibration_receipt_sha256": cell_policy["calibration_receipt_sha256"],
        "calibrated_regime_probability": state["calibrated_regime_probability"],
        "minimum_regime_probability": cell_policy["minimum_regime_probability"],
        "conditional_survival_probability": state["conditional_survival_probability"],
        "minimum_conditional_survival_probability": cell_policy[
            "minimum_conditional_survival_probability"
        ],
        "conditional_net_r_utility": state["conditional_net_r_utility"],
        "minimum_conditional_net_r_utility": cell_policy[
            "minimum_conditional_net_r_utility"
        ],
        "eligibility_reason": (
            candidate.eligibility_reason
            if candidate is not None
            else "NO_CURRENT_WINNER"
        ),
        "eligible": candidate.eligible if candidate is not None else False,
    }


def _structural_allocations(
    candidates: list[_Candidate],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    allocations: list[dict[str, Any]] = []
    advisory: list[str] = []
    eligible_ids: list[str] = []
    for slot in ("D1_STRUCTURAL", "LOWER_STRUCTURAL"):
        members = [
            candidate for candidate in candidates if candidate.structural_slot == slot
        ]
        eligible = [candidate for candidate in members if candidate.eligible]
        eligible_ids.extend(candidate.candidate_id for candidate in eligible)
        chosen: _Candidate | None = None
        tie_break_used = False
        tied: list[_Candidate] = []
        if not members:
            status = "NO_CURRENT_PROPOSAL_IN_STRUCTURAL_SLOT"
        elif not eligible:
            status = "NO_ELIGIBLE_CURRENT_PROPOSAL_IN_STRUCTURAL_SLOT"
        else:
            maximum_utility = max(candidate.net_r_utility for candidate in eligible)
            utility_ties = [
                candidate
                for candidate in eligible
                if candidate.net_r_utility == maximum_utility
            ]
            if len(utility_ties) == 1:
                chosen = utility_ties[0]
                status = "MAXIMUM_COMMON_NET_R_ADVISORY"
            else:
                maximum_strength = max(
                    candidate.evidence_strength for candidate in utility_ties
                )
                evidence_ties = [
                    candidate
                    for candidate in utility_ties
                    if candidate.evidence_strength == maximum_strength
                ]
                if len(evidence_ties) == 1:
                    chosen = evidence_ties[0]
                    tie_break_used = True
                    status = "EXACT_UTILITY_TIE_RESOLVED_BY_EVIDENCE_STRENGTH"
                else:
                    tied = evidence_ties
                    status = "EXACT_UTILITY_AND_EVIDENCE_TIE_REPORTED_UNRESOLVED"
        if chosen is not None:
            advisory.append(chosen.candidate_id)
        allocations.append(
            {
                "structural_slot": slot,
                "proposed_candidate_ids": [
                    candidate.candidate_id for candidate in members
                ],
                "eligible_candidate_ids": [
                    candidate.candidate_id for candidate in eligible
                ],
                "exact_tied_candidate_ids": [
                    candidate.candidate_id for candidate in tied
                ],
                "advisory_candidate_id": chosen.candidate_id
                if chosen is not None
                else None,
                "status": status,
                "evidence_strength_tie_break_used": tie_break_used,
                "magic_utility_epsilon_used": False,
                "capital_allocation_authority": False,
                "order_authority": False,
            }
        )
    return allocations, advisory, eligible_ids


def _decision(
    *,
    status: str,
    reason_codes: list[str],
    request_sha256: str | None,
    **fields: Any,
) -> ArbitrationDecision:
    body = {
        "contract_version": ARBITRATION_CONTRACT_VERSION,
        "authority_scope": AUTHORITY_SCOPE,
        "status": status,
        "reason_codes": reason_codes,
        "request_sha256": request_sha256,
        "production_integrated": False,
        "capital_authority": False,
        "order_authority": False,
        "funded_live_authority": False,
        "promotion_authority": False,
        "profitability_claim": False,
        "parameter_level_oos_evidence_inherited": False,
        "historical_current_winner_return_tournament_used": False,
        "raw_regime_margin_or_dwell_ratio_used": False,
        **fields,
    }
    body["receipt_sha256"] = canonical_receipt_sha256(body)
    return ArbitrationDecision(body)


def rank_specialist_surfaces(request: Any) -> ArbitrationDecision:
    """Apply one process-validated calibration policy to current closed bars."""

    request_sha: str | None = None
    try:
        verification_time = datetime.now(timezone.utc)
        owned = _owned(request)
        request_sha = _sha(owned)
        root = _exact(owned, _REQUEST_FIELDS, "REQUEST_SCHEMA_INVALID")
        if root["contract_version"] != ARBITRATION_CONTRACT_VERSION:
            raise _ContractError("REQUEST_VERSION_INVALID")
        current_cutoff = _utc(root["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
        lineage = build_process_validated_current_refit_lineage(
            historical_process_decision=root["historical_process_decision"],
            accepted_book_generation_receipt=root["accepted_book_generation_receipt"],
            current_refit_receipt=root["current_refit_receipt"],
            as_of_utc=_iso(current_cutoff),
        )
        if (
            lineage.get("book_role") != PROCESS_VALIDATED_CURRENT_REFIT_ROLE
            or lineage.get("contract_status") != "PROBATION_ONLY"
            or lineage.get("probation_candidate_eligible") is not True
            or lineage.get("funded_role_eligible") is not False
            or lineage.get("parameter_level_oos_evidence_inherited") is not False
        ):
            raise _ContractError("CURRENT_REFIT_LINEAGE_INVALID")
        current_refit_receipt_sha = str(lineage["current_refit_receipt_sha256"])
        policy_payload, policy_receipt_sha = _open(
            root["policy_receipt"],
            receipt_type=POLICY_RECEIPT_TYPE,
            code="POLICY_RECEIPT_INVALID",
        )
        policy = _parse_policy(
            policy_payload,
            lineage=lineage,
            current_refit_receipt_sha256=current_refit_receipt_sha,
            verification_time=verification_time,
        )
        signal_payload, signal_receipt_sha = _open(
            root["signal_snapshot_receipt"],
            receipt_type=SIGNAL_RECEIPT_TYPE,
            code="SIGNAL_SNAPSHOT_RECEIPT_INVALID",
        )
        signal, states = _parse_signal(
            signal_payload,
            lineage=lineage,
            policy=policy,
            current_refit_receipt_sha256=current_refit_receipt_sha,
            verification_time=verification_time,
        )
        cell_map = {
            cell["current_refit_cell_id"]: cell
            for cell in lineage.get("current_refit_cells", [])
            if cell.get("symbol") == signal["symbol"]
        }
        candidates: list[_Candidate] = []
        evaluation_by_timeframe: dict[str, dict[str, Any]] = {}
        seen_cells: set[str] = set()
        for timeframe in TIMEFRAME_ORDER:
            state = states[timeframe]
            cell_policy = policy["cell_calibration_policies"][
                (timeframe, state["regime"])
            ]
            if state["proposal_status"] == "NO_CURRENT_WINNER":
                if any(
                    cell["timeframe"] == timeframe and cell["regime"] == state["regime"]
                    for cell in cell_map.values()
                ):
                    raise _ContractError("NO_CURRENT_WINNER_CONTRADICTS_CURRENT_BOOK")
                evaluation_by_timeframe[timeframe] = _timeframe_evaluation(
                    state=state,
                    cell_policy=cell_policy,
                    candidate=None,
                )
                continue
            cell_id = state["current_refit_cell_id"]
            if cell_id in seen_cells:
                raise _ContractError("PROPOSED_REFIT_CELL_DUPLICATE")
            seen_cells.add(cell_id)
            cell = cell_map.get(cell_id)
            if cell is None:
                raise _ContractError("SIGNAL_REFIT_CELL_NOT_IN_CURRENT_LINEAGE")
            if cell["timeframe"] != timeframe or cell["regime"] != state["regime"]:
                raise _ContractError("SIGNAL_REFIT_CELL_STATE_MISMATCH")
            if _utc(state["latest_closed_bar_utc"], "SIGNAL_LATEST_BAR_INVALID") < _utc(
                cell["latest_closed_bar_utc"], "REFIT_LATEST_BAR_INVALID"
            ):
                raise _ContractError("SIGNAL_BAR_PREDATES_CURRENT_REFIT_SOURCE")
            reason, probability, survival, utility, strength = _candidate_eligibility(
                state, cell_policy
            )
            candidate = _Candidate(
                cell=cell,
                state=state,
                cell_policy=cell_policy,
                regime_probability=probability,
                survival_probability=survival,
                net_r_utility=utility,
                evidence_strength=strength,
                eligibility_reason=reason,
            )
            candidates.append(candidate)
            evaluation_by_timeframe[timeframe] = _timeframe_evaluation(
                state=state,
                cell_policy=cell_policy,
                candidate=candidate,
            )
        allocations, advisory, eligible = _structural_allocations(
            [
                candidate
                for candidate in candidates
                if candidate.state["proposal_status"] == "PROPOSED"
            ]
        )
        reasons = ["PROCESS_VALIDATED_CURRENT_CALIBRATION_APPLIED"]
        if len(advisory) == 2:
            reasons.append("D1_AND_LOWER_STRUCTURAL_SPECIALISTS_MAY_COEXIST")
        if any(item["exact_tied_candidate_ids"] for item in allocations):
            reasons.append("EXACT_TIE_REPORTED_WITHOUT_ARBITRARY_EPSILON")
        if advisory:
            status = "ADVISORY_READY"
        elif eligible:
            status = "ADVISORY_INDETERMINATE"
        else:
            status = "NO_ACTION"
        return _decision(
            status=status,
            reason_codes=reasons,
            request_sha256=request_sha,
            symbol=signal["symbol"],
            signal_as_of_utc=signal["signal_as_of_utc"],
            common_current_cutoff_utc=lineage["current_cutoff_utc"],
            historical_process_decision_sha256=lineage[
                "historical_process_decision_sha256"
            ],
            process_id=lineage["process_id"],
            process_definition_sha256=lineage["process_definition_sha256"],
            accepted_book_generation_receipt_sha256=lineage[
                "accepted_book_generation_receipt_sha256"
            ],
            accepted_book_generation_sha256=lineage["accepted_book_generation_sha256"],
            current_refit_receipt_sha256=current_refit_receipt_sha,
            policy_receipt_sha256=policy_receipt_sha,
            signal_snapshot_receipt_sha256=signal_receipt_sha,
            runtime_policy_definition_sha256=policy["runtime_policy_definition_sha256"],
            runtime_calibration_bundle_sha256=policy[
                "runtime_calibration_bundle_sha256"
            ],
            decision_hierarchy=list(_DECISION_HIERARCHY),
            utility_unit=_UTILITY_UNIT,
            selection_method=_SELECTION_METHOD,
            timeframe_evaluations=[
                evaluation_by_timeframe[item] for item in TIMEFRAME_ORDER
            ],
            candidate_advisories=[_candidate_summary(item) for item in candidates],
            structural_allocations=allocations,
            eligible_candidate_ids=eligible,
            advisory_candidate_ids=advisory,
        )
    except _ContractError as exc:
        return _decision(
            status="REJECTED",
            reason_codes=[exc.code],
            request_sha256=request_sha,
        )
    except (MemoryError, RecursionError):
        return _decision(
            status="REJECTED",
            reason_codes=["BOUNDED_RESOURCE_FAILURE"],
            request_sha256=request_sha,
        )
    except Exception:
        return _decision(
            status="REJECTED",
            reason_codes=["UNEXPECTED_VALIDATION_FAILURE"],
            request_sha256=request_sha,
        )


def verify_arbitration_decision(value: Any, request: Any) -> bool:
    """Verify an advisory by exact deterministic request replay."""

    try:
        presented = (
            value.to_dict() if type(value) is ArbitrationDecision else _owned(value)
        )
        if type(presented) is not dict:
            return False
        receipt = presented.pop("receipt_sha256", None)
        if _required_sha(
            receipt, "DECISION_RECEIPT_INVALID"
        ) != canonical_receipt_sha256(presented):
            return False
        expected = rank_specialist_surfaces(request).to_dict()
        return (
            _owned(value.to_dict() if type(value) is ArbitrationDecision else value)
            == expected
        )
    except (Exception, MemoryError, RecursionError):
        return False


def arbitration_contract_snapshot() -> dict[str, Any]:
    """Return the fixed causal hierarchy and strict non-authority boundary."""

    return {
        "contract_version": ARBITRATION_CONTRACT_VERSION,
        "policy_version": ARBITRATION_POLICY_VERSION,
        "current_refit_receipt_version": CURRENT_REFIT_RECEIPT_VERSION,
        "accepted_book_generation_receipt_version": (
            ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION
        ),
        "signal_snapshot_receipt_version": SIGNAL_SNAPSHOT_RECEIPT_VERSION,
        "policy_receipt_type": POLICY_RECEIPT_TYPE,
        "signal_receipt_type": SIGNAL_RECEIPT_TYPE,
        "authority_scope": AUTHORITY_SCOPE,
        "timeframe_order": list(TIMEFRAME_ORDER),
        "regime_order": list(REGIME_ORDER),
        "structural_slots": ["D1_STRUCTURAL", "LOWER_STRUCTURAL"],
        "decision_hierarchy": list(_DECISION_HIERARCHY),
        "utility_unit": _UTILITY_UNIT,
        "selection_method": _SELECTION_METHOD,
        "policy_calibrated_thresholds_not_hardcoded": True,
        "historical_current_winner_return_tournament": False,
        "raw_regime_margin_or_dwell_ratio": False,
        "magic_utility_epsilon": None,
        "symmetric_signing_or_verification_keys": False,
        "capital_authority": False,
        "order_authority": False,
        "funded_live_authority": False,
        "promotion_authority": False,
        "profitability_claim": False,
    }


__all__ = [
    "ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION",
    "ARBITRATION_CONTRACT_VERSION",
    "ARBITRATION_POLICY_VERSION",
    "AUTHORITY_SCOPE",
    "ArbitrationDecision",
    "CURRENT_REFIT_RECEIPT_VERSION",
    "POLICY_RECEIPT_TYPE",
    "REGIME_ORDER",
    "REGIMES",
    "SIGNAL_RECEIPT_TYPE",
    "SIGNAL_SNAPSHOT_RECEIPT_VERSION",
    "TIMEFRAME_ORDER",
    "TIMEFRAMES",
    "arbitration_contract_snapshot",
    "rank_specialist_surfaces",
    "verify_arbitration_decision",
]
