"""Causal capital-envelope selection for synchronized FxPM replay evidence.

V8 can select numeric limits only from externally authenticated, precommitted
outer-anchor evidence. Legacy candidate lists and configuration defaults have
no authority and return a typed blocked artifact.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from itertools import islice
from pathlib import Path
from statistics import NormalDist
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


CAP_CALIBRATION_SCHEMA_VERSION = "fxpm.capital-envelope-authority.v8"
CAP_PRECOMMIT_SCHEMA_VERSION = "fxpm.capital-envelope-precommitment.v8"
CAP_REPLAY_SCHEMA_VERSION = "fxpm.capital-envelope-causal-replay-ledger.v8"
CAP_REPLAY_SHARD_SCHEMA_VERSION = "fxpm.capital-envelope-attempt-shard.v1"
CAP_SIGNED_ENVELOPE_SCHEMA_VERSION = "fxpm.capital-envelope-signed-envelope.v1"
CAP_AUTHORIZATION_SCHEMA_VERSION = "fxpm.capital-envelope-authorization.v2"
CAP_CALIBRATION_SEAL_ALGORITHM = "sha256-canonical-json-v1"
CURRENT_BOOK_ADAPTER_CONTRACT_VERSION = "fxpm.current-refit-book-adapter.v8"

BLOCKED_NO_NUMERIC_AUTHORITY = "BLOCKED_NO_NUMERIC_AUTHORITY"
EVIDENCE_SELECTED_PENDING_AUTHORIZATION = (
    "EVIDENCE_SELECTED_PENDING_EXTERNAL_AUTHORIZATION"
)
AUTHORIZED_NUMERIC_ENVELOPE = "AUTHORIZED_NUMERIC_ENVELOPE"
CAPITAL_AUTHORITY_SCOPE = BLOCKED_NO_NUMERIC_AUTHORITY

PRECOMMITMENT_SIGNATURE_KIND = "PRECOMMITMENT"
REPLAY_EVIDENCE_SIGNATURE_KIND = "REPLAY_EVIDENCE"
AUTHORIZATION_SIGNATURE_KIND = "AUTHORIZATION"
_SIGNATURE_DOMAINS = {
    PRECOMMITMENT_SIGNATURE_KIND: b"fxpm.capital-precommitment.v8\0",
    REPLAY_EVIDENCE_SIGNATURE_KIND: b"fxpm.capital-replay-evidence.v8\0",
    AUTHORIZATION_SIGNATURE_KIND: b"fxpm.capital-authorization.v2\0",
}

BLOCKING_REASONS = (
    "CAP_PRECOMMITMENT_MISSING",
    "CAP_REPLAY_EVIDENCE_MISSING",
    "CAP_CURRENT_LINEAGE_UNAVAILABLE",
)

METHOD_REFERENCES = {
    "hac": "https://doi.org/10.2307/1913610",
    "nested_selection": (
        "https://jmlr.csail.mit.edu/papers/v11/cawley10a.html"
    ),
    "multiple_testing": "https://doi.org/10.1198/016214504000000539",
    "mt5_margin": "https://www.mql5.com/en/book/automation/account/account_margin",
    "mt5_order_margin": "https://www.mql5.com/en/docs/trading/ordercalcmargin",
}

MAX_CANONICAL_BYTES = 64 * 1024 * 1024
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
MAX_LINEAGE_BYTES = 2 * 1024 * 1024
MAX_JSON_DEPTH = 32
MAX_JSON_NODES = 1_000_000
MAX_COLLECTION_ITEMS = 500_000
MAX_STRING_CHARS = 1_048_576
MAX_JSON_TRAVERSAL_WORK = 64 * 1024 * 1024
MAX_STARTING_BALANCES = 8
MAX_POLICY_FIELDS = 128
MAX_DAILY_OBSERVATIONS = 500_000
MAX_REPLAY_SHARD_BYTES = 8 * 1024 * 1024
MAX_REPLAY_SHARDS = 512
FLAT_MARGIN_LEVEL_PCT = 1_000_000.0

PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT = (
    3.0,
    5.0,
    8.0,
    10.0,
    12.0,
    20.0,
    33.25,
    50.0,
    75.0,
    90.0,
)

# These are the meaningful high-exposure points at which concentration limits
# must be an empirical Capital-replay choice, not an implicit portfolio veto.
# The roster builder derives one apples-to-apples factor/same-symbol-neutral
# counterpart for a concentration-limited point at each total.  Both policies
# are frozen before any replay outcome is available; the nested replay remains
# the only numeric selector.
PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT = (
    33.25,
    50.0,
    75.0,
    90.0,
)

_CALIBRATED_POLICY_FIELDS = frozenset(
    {
        "max_open_risk_pct",
        "max_total_stop_risk_pct",
        "max_factor_group_open_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_open_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "max_concurrent_positions",
        "min_margin_level_pct",
        "min_free_margin_pct",
    }
)

_POLICY_KEYS = frozenset(
    {
        "policy_id",
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
    }
)
_WINDOW_KEYS = frozenset({"start_date", "end_date"})
_ANCHOR_KEYS = frozenset({"anchor_id", "inner_window", "outer_window"})
_CONTRACT_LINEAGE_KEYS = frozenset(
    {
        "book_contract_version",
        "refit_contract_version",
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    }
)
_ATTEMPT_LINEAGE_KEYS = frozenset(
    {
        *_CONTRACT_LINEAGE_KEYS,
        "book_role",
        "book_decision_sha256",
        "book_generation_receipt_sha256",
        "refit_receipt_sha256",
        "refit_execution_sha256",
        "output_book_sha256",
        "winner_roster_sha256",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "native_activation_roster_sha256",
        "native_bars_sha256_by_source",
        "native_sidecar_sha256_by_source",
        "replay_output_sha256",
        "margin_terms_sha256",
        "broker_terms_sha256",
    }
)
_CURRENT_LINEAGE_KEYS = frozenset(
    _ATTEMPT_LINEAGE_KEYS - {"replay_output_sha256"}
)
_STABLE_PHASE_LINEAGE_KEYS = frozenset(
    _ATTEMPT_LINEAGE_KEYS
    - {
        "replay_output_sha256",
    }
)
_STABLE_ANCHOR_LINEAGE_KEYS = frozenset(
    _ATTEMPT_LINEAGE_KEYS
    - {
        "native_bars_sha256_by_source",
        "native_sidecar_sha256_by_source",
        "replay_output_sha256",
    }
)
_DAILY_KEYS = frozenset(
    {
        "date",
        "stress_net_return",
        "mtm_drawdown_pct",
        "initial_stop_drawdown_pct",
        "total_stop_risk_pct",
        "factor_stop_risk_pct",
        "same_symbol_stop_risk_pct",
        "joint_gap_loss_pct",
        "factor_gap_loss_pct",
        "margin_utilization_pct",
        "margin_level_pct",
        "free_margin_pct",
        "admission_events",
    }
)
_ATTEMPT_KEYS = frozenset(
    {
        "attempt_id",
        "anchor_id",
        "phase",
        "policy_id",
        "policy_sha256",
        "status",
        "failure_code",
        "window",
        "lineage",
        "daily_observations",
        "accepted_positions",
        "attempt_sha256",
    }
)
_PRECOMMIT_KEYS = frozenset(
    {
        "schema_version",
        "precommitment_id",
        "created_at_utc",
        "evidence_access_not_before_utc",
        "candidate_policies",
        "anchor_plan",
        "current_selection_window",
        "selection_rule",
        "constraints_sha256",
        "contract_lineage",
    }
)
_REPLAY_INLINE_KEYS = frozenset(
    {
        "schema_version",
        "precommitment_sha256",
        "precommitment_envelope_sha256",
        "generated_at_utc",
        "attempts",
    }
)
_REPLAY_SHARDED_KEYS = frozenset(
    {
        "schema_version",
        "precommitment_sha256",
        "precommitment_envelope_sha256",
        "generated_at_utc",
        "storage_mode",
        "attempt_count",
        "daily_observation_count",
        "attempt_shards",
    }
)
_SHARD_DESCRIPTOR_KEYS = frozenset(
    {
        "shard_index",
        "relative_path",
        "file_sha256",
        "byte_size",
        "attempt_count",
        "daily_observation_count",
        "first_attempt_id",
        "last_attempt_id",
    }
)
_SHARD_KEYS = frozenset({"schema_version", "shard_index", "attempts"})
_ENVELOPE_KEYS = frozenset(
    {"schema_version", "key_id", "body", "signature_base64"}
)
_AUTH_BODY_KEYS = frozenset(
    {
        "schema_version",
        "authority_key_id",
        "pending_artifact_content_sha256",
        "calibration_decision_sha256",
        "selected_policy_point_id",
        "selected_max_total_stop_risk_pct",
        "selected_capital_envelope",
        "source_lineage_sha256",
        "constraints_sha256",
        "issued_at_utc",
        "expires_at_utc",
    }
)

_FROZEN_SELECTION_RULE = {
    "schema_version": "fxpm.capital-envelope-selection-rule.v8",
    "geometry": "SIGNED_COMPLETE_POLICY_LEDGER_NESTED_BY_CHRONOLOGICAL_OUTER_ANCHOR",
    "inner_selection_data": "INNER_SELECTION_ONLY",
    "outer_evaluation_data": "SELECTED_POLICY_ONLY_AFTER_INNER_DECISION",
    "current_policy_data": "CURRENT_SELECTION_ONLY_NO_CURRENT_OUTCOME",
    "dependence": "BARTLETT_NEWEY_WEST_PLUS_OUTER_ANCHOR_CLUSTER_MAX_VARIANCE",
    "multiplicity": "BONFERRONI_FWER_BY_CANDIDATE_FOR_SELECTION_AND_BY_JOINT_GATE_FOR_OUTER_PROOF",
    "pareto_objectives": [
        ["stress_log_growth_lcb", "MAXIMIZE"],
        ["mtm_drawdown_ucb_pct", "MINIMIZE"],
        ["initial_stop_drawdown_ucb_pct", "MINIMIZE"],
        ["joint_gap_loss_ucb_pct", "MINIMIZE"],
        ["factor_gap_loss_ucb_pct", "MINIMIZE"],
        ["margin_utilization_ucb_pct", "MINIMIZE"],
    ],
    "frontier_decision": (
        "MAX_STRESS_LOG_GROWTH_LCB_THEN_MIN_JOINT_GAP_MTM_STOP_MARGIN_"
        "THEN_LOWER_TOTAL_FACTOR_SYMBOL_CAP_THEN_POLICY_ID"
    ),
    "path_constraints": (
        "JOINT_MTM_INITIAL_STOP_TOTAL_FACTOR_SYMBOL_GAP_AND_MARGIN"
    ),
    "cap_identification": (
        "EACH_NONREDUNDANT_STOP_RISK_AXIS_REQUIRES_PRECOMMITTED_NEAR_CAP_DAYS"
    ),
}

_JOINT_OUTER_GATE_COUNT = 8


class _Blocked(ValueError):
    def __init__(self, *reasons: str) -> None:
        self.reasons = tuple(str(reason) for reason in reasons if str(reason))
        super().__init__(";".join(self.reasons))


def _bounded_json(value: Any) -> None:
    stack = [(value, 0)]
    nodes = 0
    work = 0
    conservative_bytes = 0

    def account(amount: int) -> None:
        nonlocal conservative_bytes
        conservative_bytes += amount
        if conservative_bytes > MAX_CANONICAL_BYTES:
            raise ValueError("JSON payload exceeds its conservative byte bound")

    def account_string(text: str) -> None:
        nonlocal work
        if len(text) > MAX_STRING_CHARS:
            raise ValueError("JSON string exceeds its character bound")
        account(2)
        for character in text:
            work += 1
            if work > MAX_JSON_TRAVERSAL_WORK:
                raise ValueError("JSON payload exceeds its traversal-work bound")
            codepoint = ord(character)
            if 0xD800 <= codepoint <= 0xDFFF:
                raise ValueError("JSON strings cannot contain surrogate code points")
            if codepoint < 0x20:
                account(6)
            elif character in {'"', "\\"}:
                account(2)
            elif codepoint <= 0x7F:
                account(1)
            elif codepoint <= 0x7FF:
                account(2)
            elif codepoint <= 0xFFFF:
                account(3)
            else:
                account(4)

    while stack:
        item, depth = stack.pop()
        nodes += 1
        work += 1
        if nodes > MAX_JSON_NODES:
            raise ValueError("JSON payload exceeds its node bound")
        if work > MAX_JSON_TRAVERSAL_WORK:
            raise ValueError("JSON payload exceeds its traversal-work bound")
        if depth > MAX_JSON_DEPTH:
            raise ValueError("JSON payload exceeds its depth bound")
        if item is None:
            account(4)
        elif type(item) is bool:
            account(4 if item else 5)
        elif type(item) is str:
            account_string(item)
        elif type(item) is int:
            rendered = str(item)
            if len(rendered.lstrip("-")) > 128:
                raise ValueError("JSON integer exceeds its digit bound")
            account(len(rendered))
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("non-finite JSON numbers are forbidden")
            account(32)
        elif type(item) is dict:
            if len(item) > MAX_COLLECTION_ITEMS:
                raise ValueError("JSON mapping exceeds its item bound")
            account(2 + max(0, len(item) - 1))
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("JSON mapping keys must be strings")
                account_string(key)
                account(1)
                stack.append((child, depth + 1))
        elif type(item) in {list, tuple}:
            if len(item) > MAX_COLLECTION_ITEMS:
                raise ValueError("JSON sequence exceeds its item bound")
            account(2 + max(0, len(item) - 1))
            stack.extend((child, depth + 1) for child in item)
        else:
            raise ValueError(f"unsupported JSON value type: {type(item).__name__}")


def _canonical_json_bytes(value: Any) -> bytes:
    _bounded_json(value)
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("payload is not bounded canonical JSON") from exc
    if len(encoded) > MAX_CANONICAL_BYTES:
        raise ValueError("canonical JSON payload exceeds its byte bound")
    return encoded


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _json_copy(value: Any) -> Any:
    return json.loads(_canonical_json_bytes(value).decode("utf-8"))


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _finite(value: Any, name: str) -> float:
    if type(value) not in {int, float}:
        raise _Blocked(f"{name.upper()}_NOT_NUMERIC")
    result = float(value)
    if not math.isfinite(result):
        raise _Blocked(f"{name.upper()}_NOT_FINITE")
    return result


def _utc_datetime(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _date(value: Any, name: str) -> date:
    if type(value) is not str:
        raise _Blocked(f"{name.upper()}_NOT_ISO_DATE")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise _Blocked(f"{name.upper()}_NOT_ISO_DATE") from exc
    if parsed.isoformat() != value:
        raise _Blocked(f"{name.upper()}_NOT_CANONICAL")
    return parsed


def _window(value: Any, name: str) -> tuple[dict[str, str], date, date]:
    if type(value) is not dict or set(value) != _WINDOW_KEYS:
        raise _Blocked(f"{name.upper()}_SCHEMA_INVALID")
    start = _date(value["start_date"], f"{name}_start")
    end = _date(value["end_date"], f"{name}_end")
    if end <= start:
        raise _Blocked(f"{name.upper()}_EMPTY")
    return dict(value), start, end


def _bounded_lineage_copy(value: Any) -> Dict[str, Any]:
    if type(value) is not dict or len(value) > 256:
        raise ValueError("capital lineage must be a bounded JSON object")
    encoded = _canonical_json_bytes(value)
    if len(encoded) > MAX_LINEAGE_BYTES:
        raise ValueError("capital lineage exceeds its byte bound")
    copied = json.loads(encoded.decode("utf-8"))
    if type(copied) is not dict:
        raise ValueError("capital lineage must be a JSON object")
    return copied


@dataclass(frozen=True)
class CapCalibrationConstraints:
    minimum_accepted_positions: int = 100
    maximum_mtm_drawdown_pct: float = 35.0
    maximum_initial_stop_stress_drawdown_pct: float = 40.0
    minimum_cap_utilization_fraction: float = 0.80
    maximum_joint_gap_stress_loss_pct: float = 35.0
    maximum_factor_gap_stress_loss_pct: float = 15.0
    maximum_margin_utilization_pct: float = 90.0
    minimum_outer_anchors: int = 4
    maximum_outer_anchors: int = 16
    maximum_candidate_policies: int = 64
    minimum_exposure_effective_observations: float = 100.0
    minimum_near_cap_observations: int = 20
    minimum_paired_policy_observations: int = 4
    minimum_profitable_anchor_fraction: float = 0.75
    familywise_alpha: float = 0.05
    minimum_inner_calendar_days: int = 252
    minimum_outer_calendar_days: int = 252
    minimum_current_calendar_days: int = 252
    minimum_hac_effective_observations: float = 100.0
    hac_lag_days: int = 34
    minimum_margin_level_pct: float = 111.11111111111111
    minimum_free_margin_pct: float = 10.0
    minimum_stress_daily_log_growth: float = 0.0

    def validate(self) -> None:
        values = asdict(self)
        for name, value in values.items():
            if type(value) is bool or not isinstance(value, (int, float)):
                raise ValueError(f"constraint {name} must be numeric")
            if not math.isfinite(float(value)):
                raise ValueError(f"constraint {name} must be finite")
        integer_fields = (
            "minimum_accepted_positions",
            "minimum_outer_anchors",
            "maximum_outer_anchors",
            "maximum_candidate_policies",
            "minimum_near_cap_observations",
            "minimum_paired_policy_observations",
            "minimum_inner_calendar_days",
            "minimum_outer_calendar_days",
            "minimum_current_calendar_days",
            "hac_lag_days",
        )
        for name in integer_fields:
            if type(values[name]) is not int or not 0 <= values[name] <= 10_000_000:
                raise ValueError(f"constraint {name} is outside its integer bound")
        if not 2 <= self.minimum_outer_anchors <= self.maximum_outer_anchors <= 32:
            raise ValueError("outer-anchor bounds are invalid")
        if not 1 <= self.maximum_candidate_policies <= 128:
            raise ValueError("maximum_candidate_policies is invalid")
        if not 0.0 < self.familywise_alpha < 0.20:
            raise ValueError("familywise_alpha must be in (0,0.20)")
        if not 0.0 <= self.minimum_cap_utilization_fraction <= 1.0:
            raise ValueError("minimum_cap_utilization_fraction must be in [0,1]")
        if not 0.0 <= self.minimum_profitable_anchor_fraction <= 1.0:
            raise ValueError("minimum_profitable_anchor_fraction must be in [0,1]")
        if self.hac_lag_days < 1:
            raise ValueError("hac_lag_days must be positive")
        for name in (
            "maximum_mtm_drawdown_pct",
            "maximum_initial_stop_stress_drawdown_pct",
            "maximum_joint_gap_stress_loss_pct",
            "maximum_factor_gap_stress_loss_pct",
            "maximum_margin_utilization_pct",
            "minimum_free_margin_pct",
        ):
            if not 0.0 <= float(values[name]) <= 100.0:
                raise ValueError(f"constraint {name} must be in [0,100]")
        if self.minimum_margin_level_pct <= 100.0:
            raise ValueError("minimum_margin_level_pct must exceed 100")


def cap_neutral_config_sha256(config_payload: Mapping[str, Any]) -> str:
    if type(config_payload) is not dict:
        raise ValueError("config payload must be a bounded JSON object")
    neutral = _json_copy(config_payload)
    pipeline = neutral.get("pipeline")
    pipeline = pipeline if type(pipeline) is dict else {}
    pipeline["max_total_open_risk_pct"] = "SELECTED_BY_AUTHENTICATED_NESTED_PROCESS"
    pipeline["max_factor_group_open_risk_pct"] = (
        "SELECTED_BY_AUTHENTICATED_NESTED_PROCESS"
    )
    pipeline["max_combined_risk_pct"] = (
        "SELECTED_BY_AUTHENTICATED_NESTED_PROCESS"
    )
    for key in (
        "production_prelockbox_roster_path",
        "production_prelockbox_roster_sha256",
        "production_cap_calibration_path",
        "production_cap_calibration_sha256",
        "production_portfolio_replay_evidence_path",
        "production_promotion_authorization_path",
        "production_promotion_authorization_sha256",
    ):
        pipeline[key] = ""
    pipeline["winner_ledger_path"] = "RELEASED_LEDGER_POINTER"
    pipeline["manual_new_entries_enabled"] = "OPERATOR_RELEASE_CONTROL"
    neutral["pipeline"] = pipeline
    return canonical_json_sha256(neutral)


def _policy_payload(policy: Any) -> Dict[str, Any]:
    if is_dataclass(policy) and not isinstance(policy, type):
        dataclass_fields = fields(policy)
        if len(dataclass_fields) > MAX_POLICY_FIELDS:
            raise ValueError("policy dataclass exceeds its field bound")
        payload = {field.name: getattr(policy, field.name) for field in dataclass_fields}
    elif type(policy) is dict:
        if len(policy) > MAX_POLICY_FIELDS:
            raise ValueError("policy mapping exceeds its field bound")
        payload = dict(policy)
    else:
        raise TypeError("cap-neutral policies must be dataclasses or dictionaries")
    copied = _json_copy(payload)
    if type(copied) is not dict:
        raise ValueError("policy payload is invalid")
    return copied


def cap_neutral_policy_contract_sha256(policies: Mapping[float, Any]) -> str:
    if type(policies) is not dict or not 1 <= len(policies) <= MAX_STARTING_BALANCES:
        raise ValueError("starting-balance policy mapping is outside its bound")
    rows = []
    seen: set[float] = set()
    for raw_balance, policy in policies.items():
        if type(raw_balance) is bool:
            raise ValueError("starting balances must be numeric")
        balance = float(raw_balance)
        if not math.isfinite(balance) or balance <= 0.0 or balance in seen:
            raise ValueError("starting balances must be unique, finite and positive")
        seen.add(balance)
        payload = _policy_payload(policy)
        for field in _CALIBRATED_POLICY_FIELDS:
            if field in payload:
                payload[field] = f"CALIBRATED_AXIS:{field}"
        rows.append({"starting_balance": balance, "policy": payload})
    rows.sort(key=lambda row: row["starting_balance"])
    return canonical_json_sha256(rows)


def current_book_authority_adapter_contract() -> Dict[str, Any]:
    return {
        "schema_version": CURRENT_BOOK_ADAPTER_CONTRACT_VERSION,
        "implementation_status": "AVAILABLE_AT_SIGNED_V8_BOUNDARY",
        "numeric_cap_authority": True,
        "required_inputs": [
            "signed_precommitment",
            "signed_complete_attempt_ledger",
            "exact_current_book_and_refit_lineage",
            "exact_cost_native_replay_margin_lineage",
            "external_capital_authorization_for_live_use",
        ],
    }


def capital_authority_signature_payload(
    kind: str, key_id: str, body: Mapping[str, Any]
) -> bytes:
    if kind not in _SIGNATURE_DOMAINS:
        raise ValueError("capital signature kind is invalid")
    if type(key_id) is not str or not 1 <= len(key_id) <= 128:
        raise ValueError("capital signature key_id is invalid")
    canonical = _canonical_json_bytes(body)
    encoded_key = key_id.encode("utf-8")
    return (
        _SIGNATURE_DOMAINS[kind]
        + len(encoded_key).to_bytes(4, "big")
        + encoded_key
        + len(canonical).to_bytes(8, "big")
        + canonical
    )


def _verified_envelope(
    value: Any,
    *,
    kind: str,
    trusted_public_keys: Optional[Mapping[str, bytes]],
    missing_reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if value is None:
        raise _Blocked(missing_reason)
    if type(value) is not dict or set(value) != _ENVELOPE_KEYS:
        raise _Blocked(f"CAP_{kind}_ENVELOPE_SCHEMA_INVALID")
    if value["schema_version"] != CAP_SIGNED_ENVELOPE_SCHEMA_VERSION:
        raise _Blocked(f"CAP_{kind}_ENVELOPE_VERSION_INVALID")
    key_id = value["key_id"]
    if type(key_id) is not str or not key_id:
        raise _Blocked(f"CAP_{kind}_KEY_ID_INVALID")
    if not isinstance(trusted_public_keys, Mapping):
        raise _Blocked(f"CAP_{kind}_TRUST_ROOT_MISSING")
    public_bytes = trusted_public_keys.get(key_id)
    if type(public_bytes) is not bytes or len(public_bytes) != 32:
        raise _Blocked(f"CAP_{kind}_TRUSTED_KEY_MISSING")
    signature_text = value["signature_base64"]
    if type(signature_text) is not str or len(signature_text) > 128:
        raise _Blocked(f"CAP_{kind}_SIGNATURE_INVALID")
    try:
        signature = base64.b64decode(signature_text, validate=True)
        if len(signature) != 64:
            raise ValueError("signature length")
        Ed25519PublicKey.from_public_bytes(public_bytes).verify(
            signature,
            capital_authority_signature_payload(kind, key_id, value["body"]),
        )
    except (InvalidSignature, TypeError, ValueError) as exc:
        raise _Blocked(f"CAP_{kind}_SIGNATURE_INVALID") from exc
    body = _json_copy(value["body"])
    return body, {
        "authentication_status": "VERIFIED_ED25519",
        "key_id": key_id,
        "body_sha256": canonical_json_sha256(body),
        "envelope_sha256": canonical_json_sha256(value),
    }


def _policy(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _POLICY_KEYS:
        raise _Blocked("CAP_POLICY_SCHEMA_INVALID")
    policy_id = value["policy_id"]
    if type(policy_id) is not str or not 1 <= len(policy_id) <= 128:
        raise _Blocked("CAP_POLICY_ID_INVALID")
    result = {"policy_id": policy_id}
    for key in _POLICY_KEYS - {"policy_id"}:
        result[key] = _finite(value[key], f"cap_policy_{key}")
    total = result["max_total_stop_risk_pct"]
    factor = result["max_factor_stop_risk_pct"]
    symbol = result["max_same_symbol_stop_risk_pct"]
    margin = result["max_margin_utilization_pct"]
    if not 0.0 < symbol <= factor <= total < 100.0:
        raise _Blocked("CAP_POLICY_STOP_RISK_HIERARCHY_INVALID")
    if not 0.0 < margin < 100.0:
        raise _Blocked("CAP_POLICY_MARGIN_UTILIZATION_INVALID")
    if abs(result["min_margin_level_pct"] - 10_000.0 / margin) > 1e-7:
        raise _Blocked("CAP_POLICY_MARGIN_LEVEL_NOT_RECIPROCAL")
    if abs(result["min_free_margin_pct"] - (100.0 - margin)) > 1e-7:
        raise _Blocked("CAP_POLICY_FREE_MARGIN_NOT_RECIPROCAL")
    if result["gap_stress_multiple"] < 1.0:
        raise _Blocked("CAP_POLICY_GAP_MULTIPLE_INVALID")
    return result


def _quarter_percent_units(value: float) -> Optional[int]:
    numerator, denominator = float(value).as_integer_ratio()
    scaled_numerator = numerator * 4
    if scaled_numerator % denominator:
        return None
    units = scaled_numerator // denominator
    return units if 1 <= units <= 399 else None


def build_frozen_production_candidate_roster(
    policy_points: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build the bounded precommit roster without granting numeric authority.

    At each meaningful high-total point, one concentration-limited policy is
    paired with an otherwise identical policy whose factor and same-symbol
    stop-risk ceilings equal the total ceiling.  This prevents the candidate
    roster itself from silently imposing an ``uncorrelated winners only``
    portfolio.  The neutral point does not bypass total risk, margin, gap,
    drawdown, ruin, or positive-growth gates; genuine nested replay must still
    admit and select it.
    """

    point_keys = _POLICY_KEYS - {"policy_id"}
    bounded = list(islice(iter(policy_points), 65))
    if not bounded or len(bounded) > 64:
        raise ValueError("production capital candidate roster is outside its bound")

    normalized_axes: list[dict[str, Any]] = []
    seen_axes: set[str] = set()
    for raw in bounded:
        if type(raw) is not dict or set(raw) != point_keys:
            raise ValueError("production capital candidate point schema is invalid")
        axes = _json_copy(raw)
        axis_sha = canonical_json_sha256(axes)
        try:
            policy = _policy(
                {
                    "policy_id": f"capital-envelope-{axis_sha[:24]}",
                    **axes,
                }
            )
        except _Blocked as exc:
            raise ValueError(str(exc)) from exc
        for name in (
            "max_total_stop_risk_pct",
            "max_factor_stop_risk_pct",
            "max_same_symbol_stop_risk_pct",
        ):
            if _quarter_percent_units(policy[name]) is None:
                raise ValueError(
                    "production stop-risk candidates must align to the 0.25% live quantum"
                )
        normalized = {key: policy[key] for key in point_keys}
        normalized_sha = canonical_json_sha256(normalized)
        if normalized_sha in seen_axes:
            raise ValueError("production capital candidate point is duplicated")
        seen_axes.add(normalized_sha)
        normalized_axes.append(normalized)

    # Add no more than one matched neutral counterpart per meaningful total.
    # Choosing the smallest canonical axis hash is deterministic and
    # outcome-blind when callers provide several concentration-limited points.
    for total in PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT:
        limited = [
            axes
            for axes in normalized_axes
            if axes["max_total_stop_risk_pct"] == total
            and (
                axes["max_factor_stop_risk_pct"] < total
                or axes["max_same_symbol_stop_risk_pct"] < total
            )
        ]
        if not limited:
            raise ValueError(
                "production concentration-limited candidate coverage is incomplete: "
                f"{total:g}"
            )
        matched = False
        for axes in limited:
            counterpart = dict(axes)
            counterpart["max_factor_stop_risk_pct"] = total
            counterpart["max_same_symbol_stop_risk_pct"] = total
            if canonical_json_sha256(counterpart) in seen_axes:
                matched = True
                break
        if matched:
            continue
        template = min(limited, key=canonical_json_sha256)
        neutral = dict(template)
        neutral["max_factor_stop_risk_pct"] = total
        neutral["max_same_symbol_stop_risk_pct"] = total
        neutral_sha = canonical_json_sha256(neutral)
        if neutral_sha in seen_axes:  # pragma: no cover - guarded above
            continue
        normalized_axes.append(neutral)
        seen_axes.add(neutral_sha)

    if len(normalized_axes) > 64:
        raise ValueError(
            "production capital candidate roster exceeds its bound after "
            "matched neutral concentration coverage"
        )

    roster: list[dict[str, Any]] = []
    for axes in normalized_axes:
        axis_sha = canonical_json_sha256(axes)
        try:
            roster.append(
                _policy(
                    {
                        "policy_id": f"capital-envelope-{axis_sha[:24]}",
                        **axes,
                    }
                )
            )
        except _Blocked as exc:  # pragma: no cover - normalized above
            raise ValueError(str(exc)) from exc

    observed_totals = {policy["max_total_stop_risk_pct"] for policy in roster}
    missing = [
        value
        for value in PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
        if value not in observed_totals
    ]
    if missing:
        raise ValueError(
            "production total stop-risk candidate coverage is incomplete: "
            + ",".join(f"{value:g}" for value in missing)
        )
    roster.sort(key=lambda policy: policy["policy_id"])
    return _json_copy(roster)


def _validate_sha_map(value: Any, name: str) -> dict[str, str]:
    if type(value) is not dict or not value or len(value) > 10_000:
        raise _Blocked(f"{name.upper()}_INVALID")
    result: dict[str, str] = {}
    for key, digest in value.items():
        if type(key) is not str or not key or not _is_sha256(digest):
            raise _Blocked(f"{name.upper()}_INVALID")
        result[key] = digest
    return result


def _attempt_lineage(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ATTEMPT_LINEAGE_KEYS:
        raise _Blocked("CAP_ATTEMPT_LINEAGE_SCHEMA_INVALID")
    result = _json_copy(value)
    for key in _ATTEMPT_LINEAGE_KEYS:
        if key in {
            "book_contract_version",
            "refit_contract_version",
            "book_role",
            "native_activation_id",
            "native_bars_sha256_by_source",
            "native_sidecar_sha256_by_source",
        }:
            continue
        if not _is_sha256(result[key]):
            raise _Blocked(f"CAP_ATTEMPT_LINEAGE_{key.upper()}_INVALID")
    for key in (
        "book_contract_version",
        "refit_contract_version",
        "book_role",
        "native_activation_id",
    ):
        if type(result[key]) is not str or not result[key]:
            raise _Blocked(f"CAP_ATTEMPT_LINEAGE_{key.upper()}_INVALID")
    bars = _validate_sha_map(
        result["native_bars_sha256_by_source"], "cap_native_bars_lineage"
    )
    sidecars = _validate_sha_map(
        result["native_sidecar_sha256_by_source"],
        "cap_native_sidecar_lineage",
    )
    if set(bars) != set(sidecars):
        raise _Blocked("CAP_NATIVE_SOURCE_ROSTER_MISMATCH")
    result["native_bars_sha256_by_source"] = bars
    result["native_sidecar_sha256_by_source"] = sidecars
    return result


def _current_lineage(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _CURRENT_LINEAGE_KEYS:
        raise _Blocked("CAP_CURRENT_LINEAGE_SCHEMA_INVALID")
    probe = dict(value)
    probe["replay_output_sha256"] = "0" * 64
    validated = _attempt_lineage(probe)
    validated.pop("replay_output_sha256")
    return validated


def _precommitment(
    body: Any, constraints: CapCalibrationConstraints
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if type(body) is not dict or set(body) != _PRECOMMIT_KEYS:
        raise _Blocked("CAP_PRECOMMITMENT_SCHEMA_INVALID")
    if body["schema_version"] != CAP_PRECOMMIT_SCHEMA_VERSION:
        raise _Blocked("CAP_PRECOMMITMENT_VERSION_INVALID")
    if type(body["precommitment_id"]) is not str or not body["precommitment_id"]:
        raise _Blocked("CAP_PRECOMMITMENT_ID_INVALID")
    try:
        created = _utc_datetime(body["created_at_utc"], name="precommitment time")
        not_before = _utc_datetime(
            body["evidence_access_not_before_utc"], name="evidence access time"
        )
    except ValueError as exc:
        raise _Blocked("CAP_PRECOMMITMENT_TIME_INVALID") from exc
    if body["created_at_utc"] != _iso_utc(created) or body[
        "evidence_access_not_before_utc"
    ] != _iso_utc(not_before):
        raise _Blocked("CAP_PRECOMMITMENT_TIME_NOT_CANONICAL")
    if not_before <= created:
        raise _Blocked("CAP_PRECOMMITMENT_NOT_BEFORE_EVIDENCE_ACCESS")
    if body["selection_rule"] != _FROZEN_SELECTION_RULE:
        raise _Blocked("CAP_SELECTION_RULE_NOT_PRECOMMITTED_V8")
    constraints_sha = canonical_json_sha256(asdict(constraints))
    if body["constraints_sha256"] != constraints_sha:
        raise _Blocked("CAP_PRECOMMITMENT_CONSTRAINTS_MISMATCH")
    raw_contract = body["contract_lineage"]
    if type(raw_contract) is not dict or set(raw_contract) != _CONTRACT_LINEAGE_KEYS:
        raise _Blocked("CAP_PRECOMMITMENT_CONTRACT_LINEAGE_INVALID")
    contract = dict(raw_contract)
    for key, value in contract.items():
        if key in {"book_contract_version", "refit_contract_version"}:
            if type(value) is not str or not value:
                raise _Blocked("CAP_PRECOMMITMENT_CONTRACT_LINEAGE_INVALID")
        elif not _is_sha256(value):
            raise _Blocked("CAP_PRECOMMITMENT_CONTRACT_LINEAGE_INVALID")

    raw_policies = body["candidate_policies"]
    if type(raw_policies) is not list or not 2 <= len(raw_policies) <= (
        constraints.maximum_candidate_policies
    ):
        raise _Blocked("CAP_POLICY_ROSTER_SIZE_INVALID")
    policies: dict[str, dict[str, Any]] = {}
    axes: set[tuple[float, ...]] = set()
    for raw in raw_policies:
        item = _policy(raw)
        policy_id = item["policy_id"]
        axis = tuple(item[key] for key in sorted(_POLICY_KEYS - {"policy_id"}))
        if policy_id in policies or axis in axes:
            raise _Blocked("CAP_POLICY_ROSTER_DUPLICATE")
        policies[policy_id] = item
        axes.add(axis)
    if list(policies) != sorted(policies):
        raise _Blocked("CAP_POLICY_ROSTER_NOT_CANONICAL")

    raw_anchors = body["anchor_plan"]
    if type(raw_anchors) is not list or not (
        constraints.minimum_outer_anchors
        <= len(raw_anchors)
        <= constraints.maximum_outer_anchors
    ):
        raise _Blocked("CAP_OUTER_ANCHOR_COUNT_INVALID")
    anchors: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    previous_outer_end: Optional[date] = None
    for raw in raw_anchors:
        if type(raw) is not dict or set(raw) != _ANCHOR_KEYS:
            raise _Blocked("CAP_ANCHOR_PLAN_SCHEMA_INVALID")
        anchor_id = raw["anchor_id"]
        if type(anchor_id) is not str or not anchor_id or anchor_id in seen_ids:
            raise _Blocked("CAP_ANCHOR_ID_INVALID_OR_DUPLICATE")
        inner, inner_start, inner_end = _window(
            raw["inner_window"], f"cap_{anchor_id}_inner"
        )
        outer, outer_start, outer_end = _window(
            raw["outer_window"], f"cap_{anchor_id}_outer"
        )
        if inner_end > outer_start:
            raise _Blocked("CAP_PRECOMMITMENT_ANCHOR_WINDOW_LEAKAGE")
        if previous_outer_end is not None and outer_start < previous_outer_end:
            raise _Blocked("CAP_OUTER_ANCHORS_OVERLAP_OR_REORDER")
        if (inner_end - inner_start).days < constraints.minimum_inner_calendar_days:
            raise _Blocked("CAP_INNER_WINDOW_TOO_SHORT")
        if (outer_end - outer_start).days < constraints.minimum_outer_calendar_days:
            raise _Blocked("CAP_OUTER_WINDOW_TOO_SHORT")
        anchors.append(
            {
                "anchor_id": anchor_id,
                "inner_window": inner,
                "outer_window": outer,
            }
        )
        seen_ids.add(anchor_id)
        previous_outer_end = outer_end
    current, current_start, current_end = _window(
        body["current_selection_window"], "cap_current_selection"
    )
    if previous_outer_end is None or current_start < previous_outer_end:
        raise _Blocked("CAP_CURRENT_SELECTION_REUSES_OUTER_EVIDENCE")
    if (current_end - current_start).days < constraints.minimum_current_calendar_days:
        raise _Blocked("CAP_CURRENT_WINDOW_TOO_SHORT")
    return policies, anchors, {
        "created_at": created,
        "not_before": not_before,
        "current_window": current,
        "contract_lineage": contract,
    }


def _newey_west_mean(values: Sequence[float], lag: int) -> dict[str, float]:
    n = len(values)
    if n < 2:
        raise _Blocked("CAP_HAC_SAMPLE_TOO_SHORT")
    lag = min(lag, n - 1)
    mean = math.fsum(values) / n
    centered = [value - mean for value in values]
    gamma0 = math.fsum(value * value for value in centered) / n
    long_run = gamma0
    for offset in range(1, lag + 1):
        covariance = math.fsum(
            centered[index] * centered[index - offset]
            for index in range(offset, n)
        ) / n
        long_run += 2.0 * (1.0 - offset / (lag + 1.0)) * covariance
    long_run = max(0.0, long_run)
    variance_mean = long_run / n
    if long_run <= 1e-30:
        effective = float(n) if gamma0 <= 1e-30 else 1.0
    else:
        effective = min(float(n), max(1.0, n * gamma0 / long_run))
    return {
        "mean": mean,
        "standard_error": math.sqrt(max(0.0, variance_mean)),
        "long_run_variance": long_run,
        "effective_observations": effective,
    }


def _t_critical(df: float, alpha: float) -> float:
    if not 0.0 < alpha < 0.5:
        raise _Blocked("CAP_INFERENCE_ALPHA_INVALID")
    try:
        from scipy.stats import t as student_t

        value = float(student_t.ppf(1.0 - alpha, max(1.0, df)))
    except Exception:
        value = NormalDist().inv_cdf(1.0 - alpha)
    if not math.isfinite(value) or value <= 0.0:
        raise _Blocked("CAP_INFERENCE_CRITICAL_VALUE_INVALID")
    return value


def _sample_sd(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = math.fsum(values) / len(values)
    return math.sqrt(
        max(
            0.0,
            math.fsum((value - mean) ** 2 for value in values)
            / (len(values) - 1),
        )
    )


def _block_bound(
    values: Sequence[float], *, lag: int, alpha: float, upper: bool
) -> tuple[float, int]:
    width = lag + 1
    blocks = [values[index : index + width] for index in range(0, len(values), width)]
    blocks = [block for block in blocks if len(block) == width]
    if len(blocks) < 2:
        raise _Blocked("CAP_DEPENDENCE_BLOCK_COUNT_INSUFFICIENT")
    extrema = [max(block) if upper else min(block) for block in blocks]
    mean = math.fsum(extrema) / len(extrema)
    critical = _t_critical(len(extrema) - 1, alpha)
    radius = critical * _sample_sd(extrema) / math.sqrt(len(extrema))
    observed = max(values) if upper else min(values)
    bound = max(observed, mean + radius) if upper else min(observed, mean - radius)
    return bound, len(blocks)


def _attempt_summary(
    attempt: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    expected_window: Mapping[str, str],
    minimum_days: int,
    alpha: float,
    constraints: CapCalibrationConstraints,
) -> dict[str, Any]:
    if type(attempt) is not dict or set(attempt) != _ATTEMPT_KEYS:
        raise _Blocked("CAP_ATTEMPT_SCHEMA_INVALID")
    unsigned = dict(attempt)
    attempt_sha = unsigned.pop("attempt_sha256")
    if not _is_sha256(attempt_sha) or attempt_sha != canonical_json_sha256(unsigned):
        raise _Blocked("CAP_ATTEMPT_SHA256_INVALID")
    if attempt["status"] != "COMPLETE" or attempt["failure_code"] is not None:
        code = str(attempt.get("failure_code") or attempt.get("status") or "UNKNOWN")
        raise _Blocked(f"CAP_ATTEMPT_NOT_COMPLETE:{code}")
    if attempt["window"] != expected_window:
        raise _Blocked("CAP_ATTEMPT_WINDOW_MISMATCH")
    _, start, end = _window(attempt["window"], "cap_attempt_window")
    if (end - start).days < minimum_days:
        raise _Blocked("CAP_ATTEMPT_WINDOW_TOO_SHORT")
    if attempt["policy_sha256"] != canonical_json_sha256(policy):
        raise _Blocked("CAP_ATTEMPT_POLICY_SHA256_MISMATCH")
    if attempt["policy_id"] != policy["policy_id"]:
        raise _Blocked("CAP_ATTEMPT_POLICY_ID_MISMATCH")
    expected_attempt_id = (
        f"{attempt['anchor_id']}|{attempt['phase']}|{attempt['policy_id']}"
    )
    if attempt["attempt_id"] != expected_attempt_id:
        raise _Blocked("CAP_ATTEMPT_IDENTITY_INVALID")
    lineage = _attempt_lineage(attempt["lineage"])
    raw_daily = attempt["daily_observations"]
    if type(raw_daily) is not list or len(raw_daily) != (end - start).days:
        raise _Blocked("CAP_DAILY_CALENDAR_INCOMPLETE")
    if len(raw_daily) < minimum_days:
        raise _Blocked("CAP_DAILY_SAMPLE_TOO_SHORT")
    expected_dates = [date.fromordinal(start.toordinal() + index).isoformat() for index in range((end - start).days)]
    series: dict[str, list[float]] = {
        key: [] for key in _DAILY_KEYS - {"date", "admission_events"}
    }
    accepted = 0
    for index, row in enumerate(raw_daily):
        if type(row) is not dict or set(row) != _DAILY_KEYS:
            raise _Blocked("CAP_DAILY_OBSERVATION_SCHEMA_INVALID")
        if row["date"] != expected_dates[index]:
            raise _Blocked("CAP_DAILY_CALENDAR_REORDERED_OR_GAPPED")
        events = row["admission_events"]
        if type(events) is not int or type(events) is bool or events < 0:
            raise _Blocked("CAP_DAILY_ADMISSION_COUNT_INVALID")
        accepted += events
        for key in series:
            value = _finite(row[key], f"cap_daily_{key}")
            if key == "stress_net_return":
                if not -1.0 < value <= 10.0:
                    raise _Blocked("CAP_DAILY_RETURN_OUTSIDE_BOUND")
            elif value < 0.0:
                raise _Blocked(f"CAP_DAILY_{key.upper()}_NEGATIVE")
            series[key].append(value)
        total = series["total_stop_risk_pct"][-1]
        factor = series["factor_stop_risk_pct"][-1]
        symbol = series["same_symbol_stop_risk_pct"][-1]
        gap = series["joint_gap_loss_pct"][-1]
        factor_gap = series["factor_gap_loss_pct"][-1]
        margin = series["margin_utilization_pct"][-1]
        margin_level = series["margin_level_pct"][-1]
        free_margin = series["free_margin_pct"][-1]
        if factor > total + 1e-9 or symbol > factor + 1e-9:
            raise _Blocked("CAP_DAILY_STOP_RISK_HIERARCHY_INVALID")
        if factor_gap > gap + 1e-9:
            raise _Blocked("CAP_DAILY_GAP_HIERARCHY_INVALID")
        if total > policy["max_total_stop_risk_pct"] + 1e-9:
            raise _Blocked("CAP_REPLAY_EXCEEDS_TOTAL_POLICY")
        if factor > policy["max_factor_stop_risk_pct"] + 1e-9:
            raise _Blocked("CAP_REPLAY_EXCEEDS_FACTOR_POLICY")
        if symbol > policy["max_same_symbol_stop_risk_pct"] + 1e-9:
            raise _Blocked("CAP_REPLAY_EXCEEDS_SYMBOL_POLICY")
        if margin > policy["max_margin_utilization_pct"] + 1e-9:
            raise _Blocked("CAP_REPLAY_EXCEEDS_MARGIN_POLICY")
        if margin > 0.0:
            if abs(margin_level - 10_000.0 / margin) > 1e-6:
                raise _Blocked("CAP_DAILY_MARGIN_LEVEL_RECONCILIATION_FAILED")
            if abs(free_margin - (100.0 - margin)) > 1e-6:
                raise _Blocked("CAP_DAILY_FREE_MARGIN_RECONCILIATION_FAILED")
        elif (
            margin_level != FLAT_MARGIN_LEVEL_PCT
            or free_margin != 100.0
        ):
            raise _Blocked("CAP_DAILY_FLAT_MARGIN_RECONCILIATION_FAILED")
    if type(attempt["accepted_positions"]) is not int or attempt[
        "accepted_positions"
    ] != accepted:
        raise _Blocked("CAP_ACCEPTED_POSITION_COUNT_MISMATCH")

    log_returns = [math.log1p(value) for value in series["stress_net_return"]]
    hac = _newey_west_mean(log_returns, constraints.hac_lag_days)
    critical = _t_critical(max(1.0, hac["effective_observations"] - 1.0), alpha)
    lcb = hac["mean"] - critical * hac["standard_error"]
    risk_fields = {
        "mtm_drawdown_ucb_pct": "mtm_drawdown_pct",
        "initial_stop_drawdown_ucb_pct": "initial_stop_drawdown_pct",
        "joint_gap_loss_ucb_pct": "joint_gap_loss_pct",
        "factor_gap_loss_ucb_pct": "factor_gap_loss_pct",
        "margin_utilization_ucb_pct": "margin_utilization_pct",
    }
    bounds: dict[str, float] = {}
    block_counts: list[int] = []
    for output, source in risk_fields.items():
        bound, count = _block_bound(
            series[source], lag=constraints.hac_lag_days, alpha=alpha, upper=True
        )
        bounds[output] = min(100.0, max(0.0, bound))
        block_counts.append(count)
    margin_level_lcb, count = _block_bound(
        series["margin_level_pct"],
        lag=constraints.hac_lag_days,
        alpha=alpha,
        upper=False,
    )
    block_counts.append(count)
    free_margin_lcb, count = _block_bound(
        series["free_margin_pct"],
        lag=constraints.hac_lag_days,
        alpha=alpha,
        upper=False,
    )
    block_counts.append(count)
    fraction = constraints.minimum_cap_utilization_fraction
    near_counts = {
        "total": sum(
            value + 1e-12 >= fraction * policy["max_total_stop_risk_pct"]
            for value in series["total_stop_risk_pct"]
        ),
        "factor": sum(
            value + 1e-12 >= fraction * policy["max_factor_stop_risk_pct"]
            for value in series["factor_stop_risk_pct"]
        ),
        "same_symbol": sum(
            value + 1e-12 >= fraction * policy["max_same_symbol_stop_risk_pct"]
            for value in series["same_symbol_stop_risk_pct"]
        ),
    }
    return {
        "attempt_id": attempt["attempt_id"],
        "anchor_id": attempt["anchor_id"],
        "phase": attempt["phase"],
        "policy_id": policy["policy_id"],
        "days": len(raw_daily),
        "accepted_positions": accepted,
        "stress_log_growth_mean": hac["mean"],
        "stress_log_growth_lcb": lcb,
        "stress_log_growth_standard_error": hac["standard_error"],
        "hac_effective_observations": hac["effective_observations"],
        "dependence_block_count": min(block_counts),
        **bounds,
        "margin_level_lcb_pct": max(0.0, margin_level_lcb),
        "free_margin_lcb_pct": max(0.0, free_margin_lcb),
        "max_total_stop_risk_pct": max(series["total_stop_risk_pct"]),
        "max_factor_stop_risk_pct": max(series["factor_stop_risk_pct"]),
        "max_same_symbol_stop_risk_pct": max(
            series["same_symbol_stop_risk_pct"]
        ),
        "near_cap_observations": near_counts,
        "attempt_sha256": attempt_sha,
        "lineage": lineage,
        "log_returns": log_returns,
    }


def _safety_failures(
    summary: Mapping[str, Any],
    policy: Mapping[str, Any],
    constraints: CapCalibrationConstraints,
) -> list[str]:
    failures: list[str] = []
    if summary["accepted_positions"] < constraints.minimum_accepted_positions:
        failures.append("ACCEPTED_POSITIONS_INSUFFICIENT")
    if summary["hac_effective_observations"] < max(
        constraints.minimum_hac_effective_observations,
        constraints.minimum_exposure_effective_observations,
    ):
        failures.append("DEPENDENCE_EFFECTIVE_OBSERVATIONS_INSUFFICIENT")
    if summary["dependence_block_count"] < constraints.minimum_paired_policy_observations:
        failures.append("DEPENDENCE_BLOCKS_INSUFFICIENT")
    total_cap = policy["max_total_stop_risk_pct"]
    factor_cap = policy["max_factor_stop_risk_pct"]
    symbol_cap = policy["max_same_symbol_stop_risk_pct"]
    required_identification_axes = {"total"}
    if factor_cap < total_cap:
        required_identification_axes.add("factor")
    if symbol_cap < factor_cap:
        required_identification_axes.add("same_symbol")
    for axis, count in summary["near_cap_observations"].items():
        if axis not in required_identification_axes:
            continue
        if count < constraints.minimum_near_cap_observations:
            failures.append(f"{axis.upper()}_CAP_NOT_IDENTIFIED")
    gates = (
        ("mtm_drawdown_ucb_pct", constraints.maximum_mtm_drawdown_pct),
        (
            "initial_stop_drawdown_ucb_pct",
            constraints.maximum_initial_stop_stress_drawdown_pct,
        ),
        ("joint_gap_loss_ucb_pct", constraints.maximum_joint_gap_stress_loss_pct),
        ("factor_gap_loss_ucb_pct", constraints.maximum_factor_gap_stress_loss_pct),
        ("margin_utilization_ucb_pct", constraints.maximum_margin_utilization_pct),
    )
    for metric, limit in gates:
        if summary[metric] > limit + 1e-12:
            failures.append(f"{metric.upper()}_FAILED")
    if summary["margin_utilization_ucb_pct"] > policy[
        "max_margin_utilization_pct"
    ] + 1e-12:
        failures.append("POLICY_MARGIN_UTILIZATION_UCB_FAILED")
    if summary["margin_level_lcb_pct"] + 1e-12 < max(
        constraints.minimum_margin_level_pct,
        policy["min_margin_level_pct"],
    ):
        failures.append("MARGIN_LEVEL_LCB_FAILED")
    if summary["free_margin_lcb_pct"] + 1e-12 < max(
        constraints.minimum_free_margin_pct,
        policy["min_free_margin_pct"],
    ):
        failures.append("FREE_MARGIN_LCB_FAILED")
    return failures


def _selection_candidates(
    summaries: Sequence[Mapping[str, Any]],
    policies: Mapping[str, Mapping[str, Any]],
    constraints: CapCalibrationConstraints,
) -> tuple[list[dict[str, Any]], list[str]]:
    admissible: list[dict[str, Any]] = []
    failures: list[str] = []
    for summary in summaries:
        policy = policies[summary["policy_id"]]
        local = _safety_failures(summary, policy, constraints)
        if summary["stress_log_growth_lcb"] <= (
            constraints.minimum_stress_daily_log_growth
        ):
            local.append("STRESS_LOG_GROWTH_LCB_NOT_POSITIVE")
        if local:
            failures.extend(f"{policy['policy_id']}:{reason}" for reason in local)
        else:
            admissible.append(dict(summary))
    return admissible, failures


_PARETO_METRICS = (
    ("stress_log_growth_lcb", 1),
    ("mtm_drawdown_ucb_pct", -1),
    ("initial_stop_drawdown_ucb_pct", -1),
    ("joint_gap_loss_ucb_pct", -1),
    ("factor_gap_loss_ucb_pct", -1),
    ("margin_utilization_ucb_pct", -1),
)


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    weak = True
    strict = False
    for metric, direction in _PARETO_METRICS:
        left_value = direction * float(left[metric])
        right_value = direction * float(right[metric])
        weak = weak and left_value >= right_value - 1e-15
        strict = strict or left_value > right_value + 1e-15
    return weak and strict


def _pareto_select(
    admissible: Sequence[Mapping[str, Any]],
    policies: Mapping[str, Mapping[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    if not admissible:
        raise _Blocked("CAP_NO_ADMISSIBLE_POLICY")
    frontier = [
        dict(candidate)
        for candidate in admissible
        if not any(
            _dominates(other, candidate)
            for other in admissible
            if other["policy_id"] != candidate["policy_id"]
        )
    ]

    def key(summary: Mapping[str, Any]) -> tuple[Any, ...]:
        policy = policies[summary["policy_id"]]
        return (
            -summary["stress_log_growth_lcb"],
            summary["joint_gap_loss_ucb_pct"],
            summary["mtm_drawdown_ucb_pct"],
            summary["initial_stop_drawdown_ucb_pct"],
            summary["margin_utilization_ucb_pct"],
            policy["max_total_stop_risk_pct"],
            policy["max_factor_stop_risk_pct"],
            policy["max_same_symbol_stop_risk_pct"],
            policy["policy_id"],
        )

    selected = min(frontier, key=key)
    return sorted(item["policy_id"] for item in frontier), selected


def _cluster_bound(values: Sequence[float], alpha: float, upper: bool) -> float:
    if len(values) < 2:
        raise _Blocked("CAP_OUTER_CLUSTER_COUNT_INSUFFICIENT")
    mean = math.fsum(values) / len(values)
    radius = _t_critical(len(values) - 1, alpha) * _sample_sd(values) / math.sqrt(
        len(values)
    )
    observed = max(values) if upper else min(values)
    return max(observed, mean + radius) if upper else min(observed, mean - radius)


def _outer_process_summary(
    selected: Sequence[Mapping[str, Any]], constraints: CapCalibrationConstraints
) -> tuple[dict[str, Any], list[str]]:
    alpha = constraints.familywise_alpha / _JOINT_OUTER_GATE_COUNT
    total_days = sum(int(item["days"]) for item in selected)
    weighted_mean = math.fsum(
        item["stress_log_growth_mean"] * item["days"] for item in selected
    ) / total_days
    hac_variance = math.fsum(
        (item["days"] / total_days) ** 2
        * item["stress_log_growth_standard_error"] ** 2
        for item in selected
    )
    anchor_means = [item["stress_log_growth_mean"] for item in selected]
    cluster_variance = _sample_sd(anchor_means) ** 2 / len(anchor_means)
    standard_error = math.sqrt(max(hac_variance, cluster_variance))
    lcb = weighted_mean - _t_critical(len(selected) - 1, alpha) * standard_error
    upper_metrics = (
        "mtm_drawdown_ucb_pct",
        "initial_stop_drawdown_ucb_pct",
        "joint_gap_loss_ucb_pct",
        "factor_gap_loss_ucb_pct",
        "margin_utilization_ucb_pct",
    )
    bounds = {
        metric: min(
            100.0,
            max(
                0.0,
                _cluster_bound([item[metric] for item in selected], alpha, True),
            ),
        )
        for metric in upper_metrics
    }
    margin_level = max(
        0.0,
        _cluster_bound(
            [item["margin_level_lcb_pct"] for item in selected], alpha, False
        ),
    )
    free_margin = max(
        0.0,
        _cluster_bound(
            [item["free_margin_lcb_pct"] for item in selected], alpha, False
        ),
    )
    profitable_fraction = sum(value > 0.0 for value in anchor_means) / len(anchor_means)
    summary = {
        "outer_anchor_count": len(selected),
        "total_calendar_days": total_days,
        "stress_log_growth_mean": weighted_mean,
        "stress_log_growth_lcb": lcb,
        "stress_log_growth_standard_error": standard_error,
        "profitable_anchor_fraction": profitable_fraction,
        "newest_anchor_stress_log_growth_mean": anchor_means[-1],
        "margin_level_lcb_pct": margin_level,
        "free_margin_lcb_pct": free_margin,
        "multiplicity_alpha": alpha,
        **bounds,
    }
    failures: list[str] = []
    if lcb <= constraints.minimum_stress_daily_log_growth:
        failures.append("CAP_OUTER_STRESS_LOG_GROWTH_LCB_FAILED")
    if anchor_means[-1] <= constraints.minimum_stress_daily_log_growth:
        failures.append("CAP_NEWEST_OUTER_ANCHOR_NOT_PROFITABLE")
    if profitable_fraction < constraints.minimum_profitable_anchor_fraction:
        failures.append("CAP_PROFITABLE_OUTER_ANCHOR_FRACTION_FAILED")
    gates = (
        ("mtm_drawdown_ucb_pct", constraints.maximum_mtm_drawdown_pct),
        (
            "initial_stop_drawdown_ucb_pct",
            constraints.maximum_initial_stop_stress_drawdown_pct,
        ),
        ("joint_gap_loss_ucb_pct", constraints.maximum_joint_gap_stress_loss_pct),
        ("factor_gap_loss_ucb_pct", constraints.maximum_factor_gap_stress_loss_pct),
        ("margin_utilization_ucb_pct", constraints.maximum_margin_utilization_pct),
    )
    for metric, limit in gates:
        if summary[metric] > limit + 1e-12:
            failures.append(f"CAP_OUTER_{metric.upper()}_FAILED")
    if margin_level + 1e-12 < constraints.minimum_margin_level_pct:
        failures.append("CAP_OUTER_MARGIN_LEVEL_LCB_FAILED")
    if free_margin + 1e-12 < constraints.minimum_free_margin_pct:
        failures.append("CAP_OUTER_FREE_MARGIN_LCB_FAILED")
    return summary, failures


def _compact_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in summary.items()
        if key not in {"log_returns", "lineage"}
    }


def _decision_material(artifact: Mapping[str, Any]) -> dict[str, Any]:
    selection = dict(artifact["selection"])
    if selection["status"] == AUTHORIZED_NUMERIC_ENVELOPE:
        selection["status"] = EVIDENCE_SELECTED_PENDING_AUTHORIZATION
    return {
        "schema_version": artifact["schema_version"],
        "authority_scope": EVIDENCE_SELECTED_PENDING_AUTHORIZATION,
        "profitability_guarantee": artifact["profitability_guarantee"],
        "places_trades": artifact["places_trades"],
        "content_seal_is_authority": artifact["content_seal_is_authority"],
        "window": artifact["window"],
        "evidence_receipts": artifact["evidence_receipts"],
        "source_lineage": artifact["source_lineage"],
        "constraints": artifact["constraints"],
        "constraints_sha256": artifact["constraints_sha256"],
        "authority_adapter": artifact["authority_adapter"],
        "method_contract": artifact["method_contract"],
        "method_references": artifact["method_references"],
        "policy_records": artifact["policy_records"],
        "cap_records": artifact["cap_records"],
        "frontier": artifact["frontier"],
        "outer_process": artifact["outer_process"],
        "measured_edge_freshness": artifact["measured_edge_freshness"],
        "selection": selection,
        "limitations": artifact["limitations"],
    }


_ARTIFACT_KEYS = frozenset(
    {
        "schema_version",
        "authority_scope",
        "generated_at_utc",
        "profitability_guarantee",
        "places_trades",
        "content_seal_is_authority",
        "window",
        "source_lineage",
        "constraints",
        "constraints_sha256",
        "authority_adapter",
        "method_contract",
        "method_references",
        "evidence_receipts",
        "policy_records",
        "cap_records",
        "frontier",
        "outer_process",
        "measured_edge_freshness",
        "selection",
        "decision_sha256",
        "external_authorization",
        "limitations",
        "content_seal",
    }
)


def _seal_artifact(body: Mapping[str, Any]) -> dict[str, Any]:
    result = _json_copy(body)
    result.pop("content_seal", None)
    result["content_seal"] = {
        "algorithm": CAP_CALIBRATION_SEAL_ALGORITHM,
        "sha256": canonical_json_sha256(result),
    }
    if len(_canonical_json_bytes(result)) > MAX_ARTIFACT_BYTES:
        raise ValueError("capital artifact exceeds its byte bound")
    return result


def _artifact_seal_valid(artifact: Any) -> bool:
    if type(artifact) is not dict or set(artifact) != _ARTIFACT_KEYS:
        return False
    try:
        if len(_canonical_json_bytes(artifact)) > MAX_ARTIFACT_BYTES:
            return False
        seal = artifact["content_seal"]
        if type(seal) is not dict or set(seal) != {"algorithm", "sha256"}:
            return False
        if seal["algorithm"] != CAP_CALIBRATION_SEAL_ALGORITHM:
            return False
        unsigned = dict(artifact)
        unsigned.pop("content_seal")
        return _is_sha256(seal["sha256"]) and seal["sha256"] == canonical_json_sha256(
            unsigned
        )
    except (TypeError, ValueError, KeyError, RecursionError):
        return False


def _base_artifact(
    *,
    start: datetime,
    end: datetime,
    lineage: Mapping[str, Any],
    constraints: CapCalibrationConstraints,
    status: str,
    reasons: Sequence[str],
    evidence_receipts: Optional[Mapping[str, Any]] = None,
    policy_records: Optional[Sequence[Mapping[str, Any]]] = None,
    cap_records: Optional[Sequence[Mapping[str, Any]]] = None,
    frontier: Optional[Sequence[str]] = None,
    outer_process: Optional[Mapping[str, Any]] = None,
    selection_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    numeric = status in {
        EVIDENCE_SELECTED_PENDING_AUTHORIZATION,
        AUTHORIZED_NUMERIC_ENVELOPE,
    }
    selection = {
        "status": status,
        "selected_policy_point_id": None,
        "selected_max_total_stop_risk_pct": None,
        "selected_capital_envelope": None,
        "admissible_policy_point_ids": [],
        "pareto_frontier": [],
        "identification_failures": list(dict.fromkeys(reasons)),
        "authorization_required": True,
    }
    if selection_payload is not None:
        selection.update(_json_copy(selection_payload))
    body: dict[str, Any] = {
        "schema_version": CAP_CALIBRATION_SCHEMA_VERSION,
        "authority_scope": status,
        "generated_at_utc": _iso_utc(datetime.now(timezone.utc)),
        "profitability_guarantee": False,
        "places_trades": False,
        "content_seal_is_authority": False,
        "window": {
            "evaluation_start": _iso_utc(start),
            "evaluation_end": _iso_utc(end),
            "calendar_days": (end - start).days,
        },
        "source_lineage": _bounded_lineage_copy(lineage),
        "constraints": asdict(constraints),
        "constraints_sha256": canonical_json_sha256(asdict(constraints)),
        "authority_adapter": current_book_authority_adapter_contract(),
        "method_contract": _json_copy(_FROZEN_SELECTION_RULE),
        "method_references": dict(METHOD_REFERENCES),
        "evidence_receipts": _json_copy(evidence_receipts or {}),
        "policy_records": _json_copy(list(policy_records or [])),
        "cap_records": _json_copy(list(cap_records or [])),
        "frontier": list(frontier or []),
        "outer_process": _json_copy(outer_process) if outer_process else None,
        "measured_edge_freshness": (
            None
            if not outer_process
            else {
                "newest_anchor_stress_log_growth_mean": outer_process.get(
                    "newest_anchor_stress_log_growth_mean"
                ),
                "outer_anchor_count": outer_process.get("outer_anchor_count"),
            }
        ),
        "selection": selection,
        "decision_sha256": None,
        "external_authorization": None,
        "limitations": [
            "historical evidence does not guarantee future profitability",
            "numeric evidence selection requires external authorization for live use",
            "margin authority is exact only for the bound broker terms and replay model",
        ],
    }
    if numeric:
        body["decision_sha256"] = canonical_json_sha256(_decision_material(body))
    return _seal_artifact(body)


def _read_regular_file_bounded(path: Path, maximum_bytes: int) -> bytes:
    if _is_link_or_reparse(path):
        raise _Blocked("CAP_REPLAY_SHARD_PATH_UNSAFE")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    chunks: list[bytes] = []
    try:
        before = os.fstat(descriptor)
        if before.st_nlink != 1 or not 1 <= before.st_size <= maximum_bytes:
            raise _Blocked("CAP_REPLAY_SHARD_SIZE_INVALID")
        remaining = before.st_size
        while remaining:
            block = os.read(descriptor, min(1024 * 1024, remaining))
            if not block:
                raise _Blocked("CAP_REPLAY_SHARD_TRUNCATED")
            chunks.append(block)
            remaining -= len(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        or (before.st_dev, before.st_ino) != (current.st_dev, current.st_ino)
        or current.st_nlink != 1
    ):
        raise _Blocked("CAP_REPLAY_SHARD_CHANGED_DURING_READ")
    return b"".join(chunks)


def _materialize_attempt_shards(
    replay_body: Mapping[str, Any], replay_root: Optional[Path]
) -> list[dict[str, Any]]:
    if replay_body.get("storage_mode") != "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1":
        raise _Blocked("CAP_REPLAY_STORAGE_MODE_INVALID")
    if replay_root is None:
        raise _Blocked("CAP_REPLAY_SHARD_ROOT_MISSING")
    raw_root = Path(replay_root)
    if _is_link_or_reparse(raw_root):
        raise _Blocked("CAP_REPLAY_SHARD_ROOT_UNSAFE")
    root = raw_root.resolve(strict=True)
    if not root.is_dir() or _is_link_or_reparse(root):
        raise _Blocked("CAP_REPLAY_SHARD_ROOT_UNSAFE")
    descriptors = replay_body.get("attempt_shards")
    if type(descriptors) is not list or not 1 <= len(descriptors) <= MAX_REPLAY_SHARDS:
        raise _Blocked("CAP_REPLAY_SHARD_ROSTER_INVALID")
    attempts: list[dict[str, Any]] = []
    total_daily = 0
    for index, descriptor in enumerate(descriptors):
        if type(descriptor) is not dict or set(descriptor) != _SHARD_DESCRIPTOR_KEYS:
            raise _Blocked("CAP_REPLAY_SHARD_DESCRIPTOR_INVALID")
        if descriptor["shard_index"] != index:
            raise _Blocked("CAP_REPLAY_SHARD_INDEX_INVALID")
        digest = descriptor["file_sha256"]
        relative = descriptor["relative_path"]
        expected_relative = f"attempt_shards/attempts_{index:04d}_{digest}.json"
        if not _is_sha256(digest) or relative != expected_relative:
            raise _Blocked("CAP_REPLAY_SHARD_PATH_OR_HASH_INVALID")
        path = root.joinpath(*relative.split("/"))
        if _is_link_or_reparse(path) or _is_link_or_reparse(path.parent):
            raise _Blocked("CAP_REPLAY_SHARD_PATH_UNSAFE")
        resolved = path.resolve(strict=True)
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise _Blocked("CAP_REPLAY_SHARD_ESCAPES_ROOT") from exc
        cursor = resolved.parent
        while cursor != root:
            if _is_link_or_reparse(cursor):
                raise _Blocked("CAP_REPLAY_SHARD_PATH_UNSAFE")
            cursor = cursor.parent
        raw = _read_regular_file_bounded(resolved, MAX_REPLAY_SHARD_BYTES)
        if descriptor["byte_size"] != len(raw):
            raise _Blocked("CAP_REPLAY_SHARD_BYTE_SIZE_MISMATCH")
        if hashlib.sha256(raw).hexdigest() != digest:
            raise _Blocked("CAP_REPLAY_SHARD_SHA256_MISMATCH")
        try:
            shard = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _Blocked("CAP_REPLAY_SHARD_JSON_INVALID") from exc
        if type(shard) is not dict or set(shard) != _SHARD_KEYS:
            raise _Blocked("CAP_REPLAY_SHARD_SCHEMA_INVALID")
        if (
            shard["schema_version"] != CAP_REPLAY_SHARD_SCHEMA_VERSION
            or shard["shard_index"] != index
        ):
            raise _Blocked("CAP_REPLAY_SHARD_VERSION_OR_INDEX_INVALID")
        if raw != _canonical_json_bytes(shard) + b"\n":
            raise _Blocked("CAP_REPLAY_SHARD_NOT_CANONICAL")
        shard_attempts = shard["attempts"]
        if type(shard_attempts) is not list or not shard_attempts:
            raise _Blocked("CAP_REPLAY_SHARD_ATTEMPTS_INVALID")
        daily_count = sum(
            len(item.get("daily_observations", []))
            for item in shard_attempts
            if type(item) is dict and type(item.get("daily_observations")) is list
        )
        if (
            descriptor["attempt_count"] != len(shard_attempts)
            or descriptor["daily_observation_count"] != daily_count
            or descriptor["first_attempt_id"]
            != shard_attempts[0].get("attempt_id")
            or descriptor["last_attempt_id"]
            != shard_attempts[-1].get("attempt_id")
        ):
            raise _Blocked("CAP_REPLAY_SHARD_DESCRIPTOR_CONTENT_MISMATCH")
        attempts.extend(shard_attempts)
        total_daily += daily_count
        if total_daily > MAX_DAILY_OBSERVATIONS:
            raise _Blocked("CAP_ATTEMPT_LEDGER_OBSERVATION_BOUND_EXCEEDED")
    if (
        replay_body.get("attempt_count") != len(attempts)
        or replay_body.get("daily_observation_count") != total_daily
    ):
        raise _Blocked("CAP_REPLAY_SHARD_TOTALS_MISMATCH")
    return attempts


def write_capital_replay_attempt_shards(
    directory: Path,
    attempts: Iterable[Mapping[str, Any]],
    *,
    shard_attempt_limit: int = 8,
) -> dict[str, Any]:
    """Write immutable replay shards and return signed-body descriptors."""

    if type(shard_attempt_limit) is not int or not 1 <= shard_attempt_limit <= 128:
        raise ValueError("shard_attempt_limit is outside its bound")
    root = Path(directory)
    shard_dir = root / "attempt_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or not shard_dir.is_dir():
        raise ValueError("capital replay shard directory is invalid")
    if _is_link_or_reparse(root) or _is_link_or_reparse(shard_dir):
        raise ValueError("capital replay shard directory is unsafe")
    descriptors: list[dict[str, Any]] = []
    chunk: list[dict[str, Any]] = []
    chunk_payload_bytes = 0
    total_attempts = 0
    total_daily = 0

    def flush() -> None:
        nonlocal total_attempts, total_daily, chunk, chunk_payload_bytes
        if not chunk:
            return
        index = len(descriptors)
        if index >= MAX_REPLAY_SHARDS:
            raise ValueError("capital replay shard count exceeds its bound")
        shard = {
            "schema_version": CAP_REPLAY_SHARD_SCHEMA_VERSION,
            "shard_index": index,
            "attempts": chunk,
        }
        encoded = _canonical_json_bytes(shard) + b"\n"
        if len(encoded) > MAX_REPLAY_SHARD_BYTES:
            raise ValueError("one capital replay shard exceeds its byte bound")
        digest = hashlib.sha256(encoded).hexdigest()
        path = shard_dir / f"attempts_{index:04d}_{digest}.json"
        if path.exists():
            if _read_regular_file_bounded(path, MAX_REPLAY_SHARD_BYTES) != encoded:
                raise ValueError("immutable replay shard contains different bytes")
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
        daily = sum(len(item.get("daily_observations", [])) for item in chunk)
        descriptors.append(
            {
                "shard_index": index,
                "relative_path": f"attempt_shards/{path.name}",
                "file_sha256": digest,
                "byte_size": len(encoded),
                "attempt_count": len(chunk),
                "daily_observation_count": daily,
                "first_attempt_id": chunk[0].get("attempt_id"),
                "last_attempt_id": chunk[-1].get("attempt_id"),
            }
        )
        total_attempts += len(chunk)
        total_daily += daily
        if total_daily > MAX_DAILY_OBSERVATIONS:
            raise ValueError("capital replay observations exceed their global bound")
        chunk = []
        chunk_payload_bytes = 0

    for raw in attempts:
        copied = _json_copy(raw)
        if type(copied) is not dict:
            raise ValueError("capital replay attempts must be mappings")
        payload_bytes = len(_canonical_json_bytes(copied))
        index = len(descriptors)
        empty_size = len(
            _canonical_json_bytes(
                {
                    "schema_version": CAP_REPLAY_SHARD_SCHEMA_VERSION,
                    "shard_index": index,
                    "attempts": [],
                }
            )
        )
        prospective_size = (
            empty_size
            + chunk_payload_bytes
            + payload_bytes
            + len(chunk)
            + 1
        )
        if chunk and (
            len(chunk) >= shard_attempt_limit
            or prospective_size > MAX_REPLAY_SHARD_BYTES
        ):
            flush()
            index = len(descriptors)
            empty_size = len(
                _canonical_json_bytes(
                    {
                        "schema_version": CAP_REPLAY_SHARD_SCHEMA_VERSION,
                        "shard_index": index,
                        "attempts": [],
                    }
                )
            )
            prospective_size = empty_size + payload_bytes + 1
        if prospective_size > MAX_REPLAY_SHARD_BYTES:
            raise ValueError("one capital replay attempt exceeds the shard byte bound")
        chunk.append(copied)
        chunk_payload_bytes += payload_bytes
    flush()
    if total_attempts < 1:
        raise ValueError("capital replay attempt roster is empty")
    return {
        "storage_mode": "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1",
        "attempt_count": total_attempts,
        "daily_observation_count": total_daily,
        "attempt_shards": descriptors,
    }


def calibrate_total_stop_risk_cap(
    *,
    candidates: Iterable[Any],
    base_policies: Mapping[float, Any],
    evaluation_start: Any,
    evaluation_end: Any,
    lineage: Mapping[str, Any],
    constraints: CapCalibrationConstraints,
    correlation_lookup: Any = None,
    replay_evidence_root: Optional[Path] = None,
    replay_evidence_receipt_relative_path: Optional[str] = None,
    trusted_replay_public_keys: Optional[Mapping[str, bytes]] = None,
    evidence_validation_time_utc: Any = None,
    precommitment_receipt: Optional[Mapping[str, Any]] = None,
    replay_evidence_receipt: Optional[Mapping[str, Any]] = None,
    trusted_precommitment_public_keys: Optional[Mapping[str, bytes]] = None,
) -> Dict[str, Any]:
    """Select a pending envelope or return a typed blocked artifact."""

    del (
        candidates,
        base_policies,
        correlation_lookup,
        replay_evidence_receipt_relative_path,
        evidence_validation_time_utc,
    )
    if type(constraints) is not CapCalibrationConstraints:
        raise ValueError("exact CapCalibrationConstraints are required")
    constraints.validate()
    start = _utc_datetime(evaluation_start, name="evaluation start")
    end = _utc_datetime(evaluation_end, name="evaluation end")
    if end <= start:
        raise ValueError("capital evaluation window is invalid")
    current_lineage = _bounded_lineage_copy(lineage)
    reasons: list[str] = []
    precommit_meta: Optional[dict[str, Any]] = None
    replay_meta: Optional[dict[str, Any]] = None
    try:
        precommit_body, precommit_meta = _verified_envelope(
            precommitment_receipt,
            kind=PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys=trusted_precommitment_public_keys,
            missing_reason="CAP_PRECOMMITMENT_MISSING",
        )
        policies, anchors, precommit = _precommitment(precommit_body, constraints)
        expected_start = _date(
            anchors[0]["inner_window"]["start_date"], "cap_evaluation_start"
        )
        expected_end = _date(
            precommit["current_window"]["end_date"], "cap_evaluation_end"
        )
        if (
            start != datetime.combine(expected_start, datetime.min.time(), timezone.utc)
            or end
            != datetime.combine(expected_end, datetime.min.time(), timezone.utc)
        ):
            raise _Blocked("CAP_EVALUATION_WINDOW_NOT_EXACT_PROCESS_WINDOW")
        replay_body, replay_meta = _verified_envelope(
            replay_evidence_receipt,
            kind=REPLAY_EVIDENCE_SIGNATURE_KIND,
            trusted_public_keys=trusted_replay_public_keys,
            missing_reason="CAP_REPLAY_EVIDENCE_MISSING",
        )
        if type(replay_body) is not dict or set(replay_body) not in {
            _REPLAY_INLINE_KEYS,
            _REPLAY_SHARDED_KEYS,
        }:
            raise _Blocked("CAP_REPLAY_EVIDENCE_SCHEMA_INVALID")
        if replay_body["schema_version"] != CAP_REPLAY_SCHEMA_VERSION:
            raise _Blocked("CAP_REPLAY_EVIDENCE_VERSION_INVALID")
        if replay_body["precommitment_sha256"] != precommit_meta["body_sha256"]:
            raise _Blocked("CAP_REPLAY_PRECOMMITMENT_BODY_MISMATCH")
        if replay_body["precommitment_envelope_sha256"] != precommit_meta[
            "envelope_sha256"
        ]:
            raise _Blocked("CAP_REPLAY_PRECOMMITMENT_ENVELOPE_MISMATCH")
        try:
            generated = _utc_datetime(
                replay_body["generated_at_utc"], name="replay generation time"
            )
        except ValueError as exc:
            raise _Blocked("CAP_REPLAY_GENERATION_TIME_INVALID") from exc
        if replay_body["generated_at_utc"] != _iso_utc(generated):
            raise _Blocked("CAP_REPLAY_GENERATION_TIME_NOT_CANONICAL")
        if generated < precommit["not_before"]:
            raise _Blocked("CAP_REPLAY_PRECEDES_PRECOMMITTED_ACCESS")
        raw_attempts = (
            replay_body["attempts"]
            if set(replay_body) == _REPLAY_INLINE_KEYS
            else _materialize_attempt_shards(replay_body, replay_evidence_root)
        )
        expected_count = len(policies) * (2 * len(anchors) + 1)
        if type(raw_attempts) is not list or len(raw_attempts) != expected_count:
            raise _Blocked("CAP_ATTEMPT_LEDGER_INCOMPLETE")
        if sum(
            len(item.get("daily_observations", []))
            for item in raw_attempts
            if type(item) is dict and type(item.get("daily_observations")) is list
        ) > MAX_DAILY_OBSERVATIONS:
            raise _Blocked("CAP_ATTEMPT_LEDGER_OBSERVATION_BOUND_EXCEEDED")
        attempts: dict[tuple[str, str, str], Mapping[str, Any]] = {}
        replay_outputs: set[str] = set()
        for raw in raw_attempts:
            if type(raw) is not dict or set(raw) != _ATTEMPT_KEYS:
                raise _Blocked("CAP_ATTEMPT_SCHEMA_INVALID")
            key = (str(raw["anchor_id"]), str(raw["phase"]), str(raw["policy_id"]))
            if key in attempts:
                raise _Blocked("CAP_ATTEMPT_LEDGER_DUPLICATE")
            attempts[key] = raw
            if raw.get("status") != "COMPLETE":
                raise _Blocked(
                    f"CAP_ATTEMPT_NOT_COMPLETE:{raw.get('failure_code') or raw.get('status')}"
                )
            raw_lineage = raw.get("lineage")
            if type(raw_lineage) is dict:
                output = raw_lineage.get("replay_output_sha256")
                if output in replay_outputs:
                    raise _Blocked("CAP_REPLAY_OUTPUT_IDENTITY_REUSED")
                replay_outputs.add(output)
        expected_keys = {
            (anchor["anchor_id"], phase, policy_id)
            for anchor in anchors
            for phase in ("INNER_SELECTION", "OUTER_EVALUATION")
            for policy_id in policies
        } | {("CURRENT", "CURRENT_SELECTION", policy_id) for policy_id in policies}
        if set(attempts) != expected_keys:
            raise _Blocked("CAP_ATTEMPT_LEDGER_ROSTER_MISMATCH")

        alpha_selection = constraints.familywise_alpha / len(policies)
        summaries: dict[tuple[str, str, str], dict[str, Any]] = {}
        for anchor in anchors:
            phase_summaries: dict[str, dict[str, dict[str, Any]]] = {}
            for phase, expected_window, minimum_days, alpha in (
                (
                    "INNER_SELECTION",
                    anchor["inner_window"],
                    constraints.minimum_inner_calendar_days,
                    alpha_selection,
                ),
                (
                    "OUTER_EVALUATION",
                    anchor["outer_window"],
                    constraints.minimum_outer_calendar_days,
                    constraints.familywise_alpha / _JOINT_OUTER_GATE_COUNT,
                ),
            ):
                shared_lineage: Optional[dict[str, Any]] = None
                phase_summaries[phase] = {}
                for policy_id, policy in policies.items():
                    key = (anchor["anchor_id"], phase, policy_id)
                    summary = _attempt_summary(
                        attempts[key],
                        policy=policy,
                        expected_window=expected_window,
                        minimum_days=minimum_days,
                        alpha=alpha,
                        constraints=constraints,
                    )
                    if any(
                        summary["lineage"][name]
                        != precommit["contract_lineage"][name]
                        for name in _CONTRACT_LINEAGE_KEYS
                    ):
                        raise _Blocked("CAP_ATTEMPT_CONTRACT_LINEAGE_MISMATCH")
                    stable = {
                        name: summary["lineage"][name]
                        for name in _STABLE_PHASE_LINEAGE_KEYS
                    }
                    if shared_lineage is not None and stable != shared_lineage:
                        raise _Blocked(
                            f"CAP_POLICY_REPLAYS_DO_NOT_SHARE_{phase}_LINEAGE"
                        )
                    shared_lineage = stable
                    summaries[key] = summary
                    phase_summaries[phase][policy_id] = summary
            for policy_id in policies:
                inner_lineage = phase_summaries["INNER_SELECTION"][policy_id][
                    "lineage"
                ]
                outer_lineage = phase_summaries["OUTER_EVALUATION"][policy_id][
                    "lineage"
                ]
                if any(
                    inner_lineage[name] != outer_lineage[name]
                    for name in _STABLE_ANCHOR_LINEAGE_KEYS
                ):
                    raise _Blocked("CAP_INNER_OUTER_PROCESS_LINEAGE_MISMATCH")

        exact_current_lineage = _current_lineage(current_lineage)
        if exact_current_lineage["book_role"] != "PROCESS_VALIDATED_CURRENT_REFIT":
            raise _Blocked("CAP_CURRENT_BOOK_ROLE_INVALID")
        current_phase_lineage: Optional[dict[str, Any]] = None
        for policy_id, policy in policies.items():
            key = ("CURRENT", "CURRENT_SELECTION", policy_id)
            summary = _attempt_summary(
                attempts[key],
                policy=policy,
                expected_window=precommit["current_window"],
                minimum_days=constraints.minimum_current_calendar_days,
                alpha=alpha_selection,
                constraints=constraints,
            )
            observed_current_lineage = dict(summary["lineage"])
            observed_current_lineage.pop("replay_output_sha256")
            if observed_current_lineage != exact_current_lineage:
                raise _Blocked("CAP_CURRENT_LINEAGE_MISMATCH")
            stable = {
                name: summary["lineage"][name]
                for name in _STABLE_PHASE_LINEAGE_KEYS
            }
            if current_phase_lineage is not None and stable != current_phase_lineage:
                raise _Blocked("CAP_CURRENT_POLICY_REPLAYS_DO_NOT_SHARE_LINEAGE")
            current_phase_lineage = stable
            summaries[key] = summary

        anchor_records: list[dict[str, Any]] = []
        selected_outer: list[dict[str, Any]] = []
        for anchor in anchors:
            inner_summaries = [
                summaries[(anchor["anchor_id"], "INNER_SELECTION", policy_id)]
                for policy_id in policies
            ]
            admissible, local_failures = _selection_candidates(
                inner_summaries, policies, constraints
            )
            frontier, selected_inner = _pareto_select(admissible, policies)
            selected_id = selected_inner["policy_id"]
            outer_summary = summaries[
                (anchor["anchor_id"], "OUTER_EVALUATION", selected_id)
            ]
            outer_failures = _safety_failures(
                outer_summary, policies[selected_id], constraints
            )
            if outer_failures:
                raise _Blocked(
                    *(f"CAP_OUTER_{reason}" for reason in outer_failures)
                )
            selected_outer.append(outer_summary)
            anchor_records.append(
                {
                    "anchor_id": anchor["anchor_id"],
                    "inner_selected_policy_id": selected_id,
                    "inner_admissible_policy_ids": sorted(
                        item["policy_id"] for item in admissible
                    ),
                    "inner_pareto_frontier": frontier,
                    "inner_identification_failures": local_failures,
                    "inner_selected_attempt_sha256": selected_inner["attempt_sha256"],
                    "outer_selected_attempt_sha256": outer_summary["attempt_sha256"],
                    "outer_stress_log_growth_mean": outer_summary[
                        "stress_log_growth_mean"
                    ],
                }
            )
        outer_process, outer_failures = _outer_process_summary(
            selected_outer, constraints
        )
        if outer_failures:
            raise _Blocked(*outer_failures)

        current_summaries = [
            summaries[("CURRENT", "CURRENT_SELECTION", policy_id)]
            for policy_id in policies
        ]
        current_admissible, current_failures = _selection_candidates(
            current_summaries, policies, constraints
        )
        current_frontier, current_selected = _pareto_select(
            current_admissible, policies
        )
        selected_policy = policies[current_selected["policy_id"]]
        envelope = {
            "max_total_stop_risk_pct": selected_policy[
                "max_total_stop_risk_pct"
            ],
            "max_factor_stop_risk_pct": selected_policy[
                "max_factor_stop_risk_pct"
            ],
            "max_same_symbol_stop_risk_pct": selected_policy[
                "max_same_symbol_stop_risk_pct"
            ],
            "max_margin_utilization_pct": selected_policy[
                "max_margin_utilization_pct"
            ],
            "min_margin_level_pct": selected_policy["min_margin_level_pct"],
            "min_free_margin_pct": selected_policy["min_free_margin_pct"],
            "gap_stress_multiple": selected_policy["gap_stress_multiple"],
        }
        receipts = {
            "precommitment": precommit_meta,
            "replay_evidence": replay_meta,
        }
        return _base_artifact(
            start=start,
            end=end,
            lineage=exact_current_lineage,
            constraints=constraints,
            status=EVIDENCE_SELECTED_PENDING_AUTHORIZATION,
            reasons=current_failures,
            evidence_receipts=receipts,
            policy_records=[
                _compact_summary(summary) for summary in current_summaries
            ],
            cap_records=anchor_records,
            frontier=current_frontier,
            outer_process=outer_process,
            selection_payload={
                "selected_policy_point_id": selected_policy["policy_id"],
                "selected_max_total_stop_risk_pct": selected_policy[
                    "max_total_stop_risk_pct"
                ],
                "selected_capital_envelope": envelope,
                "admissible_policy_point_ids": sorted(
                    item["policy_id"] for item in current_admissible
                ),
                "pareto_frontier": current_frontier,
            },
        )
    except _Blocked as exc:
        reasons.extend(exc.reasons)
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError) as exc:
        reasons.append(f"CAP_TECHNICAL_EVIDENCE_ERROR:{type(exc).__name__}")
    receipts = {}
    if precommit_meta:
        receipts["precommitment"] = precommit_meta
    if replay_meta:
        receipts["replay_evidence"] = replay_meta
    return _base_artifact(
        start=start,
        end=end,
        lineage=current_lineage,
        constraints=constraints,
        status=BLOCKED_NO_NUMERIC_AUTHORITY,
        reasons=reasons or BLOCKING_REASONS,
        evidence_receipts=receipts,
    )


def _authorization_valid(
    artifact: Mapping[str, Any],
    *,
    trusted_public_keys: Optional[Mapping[str, bytes]],
    validation_time_utc: Any,
) -> bool:
    receipt = artifact.get("external_authorization")
    try:
        body, metadata = _verified_envelope(
            receipt,
            kind=AUTHORIZATION_SIGNATURE_KIND,
            trusted_public_keys=trusted_public_keys,
            missing_reason="CAP_AUTHORIZATION_MISSING",
        )
        if type(body) is not dict or set(body) != _AUTH_BODY_KEYS:
            return False
        if body["schema_version"] != CAP_AUTHORIZATION_SCHEMA_VERSION:
            return False
        if body["authority_key_id"] != metadata["key_id"]:
            return False
        pending = _json_copy(artifact)
        pending.pop("content_seal")
        pending["external_authorization"] = None
        pending["authority_scope"] = EVIDENCE_SELECTED_PENDING_AUTHORIZATION
        pending["selection"]["status"] = EVIDENCE_SELECTED_PENDING_AUTHORIZATION
        pending_seal = _seal_artifact(pending)["content_seal"]["sha256"]
        if body["pending_artifact_content_sha256"] != pending_seal:
            return False
        if body["calibration_decision_sha256"] != artifact["decision_sha256"]:
            return False
        selection = artifact["selection"]
        if body["selected_policy_point_id"] != selection[
            "selected_policy_point_id"
        ]:
            return False
        if body["selected_max_total_stop_risk_pct"] != selection[
            "selected_max_total_stop_risk_pct"
        ]:
            return False
        if body["selected_capital_envelope"] != selection[
            "selected_capital_envelope"
        ]:
            return False
        if body["source_lineage_sha256"] != canonical_json_sha256(
            artifact["source_lineage"]
        ):
            return False
        if body["constraints_sha256"] != artifact["constraints_sha256"]:
            return False
        issued = _utc_datetime(body["issued_at_utc"], name="authorization issue")
        expires = _utc_datetime(body["expires_at_utc"], name="authorization expiry")
        validation = _utc_datetime(validation_time_utc, name="validation time")
        return issued <= validation < expires and expires > issued
    except (TypeError, ValueError, KeyError, _Blocked):
        return False


def validate_cap_calibration_artifact(
    artifact: Any,
    *,
    expected_lineage: Mapping[str, Any],
    expected_selected_cap_pct: Optional[float],
    expected_selected_policy_point_id: Optional[str] = None,
    expected_constraints: Optional[CapCalibrationConstraints] = None,
    trusted_authority_public_keys: Optional[Mapping[str, bytes]] = None,
    replay_evidence_root: Optional[Path] = None,
    trusted_replay_public_keys: Optional[Mapping[str, bytes]] = None,
    validation_time_utc: Any = None,
    precommitment_receipt: Optional[Mapping[str, Any]] = None,
    replay_evidence_receipt: Optional[Mapping[str, Any]] = None,
    trusted_precommitment_public_keys: Optional[Mapping[str, bytes]] = None,
    require_external_authorization: bool = True,
) -> list[str]:
    errors: list[str] = []
    if type(require_external_authorization) is not bool:
        return ["CAP_CALIBRATION_USAGE_SCOPE_INVALID"]
    if not _artifact_seal_valid(artifact):
        return ["CAP_CALIBRATION_ARTIFACT_SEAL_OR_SCHEMA_INVALID"]
    assert type(artifact) is dict
    if artifact["schema_version"] != CAP_CALIBRATION_SCHEMA_VERSION:
        errors.append("CAP_CALIBRATION_SCHEMA_VERSION_INVALID")
    if artifact["profitability_guarantee"] is not False:
        errors.append("CAP_CALIBRATION_PROFITABILITY_GUARANTEE_INVALID")
    if artifact["places_trades"] is not False:
        errors.append("CAP_CALIBRATION_PLACES_TRADES_INVALID")
    if artifact["content_seal_is_authority"] is not False:
        errors.append("CAP_CALIBRATION_CONTENT_SEAL_AUTHORITY_INVALID")
    try:
        expected = _bounded_lineage_copy(expected_lineage)
        if artifact["source_lineage"] != expected:
            errors.append("CAP_CALIBRATION_SOURCE_LINEAGE_MISMATCH")
    except (TypeError, ValueError):
        errors.append("CAP_CALIBRATION_EXPECTED_LINEAGE_INVALID")
    if artifact["constraints_sha256"] != canonical_json_sha256(
        artifact["constraints"]
    ):
        errors.append("CAP_CALIBRATION_CONSTRAINTS_SHA256_INVALID")
    if expected_constraints is not None:
        try:
            if type(expected_constraints) is not CapCalibrationConstraints:
                raise ValueError("exact constraints required")
            expected_constraints.validate()
            if artifact["constraints"] != asdict(expected_constraints):
                errors.append("CAP_CALIBRATION_EXPECTED_CONSTRAINTS_MISMATCH")
        except ValueError:
            errors.append("CAP_CALIBRATION_EXPECTED_CONSTRAINTS_INVALID")
    selection = artifact["selection"]
    status = selection.get("status") if type(selection) is dict else None
    numeric_fields = (
        selection.get("selected_policy_point_id") if type(selection) is dict else None,
        selection.get("selected_max_total_stop_risk_pct")
        if type(selection) is dict
        else None,
        selection.get("selected_capital_envelope") if type(selection) is dict else None,
    )
    if status == BLOCKED_NO_NUMERIC_AUTHORITY:
        if artifact["authority_scope"] != BLOCKED_NO_NUMERIC_AUTHORITY:
            errors.append("CAP_CALIBRATION_AUTHORITY_SCOPE_MISMATCH")
        if any(value is not None for value in numeric_fields):
            errors.append("CAP_CALIBRATION_BLOCKED_ARTIFACT_CONTAINS_NUMERIC_SELECTION")
        if artifact["decision_sha256"] is not None:
            errors.append("CAP_CALIBRATION_BLOCKED_ARTIFACT_CONTAINS_DECISION")
    elif status in {
        EVIDENCE_SELECTED_PENDING_AUTHORIZATION,
        AUTHORIZED_NUMERIC_ENVELOPE,
    }:
        if artifact["authority_scope"] != status:
            errors.append("CAP_CALIBRATION_AUTHORITY_SCOPE_MISMATCH")
        if any(value is None for value in numeric_fields):
            errors.append("CAP_CALIBRATION_NUMERIC_SELECTION_INCOMPLETE")
        if artifact["decision_sha256"] != canonical_json_sha256(
            _decision_material(artifact)
        ):
            errors.append("CAP_CALIBRATION_DECISION_SHA256_INVALID")
        envelope = selection.get("selected_capital_envelope")
        if type(envelope) is not dict or envelope.get(
            "max_total_stop_risk_pct"
        ) != selection.get("selected_max_total_stop_risk_pct"):
            errors.append("CAP_CALIBRATION_ENVELOPE_TOTAL_CAP_MISMATCH")
    else:
        errors.append("CAP_CALIBRATION_STATUS_INVALID")
    if validation_time_utc is not None:
        try:
            generated = _utc_datetime(
                artifact["generated_at_utc"], name="generation time"
            )
            validation = _utc_datetime(validation_time_utc, name="validation time")
            if generated > validation:
                errors.append("CAP_CALIBRATION_GENERATED_IN_FUTURE")
        except ValueError:
            errors.append("CAP_CALIBRATION_VALIDATION_TIME_INVALID")
    if expected_selected_cap_pct is not None:
        if type(expected_selected_cap_pct) not in {int, float} or not math.isfinite(
            float(expected_selected_cap_pct)
        ):
            errors.append("CAP_CALIBRATION_EXPECTED_CAP_INVALID")
            expected_cap = None
        else:
            expected_cap = float(expected_selected_cap_pct)
        if expected_cap is not None and selection.get(
            "selected_max_total_stop_risk_pct"
        ) != expected_cap:
            errors.append("CAP_CALIBRATION_CONFIG_CAP_MISMATCH")
    numeric_status = status in {
        EVIDENCE_SELECTED_PENDING_AUTHORIZATION,
        AUTHORIZED_NUMERIC_ENVELOPE,
    }
    if numeric_status and require_external_authorization:
        if status != AUTHORIZED_NUMERIC_ENVELOPE:
            errors.append("CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_REQUIRED")
        elif validation_time_utc is None or not _authorization_valid(
            artifact,
            trusted_public_keys=trusted_authority_public_keys,
            validation_time_utc=validation_time_utc,
        ):
            errors.append("CAP_CALIBRATION_EXTERNAL_AUTHORIZATION_INVALID")
    elif numeric_status:
        try:
            replayed_constraints = CapCalibrationConstraints(**artifact["constraints"])
            replayed = calibrate_total_stop_risk_cap(
                candidates=(),
                base_policies={},
                evaluation_start=artifact["window"]["evaluation_start"],
                evaluation_end=artifact["window"]["evaluation_end"],
                lineage=artifact["source_lineage"],
                constraints=replayed_constraints,
                trusted_replay_public_keys=trusted_replay_public_keys,
                replay_evidence_root=replay_evidence_root,
                precommitment_receipt=precommitment_receipt,
                replay_evidence_receipt=replay_evidence_receipt,
                trusted_precommitment_public_keys=trusted_precommitment_public_keys,
            )
            if (
                replayed["selection"]["status"]
                != EVIDENCE_SELECTED_PENDING_AUTHORIZATION
                or replayed["decision_sha256"] != artifact["decision_sha256"]
                or replayed["selection"]["selected_capital_envelope"]
                != selection.get("selected_capital_envelope")
                or replayed["evidence_receipts"] != artifact["evidence_receipts"]
            ):
                errors.append("CAP_CALIBRATION_EVIDENCE_REVERIFICATION_FAILED")
        except (TypeError, ValueError, KeyError, RecursionError):
            errors.append("CAP_CALIBRATION_EVIDENCE_REVERIFICATION_FAILED")
    if expected_selected_policy_point_id is not None:
        if selection.get("selected_policy_point_id") != expected_selected_policy_point_id:
            errors.append("CAP_CALIBRATION_POLICY_POINT_MISMATCH")
    return list(dict.fromkeys(errors))


def _verify_pending_artifact_evidence(
    diagnostic: Mapping[str, Any],
    *,
    replay_evidence_root: Optional[Path],
    trusted_replay_public_keys: Optional[Mapping[str, bytes]],
    precommitment_receipt: Optional[Mapping[str, Any]],
    replay_evidence_receipt: Optional[Mapping[str, Any]],
    trusted_precommitment_public_keys: Optional[Mapping[str, bytes]],
    validation_time_utc: Any,
    expected_deployment_lineage: Optional[Mapping[str, Any]],
    expected_constraints: Optional[CapCalibrationConstraints],
) -> None:
    if not _artifact_seal_valid(diagnostic):
        raise ValueError("CAPITAL_ENVELOPE_DIAGNOSTIC_INVALID")
    if diagnostic["selection"]["status"] != EVIDENCE_SELECTED_PENDING_AUTHORIZATION:
        raise ValueError("CAPITAL_ENVELOPE_NOT_PENDING_AUTHORIZATION")
    try:
        artifact_constraints = CapCalibrationConstraints(**diagnostic["constraints"])
        artifact_constraints.validate()
        expected_lineage = (
            diagnostic["source_lineage"]
            if expected_deployment_lineage is None
            else expected_deployment_lineage
        )
        expected_cap = diagnostic["selection"][
            "selected_max_total_stop_risk_pct"
        ]
        expected_policy_id = diagnostic["selection"]["selected_policy_point_id"]
        constraints = expected_constraints or artifact_constraints
        errors = validate_cap_calibration_artifact(
            diagnostic,
            expected_lineage=expected_lineage,
            expected_selected_cap_pct=expected_cap,
            expected_selected_policy_point_id=expected_policy_id,
            expected_constraints=constraints,
            replay_evidence_root=replay_evidence_root,
            trusted_replay_public_keys=trusted_replay_public_keys,
            validation_time_utc=validation_time_utc,
            precommitment_receipt=precommitment_receipt,
            replay_evidence_receipt=replay_evidence_receipt,
            trusted_precommitment_public_keys=trusted_precommitment_public_keys,
            require_external_authorization=False,
        )
    except (TypeError, ValueError, KeyError, RecursionError) as exc:
        raise ValueError(
            "CAPITAL_ENVELOPE_EVIDENCE_REVERIFICATION_FAILED"
        ) from exc
    if errors:
        raise ValueError(
            "CAPITAL_ENVELOPE_EVIDENCE_REVERIFICATION_FAILED:"
            + ",".join(errors)
        )


def build_capital_envelope_authorization_body(
    diagnostic: Mapping[str, Any],
    *,
    authority_key_id: str,
    selected_policy_point_id: Optional[str] = None,
    selected_cap_pct: Optional[float] = None,
    issued_at_utc: Any,
    expires_at_utc: Any,
    replay_evidence_root: Path,
    trusted_replay_public_keys: Mapping[str, bytes],
    precommitment_receipt: Optional[Mapping[str, Any]] = None,
    replay_evidence_receipt: Optional[Mapping[str, Any]] = None,
    trusted_precommitment_public_keys: Optional[Mapping[str, bytes]] = None,
    expected_deployment_lineage: Optional[Mapping[str, Any]] = None,
    expected_constraints: Optional[CapCalibrationConstraints] = None,
) -> Dict[str, Any]:
    if type(authority_key_id) is not str or not authority_key_id:
        raise ValueError("CAPITAL_ENVELOPE_AUTHORITY_KEY_ID_INVALID")
    issued = _utc_datetime(issued_at_utc, name="authorization issue")
    expires = _utc_datetime(expires_at_utc, name="authorization expiry")
    if expires <= issued:
        raise ValueError("CAPITAL_ENVELOPE_AUTHORIZATION_WINDOW_INVALID")
    _verify_pending_artifact_evidence(
        diagnostic,
        replay_evidence_root=replay_evidence_root,
        trusted_replay_public_keys=trusted_replay_public_keys,
        precommitment_receipt=precommitment_receipt,
        replay_evidence_receipt=replay_evidence_receipt,
        trusted_precommitment_public_keys=trusted_precommitment_public_keys,
        validation_time_utc=issued,
        expected_deployment_lineage=expected_deployment_lineage,
        expected_constraints=expected_constraints,
    )
    selection = diagnostic["selection"]
    if selected_policy_point_id is not None and selected_policy_point_id != selection[
        "selected_policy_point_id"
    ]:
        raise ValueError("CAPITAL_ENVELOPE_POLICY_POINT_MISMATCH")
    if selected_cap_pct is not None and float(selected_cap_pct) != selection[
        "selected_max_total_stop_risk_pct"
    ]:
        raise ValueError("CAPITAL_ENVELOPE_CAP_MISMATCH")
    if expected_deployment_lineage is not None and _bounded_lineage_copy(
        expected_deployment_lineage
    ) != diagnostic["source_lineage"]:
        raise ValueError("CAPITAL_ENVELOPE_LINEAGE_MISMATCH")
    if expected_constraints is not None and asdict(expected_constraints) != diagnostic[
        "constraints"
    ]:
        raise ValueError("CAPITAL_ENVELOPE_CONSTRAINTS_MISMATCH")
    return {
        "schema_version": CAP_AUTHORIZATION_SCHEMA_VERSION,
        "authority_key_id": authority_key_id,
        "pending_artifact_content_sha256": diagnostic["content_seal"]["sha256"],
        "calibration_decision_sha256": diagnostic["decision_sha256"],
        "selected_policy_point_id": selection["selected_policy_point_id"],
        "selected_max_total_stop_risk_pct": selection[
            "selected_max_total_stop_risk_pct"
        ],
        "selected_capital_envelope": selection["selected_capital_envelope"],
        "source_lineage_sha256": canonical_json_sha256(
            diagnostic["source_lineage"]
        ),
        "constraints_sha256": diagnostic["constraints_sha256"],
        "issued_at_utc": _iso_utc(issued),
        "expires_at_utc": _iso_utc(expires),
    }


def attach_capital_envelope_authorization(
    diagnostic: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    trusted_public_keys: Mapping[str, bytes],
    validation_time_utc: Any,
    replay_evidence_root: Path,
    trusted_replay_public_keys: Mapping[str, bytes],
    precommitment_receipt: Optional[Mapping[str, Any]] = None,
    replay_evidence_receipt: Optional[Mapping[str, Any]] = None,
    trusted_precommitment_public_keys: Optional[Mapping[str, bytes]] = None,
    expected_deployment_lineage: Optional[Mapping[str, Any]] = None,
    expected_constraints: Optional[CapCalibrationConstraints] = None,
) -> Dict[str, Any]:
    if not _artifact_seal_valid(diagnostic):
        raise ValueError("CAPITAL_ENVELOPE_DIAGNOSTIC_INVALID")
    body, metadata = _verified_envelope(
        receipt,
        kind=AUTHORIZATION_SIGNATURE_KIND,
        trusted_public_keys=trusted_public_keys,
        missing_reason="CAP_AUTHORIZATION_MISSING",
    )
    expected_body = build_capital_envelope_authorization_body(
        diagnostic,
        authority_key_id=metadata["key_id"],
        issued_at_utc=body.get("issued_at_utc"),
        expires_at_utc=body.get("expires_at_utc"),
        replay_evidence_root=replay_evidence_root,
        trusted_replay_public_keys=trusted_replay_public_keys,
        precommitment_receipt=precommitment_receipt,
        replay_evidence_receipt=replay_evidence_receipt,
        trusted_precommitment_public_keys=trusted_precommitment_public_keys,
        expected_deployment_lineage=expected_deployment_lineage,
        expected_constraints=expected_constraints,
    )
    if body != expected_body or body.get("authority_key_id") != metadata["key_id"]:
        raise ValueError("CAPITAL_ENVELOPE_AUTHORIZATION_BODY_MISMATCH")
    validation = _utc_datetime(validation_time_utc, name="validation time")
    issued = _utc_datetime(body["issued_at_utc"], name="authorization issue")
    expires = _utc_datetime(body["expires_at_utc"], name="authorization expiry")
    if not issued <= validation < expires:
        raise ValueError("CAPITAL_ENVELOPE_AUTHORIZATION_NOT_CURRENT")
    result = _json_copy(diagnostic)
    result.pop("content_seal")
    result["external_authorization"] = _json_copy(receipt)
    result["authority_scope"] = AUTHORIZED_NUMERIC_ENVELOPE
    result["selection"]["status"] = AUTHORIZED_NUMERIC_ENVELOPE
    return _seal_artifact(result)


def _is_link_or_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return path.is_symlink() or bool(getattr(info, "st_file_attributes", 0) & 0x400)


def write_immutable_cap_calibration(
    directory: Path, artifact: Mapping[str, Any]
) -> Path:
    if not _artifact_seal_valid(artifact):
        raise ValueError("CAPITAL_ENVELOPE_ARTIFACT_INVALID")
    target_dir = Path(directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    if not target_dir.is_dir() or _is_link_or_reparse(target_dir):
        raise ValueError("capital artifact directory is unsafe")
    digest = artifact["content_seal"]["sha256"]
    target = target_dir / f"cap_calibration_{digest}.json"
    encoded = _canonical_json_bytes(artifact) + b"\n"
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise ValueError("capital artifact exceeds its byte bound")
    if target.exists():
        if _read_regular_file_bounded(target, MAX_ARTIFACT_BYTES) != encoded:
            raise ValueError("immutable capital artifact path contains different bytes")
        return target
    descriptor = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return target


__all__ = [
    "AUTHORIZED_NUMERIC_ENVELOPE",
    "BLOCKING_REASONS",
    "BLOCKED_NO_NUMERIC_AUTHORITY",
    "CAPITAL_AUTHORITY_SCOPE",
    "CAP_AUTHORIZATION_SCHEMA_VERSION",
    "CAP_CALIBRATION_SCHEMA_VERSION",
    "CAP_PRECOMMIT_SCHEMA_VERSION",
    "CAP_REPLAY_SCHEMA_VERSION",
    "CAP_REPLAY_SHARD_SCHEMA_VERSION",
    "CAP_SIGNED_ENVELOPE_SCHEMA_VERSION",
    "EVIDENCE_SELECTED_PENDING_AUTHORIZATION",
    "FLAT_MARGIN_LEVEL_PCT",
    "METHOD_REFERENCES",
    "PRECOMMITMENT_SIGNATURE_KIND",
    "REPLAY_EVIDENCE_SIGNATURE_KIND",
    "AUTHORIZATION_SIGNATURE_KIND",
    "CapCalibrationConstraints",
    "PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT",
    "PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT",
    "attach_capital_envelope_authorization",
    "build_capital_envelope_authorization_body",
    "build_frozen_production_candidate_roster",
    "calibrate_total_stop_risk_cap",
    "canonical_json_sha256",
    "cap_neutral_config_sha256",
    "cap_neutral_policy_contract_sha256",
    "capital_authority_signature_payload",
    "current_book_authority_adapter_contract",
    "validate_cap_calibration_artifact",
    "write_immutable_cap_calibration",
    "write_capital_replay_attempt_shards",
]
