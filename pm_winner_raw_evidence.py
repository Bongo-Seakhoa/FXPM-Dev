"""Search-time Winner V11 evidence capture and authority spooling."""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

import pm_current_refit_artifacts as refit_artifacts
import pm_winner_selection as winner
from pm_authority_spool import SymbolAuthoritySink
from pm_core import (
    BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
    BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
    BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
    BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION,
    BACKTEST_WINNER_DAILY_R_SCHEMA_VERSION,
    BACKTEST_WINNER_RISK_UNIT_SCHEMA_VERSION,
    InstrumentSpec,
    build_backtest_winner_risk_unit_contract,
    canonical_backtest_winner_r,
)
from pm_search_outcomes import SearchOutcomeKind, TrialOutcome
from pm_trade_power import (
    NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION,
    REQUIRED_TRADE_RECORD_SCHEMA,
    TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
    TradePowerDecision,
    TradePowerPolicy,
    build_numerical_execution_cost_attestation,
    build_trade_power_multiplicity_receipt,
    build_trade_power_partition_contract,
    build_trade_sample_evidence,
    calibrate_train_trade_power,
    evaluate_trade_power_partition,
    trade_power_decision_sha256,
    trade_power_requirement_sha256,
)


FROZEN_PLAN_SCHEMA = "fxpm.winner-search-plan-frozen.v1"
FROZEN_COST_POLICY_SCHEMA = "fxpm.winner-numerical-execution-cost-policy.v2"
RAW_LANE_SCHEMA = "fxpm.winner-raw-backtester-lane.v1"
DAILY_R_SURFACE_SCHEMA = BACKTEST_WINNER_DAILY_R_SCHEMA_VERSION
RISK_UNIT_SCHEMA = BACKTEST_WINNER_RISK_UNIT_SCHEMA_VERSION
POSITION_ACTIVITY_SCHEMA = "fxpm.winner-position-activity-ledger.v1"
SHARED_OOS_PARTITION_SCHEMA = winner.SHARED_OOS_PARTITION_VERSION
POWER_CHAIN_SCHEMA = "fxpm.winner-trade-power-chain.v1"
PRODUCTION_SCHEMA = "fxpm.winner-raw-evidence-production.v1"
AUTHORITATIVE_PROVENANCE_SCHEMA = "fxpm.authoritative-search-provenance.v1"
HORIZON_WINNER_CROSS_BINDING_SCHEMA = "fxpm.historical-horizon-winner-cross-binding.v1"
HORIZON_WINNER_CROSS_BINDING_SCHEMA_V2 = "fxpm.horizon-winner-cross-binding.v2"
HISTORICAL_CROSS_BINDING_MODE = "HISTORICAL"
CURRENT_CROSS_BINDING_MODE = "CURRENT"
PARTITION_GEOMETRY_SCHEMA = "fxpm.winner-partition-geometry.v1"
SIGNAL_VECTOR_SCHEMA = "fxpm.authoritative-search-signal-vector.v1"
SOURCE_COUNT_RECEIPT_SCHEMA = "fxpm.historical-attempt-source-count.v1"
EXECUTION_SPEC_RECEIPT_SCHEMA = "fxpm.execution-instrument-spec-receipt.v2"
COST_STRESS_ADVERSITY_PLAN_SCHEMA = (
    "fxpm.winner-cost-stress-adversity-plan.v1"
)
COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK = (
    BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
)
COST_STRESS_ADVERSITY_EXPECTED_STRICT = (
    "STRICT_NUMERICAL_ADVERSITY_REQUIRED"
)
MAX_SIGNAL_VECTOR_OBSERVATIONS = 5_000_000
FROZEN_PATH_COST_STRESS_SCHEMA = BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION
ASCII_JSON_FORMAT = "fxpm-ascii-json-v1"

PRODUCTION_TRADE_POWER_POLICY = TradePowerPolicy(
    minimum_economic_edge_r=0.25,
    maximum_ci_half_width_r=0.25,
    target_power=0.80,
)
PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS = 8

_HEX = frozenset("0123456789abcdef")
_NAMESPACE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_ATTEMPT_ARTIFACT_KEYS = frozenset(
    {"winner_process", "horizon_process", "trial_definition", "refit_recipe"}
)
_HORIZON_EXECUTION_CONTRACT_KEYS = frozenset(
    {"lookback_kind", "lookback_value", "source_bar_count"}
)
_HORIZON_LOOKBACK_KINDS = frozenset(
    {"NATIVE_OBSERVATIONS", "ACTIVE_SESSIONS", "CALENDAR_INTERVAL"}
)
_PROVENANCE_KEYS = (
    "anchor_selection_receipt",
    "anchor_horizon_decision",
    "anchor_trial_ledger",
    "anchor_selected_trial",
    "anchor_refit_receipt",
    "attempt_replay_receipt",
    "oos_source_receipt",
)
_LANE_KEYS = (
    "optimizer_train_base",
    "tuning_validation_base",
    "final_lockbox_base",
    "final_lockbox_stress",
)
_V11_STATUS_REASONS = {
    "AUTHENTICATED": None,
    "PROCESS_REJECTED": "EVALUATED_OOS_PROCESS_REJECTED",
    "TECHNICAL_FAILURE": "EVALUATOR_TECHNICAL_FAILURE",
}
_PRECOMMITTED_EVIDENCE_ROLES = frozenset(
    {"EVALUATION_REQUIRED", "PRESEARCH_PROCESS_REJECTED"}
)
_FROZEN_COST_POLICY_BODY_FIELDS = frozenset(
    {
        "schema_version",
        "lane",
        "attestation_schema_version",
        "use_spread",
        "spread_mode",
        "instrument_spec_sha256",
        "numerical_execution_cost_policy_sha256",
    }
)
_FROZEN_COST_POLICY_FIELDS = _FROZEN_COST_POLICY_BODY_FIELDS | {
    "policy_identity_sha256"
}


class WinnerRawEvidenceError(ValueError):
    """Typed fail-closed producer error."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        super().__init__(code if not detail else f"{code}: {detail}")


class WinnerRawEvidenceIncomplete(WinnerRawEvidenceError):
    """Technical evidence failure; never an economic no-trade verdict."""


def _validated_namespace(value: Any) -> Optional[str]:
    """Validate an immutable spool namespace such as historical-anchor/run/anchor."""
    if value is None:
        return None
    # 512 total admits historical-anchor/{run_id}/{anchor_id} with two
    # maximum-length (128) safe-ids; spool artifact-id bound is 1024.
    if type(value) is not str or not 1 <= len(value) <= 512:
        _fail("ARTIFACT_NAMESPACE_INVALID")
    segments = value.split("/")
    if not 1 <= len(segments) <= 8 or any(
        _NAMESPACE_SEGMENT.fullmatch(segment) is None for segment in segments
    ):
        _fail("ARTIFACT_NAMESPACE_INVALID")
    return value


def _artifact_prefix(namespace: Optional[str], timeframe: str, regime: str) -> str:
    base = f"winner-search/{timeframe}/{regime}"
    return base if namespace is None else f"{namespace}/{base}"


@dataclass(frozen=True)
class FrozenWinnerSearchPlan:
    symbol: str
    timeframe: str
    regime: str
    artifact_namespace: Optional[str]
    generation_context_sha256: str
    precommitment_bytes: bytes
    attempt_ledger_bytes: bytes
    source_contract_seed_bytes: bytes
    base_cost_contract_bytes: bytes
    stress_cost_contract_bytes: bytes
    risk_unit_contract_bytes: bytes
    cost_stress_adversity_plan_bytes: bytes
    frozen_receipt_bytes: bytes
    frozen_receipt_entry_bytes: bytes
    horizon_execution_contracts_bytes: bytes
    # Canonical attempt bodies are retained as an immutable, bounded bundle so
    # evidence-time provenance can be reconstructed from the exact executable
    # inputs frozen before search.  Hash-only attempt ledgers are insufficient
    # for binding source snapshots and refit geometry at this boundary.
    attempt_artifacts_bundle_bytes: bytes = b""

    @property
    def frozen_receipt_entry(self) -> dict[str, Any]:
        """The exact sink spool entry for {prefix}/frozen/receipt.

        Handed to adapters so they can register the GENUINE raw descriptor
        (artifact_id/type/relative_path) in an orchestration-owned manifest
        without private sink access.  Its descriptor sha256 is bound to
        ``frozen_receipt_bytes``; production re-verifies the binding.
        """
        return _decode_object(
            self.frozen_receipt_entry_bytes, "FROZEN_RECEIPT_ENTRY_INVALID"
        )

    @property
    def precommitment(self) -> dict[str, Any]:
        return _decode_object(self.precommitment_bytes, "PRECOMMITMENT_BYTES_INVALID")

    @property
    def attempt_ledger(self) -> dict[str, Any]:
        return _decode_object(self.attempt_ledger_bytes, "ATTEMPT_LEDGER_BYTES_INVALID")

    @property
    def source_contract_seed(self) -> dict[str, Any]:
        return _decode_object(
            self.source_contract_seed_bytes, "SOURCE_SEED_BYTES_INVALID"
        )

    @property
    def base_cost_contract(self) -> dict[str, Any]:
        """Frozen BASE cost policy (the historical property name is retained)."""
        return _decode_object(self.base_cost_contract_bytes, "BASE_COST_BYTES_INVALID")

    @property
    def stress_cost_contract(self) -> dict[str, Any]:
        """Frozen STRESS cost policy (the historical property name is retained)."""
        return _decode_object(
            self.stress_cost_contract_bytes, "STRESS_COST_BYTES_INVALID"
        )

    @property
    def base_cost_policy(self) -> dict[str, Any]:
        return self.base_cost_contract

    @property
    def stress_cost_policy(self) -> dict[str, Any]:
        return self.stress_cost_contract

    @property
    def risk_unit_contract(self) -> dict[str, Any]:
        return _decode_object(self.risk_unit_contract_bytes, "RISK_UNIT_BYTES_INVALID")

    @property
    def cost_stress_adversity_plan(self) -> dict[str, Any]:
        return _decode_object(
            self.cost_stress_adversity_plan_bytes,
            "COST_STRESS_ADVERSITY_PLAN_BYTES_INVALID",
        )

    @property
    def attempt_artifacts_bundle(self) -> list[dict[str, Any]]:
        if (
            type(self.attempt_artifacts_bundle_bytes) is not bytes
            or not self.attempt_artifacts_bundle_bytes
            or len(self.attempt_artifacts_bundle_bytes) > winner.MAX_REQUEST_BYTES
        ):
            _fail("ATTEMPT_ARTIFACT_BUNDLE_BYTES_INVALID")
        try:
            value = json.loads(self.attempt_artifacts_bundle_bytes.decode("ascii"))
        except (AttributeError, UnicodeError, json.JSONDecodeError) as exc:
            raise WinnerRawEvidenceError(
                "ATTEMPT_ARTIFACT_BUNDLE_BYTES_INVALID"
            ) from exc
        if type(value) is not list or _canonical_bytes(value) != (
            self.attempt_artifacts_bundle_bytes
        ):
            _fail("ATTEMPT_ARTIFACT_BUNDLE_BYTES_INVALID")
        return value

    @property
    def horizon_execution_contracts(self) -> list[dict[str, Any]]:
        if (
            type(self.horizon_execution_contracts_bytes) is not bytes
            or not self.horizon_execution_contracts_bytes
            or len(self.horizon_execution_contracts_bytes) > winner.MAX_REQUEST_BYTES
        ):
            _fail("HORIZON_EXECUTION_CONTRACT_BYTES_INVALID")
        value = _decode_object_list(
            self.horizon_execution_contracts_bytes,
            "HORIZON_EXECUTION_CONTRACT_BYTES_INVALID",
        )
        if not 1 <= len(value) <= winner.MAX_ATTEMPTS:
            _fail("HORIZON_EXECUTION_CONTRACT_BYTES_INVALID")
        return value

    def registry_commitments(self) -> dict[str, str]:
        """Raw-Winner roster commitments derived from the owned frozen bytes.

        These hash raw-Winner-specific shapes and are NOT equal to the
        released pm_training_horizon registry hashes (different schemas).
        A consumer binding both lineages must persist and reverify a
        bijective semantic cross-binding receipt over the two independently
        hashed rosters — never assert direct hash equality, and never accept
        caller-declared values in place of these byte-derived ones.
        """
        precommitment = self.precommitment
        attempt_ledger = self.attempt_ledger
        return {
            "run_id": precommitment["run_id"],
            "process_registry_sha256": precommitment["process_registry_sha256"],
            "horizon_registry_sha256": precommitment["horizon_registry_sha256"],
            "trial_registry_sha256": precommitment["trial_registry_sha256"],
            "attempts_sha256": attempt_ledger["attempts_sha256"],
            "causal_fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
            "precommitment_sha256": _sha_json(precommitment),
            "attempt_ledger_sha256": _sha_json(attempt_ledger),
        }


@dataclass(frozen=True)
class WinnerRawEvidenceProduction:
    evidence_ledger: dict[str, Any]
    request: winner.WinnerSelectionRequest
    decision: winner.WinnerSelectionDecision
    marginal_receipt: Optional[dict[str, Any]]
    entries: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _FoldOutcome:
    status: str
    fold: Optional[dict[str, Any]]
    pbo_fold: Optional[dict[str, Any]]
    reason_code: str


def _fail(code: str, detail: str = "") -> None:
    raise WinnerRawEvidenceError(code, detail)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise WinnerRawEvidenceError("VALUE_NOT_CANONICAL_JSON", str(exc)) from exc


def _decode_object(encoded: bytes, code: str) -> dict[str, Any]:
    if type(encoded) is not bytes or not encoded:
        _fail(code)
    try:
        value = json.loads(encoded.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WinnerRawEvidenceError(code) from exc
    if type(value) is not dict or _canonical_bytes(value) != encoded:
        _fail(code)
    return value


def _decode_object_list(encoded: bytes, code: str) -> list[dict[str, Any]]:
    if type(encoded) is not bytes or not encoded:
        _fail(code)
    try:
        value = json.loads(encoded.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WinnerRawEvidenceError(code) from exc
    if (
        type(value) is not list
        or any(type(item) is not dict for item in value)
        or _canonical_bytes(value) != encoded
    ):
        _fail(code)
    return value


def _sha_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha1_json(value: Any) -> str:
    return hashlib.sha1(_canonical_bytes(value)).hexdigest()


def _backtester_receipt_sha256(value: Any) -> str:
    """Mirror Backtester's float-exact receipt canonicalization.

    Position roster receipts encode every float as its IEEE-754 hexadecimal
    representation.  Re-hashing ordinary JSON would accept test fixtures but
    reject the genuine Backtester receipt (or, worse, compare a weaker shape).
    """

    def exact(item: Any) -> Any:
        if isinstance(item, bool):
            return item
        if isinstance(item, int):
            return item
        if isinstance(item, float):
            if not math.isfinite(item):
                _fail("BACKTESTER_RECEIPT_FLOAT_INVALID")
            return {"float64_hex": item.hex().lower()}
        if item is None or isinstance(item, str):
            return item
        if isinstance(item, Mapping):
            return {
                str(key): exact(child)
                for key, child in sorted(item.items(), key=lambda pair: str(pair[0]))
            }
        if isinstance(item, (list, tuple)):
            return [exact(child) for child in item]
        _fail("BACKTESTER_RECEIPT_VALUE_INVALID")

    return _sha_json(exact(value))


def _validated_signal_vector_receipt(value: Any) -> dict[str, Any]:
    expected = {
        "schema_version",
        "encoding",
        "observation_count",
        "values",
        "signal_vector_sha256",
        "receipt_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        _fail("SIGNAL_VECTOR_RECEIPT_SCHEMA_INVALID")
    receipt = dict(value)
    body = dict(receipt)
    receipt_sha = body.pop("receipt_sha256", None)
    if (
        body.get("schema_version") != SIGNAL_VECTOR_SCHEMA
        or body.get("encoding") != "little-endian-signed-int32"
        or receipt_sha != _sha_json(body)
    ):
        _fail("SIGNAL_VECTOR_RECEIPT_HASH_INVALID")
    values = body.get("values")
    count = body.get("observation_count")
    if (
        type(values) is not list
        or type(count) is not int
        or not 1 <= count <= MAX_SIGNAL_VECTOR_OBSERVATIONS
        or len(values) != count
    ):
        _fail("SIGNAL_VECTOR_RECEIPT_BOUNDS_INVALID")
    digest = hashlib.sha256()
    for signal in values:
        if type(signal) is not int or signal not in {-1, 0, 1}:
            _fail("SIGNAL_VECTOR_VALUE_INVALID")
        digest.update(struct.pack("<i", signal))
    if body.get("signal_vector_sha256") != digest.hexdigest():
        _fail("SIGNAL_VECTOR_CONTENT_HASH_INVALID")
    return receipt


def _validated_execution_spec_receipt(
    value: Any,
    *,
    frozen: FrozenWinnerSearchPlan,
    cross_binding: Mapping[str, Any],
    lanes: Mapping[str, Any],
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "symbol",
        "authority_timing",
        "deployment_scope",
        "deployment_config_sha256",
        "deployment_config_size_bytes",
        "broker_specs_relative_path",
        "broker_specs_sha256",
        "broker_specs_size_bytes",
        "broker_record_key",
        "broker_record_sha256",
        "native_prefix_index_sha256",
        "native_provenance_sha256",
        "native_instrument_snapshot_sha256",
        "tick_value_source",
        "volume_source",
        "instrument_spec",
        "instrument_spec_sha256",
        "grants_search_authority",
        "grants_trading_authority",
        "receipt_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        _fail("EXECUTION_SPEC_RECEIPT_SCHEMA_INVALID")
    receipt = dict(value)
    body = dict(receipt)
    receipt_sha256 = body.pop("receipt_sha256", None)
    spec = receipt.get("instrument_spec")
    if not isinstance(spec, Mapping) or set(spec) != {
        item.name for item in fields(InstrumentSpec)
    }:
        _fail("EXECUTION_SPEC_BODY_SCHEMA_INVALID")
    try:
        rebuilt = InstrumentSpec(**dict(spec))
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError("EXECUTION_SPEC_BODY_INVALID") from exc
    integer_fields = {
        "pip_position",
        "swap_mode",
        "swap_rollover3days",
        "stops_level",
        "digits",
    }
    numeric_fields = {
        "pip_value",
        "spread_avg",
        "min_lot",
        "max_lot",
        "swap_long",
        "swap_short",
        "tick_size",
        "tick_value",
        "contract_size",
        "volume_step",
        "point",
    }
    optional_numeric_fields = {
        "swap_sunday",
        "swap_monday",
        "swap_tuesday",
        "swap_wednesday",
        "swap_thursday",
        "swap_friday",
        "swap_saturday",
    }
    if (
        any(type(spec.get(name)) is not int for name in integer_fields)
        or any(
            type(spec.get(name)) not in {int, float}
            or not math.isfinite(float(spec[name]))
            for name in numeric_fields
        )
        or any(
            spec.get(name) is not None
            and (
                type(spec.get(name)) not in {int, float}
                or not math.isfinite(float(spec[name]))
            )
            for name in optional_numeric_fields
        )
    ):
        _fail("EXECUTION_SPEC_BODY_NUMERIC_INVALID")
    if (
        not 0 <= spec["pip_position"] <= 20
        or not 0 <= spec["digits"] <= 20
        or spec["stops_level"] < 0
        or not 0 <= spec["swap_rollover3days"] <= 6
        or any(
            float(spec[name]) <= 0.0
            for name in (
                "pip_value",
                "min_lot",
                "max_lot",
                "tick_size",
                "tick_value",
                "contract_size",
                "volume_step",
                "point",
            )
        )
        or float(spec["spread_avg"]) < 0.0
        or float(spec["min_lot"]) > float(spec["max_lot"])
        or float(spec["volume_step"]) > float(spec["max_lot"])
        or type(spec.get("asset_class")) is not str
        or not spec["asset_class"].strip()
        or type(spec.get("swap_terms_provenance_status")) is not str
        or not spec["swap_terms_provenance_status"].strip()
        or type(spec.get("swap_terms_snapshot_sha256")) is not str
        or (
            spec["swap_terms_snapshot_sha256"]
            and (
                len(spec["swap_terms_snapshot_sha256"]) != 64
                or any(
                    character not in _HEX
                    for character in spec["swap_terms_snapshot_sha256"]
                )
            )
        )
    ):
        _fail("EXECUTION_SPEC_BODY_ECONOMICS_INVALID")
    if _canonical_bytes(asdict(rebuilt)) != _canonical_bytes(dict(spec)):
        _fail("EXECUTION_SPEC_BODY_REPLAY_INVALID")
    expected_instrument_hashes = {
        frozen.base_cost_policy.get("instrument_spec_sha256"),
        frozen.stress_cost_policy.get("instrument_spec_sha256"),
        (
            frozen.source_contract_seed.get("numerical_execution_cost_attestation")
            or {}
        ).get("instrument_spec_sha256"),
    }
    for lane in _LANE_KEYS:
        lane_body = lanes.get(lane)
        attestation = (
            lane_body.get("numerical_execution_cost_attestation")
            if isinstance(lane_body, Mapping)
            else None
        )
        expected_instrument_hashes.add(
            attestation.get("instrument_spec_sha256")
            if isinstance(attestation, Mapping)
            else None
        )
    if (
        receipt.get("schema_version") != EXECUTION_SPEC_RECEIPT_SCHEMA
        or receipt_sha256 != _sha_json(body)
        or receipt.get("symbol") != frozen.symbol
        or spec.get("symbol") != frozen.symbol
        or receipt.get("authority_timing") != "FROZEN_BEFORE_FIRST_SEARCH_EVALUATION"
        or receipt.get("native_prefix_index_sha256")
        != cross_binding.get("native_prefix_index_sha256")
        or receipt.get("tick_value_source")
        not in {
            "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT",
            "FROZEN_BROKER_SPECS_RECORD_NATIVE_ZERO_UNAVAILABLE",
        }
        or receipt.get("volume_source") != "FROZEN_BROKER_SPECS_RECORD"
        or receipt.get("instrument_spec_sha256")
        != _backtester_receipt_sha256(
            {
                "schema_version": BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
                "instrument_spec": dict(spec),
            }
        )
        or expected_instrument_hashes != {receipt.get("instrument_spec_sha256")}
        or receipt.get("grants_search_authority") is not False
        or receipt.get("grants_trading_authority") is not False
        or type(receipt.get("deployment_config_size_bytes")) is not int
        or not 1 <= receipt["deployment_config_size_bytes"] <= 32 * 1024**2
        or type(receipt.get("broker_specs_size_bytes")) is not int
        or not 1 <= receipt["broker_specs_size_bytes"] <= 64 * 1024**2
    ):
        _fail("EXECUTION_SPEC_RECEIPT_BINDING_INVALID")
    for name in (
        "deployment_scope",
        "broker_specs_relative_path",
        "broker_record_key",
    ):
        _identity(receipt.get(name), f"EXECUTION_SPEC_{name.upper()}_INVALID", 4096)
    for name in (
        "deployment_config_sha256",
        "broker_specs_sha256",
        "broker_record_sha256",
        "native_prefix_index_sha256",
        "native_provenance_sha256",
        "native_instrument_snapshot_sha256",
        "instrument_spec_sha256",
        "receipt_sha256",
    ):
        _hex(receipt.get(name), 64, f"EXECUTION_SPEC_{name.upper()}_INVALID")
    return receipt


def _hex(value: Any, length: int, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != length
        or any(character not in _HEX for character in value)
    ):
        _fail(code)
    return value


def _identity(value: Any, code: str, maximum: int = 128) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail(code)
    if len(value.encode("utf-8")) > maximum or any(ord(char) < 32 for char in value):
        _fail(code)
    return value


def _utc(value: Any, code: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WinnerRawEvidenceError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _day(value: Any, code: str) -> date:
    if type(value) is not str:
        _fail(code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise WinnerRawEvidenceError(code) from exc
    if parsed.isoformat() != value:
        _fail(code)
    return parsed


def _put_ascii_json(
    sink: SymbolAuthoritySink,
    artifact_id: str,
    artifact_type: str,
    value: Any,
) -> dict[str, Any]:
    encoded = _canonical_bytes(value)
    entry = sink.put_opaque(
        artifact_id,
        artifact_type,
        (encoded,),
        format=ASCII_JSON_FORMAT,
    )
    descriptor = entry["descriptor"]
    if descriptor["sha256"] != hashlib.sha256(encoded).hexdigest():
        _fail("SPOOL_CANONICAL_HASH_MISMATCH")
    return entry


def build_frozen_numerical_execution_cost_policy(
    *,
    lane: str,
    use_spread: bool,
    spread_mode: str,
    instrument_spec_sha256: str,
) -> dict[str, Any]:
    """Freeze only invariant numerical-cost facts, never partition arrays.

    Backtester spread arrays are causal-partition-local.  Their exact hash belongs
    in each numerical execution-cost attestation, not in the pre-search policy.
    This policy is intentionally constructible from broker/config/method facts
    before any train, tuning, or lockbox result is evaluated.
    """

    normalized_lane = str(lane or "").strip().upper()
    if normalized_lane not in {"BASE", "STRESS"}:
        _fail("COST_POLICY_LANE_INVALID")
    try:
        # The TradePower builder is the canonical owner of policy
        # canonicalization.  The dummy vector hash cannot enter policy_sha256.
        reference = build_numerical_execution_cost_attestation(
            use_spread=use_spread,
            spread_mode=spread_mode,
            spread_values_sha256="0" * 64,
            instrument_spec_sha256=instrument_spec_sha256,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError("COST_POLICY_FACTS_INVALID") from exc
    if reference["use_spread"] is not True:
        _fail("COST_POLICY_COMPONENT_DISABLED")
    body = {
        "schema_version": FROZEN_COST_POLICY_SCHEMA,
        "lane": normalized_lane,
        "attestation_schema_version": (NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION),
        "use_spread": reference["use_spread"],
        "spread_mode": reference["spread_mode"],
        "instrument_spec_sha256": reference["instrument_spec_sha256"],
        "numerical_execution_cost_policy_sha256": reference["policy_sha256"],
    }
    return {**body, "policy_identity_sha256": _sha_json(body)}


def _validated_frozen_cost_policy(
    value: Any,
    *,
    lane: str,
    code: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _FROZEN_COST_POLICY_FIELDS:
        _fail(code)
    policy = dict(value)
    if (
        policy.get("schema_version") != FROZEN_COST_POLICY_SCHEMA
        or policy.get("lane") != lane
        or policy.get("attestation_schema_version")
        != NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION
    ):
        _fail(code)
    try:
        rebuilt = build_frozen_numerical_execution_cost_policy(
            lane=lane,
            use_spread=policy.get("use_spread"),
            spread_mode=policy.get("spread_mode"),
            instrument_spec_sha256=policy.get("instrument_spec_sha256"),
        )
    except WinnerRawEvidenceError as exc:
        raise WinnerRawEvidenceError(code) from exc
    if rebuilt != policy:
        _fail(code)
    return rebuilt


def _validated_partition_cost_attestation(
    value: Any,
    *,
    policy: Mapping[str, Any],
    lane: str,
    code: str,
) -> dict[str, Any]:
    """Rebuild one exact Backtester attestation and bind it to frozen policy."""

    if not isinstance(value, Mapping):
        _fail(code)
    attestation = dict(value)
    try:
        rebuilt = build_numerical_execution_cost_attestation(
            use_spread=attestation.get("use_spread"),
            spread_mode=attestation.get("spread_mode"),
            spread_values_sha256=attestation.get("spread_values_sha256"),
            instrument_spec_sha256=attestation.get("instrument_spec_sha256"),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError(code) from exc
    if rebuilt != attestation:
        _fail(code)
    derived_policy = build_frozen_numerical_execution_cost_policy(
        lane=lane,
        use_spread=rebuilt["use_spread"],
        spread_mode=rebuilt["spread_mode"],
        instrument_spec_sha256=rebuilt["instrument_spec_sha256"],
    )
    if derived_policy != dict(policy):
        _fail(f"{code}_POLICY_DRIFT")
    return rebuilt


def build_precommitted_risk_unit_contract() -> dict[str, Any]:
    """Return the only daily/trade R accounting contract accepted here."""
    return build_backtest_winner_risk_unit_contract()


def _source_contract_from_result(result: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(result)
    swap = dict(payload.get("swap_accrual_contract") or {})
    source_binding = {
        name: swap.get(name)
        for name in (
            "price_activation_manifest_sha256",
            "terms_activation_manifest_sha256",
            "terms_snapshot_sha256",
            "schedule_sha256",
            "target_bars_sha256",
            "target_sidecar_sha256",
        )
    }
    for name in ("historical_rates_sha256", "broker_disabled_proof_sha256"):
        if swap.get(name) not in (None, ""):
            source_binding[name] = swap[name]
    if "broker_swap_disabled" not in swap:
        swap["broker_swap_disabled"] = bool(
            swap.get("broker_swap_disabled_authenticated", False)
        )
    return {
        "schema_version": TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
        "source_binding": source_binding,
        "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
        "execution_cost_timing_contract": dict(
            payload.get("execution_cost_timing_contract") or {}
        ),
        "numerical_execution_cost_attestation": dict(
            payload.get("numerical_execution_cost_attestation") or {}
        ),
        "swap_accrual_contract": swap,
    }


def build_trade_source_contract_from_backtester_result(
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose the exact search-time Backtester-to-TradePower boundary."""

    if not isinstance(result, Mapping):
        _fail("BACKTESTER_RESULT_INVALID")
    contract = _source_contract_from_result(result)
    _validated_source_identity(contract)
    return contract


def _validated_source_identity(source_contract: Mapping[str, Any]) -> str:
    sample = build_trade_sample_evidence(
        [],
        source_contract=dict(source_contract),
        minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    if sample.source_contract is None:
        _fail("SOURCE_CONTRACT_INVALID", ",".join(sample.reason_codes))
    return sample.source_contract.source_contract_sha256


def _normalized_horizon_execution_contracts(
    raw: Sequence[Mapping[str, Any]],
    *,
    expected_count: int,
) -> list[dict[str, Any]]:
    if (
        not isinstance(raw, (list, tuple))
        or type(expected_count) is not int
        or not 1 <= expected_count <= winner.MAX_ATTEMPTS
        or len(raw) != expected_count
    ):
        _fail("HORIZON_EXECUTION_CONTRACT_ROSTER_INVALID")
    result: list[dict[str, Any]] = []
    for ordinal, value in enumerate(raw):
        if not isinstance(value, Mapping) or set(value) != (
            _HORIZON_EXECUTION_CONTRACT_KEYS
        ):
            _fail(f"HORIZON_EXECUTION_CONTRACT_{ordinal:04d}_SCHEMA_INVALID")
        kind = value.get("lookback_kind")
        lookback = value.get("lookback_value")
        count = value.get("source_bar_count")
        if (
            type(kind) is not str
            or kind not in _HORIZON_LOOKBACK_KINDS
            or type(lookback) is not int
            or not 1 <= lookback <= 10_000_000_000
            or type(count) is not int
            or not 2 <= count <= 10_000_000
            or (kind == "NATIVE_OBSERVATIONS" and lookback != count)
        ):
            _fail(f"HORIZON_EXECUTION_CONTRACT_{ordinal:04d}_VALUE_INVALID")
        result.append(
            {
                "lookback_kind": kind,
                "lookback_value": lookback,
                "source_bar_count": count,
            }
        )
    return result


def _verified_attempt_artifacts(
    bodies: Mapping[str, dict[str, Any]],
    label: str,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    horizon_execution_contract: Mapping[str, Any],
) -> refit_artifacts.CurrentRefitSearchArtifacts:
    """Authenticate the four executable bodies via the accepted v2 verifier.

    The verifier reconstructs the whole chain from the live strategy registry
    and the independently supplied cell, so invented strategy classes, forged
    code/rule pins, caller-chosen candidate ids, off-grid or non-default
    parameters all fail here — nothing is taken from declared semantics.
    """
    process = bodies["winner_process"]
    horizon = bodies["horizon_process"]
    recipe = bodies["refit_recipe"]
    try:
        encoded = {
            name: refit_artifacts.canonical_artifact_bytes(body)
            for name, body in bodies.items()
        }
        return refit_artifacts.verify_current_refit_search_artifacts_bytes_v2(
            winner_process_bytes=encoded["winner_process"],
            horizon_process_bytes=encoded["horizon_process"],
            trial_definition_bytes=encoded["trial_definition"],
            refit_recipe_bytes=encoded["refit_recipe"],
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_id=process.get("strategy_id"),
            lookback_kind=horizon_execution_contract.get("lookback_kind"),
            lookback_value=horizon_execution_contract.get("lookback_value"),
            fit_start_utc=horizon.get("fit_start_utc"),
            fit_end_utc=horizon.get("fit_end_utc"),
            latest_closed_bar_utc=horizon.get("latest_closed_bar_utc"),
            source_snapshot_sha256=horizon.get("source_snapshot_sha256"),
            source_bar_count=horizon_execution_contract.get("source_bar_count"),
            seed=recipe.get("seed"),
        )
    except refit_artifacts.CurrentRefitArtifactError as exc:
        raise WinnerRawEvidenceError(
            f"{label}_ARTIFACT_UNVERIFIABLE", str(exc)
        ) from exc
    except (TypeError, ValueError, KeyError) as exc:
        raise WinnerRawEvidenceError(
            f"{label}_ARTIFACT_UNVERIFIABLE", type(exc).__name__
        ) from exc


def _normalize_attempt_artifacts(
    raw_attempts: Sequence[Mapping[str, Any]],
    *,
    precommitted_evidence_roles: Sequence[str],
    horizon_execution_contracts: Sequence[Mapping[str, Any]],
    common_hashes: Mapping[str, str],
    symbol: str,
    timeframe: str,
    regime: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(raw_attempts, (list, tuple)) or not (
        1 <= len(raw_attempts) <= winner.MAX_ATTEMPTS
    ):
        _fail("ATTEMPT_FAMILY_BOUNDS")
    attempts: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    seen_attempts: set[str] = set()
    if len(horizon_execution_contracts) != len(raw_attempts):
        _fail("HORIZON_EXECUTION_CONTRACT_ROSTER_INVALID")
    if (
        not isinstance(precommitted_evidence_roles, (list, tuple))
        or len(precommitted_evidence_roles) != len(raw_attempts)
        or any(
            type(role) is not str or role not in _PRECOMMITTED_EVIDENCE_ROLES
            for role in precommitted_evidence_roles
        )
    ):
        _fail("PRESEARCH_EVIDENCE_ROLE_ROSTER_INVALID")
    for ordinal, (raw, horizon_contract, evidence_role) in enumerate(
        zip(
            raw_attempts,
            horizon_execution_contracts,
            precommitted_evidence_roles,
            strict=True,
        )
    ):
        label = f"ATTEMPT_{ordinal:04d}"
        if (
            not isinstance(raw, Mapping)
            or set(raw) != _ATTEMPT_ARTIFACT_KEYS
            or any(not isinstance(raw[name], Mapping) for name in raw)
        ):
            _fail(f"{label}_ARTIFACT_SCHEMA_INVALID")
        bodies = {name: dict(raw[name]) for name in _ATTEMPT_ARTIFACT_KEYS}
        verified = _verified_attempt_artifacts(
            bodies,
            label,
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            horizon_execution_contract=horizon_contract,
        )
        hashes = {
            "winner_process": verified.winner_process_sha256,
            "horizon_process": verified.horizon_process_sha256,
            "trial_definition": verified.trial_definition_sha256,
            "refit_recipe": verified.refit_recipe_sha256,
        }
        candidate_id = verified.process_candidate_id
        strategy_id = verified.winner_process["strategy_id"]
        strategy_family = verified.winner_process["strategy_family"]
        parameter_count = verified.trial_definition["parameter_count"]
        attempt_id = _sha1_json(
            {
                "schema_version": "fxpm.winner-attempt-identity.v1",
                "ordinal": ordinal,
                "process_candidate_id": candidate_id,
                "process_sha256": hashes["winner_process"],
                "horizon_process_sha256": hashes["horizon_process"],
                "trial_definition_sha256": hashes["trial_definition"],
                "refit_recipe_sha256": hashes["refit_recipe"],
                "horizon_execution_contract_sha256": _sha_json(horizon_contract),
                "precommitted_evidence_role": evidence_role,
                **common_hashes,
            }
        )
        if candidate_id in seen_candidates or attempt_id in seen_attempts:
            _fail(f"ATTEMPT_{ordinal:04d}_IDENTITY_DUPLICATED")
        seen_candidates.add(candidate_id)
        seen_attempts.add(attempt_id)
        attempts.append(
            {
                "ordinal": ordinal,
                "attempt_id": attempt_id,
                "process_candidate_id": candidate_id,
                "strategy_family": strategy_family,
                "strategy_id": strategy_id,
                "parameter_count": parameter_count,
                "precommitted_evidence_role": evidence_role,
                "process_sha256": hashes["winner_process"],
                "horizon_process_sha256": hashes["horizon_process"],
                "trial_definition_sha256": hashes["trial_definition"],
                "refit_recipe_sha256": hashes["refit_recipe"],
                **common_hashes,
            }
        )
        artifacts.append(bodies)
    return attempts, artifacts


def _process_registry(attempts: Sequence[Mapping[str, Any]]) -> str:
    return _sha_json(
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


def _horizon_registry(attempts: Sequence[Mapping[str, Any]]) -> str:
    return _sha_json(
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


def _trial_registry(attempts: Sequence[Mapping[str, Any]]) -> str:
    return _sha_json(
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


def _normalize_fold_plan(
    raw_plan: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(raw_plan, (list, tuple)) or not (
        int(winner.PINNED_SELECTION_POLICY["min_causal_folds"])
        <= len(raw_plan)
        <= winner.MAX_FOLDS
    ):
        _fail("FOLD_PLAN_BOUNDS")
    normalized: list[dict[str, Any]] = []
    prior_end: Optional[date] = None
    for ordinal, raw in enumerate(raw_plan):
        if not isinstance(raw, Mapping) or set(raw) != {
            "anchor_as_of_utc",
            "oos_start_date_utc",
            "oos_end_date_utc",
        }:
            _fail(f"FOLD_PLAN_{ordinal:03d}_SCHEMA_INVALID")
        start = _day(
            raw["oos_start_date_utc"], f"FOLD_PLAN_{ordinal:03d}_START_INVALID"
        )
        end = _day(raw["oos_end_date_utc"], f"FOLD_PLAN_{ordinal:03d}_END_INVALID")
        anchor = _utc(
            raw["anchor_as_of_utc"], f"FOLD_PLAN_{ordinal:03d}_ANCHOR_INVALID"
        )
        start_boundary = datetime.combine(
            start, datetime.min.time(), tzinfo=timezone.utc
        )
        if datetime.fromisoformat(anchor.replace("Z", "+00:00")) != start_boundary:
            _fail(f"FOLD_PLAN_{ordinal:03d}_ANCHOR_MISMATCH")
        span = (end - start).days + 1
        if not (
            int(winner.PINNED_SELECTION_POLICY["min_days_per_fold"])
            <= span
            <= winner.MAX_DAYS_PER_FOLD
        ):
            _fail(f"FOLD_PLAN_{ordinal:03d}_SPAN_INVALID")
        if prior_end is not None and start <= prior_end:
            _fail("FOLD_PLAN_INTERVALS_OVERLAP")
        fold_id = _sha1_json(
            {
                "schema_version": "fxpm.winner-fold-identity.v1",
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
                "fold_ordinal": ordinal,
                "anchor_as_of_utc": anchor,
                "oos_start_date_utc": start.isoformat(),
                "oos_end_date_utc": end.isoformat(),
            }
        )
        normalized.append(
            {
                "fold_ordinal": ordinal,
                "fold_id": fold_id,
                "anchor_as_of_utc": anchor,
                "oos_start_date_utc": start.isoformat(),
                "oos_end_date_utc": end.isoformat(),
            }
        )
        prior_end = end
    latest = datetime.combine(
        _day(normalized[-1]["oos_end_date_utc"], "FOLD_PLAN_END_INVALID")
        + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return normalized, latest.isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalized_cost_stress_adversity_plan(
    value: Any,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    attempts: Sequence[Mapping[str, Any]],
    folds: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    expected_body_keys = {
        "schema_version",
        "symbol",
        "timeframe",
        "regime",
        "row_count",
        "rows_sha256",
        "rows",
    }
    expected_row_keys = {
        "attempt_ordinal",
        "horizon_attempt_id",
        "fold_ordinal",
        "fold_id",
        "final_partition_geometry_receipt_sha256",
        "final_partition_frame_sha256",
        "base_cost_attestation_sha256",
        "stress_cost_attestation_sha256",
        "base_spread_values_sha256",
        "stress_spread_values_sha256",
        "expected_adversity_status",
    }
    if not isinstance(value, Mapping) or set(value) != expected_body_keys:
        _fail("COST_STRESS_ADVERSITY_PLAN_SCHEMA_INVALID")
    if (
        not isinstance(attempts, (list, tuple))
        or not isinstance(folds, (list, tuple))
        or any(not isinstance(item, Mapping) for item in attempts)
        or any(not isinstance(item, Mapping) for item in folds)
    ):
        _fail("COST_STRESS_ADVERSITY_PLAN_FROZEN_ROSTER_INVALID")
    rows = value.get("rows")
    expected_pairs = [
        (attempt, fold)
        for attempt in attempts
        if attempt.get("precommitted_evidence_role") == "EVALUATION_REQUIRED"
        for fold in folds
    ]
    if (
        value.get("schema_version") != COST_STRESS_ADVERSITY_PLAN_SCHEMA
        or value.get("symbol") != symbol
        or value.get("timeframe") != timeframe
        or value.get("regime") != regime
        or type(value.get("row_count")) is not int
        or value.get("row_count") != len(expected_pairs)
        or type(rows) is not list
        or len(rows) != len(expected_pairs)
    ):
        _fail("COST_STRESS_ADVERSITY_PLAN_BINDING_INVALID")

    normalized_rows: list[dict[str, Any]] = []
    horizon_attempt_ids: dict[int, str] = {}
    for row_ordinal, (raw, (attempt, fold)) in enumerate(
        zip(rows, expected_pairs, strict=True)
    ):
        label = f"COST_STRESS_ADVERSITY_PLAN_ROW_{row_ordinal:04d}"
        if not isinstance(raw, Mapping) or set(raw) != expected_row_keys:
            _fail(f"{label}_SCHEMA_INVALID")
        row = dict(raw)
        attempt_ordinal = attempt.get("ordinal")
        fold_ordinal = fold.get("fold_ordinal")
        if (
            row.get("attempt_ordinal") != attempt_ordinal
            or row.get("fold_ordinal") != fold_ordinal
            or row.get("fold_id") != fold.get("fold_id")
        ):
            _fail(f"{label}_ROSTER_MISMATCH")
        horizon_attempt_id = _identity(
            row.get("horizon_attempt_id"),
            f"{label}_HORIZON_ATTEMPT_ID_INVALID",
            256,
        )
        prior_horizon_attempt_id = horizon_attempt_ids.setdefault(
            int(attempt_ordinal), horizon_attempt_id
        )
        if prior_horizon_attempt_id != horizon_attempt_id:
            _fail(f"{label}_HORIZON_ATTEMPT_ID_DRIFT")
        for name in (
            "final_partition_geometry_receipt_sha256",
            "final_partition_frame_sha256",
            "base_cost_attestation_sha256",
            "stress_cost_attestation_sha256",
            "base_spread_values_sha256",
            "stress_spread_values_sha256",
        ):
            _hex(row.get(name), 64, f"{label}_{name.upper()}_INVALID")
        if (
            row["base_cost_attestation_sha256"]
            == row["stress_cost_attestation_sha256"]
        ):
            _fail(f"{label}_COST_ATTESTATION_NOT_DISTINCT")
        expected_status = (
            COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK
            if row["base_spread_values_sha256"]
            == row["stress_spread_values_sha256"]
            else COST_STRESS_ADVERSITY_EXPECTED_STRICT
        )
        if row.get("expected_adversity_status") != expected_status:
            _fail(f"{label}_EXPECTED_STATUS_INVALID")
        normalized_rows.append(row)

    normalized = {
        "schema_version": COST_STRESS_ADVERSITY_PLAN_SCHEMA,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "row_count": len(normalized_rows),
        "rows_sha256": _sha_json(normalized_rows),
        "rows": normalized_rows,
    }
    if (
        value.get("rows_sha256") != normalized["rows_sha256"]
        or _canonical_bytes(dict(value)) != _canonical_bytes(normalized)
    ):
        _fail("COST_STRESS_ADVERSITY_PLAN_HASH_INVALID")
    return normalized


def freeze_winner_search_plan_v3(
    sink: SymbolAuthoritySink,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    precommitted_at_utc: str,
    attempts_frozen_at_utc: str,
    fold_plan: Sequence[Mapping[str, Any]],
    attempt_artifacts: Sequence[Mapping[str, Any]],
    precommitted_evidence_roles: Sequence[str],
    horizon_execution_contracts: Sequence[Mapping[str, Any]],
    source_contract_seed: Mapping[str, Any],
    base_cost_contract: Mapping[str, Any],
    stress_cost_contract: Mapping[str, Any],
    risk_unit_contract: Mapping[str, Any],
    cost_stress_adversity_plan: Mapping[str, Any],
    artifact_namespace: Optional[str] = None,
) -> FrozenWinnerSearchPlan:
    """Freeze exact attempt/fold truth and persist it before evaluation.

    ``artifact_namespace`` (e.g. ``historical-anchor/{run_id}/{anchor_id}``)
    prefixes every spooled artifact id so one caller-owned sink can hold
    repeated anchors without collision; it is bound into the frozen receipt
    and production fail-closes on any mismatch.  ``None`` is the current
    (non-anchor) lane and is bound explicitly as well.
    """

    if type(sink) is not SymbolAuthoritySink:
        _fail("AUTHORITY_SINK_INVALID")
    namespace = _validated_namespace(artifact_namespace)
    normalized_symbol = _identity(symbol, "SYMBOL_INVALID", 64)
    normalized_timeframe = _identity(timeframe, "TIMEFRAME_INVALID", 16).upper()
    normalized_regime = _identity(regime, "REGIME_INVALID", 32).upper()
    if sink.symbol != normalized_symbol:
        _fail("AUTHORITY_SINK_SYMBOL_MISMATCH")
    if normalized_timeframe not in winner.TIMEFRAMES:
        _fail("TIMEFRAME_INVALID")
    if normalized_regime not in winner.REGIMES:
        _fail("REGIME_INVALID")
    if not isinstance(attempt_artifacts, (list, tuple)) or not (
        1 <= len(attempt_artifacts) <= winner.MAX_ATTEMPTS
    ):
        _fail("ATTEMPT_FAMILY_BOUNDS")
    normalized_horizon_contracts = _normalized_horizon_execution_contracts(
        horizon_execution_contracts,
        expected_count=len(attempt_artifacts),
    )
    horizon_contracts_sha256 = _sha_json(normalized_horizon_contracts)
    precommitted_at = _utc(precommitted_at_utc, "PRECOMMITTED_AT_INVALID")
    frozen_at = _utc(attempts_frozen_at_utc, "ATTEMPTS_FROZEN_AT_INVALID")
    if datetime.fromisoformat(
        frozen_at.replace("Z", "+00:00")
    ) <= datetime.fromisoformat(precommitted_at.replace("Z", "+00:00")):
        _fail("ATTEMPTS_NOT_FROZEN_AFTER_PRECOMMITMENT")

    source_seed = dict(source_contract_seed)
    base_cost = _validated_frozen_cost_policy(
        base_cost_contract,
        lane="BASE",
        code="BASE_COST_POLICY_INVALID",
    )
    stress_cost = _validated_frozen_cost_policy(
        stress_cost_contract,
        lane="STRESS",
        code="STRESS_COST_POLICY_INVALID",
    )
    risk_contract = dict(risk_unit_contract)
    if risk_contract != build_precommitted_risk_unit_contract():
        _fail("RISK_UNIT_CONTRACT_INVALID")
    _validated_partition_cost_attestation(
        source_seed.get("numerical_execution_cost_attestation"),
        policy=base_cost,
        lane="BASE",
        code="BASE_COST_SOURCE_SEED_INVALID",
    )
    source_sha = _validated_source_identity(source_seed)
    common_hashes = {
        "source_contract_sha256": source_sha,
        "cost_contract_sha256": _sha_json(base_cost),
        "cost_stress_contract_sha256": _sha_json(stress_cost),
        "risk_unit_contract_sha256": _sha_json(risk_contract),
    }
    if (
        base_cost["numerical_execution_cost_policy_sha256"]
        == stress_cost["numerical_execution_cost_policy_sha256"]
    ):
        _fail("COST_STRESS_POLICY_NOT_DISTINCT")

    attempts, raw_artifacts = _normalize_attempt_artifacts(
        attempt_artifacts,
        precommitted_evidence_roles=precommitted_evidence_roles,
        horizon_execution_contracts=normalized_horizon_contracts,
        common_hashes=common_hashes,
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
    )
    normalized_folds, latest_boundary = _normalize_fold_plan(
        fold_plan,
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
    )
    normalized_adversity_plan = _normalized_cost_stress_adversity_plan(
        cost_stress_adversity_plan,
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
        attempts=attempts,
        folds=normalized_folds,
    )
    adversity_plan_sha256 = _sha_json(normalized_adversity_plan)
    registries = {
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    run_id = _sha_json(
        {
            "schema_version": FROZEN_PLAN_SCHEMA,
            "generation_context_sha256": sink.context.context_sha256,
            "artifact_namespace": namespace,
            "precommitted_at_utc": precommitted_at,
            "symbol": normalized_symbol,
            "timeframe": normalized_timeframe,
            "regime": normalized_regime,
            "attempts": attempts,
            "fold_plan": normalized_folds,
            "horizon_execution_contracts_sha256": horizon_contracts_sha256,
            "cost_stress_adversity_plan_sha256": adversity_plan_sha256,
            **common_hashes,
            **registries,
        }
    )
    cell_id = winner.compute_cell_id(
        normalized_symbol, normalized_timeframe, normalized_regime
    )
    if cell_id is None:
        _fail("CELL_ID_INVALID")
    common = {
        "cell_id": cell_id,
        "symbol": normalized_symbol,
        "timeframe": normalized_timeframe,
        "regime": normalized_regime,
        **common_hashes,
        **registries,
    }
    precommitment = {
        "contract_version": winner.PRECOMMITMENT_VERSION,
        "run_id": run_id,
        "precommitted_at_utc": precommitted_at,
        **common,
        "selection_policy_sha256": winner.PINNED_SELECTION_POLICY_SHA256,
        "planned_attempt_count": len(attempts),
        "planned_fold_count": len(normalized_folds),
        "causal_fold_plan_sha256": _sha_json(normalized_folds),
        "causal_fold_plan": normalized_folds,
        "latest_evidence_boundary_utc": latest_boundary,
    }
    precommitment_sha = _sha_json(precommitment)
    attempt_ledger = {
        "contract_version": winner.ATTEMPT_LEDGER_VERSION,
        "run_id": run_id,
        "frozen_at_utc": frozen_at,
        "precommitment_payload_sha256": precommitment_sha,
        **common,
        "attempt_count": len(attempts),
        "attempts_sha256": _sha_json(attempts),
        "attempts": attempts,
    }
    multiplicity = build_trade_power_multiplicity_receipt(attempt_ledger)

    prefix = _artifact_prefix(namespace, normalized_timeframe, normalized_regime)
    spool_entries: dict[str, dict[str, Any]] = {}
    bodies = {
        "precommitment": ("winner_precommitment", precommitment),
        "attempt_ledger": ("winner_attempt_ledger", attempt_ledger),
        "source_contract_seed": ("winner_source_contract_seed", source_seed),
        "base_cost_contract": ("winner_base_cost_contract", base_cost),
        "stress_cost_contract": ("winner_stress_cost_contract", stress_cost),
        "risk_unit_contract": ("winner_risk_unit_contract", risk_contract),
        "cost_stress_adversity_plan": (
            "winner_cost_stress_adversity_plan",
            normalized_adversity_plan,
        ),
        "horizon_execution_contracts": (
            "winner_horizon_execution_contracts",
            normalized_horizon_contracts,
        ),
        "trade_power_policy": (
            "winner_trade_power_policy",
            asdict(PRODUCTION_TRADE_POWER_POLICY),
        ),
        "multiplicity_receipt": ("winner_trade_power_multiplicity", multiplicity),
    }
    for name, (artifact_type, body) in bodies.items():
        spool_entries[name] = _put_ascii_json(
            sink, f"{prefix}/frozen/{name}", artifact_type, body
        )
    for attempt, artifacts in zip(attempts, raw_artifacts, strict=True):
        for name, body in artifacts.items():
            key = f"attempt-{attempt['ordinal']:04d}-{name}"
            spool_entries[key] = _put_ascii_json(
                sink,
                f"{prefix}/frozen/attempt-{attempt['ordinal']:04d}/{name}",
                f"winner_attempt_{name}",
                body,
            )
    receipt_body = {
        "schema_version": FROZEN_PLAN_SCHEMA,
        "generation_id": sink.context.generation_id,
        "generation_context_sha256": sink.context.context_sha256,
        "native_universe_sha256": sink.context.native_universe_sha256,
        "config_sha256": sink.context.config_sha256,
        "artifact_namespace": namespace,
        "symbol": normalized_symbol,
        "timeframe": normalized_timeframe,
        "regime": normalized_regime,
        "run_id": run_id,
        "precommitment_sha256": precommitment_sha,
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "attempts_sha256": attempt_ledger["attempts_sha256"],
        "attempt_count": len(attempts),
        "fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
        "fold_count": len(normalized_folds),
        "horizon_execution_contracts_sha256": horizon_contracts_sha256,
        "cost_stress_adversity_plan_sha256": adversity_plan_sha256,
        "multiplicity_receipt_sha256": multiplicity["receipt_sha256"],
        **registries,
        "artifact_roster": {
            key: entry["descriptor"] for key, entry in sorted(spool_entries.items())
        },
    }
    receipt_bytes = _canonical_bytes(receipt_body)
    receipt_entry = _put_ascii_json(
        sink,
        f"{prefix}/frozen/receipt",
        "winner_frozen_search_plan_receipt",
        receipt_body,
    )
    if (
        receipt_entry.get("artifact_id") != f"{prefix}/frozen/receipt"
        or receipt_entry["descriptor"]["sha256"]
        != hashlib.sha256(receipt_bytes).hexdigest()
    ):
        _fail("FROZEN_RECEIPT_ENTRY_BINDING_INVALID")
    attempt_bundle_bytes = _canonical_bytes(raw_artifacts)
    if len(attempt_bundle_bytes) > winner.MAX_REQUEST_BYTES:
        _fail("ATTEMPT_ARTIFACT_BUNDLE_BYTE_BOUND_EXCEEDED")
    horizon_contracts_bytes = _canonical_bytes(normalized_horizon_contracts)
    if len(horizon_contracts_bytes) > winner.MAX_REQUEST_BYTES:
        _fail("HORIZON_EXECUTION_CONTRACT_BYTE_BOUND_EXCEEDED")
    adversity_plan_bytes = _canonical_bytes(normalized_adversity_plan)
    if len(adversity_plan_bytes) > winner.MAX_REQUEST_BYTES:
        _fail("COST_STRESS_ADVERSITY_PLAN_BYTE_BOUND_EXCEEDED")
    return FrozenWinnerSearchPlan(
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
        artifact_namespace=namespace,
        generation_context_sha256=sink.context.context_sha256,
        precommitment_bytes=_canonical_bytes(precommitment),
        attempt_ledger_bytes=_canonical_bytes(attempt_ledger),
        source_contract_seed_bytes=_canonical_bytes(source_seed),
        base_cost_contract_bytes=_canonical_bytes(base_cost),
        stress_cost_contract_bytes=_canonical_bytes(stress_cost),
        risk_unit_contract_bytes=_canonical_bytes(risk_contract),
        cost_stress_adversity_plan_bytes=adversity_plan_bytes,
        frozen_receipt_bytes=receipt_bytes,
        frozen_receipt_entry_bytes=_canonical_bytes(receipt_entry),
        horizon_execution_contracts_bytes=horizon_contracts_bytes,
        attempt_artifacts_bundle_bytes=attempt_bundle_bytes,
    )


def build_flat_daily_r_surface(
    position_events: Sequence[Mapping[str, Any]],
    *,
    start_date_utc: str,
    end_date_utc: str,
    risk_unit_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Build exact daily MTM only when every position is flat within its day."""

    risk_contract = dict(risk_unit_contract)
    if risk_contract != build_precommitted_risk_unit_contract():
        _fail("RISK_UNIT_CONTRACT_INVALID")
    start = _day(start_date_utc, "DAILY_SURFACE_START_INVALID")
    end = _day(end_date_utc, "DAILY_SURFACE_END_INVALID")
    if end < start:
        _fail("DAILY_SURFACE_INTERVAL_INVALID")
    if not isinstance(position_events, (list, tuple)):
        _fail("POSITION_EVENTS_INVALID")
    values = {
        start + timedelta(days=offset): Decimal(0)
        for offset in range((end - start).days + 1)
    }
    event_r: list[Decimal] = []
    for ordinal, event in enumerate(position_events):
        if not isinstance(event, Mapping):
            _fail(f"POSITION_EVENT_{ordinal:05d}_INVALID")
        if event.get("trade_record_schema_version") != REQUIRED_TRADE_RECORD_SCHEMA:
            _fail(f"POSITION_EVENT_{ordinal:05d}_SCHEMA_INVALID")
        try:
            opened = pd.Timestamp(event.get("entry_time"))
            closed = pd.Timestamp(event.get("exit_time"))
            pnl = Decimal(str(event.get("pnl_dollars")))
            risk = Decimal(str(event.get("risk_amount")))
        except (TypeError, ValueError, InvalidOperation) as exc:
            raise WinnerRawEvidenceError(
                f"POSITION_EVENT_{ordinal:05d}_VALUE_INVALID"
            ) from exc
        if opened.tzinfo is None or closed.tzinfo is None:
            _fail(f"POSITION_EVENT_{ordinal:05d}_TIMEZONE_INVALID")
        opened = opened.tz_convert("UTC")
        closed = closed.tz_convert("UTC")
        if opened.date() != closed.date():
            _fail("DAILY_R_REQUIRES_SEARCH_TIME_FULL_PATH_FOR_OVERNIGHT_POSITION")
        if (
            closed.date() not in values
            or risk <= 0
            or not pnl.is_finite()
            or not risk.is_finite()
        ):
            _fail(f"POSITION_EVENT_{ordinal:05d}_VALUE_INVALID")
        net_r = Decimal(str(canonical_backtest_winner_r(pnl, risk)))
        values[closed.date()] += net_r
        event_r.append(net_r)
    dates = [day.isoformat() for day in sorted(values)]
    daily_r = [float(values[day]) for day in sorted(values)]
    if sum((Decimal(str(value)) for value in daily_r), Decimal(0)) != sum(
        event_r, Decimal(0)
    ):
        _fail("DAILY_R_FLOAT_REPRESENTATION_BREAKS_ACCOUNTING")
    return {
        "schema_version": DAILY_R_SURFACE_SCHEMA,
        "status": "AVAILABLE",
        "origin": "backtester_search_time_flat_daily_positions",
        "daily_axis_kind": winner.PINNED_SELECTION_POLICY["daily_axis"],
        "daily_evidence_kind": winner.PINNED_SELECTION_POLICY["daily_evidence"],
        "daily_unit": winner.PINNED_SELECTION_POLICY["daily_unit"],
        "inventory_boundary_kind": winner.PINNED_SELECTION_POLICY["inventory_boundary"],
        "risk_unit_contract": risk_contract,
        "daily_dates_utc": dates,
        "daily_net_mtm_r": daily_r,
    }


def _validate_daily_surface(
    result: Mapping[str, Any],
    *,
    risk_contract: Mapping[str, Any],
    planned_fold: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str], list[float]]:
    raw = result.get("winner_daily_r_surface")
    expected_keys = {
        "schema_version",
        "status",
        "origin",
        "daily_axis_kind",
        "daily_evidence_kind",
        "daily_unit",
        "inventory_boundary_kind",
        "risk_unit_contract",
        "daily_dates_utc",
        "daily_net_mtm_r",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected_keys:
        _fail("WINNER_DAILY_R_SURFACE_MISSING_OR_INVALID")
    surface = dict(raw)
    if (
        surface["schema_version"] != DAILY_R_SURFACE_SCHEMA
        or surface["status"] != "AVAILABLE"
        or surface["origin"]
        not in {
            "backtester_search_time_full_path",
            "backtester_search_time_flat_daily_positions",
        }
        or surface["daily_axis_kind"] != winner.PINNED_SELECTION_POLICY["daily_axis"]
        or surface["daily_evidence_kind"]
        != winner.PINNED_SELECTION_POLICY["daily_evidence"]
        or surface["daily_unit"] != winner.PINNED_SELECTION_POLICY["daily_unit"]
        or surface["inventory_boundary_kind"]
        != winner.PINNED_SELECTION_POLICY["inventory_boundary"]
        or surface["risk_unit_contract"] != dict(risk_contract)
    ):
        _fail("WINNER_DAILY_R_SURFACE_CONTRACT_INVALID")
    start = _day(planned_fold["oos_start_date_utc"], "FOLD_START_INVALID")
    end = _day(planned_fold["oos_end_date_utc"], "FOLD_END_INVALID")
    expected_dates = [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]
    dates = surface["daily_dates_utc"]
    raw_values = surface["daily_net_mtm_r"]
    if (
        type(dates) is not list
        or dates != expected_dates
        or type(raw_values) is not list
    ):
        _fail("WINNER_DAILY_R_AXIS_INVALID")
    if len(raw_values) != len(dates):
        _fail("WINNER_DAILY_R_LENGTH_INVALID")
    values: list[float] = []
    for raw_value in raw_values:
        if type(raw_value) not in (int, float):
            _fail("WINNER_DAILY_R_VALUE_INVALID")
        value = float(raw_value)
        if not math.isfinite(value) or abs(value) > winner.MAX_ABS_DAILY_R:
            _fail("WINNER_DAILY_R_VALUE_INVALID")
        values.append(value)
    return surface, dates, values


def _partition_axis(result: Mapping[str, Any], partition: str) -> dict[str, Any]:
    axis = result.get("trade_power_partition_axis")
    expected = {
        "schema_version",
        "timeframe",
        "start_utc",
        "end_utc",
        "scored_bars",
        "warmup_bars",
        "horizon_receipt_sha256",
        "event_axis_sha256",
    }
    if not isinstance(axis, Mapping) or set(axis) != expected:
        _fail(f"{partition.upper()}_PARTITION_AXIS_MISSING")
    body = dict(axis)
    for name in ("horizon_receipt_sha256", "event_axis_sha256"):
        _hex(body.get(name), 64, f"{partition.upper()}_{name.upper()}_INVALID")
    _utc(str(body.get("start_utc")), f"{partition.upper()}_START_INVALID")
    _utc(str(body.get("end_utc")), f"{partition.upper()}_END_INVALID")
    if body.get("schema_version") != "fxpm.backtest-partition-axis.v1":
        _fail(f"{partition.upper()}_PARTITION_AXIS_SCHEMA_INVALID")
    if body.get("timeframe") not in winner.TIMEFRAMES:
        _fail(f"{partition.upper()}_PARTITION_AXIS_TIMEFRAME_INVALID")
    if (
        type(body.get("scored_bars")) is not int
        or body["scored_bars"] <= 0
        or type(body.get("warmup_bars")) is not int
        or body["warmup_bars"] < 0
    ):
        _fail(f"{partition.upper()}_PARTITION_AXIS_COUNT_INVALID")
    horizon_body = {
        name: value for name, value in body.items() if name != "horizon_receipt_sha256"
    }
    if body["horizon_receipt_sha256"] != _sha_json(horizon_body):
        _fail(f"{partition.upper()}_PARTITION_AXIS_HASH_INVALID")
    return body


def _decision_relevant_lane(
    result: Mapping[str, Any],
    *,
    lane: str,
) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        _fail(f"{lane.upper()}_RESULT_INVALID")
    events = result.get("position_events")
    if type(events) is not list:
        _fail(f"{lane.upper()}_POSITION_EVENTS_INVALID")
    return {
        "schema_version": RAW_LANE_SCHEMA,
        "lane": lane,
        "position_events": events,
        "winner_daily_r_surface": result.get("winner_daily_r_surface"),
        "trade_power_partition_axis": result.get("trade_power_partition_axis"),
        "execution_cost_timing_contract": result.get("execution_cost_timing_contract"),
        "numerical_execution_cost_attestation": result.get(
            "numerical_execution_cost_attestation"
        ),
        "swap_accrual_contract": result.get("swap_accrual_contract"),
    }


def _validate_base_result(
    result: Mapping[str, Any],
    *,
    frozen: FrozenWinnerSearchPlan,
    partition: str,
) -> dict[str, Any]:
    _validated_partition_cost_attestation(
        result.get("numerical_execution_cost_attestation"),
        policy=frozen.base_cost_policy,
        lane="BASE",
        code=f"{partition.upper()}_BASE_COST_ATTESTATION_INVALID",
    )
    source = _source_contract_from_result(result)
    observed_source_sha = _validated_source_identity(source)
    expected_source_sha = frozen.precommitment["source_contract_sha256"]
    if observed_source_sha != expected_source_sha:
        _fail(f"{partition.upper()}_SOURCE_CONTRACT_DRIFT")
    axis = _partition_axis(result, partition)
    if axis["timeframe"] != frozen.timeframe:
        _fail(f"{partition.upper()}_PARTITION_AXIS_TIMEFRAME_DRIFT")
    if type(result.get("position_events")) is not list:
        _fail(f"{partition.upper()}_POSITION_EVENTS_INVALID")
    return source


def _validate_stress_pair(
    base: Mapping[str, Any],
    stress: Mapping[str, Any],
    *,
    frozen: FrozenWinnerSearchPlan,
) -> None:
    _validated_partition_cost_attestation(
        base.get("numerical_execution_cost_attestation"),
        policy=frozen.base_cost_policy,
        lane="BASE",
        code="FINAL_LOCKBOX_BASE_COST_ATTESTATION_INVALID",
    )
    _validated_partition_cost_attestation(
        stress.get("numerical_execution_cost_attestation"),
        policy=frozen.stress_cost_policy,
        lane="STRESS",
        code="FINAL_LOCKBOX_STRESS_COST_ATTESTATION_INVALID",
    )
    base_contract = _source_contract_from_result(base)
    stress_contract = _source_contract_from_result(stress)
    for field in (
        "source_binding",
        "trade_record_schema_version",
        "execution_cost_timing_contract",
        "swap_accrual_contract",
    ):
        if base_contract.get(field) != stress_contract.get(field):
            _fail("FINAL_LOCKBOX_BASE_STRESS_SOURCE_DRIFT")
    if base.get("trade_power_partition_axis") != stress.get(
        "trade_power_partition_axis"
    ):
        _fail("FINAL_LOCKBOX_BASE_STRESS_AXIS_DRIFT")


def _spool_lane(
    sink: SymbolAuthoritySink,
    *,
    prefix: str,
    lane: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    return _put_ascii_json(
        sink,
        f"{prefix}/raw/{lane}",
        "winner_raw_backtester_lane",
        _decision_relevant_lane(result, lane=lane),
    )


def _power_chain(
    *,
    attempt_ledger: Mapping[str, Any],
    train: Mapping[str, Any],
    tuning: Mapping[str, Any],
    final: Mapping[str, Any],
    frozen: FrozenWinnerSearchPlan,
) -> tuple[
    TradePowerDecision, Optional[TradePowerDecision], Optional[TradePowerDecision]
]:
    multiplicity = build_trade_power_multiplicity_receipt(dict(attempt_ledger))
    train_axis = _partition_axis(train, "optimizer_train")
    train_contract = build_trade_power_partition_contract(
        partition="optimizer_train",
        start_utc=train_axis["start_utc"],
        end_utc=train_axis["end_utc"],
        horizon_receipt_sha256=train_axis["horizon_receipt_sha256"],
        event_axis_sha256=train_axis["event_axis_sha256"],
        multiplicity_receipt=multiplicity,
    )
    train_decision = calibrate_train_trade_power(
        train.get("position_events"),
        policy=PRODUCTION_TRADE_POWER_POLICY,
        multiplicity_receipt=multiplicity,
        partition_contract=train_contract,
        source_contract=_validate_base_result(
            train, frozen=frozen, partition="optimizer_train"
        ),
        minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    if not train_decision.passed:
        return train_decision, None, None
    requirement = train_decision.frozen_requirement
    if requirement is None:
        _fail("TRAIN_FROZEN_REQUIREMENT_MISSING")
    tuning_axis = _partition_axis(tuning, "tuning_validation")
    tuning_contract = build_trade_power_partition_contract(
        partition="tuning_validation",
        start_utc=tuning_axis["start_utc"],
        end_utc=tuning_axis["end_utc"],
        horizon_receipt_sha256=tuning_axis["horizon_receipt_sha256"],
        event_axis_sha256=tuning_axis["event_axis_sha256"],
        multiplicity_receipt=multiplicity,
        prior_partition_contract_sha256=train_contract["contract_sha256"],
    )
    tuning_decision = evaluate_trade_power_partition(
        tuning.get("position_events"),
        frozen_requirement=requirement,
        partition="tuning_validation",
        partition_contract=tuning_contract,
        prior_partition_contract=train_contract,
        source_contract=_validate_base_result(
            tuning, frozen=frozen, partition="tuning_validation"
        ),
        minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    if not tuning_decision.passed:
        return train_decision, tuning_decision, None
    final_axis = _partition_axis(final, "final_lockbox")
    final_contract = build_trade_power_partition_contract(
        partition="final_lockbox",
        start_utc=final_axis["start_utc"],
        end_utc=final_axis["end_utc"],
        horizon_receipt_sha256=final_axis["horizon_receipt_sha256"],
        event_axis_sha256=final_axis["event_axis_sha256"],
        multiplicity_receipt=multiplicity,
        prior_partition_contract_sha256=tuning_contract["contract_sha256"],
    )
    final_decision = evaluate_trade_power_partition(
        final.get("position_events"),
        frozen_requirement=requirement,
        partition="final_lockbox",
        partition_contract=final_contract,
        prior_partition_contract=tuning_contract,
        source_contract=_validate_base_result(
            final, frozen=frozen, partition="final_lockbox"
        ),
        minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    return train_decision, tuning_decision, final_decision


def _power_status(decision: TradePowerDecision) -> str:
    if decision.status == "PASS" and decision.outcome_kind is SearchOutcomeKind.VALID:
        return "AUTHENTICATED"
    if (
        decision.status == "EXPECTED_INELIGIBLE"
        and decision.outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    ):
        return "PROCESS_REJECTED"
    if (
        decision.status == "INFRA_FAILURE"
        and decision.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    ):
        return "TECHNICAL_FAILURE"
    _fail("TRADE_POWER_STATUS_VOCABULARY_INVALID")
    raise AssertionError


def _power_chain_artifact(
    decisions: Sequence[Optional[TradePowerDecision]],
) -> dict[str, Any]:
    bodies = [
        decision.to_dict() if decision is not None else None for decision in decisions
    ]
    return {
        "schema_version": POWER_CHAIN_SCHEMA,
        "optimizer_train": bodies[0],
        "tuning_validation": bodies[1],
        "final_lockbox": bodies[2],
    }


def _event_vectors(
    base_events: Any,
    stress_events: Any,
    *,
    fold_start: datetime,
    fold_end_exclusive: datetime,
) -> tuple[
    list[str], list[str], list[str], list[int], list[int], list[float], list[float]
]:
    if type(base_events) is not list or type(stress_events) is not list:
        _fail("FINAL_POSITION_EVENTS_INVALID")
    if len(base_events) != len(stress_events):
        _fail("FINAL_BASE_STRESS_TRADE_COUNT_MISMATCH")
    if len(base_events) > winner.MAX_TRADES_PER_FOLD:
        _fail("FINAL_TRADE_COUNT_LIMIT_EXCEEDED")
    opens: list[str] = []
    closes: list[str] = []
    ids: list[str] = []
    open_orders: list[int] = []
    close_orders: list[int] = []
    base_r: list[float] = []
    stress_r: list[float] = []
    for ordinal, (base, stress) in enumerate(
        zip(base_events, stress_events, strict=True)
    ):
        if not isinstance(base, Mapping) or not isinstance(stress, Mapping):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_INVALID")
        identity_fields = (
            "trade_record_schema_version",
            "position_id",
            "entry_time",
            "exit_time",
            "entry_event_order",
            "exit_event_order",
            "risk_amount",
        )
        if any(base.get(field) != stress.get(field) for field in identity_fields):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_STRESS_IDENTITY_DRIFT")
        if base.get("trade_record_schema_version") != REQUIRED_TRADE_RECORD_SCHEMA:
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_SCHEMA_INVALID")
        try:
            base_pnl = float(base.get("pnl_dollars"))
            stress_pnl = float(stress.get("pnl_dollars"))
            risk = float(base.get("risk_amount"))
            base_value = canonical_backtest_winner_r(base_pnl, risk)
            stress_value = canonical_backtest_winner_r(stress_pnl, risk)
        except (TypeError, ValueError, OverflowError) as exc:
            raise WinnerRawEvidenceError(
                f"FINAL_POSITION_EVENT_{ordinal:05d}_VALUE_INVALID"
            ) from exc
        if (
            not math.isfinite(base_pnl)
            or not math.isfinite(stress_pnl)
            or not math.isfinite(risk)
            or risk <= 0.0
            or stress_pnl > base_pnl + 1e-12
        ):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_VALUE_INVALID")
        if (
            not math.isfinite(base_value)
            or not math.isfinite(stress_value)
            or abs(base_value) > winner.MAX_ABS_TRADE_R
            or abs(stress_value) > winner.MAX_ABS_TRADE_R
        ):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_R_INVALID")
        ids.append(str(base.get("position_id") or ""))
        opens.append(_utc(str(base.get("entry_time")), "FINAL_ENTRY_TIME_INVALID"))
        closes.append(_utc(str(base.get("exit_time")), "FINAL_EXIT_TIME_INVALID"))
        open_order = base.get("entry_event_order")
        close_order = base.get("exit_event_order")
        if (
            type(open_order) is not int
            or type(close_order) is not int
            or not 0 <= open_order <= 2_147_483_647
            or not 0 <= close_order <= 2_147_483_647
        ):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_ORDER_INVALID")
        open_orders.append(open_order)
        close_orders.append(close_order)
        base_r.append(float(base_value))
        stress_r.append(float(stress_value))
    if len(set(ids)) != len(ids) or any(not value or len(value) > 256 for value in ids):
        _fail("FINAL_POSITION_IDS_INVALID")
    # Mirror the exact V11 causal-order contract before spooling: strictly
    # increasing (close, close_order, open, open_order, position_id), each
    # open event strictly before its close event (an open_order greater than
    # its close_order stays legal across different timestamps — counter
    # reset), and every trade inside the planned OOS fold boundary.
    prior_key: Optional[tuple[datetime, int, datetime, int, str]] = None
    for ordinal in range(len(ids)):
        opened_dt = datetime.fromisoformat(opens[ordinal].replace("Z", "+00:00"))
        closed_dt = datetime.fromisoformat(closes[ordinal].replace("Z", "+00:00"))
        open_event = (opened_dt, open_orders[ordinal])
        close_event = (closed_dt, close_orders[ordinal])
        order_key = (
            closed_dt,
            close_orders[ordinal],
            opened_dt,
            open_orders[ordinal],
            ids[ordinal],
        )
        if (
            opened_dt < fold_start
            or open_event >= close_event
            or closed_dt >= fold_end_exclusive
            or (prior_key is not None and order_key <= prior_key)
        ):
            _fail(f"FINAL_POSITION_EVENT_{ordinal:05d}_CAUSAL_ORDER_INVALID")
        prior_key = order_key
    return opens, closes, ids, open_orders, close_orders, base_r, stress_r


def _validated_self_hashed_receipt(
    value: Any,
    *,
    role: str,
    expected_keys: frozenset[str],
) -> dict[str, Any]:
    code = role.upper()
    if not isinstance(value, Mapping) or set(value) != set(expected_keys):
        _fail(f"{code}_PROVENANCE_SCHEMA_INVALID")
    body = dict(value)
    if (
        body.get("schema_version") != AUTHORITATIVE_PROVENANCE_SCHEMA
        or body.get("receipt_role") != role
    ):
        _fail(f"{code}_PROVENANCE_ROLE_INVALID")
    receipt_sha = body.pop("receipt_sha256", None)
    _hex(receipt_sha, 64, f"{code}_PROVENANCE_RECEIPT_INVALID")
    if receipt_sha != _sha_json(body):
        _fail(f"{code}_PROVENANCE_RECEIPT_INVALID")
    return dict(value)


def _validated_stress_method_receipt(
    value: Any,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt_artifacts: Mapping[str, Any],
    lanes: Mapping[str, Any],
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "symbol",
        "timeframe",
        "strategy_name",
        "stress_mode",
        "warmup_bars",
        "partition_axis_sha256",
        "signal_vector_sha256",
        "base_cost_attestation_sha256",
        "stress_cost_attestation_sha256",
        "base_spread_values_sha256",
        "stress_spread_values_sha256",
        "base_position_roster_sha256",
        "stress_position_roster_sha256",
        "position_count",
        "daily_observation_count",
        "path_identity_preserved",
        "stress_never_improves",
        "strictly_worse_trade_count",
        "strictly_worse_daily_observation_count",
        "realized_adversity_status",
        "receipt_sha256",
    }
    diagnostic_key = "stress_daily_improvement_observation_count"
    valid_keysets = (expected_keys, expected_keys | {diagnostic_key})
    if not isinstance(value, Mapping) or set(value) not in valid_keysets:
        _fail("STRESS_METHOD_RECEIPT_SCHEMA_INVALID")
    receipt = dict(value)
    receipt_sha = receipt.pop("receipt_sha256", None)
    _hex(receipt_sha, 64, "STRESS_METHOD_RECEIPT_HASH_INVALID")
    if receipt.get(
        "schema_version"
    ) != FROZEN_PATH_COST_STRESS_SCHEMA or receipt_sha != _sha_json(receipt):
        _fail("STRESS_METHOD_RECEIPT_HASH_INVALID")
    base = lanes.get("final_lockbox_base")
    stress = lanes.get("final_lockbox_stress")
    if not isinstance(base, Mapping) or not isinstance(stress, Mapping):
        _fail("STRESS_METHOD_LANES_INVALID")
    base_axis = base.get("trade_power_partition_axis")
    base_cost = base.get("numerical_execution_cost_attestation")
    stress_cost = stress.get("numerical_execution_cost_attestation")
    base_events = base.get("position_events")
    stress_events = stress.get("position_events")
    stress_surface = stress.get("winner_daily_r_surface")
    process = attempt_artifacts.get("winner_process")
    if (
        any(
            not isinstance(item, Mapping)
            for item in (base_axis, base_cost, stress_cost, stress_surface, process)
        )
        or type(base_events) is not list
        or type(stress_events) is not list
    ):
        _fail("STRESS_METHOD_LANES_INVALID")
    frozen_spread_mode = frozen.stress_cost_policy.get("spread_mode")
    try:
        spread_policy = json.loads(frozen_spread_mode)
    except (TypeError, json.JSONDecodeError):
        spread_policy = None
    expected_stress_mode = (
        spread_policy.get("mode") if isinstance(spread_policy, Mapping) else None
    )
    if (
        not isinstance(spread_policy, Mapping)
        or _canonical_bytes(spread_policy).decode("ascii") != frozen_spread_mode
        or expected_stress_mode not in {"stress_p90", "stress_p95", "stress_max"}
    ):
        _fail("STRESS_METHOD_POLICY_MODE_INVALID")
    if (
        receipt.get("symbol") != frozen.symbol
        or receipt.get("timeframe") != frozen.timeframe
        or receipt.get("strategy_name") != process.get("strategy_id")
        or receipt.get("stress_mode") != expected_stress_mode
        or receipt.get("warmup_bars") != base_axis.get("warmup_bars")
        or receipt.get("partition_axis_sha256")
        != base_axis.get("horizon_receipt_sha256")
        or receipt.get("base_cost_attestation_sha256")
        != base_cost.get("attestation_sha256")
        or receipt.get("stress_cost_attestation_sha256")
        != stress_cost.get("attestation_sha256")
        or receipt.get("base_spread_values_sha256")
        != base_cost.get("spread_values_sha256")
        or receipt.get("stress_spread_values_sha256")
        != stress_cost.get("spread_values_sha256")
        or receipt.get("base_position_roster_sha256")
        != _backtester_receipt_sha256({"position_events": base_events})
        or receipt.get("stress_position_roster_sha256")
        != _backtester_receipt_sha256({"position_events": stress_events})
        or receipt.get("position_count") != len(stress_events)
        or receipt.get("daily_observation_count")
        != len(stress_surface.get("daily_net_mtm_r") or [])
        or receipt.get("path_identity_preserved") is not True
        or receipt.get("stress_never_improves") is not True
        or type(receipt.get("strictly_worse_trade_count")) is not int
        or receipt.get("strictly_worse_trade_count") < 0
        or type(receipt.get("strictly_worse_daily_observation_count")) is not int
        or receipt.get("strictly_worse_daily_observation_count") < 0
        or (
            diagnostic_key in receipt
            and (
                type(receipt.get(diagnostic_key)) is not int
                or receipt.get(diagnostic_key) < 0
            )
        )
        or receipt.get("realized_adversity_status")
        not in {
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
        }
    ):
        _fail("STRESS_METHOD_RECEIPT_BINDING_INVALID")
    _hex(
        receipt.get("signal_vector_sha256"),
        64,
        "STRESS_METHOD_SIGNAL_HASH_INVALID",
    )
    # Independently derive the receipt's adversity status.  A resealed label
    # cannot turn an unchanged path into stress-survival evidence, nor can it
    # demote a genuinely degraded path without being detected.
    base_trade = [
        Decimal(str(row.get("pnl_dollars"))) / Decimal(str(row.get("risk_amount")))
        for row in base_events
    ]
    stress_trade = [
        Decimal(str(row.get("pnl_dollars"))) / Decimal(str(row.get("risk_amount")))
        for row in stress_events
    ]
    base_daily = (base.get("winner_daily_r_surface") or {}).get(
        "daily_net_mtm_r"
    ) or []
    stress_daily = stress_surface.get("daily_net_mtm_r") or []
    strictly_worse_trade_count = sum(
        right < left for left, right in zip(base_trade, stress_trade, strict=True)
    )
    strictly_worse_daily_observation_count = sum(
        Decimal(str(right)) < Decimal(str(left))
        for left, right in zip(base_daily, stress_daily, strict=True)
    )
    stress_daily_improvement_observation_count = sum(
        Decimal(str(right)) > Decimal(str(left))
        for left, right in zip(base_daily, stress_daily, strict=True)
    )
    vector_changed = (
        receipt["base_spread_values_sha256"]
        != receipt["stress_spread_values_sha256"]
    )
    strictly_adverse = bool(
        strictly_worse_trade_count
        or strictly_worse_daily_observation_count
    )
    if strictly_adverse and not vector_changed:
        _fail("STRESS_METHOD_VECTOR_ADVERSITY_INCONSISTENT")
    expected_adversity_status = (
        BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
        if strictly_adverse
        else BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    )
    if (
        receipt.get("strictly_worse_trade_count")
        != strictly_worse_trade_count
        or receipt.get("strictly_worse_daily_observation_count")
        != strictly_worse_daily_observation_count
        or (
            diagnostic_key in receipt
            and receipt.get(diagnostic_key)
            != stress_daily_improvement_observation_count
        )
        or receipt.get("realized_adversity_status") != expected_adversity_status
    ):
        _fail("STRESS_METHOD_ADVERSITY_STATUS_MISMATCH")
    return dict(value)


_PROVENANCE_COMMON_KEYS = frozenset(
    {
        "schema_version",
        "authority_scope",
        "anchor_id",
        "symbol",
        "timeframe",
        "regime",
        "attempt_ordinal",
        "horizon_attempt_id",
        "fold_ordinal",
        "fold_id",
        "confers_final_oos_or_trading_authority",
        "receipt_role",
        "receipt_sha256",
    }
)


def _validated_partition_geometry(
    values: Any,
    *,
    lanes: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    lane_order = (
        ("optimizer_train", "optimizer_train_base"),
        ("tuning_validation", "tuning_validation_base"),
        ("final_lockbox", "final_lockbox_base"),
    )
    expected_keys = {
        "schema_version",
        "role",
        "frame_start_utc",
        "scored_start_utc",
        "end_exclusive_utc",
        "warmup_bars",
        "scored_bars",
        "frame_rows",
        "frame_sha256",
        "receipt_sha256",
    }
    if type(values) is not list or len(values) != len(lane_order):
        _fail("PARTITION_GEOMETRY_ROSTER_INVALID")
    result: list[dict[str, Any]] = []
    for ordinal, ((role, lane_name), raw) in enumerate(
        zip(lane_order, values, strict=True)
    ):
        if not isinstance(raw, Mapping) or set(raw) != expected_keys:
            _fail(f"PARTITION_GEOMETRY_{ordinal}_SCHEMA_INVALID")
        geometry = dict(raw)
        body = dict(geometry)
        receipt_sha = body.pop("receipt_sha256", None)
        lane = lanes.get(lane_name)
        axis = (
            lane.get("trade_power_partition_axis")
            if isinstance(lane, Mapping)
            else None
        )
        if (
            body.get("schema_version") != PARTITION_GEOMETRY_SCHEMA
            or body.get("role") != role
            or receipt_sha != _sha_json(body)
            or not isinstance(axis, Mapping)
        ):
            _fail(f"PARTITION_GEOMETRY_{ordinal}_RECEIPT_INVALID")
        frame_start = _utc(
            body.get("frame_start_utc"),
            f"PARTITION_GEOMETRY_{ordinal}_FRAME_START_INVALID",
        )
        scored_start = _utc(
            body.get("scored_start_utc"),
            f"PARTITION_GEOMETRY_{ordinal}_SCORED_START_INVALID",
        )
        end = _utc(
            body.get("end_exclusive_utc"),
            f"PARTITION_GEOMETRY_{ordinal}_END_INVALID",
        )
        warmup = body.get("warmup_bars")
        scored = body.get("scored_bars")
        rows = body.get("frame_rows")
        if (
            type(warmup) is not int
            or warmup < 0
            or type(scored) is not int
            or scored <= 0
            or type(rows) is not int
            or rows != warmup + scored
            or scored_start
            != _utc(
                axis.get("start_utc"),
                f"PARTITION_GEOMETRY_{ordinal}_AXIS_START_INVALID",
            )
            or end
            != _utc(
                axis.get("end_utc"),
                f"PARTITION_GEOMETRY_{ordinal}_AXIS_END_INVALID",
            )
            or axis.get("warmup_bars") != warmup
            or axis.get("scored_bars") != scored
            or datetime.fromisoformat(frame_start.replace("Z", "+00:00"))
            > datetime.fromisoformat(scored_start.replace("Z", "+00:00"))
        ):
            _fail(f"PARTITION_GEOMETRY_{ordinal}_LANE_SPLICE")
        _hex(
            body.get("frame_sha256"),
            64,
            f"PARTITION_GEOMETRY_{ordinal}_FRAME_HASH_INVALID",
        )
        result.append(geometry)
    return tuple(result)


def _validated_current_cell_plan_context(
    sink: SymbolAuthoritySink,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt_artifacts: Sequence[Mapping[str, Any]],
    binding: Mapping[str, Any],
    production_cell_search_plan: Any,
    production_search_process: Any,
) -> dict[str, Any]:
    """Reopen the genuine CURRENT plan and derive its historical oracle context.

    The production planner deliberately reuses a sealed HistoricalHorizonPlan for
    current refit.  CURRENT cross-binding bytes therefore carry only the three
    outer production-plan receipts; the anchor-shaped values required by each
    inner-fold provenance receipt are derived here from that sealed plan rather
    than repeated as caller-asserted top-level fields.
    """

    try:
        # Local import is required: the planner imports the authoritative
        # executor, which imports this module.  At evidence-production time the
        # module graph is fully initialized and the planner can be reopened.
        import pm_production_search_planner as planner
    except Exception as exc:  # pragma: no cover - import failures are typed
        raise WinnerRawEvidenceError(
            "CURRENT_CELL_PLAN_VERIFIER_UNAVAILABLE", type(exc).__name__
        ) from exc
    if production_cell_search_plan is None or production_search_process is None:
        _fail("CURRENT_CELL_PLAN_REQUIRED")
    try:
        verified = planner.verify_production_cell_search_plan(
            sink.context,
            process=production_search_process,
            sink=sink,
            plan=production_cell_search_plan,
        )
    except Exception as exc:
        detail = getattr(exc, "code", type(exc).__name__)
        raise WinnerRawEvidenceError(
            "CURRENT_CELL_PLAN_REVERIFY_FAILED", str(detail)
        ) from exc
    if verified is not True:
        _fail("CURRENT_CELL_PLAN_REVERIFY_FAILED")
    plan = production_cell_search_plan
    process = production_search_process
    try:
        receipt = plan.cell_receipt
        plan_body = plan.horizon_plan.to_dict()
        carried_attempts = plan.attempt_artifacts()
        plan_receipt_sha256 = plan.receipt_sha256
        process_receipt_sha256 = process.receipt_sha256
    except Exception as exc:
        raise WinnerRawEvidenceError(
            "CURRENT_CELL_PLAN_REOPEN_FAILED", type(exc).__name__
        ) from exc
    expected_cell = {
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
    }
    if (
        plan.mode != "CURRENT_REFIT"
        or plan.symbol != frozen.symbol
        or plan.timeframe != frozen.timeframe
        or plan.regime != frozen.regime
        or not isinstance(receipt, Mapping)
        or not isinstance(plan_body, Mapping)
        or receipt.get("cell") != expected_cell
        or plan_body.get("cell") != expected_cell
        or receipt.get("artifact_namespace") != frozen.artifact_namespace
        or receipt.get("attempts_frozen_before_search") is not True
        or receipt.get("state_replay_required_before_scored_partition") is not True
        or receipt.get(
            "confers_winner_causal_current_capital_publication_or_live_authority"
        )
        is not False
        or plan_receipt_sha256 != binding.get("cell_plan_receipt_sha256")
        or plan.causal_precommit_receipt_sha256
        != binding.get("causal_precommit_receipt_sha256")
        or receipt.get("causal_process_precommit_receipt_sha256")
        != binding.get("causal_precommit_receipt_sha256")
        or plan.search_process_receipt_sha256
        != binding.get("search_process_receipt_sha256")
        or receipt.get("search_process_receipt_sha256")
        != binding.get("search_process_receipt_sha256")
        or process_receipt_sha256 != binding.get("search_process_receipt_sha256")
    ):
        _fail("PROVENANCE_CURRENT_CELL_PLAN_SPLICE")
    if tuple(carried_attempts) != tuple(dict(value) for value in attempt_artifacts):
        _fail("PROVENANCE_CURRENT_CELL_PLAN_ATTEMPT_SPLICE")

    plan_binding = receipt.get("horizon_plan_binding")
    expected_horizon_roster = {
        "plan_receipt_sha256": (
            plan_binding.get("plan_receipt_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
        "source_prefix_receipt_sha256": (
            plan_binding.get("source_prefix_receipt_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
        "candidate_registry_sha256": (
            plan_binding.get("candidate_registry_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
        "trial_registry_sha256": (
            plan_binding.get("trial_registry_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
        "attempt_roster_sha256": (
            plan_binding.get("attempt_roster_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
        "evidence_roster_sha256": (
            plan_binding.get("evidence_roster_sha256")
            if isinstance(plan_binding, Mapping)
            else None
        ),
    }
    anchor = plan_body.get("anchor")
    prefix = plan_body.get("native_prefix")
    receipt_prefix = receipt.get("native_prefix")
    if (
        not isinstance(plan_binding, Mapping)
        or not isinstance(anchor, Mapping)
        or not isinstance(prefix, Mapping)
        or not isinstance(receipt_prefix, Mapping)
        or dict(binding.get("horizon_roster", {})) != expected_horizon_roster
        or plan_binding.get("anchor_id") != anchor.get("anchor_id")
        or plan_binding.get("selection_cutoff_utc")
        != anchor.get("selection_cutoff_utc")
        or plan_binding.get("process_precommit_receipt_sha256")
        != anchor.get("process_precommit_receipt_sha256")
        or anchor.get("process_precommit_receipt_sha256")
        != binding.get("causal_precommit_receipt_sha256")
        or prefix.get("source_state") != "AVAILABLE"
        or prefix.get("prefix_index_sha256")
        != binding.get("native_prefix_index_sha256")
        or receipt_prefix.get("index_sha256")
        != binding.get("native_prefix_index_sha256")
        or receipt_prefix.get("content_sha256") != prefix.get("prefix_content_sha256")
        or receipt.get("source_group_decision_sha256")
        != binding.get("source_group_decision_sha256")
        or receipt.get("regime_fit_decision_sha256")
        != binding.get("regime_fit_decision_sha256")
    ):
        _fail("PROVENANCE_CURRENT_HORIZON_PLAN_SPLICE")
    for roster_hash, roster_name in (
        ("candidate_registry_sha256", "candidates"),
        ("trial_registry_sha256", "trials"),
        ("attempt_roster_sha256", "attempts"),
        ("evidence_roster_sha256", "expected_evaluations"),
    ):
        roster = plan_body.get(roster_name)
        if type(roster) is not list or plan_body.get(roster_hash) != _sha_json(roster):
            _fail("PROVENANCE_CURRENT_HORIZON_ROSTER_INVALID")
    horizon_policy = plan_body.get("horizon_policy")
    if (
        plan_body.get("plan_status")
        not in {"READY_FOR_HISTORICAL_EVIDENCE", "HISTORICAL_BOUNDARY_LIMITED"}
        or not isinstance(horizon_policy, Mapping)
        or horizon_policy.get("cost_contract_sha256")
        != frozen.precommitment.get("cost_contract_sha256")
        or horizon_policy.get("cost_stress_contract_sha256")
        != frozen.precommitment.get("cost_stress_contract_sha256")
    ):
        _fail("PROVENANCE_CURRENT_HORIZON_POLICY_SPLICE")
    return {
        "mode": CURRENT_CROSS_BINDING_MODE,
        "anchor_id": _identity(
            anchor.get("anchor_id"), "PROVENANCE_CURRENT_ANCHOR_INVALID", 256
        ),
        "selection_cutoff_utc": _utc(
            anchor.get("selection_cutoff_utc"),
            "PROVENANCE_CURRENT_SELECTION_CUTOFF_INVALID",
        ),
        "process_precommit_receipt_sha256": _hex(
            anchor.get("process_precommit_receipt_sha256"),
            64,
            "PROVENANCE_CURRENT_PROCESS_PRECOMMIT_INVALID",
        ),
        "plan_body": dict(plan_body),
    }


def _validated_current_plan_bijection(
    *,
    plan_body: Mapping[str, Any],
    mapping: Sequence[Mapping[str, Any]],
    attempt_artifacts: Sequence[Mapping[str, Any]],
) -> None:
    """Replay the exact Horizon candidate/trial/fold bijection from CURRENT plan."""

    candidates = plan_body.get("candidates")
    trials = plan_body.get("trials")
    plan_attempts = plan_body.get("attempts")
    expected_evaluations = plan_body.get("expected_evaluations")
    prefix = plan_body.get("native_prefix")
    if (
        type(candidates) is not list
        or type(trials) is not list
        or type(plan_attempts) is not list
        or type(expected_evaluations) is not list
        or not isinstance(prefix, Mapping)
        or len(plan_attempts) != len(mapping)
        or len(attempt_artifacts) != len(mapping)
    ):
        _fail("PROVENANCE_CURRENT_PLAN_BIJECTION_ROSTER_INVALID")
    candidate_by_id = {
        value.get("candidate_id"): value
        for value in candidates
        if isinstance(value, Mapping)
    }
    trial_by_id = {
        value.get("trial_id"): value for value in trials if isinstance(value, Mapping)
    }
    if len(candidate_by_id) != len(candidates) or len(trial_by_id) != len(trials):
        _fail("PROVENANCE_CURRENT_PLAN_BIJECTION_IDENTITY_INVALID")
    expected_by_attempt: dict[str, list[Mapping[str, Any]]] = {}
    for value in expected_evaluations:
        if not isinstance(value, Mapping) or type(value.get("attempt_id")) is not str:
            _fail("PROVENANCE_CURRENT_PLAN_EVIDENCE_ROSTER_INVALID")
        expected_by_attempt.setdefault(value["attempt_id"], []).append(value)
    observed_attempt_ids: list[str] = []
    for ordinal, (row, artifacts, planned) in enumerate(
        zip(mapping, attempt_artifacts, plan_attempts, strict=True)
    ):
        if not isinstance(row, Mapping) or not isinstance(planned, Mapping):
            _fail("PROVENANCE_CURRENT_PLAN_BIJECTION_ATTEMPT_INVALID")
        candidate = candidate_by_id.get(planned.get("candidate_id"))
        trial = trial_by_id.get(planned.get("trial_id"))
        process = artifacts.get("winner_process")
        horizon = artifacts.get("horizon_process")
        trial_definition = artifacts.get("trial_definition")
        if any(
            not isinstance(value, Mapping)
            for value in (candidate, trial, process, horizon, trial_definition)
        ):
            _fail("PROVENANCE_CURRENT_PLAN_BIJECTION_ATTEMPT_INVALID")
        try:
            fit_end = _utc(
                horizon.get("fit_end_utc"),
                "PROVENANCE_CURRENT_WINNER_FIT_END_INVALID",
            )
            latest_closed = _utc(
                horizon.get("latest_closed_bar_utc"),
                "PROVENANCE_CURRENT_WINNER_LATEST_CLOSED_INVALID",
            )
        except WinnerRawEvidenceError:
            raise
        if (
            row.get("ordinal") != ordinal
            or row.get("horizon_attempt_id") != planned.get("attempt_id")
            or row.get("horizon_candidate_id") != candidate.get("candidate_id")
            or row.get("horizon_candidate_definition_sha256")
            != candidate.get("candidate_definition_sha256")
            or row.get("horizon_trial_id") != trial.get("trial_id")
            or row.get("horizon_trial_definition_sha256")
            != trial.get("trial_definition_sha256")
            or planned.get("strategy_id") != process.get("strategy_id")
            or candidate.get("lookback_kind") != horizon.get("lookback_kind")
            or candidate.get("lookback_value") != horizon.get("lookback_value")
            or fit_end
            != _utc(
                prefix.get("prefix_end_exclusive_utc"),
                "PROVENANCE_CURRENT_PREFIX_END_INVALID",
            )
            or latest_closed
            != _utc(
                prefix.get("prefix_last_close_utc"),
                "PROVENANCE_CURRENT_PREFIX_LATEST_INVALID",
            )
            or horizon.get("source_snapshot_sha256")
            != prefix.get("prefix_content_sha256")
            or trial.get("strategy_id") != process.get("strategy_id")
            or trial.get("strategy_release_sha256")
            != process.get("strategy_code_sha256")
            or trial.get("parameter_set_sha256")
            != _sha_json(trial_definition.get("parameters"))
        ):
            _fail("PROVENANCE_CURRENT_PLAN_BIJECTION_SPLICE")
        observed_attempt_ids.append(str(planned.get("attempt_id")))
        expected = expected_by_attempt.get(str(planned.get("attempt_id")), [])
        folds = row.get("fold_mapping")
        if type(folds) is not list:
            _fail("PROVENANCE_CURRENT_PLAN_FOLD_ROSTER_INVALID")
        if planned.get("status") == "PLANNED":
            causal = [
                value
                for value in expected
                if value.get("downstream_causal_eligible") is True
            ]
            if len(causal) != len(folds):
                _fail("PROVENANCE_CURRENT_PLAN_FOLD_ROSTER_INVALID")
            candidate_folds = candidate.get("folds")
            if type(candidate_folds) is not list:
                _fail("PROVENANCE_CURRENT_PLAN_FOLD_ROSTER_INVALID")
            for fold, expected_fold in zip(folds, causal, strict=True):
                candidate_fold = next(
                    (
                        value
                        for value in candidate_folds
                        if isinstance(value, Mapping)
                        and value.get("fold_id") == expected_fold.get("fold_id")
                    ),
                    None,
                )
                if (
                    not isinstance(fold, Mapping)
                    or not isinstance(candidate_fold, Mapping)
                    or expected_fold.get("expected_status") != "EVALUATION_REQUIRED"
                    or fold.get("horizon_fold_id") != expected_fold.get("fold_id")
                    or fold.get("horizon_context_sha256")
                    != expected_fold.get("context_sha256")
                    or fold.get("horizon_fold_definition_sha256")
                    != candidate_fold.get("fold_definition_sha256")
                    or fold.get("validation_start_utc")
                    != expected_fold.get("validation_start_utc")
                    or fold.get("validation_end_exclusive_utc")
                    != expected_fold.get("validation_end_exclusive_utc")
                ):
                    _fail("PROVENANCE_CURRENT_PLAN_FOLD_SPLICE")
        elif planned.get("status") == "PROCESS_REJECTED":
            if (
                folds
                or not expected
                or any(
                    value.get("expected_status") != "PROCESS_REJECTED"
                    for value in expected
                )
            ):
                _fail("PROVENANCE_CURRENT_PLAN_TERMINAL_SPLICE")
        else:
            _fail("PROVENANCE_CURRENT_PLAN_ATTEMPT_STATUS_INVALID")
    if observed_attempt_ids != [value.get("attempt_id") for value in plan_attempts]:
        _fail("PROVENANCE_CURRENT_PLAN_ATTEMPT_ORDER_INVALID")


def _validated_horizon_winner_cross_binding(
    encoded: Any,
    *,
    sink: SymbolAuthoritySink,
    frozen: FrozenWinnerSearchPlan,
    attempt_artifacts: Sequence[Mapping[str, Any]],
    production_cell_search_plan: Any = None,
    production_search_process: Any = None,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], dict[str, Any]]:
    if (
        type(encoded) is not bytes
        or not encoded
        or len(encoded) > winner.MAX_REQUEST_BYTES
    ):
        _fail("PROVENANCE_CROSS_BINDING_BYTES_INVALID")
    binding = _decode_object(encoded, "PROVENANCE_CROSS_BINDING_BYTES_INVALID")
    common_top = {
        "schema_version",
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
    historical_fields = {
        "producer_request_sha256",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
    }
    current_fields = {
        "cell_plan_receipt_sha256",
        "causal_precommit_receipt_sha256",
        "search_process_receipt_sha256",
    }
    schema = binding.get("schema_version")
    if schema == HORIZON_WINNER_CROSS_BINDING_SCHEMA:
        mode = HISTORICAL_CROSS_BINDING_MODE
        expected_top = common_top | historical_fields
        if (
            production_cell_search_plan is not None
            or production_search_process is not None
        ):
            _fail("HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN")
    elif schema == HORIZON_WINNER_CROSS_BINDING_SCHEMA_V2:
        mode = binding.get("mode")
        if mode == HISTORICAL_CROSS_BINDING_MODE:
            expected_top = common_top | historical_fields | {"mode"}
            if (
                production_cell_search_plan is not None
                or production_search_process is not None
            ):
                _fail("HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN")
        elif mode == CURRENT_CROSS_BINDING_MODE:
            expected_top = common_top | current_fields | {"mode"}
        else:
            _fail("PROVENANCE_CROSS_BINDING_MODE_INVALID")
    else:
        _fail("PROVENANCE_CROSS_BINDING_SCHEMA_INVALID")
    if set(binding) != expected_top or (
        binding.get("symbol") != frozen.symbol
        or binding.get("timeframe") != frozen.timeframe
        or binding.get("regime") != frozen.regime
        or binding.get("artifact_namespace") != frozen.artifact_namespace
        or binding.get("bijective_coverage") is not True
        or binding.get("created_before_winner_evidence_access") is not True
        or binding.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail("PROVENANCE_CROSS_BINDING_SCHEMA_INVALID")
    if mode == HISTORICAL_CROSS_BINDING_MODE:
        binding_context = {
            "mode": HISTORICAL_CROSS_BINDING_MODE,
            "anchor_id": _identity(
                binding.get("anchor_id"),
                "PROVENANCE_CROSS_BINDING_ANCHOR_INVALID",
                256,
            ),
            "selection_cutoff_utc": _utc(
                binding.get("selection_cutoff_utc"),
                "PROVENANCE_SELECTION_CUTOFF_INVALID",
            ),
            "process_precommit_receipt_sha256": _hex(
                binding.get("process_precommit_receipt_sha256"),
                64,
                "PROVENANCE_PROCESS_PRECOMMIT_RECEIPT_SHA256_INVALID",
            ),
        }
        _hex(
            binding.get("producer_request_sha256"),
            64,
            "PROVENANCE_PRODUCER_REQUEST_INVALID",
        )
    else:
        for name in current_fields:
            _hex(binding.get(name), 64, f"PROVENANCE_{name.upper()}_INVALID")
        binding_context = {}
    for name in (
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "native_prefix_index_sha256",
    ):
        _hex(binding.get(name), 64, f"PROVENANCE_{name.upper()}_INVALID")
    horizon_roster = binding.get("horizon_roster")
    winner_roster = binding.get("winner_roster")
    expected_horizon = {
        "plan_receipt_sha256",
        "source_prefix_receipt_sha256",
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "attempt_roster_sha256",
        "evidence_roster_sha256",
    }
    expected_winner = {
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
    if (
        not isinstance(horizon_roster, Mapping)
        or set(horizon_roster) != expected_horizon
        or not isinstance(winner_roster, Mapping)
        or set(winner_roster) != expected_winner
    ):
        _fail("PROVENANCE_CROSS_BINDING_ROSTER_INVALID")
    for value in horizon_roster.values():
        _hex(value, 64, "PROVENANCE_HORIZON_ROSTER_HASH_INVALID")
    precommitment = frozen.precommitment
    attempt_ledger = frozen.attempt_ledger
    expected_winner_values = {
        "precommitment_sha256": _sha_json(precommitment),
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "frozen_receipt_sha256": hashlib.sha256(
            frozen.frozen_receipt_bytes
        ).hexdigest(),
        "process_registry_sha256": precommitment["process_registry_sha256"],
        "horizon_registry_sha256": precommitment["horizon_registry_sha256"],
        "trial_registry_sha256": precommitment["trial_registry_sha256"],
        "attempts_sha256": attempt_ledger["attempts_sha256"],
        "causal_fold_plan_sha256": precommitment["causal_fold_plan_sha256"],
        "source_contract_sha256": precommitment["source_contract_sha256"],
        "cost_contract_sha256": precommitment["cost_contract_sha256"],
        "cost_stress_contract_sha256": precommitment["cost_stress_contract_sha256"],
    }
    if dict(winner_roster) != expected_winner_values:
        _fail("PROVENANCE_CROSS_BINDING_WINNER_SPLICE")
    if mode == CURRENT_CROSS_BINDING_MODE:
        binding_context = _validated_current_cell_plan_context(
            sink,
            frozen=frozen,
            attempt_artifacts=attempt_artifacts,
            binding=binding,
            production_cell_search_plan=production_cell_search_plan,
            production_search_process=production_search_process,
        )
    mapping = binding.get("mapping")
    frozen_horizon_contracts = _normalized_horizon_execution_contracts(
        frozen.horizon_execution_contracts,
        expected_count=len(attempt_ledger["attempts"]),
    )
    if (
        type(mapping) is not list
        or binding.get("mapping_count") != len(mapping)
        or len(mapping) != len(attempt_ledger["attempts"])
        or len(attempt_artifacts) != len(mapping)
        or binding.get("mapping_sha256") != _sha_json(mapping)
    ):
        _fail("PROVENANCE_CROSS_BINDING_MAPPING_INVALID")
    expected_mapping_keys = {
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
    expected_fold_keys = {
        "horizon_fold_id",
        "horizon_fold_definition_sha256",
        "horizon_context_sha256",
        "winner_fold_id",
        "validation_start_utc",
        "validation_end_exclusive_utc",
    }
    source_count_receipt_keys = {
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
    result: list[dict[str, Any]] = []
    for ordinal, (row, attempt, horizon_contract, artifacts) in enumerate(
        zip(
            mapping,
            attempt_ledger["attempts"],
            frozen_horizon_contracts,
            attempt_artifacts,
            strict=True,
        )
    ):
        if not isinstance(row, Mapping) or set(row) != expected_mapping_keys:
            _fail("PROVENANCE_CROSS_BINDING_MAPPING_SCHEMA_INVALID")
        expected_attempt = {
            "ordinal": ordinal,
            "winner_attempt_id": attempt["attempt_id"],
            "winner_process_candidate_id": attempt["process_candidate_id"],
            "process_sha256": attempt["process_sha256"],
            "horizon_process_sha256": attempt["horizon_process_sha256"],
            "trial_definition_sha256": attempt["trial_definition_sha256"],
            "refit_recipe_sha256": attempt["refit_recipe_sha256"],
        }
        if any(row.get(name) != value for name, value in expected_attempt.items()):
            _fail("PROVENANCE_CROSS_BINDING_ATTEMPT_SPLICE")
        observed_contract = row.get("horizon_execution_contract")
        source_count_receipt = row.get("source_count_receipt")
        if (
            not isinstance(observed_contract, Mapping)
            or dict(observed_contract) != horizon_contract
            or not isinstance(artifacts, Mapping)
            or set(artifacts) != _ATTEMPT_ARTIFACT_KEYS
            or not isinstance(artifacts.get("horizon_process"), Mapping)
            or not isinstance(source_count_receipt, Mapping)
            or set(source_count_receipt) != source_count_receipt_keys
        ):
            _fail("PROVENANCE_CROSS_BINDING_SOURCE_COUNT_SCHEMA_INVALID")
        horizon_artifact = artifacts["horizon_process"]
        source_count_body = dict(source_count_receipt)
        source_count_sha256 = source_count_body.pop("receipt_sha256", None)
        fit_start = _utc(
            source_count_receipt.get("fit_start_utc"),
            "PROVENANCE_SOURCE_COUNT_FIT_START_INVALID",
        )
        fit_end = _utc(
            source_count_receipt.get("fit_end_exclusive_utc"),
            "PROVENANCE_SOURCE_COUNT_FIT_END_INVALID",
        )
        latest_closed = _utc(
            source_count_receipt.get("latest_closed_bar_utc"),
            "PROVENANCE_SOURCE_COUNT_LATEST_CLOSED_INVALID",
        )
        if (
            source_count_receipt.get("schema_version") != SOURCE_COUNT_RECEIPT_SCHEMA
            or source_count_sha256 != _sha_json(source_count_body)
            or source_count_receipt.get("anchor_id") != binding_context.get("anchor_id")
            or source_count_receipt.get("symbol") != frozen.symbol
            or source_count_receipt.get("timeframe") != frozen.timeframe
            or source_count_receipt.get("regime") != frozen.regime
            or source_count_receipt.get("horizon_plan_receipt_sha256")
            != binding["horizon_roster"]["plan_receipt_sha256"]
            or source_count_receipt.get("native_prefix_index_sha256")
            != binding.get("native_prefix_index_sha256")
            or source_count_receipt.get("native_prefix_content_sha256")
            != horizon_artifact.get("source_snapshot_sha256")
            or source_count_receipt.get("horizon_attempt_id")
            != row.get("horizon_attempt_id")
            or source_count_receipt.get("horizon_candidate_id")
            != row.get("horizon_candidate_id")
            or source_count_receipt.get("horizon_candidate_definition_sha256")
            != row.get("horizon_candidate_definition_sha256")
            or any(
                source_count_receipt.get(name) != horizon_contract[name]
                for name in _HORIZON_EXECUTION_CONTRACT_KEYS
            )
            or horizon_artifact.get("lookback_kind")
            != horizon_contract["lookback_kind"]
            or horizon_artifact.get("lookback_value")
            != horizon_contract["lookback_value"]
            or horizon_artifact.get("source_bar_count")
            != horizon_contract["source_bar_count"]
            or fit_start != source_count_receipt.get("fit_start_utc")
            or fit_end != source_count_receipt.get("fit_end_exclusive_utc")
            or latest_closed != source_count_receipt.get("latest_closed_bar_utc")
            or fit_start
            != _utc(
                horizon_artifact.get("fit_start_utc"),
                "PROVENANCE_HORIZON_FIT_START_INVALID",
            )
            or fit_end
            != _utc(
                horizon_artifact.get("fit_end_utc"),
                "PROVENANCE_HORIZON_FIT_END_INVALID",
            )
            or latest_closed
            != _utc(
                horizon_artifact.get("latest_closed_bar_utc"),
                "PROVENANCE_HORIZON_LATEST_CLOSED_INVALID",
            )
            or datetime.fromisoformat(fit_start.replace("Z", "+00:00"))
            >= datetime.fromisoformat(fit_end.replace("Z", "+00:00"))
            or datetime.fromisoformat(latest_closed.replace("Z", "+00:00"))
            > datetime.fromisoformat(fit_end.replace("Z", "+00:00"))
        ):
            _fail("PROVENANCE_CROSS_BINDING_SOURCE_COUNT_SPLICE")
        for name in (
            "horizon_plan_receipt_sha256",
            "native_prefix_index_sha256",
            "native_prefix_content_sha256",
            "horizon_candidate_definition_sha256",
            "fit_frame_sha256",
            "receipt_sha256",
        ):
            _hex(
                source_count_receipt.get(name),
                64,
                f"PROVENANCE_SOURCE_COUNT_{name.upper()}_INVALID",
            )
        for name in (
            "horizon_attempt_id",
            "horizon_candidate_id",
            "horizon_trial_id",
        ):
            _identity(row.get(name), f"PROVENANCE_{name.upper()}_INVALID", 256)
        for name in (
            "horizon_candidate_definition_sha256",
            "horizon_trial_definition_sha256",
        ):
            _hex(row.get(name), 64, f"PROVENANCE_{name.upper()}_INVALID")
        folds = row.get("fold_mapping")
        planned_folds = precommitment["causal_fold_plan"]
        if (
            type(folds) is not list
            or row.get("fold_mapping_sha256") != _sha_json(folds)
            or len(folds) not in {0, len(planned_folds)}
        ):
            _fail("PROVENANCE_CROSS_BINDING_FOLD_MAPPING_INVALID")
        expected_evidence_role = (
            "EVALUATION_REQUIRED"
            if len(folds) == len(planned_folds)
            else "PRESEARCH_PROCESS_REJECTED"
        )
        if attempt.get("precommitted_evidence_role") != expected_evidence_role:
            _fail("PRESEARCH_EVIDENCE_ROLE_HORIZON_SPLICE")
        for fold_ordinal, (fold, planned) in enumerate(zip(folds, planned_folds)):
            if (
                not isinstance(fold, Mapping)
                or set(fold) != expected_fold_keys
                or fold.get("winner_fold_id") != planned.get("fold_id")
            ):
                _fail("PROVENANCE_CROSS_BINDING_FOLD_SPLICE")
            _identity(
                fold.get("horizon_fold_id"),
                "PROVENANCE_HORIZON_FOLD_ID_INVALID",
                256,
            )
            _hex(
                fold.get("horizon_context_sha256"),
                64,
                "PROVENANCE_HORIZON_CONTEXT_INVALID",
            )
            _hex(
                fold.get("horizon_fold_definition_sha256"),
                64,
                "PROVENANCE_HORIZON_FOLD_DEFINITION_INVALID",
            )
            start = _utc(
                fold.get("validation_start_utc"),
                "PROVENANCE_VALIDATION_START_INVALID",
            )
            end = _utc(
                fold.get("validation_end_exclusive_utc"),
                "PROVENANCE_VALIDATION_END_INVALID",
            )
            if datetime.fromisoformat(
                start.replace("Z", "+00:00")
            ) >= datetime.fromisoformat(
                end.replace("Z", "+00:00")
            ) or start != planned.get("anchor_as_of_utc"):
                _fail("PROVENANCE_CROSS_BINDING_FOLD_INTERVAL_INVALID")
            if fold_ordinal != planned.get("fold_ordinal"):
                _fail("PROVENANCE_CROSS_BINDING_FOLD_ORDER_INVALID")
        result.append(dict(row))
    if mode == CURRENT_CROSS_BINDING_MODE:
        _validated_current_plan_bijection(
            plan_body=binding_context["plan_body"],
            mapping=result,
            attempt_artifacts=attempt_artifacts,
        )
    return binding, tuple(result), binding_context


def _validated_provenance(
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt: Mapping[str, Any],
    attempt_artifacts: Mapping[str, Any],
    cross_binding: Mapping[str, Any],
    cross_binding_context: Mapping[str, Any],
    cross_binding_attempt: Mapping[str, Any],
    planned_fold: Mapping[str, Any],
    lanes: Mapping[str, Any],
    raw: Any,
) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, Mapping) or set(raw) != set(_PROVENANCE_KEYS):
        _fail("FOLD_PROVENANCE_SCHEMA_INVALID")
    specific = {
        "anchor_selection_receipt": frozenset(
            {
                "process_precommit_receipt_sha256",
                "horizon_plan_receipt_sha256",
                "selection_cutoff_utc",
            }
        ),
        "anchor_horizon_decision": frozenset(
            {
                "candidate_id",
                "candidate_definition_sha256",
                "horizon_process_sha256",
                "fold_definition_sha256",
                "partition_geometry",
                "partition_geometry_receipts",
            }
        ),
        "anchor_trial_ledger": frozenset(
            {
                "trial_registry_sha256",
                "attempt_roster_sha256",
                "attempted_trials_exact",
            }
        ),
        "anchor_selected_trial": frozenset(
            {
                "selection_scope",
                "trial_id",
                "trial_definition_sha256",
                "winner_process_sha256",
                "executable_trial_sha256",
            }
        ),
        "anchor_refit_receipt": frozenset(
            {
                "refit_recipe_sha256",
                "fit_start_utc",
                "fit_end_exclusive_utc",
                "post_selection_search",
            }
        ),
        "attempt_replay_receipt": frozenset(
            {
                "lane_sha256",
                "stress_method_receipt_sha256",
                "stress_method_receipt",
                "signal_vector_receipt",
                "position_events_are_genuine_backtester_v2",
            }
        ),
        "oos_source_receipt": frozenset(
            {
                "native_prefix_index_sha256",
                "native_prefix_content_sha256",
                "source_contract_sha256",
                "native_source_sha256",
                "activation_manifest_sha256",
                "source_group_decision_sha256",
                "regime_fit_decision_sha256",
                "execution_spec_receipt",
                "partition_frame_sha256",
            }
        ),
    }
    receipts = {
        name: _validated_self_hashed_receipt(
            raw.get(name),
            role=name,
            expected_keys=_PROVENANCE_COMMON_KEYS | specific[name],
        )
        for name in _PROVENANCE_KEYS
    }
    first = receipts[_PROVENANCE_KEYS[0]]
    common = {
        name: first[name]
        for name in _PROVENANCE_COMMON_KEYS
        if name not in {"receipt_role", "receipt_sha256", "schema_version"}
    }
    for receipt in receipts.values():
        observed = {name: receipt[name] for name in common}
        if observed != common:
            _fail("FOLD_PROVENANCE_COMMON_BINDING_INVALID")
    if (
        common.get("authority_scope") != "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY"
        or common.get("symbol") != frozen.symbol
        or common.get("timeframe") != frozen.timeframe
        or common.get("regime") != frozen.regime
        or common.get("attempt_ordinal") != attempt.get("ordinal")
        or common.get("fold_ordinal") != planned_fold.get("fold_ordinal")
        or common.get("confers_final_oos_or_trading_authority") is not False
        or common.get("anchor_id") != cross_binding_context.get("anchor_id")
        or common.get("horizon_attempt_id")
        != cross_binding_attempt.get("horizon_attempt_id")
    ):
        _fail("FOLD_PROVENANCE_FROZEN_BINDING_INVALID")
    _identity(common.get("anchor_id"), "PROVENANCE_ANCHOR_ID_INVALID", 256)
    _identity(
        common.get("horizon_attempt_id"),
        "PROVENANCE_HORIZON_ATTEMPT_ID_INVALID",
        256,
    )
    _identity(common.get("fold_id"), "PROVENANCE_HORIZON_FOLD_ID_INVALID", 256)
    fold_mapping = cross_binding_attempt.get("fold_mapping")
    if (
        type(fold_mapping) is not list
        or planned_fold.get("fold_ordinal") >= len(fold_mapping)
        or common.get("fold_id")
        != fold_mapping[planned_fold["fold_ordinal"]].get("horizon_fold_id")
    ):
        _fail("FOLD_PROVENANCE_CROSS_BINDING_INVALID")

    selection = receipts["anchor_selection_receipt"]
    for name in (
        "process_precommit_receipt_sha256",
        "horizon_plan_receipt_sha256",
    ):
        _hex(selection.get(name), 64, f"{name.upper()}_INVALID")
    if (
        selection.get("horizon_plan_receipt_sha256")
        != cross_binding["horizon_roster"]["plan_receipt_sha256"]
        or selection.get("process_precommit_receipt_sha256")
        != cross_binding_context.get("process_precommit_receipt_sha256")
        or selection.get("selection_cutoff_utc")
        != cross_binding_context.get("selection_cutoff_utc")
    ):
        _fail("HORIZON_PLAN_PROVENANCE_SPLICE")
    _utc(selection.get("selection_cutoff_utc"), "SELECTION_CUTOFF_INVALID")

    horizon = receipts["anchor_horizon_decision"]
    _identity(horizon.get("candidate_id"), "HORIZON_CANDIDATE_ID_INVALID", 256)
    for name in ("candidate_definition_sha256", "fold_definition_sha256"):
        _hex(horizon.get(name), 64, f"{name.upper()}_INVALID")
    if (
        horizon.get("horizon_process_sha256") != attempt.get("horizon_process_sha256")
        or horizon.get("candidate_id")
        != cross_binding_attempt.get("horizon_candidate_id")
        or horizon.get("candidate_definition_sha256")
        != cross_binding_attempt.get("horizon_candidate_definition_sha256")
    ):
        _fail("HORIZON_DECISION_PROVENANCE_SPLICE")
    if horizon.get("fold_definition_sha256") != fold_mapping[
        planned_fold["fold_ordinal"]
    ].get("horizon_fold_definition_sha256"):
        _fail("HORIZON_FOLD_DEFINITION_PROVENANCE_SPLICE")
    geometry_bodies = _validated_partition_geometry(
        horizon.get("partition_geometry"),
        lanes=lanes,
    )
    geometry = horizon.get("partition_geometry_receipts")
    if geometry != [item["receipt_sha256"] for item in geometry_bodies]:
        _fail("PARTITION_GEOMETRY_RECEIPT_ROSTER_INVALID")

    trial = receipts["anchor_trial_ledger"]
    if (
        trial.get("trial_registry_sha256")
        != cross_binding["horizon_roster"].get("trial_registry_sha256")
        or trial.get("attempt_roster_sha256")
        != cross_binding["horizon_roster"].get("attempt_roster_sha256")
        or trial.get("attempted_trials_exact")
        != frozen.attempt_ledger.get("attempt_count")
    ):
        _fail("TRIAL_LEDGER_PROVENANCE_SPLICE")
    _hex(
        trial.get("attempt_roster_sha256"),
        64,
        "ATTEMPT_ROSTER_PROVENANCE_INVALID",
    )

    selected = receipts["anchor_selected_trial"]
    if (
        selected.get("selection_scope") != "PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER"
        or selected.get("winner_process_sha256") != attempt.get("process_sha256")
        or selected.get("executable_trial_sha256")
        != attempt.get("trial_definition_sha256")
        or selected.get("trial_id") != cross_binding_attempt.get("horizon_trial_id")
        or selected.get("trial_definition_sha256")
        != cross_binding_attempt.get("horizon_trial_definition_sha256")
    ):
        _fail("SELECTED_TRIAL_PROVENANCE_SPLICE")
    _identity(selected.get("trial_id"), "SELECTED_TRIAL_ID_INVALID", 256)
    _hex(
        selected.get("trial_definition_sha256"),
        64,
        "SELECTED_TRIAL_DEFINITION_INVALID",
    )

    refit = receipts["anchor_refit_receipt"]
    if (
        refit.get("refit_recipe_sha256") != attempt.get("refit_recipe_sha256")
        or refit.get("post_selection_search") is not False
    ):
        _fail("REFIT_PROVENANCE_SPLICE")
    fit_start = datetime.fromisoformat(
        _utc(refit.get("fit_start_utc"), "REFIT_START_INVALID").replace("Z", "+00:00")
    )
    fit_end = datetime.fromisoformat(
        _utc(refit.get("fit_end_exclusive_utc"), "REFIT_END_INVALID").replace(
            "Z", "+00:00"
        )
    )
    oos_start = datetime.combine(
        _day(planned_fold.get("oos_start_date_utc"), "FOLD_START_INVALID"),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    if not fit_start < fit_end <= oos_start:
        _fail("REFIT_PROVENANCE_INTERVAL_INVALID")

    replay = receipts["attempt_replay_receipt"]
    expected_lanes = {name: _sha_json(dict(lanes[name])) for name in _LANE_KEYS}
    stress_receipt = _validated_stress_method_receipt(
        replay.get("stress_method_receipt"),
        frozen=frozen,
        attempt_artifacts=attempt_artifacts,
        lanes=lanes,
    )
    signal_receipt = _validated_signal_vector_receipt(
        replay.get("signal_vector_receipt")
    )
    final_axis = lanes.get("final_lockbox_base", {}).get("trade_power_partition_axis")
    if (
        replay.get("lane_sha256") != expected_lanes
        or replay.get("stress_method_receipt_sha256")
        != stress_receipt["receipt_sha256"]
        or signal_receipt.get("signal_vector_sha256")
        != stress_receipt.get("signal_vector_sha256")
        or not isinstance(final_axis, Mapping)
        or signal_receipt.get("observation_count")
        != final_axis.get("warmup_bars", 0) + final_axis.get("scored_bars", 0)
        or replay.get("position_events_are_genuine_backtester_v2") is not True
    ):
        _fail("ATTEMPT_REPLAY_PROVENANCE_SPLICE")

    source = receipts["oos_source_receipt"]
    horizon_process = attempt_artifacts.get("horizon_process")
    source_binding = frozen.source_contract_seed.get("source_binding")
    if not isinstance(horizon_process, Mapping) or not isinstance(
        source_binding, Mapping
    ):
        _fail("OOS_SOURCE_FROZEN_INPUT_INVALID")
    if (
        source.get("native_prefix_content_sha256")
        != horizon_process.get("source_snapshot_sha256")
        or source.get("source_contract_sha256")
        != frozen.precommitment.get("source_contract_sha256")
        or source.get("native_source_sha256")
        != source_binding.get("target_bars_sha256")
        or source.get("activation_manifest_sha256")
        != source_binding.get("price_activation_manifest_sha256")
        or source.get("source_group_decision_sha256")
        != cross_binding.get("source_group_decision_sha256")
        or source.get("regime_fit_decision_sha256")
        != cross_binding.get("regime_fit_decision_sha256")
        or source.get("native_prefix_index_sha256")
        != cross_binding.get("native_prefix_index_sha256")
    ):
        _fail("OOS_SOURCE_PROVENANCE_SPLICE")
    for name in (
        "native_prefix_index_sha256",
        "native_prefix_content_sha256",
        "source_contract_sha256",
        "native_source_sha256",
        "activation_manifest_sha256",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
    ):
        _hex(source.get(name), 64, f"{name.upper()}_INVALID")
    _validated_execution_spec_receipt(
        source.get("execution_spec_receipt"),
        frozen=frozen,
        cross_binding=cross_binding,
        lanes=lanes,
    )
    frame_hashes = source.get("partition_frame_sha256")
    if frame_hashes != [item["frame_sha256"] for item in geometry_bodies]:
        _fail("PARTITION_FRAME_PROVENANCE_ROSTER_INVALID")
    return receipts


def _provenance_artifacts(
    sink: SymbolAuthoritySink,
    *,
    prefix: str,
    frozen: FrozenWinnerSearchPlan,
    attempt: Mapping[str, Any],
    attempt_artifacts: Mapping[str, Any],
    cross_binding: Mapping[str, Any],
    cross_binding_context: Mapping[str, Any],
    cross_binding_attempt: Mapping[str, Any],
    planned_fold: Mapping[str, Any],
    lanes: Mapping[str, Any],
    raw: Any,
) -> tuple[dict[str, str], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    validated = _validated_provenance(
        frozen=frozen,
        attempt=attempt,
        attempt_artifacts=attempt_artifacts,
        cross_binding=cross_binding,
        cross_binding_context=cross_binding_context,
        cross_binding_attempt=cross_binding_attempt,
        planned_fold=planned_fold,
        lanes=lanes,
        raw=raw,
    )
    hashes: dict[str, str] = {}
    entries: dict[str, dict[str, Any]] = {}
    for name in _PROVENANCE_KEYS:
        body = validated[name]
        entry = _put_ascii_json(
            sink,
            f"{prefix}/provenance/{name}",
            f"winner_{name}",
            dict(body),
        )
        hashes[name] = entry["descriptor"]["sha256"]
        entries[name] = entry
    return hashes, entries, validated


def _shared_oos_partition_receipt(
    *,
    frozen: FrozenWinnerSearchPlan,
    planned_fold: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    execution_spec = source_receipt.get("execution_spec_receipt")
    if not isinstance(execution_spec, Mapping):
        _fail("SHARED_OOS_PARTITION_EXECUTION_SPEC_INVALID")
    body = {
        "schema_version": SHARED_OOS_PARTITION_SCHEMA,
        "cell_id": frozen.precommitment["cell_id"],
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "fold_ordinal": planned_fold["fold_ordinal"],
        "fold_id": planned_fold["fold_id"],
        "anchor_as_of_utc": planned_fold["anchor_as_of_utc"],
        "oos_start_date_utc": planned_fold["oos_start_date_utc"],
        "oos_end_date_utc": planned_fold["oos_end_date_utc"],
        "native_prefix_index_sha256": source_receipt["native_prefix_index_sha256"],
        "native_prefix_content_sha256": source_receipt["native_prefix_content_sha256"],
        "source_contract_sha256": source_receipt["source_contract_sha256"],
        "native_source_sha256": source_receipt["native_source_sha256"],
        "activation_manifest_sha256": source_receipt["activation_manifest_sha256"],
        "source_group_decision_sha256": source_receipt["source_group_decision_sha256"],
        "regime_fit_decision_sha256": source_receipt["regime_fit_decision_sha256"],
        "execution_spec_receipt_sha256": execution_spec["receipt_sha256"],
        "cost_contract_sha256": frozen.precommitment["cost_contract_sha256"],
        "cost_stress_contract_sha256": frozen.precommitment[
            "cost_stress_contract_sha256"
        ],
        "risk_unit_contract_sha256": frozen.precommitment["risk_unit_contract_sha256"],
    }
    return {**body, "receipt_sha256": _sha_json(body)}


def _fold_power_binding(
    *,
    attempt_ledger: Mapping[str, Any],
    train: TradePowerDecision,
    final: TradePowerDecision,
) -> dict[str, Any]:
    requirement = train.frozen_requirement
    train_sample = train.sample_evidence
    final_contract = final.partition_contract
    train_contract = train.partition_contract
    if (
        requirement is None
        or train_sample is None
        or not train.passed
        or not final.passed
        or not isinstance(train_contract, Mapping)
        or not isinstance(final_contract, Mapping)
        or type(final.required_effective_trades) is not int
        or final.required_effective_trades < 2
    ):
        _fail("TRADE_POWER_BINDING_INPUT_INVALID")
    body = {
        "schema_version": winner.TRADE_POWER_BINDING_VERSION,
        "attempt_ledger_payload_sha256": _sha_json(dict(attempt_ledger)),
        "attempted_trials_exact": int(attempt_ledger["attempt_count"]),
        "train_requirement_sha256": trade_power_requirement_sha256(requirement),
        "train_calibration_sample_sha256": train_sample.sample_sha256,
        "train_partition_contract_sha256": str(train_contract["contract_sha256"]),
        "validation_decision_sha256": trade_power_decision_sha256(final),
        "validation_partition_contract_sha256": str(final_contract["contract_sha256"]),
        "required_effective_trades": final.required_effective_trades,
    }
    return {**body, "binding_sha256": _sha_json(body)}


def _build_authenticated_fold(
    sink: SymbolAuthoritySink,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt: Mapping[str, Any],
    attempt_artifacts: Mapping[str, Any],
    cross_binding: Mapping[str, Any],
    cross_binding_context: Mapping[str, Any],
    cross_binding_attempt: Mapping[str, Any],
    planned_fold: Mapping[str, Any],
    expected_adversity_row: Mapping[str, Any],
    raw_fold: Mapping[str, Any],
    evidence_access_completed_at_utc: str,
) -> tuple[_FoldOutcome, dict[str, dict[str, Any]]]:
    ordinal = planned_fold["fold_ordinal"]
    if not isinstance(raw_fold, Mapping) or set(raw_fold) != {
        "fold_ordinal",
        "provenance",
        "lanes",
    }:
        _fail(f"FOLD_{ordinal:03d}_RAW_SCHEMA_INVALID")
    if raw_fold.get("fold_ordinal") != ordinal:
        _fail(f"FOLD_{ordinal:03d}_ORDINAL_MISMATCH")
    lanes = raw_fold.get("lanes")
    if not isinstance(lanes, Mapping) or set(lanes) != set(_LANE_KEYS):
        _fail(f"FOLD_{ordinal:03d}_LANE_ROSTER_INVALID")
    lane_bodies = {name: lanes[name] for name in _LANE_KEYS}
    if any(not isinstance(value, Mapping) for value in lane_bodies.values()):
        _fail(f"FOLD_{ordinal:03d}_LANE_INVALID")
    train = lane_bodies["optimizer_train_base"]
    tuning = lane_bodies["tuning_validation_base"]
    final = lane_bodies["final_lockbox_base"]
    stress = lane_bodies["final_lockbox_stress"]
    prefix = (
        f"{_artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)}/"
        f"attempt-{attempt['ordinal']:04d}/fold-{ordinal:03d}"
    )
    entries: dict[str, dict[str, Any]] = {}
    for name, result in lane_bodies.items():
        entries[f"raw-{name}"] = _spool_lane(
            sink, prefix=prefix, lane=name, result=result
        )
    _validate_stress_pair(final, stress, frozen=frozen)

    final_axis = _partition_axis(final, "final_lockbox")
    fold_start = datetime.combine(
        _day(planned_fold["oos_start_date_utc"], "FOLD_START_INVALID"),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    fold_end = datetime.combine(
        _day(planned_fold["oos_end_date_utc"], "FOLD_END_INVALID") + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    if (
        pd.Timestamp(final_axis["start_utc"]).to_pydatetime() != fold_start
        or pd.Timestamp(final_axis["end_utc"]).to_pydatetime() != fold_end
    ):
        _fail(f"FOLD_{ordinal:03d}_FINAL_AXIS_PLAN_MISMATCH")

    decisions = _power_chain(
        attempt_ledger=frozen.attempt_ledger,
        train=train,
        tuning=tuning,
        final=final,
        frozen=frozen,
    )
    power_artifact = _power_chain_artifact(decisions)
    entries["trade-power-chain"] = _put_ascii_json(
        sink,
        f"{prefix}/trade-power-chain",
        "winner_trade_power_chain",
        power_artifact,
    )
    risk_contract = frozen.risk_unit_contract
    base_surface, dates, base_daily = _validate_daily_surface(
        final,
        risk_contract=risk_contract,
        planned_fold=planned_fold,
    )
    stress_surface, stress_dates, stress_daily = _validate_daily_surface(
        stress,
        risk_contract=risk_contract,
        planned_fold=planned_fold,
    )
    if dates != stress_dates:
        _fail(f"FOLD_{ordinal:03d}_BASE_STRESS_DAILY_AXIS_MISMATCH")
    base_cumulative = Decimal(0)
    stress_cumulative = Decimal(0)
    cumulative_mtm_improves = False
    for base_value, stress_value in zip(base_daily, stress_daily, strict=True):
        base_cumulative += Decimal(str(base_value))
        stress_cumulative += Decimal(str(stress_value))
        if stress_cumulative > base_cumulative + Decimal("1e-12"):
            cumulative_mtm_improves = True
            break
    if cumulative_mtm_improves:
        _fail(f"FOLD_{ordinal:03d}_COST_STRESS_IMPROVES_DAILY_R")
    (
        trade_opens,
        trade_closes,
        position_ids,
        open_orders,
        close_orders,
        base_trade_r,
        stress_trade_r,
    ) = _event_vectors(
        final.get("position_events"),
        stress.get("position_events"),
        fold_start=fold_start,
        fold_end_exclusive=fold_end,
    )
    base_daily_total = sum((Decimal(str(value)) for value in base_daily), Decimal(0))
    stress_daily_total = sum(
        (Decimal(str(value)) for value in stress_daily), Decimal(0)
    )
    base_trade_total = sum((Decimal(str(value)) for value in base_trade_r), Decimal(0))
    stress_trade_total = sum(
        (Decimal(str(value)) for value in stress_trade_r), Decimal(0)
    )
    if base_daily_total != base_trade_total or stress_daily_total != stress_trade_total:
        _fail(f"FOLD_{ordinal:03d}_DAILY_TRADE_ACCOUNTING_MISMATCH")
    entries["base-daily-surface"] = _put_ascii_json(
        sink,
        f"{prefix}/base-daily-r-surface",
        "winner_base_daily_r_surface",
        base_surface,
    )
    entries["stress-daily-surface"] = _put_ascii_json(
        sink,
        f"{prefix}/stress-daily-r-surface",
        "winner_stress_daily_r_surface",
        stress_surface,
    )
    activity = {
        "schema_version": POSITION_ACTIVITY_SCHEMA,
        "attempt_id": attempt["attempt_id"],
        "fold_ordinal": ordinal,
        "base_position_events": final["position_events"],
        "stress_position_events": stress["position_events"],
    }
    entries["position-activity"] = _put_ascii_json(
        sink,
        f"{prefix}/position-activity",
        "winner_position_activity_ledger",
        activity,
    )
    provenance_hashes, provenance_entries, provenance_bodies = _provenance_artifacts(
        sink,
        prefix=prefix,
        frozen=frozen,
        attempt=attempt,
        attempt_artifacts=attempt_artifacts,
        cross_binding=cross_binding,
        cross_binding_context=cross_binding_context,
        cross_binding_attempt=cross_binding_attempt,
        planned_fold=planned_fold,
        lanes=lane_bodies,
        raw=raw_fold.get("provenance"),
    )
    entries.update(
        {f"provenance-{name}": value for name, value in provenance_entries.items()}
    )
    replay_receipt = provenance_bodies["attempt_replay_receipt"]
    stress_method_receipt = replay_receipt.get("stress_method_receipt")
    horizon_receipt = provenance_bodies["anchor_horizon_decision"]
    partition_geometry = horizon_receipt.get("partition_geometry")
    partition_geometry_receipts = horizon_receipt.get(
        "partition_geometry_receipts"
    )
    base_cost_attestation = final.get("numerical_execution_cost_attestation")
    stress_cost_attestation = stress.get("numerical_execution_cost_attestation")
    expected_declared_status = (
        expected_adversity_row.get("expected_adversity_status")
        if isinstance(expected_adversity_row, Mapping)
        else None
    )
    expected_realized_status = {
        COST_STRESS_ADVERSITY_EXPECTED_NO_SHOCK: (
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
        ),
        COST_STRESS_ADVERSITY_EXPECTED_STRICT: (
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT
        ),
    }.get(expected_declared_status)
    realized_status = (
        stress_method_receipt.get("realized_adversity_status")
        if isinstance(stress_method_receipt, Mapping)
        else None
    )
    valid_realized_status = (
        realized_status == expected_realized_status
        if expected_realized_status
        == BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
        else realized_status
        in {
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT,
            BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT,
        }
    )
    if (
        not isinstance(expected_adversity_row, Mapping)
        or not isinstance(stress_method_receipt, Mapping)
        or type(partition_geometry) is not list
        or len(partition_geometry) != 3
        or type(partition_geometry_receipts) is not list
        or len(partition_geometry_receipts) != 3
        or not isinstance(partition_geometry[2], Mapping)
        or not isinstance(base_cost_attestation, Mapping)
        or not isinstance(stress_cost_attestation, Mapping)
        or expected_adversity_row.get("attempt_ordinal")
        != attempt.get("ordinal")
        or expected_adversity_row.get("horizon_attempt_id")
        != horizon_receipt.get("horizon_attempt_id")
        or expected_adversity_row.get("fold_ordinal") != ordinal
        or expected_adversity_row.get("fold_id") != planned_fold.get("fold_id")
        or expected_adversity_row.get(
            "final_partition_geometry_receipt_sha256"
        )
        != partition_geometry_receipts[2]
        or expected_adversity_row.get("final_partition_frame_sha256")
        != partition_geometry[2].get("frame_sha256")
        or expected_adversity_row.get("base_cost_attestation_sha256")
        != base_cost_attestation.get("attestation_sha256")
        or expected_adversity_row.get("stress_cost_attestation_sha256")
        != stress_cost_attestation.get("attestation_sha256")
        or expected_adversity_row.get("base_spread_values_sha256")
        != base_cost_attestation.get("spread_values_sha256")
        or expected_adversity_row.get("stress_spread_values_sha256")
        != stress_cost_attestation.get("spread_values_sha256")
        or expected_realized_status is None
        or not valid_realized_status
    ):
        _fail("STRESS_ADVERSITY_PRECOMMITMENT_MISMATCH")
    shared_partition = _shared_oos_partition_receipt(
        frozen=frozen,
        planned_fold=planned_fold,
        source_receipt=provenance_bodies["oos_source_receipt"],
    )
    entries["shared-oos-partition"] = _put_ascii_json(
        sink,
        f"{prefix}/shared-oos-partition",
        "winner_shared_oos_partition",
        shared_partition,
    )
    daily_body = {
        "daily_dates_utc": dates,
        "daily_net_mtm_r": base_daily,
        "daily_net_mtm_r_cost_stress": stress_daily,
    }
    pbo_trade_body = {
        "accounting_identity_kind": winner.PINNED_SELECTION_POLICY[
            "accounting_identity"
        ],
        "completed_trade_open_utc": trade_opens,
        "completed_trade_close_utc": trade_closes,
        "completed_trade_position_ids": position_ids,
        "completed_trade_open_event_order": open_orders,
        "completed_trade_close_event_order": close_orders,
        "completed_trade_net_mtm_r": base_trade_r,
        "completed_trade_net_mtm_r_cost_stress": stress_trade_r,
        "completed_trade_unit": winner.PINNED_SELECTION_POLICY["daily_unit"],
        "inventory_boundary_kind": winner.PINNED_SELECTION_POLICY["inventory_boundary"],
        "position_activity_ledger_sha256": entries["position-activity"]["descriptor"][
            "sha256"
        ],
        "risk_unit_contract_sha256": frozen.precommitment["risk_unit_contract_sha256"],
    }
    pbo_fold = {
        **dict(planned_fold),
        "attempt_replay_receipt_sha256": provenance_hashes["attempt_replay_receipt"],
        "attempt_oos_source_receipt_sha256": provenance_hashes["oos_source_receipt"],
        "shared_oos_partition": shared_partition,
        "position_activity_ledger_sha256": pbo_trade_body[
            "position_activity_ledger_sha256"
        ],
        "cost_contract_sha256": frozen.precommitment["cost_contract_sha256"],
        "cost_stress_contract_sha256": frozen.precommitment[
            "cost_stress_contract_sha256"
        ],
        "risk_unit_contract_sha256": frozen.precommitment["risk_unit_contract_sha256"],
        "oos_evidence_accessed_at_utc": evidence_access_completed_at_utc,
        "fold_evaluated_at_utc": evidence_access_completed_at_utc,
        "daily_axis_kind": winner.PINNED_SELECTION_POLICY["daily_axis"],
        "daily_evidence_kind": winner.PINNED_SELECTION_POLICY["daily_evidence"],
        "daily_unit": winner.PINNED_SELECTION_POLICY["daily_unit"],
        "completed_trade_unit": winner.PINNED_SELECTION_POLICY["daily_unit"],
        "accounting_identity_kind": winner.PINNED_SELECTION_POLICY[
            "accounting_identity"
        ],
        "inventory_boundary_kind": winner.PINNED_SELECTION_POLICY["inventory_boundary"],
        **daily_body,
        "daily_vector_sha256": _sha_json(daily_body),
        **pbo_trade_body,
        "pbo_trade_vector_sha256": _sha_json(pbo_trade_body),
    }
    entries["pbo-fold"] = _put_ascii_json(
        sink,
        f"{prefix}/pbo-fold",
        "winner_pbo_evaluated_fold",
        pbo_fold,
    )
    adversity_status = stress_method_receipt.get("realized_adversity_status")
    terminal = next(
        (
            decision
            for decision in decisions
            if decision is not None and not decision.passed
        ),
        None,
    )
    if terminal is not None:
        status = _power_status(terminal)
        return _FoldOutcome(
            status=status,
            fold=None,
            pbo_fold=pbo_fold,
            reason_code=(
                terminal.reason_codes[0] if terminal.reason_codes else terminal.status
            ),
        ), entries
    if (
        adversity_status
        == BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_INSUFFICIENT
    ):
        return _FoldOutcome(
            status="PROCESS_REJECTED",
            fold=None,
            pbo_fold=pbo_fold,
            reason_code="COST_STRESS_NO_NUMERICAL_SHOCK",
        ), entries
    if adversity_status != BACKTEST_FROZEN_PATH_COST_STRESS_ADVERSITY_STRICT:
        _fail("STRESS_METHOD_ADVERSITY_STATUS_INVALID_AFTER_VALIDATION")
    train_decision, tuning_decision, final_decision = decisions
    if tuning_decision is None or final_decision is None:
        _fail("TRADE_POWER_CHAIN_INCOMPLETE")
    binding = _fold_power_binding(
        attempt_ledger=frozen.attempt_ledger,
        train=train_decision,
        final=final_decision,
    )
    trade_body = {**pbo_trade_body, "trade_power_binding": binding}
    fold = {
        **pbo_fold,
        "anchor_selection_receipt_sha256": provenance_hashes[
            "anchor_selection_receipt"
        ],
        "anchor_horizon_decision_sha256": provenance_hashes["anchor_horizon_decision"],
        "anchor_trial_ledger_sha256": provenance_hashes["anchor_trial_ledger"],
        "anchor_selected_trial_sha256": provenance_hashes["anchor_selected_trial"],
        "anchor_refit_receipt_sha256": provenance_hashes["anchor_refit_receipt"],
        "trade_power_binding": binding,
        "trade_vector_sha256": _sha_json(trade_body),
    }
    entries["winner-fold"] = _put_ascii_json(
        sink,
        f"{prefix}/winner-fold",
        "winner_authenticated_fold",
        fold,
    )
    return _FoldOutcome(
        status="AUTHENTICATED", fold=fold, pbo_fold=pbo_fold, reason_code=""
    ), entries


def _outcome_status(
    outcome: TrialOutcome,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt: Mapping[str, Any],
) -> str:
    if not isinstance(outcome, TrialOutcome):
        _fail("TRIAL_OUTCOME_INVALID")
    context = {
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "strategy_family": attempt["strategy_family"],
    }
    body = outcome.to_dict()
    if any(body.get(name) not in ("", expected) for name, expected in context.items()):
        _fail("TRIAL_OUTCOME_CONTEXT_MISMATCH")
    if outcome.kind is SearchOutcomeKind.VALID:
        _fail("VALID_TRIAL_OUTCOME_REQUIRES_RAW_FOLDS")
    if outcome.kind in {
        SearchOutcomeKind.EXPECTED_INELIGIBLE,
        SearchOutcomeKind.INVALID_PARAM,
    }:
        return "PROCESS_REJECTED"
    if outcome.kind is SearchOutcomeKind.INFRA_FAILURE:
        return "TECHNICAL_FAILURE"
    _fail("TRIAL_OUTCOME_KIND_INVALID")
    raise AssertionError


def _evidence_entry(
    attempt: Mapping[str, Any],
    *,
    status: str,
    folds: Sequence[Mapping[str, Any]],
    pbo_folds: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if status not in _V11_STATUS_REASONS:
        _fail("EVIDENCE_STATUS_INVALID")
    normalized_folds = [
        json.loads(_canonical_bytes(dict(fold)).decode("ascii")) for fold in folds
    ]
    normalized_pbo_folds = [
        json.loads(_canonical_bytes(dict(fold)).decode("ascii")) for fold in pbo_folds
    ]
    role = attempt["precommitted_evidence_role"]
    if role == "PRESEARCH_PROCESS_REJECTED":
        if status != "PROCESS_REJECTED" or normalized_folds or normalized_pbo_folds:
            _fail("PRESEARCH_REJECTED_ATTEMPT_CARRIES_EVIDENCE")
        status_reason = "PRESEARCH_PROCESS_REJECTED"
        pbo_evidence_source = "NOT_APPLICABLE_PRESEARCH_REJECTION"
    elif role == "EVALUATION_REQUIRED":
        if len(normalized_pbo_folds) == 0:
            _fail("EVALUATION_REQUIRED_PBO_FOLD_ROSTER_INCOMPLETE")
        status_reason = (
            None if status == "AUTHENTICATED" else _V11_STATUS_REASONS[status]
        )
        pbo_evidence_source = "COMPLETE_EVALUATION_REQUIRED_FOLDS"
    else:
        _fail("PRECOMMITTED_EVIDENCE_ROLE_INVALID")
    return {
        "ordinal": attempt["ordinal"],
        "attempt_id": attempt["attempt_id"],
        "process_candidate_id": attempt["process_candidate_id"],
        "process_sha256": attempt["process_sha256"],
        "horizon_process_sha256": attempt["horizon_process_sha256"],
        "trial_definition_sha256": attempt["trial_definition_sha256"],
        "refit_recipe_sha256": attempt["refit_recipe_sha256"],
        "source_contract_sha256": attempt["source_contract_sha256"],
        "cost_contract_sha256": attempt["cost_contract_sha256"],
        "cost_stress_contract_sha256": attempt["cost_stress_contract_sha256"],
        "risk_unit_contract_sha256": attempt["risk_unit_contract_sha256"],
        "evidence_status": status,
        "status_reason": status_reason,
        "fold_count": len(normalized_folds),
        "folds_sha256": _sha_json(normalized_folds),
        "folds": normalized_folds,
        "pbo_evidence_source": pbo_evidence_source,
        "pbo_fold_count": len(normalized_pbo_folds),
        "pbo_folds_sha256": _sha_json(normalized_pbo_folds),
        "pbo_folds": normalized_pbo_folds,
    }


def _verified_frozen_attempt_bundle(
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt_ledger: Mapping[str, Any],
    frozen_receipt: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    bundle = frozen.attempt_artifacts_bundle
    horizon_contracts = _normalized_horizon_execution_contracts(
        frozen.horizon_execution_contracts,
        expected_count=len(bundle),
    )
    attempts = attempt_ledger.get("attempts")
    roster = frozen_receipt.get("artifact_roster")
    if (
        type(attempts) is not list
        or type(roster) is not dict
        or len(bundle) != len(attempts)
    ):
        _fail("ATTEMPT_ARTIFACT_BUNDLE_ROSTER_INVALID")
    verified_bundle: list[dict[str, Any]] = []
    hash_names = {
        "winner_process": "process_sha256",
        "horizon_process": "horizon_process_sha256",
        "trial_definition": "trial_definition_sha256",
        "refit_recipe": "refit_recipe_sha256",
    }
    for ordinal, (raw, attempt, horizon_contract) in enumerate(
        zip(bundle, attempts, horizon_contracts, strict=True)
    ):
        if (
            not isinstance(raw, Mapping)
            or set(raw) != _ATTEMPT_ARTIFACT_KEYS
            or any(
                not isinstance(raw[name], Mapping) for name in _ATTEMPT_ARTIFACT_KEYS
            )
            or not isinstance(attempt, Mapping)
            or attempt.get("ordinal") != ordinal
        ):
            _fail(f"ATTEMPT_{ordinal:04d}_ARTIFACT_BUNDLE_INVALID")
        bodies = {name: dict(raw[name]) for name in _ATTEMPT_ARTIFACT_KEYS}
        verified = _verified_attempt_artifacts(
            bodies,
            f"ATTEMPT_{ordinal:04d}",
            symbol=frozen.symbol,
            timeframe=frozen.timeframe,
            regime=frozen.regime,
            horizon_execution_contract=horizon_contract,
        )
        verified_hashes = {
            "winner_process": verified.winner_process_sha256,
            "horizon_process": verified.horizon_process_sha256,
            "trial_definition": verified.trial_definition_sha256,
            "refit_recipe": verified.refit_recipe_sha256,
        }
        for body_name, ledger_name in hash_names.items():
            key = f"attempt-{ordinal:04d}-{body_name}"
            descriptor = roster.get(key)
            if (
                verified_hashes[body_name] != attempt.get(ledger_name)
                or type(descriptor) is not dict
                or descriptor.get("sha256") != verified_hashes[body_name]
            ):
                _fail(f"ATTEMPT_{ordinal:04d}_ARTIFACT_BUNDLE_HASH_INVALID")
        verified_bundle.append(bodies)
    return tuple(verified_bundle)


def _reopen_frozen_plan_artifacts(
    sink: SymbolAuthoritySink,
    *,
    frozen: FrozenWinnerSearchPlan,
    precommitment: Mapping[str, Any],
    attempt_ledger: Mapping[str, Any],
    source_seed: Mapping[str, Any],
    base_policy: Mapping[str, Any],
    stress_policy: Mapping[str, Any],
    risk_contract: Mapping[str, Any],
    adversity_plan: Mapping[str, Any],
    horizon_contracts: Sequence[Mapping[str, Any]],
    attempt_artifacts: Sequence[Mapping[str, Mapping[str, Any]]],
    frozen_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Reopen every pre-search artifact under its immutable sink identity.

    ``FrozenWinnerSearchPlan`` deliberately carries canonical bytes so a
    historical adapter can transport the precommitment without private spool
    access.  Those caller-held bytes are not themselves authority, however.
    Re-registering every body against the already-populated symbol sink makes
    its original artifact-id mapping authoritative: any coherent post-freeze
    reseal conflicts before evidence can be evaluated.  The independently
    reconstructed receipt below also rejects a forged bundle presented with a
    fresh, empty sink sharing the same generation context.
    """

    attempts = attempt_ledger.get("attempts")
    fold_plan = precommitment.get("causal_fold_plan")
    if (
        type(attempts) is not list
        or type(fold_plan) is not list
        or len(attempt_artifacts) != len(attempts)
        or len(horizon_contracts) != len(attempts)
        or attempt_ledger.get("run_id") != precommitment.get("run_id")
    ):
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")

    prefix = _artifact_prefix(
        frozen.artifact_namespace, frozen.timeframe, frozen.regime
    )
    multiplicity = build_trade_power_multiplicity_receipt(attempt_ledger)
    bodies: dict[str, tuple[str, Any]] = {
        "precommitment": ("winner_precommitment", dict(precommitment)),
        "attempt_ledger": ("winner_attempt_ledger", dict(attempt_ledger)),
        "source_contract_seed": ("winner_source_contract_seed", dict(source_seed)),
        "base_cost_contract": ("winner_base_cost_contract", dict(base_policy)),
        "stress_cost_contract": ("winner_stress_cost_contract", dict(stress_policy)),
        "risk_unit_contract": ("winner_risk_unit_contract", dict(risk_contract)),
        "cost_stress_adversity_plan": (
            "winner_cost_stress_adversity_plan",
            dict(adversity_plan),
        ),
        "horizon_execution_contracts": (
            "winner_horizon_execution_contracts",
            [dict(contract) for contract in horizon_contracts],
        ),
        "trade_power_policy": (
            "winner_trade_power_policy",
            asdict(PRODUCTION_TRADE_POWER_POLICY),
        ),
        "multiplicity_receipt": ("winner_trade_power_multiplicity", multiplicity),
    }
    reopened: dict[str, dict[str, Any]] = {}
    for name, (artifact_type, body) in bodies.items():
        reopened[name] = _put_ascii_json(
            sink, f"{prefix}/frozen/{name}", artifact_type, body
        )
    for ordinal, (attempt, artifacts) in enumerate(
        zip(attempts, attempt_artifacts, strict=True)
    ):
        if (
            attempt.get("ordinal") != ordinal
            or set(artifacts) != _ATTEMPT_ARTIFACT_KEYS
        ):
            _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
        for name in sorted(_ATTEMPT_ARTIFACT_KEYS):
            key = f"attempt-{ordinal:04d}-{name}"
            reopened[key] = _put_ascii_json(
                sink,
                f"{prefix}/frozen/attempt-{ordinal:04d}/{name}",
                f"winner_attempt_{name}",
                dict(artifacts[name]),
            )

    registries = {
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    expected_receipt = {
        "schema_version": FROZEN_PLAN_SCHEMA,
        "generation_id": sink.context.generation_id,
        "generation_context_sha256": sink.context.context_sha256,
        "native_universe_sha256": sink.context.native_universe_sha256,
        "config_sha256": sink.context.config_sha256,
        "artifact_namespace": frozen.artifact_namespace,
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "run_id": precommitment.get("run_id"),
        "precommitment_sha256": _sha_json(precommitment),
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "attempts_sha256": _sha_json(attempts),
        "attempt_count": len(attempts),
        "fold_plan_sha256": _sha_json(fold_plan),
        "fold_count": len(fold_plan),
        "horizon_execution_contracts_sha256": _sha_json(horizon_contracts),
        "cost_stress_adversity_plan_sha256": _sha_json(adversity_plan),
        "multiplicity_receipt_sha256": multiplicity.get("receipt_sha256"),
        **registries,
        "artifact_roster": {
            key: entry["descriptor"] for key, entry in sorted(reopened.items())
        },
    }

    # Reopen the receipt before comparing the reconstructed schema.  With the
    # original sink, a changed byte must hit ARTIFACT_ID_CONFLICT; with a fresh
    # sink, exact body equality below still prevents accepting a resealed plan.
    receipt_entry = _put_ascii_json(
        sink,
        f"{prefix}/frozen/receipt",
        "winner_frozen_search_plan_receipt",
        dict(frozen_receipt),
    )
    if dict(frozen_receipt) != expected_receipt:
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
    if frozen.frozen_receipt_entry != receipt_entry:
        _fail("FROZEN_RECEIPT_ENTRY_BINDING_INVALID")
    return expected_receipt


def produce_winner_v11_raw_evidence(
    sink: SymbolAuthoritySink,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt_evidence: Iterable[Mapping[str, Any]],
    evidence_access_started_at_utc: str,
    evidence_access_completed_at_utc: str,
    provenance_cross_binding_bytes: Optional[bytes] = None,
    production_cell_search_plan: Any = None,
    production_search_process: Any = None,
    artifact_namespace: Optional[str] = None,
) -> WinnerRawEvidenceProduction:
    """Consume raw search evidence, run V11 power lineage, and spool authority.

    ``artifact_namespace`` must equal the namespace bound at freeze time;
    any mismatch (including namespaced-vs-current-lane) fails closed.
    Legacy/v2 HISTORICAL bindings retain the released anchor semantics.
    A v2 CURRENT binding additionally requires the genuine sealed production
    cell plan and frozen search process; both are reopened before the evidence
    iterator is touched and confer no downstream trading authority.
    """

    if type(sink) is not SymbolAuthoritySink or not isinstance(
        frozen, FrozenWinnerSearchPlan
    ):
        _fail("PRODUCER_INPUT_INVALID")
    if sink.symbol != frozen.symbol:
        _fail("AUTHORITY_SINK_SYMBOL_MISMATCH")
    if sink.context.context_sha256 != frozen.generation_context_sha256:
        _fail("FROZEN_PLAN_CONTEXT_MISMATCH")
    if _validated_namespace(artifact_namespace) != frozen.artifact_namespace:
        _fail("ARTIFACT_NAMESPACE_MISMATCH")
    precommitment = frozen.precommitment
    attempt_ledger = frozen.attempt_ledger
    risk_contract = frozen.risk_unit_contract
    base_policy = _validated_frozen_cost_policy(
        frozen.base_cost_policy,
        lane="BASE",
        code="FROZEN_BASE_COST_POLICY_INVALID",
    )
    stress_policy = _validated_frozen_cost_policy(
        frozen.stress_cost_policy,
        lane="STRESS",
        code="FROZEN_STRESS_COST_POLICY_INVALID",
    )
    source_seed = frozen.source_contract_seed
    _validated_partition_cost_attestation(
        source_seed.get("numerical_execution_cost_attestation"),
        policy=base_policy,
        lane="BASE",
        code="FROZEN_SOURCE_SEED_COST_ATTESTATION_INVALID",
    )
    if (
        _sha_json(base_policy) != precommitment.get("cost_contract_sha256")
        or _sha_json(stress_policy) != precommitment.get("cost_stress_contract_sha256")
        or base_policy["numerical_execution_cost_policy_sha256"]
        == stress_policy["numerical_execution_cost_policy_sha256"]
        or _validated_source_identity(source_seed)
        != precommitment.get("source_contract_sha256")
    ):
        _fail("FROZEN_COST_POLICY_BINDING_INVALID")
    receipt = _decode_object(frozen.frozen_receipt_bytes, "FROZEN_RECEIPT_INVALID")
    horizon_contracts = _normalized_horizon_execution_contracts(
        frozen.horizon_execution_contracts,
        expected_count=attempt_ledger.get("attempt_count"),
    )
    horizon_contracts_sha256 = _sha_json(horizon_contracts)
    adversity_plan = _normalized_cost_stress_adversity_plan(
        frozen.cost_stress_adversity_plan,
        symbol=frozen.symbol,
        timeframe=frozen.timeframe,
        regime=frozen.regime,
        attempts=attempt_ledger.get("attempts"),
        folds=precommitment.get("causal_fold_plan"),
    )
    adversity_plan_sha256 = _sha_json(adversity_plan)
    adversity_rows_by_key = {
        (row["attempt_ordinal"], row["fold_ordinal"]): row
        for row in adversity_plan["rows"]
    }
    if len(adversity_rows_by_key) != adversity_plan["row_count"]:
        _fail("COST_STRESS_ADVERSITY_PLAN_ROSTER_DUPLICATED")
    frozen_roster = receipt.get("artifact_roster")
    horizon_contract_descriptor = (
        frozen_roster.get("horizon_execution_contracts")
        if isinstance(frozen_roster, Mapping)
        else None
    )
    adversity_plan_descriptor = (
        frozen_roster.get("cost_stress_adversity_plan")
        if isinstance(frozen_roster, Mapping)
        else None
    )
    if (
        receipt.get("precommitment_sha256") != _sha_json(precommitment)
        or receipt.get("attempt_ledger_sha256") != _sha_json(attempt_ledger)
        or receipt.get("attempts_sha256") != attempt_ledger.get("attempts_sha256")
        or receipt.get("fold_plan_sha256")
        != precommitment.get("causal_fold_plan_sha256")
        or receipt.get("horizon_execution_contracts_sha256") != horizon_contracts_sha256
        or receipt.get("cost_stress_adversity_plan_sha256")
        != adversity_plan_sha256
        or not isinstance(horizon_contract_descriptor, Mapping)
        or horizon_contract_descriptor.get("sha256") != horizon_contracts_sha256
        or not isinstance(adversity_plan_descriptor, Mapping)
        or adversity_plan_descriptor.get("sha256") != adversity_plan_sha256
        or "artifact_namespace" not in receipt
        or receipt["artifact_namespace"] != frozen.artifact_namespace
    ):
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
    attempt_bundles = _verified_frozen_attempt_bundle(
        frozen=frozen,
        attempt_ledger=attempt_ledger,
        frozen_receipt=receipt,
    )
    receipt = _reopen_frozen_plan_artifacts(
        sink,
        frozen=frozen,
        precommitment=precommitment,
        attempt_ledger=attempt_ledger,
        source_seed=source_seed,
        base_policy=base_policy,
        stress_policy=stress_policy,
        risk_contract=risk_contract,
        adversity_plan=adversity_plan,
        horizon_contracts=horizon_contracts,
        attempt_artifacts=attempt_bundles,
        frozen_receipt=receipt,
    )
    (
        cross_binding,
        cross_binding_attempts,
        cross_binding_context,
    ) = _validated_horizon_winner_cross_binding(
        provenance_cross_binding_bytes,
        sink=sink,
        frozen=frozen,
        attempt_artifacts=attempt_bundles,
        production_cell_search_plan=production_cell_search_plan,
        production_search_process=production_search_process,
    )
    started = _utc(evidence_access_started_at_utc, "EVIDENCE_ACCESS_START_INVALID")
    completed = _utc(
        evidence_access_completed_at_utc, "EVIDENCE_ACCESS_COMPLETED_INVALID"
    )
    started_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
    completed_dt = datetime.fromisoformat(completed.replace("Z", "+00:00"))
    frozen_dt = datetime.fromisoformat(
        attempt_ledger["frozen_at_utc"].replace("Z", "+00:00")
    )
    boundary_dt = datetime.fromisoformat(
        precommitment["latest_evidence_boundary_utc"].replace("Z", "+00:00")
    )
    if started_dt <= frozen_dt or started_dt < boundary_dt or completed_dt < started_dt:
        _fail("EVIDENCE_ACCESS_INTERVAL_INVALID")
    raw_iterator = iter(attempt_evidence)
    evidence_entries: list[dict[str, Any]] = []
    spool_entries: dict[str, dict[str, Any]] = {}
    for attempt, attempt_artifacts, cross_binding_attempt in zip(
        attempt_ledger["attempts"],
        attempt_bundles,
        cross_binding_attempts,
        strict=True,
    ):
        try:
            raw = next(raw_iterator)
        except StopIteration as exc:
            raise WinnerRawEvidenceError("ATTEMPT_EVIDENCE_ROSTER_OMISSION") from exc
        ordinal = attempt["ordinal"]
        if (
            not isinstance(raw, Mapping)
            or raw.get("attempt_id") != attempt["attempt_id"]
        ):
            _fail(f"ATTEMPT_{ordinal:04d}_EVIDENCE_IDENTITY_MISMATCH")
        prefix = (
            f"{_artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)}/"
            f"attempt-{ordinal:04d}"
        )
        if set(raw) == {"attempt_id", "outcome"}:
            outcome = raw.get("outcome")
            status = _outcome_status(outcome, frozen=frozen, attempt=attempt)
            outcome_entry = _put_ascii_json(
                sink,
                f"{prefix}/search-outcome",
                "winner_search_outcome",
                outcome.to_dict(),
            )
            spool_entries[f"attempt-{ordinal:04d}-outcome"] = outcome_entry
            if status == "TECHNICAL_FAILURE":
                raise WinnerRawEvidenceIncomplete(
                    "ATTEMPT_TECHNICAL_EVIDENCE_FAILURE",
                    str(outcome.reason_code),
                )
            if attempt["precommitted_evidence_role"] != "PRESEARCH_PROCESS_REJECTED":
                raise WinnerRawEvidenceIncomplete(
                    "EVALUATION_REQUIRED_ATTEMPT_OUTCOME_ONLY",
                    str(outcome.reason_code),
                )
            evidence_entries.append(
                _evidence_entry(attempt, status=status, folds=[], pbo_folds=[])
            )
            continue
        if attempt["precommitted_evidence_role"] != "EVALUATION_REQUIRED":
            _fail(f"ATTEMPT_{ordinal:04d}_PRESEARCH_REJECTED_CARRIES_RAW_FOLDS")
        if set(raw) != {"attempt_id", "folds"}:
            _fail(f"ATTEMPT_{ordinal:04d}_EVIDENCE_SCHEMA_INVALID")
        raw_folds = raw.get("folds")
        planned_folds = precommitment["causal_fold_plan"]
        if type(raw_folds) is not list or len(raw_folds) != len(planned_folds):
            _fail(f"ATTEMPT_{ordinal:04d}_FOLD_ROSTER_INCOMPLETE")
        folds: list[dict[str, Any]] = []
        pbo_folds: list[dict[str, Any]] = []
        attempt_status = "AUTHENTICATED"
        seen_position_ids: set[str] = set()
        for planned_fold, raw_fold in zip(planned_folds, raw_folds, strict=True):
            try:
                outcome, fold_entries = _build_authenticated_fold(
                    sink,
                    frozen=frozen,
                    attempt=attempt,
                    attempt_artifacts=attempt_artifacts,
                    cross_binding=cross_binding,
                    cross_binding_context=cross_binding_context,
                    cross_binding_attempt=cross_binding_attempt,
                    planned_fold=planned_fold,
                    expected_adversity_row=adversity_rows_by_key.get(
                        (ordinal, planned_fold["fold_ordinal"])
                    ),
                    raw_fold=raw_fold,
                    evidence_access_completed_at_utc=completed,
                )
            except WinnerRawEvidenceIncomplete:
                raise
            except WinnerRawEvidenceError as exc:
                raise WinnerRawEvidenceIncomplete(
                    "ATTEMPT_TECHNICAL_EVIDENCE_FAILURE", exc.code
                ) from exc
            for name, entry in fold_entries.items():
                spool_entries[
                    f"attempt-{ordinal:04d}-fold-"
                    f"{planned_fold['fold_ordinal']:03d}-{name}"
                ] = entry
            if outcome.status == "TECHNICAL_FAILURE":
                raise WinnerRawEvidenceIncomplete(
                    "ATTEMPT_TECHNICAL_EVIDENCE_FAILURE", outcome.reason_code
                )
            if outcome.pbo_fold is None:
                _fail("EVALUATION_REQUIRED_PBO_FOLD_MISSING")
            pbo_folds.append(outcome.pbo_fold)
            if outcome.status == "PROCESS_REJECTED":
                attempt_status = "PROCESS_REJECTED"
                continue
            if outcome.fold is None:
                _fail("AUTHENTICATED_FOLD_MISSING")
            # Mirror V11: one position id may appear in only one fold.
            fold_positions = set(outcome.fold["completed_trade_position_ids"])
            if seen_position_ids & fold_positions:
                _fail(f"ATTEMPT_{ordinal:04d}_POSITION_ID_REUSED_ACROSS_FOLDS")
            seen_position_ids |= fold_positions
            folds.append(outcome.fold)
        if len(pbo_folds) != len(planned_folds):
            _fail("EVALUATION_REQUIRED_PBO_FOLD_ROSTER_INCOMPLETE")
        if attempt_status == "PROCESS_REJECTED":
            folds = []
        evidence_entries.append(
            _evidence_entry(
                attempt,
                status=attempt_status,
                folds=folds,
                pbo_folds=pbo_folds,
            )
        )
    try:
        next(raw_iterator)
    except StopIteration:
        pass
    else:
        _fail("ATTEMPT_EVIDENCE_ROSTER_EXTRA")

    common_names = (
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_contract_sha256",
        "cost_contract_sha256",
        "cost_stress_contract_sha256",
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    )
    evidence_ledger = {
        "contract_version": winner.EVIDENCE_LEDGER_VERSION,
        "run_id": precommitment["run_id"],
        "generated_at_utc": completed,
        "evidence_access_started_at_utc": started,
        "evidence_access_completed_at_utc": completed,
        "latest_evidence_boundary_utc": precommitment["latest_evidence_boundary_utc"],
        "precommitment_payload_sha256": _sha_json(precommitment),
        "attempt_ledger_payload_sha256": _sha_json(attempt_ledger),
        **{name: precommitment[name] for name in common_names},
        "attempt_count": len(attempt_ledger["attempts"]),
        "entries_sha256": _sha_json(evidence_entries),
        "entries": evidence_entries,
        "evidence_scope": "PROCESS_LEVEL_ONLY",
        "parameter_evidence_inherited": False,
    }
    request = winner.create_winner_selection_request(
        precommitment, attempt_ledger, evidence_ledger
    )
    if request is None:
        _fail("WINNER_V11_REQUEST_REJECTED")
    if len(_canonical_bytes(request.to_dict())) > winner.MAX_REQUEST_BYTES:
        _fail("WINNER_V11_REQUEST_BYTE_BOUND_EXCEEDED")
    decision = winner.evaluate_historical_cell_winner(request)
    if not winner.verify_historical_cell_winner_decision(decision, request):
        _fail("WINNER_V11_DECISION_VERIFICATION_FAILED")
    marginal = winner.historical_book_marginal_receipt(decision, request)
    prefix = (
        f"{_artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)}"
        "/authority"
    )
    final_bodies = {
        "evidence-ledger": ("winner_evidence_ledger", evidence_ledger),
        "winner-request": ("winner_v11_request", request.to_dict()),
        "winner-decision": ("winner_v11_decision", decision.to_dict()),
    }
    if marginal is not None:
        final_bodies["book-marginal-receipt"] = (
            "winner_book_marginal_receipt",
            marginal,
        )
    for name, (artifact_type, body) in final_bodies.items():
        spool_entries[name] = _put_ascii_json(
            sink, f"{prefix}/{name}", artifact_type, body
        )
    production = {
        "schema_version": PRODUCTION_SCHEMA,
        "generation_context_sha256": sink.context.context_sha256,
        "run_id": precommitment["run_id"],
        "artifact_namespace": frozen.artifact_namespace,
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "precommitment_sha256": _sha_json(precommitment),
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "provenance_cross_binding_sha256": hashlib.sha256(
            provenance_cross_binding_bytes
        ).hexdigest(),
        "evidence_ledger_sha256": _sha_json(evidence_ledger),
        "request_sha256": request.request_sha256,
        "decision_receipt_sha256": decision.receipt_sha256,
        "artifact_roster": {
            name: entry["descriptor"] for name, entry in sorted(spool_entries.items())
        },
    }
    spool_entries["production-receipt"] = _put_ascii_json(
        sink,
        f"{prefix}/production-receipt",
        "winner_raw_evidence_production_receipt",
        production,
    )
    return WinnerRawEvidenceProduction(
        evidence_ledger=evidence_ledger,
        request=request,
        decision=decision,
        marginal_receipt=marginal,
        entries=spool_entries,
    )


__all__ = [
    "CURRENT_CROSS_BINDING_MODE",
    "DAILY_R_SURFACE_SCHEMA",
    "FROZEN_COST_POLICY_SCHEMA",
    "FROZEN_PLAN_SCHEMA",
    "HISTORICAL_CROSS_BINDING_MODE",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA_V2",
    "PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS",
    "PRODUCTION_TRADE_POWER_POLICY",
    "FrozenWinnerSearchPlan",
    "WinnerRawEvidenceError",
    "WinnerRawEvidenceIncomplete",
    "WinnerRawEvidenceProduction",
    "build_flat_daily_r_surface",
    "build_frozen_numerical_execution_cost_policy",
    "build_precommitted_risk_unit_contract",
    "build_trade_source_contract_from_backtester_result",
    "freeze_winner_search_plan_v3",
    "produce_winner_v11_raw_evidence",
]
