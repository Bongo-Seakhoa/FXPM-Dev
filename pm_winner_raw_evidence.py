"""Search-time Winner-v14 evidence capture and authority spooling."""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd

import pm_authority_spool as authority_spool
import pm_current_refit_artifacts as refit_artifacts
import pm_winner_selection as winner
from pm_authority_spool import ARTIFACT_DESCRIPTOR_SCHEMA, SymbolAuthoritySink
from pm_core import (
    BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
    BACKTEST_COST_LADDER_CONTRACT_SCHEMA_VERSION,
    BACKTEST_FROZEN_PATH_COST_LADDER_ADVERSITY_INSUFFICIENT,
    BACKTEST_FROZEN_PATH_COST_LADDER_ADVERSITY_STRICT,
    BACKTEST_FROZEN_PATH_COST_LADDER_SCHEMA_VERSION,
    BACKTEST_TRADE_IDENTITY_PNL_FIELDS,
    BACKTEST_WINNER_DAILY_R_SCHEMA_VERSION,
    BACKTEST_WINNER_RISK_UNIT_SCHEMA_VERSION,
    InstrumentSpec,
    build_backtest_cost_ladder_contract,
    build_backtest_frozen_tier_pair_contract,
    validate_backtest_frozen_tier_pair_contract,
    build_backtest_position_events,
    build_backtest_winner_risk_unit_contract,
    canonical_backtest_winner_r,
)
from pm_search_outcomes import SearchOutcomeKind, TrialOutcome
from pm_spread_profile import ADMISSION_POLICY, ADMISSION_SCHEMA, FrozenSpreadTier, align_prior_completed_spreads, spread_admission
from pm_trade_power import (
    NUMERICAL_EXECUTION_COST_ATTESTATION_VERSION,
    REQUIRED_TRADE_RECORD_SCHEMA,
    TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
    TradePowerDecision,
    TradePowerPolicy,
    build_numerical_execution_cost_attestation,
    build_numerical_execution_cost_policy,
    build_trade_power_multiplicity_receipt,
    build_trade_power_partition_contract,
    build_trade_sample_evidence,
    calibrate_train_trade_power,
    evaluate_trade_power_partition,
    trade_power_decision_sha256,
    trade_power_requirement_sha256,
)


ARCHIVED_FROZEN_PLAN_SCHEMA = "fxpm.winner-search-plan-frozen.v3"
FROZEN_PLAN_SCHEMA = ARCHIVED_FROZEN_PLAN_SCHEMA
CURRENT_FROZEN_PLAN_SCHEMA = "fxpm.winner-search-plan-frozen.v4"
V16_FROZEN_PLAN_SCHEMA = "fxpm.winner-search-plan-frozen.v5"
FROZEN_COST_LADDER_CONTRACT_SCHEMA = BACKTEST_COST_LADDER_CONTRACT_SCHEMA_VERSION
ARCHIVED_RAW_LANE_SCHEMA = "fxpm.winner-raw-backtester-lane.v4"
RAW_LANE_SCHEMA = ARCHIVED_RAW_LANE_SCHEMA
CURRENT_RAW_LANE_SCHEMA = "fxpm.winner-raw-backtester-lane.v5"
DAILY_R_SURFACE_SCHEMA = BACKTEST_WINNER_DAILY_R_SCHEMA_VERSION
RISK_UNIT_SCHEMA = BACKTEST_WINNER_RISK_UNIT_SCHEMA_VERSION
ARCHIVED_POSITION_ACTIVITY_SCHEMA = "fxpm.winner-position-activity-ledger.v2"
POSITION_ACTIVITY_SCHEMA = ARCHIVED_POSITION_ACTIVITY_SCHEMA
CURRENT_POSITION_ACTIVITY_SCHEMA = "fxpm.winner-position-activity-ledger.v3"
SHARED_OOS_PARTITION_SCHEMA = winner.SHARED_OOS_PARTITION_VERSION
POWER_CHAIN_SCHEMA = "fxpm.winner-trade-power-chain.v1"
ARCHIVED_PRODUCTION_SCHEMA = "fxpm.winner-raw-evidence-production.v4"
PRODUCTION_SCHEMA = ARCHIVED_PRODUCTION_SCHEMA
CURRENT_PRODUCTION_SCHEMA = "fxpm.winner-raw-evidence-production.v5"
V16_PRODUCTION_SCHEMA = "fxpm.winner-raw-evidence-production.v6"
ARCHIVED_AUTHORITATIVE_PROVENANCE_SCHEMA = "fxpm.authoritative-search-provenance.v3"
AUTHORITATIVE_PROVENANCE_SCHEMA = ARCHIVED_AUTHORITATIVE_PROVENANCE_SCHEMA
CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA = "fxpm.authoritative-search-provenance.v4"
REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA = "fxpm.authoritative-search-provenance.v5"
HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA = (
    "fxpm.historical-horizon-winner-cross-binding.v2"
)
CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA = (
    "fxpm.horizon-winner-cross-binding.v3"
)
HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3 = "fxpm.historical-horizon-winner-cross-binding.v3"
CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4 = "fxpm.horizon-winner-cross-binding.v4"
HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4 = "fxpm.historical-horizon-winner-cross-binding.v4"
CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5 = "fxpm.horizon-winner-cross-binding.v5"
HISTORICAL_CROSS_BINDING_MODE = "HISTORICAL"
CURRENT_CROSS_BINDING_MODE = "CURRENT"
PARTITION_GEOMETRY_SCHEMA = "fxpm.winner-partition-geometry.v1"
SIGNAL_VECTOR_SCHEMA = "fxpm.authoritative-search-signal-vector.v1"
SOURCE_COUNT_RECEIPT_SCHEMA = "fxpm.historical-attempt-source-count.v1"
EXECUTION_SPEC_RECEIPT_SCHEMA = "fxpm.execution-instrument-spec-receipt.v2"
COST_LADDER_ADVERSITY_PLAN_SCHEMA = (
    "fxpm.winner-cost-ladder-adversity-plan.v1"
)
COST_LADDER_ADVERSITY_EXPECTED_NO_SHOCK = (
    BACKTEST_FROZEN_PATH_COST_LADDER_ADVERSITY_INSUFFICIENT
)
COST_LADDER_ADVERSITY_EXPECTED_STRICT = (
    "STRICT_NUMERICAL_ADVERSITY_REQUIRED"
)
MAX_SIGNAL_VECTOR_OBSERVATIONS = 5_000_000
FROZEN_PATH_COST_LADDER_SCHEMA = BACKTEST_FROZEN_PATH_COST_LADDER_SCHEMA_VERSION
ASCII_JSON_FORMAT = "fxpm-ascii-json-v1"
PARTITION_COST_PRECOMMITMENTS_SCHEMA = "fxpm.winner-partition-cost-precommitments.v1"
PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA = "fxpm.winner-partition-cost-precommitments.v2"
INDEPENDENT_TIER_EXECUTION_SCHEMA = "fxpm.independent-frozen-tier-execution.v1"
SOURCE_OBSERVATIONS_SCHEMA = "fxpm.original-fine-spread-observations.v1"
SOURCE_OBSERVATION_REFERENCE_SCHEMA = "fxpm.original-fine-spread-observation-reference.v1"
SOURCE_OBSERVATION_ARTIFACT_TYPE = "winner_source_observation_receipt"
_TIERS = ("P75", "P95")
_PARTITIONS = ("optimizer_train", "tuning_validation", "final_lockbox")

PRODUCTION_TRADE_POWER_POLICY = TradePowerPolicy(
    minimum_economic_edge_r=0.25,
    maximum_ci_half_width_r=0.25,
    target_power=0.80,
)
PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS = 8

_HEX = frozenset("0123456789abcdef")
_BACKTEST_POSITION_ID = re.compile(r"btp:[0-9a-f]{64}\Z", re.ASCII)
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
    "optimizer_train_p75",
    "optimizer_train_p95",
    "tuning_validation_p75",
    "tuning_validation_p95",
    "final_lockbox_p75",
    "final_lockbox_p95",
)
_ARCHIVED_LANE_KEYS = (
    "optimizer_train_base", "tuning_validation_base", "final_lockbox_base", "final_lockbox_p75", "final_lockbox_p95",
)
_WINNER_STATUS_REASONS = {
    "AUTHENTICATED": None,
    "PROCESS_REJECTED": "EVALUATED_OOS_PROCESS_REJECTED",
    "TECHNICAL_FAILURE": "EVALUATOR_TECHNICAL_FAILURE",
}
_PRECOMMITTED_EVIDENCE_ROLES = frozenset(
    {"EVALUATION_REQUIRED", "PRESEARCH_PROCESS_REJECTED"}
)
_COST_LADDER_LEVEL_IDS = (
    "BASE",
    "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P75",
    "UNIFORM_ALL_FILLS_SINGLE_FILL_UPLIFT_P95",
)


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
    cost_ladder_contract_bytes: bytes
    risk_unit_contract_bytes: bytes
    cost_ladder_adversity_plan_bytes: bytes
    frozen_receipt_bytes: bytes
    frozen_receipt_entry_bytes: bytes
    horizon_execution_contracts_bytes: bytes
    # Canonical attempt bodies are retained as an immutable, bounded bundle so
    # evidence-time provenance can be reconstructed from the exact executable
    # inputs frozen before search.  Hash-only attempt ledgers are insufficient
    # for binding source snapshots and refit geometry at this boundary.
    attempt_artifacts_bundle_bytes: bytes = b""
    source_contract_seeds_bytes: bytes = b""
    frozen_tier_pair_contract_bytes: bytes = b""
    partition_cost_precommitments_bytes: bytes = b""

    @property
    def selection_generation(self):
        return winner.winner_generation_for_precommitment(self.precommitment)

    @property
    def source_contract_seeds(self) -> dict[str, Any]:
        return _decode_object(self.source_contract_seeds_bytes, "SOURCE_SEEDS_BYTES_INVALID")

    @property
    def frozen_tier_pair_contract(self) -> dict[str, Any]:
        return _decode_object(self.frozen_tier_pair_contract_bytes, "FROZEN_TIER_PAIR_CONTRACT_BYTES_INVALID")

    @property
    def partition_cost_precommitments(self) -> dict[str, Any]:
        return _decode_object(self.partition_cost_precommitments_bytes, "PARTITION_COST_PRECOMMITMENTS_BYTES_INVALID")

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
    def cost_ladder_contract(self) -> dict[str, Any]:
        return _decode_object(
            self.cost_ladder_contract_bytes,
            "COST_LADDER_CONTRACT_BYTES_INVALID",
        )

    @property
    def risk_unit_contract(self) -> dict[str, Any]:
        return _decode_object(self.risk_unit_contract_bytes, "RISK_UNIT_BYTES_INVALID")

    @property
    def cost_ladder_adversity_plan(self) -> dict[str, Any]:
        return _decode_object(
            self.cost_ladder_adversity_plan_bytes,
            "COST_LADDER_ADVERSITY_PLAN_BYTES_INVALID",
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
    fold: dict[str, Any]
    shared_fold_axis_row: dict[str, Any]
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


def _backtester_exact_sha256(value: Any) -> str:
    """Mirror the Backtester type-tagged exact hash for transported trade rows."""

    def exact(item: Any) -> Any:
        if item is None:
            return {"type": "none"}
        if isinstance(item, bool):
            return {"type": "bool", "value": item}
        if isinstance(item, int):
            return {"type": "int", "value": str(item)}
        if isinstance(item, float):
            if not math.isfinite(item):
                _fail("BACKTESTER_EXACT_FLOAT_INVALID")
            return {"type": "float64", "value": item.hex().lower()}
        if isinstance(item, str):
            return {"type": "str", "value": item}
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                _fail("BACKTESTER_EXACT_MAPPING_INVALID")
            return {
                "type": "mapping",
                "value": {key: exact(item[key]) for key in sorted(item)},
            }
        if isinstance(item, list):
            return {"type": "list", "value": [exact(child) for child in item]}
        _fail("BACKTESTER_EXACT_VALUE_INVALID")

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
    current = bool(frozen.frozen_tier_pair_contract_bytes)
    if current:
        pair = validate_backtest_frozen_tier_pair_contract(frozen.frozen_tier_pair_contract)
        expected_instrument_hashes = {item["numerical_execution_cost_policy"]["instrument_spec_sha256"] for item in pair["tiers"]}
        expected_instrument_hashes |= {seed["numerical_execution_cost_attestation"]["instrument_spec_sha256"]
            for seed in frozen.source_contract_seeds.values()}
    else:
        ladder_contract = _validated_cost_ladder_contract(frozen.cost_ladder_contract, code="FROZEN_COST_LADDER_CONTRACT_INVALID")
        expected_instrument_hashes = {policy.get("instrument_spec_sha256") for policy in _cost_ladder_policies(ladder_contract)} | {
            (frozen.source_contract_seed.get("numerical_execution_cost_attestation") or {}).get("instrument_spec_sha256")}
    for lane in (_LANE_KEYS if current else _ARCHIVED_LANE_KEYS):
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


def _utc_second(value: Any, code: str) -> str:
    """Canonicalize a timestamp only when no subsecond information is lost."""

    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WinnerRawEvidenceError(code) from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.microsecond != 0
    ):
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


def _validated_cost_ladder_contract(
    value: Any,
    *,
    code: str,
) -> dict[str, Any]:
    """Rebuild the one frozen ordered BASE/P75/P95 policy contract."""

    if not isinstance(value, Mapping):
        _fail(code)
    try:
        contract = json.loads(_canonical_bytes(dict(value)).decode("ascii"))
    except WinnerRawEvidenceError as exc:
        raise WinnerRawEvidenceError(code) from exc
    levels = contract.get("levels")
    if (
        set(contract)
        != {
            "schema_version",
            "percentile_choice_kind",
            "p95_over_max_rationale",
            "levels",
        }
        or contract.get("schema_version") != FROZEN_COST_LADDER_CONTRACT_SCHEMA
        or type(levels) is not list
        or len(levels) != len(_COST_LADDER_LEVEL_IDS)
        or any(not isinstance(level, Mapping) for level in levels)
        or tuple(level.get("level_id") for level in levels)
        != _COST_LADDER_LEVEL_IDS
    ):
        _fail(code)
    base_level = levels[0]
    base_policy = base_level.get("numerical_execution_cost_policy")
    if not isinstance(base_level.get("spread_model"), Mapping) or not isinstance(
        base_policy, Mapping
    ):
        _fail(code)
    try:
        rebuilt = build_backtest_cost_ladder_contract(
            base_level["spread_model"],
            use_spread=base_policy.get("use_spread"),
            instrument_spec_sha256=base_policy.get("instrument_spec_sha256"),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError(code) from exc
    policies: list[dict[str, Any]] = []
    for level in levels:
        policy = level.get("numerical_execution_cost_policy")
        if not isinstance(policy, Mapping):
            _fail(code)
        try:
            normalized_policy = build_numerical_execution_cost_policy(
                use_spread=policy.get("use_spread"),
                spread_mode=policy.get("spread_mode"),
                instrument_spec_sha256=policy.get("instrument_spec_sha256"),
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise WinnerRawEvidenceError(code) from exc
        if normalized_policy != dict(policy) or policy.get("use_spread") is not True:
            _fail(code)
        policies.append(normalized_policy)
    if (
        rebuilt != contract
        or len({policy["policy_sha256"] for policy in policies}) != len(policies)
        or len({policy["instrument_spec_sha256"] for policy in policies}) != 1
    ):
        _fail(code)
    return rebuilt


def _cost_ladder_policies(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    levels = contract["levels"]
    policies = tuple(
        dict(level["numerical_execution_cost_policy"]) for level in levels
    )
    return policies[0], policies[1], policies[2]


def _validated_partition_cost_attestation(
    value: Any,
    *,
    policy: Mapping[str, Any],
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
            entry_half_spread_values_sha256=attestation.get(
                "entry_half_spread_values_sha256"
            ),
            intrabar_half_spread_values_sha256=attestation.get(
                "intrabar_half_spread_values_sha256"
            ),
            instrument_spec_sha256=attestation.get("instrument_spec_sha256"),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError(code) from exc
    if rebuilt != attestation:
        _fail(code)
    derived_policy = build_numerical_execution_cost_policy(
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
        int(winner.CURRENT_GENERATION.policy["min_causal_folds"])
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
            int(winner.CURRENT_GENERATION.policy["min_days_per_fold"])
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


def _normalized_cost_ladder_adversity_plan(
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
        "p75_cost_attestation_sha256",
        "p95_cost_attestation_sha256",
        "base_entry_half_spread_values_sha256",
        "p75_entry_half_spread_values_sha256",
        "p95_entry_half_spread_values_sha256",
        "base_intrabar_half_spread_values_sha256",
        "p75_intrabar_half_spread_values_sha256",
        "p95_intrabar_half_spread_values_sha256",
        "expected_p75_adversity_status",
        "expected_p95_adversity_status",
    }
    if not isinstance(value, Mapping) or set(value) != expected_body_keys:
        _fail("COST_LADDER_ADVERSITY_PLAN_SCHEMA_INVALID")
    if (
        not isinstance(attempts, (list, tuple))
        or not isinstance(folds, (list, tuple))
        or any(not isinstance(item, Mapping) for item in attempts)
        or any(not isinstance(item, Mapping) for item in folds)
    ):
        _fail("COST_LADDER_ADVERSITY_PLAN_FROZEN_ROSTER_INVALID")
    rows = value.get("rows")
    expected_pairs = [
        (attempt, fold)
        for attempt in attempts
        if attempt.get("precommitted_evidence_role") == "EVALUATION_REQUIRED"
        for fold in folds
    ]
    if (
        value.get("schema_version") != COST_LADDER_ADVERSITY_PLAN_SCHEMA
        or value.get("symbol") != symbol
        or value.get("timeframe") != timeframe
        or value.get("regime") != regime
        or type(value.get("row_count")) is not int
        or value.get("row_count") != len(expected_pairs)
        or type(rows) is not list
        or len(rows) != len(expected_pairs)
    ):
        _fail("COST_LADDER_ADVERSITY_PLAN_BINDING_INVALID")

    normalized_rows: list[dict[str, Any]] = []
    horizon_attempt_ids: dict[int, str] = {}
    for row_ordinal, (raw, (attempt, fold)) in enumerate(
        zip(rows, expected_pairs, strict=True)
    ):
        label = f"COST_LADDER_ADVERSITY_PLAN_ROW_{row_ordinal:04d}"
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
            "p75_cost_attestation_sha256",
            "p95_cost_attestation_sha256",
            "base_entry_half_spread_values_sha256",
            "p75_entry_half_spread_values_sha256",
            "p95_entry_half_spread_values_sha256",
            "base_intrabar_half_spread_values_sha256",
            "p75_intrabar_half_spread_values_sha256",
            "p95_intrabar_half_spread_values_sha256",
        ):
            _hex(row.get(name), 64, f"{label}_{name.upper()}_INVALID")
        if (
            len(
                {
                    row["base_cost_attestation_sha256"],
                    row["p75_cost_attestation_sha256"],
                    row["p95_cost_attestation_sha256"],
                }
            )
            != 3
        ):
            _fail(f"{label}_COST_ATTESTATION_NOT_DISTINCT")
        if len(
            {
                row["base_intrabar_half_spread_values_sha256"],
                row["p75_intrabar_half_spread_values_sha256"],
                row["p95_intrabar_half_spread_values_sha256"],
            }
        ) != 1:
            _fail(f"{label}_INTRABAR_VECTOR_DRIFT")
        for level in ("p75", "p95"):
            expected_status = (
                COST_LADDER_ADVERSITY_EXPECTED_NO_SHOCK
                if row["base_entry_half_spread_values_sha256"]
                == row[f"{level}_entry_half_spread_values_sha256"]
                else COST_LADDER_ADVERSITY_EXPECTED_STRICT
            )
            if row.get(f"expected_{level}_adversity_status") != expected_status:
                _fail(f"{label}_{level.upper()}_EXPECTED_STATUS_INVALID")
        normalized_rows.append(row)

    normalized = {
        "schema_version": COST_LADDER_ADVERSITY_PLAN_SCHEMA,
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
        _fail("COST_LADDER_ADVERSITY_PLAN_HASH_INVALID")
    return normalized


def _current_pair_inputs(source_contract_seeds: Any, frozen_tier_pair_contract: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    if (not isinstance(source_contract_seeds, Mapping) or set(source_contract_seeds) != set(_TIERS)
            or any(not isinstance(source_contract_seeds[tier], Mapping) for tier in _TIERS)):
        _fail("SOURCE_SEED_TIER_ROSTER_INVALID")
    try:
        pair = validate_backtest_frozen_tier_pair_contract(frozen_tier_pair_contract)
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise WinnerRawEvidenceError("FROZEN_TIER_PAIR_CONTRACT_INVALID") from exc
    seeds = {tier: dict(source_contract_seeds[tier]) for tier in _TIERS}
    source_hashes, cost_hashes = {}, {}
    for tier, level in zip(_TIERS, pair["tiers"], strict=True):
        policy = level["numerical_execution_cost_policy"]
        _validated_partition_cost_attestation(seeds[tier].get("numerical_execution_cost_attestation"),
            policy=policy, code=f"{tier}_SOURCE_SEED_COST_ATTESTATION_INVALID")
        source_hashes[tier] = _validated_source_identity(seeds[tier])
        cost_hashes[tier] = _sha_json(policy)
    for field in ("source_binding", "trade_record_schema_version", "execution_cost_timing_contract", "swap_accrual_contract"):
        if seeds["P75"].get(field) != seeds["P95"].get(field):
            _fail("SOURCE_SEED_PAIR_NATIVE_BINDING_MISMATCH")
    return seeds, pair, {"source_contract_sha256": _sha_json(source_hashes),
        "cost_contract_sha256": _sha_json(cost_hashes), "frozen_tier_pair_contract_sha256": _sha_json(pair)}


def _validated_partition_cost_precommitments_v1(value: Any, *, pair: Mapping[str, Any],
        attempts: Sequence[Mapping[str, Any]], folds: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"schema_version", "frozen_tier_pair_contract_sha256", "rows", "receipt_sha256"}:
        _fail("PARTITION_COST_PRECOMMITMENTS_SCHEMA_INVALID")
    body = dict(value)
    receipt_sha = body.pop("receipt_sha256")
    rows = body.get("rows")
    if (body.get("schema_version") != PARTITION_COST_PRECOMMITMENTS_SCHEMA
            or body.get("frozen_tier_pair_contract_sha256") != _sha_json(pair)
            or receipt_sha != _sha_json(body) or type(rows) is not list):
        _fail("PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID")
    if (type(attempts) is not list or type(folds) is not list
            or any(not isinstance(attempt, Mapping) or type(attempt.get("ordinal")) is not int
                   or attempt.get("precommitted_evidence_role") not in _PRECOMMITTED_EVIDENCE_ROLES for attempt in attempts)
            or any(not isinstance(fold, Mapping) or type(fold.get("fold_ordinal")) is not int for fold in folds)):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    expected = [(attempt["ordinal"], fold["fold_ordinal"], partition)
        for attempt in attempts if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
        for fold in folds for partition in _PARTITIONS]
    if len(rows) != len(expected):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    for row, identity in zip(rows, expected, strict=True):
        if (not isinstance(row, Mapping) or set(row) != {"attempt_ordinal", "fold_ordinal", "partition",
                "partition_geometry_receipt_sha256", "partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}
                or (row["attempt_ordinal"], row["fold_ordinal"], row["partition"]) != identity
                or not isinstance(row["tiers"], Mapping) or set(row["tiers"]) != set(_TIERS)):
            _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
        for name in ("partition_geometry_receipt_sha256", "partition_frame_sha256", "source_observation_receipt_sha256"):
            _hex(row[name], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
        for tier, level in zip(_TIERS, pair["tiers"], strict=True):
            data = row["tiers"][tier]
            if not isinstance(data, Mapping) or set(data) != {"numerical_execution_cost_attestation",
                    "entry_observations_sha256", "entry_admission_mask_sha256"}:
                _fail("PARTITION_COST_PRECOMMITMENTS_TIER_INVALID")
            _validated_partition_cost_attestation(data["numerical_execution_cost_attestation"],
                policy=level["numerical_execution_cost_policy"], code="PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID")
            for name in ("entry_observations_sha256", "entry_admission_mask_sha256"):
                _hex(data[name], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
    return json.loads(_canonical_bytes(dict(value)))


def _compact_partition_cost_record(row: Mapping[str, Any], *,
        pair: Mapping[str, Any]) -> dict[str, Any]:
    if (not isinstance(row, Mapping) or set(row) != {"attempt_ordinal", "fold_ordinal", "partition",
            "partition_geometry_receipt_sha256", "partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}
            or type(row["attempt_ordinal"]) is not int or type(row["fold_ordinal"]) is not int
            or row["partition"] not in _PARTITIONS
            or not isinstance(row["tiers"], Mapping) or set(row["tiers"]) != set(_TIERS)):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    for name in ("partition_geometry_receipt_sha256", "partition_frame_sha256", "source_observation_receipt_sha256"):
        _hex(row[name], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
    tiers = {}
    for tier, level in zip(_TIERS, pair["tiers"], strict=True):
        data = row["tiers"][tier]
        if not isinstance(data, Mapping) or set(data) != {"numerical_execution_cost_attestation",
                "entry_observations_sha256", "entry_admission_mask_sha256"}:
            _fail("PARTITION_COST_PRECOMMITMENTS_TIER_INVALID")
        attestation = _validated_partition_cost_attestation(data["numerical_execution_cost_attestation"],
            policy=level["numerical_execution_cost_policy"], code="PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID")
        for name in ("entry_observations_sha256", "entry_admission_mask_sha256"):
            _hex(data[name], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
        tiers[tier] = {
            "entry_half_spread_values_sha256": attestation["entry_half_spread_values_sha256"],
            "intrabar_half_spread_values_sha256": attestation["intrabar_half_spread_values_sha256"],
            "numerical_execution_cost_attestation_sha256": _sha_json(attestation),
            "entry_observations_sha256": data["entry_observations_sha256"],
            "entry_admission_mask_sha256": data["entry_admission_mask_sha256"],
        }
    return {"partition_frame_sha256": row["partition_frame_sha256"],
        "source_observation_receipt_sha256": row["source_observation_receipt_sha256"], "tiers": tiers}


def build_partition_cost_precommitments(rows: Sequence[Mapping[str, Any]], *,
        pair: Mapping[str, Any]) -> dict[str, Any]:
    """Store exact repeated geometry once without changing its cost commitments."""
    if type(rows) is not list:
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    records, record_sources, identities = {}, {}, []
    for row in rows:
        if (not isinstance(row, Mapping) or set(row) != {"attempt_ordinal", "fold_ordinal", "partition",
                "partition_geometry_receipt_sha256", "partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}
                or type(row["attempt_ordinal"]) is not int or type(row["fold_ordinal"]) is not int
                or row["partition"] not in _PARTITIONS):
            _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
        identity = row["partition_geometry_receipt_sha256"]
        _hex(identity, 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
        source = {key: row[key] for key in (
            "partition_frame_sha256", "source_observation_receipt_sha256", "tiers")}
        if identity in record_sources and record_sources[identity] != source:
            _fail("PARTITION_COST_PRECOMMITMENTS_RECORD_CONFLICT")
        if identity not in records:
            records[identity] = _compact_partition_cost_record(row, pair=pair)
            record_sources[identity] = source
        identities.append({key: row[key] for key in (
            "attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256")})
    body = {"schema_version": PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA,
        "frozen_tier_pair_contract_sha256": _sha_json(pair), "rows": identities, "records": records}
    return {**body, "receipt_sha256": _sha_json(body)}


def _expanded_partition_cost_record(record: Any, *, pair: Mapping[str, Any]) -> dict[str, Any]:
    if (not isinstance(record, Mapping) or set(record) != {
            "partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}):
        _fail("PARTITION_COST_PRECOMMITMENTS_RECORD_INVALID")
    for name in ("partition_frame_sha256", "source_observation_receipt_sha256"):
        _hex(record[name], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
    if not isinstance(record["tiers"], Mapping) or set(record["tiers"]) != set(_TIERS):
        _fail("PARTITION_COST_PRECOMMITMENTS_TIER_INVALID")
    tiers = {}
    for tier, level in zip(_TIERS, pair["tiers"], strict=True):
        data = record["tiers"][tier]
        if not isinstance(data, Mapping) or set(data) != {
                "entry_half_spread_values_sha256", "intrabar_half_spread_values_sha256",
                "numerical_execution_cost_attestation_sha256", "entry_observations_sha256",
                "entry_admission_mask_sha256"}:
            _fail("PARTITION_COST_PRECOMMITMENTS_TIER_INVALID")
        for value in data.values():
            _hex(value, 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
        policy = level["numerical_execution_cost_policy"]
        try:
            attestation = build_numerical_execution_cost_attestation(
                use_spread=policy["use_spread"], spread_mode=policy["spread_mode"],
                instrument_spec_sha256=policy["instrument_spec_sha256"],
                entry_half_spread_values_sha256=data["entry_half_spread_values_sha256"],
                intrabar_half_spread_values_sha256=data["intrabar_half_spread_values_sha256"])
        except (TypeError, ValueError, KeyError, OverflowError) as exc:
            raise WinnerRawEvidenceError("PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID") from exc
        _validated_partition_cost_attestation(attestation, policy=policy,
            code="PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID")
        if _sha_json(attestation) != data["numerical_execution_cost_attestation_sha256"]:
            _fail("PARTITION_COST_PRECOMMITMENTS_ATTESTATION_INVALID")
        tiers[tier] = {"numerical_execution_cost_attestation": attestation,
            "entry_observations_sha256": data["entry_observations_sha256"],
            "entry_admission_mask_sha256": data["entry_admission_mask_sha256"]}
    return {"partition_frame_sha256": record["partition_frame_sha256"],
        "source_observation_receipt_sha256": record["source_observation_receipt_sha256"], "tiers": tiers}


def expand_partition_cost_precommitment(precommitments: Mapping[str, Any], row: Mapping[str, Any], *,
        pair: Mapping[str, Any]) -> dict[str, Any]:
    """Expand one selected row; archived v1 bodies retain their original meaning."""
    if precommitments.get("schema_version") == PARTITION_COST_PRECOMMITMENTS_SCHEMA:
        return dict(row)
    if precommitments.get("schema_version") != PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA:
        _fail("PARTITION_COST_PRECOMMITMENTS_SCHEMA_INVALID")
    if precommitments.get("frozen_tier_pair_contract_sha256") != _sha_json(pair):
        _fail("PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID")
    if (not isinstance(row, Mapping) or set(row) != {
            "attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256"}):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    identity = _hex(row["partition_geometry_receipt_sha256"], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
    records = precommitments.get("records")
    if not isinstance(records, Mapping) or identity not in records:
        _fail("PARTITION_COST_PRECOMMITMENTS_RECORD_UNAVAILABLE")
    return {**dict(row), **_expanded_partition_cost_record(records[identity], pair=pair)}


def _validated_partition_cost_precommitments(value: Any, *, pair: Mapping[str, Any],
        attempts: Sequence[Mapping[str, Any]], folds: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if isinstance(value, Mapping) and value.get("schema_version") == PARTITION_COST_PRECOMMITMENTS_SCHEMA:
        return _validated_partition_cost_precommitments_v1(value, pair=pair, attempts=attempts, folds=folds)
    if not isinstance(value, Mapping) or set(value) != {
            "schema_version", "frozen_tier_pair_contract_sha256", "rows", "records", "receipt_sha256"}:
        _fail("PARTITION_COST_PRECOMMITMENTS_SCHEMA_INVALID")
    body = dict(value)
    receipt_sha = body.pop("receipt_sha256")
    rows, records = body.get("rows"), body.get("records")
    if (body.get("schema_version") != PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
            or body.get("frozen_tier_pair_contract_sha256") != _sha_json(pair)
            or receipt_sha != _sha_json(body) or type(rows) is not list):
        _fail("PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID")
    if (type(attempts) is not list or type(folds) is not list
            or any(not isinstance(attempt, Mapping) or type(attempt.get("ordinal")) is not int
                   or attempt.get("precommitted_evidence_role") not in _PRECOMMITTED_EVIDENCE_ROLES for attempt in attempts)
            or any(not isinstance(fold, Mapping) or type(fold.get("fold_ordinal")) is not int for fold in folds)):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    expected = [(attempt["ordinal"], fold["fold_ordinal"], partition)
        for attempt in attempts if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
        for fold in folds for partition in _PARTITIONS]
    if len(rows) != len(expected):
        _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
    if not isinstance(records, Mapping):
        _fail("PARTITION_COST_PRECOMMITMENTS_RECORD_INVALID")
    used = set()
    for row, identity in zip(rows, expected, strict=True):
        if (not isinstance(row, Mapping) or set(row) != {
                "attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256"}
                or (row["attempt_ordinal"], row["fold_ordinal"], row["partition"]) != identity):
            _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
        geometry = _hex(row["partition_geometry_receipt_sha256"], 64, "PARTITION_COST_PRECOMMITMENTS_HASH_INVALID")
        if geometry not in used:
            expand_partition_cost_precommitment(value, row, pair=pair)
            used.add(geometry)
    if set(records) != used:
        _fail("PARTITION_COST_PRECOMMITMENTS_RECORD_ROSTER_INVALID")
    return json.loads(_canonical_bytes(dict(value)))


@dataclass(frozen=True)
class _PartitionCostIndex:
    frozen: FrozenWinnerSearchPlan
    partition_bytes: bytes
    precommitments: Mapping[str, Any]
    rows_by_fold: Mapping[tuple[int, int], list[Mapping[str, Any]]]


def _index_partition_cost_precommitments(
    frozen: FrozenWinnerSearchPlan, validated: Mapping[str, Any],
) -> _PartitionCostIndex:
    rows_by_fold: dict[tuple[int, int], list[Mapping[str, Any]]] = {}
    for row in validated["rows"]:
        rows_by_fold.setdefault((row["attempt_ordinal"], row["fold_ordinal"]), []).append(row)
    return _PartitionCostIndex(
        frozen, frozen.partition_cost_precommitments_bytes, validated, rows_by_fold)


def _selected_partition_cost_precommitments(
    *, frozen: FrozenWinnerSearchPlan, attempt: Mapping[str, Any], planned_fold: Mapping[str, Any],
    partition_cost_index: Optional[_PartitionCostIndex] = None,
) -> list[dict[str, Any]]:
    if partition_cost_index is None:
        precommitments = _validated_partition_cost_precommitments(frozen.partition_cost_precommitments,
            pair=frozen.frozen_tier_pair_contract, attempts=frozen.attempt_ledger["attempts"],
            folds=frozen.precommitment["causal_fold_plan"])
        rows = [row for row in precommitments["rows"] if row["attempt_ordinal"] == attempt["ordinal"]
            and row["fold_ordinal"] == planned_fold["fold_ordinal"]]
    else:
        if (type(partition_cost_index) is not _PartitionCostIndex
                or partition_cost_index.frozen is not frozen
                or partition_cost_index.partition_bytes != frozen.partition_cost_precommitments_bytes):
            _fail("PARTITION_COST_PRECOMMITMENTS_BINDING_INVALID")
        precommitments = partition_cost_index.precommitments
        rows = partition_cost_index.rows_by_fold.get((attempt["ordinal"], planned_fold["fold_ordinal"]), [])
    return [expand_partition_cost_precommitment(precommitments, row, pair=frozen.frozen_tier_pair_contract)
            for row in rows]


def _restored_frozen_context_identity(context_identity: Any) -> dict[str, str]:
    identity_fields = {"generation_id", "generation_context_sha256",
                       "native_universe_sha256", "config_sha256"}
    if not isinstance(context_identity, Mapping) or set(context_identity) != identity_fields:
        _fail("FROZEN_PLAN_RESTORE_CONTEXT_INVALID")
    identity = dict(context_identity)
    _identity(identity["generation_id"], "FROZEN_PLAN_RESTORE_CONTEXT_INVALID", 512)
    for name in identity_fields - {"generation_id"}:
        _hex(identity[name], 64, "FROZEN_PLAN_RESTORE_CONTEXT_INVALID")
    return identity


def _build_frozen_winner_search_plan_receipt(
    *,
    context_identity: Mapping[str, str],
    symbol: str,
    timeframe: str,
    regime: str,
    artifact_namespace: Optional[str],
    precommitment: Mapping[str, Any],
    attempt_ledger: Mapping[str, Any],
    horizon_execution_contracts: Sequence[Mapping[str, Any]],
    partition_cost_precommitments: Mapping[str, Any],
    artifact_entries: Mapping[str, Mapping[str, Any]],
    _generation=winner.CURRENT_GENERATION,
) -> dict[str, Any]:
    """Reconstruct the canonical current receipt without reading or writing a sink."""
    identity = _restored_frozen_context_identity(context_identity)
    if (not isinstance(precommitment, Mapping) or not isinstance(attempt_ledger, Mapping)
            or not isinstance(partition_cost_precommitments, Mapping)
            or not isinstance(artifact_entries, Mapping)
            or any(type(key) is not str or not isinstance(entry, Mapping)
                   or not isinstance(entry.get("descriptor"), Mapping)
                   for key, entry in artifact_entries.items())):
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
    attempts = attempt_ledger.get("attempts")
    fold_plan = precommitment.get("causal_fold_plan")
    if (type(attempts) is not list or type(fold_plan) is not list
            or attempt_ledger.get("run_id") != precommitment.get("run_id")
            or len(horizon_execution_contracts) != len(attempts)):
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
    multiplicity = build_trade_power_multiplicity_receipt(attempt_ledger)
    registries = {
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    return {
        "schema_version": V16_FROZEN_PLAN_SCHEMA if _generation is winner.V16_GENERATION else CURRENT_FROZEN_PLAN_SCHEMA,
        **identity,
        "artifact_namespace": artifact_namespace,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "run_id": precommitment.get("run_id"),
        "precommitment_sha256": _sha_json(precommitment),
        "attempt_ledger_sha256": _sha_json(attempt_ledger),
        "attempts_sha256": _sha_json(attempts),
        "attempt_count": len(attempts),
        "fold_plan_sha256": _sha_json(fold_plan),
        "fold_count": len(fold_plan),
        "horizon_execution_contracts_sha256": _sha_json(horizon_execution_contracts),
        "partition_cost_precommitments_sha256": _sha_json(partition_cost_precommitments),
        "multiplicity_receipt_sha256": multiplicity.get("receipt_sha256"),
        **registries,
        "artifact_roster": {
            key: entry["descriptor"] for key, entry in sorted(artifact_entries.items())
        },
    }


def _freeze_winner_search_plan(
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
    source_contract_seeds: Mapping[str, Any],
    frozen_tier_pair_contract: Mapping[str, Any],
    risk_unit_contract: Mapping[str, Any],
    partition_cost_precommitments: Mapping[str, Any],
    artifact_namespace: Optional[str] = None,
    _generation=winner.CURRENT_GENERATION,
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

    source_seeds, pair_contract, pair_hashes = _current_pair_inputs(source_contract_seeds, frozen_tier_pair_contract)
    risk_contract = dict(risk_unit_contract)
    if risk_contract != build_precommitted_risk_unit_contract():
        _fail("RISK_UNIT_CONTRACT_INVALID")
    common_hashes = {
        **pair_hashes,
        "risk_unit_contract_sha256": _sha_json(risk_contract),
    }

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
    partition_precommits = _validated_partition_cost_precommitments(partition_cost_precommitments,
        pair=pair_contract, attempts=attempts, folds=normalized_folds)
    partition_precommits_sha256 = _sha_json(partition_precommits)
    registries = {
        "process_registry_sha256": _process_registry(attempts),
        "horizon_registry_sha256": _horizon_registry(attempts),
        "trial_registry_sha256": _trial_registry(attempts),
    }
    run_id = _sha_json(
        {
            "schema_version": V16_FROZEN_PLAN_SCHEMA if _generation is winner.V16_GENERATION else CURRENT_FROZEN_PLAN_SCHEMA,
            "generation_context_sha256": sink.context.context_sha256,
            "artifact_namespace": namespace,
            "precommitted_at_utc": precommitted_at,
            "symbol": normalized_symbol,
            "timeframe": normalized_timeframe,
            "regime": normalized_regime,
            "attempts": attempts,
            "fold_plan": normalized_folds,
            "horizon_execution_contracts_sha256": horizon_contracts_sha256,
            "partition_cost_precommitments_sha256": partition_precommits_sha256,
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
        "contract_version": _generation.precommitment_version,
        "run_id": run_id,
        "precommitted_at_utc": precommitted_at,
        **common,
        "selection_policy_sha256": _generation.policy_sha256,
        "frozen_tier_pair_contract": pair_contract,
        "partition_cost_precommitments_sha256": partition_precommits_sha256,
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
        "partition_cost_precommitments_sha256": partition_precommits_sha256,
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
        "source_contract_seeds": ("winner_source_contract_seeds", source_seeds),
        "frozen_tier_pair_contract": ("winner_frozen_tier_pair_contract", pair_contract),
        "risk_unit_contract": ("winner_risk_unit_contract", risk_contract),
        "partition_cost_precommitments": ("winner_partition_cost_precommitments", partition_precommits),
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
    receipt_body = _build_frozen_winner_search_plan_receipt(
        context_identity={
            "generation_id": sink.context.generation_id,
            "generation_context_sha256": sink.context.context_sha256,
            "native_universe_sha256": sink.context.native_universe_sha256,
            "config_sha256": sink.context.config_sha256,
        },
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
        artifact_namespace=namespace,
        precommitment=precommitment,
        attempt_ledger=attempt_ledger,
        horizon_execution_contracts=normalized_horizon_contracts,
        partition_cost_precommitments=partition_precommits,
        artifact_entries=spool_entries,
        _generation=_generation,
    )
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
    partition_bytes = _canonical_bytes(partition_precommits)
    if len(partition_bytes) > winner.MAX_REQUEST_BYTES:
        _fail("PARTITION_COST_PRECOMMITMENTS_BYTE_BOUND_EXCEEDED")
    return FrozenWinnerSearchPlan(
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
        artifact_namespace=namespace,
        generation_context_sha256=sink.context.context_sha256,
        precommitment_bytes=_canonical_bytes(precommitment),
        attempt_ledger_bytes=_canonical_bytes(attempt_ledger),
        source_contract_seed_bytes=b"",
        cost_ladder_contract_bytes=b"",
        risk_unit_contract_bytes=_canonical_bytes(risk_contract),
        cost_ladder_adversity_plan_bytes=b"",
        frozen_receipt_bytes=receipt_bytes,
        frozen_receipt_entry_bytes=_canonical_bytes(receipt_entry),
        horizon_execution_contracts_bytes=horizon_contracts_bytes,
        attempt_artifacts_bundle_bytes=attempt_bundle_bytes,
        source_contract_seeds_bytes=_canonical_bytes(source_seeds),
        frozen_tier_pair_contract_bytes=_canonical_bytes(pair_contract),
        partition_cost_precommitments_bytes=partition_bytes,
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


_TRADE_IDENTITY_PNL_FIELDS = BACKTEST_TRADE_IDENTITY_PNL_FIELDS
_TRADE_PATH_IDENTITY_FIELDS = _TRADE_IDENTITY_PNL_FIELDS[:-1]


def _trade_identity_pnl_evidence(
    result: Mapping[str, Any],
    *,
    lane: str,
) -> list[dict[str, Any]]:
    """Project the exact trade identity and PnL needed by the raw verifier."""

    trades = result.get("trade_identity_pnl")
    code = f"{lane.upper()}_TRADE_EVIDENCE_INVALID"
    if type(trades) is not list:
        _fail(code)
    projected: list[dict[str, Any]] = []
    for ordinal, raw in enumerate(trades):
        if not isinstance(raw, Mapping) or set(raw) != set(
            _TRADE_IDENTITY_PNL_FIELDS
        ):
            _fail(f"{code}_{ordinal:05d}")
        row = {field: raw[field] for field in _TRADE_IDENTITY_PNL_FIELDS}
        entry_bar = row["entry_bar"]
        exit_bar = row["exit_bar"]
        signal_bar = row["signal_bar"]
        direction = row["direction"]
        exit_reason = row["exit_reason"]
        if (
            type(entry_bar) is not int
            or type(exit_bar) is not int
            or type(signal_bar) is not int
            or entry_bar < 0
            or exit_bar < entry_bar
            or signal_bar < 0
            or type(direction) is not str
            or direction not in {"LONG", "SHORT"}
            or type(exit_reason) is not str
            or not exit_reason
            or len(exit_reason.encode("utf-8")) > 256
        ):
            _fail(f"{code}_{ordinal:05d}")
        entry_time = _utc(row["entry_time"], f"{code}_{ordinal:05d}_ENTRY_TIME")
        exit_time = _utc(row["exit_time"], f"{code}_{ordinal:05d}_EXIT_TIME")
        if (
            row["entry_time"] != entry_time
            or row["exit_time"] != exit_time
            or exit_time < entry_time
        ):
            _fail(f"{code}_{ordinal:05d}")
        for field in ("position_size", "risk_amount", "pnl_dollars"):
            raw_number = row[field]
            if (
                type(raw_number) is not float
                or not math.isfinite(raw_number)
                or (field != "pnl_dollars" and raw_number <= 0.0)
            ):
                _fail(f"{code}_{ordinal:05d}_{field.upper()}")
        canonical = {
            "entry_bar": entry_bar,
            "exit_bar": exit_bar,
            "signal_bar": signal_bar,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "direction": direction,
            "position_size": row["position_size"],
            "risk_amount": row["risk_amount"],
            "exit_reason": exit_reason,
            "pnl_dollars": row["pnl_dollars"],
        }
        if row != canonical:
            _fail(f"{code}_{ordinal:05d}")
        projected.append(row)
    return projected


def _canonical_trade_events(
    trades: Sequence[Mapping[str, Any]],
    *,
    namespace: str,
    lane: str,
) -> list[dict[str, Any]]:
    try:
        return build_backtest_position_events(trades, namespace=namespace)
    except (TypeError, ValueError, OverflowError) as exc:
        raise WinnerRawEvidenceError(
            f"{lane.upper()}_TRADE_POSITION_RECONCILIATION_INVALID"
        ) from exc


def _trade_position_reconciliation_evidence(
    events: Sequence[Mapping[str, Any]],
    *,
    lane: str,
) -> list[dict[str, Any]]:
    fields = (
        "trade_record_schema_version",
        "position_id",
        "entry_time",
        "exit_time",
        "entry_event_order",
        "exit_event_order",
        "pnl_dollars",
        "risk_amount",
    )
    result: list[dict[str, Any]] = []
    for ordinal, event in enumerate(events):
        if not isinstance(event, Mapping) or any(field not in event for field in fields):
            _fail(f"{lane.upper()}_TRADE_POSITION_{ordinal:05d}_INVALID")
        result.append(
            {
                **{field: event[field] for field in fields},
                "entry_time": _utc(
                    event["entry_time"],
                    f"{lane.upper()}_TRADE_POSITION_{ordinal:05d}_ENTRY_TIME_INVALID",
                ),
                "exit_time": _utc(
                    event["exit_time"],
                    f"{lane.upper()}_TRADE_POSITION_{ordinal:05d}_EXIT_TIME_INVALID",
                ),
            }
        )
    return result


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
        "schema_version": CURRENT_RAW_LANE_SCHEMA,
        "lane": lane,
        "trade_identity_pnl": _trade_identity_pnl_evidence(result, lane=lane),
        "position_events": events,
        "winner_daily_r_surface": result.get("winner_daily_r_surface"),
        "trade_power_partition_axis": result.get("trade_power_partition_axis"),
        "execution_cost_timing_contract": result.get("execution_cost_timing_contract"),
        "numerical_execution_cost_attestation": result.get(
            "numerical_execution_cost_attestation"
        ),
        "swap_accrual_contract": result.get("swap_accrual_contract"),
        "spread_model": result.get("spread_model"),
        "spread_admission": result.get("spread_admission"),
    }


def _validate_tier_result(
    result: Mapping[str, Any],
    *,
    frozen: FrozenWinnerSearchPlan,
    partition: str,
    tier: str,
) -> dict[str, Any]:
    pair = validate_backtest_frozen_tier_pair_contract(frozen.frozen_tier_pair_contract)
    policy = next(item["numerical_execution_cost_policy"] for item in pair["tiers"] if item["tier"] == tier)
    _validated_partition_cost_attestation(
        result.get("numerical_execution_cost_attestation"),
        policy=policy,
        code=f"{partition.upper()}_{tier}_COST_ATTESTATION_INVALID",
    )
    source = _source_contract_from_result(result)
    observed_source_sha = _validated_source_identity(source)
    expected_source_sha = _validated_source_identity(frozen.source_contract_seeds[tier])
    if observed_source_sha != expected_source_sha:
        _fail(f"{partition.upper()}_SOURCE_CONTRACT_DRIFT")
    axis = _partition_axis(result, partition)
    if axis["timeframe"] != frozen.timeframe:
        _fail(f"{partition.upper()}_PARTITION_AXIS_TIMEFRAME_DRIFT")
    if type(result.get("position_events")) is not list:
        _fail(f"{partition.upper()}_POSITION_EVENTS_INVALID")
    return source




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
    tier: str,
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
        source_contract=_validate_tier_result(
            train, frozen=frozen, partition="optimizer_train", tier=tier
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
        source_contract=_validate_tier_result(
            tuning, frozen=frozen, partition="tuning_validation", tier=tier
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
        source_contract=_validate_tier_result(
            final, frozen=frozen, partition="final_lockbox", tier=tier
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




def _validated_self_hashed_receipt(
    value: Any,
    *,
    role: str,
    expected_keys: frozenset[str],
    schema_version: str = ARCHIVED_AUTHORITATIVE_PROVENANCE_SCHEMA,
) -> dict[str, Any]:
    code = role.upper()
    if not isinstance(value, Mapping) or set(value) != set(expected_keys):
        _fail(f"{code}_PROVENANCE_SCHEMA_INVALID")
    body = dict(value)
    if (
        body.get("schema_version") != schema_version
        or body.get("receipt_role") != role
    ):
        _fail(f"{code}_PROVENANCE_ROLE_INVALID")
    receipt_sha = body.pop("receipt_sha256", None)
    _hex(receipt_sha, 64, f"{code}_PROVENANCE_RECEIPT_INVALID")
    if receipt_sha != _sha_json(body):
        _fail(f"{code}_PROVENANCE_RECEIPT_INVALID")
    return dict(value)


def _validated_cost_ladder_method_receipt(
    value: Any,
    *,
    frozen: FrozenWinnerSearchPlan,
    attempt_artifacts: Mapping[str, Any],
    lanes: Mapping[str, Any],
) -> dict[str, Any]:
    """Independently bind the core BASE/P75/P95 frozen-path receipt."""

    expected_keys = {
        "schema_version",
        "symbol",
        "timeframe",
        "strategy_name",
        "position_namespace",
        "warmup_bars",
        "partition_axis_sha256",
        "signal_vector_sha256",
        "base_trade_identity_pnl_sha256",
        "base_cost_attestation_sha256",
        "base_entry_half_spread_values_sha256",
        "base_intrabar_half_spread_values_sha256",
        "cost_ladder_contract_sha256",
        "shared_position_roster_sha256",
        "position_count",
        "daily_observation_count",
        "entry_cost_application",
        "uniform_application",
        "uplift_distribution_basis",
        "stochastic_scenario",
        "intrabar_spread_unchanged",
        "swap_financing_policy_unchanged",
        "path_identity_preserved",
        "levels",
        "receipt_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        _fail("COST_LADDER_METHOD_RECEIPT_SCHEMA_INVALID")
    receipt = dict(value)
    receipt_body = dict(receipt)
    receipt_sha = receipt_body.pop("receipt_sha256", None)
    _hex(receipt_sha, 64, "COST_LADDER_METHOD_RECEIPT_HASH_INVALID")
    if (
        receipt.get("schema_version") != FROZEN_PATH_COST_LADDER_SCHEMA
        or receipt_sha != _sha_json(receipt_body)
    ):
        _fail("COST_LADDER_METHOD_RECEIPT_HASH_INVALID")

    base = lanes.get("final_lockbox_base")
    p75 = lanes.get("final_lockbox_p75")
    p95 = lanes.get("final_lockbox_p95")
    process = attempt_artifacts.get("winner_process")
    if any(not isinstance(item, Mapping) for item in (base, p75, p95, process)):
        _fail("COST_LADDER_METHOD_LANES_INVALID")
    assert isinstance(base, Mapping)
    assert isinstance(p75, Mapping)
    assert isinstance(p95, Mapping)
    base_axis = base.get("trade_power_partition_axis")
    base_cost = base.get("numerical_execution_cost_attestation")
    base_events = base.get("position_events")
    base_surface = base.get("winner_daily_r_surface")
    if (
        any(not isinstance(item, Mapping) for item in (base_axis, base_cost, base_surface))
        or type(base_events) is not list
    ):
        _fail("COST_LADDER_METHOD_LANES_INVALID")
    assert isinstance(base_axis, Mapping)
    assert isinstance(base_cost, Mapping)
    assert isinstance(base_surface, Mapping)
    strategy_name = process.get("strategy_id")
    if type(strategy_name) is not str:
        _fail("COST_LADDER_METHOD_LANES_INVALID")
    position_namespace = "|".join(
        (frozen.symbol.upper(), frozen.timeframe.upper(), strategy_name)
    )
    base_trades = _trade_identity_pnl_evidence(
        base,
        lane="final_lockbox_base",
    )
    rebuilt_base_events = _canonical_trade_events(
        base_trades,
        namespace=position_namespace,
        lane="final_lockbox_base",
    )
    if _trade_position_reconciliation_evidence(
        rebuilt_base_events,
        lane="final_lockbox_base",
    ) != _trade_position_reconciliation_evidence(
        base_events,
        lane="final_lockbox_base",
    ):
        _fail("COST_LADDER_METHOD_BASE_TRADE_POSITION_RECONCILIATION_INVALID")
    ladder_contract = _validated_cost_ladder_contract(
        frozen.cost_ladder_contract,
        code="FROZEN_COST_LADDER_CONTRACT_INVALID",
    )
    try:
        r_quantum = winner.risk_unit_quantum(frozen.risk_unit_contract)
    except ValueError as exc:
        raise WinnerRawEvidenceError("RISK_UNIT_CONTRACT_INVALID") from exc
    if receipt.get("cost_ladder_contract_sha256") != _sha_json(ladder_contract):
        _fail("COST_LADDER_METHOD_CONTRACT_BINDING_INVALID")
    for name in (
        "signal_vector_sha256",
        "base_trade_identity_pnl_sha256",
        "shared_position_roster_sha256",
    ):
        _hex(receipt.get(name), 64, f"COST_LADDER_METHOD_{name.upper()}_INVALID")
    roster_fields = (
        "trade_record_schema_version",
        "position_id",
        "entry_time",
        "exit_time",
        "entry_event_order",
        "exit_event_order",
        "risk_amount",
    )
    shared_roster = [
        {field: event.get(field) for field in roster_fields}
        for event in base_events
        if isinstance(event, Mapping)
    ]
    if (
        len(shared_roster) != len(base_events)
        or receipt.get("symbol") != frozen.symbol
        or receipt.get("timeframe") != frozen.timeframe
        or receipt.get("strategy_name") != strategy_name
        or receipt.get("position_namespace") != position_namespace
        or receipt.get("warmup_bars") != base_axis.get("warmup_bars")
        or receipt.get("partition_axis_sha256")
        != base_axis.get("horizon_receipt_sha256")
        or receipt.get("base_cost_attestation_sha256")
        != base_cost.get("attestation_sha256")
        or receipt.get("base_entry_half_spread_values_sha256")
        != base_cost.get("entry_half_spread_values_sha256")
        or receipt.get("base_intrabar_half_spread_values_sha256")
        != base_cost.get("intrabar_half_spread_values_sha256")
        or receipt.get("base_trade_identity_pnl_sha256")
        != _backtester_exact_sha256(base_trades)
        or receipt.get("shared_position_roster_sha256")
        != _backtester_receipt_sha256({"position_events": shared_roster})
        or receipt.get("position_count") != len(base_events)
        or receipt.get("daily_observation_count")
        != len(base_surface.get("daily_net_mtm_r") or [])
        or receipt.get("entry_cost_application") != "ENTRY_SIDE_ONLY"
        or receipt.get("uniform_application") != "EVERY_FILL"
        or receipt.get("uplift_distribution_basis")
        != "UNCONDITIONAL_MEASURED_SINGLE_FILL_UPLIFT_DISTRIBUTION"
        or receipt.get("stochastic_scenario") is not False
        or receipt.get("intrabar_spread_unchanged") is not True
        or receipt.get("swap_financing_policy_unchanged") is not True
        or receipt.get("path_identity_preserved") is not True
    ):
        _fail("COST_LADDER_METHOD_RECEIPT_BINDING_INVALID")

    receipt_levels = receipt.get("levels")
    contract_levels = ladder_contract["levels"][1:]
    if type(receipt_levels) is not list or len(receipt_levels) != 2:
        _fail("COST_LADDER_METHOD_LEVEL_ROSTER_INVALID")
    lane_results = (base, p75, p95)
    prior_daily = list(base_surface.get("daily_net_mtm_r") or [])
    base_daily = prior_daily
    prior_events = base_events
    prior_trades = base_trades
    for index, (raw_level, contract_level, current) in enumerate(
        zip(receipt_levels, contract_levels, lane_results[1:], strict=True),
        start=1,
    ):
        if not isinstance(raw_level, Mapping):
            _fail("COST_LADDER_METHOD_LEVEL_SCHEMA_INVALID")
        level = dict(raw_level)
        level_body = dict(level)
        level_sha = level_body.pop("level_receipt_sha256", None)
        expected_level_keys = {
            "level_id",
            "single_fill_uplift_percentile",
            "uniform_entry_half_spread_multiplier_float64_hex",
            "trade_identity_pnl_sha256",
            "cost_attestation_sha256",
            "cost_policy_sha256",
            "entry_half_spread_values_sha256",
            "intrabar_half_spread_values_sha256",
            "base_relative",
            "adjacent_guard",
            "level_receipt_sha256",
        }
        current_cost = current.get("numerical_execution_cost_attestation")
        current_events = current.get("position_events")
        current_surface = current.get("winner_daily_r_surface")
        if (
            set(level) != expected_level_keys
            or level_sha != _sha_json(level_body)
            or not isinstance(current_cost, Mapping)
            or type(current_events) is not list
            or not isinstance(current_surface, Mapping)
        ):
            _fail("COST_LADDER_METHOD_LEVEL_SCHEMA_INVALID")
        for name in (
            "trade_identity_pnl_sha256",
            "cost_attestation_sha256",
            "cost_policy_sha256",
            "entry_half_spread_values_sha256",
            "intrabar_half_spread_values_sha256",
            "level_receipt_sha256",
        ):
            _hex(level.get(name), 64, f"COST_LADDER_METHOD_LEVEL_{name.upper()}_INVALID")
        policy = contract_level["numerical_execution_cost_policy"]
        current_daily = list(current_surface.get("daily_net_mtm_r") or [])
        current_trades = _trade_identity_pnl_evidence(
            current,
            lane=f"final_lockbox_{'p75' if index == 1 else 'p95'}",
        )
        level_lane = f"final_lockbox_{'p75' if index == 1 else 'p95'}"
        rebuilt_current_events = _canonical_trade_events(
            current_trades,
            namespace=position_namespace,
            lane=level_lane,
        )
        if _trade_position_reconciliation_evidence(
            rebuilt_current_events,
            lane=level_lane,
        ) != _trade_position_reconciliation_evidence(
            current_events,
            lane=level_lane,
        ):
            _fail("COST_LADDER_METHOD_LEVEL_TRADE_POSITION_RECONCILIATION_INVALID")
        if (
            level.get("level_id") != contract_level.get("level_id")
            or level.get("single_fill_uplift_percentile")
            != contract_level.get("single_fill_uplift_percentile")
            or level.get("uniform_entry_half_spread_multiplier_float64_hex")
            != contract_level.get("uniform_entry_half_spread_multiplier_float64_hex")
            or level.get("cost_attestation_sha256")
            != current_cost.get("attestation_sha256")
            or level.get("cost_policy_sha256") != policy.get("policy_sha256")
            or level.get("entry_half_spread_values_sha256")
            != current_cost.get("entry_half_spread_values_sha256")
            or level.get("intrabar_half_spread_values_sha256")
            != current_cost.get("intrabar_half_spread_values_sha256")
            or level.get("trade_identity_pnl_sha256")
            != _backtester_exact_sha256(current_trades)
            or current_cost.get("intrabar_half_spread_values_sha256")
            != base_cost.get("intrabar_half_spread_values_sha256")
            or len(current_events) != len(base_events)
            or len(current_trades) != len(base_trades)
            or len(current_trades) != len(prior_trades)
            or len(current_daily) != len(base_daily)
        ):
            _fail("COST_LADDER_METHOD_LEVEL_BINDING_INVALID")

        base_trade_worse = 0
        adjacent_trade_worse = 0
        for ordinal, (base_trade, prior_trade, current_trade) in enumerate(
            zip(base_trades, prior_trades, current_trades, strict=True)
        ):
            if any(
                current_trade[field] != base_trade[field]
                for field in _TRADE_PATH_IDENTITY_FIELDS
            ) or any(
                prior_trade[field] != base_trade[field]
                for field in _TRADE_PATH_IDENTITY_FIELDS
            ):
                _fail(f"COST_LADDER_METHOD_TRADE_{ordinal:05d}_IDENTITY_DRIFT")
            base_trade_pnl = Decimal(str(base_trade["pnl_dollars"]))
            prior_trade_pnl = Decimal(str(prior_trade["pnl_dollars"]))
            current_trade_pnl = Decimal(str(current_trade["pnl_dollars"]))
            if current_trade_pnl > prior_trade_pnl + Decimal("1e-12"):
                _fail(f"COST_LADDER_METHOD_TRADE_{ordinal:05d}_IMPROVES")
            base_trade_worse += int(current_trade_pnl < base_trade_pnl)
            adjacent_trade_worse += int(current_trade_pnl < prior_trade_pnl)

        identity_fields = roster_fields
        base_final_worse = 0
        adjacent_final_worse = 0
        for ordinal, (base_event, prior_event, current_event) in enumerate(
            zip(base_events, prior_events, current_events, strict=True)
        ):
            if any(
                not isinstance(event, Mapping)
                for event in (base_event, prior_event, current_event)
            ) or any(
                current_event.get(field) != base_event.get(field)
                for field in identity_fields
            ):
                _fail(f"COST_LADDER_METHOD_POSITION_{ordinal:05d}_IDENTITY_DRIFT")
            try:
                base_pnl = Decimal(str(base_event.get("pnl_dollars")))
                prior_pnl = Decimal(str(prior_event.get("pnl_dollars")))
                current_pnl = Decimal(str(current_event.get("pnl_dollars")))
            except InvalidOperation as exc:
                raise WinnerRawEvidenceError(
                    f"COST_LADDER_METHOD_POSITION_{ordinal:05d}_VALUE_INVALID"
                ) from exc
            if current_pnl > prior_pnl + Decimal("1e-12"):
                _fail(f"COST_LADDER_METHOD_POSITION_{ordinal:05d}_IMPROVES")
            base_final_worse += int(current_pnl < base_pnl)
            adjacent_final_worse += int(current_pnl < prior_pnl)

        base_running = Decimal(0)
        current_running = Decimal(0)
        prior_running = Decimal(0)
        base_prefix_worse = 0
        adjacent_prefix_worse = 0
        base_daily_improvements = 0
        adjacent_daily_improvements = 0
        for base_value, prior_value, current_value in zip(
            base_daily, prior_daily, current_daily, strict=True
        ):
            base_decimal = Decimal(str(base_value))
            prior_decimal = Decimal(str(prior_value))
            current_decimal = Decimal(str(current_value))
            base_running += base_decimal
            prior_running += prior_decimal
            current_running += current_decimal
            if current_running > prior_running + r_quantum:
                _fail("COST_LADDER_METHOD_CUMULATIVE_PREFIX_IMPROVES")
            base_prefix_worse += int(current_running < base_running)
            adjacent_prefix_worse += int(current_running < prior_running)
            base_daily_improvements += int(current_decimal > base_decimal)
            adjacent_daily_improvements += int(current_decimal > prior_decimal)

        base_relative = level.get("base_relative")
        adjacent = level.get("adjacent_guard")
        if not isinstance(base_relative, Mapping) or set(base_relative) != {
            "strictly_worse_trade_count",
            "strictly_worse_position_final_count",
            "strictly_worse_aggregate_prefix_count",
            "daily_increment_improvement_observation_count",
            "realized_adversity_status",
        }:
            _fail("COST_LADDER_METHOD_BASE_RELATIVE_INVALID")
        for name in (
            "strictly_worse_trade_count",
            "strictly_worse_position_final_count",
            "strictly_worse_aggregate_prefix_count",
            "daily_increment_improvement_observation_count",
        ):
            if type(base_relative.get(name)) is not int or base_relative[name] < 0:
                _fail("COST_LADDER_METHOD_BASE_RELATIVE_INVALID")
        strictly_adverse = bool(
            base_trade_worse
            or base_final_worse
            or base_prefix_worse
        )
        expected_status = (
            BACKTEST_FROZEN_PATH_COST_LADDER_ADVERSITY_STRICT
            if strictly_adverse
            else BACKTEST_FROZEN_PATH_COST_LADDER_ADVERSITY_INSUFFICIENT
        )
        if (
            base_relative.get("strictly_worse_trade_count") != base_trade_worse
            or base_relative.get("strictly_worse_position_final_count")
            != base_final_worse
            or base_relative.get("strictly_worse_aggregate_prefix_count")
            != base_prefix_worse
            or base_relative.get("daily_increment_improvement_observation_count")
            != base_daily_improvements
            or base_relative.get("realized_adversity_status") != expected_status
            or (
                strictly_adverse
                and level.get("entry_half_spread_values_sha256")
                == receipt.get("base_entry_half_spread_values_sha256")
            )
        ):
            _fail("COST_LADDER_METHOD_ADVERSITY_STATUS_MISMATCH")
        adjacent_expected_keys = {
            "predecessor_level_id",
            "trade_comparison_count",
            "strictly_worse_trade_count",
            "position_final_comparison_count",
            "strictly_worse_position_final_count",
            "aggregate_prefix_comparison_count",
            "strictly_worse_aggregate_prefix_count",
            "daily_increment_improvement_observation_count",
            "non_improvement_verified",
        }
        if not isinstance(adjacent, Mapping) or set(adjacent) != adjacent_expected_keys:
            _fail("COST_LADDER_METHOD_ADJACENT_GUARD_INVALID")
        for name in adjacent_expected_keys - {"predecessor_level_id", "non_improvement_verified"}:
            if type(adjacent.get(name)) is not int or adjacent[name] < 0:
                _fail("COST_LADDER_METHOD_ADJACENT_GUARD_INVALID")
        expected_predecessor = "BASE" if index == 1 else contract_levels[index - 2]["level_id"]
        if (
            adjacent.get("predecessor_level_id") != expected_predecessor
            or adjacent.get("trade_comparison_count") != len(current_trades)
            or adjacent.get("strictly_worse_trade_count")
            != adjacent_trade_worse
            or adjacent.get("position_final_comparison_count") != len(current_events)
            or adjacent.get("strictly_worse_position_final_count")
            != adjacent_final_worse
            or adjacent.get("aggregate_prefix_comparison_count") != len(current_daily)
            or adjacent.get("strictly_worse_aggregate_prefix_count")
            != adjacent_prefix_worse
            or adjacent.get("daily_increment_improvement_observation_count")
            != adjacent_daily_improvements
            or adjacent.get("non_improvement_verified") is not True
        ):
            _fail("COST_LADDER_METHOD_ADJACENT_GUARD_INVALID")
        prior_trades = current_trades
        prior_events = current_events
        prior_daily = current_daily
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
    archived = set(lanes) == set(_ARCHIVED_LANE_KEYS)
    if not archived and set(lanes) != set(_LANE_KEYS):
        _fail("PARTITION_GEOMETRY_LANE_ROSTER_INVALID")
    lane_order = tuple((role, role + ("_base" if archived else "_p75")) for role in _PARTITIONS)
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
        if not archived and lanes[role + "_p95"].get("trade_power_partition_axis") != axis:
            _fail(f"PARTITION_GEOMETRY_{ordinal}_TIER_AXIS_SPLICE")
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
        ("candidate_registry_sha256", "candidate_registry"),
        ("trial_registry_sha256", "trial_registry"),
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
        or horizon_policy.get("frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256")
        != frozen.precommitment.get("frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256")
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
    sink: Optional[SymbolAuthoritySink],
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
    current_pair = bool(frozen.frozen_tier_pair_contract_bytes)
    transport_v16 = frozen.selection_generation is winner.V16_GENERATION
    historical_schema = (HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4 if transport_v16
        else HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3 if current_pair else HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA)
    current_schema = (CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5 if transport_v16
        else CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4 if current_pair else CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA)
    if schema == historical_schema:
        mode = HISTORICAL_CROSS_BINDING_MODE
        expected_top = common_top | historical_fields
        if (
            production_cell_search_plan is not None
            or production_search_process is not None
        ):
            _fail("HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN")
    elif schema == current_schema:
        mode = binding.get("mode")
        if mode == CURRENT_CROSS_BINDING_MODE:
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
        "frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256",
    }
    if frozen.frozen_tier_pair_contract_bytes:
        expected_winner.add("partition_cost_precommitments_sha256")
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
        ("frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256"):
            precommitment["frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256"],
    }
    if frozen.frozen_tier_pair_contract_bytes:
        expected_winner_values["partition_cost_precommitments_sha256"] = precommitment["partition_cost_precommitments_sha256"]
    if dict(winner_roster) != expected_winner_values:
        _fail("PROVENANCE_CROSS_BINDING_WINNER_SPLICE")
    if mode == CURRENT_CROSS_BINDING_MODE:
        if sink is None:
            _fail("CURRENT_CROSS_BINDING_AUTHORITY_SINK_REQUIRED")
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


def _independent_tier_execution_receipt(lanes: Mapping[str, Any], pair_sha256: str) -> dict[str, Any]:
    if set(lanes) != set(_LANE_KEYS):
        _fail("INDEPENDENT_TIER_LANE_ROSTER_INVALID")
    hashes = {}
    for name in _LANE_KEYS:
        result = lanes[name]
        admission = result.get("spread_admission")
        if not isinstance(admission, Mapping):
            _fail("INDEPENDENT_TIER_ADMISSION_MISSING")
        hashes[name] = {
            "trade_identity_pnl_sha256": _sha_json(_trade_identity_pnl_evidence(result, lane=name)),
            "position_events_sha256": _sha_json(result.get("position_events")),
            "partition_axis_sha256": _sha_json(result.get("trade_power_partition_axis")),
            "numerical_execution_cost_attestation_sha256": _sha_json(result.get("numerical_execution_cost_attestation")),
            "entry_observations_sha256": admission.get("entry_observations_sha256"),
            "entry_admission_mask_sha256": admission.get("entry_admission_mask_sha256")}
        for value in hashes[name].values():
            _hex(value, 64, "INDEPENDENT_TIER_EXECUTION_HASH_INVALID")
    body = {"schema_version": INDEPENDENT_TIER_EXECUTION_SCHEMA,
        "frozen_tier_pair_contract_sha256": pair_sha256, "lanes": hashes}
    return {**body, "receipt_sha256": _sha_json(body)}


def _source_observation_identity(context: Any, frozen: FrozenWinnerSearchPlan) -> str:
    if (type(context) is not authority_spool.GenerationSpoolContext
            or type(frozen) is not FrozenWinnerSearchPlan
            or context.context_sha256 != frozen.generation_context_sha256):
        _fail("SOURCE_OBSERVATION_REFERENCE_CONTEXT_INVALID")
    return _artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)


def _source_observation_body(value: Any, *, frozen: FrozenWinnerSearchPlan,
        expected_receipt_sha256: Optional[str] = None) -> dict[str, Any]:
    keys = {"schema_version", "symbol", "timeframe", "source_timeframe", "source_product", "observations",
        "entry_observations_sha256", "receipt_sha256"}
    if not isinstance(value, Mapping) or set(value) != keys:
        _fail("SOURCE_OBSERVATION_RECEIPT_SCHEMA_INVALID")
    body = dict(value)
    receipt_sha = _hex(body.pop("receipt_sha256"), 64, "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    if (body["schema_version"] != SOURCE_OBSERVATIONS_SCHEMA
            or body["symbol"] != frozen.symbol or body["timeframe"] != frozen.timeframe
            or receipt_sha != _sha_json(body)
            or (expected_receipt_sha256 is not None and receipt_sha != expected_receipt_sha256)):
        _fail("SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    _identity(body["source_timeframe"], "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID", 16)
    _hex(body["entry_observations_sha256"], 64, "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    carrier = body["source_product"]
    carrier_keys = {"data_dir", "index_descriptor", "index_sha256", "symbol", "timeframe", "cutoff_utc",
        "activation_manifest_sha256", "production_contract_required"}
    if (not isinstance(carrier, Mapping) or set(carrier) != carrier_keys
            or not isinstance(carrier["index_descriptor"], Mapping)
            or type(carrier["data_dir"]) is not str or not carrier["data_dir"]
            or type(carrier["production_contract_required"]) is not bool
            or carrier["symbol"] != frozen.symbol or carrier["timeframe"] != body["source_timeframe"]):
        _fail("SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID")
    for name in ("index_sha256", "activation_manifest_sha256"):
        _hex(carrier[name], 64, "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID")
    if _utc_second(carrier["cutoff_utc"], "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID") != carrier["cutoff_utc"]:
        _fail("SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID")
    rows = body["observations"]
    if type(rows) is not list or not 1 <= len(rows) <= MAX_SIGNAL_VECTOR_OBSERVATIONS:
        _fail("SOURCE_OBSERVATION_ROSTER_INVALID")
    row_keys = {"target_open_utc", "expected_prior_fine_close_utc", "prior_fine_close_utc", "points"}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != row_keys:
            _fail("SOURCE_OBSERVATION_ROW_INVALID")
        for name in ("target_open_utc", "expected_prior_fine_close_utc", "prior_fine_close_utc"):
            if name == "prior_fine_close_utc" and row[name] is None:
                continue
            if _utc_second(row[name], "SOURCE_OBSERVATION_AXIS_INVALID") != row[name]:
                _fail("SOURCE_OBSERVATION_AXIS_INVALID")
        point = row["points"]
        if point is not None and (type(point) not in {int, float}
                or not math.isfinite(point) or point < 0 or point != math.floor(point)):
            _fail("SOURCE_OBSERVATION_VALUE_INVALID")
    return dict(value)


def _source_observation_reference_entry(context: Any, *, frozen: FrozenWinnerSearchPlan,
        reference: Any, expected_receipt_sha256: str) -> dict[str, Any]:
    prefix = _source_observation_identity(context, frozen)
    if (not isinstance(reference, Mapping)
            or set(reference) != {"schema_version", "source_observation_receipt_sha256", "artifact_entry"}
            or reference.get("schema_version") != SOURCE_OBSERVATION_REFERENCE_SCHEMA):
        _fail("SOURCE_OBSERVATION_REFERENCE_SCHEMA_INVALID")
    _hex(expected_receipt_sha256, 64, "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    if reference["source_observation_receipt_sha256"] != expected_receipt_sha256:
        _fail("SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    entry = reference["artifact_entry"]
    if (not isinstance(entry, Mapping) or set(entry) != {"artifact_id", "artifact_type", "descriptor"}
            or entry.get("artifact_type") != SOURCE_OBSERVATION_ARTIFACT_TYPE):
        _fail("SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID")
    descriptor = entry["descriptor"]
    if (not isinstance(descriptor, Mapping)
            or set(descriptor) != {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
            or descriptor.get("schema_version") != ARTIFACT_DESCRIPTOR_SCHEMA
            or descriptor.get("format") != ASCII_JSON_FORMAT
            or type(descriptor.get("size_bytes")) is not int
            or not 0 < descriptor["size_bytes"] <= context.limits.max_artifact_bytes):
        _fail("SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID")
    digest = _hex(descriptor.get("sha256"), 64, "SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID")
    if (descriptor["relative_path"] != f"objects/sha256/{digest[:2]}/{digest}.{ASCII_JSON_FORMAT}"
            or entry["artifact_id"] != f"{prefix}/source-observations/{digest}"):
        _fail("SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID")
    return {"artifact_id": entry["artifact_id"], "artifact_type": entry["artifact_type"],
        "descriptor": dict(descriptor)}


def _reopen_source_observation_reference(context: Any, *, frozen: FrozenWinnerSearchPlan,
        reference: Any, expected_receipt_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
    entry = _source_observation_reference_entry(context, frozen=frozen, reference=reference,
        expected_receipt_sha256=expected_receipt_sha256)
    descriptor = entry["descriptor"]
    try:
        digest, size, encoded = authority_spool._stream_file(context.root, descriptor["relative_path"],
            maximum_bytes=context.limits.max_artifact_bytes, expected_size=descriptor["size_bytes"],
            collect=True, chunk_bytes=context.limits.stream_chunk_bytes)
        if digest != descriptor["sha256"] or size != descriptor["size_bytes"]:
            _fail("SOURCE_OBSERVATION_REFERENCE_REOPEN_FAILED")
        value = _decode_object(encoded, "SOURCE_OBSERVATION_REFERENCE_REOPEN_FAILED")
    except (authority_spool.AuthoritySpoolError, OSError, TypeError, ValueError) as exc:
        raise WinnerRawEvidenceError("SOURCE_OBSERVATION_REFERENCE_REOPEN_FAILED") from exc
    return _source_observation_body(value, frozen=frozen,
        expected_receipt_sha256=expected_receipt_sha256), entry


def publish_source_observation_receipt(sink: SymbolAuthoritySink, *, frozen: FrozenWinnerSearchPlan,
        receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Publish the unchanged logical map once, outside the frozen-plan subtree."""
    if (type(sink) is not SymbolAuthoritySink or type(frozen) is not FrozenWinnerSearchPlan
            or sink.symbol != frozen.symbol):
        _fail("SOURCE_OBSERVATION_REFERENCE_CONTEXT_INVALID")
    prefix = _source_observation_identity(sink.context, frozen)
    body = _source_observation_body(receipt, frozen=frozen)
    physical_sha = _sha_json(body)
    entry = _put_ascii_json(sink, f"{prefix}/source-observations/{physical_sha}",
        SOURCE_OBSERVATION_ARTIFACT_TYPE, body)
    if entry["descriptor"]["sha256"] != physical_sha:
        _fail("SOURCE_OBSERVATION_REFERENCE_ARTIFACT_INVALID")
    return {"schema_version": SOURCE_OBSERVATION_REFERENCE_SCHEMA,
        "source_observation_receipt_sha256": body["receipt_sha256"], "artifact_entry": entry}


def resolve_source_observation_reference(sink: SymbolAuthoritySink, *, frozen: FrozenWinnerSearchPlan,
        reference: Mapping[str, Any], expected_receipt_sha256: str) -> dict[str, Any]:
    """Reopen physical bytes and their separate logical receipt commitment."""
    if (type(sink) is not SymbolAuthoritySink or type(frozen) is not FrozenWinnerSearchPlan
            or sink.symbol != frozen.symbol):
        _fail("SOURCE_OBSERVATION_REFERENCE_CONTEXT_INVALID")
    authority_spool._assert_context_current(sink.context)
    body, _ = _reopen_source_observation_reference(sink.context, frozen=frozen,
        reference=reference, expected_receipt_sha256=expected_receipt_sha256)
    return body


def _source_observation_closure_body(context: Any, entry: Mapping[str, Any]) -> dict[str, Any]:
    try:
        if set(entry) != {"artifact_id", "artifact_type", "descriptor"}:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        descriptor = authority_spool._validate_descriptor(dict(entry["descriptor"]), content_addressed=True)
        if descriptor["format"] != ASCII_JSON_FORMAT:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        digest, size, encoded = authority_spool._stream_file(context.root, descriptor["relative_path"],
            maximum_bytes=context.limits.max_artifact_bytes, expected_size=descriptor["size_bytes"],
            collect=True, chunk_bytes=context.limits.stream_chunk_bytes)
        if digest != descriptor["sha256"] or size != descriptor["size_bytes"]:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        return _decode_object(encoded, "SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
    except (authority_spool.AuthoritySpoolError, OSError, TypeError, ValueError, KeyError) as exc:
        raise WinnerRawEvidenceError("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID") from exc


def verify_source_observation_reference_closure(context: Any, *, frozen: FrozenWinnerSearchPlan,
        manifest_entries: Iterable[Mapping[str, Any]], production_receipt: Mapping[str, Any]) -> None:
    """Verify completed referenced transport, one receipt or source map at a time."""
    prefix = _source_observation_identity(context, frozen)
    authority_spool._assert_context_current(context)
    if not isinstance(production_receipt, Mapping) or not isinstance(production_receipt.get("artifact_roster"), Mapping):
        _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
    production_roster = production_receipt["artifact_roster"]
    entries: dict[str, Mapping[str, Any]] = {}
    for entry in manifest_entries:
        if not isinstance(entry, Mapping) or type(entry.get("artifact_id")) is not str:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        if not entry["artifact_id"].startswith(prefix + "/"):
            continue
        if entry["artifact_id"] in entries:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        entries[entry["artifact_id"]] = entry
    source_entries = {name: entry for name, entry in entries.items()
        if name.startswith(prefix + "/source-observations/")
        or entry.get("artifact_type") == SOURCE_OBSERVATION_ARTIFACT_TYPE}
    provenance_entries = {name: entry for name, entry in entries.items()
        if any(name.endswith("/provenance/" + role)
            or entry.get("artifact_type") == "winner_" + role for role in _PROVENANCE_KEYS)}
    versions: set[str] = set()
    pair_common: dict[tuple[int, int], dict[str, Any]] = {}
    replay_entries: dict[tuple[int, int], Mapping[str, Any]] = {}
    specifics = {schema: _provenance_specific_keys(current=True,
        referenced=schema == REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA)
        for schema in (CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA, REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA)}
    for artifact_id, entry in provenance_entries.items():
        receipt = _source_observation_closure_body(context, entry)
        schema = receipt.get("schema_version")
        if type(schema) is not str or schema not in {CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA, REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA}:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        versions.add(schema)
        if len(versions) > 1:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        specific = specifics[schema]
        role = receipt.get("receipt_role")
        if type(role) is not str or role not in specific:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        _validated_self_hashed_receipt(receipt, role=role,
            expected_keys=_PROVENANCE_COMMON_KEYS | specific[role],
            schema_version=schema)
        ordinal, fold_ordinal = receipt.get("attempt_ordinal"), receipt.get("fold_ordinal")
        if (type(ordinal) is not int or type(fold_ordinal) is not int
                or entry.get("artifact_type") != "winner_" + role
                or artifact_id != f"{prefix}/attempt-{ordinal:04d}/fold-{fold_ordinal:03d}/provenance/{role}"
                or receipt.get("symbol") != frozen.symbol or receipt.get("timeframe") != frozen.timeframe
                or receipt.get("regime") != frozen.regime
                or receipt.get("confers_final_oos_or_trading_authority") is not False):
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        common = {key: receipt[key] for key in _PROVENANCE_COMMON_KEYS
            if key not in {"receipt_role", "receipt_sha256", "schema_version"}}
        pair = (ordinal, fold_ordinal)
        if pair in pair_common and pair_common[pair] != common:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        pair_common[pair] = common
        production_key = f"attempt-{ordinal:04d}-fold-{fold_ordinal:03d}-provenance-{role}"
        if production_roster.get(production_key) != entry["descriptor"]:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        if role == "attempt_replay_receipt":
            replay_entries[pair] = entry
    observed_keys = {key for key in production_roster if type(key) is str and key.startswith("source-observation-")}
    expected_pairs = {(attempt["ordinal"], fold["fold_ordinal"])
        for attempt in frozen.attempt_ledger["attempts"]
        if attempt["precommitted_evidence_role"] == "EVALUATION_REQUIRED"
        for fold in frozen.precommitment["causal_fold_plan"]}
    expected_provenance_ids = {f"{prefix}/attempt-{ordinal:04d}/fold-{fold_ordinal:03d}/provenance/{role}"
        for ordinal, fold_ordinal in expected_pairs for role in _PROVENANCE_KEYS}
    if set(provenance_entries) != expected_provenance_ids or set(replay_entries) != expected_pairs:
        _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
    if REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA not in versions:
        if source_entries or observed_keys:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        return
    precommits: dict[tuple[int, int, str], str] = {}
    partition_precommits = frozen.partition_cost_precommitments
    compact_precommits = partition_precommits.get("schema_version") == PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
    pair_contract = frozen.frozen_tier_pair_contract if compact_precommits else None
    for row in partition_precommits["rows"]:
        key = (row["attempt_ordinal"], row["fold_ordinal"], row["partition"])
        if key in precommits:
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        expanded = (expand_partition_cost_precommitment(partition_precommits, row,
            pair=pair_contract) if compact_precommits else row)
        precommits[key] = expanded["source_observation_receipt_sha256"]
        del expanded
    used_sources: set[str] = set()
    for (ordinal, fold_ordinal), entry in replay_entries.items():
        replay = _source_observation_closure_body(context, entry)
        references = replay["source_observation_references"]
        if not isinstance(references, Mapping) or set(references) != set(_PARTITIONS):
            _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
        for partition in _PARTITIONS:
            expected_source_sha = precommits.get((ordinal, fold_ordinal, partition))
            if expected_source_sha is None:
                _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
            body, source_entry = _reopen_source_observation_reference(context, frozen=frozen,
                reference=references[partition], expected_receipt_sha256=expected_source_sha)
            source_id = source_entry["artifact_id"]
            digest = source_entry["descriptor"]["sha256"]
            if (source_entries.get(source_id) != source_entry
                    or production_roster.get(f"source-observation-{digest}") != source_entry["descriptor"]):
                _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
            used_sources.add(source_id)
            del body
    expected_keys = {f"source-observation-{entries[name]['descriptor']['sha256']}" for name in used_sources}
    if used_sources != set(source_entries) or observed_keys != expected_keys:
        _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")


def _validated_source_observation_receipt(value: Any, *, sink: SymbolAuthoritySink,
        frozen: FrozenWinnerSearchPlan, expected_sha256: str, partition: str,
        signals: Mapping[str, Any], lanes: Mapping[str, Any]) -> dict[str, Any]:
    import numpy as np
    import pm_native_prefix_production as native

    keys = {"schema_version", "symbol", "timeframe", "source_timeframe", "source_product", "observations",
        "entry_observations_sha256", "receipt_sha256"}
    if not isinstance(value, Mapping) or set(value) != keys:
        _fail("SOURCE_OBSERVATION_RECEIPT_SCHEMA_INVALID")
    body = dict(value)
    receipt_sha = body.pop("receipt_sha256")
    if (body["schema_version"] != SOURCE_OBSERVATIONS_SCHEMA or body["symbol"] != frozen.symbol
            or body["timeframe"] != frozen.timeframe or receipt_sha != _sha_json(body)
            or receipt_sha != expected_sha256):
        _fail("SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID")
    carrier = body["source_product"]
    carrier_keys = {"data_dir", "index_descriptor", "index_sha256", "symbol", "timeframe", "cutoff_utc",
        "activation_manifest_sha256", "production_contract_required"}
    if (not isinstance(carrier, Mapping) or set(carrier) != carrier_keys
            or type(carrier["production_contract_required"]) is not bool
            or carrier["symbol"] != frozen.symbol or carrier["timeframe"] != body["source_timeframe"]):
        _fail("SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID")
    try:
        product = native.verify_activated_native_prefix(data_dir=Path(carrier["data_dir"]), context=sink.context,
            index_descriptor=carrier["index_descriptor"], expected_index_sha256=carrier["index_sha256"],
            expected_symbol=frozen.symbol, expected_timeframe=carrier["timeframe"], expected_cutoff_utc=carrier["cutoff_utc"],
            expected_activation_manifest_sha256=carrier["activation_manifest_sha256"],
            production_contract_required=carrier["production_contract_required"], require_current_activation=False)
        fine = native.open_verified_prefix_frame(product)
    except (native.NativePrefixError, TypeError, ValueError, OSError, KeyError) as exc:
        raise WinnerRawEvidenceError("SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED") from exc
    rows = body["observations"]
    if type(rows) is not list or not 1 <= len(rows) <= MAX_SIGNAL_VECTOR_OBSERVATIONS:
        _fail("SOURCE_OBSERVATION_ROSTER_INVALID")
    row_keys = {"target_open_utc", "expected_prior_fine_close_utc", "prior_fine_close_utc", "points"}
    if any(not isinstance(row, Mapping) or set(row) != row_keys for row in rows):
        _fail("SOURCE_OBSERVATION_ROW_INVALID")
    if any(row["points"] is not None and (type(row["points"]) not in {int, float}
            or not math.isfinite(row["points"]) or row["points"] < 0 or row["points"] != math.floor(row["points"])) for row in rows):
        _fail("SOURCE_OBSERVATION_VALUE_INVALID")
    try:
        target = pd.DatetimeIndex([row["target_open_utc"] for row in rows])
        observations, matched = align_prior_completed_spreads(target, fine, body["source_timeframe"], include_matched_closes=True)
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise WinnerRawEvidenceError("SOURCE_OBSERVATION_AXIS_INVALID") from exc
    rebuilt_rows = [{"target_open_utc": _utc_second(stamp.isoformat(), "SOURCE_OBSERVATION_AXIS_INVALID"),
        "expected_prior_fine_close_utc": _utc_second(stamp.isoformat(), "SOURCE_OBSERVATION_AXIS_INVALID"),
        "prior_fine_close_utc": None if pd.isna(matched.iloc[index]) else _utc_second(
            matched.iloc[index].isoformat(), "SOURCE_OBSERVATION_AXIS_INVALID"),
        "points": None if pd.isna(point) else float(point)} for index, (stamp, point) in enumerate(observations.items())]
    observations_sha = hashlib.sha256(pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest()
    if rows != rebuilt_rows or body["entry_observations_sha256"] != observations_sha:
        _fail("SOURCE_OBSERVATION_VALUES_CHANGED")
    source, activation = product.index["source"], product.index["activation"]
    pair = frozen.frozen_tier_pair_contract
    for level in pair["tiers"]:
        name = level["tier"]
        descriptor = level["frozen_spread_tier"]
        binding = descriptor["source_binding"]
        if (descriptor["source_timeframe"] != body["source_timeframe"]
                or source["bars_sha256"] != binding["bars_sha256"]
                or source["audit_sidecar_sha256"] != binding["sidecar_sha256"]
                or source["provenance_sha256"] != binding["provenance_sha256"]
                or source["source_identity_sha256"] != descriptor["source_identity_sha256"]
                or activation["manifest_sha256"] != descriptor["activation"]["manifest_sha256"]
                or activation["activation_id"] != descriptor["activation"]["id"]):
            _fail("SOURCE_OBSERVATION_PROFILE_NATIVE_SPLICE")
        tier = FrozenSpreadTier(json.dumps(descriptor, sort_keys=True, separators=(",", ":"), allow_nan=False))
        result = lanes[partition + "_" + name.lower()]
        axis = _partition_axis(result, partition)
        if len(target) != axis["warmup_bars"] + axis["scored_bars"] or len(signals["values"]) != len(target):
            _fail("SOURCE_OBSERVATION_PARTITION_AXIS_SPLICE")
        if target[axis["warmup_bars"]] != pd.Timestamp(axis["start_utc"]) or target[-1] >= pd.Timestamp(axis["end_utc"]):
            _fail("SOURCE_OBSERVATION_PARTITION_AXIS_SPLICE")
        states = np.array([spread_admission(tier, point) for point in observations])
        if (states == "INVALID").any():
            _fail("SOURCE_OBSERVATION_VALUE_INVALID")
        mask = states == "ADMIT"
        opportunities = np.zeros(len(target), dtype=bool)
        start = max(1, axis["warmup_bars"])
        opportunities[start:] = np.array(signals["values"], dtype=np.int32)[start - 1:-1] != 0
        admission = result.get("spread_admission")
        model = result.get("spread_model")
        if not isinstance(admission, Mapping) or not isinstance(model, Mapping):
            _fail("TIER_ADMISSION_EVIDENCE_MISSING")
        counts = {"signal_opportunities": int(opportunities.sum()),
            "known_cap_pass_opportunities": int((opportunities & (states == "ADMIT")).sum()),
            "known_cap_refused_opportunities": int((opportunities & (states == "REFUSE")).sum()),
            "missing_refused_opportunities": int((opportunities & (states == "MISSING")).sum()), "missing_executed_legs": 0}
        if (admission.get("schema_version") != ADMISSION_SCHEMA or admission.get("entry_admission_policy") != ADMISSION_POLICY
                or admission.get("entry_observations_sha256") != observations_sha
                or admission.get("entry_admission_mask_sha256") != hashlib.sha256(mask.astype(np.uint8).tobytes()).hexdigest()
                or admission.get("input_provenance_authenticated") is not False or admission.get("qualification_authority") is not False
                or model.get("entry_admission_schema") != ADMISSION_SCHEMA or model.get("entry_admission_policy") != ADMISSION_POLICY
                or model.get("entry_observations_sha256") != observations_sha
                or any(type(admission.get(key)) is not int or admission[key] != count for key, count in counts.items())):
            _fail("TIER_ADMISSION_RECONSTRUCTION_FAILED")
        for trade in _trade_identity_pnl_evidence(result, lane=partition + "_" + name.lower()):
            entry_bar = trade["entry_bar"]
            if (not max(1, axis["warmup_bars"]) <= entry_bar <= trade["exit_bar"] < len(target)
                    or trade["signal_bar"] != entry_bar - 1 or not mask[entry_bar]
                    or signals["values"][trade["signal_bar"]] != (1 if trade["direction"] == "LONG" else -1)
                    or pd.Timestamp(trade["entry_time"]) != target[entry_bar]
                    or pd.Timestamp(trade["exit_time"]) != target[trade["exit_bar"]]):
                _fail("TIER_TRADE_ENTRY_ADMISSION_FAILED")
    return dict(value)


def _provenance_specific_keys(*, current: bool, referenced: bool = False) -> dict[str, frozenset[str]]:
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
                "cost_ladder_method_receipt_sha256",
                "cost_ladder_method_receipt",
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
    if current:
        specific["attempt_replay_receipt"] = frozenset({"lane_sha256", "independent_tier_execution_receipt_sha256",
            "independent_tier_execution_receipt", "signal_vector_receipts",
            "source_observation_references" if referenced else "source_observation_receipts",
            "position_events_are_genuine_backtester_v2"})
    return specific


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
    sink: Optional[SymbolAuthoritySink] = None,
    source_observation_entries: Optional[dict[str, dict[str, Any]]] = None,
    provenance_schemas: Optional[set[str]] = None,
    partition_cost_index: Optional[_PartitionCostIndex] = None,
) -> dict[str, dict[str, Any]]:
    current = bool(frozen.frozen_tier_pair_contract_bytes)
    if not isinstance(raw, Mapping) or set(raw) != set(_PROVENANCE_KEYS):
        _fail("FOLD_PROVENANCE_SCHEMA_INVALID")
    provenance_schema = ARCHIVED_AUTHORITATIVE_PROVENANCE_SCHEMA
    referenced = False
    if current:
        first_raw = raw.get(_PROVENANCE_KEYS[0])
        provenance_schema = first_raw.get("schema_version") if isinstance(first_raw, Mapping) else None
        if type(provenance_schema) is not str or provenance_schema not in {CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA, REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA}:
            _fail("FOLD_PROVENANCE_SCHEMA_INVALID")
        referenced = provenance_schema == REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA
    specific = _provenance_specific_keys(current=current, referenced=referenced)
    receipts = {
        name: _validated_self_hashed_receipt(
            raw.get(name),
            role=name,
            expected_keys=_PROVENANCE_COMMON_KEYS | specific[name],
            schema_version=provenance_schema,
        )
        for name in _PROVENANCE_KEYS
    }
    if provenance_schemas is not None:
        provenance_schemas.add(provenance_schema)
        if len(provenance_schemas) > 1:
            _fail("ATTEMPT_REPLAY_PROVENANCE_VERSION_MIXED")
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
    if current:
        if type(sink) is not SymbolAuthoritySink:
            _fail("CURRENT_PROVENANCE_SINK_REQUIRED")
        expected_receipt = _independent_tier_execution_receipt(lanes, frozen.precommitment["frozen_tier_pair_contract_sha256"])
        signals = replay.get("signal_vector_receipts")
        observation_receipts = replay.get("source_observation_references" if referenced else "source_observation_receipts")
        if (replay.get("lane_sha256") != {name: _sha_json(dict(lanes[name])) for name in _LANE_KEYS}
                or replay.get("independent_tier_execution_receipt") != expected_receipt
                or replay.get("independent_tier_execution_receipt_sha256") != expected_receipt["receipt_sha256"]
                or replay.get("position_events_are_genuine_backtester_v2") is not True
                or not isinstance(signals, Mapping) or set(signals) != set(_PARTITIONS)
                or not isinstance(observation_receipts, Mapping) or set(observation_receipts) != set(_PARTITIONS)):
            _fail("ATTEMPT_REPLAY_PROVENANCE_SPLICE")
        selected = _selected_partition_cost_precommitments(frozen=frozen, attempt=attempt,
            planned_fold=planned_fold, partition_cost_index=partition_cost_index)
        if [row["partition"] for row in selected] != list(_PARTITIONS):
            _fail("PARTITION_COST_PRECOMMITMENTS_ROSTER_INVALID")
        raw_spec = receipts["oos_source_receipt"].get("execution_spec_receipt", {}).get("instrument_spec")
        if not isinstance(raw_spec, Mapping):
            _fail("EXECUTION_SPEC_BODY_SCHEMA_INVALID")
        spec = InstrumentSpec(**dict(raw_spec))
        for partition, geometry, precommit_row in zip(_PARTITIONS, geometry_bodies, selected, strict=True):
            signal = _validated_signal_vector_receipt(signals[partition])
            if (precommit_row["partition_geometry_receipt_sha256"] != geometry["receipt_sha256"]
                    or precommit_row["partition_frame_sha256"] != geometry["frame_sha256"]
                    or signal["observation_count"] != geometry["frame_rows"]):
                _fail("PARTITION_COST_PRECOMMITMENTS_GEOMETRY_SPLICE")
            tier_results = {tier: lanes[partition + "_" + tier.lower()] for tier in _TIERS}
            try:
                actual_pair = build_backtest_frozen_tier_pair_contract(tier_results, spec=spec, use_spread=True)
            except (TypeError, ValueError, KeyError, OverflowError) as exc:
                raise WinnerRawEvidenceError("ACTUAL_TIER_PAIR_CONTRACT_INVALID") from exc
            if actual_pair != frozen.frozen_tier_pair_contract:
                _fail("ACTUAL_TIER_PAIR_FROZEN_POLICY_DRIFT")
            observation_receipt = observation_receipts[partition]
            if referenced:
                observation_receipt = resolve_source_observation_reference(sink, frozen=frozen,
                    reference=observation_receipt, expected_receipt_sha256=precommit_row["source_observation_receipt_sha256"])
                source_entry = _source_observation_reference_entry(sink.context, frozen=frozen,
                    reference=observation_receipts[partition], expected_receipt_sha256=precommit_row["source_observation_receipt_sha256"])
                if source_observation_entries is not None:
                    source_observation_entries[source_entry["artifact_id"]] = source_entry
            _validated_source_observation_receipt(observation_receipt, sink=sink, frozen=frozen,
                expected_sha256=precommit_row["source_observation_receipt_sha256"], partition=partition,
                signals=signal, lanes=lanes)
            for tier, result in tier_results.items():
                expected = precommit_row["tiers"][tier]
                admission = result["spread_admission"]
                if (result.get("numerical_execution_cost_attestation") != expected["numerical_execution_cost_attestation"]
                        or admission.get("entry_observations_sha256") != expected["entry_observations_sha256"]
                        or admission.get("entry_admission_mask_sha256") != expected["entry_admission_mask_sha256"]):
                    _fail("PARTITION_COST_PRECOMMITMENTS_ACTUAL_ACCOUNT_SPLICE")
                _validate_tier_result(result, frozen=frozen, partition=partition, tier=tier)
                namespace = "|".join((frozen.symbol.upper(), frozen.timeframe.upper(), attempt_artifacts["winner_process"]["strategy_id"]))
                rebuilt = _canonical_trade_events(_trade_identity_pnl_evidence(result, lane=partition + "_" + tier.lower()),
                    namespace=namespace, lane=partition + "_" + tier.lower())
                if _trade_position_reconciliation_evidence(rebuilt, lane=tier) != _trade_position_reconciliation_evidence(result["position_events"], lane=tier):
                    _fail("TIER_TRADE_POSITION_RECONCILIATION_INVALID")
    else:
        replay = receipts["attempt_replay_receipt"]
        expected_lanes = {name: _sha_json(dict(lanes[name])) for name in _ARCHIVED_LANE_KEYS}
        ladder_receipt = _validated_cost_ladder_method_receipt(
            replay.get("cost_ladder_method_receipt"),
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
            or replay.get("cost_ladder_method_receipt_sha256")
            != ladder_receipt["receipt_sha256"]
            or signal_receipt.get("signal_vector_sha256")
            != ladder_receipt.get("signal_vector_sha256")
            or not isinstance(final_axis, Mapping)
            or signal_receipt.get("observation_count")
            != final_axis.get("warmup_bars", 0) + final_axis.get("scored_bars", 0)
            or replay.get("position_events_are_genuine_backtester_v2") is not True
        ):
            _fail("ATTEMPT_REPLAY_PROVENANCE_SPLICE")
    source = receipts["oos_source_receipt"]
    horizon_process = attempt_artifacts.get("horizon_process")
    source_binding = (frozen.source_contract_seeds["P75"] if current else frozen.source_contract_seed).get("source_binding")
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
    source_observation_entries: Optional[dict[str, dict[str, Any]]] = None,
    provenance_schemas: Optional[set[str]] = None,
    partition_cost_index: Optional[_PartitionCostIndex] = None,
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
        sink=sink,
        source_observation_entries=source_observation_entries,
        provenance_schemas=provenance_schemas,
        partition_cost_index=partition_cost_index,
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
        "schema_version": frozen.selection_generation.shared_partition_version,
        "cell_id": frozen.precommitment["cell_id"],
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "causal_fold_plan_row_sha256": _sha_json(dict(planned_fold)),
        "native_prefix_index_sha256": source_receipt["native_prefix_index_sha256"],
        "native_prefix_content_sha256": source_receipt["native_prefix_content_sha256"],
        "source_contract_sha256": source_receipt["source_contract_sha256"],
        "native_source_sha256": source_receipt["native_source_sha256"],
        "activation_manifest_sha256": source_receipt["activation_manifest_sha256"],
        "source_group_decision_sha256": source_receipt["source_group_decision_sha256"],
        "regime_fit_decision_sha256": source_receipt["regime_fit_decision_sha256"],
        "execution_spec_receipt_sha256": execution_spec["receipt_sha256"],
        "cost_contract_sha256": frozen.precommitment["cost_contract_sha256"],
        ("frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256"):
            frozen.precommitment["frozen_tier_pair_contract_sha256" if frozen.frozen_tier_pair_contract_bytes else "cost_ladder_contract_sha256"],
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




def _packed_r_e12_vector(
    values: Sequence[float],
    *,
    maximum_abs_r: float,
    code: str,
) -> str:
    try:
        e12_values = [
            winner._encode_r_e12(value, maximum_abs_r, code) for value in values
        ]
        return winner._encode_r_e12_i64be_base64(
            e12_values,
            maximum_abs_r,
            code,
        )
    except ValueError as exc:
        raise WinnerRawEvidenceError(code) from exc




def _tier_event_vectors(events: Any, *, fold_start: datetime, fold_end_exclusive: datetime) -> tuple[list, list, list, list, list, list]:
    if type(events) is not list or len(events) > winner.MAX_TRADES_PER_FOLD:
        _fail("TIER_COMPLETED_POSITION_ROSTER_INVALID")
    opens, closes, ids, open_orders, close_orders, values = [], [], [], [], [], []
    previous = None
    for ordinal, event in enumerate(events):
        if not isinstance(event, Mapping) or event.get("trade_record_schema_version") != REQUIRED_TRADE_RECORD_SCHEMA:
            _fail("TIER_COMPLETED_POSITION_SCHEMA_INVALID")
        position = event.get("position_id")
        if type(position) is not str or _BACKTEST_POSITION_ID.fullmatch(position) is None or position in ids:
            _fail("TIER_COMPLETED_POSITION_ID_INVALID")
        opened = _utc_second(event.get("entry_time"), "TIER_COMPLETED_POSITION_ENTRY_INVALID")
        closed = _utc_second(event.get("exit_time"), "TIER_COMPLETED_POSITION_EXIT_INVALID")
        opening = datetime.fromisoformat(opened.replace("Z", "+00:00"))
        closing = datetime.fromisoformat(closed.replace("Z", "+00:00"))
        open_order, close_order = event.get("entry_event_order"), event.get("exit_event_order")
        if any(type(value) is not int or not 0 <= value <= 2_147_483_647 for value in (open_order, close_order)):
            _fail("TIER_COMPLETED_POSITION_ORDER_INVALID")
        key = (closing, close_order, opening, open_order, position)
        if (opening < fold_start or closing >= fold_end_exclusive or (opening, open_order) >= (closing, close_order)
                or (previous is not None and key <= previous)):
            _fail("TIER_COMPLETED_POSITION_CAUSAL_ORDER_INVALID")
        previous = key
        try:
            value = canonical_backtest_winner_r(event.get("pnl_dollars"), event.get("risk_amount"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise WinnerRawEvidenceError("TIER_COMPLETED_POSITION_R_INVALID") from exc
        if not math.isfinite(value) or abs(value) > winner.MAX_ABS_TRADE_R:
            _fail("TIER_COMPLETED_POSITION_R_INVALID")
        opens.append(opened); closes.append(closed); ids.append(position)
        open_orders.append(open_order); close_orders.append(close_order); values.append(value)
    return opens, closes, ids, open_orders, close_orders, values


def _current_tier_trade_vector_body(row: Mapping[str, Any], *, axis_sha256: str, risk_sha256: str) -> dict[str, Any]:
    policy = winner.CURRENT_GENERATION.policy
    keys = ("spread_tier", "source_contract_sha256", "cost_contract_sha256", "entry_spread_admission_policy_sha256",
        "completed_trade_open_offset_seconds", "completed_trade_close_offset_seconds", "completed_trade_position_id_digests_base64",
        "completed_trade_event_order_anchor", "completed_trade_open_event_order_offsets", "completed_trade_close_event_order_offsets",
        "completed_trade_net_mtm_r_e12_i64be_base64", "position_activity_ledger_sha256", "trade_power_binding")
    return {"shared_fold_axis_row_sha256": axis_sha256, "accounting_identity_kind": policy["accounting_identity"],
        "completed_trade_unit": policy["daily_unit"], "inventory_boundary_kind": policy["inventory_boundary"],
        **{key: row[key] for key in keys}, "risk_unit_contract_sha256": risk_sha256}


def _build_evaluated_fold(
    sink: SymbolAuthoritySink, *, frozen: FrozenWinnerSearchPlan, attempt: Mapping[str, Any],
    attempt_artifacts: Mapping[str, Any], cross_binding: Mapping[str, Any],
    cross_binding_context: Mapping[str, Any], cross_binding_attempt: Mapping[str, Any],
    planned_fold: Mapping[str, Any], raw_fold: Mapping[str, Any],
    source_observation_entries: Optional[dict[str, dict[str, Any]]] = None,
    provenance_schemas: Optional[set[str]] = None,
    partition_cost_index: Optional[_PartitionCostIndex] = None,
) -> tuple[_FoldOutcome, dict[str, dict[str, Any]]]:
    ordinal = planned_fold["fold_ordinal"]
    if (not isinstance(raw_fold, Mapping) or set(raw_fold) != {"fold_ordinal", "provenance", "lanes"}
            or raw_fold["fold_ordinal"] != ordinal or not isinstance(raw_fold["lanes"], Mapping)
            or set(raw_fold["lanes"]) != set(_LANE_KEYS)):
        _fail(f"FOLD_{ordinal:03d}_RAW_SCHEMA_INVALID")
    lanes = raw_fold["lanes"]
    prefix = f"{_artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)}/attempt-{attempt['ordinal']:04d}/fold-{ordinal:03d}"
    entries = {"raw-" + name: _spool_lane(sink, prefix=prefix, lane=name, result=lanes[name]) for name in _LANE_KEYS}
    hashes, provenance_entries, provenance = _provenance_artifacts(sink, prefix=prefix, frozen=frozen, attempt=attempt,
        attempt_artifacts=attempt_artifacts, cross_binding=cross_binding, cross_binding_context=cross_binding_context,
        cross_binding_attempt=cross_binding_attempt, planned_fold=planned_fold, lanes=lanes, raw=raw_fold["provenance"],
        source_observation_entries=source_observation_entries, provenance_schemas=provenance_schemas,
        partition_cost_index=partition_cost_index)
    entries.update({"provenance-" + key: value for key, value in provenance_entries.items()})
    start = datetime.combine(_day(planned_fold["oos_start_date_utc"], "FOLD_START_INVALID"), datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(_day(planned_fold["oos_end_date_utc"], "FOLD_END_INVALID") + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    final_axis = _partition_axis(lanes["final_lockbox_p75"], "final_lockbox")
    if pd.Timestamp(final_axis["start_utc"]).to_pydatetime() != start or pd.Timestamp(final_axis["end_utc"]).to_pydatetime() != end:
        _fail(f"FOLD_{ordinal:03d}_FINAL_AXIS_PLAN_MISMATCH")
    shared_partition = _shared_oos_partition_receipt(frozen=frozen, planned_fold=planned_fold,
        source_receipt=provenance["oos_source_receipt"])
    entries["shared-oos-partition"] = _put_ascii_json(sink, prefix + "/shared-oos-partition", "winner_shared_oos_partition", shared_partition)
    tier_evidence = {}
    dates = None
    pending = {}
    for tier in _TIERS:
        final = lanes["final_lockbox_" + tier.lower()]
        surface, tier_dates, daily = _validate_daily_surface(final, risk_contract=frozen.risk_unit_contract, planned_fold=planned_fold)
        if dates is None:
            dates = tier_dates
        elif dates != tier_dates:
            _fail("TIER_DAILY_CALENDAR_GEOMETRY_MISMATCH")
        strategy = attempt_artifacts["winner_process"]["strategy_id"]
        trades = _trade_identity_pnl_evidence(final, lane="final_lockbox_" + tier.lower())
        rebuilt = _canonical_trade_events(trades, namespace="|".join((frozen.symbol.upper(), frozen.timeframe.upper(), strategy)),
            lane="final_lockbox_" + tier.lower())
        if _trade_position_reconciliation_evidence(rebuilt, lane=tier) != _trade_position_reconciliation_evidence(final["position_events"], lane=tier):
            _fail("TIER_TRADE_POSITION_RECONCILIATION_INVALID")
        opens, closes, ids, open_orders, close_orders, trade_r = _tier_event_vectors(final["position_events"], fold_start=start, fold_end_exclusive=end)
        if sum((Decimal(str(value)) for value in daily), Decimal(0)) != sum((Decimal(str(value)) for value in trade_r), Decimal(0)):
            _fail("TIER_DAILY_TRADE_ACCOUNTING_MISMATCH")
        decisions = _power_chain(attempt_ledger=frozen.attempt_ledger, train=lanes["optimizer_train_" + tier.lower()],
            tuning=lanes["tuning_validation_" + tier.lower()], final=final, frozen=frozen, tier=tier)
        for decision in decisions:
            if decision is not None and not decision.passed and _power_status(decision) == "TECHNICAL_FAILURE":
                raise WinnerRawEvidenceIncomplete("TIER_TRADE_POWER_TECHNICAL_FAILURE", tier)
        power = _power_chain_artifact(decisions)
        entries[tier.lower() + "-trade-power-chain"] = _put_ascii_json(sink, prefix + "/" + tier.lower() + "/trade-power-chain", "winner_trade_power_chain", power)
        activity = {"schema_version": CURRENT_POSITION_ACTIVITY_SCHEMA, "attempt_id": attempt["attempt_id"],
            "fold_ordinal": ordinal, "spread_tier": tier, "position_events": final["position_events"]}
        activity_entry = _put_ascii_json(sink, prefix + "/" + tier.lower() + "/position-activity", "winner_position_activity_ledger", activity)
        entries[tier.lower() + "-position-activity"] = activity_entry
        entries[tier.lower() + "-daily-surface"] = _put_ascii_json(sink, prefix + "/" + tier.lower() + "/daily-r-surface", "winner_daily_r_surface", surface)
        binding = _fold_power_binding(attempt_ledger=frozen.attempt_ledger, train=decisions[0], final=decisions[2]) if all(decision is not None and decision.passed for decision in decisions) else None
        order_anchor, open_offsets, close_offsets = winner._encode_event_order_offsets(open_orders, close_orders, "TIER_TRADE_EVENT_ORDER_INVALID")
        level = next(item for item in frozen.frozen_tier_pair_contract["tiers"] if item["tier"] == tier)
        row = {"spread_tier": tier, "source_contract_sha256": _validated_source_identity(frozen.source_contract_seeds[tier]),
            "cost_contract_sha256": _sha_json(level["numerical_execution_cost_policy"]),
            "entry_spread_admission_policy_sha256": _sha_json({"schema_version": ADMISSION_SCHEMA, "entry_admission_policy": ADMISSION_POLICY}),
            "position_activity_ledger_sha256": activity_entry["descriptor"]["sha256"],
            "daily_net_mtm_r_e12_i64be_base64": _packed_r_e12_vector(daily, maximum_abs_r=winner.MAX_ABS_DAILY_R, code="TIER_DAILY_R_NOT_EXACT_E12"),
            "completed_trade_open_offset_seconds": winner._encode_utc_second_offsets(opens, fold_start_utc=start, fold_end_exclusive_utc=end, code="TIER_TRADE_OPEN_INVALID"),
            "completed_trade_close_offset_seconds": winner._encode_utc_second_offsets(closes, fold_start_utc=start, fold_end_exclusive_utc=end, code="TIER_TRADE_CLOSE_INVALID"),
            "completed_trade_position_id_digests_base64": winner._encode_position_id_digests(ids, "TIER_POSITION_IDS_INVALID"),
            "completed_trade_event_order_anchor": order_anchor, "completed_trade_open_event_order_offsets": open_offsets,
            "completed_trade_close_event_order_offsets": close_offsets,
            "completed_trade_net_mtm_r_e12_i64be_base64": _packed_r_e12_vector(trade_r, maximum_abs_r=winner.MAX_ABS_TRADE_R, code="TIER_TRADE_R_NOT_EXACT_E12"),
            "trade_power_binding": binding}
        pending[tier] = row
    axis_row = {**dict(planned_fold), "daily_dates_utc": dates, "shared_oos_partition": shared_partition}
    axis_sha = _sha_json(axis_row)
    for tier, row in pending.items():
        row["daily_surface_sha256"] = _sha_json({"shared_fold_axis_row_sha256": axis_sha,
            **{key: row[key] for key in ("spread_tier", "source_contract_sha256", "cost_contract_sha256", "entry_spread_admission_policy_sha256", "daily_net_mtm_r_e12_i64be_base64")}})
        row["trade_vector_sha256"] = _sha_json(_current_tier_trade_vector_body(row, axis_sha256=axis_sha, risk_sha256=frozen.precommitment["risk_unit_contract_sha256"]))
        tier_evidence[tier] = row
    common_hash_fields = {"anchor_selection_receipt_sha256": "anchor_selection_receipt", "anchor_horizon_decision_sha256": "anchor_horizon_decision",
        "anchor_trial_ledger_sha256": "anchor_trial_ledger", "anchor_selected_trial_sha256": "anchor_selected_trial", "anchor_refit_receipt_sha256": "anchor_refit_receipt",
        "attempt_replay_receipt_sha256": "attempt_replay_receipt", "attempt_oos_source_receipt_sha256": "oos_source_receipt"}
    fold = {"fold_ordinal": ordinal, **{key: hashes[role] for key, role in common_hash_fields.items()}, "tier_evidence": tier_evidence}
    return _FoldOutcome("AUTHENTICATED", fold, axis_row, ""), entries


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
) -> dict[str, Any]:
    if status not in _WINNER_STATUS_REASONS:
        _fail("EVIDENCE_STATUS_INVALID")
    normalized_folds = [
        json.loads(_canonical_bytes(dict(fold)).decode("ascii")) for fold in folds
    ]
    role = attempt["precommitted_evidence_role"]
    if role == "PRESEARCH_PROCESS_REJECTED":
        if status != "PROCESS_REJECTED" or normalized_folds:
            _fail("PRESEARCH_REJECTED_ATTEMPT_CARRIES_EVIDENCE")
        status_reason = "PRESEARCH_PROCESS_REJECTED"
    elif role == "EVALUATION_REQUIRED":
        if status != "TECHNICAL_FAILURE" and len(normalized_folds) == 0:
            _fail("EVALUATION_REQUIRED_FOLD_ROSTER_INCOMPLETE")
        status_reason = (
            None if status == "AUTHENTICATED" else _WINNER_STATUS_REASONS[status]
        )
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
        ("frozen_tier_pair_contract_sha256" if "frozen_tier_pair_contract_sha256" in attempt else "cost_ladder_contract_sha256"):
            attempt["frozen_tier_pair_contract_sha256" if "frozen_tier_pair_contract_sha256" in attempt else "cost_ladder_contract_sha256"],
        "risk_unit_contract_sha256": attempt["risk_unit_contract_sha256"],
        "evidence_status": status,
        "status_reason": status_reason,
        "fold_count": len(normalized_folds),
        "folds_sha256": _sha_json(normalized_folds),
        "folds": normalized_folds,
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


def _reopen_current_frozen_plan_artifacts(
    sink: SymbolAuthoritySink,
    *,
    frozen: FrozenWinnerSearchPlan,
    precommitment: Mapping[str, Any],
    attempt_ledger: Mapping[str, Any],
    source_seeds: Mapping[str, Any],
    pair_contract: Mapping[str, Any],
    risk_contract: Mapping[str, Any],
    partition_precommits: Mapping[str, Any],
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
        "source_contract_seeds": ("winner_source_contract_seeds", dict(source_seeds)),
        "frozen_tier_pair_contract": ("winner_frozen_tier_pair_contract", dict(pair_contract)),
        "risk_unit_contract": ("winner_risk_unit_contract", dict(risk_contract)),
        "partition_cost_precommitments": ("winner_partition_cost_precommitments", dict(partition_precommits)),
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

    expected_receipt = _build_frozen_winner_search_plan_receipt(
        context_identity={
            "generation_id": sink.context.generation_id,
            "generation_context_sha256": sink.context.context_sha256,
            "native_universe_sha256": sink.context.native_universe_sha256,
            "config_sha256": sink.context.config_sha256,
        },
        symbol=frozen.symbol,
        timeframe=frozen.timeframe,
        regime=frozen.regime,
        artifact_namespace=frozen.artifact_namespace,
        precommitment=precommitment,
        attempt_ledger=attempt_ledger,
        horizon_execution_contracts=horizon_contracts,
        partition_cost_precommitments=partition_precommits,
        artifact_entries=reopened,
        _generation=frozen.selection_generation,
    )

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


def _restore_frozen_winner_search_plan(
    *,
    context_identity: Mapping[str, str],
    symbol: str,
    timeframe: str,
    regime: str,
    artifact_namespace: Optional[str],
    manifest_entries: Sequence[Mapping[str, Any]],
    body_bytes: Sequence[bytes],
    _generation=winner.CURRENT_GENERATION,
) -> FrozenWinnerSearchPlan:
    """Restore a current plan from caller-authenticated sealed-manifest bodies.

    Entries and bodies are parallel sequences for the complete frozen subtree,
    including its receipt. This verifies their internal binding, not authority
    to issue a manifest or trade. No filesystem or sink access occurs.
    """
    identity = _restored_frozen_context_identity(context_identity)
    normalized_symbol = _identity(symbol, "SYMBOL_INVALID", 64)
    normalized_timeframe = _identity(timeframe, "TIMEFRAME_INVALID", 16).upper()
    normalized_regime = _identity(regime, "REGIME_INVALID", 32).upper()
    namespace = _validated_namespace(artifact_namespace)
    if normalized_timeframe not in winner.TIMEFRAMES or normalized_regime not in winner.REGIMES:
        _fail("FROZEN_PLAN_RESTORE_CELL_INVALID")
    if (not isinstance(manifest_entries, (list, tuple))
            or not isinstance(body_bytes, (list, tuple))
            or len(manifest_entries) != len(body_bytes)
            or not 1 <= len(manifest_entries) <= 10 + 4 * winner.MAX_ATTEMPTS):
        _fail("FROZEN_PLAN_RESTORE_INPUT_ROSTER_INVALID")
    prefix = _artifact_prefix(namespace, normalized_timeframe, normalized_regime)
    frozen_prefix = f"{prefix}/frozen/"
    entries_by_id: dict[str, dict[str, Any]] = {}
    encoded_by_id: dict[str, bytes] = {}
    decoded_by_id: dict[str, Any] = {}
    descriptor_fields = {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
    for entry, encoded in zip(manifest_entries, body_bytes, strict=True):
        if type(entry) is not dict or set(entry) != {"artifact_id", "artifact_type", "descriptor"}:
            _fail("FROZEN_PLAN_RESTORE_ENTRY_INVALID")
        artifact_id = _identity(entry["artifact_id"], "FROZEN_PLAN_RESTORE_ENTRY_INVALID", 1024)
        if not artifact_id.startswith(frozen_prefix) or artifact_id in entries_by_id:
            _fail("FROZEN_PLAN_RESTORE_ENTRY_ROSTER_INVALID")
        descriptor = entry["descriptor"]
        if (type(descriptor) is not dict or set(descriptor) != descriptor_fields
                or descriptor.get("schema_version") != ARTIFACT_DESCRIPTOR_SCHEMA
                or descriptor.get("format") != ASCII_JSON_FORMAT
                or type(descriptor.get("size_bytes")) is not int
                or type(encoded) is not bytes
                or not 1 <= len(encoded) <= winner.MAX_REQUEST_BYTES):
            _fail("FROZEN_PLAN_RESTORE_DESCRIPTOR_INVALID")
        digest = _hex(descriptor.get("sha256"), 64, "FROZEN_PLAN_RESTORE_DESCRIPTOR_INVALID")
        if (descriptor["size_bytes"] != len(encoded)
                or hashlib.sha256(encoded).hexdigest() != digest
                or descriptor["relative_path"] != f"objects/sha256/{digest[:2]}/{digest}.{ASCII_JSON_FORMAT}"):
            _fail("FROZEN_PLAN_RESTORE_BODY_BINDING_INVALID")
        try:
            body = json.loads(encoded.decode("ascii"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise WinnerRawEvidenceError("FROZEN_PLAN_RESTORE_BODY_INVALID") from exc
        if type(body) not in (dict, list) or _canonical_bytes(body) != encoded:
            _fail("FROZEN_PLAN_RESTORE_BODY_INVALID")
        entries_by_id[artifact_id] = dict(entry)
        encoded_by_id[artifact_id] = encoded
        decoded_by_id[artifact_id] = body

    def body_for(name: str, artifact_type: str, body_type: type = dict) -> Any:
        artifact_id = frozen_prefix + name
        if artifact_id not in entries_by_id:
            _fail("FROZEN_PLAN_RESTORE_ENTRY_MISSING", name)
        if (entries_by_id[artifact_id]["artifact_type"] != artifact_type
                or type(decoded_by_id[artifact_id]) is not body_type):
            _fail("FROZEN_PLAN_RESTORE_ARTIFACT_TYPE_INVALID", name)
        return decoded_by_id[artifact_id]

    receipt = body_for("receipt", "winner_frozen_search_plan_receipt")
    if (receipt.get("schema_version") != (V16_FROZEN_PLAN_SCHEMA if _generation is winner.V16_GENERATION else CURRENT_FROZEN_PLAN_SCHEMA)
            or any(receipt.get(name) != value for name, value in identity.items())
            or receipt.get("artifact_namespace") != namespace
            or receipt.get("symbol") != normalized_symbol
            or receipt.get("timeframe") != normalized_timeframe
            or receipt.get("regime") != normalized_regime):
        _fail("FROZEN_PLAN_RESTORE_RECEIPT_CONTEXT_INVALID")
    precommitment = body_for("precommitment", "winner_precommitment")
    attempt_ledger = body_for("attempt_ledger", "winner_attempt_ledger")
    source_seeds = body_for("source_contract_seeds", "winner_source_contract_seeds")
    pair_contract = body_for("frozen_tier_pair_contract", "winner_frozen_tier_pair_contract")
    risk_contract = body_for("risk_unit_contract", "winner_risk_unit_contract")
    partition_precommits = body_for("partition_cost_precommitments", "winner_partition_cost_precommitments")
    horizon_contracts = body_for("horizon_execution_contracts", "winner_horizon_execution_contracts", list)
    power_policy = body_for("trade_power_policy", "winner_trade_power_policy")
    multiplicity = body_for("multiplicity_receipt", "winner_trade_power_multiplicity")
    attempts = attempt_ledger.get("attempts")
    if type(attempts) is not list or not 1 <= len(attempts) <= winner.MAX_ATTEMPTS:
        _fail("FROZEN_PLAN_RESTORE_ATTEMPTS_INVALID")
    logical_entries = {
        name: entries_by_id[frozen_prefix + name]
        for name in ("precommitment", "attempt_ledger", "source_contract_seeds",
                     "frozen_tier_pair_contract", "risk_unit_contract", "partition_cost_precommitments",
                     "horizon_execution_contracts", "trade_power_policy", "multiplicity_receipt")
    }
    bundle = []
    for ordinal in range(len(attempts)):
        artifacts = {}
        for name in sorted(_ATTEMPT_ARTIFACT_KEYS):
            relative_id = f"attempt-{ordinal:04d}/{name}"
            artifacts[name] = body_for(relative_id, f"winner_attempt_{name}")
            logical_entries[f"attempt-{ordinal:04d}-{name}"] = entries_by_id[frozen_prefix + relative_id]
        bundle.append(artifacts)
    expected_ids = {frozen_prefix + "receipt"} | {
        entry["artifact_id"] for entry in logical_entries.values()
    }
    if set(entries_by_id) != expected_ids:
        _fail("FROZEN_PLAN_RESTORE_ENTRY_ROSTER_INVALID")
    try:
        expected_receipt = _build_frozen_winner_search_plan_receipt(
            context_identity=identity, symbol=normalized_symbol, timeframe=normalized_timeframe,
            regime=normalized_regime, artifact_namespace=namespace,
            precommitment=precommitment, attempt_ledger=attempt_ledger,
            horizon_execution_contracts=horizon_contracts,
            partition_cost_precommitments=partition_precommits, artifact_entries=logical_entries,
            _generation=_generation,
        )
    except WinnerRawEvidenceError:
        raise
    except (ValueError, KeyError, TypeError) as exc:
        raise WinnerRawEvidenceError("FROZEN_PLAN_RESTORE_RECEIPT_INVALID") from exc
    if receipt != expected_receipt:
        _fail("FROZEN_PLAN_RECEIPT_MISMATCH")
    source_seeds, pair_contract, pair_hashes = _current_pair_inputs(source_seeds, pair_contract)
    normalized_horizons = _normalized_horizon_execution_contracts(horizon_contracts,
        expected_count=len(attempts))
    stored_folds = precommitment.get("causal_fold_plan")
    if (type(stored_folds) is not list
            or any(type(fold) is not dict or set(fold) != {
                "fold_ordinal", "fold_id", "anchor_as_of_utc", "oos_start_date_utc", "oos_end_date_utc"
            } for fold in stored_folds)):
        _fail("FROZEN_PLAN_RESTORE_BINDING_INVALID")
    fold_inputs = [{name: fold[name] for name in (
        "anchor_as_of_utc", "oos_start_date_utc", "oos_end_date_utc")}
        for fold in stored_folds]
    folds, latest_boundary = _normalize_fold_plan(fold_inputs,
        symbol=normalized_symbol, timeframe=normalized_timeframe, regime=normalized_regime)
    partition_precommits = _validated_partition_cost_precommitments(partition_precommits,
        pair=pair_contract, attempts=attempts, folds=folds)
    common = {
        "cell_id": winner.compute_cell_id(normalized_symbol, normalized_timeframe, normalized_regime),
        "symbol": normalized_symbol, "timeframe": normalized_timeframe, "regime": normalized_regime,
        **pair_hashes, "risk_unit_contract_sha256": _sha_json(risk_contract),
        **{name: expected_receipt[name] for name in (
            "process_registry_sha256", "horizon_registry_sha256", "trial_registry_sha256")},
        "partition_cost_precommitments_sha256": _sha_json(partition_precommits),
    }
    if (precommitment.get("contract_version") != _generation.precommitment_version
            or precommitment.get("selection_policy_sha256") != _generation.policy_sha256
            or precommitment.get("frozen_tier_pair_contract") != pair_contract
            or precommitment.get("causal_fold_plan") != folds
            or precommitment.get("latest_evidence_boundary_utc") != latest_boundary
            or precommitment.get("planned_attempt_count") != len(attempts)
            or precommitment.get("planned_fold_count") != len(folds)
            or precommitment.get("causal_fold_plan_sha256") != _sha_json(folds)
            or attempt_ledger.get("contract_version") != winner.ATTEMPT_LEDGER_VERSION
            or attempt_ledger.get("precommitment_payload_sha256") != _sha_json(precommitment)
            or attempt_ledger.get("attempt_count") != len(attempts)
            or attempt_ledger.get("attempts_sha256") != _sha_json(attempts)
            or risk_contract != build_precommitted_risk_unit_contract()
            or normalized_horizons != horizon_contracts
            or power_policy != asdict(PRODUCTION_TRADE_POWER_POLICY)
            or multiplicity != build_trade_power_multiplicity_receipt(attempt_ledger)
            or any(precommitment.get(name) != value or attempt_ledger.get(name) != value
                   for name, value in common.items())):
        _fail("FROZEN_PLAN_RESTORE_BINDING_INVALID")
    precommitted_at = _utc(precommitment.get("precommitted_at_utc"), "PRECOMMITTED_AT_INVALID")
    frozen_at = _utc(attempt_ledger.get("frozen_at_utc"), "ATTEMPTS_FROZEN_AT_INVALID")
    if frozen_at <= precommitted_at:
        _fail("ATTEMPTS_NOT_FROZEN_AFTER_PRECOMMITMENT")
    frozen = FrozenWinnerSearchPlan(
        symbol=normalized_symbol, timeframe=normalized_timeframe, regime=normalized_regime,
        artifact_namespace=namespace, generation_context_sha256=identity["generation_context_sha256"],
        precommitment_bytes=encoded_by_id[frozen_prefix + "precommitment"],
        attempt_ledger_bytes=encoded_by_id[frozen_prefix + "attempt_ledger"],
        source_contract_seed_bytes=b"", cost_ladder_contract_bytes=b"",
        risk_unit_contract_bytes=encoded_by_id[frozen_prefix + "risk_unit_contract"],
        cost_ladder_adversity_plan_bytes=b"",
        frozen_receipt_bytes=encoded_by_id[frozen_prefix + "receipt"],
        frozen_receipt_entry_bytes=_canonical_bytes(entries_by_id[frozen_prefix + "receipt"]),
        horizon_execution_contracts_bytes=encoded_by_id[frozen_prefix + "horizon_execution_contracts"],
        attempt_artifacts_bundle_bytes=_canonical_bytes(bundle),
        source_contract_seeds_bytes=encoded_by_id[frozen_prefix + "source_contract_seeds"],
        frozen_tier_pair_contract_bytes=encoded_by_id[frozen_prefix + "frozen_tier_pair_contract"],
        partition_cost_precommitments_bytes=encoded_by_id[frozen_prefix + "partition_cost_precommitments"],
    )
    verified_bundle = _verified_frozen_attempt_bundle(frozen=frozen,
        attempt_ledger=attempt_ledger, frozen_receipt=receipt)
    normalized_attempts, _ = _normalize_attempt_artifacts(verified_bundle,
        precommitted_evidence_roles=[attempt.get("precommitted_evidence_role") for attempt in attempts],
        horizon_execution_contracts=normalized_horizons,
        common_hashes={**pair_hashes, "risk_unit_contract_sha256": _sha_json(risk_contract)},
        symbol=normalized_symbol, timeframe=normalized_timeframe, regime=normalized_regime)
    if normalized_attempts != attempts:
        _fail("FROZEN_PLAN_RESTORE_BINDING_INVALID")
    return frozen


def verify_historical_frozen_winner_cross_binding(
    encoded: bytes,
    *,
    frozen: FrozenWinnerSearchPlan,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], dict[str, Any]]:
    """Verify the canonical historical cross binding without a sink or writes."""
    if not isinstance(frozen, FrozenWinnerSearchPlan):
        _fail("FROZEN_PLAN_RESTORE_INPUT_INVALID")
    generation = frozen.selection_generation
    if type(encoded) is not bytes or not 1 <= len(encoded) <= winner.MAX_REQUEST_BYTES:
        _fail("PROVENANCE_CROSS_BINDING_BYTES_INVALID")
    binding = _decode_object(encoded, "PROVENANCE_CROSS_BINDING_BYTES_INVALID")
    if binding.get("schema_version") in {
        CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA,
        CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4,
        CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5,
    }:
        _fail("HISTORICAL_CROSS_BINDING_CURRENT_PLAN_FORBIDDEN")
    receipt = _decode_object(frozen.frozen_receipt_bytes, "FROZEN_RECEIPT_INVALID")
    if (receipt.get("schema_version") != (V16_FROZEN_PLAN_SCHEMA if generation is winner.V16_GENERATION else CURRENT_FROZEN_PLAN_SCHEMA)
            or not frozen.frozen_tier_pair_contract_bytes
            or binding.get("schema_version") != (HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
                if generation is winner.V16_GENERATION else HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3)):
        _fail("PROVENANCE_CROSS_BINDING_SCHEMA_INVALID")
    attempts = _verified_frozen_attempt_bundle(frozen=frozen,
        attempt_ledger=frozen.attempt_ledger, frozen_receipt=receipt)
    return _validated_horizon_winner_cross_binding(encoded, sink=None,
        frozen=frozen, attempt_artifacts=attempts)


def _check_cumulative_completed_trades(total, generation):
    try:
        winner.check_completed_trade_transport_count(total, generation=generation)
    except ValueError as exc:
        _fail(str(exc))


def _verify_produced_winner_decision(decision, request, generation):
    if not winner.verify_historical_cell_winner_decision(decision, request):
        if generation is winner.V16_GENERATION:
            _fail("WINNER_V16_DECISION_VERIFICATION_FAILED", ",".join(decision.reason_codes))
        _fail("WINNER_V15_DECISION_VERIFICATION_FAILED")


def _produce_winner_raw_evidence(
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
    _generation=winner.CURRENT_GENERATION,
) -> WinnerRawEvidenceProduction:
    """Produce current independent P75/P95 candidate evidence; no live authority."""

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
    source_seeds, pair_contract, pair_hashes = _current_pair_inputs(frozen.source_contract_seeds, frozen.frozen_tier_pair_contract)
    if (precommitment.get("contract_version") != _generation.precommitment_version
            or precommitment.get("selection_policy_sha256") != _generation.policy_sha256
            or precommitment.get("frozen_tier_pair_contract") != pair_contract
            or any(precommitment.get(name) != value for name, value in pair_hashes.items())
            or risk_contract != build_precommitted_risk_unit_contract()):
        _fail("FROZEN_TIER_PAIR_BINDING_INVALID")
    receipt = _decode_object(frozen.frozen_receipt_bytes, "FROZEN_RECEIPT_INVALID")
    horizon_contracts = _normalized_horizon_execution_contracts(frozen.horizon_execution_contracts,
        expected_count=attempt_ledger.get("attempt_count"))
    partition_precommits = _validated_partition_cost_precommitments(frozen.partition_cost_precommitments,
        pair=pair_contract, attempts=attempt_ledger.get("attempts"), folds=precommitment.get("causal_fold_plan"))
    partition_sha = _sha_json(partition_precommits)
    if (precommitment.get("partition_cost_precommitments_sha256") != partition_sha
            or attempt_ledger.get("partition_cost_precommitments_sha256") != partition_sha
            or receipt.get("partition_cost_precommitments_sha256") != partition_sha
            or receipt.get("precommitment_sha256") != _sha_json(precommitment)
            or receipt.get("attempt_ledger_sha256") != _sha_json(attempt_ledger)):
        _fail("FROZEN_PARTITION_COST_BINDING_INVALID")
    partition_cost_index = _index_partition_cost_precommitments(frozen, partition_precommits)
    attempt_bundles = _verified_frozen_attempt_bundle(frozen=frozen, attempt_ledger=attempt_ledger, frozen_receipt=receipt)
    receipt = _reopen_current_frozen_plan_artifacts(sink, frozen=frozen, precommitment=precommitment,
        attempt_ledger=attempt_ledger, source_seeds=source_seeds, pair_contract=pair_contract,
        risk_contract=risk_contract, partition_precommits=partition_precommits, horizon_contracts=horizon_contracts,
        attempt_artifacts=attempt_bundles, frozen_receipt=receipt)
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
    source_observation_entries: dict[str, dict[str, Any]] = {}
    provenance_schemas: set[str] = set()
    shared_fold_axis: Optional[list[dict[str, Any]]] = None
    total_completed_trades = 0
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
                _evidence_entry(attempt, status=status, folds=[])
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
        attempt_shared_axis: list[dict[str, Any]] = []
        attempt_status = "AUTHENTICATED"
        seen_position_ids = {tier: set() for tier in _TIERS}
        for planned_fold, raw_fold in zip(planned_folds, raw_folds, strict=True):
            try:
                outcome, fold_entries = _build_evaluated_fold(
                    sink,
                    frozen=frozen,
                    attempt=attempt,
                    attempt_artifacts=attempt_artifacts,
                    cross_binding=cross_binding,
                    cross_binding_context=cross_binding_context,
                    cross_binding_attempt=cross_binding_attempt,
                    planned_fold=planned_fold,
                    raw_fold=raw_fold,
                    source_observation_entries=source_observation_entries,
                    provenance_schemas=provenance_schemas,
                    partition_cost_index=partition_cost_index,
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
            folds.append(outcome.fold)
            attempt_shared_axis.append(outcome.shared_fold_axis_row)
            if outcome.status == "PROCESS_REJECTED":
                attempt_status = "PROCESS_REJECTED"
            for tier, tier_row in outcome.fold["tier_evidence"].items():
                count = len(tier_row["completed_trade_open_offset_seconds"])
                try:
                    positions = set(winner._decode_position_id_digests(
                        tier_row["completed_trade_position_id_digests_base64"], count, "COMPLETED_TRADE_POSITION_ID_DIGESTS_INVALID"))
                except ValueError as exc:
                    raise WinnerRawEvidenceError(str(exc)) from exc
                if seen_position_ids[tier] & positions:
                    _fail(f"ATTEMPT_{ordinal:04d}_{tier}_POSITION_ID_REUSED_ACROSS_FOLDS")
                seen_position_ids[tier] |= positions
                total_completed_trades += count
                _check_cumulative_completed_trades(total_completed_trades, _generation)
        if len(folds) != len(planned_folds):
            _fail("EVALUATION_REQUIRED_FOLD_ROSTER_INCOMPLETE")
        if shared_fold_axis is None:
            shared_fold_axis = attempt_shared_axis
        elif shared_fold_axis != attempt_shared_axis:
            _fail("SHARED_FOLD_AXIS_DRIFT_ACROSS_ATTEMPTS")
        finalized_folds: list[dict[str, Any]] = []
        for fold, axis_row in zip(folds, attempt_shared_axis, strict=True):
            finalized = dict(fold)
            fold_ordinal = finalized["fold_ordinal"]
            fold_entry = _put_ascii_json(
                sink,
                f"{prefix}/fold-{fold_ordinal:03d}/winner-fold",
                "winner_evaluated_fold",
                finalized,
            )
            spool_entries[
                f"attempt-{ordinal:04d}-fold-{fold_ordinal:03d}-winner-fold"
            ] = fold_entry
            finalized_folds.append(finalized)
        evidence_entries.append(
            _evidence_entry(
                attempt,
                status=attempt_status,
                folds=finalized_folds,
            )
        )
    try:
        next(raw_iterator)
    except StopIteration:
        pass
    else:
        _fail("ATTEMPT_EVIDENCE_ROSTER_EXTRA")
    if shared_fold_axis is None or len(shared_fold_axis) != len(
        precommitment["causal_fold_plan"]
    ):
        _fail("SHARED_FOLD_AXIS_UNAVAILABLE")
    source_prefix = _artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)
    registered_sources = {entry["artifact_id"]: entry for entry in sink.artifact_roster()
        if entry["artifact_id"].startswith(source_prefix + "/")
        and (entry["artifact_id"].startswith(source_prefix + "/source-observations/")
            or entry["artifact_type"] == SOURCE_OBSERVATION_ARTIFACT_TYPE)}
    if registered_sources != source_observation_entries:
        _fail("SOURCE_OBSERVATION_REFERENCE_CLOSURE_INVALID")
    for entry in source_observation_entries.values():
        spool_entries[f"source-observation-{entry['descriptor']['sha256']}"] = entry

    common_names = (
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "source_contract_sha256",
        "cost_contract_sha256",
        "frozen_tier_pair_contract_sha256",
        "risk_unit_contract_sha256",
        "process_registry_sha256",
        "horizon_registry_sha256",
        "trial_registry_sha256",
    )
    evidence_ledger = {
        "contract_version": _generation.evidence_version,
        "run_id": precommitment["run_id"],
        "generated_at_utc": completed,
        "evidence_access_started_at_utc": started,
        "evidence_access_completed_at_utc": completed,
        "latest_evidence_boundary_utc": precommitment["latest_evidence_boundary_utc"],
        "precommitment_payload_sha256": _sha_json(precommitment),
        "partition_cost_precommitments_sha256": partition_sha,
        "attempt_ledger_payload_sha256": _sha_json(attempt_ledger),
        **{name: precommitment[name] for name in common_names},
        "shared_fold_axis_count": len(shared_fold_axis),
        "shared_fold_axis_sha256": _sha_json(shared_fold_axis),
        "shared_fold_axis": shared_fold_axis,
        "attempt_count": len(attempt_ledger["attempts"]),
        "entries_sha256": _sha_json(evidence_entries),
        "entries": evidence_entries,
        "evidence_scope": "PROCESS_LEVEL_ONLY",
        "parameter_evidence_inherited": False,
    }
    request_builder = (winner.create_winner_selection_request_v16 if _generation is winner.V16_GENERATION
        else winner.create_winner_selection_request)
    try:
        request = request_builder(precommitment, attempt_ledger, evidence_ledger)
    except ValueError as exc:
        raise WinnerRawEvidenceError(str(exc)) from exc
    if _generation is not winner.V16_GENERATION:
        if request is None:
            _fail("WINNER_V15_REQUEST_REJECTED")
        if len(_canonical_bytes(request.to_dict())) > _generation.transport.max_request_bytes:
            _fail("WINNER_V15_REQUEST_BYTE_BOUND_EXCEEDED")
    decision = winner.evaluate_historical_cell_winner(request)
    _verify_produced_winner_decision(decision, request, _generation)
    marginal = winner.historical_book_marginal_receipt(decision, request)
    prefix = (
        f"{_artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)}"
        "/authority"
    )
    final_bodies = {
        "evidence-ledger": ("winner_evidence_ledger", evidence_ledger),
        "winner-request": ("winner_v16_request" if _generation is winner.V16_GENERATION else "winner_v15_request", request.to_dict()),
        "winner-decision": ("winner_v16_decision" if _generation is winner.V16_GENERATION else "winner_v15_decision", decision.to_dict()),
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
        "schema_version": V16_PRODUCTION_SCHEMA if _generation is winner.V16_GENERATION else CURRENT_PRODUCTION_SCHEMA,
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


def build_frozen_winner_search_plan_v4_receipt(*, context_identity, symbol, timeframe, regime, artifact_namespace,
        precommitment, attempt_ledger, horizon_execution_contracts, partition_cost_precommitments, artifact_entries):
    return _build_frozen_winner_search_plan_receipt(**locals(), _generation=winner.CURRENT_GENERATION)


def build_frozen_winner_search_plan_v5_receipt(*, context_identity, symbol, timeframe, regime, artifact_namespace,
        precommitment, attempt_ledger, horizon_execution_contracts, partition_cost_precommitments, artifact_entries):
    return _build_frozen_winner_search_plan_receipt(**locals(), _generation=winner.V16_GENERATION)


def freeze_winner_search_plan_v4(sink, *, symbol, timeframe, regime, precommitted_at_utc, attempts_frozen_at_utc,
        fold_plan, attempt_artifacts, precommitted_evidence_roles, horizon_execution_contracts, source_contract_seeds,
        frozen_tier_pair_contract, risk_unit_contract, partition_cost_precommitments, artifact_namespace=None):
    return _freeze_winner_search_plan(**locals(), _generation=winner.CURRENT_GENERATION)


def freeze_winner_search_plan_v5(sink, *, symbol, timeframe, regime, precommitted_at_utc, attempts_frozen_at_utc,
        fold_plan, attempt_artifacts, precommitted_evidence_roles, horizon_execution_contracts, source_contract_seeds,
        frozen_tier_pair_contract, risk_unit_contract, partition_cost_precommitments, artifact_namespace=None):
    return _freeze_winner_search_plan(**locals(), _generation=winner.V16_GENERATION)


def restore_frozen_winner_search_plan_v4(*, context_identity, symbol, timeframe, regime, artifact_namespace,
        manifest_entries, body_bytes):
    return _restore_frozen_winner_search_plan(**locals(), _generation=winner.CURRENT_GENERATION)


def restore_frozen_winner_search_plan_v5(*, context_identity, symbol, timeframe, regime, artifact_namespace,
        manifest_entries, body_bytes):
    return _restore_frozen_winner_search_plan(**locals(), _generation=winner.V16_GENERATION)


def produce_winner_v15_raw_evidence(sink, *, frozen, attempt_evidence, evidence_access_started_at_utc,
        evidence_access_completed_at_utc, provenance_cross_binding_bytes=None, production_cell_search_plan=None,
        production_search_process=None, artifact_namespace=None):
    return _produce_winner_raw_evidence(**locals(), _generation=winner.CURRENT_GENERATION)


def produce_winner_v16_raw_evidence(sink, *, frozen, attempt_evidence, evidence_access_started_at_utc,
        evidence_access_completed_at_utc, provenance_cross_binding_bytes=None, production_cell_search_plan=None,
        production_search_process=None, artifact_namespace=None):
    return _produce_winner_raw_evidence(**locals(), _generation=winner.V16_GENERATION)


def freeze_winner_search_plan_v3(*args: Any, **kwargs: Any) -> FrozenWinnerSearchPlan:
    _fail("ARCHIVED_WINNER_V14_WRITER_RETIRED_FROZEN_TIER_PAIR_REQUIRED")


def produce_winner_v14_raw_evidence(*args: Any, **kwargs: Any) -> WinnerRawEvidenceProduction:
    _fail("ARCHIVED_WINNER_V14_WRITER_RETIRED_FROZEN_TIER_PAIR_REQUIRED")


__all__ = [
    "V16_FROZEN_PLAN_SCHEMA", "V16_PRODUCTION_SCHEMA",
    "HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4", "CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5",
    "build_frozen_winner_search_plan_v5_receipt", "freeze_winner_search_plan_v5",
    "restore_frozen_winner_search_plan_v5", "produce_winner_v16_raw_evidence",
    "CURRENT_FROZEN_PLAN_SCHEMA", "CURRENT_RAW_LANE_SCHEMA", "CURRENT_POSITION_ACTIVITY_SCHEMA",
    "CURRENT_PRODUCTION_SCHEMA", "CURRENT_AUTHORITATIVE_PROVENANCE_SCHEMA",
    "REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA", "SOURCE_OBSERVATION_REFERENCE_SCHEMA",
    "SOURCE_OBSERVATION_ARTIFACT_TYPE", "publish_source_observation_receipt",
    "resolve_source_observation_reference", "verify_source_observation_reference_closure",
    "HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3", "CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4",
    "COST_LADDER_ADVERSITY_EXPECTED_NO_SHOCK",
    "COST_LADDER_ADVERSITY_EXPECTED_STRICT",
    "COST_LADDER_ADVERSITY_PLAN_SCHEMA",
    "CURRENT_CROSS_BINDING_MODE",
    "DAILY_R_SURFACE_SCHEMA",
    "FROZEN_COST_LADDER_CONTRACT_SCHEMA",
    "FROZEN_PLAN_SCHEMA",
    "HISTORICAL_CROSS_BINDING_MODE",
    "HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA",
    "CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA",
    "PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS",
    "PRODUCTION_TRADE_POWER_POLICY",
    "PARTITION_COST_PRECOMMITMENTS_SCHEMA",
    "PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA",
    "FrozenWinnerSearchPlan",
    "WinnerRawEvidenceError",
    "WinnerRawEvidenceIncomplete",
    "WinnerRawEvidenceProduction",
    "build_frozen_winner_search_plan_v4_receipt",
    "restore_frozen_winner_search_plan_v4",
    "verify_historical_frozen_winner_cross_binding",
    "build_flat_daily_r_surface",
    "build_precommitted_risk_unit_contract",
    "build_partition_cost_precommitments",
    "expand_partition_cost_precommitment",
    "build_trade_source_contract_from_backtester_result",
    "freeze_winner_search_plan_v3",
    "freeze_winner_search_plan_v4",
    "produce_winner_v14_raw_evidence",
    "produce_winner_v15_raw_evidence",
]
