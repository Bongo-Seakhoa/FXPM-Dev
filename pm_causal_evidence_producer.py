"""Bounded production of decision-v8 historical causal evidence.

The producer in this module is deliberately an adapter, not an authority.  It
reopens an already verified historical-anchor orchestration run and a sealed,
standalone-verified forward-OOS replay product.  Portfolio returns,
cost lanes, activity, specialist Book projections, and reconciliation are all
derived from those bytes.  Callers cannot supply return vectors or evidence
hashes.

The resulting payloads still have to pass :mod:`pm_causal_process`'s released
decision-v8 evaluator and verifier before a ``CausalEvidenceProduct`` is
returned.  No current-refit, capital, publication, or trading authority is
created here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, DecimalException
import hashlib
import json
from pathlib import Path
import re
import threading
from typing import Any, Callable, Final, Mapping, Sequence

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_historical_anchor_orchestration as historical
import pm_production_capital_bootstrap as capital_bootstrap
import pm_production_authority_coordinator as coordinator
import pm_specialist_arbitration as specialist_arbitration
import pm_winner_selection as winner_selection


EVENT_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-portfolio-event-ledger.v1"
FILL_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-fill-ledger.v1"
MTM_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-daily-mtm-ledger.v1"
COST_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-cost-ledger.v1"
EXPOSURE_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-exposure-ledger.v1"
MARGIN_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-margin-ledger.v1"
SPECIALIST_SCHEMA: Final = "fxpm.anchor-replayed-specialist.v1"
CONTROLLER_EVENT_SCHEMA: Final = "fxpm.anchor-controller-event.v1"
ARBITRATION_EVENT_SCHEMA: Final = "fxpm.anchor-arbitration-event.v1"
ORDER_EVENT_SCHEMA: Final = "fxpm.anchor-order-event.v1"
FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA: Final = (
    "fxpm.causal-forward-native-product-request.v1"
)

_FORWARD_PRODUCER_RECEIPT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "production_contract_version",
        "generation_id",
        "generation_context_sha256",
        "native_universe_sha256",
        "config_sha256",
        "historical_precommit_receipt_sha256",
        "historical_run_receipt_sha256",
        "causal_precommit_payload_sha256",
        "forward_replay_precommit_receipt_sha256",
        "forward_replay_precommit_descriptor",
        "capital_bootstrap_identity_sha256",
        "capital_precommitment_body_sha256",
        "capital_contract_lineage_sha256",
        "capital_margin_terms_sha256",
        "capital_broker_terms_sha256",
        "evidence_access_started_at_utc",
        "execution_started_at_utc",
        "execution_completed_at_utc",
        "reconciled_at_utc",
        "deployment_binding",
        "code_identity",
        "anchor_ledger_descriptors",
        "anchor_production_metadata",
        "ordered_anchor_ids",
        "capital_phase_manifest_descriptors",
        "ordered_capital_phase_ids",
        "capital_candidate_records_complete_pre_policy",
        "capital_phase_window_contract",
        "valuation_contract",
        "valuation_event_order",
        "full_round_trip_commission_in_every_candidate_mark",
        "stop_risk_includes_adverse_configured_stop_slippage",
        "candidate_marks_thinned_or_sampled",
        "causal_consumer_reopened_and_verified",
        "causal_binding_followup_required",
        "confers_causal_process_current_refit_capital_readiness_publication_or_trading_authority",
    }
)
_FORWARD_PRECOMMIT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "generation_id",
        "generation_context_sha256",
        "native_universe_sha256",
        "config_sha256",
        "historical_precommit_receipt_sha256",
        "historical_run_receipt_sha256",
        "historical_run_payload_sha256",
        "causal_precommit_payload_sha256",
        "created_at_utc",
        "created_before_first_forward_oos_frame_access",
        "all_native_source_access_times_not_after_precommit",
        "required_rosters",
        "anchor_product_identities",
        "anchor_product_identities_sha256",
        "capital_phase_windows_by_anchor",
        "capital_phase_windows_sha256",
        "capital_forward_phase_binding",
        "capital_forward_phase_binding_sha256",
        "capital_bootstrap_identity_sha256",
        "capital_precommitment_body_sha256",
        "capital_contract_lineage_sha256",
        "capital_margin_terms_sha256",
        "capital_broker_terms_sha256",
        "deployment_binding",
        "code_identity",
        "valuation_contract",
        "product_cutoff_contract",
        "confers_causal_current_refit_capital_publication_or_trading_authority",
    }
)

_CAPITAL_CONTRACT_FIELDS: Final = frozenset(
    {
        "book_contract_version",
        "refit_contract_version",
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    }
)
_CAPITAL_OPERATIONAL_LINEAGE_FIELDS: Final = frozenset(
    {
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
        "margin_terms_sha256",
        "broker_terms_sha256",
    }
)
_CAPITAL_LINEAGE_FIELDS: Final = frozenset(
    {*_CAPITAL_CONTRACT_FIELDS, "book_role", *_CAPITAL_OPERATIONAL_LINEAGE_FIELDS}
)

_DESCRIPTOR_FIELDS: Final = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
)
_HEX64: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SAFE_ID: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$", re.ASCII)
_MAX_RAW_ARTIFACT_BYTES: Final = 48 * 1024 * 1024
_MAX_RAW_BYTES_PER_ANCHOR: Final = 64 * 1024 * 1024
_MAX_RAW_BYTES_TOTAL: Final = 256 * 1024 * 1024
_MAX_EVENT_DECISION_BYTES: Final = 64 * 1024
_MAX_PARAMETERS_BYTES: Final = 256 * 1024
_MAX_EVENTS_PER_DAY: Final = 10_000
_MAX_POSITIONS: Final = 10_000
_MAX_COMPONENT_SOURCE_BYTES: Final = 32 * 1024 * 1024
_MIN_PRECOMMIT_WINDOW_SECONDS: Final = 60
_MAX_PRECOMMIT_WINDOW_SECONDS: Final = 7 * 24 * 60 * 60

_COMPONENT_SOURCE_FILES: Final = {
    "native_source_contract_code_sha256": ("pm_native_request_contract.py",),
    "activation_contract_code_sha256": ("pm_native_activation.py",),
    "continuity_validation_code_sha256": ("pm_native_continuity.py",),
    "regime_fit_code_sha256": ("pm_regime.py", "pm_book_multiplicity.py"),
    "training_horizon_code_sha256": ("pm_training_horizon.py",),
    "trial_registry_code_sha256": ("pm_current_refit_artifacts.py",),
    "winner_selection_code_sha256": (
        "pm_winner_selection.py",
        "pm_winner_raw_evidence.py",
    ),
    "book_multiplicity_code_sha256": ("pm_book_multiplicity.py",),
    "specialist_arbitration_code_sha256": ("pm_specialist_arbitration.py",),
    "institution_state_code_sha256": ("pm_institutional_supervisor.py",),
    "risk_policy_code_sha256": ("pm_risk_admission.py",),
    "cost_model_code_sha256": ("pm_core.py",),
    "synchronized_replay_code_sha256": (
        "pm_portfolio_replay.py",
        "pm_causal_evidence_producer.py",
        "pm_forward_oos_replay_producer.py",
    ),
    "reconciliation_authority_code_sha256": (
        "pm_causal_process.py",
        "pm_causal_evidence_producer.py",
    ),
}
_POLICY_COMPONENT_KEYS: Final = {
    "native_source_policy_sha256": "native_source_contract_code_sha256",
    "activation_contract_policy_sha256": "activation_contract_code_sha256",
    "continuity_validation_policy_sha256": "continuity_validation_code_sha256",
    "regime_fit_policy_sha256": "regime_fit_code_sha256",
    "training_horizon_policy_sha256": "training_horizon_code_sha256",
    "trial_registry_policy_sha256": "trial_registry_code_sha256",
    "winner_selection_policy_sha256": "winner_selection_code_sha256",
    "book_multiplicity_policy_sha256": "book_multiplicity_code_sha256",
    "specialist_arbitration_policy_sha256": ("specialist_arbitration_code_sha256"),
    "institution_state_policy_sha256": "institution_state_code_sha256",
    "risk_policy_sha256": "risk_policy_code_sha256",
    "cost_model_policy_sha256": "cost_model_code_sha256",
    "synchronized_replay_policy_sha256": "synchronized_replay_code_sha256",
    "reconciliation_authority_policy_sha256": ("reconciliation_authority_code_sha256"),
}

_COMMON_LEDGER_FIELDS: Final = frozenset(
    {
        "schema_version",
        "anchor_id",
        "forward_start_date",
        "forward_end_date",
        "days",
    }
)
_EVENT_LEDGER_FIELDS: Final = frozenset(
    {
        *_COMMON_LEDGER_FIELDS,
        "selection_cutoff_utc",
        "evidence_access_started_at_utc",
        "execution_started_at_utc",
        "execution_completed_at_utc",
        "reconciled_at_utc",
        "specialists",
        "initial_positions",
    }
)
_FILL_LEDGER_FIELDS: Final = frozenset({*_COMMON_LEDGER_FIELDS, "initial_positions"})
_DAY_EVENT_FIELDS: Final = frozenset({"date", "entries", "marks", "exits"})
_DAY_FILL_FIELDS: Final = frozenset({"date", "entries", "exits"})
_DAY_MTM_FIELDS: Final = frozenset({"date", "equity_basis_cash", "position_marks"})
_DAY_COST_FIELDS: Final = frozenset({"date", "entries", "marks", "exits"})
_DAY_EXPOSURE_FIELDS: Final = frozenset({"date", "positions"})
_DAY_MARGIN_FIELDS: Final = frozenset(
    {"date", "equity_basis_cash", "used_margin_cash", "free_margin_cash"}
)
_RAW_OPEN_FIELDS: Final = frozenset(
    {
        "event_id",
        "position_id",
        "cell_id",
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
    }
)
_RAW_CARRIED_OPEN_FIELDS: Final = frozenset(
    {*_RAW_OPEN_FIELDS, "forward_window_open_mark_price"}
)
_RAW_CLOSE_FIELDS: Final = frozenset(
    {
        "event_id",
        "position_id",
        "closed_at_utc",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
    }
)
_RAW_MARK_FIELDS: Final = frozenset(
    {
        "event_id",
        "position_id",
        "marked_at_utc",
        "mark_price",
        "mark_bid_price",
        "mark_ask_price",
    }
)
_OPEN_COST_FIELDS: Final = frozenset(
    {
        "event_id",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    }
)
_MARK_COST_FIELDS: Final = frozenset(
    {"event_id", "base_financing_cash", "stress_financing_cash"}
)
_EXPOSURE_POSITION_FIELDS: Final = frozenset(
    {
        "position_id",
        "quantity",
        "contract_multiplier",
        "stop_price",
        "required_margin_cash",
    }
)
_EVENT_RECORD_FIELDS: Final = frozenset(
    {"event_id", "position_id", "cell_id", "controller", "arbitration", "order"}
)
_MARK_EVENT_FIELDS: Final = frozenset({"event_id", "position_id"})
_SPECIALIST_FIELDS: Final = frozenset(
    {
        "schema_version",
        "cell_id",
        "strategy_module",
        "strategy_class",
        "parameters",
        "refit_market_data_end_utc",
    }
)
_CONTROLLER_FIELDS: Final = frozenset(
    {"schema_version", "event_id", "cell_id", "regime", "decided_at_utc"}
)
_ARBITRATION_FIELDS: Final = frozenset(
    {"schema_version", "event_id", "cell_id", "decision", "decided_at_utc"}
)
_ORDER_FIELDS: Final = frozenset(
    {"schema_version", "event_id", "position_id", "action", "decided_at_utc"}
)


class CausalEvidenceProductionError(ValueError):
    """One typed, fail-closed producer failure."""

    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


def _fail(code: str) -> None:
    raise CausalEvidenceProductionError(code)


def _canonical_bytes(value: Any, maximum: int | None = None) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise CausalEvidenceProductionError("CANONICAL_JSON_INVALID") from exc
    if maximum is not None and len(encoded) > maximum:
        _fail("CANONICAL_JSON_BOUND_EXCEEDED")
    return encoded


def _copy_json(value: Any, maximum: int) -> Any:
    return json.loads(_canonical_bytes(value, maximum).decode("utf-8"))


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _exact_dict(value: Any, keys: frozenset[str], code: str) -> dict[str, Any]:
    if type(value) is not dict or frozenset(value) != keys:
        _fail(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        _fail(code)
    return value


def _hex64(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or len(value) != 20 or not value.endswith("Z"):
        _fail(code)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise CausalEvidenceProductionError(code) from exc
    return parsed


def _day(value: Any, code: str) -> date:
    if type(value) is not str or len(value) != 10:
        _fail(code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise CausalEvidenceProductionError(code) from exc
    if parsed.isoformat() != value:
        _fail(code)
    return parsed


def _decimal(value: Any, code: str, *, positive: bool = False) -> Decimal:
    try:
        parsed = causal._economic_decimal(value, code, strictly_positive=positive)
    except Exception as exc:
        raise CausalEvidenceProductionError(code) from exc
    return parsed


def _descriptor(value: Any, code: str) -> dict[str, Any]:
    descriptor = _exact_dict(value, _DESCRIPTOR_FIELDS, code)
    if (
        descriptor["schema_version"] != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA
        or descriptor["format"] != authority_spool.CANONICAL_JSON_FORMAT
        or type(descriptor["relative_path"]) is not str
        or not descriptor["relative_path"]
        or type(descriptor["size_bytes"]) is not int
        or type(descriptor["size_bytes"]) is bool
        or not 1 <= descriptor["size_bytes"] <= _MAX_RAW_ARTIFACT_BYTES
    ):
        _fail(code)
    _hex64(descriptor["sha256"], code)
    return _copy_json(descriptor, 64 * 1024)


def _read_descriptor_json(
    context: authority_spool.GenerationSpoolContext,
    value: Any,
    *,
    code: str,
    maximum: int = _MAX_RAW_ARTIFACT_BYTES,
) -> dict[str, Any]:
    descriptor = _descriptor(value, f"{code}_DESCRIPTOR_INVALID")
    if descriptor["size_bytes"] > maximum:
        _fail(f"{code}_BOUND_EXCEEDED")
    try:
        root = Path(context.root).resolve(strict=True)
        path = root.joinpath(*Path(descriptor["relative_path"]).parts).resolve(
            strict=True
        )
        path.relative_to(root)
        raw = path.read_bytes()
    except (OSError, RuntimeError, ValueError) as exc:
        raise CausalEvidenceProductionError(f"{code}_ARTIFACT_UNAVAILABLE") from exc
    if (
        len(raw) != descriptor["size_bytes"]
        or len(raw) > maximum
        or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]
    ):
        _fail(f"{code}_ARTIFACT_INTEGRITY_MISMATCH")
    try:
        result = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CausalEvidenceProductionError(f"{code}_JSON_INVALID") from exc
    if type(result) is not dict or _canonical_bytes(result, maximum) != raw:
        _fail(f"{code}_CANONICAL_MISMATCH")
    return result


def _forward_utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CausalEvidenceProductionError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(code)
    return parsed


def _forward_oos_replay_lineage(
    *,
    context: authority_spool.GenerationSpoolContext,
    product: Any,
    historical_precommit: historical.HistoricalAnchorPrecommit,
    historical_run_receipt: historical.HistoricalAnchorRunReceipt,
    causal_precommit_payload: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    dict[str, "AnchorReplayArtifactDescriptors"],
    dict[str, tuple[Any, ...]],
    dict[str, dict[str, Any]],
]:
    """Verify the sealed forward product and derive its persistent v8 lineage."""

    try:
        # Local import avoids the intentional module cycle: the forward
        # producer owns the product type and imports our raw-ledger dataclass.
        import pm_forward_oos_replay_producer as forward

        receipt, capital_candidates, operational_lineages = (
            forward.reopen_forward_oos_capital_phase_evidence(context, product)
        )
    except Exception as exc:
        raise CausalEvidenceProductionError(
            "FORWARD_OOS_REPLAY_PRODUCT_REVERIFY_FAILED"
        ) from exc
    receipt = _exact_dict(
        receipt,
        _FORWARD_PRODUCER_RECEIPT_FIELDS,
        "FORWARD_OOS_PRODUCER_RECEIPT_FIELDS_INVALID",
    )
    precommit_sha = causal.canonical_sha256(dict(causal_precommit_payload))
    receipt_descriptor = _descriptor(
        dict(product.producer_receipt_descriptor),
        "FORWARD_OOS_PRODUCER_RECEIPT_DESCRIPTOR_INVALID",
    )
    forward_precommit_descriptor = _descriptor(
        receipt["forward_replay_precommit_descriptor"],
        "FORWARD_OOS_PRECOMMIT_DESCRIPTOR_INVALID",
    )
    forward_precommit = _exact_dict(
        _read_descriptor_json(
            context,
            forward_precommit_descriptor,
            code="FORWARD_OOS_PRECOMMIT",
        ),
        _FORWARD_PRECOMMIT_FIELDS,
        "FORWARD_OOS_PRECOMMIT_FIELDS_INVALID",
    )
    plans = causal_precommit_payload.get("process_definition")
    plans = plans.get("anchor_plan") if type(plans) is dict else None
    if type(plans) is not list:
        _fail("CAUSAL_PRECOMMIT_ANCHOR_PLAN_INVALID")
    anchor_ids = [plan.get("anchor_id") for plan in plans]
    ledger_roster = receipt["anchor_ledger_descriptors"]
    metadata = receipt["anchor_production_metadata"]
    required = forward_precommit["required_rosters"]
    product_identities = forward_precommit["anchor_product_identities"]
    if (
        type(ledger_roster) is not dict
        or set(ledger_roster) != set(anchor_ids)
        or type(metadata) is not dict
        or set(metadata) != set(anchor_ids)
        or type(required) is not dict
        or set(required) != set(anchor_ids)
        or type(product_identities) is not list
        or [row.get("anchor_id") if type(row) is dict else None for row in product_identities]
        != anchor_ids
        or receipt.get("ordered_anchor_ids") != anchor_ids
    ):
        _fail("FORWARD_OOS_ANCHOR_ROSTER_MISMATCH")
    try:
        code_identity = forward._validate_replay_code_identity(
            receipt["code_identity"]
        )
    except Exception as exc:
        raise CausalEvidenceProductionError(
            "FORWARD_OOS_CODE_IDENTITY_INVALID"
        ) from exc
    causal_component = code_identity.get(
        "causal_synchronized_replay_component_identity_sha256"
    )
    causal_manifest = code_identity.get(
        "causal_synchronized_replay_component_manifest"
    )
    producer_identity = code_identity.get(
        "forward_oos_replay_producer_source_identity_sha256"
    )
    producer_manifest = code_identity.get(
        "forward_oos_replay_producer_source_manifest"
    )
    process = causal_precommit_payload["process_definition"]
    components = process.get("component_identities")
    if (
        type(components) is not dict
        or causal_component
        != components.get("synchronized_replay_code_sha256")
        or _sha(causal_manifest) != causal_component
        or _sha(producer_manifest) != producer_identity
        or code_identity.get("forward_producer_in_causal_component_manifest")
        is not True
        or code_identity.get("causal_binding_followup_required") is not True
    ):
        _fail("FORWARD_OOS_CODE_IDENTITY_BINDING_MISMATCH")
    causal_files = causal_manifest.get("files") if type(causal_manifest) is dict else None
    producer_files = (
        producer_manifest.get("files") if type(producer_manifest) is dict else None
    )
    if (
        type(causal_files) is not list
        or type(producer_files) is not list
        or len(producer_files) != 1
        or producer_files[0] not in causal_files
        or producer_files[0].get("relative_path")
        != "pm_forward_oos_replay_producer.py"
    ):
        _fail("FORWARD_OOS_PRODUCER_NOT_IN_CAUSAL_COMPONENT")
    expected_context = {
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": historical_precommit.receipt_sha256,
        "historical_run_receipt_sha256": historical_run_receipt.receipt_sha256,
        "causal_precommit_payload_sha256": precommit_sha,
    }
    if any(
        receipt.get(key) != expected or forward_precommit.get(key) != expected
        for key, expected in expected_context.items()
    ):
        _fail("FORWARD_OOS_PRODUCT_CONTEXT_BINDING_MISMATCH")
    if (
        receipt.get("schema_version")
        != causal.PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA
        or receipt.get("production_contract_version")
        != causal.PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION
        or forward_precommit.get("schema_version")
        != causal.PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA
        or receipt_descriptor["sha256"] != product.producer_receipt_sha256
        or forward_precommit_descriptor["sha256"]
        != receipt.get("forward_replay_precommit_receipt_sha256")
        or forward_precommit.get("historical_run_payload_sha256")
        != _sha(historical_run_receipt.to_dict())
        or forward_precommit.get("anchor_product_identities_sha256")
        != _sha(product_identities)
        or receipt.get("deployment_binding")
        != forward_precommit.get("deployment_binding")
        or receipt.get("code_identity") != forward_precommit.get("code_identity")
        or receipt.get("valuation_contract") != forward.VALUATION_CONTRACT
        or forward_precommit.get("valuation_contract") != forward.VALUATION_CONTRACT
        or forward_precommit.get("product_cutoff_contract")
        != "EXACT_FORWARD_END_EXCLUSIVE_NO_LATER_ROWS"
        or receipt.get("valuation_event_order")
        != "close_existing_then_mark_existing_then_admit"
        or receipt.get("full_round_trip_commission_in_every_candidate_mark")
        is not True
        or receipt.get("stop_risk_includes_adverse_configured_stop_slippage")
        is not True
        or receipt.get("candidate_marks_thinned_or_sampled") is not False
        or forward_precommit.get("created_before_first_forward_oos_frame_access")
        is not True
        or forward_precommit.get("all_native_source_access_times_not_after_precommit")
        is not True
        or forward_precommit.get(
            "confers_causal_current_refit_capital_publication_or_trading_authority"
        )
        is not False
        or receipt.get("causal_consumer_reopened_and_verified") is not False
        or receipt.get("causal_binding_followup_required") is not True
        or receipt.get(
            "confers_causal_process_current_refit_capital_readiness_publication_or_trading_authority"
        )
        is not False
    ):
        _fail("FORWARD_OOS_PRODUCT_PRECOMMIT_BINDING_MISMATCH")
    clocks = (
        _forward_utc(forward_precommit["created_at_utc"], "FORWARD_PRECOMMIT_TIME_INVALID"),
        _forward_utc(receipt["evidence_access_started_at_utc"], "FORWARD_ACCESS_TIME_INVALID"),
        _forward_utc(receipt["execution_started_at_utc"], "FORWARD_EXECUTION_START_INVALID"),
        _forward_utc(receipt["execution_completed_at_utc"], "FORWARD_EXECUTION_END_INVALID"),
        _forward_utc(receipt["reconciled_at_utc"], "FORWARD_RECONCILIATION_TIME_INVALID"),
    )
    if list(clocks) != sorted(clocks):
        _fail("FORWARD_OOS_PROCESS_CLOCKS_REVERSED")
    normalized_rows: list[dict[str, Any]] = []
    per_anchor_sha: dict[str, str] = {}
    metadata_sha: dict[str, str] = {}
    derived_replay_artifacts: dict[str, AnchorReplayArtifactDescriptors] = {}
    lineage_descriptor_rows: dict[str, dict[str, Any]] = {}
    ledger_roles = (
        "EVENT_LEDGER",
        "FILL_LEDGER",
        "MTM_LEDGER",
        "COST_LEDGER",
        "EXPOSURE_LEDGER",
        "MARGIN_LEDGER",
    )
    for anchor_ordinal, anchor_id in enumerate(anchor_ids):
        row = ledger_roster[anchor_id]
        if type(row) is not dict or set(row) != set(ledger_roles):
            _fail("FORWARD_OOS_LEDGER_ROLE_ROSTER_INVALID")
        normalized = {
            role: _descriptor(
                row[role], "FORWARD_OOS_LEDGER_DESCRIPTOR_INVALID"
            )
            for role in ledger_roles
        }
        lineage_descriptor_rows[anchor_id] = normalized
        derived_replay_artifacts[anchor_id] = AnchorReplayArtifactDescriptors(
            event_ledger=normalized["EVENT_LEDGER"],
            fill_ledger=normalized["FILL_LEDGER"],
            mtm_ledger=normalized["MTM_LEDGER"],
            cost_ledger=normalized["COST_LEDGER"],
            exposure_ledger=normalized["EXPOSURE_LEDGER"],
            margin_ledger=normalized["MARGIN_LEDGER"],
        )
        per_anchor_sha[anchor_id] = _sha(
            {
                "schema": "fxpm.causal-forward-oos-anchor-descriptor-roster.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": anchor_ordinal,
                "ledger_descriptors": normalized,
            }
        )
        metadata_sha[anchor_id] = _sha(
            {
                "schema": "fxpm.causal-forward-oos-anchor-production-metadata.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": anchor_ordinal,
                "production_metadata": metadata[anchor_id],
            }
        )
        normalized_rows.append(
            {
                "anchor_id": anchor_id,
                "anchor_descriptor_roster_sha256": per_anchor_sha[anchor_id],
            }
        )
    metadata_rows = [
        {
            "anchor_id": anchor_id,
            "anchor_production_metadata_sha256": metadata_sha[anchor_id],
        }
        for anchor_id in anchor_ids
    ]
    lineage = {
        "schema": causal.FORWARD_OOS_REPLAY_LINEAGE_SCHEMA,
        "production_contract_version": receipt["production_contract_version"],
        "producer_receipt_schema_version": receipt["schema_version"],
        "producer_receipt_sha256": product.producer_receipt_sha256,
        "producer_receipt_descriptor_sha256": receipt_descriptor["sha256"],
        "forward_precommit_schema_version": forward_precommit["schema_version"],
        "forward_precommit_receipt_sha256": forward_precommit_descriptor["sha256"],
        "forward_precommit_descriptor_sha256": forward_precommit_descriptor["sha256"],
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "historical_precommit_receipt_sha256": historical_precommit.receipt_sha256,
        "historical_run_receipt_sha256": historical_run_receipt.receipt_sha256,
        "historical_run_payload_sha256": forward_precommit[
            "historical_run_payload_sha256"
        ],
        "causal_precommit_payload_sha256": precommit_sha,
        "code_identity_sha256": _sha(code_identity),
        "deployment_binding_sha256": _sha(receipt["deployment_binding"]),
        "evidence_access_started_at_utc": receipt[
            "evidence_access_started_at_utc"
        ],
        "execution_started_at_utc": receipt["execution_started_at_utc"],
        "execution_completed_at_utc": receipt["execution_completed_at_utc"],
        "reconciled_at_utc": receipt["reconciled_at_utc"],
        "ordered_anchor_ids": anchor_ids,
        "ordered_anchor_descriptor_roster_sha256": _sha(normalized_rows),
        "anchor_descriptor_roster_sha256_by_id": per_anchor_sha,
        "anchor_ledger_descriptors_by_id": lineage_descriptor_rows,
        "ordered_anchor_production_metadata_roster_sha256": _sha(metadata_rows),
        "anchor_production_metadata_sha256_by_id": metadata_sha,
        "anchor_production_metadata_by_id": _copy_json(
            metadata, 16 * 1024 * 1024
        ),
        "causal_binding_followup_completed": True,
        "confers_current_refit_capital_readiness_publication_or_trading_authority": False,
    }
    causal._validate_forward_oos_replay_lineage(lineage, plans, precommit_sha)
    binding = forward_precommit.get("capital_forward_phase_binding")
    try:
        binding = capital_bootstrap.verify_capital_forward_phase_binding_body(
            context, binding
        )
    except Exception as exc:
        raise CausalEvidenceProductionError(
            "FORWARD_CAPITAL_PHASE_BINDING_REVERIFY_FAILED"
        ) from exc
    contract = binding.get("contract_lineage")
    if type(contract) is not dict or set(contract) != _CAPITAL_CONTRACT_FIELDS:
        _fail("FORWARD_CAPITAL_CONTRACT_LINEAGE_INVALID")
    expected_phase_ids = {
        f"{anchor_id}|{phase}"
        for anchor_id in anchor_ids
        for phase in ("INNER_SELECTION", "OUTER_EVALUATION")
    }
    if (
        type(capital_candidates) is not dict
        or type(operational_lineages) is not dict
        or set(capital_candidates) != expected_phase_ids
        or set(operational_lineages) != expected_phase_ids
    ):
        _fail("FORWARD_CAPITAL_PHASE_ROSTER_INVALID")
    capital_lineages: dict[str, dict[str, Any]] = {}
    for phase_id in sorted(expected_phase_ids):
        operational = operational_lineages[phase_id]
        if (
            type(operational) is not dict
            or set(operational) != _CAPITAL_OPERATIONAL_LINEAGE_FIELDS
            or operational.get("margin_terms_sha256")
            != binding.get("margin_terms_sha256")
            or operational.get("broker_terms_sha256")
            != binding.get("broker_terms_sha256")
        ):
            _fail("FORWARD_CAPITAL_OPERATIONAL_LINEAGE_INVALID")
        completed = {
            **contract,
            "book_role": "ANCHOR_LOCAL_REFIT",
            **operational,
        }
        if set(completed) != _CAPITAL_LINEAGE_FIELDS:
            _fail("FORWARD_CAPITAL_LINEAGE_FIELDS_INVALID")
        capital_lineages[phase_id] = _copy_json(completed, 8 * 1024 * 1024)
    return (
        lineage,
        derived_replay_artifacts,
        capital_candidates,
        capital_lineages,
    )


@dataclass(frozen=True, slots=True)
class AnchorReplayArtifactDescriptors:
    """Six raw, independently content-addressed replay ledgers for one anchor."""

    event_ledger: Mapping[str, Any]
    fill_ledger: Mapping[str, Any]
    mtm_ledger: Mapping[str, Any]
    cost_ledger: Mapping[str, Any]
    exposure_ledger: Mapping[str, Any]
    margin_ledger: Mapping[str, Any]

    def descriptors(self) -> tuple[tuple[str, Mapping[str, Any]], ...]:
        return (
            ("EVENT_LEDGER", self.event_ledger),
            ("FILL_LEDGER", self.fill_ledger),
            ("MTM_LEDGER", self.mtm_ledger),
            ("COST_LEDGER", self.cost_ledger),
            ("EXPOSURE_LEDGER", self.exposure_ledger),
            ("MARGIN_LEDGER", self.margin_ledger),
        )


@dataclass(frozen=True, slots=True)
class FrozenCausalProcessPrecommit:
    """Immutable canonical decision-v8 precommitment and identity manifest."""

    _body_json: bytes
    _identity_manifest_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def identity_manifest(self) -> dict[str, Any]:
        return json.loads(self._identity_manifest_json.decode("utf-8"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _component_identities(
    repository_root: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    try:
        root = repository_root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CausalEvidenceProductionError("REPOSITORY_ROOT_INVALID") from exc
    if set(_COMPONENT_SOURCE_FILES) != set(causal.COMPONENT_IDENTITY_KEYS):
        _fail("COMPONENT_SOURCE_MAP_INCOMPLETE")
    identities: dict[str, str] = {}
    components: dict[str, Any] = {}
    for key in causal.COMPONENT_IDENTITY_KEYS:
        files: list[dict[str, Any]] = []
        for relative in _COMPONENT_SOURCE_FILES[key]:
            try:
                path = (root / relative).resolve(strict=True)
                path.relative_to(root)
                raw = path.read_bytes()
            except (OSError, RuntimeError, ValueError) as exc:
                raise CausalEvidenceProductionError(
                    "COMPONENT_SOURCE_UNAVAILABLE"
                ) from exc
            if not raw or len(raw) > _MAX_COMPONENT_SOURCE_BYTES:
                _fail("COMPONENT_SOURCE_BOUND_INVALID")
            files.append(
                {
                    "relative_path": relative,
                    "size_bytes": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            )
        body = {
            "schema": "fxpm.causal-component-source-roster.v1",
            "component_identity_key": key,
            "files": files,
        }
        identities[key] = _sha(body)
        components[key] = {**body, "component_identity_sha256": identities[key]}
    return identities, components


def _policy_identities(
    component_identities: Mapping[str, str],
    *,
    search_process_receipt_sha256: str,
) -> tuple[dict[str, str], dict[str, Any]]:
    if set(_POLICY_COMPONENT_KEYS) != set(causal.POLICY_IDENTITY_KEYS):
        _fail("POLICY_COMPONENT_MAP_INCOMPLETE")
    process_policy = causal.pinned_process_policy()
    exact_bodies: dict[str, Any] = {
        "regime_fit_policy_sha256": causal._FROZEN_REGIME_FIT_POLICY,
        "risk_policy_sha256": causal._FROZEN_RISK_POLICY,
        "cost_model_policy_sha256": causal._FROZEN_COST_POLICY,
        "winner_selection_policy_sha256": winner_selection.pinned_policy_snapshot(),
        "book_multiplicity_policy_sha256": book.pinned_policy_snapshot(),
        "specialist_arbitration_policy_sha256": (
            specialist_arbitration.arbitration_contract_snapshot()
        ),
        "reconciliation_authority_policy_sha256": process_policy,
    }
    identities: dict[str, str] = {}
    manifests: dict[str, Any] = {}
    process_policy_sha = _sha(process_policy)
    for key in causal.POLICY_IDENTITY_KEYS:
        component_key = _POLICY_COMPONENT_KEYS[key]
        component_sha = component_identities[component_key]
        exact = exact_bodies.get(key)
        if key == "trial_registry_policy_sha256":
            identity = _hex64(
                search_process_receipt_sha256,
                "SEARCH_PROCESS_RECEIPT_INVALID",
            )
            policy_body = {
                "schema": "fxpm.causal-verifier-sealed-search-process-policy.v1",
                "production_search_process_receipt_sha256": identity,
                "component_identity_sha256": component_sha,
                "opaque_or_caller_asserted_hash": False,
            }
            body = {
                "schema": "fxpm.causal-policy-identity-manifest.v1",
                "policy_identity_key": key,
                "component_identity_key": component_key,
                "component_identity_sha256": component_sha,
                "policy_body": policy_body,
                "policy_identity_sha256": identity,
            }
        elif key in {
            "regime_fit_policy_sha256",
            "risk_policy_sha256",
            "cost_model_policy_sha256",
        }:
            identity = _sha(exact)
            body = {
                "schema": "fxpm.causal-exact-policy-identity.v1",
                "policy_identity_key": key,
                "component_identity_key": component_key,
                "component_identity_sha256": component_sha,
                "policy_body": _copy_json(exact, 2 * 1024 * 1024),
                "policy_identity_sha256": identity,
            }
        else:
            policy_body = (
                _copy_json(exact, 2 * 1024 * 1024)
                if exact is not None
                else {
                    "schema": "fxpm.causal-code-bound-policy.v1",
                    "policy_identity_key": key,
                    "component_identity_sha256": component_sha,
                    "process_policy_sha256": process_policy_sha,
                }
            )
            identity = _sha(policy_body)
            body = {
                "schema": "fxpm.causal-policy-identity-manifest.v1",
                "policy_identity_key": key,
                "component_identity_key": component_key,
                "component_identity_sha256": component_sha,
                "policy_body": policy_body,
                "policy_identity_sha256": identity,
            }
        identities[key] = identity
        manifests[key] = body
    return identities, manifests


def freeze_causal_process_precommit_v8(
    *,
    context: authority_spool.GenerationSpoolContext,
    search_process: Any,
    process_id: str,
    anchors: Sequence[Mapping[str, Any]],
    evidence_access_window_seconds: int = 3 * 24 * 60 * 60,
    repository_root: Path | None = None,
) -> FrozenCausalProcessPrecommit:
    """Freeze genuine code/policy/geometry identities before evidence access.

    Authorization and access-window clocks are created internally from the
    trusted process clock.  The caller supplies only the already selected
    historical market geometry and a bounded duration; it cannot backdate an
    authorization timestamp or inject opaque component/policy hashes.  The
    trial-registry identity is derived only by reopening the verifier-sealed
    production search process in the exact generation context.
    """

    try:
        try:
            import pm_production_search_planner as search_planner

            if type(context) is not authority_spool.GenerationSpoolContext:
                _fail("GENERATION_CONTEXT_INVALID")
            search_planner.verify_frozen_production_search_process(
                context,
                search_process,
            )
            search_process_receipt_sha256 = _hex64(
                search_process.receipt_sha256,
                "SEARCH_PROCESS_RECEIPT_INVALID",
            )
        except CausalEvidenceProductionError:
            raise
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "SEARCH_PROCESS_REVERIFY_FAILED"
            ) from exc
        _safe_id(process_id, "PROCESS_ID_INVALID")
        if (
            type(evidence_access_window_seconds) is not int
            or type(evidence_access_window_seconds) is bool
            or not _MIN_PRECOMMIT_WINDOW_SECONDS
            <= evidence_access_window_seconds
            <= _MAX_PRECOMMIT_WINDOW_SECONDS
        ):
            _fail("EVIDENCE_ACCESS_WINDOW_INVALID")
        if type(anchors) not in {list, tuple}:
            _fail("ANCHOR_PLAN_TYPE_INVALID")
        normalized_anchors = [
            _exact_dict(
                _copy_json(value, 64 * 1024),
                frozenset(
                    {
                        "anchor_id",
                        "selection_cutoff_utc",
                        "forward_start_date",
                        "forward_end_date",
                        "purge_calendar_days",
                    }
                ),
                "ANCHOR_PLAN_FIELDS_INVALID",
            )
            for value in anchors
        ]
        # Decision-v8 owns ordering, purge, overlap, and 4-16 bounds.
        causal._validate_anchor_plan(normalized_anchors)
        root = (
            Path(__file__).resolve().parent
            if repository_root is None
            else Path(repository_root)
        )
        components, component_manifest = _component_identities(root)
        policies, policy_manifest = _policy_identities(
            components,
            search_process_receipt_sha256=search_process_receipt_sha256,
        )
        now = _now_utc()
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            _fail("TRUSTED_PRECOMMIT_CLOCK_INVALID")
        authorized = now.astimezone(timezone.utc).replace(microsecond=0)
        # The sealed process exists before this function returns, so the
        # process access boundary can begin at its own whole-second
        # authorization timestamp.  A synthetic +1 second gap made the real
        # caller impossible: Capital must be frozen after this precommit but
        # before evidence, while the caller is forbidden to sleep around a
        # clock race.  Concrete evidence producers are still invoked only
        # after the separately sealed Capital bootstrap.
        access_not_before = authorized
        deadline = authorized + timedelta(seconds=evidence_access_window_seconds)
        definition = {
            "schema": "fxpm-frozen-historical-institution-process-v8",
            "symbols": list(causal.SYMBOLS),
            "timeframes": list(causal.TIMEFRAMES),
            "regimes": list(causal.REGIMES),
            "cell_count": causal.CELL_COUNT,
            "book_multiplicity_contract_version": (
                causal.PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION
            ),
            "winner_selection_contract_version": (
                causal.PINNED_WINNER_SELECTION_CONTRACT_VERSION
            ),
            "winner_evidence_ledger_version": (
                causal.PINNED_WINNER_EVIDENCE_LEDGER_VERSION
            ),
            "regime_fit_contract_version": causal.PINNED_REGIME_FIT_CONTRACT_VERSION,
            "regime_detector_record_schema_version": (
                causal.REGIME_DETECTOR_RECORD_SCHEMA_VERSION
            ),
            "regime_detector_ledger_record_schema_version": (
                causal.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
            ),
            "component_identities": components,
            "policy_identities": policies,
            "regime_fit_policy": _copy_json(
                causal._FROZEN_REGIME_FIT_POLICY, 2 * 1024 * 1024
            ),
            "risk_policy": _copy_json(causal._FROZEN_RISK_POLICY, 2 * 1024 * 1024),
            "cost_policy": _copy_json(causal._FROZEN_COST_POLICY, 2 * 1024 * 1024),
            "anchor_plan": normalized_anchors,
            "scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
            "forbidden_authorities": [
                "CURRENT_REFIT",
                "MODEL_PROMOTION",
                "CAPITAL_ALLOCATION",
                "FUNDED_LIVE_TRADING",
                "LIVE_TRADING",
                "PROFITABILITY_CLAIM",
            ],
        }
        payload = {
            "schema": causal.PRECOMMIT_SCHEMA,
            "process_id": process_id,
            "authorized_at_utc": authorized.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "evidence_access_not_before_utc": access_not_before.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "evidence_access_deadline_utc": deadline.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "policy": causal.pinned_process_policy(),
            "policy_sha256": _sha(causal.pinned_process_policy()),
            "process_definition": definition,
            "process_definition_sha256": _sha(definition),
        }
        causal._validate_precommit_payload(payload)
        body_json = _canonical_bytes(payload, 16 * 1024 * 1024)
        identity_manifest = {
            "schema_version": "fxpm.causal-process-identity-manifest.v1",
            "process_id": process_id,
            "authorized_at_utc": payload["authorized_at_utc"],
            "repository_root": str(root.resolve(strict=True)),
            "components": component_manifest,
            "policies": policy_manifest,
            "process_definition_sha256": payload["process_definition_sha256"],
            "precommit_payload_sha256": hashlib.sha256(body_json).hexdigest(),
        }
        return FrozenCausalProcessPrecommit(
            body_json,
            _canonical_bytes(identity_manifest, 16 * 1024 * 1024),
        )
    except CausalEvidenceProductionError:
        raise
    except causal.ProcessVerificationError as exc:
        raise CausalEvidenceProductionError("PRECOMMIT_V8_REJECTED") from exc
    except Exception as exc:
        raise CausalEvidenceProductionError("PRECOMMIT_FREEZE_FAILED") from exc


@dataclass(frozen=True, slots=True)
class _HistoricalArtifacts:
    run_body: dict[str, Any]
    book_by_anchor: dict[str, dict[str, Any]]
    horizon_by_anchor: dict[str, list[dict[str, Any]]]


def _owned_book(payload: Any, expected_receipt: str) -> dict[str, Any]:
    if type(payload) is not dict:
        _fail("BOOK_DECISION_NOT_OBJECT")
    body = _copy_json(payload, 32 * 1024 * 1024)
    receipt = body.pop("receipt_sha256", None)
    if receipt != expected_receipt or _sha(body) != receipt:
        _fail("BOOK_DECISION_RECEIPT_MISMATCH")
    try:
        raw = json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        decision = book.BookMultiplicityDecision(raw, receipt)
    except Exception as exc:
        raise CausalEvidenceProductionError("BOOK_DECISION_OWNERSHIP_FAILED") from exc
    if book.verify_historical_book_multiplicity_decision(decision) is not True:
        _fail("BOOK_DECISION_RAW_REVERIFY_FAILED")
    verified = decision.to_dict()
    if (
        verified != payload
        or verified.get("valid") is not True
        or verified.get("optimization_run_complete") is not True
    ):
        _fail("BOOK_DECISION_INCOMPLETE")
    return verified


def _request_body(request: Any) -> dict[str, Any]:
    if type(request) is not coordinator.HistoricalEvidenceRequest:
        _fail("COORDINATOR_REQUEST_TYPE_INVALID")
    try:
        value = request.to_dict()
    except Exception as exc:
        raise CausalEvidenceProductionError("COORDINATOR_REQUEST_INVALID") from exc
    return _exact_dict(
        value,
        frozenset(
            {
                "schema_version",
                "historical_run_receipt_sha256",
                "anchor_book_receipt_sha256_by_id",
                "causal_precommit_payload_sha256",
                "authority_claimed",
            }
        ),
        "COORDINATOR_REQUEST_FIELDS_INVALID",
    )


def _reopen_historical_artifacts(
    context: authority_spool.GenerationSpoolContext,
    precommit: historical.HistoricalAnchorPrecommit,
    run_receipt: historical.HistoricalAnchorRunReceipt,
    request: Mapping[str, Any],
    causal_precommit_sha256: str,
) -> _HistoricalArtifacts:
    if (
        type(precommit) is not historical.HistoricalAnchorPrecommit
        or type(run_receipt) is not historical.HistoricalAnchorRunReceipt
    ):
        _fail("HISTORICAL_RECEIPT_OBJECT_INVALID")
    if (
        historical.verify_historical_anchor_run_receipt(context, precommit, run_receipt)
        is not True
    ):
        _fail("HISTORICAL_RUN_RAW_REVERIFY_FAILED")
    precommit_body = precommit.to_dict()
    run_body = run_receipt.to_dict()
    if (
        precommit_body.get("process_precommit_receipt_sha256")
        != causal_precommit_sha256
        or request["historical_run_receipt_sha256"] != run_receipt.receipt_sha256
        or request["causal_precommit_payload_sha256"] != causal_precommit_sha256
        or request["authority_claimed"] is not False
        or request["schema_version"] != coordinator.COORDINATOR_SCHEMA
    ):
        _fail("COORDINATOR_REQUEST_BINDING_MISMATCH")
    anchors = run_body.get("anchors")
    symbols = run_body.get("symbol_receipts")
    if type(anchors) is not list or type(symbols) is not list:
        _fail("HISTORICAL_RUN_ROSTER_INVALID")
    expected_book_receipts = {
        item["anchor_id"]: item["book_decision_sha256"] for item in anchors
    }
    if request["anchor_book_receipt_sha256_by_id"] != expected_book_receipts:
        _fail("COORDINATOR_BOOK_ROSTER_MISMATCH")

    anchor_ids = list(expected_book_receipts)
    books: dict[str, dict[str, Any]] = {}
    book_descriptors: dict[str, dict[str, Any]] = {}
    horizons: dict[str, list[dict[str, Any]]] = {
        anchor_id: [] for anchor_id in anchor_ids
    }
    if len(symbols) != len(book.PINNED_SYMBOL_ORDER):
        _fail("HISTORICAL_SYMBOL_RECEIPT_ROSTER_INVALID")

    # Deliberately reopen and release one symbol manifest/output at a time.
    for expected_symbol, symbol_receipt in zip(
        book.PINNED_SYMBOL_ORDER, symbols, strict=True
    ):
        try:
            manifest = authority_spool.verify_symbol_receipt(
                context.root,
                symbol_receipt,
                expected_generation_id=context.generation_id,
                expected_symbol=expected_symbol,
                expected_native_universe_sha256=context.native_universe_sha256,
                expected_config_sha256=context.config_sha256,
            )
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "HISTORICAL_SYMBOL_MANIFEST_REVERIFY_FAILED"
            ) from exc
        entries = {
            item.get("artifact_id"): item
            for item in manifest.get("artifacts", [])
            if type(item) is dict
        }
        for anchor_id in anchor_ids:
            output_entry = entries.get(f"{anchor_id}/optimizer-output")
            book_entry = entries.get(f"{anchor_id}/book-decision")
            if type(output_entry) is not dict or type(book_entry) is not dict:
                _fail("HISTORICAL_MANIFEST_ARTIFACT_MISSING")
            output = _read_descriptor_json(
                context,
                output_entry.get("descriptor"),
                code="HISTORICAL_OPTIMIZER_OUTPUT",
                maximum=context.limits.max_json_bytes,
            )
            if (
                output.get("schema_version") != historical.SYMBOL_OUTPUT_SCHEMA
                or output.get("anchor_id") != anchor_id
                or output.get("symbol") != expected_symbol
                or output.get("horizon_binding_count")
                != len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER)
                or output.get("confers_causal_or_current_authority") is not False
            ):
                _fail("HISTORICAL_OPTIMIZER_OUTPUT_INVALID")
            raw_bindings = output.get("horizon_bindings")
            if type(raw_bindings) is not list or len(raw_bindings) != 24:
                _fail("HISTORICAL_HORIZON_BINDINGS_INCOMPLETE")
            for binding in raw_bindings:
                horizons[anchor_id].append(_copy_json(binding, 64 * 1024))

            observed_descriptor = _descriptor(
                book_entry.get("descriptor"), "HISTORICAL_BOOK_DESCRIPTOR_INVALID"
            )
            prior = book_descriptors.get(anchor_id)
            if prior is None:
                book_descriptors[anchor_id] = observed_descriptor
                payload = _read_descriptor_json(
                    context,
                    observed_descriptor,
                    code="HISTORICAL_BOOK_DECISION",
                    maximum=context.limits.max_json_bytes,
                )
                books[anchor_id] = _owned_book(
                    payload, expected_book_receipts[anchor_id]
                )
            elif prior != observed_descriptor:
                _fail("HISTORICAL_BOOK_DESCRIPTOR_SPLICE")
        del manifest, entries

    planned = {item["anchor_id"]: item for item in precommit_body["anchors"]}
    for anchor_id, roster in horizons.items():
        if (
            len(roster) != book.FAMILY_SIZE
            or historical.historical_horizon_binding_roster_sha256(roster)
            != planned[anchor_id]["horizon_binding_roster_sha256"]
        ):
            _fail("HISTORICAL_HORIZON_ROSTER_MISMATCH")
    return _HistoricalArtifacts(run_body, books, horizons)


def _expected_dates(start_text: str, end_text: str) -> list[str]:
    start = _day(start_text, "FORWARD_START_INVALID")
    end = _day(end_text, "FORWARD_END_INVALID")
    if end < start:
        _fail("FORWARD_GEOMETRY_INVALID")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _validate_common_ledger(
    value: Any,
    *,
    schema: str,
    anchor: Mapping[str, Any],
    day_fields: frozenset[str],
    outer_fields: frozenset[str] = _COMMON_LEDGER_FIELDS,
) -> dict[str, Any]:
    ledger = _exact_dict(value, outer_fields, "RAW_LEDGER_FIELDS_INVALID")
    if (
        ledger["schema_version"] != schema
        or ledger["anchor_id"] != anchor["anchor_id"]
        or ledger["forward_start_date"] != anchor["forward_start_date"]
        or ledger["forward_end_date"] != anchor["forward_end_date"]
    ):
        _fail("RAW_LEDGER_ANCHOR_BINDING_MISMATCH")
    dates = _expected_dates(anchor["forward_start_date"], anchor["forward_end_date"])
    days = ledger["days"]
    if type(days) is not list or len(days) != len(dates):
        _fail("RAW_LEDGER_CALENDAR_INCOMPLETE")
    for expected, row in zip(dates, days, strict=True):
        _exact_dict(row, day_fields, "RAW_LEDGER_DAY_FIELDS_INVALID")
        if row["date"] != expected:
            _fail("RAW_LEDGER_CALENDAR_REORDERED")
    return ledger


def _load_replay_ledgers(
    context: authority_spool.GenerationSpoolContext,
    descriptors: AnchorReplayArtifactDescriptors,
    anchor: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if type(descriptors) is not AnchorReplayArtifactDescriptors:
        _fail("REPLAY_DESCRIPTOR_SET_INVALID")
    total = 0
    values: dict[str, dict[str, Any]] = {}
    descriptor_hashes: dict[str, str] = {}
    for role, raw_descriptor in descriptors.descriptors():
        descriptor = _descriptor(raw_descriptor, f"{role}_DESCRIPTOR_INVALID")
        total += descriptor["size_bytes"]
        if total > _MAX_RAW_BYTES_PER_ANCHOR:
            _fail("REPLAY_ANCHOR_BYTE_BOUND_EXCEEDED")
        descriptor_hashes[role] = descriptor["sha256"]
        values[role] = _read_descriptor_json(
            context, descriptor, code=role, maximum=_MAX_RAW_ARTIFACT_BYTES
        )
    event = _validate_common_ledger(
        values["EVENT_LEDGER"],
        schema=EVENT_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_EVENT_FIELDS,
        outer_fields=_EVENT_LEDGER_FIELDS,
    )
    if event["selection_cutoff_utc"] != anchor["selection_cutoff_utc"]:
        _fail("EVENT_LEDGER_CUTOFF_MISMATCH")
    _validate_common_ledger(
        values["FILL_LEDGER"],
        schema=FILL_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_FILL_FIELDS,
        outer_fields=_FILL_LEDGER_FIELDS,
    )
    _validate_common_ledger(
        values["MTM_LEDGER"],
        schema=MTM_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_MTM_FIELDS,
    )
    _validate_common_ledger(
        values["COST_LEDGER"],
        schema=COST_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_COST_FIELDS,
    )
    _validate_common_ledger(
        values["EXPOSURE_LEDGER"],
        schema=EXPOSURE_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_EXPOSURE_FIELDS,
    )
    _validate_common_ledger(
        values["MARGIN_LEDGER"],
        schema=MARGIN_LEDGER_SCHEMA,
        anchor=anchor,
        day_fields=_DAY_MARGIN_FIELDS,
    )
    return values, descriptor_hashes


def _specialists(
    event_ledger: Mapping[str, Any],
    book_body: Mapping[str, Any],
    cutoff: str,
) -> dict[str, dict[str, Any]]:
    winner_cells = {
        cell["cell_id"]: cell
        for cell in book_body["cells"]
        if cell["historical_state"] == "WINNER"
    }
    raw = event_ledger["specialists"]
    if type(raw) is not list or len(raw) != len(winner_cells):
        _fail("REPLAY_SPECIALIST_ROSTER_INCOMPLETE")
    result: dict[str, dict[str, Any]] = {}
    for item in raw:
        specialist = _exact_dict(
            item, _SPECIALIST_FIELDS, "REPLAY_SPECIALIST_FIELDS_INVALID"
        )
        cell_id = specialist["cell_id"]
        if (
            specialist["schema_version"] != SPECIALIST_SCHEMA
            or cell_id not in winner_cells
            or cell_id in result
            or specialist["strategy_module"] != "pm_strategies"
            or type(specialist["strategy_class"]) is not str
            or not specialist["strategy_class"]
            or type(specialist["parameters"]) is not dict
            or not specialist["parameters"]
        ):
            _fail("REPLAY_SPECIALIST_INVALID")
        _canonical_bytes(specialist["parameters"], _MAX_PARAMETERS_BYTES)
        if specialist["refit_market_data_end_utc"] != cutoff:
            _fail("SPECIALIST_REFIT_FUTURE_LEAKAGE")
        result[cell_id] = specialist
    return result


def _detector_ledger(raw_fit: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = raw_fit["payload"]
    record = payload.get("regime_detector_record")
    if record is None:
        return None
    body = {
        "schema_version": causal.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION,
        "regime_fit_contract_version": causal.PINNED_REGIME_FIT_CONTRACT_VERSION,
        "regime_fit_receipt_sha256": raw_fit["receipt_sha256"],
        "detector_record": _copy_json(record, 64 * 1024),
        "detector_record_sha256": record["record_sha256"],
    }
    return {**body, "ledger_record_sha256": _sha(body)}


def _causal_book_projection(
    *,
    anchor: Mapping[str, Any],
    book_body: Mapping[str, Any],
    horizon_bindings: Sequence[Mapping[str, Any]],
    specialists: Mapping[str, Mapping[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any] | None],
    list[dict[str, Any] | None],
    dict[str, str],
]:
    cutoff = anchor["selection_cutoff_utc"]
    cells = book_body["cells"]
    sources = book_body["source_groups"]
    fits = book_body["regime_fits"]
    if (
        len(cells) != causal.CELL_COUNT
        or len(sources) != 600
        or len(fits) != 600
        or len(horizon_bindings) != causal.CELL_COUNT
    ):
        _fail("BOOK_PROJECTION_ROSTER_INCOMPLETE")
    supports: list[dict[str, Any] | None] = []
    detectors: list[dict[str, Any] | None] = []
    for source, fit in zip(sources, fits, strict=True):
        if type(source) is not dict or type(fit) is not dict:
            _fail("BOOK_SOURCE_OR_FIT_INVALID")
        source_payload = source.get("payload")
        fit_payload = fit.get("payload")
        if type(source_payload) is not dict or type(fit_payload) is not dict:
            _fail("BOOK_SOURCE_OR_FIT_INVALID")
        support = fit_payload.get("train_regime_support")
        supports.append(None if support is None else _copy_json(support, 64 * 1024))
        detectors.append(_detector_ledger(fit))

    projected: list[dict[str, Any]] = []
    cell_sha_by_id: dict[str, str] = {}
    for ordinal, (raw_cell, binding) in enumerate(
        zip(cells, horizon_bindings, strict=True)
    ):
        expected = causal.expected_cell_roster()[ordinal]
        if (
            raw_cell["symbol"],
            raw_cell["timeframe"],
            raw_cell["regime"],
        ) != expected or binding.get("cell") != {
            "symbol": expected[0],
            "timeframe": expected[1],
            "regime": expected[2],
        }:
            _fail("BOOK_PROJECTION_CELL_ROSTER_MISMATCH")
        group = ordinal // len(causal.REGIMES)
        source = sources[group]
        fit = fits[group]
        source_payload = source["payload"]
        fit_payload = fit["payload"]
        raw_state = raw_cell["historical_state"]
        status = {
            "WINNER": "WINNER",
            "NO_WINNER": causal.NO_LOCAL_WINNER,
            book.REGIME_UNOBSERVED: causal.REGIME_UNOBSERVED,
            book.INSUFFICIENT_EVIDENCE: causal.INSUFFICIENT_EVIDENCE,
            "SOURCE_UNAVAILABLE": "SOURCE_UNAVAILABLE",
        }.get(raw_state)
        if status is None:
            _fail("BOOK_TECHNICAL_OR_UNSEARCHED_CELL")
        available = source_payload["availability"] == book.AVAILABLE
        if available is (status == "SOURCE_UNAVAILABLE"):
            _fail("BOOK_SOURCE_STATE_CONTRADICTION")
        searched = status in {"WINNER", causal.NO_LOCAL_WINNER}
        unobserved = status == causal.REGIME_UNOBSERVED
        insufficient = status == causal.INSUFFICIENT_EVIDENCE
        insufficient_binding = None
        if insufficient:
            process_state = raw_cell.get("process_state_payload")
            if type(process_state) is not dict:
                _fail("BOOK_INSUFFICIENT_PROCESS_STATE_MISSING")
            raw_insufficient = process_state.get("insufficient_evidence_binding")
            if type(raw_insufficient) is not dict:
                _fail("BOOK_INSUFFICIENT_BINDING_MISSING")
            insufficient_binding = _copy_json(raw_insufficient, 256 * 1024)
            horizon = insufficient_binding.get("horizon_plan_receipt_sha256")
            attempt = insufficient_binding.get(
                "process_rejection_attempt_ledger_sha256"
            )
        else:
            horizon = binding["plan_receipt_sha256"] if searched else None
            attempt = (
                raw_cell["winner_attempt_ledger_payload_sha256"] if searched else None
            )
        winner_request = raw_cell["winner_request_sha256"] if searched else None
        winner_evidence = (
            raw_cell["winner_evidence_ledger_payload_sha256"] if searched else None
        )
        winner_decision = raw_cell["winner_decision_sha256"] if searched else None
        if searched and binding.get("plan_status") not in {
            "READY_FOR_HISTORICAL_EVIDENCE",
            "HISTORICAL_BOUNDARY_LIMITED",
        }:
            _fail("BOOK_HORIZON_STATUS_MISMATCH")
        if unobserved and binding.get("plan_status") != (
            "HISTORICAL_REGIME_UNOBSERVED"
        ):
            _fail("BOOK_HORIZON_STATUS_MISMATCH")
        if insufficient and (
            binding.get("plan_status")
            not in {
                "READY_FOR_HISTORICAL_EVIDENCE",
                "HISTORICAL_BOUNDARY_LIMITED",
            }
            or binding.get("plan_receipt_sha256") != horizon
        ):
            _fail("BOOK_INSUFFICIENT_HORIZON_BINDING_MISMATCH")
        if status == "SOURCE_UNAVAILABLE" and binding.get("plan_status") != (
            "HISTORICAL_SOURCE_UNAVAILABLE"
        ):
            _fail("BOOK_HORIZON_STATUS_MISMATCH")

        specialist = specialists.get(raw_cell["cell_id"])
        if status == "WINNER":
            if specialist is None:
                _fail("WINNER_SPECIALIST_MISSING")
            process_sha = _hex64(
                raw_cell["local_process_sha256"], "WINNER_PROCESS_IDENTITY_INVALID"
            )
            strategy_code = _sha(
                {
                    "schema": "fxpm.anchor-strategy-code-binding.v1",
                    "strategy_module": specialist["strategy_module"],
                    "strategy_class": specialist["strategy_class"],
                    "winner_process_sha256": process_sha,
                }
            )
            parameters_sha = _sha(specialist["parameters"])
            trial_sha = _sha(
                {
                    "schema": "fxpm.anchor-trial-binding.v1",
                    "winner_process_sha256": process_sha,
                    "attempt_ledger_sha256": attempt,
                    "parameters_sha256": parameters_sha,
                }
            )
            refit_sha = _sha(
                {
                    "schema": "fxpm.anchor-refit-execution.v1",
                    "anchor_id": anchor["anchor_id"],
                    "selection_cutoff_utc": cutoff,
                    "book_entry_sha256": raw_cell["historical_book_entry_sha256"],
                    "winner_decision_sha256": winner_decision,
                    "specialist": specialist,
                }
            )
            refit_end = specialist["refit_market_data_end_utc"]
        else:
            if specialist is not None:
                _fail("NONWINNER_CARRIES_REPLAY_SPECIALIST")
            strategy_code = trial_sha = parameters_sha = refit_sha = refit_end = None

        detector = detectors[group]
        support = supports[group]
        decision = {
            "schema": "fxpm-anchor-local-cell-decision-v5",
            "anchor_id": anchor["anchor_id"],
            "symbol": raw_cell["symbol"],
            "timeframe": raw_cell["timeframe"],
            "regime": raw_cell["regime"],
            "selection_cutoff_utc": cutoff,
            "status": status,
            "source_state": (
                "AVAILABLE" if available else "AUTHENTIC_TERMINAL_UNAVAILABLE"
            ),
            "source_terminal_reason": (
                None if available else "BROKER_HISTORY_TERMINAL_ABSENCE"
            ),
            "source_receipt_sha256": source["receipt_sha256"],
            "regime_receipt_sha256": fit["receipt_sha256"],
            "regime_detector_ledger_record_sha256": (
                None if detector is None else detector["ledger_record_sha256"]
            ),
            "train_regime_support_receipt_sha256": (
                None if support is None else support["receipt_sha256"]
            ),
            "horizon_receipt_sha256": horizon,
            "full_attempt_ledger_sha256": attempt,
            "state_decision_receipt_sha256": raw_cell["historical_book_entry_sha256"],
            "insufficient_evidence_binding": insufficient_binding,
            "winner_selection_contract_version": (
                causal.PINNED_WINNER_SELECTION_CONTRACT_VERSION if searched else None
            ),
            "winner_evidence_ledger_contract_version": (
                causal.PINNED_WINNER_EVIDENCE_LEDGER_VERSION if searched else None
            ),
            "winner_request_sha256": winner_request,
            "winner_evidence_ledger_payload_sha256": winner_evidence,
            "training_feature_end_utc": None if not available else cutoff,
            "training_label_end_utc": None if not available else cutoff,
            "refit_market_data_end_utc": refit_end,
            "strategy_code_sha256": strategy_code,
            "trial_definition_sha256": trial_sha,
            "parameters_sha256": parameters_sha,
            "winner_decision_receipt_sha256": winner_decision,
            "anchor_refit_receipt_sha256": refit_sha,
        }
        decision_sha = _sha(decision)
        outer = {
            "symbol": raw_cell["symbol"],
            "timeframe": raw_cell["timeframe"],
            "regime": raw_cell["regime"],
            "status": status,
            "search_complete": True,
            "attempts_terminal": True,
            "technical_failure": False,
            "terminal_source_unavailable": not available,
            "attempt_count": (
                raw_cell["within_cell_attempted_trials_exact"]
                if searched or insufficient
                else 0
            ),
            "selection_as_of_utc": cutoff,
            "market_data_end_utc": None if not available else (refit_end or cutoff),
            "cell_decision": decision,
            "cell_decision_sha256": decision_sha,
        }
        projected.append(outer)
        cell_sha_by_id[raw_cell["cell_id"]] = decision_sha
    return projected, supports, detectors, cell_sha_by_id


def _unique_records(
    values: Any, fields: frozenset[str], code: str, *, identity: str = "event_id"
) -> dict[str, dict[str, Any]]:
    if type(values) is not list or len(values) > _MAX_EVENTS_PER_DAY:
        _fail(code)
    result: dict[str, dict[str, Any]] = {}
    for value in values:
        record = _exact_dict(value, fields, code)
        key = _safe_id(record.get(identity), code)
        if key in result:
            _fail(code)
        result[key] = record
    return result


def _event_decision_hashes(
    record: Any,
    *,
    cell: Mapping[str, Any],
    action: str,
    current_date: str,
) -> tuple[str, str, str]:
    event = _exact_dict(record, _EVENT_RECORD_FIELDS, "EVENT_RECORD_FIELDS_INVALID")
    event_id = _safe_id(event["event_id"], "EVENT_ID_INVALID")
    position_id = _safe_id(event["position_id"], "POSITION_ID_INVALID")
    if event["cell_id"] != cell["cell_id"]:
        _fail("EVENT_CELL_MISMATCH")
    controller = _exact_dict(
        event["controller"], _CONTROLLER_FIELDS, "CONTROLLER_EVENT_INVALID"
    )
    arbitration = _exact_dict(
        event["arbitration"], _ARBITRATION_FIELDS, "ARBITRATION_EVENT_INVALID"
    )
    order = _exact_dict(event["order"], _ORDER_FIELDS, "ORDER_EVENT_INVALID")
    for decision in (controller, arbitration, order):
        if decision["event_id"] != event_id:
            _fail("EVENT_DECISION_IDENTITY_MISMATCH")
        if _utc(decision["decided_at_utc"], "EVENT_DECISION_TIME_INVALID").date() != (
            _day(current_date, "EVENT_DATE_INVALID")
        ):
            _fail("EVENT_DECISION_DATE_MISMATCH")
    if (
        controller["schema_version"] != CONTROLLER_EVENT_SCHEMA
        or controller["cell_id"] != cell["cell_id"]
        or controller["regime"] != cell["regime"]
        or arbitration["schema_version"] != ARBITRATION_EVENT_SCHEMA
        or arbitration["cell_id"] != cell["cell_id"]
        or arbitration["decision"] != ("ADMIT" if action == "OPEN" else "EXIT")
        or order["schema_version"] != ORDER_EVENT_SCHEMA
        or order["position_id"] != position_id
        or order["action"] != action
    ):
        _fail("EVENT_DECISION_SEMANTICS_INVALID")
    for body in (controller, arbitration, order):
        _canonical_bytes(body, _MAX_EVENT_DECISION_BYTES)
    return _sha(controller), _sha(arbitration), _sha(order)


def _causal_open_fill(
    raw: Mapping[str, Any],
    cost: Mapping[str, Any],
    *,
    cell: Mapping[str, Any],
    anchor_id: str,
    decisions: tuple[str, str, str],
    descriptor_hashes: Mapping[str, str],
    carried: bool,
) -> dict[str, Any]:
    primitive = {
        key: raw[key]
        for key in (
            "position_id",
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
        )
    }
    primitive["cell_decision_sha256"] = cell["cell_decision_sha256"]
    primitive["timeframe"] = cell["timeframe"]
    primitive.update({key: cost[key] for key in _OPEN_COST_FIELDS if key != "event_id"})
    payload_sha = _sha(
        {
            "schema": "fxpm-position-opening-fill-payload-v2",
            "anchor_id": anchor_id,
            "fill": primitive,
        }
    )
    result = {
        **primitive,
        "opening_fill_payload_sha256": payload_sha,
        "opening_fill_receipt_sha256": _sha(
            {
                "schema": "fxpm-derived-opening-fill-receipt.v1",
                "anchor_id": anchor_id,
                "opening_fill_payload_sha256": payload_sha,
                "controller_decision_sha256": decisions[0],
                "arbitration_decision_sha256": decisions[1],
                "order_decision_sha256": decisions[2],
                "raw_ledger_sha256_by_role": dict(sorted(descriptor_hashes.items())),
            }
        ),
    }
    if carried:
        result["forward_window_open_mark_price"] = raw["forward_window_open_mark_price"]
        mark_payload = _sha(
            {
                "schema": "fxpm-forward-window-opening-mark-payload-v1",
                "anchor_id": anchor_id,
                "position_id": raw["position_id"],
                "forward_start_date": None,
                "mark_price": raw["forward_window_open_mark_price"],
            }
        )
        # The exact forward date is substituted by _assemble_activity.
        result["forward_window_open_mark_payload_sha256"] = mark_payload
        result["forward_window_open_mark_receipt_sha256"] = _sha(
            {
                "schema": "fxpm-derived-forward-mark-receipt.v1",
                "payload_sha256": mark_payload,
                "raw_ledger_sha256_by_role": dict(sorted(descriptor_hashes.items())),
            }
        )
    return result


def _opening_cost(fill: Mapping[str, Any], *, stress: bool) -> Decimal:
    side = Decimal(1) if fill["side"] == "LONG" else Decimal(-1)
    unit = _decimal(fill["quantity"], "QUANTITY_INVALID", positive=True) * _decimal(
        fill["contract_multiplier"], "CONTRACT_MULTIPLIER_INVALID", positive=True
    )
    mid = _decimal(fill["reference_mid_price"], "REFERENCE_MID_INVALID", positive=True)
    executable = _decimal(
        fill["reference_ask_price"] if side > 0 else fill["reference_bid_price"],
        "REFERENCE_EXECUTABLE_INVALID",
        positive=True,
    )
    lane = "stress" if stress else "base"
    lane_fill = _decimal(
        fill[f"{lane}_fill_price"], "FILL_PRICE_INVALID", positive=True
    )
    spread = side * (executable - mid) * unit
    slippage = side * (lane_fill - executable) * unit
    execution = max(Decimal(0), spread + slippage)
    return (
        execution
        + _decimal(fill[f"{lane}_commission_cash"], "COMMISSION_INVALID")
        + _decimal(fill[f"{lane}_fee_cash"], "FEE_INVALID")
    )


def _closing_cost(
    fill: Mapping[str, Any], state: Mapping[str, Any], *, stress: bool
) -> Decimal:
    side = state["side_sign"]
    unit = state["unit_scale"]
    mid = _decimal(fill["reference_mid_price"], "REFERENCE_MID_INVALID", positive=True)
    executable = _decimal(
        fill["reference_bid_price"] if side > 0 else fill["reference_ask_price"],
        "REFERENCE_EXECUTABLE_INVALID",
        positive=True,
    )
    lane = "stress" if stress else "base"
    lane_fill = _decimal(
        fill[f"{lane}_fill_price"], "FILL_PRICE_INVALID", positive=True
    )
    spread = side * (mid - executable) * unit
    slippage = side * (executable - lane_fill) * unit
    execution = max(Decimal(0), spread + slippage)
    return (
        execution
        + _decimal(fill[f"{lane}_commission_cash"], "COMMISSION_INVALID")
        + _decimal(fill[f"{lane}_fee_cash"], "FEE_INVALID")
    )


def _assemble_activity(
    *,
    anchor: Mapping[str, Any],
    ledgers: Mapping[str, Mapping[str, Any]],
    descriptor_hashes: Mapping[str, str],
    projected_cells: Sequence[Mapping[str, Any]],
    book_cells: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[Decimal], list[Decimal], list[Decimal]]:
    dates = _expected_dates(anchor["forward_start_date"], anchor["forward_end_date"])
    by_cell_id = {
        raw["cell_id"]: projected_cells[index] for index, raw in enumerate(book_cells)
    }
    raw_book_by_id = {raw["cell_id"]: raw for raw in book_cells}
    event = ledgers["EVENT_LEDGER"]
    fill = ledgers["FILL_LEDGER"]
    mtm = ledgers["MTM_LEDGER"]
    cost = ledgers["COST_LEDGER"]
    exposure = ledgers["EXPOSURE_LEDGER"]
    margin = ledgers["MARGIN_LEDGER"]
    if event["initial_positions"] != [] or fill["initial_positions"] != []:
        # Carried positions are valid in decision-v8, but a production replay
        # seam must provide a dedicated forward-boundary valuation artifact.
        # The raw v1 contract intentionally fails closed until that descriptor
        # is available rather than inventing an opening mark receipt.
        _fail("CARRIED_POSITION_BOUNDARY_ARTIFACT_REQUIRED")

    open_states: dict[str, dict[str, Any]] = {}
    activity_days: list[dict[str, Any]] = []
    gross_vector: list[Decimal] = []
    base_cost_vector: list[Decimal] = []
    stress_cost_vector: list[Decimal] = []
    seen_event_ids: set[str] = set()
    seen_positions: set[str] = set()

    for index, date_text in enumerate(dates):
        event_day = event["days"][index]
        fill_day = fill["days"][index]
        mtm_day = mtm["days"][index]
        cost_day = cost["days"][index]
        exposure_day = exposure["days"][index]
        margin_day = margin["days"][index]
        equity = _decimal(
            mtm_day["equity_basis_cash"], "EQUITY_BASIS_INVALID", positive=True
        )
        if margin_day["equity_basis_cash"] != mtm_day["equity_basis_cash"]:
            _fail("MARGIN_MTM_EQUITY_MISMATCH")
        used_margin = _decimal(margin_day["used_margin_cash"], "USED_MARGIN_INVALID")
        free_margin = _decimal(margin_day["free_margin_cash"], "FREE_MARGIN_INVALID")
        if free_margin != equity - used_margin or free_margin < 0:
            _fail("MARGIN_LEDGER_DOES_NOT_RECONCILE")

        raw_entries = _unique_records(
            fill_day["entries"], _RAW_OPEN_FIELDS, "OPENING_FILL_ROSTER_INVALID"
        )
        entry_costs = _unique_records(
            cost_day["entries"], _OPEN_COST_FIELDS, "OPENING_COST_ROSTER_INVALID"
        )
        entry_events = _unique_records(
            event_day["entries"], _EVENT_RECORD_FIELDS, "OPEN_EVENT_ROSTER_INVALID"
        )
        if set(raw_entries) != set(entry_costs) or set(raw_entries) != set(
            entry_events
        ):
            _fail("OPEN_EVENT_FILL_COST_ROSTER_MISMATCH")

        entries: list[dict[str, Any]] = []
        for event_id in sorted(
            raw_entries, key=lambda key: raw_entries[key]["position_id"]
        ):
            if event_id in seen_event_ids:
                _fail("REPLAY_EVENT_ID_REUSED")
            seen_event_ids.add(event_id)
            raw_open = raw_entries[event_id]
            cell = by_cell_id.get(raw_open["cell_id"])
            if cell is None or cell["status"] != "WINNER":
                _fail("OPEN_EVENT_NOT_BOUND_TO_WINNER")
            if entry_events[event_id]["position_id"] != raw_open["position_id"]:
                _fail("OPEN_EVENT_POSITION_MISMATCH")
            decisions = _event_decision_hashes(
                entry_events[event_id],
                cell=raw_book_by_id[raw_open["cell_id"]],
                action="OPEN",
                current_date=date_text,
            )
            opened = _causal_open_fill(
                raw_open,
                entry_costs[event_id],
                cell=cell,
                anchor_id=anchor["anchor_id"],
                decisions=decisions,
                descriptor_hashes=descriptor_hashes,
                carried=False,
            )
            position_id = opened["position_id"]
            if position_id in seen_positions:
                _fail("POSITION_ID_REUSED")
            seen_positions.add(position_id)
            side_sign = Decimal(1) if opened["side"] == "LONG" else Decimal(-1)
            unit_scale = _decimal(
                opened["quantity"], "QUANTITY_INVALID", positive=True
            ) * _decimal(
                opened["contract_multiplier"],
                "CONTRACT_MULTIPLIER_INVALID",
                positive=True,
            )
            open_states[position_id] = {
                "side": opened["side"],
                "side_sign": side_sign,
                "unit_scale": unit_scale,
                "quantity": opened["quantity"],
                "contract_multiplier": opened["contract_multiplier"],
                "stop_price": opened["stop_price"],
                "last_mark": _decimal(
                    opened["reference_mid_price"], "OPENING_MID_INVALID", positive=True
                ),
                "pending_base_cost": _opening_cost(opened, stress=False),
                "pending_stress_cost": _opening_cost(opened, stress=True),
                "cell_id": raw_open["cell_id"],
            }
            entries.append(opened)

        mark_events = _unique_records(
            event_day["marks"], _MARK_EVENT_FIELDS, "MARK_EVENT_ROSTER_INVALID"
        )
        raw_marks = _unique_records(
            mtm_day["position_marks"], _RAW_MARK_FIELDS, "MARK_ROSTER_INVALID"
        )
        mark_costs = _unique_records(
            cost_day["marks"], _MARK_COST_FIELDS, "MARK_COST_ROSTER_INVALID"
        )
        if set(raw_marks) != set(mark_costs) or set(raw_marks) != set(mark_events):
            _fail("MARK_EVENT_MTM_COST_ROSTER_MISMATCH")
        if len(raw_marks) != len(open_states):
            _fail("DAILY_OPEN_POSITION_MARK_COVERAGE_INCOMPLETE")
        marks: list[dict[str, Any]] = []
        gross_cash = Decimal(0)
        base_cost_cash = Decimal(0)
        stress_cost_cash = Decimal(0)
        marked_positions: set[str] = set()
        for event_id in sorted(
            raw_marks, key=lambda key: raw_marks[key]["position_id"]
        ):
            if event_id in seen_event_ids:
                _fail("REPLAY_EVENT_ID_REUSED")
            seen_event_ids.add(event_id)
            raw_mark = raw_marks[event_id]
            position_id = raw_mark["position_id"]
            if mark_events[event_id]["position_id"] != position_id:
                _fail("MARK_EVENT_POSITION_MISMATCH")
            state = open_states.get(position_id)
            if state is None or position_id in marked_positions:
                _fail("MARK_NONOPEN_OR_DUPLICATE_POSITION")
            marked_positions.add(position_id)
            mark = {
                key: raw_mark[key]
                for key in (
                    "position_id",
                    "marked_at_utc",
                    "mark_price",
                    "mark_bid_price",
                    "mark_ask_price",
                )
            }
            mark.update(
                {
                    "base_financing_cash": mark_costs[event_id]["base_financing_cash"],
                    "stress_financing_cash": mark_costs[event_id][
                        "stress_financing_cash"
                    ],
                }
            )
            payload_sha = _sha(
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
            mark["market_mark_payload_sha256"] = payload_sha
            mark["market_mark_receipt_sha256"] = _sha(
                {
                    "schema": "fxpm-derived-market-mark-receipt.v1",
                    "anchor_id": anchor["anchor_id"],
                    "market_mark_payload_sha256": payload_sha,
                    "event_ledger_sha256": descriptor_hashes["EVENT_LEDGER"],
                    "mtm_ledger_sha256": descriptor_hashes["MTM_LEDGER"],
                    "exposure_ledger_sha256": descriptor_hashes["EXPOSURE_LEDGER"],
                    "margin_ledger_sha256": descriptor_hashes["MARGIN_LEDGER"],
                }
            )
            mark_price = _decimal(
                mark["mark_price"], "MARK_PRICE_INVALID", positive=True
            )
            gross_cash += (
                state["side_sign"]
                * state["unit_scale"]
                * (mark_price - state["last_mark"])
            )
            base_finance = _decimal(
                mark["base_financing_cash"], "BASE_FINANCING_INVALID"
            )
            stress_finance = _decimal(
                mark["stress_financing_cash"], "STRESS_FINANCING_INVALID"
            )
            if stress_finance < base_finance:
                _fail("STRESS_FINANCING_LESS_SEVERE")
            base_cost_cash += state["pending_base_cost"] + base_finance
            stress_cost_cash += state["pending_stress_cost"] + stress_finance
            state["pending_base_cost"] = Decimal(0)
            state["pending_stress_cost"] = Decimal(0)
            state["last_mark"] = mark_price
            marks.append(mark)

        exposure_positions = exposure_day["positions"]
        if (
            type(exposure_positions) is not list
            or len(exposure_positions) > _MAX_POSITIONS
        ):
            _fail("EXPOSURE_POSITION_ROSTER_INVALID")
        expected_exposure_ids = sorted(open_states)
        observed_exposure_ids: list[str] = []
        derived_margin = Decimal(0)
        for raw_exposure in exposure_positions:
            item = _exact_dict(
                raw_exposure, _EXPOSURE_POSITION_FIELDS, "EXPOSURE_POSITION_INVALID"
            )
            position_id = _safe_id(item["position_id"], "EXPOSURE_POSITION_INVALID")
            state = open_states.get(position_id)
            if state is None:
                _fail("EXPOSURE_NONOPEN_POSITION")
            if any(
                item[key] != state[key]
                for key in ("quantity", "contract_multiplier", "stop_price")
            ):
                _fail("EXPOSURE_POSITION_STATE_MISMATCH")
            observed_exposure_ids.append(position_id)
            derived_margin += _decimal(
                item["required_margin_cash"], "POSITION_MARGIN_INVALID"
            )
        if observed_exposure_ids != expected_exposure_ids:
            _fail("EXPOSURE_POSITION_ROSTER_MISMATCH")
        if derived_margin != used_margin:
            _fail("MARGIN_NOT_DERIVED_FROM_EXPOSURES")

        raw_exits = _unique_records(
            fill_day["exits"], _RAW_CLOSE_FIELDS, "CLOSING_FILL_ROSTER_INVALID"
        )
        exit_costs = _unique_records(
            cost_day["exits"], _OPEN_COST_FIELDS, "CLOSING_COST_ROSTER_INVALID"
        )
        exit_events = _unique_records(
            event_day["exits"], _EVENT_RECORD_FIELDS, "CLOSE_EVENT_ROSTER_INVALID"
        )
        if set(raw_exits) != set(exit_costs) or set(raw_exits) != set(exit_events):
            _fail("CLOSE_EVENT_FILL_COST_ROSTER_MISMATCH")
        exits: list[dict[str, Any]] = []
        for event_id in sorted(
            raw_exits, key=lambda key: raw_exits[key]["position_id"]
        ):
            if event_id in seen_event_ids:
                _fail("REPLAY_EVENT_ID_REUSED")
            seen_event_ids.add(event_id)
            raw_close = raw_exits[event_id]
            position_id = raw_close["position_id"]
            state = open_states.get(position_id)
            if state is None:
                _fail("CLOSE_NONOPEN_POSITION")
            cell = by_cell_id[state["cell_id"]]
            if exit_events[event_id]["position_id"] != position_id:
                _fail("CLOSE_EVENT_POSITION_MISMATCH")
            decisions = _event_decision_hashes(
                exit_events[event_id],
                cell=raw_book_by_id[state["cell_id"]],
                action="CLOSE",
                current_date=date_text,
            )
            primitive = {
                key: raw_close[key]
                for key in (
                    "position_id",
                    "closed_at_utc",
                    "broker_fill_id",
                    "reference_mid_price",
                    "reference_bid_price",
                    "reference_ask_price",
                    "base_fill_price",
                    "stress_fill_price",
                )
            }
            primitive.update(
                {
                    key: exit_costs[event_id][key]
                    for key in _OPEN_COST_FIELDS
                    if key != "event_id"
                }
            )
            payload_sha = _sha(
                {
                    "schema": "fxpm-position-closing-fill-payload-v2",
                    "anchor_id": anchor["anchor_id"],
                    "fill": primitive,
                }
            )
            closing = {
                **primitive,
                "closing_fill_payload_sha256": payload_sha,
                "closing_fill_receipt_sha256": _sha(
                    {
                        "schema": "fxpm-derived-closing-fill-receipt.v1",
                        "anchor_id": anchor["anchor_id"],
                        "closing_fill_payload_sha256": payload_sha,
                        "controller_decision_sha256": decisions[0],
                        "arbitration_decision_sha256": decisions[1],
                        "order_decision_sha256": decisions[2],
                        "raw_ledger_sha256_by_role": dict(
                            sorted(descriptor_hashes.items())
                        ),
                    }
                ),
            }
            base_exit = _closing_cost(closing, state, stress=False)
            stress_exit = _closing_cost(closing, state, stress=True)
            if stress_exit < base_exit:
                _fail("STRESS_CLOSE_COST_LESS_SEVERE")
            base_cost_cash += base_exit
            stress_cost_cash += stress_exit
            exits.append(closing)
            del open_states[position_id]

        try:
            gross_return = causal._cash_return(
                gross_cash, equity, "derived gross return"
            )
            base_cost_return = causal._cash_return(
                -base_cost_cash, equity, "derived base cost return"
            )
            stress_cost_return = causal._cash_return(
                -stress_cost_cash, equity, "derived stress cost return"
            )
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "DAILY_RETURN_DERIVATION_FAILED"
            ) from exc
        if stress_cost_return > base_cost_return:
            _fail("STRESS_DAILY_COST_LESS_SEVERE")
        gross_vector.append(gross_return)
        base_cost_vector.append(base_cost_return)
        stress_cost_vector.append(stress_cost_return)
        activity_days.append(
            {
                "date": date_text,
                "equity_basis_cash": mtm_day["equity_basis_cash"],
                "entries": entries,
                "position_marks": marks,
                "exits": exits,
            }
        )
    if open_states:
        _fail("FORWARD_REPLAY_ENDS_WITH_UNVALUED_OPEN_POSITIONS")
    return (
        {
            "schema": "fxpm-anchor-economic-position-activity-ledger-v2",
            "initial_positions": [],
            "days": activity_days,
        },
        gross_vector,
        base_cost_vector,
        stress_cost_vector,
    )


def _decimal_texts(values: Sequence[Decimal]) -> list[str]:
    return [causal._decimal_text(value) for value in values]


def _reconciliation_anchor(anchor: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
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
    )
    return {
        "anchor_id": anchor["anchor_id"],
        "evidence_anchor_sha256": _sha(anchor),
        **{key: _copy_json(anchor[key], 64 * 1024 * 1024) for key in keys},
        "events_complete": True,
        "fills_complete": True,
        "costs_complete": True,
        "exposures_complete": True,
        "open_position_valuation_complete": True,
        "calendar_complete": True,
        "net_mtm_reconciled": True,
    }


def _build_anchor_evidence(
    *,
    anchor: Mapping[str, Any],
    book_body: Mapping[str, Any],
    horizons: Sequence[Mapping[str, Any]],
    ledgers: Mapping[str, Mapping[str, Any]],
    descriptor_hashes: Mapping[str, str],
    process: Mapping[str, Any],
    forward_oos_replay_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    specialists = _specialists(
        ledgers["EVENT_LEDGER"], book_body, anchor["selection_cutoff_utc"]
    )
    cells, supports, detectors, _ = _causal_book_projection(
        anchor=anchor,
        book_body=book_body,
        horizon_bindings=horizons,
        specialists=specialists,
    )
    book_cells_by_id = {raw["cell_id"]: raw for raw in book_body["cells"]}
    activity, gross, base_cost, stress_cost = _assemble_activity(
        anchor=anchor,
        ledgers=ledgers,
        descriptor_hashes=descriptor_hashes,
        projected_cells=cells,
        book_cells=book_body["cells"],
    )
    base = [
        causal._quantized_return(g + c, "derived base net return")
        for g, c in zip(gross, base_cost, strict=True)
    ]
    stress = [
        causal._quantized_return(g + c, "derived stress net return")
        for g, c in zip(gross, stress_cost, strict=True)
    ]
    dates = _expected_dates(anchor["forward_start_date"], anchor["forward_end_date"])
    gross_text = _decimal_texts(gross)
    base_cost_text = _decimal_texts(base_cost)
    stress_cost_text = _decimal_texts(stress_cost)
    base_text = _decimal_texts(base)
    stress_text = _decimal_texts(stress)
    skeleton: dict[str, Any] = {
        "anchor_id": anchor["anchor_id"],
        "book_cells": cells,
        "activity_ledger": activity,
    }
    try:
        completed, observed_holding, ledger_ids, risk_diagnostics, activity_sha = (
            causal._validate_activity_ledger(
                skeleton, dates, gross, base_cost, stress_cost
            )
        )
    except Exception as exc:
        raise CausalEvidenceProductionError("DERIVED_ACTIVITY_REPLAY_REJECTED") from exc
    risk_policy_sha = process["policy_identities"]["risk_policy_sha256"]
    cost_policy_sha = process["policy_identities"]["cost_model_policy_sha256"]
    sample_floor, primary, sensitivity = causal._pinned_hac_bandwidths([base])
    dependence = {
        "risk_policy_sha256": risk_policy_sha,
        "inference_unit": causal.pinned_process_policy()["portfolio_inference_unit"],
        "maximum_observed_holding_calendar_days_by_timeframe": observed_holding,
        "maximum_observed_holding_calendar_days": max(observed_holding.values()),
        "execution_overlap_allowance_calendar_days": causal.pinned_process_policy()[
            "execution_overlap_allowance_calendar_days"
        ],
        "holding_overlap_hac_floor_calendar_days": causal.pinned_process_policy()[
            "holding_overlap_hac_floor_calendar_days"
        ],
        "sample_size_hac_floor_calendar_days": sample_floor,
        "primary_hac_bandwidth_calendar_days": primary,
        "sensitivity_hac_bandwidth_calendar_days": sensitivity,
    }
    book_sha = _sha(
        {
            "cells": cells,
            "train_regime_supports": supports,
            "regime_detector_records": detectors,
        }
    )
    calendar_sha = causal._calendar_hash(dates)
    vector_hashes = {
        "gross_vector_sha256": causal._vector_hash(dates, gross_text, "GROSS_MTM"),
        "base_cost_vector_sha256": causal._vector_hash(
            dates, base_cost_text, "BASE_COST_DEBIT"
        ),
        "stress_cost_vector_sha256": causal._vector_hash(
            dates, stress_cost_text, "STRESS_COST_DEBIT"
        ),
        "base_vector_sha256": causal._vector_hash(dates, base_text, "BASE"),
        "stress_vector_sha256": causal._vector_hash(dates, stress_text, "STRESS"),
    }
    completed_text = _decimal_texts(completed)
    trade_sha = _sha(
        {
            "lane": "COMPLETED_POSITION_NET_EQUITY_CONTRIBUTIONS_IN_FORWARD_WINDOW",
            "completed_position_net_equity_contributions": completed_text,
        }
    )
    forward_anchor_lineage = causal._forward_oos_anchor_lineage_sha256(
        forward_oos_replay_lineage, anchor["anchor_id"]
    )
    result: dict[str, Any] = {
        "anchor_id": anchor["anchor_id"],
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_date": anchor["forward_start_date"],
        "forward_end_date": anchor["forward_end_date"],
        "forward_oos_replay_anchor_lineage_sha256": forward_anchor_lineage,
        "historical_selection_mode": "ANCHOR_LOCAL_AS_OF_ONLY",
        "cutoff_locality": {
            key: anchor["selection_cutoff_utc"] for key in causal.CUTOFF_LOCALITY_KEYS
        },
        "component_outputs": {},
        "book_cells": cells,
        "train_regime_supports": supports,
        "regime_detector_records": detectors,
        "book_sha256": book_sha,
        "calendar_dates": dates,
        "calendar_sha256": calendar_sha,
        "daily_gross_mtm_returns": gross_text,
        "daily_base_cost_returns": base_cost_text,
        "daily_stress_cost_returns": stress_cost_text,
        "daily_base_net_mtm_returns": base_text,
        "daily_stress_net_mtm_returns": stress_text,
        **vector_hashes,
        "activity_ledger": activity,
        "activity_ledger_sha256": activity_sha,
        "completed_trade_count": len(completed_text),
        "completed_trade_outcomes": completed_text,
        "trade_vector_sha256": trade_sha,
        "dependence_diagnostics": dependence,
        "risk_diagnostics": risk_diagnostics,
        "ledger_identities": ledger_ids,
    }
    outputs = causal._expected_lineage_outputs(result)
    outputs["exact_2400_cell_book_sha256"] = book_sha
    risk_output = _sha(
        {
            "schema": "fxpm-anchor-risk-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "risk_policy_sha256": risk_policy_sha,
            "exposure_ledger_sha256": ledger_ids["exposure_ledger_sha256"],
            "open_position_ledger_sha256": ledger_ids["open_position_ledger_sha256"],
            "risk_diagnostics": risk_diagnostics,
            "dependence_diagnostics": dependence,
        }
    )
    cost_output = _sha(
        {
            "schema": "fxpm-anchor-cost-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "cost_policy_sha256": cost_policy_sha,
            "cost_ledger_sha256": ledger_ids["cost_ledger_sha256"],
            **vector_hashes,
        }
    )
    replay_output = _sha(
        {
            "schema": "fxpm-anchor-synchronized-replay-v1",
            "anchor_id": anchor["anchor_id"],
            "forward_oos_replay_anchor_lineage_sha256": forward_anchor_lineage,
            "book_sha256": book_sha,
            "calendar_sha256": calendar_sha,
            "event_ledger_sha256": ledger_ids["event_ledger_sha256"],
            "fill_ledger_sha256": ledger_ids["fill_ledger_sha256"],
            "gross_vector_sha256": vector_hashes["gross_vector_sha256"],
            "base_vector_sha256": vector_hashes["base_vector_sha256"],
            "stress_vector_sha256": vector_hashes["stress_vector_sha256"],
        }
    )
    outputs.update(
        {
            "risk_execution_sha256": risk_output,
            "cost_execution_sha256": cost_output,
            "synchronized_replay_sha256": replay_output,
            "reconciliation_input_sha256": _sha(
                {
                    "schema": "fxpm-anchor-reconciliation-input-v1",
                    "anchor_id": anchor["anchor_id"],
                    "lineage_outputs": causal._expected_lineage_outputs(result),
                    "risk_execution_sha256": risk_output,
                    "cost_execution_sha256": cost_output,
                    "synchronized_replay_sha256": replay_output,
                    "forward_oos_replay_anchor_lineage_sha256": (
                        forward_anchor_lineage
                    ),
                    "ledger_identities": ledger_ids,
                }
            ),
        }
    )
    result["component_outputs"] = outputs
    del book_cells_by_id
    _canonical_bytes(result, 64 * 1024 * 1024)
    return result


@dataclass(frozen=True, slots=True)
class ForwardNativeProductRequest:
    """Exact winner-derived native-product roster issued after Book close."""

    _body_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class ForwardNativeProductResolution:
    """Authentic sealed products returned before any Forward frame is opened."""

    execution_products_by_anchor: Mapping[str, Mapping[str, Any]]
    valuation_products_by_anchor: Mapping[str, Mapping[str, Any]]


class BoundedCausalEvidenceFactory:
    """One-shot construction of the causal producer after historical close.

    The historical precommit and run receipt do not exist when pipeline inputs
    are initially assembled.  Holding a preconstructed
    :class:`BoundedCausalEvidenceProducer` at that point would therefore
    require either a duplicate historical run or caller-authored stand-ins.
    This factory snapshots only pre-evidence inputs.  The coordinator invokes
    it exactly once after it has produced and verified the genuine historical
    run; the factory then freezes the forward replay precommit, produces the
    issuer-authenticated forward product, and returns the only supported
    causal producer.
    """

    __slots__ = (
        "context",
        "_capital_bootstrap_identity_sha256",
        "_capital_phase_binding",
        "_causal_precommit_json",
        "_config",
        "_execution_rows",
        "_lock",
        "_product_resolver",
        "_used",
        "_valuation_rows",
    )

    def __init__(
        self,
        *,
        context: authority_spool.GenerationSpoolContext,
        causal_precommit_payload: Mapping[str, Any],
        capital_phase_binding: capital_bootstrap.CapitalForwardPhaseBinding,
        execution_products_by_anchor: Mapping[str, Mapping[str, Any]] | None = None,
        valuation_products_by_anchor: Mapping[str, Mapping[str, Any]] | None = None,
        forward_product_resolver: Callable[
            [ForwardNativeProductRequest], ForwardNativeProductResolution
        ]
        | None = None,
        config: Any,
    ) -> None:
        if type(context) is not authority_spool.GenerationSpoolContext:
            _fail("CAUSAL_FACTORY_GENERATION_CONTEXT_INVALID")
        payload = _copy_json(causal_precommit_payload, 16 * 1024 * 1024)
        process = payload.get("process_definition") if type(payload) is dict else None
        plans = process.get("anchor_plan") if type(process) is dict else None
        if (
            type(plans) is not list
            or not plans
            or any(type(plan) is not dict for plan in plans)
        ):
            _fail("CAUSAL_FACTORY_ANCHOR_PLAN_INVALID")
        anchor_ids = tuple(
            _safe_id(plan.get("anchor_id"), "CAUSAL_FACTORY_ANCHOR_ID_INVALID")
            for plan in plans
        )
        if len(anchor_ids) != len(set(anchor_ids)):
            _fail("CAUSAL_FACTORY_ANCHOR_ROSTER_INVALID")

        def freeze_products(
            value: Mapping[str, Mapping[str, Any]], code: str
        ) -> tuple[tuple[str, tuple[tuple[str, Any], ...]], ...]:
            if type(value) is not dict or set(value) != set(anchor_ids):
                _fail(code)
            rows: list[tuple[str, tuple[tuple[str, Any], ...]]] = []
            for anchor_id in anchor_ids:
                products = value[anchor_id]
                if type(products) is not dict or any(
                    type(key) is not str or not key for key in products
                ):
                    _fail(code)
                rows.append(
                    (
                        anchor_id,
                        tuple((key, products[key]) for key in sorted(products)),
                    )
                )
            return tuple(rows)

        self.context = context
        self._causal_precommit_json = _canonical_bytes(
            payload, 16 * 1024 * 1024
        )
        try:
            capital_binding_body = (
                capital_bootstrap.verify_capital_forward_phase_binding(
                    context, capital_phase_binding
                )
            )
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "CAUSAL_FACTORY_CAPITAL_PHASE_BINDING_INVALID"
            ) from exc
        identity = capital_binding_body.get("bootstrap_identity")
        if (
            type(identity) is not dict
            or identity.get("process_precommitment_sha256")
            != causal.canonical_sha256(payload)
            or identity.get("process_definition_sha256")
            != payload.get("process_definition_sha256")
        ):
            _fail("CAUSAL_FACTORY_CAPITAL_PROCESS_BINDING_MISMATCH")
        self._capital_bootstrap_identity_sha256 = _hex64(
            capital_binding_body.get("bootstrap_identity_sha256"),
            "CAUSAL_FACTORY_CAPITAL_BOOTSTRAP_IDENTITY_INVALID",
        )
        self._capital_phase_binding = capital_phase_binding
        static_products = (
            execution_products_by_anchor is not None
            or valuation_products_by_anchor is not None
        )
        if static_products == (forward_product_resolver is not None):
            _fail("CAUSAL_FACTORY_PRODUCT_SOURCE_AMBIGUOUS")
        if static_products:
            if (
                execution_products_by_anchor is None
                or valuation_products_by_anchor is None
            ):
                _fail("CAUSAL_FACTORY_PRODUCT_ROSTERS_INCOMPLETE")
            self._execution_rows = freeze_products(
                execution_products_by_anchor,
                "CAUSAL_FACTORY_EXECUTION_PRODUCT_ROSTER_INVALID",
            )
            self._valuation_rows = freeze_products(
                valuation_products_by_anchor,
                "CAUSAL_FACTORY_VALUATION_PRODUCT_ROSTER_INVALID",
            )
            self._product_resolver = None
        else:
            if not callable(forward_product_resolver):
                _fail("CAUSAL_FACTORY_PRODUCT_RESOLVER_INVALID")
            self._execution_rows = ()
            self._valuation_rows = ()
            self._product_resolver = forward_product_resolver
        self._config = config
        self._lock = threading.Lock()
        self._used = False

    @property
    def causal_precommit_payload_sha256(self) -> str:
        return hashlib.sha256(self._causal_precommit_json).hexdigest()

    @property
    def capital_bootstrap_identity_sha256(self) -> str:
        """Return the exact bootstrap that licensed the historical replay phases."""

        return self._capital_bootstrap_identity_sha256

    @staticmethod
    def _products(
        rows: tuple[tuple[str, tuple[tuple[str, Any], ...]], ...],
    ) -> dict[str, dict[str, Any]]:
        return {
            anchor_id: {key: product for key, product in products}
            for anchor_id, products in rows
        }

    def _resolve_forward_products(
        self,
        *,
        forward: Any,
        historical_precommit: historical.HistoricalAnchorPrecommit,
        historical_run_receipt: historical.HistoricalAnchorRunReceipt,
        payload: Mapping[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        resolver = self._product_resolver
        if resolver is None:
            return (
                self._products(self._execution_rows),
                self._products(self._valuation_rows),
            )
        try:
            _, plans, books, _ = forward._reopen_books(  # noqa: SLF001
                self.context,
                historical_precommit,
                historical_run_receipt,
            )
            required = forward._required_forward_product_rosters(  # noqa: SLF001
                plans,
                books,
            )
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "CAUSAL_FACTORY_FORWARD_REQUIRED_ROSTER_FAILED"
            ) from exc
        anchor_rows = []
        for plan in plans:
            anchor_id = plan["anchor_id"]
            forward_end_exclusive_utc = (
                _day(plan["forward_end_date"], "CAUSAL_FACTORY_FORWARD_END_INVALID")
                + timedelta(days=1)
            ).isoformat() + "T00:00:00Z"
            anchor_rows.append(
                {
                    "anchor_id": anchor_id,
                    "forward_end_exclusive_utc": forward_end_exclusive_utc,
                    "execution_product_keys": list(
                        required[anchor_id]["execution_product_keys"]
                    ),
                    "valuation_m5_product_keys": list(
                        required[anchor_id]["valuation_m5_product_keys"]
                    ),
                }
            )
        request_body = {
            "schema_version": FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA,
            "generation_id": self.context.generation_id,
            "generation_context_sha256": self.context.context_sha256,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "historical_precommit_receipt_sha256": (
                historical_precommit.receipt_sha256
            ),
            "historical_run_receipt_sha256": historical_run_receipt.receipt_sha256,
            "causal_precommit_payload_sha256": causal.canonical_sha256(payload),
            "anchor_count": len(anchor_rows),
            "anchors": anchor_rows,
            "required_roster_sha256": _sha(anchor_rows),
            "native_products_must_be_sealed_before_forward_frame_access": True,
            "resolver_may_choose_winners_or_product_keys": False,
            "confers_causal_current_refit_capital_publication_or_trading_authority": (
                False
            ),
        }
        request = ForwardNativeProductRequest(
            _canonical_bytes(request_body, 4 * 1024 * 1024)
        )
        try:
            resolution = resolver(request)
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "CAUSAL_FACTORY_FORWARD_PRODUCT_RESOLVER_FAILED"
            ) from exc
        if type(resolution) is not ForwardNativeProductResolution:
            _fail("CAUSAL_FACTORY_FORWARD_PRODUCT_RESOLUTION_INVALID")
        execution = resolution.execution_products_by_anchor
        valuation = resolution.valuation_products_by_anchor
        anchor_ids = [row["anchor_id"] for row in anchor_rows]
        if (
            type(execution) is not dict
            or type(valuation) is not dict
            or list(execution) != anchor_ids
            or list(valuation) != anchor_ids
        ):
            _fail("CAUSAL_FACTORY_FORWARD_PRODUCT_ANCHOR_ROSTER_INVALID")
        owned_execution: dict[str, dict[str, Any]] = {}
        owned_valuation: dict[str, dict[str, Any]] = {}
        for row in anchor_rows:
            anchor_id = row["anchor_id"]
            observed_execution = execution[anchor_id]
            observed_valuation = valuation[anchor_id]
            if (
                type(observed_execution) is not dict
                or type(observed_valuation) is not dict
                or list(observed_execution) != row["execution_product_keys"]
                or list(observed_valuation) != row["valuation_m5_product_keys"]
            ):
                _fail("CAUSAL_FACTORY_FORWARD_PRODUCT_KEY_ROSTER_INVALID")
            owned_execution[anchor_id] = dict(observed_execution)
            owned_valuation[anchor_id] = dict(observed_valuation)
        return owned_execution, owned_valuation

    def build_after_historical_run(
        self,
        *,
        historical_precommit: historical.HistoricalAnchorPrecommit,
        historical_run_receipt: historical.HistoricalAnchorRunReceipt,
    ) -> "BoundedCausalEvidenceProducer":
        with self._lock:
            if self._used:
                _fail("CAUSAL_EVIDENCE_FACTORY_ALREADY_CONSUMED")
            # Consume before any forward call.  A partial evidence access is
            # never retried inside the same frozen generation.
            self._used = True
        try:
            import pm_forward_oos_replay_producer as forward

            payload = json.loads(self._causal_precommit_json.decode("utf-8"))
            execution, valuation = self._resolve_forward_products(
                forward=forward,
                historical_precommit=historical_precommit,
                historical_run_receipt=historical_run_receipt,
                payload=payload,
            )
            replay_precommit = forward.precommit_forward_oos_replay_products(
                context=self.context,
                historical_precommit=historical_precommit,
                historical_run_receipt=historical_run_receipt,
                causal_precommit_payload=payload,
                execution_products_by_anchor=execution,
                valuation_products_by_anchor=valuation,
                capital_phase_binding=self._capital_phase_binding,
                config=self._config,
            )
            product = forward.produce_forward_oos_replay_artifacts(
                context=self.context,
                historical_precommit=historical_precommit,
                historical_run_receipt=historical_run_receipt,
                causal_precommit_payload=payload,
                replay_precommit=replay_precommit,
                native_products_by_anchor=execution,
                valuation_products_by_anchor=valuation,
                config=self._config,
            )
            forward.verify_forward_oos_replay_product(self.context, product)
            return BoundedCausalEvidenceProducer(
                context=self.context,
                historical_precommit=historical_precommit,
                historical_run_receipt=historical_run_receipt,
                causal_precommit_payload=payload,
                forward_replay_product=product,
            )
        except CausalEvidenceProductionError:
            raise
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "CAUSAL_FORWARD_PRODUCT_CONSTRUCTION_FAILED"
            ) from exc


@dataclass(frozen=True, slots=True)
class BoundedCausalEvidenceProducer:
    """Concrete callback accepted by ``HistoricalAuthorityInputs``."""

    context: authority_spool.GenerationSpoolContext
    historical_precommit: historical.HistoricalAnchorPrecommit
    historical_run_receipt: historical.HistoricalAnchorRunReceipt
    causal_precommit_payload: Mapping[str, Any]
    forward_replay_product: Any

    def __call__(
        self, request: coordinator.HistoricalEvidenceRequest
    ) -> coordinator.CausalEvidenceProduct:
        try:
            request_body = _request_body(request)
            precommit_payload = _copy_json(
                self.causal_precommit_payload, 16 * 1024 * 1024
            )
            precommit_sha = causal.canonical_sha256(precommit_payload)
            (
                forward_lineage,
                replay_artifacts_by_anchor,
                capital_candidates_by_phase,
                capital_lineages_by_phase,
            ) = (
                _forward_oos_replay_lineage(
                    context=self.context,
                    product=self.forward_replay_product,
                    historical_precommit=self.historical_precommit,
                    historical_run_receipt=self.historical_run_receipt,
                    causal_precommit_payload=precommit_payload,
                )
            )
            artifacts = _reopen_historical_artifacts(
                self.context,
                self.historical_precommit,
                self.historical_run_receipt,
                request_body,
                precommit_sha,
            )
            process = precommit_payload.get("process_definition")
            plans = process.get("anchor_plan") if type(process) is dict else None
            if type(plans) is not list:
                _fail("CAUSAL_PRECOMMIT_ANCHOR_PLAN_INVALID")
            anchor_ids = [item.get("anchor_id") for item in plans]
            if (
                type(replay_artifacts_by_anchor) is not dict
                or list(replay_artifacts_by_anchor) != anchor_ids
            ):
                _fail("REPLAY_ANCHOR_ROSTER_MISMATCH")
            historical_plans = {
                item["anchor_id"]: item
                for item in self.historical_precommit.to_dict()["anchors"]
            }
            for plan in plans:
                observed = historical_plans.get(plan["anchor_id"])
                if observed is None or any(
                    observed[key] != plan[key]
                    for key in (
                        "selection_cutoff_utc",
                        "forward_start_date",
                        "forward_end_date",
                        "purge_calendar_days",
                    )
                ):
                    _fail("ANCHOR_GEOMETRY_MISMATCH")

            evidence_anchors: list[dict[str, Any]] = []
            clocks: tuple[str, str, str, str] | None = None
            raw_total = 0
            for plan in plans:
                anchor_id = plan["anchor_id"]
                descriptor_set = replay_artifacts_by_anchor[anchor_id]
                raw_total += sum(
                    _descriptor(value, f"{role}_DESCRIPTOR_INVALID")["size_bytes"]
                    for role, value in descriptor_set.descriptors()
                )
                if raw_total > _MAX_RAW_BYTES_TOTAL:
                    _fail("REPLAY_TOTAL_BYTE_BOUND_EXCEEDED")
                ledgers, descriptor_hashes = _load_replay_ledgers(
                    self.context, descriptor_set, plan
                )
                event = ledgers["EVENT_LEDGER"]
                anchor_clocks = tuple(
                    event[key]
                    for key in (
                        "evidence_access_started_at_utc",
                        "execution_started_at_utc",
                        "execution_completed_at_utc",
                        "reconciled_at_utc",
                    )
                )
                for key, value in zip(
                    (
                        "EVIDENCE_ACCESS_TIME_INVALID",
                        "EXECUTION_START_TIME_INVALID",
                        "EXECUTION_COMPLETION_TIME_INVALID",
                        "RECONCILIATION_TIME_INVALID",
                    ),
                    anchor_clocks,
                    strict=True,
                ):
                    _utc(value, key)
                if clocks is None:
                    clocks = anchor_clocks
                elif clocks != anchor_clocks:
                    _fail("REPLAY_PROCESS_CLOCKS_NOT_SYNCHRONIZED")
                evidence_anchors.append(
                    _build_anchor_evidence(
                        anchor=plan,
                        book_body=artifacts.book_by_anchor[anchor_id],
                        horizons=artifacts.horizon_by_anchor[anchor_id],
                        ledgers=ledgers,
                        descriptor_hashes=descriptor_hashes,
                        process=process,
                        forward_oos_replay_lineage=forward_lineage,
                    )
                )
                del ledgers
            if clocks is None:
                _fail("REPLAY_ANCHOR_ROSTER_EMPTY")
            evidence = {
                "schema": causal.EVIDENCE_SCHEMA,
                "process_id": precommit_payload["process_id"],
                "precommit_payload_sha256": precommit_sha,
                "process_definition_sha256": precommit_payload[
                    "process_definition_sha256"
                ],
                "evidence_access_started_at_utc": clocks[0],
                "execution_started_at_utc": clocks[1],
                "execution_completed_at_utc": clocks[2],
                "forward_oos_replay_lineage": forward_lineage,
                "anchors": evidence_anchors,
            }
            reconciliation = {
                "schema": causal.RECONCILIATION_SCHEMA,
                "process_id": precommit_payload["process_id"],
                "precommit_payload_sha256": precommit_sha,
                "evidence_payload_sha256": _sha(evidence),
                "process_definition_sha256": precommit_payload[
                    "process_definition_sha256"
                ],
                "reconciled_at_utc": clocks[3],
                "forward_oos_replay_lineage": forward_lineage,
                "anchors": [
                    _reconciliation_anchor(value) for value in evidence_anchors
                ],
            }
            decision = causal.evaluate_historical_process(
                precommit_payload, evidence, reconciliation
            )
            if (
                causal.verify_historical_process_decision(
                    decision, precommit_payload, evidence, reconciliation
                )
                is not True
            ):
                _fail("CAUSAL_DECISION_REVERIFY_FAILED")
            latest_id = plans[-1]["anchor_id"]
            return coordinator.CausalEvidenceProduct(
                evidence_payload=evidence,
                reconciliation_payload=reconciliation,
                historical_book_decision=artifacts.book_by_anchor[latest_id],
                capital_replay_candidates_by_phase=(
                    capital_candidates_by_phase
                ),
                capital_lineage_by_phase=capital_lineages_by_phase,
            )
        except CausalEvidenceProductionError:
            raise
        except causal.ProcessVerificationError as exc:
            raise CausalEvidenceProductionError("CAUSAL_DECISION_V8_REJECTED") from exc
        except (DecimalException, ArithmeticError) as exc:
            raise CausalEvidenceProductionError("ECONOMIC_REPLAY_FAILED") from exc
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "CAUSAL_EVIDENCE_PRODUCTION_FAILED"
            ) from exc


__all__ = [
    "ARBITRATION_EVENT_SCHEMA",
    "AnchorReplayArtifactDescriptors",
    "BoundedCausalEvidenceFactory",
    "BoundedCausalEvidenceProducer",
    "COST_LEDGER_SCHEMA",
    "CONTROLLER_EVENT_SCHEMA",
    "CausalEvidenceProductionError",
    "EVENT_LEDGER_SCHEMA",
    "EXPOSURE_LEDGER_SCHEMA",
    "FILL_LEDGER_SCHEMA",
    "FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA",
    "ForwardNativeProductRequest",
    "ForwardNativeProductResolution",
    "FrozenCausalProcessPrecommit",
    "MARGIN_LEDGER_SCHEMA",
    "MTM_LEDGER_SCHEMA",
    "ORDER_EVENT_SCHEMA",
    "SPECIALIST_SCHEMA",
    "freeze_causal_process_precommit_v8",
]
