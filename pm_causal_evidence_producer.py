"""Bounded production of decision-v10 historical causal evidence.

The producer in this module is deliberately an adapter, not an authority.  It
reopens an already verified historical-anchor orchestration run and a sealed,
standalone-verified forward-OOS replay product.  Portfolio returns,
cost lanes, activity, specialist Book projections, and reconciliation are all
derived from those bytes.  Callers cannot supply return vectors or evidence
hashes.

The resulting payloads still have to pass :mod:`pm_causal_process`'s released
decision-v10 evaluator and verifier before a ``CausalEvidenceProduct`` is
returned.  No current-refit, capital, publication, or trading authority is
created here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, DecimalException
import hashlib
import json
import math
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
FILL_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-fill-ledger.v2"
MTM_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-daily-mtm-ledger.v1"
COST_LEDGER_SCHEMA: Final = "fxpm.anchor-raw-cost-ledger.v3"
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
_DAY_COST_FIELDS: Final = frozenset({"date", "marks"})
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
        "p75_fill_price",
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
        "p75_fill_price",
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
_MARK_COST_FIELDS: Final = frozenset(
    {"event_id", "base_financing_cash", "p75_financing_cash"}
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
    """Verify the sealed forward product and derive its persistent v10 lineage."""

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
        or [
            row.get("anchor_id") if type(row) is dict else None
            for row in product_identities
        ]
        != anchor_ids
        or receipt.get("ordered_anchor_ids") != anchor_ids
    ):
        _fail("FORWARD_OOS_ANCHOR_ROSTER_MISMATCH")
    try:
        code_identity = forward._validate_replay_code_identity(receipt["code_identity"])
    except Exception as exc:
        raise CausalEvidenceProductionError(
            "FORWARD_OOS_CODE_IDENTITY_INVALID"
        ) from exc
    causal_component = code_identity.get(
        "causal_synchronized_replay_component_identity_sha256"
    )
    causal_manifest = code_identity.get("causal_synchronized_replay_component_manifest")
    producer_identity = code_identity.get(
        "forward_oos_replay_producer_source_identity_sha256"
    )
    producer_manifest = code_identity.get("forward_oos_replay_producer_source_manifest")
    process = causal_precommit_payload["process_definition"]
    components = process.get("component_identities")
    if (
        type(components) is not dict
        or causal_component != components.get("synchronized_replay_code_sha256")
        or _sha(causal_manifest) != causal_component
        or _sha(producer_manifest) != producer_identity
        or code_identity.get("forward_producer_in_causal_component_manifest")
        is not True
        or code_identity.get("causal_binding_followup_required") is not True
    ):
        _fail("FORWARD_OOS_CODE_IDENTITY_BINDING_MISMATCH")
    causal_files = (
        causal_manifest.get("files") if type(causal_manifest) is dict else None
    )
    producer_files = (
        producer_manifest.get("files") if type(producer_manifest) is dict else None
    )
    if (
        type(causal_files) is not list
        or type(producer_files) is not list
        or len(producer_files) != 1
        or producer_files[0] not in causal_files
        or producer_files[0].get("relative_path") != "pm_forward_oos_replay_producer.py"
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
        _forward_utc(
            forward_precommit["created_at_utc"], "FORWARD_PRECOMMIT_TIME_INVALID"
        ),
        _forward_utc(
            receipt["evidence_access_started_at_utc"], "FORWARD_ACCESS_TIME_INVALID"
        ),
        _forward_utc(
            receipt["execution_started_at_utc"], "FORWARD_EXECUTION_START_INVALID"
        ),
        _forward_utc(
            receipt["execution_completed_at_utc"], "FORWARD_EXECUTION_END_INVALID"
        ),
        _forward_utc(
            receipt["reconciled_at_utc"], "FORWARD_RECONCILIATION_TIME_INVALID"
        ),
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
            role: _descriptor(row[role], "FORWARD_OOS_LEDGER_DESCRIPTOR_INVALID")
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
        "evidence_access_started_at_utc": receipt["evidence_access_started_at_utc"],
        "execution_started_at_utc": receipt["execution_started_at_utc"],
        "execution_completed_at_utc": receipt["execution_completed_at_utc"],
        "reconciled_at_utc": receipt["reconciled_at_utc"],
        "ordered_anchor_ids": anchor_ids,
        "ordered_anchor_descriptor_roster_sha256": _sha(normalized_rows),
        "anchor_descriptor_roster_sha256_by_id": per_anchor_sha,
        "anchor_ledger_descriptors_by_id": lineage_descriptor_rows,
        "ordered_anchor_production_metadata_roster_sha256": _sha(metadata_rows),
        "anchor_production_metadata_sha256_by_id": metadata_sha,
        "anchor_production_metadata_by_id": _copy_json(metadata, 16 * 1024 * 1024),
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
    """Immutable canonical decision-v10 precommitment and identity manifest."""

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


def freeze_causal_process_precommit_v10(
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
        # Decision-v10 owns ordering, purge, overlap, and 4-16 bounds.
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
            "schema": "fxpm-frozen-historical-institution-process-v10",
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
        raise CausalEvidenceProductionError("PRECOMMIT_V10_REJECTED") from exc
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


@dataclass(frozen=True, slots=True)
class FrozenTierRawAccountsArtifact:
    """Content-addressed raw R&D account transport; grants no authority."""

    descriptor: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class FrozenTierRawAccountsProjection:
    cell_id: str
    account_tiers: tuple[str, ...]
    descriptor_sha256: str
    accounts: Mapping[str, Any]
    qualification_authority: bool = False

    def require_interval_qualification(self) -> None:
        _fail("FROZEN_TIER_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED")


@dataclass(frozen=True, slots=True)
class DeclaredCellRawAccountArtifact:
    """Descriptor of one declared-cell raw account; grants no authority."""

    descriptor: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DeclaredCellRawAccountProjection:
    cell_id: str
    descriptor_sha256: str
    progress_status: str
    reason_code: str | None
    slot_schedule: tuple[Mapping[str, Any], ...]
    owner_records: tuple[Mapping[str, Any], ...]
    account: Mapping[str, Any] | None
    qualification_authority: bool = False
    capital_authority: bool = False
    live_authority: bool = False
    order_authority: bool = False
    _verified_body_json: bytes = b"{}"

    @property
    def verified_body(self) -> dict[str, Any]:
        """Return a fresh view of the exact verified, nonauthorizing body."""
        return json.loads(self._verified_body_json.decode("utf-8"))

    def _verified_result_value(self, name: str) -> Any:
        body = self.verified_body
        if body.get("progress_status") != "COMPLETE" or body.get("account") is None:
            return None
        return body["account"]["engine_result"][name]

    @property
    def net_account_profit(self) -> Any:
        return self._verified_result_value("net_account_profit")

    @property
    def final_equity(self) -> Any:
        return self._verified_result_value("final_equity")

    @property
    def terminal(self) -> Any:
        return self._verified_result_value("terminal")

    @property
    def first_depletion_bar(self) -> Any:
        return self._verified_result_value("first_depletion_bar")

    @property
    def max_drawdown_pct(self) -> Any:
        return self._verified_result_value("max_drawdown_pct")

    @property
    def admissible(self) -> bool:
        """Raw account completeness, never interval qualification."""
        return self.progress_status == "COMPLETE" and self.account is not None

    def require_interval_qualification(self) -> None:
        _fail("DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED")


def _reopen_frozen_transport_source(
    context, identity, config, *, pin_consumed_activation=False,
):
    """Use the retained native verifier so source and financing attrs survive."""
    import pm_native_prefix_production as native
    import pm_forward_oos_replay_producer as forward

    try:
        pinned = ({"pinned_activation_manifest_sha256": identity["activation_manifest_sha256"]}
                  if pin_consumed_activation else {})
        product = native.verify_activated_native_prefix(
            data_dir=Path(config.data_dir), context=context,
            index_descriptor=identity["index_descriptor"], expected_index_sha256=identity["index_sha256"],
            expected_symbol=identity["symbol"], expected_timeframe=identity["timeframe"],
            expected_cutoff_utc=identity["cutoff_utc"],
            expected_activation_manifest_sha256=identity["activation_manifest_sha256"],
            production_contract_required=not bool(config.research_mode), require_current_activation=False,
            **pinned)
        observed = forward._native_product_identity(product, expected_symbol=identity["symbol"],
            expected_timeframe=identity["timeframe"], expected_cutoff_utc=identity["cutoff_utc"], role=identity["role"])
        if observed != identity:
            _fail("FROZEN_TIER_NATIVE_SOURCE_IDENTITY_CHANGED")
        return native.open_verified_prefix_frame(product), product
    except CausalEvidenceProductionError:
        raise
    except (native.NativePrefixError, forward.ForwardOOSReplayProductionError, KeyError, TypeError, ValueError) as exc:
        raise CausalEvidenceProductionError("FROZEN_TIER_NATIVE_SOURCE_REOPEN_FAILED") from exc


def _frozen_spread_read_model(derived: Mapping[str, Any], schema: str) -> dict[str, Any]:
    """Reconstruct recorded metadata; v1 is an archive format, never execution."""
    import pm_forward_oos_replay_producer as forward

    result = dict(derived)
    if schema == forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA:
        result.pop("entry_admission_schema", None)
        result["entry_admission_policy"] = "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"
    elif schema not in (forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                         forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA):
        _fail("FROZEN_TIER_RAW_SCHEMA_UNSUPPORTED")
    return result


def _verify_frozen_execution_spread_model(
    derived: Mapping[str, Any], stored: Mapping[str, Any], schema: str,
    *, features: Any, tier: Any, observations: Any,
) -> dict[str, Any]:
    """Rebuild execution bindings without changing retained/archive formats."""
    import pandas as pd
    import pm_forward_oos_replay_producer as forward

    result = dict(derived)
    if schema == forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA:
        _, binding = forward.Backtester._entry_spread_admission_inputs(features, tier, observations)
        result.update(binding)
    else:
        result["entry_observations_sha256"] = hashlib.sha256(
            pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest()
    result = _frozen_spread_read_model(result, schema)
    if forward._raw_json(result) != stored:
        _fail("FROZEN_TIER_EXECUTION_SPREAD_MODEL_CHANGED")
    return result


def _verify_frozen_spread_admission(*, schema: str, account: Mapping[str, Any], observations: Any,
                                  tier: Any, opportunities: Any = None) -> None:
    import numpy as np
    import pm_forward_oos_replay_producer as forward

    archived = schema == forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
    if not archived and schema not in (forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                                       forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA):
        _fail("FROZEN_TIER_RAW_SCHEMA_UNSUPPORTED")
    admission, model = account["engine_spread_admission"], account["engine_spread_model"]
    fields = {"signal_opportunities", "known_cap_pass_opportunities", "known_cap_refused_opportunities",
              "entry_observations_sha256", "entry_admission_mask_sha256",
              "input_provenance_authenticated", "qualification_authority"}
    if archived:
        missing_count, executed_count = "untested_missing_observation_opportunities", "untested_executed_legs"
        if (model.get("entry_admission_policy") != "EXACT_PRIOR_FINE_CLOSE_CAP_MISSING_UNTESTED"
                or "entry_admission_schema" in model):
            _fail("FROZEN_TIER_ADMISSION_VERSION_MISMATCH")
    else:
        fields |= {"schema_version", "entry_admission_policy"}
        missing_count, executed_count = "missing_refused_opportunities", "missing_executed_legs"
        if (model.get("entry_admission_policy") != forward.spread_profile.ADMISSION_POLICY
                or model.get("entry_admission_schema") != forward.spread_profile.ADMISSION_SCHEMA
                or admission.get("entry_admission_policy") != forward.spread_profile.ADMISSION_POLICY
                or admission.get("schema_version") != forward.spread_profile.ADMISSION_SCHEMA):
            _fail("FROZEN_TIER_ADMISSION_VERSION_MISMATCH")
    fields |= {missing_count, executed_count}
    _exact_dict(admission, fields, "FROZEN_TIER_ADMISSION_VERSION_MISMATCH")
    states = np.array([forward.spread_profile.spread_admission(tier, value) for value in observations])
    allowed = states != "REFUSE" if archived else states == "ADMIT"
    observations_sha = hashlib.sha256(
        forward.pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest()
    if (admission["entry_observations_sha256"] != observations_sha
            or admission["entry_admission_mask_sha256"] != hashlib.sha256(allowed.astype(np.uint8).tobytes()).hexdigest()):
        _fail("FROZEN_TIER_ENTRY_ADMISSION_BINDING_CHANGED")
    counts = ("signal_opportunities", "known_cap_pass_opportunities", "known_cap_refused_opportunities", missing_count, executed_count)
    scored = states[max(1, account["warmup_bars"]):]
    if (any(type(admission[name]) is not int or admission[name] < 0 for name in counts)
            or admission["signal_opportunities"] != sum(admission[name] for name in counts[1:4])
            or admission["known_cap_pass_opportunities"] > int((scored == "ADMIT").sum())
            or admission["known_cap_refused_opportunities"] > int((scored == "REFUSE").sum())
            or admission[missing_count] > int((scored == "MISSING").sum())
            or admission[executed_count] != 0):
        _fail("FROZEN_TIER_ADMISSION_COUNTS_INVALID")
    if not archived:
        if (not isinstance(opportunities, np.ndarray) or opportunities.dtype != np.dtype(bool)
                or opportunities.shape != states.shape):
            _fail("FROZEN_TIER_ORIGINAL_SIGNAL_OPPORTUNITIES_UNAVAILABLE")
        expected_counts = {"signal_opportunities": int(opportunities.sum()),
            "known_cap_pass_opportunities": int((opportunities & (states == "ADMIT")).sum()),
            "known_cap_refused_opportunities": int((opportunities & (states == "REFUSE")).sum()),
            missing_count: int((opportunities & (states == "MISSING")).sum())}
        if any(admission[name] != value for name, value in expected_counts.items()):
            _fail("FROZEN_TIER_ADMISSION_COUNTS_INVALID")
    if archived and bool(observations.iloc[account["warmup_bars"]:].isna().any()):
        _fail("FROZEN_TIER_PRIOR_FINE_ROW_UNAVAILABLE")


def _materialize_tier_source_rows(account: Mapping[str, Any], expected_retained: Any,
                                  entry_half: Any, intrabar_half: Any,
                                  spread_meta: Mapping[str, Any]) -> dict[str, Any]:
    import pm_forward_oos_replay_producer as forward

    fields = {"symbol", "timeframe", "cell_id", "tier", "initial_cash", "warmup_bars",
              "instrument_spec", "position_namespace", "trades", "position_events",
              "engine_equity", "engine_equity_growth", "engine_spread_admission",
              "engine_spread_model", "execution_cost_timing_contract",
              "numerical_execution_cost_attestation", "financing_model", "derived", "valuation_source"}
    _exact_dict(account, fields, "FROZEN_TIER_ACCOUNT_FIELDS_INVALID")
    curve = account["engine_equity"]
    if type(curve) is not list or not curve or len(curve) != len(expected_retained):
        _fail("FROZEN_TIER_ENGINE_EQUITY_AXIS_MISSING")
    for value in curve:
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
        if not valid:
            _fail("FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE")
    result = {key: value for key, value in account.items() if key != "engine_equity"}
    result["source_rows"] = forward._tier_source_rows(
        expected_retained, entry_half, intrabar_half, spread_meta, curve)
    return result


def _read_frozen_tier_raw_accounts(
    context: authority_spool.GenerationSpoolContext,
    artifact: FrozenTierRawAccountsArtifact,
    anchor: Mapping[str, Any],
) -> FrozenTierRawAccountsProjection:
    import numpy as np
    import pandas as pd
    import pm_forward_oos_replay_producer as forward

    try:
        if type(artifact) is not FrozenTierRawAccountsArtifact:
            _fail("FROZEN_TIER_RAW_DESCRIPTOR_INVALID")
        raw_descriptor = _descriptor(artifact.descriptor, "FROZEN_TIER_RAW_DESCRIPTOR_INVALID")
        body = _read_descriptor_json(context, raw_descriptor, code="FROZEN_TIER_RAW_ACCOUNTS", maximum=_MAX_RAW_ARTIFACT_BYTES)
        fields = {"schema_version", "generation_id", "anchor", "capital_phase", "capital_window", "genesis", "full_book_descriptor",
                  "original_precommit_anchor", "cell_projection", "owner_provenance", "execution_source",
                  "execution_spec_receipt", "spread_source", "spread_source_execution_spec_receipt",
                  "source_observations", "spread_source_rows", "frozen_profile_descriptor", "accounts", "qualification_authority", "coverage_contract", "selection_policy",
                  "owner_source", "original_global_precommit_descriptor", "pipeline_config_binding", "deployment_source_descriptors",
                  "precommit_binding_scope", "release_authority_status"}
        schema = body.get("schema_version") if type(body) is dict else None
        if schema not in (forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                          forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                          forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA):
            _fail("FROZEN_TIER_RAW_SCHEMA_UNSUPPORTED")
        compact = schema == forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA
        if compact:
            fields -= {"source_observations", "spread_source_rows"}
        _exact_dict(body, fields, "FROZEN_TIER_RAW_FIELDS_INVALID")
        if (body["generation_id"] != context.generation_id or body["anchor"] != anchor
                or body["qualification_authority"] is not False
                or body["owner_provenance"].get("qualification_authority") is not False
                or body["precommit_binding_scope"] != "REOPENED_INPUT_DESCRIPTOR_IDENTITY_NOT_ISSUED_RELEASE_AUTHORITY"
                or body["release_authority_status"] != "UNAVAILABLE_AUTHORITY_MIGRATION_NOT_IMPLEMENTED"
                or body["coverage_contract"] != "ACTUAL_CLOSE_AND_AVAILABLE_M5_MARKS_NOT_COMPLETE_DAILY_VALUATION"
                or body["selection_policy"] != "P75_REQUIRED_P95_PREFERRED_AMONG_P75_PASSES"):
            _fail("FROZEN_TIER_RAW_BINDING_INVALID")
        projection, original = body["cell_projection"], body["original_precommit_anchor"]
        _exact_dict(projection, {"cell_id", "process_sha256"}, "FROZEN_TIER_PROJECTION_INVALID")
        if (original.get("full_book_descriptor") != body["full_book_descriptor"]
                or original.get("full_book_sha256") != body["full_book_descriptor"].get("sha256")):
            _fail("FROZEN_TIER_ORIGINAL_BOOK_BINDING_INVALID")
        full_book = _read_descriptor_json(context, body["full_book_descriptor"], code="FROZEN_TIER_ORIGINAL_BOOK", maximum=_MAX_RAW_ARTIFACT_BYTES)
        full_book = forward._owned_book(full_book, full_book.get("receipt_sha256"))
        selected = [cell for cell in full_book.get("cells", ()) if type(cell) is dict and cell.get("cell_id") == projection["cell_id"]]
        if len(selected) != 1 or selected[0].get("local_process_sha256") != projection["process_sha256"]:
            _fail("FROZEN_TIER_ORIGINAL_BOOK_OWNER_MISMATCH")
        original_owner = forward._reopen_owner_capture(context=context, anchor=anchor, book_body=full_book,
            cell_id=projection["cell_id"], symbol_receipt=body["owner_source"]["symbol_receipt"],
            adapter_index_descriptor=body["owner_source"]["adapter_index_descriptor"])
        if (forward._owner_provenance(original_owner) != body["owner_provenance"]
                or original_owner.process_sha256 != projection["process_sha256"]):
            _fail("FROZEN_TIER_ORIGINAL_OWNER_PROVENANCE_CHANGED")
        original_global = _read_descriptor_json(context, body["original_global_precommit_descriptor"],
            code="FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT")
        config, deployment, retained_deployment_bytes = forward._reopen_retained_deployment(
            context=context, descriptors=body["deployment_source_descriptors"],
            original_receipt=original_global["deployment_binding"], config_record=body["pipeline_config_binding"])
        forward._reopen_original_global_precommit(context=context, descriptor=body["original_global_precommit_descriptor"],
            precommit_anchor=original, deployment=deployment)
        if (body["capital_phase"] not in forward.CAPITAL_PHASES
                or original.get("anchor_id") != anchor.get("anchor_id")
                or forward._original_capital_window(anchor, original, body["capital_phase"]) != body["capital_window"]):
            _fail("FROZEN_TIER_ORIGINAL_WINDOW_BINDING_CHANGED")
        start, end = forward._capital_window_datetimes(body["capital_window"],
            phase_id=forward._capital_phase_id(anchor["anchor_id"], body["capital_phase"]))
        def original_roster(field, digest_key):
            values = original.get(field)
            if type(values) is not list or original.get(digest_key) != forward._sha(values):
                _fail("FROZEN_TIER_ORIGINAL_SOURCE_ROSTER_CHANGED")
            result = {str(value.get("role")).rsplit(":", 1)[-1]: value for value in values if type(value) is dict}
            if len(result) != len(values):
                _fail("FROZEN_TIER_ORIGINAL_SOURCE_ROSTER_CHANGED")
            return result
        execution_roster = original_roster("execution_products", "execution_product_roster_sha256")
        fine_roster = original_roster("spread_source_products", "spread_source_product_roster_sha256")
        valuation_roster = original_roster("valuation_m5_products", "valuation_m5_product_roster_sha256")
        for identity, roster in ((body["execution_source"], execution_roster), (body["spread_source"], fine_roster)):
            key = forward.native_product_key(identity["symbol"], identity["timeframe"])
            if roster.get(key) != identity:
                _fail("FROZEN_TIER_USED_SOURCE_PRECOMMIT_SPLICE")
        accounts = body["accounts"]
        if type(accounts) is not dict or list(accounts) != ["P75", "P95"]:
            # Canonical JSON sorts these two names in the same order.
            _fail("FROZEN_TIER_ACCOUNT_ROSTER_INVALID")
        descriptors = [dict(accounts[level]["tier"]) for level in ("P75", "P95")]
        for value, level in zip(descriptors, ("P75", "P95"), strict=True):
            if value.pop("tier") != level:
                _fail("FROZEN_TIER_ACCOUNT_ROSTER_INVALID")
            value.pop("spread_points")
            value.pop("full_spread_price")
        if descriptors[0] != descriptors[1] or accounts["P95"]["tier"]["spread_points"] < accounts["P75"]["tier"]["spread_points"]:
            _fail("FROZEN_TIER_PROFILE_PAIR_SPLICE")
        if not compact:
            fine_rows = body["spread_source_rows"]
            if type(fine_rows) is not list or not fine_rows:
                _fail("FROZEN_TIER_CAPTURED_FINE_TIMEFRAME_UNAVAILABLE")
            fine_frame = pd.DataFrame({"Spread": [row["spread_points"] for row in fine_rows],
                                       "_BAR_CLOSE_TS": [row["close_utc"] for row in fine_rows]},
                                      index=pd.DatetimeIndex([row["open_utc"] for row in fine_rows]))
        try:
            profile_descriptor, _ = forward.native_prefix._read_spool_descriptor(
                context, body["frozen_profile_descriptor"], expected_format="json-profile", collect=False)
            profile = forward.spread_profile.FrozenSpreadProfile(Path(context.root) / profile_descriptor["relative_path"])
            if profile.sha256 != profile_descriptor["sha256"]:
                _fail("FROZEN_TIER_PROFILE_ARTIFACT_CHANGED")
            for receipt, identity in ((body["execution_spec_receipt"], body["execution_source"]),
                                      (body["spread_source_execution_spec_receipt"], body["spread_source"])):
                unsigned = dict(receipt)
                receipt_sha = unsigned.pop("receipt_sha256", None)
                if (receipt_sha != forward._sha(unsigned)
                        or receipt.get("native_prefix_index_sha256") != identity["index_sha256"]
                        or receipt.get("grants_search_authority") is not False
                        or receipt.get("grants_trading_authority") is not False
                        or receipt.get("instrument_spec") != body["execution_spec_receipt"].get("instrument_spec")):
                    _fail("FROZEN_TIER_EXECUTION_SPEC_RECEIPT_CHANGED")
            reopened_fine, fine_product = _reopen_frozen_transport_source(context, body["spread_source"], config)
            if compact:
                fine_frame = reopened_fine
            else:
                expected_fine = [{"open_utc": stamp.isoformat(), "close_utc": reopened_fine["_BAR_CLOSE_TS"].iloc[index].isoformat(),
                                  "spread_points": (None if pd.isna(reopened_fine["Spread"].iloc[index])
                                                    else float(reopened_fine["Spread"].iloc[index]))}
                                 for index, stamp in enumerate(reopened_fine.index)]
                if expected_fine != fine_rows:
                    _fail("FROZEN_TIER_FINE_SOURCE_ROWS_CHANGED")
            reopened_execution, execution_product = _reopen_frozen_transport_source(context, body["execution_source"], config)
            actual_spec, actual_receipt = forward._derive_retained_execution_spec(product=execution_product,
                deployment=deployment, source_bytes=retained_deployment_bytes)
            _, actual_fine_receipt = forward._derive_retained_execution_spec(product=fine_product,
                deployment=deployment, source_bytes=retained_deployment_bytes)
            if actual_receipt != body["execution_spec_receipt"] or actual_fine_receipt != body["spread_source_execution_spec_receipt"]:
                _fail("FROZEN_TIER_EXECUTION_SPEC_RECEIPT_CHANGED")
            expected_retained = reopened_execution.loc[reopened_execution["_BAR_CLOSE_TS"] <= end]
            expected_warmup = int(expected_retained.index.searchsorted(start, side="left"))
            if not 1 <= expected_warmup < len(expected_retained) or not bool((expected_retained.index < end).all()):
                _fail("FROZEN_TIER_PRECOMMITTED_WINDOW_UNAVAILABLE")
            observations, matched_closes = forward.spread_profile.align_prior_completed_spreads(
                (expected_retained.index if compact else
                 pd.DatetimeIndex([row["target_open_utc"] for row in body["source_observations"]])), fine_frame,
                accounts["P75"]["tier"]["source_timeframe"], include_matched_closes=True)
            if schema == forward.ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA:
                expected_observations = [{"target_open_utc": stamp.isoformat(), "prior_fine_close_utc": stamp.isoformat(),
                    "points": None if pd.isna(value) else float(value)} for stamp, value in observations.items()]
            else:
                expected_observations = forward._spread_source_observations(observations, matched_closes)
            if not compact and expected_observations != body["source_observations"]:
                _fail("FROZEN_TIER_SOURCE_OBSERVATIONS_CHANGED")
            opportunities = None
            if schema in (forward.RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
                          forward.FROZEN_TIER_RAW_ACCOUNTS_SCHEMA):
                _, original_signals = forward._features_and_prefix_parity(expected_retained, [original_owner])
                signal = original_signals[original_owner.cell_id]
                if not signal.index.equals(expected_retained.index):
                    _fail("FROZEN_TIER_ORIGINAL_SIGNAL_OPPORTUNITIES_UNAVAILABLE")
                opportunities = np.zeros(len(expected_retained), dtype=bool)
                scored_start = max(1, expected_warmup)
                opportunities[scored_start:] = signal.to_numpy(dtype=np.int32)[scored_start - 1:-1] != 0
            for level, account in accounts.items():
                tier = profile.from_descriptor(account["tier"], symbol=account["symbol"], timeframe=account["timeframe"],
                    point=account["instrument_spec"]["point"], source_identity_sha256=body["execution_source"]["source_identity_sha256"])
                binding = tier.descriptor()["source_binding"]
                if (tier.source_timeframe != body["spread_source"]["timeframe"]
                        or binding["bars_sha256"] != body["spread_source"]["source_bars_sha256"]
                        or binding["sidecar_sha256"] != body["spread_source"]["source_audit_sidecar_sha256"]
                        or binding["provenance_sha256"] != body["spread_source"]["source_provenance_sha256"]
                        or binding["target_bars_sha256"] != body["execution_source"]["source_bars_sha256"]
                        or tier.descriptor()["activation"]["id"] != body["spread_source"]["activation_id"]
                        or tier.descriptor()["activation"]["manifest_sha256"] != body["spread_source"]["activation_manifest_sha256"]
                        or body["spread_source"]["source_identity_sha256"] != body["execution_source"]["source_identity_sha256"]):
                    _fail("FROZEN_TIER_PROFILE_SOURCE_BINDING_MISMATCH")
                if (account["cell_id"] != projection["cell_id"] or account["tier"]["tier"] != level
                        or account["initial_cash"] != float(config.initial_capital)
                        or account["position_namespace"] != "|".join((original_owner.symbol, original_owner.timeframe,
                            str(getattr(forward._strategy(original_owner), "name", original_owner.strategy_class))))
                        or body["genesis"] != {"mode": forward.NEW_SIMULATED_FLAT_GENESIS,
                            "initial_cash": account["initial_cash"], "initial_inventory": [], "owner_changes": []}
                        or account["instrument_spec"] != body["execution_spec_receipt"].get("instrument_spec")
                        or account["engine_spread_admission"].get("input_provenance_authenticated") is not False
                        or account["engine_spread_admission"].get("qualification_authority") is not False):
                    _fail("FROZEN_TIER_ACCOUNT_GENESIS_OR_TERMS_SPLICE")
                if not compact:
                    if [row["open_utc"] for row in account["source_rows"]] != [row["target_open_utc"] for row in expected_observations]:
                        _fail("FROZEN_TIER_ACCOUNT_OBSERVATION_AXIS_SPLICE")
                    if (account["warmup_bars"] != expected_warmup
                            or [row["open_utc"] for row in account["source_rows"]] != [stamp.isoformat() for stamp in expected_retained.index]
                            or any(pd.Timestamp(row["close_utc"]) > end for row in account["source_rows"][expected_warmup:])):
                        _fail("FROZEN_TIER_RETAINED_WINDOW_OR_WARMUP_CHANGED")
                actual_entry_half, actual_intrabar_half, actual_spread_meta = forward.Backtester(config)._build_half_spread_arrays(
                    expected_retained, actual_spec, spread_tier=tier, timeframe=account["timeframe"])
                # V3 run() records the complete admission binding. Rebuild it
                # from authenticated observations; v1/v2 retain obs-only models.
                actual_spread_meta = _verify_frozen_execution_spread_model(
                    actual_spread_meta, account["engine_spread_model"], schema,
                    features=expected_retained, tier=tier, observations=observations)
                if compact:
                    account = _materialize_tier_source_rows(account, expected_retained,
                        actual_entry_half, actual_intrabar_half, actual_spread_meta)
                    accounts[level] = account
                    if [row["open_utc"] for row in account["source_rows"]] != [row["target_open_utc"] for row in expected_observations]:
                        _fail("FROZEN_TIER_ACCOUNT_OBSERVATION_AXIS_SPLICE")
                    if (account["warmup_bars"] != expected_warmup
                            or [row["open_utc"] for row in account["source_rows"]] != [stamp.isoformat() for stamp in expected_retained.index]
                            or any(pd.Timestamp(row["close_utc"]) > end for row in account["source_rows"][expected_warmup:])):
                        _fail("FROZEN_TIER_RETAINED_WINDOW_OR_WARMUP_CHANGED")
                _verify_frozen_spread_admission(schema=schema, account=account, observations=observations,
                    tier=tier, opportunities=opportunities)
                entry_column, bar_column = actual_spread_meta["entry_column"], actual_spread_meta["bar_column"]
                def source_points(source, column):
                    return None if column not in source or pd.isna(source[column]) else float(source[column])
                for row_index, row in enumerate(account["source_rows"]):
                    source = reopened_execution.loc[pd.Timestamp(row["open_utc"])]
                    if (row["close_utc"] != source["_BAR_CLOSE_TS"].isoformat()
                            or row["open"] != float(source["Open"]) or row["close"] != float(source["Close"])
                            or row["high"] != float(source["High"]) or row["low"] != float(source["Low"])
                            or row["recorded_entry_points"] != source_points(source, entry_column)
                            or row["recorded_intrabar_points"] != source_points(source, bar_column)
                            or row["entry_half_spread"] != float(actual_entry_half[row_index])
                            or row["intrabar_half_spread"] != float(actual_intrabar_half[row_index])):
                        _fail("FROZEN_TIER_EXECUTION_SOURCE_ROWS_CHANGED")
                for trade in account["trades"]:
                    value = observations.iloc[trade["entry_bar"]]
                    if pd.isna(value) or value > account["tier"]["spread_points"] or trade.get("entry_spread_admission") != "ADMIT":
                        _fail("FROZEN_TIER_TRADE_ADMISSION_RECONCILIATION_FAILED")
                entry_prices = np.full(len(expected_retained), np.nan)
                for trade in account["trades"]:
                    entry_prices[trade["entry_bar"]] = trade["entry_price"]
                pre, close, long_rates, short_rates, enabled, contract = forward.Backtester(config)._build_causal_swap_inputs(
                    expected_retained, actual_spec, entry_prices, timeframe=account["timeframe"])
                reconstructed_model = {"pre_open_units": forward._raw_json(pre), "close_units": forward._raw_json(close),
                    "long_per_lot": forward._raw_json(long_rates), "short_per_lot": forward._raw_json(short_rates),
                    "enabled": bool(enabled), "contract": forward._raw_json(contract)}
                if reconstructed_model != account["financing_model"]:
                    _fail("FROZEN_TIER_ORIGINAL_FINANCING_MODEL_CHANGED")
                rebuilt = forward._reconcile_tier_account(account)
                if rebuilt != account["derived"]:
                    _fail("FROZEN_TIER_DERIVED_STATE_CHANGED")
                valuation = account["valuation_source"]
                valuation_key = forward.native_product_key(account["symbol"], "M5")
                if valuation_roster.get(valuation_key) != valuation["native_product_identity"]:
                    _fail("FROZEN_TIER_USED_M5_SOURCE_PRECOMMIT_SPLICE")
                if (valuation["native_product_identity"].get("timeframe") != "M5"
                        or valuation["native_product_identity"].get("symbol") != account["symbol"]
                        or valuation["execution_spec_receipt"].get("instrument_spec") != account["instrument_spec"]
                        or valuation["spread_and_cost_surface"]["spread_model"].get("frozen_spread_tier") != account["tier"]):
                    _fail("FROZEN_TIER_M5_TARGET_CELL_OR_TERMS_SPLICE")
                rows = None if compact else account["valuation_rows"]
                reopened_valuation, valuation_product = _reopen_frozen_transport_source(context, valuation["native_product_identity"], config)
                _, actual_valuation_receipt = forward._derive_retained_execution_spec(product=valuation_product,
                    deployment=deployment, source_bytes=retained_deployment_bytes)
                if actual_valuation_receipt != valuation["execution_spec_receipt"]:
                    _fail("FROZEN_TIER_M5_EXECUTION_SPEC_RECEIPT_CHANGED")
                valuation_opens, valuation_closes = forward._frame_interval(reopened_valuation)
                valuation_start = max(0, int(valuation_opens.searchsorted(start, side="left")) - 1)
                valuation_end = int(valuation_closes.searchsorted(end, side="right"))
                retained_valuation = reopened_valuation.iloc[valuation_start:valuation_end]
                half, intrabar_half, spread_meta = forward.Backtester(config)._build_half_spread_arrays(
                    retained_valuation, actual_spec, spread_tier=tier, timeframe=tier.timeframe)
                spread_meta = _frozen_spread_read_model(spread_meta, schema)
                expected_valuation_rows = [{"open_utc": stamp.isoformat(), "open": float(source["Open"]),
                    "recorded_entry_points": source_points(source, spread_meta["entry_column"])}
                    for stamp, source in retained_valuation.iterrows()]
                if compact:
                    rows = expected_valuation_rows
                    account["valuation_rows"] = rows
                elif rows != expected_valuation_rows:
                    _fail("FROZEN_TIER_M5_RETAINED_SOURCE_ROWS_CHANGED")
                index = pd.DatetimeIndex([row["open_utc"] for row in rows])
                frame = pd.DataFrame({"Open": [row["open"] for row in rows],
                                       "SpreadOpen": [row["recorded_entry_points"] for row in rows]}, index=index)
                cost_attestation = forward.Backtester(config)._numerical_execution_cost_attestation(
                    entry_half_spread=half, intrabar_half_spread=intrabar_half,
                    spread_meta=spread_meta, spec=actual_spec)
                actual_cost_surface = {"spread_model": forward._raw_json(spread_meta),
                    "cost_attestation": forward._raw_json(cost_attestation),
                    "valuation_price_field": "Open", "valuation_timeframe": "M5",
                    "executable_liquidation": "BID_FOR_LONG_ASK_FOR_SHORT",
                    "mark_cost_basis": "EXECUTABLE_BID_ASK_PLUS_ACCRUED_SWAP"}
                if actual_cost_surface != valuation["spread_and_cost_surface"]:
                    _fail("FROZEN_TIER_M5_COST_SURFACE_CHANGED")
                tape = forward._M5ValuationTape(account["symbol"], frame, index, half,
                    valuation["spread_and_cost_surface"], valuation["execution_spec_receipt"], valuation["native_product_identity"])
                reconstructed_marks = forward._tier_m5_marks(account, tape)
                if compact:
                    account["m5_marks"] = reconstructed_marks
                elif reconstructed_marks != account["m5_marks"]:
                    _fail("FROZEN_TIER_M5_EXECUTABLE_MARK_CHANGED")
        except CausalEvidenceProductionError:
            raise
        except forward.ForwardOOSReplayProductionError as exc:
            raise CausalEvidenceProductionError(exc.code) from exc
        except (KeyError, TypeError, ValueError, IndexError, OverflowError) as exc:
            raise CausalEvidenceProductionError("FROZEN_TIER_RAW_ACCOUNT_INVALID") from exc
        return FrozenTierRawAccountsProjection(projection["cell_id"], ("P75", "P95"), raw_descriptor["sha256"], accounts)
    except CausalEvidenceProductionError:
        raise
    except forward.ForwardOOSReplayProductionError as exc:
        raise CausalEvidenceProductionError(exc.code) from exc
    except forward.config_binding.ProductionConfigBindingError as exc:
        raise CausalEvidenceProductionError("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_INVALID") from exc
    except (KeyError, TypeError, ValueError, ArithmeticError, IndexError, AttributeError, OSError) as exc:
        raise CausalEvidenceProductionError("FROZEN_TIER_RAW_ACCOUNT_INVALID") from exc


def _verify_declared_cell_account_manifest(context, body, descriptor):
    import pm_forward_oos_replay_producer as forward

    try:
        symbol = forward._declared_cell_transport_symbol(body)
        relative_path = authority_spool._manifest_relative_path(
            symbol, authority_spool.GENERAL_MANIFEST_DOMAIN)
        digest, size, _ = authority_spool._stream_file(
            context.root, relative_path, maximum_bytes=context.limits.max_manifest_bytes,
            collect=False, chunk_bytes=context.limits.stream_chunk_bytes)
        receipt = {
            "schema_version": authority_spool.COMPACT_RECEIPT_SCHEMA,
            "generation_id": context.generation_id,
            "symbol": symbol,
            "manifest_domain": authority_spool.GENERAL_MANIFEST_DOMAIN,
            "native_universe_sha256": context.native_universe_sha256,
            "config_sha256": context.config_sha256,
            "generation_context_sha256": context.context_sha256,
            "symbol_manifest": {
                "schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
                "relative_path": relative_path,
                "sha256": digest,
                "size_bytes": size,
                "format": authority_spool.CANONICAL_JSON_FORMAT,
            },
        }
        manifest = authority_spool.verify_symbol_receipt(
            context.root, receipt, expected_generation_id=context.generation_id,
            expected_symbol=symbol, expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
            expected_manifest_domain=authority_spool.GENERAL_MANIFEST_DOMAIN)
    except (authority_spool.AuthoritySpoolError, OSError, KeyError, TypeError, ValueError) as exc:
        raise CausalEvidenceProductionError("DECLARED_CELL_RAW_MANIFEST_INVALID") from exc
    expected = [{"artifact_id": "declared-cell-account", "artifact_type": "declared_cell_raw_account",
                 "descriptor": descriptor}]
    if manifest["artifacts"] != expected:
        _fail("DECLARED_CELL_RAW_MANIFEST_BINDING_MISMATCH")


def read_declared_cell_account_artifact(
    context: authority_spool.GenerationSpoolContext,
    artifact: DeclaredCellRawAccountArtifact,
    *,
    process: Any,
    frozen_pipeline_config: Any,
) -> DeclaredCellRawAccountProjection:
    """Reopen a declared raw account without replaying or qualifying it."""
    import pm_forward_oos_replay_producer as forward

    if type(artifact) is not DeclaredCellRawAccountArtifact:
        _fail("DECLARED_CELL_RAW_DESCRIPTOR_INVALID")
    if process is None or frozen_pipeline_config is None:
        _fail("DECLARED_CELL_ACCOUNT_INPUTS_REQUIRED")
    try:
        descriptor = _descriptor(artifact.descriptor, "DECLARED_CELL_RAW_DESCRIPTOR_INVALID")
        body = _read_descriptor_json(context, descriptor, code="DECLARED_CELL_RAW_ACCOUNT")
        if body.get("schema_version") != forward.DECLARED_CELL_RAW_ACCOUNT_SCHEMA:
            _fail("DECLARED_CELL_RAW_SCHEMA_UNSUPPORTED")
        _verify_declared_cell_account_manifest(context, body, descriptor)

        def reopen_source(source_context, identity, config):
            return _reopen_frozen_transport_source(
                source_context, identity, config, pin_consumed_activation=True)

        verified = forward.verify_declared_cell_account_transport(
            context, body, process=process, frozen_pipeline_config=frozen_pipeline_config,
            reopen_source=reopen_source)
        if verified != body:
            _fail("DECLARED_CELL_RAW_ACCOUNT_INVALID")
        return DeclaredCellRawAccountProjection(
            cell_id=body["cell"]["cell_id"], descriptor_sha256=descriptor["sha256"],
            progress_status=body["progress_status"], reason_code=body["reason_code"],
            slot_schedule=tuple(body["slot_schedule"]), owner_records=tuple(body["owner_records"]),
            account=body["account"],
            _verified_body_json=_canonical_bytes(verified, _MAX_RAW_ARTIFACT_BYTES))
    except CausalEvidenceProductionError:
        raise
    except forward.ForwardOOSReplayProductionError as exc:
        raise CausalEvidenceProductionError(exc.code) from exc
    except (KeyError, TypeError, ValueError, ArithmeticError, IndexError, AttributeError, OSError) as exc:
        raise CausalEvidenceProductionError("DECLARED_CELL_RAW_ACCOUNT_INVALID") from exc


_DECLARED_CELL_DISPOSITION_SCHEMA = "fxpm.declared-cell-interval-disposition.v1"
_DECLARED_CELL_DISPOSITION_MAX_BYTES = 4096
_DECLARED_CELL_DISPOSITION_IDENTITIES = (
    "raw_account_descriptor_sha256", "generation_context_sha256",
    "declared_precommit_sha256", "search_process_receipt_sha256",
    "config_sha256", "effective_config_sha256",
)
_DECLARED_CELL_DISPOSITION_FALSE_FIELDS = (
    "interval_policy_adopted", "book_admissible", "qualification_authority",
    "capital_authority", "live_authority", "order_authority",
)


@dataclass(frozen=True, slots=True, repr=False)
class DeclaredCellAccountInputs:
    """Opaque references to one already-produced declared account."""

    context: authority_spool.GenerationSpoolContext
    artifact: DeclaredCellRawAccountArtifact
    process: Any
    frozen_pipeline_config: Any


@dataclass(frozen=True, slots=True, repr=False)
class DeclaredCellIntervalDisposition:
    """Bound nonqualification result with no account values or authority."""

    _body_json: bytes

    def __post_init__(self) -> None:
        if (type(self._body_json) is not bytes
                or len(self._body_json) > _DECLARED_CELL_DISPOSITION_MAX_BYTES):
            _fail("DECLARED_CELL_DISPOSITION_INVALID")
        try:
            body = json.loads(self._body_json.decode("utf-8"))
        except (UnicodeError, ValueError) as exc:
            raise CausalEvidenceProductionError("DECLARED_CELL_DISPOSITION_INVALID") from exc
        fields = {"schema_version", "status", "reason_code", "cell_id",
                  *_DECLARED_CELL_DISPOSITION_IDENTITIES,
                  *_DECLARED_CELL_DISPOSITION_FALSE_FIELDS}
        _exact_dict(body, fields, "DECLARED_CELL_DISPOSITION_INVALID")
        if (body["schema_version"] != _DECLARED_CELL_DISPOSITION_SCHEMA
                or body["status"] != "NOT_QUALIFIED"
                or body["reason_code"] != "NO_ADOPTED_INTERVAL_POLICY"
                or any(body[name] is not False
                       for name in _DECLARED_CELL_DISPOSITION_FALSE_FIELDS)
                or _canonical_bytes(body, _DECLARED_CELL_DISPOSITION_MAX_BYTES) != self._body_json):
            _fail("DECLARED_CELL_DISPOSITION_INVALID")
        _safe_id(body["cell_id"], "DECLARED_CELL_DISPOSITION_INVALID")
        for name in _DECLARED_CELL_DISPOSITION_IDENTITIES:
            _hex64(body[name], "DECLARED_CELL_DISPOSITION_INVALID")

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def status(self) -> str:
        return "NOT_QUALIFIED"

    @property
    def reason_code(self) -> str:
        return "NO_ADOPTED_INTERVAL_POLICY"


def evaluate_declared_cell_interval_admission(
    inputs: DeclaredCellAccountInputs,
) -> DeclaredCellIntervalDisposition:
    """Reverify a complete account and record the absent interval policy."""
    import pm_production_config_binding as config_binding
    import pm_production_search_planner as search_planner

    if (type(inputs) is not DeclaredCellAccountInputs
            or type(inputs.context) is not authority_spool.GenerationSpoolContext
            or type(inputs.artifact) is not DeclaredCellRawAccountArtifact
            or type(inputs.process) is not search_planner.FrozenProductionSearchProcess
            or type(inputs.frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig):
        _fail("DECLARED_CELL_ADMISSION_INPUTS_INVALID")
    projection = read_declared_cell_account_artifact(
        inputs.context, inputs.artifact, process=inputs.process,
        frozen_pipeline_config=inputs.frozen_pipeline_config)
    body = projection.verified_body
    progress = body.get("progress_status")
    if progress == "STOP":
        _fail("DECLARED_CELL_ACCOUNT_STOP")
    if progress == "INCOMPLETE":
        _fail("DECLARED_CELL_ACCOUNT_INCOMPLETE")
    if (progress != "COMPLETE" or type(body.get("account")) is not dict
            or body.get("reason_code") is not None):
        _fail("DECLARED_CELL_ACCOUNT_PROGRESS_INVALID")
    binding = body["pipeline_config_binding"]
    config = inputs.frozen_pipeline_config
    if (body["generation_context_sha256"] != inputs.context.context_sha256
            or body["search_process_receipt_sha256"] != inputs.process.receipt_sha256
            or binding["config_sha256"] != config.config_sha256
            or binding["effective_config_sha256"] != config.effective_config_sha256
            or config.config_sha256 != inputs.context.config_sha256
            or any(body[name] is not False for name in
                   ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_CELL_ADMISSION_BINDING_INVALID")
    result = {
        "schema_version": _DECLARED_CELL_DISPOSITION_SCHEMA,
        "status": "NOT_QUALIFIED",
        "reason_code": "NO_ADOPTED_INTERVAL_POLICY",
        "cell_id": body["cell"]["cell_id"],
        "raw_account_descriptor_sha256": projection.descriptor_sha256,
        "generation_context_sha256": inputs.context.context_sha256,
        "declared_precommit_sha256": body["declared_precommit_sha256"],
        "search_process_receipt_sha256": inputs.process.receipt_sha256,
        "config_sha256": config.config_sha256,
        "effective_config_sha256": config.effective_config_sha256,
        **{name: False for name in _DECLARED_CELL_DISPOSITION_FALSE_FIELDS},
    }
    return DeclaredCellIntervalDisposition(
        _canonical_bytes(result, _DECLARED_CELL_DISPOSITION_MAX_BYTES))


def _load_replay_ledgers(
    context: authority_spool.GenerationSpoolContext,
    descriptors: AnchorReplayArtifactDescriptors | DeclaredCellRawAccountArtifact,
    anchor: Mapping[str, Any],
    *,
    declared_process: Any = None,
    declared_frozen_pipeline_config: Any = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if type(descriptors) is DeclaredCellRawAccountArtifact:
        read_declared_cell_account_artifact(
            context, descriptors, process=declared_process,
            frozen_pipeline_config=declared_frozen_pipeline_config).require_interval_qualification()
    if type(descriptors) is FrozenTierRawAccountsArtifact:
        _read_frozen_tier_raw_accounts(context, descriptors, anchor).require_interval_qualification()
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
            book.TERMINAL_TECHNICAL: causal.TERMINAL_TECHNICAL,
        }.get(raw_state)
        if status is None:
            _fail("BOOK_TECHNICAL_OR_UNSEARCHED_CELL")
        expected_source_availability = (
            book.TERMINAL_UNAVAILABLE
            if status == "SOURCE_UNAVAILABLE"
            else book.TERMINAL_TECHNICAL
            if status == causal.TERMINAL_TECHNICAL
            else book.AVAILABLE
        )
        if source_payload["availability"] != expected_source_availability:
            _fail("BOOK_SOURCE_STATE_CONTRADICTION")
        available = expected_source_availability == book.AVAILABLE
        terminal_technical = status == causal.TERMINAL_TECHNICAL
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
        if terminal_technical and binding.get("plan_status") != (
            "HISTORICAL_TERMINAL_TECHNICAL"
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
            "schema": "fxpm-anchor-local-cell-decision-v6",
            "anchor_id": anchor["anchor_id"],
            "symbol": raw_cell["symbol"],
            "timeframe": raw_cell["timeframe"],
            "regime": raw_cell["regime"],
            "selection_cutoff_utc": cutoff,
            "status": status,
            "source_state": (
                "AVAILABLE"
                if available
                else causal.TERMINAL_TECHNICAL_SOURCE_STATE
                if terminal_technical
                else "AUTHENTIC_TERMINAL_UNAVAILABLE"
            ),
            "source_terminal_reason": (
                None
                if available
                else causal.TERMINAL_TECHNICAL_REASON
                if terminal_technical
                else "BROKER_HISTORY_TERMINAL_ABSENCE"
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
            "terminal_source_unavailable": status == "SOURCE_UNAVAILABLE",
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
            "p75_fill_price",
        )
    }
    primitive["cell_decision_sha256"] = cell["cell_decision_sha256"]
    primitive["timeframe"] = cell["timeframe"]
    payload_sha = _sha(
        {
            "schema": "fxpm-position-opening-fill-payload-v3",
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


def _opening_execution_cost(fill: Mapping[str, Any], *, p75: bool) -> Decimal:
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
    lane = "p75" if p75 else "base"
    lane_fill = _decimal(
        fill[f"{lane}_fill_price"], "FILL_PRICE_INVALID", positive=True
    )
    spread = side * (executable - mid) * unit
    fill_degradation = side * (lane_fill - executable) * unit
    return max(Decimal(0), spread + fill_degradation)


def _closing_execution_cost(
    fill: Mapping[str, Any], state: Mapping[str, Any], *, p75: bool
) -> Decimal:
    side = state["side_sign"]
    unit = state["unit_scale"]
    mid = _decimal(fill["reference_mid_price"], "REFERENCE_MID_INVALID", positive=True)
    executable = _decimal(
        fill["reference_bid_price"] if side > 0 else fill["reference_ask_price"],
        "REFERENCE_EXECUTABLE_INVALID",
        positive=True,
    )
    lane = "p75" if p75 else "base"
    lane_fill = _decimal(
        fill[f"{lane}_fill_price"], "FILL_PRICE_INVALID", positive=True
    )
    spread = side * (mid - executable) * unit
    fill_degradation = side * (executable - lane_fill) * unit
    return max(Decimal(0), spread + fill_degradation)


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
        # Carried positions are valid in decision-v10, but a production replay
        # seam must provide a dedicated forward-boundary valuation artifact.
        # The raw contract intentionally fails closed until that descriptor
        # is available rather than inventing an opening mark receipt.
        _fail("CARRIED_POSITION_BOUNDARY_ARTIFACT_REQUIRED")

    open_states: dict[str, dict[str, Any]] = {}
    activity_days: list[dict[str, Any]] = []
    gross_vector: list[Decimal] = []
    base_cost_vector: list[Decimal] = []
    p75_cost_vector: list[Decimal] = []
    seen_event_ids: set[str] = set()
    seen_positions: set[str] = set()
    cumulative_base_cost_return = Decimal(0)
    cumulative_p75_cost_return = Decimal(0)

    def require_position_cost_prefix(state: Mapping[str, Any]) -> None:
        if (
            state["p75_cost_prefix"] + causal._RETURN_QUANTUM
            < state["base_cost_prefix"]
        ):
            _fail("P75_POSITION_COST_PREFIX_IMPROVEMENT")

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
        entry_events = _unique_records(
            event_day["entries"], _EVENT_RECORD_FIELDS, "OPEN_EVENT_ROSTER_INVALID"
        )
        if set(raw_entries) != set(entry_events):
            _fail("OPEN_EVENT_FILL_ROSTER_MISMATCH")

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
            base_open_cost = _opening_execution_cost(opened, p75=False)
            p75_open_cost = _opening_execution_cost(opened, p75=True)
            if p75_open_cost < base_open_cost:
                _fail("P75_OPEN_COST_LESS_SEVERE")
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
                "pending_base_cost": base_open_cost,
                "pending_p75_cost": p75_open_cost,
                "base_cost_prefix": base_open_cost,
                "p75_cost_prefix": p75_open_cost,
                "cell_id": raw_open["cell_id"],
            }
            require_position_cost_prefix(open_states[position_id])
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
        p75_cost_cash = Decimal(0)
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
                    "p75_financing_cash": mark_costs[event_id][
                        "p75_financing_cash"
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
            p75_finance = _decimal(
                mark["p75_financing_cash"], "P75_FINANCING_INVALID"
            )
            base_cost_cash += state["pending_base_cost"] + base_finance
            p75_cost_cash += state["pending_p75_cost"] + p75_finance
            state["base_cost_prefix"] += base_finance
            state["p75_cost_prefix"] += p75_finance
            require_position_cost_prefix(state)
            state["pending_base_cost"] = Decimal(0)
            state["pending_p75_cost"] = Decimal(0)
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
        exit_events = _unique_records(
            event_day["exits"], _EVENT_RECORD_FIELDS, "CLOSE_EVENT_ROSTER_INVALID"
        )
        if set(raw_exits) != set(exit_events):
            _fail("CLOSE_EVENT_FILL_ROSTER_MISMATCH")
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
                    "p75_fill_price",
                )
            }
            payload_sha = _sha(
                {
                    "schema": "fxpm-position-closing-fill-payload-v3",
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
            base_exit = _closing_execution_cost(closing, state, p75=False)
            p75_exit = _closing_execution_cost(closing, state, p75=True)
            if p75_exit < base_exit:
                _fail("P75_CLOSE_COST_LESS_SEVERE")
            base_cost_cash += base_exit
            p75_cost_cash += p75_exit
            state["base_cost_prefix"] += base_exit
            state["p75_cost_prefix"] += p75_exit
            require_position_cost_prefix(state)
            exits.append(closing)
            del open_states[position_id]

        try:
            gross_return = causal._cash_return(
                gross_cash, equity, "derived gross return"
            )
            base_cost_return = causal._cash_return(
                -base_cost_cash, equity, "derived base cost return"
            )
            p75_cost_return = causal._cash_return(
                -p75_cost_cash, equity, "derived p75 cost return"
            )
        except Exception as exc:
            raise CausalEvidenceProductionError(
                "DAILY_RETURN_DERIVATION_FAILED"
            ) from exc
        cumulative_base_cost_return += base_cost_return
        cumulative_p75_cost_return += p75_cost_return
        if (
            cumulative_p75_cost_return
            > cumulative_base_cost_return + causal._RETURN_QUANTUM
        ):
            _fail("P75_CUMULATIVE_COST_PREFIX_IMPROVEMENT")
        for state in open_states.values():
            require_position_cost_prefix(state)
        gross_vector.append(gross_return)
        base_cost_vector.append(base_cost_return)
        p75_cost_vector.append(p75_cost_return)
        activity_days.append(
            {
                "date": date_text,
                "equity_basis_cash": mtm_day["equity_basis_cash"],
                "entries": entries,
                "position_marks": marks,
                "exits": exits,
            }
        )
    for state in open_states.values():
        require_position_cost_prefix(state)
    if open_states:
        _fail("FORWARD_REPLAY_ENDS_WITH_UNVALUED_OPEN_POSITIONS")
    return (
        {
            "schema": "fxpm-anchor-economic-position-activity-ledger-v3",
            "initial_positions": [],
            "days": activity_days,
        },
        gross_vector,
        base_cost_vector,
        p75_cost_vector,
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
        "daily_p75_cost_returns",
        "daily_base_net_mtm_returns",
        "daily_p75_net_mtm_returns",
        "activity_ledger",
        "activity_ledger_sha256",
        "calendar_sha256",
        "gross_vector_sha256",
        "base_cost_vector_sha256",
        "p75_cost_vector_sha256",
        "base_vector_sha256",
        "p75_vector_sha256",
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
    activity, gross, base_cost, p75_cost = _assemble_activity(
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
    p75 = [
        causal._quantized_return(g + c, "derived p75 net return")
        for g, c in zip(gross, p75_cost, strict=True)
    ]
    dates = _expected_dates(anchor["forward_start_date"], anchor["forward_end_date"])
    gross_text = _decimal_texts(gross)
    base_cost_text = _decimal_texts(base_cost)
    p75_cost_text = _decimal_texts(p75_cost)
    base_text = _decimal_texts(base)
    p75_text = _decimal_texts(p75)
    skeleton: dict[str, Any] = {
        "anchor_id": anchor["anchor_id"],
        "book_cells": cells,
        "activity_ledger": activity,
    }
    try:
        completed, observed_holding, ledger_ids, risk_diagnostics, activity_sha = (
            causal._validate_activity_ledger(
                skeleton, dates, gross, base_cost, p75_cost
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
        "p75_cost_vector_sha256": causal._vector_hash(
            dates, p75_cost_text, "P75_COST_DEBIT"
        ),
        "base_vector_sha256": causal._vector_hash(dates, base_text, "BASE"),
        "p75_vector_sha256": causal._vector_hash(dates, p75_text, "P75"),
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
        "daily_p75_cost_returns": p75_cost_text,
        "daily_base_net_mtm_returns": base_text,
        "daily_p75_net_mtm_returns": p75_text,
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
            "p75_vector_sha256": vector_hashes["p75_vector_sha256"],
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
        self._causal_precommit_json = _canonical_bytes(payload, 16 * 1024 * 1024)
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
            ) = _forward_oos_replay_lineage(
                context=self.context,
                product=self.forward_replay_product,
                historical_precommit=self.historical_precommit,
                historical_run_receipt=self.historical_run_receipt,
                causal_precommit_payload=precommit_payload,
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
                capital_replay_candidates_by_phase=(capital_candidates_by_phase),
                capital_lineage_by_phase=capital_lineages_by_phase,
            )
        except CausalEvidenceProductionError:
            raise
        except causal.ProcessVerificationError as exc:
            raise CausalEvidenceProductionError("CAUSAL_DECISION_V10_REJECTED") from exc
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
    "DeclaredCellAccountInputs",
    "DeclaredCellIntervalDisposition",
    "DeclaredCellRawAccountArtifact",
    "DeclaredCellRawAccountProjection",
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
    "freeze_causal_process_precommit_v10",
    "evaluate_declared_cell_interval_admission",
    "read_declared_cell_account_artifact",
]
