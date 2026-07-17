"""Fail-closed production authority orchestration before ledger CAS.

Callbacks may produce raw computations, read artifacts, or obtain external
signatures.  Every authority fact is recomputed by a released verifier.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

import pm_authority_assembly as authority_assembly
import pm_book_multiplicity as book
import pm_cap_calibration as cap
import pm_cap_replay_producer as cap_replay
import pm_causal_process as causal
import pm_current_refit_execution as current_execution
import pm_historical_anchor_orchestration as historical
import pm_portfolio_replay as replay
import pm_risk_admission as risk_admission


COORDINATOR_SCHEMA = "fxpm.production-authority-coordinator.v1"
MAX_COORDINATOR_JSON_BYTES = 64 * 1024 * 1024
MAX_CURRENT_LEDGER_BYTES = 256 * 1024 * 1024
MAX_BOUND_CONFIG_BYTES = 16 * 1024 * 1024
MAX_BOUND_BROKER_BYTES = 64 * 1024 * 1024
PRODUCTION_MARGIN_MODEL = "AUTHENTICATED_TICK_CASH_NOTIONAL_100PCT_V1"
PRODUCTION_FORWARD_REPLAY_CONTRACT_VERSION = "fxpm.forward-oos-replay-producer.v3"
PRODUCTION_MARGIN_FORMULA = "abs(authenticated_entry_fill)*tick_value/tick_size*volume"
_HEX = frozenset("0123456789abcdef")
_PORTFOLIO_VALIDATION_FIELDS = frozenset(
    {
        "minimum_calendar_days",
        "minimum_accepted_positions",
        "minimum_profit_factor",
        "minimum_return_pct",
        "maximum_drawdown_pct",
        "require_mtm",
        "maximum_bootstrap_ruin_upper_95pct",
        "maximum_bootstrap_p95_drawdown_pct",
        "minimum_bootstrap_daily_return_observations",
    }
)
_NATIVE_LINEAGE_FIELDS = (
    "native_activation_id",
    "native_activation_manifest_sha256",
    "native_activation_roster_sha256",
    "native_bars_sha256_by_source",
    "native_sidecar_sha256_by_source",
)
_CAP_CONTRACT_FIELDS = frozenset(
    {
        "book_contract_version",
        "refit_contract_version",
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    }
)
_CAP_LINEAGE_FIELDS = frozenset(
    {
        *_CAP_CONTRACT_FIELDS,
        "book_role",
        "book_decision_sha256",
        "book_generation_receipt_sha256",
        "refit_receipt_sha256",
        "refit_execution_sha256",
        "output_book_sha256",
        "winner_roster_sha256",
        *_NATIVE_LINEAGE_FIELDS,
        "margin_terms_sha256",
        "broker_terms_sha256",
    }
)


class ProductionAuthorityBlocked(RuntimeError):
    def __init__(self, code: str, details: Sequence[str] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(f"{self.code}{suffix}")


@dataclass(frozen=True, slots=True)
class HistoricalEvidenceRequest:
    _body: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class CurrentRefitRequest:
    _body: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class PortfolioEvidenceRequest:
    _body: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class CausalEvidenceProduct:
    evidence_payload: Mapping[str, Any]
    reconciliation_payload: Mapping[str, Any]
    historical_book_decision: Mapping[str, Any]
    capital_replay_candidates_by_phase: Mapping[
        str, Sequence[replay.CandidateTrade]
    ] = field(default_factory=dict)
    capital_lineage_by_phase: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class CurrentRefitProduct:
    current_book_decision: Mapping[str, Any]
    accepted_book_generation_receipt: Mapping[str, Any]
    current_refit_receipt: Mapping[str, Any]
    raw_manifest_path: Path
    artifact_root: Path
    detector_ledger_records: Sequence[Mapping[str, Any]]
    replay_candidates: Iterable[replay.CandidateTrade]
    replayed_cell_ids: Sequence[str]
    portfolio_evidence_producer: Callable[[PortfolioEvidenceRequest], Mapping[str, Any]]
    staged_ledger_path: Path
    staged_ledger: Mapping[str, Any]
    staged_ledger_content_sha256: str
    physical_ledger_sha256: str


class CausalEvidenceProducer(Protocol):
    def __call__(self, request: HistoricalEvidenceRequest) -> CausalEvidenceProduct: ...


class CausalEvidenceFactory(Protocol):
    def build_after_historical_run(
        self,
        *,
        historical_precommit: Any,
        historical_run_receipt: Any,
    ) -> CausalEvidenceProducer: ...


class CurrentRefitProducer(Protocol):
    def __call__(self, request: CurrentRefitRequest) -> CurrentRefitProduct: ...


class CapitalAuthorityFactory(Protocol):
    @property
    def scope_binding(self) -> Any: ...

    def require_authorization_scope(self, value: str) -> None: ...

    def __call__(
        self, request: "CapitalAuthorityRequest"
    ) -> "CapitalAuthorityInputs": ...


@dataclass(frozen=True, slots=True)
class HistoricalAuthorityInputs:
    context: Any
    run_id: str
    causal_precommit_payload: Mapping[str, Any]
    anchors: Sequence[Mapping[str, Any]]
    optimizer_producer_release_sha256: str
    anchor_producers: Mapping[str, Callable[..., Any]]
    causal_evidence_factory: CausalEvidenceFactory


@dataclass(frozen=True, slots=True)
class CapitalAuthorityInputs:
    output_root: Path
    constraints: cap.CapCalibrationConstraints
    precommitment_receipt: Mapping[str, Any]
    trusted_precommitment_public_keys: Mapping[str, bytes]
    frozen_candidate_rosters_by_phase: Mapping[str, Mapping[str, Any]]
    lineage_by_phase: Mapping[str, Mapping[str, Any]]
    replay_output_loader: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    generated_at_utc: str
    replay_evidence_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    trusted_replay_public_keys: Mapping[str, bytes]
    authority_key_id: str
    authorization_issued_at_utc: str
    authorization_expires_at_utc: str
    authorization_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    trusted_authority_public_keys: Mapping[str, bytes]
    authorization_scope: str
    bootstrap_identity_sha256: str
    capital_config_sha256: str
    authority_public_key_sha256: str
    shard_attempt_limit: int = 8


@dataclass(frozen=True, slots=True)
class AssemblyInputs:
    deployment_root: Path
    target_name: str
    generation_id: str
    authorization_scope: str
    active_config_path: Path
    config: Any
    config_payload: Mapping[str, Any]
    requested_symbols: Sequence[str]
    native_universe: Any
    current_selection_window: Mapping[str, str]
    # Must be None in production.  The coordinator samples one trusted clock
    # after the potentially long historical/current computation has finished.
    validation_time_utc: Any


@dataclass(frozen=True, slots=True)
class ProductionAuthorityRun:
    schema_version: str
    assembly: authority_assembly.AuthorityAssembly
    staged_ledger_path: Path
    staged_ledger_content_sha256: str
    physical_ledger_sha256: str
    historical_run_receipt_sha256: str
    historical_process_decision_sha256: str
    current_roster_sha256: str
    capital_decision_sha256: str
    selected_policy_point_id: str


@dataclass(frozen=True, slots=True)
class _HistoricalState:
    run_receipt: Any
    run_body: dict[str, Any]
    process_decision: dict[str, Any]
    precommit_payload: dict[str, Any]
    evidence_payload: dict[str, Any]
    reconciliation_payload: dict[str, Any]
    historical_book_decision: dict[str, Any]
    capital_bootstrap_identity_sha256: str
    capital_candidates_by_phase: Mapping[str, tuple[replay.CandidateTrade, ...]] = (
        field(default_factory=dict)
    )
    capital_lineage_by_phase: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class _CurrentState:
    product: CurrentRefitProduct
    roster: dict[str, Any]
    candidates: tuple[replay.CandidateTrade, ...]
    portfolio_evidence: dict[str, Any]
    portfolio_lineage: dict[str, Any]
    cost_contract: dict[str, Any]
    policies: dict[float, dict[str, Any]]
    validation_policy: dict[str, Any]
    native_identity: dict[str, Any]
    margin_contract_sha256: str = ""
    margin_terms_sha256: str = ""
    broker_terms_sha256: str = ""
    current_selection_window: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _CapitalState:
    artifact: dict[str, Any]
    expected_lineage: dict[str, Any]
    signed_replay_evidence: dict[str, Any]
    current_lineage: dict[str, Any]
    precommitment_receipt: dict[str, Any]
    trusted_authority_public_keys: dict[str, bytes]
    trusted_replay_public_keys: dict[str, bytes]
    trusted_precommitment_public_keys: dict[str, bytes]


@dataclass(frozen=True, slots=True)
class CapitalAuthorityRequest:
    """Verified states made available to the lazy capital-input factory.

    Instances are created only after historical causal PASS and raw current
    refit/portfolio replay verification.  The downstream factory may derive
    candidate receipts and lineages from these states, but cannot supply a
    replacement current roster to the coordinator.
    """

    _historical_state: _HistoricalState
    _current_state: _CurrentState
    validation_time_utc: Any
    authorization_scope: str


def _canonical_bytes(value: Any, maximum: int = MAX_COORDINATOR_JSON_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ProductionAuthorityBlocked("COORDINATOR_JSON_INVALID") from exc
    if len(encoded) > maximum:
        raise ProductionAuthorityBlocked("COORDINATOR_JSON_BOUND_EXCEEDED")
    return encoded


def _snapshot(value: Any, maximum: int = MAX_COORDINATOR_JSON_BYTES) -> Any:
    return json.loads(_canonical_bytes(value, maximum).decode("utf-8"))


def _object(value: Any, code: str) -> dict[str, Any]:
    copied = _snapshot(value)
    if type(copied) is not dict:
        raise ProductionAuthorityBlocked(code)
    return copied


def _owned_callback_isolated_object(value: Any, code: str) -> Any:
    """Detach mutable runtime objects from callback-held caller references."""

    try:
        owned = copy.deepcopy(value)
    except Exception as exc:
        raise ProductionAuthorityBlocked(code) from exc
    if owned is value and hasattr(value, "__dict__"):
        raise ProductionAuthorityBlocked(code)
    return owned


def _snapshot_assembly_inputs(inputs: AssemblyInputs) -> AssemblyInputs:
    """Own mutable authority facts before any producer callback can run."""

    if type(inputs) is not AssemblyInputs:
        raise ProductionAuthorityBlocked("ASSEMBLY_INPUTS_INVALID")
    try:
        requested_symbols = _snapshot(list(inputs.requested_symbols))
    except TypeError as exc:
        raise ProductionAuthorityBlocked("REQUESTED_SYMBOL_ROSTER_INVALID") from exc
    if type(requested_symbols) is not list or any(
        type(symbol) is not str for symbol in requested_symbols
    ):
        raise ProductionAuthorityBlocked("REQUESTED_SYMBOL_ROSTER_INVALID")
    return replace(
        inputs,
        config=_owned_callback_isolated_object(
            inputs.config,
            "PIPELINE_CONFIG_SNAPSHOT_INVALID",
        ),
        config_payload=_object(inputs.config_payload, "CONFIG_PAYLOAD_INVALID"),
        requested_symbols=tuple(requested_symbols),
        native_universe=_owned_callback_isolated_object(
            inputs.native_universe,
            "NATIVE_UNIVERSE_SNAPSHOT_INVALID",
        ),
        current_selection_window=_object(
            inputs.current_selection_window,
            "CURRENT_SELECTION_WINDOW_INVALID",
        ),
    )


def _strict_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _read_bound_json_file(
    path: Path,
    *,
    maximum_bytes: int,
    code: str,
) -> tuple[dict[str, Any], Path, bytes]:
    """Read one exact bounded file while detecting replacement during read."""

    try:
        resolved = Path(path).resolve(strict=True)
        if not resolved.is_file() or resolved.is_symlink():
            raise ValueError("not a regular file")
        size = resolved.stat().st_size
        if not 2 <= size <= maximum_bytes:
            raise ValueError("byte bound")
        with resolved.open("rb") as handle:
            before = os.fstat(handle.fileno())
            raw = handle.read(maximum_bytes + 1)
            after = os.fstat(handle.fileno())
        if (
            len(raw) != size
            or before.st_size != size
            or after.st_size != size
            or before.st_ino != after.st_ino
        ):
            raise ValueError("file changed")
        parsed = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_strict_pairs,
            parse_constant=_reject_json_constant,
        )
    except Exception as exc:
        raise ProductionAuthorityBlocked(f"{code}_INVALID") from exc
    if type(parsed) is not dict:
        raise ProductionAuthorityBlocked(f"{code}_INVALID")
    return parsed, resolved, raw


def _sha(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX for character in value.lower())
    ):
        raise ProductionAuthorityBlocked(code)
    return value.lower()


def _assert_anchor_plan(
    causal_precommit: Mapping[str, Any], anchors: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    definition = causal_precommit.get("process_definition")
    plan = definition.get("anchor_plan") if type(definition) is dict else None
    maximum = int(causal.pinned_process_policy()["maximum_anchor_count"])
    try:
        bounded = list(islice(iter(anchors), maximum + 1))
    except TypeError as exc:
        raise ProductionAuthorityBlocked(
            "HISTORICAL_CAUSAL_ANCHOR_ROSTER_MISMATCH"
        ) from exc
    if len(bounded) > maximum:
        raise ProductionAuthorityBlocked("HISTORICAL_CAUSAL_ANCHOR_ROSTER_MISMATCH")
    normalized = _snapshot(bounded)
    if (
        type(plan) is not list
        or type(normalized) is not list
        or len(plan) != len(normalized)
    ):
        raise ProductionAuthorityBlocked("HISTORICAL_CAUSAL_ANCHOR_ROSTER_MISMATCH")
    fields = (
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
        "purge_calendar_days",
    )
    projected = [{name: item.get(name) for name in fields} for item in normalized]
    if any(type(item) is not dict for item in plan) or projected != [
        {name: item.get(name) for name in fields} for item in plan
    ]:
        raise ProductionAuthorityBlocked("HISTORICAL_CAUSAL_ANCHOR_GEOMETRY_MISMATCH")
    return normalized


def _owned_book_decision(
    value: Any,
    *,
    expected_receipt_sha256: str | None = None,
    code_prefix: str,
) -> tuple[book.BookMultiplicityDecision, dict[str, Any]]:
    body = _object(value, f"{code_prefix}_BOOK_DECISION_NOT_OBJECT")
    receipt = body.pop("receipt_sha256", None)
    _sha(receipt, f"{code_prefix}_BOOK_RECEIPT_INVALID")
    if expected_receipt_sha256 is not None and receipt != expected_receipt_sha256:
        raise ProductionAuthorityBlocked(f"{code_prefix}_BOOK_RECEIPT_MISMATCH")
    encoded = json.dumps(
        body,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    try:
        decision = book.BookMultiplicityDecision(encoded, receipt)
        verified = book.verify_historical_book_multiplicity_decision(decision)
    except Exception as exc:
        raise ProductionAuthorityBlocked(
            f"{code_prefix}_BOOK_RAW_REVERIFY_FAILED"
        ) from exc
    if verified is not True:
        raise ProductionAuthorityBlocked(f"{code_prefix}_BOOK_RAW_REVERIFY_FAILED")
    return decision, decision.to_dict()


def _verified_book_decision(
    value: Any, *, expected_receipt_sha256: str
) -> dict[str, Any]:
    _, body = _owned_book_decision(
        value,
        expected_receipt_sha256=expected_receipt_sha256,
        code_prefix="HISTORICAL",
    )
    return body


def _run_historical(inputs: HistoricalAuthorityInputs) -> _HistoricalState:
    precommit_payload = _object(
        inputs.causal_precommit_payload, "CAUSAL_PRECOMMIT_NOT_OBJECT"
    )
    anchor_plan = _assert_anchor_plan(precommit_payload, inputs.anchors)
    process_precommit_sha = causal.canonical_sha256(precommit_payload)
    precommit = historical.precommit_historical_anchor_run(
        inputs.context,
        run_id=inputs.run_id,
        process_precommit_receipt_sha256=process_precommit_sha,
        optimizer_producer_release_sha256=_sha(
            inputs.optimizer_producer_release_sha256,
            "OPTIMIZER_PRODUCER_RELEASE_INVALID",
        ),
        anchors=anchor_plan,
    )
    run_receipt = historical.execute_historical_anchor_run(
        inputs.context, precommit, inputs.anchor_producers
    )
    if (
        historical.verify_historical_anchor_run_receipt(
            inputs.context, precommit, run_receipt
        )
        is not True
    ):
        raise ProductionAuthorityBlocked("HISTORICAL_ANCHOR_RUN_REVERIFY_FAILED")
    run_body = _object(run_receipt.to_dict(), "HISTORICAL_RUN_RECEIPT_INVALID")
    anchor_receipts = {
        item["anchor_id"]: item["book_decision_sha256"]
        for item in run_body.get("anchors", [])
        if type(item) is dict
    }
    if list(anchor_receipts) != [item["anchor_id"] for item in anchor_plan]:
        raise ProductionAuthorityBlocked("HISTORICAL_RUN_ANCHOR_ROSTER_MISMATCH")
    request = HistoricalEvidenceRequest(
        _canonical_bytes(
            {
                "schema_version": COORDINATOR_SCHEMA,
                "historical_run_receipt_sha256": run_receipt.receipt_sha256,
                "anchor_book_receipt_sha256_by_id": anchor_receipts,
                "causal_precommit_payload_sha256": process_precommit_sha,
                "authority_claimed": False,
            },
            256 * 1024,
        )
    )
    # The historical run exists only here.  Requiring a preconstructed causal
    # producer would force a caller to pre-run the optimizer or fabricate the
    # run it needs.  Production therefore accepts only the canonical one-shot
    # factory, binds it to this exact context/precommit, and lets it create the
    # issued forward product after the verified historical close.
    try:
        import pm_causal_evidence_producer as causal_producer

        factory = inputs.causal_evidence_factory
        if type(factory) is not (causal_producer.BoundedCausalEvidenceFactory):
            raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_FACTORY_NOT_CANONICAL")
        if (
            factory.context is not inputs.context
            or factory.causal_precommit_payload_sha256 != process_precommit_sha
        ):
            raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_FACTORY_BINDING_MISMATCH")
        producer = factory.build_after_historical_run(
            historical_precommit=precommit,
            historical_run_receipt=run_receipt,
        )
        if type(producer) is not causal_producer.BoundedCausalEvidenceProducer:
            raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_PRODUCER_NOT_CANONICAL")
    except ProductionAuthorityBlocked:
        raise
    except Exception as exc:
        raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_FACTORY_FAILED") from exc
    try:
        product = producer(request)
    except ProductionAuthorityBlocked:
        raise
    except Exception as exc:
        raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_PRODUCTION_FAILED") from exc
    if type(product) is not CausalEvidenceProduct:
        raise ProductionAuthorityBlocked("CAUSAL_EVIDENCE_PRODUCT_INVALID")
    latest_receipt = run_body["anchors"][-1]["book_decision_sha256"]
    historical_book = _verified_book_decision(
        product.historical_book_decision,
        expected_receipt_sha256=latest_receipt,
    )
    evidence = _object(product.evidence_payload, "CAUSAL_EVIDENCE_NOT_OBJECT")
    reconciliation = _object(
        product.reconciliation_payload, "CAUSAL_RECONCILIATION_NOT_OBJECT"
    )
    try:
        decision = causal.evaluate_historical_process(
            precommit_payload, evidence, reconciliation
        )
        if (
            causal.verify_historical_process_decision(
                decision, precommit_payload, evidence, reconciliation
            )
            is not True
        ):
            raise ValueError("not verified")
        decision_body = decision.body()
    except Exception as exc:
        raise ProductionAuthorityBlocked("CAUSAL_PROCESS_RAW_REVERIFY_FAILED") from exc
    if decision_body.get("decision") != "OFFLINE_PROCESS_VALIDATION_PASS":
        raise ProductionAuthorityBlocked(
            "CAUSAL_PROCESS_NOT_PASS",
            tuple(decision_body.get("failure_reasons") or ())
            + tuple(decision_body.get("insufficiency_reasons") or ()),
        )
    expected_capital_phases = {
        f"{row['anchor_id']}|{phase}"
        for row in run_body["anchors"]
        for phase in ("INNER_SELECTION", "OUTER_EVALUATION")
    }
    candidate_map = product.capital_replay_candidates_by_phase
    lineage_map = product.capital_lineage_by_phase
    if (
        type(candidate_map) is not dict
        or type(lineage_map) is not dict
        or set(candidate_map) != expected_capital_phases
        or set(lineage_map) != expected_capital_phases
    ):
        raise ProductionAuthorityBlocked("HISTORICAL_CAPITAL_PHASE_EVIDENCE_INCOMPLETE")
    frozen_candidates: dict[str, tuple[replay.CandidateTrade, ...]] = {}
    for phase_id in sorted(expected_capital_phases):
        try:
            values = list(
                islice(
                    iter(candidate_map[phase_id]),
                    replay.MAX_PORTFOLIO_REPLAY_CANDIDATES + 1,
                )
            )
        except TypeError as exc:
            raise ProductionAuthorityBlocked(
                "HISTORICAL_CAPITAL_CANDIDATES_INVALID", (phase_id,)
            ) from exc
        if (
            not values
            or len(values) > replay.MAX_PORTFOLIO_REPLAY_CANDIDATES
            or any(type(value) is not replay.CandidateTrade for value in values)
        ):
            raise ProductionAuthorityBlocked(
                "HISTORICAL_CAPITAL_CANDIDATES_INVALID", (phase_id,)
            )
        normalized_candidates = tuple(value.normalized() for value in values)
        _require_canonical_margin_candidates(
            normalized_candidates,
            phase_id=phase_id,
        )
        frozen_candidates[phase_id] = normalized_candidates
    frozen_lineages = _snapshot(dict(lineage_map))
    if type(frozen_lineages) is not dict or any(
        type(value) is not dict for value in frozen_lineages.values()
    ):
        raise ProductionAuthorityBlocked("HISTORICAL_CAPITAL_LINEAGE_INVALID")
    return _HistoricalState(
        run_receipt=run_receipt,
        run_body=run_body,
        process_decision=_object(decision_body, "CAUSAL_DECISION_INVALID"),
        precommit_payload=precommit_payload,
        evidence_payload=evidence,
        reconciliation_payload=reconciliation,
        historical_book_decision=historical_book,
        capital_bootstrap_identity_sha256=_sha(
            factory.capital_bootstrap_identity_sha256,
            "HISTORICAL_CAPITAL_BOOTSTRAP_IDENTITY_INVALID",
        ),
        capital_candidates_by_phase=frozen_candidates,
        capital_lineage_by_phase=frozen_lineages,
    )


def _current_management_window_contract(
    value: Mapping[str, Any],
    *,
    current_cutoff: Any,
    config_payload: Mapping[str, Any],
) -> tuple[dict[str, str], float]:
    """Derive the verifier window from the independently frozen anchor plan."""

    if not isinstance(value, Mapping) or set(value) != {"start_date", "end_date"}:
        raise ProductionAuthorityBlocked("CURRENT_SELECTION_WINDOW_SCHEMA_INVALID")
    try:
        start = datetime.fromisoformat(f"{value['start_date']}T00:00:00+00:00")
        end = datetime.fromisoformat(f"{value['end_date']}T00:00:00+00:00")
        cutoff = datetime.fromisoformat(str(current_cutoff).replace("Z", "+00:00"))
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("naive cutoff")
        cutoff = cutoff.astimezone(timezone.utc)
        pipeline = config_payload["pipeline"]
        authority_policy = pipeline["production_capital_authority"]
        constraints = authority_policy["constraints"]
        minimum_current_days = float(constraints["minimum_current_calendar_days"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ProductionAuthorityBlocked("CURRENT_SELECTION_WINDOW_INVALID") from exc
    span_days = (end - start).total_seconds() / 86400.0
    if (
        cutoff.microsecond
        or cutoff.time() != datetime.min.time()
        or start >= end
        or end != cutoff
        or not math.isfinite(span_days)
        or not math.isfinite(minimum_current_days)
        or minimum_current_days <= 0.0
        or span_days + 1e-9 < minimum_current_days
    ):
        raise ProductionAuthorityBlocked("CURRENT_SELECTION_WINDOW_INVALID")
    return (
        {
            "mode": "common_intersection",
            "partition": "current_refit_management_replay",
            "evaluation_start": start.isoformat(),
            "evaluation_end": end.isoformat(),
        },
        span_days,
    )


def _portfolio_validation_policy(
    config_payload: Mapping[str, Any],
    *,
    minimum_calendar_days: float | None = None,
) -> dict[str, Any]:
    pipeline = config_payload.get("pipeline")
    if type(pipeline) is not dict:
        raise ProductionAuthorityBlocked("CONFIG_PIPELINE_NOT_OBJECT")
    try:
        result = {
            "minimum_calendar_days": float(
                pipeline["production_portfolio_replay_min_calendar_days"]
                if minimum_calendar_days is None
                else minimum_calendar_days
            ),
            "minimum_accepted_positions": int(
                pipeline["production_portfolio_replay_min_positions"]
            ),
            "minimum_profit_factor": float(
                pipeline["production_portfolio_replay_min_profit_factor"]
            ),
            "minimum_return_pct": float(
                pipeline["production_portfolio_replay_min_return_pct"]
            ),
            "maximum_drawdown_pct": float(
                pipeline["production_portfolio_replay_max_drawdown_pct"]
            ),
            "require_mtm": pipeline["production_portfolio_replay_require_mtm"],
            "maximum_bootstrap_ruin_upper_95pct": 0.05,
            "maximum_bootstrap_p95_drawdown_pct": float(
                pipeline["production_portfolio_replay_max_drawdown_pct"]
            ),
            "minimum_bootstrap_daily_return_observations": (
                replay.MINIMUM_BOOTSTRAP_DAILY_RETURN_OBSERVATIONS
            ),
        }
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ProductionAuthorityBlocked("PORTFOLIO_VALIDATION_POLICY_INVALID") from exc
    numeric = [
        value
        for key, value in result.items()
        if key not in {"require_mtm", "minimum_accepted_positions"}
    ]
    if (
        set(result) != _PORTFOLIO_VALIDATION_FIELDS
        or type(result["require_mtm"]) is not bool
        or result["require_mtm"] is not True
        or result["minimum_accepted_positions"] < 1
        or any(not math.isfinite(value) for value in numeric)
    ):
        raise ProductionAuthorityBlocked("PORTFOLIO_VALIDATION_POLICY_INVALID")
    return result


def _starting_balance_policies(
    config_payload: Mapping[str, Any],
) -> dict[float, dict[str, Any]]:
    pipeline = config_payload.get("pipeline")
    raw = (
        pipeline.get("production_portfolio_replay_required_starting_balances")
        if type(pipeline) is dict
        else None
    )
    if type(raw) is not list or not 1 <= len(raw) <= cap.MAX_STARTING_BALANCES:
        raise ProductionAuthorityBlocked("PORTFOLIO_STARTING_BALANCE_ROSTER_INVALID")
    balances: list[float] = []
    for value in raw:
        if type(value) not in {int, float} or type(value) is bool:
            raise ProductionAuthorityBlocked("PORTFOLIO_STARTING_BALANCE_INVALID")
        balance = float(value)
        if not math.isfinite(balance) or balance <= 0.0 or balance in balances:
            raise ProductionAuthorityBlocked("PORTFOLIO_STARTING_BALANCE_INVALID")
        balances.append(balance)
    return {
        balance: asdict(
            replay.portfolio_policy_from_config_payload(config_payload, balance)
        )
        for balance in sorted(balances)
    }


def _active_production_environment(
    inputs: AssemblyInputs,
    *,
    expected_native_universe_sha256: str | None = None,
    expected_config_sha256: str | None = None,
) -> dict[str, Any]:
    environment = derive_production_environment_authority(
        active_config_path=inputs.active_config_path,
        config=inputs.config,
        config_payload=inputs.config_payload,
        requested_symbols=inputs.requested_symbols,
        native_universe=inputs.native_universe,
    )
    if expected_native_universe_sha256 is not None and environment[
        "native_universe_sha256"
    ] != _sha(
        expected_native_universe_sha256,
        "GENERATION_NATIVE_UNIVERSE_HASH_INVALID",
    ):
        raise ProductionAuthorityBlocked("GENERATION_NATIVE_UNIVERSE_MISMATCH")
    if expected_config_sha256 is not None and environment["config_sha256"] != _sha(
        expected_config_sha256,
        "GENERATION_CONFIG_HASH_INVALID",
    ):
        raise ProductionAuthorityBlocked("GENERATION_CONFIG_MISMATCH")
    return environment


def _active_native_identity(
    inputs: AssemblyInputs,
    *,
    expected_native_universe_sha256: str | None = None,
) -> dict[str, Any]:
    return _object(
        _active_production_environment(
            inputs,
            expected_native_universe_sha256=expected_native_universe_sha256,
        )["native_identity"],
        "NATIVE_PORTFOLIO_IDENTITY_INVALID",
    )


def _bounded_candidates(
    values: Iterable[replay.CandidateTrade],
) -> tuple[replay.CandidateTrade, ...]:
    try:
        observed = list(
            islice(iter(values), replay.MAX_PORTFOLIO_REPLAY_CANDIDATES + 1)
        )
    except TypeError as exc:
        raise ProductionAuthorityBlocked("CURRENT_REPLAY_CANDIDATES_INVALID") from exc
    if (
        not observed
        or len(observed) > replay.MAX_PORTFOLIO_REPLAY_CANDIDATES
        or any(type(value) is not replay.CandidateTrade for value in observed)
    ):
        raise ProductionAuthorityBlocked("CURRENT_REPLAY_CANDIDATE_ROSTER_INVALID")
    return tuple(observed)


def _require_canonical_margin_candidates(
    values: Sequence[replay.CandidateTrade],
    *,
    phase_id: str,
) -> None:
    if any(
        type(value) is not replay.CandidateTrade
        or value.margin_model != PRODUCTION_MARGIN_MODEL
        for value in values
    ):
        raise ProductionAuthorityBlocked(
            "CAPITAL_CANDIDATE_MARGIN_MODEL_MISMATCH", (phase_id,)
        )


def _bounded_detector_rows(
    values: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    try:
        bounded = list(islice(iter(values), book.SOURCE_GROUP_SIZE + 1))
    except TypeError as exc:
        raise ProductionAuthorityBlocked("DETECTOR_LEDGER_ROSTER_INVALID") from exc
    copied = _snapshot(bounded)
    if (
        type(copied) is not list
        or len(copied) > book.SOURCE_GROUP_SIZE
        or any(type(row) is not dict for row in copied)
    ):
        raise ProductionAuthorityBlocked("DETECTOR_LEDGER_ROSTER_INVALID")
    return tuple(copied)


def _frozen_current_product(value: CurrentRefitProduct) -> CurrentRefitProduct:
    if type(value) is not CurrentRefitProduct:
        raise ProductionAuthorityBlocked("CURRENT_REFIT_PRODUCT_INVALID")
    if not callable(value.portfolio_evidence_producer):
        raise ProductionAuthorityBlocked("PORTFOLIO_EVIDENCE_PRODUCER_NOT_CALLABLE")
    ledger_path, ledger, content_sha, physical_sha = _verified_product_ledger(value)
    return CurrentRefitProduct(
        current_book_decision=_object(
            value.current_book_decision, "CURRENT_BOOK_DECISION_NOT_OBJECT"
        ),
        accepted_book_generation_receipt=_object(
            value.accepted_book_generation_receipt,
            "CURRENT_BOOK_GENERATION_RECEIPT_NOT_OBJECT",
        ),
        current_refit_receipt=_object(
            value.current_refit_receipt, "CURRENT_REFIT_RECEIPT_NOT_OBJECT"
        ),
        raw_manifest_path=Path(value.raw_manifest_path),
        artifact_root=Path(value.artifact_root),
        detector_ledger_records=_bounded_detector_rows(value.detector_ledger_records),
        replay_candidates=value.replay_candidates,
        replayed_cell_ids=tuple(str(item) for item in value.replayed_cell_ids),
        portfolio_evidence_producer=value.portfolio_evidence_producer,
        staged_ledger_path=ledger_path,
        staged_ledger=ledger,
        staged_ledger_content_sha256=content_sha,
        physical_ledger_sha256=physical_sha,
    )


def _verified_product_ledger(
    value: CurrentRefitProduct,
) -> tuple[Path, dict[str, Any], str, str]:
    try:
        path = Path(value.staged_ledger_path).resolve(strict=True)
        size = path.stat().st_size
        if not path.is_file() or not 1 <= size <= MAX_CURRENT_LEDGER_BYTES:
            raise ValueError("size")
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            raw = handle.read(MAX_CURRENT_LEDGER_BYTES + 1)
            after = os.fstat(handle.fileno())
        if (
            len(raw) != size
            or before.st_size != size
            or after.st_size != size
            or before.st_ino != after.st_ino
        ):
            raise ValueError("changed")
        content_sha = hashlib.sha256(raw).hexdigest()
        parsed = json.loads(raw.decode("utf-8"))
        if _canonical_bytes(parsed, MAX_CURRENT_LEDGER_BYTES) != raw:
            raise ValueError("noncanonical")
    except Exception as exc:
        raise ProductionAuthorityBlocked("CURRENT_PHYSICAL_LEDGER_INVALID") from exc
    ledger = _object(parsed, "CURRENT_PHYSICAL_LEDGER_INVALID")
    declared = _object(value.staged_ledger, "CURRENT_PHYSICAL_LEDGER_INVALID")
    physical_sha = replay.canonical_json_sha256(ledger)
    if (
        not ledger
        or ledger != declared
        or content_sha
        != _sha(
            value.staged_ledger_content_sha256,
            "CURRENT_PHYSICAL_LEDGER_CONTENT_SHA_INVALID",
        )
        or physical_sha
        != _sha(
            value.physical_ledger_sha256,
            "CURRENT_PHYSICAL_LEDGER_SHA_INVALID",
        )
    ):
        raise ProductionAuthorityBlocked("CURRENT_PHYSICAL_LEDGER_BINDING_MISMATCH")
    return path, ledger, content_sha, physical_sha


def _run_current(
    historical_state: _HistoricalState,
    inputs: AssemblyInputs,
    producer: CurrentRefitProducer,
    *,
    expected_native_universe_sha256: str | None = None,
    expected_config_sha256: str | None = None,
) -> _CurrentState:
    # _run_current is private, but snapshot again so direct/internal callers
    # receive the same callback isolation as the top-level coordinator.
    inputs = _snapshot_assembly_inputs(inputs)
    if not callable(producer):
        raise ProductionAuthorityBlocked("CURRENT_REFIT_PRODUCER_NOT_CALLABLE")
    request = CurrentRefitRequest(
        _canonical_bytes(
            {
                "schema_version": COORDINATOR_SCHEMA,
                "historical_run_receipt_sha256": (
                    historical_state.run_receipt.receipt_sha256
                ),
                "historical_process_decision_sha256": causal.canonical_sha256(
                    historical_state.process_decision
                ),
                "historical_process_decision": historical_state.process_decision,
                "historical_book_receipt_sha256": historical_state.historical_book_decision[
                    "receipt_sha256"
                ],
                "causal_precommit_payload": historical_state.precommit_payload,
                "causal_evidence_payload": historical_state.evidence_payload,
                "causal_reconciliation_payload": historical_state.reconciliation_payload,
                "authority_claimed": False,
            },
            MAX_COORDINATOR_JSON_BYTES,
        )
    )
    product = _frozen_current_product(producer(request))
    raw_verification = current_execution.verify_raw_current_refit_execution(
        product.current_refit_receipt,
        product.raw_manifest_path,
        artifact_root=product.artifact_root,
    )
    if (
        type(raw_verification) is not dict
        or raw_verification.get("verified") is not True
        or raw_verification.get("errors") != []
    ):
        raise ProductionAuthorityBlocked(
            "CURRENT_REFIT_RAW_REVERIFY_FAILED",
            tuple(
                raw_verification.get("errors", [])
                if type(raw_verification) is dict
                else ()
            ),
        )
    _, current_ledger, _, current_ledger_sha = _verified_product_ledger(product)
    current_book, _ = _owned_book_decision(
        product.current_book_decision,
        code_prefix="CURRENT",
    )
    accepted_payload = product.accepted_book_generation_receipt.get("payload")
    current_cutoff = (
        accepted_payload.get("current_cutoff_utc")
        if type(accepted_payload) is dict
        else None
    )
    expected_management_window, current_window_days = (
        _current_management_window_contract(
            inputs.current_selection_window,
            current_cutoff=current_cutoff,
            config_payload=inputs.config_payload,
        )
    )
    roster, roster_errors = replay.build_current_refit_replay_roster(
        current_ledger,
        inputs.requested_symbols,
        current_book_decision=current_book,
        book_evidence_family=None,
        historical_process_decision=historical_state.process_decision,
        causal_precommit_payload=historical_state.precommit_payload,
        causal_evidence_payload=historical_state.evidence_payload,
        causal_reconciliation_payload=historical_state.reconciliation_payload,
        accepted_book_generation_receipt=product.accepted_book_generation_receipt,
        current_refit_receipt=product.current_refit_receipt,
        as_of_utc=current_cutoff,
        current_refit_raw_manifest=product.raw_manifest_path,
        current_refit_artifact_root=product.artifact_root,
    )
    if roster_errors or roster.get("authoritative") is not True:
        raise ProductionAuthorityBlocked(
            "CURRENT_REFIT_ROSTER_NOT_AUTHORITATIVE", roster_errors
        )
    if roster.get("physical_ledger_sha256") != current_ledger_sha:
        raise ProductionAuthorityBlocked("CURRENT_PHYSICAL_LEDGER_ROSTER_MISMATCH")
    candidates = _bounded_candidates(product.replay_candidates)
    environment = _active_production_environment(
        inputs,
        expected_native_universe_sha256=expected_native_universe_sha256,
        expected_config_sha256=expected_config_sha256,
    )
    native_identity = _object(
        environment["native_identity"], "NATIVE_PORTFOLIO_IDENTITY_INVALID"
    )
    _require_canonical_margin_candidates(
        candidates, phase_id="CURRENT|CURRENT_SELECTION"
    )
    portfolio_lineage = {
        **native_identity,
        "physical_ledger_sha256": roster.get("physical_ledger_sha256"),
        **dict(roster.get("current_refit_lineage") or {}),
    }
    cost_contract = replay.portfolio_cost_contract(inputs.config_payload)
    policies = _starting_balance_policies(inputs.config_payload)
    successor, scenarios = replay.build_current_refit_replay_successor(
        candidates,
        roster,
        replayed_cell_ids=product.replayed_cell_ids,
        expected_lineage=portfolio_lineage,
        expected_cost_contract=cost_contract,
        expected_policies=policies,
    )
    evidence_request = PortfolioEvidenceRequest(
        _canonical_bytes(
            {
                "schema_version": COORDINATOR_SCHEMA,
                "roster": roster,
                "current_refit_successor": successor,
                "scenarios": scenarios,
                "expected_lineage": portfolio_lineage,
                "expected_cost_contract": cost_contract,
                "expected_policies": {
                    str(key): value for key, value in policies.items()
                },
                "authority_claimed": False,
            }
        )
    )
    evidence = _object(
        product.portfolio_evidence_producer(evidence_request),
        "PORTFOLIO_EVIDENCE_NOT_OBJECT",
    )
    replay_body = evidence.get("replay")
    if (
        evidence.get("lineage") != portfolio_lineage
        or evidence.get("roster") != roster
        or type(replay_body) is not dict
        or replay_body.get("window") != expected_management_window
        or replay_body.get("current_refit_successor") != successor
        or replay_body.get("scenarios") != scenarios
    ):
        raise ProductionAuthorityBlocked("PORTFOLIO_EVIDENCE_BINDING_MISMATCH")
    validation_policy = _portfolio_validation_policy(
        inputs.config_payload,
        minimum_calendar_days=current_window_days,
    )
    evidence_errors = replay.validate_funded_portfolio_replay_evidence(
        evidence,
        expected_lineage=portfolio_lineage,
        expected_roster=roster,
        expected_cost_contract=cost_contract,
        expected_policies=policies,
        **validation_policy,
    )
    if evidence_errors:
        raise ProductionAuthorityBlocked(
            "PORTFOLIO_EVIDENCE_RAW_REVERIFY_FAILED", evidence_errors
        )
    return _CurrentState(
        product=product,
        roster=_object(roster, "CURRENT_ROSTER_INVALID"),
        candidates=candidates,
        portfolio_evidence=evidence,
        portfolio_lineage=portfolio_lineage,
        cost_contract=cost_contract,
        policies=policies,
        validation_policy=validation_policy,
        native_identity=native_identity,
        margin_contract_sha256=environment["capital_contract"][
            "margin_contract_sha256"
        ],
        margin_terms_sha256=environment["margin_terms_sha256"],
        broker_terms_sha256=environment["broker_terms_sha256"],
        current_selection_window=_object(
            inputs.current_selection_window,
            "CURRENT_SELECTION_WINDOW_INVALID",
        ),
    )


def _signed_exact_body(
    value: Any, expected_body: Mapping[str, Any], *, code: str
) -> dict[str, Any]:
    envelope = _object(value, code)
    if (
        set(envelope) != {"schema_version", "key_id", "body", "signature_base64"}
        or envelope.get("schema_version") != cap.CAP_SIGNED_ENVELOPE_SCHEMA_VERSION
        or envelope.get("body") != _snapshot(expected_body)
    ):
        raise ProductionAuthorityBlocked(code)
    return envelope


def _capital_replay_contract_sha256() -> str:
    return cap.canonical_json_sha256(
        {
            "portfolio_replay_schema_version": replay.PORTFOLIO_REPLAY_SCHEMA_VERSION,
            "portfolio_path_contract_version": replay.PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION,
            "current_refit_successor_schema_version": (
                replay.CURRENT_REFIT_REPLAY_SUCCESSOR_SCHEMA_VERSION
            ),
            "capital_replay_producer_version": cap_replay.CAP_REPLAY_PRODUCER_VERSION,
        }
    )


def production_margin_contract() -> dict[str, Any]:
    """Describe the exact candidate-margin and portfolio-admission semantics."""

    return {
        "schema_version": "fxpm.production-margin-contract.v1",
        "forward_replay_contract_version": (PRODUCTION_FORWARD_REPLAY_CONTRACT_VERSION),
        "portfolio_replay_schema_version": replay.PORTFOLIO_REPLAY_SCHEMA_VERSION,
        "risk_admission_schema_version": risk_admission.RISK_ADMISSION_SCHEMA_VERSION,
        "candidate_margin_model": PRODUCTION_MARGIN_MODEL,
        "candidate_margin_per_lot_formula": PRODUCTION_MARGIN_FORMULA,
        "candidate_margin_source": "AUTHENTICATED_NATIVE_TICK_CASH",
        "broker_leverage_assumed": False,
        "portfolio_admission_formula": (
            "(current_used_margin+candidate_required_margin)/equity*100"
        ),
        "missing_or_nonpositive_margin_fails_closed": True,
    }


def derive_production_environment_authority(
    *,
    active_config_path: Path,
    config: Any,
    config_payload: Mapping[str, Any],
    requested_symbols: Sequence[str],
    native_universe: Any,
) -> dict[str, Any]:
    """Reopen and derive all pre-evidence native/broker/margin identities.

    The production caller uses this result to freeze its generation and Capital
    bootstrap.  The coordinator calls the same function again before and after
    the long optimization, so callback-supplied hashes can never become truth.
    """

    config_body, config_path, _ = _read_bound_json_file(
        Path(active_config_path),
        maximum_bytes=MAX_BOUND_CONFIG_BYTES,
        code="ACTIVE_DEPLOYMENT_CONFIG",
    )
    supplied_config = _object(config_payload, "ACTIVE_CONFIG_PAYLOAD_INVALID")
    if config_body != supplied_config:
        raise ProductionAuthorityBlocked("ACTIVE_CONFIG_PAYLOAD_MISMATCH")
    configured_path = str(getattr(config, "_deployment_config_path", "") or "").strip()
    try:
        bound_config_path = Path(configured_path).resolve(strict=True)
    except Exception as exc:
        raise ProductionAuthorityBlocked("PIPELINE_CONFIG_SOURCE_UNBOUND") from exc
    if bound_config_path != config_path:
        raise ProductionAuthorityBlocked("PIPELINE_CONFIG_SOURCE_MISMATCH")

    pipeline_payload = supplied_config.get("pipeline")
    declared_data_dir = (
        pipeline_payload.get("data_dir", "./data")
        if type(pipeline_payload) is dict
        else None
    )
    runtime_data_dir = getattr(config, "data_dir", Path("./data"))
    if (
        type(declared_data_dir) is not str
        or not declared_data_dir.strip()
        or not isinstance(runtime_data_dir, (str, Path))
        or not str(runtime_data_dir).strip()
    ):
        raise ProductionAuthorityBlocked("PIPELINE_CONFIG_DATA_DIR_INVALID")

    def bound_data_path(value: str | Path) -> Path:
        raw = Path(value)
        return Path(
            os.path.abspath(raw if raw.is_absolute() else config_path.parent / raw)
        )

    configured_data_dir = bound_data_path(declared_data_dir)
    runtime_data_path = bound_data_path(runtime_data_dir)
    if runtime_data_path != configured_data_dir:
        raise ProductionAuthorityBlocked("PIPELINE_CONFIG_DATA_DIR_MISMATCH")

    symbols = tuple(str(value).strip().upper() for value in requested_symbols)
    if (
        not symbols
        or any(not value for value in symbols)
        or len(symbols) != len(set(symbols))
    ):
        raise ProductionAuthorityBlocked("PRODUCTION_SYMBOL_ROSTER_INVALID")
    contract, errors = replay.active_native_portfolio_contract(
        configured_data_dir,
        symbols,
        timeframes=replay.PORTFOLIO_REPLAY_REQUIRED_TIMEFRAMES,
    )
    if errors:
        raise ProductionAuthorityBlocked("NATIVE_PORTFOLIO_CONTRACT_INVALID", errors)
    contract = _object(contract, "NATIVE_PORTFOLIO_CONTRACT_INVALID")
    native_identity = {
        "native_activation_id": contract.get("activation_id"),
        "native_activation_manifest_sha256": contract.get("activation_manifest_sha256"),
        "native_activation_roster_sha256": contract.get("activation_roster_sha256"),
        "native_bars_sha256_by_source": contract.get("bars_sha256_by_source"),
        "native_sidecar_sha256_by_source": contract.get("sidecar_sha256_by_source"),
    }
    native_identity = _object(native_identity, "NATIVE_PORTFOLIO_IDENTITY_INVALID")
    for name in (
        "native_activation_manifest_sha256",
        "native_activation_roster_sha256",
    ):
        _sha(native_identity.get(name), f"NATIVE_PORTFOLIO_IDENTITY_INVALID:{name}")
    if (
        getattr(native_universe, "activation_id", None)
        != native_identity["native_activation_id"]
        or getattr(native_universe, "activation_manifest_sha256", None)
        != native_identity["native_activation_manifest_sha256"]
    ):
        raise ProductionAuthorityBlocked("NATIVE_UNIVERSE_OBJECT_MISMATCH")

    broker_relative = supplied_config.get("broker_specs_path")
    if type(broker_relative) is not str or not broker_relative.strip():
        raise ProductionAuthorityBlocked("BROKER_TERMS_PATH_UNBOUND")
    broker_path = Path(broker_relative)
    if not broker_path.is_absolute():
        broker_path = config_path.parent / broker_path
    broker_body, broker_path, broker_raw = _read_bound_json_file(
        broker_path,
        maximum_bytes=MAX_BOUND_BROKER_BYTES,
        code="BROKER_TERMS",
    )
    if not broker_body:
        raise ProductionAuthorityBlocked("BROKER_TERMS_INVALID")
    broker_terms_sha256 = hashlib.sha256(broker_raw).hexdigest()

    margin_contract = production_margin_contract()
    margin_contract_sha256 = cap.canonical_json_sha256(margin_contract)
    native_source_contract_sha256 = cap.canonical_json_sha256(native_identity)
    margin_terms = {
        "schema_version": "fxpm.production-margin-terms.v1",
        "margin_contract_sha256": margin_contract_sha256,
        "native_source_contract_sha256": native_source_contract_sha256,
        "native_activation_id": native_identity["native_activation_id"],
        "native_activation_manifest_sha256": native_identity[
            "native_activation_manifest_sha256"
        ],
        "native_sidecar_sha256_by_source": native_identity[
            "native_sidecar_sha256_by_source"
        ],
        "broker_terms_sha256": broker_terms_sha256,
    }
    cost_contract = replay.portfolio_cost_contract(supplied_config)
    capital_contract = {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(cost_contract),
        "native_source_contract_sha256": native_source_contract_sha256,
        "replay_contract_sha256": _capital_replay_contract_sha256(),
        "margin_contract_sha256": margin_contract_sha256,
    }
    return _object(
        {
            "schema_version": "fxpm.production-environment-authority.v1",
            "config_sha256": cap.canonical_json_sha256(supplied_config),
            "native_universe_sha256": replay.canonical_json_sha256(contract),
            "native_contract": contract,
            "native_identity": native_identity,
            "capital_contract": capital_contract,
            "margin_contract": margin_contract,
            "margin_terms": margin_terms,
            "margin_terms_sha256": cap.canonical_json_sha256(margin_terms),
            "broker_terms_path": str(broker_path),
            "broker_terms_sha256": broker_terms_sha256,
        },
        "PRODUCTION_ENVIRONMENT_AUTHORITY_INVALID",
    )


def _expected_current_capital_lineage(
    current: _CurrentState,
    contract: Mapping[str, Any],
    supplied: Mapping[str, Any],
) -> dict[str, Any]:
    if type(contract) is not dict or set(contract) != _CAP_CONTRACT_FIELDS:
        raise ProductionAuthorityBlocked("CAPITAL_CONTRACT_LINEAGE_INVALID")
    exact_contract = _object(contract, "CAPITAL_CONTRACT_LINEAGE_INVALID")
    for name in (
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    ):
        _sha(exact_contract.get(name), f"CAPITAL_CONTRACT_HASH_INVALID:{name}")
    expected_contract = {
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "refit_contract_version": current_execution.CURRENT_REFIT_RECEIPT_VERSION,
        "cost_contract_sha256": cap.canonical_json_sha256(current.cost_contract),
        "native_source_contract_sha256": cap.canonical_json_sha256(
            current.native_identity
        ),
        "replay_contract_sha256": _capital_replay_contract_sha256(),
        "margin_contract_sha256": _sha(
            current.margin_contract_sha256,
            "CURRENT_MARGIN_CONTRACT_HASH_INVALID",
        ),
    }
    for name, expected in expected_contract.items():
        if exact_contract.get(name) != expected:
            raise ProductionAuthorityBlocked(
                "CAPITAL_RELEASE_CONTRACT_MISMATCH", (name,)
            )
    supplied_lineage = _object(supplied, "CAPITAL_CURRENT_LINEAGE_INVALID")
    if set(supplied_lineage) != _CAP_LINEAGE_FIELDS:
        raise ProductionAuthorityBlocked("CAPITAL_CURRENT_LINEAGE_INVALID")
    current_refit = current.roster["current_refit_lineage"]
    expected = {
        **exact_contract,
        "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
        "book_decision_sha256": current.product.current_book_decision.get(
            "receipt_sha256"
        ),
        "book_generation_receipt_sha256": current_refit[
            "accepted_book_generation_receipt_sha256"
        ],
        "refit_receipt_sha256": current_refit["current_refit_receipt_sha256"],
        "refit_execution_sha256": current_refit["current_refit_execution_sha256"],
        "output_book_sha256": current_refit["current_refit_output_book_sha256"],
        "winner_roster_sha256": current_refit["current_selected_winner_roster_sha256"],
        **current.native_identity,
        "margin_terms_sha256": _sha(
            current.margin_terms_sha256,
            "CURRENT_MARGIN_TERMS_HASH_INVALID",
        ),
        "broker_terms_sha256": _sha(
            current.broker_terms_sha256,
            "CURRENT_BROKER_TERMS_HASH_INVALID",
        ),
    }
    for name in (
        "book_decision_sha256",
        "book_generation_receipt_sha256",
        "refit_receipt_sha256",
        "refit_execution_sha256",
        "output_book_sha256",
        "winner_roster_sha256",
        "margin_terms_sha256",
        "broker_terms_sha256",
    ):
        _sha(expected.get(name), f"CAPITAL_CURRENT_LINEAGE_HASH_INVALID:{name}")
    if supplied_lineage != expected:
        changed = sorted(
            name for name in expected if supplied_lineage.get(name) != expected[name]
        )
        raise ProductionAuthorityBlocked("CAPITAL_CURRENT_LINEAGE_MISMATCH", changed)
    return expected


def derive_current_capital_lineage(
    current: _CurrentState,
    contract: Mapping[str, Any],
    *,
    margin_terms_sha256: str,
    broker_terms_sha256: str,
) -> dict[str, Any]:
    """Derive, then independently replay-check, current capital lineage.

    The pre-evidence broker/margin digests are accepted only for exact comparison
    with the coordinator-reopened environment.  The current book, refit, output,
    roster, cost, native and term facts are always coordinator-derived.
    """

    exact_contract = _object(contract, "CAPITAL_CONTRACT_LINEAGE_INVALID")
    current_refit = current.roster.get("current_refit_lineage")
    if type(current_refit) is not dict:
        raise ProductionAuthorityBlocked("CAPITAL_CURRENT_LINEAGE_INVALID")
    expected = {
        **exact_contract,
        "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
        "book_decision_sha256": current.product.current_book_decision.get(
            "receipt_sha256"
        ),
        "book_generation_receipt_sha256": current_refit.get(
            "accepted_book_generation_receipt_sha256"
        ),
        "refit_receipt_sha256": current_refit.get("current_refit_receipt_sha256"),
        "refit_execution_sha256": current_refit.get("current_refit_execution_sha256"),
        "output_book_sha256": current_refit.get("current_refit_output_book_sha256"),
        "winner_roster_sha256": current_refit.get(
            "current_selected_winner_roster_sha256"
        ),
        **current.native_identity,
        "margin_terms_sha256": _sha(
            current.margin_terms_sha256, "CURRENT_MARGIN_TERMS_HASH_INVALID"
        ),
        "broker_terms_sha256": _sha(
            current.broker_terms_sha256, "CURRENT_BROKER_TERMS_HASH_INVALID"
        ),
    }
    if (
        _sha(margin_terms_sha256, "CAPITAL_MARGIN_TERMS_HASH_INVALID")
        != expected["margin_terms_sha256"]
        or _sha(broker_terms_sha256, "CAPITAL_BROKER_TERMS_HASH_INVALID")
        != expected["broker_terms_sha256"]
    ):
        raise ProductionAuthorityBlocked("CAPITAL_ENVIRONMENT_TERMS_MISMATCH")
    return _expected_current_capital_lineage(current, exact_contract, expected)


def _validate_capital_anchor_lineage(
    historical_state: _HistoricalState,
    current: _CurrentState,
    contract: Mapping[str, Any],
    lineage_by_phase: Mapping[str, Mapping[str, Any]],
) -> None:
    results = {row["anchor_id"]: row for row in historical_state.run_body["anchors"]}
    for anchor_id, result in results.items():
        for phase in ("INNER_SELECTION", "OUTER_EVALUATION"):
            phase_id = f"{anchor_id}|{phase}"
            lineage = lineage_by_phase.get(phase_id)
            if type(lineage) is not dict or set(lineage) != _CAP_LINEAGE_FIELDS:
                raise ProductionAuthorityBlocked(
                    "CAPITAL_HISTORICAL_LINEAGE_INVALID", (phase_id,)
                )
            genuine_lineage = historical_state.capital_lineage_by_phase.get(phase_id)
            if type(genuine_lineage) is not dict or lineage != genuine_lineage:
                raise ProductionAuthorityBlocked(
                    "CAPITAL_HISTORICAL_SOURCE_LINEAGE_MISMATCH", (phase_id,)
                )
            if any(lineage.get(name) != contract.get(name) for name in contract):
                raise ProductionAuthorityBlocked(
                    "CAPITAL_HISTORICAL_CONTRACT_MISMATCH", (phase_id,)
                )
            if (
                any(
                    lineage.get(name) != current.native_identity.get(name)
                    for name in (
                        "native_activation_id",
                        "native_activation_manifest_sha256",
                    )
                )
                or lineage.get("margin_terms_sha256") != current.margin_terms_sha256
                or lineage.get("broker_terms_sha256") != current.broker_terms_sha256
            ):
                raise ProductionAuthorityBlocked(
                    "CAPITAL_HISTORICAL_ENVIRONMENT_MISMATCH", (phase_id,)
                )
            if (
                lineage.get("book_role") != "ANCHOR_LOCAL_REFIT"
                or lineage.get("book_decision_sha256") != result["book_decision_sha256"]
                or lineage.get("book_generation_receipt_sha256")
                != result["book_generation_sha256"]
            ):
                raise ProductionAuthorityBlocked(
                    "CAPITAL_HISTORICAL_BOOK_LINEAGE_MISMATCH", (phase_id,)
                )


def _capital_window(precommit_body: Mapping[str, Any]) -> tuple[datetime, datetime]:
    try:
        anchors = precommit_body["anchor_plan"]
        start = datetime.fromisoformat(
            anchors[0]["inner_window"]["start_date"] + "T00:00:00+00:00"
        )
        end = datetime.fromisoformat(
            precommit_body["current_selection_window"]["end_date"] + "T00:00:00+00:00"
        )
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ProductionAuthorityBlocked("CAPITAL_PRECOMMIT_WINDOW_INVALID") from exc
    if start.tzinfo != timezone.utc or end.tzinfo != timezone.utc or end <= start:
        raise ProductionAuthorityBlocked("CAPITAL_PRECOMMIT_WINDOW_INVALID")
    return start, end


def _trusted_keys(value: Mapping[str, bytes], code: str) -> dict[str, bytes]:
    if not isinstance(value, Mapping) or not value:
        raise ProductionAuthorityBlocked(code)
    copied: dict[str, bytes] = {}
    for key_id, public_key in value.items():
        if (
            type(key_id) is not str
            or not key_id
            or type(public_key) is not bytes
            or len(public_key) != 32
            or key_id in copied
        ):
            raise ProductionAuthorityBlocked(code)
        copied[key_id] = bytes(public_key)
    return copied


def _capital_scope_binding(
    *,
    historical_state: _HistoricalState,
    capital_factory: CapitalAuthorityFactory,
    request: CapitalAuthorityRequest,
    inputs: CapitalAuthorityInputs,
    assembly_inputs: AssemblyInputs,
) -> None:
    """Close the DEMO/FUNDED trust boundary before replay or signing."""

    scope = assembly_inputs.authorization_scope
    if type(scope) is not str or scope not in {"DEMO_PROBATION", "FUNDED_LIVE"}:
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORIZATION_SCOPE_INVALID")
    if request.authorization_scope != scope or inputs.authorization_scope != scope:
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORIZATION_SCOPE_BINDING_MISMATCH")
    try:
        capital_factory.require_authorization_scope(scope)
        factory_binding = capital_factory.scope_binding
        signer = inputs.authorization_signer
        signer.require_authorization_scope(scope)
        signer_binding = signer.scope_binding
    except Exception as exc:
        raise ProductionAuthorityBlocked(
            "CAPITAL_AUTHORIZATION_SCOPE_BINDING_MISMATCH"
        ) from exc

    config_sha256 = hashlib.sha256(
        _canonical_bytes(assembly_inputs.config_payload)
    ).hexdigest()
    bootstrap_sha256 = _sha(
        inputs.bootstrap_identity_sha256,
        "CAPITAL_BOOTSTRAP_IDENTITY_INVALID",
    )
    if bootstrap_sha256 != _sha(
        historical_state.capital_bootstrap_identity_sha256,
        "HISTORICAL_CAPITAL_BOOTSTRAP_IDENTITY_INVALID",
    ):
        raise ProductionAuthorityBlocked("CAPITAL_BOOTSTRAP_PHASE_BINDING_MISMATCH")
    input_config_sha256 = _sha(
        inputs.capital_config_sha256,
        "CAPITAL_CONFIG_IDENTITY_INVALID",
    )
    input_public_sha256 = _sha(
        inputs.authority_public_key_sha256,
        "CAPITAL_AUTHORITY_PUBLIC_KEY_IDENTITY_INVALID",
    )
    authority_keys = _trusted_keys(
        inputs.trusted_authority_public_keys,
        "CAPITAL_AUTHORITY_TRUST_ROOT_INVALID",
    )
    authority_public = authority_keys.get(inputs.authority_key_id)
    if len(authority_keys) != 1 or authority_public is None:
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORITY_TRUST_ROOT_INVALID")
    observed_public_sha256 = hashlib.sha256(authority_public).hexdigest()

    expected_binding = {
        "authorization_scope": scope,
        "bootstrap_identity_sha256": bootstrap_sha256,
        "config_sha256": input_config_sha256,
        "authority_key_id": inputs.authority_key_id,
        "authority_public_key_sha256": input_public_sha256,
    }
    for binding in (factory_binding, signer_binding):
        observed = {name: getattr(binding, name, None) for name in expected_binding}
        if observed != expected_binding:
            raise ProductionAuthorityBlocked(
                "CAPITAL_AUTHORIZATION_SCOPE_BINDING_MISMATCH"
            )
    if (
        input_config_sha256 != config_sha256
        or input_public_sha256 != observed_public_sha256
    ):
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORIZATION_SCOPE_BINDING_MISMATCH")

    pipeline = assembly_inputs.config_payload.get("pipeline")
    trust = (
        pipeline.get("funded_authorization_trust") if type(pipeline) is dict else None
    )
    configured_key_id = (
        trust.get("authorization_key_id") if type(trust) is dict else None
    )
    configured_public_sha256 = (
        trust.get("authorization_public_key_sha256") if type(trust) is dict else None
    )
    is_configured_funded_key = (
        configured_key_id == inputs.authority_key_id
        and configured_public_sha256 == observed_public_sha256
    )
    if scope == "FUNDED_LIVE":
        if (
            type(trust) is not dict
            or configured_key_id != inputs.authority_key_id
            or configured_public_sha256 != observed_public_sha256
        ):
            raise ProductionAuthorityBlocked(
                "CAPITAL_AUTHORIZATION_FUNDED_TRUST_MISMATCH"
            )
    elif is_configured_funded_key:
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORIZATION_FUNDED_TRUST_MISMATCH")


def _run_capital(
    historical_state: _HistoricalState,
    current: _CurrentState,
    inputs: CapitalAuthorityInputs,
    validation_time_utc: Any,
) -> _CapitalState:
    if type(inputs.constraints) is not cap.CapCalibrationConstraints:
        raise ProductionAuthorityBlocked("CAPITAL_CONSTRAINTS_INVALID")
    inputs.constraints.validate()
    precommitment_receipt = _object(
        inputs.precommitment_receipt, "CAPITAL_PRECOMMIT_RECEIPT_INVALID"
    )
    precommitment_keys = _trusted_keys(
        inputs.trusted_precommitment_public_keys,
        "CAPITAL_PRECOMMIT_TRUST_ROOT_INVALID",
    )
    replay_keys = _trusted_keys(
        inputs.trusted_replay_public_keys, "CAPITAL_REPLAY_TRUST_ROOT_INVALID"
    )
    authority_keys = _trusted_keys(
        inputs.trusted_authority_public_keys,
        "CAPITAL_AUTHORITY_TRUST_ROOT_INVALID",
    )
    try:
        precommit_body, _ = cap._verified_envelope(
            precommitment_receipt,
            kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
            trusted_public_keys=precommitment_keys,
            missing_reason="CAP_PRECOMMITMENT_MISSING",
        )
    except Exception as exc:
        raise ProductionAuthorityBlocked("CAPITAL_PRECOMMIT_SIGNATURE_INVALID") from exc
    expected_current_window = _object(
        current.current_selection_window,
        "CURRENT_SELECTION_WINDOW_INVALID",
    )
    if (
        not expected_current_window
        or precommit_body.get("current_selection_window") != expected_current_window
    ):
        raise ProductionAuthorityBlocked("CAPITAL_CURRENT_SELECTION_WINDOW_MISMATCH")
    historical_ids = [row["anchor_id"] for row in historical_state.run_body["anchors"]]
    precommit_ids = [
        row.get("anchor_id")
        for row in precommit_body.get("anchor_plan", [])
        if type(row) is dict
    ]
    if precommit_ids != historical_ids:
        raise ProductionAuthorityBlocked("CAPITAL_HISTORICAL_ANCHOR_ROSTER_MISMATCH")
    contract = precommit_body.get("contract_lineage")
    current_phase = "CURRENT|CURRENT_SELECTION"
    supplied_lineages = _snapshot(dict(inputs.lineage_by_phase))
    if type(supplied_lineages) is not dict:
        raise ProductionAuthorityBlocked("CAPITAL_PHASE_LINEAGE_INVALID")
    current_lineage = _expected_current_capital_lineage(
        current,
        contract,
        supplied_lineages.get(current_phase),
    )
    _validate_capital_anchor_lineage(
        historical_state,
        current,
        contract,
        supplied_lineages,
    )
    try:
        frozen = _snapshot(dict(inputs.frozen_candidate_rosters_by_phase))
    except Exception as exc:
        raise ProductionAuthorityBlocked(
            "CAPITAL_CANDIDATE_PHASE_ROSTERS_INVALID"
        ) from exc
    if type(frozen) is not dict:
        raise ProductionAuthorityBlocked("CAPITAL_CANDIDATE_PHASE_ROSTERS_INVALID")
    historical_phase_ids = set(historical_state.capital_candidates_by_phase)
    expected_phase_ids = historical_phase_ids | {current_phase}
    if set(frozen) != expected_phase_ids:
        missing = sorted(expected_phase_ids - set(frozen))
        extra = sorted(set(frozen) - expected_phase_ids)
        raise ProductionAuthorityBlocked(
            "CAPITAL_CANDIDATE_PHASE_ROSTER_MISMATCH",
            tuple(f"MISSING:{phase_id}" for phase_id in missing)
            + tuple(f"EXTRA:{phase_id}" for phase_id in extra),
        )
    for phase_id in sorted(historical_phase_ids):
        try:
            expected_anchor_roster = cap_replay._frozen_roster(
                replay.portfolio_replay_candidate_roster_receipt(
                    historical_state.capital_candidates_by_phase[phase_id]
                )
            )
        except Exception as exc:
            raise ProductionAuthorityBlocked(
                "CAPITAL_HISTORICAL_CANDIDATE_ROSTER_DERIVATION_FAILED",
                (phase_id,),
            ) from exc
        if frozen[phase_id] != expected_anchor_roster:
            raise ProductionAuthorityBlocked(
                "CAPITAL_HISTORICAL_CANDIDATE_ROSTER_MISMATCH",
                (phase_id,),
            )
    try:
        expected_current_roster = cap_replay._frozen_roster(
            replay.portfolio_replay_candidate_roster_receipt(current.candidates)
        )
    except Exception as exc:
        raise ProductionAuthorityBlocked(
            "CAPITAL_CURRENT_CANDIDATE_ROSTER_DERIVATION_FAILED"
        ) from exc
    if frozen.get(current_phase) != expected_current_roster:
        raise ProductionAuthorityBlocked("CAPITAL_CURRENT_CANDIDATE_ROSTER_MISMATCH")
    try:
        unsigned_replay = cap_replay.produce_unsigned_capital_v8_replay_body(
            output_root=inputs.output_root,
            precommitment_receipt=precommitment_receipt,
            trusted_precommitment_public_keys=precommitment_keys,
            constraints=inputs.constraints,
            frozen_candidate_rosters_by_phase=frozen,
            lineage_by_phase=supplied_lineages,
            replay_output_loader=inputs.replay_output_loader,
            generated_at_utc=inputs.generated_at_utc,
            shard_attempt_limit=inputs.shard_attempt_limit,
        )
    except Exception as exc:
        raise ProductionAuthorityBlocked("CAPITAL_REPLAY_PRODUCTION_FAILED") from exc
    signed_replay = _signed_exact_body(
        inputs.replay_evidence_signer(_snapshot(unsigned_replay)),
        unsigned_replay,
        code="CAPITAL_REPLAY_EXTERNAL_SIGNATURE_INVALID",
    )
    start, end = _capital_window(precommit_body)
    pending = cap.calibrate_total_stop_risk_cap(
        candidates=(),
        base_policies={},
        evaluation_start=start,
        evaluation_end=end,
        lineage=current_lineage,
        constraints=inputs.constraints,
        replay_evidence_root=inputs.output_root,
        precommitment_receipt=precommitment_receipt,
        replay_evidence_receipt=signed_replay,
        trusted_precommitment_public_keys=precommitment_keys,
        trusted_replay_public_keys=replay_keys,
    )
    selection = pending.get("selection") if type(pending) is dict else None
    if (
        type(selection) is not dict
        or selection.get("status") != cap.EVIDENCE_SELECTED_PENDING_AUTHORIZATION
    ):
        failures = (
            selection.get("identification_failures", [])
            if type(selection) is dict
            else []
        )
        raise ProductionAuthorityBlocked("CAPITAL_EVIDENCE_NOT_SELECTED", failures)
    try:
        authorization_body = cap.build_capital_envelope_authorization_body(
            pending,
            authority_key_id=inputs.authority_key_id,
            selected_policy_point_id=selection["selected_policy_point_id"],
            selected_cap_pct=selection["selected_max_total_stop_risk_pct"],
            issued_at_utc=inputs.authorization_issued_at_utc,
            expires_at_utc=inputs.authorization_expires_at_utc,
            replay_evidence_root=inputs.output_root,
            trusted_replay_public_keys=replay_keys,
            precommitment_receipt=precommitment_receipt,
            replay_evidence_receipt=signed_replay,
            trusted_precommitment_public_keys=precommitment_keys,
            expected_deployment_lineage=current_lineage,
            expected_constraints=inputs.constraints,
        )
        signed_authorization = _signed_exact_body(
            inputs.authorization_signer(_snapshot(authorization_body)),
            authorization_body,
            code="CAPITAL_AUTHORIZATION_EXTERNAL_SIGNATURE_INVALID",
        )
        artifact = cap.attach_capital_envelope_authorization(
            pending,
            signed_authorization,
            trusted_public_keys=authority_keys,
            validation_time_utc=validation_time_utc,
            replay_evidence_root=inputs.output_root,
            trusted_replay_public_keys=replay_keys,
            precommitment_receipt=precommitment_receipt,
            replay_evidence_receipt=signed_replay,
            trusted_precommitment_public_keys=precommitment_keys,
            expected_deployment_lineage=current_lineage,
            expected_constraints=inputs.constraints,
        )
    except ProductionAuthorityBlocked:
        raise
    except Exception as exc:
        raise ProductionAuthorityBlocked(
            "CAPITAL_EXTERNAL_AUTHORIZATION_INVALID"
        ) from exc
    errors = cap.validate_cap_calibration_artifact(
        artifact,
        expected_lineage=current_lineage,
        expected_selected_cap_pct=artifact["selection"][
            "selected_max_total_stop_risk_pct"
        ],
        expected_selected_policy_point_id=artifact["selection"][
            "selected_policy_point_id"
        ],
        expected_constraints=inputs.constraints,
        trusted_authority_public_keys=authority_keys,
        replay_evidence_root=inputs.output_root,
        trusted_replay_public_keys=replay_keys,
        validation_time_utc=validation_time_utc,
        precommitment_receipt=precommitment_receipt,
        replay_evidence_receipt=signed_replay,
        trusted_precommitment_public_keys=precommitment_keys,
        require_external_authorization=True,
    )
    if errors:
        raise ProductionAuthorityBlocked("CAPITAL_ARTIFACT_RAW_REVERIFY_FAILED", errors)
    expected_lineage = {
        **current_lineage,
        "replay_output_sha256": cap.canonical_json_sha256(current.portfolio_evidence),
    }
    return _CapitalState(
        artifact=_object(artifact, "CAPITAL_ARTIFACT_INVALID"),
        expected_lineage=expected_lineage,
        signed_replay_evidence=signed_replay,
        current_lineage=current_lineage,
        precommitment_receipt=precommitment_receipt,
        trusted_authority_public_keys=authority_keys,
        trusted_replay_public_keys=replay_keys,
        trusted_precommitment_public_keys=precommitment_keys,
    )


def _trusted_validation_time_utc() -> str:
    """Sample the wall clock at the lazy Capital boundary, never at run start."""

    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sample_post_current_validation_time() -> str:
    raw = _trusted_validation_time_utc()
    if type(raw) is not str:
        raise ProductionAuthorityBlocked("VALIDATION_CLOCK_INVALID")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionAuthorityBlocked("VALIDATION_CLOCK_INVALID") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProductionAuthorityBlocked("VALIDATION_CLOCK_INVALID")
    canonical = (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    if canonical != raw:
        raise ProductionAuthorityBlocked("VALIDATION_CLOCK_INVALID")
    return canonical


def coordinate_production_authority(
    *,
    historical_inputs: HistoricalAuthorityInputs,
    current_refit_factory: CurrentRefitProducer,
    capital_factory: CapitalAuthorityFactory,
    assembly_inputs: AssemblyInputs,
) -> ProductionAuthorityRun:
    """Recompute the full chain and publish a verified pre-CAS assembly.

    The caller must invoke this before any staged-ledger compare-and-swap.  The
    returned directory contains ``authority_assembly.json``, seven authority
    indexes, and a chain receipt; it is not an active ledger generation.
    """

    if type(assembly_inputs) is not AssemblyInputs:
        raise ProductionAuthorityBlocked("ASSEMBLY_INPUTS_INVALID")
    if assembly_inputs.validation_time_utc is not None:
        raise ProductionAuthorityBlocked("CALLER_VALIDATION_TIME_FORBIDDEN")
    # Freeze all JSON-shaped authority inputs before historical/current/capital
    # factories execute.  A callback may hold a reference to the caller's
    # mutable mappings, but it cannot redefine the facts verified here.
    assembly_inputs = _snapshot_assembly_inputs(assembly_inputs)
    generation_native_universe_sha256 = _sha(
        getattr(historical_inputs.context, "native_universe_sha256", None),
        "GENERATION_NATIVE_UNIVERSE_HASH_INVALID",
    )
    generation_config_sha256 = _sha(
        getattr(historical_inputs.context, "config_sha256", None),
        "GENERATION_CONFIG_HASH_INVALID",
    )
    generation_environment = _active_production_environment(
        assembly_inputs,
        expected_native_universe_sha256=generation_native_universe_sha256,
        expected_config_sha256=generation_config_sha256,
    )
    historical_state = _run_historical(historical_inputs)
    current = _run_current(
        historical_state,
        assembly_inputs,
        current_refit_factory,
        expected_native_universe_sha256=generation_native_universe_sha256,
        expected_config_sha256=generation_config_sha256,
    )
    validation_time_utc = _sample_post_current_validation_time()
    if not callable(capital_factory):
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORITY_FACTORY_NOT_CALLABLE")
    request = CapitalAuthorityRequest(
        _historical_state=historical_state,
        _current_state=current,
        validation_time_utc=validation_time_utc,
        authorization_scope=assembly_inputs.authorization_scope,
    )
    capital_inputs = capital_factory(request)
    if type(capital_inputs) is not CapitalAuthorityInputs:
        raise ProductionAuthorityBlocked("CAPITAL_AUTHORITY_INPUTS_INVALID")
    _capital_scope_binding(
        historical_state=historical_state,
        capital_factory=capital_factory,
        request=request,
        inputs=capital_inputs,
        assembly_inputs=assembly_inputs,
    )
    capital_state = _run_capital(
        historical_state,
        current,
        capital_inputs,
        validation_time_utc,
    )
    final_environment = _active_production_environment(
        assembly_inputs,
        expected_native_universe_sha256=generation_native_universe_sha256,
        expected_config_sha256=generation_config_sha256,
    )
    if final_environment != generation_environment:
        raise ProductionAuthorityBlocked(
            "PRODUCTION_ENVIRONMENT_CHANGED_DURING_AUTHORITY_RUN"
        )
    try:
        detector_rows = _snapshot(
            list(
                islice(
                    iter(current.product.detector_ledger_records),
                    book.SOURCE_GROUP_SIZE + 1,
                )
            )
        )
    except TypeError as exc:
        raise ProductionAuthorityBlocked("DETECTOR_LEDGER_ROSTER_INVALID") from exc
    if (
        type(detector_rows) is not list
        or len(detector_rows) > book.SOURCE_GROUP_SIZE
        or any(type(row) is not dict for row in detector_rows)
    ):
        raise ProductionAuthorityBlocked("DETECTOR_LEDGER_ROSTER_INVALID")
    raw = authority_assembly.RawAuthorityInputs(
        detector_ledger_records=detector_rows,
        historical_process_decision=historical_state.process_decision,
        causal_precommit_payload=historical_state.precommit_payload,
        causal_evidence_payload=historical_state.evidence_payload,
        causal_reconciliation_payload=historical_state.reconciliation_payload,
        historical_book_decision=historical_state.historical_book_decision,
        accepted_book_generation_receipt=(
            current.product.accepted_book_generation_receipt
        ),
        current_book_decision=current.product.current_book_decision,
        current_refit_receipt=current.product.current_refit_receipt,
        current_refit_raw_manifest_path=current.product.raw_manifest_path,
        current_refit_artifact_root=current.product.artifact_root,
        portfolio_replay_evidence=current.portfolio_evidence,
        portfolio_expected_lineage=current.portfolio_lineage,
        portfolio_expected_policies=current.policies,
        portfolio_validation_policy=current.validation_policy,
        capital_artifact=capital_state.artifact,
        capital_expected_lineage=capital_state.expected_lineage,
        capital_constraints=asdict(capital_inputs.constraints),
        capital_expected_selected_policy_point_id=capital_state.artifact["selection"][
            "selected_policy_point_id"
        ],
        capital_replay_evidence_root=capital_inputs.output_root,
        capital_trusted_authority_public_keys=(
            capital_state.trusted_authority_public_keys
        ),
        capital_trusted_replay_public_keys=(capital_state.trusted_replay_public_keys),
        capital_trusted_precommitment_public_keys=(
            capital_state.trusted_precommitment_public_keys
        ),
        capital_precommitment_receipt=capital_state.precommitment_receipt,
        capital_replay_evidence_receipt=capital_state.signed_replay_evidence,
    )
    (
        staged_ledger_path,
        staged_ledger,
        staged_ledger_content_sha256,
        physical_ledger_sha256,
    ) = _verified_product_ledger(current.product)
    assembly = authority_assembly.assemble_authority_directory(
        deployment_root=assembly_inputs.deployment_root,
        target_name=assembly_inputs.target_name,
        generation_id=assembly_inputs.generation_id,
        authorization_scope=assembly_inputs.authorization_scope,
        active_config_path=assembly_inputs.active_config_path,
        config=assembly_inputs.config,
        config_payload=assembly_inputs.config_payload,
        staged_ledger_path=staged_ledger_path,
        ledger=staged_ledger,
        requested_symbols=assembly_inputs.requested_symbols,
        native_universe=assembly_inputs.native_universe,
        validation_time_utc=validation_time_utc,
        raw=raw,
    )
    return ProductionAuthorityRun(
        schema_version=COORDINATOR_SCHEMA,
        assembly=assembly,
        staged_ledger_path=staged_ledger_path,
        staged_ledger_content_sha256=staged_ledger_content_sha256,
        physical_ledger_sha256=physical_ledger_sha256,
        historical_run_receipt_sha256=(historical_state.run_receipt.receipt_sha256),
        historical_process_decision_sha256=causal.canonical_sha256(
            historical_state.process_decision
        ),
        current_roster_sha256=current.roster["roster_sha256"],
        capital_decision_sha256=capital_state.artifact["decision_sha256"],
        selected_policy_point_id=capital_state.artifact["selection"][
            "selected_policy_point_id"
        ],
    )


def production_authority_integration_contract() -> dict[str, Any]:
    """Describe the only supported pre-CAS pipeline binding."""

    return {
        "schema_version": COORDINATOR_SCHEMA,
        "entrypoint": (
            "coordinate_production_authority(*, historical_inputs, "
            "current_refit_factory, capital_factory, assembly_inputs)"
        ),
        "raw_callbacks": [
            "HistoricalAuthorityInputs.anchor_producers: anchor-local optimizer IO",
            "HistoricalAuthorityInputs.causal_evidence_factory: canonical post-run forward replay IO",
            "current_refit_factory: invoked only after causal PASS",
            "CurrentRefitProduct.portfolio_evidence_producer: raw source ledger packaging",
            "capital_factory: invoked only after verified current physical ledger",
            "derived CapitalAuthorityInputs.replay_output_loader: synchronized replay computation",
            "CapitalAuthorityInputs.replay_evidence_signer: external Ed25519 IO",
            "CapitalAuthorityInputs.authorization_signer: external Ed25519 IO",
        ],
        "callback_authority_facts_permitted": False,
        "caller_validation_time_permitted": False,
        "authorization_validation_clock": (
            "SAMPLED_ONCE_AFTER_CURRENT_REFIT_BEFORE_CAPITAL_FACTORY"
        ),
        "production_environment_authority": (
            "REOPENED_BEFORE_HISTORICAL_AFTER_CURRENT_AND_BEFORE_ASSEMBLY"
        ),
        "caller_native_broker_or_margin_hash_permitted": False,
        "production_factory_status": "NOT_WIRED",
        "pipeline_callsite": (
            "after the coordinator-derived current ledger and all raw artifacts are frozen; before "
            "_promote_staged_generation or any ledger compare-and-swap"
        ),
        "returned_run_fields": [
            "assembly",
            "staged_ledger_path",
            "staged_ledger_content_sha256",
            "physical_ledger_sha256",
            "historical_run_receipt_sha256",
            "historical_process_decision_sha256",
            "current_roster_sha256",
            "capital_decision_sha256",
            "selected_policy_point_id",
        ],
        "assembly_fields": [
            "directory",
            "metadata_path",
            "index_paths",
            "descriptors_canonical_json",
            "chain_receipt_canonical_json",
            "verification_result_canonical_json",
        ],
        "activates_ledger_or_places_trades": False,
    }


__all__ = [
    "AssemblyInputs",
    "CapitalAuthorityRequest",
    "CapitalAuthorityInputs",
    "CausalEvidenceProduct",
    "COORDINATOR_SCHEMA",
    "CurrentRefitProduct",
    "HistoricalAuthorityInputs",
    "PortfolioEvidenceRequest",
    "ProductionAuthorityBlocked",
    "ProductionAuthorityRun",
    "coordinate_production_authority",
    "derive_current_capital_lineage",
    "derive_production_environment_authority",
    "production_margin_contract",
    "production_authority_integration_contract",
]
