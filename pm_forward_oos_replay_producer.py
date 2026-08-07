"""Genuine repeated-anchor forward-OOS synchronized replay production.

This module is an evidence producer, never an authority.  It reopens a complete
historical-anchor run, independently replays each selected Winner V11 decision,
materializes only its frozen strategy/detector process, executes that process on
verifier-sealed native data that extends the anchor, and emits the six raw
ledgers consumed by :class:`pm_causal_evidence_producer.BoundedCausalEvidenceProducer`.

No return vector, selected parameter, fill, risk limit, margin amount, or clock
is accepted from a caller.  A failed replay leaves all prior production books
untouched; this module has no publication or trading capability.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import secrets
from typing import Any, Final, Mapping, Sequence

import numpy as np
import pandas as pd

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal_producer
import pm_causal_process as causal
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as historical_adapter
import pm_native_prefix_production as native_prefix
import pm_portfolio_replay as portfolio
import pm_production_capital_bootstrap as capital_bootstrap
import pm_regime
import pm_winner_selection as winner_v11
from pm_core import (
    BACKTEST_POSITION_EVENT_SCHEMA_VERSION,
    Backtester,
    FeatureComputer,
    InstrumentSpec,
    PipelineConfig,
    build_backtest_position_events,
)
from pm_current_refit_parameters import decode_parameter_mapping
from pm_strategies import StrategyRegistry


UTC: Final = timezone.utc
PRODUCTION_CONTRACT_VERSION: Final = "fxpm.forward-oos-replay-producer.v3"
REPLAY_PRECOMMIT_SCHEMA: Final = "fxpm.forward-oos-replay-precommit.v3"
PRODUCER_RECEIPT_SCHEMA: Final = "fxpm.forward-oos-replay-producer-receipt.v3"
CAPITAL_CANDIDATE_CHUNK_SCHEMA: Final = (
    "fxpm.forward-oos-capital-candidate-chunk.v1"
)
CAPITAL_PHASE_MANIFEST_SCHEMA: Final = "fxpm.forward-oos-capital-phase-manifest.v1"
CAPITAL_INNER_SELECTION: Final = "INNER_SELECTION"
CAPITAL_OUTER_EVALUATION: Final = "OUTER_EVALUATION"
CAPITAL_PHASES: Final = (CAPITAL_INNER_SELECTION, CAPITAL_OUTER_EVALUATION)
CAPITAL_SELECTION_CALENDAR_DAYS: Final = 365
MARGIN_MODEL: Final = "AUTHENTICATED_TICK_CASH_NOTIONAL_100PCT_V1"
POSITION_SOURCE_SCHEMA: Final = "fxpm.forward-oos-backtester-position.v1"
VALUATION_CONTRACT: Final = (
    "AUTHENTICATED_NATIVE_M5_OPEN_EXECUTABLE_BID_ASK_FULL_ROUND_TRIP_COST_V1"
)

_HEX64: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SAFE_ID: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$", re.ASCII)
_DESCRIPTOR_FIELDS: Final = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
)
_PRODUCER_RECEIPT_FIELDS: Final = frozenset(
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
_REPLAY_PRECOMMIT_FIELDS: Final = frozenset(
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
_CAPITAL_CANDIDATE_CHUNK_FIELDS: Final = frozenset(
    {
        "schema_version",
        "phase_id",
        "chunk_index",
        "candidate_count",
        "candidate_records",
        "candidate_record_sha256s",
        "candidate_record_roster_sha256",
        "complete_records_not_policy_filtered",
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
_CAPITAL_PHASE_MANIFEST_FIELDS: Final = frozenset(
    {
        "schema_version",
        "production_contract_version",
        "phase_id",
        "anchor_id",
        "phase",
        "window",
        "historical_run_receipt_sha256",
        "forward_replay_precommit_receipt_sha256",
        "capital_bootstrap_identity_sha256",
        "selected_specialist_roster_sha256",
        "selected_cell_ids",
        "candidate_chunk_descriptors",
        "candidate_chunk_descriptor_roster_sha256",
        "candidate_count",
        "candidate_record_sha256s_sha256",
        "candidate_roster_receipt",
        "operational_lineage",
        "complete_pre_policy_candidate_roster",
        "candidate_marks_thinned_or_sampled",
        "confers_capital_publication_or_trading_authority",
    }
)
_MAX_JSON_BYTES: Final = 64 * 1024 * 1024
_MAX_LEDGER_BYTES_PER_ANCHOR: Final = 64 * 1024 * 1024
_MAX_LEDGER_BYTES_TOTAL: Final = 256 * 1024 * 1024
_MAX_CANDIDATE_POSITIONS_PER_ANCHOR: Final = 10_000
_MAX_WINNERS_PER_ANCHOR: Final = book.FAMILY_SIZE
_MAX_NATIVE_ROWS_PER_ANCHOR: Final = 25_000_000
_MAX_NATIVE_BYTES_PER_ANCHOR: Final = 8 * 1024 * 1024 * 1024
_MAX_ACTIVE_SYMBOL_NATIVE_BYTES: Final = 512 * 1024 * 1024
_MAX_CANDIDATE_MARKS_PER_ANCHOR: Final = 2_000_000
_MAX_CANDIDATE_MARK_BYTES_PER_ANCHOR: Final = 256 * 1024 * 1024
_ESTIMATED_CANDIDATE_MARK_BYTES: Final = 128
_MAX_CAPITAL_CANDIDATE_CHUNK_BYTES: Final = 64 * 1024 * 1024
_MAX_CAPITAL_CANDIDATE_RECORD_BYTES: Final = 48 * 1024 * 1024
_MAX_CAPITAL_PHASE_RECORD_BYTES: Final = 512 * 1024 * 1024
_MAX_CAPITAL_PHASE_CHUNKS: Final = 10_000
_DECIMAL_QUANTUM: Final = Decimal("0.000000000001")
_REGIME_COLUMNS: Final = (
    "TREND_SCORE",
    "RANGE_SCORE",
    "BREAKOUT_SCORE",
    "CHOP_SCORE",
    "REGIME_RAW",
    "REGIME",
    "REGIME_STRENGTH",
    "REGIME_GAP",
    "REGIME_WARMUP",
    "REGIME_LIVE",
    "REGIME_STRENGTH_LIVE",
)
_EOD_EXIT_REASONS: Final = frozenset({"end_of_data", "closed_eod", "eod"})


class ForwardOOSReplayProductionError(RuntimeError):
    """Typed fail-closed production error; never an economic verdict."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ForwardOOSReplayProductionError(code, details)


def _canonical_bytes(value: Any, maximum: int = _MAX_JSON_BYTES) -> bytes:
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForwardOOSReplayProductionError("CANONICAL_JSON_INVALID") from exc
    if len(raw) > maximum:
        _fail("CANONICAL_JSON_BOUND_EXCEEDED", len(raw), maximum)
    return raw


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _hex(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        _fail(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ForwardOOSReplayProductionError(code) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        _fail(code)
    return result.astimezone(UTC)


def _utc_seconds(value: Any, code: str) -> str:
    return _utc(value, code).isoformat(timespec="seconds").replace("+00:00", "Z")


def _date(value: Any, code: str) -> date:
    if type(value) is not str or len(value) != 10:
        _fail(code)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ForwardOOSReplayProductionError(code) from exc


def _capital_phase_windows(
    plans: Sequence[Mapping[str, Any]],
    capital_binding: Mapping[str, Any],
) -> dict[str, dict[str, dict[str, str]]]:
    """Derive the pre-evidence Capital-v8 windows from causal anchor geometry.

    ``end_date`` is exclusive.  The inner lane is deliberately a selection
    lane: it ends at the anchor-local search cutoff and is never used as outer
    evidence.  The outer lane is the already-purged historical forward window.
    No caller dates are accepted at this seam.
    """

    if not isinstance(plans, Sequence) or isinstance(
        plans, (str, bytes, bytearray)
    ):
        _fail("CAPITAL_PHASE_ANCHOR_PLAN_INVALID")
    if type(capital_binding) is not dict:
        _fail("CAPITAL_PHASE_BINDING_INVALID")
    capital_anchors = capital_binding.get("anchor_plan")
    if type(capital_anchors) is not list or len(capital_anchors) != len(plans):
        _fail("CAPITAL_PHASE_ANCHOR_PLAN_INVALID")
    result: dict[str, dict[str, dict[str, str]]] = {}
    for raw, capital_anchor in zip(plans, capital_anchors, strict=True):
        if type(raw) is not dict:
            _fail("CAPITAL_PHASE_ANCHOR_PLAN_INVALID")
        anchor_id = _safe_id(raw.get("anchor_id"), "CAPITAL_PHASE_ANCHOR_ID_INVALID")
        if anchor_id in result:
            _fail("CAPITAL_PHASE_ANCHOR_ID_REUSED", anchor_id)
        if (
            type(capital_anchor) is not dict
            or set(capital_anchor) != {"anchor_id", "inner_window", "outer_window"}
            or capital_anchor.get("anchor_id") != anchor_id
        ):
            _fail("CAPITAL_PHASE_ANCHOR_PLAN_INVALID", anchor_id)
        cutoff = _utc(
            raw.get("selection_cutoff_utc"), "CAPITAL_INNER_CUTOFF_INVALID"
        )
        if cutoff.time() != datetime.min.time():
            _fail("CAPITAL_INNER_CUTOFF_NOT_UTC_MIDNIGHT", anchor_id)
        inner = capital_anchor.get("inner_window")
        outer = capital_anchor.get("outer_window")
        inner_start_stamp, inner_end_stamp = _capital_window_datetimes(
            inner, phase_id=_capital_phase_id(anchor_id, CAPITAL_INNER_SELECTION)
        )
        outer_start_stamp, outer_end_stamp = _capital_window_datetimes(
            outer, phase_id=_capital_phase_id(anchor_id, CAPITAL_OUTER_EVALUATION)
        )
        inner_start = inner_start_stamp.date()
        inner_end = inner_end_stamp.date()
        outer_start = _date(
            raw.get("forward_start_date"), "CAPITAL_OUTER_START_INVALID"
        )
        outer_end = _date(
            raw.get("forward_end_date"), "CAPITAL_OUTER_END_INVALID"
        ) + timedelta(days=1)
        if (
            inner_end != cutoff.date()
            or inner_end > outer_start
            or (inner_end - inner_start).days != CAPITAL_SELECTION_CALENDAR_DAYS
            or outer_start_stamp.date() != outer_start
            or outer_end_stamp.date() != outer_end
        ):
            _fail("CAPITAL_PHASE_WINDOW_GEOMETRY_INVALID", anchor_id)
        result[anchor_id] = {
            CAPITAL_INNER_SELECTION: dict(inner),
            CAPITAL_OUTER_EVALUATION: dict(outer),
        }
    if not result:
        _fail("CAPITAL_PHASE_ANCHOR_PLAN_EMPTY")
    return json.loads(_canonical_bytes(result).decode("ascii"))


def _capital_phase_id(anchor_id: str, phase: str) -> str:
    if phase not in CAPITAL_PHASES:
        _fail("CAPITAL_PHASE_INVALID", phase)
    return f"{_safe_id(anchor_id, 'CAPITAL_PHASE_ANCHOR_ID_INVALID')}|{phase}"


def _capital_window_datetimes(
    value: Mapping[str, Any], *, phase_id: str
) -> tuple[pd.Timestamp, pd.Timestamp]:
    if type(value) is not dict or set(value) != {"start_date", "end_date"}:
        _fail("CAPITAL_PHASE_WINDOW_INVALID", phase_id)
    start = _date(value.get("start_date"), "CAPITAL_PHASE_START_INVALID")
    end = _date(value.get("end_date"), "CAPITAL_PHASE_END_INVALID")
    if end <= start:
        _fail("CAPITAL_PHASE_WINDOW_INVALID", phase_id)
    return pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")


def _decimal(value: Any, code: str, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError, OverflowError) as exc:
        raise ForwardOOSReplayProductionError(code) from exc
    if not result.is_finite() or (positive and result <= 0):
        _fail(code)
    return result


def _decimal_text(value: Any, code: str = "DECIMAL_INVALID") -> str:
    number = _decimal(value, code)
    try:
        quantized = number.quantize(_DECIMAL_QUANTUM, rounding=ROUND_HALF_EVEN)
    except InvalidOperation as exc:
        raise ForwardOOSReplayProductionError(code) from exc
    text = format(quantized, "f").rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _descriptor(value: Any, code: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _DESCRIPTOR_FIELDS:
        _fail(code)
    if (
        value.get("schema_version") != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA
        or type(value.get("relative_path")) is not str
        or not value["relative_path"]
        or type(value.get("size_bytes")) is not int
        or value["size_bytes"] <= 0
        or type(value.get("format")) is not str
        or not value["format"]
    ):
        _fail(code)
    _hex(value.get("sha256"), code)
    return json.loads(_canonical_bytes(value, 64 * 1024).decode("ascii"))


def _read_descriptor_json(
    context: authority_spool.GenerationSpoolContext,
    value: Any,
    *,
    code: str,
    maximum: int | None = None,
    allowed_formats: Sequence[str] = (
        authority_spool.CANONICAL_JSON_FORMAT,
        "fxpm-ascii-json-v1",
    ),
) -> dict[str, Any]:
    descriptor = _descriptor(value, f"{code}_DESCRIPTOR_INVALID")
    limit = int(maximum or context.limits.max_json_bytes)
    if (
        descriptor["format"] not in tuple(allowed_formats)
        or descriptor["size_bytes"] > limit
    ):
        _fail(f"{code}_FORMAT_OR_BOUND_INVALID")
    try:
        root = Path(context.root).resolve(strict=True)
        path = root.joinpath(*Path(descriptor["relative_path"]).parts).resolve(
            strict=True
        )
        path.relative_to(root)
        if not path.is_file() or path.is_symlink():
            _fail(f"{code}_OBJECT_INVALID")
        raw = path.read_bytes()
    except ForwardOOSReplayProductionError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_UNAVAILABLE") from exc
    if (
        len(raw) != descriptor["size_bytes"]
        or len(raw) > limit
        or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]
    ):
        _fail(f"{code}_INTEGRITY_MISMATCH")
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_JSON_INVALID") from exc
    if type(body) is not dict or _canonical_bytes(body, limit) != raw:
        _fail(f"{code}_CANONICAL_MISMATCH")
    return body


def _frame_sha256(frame: pd.DataFrame) -> str:
    try:
        hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype="<u8")
    except Exception as exc:
        raise ForwardOOSReplayProductionError("FIT_FRAME_HASH_FAILED") from exc
    digest = hashlib.sha256()
    digest.update(b"fxpm-authoritative-search-frame-v1\x00")
    digest.update(_canonical_bytes([str(value) for value in frame.columns]))
    digest.update(hashed.tobytes(order="C"))
    return digest.hexdigest()


def _product_issuance_payload(
    anchor_rows: Sequence[tuple[str, causal_producer.AnchorReplayArtifactDescriptors]],
    producer_receipt_json: bytes,
    producer_receipt_descriptor: Mapping[str, Any],
) -> bytes:
    return _canonical_bytes(
        {
            "schema": "fxpm.forward-oos-replay-issued-product.v1",
            "producer_receipt_sha256": hashlib.sha256(
                producer_receipt_json
            ).hexdigest(),
            "producer_receipt_size_bytes": len(producer_receipt_json),
            "producer_receipt_descriptor": dict(producer_receipt_descriptor),
            "anchor_rows": [
                {
                    "anchor_id": anchor_id,
                    "ledger_descriptors": {
                        role: dict(value)
                        for role, value in descriptors.descriptors()
                    },
                }
                for anchor_id, descriptors in anchor_rows
            ],
        },
        _MAX_JSON_BYTES,
    )


def _make_precommit_issuance_capability():
    key = secrets.token_bytes(32)

    def payload(body_json: bytes, descriptor: Mapping[str, Any]) -> bytes:
        return _canonical_bytes(
            {
                "schema": "fxpm.forward-oos-replay-issued-precommit.v1",
                "body_sha256": hashlib.sha256(body_json).hexdigest(),
                "body_size_bytes": len(body_json),
                "descriptor": dict(descriptor),
            },
            _MAX_JSON_BYTES,
        )

    def issue(body_json: bytes, descriptor: Mapping[str, Any]) -> str:
        return hmac.new(
            key, payload(body_json, descriptor), hashlib.sha256
        ).hexdigest()

    def verify(value: Any) -> bool:
        try:
            expected = issue(bytes(value._body_json), value.descriptor)
            return hmac.compare_digest(value._issuance_tag, expected)
        except (AttributeError, TypeError, ValueError, OverflowError):
            return False

    return issue, verify


_issue_precommit_tag, _verify_precommit_issuance = (
    _make_precommit_issuance_capability()
)


def _make_product_issuance_capability():
    # The issuance key is closure-held and never serialized.  A copied tag
    # authenticates only byte-identical evidence; replacing or resealing any
    # descriptor/receipt row invalidates it.
    key = secrets.token_bytes(32)

    def issue(
        anchor_rows: Sequence[
            tuple[str, causal_producer.AnchorReplayArtifactDescriptors]
        ],
        producer_receipt_json: bytes,
        producer_receipt_descriptor: Mapping[str, Any],
    ) -> str:
        return hmac.new(
            key,
            _product_issuance_payload(
                anchor_rows,
                producer_receipt_json,
                producer_receipt_descriptor,
            ),
            hashlib.sha256,
        ).hexdigest()

    def verify(product: Any) -> bool:
        try:
            expected = issue(
                product._anchor_rows,
                bytes(product._producer_receipt_json),
                product.producer_receipt_descriptor,
            )
            return hmac.compare_digest(product._issuance_tag, expected)
        except (AttributeError, TypeError, ValueError, OverflowError):
            return False

    return issue, verify


_issue_product_tag, _verify_product_issuance = _make_product_issuance_capability()


@dataclass(frozen=True, slots=True)
class ForwardOOSReplayPrecommit:
    """Immutable content-addressed execution/valuation roster precommitment."""

    _body_json: bytes
    descriptor: Mapping[str, Any]
    _issuance_tag: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class ForwardOOSReplayProduct:
    """Six raw ledgers per anchor plus the seventh producer receipt."""

    _anchor_rows: tuple[
        tuple[str, causal_producer.AnchorReplayArtifactDescriptors], ...
    ]
    _producer_receipt_json: bytes
    producer_receipt_descriptor: Mapping[str, Any]
    _issuance_tag: str

    @property
    def replay_artifacts_by_anchor(
        self,
    ) -> dict[str, causal_producer.AnchorReplayArtifactDescriptors]:
        return dict(self._anchor_rows)

    @property
    def producer_receipt(self) -> dict[str, Any]:
        return json.loads(self._producer_receipt_json.decode("ascii"))

    @property
    def producer_receipt_sha256(self) -> str:
        return hashlib.sha256(self._producer_receipt_json).hexdigest()

    @property
    def causal_binding_followup_required(self) -> bool:
        return True


def _verify_forward_oos_replay_product_complete(
    context: authority_spool.GenerationSpoolContext,
    product: ForwardOOSReplayProduct,
) -> tuple[
    dict[str, Any],
    dict[str, tuple[portfolio.CandidateTrade, ...]],
    dict[str, dict[str, Any]],
]:
    """Reopen the receipt, six ledgers, and every Capital candidate byte."""

    if (
        type(product) is not ForwardOOSReplayProduct
        or type(product._issuance_tag) is not str
        or _HEX64.fullmatch(product._issuance_tag) is None
        or not _verify_product_issuance(product)
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_NOT_ISSUED")
    descriptor = _descriptor(
        dict(product.producer_receipt_descriptor),
        "FORWARD_OOS_PRODUCER_RECEIPT_DESCRIPTOR_INVALID",
    )
    raw = bytes(product._producer_receipt_json)
    if (
        descriptor["sha256"] != hashlib.sha256(raw).hexdigest()
        or descriptor["size_bytes"] != len(raw)
        or product.producer_receipt_sha256 != descriptor["sha256"]
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_RECEIPT_IDENTITY_INVALID")
    body = _read_descriptor_json(
        context,
        descriptor,
        code="FORWARD_OOS_PRODUCER_RECEIPT",
        maximum=_MAX_JSON_BYTES,
    )
    if _canonical_bytes(body) != raw:
        _fail("FORWARD_OOS_REPLAY_PRODUCT_RECEIPT_BYTE_MISMATCH")
    if type(body) is not dict or frozenset(body) != _PRODUCER_RECEIPT_FIELDS:
        _fail("FORWARD_OOS_REPLAY_PRODUCT_RECEIPT_FIELDS_INVALID")
    precommit_descriptor = _descriptor(
        body.get("forward_replay_precommit_descriptor"),
        "FORWARD_OOS_REPLAY_PRODUCT_PRECOMMIT_DESCRIPTOR_INVALID",
    )
    precommit = _read_descriptor_json(
        context,
        precommit_descriptor,
        code="FORWARD_OOS_REPLAY_PRODUCT_PRECOMMIT",
        maximum=_MAX_JSON_BYTES,
    )
    if type(precommit) is not dict or frozenset(precommit) != _REPLAY_PRECOMMIT_FIELDS:
        _fail("FORWARD_OOS_REPLAY_PRODUCT_PRECOMMIT_FIELDS_INVALID")
    deployment_binding = _validate_deployment_binding_projection(
        body.get("deployment_binding")
    )
    if precommit.get("deployment_binding") != deployment_binding:
        _fail("FORWARD_OOS_REPLAY_PRODUCT_DEPLOYMENT_BINDING_MISMATCH")
    try:
        capital_binding = (
            capital_bootstrap.verify_capital_forward_phase_binding_body(
                context, precommit.get("capital_forward_phase_binding")
            )
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_OOS_CAPITAL_BINDING_REVERIFY_FAILED",
            (type(exc).__name__,),
        ) from exc
    capital_windows = _binding_windows(capital_binding)
    common = (
        "generation_id",
        "generation_context_sha256",
        "native_universe_sha256",
        "config_sha256",
        "historical_precommit_receipt_sha256",
        "historical_run_receipt_sha256",
        "causal_precommit_payload_sha256",
        "capital_bootstrap_identity_sha256",
        "capital_precommitment_body_sha256",
        "capital_contract_lineage_sha256",
        "capital_margin_terms_sha256",
        "capital_broker_terms_sha256",
    )
    if (
        body.get("schema_version") != PRODUCER_RECEIPT_SCHEMA
        or body.get("production_contract_version") != PRODUCTION_CONTRACT_VERSION
        or precommit.get("schema_version") != REPLAY_PRECOMMIT_SCHEMA
        or body.get("generation_id") != context.generation_id
        or body.get("generation_context_sha256") != context.context_sha256
        or body.get("native_universe_sha256") != context.native_universe_sha256
        or body.get("config_sha256") != context.config_sha256
        or any(body.get(key) != precommit.get(key) for key in common)
        or body.get("forward_replay_precommit_receipt_sha256")
        != precommit_descriptor["sha256"]
        or precommit.get("anchor_product_identities_sha256")
        != _sha(precommit.get("anchor_product_identities"))
        or precommit.get("capital_forward_phase_binding_sha256")
        != _sha(capital_binding)
        or precommit.get("capital_phase_windows_by_anchor") != capital_windows
        or precommit.get("capital_phase_windows_sha256") != _sha(capital_windows)
        or precommit.get("capital_bootstrap_identity_sha256")
        != capital_binding.get("bootstrap_identity_sha256")
        or precommit.get("capital_precommitment_body_sha256")
        != capital_binding.get("capital_precommitment_body_sha256")
        or precommit.get("capital_contract_lineage_sha256")
        != _sha(capital_binding.get("contract_lineage"))
        or precommit.get("capital_margin_terms_sha256")
        != capital_binding.get("margin_terms_sha256")
        or precommit.get("capital_broker_terms_sha256")
        != capital_binding.get("broker_terms_sha256")
        or body.get("deployment_binding") != precommit.get("deployment_binding")
        or body.get("code_identity") != precommit.get("code_identity")
        or body.get("valuation_contract") != VALUATION_CONTRACT
        or precommit.get("valuation_contract") != VALUATION_CONTRACT
        or precommit.get("product_cutoff_contract")
        != "EXACT_FORWARD_END_EXCLUSIVE_NO_LATER_ROWS"
        or body.get("valuation_event_order")
        != "close_existing_then_mark_existing_then_admit"
        or body.get("full_round_trip_commission_in_every_candidate_mark")
        is not True
        or body.get("stop_risk_includes_adverse_configured_stop_slippage")
        is not True
        or body.get("candidate_marks_thinned_or_sampled") is not False
        or body.get("capital_candidate_records_complete_pre_policy") is not True
        or body.get("capital_phase_window_contract")
        != "SIGNED_BOOTSTRAP_INNER_AND_CAUSAL_OUTER_HALF_OPEN_V1"
        or body.get("causal_consumer_reopened_and_verified") is not False
        or body.get("causal_binding_followup_required") is not True
        or body.get(
            "confers_causal_process_current_refit_capital_readiness_publication_"
            "or_trading_authority"
        )
        is not False
        or precommit.get("created_before_first_forward_oos_frame_access") is not True
        or precommit.get("all_native_source_access_times_not_after_precommit")
        is not True
        or precommit.get(
            "confers_causal_current_refit_capital_publication_or_trading_authority"
        )
        is not False
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_PROVENANCE_INVALID")
    code_identity = _validate_replay_code_identity(body.get("code_identity"))
    causal_manifest = (
        code_identity.get("causal_synchronized_replay_component_manifest")
        if type(code_identity) is dict
        else None
    )
    producer_manifest = (
        code_identity.get("forward_oos_replay_producer_source_manifest")
        if type(code_identity) is dict
        else None
    )
    causal_files = causal_manifest.get("files") if type(causal_manifest) is dict else None
    producer_files = (
        producer_manifest.get("files") if type(producer_manifest) is dict else None
    )
    if (
        type(code_identity) is not dict
        or _sha(causal_manifest)
        != code_identity.get(
            "causal_synchronized_replay_component_identity_sha256"
        )
        or _sha(producer_manifest)
        != code_identity.get("forward_oos_replay_producer_source_identity_sha256")
        or code_identity.get("forward_producer_in_causal_component_manifest")
        is not True
        or code_identity.get("causal_binding_followup_required") is not True
        or type(causal_files) is not list
        or type(producer_files) is not list
        or len(producer_files) != 1
        or type(producer_files[0]) is not dict
        or producer_files[0] not in causal_files
        or producer_files[0].get("relative_path")
        != "pm_forward_oos_replay_producer.py"
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_CODE_IDENTITY_INVALID")
    precommit_time = _utc(
        precommit.get("created_at_utc"), "FORWARD_OOS_PRECOMMIT_TIME_INVALID"
    )
    receipt_clocks = [
        _utc(body.get(key), f"FORWARD_OOS_{key.upper()}_INVALID")
        for key in (
            "evidence_access_started_at_utc",
            "execution_started_at_utc",
            "execution_completed_at_utc",
            "reconciled_at_utc",
        )
    ]
    if [precommit_time, *receipt_clocks] != sorted(
        [precommit_time, *receipt_clocks]
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_CLOCKS_REVERSED")
    observed: dict[str, dict[str, Any]] = {}
    for anchor_id, descriptors in product._anchor_rows:
        if (
            anchor_id in observed
            or type(descriptors) is not causal_producer.AnchorReplayArtifactDescriptors
        ):
            _fail("FORWARD_OOS_REPLAY_PRODUCT_ANCHOR_ROSTER_INVALID")
        anchor_descriptors: dict[str, dict[str, Any]] = {}
        for role, value in descriptors.descriptors():
            normalized = _descriptor(
                dict(value), "FORWARD_OOS_REPLAY_PRODUCT_LEDGER_DESCRIPTOR_INVALID"
            )
            # The seventh receipt authenticates the descriptor roster, but a
            # descriptor alone does not prove that its content-addressed object
            # is still present and intact.  Reopen every ledger on every public
            # verification so deletion, truncation, corruption, path escape,
            # or non-canonical replacement fails closed after production too.
            _read_descriptor_json(
                context,
                normalized,
                code=f"FORWARD_OOS_REPLAY_PRODUCT_{role}",
                maximum=_MAX_LEDGER_BYTES_PER_ANCHOR,
            )
            anchor_descriptors[role] = normalized
        observed[anchor_id] = anchor_descriptors
    anchor_ids = list(observed)
    required = precommit.get("required_rosters")
    identities = precommit.get("anchor_product_identities")
    metadata = body.get("anchor_production_metadata")
    if (
        body.get("anchor_ledger_descriptors") != observed
        or body.get("ordered_anchor_ids") != anchor_ids
        or type(required) is not dict
        or set(required) != set(anchor_ids)
        or type(identities) is not list
        or [row.get("anchor_id") if type(row) is dict else None for row in identities]
        != anchor_ids
        or type(metadata) is not dict
        or set(metadata) != set(anchor_ids)
    ):
        _fail("FORWARD_OOS_REPLAY_PRODUCT_RECEIPT_SEMANTICS_INVALID")
    candidates, lineages = _reopen_capital_phase_evidence(
        context=context,
        receipt=body,
        precommit=precommit,
    )
    return body, candidates, lineages


def verify_forward_oos_replay_product(
    context: authority_spool.GenerationSpoolContext,
    product: ForwardOOSReplayProduct,
) -> dict[str, Any]:
    """Standalone-reverify every causal and Capital artifact in the product."""

    body, _, _ = _verify_forward_oos_replay_product_complete(context, product)
    return body


def reopen_forward_oos_capital_phase_evidence(
    context: authority_spool.GenerationSpoolContext,
    product: ForwardOOSReplayProduct,
) -> tuple[
    dict[str, Any],
    dict[str, tuple[portfolio.CandidateTrade, ...]],
    dict[str, dict[str, Any]],
]:
    """Return candidates only after complete independent artifact replay."""

    return _verify_forward_oos_replay_product_complete(context, product)


@dataclass(frozen=True, slots=True)
class _DeploymentBinding:
    config_body: Mapping[str, Any]
    config_path: Path
    config_sha256: str
    config_size_bytes: int
    broker_body: Mapping[str, Any]
    broker_path: Path
    broker_relative_path: str
    broker_sha256: str
    broker_size_bytes: int
    pipeline_config_identity: Mapping[str, Any]
    pipeline_config_identity_sha256: str

    def receipt_body(self) -> dict[str, Any]:
        return {
            "deployment_config_path": str(self.config_path),
            "deployment_config_sha256": self.config_sha256,
            "deployment_config_size_bytes": self.config_size_bytes,
            "broker_specs_path": str(self.broker_path),
            "broker_specs_relative_path": self.broker_relative_path,
            "broker_specs_sha256": self.broker_sha256,
            "broker_specs_size_bytes": self.broker_size_bytes,
            "pipeline_config_identity": dict(self.pipeline_config_identity),
            "pipeline_config_identity_sha256": self.pipeline_config_identity_sha256,
        }


@dataclass(slots=True)
class _ResourceBudget:
    native_rows: int = 0
    native_bytes: int = 0
    candidate_positions: int = 0
    candidate_marks: int = 0

    def reserve_positions(self, count: int, anchor_id: str) -> None:
        if type(count) is not int or count < 0:
            _fail("FORWARD_POSITION_RESERVATION_INVALID", anchor_id)
        projected = self.candidate_positions + count
        if projected > _MAX_CANDIDATE_POSITIONS_PER_ANCHOR:
            _fail("FORWARD_POSITION_BOUND_EXCEEDED", anchor_id, projected)
        self.candidate_positions = projected

    def reserve_marks(self, count: int, anchor_id: str) -> None:
        if type(count) is not int or count < 0:
            _fail("FORWARD_MARK_RESERVATION_INVALID", anchor_id)
        projected = self.candidate_marks + count
        projected_bytes = projected * _ESTIMATED_CANDIDATE_MARK_BYTES
        if projected > _MAX_CANDIDATE_MARKS_PER_ANCHOR:
            _fail("FORWARD_MARK_COUNT_BOUND_EXCEEDED", anchor_id, projected)
        if projected_bytes > _MAX_CANDIDATE_MARK_BYTES_PER_ANCHOR:
            _fail("FORWARD_MARK_BYTE_BOUND_EXCEEDED", anchor_id, projected_bytes)
        self.candidate_marks = projected


@dataclass(frozen=True, slots=True)
class _SwapCostEvidence:
    daily_costs: Mapping[str, Decimal]
    events: tuple[tuple[pd.Timestamp, Decimal], ...]


@dataclass(frozen=True, slots=True)
class _M5ValuationTape:
    symbol: str
    frame: pd.DataFrame
    opens: pd.DatetimeIndex
    entry_half_spread: np.ndarray
    spread_meta: Mapping[str, Any]
    execution_spec_receipt: Mapping[str, Any]
    native_product_identity: Mapping[str, Any]


def native_product_key(symbol: str, timeframe: str) -> str:
    """Return the exact public key used by an anchor's native-product map."""

    normalized_symbol = str(symbol or "").strip()
    normalized_timeframe = str(timeframe or "").strip().upper()
    if (
        normalized_symbol not in book.PINNED_SYMBOL_ORDER
        or normalized_timeframe not in book.TIMEFRAME_ORDER
    ):
        _fail("NATIVE_PRODUCT_KEY_INVALID", normalized_symbol, normalized_timeframe)
    return f"{normalized_symbol}|{normalized_timeframe}"


def _source_file_identity(relative_path: str) -> dict[str, Any]:
    try:
        root = Path(__file__).resolve(strict=True).parent
        path = (root / relative_path).resolve(strict=True)
        path.relative_to(root)
        if not path.is_file() or path.is_symlink():
            _fail("REPLAY_SOURCE_OBJECT_INVALID", relative_path)
        raw = path.read_bytes()
    except ForwardOOSReplayProductionError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise ForwardOOSReplayProductionError(
            "REPLAY_SOURCE_OBJECT_UNAVAILABLE", (relative_path,)
        ) from exc
    if not raw or len(raw) > 64 * 1024 * 1024:
        _fail("REPLAY_SOURCE_OBJECT_BOUND_INVALID", relative_path)
    return {
        "relative_path": relative_path,
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _validate_replay_code_identity(value: Any) -> dict[str, Any]:
    expected_fields = {
        "causal_synchronized_replay_component_identity_sha256",
        "causal_synchronized_replay_component_manifest",
        "forward_oos_replay_producer_source_identity_sha256",
        "forward_oos_replay_producer_source_manifest",
        "forward_producer_in_causal_component_manifest",
        "causal_binding_followup_required",
    }
    if type(value) is not dict or set(value) != expected_fields:
        _fail("FORWARD_REPLAY_CODE_IDENTITY_FIELDS_INVALID")
    causal_manifest = value["causal_synchronized_replay_component_manifest"]
    producer_manifest = value["forward_oos_replay_producer_source_manifest"]
    if (
        type(causal_manifest) is not dict
        or set(causal_manifest)
        != {"schema", "component_identity_key", "files"}
        or causal_manifest["schema"]
        != "fxpm.causal-component-source-roster.v1"
        or causal_manifest["component_identity_key"]
        != "synchronized_replay_code_sha256"
        or type(producer_manifest) is not dict
        or set(producer_manifest) != {"schema", "files"}
        or producer_manifest["schema"]
        != "fxpm.forward-oos-replay-source-roster.v1"
    ):
        _fail("FORWARD_REPLAY_CODE_MANIFEST_FIELDS_INVALID")
    causal_files = causal_manifest["files"]
    producer_files = producer_manifest["files"]
    expected_paths = [
        "pm_portfolio_replay.py",
        "pm_causal_evidence_producer.py",
        "pm_forward_oos_replay_producer.py",
    ]
    if (
        type(causal_files) is not list
        or type(producer_files) is not list
        or len(causal_files) != len(expected_paths)
        or len(producer_files) != 1
    ):
        _fail("FORWARD_REPLAY_CODE_FILE_ROSTER_INVALID")
    for row, expected_path in zip(causal_files, expected_paths, strict=True):
        if (
            type(row) is not dict
            or set(row) != {"relative_path", "size_bytes", "sha256"}
            or row["relative_path"] != expected_path
            or type(row["size_bytes"]) is not int
            or type(row["size_bytes"]) is bool
            or not 1 <= row["size_bytes"] <= 64 * 1024 * 1024
        ):
            _fail("FORWARD_REPLAY_CODE_FILE_IDENTITY_INVALID")
        _hex(row["sha256"], "FORWARD_REPLAY_CODE_FILE_IDENTITY_INVALID")
    if (
        producer_files != [causal_files[-1]]
        or _sha(causal_manifest)
        != value["causal_synchronized_replay_component_identity_sha256"]
        or _sha(producer_manifest)
        != value["forward_oos_replay_producer_source_identity_sha256"]
        or value["forward_producer_in_causal_component_manifest"] is not True
        or value["causal_binding_followup_required"] is not True
    ):
        _fail("FORWARD_REPLAY_CODE_IDENTITY_SEMANTICS_INVALID")
    return json.loads(_canonical_bytes(value, 2 * 1024 * 1024).decode("ascii"))


def _validate_deployment_binding_projection(value: Any) -> dict[str, Any]:
    fields = {
        "deployment_config_path",
        "deployment_config_sha256",
        "deployment_config_size_bytes",
        "broker_specs_path",
        "broker_specs_relative_path",
        "broker_specs_sha256",
        "broker_specs_size_bytes",
        "pipeline_config_identity",
        "pipeline_config_identity_sha256",
    }
    if type(value) is not dict or set(value) != fields:
        _fail("FORWARD_DEPLOYMENT_BINDING_FIELDS_INVALID")
    for key in (
        "deployment_config_path",
        "broker_specs_path",
        "broker_specs_relative_path",
    ):
        if type(value[key]) is not str or not value[key]:
            _fail("FORWARD_DEPLOYMENT_BINDING_PATH_INVALID", key)
    for key in ("deployment_config_size_bytes", "broker_specs_size_bytes"):
        if (
            type(value[key]) is not int
            or type(value[key]) is bool
            or not 1 <= value[key] <= 64 * 1024 * 1024
        ):
            _fail("FORWARD_DEPLOYMENT_BINDING_SIZE_INVALID", key)
    _hex(
        value["deployment_config_sha256"],
        "FORWARD_DEPLOYMENT_BINDING_HASH_INVALID",
    )
    _hex(
        value["broker_specs_sha256"],
        "FORWARD_DEPLOYMENT_BINDING_HASH_INVALID",
    )
    _hex(
        value["pipeline_config_identity_sha256"],
        "FORWARD_DEPLOYMENT_BINDING_HASH_INVALID",
    )
    identity = value["pipeline_config_identity"]
    if (
        type(identity) is not dict
        or set(identity)
        != {
            "schema_version",
            "deployment_config_path",
            "full_dataclass_surface",
            "backtester_cost_surface",
        }
        or identity["schema_version"]
        != "fxpm.forward-oos-pipeline-config-identity.v1"
        or type(identity["deployment_config_path"]) is not str
        or type(identity["full_dataclass_surface"]) is not dict
        or type(identity["backtester_cost_surface"]) is not dict
        or _sha(identity) != value["pipeline_config_identity_sha256"]
    ):
        _fail("FORWARD_DEPLOYMENT_CONFIG_IDENTITY_INVALID")
    return json.loads(_canonical_bytes(value, 8 * 1024 * 1024).decode("ascii"))


def _replay_code_identity(
    causal_precommit_payload: Mapping[str, Any],
) -> dict[str, Any]:
    process = causal_precommit_payload.get("process_definition")
    components = process.get("component_identities") if type(process) is dict else None
    if type(components) is not dict:
        _fail("CAUSAL_COMPONENT_IDENTITIES_MISSING")
    causal_files = [
        _source_file_identity("pm_portfolio_replay.py"),
        _source_file_identity("pm_causal_evidence_producer.py"),
        _source_file_identity("pm_forward_oos_replay_producer.py"),
    ]
    causal_body = {
        "schema": "fxpm.causal-component-source-roster.v1",
        "component_identity_key": "synchronized_replay_code_sha256",
        "files": causal_files,
    }
    causal_identity = _sha(causal_body)
    if components.get("synchronized_replay_code_sha256") != causal_identity:
        _fail("CAUSAL_SYNCHRONIZED_REPLAY_COMPONENT_DRIFT")
    producer_file = _source_file_identity(Path(__file__).name)
    producer_body = {
        "schema": "fxpm.forward-oos-replay-source-roster.v1",
        "files": [producer_file],
    }
    return _validate_replay_code_identity({
        "causal_synchronized_replay_component_identity_sha256": causal_identity,
        "causal_synchronized_replay_component_manifest": causal_body,
        "forward_oos_replay_producer_source_identity_sha256": _sha(producer_body),
        "forward_oos_replay_producer_source_manifest": producer_body,
        "forward_producer_in_causal_component_manifest": True,
        "causal_binding_followup_required": True,
    })


def _native_product_identity(
    product: native_prefix.NativePrefixProduct,
    *,
    expected_symbol: str,
    expected_timeframe: str,
    expected_cutoff_utc: str,
    role: str,
) -> dict[str, Any]:
    if (
        type(product) is not native_prefix.NativePrefixProduct
        or getattr(product, "_seal", None) is not native_prefix._PRODUCT_SEAL
        or product.source_state != native_prefix.AVAILABLE
        or type(product.index) is not dict
        or type(product.native_prefix) is not dict
    ):
        _fail("FORWARD_NATIVE_PRODUCT_NOT_VERIFIER_SEALED", role)
    index = dict(product.index)
    raw = _canonical_bytes(index, native_prefix.MAX_INDEX_BYTES)
    index_descriptor = _descriptor(
        dict(product.index_descriptor), "FORWARD_NATIVE_INDEX_DESCRIPTOR_INVALID"
    )
    prefix_descriptor = _descriptor(
        dict(product.prefix_descriptor or {}),
        "FORWARD_NATIVE_PREFIX_DESCRIPTOR_INVALID",
    )
    artifacts = [
        _descriptor(dict(value), "FORWARD_NATIVE_ARTIFACT_DESCRIPTOR_INVALID")
        for value in product.artifact_descriptors
    ]
    native = dict(product.native_prefix)
    instrument = dict(product.instrument_snapshot or {})
    symbol = str(index.get("symbol") or "")
    timeframe = str(index.get("timeframe") or "").upper()
    expected_cutoff = _utc_seconds(
        expected_cutoff_utc, "FORWARD_NATIVE_EXPECTED_CUTOFF_INVALID"
    )
    if (
        index.get("schema_version") != native_prefix.PREFIX_INDEX_SCHEMA
        or index.get("source_state") != native_prefix.AVAILABLE
        or index.get("grants_search_authority") is not False
        or index.get("grants_trading_authority") is not False
        or symbol != expected_symbol
        or timeframe != expected_timeframe
        or index.get("cutoff_utc") != expected_cutoff
        or product.index_sha256 != hashlib.sha256(raw).hexdigest()
        or index_descriptor["sha256"] != product.index_sha256
        or index_descriptor["size_bytes"] != len(raw)
        or prefix_descriptor != index.get("prefix", {}).get("content")
        or native
        != native_prefix._native_prefix_from_index(index, product.index_sha256)
        or instrument != dict(index.get("source", {}).get("instrument_snapshot") or {})
        or index_descriptor not in artifacts
        or prefix_descriptor not in artifacts
    ):
        _fail("FORWARD_NATIVE_PRODUCT_IDENTITY_INVALID", role)
    activation = index.get("activation")
    source = index.get("source")
    prefix_body = index.get("prefix")
    continuity = index.get("continuity")
    if not all(
        type(value) is dict for value in (activation, source, prefix_body, continuity)
    ):
        _fail("FORWARD_NATIVE_PRODUCT_BINDING_SURFACE_INVALID", role)
    rows = prefix_body.get("prefix_rows")
    bytes_value = prefix_descriptor.get("size_bytes")
    if (
        type(rows) is not int
        or not 1 <= rows <= native_prefix.MAX_OPEN_FRAME_ROWS
        or type(bytes_value) is not int
        or not 1 <= bytes_value <= native_prefix.MAX_OPEN_FRAME_BYTES
    ):
        _fail("FORWARD_NATIVE_PRODUCT_RESOURCE_GEOMETRY_INVALID", role)
    return {
        "role": role,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": expected_cutoff,
        "source_accessed_at_utc": _utc_seconds(
            index.get("source_accessed_at_utc"),
            "FORWARD_NATIVE_SOURCE_ACCESS_TIME_INVALID",
        ),
        "index_sha256": product.index_sha256,
        "index_descriptor": index_descriptor,
        "activation_sha256": _sha(activation),
        "activation_id": activation.get("activation_id"),
        "activation_manifest_sha256": activation.get("manifest_sha256"),
        "activation_entry_sha256": activation.get("entry_sha256"),
        "source_sha256": _sha(source),
        "source_bars_sha256": source.get("bars_sha256"),
        "source_audit_sidecar_sha256": source.get("audit_sidecar_sha256"),
        "source_provenance_sha256": source.get("provenance_sha256"),
        "source_request_map_sha256": source.get("request_map_sha256"),
        "source_identity_sha256": source.get("source_identity_sha256"),
        "source_request_start_utc": source.get("request_start_utc"),
        "source_request_end_exclusive_utc": source.get("request_end_exclusive_utc"),
        "prefix_sha256": _sha(prefix_body),
        "prefix_content_sha256": prefix_descriptor["sha256"],
        "prefix_rows": rows,
        "prefix_bytes": bytes_value,
        "continuity_sha256": _sha(continuity),
        "continuity_policy_sha256": continuity.get("policy", {}).get("sha256"),
        "continuity_receipt_sha256": continuity.get("receipt", {}).get("sha256"),
        "continuity_geometry_sha256": continuity.get("geometry_sha256"),
        "native_prefix_sha256": _sha(native),
        "instrument_snapshot_sha256": _sha(instrument),
        "prefix_descriptor": prefix_descriptor,
        "artifact_descriptor_roster_sha256": _sha(artifacts),
    }


def _reopen_bound_native_product(
    *,
    context: authority_spool.GenerationSpoolContext,
    product: native_prefix.NativePrefixProduct,
    expected_identity: Mapping[str, Any],
    config: PipelineConfig,
    production_contract_required: bool = True,
) -> native_prefix.NativePrefixProduct:
    if type(expected_identity) is not dict:
        _fail("FORWARD_NATIVE_PRECOMMIT_IDENTITY_INVALID")
    try:
        reopened = native_prefix.verify_activated_native_prefix(
            data_dir=Path(config.data_dir),
            context=context,
            index_descriptor=product.index_descriptor,
            expected_index_sha256=product.index_sha256,
            expected_symbol=expected_identity["symbol"],
            expected_timeframe=expected_identity["timeframe"],
            expected_cutoff_utc=expected_identity["cutoff_utc"],
            expected_activation_manifest_sha256=expected_identity[
                "activation_manifest_sha256"
            ],
            production_contract_required=production_contract_required,
            require_current_activation=False,
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_NATIVE_PRODUCT_RAW_REVERIFY_FAILED",
            (expected_identity.get("role"), type(exc).__name__, str(exc)),
        ) from exc
    observed = _native_product_identity(
        reopened,
        expected_symbol=expected_identity["symbol"],
        expected_timeframe=expected_identity["timeframe"],
        expected_cutoff_utc=expected_identity["cutoff_utc"],
        role=expected_identity["role"],
    )
    if observed != expected_identity:
        _fail("FORWARD_NATIVE_PRODUCT_PRECOMMIT_DRIFT", expected_identity["role"])
    return reopened


def _freeze_execution_spec(
    *,
    product: native_prefix.NativePrefixProduct,
    config: PipelineConfig,
    deployment: _DeploymentBinding,
) -> tuple[InstrumentSpec, dict[str, Any]]:
    try:
        spec, receipt = search_execution._freeze_bound_execution_spec(
            product=product, config=config
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_EXECUTION_SPEC_FREEZE_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if type(receipt) is not dict:
        _fail("FORWARD_EXECUTION_SPEC_RECEIPT_INVALID")
    owned = dict(receipt)
    receipt_sha = owned.pop("receipt_sha256", None)
    if (
        receipt_sha != _sha(owned)
        or receipt.get("deployment_config_sha256") != deployment.config_sha256
        or receipt.get("deployment_config_size_bytes") != deployment.config_size_bytes
        or receipt.get("broker_specs_relative_path") != deployment.broker_relative_path
        or receipt.get("broker_specs_sha256") != deployment.broker_sha256
        or receipt.get("broker_specs_size_bytes") != deployment.broker_size_bytes
        or receipt.get("native_prefix_index_sha256") != product.index_sha256
        or receipt.get("grants_search_authority") is not False
        or receipt.get("grants_trading_authority") is not False
    ):
        _fail("FORWARD_EXECUTION_SPEC_RECEIPT_BINDING_INVALID")
    return spec, json.loads(_canonical_bytes(receipt).decode("ascii"))


@dataclass(frozen=True, slots=True)
class _VerifiedSpecialist:
    anchor_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    strategy_id: str
    strategy_class: str
    strategy_module: str
    parameters: Mapping[str, Any]
    process_candidate_id: str
    process_sha256: str
    priority_score: float
    source_count_receipt: Mapping[str, Any]
    detector_parameters: Mapping[str, Any]
    detector_parameters_sha256: str
    detector_record: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _DailyMark:
    date: str
    marked_at_utc: str
    mid_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    base_financing_per_lot: Decimal
    stress_financing_per_lot: Decimal
    candidate_net_pnl_per_lot: float


@dataclass(frozen=True, slots=True)
class _PositionEvidence:
    candidate: portfolio.CandidateTrade
    position_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    side: str
    contract_multiplier: Decimal
    stop_price: Decimal
    entry_mid: Decimal
    entry_bid: Decimal
    entry_ask: Decimal
    base_entry_fill: Decimal
    stress_entry_fill: Decimal
    exit_mid: Decimal
    exit_bid: Decimal
    exit_ask: Decimal
    base_exit_fill: Decimal
    stress_exit_fill: Decimal
    base_commission_per_lot: Decimal
    stress_commission_per_lot: Decimal
    margin_per_lot: Decimal
    marks: tuple[_DailyMark, ...]


@dataclass(frozen=True, slots=True)
class _AnchorRawReplay:
    anchor: Mapping[str, Any]
    specialists: tuple[_VerifiedSpecialist, ...]
    positions: tuple[_PositionEvidence, ...]
    accepted_volume_by_position_id: Mapping[str, Decimal]
    production_metadata: Mapping[str, Any] | None = None


def _bound_json_file(
    path: Path,
    *,
    code: str,
    maximum_bytes: int,
) -> tuple[dict[str, Any], Path, bytes]:
    try:
        resolved = Path(path).resolve(strict=True)
        if not resolved.is_file() or resolved.is_symlink():
            _fail(f"{code}_OBJECT_INVALID")
        raw = resolved.read_bytes()
    except ForwardOOSReplayProductionError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_UNAVAILABLE") from exc
    if not 2 <= len(raw) <= maximum_bytes:
        _fail(f"{code}_BYTE_BOUND_INVALID")
    try:
        body = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_JSON_INVALID") from exc
    if type(body) is not dict:
        _fail(f"{code}_NOT_OBJECT")
    return body, resolved, raw


def _config_identity_value(value: Any) -> Any:
    if isinstance(value, Path):
        return {"path": str(value)}
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.integer, int)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        if not math.isfinite(number):
            _fail("PIPELINE_CONFIG_NONFINITE")
        return {"float64_hex": number.hex().lower()}
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _config_identity_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_config_identity_value(item) for item in value]
    _fail("PIPELINE_CONFIG_VALUE_UNSUPPORTED", type(value).__name__)


def _pipeline_config_identity(config: PipelineConfig) -> dict[str, Any]:
    if type(config) is not PipelineConfig:
        _fail("PIPELINE_CONFIG_INVALID")
    full = _config_identity_value(asdict(config))
    cost_fields = (
        "initial_capital",
        "discovery_risk_per_trade_pct",
        "use_spread",
        "use_commission",
        "use_slippage",
        "slippage_pips",
        "backtest_spread_mode",
        "swap_model_enabled",
        "swap_model_triple_rollover_day",
        "swap_model_min_holding_hours",
        "oos_evidence_execution_stress_mode",
    )
    cost = {name: _config_identity_value(getattr(config, name)) for name in cost_fields}
    return {
        "schema_version": "fxpm.forward-oos-pipeline-config-identity.v1",
        "deployment_config_path": str(
            Path(str(getattr(config, "_deployment_config_path", "") or ""))
        ),
        "full_dataclass_surface": full,
        "backtester_cost_surface": cost,
    }


def _deployment_config(config: PipelineConfig) -> _DeploymentBinding:
    if type(config) is not PipelineConfig:
        _fail("PIPELINE_CONFIG_INVALID")
    raw_path = str(getattr(config, "_deployment_config_path", "") or "").strip()
    if not raw_path:
        _fail("DEPLOYMENT_CONFIG_UNBOUND")
    body, path, raw = _bound_json_file(
        Path(raw_path), code="DEPLOYMENT_CONFIG", maximum_bytes=16 * 1024 * 1024
    )
    pipeline_body = body.get("pipeline")
    position_body = body.get("position")
    metadata = body.get("_meta")
    if (
        type(pipeline_body) is not dict
        or type(position_body) is not dict
        or type(metadata) is not dict
        or not str(metadata.get("scope") or "").strip()
    ):
        _fail("DEPLOYMENT_RISK_OR_SCOPE_POLICY_MISSING")
    try:
        configured_initial = float(pipeline_body.get("initial_capital"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ForwardOOSReplayProductionError(
            "DEPLOYMENT_INITIAL_CAPITAL_INVALID"
        ) from exc
    if (
        not math.isfinite(configured_initial)
        or configured_initial <= 0
        or configured_initial != float(config.initial_capital)
    ):
        _fail("PIPELINE_CONFIG_DEPLOYMENT_DRIFT")
    broker_relative = body.get("broker_specs_path")
    if type(broker_relative) is not str or not broker_relative.strip():
        _fail("DEPLOYMENT_BROKER_SPECS_PATH_UNBOUND")
    broker_candidate = Path(broker_relative)
    if not broker_candidate.is_absolute():
        broker_candidate = path.parent / broker_candidate
    broker_body, broker_path, broker_raw = _bound_json_file(
        broker_candidate,
        code="DEPLOYMENT_BROKER_SPECS",
        maximum_bytes=64 * 1024 * 1024,
    )
    identity = _pipeline_config_identity(config)
    return _DeploymentBinding(
        config_body=json.loads(_canonical_bytes(body).decode("ascii")),
        config_path=path,
        config_sha256=hashlib.sha256(raw).hexdigest(),
        config_size_bytes=len(raw),
        broker_body=json.loads(_canonical_bytes(broker_body).decode("ascii")),
        broker_path=broker_path,
        broker_relative_path=broker_relative,
        broker_sha256=hashlib.sha256(broker_raw).hexdigest(),
        broker_size_bytes=len(broker_raw),
        pipeline_config_identity=identity,
        pipeline_config_identity_sha256=_sha(identity),
    )


def _assert_deployment_config_unchanged(
    binding: _DeploymentBinding, config: PipelineConfig
) -> None:
    refreshed = _deployment_config(config)
    if refreshed.receipt_body() != binding.receipt_body():
        _fail("DEPLOYMENT_OR_BROKER_CONFIG_CHANGED_DURING_REPLAY")


def _owned_book(payload: Any, expected_receipt: str) -> dict[str, Any]:
    if type(payload) is not dict:
        _fail("BOOK_DECISION_NOT_OBJECT")
    body = json.loads(_canonical_bytes(payload, 32 * 1024 * 1024).decode("ascii"))
    receipt = body.pop("receipt_sha256", None)
    if receipt != expected_receipt or _sha(body) != receipt:
        _fail("BOOK_DECISION_RECEIPT_MISMATCH")
    try:
        decision = book.BookMultiplicityDecision(
            _canonical_bytes(body, 32 * 1024 * 1024), receipt
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError("BOOK_DECISION_OWNERSHIP_FAILED") from exc
    if book.verify_historical_book_multiplicity_decision(decision) is not True:
        _fail("BOOK_DECISION_RAW_REVERIFY_FAILED")
    verified = decision.to_dict()
    if verified != payload or verified.get("valid") is not True:
        _fail("BOOK_DECISION_INCOMPLETE")
    return verified


def _manifest_entries(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = manifest.get("artifacts")
    if type(raw) is not list:
        _fail("SYMBOL_MANIFEST_ARTIFACTS_INVALID")
    result: dict[str, dict[str, Any]] = {}
    for value in raw:
        if type(value) is not dict or set(value) != {
            "artifact_id",
            "artifact_type",
            "descriptor",
        }:
            _fail("SYMBOL_MANIFEST_ENTRY_INVALID")
        artifact_id = value.get("artifact_id")
        if type(artifact_id) is not str or not artifact_id or artifact_id in result:
            _fail("SYMBOL_MANIFEST_ARTIFACT_ID_INVALID")
        _descriptor(value.get("descriptor"), "SYMBOL_MANIFEST_DESCRIPTOR_INVALID")
        result[artifact_id] = dict(value)
    return result


def _verified_manifest(
    context: authority_spool.GenerationSpoolContext,
    symbol_receipt: Mapping[str, Any],
    symbol: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    try:
        manifest = authority_spool.verify_symbol_receipt(
            context.root,
            symbol_receipt,
            expected_generation_id=context.generation_id,
            expected_symbol=symbol,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "HISTORICAL_SYMBOL_MANIFEST_REVERIFY_FAILED", (symbol,)
        ) from exc
    if type(manifest) is not dict:
        _fail("HISTORICAL_SYMBOL_MANIFEST_INVALID", symbol)
    return manifest, _manifest_entries(manifest)


def _anchor_by_id(
    precommit_body: Mapping[str, Any], run_body: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    planned = precommit_body.get("anchors")
    completed = run_body.get("anchors")
    if (
        type(planned) is not list
        or type(completed) is not list
        or len(planned) != len(completed)
    ):
        _fail("HISTORICAL_ANCHOR_ROSTER_INVALID")
    result: dict[str, dict[str, Any]] = {}
    for plan, receipt in zip(planned, completed, strict=True):
        if type(plan) is not dict or type(receipt) is not dict:
            _fail("HISTORICAL_ANCHOR_RECORD_INVALID")
        anchor_id = plan.get("anchor_id")
        if anchor_id != receipt.get("anchor_id") or anchor_id in result:
            _fail("HISTORICAL_ANCHOR_ID_MISMATCH")
        result[anchor_id] = dict(receipt)
    return [dict(value) for value in planned], result


def _reopen_books(
    context: authority_spool.GenerationSpoolContext,
    historical_precommit: historical.HistoricalAnchorPrecommit,
    historical_run_receipt: historical.HistoricalAnchorRunReceipt,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    list[Mapping[str, Any]],
]:
    if (
        type(historical_precommit) is not historical.HistoricalAnchorPrecommit
        or type(historical_run_receipt) is not historical.HistoricalAnchorRunReceipt
    ):
        _fail("HISTORICAL_RECEIPT_OBJECT_INVALID")
    if (
        historical.verify_historical_anchor_run_receipt(
            context, historical_precommit, historical_run_receipt
        )
        is not True
    ):
        _fail("HISTORICAL_RUN_RAW_REVERIFY_FAILED")
    precommit_body = historical_precommit.to_dict()
    run_body = historical_run_receipt.to_dict()
    plans, receipts_by_anchor = _anchor_by_id(precommit_body, run_body)
    symbol_receipts = run_body.get("symbol_receipts")
    if type(symbol_receipts) is not list or len(symbol_receipts) != len(
        book.PINNED_SYMBOL_ORDER
    ):
        _fail("HISTORICAL_SYMBOL_RECEIPT_ROSTER_INVALID")
    first_symbol = book.PINNED_SYMBOL_ORDER[0]
    _, entries = _verified_manifest(context, symbol_receipts[0], first_symbol)
    books: dict[str, dict[str, Any]] = {}
    for plan in plans:
        anchor_id = plan["anchor_id"]
        entry = entries.get(f"{anchor_id}/book-decision")
        if type(entry) is not dict:
            _fail("HISTORICAL_BOOK_ARTIFACT_MISSING", anchor_id)
        descriptor = entry.get("descriptor")
        payload = _read_descriptor_json(
            context,
            descriptor,
            code="HISTORICAL_BOOK_DECISION",
            maximum=32 * 1024 * 1024,
        )
        books[anchor_id] = _owned_book(
            payload, receipts_by_anchor[anchor_id]["book_decision_sha256"]
        )
    return run_body, plans, books, symbol_receipts


def _manifest_body(
    context: authority_spool.GenerationSpoolContext,
    entries: Mapping[str, Mapping[str, Any]],
    artifact_id: str,
    *,
    code: str,
    expected_descriptor: Mapping[str, Any] | None = None,
    maximum: int = _MAX_JSON_BYTES,
) -> dict[str, Any]:
    entry = entries.get(artifact_id)
    if type(entry) is not dict:
        _fail(f"{code}_MISSING", artifact_id)
    descriptor = _descriptor(entry.get("descriptor"), f"{code}_DESCRIPTOR_INVALID")
    if expected_descriptor is not None and descriptor != _descriptor(
        expected_descriptor, f"{code}_EXPECTED_DESCRIPTOR_INVALID"
    ):
        _fail(f"{code}_DESCRIPTOR_SPLICE", artifact_id)
    return _read_descriptor_json(
        context,
        descriptor,
        code=code,
        maximum=maximum,
    )


def _winner_request(
    body: Mapping[str, Any], expected_sha256: str
) -> winner_v11.WinnerSelectionRequest:
    if type(body) is not dict or set(body) != {
        "contract_version",
        "precommitment",
        "attempt_ledger",
        "evidence_ledger",
    }:
        _fail("WINNER_REQUEST_FIELDS_INVALID")
    wrappers: list[dict[str, Any]] = []
    for name in ("precommitment", "attempt_ledger", "evidence_ledger"):
        wrapper = body.get(name)
        if type(wrapper) is not dict or set(wrapper) != {"payload", "payload_sha256"}:
            _fail("WINNER_REQUEST_WRAPPER_INVALID", name)
        payload = wrapper.get("payload")
        if type(payload) is not dict or _sha(payload) != wrapper.get("payload_sha256"):
            _fail("WINNER_REQUEST_PAYLOAD_HASH_INVALID", name)
        wrappers.append(wrapper)
    request = winner_v11.create_winner_selection_request(
        wrappers[0]["payload"], wrappers[1]["payload"], wrappers[2]["payload"]
    )
    if (
        request is None
        or request.request_sha256 != expected_sha256
        or request.to_dict() != body
    ):
        _fail("WINNER_REQUEST_RAW_REPLAY_FAILED")
    return request


def _winner_decision(
    body: Mapping[str, Any],
    request: winner_v11.WinnerSelectionRequest,
    expected_sha256: str,
) -> winner_v11.WinnerSelectionDecision:
    if type(body) is not dict:
        _fail("WINNER_DECISION_NOT_OBJECT")
    owned = json.loads(
        _canonical_bytes(body, winner_v11.MAX_DECISION_BYTES).decode("ascii")
    )
    receipt = owned.pop("receipt_sha256", None)
    if receipt != expected_sha256 or _sha(owned) != receipt:
        _fail("WINNER_DECISION_RECEIPT_MISMATCH")
    decision = winner_v11.WinnerSelectionDecision(
        _canonical_bytes(owned, winner_v11.MAX_DECISION_BYTES), receipt
    )
    if (
        winner_v11.verify_historical_cell_winner_decision(decision, request) is not True
        or decision.to_dict() != body
    ):
        _fail("WINNER_DECISION_RAW_REPLAY_FAILED")
    return decision


def _selected_group_records(
    *,
    book_body: Mapping[str, Any],
    book_cell: Mapping[str, Any],
    index: Mapping[str, Any],
    symbol: str,
    timeframe: str,
    cell_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ordinal = book_cell.get("ordinal")
    cells = book_body.get("cells")
    book_sources = book_body.get("source_groups")
    book_fits = book_body.get("regime_fits")
    index_sources = index.get("source_groups")
    index_fits = index.get("regime_fits")
    if (
        type(ordinal) is not int
        or not 0 <= ordinal < book.FAMILY_SIZE
        or type(cells) is not list
        or len(cells) != book.FAMILY_SIZE
        or cells[ordinal] != book_cell
        or type(book_sources) is not list
        or len(book_sources)
        != len(book.PINNED_SYMBOL_ORDER) * len(book.TIMEFRAME_ORDER)
        or type(book_fits) is not list
        or len(book_fits) != len(book_sources)
        or type(index_sources) is not list
        or len(index_sources) != len(book.TIMEFRAME_ORDER)
        or type(index_fits) is not list
        or len(index_fits) != len(book.TIMEFRAME_ORDER)
    ):
        _fail("SELECTED_BOOK_GROUP_ROSTER_INVALID", cell_id)
    global_group_ordinal = ordinal // len(book.REGIME_ORDER)
    try:
        local_group_ordinal = book.TIMEFRAME_ORDER.index(timeframe)
    except ValueError as exc:
        raise ForwardOOSReplayProductionError(
            "SELECTED_TIMEFRAME_INVALID", (cell_id, timeframe)
        ) from exc
    try:
        symbol_ordinal = book.PINNED_SYMBOL_ORDER.index(symbol)
    except ValueError as exc:
        raise ForwardOOSReplayProductionError(
            "SELECTED_SYMBOL_INVALID", (cell_id, symbol)
        ) from exc
    expected_global_group = (
        symbol_ordinal * len(book.TIMEFRAME_ORDER)
        + local_group_ordinal
    )
    if global_group_ordinal != expected_global_group:
        _fail("SELECTED_BOOK_GROUP_IDENTITY_INVALID", cell_id)
    source = index_sources[local_group_ordinal]
    fit = index_fits[local_group_ordinal]
    if (
        type(source) is not dict
        or type(fit) is not dict
        or source != book_sources[global_group_ordinal]
        or fit != book_fits[global_group_ordinal]
    ):
        _fail("SELECTED_ADAPTER_BOOK_GROUP_SPLICE", cell_id)
    return dict(source), dict(fit)


def _selected_artifacts(
    *,
    context: authority_spool.GenerationSpoolContext,
    entries: Mapping[str, Mapping[str, Any]],
    anchor: Mapping[str, Any],
    book_body: Mapping[str, Any],
    book_cell: Mapping[str, Any],
    index: Mapping[str, Any],
) -> _VerifiedSpecialist:
    symbol = str(book_cell.get("symbol") or "")
    timeframe = str(book_cell.get("timeframe") or "").upper()
    regime = str(book_cell.get("regime") or "").upper()
    cell_id = _hex(book_cell.get("cell_id"), "BOOK_WINNER_CELL_ID_INVALID")
    if book_cell.get("historical_state") != "WINNER":
        _fail("NONWINNER_SELECTED_FOR_REPLAY", cell_id)
    records = index.get("cell_records")
    if type(records) is not list:
        _fail("ADAPTER_CELL_RECORDS_INVALID")
    matches = [
        value
        for value in records
        if type(value) is dict
        and (value.get("symbol"), value.get("timeframe"), value.get("regime"))
        == (symbol, timeframe, regime)
    ]
    if len(matches) != 1:
        _fail("ADAPTER_WINNER_RECORD_CARDINALITY_INVALID", cell_id)
    record = matches[0]
    namespace = record.get("raw_artifact_namespace")
    if (
        record.get("historical_state") != "WINNER_DECISION"
        or type(namespace) is not str
        or not namespace
        or record.get("winner_request_sha256") != book_cell.get("winner_request_sha256")
        or record.get("winner_decision_sha256")
        != book_cell.get("winner_decision_sha256")
    ):
        _fail("ADAPTER_BOOK_WINNER_SPLICE", cell_id)
    cell_namespace = f"{namespace}/winner-search/{timeframe}/{regime}"
    request_body = _manifest_body(
        context,
        entries,
        f"{cell_namespace}/authority/winner-request",
        code="WINNER_REQUEST",
        maximum=winner_v11.MAX_REQUEST_BYTES,
    )
    request = _winner_request(request_body, book_cell["winner_request_sha256"])
    decision_body = _manifest_body(
        context,
        entries,
        f"{cell_namespace}/authority/winner-decision",
        code="WINNER_DECISION",
        maximum=winner_v11.MAX_DECISION_BYTES,
    )
    decision = _winner_decision(
        decision_body, request, book_cell["winner_decision_sha256"]
    )
    decision_winner = decision.to_dict().get("winner")
    process_candidate_id = book_cell.get("local_process_candidate_id")
    process_sha256 = book_cell.get("local_process_sha256")
    if (
        type(decision_winner) is not dict
        or decision.status != "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
        or decision_winner.get("process_candidate_id") != process_candidate_id
        or decision_winner.get("process_sha256") != process_sha256
    ):
        _fail("BOOK_SELECTED_PROCESS_NOT_WINNER_V11", cell_id)

    attempt_ledger = _manifest_body(
        context,
        entries,
        f"{cell_namespace}/frozen/attempt_ledger",
        code="WINNER_ATTEMPT_LEDGER",
        maximum=winner_v11.MAX_REQUEST_BYTES,
    )
    if attempt_ledger != request.attempt_ledger.to_dict() or _sha(
        attempt_ledger
    ) != book_cell.get("winner_attempt_ledger_payload_sha256"):
        _fail("WINNER_ATTEMPT_LEDGER_SPLICE", cell_id)
    attempts = attempt_ledger.get("attempts")
    if type(attempts) is not list:
        _fail("WINNER_ATTEMPT_ROSTER_INVALID", cell_id)
    selected = [
        (ordinal, value)
        for ordinal, value in enumerate(attempts)
        if type(value) is dict
        and value.get("process_candidate_id") == process_candidate_id
        and value.get("process_sha256") == process_sha256
    ]
    if len(selected) != 1:
        _fail("WINNER_SELECTED_ATTEMPT_CARDINALITY_INVALID", cell_id)
    ordinal, attempt = selected[0]

    cross = _manifest_body(
        context,
        entries,
        f"{anchor['anchor_id']}/adapter/{timeframe}/{regime}/horizon-winner-cross-binding",
        code="HORIZON_WINNER_CROSS_BINDING",
        expected_descriptor=record.get("horizon_winner_cross_binding"),
    )
    mapping = cross.get("mapping")
    if (
        cross.get("schema_version")
        != historical_adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA
        or cross.get("anchor_id") != anchor["anchor_id"]
        or cross.get("symbol") != symbol
        or cross.get("timeframe") != timeframe
        or cross.get("regime") != regime
        or cross.get("artifact_namespace") != namespace
        or cross.get("bijective_coverage") is not True
        or cross.get("created_before_winner_evidence_access") is not True
        or cross.get("confers_causal_current_live_or_capital_authority") is not False
        or type(mapping) is not list
        or cross.get("mapping_count") != len(mapping)
        or cross.get("mapping_sha256") != _sha(mapping)
    ):
        _fail("HORIZON_WINNER_CROSS_BINDING_INVALID", cell_id)
    mapped = [
        value
        for value in mapping
        if type(value) is dict
        and value.get("winner_process_candidate_id") == process_candidate_id
        and value.get("process_sha256") == process_sha256
    ]
    if len(mapped) != 1 or mapped[0].get("ordinal") != ordinal:
        _fail("HORIZON_WINNER_SELECTED_MAPPING_INVALID", cell_id)
    selected_mapping = mapped[0]
    count_receipt = selected_mapping.get("source_count_receipt")
    if type(count_receipt) is not dict:
        _fail("WINNER_SOURCE_COUNT_RECEIPT_INVALID", cell_id)
    count_body = dict(count_receipt)
    count_sha = count_body.pop("receipt_sha256", None)
    if (
        count_sha != _sha(count_body)
        or count_receipt.get("schema_version")
        != historical_adapter.SOURCE_COUNT_RECEIPT_SCHEMA
        or count_receipt.get("anchor_id") != anchor["anchor_id"]
        or count_receipt.get("symbol") != symbol
        or count_receipt.get("timeframe") != timeframe
        or count_receipt.get("regime") != regime
    ):
        _fail("WINNER_SOURCE_COUNT_RECEIPT_INVALID", cell_id)

    bodies: dict[str, dict[str, Any]] = {}
    for name in (
        "winner_process",
        "horizon_process",
        "trial_definition",
        "refit_recipe",
    ):
        body = _manifest_body(
            context,
            entries,
            f"{cell_namespace}/frozen/attempt-{ordinal:04d}/{name}",
            code=f"SELECTED_{name.upper()}",
        )
        if _sha(body) != attempt.get(
            {
                "winner_process": "process_sha256",
                "horizon_process": "horizon_process_sha256",
                "trial_definition": "trial_definition_sha256",
                "refit_recipe": "refit_recipe_sha256",
            }[name]
        ) or _sha(body) != selected_mapping.get(
            {
                "winner_process": "process_sha256",
                "horizon_process": "horizon_process_sha256",
                "trial_definition": "trial_definition_sha256",
                "refit_recipe": "refit_recipe_sha256",
            }[name]
        ):
            _fail("SELECTED_ATTEMPT_ARTIFACT_HASH_MISMATCH", name, cell_id)
        bodies[name] = body
    execution_contract = selected_mapping.get("horizon_execution_contract")
    if type(execution_contract) is not dict or set(execution_contract) != {
        "lookback_kind",
        "lookback_value",
        "source_bar_count",
    }:
        _fail("SELECTED_HORIZON_EXECUTION_CONTRACT_INVALID", cell_id)
    horizon = bodies["horizon_process"]
    recipe = bodies["refit_recipe"]
    try:
        verified = current_refit.verify_current_refit_search_artifacts_bytes_v2(
            winner_process_bytes=_canonical_bytes(bodies["winner_process"]),
            horizon_process_bytes=_canonical_bytes(horizon),
            trial_definition_bytes=_canonical_bytes(bodies["trial_definition"]),
            refit_recipe_bytes=_canonical_bytes(recipe),
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_id=attempt.get("strategy_id"),
            lookback_kind=execution_contract["lookback_kind"],
            lookback_value=execution_contract["lookback_value"],
            fit_start_utc=count_receipt["fit_start_utc"],
            fit_end_utc=count_receipt["fit_end_exclusive_utc"],
            latest_closed_bar_utc=count_receipt["latest_closed_bar_utc"],
            source_snapshot_sha256=count_receipt["native_prefix_content_sha256"],
            source_bar_count=execution_contract["source_bar_count"],
            seed=recipe.get("seed"),
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "SELECTED_ATTEMPT_RAW_V2_REVERIFY_FAILED", (cell_id, type(exc).__name__)
        ) from exc
    process = verified.winner_process
    trial = verified.trial_definition
    if (
        verified.process_candidate_id != process_candidate_id
        or verified.winner_process_sha256 != process_sha256
        or process.get("strategy_module") != "pm_strategies"
        or process.get("strategy_class") != attempt.get("strategy_id")
        or trial.get("strategy_id") != attempt.get("strategy_id")
    ):
        _fail("SELECTED_ATTEMPT_PROCESS_IDENTITY_MISMATCH", cell_id)

    _, fit = _selected_group_records(
        book_body=book_body,
        book_cell=book_cell,
        index=index,
        symbol=symbol,
        timeframe=timeframe,
        cell_id=cell_id,
    )
    fit_payload = fit.get("payload") if type(fit) is dict else None
    detector_record = (
        fit_payload.get("regime_detector_record") if type(fit_payload) is dict else None
    )
    if type(detector_record) is not dict:
        _fail("SELECTED_REGIME_DETECTOR_RECORD_MISSING", cell_id)
    detector_body = dict(detector_record)
    detector_sha = detector_body.pop("record_sha256", None)
    parameters_text = detector_record.get("parameters_canonical_json")
    try:
        detector_parameters = json.loads(parameters_text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ForwardOOSReplayProductionError(
            "SELECTED_REGIME_PARAMETERS_INVALID", (cell_id,)
        ) from exc
    detector_parameters_sha = _sha(detector_parameters)
    if (
        detector_sha != _sha(detector_body)
        or detector_record.get("fit_cutoff_utc") != anchor["selection_cutoff_utc"]
        or detector_record.get("detector_code_sha256")
        != pm_regime.detector_code_sha256()
        or detector_record.get("parameters_sha256") != detector_parameters_sha
        or parameters_text != _canonical_bytes(detector_parameters).decode("ascii")
        or fit_payload.get("regime_params_sha256") != detector_parameters_sha
        or fit_payload.get("regime_detector_record_sha256") != detector_sha
    ):
        _fail("SELECTED_REGIME_DETECTOR_IDENTITY_MISMATCH", cell_id)

    q_value = book_cell.get("adjusted_q_value")
    if type(q_value) not in {int, float} or not math.isfinite(float(q_value)):
        _fail("BOOK_WINNER_PRIORITY_INVALID", cell_id)
    return _VerifiedSpecialist(
        anchor_id=anchor["anchor_id"],
        cell_id=cell_id,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_id=process["strategy_id"],
        strategy_class=process["strategy_class"],
        strategy_module=process["strategy_module"],
        parameters=json.loads(_canonical_bytes(trial["parameters"]).decode("ascii")),
        process_candidate_id=process_candidate_id,
        process_sha256=process_sha256,
        priority_score=1.0 - float(q_value),
        source_count_receipt=json.loads(
            _canonical_bytes(count_receipt).decode("ascii")
        ),
        detector_parameters=json.loads(
            _canonical_bytes(detector_parameters).decode("ascii")
        ),
        detector_parameters_sha256=detector_parameters_sha,
        detector_record=json.loads(_canonical_bytes(detector_record).decode("ascii")),
    )


def _reopen_specialists(
    context: authority_spool.GenerationSpoolContext,
    plans: Sequence[Mapping[str, Any]],
    books: Mapping[str, Mapping[str, Any]],
    symbol_receipts: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[_VerifiedSpecialist, ...]]:
    by_anchor: dict[str, list[_VerifiedSpecialist]] = {
        plan["anchor_id"]: [] for plan in plans
    }
    for symbol, symbol_receipt in zip(
        book.PINNED_SYMBOL_ORDER, symbol_receipts, strict=True
    ):
        _, entries = _verified_manifest(context, symbol_receipt, symbol)
        for anchor in plans:
            anchor_id = anchor["anchor_id"]
            output = _manifest_body(
                context,
                entries,
                f"{anchor_id}/optimizer-output",
                code="HISTORICAL_OPTIMIZER_OUTPUT",
                maximum=32 * 1024 * 1024,
            )
            payload = output.get("optimizer_payload")
            if (
                type(payload) is not dict
                or payload.get("schema_version")
                != historical_adapter.OPTIMIZER_PAYLOAD_SCHEMA
                or payload.get("anchor_id") != anchor_id
                or payload.get("symbol") != symbol
                or payload.get("complete_sparse_state_roster") is not True
                or payload.get("posthoc_evidence_reconstruction") is not False
                or payload.get("confers_causal_current_live_or_capital_authority")
                is not False
            ):
                _fail("HISTORICAL_OPTIMIZER_ADAPTER_PAYLOAD_INVALID", anchor_id, symbol)
            index = _manifest_body(
                context,
                entries,
                f"{anchor_id}/adapter/production-index",
                code="HISTORICAL_ADAPTER_INDEX",
                expected_descriptor=payload.get("production_index"),
                maximum=32 * 1024 * 1024,
            )
            if (
                index.get("schema_version") != historical_adapter.SYMBOL_INDEX_SCHEMA
                or index.get("anchor") != anchor
                or index.get("symbol") != symbol
                or index.get("generation_id") != context.generation_id
                or index.get("generation_context_sha256") != context.context_sha256
                or index.get("cell_count")
                != len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER)
                or index.get("raw_vectors_spooled_before_compaction") is not True
                or index.get("posthoc_evidence_reconstruction") is not False
                or index.get("complete_sparse_state_roster") is not True
                or index.get("confers_causal_current_live_or_capital_authority")
                is not False
            ):
                _fail("HISTORICAL_ADAPTER_INDEX_INVALID", anchor_id, symbol)
            winner_cells = [
                value
                for value in books[anchor_id]["cells"]
                if value.get("symbol") == symbol
                and value.get("historical_state") == "WINNER"
            ]
            for winner_cell in winner_cells:
                by_anchor[anchor_id].append(
                    _selected_artifacts(
                        context=context,
                        entries=entries,
                        anchor=anchor,
                        book_body=books[anchor_id],
                        book_cell=winner_cell,
                        index=index,
                    )
                )
    result: dict[str, tuple[_VerifiedSpecialist, ...]] = {}
    for plan in plans:
        anchor_id = plan["anchor_id"]
        expected = [
            value["cell_id"]
            for value in books[anchor_id]["cells"]
            if value.get("historical_state") == "WINNER"
        ]
        observed = sorted(
            by_anchor[anchor_id], key=lambda value: expected.index(value.cell_id)
        )
        if [value.cell_id for value in observed] != expected or len(
            observed
        ) > _MAX_WINNERS_PER_ANCHOR:
            _fail("HISTORICAL_WINNER_SPECIALIST_ROSTER_INCOMPLETE", anchor_id)
        result[anchor_id] = tuple(observed)
    return result


def _timestamp(value: Any, code: str) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ForwardOOSReplayProductionError(code) from exc
    if pd.isna(stamp):
        _fail(code)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp


def _money_round(value: Decimal) -> Decimal:
    try:
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    except InvalidOperation as exc:
        raise ForwardOOSReplayProductionError("MONEY_ROUND_INVALID") from exc


def _quote(mid: Any, half_spread: Any) -> tuple[Decimal, Decimal, Decimal]:
    middle = _decimal(mid, "QUOTE_MID_INVALID", positive=True)
    half = _decimal(half_spread, "QUOTE_SPREAD_INVALID")
    if half < 0 or middle <= half:
        _fail("QUOTE_GEOMETRY_INVALID")
    return middle, middle - half, middle + half


def _frame_interval(
    frame: pd.DataFrame,
) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    if (
        not isinstance(frame, pd.DataFrame)
        or frame.empty
        or "_BAR_CLOSE_TS" not in frame
    ):
        _fail("NATIVE_FORWARD_FRAME_INVALID")
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    if (
        len(opens) != len(frame)
        or len(closes) != len(frame)
        or opens.isna().any()
        or closes.isna().any()
        or opens.has_duplicates
        or closes.has_duplicates
        or not opens.is_monotonic_increasing
        or not closes.is_monotonic_increasing
        or bool((opens >= closes).any())
    ):
        _fail("NATIVE_FORWARD_FRAME_AXIS_INVALID")
    return opens, closes


def _selection_prefix_match(
    frame: pd.DataFrame,
    specialist: _VerifiedSpecialist,
) -> None:
    receipt = specialist.source_count_receipt
    opens, closes = _frame_interval(frame)
    start = _timestamp(receipt.get("fit_start_utc"), "FIT_START_INVALID")
    end = _timestamp(receipt.get("fit_end_exclusive_utc"), "FIT_END_INVALID")
    mask = (opens >= start) & (opens < end) & (closes <= end)
    selected = frame.loc[mask].copy(deep=True)
    if (
        selected.empty
        or len(selected) != receipt.get("source_bar_count")
        or _frame_sha256(selected) != receipt.get("fit_frame_sha256")
        or _utc_seconds(
            pd.Timestamp(selected["_BAR_CLOSE_TS"].iloc[-1]).isoformat(),
            "FIT_LATEST_CLOSE_INVALID",
        )
        != _utc_seconds(
            receipt.get("latest_closed_bar_utc"), "FIT_LATEST_CLOSE_INVALID"
        )
    ):
        _fail(
            "LATER_NATIVE_PRODUCT_DOES_NOT_EXTEND_EXACT_SELECTION_PREFIX",
            specialist.cell_id,
        )


def _strategy(specialist: _VerifiedSpecialist) -> Any:
    try:
        template = StrategyRegistry.get(specialist.strategy_id)
        defaults = template.get_default_params()
        decoded = decode_parameter_mapping(specialist.parameters, defaults)
        materialized = StrategyRegistry.get(specialist.strategy_id, **decoded)
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FROZEN_STRATEGY_MATERIALIZATION_FAILED",
            (specialist.cell_id, type(exc).__name__),
        ) from exc
    if (
        materialized.__class__.__name__ != specialist.strategy_class
        or materialized.__class__.__module__ != specialist.strategy_module
    ):
        _fail("FROZEN_STRATEGY_CODE_IDENTITY_MISMATCH", specialist.cell_id)
    return materialized


def _signal_vector(
    strategy: Any,
    features: pd.DataFrame,
    specialist: _VerifiedSpecialist,
) -> pd.Series:
    try:
        raw = strategy.generate_signals(features, specialist.symbol)
        signal = pd.Series(raw, index=features.index)
        numeric = pd.to_numeric(signal, errors="coerce")
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FROZEN_STRATEGY_SIGNAL_FAILED",
            (specialist.cell_id, type(exc).__name__),
        ) from exc
    if (
        not signal.index.equals(features.index)
        or numeric.isna().any()
        or bool((~numeric.isin([-1, 0, 1])).any())
        or "REGIME_LIVE" not in features
    ):
        _fail("FROZEN_STRATEGY_SIGNAL_INVALID", specialist.cell_id)
    gated = numeric.astype(np.int32).where(
        features["REGIME_LIVE"].astype(str).str.upper() == specialist.regime,
        0,
    )
    return gated.astype(np.int32)


def _features_and_prefix_parity(
    frame: pd.DataFrame,
    specialists: Sequence[_VerifiedSpecialist],
) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    if not specialists:
        _fail("FEATURE_SPECIALIST_ROSTER_EMPTY")
    first = specialists[0]
    if any(
        value.symbol != first.symbol
        or value.timeframe != first.timeframe
        or value.detector_parameters_sha256 != first.detector_parameters_sha256
        or value.detector_record != first.detector_record
        for value in specialists
    ):
        _fail(
            "SHARED_TIMEFRAME_DETECTOR_SPLICE",
            first.anchor_id,
            first.symbol,
            first.timeframe,
        )
    cutoff = _timestamp(
        first.source_count_receipt["fit_end_exclusive_utc"],
        "SELECTION_CUTOFF_INVALID",
    )
    _, closes = _frame_interval(frame)
    prefix = frame.loc[closes <= cutoff].copy(deep=True)
    if prefix.empty:
        _fail("SELECTION_PREFIX_EMPTY", first.cell_id)
    try:
        full_features = FeatureComputer.compute_all(
            frame,
            symbol=first.symbol,
            timeframe=first.timeframe,
            regime_params_override=dict(first.detector_parameters),
            regime_params_sha256=first.detector_parameters_sha256,
        )
        prefix_features = FeatureComputer.compute_all(
            prefix,
            symbol=first.symbol,
            timeframe=first.timeframe,
            regime_params_override=dict(first.detector_parameters),
            regime_params_sha256=first.detector_parameters_sha256,
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FROZEN_FEATURE_EXECUTION_FAILED",
            (first.cell_id, type(exc).__name__),
        ) from exc
    if (
        not prefix_features.index.equals(prefix.index)
        or not full_features.loc[prefix.index].equals(prefix_features)
        or any(name not in full_features for name in _REGIME_COLUMNS)
    ):
        _fail("FORWARD_FEATURE_PREFIX_INVARIANCE_FAILED", first.cell_id)
    signals: dict[str, pd.Series] = {}
    for specialist in specialists:
        materialized = _strategy(specialist)
        full_signal = _signal_vector(materialized, full_features, specialist)
        prefix_signal = _signal_vector(materialized, prefix_features, specialist)
        if not full_signal.loc[prefix.index].equals(prefix_signal):
            _fail("FORWARD_SIGNAL_PREFIX_INVARIANCE_FAILED", specialist.cell_id)
        signals[specialist.cell_id] = full_signal
    return full_features, signals


def _exit_quote(
    *,
    trade: Mapping[str, Any],
    features: pd.DataFrame,
    half_spread: np.ndarray,
    slippage_price: float,
) -> tuple[Decimal, Decimal, Decimal, pd.Timestamp]:
    exit_bar = int(trade["exit_bar"])
    if not 0 <= exit_bar < len(features):
        _fail("BACKTEST_EXIT_BAR_INVALID")
    side = str(trade.get("direction") or "").upper()
    sign = 1 if side == "LONG" else -1 if side == "SHORT" else 0
    if not sign:
        _fail("BACKTEST_DIRECTION_INVALID")
    fill = float(trade["exit_price"])
    half = float(half_spread[exit_bar])
    opened = float(features["Open"].iloc[exit_bar])
    closed = float(features["Close"].iloc[exit_bar])
    executable_open = opened - (sign * half)
    reason = str(trade.get("exit_reason") or "").lower()
    tolerance = max(1e-12, abs(fill) * 1e-12)
    if math.isclose(fill, executable_open, rel_tol=0.0, abs_tol=tolerance):
        mid = opened
        effective_time = _timestamp(
            features.index[exit_bar], "BACKTEST_EXIT_OPEN_TIME_INVALID"
        )
    elif reason in _EOD_EXIT_REASONS:
        mid = closed
        effective_time = _timestamp(
            features["_BAR_CLOSE_TS"].iloc[exit_bar],
            "BACKTEST_EXIT_CLOSE_TIME_INVALID",
        ) - pd.Timedelta(seconds=1)
    elif "governance" in reason or "policy" in reason:
        mid = opened
        effective_time = _timestamp(
            features.index[exit_bar], "BACKTEST_EXIT_OPEN_TIME_INVALID"
        )
    elif "sl" in reason or "stop" in reason:
        stop = float(trade["stop_loss"])
        expected = stop - (sign * slippage_price)
        if not math.isclose(fill, expected, rel_tol=0.0, abs_tol=tolerance):
            _fail("BACKTEST_STOP_FILL_NOT_RECONSTRUCTIBLE")
        mid = stop + (sign * half)
        effective_time = _timestamp(
            features["_BAR_CLOSE_TS"].iloc[exit_bar],
            "BACKTEST_EXIT_CLOSE_TIME_INVALID",
        ) - pd.Timedelta(seconds=1)
    elif "tp" in reason or "target" in reason:
        target = float(trade["take_profit"])
        if not math.isclose(fill, target, rel_tol=0.0, abs_tol=tolerance):
            _fail("BACKTEST_TARGET_FILL_NOT_RECONSTRUCTIBLE")
        mid = target + (sign * half)
        effective_time = _timestamp(
            features["_BAR_CLOSE_TS"].iloc[exit_bar],
            "BACKTEST_EXIT_CLOSE_TIME_INVALID",
        ) - pd.Timedelta(seconds=1)
    else:
        _fail("BACKTEST_EXIT_REASON_UNSUPPORTED", reason)
    recorded = _timestamp(trade.get("exit_time"), "BACKTEST_EXIT_TIME_INVALID")
    bar_open = _timestamp(features.index[exit_bar], "BACKTEST_EXIT_OPEN_TIME_INVALID")
    bar_close = _timestamp(
        features["_BAR_CLOSE_TS"].iloc[exit_bar], "BACKTEST_EXIT_CLOSE_TIME_INVALID"
    )
    if recorded != bar_open or not bar_open <= effective_time < bar_close:
        _fail("BACKTEST_EXIT_TIME_AXIS_INVALID")
    return (*_quote(mid, half), effective_time)


def _daily_swap_costs_per_lot(
    *,
    trade: Mapping[str, Any],
    features: pd.DataFrame,
    pre_open_units: np.ndarray,
    close_units: np.ndarray,
    per_lot_rate: np.ndarray,
) -> _SwapCostEvidence:
    entry_bar = int(trade["entry_bar"])
    exit_bar = int(trade["exit_bar"])
    if not 0 <= entry_bar <= exit_bar < len(features):
        _fail("BACKTEST_SWAP_POSITION_AXIS_INVALID")
    opens, closes = _frame_interval(features)
    is_eod = str(trade.get("exit_reason") or "").lower() in _EOD_EXIT_REASONS
    units_by_day: dict[str, int] = {}
    events: list[tuple[pd.Timestamp, Decimal]] = []
    total_units = 0
    rate = _decimal(per_lot_rate[entry_bar], "BACKTEST_SWAP_RATE_INVALID")
    for index in range(entry_bar, exit_bar + 1):
        pre_units = int(pre_open_units[index]) if index > entry_bar else 0
        close_count = int(close_units[index]) if index < exit_bar or is_eod else 0
        if pre_units:
            day = opens[index].date().isoformat()
            units_by_day[day] = units_by_day.get(day, 0) + pre_units
            events.append((opens[index], -(rate * Decimal(pre_units))))
        if close_count:
            effective_close = closes[index] - pd.Timedelta(nanoseconds=1)
            day = effective_close.date().isoformat()
            units_by_day[day] = units_by_day.get(day, 0) + close_count
            events.append((effective_close, -(rate * Decimal(close_count))))
        total_units += pre_units + close_count
    if total_units != int(trade.get("swap_rollover_units", -1)):
        _fail("BACKTEST_SWAP_UNIT_RECONCILIATION_FAILED")
    size = _decimal(
        trade.get("position_size"), "BACKTEST_POSITION_SIZE_INVALID", positive=True
    )
    exact_cash = rate * Decimal(total_units) * size
    if _money_round(exact_cash) != _decimal(
        trade.get("swap_dollars"), "BACKTEST_SWAP_CASH_INVALID"
    ):
        _fail("BACKTEST_SWAP_CASH_RECONCILIATION_FAILED")
    daily = {day: -(rate * Decimal(units)) for day, units in units_by_day.items()}
    if sum((value for _, value in events), Decimal(0)) != sum(
        daily.values(), Decimal(0)
    ):
        _fail("BACKTEST_SWAP_EVENT_DAILY_RECONCILIATION_FAILED")
    return _SwapCostEvidence(
        daily_costs=daily,
        events=tuple(sorted(events, key=lambda value: value[0])),
    )


def _valuation_mark_indices(
    tape: _M5ValuationTape,
    entry_time: pd.Timestamp,
    exit_time: pd.Timestamp,
) -> tuple[int, int]:
    # A same-timestamp mark is processed before admission by the synchronized
    # simulator, so exclude it instead of manufacturing a pre-entry valuation.
    left = int(tape.opens.searchsorted(entry_time, side="right"))
    right = int(tape.opens.searchsorted(exit_time, side="right"))
    if left > right or left < 0 or right > len(tape.frame):
        _fail("M5_VALUATION_POSITION_AXIS_INVALID", tape.symbol)
    return left, right


def _accumulated_swap_cost(
    evidence: _SwapCostEvidence, effective_time: pd.Timestamp
) -> Decimal:
    return sum(
        (cost for stamp, cost in evidence.events if stamp <= effective_time),
        Decimal(0),
    )


def _executable_mark_net_per_lot(
    *,
    side: str,
    bid: Decimal,
    ask: Decimal,
    base_entry_fill: Decimal,
    contract_multiplier: Decimal,
    commission_per_lot: Decimal,
    accumulated_financing_cost: Decimal,
) -> Decimal:
    sign = Decimal(1) if side == "LONG" else Decimal(-1)
    liquidation = bid if side == "LONG" else ask
    return (
        sign * contract_multiplier * (liquidation - base_entry_fill)
        - commission_per_lot
        - accumulated_financing_cost
    )


def _m5_valuation_marks(
    *,
    trade: Mapping[str, Any],
    tape: _M5ValuationTape,
    base_swap_cost: _SwapCostEvidence,
    stress_swap_cost: _SwapCostEvidence,
    base_entry_fill: Decimal,
    stop_price: Decimal,
    effective_exit_time: pd.Timestamp,
    contract_multiplier: Decimal,
    commission_per_lot: Decimal,
) -> tuple[tuple[portfolio.MarkPoint, ...], tuple[_DailyMark, ...]]:
    side = str(trade["direction"]).upper()
    if side not in {"LONG", "SHORT"}:
        _fail("BACKTEST_DIRECTION_INVALID")
    entry_time = _timestamp(trade["entry_time"], "BACKTEST_ENTRY_TIME_INVALID")
    exit_time = _timestamp(effective_exit_time, "BACKTEST_EXIT_TIME_INVALID")
    if exit_time < entry_time:
        _fail("M5_VALUATION_POSITION_TIME_INVALID")
    left, right = _valuation_mark_indices(tape, entry_time, exit_time)
    candidate_marks: list[portfolio.MarkPoint] = []
    for index in range(left, right):
        marked_at = tape.opens[index]
        mid, bid, ask = _quote(
            tape.frame["Open"].iloc[index], tape.entry_half_spread[index]
        )
        if marked_at < exit_time and (
            (side == "LONG" and bid <= stop_price)
            or (side == "SHORT" and ask >= stop_price)
        ):
            _fail("M5_OPEN_MARK_CROSSES_PROTECTIVE_STOP", tape.symbol)
        net = _executable_mark_net_per_lot(
            side=side,
            bid=bid,
            ask=ask,
            base_entry_fill=base_entry_fill,
            contract_multiplier=contract_multiplier,
            commission_per_lot=commission_per_lot,
            accumulated_financing_cost=_accumulated_swap_cost(
                base_swap_cost, marked_at
            ),
        )
        candidate_marks.append(
            portfolio.MarkPoint(
                time=marked_at,
                observation_time=marked_at,
                pnl_per_current_lot=float(net),
            )
        )

    entry_day = entry_time.date()
    exit_day = exit_time.date()
    daily: list[_DailyMark] = []
    for offset in range((exit_day - entry_day).days + 1):
        current = entry_day + timedelta(days=offset)
        day_text = current.isoformat()
        day_start = pd.Timestamp(current, tz="UTC")
        day_end = day_start + pd.Timedelta(days=1)
        boundary = min(day_end - pd.Timedelta(seconds=1), exit_time)
        candidate_index = int(tape.opens.searchsorted(boundary, side="right") - 1)
        if candidate_index < 0 or tape.opens[candidate_index] < entry_time:
            _fail("M5_DAILY_VALUATION_QUOTE_UNAVAILABLE", tape.symbol, day_text)
        observed_at = tape.opens[candidate_index]
        mid, bid, ask = _quote(
            tape.frame["Open"].iloc[candidate_index],
            tape.entry_half_spread[candidate_index],
        )
        # On a broker closure the last authenticated M5 Open is carried
        # forward only after it was observed.  The effective mark remains in
        # the current UTC day because the causal ledger has no observation-time
        # field; candidate marks retain the exact original M5 observation time.
        marked_at = observed_at if observed_at >= day_start else boundary
        if marked_at < entry_time or marked_at > exit_time:
            _fail("M5_DAILY_VALUATION_TIME_INVALID", tape.symbol, day_text)
        base_finance = base_swap_cost.daily_costs.get(day_text, Decimal(0))
        stress_finance = stress_swap_cost.daily_costs.get(day_text, Decimal(0))
        if stress_finance < base_finance:
            _fail("STRESS_FINANCING_LESS_SEVERE")
        net = _executable_mark_net_per_lot(
            side=side,
            bid=bid,
            ask=ask,
            base_entry_fill=base_entry_fill,
            contract_multiplier=contract_multiplier,
            commission_per_lot=commission_per_lot,
            accumulated_financing_cost=_accumulated_swap_cost(
                base_swap_cost, marked_at
            ),
        )
        daily.append(
            _DailyMark(
                date=day_text,
                marked_at_utc=_utc_seconds(
                    marked_at.isoformat(), "DAILY_MARK_TIME_INVALID"
                ),
                mid_price=mid,
                bid_price=bid,
                ask_price=ask,
                base_financing_per_lot=base_finance,
                stress_financing_per_lot=stress_finance,
                candidate_net_pnl_per_lot=float(net),
            )
        )
    return tuple(candidate_marks), tuple(daily)


def _position_evidence(
    *,
    specialist: _VerifiedSpecialist,
    capital_phase: str,
    strategy: Any,
    features: pd.DataFrame,
    spec: InstrumentSpec,
    config: PipelineConfig,
    base_result: Mapping[str, Any],
    stress_result: Mapping[str, Any],
    valuation_tape: _M5ValuationTape,
    resource_budget: _ResourceBudget,
) -> tuple[_PositionEvidence, ...]:
    if capital_phase not in CAPITAL_PHASES:
        _fail("CAPITAL_PHASE_INVALID", capital_phase)
    if (
        type(valuation_tape) is not _M5ValuationTape
        or valuation_tape.symbol != specialist.symbol
        or valuation_tape.execution_spec_receipt.get("instrument_spec") != asdict(spec)
    ):
        _fail("M5_VALUATION_EXECUTION_SPEC_SPLICE", specialist.cell_id)
    base_trades = base_result.get("trades")
    base_events = base_result.get("position_events")
    stress_trades = stress_result.get("trades")
    stress_events = stress_result.get("position_events")
    if not all(
        type(value) is list
        for value in (base_trades, base_events, stress_trades, stress_events)
    ):
        _fail("BACKTEST_POSITION_EVIDENCE_MISSING", specialist.cell_id)
    if len(base_trades) != len(stress_trades) or len(base_events) != len(stress_events):
        _fail("BACKTEST_STRESS_POSITION_ROSTER_MISMATCH", specialist.cell_id)
    namespace = "|".join(
        (
            specialist.symbol,
            specialist.timeframe,
            str(getattr(strategy, "name", type(strategy).__name__)),
        )
    )
    try:
        rebuilt_base_events = build_backtest_position_events(
            base_trades, namespace=namespace
        )
        rebuilt_stress_events = build_backtest_position_events(
            stress_trades, namespace=namespace
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "BACKTEST_POSITION_EVENTS_V2_REBUILD_FAILED",
            (specialist.cell_id, type(exc).__name__, str(exc)),
        ) from exc
    if rebuilt_base_events != base_events or rebuilt_stress_events != stress_events:
        _fail("BACKTEST_POSITION_EVENTS_V2_RAW_REVERIFY_FAILED", specialist.cell_id)
    by_position: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for trade, stress_trade in zip(base_trades, stress_trades, strict=True):
        if type(trade) is not dict or type(stress_trade) is not dict:
            _fail("BACKTEST_TRADE_RECORD_INVALID")
        single = build_backtest_position_events([trade], namespace=namespace)
        stress_single = build_backtest_position_events(
            [stress_trade], namespace=namespace
        )
        if (
            len(single) != 1
            or single[0]["position_id"] != stress_single[0]["position_id"]
        ):
            _fail("BACKTEST_POSITION_IDENTITY_INVALID")
        position_id = single[0]["position_id"]
        if position_id in by_position:
            _fail("PARTIAL_OR_MULTI_LEG_POSITION_NOT_REPRESENTABLE", position_id)
        by_position[position_id] = (trade, stress_trade)
    base_event_ids = {value.get("position_id") for value in base_events}
    stress_event_ids = {value.get("position_id") for value in stress_events}
    if base_event_ids != set(by_position) or stress_event_ids != set(by_position):
        _fail("BACKTEST_STRESS_POSITION_ROSTER_MISMATCH", specialist.cell_id)
    resource_budget.reserve_positions(len(by_position), specialist.anchor_id)
    # P&L differs by lane, so exact event equality is not expected.  The
    # immutable event identities and ordering must nevertheless match.
    for left, right in zip(base_events, stress_events, strict=True):
        if any(
            left.get(name) != right.get(name)
            for name in (
                "trade_record_schema_version",
                "position_id",
                "entry_time",
                "exit_time",
                "entry_event_order",
                "exit_event_order",
                "risk_amount",
            )
        ):
            _fail("BACKTEST_STRESS_POSITION_IDENTITY_MISMATCH")
    base_backtester = Backtester(config)
    stress_mode = str(config.oos_evidence_execution_stress_mode).strip().lower()
    stress_backtester = Backtester(replace(config, backtest_spread_mode=stress_mode))
    base_entry_spread, base_half_spread, _ = base_backtester._build_half_spread_arrays(
        features, spec
    )
    _, stress_half_spread, _ = stress_backtester._build_half_spread_arrays(
        features, spec
    )
    slippage = spec.pips_to_price(config.slippage_pips) if config.use_slippage else 0.0
    entry_prices = np.full(len(features), np.nan, dtype=np.float64)
    stress_entry_prices = np.full(len(features), np.nan, dtype=np.float64)
    for trade, stress_trade in by_position.values():
        entry_prices[int(trade["entry_bar"])] = float(trade["entry_price"])
        stress_entry_prices[int(stress_trade["entry_bar"])] = float(
            stress_trade["entry_price"]
        )
    (
        base_pre,
        base_close,
        base_long,
        base_short,
        base_causal,
        _,
    ) = base_backtester._build_causal_swap_inputs(
        features, spec, entry_prices, timeframe=specialist.timeframe
    )
    (
        stress_pre,
        stress_close,
        stress_long,
        stress_short,
        stress_causal,
        _,
    ) = stress_backtester._build_causal_swap_inputs(
        features, spec, stress_entry_prices, timeframe=specialist.timeframe
    )
    if (
        base_causal != stress_causal
        or not np.array_equal(base_pre, stress_pre)
        or not np.array_equal(base_close, stress_close)
    ):
        _fail("BACKTEST_SWAP_PROJECTION_STRESS_DRIFT")
    contract_multiplier = _decimal(
        spec.tick_value, "TICK_VALUE_INVALID", positive=True
    ) / _decimal(spec.tick_size, "TICK_SIZE_INVALID", positive=True)
    commission = (
        _decimal(spec.commission_per_lot, "COMMISSION_INVALID")
        if config.use_commission
        else Decimal(0)
    )
    if commission < 0:
        _fail("COMMISSION_NEGATIVE")
    positions: list[_PositionEvidence] = []
    for position_id in sorted(by_position):
        trade, stress_trade = by_position[position_id]
        entry_bar = int(trade["entry_bar"])
        side = str(trade["direction"]).upper()
        sign = Decimal(1) if side == "LONG" else Decimal(-1)
        entry_mid, entry_bid, entry_ask = _quote(
            features["Open"].iloc[entry_bar], base_entry_spread[entry_bar]
        )
        base_entry = _decimal(
            trade["entry_price"], "BASE_ENTRY_FILL_INVALID", positive=True
        )
        stress_entry = _decimal(
            stress_trade["entry_price"], "STRESS_ENTRY_FILL_INVALID", positive=True
        )
        executable_entry = entry_ask if side == "LONG" else entry_bid
        expected_base_entry = executable_entry + sign * _decimal(
            slippage, "SLIPPAGE_PRICE_INVALID"
        )
        tolerance = max(
            Decimal("0.000000000001"), abs(base_entry) * Decimal("0.000000000001")
        )
        if abs(base_entry - expected_base_entry) > tolerance:
            _fail("BACKTEST_ENTRY_FILL_RECONSTRUCTION_FAILED", position_id)
        if sign * (stress_entry - base_entry) < 0:
            _fail("BACKTEST_STRESS_ENTRY_IMPROVES", position_id)
        exit_mid, exit_bid, exit_ask, effective_exit_time = _exit_quote(
            trade=trade,
            features=features,
            half_spread=base_half_spread,
            slippage_price=slippage,
        )
        base_exit = _decimal(
            trade["exit_price"], "BASE_EXIT_FILL_INVALID", positive=True
        )
        stress_exit = _decimal(
            stress_trade["exit_price"], "STRESS_EXIT_FILL_INVALID", positive=True
        )
        if stress_exit != base_exit:
            _fail("FROZEN_PATH_STRESS_EXIT_FILL_CHANGED", position_id)
        stop = _decimal(trade["stop_loss"], "BACKTEST_STOP_INVALID", positive=True)
        if (side == "LONG" and stop >= entry_mid) or (
            side == "SHORT" and stop <= entry_mid
        ):
            _fail("BACKTEST_OPENING_STOP_NOT_ADVERSE", position_id)
        size = _decimal(
            trade["position_size"], "BACKTEST_POSITION_SIZE_INVALID", positive=True
        )
        stress_size = _decimal(
            stress_trade["position_size"], "BACKTEST_STRESS_SIZE_INVALID", positive=True
        )
        if size != stress_size:
            _fail("BACKTEST_STRESS_POSITION_SIZE_CHANGED", position_id)
        base_rates = base_long if side == "LONG" else base_short
        stress_rates = stress_long if side == "LONG" else stress_short
        base_swap = _daily_swap_costs_per_lot(
            trade=trade,
            features=features,
            pre_open_units=base_pre,
            close_units=base_close,
            per_lot_rate=base_rates,
        )
        stress_swap = _daily_swap_costs_per_lot(
            trade=stress_trade,
            features=features,
            pre_open_units=stress_pre,
            close_units=stress_close,
            per_lot_rate=stress_rates,
        )
        valuation_left, valuation_right = _valuation_mark_indices(
            valuation_tape,
            _timestamp(trade["entry_time"], "BACKTEST_ENTRY_TIME_INVALID"),
            effective_exit_time,
        )
        resource_budget.reserve_marks(
            valuation_right - valuation_left, specialist.anchor_id
        )
        candidate_marks, marks = _m5_valuation_marks(
            trade=trade,
            tape=valuation_tape,
            base_swap_cost=base_swap,
            stress_swap_cost=stress_swap,
            base_entry_fill=base_entry,
            stop_price=stop,
            effective_exit_time=effective_exit_time,
            contract_multiplier=contract_multiplier,
            commission_per_lot=commission,
        )
        if any(
            (side == "LONG" and stop >= mark.bid_price)
            or (side == "SHORT" and stop <= mark.ask_price)
            for mark in marks[:-1]
        ):
            _fail("OPEN_POSITION_MARK_CROSSES_PROTECTIVE_STOP", position_id)
        base_swap_per_lot = -sum(base_swap.daily_costs.values(), Decimal(0))
        price_pnl_per_lot = sign * (base_exit - base_entry) * contract_multiplier
        exact_net_per_lot = price_pnl_per_lot - commission + base_swap_per_lot
        exact_gross_cash = (price_pnl_per_lot - commission) * size
        exact_net_cash = exact_net_per_lot * size
        if _money_round(exact_gross_cash) != _decimal(
            trade["gross_pnl_dollars"], "BACKTEST_GROSS_PNL_INVALID"
        ) or _money_round(exact_net_cash) != _decimal(
            trade["pnl_dollars"], "BACKTEST_NET_PNL_INVALID"
        ):
            _fail("BACKTEST_POSITION_CASH_RECONCILIATION_FAILED", position_id)
        adverse_stop_fill = stop - sign * _decimal(
            slippage, "STOP_SLIPPAGE_PRICE_INVALID"
        )
        stop_risk = (
            abs(base_entry - adverse_stop_fill) * contract_multiplier + commission
        )
        margin_per_lot = abs(base_entry) * contract_multiplier
        candidate_id = (
            "oos:"
            + hashlib.sha256(
                (
                    f"{specialist.anchor_id}|{capital_phase}|"
                    f"{specialist.cell_id}|{position_id}"
                ).encode("ascii")
            ).hexdigest()
        )
        candidate = portfolio.CandidateTrade(
            candidate_id=candidate_id,
            cell_id=specialist.cell_id,
            symbol=specialist.symbol,
            timeframe=specialist.timeframe,
            regime=specialist.regime,
            strategy_name=specialist.strategy_id,
            promotion_status="HISTORICAL_OOS_REPLAY",
            asset_class=str(spec.asset_class),
            risk_groups=(),
            entry_time=trade["entry_time"],
            direction=1 if side == "LONG" else -1,
            stop_risk_per_lot=float(stop_risk),
            margin_per_lot=float(margin_per_lot),
            volume_min=float(spec.min_lot),
            volume_max=float(spec.max_lot),
            volume_step=float(spec.volume_step),
            priority_score=specialist.priority_score,
            legs=(
                portfolio.ExitLeg(
                    exit_time=effective_exit_time.isoformat(),
                    fraction=1.0,
                    net_pnl_per_entry_lot=float(exact_net_per_lot),
                    price_pnl_per_entry_lot=float(price_pnl_per_lot),
                    commission_per_entry_lot=float(-commission),
                    swap_per_entry_lot=float(base_swap_per_lot),
                    exit_reason=str(trade["exit_reason"]),
                ),
            ),
            marks=candidate_marks,
            source_schema=POSITION_SOURCE_SCHEMA,
            margin_model=MARGIN_MODEL,
        ).normalized()
        positions.append(
            _PositionEvidence(
                candidate=candidate,
                position_id=position_id,
                cell_id=specialist.cell_id,
                symbol=specialist.symbol,
                timeframe=specialist.timeframe,
                regime=specialist.regime,
                side=side,
                contract_multiplier=contract_multiplier,
                stop_price=stop,
                entry_mid=entry_mid,
                entry_bid=entry_bid,
                entry_ask=entry_ask,
                base_entry_fill=base_entry,
                stress_entry_fill=stress_entry,
                exit_mid=exit_mid,
                exit_bid=exit_bid,
                exit_ask=exit_ask,
                base_exit_fill=base_exit,
                stress_exit_fill=stress_exit,
                base_commission_per_lot=commission,
                stress_commission_per_lot=commission,
                margin_per_lot=margin_per_lot,
                marks=marks,
            )
        )
    return tuple(positions)


def _open_m5_valuation_tape(
    *,
    context: authority_spool.GenerationSpoolContext,
    product: native_prefix.NativePrefixProduct,
    expected_identity: Mapping[str, Any],
    config: PipelineConfig,
    deployment: _DeploymentBinding,
    forward_start: pd.Timestamp,
    forward_end_exclusive: pd.Timestamp,
) -> tuple[_M5ValuationTape, InstrumentSpec]:
    reopened = _reopen_bound_native_product(
        context=context,
        product=product,
        expected_identity=expected_identity,
        config=config,
        production_contract_required=not bool(config.research_mode),
    )
    spec, spec_receipt = _freeze_execution_spec(
        product=reopened, config=config, deployment=deployment
    )
    try:
        opened = native_prefix.open_verified_prefix_frame(reopened)
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_M5_VALUATION_FRAME_REOPEN_FAILED",
            (expected_identity.get("symbol"), type(exc).__name__, str(exc)),
        ) from exc
    opens, closes = _frame_interval(opened)
    end_index = int(closes.searchsorted(forward_end_exclusive, side="right"))
    first_forward = int(opens.searchsorted(forward_start, side="left"))
    # Retain only the forward tape plus one already-observed predecessor for
    # causal weekend/session carry.  No M5 observations are thinned inside the
    # retained interval, and the independently verified source prefix remains
    # content-bound by ``expected_identity``.
    start_index = max(0, first_forward - 1)
    frame = opened.iloc[start_index:end_index].copy(deep=True)
    del opened
    opens, _ = _frame_interval(frame)
    if (
        frame.empty
        or expected_identity.get("timeframe") != "M5"
        or str(expected_identity.get("symbol")) != str(spec.symbol)
        or not bool((opens < forward_end_exclusive).all())
        or not bool((opens <= forward_start).any())
    ):
        _fail("FORWARD_M5_VALUATION_GEOMETRY_INVALID", expected_identity.get("role"))
    memory_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    if memory_bytes > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
        _fail(
            "FORWARD_M5_VALUATION_MEMORY_BOUND_EXCEEDED",
            expected_identity.get("symbol"),
            memory_bytes,
        )
    backtester = Backtester(config)
    try:
        entry_half, intrabar_half, spread_meta = backtester._build_half_spread_arrays(
            frame, spec
        )
        slippage_price = (
            spec.pips_to_price(config.slippage_pips) if config.use_slippage else 0.0
        )
        cost_attestation = backtester._numerical_execution_cost_attestation(
            entry_half_spread=entry_half,
            intrabar_half_spread=intrabar_half,
            spread_meta=spread_meta,
            slippage_price=slippage_price,
            spec=spec,
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_M5_VALUATION_COST_SURFACE_FAILED",
            (expected_identity.get("symbol"), type(exc).__name__, str(exc)),
        ) from exc
    tape = _M5ValuationTape(
        symbol=str(expected_identity["symbol"]),
        frame=frame,
        opens=opens,
        entry_half_spread=np.asarray(entry_half, dtype=np.float64),
        spread_meta={
            "spread_model": json.loads(_canonical_bytes(spread_meta).decode("ascii")),
            "cost_attestation": json.loads(
                _canonical_bytes(cost_attestation).decode("ascii")
            ),
            "valuation_price_field": "Open",
            "valuation_timeframe": "M5",
            "executable_liquidation": "BID_FOR_LONG_ASK_FOR_SHORT",
            "round_trip_commission_deducted_at_every_mark": True,
        },
        execution_spec_receipt=spec_receipt,
        native_product_identity=dict(expected_identity),
    )
    return tape, spec


def _execute_anchor_replay(
    *,
    context: authority_spool.GenerationSpoolContext,
    anchor: Mapping[str, Any],
    specialists: Sequence[_VerifiedSpecialist],
    native_products: Mapping[str, native_prefix.NativePrefixProduct],
    valuation_products: Mapping[str, native_prefix.NativePrefixProduct],
    precommit_anchor: Mapping[str, Any],
    capital_phase: str,
    capital_window: Mapping[str, Any],
    config: PipelineConfig,
    deployment: _DeploymentBinding,
) -> _AnchorRawReplay:
    phase_id = _capital_phase_id(str(anchor.get("anchor_id")), capital_phase)
    grouped: dict[str, list[_VerifiedSpecialist]] = {}
    for specialist in specialists:
        grouped.setdefault(
            native_product_key(specialist.symbol, specialist.timeframe), []
        ).append(specialist)
    expected_valuation_keys = sorted(
        {native_product_key(value.symbol, "M5") for value in specialists}
    )
    if type(native_products) is not dict or set(native_products) != set(grouped):
        _fail("ANCHOR_NATIVE_PRODUCT_ROSTER_MISMATCH", anchor["anchor_id"])
    if (
        type(valuation_products) is not dict
        or sorted(valuation_products) != expected_valuation_keys
    ):
        _fail("ANCHOR_M5_VALUATION_PRODUCT_ROSTER_MISMATCH", anchor["anchor_id"])
    forward_start, forward_end_exclusive = _capital_window_datetimes(
        capital_window, phase_id=phase_id
    )
    outer_end_exclusive = pd.Timestamp(
        _date(anchor["forward_end_date"], "FORWARD_END_INVALID") + timedelta(days=1),
        tz="UTC",
    )
    expected_phase_windows = precommit_anchor.get("capital_phase_windows")
    if (
        type(precommit_anchor) is not dict
        or precommit_anchor.get("anchor_id") != anchor["anchor_id"]
        or precommit_anchor.get("forward_end_exclusive_utc")
        != _utc_seconds(
            outer_end_exclusive.isoformat(), "FORWARD_END_EXCLUSIVE_INVALID"
        )
        or type(expected_phase_windows) is not dict
        or expected_phase_windows.get(capital_phase) != dict(capital_window)
        or type(precommit_anchor.get("execution_products")) is not list
        or type(precommit_anchor.get("valuation_m5_products")) is not list
        or precommit_anchor.get("execution_product_roster_sha256")
        != _sha(precommit_anchor.get("execution_products"))
        or precommit_anchor.get("valuation_m5_product_roster_sha256")
        != _sha(precommit_anchor.get("valuation_m5_products"))
        or precommit_anchor.get("valuation_products_supplied_separately") is not True
    ):
        _fail("FORWARD_PRECOMMIT_ANCHOR_IDENTITY_INVALID", anchor["anchor_id"])
    execution_identities = {
        str(value.get("role")).rsplit(":", 1)[-1]: value
        for value in precommit_anchor["execution_products"]
        if type(value) is dict
    }
    valuation_identities = {
        str(value.get("role")).rsplit(":", 1)[-1]: value
        for value in precommit_anchor["valuation_m5_products"]
        if type(value) is dict
    }
    if (
        set(execution_identities) != set(grouped)
        or set(valuation_identities) != set(expected_valuation_keys)
        or len(execution_identities) != len(precommit_anchor["execution_products"])
        or len(valuation_identities) != len(precommit_anchor["valuation_m5_products"])
    ):
        _fail("FORWARD_PRECOMMIT_PRODUCT_ROSTER_INVALID", anchor["anchor_id"])
    native_rows = sum(
        int(value["prefix_rows"])
        for value in (*execution_identities.values(), *valuation_identities.values())
    )
    native_bytes = sum(
        int(value["prefix_bytes"])
        for value in (*execution_identities.values(), *valuation_identities.values())
    )
    if native_rows > _MAX_NATIVE_ROWS_PER_ANCHOR:
        _fail("FORWARD_NATIVE_ROW_BOUND_EXCEEDED", anchor["anchor_id"], native_rows)
    if native_bytes > _MAX_NATIVE_BYTES_PER_ANCHOR:
        _fail("FORWARD_NATIVE_BYTE_BOUND_EXCEEDED", anchor["anchor_id"], native_bytes)
    resource_budget = _ResourceBudget(
        native_rows=native_rows,
        native_bytes=native_bytes,
    )
    positions: list[_PositionEvidence] = []
    execution_spec_rows: list[dict[str, Any]] = []
    valuation_rows: list[dict[str, Any]] = []
    grouped_by_symbol: dict[str, list[str]] = {}
    for key in grouped:
        grouped_by_symbol.setdefault(key.split("|", 1)[0], []).append(key)
    for symbol in sorted(grouped_by_symbol):
        valuation_key = native_product_key(symbol, "M5")
        symbol_bytes = int(valuation_identities[valuation_key]["prefix_bytes"]) + sum(
            int(execution_identities[key]["prefix_bytes"])
            for key in grouped_by_symbol[symbol]
        )
        if symbol_bytes > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
            _fail(
                "FORWARD_ACTIVE_SYMBOL_NATIVE_BYTE_BOUND_EXCEEDED",
                anchor["anchor_id"],
                symbol,
                symbol_bytes,
            )
        tape, valuation_spec = _open_m5_valuation_tape(
            context=context,
            product=valuation_products[valuation_key],
            expected_identity=valuation_identities[valuation_key],
            config=config,
            deployment=deployment,
            forward_start=forward_start,
            forward_end_exclusive=forward_end_exclusive,
        )
        valuation_rows.append(
            {
                "symbol": symbol,
                "native_product_identity_sha256": _sha(tape.native_product_identity),
                "execution_spec_receipt_sha256": tape.execution_spec_receipt[
                    "receipt_sha256"
                ],
                "spread_and_cost_surface": dict(tape.spread_meta),
                "m5_rows_opened": len(tape.frame),
                "m5_frame_memory_bytes": int(
                    tape.frame.memory_usage(index=True, deep=True).sum()
                ),
            }
        )
        try:
            for key in sorted(grouped_by_symbol[symbol]):
                expected_identity = execution_identities[key]
                product = _reopen_bound_native_product(
                    context=context,
                    product=native_products[key],
                    expected_identity=expected_identity,
                    config=config,
                    production_contract_required=not bool(config.research_mode),
                )
                spec, spec_receipt = _freeze_execution_spec(
                    product=product, config=config, deployment=deployment
                )
                if (
                    asdict(spec) != asdict(valuation_spec)
                    or spec_receipt.get("instrument_spec_sha256")
                    != tape.execution_spec_receipt.get("instrument_spec_sha256")
                    or spec_receipt.get("broker_record_sha256")
                    != tape.execution_spec_receipt.get("broker_record_sha256")
                    or spec_receipt.get("configured_instrument_record_sha256")
                    != tape.execution_spec_receipt.get(
                        "configured_instrument_record_sha256"
                    )
                ):
                    _fail(
                        "EXECUTION_AND_M5_VALUATION_SPEC_MISMATCH",
                        anchor["anchor_id"],
                        key,
                    )
                execution_spec_rows.append(
                    {
                        "product_key": key,
                        "native_product_identity_sha256": _sha(expected_identity),
                        "execution_spec_receipt": spec_receipt,
                    }
                )
                try:
                    opened = native_prefix.open_verified_prefix_frame(product)
                except Exception as exc:
                    raise ForwardOOSReplayProductionError(
                        "FORWARD_NATIVE_PRODUCT_FRAME_REOPEN_FAILED",
                        (anchor["anchor_id"], key, type(exc).__name__, str(exc)),
                    ) from exc
                _, closes = _frame_interval(opened)
                frame = opened.loc[closes <= forward_end_exclusive].copy(deep=True)
                del opened
                if frame.empty or not bool(
                    (pd.DatetimeIndex(frame.index) < forward_end_exclusive).all()
                ):
                    _fail(
                        "FORWARD_NATIVE_FRAME_EMPTY_OR_FUTURE",
                        anchor["anchor_id"],
                        key,
                    )
                active_frame_bytes = int(
                    frame.memory_usage(index=True, deep=True).sum()
                ) + int(tape.frame.memory_usage(index=True, deep=True).sum())
                if active_frame_bytes > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
                    _fail(
                        "FORWARD_ACTIVE_SYMBOL_FRAME_MEMORY_BOUND_EXCEEDED",
                        anchor["anchor_id"],
                        key,
                        active_frame_bytes,
                    )
                for specialist in grouped[key]:
                    _selection_prefix_match(frame, specialist)
                features: pd.DataFrame | None = None
                signals: dict[str, pd.Series] | None = None
                try:
                    features, signals = _features_and_prefix_parity(frame, grouped[key])
                    active_feature_bytes = int(
                        features.memory_usage(index=True, deep=True).sum()
                    ) + int(tape.frame.memory_usage(index=True, deep=True).sum())
                    if active_feature_bytes > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
                        _fail(
                            "FORWARD_ACTIVE_SYMBOL_FEATURE_MEMORY_BOUND_EXCEEDED",
                            anchor["anchor_id"],
                            key,
                            active_feature_bytes,
                        )
                    feature_opens, _ = _frame_interval(features)
                    warmup = int(feature_opens.searchsorted(forward_start, side="left"))
                    if not 1 <= warmup < len(features):
                        _fail(
                            "FORWARD_SCORING_WINDOW_HAS_NO_NATIVE_BARS",
                            anchor["anchor_id"],
                            key,
                        )
                    for specialist in grouped[key]:
                        materialized = _strategy(specialist)
                        base_backtester = Backtester(config)
                        try:
                            base = base_backtester.run(
                                features,
                                signals[specialist.cell_id],
                                specialist.symbol,
                                materialized,
                                spec=spec,
                                timeframe=specialist.timeframe,
                                warmup_bars=warmup,
                            )
                            stressed = base_backtester.replay_frozen_path_cost_stress(
                                features,
                                signals[specialist.cell_id],
                                specialist.symbol,
                                materialized,
                                base,
                                stress_mode=str(
                                    config.oos_evidence_execution_stress_mode
                                )
                                .strip()
                                .lower(),
                                spec=spec,
                                timeframe=specialist.timeframe,
                                warmup_bars=warmup,
                            )
                        except Exception as exc:
                            raise ForwardOOSReplayProductionError(
                                "FORWARD_BACKTEST_OR_STRESS_FAILED",
                                (specialist.cell_id, type(exc).__name__, str(exc)),
                            ) from exc
                        produced = _position_evidence(
                            specialist=specialist,
                            strategy=materialized,
                            features=features,
                            spec=spec,
                            config=config,
                            base_result=base,
                            stress_result=stressed,
                            valuation_tape=tape,
                            resource_budget=resource_budget,
                            capital_phase=capital_phase,
                        )
                        for position in produced:
                            entry = _timestamp(
                                position.candidate.entry_time,
                                "FORWARD_ENTRY_TIME_INVALID",
                            )
                            exit_time = _timestamp(
                                position.candidate.legs[-1].exit_time,
                                "FORWARD_EXIT_TIME_INVALID",
                            )
                            if (
                                entry < forward_start
                                or exit_time < entry
                                or exit_time >= forward_end_exclusive
                            ):
                                _fail(
                                    "FORWARD_POSITION_OUTSIDE_ANCHOR",
                                    position.position_id,
                                )
                        positions.extend(produced)
                finally:
                    FeatureComputer.clear_cache()
                    features = None
                    signals = None
                    del frame
        finally:
            FeatureComputer.clear_cache()
            del tape
    identifiers = [position.candidate.candidate_id for position in positions]
    if len(set(identifiers)) != len(identifiers):
        _fail("FORWARD_CANDIDATE_ID_REUSED", anchor["anchor_id"])
    accepted: dict[str, Decimal] = {}
    if positions:
        try:
            policy = portfolio.portfolio_policy_from_config_payload(
                deployment.config_body, float(config.initial_capital)
            )
            policy = replace(
                policy,
                allowed_promotion_statuses=("HISTORICAL_OOS_REPLAY",),
            )
            simulation = portfolio.SynchronizedPortfolioSimulator(policy).run(
                [position.candidate for position in positions]
            )
        except Exception as exc:
            raise ForwardOOSReplayProductionError(
                "SYNCHRONIZED_PORTFOLIO_REPLAY_FAILED",
                (anchor["anchor_id"], type(exc).__name__, str(exc)),
            ) from exc
        expected_roster_receipt = portfolio.portfolio_replay_candidate_roster_receipt(
            [position.candidate for position in positions]
        )
        input_receipt = simulation.get("input_receipt")
        if (
            type(input_receipt) is not dict
            or any(
                input_receipt.get(name) != value
                for name, value in expected_roster_receipt.items()
            )
            or simulation.get("policy") != asdict(policy)
        ):
            _fail("SYNCHRONIZED_PORTFOLIO_RECEIPT_MISMATCH", anchor["anchor_id"])
        closed = simulation.get("closed_positions")
        blocked = simulation.get("blocked_positions")
        if type(closed) is not list or type(blocked) is not list:
            _fail("SYNCHRONIZED_PORTFOLIO_OUTPUT_INVALID", anchor["anchor_id"])
        for row in closed:
            candidate_id = row.get("candidate_id") if type(row) is dict else None
            volume = row.get("volume") if type(row) is dict else None
            if candidate_id not in identifiers or candidate_id in accepted:
                _fail("SYNCHRONIZED_ACCEPTED_POSITION_INVALID", anchor["anchor_id"])
            accepted[candidate_id] = _decimal(
                volume, "SYNCHRONIZED_ACCEPTED_VOLUME_INVALID", positive=True
            )
        blocked_ids = {row.get("candidate_id") for row in blocked if type(row) is dict}
        if (
            set(identifiers) != set(accepted) | blocked_ids
            or set(accepted) & blocked_ids
        ):
            _fail("SYNCHRONIZED_PORTFOLIO_COVERAGE_INVALID", anchor["anchor_id"])
        candidate_roster_receipt: Mapping[str, Any] | None = expected_roster_receipt
        simulation_input_receipt: Mapping[str, Any] | None = input_receipt
    else:
        candidate_roster_receipt = None
        simulation_input_receipt = None
    return _AnchorRawReplay(
        anchor=dict(anchor),
        specialists=tuple(specialists),
        positions=tuple(positions),
        accepted_volume_by_position_id={
            position.position_id: accepted[position.candidate.candidate_id]
            for position in positions
            if position.candidate.candidate_id in accepted
        },
        production_metadata={
            "candidate_roster_receipt": candidate_roster_receipt,
            "simulation_input_receipt": simulation_input_receipt,
            "capital_phase_id": phase_id,
            "capital_phase": capital_phase,
            "capital_window": dict(capital_window),
            "complete_pre_policy_candidate_roster": True,
            "execution_spec_receipts": execution_spec_rows,
            "valuation_tapes": valuation_rows,
            "resource_usage": {
                "native_rows_bound_before_open": resource_budget.native_rows,
                "native_bytes_bound_before_open": resource_budget.native_bytes,
                "candidate_positions_reserved_before_materialization": (
                    resource_budget.candidate_positions
                ),
                "candidate_marks_reserved_before_materialization": (
                    resource_budget.candidate_marks
                ),
                "candidate_mark_estimated_bytes": (
                    resource_budget.candidate_marks * _ESTIMATED_CANDIDATE_MARK_BYTES
                ),
                "candidate_marks_thinned_or_sampled": False,
            },
            "valuation_contract": VALUATION_CONTRACT,
            "valuation_event_order": ("close_existing_then_mark_existing_then_admit"),
            "margin_model": MARGIN_MODEL,
            "margin_formula": (
                "abs(authenticated_entry_fill)*tick_value/tick_size*volume"
            ),
            "stop_risk_formula": (
                "abs(authenticated_entry_fill-adverse_stop_fill_with_configured_"
                "slippage)*tick_value/tick_size+full_round_trip_commission"
            ),
            "position_source_schema": POSITION_SOURCE_SCHEMA,
            "backtester_position_event_schema": (
                BACKTEST_POSITION_EVENT_SCHEMA_VERSION
            ),
            "position_events_v2_raw_rebuilt_and_exact_compared": True,
            "partial_exit_policy": (
                "FAIL_TYPED_WHILE_RAW_CAUSAL_LEDGER_V1_HAS_NO_PARTIAL_QUANTITY"
            ),
        },
    )


def _event_decisions(
    *,
    event_id: str,
    position_id: str,
    cell_id: str,
    regime: str,
    decided_at_utc: str,
    action: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "position_id": position_id,
        "cell_id": cell_id,
        "controller": {
            "schema_version": causal_producer.CONTROLLER_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "regime": regime,
            "decided_at_utc": decided_at_utc,
        },
        "arbitration": {
            "schema_version": causal_producer.ARBITRATION_EVENT_SCHEMA,
            "event_id": event_id,
            "cell_id": cell_id,
            "decision": "ADMIT" if action == "OPEN" else "EXIT",
            "decided_at_utc": decided_at_utc,
        },
        "order": {
            "schema_version": causal_producer.ORDER_EVENT_SCHEMA,
            "event_id": event_id,
            "position_id": position_id,
            "action": action,
            "decided_at_utc": decided_at_utc,
        },
    }


def _causal_precommit_geometry(
    causal_precommit_payload: Mapping[str, Any],
    historical_plans: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], datetime, datetime]:
    if type(causal_precommit_payload) is not dict:
        _fail("CAUSAL_PRECOMMIT_NOT_OBJECT")
    body = json.loads(
        _canonical_bytes(causal_precommit_payload, 16 * 1024 * 1024).decode("ascii")
    )
    try:
        _, causal_plans, _, not_before, deadline = causal._validate_precommit_payload(
            body
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "CAUSAL_PRECOMMIT_RAW_REVERIFY_FAILED", (type(exc).__name__,)
        ) from exc
    if len(causal_plans) != len(historical_plans):
        _fail("CAUSAL_HISTORICAL_ANCHOR_COUNT_MISMATCH")
    for causal_plan, historical_plan in zip(
        causal_plans, historical_plans, strict=True
    ):
        if any(
            causal_plan.get(key) != historical_plan.get(key)
            for key in (
                "anchor_id",
                "selection_cutoff_utc",
                "forward_start_date",
                "forward_end_date",
                "purge_calendar_days",
            )
        ):
            _fail("CAUSAL_HISTORICAL_ANCHOR_GEOMETRY_MISMATCH")
    return body, not_before, deadline


def _causal_access_window(
    causal_precommit_payload: Mapping[str, Any],
    historical_plans: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], str, datetime]:
    body, not_before, deadline = _causal_precommit_geometry(
        causal_precommit_payload, historical_plans
    )
    now = datetime.now(UTC).replace(microsecond=0)
    if now < not_before:
        _fail("CAUSAL_EVIDENCE_ACCESS_NOT_YET_AUTHORIZED", not_before.isoformat())
    if now > deadline:
        _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED", deadline.isoformat())
    return body, now.isoformat(timespec="seconds").replace("+00:00", "Z"), deadline


def _write_raw_ledger(
    context: authority_spool.GenerationSpoolContext,
    body: Mapping[str, Any],
    *,
    role: str,
) -> dict[str, Any]:
    raw = _canonical_bytes(body, _MAX_LEDGER_BYTES_PER_ANCHOR)
    if len(raw) > 48 * 1024 * 1024:
        _fail("RAW_LEDGER_ARTIFACT_BOUND_EXCEEDED", role, len(raw))
    try:
        descriptor = authority_spool.write_canonical_json_artifact(context, body)
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "RAW_LEDGER_SPOOL_FAILED", (role, type(exc).__name__)
        ) from exc
    normalized = _descriptor(descriptor, "RAW_LEDGER_DESCRIPTOR_INVALID")
    if normalized["sha256"] != hashlib.sha256(raw).hexdigest():
        _fail("RAW_LEDGER_SPOOL_IDENTITY_MISMATCH", role)
    return normalized


def _write_capital_json_artifact(
    context: authority_spool.GenerationSpoolContext,
    body: Mapping[str, Any],
    *,
    role: str,
    maximum: int,
) -> dict[str, Any]:
    raw = _canonical_bytes(body, maximum)
    try:
        descriptor = authority_spool.write_canonical_json_artifact(context, body)
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "CAPITAL_EVIDENCE_SPOOL_FAILED", (role, type(exc).__name__)
        ) from exc
    normalized = _descriptor(descriptor, "CAPITAL_EVIDENCE_DESCRIPTOR_INVALID")
    if (
        normalized["format"] != authority_spool.CANONICAL_JSON_FORMAT
        or normalized["size_bytes"] != len(raw)
        or normalized["sha256"] != hashlib.sha256(raw).hexdigest()
        or normalized["size_bytes"] > maximum
    ):
        _fail("CAPITAL_EVIDENCE_SPOOL_IDENTITY_MISMATCH", role)
    return normalized


def _capital_specialist_roster(
    specialists: Sequence[_VerifiedSpecialist],
) -> list[dict[str, Any]]:
    rows = [
        {
            "anchor_id": value.anchor_id,
            "cell_id": value.cell_id,
            "symbol": value.symbol,
            "timeframe": value.timeframe,
            "regime": value.regime,
            "strategy_id": value.strategy_id,
            "strategy_class": value.strategy_class,
            "strategy_module": value.strategy_module,
            "parameters": dict(value.parameters),
            "process_candidate_id": value.process_candidate_id,
            "process_sha256": value.process_sha256,
            "source_count_receipt_sha256": _sha(value.source_count_receipt),
            "detector_parameters_sha256": value.detector_parameters_sha256,
            "detector_record_sha256": _sha(value.detector_record),
        }
        for value in specialists
    ]
    rows.sort(key=lambda value: value["cell_id"])
    if not rows or len({value["cell_id"] for value in rows}) != len(rows):
        _fail("CAPITAL_SPECIALIST_ROSTER_INVALID")
    return rows


def _capital_operational_lineage(
    *,
    run_anchor: Mapping[str, Any],
    specialists: Sequence[_VerifiedSpecialist],
    precommit_anchor: Mapping[str, Any],
    replay_precommit_body: Mapping[str, Any],
) -> dict[str, Any]:
    if type(run_anchor) is not dict or type(precommit_anchor) is not dict:
        _fail("CAPITAL_OPERATIONAL_LINEAGE_INPUT_INVALID")
    source_rows: dict[str, dict[str, Any]] = {}
    identities = [
        *precommit_anchor.get("execution_products", ()),
        *precommit_anchor.get("valuation_m5_products", ()),
    ]
    if not identities or any(type(value) is not dict for value in identities):
        _fail("CAPITAL_NATIVE_LINEAGE_ROSTER_INVALID")
    for value in identities:
        key = native_product_key(value.get("symbol"), value.get("timeframe"))
        row = {
            "source_key": key,
            "activation_id": value.get("activation_id"),
            "activation_manifest_sha256": value.get("activation_manifest_sha256"),
            "source_bars_sha256": value.get("source_bars_sha256"),
            "source_audit_sidecar_sha256": value.get(
                "source_audit_sidecar_sha256"
            ),
            "index_sha256": value.get("index_sha256"),
        }
        existing = source_rows.get(key)
        if existing is not None and existing != row:
            _fail("CAPITAL_NATIVE_LINEAGE_SOURCE_SPLICE", key)
        source_rows[key] = row
    ordered_sources = [source_rows[key] for key in sorted(source_rows)]
    activation_ids = {value["activation_id"] for value in ordered_sources}
    activation_manifests = {
        value["activation_manifest_sha256"] for value in ordered_sources
    }
    if len(activation_ids) != 1 or len(activation_manifests) != 1:
        _fail("CAPITAL_NATIVE_ACTIVATION_ROSTER_MIXED")
    activation_id = next(iter(activation_ids))
    activation_manifest = next(iter(activation_manifests))
    if type(activation_id) is not str or not activation_id:
        _fail("CAPITAL_NATIVE_ACTIVATION_ID_INVALID")
    _hex(activation_manifest, "CAPITAL_NATIVE_ACTIVATION_MANIFEST_INVALID")
    bars = {value["source_key"]: value["source_bars_sha256"] for value in ordered_sources}
    sidecars = {
        value["source_key"]: value["source_audit_sidecar_sha256"]
        for value in ordered_sources
    }
    for value in (*bars.values(), *sidecars.values()):
        _hex(value, "CAPITAL_NATIVE_SOURCE_HASH_INVALID")
    specialist_roster = _capital_specialist_roster(specialists)
    result = {
        "book_decision_sha256": run_anchor.get("book_decision_sha256"),
        "book_generation_receipt_sha256": run_anchor.get(
            "book_generation_sha256"
        ),
        "refit_receipt_sha256": run_anchor.get("producer_request_sha256"),
        "refit_execution_sha256": run_anchor.get("symbol_output_roster_sha256"),
        "output_book_sha256": run_anchor.get("book_decision_sha256"),
        "winner_roster_sha256": _sha(specialist_roster),
        "native_activation_id": activation_id,
        "native_activation_manifest_sha256": activation_manifest,
        "native_activation_roster_sha256": _sha(ordered_sources),
        "native_bars_sha256_by_source": bars,
        "native_sidecar_sha256_by_source": sidecars,
        "margin_terms_sha256": replay_precommit_body.get(
            "capital_margin_terms_sha256"
        ),
        "broker_terms_sha256": replay_precommit_body.get(
            "capital_broker_terms_sha256"
        ),
    }
    if set(result) != _CAPITAL_OPERATIONAL_LINEAGE_FIELDS:
        _fail("CAPITAL_OPERATIONAL_LINEAGE_FIELDS_INVALID")
    for name in (
        "book_decision_sha256",
        "book_generation_receipt_sha256",
        "refit_receipt_sha256",
        "refit_execution_sha256",
        "output_book_sha256",
        "winner_roster_sha256",
        "native_activation_manifest_sha256",
        "native_activation_roster_sha256",
        "margin_terms_sha256",
        "broker_terms_sha256",
    ):
        _hex(result[name], f"CAPITAL_OPERATIONAL_{name.upper()}_INVALID")
    return json.loads(_canonical_bytes(result, 8 * 1024 * 1024).decode("ascii"))


def _capital_candidate_record(
    candidate: portfolio.CandidateTrade,
    *,
    phase_id: str,
    window: Mapping[str, Any],
    selected_cell_ids: frozenset[str],
) -> tuple[dict[str, Any], str]:
    if type(candidate) is not portfolio.CandidateTrade:
        _fail("CAPITAL_CANDIDATE_TYPE_INVALID", phase_id)
    try:
        normalized = candidate.normalized()
        record = portfolio.current_refit_candidate_record(normalized)
        rebuilt = portfolio.candidate_trade_from_record(record)
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "CAPITAL_CANDIDATE_CANONICALIZATION_FAILED",
            (phase_id, type(exc).__name__),
        ) from exc
    start, end = _capital_window_datetimes(window, phase_id=phase_id)
    entry = _timestamp(normalized.entry_time, "CAPITAL_CANDIDATE_ENTRY_INVALID")
    exit_time = _timestamp(
        normalized.legs[-1].exit_time, "CAPITAL_CANDIDATE_EXIT_INVALID"
    )
    if (
        rebuilt != normalized
        or normalized.cell_id not in selected_cell_ids
        or normalized.promotion_status != "HISTORICAL_OOS_REPLAY"
        or normalized.source_schema != POSITION_SOURCE_SCHEMA
        or normalized.margin_model != MARGIN_MODEL
        or entry < start
        or exit_time < entry
        or exit_time >= end
        or any(
            _timestamp(mark.time, "CAPITAL_CANDIDATE_MARK_INVALID") < entry
            or _timestamp(mark.time, "CAPITAL_CANDIDATE_MARK_INVALID") > exit_time
            for mark in normalized.marks
        )
    ):
        _fail("CAPITAL_CANDIDATE_SEMANTICS_INVALID", phase_id)
    raw = _canonical_bytes(record, _MAX_CAPITAL_CANDIDATE_RECORD_BYTES)
    return record, hashlib.sha256(raw).hexdigest()


def _persist_capital_phase_evidence(
    *,
    context: authority_spool.GenerationSpoolContext,
    replay: _AnchorRawReplay,
    run_anchor: Mapping[str, Any],
    replay_precommit: ForwardOOSReplayPrecommit,
    replay_precommit_body: Mapping[str, Any],
    precommit_anchor: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    phase_id = str(replay.production_metadata.get("capital_phase_id"))
    phase = str(replay.production_metadata.get("capital_phase"))
    window = replay.production_metadata.get("capital_window")
    expected_phase_id = _capital_phase_id(str(replay.anchor["anchor_id"]), phase)
    if phase_id != expected_phase_id or type(window) is not dict:
        _fail("CAPITAL_PHASE_REPLAY_METADATA_INVALID", phase_id)
    ordered_candidates = sorted(
        (position.candidate for position in replay.positions),
        key=lambda value: value.candidate_id,
    )
    if not ordered_candidates:
        _fail("CAPITAL_PHASE_CANDIDATE_ROSTER_EMPTY", phase_id)
    if len(ordered_candidates) > portfolio.MAX_PORTFOLIO_REPLAY_CANDIDATES:
        _fail("CAPITAL_PHASE_CANDIDATE_COUNT_BOUND_EXCEEDED", phase_id)
    identifiers = [value.candidate_id for value in ordered_candidates]
    if len(set(identifiers)) != len(identifiers):
        _fail("CAPITAL_PHASE_CANDIDATE_ID_REUSED", phase_id)
    specialist_roster = _capital_specialist_roster(replay.specialists)
    selected_ids = frozenset(value["cell_id"] for value in specialist_roster)
    records: list[tuple[dict[str, Any], str]] = [
        _capital_candidate_record(
            candidate,
            phase_id=phase_id,
            window=window,
            selected_cell_ids=selected_ids,
        )
        for candidate in ordered_candidates
    ]
    descriptors: list[dict[str, Any]] = []
    all_record_hashes: list[str] = []
    pending_records: list[dict[str, Any]] = []
    pending_hashes: list[str] = []
    pending_bytes = 0

    def flush() -> None:
        nonlocal pending_records, pending_hashes, pending_bytes
        if not pending_records:
            return
        chunk_index = len(descriptors)
        if chunk_index >= _MAX_CAPITAL_PHASE_CHUNKS:
            _fail("CAPITAL_PHASE_CHUNK_COUNT_BOUND_EXCEEDED", phase_id)
        chunk = {
            "schema_version": CAPITAL_CANDIDATE_CHUNK_SCHEMA,
            "phase_id": phase_id,
            "chunk_index": chunk_index,
            "candidate_count": len(pending_records),
            "candidate_records": pending_records,
            "candidate_record_sha256s": pending_hashes,
            "candidate_record_roster_sha256": _sha(
                [
                    {
                        "candidate_id": record["candidate_id"],
                        "candidate_record_sha256": record_sha,
                    }
                    for record, record_sha in zip(
                        pending_records, pending_hashes, strict=True
                    )
                ]
            ),
            "complete_records_not_policy_filtered": True,
        }
        descriptors.append(
            _write_capital_json_artifact(
                context,
                chunk,
                role=f"{phase_id}:CANDIDATE_CHUNK:{chunk_index}",
                maximum=_MAX_CAPITAL_CANDIDATE_CHUNK_BYTES,
            )
        )
        pending_records = []
        pending_hashes = []
        pending_bytes = 0

    for record, record_sha in records:
        record_bytes = len(
            _canonical_bytes(record, _MAX_CAPITAL_CANDIDATE_RECORD_BYTES)
        )
        if pending_records and (
            pending_bytes + record_bytes + 64 * 1024
            > _MAX_CAPITAL_CANDIDATE_CHUNK_BYTES
        ):
            flush()
        pending_records.append(record)
        pending_hashes.append(record_sha)
        pending_bytes += record_bytes
        all_record_hashes.append(record_sha)
    flush()
    total_bytes = sum(value["size_bytes"] for value in descriptors)
    if total_bytes > _MAX_CAPITAL_PHASE_RECORD_BYTES:
        _fail("CAPITAL_PHASE_RECORD_BYTE_BOUND_EXCEEDED", phase_id, total_bytes)
    lineage = _capital_operational_lineage(
        run_anchor=run_anchor,
        specialists=replay.specialists,
        precommit_anchor=precommit_anchor,
        replay_precommit_body=replay_precommit_body,
    )
    roster_receipt = portfolio.portfolio_replay_candidate_roster_receipt(
        ordered_candidates
    )
    manifest = {
        "schema_version": CAPITAL_PHASE_MANIFEST_SCHEMA,
        "production_contract_version": PRODUCTION_CONTRACT_VERSION,
        "phase_id": phase_id,
        "anchor_id": replay.anchor["anchor_id"],
        "phase": phase,
        "window": dict(window),
        "historical_run_receipt_sha256": replay_precommit_body[
            "historical_run_receipt_sha256"
        ],
        "forward_replay_precommit_receipt_sha256": replay_precommit.receipt_sha256,
        "capital_bootstrap_identity_sha256": replay_precommit_body[
            "capital_bootstrap_identity_sha256"
        ],
        "selected_specialist_roster_sha256": _sha(specialist_roster),
        "selected_cell_ids": sorted(selected_ids),
        "candidate_chunk_descriptors": descriptors,
        "candidate_chunk_descriptor_roster_sha256": _sha(descriptors),
        "candidate_count": len(ordered_candidates),
        "candidate_record_sha256s_sha256": _sha(all_record_hashes),
        "candidate_roster_receipt": roster_receipt,
        "operational_lineage": lineage,
        "complete_pre_policy_candidate_roster": True,
        "candidate_marks_thinned_or_sampled": False,
        "confers_capital_publication_or_trading_authority": False,
    }
    descriptor = _write_capital_json_artifact(
        context,
        manifest,
        role=f"{phase_id}:CAPITAL_PHASE_MANIFEST",
        maximum=_MAX_JSON_BYTES,
    )
    return descriptor, lineage


def _binding_windows(value: Mapping[str, Any]) -> dict[str, dict[str, dict[str, str]]]:
    anchors = value.get("anchor_plan") if type(value) is dict else None
    if type(anchors) is not list or not anchors:
        _fail("CAPITAL_PHASE_BINDING_ANCHORS_INVALID")
    result: dict[str, dict[str, dict[str, str]]] = {}
    for row in anchors:
        if (
            type(row) is not dict
            or set(row) != {"anchor_id", "inner_window", "outer_window"}
        ):
            _fail("CAPITAL_PHASE_BINDING_ANCHORS_INVALID")
        anchor_id = _safe_id(row["anchor_id"], "CAPITAL_PHASE_ANCHOR_ID_INVALID")
        if anchor_id in result:
            _fail("CAPITAL_PHASE_ANCHOR_ID_REUSED", anchor_id)
        for phase, name in (
            (CAPITAL_INNER_SELECTION, "inner_window"),
            (CAPITAL_OUTER_EVALUATION, "outer_window"),
        ):
            _capital_window_datetimes(
                row[name], phase_id=_capital_phase_id(anchor_id, phase)
            )
        result[anchor_id] = {
            CAPITAL_INNER_SELECTION: dict(row["inner_window"]),
            CAPITAL_OUTER_EVALUATION: dict(row["outer_window"]),
        }
    return json.loads(_canonical_bytes(result).decode("ascii"))


def _reopen_capital_phase_evidence(
    *,
    context: authority_spool.GenerationSpoolContext,
    receipt: Mapping[str, Any],
    precommit: Mapping[str, Any],
) -> tuple[
    dict[str, tuple[portfolio.CandidateTrade, ...]],
    dict[str, dict[str, Any]],
]:
    try:
        binding = capital_bootstrap.verify_capital_forward_phase_binding_body(
            context, precommit.get("capital_forward_phase_binding")
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "CAPITAL_PHASE_BINDING_REOPEN_FAILED", (type(exc).__name__,)
        ) from exc
    windows = _binding_windows(binding)
    anchor_ids = list(windows)
    expected_phase_ids = [
        _capital_phase_id(anchor_id, phase)
        for anchor_id in anchor_ids
        for phase in CAPITAL_PHASES
    ]
    manifest_descriptors = receipt.get("capital_phase_manifest_descriptors")
    if (
        precommit.get("capital_phase_windows_by_anchor") != windows
        or precommit.get("capital_phase_windows_sha256") != _sha(windows)
        or receipt.get("ordered_capital_phase_ids") != expected_phase_ids
        or type(manifest_descriptors) is not dict
        or list(manifest_descriptors) != expected_phase_ids
        or receipt.get("capital_candidate_records_complete_pre_policy") is not True
        or receipt.get("capital_phase_window_contract")
        != "SIGNED_BOOTSTRAP_INNER_AND_CAUSAL_OUTER_HALF_OPEN_V1"
    ):
        _fail("CAPITAL_PHASE_RECEIPT_ROSTER_INVALID")
    candidates_by_phase: dict[str, tuple[portfolio.CandidateTrade, ...]] = {}
    lineage_by_phase: dict[str, dict[str, Any]] = {}
    for phase_id in expected_phase_ids:
        anchor_id, phase = phase_id.rsplit("|", 1)
        manifest = _read_descriptor_json(
            context,
            manifest_descriptors[phase_id],
            code="CAPITAL_PHASE_MANIFEST",
            maximum=_MAX_JSON_BYTES,
        )
        if type(manifest) is not dict or set(manifest) != _CAPITAL_PHASE_MANIFEST_FIELDS:
            _fail("CAPITAL_PHASE_MANIFEST_FIELDS_INVALID", phase_id)
        expected_window = windows[anchor_id][phase]
        selected = manifest.get("selected_cell_ids")
        chunks = manifest.get("candidate_chunk_descriptors")
        lineage = manifest.get("operational_lineage")
        if (
            manifest.get("schema_version") != CAPITAL_PHASE_MANIFEST_SCHEMA
            or manifest.get("production_contract_version")
            != PRODUCTION_CONTRACT_VERSION
            or manifest.get("phase_id") != phase_id
            or manifest.get("anchor_id") != anchor_id
            or manifest.get("phase") != phase
            or manifest.get("window") != expected_window
            or manifest.get("historical_run_receipt_sha256")
            != receipt.get("historical_run_receipt_sha256")
            or manifest.get("forward_replay_precommit_receipt_sha256")
            != receipt.get("forward_replay_precommit_receipt_sha256")
            or manifest.get("capital_bootstrap_identity_sha256")
            != precommit.get("capital_bootstrap_identity_sha256")
            or type(selected) is not list
            or not selected
            or selected != sorted(set(selected))
            or any(type(value) is not str or not value for value in selected)
            or type(chunks) is not list
            or not 1 <= len(chunks) <= _MAX_CAPITAL_PHASE_CHUNKS
            or manifest.get("candidate_chunk_descriptor_roster_sha256")
            != _sha(chunks)
            or type(lineage) is not dict
            or set(lineage) != _CAPITAL_OPERATIONAL_LINEAGE_FIELDS
            or manifest.get("selected_specialist_roster_sha256")
            != lineage.get("winner_roster_sha256")
            or lineage.get("margin_terms_sha256")
            != precommit.get("capital_margin_terms_sha256")
            or lineage.get("broker_terms_sha256")
            != precommit.get("capital_broker_terms_sha256")
            or manifest.get("complete_pre_policy_candidate_roster") is not True
            or manifest.get("candidate_marks_thinned_or_sampled") is not False
            or manifest.get("confers_capital_publication_or_trading_authority")
            is not False
        ):
            _fail("CAPITAL_PHASE_MANIFEST_SEMANTICS_INVALID", phase_id)
        selected_set = frozenset(selected)
        total_bytes = 0
        observed_candidates: list[portfolio.CandidateTrade] = []
        observed_hashes: list[str] = []
        for expected_chunk_index, descriptor in enumerate(chunks):
            normalized_descriptor = _descriptor(
                descriptor, "CAPITAL_CANDIDATE_CHUNK_DESCRIPTOR_INVALID"
            )
            total_bytes += normalized_descriptor["size_bytes"]
            if total_bytes > _MAX_CAPITAL_PHASE_RECORD_BYTES:
                _fail("CAPITAL_PHASE_RECORD_BYTE_BOUND_EXCEEDED", phase_id)
            chunk = _read_descriptor_json(
                context,
                normalized_descriptor,
                code="CAPITAL_CANDIDATE_CHUNK",
                maximum=_MAX_CAPITAL_CANDIDATE_CHUNK_BYTES,
            )
            if type(chunk) is not dict or set(chunk) != _CAPITAL_CANDIDATE_CHUNK_FIELDS:
                _fail("CAPITAL_CANDIDATE_CHUNK_FIELDS_INVALID", phase_id)
            records = chunk.get("candidate_records")
            hashes = chunk.get("candidate_record_sha256s")
            if (
                chunk.get("schema_version") != CAPITAL_CANDIDATE_CHUNK_SCHEMA
                or chunk.get("phase_id") != phase_id
                or chunk.get("chunk_index") != expected_chunk_index
                or type(records) is not list
                or not records
                or type(hashes) is not list
                or len(records) != len(hashes)
                or chunk.get("candidate_count") != len(records)
                or chunk.get("complete_records_not_policy_filtered") is not True
                or chunk.get("candidate_record_roster_sha256")
                != _sha(
                    [
                        {
                            "candidate_id": record.get("candidate_id")
                            if type(record) is dict
                            else None,
                            "candidate_record_sha256": record_sha,
                        }
                        for record, record_sha in zip(records, hashes, strict=True)
                    ]
                )
            ):
                _fail("CAPITAL_CANDIDATE_CHUNK_SEMANTICS_INVALID", phase_id)
            for record, expected_sha in zip(records, hashes, strict=True):
                raw = _canonical_bytes(record, _MAX_CAPITAL_CANDIDATE_RECORD_BYTES)
                if hashlib.sha256(raw).hexdigest() != expected_sha:
                    _fail("CAPITAL_CANDIDATE_RECORD_IDENTITY_MISMATCH", phase_id)
                try:
                    candidate = portfolio.candidate_trade_from_record(record)
                except Exception as exc:
                    raise ForwardOOSReplayProductionError(
                        "CAPITAL_CANDIDATE_RECORD_RECONSTRUCTION_FAILED",
                        (phase_id, type(exc).__name__),
                    ) from exc
                rebuilt_record, rebuilt_sha = _capital_candidate_record(
                    candidate,
                    phase_id=phase_id,
                    window=expected_window,
                    selected_cell_ids=selected_set,
                )
                if rebuilt_record != record or rebuilt_sha != expected_sha:
                    _fail("CAPITAL_CANDIDATE_RECORD_ROUND_TRIP_MISMATCH", phase_id)
                observed_candidates.append(candidate)
                observed_hashes.append(expected_sha)
        identifiers = [value.candidate_id for value in observed_candidates]
        if identifiers != sorted(identifiers) or len(set(identifiers)) != len(identifiers):
            _fail("CAPITAL_CANDIDATE_ROSTER_ORDER_OR_IDENTITY_INVALID", phase_id)
        try:
            roster_receipt = portfolio.portfolio_replay_candidate_roster_receipt(
                observed_candidates
            )
        except Exception as exc:
            raise ForwardOOSReplayProductionError(
                "CAPITAL_CANDIDATE_ROSTER_RECEIPT_FAILED",
                (phase_id, type(exc).__name__),
            ) from exc
        if (
            manifest.get("candidate_count") != len(observed_candidates)
            or manifest.get("candidate_record_sha256s_sha256")
            != _sha(observed_hashes)
            or manifest.get("candidate_roster_receipt") != roster_receipt
        ):
            _fail("CAPITAL_CANDIDATE_ROSTER_RECEIPT_MISMATCH", phase_id)
        candidates_by_phase[phase_id] = tuple(observed_candidates)
        lineage_by_phase[phase_id] = json.loads(
            _canonical_bytes(lineage, 8 * 1024 * 1024).decode("ascii")
        )
    return candidates_by_phase, lineage_by_phase


def _required_forward_product_rosters(
    plans: Sequence[Mapping[str, Any]],
    books: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, list[str]]]:
    result: dict[str, dict[str, list[str]]] = {}
    for plan in plans:
        anchor_id = plan["anchor_id"]
        cells = books[anchor_id].get("cells")
        if type(cells) is not list or len(cells) != book.FAMILY_SIZE:
            _fail("FORWARD_BOOK_CELL_ROSTER_INVALID", anchor_id)
        winners = [
            value
            for value in cells
            if type(value) is dict and value.get("historical_state") == "WINNER"
        ]
        execution = sorted(
            {
                native_product_key(value.get("symbol"), value.get("timeframe"))
                for value in winners
            }
        )
        valuation = sorted(
            {native_product_key(value.get("symbol"), "M5") for value in winners}
        )
        result[anchor_id] = {
            "execution_product_keys": execution,
            "valuation_m5_product_keys": valuation,
        }
    return result


def _product_roster_identities(
    *,
    plans: Sequence[Mapping[str, Any]],
    capital_phase_windows_by_anchor: Mapping[
        str, Mapping[str, Mapping[str, str]]
    ],
    required: Mapping[str, Mapping[str, Sequence[str]]],
    execution_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
    valuation_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
) -> list[dict[str, Any]]:
    anchor_ids = [plan["anchor_id"] for plan in plans]
    if (
        type(capital_phase_windows_by_anchor) is not dict
        or list(capital_phase_windows_by_anchor) != anchor_ids
        or
        type(execution_products_by_anchor) is not dict
        or type(valuation_products_by_anchor) is not dict
        or list(execution_products_by_anchor) != anchor_ids
        or list(valuation_products_by_anchor) != anchor_ids
    ):
        _fail("FORWARD_PRECOMMIT_ANCHOR_ROSTER_MISMATCH")
    rows: list[dict[str, Any]] = []
    for plan in plans:
        anchor_id = plan["anchor_id"]
        cutoff = (
            _date(plan["forward_end_date"], "FORWARD_END_INVALID") + timedelta(days=1)
        ).isoformat() + "T00:00:00Z"
        execution = execution_products_by_anchor[anchor_id]
        valuation = valuation_products_by_anchor[anchor_id]
        expected_execution = list(required[anchor_id]["execution_product_keys"])
        expected_valuation = list(required[anchor_id]["valuation_m5_product_keys"])
        if type(execution) is not dict or sorted(execution) != expected_execution:
            _fail("FORWARD_EXECUTION_PRODUCT_ROSTER_MISMATCH", anchor_id)
        if type(valuation) is not dict or sorted(valuation) != expected_valuation:
            _fail("FORWARD_M5_VALUATION_PRODUCT_ROSTER_MISMATCH", anchor_id)
        execution_rows: list[dict[str, Any]] = []
        valuation_rows: list[dict[str, Any]] = []
        anchor_rows = 0
        anchor_bytes = 0
        for key in expected_execution:
            symbol, timeframe = key.split("|", 1)
            identity = _native_product_identity(
                execution[key],
                expected_symbol=symbol,
                expected_timeframe=timeframe,
                expected_cutoff_utc=cutoff,
                role=f"EXECUTION:{anchor_id}:{key}",
            )
            execution_rows.append(identity)
            anchor_rows += identity["prefix_rows"]
            anchor_bytes += identity["prefix_bytes"]
        for key in expected_valuation:
            symbol, timeframe = key.split("|", 1)
            if timeframe != "M5":
                _fail("FORWARD_VALUATION_TIMEFRAME_NOT_M5", anchor_id, key)
            identity = _native_product_identity(
                valuation[key],
                expected_symbol=symbol,
                expected_timeframe="M5",
                expected_cutoff_utc=cutoff,
                role=f"VALUATION_M5:{anchor_id}:{key}",
            )
            valuation_rows.append(identity)
            anchor_rows += identity["prefix_rows"]
            anchor_bytes += identity["prefix_bytes"]
        if anchor_rows > _MAX_NATIVE_ROWS_PER_ANCHOR:
            _fail("FORWARD_PRECOMMIT_NATIVE_ROW_BOUND_EXCEEDED", anchor_id, anchor_rows)
        if anchor_bytes > _MAX_NATIVE_BYTES_PER_ANCHOR:
            _fail(
                "FORWARD_PRECOMMIT_NATIVE_BYTE_BOUND_EXCEEDED",
                anchor_id,
                anchor_bytes,
            )
        rows.append(
            {
                "anchor_id": anchor_id,
                "forward_end_exclusive_utc": cutoff,
                "execution_products": execution_rows,
                "valuation_m5_products": valuation_rows,
                "execution_product_roster_sha256": _sha(execution_rows),
                "valuation_m5_product_roster_sha256": _sha(valuation_rows),
                "valuation_products_supplied_separately": True,
                "capital_phase_windows": json.loads(
                    _canonical_bytes(
                        capital_phase_windows_by_anchor[anchor_id]
                    ).decode("ascii")
                ),
            }
        )
    return rows


def precommit_forward_oos_replay_products(
    *,
    context: authority_spool.GenerationSpoolContext,
    historical_precommit: historical.HistoricalAnchorPrecommit,
    historical_run_receipt: historical.HistoricalAnchorRunReceipt,
    causal_precommit_payload: Mapping[str, Any],
    execution_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
    valuation_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
    capital_phase_binding: capital_bootstrap.CapitalForwardPhaseBinding,
    config: PipelineConfig,
    _issue_precommit: Any = _issue_precommit_tag,
) -> ForwardOOSReplayPrecommit:
    """Freeze exact execution and separate M5 valuation products before use.

    This function never opens a prefix frame.  It seals product/content/source
    identities, deployment economics, and code identities before the producer
    is permitted to observe any forward price row.
    """

    try:
        deployment = _deployment_config(config)
        run_body, plans, books, _ = _reopen_books(
            context, historical_precommit, historical_run_receipt
        )
        causal_body, _, deadline = _causal_precommit_geometry(
            causal_precommit_payload, plans
        )
        capital_binding_body = capital_bootstrap.verify_capital_forward_phase_binding(
            context, capital_phase_binding
        )
        capital_identity = capital_binding_body.get("bootstrap_identity")
        if (
            type(capital_identity) is not dict
            or capital_identity.get("process_precommitment_sha256")
            != causal.canonical_sha256(causal_body)
            or capital_identity.get("process_definition_sha256")
            != causal_body.get("process_definition_sha256")
        ):
            _fail("CAPITAL_FORWARD_PHASE_PROCESS_PRECOMMITMENT_MISMATCH")
        capital_windows = _capital_phase_windows(plans, capital_binding_body)
        now = datetime.now(UTC)
        if now > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED", deadline.isoformat())
        required = _required_forward_product_rosters(plans, books)
        product_rows = _product_roster_identities(
            plans=plans,
            capital_phase_windows_by_anchor=capital_windows,
            required=required,
            execution_products_by_anchor=execution_products_by_anchor,
            valuation_products_by_anchor=valuation_products_by_anchor,
        )
        if any(
            _utc(
                identity["source_accessed_at_utc"],
                "FORWARD_PRODUCT_ACCESS_TIME_INVALID",
            )
            > now
            for row in product_rows
            for identity in (
                *row["execution_products"],
                *row["valuation_m5_products"],
            )
        ):
            _fail("FORWARD_PRODUCT_SOURCE_ACCESSED_AFTER_REPLAY_PRECOMMIT")
        code_identity = _replay_code_identity(causal_body)
        body = {
            "schema_version": REPLAY_PRECOMMIT_SCHEMA,
            "generation_id": context.generation_id,
            "generation_context_sha256": context.context_sha256,
            "native_universe_sha256": context.native_universe_sha256,
            "config_sha256": context.config_sha256,
            "historical_precommit_receipt_sha256": (
                historical_precommit.receipt_sha256
            ),
            "historical_run_receipt_sha256": (historical_run_receipt.receipt_sha256),
            "historical_run_payload_sha256": _sha(run_body),
            "causal_precommit_payload_sha256": causal.canonical_sha256(causal_body),
            "created_at_utc": now.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "created_before_first_forward_oos_frame_access": True,
            "all_native_source_access_times_not_after_precommit": True,
            "required_rosters": required,
            "anchor_product_identities": product_rows,
            "anchor_product_identities_sha256": _sha(product_rows),
            "capital_phase_windows_by_anchor": capital_windows,
            "capital_phase_windows_sha256": _sha(capital_windows),
            "capital_forward_phase_binding": capital_binding_body,
            "capital_forward_phase_binding_sha256": _sha(capital_binding_body),
            "capital_bootstrap_identity_sha256": capital_binding_body[
                "bootstrap_identity_sha256"
            ],
            "capital_precommitment_body_sha256": capital_binding_body[
                "capital_precommitment_body_sha256"
            ],
            "capital_contract_lineage_sha256": _sha(
                capital_binding_body["contract_lineage"]
            ),
            "capital_margin_terms_sha256": capital_binding_body[
                "margin_terms_sha256"
            ],
            "capital_broker_terms_sha256": capital_binding_body[
                "broker_terms_sha256"
            ],
            "deployment_binding": deployment.receipt_body(),
            "code_identity": code_identity,
            "valuation_contract": VALUATION_CONTRACT,
            "product_cutoff_contract": "EXACT_FORWARD_END_EXCLUSIVE_NO_LATER_ROWS",
            "confers_causal_current_refit_capital_publication_or_trading_authority": False,
        }
        descriptor = _write_raw_ledger(
            context, body, role="FORWARD_OOS_REPLAY_PRECOMMIT"
        )
        raw = _canonical_bytes(body)
        if descriptor["sha256"] != hashlib.sha256(raw).hexdigest():
            _fail("FORWARD_REPLAY_PRECOMMIT_SPOOL_IDENTITY_MISMATCH")
        return ForwardOOSReplayPrecommit(
            raw,
            descriptor,
            _issue_precommit(raw, descriptor),
        )
    except ForwardOOSReplayProductionError:
        raise
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_REPLAY_PRECOMMIT_FAILED", (type(exc).__name__, str(exc))
        ) from exc


def _verify_forward_replay_precommit(
    *,
    context: authority_spool.GenerationSpoolContext,
    value: ForwardOOSReplayPrecommit,
    historical_precommit: historical.HistoricalAnchorPrecommit,
    historical_run_receipt: historical.HistoricalAnchorRunReceipt,
    causal_precommit_payload: Mapping[str, Any],
    deployment: _DeploymentBinding,
) -> dict[str, Any]:
    if (
        type(value) is not ForwardOOSReplayPrecommit
        or type(value._issuance_tag) is not str
        or _HEX64.fullmatch(value._issuance_tag) is None
        or not _verify_precommit_issuance(value)
    ):
        _fail("FORWARD_REPLAY_PRECOMMIT_NOT_ISSUED")
    descriptor = _descriptor(
        dict(value.descriptor), "FORWARD_REPLAY_PRECOMMIT_DESCRIPTOR_INVALID"
    )
    raw = bytes(value._body_json)
    if (
        descriptor["sha256"] != hashlib.sha256(raw).hexdigest()
        or descriptor["size_bytes"] != len(raw)
        or descriptor["sha256"] != value.receipt_sha256
    ):
        _fail("FORWARD_REPLAY_PRECOMMIT_IDENTITY_INVALID")
    body = _read_descriptor_json(
        context,
        descriptor,
        code="FORWARD_REPLAY_PRECOMMIT",
        maximum=_MAX_JSON_BYTES,
    )
    if _canonical_bytes(body) != raw:
        _fail("FORWARD_REPLAY_PRECOMMIT_BYTE_REPLAY_MISMATCH")
    expected_code = _replay_code_identity(causal_precommit_payload)
    try:
        capital_binding = (
            capital_bootstrap.verify_capital_forward_phase_binding_body(
                context, body.get("capital_forward_phase_binding")
            )
        )
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_CAPITAL_PHASE_BINDING_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    capital_windows = _capital_phase_windows(
        historical_precommit.to_dict()["anchors"], capital_binding
    )
    if (
        body.get("schema_version") != REPLAY_PRECOMMIT_SCHEMA
        or body.get("generation_id") != context.generation_id
        or body.get("generation_context_sha256") != context.context_sha256
        or body.get("native_universe_sha256") != context.native_universe_sha256
        or body.get("config_sha256") != context.config_sha256
        or body.get("historical_precommit_receipt_sha256")
        != historical_precommit.receipt_sha256
        or body.get("historical_run_receipt_sha256")
        != historical_run_receipt.receipt_sha256
        or body.get("causal_precommit_payload_sha256")
        != causal.canonical_sha256(dict(causal_precommit_payload))
        or body.get("deployment_binding") != deployment.receipt_body()
        or body.get("code_identity") != expected_code
        or body.get("capital_forward_phase_binding_sha256")
        != _sha(capital_binding)
        or body.get("capital_phase_windows_by_anchor") != capital_windows
        or body.get("capital_phase_windows_sha256") != _sha(capital_windows)
        or body.get("capital_bootstrap_identity_sha256")
        != capital_binding["bootstrap_identity_sha256"]
        or body.get("capital_precommitment_body_sha256")
        != capital_binding["capital_precommitment_body_sha256"]
        or body.get("capital_contract_lineage_sha256")
        != _sha(capital_binding["contract_lineage"])
        or body.get("capital_margin_terms_sha256")
        != capital_binding["margin_terms_sha256"]
        or body.get("capital_broker_terms_sha256")
        != capital_binding["broker_terms_sha256"]
        or body.get("created_before_first_forward_oos_frame_access") is not True
        or body.get("all_native_source_access_times_not_after_precommit") is not True
        or body.get(
            "confers_causal_current_refit_capital_publication_or_trading_authority"
        )
        is not False
    ):
        _fail("FORWARD_REPLAY_PRECOMMIT_BINDING_MISMATCH")
    created = _utc(body.get("created_at_utc"), "FORWARD_PRECOMMIT_TIME_INVALID")
    if datetime.now(UTC) < created:
        _fail("FORWARD_REPLAY_PRECOMMIT_FROM_FUTURE")
    return body


def produce_forward_oos_replay_artifacts(
    *,
    context: authority_spool.GenerationSpoolContext,
    historical_precommit: historical.HistoricalAnchorPrecommit,
    historical_run_receipt: historical.HistoricalAnchorRunReceipt,
    causal_precommit_payload: Mapping[str, Any],
    replay_precommit: ForwardOOSReplayPrecommit,
    native_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
    valuation_products_by_anchor: Mapping[
        str, Mapping[str, native_prefix.NativePrefixProduct]
    ],
    config: PipelineConfig,
    _issue_product: Any = _issue_product_tag,
) -> ForwardOOSReplayProduct:
    """Produce genuine raw repeated-anchor forward evidence.

    The caller supplies only products sealed in ``replay_precommit``.  The M5
    valuation roster is explicit and separate from specialist execution inputs.
    Strategies, detector state, costs, fills, portfolio policy, and clocks are
    derived from independently reopened bytes.  The returned seventh receipt
    binds the six ledgers but grants no causal, publication, or trading authority.
    """

    try:
        deployment = _deployment_config(config)
        run_body, plans, books, symbol_receipts = _reopen_books(
            context, historical_precommit, historical_run_receipt
        )
        causal_body, access_started, deadline = _causal_access_window(
            causal_precommit_payload, plans
        )
        replay_precommit_body = _verify_forward_replay_precommit(
            context=context,
            value=replay_precommit,
            historical_precommit=historical_precommit,
            historical_run_receipt=historical_run_receipt,
            causal_precommit_payload=causal_body,
            deployment=deployment,
        )
        required_rosters = _required_forward_product_rosters(plans, books)
        if (
            replay_precommit_body.get("required_rosters") != required_rosters
            or replay_precommit_body.get("historical_run_payload_sha256")
            != _sha(run_body)
            or replay_precommit_body.get("anchor_product_identities_sha256")
            != _sha(replay_precommit_body.get("anchor_product_identities"))
        ):
            _fail("FORWARD_REPLAY_PRECOMMIT_HISTORICAL_ROSTER_DRIFT")
        anchor_ids = [plan["anchor_id"] for plan in plans]
        if (
            type(native_products_by_anchor) is not dict
            or type(valuation_products_by_anchor) is not dict
            or list(native_products_by_anchor) != anchor_ids
            or list(valuation_products_by_anchor) != anchor_ids
        ):
            _fail("FORWARD_NATIVE_ANCHOR_ROSTER_MISMATCH")
        precommit_anchors = {
            value.get("anchor_id"): value
            for value in replay_precommit_body["anchor_product_identities"]
            if type(value) is dict
        }
        if set(precommit_anchors) != set(anchor_ids):
            _fail("FORWARD_PRECOMMIT_ANCHOR_ROSTER_INVALID")
        specialists = _reopen_specialists(context, plans, books, symbol_receipts)
        execution_started_dt = datetime.now(UTC).replace(microsecond=0)
        if execution_started_dt > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED_DURING_REVERIFY")
        execution_started = execution_started_dt.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )

        pending_events: dict[str, dict[str, Any]] = {}
        descriptor_rows: dict[str, dict[str, dict[str, Any]]] = {}
        anchor_metadata: dict[str, dict[str, Any]] = {}
        capital_phase_manifest_descriptors: dict[str, dict[str, Any]] = {}
        run_anchors = {
            value.get("anchor_id"): value
            for value in run_body.get("anchors", ())
            if type(value) is dict
        }
        if set(run_anchors) != set(anchor_ids):
            _fail("FORWARD_HISTORICAL_RUN_ANCHOR_ROSTER_INVALID")
        capital_windows = replay_precommit_body.get(
            "capital_phase_windows_by_anchor"
        )
        if type(capital_windows) is not dict or list(capital_windows) != anchor_ids:
            _fail("FORWARD_CAPITAL_PHASE_WINDOW_ROSTER_INVALID")
        total_bytes = 0
        for plan in plans:
            anchor_id = plan["anchor_id"]
            for phase in CAPITAL_PHASES:
                replay = _execute_anchor_replay(
                    context=context,
                    anchor=plan,
                    specialists=specialists[anchor_id],
                    native_products=native_products_by_anchor[anchor_id],
                    valuation_products=valuation_products_by_anchor[anchor_id],
                    precommit_anchor=precommit_anchors[anchor_id],
                    capital_phase=phase,
                    capital_window=capital_windows[anchor_id][phase],
                    config=config,
                    deployment=deployment,
                )
                phase_id = _capital_phase_id(anchor_id, phase)
                manifest_descriptor, _ = _persist_capital_phase_evidence(
                    context=context,
                    replay=replay,
                    run_anchor=run_anchors[anchor_id],
                    replay_precommit=replay_precommit,
                    replay_precommit_body=replay_precommit_body,
                    precommit_anchor=precommit_anchors[anchor_id],
                )
                capital_phase_manifest_descriptors[phase_id] = manifest_descriptor
                if phase == CAPITAL_OUTER_EVALUATION:
                    anchor_metadata[anchor_id] = json.loads(
                        _canonical_bytes(
                            dict(replay.production_metadata or {})
                        ).decode("ascii")
                    )
                    bodies = _ledger_bodies(
                        replay,
                        starting_balance=_decimal(
                            config.initial_capital,
                            "REPLAY_STARTING_BALANCE_INVALID",
                            positive=True,
                        ),
                    )
                    anchor_size = sum(
                        len(_canonical_bytes(value)) for value in bodies.values()
                    )
                    if anchor_size > _MAX_LEDGER_BYTES_PER_ANCHOR:
                        _fail(
                            "RAW_LEDGER_ANCHOR_BOUND_EXCEEDED",
                            anchor_id,
                            anchor_size,
                        )
                    total_bytes += anchor_size
                    if total_bytes > _MAX_LEDGER_BYTES_TOTAL:
                        _fail("RAW_LEDGER_TOTAL_BOUND_EXCEEDED", total_bytes)
                    pending_events[anchor_id] = bodies.pop("EVENT_LEDGER")
                    descriptor_rows[anchor_id] = {}
                    for role in (
                        "FILL_LEDGER",
                        "MTM_LEDGER",
                        "COST_LEDGER",
                        "EXPOSURE_LEDGER",
                        "MARGIN_LEDGER",
                    ):
                        descriptor_rows[anchor_id][role] = _write_raw_ledger(
                            context, bodies[role], role=f"{anchor_id}:{role}"
                        )
                    del bodies
                del replay
                FeatureComputer.clear_cache()

        execution_completed_dt = datetime.now(UTC).replace(microsecond=0)
        reconciled_dt = datetime.now(UTC).replace(microsecond=0)
        if reconciled_dt > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED_DURING_REPLAY")
        execution_completed = execution_completed_dt.isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        reconciled = reconciled_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
        for plan in plans:
            anchor_id = plan["anchor_id"]
            event = pending_events.pop(anchor_id)
            event.update(
                {
                    "evidence_access_started_at_utc": access_started,
                    "execution_started_at_utc": execution_started,
                    "execution_completed_at_utc": execution_completed,
                    "reconciled_at_utc": reconciled,
                }
            )
            descriptor_rows[anchor_id]["EVENT_LEDGER"] = _write_raw_ledger(
                context, event, role=f"{anchor_id}:EVENT_LEDGER"
            )

        result: dict[str, causal_producer.AnchorReplayArtifactDescriptors] = {}
        for plan in plans:
            anchor_id = plan["anchor_id"]
            row = descriptor_rows[anchor_id]
            descriptors = causal_producer.AnchorReplayArtifactDescriptors(
                event_ledger=row["EVENT_LEDGER"],
                fill_ledger=row["FILL_LEDGER"],
                mtm_ledger=row["MTM_LEDGER"],
                cost_ledger=row["COST_LEDGER"],
                exposure_ledger=row["EXPOSURE_LEDGER"],
                margin_ledger=row["MARGIN_LEDGER"],
            )
            # Reopen every byte immediately, before allowing it to leave the
            # producer boundary.
            causal_producer._load_replay_ledgers(context, descriptors, plan)
            result[anchor_id] = descriptors

        _assert_deployment_config_unchanged(deployment, config)
        code_identity = _replay_code_identity(causal_body)
        if code_identity != replay_precommit_body.get("code_identity"):
            _fail("FORWARD_REPLAY_CODE_CHANGED_AFTER_PRECOMMIT")
        producer_receipt_body = {
            "schema_version": PRODUCER_RECEIPT_SCHEMA,
            "production_contract_version": PRODUCTION_CONTRACT_VERSION,
            "generation_id": context.generation_id,
            "generation_context_sha256": context.context_sha256,
            "native_universe_sha256": context.native_universe_sha256,
            "config_sha256": context.config_sha256,
            "historical_precommit_receipt_sha256": (
                historical_precommit.receipt_sha256
            ),
            "historical_run_receipt_sha256": (historical_run_receipt.receipt_sha256),
            "causal_precommit_payload_sha256": causal.canonical_sha256(causal_body),
            "forward_replay_precommit_receipt_sha256": (
                replay_precommit.receipt_sha256
            ),
            "forward_replay_precommit_descriptor": _descriptor(
                dict(replay_precommit.descriptor),
                "FORWARD_REPLAY_PRECOMMIT_DESCRIPTOR_INVALID",
            ),
            "capital_bootstrap_identity_sha256": replay_precommit_body[
                "capital_bootstrap_identity_sha256"
            ],
            "capital_precommitment_body_sha256": replay_precommit_body[
                "capital_precommitment_body_sha256"
            ],
            "capital_contract_lineage_sha256": replay_precommit_body[
                "capital_contract_lineage_sha256"
            ],
            "capital_margin_terms_sha256": replay_precommit_body[
                "capital_margin_terms_sha256"
            ],
            "capital_broker_terms_sha256": replay_precommit_body[
                "capital_broker_terms_sha256"
            ],
            "evidence_access_started_at_utc": access_started,
            "execution_started_at_utc": execution_started,
            "execution_completed_at_utc": execution_completed,
            "reconciled_at_utc": reconciled,
            "deployment_binding": deployment.receipt_body(),
            "code_identity": code_identity,
            "anchor_ledger_descriptors": {
                anchor_id: {
                    role: dict(value)
                    for role, value in sorted(descriptor_rows[anchor_id].items())
                }
                for anchor_id in anchor_ids
            },
            "anchor_production_metadata": {
                anchor_id: anchor_metadata[anchor_id] for anchor_id in anchor_ids
            },
            "ordered_anchor_ids": anchor_ids,
            "capital_phase_manifest_descriptors": (
                capital_phase_manifest_descriptors
            ),
            "ordered_capital_phase_ids": list(
                capital_phase_manifest_descriptors
            ),
            "capital_candidate_records_complete_pre_policy": True,
            "capital_phase_window_contract": (
                "SIGNED_BOOTSTRAP_INNER_AND_CAUSAL_OUTER_HALF_OPEN_V1"
            ),
            "valuation_contract": VALUATION_CONTRACT,
            "valuation_event_order": ("close_existing_then_mark_existing_then_admit"),
            "full_round_trip_commission_in_every_candidate_mark": True,
            "stop_risk_includes_adverse_configured_stop_slippage": True,
            "candidate_marks_thinned_or_sampled": False,
            # Causal evidence may be produced only after this receipt/product
            # exists and passes its standalone verifier.  The former v1
            # pre-receipt callback exposed bare descriptors and is forbidden.
            "causal_consumer_reopened_and_verified": False,
            "causal_binding_followup_required": True,
            "confers_causal_process_current_refit_capital_readiness_publication_"
            "or_trading_authority": False,
        }
        producer_descriptor = _write_raw_ledger(
            context, producer_receipt_body, role="FORWARD_OOS_PRODUCER_RECEIPT"
        )
        producer_raw = _canonical_bytes(producer_receipt_body)
        if producer_descriptor["sha256"] != hashlib.sha256(producer_raw).hexdigest():
            _fail("FORWARD_OOS_PRODUCER_RECEIPT_SPOOL_MISMATCH")
        if (
            _read_descriptor_json(
                context,
                producer_descriptor,
                code="FORWARD_OOS_PRODUCER_RECEIPT",
                maximum=_MAX_JSON_BYTES,
            )
            != producer_receipt_body
        ):
            _fail("FORWARD_OOS_PRODUCER_RECEIPT_REOPEN_MISMATCH")
        anchor_rows = tuple(
            (anchor_id, result[anchor_id]) for anchor_id in anchor_ids
        )
        product = ForwardOOSReplayProduct(
            anchor_rows,
            producer_raw,
            producer_descriptor,
            _issue_product(anchor_rows, producer_raw, producer_descriptor),
        )
        verify_forward_oos_replay_product(context, product)
        return product
    except ForwardOOSReplayProductionError:
        raise
    except Exception as exc:
        raise ForwardOOSReplayProductionError(
            "FORWARD_OOS_REPLAY_PRODUCTION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc


# The issuer remains captured only by the production factory's default; the
# module exposes the verifier but no free-standing issuance primitive.
del _issue_product_tag
del _issue_precommit_tag


__all__ = [
    "ForwardOOSReplayPrecommit",
    "ForwardOOSReplayProduct",
    "ForwardOOSReplayProductionError",
    "MARGIN_MODEL",
    "POSITION_SOURCE_SCHEMA",
    "PRODUCER_RECEIPT_SCHEMA",
    "PRODUCTION_CONTRACT_VERSION",
    "REPLAY_PRECOMMIT_SCHEMA",
    "VALUATION_CONTRACT",
    "native_product_key",
    "precommit_forward_oos_replay_products",
    "produce_forward_oos_replay_artifacts",
    "verify_forward_oos_replay_product",
]


def _raw_event_id(anchor_id: str, position_id: str, lane: str, day: str = "") -> str:
    raw = f"{anchor_id}|{position_id}|{lane}|{day}".encode("utf-8")
    return "evt:" + hashlib.sha256(raw).hexdigest()


def _broker_fill_id(anchor_id: str, position_id: str, lane: str) -> str:
    raw = f"{anchor_id}|{position_id}|{lane}".encode("utf-8")
    return "fill:" + hashlib.sha256(raw).hexdigest()


def _ledger_bodies(
    replay: _AnchorRawReplay,
    *,
    starting_balance: Decimal,
) -> dict[str, dict[str, Any]]:
    anchor = replay.anchor
    dates = [
        (
            _date(anchor["forward_start_date"], "FORWARD_START_INVALID")
            + timedelta(days=offset)
        ).isoformat()
        for offset in range(
            (
                _date(anchor["forward_end_date"], "FORWARD_END_INVALID")
                - _date(anchor["forward_start_date"], "FORWARD_START_INVALID")
            ).days
            + 1
        )
    ]
    accepted = [
        (position, replay.accepted_volume_by_position_id[position.position_id])
        for position in replay.positions
        if position.position_id in replay.accepted_volume_by_position_id
    ]
    if len(accepted) > _MAX_CANDIDATE_POSITIONS_PER_ANCHOR:
        _fail("ACCEPTED_POSITION_BOUND_EXCEEDED", anchor["anchor_id"])
    by_entry: dict[str, list[tuple[_PositionEvidence, Decimal]]] = {}
    by_exit: dict[str, list[tuple[_PositionEvidence, Decimal]]] = {}
    for position, volume in accepted:
        entry_day = (
            _timestamp(position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID")
            .date()
            .isoformat()
        )
        exit_day = (
            _timestamp(
                position.candidate.legs[-1].exit_time, "POSITION_EXIT_TIME_INVALID"
            )
            .date()
            .isoformat()
        )
        by_entry.setdefault(entry_day, []).append((position, volume))
        by_exit.setdefault(exit_day, []).append((position, volume))

    event_days: list[dict[str, Any]] = []
    fill_days: list[dict[str, Any]] = []
    mtm_days: list[dict[str, Any]] = []
    cost_days: list[dict[str, Any]] = []
    exposure_days: list[dict[str, Any]] = []
    margin_days: list[dict[str, Any]] = []
    open_states: dict[str, tuple[_PositionEvidence, Decimal, Decimal]] = {}
    equity = starting_balance
    if not equity.is_finite() or equity <= 0:
        _fail("REPLAY_STARTING_BALANCE_INVALID")
    for day_text in dates:
        entries_event: list[dict[str, Any]] = []
        entries_fill: list[dict[str, Any]] = []
        entries_cost: list[dict[str, Any]] = []
        for position, volume in sorted(
            by_entry.get(day_text, []), key=lambda item: item[0].position_id
        ):
            if position.position_id in open_states:
                _fail("ACCEPTED_POSITION_REOPENED", position.position_id)
            opened_at = _utc_seconds(
                _timestamp(
                    position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID"
                ).isoformat(),
                "POSITION_ENTRY_TIME_INVALID",
            )
            event_id = _raw_event_id(anchor["anchor_id"], position.position_id, "OPEN")
            quantity = _decimal_text(volume)
            entries_event.append(
                _event_decisions(
                    event_id=event_id,
                    position_id=position.position_id,
                    cell_id=position.cell_id,
                    regime=position.regime,
                    decided_at_utc=opened_at,
                    action="OPEN",
                )
            )
            entries_fill.append(
                {
                    "event_id": event_id,
                    "position_id": position.position_id,
                    "cell_id": position.cell_id,
                    "opened_at_utc": opened_at,
                    "side": position.side,
                    "quantity": quantity,
                    "contract_multiplier": _decimal_text(position.contract_multiplier),
                    "stop_price": _decimal_text(position.stop_price),
                    "broker_fill_id": _broker_fill_id(
                        anchor["anchor_id"], position.position_id, "OPEN"
                    ),
                    "reference_mid_price": _decimal_text(position.entry_mid),
                    "reference_bid_price": _decimal_text(position.entry_bid),
                    "reference_ask_price": _decimal_text(position.entry_ask),
                    "base_fill_price": _decimal_text(position.base_entry_fill),
                    "stress_fill_price": _decimal_text(position.stress_entry_fill),
                }
            )
            entries_cost.append(
                {
                    "event_id": event_id,
                    "base_commission_cash": _decimal_text(
                        position.base_commission_per_lot * volume / Decimal(2)
                    ),
                    "stress_commission_cash": _decimal_text(
                        position.stress_commission_per_lot * volume / Decimal(2)
                    ),
                    "base_fee_cash": "0",
                    "stress_fee_cash": "0",
                }
            )
            open_states[position.position_id] = (
                position,
                volume,
                position.entry_mid,
            )

        mark_events: list[dict[str, Any]] = []
        raw_marks: list[dict[str, Any]] = []
        mark_costs: list[dict[str, Any]] = []
        exposure_positions: list[dict[str, Any]] = []
        daily_gross = Decimal(0)
        daily_base_cost = Decimal(0)
        daily_stress_cost = Decimal(0)
        for position_id in sorted(open_states):
            position, volume, last_mark = open_states[position_id]
            match = [mark for mark in position.marks if mark.date == day_text]
            if len(match) != 1:
                _fail("ACCEPTED_POSITION_DAILY_MARK_INCOMPLETE", position_id, day_text)
            mark = match[0]
            event_id = _raw_event_id(anchor["anchor_id"], position_id, "MARK", day_text)
            mark_events.append({"event_id": event_id, "position_id": position_id})
            raw_marks.append(
                {
                    "event_id": event_id,
                    "position_id": position_id,
                    "marked_at_utc": mark.marked_at_utc,
                    "mark_price": _decimal_text(mark.mid_price),
                    "mark_bid_price": _decimal_text(mark.bid_price),
                    "mark_ask_price": _decimal_text(mark.ask_price),
                }
            )
            base_finance = mark.base_financing_per_lot * volume
            stress_finance = mark.stress_financing_per_lot * volume
            mark_costs.append(
                {
                    "event_id": event_id,
                    "base_financing_cash": _decimal_text(base_finance),
                    "stress_financing_cash": _decimal_text(stress_finance),
                }
            )
            sign = Decimal(1) if position.side == "LONG" else Decimal(-1)
            unit = volume * position.contract_multiplier
            daily_gross += sign * unit * (mark.mid_price - last_mark)
            daily_base_cost += base_finance
            daily_stress_cost += stress_finance
            if (
                day_text
                == _timestamp(
                    position.candidate.entry_time, "POSITION_ENTRY_TIME_INVALID"
                )
                .date()
                .isoformat()
            ):
                executable = (
                    position.entry_ask
                    if position.side == "LONG"
                    else position.entry_bid
                )
                spread = sign * (executable - position.entry_mid) * unit
                base_slip = sign * (position.base_entry_fill - executable) * unit
                stress_slip = sign * (position.stress_entry_fill - executable) * unit
                daily_base_cost += max(Decimal(0), spread + base_slip)
                daily_stress_cost += max(Decimal(0), spread + stress_slip)
                daily_base_cost += (
                    position.base_commission_per_lot * volume / Decimal(2)
                )
                daily_stress_cost += (
                    position.stress_commission_per_lot * volume / Decimal(2)
                )
            required_margin = position.margin_per_lot * volume
            exposure_positions.append(
                {
                    "position_id": position_id,
                    "quantity": _decimal_text(volume),
                    "contract_multiplier": _decimal_text(position.contract_multiplier),
                    "stop_price": _decimal_text(position.stop_price),
                    "required_margin_cash": _decimal_text(required_margin),
                }
            )
            open_states[position_id] = (position, volume, mark.mid_price)

        exits_event: list[dict[str, Any]] = []
        exits_fill: list[dict[str, Any]] = []
        exits_cost: list[dict[str, Any]] = []
        for position, volume in sorted(
            by_exit.get(day_text, []), key=lambda item: item[0].position_id
        ):
            state = open_states.get(position.position_id)
            if state is None:
                _fail("ACCEPTED_POSITION_EXIT_NOT_OPEN", position.position_id)
            closed_at = _utc_seconds(
                _timestamp(
                    position.candidate.legs[-1].exit_time,
                    "POSITION_EXIT_TIME_INVALID",
                ).isoformat(),
                "POSITION_EXIT_TIME_INVALID",
            )
            event_id = _raw_event_id(anchor["anchor_id"], position.position_id, "CLOSE")
            exits_event.append(
                _event_decisions(
                    event_id=event_id,
                    position_id=position.position_id,
                    cell_id=position.cell_id,
                    regime=position.regime,
                    decided_at_utc=closed_at,
                    action="CLOSE",
                )
            )
            exits_fill.append(
                {
                    "event_id": event_id,
                    "position_id": position.position_id,
                    "closed_at_utc": closed_at,
                    "broker_fill_id": _broker_fill_id(
                        anchor["anchor_id"], position.position_id, "CLOSE"
                    ),
                    "reference_mid_price": _decimal_text(position.exit_mid),
                    "reference_bid_price": _decimal_text(position.exit_bid),
                    "reference_ask_price": _decimal_text(position.exit_ask),
                    "base_fill_price": _decimal_text(position.base_exit_fill),
                    "stress_fill_price": _decimal_text(position.stress_exit_fill),
                }
            )
            exits_cost.append(
                {
                    "event_id": event_id,
                    "base_commission_cash": _decimal_text(
                        position.base_commission_per_lot * volume / Decimal(2)
                    ),
                    "stress_commission_cash": _decimal_text(
                        position.stress_commission_per_lot * volume / Decimal(2)
                    ),
                    "base_fee_cash": "0",
                    "stress_fee_cash": "0",
                }
            )
            sign = Decimal(1) if position.side == "LONG" else Decimal(-1)
            unit = volume * position.contract_multiplier
            # The daily mark is an independently acquired M5 Open, while the
            # close is the exact specialist execution fill.  Realize the gross
            # mid-price move from that last common valuation to the execution
            # reference before charging closing spread/slippage/commission.
            daily_gross += sign * unit * (position.exit_mid - state[2])
            executable = (
                position.exit_bid if position.side == "LONG" else position.exit_ask
            )
            spread = sign * (position.exit_mid - executable) * unit
            base_slip = sign * (executable - position.base_exit_fill) * unit
            stress_slip = sign * (executable - position.stress_exit_fill) * unit
            daily_base_cost += max(Decimal(0), spread + base_slip)
            daily_stress_cost += max(Decimal(0), spread + stress_slip)
            daily_base_cost += position.base_commission_per_lot * volume / Decimal(2)
            daily_stress_cost += (
                position.stress_commission_per_lot * volume / Decimal(2)
            )
            del open_states[position.position_id]

        used_margin = sum(
            (
                _decimal(item["required_margin_cash"], "POSITION_MARGIN_INVALID")
                for item in exposure_positions
            ),
            Decimal(0),
        )
        free_margin = equity - used_margin
        if free_margin < 0:
            _fail("FORWARD_REPLAY_UNMODELED_MARGIN_CALL", anchor["anchor_id"], day_text)
        basis_text = _decimal_text(equity)
        event_days.append(
            {
                "date": day_text,
                "entries": entries_event,
                "marks": mark_events,
                "exits": exits_event,
            }
        )
        fill_days.append(
            {
                "date": day_text,
                "entries": entries_fill,
                "exits": exits_fill,
            }
        )
        mtm_days.append(
            {
                "date": day_text,
                "equity_basis_cash": basis_text,
                "position_marks": raw_marks,
            }
        )
        cost_days.append(
            {
                "date": day_text,
                "entries": entries_cost,
                "marks": mark_costs,
                "exits": exits_cost,
            }
        )
        exposure_days.append({"date": day_text, "positions": exposure_positions})
        margin_days.append(
            {
                "date": day_text,
                "equity_basis_cash": basis_text,
                "used_margin_cash": _decimal_text(used_margin),
                "free_margin_cash": _decimal_text(free_margin),
            }
        )
        equity += daily_gross - daily_base_cost
        if not equity.is_finite() or equity <= 0:
            _fail("FORWARD_REPLAY_EQUITY_RUIN", anchor["anchor_id"], day_text)
        if daily_stress_cost < daily_base_cost:
            _fail("FORWARD_REPLAY_STRESS_COST_IMPROVES", anchor["anchor_id"], day_text)
    if open_states:
        _fail("FORWARD_REPLAY_ENDS_WITH_OPEN_POSITIONS", anchor["anchor_id"])

    common = {
        "anchor_id": anchor["anchor_id"],
        "forward_start_date": anchor["forward_start_date"],
        "forward_end_date": anchor["forward_end_date"],
    }
    specialists_body = [
        {
            "schema_version": causal_producer.SPECIALIST_SCHEMA,
            "cell_id": value.cell_id,
            "strategy_module": value.strategy_module,
            "strategy_class": value.strategy_class,
            "parameters": dict(value.parameters),
            "refit_market_data_end_utc": anchor["selection_cutoff_utc"],
        }
        for value in replay.specialists
    ]
    return {
        "EVENT_LEDGER": {
            "schema_version": causal_producer.EVENT_LEDGER_SCHEMA,
            **common,
            "selection_cutoff_utc": anchor["selection_cutoff_utc"],
            "specialists": specialists_body,
            "initial_positions": [],
            "days": event_days,
        },
        "FILL_LEDGER": {
            "schema_version": causal_producer.FILL_LEDGER_SCHEMA,
            **common,
            "initial_positions": [],
            "days": fill_days,
        },
        "MTM_LEDGER": {
            "schema_version": causal_producer.MTM_LEDGER_SCHEMA,
            **common,
            "days": mtm_days,
        },
        "COST_LEDGER": {
            "schema_version": causal_producer.COST_LEDGER_SCHEMA,
            **common,
            "days": cost_days,
        },
        "EXPOSURE_LEDGER": {
            "schema_version": causal_producer.EXPOSURE_LEDGER_SCHEMA,
            **common,
            "days": exposure_days,
        },
        "MARGIN_LEDGER": {
            "schema_version": causal_producer.MARGIN_LEDGER_SCHEMA,
            **common,
            "days": margin_days,
        },
    }
