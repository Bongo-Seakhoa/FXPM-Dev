"""Source-bound frozen-tier R&D replay and strict legacy archive verification.

The private replay transports one fixed owner's independently executed P75/P95
accounts from a declared new simulated genesis. It does not qualify either
account. Public production entry points remain retired until the interval
qualification replacement is implemented and independently accepted.
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
import pm_production_config_binding as config_binding
import pm_regime
import pm_spread_profile as spread_profile
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner
from pm_core import (
    Backtester,
    ContinuousEntryOwner,
    ContinuousEntrySchedule,
    ContinuousEntrySlot,
    FeatureComputer,
    InstrumentSpec,
    PipelineConfig,
    build_backtest_equity_growth,
    build_backtest_position_events,
)
from pm_current_refit_parameters import decode_parameter_mapping
from pm_strategies import StrategyRegistry


UTC: Final = timezone.utc
PRODUCTION_CONTRACT_VERSION: Final = "fxpm.forward-oos-replay-producer.v5"
REPLAY_PRECOMMIT_SCHEMA: Final = "fxpm.forward-oos-replay-precommit.v5"
PRODUCER_RECEIPT_SCHEMA: Final = "fxpm.forward-oos-replay-producer-receipt.v5"
CAPITAL_CANDIDATE_CHUNK_SCHEMA: Final = (
    "fxpm.forward-oos-capital-candidate-chunk.v2"
)
CAPITAL_PHASE_MANIFEST_SCHEMA: Final = "fxpm.forward-oos-capital-phase-manifest.v1"
CAPITAL_INNER_SELECTION: Final = "INNER_SELECTION"
CAPITAL_OUTER_EVALUATION: Final = "OUTER_EVALUATION"
CAPITAL_PHASES: Final = (CAPITAL_INNER_SELECTION, CAPITAL_OUTER_EVALUATION)
CAPITAL_SELECTION_CALENDAR_DAYS: Final = 365
MARGIN_MODEL: Final = "AUTHENTICATED_TICK_CASH_NOTIONAL_100PCT_V1"
POSITION_SOURCE_SCHEMA: Final = "fxpm.forward-oos-backtester-position.v2"
FROZEN_TIER_RAW_ACCOUNTS_SCHEMA: Final = "fxpm.frozen-tier-raw-accounts.v3"
DECLARED_CELL_RAW_ACCOUNT_SCHEMA: Final = "fxpm.declared-cell-raw-account.v4"
RETAINED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA: Final = "fxpm.frozen-tier-raw-accounts.v2"
ARCHIVED_FROZEN_TIER_RAW_ACCOUNTS_SCHEMA: Final = "fxpm.frozen-tier-raw-accounts.v1"
NEW_SIMULATED_FLAT_GENESIS: Final = "NEW_SIMULATED_FLAT_GENESIS"
VALUATION_CONTRACT: Final = (
    "AUTHENTICATED_NATIVE_M5_OPEN_EXECUTABLE_BID_ASK_PLUS_SWAP_V2"
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


def _original_capital_window(anchor, precommit_anchor, phase):
    """Reapply the existing anchor/phase geometry to the retained precommit."""
    windows = precommit_anchor.get("capital_phase_windows", {})
    try:
        derived = _capital_phase_windows([anchor], {"anchor_plan": [{
            "anchor_id": anchor["anchor_id"],
            "inner_window": windows[CAPITAL_INNER_SELECTION],
            "outer_window": windows[CAPITAL_OUTER_EVALUATION],
        }]})
        return derived[anchor["anchor_id"]][phase]
    except (KeyError, TypeError) as exc:
        raise ForwardOOSReplayProductionError("FROZEN_TIER_ORIGINAL_WINDOW_GEOMETRY_UNAVAILABLE") from exc


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
    pin_consumed_activation: bool = False,
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
            **({"pinned_activation_manifest_sha256": expected_identity[
                "activation_manifest_sha256"]} if pin_consumed_activation else {}),
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
    return _checked_execution_spec_receipt(product=product, deployment=deployment, spec=spec, receipt=receipt)


def _checked_execution_spec_receipt(*, product, deployment, spec, receipt):
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


def _derive_retained_execution_spec(*, product, deployment, source_bytes):
    """Reuse canonical economics with already reopened original source bytes."""
    try:
        spec, receipt = search_execution._derive_bound_execution_spec(
            product=product, deployment_config_bytes=source_bytes["config"],
            broker_specs_bytes=source_bytes["broker"],
            broker_specs_relative_path=deployment.broker_relative_path)
    except search_execution.AuthoritativeSearchExecutionError as exc:
        raise ForwardOOSReplayProductionError("FORWARD_EXECUTION_SPEC_FREEZE_FAILED", (exc.code,)) from exc
    return _checked_execution_spec_receipt(product=product, deployment=deployment, spec=spec, receipt=receipt)


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
    origin_symbol_receipt: Mapping[str, Any] | None = None
    origin_adapter_index_descriptor: Mapping[str, Any] | None = None








@dataclass(frozen=True, slots=True)
class _AnchorRawReplay:
    """One new simulated account per frozen tier, never a qualified Book."""

    anchor: Mapping[str, Any]
    specialists: tuple[_VerifiedSpecialist, ...]
    raw_accounts: Mapping[str, Any]
    production_metadata: Mapping[str, Any]


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
        "backtest_spread_mode",
        "swap_model_enabled",
        "swap_model_triple_rollover_day",
        "swap_model_min_holding_hours",
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


def _read_descriptor_bytes(
    context: authority_spool.GenerationSpoolContext,
    value: Any,
    *,
    code: str,
    maximum: int | None = None,
) -> bytes:
    """Read sealed canonical objects or lists without changing the object reader."""
    descriptor = _descriptor(value, f"{code}_DESCRIPTOR_INVALID")
    limit = int(maximum or context.limits.max_json_bytes)
    if (
        descriptor["format"] not in (
            authority_spool.CANONICAL_JSON_FORMAT, "fxpm-ascii-json-v1"
        )
        or descriptor["size_bytes"] > limit
    ):
        _fail(f"{code}_FORMAT_OR_BOUND_INVALID")
    try:
        root = Path(context.root).resolve(strict=True)
        target = root.joinpath(*Path(descriptor["relative_path"]).parts)
        if target.is_symlink():
            _fail(f"{code}_OBJECT_INVALID")
        path = target.resolve(strict=True)
        path.relative_to(root)
        if not path.is_file():
            _fail(f"{code}_OBJECT_INVALID")
        with path.open("rb") as handle:
            encoded = handle.read(limit + 1)
    except ForwardOOSReplayProductionError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_UNAVAILABLE") from exc
    if (
        len(encoded) != descriptor["size_bytes"]
        or len(encoded) > limit
        or hashlib.sha256(encoded).hexdigest() != descriptor["sha256"]
    ):
        _fail(f"{code}_INTEGRITY_MISMATCH")
    try:
        body = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForwardOOSReplayProductionError(f"{code}_JSON_INVALID") from exc
    if type(body) not in {dict, list} or _canonical_bytes(body, limit) != encoded:
        _fail(f"{code}_CANONICAL_MISMATCH")
    return encoded


def _selected_owner_generation(
    book_body: Mapping[str, Any],
    request_body: Mapping[str, Any],
    decision_body: Mapping[str, Any],
) -> Any:
    if any(type(body) is not dict for body in (book_body, request_body, decision_body)):
        _fail("SELECTED_OWNER_GENERATION_TUPLE_INVALID")
    payload_versions = []
    for name in ("precommitment", "evidence_ledger"):
        wrapper = request_body.get(name)
        payload = wrapper.get("payload") if type(wrapper) is dict else None
        payload_versions.append(payload.get("contract_version") if type(payload) is dict else None)
    observed = (
        book_body.get("contract_version"), request_body.get("contract_version"),
        *payload_versions, decision_body.get("contract_version"),
    )
    for generation in (book.ARCHIVED_GENERATION, book.CURRENT_GENERATION, book.V16_GENERATION):
        expected = generation.winner_generation
        if observed == (
            generation.version, expected.request_version,
            expected.precommitment_version, expected.evidence_version,
            expected.selection_version,
        ):
            return generation
    _fail("SELECTED_OWNER_GENERATION_TUPLE_INVALID")


def _restore_current_owner_frozen_binding(
    *,
    context: authority_spool.GenerationSpoolContext,
    entries: Mapping[str, Mapping[str, Any]],
    symbol: str,
    timeframe: str,
    regime: str,
    namespace: str,
    cross: Mapping[str, Any],
    _generation: Any = book.CURRENT_GENERATION,
) -> tuple[Any, dict[str, Any], tuple[dict[str, Any], ...], dict[str, Any]]:
    """Restore only the sealed current historical owner, without an issuer or sink."""
    schema = cross.get("schema_version")
    if schema in (
        raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA,
        raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4,
    ) or (_generation is book.V16_GENERATION
          and schema == raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5):
        _fail("CURRENT_OWNER_CROSS_BINDING_MODE_FORBIDDEN")
    if (_generation is not book.CURRENT_GENERATION and _generation is not book.V16_GENERATION
            or schema != _generation.cross_binding_schema):
        _fail("CURRENT_OWNER_CROSS_BINDING_SCHEMA_INVALID")
    prefix = f"{namespace}/winner-search/{timeframe}/{regime}/frozen/"
    selected_entries = [
        dict(entry) for artifact_id, entry in sorted(entries.items())
        if artifact_id.startswith(prefix)
    ]
    bodies = [
        _read_descriptor_bytes(context, entry.get("descriptor"),
            code="CURRENT_OWNER_FROZEN_ARTIFACT", maximum=winner.MAX_REQUEST_BYTES)
        for entry in selected_entries
    ]
    try:
        restore = (raw_winner.restore_frozen_winner_search_plan_v5
                   if _generation is book.V16_GENERATION
                   else raw_winner.restore_frozen_winner_search_plan_v4)
        frozen = restore(
            context_identity={
                "generation_id": context.generation_id,
                "generation_context_sha256": context.context_sha256,
                "native_universe_sha256": context.native_universe_sha256,
                "config_sha256": context.config_sha256,
            },
            symbol=symbol, timeframe=timeframe, regime=regime,
            artifact_namespace=namespace,
            manifest_entries=selected_entries, body_bytes=bodies,
        )
    except raw_winner.WinnerRawEvidenceError as exc:
        raise ForwardOOSReplayProductionError(
            "CURRENT_OWNER_FROZEN_RESTORE_FAILED", (exc.code,)
        ) from exc
    if not frozen.frozen_tier_pair_contract_bytes:
        _fail("CURRENT_OWNER_FROZEN_PAIR_GENERATION_MISMATCH")
    if (_generation is book.V16_GENERATION
            and frozen.selection_generation is not winner.V16_GENERATION):
        _fail("CURRENT_OWNER_FROZEN_PAIR_GENERATION_MISMATCH")
    try:
        binding, attempts, binding_context = raw_winner.verify_historical_frozen_winner_cross_binding(
            _canonical_bytes(dict(cross), winner.MAX_REQUEST_BYTES), frozen=frozen
        )
    except raw_winner.WinnerRawEvidenceError as exc:
        raise ForwardOOSReplayProductionError(
            "CURRENT_OWNER_CROSS_BINDING_REVERIFY_FAILED", (exc.code,)
        ) from exc
    return frozen, binding, attempts, binding_context


def _verify_current_owner_capture_binding(
    *,
    context: authority_spool.GenerationSpoolContext,
    entries: Mapping[str, Mapping[str, Any]],
    anchor: Mapping[str, Any],
    index: Mapping[str, Any],
    record: Mapping[str, Any],
    request: winner.WinnerSelectionRequest,
    decision: winner.WinnerSelectionDecision,
    frozen: Any,
    cross: Mapping[str, Any],
    cell_id: str,
    _generation: Any = book.CURRENT_GENERATION,
) -> None:
    """Bind the restored pre-search graph to its original adapter and raw result."""
    symbol, timeframe, regime = frozen.symbol, frozen.timeframe, frozen.regime
    cell = {"symbol": symbol, "timeframe": timeframe, "regime": regime}
    prefix = f"{frozen.artifact_namespace}/winner-search/{timeframe}/{regime}"
    if (
        index.get("schema_version") != historical_adapter.SYMBOL_INDEX_SCHEMA
        or index.get("generation_id") != context.generation_id
        or index.get("generation_context_sha256") != context.context_sha256
        or index.get("anchor") != anchor
        or index.get("symbol") != symbol
        or cross.get("producer_request_sha256") != index.get("producer_request_sha256")
        or frozen.precommitment != request.precommitment.to_dict()
        or frozen.attempt_ledger != request.attempt_ledger.to_dict()
    ):
        _fail("CURRENT_OWNER_INDEX_BINDING_MISMATCH", cell_id)
    bindings = index.get("horizon_bindings")
    if type(bindings) is not list:
        _fail("CURRENT_OWNER_HORIZON_BINDING_INVALID", cell_id)
    matches = [row for row in bindings if type(row) is dict and row.get("cell") == cell]
    if len(matches) != 1:
        _fail("CURRENT_OWNER_HORIZON_BINDING_INVALID", cell_id)
    binding = matches[0]
    if (
        binding.get("schema_version") != historical.CURRENT_HORIZON_BINDING_SCHEMA
        or binding.get("anchor_id") != anchor["anchor_id"]
        or binding.get("selection_cutoff_utc") != anchor["selection_cutoff_utc"]
        or binding.get("process_precommit_receipt_sha256")
        != cross.get("process_precommit_receipt_sha256")
        or cross.get("selection_cutoff_utc") != anchor["selection_cutoff_utc"]
        or cross.get("horizon_roster") != {
            name: binding.get(name) for name in (
                "plan_receipt_sha256", "source_prefix_receipt_sha256",
                "candidate_registry_sha256", "trial_registry_sha256",
                "attempt_roster_sha256", "evidence_roster_sha256",
            )
        }
    ):
        _fail("CURRENT_OWNER_HORIZON_BINDING_SPLICE", cell_id)
    try:
        group_ordinal = book.TIMEFRAME_ORDER.index(timeframe)
        sources, fits = index["source_groups"], index["regime_fits"]
        if (type(sources) is not list or type(fits) is not list
                or len(sources) != len(book.TIMEFRAME_ORDER) or len(fits) != len(sources)):
            _fail("CURRENT_OWNER_GROUP_ROSTER_INVALID", cell_id)
        source, fit = sources[group_ordinal], fits[group_ordinal]
        for group in (source, fit):
            if (type(group) is not dict or group.get("symbol") != symbol
                    or group.get("timeframe") != timeframe
                    or group.get("receipt_sha256")
                    != _sha({key: value for key, value in group.items() if key != "receipt_sha256"})):
                _fail("CURRENT_OWNER_GROUP_BINDING_INVALID", cell_id)
    except (KeyError, ValueError, TypeError) as exc:
        raise ForwardOOSReplayProductionError("CURRENT_OWNER_GROUP_ROSTER_INVALID", (cell_id,)) from exc
    if (
        cross.get("source_group_decision_sha256") != source["receipt_sha256"]
        or cross.get("regime_fit_decision_sha256") != fit["receipt_sha256"]
        or fit.get("source_group_decision_sha256") != source["receipt_sha256"]
        or source.get("source_contract_sha256") != frozen.precommitment.get("source_contract_sha256")
    ):
        _fail("CURRENT_OWNER_GROUP_BINDING_SPLICE", cell_id)
    native_index = _hex(cross.get("native_prefix_index_sha256"), "CURRENT_OWNER_NATIVE_PREFIX_INDEX_INVALID")
    if source.get("source_group_evidence_sha256") != native_index:
        _fail("CURRENT_OWNER_NATIVE_PREFIX_INDEX_SPLICE", cell_id)
    for mapped in cross["mapping"]:
        count = mapped["source_count_receipt"]
        if (count["native_prefix_index_sha256"] != native_index
                or count["horizon_plan_receipt_sha256"] != binding["plan_receipt_sha256"]):
            _fail("CURRENT_OWNER_SOURCE_COUNT_SPLICE", cell_id)
    frozen_entry = entries.get(f"{prefix}/frozen/receipt")
    if (type(frozen_entry) is not dict
            or _descriptor(record.get("frozen_plan_receipt"), "CURRENT_OWNER_FROZEN_RECEIPT_INVALID")
            != frozen_entry.get("descriptor")
            or frozen_entry["descriptor"]["sha256"]
            != cross["winner_roster"]["frozen_receipt_sha256"]):
        _fail("CURRENT_OWNER_FROZEN_RECEIPT_SPLICE", cell_id)
    production_id = f"{prefix}/authority/production-receipt"
    production = _manifest_body(context, entries, production_id,
        code="CURRENT_OWNER_RAW_PRODUCTION", expected_descriptor=record.get("raw_production_receipt"),
        maximum=winner.MAX_REQUEST_BYTES)
    production_fields = {
        "schema_version", "generation_context_sha256", "run_id", "artifact_namespace",
        "symbol", "timeframe", "regime", "precommitment_sha256", "attempt_ledger_sha256",
        "provenance_cross_binding_sha256", "evidence_ledger_sha256", "request_sha256",
        "decision_receipt_sha256", "artifact_roster",
    }
    roster = production.get("artifact_roster")
    if (
        set(production) != production_fields
        or entries[production_id].get("artifact_type") != "winner_raw_evidence_production_receipt"
        or production.get("schema_version") != _generation.raw_schema
        or production.get("generation_context_sha256") != context.context_sha256
        or production.get("run_id") != frozen.precommitment["run_id"]
        or production.get("artifact_namespace") != frozen.artifact_namespace
        or {name: production.get(name) for name in cell} != cell
        or production.get("precommitment_sha256") != request.precommitment.payload_sha256
        or production.get("attempt_ledger_sha256") != request.attempt_ledger.payload_sha256
        or production.get("evidence_ledger_sha256") != request.evidence_ledger.payload_sha256
        or production.get("request_sha256") != request.request_sha256
        or production.get("decision_receipt_sha256") != decision.receipt_sha256
        or production.get("provenance_cross_binding_sha256") != _sha(dict(cross))
        or type(roster) is not dict
    ):
        _fail("CURRENT_OWNER_RAW_PRODUCTION_SPLICE", cell_id)
    sealed_descriptors = {_canonical_bytes(entry["descriptor"]) for entry in entries.values()}
    for descriptor in roster.values():
        normalized = _descriptor(descriptor, "CURRENT_OWNER_RAW_ROSTER_DESCRIPTOR_INVALID")
        if _canonical_bytes(normalized) not in sealed_descriptors:
            _fail("CURRENT_OWNER_RAW_ROSTER_SPLICE", cell_id)
    for name in ("evidence-ledger", "winner-request", "winner-decision"):
        entry = entries.get(f"{prefix}/authority/{name}")
        if type(entry) is not dict or roster.get(name) != entry.get("descriptor"):
            _fail("CURRENT_OWNER_RAW_ROSTER_SPLICE", cell_id)


def _winner_request(
    body: Mapping[str, Any], expected_sha256: str
) -> winner.WinnerSelectionRequest:
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
    try:
        request = winner.create_winner_selection_request(
            wrappers[0]["payload"], wrappers[1]["payload"], wrappers[2]["payload"]
        )
    except ValueError as exc:
        if body["contract_version"] == winner.V16_GENERATION.request_version:
            code = str(exc) if str(exc).startswith("V16_") else "WINNER_REQUEST_RAW_REPLAY_FAILED"
            raise ForwardOOSReplayProductionError(code, (str(exc),)) from exc
        raise
    if (
        request is None
        or request.request_sha256 != expected_sha256
        or request.to_dict() != body
    ):
        _fail("WINNER_REQUEST_RAW_REPLAY_FAILED")
    return request


def _winner_decision(
    body: Mapping[str, Any],
    request: winner.WinnerSelectionRequest,
    expected_sha256: str,
) -> winner.WinnerSelectionDecision:
    if type(body) is not dict:
        _fail("WINNER_DECISION_NOT_OBJECT")
    owned = json.loads(
        _canonical_bytes(body, winner.MAX_DECISION_BYTES).decode("ascii")
    )
    receipt = owned.pop("receipt_sha256", None)
    if receipt != expected_sha256 or _sha(owned) != receipt:
        _fail("WINNER_DECISION_RECEIPT_MISMATCH")
    decision = winner.WinnerSelectionDecision(
        _canonical_bytes(owned, winner.MAX_DECISION_BYTES), receipt
    )
    if (
        winner.verify_historical_cell_winner_decision(decision, request) is not True
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
    original_source = book_sources[global_group_ordinal]
    original_fit = book_fits[global_group_ordinal]
    # The verified adapter index retains Decision.to_dict(); the verified Book
    # retains _serialized_owned(). Bridge representation only, comparing every
    # original payload field AND its unchanged original receipt. This is not
    # standalone authentication of a caller-supplied self hash.
    for flat, envelope in ((source, original_source), (fit, original_fit)):
        if (type(flat) is not dict or type(envelope) is not dict
                or set(envelope) != {"payload", "receipt_sha256"}
                or type(envelope["payload"]) is not dict
                or "receipt_sha256" not in flat
                or flat["receipt_sha256"] != envelope["receipt_sha256"]
                or {key: value for key, value in flat.items() if key != "receipt_sha256"} != envelope["payload"]):
            _fail("SELECTED_ADAPTER_BOOK_GROUP_SPLICE", cell_id)
    return dict(original_source), dict(original_fit)


def _selected_regime_cutoff_matches_source(
    *, detector_cutoff_utc: Any, source_prefix_end_utc: Any, selection_cutoff_utc: Any,
) -> bool:
    """The detector owns its exact closed prefix, which may precede selection."""
    detector_cutoff = _utc(detector_cutoff_utc, "SELECTED_REGIME_CUTOFF_INVALID")
    source_end = _utc(source_prefix_end_utc, "SELECTED_SOURCE_PREFIX_END_INVALID")
    selection_cutoff = _utc(selection_cutoff_utc, "SELECTION_CUTOFF_INVALID")
    return detector_cutoff == source_end and source_end <= selection_cutoff


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
        maximum=(winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES
                 if book_body.get("contract_version") == book.V16_GENERATION.version
                 else winner.MAX_REQUEST_BYTES),
    )
    request = _winner_request(request_body, book_cell["winner_request_sha256"])
    decision_body = _manifest_body(
        context,
        entries,
        f"{cell_namespace}/authority/winner-decision",
        code="WINNER_DECISION",
        maximum=winner.MAX_DECISION_BYTES,
    )
    decision = _winner_decision(
        decision_body, request, book_cell["winner_decision_sha256"]
    )
    generation = _selected_owner_generation(book_body, request.to_dict(), decision.to_dict())
    decision_winner = decision.to_dict().get("winner")
    process_candidate_id = book_cell.get("local_process_candidate_id")
    process_sha256 = book_cell.get("local_process_sha256")
    if (
        type(decision_winner) is not dict
        or decision.status != "PROCESS_WINNER_PENDING_BOOK_MULTIPLICITY"
        or decision_winner.get("process_candidate_id") != process_candidate_id
        or decision_winner.get("process_sha256") != process_sha256
    ):
        _fail("BOOK_SELECTED_PROCESS_NOT_WINNER_V14", cell_id)

    attempt_ledger = _manifest_body(
        context,
        entries,
        f"{cell_namespace}/frozen/attempt_ledger",
        code="WINNER_ATTEMPT_LEDGER",
        maximum=winner.MAX_REQUEST_BYTES,
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
    if generation is book.CURRENT_GENERATION or generation is book.V16_GENERATION:
        frozen, cross, _verified_attempts, _binding_context = _restore_current_owner_frozen_binding(
            context=context, entries=entries, symbol=symbol, timeframe=timeframe,
            regime=regime, namespace=namespace, cross=cross, _generation=generation,
        )
        _verify_current_owner_capture_binding(
            context=context, entries=entries, anchor=anchor, index=index, record=record,
            request=request, decision=decision, frozen=frozen, cross=cross, cell_id=cell_id,
            _generation=generation,
        )
    expected_cross_schema = (generation.cross_binding_schema
        if not generation.winner_generation.archived
        else historical_adapter.HORIZON_WINNER_CROSS_BINDING_SCHEMA)
    mapping = cross.get("mapping")
    if (
        cross.get("schema_version")
        != expected_cross_schema
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
        or not _selected_regime_cutoff_matches_source(
            detector_cutoff_utc=detector_record.get("fit_cutoff_utc"),
            source_prefix_end_utc=count_receipt.get("fit_end_exclusive_utc"),
            selection_cutoff_utc=anchor["selection_cutoff_utc"],
        )
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
                    replace(_selected_artifacts(
                        context=context,
                        entries=entries,
                        anchor=anchor,
                        book_body=books[anchor_id],
                        book_cell=winner_cell,
                        index=index,
                    ), origin_symbol_receipt=dict(symbol_receipt),
                       origin_adapter_index_descriptor=dict(payload["production_index"]))
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
















def _open_m5_valuation_tape(
    *,
    context: authority_spool.GenerationSpoolContext,
    product: native_prefix.NativePrefixProduct,
    expected_identity: Mapping[str, Any],
    config: PipelineConfig,
    deployment: _DeploymentBinding,
    forward_start: pd.Timestamp,
    forward_end_exclusive: pd.Timestamp,
    spread_tier: spread_profile.FrozenSpreadTier,
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
            frame, spec, spread_tier=spread_tier, timeframe=spread_tier.timeframe
        )
        cost_attestation = backtester._numerical_execution_cost_attestation(
            entry_half_spread=entry_half,
            intrabar_half_spread=intrabar_half,
            spread_meta=spread_meta,
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
            "mark_cost_basis": "EXECUTABLE_BID_ASK_PLUS_ACCRUED_SWAP",
        },
        execution_spec_receipt=spec_receipt,
        native_product_identity=dict(expected_identity),
    )
    return tape, spec


def _raw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _raw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [_raw_json(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _owner_provenance(owner: _VerifiedSpecialist) -> dict[str, Any]:
    return {"parameters": _raw_json(owner.parameters), "source_count_receipt": dict(owner.source_count_receipt),
            "detector_parameters_sha256": owner.detector_parameters_sha256,
            "strategy_id": owner.strategy_id, "strategy_class": owner.strategy_class,
            "strategy_module": owner.strategy_module, "qualification_authority": False}


def _reopen_owner_capture(*, context, anchor, book_body, cell_id, symbol_receipt, adapter_index_descriptor):
    if type(symbol_receipt) is not dict or type(adapter_index_descriptor) is not dict:
        _fail("FROZEN_TIER_ORIGINAL_OWNER_CAPTURE_UNAVAILABLE")
    matches = [cell for cell in book_body.get("cells", ()) if cell.get("cell_id") == cell_id]
    if len(matches) != 1:
        _fail("FROZEN_TIER_ORIGINAL_BOOK_OWNER_MISMATCH")
    cell = matches[0]
    _, entries = _verified_manifest(context, symbol_receipt, cell["symbol"])
    index = _manifest_body(context, entries, f"{anchor['anchor_id']}/adapter/production-index",
        code="FROZEN_TIER_ORIGINAL_ADAPTER_INDEX", expected_descriptor=adapter_index_descriptor,
        maximum=32 * 1024 * 1024)
    if book_body.get("contract_version") in (book.CURRENT_GENERATION.version, book.V16_GENERATION.version):
        stored_anchor = index.get("anchor")
        if (type(anchor) is dict and type(stored_anchor) is dict
                and anchor.keys() == stored_anchor.keys()
                and all(type(anchor[key]) is type(stored_anchor[key])
                        and anchor[key] == stored_anchor[key]
                        for key in anchor if key != "selection_cutoff_utc")):
            cutoff, stored_cutoff = (value.get("selection_cutoff_utc")
                                     for value in (anchor, stored_anchor))
            utc_form = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)"
            if all(type(value) is str and re.fullmatch(utc_form, value)
                   for value in (cutoff, stored_cutoff)):
                try:
                    same_instant = datetime.fromisoformat(cutoff) == datetime.fromisoformat(stored_cutoff)
                except ValueError:
                    same_instant = False
                if same_instant:
                    # Resolve caller notation; keep every sealed binding byte unchanged.
                    anchor = stored_anchor
        else:
            _fail("CURRENT_OWNER_INDEX_BINDING_MISMATCH", cell_id)
    return _selected_artifacts(context=context, entries=entries, anchor=anchor, book_body=book_body,
                              book_cell=cell, index=index)


def _reopen_original_global_precommit(*, context, descriptor, precommit_anchor, deployment):
    """Bind original input bytes; this is not issued-release authentication."""
    if type(descriptor) is not dict:
        _fail("FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT_UNAVAILABLE")
    original = _read_descriptor_json(context, descriptor, code="FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT")
    anchors = original.get("anchor_product_identities")
    if (original.get("schema_version") != REPLAY_PRECOMMIT_SCHEMA
            or original.get("generation_id") != context.generation_id
            or original.get("generation_context_sha256") != context.context_sha256
            or original.get("native_universe_sha256") != context.native_universe_sha256
            or original.get("config_sha256") != context.config_sha256
            or original.get("deployment_binding") != deployment.receipt_body()
            or type(anchors) is not list or original.get("anchor_product_identities_sha256") != _sha(anchors)
            or sum(value == precommit_anchor for value in anchors) != 1):
        _fail("FROZEN_TIER_ORIGINAL_GLOBAL_PRECOMMIT_BINDING_CHANGED")
    return original


def _reopen_retained_deployment(*, context, descriptors, original_receipt, config_record):
    """Rebuild the original effective config without opening live config files."""
    original = _validate_deployment_binding_projection(original_receipt)
    if (type(config_record) is not dict
            or set(config_record) != {"active_config_path", "config_sha256", "effective_config_sha256"}
            or config_record["active_config_path"] != original["deployment_config_path"]):
        _fail("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_CHANGED")
    sources, source_bytes = {}, {}
    for role, prefix in (("config", "deployment_config"), ("broker", "broker_specs")):
        descriptor, raw = native_prefix._read_spool_descriptor(context, descriptors[role],
            expected_format="json-" + role, collect=True)
        if (descriptor["sha256"] != original[prefix + "_sha256"]
                or descriptor["size_bytes"] != original[prefix + "_size_bytes"]):
            _fail("FROZEN_TIER_DEPLOYMENT_SOURCE_PRECOMMIT_SPLICE")
        try:
            body = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=search_execution._strict_pairs,
                              parse_constant=search_execution._reject_json_constant)
        except (search_execution.AuthoritativeSearchExecutionError, UnicodeError, ValueError) as exc:
            raise ForwardOOSReplayProductionError("FROZEN_TIER_RETAINED_DEPLOYMENT_JSON_INVALID") from exc
        if type(body) is not dict:
            _fail("FROZEN_TIER_RETAINED_DEPLOYMENT_JSON_INVALID")
        sources[role] = body
        source_bytes[role] = raw
    payload, broker = sources["config"], sources["broker"]
    canonical_sha = config_binding.cap.canonical_json_sha256(payload)
    if canonical_sha != context.config_sha256 or canonical_sha != config_record["config_sha256"]:
        _fail("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_CHANGED")
    config_binding._verify_context(context, canonical_sha)
    path = Path(original["deployment_config_path"])
    if (not path.is_absolute() or type(payload.get("pipeline")) is not dict
            or type(payload.get("position")) is not dict or type(payload.get("_meta")) is not dict
            or not str(payload["_meta"].get("scope") or "").strip()):
        _fail("FROZEN_TIER_RETAINED_DEPLOYMENT_POLICY_INVALID")
    config = config_binding._derive_pipeline_config(payload, path)
    effective = {"schema_version": config_binding.BINDING_SCHEMA,
                 "pipeline_config": config_binding._snapshot(config)}
    effective_sha = hashlib.sha256(config_binding._canonical_bytes(
        effective, maximum=config_binding.MAX_SNAPSHOT_BYTES)).hexdigest()
    identity = _pipeline_config_identity(config)
    if (effective_sha != config_record["effective_config_sha256"]
            or identity != original["pipeline_config_identity"]
            or _sha(identity) != original["pipeline_config_identity_sha256"]
            or float(payload["pipeline"].get("initial_capital")) != float(config.initial_capital)):
        _fail("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_CHANGED")
    broker_relative = payload.get("broker_specs_path")
    if type(broker_relative) is not str or not broker_relative.strip():
        _fail("FROZEN_TIER_RETAINED_DEPLOYMENT_POLICY_INVALID")
    broker_path = Path(broker_relative)
    if not broker_path.is_absolute():
        broker_path = path.parent / broker_path
    broker_path = broker_path.resolve()
    if broker_relative != original["broker_specs_relative_path"] or str(broker_path) != original["broker_specs_path"]:
        _fail("FROZEN_TIER_DEPLOYMENT_SOURCE_PRECOMMIT_SPLICE")
    deployment = _DeploymentBinding(payload, path, original["deployment_config_sha256"], original["deployment_config_size_bytes"],
        broker, broker_path, broker_relative, original["broker_specs_sha256"], original["broker_specs_size_bytes"],
        identity, _sha(identity))
    if deployment.receipt_body() != original:
        _fail("FROZEN_TIER_DEPLOYMENT_SOURCE_PRECOMMIT_SPLICE")
    return config, deployment, source_bytes


def _reconcile_tier_account(account: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild derived state from actual fills, model events and close quotes.

    Cash is a derivation, not an engine observation. Matching float operations
    and JSON round-trip precision permit exact comparison with the unrounded
    engine curve; a mismatch is an error, never missing coverage.
    """
    if type(account) is not dict:
        _fail("FROZEN_TIER_ACCOUNT_INVALID")
    rows, trades, events = (account.get(key) for key in ("source_rows", "trades", "position_events"))
    if not all(type(value) is list for value in (rows, trades, events)) or not rows:
        _fail("FROZEN_TIER_ACCOUNT_RECORDS_MISSING")
    if any(row.get("engine_equity") is None for row in rows):
        _fail("FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE")
    if (type(account.get("warmup_bars")) is not int or not 1 <= account["warmup_bars"] < len(rows)
            or not math.isfinite(float(account.get("initial_cash", 0))) or account["initial_cash"] <= 0):
        _fail("FROZEN_TIER_ACCOUNT_GENESIS_INVALID")
    if len(trades) > _MAX_CANDIDATE_POSITIONS_PER_ANCHOR:
        _fail("FORWARD_POSITION_COUNT_BOUND_EXCEEDED")
    spec = InstrumentSpec(**account["instrument_spec"])
    tier = account["tier"]
    if tier.get("tier") not in ("P75", "P95") or tier.get("qualification_authority") is not False:
        _fail("FROZEN_TIER_ACCOUNT_DESCRIPTOR_INVALID")
    if (tier.get("symbol"), tier.get("timeframe"), tier.get("point_price")) != (
        account["symbol"], account["timeframe"], spec.point
    ):
        _fail("FROZEN_TIER_ACCOUNT_DESCRIPTOR_SPLICE")
    floor = float(tier["full_spread_price"]) / 2.0
    if floor != float(tier["spread_points"]) * spec.point / 2.0 or floor <= 0:
        _fail("FROZEN_TIER_ACCOUNT_WIDTH_INVALID")
    namespace = account["position_namespace"]
    rebuilt = _raw_json(build_backtest_position_events(trades, namespace=namespace))
    if rebuilt != events:
        _fail("BACKTEST_POSITION_EVENTS_V2_RAW_REVERIFY_FAILED", account["cell_id"])
    position_ids = []
    for trade in trades:
        single = build_backtest_position_events([trade], namespace=namespace)
        if len(single) != 1:
            _fail("PARTIAL_OR_MULTI_LEG_POSITION_NOT_REPRESENTABLE")
        position_ids.append(single[0]["position_id"])
    if len(set(position_ids)) != len(position_ids) or len(rebuilt) != len(trades):
        _fail("PARTIAL_OR_MULTI_LEG_POSITION_NOT_REPRESENTABLE")
    model = account["financing_model"]
    pre, close = model["pre_open_units"], model["close_units"]
    long_rates, short_rates = model["long_per_lot"], model["short_per_lot"]
    if any(len(value) != len(rows) for value in (pre, close, long_rates, short_rates)):
        _fail("FROZEN_TIER_FINANCING_AXIS_INVALID")
    if any(type(value) is not int or value < 0 for value in (*pre, *close)):
        _fail("FROZEN_TIER_FINANCING_UNITS_INVALID")
    if model["enabled"] is not True and (any(pre) or any(close)):
        _fail("FROZEN_TIER_FINANCING_DISABLED_WITH_EVENTS")
    opens = pd.DatetimeIndex([row["open_utc"] for row in rows])
    closes = pd.DatetimeIndex([row["close_utc"] for row in rows])
    if (opens.tz is None or closes.tz is None or opens.has_duplicates or closes.has_duplicates
            or not opens.is_monotonic_increasing or not closes.is_monotonic_increasing
            or bool((opens >= closes).any()) or bool((closes[:-1] > opens[1:]).any())):
        _fail("FROZEN_TIER_SOURCE_AXIS_INVALID")
    for row in rows:
        for key, recorded in (("entry_half_spread", "recorded_entry_points"),
                              ("intrabar_half_spread", "recorded_intrabar_points")):
            points = row[recorded]
            if points is not None and (type(points) not in (int, float) or not math.isfinite(points)
                                       or points < 0 or points != math.floor(points)):
                _fail("RECORDED_SPREAD_POINTS_INVALID")
            expected = max(floor, floor if points is None else float(points) * spec.point / 2.0)
            if row[key] != expected:
                _fail("FROZEN_TIER_SOURCE_QUOTE_MISMATCH")
    spread_meta = account["engine_spread_model"]
    if spread_meta.get("frozen_spread_tier") != tier:
        _fail("FROZEN_TIER_ENGINE_COST_DESCRIPTOR_CHANGED")
    attestation = Backtester(PipelineConfig(use_spread=True))._numerical_execution_cost_attestation(
        entry_half_spread=np.asarray([row["entry_half_spread"] for row in rows], dtype=np.float64),
        intrabar_half_spread=np.asarray([row["intrabar_half_spread"] for row in rows], dtype=np.float64),
        spread_meta=spread_meta, spec=spec)
    if attestation != account["numerical_execution_cost_attestation"]:
        _fail("FROZEN_TIER_ENGINE_COST_ATTESTATION_CHANGED")
    positions, transactions = [], []
    multiplier = spec.tick_value / spec.tick_size
    for pid, trade in zip(position_ids, trades, strict=True):
        entry, exit_bar = trade["entry_bar"], trade["exit_bar"]
        if (type(entry) is not int or type(exit_bar) is not int
                or not account["warmup_bars"] <= entry <= exit_bar < len(rows)):
            _fail("FROZEN_TIER_TRADE_AXIS_INVALID")
        side = trade["direction"]
        sign = 1 if side == "LONG" else -1 if side == "SHORT" else 0
        lots = float(trade["position_size"])
        if not sign or not math.isfinite(lots) or not spec.min_lot <= lots <= spec.max_lot:
            _fail("BACKTEST_POSITION_SIZE_INVALID")
        entry_price, exit_price = float(trade["entry_price"]), float(trade["exit_price"])
        expected_entry = rows[entry]["open"] + sign * rows[entry]["entry_half_spread"]
        if entry_price != expected_entry or pd.Timestamp(trade["entry_time"]) != opens[entry]:
            _fail("BACKTEST_ENTRY_FILL_RECONSTRUCTION_FAILED", pid)
        stop = float(trade["stop_loss"])
        gross = spec.calculate_tick_profit(entry_price, exit_price, lots, sign)
        eod = str(trade["exit_reason"]).lower() in _EOD_EXIT_REASONS
        open_exit = rows[exit_bar]["open"] - sign * rows[exit_bar]["intrabar_half_spread"]
        reason = str(trade["exit_reason"]).lower()
        if eod:
            if exit_bar != len(rows) - 1 or exit_price != rows[exit_bar]["close"] - sign * rows[exit_bar]["intrabar_half_spread"]:
                _fail("BACKTEST_TERMINAL_FILL_RECONSTRUCTION_FAILED", pid)
            exit_phase = "TERMINAL_AFTER_CLOSE_ROLLOVER"
        elif ("sl" in reason or "stop" in reason) and exit_price == open_exit and (
                (sign == 1 and open_exit <= stop) or (sign == -1 and open_exit >= stop)):
            exit_phase = "BAR_OPEN"
        elif ("sl" in reason or "stop" in reason) and exit_price == stop and (
                (sign == 1 and rows[exit_bar]["low"] - rows[exit_bar]["intrabar_half_spread"] <= stop)
                or (sign == -1 and rows[exit_bar]["high"] + rows[exit_bar]["intrabar_half_spread"] >= stop)):
            exit_phase = "INTRABAR_ORDER_ONLY"
        elif ("tp" in reason or "target" in reason) and exit_price == float(trade["take_profit"]) and (
                (sign == 1 and rows[exit_bar]["high"] - rows[exit_bar]["intrabar_half_spread"] >= exit_price)
                or (sign == -1 and rows[exit_bar]["low"] + rows[exit_bar]["intrabar_half_spread"] <= exit_price)):
            exit_phase = "INTRABAR_ORDER_ONLY"
        else:
            _fail("BACKTEST_EXIT_FILL_NOT_RECONSTRUCTIBLE", pid)
        if pd.Timestamp(trade["exit_time"]) != opens[exit_bar]:
            _fail("BACKTEST_EXIT_TIME_AXIS_INVALID", pid)
        if (sign == 1 and stop >= rows[entry]["open"]) or (sign == -1 and stop <= rows[entry]["open"]):
            _fail("BACKTEST_OPENING_STOP_NOT_ADVERSE", pid)
        rate = float((long_rates if sign == 1 else short_rates)[entry])
        swap_events, units, swap_cash = [], 0, 0.0
        for index in range(entry, exit_bar + 1):
            for phase, count in (("PRE_OPEN", pre[index] if index > entry else 0),
                                 ("BAR_CLOSE", close[index] if index < exit_bar or eod else 0)):
                if count:
                    cash = rate * lots * count
                    stamp = opens[index] if phase == "PRE_OPEN" else closes[index]
                    event = {"bar": index, "phase": phase, "effective_at_utc": stamp.isoformat(),
                             "units": count, "rate_per_lot": rate, "cash": cash, "position_id": pid}
                    swap_events.append(event)
                    transactions.append({**event, "kind": "SWAP"})
                    units += count
                    swap_cash += cash
        if (units != trade["swap_rollover_units"] or round(swap_cash, 2) != trade["swap_dollars"]
                or round(gross, 2) != trade["gross_pnl_dollars"]
                or round(gross + swap_cash, 2) != trade["pnl_dollars"]):
            _fail("BACKTEST_POSITION_CASH_RECONCILIATION_FAILED", pid)
        transactions.append({"bar": exit_bar, "phase": exit_phase,
                             "effective_at_utc": (opens[exit_bar] if exit_phase == "BAR_OPEN" else closes[exit_bar]).isoformat(),
                             "cash": gross, "position_id": pid, "kind": "REALIZED_GROSS"})
        positions.append({"position_id": pid, "entry_bar": entry, "exit_bar": exit_bar,
                          "side": side, "lots": lots, "entry_fill": entry_price, "exit_fill": exit_price,
                          "stop_price": stop, "exit_phase": exit_phase,
                          "gross_stop_risk_per_lot": abs(entry_price - stop) * multiplier,
                          "margin_per_lot": abs(entry_price) * multiplier,
                          "margin_amount": abs(entry_price) * multiplier * lots,
                          "margin_model": MARGIN_MODEL, "financing_events": swap_events})
    phase_order = {"PRE_OPEN": 0, "BAR_OPEN": 1, "INTRABAR_ORDER_ONLY": 1,
                   "BAR_CLOSE": 2, "TERMINAL_AFTER_CLOSE_ROLLOVER": 3}
    transactions.sort(key=lambda event: (event["bar"], phase_order[event["phase"]]))
    cash = float(account["initial_cash"])
    cash_rows = []
    transactions_by_bar = {}
    for transaction in transactions:
        transactions_by_bar.setdefault(transaction["bar"], []).append(transaction)
    for index, row in enumerate(rows):
        for transaction in transactions_by_bar.get(index, ()):
            cash += transaction["cash"]
        inventory = [position for position in positions if position["entry_bar"] <= index < position["exit_bar"]]
        if len(inventory) > 1:
            _fail("FROZEN_TIER_ENGINE_INVENTORY_OVERLAP")
        marked = cash
        for position in inventory:
            sign = 1 if position["side"] == "LONG" else -1
            quote = row["close"] - sign * row["intrabar_half_spread"]
            marked += spec.calculate_tick_profit(position["entry_fill"], quote, position["lots"], sign)
        if index >= account["warmup_bars"]:
            observed = row["engine_equity"]
            if not math.isfinite(observed) or marked != observed:
                _fail("FROZEN_TIER_INTERIOR_EQUITY_RECONCILIATION_FAILED", index, marked, observed)
            else:
                state = "DEPLETED" if observed <= 0 else "AVAILABLE"
            cash_rows.append({"bar": index, "effective_at_utc": row["close_utc"],
                              "engine_recording_phase": "AFTER_INTRABAR_EXITS_AND_CLOSE_ROLLOVER",
                              "cash_basis": "DERIVED_FROM_ACTUAL_FILLS_AND_MODEL_EVENTS", "cash": cash,
                              "inventory": [{"position_id": p["position_id"], "lots": p["lots"]} for p in inventory],
                              "derived_equity": marked, "engine_equity": observed, "state": state})
    close_frame = pd.DataFrame({"_BAR_CLOSE_TS": closes}, index=opens)
    growth = build_backtest_equity_growth(close_frame, [row["engine_equity"] for row in rows],
        initial_equity=account["initial_cash"], warmup_bars=account["warmup_bars"])
    if _raw_json(growth) != account["engine_equity_growth"]:
        _fail("FROZEN_TIER_ENGINE_GROWTH_RECONCILIATION_FAILED")
    return {"positions": positions, "transactions": transactions, "cash_inventory": cash_rows,
            "terminal_inventory": [], "terminal_simulation": "END_OF_DATA_LIQUIDATED",
            "reconciliation": "EXACT_FLOAT64_AT_EVERY_AVAILABLE_ENGINE_CLOSE"}


def _spread_source_observations(observations: pd.Series, matched_closes: pd.Series) -> list[dict[str, Any]]:
    if not observations.index.equals(matched_closes.index):
        _fail("FROZEN_TIER_ACCOUNT_OBSERVATION_AXIS_SPLICE")
    return [{"target_open_utc": pd.Timestamp(stamp).isoformat(),
             "expected_prior_fine_close_utc": pd.Timestamp(stamp).isoformat(),
             "prior_fine_close_utc": None if pd.isna(matched_closes.iloc[index])
                 else pd.Timestamp(matched_closes.iloc[index]).isoformat(),
             "points": None if pd.isna(value) else float(value)}
            for index, (stamp, value) in enumerate(observations.items())]


def _tier_source_rows(features, entry_half, intrabar_half, spread_meta, curve):
    opens, closes = _frame_interval(features)
    source_rows = []
    for index in range(len(features)):
        def recorded(column):
            value = features[column].iloc[index] if column in features else None
            return None if value is None or pd.isna(value) else float(value)
        source_rows.append({"open_utc": opens[index].isoformat(), "close_utc": closes[index].isoformat(),
                            "open": float(features["Open"].iloc[index]), "close": float(features["Close"].iloc[index]),
                            "high": float(features["High"].iloc[index]), "low": float(features["Low"].iloc[index]),
                            "recorded_entry_points": recorded(spread_meta["entry_column"]),
                            "recorded_intrabar_points": recorded(spread_meta["bar_column"]),
                            "entry_half_spread": float(entry_half[index]), "intrabar_half_spread": float(intrabar_half[index]),
                            "engine_equity": None if not np.isfinite(curve[index]) else float(curve[index])})
    return source_rows


def _position_evidence(*, specialist: _VerifiedSpecialist, strategy: Any,
                       features: pd.DataFrame, spec: InstrumentSpec, config: PipelineConfig,
                       result: Mapping[str, Any], tier: spread_profile.FrozenSpreadTier,
                       warmup_bars: int, valuation_tape: _M5ValuationTape) -> dict[str, Any]:
    backtester = Backtester(config)
    entry_half, intrabar_half, spread_meta = backtester._build_half_spread_arrays(
        features, spec, spread_tier=tier, timeframe=specialist.timeframe)
    trades = result.get("trades")
    if type(trades) is not list or type(result.get("position_events")) is not list:
        _fail("BACKTEST_POSITION_EVIDENCE_MISSING", specialist.cell_id)
    admission = result.get("spread_admission")
    model = result.get("spread_model")
    if (type(admission) is not dict or type(model) is not dict
            or admission.get("schema_version") != spread_profile.ADMISSION_SCHEMA
            or admission.get("entry_admission_policy") != spread_profile.ADMISSION_POLICY
            or model.get("entry_admission_schema") != spread_profile.ADMISSION_SCHEMA
            or model.get("entry_admission_policy") != spread_profile.ADMISSION_POLICY
            or admission.get("missing_executed_legs") != 0):
        _fail("FROZEN_TIER_CURRENT_ADMISSION_POLICY_REQUIRED")
    entry_prices = np.full(len(features), np.nan)
    for trade in trades:
        entry_prices[int(trade["entry_bar"])] = trade["entry_price"]
    pre, close, long_rates, short_rates, enabled, swap_contract = backtester._build_causal_swap_inputs(
        features, spec, entry_prices, timeframe=specialist.timeframe)
    if _raw_json(swap_contract) != _raw_json(result.get("swap_accrual_contract")):
        _fail("FROZEN_TIER_FINANCING_MODEL_CHANGED_AFTER_ENGINE")
    if not enabled and swap_contract.get("status") != "DISABLED":
        _fail("FROZEN_TIER_FINANCING_TIMING_UNAVAILABLE")
    curve = result.get("equity_curve")
    if curve is None or len(curve) != len(features):
        _fail("FROZEN_TIER_ENGINE_EQUITY_AXIS_MISSING", "source_rows", len(features),
              "exported_curve_type", type(curve).__name__,
              "exported_curve_rows", None if curve is None else len(curve),
              "exported_trade_count", len(trades),
              "reduced_raw_mark_count", result.get("equity_growth", {}).get("raw_mark_count"))
    source_rows = _tier_source_rows(features, entry_half, intrabar_half, spread_meta, curve)
    account = {"symbol": specialist.symbol, "timeframe": specialist.timeframe, "cell_id": specialist.cell_id,
               "tier": tier.descriptor(), "initial_cash": float(config.initial_capital), "warmup_bars": warmup_bars,
               "instrument_spec": asdict(spec), "position_namespace": "|".join((specialist.symbol, specialist.timeframe,
                   str(getattr(strategy, "name", type(strategy).__name__)))),
               "trades": _raw_json(trades), "position_events": _raw_json(result["position_events"]),
               "source_rows": source_rows, "engine_equity_growth": _raw_json(result.get("equity_growth")),
               "engine_spread_admission": _raw_json(result.get("spread_admission")),
               "engine_spread_model": _raw_json(result.get("spread_model")),
               "execution_cost_timing_contract": _raw_json(result.get("execution_cost_timing_contract")),
               "numerical_execution_cost_attestation": _raw_json(result.get("numerical_execution_cost_attestation")),
               "financing_model": {"pre_open_units": _raw_json(pre), "close_units": _raw_json(close),
                   "long_per_lot": _raw_json(long_rates), "short_per_lot": _raw_json(short_rates),
                   "enabled": bool(enabled), "contract": _raw_json(swap_contract)}}
    account["derived"] = _reconcile_tier_account(account)
    entry_column = valuation_tape.spread_meta["spread_model"]["entry_column"]
    account["valuation_rows"] = [
        {"open_utc": stamp.isoformat(), "open": float(valuation_tape.frame["Open"].iloc[index]),
         "recorded_entry_points": (None if entry_column not in valuation_tape.frame or pd.isna(valuation_tape.frame[entry_column].iloc[index])
                                   else float(valuation_tape.frame[entry_column].iloc[index]))}
        for index, stamp in enumerate(valuation_tape.opens)
    ]
    account["m5_marks"] = _tier_m5_marks(account, valuation_tape)
    account["valuation_source"] = {"native_product_identity": dict(valuation_tape.native_product_identity),
                                    "execution_spec_receipt": dict(valuation_tape.execution_spec_receipt),
                                    "spread_and_cost_surface": dict(valuation_tape.spread_meta)}
    return account


def _compact_tier_account(account: Mapping[str, Any]) -> dict[str, Any]:
    view_fields = ("source_rows", "valuation_rows", "m5_marks")
    if (type(account) is not dict or "engine_equity" in account
            or any(type(account.get(key)) is not list for key in view_fields)
            or not account["source_rows"]):
        _fail("FROZEN_TIER_COMPACT_ACCOUNT_INPUT_INVALID")
    curve = []
    for row in account["source_rows"]:
        value = row.get("engine_equity") if type(row) is dict else None
        valid = type(value) in (int, float)
        if valid:
            try:
                valid = math.isfinite(value)
            except OverflowError:
                valid = False
        if not valid:
            _fail("FROZEN_TIER_ENGINE_EQUITY_MARK_UNAVAILABLE")
        curve.append(value)
    compact = {key: value for key, value in account.items() if key not in view_fields}
    compact["engine_equity"] = curve
    return compact


def _tier_m5_marks(account: Mapping[str, Any], tape: _M5ValuationTape) -> list[dict[str, Any]]:
    if tape.symbol != account["symbol"] or tape.execution_spec_receipt.get("instrument_spec") != account["instrument_spec"]:
        _fail("M5_VALUATION_EXECUTION_SPEC_SPLICE")
    spec = InstrumentSpec(**account["instrument_spec"])
    positions, transactions = account["derived"]["positions"], account["derived"]["transactions"]
    rows = account["source_rows"]
    marks = []
    for index, stamp in enumerate(tape.opens):
        if stamp < pd.Timestamp(rows[account["warmup_bars"]]["open_utc"]):
            continue
        mid, bid, ask = _quote(tape.frame["Open"].iloc[index], tape.entry_half_spread[index])
        unknown = any(p["exit_phase"] == "INTRABAR_ORDER_ONLY"
                      and pd.Timestamp(rows[p["exit_bar"]]["open_utc"]) <= stamp < pd.Timestamp(rows[p["exit_bar"]]["close_utc"])
                      for p in positions)
        cash = float(account["initial_cash"])
        for transaction in transactions:
            if pd.Timestamp(transaction["effective_at_utc"]) <= stamp:
                cash += transaction["cash"]
        inventory = [p for p in positions if pd.Timestamp(rows[p["entry_bar"]]["open_utc"]) < stamp
                     and (pd.Timestamp(rows[p["exit_bar"]]["close_utc"])
                          if p["exit_phase"] != "BAR_OPEN" else pd.Timestamp(rows[p["exit_bar"]]["open_utc"])) > stamp]
        value = cash
        for position in inventory:
            sign = 1 if position["side"] == "LONG" else -1
            quote = float(bid if sign == 1 else ask)
            value += spec.calculate_tick_profit(position["entry_fill"], quote, position["lots"], sign)
        marks.append({"effective_at_utc": stamp.isoformat(), "mid": float(mid), "bid": float(bid), "ask": float(ask),
                      "half_spread": float(tape.entry_half_spread[index]),
                      "cash_basis": "DERIVED", "cash": None if unknown else cash,
                      "inventory": None if unknown else [{"position_id": p["position_id"], "lots": p["lots"]} for p in inventory],
                      "equity": None if unknown else value,
                      "state": "UNAVAILABLE_INTRABAR_EXIT_TIME" if unknown else "AVAILABLE",
                      "mark_phase": "M5_OPEN_AFTER_EXISTING_CASH_EVENTS_BEFORE_NEW_ENTRY"})
    return marks


def _execute_anchor_replay(
    *, context: authority_spool.GenerationSpoolContext, anchor: Mapping[str, Any],
    specialists: Sequence[_VerifiedSpecialist],
    native_products: Mapping[str, native_prefix.NativePrefixProduct],
    valuation_products: Mapping[str, native_prefix.NativePrefixProduct],
    precommit_anchor: Mapping[str, Any], capital_phase: str,
    capital_window: Mapping[str, Any], config: PipelineConfig, deployment: _DeploymentBinding,
    account_genesis: Mapping[str, Any] | None = None,
    cell_projection: Mapping[str, Any] | None = None,
    spread_source_products: Mapping[str, native_prefix.NativePrefixProduct] | None = None,
    frozen_profile_path: Path | None = None,
    full_book_body: Mapping[str, Any] | None = None,
    frozen_pipeline_config: config_binding.FrozenProductionPipelineConfig | None = None,
    owner_symbol_receipt: Mapping[str, Any] | None = None,
    original_adapter_index_descriptor: Mapping[str, Any] | None = None,
    original_global_precommit_descriptor: Mapping[str, Any] | None = None,
) -> _AnchorRawReplay:
    """Execute one fixed owner in independently funded, new R&D tier accounts.

    The retired public entry points remain closed. These internals cannot
    continue an existing account or qualify a strategy/cell.
    """
    if (type(account_genesis) is not dict or account_genesis != {
            "mode": NEW_SIMULATED_FLAT_GENESIS, "initial_cash": float(config.initial_capital),
            "initial_inventory": [], "owner_changes": []}):
        _fail("FROZEN_TIER_ACCOUNT_GENESIS_OR_CARRY_UNAVAILABLE")
    if (type(cell_projection) is not dict or set(cell_projection) != {"cell_id", "process_sha256"}
            or len(specialists) != 1):
        _fail("FROZEN_TIER_FIXED_SINGLE_CELL_PROJECTION_REQUIRED")
    owner = specialists[0]
    if (cell_projection != {"cell_id": owner.cell_id, "process_sha256": owner.process_sha256}
            or owner.anchor_id != anchor.get("anchor_id")):
        _fail("FROZEN_TIER_OWNER_PROJECTION_MISMATCH")
    if config.backtest_spread_mode != "conservative_max" or not config.use_spread:
        _fail("FROZEN_TIER_ACCOUNT_COST_POLICY_REQUIRED")
    if frozen_profile_path is None or type(spread_source_products) is not dict:
        _fail("FROZEN_TIER_PROFILE_SOURCE_UNAVAILABLE")
    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_UNAVAILABLE")
    try:
        bound_config = frozen_pipeline_config.materialize(context)
    except config_binding.ProductionConfigBindingError as exc:
        raise ForwardOOSReplayProductionError("FROZEN_TIER_ORIGINAL_PIPELINE_CONFIG_INVALID", (str(exc),)) from exc
    if _pipeline_config_identity(bound_config) != _pipeline_config_identity(config):
        _fail("FROZEN_TIER_PIPELINE_CONFIG_SUBSTITUTED")
    _assert_deployment_config_unchanged(deployment, bound_config)
    _reopen_original_global_precommit(context=context, descriptor=original_global_precommit_descriptor,
                                      precommit_anchor=precommit_anchor, deployment=deployment)
    config = bound_config
    # Keep the full original Book/precommit; projection never rewrites either.
    descriptor = precommit_anchor.get("full_book_descriptor")
    if type(descriptor) is not dict or precommit_anchor.get("full_book_sha256") != descriptor.get("sha256"):
        _fail("FROZEN_TIER_ORIGINAL_BOOK_BINDING_UNAVAILABLE")
    original_book = _read_descriptor_json(context, descriptor, code="FROZEN_TIER_ORIGINAL_BOOK")
    original_book = _owned_book(original_book, original_book.get("receipt_sha256"))
    if full_book_body is not None and original_book != full_book_body:
        _fail("FROZEN_TIER_ORIGINAL_BOOK_SUBSTITUTED")
    book_cells = original_book.get("cells")
    matches = [value for value in book_cells if type(value) is dict and value.get("cell_id") == owner.cell_id] if type(book_cells) is list else []
    if len(matches) != 1 or matches[0].get("local_process_sha256") != owner.process_sha256:
        _fail("FROZEN_TIER_ORIGINAL_BOOK_OWNER_MISMATCH")
    owner_symbol_receipt = owner_symbol_receipt or owner.origin_symbol_receipt
    original_adapter_index_descriptor = original_adapter_index_descriptor or owner.origin_adapter_index_descriptor
    original_owner = _reopen_owner_capture(context=context, anchor=anchor, book_body=original_book, cell_id=owner.cell_id,
        symbol_receipt=owner_symbol_receipt, adapter_index_descriptor=original_adapter_index_descriptor)
    if (_owner_provenance(original_owner) != _owner_provenance(owner)
            or original_owner.detector_record != owner.detector_record
            or original_owner.detector_parameters != owner.detector_parameters
            or original_owner.process_sha256 != owner.process_sha256):
        _fail("FROZEN_TIER_ORIGINAL_OWNER_SUBSTITUTED")
    owner = original_owner
    start, end = _capital_window_datetimes(capital_window, phase_id=_capital_phase_id(owner.anchor_id, capital_phase))
    if (precommit_anchor.get("anchor_id") != owner.anchor_id
            or _original_capital_window(anchor, precommit_anchor, capital_phase) != capital_window):
        _fail("FORWARD_PRECOMMIT_ANCHOR_IDENTITY_INVALID")
    def identities(field, supplied):
        values = precommit_anchor.get(field)
        roster_key = {"execution_products": "execution_product_roster_sha256",
                      "valuation_m5_products": "valuation_m5_product_roster_sha256",
                      "spread_source_products": "spread_source_product_roster_sha256"}[field]
        if type(values) is not list or precommit_anchor.get(roster_key) != _sha(values):
            _fail("FORWARD_PRECOMMIT_PRODUCT_ROSTER_INVALID", field)
        result = {str(value.get("role")).rsplit(":", 1)[-1]: value for value in values if type(value) is dict}
        if type(supplied) is not dict or len(result) != len(values) or set(result) != set(supplied):
            _fail("ANCHOR_NATIVE_PRODUCT_ROSTER_MISMATCH", field)
        return result
    execution_ids = identities("execution_products", native_products)
    valuation_ids = identities("valuation_m5_products", valuation_products)
    source_ids = identities("spread_source_products", spread_source_products)
    all_identities = (*execution_ids.values(), *valuation_ids.values(), *source_ids.values())
    if (sum(int(value["prefix_rows"]) for value in all_identities) > _MAX_NATIVE_ROWS_PER_ANCHOR
            or sum(int(value["prefix_bytes"]) for value in all_identities) > _MAX_NATIVE_BYTES_PER_ANCHOR):
        _fail("FROZEN_TIER_NATIVE_RESOURCE_BOUND_EXCEEDED")
    key = native_product_key(owner.symbol, owner.timeframe)
    valuation_key = native_product_key(owner.symbol, "M5")
    if key not in execution_ids or valuation_key not in valuation_ids:
        _fail("FROZEN_TIER_REQUIRED_NATIVE_PRODUCT_UNAVAILABLE")
    product = _reopen_bound_native_product(context=context, product=native_products[key], expected_identity=execution_ids[key],
                                         config=config, production_contract_required=not config.research_mode)
    spec, spec_receipt = _freeze_execution_spec(product=product, config=config, deployment=deployment)
    try:
        profile = spread_profile.load_spread_profile(frozen_profile_path)
        tiers = {level: profile.resolve(owner.symbol, owner.timeframe, level, point=spec.point,
                                       source_identity_sha256=execution_ids[key]["source_identity_sha256"])
                 for level in ("P75", "P95")}
    except (spread_profile.SpreadProfileError, OSError, ValueError) as exc:
        raise ForwardOOSReplayProductionError("FROZEN_TIER_PROFILE_UNAVAILABLE", (str(exc),)) from exc
    descriptor_tier = tiers["P75"].descriptor()
    source_key = native_product_key(owner.symbol, tiers["P75"].source_timeframe)
    if source_key not in source_ids:
        _fail("FROZEN_TIER_CAPTURED_FINE_TIMEFRAME_UNAVAILABLE", source_key)
    source_identity = source_ids[source_key]
    binding = descriptor_tier["source_binding"]
    if (source_identity["source_bars_sha256"] != binding["bars_sha256"]
            or source_identity["source_audit_sidecar_sha256"] != binding["sidecar_sha256"]
            or source_identity["source_provenance_sha256"] != binding["provenance_sha256"]
            or execution_ids[key]["source_bars_sha256"] != binding["target_bars_sha256"]
            or source_identity["activation_id"] != descriptor_tier["activation"]["id"]
            or source_identity["activation_manifest_sha256"] != descriptor_tier["activation"]["manifest_sha256"]
            or source_identity["source_identity_sha256"] != execution_ids[key]["source_identity_sha256"]):
        _fail("FROZEN_TIER_PROFILE_SOURCE_BINDING_MISMATCH")
    fine_product = _reopen_bound_native_product(context=context, product=spread_source_products[source_key],
        expected_identity=source_identity, config=config, production_contract_required=not config.research_mode)
    fine_spec, fine_receipt = _freeze_execution_spec(product=fine_product, config=config, deployment=deployment)
    if asdict(fine_spec) != asdict(spec):
        _fail("FROZEN_TIER_PROFILE_SOURCE_TERMS_MISMATCH")
    opened = native_prefix.open_verified_prefix_frame(product)
    _, closes = _frame_interval(opened)
    frame = opened.loc[closes <= end].copy(deep=True)
    _selection_prefix_match(frame, owner)
    features, signals = _features_and_prefix_parity(frame, [owner])
    if int(features.memory_usage(index=True, deep=True).sum()) > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
        _fail("FORWARD_ACTIVE_SYMBOL_FEATURE_MEMORY_BOUND_EXCEEDED")
    opens, _ = _frame_interval(features)
    warmup = int(opens.searchsorted(start, side="left"))
    if not 1 <= warmup < len(features):
        _fail("FORWARD_SCORING_WINDOW_HAS_NO_NATIVE_BARS")
    fine_frame = native_prefix.open_verified_prefix_frame(fine_product)
    observations, matched_closes = spread_profile.align_prior_completed_spreads(
        features.index, fine_frame, tiers["P75"].source_timeframe, include_matched_closes=True)
    strategy = _strategy(owner)
    accounts = {}
    for level, tier in tiers.items():
        tape, valuation_spec = _open_m5_valuation_tape(context=context, product=valuation_products[valuation_key],
            expected_identity=valuation_ids[valuation_key], config=config, deployment=deployment,
            forward_start=start, forward_end_exclusive=end, spread_tier=tier)
        if asdict(valuation_spec) != asdict(spec):
            _fail("EXECUTION_AND_M5_VALUATION_SPEC_MISMATCH")
        result = Backtester(config).run(features, signals[owner.cell_id], owner.symbol, strategy, spec=spec,
            timeframe=owner.timeframe, warmup_bars=warmup, spread_tier=tier, entry_spread_observations=observations)
        accounts[level] = _compact_tier_account(_position_evidence(
            specialist=owner, strategy=strategy, features=features,
            spec=spec, config=config, result=result, tier=tier, warmup_bars=warmup, valuation_tape=tape))
    profile_descriptor = _persist_frozen_profile(context, frozen_profile_path, profile.sha256)
    body = {"schema_version": FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
            "generation_id": context.generation_id, "anchor": dict(anchor), "capital_phase": capital_phase,
            "capital_window": dict(capital_window),
            "genesis": dict(account_genesis), "full_book_descriptor": dict(descriptor),
            "original_precommit_anchor": dict(precommit_anchor), "cell_projection": dict(cell_projection),
            "owner_provenance": _owner_provenance(owner),
            "owner_source": {"symbol_receipt": dict(owner_symbol_receipt),
                             "adapter_index_descriptor": dict(original_adapter_index_descriptor)},
            "original_global_precommit_descriptor": dict(original_global_precommit_descriptor),
            "pipeline_config_binding": {"active_config_path": str(frozen_pipeline_config.active_config_path),
                "config_sha256": frozen_pipeline_config.config_sha256,
                "effective_config_sha256": frozen_pipeline_config.effective_config_sha256},
            "deployment_source_descriptors": _persist_deployment_sources(context, deployment),
            "execution_source": execution_ids[key], "execution_spec_receipt": spec_receipt,
            "frozen_profile_descriptor": profile_descriptor,
            "spread_source": source_identity, "spread_source_execution_spec_receipt": fine_receipt,
            "accounts": accounts, "qualification_authority": False,
            "precommit_binding_scope": "REOPENED_INPUT_DESCRIPTOR_IDENTITY_NOT_ISSUED_RELEASE_AUTHORITY",
            "release_authority_status": "UNAVAILABLE_AUTHORITY_MIGRATION_NOT_IMPLEMENTED",
            "coverage_contract": "ACTUAL_CLOSE_AND_AVAILABLE_M5_MARKS_NOT_COMPLETE_DAILY_VALUATION",
            "selection_policy": "P75_REQUIRED_P95_PREFERRED_AMONG_P75_PASSES"}
    return _AnchorRawReplay(dict(anchor), (owner,), body, {"schema_version": FROZEN_TIER_RAW_ACCOUNTS_SCHEMA,
        "qualification_authority": False, "source_observations_internally_derived": True})


def _persist_frozen_profile(context, path, expected_sha256):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        _fail("FROZEN_TIER_PROFILE_CHANGED_AFTER_RESOLVE")
    return _descriptor(authority_spool.write_opaque_artifact(context, (raw,), format="json-profile"),
                       "FROZEN_TIER_PROFILE_DESCRIPTOR_INVALID")


def _persist_deployment_sources(context, deployment):
    result = {}
    for role, path, expected in (("config", deployment.config_path, deployment.config_sha256),
                                 ("broker", deployment.broker_path, deployment.broker_sha256)):
        raw = Path(path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            _fail("FROZEN_TIER_PRECOMMITTED_DEPLOYMENT_SOURCE_CHANGED", role)
        result[role] = _descriptor(authority_spool.write_opaque_artifact(context, (raw,), format="json-" + role),
                                  "FROZEN_TIER_DEPLOYMENT_SOURCE_DESCRIPTOR_INVALID")
    return result


_DECLARED_CELL_MAX_RAW_BYTES = 48 * 1024 * 1024
_DECLARED_CELL_FIELDS = frozenset({
    "schema_version", "generation_id", "generation_context_sha256", "native_universe_sha256",
    "config_sha256", "declared_precommit_sha256", "search_process_receipt_sha256", "cell",
    "pipeline_config_binding", "declared_commit_artifacts", "slot_summaries", "slot_schedule",
    "progress_status", "reason_code", "measurement", "genesis", "owner_records",
    "execution_source", "spread_source", "execution_spec_receipt",
    "spread_source_execution_spec_receipt", "frozen_profile_descriptor",
    "deployment_source_descriptors", "original_config_identity", "measured_config_identity",
    "account", "qualification_authority", "capital_authority", "live_authority", "order_authority",
})
_DECLARED_CELL_MARKET_FIELDS = (
    "execution_source", "spread_source", "execution_spec_receipt",
    "spread_source_execution_spec_receipt", "frozen_profile_descriptor",
    "deployment_source_descriptors", "original_config_identity", "measured_config_identity",
)


def _declared_cell_measured_config(config):
    if (type(config) is not PipelineConfig or config.initial_capital != 10000.0
            or config.discovery_risk_per_trade_pct != 1.0
            or config.backtest_spread_mode != "conservative_max" or config.use_spread is not True):
        _fail("DECLARED_CELL_ORIGINAL_CONFIG_INVALID")
    measured = replace(config, discovery_risk_per_trade_pct=0.25)
    for name in ("_deployment_config_path", "_funded_live_execution"):
        if hasattr(config, name):
            setattr(measured, name, getattr(config, name))
    return measured


def _run_declared_cell_account(*, config, features, symbol, timeframe, spec, warmup_bars,
                               entry_schedule, entry_spread_observations):
    """One account, including positions and debt carried across every refit."""
    return Backtester(config).run(features, None, symbol, None, spec=spec,
        timeframe=timeframe, warmup_bars=warmup_bars, entry_schedule=entry_schedule,
        entry_spread_observations=entry_spread_observations, terminal_policy="MARK_OPEN")


def _declared_cell_measurement(parent):
    slots = parent["slot_roster"]["slots"]
    if type(slots) is not list or len(slots) != 13:
        _fail("DECLARED_CELL_H364_GEOMETRY_INVALID")
    previous = None
    for ordinal, slot in enumerate(slots, 1):
        start = _timestamp(slot["activation_utc"], "DECLARED_CELL_SLOT_TIME_INVALID")
        end = _timestamp(slot["end_exclusive_utc"], "DECLARED_CELL_SLOT_TIME_INVALID")
        if (slot["ordinal"] != ordinal or end - start != pd.Timedelta(days=28)
                or (previous is not None and start != previous)):
            _fail("DECLARED_CELL_H364_GEOMETRY_INVALID")
        previous = end
    return {"calendar_days": 364, "refit_cycles": 13, "cycle_days": 28,
        "risk_per_trade_pct": 0.25, "terminal_policy": "MARK_OPEN",
        "start_utc": slots[0]["activation_utc"],
        "end_exclusive_utc": slots[-1]["end_exclusive_utc"]}


def _reopen_declared_cell_inputs(context, *, precommit, process, frozen_pipeline_config):
    import pm_historical_search_producer as producer

    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("DECLARED_CELL_ORIGINAL_CONFIG_REQUIRED")
    try:
        parent = historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
        config = frozen_pipeline_config.materialize(context)
        _declared_cell_measured_config(config)
        _declared_cell_measurement(parent)
        cell = parent["cells"][0]
        progress = producer.reopen_declared_cell_owner_progress(context, precommit=precommit,
            process=process, cell_id=cell["cell_id"], frozen_pipeline_config=frozen_pipeline_config)
    except ForwardOOSReplayProductionError:
        raise
    except Exception as exc:
        raise ForwardOOSReplayProductionError("DECLARED_CELL_OWNER_PROGRESS_INVALID",
            (type(exc).__name__, getattr(exc, "code", ""))) from exc
    if len(progress.slot_commit_artifacts) != len(progress.slot_summaries):
        _fail("DECLARED_CELL_COMMIT_ROSTER_INVALID")
    commits, records, schedule = [], [], []
    for summary, entry in zip(progress.slot_summaries, progress.slot_commit_artifacts, strict=True):
        commit = _read_descriptor_json(context, entry["descriptor"], code="DECLARED_CELL_COMMIT")
        if (entry["descriptor"]["sha256"] != summary["commit_sha256"]
                or commit["cell_id"] != cell["cell_id"] or commit["slot_id"] != summary["slot_id"]
                or commit["ordinal"] != summary["ordinal"]
                or commit["completion"]["status"] != summary["status"]):
            _fail("DECLARED_CELL_COMMIT_BINDING_MISMATCH")
        completion = commit["completion"]
        if completion["status"] == "OWNER":
            owner = completion["owner"]
            cross = _read_descriptor_json(context, completion["horizon_winner_cross_binding"],
                code="DECLARED_CELL_OWNER_CROSS_BINDING")
            selected = owner["selected_attempt"]
            matches = [row for row in cross["mapping"]
                if row.get("winner_process_candidate_id") == selected["process_candidate_id"]
                and row.get("process_sha256") == selected["process_sha256"]]
            if len(matches) != 1 or type(matches[0].get("source_count_receipt")) is not dict:
                _fail("DECLARED_CELL_OWNER_SOURCE_COUNT_INVALID")
            count = matches[0]["source_count_receipt"]
            if count.get("receipt_sha256") != _sha({k: v for k, v in count.items() if k != "receipt_sha256"}):
                _fail("DECLARED_CELL_OWNER_SOURCE_COUNT_INVALID")
            records.append({"owner": owner, "source_count_receipt": count})
        commits.append(commit)
    by_slot = {commit["slot_id"]: commit["completion"] for commit in commits}
    for slot in parent["slot_roster"]["slots"]:
        completion = by_slot.get(slot["slot_id"])
        schedule.append({"slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "start_utc": slot["activation_utc"], "end_exclusive_utc": slot["end_exclusive_utc"],
            "status": "MISSING" if completion is None else completion["status"],
            "owner_id": completion["owner"]["owner_id"]
                if completion is not None and completion["status"] == "OWNER" else None})
    status = "STOP" if progress.terminal_stop else "INCOMPLETE" if (
        progress.first_uncommitted_slot is not None or len(commits) != 13
        or any(row["status"] == "MISSING" for row in schedule)) else "COMPLETE"
    if status == "COMPLETE" and any(row["status"] not in ("OWNER", "NO_TRADE") for row in schedule):
        _fail("DECLARED_CELL_COMPLETED_SCHEDULE_INVALID")
    return {"parent": parent, "config": config, "progress": progress, "commits": commits,
        "owner_records": records, "slot_schedule": schedule, "progress_status": status,
        "reason_code": None if status == "COMPLETE" else "DECLARED_CELL_OWNER_PREFIX_" + status}


def _declared_cell_transport_symbol(body):
    identity = {"schema_version": DECLARED_CELL_RAW_ACCOUNT_SCHEMA,
        "precommit_sha256": body["declared_precommit_sha256"], "cell_id": body["cell"]["cell_id"],
        "commit_sha256s": [row["commit_sha256"] for row in body["slot_summaries"]],
        "progress_status": body["progress_status"]}
    return "DECLARED_CELL_ACCOUNT:" + _sha(identity)


@dataclass(frozen=True, slots=True)
class _DeclaredCellSpecialist:
    anchor_id: str
    cell_id: str
    symbol: str
    timeframe: str
    regime: str
    strategy_id: str
    strategy_class: str
    strategy_module: str
    parameters: Mapping[str, Any]
    source_count_receipt: Mapping[str, Any]
    detector_parameters: Mapping[str, Any]
    detector_parameters_sha256: str
    detector_record: Mapping[str, Any]


def _declared_cell_specialist(record, cell):
    owner, count = record["owner"], record["source_count_receipt"]
    process = owner["selected_artifacts"]["winner_process"]
    detector = owner["regime_detector_record"]
    parameters = json.loads(detector["parameters_canonical_json"])
    if (process.get("strategy_id") != owner["selected_attempt"]["strategy_id"]
            or _sha(process) != owner["selected_attempt"]["process_sha256"]
            or detector.get("parameters_sha256") != _sha(parameters)
            or owner["cell_id"] != cell["cell_id"]
            or any(count.get(k) != cell[k] for k in ("symbol", "timeframe", "regime"))):
        _fail("DECLARED_CELL_OWNER_MATERIALIZATION_INVALID")
    return _DeclaredCellSpecialist(owner["slot_id"], cell["cell_id"], cell["symbol"],
        cell["timeframe"], cell["regime"], process["strategy_id"], process["strategy_class"],
        process["strategy_module"], owner["selected_attempt"]["encoded_parameters"], count,
        parameters, detector["parameters_sha256"], detector)


def _declared_cell_native_frames(*, inputs, execution_frame, fine_frame, profile, spec,
                                 execution_identity, spread_identity):
    cell, measurement = inputs["parent"]["cells"][0], _declared_cell_measurement(inputs["parent"])
    start = _timestamp(measurement["start_utc"], "DECLARED_CELL_WINDOW_INVALID")
    end = _timestamp(measurement["end_exclusive_utc"], "DECLARED_CELL_WINDOW_INVALID")
    _, closes = _frame_interval(execution_frame)
    retained_closes = closes[closes <= end]
    if len(retained_closes) == 0 or retained_closes[-1] != end:
        _fail("DECLARED_CELL_TERMINAL_BOUNDARY_UNAVAILABLE")
    frame = execution_frame.loc[closes <= end].copy(deep=True)
    opens, _ = _frame_interval(frame)
    warmup = int(opens.searchsorted(start, side="left"))
    if not 1 <= warmup < len(frame):
        _fail("DECLARED_CELL_SCORING_WINDOW_EMPTY")
    tiers = {level: profile.resolve(cell["symbol"], cell["timeframe"], level, point=spec.point,
        source_identity_sha256=execution_identity["source_identity_sha256"]) for level in ("P75", "P95")}
    descriptor = tiers["P75"].descriptor()
    binding = descriptor["source_binding"]
    if (spread_identity["timeframe"] != tiers["P75"].source_timeframe
            or spread_identity["source_bars_sha256"] != binding["bars_sha256"]
            or spread_identity["source_audit_sidecar_sha256"] != binding["sidecar_sha256"]
            or spread_identity["source_provenance_sha256"] != binding["provenance_sha256"]
            or execution_identity["source_bars_sha256"] != binding["target_bars_sha256"]
            or spread_identity["activation_id"] != descriptor["activation"]["id"]
            or spread_identity["activation_manifest_sha256"] != descriptor["activation"]["manifest_sha256"]
            or spread_identity["source_identity_sha256"] != execution_identity["source_identity_sha256"]):
        _fail("DECLARED_CELL_PROFILE_SOURCE_BINDING_MISMATCH")
    owners = []
    for record in inputs["owner_records"]:
        owner = record["owner"]
        specialist = _declared_cell_specialist(record, cell)
        _selection_prefix_match(frame, specialist)
        own_features, own_signals = _features_and_prefix_parity(frame, [specialist])
        tier = profile.from_descriptor(owner["selected_tier_contract"]["frozen_spread_tier"], symbol=cell["symbol"],
            timeframe=cell["timeframe"], point=spec.point,
            source_identity_sha256=execution_identity["source_identity_sha256"])
        if tier.tier != owner["selected_tier"]:
            _fail("DECLARED_CELL_OWNER_TIER_BINDING_MISMATCH")
        owners.append(ContinuousEntryOwner(owner["owner_id"], _strategy(specialist),
            own_features, own_signals[cell["cell_id"]], tier))
    features = owners[0].features if owners else frame
    feature_bytes = sum(int(owner.features.memory_usage(index=True, deep=True).sum()) for owner in owners)
    if max(feature_bytes, int(features.memory_usage(index=True, deep=True).sum())) > _MAX_ACTIVE_SYMBOL_NATIVE_BYTES:
        _fail("FORWARD_ACTIVE_SYMBOL_FEATURE_MEMORY_BOUND_EXCEEDED")
    observations, matched_closes = spread_profile.align_prior_completed_spreads(
        features.index, fine_frame, tiers["P75"].source_timeframe, include_matched_closes=True)
    schedule = ContinuousEntrySchedule(tuple(owners), tuple(ContinuousEntrySlot(
        row["slot_id"], _timestamp(row["start_utc"], "DECLARED_CELL_SLOT_TIME_INVALID"),
        _timestamp(row["end_exclusive_utc"], "DECLARED_CELL_SLOT_TIME_INVALID"), row["owner_id"])
        for row in inputs["slot_schedule"]))
    return features, tuple(owners), schedule, observations, matched_closes, warmup


def _declared_cell_financing(result, *, frame, spec, config, timeframe):
    prices = np.full(len(frame), np.nan)
    for trade in result["trades"]:
        prices[int(trade["entry_bar"])] = trade["entry_price"]
    position = result["terminal"]["open_position"]
    if position is not None:
        prices[int(position["entry_bar"])] = position["entry_price"]
    pre, close, long_rates, short_rates, enabled, contract = Backtester(config)._build_causal_swap_inputs(
        frame, spec, prices, timeframe=timeframe)
    if (_raw_json(contract) != result["swap_accrual_contract"]
            or (config.swap_model_enabled and not enabled)):
        _fail("DECLARED_CELL_FINANCING_BINDING_MISMATCH")
    return {"pre_open_units": _raw_json(pre), "close_units": _raw_json(close),
        "long_per_lot": _raw_json(long_rates), "short_per_lot": _raw_json(short_rates),
        "enabled": bool(enabled), "contract": _raw_json(contract)}


def _declared_cell_owner_input_bindings(owners):
    return [{"owner_id": owner.owner_id, "features_sha256": _frame_sha256(owner.features),
        "signals_sha256": hashlib.sha256(pd.util.hash_pandas_object(
            owner.signals, index=True).values.tobytes()).hexdigest()} for owner in owners]


def _declared_account_payload(*, result, frame, owner_inputs, spec, config, observations,
                              matched_closes, timeframe, warmup_bars):
    result = _raw_json(result)
    return {"engine_result": result, "source_frame_sha256": _frame_sha256(frame),
        "warmup_bars": warmup_bars, "owner_input_bindings": _declared_cell_owner_input_bindings(owner_inputs),
        "entry_spread_observations": _spread_source_observations(observations, matched_closes),
        "financing_model": _declared_cell_financing(result, frame=frame, spec=spec,
            config=config, timeframe=timeframe)}


def _declared_cell_close(actual, expected, code):
    if (type(actual) not in (int, float) or not math.isfinite(float(actual))
            or abs(float(actual) - float(expected)) > max(1e-8, 64 * math.ulp(max(abs(float(expected)), 1.0)))):
        _fail(code)


def _declared_cell_stop_prices(owner, *, entry_bar, entry_price, direction, symbol, timeframe, spec):
    strategy, features, signal_bar = owner.strategy, owner.features, entry_bar - 1
    try:
        if callable(getattr(strategy, "build_trade_intent", None)):
            regime_column = "REGIME_LIVE" if "REGIME_LIVE" in features else "REGIME" if "REGIME" in features else None
            intent = strategy.build_trade_intent(features, symbol=symbol, timeframe=timeframe,
                regime="" if regime_column is None else str(features[regime_column].iloc[signal_bar]),
                signal=direction, spec=spec, bar_index=signal_bar, apply_regime_tp_multiplier=False)
            stop_pips, target_pips = float(intent.stop_loss_pips), float(intent.take_profit_pips)
        else:
            stop_pips, target_pips = strategy.calculate_stops(features, direction, symbol,
                spec=spec, bar_index=signal_bar)
    except TypeError:
        stop_pips, target_pips = strategy.calculate_stops(features.iloc[:signal_bar + 1].copy(), direction, symbol)
    if (not math.isfinite(stop_pips) or not math.isfinite(target_pips)
            or stop_pips <= 0 or target_pips <= 0):
        _fail("DECLARED_CELL_POSITION_STOPS_INVALID")
    return (entry_price - direction * spec.pips_to_price(stop_pips),
        entry_price + direction * spec.pips_to_price(target_pips))


def _declared_cell_exit_at(frame, bar, *, direction, half_spread, stop, target):
    open_quote = float(frame["Open"].iloc[bar]) - direction * half_spread
    adverse = float(frame["Low" if direction == 1 else "High"].iloc[bar]) - direction * half_spread
    favourable = float(frame["High" if direction == 1 else "Low"].iloc[bar]) - direction * half_spread
    if direction * (open_quote - stop) <= 0:
        return open_quote, "closed_sl", True
    if direction * (adverse - stop) <= 0:
        return stop, "closed_sl", False
    if direction * (favourable - target) >= 0:
        return target, "closed_tp", False
    return None


def _reconcile_declared_cell_account(account, *, frame, owner_inputs, spec, config,
                                     observations, matched_closes, timeframe, schedule):
    expected_fields = {"engine_result", "source_frame_sha256", "warmup_bars", "owner_input_bindings",
        "entry_spread_observations", "financing_model"}
    if type(account) is not dict or set(account) != expected_fields:
        _fail("DECLARED_CELL_ACCOUNT_FIELDS_INVALID")
    result = account["engine_result"]
    result_fields = {"schema_version", "terminal_policy", "cash", "final_equity", "net_account_profit",
        "equity_curve", "max_drawdown_pct", "total_trades", "trades", "closed_trade_statistics_basis",
        "closed_trade_gross_profit", "terminal", "first_depletion_bar", "swap_accrual_contract",
        "qualification_authority", "tier_entry_counts", "spread_admission"}
    if (type(result) is not dict or set(result) != result_fields
            or result.get("schema_version") != "fxpm.continuous-account-result.v1"
            or result.get("terminal_policy") != "MARK_OPEN" or result.get("qualification_authority") is not False
            or result.get("closed_trade_statistics_basis") != "CLOSED_TRADES_ONLY_EXCLUDES_OPEN_INVENTORY"
            or account["source_frame_sha256"] != _frame_sha256(frame)
            or account["owner_input_bindings"] != _declared_cell_owner_input_bindings(owner_inputs)
            or account["entry_spread_observations"] != _spread_source_observations(
                observations, matched_closes)):
        _fail("DECLARED_CELL_ACCOUNT_SOURCE_BINDING_MISMATCH")
    n = len(frame)
    warmup = account["warmup_bars"]
    if type(warmup) is not int or not 1 <= warmup < n:
        _fail("DECLARED_CELL_ACCOUNT_WARMUP_INVALID")
    curve, trades, terminal = result.get("equity_curve"), result.get("trades"), result.get("terminal")
    terminal_fields = {"schema_version", "policy", "valuation_status", "cash", "marked_equity", "asof_bar",
        "asof_open", "asof_close", "bid", "ask", "unrealized_gross", "open_position", "first_depletion_bar"}
    if (type(curve) is not list or len(curve) != n or type(trades) is not list
            or len(trades) > _MAX_CANDIDATE_POSITIONS_PER_ANCHOR or result.get("total_trades") != len(trades)
            or type(terminal) is not dict or set(terminal) != terminal_fields
            or terminal.get("schema_version") != "fxpm.continuous-account-terminal.v1"
            or terminal.get("policy") != "MARK_OPEN" or terminal.get("asof_bar") != n - 1
            or terminal.get("asof_open") != _raw_json(frame.index[-1])
            or terminal.get("asof_close") != _raw_json(frame["_BAR_CLOSE_TS"].iloc[-1])):
        _fail("DECLARED_CELL_ACCOUNT_TERMINAL_OR_AXIS_INVALID")
    financing = _declared_cell_financing(result, frame=frame, spec=spec, config=config, timeframe=timeframe)
    if account["financing_model"] != financing:
        _fail("DECLARED_CELL_FINANCING_BINDING_MISMATCH")
    owners = {owner.owner_id: owner for owner in owner_inputs}
    slots = {slot.slot_id: slot for slot in schedule.slots}
    bt = Backtester(config)
    spreads = {}
    for owner_id, owner in owners.items():
        entry, bar, _ = bt._build_half_spread_arrays(frame, spec, spread_tier=owner.spread_tier, timeframe=timeframe)
        states, _ = bt._entry_spread_admission_inputs(frame, owner.spread_tier, observations)
        spreads[owner_id] = (entry, bar, states)
    tier_counts = {owner.spread_tier.tier: {"admitted": 0, "refused": 0, "missing": 0}
        for owner in owner_inputs}
    counters = {"known_cap_refused_opportunities": 0, "known_cap_pass_opportunities": 0,
        "missing_refused_opportunities": 0, "no_trade_scored_bar_count": 0}
    entry_owners = [None] * n
    for slot in schedule.slots:
        mask = (frame.index >= slot.start_utc) & (frame.index < slot.end_utc)
        for i in np.flatnonzero(mask & (np.arange(n) >= warmup)):
            if slot.owner_id is None:
                counters["no_trade_scored_bar_count"] += 1
                continue
            owner = owners.get(slot.owner_id)
            if owner is None:
                _fail("DECLARED_CELL_POSITION_OWNER_OR_SLOT_INVALID")
            entry_owners[i] = owner
            if owner.signals.iloc[i - 1] == 0:
                continue
            state = spreads[owner.owner_id][2][i]
            key, counter = {"ADMIT": ("admitted", "known_cap_pass_opportunities"),
                "REFUSE": ("refused", "known_cap_refused_opportunities"),
                "MISSING": ("missing", "missing_refused_opportunities")}[state]
            tier_counts[owner.spread_tier.tier][key] += 1
            counters[counter] += 1
    if result.get("tier_entry_counts") != tier_counts or result.get("spread_admission") != counters:
        _fail("DECLARED_CELL_SPREAD_ADMISSION_BINDING_MISMATCH")
    gross_gap_events, ordinary_exit_events = np.zeros(n), np.zeros(n)
    pre_open_swap_events, close_swap_events = np.zeros(n), np.zeros(n)
    entry_counts, gap_exit_counts = np.zeros(n, dtype=np.int32), np.zeros(n, dtype=np.int32)
    inventory = np.zeros(n, dtype=np.int32)
    unrealized = np.zeros(n)
    positions = [(trade, False) for trade in trades]
    position = terminal.get("open_position")
    if position is not None:
        if type(position) is not dict:
            _fail("DECLARED_CELL_OPEN_POSITION_INVALID")
        positions.append((position, True))
    previous_exit, previous_gap = -1, False
    risk_joins = []
    for leg, is_open in positions:
        owner = owners.get(leg.get("owner_id"))
        slot = slots.get(leg.get("slot_id"))
        entry_bar, signal_bar = leg.get("entry_bar"), leg.get("signal_bar")
        exit_bar = n - 1 if is_open else leg.get("exit_bar")
        direction = leg.get("direction") if is_open else {"LONG": 1, "SHORT": -1}.get(leg.get("direction"))
        if (owner is None or slot is None or slot.owner_id != owner.owner_id
                or type(entry_bar) is not int or type(exit_bar) is not int or type(signal_bar) is not int
                or not warmup <= entry_bar <= exit_bar < n or signal_bar != entry_bar - 1
                or direction not in (-1, 1) or entry_bar < previous_exit
                or (entry_bar == previous_exit and not previous_gap)
                or not slot.start_utc <= frame.index[entry_bar] < slot.end_utc
                or int(owner.signals.iloc[signal_bar]) != direction
                or (leg.get("tier") if is_open else leg.get("frozen_spread_tier")) != owner.spread_tier.descriptor()
                or spreads[owner.owner_id][2][entry_bar] != "ADMIT"):
            _fail("DECLARED_CELL_POSITION_OWNER_OR_SLOT_INVALID")
        previous_exit = exit_bar
        entry_counts[entry_bar] += 1
        entry_price = float(leg["entry_price"])
        lots = float(leg["lots"] if is_open else leg["position_size"])
        if (not math.isfinite(lots) or lots <= 0 or spec.round_volume(lots) != lots
                or not math.isfinite(entry_price)):
            _fail("DECLARED_CELL_POSITION_SIZE_INVALID")
        expected_entry = float(frame["Open"].iloc[entry_bar]) + direction * spreads[owner.owner_id][0][entry_bar]
        _declared_cell_close(entry_price, expected_entry, "DECLARED_CELL_ENTRY_COST_INVALID")
        stop, target = _declared_cell_stop_prices(owner, entry_bar=entry_bar, entry_price=entry_price,
            direction=direction, symbol=owner.spread_tier.symbol, timeframe=timeframe, spec=spec)
        _declared_cell_close(leg.get("stop_loss"), stop, "DECLARED_CELL_POSITION_STOPS_INVALID")
        _declared_cell_close(leg.get("take_profit"), target, "DECLARED_CELL_POSITION_STOPS_INVALID")
        time_field = "entry_utc_open" if is_open else "entry_time"
        if leg.get(time_field) != _raw_json(frame.index[entry_bar]):
            _fail("DECLARED_CELL_POSITION_TIME_INVALID")
        rates = financing["long_per_lot"] if direction == 1 else financing["short_per_lot"]
        rate = float(rates[entry_bar]) if financing["enabled"] else 0.0
        last_close = exit_bar + 1 if is_open else exit_bar
        accrued_swap, units = 0.0, 0
        for i in range(entry_bar, exit_bar + 1):
            pre_units = financing["pre_open_units"][i] if i > entry_bar else 0
            if pre_units > 0:
                flow = rate * lots * pre_units
                accrued_swap += flow
                units += pre_units
                pre_open_swap_events[i] += flow
            close_units = financing["close_units"][i] if i < last_close else 0
            if close_units > 0:
                flow = rate * lots * close_units
                accrued_swap += flow
                units += close_units
                close_swap_events[i] += flow
        if (leg.get("posted_swap_units") if is_open else leg.get("swap_rollover_units")) != units:
            _fail("DECLARED_CELL_POSITION_FINANCING_INVALID")
        for i in range(entry_bar, exit_bar + 1):
            exit_at = _declared_cell_exit_at(frame, i, direction=direction,
                half_spread=spreads[owner.owner_id][1][i], stop=stop, target=target)
            if (i < exit_bar or is_open) and exit_at is not None:
                _fail("DECLARED_CELL_POSITION_EXIT_TIMING_INVALID")
        if is_open:
            _declared_cell_close(leg["posted_swap_cash"], accrued_swap, "DECLARED_CELL_POSITION_FINANCING_INVALID")
            _declared_cell_close(leg["latched_rate_per_lot"], rate, "DECLARED_CELL_POSITION_FINANCING_INVALID")
        else:
            if leg.get("exit_reason") in _EOD_EXIT_REASONS or str(leg.get("exit_reason", "")).lower() in _EOD_EXIT_REASONS:
                _fail("DECLARED_CELL_FORCED_TERMINAL_EXIT_INVALID")
            gross = spec.calculate_tick_profit(entry_price, float(leg["exit_price"]), lots, direction)
            if exit_at is None or str(leg.get("exit_reason", "")).lower() != exit_at[1]:
                _fail("DECLARED_CELL_EXIT_COST_INVALID")
            _declared_cell_close(leg.get("exit_price"), exit_at[0], "DECLARED_CELL_EXIT_COST_INVALID")
            previous_gap = bool(exit_at[2] and exit_bar > entry_bar)
            if previous_gap:
                gross_gap_events[exit_bar] += gross
                gap_exit_counts[exit_bar] += 1
            else:
                ordinary_exit_events[exit_bar] += gross
            if (leg.get("gross_pnl_dollars") != round(gross, 2)
                    or leg.get("swap_dollars") != round(accrued_swap, 2)
                    or leg.get("pnl_dollars") != round(gross + accrued_swap, 2)
                    or leg.get("exit_time") != _raw_json(frame.index[exit_bar])):
                _fail("DECLARED_CELL_CLOSED_TRADE_ACCOUNTING_INVALID")
        risk_joins.append((leg, is_open, entry_bar, lots, direction))
        for i in range(entry_bar, last_close):
            inventory[i] += 1
            quote = float(frame["Close"].iloc[i]) - direction * spreads[owner.owner_id][1][i]
            unrealized[i] += spec.calculate_tick_profit(entry_price, quote, lots, direction)
    if bool((inventory > 1).any()):
        _fail("DECLARED_CELL_OVERLAPPING_POSITIONS_INVALID")
    cash_curve, entry_cash = np.empty(n), np.empty(n)
    cash = float(config.initial_capital)
    for i in range(n):
        # Match the kernel's binary64 cash additions, not a reduced event sum.
        cash += float(pre_open_swap_events[i])
        cash += float(gross_gap_events[i])
        entry_cash[i] = cash
        cash += float(ordinary_exit_events[i])
        cash += float(close_swap_events[i])
        cash_curve[i] = cash
    for leg, is_open, entry_bar, lots, direction in risk_joins:
        requested = float(entry_cash[entry_bar]) * (config.discovery_risk_per_trade_pct / 100.0)
        loss_per_lot = spec.calculate_loss_at_stop(leg["entry_price"], leg["stop_loss"], 1.0, direction)
        if loss_per_lot <= 0 or lots != spec.round_volume(requested / loss_per_lot):
            _fail("DECLARED_CELL_RISK_SIZING_INVALID")
        if is_open:
            _declared_cell_close(leg.get("requested_risk_amount"), requested, "DECLARED_CELL_RISK_SIZING_INVALID")
        elif leg.get("risk_amount") != round(requested, 2):
            _fail("DECLARED_CELL_RISK_SIZING_INVALID")
    for i in range(warmup, n):
        # An intrabar exit cannot enable a replacement at that bar's open.
        flat_at_entry = inventory[i - 1] == 0 or gap_exit_counts[i] == 1
        owner = entry_owners[i]
        expected_entry_count = 0
        if flat_at_entry and owner is not None:
            direction = int(owner.signals.iloc[i - 1])
            if direction != 0 and spreads[owner.owner_id][2][i] == "ADMIT":
                entry_price = float(frame["Open"].iloc[i]) + direction * spreads[owner.owner_id][0][i]
                stop, _ = _declared_cell_stop_prices(owner, entry_bar=i, entry_price=entry_price,
                    direction=direction, symbol=owner.spread_tier.symbol, timeframe=timeframe, spec=spec)
                loss_per_lot = spec.calculate_loss_at_stop(entry_price, stop, 1.0, direction)
                cash_at_entry = float(entry_cash[i])
                requested = cash_at_entry * (config.discovery_risk_per_trade_pct / 100.0)
                expected_entry_count = int(loss_per_lot > 0 and spec.round_volume(requested / loss_per_lot) > 0)
        if entry_counts[i] != expected_entry_count:
            _fail("DECLARED_CELL_ENTRY_COMPLETENESS_INVALID")
    expected_curve = cash_curve + unrealized
    for actual, expected in zip(curve, expected_curve, strict=True):
        _declared_cell_close(actual, expected, "DECLARED_CELL_EQUITY_RECONCILIATION_FAILED")
    _declared_cell_close(result.get("cash"), cash_curve[-1], "DECLARED_CELL_CASH_RECONCILIATION_FAILED")
    _declared_cell_close(terminal.get("cash"), cash_curve[-1], "DECLARED_CELL_CASH_RECONCILIATION_FAILED")
    _declared_cell_close(result.get("final_equity"), expected_curve[-1], "DECLARED_CELL_TERMINAL_RECONCILIATION_FAILED")
    _declared_cell_close(terminal.get("marked_equity"), expected_curve[-1], "DECLARED_CELL_TERMINAL_RECONCILIATION_FAILED")
    _declared_cell_close(terminal.get("unrealized_gross"), unrealized[-1], "DECLARED_CELL_TERMINAL_RECONCILIATION_FAILED")
    _declared_cell_close(result.get("net_account_profit"), expected_curve[-1] - config.initial_capital,
        "DECLARED_CELL_PROFIT_RECONCILIATION_FAILED")
    _declared_cell_close(result.get("closed_trade_gross_profit"), sum(t["gross_pnl_dollars"] for t in trades),
        "DECLARED_CELL_CLOSED_TRADE_ACCOUNTING_INVALID")
    depletion = next((i for i, value in enumerate(expected_curve) if value <= 0), None)
    if result.get("first_depletion_bar") != depletion or terminal.get("first_depletion_bar") != depletion:
        _fail("DECLARED_CELL_DEPLETION_RECONCILIATION_FAILED")
    peak, drawdown = float(config.initial_capital), 0.0
    for value in expected_curve:
        peak = max(peak, float(value))
        drawdown = max(drawdown, (peak - value) / peak * 100 if peak > 0 else 0.0)
    _declared_cell_close(result.get("max_drawdown_pct"), drawdown, "DECLARED_CELL_DRAWDOWN_RECONCILIATION_FAILED")
    if terminal.get("valuation_status") != ("MARKED_OPEN" if position is not None else "FLAT_CASH_ONLY"):
        _fail("DECLARED_CELL_TERMINAL_STATUS_INVALID")
    if position is None:
        if terminal.get("bid") is not None or terminal.get("ask") is not None:
            _fail("DECLARED_CELL_TERMINAL_QUOTE_INVALID")
    else:
        half = spreads[position["owner_id"]][1][-1]
        _declared_cell_close(terminal.get("bid"), float(frame["Close"].iloc[-1]) - half,
            "DECLARED_CELL_TERMINAL_QUOTE_INVALID")
        _declared_cell_close(terminal.get("ask"), float(frame["Close"].iloc[-1]) + half,
            "DECLARED_CELL_TERMINAL_QUOTE_INVALID")
        risk = abs(spec.calculate_tick_profit(position["entry_price"], position["stop_loss"],
            position["lots"], position["direction"]))
        _declared_cell_close(position["executed_stop_risk"], risk, "DECLARED_CELL_OPEN_RISK_INVALID")


def _declared_cell_body(context, *, precommit, process, frozen_pipeline_config, inputs):
    return {"schema_version": DECLARED_CELL_RAW_ACCOUNT_SCHEMA,
        "generation_id": context.generation_id, "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "declared_precommit_sha256": precommit.receipt_sha256,
        "search_process_receipt_sha256": process.receipt_sha256, "cell": inputs["parent"]["cells"][0],
        "pipeline_config_binding": {"active_config_path": str(frozen_pipeline_config.active_config_path),
            "config_sha256": frozen_pipeline_config.config_sha256,
            "effective_config_sha256": frozen_pipeline_config.effective_config_sha256},
        "declared_commit_artifacts": list(inputs["progress"].slot_commit_artifacts),
        "slot_summaries": list(inputs["progress"].slot_summaries),
        "slot_schedule": inputs["slot_schedule"], "progress_status": inputs["progress_status"],
        "reason_code": inputs["reason_code"], "measurement": _declared_cell_measurement(inputs["parent"]),
        "genesis": {"mode": NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0, "initial_inventory": []},
        "owner_records": inputs["owner_records"], **{name: None for name in _DECLARED_CELL_MARKET_FIELDS},
        "account": None, "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}


def produce_declared_cell_account_artifact(context, *, precommit, process, frozen_pipeline_config,
        execution_product=None, spread_source_product=None, frozen_profile_path=None):
    """Write a raw historical cell account, never a qualified trading decision."""
    inputs = _reopen_declared_cell_inputs(context, precommit=precommit, process=process,
        frozen_pipeline_config=frozen_pipeline_config)
    body = _declared_cell_body(context, precommit=precommit, process=process,
        frozen_pipeline_config=frozen_pipeline_config, inputs=inputs)
    if inputs["progress_status"] == "COMPLETE":
        if execution_product is None or spread_source_product is None or frozen_profile_path is None:
            _fail("DECLARED_CELL_NATIVE_INPUTS_REQUIRED")
        config = inputs["config"]
        measured = _declared_cell_measured_config(config)
        deployment = _deployment_config(config)
        cell, end = body["cell"], body["measurement"]["end_exclusive_utc"]
        execution_identity = _native_product_identity(execution_product, expected_symbol=cell["symbol"],
            expected_timeframe=cell["timeframe"], expected_cutoff_utc=end, role="DECLARED_CELL_EXECUTION")
        profile = spread_profile.load_spread_profile(frozen_profile_path)
        if profile.sha256 != inputs["parent"]["spread_profile_identity"]["profile_sha256"]:
            _fail("DECLARED_CELL_PROFILE_CHANGED")
        product = _reopen_bound_native_product(context=context, product=execution_product,
            expected_identity=execution_identity, config=config,
            production_contract_required=not config.research_mode, pin_consumed_activation=True)
        spec, execution_receipt = _freeze_execution_spec(product=product, config=config, deployment=deployment)
        source_tf = profile.resolve(cell["symbol"], cell["timeframe"], "P75", point=spec.point,
            source_identity_sha256=execution_identity["source_identity_sha256"]).source_timeframe
        source_identity = _native_product_identity(spread_source_product, expected_symbol=cell["symbol"],
            expected_timeframe=source_tf, expected_cutoff_utc=end, role="DECLARED_CELL_SPREAD")
        fine = _reopen_bound_native_product(context=context, product=spread_source_product,
            expected_identity=source_identity, config=config,
            production_contract_required=not config.research_mode, pin_consumed_activation=True)
        fine_spec, fine_receipt = _freeze_execution_spec(product=fine, config=config, deployment=deployment)
        if asdict(spec) != asdict(fine_spec):
            _fail("DECLARED_CELL_EXECUTION_SOURCE_TERMS_MISMATCH")
        frame, owners, schedule, observations, matched, warmup = _declared_cell_native_frames(
            inputs=inputs, execution_frame=native_prefix.open_verified_prefix_frame(product),
            fine_frame=native_prefix.open_verified_prefix_frame(fine), profile=profile, spec=spec,
            execution_identity=execution_identity, spread_identity=source_identity)
        # All external inputs are bound before entering the single continuous account.
        body.update({"execution_source": execution_identity, "spread_source": source_identity,
            "execution_spec_receipt": execution_receipt, "spread_source_execution_spec_receipt": fine_receipt,
            "frozen_profile_descriptor": _persist_frozen_profile(context, frozen_profile_path, profile.sha256),
            "deployment_source_descriptors": _persist_deployment_sources(context, deployment),
            "original_config_identity": _pipeline_config_identity(config),
            "measured_config_identity": _pipeline_config_identity(measured)})
        result = _raw_json(_run_declared_cell_account(config=measured, features=frame,
            symbol=cell["symbol"], timeframe=cell["timeframe"], spec=spec, warmup_bars=warmup,
            entry_schedule=schedule, entry_spread_observations=observations))
        account = _declared_account_payload(result=result, frame=frame, owner_inputs=owners,
            spec=spec, config=measured, observations=observations, matched_closes=matched,
            timeframe=cell["timeframe"], warmup_bars=warmup)
        _reconcile_declared_cell_account(account, frame=frame, owner_inputs=owners, spec=spec,
            config=measured, observations=observations, matched_closes=matched,
            timeframe=cell["timeframe"], schedule=schedule)
        body["account"] = account
        _assert_deployment_config_unchanged(deployment, config)
    _canonical_bytes(body, _DECLARED_CELL_MAX_RAW_BYTES)
    sink = authority_spool.open_symbol_sink(context, _declared_cell_transport_symbol(body),
        manifest_domain=authority_spool.GENERAL_MANIFEST_DOMAIN)
    entry = sink.put_json("declared-cell-account", "declared_cell_raw_account", body)
    sink.seal()
    return causal_producer.DeclaredCellRawAccountArtifact(dict(entry["descriptor"]))


def verify_declared_cell_account_transport(context, body, *, process, frozen_pipeline_config, reopen_source):
    if type(body) is not dict or set(body) != _DECLARED_CELL_FIELDS or body.get("schema_version") != DECLARED_CELL_RAW_ACCOUNT_SCHEMA:
        _fail("DECLARED_CELL_TRANSPORT_FIELDS_INVALID")
    try:
        precommit = historical.reopen_declared_slot_owner_run_precommit(context,
            receipt_sha256=body["declared_precommit_sha256"], process=process,
            frozen_pipeline_config=frozen_pipeline_config)
        inputs = _reopen_declared_cell_inputs(context, precommit=precommit, process=process,
            frozen_pipeline_config=frozen_pipeline_config)
        expected = _declared_cell_body(context, precommit=precommit, process=process,
            frozen_pipeline_config=frozen_pipeline_config, inputs=inputs)
        for name in _DECLARED_CELL_FIELDS - set(_DECLARED_CELL_MARKET_FIELDS) - {"account"}:
            if body[name] != expected[name]:
                _fail("DECLARED_CELL_TRANSPORT_BINDING_MISMATCH", name)
        if inputs["progress_status"] != "COMPLETE":
            if body["account"] is not None or any(body[name] is not None for name in _DECLARED_CELL_MARKET_FIELDS):
                _fail("DECLARED_CELL_INCOMPLETE_ACCOUNT_FORBIDDEN")
            return body
        if type(body["account"]) is not dict or not callable(reopen_source):
            _fail("DECLARED_CELL_COMPLETE_ACCOUNT_REQUIRED")
        config, cell = inputs["config"], body["cell"]
        measured = _declared_cell_measured_config(config)
        if (body["original_config_identity"] != _pipeline_config_identity(config)
                or body["measured_config_identity"] != _pipeline_config_identity(measured)):
            _fail("DECLARED_CELL_CONFIG_DERIVATION_CHANGED")
        deployment = _deployment_config(config)
        sources = body["deployment_source_descriptors"]
        if type(sources) is not dict or set(sources) != {"config", "broker"}:
            _fail("DECLARED_CELL_DEPLOYMENT_SOURCES_INVALID")
        for role, digest, size in (("config", deployment.config_sha256, deployment.config_size_bytes),
                ("broker", deployment.broker_sha256, deployment.broker_size_bytes)):
            descriptor = _descriptor(sources[role], "DECLARED_CELL_DEPLOYMENT_DESCRIPTOR_INVALID")
            observed, actual_size, _ = authority_spool._stream_file(context.root, descriptor["relative_path"],
                maximum_bytes=size, expected_size=size, collect=False)
            if (descriptor["format"] != "json-" + role or descriptor["sha256"] != digest
                    or descriptor["size_bytes"] != size or observed != digest or actual_size != size):
                _fail("DECLARED_CELL_DEPLOYMENT_SOURCES_CHANGED")
        execution_identity, spread_identity = body["execution_source"], body["spread_source"]
        if (execution_identity["role"] != "DECLARED_CELL_EXECUTION"
                or spread_identity["role"] != "DECLARED_CELL_SPREAD"
                or execution_identity["symbol"] != cell["symbol"] or execution_identity["timeframe"] != cell["timeframe"]
                or spread_identity["symbol"] != cell["symbol"]
                or any(identity["cutoff_utc"] != body["measurement"]["end_exclusive_utc"]
                    for identity in (execution_identity, spread_identity))):
            _fail("DECLARED_CELL_NATIVE_WINDOW_INVALID")
        execution_frame, product = reopen_source(context, execution_identity, config)
        fine_frame, fine = reopen_source(context, spread_identity, config)
        spec, receipt = _freeze_execution_spec(product=product, config=config, deployment=deployment)
        fine_spec, fine_receipt = _freeze_execution_spec(product=fine, config=config, deployment=deployment)
        if (receipt != body["execution_spec_receipt"] or fine_receipt != body["spread_source_execution_spec_receipt"]
                or asdict(spec) != asdict(fine_spec)):
            _fail("DECLARED_CELL_EXECUTION_SPEC_CHANGED")
        profile_descriptor = _descriptor(body["frozen_profile_descriptor"], "DECLARED_CELL_PROFILE_DESCRIPTOR_INVALID")
        profile_sha = inputs["parent"]["spread_profile_identity"]["profile_sha256"]
        observed, size, _ = authority_spool._stream_file(context.root, profile_descriptor["relative_path"],
            maximum_bytes=_DECLARED_CELL_MAX_RAW_BYTES, expected_size=profile_descriptor["size_bytes"], collect=False)
        if (profile_descriptor["format"] != "json-profile" or profile_descriptor["sha256"] != profile_sha
                or observed != profile_sha or size != profile_descriptor["size_bytes"]):
            _fail("DECLARED_CELL_PROFILE_CHANGED")
        profile = spread_profile.load_spread_profile(Path(context.root) / profile_descriptor["relative_path"])
        frame, owners, schedule, observations, matched, warmup = _declared_cell_native_frames(
            inputs=inputs, execution_frame=execution_frame, fine_frame=fine_frame, profile=profile, spec=spec,
            execution_identity=execution_identity, spread_identity=spread_identity)
        if body["account"]["warmup_bars"] != warmup:
            _fail("DECLARED_CELL_ACCOUNT_WARMUP_INVALID")
        _reconcile_declared_cell_account(body["account"], frame=frame, owner_inputs=owners, spec=spec,
            config=measured, observations=observations, matched_closes=matched,
            timeframe=cell["timeframe"], schedule=schedule)
        return body
    except ForwardOOSReplayProductionError:
        raise
    except Exception as exc:
        raise ForwardOOSReplayProductionError("DECLARED_CELL_TRANSPORT_REVERIFY_FAILED",
            (type(exc).__name__, getattr(exc, "code", ""))) from exc


def _ledger_bodies(replay: _AnchorRawReplay, *, starting_balance: Decimal) -> dict[str, dict[str, Any]]:
    if type(replay) is not _AnchorRawReplay or float(starting_balance) != replay.raw_accounts["genesis"]["initial_cash"]:
        _fail("FROZEN_TIER_ACCOUNT_WRITER_INPUT_INVALID")
    return {"FROZEN_TIER_RAW_ACCOUNTS": dict(replay.raw_accounts)}




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

    # Archived verifiers remain available.  Replace this paired frozen-path
    # producer in place when the interval qualification policy is adopted.
    _fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")


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

    _fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")


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
    "DECLARED_CELL_RAW_ACCOUNT_SCHEMA",
    "native_product_key",
    "precommit_forward_oos_replay_products",
    "produce_forward_oos_replay_artifacts",
    "produce_declared_cell_account_artifact",
    "verify_declared_cell_account_transport",
    "verify_forward_oos_replay_product",
]
