"""Genuine native-prefix search execution for historical production authority.

This module owns the decision-time seam between released Horizon plans and the
raw Winner-v15 builder.  It accepts only a byte-reopened activated native
prefix, derives an anchor-local regime surface, authenticates the complete
current-refit attempt roster, and calls :class:`pm_core.Backtester` at the true
optimizer-train, tuning-validation, and final-lockbox sites.  No metric or
position event is reconstructed from aggregate scores.

The public lifecycle is deliberately narrow:

* :func:`fit_native_timeframe_for_search` opens one verified native cell and
  creates the exact source/regime decisions used by the historical Book.
* :func:`execute_historical_observed_cell` freezes the already released plan
  before its first strategy evaluation and streams genuine attempts/folds into
  the historical adapter.
* :func:`execute_current_observed_cell` repeats that exact execution process at
  the latest authorized cutoff while binding the sealed current cell plan.

Neither function grants causal, current-refit, capital, publication, demo, or
live authority by itself.
"""

from __future__ import annotations

import copy
import hashlib
import hmac
import io
import json
import math
import os
import pickle
import secrets
import stat
import tempfile
import threading
import time
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

import numpy as np
import pandas as pd

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_core
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as historical_adapter
import pm_native_prefix_production as native_prefix
import pm_regime
import pm_signal_opportunities as signal_opportunities
import pm_spread_profile as spread_profile
import pm_trade_power as trade_power
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner
from pm_core import (
    BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
    Backtester,
    FeatureComputer,
    InstrumentSpec,
    PipelineConfig,
    build_backtest_trade_identity_pnl_evidence,
)
from pm_current_refit_parameters import decode_parameter_mapping
from pm_search_outcomes import (
    SearchInfrastructureError,
    SearchOutcomeKind,
    SearchStage,
    TrialOutcome,
)
from pm_strategies import StrategyRegistry, resolve_strategy_parameters


UTC: Final = timezone.utc
EXECUTION_CONTRACT_VERSION: Final = "fxpm.authoritative-search-execution.v6"
REGIME_FIT_POLICY_VERSION: Final = "fxpm.historical-regime-fit-policy.v1"
REGIME_FIT_EVIDENCE_VERSION: Final = "fxpm.historical-regime-fit-evidence.v1"
PARTITION_GEOMETRY_VERSION: Final = "fxpm.winner-partition-geometry.v1"
SIGNAL_VECTOR_VERSION: Final = "fxpm.authoritative-search-signal-vector.v1"
PROVENANCE_RECEIPT_VERSION: Final = "fxpm.authoritative-search-provenance.v4"
STATE_CONTEXT_DISPOSITION_SCHEMA: Final = (
    "fxpm.production-trial-state-context-disposition.v1"
)
STATE_CONTEXT_BOUNDARY_REASON: Final = "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
SIGNAL_DESIGN_REQUEST_SCHEMA: Final = "fxpm.signal-opportunity-design-request.v1"
SIGNAL_DESIGN_TAPE_SCHEMA: Final = "fxpm.signal-opportunity-train-design-tape.v1"
SIGNAL_DESIGN_TAPE_FORMAT: Final = "fxpm-signal-opportunity-design-jsonl-v1"
_SIGNAL_DESIGN_REQUEST_SEAL = object()
MAX_PREFIX_ROWS: Final = 10_000_000
MAX_ATTEMPTS: Final = winner.MAX_ATTEMPTS
MAX_FOLDS: Final = winner.MAX_FOLDS
MIN_FOLDS: Final = int(winner.CURRENT_SELECTION_POLICY["min_causal_folds"])
MIN_DAYS_PER_FOLD: Final = int(winner.CURRENT_SELECTION_POLICY["min_days_per_fold"])
MAX_DAYS_PER_FOLD: Final = winner.MAX_DAYS_PER_FOLD
MIN_PARTITION_SCORED_BARS: Final = 2
MAX_ATTEMPT_EVIDENCE_BYTES: Final = 512 * 1024**2
MAX_CAPACITY_CUSTODY_BYTES: Final = 128 * 1024
CAPACITY_CUSTODY_SCHEMA: Final = "fxpm.authoritative-search-capacity-custody.v1"
MAX_DEPLOYMENT_CONFIG_BYTES: Final = 32 * 1024**2
MAX_BROKER_SPECS_BYTES: Final = 64 * 1024**2
EXECUTION_SPEC_RECEIPT_SCHEMA: Final = "fxpm.execution-instrument-spec-receipt.v2"
TERMINAL_SOURCE_CONTRACT_SCHEMA: Final = (
    "fxpm.terminal-source-non-evaluation-contract.v2"
)
TERMINAL_REGIME_FIT_POLICY_SCHEMA: Final = "fxpm.terminal-regime-fit-policy.v2"
TERMINAL_SOURCE_REASON_CODE: Final = "AUTHENTICATED_NATIVE_PREFIX_SOURCE_UNAVAILABLE"
TERMINAL_TECHNICAL_REASON_CODE: Final = "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
_SHA: Final = frozenset("0123456789abcdef")
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
_ATTEMPT_ARTIFACT_KEYS: Final = frozenset(
    {
        "winner_process",
        "horizon_process",
        "trial_definition",
        "refit_recipe",
    }
)
_RAW_LANE_KEYS: Final = (
    "trade_identity_pnl",
    "position_events",
    "winner_daily_r_surface",
    "trade_power_partition_axis",
    "execution_cost_timing_contract",
    "numerical_execution_cost_attestation",
    "swap_accrual_contract",
    "spread_model",
    "spread_admission",
)


class AuthoritativeSearchExecutionError(RuntimeError):
    """Typed mechanical failure; never an economic NO_TRADE verdict."""

    def __init__(self, code: str, details: Sequence[str] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


@dataclass(frozen=True, slots=True)
class FittedNativeSearchTimeframe:
    """One byte-verified native prefix plus its anchor-local regime fit."""

    product: native_prefix.NativePrefixProduct
    source_group_decision: book.HistoricalSourceGroupDecision
    regime_fit_decision: book.RegimeFitDecision
    source_contract_seed: Mapping[str, Any]
    cost_ladder_contract: Mapping[str, Any]
    regime_parameters: Mapping[str, Any]
    train_regime_support: Mapping[str, Any]
    instrument_spec: InstrumentSpec
    execution_spec_receipt: Mapping[str, Any]
    frame_sha256: str
    _frame: pd.DataFrame
    source_contract_seeds: Mapping[str, Any] | None = None
    frozen_tier_pair_contract: Mapping[str, Any] | None = None
    _spread_tiers: Mapping[str, spread_profile.FrozenSpreadTier] | None = None
    _entry_spread_observations: pd.Series | None = None
    _spread_source_product: native_prefix.NativePrefixProduct | None = None
    _spread_source_frame: pd.DataFrame | None = None
    _bound_fit_owner: Any = field(default=None, repr=False, compare=False)
    _derived_binding: Any = field(default=None, repr=False, compare=False)
    _numeric_preparation: Any = field(default=None, repr=False, compare=False)
    _numeric_key: Any = field(default=None, repr=False, compare=False)
    _numeric_source_columns: tuple[str, ...] = field(default=(), repr=False, compare=False)

    def frame_copy(self) -> pd.DataFrame:
        """Return an isolated copy; callers cannot mutate the bound surface."""

        return self._frame.copy(deep=True)


@dataclass(frozen=True, slots=True)
class BoundedRawWinnerCanarySourceSession:
    """Authenticated post-request prefix-open and fit result."""

    _body_json: bytes
    _product: native_prefix.NativePrefixProduct
    _fitted: FittedNativeSearchTimeframe
    _pipeline_config: PipelineConfig
    _frozen_pipeline_config: Any
    _spool_entry_json: bytes
    _issuance_tag: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def spool_entry(self) -> dict[str, Any]:
        return json.loads(self._spool_entry_json.decode("ascii"))


def _verify_bounded_canary_held_source_state(
    *,
    source_session: BoundedRawWinnerCanarySourceSession,
    source_body: Mapping[str, Any],
    current_config: PipelineConfig,
) -> None:
    """Cheaply bind the held canary source until the final full replay.

    The canary executor performs one authoritative native reopen/refit directly
    before evaluation.  Intermediate capability checks only need to prove that
    the exact in-memory source/config objects issued by this executor still
    match their sealed source receipt; recomputing the entire regime fit at
    every hand-off adds no decision evidence.
    """

    fitted = source_session._fitted
    product = source_session._product
    if (
        type(fitted) is not FittedNativeSearchTimeframe
        or fitted.product is not product
        or source_session._pipeline_config != current_config
    ):
        _fail("CANARY_SOURCE_PIPELINE_CONFIG_BINDING_INVALID")
    try:
        frame_sha256 = _frame_sha256(fitted._frame)
        native = product.native_prefix
        observed = (
            fitted.frame_sha256,
            frame_sha256,
            product.index_sha256,
            native.get("prefix_content_sha256"),
            fitted.source_group_decision.receipt_sha256,
            fitted.regime_fit_decision.receipt_sha256,
            fitted.train_regime_support.get("receipt_sha256"),
        )
    except AuthoritativeSearchExecutionError:
        raise
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "CANARY_SOURCE_SESSION_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    expected = (
        source_body.get("fitted_frame_sha256"),
        source_body.get("fitted_frame_sha256"),
        source_body.get("native_prefix_index_sha256"),
        source_body.get("native_prefix_content_sha256"),
        source_body.get("source_group_decision_sha256"),
        source_body.get("regime_fit_decision_sha256"),
        source_body.get("train_regime_support_receipt_sha256"),
    )
    if observed != expected:
        _fail("CANARY_SOURCE_SESSION_REPLAY_MISMATCH")


@dataclass(frozen=True, slots=True)
class _BoundedRawWinnerCanaryPlanPermit:
    _body_json: bytes
    _issuance_tag: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class _BoundedRawWinnerCanaryCapturePermit:
    _body_json: bytes
    _issuance_tag: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class TerminalNativeTimeframe:
    """A reverified terminal (non-searchable) native cell and its Book facts.

    Authenticated ``SOURCE_UNAVAILABLE`` and ``TECHNICAL_TERMINAL`` products
    yield distinct complete Book abstentions.  Ordinary ``TECHNICAL_FAILURE``
    never reaches this object because :func:`fit_terminal_native_timeframe`
    raises before construction.
    """

    product: native_prefix.NativePrefixProduct
    source_group_decision: book.HistoricalSourceGroupDecision
    regime_fit_decision: book.RegimeFitDecision
    terminal_source_contract: Mapping[str, Any]
    terminal_source_contract_sha256: str
    terminal_regime_fit_policy: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class HistoricalObservedCellExecution:
    status: str
    attempted_trials_exact: int
    production: raw_winner.WinnerRawEvidenceProduction | None
    failure_code: str | None


@dataclass(frozen=True, slots=True)
class CurrentObservedCellExecution:
    """One latest-cutoff observed cell after genuine raw Winner production.

    Boundary-limited cells return without raw production and remain statistical
    insufficiencies for the current Book.  Mechanical failures are raised and
    therefore cannot be persisted as market/NO_TRADE decisions.
    """

    status: str
    attempted_trials_exact: int
    production: raw_winner.WinnerRawEvidenceProduction | None
    cross_binding_bytes: bytes | None
    cross_binding_entry: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class SignalOpportunityDesignRequest:
    """A separate TRAIN-design grant; not an old planning/validation grant."""

    _body_json: bytes
    _entry_json: bytes
    _planning_request: Any
    _anchor_plan_receipt: Any
    _causal_precommit_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class SignalOpportunityDesignExecution:
    """Reference to one complete nonauthorising GENERAL research tape."""

    _body_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True, slots=True)
class _VerifiedAttempt:
    ordinal: int
    plan_attempt: Mapping[str, Any]
    candidate: Mapping[str, Any]
    trial: Mapping[str, Any]
    artifacts: current_refit.CurrentRefitSearchArtifacts


@dataclass(frozen=True, slots=True)
class _PartitionSlice:
    role: str
    frame: pd.DataFrame
    warmup_bars: int
    scored_start_utc: str
    end_exclusive_utc: str
    geometry_receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _FullPrefixStrategySurface:
    """One causal feature/signal replay reused by every partition of an attempt."""

    features: pd.DataFrame
    raw_signals: pd.Series
    _content_sha256: str | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class _StagedAttemptEvidence:
    ordinal: int
    kind: str
    path: Path | None
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _StageCapacityCustody:
    root: Path
    stage_identity: tuple[int, int]
    receipt_identity: tuple[int, int]
    receipt: _StagedAttemptEvidence
    binding: Mapping[str, Any]


def _fail(code: str, *details: Any) -> None:
    raise AuthoritativeSearchExecutionError(
        code,
        tuple(str(value) for value in details),
    )


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
        raise AuthoritativeSearchExecutionError("CANONICAL_JSON_INVALID") from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise AuthoritativeSearchExecutionError("RELEASE_SOURCE_UNAVAILABLE") from exc
    return digest.hexdigest()


def _hex64(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _SHA for character in value.lower())
    ):
        _fail(code)
    return value.lower()


def _utc(value: Any, code: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AuthoritativeSearchExecutionError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _book_canonical_utc(value: Any, code: str) -> str:
    """Bridge canonical native ``Z`` time to the Book's aware-UTC spelling."""

    normalized = _utc(value, code)
    return normalized[:-1] + "+00:00"


def _canary_native_prefix_matches_request(
    native: Mapping[str, Any],
    *,
    index: Mapping[str, Any],
    request_body: Mapping[str, Any],
    source_accessed_at_utc: str,
) -> bool:
    """Bind the prefix product to its request without requiring a bar at cutoff."""

    anchor = request_body.get("anchor")
    return bool(
        isinstance(anchor, Mapping)
        and native.get("source_accessed_at_utc") == source_accessed_at_utc
        and native.get("symbol") == request_body.get("symbol")
        and native.get("timeframe") == request_body.get("timeframe")
        # The authenticated index binds the exact producer input. The observed
        # prefix end can be earlier across a weekend or another closed session.
        and index.get("cutoff_utc") == anchor.get("selection_cutoff_utc")
        and native.get("activation_id") == request_body.get("activation_id")
        and native.get("activation_manifest_sha256")
        == request_body.get("activation_manifest_sha256")
    )


def _now_utc_seconds() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _next_observed_utc_second(after_utc: str) -> str:
    """Return an actually observed later second without invented timestamps."""

    after = datetime.fromisoformat(
        _utc(after_utc, "CLOCK_BOUNDARY_INVALID").replace("Z", "+00:00")
    )
    deadline = time.monotonic() + 2.0
    while True:
        observed = datetime.now(UTC).replace(microsecond=0)
        if observed > after:
            return observed.isoformat(timespec="seconds").replace("+00:00", "Z")
        if time.monotonic() >= deadline:
            _fail("TRUSTED_CLOCK_DID_NOT_ADVANCE")
        time.sleep(0.01)


def _frame_sha256(frame: pd.DataFrame) -> str:
    try:
        hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype="<u8")
    except Exception as exc:
        raise AuthoritativeSearchExecutionError("FRAME_HASH_FAILED") from exc
    digest = hashlib.sha256()
    digest.update(b"fxpm-authoritative-search-frame-v1\x00")
    digest.update(_canonical_bytes([str(value) for value in frame.columns]))
    digest.update(hashed.tobytes(order="C"))
    return digest.hexdigest()


def _receipt(schema: str, body: Mapping[str, Any]) -> dict[str, Any]:
    payload = {"schema_version": schema, **dict(body)}
    return {**payload, "receipt_sha256": _sha256(payload)}


def _source_identity(source_contract: Mapping[str, Any]) -> str:
    sample = trade_power.build_trade_sample_evidence(
        [],
        source_contract=dict(source_contract),
        minimum_observations=raw_winner.PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    if sample.source_contract is None:
        _fail("TRADE_POWER_SOURCE_CONTRACT_INVALID", *sample.reason_codes)
    return _hex64(
        sample.source_contract.source_contract_sha256,
        "TRADE_POWER_SOURCE_IDENTITY_INVALID",
    )


def _native_spread_inputs(
    *, product: native_prefix.NativePrefixProduct, config: PipelineConfig,
    frame: pd.DataFrame, spec: InstrumentSpec, _reopened_fine_product=None,
) -> tuple[dict[str, spread_profile.FrozenSpreadTier], pd.Series, native_prefix.NativePrefixProduct, pd.DataFrame]:
    """Reopen the same sealed generation and its exact prior-fine source."""
    descriptor = product.prefix_descriptor
    path = product.prefix_path
    if not isinstance(descriptor, Mapping) or not isinstance(path, Path):
        _fail("FROZEN_TIER_TARGET_PREFIX_ROUTE_INVALID")
    relative = descriptor.get("relative_path")
    if (type(relative) is not str or not relative or "\\" in relative
            or relative.startswith("/") or ":" in relative):
        _fail("FROZEN_TIER_TARGET_PREFIX_ROUTE_INVALID")
    parts = relative.split("/")
    if (any(part in {"", ".", ".."} for part in parts) or not path.is_absolute()
            or tuple(path.parts[-len(parts):]) != tuple(parts)):
        _fail("FROZEN_TIER_TARGET_PREFIX_ROUTE_INVALID")
    root = path.parents[len(parts) - 1]
    try:
        context = authority_spool.reopen_generation_context(root)
        replayed = native_prefix.reverify_activated_native_prefix_product(
            product, context, require_current_activation=True,
        )
        if (replayed.index_sha256 != product.index_sha256
                or replayed.prefix_path != path
                or Path(config.data_dir).resolve() != product._data_dir):
            _fail("FROZEN_TIER_TARGET_PREFIX_CONTEXT_MISMATCH")
        native = dict(product.native_prefix)
        profile = spread_profile.load_spread_profile(
            Path(config.data_dir) / "native" / "terms" / "spread_profile.json"
        )
        tiers = {tier: profile.resolve(
            native["symbol"], native["timeframe"], tier, point=spec.point,
            source_identity_sha256=native["source_identity_sha256"],
        ) for tier in ("P75", "P95")}
        source_timeframe = tiers["P75"].source_timeframe
        if _reopened_fine_product is not None:
            fine = native_prefix.reverify_activated_native_prefix_product(_reopened_fine_product, context, require_current_activation=True)
            if fine.index["timeframe"] != source_timeframe:
                _fail("BOUND_FIT_FINE_TIMEFRAME_CHANGED")
        elif source_timeframe == native["timeframe"]:
            fine = replayed
        else:
            fine = native_prefix.produce_activated_native_prefix(
                data_dir=Path(config.data_dir), context=context,
                symbol=native["symbol"], timeframe=source_timeframe,
                cutoff_utc=product.index["cutoff_utc"],
                source_accessed_at_utc=product.index["source_accessed_at_utc"],
                production_contract_required=product._production_contract_required,
            )
        if fine.source_state != native_prefix.AVAILABLE:
            _fail("FROZEN_TIER_PRIOR_FINE_SOURCE_UNAVAILABLE")
        source = fine.index["source"]
        tier = tiers["P75"].descriptor()
        binding = tier["source_binding"]
        if (source["bars_sha256"] != binding["bars_sha256"]
                or source["audit_sidecar_sha256"] != binding["sidecar_sha256"]
                or source["provenance_sha256"] != binding["provenance_sha256"]
                or product.index["source"]["bars_sha256"] != binding["target_bars_sha256"]
                or fine.index["activation"]["activation_id"] != tier["activation"]["id"]
                or fine.index["activation"]["manifest_sha256"] != tier["activation"]["manifest_sha256"]
                or source["source_identity_sha256"] != native["source_identity_sha256"]
                or fine.index["cutoff_utc"] != product.index["cutoff_utc"]
                or fine.index["source_accessed_at_utc"] != product.index["source_accessed_at_utc"]):
            _fail("FROZEN_TIER_PROFILE_FINE_SOURCE_BINDING_MISMATCH")
        fine_spec, _receipt = _freeze_bound_execution_spec(product=fine, config=config)
        if asdict(fine_spec) != asdict(spec):
            _fail("FROZEN_TIER_PRIOR_FINE_SPEC_MISMATCH")
        fine_frame = (frame if _reopened_fine_product is None
            and source_timeframe == native["timeframe"]
            else native_prefix.open_verified_prefix_frame(fine))
        observations = spread_profile.align_prior_completed_spreads(
            frame.index, fine_frame, source_timeframe,
        )
    except AuthoritativeSearchExecutionError:
        raise
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "FROZEN_TIER_SOURCE_REOPEN_FAILED", (type(exc).__name__, str(exc)),
        ) from exc
    return tiers, observations, fine, fine_frame


def _default_regime_parameters(timeframe: str) -> dict[str, Any]:
    tf = str(timeframe or "").strip().upper()
    base = pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME.get(tf)
    if base is None:
        _fail("REGIME_TIMEFRAME_UNSUPPORTED", tf)
    return pm_regime.RegimeParams.from_dict(base.to_dict()).to_dict()


def _build_train_regime_support(
    *,
    frame: pd.DataFrame,
    surface: pd.DataFrame,
    product: native_prefix.NativePrefixProduct,
    symbol: str,
    timeframe: str,
    parameters_sha256: str,
    detector_code_sha256: str,
    m5_view_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not surface.index.equals(frame.index) or not {
        "REGIME_LIVE",
        "REGIME_WARMUP",
    }.issubset(surface.columns):
        _fail("REGIME_SURFACE_INVALID")
    warmup = surface["REGIME_WARMUP"]
    if warmup.isna().any() or not warmup.isin([True, False]).all():
        _fail("REGIME_WARMUP_SURFACE_INVALID")
    warmup = warmup.astype(bool)
    eligible = (~warmup) & (~warmup.shift(1, fill_value=True))
    labels = surface.loc[eligible, "REGIME_LIVE"]
    if labels.isna().any():
        _fail("REGIME_SUPPORT_LABEL_MISSING")
    counts = {label: 0 for label in book.REGIME_ORDER}
    vector = hashlib.sha256()
    vector.update(b"fxpm.regime-live-labels-nul/v1\x00")
    for raw_label in labels.array:
        label = getattr(raw_label, "value", raw_label)
        if type(label) is not str or label not in counts:
            _fail("REGIME_SUPPORT_LABEL_INVALID", label)
        counts[label] += 1
        vector.update(label.encode("ascii"))
        vector.update(b"\x00")
    observed = [label for label in book.REGIME_ORDER if counts[label] > 0]
    unobserved = [label for label in book.REGIME_ORDER if counts[label] == 0]
    native = dict(product.native_prefix)
    window = _train_regime_support_window(frame=frame, product=product, symbol=symbol,
        timeframe=timeframe, m5_view_evidence=m5_view_evidence)
    body = {
        "schema_version": book.REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "label_order": list(book.REGIME_ORDER),
        "eligible_rows": int(len(labels)),
        "counts": counts,
        "observed_regimes": observed,
        "unobserved_regimes": unobserved,
        "support_state_by_regime": {
            label: ("EMPIRICALLY_OBSERVED" if counts[label] > 0 else "UNOBSERVED")
            for label in book.REGIME_ORDER
        },
        "first_ts": (
            None if labels.empty else pd.Timestamp(labels.index[0]).isoformat()
        ),
        "last_ts": (
            None if labels.empty else pd.Timestamp(labels.index[-1]).isoformat()
        ),
        "vector_encoding": book.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": vector.hexdigest(),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": native["activation_id"],
            "activation_manifest_sha256": native["activation_manifest_sha256"],
            "source_sha256": native["artifact_sha256"],
            "train_window_sha256": _sha256(window),
            "params_sha256": parameters_sha256,
            "detector_code_sha256": detector_code_sha256,
        },
    }
    return {**body, "receipt_sha256": _sha256(body)}


def _read_bound_json_file(
    path: Path,
    *,
    maximum_bytes: int,
    code: str,
    include_raw: bool = False,
) -> tuple[dict[str, Any], str, int] | tuple[dict[str, Any], str, int, bytes]:
    """Read one regular JSON file through a single identity-checked handle."""

    try:
        absolute = path.resolve(strict=True)
        before = absolute.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or _is_reparse(before)
            or not 0 < before.st_size <= maximum_bytes
        ):
            _fail(f"{code}_FILE_INVALID")
        with absolute.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_size != before.st_size
                or opened.st_mtime_ns != before.st_mtime_ns
            ):
                _fail(f"{code}_FILE_TOCTOU")
            raw = stream.read(before.st_size + 1)
            if len(raw) != before.st_size:
                _fail(f"{code}_FILE_SIZE_CHANGED")
            after_open = os.fstat(stream.fileno())
        after_path = absolute.lstat()
        identity = (
            int(before.st_dev),
            int(before.st_ino),
            int(before.st_size),
            int(before.st_mtime_ns),
        )
        if identity != (
            int(after_open.st_dev),
            int(after_open.st_ino),
            int(after_open.st_size),
            int(after_open.st_mtime_ns),
        ) or identity != (
            int(after_path.st_dev),
            int(after_path.st_ino),
            int(after_path.st_size),
            int(after_path.st_mtime_ns),
        ):
            _fail(f"{code}_FILE_CHANGED")
        value = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_strict_pairs,
            parse_constant=_reject_json_constant,
        )
    except AuthoritativeSearchExecutionError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise AuthoritativeSearchExecutionError(f"{code}_READ_FAILED") from exc
    if type(value) is not dict:
        _fail(f"{code}_OBJECT_REQUIRED")
    result = (value, hashlib.sha256(raw).hexdigest(), len(raw))
    return (*result, raw) if include_raw else result


def _normalized_symbol(value: Any) -> str:
    return str(value or "").split(".", 1)[0].split("#", 1)[0].upper()


def _matched_symbol_record(
    values: Mapping[str, Any],
    symbol: str,
    *,
    code: str,
) -> tuple[str, dict[str, Any]]:
    normalized = _normalized_symbol(symbol)
    matches: list[tuple[str, dict[str, Any]]] = []
    for key, raw in values.items():
        if type(raw) is not dict:
            continue
        aliases = (key, raw.get("symbol"), raw.get("broker_symbol"))
        if any(_normalized_symbol(alias) == normalized for alias in aliases if alias):
            matches.append((str(key), dict(raw)))
    if len(matches) != 1:
        _fail(code, normalized, len(matches))
    return matches[0]


def _finite_number(value: Any, code: str, *, allow_zero: bool = False) -> float:
    if type(value) not in {int, float}:
        _fail(code)
    number = float(value)
    if not math.isfinite(number) or number < 0.0 or (number == 0.0 and not allow_zero):
        _fail(code)
    return number


def _freeze_bound_execution_spec(
    *,
    product: native_prefix.NativePrefixProduct,
    config: PipelineConfig,
) -> tuple[InstrumentSpec, dict[str, Any]]:
    """Derive execution economics from pre-search authenticated file bytes."""

    native = dict(product.native_prefix)
    symbol = str(native.get("symbol") or "")
    instrument = dict(product.instrument_snapshot or {})
    if instrument != dict(product.index["source"].get("instrument_snapshot") or {}):
        _fail("EXECUTION_SPEC_NATIVE_INSTRUMENT_BINDING_INVALID")
    config_raw = str(getattr(config, "_deployment_config_path", "") or "").strip()
    if not config_raw:
        _fail("EXECUTION_SPEC_DEPLOYMENT_CONFIG_UNBOUND")
    config_path = Path(config_raw)
    config_body, config_sha, config_size, config_bytes = _read_bound_json_file(
        config_path,
        maximum_bytes=MAX_DEPLOYMENT_CONFIG_BYTES,
        code="EXECUTION_SPEC_DEPLOYMENT_CONFIG",
        include_raw=True,
    )
    metadata = config_body.get("_meta")
    if type(metadata) is not dict:
        _fail("EXECUTION_SPEC_DEPLOYMENT_METADATA_INVALID")
    scope = str(metadata.get("scope") or "")
    if not scope:
        _fail("EXECUTION_SPEC_DEPLOYMENT_SCOPE_UNBOUND")
    broker_raw = config_body.get("broker_specs_path")
    if type(broker_raw) is not str or not broker_raw.strip():
        _fail("EXECUTION_SPEC_BROKER_PATH_UNBOUND")
    broker_path = Path(broker_raw)
    if not broker_path.is_absolute():
        broker_path = config_path.resolve(strict=True).parent / broker_path
    broker_body, broker_sha, broker_size, broker_bytes = _read_bound_json_file(
        broker_path,
        maximum_bytes=MAX_BROKER_SPECS_BYTES,
        code="EXECUTION_SPEC_BROKER_SPECS",
        include_raw=True,
    )
    return _derive_bound_execution_spec(
        product=product,
        deployment_config_bytes=config_bytes,
        broker_specs_bytes=broker_bytes,
        broker_specs_relative_path=broker_raw,
    )


def _derive_bound_execution_spec(
    *,
    product: native_prefix.NativePrefixProduct,
    deployment_config_bytes: bytes,
    broker_specs_bytes: bytes,
    broker_specs_relative_path: str,
) -> tuple[InstrumentSpec, dict[str, Any]]:
    """Derive identical economics from captured bytes, without authenticating them."""

    def captured_object(raw: bytes, *, maximum_bytes: int, code: str):
        if type(raw) is not bytes or not 0 < len(raw) <= maximum_bytes:
            _fail(f"{code}_FILE_INVALID")
        try:
            value = json.loads(
                raw.decode("utf-8-sig"),
                object_pairs_hook=_strict_pairs,
                parse_constant=_reject_json_constant,
            )
        except AuthoritativeSearchExecutionError:
            raise
        except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise AuthoritativeSearchExecutionError(f"{code}_READ_FAILED") from exc
        if type(value) is not dict:
            _fail(f"{code}_OBJECT_REQUIRED")
        return value, hashlib.sha256(raw).hexdigest(), len(raw)

    native = dict(product.native_prefix)
    symbol = str(native.get("symbol") or "")
    instrument = dict(product.instrument_snapshot or {})
    if instrument != dict(product.index["source"].get("instrument_snapshot") or {}):
        _fail("EXECUTION_SPEC_NATIVE_INSTRUMENT_BINDING_INVALID")
    config_body, config_sha, config_size = captured_object(
        deployment_config_bytes,
        maximum_bytes=MAX_DEPLOYMENT_CONFIG_BYTES,
        code="EXECUTION_SPEC_DEPLOYMENT_CONFIG",
    )
    metadata = config_body.get("_meta")
    if type(metadata) is not dict:
        _fail("EXECUTION_SPEC_DEPLOYMENT_METADATA_INVALID")
    scope = str(metadata.get("scope") or "")
    if not scope:
        _fail("EXECUTION_SPEC_DEPLOYMENT_SCOPE_UNBOUND")
    broker_raw = config_body.get("broker_specs_path")
    if type(broker_raw) is not str or not broker_raw.strip():
        _fail("EXECUTION_SPEC_BROKER_PATH_UNBOUND")
    if broker_raw != broker_specs_relative_path:
        _fail("EXECUTION_SPEC_BROKER_PATH_BINDING_INVALID")
    broker_body, broker_sha, broker_size = captured_object(
        broker_specs_bytes,
        maximum_bytes=MAX_BROKER_SPECS_BYTES,
        code="EXECUTION_SPEC_BROKER_SPECS",
    )
    broker_key, broker = _matched_symbol_record(
        broker_body,
        symbol,
        code="EXECUTION_SPEC_BROKER_SYMBOL_CARDINALITY_INVALID",
    )
    if _normalized_symbol(instrument.get("symbol")) != _normalized_symbol(
        symbol
    ) or _normalized_symbol(broker.get("symbol") or broker_key) != _normalized_symbol(
        symbol
    ):
        _fail("EXECUTION_SPEC_NATIVE_BROKER_IDENTITY_MISMATCH")

    digits = int(instrument["digits"])
    point = _finite_number(instrument["point"], "EXECUTION_SPEC_POINT_INVALID")
    tick_size = _finite_number(
        instrument["trade_tick_size"], "EXECUTION_SPEC_TICK_SIZE_INVALID"
    )
    native_tick_value = _finite_number(
        instrument["trade_tick_value"],
        "EXECUTION_SPEC_TICK_VALUE_INVALID",
        allow_zero=True,
    )
    contract_size = _finite_number(
        instrument["trade_contract_size"], "EXECUTION_SPEC_CONTRACT_SIZE_INVALID"
    )
    if native_tick_value == 0.0:
        if instrument.get("digits") != broker.get("digits"):
            _fail("EXECUTION_SPEC_NATIVE_BROKER_IDENTITY_MISMATCH")
        structural = (
            ("point", "point"),
            ("trade_tick_size", "tick_size"),
            ("trade_contract_size", "contract_size"),
        )
        for native_name, broker_name in structural:
            observed = _finite_number(
                instrument.get(native_name),
                f"EXECUTION_SPEC_NATIVE_{native_name.upper()}_INVALID",
            )
            expected = _finite_number(
                broker.get(broker_name),
                f"EXECUTION_SPEC_BROKER_{broker_name.upper()}_INVALID",
            )
            if not math.isclose(observed, expected, rel_tol=1e-12, abs_tol=1e-15):
                _fail(
                    "EXECUTION_SPEC_NATIVE_BROKER_STRUCTURAL_MISMATCH",
                    native_name,
                )
        broker_tick_value = _finite_number(
            broker.get("tick_value"), "EXECUTION_SPEC_BROKER_TICK_VALUE_INVALID"
        )
        tick_value = broker_tick_value
        tick_value_source = "FROZEN_BROKER_SPECS_RECORD_NATIVE_ZERO_UNAVAILABLE"
    else:
        tick_value = native_tick_value
        tick_value_source = "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT"
    pip_position = digits - 1 if digits in {3, 5} else digits
    pip_size = _finite_number(
        point * 10.0 if digits in {3, 5} else point,
        "EXECUTION_SPEC_PIP_SIZE_INVALID",
    )
    volume_min = _finite_number(
        broker.get("volume_min"), "EXECUTION_SPEC_VOLUME_MIN_INVALID"
    )
    volume_max = _finite_number(
        broker.get("volume_max"), "EXECUTION_SPEC_VOLUME_MAX_INVALID"
    )
    volume_step = _finite_number(
        broker.get("volume_step"), "EXECUTION_SPEC_VOLUME_STEP_INVALID"
    )
    if volume_min > volume_max or volume_step > volume_max:
        _fail("EXECUTION_SPEC_VOLUME_GEOMETRY_INVALID")
    spread_points = _finite_number(
        broker.get("spread"), "EXECUTION_SPEC_SPREAD_INVALID", allow_zero=True
    )
    spec = InstrumentSpec(
        symbol=symbol,
        pip_position=pip_position,
        pip_value=(pip_size / tick_size) * tick_value,
        spread_avg=(spread_points * point / pip_size),
        min_lot=volume_min,
        max_lot=volume_max,
        swap_long=float(broker.get("swap_long") or 0.0),
        swap_short=float(broker.get("swap_short") or 0.0),
        swap_mode=int(broker.get("swap_mode", -1)),
        swap_rollover3days=int(broker.get("swap_rollover3days", 3)),
        tick_size=tick_size,
        tick_value=tick_value,
        contract_size=contract_size,
        volume_step=volume_step,
        stops_level=int(broker.get("stops_level", 0) or 0),
        point=point,
        digits=digits,
        asset_class=str(instrument["asset_class"]),
    )
    spec_body = asdict(spec)
    receipt = _receipt(
        EXECUTION_SPEC_RECEIPT_SCHEMA,
        {
            "symbol": symbol,
            "authority_timing": "FROZEN_BEFORE_FIRST_SEARCH_EVALUATION",
            "deployment_scope": scope,
            "deployment_config_sha256": config_sha,
            "deployment_config_size_bytes": config_size,
            "broker_specs_relative_path": broker_raw,
            "broker_specs_sha256": broker_sha,
            "broker_specs_size_bytes": broker_size,
            "broker_record_key": broker_key,
            "broker_record_sha256": _sha256(broker),
            "native_prefix_index_sha256": product.index_sha256,
            "native_provenance_sha256": native["provenance_sha256"],
            "native_instrument_snapshot_sha256": _sha256(instrument),
            "tick_value_source": tick_value_source,
            "volume_source": "FROZEN_BROKER_SPECS_RECORD",
            "instrument_spec": spec_body,
            "instrument_spec_sha256": Backtester._cost_attestation_json_sha256(
                {
                    "schema_version": BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
                    "instrument_spec": spec_body,
                }
            ),
            "grants_search_authority": False,
            "grants_trading_authority": False,
        },
    )
    return spec, receipt


def fit_native_timeframe_for_search(
    *,
    product: native_prefix.NativePrefixProduct,
    config: PipelineConfig,
    instrument_spec: InstrumentSpec | None = None,
    acquisition_attempts_exact: int = 1,
    _numeric_preparation=None, _numeric_config_sha256=None,
) -> FittedNativeSearchTimeframe:
    """Open one native prefix and derive its genuine source/regime authority."""

    if type(product) is not native_prefix.NativePrefixProduct:
        _fail("NATIVE_PREFIX_PRODUCT_INVALID")
    if product.source_state != native_prefix.AVAILABLE:
        _fail("NATIVE_PREFIX_NOT_AVAILABLE", product.source_state)
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    if type(acquisition_attempts_exact) is not int or not (
        1 <= acquisition_attempts_exact <= 1_000_000
    ):
        _fail("ACQUISITION_ATTEMPT_COUNT_INVALID")
    if instrument_spec is not None:
        _fail("CALLER_INSTRUMENT_SPEC_FORBIDDEN")
    try:
        frame = native_prefix.open_verified_prefix_frame(product)
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "NATIVE_PREFIX_FRAME_REOPEN_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    native = dict(product.native_prefix)
    symbol = str(native.get("symbol") or "")
    timeframe = str(native.get("timeframe") or "").upper()
    if (
        symbol not in book.PINNED_SYMBOL_ORDER
        or timeframe not in book.TIMEFRAME_ORDER
        or not 1 <= len(frame) <= MAX_PREFIX_ROWS
        or native.get("source_state") != native_prefix.AVAILABLE
        or native.get("prefix_rows") != len(frame)
    ):
        _fail("NATIVE_PREFIX_SEARCH_IDENTITY_INVALID")
    instrument_spec, execution_spec_receipt = _freeze_bound_execution_spec(
        product=product,
        config=config,
    )
    if str(config.backtest_spread_mode).lower() != "conservative_max":
        _fail("BASE_SPREAD_POLICY_NOT_PRODUCTION_CONSERVATIVE")

    tiers, observations, fine_product, fine_frame = _native_spread_inputs(
        product=product, config=config, frame=frame, spec=instrument_spec,
    )
    return _fit_verified_search_frame(
        product=product, config=config, frame=frame, native=native, symbol=symbol,
        timeframe=timeframe, instrument_spec=instrument_spec,
        execution_spec_receipt=execution_spec_receipt, tiers=tiers,
        observations=observations, fine_product=fine_product, fine_frame=fine_frame,
        acquisition_attempts_exact=acquisition_attempts_exact,
        source_reason_code="AUTHENTICATED_NATIVE_PREFIX_AVAILABLE",
        fit_cutoff_utc=native["prefix_end_exclusive_utc"],
        _numeric_preparation=_numeric_preparation, _numeric_config_sha256=_numeric_config_sha256,
    )


def _unit_numeric_identity(*, product, timeframe, config_sha256, tiers, derived_binding=None):
    """Use facts already authenticated by this cell; missing facts simply do not reuse."""
    native = product.native_prefix
    fields = ("activation_id", "activation_manifest_sha256", "activation_entry_sha256",
        "artifact_sha256", "prefix_content_sha256", "prefix_rows")
    facts = tuple(native.get(name) for name in fields)
    cutoff = product.index.get("cutoff_utc")
    profiles = tuple((name, tier.descriptor().get("profile_sha256")) for name, tier in sorted(tiers.items()))
    if (not config_sha256 or not cutoff or any(value is None for value in facts)
            or any(not value for _, value in profiles)):
        return None
    view = None
    if derived_binding is not None:
        evidence = derived_binding.view_evidence
        view = tuple(evidence.get(name) for name in ("view_timeframe", "view_payload_sha256", "view_membership_sha256"))
        if any(value is None for value in view):
            return None
    return ("m5_derived" if view is not None else "broker_native", native.get("symbol"),
        timeframe, cutoff, facts, config_sha256, profiles, view)


def _fit_verified_search_frame(*, product, config, frame, native, symbol, timeframe,
        instrument_spec, execution_spec_receipt, tiers, observations, fine_product,
        fine_frame, acquisition_attempts_exact, source_reason_code, fit_cutoff_utc,
        derived_binding=None, _numeric_preparation=None, _numeric_config_sha256=None):
    """Shared original probe/regime calculations after each source branch verifies inputs."""
    probe_backtester = Backtester(config)
    try:
        probes = {tier: probe_backtester.build_evidence_source_seed_result(
            frame, symbol, spec=instrument_spec, timeframe=timeframe,
            spread_tier=descriptor, entry_spread_observations=observations,
        ) for tier, descriptor in tiers.items()}
        source_seeds = {tier: raw_winner.build_trade_source_contract_from_backtester_result(result)
                        for tier, result in probes.items()}
        pair = pm_core.build_backtest_frozen_tier_pair_contract(
            probes, spec=instrument_spec, use_spread=bool(config.use_spread),
        )
        _seeds, pair, pair_hashes = raw_winner._current_pair_inputs(source_seeds, pair)
        source_contract_sha256 = pair_hashes["source_contract_sha256"]
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "SOURCE_CONTRACT_SEED_FAILED", (type(exc).__name__, str(exc)),
        ) from exc

    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.AVAILABLE,
        reason_code=source_reason_code,
        source_contract_sha256=source_contract_sha256,
        source_group_evidence_sha256=product.index_sha256,
        acquisition_attempts_exact=acquisition_attempts_exact,
        activation_id=native["activation_id"],
        activation_manifest_sha256=native["activation_manifest_sha256"],
        activation_entry_sha256=native["activation_entry_sha256"],
        native_source_sha256=native["artifact_sha256"],
    )
    if source is None:
        _fail("SOURCE_GROUP_DECISION_REJECTED")

    parameters = _default_regime_parameters(timeframe)
    parameters_sha256 = pm_regime.regime_params_sha256(parameters)
    detector_code_sha256 = pm_regime.detector_code_sha256()
    if detector_code_sha256 is None:
        _fail("REGIME_DETECTOR_CODE_UNAVAILABLE")
    numeric_identity = (_unit_numeric_identity(product=product, timeframe=timeframe,
        config_sha256=_numeric_config_sha256, tiers=tiers, derived_binding=derived_binding)
        if _numeric_preparation is not None else None)
    numeric_key = (None if numeric_identity is None else (numeric_identity, parameters_sha256, detector_code_sha256))
    try:
        def compute_surface():
            detector = pm_regime.MarketRegimeDetector(pm_regime.RegimeParams.from_dict(parameters))
            return detector.compute_regime_scores(frame)
        surface = (compute_surface() if _numeric_preparation is None else
            _numeric_preparation.prepare(("regime", numeric_key) if numeric_key is not None else None,
                frame, compute_surface, source_columns=tuple(frame.columns)))
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "REGIME_DETECTOR_EXECUTION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    support = _build_train_regime_support(
        frame=frame,
        surface=surface,
        product=product,
        symbol=symbol,
        timeframe=timeframe,
        parameters_sha256=parameters_sha256,
        detector_code_sha256=detector_code_sha256,
        m5_view_evidence=None if derived_binding is None else dict(derived_binding.view_evidence),
    )
    detector_record = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=_book_canonical_utc(
            fit_cutoff_utc,
            "REGIME_DETECTOR_CUTOFF_INVALID",
        ),
        detector_version=REGIME_FIT_POLICY_VERSION,
        detector_code_sha256=detector_code_sha256,
        parameters=parameters,
    )
    if detector_record is None:
        _fail("REGIME_DETECTOR_RECORD_REJECTED")
    policy = {
        "schema_version": REGIME_FIT_POLICY_VERSION,
        "parameter_source": "PINNED_TIMEFRAME_DEFAULT_NO_POSTHOC_TUNING",
        "timeframe": timeframe,
        "label_order": list(book.REGIME_ORDER),
        "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "detector_code_sha256": detector_code_sha256,
        "parameters_sha256": parameters_sha256,
    }
    evidence = _regime_fit_evidence(source=source, product=product, support=support,
        detector_record=detector_record, derived_binding=derived_binding)
    mapping = {
        "schema_version": "fxpm.four-regime-mapping.v1",
        "label_order": list(book.REGIME_ORDER),
        "unobserved_regimes_are_explicit": True,
        "all_regimes_are_not_required": True,
    }
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_CAUSAL_REGIME_FIT_COMPLETE",
        regime_fit_policy_sha256=_sha256(policy),
        detector_process_sha256=detector_code_sha256,
        regime_fit_evidence_sha256=_sha256(evidence),
        four_regime_mapping_sha256=_sha256(mapping),
        regime_params_sha256=parameters_sha256,
        regime_detector_record=detector_record,
        train_regime_support=support,
    )
    if fit is None:
        _fail("REGIME_FIT_DECISION_REJECTED")

    combined = frame.copy(deep=True)
    for name in _REGIME_COLUMNS:
        combined.loc[:, name] = surface[name]
    digest = _frame_sha256(combined)
    return FittedNativeSearchTimeframe(
        product=product,
        source_group_decision=source,
        regime_fit_decision=fit,
        source_contract_seed={},
        cost_ladder_contract={},
        regime_parameters=json.loads(_canonical_bytes(parameters).decode("ascii")),
        train_regime_support=json.loads(_canonical_bytes(support).decode("ascii")),
        instrument_spec=replace(instrument_spec),
        execution_spec_receipt=json.loads(
            _canonical_bytes(execution_spec_receipt).decode("ascii")
        ),
        frame_sha256=digest,
        _frame=combined,
        source_contract_seeds=json.loads(_canonical_bytes(source_seeds).decode("ascii")),
        frozen_tier_pair_contract=json.loads(_canonical_bytes(pair).decode("ascii")),
        _spread_tiers=tiers,
        _entry_spread_observations=observations.copy(deep=True),
        _spread_source_product=fine_product,
        _spread_source_frame=fine_frame,
        _derived_binding=derived_binding,
        _numeric_preparation=_numeric_preparation, _numeric_key=numeric_key,
        _numeric_source_columns=tuple(frame.columns),
    )


@dataclass(frozen=True, slots=True)
class _DerivedSearchBinding:
    context: Any
    owner: Any
    request_lease: Any
    calibration_lease: Any
    cost_edition: Any
    declared_m5_source_pins: Mapping[str, Any]
    view_evidence: Mapping[str, Any]


def fit_m5_derived_timeframe_for_search(*, context, owner, request_lease, calibration_lease,
        cost_edition, declared_m5_source_pins, config, acquisition_attempts_exact=1, _numeric_preparation=None, _numeric_config_sha256=None):
    """Use a registered M5 view and recorded costs; the product remains genuine base M5."""
    from pm_m5_job_view import M5JobViews
    from pm_m5_derived_costs import RecordedDerivedCosts
    import pm_historical_anchor_orchestration as orchestration
    if (type(context) is not authority_spool.GenerationSpoolContext
            or type(owner) is not M5JobViews or type(cost_edition) is not RecordedDerivedCosts
            or not isinstance(config, PipelineConfig)
            or owner.binding.generation_context_sha256 != context.context_sha256
            or owner.binding.config_sha256 != context.config_sha256):
        _fail("M5_DERIVED_SEARCH_INPUT_INVALID")
    if type(acquisition_attempts_exact) is not int or not 1 <= acquisition_attempts_exact <= 1_000_000:
        _fail("ACQUISITION_ATTEMPT_COUNT_INVALID")
    view = owner.view(request_lease)
    declared_digest = orchestration._validate_declared_m5_source_pins(
        declared_m5_source_pins, {"timeframe": view.timeframe})
    if (declared_digest != owner.binding.m5_view_contract_sha256
            or declared_m5_source_pins["view_plan_sha256"] != owner.view_plan_sha256):
        _fail("M5_DERIVED_DECLARED_VIEW_PLAN_MISMATCH")
    frame = owner.frame(request_lease)
    product = owner.product_at(view.cutoff_utc)
    replayed = native_prefix.reverify_activated_native_prefix_product(
        product, context, require_current_activation=True)
    if replayed.index_sha256 != product.index_sha256:
        _fail("M5_DERIVED_SEARCH_SOURCE_REVERIFY_MISMATCH")
    native = dict(product.native_prefix)
    if (product.source_state != native_prefix.AVAILABLE or product.index["timeframe"] != "M5"
            or view.timeframe not in {"M15", "M30", "H1"}
            or not 1 <= len(frame) <= MAX_PREFIX_ROWS
            or Path(config.data_dir).resolve() != product._data_dir):
        _fail("M5_DERIVED_SEARCH_IDENTITY_INVALID")
    marker = frame.attrs.get(pm_core.NATIVE_SOURCE_BINDING_ATTR)
    if (type(marker) is not dict or marker.get("source_kind") != "m5_derived"
            or marker.get("timeframe") != "M5" or marker.get("view_timeframe") != view.timeframe
            or marker.get("m5_view_contract_sha256") != owner.binding.m5_view_contract_sha256):
        _fail("M5_DERIVED_SEARCH_VIEW_BINDING_INVALID")
    instrument_spec, execution_spec_receipt = _freeze_bound_execution_spec(product=product, config=config)
    if str(config.backtest_spread_mode).lower() != "conservative_max":
        _fail("BASE_SPREAD_POLICY_NOT_PRODUCTION_CONSERVATIVE")
    bound = cost_edition.bind_view(owner, context=context, calibration_lease=calibration_lease,
                                  request_lease=request_lease, point=instrument_spec.point)
    tiers = {tier: bound.resolve(native["symbol"], view.timeframe, tier,
                point=instrument_spec.point,
                source_identity_sha256=native["source_identity_sha256"])
             for tier in ("P75", "P95")}
    expected_cost = declared_m5_source_pins["cost_edition_identity"]
    for tier in tiers.values():
        descriptor = tier.descriptor()
        if (descriptor.get("profile_sha256") != expected_cost["profile_sha256"]
                or descriptor.get("m5_derived_cost_binding", {}).get("cost_edition_identity") != expected_cost):
            _fail("M5_DERIVED_DECLARED_COST_EDITION_MISMATCH")
    fine_frame = native_prefix.open_verified_prefix_frame(product)
    observations = spread_profile.align_prior_completed_spreads(frame.index, fine_frame, "M5")
    view_evidence = {
        "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": view.timeframe,
        "m5_view_contract_sha256": owner.binding.m5_view_contract_sha256,
        "view_key": view.view_key, "view_payload_sha256": view.payload_sha256,
        "view_membership_sha256": view.membership_sha256,
        "base_native_prefix_index_sha256": product.index_sha256,
        "source_accessed_at_utc": product.index["source_accessed_at_utc"],
    }
    binding = _DerivedSearchBinding(context, owner, request_lease, calibration_lease, cost_edition,
        json.loads(_canonical_bytes(declared_m5_source_pins).decode("ascii")),
        json.loads(_canonical_bytes(view_evidence).decode("ascii")))
    return _fit_verified_search_frame(
        product=product, config=config, frame=frame, native=native, symbol=native["symbol"],
        timeframe=view.timeframe, instrument_spec=instrument_spec,
        execution_spec_receipt=execution_spec_receipt, tiers=tiers,
        observations=observations, fine_product=product, fine_frame=fine_frame,
        acquisition_attempts_exact=acquisition_attempts_exact,
        source_reason_code="AUTHENTICATED_BASE_M5_DERIVED_PREFIX_AVAILABLE",
        fit_cutoff_utc=pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1]).isoformat(),
        derived_binding=binding,
        _numeric_preparation=_numeric_preparation, _numeric_config_sha256=_numeric_config_sha256,
    )


def m5_view_price_frame(*, context, owner, lease):
    """Reopen one registered target view without calling a detector or a fitter."""
    from pm_m5_job_view import M5JobViews
    if (type(context) is not authority_spool.GenerationSpoolContext or type(owner) is not M5JobViews
            or owner.binding.generation_context_sha256 != context.context_sha256
            or owner.binding.config_sha256 != context.config_sha256):
        _fail("M5_DERIVED_PRICE_OWNER_INVALID")
    view = owner.view(lease)
    frame = owner.frame(lease)
    product = owner.product_at(view.cutoff_utc)
    reopened = native_prefix.reverify_activated_native_prefix_product(
        product, context, require_current_activation=True)
    if (reopened.source_state != native_prefix.AVAILABLE
            or reopened.index_sha256 != product.index_sha256
            or reopened.index["timeframe"] != "M5"
            or reopened.index["symbol"] != view.source.symbol or frame.empty):
        _fail("M5_DERIVED_PRICE_SOURCE_CHANGED")
    base = dict(reopened.native_prefix)
    end = pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1]).isoformat()
    window = {
        "schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
        "symbol": view.source.symbol, "timeframe": view.timeframe,
        "m5_view_contract_sha256": owner.binding.m5_view_contract_sha256,
        "view_key": view.view_key, "view_payload_sha256": view.payload_sha256,
        "view_membership_sha256": view.membership_sha256,
        "view_frame_sha256": _frame_sha256(frame),
        "view_start_utc": pd.Timestamp(frame.index[0]).isoformat(),
        "view_end_exclusive_utc": end, "view_last_close_utc": end,
        "view_rows": len(frame), "base_native_prefix_index_sha256": reopened.index_sha256,
        "base_native_prefix_content_sha256": base["prefix_content_sha256"],
    }
    return frame, window


def m5_derived_fitted_frame(fitted):
    """Join an actual live fitted binding to its price-only frame and target window."""
    if type(fitted) is not FittedNativeSearchTimeframe or type(fitted._derived_binding) is not _DerivedSearchBinding:
        _fail("M5_DERIVED_FITTED_BINDING_INVALID")
    binding = fitted._derived_binding
    frame, window = m5_view_price_frame(context=binding.context, owner=binding.owner,
                                       lease=binding.request_lease)
    evidence = binding.view_evidence
    if (window["base_native_prefix_index_sha256"] != fitted.product.index_sha256
            or any(window[key] != evidence[key] for key in (
                "m5_view_contract_sha256", "view_key", "view_payload_sha256", "view_membership_sha256"))
            or not set(frame.columns).issubset(fitted._frame.columns)
            or _frame_sha256(fitted._frame.loc[:, frame.columns]) != window["view_frame_sha256"]
            or _frame_sha256(fitted._frame) != fitted.frame_sha256):
        _fail("M5_DERIVED_FITTED_PRICE_FRAME_CHANGED")
    return frame, window


def _terminal_field(native: Mapping[str, Any], name: str, code: str) -> Any:
    value = native.get(name)
    if value is None:
        _fail(code, name)
    return value


def fit_terminal_native_timeframe(
    *,
    product: native_prefix.NativePrefixProduct,
    config: PipelineConfig,
) -> TerminalNativeTimeframe:
    """Derive Book source/regime facts for a reverified terminal native cell.

    ``SOURCE_UNAVAILABLE`` and the exact authenticated
    ``TECHNICAL_TERMINAL`` state yield different complete non-evaluation
    decisions.  Ordinary ``TECHNICAL_FAILURE`` remains an abort and can never
    be laundered into either terminal state or a market/no-trade verdict.
    """

    if type(product) is not native_prefix.NativePrefixProduct:
        _fail("NATIVE_PREFIX_PRODUCT_INVALID")
    if getattr(product, "_seal", None) is not native_prefix._PRODUCT_SEAL:
        _fail("NATIVE_PREFIX_PRODUCT_UNSEALED")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    state = product.source_state
    if state not in {
        native_prefix.SOURCE_UNAVAILABLE,
        native_prefix.TECHNICAL_TERMINAL,
        native_prefix.TECHNICAL_FAILURE,
    }:
        _fail("TERMINAL_NATIVE_PRODUCT_NOT_TERMINAL", state)
    native = dict(product.native_prefix)
    if native.get("source_state") != state:
        _fail("TERMINAL_NATIVE_STATE_MISMATCH")
    symbol = str(_terminal_field(native, "symbol", "TERMINAL_SYMBOL_INVALID"))
    timeframe = str(
        _terminal_field(native, "timeframe", "TERMINAL_TIMEFRAME_INVALID")
    ).upper()
    if symbol not in book.PINNED_SYMBOL_ORDER or timeframe not in book.TIMEFRAME_ORDER:
        _fail("TERMINAL_NATIVE_CELL_INVALID")
    terminal_receipt_sha = _hex64(
        _terminal_field(
            native,
            "source_terminal_receipt_sha256",
            "TERMINAL_RECEIPT_SHA_MISSING",
        ),
        "TERMINAL_RECEIPT_SHA_INVALID",
    )
    source_reason = str(
        _terminal_field(native, "source_reason", "TERMINAL_SOURCE_REASON_MISSING")
    )
    if not source_reason or len(source_reason) > 256:
        _fail("TERMINAL_SOURCE_REASON_INVALID")

    if state == native_prefix.TECHNICAL_FAILURE:
        # Never construct a Book decision for infrastructure failure; force the
        # anchor/generation caller to abort with a typed technical state.
        raise AuthoritativeSearchExecutionError(
            "TERMINAL_NATIVE_TECHNICAL_FAILURE_REQUIRES_ABORT",
            (symbol, timeframe, source_reason, terminal_receipt_sha),
        )

    # Derive the exact non-evaluation contract from the reverified terminal
    # product bytes.  The authenticated technical terminal additionally binds
    # the real activation manifest and cell entry; no bar artifact is asserted.
    source_identity_sha = _hex64(
        _terminal_field(
            native, "source_identity_sha256", "TERMINAL_SOURCE_IDENTITY_MISSING"
        ),
        "TERMINAL_SOURCE_IDENTITY_INVALID",
    )
    request_map_sha = _hex64(
        _terminal_field(native, "request_map_sha256", "TERMINAL_REQUEST_MAP_MISSING"),
        "TERMINAL_REQUEST_MAP_INVALID",
    )
    request_start = _utc(
        _terminal_field(native, "request_start_utc", "TERMINAL_REQUEST_START_MISSING"),
        "TERMINAL_REQUEST_START_INVALID",
    )
    request_end = _utc(
        _terminal_field(
            native, "request_end_exclusive_utc", "TERMINAL_REQUEST_END_MISSING"
        ),
        "TERMINAL_REQUEST_END_INVALID",
    )
    index = dict(product.index)
    cutoff = _utc(index.get("cutoff_utc"), "TERMINAL_CUTOFF_INVALID")
    contract = {
        "schema_version": TERMINAL_SOURCE_CONTRACT_SCHEMA,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_state": state,
        "source_reason": source_reason,
        "native_prefix_index_sha256": _hex64(
            product.index_sha256, "TERMINAL_INDEX_SHA_INVALID"
        ),
        "source_terminal_receipt_sha256": terminal_receipt_sha,
        "source_identity_sha256": source_identity_sha,
        "request_map_sha256": request_map_sha,
        "request_start_utc": request_start,
        "request_end_exclusive_utc": request_end,
        "cutoff_utc": cutoff,
        "economic_evaluation": "NOT_APPLICABLE",
        "exact_source_group_resolution_attempts": 1,
    }
    if state == native_prefix.TECHNICAL_TERMINAL:
        contract.update(
            {
                "activation_id": _terminal_field(
                    native, "activation_id", "TERMINAL_ACTIVATION_ID_MISSING"
                ),
                "activation_manifest_sha256": _hex64(
                    _terminal_field(
                        native,
                        "activation_manifest_sha256",
                        "TERMINAL_ACTIVATION_MANIFEST_MISSING",
                    ),
                    "TERMINAL_ACTIVATION_MANIFEST_INVALID",
                ),
                "activation_entry_sha256": _hex64(
                    _terminal_field(
                        native,
                        "activation_entry_sha256",
                        "TERMINAL_ACTIVATION_ENTRY_MISSING",
                    ),
                    "TERMINAL_ACTIVATION_ENTRY_INVALID",
                ),
            }
        )
    contract_sha = _sha256(contract)

    technical_terminal = state == native_prefix.TECHNICAL_TERMINAL
    reason_code = (
        TERMINAL_TECHNICAL_REASON_CODE
        if technical_terminal
        else TERMINAL_SOURCE_REASON_CODE
    )

    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=(
            book.TERMINAL_TECHNICAL if technical_terminal else book.TERMINAL_UNAVAILABLE
        ),
        reason_code=reason_code,
        source_contract_sha256=contract_sha,
        source_group_evidence_sha256=product.index_sha256,
        acquisition_attempts_exact=1,
        activation_id=contract.get("activation_id"),
        activation_manifest_sha256=contract.get("activation_manifest_sha256"),
        activation_entry_sha256=contract.get("activation_entry_sha256"),
        terminal_proof_sha256=terminal_receipt_sha,
    )
    if source is None:
        _fail("TERMINAL_SOURCE_GROUP_DECISION_REJECTED")

    policy = {
        "schema_version": TERMINAL_REGIME_FIT_POLICY_SCHEMA,
        "source_group_decision_sha256": source.receipt_sha256,
        "terminal_source_contract_sha256": contract_sha,
        "native_prefix_index_sha256": product.index_sha256,
        "source_terminal_receipt_sha256": terminal_receipt_sha,
        "timeframe": timeframe,
        "label_order": list(book.REGIME_ORDER),
        "fit_state": (
            book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL
            if technical_terminal
            else book.REGIME_NOT_APPLICABLE
        ),
        "detector_execution": "FORBIDDEN",
    }
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=policy["fit_state"],
        reason_code=reason_code,
        regime_fit_policy_sha256=_sha256(policy),
    )
    if fit is None:
        _fail("TERMINAL_REGIME_FIT_DECISION_REJECTED")

    return TerminalNativeTimeframe(
        product=product,
        source_group_decision=source,
        regime_fit_decision=fit,
        terminal_source_contract=json.loads(_canonical_bytes(contract).decode("ascii")),
        terminal_source_contract_sha256=contract_sha,
        terminal_regime_fit_policy=json.loads(_canonical_bytes(policy).decode("ascii")),
    )


def _realize_cutoff_refit_window(
    *,
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    fit_end: pd.Timestamp,
    lookback_kind: str,
    lookback_value: int,
    state_context_bars: int,
    boundary_limited: bool,
) -> tuple[pd.Timestamp, int]:
    """Realize one authenticated lookback at the latest closed prefix."""

    if (
        type(opens) is not pd.DatetimeIndex
        or type(closes) is not pd.DatetimeIndex
        or len(opens) != len(closes)
        or len(opens) == 0
        or opens.hasnans
        or closes.hasnans
        or not opens.is_monotonic_increasing
        or opens.has_duplicates
        or not isinstance(fit_end, pd.Timestamp)
        or fit_end.tzinfo is None
        or type(lookback_kind) is not str
        or lookback_kind not in {"CALENDAR_INTERVAL", "NATIVE_OBSERVATIONS"}
        or type(lookback_value) is not int
        or lookback_value <= 0
        or type(state_context_bars) is not int
        or state_context_bars < 0
        or type(boundary_limited) is not bool
    ):
        _fail("CURRENT_REFIT_WINDOW_GEOMETRY_INVALID")
    eligible_opens = opens[opens < fit_end]
    if lookback_kind == "CALENDAR_INTERVAL":
        target = fit_end - pd.Timedelta(days=lookback_value)
        eligible_opens = eligible_opens[eligible_opens >= target]
        fit_start = None if eligible_opens.empty else pd.Timestamp(eligible_opens[0])
    else:
        fit_start = (
            None
            if len(eligible_opens) < lookback_value
            else pd.Timestamp(eligible_opens[-lookback_value])
        )
    if fit_start is None or fit_start >= fit_end:
        _fail("CURRENT_REFIT_WINDOW_UNAVAILABLE")
    fit_start_index = int(opens.get_indexer([fit_start])[0])
    if not boundary_limited and fit_start_index < state_context_bars:
        _fail("CURRENT_REFIT_STATE_CONTEXT_UNAVAILABLE")
    source_count = int(
        ((opens >= fit_start) & (opens < fit_end) & (closes <= fit_end)).sum()
    )
    return fit_start, source_count


def _verify_trial_state_context_disposition(
    *,
    plan_body: Mapping[str, Any],
    candidate: Mapping[str, Any],
    trial: Mapping[str, Any],
    planned: Mapping[str, Any],
    ordinal: int,
) -> None:
    disposition = trial.get("state_context_disposition")
    if disposition is None:
        return
    expected_fields = {
        "schema_version",
        "candidate_id",
        "candidate_definition_sha256",
        "state_requirement_sha256",
        "required_context_bars",
        "selected_context_bars",
        "status",
        "reason_code",
        "disposition_sha256",
    }
    dependency_policy = plan_body.get("dependency_policy")
    payload = (
        dependency_policy.get("payload") if type(dependency_policy) is dict else None
    )
    geometry = plan_body.get("derived_dependency_geometry")
    if (
        type(disposition) is not dict
        or set(disposition) != expected_fields
        or type(payload) is not dict
        or type(geometry) is not dict
    ):
        _fail("STATE_CONTEXT_DISPOSITION_INVALID", ordinal)
    required = disposition["required_context_bars"]
    selected = disposition["selected_context_bars"]
    within = (
        type(required) is int
        and required >= 0
        and type(selected) is int
        and selected >= 1
        and required <= selected
    )
    expected_status = "PLANNED" if within else "PROCESS_REJECTED"
    expected_reason = None if within else STATE_CONTEXT_BOUNDARY_REASON
    body = {
        key: value for key, value in disposition.items() if key != "disposition_sha256"
    }
    if (
        disposition["schema_version"] != STATE_CONTEXT_DISPOSITION_SCHEMA
        or disposition["candidate_id"] != candidate.get("candidate_id")
        or disposition["candidate_definition_sha256"]
        != candidate.get("candidate_definition_sha256")
        or disposition["status"] != expected_status
        or disposition["reason_code"] != expected_reason
        or disposition["disposition_sha256"] != _sha256(body)
        or selected != payload.get("feature_lookback_bars")
        or geometry.get("warmup_bars", -1) < selected
        or planned.get("state_context_disposition_sha256")
        != disposition["disposition_sha256"]
    ):
        _fail("STATE_CONTEXT_DISPOSITION_BINDING_INVALID", ordinal)
    boundary = not bool(candidate.get("folds"))
    final_status = "PROCESS_REJECTED" if boundary else expected_status
    final_reason = "SOURCE_BOUNDARY_SHORTFALL" if boundary else expected_reason
    if (
        planned.get("status") != final_status
        or planned.get("reason_code") != final_reason
    ):
        _fail("STATE_CONTEXT_DISPOSITION_STATUS_INVALID", ordinal)


def _verify_attempt_roster(
    *,
    plan: Any,
    attempt_artifacts: Sequence[Mapping[str, Any]],
    fitted: FittedNativeSearchTimeframe,
    regime: str,
    search_process: Any = None,
) -> tuple[dict[str, Any], tuple[_VerifiedAttempt, ...]]:
    if type(fitted) is not FittedNativeSearchTimeframe:
        _fail("FITTED_TIMEFRAME_INVALID")
    try:
        binding = historical.historical_horizon_plan_binding(plan)
        plan_body = plan.to_dict()
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "HORIZON_PLAN_VERIFICATION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    source_body = fitted.source_group_decision.to_dict()
    symbol = source_body["symbol"]
    timeframe = source_body["timeframe"]
    m5_window = None
    if fitted._derived_binding is not None:
        _, m5_window = m5_derived_fitted_frame(fitted)
        if (plan_body.get("source_kind") != "m5_derived"
                or plan_body.get("m5_view_window") != m5_window):
            _fail("HORIZON_PLAN_M5_VIEW_MISMATCH")
    elif plan_body.get("source_kind", "broker_native") != "broker_native":
        _fail("HORIZON_PLAN_NATIVE_SOURCE_KIND_MISMATCH")
    normalized_regime = str(regime or "").strip().upper()
    attempt_limit = MAX_ATTEMPTS
    if search_process is not None:
        import pm_production_search_planner as planner
        process_body = search_process.to_dict()
        attempt_limit = planner.process_attempt_count(process_body)
    if (
        binding.get("cell")
        != {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": normalized_regime,
        }
        or plan_body.get("cell") != binding["cell"]
        or plan_body.get("native_prefix") != dict(fitted.product.native_prefix)
        or plan_body.get("source_prefix_receipt_sha256")
        != binding.get("source_prefix_receipt_sha256")
        or plan_body.get("plan_status")
        not in {"READY_FOR_HISTORICAL_EVIDENCE", "HISTORICAL_BOUNDARY_LIMITED"}
    ):
        _fail("HORIZON_PLAN_CELL_OR_SOURCE_MISMATCH")
    for digest_name, roster_name in (
        ("candidate_registry_sha256", "candidate_registry"),
        ("trial_registry_sha256", "trial_registry"),
        ("attempt_roster_sha256", "attempts"),
        ("evidence_roster_sha256", "expected_evaluations"),
    ):
        roster = plan_body.get(roster_name)
        if type(roster) is not list or plan_body.get(digest_name) != _sha256(roster):
            _fail("HORIZON_PLAN_ROSTER_HASH_INVALID", digest_name)
    candidates = plan_body.get("candidates")
    trials = plan_body.get("trials")
    planned_attempts = plan_body.get("attempts")
    if (
        type(candidates) is not list
        or type(trials) is not list
        or type(planned_attempts) is not list
        or not 1 <= len(planned_attempts) <= attempt_limit
        or type(attempt_artifacts) not in {list, tuple}
        or len(attempt_artifacts) != len(planned_attempts)
    ):
        _fail("HORIZON_ATTEMPT_ROSTER_INVALID")
    candidate_by_id = {
        item.get("candidate_id"): item for item in candidates if type(item) is dict
    }
    trial_by_id = {item.get("trial_id"): item for item in trials if type(item) is dict}
    if len(candidate_by_id) != len(candidates) or len(trial_by_id) != len(trials):
        _fail("HORIZON_CANDIDATE_OR_TRIAL_IDENTITY_INVALID")
    attempt_by_pair = {}
    for item in planned_attempts:
        if type(item) is not dict:
            _fail("HORIZON_ATTEMPT_INVALID")
        pair = (item.get("candidate_id"), item.get("trial_id"))
        if pair in attempt_by_pair:
            _fail("HORIZON_ATTEMPT_PAIR_DUPLICATED")
        attempt_by_pair[pair] = item

    prefix = dict(fitted.product.native_prefix)
    target_end = (m5_window["view_end_exclusive_utc"] if m5_window is not None
                  else prefix["prefix_end_exclusive_utc"])
    target_latest = (m5_window["view_last_close_utc"] if m5_window is not None
                     else prefix["prefix_last_close_utc"])
    target_snapshot = (m5_window["view_payload_sha256"] if m5_window is not None
                       else prefix["prefix_content_sha256"])
    policy = plan_body.get("horizon_policy")
    try:
        pair = pm_core.validate_backtest_frozen_tier_pair_contract(fitted.frozen_tier_pair_contract)
        pair_hashes = {
            "cost_contract_sha256": _sha256({level["tier"]: _sha256(level["numerical_execution_cost_policy"])
                for level in pair["tiers"]}),
            "frozen_tier_pair_contract_sha256": _sha256(pair),
        }
    except Exception as exc:
        raise AuthoritativeSearchExecutionError("HORIZON_COST_POLICY_BINDING_INVALID") from exc
    if (type(policy) is not dict
            or policy.get("schema_version") != "fxpm.historical-horizon-policy.v3"
            or policy.get("evaluator_release_sha256") != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
            or policy.get("cost_contract_sha256") != pair_hashes["cost_contract_sha256"]
            or policy.get("frozen_tier_pair_contract_sha256") != pair_hashes["frozen_tier_pair_contract_sha256"]):
        _fail("HORIZON_COST_POLICY_BINDING_INVALID")

    frame_opens = pd.DatetimeIndex(
        pd.to_datetime(fitted._frame.index, utc=True, errors="coerce")
    )
    if (
        frame_opens.hasnans
        or not frame_opens.is_monotonic_increasing
        or frame_opens.has_duplicates
    ):
        _fail("ATTEMPT_CUTOFF_REFIT_FRAME_INVALID")
    prefix_fit_end = pd.Timestamp(
        _utc(target_end, "PREFIX_END_INVALID")
    )
    frame_closes = pd.DatetimeIndex(
        pd.to_datetime(
            fitted._frame["_BAR_CLOSE_TS"],
            utc=True,
            errors="coerce",
        )
    )
    candidate_windows: dict[
        tuple[str, int], tuple[dict[str, Any], pd.Timestamp, int]
    ] = {}
    for candidate in candidates:
        if type(candidate) is not dict:
            _fail("HORIZON_CANDIDATE_SEMANTIC_IDENTITY_INVALID")
        identity = (candidate.get("lookback_kind"), candidate.get("lookback_value"))
        if (
            type(identity[0]) is not str
            or type(identity[1]) is not int
            or identity in candidate_windows
        ):
            _fail("HORIZON_CANDIDATE_SEMANTIC_IDENTITY_INVALID")
        try:
            expected_start, expected_count = _realize_cutoff_refit_window(
                opens=frame_opens,
                closes=frame_closes,
                fit_end=prefix_fit_end,
                lookback_kind=identity[0],
                lookback_value=identity[1],
                state_context_bars=0,
                boundary_limited=True,
            )
        except AuthoritativeSearchExecutionError as exc:
            raise AuthoritativeSearchExecutionError(
                "ATTEMPT_CUTOFF_REFIT_WINDOW_INVALID",
                (candidate.get("candidate_id"), exc.code),
            ) from exc
        candidate_windows[identity] = (candidate, expected_start, expected_count)

    verified_attempts: list[_VerifiedAttempt] = []
    mapped_plan_ids: list[str] = []
    for ordinal, raw in enumerate(attempt_artifacts):
        if type(raw) is not dict or set(raw) != _ATTEMPT_ARTIFACT_KEYS:
            _fail("ATTEMPT_ARTIFACT_SCHEMA_INVALID", ordinal)
        if any(type(raw[name]) is not dict for name in _ATTEMPT_ARTIFACT_KEYS):
            _fail("ATTEMPT_ARTIFACT_BODY_INVALID", ordinal)
        process = raw["winner_process"]
        horizon_process = raw["horizon_process"]
        recipe = raw["refit_recipe"]
        candidate_window = candidate_windows.get(
            (
                horizon_process.get("lookback_kind"),
                horizon_process.get("lookback_value"),
            )
        )
        if candidate_window is None:
            _fail("ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID", ordinal)
        candidate, expected_fit_start, source_bar_count = candidate_window
        fit_start = pd.Timestamp(
            _utc(horizon_process.get("fit_start_utc"), "ATTEMPT_FIT_START_INVALID")
        )
        _utc(horizon_process.get("fit_end_utc"), "ATTEMPT_FIT_END_INVALID")
        # ``candidate.realized_start_utc`` is historical-fold geometry (or a
        # pre-prefix boundary sentinel).  Current-refit artifacts instead use
        # the lookback realized once at the latest cutoff.  Bind that distinct
        # timestamp independently to the authenticated frame and prefix.
        if fit_start != expected_fit_start:
            _fail("ATTEMPT_CUTOFF_REFIT_WINDOW_INVALID", ordinal)
        if source_bar_count < 2:
            _fail("ATTEMPT_REALIZED_SOURCE_COUNT_INVALID", ordinal)
        try:
            verified = current_refit.verify_current_refit_search_artifacts_bytes_v2(
                winner_process_bytes=_canonical_bytes(process),
                horizon_process_bytes=_canonical_bytes(horizon_process),
                trial_definition_bytes=_canonical_bytes(raw["trial_definition"]),
                refit_recipe_bytes=_canonical_bytes(recipe),
                symbol=symbol,
                timeframe=timeframe,
                regime=normalized_regime,
                strategy_id=process.get("strategy_id"),
                lookback_kind=candidate["lookback_kind"],
                lookback_value=candidate["lookback_value"],
                fit_start_utc=horizon_process.get("fit_start_utc"),
                fit_end_utc=horizon_process.get("fit_end_utc"),
                latest_closed_bar_utc=horizon_process.get("latest_closed_bar_utc"),
                source_snapshot_sha256=horizon_process.get("source_snapshot_sha256"),
                source_bar_count=source_bar_count,
                seed=recipe.get("seed"),
            )
        except Exception as exc:
            raise AuthoritativeSearchExecutionError(
                "ATTEMPT_ARTIFACT_VERIFICATION_FAILED",
                (str(ordinal), type(exc).__name__, str(exc)),
            ) from exc
        horizon_body = verified.horizon_process
        if (
            _utc(horizon_body["fit_end_utc"], "ATTEMPT_FIT_END_INVALID")
            != _utc(target_end, "PREFIX_END_INVALID")
            or _utc(
                horizon_body["latest_closed_bar_utc"],
                "ATTEMPT_LATEST_CLOSE_INVALID",
            )
            != _utc(target_latest, "PREFIX_LAST_CLOSE_INVALID")
            or horizon_body["source_snapshot_sha256"] != target_snapshot
        ):
            _fail("ATTEMPT_NATIVE_PREFIX_SPLICE", ordinal)
        parameter_sha256 = _sha256(verified.trial_definition["parameters"])
        trial_matches = [
            item
            for item in trials
            if item.get("strategy_id") == verified.winner_process["strategy_id"]
            and item.get("strategy_release_sha256")
            == verified.winner_process["strategy_code_sha256"]
            and item.get("parameter_set_sha256") == parameter_sha256
        ]
        if len(trial_matches) != 1:
            _fail("ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID", ordinal)
        trial = trial_matches[0]
        planned = attempt_by_pair.get((candidate["candidate_id"], trial["trial_id"]))
        if (
            planned is None
            or planned.get("attempt_id") in mapped_plan_ids
            or planned.get("strategy_id") != verified.winner_process["strategy_id"]
            or planned.get("status") not in {"PLANNED", "PROCESS_REJECTED"}
        ):
            _fail("ATTEMPT_HORIZON_PLAN_MAPPING_INVALID", ordinal)
        _verify_trial_state_context_disposition(
            plan_body=plan_body,
            candidate=candidate,
            trial=trial,
            planned=planned,
            ordinal=ordinal,
        )
        mapped_plan_ids.append(planned["attempt_id"])
        verified_attempts.append(
            _VerifiedAttempt(
                ordinal=ordinal,
                plan_attempt=planned,
                candidate=candidate,
                trial=trial,
                artifacts=verified,
            )
        )
    if mapped_plan_ids != [item.get("attempt_id") for item in planned_attempts]:
        _fail("ATTEMPT_HORIZON_ORDER_OR_COVERAGE_INVALID")
    return plan_body, tuple(verified_attempts)


def _winner_fold_plan(
    verified_attempts: Sequence[_VerifiedAttempt],
) -> tuple[dict[str, Any], ...]:
    reference: list[tuple[str, str]] | None = None
    for attempt in verified_attempts:
        if attempt.plan_attempt["status"] != "PLANNED":
            continue
        folds = attempt.candidate.get("folds")
        if type(folds) is not list:
            _fail("CANDIDATE_FOLD_ROSTER_INVALID", attempt.ordinal)
        axes: list[tuple[str, str]] = []
        prior_end_exclusive: datetime | None = None
        for fold in folds:
            if (
                type(fold) is not dict
                or fold.get("role") != "CAUSAL_ROLLING_ORIGIN_INNER"
            ):
                continue
            start = _utc(fold.get("validation_start_utc"), "FOLD_START_INVALID")
            end = _utc(
                fold.get("validation_end_exclusive_utc"),
                "FOLD_END_INVALID",
            )
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if (
                start_dt.time() != datetime.min.time()
                or end_dt.time() != datetime.min.time()
                or end_dt <= start_dt
            ):
                _fail("WINNER_FOLD_REQUIRES_WHOLE_UTC_DAYS")
            days = (end_dt.date() - start_dt.date()).days
            if not MIN_DAYS_PER_FOLD <= days <= MAX_DAYS_PER_FOLD:
                _fail("WINNER_FOLD_DATE_SPAN_INVALID", attempt.ordinal, days)
            if prior_end_exclusive is not None and start_dt < prior_end_exclusive:
                _fail("WINNER_FOLD_INTERVALS_OVERLAP", attempt.ordinal)
            prior_end_exclusive = end_dt
            axes.append((start, end))
        if not axes:
            _fail("PLANNED_ATTEMPT_HAS_NO_CAUSAL_FOLDS", attempt.ordinal)
        if reference is None:
            reference = axes
        elif axes != reference:
            _fail("PLANNED_ATTEMPT_CAUSAL_FOLD_AXES_DRIFT", attempt.ordinal)
    if reference is None:
        return ()
    if not MIN_FOLDS <= len(reference) <= MAX_FOLDS:
        _fail("WINNER_FOLD_COUNT_INVALID")
    return tuple(
        {
            "anchor_as_of_utc": start,
            "oos_start_date_utc": datetime.fromisoformat(start.replace("Z", "+00:00"))
            .date()
            .isoformat(),
            "oos_end_date_utc": (
                datetime.fromisoformat(end.replace("Z", "+00:00")).date()
                - timedelta(days=1)
            ).isoformat(),
        }
        for start, end in reference
    )


def _precommitted_evidence_roles(
    verified_attempts: Sequence[_VerifiedAttempt],
) -> tuple[str, ...]:
    """Derive Winner evidence roles from the verified sealed Horizon roster."""

    roles: list[str] = []
    for attempt in verified_attempts:
        status = attempt.plan_attempt.get("status")
        if status == "PLANNED":
            roles.append("EVALUATION_REQUIRED")
        elif status == "PROCESS_REJECTED":
            roles.append("PRESEARCH_PROCESS_REJECTED")
        else:  # _verify_attempt_roster normally rejects this first.
            _fail("ATTEMPT_PRECOMMITTED_EVIDENCE_ROLE_INVALID", attempt.ordinal)
    return tuple(roles)


def _causal_folds(attempt: _VerifiedAttempt) -> tuple[dict[str, Any], ...]:
    folds = attempt.candidate.get("folds")
    if type(folds) is not list:
        _fail("CANDIDATE_FOLD_ROSTER_INVALID", attempt.ordinal)
    result = tuple(
        dict(value)
        for value in folds
        if type(value) is dict and value.get("role") == "CAUSAL_ROLLING_ORIGIN_INNER"
    )
    if attempt.plan_attempt["status"] == "PLANNED" and not result:
        _fail("PLANNED_ATTEMPT_HAS_NO_CAUSAL_FOLDS", attempt.ordinal)
    return result


def _partition_slice(
    frame: pd.DataFrame,
    *,
    role: str,
    frame_start_utc: str,
    scored_start_utc: str,
    end_exclusive_utc: str,
    minimum_warmup_bars: int,
    _full_frame_time_axes: tuple[pd.DatetimeIndex, pd.DatetimeIndex] | None = None,
) -> _PartitionSlice:
    start = pd.Timestamp(_utc(frame_start_utc, "PARTITION_FRAME_START_INVALID"))
    scored = pd.Timestamp(_utc(scored_start_utc, "PARTITION_SCORE_START_INVALID"))
    end = pd.Timestamp(_utc(end_exclusive_utc, "PARTITION_END_INVALID"))
    if not start <= scored < end:
        _fail("PARTITION_INTERVAL_INVALID", role)
    if _full_frame_time_axes is None:
        opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
        closes = pd.DatetimeIndex(
            pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        )
    else:
        opens, closes = _full_frame_time_axes
    mask = (opens >= start) & (opens < end) & (closes <= end)
    selected = frame.loc[mask].copy(deep=True)
    if selected.empty:
        _fail("PARTITION_FRAME_EMPTY", role)
    selected_opens = pd.DatetimeIndex(selected.index)
    selected_closes = pd.DatetimeIndex(
        pd.to_datetime(selected["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    warmup_bars = int(selected_opens.searchsorted(scored, side="left"))
    if (
        warmup_bars >= len(selected)
        or selected_opens[warmup_bars] != scored
        or selected_closes[-1] != end
        or warmup_bars < minimum_warmup_bars
        or len(selected) - warmup_bars < MIN_PARTITION_SCORED_BARS
    ):
        _fail("PARTITION_AXIS_NOT_EXACTLY_BACKED", role)
    required = {"Open", "High", "Low", "Close", "_BAR_CLOSE_TS", "REGIME_LIVE"}
    if not required.issubset(selected.columns):
        _fail("PARTITION_REQUIRED_COLUMNS_MISSING", role)
    if selected[["Open", "High", "Low", "Close"]].isna().any().any():
        _fail("PARTITION_OHLC_INVALID", role)
    start_text = _utc(start.isoformat(), "PARTITION_FRAME_START_INVALID")
    scored_text = _utc(scored.isoformat(), "PARTITION_SCORE_START_INVALID")
    end_text = _utc(end.isoformat(), "PARTITION_END_INVALID")
    geometry = _receipt(
        PARTITION_GEOMETRY_VERSION,
        {
            "role": role,
            "frame_start_utc": start_text,
            "scored_start_utc": scored_text,
            "end_exclusive_utc": end_text,
            "warmup_bars": warmup_bars,
            "scored_bars": len(selected) - warmup_bars,
            "frame_rows": len(selected),
            "frame_sha256": _frame_sha256(selected),
        },
    )
    return _PartitionSlice(
        role=role,
        frame=selected,
        warmup_bars=warmup_bars,
        scored_start_utc=scored_text,
        end_exclusive_utc=end_text,
        geometry_receipt=geometry,
    )


def minimum_fold_fit_rows(
    *,
    recipe_warmup_bars: int,
    dependency_warmup_bars: int,
) -> int:
    """Return the shared train-plus-tuning fit-adequacy floor."""

    if type(recipe_warmup_bars) is not int or recipe_warmup_bars < 0:
        _fail("RECIPE_WARMUP_INVALID")
    if type(dependency_warmup_bars) is not int or dependency_warmup_bars < 0:
        _fail("DEPENDENCY_WARMUP_INVALID")
    return (
        2 * max(recipe_warmup_bars, dependency_warmup_bars)
        + 2 * MIN_PARTITION_SCORED_BARS
    )


def _fold_partitions(
    *,
    frame: pd.DataFrame,
    fold: Mapping[str, Any],
    attempt: _VerifiedAttempt,
    dependency_warmup_bars: int,
) -> tuple[_PartitionSlice, _PartitionSlice, _PartitionSlice]:
    recipe = attempt.artifacts.refit_recipe
    recipe_warmup = int(recipe["data_requirements"]["warmup_bars"])
    fit_start = pd.Timestamp(_utc(fold.get("fit_start_utc"), "FIT_START_INVALID"))
    fit_end = pd.Timestamp(_utc(fold.get("fit_end_exclusive_utc"), "FIT_END_INVALID"))
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    full_frame_time_axes = opens, closes
    fit_mask = (opens >= fit_start) & (opens < fit_end) & (closes <= fit_end)
    fit_frame = frame.loc[fit_mask]
    if fit_frame.empty or pd.Timestamp(fit_frame["_BAR_CLOSE_TS"].iloc[-1]) != fit_end:
        _fail("FIT_WINDOW_NOT_EXACTLY_BACKED", attempt.ordinal)
    if type(dependency_warmup_bars) is not int or dependency_warmup_bars < 0:
        _fail("DEPENDENCY_WARMUP_INVALID", attempt.ordinal)
    minimum_warmup = max(recipe_warmup, dependency_warmup_bars)
    row_count = len(fit_frame)
    minimum_required = minimum_fold_fit_rows(
        recipe_warmup_bars=recipe_warmup,
        dependency_warmup_bars=dependency_warmup_bars,
    )
    if row_count < minimum_required:
        _fail("FIT_WINDOW_TOO_SHORT_FOR_TRAIN_TUNING", attempt.ordinal)
    tuning_scored = max(MIN_PARTITION_SCORED_BARS, int(math.ceil(row_count * 0.25)))
    split_index = row_count - tuning_scored
    if split_index - minimum_warmup < MIN_PARTITION_SCORED_BARS:
        split_index = minimum_warmup + MIN_PARTITION_SCORED_BARS
    if row_count - split_index < MIN_PARTITION_SCORED_BARS:
        _fail("FIT_SPLIT_GEOMETRY_INVALID", attempt.ordinal)
    fit_opens = pd.DatetimeIndex(fit_frame.index)
    fit_closes = pd.DatetimeIndex(
        pd.to_datetime(fit_frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    train_score_start = fit_opens[minimum_warmup]
    train_end = fit_closes[split_index - 1]
    tuning_score_start = fit_opens[split_index]
    tuning_frame_start = fit_opens[split_index - minimum_warmup]

    train = _partition_slice(
        frame,
        role="optimizer_train",
        frame_start_utc=fit_start.isoformat(),
        scored_start_utc=train_score_start.isoformat(),
        end_exclusive_utc=train_end.isoformat(),
        minimum_warmup_bars=minimum_warmup,
        _full_frame_time_axes=full_frame_time_axes,
    )
    tuning = _partition_slice(
        frame,
        role="tuning_validation",
        frame_start_utc=tuning_frame_start.isoformat(),
        scored_start_utc=tuning_score_start.isoformat(),
        end_exclusive_utc=fit_end.isoformat(),
        minimum_warmup_bars=minimum_warmup,
        _full_frame_time_axes=full_frame_time_axes,
    )
    final = _partition_slice(
        frame,
        role="final_lockbox",
        frame_start_utc=fold.get("validation_warmup_start_utc"),
        scored_start_utc=fold.get("validation_start_utc"),
        end_exclusive_utc=fold.get("validation_end_exclusive_utc"),
        minimum_warmup_bars=minimum_warmup,
        _full_frame_time_axes=full_frame_time_axes,
    )
    return train, tuning, final


def _winner_fold_identity(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    fold_ordinal: int,
    fold: Mapping[str, Any],
) -> str:
    """Reproduce the frozen Winner fold identity before raw evaluation."""

    anchor = _utc(fold.get("anchor_as_of_utc"), "FOLD_ANCHOR_INVALID")
    start = datetime.fromisoformat(
        _utc(
            f"{fold.get('oos_start_date_utc')}T00:00:00Z",
            "FOLD_START_INVALID",
        ).replace("Z", "+00:00")
    ).date()
    end = datetime.fromisoformat(
        _utc(
            f"{fold.get('oos_end_date_utc')}T00:00:00Z",
            "FOLD_END_INVALID",
        ).replace("Z", "+00:00")
    ).date()
    body = {
        "schema_version": "fxpm.winner-fold-identity.v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "fold_ordinal": fold_ordinal,
        "anchor_as_of_utc": anchor,
        "oos_start_date_utc": start.isoformat(),
        "oos_end_date_utc": end.isoformat(),
    }
    return hashlib.sha1(_canonical_bytes(body), usedforsecurity=False).hexdigest()


def _source_observation_receipt(fitted: FittedNativeSearchTimeframe,
        target: pd.Index) -> dict[str, Any]:
    fine = fitted._spread_source_product
    frame = fitted._spread_source_frame
    if (type(fine) is not native_prefix.NativePrefixProduct or not isinstance(frame, pd.DataFrame)
            or not isinstance(fitted._spread_tiers, Mapping)):
        _fail("FROZEN_TIER_PRIOR_FINE_SOURCE_UNAVAILABLE")
    tier = fitted._spread_tiers["P75"]
    observations, actual_closes = spread_profile.align_prior_completed_spreads(
        target, frame, tier.source_timeframe, include_matched_closes=True)
    rows = []
    for opened, closed, points in zip(target, actual_closes, observations, strict=True):
        opened_text = _utc(pd.Timestamp(opened).isoformat(), "SPREAD_TARGET_OPEN_INVALID")
        rows.append({"target_open_utc": opened_text,
            "expected_prior_fine_close_utc": opened_text,
            "prior_fine_close_utc": None if pd.isna(closed) else _utc(pd.Timestamp(closed).isoformat(), "SPREAD_SOURCE_CLOSE_INVALID"),
            "points": None if pd.isna(points) else float(points)})
    return _receipt("fxpm.original-fine-spread-observations.v1", {
        "symbol": tier.symbol, "timeframe": tier.timeframe, "source_timeframe": tier.source_timeframe,
        "source_product": {"data_dir": str(fine._data_dir),
            "index_descriptor": dict(fine.index_descriptor), "index_sha256": fine.index_sha256,
            "symbol": fine.index["symbol"], "timeframe": fine.index["timeframe"],
            "cutoff_utc": fine.index["cutoff_utc"],
            "activation_manifest_sha256": fine.index["activation"]["manifest_sha256"],
            "production_contract_required": fine._production_contract_required},
        "observations": rows,
        "entry_observations_sha256": hashlib.sha256(
            pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest(),
    })


def _partition_cost_precommitments(
    *, attempts: Sequence[_VerifiedAttempt], fold_plan: Sequence[Mapping[str, Any]],
    plan_body: Mapping[str, Any], fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
    source_observation_cache: dict[str, tuple[str, bytes]] | None = None,
    _prepared_inputs: dict | None = None,
) -> dict[str, Any]:
    """Freeze each real partition's two costs/admissions before any signal outcome."""
    geometry = plan_body.get("derived_dependency_geometry")
    if not isinstance(geometry, Mapping) or type(geometry.get("warmup_bars")) is not int:
        _fail("PARTITION_COST_PRECOMMIT_CONTEXT_INVALID")
    if (not isinstance(fitted._spread_tiers, Mapping)
            or set(fitted._spread_tiers) != {"P75", "P95"}
            or not isinstance(fitted._entry_spread_observations, pd.Series)
            or not isinstance(fitted.frozen_tier_pair_contract, Mapping)):
        _fail("FROZEN_TIER_FITTED_INPUTS_UNAVAILABLE")
    backtester = Backtester(config)
    cache: dict[str, dict[str, Any]] = {}
    fold_summaries: dict[tuple[Any, ...], tuple[dict[str, Any], ...]] = {}
    source_cache = {} if source_observation_cache is None else source_observation_cache
    rows = []
    for attempt in attempts:
        if attempt.plan_attempt.get("status") != "PLANNED":
            continue
        causal_folds = _causal_folds(attempt)
        if len(causal_folds) != len(fold_plan):
            _fail("PARTITION_COST_PRECOMMIT_FOLD_ROSTER_INVALID", attempt.ordinal)
        recipe = getattr(attempt.artifacts, "refit_recipe", None)
        requirements = recipe.get("data_requirements") if isinstance(recipe, Mapping) else None
        recipe_warmup = requirements.get("warmup_bars") if isinstance(requirements, Mapping) else None
        for fold_ordinal, fold in enumerate(causal_folds):
            bounds = tuple(fold.get(name) for name in (
                "fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc",
                "validation_start_utc", "validation_end_exclusive_utc"))
            # Only exact input types may share successful geometry. Other inputs
            # still reach the original fold validation/conversion without reuse.
            key = ((*bounds, recipe_warmup, geometry["warmup_bars"])
                if all(type(value) is str for value in bounds)
                and type(recipe_warmup) is int and type(geometry["warmup_bars"]) is int else None)
            summaries = fold_summaries.get(key) if key is not None else None
            if summaries is not None:
                rows.extend({"attempt_ordinal": attempt.ordinal, "fold_ordinal": fold_ordinal,
                    **summary} for summary in summaries)
                continue
            first_row = len(rows)
            partitions = _fold_partitions(frame=fitted._frame, fold=fold, attempt=attempt,
                dependency_warmup_bars=geometry["warmup_bars"])
            for partition in partitions:
                identity = partition.geometry_receipt["receipt_sha256"]
                tiers = cache.get(identity)
                if tiers is None:
                    observations = fitted._entry_spread_observations.loc[partition.frame.index]
                    prepared = ({tier: {} for tier in fitted._spread_tiers}
                        if _prepared_inputs is not None else None)
                    probes = {tier: backtester.build_evidence_source_seed_result(
                        partition.frame, plan_body["cell"]["symbol"],
                        spec=fitted.instrument_spec, timeframe=plan_body["cell"]["timeframe"],
                        spread_tier=bound_tier, entry_spread_observations=observations,
                        **({"_prepared_inputs_out": prepared[tier]} if _prepared_inputs is not None else {}),
                    ) for tier, bound_tier in fitted._spread_tiers.items()}
                    if _prepared_inputs is not None:
                        _prepared_inputs[identity] = prepared
                    pair = pm_core.build_backtest_frozen_tier_pair_contract(
                        probes, spec=fitted.instrument_spec, use_spread=bool(config.use_spread))
                    if pair != dict(fitted.frozen_tier_pair_contract):
                        _fail("PARTITION_FROZEN_TIER_POLICY_DRIFT")
                    tiers = {tier: {
                        "numerical_execution_cost_attestation": probe["numerical_execution_cost_attestation"],
                        "entry_observations_sha256": probe["spread_admission"]["entry_observations_sha256"],
                        "entry_admission_mask_sha256": probe["spread_admission"]["entry_admission_mask_sha256"],
                    } for tier, probe in probes.items()}
                    cache[identity] = tiers
                source_receipt = source_cache.get(identity)
                if source_receipt is None:
                    body = _source_observation_receipt(fitted, partition.frame.index)
                    source_receipt = (body["receipt_sha256"], _canonical_bytes(body))
                    source_cache[identity] = source_receipt
                rows.append({"attempt_ordinal": attempt.ordinal, "fold_ordinal": fold_ordinal,
                    "partition": partition.role,
                    "partition_geometry_receipt_sha256": identity,
                    "partition_frame_sha256": partition.geometry_receipt["frame_sha256"],
                    "source_observation_receipt_sha256": source_receipt[0],
                    "tiers": tiers})
            if key is not None:
                # Retain metadata only; strategy frames/working copies never
                # enter this call-local cache or escape through its summaries.
                fold_summaries[key] = tuple({name: value for name, value in row.items()
                    if name not in {"attempt_ordinal", "fold_ordinal"}}
                    for row in rows[first_row:])
    try:
        return raw_winner.build_partition_cost_precommitments(rows,
            pair=fitted.frozen_tier_pair_contract)
    except raw_winner.WinnerRawEvidenceError as exc:
        raise AuthoritativeSearchExecutionError(exc.code) from exc


def _publish_source_observation_references(sink, *, frozen, source_observation_cache):
    references = {}
    published = {}
    while source_observation_cache:
        geometry = next(iter(source_observation_cache))
        logical_sha, encoded = source_observation_cache[geometry]
        physical_sha = hashlib.sha256(encoded).hexdigest()
        reference = published.get(physical_sha)
        if reference is None:
            receipt = json.loads(encoded.decode("ascii"))
            if receipt.get("receipt_sha256") != logical_sha:
                _fail("SOURCE_OBSERVATION_CACHE_BINDING_MISMATCH")
            reference = raw_winner.publish_source_observation_receipt(sink, frozen=frozen, receipt=receipt)
            published[physical_sha] = reference
        if (reference.get("source_observation_receipt_sha256") != logical_sha
                or reference["artifact_entry"]["descriptor"].get("sha256") != physical_sha):
            _fail("SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH")
        references[geometry] = reference
        del source_observation_cache[geometry]
    return references


def _require_source_observation_references(value):
    if not isinstance(value, Mapping) or not value:
        _fail("SOURCE_OBSERVATION_REFERENCE_SET_REQUIRED")
    return value


def _index_partition_precommitment_rows(precommitments):
    """Index already private rows once; keep duplicate cardinality for existing refusals."""
    try:
        rows = precommitments.get("rows")
        if type(rows) is not list:
            return None
        indexed = {}
        for row in rows:
            key = row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition")
            indexed.setdefault(key, []).append(row)
    except (AttributeError, TypeError):
        return None
    return {key: tuple(values) for key, values in indexed.items()}


def _partition_source_observation_reference(*, references, precommitments, pair,
        attempt, fold_ordinal, partition, _partition_row_lookup=None):
    geometry = partition.geometry_receipt
    reference = references.get(geometry["receipt_sha256"])
    if _partition_row_lookup is None:
        rows = precommitments.get("rows")
        matches = [row for row in rows if (row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition"))
            == (attempt.ordinal, fold_ordinal, partition.role)] if type(rows) is list else []
    else:
        matches = _partition_row_lookup.get((attempt.ordinal, fold_ordinal, partition.role), ())
    if not isinstance(reference, Mapping) or len(matches) != 1:
        _fail("SOURCE_OBSERVATION_REFERENCE_GEOMETRY_MISMATCH")
    try:
        row = raw_winner.expand_partition_cost_precommitment(precommitments, matches[0], pair=pair)
    except raw_winner.WinnerRawEvidenceError as exc:
        raise AuthoritativeSearchExecutionError(exc.code) from exc
    if (row.get("partition_geometry_receipt_sha256") != geometry["receipt_sha256"]
            or row.get("partition_frame_sha256") != geometry["frame_sha256"]
            or reference.get("source_observation_receipt_sha256") != row.get("source_observation_receipt_sha256")):
        _fail("SOURCE_OBSERVATION_REFERENCE_BINDING_MISMATCH")
    return reference


def _verify_partition_precommitment(
    *, precommitments: Mapping[str, Any], attempt: _VerifiedAttempt,
    fold_ordinal: int, partition: _PartitionSlice, tier: str,
    result: Mapping[str, Any], pair: Mapping[str, Any],
    _partition_row_lookup=None,
) -> None:
    if _partition_row_lookup is None:
        rows = precommitments.get("rows")
        matches = [row for row in rows if (row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition"))
            == (attempt.ordinal, fold_ordinal, partition.role)] if type(rows) is list else []
    else:
        matches = _partition_row_lookup.get((attempt.ordinal, fold_ordinal, partition.role), ())
    if len(matches) != 1:
        _fail("PARTITION_COST_PRECOMMIT_ROW_UNAVAILABLE")
    try:
        row = raw_winner.expand_partition_cost_precommitment(precommitments, matches[0], pair=pair)
    except raw_winner.WinnerRawEvidenceError as exc:
        raise AuthoritativeSearchExecutionError(exc.code) from exc
    admission = result.get("spread_admission")
    expected = row.get("tiers", {}).get(tier)
    observed = {"numerical_execution_cost_attestation": result.get("numerical_execution_cost_attestation"),
        "entry_observations_sha256": admission.get("entry_observations_sha256") if isinstance(admission, Mapping) else None,
        "entry_admission_mask_sha256": admission.get("entry_admission_mask_sha256") if isinstance(admission, Mapping) else None}
    if (row.get("partition_geometry_receipt_sha256") != partition.geometry_receipt["receipt_sha256"]
            or row.get("partition_frame_sha256") != partition.geometry_receipt["frame_sha256"]
            or observed != expected):
        _fail("PARTITION_COST_PRECOMMIT_RESULT_MISMATCH", attempt.ordinal, fold_ordinal, partition.role, tier)


def _independent_tier_execution_receipt(lanes: Mapping[str, Mapping[str, Any]],
        pair_contract: Mapping[str, Any]) -> dict[str, Any]:
    if lanes and all(value.get("schema_version") == "fxpm.independent-signal-partition.v1" for value in lanes.values()):
        return raw_winner.build_independent_tier_execution_receipt(lanes, _sha256(pair_contract))
    rows = {}
    for name, lane in lanes.items():
        admission = lane["spread_admission"]
        rows[name] = {"trade_identity_pnl_sha256": _sha256(raw_winner._trade_identity_pnl_evidence(lane, lane=name)),
            "position_events_sha256": _sha256(lane["position_events"]),
            "partition_axis_sha256": _sha256(lane["trade_power_partition_axis"]),
            "numerical_execution_cost_attestation_sha256": _sha256(lane["numerical_execution_cost_attestation"]),
            "entry_observations_sha256": admission["entry_observations_sha256"],
            "entry_admission_mask_sha256": admission["entry_admission_mask_sha256"]}
    return _receipt("fxpm.independent-frozen-tier-execution.v1", {
        "frozen_tier_pair_contract_sha256": _sha256(pair_contract), "lanes": rows,
    })


def _runtime_strategy(
    attempt: _VerifiedAttempt,
    *, timeframe=None,
) -> tuple[Any, dict[str, Any]]:
    strategy_id = attempt.artifacts.winner_process["strategy_id"]
    base = StrategyRegistry.get(strategy_id)
    try:
        defaults, _grid, _coupled = resolve_strategy_parameters(base, timeframe,
            defaults_only=True, persisted_parameters=attempt.artifacts.trial_definition["parameters"])
        parameters = decode_parameter_mapping(
            attempt.artifacts.trial_definition["parameters"],
            defaults,
        )
        strategy = StrategyRegistry.get(strategy_id, **parameters)
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "STRATEGY_MATERIALIZATION_FAILED",
            (str(attempt.ordinal), strategy_id, type(exc).__name__, str(exc)),
        ) from exc
    return strategy, parameters


def _verify_result_cost_attestation(
    *,
    backtester: Backtester,
    result: Mapping[str, Any],
    features: pd.DataFrame,
    spec: InstrumentSpec,
    spread_tier: spread_profile.FrozenSpreadTier,
    timeframe: str,
) -> None:
    try:
        entry, intrabar, meta = backtester._build_half_spread_arrays(
            features, spec, spread_tier=spread_tier, timeframe=timeframe,
        )
        rebuilt = backtester._numerical_execution_cost_attestation(
            entry_half_spread=entry,
            intrabar_half_spread=intrabar,
            spread_meta=meta,
            spec=spec,
        )
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "BACKTEST_COST_ATTESTATION_REBUILD_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if result.get("numerical_execution_cost_attestation") != rebuilt:
        _fail("BACKTEST_COST_ATTESTATION_MISMATCH")


def _decision_relevant_result(result: Mapping[str, Any], role: str) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        _fail("BACKTEST_RESULT_INVALID", role)
    body = {
        name: result.get(name)
        for name in _RAW_LANE_KEYS
        if name != "trade_identity_pnl"
    }
    try:
        body["trade_identity_pnl"] = build_backtest_trade_identity_pnl_evidence(
            result.get("trades")
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise AuthoritativeSearchExecutionError(
            "BACKTEST_TRADE_IDENTITY_PNL_INVALID", (role,)
        ) from exc
    if (
        type(body["trade_identity_pnl"]) is not list
        or type(body["position_events"]) is not list
        or type(body["winner_daily_r_surface"]) is not dict
        or body["winner_daily_r_surface"].get("status") != "AVAILABLE"
        or type(body["trade_power_partition_axis"]) is not dict
        or type(body["execution_cost_timing_contract"]) is not dict
        or type(body["numerical_execution_cost_attestation"]) is not dict
        or type(body["swap_accrual_contract"]) is not dict
        or type(body["spread_model"]) is not dict
        or type(body["spread_admission"]) is not dict
    ):
        _fail("BACKTEST_DECISION_RELEVANT_RESULT_INCOMPLETE", role)
    return json.loads(_canonical_bytes(body).decode("ascii"))


def _compute_fitted_strategy_features(*, frame, strategy, fitted, symbol, timeframe):
    """Use the held authenticated fit recipe when full features are requested."""
    def prepare(recipe, compute):
        owner, identity = getattr(fitted, "_numeric_preparation", None), getattr(fitted, "_numeric_key", None)
        if owner is None or identity is None:
            return compute()
        return owner.prepare(("features", identity, tuple(frame.columns), recipe), frame, compute,
            source_columns=fitted._numeric_source_columns)

    def full_features():
        parameters = dict(fitted.regime_parameters)
        parameters_sha256 = fitted.regime_fit_decision.to_dict()["regime_params_sha256"]
        return prepare(("all", _canonical_bytes(parameters), parameters_sha256),
            lambda: FeatureComputer.compute_all(frame, symbol=symbol, timeframe=timeframe,
                regime_params_override=parameters, regime_params_sha256=parameters_sha256))

    if strategy is None:
        return full_features()
    request = set()
    if hasattr(strategy, "get_feature_request"):
        request.update(set(strategy.get_feature_request() or set()))
    elif hasattr(strategy, "get_required_features"):
        request.update(set(strategy.get_required_features() or set()))
        if hasattr(strategy, "calculate_stops"):
            request.add("ATR_14")
    if not request:
        return frame.copy()
    if request & FeatureComputer._REGIME_FEATURE_COLUMNS:
        return full_features()
    return prepare(("required", tuple(sorted(request))),
        lambda: FeatureComputer.compute_required(frame, request, symbol=symbol, timeframe=timeframe))


def _build_full_prefix_strategy_surface(
    *,
    frame: pd.DataFrame,
    strategy: Any,
    fitted: FittedNativeSearchTimeframe,
    symbol: str,
    timeframe: str,
) -> _FullPrefixStrategySurface:
    """Replay path state once over the authenticated prefix before slicing."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        _fail("FULL_PREFIX_FRAME_INVALID")
    try:
        features = _compute_fitted_strategy_features(
            frame=frame, strategy=strategy, fitted=fitted,
            symbol=symbol, timeframe=timeframe)
        if (
            not isinstance(features, pd.DataFrame)
            or not features.index.equals(frame.index)
            or not set(frame.columns).issubset(features.columns)
            or not features.loc[:, list(frame.columns)].equals(frame)
        ):
            _fail("FULL_PREFIX_FEATURE_AXIS_CHANGED")
        raw_signals = strategy.generate_signals(features, symbol)
        if not isinstance(raw_signals, pd.Series) or not raw_signals.index.equals(
            features.index
        ):
            _fail("FULL_PREFIX_SIGNAL_AXIS_INVALID")
        numeric = pd.to_numeric(raw_signals, errors="coerce")
        if numeric.isna().any() or bool((~numeric.isin([-1, 0, 1])).any()):
            _fail("FULL_PREFIX_SIGNAL_VALUES_INVALID")
    except AuthoritativeSearchExecutionError:
        raise
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "FULL_PREFIX_STATE_REPLAY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    return _FullPrefixStrategySurface(
        features=features,
        raw_signals=numeric.astype(np.int32),
    )


def _slice_full_prefix_strategy_surface(
    *,
    partition: _PartitionSlice,
    full_prefix_surface: _FullPrefixStrategySurface,
) -> tuple[pd.DataFrame, pd.Series]:
    if type(full_prefix_surface) is not _FullPrefixStrategySurface:
        _fail("FULL_PREFIX_SURFACE_INVALID", partition.role)
    try:
        features = full_prefix_surface.features.loc[partition.frame.index].copy(
            deep=True
        )
        raw_signals = full_prefix_surface.raw_signals.loc[partition.frame.index].copy(
            deep=True
        )
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "FULL_PREFIX_PARTITION_SLICE_INVALID",
            (partition.role, type(exc).__name__, str(exc)),
        ) from exc
    if (
        not features.index.equals(partition.frame.index)
        or not raw_signals.index.equals(partition.frame.index)
        or not set(partition.frame.columns).issubset(features.columns)
        or not features.loc[:, list(partition.frame.columns)].equals(partition.frame)
    ):
        _fail("FULL_PREFIX_PARTITION_SLICE_INVALID", partition.role)
    return features, raw_signals


def _run_tier_partition(
    *,
    partition: _PartitionSlice,
    full_prefix_surface: _FullPrefixStrategySurface,
    backtester: Backtester,
    strategy: Any,
    symbol: str,
    timeframe: str,
    regime: str,
    config: PipelineConfig,
    spec: InstrumentSpec,
    expected_source_contract_sha256: str,
    spread_tier: spread_profile.FrozenSpreadTier,
    entry_spread_observations: pd.Series,
    decision_result_out: dict[str, Any] | None = None,
    _entry_admission_preparation: Any = None,
    _matched_base_facts_out: dict[str, Any] | None = None,
    _matched_stress_tier: spread_profile.FrozenSpreadTier | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.Series, Backtester]:
    try:
        features, raw_signals = _slice_full_prefix_strategy_surface(
            partition=partition,
            full_prefix_surface=full_prefix_surface,
        )
        labels = features["REGIME_LIVE"]
        if labels.isna().iloc[partition.warmup_bars :].any():
            _fail("REGIME_SIGNAL_SURFACE_MISSING", partition.role)
        signals = raw_signals.where(labels == regime, 0).astype(np.int32)
        admission_scope = (
            backtester._partition_entry_admission_reuse(
                _entry_admission_preparation
                if partition.role in {"optimizer_train", "tuning_validation"} else None)
            if _entry_admission_preparation is not None else nullcontext()
        )
        exact_trade_capture = {} if _matched_base_facts_out is not None else None
        with admission_scope:
            result = backtester.run(
                features,
                signals,
                symbol,
                strategy,
                spec=spec,
                timeframe=timeframe,
                warmup_bars=partition.warmup_bars,
                spread_tier=spread_tier,
                entry_spread_observations=entry_spread_observations.loc[features.index],
                **({"_exact_trade_facts_out": exact_trade_capture} if exact_trade_capture is not None else {}),
            )
    except AuthoritativeSearchExecutionError:
        raise
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "BACKTEST_PARTITION_EXECUTION_FAILED",
            (partition.role, type(exc).__name__, str(exc)),
        ) from exc
    _verify_result_cost_attestation(
        backtester=backtester,
        result=result,
        features=features,
        spec=spec, spread_tier=spread_tier, timeframe=timeframe,
    )
    axis = result.get("trade_power_partition_axis")
    if (
        not isinstance(axis, Mapping)
        or pd.Timestamp(axis.get("start_utc"))
        != pd.Timestamp(partition.scored_start_utc)
        or pd.Timestamp(axis.get("end_utc"))
        != pd.Timestamp(partition.end_exclusive_utc)
        or axis.get("warmup_bars") != partition.warmup_bars
    ):
        _fail("BACKTEST_PARTITION_AXIS_MISMATCH", partition.role)
    try:
        source_contract = raw_winner.build_trade_source_contract_from_backtester_result(
            result
        )
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "BACKTEST_SOURCE_CONTRACT_INVALID",
            (partition.role, type(exc).__name__, str(exc)),
        ) from exc
    if _source_identity(source_contract) != expected_source_contract_sha256:
        _fail("BACKTEST_SOURCE_CONTRACT_DRIFT", partition.role)
    if _matched_base_facts_out is not None:
        facts = _route_a_matched_base_trade_facts(
            result=result, features=features, backtester=backtester, spec=spec,
            base_tier=spread_tier, stress_tier=_matched_stress_tier,
            timeframe=timeframe, partition_role=partition.role, exact_trade_capture=exact_trade_capture, strategy=strategy)
        # This call omits governance/schedules: both fixed-control backends keep
        # the entry stop unchanged through ordinary and terminal trade emission.
        facts["initial_risk_source_binding"] = {
            "executor_release_sha256": AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
            "backtest_source_contract_sha256": expected_source_contract_sha256,
            "instrument_spec_sha256": _sha256(facts["instrument_spec"]),
            "caller": "pm_authoritative_search_execution._run_tier_partition",
            "backtester": "pm_core.Backtester.run",
            "governance_policy": None, "entry_schedule": None,
            "terminal_policy": "LIQUIDATE",
            "stop_provenance": "ORIGINAL_ENTRY_STOP_PRESERVED_BOTH_FIXED_CONTROL_BACKENDS",
        }
        for trade_ordinal, trade_facts in enumerate(facts["trades"]):
            trade_facts["actual_initial_risk_dollars"] = trade_facts["risk_at_recorded_stop_dollars"]
            trade_facts["initial_stop_status"] = "ORIGINAL_ENTRY_STOP_FIXED_CONTROL"
            try:
                components = _route_a_exact_base_return_components(trade_facts,
                    spec=spec, ordinal=trade_ordinal,
                    swap_model_enabled=facts["exact_swap_model_enabled"])
                trade_facts["base_return_components"] = components
                trade_facts["base_actual_initial_risk_net_r"] = components["canonical_net_r"]
            except (KeyError, TypeError, ValueError, IndexError, OverflowError, ZeroDivisionError):
                trade_facts["base_return_components"] = None
                trade_facts["base_actual_initial_risk_net_r"] = None
                facts["floor_reason_codes"].append("EXACT_EXECUTED_PRICE_SWAP_CAPTURE_MISMATCH")
            trade_facts["matched_p95_spread_cost_r_unrounded"] = None
            if trade_facts["base_fill_witness"]["status"] == "VERIFIED":
                sides = trade_facts["spread_sides"]["P95"]
                numerator = sides["entry_half_spread_at_entry"] + sides["intrabar_half_spread_at_exit"]
                distance = abs(trade_facts["entry_price"] - trade_facts["recorded_stop_price"])
                trade_facts["matched_p95_cost_inputs"] = {
                    "entry_half_price": sides["entry_half_spread_at_entry"],
                    "exit_intrabar_half_price": sides["intrabar_half_spread_at_exit"],
                    "numerator_price": numerator if math.isfinite(numerator) else None,
                    "original_initial_stop_distance_price": distance if math.isfinite(distance) else None}
                try:
                    ratio = numerator / distance
                    if not math.isfinite(ratio):
                        raise ValueError("nonfinite matched cost")
                    trade_facts["matched_p95_spread_cost_r_unrounded"] = ratio
                    trade_facts["matched_p95_spread_cost_r"] = pm_core.canonical_backtest_winner_r(numerator, distance)
                except (ValueError, ZeroDivisionError, OverflowError):
                    facts["floor_reason_codes"].append("MATCHED_COST_RATIO_INVALID")
        if not facts["trades"]:
            facts["floor_status"] = "UNAVAILABLE_NO_COMPLETED_BASE_TRADES"
        elif facts["floor_reason_codes"] or facts["base_fill_witness_status"] != "VERIFIED":
            facts["floor_status"] = "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH"
        else:
            facts["floor_status"] = "AVAILABLE_MATCHED_BASE_VERIFIED"
            facts["completed_position_ids"] = [row["base_position_event"]["position_id"] for row in facts["trades"]]
            for target, source in (("completed_position_open_utc", "entry_time"), ("completed_position_close_utc", "exit_time")):
                facts[target] = [pd.Timestamp(row["base_position_event"][source]).strftime("%Y-%m-%dT%H:%M:%SZ")
                    for row in facts["trades"]]
            for target, source in (("completed_position_open_event_order", "entry_event_order"),
                                   ("completed_position_close_event_order", "exit_event_order")):
                facts[target] = [row["base_position_event"][source] for row in facts["trades"]]
            facts["base_actual_initial_risk_net_r"] = [row["base_actual_initial_risk_net_r"] for row in facts["trades"]]
            facts["matched_p95_spread_cost_r"] = [row["matched_p95_spread_cost_r"] for row in facts["trades"]]
        facts["stress_cost_status"] = "PER_TRADE_MATCHED_PRICE_RATIO"
        facts["r_transport_normalization"] = "pm_core.canonical_backtest_winner_r"
        _matched_base_facts_out.update(facts)
    decision_result = _decision_relevant_result(result, partition.role)
    if decision_result_out is not None:
        decision_result_out.update(decision_result)
    return result, features, signals, backtester


def _fold_provenance(
    *,
    plan_body: Mapping[str, Any],
    plan_receipt_sha256: str,
    attempt: _VerifiedAttempt,
    fold: Mapping[str, Any],
    fold_ordinal: int,
    partitions: Sequence[_PartitionSlice],
    lanes: Mapping[str, Mapping[str, Any]],
    fitted: FittedNativeSearchTimeframe,
    independent_tier_execution_receipt: Mapping[str, Any],
    partition_signals: Mapping[str, pd.Series],
    source_observation_receipts: Mapping[str, Mapping[str, Any]] | None,
    source_observation_references: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    independent_r = bool(lanes) and all(value.get("schema_version") == "fxpm.independent-signal-partition.v1" for value in lanes.values())
    if source_observation_references is None:
        if source_observation_receipts is None:
            _fail("SOURCE_OBSERVATION_PROVENANCE_MISSING")
        provenance_version = PROVENANCE_RECEIPT_VERSION
        observation_transport = {"source_observation_receipts": dict(source_observation_receipts)}
    else:
        if (source_observation_receipts is not None
                or set(source_observation_references) != {partition.role for partition in partitions}):
            _fail("SOURCE_OBSERVATION_PROVENANCE_MIXED")
        provenance_version = raw_winner.REFERENCED_AUTHORITATIVE_PROVENANCE_SCHEMA
        observation_transport = {"source_observation_references": dict(source_observation_references)}
    if independent_r:
        provenance_version = "fxpm.authoritative-search-provenance.v6"
    artifacts = attempt.artifacts
    lane_hashes = {name: _sha256(dict(value)) for name, value in lanes.items()}
    common = {
        "authority_scope": "PRECOMMITTED_HISTORICAL_INNER_FOLD_ONLY",
        "anchor_id": plan_body["anchor"]["anchor_id"],
        "symbol": plan_body["cell"]["symbol"],
        "timeframe": plan_body["cell"]["timeframe"],
        "regime": plan_body["cell"]["regime"],
        "attempt_ordinal": attempt.ordinal,
        "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
        "fold_ordinal": fold_ordinal,
        "fold_id": fold["fold_id"],
        "confers_final_oos_or_trading_authority": False,
    }
    anchor_selection = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "anchor_selection_receipt",
            "process_precommit_receipt_sha256": plan_body["anchor"][
                "process_precommit_receipt_sha256"
            ],
            "horizon_plan_receipt_sha256": plan_receipt_sha256,
            "selection_cutoff_utc": plan_body["anchor"]["selection_cutoff_utc"],
        },
    )
    horizon_decision = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "anchor_horizon_decision",
            "candidate_id": attempt.candidate["candidate_id"],
            "candidate_definition_sha256": attempt.candidate[
                "candidate_definition_sha256"
            ],
            "horizon_process_sha256": artifacts.horizon_process_sha256,
            "fold_definition_sha256": fold["fold_definition_sha256"],
            "partition_geometry": [
                json.loads(
                    _canonical_bytes(dict(value.geometry_receipt)).decode("ascii")
                )
                for value in partitions
            ],
            "partition_geometry_receipts": [
                value.geometry_receipt["receipt_sha256"] for value in partitions
            ],
        },
    )
    trial_ledger = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "anchor_trial_ledger",
            "trial_registry_sha256": plan_body["trial_registry_sha256"],
            "attempt_roster_sha256": plan_body["attempt_roster_sha256"],
            "attempted_trials_exact": len(plan_body["attempts"]),
        },
    )
    selected_trial = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "anchor_selected_trial",
            "selection_scope": "PRECOMMITTED_ATTEMPT_NOT_POSTHOC_WINNER",
            "trial_id": attempt.trial["trial_id"],
            "trial_definition_sha256": attempt.trial["trial_definition_sha256"],
            "winner_process_sha256": artifacts.winner_process_sha256,
            "executable_trial_sha256": artifacts.trial_definition_sha256,
        },
    )
    refit_receipt = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "anchor_refit_receipt",
            "refit_recipe_sha256": artifacts.refit_recipe_sha256,
            "fit_start_utc": fold["fit_start_utc"],
            "fit_end_exclusive_utc": fold["fit_end_exclusive_utc"],
            "post_selection_search": False,
        },
    )
    signal_vector_receipts = {}
    for partition, signals in partition_signals.items():
        if not isinstance(signals, pd.Series) or not len(signals):
            _fail("PARTITION_SIGNAL_VECTOR_INVALID", attempt.ordinal, fold_ordinal, partition)
        numeric = pd.to_numeric(signals, errors="coerce")
        if numeric.isna().any() or bool((~numeric.isin([-1, 0, 1])).any()):
            _fail("PARTITION_SIGNAL_VECTOR_INVALID", attempt.ordinal, fold_ordinal, partition)
        values = numeric.astype(np.int32).tolist()
        digest = hashlib.sha256(np.asarray(values, dtype="<i4").tobytes(order="C")).hexdigest()
        signal_vector_receipts[partition] = _receipt(SIGNAL_VECTOR_VERSION, {
            "encoding": "little-endian-signed-int32", "observation_count": len(values),
            "values": values, "signal_vector_sha256": digest,
        })
    if set(signal_vector_receipts) != {"optimizer_train", "tuning_validation", "final_lockbox"}:
        _fail("PARTITION_SIGNAL_VECTOR_ROSTER_INVALID")
    replay_receipt = _receipt(provenance_version, {
        **common, "receipt_role": "attempt_replay_receipt", "lane_sha256": lane_hashes,
        "independent_tier_execution_receipt_sha256": independent_tier_execution_receipt["receipt_sha256"],
        "independent_tier_execution_receipt": json.loads(_canonical_bytes(dict(independent_tier_execution_receipt)).decode("ascii")),
        "signal_vector_receipts": signal_vector_receipts,
        **observation_transport,
        **({"genuine_independent_signal_r_capture": True} if independent_r
            else {"position_events_are_genuine_backtester_v2": True}),
    })
    source_decision = fitted.source_group_decision.to_dict()
    oos_source = _receipt(
        provenance_version,
        {
            **common,
            "receipt_role": "oos_source_receipt",
            "native_prefix_index_sha256": fitted.product.index_sha256,
            "native_prefix_content_sha256": fitted.product.native_prefix[
                "prefix_content_sha256"
            ],
            "source_contract_sha256": source_decision["source_contract_sha256"],
            "native_source_sha256": source_decision["native_source_sha256"],
            "activation_manifest_sha256": source_decision["activation_manifest_sha256"],
            "source_group_decision_sha256": fitted.source_group_decision.receipt_sha256,
            "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
            "execution_spec_receipt": json.loads(
                _canonical_bytes(dict(fitted.execution_spec_receipt)).decode("ascii")
            ),
            "partition_frame_sha256": [
                value.geometry_receipt["frame_sha256"] for value in partitions
            ],
        },
    )
    return {
        "anchor_selection_receipt": anchor_selection,
        "anchor_horizon_decision": horizon_decision,
        "anchor_trial_ledger": trial_ledger,
        "anchor_selected_trial": selected_trial,
        "anchor_refit_receipt": refit_receipt,
        "attempt_replay_receipt": replay_receipt,
        "oos_source_receipt": oos_source,
    }


def _build_planned_attempt_evidence(
    *, attempt: _VerifiedAttempt, raw_attempt_id: str,
    plan_body: Mapping[str, Any], plan_receipt_sha256: str,
    fitted: FittedNativeSearchTimeframe, config: PipelineConfig,
    partition_cost_precommitments: Mapping[str, Any],
    source_observation_references: Mapping[str, Mapping[str, Any]] | None,
    _entry_admission_preparation: Any = None,
    _full_panel_lane_capture: Any = None,
    _generation: Any = None,
    _independent_inputs: dict | None = None,
    _source_sink=None, _frozen=None, _independent_array_entries=None,
) -> dict[str, Any]:
    if _generation is not None and _generation.independent_signal_r_enabled:
        return _build_independent_r_attempt_evidence(attempt=attempt, raw_attempt_id=raw_attempt_id,
            plan_body=plan_body, plan_receipt_sha256=plan_receipt_sha256, fitted=fitted,
            config=config, partition_cost_precommitments=partition_cost_precommitments,
            source_observation_references=source_observation_references,
            prepared_inputs=_independent_inputs, source_sink=_source_sink,
            frozen=_frozen, array_entries=_independent_array_entries)
    if (_full_panel_lane_capture is not None
            and type(_full_panel_lane_capture) is not _FullPanelLaneCapture):
        _fail("ROUTE_A_FULL_PANEL_CAPTURE_INVALID")
    strategy, _parameters = _runtime_strategy(attempt, timeframe=plan_body["cell"]["timeframe"])
    frame = fitted._frame
    symbol, timeframe, regime = (plan_body["cell"][key] for key in ("symbol", "timeframe", "regime"))
    geometry = plan_body.get("derived_dependency_geometry")
    if type(geometry) is not dict or type(geometry.get("warmup_bars")) is not int:
        _fail("DEPENDENCY_GEOMETRY_MISSING")
    if (not isinstance(fitted.source_contract_seeds, Mapping)
            or not isinstance(fitted.frozen_tier_pair_contract, Mapping)
            or not isinstance(fitted._spread_tiers, Mapping)
            or set(fitted._spread_tiers) != {"P75", "P95"}
            or not isinstance(fitted._entry_spread_observations, pd.Series)):
        _fail("FROZEN_TIER_FITTED_INPUTS_UNAVAILABLE")
    full_prefix_surface = _build_full_prefix_strategy_surface(
        frame=frame, strategy=strategy, fitted=fitted, symbol=symbol, timeframe=timeframe)
    backtester = Backtester(config)
    folds = []
    for fold_ordinal, fold in enumerate(_causal_folds(attempt)):
        partitions = _fold_partitions(frame=frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=geometry["warmup_bars"])
        lanes, partition_signals = {}, {}
        for partition in partitions:
            actual_results = {}
            for tier in ("P75", "P95"):
                decision_result = {}
                matched_base_facts = {} if _full_panel_lane_capture is not None and tier == "P75" else None
                if _full_panel_lane_capture is not None:
                    _full_panel_lane_capture.require_current()
                result, _features, signals, _backtester = _run_tier_partition(
                    partition=partition, full_prefix_surface=full_prefix_surface,
                    backtester=backtester, strategy=strategy, symbol=symbol,
                    timeframe=timeframe, regime=regime, config=config,
                    spec=fitted.instrument_spec,
                    expected_source_contract_sha256=_source_identity(fitted.source_contract_seeds[tier]),
                    spread_tier=fitted._spread_tiers[tier],
                    entry_spread_observations=fitted._entry_spread_observations,
                    decision_result_out=decision_result,
                    **({"_entry_admission_preparation": _entry_admission_preparation}
                       if _entry_admission_preparation is not None else {}),
                    **({"_matched_base_facts_out": matched_base_facts,
                        "_matched_stress_tier": fitted._spread_tiers["P95"]}
                       if _full_panel_lane_capture is not None and tier == "P75" else {}),
                )
                _verify_partition_precommitment(precommitments=partition_cost_precommitments,
                    attempt=attempt, fold_ordinal=fold_ordinal, partition=partition,
                    tier=tier, result=result, pair=fitted.frozen_tier_pair_contract)
                if _full_panel_lane_capture is not None:
                    _full_panel_lane_capture.record_lane(fold_ordinal, partition.role,
                        tier, decision_result, matched_base_facts)
                if partition.role in partition_signals and not partition_signals[partition.role].equals(signals):
                    _fail("PARTITION_SIGNAL_VECTOR_TIER_MISMATCH")
                partition_signals[partition.role] = signals
                name = f"{partition.role}_{tier.lower()}"
                lanes[name] = decision_result
                actual_results[tier] = result
            pair = pm_core.build_backtest_frozen_tier_pair_contract(
                actual_results, spec=fitted.instrument_spec, use_spread=bool(config.use_spread))
            if pair != dict(fitted.frozen_tier_pair_contract):
                _fail("REALIZED_FROZEN_TIER_POLICY_DRIFT", partition.role)
        receipt = _independent_tier_execution_receipt(lanes, fitted.frozen_tier_pair_contract)
        provenance = _fold_provenance(plan_body=plan_body,
            plan_receipt_sha256=plan_receipt_sha256, attempt=attempt, fold=fold,
            fold_ordinal=fold_ordinal, partitions=partitions, lanes=lanes, fitted=fitted,
            independent_tier_execution_receipt=receipt, partition_signals=partition_signals,
            source_observation_receipts=({partition.role: _source_observation_receipt(fitted, partition.frame.index)
                for partition in partitions} if source_observation_references is None else None),
            source_observation_references=({partition.role: _partition_source_observation_reference(
                references=source_observation_references, precommitments=partition_cost_precommitments,
                pair=fitted.frozen_tier_pair_contract, attempt=attempt,
                fold_ordinal=fold_ordinal, partition=partition) for partition in partitions}
                if source_observation_references is not None else None))
        folds.append({"fold_ordinal": fold_ordinal, "provenance": provenance, "lanes": lanes})
    return {"attempt_id": raw_attempt_id, "folds": folds}


def _independent_r_session_state(strategy, symbol):
    session = getattr(strategy, "session_spec", None)
    if callable(session):
        session = session(symbol)
    if session is None or isinstance(session, (Mapping, list, tuple)):
        return session
    if hasattr(session, "__dataclass_fields__"):
        return asdict(session)
    return vars(session)


def _independent_r_configuration_sha256(config, owner=None):
    import pm_production_config_binding as config_binding
    key = ("independent-configuration", id(config))
    value = None if owner is None else owner.get(key)
    if value is None:
        value = hashlib.sha256(config_binding._effective_snapshot_bytes(
            config_binding._snapshot(config))).hexdigest()
        if owner is not None:
            owner[key] = value
    return value


def _independent_r_surface_sha(surface):
    cached = getattr(surface, "_content_sha256", None)
    return cached if cached is not None else _sha256({
        "frame_sha256": _frame_sha256(surface.features),
        "raw_signals_sha256": hashlib.sha256(surface.raw_signals.to_numpy(dtype="<i4").tobytes()).hexdigest()})


def _independent_r_cell_signals(features, raw_signals, regime):
    if regime not in book.REGIME_ORDER or "REGIME_LIVE" not in features:
        _fail("INDEPENDENT_R_CELL_REGIME_INVALID")
    return raw_signals.where(features["REGIME_LIVE"].eq(regime), 0).astype(np.int32)


def _independent_r_geometry_inputs(prepared, features, *, timeframe, warmup_bars):
    axis = prepared.get("_partition_axis")
    if axis is None:
        axis = pm_core.build_backtest_partition_axis(features, timeframe=timeframe, warmup_bars=warmup_bars)
        prepared["_partition_axis"] = axis
    arrays = {}
    for tier in ("P75", "P95"):
        values = prepared[tier]
        encoded = values.get("encoded_half_spread_arrays")
        if encoded is None:
            encoded = {name: _route_a_encode_spread_array(values[name], name)
                for name in ("entry_half_spread", "intrabar_half_spread")}
            values["encoded_half_spread_arrays"] = encoded
        arrays[tier] = encoded
    return axis, arrays


def _independent_r_strategy_surface(*, attempt, parameters, strategy, fitted,
        config_sha, symbol, timeframe):
    before = _route_a_plain_strategy_state(strategy)
    owner, identity = fitted._numeric_preparation, fitted._numeric_key
    key = None
    if owner is not None and identity is not None and before is not None:
        try:
            # The fitted numeric identity includes every consumed detector parameter/seed.
            key = (identity, type(strategy), _canonical_bytes(parameters), config_sha,
                fitted.frame_sha256, pickle.dumps(fitted._frame.attrs, protocol=5))
        except (TypeError, pickle.PickleError):
            key = None

    def compute():
        original = _frame_sha256(fitted._frame)
        surface = _build_full_prefix_strategy_surface(frame=fitted._frame, strategy=strategy,
            fitted=fitted, symbol=symbol, timeframe=timeframe)
        if (_frame_sha256(fitted._frame) != original
                or not surface.features.loc[:, list(fitted._frame.columns)].equals(fitted._frame)):
            _fail("FULL_PREFIX_STRATEGY_INPUT_MUTATED")
        return _FullPrefixStrategySurface(surface.features, surface.raw_signals,
            _independent_r_surface_sha(surface))

    return (compute() if owner is None else
        owner.prepare_strategy_surface(key, strategy, before, fitted._frame, compute))


def _build_independent_r_attempt_evidence(*, attempt, raw_attempt_id, plan_body,
        plan_receipt_sha256, fitted, config, partition_cost_precommitments,
        source_observation_references, prepared_inputs, source_sink=None, frozen=None, array_entries=None):
    """Capture real independent outcomes; account replay remains a separate consumer."""
    import pm_production_config_binding as config_binding
    prepared_inputs = {} if prepared_inputs is None else prepared_inputs
    strategy, parameters = _runtime_strategy(attempt, timeframe=plan_body["cell"]["timeframe"])
    symbol, timeframe, regime = (plan_body["cell"][key] for key in ("symbol", "timeframe", "regime"))
    config_sha = _independent_r_configuration_sha256(config, prepared_inputs)
    surface = _independent_r_strategy_surface(attempt=attempt, parameters=parameters,
        strategy=strategy, fitted=fitted, config_sha=config_sha, symbol=symbol, timeframe=timeframe)
    cutoff = plan_body["anchor"]["selection_cutoff_utc"]
    episode_key = ("independent-episodes", fitted.frame_sha256, symbol, timeframe, cutoff)
    episodes = prepared_inputs.get(episode_key)
    if episodes is None:
        episodes = signal_opportunities.independent_entry_episodes(fitted._frame,
            symbol=symbol, timeframe=timeframe, cutoff_utc=cutoff)
        prepared_inputs[episode_key] = episodes
    surface_sha = _independent_r_surface_sha(surface)
    pricing, folds = Backtester(config), []
    for ordinal, fold in enumerate(_causal_folds(attempt)):
        partitions = _fold_partitions(frame=fitted._frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=plan_body["derived_dependency_geometry"]["warmup_bars"])
        lanes, partition_signals, indexes = {}, {}, {}
        for partition in partitions:
            features, signals = _slice_full_prefix_strategy_surface(
                partition=partition, full_prefix_surface=surface)
            signals = _independent_r_cell_signals(features, signals, regime)
            partition_signals[partition.role] = signals
            geometry_sha = partition.geometry_receipt["receipt_sha256"]
            prepared = prepared_inputs.get(geometry_sha)
            if prepared is None:
                observations = fitted._entry_spread_observations.loc[features.index]
                prepared = {}
                for tier, tier_contract in fitted._spread_tiers.items():
                    values = {}
                    pricing.build_evidence_source_seed_result(features, symbol,
                        spec=fitted.instrument_spec, timeframe=timeframe, spread_tier=tier_contract,
                        entry_spread_observations=observations, _prepared_inputs_out=values)
                    prepared[tier] = values
                prepared_inputs[geometry_sha] = prepared
            reference = (_source_observation_receipt(fitted, features.index)
                if source_observation_references is None else _partition_source_observation_reference(
                    references=source_observation_references, precommitments=partition_cost_precommitments,
                    pair=fitted.frozen_tier_pair_contract, attempt=attempt,
                    fold_ordinal=ordinal, partition=partition))
            axis, half_spread_arrays = _independent_r_geometry_inputs(prepared, features,
                timeframe=timeframe, warmup_bars=partition.warmup_bars)
            start = pd.Timestamp(features["_BAR_CLOSE_TS"].iloc[max(1, partition.warmup_bars) - 1])
            for tier in ("P75", "P95"):
                values = prepared[tier]
                probe = values["source_probe"]
                source_contract = raw_winner.build_trade_source_contract_from_backtester_result(probe)
                provenance = signal_opportunities.OpportunityProvenance(strategy.name, config_sha,
                    surface_sha, tier, _sha256(probe["numerical_execution_cost_attestation"]),
                    _sha256(probe["spread_admission"]))
                batch = signal_opportunities.evaluate_independent_signal_r(features, signals,
                    pricing=pricing, episode_ids=episodes.loc[features.index], strategy=strategy,
                    spec=fitted.instrument_spec, symbol=symbol, timeframe=timeframe,
                    cutoff_utc=partition.end_exclusive_utc, signal_start_utc=start,
                    entry_half_spreads=values["entry_half_spread"],
                    exit_half_spreads=values["intrabar_half_spread"],
                    entry_admission=values["admission_states"], provenance=provenance)
                lane = {"schema_version": "fxpm.independent-signal-partition.v1",
                    "partition_axis": dict(axis), "trade_power_partition_axis": dict(axis),
                    "source_reference": dict(reference), "batch": asdict(batch),
                    "source_contract": source_contract,
                    **{key: copy.deepcopy(probe[key]) for key in (
                        "numerical_execution_cost_attestation", "spread_model", "spread_admission",
                        "swap_accrual_contract")}}
                _verify_partition_precommitment(precommitments=partition_cost_precommitments,
                    attempt=attempt, fold_ordinal=ordinal, partition=partition, tier=tier,
                    result=lane, pair=fitted.frozen_tier_pair_contract)
                lanes[f"{partition.role}_{tier.lower()}"] = lane
            indexes[partition.role] = {
                "schema_version": "fxpm.independent-signal-source-index.v1",
                "partition_geometry": dict(partition.geometry_receipt),
                "half_spread_arrays": half_spread_arrays,
                "instrument_spec": asdict(fitted.instrument_spec),
                "numerical_execution_cost_attestations": {tier: prepared[tier]["source_probe"]["numerical_execution_cost_attestation"]
                    for tier in ("P75", "P95")},
                "spread_model_metadata": {tier: prepared[tier]["source_probe"]["spread_model"] for tier in ("P75", "P95")},
                "strategy_id": strategy.name, "parameters": parameters,
                "source_reference": dict(reference), "configuration_sha256": config_sha,
                "regime_parameters": dict(fitted.regime_parameters),
                "regime_parameters_sha256": fitted.regime_fit_decision.to_dict()["regime_params_sha256"],
                "source_prefix_frame_sha256": fitted.frame_sha256,
                "surface_sha256": surface_sha,
                "session_spec": config_binding._json_value(_independent_r_session_state(strategy, symbol))}
        receipt = _independent_tier_execution_receipt(lanes, fitted.frozen_tier_pair_contract)
        provenance = _fold_provenance(plan_body=plan_body, plan_receipt_sha256=plan_receipt_sha256,
            attempt=attempt, fold=fold, fold_ordinal=ordinal, partitions=partitions,
            lanes=lanes, fitted=fitted, independent_tier_execution_receipt=receipt,
            partition_signals=partition_signals,
            source_observation_receipts=({p.role: _source_observation_receipt(fitted, p.frame.index)
                for p in partitions} if source_observation_references is None else None),
            source_observation_references=({p.role: _partition_source_observation_reference(
                references=source_observation_references, precommitments=partition_cost_precommitments,
                pair=fitted.frozen_tier_pair_contract, attempt=attempt, fold_ordinal=ordinal, partition=p)
                for p in partitions} if source_observation_references is not None else None))
        if source_sink is not None:
            if frozen is None or array_entries is None:
                _fail("INDEPENDENT_R_ARRAY_PUBLICATION_OWNER_REQUIRED")
            indexes, _ = raw_winner._pack_independent_index_arrays(
                sink=source_sink, frozen=frozen, indexes=indexes, array_entries=array_entries)
        folds.append({"fold_ordinal": ordinal, "provenance": provenance, "lanes": lanes,
            "independent_signal_r_indexes": indexes})
    return {"attempt_id": raw_attempt_id, "folds": folds}


def _reverify_fitted_timeframe(
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> FittedNativeSearchTimeframe:
    """Re-open every caller-held byte and recompute the bound regime surface."""

    if type(fitted) is not FittedNativeSearchTimeframe:
        _fail("FITTED_TIMEFRAME_INVALID")
    if getattr(fitted, "_bound_fit_owner", None) is not None:
        _fail("REOPENED_FITTED_STATE_REQUIRES_DECLARED_VERIFIER")
    source = fitted.source_group_decision.to_dict()
    attempts = source.get("acquisition_attempts_exact")
    if type(attempts) is not int or attempts <= 0:
        _fail("SOURCE_ACQUISITION_ATTEMPT_COUNT_INVALID")
    derived = fitted._derived_binding
    numeric_owner = getattr(fitted, "_numeric_preparation", None)
    numeric_kwargs = ({"_numeric_preparation": numeric_owner,
        "_numeric_config_sha256": _independent_r_configuration_sha256(config)}
        if numeric_owner is not None else {})
    if derived is None:
        refreshed = fit_native_timeframe_for_search(
            product=fitted.product, config=config, acquisition_attempts_exact=attempts, **numeric_kwargs)
    else:
        if type(derived) is not _DerivedSearchBinding:
            _fail("M5_DERIVED_FITTED_BINDING_INVALID")
        refreshed = fit_m5_derived_timeframe_for_search(
            context=derived.context, owner=derived.owner, request_lease=derived.request_lease,
            calibration_lease=derived.calibration_lease, cost_edition=derived.cost_edition,
            declared_m5_source_pins=derived.declared_m5_source_pins,
            config=config, acquisition_attempts_exact=attempts, **numeric_kwargs)
        if _canonical_bytes(derived.view_evidence) != _canonical_bytes(refreshed._derived_binding.view_evidence):
            _fail("M5_DERIVED_FITTED_VIEW_CHANGED")
    comparisons = (
        (source, refreshed.source_group_decision.to_dict()),
        (fitted.regime_fit_decision.to_dict(), refreshed.regime_fit_decision.to_dict()),
        (dict(fitted.source_contract_seed), dict(refreshed.source_contract_seed)),
        (dict(fitted.cost_ladder_contract), dict(refreshed.cost_ladder_contract)),
        (fitted.source_contract_seeds, refreshed.source_contract_seeds),
        (fitted.frozen_tier_pair_contract, refreshed.frozen_tier_pair_contract),
        (dict(fitted.regime_parameters), dict(refreshed.regime_parameters)),
        (dict(fitted.train_regime_support), dict(refreshed.train_regime_support)),
        (dict(fitted.execution_spec_receipt), dict(refreshed.execution_spec_receipt)),
    )
    if any(
        _canonical_bytes(left) != _canonical_bytes(right) for left, right in comparisons
    ):
        _fail("FITTED_TIMEFRAME_REPLAY_MISMATCH")
    if (
        fitted.frame_sha256 != refreshed.frame_sha256
        or _frame_sha256(fitted._frame) != fitted.frame_sha256
    ):
        _fail("FITTED_TIMEFRAME_FRAME_CHANGED")
    if (not isinstance(fitted._entry_spread_observations, pd.Series)
            or not fitted._entry_spread_observations.equals(refreshed._entry_spread_observations)
            or not isinstance(fitted._spread_source_product, native_prefix.NativePrefixProduct)
            or fitted._spread_source_product.index_sha256 != refreshed._spread_source_product.index_sha256):
        _fail("FITTED_TIMEFRAME_SPREAD_SOURCE_CHANGED")
    return refreshed


_BOUND_FIT_OWNER_SEAL = object()


class _FreshFitOwner:
    __slots__ = ("seal", "fitted", "context_id", "request_sha256", "original_fitted", "config")
    def __new__(cls, *args, **kwargs):
        _fail("BOUND_FIT_ISSUER_PRIVATE")


def _issue_fresh_fit_owner(*, context, request_sha256, fitted, config):
    issued = object.__new__(_FreshFitOwner)
    issued.seal, issued.fitted = _BOUND_FIT_OWNER_SEAL, fitted
    issued.context_id, issued.request_sha256 = id(context), request_sha256
    issued.original_fitted, issued.config = fitted, config
    return issued


class _BoundFitOwner:
    __slots__ = ("seal", "fitted", "context_id", "request_sha256", "header_bytes", "reference_bytes", "validation_receipt", "verification_only")
    def __new__(cls, *args, **kwargs):
        _fail("BOUND_FIT_OWNER_PRIVATE")


def _bound_fit_runtime(parameters):
    import sys
    current_release = _sha256(authoritative_search_execution_contract())
    if current_release != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256:
        _fail("BOUND_FIT_SOURCE_CHANGED_AFTER_IMPORT")
    return {"python": sys.version, "numpy": np.__version__, "pandas": pd.__version__,
        "detector_code_sha256": pm_regime.detector_code_sha256(),
        "regime_parameters_sha256": pm_regime.regime_params_sha256(parameters),
        "execution_release_sha256": AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        "pm_core_sha256": _file_sha256(Path(pm_core.__file__))}


def _bound_fit_carrier(product):
    return {"index_descriptor": dict(product.index_descriptor), "index_sha256": product.index_sha256,
        "symbol": product.index["symbol"], "timeframe": product.index["timeframe"],
        "cutoff_utc": product.index["cutoff_utc"],
        "activation_id": product.index["activation"]["activation_id"],
        "activation_manifest_sha256": product.index["activation"]["manifest_sha256"],
        "production_contract_required": product._production_contract_required}


def _bound_fit_entry_bytes(context, entry, *, expected_format=None):
    descriptor = authority_spool._validate_descriptor(dict(entry["descriptor"]), content_addressed=True)
    if expected_format is not None and descriptor["format"] != expected_format:
        _fail("BOUND_FIT_DESCRIPTOR_FORMAT_INVALID")
    authority_spool._verify_artifact_file(context, descriptor)
    digest, size, encoded = authority_spool._stream_file(context.root, descriptor["relative_path"],
        maximum_bytes=context.limits.max_artifact_bytes, expected_size=descriptor["size_bytes"], collect=True)
    if digest != descriptor["sha256"] or size != descriptor["size_bytes"]:
        _fail("BOUND_FIT_DESCRIPTOR_CHANGED")
    return encoded


def _bound_fit_frozen_chain(context, body, header):
    namespace = body["artifact_namespace"]
    prefix = f"{namespace}/winner-search/{body['timeframe']}/{body['regime']}/frozen/"
    entries = header.get("artifact_roster")
    if type(entries) is not list or len({entry["artifact_id"] for entry in entries}) != len(entries):
        _fail("BOUND_FIT_FROZEN_ROSTER_INVALID")
    frozen_entries = [dict(entry) for entry in entries if entry["artifact_id"].startswith(prefix)]
    receipt_entry = next((x for x in frozen_entries if x["artifact_id"] == prefix + "receipt"), None)
    if receipt_entry is None or receipt_entry["descriptor"] != header.get("frozen_plan_receipt"):
        _fail("BOUND_FIT_FROZEN_BINDING_INVALID")
    raws = [_bound_fit_entry_bytes(context, entry, expected_format=raw_winner.ASCII_JSON_FORMAT) for entry in frozen_entries]
    schema = json.loads(_bound_fit_entry_bytes(context, receipt_entry,
        expected_format=raw_winner.ASCII_JSON_FORMAT).decode("ascii")).get("schema_version")
    restore = raw_winner.frozen_plan_restorer_for_schema(schema, bound_only=True)
    if restore is None:
        _fail("BOUND_FIT_LEGACY_FROZEN_PLAN_FORBIDDEN")
    frozen = restore(context_identity={"generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256, "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256}, symbol=body["symbol"], timeframe=body["timeframe"],
        regime=body["regime"], artifact_namespace=namespace, manifest_entries=frozen_entries, body_bytes=raws)
    if frozen.bound_fitted_state_entry is None or _canonical_bytes(header.get("fitted_state_entry")) != _canonical_bytes(frozen.bound_fitted_state_entry):
        _fail("BOUND_FIT_FROZEN_BINDING_INVALID")
    cross_descriptor = header.get("horizon_winner_cross_binding")
    cross_entry = next((entry for entry in entries if entry.get("descriptor") == cross_descriptor
        and entry.get("artifact_type") == "historical_horizon_winner_cross_binding"), None)
    if cross_entry is None:
        _fail("BOUND_FIT_CROSS_BINDING_MISSING")
    cross, _, _ = raw_winner.verify_historical_frozen_winner_cross_binding(
        _bound_fit_entry_bytes(context, cross_entry, expected_format=raw_winner.ASCII_JSON_FORMAT), frozen=frozen)
    return frozen, cross


def _bound_fit_open_carrier(context, config, carrier):
    keys = {"index_descriptor", "index_sha256", "symbol", "timeframe", "cutoff_utc", "activation_id",
        "activation_manifest_sha256", "production_contract_required"}
    if type(carrier) is not dict or set(carrier) != keys or carrier["production_contract_required"] is not True:
        _fail("BOUND_FIT_NATIVE_CARRIER_INVALID")
    product = native_prefix.verify_activated_native_prefix(data_dir=Path(config.data_dir), context=context,
        index_descriptor=carrier["index_descriptor"], expected_index_sha256=carrier["index_sha256"],
        expected_symbol=carrier["symbol"], expected_timeframe=carrier["timeframe"], expected_cutoff_utc=carrier["cutoff_utc"],
        expected_activation_manifest_sha256=carrier["activation_manifest_sha256"], production_contract_required=True,
        pinned_activation_manifest_sha256=carrier["activation_manifest_sha256"])
    active = native_prefix.resolve_active_native_artifact(Path(config.data_dir), carrier["symbol"], carrier["timeframe"],
        production_contract_required=True)
    if (not active.valid or active.activation_id != carrier["activation_id"]
            or active.activation_manifest_sha256 != carrier["activation_manifest_sha256"]
            or product.source_state != native_prefix.AVAILABLE):
        _fail("BOUND_FIT_CURRENT_ACTIVATION_CHANGED")
    return product


def _bound_fit_decision(body, cls):
    if type(body) is not dict or "receipt_sha256" not in body:
        _fail("BOUND_FIT_DECISION_INVALID")
    unsigned = dict(body);digest = unsigned.pop("receipt_sha256")
    if digest != _sha256(unsigned):
        _fail("BOUND_FIT_DECISION_INVALID")
    return cls(_canonical_bytes(unsigned), digest)


def bound_fitted_state_reference(fitted, *, context, request):
    owner = getattr(fitted, "_bound_fit_owner", None)
    if (type(owner) is not _BoundFitOwner or owner.seal is not _BOUND_FIT_OWNER_SEAL
            or owner.fitted is not fitted or owner.context_id != id(context)
            or owner.request_sha256 != request.receipt_sha256
            or _frame_sha256(fitted._frame) != fitted.frame_sha256):
        _fail("BOUND_FIT_REOPEN_CAPABILITY_INVALID")
    return json.loads(owner.reference_bytes.decode("ascii"))


def _m5_bound_source_inputs(context, *, request, process, config, source_context,
                            source_session_entry, source_session_sha256, snapshot_issued_at_utc=None,
                            source_validation_completed_at_utc=None):
    if source_validation_completed_at_utc is None:
        source_validation_completed_at_utc = snapshot_issued_at_utc
    elif snapshot_issued_at_utc is not None:
        _fail("BOUND_M5_VALIDATION_TIME_AMBIGUOUS")
    from pm_m5_job_view import M5JobViews, SlotViewBinding, CalibrationViewBinding
    from pm_m5_derived_costs import RecordedDerivedCosts
    import pm_historical_search_producer as producer
    from dataclasses import asdict
    keys = {"owner", "request_lease", "calibration_lease", "cost_edition",
            "source_session", "source_session_entry"}
    if type(source_context) is not dict or set(source_context) != keys:
        _fail("BOUND_M5_REACQUIRED_SOURCE_REQUIRED")
    owner, cost = source_context["owner"], source_context["cost_edition"]
    if (type(owner) is not M5JobViews or type(cost) is not RecordedDerivedCosts
            or _canonical_bytes(source_context["source_session_entry"]) != _canonical_bytes(source_session_entry)):
        _fail("BOUND_M5_REACQUIRED_SOURCE_CHANGED")
    body = historical.verify_declared_slot_producer_request(context, request, process=process)
    session = authority_spool._strict_json_bytes(_bound_fit_entry_bytes(context, source_session_entry),
                                               context.limits.max_json_bytes)
    if (_sha256(session) != source_session_sha256
            or _canonical_bytes(session) != _canonical_bytes(source_context["source_session"])):
        _fail("BOUND_M5_SOURCE_SESSION_CHANGED")
    historical_adapter._validate_declared_m5_source_session(context, body=body, source_session=session,
        request_sha256=request.receipt_sha256, parent=request._precommit.to_dict(),
        completed_at_utc=source_validation_completed_at_utc)
    if session["failure"] is not None or session["cost_status"] != "COST_READY":
        _fail("BOUND_M5_SOURCE_NOT_FITTED")
    entry = session.get("m5_reopen_context_entry")
    if type(entry) is not dict or entry.get("artifact_type") != "declared_m5_reopen_context":
        _fail("BOUND_M5_REOPEN_CONTEXT_MISSING")
    saved = authority_spool._strict_json_bytes(_bound_fit_entry_bytes(context, entry),
                                               min(context.limits.max_json_bytes, 4 * 1024 * 1024))
    if (type(saved) is not dict or set(saved) != {"schema_version", "request_sha256",
            "m5_request_view_id", "m5_calibration_view_id", "owner_metadata", "cost_edition_route", "fine_product_routes"}
            or saved["schema_version"] != "fxpm.declared-m5-reopen-context.v1"
            or saved["request_sha256"] != request.receipt_sha256
            or saved["m5_request_view_id"] != session["m5_request_view_id"]
            or saved["m5_calibration_view_id"] != session["m5_calibration_view_id"]
            or _canonical_bytes(owner.reopen_metadata()) != _canonical_bytes(saved["owner_metadata"])
            or _canonical_bytes(cost.reopen_route()) != _canonical_bytes(saved["cost_edition_route"])):
        _fail("BOUND_M5_REOPEN_CONTEXT_CHANGED")
    expected_binding = {"generation_context_sha256": context.context_sha256, "config_sha256": context.config_sha256,
        "effective_config_sha256": body["effective_config_sha256"], "precommit_sha256": body["precommit_sha256"],
        "search_process_receipt_sha256": process.receipt_sha256, "run_id": body["run_id"], "cell_id": body["cell_id"],
        "m5_view_contract_sha256": body["m5_view_contract_sha256"],
        "view_plan_sha256": body["m5_source_pins"]["view_plan_sha256"]}
    if (any(getattr(owner.binding, key) != value for key, value in expected_binding.items())
            or cost.sha256 != body["spread_profile_identity"]["profile_sha256"]
            or _canonical_bytes(asdict(owner.source_identity)) != _canonical_bytes(session["m5_source_identity"])):
        _fail("BOUND_M5_OWNER_BINDING_CHANGED")
    routes = [producer._prefix_route(owner.product_at(cutoff))
              for cutoff in saved["owner_metadata"]["product_cutoffs"]]
    if _canonical_bytes(routes) != _canonical_bytes(saved["fine_product_routes"]):
        _fail("BOUND_M5_ORIGINAL_ROUTES_CHANGED")
    request_lease, calibration_lease = source_context["request_lease"], source_context["calibration_lease"]
    if (owner.consumer_binding(request_lease) != SlotViewBinding(request.receipt_sha256, body["slot_id"])
            or owner.consumer_binding(calibration_lease) != CalibrationViewBinding(cost.sha256)):
        _fail("BOUND_M5_CONSUMER_BINDING_CHANGED")
    view, calibration = owner.view(request_lease), owner.view(calibration_lease)
    if (_canonical_bytes(producer._declared_m5_view_observation(view))
            != _canonical_bytes(session["m5_request_view_observation"])
            or _canonical_bytes(producer._declared_m5_view_observation(calibration))
            != _canonical_bytes(session["m5_calibration_view_observation"])):
        _fail("BOUND_M5_VIEW_OBSERVATIONS_CHANGED")
    product = owner.product_at(body["anchor"]["selection_cutoff_utc"])
    if (product.index_sha256 != session["native_prefix_index_sha256"]
            or product.index["source_accessed_at_utc"] != session["source_accessed_at_utc"]
            or product.index["timeframe"] != "M5" or product.index["symbol"] != body["symbol"]
            or product._data_dir != Path(config.data_dir).resolve(strict=False)):
        _fail("BOUND_M5_BASE_PRODUCT_CHANGED")
    spec, spec_receipt = _freeze_bound_execution_spec(product=product, config=config)
    bound = cost.bind_view(owner, context=context, calibration_lease=calibration_lease,
                           request_lease=request_lease, point=spec.point)
    base, window = m5_view_price_frame(context=context, owner=owner, lease=request_lease)
    tiers = {tier: bound.resolve(body["symbol"], body["timeframe"], tier, point=spec.point,
        source_identity_sha256=product.native_prefix["source_identity_sha256"]) for tier in ("P75", "P95")}
    fine_frame = native_prefix.open_verified_prefix_frame(product)
    observations = spread_profile.align_prior_completed_spreads(base.index, fine_frame, "M5")
    evidence = {"source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": body["timeframe"],
        "m5_view_contract_sha256": owner.binding.m5_view_contract_sha256,
        "view_key": view.view_key, "view_payload_sha256": view.payload_sha256,
        "view_membership_sha256": view.membership_sha256,
        "base_native_prefix_index_sha256": product.index_sha256,
        "source_accessed_at_utc": product.index["source_accessed_at_utc"]}
    if _canonical_bytes(evidence) != _canonical_bytes(session["fitted_view_evidence"]):
        _fail("BOUND_M5_FITTED_VIEW_CHANGED")
    derived = _DerivedSearchBinding(context, owner, request_lease, calibration_lease,
        cost, json.loads(_canonical_bytes(body["m5_source_pins"])), evidence)
    return product, base, window, tiers, observations, fine_frame, spec, spec_receipt, derived, session


def _publish_declared_m5_bound_fit(capture, fitted, fold_plan, issue_owner):
    _require_executable_bound_fit(fitted)
    if not historical_adapter._verify_declared_slot_capture(capture):
        _fail("BOUND_FIT_CAPTURE_INVALID")
    normalized_folds, _ = raw_winner._normalize_fold_plan(fold_plan, symbol=capture.symbol,
        timeframe=capture.request_body["timeframe"], regime=capture.regime)
    original = getattr(fitted, "_bound_fit_owner", None)
    if original is not None:
        reference = bound_fitted_state_reference(fitted, context=capture.context, request=capture.request)
        state, _ = current_refit.read_bound_fitted_state_reference(capture.context, reference,
            artifact_namespace=capture.raw_artifact_namespace)
        if _canonical_bytes(state["metadata"]["fold_plan"]) != _canonical_bytes(normalized_folds):
            _fail("BOUND_FIT_FOLD_PLAN_CHANGED")
        return reference
    if (type(issue_owner) is not _FreshFitOwner or issue_owner.seal is not _BOUND_FIT_OWNER_SEAL
            or issue_owner.fitted is not fitted or issue_owner.context_id != id(capture.context)
            or issue_owner.request_sha256 != capture.request.receipt_sha256
            or issue_owner.original_fitted is not capture.verified_fitted_timeframe
            or issue_owner.config is not capture.verified_pipeline_config):
        _fail("BOUND_FIT_SUCCESSFUL_REVERIFY_REQUIRED")
    frozen_config = capture.source_session._frozen_config
    frozen_config.verify_current(capture.context)
    receipts = {"source_group_decision": fitted.source_group_decision.to_dict(),
        "regime_fit_decision": fitted.regime_fit_decision.to_dict(),
        "source_contract_seed": dict(fitted.source_contract_seed), "cost_ladder_contract": dict(fitted.cost_ladder_contract),
        "source_contract_seeds": fitted.source_contract_seeds, "frozen_tier_pair_contract": fitted.frozen_tier_pair_contract,
        "regime_parameters": dict(fitted.regime_parameters), "train_regime_support": dict(fitted.train_regime_support),
        "instrument_spec": asdict(fitted.instrument_spec), "execution_spec_receipt": dict(fitted.execution_spec_receipt)}
    metadata = {"binding": {"generation_context_sha256": capture.context.context_sha256,
        "producer_request_sha256": capture.request.receipt_sha256, "precommit_sha256": capture.request_body["precommit_sha256"],
        "process_sha256": capture.process.receipt_sha256, "artifact_namespace": capture.raw_artifact_namespace,
        "effective_config_sha256": frozen_config.effective_config_sha256,
        "horizon_plan_receipt_sha256": capture.cell_plan.horizon_plan.receipt_sha256,
        "cell_plan_receipt_sha256": capture.cell_plan.receipt_sha256},
        "snapshot_issued_at_utc": _now_utc_seconds(), "runtime": _bound_fit_runtime(fitted.regime_parameters),
        "frame_sha256": fitted.frame_sha256, "receipts": receipts, "fold_plan": normalized_folds,
        "source_session_entry": dict(capture.source_session._entry),
        "source_session_receipt_sha256": capture.source_session.receipt_sha256,
        "target_carrier": _bound_fit_carrier(fitted.product), "fine_carrier": _bound_fit_carrier(fitted._spread_source_product),
        "base_columns": [{"name": name, "dtype": str(dtype)} for name, dtype in fitted._frame.dtypes.items() if name not in _REGIME_COLUMNS],
        "entry_observations_sha256": hashlib.sha256(pd.util.hash_pandas_object(
            fitted._entry_spread_observations, index=True).values.tobytes()).hexdigest(),
        "spread_profile_sha256": capture.request_body["spread_profile_identity"]["profile_sha256"]}
    derived = fitted._derived_binding
    if type(derived) is not _DerivedSearchBinding or derived.context is not capture.context:
        _fail("BOUND_M5_LIVE_BINDING_REQUIRED")
    actual_context = {"owner": derived.owner, "request_lease": derived.request_lease,
        "calibration_lease": derived.calibration_lease, "cost_edition": derived.cost_edition,
        "source_session": capture.source_session.to_dict(), "source_session_entry": dict(capture.source_session._entry)}
    _, _, window, _, _, _, _, _, checked, session = _m5_bound_source_inputs(capture.context,
        request=capture.request, process=capture.process, config=capture.pipeline_config,
        source_context=actual_context, source_session_entry=metadata["source_session_entry"],
        source_session_sha256=metadata["source_session_receipt_sha256"],
        snapshot_issued_at_utc=metadata["snapshot_issued_at_utc"])
    if _canonical_bytes(checked.view_evidence) != _canonical_bytes(derived.view_evidence):
        _fail("BOUND_M5_LIVE_BINDING_CHANGED")
    metadata.update(schema_version="fxpm.bound-fitted-m5-metadata.v1", source_kind="m5_derived",
        m5_view_window=window, m5_reopen_context_entry=session["m5_reopen_context_entry"])
    return current_refit.spool_bound_fitted_state(capture.sink, frame=fitted._frame, metadata=metadata)


def _publish_declared_bound_fit(capture, fitted, fold_plan, issue_owner):
    if historical.declared_slot_source_kind(capture.request_body) == "m5_derived":
        return _publish_declared_m5_bound_fit(capture, fitted, fold_plan, issue_owner)
    if not historical_adapter._verify_declared_slot_capture(capture):
        _fail("BOUND_FIT_CAPTURE_INVALID")
    normalized_folds, _ = raw_winner._normalize_fold_plan(fold_plan, symbol=capture.symbol,
        timeframe=capture.request_body["timeframe"], regime=capture.regime)
    original = getattr(fitted, "_bound_fit_owner", None)
    if original is not None:
        reference = bound_fitted_state_reference(fitted, context=capture.context, request=capture.request)
        state, _ = current_refit.read_bound_fitted_state_reference(capture.context, reference,
            artifact_namespace=capture.raw_artifact_namespace)
        if _canonical_bytes(state["metadata"]["fold_plan"]) != _canonical_bytes(normalized_folds):
            _fail("BOUND_FIT_FOLD_PLAN_CHANGED")
        return reference
    if (type(issue_owner) is not _FreshFitOwner or issue_owner.seal is not _BOUND_FIT_OWNER_SEAL
            or issue_owner.fitted is not fitted or issue_owner.context_id != id(capture.context)
            or issue_owner.request_sha256 != capture.request.receipt_sha256
            or issue_owner.original_fitted is not capture.verified_fitted_timeframe
            or issue_owner.config is not capture.verified_pipeline_config):
        _fail("BOUND_FIT_SUCCESSFUL_REVERIFY_REQUIRED")
    frozen_config = capture.source_session._frozen_config
    frozen_config.verify_current(capture.context)
    receipts = {"source_group_decision": fitted.source_group_decision.to_dict(),
        "regime_fit_decision": fitted.regime_fit_decision.to_dict(),
        "source_contract_seed": dict(fitted.source_contract_seed), "cost_ladder_contract": dict(fitted.cost_ladder_contract),
        "source_contract_seeds": fitted.source_contract_seeds, "frozen_tier_pair_contract": fitted.frozen_tier_pair_contract,
        "regime_parameters": dict(fitted.regime_parameters), "train_regime_support": dict(fitted.train_regime_support),
        "instrument_spec": asdict(fitted.instrument_spec), "execution_spec_receipt": dict(fitted.execution_spec_receipt)}
    metadata = {"binding": {"generation_context_sha256": capture.context.context_sha256,
        "producer_request_sha256": capture.request.receipt_sha256, "precommit_sha256": capture.request_body["precommit_sha256"],
        "process_sha256": capture.process.receipt_sha256, "artifact_namespace": capture.raw_artifact_namespace,
        "effective_config_sha256": frozen_config.effective_config_sha256,
        "horizon_plan_receipt_sha256": capture.cell_plan.horizon_plan.receipt_sha256,
        "cell_plan_receipt_sha256": capture.cell_plan.receipt_sha256},
        "snapshot_issued_at_utc": _now_utc_seconds(), "runtime": _bound_fit_runtime(fitted.regime_parameters),
        "frame_sha256": fitted.frame_sha256, "receipts": receipts, "fold_plan": normalized_folds,
        "source_session_entry": dict(capture.source_session._entry),
        "source_session_receipt_sha256": capture.source_session.receipt_sha256,
        "target_carrier": _bound_fit_carrier(fitted.product), "fine_carrier": _bound_fit_carrier(fitted._spread_source_product),
        "base_columns": [{"name": name, "dtype": str(dtype)} for name, dtype in fitted._frame.dtypes.items() if name not in _REGIME_COLUMNS],
        "entry_observations_sha256": hashlib.sha256(pd.util.hash_pandas_object(
            fitted._entry_spread_observations, index=True).values.tobytes()).hexdigest(),
        "spread_profile_sha256": capture.request_body["spread_profile_identity"]["profile_sha256"]}
    return current_refit.spool_bound_fitted_state(capture.sink, frame=fitted._frame, metadata=metadata)


def _check_m5_bound_reopen_window(body, *, verification_only):
    if type(verification_only) is not bool:
        _fail("BOUND_FIT_VERIFICATION_MODE_INVALID")
    if verification_only is False:
        window = body["source_access_window_utc"]
        if not window["start_utc"] <= _now_utc_seconds() < window["end_exclusive_utc"]:
            _fail("BOUND_FIT_ORIGINAL_WINDOW_CLOSED")


def _require_executable_bound_fit(fitted):
    owner = getattr(fitted, "_bound_fit_owner", None)
    if owner is not None and getattr(owner, "verification_only", False) is not False:
        _fail("BOUND_FIT_VERIFICATION_ONLY_NOT_EXECUTABLE")


def _reopen_m5_bound_fitted_timeframe(context, *, request, process, frozen_pipeline_config, header, m5_source_context, verification_only=False):
    """Original run-owned hash chain only; this is not whole-chain signature authentication."""
    started = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    body = historical.verify_declared_slot_producer_request(context, request, process=process)
    if (type(header) is not dict or header.get("schema_version") != "fxpm.declared-attempt-unit-header.v2"
            or _canonical_bytes(header.get("request_body")) != _canonical_bytes(body)
            or "source_access_renewal_sha256" in body):
        _fail("BOUND_FIT_ORIGINAL_HEADER_REQUIRED")
    frozen_pipeline_config.verify_current(context)
    if frozen_pipeline_config.effective_config_sha256 != body["effective_config_sha256"]:
        _fail("BOUND_FIT_CONFIG_CHANGED")
    config = frozen_pipeline_config.materialize(context)
    _check_m5_bound_reopen_window(body, verification_only=verification_only)
    frozen, cross = _bound_fit_frozen_chain(context, body, header)
    reference = frozen.bound_fitted_state_entry
    state, delta = current_refit.read_bound_fitted_state_reference(context, reference,
        artifact_namespace=body["artifact_namespace"])
    meta = state["metadata"]
    required = {"schema_version", "source_kind", "m5_view_window", "m5_reopen_context_entry", "binding", "snapshot_issued_at_utc", "runtime", "frame_sha256", "receipts", "fold_plan",
        "source_session_entry", "source_session_receipt_sha256", "target_carrier", "fine_carrier", "base_columns",
        "entry_observations_sha256", "spread_profile_sha256"}
    if (type(meta) is not dict or set(meta) != required
            or meta["schema_version"] != "fxpm.bound-fitted-m5-metadata.v1" or meta["source_kind"] != "m5_derived"):
        _fail("BOUND_FIT_METADATA_INVALID")
    binding, receipts = meta["binding"], meta["receipts"]
    if (type(binding) is not dict or set(binding) != {"generation_context_sha256", "producer_request_sha256", "precommit_sha256",
            "process_sha256", "artifact_namespace", "effective_config_sha256", "horizon_plan_receipt_sha256", "cell_plan_receipt_sha256"}
            or type(receipts) is not dict or set(receipts) != {"source_group_decision", "regime_fit_decision", "source_contract_seed",
                "cost_ladder_contract", "source_contract_seeds", "frozen_tier_pair_contract", "regime_parameters", "train_regime_support",
                "instrument_spec", "execution_spec_receipt"}):
        _fail("BOUND_FIT_METADATA_INVALID")
    if (binding.get("producer_request_sha256") != request.receipt_sha256
            or binding.get("precommit_sha256") != body["precommit_sha256"]
            or binding.get("process_sha256") != process.receipt_sha256
            or binding.get("effective_config_sha256") != frozen_pipeline_config.effective_config_sha256
            or cross.get("producer_request_sha256") != request.receipt_sha256
            or cross.get("process_precommit_receipt_sha256") != body["precommit_sha256"]
            or cross.get("selection_cutoff_utc") != body["anchor"]["selection_cutoff_utc"]
            or cross.get("anchor_id") != body["slot_id"]
            or cross["horizon_roster"]["plan_receipt_sha256"] != binding.get("horizon_plan_receipt_sha256")
            or binding["horizon_plan_receipt_sha256"] != header["binding"].get("horizon_plan_receipt_sha256")
            or binding["cell_plan_receipt_sha256"] != header["binding"].get("cell_plan_receipt_sha256")
            or _canonical_bytes(meta["fold_plan"]) != _canonical_bytes(frozen.precommitment["causal_fold_plan"])
            or _canonical_bytes(receipts["source_contract_seeds"]) != _canonical_bytes(frozen.source_contract_seeds)
            or _canonical_bytes(receipts["frozen_tier_pair_contract"]) != _canonical_bytes(frozen.frozen_tier_pair_contract)
            or meta["spread_profile_sha256"] != body["spread_profile_identity"]["profile_sha256"]):
        _fail("BOUND_FIT_ORIGINAL_CHAIN_CHANGED")
    issued = _utc(meta["snapshot_issued_at_utc"], "BOUND_FIT_ISSUED_TIME_INVALID")
    if issued > frozen.precommitment["precommitted_at_utc"]:
        _fail("BOUND_FIT_SNAPSHOT_AFTER_FREEZE")
    source = _bound_fit_decision(receipts["source_group_decision"], book.HistoricalSourceGroupDecision)
    fit = _bound_fit_decision(receipts["regime_fit_decision"], book.RegimeFitDecision)
    book._source_body(source);fit_body = book._regime_fit_body(fit, source)
    if (cross.get("source_group_decision_sha256") != source.receipt_sha256
            or cross.get("regime_fit_decision_sha256") != fit.receipt_sha256
            or meta["runtime"] != _bound_fit_runtime(receipts["regime_parameters"])):
        _fail("BOUND_FIT_CODE_RUNTIME_OR_RECEIPT_CHANGED")
    if any(entry not in header["artifact_roster"] for entry in
           (meta["source_session_entry"], meta["m5_reopen_context_entry"])):
        _fail("BOUND_M5_ORIGINAL_CONTEXT_ARTIFACT_MISSING")
    target, base, window, tiers, observations, fine_frame, spec, spec_receipt, derived, source_session = _m5_bound_source_inputs(
        context, request=request, process=process, config=config, source_context=m5_source_context,
        source_session_entry=meta["source_session_entry"], source_session_sha256=meta["source_session_receipt_sha256"],
        snapshot_issued_at_utc=meta["snapshot_issued_at_utc"])
    fine = verified_fine = target
    if (_canonical_bytes(_bound_fit_carrier(target)) != _canonical_bytes(meta["target_carrier"])
            or _canonical_bytes(_bound_fit_carrier(fine)) != _canonical_bytes(meta["fine_carrier"])
            or _canonical_bytes(window) != _canonical_bytes(meta["m5_view_window"])
            or _canonical_bytes(source_session["m5_reopen_context_entry"]) != _canonical_bytes(meta["m5_reopen_context_entry"])):
        _fail("BOUND_M5_ORIGINAL_SOURCE_CHANGED")
    if ([{"name": name, "dtype": str(dtype)} for name, dtype in base.dtypes.items()] != meta["base_columns"]
            or not base.index.equals(delta.index) or base.index.name != delta.index.name):
        _fail("BOUND_FIT_NATIVE_FRAME_CHANGED")
    for label in ("REGIME_RAW", "REGIME", "REGIME_LIVE"):
        if (list(delta[label].cat.categories) != list(pm_regime.REGIME_DTYPE.categories)
                or delta[label].cat.ordered is not False):
            _fail("BOUND_FIT_CATEGORY_CHANGED")
    frame = base.copy(deep=True)
    for name in _REGIME_COLUMNS:
        frame[name] = delta[name]
    if _frame_sha256(frame) != meta["frame_sha256"]:
        _fail("BOUND_FIT_FRAME_CHANGED")
    profile = derived.cost_edition
    if (profile.sha256 != meta["spread_profile_sha256"] or verified_fine.index_sha256 != meta["fine_carrier"]["index_sha256"]
            or hashlib.sha256(pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest() != meta["entry_observations_sha256"]
            or _canonical_bytes(spec_receipt) != _canonical_bytes(receipts["execution_spec_receipt"])
            or _canonical_bytes(asdict(spec)) != _canonical_bytes(receipts["instrument_spec"])):
        _fail("BOUND_FIT_COST_OR_OBSERVATIONS_CHANGED")
    seeds, pair, pair_hashes = raw_winner._current_pair_inputs(receipts["source_contract_seeds"], receipts["frozen_tier_pair_contract"])
    if any(row["frozen_spread_tier"] != tiers[row["tier"]].descriptor() for row in pair["tiers"]):
        _fail("BOUND_FIT_FROZEN_TIERS_CHANGED")
    native = target.native_prefix
    rebuilt_source = book.create_historical_source_group_decision(symbol=body["symbol"], timeframe=body["timeframe"],
        availability=book.AVAILABLE, reason_code="AUTHENTICATED_BASE_M5_DERIVED_PREFIX_AVAILABLE",
        source_contract_sha256=pair_hashes["source_contract_sha256"], source_group_evidence_sha256=target.index_sha256,
        acquisition_attempts_exact=source.to_dict()["acquisition_attempts_exact"], activation_id=native["activation_id"],
        activation_manifest_sha256=native["activation_manifest_sha256"], activation_entry_sha256=native["activation_entry_sha256"],
        native_source_sha256=native["artifact_sha256"])
    parameters = _default_regime_parameters(body["timeframe"])
    if rebuilt_source is None or _canonical_bytes(rebuilt_source.to_dict()) != _canonical_bytes(source.to_dict()) or _canonical_bytes(parameters) != _canonical_bytes(receipts["regime_parameters"]):
        _fail("BOUND_FIT_SOURCE_OR_PARAMETERS_CHANGED")
    detector_sha = pm_regime.detector_code_sha256();params_sha = pm_regime.regime_params_sha256(parameters)
    support = _build_train_regime_support(frame=base, surface=delta, product=target, symbol=body["symbol"],
        timeframe=body["timeframe"], parameters_sha256=params_sha, detector_code_sha256=detector_sha,
        m5_view_evidence=dict(derived.view_evidence))
    detector = book.create_regime_detector_record(source_group_decision=source,
        fit_cutoff_utc=_book_canonical_utc(window["view_last_close_utc"], "REGIME_DETECTOR_CUTOFF_INVALID"),
        detector_version=REGIME_FIT_POLICY_VERSION, detector_code_sha256=detector_sha, parameters=parameters)
    policy = {"schema_version": REGIME_FIT_POLICY_VERSION, "parameter_source": "PINNED_TIMEFRAME_DEFAULT_NO_POSTHOC_TUNING",
        "timeframe": body["timeframe"], "label_order": list(book.REGIME_ORDER), "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY, "detector_code_sha256": detector_sha, "parameters_sha256": params_sha}
    evidence = {"schema_version": REGIME_FIT_EVIDENCE_VERSION, "source_group_decision_sha256": source.receipt_sha256,
        "native_prefix_index_sha256": target.index_sha256, "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "detector_record_sha256": detector["record_sha256"], "m5_view": dict(derived.view_evidence)}
    mapping = {"schema_version": "fxpm.four-regime-mapping.v1", "label_order": list(book.REGIME_ORDER),
        "unobserved_regimes_are_explicit": True, "all_regimes_are_not_required": True}
    rebuilt_fit = book.create_regime_fit_decision(source_group_decision=source, fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_CAUSAL_REGIME_FIT_COMPLETE", regime_fit_policy_sha256=_sha256(policy),
        detector_process_sha256=detector_sha, regime_fit_evidence_sha256=_sha256(evidence),
        four_regime_mapping_sha256=_sha256(mapping), regime_params_sha256=params_sha,
        regime_detector_record=detector, train_regime_support=support)
    if (rebuilt_fit is None or _canonical_bytes(rebuilt_fit.to_dict()) != _canonical_bytes(fit.to_dict())
            or _canonical_bytes(support) != _canonical_bytes(receipts["train_regime_support"])
            or receipts["source_contract_seed"] != {} or receipts["cost_ladder_contract"] != {}):
        _fail("BOUND_FIT_FULL_RECEIPT_COMPARISON_FAILED")
    source_raw = _bound_fit_entry_bytes(context, meta["source_session_entry"])
    source_session = authority_spool._strict_json_bytes(source_raw, context.limits.max_json_bytes)
    if (_sha256(source_session) != meta["source_session_receipt_sha256"]
            or source_session.get("source_state") != native_prefix.AVAILABLE
            or source_session.get("generation_context_sha256") != context.context_sha256
            or source_session.get("config_sha256") != context.config_sha256
            or source_session.get("effective_config_sha256") != body["effective_config_sha256"]
            or source_session.get("selection_cutoff_utc") != body["anchor"]["selection_cutoff_utc"]
            or source_session.get("spread_profile_identity") != body["spread_profile_identity"]
            or source_session.get("native_prefix_index_descriptor") != target.index_descriptor
            or meta["source_session_entry"] != header.get("source_session_entry")
            or source_session.get("request_sha256") != request.receipt_sha256
            or source_session.get("source_group_decision_sha256") != source.receipt_sha256
            or source_session.get("regime_fit_decision_sha256") != fit.receipt_sha256
            or source_session.get("native_prefix_index_sha256") != target.index_sha256
            or source_session.get("source_accessed_at_utc") != target.index["source_accessed_at_utc"]):
        _fail("BOUND_FIT_ORIGINAL_SOURCE_SESSION_CHANGED")
    fitted = FittedNativeSearchTimeframe(product=target, source_group_decision=source, regime_fit_decision=fit,
        source_contract_seed={}, cost_ladder_contract={}, regime_parameters=parameters, train_regime_support=support,
        instrument_spec=spec, execution_spec_receipt=spec_receipt, frame_sha256=meta["frame_sha256"], _frame=frame,
        source_contract_seeds=seeds, frozen_tier_pair_contract=pair, _spread_tiers=tiers,
        _entry_spread_observations=observations, _spread_source_product=verified_fine, _spread_source_frame=fine_frame,
        _derived_binding=derived)
    receipt = {"schema_version": "fxpm.bound-fit-m5-reopen-validation.v1", "origin": "REOPENED_BOUND_FIT_M5",
        "producer_request_sha256": request.receipt_sha256, "fitted_state_receipt_sha256": state["receipt_sha256"],
        "frozen_receipt_sha256": header["frozen_plan_receipt"]["sha256"],
        "original_horizon_plan_receipt_sha256": binding["horizon_plan_receipt_sha256"],
        "original_cell_plan_receipt_sha256": binding["cell_plan_receipt_sha256"],
        "validation_started_at_utc": started,
        "validation_completed_at_utc": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")}
    if context.writable is True and verification_only is False:
        # Separate current validation record in the existing original-request unit domain.
        # Never rewrite historical acquisition/freeze/source-session clocks.
        unit_root = _declared_unit_relative(body, "header.json").rsplit("/", 1)[0]
        authority_spool._publish_exact_json(context, unit_root + "/fit-reopens/" + _sha256(receipt) + ".json",
            receipt, context.limits.max_json_bytes)
    owner = object.__new__(_BoundFitOwner)
    owner.seal, owner.fitted, owner.context_id, owner.request_sha256 = _BOUND_FIT_OWNER_SEAL, fitted, id(context), request.receipt_sha256
    owner.header_bytes, owner.reference_bytes, owner.validation_receipt = _canonical_bytes(header), _canonical_bytes(reference), receipt
    owner.verification_only = verification_only
    object.__setattr__(fitted, "_bound_fit_owner", owner)
    return fitted, dict(receipt)


def reopen_bound_fitted_timeframe(context, *, request, process, frozen_pipeline_config, header, m5_source_context=None, verification_only=False):
    """Original run-owned hash chain only; this is not whole-chain signature authentication."""
    body_kind = historical.declared_slot_source_kind(request.to_dict())
    if body_kind == "m5_derived":
        return _reopen_m5_bound_fitted_timeframe(context, request=request, process=process,
            frozen_pipeline_config=frozen_pipeline_config, header=header, m5_source_context=m5_source_context, verification_only=verification_only)
    if m5_source_context is not None or verification_only is not False:
        _fail("BOUND_FIT_NATIVE_REJECTS_M5_CONTEXT")
    started = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    body = historical.verify_declared_slot_producer_request(context, request, process=process)
    if (type(header) is not dict or header.get("schema_version") != "fxpm.declared-attempt-unit-header.v2"
            or _canonical_bytes(header.get("request_body")) != _canonical_bytes(body)
            or "source_access_renewal_sha256" in body):
        _fail("BOUND_FIT_ORIGINAL_HEADER_REQUIRED")
    frozen_pipeline_config.verify_current(context)
    if frozen_pipeline_config.effective_config_sha256 != body["effective_config_sha256"]:
        _fail("BOUND_FIT_CONFIG_CHANGED")
    config = frozen_pipeline_config.materialize(context)
    window = body["source_access_window_utc"]
    if not window["start_utc"] <= _now_utc_seconds() < window["end_exclusive_utc"]:
        _fail("BOUND_FIT_ORIGINAL_WINDOW_CLOSED")
    frozen, cross = _bound_fit_frozen_chain(context, body, header)
    reference = frozen.bound_fitted_state_entry
    state, delta = current_refit.read_bound_fitted_state_reference(context, reference,
        artifact_namespace=body["artifact_namespace"])
    meta = state["metadata"]
    required = {"binding", "snapshot_issued_at_utc", "runtime", "frame_sha256", "receipts", "fold_plan",
        "source_session_entry", "source_session_receipt_sha256", "target_carrier", "fine_carrier", "base_columns",
        "entry_observations_sha256", "spread_profile_sha256"}
    if type(meta) is not dict or set(meta) != required:
        _fail("BOUND_FIT_METADATA_INVALID")
    binding, receipts = meta["binding"], meta["receipts"]
    if (type(binding) is not dict or set(binding) != {"generation_context_sha256", "producer_request_sha256", "precommit_sha256",
            "process_sha256", "artifact_namespace", "effective_config_sha256", "horizon_plan_receipt_sha256", "cell_plan_receipt_sha256"}
            or type(receipts) is not dict or set(receipts) != {"source_group_decision", "regime_fit_decision", "source_contract_seed",
                "cost_ladder_contract", "source_contract_seeds", "frozen_tier_pair_contract", "regime_parameters", "train_regime_support",
                "instrument_spec", "execution_spec_receipt"}):
        _fail("BOUND_FIT_METADATA_INVALID")
    if (binding.get("producer_request_sha256") != request.receipt_sha256
            or binding.get("precommit_sha256") != body["precommit_sha256"]
            or binding.get("process_sha256") != process.receipt_sha256
            or binding.get("effective_config_sha256") != frozen_pipeline_config.effective_config_sha256
            or cross.get("producer_request_sha256") != request.receipt_sha256
            or cross.get("process_precommit_receipt_sha256") != body["precommit_sha256"]
            or cross.get("selection_cutoff_utc") != body["anchor"]["selection_cutoff_utc"]
            or cross.get("anchor_id") != body["slot_id"]
            or cross["horizon_roster"]["plan_receipt_sha256"] != binding.get("horizon_plan_receipt_sha256")
            or binding["horizon_plan_receipt_sha256"] != header["binding"].get("horizon_plan_receipt_sha256")
            or binding["cell_plan_receipt_sha256"] != header["binding"].get("cell_plan_receipt_sha256")
            or _canonical_bytes(meta["fold_plan"]) != _canonical_bytes(frozen.precommitment["causal_fold_plan"])
            or _canonical_bytes(receipts["source_contract_seeds"]) != _canonical_bytes(frozen.source_contract_seeds)
            or _canonical_bytes(receipts["frozen_tier_pair_contract"]) != _canonical_bytes(frozen.frozen_tier_pair_contract)
            or meta["spread_profile_sha256"] != body["spread_profile_identity"]["profile_sha256"]):
        _fail("BOUND_FIT_ORIGINAL_CHAIN_CHANGED")
    issued = _utc(meta["snapshot_issued_at_utc"], "BOUND_FIT_ISSUED_TIME_INVALID")
    if issued > frozen.precommitment["precommitted_at_utc"]:
        _fail("BOUND_FIT_SNAPSHOT_AFTER_FREEZE")
    source = _bound_fit_decision(receipts["source_group_decision"], book.HistoricalSourceGroupDecision)
    fit = _bound_fit_decision(receipts["regime_fit_decision"], book.RegimeFitDecision)
    book._source_body(source);fit_body = book._regime_fit_body(fit, source)
    if (cross.get("source_group_decision_sha256") != source.receipt_sha256
            or cross.get("regime_fit_decision_sha256") != fit.receipt_sha256
            or meta["runtime"] != _bound_fit_runtime(receipts["regime_parameters"])):
        _fail("BOUND_FIT_CODE_RUNTIME_OR_RECEIPT_CHANGED")
    target = _bound_fit_open_carrier(context, config, meta["target_carrier"])
    fine = _bound_fit_open_carrier(context, config, meta["fine_carrier"])
    if (target.index["symbol"] != body["symbol"] or target.index["timeframe"] != body["timeframe"]
            or target.index["cutoff_utc"] != body["anchor"]["selection_cutoff_utc"]):
        _fail("BOUND_FIT_SOURCE_CUTOFF_CHANGED")
    base = native_prefix.open_verified_prefix_frame(target)
    if ([{"name": name, "dtype": str(dtype)} for name, dtype in base.dtypes.items()] != meta["base_columns"]
            or not base.index.equals(delta.index) or base.index.name != delta.index.name):
        _fail("BOUND_FIT_NATIVE_FRAME_CHANGED")
    for label in ("REGIME_RAW", "REGIME", "REGIME_LIVE"):
        if (list(delta[label].cat.categories) != list(pm_regime.REGIME_DTYPE.categories)
                or delta[label].cat.ordered is not False):
            _fail("BOUND_FIT_CATEGORY_CHANGED")
    frame = base.copy(deep=True)
    for name in _REGIME_COLUMNS:
        frame[name] = delta[name]
    if _frame_sha256(frame) != meta["frame_sha256"]:
        _fail("BOUND_FIT_FRAME_CHANGED")
    spec, spec_receipt = _freeze_bound_execution_spec(product=target, config=config)
    tiers, observations, verified_fine, fine_frame = _native_spread_inputs(product=target, config=config,
        frame=base, spec=spec, _reopened_fine_product=fine)
    profile = spread_profile.load_spread_profile(Path(config.data_dir) / "native" / "terms" / "spread_profile.json")
    if (profile.sha256 != meta["spread_profile_sha256"] or verified_fine.index_sha256 != meta["fine_carrier"]["index_sha256"]
            or hashlib.sha256(pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest() != meta["entry_observations_sha256"]
            or _canonical_bytes(spec_receipt) != _canonical_bytes(receipts["execution_spec_receipt"])
            or _canonical_bytes(asdict(spec)) != _canonical_bytes(receipts["instrument_spec"])):
        _fail("BOUND_FIT_COST_OR_OBSERVATIONS_CHANGED")
    seeds, pair, pair_hashes = raw_winner._current_pair_inputs(receipts["source_contract_seeds"], receipts["frozen_tier_pair_contract"])
    if any(row["frozen_spread_tier"] != tiers[row["tier"]].descriptor() for row in pair["tiers"]):
        _fail("BOUND_FIT_FROZEN_TIERS_CHANGED")
    native = target.native_prefix
    rebuilt_source = book.create_historical_source_group_decision(symbol=body["symbol"], timeframe=body["timeframe"],
        availability=book.AVAILABLE, reason_code="AUTHENTICATED_NATIVE_PREFIX_AVAILABLE",
        source_contract_sha256=pair_hashes["source_contract_sha256"], source_group_evidence_sha256=target.index_sha256,
        acquisition_attempts_exact=source.to_dict()["acquisition_attempts_exact"], activation_id=native["activation_id"],
        activation_manifest_sha256=native["activation_manifest_sha256"], activation_entry_sha256=native["activation_entry_sha256"],
        native_source_sha256=native["artifact_sha256"])
    parameters = _default_regime_parameters(body["timeframe"])
    if rebuilt_source is None or _canonical_bytes(rebuilt_source.to_dict()) != _canonical_bytes(source.to_dict()) or _canonical_bytes(parameters) != _canonical_bytes(receipts["regime_parameters"]):
        _fail("BOUND_FIT_SOURCE_OR_PARAMETERS_CHANGED")
    detector_sha = pm_regime.detector_code_sha256();params_sha = pm_regime.regime_params_sha256(parameters)
    support = _build_train_regime_support(frame=base, surface=delta, product=target, symbol=body["symbol"],
        timeframe=body["timeframe"], parameters_sha256=params_sha, detector_code_sha256=detector_sha)
    detector = book.create_regime_detector_record(source_group_decision=source,
        fit_cutoff_utc=_book_canonical_utc(native["prefix_end_exclusive_utc"], "REGIME_DETECTOR_CUTOFF_INVALID"),
        detector_version=REGIME_FIT_POLICY_VERSION, detector_code_sha256=detector_sha, parameters=parameters)
    policy = {"schema_version": REGIME_FIT_POLICY_VERSION, "parameter_source": "PINNED_TIMEFRAME_DEFAULT_NO_POSTHOC_TUNING",
        "timeframe": body["timeframe"], "label_order": list(book.REGIME_ORDER), "surface": "REGIME_LIVE",
        "warmup_policy": book.REGIME_TRAIN_SUPPORT_WARMUP_POLICY, "detector_code_sha256": detector_sha, "parameters_sha256": params_sha}
    evidence = {"schema_version": REGIME_FIT_EVIDENCE_VERSION, "source_group_decision_sha256": source.receipt_sha256,
        "native_prefix_index_sha256": target.index_sha256, "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "detector_record_sha256": detector["record_sha256"]}
    mapping = {"schema_version": "fxpm.four-regime-mapping.v1", "label_order": list(book.REGIME_ORDER),
        "unobserved_regimes_are_explicit": True, "all_regimes_are_not_required": True}
    rebuilt_fit = book.create_regime_fit_decision(source_group_decision=source, fit_state=book.REGIME_FITTED,
        reason_code="ANCHOR_LOCAL_CAUSAL_REGIME_FIT_COMPLETE", regime_fit_policy_sha256=_sha256(policy),
        detector_process_sha256=detector_sha, regime_fit_evidence_sha256=_sha256(evidence),
        four_regime_mapping_sha256=_sha256(mapping), regime_params_sha256=params_sha,
        regime_detector_record=detector, train_regime_support=support)
    if (rebuilt_fit is None or _canonical_bytes(rebuilt_fit.to_dict()) != _canonical_bytes(fit.to_dict())
            or _canonical_bytes(support) != _canonical_bytes(receipts["train_regime_support"])
            or receipts["source_contract_seed"] != {} or receipts["cost_ladder_contract"] != {}):
        _fail("BOUND_FIT_FULL_RECEIPT_COMPARISON_FAILED")
    source_raw = _bound_fit_entry_bytes(context, meta["source_session_entry"])
    source_session = authority_spool._strict_json_bytes(source_raw, context.limits.max_json_bytes)
    if (_sha256(source_session) != meta["source_session_receipt_sha256"]
            or source_session.get("source_state") != native_prefix.AVAILABLE
            or source_session.get("generation_context_sha256") != context.context_sha256
            or source_session.get("config_sha256") != context.config_sha256
            or source_session.get("effective_config_sha256") != body["effective_config_sha256"]
            or source_session.get("selection_cutoff_utc") != body["anchor"]["selection_cutoff_utc"]
            or source_session.get("spread_profile_identity") != body["spread_profile_identity"]
            or source_session.get("native_prefix_index_descriptor") != target.index_descriptor
            or meta["source_session_entry"] != header.get("source_session_entry")
            or source_session.get("request_sha256") != request.receipt_sha256
            or source_session.get("source_group_decision_sha256") != source.receipt_sha256
            or source_session.get("regime_fit_decision_sha256") != fit.receipt_sha256
            or source_session.get("native_prefix_index_sha256") != target.index_sha256
            or source_session.get("source_accessed_at_utc") != target.index["source_accessed_at_utc"]):
        _fail("BOUND_FIT_ORIGINAL_SOURCE_SESSION_CHANGED")
    fitted = FittedNativeSearchTimeframe(product=target, source_group_decision=source, regime_fit_decision=fit,
        source_contract_seed={}, cost_ladder_contract={}, regime_parameters=parameters, train_regime_support=support,
        instrument_spec=spec, execution_spec_receipt=spec_receipt, frame_sha256=meta["frame_sha256"], _frame=frame,
        source_contract_seeds=seeds, frozen_tier_pair_contract=pair, _spread_tiers=tiers,
        _entry_spread_observations=observations, _spread_source_product=verified_fine, _spread_source_frame=fine_frame)
    receipt = {"schema_version": "fxpm.bound-fit-reopen-validation.v1", "origin": "REOPENED_BOUND_FIT",
        "producer_request_sha256": request.receipt_sha256, "fitted_state_receipt_sha256": state["receipt_sha256"],
        "frozen_receipt_sha256": header["frozen_plan_receipt"]["sha256"],
        "original_horizon_plan_receipt_sha256": binding["horizon_plan_receipt_sha256"],
        "original_cell_plan_receipt_sha256": binding["cell_plan_receipt_sha256"],
        "validation_started_at_utc": started,
        "validation_completed_at_utc": datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")}
    if context.writable is True:
        # Separate current validation record in the existing original-request unit domain.
        # Never rewrite historical acquisition/freeze/source-session clocks.
        unit_root = _declared_unit_relative(body, "header.json").rsplit("/", 1)[0]
        authority_spool._publish_exact_json(context, unit_root + "/fit-reopens/" + _sha256(receipt) + ".json",
            receipt, context.limits.max_json_bytes)
    owner = object.__new__(_BoundFitOwner)
    owner.seal, owner.fitted, owner.context_id, owner.request_sha256 = _BOUND_FIT_OWNER_SEAL, fitted, id(context), request.receipt_sha256
    owner.header_bytes, owner.reference_bytes, owner.validation_receipt = _canonical_bytes(header), _canonical_bytes(reference), receipt
    object.__setattr__(fitted, "_bound_fit_owner", owner)
    return fitted, dict(receipt)


def _declared_fitted_for_execution(capture, fitted, config):
    _require_executable_bound_fit(fitted)
    owner = getattr(fitted, "_bound_fit_owner", None)
    if owner is None:
        # The declared capture already owns this exact producer-fitted object/config.
        refreshed = fitted
        issued = _issue_fresh_fit_owner(context=capture.context,
            request_sha256=capture.request.receipt_sha256, fitted=fitted, config=config)
        return refreshed, issued
    bound_fitted_state_reference(fitted, context=capture.context, request=capture.request)
    return fitted, owner


def _reverify_cell_state_error_envelope(
    *,
    cell_plan: Any,
    fitted: FittedNativeSearchTimeframe,
) -> None:
    """Recompute the planner's preprotected envelope before any evaluation."""

    try:
        import pm_production_search_planner as planner

        receipt = cell_plan.cell_receipt
        declared_request = getattr(cell_plan, "_declared_slot_request", None)
        if (type(declared_request) is historical.DeclaredSlotProducerRequest
                and historical.declared_slot_source_kind(declared_request.to_dict()) == "m5_derived"):
            if (type(receipt) is not dict or receipt.get("source_kind") != "m5_derived"
                    or type(receipt.get("m5_view_window")) is not dict):
                _fail("STATE_ERROR_ENVELOPE_M5_WINDOW_INVALID")
            verified = planner.verify_preprotected_state_error_envelope(
                fitted, receipt.get("state_error_envelope"),
                m5_view_window=receipt["m5_view_window"],
            )
        else:
            if type(receipt) is dict and receipt.get("source_kind") == "m5_derived":
                _fail("STATE_ERROR_ENVELOPE_M5_WINDOW_INVALID")
            verified = planner.verify_preprotected_state_error_envelope(
                fitted,
                receipt.get("state_error_envelope") if type(receipt) is dict else None,
            )
    except Exception as exc:
        detail = getattr(exc, "code", type(exc).__name__)
        raise AuthoritativeSearchExecutionError(
            "STATE_ERROR_ENVELOPE_REVERIFY_FAILED", (str(detail),)
        ) from exc
    if verified is not True:
        _fail("STATE_ERROR_ENVELOPE_REVERIFY_FAILED")


def reverify_fitted_native_timeframe_for_search(
    *,
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> FittedNativeSearchTimeframe:
    """Public fail-closed replay for consumers selecting from fitted support."""

    return _reverify_fitted_timeframe(fitted, config)


def _process_rejection_outcome(
    *,
    attempt: _VerifiedAttempt,
    symbol: str,
    timeframe: str,
    regime: str,
) -> TrialOutcome:
    reason_code = attempt.plan_attempt.get("reason_code")
    if type(reason_code) is not str or not reason_code:
        _fail("PROCESS_REJECTION_REASON_INVALID", attempt.ordinal)
    disposition = attempt.trial.get("state_context_disposition")
    evidence = {
        "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
        "candidate_id": attempt.candidate["candidate_id"],
        "trial_id": attempt.trial["trial_id"],
        "decision_was_pre_search": True,
    }
    if type(disposition) is dict:
        evidence.update(
            {
                "state_context_disposition_sha256": disposition["disposition_sha256"],
                "state_requirement_sha256": disposition["state_requirement_sha256"],
                "required_context_bars": disposition["required_context_bars"],
                "selected_context_bars": disposition["selected_context_bars"],
            }
        )
    return TrialOutcome(
        kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
        stage=SearchStage.FROZEN_VALIDATION,
        reason_code=reason_code,
        reason="released Horizon process rejected this attempt before evaluation",
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_family=attempt.artifacts.winner_process["strategy_family"],
        parameter_hash=attempt.trial["parameter_set_sha256"],
        evidence=evidence,
    )


def _raw_attempt_ids(
    frozen: raw_winner.FrozenWinnerSearchPlan,
    attempts: Sequence[_VerifiedAttempt],
) -> tuple[str, ...]:
    raw_attempts = frozen.attempt_ledger.get("attempts")
    if type(raw_attempts) is not list or len(raw_attempts) != len(attempts):
        _fail("RAW_ATTEMPT_ROSTER_MISMATCH")
    identities: list[str] = []
    for expected, observed in zip(attempts, raw_attempts, strict=True):
        artifacts = expected.artifacts
        if (
            type(observed) is not dict
            or observed.get("ordinal") != expected.ordinal
            or observed.get("process_candidate_id") != artifacts.process_candidate_id
            or observed.get("process_sha256") != artifacts.winner_process_sha256
            or observed.get("horizon_process_sha256")
            != artifacts.horizon_process_sha256
            or observed.get("trial_definition_sha256")
            != artifacts.trial_definition_sha256
            or observed.get("refit_recipe_sha256") != artifacts.refit_recipe_sha256
        ):
            _fail("RAW_ATTEMPT_IDENTITY_MISMATCH", expected.ordinal)
        attempt_id = observed.get("attempt_id")
        if type(attempt_id) is not str or not attempt_id:
            _fail("RAW_ATTEMPT_ID_INVALID", expected.ordinal)
        identities.append(attempt_id)
    if len(set(identities)) != len(identities):
        _fail("RAW_ATTEMPT_ID_DUPLICATED")
    return tuple(identities)


def _is_reparse(info: os.stat_result) -> bool:
    marker = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))
    return bool(marker and int(getattr(info, "st_file_attributes", 0)) & marker)



# Original-request completed attempt units live in the existing generation store.
# This is neither a search outcome nor a grant to qualify, publish or trade.
_DECLARED_UNIT_HEADER_SCHEMA = "fxpm.declared-attempt-unit-header.v2"
_DECLARED_UNIT_SCHEMA = "fxpm.declared-attempt-unit-complete.v1"


def _declared_unit_relative(body: Mapping[str, Any], name: str) -> str:
    for field in ("cell_id", "slot_id", "attempt_id"):
        historical._safe_id(body.get(field), "DECLARED_UNIT_OWNER_INVALID")
    if (type(body.get("precommit_sha256")) is not str or len(body["precommit_sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in body["precommit_sha256"])):
        _fail("DECLARED_UNIT_OWNER_INVALID")
    if name not in {"header.json", "evidence-completed.json", "raw-completion.json", "units.lock"} and (
        len(name) != 11 or not name[:6].isdigit() or name[6:] != ".json"
    ):
        _fail("DECLARED_UNIT_NAME_INVALID")
    return (f"declared-slots/{body['precommit_sha256']}/requests/"
            f"{body['cell_id']}/{body['slot_id']}/{body['attempt_id']}/units/{name}")


def _read_declared_unit_json(context: Any, relative: str) -> dict[str, Any] | None:
    path = authority_spool._local_path(context.root, relative)
    authority_spool._reject_reparse_chain(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or _is_reparse(info):
        _fail("DECLARED_UNIT_PATH_INVALID")
    _, _, raw = authority_spool._stream_file(context.root, relative,
        maximum_bytes=context.limits.max_json_bytes, collect=True)
    value = authority_spool._strict_json_bytes(raw, context.limits.max_json_bytes)
    if type(value) is not dict:
        _fail("DECLARED_UNIT_RECORD_INVALID")
    return value


def _read_declared_unit_header(context: Any, body: Mapping[str, Any]) -> dict[str, Any] | None:
    header = _read_declared_unit_json(context, _declared_unit_relative(body, "header.json"))
    if header is None:
        return None
    if header.get("schema_version") == "fxpm.declared-attempt-unit-header.v1":
        _fail("DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE")
    binding = header.get("binding")
    if (header.get("schema_version") != _DECLARED_UNIT_HEADER_SCHEMA
            or header.get("request_body") != dict(body)
            or type(binding) is not dict
            or binding.get("generation_context_sha256") != context.context_sha256
            or binding.get("producer_request_sha256") != _sha256(body)
            or binding.get("executor_release_sha256") != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
            or binding.get("producer_source_sha256") != _file_sha256(Path(__file__).with_name("pm_historical_search_producer.py"))
            or header.get("qualification_authority") is not False
            or header.get("live_authority") is not False):
        _fail("DECLARED_UNIT_HEADER_BINDING_MISMATCH")
    reference = header.get("fitted_state_entry")
    if type(reference) is not dict or not reference:
        _fail("DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE")
    if (binding.get("fitted_state_reference_sha256") != _sha256(reference)
            or header.get("fit_state_origin_contract") !=
                "RECOMPUTED_OR_REOPENED_FROM_PRECOMMITTED_BOUND_STATE"
            or type(header.get("plan_transport")) is not dict
            or type(header.get("horizon_plan")) is not dict):
        _fail("DECLARED_UNIT_HEADER_BINDING_MISMATCH")
    return header


def _declared_unit_identity(attempt: Any, raw_id: str) -> dict[str, Any]:
    return {"ordinal": attempt.ordinal, "raw_attempt_id": raw_id,
            "plan_attempt_sha256": _sha256(attempt.plan_attempt),
            "status": attempt.plan_attempt["status"]}


def _read_declared_completed_unit(context: Any, body: Mapping[str, Any],
        binding_sha256: str, unit: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    record = _read_declared_unit_json(context,
        _declared_unit_relative(body, f"{unit['ordinal']:06d}.json"))
    if record is None:
        return None
    if (set(record) != {"schema_version", "binding_sha256", "unit", "payload",
                        "completed_at_utc"}
            or record["schema_version"] != _DECLARED_UNIT_SCHEMA
            or record["binding_sha256"] != binding_sha256 or record["unit"] != dict(unit)):
        _fail("DECLARED_UNIT_BINDING_MISMATCH", unit["ordinal"])
    descriptor = record["payload"]
    authority_spool._verify_artifact_file(context, descriptor)
    _, _, raw = authority_spool._stream_file(context.root, descriptor["relative_path"],
        maximum_bytes=MAX_ATTEMPT_EVIDENCE_BYTES,
        expected_size=descriptor["size_bytes"], collect=True)
    value = json.loads(raw.decode("ascii"), object_pairs_hook=_strict_pairs,
                      parse_constant=_reject_json_constant)
    if (type(value) is not dict or value.get("attempt_id") != unit["raw_attempt_id"]
            or _canonical_bytes(value) != raw):
        _fail("DECLARED_UNIT_PAYLOAD_INVALID", unit["ordinal"])
    _utc(record["completed_at_utc"], "DECLARED_UNIT_COMPLETION_TIME_INVALID")
    return record, value


def _declared_completed_prefix(context: Any, body: Mapping[str, Any],
        binding_sha256: str, units: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    prefix = []
    missing = False
    for ordinal, unit in enumerate(units):
        if unit.get("ordinal") != ordinal:
            _fail("DECLARED_UNIT_ROSTER_ORDER_INVALID")
        found = _read_declared_completed_unit(context, body, binding_sha256, unit)
        if found is None:
            missing = True
        elif missing:
            _fail("DECLARED_UNIT_PREFIX_HOLE", ordinal)
        else:
            prefix.append(found[0])
    return prefix


def _publish_declared_completed_unit(context: Any, body: Mapping[str, Any],
        binding_sha256: str, unit: Mapping[str, Any], value: Mapping[str, Any]) -> dict[str, Any]:
    if _read_declared_completed_unit(context, body, binding_sha256, unit) is not None:
        _fail("DECLARED_UNIT_ALREADY_COMPLETE", unit["ordinal"])
    _, size = _canonical_digest_and_size(value)
    if size > MAX_ATTEMPT_EVIDENCE_BYTES:
        _fail("ATTEMPT_EVIDENCE_BYTE_BOUND_EXCEEDED", unit["ordinal"])
    if value.get("attempt_id") != unit["raw_attempt_id"]:
        _fail("DECLARED_UNIT_PAYLOAD_INVALID", unit["ordinal"])
    encoder = json.JSONEncoder(allow_nan=False, ensure_ascii=True,
        separators=(",", ":"), sort_keys=True)
    descriptor = authority_spool.write_opaque_artifact(context,
        (piece.encode("ascii") for piece in encoder.iterencode(value)),
        format=raw_winner.ASCII_JSON_FORMAT)
    record = {"schema_version": _DECLARED_UNIT_SCHEMA, "binding_sha256": binding_sha256,
        "unit": dict(unit), "payload": descriptor,
        "completed_at_utc": _now_utc_seconds()}
    # Payload is durable first. An interruption here leaves no claimed unit.
    # This exact metadata publication is the unit's sole completion boundary.
    authority_spool._publish_exact_json(context,
        _declared_unit_relative(body, f"{unit['ordinal']:06d}.json"),
        record, context.limits.max_json_bytes)
    return record


def _declared_unit_binding(capture: Any, fitted: Any, cell_plan: Any,
        attempts: Sequence[Any], fold_plan: Any, handle: Any) -> dict[str, Any]:
    return {"generation_context_sha256": capture.context.context_sha256,
        "producer_request_sha256": capture.request.receipt_sha256,
        "executor_release_sha256": AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        "producer_source_sha256": _file_sha256(Path(__file__).with_name("pm_historical_search_producer.py")),
        "horizon_plan_receipt_sha256": cell_plan.horizon_plan.receipt_sha256,
        "cell_plan_receipt_sha256": cell_plan.receipt_sha256,
        "source_group_sha256": fitted.source_group_decision.receipt_sha256,
        "regime_fit_sha256": fitted.regime_fit_decision.receipt_sha256,
        "frame_sha256": fitted.frame_sha256,
        "fitted_state_reference_sha256": _sha256(handle.frozen.bound_fitted_state_entry),
        "execution_spec_sha256": _sha256(fitted.execution_spec_receipt),
        "frozen_receipt_sha256": handle.frozen_receipt_descriptor["sha256"],
        "cost_precommitments_sha256": _sha256(handle.frozen.partition_cost_precommitments),
        "fold_plan_sha256": _sha256(fold_plan),
        "attempt_roster_sha256": _sha256([attempt.plan_attempt for attempt in attempts])}


def _make_declared_unit_header(capture: Any, fitted: Any, cell_plan: Any,
        attempts: Sequence[Any], fold_plan: Any, handle: Any, references: Any,
        evidence_started: str) -> dict[str, Any]:
    _require_executable_bound_fit(fitted)
    reference = handle.frozen.bound_fitted_state_entry
    if type(reference) is not dict or not reference:
        _fail("DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE")
    # Same existing horizon artifact is now durably referenced before evidence.
    body = capture.request.to_dict()
    horizon_entry = capture.sink.put_json(f"{body['artifact_namespace']}/horizon-plan",
        "declared_slot_horizon_plan", cell_plan.horizon_plan.to_dict())
    return {"schema_version": _DECLARED_UNIT_HEADER_SCHEMA,
        "fit_state_origin_contract": "RECOMPUTED_OR_REOPENED_FROM_PRECOMMITTED_BOUND_STATE",
        "fitted_state_entry": reference,
        "horizon_plan": {"descriptor": horizon_entry["descriptor"],
            "receipt_sha256": cell_plan.horizon_plan.receipt_sha256},
        "plan_transport": {"cell_receipt": cell_plan.cell_receipt, "spool_entry": cell_plan.spool_entry,
            "attempt_artifacts": list(cell_plan.attempt_artifacts()),
            "state_requirements": list(cell_plan.state_requirements())},
        "binding": _declared_unit_binding(capture, fitted, cell_plan, attempts, fold_plan, handle),
        "request_body": capture.request.to_dict(),
        "source_session_entry": dict(capture.source_session._entry),
        "native_activation_manifest_sha256": fitted.product.index["activation"]["manifest_sha256"],
        "frozen_plan_receipt": dict(handle.frozen_receipt_descriptor),
        "frozen_descriptor_graph": dict(handle.frozen_descriptor_graph),
        "horizon_winner_cross_binding": dict(handle.cross_binding_descriptor),
        "partition_cost_precommitments": dict(handle.frozen.partition_cost_precommitments),
        "source_observation_references": references,
        "artifact_roster": capture.sink.artifact_roster(),
        "evidence_access_started_at_utc": evidence_started,
        "qualification_authority": False, "live_authority": False}



def _continue_declared_attempt_units(context: Any, body: Mapping[str, Any],
        binding_sha256: str, units: Sequence[Mapping[str, Any]], produce: Any, *, progress=None) -> tuple[list[dict[str, Any]], str]:
    """Resume whole attempts; no partially executed fold or ledger is inferred."""
    records = _declared_completed_prefix(context, body, binding_sha256, units)
    from pm_historical_scheduler import _declared_observe_progress
    _declared_observe_progress(progress, 'search_attempts', len(records), len(units))
    end_path = _declared_unit_relative(body, "evidence-completed.json")
    end = _read_declared_unit_json(context, end_path)
    if end is not None and len(records) != len(units):
        _fail("DECLARED_UNIT_COMPLETION_ROSTER_INCOMPLETE")
    if len(records) < len(units):
        window = body["source_access_window_utc"]
        now = _now_utc_seconds()
        if not window["start_utc"] <= now < window["end_exclusive_utc"]:
            _fail("DECLARED_UNIT_ORIGINAL_WINDOW_CLOSED")
    for unit in units[len(records):]:
        value = produce(unit)
        row = _publish_declared_completed_unit(context, body, binding_sha256, unit, value)
        records.append(row)
        _declared_observe_progress(progress, 'search_attempts', len(records), len(units))
        del value
    completed = {"binding_sha256": binding_sha256, "unit_count": len(units),
        "unit_receipt_sha256": [_sha256(row) for row in records]}
    if end is None:
        # This clock is observed only AFTER the last actual missing unit completed.
        # Publication precedes the first Raw semantic consumption.
        end = {**completed, "evidence_access_completed_at_utc": _now_utc_seconds()}
        authority_spool._publish_exact_json(context, end_path, end, context.limits.max_json_bytes)
    if (set(end) != set(completed) | {"evidence_access_completed_at_utc"}
            or any(end.get(key) != value for key, value in completed.items())):
        _fail("DECLARED_UNIT_EVIDENCE_END_CHANGED")
    ended = _utc(end["evidence_access_completed_at_utc"], "DECLARED_UNIT_COMPLETION_TIME_INVALID")
    if any(row["completed_at_utc"] > ended for row in records):
        _fail("DECLARED_UNIT_COMPLETION_TIME_INVALID")
    return records, ended



def _declared_unit_lease(context: Any, body: Mapping[str, Any]) -> Any:
    from contextlib import contextmanager
    import pm_native_storage as storage
    @contextmanager
    def locked():
        relative = _declared_unit_relative(body, "units.lock")
        path = authority_spool._local_path(context.root, relative)
        authority_spool._reject_reparse_chain(path)
        if not path.exists():
            # Atomic one-byte creation uses the existing quota/publication
            # primitives. Never hash/read a contender's already locked byte.
            with authority_spool.spool_quota.publication_lease(context.root,
                    max_generation_bytes=context.limits.max_generation_bytes,
                    min_free_bytes=context.limits.min_free_bytes) as reservation:
                temporary = None
                created = False
                try:
                    reservation.consume(1)
                    temporary = authority_spool._temporary_path(context.root)
                    fd = authority_spool._open_exclusive(temporary)
                    try:
                        authority_spool._write_all(fd, memoryview(b"0"))
                        reservation.materialize(1)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    try:
                        authority_spool._publish_temp_exclusive(context.root, temporary, relative)
                        created = True
                    except FileExistsError:
                        # The winning file is checked after kernel acquisition.
                        pass
                finally:
                    if temporary is not None:
                        temporary.unlink(missing_ok=True)
                reservation.commit(1 if created else 0)
        authority_spool._reject_reparse_chain(path)
        descriptor = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "r+b", buffering=0) as stream:
            before, opened = path.lstat(), os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode)
                    or _is_reparse(before) or opened.st_size != 1
                    or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)):
                _fail("DECLARED_UNIT_LEASE_INVALID")
            if not storage._try_kernel_lock(stream):
                _fail("DECLARED_SLOT_RUN_ACTIVE")
            try:
                authority_spool._reject_reparse_chain(path)
                current, pinned = path.lstat(), os.fstat(stream.fileno())
                if ((current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
                        or (pinned.st_dev, pinned.st_ino) != (opened.st_dev, opened.st_ino)
                        or pinned.st_size != 1):
                    _fail("DECLARED_UNIT_LEASE_INVALID")
                stream.seek(0)
                if stream.read(1) != b"0":
                    _fail("DECLARED_UNIT_LEASE_INVALID")
                yield
            finally:
                storage._release_kernel_lock(stream)
    return locked()


def _create_stage_directory(root: Path, key: str) -> Path:
    """Create one bounded private stage under an already selected spool root."""

    root = Path(os.path.abspath(root))
    try:
        root_info = root.lstat()
    except OSError as exc:
        raise AuthoritativeSearchExecutionError("STAGE_ROOT_UNAVAILABLE") from exc
    if (
        not root.is_absolute()
        or not stat.S_ISDIR(root_info.st_mode)
        or stat.S_ISLNK(root_info.st_mode)
        or _is_reparse(root_info)
    ):
        _fail("STAGE_ROOT_UNSAFE")
    stage = root / f".fxpm-authoritative-search-{key[:32]}"
    try:
        stage.relative_to(root)
        os.mkdir(stage, 0o700)
        info = stage.lstat()
    except FileExistsError as exc:
        raise AuthoritativeSearchExecutionError(
            "STAGE_RESIDUE_REQUIRES_RECOVERY",
            (stage.name,),
        ) from exc
    except OSError as exc:
        raise AuthoritativeSearchExecutionError(
            "STAGE_DIRECTORY_CREATE_FAILED"
        ) from exc
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_ISLNK(info.st_mode)
        or _is_reparse(info)
    ):
        try:
            stage.rmdir()
        except OSError:
            pass
        _fail("STAGE_DIRECTORY_UNSAFE")
    return stage


def _stage_directory(
    capture: historical_adapter.HistoricalSymbolSearchCapture,
    handle: historical_adapter.FrozenHistoricalCell,
) -> Path:
    key = _sha256(
        {
            "schema_version": "fxpm.authoritative-search-stage-key.v1",
            "generation_context_sha256": capture.context.context_sha256,
            "producer_request_sha256": capture.request.receipt_sha256,
            "frozen_receipt_sha256": handle.frozen_receipt_descriptor["sha256"],
            "cell": [handle.symbol, handle.timeframe, handle.regime],
        }
    )
    return _create_stage_directory(Path(capture.context.root), key)


def _current_stage_directory(
    *,
    sink: authority_spool.SymbolAuthoritySink,
    cell_plan: Any,
    frozen: raw_winner.FrozenWinnerSearchPlan,
) -> Path:
    frozen_entry = frozen.frozen_receipt_entry
    descriptor = frozen_entry.get("descriptor")
    if (
        not isinstance(descriptor, Mapping)
        or descriptor.get("sha256")
        != hashlib.sha256(frozen.frozen_receipt_bytes).hexdigest()
    ):
        _fail("CURRENT_FROZEN_RECEIPT_DESCRIPTOR_INVALID")
    key = _sha256(
        {
            "schema_version": "fxpm.current-authoritative-search-stage-key.v1",
            "generation_context_sha256": sink.context.context_sha256,
            "cell_plan_receipt_sha256": cell_plan.receipt_sha256,
            "frozen_receipt_sha256": descriptor["sha256"],
            "cell": [cell_plan.symbol, cell_plan.timeframe, cell_plan.regime],
        }
    )
    return _create_stage_directory(Path(sink.context.root), key)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(
        path,
        os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0)),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _canonical_digest_and_size(value: Any) -> tuple[str, int]:
    encoder = json.JSONEncoder(
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256()
    size = 0
    try:
        for piece in encoder.iterencode(value):
            encoded = piece.encode("ascii")
            digest.update(encoded)
            size += len(encoded)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AuthoritativeSearchExecutionError("STAGE_CANONICAL_JSON_INVALID") from exc
    return digest.hexdigest(), size


def _write_staged_attempt(
    *,
    stage: Path,
    ordinal: int,
    value: Mapping[str, Any],
    reservation: Any,
) -> _StagedAttemptEvidence:
    if type(ordinal) is not int or ordinal < 0 or not isinstance(value, Mapping):
        _fail("STAGE_ATTEMPT_INPUT_INVALID")
    expected_sha256, expected_size = _canonical_digest_and_size(value)
    if expected_size > MAX_ATTEMPT_EVIDENCE_BYTES:
        _fail("ATTEMPT_EVIDENCE_BYTE_BOUND_EXCEEDED", ordinal)
    reservation.consume(expected_size)
    encoder = json.JSONEncoder(
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{ordinal:06d}-",
        suffix=".tmp",
        dir=stage,
    )
    temporary = Path(temporary_name)
    final_path: Path | None = None
    succeeded = False
    digest = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            try:
                pieces = encoder.iterencode(value)
                for piece in pieces:
                    encoded = piece.encode("ascii")
                    size += len(encoded)
                    if size > MAX_ATTEMPT_EVIDENCE_BYTES:
                        _fail("ATTEMPT_EVIDENCE_BYTE_BOUND_EXCEEDED", ordinal)
                    stream.write(encoded)
                    digest.update(encoded)
            except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
                raise AuthoritativeSearchExecutionError(
                    "STAGE_CANONICAL_JSON_INVALID",
                    (str(ordinal),),
                ) from exc
            stream.flush()
            os.fsync(stream.fileno())
        if size <= 0:
            _fail("STAGE_ATTEMPT_EMPTY", ordinal)
        hexadecimal = digest.hexdigest()
        if size != expected_size or hexadecimal != expected_sha256:
            _fail("STAGE_CANONICAL_REPLAY_CHANGED", ordinal)
        final_path = stage / f"{ordinal:06d}-{hexadecimal}.json"
        if final_path.exists() or final_path.is_symlink():
            _fail("STAGE_CONTENT_ADDRESS_COLLISION", ordinal)
        os.replace(temporary, final_path)
        _fsync_directory(stage)
        info = final_path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or _is_reparse(info)
            or info.st_size != size
        ):
            _fail("STAGE_FILE_INVALID", ordinal)
        reservation.materialize(size)
        result = _StagedAttemptEvidence(
            ordinal=ordinal,
            kind="RAW_FOLDS",
            path=final_path,
            size_bytes=size,
            sha256=hexadecimal,
        )
        succeeded = True
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        if not succeeded and final_path is not None:
            try:
                final_path.unlink(missing_ok=True)
            except OSError:
                pass


def _strict_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("STAGE_JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    _fail("STAGE_JSON_NONFINITE_CONSTANT", value)


def _reopen_staged_attempt(row: _StagedAttemptEvidence) -> dict[str, Any]:
    path = row.path
    if row.kind != "RAW_FOLDS" or path is None:
        _fail("STAGE_ROW_KIND_INVALID", row.ordinal)
    try:
        path_info = path.lstat()
        if (
            not stat.S_ISREG(path_info.st_mode)
            or stat.S_ISLNK(path_info.st_mode)
            or _is_reparse(path_info)
            or path_info.st_size != row.size_bytes
        ):
            _fail("STAGE_FILE_CHANGED", row.ordinal)
        with path.open("rb") as raw:
            opened = os.fstat(raw.fileno())
            if (
                opened.st_size != path_info.st_size
                or opened.st_mtime_ns != path_info.st_mtime_ns
            ):
                _fail("STAGE_FILE_TOCTOU", row.ordinal)
            digest = hashlib.sha256()
            size = 0
            while True:
                chunk = raw.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_ATTEMPT_EVIDENCE_BYTES:
                    _fail("STAGE_FILE_BYTE_BOUND_EXCEEDED", row.ordinal)
                digest.update(chunk)
            if size != row.size_bytes or digest.hexdigest() != row.sha256:
                _fail("STAGE_FILE_CONTENT_MISMATCH", row.ordinal)
            raw.seek(0)
            text_stream = io.TextIOWrapper(raw, encoding="ascii", newline="")
            try:
                value = json.load(
                    text_stream,
                    object_pairs_hook=_strict_pairs,
                    parse_constant=_reject_json_constant,
                )
            finally:
                text_stream.detach()
    except AuthoritativeSearchExecutionError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise AuthoritativeSearchExecutionError(
            "STAGE_FILE_REOPEN_FAILED",
            (str(row.ordinal), type(exc).__name__),
        ) from exc
    if type(value) is not dict:
        _fail("STAGE_ATTEMPT_BODY_INVALID", row.ordinal)
    canonical_sha, canonical_size = _canonical_digest_and_size(value)
    if canonical_sha != row.sha256 or canonical_size != row.size_bytes:
        _fail("STAGE_FILE_NOT_CANONICAL", row.ordinal)
    return value


def _cleanup_stage(
    stage: Path | None,
    rows: Sequence[_StagedAttemptEvidence],
) -> None:
    if stage is None:
        return
    errors: list[str] = []
    for row in rows:
        path = row.path
        if path is None:
            continue
        try:
            if path.parent != stage:
                errors.append(f"PATH:{row.ordinal}")
                continue
            path.unlink(missing_ok=True)
        except OSError as exc:
            errors.append(f"UNLINK:{row.ordinal}:{type(exc).__name__}")
    try:
        stage.rmdir()
    except OSError as exc:
        errors.append(f"RMDIR:{type(exc).__name__}")
    if errors:
        raise AuthoritativeSearchExecutionError("STAGE_CLEANUP_FAILED", errors)


def _close_reserved_stage(
    *,
    stage: Path | None,
    rows: Sequence[_StagedAttemptEvidence],
    reservation: Any,
    reservation_scope: Any,
    primary_error: BaseException | None,
) -> None:
    """Remove transient bytes before releasing their generation reservation."""

    close_error: BaseException | None = None
    try:
        _cleanup_stage(stage, rows)
    except BaseException as exc:
        close_error = exc
    if close_error is None and reservation is not None:
        try:
            reservation.cancel_after_cleanup()
        except BaseException as exc:
            close_error = exc
    if reservation_scope is not None:
        try:
            reservation_scope.__exit__(None, None, None)
        except BaseException as exc:
            if close_error is None:
                close_error = exc
    if close_error is not None:
        if primary_error is not None:
            raise close_error from primary_error
        raise close_error


def _capacity_custody_bytes(binding: Mapping[str, Any], **facts: Any) -> bytes:
    body = {
        **binding, **facts,
        "schema_version": CAPACITY_CUSTODY_SCHEMA,
        "classification": "TECHNICAL_CUSTODY_NOT_EVIDENCE",
        "meaning": "NOT_EVIDENCE_NO_REUSE_OR_RESUME_AUTHORITY",
        "reuse_authority": False, "resume_authority": False,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False,
        "retirement": "EXPLICIT_OWNER_OR_QA_ACTION_ONLY",
    }
    encoded = _canonical_bytes(body)
    if len(encoded) > MAX_CAPACITY_CUSTODY_BYTES:
        _fail("CAPACITY_CUSTODY_BOUND_EXCEEDED")
    return encoded + b" " * (MAX_CAPACITY_CUSTODY_BYTES - len(encoded))


def _write_custody_descriptor(descriptor: int, encoded: bytes) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = memoryview(encoded)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("capacity custody short write")
        remaining = remaining[written:]
    os.fsync(descriptor)


def _create_stage_capacity_custody(
    *, root: Path, stage: Path, binding: Mapping[str, Any], reservation: Any,
) -> _StageCapacityCustody:
    root = root.resolve(strict=True)
    info = stage.lstat()
    if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or _is_reparse(info)
            or stage.resolve(strict=True).parent != root):
        _fail("CAPACITY_CUSTODY_PATH_INVALID")
    path = stage / "capacity-custody.json"
    original = {**binding, "original_stage_directory": stage.name,
                "custody_allocated_bytes": MAX_CAPACITY_CUSTODY_BYTES}
    encoded = _capacity_custody_bytes(original,
        custody_state="PREALLOCATED_NONACTIVATING", observed_capacity_failure=None,
        accounting="ACTIVE_EXISTING_TRANSIENT_RESERVATION")
    reservation.consume(len(encoded))
    descriptor = -1
    complete = False
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        _write_custody_descriptor(descriptor, encoded)
        opened, current = os.fstat(descriptor), path.lstat()
        if (not stat.S_ISREG(current.st_mode) or _is_reparse(current)
                or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
                or current.st_size != len(encoded)):
            _fail("CAPACITY_CUSTODY_FILE_INVALID")
        reservation.materialize(len(encoded))
        result = _StageCapacityCustody(root, (info.st_dev, info.st_ino),
            (opened.st_dev, opened.st_ino),
            _StagedAttemptEvidence(-1, "CUSTODY_METADATA", path, len(encoded),
                                  hashlib.sha256(encoded).hexdigest()), original)
        complete = True
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not complete:
            path.unlink(missing_ok=True)


def _retire_capacity_stage_path(stage: Path, target: Path, custody: _StageCapacityCustody) -> None:
    # Resolve and pin both endpoints before the same-parent directory rename.
    root = custody.root.resolve(strict=True)
    root_info = custody.root.lstat()
    before = stage.lstat()
    if (root != custody.root or not stat.S_ISDIR(root_info.st_mode)
            or stat.S_ISLNK(root_info.st_mode) or _is_reparse(root_info)
            or stage.resolve(strict=True).parent != root or target.parent.resolve(strict=True) != root
            or target.resolve(strict=False).parent != root or target.exists()
            or not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode) or _is_reparse(before)
            or (before.st_dev, before.st_ino) != custody.stage_identity):
        _fail("CAPACITY_CUSTODY_PATH_INVALID")
    os.rename(stage, target)
    after = target.lstat()
    if (not stat.S_ISDIR(after.st_mode) or stat.S_ISLNK(after.st_mode) or _is_reparse(after)
            or target.resolve(strict=True).parent != root
            or (after.st_dev, after.st_ino) != custody.stage_identity):
        _fail("CAPACITY_CUSTODY_PATH_INVALID")


def _refresh_capacity_custody(path: Path, custody: _StageCapacityCustody, encoded: bytes) -> None:
    current = path.lstat()
    if (not stat.S_ISREG(current.st_mode) or stat.S_ISLNK(current.st_mode) or _is_reparse(current)
            or (current.st_dev, current.st_ino) != custody.receipt_identity
            or current.st_size != MAX_CAPACITY_CUSTODY_BYTES
            or path.parent.resolve(strict=True).parent != custody.root):
        _fail("CAPACITY_CUSTODY_FILE_INVALID")
    descriptor = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_size != MAX_CAPACITY_CUSTODY_BYTES
                or (opened.st_dev, opened.st_ino) != custody.receipt_identity):
            _fail("CAPACITY_CUSTODY_FILE_INVALID")
        _write_custody_descriptor(descriptor, encoded)
    finally:
        os.close(descriptor)


def _close_historical_reserved_stage(
    *, stage: Path | None, rows: Sequence[_StagedAttemptEvidence], reservation: Any,
    reservation_scope: Any, primary_error: BaseException | None,
    custody: _StageCapacityCustody | None, evidence_started: str | None,
    evidence_completed: str | None,
) -> None:
    if custody is None or not authority_spool.is_resource_capacity_error(primary_error):
        cleanup_rows = list(rows) + ([] if custody is None else [custody.receipt])
        return _close_reserved_stage(stage=stage, rows=cleanup_rows,
            reservation=reservation, reservation_scope=reservation_scope, primary_error=primary_error)
    assert primary_error is not None and stage is not None
    retained = custody.root / (".fxpm-capacity-custody-" + secrets.token_hex(8))
    try:
        _retire_capacity_stage_path(stage, retained, custody)
        _fsync_directory(custody.root)
        primary_error.add_note("CAPACITY_STAGE_RETAINED:" + retained.name)
    except Exception as exc:
        primary_error.add_note("CAPACITY_STAGE_RETIREMENT_FAILED:" + type(exc).__name__)
        if stage.exists():
            retained = stage
    abandoned = reservation_scope is None
    if reservation_scope is not None:
        try:
            # Leave the unfinished record for the existing abandoned-guard rescan.
            # cancel_after_cleanup is forbidden while these bytes remain.
            reservation_scope.__exit__(None, None, None)
            abandoned = True
        except Exception as exc:
            primary_error.add_note("CAPACITY_RESERVATION_EXIT_FAILED:" + type(exc).__name__)
    try:
        metadata = [{"ordinal": row.ordinal, "kind": row.kind, "sha256": row.sha256,
                     "size_bytes": row.size_bytes,
                     "file": None if row.path is None else row.path.name} for row in rows]
        valid_bytes = sum(row.size_bytes for row in rows if row.path is not None)
        encoded = _capacity_custody_bytes(custody.binding,
            custody_state="CAPACITY_RETAINED_NONACTIVATING", retained_directory=retained.name,
            observed_capacity_failure="SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED",
            observed_at_utc=_now_utc_seconds(), evidence_access_started_at_utc=evidence_started,
            evidence_access_completed_at_utc=evidence_completed, completed_row_metadata=metadata,
            completed_row_count=len(rows), known_completed_row_bytes=valid_bytes,
            known_retained_bytes=valid_bytes + MAX_CAPACITY_CUSTODY_BYTES,
            accounting="UNFINISHED_GUARD_CLOSED_RESCAN_REQUIRED" if abandoned else "SCOPE_EXIT_FAILED_NO_CANCEL")
        _refresh_capacity_custody(retained / custody.receipt.path.name, custody, encoded)
    except Exception as exc:
        # Retain the original refusal and rows even if diagnostic I/O also fails.
        primary_error.add_note("CAPACITY_CUSTODY_REFRESH_FAILED:" + type(exc).__name__)


def _infrastructure_failure(
    *,
    code: str,
    details: Sequence[str],
    symbol: str,
    timeframe: str,
    regime: str,
) -> SearchInfrastructureError:
    return SearchInfrastructureError(
        TrialOutcome(
            kind=SearchOutcomeKind.INFRA_FAILURE,
            stage=SearchStage.FROZEN_VALIDATION,
            reason_code=code,
            reason=":".join((code, *tuple(details)))[:320],
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            exception_type="AuthoritativeSearchExecutionError",
            evidence={"technical_failure": True, "economic_no_winner": False},
        )
    )


def _technical_failure_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if (
        1 <= len(code) <= 128
        and code[0].isalpha()
        and all(character.isalnum() or character == "_" for character in code)
    ):
        return code
    return "AUTHORITATIVE_SEARCH_EXECUTION_FAILED"


def _bounded_canary_failure_diagnostic(
    exc: BaseException,
    *,
    attempted_trials_exact: int,
) -> dict[str, Any]:
    """Preserve the exact useful failure tuple without risking artifact bounds."""

    raw_details: tuple[str, ...]
    exception_type = type(exc).__name__
    attempt_ordinal: int | None = None
    fold_ordinal: int | None = None
    if isinstance(exc, AuthoritativeSearchExecutionError):
        raw_details = tuple(str(value) for value in exc.details)
        if (
            exc.code
            in {
                "FROZEN_PATH_COST_LADDER_FAILED",
                "FROZEN_COST_LADDER_ADVERSITY_PLAN_MISMATCH",
            }
            and len(raw_details) >= 2
        ):
            try:
                parsed_attempt = int(raw_details[0])
                parsed_fold = int(raw_details[1])
            except (TypeError, ValueError, OverflowError):
                pass
            else:
                if 0 <= parsed_attempt < attempted_trials_exact:
                    attempt_ordinal = parsed_attempt
                    if 0 <= parsed_fold < winner.MAX_FOLDS:
                        fold_ordinal = parsed_fold
            if exc.code == "FROZEN_PATH_COST_LADDER_FAILED" and len(raw_details) >= 3:
                candidate_type = raw_details[2].strip()
                if candidate_type:
                    exception_type = candidate_type
    else:
        raw_details = (str(exc),)

    maximum_details = historical_adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_LIMIT
    maximum_bytes = historical_adapter.BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_BYTES

    def bounded(value: str) -> str:
        text = str(value).strip()
        if not text:
            text = type(exc).__name__
        if not all(character.isprintable() for character in text):
            text = repr(text)
        encoded = text.encode("utf-8")
        if len(encoded) <= maximum_bytes:
            return text
        digest = hashlib.sha256(encoded).hexdigest()
        suffix = f"...[sha256={digest}]"
        prefix = text
        while prefix and len((prefix + suffix).encode("utf-8")) > maximum_bytes:
            prefix = prefix[:-1]
        return (prefix + suffix) if prefix else f"sha256={digest}"

    details = tuple(
        bounded(value) for value in raw_details[:maximum_details] if str(value).strip()
    )
    return {
        "exception_type": bounded(exception_type)[
            : historical_adapter.BOUNDED_RAW_WINNER_CANARY_EXCEPTION_TYPE_BYTES
        ],
        "failure_details": details,
        "attempt_ordinal": attempt_ordinal,
        "fold_ordinal": fold_ordinal,
    }


def _verified_current_observed_inputs(
    *,
    process: Any,
    sink: authority_spool.SymbolAuthoritySink,
    cell_plan: Any,
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
    _fresh_fit_owner=None,
) -> tuple[FittedNativeSearchTimeframe, dict[str, Any], dict[str, Any]]:
    """Reopen the sealed current plan and bind it to one reverified frame."""

    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CURRENT_AUTHORITY_SINK_INVALID")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    try:
        # Local import avoids the planner -> executor module cycle.  Runtime
        # execution occurs only after both modules are fully initialized.
        import pm_production_search_planner as planner

        verified = planner.verify_production_cell_search_plan(
            sink.context,
            process=process,
            sink=sink,
            plan=cell_plan,
        )
    except AuthoritativeSearchExecutionError:
        raise
    except Exception as exc:
        detail = getattr(exc, "code", type(exc).__name__)
        raise AuthoritativeSearchExecutionError(
            "CURRENT_CELL_PLAN_REVERIFY_FAILED", (str(detail),)
        ) from exc
    if verified is not True:
        _fail("CURRENT_CELL_PLAN_REVERIFY_FAILED")
    if (
        getattr(cell_plan, "mode", None) != "CURRENT_REFIT"
        or getattr(cell_plan, "native_prefix_product", None) is not None
        or getattr(cell_plan, "fitted_timeframe", None) is not None
    ):
        _fail("CURRENT_OBSERVED_CELL_PLAN_MODE_INVALID")

    if _fresh_fit_owner is None:
        refreshed = _reverify_fitted_timeframe(fitted, config)
    else:
        owner = _fresh_fit_owner
        if (type(owner) is not _FreshFitOwner or owner.seal is not _BOUND_FIT_OWNER_SEAL
                or owner.context_id != id(sink.context) or owner.request_sha256 != process.receipt_sha256
                or owner.fitted is not fitted or owner.original_fitted is not fitted or owner.config is not config):
            _fail("CURRENT_FRESH_FIT_OWNER_INVALID")
        refreshed = fitted
    try:
        receipt = cell_plan.cell_receipt
        plan_body = cell_plan.horizon_plan.to_dict()
        binding = historical.historical_horizon_plan_binding(cell_plan.horizon_plan)
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "CURRENT_CELL_PLAN_REOPEN_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    native = dict(refreshed.product.native_prefix)
    expected_cell = {
        "symbol": refreshed.source_group_decision.to_dict()["symbol"],
        "timeframe": refreshed.source_group_decision.to_dict()["timeframe"],
        "regime": str(getattr(cell_plan, "regime", "")).upper(),
    }
    expected_native = {
        "index_sha256": refreshed.product.index_sha256,
        "content_sha256": native["prefix_content_sha256"],
        "start_utc": native["prefix_start_utc"],
        "end_exclusive_utc": native["prefix_end_exclusive_utc"],
        "last_close_utc": native["prefix_last_close_utc"],
        "rows": native["prefix_rows"],
    }
    support_receipt = dict(refreshed.train_regime_support).get("receipt_sha256")
    if (
        receipt.get("mode") != "CURRENT_REFIT"
        or receipt.get("cell") != expected_cell
        or plan_body.get("cell") != expected_cell
        or receipt.get("native_prefix") != expected_native
        or plan_body.get("native_prefix") != native
        or receipt.get("source_group_decision_sha256")
        != refreshed.source_group_decision.receipt_sha256
        or receipt.get("regime_fit_decision_sha256")
        != refreshed.regime_fit_decision.receipt_sha256
        or receipt.get("train_regime_support_sha256") != support_receipt
        or binding.get("plan_receipt_sha256") != _sha256(plan_body)
    ):
        _fail("CURRENT_FITTED_PLAN_BINDING_MISMATCH")
    _reverify_cell_state_error_envelope(cell_plan=cell_plan, fitted=refreshed)
    return refreshed, dict(receipt), dict(binding)


def _build_current_cross_binding_bytes(
    *,
    cell_plan: Any,
    process: Any,
    plan_body: Mapping[str, Any],
    plan_binding: Mapping[str, Any],
    frozen: raw_winner.FrozenWinnerSearchPlan,
    attempt_artifacts: Sequence[Mapping[str, Any]],
    fitted: FittedNativeSearchTimeframe,
    horizon_execution_contracts: Sequence[Mapping[str, Any]],
    source_count_receipts: Sequence[Mapping[str, Any]],
) -> bytes:
    """Derive the CURRENT binding from the released Horizon<->Winner bijection."""

    anchor = plan_body.get("anchor")
    if not isinstance(anchor, Mapping):
        _fail("CURRENT_HORIZON_ANCHOR_INVALID")
    historical_body = historical_adapter._cross_binding_body(
        plan_body=plan_body,
        plan_receipt_sha256=plan_binding["plan_receipt_sha256"],
        frozen=frozen,
        attempt_artifacts=attempt_artifacts,
        producer_request_sha256=cell_plan.receipt_sha256,
        anchor_id=anchor["anchor_id"],
        artifact_namespace=frozen.artifact_namespace,
        source_group_decision_sha256=fitted.source_group_decision.receipt_sha256,
        regime_fit_decision_sha256=fitted.regime_fit_decision.receipt_sha256,
        horizon_execution_contracts=horizon_execution_contracts,
        source_count_receipts=source_count_receipts,
    )
    historical_schema, current_schema = (
        raw_winner.horizon_winner_cross_binding_schemas_for_generation(
            frozen.selection_generation, current_pair=True
        )
    )
    if historical_body.get("schema_version") != historical_schema:
        _fail("CURRENT_CROSS_BINDING_BASE_INVALID")
    current = dict(historical_body)
    for name in (
        "producer_request_sha256",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
    ):
        if name not in current:
            _fail("CURRENT_CROSS_BINDING_BASE_INVALID", name)
        current.pop(name)
    current.update(
        {
            "schema_version": current_schema,
            "mode": raw_winner.CURRENT_CROSS_BINDING_MODE,
            "cell_plan_receipt_sha256": cell_plan.receipt_sha256,
            "causal_precommit_receipt_sha256": (
                cell_plan.causal_precommit_receipt_sha256
            ),
            "search_process_receipt_sha256": process.receipt_sha256,
        }
    )
    return _canonical_bytes(current)


def _persist_current_cross_binding(
    *,
    sink: authority_spool.SymbolAuthoritySink,
    cell_plan: Any,
    encoded: bytes,
) -> dict[str, Any]:
    namespace = cell_plan.cell_receipt.get("artifact_namespace")
    if type(namespace) is not str or not namespace:
        _fail("CURRENT_ARTIFACT_NAMESPACE_INVALID")
    artifact_id = (
        f"{namespace}/current-executor/{cell_plan.timeframe}/{cell_plan.regime}/"
        "horizon-winner-cross-binding"
    )
    entry = sink.put_opaque(
        artifact_id,
        "current_horizon_winner_cross_binding",
        (encoded,),
        format=raw_winner.ASCII_JSON_FORMAT,
    )
    descriptor = entry.get("descriptor")
    if (
        not isinstance(descriptor, Mapping)
        or descriptor.get("sha256") != hashlib.sha256(encoded).hexdigest()
        or descriptor.get("size_bytes") != len(encoded)
        or descriptor.get("format") != raw_winner.ASCII_JSON_FORMAT
    ):
        _fail("CURRENT_CROSS_BINDING_DESCRIPTOR_INVALID")
    return json.loads(_canonical_bytes(entry).decode("ascii"))


def _signal_design_utc(value: Any, code: str) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp) or str(stamp.tz) != "UTC":
            _fail(code)
        return stamp.as_unit("ns")
    except AuthoritativeSearchExecutionError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise AuthoritativeSearchExecutionError(code, (type(exc).__name__,)) from exc


def _signal_design_frame(frame: pd.DataFrame, cap: pd.Timestamp) -> pd.DataFrame:
    """Validate native UTC before losslessly changing storage resolution only."""
    if not isinstance(frame, pd.DataFrame) or frame.empty or "_BAR_CLOSE_TS" not in frame:
        _fail("SIGNAL_DESIGN_FRAME_INVALID")
    try:
        opens = pd.DatetimeIndex(frame.index)
        closes = pd.DatetimeIndex(frame["_BAR_CLOSE_TS"])
        for axis in (opens, closes):
            if (str(axis.tz) != "UTC" or axis.hasnans
                    or not axis.is_unique or not axis.is_monotonic_increasing):
                _fail("SIGNAL_DESIGN_NATIVE_UTC_AXIS_REQUIRED")
        if (closes <= opens).any() or (closes[:-1] > opens[1:]).any():
            _fail("SIGNAL_DESIGN_BAR_AXIS_INVALID")
        if closes[-1] > cap or opens[-1] >= cap:
            _fail("SIGNAL_DESIGN_TRAIN_CAP_EXCEEDED")
        normalized_opens, normalized_closes = opens.as_unit("ns"), closes.as_unit("ns")
        if not (normalized_opens == opens).all() or not (normalized_closes == closes).all():
            _fail("SIGNAL_DESIGN_NS_NORMALIZATION_LOSS")
    except AuthoritativeSearchExecutionError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise AuthoritativeSearchExecutionError(
            "SIGNAL_DESIGN_NS_NORMALIZATION_FAILED", (type(exc).__name__,)
        ) from exc
    local = frame.copy(deep=False)
    local.index = normalized_opens
    local["_BAR_CLOSE_TS"] = normalized_closes
    local.attrs = {}
    return local


def _signal_design_window(partition: _PartitionSlice, features: pd.DataFrame,
        cap: pd.Timestamp) -> tuple[int, pd.Timestamp, pd.Timestamp]:
    first_entry = max(1, partition.warmup_bars)
    if first_entry >= len(features):
        _fail("SIGNAL_DESIGN_SIGNAL_WINDOW_INVALID")
    cutoff = _signal_design_utc(features["_BAR_CLOSE_TS"].iloc[-1],
        "SIGNAL_DESIGN_PARTITION_CUTOFF_INVALID")
    start = _signal_design_utc(features["_BAR_CLOSE_TS"].iloc[first_entry - 1],
        "SIGNAL_DESIGN_SIGNAL_START_INVALID")
    if start >= cutoff or cutoff > cap:
        _fail("SIGNAL_DESIGN_SIGNAL_WINDOW_INVALID")
    return first_entry, start, cutoff


def _signal_design_groups(records) -> list[dict[str, Any]]:
    """Descriptive denominators/moments; unresolved labels are not zero PnL."""
    groups: dict[tuple, dict[str, Any]] = {}
    for record in records:
        key = (record.entry_regime, record.entry_admission, record.status)
        row = groups.setdefault(key, {"observation_count": 0, "closed_pips": []})
        row["observation_count"] += 1
        if record.status == "CLOSED":
            row["closed_pips"].append(record.signed_pips)
    result = []
    for (regime, admission, status), row in sorted(groups.items(),
            key=lambda item: tuple("" if value is None else value for value in item[0])):
        values = row["closed_pips"]
        try:
            total = math.fsum(values) if values else None
            squares = math.fsum(value * value for value in values) if values else None
            if values and (not math.isfinite(total) or not math.isfinite(squares)):
                _fail("SIGNAL_DESIGN_GROUP_MOMENTS_INVALID")
        except (TypeError, ValueError, OverflowError) as exc:
            raise AuthoritativeSearchExecutionError("SIGNAL_DESIGN_GROUP_MOMENTS_INVALID") from exc
        result.append({"entry_regime": regime, "entry_admission": admission, "status": status,
            "observation_count": row["observation_count"], "closed_observation_count": len(values),
            "closed_signed_pip_sum": total, "closed_signed_pip_sum_squares": squares})
    return result


def _signal_design_inputs(*, process, sink, planning_request, anchor_plan_receipt,
        causal_precommit_payload, cell_plan, fitted, frozen_pipeline_config,
        declared_slot_roster=None):
    import pm_production_config_binding as config_binding
    import pm_production_anchor_plan as anchor_authority
    import pm_production_search_planner as planner

    if declared_slot_roster is not None or getattr(cell_plan, "mode", None) == "DECLARED_SLOT_UNISSUED":
        _fail("SIGNAL_DESIGN_DECLARED_SLOTS_UNSUPPORTED")
    if (type(sink) is not authority_spool.SymbolAuthoritySink
            or sink.manifest_domain != authority_spool.GENERAL_MANIFEST_DOMAIN):
        _fail("SIGNAL_DESIGN_GENERAL_SINK_REQUIRED")
    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("SIGNAL_DESIGN_FROZEN_CONFIG_REQUIRED")
    config = frozen_pipeline_config.materialize(sink.context)
    planner.verify_frozen_production_search_process(sink.context, process)
    process_body = process.to_dict()
    edition = planner.source_edition_info(process_body)
    metadata = planner._validate_request(planning_request, process=process,
        context=sink.context, causal_precommit_payload=causal_precommit_payload)
    if metadata.get("schema_version") != historical.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA:
        _fail("SIGNAL_DESIGN_PLANNING_METADATA_REQUIRED")
    if planner.verify_production_cell_search_plan(sink.context, process=process,
            sink=sink, plan=cell_plan) is not True:
        _fail("SIGNAL_DESIGN_CELL_PLAN_INVALID")
    if (getattr(cell_plan, "mode", None) != "HISTORICAL_ANCHOR"
            or sink.symbol != cell_plan.symbol
            or planning_request.symbol_sink(sink.symbol) is not sink
            or type(fitted) is not FittedNativeSearchTimeframe):
        _fail("SIGNAL_DESIGN_METADATA_BINDING_INVALID")
    precommit = json.loads(_canonical_bytes(dict(causal_precommit_payload)).decode("ascii"))
    verified_anchor_plan = anchor_authority.verify_production_anchor_plan_receipt(
        Path(config.data_dir), anchor_plan_receipt)
    anchors = verified_anchor_plan["anchor_plan"]
    if (_canonical_bytes(precommit["process_definition"]["anchor_plan"]) != _canonical_bytes(anchors)
            or _sha256(anchors) != verified_anchor_plan["anchor_plan_sha256"]):
        _fail("SIGNAL_DESIGN_ANCHOR_AUTHORITY_MISMATCH")
    earliest = min(_signal_design_utc(row["selection_cutoff_utc"],
        "SIGNAL_DESIGN_JUDGING_CUTOFF_INVALID") for row in anchors)
    if (not isinstance(fitted._spread_tiers, Mapping)
            or set(fitted._spread_tiers) != {"P75", "P95"}):
        _fail("SIGNAL_DESIGN_FROZEN_TIERS_REQUIRED")
    tier_descriptors = {tier: value.descriptor() for tier, value in fitted._spread_tiers.items()}
    protected = {_signal_design_utc(row.get("protected_cutoff_utc"),
        "SIGNAL_DESIGN_PROTECTED_CUTOFF_INVALID") for row in tier_descriptors.values()}
    profiles = {row.get("profile_sha256") for row in tier_descriptors.values()}
    if len(protected) != 1 or len(profiles) != 1:
        _fail("SIGNAL_DESIGN_PROFILE_PAIR_MISMATCH")
    calibration_cap = next(iter(protected))
    cap = min(earliest, calibration_cap)
    native = fitted.product.native_prefix
    for value in (fitted.product.index.get("cutoff_utc"), native.get("prefix_last_close_utc"),
                  native.get("prefix_end_exclusive_utc")):
        if _signal_design_utc(value, "SIGNAL_DESIGN_PREFIX_CUTOFF_INVALID") > cap:
            _fail("SIGNAL_DESIGN_TRAIN_CAP_EXCEEDED")
    # Guard the held surface before replay: a future fit is not made safe by
    # clipping its output after it has already consumed judging observations.
    _signal_design_frame(fitted._frame, cap)
    fine = fitted._spread_source_product
    if type(fine) is not native_prefix.NativePrefixProduct:
        _fail("SIGNAL_DESIGN_FINE_PREFIX_REQUIRED")
    for value in (fine.index.get("cutoff_utc"), fine.native_prefix.get("prefix_last_close_utc"),
                  fine.native_prefix.get("prefix_end_exclusive_utc")):
        if _signal_design_utc(value, "SIGNAL_DESIGN_FINE_PREFIX_CUTOFF_INVALID") > cap:
            _fail("SIGNAL_DESIGN_TRAIN_CAP_EXCEEDED")
    _signal_design_frame(fitted._spread_source_frame, cap)
    refreshed = _reverify_fitted_timeframe(fitted, config)
    frame = _signal_design_frame(refreshed._frame, cap)
    plan_body, attempts = _verify_attempt_roster(plan=cell_plan.horizon_plan,
        attempt_artifacts=cell_plan.attempt_artifacts(), fitted=refreshed, regime=cell_plan.regime,
        search_process=process)
    if len(attempts) != edition.attempt_count:
        _fail("SIGNAL_DESIGN_COMPLETE_ROSTER_REQUIRED" if edition.edition_id == planner.NOISE101_SOURCE_EDITION_ID
            else "SIGNAL_DESIGN_COMPLETE_300_ROSTER_REQUIRED")
    anchor = plan_body["anchor"]
    if (anchor.get("anchor_id") != metadata["anchor"]["anchor_id"]
            or anchor.get("selection_cutoff_utc") != metadata["anchor"]["selection_cutoff_utc"]
            or anchor.get("process_precommit_receipt_sha256")
            != metadata["process_precommit_receipt_sha256"]
            or _signal_design_utc(anchor["selection_cutoff_utc"],
                "SIGNAL_DESIGN_ANCHOR_CUTOFF_INVALID") > cap):
        _fail("SIGNAL_DESIGN_JUDGING_PLAN_MISMATCH")
    geometry = plan_body.get("derived_dependency_geometry")
    if type(geometry) is not dict or type(geometry.get("warmup_bars")) is not int:
        _fail("SIGNAL_DESIGN_DEPENDENCY_GEOMETRY_INVALID")
    roster = []
    for attempt in attempts:
        for fold in (_causal_folds(attempt) if attempt.plan_attempt["status"] == "PLANNED" else ()):
            for field in ("fit_end_exclusive_utc", "validation_end_exclusive_utc"):
                if _signal_design_utc(fold[field], "SIGNAL_DESIGN_FOLD_END_INVALID") > cap:
                    _fail("SIGNAL_DESIGN_TRAIN_CAP_EXCEEDED")
        roster.append({"ordinal": attempt.ordinal,
            "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
            "process_candidate_id": attempt.artifacts.process_candidate_id,
            "status": attempt.plan_attempt["status"],
            "reason_code": attempt.plan_attempt.get("reason_code"),
            "trial_definition_sha256": attempt.artifacts.trial_definition_sha256,
            "refit_recipe_sha256": attempt.artifacts.refit_recipe_sha256})
    binding = {"schema_version": SIGNAL_DESIGN_REQUEST_SCHEMA,
        "authority_scope": "TRAIN_DESIGN_LABELS_ONLY",
        "judging_scope": "AUTHENTICATED_HISTORICAL_ANCHOR_PLAN_ONLY",
        "declared_slots_supported": False,
        "grant_is_separate_from_planning_metadata": True,
        "validation_evidence": False, "qualification_authority": False,
        "current_or_trading_authority": False, "score_or_shortlist_authority": False,
        "generation_context_sha256": sink.context.context_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
        "candidate_domain_sha256": process_body["candidate_domain_sha256"],
        "planning_metadata_sha256": _sha256(metadata),
        "causal_precommit_sha256": _sha256(precommit),
        "production_anchor_plan_receipt_sha256": anchor_plan_receipt.receipt_sha256,
        "production_anchor_plan_sha256": verified_anchor_plan["anchor_plan_sha256"],
        "cell_plan_receipt_sha256": cell_plan.receipt_sha256,
        "horizon_plan_receipt_sha256": cell_plan.horizon_plan.receipt_sha256,
        "cell": dict(plan_body["cell"]),
        "config_sha256": frozen_pipeline_config.config_sha256,
        "effective_config_sha256": frozen_pipeline_config.effective_config_sha256,
        "training_label_cap_utc": cap.isoformat().replace("+00:00", "Z"),
        "earliest_judging_cutoff_utc": earliest.isoformat().replace("+00:00", "Z"),
        "spread_calibration_cutoff_utc": calibration_cap.isoformat().replace("+00:00", "Z"),
        "retrospective_frozen_spread_scenario": True,
        "spread_profile_sha256": next(iter(profiles)),
        "frozen_tier_pair_contract_sha256": _sha256(refreshed.frozen_tier_pair_contract),
        "native_prefix_index_sha256": refreshed.product.index_sha256,
        "native_prefix_content_sha256": native["prefix_content_sha256"],
        "fit_prefix_extent": {"requested_cutoff_utc": refreshed.product.index["cutoff_utc"],
            "first_open_utc": native["prefix_first_open_utc"],
            "last_close_utc": native["prefix_last_close_utc"], "rows": native["prefix_rows"]},
        "native_fitted_frame_sha256": refreshed.frame_sha256,
        "normalized_fitted_frame_sha256": _frame_sha256(frame),
        "execution_spec_receipt_sha256": _sha256(dict(refreshed.execution_spec_receipt)),
        "regime_fit_decision_sha256": refreshed.regime_fit_decision.receipt_sha256,
        "train_regime_support_sha256": _sha256(dict(refreshed.train_regime_support)),
        "source_sha256": {"executor": _file_sha256(Path(__file__)),
            "labeler": _file_sha256(Path(signal_opportunities.__file__))},
        "labeler_schema_version": signal_opportunities.SCHEMA_VERSION,
        "attempt_count": len(roster), "attempt_roster": roster,
        "attempt_roster_sha256": _sha256(roster)}
    if edition.edition_id == planner.NOISE101_SOURCE_EDITION_ID:
        binding["source_edition"] = edition.to_dict()
        binding["domain_roster_sha256"] = edition.domain_roster_sha256_by_timeframe[cell_plan.timeframe]
    return binding, refreshed, frame, config, plan_body, attempts


def issue_signal_opportunity_design_request(*, process, sink, planning_request,
        anchor_plan_receipt, causal_precommit_payload, cell_plan, fitted, frozen_pipeline_config,
        declared_slot_roster=None
) -> SignalOpportunityDesignRequest:
    """Issue a distinct descriptive TRAIN grant from verified planning metadata.

    This does not alter that metadata's no-return planning permission, create a
    causal PASS, or make the resulting labels validation/WFV evidence.
    """
    binding, *_ = _signal_design_inputs(process=process, sink=sink,
        planning_request=planning_request, anchor_plan_receipt=anchor_plan_receipt,
        causal_precommit_payload=causal_precommit_payload,
        cell_plan=cell_plan, fitted=fitted, frozen_pipeline_config=frozen_pipeline_config,
        declared_slot_roster=declared_slot_roster)
    encoded = _canonical_bytes(binding)
    identifier = hashlib.sha256(encoded).hexdigest()
    entry = sink.put_json("signal-design-request/" + identifier,
        "signal_opportunity_train_design_request", binding)
    return SignalOpportunityDesignRequest(encoded, _canonical_bytes(entry), planning_request, anchor_plan_receipt,
        _canonical_bytes(dict(causal_precommit_payload)), _SIGNAL_DESIGN_REQUEST_SEAL)


def describe_signal_opportunity_design(*, request, process, sink, cell_plan,
        fitted, frozen_pipeline_config) -> SignalOpportunityDesignExecution:
    """Stream every requested label; never execute an account or select a winner."""
    if (type(request) is not SignalOpportunityDesignRequest
            or request._seal is not _SIGNAL_DESIGN_REQUEST_SEAL):
        _fail("SIGNAL_DESIGN_REQUEST_REQUIRED")
    binding, refreshed, frame, config, plan_body, attempts = _signal_design_inputs(
        process=process, sink=sink, planning_request=request._planning_request,
        anchor_plan_receipt=request._anchor_plan_receipt,
        causal_precommit_payload=json.loads(request._causal_precommit_json.decode("ascii")),
        cell_plan=cell_plan, fitted=fitted, frozen_pipeline_config=frozen_pipeline_config)
    if _canonical_bytes(binding) != request._body_json:
        _fail("SIGNAL_DESIGN_REQUEST_BINDING_CHANGED")
    request_entry = json.loads(request._entry_json.decode("ascii"))
    authority_spool._verify_artifact_file(sink.context, request_entry["descriptor"])
    if request_entry["descriptor"]["sha256"] != request.receipt_sha256:
        _fail("SIGNAL_DESIGN_REQUEST_ARTIFACT_MISMATCH")
    pricing = Backtester(config)  # Source/cost helpers only; no account simulation.
    totals = {key: 0 for key in ("roster_count", "presearch_rejected_count", "evaluated_attempt_count",
        "batch_count", "opportunity_count", "closed_count", "right_censored_count",
        "no_next_observation_count", "unavailable_regime_count", "source_observation_receipt_count",
        "source_observation_row_count")}
    observation_receipts_seen: set[str] = set()
    cap = _signal_design_utc(binding["training_label_cap_utc"], "SIGNAL_DESIGN_CAP_INVALID")

    def line(value):
        return _canonical_bytes(value) + b"\n"

    def chunks():
        yield line({"record_type": "HEADER", "schema_version": SIGNAL_DESIGN_TAPE_SCHEMA,
            "request_sha256": request.receipt_sha256, "request": binding,
            "complete_cost_net_profit_available": False,
            "source_observation_reference_semantics": "EXACT_RECEIPT_EMITTED_ONCE_BEFORE_FIRST_SHA256_REFERENCE",
            "swap_status": "UNAVAILABLE_PRICE_UNIT_CONVERSION"})
        for attempt in attempts:
            totals["roster_count"] += 1
            yield line({"record_type": "ATTEMPT", **binding["attempt_roster"][attempt.ordinal]})
            if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                totals["presearch_rejected_count"] += 1
                continue
            if attempt.plan_attempt["status"] != "PLANNED":
                _fail("SIGNAL_DESIGN_ATTEMPT_DISPOSITION_INVALID")
            strategy, _ = _runtime_strategy(attempt, timeframe=cell_plan.timeframe)
            surface = _build_full_prefix_strategy_surface(frame=frame, strategy=strategy,
                fitted=refreshed, symbol=cell_plan.symbol, timeframe=cell_plan.timeframe)
            _signal_design_frame(surface.features, cap)
            totals["evaluated_attempt_count"] += 1
            config_sha = _sha256({"effective_config_sha256": binding["effective_config_sha256"],
                "trial_definition_sha256": attempt.artifacts.trial_definition_sha256,
                "refit_recipe_sha256": attempt.artifacts.refit_recipe_sha256})
            for fold_ordinal, fold in enumerate(_causal_folds(attempt)):
                partitions = _fold_partitions(frame=frame, fold=fold, attempt=attempt,
                    dependency_warmup_bars=plan_body["derived_dependency_geometry"]["warmup_bars"])
                for partition in partitions:
                    features, signals = _slice_full_prefix_strategy_surface(
                        partition=partition, full_prefix_surface=surface)
                    features = _signal_design_frame(features, cap)
                    first_entry, start, cutoff = _signal_design_window(partition, features, cap)
                    values = signals.to_numpy(dtype="<i4", copy=False)
                    surface_sha = _sha256({"features_sha256": _frame_sha256(features),
                        "raw_signals_sha256": hashlib.sha256(values.tobytes(order="C")).hexdigest(),
                        "partition_geometry": dict(partition.geometry_receipt),
                        "signal_start_utc": start.isoformat(), "cutoff_utc": cutoff.isoformat()})
                    observations = refreshed._entry_spread_observations.loc[features.index]
                    observation_receipt = _source_observation_receipt(refreshed, features.index)
                    observation_sha = _sha256(observation_receipt)
                    if observation_sha not in observation_receipts_seen:
                        observation_receipts_seen.add(observation_sha)
                        totals["source_observation_receipt_count"] += 1
                        totals["source_observation_row_count"] += len(observation_receipt["observations"])
                        yield line({"record_type": "SOURCE_OBSERVATIONS", "sha256": observation_sha,
                            "receipt": observation_receipt})
                    for tier in ("P75", "P95"):
                        spread_tier = refreshed._spread_tiers[tier]
                        entry, exits, spread_meta = pricing._build_half_spread_arrays(features,
                            refreshed.instrument_spec, spread_tier=spread_tier, timeframe=cell_plan.timeframe)
                        states, admission = pricing._entry_spread_admission_inputs(features, spread_tier, observations)
                        cost = pricing._numerical_execution_cost_attestation(entry_half_spread=entry,
                            intrabar_half_spread=exits, spread_meta=spread_meta, spec=refreshed.instrument_spec)
                        numerical = {"cost_attestation": cost, "spread_model": spread_meta,
                            "entry_half_spread_f64le_sha256": hashlib.sha256(
                                np.asarray(entry, dtype="<f8").tobytes(order="C")).hexdigest(),
                            "exit_half_spread_f64le_sha256": hashlib.sha256(
                                np.asarray(exits, dtype="<f8").tobytes(order="C")).hexdigest()}
                        admission_binding = {**admission,
                            "all_states_sha256": _sha256(states.tolist()),
                            "source_observation_receipt_sha256": observation_sha}
                        provenance = signal_opportunities.OpportunityProvenance(strategy.name,
                            config_sha, surface_sha, tier, _sha256(numerical), _sha256(admission_binding))
                        batch = signal_opportunities.label_signal_opportunities(features, signals,
                            strategy=strategy, spec=refreshed.instrument_spec, symbol=cell_plan.symbol,
                            timeframe=cell_plan.timeframe, cutoff_utc=cutoff, signal_start_utc=start,
                            entry_half_spreads=entry, exit_half_spreads=exits,
                            entry_admission=states, provenance=provenance)
                        summary = {"record_type": "BATCH", "attempt_ordinal": attempt.ordinal,
                            "fold_ordinal": fold_ordinal, "partition": partition.role, "tier": tier,
                            "partition_geometry": dict(partition.geometry_receipt),
                            "first_entry_bar": first_entry, "signal_start_utc": batch.signal_start_utc,
                            "cutoff_utc": batch.cutoff_utc, "provenance": asdict(provenance),
                            "pip_size": batch.pip_size, "numerical_spread_binding": numerical,
                            "admission_binding": admission_binding,
                            "raw_nonzero_count": batch.raw_nonzero_count,
                            "excluded_before_start_count": batch.excluded_before_start_count,
                            "window_nonzero_count": batch.window_nonzero_count,
                            "unavailable_regime_count": batch.unavailable_regime_count,
                            "descriptive_groups": _signal_design_groups(batch.records),
                            "qualification_authority": False}
                        totals["batch_count"] += 1
                        totals["opportunity_count"] += len(batch.records)
                        totals["unavailable_regime_count"] += batch.unavailable_regime_count
                        yield line(summary)
                        for record in batch.records:
                            totals[{"CLOSED": "closed_count", "RIGHT_CENSORED": "right_censored_count",
                                "NO_NEXT_OBSERVATION": "no_next_observation_count"}[record.status]] += 1
                            yield line({"record_type": "OPPORTUNITY", "attempt_ordinal": attempt.ordinal,
                                "fold_ordinal": fold_ordinal, "partition": partition.role, "tier": tier,
                                "label": asdict(record)})
                        del batch
                del partitions
            del surface
        if (totals["roster_count"] != binding["attempt_count"] or totals["evaluated_attempt_count"]
                + totals["presearch_rejected_count"] != binding["attempt_count"]):
            _fail("SIGNAL_DESIGN_COMPLETE_ROSTER_MISMATCH")
        frozen_pipeline_config.verify_current(sink.context)
        if (_frame_sha256(refreshed._frame) != binding["native_fitted_frame_sha256"]
                or _file_sha256(Path(__file__)) != binding["source_sha256"]["executor"]
                or _file_sha256(Path(signal_opportunities.__file__)) != binding["source_sha256"]["labeler"]):
            _fail("SIGNAL_DESIGN_SOURCE_CHANGED")
        native_prefix.reverify_activated_native_prefix_product(refreshed.product,
            sink.context, require_current_activation=True)
        yield line({"record_type": "COMPLETE", "schema_version": SIGNAL_DESIGN_TAPE_SCHEMA,
            "request_sha256": request.receipt_sha256, **totals,
            "validation_evidence": False, "qualification_authority": False,
            "current_or_trading_authority": False})

    def bounded_chunks():
        width = sink.context.limits.stream_chunk_bytes
        for encoded in chunks():
            for offset in range(0, len(encoded), width):
                yield encoded[offset:offset + width]

    entry = sink.put_opaque("signal-design-tape/" + request.receipt_sha256,
        "signal_opportunity_train_design_tape", bounded_chunks(), format=SIGNAL_DESIGN_TAPE_FORMAT)
    return SignalOpportunityDesignExecution(_canonical_bytes({
        "schema_version": SIGNAL_DESIGN_TAPE_SCHEMA, "status": "DESCRIPTIVE_COMPLETE",
        "authority_scope": "TRAIN_DESIGN_LABELS_ONLY", "request_sha256": request.receipt_sha256,
        "artifact_entry": entry, **totals, "validation_evidence": False,
        "qualification_authority": False, "current_or_trading_authority": False,
        "score_or_shortlist_authority": False}))


def execute_current_observed_cell(
    *,
    process: Any,
    sink: authority_spool.SymbolAuthoritySink,
    cell_plan: Any,
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> CurrentObservedCellExecution:
    """Execute the preserved v15 current-search edition."""
    return _execute_current_observed_cell(process=process, sink=sink, cell_plan=cell_plan,
        fitted=fitted, config=config, _generation=winner.CURRENT_GENERATION)


def execute_transport_current_observed_cell(
    *, process: Any, sink: authority_spool.SymbolAuthoritySink, cell_plan: Any,
    fitted: FittedNativeSearchTimeframe, config: PipelineConfig,
) -> CurrentObservedCellExecution:
    """Execute v16 transport under the same existing current-search grants."""
    return _execute_current_observed_cell(process=process, sink=sink, cell_plan=cell_plan,
        fitted=fitted, config=config, _generation=winner.V16_GENERATION)


def execute_independent_r_current_observed_cell(
    *, process: Any, sink: authority_spool.SymbolAuthoritySink, cell_plan: Any,
    fitted: FittedNativeSearchTimeframe, config: PipelineConfig,
    _fresh_fit_owner=None,
) -> CurrentObservedCellExecution:
    """Execute the explicitly frozen V19 current-refit edition."""
    import pm_production_search_planner as planner
    if not planner.source_edition_info(process).independent_signal_r_enabled:
        _fail("INDEPENDENT_R_PROCESS_REQUIRED")
    return _execute_current_observed_cell(process=process, sink=sink, cell_plan=cell_plan,
        fitted=fitted, config=config, _generation=winner.V19_GENERATION,
        _fresh_fit_owner=_fresh_fit_owner)


def _publish_current_independent_r_bound_fit(*, sink, process, cell_plan, fitted,
        config, receipt, fold_plan):
    import pm_production_config_binding as config_binding
    metadata = {"binding_mode": "CURRENT_OBSERVED_INDEPENDENT_R",
        "binding": {"generation_context_sha256": sink.context.context_sha256,
            "producer_request_sha256": cell_plan.receipt_sha256,
            "process_sha256": process.receipt_sha256,
            "artifact_namespace": receipt["artifact_namespace"],
            "effective_config_sha256": _independent_r_configuration_sha256(config),
            "horizon_plan_receipt_sha256": cell_plan.horizon_plan.receipt_sha256,
            "cell_plan_receipt_sha256": cell_plan.receipt_sha256},
        "snapshot_issued_at_utc": _now_utc_seconds(),
        "frame_sha256": fitted.frame_sha256, "fold_plan": fold_plan,
        "entry_observations_sha256": hashlib.sha256(pd.util.hash_pandas_object(
            fitted._entry_spread_observations, index=True).values.tobytes()).hexdigest(),
        "runtime": _bound_fit_runtime(fitted.regime_parameters),
        "target_carrier": _bound_fit_carrier(fitted.product),
        "fine_carrier": _bound_fit_carrier(fitted._spread_source_product),
        "base_columns": [{"name": name, "dtype": str(dtype)}
            for name, dtype in fitted._frame.dtypes.items() if name not in _REGIME_COLUMNS],
        "receipts": {"source_group_decision": fitted.source_group_decision.to_dict(),
            "regime_fit_decision": fitted.regime_fit_decision.to_dict(),
            "source_contract_seed": dict(fitted.source_contract_seed),
            "cost_ladder_contract": dict(fitted.cost_ladder_contract),
            "source_contract_seeds": fitted.source_contract_seeds,
            "frozen_tier_pair_contract": fitted.frozen_tier_pair_contract,
            "regime_parameters": dict(fitted.regime_parameters),
            "train_regime_support": dict(fitted.train_regime_support),
            "instrument_spec": asdict(fitted.instrument_spec),
            "execution_spec_receipt": dict(fitted.execution_spec_receipt)}}
    return current_refit.spool_bound_fitted_state(sink, frame=fitted._frame, metadata=metadata)


def _execute_current_observed_cell(
    *, process: Any, sink: authority_spool.SymbolAuthoritySink, cell_plan: Any,
    fitted: FittedNativeSearchTimeframe, config: PipelineConfig, _generation: Any,
    _fresh_fit_owner=None,
) -> CurrentObservedCellExecution:
    """Execute one sealed latest-cutoff observed cell through raw Winner.

    The independently fitted native frame is byte-replayed and cross-bound to
    the sealed current cell plan before any strategy evaluation.  The exact current
    Horizon/Raw-Winner binding is then persisted before evaluation begins.
    Infrastructure failures raise and abort the current generation; only a
    precommitted source-boundary shortfall can return without raw production.
    """

    _require_executable_bound_fit(fitted)
    stage: Path | None = None
    staged: list[_StagedAttemptEvidence] = []
    stage_reservation: Any = None
    stage_reservation_scope: Any = None
    primary_error: BaseException | None = None
    try:
        refreshed, receipt, plan_binding = _verified_current_observed_inputs(
            process=process,
            sink=sink,
            cell_plan=cell_plan,
            fitted=fitted,
            config=config,
            _fresh_fit_owner=_fresh_fit_owner,
        )
        normalized_regime = str(cell_plan.regime).upper()
        plan_body, attempts = _verify_attempt_roster(
            plan=cell_plan.horizon_plan,
            attempt_artifacts=cell_plan.attempt_artifacts(),
            fitted=refreshed,
            regime=normalized_regime,
            search_process=process,
        )
        planned = tuple(
            attempt
            for attempt in attempts
            if attempt.plan_attempt["status"] == "PLANNED"
        )
        if not planned:
            return CurrentObservedCellExecution(
                status="UNSEARCHED_SOURCE_BOUNDARY_SHORTFALL",
                attempted_trials_exact=0,
                production=None,
                cross_binding_bytes=None,
                cross_binding_entry=None,
            )
        fold_plan = _winner_fold_plan(attempts)
        minimum_folds = int(winner.CURRENT_SELECTION_POLICY["min_causal_folds"])
        if len(fold_plan) < minimum_folds:
            _fail("WINNER_FOLD_PLAN_BELOW_EVIDENCE_FLOOR")

        normalized_attempts, execution_contracts, count_receipts = (
            historical_adapter._strict_attempt_artifacts(
                cell_plan.attempt_artifacts(),
                symbol=cell_plan.symbol,
                timeframe=cell_plan.timeframe,
                regime=normalized_regime,
                plan_body=plan_body,
                plan_receipt_sha256=plan_binding["plan_receipt_sha256"],
                native_prefix_product=refreshed.product,
            )
        )
        source_observation_cache: dict[str, tuple[str, bytes]] = {}
        independent_inputs = {} if _generation.independent_signal_r_enabled else None
        independent_array_entries = {} if _generation.independent_signal_r_enabled else None
        partition_cost_precommitments = _partition_cost_precommitments(
            attempts=attempts,
            fold_plan=fold_plan,
            plan_body=plan_body,
            fitted=refreshed,
            config=config,
            source_observation_cache=source_observation_cache,
            _prepared_inputs=independent_inputs,
        )
        precommitted_at = _now_utc_seconds()
        attempts_frozen_at = _next_observed_utc_second(precommitted_at)
        freeze = (raw_winner.bound_frozen_plan_freezer_for_generation(_generation)
            if _generation.independent_signal_r_enabled else {
                winner.CURRENT_GENERATION.selection_version: raw_winner.freeze_winner_search_plan_v4,
                winner.V16_GENERATION.selection_version: raw_winner.freeze_winner_search_plan_v5
            }[_generation.selection_version])
        frozen = freeze(
            sink,
            symbol=cell_plan.symbol,
            timeframe=cell_plan.timeframe,
            regime=normalized_regime,
            precommitted_at_utc=precommitted_at,
            attempts_frozen_at_utc=attempts_frozen_at,
            fold_plan=fold_plan,
            attempt_artifacts=normalized_attempts,
            precommitted_evidence_roles=_precommitted_evidence_roles(attempts),
            horizon_execution_contracts=execution_contracts,
            source_contract_seeds=refreshed.source_contract_seeds,
            frozen_tier_pair_contract=refreshed.frozen_tier_pair_contract,
            risk_unit_contract=_generation.risk_unit_contract,
            partition_cost_precommitments=partition_cost_precommitments,
            artifact_namespace=receipt["artifact_namespace"],
            **({"bound_fitted_state_entry": _publish_current_independent_r_bound_fit(
                sink=sink, process=process, cell_plan=cell_plan, fitted=refreshed,
                config=config, receipt=receipt, fold_plan=fold_plan),
                "selection_complexity_roster": historical_adapter._frozen_selection_complexity_from_process(
                    process.to_dict(), normalized_attempts, timeframe=cell_plan.timeframe)}
                if _generation.independent_signal_r_enabled else {}),
        )
        raw_ids = _raw_attempt_ids(frozen, attempts)
        cross_binding_bytes = _build_current_cross_binding_bytes(
            cell_plan=cell_plan,
            process=process,
            plan_body=plan_body,
            plan_binding=plan_binding,
            frozen=frozen,
            attempt_artifacts=normalized_attempts,
            fitted=refreshed,
            horizon_execution_contracts=execution_contracts,
            source_count_receipts=count_receipts,
        )
        cross_binding_entry = _persist_current_cross_binding(
            sink=sink,
            cell_plan=cell_plan,
            encoded=cross_binding_bytes,
        )

        latest_boundary = _utc(
            frozen.precommitment["latest_evidence_boundary_utc"],
            "LATEST_EVIDENCE_BOUNDARY_INVALID",
        )
        now = datetime.now(UTC).replace(microsecond=0)
        if now < datetime.fromisoformat(latest_boundary.replace("Z", "+00:00")):
            _fail("EVIDENCE_BOUNDARY_NOT_CLOSED")
        evidence_started = _next_observed_utc_second(attempts_frozen_at)
        source_observation_references = _require_source_observation_references(
            _publish_source_observation_references(
                sink, frozen=frozen, source_observation_cache=source_observation_cache))
        reservation_scope = authority_spool.reserve_transient_publication(
            sink.context
        )
        stage_reservation = reservation_scope.__enter__()
        stage_reservation_scope = reservation_scope
        stage = _current_stage_directory(
            sink=sink,
            cell_plan=cell_plan,
            frozen=frozen,
        )
        horizon_plan_receipt_sha256 = _sha256(plan_body)
        for attempt, raw_id in zip(attempts, raw_ids, strict=True):
            if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                outcome = _process_rejection_outcome(
                    attempt=attempt,
                    symbol=cell_plan.symbol,
                    timeframe=cell_plan.timeframe,
                    regime=normalized_regime,
                )
                outcome_sha, outcome_size = _canonical_digest_and_size(
                    {"attempt_id": raw_id, "outcome": outcome.to_dict()}
                )
                staged.append(
                    _StagedAttemptEvidence(
                        ordinal=attempt.ordinal,
                        kind="PROCESS_REJECTED",
                        path=None,
                        size_bytes=outcome_size,
                        sha256=outcome_sha,
                    )
                )
                continue
            body = _build_planned_attempt_evidence(
                attempt=attempt,
                raw_attempt_id=raw_id,
                plan_body=plan_body,
                plan_receipt_sha256=horizon_plan_receipt_sha256,
                fitted=refreshed,
                config=config,
                partition_cost_precommitments=frozen.partition_cost_precommitments,
                source_observation_references=source_observation_references,
                _generation=_generation, _independent_inputs=independent_inputs,
                **({"_source_sink": sink, "_frozen": frozen,
                    "_independent_array_entries": independent_array_entries}
                    if _generation.independent_signal_r_enabled else {}),
            )
            row = _write_staged_attempt(
                stage=stage,
                ordinal=attempt.ordinal,
                value=body,
                reservation=stage_reservation,
            )
            staged.append(row)
            del body
        if len(staged) != len(attempts):
            _fail("STAGED_ATTEMPT_ROSTER_INCOMPLETE")
        evidence_completed = _now_utc_seconds()

        replayed_binding = _build_current_cross_binding_bytes(
            cell_plan=cell_plan,
            process=process,
            plan_body=plan_body,
            plan_binding=plan_binding,
            frozen=frozen,
            attempt_artifacts=normalized_attempts,
            fitted=refreshed,
            horizon_execution_contracts=execution_contracts,
            source_count_receipts=count_receipts,
        )
        replayed_entry = _persist_current_cross_binding(
            sink=sink,
            cell_plan=cell_plan,
            encoded=replayed_binding,
        )
        if (
            replayed_binding != cross_binding_bytes
            or replayed_entry != cross_binding_entry
        ):
            _fail("CURRENT_CROSS_BINDING_REPLAY_FAILED")

        def evidence_stream() -> Any:
            for attempt, raw_id, row in zip(attempts, raw_ids, staged, strict=True):
                if row.ordinal != attempt.ordinal:
                    _fail("STAGED_ATTEMPT_ORDER_INVALID", attempt.ordinal)
                if row.kind == "PROCESS_REJECTED":
                    outcome = _process_rejection_outcome(
                        attempt=attempt,
                        symbol=cell_plan.symbol,
                        timeframe=cell_plan.timeframe,
                        regime=normalized_regime,
                    )
                    sha, size = _canonical_digest_and_size(
                        {"attempt_id": raw_id, "outcome": outcome.to_dict()}
                    )
                    if sha != row.sha256 or size != row.size_bytes:
                        _fail(
                            "PROCESS_REJECTION_STAGE_BINDING_CHANGED",
                            attempt.ordinal,
                        )
                    yield {"attempt_id": raw_id, "outcome": outcome}
                    continue
                body = _reopen_staged_attempt(row)
                if body.get("attempt_id") != raw_id:
                    _fail("STAGED_ATTEMPT_IDENTITY_CHANGED", attempt.ordinal)
                yield body

        produce = raw_winner.raw_evidence_producer_for_generation(_generation)
        production = produce(
            sink,
            frozen=frozen,
            attempt_evidence=evidence_stream(),
            evidence_access_started_at_utc=evidence_started,
            evidence_access_completed_at_utc=evidence_completed,
            provenance_cross_binding_bytes=cross_binding_bytes,
            production_cell_search_plan=cell_plan,
            production_search_process=process,
            artifact_namespace=receipt["artifact_namespace"],
            **({"_independent_array_entries": independent_array_entries}
                if _generation.independent_signal_r_enabled else {}),
        )
        return CurrentObservedCellExecution(
            status="WINNER_RAW_EVIDENCE_COMPLETE",
            attempted_trials_exact=len(attempts),
            production=production,
            cross_binding_bytes=cross_binding_bytes,
            cross_binding_entry=cross_binding_entry,
        )
    except Exception as exc:
        primary_error = exc
        if isinstance(exc, AuthoritativeSearchExecutionError):
            raise
        code = _technical_failure_code(getattr(exc, "code", None))
        raise AuthoritativeSearchExecutionError(
            code,
            (type(exc).__name__, str(exc)),
        ) from exc
    finally:
        _close_reserved_stage(
            stage=stage,
            rows=staged,
            reservation=stage_reservation,
            reservation_scope=stage_reservation_scope,
            primary_error=primary_error,
        )


def _verify_capture_horizon_plan_identity(
    capture: Any,
    *,
    timeframe: str,
    regime: str,
    horizon_plan: Any,
) -> None:
    """Bind execution to the exact Horizon capability frozen in the capture."""

    key = (timeframe, regime)
    try:
        bound_plan = capture._horizon_plans.get(key)
        bound_body = capture._horizon_plan_bodies.get(key)
        bound_binding = capture._bindings.get(key)
        received_body = horizon_plan.to_dict()
        received_binding = historical.historical_horizon_plan_binding(horizon_plan)
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "HISTORICAL_CAPTURE_HORIZON_PLAN_BINDING_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        bound_plan is not horizon_plan
        or type(received_body) is not dict
        or bound_body != received_body
        or bound_binding != received_binding
    ):
        _fail("HISTORICAL_CAPTURE_HORIZON_PLAN_SUBSTITUTION")


def _verified_historical_cell_plan_inputs(
    capture: Any,
    *,
    search_process: Any,
    cell_plan: Any,
    symbol: str,
    timeframe: str,
    regime: str,
) -> tuple[Any, tuple[dict[str, Any], ...]]:
    """Reopen the planner authority before accepting its execution roster."""

    try:
        import pm_production_search_planner as production_planner

        plan_verifier = (production_planner.verify_m5_derived_production_cell_search_plan
            if type(capture) is historical_adapter.DeclaredSlotSearchCapture
            and historical.declared_slot_source_kind(capture.request_body) == "m5_derived"
            else production_planner.verify_production_cell_search_plan)
        verified = plan_verifier(
            capture.context,
            process=search_process,
            sink=capture.sink,
            plan=cell_plan,
        )
        horizon_plan = cell_plan.horizon_plan
        attempt_artifacts = cell_plan.attempt_artifacts()
        process_receipt = search_process.receipt_sha256
        request_body = capture.request_body
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "HISTORICAL_CELL_PLAN_VERIFICATION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    expected_mode = {
        historical_adapter.BoundedRawWinnerCanaryCapture: "BOUNDED_RAW_WINNER_CANARY",
        historical_adapter.DeclaredSlotSearchCapture: "DECLARED_SLOT_UNISSUED",
    }.get(type(capture), "HISTORICAL_ANCHOR")
    if (
        verified is not True
        or cell_plan.mode != expected_mode
        or cell_plan.symbol != symbol
        or cell_plan.timeframe != timeframe
        or cell_plan.regime != regime
        or cell_plan.search_process_receipt_sha256 != process_receipt
        or type(request_body) is not dict
        or request_body.get("search_process_receipt_sha256") != process_receipt
        or type(attempt_artifacts) is not tuple
        or not attempt_artifacts
    ):
        _fail("HISTORICAL_CELL_PLAN_EXECUTION_BINDING_INVALID")
    return horizon_plan, attempt_artifacts


def _record_historical_raw_failure(capture: Any, handle: Any, failure: Any,
        exc: BaseException, *, attempted_trials_exact: int) -> None:
    if type(capture) is historical_adapter.HistoricalSymbolSearchCapture:
        capture.record_raw_evidence_failure(handle, failure)
    else:
        capture.record_raw_evidence_failure(handle, failure,
            **_bounded_canary_failure_diagnostic(exc, attempted_trials_exact=attempted_trials_exact))


def execute_historical_observed_cell(
    *,
    capture: (
        historical_adapter.HistoricalSymbolSearchCapture
        | historical_adapter.BoundedRawWinnerCanaryCapture
        | historical_adapter.DeclaredSlotSearchCapture
    ),
    fitted: FittedNativeSearchTimeframe,
    regime: str,
    search_process: Any,
    cell_plan: Any,
    config: PipelineConfig,
    _execution_edition: str | None = None,
) -> HistoricalObservedCellExecution:
    """Run one observed historical cell through genuine raw Winner evidence.

    The released Horizon plan and executable attempt bodies are verified before
    a separately timed raw-Winner precommitment is frozen.  All Backtester
    lanes are then completed into bounded immutable staging files before the
    raw builder is allowed to consume the first attempt.
    """

    _require_executable_bound_fit(fitted)
    if type(capture) not in {
        historical_adapter.HistoricalSymbolSearchCapture,
        historical_adapter.BoundedRawWinnerCanaryCapture,
        historical_adapter.DeclaredSlotSearchCapture,
    }:
        _fail("HISTORICAL_CAPTURE_INVALID")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    if type(capture) is historical_adapter.BoundedRawWinnerCanaryCapture and (
        not historical_adapter._verify_bounded_raw_winner_canary_capture(capture)
        or fitted is not capture.verified_fitted_timeframe
        or config is not capture.verified_pipeline_config
    ):
        _fail("CANARY_EXECUTION_IDENTITY_SUBSTITUTION")
    if type(capture) is historical_adapter.DeclaredSlotSearchCapture and (
        not historical_adapter._verify_declared_slot_capture(capture)
        or fitted is not capture.verified_fitted_timeframe
        or config is not capture.verified_pipeline_config
    ):
        _fail("DECLARED_SLOT_EXECUTION_IDENTITY_SUBSTITUTION")
    normalized_regime = str(regime or "").strip().upper()
    if normalized_regime not in book.REGIME_ORDER:
        _fail("REGIME_INVALID", normalized_regime)
    source = fitted.source_group_decision.to_dict()
    symbol = str(source.get("symbol") or "")
    timeframe = str(source.get("timeframe") or "")
    horizon_plan, attempt_artifacts = _verified_historical_cell_plan_inputs(
        capture,
        search_process=search_process,
        cell_plan=cell_plan,
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
    )
    _verify_capture_horizon_plan_identity(
        capture,
        timeframe=timeframe,
        regime=normalized_regime,
        horizon_plan=horizon_plan,
    )
    import pm_production_search_planner as planner
    frozen_execution_edition = search_process.to_dict().get("execution_edition")
    if _execution_edition != frozen_execution_edition:
        _fail("HISTORICAL_EXECUTION_EDITION_MISMATCH")
    edition = planner.source_edition_info(search_process)
    independent_r_execution = edition.independent_signal_r_enabled
    full_panel_execution = planner.is_route_a_full_panel_process(search_process) and not independent_r_execution
    if full_panel_execution and type(capture) is not historical_adapter.DeclaredSlotSearchCapture:
        _fail("ROUTE_A_FULL_PANEL_DECLARED_CAPTURE_REQUIRED")
    if planner.is_route_a_full_panel_process(search_process) and capture._winner_generation.selection_version != edition.winner_generation_id:
        _fail("ROUTE_A_FULL_PANEL_MATCHED_FLOOR_GENERATION_REQUIRED")
    attempted_exact = (
        len(attempt_artifacts) if isinstance(attempt_artifacts, Sequence) else 0
    )
    handle: historical_adapter.FrozenHistoricalCell | None = None
    stage: Path | None = None
    staged: list[_StagedAttemptEvidence] = []
    stage_reservation: Any = None
    stage_reservation_scope: Any = None
    result: HistoricalObservedCellExecution | None = None
    primary_error: BaseException | None = None
    stage_custody: _StageCapacityCustody | None = None
    evidence_started: str | None = None
    evidence_completed: str | None = None
    declared_mode = type(capture) is historical_adapter.DeclaredSlotSearchCapture
    declared_header = None
    declared_units = []
    declared_records = []
    declared_binding_sha256 = None
    declared_scope = None
    bound_fit_issue_owner = None
    bound_fitted_state_entry = None
    if declared_mode:
        declared_scope = _declared_unit_lease(capture.context, capture.request.to_dict())
        declared_scope.__enter__()
    try:
        if full_panel_execution:
            _require_fresh_route_a_full_panel_request(capture.context, capture.request)
        if declared_mode:
            refreshed, bound_fit_issue_owner = _declared_fitted_for_execution(capture, fitted, config)
        else:
            refreshed = _reverify_fitted_timeframe(fitted, config)
        _reverify_cell_state_error_envelope(cell_plan=cell_plan, fitted=refreshed)
        plan_body, attempts = _verify_attempt_roster(
            plan=horizon_plan,
            attempt_artifacts=attempt_artifacts,
            fitted=refreshed,
            regime=normalized_regime,
            search_process=search_process,
        )
        attempted_exact = len(attempts)
        planned = tuple(
            attempt
            for attempt in attempts
            if attempt.plan_attempt["status"] == "PLANNED"
        )
        if not planned:
            capture.record_unsearched_cell(
                timeframe,
                normalized_regime,
                reason_code="SOURCE_BOUNDARY_SHORTFALL_ALL_CANDIDATES",
            )
            return HistoricalObservedCellExecution(
                status="UNSEARCHED_SOURCE_BOUNDARY_SHORTFALL",
                attempted_trials_exact=0,
                production=None,
                failure_code=None,
            )
        fold_plan = _winner_fold_plan(attempts)
        minimum_folds = int(winner.CURRENT_SELECTION_POLICY["min_causal_folds"])
        if len(fold_plan) < minimum_folds:
            _fail("WINNER_FOLD_PLAN_BELOW_EVIDENCE_FLOOR")
        source_observation_cache: dict[str, tuple[str, bytes]] = {}
        independent_inputs = {} if independent_r_execution else None
        independent_array_entries = {} if independent_r_execution else None
        if declared_mode:
            bound_fitted_state_entry = _publish_declared_bound_fit(capture, refreshed, fold_plan, bound_fit_issue_owner)
        declared_header = (_read_declared_unit_header(capture.context, capture.request.to_dict())
                           if declared_mode else None)
        if declared_header is not None:
            handle = capture.restore_completed_unit_handle(declared_header,
                native_prefix_product=refreshed.product, attempt_artifacts=attempt_artifacts)
        else:
            partition_cost_precommitments = _partition_cost_precommitments(
                attempts=attempts,
                fold_plan=fold_plan,
                plan_body=plan_body,
                fitted=refreshed,
                config=config,
                source_observation_cache=source_observation_cache,
                _prepared_inputs=independent_inputs,
            )
            precommitted_at = _now_utc_seconds()
            attempts_frozen_at = _next_observed_utc_second(precommitted_at)
            handle = capture.freeze_observed_cell(
                timeframe,
                normalized_regime,
                native_prefix_product=refreshed.product,
                precommitted_at_utc=precommitted_at,
                attempts_frozen_at_utc=attempts_frozen_at,
                fold_plan=fold_plan,
                attempt_artifacts=attempt_artifacts,
                source_contract_seeds=refreshed.source_contract_seeds,
                frozen_tier_pair_contract=refreshed.frozen_tier_pair_contract,
                risk_unit_contract=capture._winner_generation.risk_unit_contract,
                partition_cost_precommitments=partition_cost_precommitments,
                **({"bound_fitted_state_entry": bound_fitted_state_entry} if declared_mode else {}),
            )
        raw_ids = _raw_attempt_ids(handle.frozen, attempts)
        latest_boundary = _utc(
            handle.frozen.precommitment["latest_evidence_boundary_utc"],
            "LATEST_EVIDENCE_BOUNDARY_INVALID",
        )
        now = datetime.now(UTC).replace(microsecond=0)
        if now < datetime.fromisoformat(latest_boundary.replace("Z", "+00:00")):
            _fail("EVIDENCE_BOUNDARY_NOT_CLOSED")
        evidence_started = (declared_header["evidence_access_started_at_utc"]
            if declared_header is not None else _next_observed_utc_second(attempts_frozen_at))
        source_observation_references = _require_source_observation_references(
            declared_header["source_observation_references"] if declared_header is not None
            else _publish_source_observation_references(
                capture.sink, frozen=handle.frozen, source_observation_cache=source_observation_cache))
        plan_receipt_sha256 = _sha256(plan_body)
        if declared_mode:
            request_body = capture.request.to_dict()
            binding = _declared_unit_binding(capture, refreshed, cell_plan, attempts, fold_plan, handle)
            if declared_header is None:
                declared_header = _make_declared_unit_header(capture, refreshed, cell_plan,
                    attempts, fold_plan, handle, source_observation_references, evidence_started)
                authority_spool._publish_exact_json(capture.context,
                    _declared_unit_relative(request_body, "header.json"),
                    declared_header, capture.context.limits.max_json_bytes)
            elif declared_header["binding"] != binding:
                _fail("DECLARED_UNIT_HEADER_BINDING_MISMATCH")
            declared_binding_sha256 = _sha256(binding)
            declared_units = [_declared_unit_identity(attempt, raw_id)
                for attempt, raw_id in zip(attempts, raw_ids, strict=True)]
            prepared_full_panel = None
            if full_panel_execution:
                reservation_scope = authority_spool.reserve_transient_publication(capture.context)
                stage_reservation = reservation_scope.__enter__()
                stage_reservation_scope = reservation_scope
                stage = _stage_directory(capture, handle)
                stage_custody = _create_stage_capacity_custody(root=capture.context.root,
                    stage=stage, reservation=stage_reservation,
                    binding={"generation_context_sha256": capture.context.context_sha256,
                        "producer_request_sha256": capture.request.receipt_sha256,
                        "precommit_sha256": request_body.get("precommit_sha256"),
                        "process_receipt_sha256": request_body.get("search_process_receipt_sha256"),
                        "attempt_id": request_body.get("attempt_id"),
                        "cell_id": request_body.get("cell_id"), "slot_id": request_body.get("slot_id"),
                        "artifact_namespace": request_body.get("artifact_namespace"),
                        "source_access_window_utc": request_body.get("source_access_window_utc"),
                        "cell": [handle.symbol, handle.timeframe, handle.regime],
                        "frozen_receipt_descriptor": dict(handle.frozen_receipt_descriptor)})
                prepared_full_panel = _prepare_route_a_full_panel_owner_evidence(
                    config=config, handle=handle, search_process=search_process,
                    cell_plan=cell_plan, binding=binding, refreshed=refreshed, attempts=attempts,
                    request_body=request_body, declared_header=declared_header, capture=capture,
                    normalized_regime=normalized_regime, fitted=fitted, raw_ids=raw_ids,
                    plan_body=plan_body, timeframe=timeframe, fold_plan=fold_plan, symbol=symbol,
                    stage_custody=stage_custody, reservation=stage_reservation, staged_rows=staged,
                    source_observation_references=source_observation_references)
            def produce_unit(unit):
                if prepared_full_panel is not None:
                    return prepared_full_panel.unit_body(unit)
                attempt = attempts[unit["ordinal"]]
                if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                    outcome = _process_rejection_outcome(attempt=attempt, symbol=symbol,
                        timeframe=timeframe, regime=normalized_regime)
                    return {"attempt_id": unit["raw_attempt_id"], "outcome": outcome.to_dict()}
                return _build_planned_attempt_evidence(attempt=attempt,
                    raw_attempt_id=unit["raw_attempt_id"], plan_body=plan_body,
                    plan_receipt_sha256=plan_receipt_sha256, fitted=refreshed, config=config,
                    partition_cost_precommitments=handle.frozen.partition_cost_precommitments,
                    source_observation_references=source_observation_references,
                    _entry_admission_preparation=entry_admission_preparation,
                    _generation=capture._winner_generation, _independent_inputs=independent_inputs,
                    **({"_source_sink": capture.sink, "_frozen": handle.frozen,
                        "_independent_array_entries": independent_array_entries} if independent_r_execution else {}))
            if prepared_full_panel is not None:
                declared_records, evidence_completed = _continue_declared_attempt_units(
                    capture.context, request_body, declared_binding_sha256, declared_units, produce_unit,
                    progress=getattr(capture, '_progress_observer', None))
            else:
                with pm_core._EntryAdmissionPreparation(
                        refreshed._entry_spread_observations) as entry_admission_preparation:
                    declared_records, evidence_completed = _continue_declared_attempt_units(
                        capture.context, request_body, declared_binding_sha256, declared_units, produce_unit,
                    progress=getattr(capture, '_progress_observer', None))
        else:
            reservation_scope = authority_spool.reserve_transient_publication(
                capture.context
            )
            stage_reservation = reservation_scope.__enter__()
            stage_reservation_scope = reservation_scope
            stage = _stage_directory(capture, handle)
            request_body = capture.request.to_dict()
            stage_custody = _create_stage_capacity_custody(root=capture.context.root,
                stage=stage, reservation=stage_reservation,
                binding={"generation_context_sha256": capture.context.context_sha256,
                    "producer_request_sha256": capture.request.receipt_sha256,
                    "precommit_sha256": request_body.get("precommit_sha256"),
                    "process_receipt_sha256": request_body.get("search_process_receipt_sha256"),
                    "attempt_id": request_body.get("attempt_id"),
                    "cell_id": request_body.get("cell_id"), "slot_id": request_body.get("slot_id"),
                    "artifact_namespace": request_body.get("artifact_namespace"),
                    "source_access_window_utc": request_body.get("source_access_window_utc"),
                    "cell": [handle.symbol, handle.timeframe, handle.regime],
                    "frozen_receipt_descriptor": dict(handle.frozen_receipt_descriptor)})
            plan_receipt_sha256 = _sha256(plan_body)
            for attempt, raw_id in zip(attempts, raw_ids, strict=True):
                if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                    outcome = _process_rejection_outcome(
                        attempt=attempt,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=normalized_regime,
                    )
                    outcome_sha, outcome_size = _canonical_digest_and_size(
                        {"attempt_id": raw_id, "outcome": outcome.to_dict()}
                    )
                    staged.append(
                        _StagedAttemptEvidence(
                            ordinal=attempt.ordinal,
                            kind="PROCESS_REJECTED",
                            path=None,
                            size_bytes=outcome_size,
                            sha256=outcome_sha,
                        )
                    )
                    continue
                body = _build_planned_attempt_evidence(
                    attempt=attempt,
                    raw_attempt_id=raw_id,
                    plan_body=plan_body,
                    plan_receipt_sha256=plan_receipt_sha256,
                    fitted=refreshed,
                    config=config,
                    partition_cost_precommitments=handle.frozen.partition_cost_precommitments,
                    source_observation_references=source_observation_references,
                    _generation=capture._winner_generation, _independent_inputs=independent_inputs,
                    **({"_source_sink": capture.sink, "_frozen": handle.frozen,
                        "_independent_array_entries": independent_array_entries} if independent_r_execution else {}),
                )
                row = _write_staged_attempt(
                    stage=stage,
                    ordinal=attempt.ordinal,
                    value=body,
                    reservation=stage_reservation,
                )
                staged.append(row)
                del body
            if len(staged) != len(attempts):
                _fail("STAGED_ATTEMPT_ROSTER_INCOMPLETE")
            evidence_completed = _now_utc_seconds()

        def evidence_stream() -> Any:
            if declared_mode:
                for attempt, unit in zip(attempts, declared_units, strict=True):
                    found = _read_declared_completed_unit(capture.context, capture.request.to_dict(),
                        declared_binding_sha256, unit)
                    if found is None:
                        _fail("DECLARED_UNIT_COMPLETION_ROSTER_INCOMPLETE")
                    _, body = found
                    if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                        outcome = _process_rejection_outcome(attempt=attempt, symbol=symbol,
                            timeframe=timeframe, regime=normalized_regime)
                        if body != {"attempt_id": unit["raw_attempt_id"], "outcome": outcome.to_dict()}:
                            _fail("PROCESS_REJECTION_STAGE_BINDING_CHANGED", attempt.ordinal)
                        yield {"attempt_id": unit["raw_attempt_id"], "outcome": outcome}
                    else:
                        if prepared_full_panel is not None:
                            yield {**body, "selection_addendum": {"capture": capture,
                                "prepared_stage": prepared_full_panel, "unit": unit}}
                        else:
                            yield body
                return
            for attempt, raw_id, row in zip(attempts, raw_ids, staged, strict=True):
                if row.ordinal != attempt.ordinal:
                    _fail("STAGED_ATTEMPT_ORDER_INVALID", attempt.ordinal)
                if row.kind == "PROCESS_REJECTED":
                    outcome = _process_rejection_outcome(
                        attempt=attempt,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=normalized_regime,
                    )
                    body = {"attempt_id": raw_id, "outcome": outcome}
                    sha, size = _canonical_digest_and_size(
                        {"attempt_id": raw_id, "outcome": outcome.to_dict()}
                    )
                    if sha != row.sha256 or size != row.size_bytes:
                        _fail(
                            "PROCESS_REJECTION_STAGE_BINDING_CHANGED", attempt.ordinal
                        )
                    yield body
                    continue
                body = _reopen_staged_attempt(row)
                if body.get("attempt_id") != raw_id:
                    _fail("STAGED_ATTEMPT_IDENTITY_CHANGED", attempt.ordinal)
                yield body

        if declared_mode and not full_panel_execution and not independent_r_execution:
            route_a_consumer = _consume_route_a_unit
            if route_a_consumer is not _route_a_noop_consumer:
                route_a_consumer_outcome = _run_route_a_owned_unit_continuation(
                    config=config,
                    handle=handle,
                    search_process=search_process,
                    cell_plan=cell_plan,
                    binding=binding,
                    refreshed=refreshed,
                    attempts=attempts,
                    request_body=request_body,
                    declared_header=declared_header,
                    capture=capture,
                    normalized_regime=normalized_regime,
                    route_a_consumer=route_a_consumer,
                    fitted=fitted,
                    raw_ids=raw_ids,
                    plan_body=plan_body,
                    timeframe=timeframe,
                    fold_plan=fold_plan,
                    symbol=symbol,
                )
        production = capture.complete_observed_cell(
            handle,
            attempt_evidence=evidence_stream(),
            evidence_access_started_at_utc=evidence_started,
            evidence_access_completed_at_utc=evidence_completed,
            **({"_independent_array_entries": independent_array_entries} if independent_r_execution else {}),
        )
        handle = None
        result = HistoricalObservedCellExecution(
            status="WINNER_RAW_EVIDENCE_COMPLETE",
            attempted_trials_exact=attempted_exact,
            production=production,
            failure_code=None,
        )
    except raw_winner.WinnerRawEvidenceIncomplete as exc:
        primary_error = exc
        if authority_spool.is_resource_capacity_error(exc):
            raise
        if handle is None:
            raise
        _record_historical_raw_failure(capture, handle, exc, exc,
            attempted_trials_exact=attempted_exact)
        handle = None
        result = HistoricalObservedCellExecution(
            status="TECHNICAL_FAILURE",
            attempted_trials_exact=attempted_exact,
            production=None,
            failure_code=exc.code,
        )
    except Exception as exc:
        primary_error = exc
        if authority_spool.is_resource_capacity_error(exc):
            raise
        code = _technical_failure_code(
            exc.code
            if isinstance(exc, AuthoritativeSearchExecutionError)
            else getattr(exc, "code", "AUTHORITATIVE_SEARCH_EXECUTION_FAILED")
        )
        details = (
            exc.details
            if isinstance(exc, AuthoritativeSearchExecutionError)
            else (type(exc).__name__, str(exc))
        )
        if handle is not None:
            failure = raw_winner.WinnerRawEvidenceIncomplete(
                code,
                type(exc).__name__,
            )
            _record_historical_raw_failure(capture, handle, failure, exc,
                attempted_trials_exact=attempted_exact)
            handle = None
            result = HistoricalObservedCellExecution(
                status="TECHNICAL_FAILURE",
                attempted_trials_exact=attempted_exact,
                production=None,
                failure_code=code,
            )
        else:
            failure = _infrastructure_failure(
                code=code,
                details=tuple(str(value) for value in details),
                symbol=symbol,
                timeframe=timeframe,
                regime=normalized_regime,
            )
            capture.record_search_infrastructure_failure(
                timeframe,
                normalized_regime,
                failure=failure,
                attempted_trials_exact=attempted_exact,
            )
            result = HistoricalObservedCellExecution(
                status="TECHNICAL_FAILURE",
                attempted_trials_exact=attempted_exact,
                production=None,
                failure_code=code,
            )
    finally:
        try:
            _close_historical_reserved_stage(
                stage=stage,
                rows=staged,
                reservation=stage_reservation,
                reservation_scope=stage_reservation_scope,
                primary_error=primary_error,
                custody=stage_custody,
                evidence_started=evidence_started,
                evidence_completed=evidence_completed,
            )
        finally:
            if declared_scope is not None:
                declared_scope.__exit__(None, None, None)
    if result is None:
        _fail("HISTORICAL_CELL_EXECUTION_TERMINAL_STATE_MISSING")
    return result


def execute_transport_historical_observed_cell(**kwargs: Any) -> HistoricalObservedCellExecution:
    """Run the explicit v16 historical capture without changing legacy defaults."""
    capture = kwargs.get("capture")
    if getattr(capture, "_winner_generation", None) is not winner.V16_GENERATION:
        _fail("TRANSPORT_CAPTURE_GENERATION_REQUIRED")
    return execute_historical_observed_cell(**kwargs)


def _require_fresh_route_a_full_panel_request(context, request):
    """Refuse prior execution metadata without reopening or rekeying old work."""
    if (type(context) is not authority_spool.GenerationSpoolContext
            or type(request) is not historical.DeclaredSlotProducerRequest):
        _fail("ROUTE_A_FULL_PANEL_REQUEST_INVALID")
    body = request.to_dict()
    for name in ("header.json", "raw-completion.json", "evidence-completed.json"):
        if _read_declared_unit_json(context, _declared_unit_relative(body, name)) is not None:
            _fail("ROUTE_A_FULL_PANEL_EXISTING_HEADER_REENTRY_REFUSED")


def execute_route_a_full_panel_historical_observed_cell(**kwargs: Any) -> HistoricalObservedCellExecution:
    """Execute only the explicitly frozen fresh full-panel edition."""
    import pm_production_search_planner as planner
    capture = kwargs.get("capture")
    if (type(capture) is not historical_adapter.DeclaredSlotSearchCapture
            or "_execution_edition" in kwargs):
        _fail("ROUTE_A_FULL_PANEL_DECLARED_CAPTURE_REQUIRED")
    edition = planner.frozen_search_execution_edition(capture.context, kwargs.get("search_process"))
    if not planner.is_route_a_full_panel_process(kwargs.get("search_process")):
        _fail("ROUTE_A_FULL_PANEL_PROCESS_EDITION_REQUIRED")
    return execute_historical_observed_cell(_execution_edition=edition, **kwargs)


def execute_independent_r_historical_observed_cell(**kwargs: Any) -> HistoricalObservedCellExecution:
    import pm_production_search_planner as planner
    process = kwargs.get("search_process")
    if not planner.source_edition_info(process).independent_signal_r_enabled:
        _fail("INDEPENDENT_R_PROCESS_REQUIRED")
    return execute_historical_observed_cell(
        _execution_edition=planner.INDEPENDENT_R_EXECUTION_EDITION, **kwargs)


def _make_bounded_raw_winner_canary_executor():
    """Own every post-request capability and expose one nonauthorizing facade.

    Genuine source, fitted, plan, and capture objects never cross the public
    boundary.  Closure-private HMAC keys authenticate the internal hand-offs;
    the only public result is the sealed raw-Winner diagnostic receipt.
    """

    source_key = secrets.token_bytes(32)
    plan_key = secrets.token_bytes(32)
    capture_key = secrets.token_bytes(32)
    states: dict[str, dict[str, Any]] = {}

    def _tag(
        key: bytes, schema: str, body_json: bytes, bindings: Mapping[str, Any]
    ) -> str:
        payload = _canonical_bytes(
            {
                "schema_version": schema,
                "body_sha256": hashlib.sha256(body_json).hexdigest(),
                "body_size_bytes": len(body_json),
                "runtime_bindings": dict(bindings),
            }
        )
        return hmac.new(key, payload, hashlib.sha256).hexdigest()

    def _source_bindings(value: BoundedRawWinnerCanarySourceSession) -> dict[str, Any]:
        return {
            "product_identity": id(value._product),
            "fitted_identity": id(value._fitted),
            "pipeline_config_identity": id(value._pipeline_config),
            "frozen_pipeline_config_identity": id(value._frozen_pipeline_config),
            "spool_entry_sha256": hashlib.sha256(value._spool_entry_json).hexdigest(),
        }

    def _permit_bindings(value: Any) -> dict[str, Any]:
        return {"capability_type": type(value).__name__}

    def _request_state(
        context: authority_spool.GenerationSpoolContext,
        request: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        request_body = historical.verify_bounded_raw_winner_canary_request(
            context, request
        )
        state = states.get(request.receipt_sha256)
        if (
            type(state) is not dict
            or state.get("request_nonce") != request_body["request_nonce"]
            or state.get("context_identity") != id(context)
            or state.get("generation_context_sha256") != context.context_sha256
        ):
            _fail("CANARY_EXECUTOR_REQUEST_STATE_INVALID")
        return request_body, state

    def _verify_source_session(
        context: authority_spool.GenerationSpoolContext,
        request: Any,
        source_session: Any,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        request_body, state = _request_state(context, request)
        if type(source_session) is not BoundedRawWinnerCanarySourceSession:
            _fail("CANARY_SOURCE_SESSION_OBJECT_INVALID")
        try:
            source_body = source_session.to_dict()
            expected_tag = _tag(
                source_key,
                "fxpm.bounded-raw-winner-canary-source-tag.v1",
                source_session._body_json,
                _source_bindings(source_session),
            )
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AuthoritativeSearchExecutionError(
                "CANARY_SOURCE_SESSION_INVALID", (type(exc).__name__,)
            ) from exc
        if (
            _canonical_bytes(source_body) != source_session._body_json
            or _canonical_bytes(source_session.spool_entry)
            != source_session._spool_entry_json
            or not hmac.compare_digest(source_session._issuance_tag, expected_tag)
            or source_body.get("schema_version")
            != "fxpm.bounded-raw-winner-canary-source-session.v1"
            or source_body.get("request_sha256") != request.receipt_sha256
            or source_body.get("request_nonce") != request_body["request_nonce"]
            or source_body.get("generation_context_sha256") != context.context_sha256
            or source_session.receipt_sha256
            != state.get("source_session_receipt_sha256")
            or state.get("source_session_identity") != id(source_session)
            or source_session._fitted.product is not source_session._product
        ):
            _fail("CANARY_SOURCE_SESSION_BINDING_INVALID")
        expected_source_entry = request.symbol_sink().put_json(
            f"{request_body['artifact_namespace']}/source-session",
            "bounded_raw_winner_canary_source_session",
            source_body,
        )
        if expected_source_entry != source_session.spool_entry:
            _fail("CANARY_SOURCE_SESSION_SPOOL_REOPEN_INVALID")
        try:
            import pm_production_config_binding as config_binding

            if (
                type(source_session._frozen_pipeline_config)
                is not config_binding.FrozenProductionPipelineConfig
                or type(source_session._pipeline_config) is not PipelineConfig
                or source_session._frozen_pipeline_config.config_sha256
                != context.config_sha256
                or source_session._frozen_pipeline_config.effective_config_sha256
                != source_body.get("effective_pipeline_config_sha256")
            ):
                _fail("CANARY_SOURCE_PIPELINE_CONFIG_BINDING_INVALID")
            current_config = source_session._frozen_pipeline_config.materialize(context)
            _verify_bounded_canary_held_source_state(
                source_session=source_session,
                source_body=source_body,
                current_config=current_config,
            )
        except AuthoritativeSearchExecutionError:
            raise
        except Exception as exc:
            raise AuthoritativeSearchExecutionError(
                "CANARY_SOURCE_SESSION_REVERIFY_FAILED",
                (type(exc).__name__, str(exc)),
            ) from exc
        return request_body, source_body, state

    def _verify_plan_permit(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: Any,
        process: Any,
        source_session: Any,
        plan_permit: Any,
        consume: bool,
    ) -> tuple[
        dict[str, Any], BoundedRawWinnerCanarySourceSession, PipelineConfig, Any
    ]:
        request_body, source_body, state = _verify_source_session(
            context, request, source_session
        )
        if type(plan_permit) is not _BoundedRawWinnerCanaryPlanPermit:
            _fail("CANARY_PLAN_PERMIT_OBJECT_INVALID")
        try:
            import pm_production_search_planner as search_planner

            if type(process) is not search_planner.FrozenProductionSearchProcess:
                _fail("CANARY_SEARCH_PROCESS_OBJECT_INVALID")
            search_planner.verify_frozen_production_search_process(context, process)
            permit_body = plan_permit.to_dict()
            expected_tag = _tag(
                plan_key,
                "fxpm.bounded-raw-winner-canary-plan-permit-tag.v1",
                plan_permit._body_json,
                _permit_bindings(plan_permit),
            )
        except AuthoritativeSearchExecutionError:
            raise
        except Exception as exc:
            raise AuthoritativeSearchExecutionError(
                "CANARY_PLAN_PERMIT_REVERIFY_FAILED",
                (type(exc).__name__, str(exc)),
            ) from exc
        valid_states = {
            "PLAN_READY" if consume else "PLAN_CONSUMED",
            "CAPTURE_READY",
            "CAPTURE_CONSUMED",
            "COMPLETE",
        }
        if (
            _canonical_bytes(permit_body) != plan_permit._body_json
            or not hmac.compare_digest(plan_permit._issuance_tag, expected_tag)
            or permit_body.get("schema_version")
            != "fxpm.bounded-raw-winner-canary-plan-permit.v1"
            or permit_body.get("request_sha256") != request.receipt_sha256
            or permit_body.get("source_session_receipt_sha256")
            != source_session.receipt_sha256
            or permit_body.get("search_process_receipt_sha256")
            != process.receipt_sha256
            or permit_body.get("generation_context_sha256") != context.context_sha256
            or state.get("plan_permit_receipt_sha256") != plan_permit.receipt_sha256
            or state.get("plan_permit_identity") != id(plan_permit)
            or state.get("state") not in valid_states
        ):
            _fail("CANARY_PLAN_PERMIT_BINDING_INVALID")
        if consume:
            if state["state"] != "PLAN_READY":
                _fail("CANARY_PLAN_PERMIT_REPLAY")
            state["state"] = "PLAN_CONSUMED"
        return (
            request_body,
            source_session,
            source_session._pipeline_config,
            source_session._frozen_pipeline_config,
        )

    def consume_plan_permit(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: Any,
        process: Any,
        source_session: Any,
        plan_permit: Any,
    ) -> tuple[
        dict[str, Any], BoundedRawWinnerCanarySourceSession, PipelineConfig, Any
    ]:
        return _verify_plan_permit(
            context,
            request=request,
            process=process,
            source_session=source_session,
            plan_permit=plan_permit,
            consume=True,
        )

    def verify_plan_bindings(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: Any,
        process: Any,
        source_session: Any,
        plan_permit: Any,
        plan: Any,
    ) -> bool:
        _verify_plan_permit(
            context,
            request=request,
            process=process,
            source_session=source_session,
            plan_permit=plan_permit,
            consume=False,
        )
        _, state = _request_state(context, request)
        if (
            state.get("state")
            not in {
                "PLAN_CONSUMED",
                "CAPTURE_READY",
                "CAPTURE_CONSUMED",
                "COMPLETE",
            }
            or state.get("plan_receipt_sha256") not in {None, plan.receipt_sha256}
            or plan.cell_receipt.get("bounded_canary_source_session_receipt_sha256")
            != source_session.receipt_sha256
            or plan.cell_receipt.get("bounded_canary_plan_permit_receipt_sha256")
            != plan_permit.receipt_sha256
        ):
            _fail("CANARY_CELL_PLAN_CAPABILITY_BINDING_INVALID")
        return True

    def _verify_capture_permit(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: Any,
        process: Any,
        source_session: Any,
        plan_permit: Any,
        plan: Any,
        capture_permit: Any,
        consume: bool,
    ) -> tuple[
        dict[str, Any],
        authority_spool.SymbolAuthoritySink,
        BoundedRawWinnerCanarySourceSession,
        PipelineConfig,
        Any,
    ]:
        verify_plan_bindings(
            context,
            request=request,
            process=process,
            source_session=source_session,
            plan_permit=plan_permit,
            plan=plan,
        )
        request_body, state = _request_state(context, request)
        if type(capture_permit) is not _BoundedRawWinnerCanaryCapturePermit:
            _fail("CANARY_CAPTURE_PERMIT_OBJECT_INVALID")
        try:
            permit_body = capture_permit.to_dict()
            expected_tag = _tag(
                capture_key,
                "fxpm.bounded-raw-winner-canary-capture-permit-tag.v1",
                capture_permit._body_json,
                _permit_bindings(capture_permit),
            )
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AuthoritativeSearchExecutionError(
                "CANARY_CAPTURE_PERMIT_INVALID", (type(exc).__name__,)
            ) from exc
        allowed_state = "CAPTURE_READY" if consume else state.get("state")
        if (
            _canonical_bytes(permit_body) != capture_permit._body_json
            or not hmac.compare_digest(capture_permit._issuance_tag, expected_tag)
            or permit_body.get("schema_version")
            != "fxpm.bounded-raw-winner-canary-capture-permit.v1"
            or permit_body.get("request_sha256") != request.receipt_sha256
            or permit_body.get("source_session_receipt_sha256")
            != source_session.receipt_sha256
            or permit_body.get("plan_permit_receipt_sha256")
            != plan_permit.receipt_sha256
            or permit_body.get("cell_plan_receipt_sha256") != plan.receipt_sha256
            or state.get("capture_permit_receipt_sha256")
            != capture_permit.receipt_sha256
            or state.get("capture_permit_identity") != id(capture_permit)
            or allowed_state
            not in {
                "CAPTURE_READY",
                "CAPTURE_CONSUMED",
                "COMPLETE",
            }
        ):
            _fail("CANARY_CAPTURE_PERMIT_BINDING_INVALID")
        if consume:
            if state["state"] != "CAPTURE_READY":
                _fail("CANARY_CAPTURE_PERMIT_REPLAY")
            state["state"] = "CAPTURE_CONSUMED"
        return (
            request_body,
            request.symbol_sink(),
            source_session,
            source_session._pipeline_config,
            source_session._frozen_pipeline_config,
        )

    def consume_capture_permit(
        context: authority_spool.GenerationSpoolContext,
        **kwargs: Any,
    ) -> tuple[
        dict[str, Any],
        authority_spool.SymbolAuthoritySink,
        BoundedRawWinnerCanarySourceSession,
        PipelineConfig,
        Any,
    ]:
        return _verify_capture_permit(context, consume=True, **kwargs)

    def verify_capture_bindings(
        context: authority_spool.GenerationSpoolContext,
        **kwargs: Any,
    ) -> bool:
        _verify_capture_permit(context, consume=False, **kwargs)
        return True

    def execute(
        *,
        context: authority_spool.GenerationSpoolContext,
        request: historical.BoundedRawWinnerCanaryRequest,
        process: Any,
        causal_precommit_payload: Mapping[str, Any],
        frozen_pipeline_config: Any,
    ) -> historical_adapter.BoundedRawWinnerCanaryReceipt:
        """Run one genuine latest-anchor raw-Winner canary and return its receipt."""

        _fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")

        request_body, request_sink = (
            historical.consume_bounded_raw_winner_canary_request(context, request)
        )
        request_sha = request.receipt_sha256
        if request_sha in states:
            _fail("CANARY_REQUEST_ALREADY_CONSUMED")
        states[request_sha] = {
            "state": "SOURCE_OPENING",
            "request_nonce": request_body["request_nonce"],
            "context_identity": id(context),
            "generation_context_sha256": context.context_sha256,
        }
        state = states[request_sha]
        try:
            import pm_production_config_binding as config_binding
            import pm_production_search_planner as search_planner

            if (
                type(frozen_pipeline_config)
                is not config_binding.FrozenProductionPipelineConfig
            ):
                _fail("CANARY_FROZEN_PIPELINE_CONFIG_REQUIRED")
            frozen_pipeline_config.verify_current(context)
            config = frozen_pipeline_config.materialize(context)
            source_accessed_at_utc = _now_utc_seconds()
            if source_accessed_at_utc < _utc(
                request_body["earliest_source_accessed_at_utc"],
                "CANARY_SOURCE_BOUNDARY_INVALID",
            ) or source_accessed_at_utc > _utc(
                request_body["causal_evidence_access_deadline_utc"],
                "CANARY_CAUSAL_DEADLINE_INVALID",
            ):
                _fail("CANARY_TRUSTED_SOURCE_ACCESS_OUTSIDE_WINDOW")
            product = native_prefix.produce_activated_native_prefix(
                data_dir=Path(config.data_dir),
                context=context,
                symbol=request_body["symbol"],
                timeframe=request_body["timeframe"],
                cutoff_utc=request_body["anchor"]["selection_cutoff_utc"],
                source_accessed_at_utc=source_accessed_at_utc,
                production_contract_required=True,
            )
            if product.source_state != native_prefix.AVAILABLE:
                _fail("CANARY_NATIVE_PREFIX_NOT_AVAILABLE", product.source_state)
            fitted = fit_native_timeframe_for_search(
                product=product,
                config=config,
                acquisition_attempts_exact=1,
            )
            native = dict(product.native_prefix)
            if not _canary_native_prefix_matches_request(
                native,
                index=product.index,
                request_body=request_body,
                source_accessed_at_utc=source_accessed_at_utc,
            ):
                _fail("CANARY_INTERNAL_NATIVE_PREFIX_BINDING_INVALID")
            source_body = {
                "schema_version": "fxpm.bounded-raw-winner-canary-source-session.v1",
                "request_sha256": request_sha,
                "request_nonce": request_body["request_nonce"],
                "generation_context_sha256": context.context_sha256,
                "config_sha256": context.config_sha256,
                "effective_pipeline_config_sha256": (
                    frozen_pipeline_config.effective_config_sha256
                ),
                "source_accessed_at_utc": source_accessed_at_utc,
                "selection_cutoff_utc": request_body["anchor"]["selection_cutoff_utc"],
                "native_prefix_index_sha256": product.index_sha256,
                "native_prefix_content_sha256": native["prefix_content_sha256"],
                "fitted_frame_sha256": fitted.frame_sha256,
                "source_group_decision_sha256": fitted.source_group_decision.receipt_sha256,
                "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
                "train_regime_support_receipt_sha256": fitted.train_regime_support[
                    "receipt_sha256"
                ],
                "caller_supplied_product_or_fit_accepted": False,
                "confers_downstream_or_trading_authority": False,
            }
            source_json = _canonical_bytes(source_body)
            source_entry = request_sink.put_json(
                f"{request_body['artifact_namespace']}/source-session",
                "bounded_raw_winner_canary_source_session",
                source_body,
            )
            source_entry_json = _canonical_bytes(source_entry)
            source_tag = _tag(
                source_key,
                "fxpm.bounded-raw-winner-canary-source-tag.v1",
                source_json,
                {
                    "product_identity": id(product),
                    "fitted_identity": id(fitted),
                    "pipeline_config_identity": id(config),
                    "frozen_pipeline_config_identity": id(frozen_pipeline_config),
                    "spool_entry_sha256": hashlib.sha256(source_entry_json).hexdigest(),
                },
            )
            source_session = BoundedRawWinnerCanarySourceSession(
                source_json,
                product,
                fitted,
                config,
                frozen_pipeline_config,
                source_entry_json,
                source_tag,
            )
            state.update(
                {
                    "state": "SOURCE_FITTED",
                    "source_session_receipt_sha256": source_session.receipt_sha256,
                    "source_session_identity": id(source_session),
                }
            )
            permit_body = {
                "schema_version": "fxpm.bounded-raw-winner-canary-plan-permit.v1",
                "request_sha256": request_sha,
                "generation_context_sha256": context.context_sha256,
                "source_session_receipt_sha256": source_session.receipt_sha256,
                "search_process_receipt_sha256": process.receipt_sha256,
                "permit_nonce": secrets.token_hex(16),
            }
            permit_json = _canonical_bytes(permit_body)
            plan_permit = _BoundedRawWinnerCanaryPlanPermit(permit_json, "")
            plan_permit = _BoundedRawWinnerCanaryPlanPermit(
                permit_json,
                _tag(
                    plan_key,
                    "fxpm.bounded-raw-winner-canary-plan-permit-tag.v1",
                    permit_json,
                    _permit_bindings(plan_permit),
                ),
            )
            state.update(
                {
                    "state": "PLAN_READY",
                    "plan_permit_receipt_sha256": plan_permit.receipt_sha256,
                    "plan_permit_identity": id(plan_permit),
                }
            )
            plan = search_planner.build_bounded_raw_winner_canary_observed_cell_plan(
                context,
                process=process,
                request=request,
                causal_precommit_payload=causal_precommit_payload,
                source_session=source_session,
                plan_permit=plan_permit,
            )
            if state.get("state") != "PLAN_CONSUMED":
                _fail("CANARY_PLANNER_DID_NOT_CONSUME_PERMIT")
            state["plan_receipt_sha256"] = plan.receipt_sha256
            state["state"] = "CAPTURE_READY"
            capture_body = {
                "schema_version": "fxpm.bounded-raw-winner-canary-capture-permit.v1",
                "request_sha256": request_sha,
                "source_session_receipt_sha256": source_session.receipt_sha256,
                "plan_permit_receipt_sha256": plan_permit.receipt_sha256,
                "cell_plan_receipt_sha256": plan.receipt_sha256,
                "permit_nonce": secrets.token_hex(16),
            }
            capture_json = _canonical_bytes(capture_body)
            unsigned_capture = _BoundedRawWinnerCanaryCapturePermit(capture_json, "")
            capture_permit = _BoundedRawWinnerCanaryCapturePermit(
                capture_json,
                _tag(
                    capture_key,
                    "fxpm.bounded-raw-winner-canary-capture-permit-tag.v1",
                    capture_json,
                    _permit_bindings(unsigned_capture),
                ),
            )
            state.update(
                {
                    "capture_permit_receipt_sha256": capture_permit.receipt_sha256,
                    "capture_permit_identity": id(capture_permit),
                }
            )
            capture = historical_adapter.open_bounded_raw_winner_canary_capture(
                context,
                request=request,
                process=process,
                plan=plan,
                source_session=source_session,
                plan_permit=plan_permit,
                capture_permit=capture_permit,
            )
            if state.get("state") != "CAPTURE_CONSUMED":
                _fail("CANARY_ADAPTER_DID_NOT_CONSUME_PERMIT")
            execute_historical_observed_cell(
                capture=capture,
                fitted=fitted,
                regime=plan.regime,
                search_process=process,
                cell_plan=plan,
                config=config,
            )
            receipt = capture.canary_receipt
            state["state"] = "COMPLETE"
            return receipt
        except Exception:
            state.setdefault("terminal_failure", True)
            raise

    return (
        execute,
        consume_plan_permit,
        verify_plan_bindings,
        consume_capture_permit,
        verify_capture_bindings,
    )


(
    execute_bounded_raw_winner_canary,
    _consume_bounded_raw_winner_canary_plan_permit,
    _verify_bounded_raw_winner_canary_plan_bindings,
    _consume_bounded_raw_winner_canary_capture_permit,
    _verify_bounded_raw_winner_canary_capture_bindings,
) = _make_bounded_raw_winner_canary_executor()


def authoritative_search_execution_contract() -> dict[str, Any]:
    dependency_sources = {
        "pm_authoritative_search_execution": _file_sha256(Path(__file__)),
        "pm_production_search_planner": _file_sha256(Path(__file__).with_name("pm_production_search_planner.py")),
        "pm_core": _file_sha256(Path(pm_core.__file__)),
        "pm_current_refit_artifacts": _file_sha256(Path(current_refit.__file__)),
        "pm_historical_production_adapter": _file_sha256(
            Path(historical_adapter.__file__)
        ),
        "pm_native_prefix_production": _file_sha256(Path(native_prefix.__file__)),
        "pm_trade_power": _file_sha256(Path(trade_power.__file__)),
        "pm_winner_raw_evidence": _file_sha256(Path(raw_winner.__file__)),
        "pm_winner_selection": _file_sha256(Path(winner.__file__)),
    }
    return {
        "schema_version": EXECUTION_CONTRACT_VERSION,
        "dependency_source_sha256": dependency_sources,
        "native_prefix_reopened_before_search": True,
        "regime_surface_acquisition": "RECOMPUTED_OR_REOPENED_FROM_ORIGINAL_BOUND_STATE",
        "released_horizon_plan_required": True,
        "sealed_current_cell_plan_required": True,
        "current_fitted_support_receipt_cross_binding_required": True,
        "current_cross_binding_persisted_before_evaluation": True,
        "separate_raw_precommitment_before_strategy_evaluation": True,
        "position_event_source": "pm_core.Backtester.position-events-v2",
        "trade_power_chain": "pm_winner_raw_evidence.PRODUCTION_TRADE_POWER_POLICY",
        "candidate_costs": "fxpm.backtester-frozen-tier-pair-contract.v1",
        "entry_admission_policy": spread_profile.ADMISSION_POLICY,
        "independent_cost_accounts_per_partition": ["P75", "P95"],
        "outer_interval_qualification": "METHOD_UNADOPTED",
        "attempt_evidence_staging": "BOUNDED_CONTENT_ADDRESSED_ATOMIC_FILES",
        "bounded_canary_single_public_executor_facade": True,
        "bounded_canary_accepts_prebuilt_source_or_fit": False,
        "bounded_canary_returns_only_sealed_nonauthorizing_receipt": True,
        "maximum_attempt_evidence_bytes": MAX_ATTEMPT_EVIDENCE_BYTES,
        "technical_failures_are_market_verdicts": False,
        "confers_causal_current_capital_publication_or_live_authority": False,
    }


AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256: Final = _sha256(
    authoritative_search_execution_contract()
)


__all__ = [
    "AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256",
    "AuthoritativeSearchExecutionError",
    "CurrentObservedCellExecution",
    "SignalOpportunityDesignRequest",
    "SignalOpportunityDesignExecution",
    "FittedNativeSearchTimeframe",
    "HistoricalObservedCellExecution",
    "TERMINAL_REGIME_FIT_POLICY_SCHEMA",
    "TERMINAL_SOURCE_CONTRACT_SCHEMA",
    "TERMINAL_SOURCE_REASON_CODE",
    "TERMINAL_TECHNICAL_REASON_CODE",
    "TerminalNativeTimeframe",
    "authoritative_search_execution_contract",
    "execute_current_observed_cell",
    "execute_transport_current_observed_cell",
    "execute_transport_historical_observed_cell",
    "issue_signal_opportunity_design_request",
    "describe_signal_opportunity_design",
    "execute_bounded_raw_winner_canary",
    "execute_historical_observed_cell",
    "fit_native_timeframe_for_search",
    "fit_terminal_native_timeframe",
    "minimum_fold_fit_rows",
    "reverify_fitted_native_timeframe_for_search",
]

__all__ += ["reopen_bound_fitted_timeframe", "bound_fitted_state_reference"]


def _train_regime_support_window(*, frame, product, symbol, timeframe, m5_view_evidence=None):
    native = dict(product.native_prefix)
    window = {
        "schema_version": "fxpm.historical-regime-fit-window.v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "prefix_content_sha256": native["prefix_content_sha256"],
        "prefix_start_utc": native["prefix_start_utc"],
        "prefix_end_exclusive_utc": native["prefix_end_exclusive_utc"],
        "prefix_rows": native["prefix_rows"],
    }
    if m5_view_evidence is not None:
        fields = {"source_kind", "base_timeframe", "view_timeframe",
                  "m5_view_contract_sha256", "view_key", "view_payload_sha256",
                  "view_membership_sha256", "base_native_prefix_index_sha256",
                  "source_accessed_at_utc"}
        if (type(m5_view_evidence) is not dict or set(m5_view_evidence) != fields
                or m5_view_evidence["source_kind"] != "m5_derived"
                or m5_view_evidence["base_timeframe"] != "M5"
                or m5_view_evidence["view_timeframe"] != timeframe
                or m5_view_evidence["base_native_prefix_index_sha256"] != product.index_sha256
                or m5_view_evidence["source_accessed_at_utc"] != native["source_accessed_at_utc"]
                or frame.empty or "_BAR_CLOSE_TS" not in frame):
            _fail("M5_DERIVED_REGIME_WINDOW_INVALID")
        for name in ("m5_view_contract_sha256", "view_key", "view_payload_sha256",
                     "view_membership_sha256", "base_native_prefix_index_sha256"):
            _hex64(m5_view_evidence[name], "M5_DERIVED_REGIME_WINDOW_INVALID")
        window = {
            "schema_version": "fxpm.historical-m5-derived-regime-fit-window.v1",
            "symbol": symbol, "timeframe": timeframe,
            "view_start_utc": pd.Timestamp(frame.index[0]).isoformat(),
            "view_end_exclusive_utc": pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1]).isoformat(),
            "view_rows": len(frame), "view_frame_sha256": _frame_sha256(frame),
            "view_binding": dict(m5_view_evidence),
            "base_m5_prefix": {
                "prefix_content_sha256": native["prefix_content_sha256"],
                "prefix_start_utc": native["prefix_start_utc"],
                "prefix_end_exclusive_utc": native["prefix_end_exclusive_utc"],
                "prefix_rows": native["prefix_rows"],
            },
        }
    return window


def _regime_fit_evidence(*, source, product, support, detector_record, derived_binding=None):
    evidence = {
        "schema_version": REGIME_FIT_EVIDENCE_VERSION,
        "source_group_decision_sha256": source.receipt_sha256,
        "native_prefix_index_sha256": product.index_sha256,
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "detector_record_sha256": detector_record["record_sha256"],
    }
    if derived_binding is not None:
        evidence["m5_view"] = dict(derived_binding.view_evidence)
    return evidence


_ROUTE_A_UNIT_BATCH_RECIPE = "ROUTE_A_APPLIED_SPREAD_POLICY_V1"
_ROUTE_A_UNIT_BATCH_POLICY_DOMAIN = b"FXPM_ROUTE_A_APPLIED_SPREAD_POLICY_V1\x00"


def _route_a_noop_consumer(*, evaluate_batch):
    """Reserved owner continuation; production sharing is not activated."""
    return None


_consume_route_a_unit = _route_a_noop_consumer


def _route_a_diagnostic_authorities():
    return {"execution_ready": False, "overall_execution_ready": False, "qualification_authority": False,
        "capital_authority": False, "paper_authority": False,
        "live_authority": False, "order_authority": False, "live_order_authority": False}


def _route_a_contain_consumer_failure(exc):
    if not isinstance(exc, Exception) or authority_spool.is_resource_capacity_error(exc):
        raise exc
    return {"schema_version": "fxpm.route-a-unit-batch-diagnostic.v1",
        "status": "REFUSED_DIAGNOSTIC_ONLY", "metadata_recipe": _ROUTE_A_UNIT_BATCH_RECIPE,
        "requests": [], "request_to_unique": [], "unique_evaluations": [], "pair_checks": [],
        "refusal": {"code": str(getattr(exc, "code", type(exc).__name__)),
            "details": [str(value) for value in getattr(exc, "details", (str(exc),))]},
        **_route_a_diagnostic_authorities()}


def _route_a_batch_specifications(specifications):
    fields = {"strategy_id", "parameters", "fold_ordinal",
        "partition_role", "spread_tier", "bookkeeping"}
    if type(specifications) not in (list, tuple):
        _fail("ROUTE_A_UNIT_BATCH_SPECIFICATIONS_INVALID")
    encoded = _canonical_bytes(specifications)
    if len(encoded) > winner.MAX_REQUEST_BYTES:
        _fail("ROUTE_A_UNIT_BATCH_SPECIFICATIONS_INVALID")
    rows = json.loads(encoded.decode("ascii"))
    for ordinal, row in enumerate(rows):
        if (type(row) is not dict or set(row) != fields
                or type(row["strategy_id"]) is not str or not row["strategy_id"]
                or type(row["parameters"]) is not dict or type(row["bookkeeping"]) is not dict
                or type(row["fold_ordinal"]) is not int or row["fold_ordinal"] < 0):
            _fail("ROUTE_A_UNIT_BATCH_REQUEST_INVALID")
        if row["partition_role"] == "final_lockbox":
            _fail("ROUTE_A_UNIT_BATCH_LOCKBOX_REFUSED")
        if row["partition_role"] not in ("optimizer_train", "tuning_validation"):
            _fail("ROUTE_A_UNIT_BATCH_ROLE_INVALID")
        if row["spread_tier"] not in ("P75", "P95"):
            _fail("ROUTE_A_UNIT_BATCH_TIER_INVALID")
        row["request_ordinal"] = ordinal
    return tuple(rows)


def _route_a_require_idle(*, closed, busy):
    if closed:
        _fail("ROUTE_A_UNIT_BATCH_CLOSED")
    if busy:
        _fail("ROUTE_A_UNIT_BATCH_BUSY")
    return True


class _RouteABatchLifecycle:
    """Nonauthorizing state only; this cannot issue an evaluator."""
    __slots__ = ("closed", "busy")

    def __init__(self):
        self.closed, self.busy = False, False

    def enter(self):
        _route_a_require_idle(closed=self.closed, busy=self.busy)
        self.busy = True

    def leave(self):
        self.busy = False

    def close(self):
        self.closed, self.busy = True, False


def _route_a_refuse_transfer(lifecycle):
    lifecycle.close()
    _fail("ROUTE_A_UNIT_BATCH_CAPABILITY_NOT_TRANSFERABLE")


def _route_a_require_scope_current(*, closed, owner_process, owner_thread,
        current_process, current_thread, window_start, window_end, observed):
    if closed:
        _fail("ROUTE_A_UNIT_BATCH_CLOSED")
    if owner_process != current_process or owner_thread != current_thread:
        _fail("ROUTE_A_UNIT_BATCH_OWNER_MISMATCH")
    if not window_start <= observed < window_end:
        _fail("ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_CLOSED")
    return True


def _route_a_require_unchanged(*, expected, observed):
    if expected != observed:
        _fail("ROUTE_A_UNIT_BATCH_SHARED_CONTENT_CHANGED")
    return True


def _route_a_pandas_content_sha256(value):
    if isinstance(value, pd.Series):
        payload = ("series", value.name, value.index, value.dtype, value.array,
            value.attrs, value.flags.allows_duplicate_labels)
    elif isinstance(value, pd.DataFrame):
        payload = ("frame", value.index, value.columns,
            tuple((value.iloc[:, index].dtype, value.iloc[:, index].array)
                for index in range(len(value.columns))),
            value.attrs, value.flags.allows_duplicate_labels)
    else:
        _fail("ROUTE_A_UNIT_BATCH_PANDAS_INPUT_INVALID")
    # Typed axes and column arrays retain values, dtype, categories and timezone.
    return hashlib.sha256(pickle.dumps(payload, protocol=5)).hexdigest()


def _route_a_fitted_shared_content(fitted):
    owner = fitted._bound_fit_owner
    owner_state = None
    if owner is not None:
        absent_slot = object()
        owner_state = {}
        for name in owner.__slots__:
            value = getattr(owner, name, absent_slot)
            if value is absent_slot:
                owner_state[name] = ("absent",)
            else:
                owner_state[name] = ("present",
                    id(value) if name in ("seal", "fitted", "original_fitted", "config") else value)
    derived = fitted._derived_binding
    derived_state = None
    if derived is not None:
        derived_state = {"context": id(derived.context), "owner": id(derived.owner),
            "request_lease": id(derived.request_lease), "calibration_lease": id(derived.calibration_lease),
            "cost_edition": id(derived.cost_edition), "pins": derived.declared_m5_source_pins,
            "view_evidence": derived.view_evidence,
            "request_view": derived.owner.view(derived.request_lease),
            "calibration_view": derived.owner.view(derived.calibration_lease)}
    def product_state(product):
        return None if product is None else (id(product), dict(product.index_descriptor),
            product.index_sha256, product.index, dict(product.native_prefix))
    return hashlib.sha256(pickle.dumps({
        "identity": id(fitted), "frame": _route_a_pandas_content_sha256(fitted._frame),
        "observations": _route_a_pandas_content_sha256(fitted._entry_spread_observations),
        "fine_frame": (None if fitted._spread_source_frame is None
            else _route_a_pandas_content_sha256(fitted._spread_source_frame)),
        "source": fitted.source_group_decision.to_dict(), "fit": fitted.regime_fit_decision.to_dict(),
        "source_seed": fitted.source_contract_seed, "cost_ladder": fitted.cost_ladder_contract,
        "parameters": fitted.regime_parameters, "support": fitted.train_regime_support,
        "instrument": vars(fitted.instrument_spec), "execution_spec": fitted.execution_spec_receipt,
        "declared_frame": fitted.frame_sha256, "seeds": fitted.source_contract_seeds,
        "pair": fitted.frozen_tier_pair_contract,
        "tiers": {key: (value.descriptor(), value.symbol, value.timeframe, value.tier,
            value.source_timeframe, value.point, value.spread_points, value.full_spread_price)
            for key, value in fitted._spread_tiers.items()},
        "product": product_state(fitted.product), "fine_product": product_state(fitted._spread_source_product),
        "owner": owner_state, "derived": derived_state}, protocol=5)).hexdigest()


def _route_a_working_fitted(fitted):
    return replace(fitted, _frame=fitted._frame.copy(deep=True),
        _entry_spread_observations=fitted._entry_spread_observations.copy(deep=True),
        _spread_source_frame=(None if fitted._spread_source_frame is None
            else fitted._spread_source_frame.copy(deep=True)),
        source_contract_seed=copy.deepcopy(fitted.source_contract_seed),
        cost_ladder_contract=copy.deepcopy(fitted.cost_ladder_contract),
        regime_parameters=copy.deepcopy(fitted.regime_parameters),
        train_regime_support=copy.deepcopy(fitted.train_regime_support),
        instrument_spec=copy.deepcopy(fitted.instrument_spec),
        execution_spec_receipt=copy.deepcopy(fitted.execution_spec_receipt),
        source_contract_seeds=copy.deepcopy(fitted.source_contract_seeds),
        frozen_tier_pair_contract=copy.deepcopy(fitted.frozen_tier_pair_contract),
        _spread_tiers={key: spread_profile.FrozenSpreadTier(value._json)
            for key, value in fitted._spread_tiers.items()})


def _route_a_evaluate_prepared_request(*, prepare_probe, verify_probe, require_current,
        make_strategy, run_strategy, verify_realized):
    require_current()
    probe = prepare_probe()
    verify_probe(probe)
    require_current()
    strategy = make_strategy()
    realized = run_strategy(strategy, probe)
    verify_realized(realized, probe)
    require_current()
    return realized


def _route_a_plain_strategy_state(strategy):
    """Copy replayable library state; other strategies keep their original compute path."""
    cls = type(strategy)
    hooks = ("__copy__", "__deepcopy__", "__getstate__", "__setstate__",
        "__reduce__", "__reduce_ex__", "__getattribute__", "__setattr__")
    if cls.__module__ != StrategyRegistry.__module__ or any(
            base.__dict__.get("__slots__") or any(name in base.__dict__ for name in hooks)
            for base in cls.__mro__ if base is not object):
        return None

    def plain(value):
        if type(value) in (str, int, bool, type(None)):
            return True
        if type(value) is float:
            return math.isfinite(value)
        if type(value) in (list, tuple):
            return all(plain(item) for item in value)
        if type(value) is dict:
            return all(type(key) is str and plain(item) for key, item in value.items())
        return False

    try:
        state = vars(strategy)
        return copy.deepcopy(state) if type(state) is dict and plain(state) else None
    except (TypeError, RecursionError):
        return None


def _route_a_strategy_surface_key(strategy, *, strategy_id, parameters, recipe):
    if (StrategyRegistry._strategies.get(strategy_id) is not type(strategy)
            or type(recipe) is not str):
        return None
    return type(strategy), strategy_id, _canonical_bytes(parameters), recipe


def _route_a_pair_check(*, signals_by_tier, results_by_tier, spec, use_spread, frozen_pair):
    missing = sorted({"P75", "P95"} - set(results_by_tier))
    if missing:
        return {"status": "INCOMPLETE_MISSING_PARTNER", "missing_tiers": missing}
    if not signals_by_tier["P75"].equals(signals_by_tier["P95"]):
        _fail("PARTITION_SIGNAL_VECTOR_TIER_MISMATCH")
    pair = pm_core.build_backtest_frozen_tier_pair_contract(results_by_tier,
        spec=spec, use_spread=use_spread)
    if pair != dict(frozen_pair):
        _fail("REALIZED_FROZEN_TIER_POLICY_DRIFT")
    return {"status": "VERIFIED", "missing_tiers": []}


from pm_current_refit_parameters import validate_parameter_domain_member as _route_a_mapping_validate_member


def _route_a_resolve_request_membership(*, specification, frozen_domains, attempt_roster, owner_projection=False):
    """Resolve a normalized request against validated, nonauthorizing JSON metadata."""
    fields = {"request_ordinal", "strategy_id", "parameters", "fold_ordinal",
        "partition_role", "spread_tier", "bookkeeping"}
    if (type(specification) is not dict or set(specification) != fields
            or type(specification["request_ordinal"]) is not int or specification["request_ordinal"] < 0
            or type(specification["strategy_id"]) is not str or not specification["strategy_id"]
            or type(specification["parameters"]) is not dict or type(specification["bookkeeping"]) is not dict
            or type(specification["fold_ordinal"]) is not int or specification["fold_ordinal"] < 0
            or specification["partition_role"] not in ("optimizer_train", "tuning_validation")
            or specification["spread_tier"] not in ("P75", "P95")
            or type(frozen_domains) is not list or type(attempt_roster) is not list or type(owner_projection) is not bool):
        _fail("ROUTE_A_MAPPING_REQUEST_INVALID")
    domain = next((value for value in frozen_domains
        if value["strategy_id"] == specification["strategy_id"]), None)
    if domain is None:
        _fail("ROUTE_A_MAPPING_DOMAIN_UNAVAILABLE")
    try:
        parameters = _route_a_mapping_validate_member(domain["parameter_domain"], specification["parameters"])
        defaults = copy.deepcopy(domain["parameter_domain"]["defaults"])
        matches = []
        for index, attempt in enumerate(attempt_roster):
            if attempt["status"] != "PLANNED" or attempt["strategy_id"] != specification["strategy_id"]:
                continue
            actual_parameters = decode_parameter_mapping(attempt["parameters"], defaults)
            if _canonical_bytes(actual_parameters) != _canonical_bytes(parameters):
                continue
            folds = [fold for fold in attempt["folds"]
                if fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"]
            if specification["fold_ordinal"] >= len(folds):
                continue
            fold = folds[specification["fold_ordinal"]]
            partition = fold["partitions"][specification["partition_role"]]
            matches.append((index, attempt, fold, partition))
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError) as error:
        _fail("ROUTE_A_MAPPING_MATCH_DATA_INVALID", type(error).__name__)
    if len(matches) != 1:
        _fail("ROUTE_A_MAPPING_MEMBERSHIP_" + ("UNAVAILABLE" if not matches else "AMBIGUOUS"))
    index, attempt, fold, partition = matches[0]
    return json.loads(_canonical_bytes({"attempt_index": index,
        "attempt_ordinal": attempt["ordinal"], "raw_attempt_id": attempt["raw_attempt_id"],
        "horizon_attempt_id": attempt["horizon_attempt_id"],
        "plan_attempt_sha256": attempt["plan_attempt_sha256"],
        "strategy_id": specification["strategy_id"], "strategy_family": domain["strategy_family"],
        "domain_sha256": domain["domain_sha256"], "parameters": parameters,
        "fold_ordinal": specification["fold_ordinal"], "fold_id": fold["fold_id"],
        "partition_role": specification["partition_role"],
        **({"partition_identity": partition} if owner_projection else {"partition_geometry": partition["geometry"]}),
        "spread_tier": specification["spread_tier"],
        "tier_reference": partition["tier_refs"][specification["spread_tier"]]}).decode("ascii"))


def _route_a_validate_mapping_snapshot(*, frozen_domains, attempt_roster,
        owner_projection=False, declared_unit_binding=None):
    """Validate copied data for legacy designs or the narrow current-owner view."""
    def plain(value):
        if type(value) in (type(None), bool, int, str):
            return True
        if type(value) is float:
            return math.isfinite(value)
        if type(value) is list:
            return all(plain(item) for item in value)
        if type(value) is dict:
            return all(type(key) is str and plain(item) for key, item in value.items())
        return False

    def text(value):
        return type(value) is str and bool(value)

    def hexadecimal(value):
        return type(value) is str and len(value) == 64 and all(char in "0123456789abcdef" for char in value)

    valid = False
    try:
        payload = {"frozen_domains": frozen_domains, "attempt_roster": attempt_roster}
        if owner_projection:
            payload["declared_unit_binding"] = declared_unit_binding
        if type(owner_projection) is bool and plain(payload):
            encoded = _canonical_bytes(payload)
            if len(encoded) <= winner.MAX_REQUEST_BYTES:
                snapshot = json.loads(encoded.decode("ascii"))
                valid = type(snapshot["attempt_roster"]) is list
                if owner_projection:
                    fields = {"generation_context_sha256", "producer_request_sha256", "executor_release_sha256",
                        "producer_source_sha256", "horizon_plan_receipt_sha256", "cell_plan_receipt_sha256",
                        "source_group_sha256", "regime_fit_sha256", "frame_sha256", "fitted_state_reference_sha256",
                        "execution_spec_sha256", "frozen_receipt_sha256", "cost_precommitments_sha256",
                        "fold_plan_sha256", "attempt_roster_sha256"}
                    binding = snapshot["declared_unit_binding"]
                    valid = valid and type(binding) is dict and set(binding) == fields and all(hexadecimal(v) for v in binding.values())
                else:
                    valid = valid and declared_unit_binding is None
    except (AuthoritativeSearchExecutionError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        valid = False
    if not valid:
        _fail("ROUTE_A_MAPPING_SNAPSHOT_INVALID")
    roles, tiers = ("optimizer_train", "tuning_validation", "final_lockbox"), ("P75", "P95")
    domains, roster = snapshot["frozen_domains"], snapshot["attempt_roster"]
    domains_valid = type(domains) is list and bool(domains)
    domain_ids = set()
    try:
        for domain in domains if type(domains) is list else []:
            required = {"strategy_id", "strategy_family", "parameter_domain", "domain_sha256"}
            if (type(domain) is not dict or not required <= set(domain)
                    or not text(domain["strategy_id"]) or domain["strategy_id"] in domain_ids
                    or not text(domain["strategy_family"]) or not hexadecimal(domain["domain_sha256"])
                    or hashlib.sha256(_canonical_bytes({key: value for key, value in domain.items()
                        if key != "domain_sha256"})).hexdigest() != domain["domain_sha256"]):
                domains_valid = False
                continue
            domain_ids.add(domain["strategy_id"])
            _route_a_mapping_validate_member(domain["parameter_domain"], domain["parameter_domain"]["defaults"])
    except (AuthoritativeSearchExecutionError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        domains_valid = False
    if not domains_valid:
        _fail("ROUTE_A_MAPPING_DOMAINS_INVALID")

    def partition_valid(partition, role):
        if type(partition) is not dict or set(partition) != {"geometry", "tier_refs"}:
            return False
        geometry, references = partition["geometry"], partition["tier_refs"]
        fields = {"schema_version", "role", "frame_start_utc", "scored_start_utc", "end_exclusive_utc",
            "warmup_bars", "scored_bars", "frame_rows", "frame_sha256", "receipt_sha256"}
        return (type(geometry) is dict and set(geometry) == fields
            and geometry["schema_version"] == PARTITION_GEOMETRY_VERSION and geometry["role"] == role
            and all(text(geometry[name]) for name in ("frame_start_utc", "scored_start_utc", "end_exclusive_utc"))
            and type(geometry["warmup_bars"]) is int and geometry["warmup_bars"] >= 0
            and type(geometry["scored_bars"]) is int and geometry["scored_bars"] > 0
            and type(geometry["frame_rows"]) is int
            and geometry["frame_rows"] == geometry["warmup_bars"] + geometry["scored_bars"]
            and hexadecimal(geometry["frame_sha256"]) and hexadecimal(geometry["receipt_sha256"])
            and hashlib.sha256(_canonical_bytes({key: value for key, value in geometry.items()
                if key != "receipt_sha256"})).hexdigest() == geometry["receipt_sha256"]
            and type(references) is dict and set(references) == set(tiers)
            and all(text(value) for value in references.values()))


    def source_fold_valid(fold):
        fields = {"fold_id", "role", "fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc",
            "validation_start_utc", "validation_end_exclusive_utc", "right_fit_start_utc", "right_fit_end_exclusive_utc",
            "fit_rows", "validation_rows", "source_slice_sha256", "final_oos_authority", "fold_definition_sha256"}
        alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        return (type(fold) is dict and set(fold) == fields
            and text(fold["fold_id"]) and 1 <= len(fold["fold_id"]) <= 128 and fold["fold_id"][0] in alphanumeric
            and all(char in alphanumeric + "._:-" for char in fold["fold_id"])
            and fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"
            and all(text(fold[name]) for name in ("fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc",
                "validation_start_utc", "validation_end_exclusive_utc"))
            and fold["right_fit_start_utc"] is None and fold["right_fit_end_exclusive_utc"] is None
            and type(fold["fit_rows"]) is int and fold["fit_rows"] > 0
            and type(fold["validation_rows"]) is int and fold["validation_rows"] > 0
            and fold["final_oos_authority"] is False and hexadecimal(fold["source_slice_sha256"])
            and hexadecimal(fold["fold_definition_sha256"])
            and hashlib.sha256(_canonical_bytes({key: value for key, value in fold.items()
                if key != "fold_definition_sha256"})).hexdigest() == fold["fold_definition_sha256"])

    def scheduled_valid(scheduled, attempt, source_folds):
        fields = {"ordinal", "attempt_id", "fold_id", "role", "validation_start_utc", "validation_end_exclusive_utc",
            "expected_status", "expected_reason_code", "downstream_causal_eligible", "final_oos_authority", "context_sha256"}
        if (type(scheduled) is not list or len(scheduled) != (4 if attempt["status"] == "PLANNED" else 1)
                or not all(type(row) is dict and set(row) == fields and type(row["ordinal"]) is int
                    and row["ordinal"] >= 0 and row["attempt_id"] == attempt["horizon_attempt_id"]
                    and row["final_oos_authority"] is False and hexadecimal(row["context_sha256"]) for row in scheduled)):
            return False
        if attempt["status"] == "PROCESS_REJECTED":
            row = scheduled[0]
            return (row["fold_id"] == "TERMINAL" and row["role"] == "TERMINAL_MECHANICAL_REJECTION"
                and row["validation_start_utc"] is None and row["validation_end_exclusive_utc"] is None
                and row["expected_status"] == "PROCESS_REJECTED"
                and row["expected_reason_code"] == attempt["source_plan_attempt"]["reason_code"]
                and row["downstream_causal_eligible"] is False)
        return all(row["fold_id"] == fold["fold_id"] and row["role"] == fold["role"]
            and row["validation_start_utc"] == fold["validation_start_utc"]
            and row["validation_end_exclusive_utc"] == fold["validation_end_exclusive_utc"]
            and row["expected_status"] == "EVALUATION_REQUIRED" and row["expected_reason_code"] is None
            and row["downstream_causal_eligible"] is True for row, fold in zip(scheduled, source_folds, strict=True))

    def sparse_partition_valid(partition, role, ordinal, fold_ordinal):
        if type(partition) is not dict or set(partition) != {"precommitment_row",
                "partition_geometry_receipt_sha256", "partition_frame_sha256", "tier_refs"}:
            return False
        row = partition["precommitment_row"]
        return (type(row) is dict and set(row) == {"attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256"}
            and type(row["attempt_ordinal"]) is int and row["attempt_ordinal"] == ordinal
            and type(row["fold_ordinal"]) is int and row["fold_ordinal"] == fold_ordinal and row["partition"] == role
            and hexadecimal(partition["partition_geometry_receipt_sha256"])
            and row["partition_geometry_receipt_sha256"] == partition["partition_geometry_receipt_sha256"]
            and hexadecimal(partition["partition_frame_sha256"])
            and type(partition["tier_refs"]) is dict and set(partition["tier_refs"]) == set(tiers)
            and all(text(value) for value in partition["tier_refs"].values()))

    roster_valid, raw_ids, horizon_ids = True, set(), set()
    try:
        fields = {"ordinal", "raw_attempt_id", "horizon_attempt_id", "plan_attempt_sha256", "status",
            "strategy_id", "parameters", "causal_fold_count", "folds", "rejection"}
        if owner_projection:
            fields |= {"source_plan_attempt", "source_folds", "scheduled_evaluations"}
        for ordinal, attempt in enumerate(roster):
            if (type(attempt) is not dict or set(attempt) != fields
                    or type(attempt["ordinal"]) is not int or attempt["ordinal"] != ordinal
                    or not text(attempt["raw_attempt_id"]) or attempt["raw_attempt_id"] in raw_ids
                    or not text(attempt["horizon_attempt_id"]) or attempt["horizon_attempt_id"] in horizon_ids
                    or not hexadecimal(attempt["plan_attempt_sha256"])
                    or attempt["status"] not in ("PLANNED", "PROCESS_REJECTED")
                    or attempt["strategy_id"] not in domain_ids or type(attempt["parameters"]) is not dict
                    or type(attempt["causal_fold_count"]) is not int or attempt["causal_fold_count"] < 0
                    or type(attempt["folds"]) is not list):
                roster_valid = False
                continue
            raw_ids.add(attempt["raw_attempt_id"]); horizon_ids.add(attempt["horizon_attempt_id"])
            rejection = attempt["rejection"]
            if owner_projection:
                source_plan, source_folds = attempt["source_plan_attempt"], attempt["source_folds"]
                source_valid = (type(source_plan) is dict and type(source_folds) is list
                    and len(source_folds) == len(attempt["folds"]) == attempt["causal_fold_count"] == 4
                    and all(source_fold_valid(fold) for fold in source_folds)
                    and source_plan.get("attempt_id") == attempt["horizon_attempt_id"]
                    and source_plan.get("status") == attempt["status"]
                    and source_plan.get("strategy_id") == attempt["strategy_id"]
                    and "reason_code" in source_plan and hexadecimal(source_plan.get("state_context_disposition_sha256"))
                    and _sha256(source_plan) == attempt["plan_attempt_sha256"]
                    and _sha256(attempt["parameters"]) == source_plan.get("parameter_set_sha256")
                    and scheduled_valid(attempt["scheduled_evaluations"], attempt, source_folds))
                if attempt["status"] == "PLANNED":
                    roster_valid = roster_valid and source_valid and source_plan.get("reason_code") is None and rejection is None
                else:
                    expected = {"plan_status": "PROCESS_REJECTED", "reason_code": source_plan.get("reason_code"),
                        "state_context_disposition_sha256": source_plan.get("state_context_disposition_sha256")}
                    roster_valid = (roster_valid and source_valid and expected["reason_code"] == "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
                        and type(rejection) is dict and set(rejection) == set(expected) and rejection == expected)
            elif attempt["status"] == "PLANNED":
                roster_valid = roster_valid and rejection is None and attempt["causal_fold_count"] > 0
            else:
                roster_valid = roster_valid and (type(rejection) is dict and {"kind", "code", "details"} <= set(rejection)
                    and rejection["kind"] == "PROCESS_REJECTED" and text(rejection["code"])
                    and type(rejection["details"]) is list and all(type(value) is str for value in rejection["details"]))
            fold_ids, causal_count = set(), 0
            for fold_index, fold in enumerate(attempt["folds"]):
                if (type(fold) is not dict or set(fold) != {"role", "fold_id", "partitions"}
                        or not text(fold["role"]) or not text(fold["fold_id"]) or fold["fold_id"] in fold_ids
                        or type(fold["partitions"]) is not dict):
                    roster_valid = False
                    continue
                fold_ids.add(fold["fold_id"])
                if owner_projection:
                    source = source_folds[fold_index]
                    roster_valid = roster_valid and fold["fold_id"] == source["fold_id"] and fold["role"] == source["role"]
                if fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER":
                    causal_count += 1
                    if owner_projection and attempt["status"] == "PROCESS_REJECTED":
                        roster_valid = roster_valid and fold["partitions"] == {"status": "UNSCHEDULED"}
                    else:
                        roster_valid = roster_valid and set(fold["partitions"]) == set(roles)
                        for role in roles:
                            roster_valid = roster_valid and (sparse_partition_valid(fold["partitions"].get(role), role, ordinal, causal_count - 1)
                                if owner_projection else partition_valid(fold["partitions"].get(role), role))
                else:
                    roster_valid = roster_valid and not owner_projection and fold["partitions"] == {}
            roster_valid = roster_valid and causal_count == attempt["causal_fold_count"]
        if owner_projection:
            roster_valid = (roster_valid and bool(roster) and any(row["status"] == "PLANNED" for row in roster)
                and _sha256([row["source_plan_attempt"] for row in roster]) == snapshot["declared_unit_binding"]["attempt_roster_sha256"]
                and [row["ordinal"] for attempt in roster for row in attempt["scheduled_evaluations"]]
                    == list(range(sum(len(attempt["scheduled_evaluations"]) for attempt in roster))))
    except (AuthoritativeSearchExecutionError, ValueError, TypeError, KeyError, IndexError, OverflowError, RecursionError):
        roster_valid = False
    if not roster_valid:
        _fail("ROUTE_A_MAPPING_ROSTER_INVALID")
    return domains, roster


def _compile_route_a_request_attempt_mapping(*, unit_metadata, frozen_domains, attempt_roster,
        original_trial_count, specifications, metadata_recipe=_ROUTE_A_UNIT_BATCH_RECIPE):
    """Compile caller metadata only; this neither authenticates nor routes an evaluation."""
    def plain(value):
        if type(value) in (type(None), bool, int, str):
            return True
        if type(value) is float:
            return math.isfinite(value)
        if type(value) is list:
            return all(plain(item) for item in value)
        if type(value) is dict:
            return all(type(key) is str and plain(item) for key, item in value.items())
        return False

    def text(value):
        return type(value) is str and bool(value)

    def hexadecimal(value):
        return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)

    binding_fields = {"generation_context_sha256", "producer_request_sha256", "executor_release_sha256",
        "producer_source_sha256", "horizon_plan_receipt_sha256", "cell_plan_receipt_sha256",
        "source_group_sha256", "regime_fit_sha256", "frame_sha256", "fitted_state_reference_sha256",
        "execution_spec_sha256", "frozen_receipt_sha256", "cost_precommitments_sha256",
        "fold_plan_sha256", "attempt_roster_sha256"}
    unit_fields = {"declared_unit_binding", "header_reference", "source_access_window_utc", "cell",
        "anchor", "search_process_receipt_sha256", "config_sha256", "effective_config_sha256"}
    roles = ("optimizer_train", "tuning_validation", "final_lockbox")
    tiers = ("P75", "P95")
    payload = {"unit_metadata": unit_metadata, "frozen_domains": frozen_domains,
        "attempt_roster": attempt_roster, "original_trial_count": original_trial_count,
        "specifications": specifications, "metadata_recipe": metadata_recipe}
    valid = False
    try:
        if plain(payload):
            encoded = _canonical_bytes(payload)
            if len(encoded) <= winner.MAX_REQUEST_BYTES:
                snapshot = json.loads(encoded.decode("ascii"))
                unit = snapshot["unit_metadata"]
                binding = unit["declared_unit_binding"]
                header = unit["header_reference"]
                window = unit["source_access_window_utc"]
                cell = unit["cell"]
                anchor = unit["anchor"]
                valid = (type(unit) is dict and set(unit) == unit_fields
                    and type(binding) is dict and set(binding) == binding_fields
                    and all(hexadecimal(value) for value in binding.values())
                    and type(header) is dict and set(header) == {"schema_version", "sha256"}
                    and text(header["schema_version"]) and hexadecimal(header["sha256"])
                    and type(window) is dict and set(window) == {"start_utc", "end_exclusive_utc"}
                    and all(text(value) for value in window.values())
                    and type(cell) is dict and set(cell) == {"symbol", "timeframe", "regime"}
                    and text(cell["symbol"]) and text(cell["timeframe"])
                    and (cell["regime"] is None or text(cell["regime"]))
                    and type(anchor) is dict and set(anchor) == {"anchor_id", "selection_cutoff_utc"}
                    and all(text(value) for value in anchor.values())
                    and all(hexadecimal(unit[name]) for name in
                        ("search_process_receipt_sha256", "config_sha256", "effective_config_sha256"))
                    and type(snapshot["attempt_roster"]) is list
                    and type(snapshot["original_trial_count"]) is int
                    and snapshot["original_trial_count"] == len(snapshot["attempt_roster"])
                    and type(snapshot["specifications"]) is list
                    and type(snapshot["metadata_recipe"]) is str
                    and snapshot["metadata_recipe"] == _ROUTE_A_UNIT_BATCH_RECIPE)
    except (AuthoritativeSearchExecutionError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        valid = False
    if not valid:
        _fail("ROUTE_A_MAPPING_INPUT_INVALID")
    domains, roster = _route_a_validate_mapping_snapshot(
        frozen_domains=snapshot["frozen_domains"], attempt_roster=snapshot["attempt_roster"])
    rows = _route_a_batch_specifications(snapshot["specifications"])
    requests, groups, group_indices, requested_lanes = [], [], {}, set()
    for row in rows:
        resolved = _route_a_resolve_request_membership(specification=row,
            frozen_domains=domains, attempt_roster=roster)
        structural = {"unit_metadata": unit, "attempt_ordinal": resolved["attempt_ordinal"],
            "raw_attempt_id": resolved["raw_attempt_id"], "horizon_attempt_id": resolved["horizon_attempt_id"],
            "plan_attempt_sha256": resolved["plan_attempt_sha256"],
            "strategy_id": resolved["strategy_id"], "strategy_family": resolved["strategy_family"],
            "domain_sha256": resolved["domain_sha256"], "parameters": resolved["parameters"],
            "fold_ordinal": resolved["fold_ordinal"], "fold_id": resolved["fold_id"],
            "partition_geometry": resolved["partition_geometry"], "spread_tier": resolved["spread_tier"],
            "tier_reference": resolved["tier_reference"], "metadata_recipe": snapshot["metadata_recipe"]}
        identity = _canonical_bytes(structural)
        group_ordinal = group_indices.get(identity)
        if group_ordinal is None:
            group_ordinal = len(groups)
            group_indices[identity] = group_ordinal
            groups.append({"group_ordinal": group_ordinal, "first_request_ordinal": row["request_ordinal"],
                "structural_identity_json": identity.decode("ascii"), "display_sha256": _sha256(structural)})
        requests.append({**resolved, "request_ordinal": row["request_ordinal"], "group_ordinal": group_ordinal,
            "bookkeeping": row["bookkeeping"], "ordinary_lane": row["partition_role"] + "_" + row["spread_tier"].lower()})
        requested_lanes.add((resolved["attempt_ordinal"], row["fold_ordinal"], row["partition_role"], row["spread_tier"]))

    def lane(attempt, fold, fold_ordinal, role, tier):
        partition = fold["partitions"][role]
        return {"attempt_ordinal": attempt["ordinal"], "raw_attempt_id": attempt["raw_attempt_id"],
            "horizon_attempt_id": attempt["horizon_attempt_id"], "fold_ordinal": fold_ordinal,
            "fold_id": fold["fold_id"], "partition_role": role, "spread_tier": tier,
            "partition_geometry_receipt_sha256": partition["geometry"]["receipt_sha256"],
            "tier_reference": partition["tier_refs"][tier]}

    coverage = []
    for attempt in roster:
        expected, requested, missing, lockbox = [], [], [], []
        if attempt["status"] == "PLANNED":
            folds = [fold for fold in attempt["folds"] if fold["role"] == "CAUSAL_ROLLING_ORIGIN_INNER"]
            for fold_ordinal, fold in enumerate(folds):
                for role in roles:
                    for tier in tiers:
                        entry = lane(attempt, fold, fold_ordinal, role, tier)
                        if role == "final_lockbox":
                            lockbox.append(entry)
                        else:
                            expected.append(entry)
                            target = requested if (attempt["ordinal"], fold_ordinal, role, tier) in requested_lanes else missing
                            target.append(entry)
        coverage.append({"attempt_ordinal": attempt["ordinal"], "raw_attempt_id": attempt["raw_attempt_id"],
            "horizon_attempt_id": attempt["horizon_attempt_id"], "status": attempt["status"],
            "expected_batch_lanes": expected, "requested_unique_lanes": requested,
            "missing_batch_lanes": missing, "ordinary_only_lockbox_lanes": lockbox})
    result = {"schema_version": "fxpm.route-a-request-attempt-mapping.v1", "status": "MAPPED_METADATA_ONLY",
        "provenance_scope": "CALLER_SUPPLIED_METADATA_ONLY", "unit_metadata": unit,
        "attempt_roster": roster, "original_trial_count": snapshot["original_trial_count"],
        "requests": requests, "request_to_group": [row["group_ordinal"] for row in requests],
        "structural_groups": groups, "coverage_by_attempt": coverage,
        "counts": {"original_trials": snapshot["original_trial_count"],
            "planned_trials": sum(attempt["status"] == "PLANNED" for attempt in roster),
            "rejected_trials": sum(attempt["status"] == "PROCESS_REJECTED" for attempt in roster),
            "requests": len(requests), "structural_groups": len(groups)},
        "partition_responsibilities": {"optimizer_train": "ORIGINAL_ORDINARY_BUILDER",
            "tuning_validation": "ORIGINAL_ORDINARY_BUILDER", "final_lockbox": "ORDINARY_ONLY_NOT_ROUTABLE"},
        **_route_a_diagnostic_authorities()}
    encoded_result = _canonical_bytes(result)
    if len(encoded_result) > winner.MAX_REQUEST_BYTES:
        _fail("ROUTE_A_MAPPING_RESULT_TOO_LARGE")
    return json.loads(encoded_result.decode("ascii"))



def _route_a_owner_partition_identity(*, precommitments, attempt_ordinal, fold_ordinal, partition_role,
        _partition_row_lookup=None):
    """Name the owner's existing V2 row; this is not an authenticator."""
    valid = False
    try:
        hexadecimal = lambda value: type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
        matches = ([row for row in precommitments["rows"]
            if (row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition"))
                == (attempt_ordinal, fold_ordinal, partition_role)] if _partition_row_lookup is None else
            _partition_row_lookup.get((attempt_ordinal, fold_ordinal, partition_role), ()))
        row = matches[0] if len(matches) == 1 else None
        identity = row["partition_geometry_receipt_sha256"] if row is not None else None
        record = precommitments["records"].get(identity)
        tier_fields = {"entry_half_spread_values_sha256", "intrabar_half_spread_values_sha256",
            "numerical_execution_cost_attestation_sha256", "entry_observations_sha256", "entry_admission_mask_sha256"}
        valid = (type(precommitments) is dict and set(precommitments) == {"schema_version", "frozen_tier_pair_contract_sha256", "rows", "records", "receipt_sha256"}
            and precommitments["schema_version"] == raw_winner.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA
            and hexadecimal(precommitments["receipt_sha256"]) and hexadecimal(precommitments["frozen_tier_pair_contract_sha256"])
            and type(precommitments["rows"]) is list and type(precommitments["records"]) is dict
            and type(attempt_ordinal) is int and attempt_ordinal >= 0 and type(fold_ordinal) is int and fold_ordinal >= 0
            and partition_role in ("optimizer_train", "tuning_validation", "final_lockbox")
            and type(row) is dict and set(row) == {"attempt_ordinal", "fold_ordinal", "partition", "partition_geometry_receipt_sha256"}
            and type(row["attempt_ordinal"]) is int and type(row["fold_ordinal"]) is int and hexadecimal(identity)
            and type(record) is dict and set(record) == {"partition_frame_sha256", "source_observation_receipt_sha256", "tiers"}
            and hexadecimal(record["partition_frame_sha256"]) and hexadecimal(record["source_observation_receipt_sha256"])
            and type(record["tiers"]) is dict and set(record["tiers"]) == {"P75", "P95"}
            and all(type(value) is dict and set(value) == tier_fields
                and all(hexadecimal(item) for item in value.values()) for value in record["tiers"].values()))
    except (ValueError, TypeError, KeyError, AttributeError, IndexError, OverflowError, RecursionError):
        valid = False
    if not valid:
        _fail("ROUTE_A_OWNER_SNAPSHOT_PRECOMMITMENT_INVALID")
    return json.loads(_canonical_bytes({"precommitment_row": row,
        "partition_geometry_receipt_sha256": identity, "partition_frame_sha256": record["partition_frame_sha256"],
        "tier_refs": {tier: "|".join((precommitments["schema_version"], precommitments["receipt_sha256"], identity, tier))
            for tier in ("P75", "P95")}}).decode("ascii"))


def _route_a_owner_membership_snapshot(*, declared_unit_binding, frozen_domains, plan_body, attempts, raw_ids, precommitments,
        _partition_row_lookup=None):
    """Project the admitted real declared route without deriving partition geometry."""
    rows, valid = [], False
    try:
        candidate = plan_body["candidates"][0]
        valid = (type(plan_body) is dict and type(plan_body["cell"]["regime"]) is str and bool(plan_body["cell"]["regime"])
            and type(plan_body["candidates"]) is list and len(plan_body["candidates"]) == 1
            and type(attempts) in (list, tuple) and bool(attempts) and type(raw_ids) in (list, tuple)
            and len(attempts) == len(raw_ids) == len(plan_body["trials"]) == len(plan_body["attempts"])
            and len(set(raw_ids)) == len(raw_ids) and all(type(value) is str and value for value in raw_ids)
            and any(attempt.plan_attempt["status"] == "PLANNED" for attempt in attempts)
            and _canonical_bytes([attempt.plan_attempt for attempt in attempts]) == _canonical_bytes(plan_body["attempts"])
            and _sha256(plan_body["attempts"]) == declared_unit_binding["attempt_roster_sha256"]
            and _sha256(_winner_fold_plan(attempts)) == declared_unit_binding["fold_plan_sha256"]
            and precommitments["schema_version"] == raw_winner.PARTITION_COST_PRECOMMITMENTS_V2_SCHEMA)
        for ordinal, attempt in enumerate(attempts):
            source_plan, source_folds = copy.deepcopy(attempt.plan_attempt), copy.deepcopy(attempt.candidate["folds"])
            status = source_plan["status"]
            valid = (valid and type(attempt.ordinal) is int and attempt.ordinal == ordinal
                and _canonical_bytes(attempt.candidate) == _canonical_bytes(candidate)
                and type(source_folds) is list and len(source_folds) == 4
                and all(type(fold) is dict and fold.get("role") == "CAUSAL_ROLLING_ORIGIN_INNER" for fold in source_folds)
                and status in ("PLANNED", "PROCESS_REJECTED") and "reason_code" in source_plan
                and ((status == "PLANNED" and source_plan["reason_code"] is None)
                    or (status == "PROCESS_REJECTED" and source_plan["reason_code"] == "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL")))
            if not valid:
                continue
            scheduled = [copy.deepcopy(row) for row in plan_body["expected_evaluations"] if row["attempt_id"] == source_plan["attempt_id"]]
            rejection = None if status == "PLANNED" else {"plan_status": status, "reason_code": source_plan["reason_code"],
                "state_context_disposition_sha256": source_plan["state_context_disposition_sha256"]}
            folds = [{"role": fold["role"], "fold_id": fold["fold_id"], "partitions":
                ({role: _route_a_owner_partition_identity(precommitments=precommitments, attempt_ordinal=ordinal,
                    fold_ordinal=fold_ordinal, partition_role=role, _partition_row_lookup=_partition_row_lookup)
                    for role in ("optimizer_train", "tuning_validation", "final_lockbox")}
                    if status == "PLANNED" else {"status": "UNSCHEDULED"})}
                for fold_ordinal, fold in enumerate(source_folds)]
            rows.append({"ordinal": ordinal, "raw_attempt_id": raw_ids[ordinal], "horizon_attempt_id": source_plan["attempt_id"],
                "plan_attempt_sha256": _sha256(source_plan), "status": status,
                "strategy_id": attempt.artifacts.winner_process["strategy_id"],
                "parameters": copy.deepcopy(attempt.artifacts.trial_definition["parameters"]), "causal_fold_count": len(source_folds),
                "folds": folds, "rejection": rejection, "source_plan_attempt": source_plan, "source_folds": source_folds,
                "scheduled_evaluations": scheduled})
        valid = (valid and _canonical_bytes([row for attempt in rows for row in attempt["scheduled_evaluations"]])
            == _canonical_bytes(plan_body["expected_evaluations"]))
    except (ValueError, TypeError, KeyError, AttributeError, IndexError, OverflowError, RecursionError):
        valid = False
    if not valid:
        _fail("ROUTE_A_OWNER_SNAPSHOT_SOURCE_INVALID")
    domains, roster = _route_a_validate_mapping_snapshot(frozen_domains=frozen_domains, attempt_roster=rows,
        owner_projection=True, declared_unit_binding=declared_unit_binding)
    return json.loads(_canonical_bytes({"schema_version": "fxpm.route-a-owner-membership-snapshot.v2",
        "provenance_scope": "CURRENT_OWNER_METADATA_PROJECTION_ONLY", "declared_unit_binding": declared_unit_binding,
        "frozen_domains": domains, "attempt_roster": roster, "original_trial_count": len(attempts),
        **_route_a_diagnostic_authorities()}).decode("ascii"))


def _route_a_rebind_owner_membership(*, specification, snapshot, attempts, owner_attempts, raw_ids,
        frame, dependency_warmup_bars, precommitments, _partition_row_lookup=None):
    """Rebind plain membership and derive only the selected current fold."""
    valid = False
    try:
        flags = _route_a_diagnostic_authorities()
        valid = (type(snapshot) is dict and set(snapshot) == {"schema_version", "provenance_scope", "declared_unit_binding",
                "frozen_domains", "attempt_roster", "original_trial_count"} | set(flags)
            and snapshot["schema_version"] == "fxpm.route-a-owner-membership-snapshot.v2"
            and snapshot["provenance_scope"] == "CURRENT_OWNER_METADATA_PROJECTION_ONLY"
            and all(snapshot[name] is False for name in flags)
            and type(attempts) in (list, tuple) and type(owner_attempts) in (list, tuple) and type(raw_ids) in (list, tuple)
            and type(snapshot["original_trial_count"]) is int
            and len(attempts) == len(owner_attempts) == len(raw_ids) == snapshot["original_trial_count"] == len(snapshot["attempt_roster"]))
        if valid:
            resolved = _route_a_resolve_request_membership(specification=specification,
                frozen_domains=snapshot["frozen_domains"], attempt_roster=snapshot["attempt_roster"], owner_projection=True)
            index = resolved["attempt_index"]
            valid = type(index) is int and 0 <= index < len(attempts)
            if valid:
                row, selected_attempt, owner_attempt = snapshot["attempt_roster"][index], attempts[index], owner_attempts[index]
                valid = (resolved["attempt_ordinal"] == row["ordinal"] == index
                    and resolved["raw_attempt_id"] == row["raw_attempt_id"] == raw_ids[index]
                    and resolved["horizon_attempt_id"] == row["horizon_attempt_id"]
                    and resolved["plan_attempt_sha256"] == row["plan_attempt_sha256"] and row["status"] == "PLANNED")
                for current in (selected_attempt, owner_attempt):
                    valid = (valid and type(current.ordinal) is int and current.ordinal == index
                        and current.plan_attempt["status"] == "PLANNED" and current.plan_attempt["attempt_id"] == row["horizon_attempt_id"]
                        and _sha256(current.plan_attempt) == row["plan_attempt_sha256"]
                        and _canonical_bytes(current.plan_attempt) == _canonical_bytes(row["source_plan_attempt"])
                        and _canonical_bytes(current.candidate["folds"]) == _canonical_bytes(row["source_folds"])
                        and current.artifacts.winner_process["strategy_id"] == resolved["strategy_id"] == row["strategy_id"])
                domain = next(value for value in snapshot["frozen_domains"] if value["strategy_id"] == row["strategy_id"])
                defaults = copy.deepcopy(domain["parameter_domain"]["defaults"])
                parameters = decode_parameter_mapping(selected_attempt.artifacts.trial_definition["parameters"], defaults)
                original_parameters = decode_parameter_mapping(owner_attempt.artifacts.trial_definition["parameters"], defaults)
                valid = (valid and _canonical_bytes(parameters) == _canonical_bytes(original_parameters) == _canonical_bytes(resolved["parameters"]))
    except (ValueError, TypeError, KeyError, AttributeError, IndexError, OverflowError, RecursionError):
        valid = False
    if not valid:
        _fail("ROUTE_A_OWNER_SNAPSHOT_REBIND_INVALID")
    fold = _causal_folds(selected_attempt)[specification["fold_ordinal"]]
    partitions = _fold_partitions(frame=frame, fold=fold, attempt=selected_attempt, dependency_warmup_bars=dependency_warmup_bars)
    partition = next((value for value in partitions if value.role == specification["partition_role"]), None)
    geometry_valid = partition is not None
    if geometry_valid:
        identity = _route_a_owner_partition_identity(precommitments=precommitments, attempt_ordinal=selected_attempt.ordinal,
            fold_ordinal=specification["fold_ordinal"], partition_role=specification["partition_role"],
            _partition_row_lookup=_partition_row_lookup)
        expected = resolved["partition_identity"]
        geometry = partition.geometry_receipt
        geometry_valid = (fold["fold_id"] == resolved["fold_id"]
            and _canonical_bytes(identity) == _canonical_bytes(expected)
            and geometry["receipt_sha256"] == expected["partition_geometry_receipt_sha256"]
            and geometry["frame_sha256"] == expected["partition_frame_sha256"]
            and hashlib.sha256(_canonical_bytes({key: value for key, value in geometry.items()
                if key != "receipt_sha256"})).hexdigest() == geometry["receipt_sha256"])
    if not geometry_valid:
        _fail("ROUTE_A_OWNER_SNAPSHOT_GEOMETRY_MISMATCH")
    return selected_attempt, fold, partition, parameters


def _route_a_owner_selection_bytes(*, strategy_id, parameters, attempt_ordinal, fold_ordinal,
        fold_id, partition_geometry, declared_unit_binding, spread_tier):
    """Keep the admitted selection and tier-independent pair-group byte contract."""
    group = {"strategy_id": strategy_id, "parameters": parameters, "attempt_ordinal": attempt_ordinal,
        "fold_ordinal": fold_ordinal, "fold_id": fold_id, "partition_geometry": partition_geometry,
        "declared_unit_binding": declared_unit_binding}
    selection = {**group, "spread_tier": spread_tier, "metadata_recipe": _ROUTE_A_UNIT_BATCH_RECIPE}
    return _canonical_bytes(selection), _canonical_bytes(group)


def _run_route_a_owned_unit_continuation(*, config, handle, search_process, cell_plan, binding, refreshed, attempts, request_body, declared_header, capture, normalized_regime, route_a_consumer, fitted, raw_ids, plan_body, timeframe, fold_plan, symbol):
    """Run the existing continuation inside its unchanged owner scope."""
    try:
        import pm_historical_search_producer as producer
        import pm_production_config_binding as config_binding
        import pm_production_search_planner as planner
        import pm_strategies as strategy_module

        lifecycle = _RouteABatchLifecycle()
        owner_process, owner_thread = os.getpid(), threading.get_ident()
        original_handle, original_frozen = handle, handle.frozen
        original_context, original_request = capture.context, capture.request
        original_session, original_sink = capture.source_session, capture.sink
        original_process, original_plan = search_process, cell_plan
        original_config = config
        original_frozen_config = original_session._frozen_config
        unit_binding_bytes = _canonical_bytes(binding)
        private_fitted = _route_a_working_fitted(refreshed)
        private_config = copy.deepcopy(config)
        private_attempts = copy.deepcopy(attempts)
        private_plan = json.loads(_canonical_bytes(plan_body).decode("ascii"))
        private_domains = json.loads(_canonical_bytes(list(planner._domain_by_name(
            search_process.to_dict(), timeframe=timeframe).values())).decode("ascii"))
        private_precommitments = original_frozen.partition_cost_precommitments
        private_partition_rows = _index_partition_precommitment_rows(private_precommitments)
        window = request_body["source_access_window_utc"]
        window_start = _utc(window["start_utc"], "ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_INVALID")
        window_end = _utc(window["end_exclusive_utc"], "ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_INVALID")
        header_relative = _declared_unit_relative(request_body, "header.json")
        frozen_prefix = (request_body["artifact_namespace"] + "/winner-search/"
            + timeframe + "/" + normalized_regime + "/frozen/")
        descriptors = {}
        for entry in declared_header["artifact_roster"]:
            if entry["artifact_id"].startswith(frozen_prefix):
                descriptor = entry["descriptor"]
                descriptors[descriptor["relative_path"]] = copy.deepcopy(descriptor)
        for descriptor in (original_handle.frozen_descriptor_graph,
                original_handle.cross_binding_descriptor, original_session._entry["descriptor"],
                declared_header["horizon_plan"]["descriptor"]):
            descriptors[descriptor["relative_path"]] = copy.deepcopy(descriptor)
        source_paths = (Path(__file__), Path(pm_core.__file__), Path(current_refit.__file__),
            Path(authority_spool.__file__), Path(historical.__file__),
            Path(historical_adapter.__file__), Path(native_prefix.__file__),
            Path(trade_power.__file__), Path(raw_winner.__file__), Path(winner.__file__),
            Path(producer.__file__), Path(planner.__file__), Path(config_binding.__file__),
            Path(pm_regime.__file__), Path(spread_profile.__file__),
            Path(strategy_module.__file__))
        unique_entries, selection_to_unique = [], {}
        private_pair_groups, private_pair_checks, private_codec_strings = {}, {}, {}
        issued_evaluator = None
        owner_snapshot_bytes = owner_snapshot = None
        owner_partition_templates = {}
        owner_current_attempt_ordinal = None
        owner_strategy_surface = None

        def close_route_a_scope():
            nonlocal owner_snapshot_bytes, owner_snapshot, owner_current_attempt_ordinal, owner_strategy_surface
            owner_snapshot_bytes = owner_snapshot = owner_current_attempt_ordinal = None
            owner_strategy_surface = None
            owner_partition_templates.clear()
            if private_partition_rows is not None:
                private_partition_rows.clear()
            lifecycle.close()
            selection_to_unique.clear()
            private_pair_groups.clear()
            private_pair_checks.clear()
            private_codec_strings.clear()
            unique_entries.clear()

        def physical_content():
            config_payload, _ = config_binding._read_config(original_frozen_config.active_config_path)
            if _canonical_bytes(config_payload) != original_frozen_config._payload_json:
                _fail("ROUTE_A_UNIT_BATCH_ORIGINAL_CONFIG_CHANGED")
            _, _, header_bytes = authority_spool._stream_file(original_context.root,
                header_relative, maximum_bytes=original_context.limits.max_json_bytes, collect=True)
            if _canonical_bytes(authority_spool._strict_json_bytes(header_bytes,
                    original_context.limits.max_json_bytes)) != _canonical_bytes(declared_header):
                _fail("ROUTE_A_UNIT_BATCH_ORIGINAL_HEADER_CHANGED")
            observed_descriptors = []
            for relative, descriptor in sorted(descriptors.items()):
                digest, size, _ = authority_spool._stream_file(original_context.root, relative,
                    maximum_bytes=original_context.limits.max_artifact_bytes,
                    expected_size=descriptor["size_bytes"])
                if digest != descriptor["sha256"] or size != descriptor["size_bytes"]:
                    _fail("ROUTE_A_UNIT_BATCH_FROZEN_BYTES_CHANGED")
                observed_descriptors.append((relative, digest, size))
            return header_bytes, observed_descriptors

        def shared_content():
            return hashlib.sha256(pickle.dumps({
                "fitted": _route_a_fitted_shared_content(fitted),
                "refreshed": _route_a_fitted_shared_content(refreshed),
                "private_fitted": _route_a_fitted_shared_content(private_fitted),
                "config": vars(config), "private_config": vars(private_config),
                "request": capture.request.to_dict(), "precommit": capture.request._precommit.to_dict(),
                "process": search_process.to_dict(),
                "plan": (cell_plan.receipt_sha256, cell_plan.cell_receipt, cell_plan.spool_entry,
                    cell_plan.horizon_plan.to_dict(), cell_plan.attempt_artifacts(),
                    cell_plan.state_requirements()),
                "private_attempts": private_attempts, "private_plan": private_plan,
                "private_domains": private_domains, "precommitments": private_precommitments,
                "frozen": asdict(handle.frozen), "handle": (
                    handle.symbol, handle.timeframe, handle.regime, handle.artifact_namespace,
                    handle.frozen_receipt_descriptor, handle.frozen_descriptor_graph,
                    handle.cross_binding_descriptor, handle._cross_binding_bytes,
                    handle._attempt_artifacts, handle._horizon_execution_contracts,
                    handle._source_count_receipts, handle._capture_nonce),
                "roster": capture.sink.artifact_roster(),
                "context": (capture.context.generation_id, capture.context.context_sha256,
                    capture.context.config_sha256, capture.context.native_universe_sha256,
                    str(capture.context.root), asdict(capture.context.limits)),
                "frozen_config": (str(original_frozen_config.active_config_path),
                    original_frozen_config.config_sha256, original_frozen_config.effective_config_sha256,
                    original_frozen_config._payload_json, original_frozen_config._snapshot_json,
                    id(original_frozen_config._seal)),
                "sources": [(str(path), _file_sha256(path)) for path in source_paths],
                "physical": physical_content()}, protocol=5)).hexdigest()

        if (_canonical_bytes(declared_header["frozen_plan_receipt"])
                    != _canonical_bytes(handle.frozen_receipt_descriptor)
                or _canonical_bytes(declared_header["frozen_descriptor_graph"])
                    != _canonical_bytes(handle.frozen_descriptor_graph)
                or _canonical_bytes(declared_header["horizon_winner_cross_binding"])
                    != _canonical_bytes(handle.cross_binding_descriptor)
                or _canonical_bytes(declared_header["source_session_entry"])
                    != _canonical_bytes(dict(original_session._entry))
                or _canonical_bytes(original_frozen.source_contract_seeds)
                    != _canonical_bytes(refreshed.source_contract_seeds)
                or _canonical_bytes(original_frozen.frozen_tier_pair_contract)
                    != _canonical_bytes(refreshed.frozen_tier_pair_contract)
                or _canonical_bytes(_declared_unit_binding(capture, refreshed, cell_plan,
                attempts, fold_plan, handle)) != unit_binding_bytes
                or _frame_sha256(refreshed._frame) != refreshed.frame_sha256
                or config_binding._snapshot(config) != json.loads(
                    original_frozen_config._snapshot_json.decode("ascii"))["pipeline_config"]
                or _sha256(authoritative_search_execution_contract())
                    != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256):
            _fail("ROUTE_A_UNIT_BATCH_ADMISSION_CHANGED")
        admitted_content = shared_content()

        def require_route_a_owner_window():
            if lifecycle.closed:
                _fail("ROUTE_A_UNIT_BATCH_CLOSED")
            if (capture.context is not original_context or capture.request is not original_request
                    or capture.source_session is not original_session or capture.sink is not original_sink
                    or capture.process is not original_process or search_process is not original_process
                    or capture.cell_plan is not original_plan or cell_plan is not original_plan
                    or config is not original_config or capture.pipeline_config is not original_config
                    or capture.fitted_timeframe is not fitted
                    or capture.source_session._frozen_config is not original_frozen_config
                    or handle is not original_handle or handle.frozen is not original_frozen
                    or capture._finished or capture._completion is not None):
                _fail("ROUTE_A_UNIT_BATCH_OWNER_MISMATCH")
            producer._verified_declared_slot_source_session_current(original_context,
                request=original_request, process=original_process,
                source_session=original_session)
            capture._require_active(original_handle)
            _route_a_require_scope_current(closed=lifecycle.closed,
                owner_process=owner_process, owner_thread=owner_thread,
                current_process=os.getpid(), current_thread=threading.get_ident(),
                window_start=window_start, window_end=window_end,
                observed=_utc(_now_utc_seconds(), "ROUTE_A_UNIT_BATCH_SOURCE_WINDOW_INVALID"))

        def require_route_a_current():
            require_route_a_owner_window()
            historical_adapter._verify_declared_slot_capture(capture)
            _route_a_require_unchanged(expected=unit_binding_bytes,
                observed=_canonical_bytes(_declared_unit_binding(capture, refreshed,
                    cell_plan, attempts, fold_plan, original_handle)))
            _route_a_require_unchanged(expected=admitted_content, observed=shared_content())
            require_route_a_owner_window()

        request_guard = require_route_a_owner_window

        owner_snapshot_bytes = _canonical_bytes(_route_a_owner_membership_snapshot(
            declared_unit_binding=json.loads(unit_binding_bytes.decode("ascii")),
            frozen_domains=private_domains, plan_body=private_plan, attempts=private_attempts,
            raw_ids=raw_ids, precommitments=private_precommitments,
            _partition_row_lookup=private_partition_rows))
        owner_snapshot = json.loads(owner_snapshot_bytes.decode("ascii"))

        def select_request(specification):
            rebind = (_route_a_rebind_full_panel_owner_membership
                if type(route_a_consumer) is _FullPanelOwnerPreparation
                else _route_a_rebind_owner_membership)
            selected_attempt, fold, partition, parameters = rebind(
                specification=specification, snapshot=owner_snapshot,
                attempts=private_attempts, owner_attempts=attempts, raw_ids=raw_ids,
                frame=private_fitted._frame,
                dependency_warmup_bars=private_plan["derived_dependency_geometry"]["warmup_bars"],
                precommitments=private_precommitments, _partition_row_lookup=private_partition_rows,
                **({"_partition_templates": owner_partition_templates}
                    if type(route_a_consumer) is _FullPanelOwnerPreparation else {}))
            selection, group = _route_a_owner_selection_bytes(
                strategy_id=selected_attempt.artifacts.winner_process["strategy_id"], parameters=parameters,
                attempt_ordinal=selected_attempt.ordinal, fold_ordinal=specification["fold_ordinal"],
                fold_id=fold["fold_id"], partition_geometry=partition.geometry_receipt,
                declared_unit_binding=json.loads(unit_binding_bytes.decode("ascii")),
                spread_tier=specification["spread_tier"])
            return selected_attempt, fold, partition, parameters, selection, group

        def describe_request(specification, parameters, evaluation_binding):
            if type(route_a_consumer) is _FullPanelOwnerPreparation:
                domain = next(value for value in private_domains
                    if value["strategy_id"] == specification["strategy_id"])
                checked = _route_a_mapping_validate_member(domain["parameter_domain"], parameters)
                key = {"strategy_id": domain["strategy_id"],
                    "strategy_family": domain["strategy_family"], "domain_sha256": domain["domain_sha256"],
                    "parameters": checked, "evaluation_binding": copy.deepcopy(evaluation_binding)}
                return {"request_ordinal": specification["request_ordinal"],
                    "shared_evaluation_key": key, "bookkeeping": copy.deepcopy(specification["bookkeeping"])}
            described = planner.describe_route_a_evaluation_design(frozen_domains=private_domains,
                configurations=[{"strategy_id": specification["strategy_id"], "parameters": parameters,
                    "evaluation_binding": evaluation_binding,
                    "bookkeeping": specification["bookkeeping"]}])["requests"][0]
            described["request_ordinal"] = specification["request_ordinal"]
            return described

        def evaluate_route_a_batch(*, specifications, metadata_recipe=_ROUTE_A_UNIT_BATCH_RECIPE):
            nonlocal owner_strategy_surface
            try:
                lifecycle.enter()
                request_guard()
                if type(metadata_recipe) is not str or metadata_recipe != _ROUTE_A_UNIT_BATCH_RECIPE:
                    _fail("ROUTE_A_UNIT_BATCH_RECIPE_INVALID")
                if type(route_a_consumer) is _FullPanelOwnerPreparation:
                    if not specifications:
                        _fail("ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED")
                    ordinal = specifications[0]["bookkeeping"]["attempt_ordinal"]
                    rows = _route_a_full_panel_specifications(
                        snapshot=owner_snapshot,
                        contract=full_panel_contract, attempt_ordinal=ordinal)
                    if _canonical_bytes(specifications) != _canonical_bytes(rows):
                        _fail("ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED")
                else:
                    rows = _route_a_batch_specifications(specifications)
                requests, mapping, first_requests = [], [], {}
                for specification in rows:
                    request_guard()
                    selected_attempt, fold, partition, parameters, selection, group = select_request(specification)
                    tier = specification["spread_tier"]
                    index = selection_to_unique.get(selection)
                    cache_hit = index is not None
                    if cache_hit:
                        entry = unique_entries[index]
                        described = describe_request(specification, parameters,
                            json.loads(entry["binding_bytes"].decode("ascii")))
                        if _canonical_bytes(described["shared_evaluation_key"]) != entry["key_bytes"]:
                            _fail("ROUTE_A_UNIT_BATCH_REQUEST_INVALID")
                        if type(route_a_consumer) is _FullPanelOwnerPreparation:
                            _verify_partition_precommitment(precommitments=private_precommitments,
                                attempt=selected_attempt, fold_ordinal=specification["fold_ordinal"],
                                partition=partition, tier=tier, result=entry["result"],
                                pair=private_fitted.frozen_tier_pair_contract,
                                _partition_row_lookup=private_partition_rows)
                    else:
                        working_fitted = _route_a_working_fitted(private_fitted)
                        working_config = copy.deepcopy(private_config)
                        working_partitions = _fold_partitions(frame=working_fitted._frame, fold=fold,
                            attempt=selected_attempt,
                            dependency_warmup_bars=private_plan["derived_dependency_geometry"]["warmup_bars"])
                        working_partition = next(value for value in working_partitions
                            if value.role == specification["partition_role"])
                        _route_a_require_unchanged(expected=_canonical_bytes(partition.geometry_receipt),
                            observed=_canonical_bytes(working_partition.geometry_receipt))
                        stable_source = _source_identity(working_fitted.source_contract_seeds[tier])
                        prepared = {}
                        working_content = None
                        replay_candidate = None

                        def require_working_current():
                            nonlocal working_content
                            request_guard()
                            observed = (_route_a_fitted_shared_content(working_fitted),
                                pickle.dumps(vars(working_config), protocol=5))
                            if working_content is None:
                                working_content = observed
                            else:
                                _route_a_require_unchanged(expected=working_content, observed=observed)
                            require_route_a_owner_window()

                        def source_identity(result):
                            contract = raw_winner.build_trade_source_contract_from_backtester_result(result)
                            sample = trade_power.build_trade_sample_evidence([], source_contract=contract,
                                minimum_observations=raw_winner.PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS)
                            if sample.source_contract is None:
                                _fail("ROUTE_A_UNIT_BATCH_SOURCE_CONTRACT_UNAVAILABLE")
                            return sample.source_contract

                        def prepare_probe():
                            probe_backtester = Backtester(working_config)
                            if getattr(probe_backtester, "_entry_admission_preparation", None) is not None:
                                _fail("ROUTE_A_UNIT_BATCH_ENTRY_ADMISSION_POOL_REFUSED")
                            return probe_backtester.build_evidence_source_seed_result(
                                working_partition.frame, symbol, spec=working_fitted.instrument_spec,
                                timeframe=timeframe, spread_tier=working_fitted._spread_tiers[tier],
                                entry_spread_observations=working_fitted._entry_spread_observations.loc[
                                    working_partition.frame.index])

                        def verify_probe(probe):
                            _verify_partition_precommitment(precommitments=private_precommitments,
                                attempt=selected_attempt, fold_ordinal=specification["fold_ordinal"],
                                partition=working_partition, tier=tier, result=probe,
                                pair=working_fitted.frozen_tier_pair_contract,
                                _partition_row_lookup=private_partition_rows)
                            identity = source_identity(probe)
                            if identity.source_contract_sha256 != stable_source:
                                _fail("ROUTE_A_UNIT_BATCH_SOURCE_IDENTITY_MISMATCH")
                            evaluation_binding = {"symbol": symbol, "timeframe": timeframe,
                                "regime": normalized_regime, "anchor_id": private_plan["anchor"]["anchor_id"],
                                "selection_cutoff_utc": private_plan["anchor"]["selection_cutoff_utc"],
                                "search_process_receipt_sha256": original_process.receipt_sha256,
                                "config_sha256": original_context.config_sha256,
                                "effective_config_sha256": original_frozen_config.effective_config_sha256,
                                "declared_unit_binding": json.loads(unit_binding_bytes.decode("ascii")),
                                "fold_ordinal": specification["fold_ordinal"], "fold_id": fold["fold_id"],
                                "partition_geometry": working_partition.geometry_receipt, "spread_tier": tier,
                                "source_contract_sha256": stable_source,
                                "partition_source_contract_sha256": identity.partition_source_contract_sha256,
                                "spread_model_sha256": hashlib.sha256(_ROUTE_A_UNIT_BATCH_POLICY_DOMAIN
                                    + pm_core.build_backtest_spread_mode(probe["spread_model"]).encode("ascii")).hexdigest()}
                            prepared["identity"] = identity
                            prepared["described"] = describe_request(specification, parameters, evaluation_binding)

                        def run_strategy(strategy, probe):
                            nonlocal replay_candidate
                            strategy_id = selected_attempt.artifacts.winner_process["strategy_id"]
                            recipe = getattr(selected_attempt.artifacts, "refit_recipe_sha256", None)
                            key = (_route_a_strategy_surface_key(strategy, strategy_id=strategy_id,
                                parameters=parameters, recipe=recipe)
                                if type(route_a_consumer) is _FullPanelOwnerPreparation else None)
                            state = _route_a_plain_strategy_state(strategy) if key is not None else None
                            cached = owner_strategy_surface
                            if (state is not None and cached is not None
                                    and cached["key"] == key and cached["before"] == state):
                                strategy.__dict__.clear()
                                strategy.__dict__.update(copy.deepcopy(cached["after"]))
                                surface = cached["surface"]
                            else:
                                surface = _build_full_prefix_strategy_surface(frame=working_fitted._frame,
                                    strategy=strategy, fitted=working_fitted, symbol=symbol, timeframe=timeframe)
                                after = _route_a_plain_strategy_state(strategy) if state is not None else None
                                if after is not None:
                                    replay_candidate = {"key": key, "before": state,
                                        "after": after, "surface": surface}
                            decision = {}
                            matched_facts = {} if type(route_a_consumer) is _FullPanelOwnerPreparation and tier == "P75" else None
                            backtester = Backtester(working_config)
                            if getattr(backtester, "_entry_admission_preparation", None) is not None:
                                _fail("ROUTE_A_UNIT_BATCH_ENTRY_ADMISSION_POOL_REFUSED")
                            result, _features, signals, _ = _run_tier_partition(partition=working_partition,
                                full_prefix_surface=surface, backtester=backtester, strategy=strategy,
                                symbol=symbol, timeframe=timeframe, regime=normalized_regime, config=working_config,
                                spec=working_fitted.instrument_spec, expected_source_contract_sha256=stable_source,
                                spread_tier=working_fitted._spread_tiers[tier],
                                entry_spread_observations=working_fitted._entry_spread_observations,
                                decision_result_out=decision,
                                **({"_matched_base_facts_out": matched_facts,
                                    "_matched_stress_tier": working_fitted._spread_tiers["P95"]}
                                   if matched_facts is not None else {}))
                            return {"result": result, "signals": signals, "decision": decision,
                                "matched_base_facts": matched_facts}

                        def verify_realized(realized, probe):
                            _verify_partition_precommitment(precommitments=private_precommitments,
                                attempt=selected_attempt, fold_ordinal=specification["fold_ordinal"],
                                partition=working_partition, tier=tier, result=realized["result"],
                                pair=working_fitted.frozen_tier_pair_contract,
                                _partition_row_lookup=private_partition_rows)
                            actual = source_identity(realized["result"])
                            expected = prepared["identity"]
                            if (actual.source_contract_sha256 != expected.source_contract_sha256
                                    or actual.partition_source_contract_sha256
                                        != expected.partition_source_contract_sha256):
                                _fail("ROUTE_A_UNIT_BATCH_REALIZED_PARTITION_SOURCE_IDENTITY_MISMATCH")

                        realized = _route_a_evaluate_prepared_request(prepare_probe=prepare_probe,
                            verify_probe=verify_probe, require_current=require_working_current,
                            make_strategy=lambda: _runtime_strategy(selected_attempt, timeframe=timeframe)[0],
                            run_strategy=run_strategy, verify_realized=verify_realized)
                        if replay_candidate is not None:
                            owner_strategy_surface = replay_candidate
                        described = prepared["described"]
                        index = len(unique_entries)
                        diagnostic = {"schema_version": "fxpm.route-a-unit-request-diagnostic.v1",
                            "status": "EVALUATED_DIAGNOSTIC_ONLY", "metadata_recipe": metadata_recipe,
                            "request_ordinal": specification["request_ordinal"],
                            "shared_evaluation_key": described["shared_evaluation_key"],
                            "matched_attempt_ordinal": selected_attempt.ordinal,
                            "matched_horizon_attempt_id": selected_attempt.plan_attempt["attempt_id"],
                            **({"decision_lane": copy.deepcopy(realized["decision"]),
                                "signal_vector": realized["signals"].copy(deep=True)}
                               if type(route_a_consumer) is not _FullPanelOwnerPreparation else {}),
                            "signal_comparison": "INCOMPLETE_MISSING_PARTNER",
                            "unique_evaluation_ordinal": index, "cache_hit": False,
                            **_route_a_diagnostic_authorities()}
                        entry = {"diagnostic": diagnostic, "result": copy.deepcopy(realized["result"]),
                            "decision": copy.deepcopy(realized["decision"]),
                            "signals": realized["signals"].copy(deep=True),
                            "attempt_ordinal": selected_attempt.ordinal, "pair_check": None,
                            "matched_base_facts": copy.deepcopy(realized["matched_base_facts"]),
                            "group": group, "tier": tier,
                            "binding_bytes": _canonical_bytes(described["shared_evaluation_key"]["evaluation_binding"]),
                            "key_bytes": _canonical_bytes(described["shared_evaluation_key"])}
                        if type(route_a_consumer) is _FullPanelOwnerPreparation:
                            _route_a_intern_matched_codec_strings(entry["matched_base_facts"], private_codec_strings)
                        unique_entries.append(entry)
                        selection_to_unique[selection] = index
                        if type(route_a_consumer) is _FullPanelOwnerPreparation:
                            private_pair_groups.setdefault(group, {})[tier] = entry
                            private_pair_checks.pop(group, None)
                    request_guard()
                    key_bytes = entry["key_bytes"]
                    if type(route_a_consumer) is _FullPanelOwnerPreparation:
                        mapping.append(index)
                        continue
                    diagnostic = copy.deepcopy(entry["diagnostic"])
                    diagnostic.update(request_ordinal=specification["request_ordinal"],
                        same_identity_as_request_ordinal=first_requests.get(key_bytes),
                        bookkeeping=copy.deepcopy(described["bookkeeping"]), cache_hit=cache_hit)
                    first_requests.setdefault(key_bytes, specification["request_ordinal"])
                    requests.append(diagnostic)
                    mapping.append(index)
                full_panel = type(route_a_consumer) is _FullPanelOwnerPreparation
                groups = private_pair_groups if full_panel else {}
                if not full_panel:
                    for entry in unique_entries:
                        groups.setdefault(entry["group"], {})[entry["tier"]] = entry
                pair_checks = []
                for group, entries in groups.items():
                    signature = tuple((tier, id(entry), id(entry["result"]), id(entry["signals"]),
                        entry["key_bytes"], entry["binding_bytes"]) for tier, entry in sorted(entries.items()))
                    cached = private_pair_checks.get(group) if full_panel else None
                    if cached is not None and cached[0] == signature:
                        check = json.loads(cached[1])
                    else:
                        check = _route_a_pair_check(
                            signals_by_tier={tier: entry["signals"] for tier, entry in entries.items()},
                            results_by_tier={tier: entry["result"] for tier, entry in entries.items()},
                            spec=copy.deepcopy(private_fitted.instrument_spec),
                            use_spread=bool(private_config.use_spread),
                            frozen_pair=copy.deepcopy(private_fitted.frozen_tier_pair_contract))
                        if full_panel:
                            private_pair_checks[group] = (signature, _canonical_bytes(check))
                    pair_checks.append({"evaluation_group_sha256": hashlib.sha256(group).hexdigest(), **check})
                    for entry in entries.values():
                        entry["diagnostic"]["signal_comparison"] = check["status"]
                        entry["pair_check"] = copy.deepcopy(check)
                for diagnostic in requests:
                    diagnostic["signal_comparison"] = unique_entries[
                        diagnostic["unique_evaluation_ordinal"]]["diagnostic"]["signal_comparison"]
                request_guard()
                if type(route_a_consumer) is _FullPanelOwnerPreparation:
                    return tuple(mapping), tuple(pair_checks)
                return {"schema_version": "fxpm.route-a-unit-batch-diagnostic.v1",
                    "status": "EVALUATED_DIAGNOSTIC_ONLY", "metadata_recipe": metadata_recipe,
                    "requests": copy.deepcopy(requests), "request_to_unique": list(mapping),
                    "unique_evaluations": [copy.deepcopy(entry["diagnostic"]) for entry in unique_entries],
                    "pair_checks": copy.deepcopy(pair_checks), **_route_a_diagnostic_authorities()}
            except Exception as exc:
                close_route_a_scope()
                if type(route_a_consumer) is _FullPanelOwnerPreparation:
                    raise
                return _route_a_contain_consumer_failure(exc)
            except BaseException:
                close_route_a_scope()
                raise
            finally:
                lifecycle.leave()

        class UnitEvaluator:
            __slots__ = ()

            def __call__(self, *, specifications, metadata_recipe=_ROUTE_A_UNIT_BATCH_RECIPE):
                if type(route_a_consumer) is _FullPanelOwnerPreparation:
                    _fail("ROUTE_A_FULL_PANEL_PRIVATE_EVALUATOR_NOT_PUBLIC")
                if self is not issued_evaluator:
                    close_route_a_scope()
                    _fail("ROUTE_A_UNIT_BATCH_OWNER_MISMATCH")
                return evaluate_route_a_batch(specifications=specifications, metadata_recipe=metadata_recipe)

            def __copy__(self):
                close_route_a_scope()
                _route_a_refuse_transfer(lifecycle)

            def __deepcopy__(self, memo):
                return self.__copy__()

            def __reduce_ex__(self, protocol):
                return self.__copy__()

        full_panel_contract = None
        if type(route_a_consumer) is _FullPanelOwnerPreparation:
            full_panel_contract = planner._route_a_full_panel_preparation_contract(
                owner_snapshot=owner_snapshot,
                partition_cost_precommitments=private_precommitments)

        def prepare_full_panel_attempt(ordinal):
            nonlocal owner_current_attempt_ordinal, owner_strategy_surface
            request_guard()
            rows = _route_a_full_panel_specifications(snapshot=owner_snapshot,
                contract=full_panel_contract, attempt_ordinal=ordinal)
            if owner_current_attempt_ordinal != ordinal:
                # Preserve current-attempt replay; earlier attempts have distinct selection keys.
                unique_entries.clear()
                selection_to_unique.clear()
                private_pair_groups.clear()
                private_pair_checks.clear()
                private_codec_strings.clear()
                owner_strategy_surface = None
                owner_current_attempt_ordinal = ordinal
            mapping, pair_checks = evaluate_route_a_batch(specifications=rows)
            if len(mapping) != 24 or len(rows) != 24:
                _fail("ROUTE_A_FULL_PANEL_EVALUATION_ROSTER_CHANGED", ordinal)
            entries = {}
            groups = set()
            for row, index in zip(rows, mapping, strict=True):
                if type(index) is not int or not 0 <= index < len(unique_entries):
                    _fail("ROUTE_A_FULL_PANEL_EVALUATION_MAPPING_INVALID", ordinal)
                entry = unique_entries[index]
                selected, fold, partition, parameters, selection, group = select_request(row)
                if (selection_to_unique.get(selection) != index or entry["group"] != group
                        or entry["tier"] != row["spread_tier"]
                        or entry["attempt_ordinal"] != ordinal
                        or entry["pair_check"]["status"] != "VERIFIED"):
                    _fail("ROUTE_A_FULL_PANEL_EVALUATION_MAPPING_INVALID", ordinal)
                identity = row["fold_ordinal"], row["partition_role"], row["spread_tier"]
                if identity in entries:
                    _fail("ROUTE_A_FULL_PANEL_EVALUATION_MAPPING_INVALID", ordinal)
                entries[identity] = entry
                groups.add(hashlib.sha256(group).hexdigest())
            checks = {check["evaluation_group_sha256"]: check for check in pair_checks}
            if (len(groups) != 12 or any(group not in checks or checks[group]["status"] != "VERIFIED"
                    or checks[group]["missing_tiers"] for group in groups)):
                _fail("ROUTE_A_FULL_PANEL_PAIR_CHECK_INCOMPLETE", ordinal)
            request_guard()
            evidence, facts = _route_a_assemble_full_panel_attempt(
                attempt=private_attempts[ordinal], raw_attempt_id=raw_ids[ordinal],
                plan_body=private_plan, fitted=private_fitted, entries=entries,
                partition_cost_precommitments=private_precommitments,
                source_observation_references=route_a_consumer.source_observation_references,
                _partition_row_lookup=private_partition_rows)
            request_guard()
            return evidence, facts

        issued_evaluator = UnitEvaluator()
        try:
            require_route_a_owner_window()
            historical_adapter._verify_declared_slot_capture_current(capture)
            require_route_a_owner_window()
            if type(route_a_consumer) is _FullPanelOwnerPreparation:
                route_a_consumer_outcome = route_a_consumer.prepare_owned(
                    require_current=require_route_a_owner_window,
                    prepare_attempt=prepare_full_panel_attempt, private_attempts=private_attempts,
                    private_plan=private_plan, private_precommitments=private_precommitments,
                    owner_snapshot=copy.deepcopy(owner_snapshot))
            else:
                route_a_consumer_outcome = route_a_consumer(evaluate_batch=issued_evaluator)
            require_route_a_current()
        finally:
            close_route_a_scope()
    except Exception as exc:
        if type(route_a_consumer) is _FullPanelOwnerPreparation:
            raise
        route_a_consumer_outcome = _route_a_contain_consumer_failure(exc)
    return route_a_consumer_outcome


def _session_execution_context(metadata, *, bar_count, trades, index=None, owner_specs=None):
    """Check the one captured session axis; source authentication remains with its caller."""
    fields = {"schema_version", "clock", "specs", "owner_spec_codes", "exit_half_spreads", "exit_open_prices"}
    if (type(metadata) is not dict or set(metadata) != fields
            or metadata["schema_version"] != "fxpm.backtest-session-execution.v1"
            or metadata["clock"] != "AS_RECORDED"
            or type(metadata["specs"]) is not list or not metadata["specs"]
            or type(metadata["owner_spec_codes"]) is not list or not metadata["owner_spec_codes"]):
        raise ValueError("invalid captured session execution")
    specs = []
    for axis in metadata["specs"]:
        if (type(axis) is not dict or set(axis) != {"anchor_minutes", "length_minutes", "in_session", "session_keys"}
                or type(axis["anchor_minutes"]) is not int or not 0 <= axis["anchor_minutes"] < 1440
                or type(axis["length_minutes"]) is not int or not 1 <= axis["length_minutes"] <= 1440
                or type(axis["in_session"]) is not list or len(axis["in_session"]) != bar_count
                or any(type(value) is not bool for value in axis["in_session"])
                or type(axis["session_keys"]) is not list or len(axis["session_keys"]) != bar_count
                or any(type(value) is not int for value in axis["session_keys"])):
            raise ValueError("invalid captured session axis")
        spec = (axis["anchor_minutes"], axis["length_minutes"])
        if spec in specs:
            raise ValueError("duplicate captured session specification")
        specs.append(spec)
        if index is not None:
            inside, keys = pm_core._recorded_session_arrays(index, spec)
            if inside.tolist() != axis["in_session"] or keys.tolist() != axis["session_keys"]:
                raise ValueError("captured session clock changed")
    codes = metadata["owner_spec_codes"]
    if (any(type(code) is not int or not -1 <= code < len(specs) for code in codes)
            or set(code for code in codes if code >= 0) != set(range(len(specs)))):
        raise ValueError("captured session owner mapping changed")
    if owner_specs is not None:
        expected_specs, expected_codes = [], []
        for spec in owner_specs:
            if spec is None:
                expected_codes.append(-1)
            else:
                spec = tuple(spec)
                if spec not in expected_specs:
                    expected_specs.append(spec)
                expected_codes.append(expected_specs.index(spec))
        if specs != expected_specs or codes != expected_codes:
            raise ValueError("captured session owner specification changed")
    for name in ("exit_half_spreads", "exit_open_prices"):
        if type(metadata[name]) is not list or len(metadata[name]) != len(trades):
            raise ValueError("captured session exit axis changed")
    for ordinal, trade in enumerate(trades):
        half, opened = metadata["exit_half_spreads"][ordinal], metadata["exit_open_prices"][ordinal]
        if str(trade.get("exit_reason", "")).lower() == "closed_session":
            if (type(half) not in (int, float) or not math.isfinite(half) or half < 0
                    or type(opened) not in (int, float) or not math.isfinite(opened)):
                raise ValueError("captured session OPEN inputs missing")
        elif half is not None or opened is not None:
            raise ValueError("session OPEN inputs attached to another exit")
    return metadata


def _session_entry_allowed(axis, signal_bar, entry_bar):
    return (0 <= signal_bar < entry_bar < len(axis["in_session"])
        and axis["in_session"][signal_bar] and axis["in_session"][entry_bar]
        and axis["session_keys"][signal_bar] == axis["session_keys"][entry_bar])


def _session_boundary_at(axis, entry_bar, bar):
    return (bar > entry_bar and (not axis["in_session"][bar]
        or axis["session_keys"][bar] != axis["session_keys"][entry_bar]))


def _route_a_matched_base_trade_facts(*, result, features, backtester, spec,
                                     base_tier, stress_tier, timeframe, partition_role, exact_trade_capture=None, strategy=None):
    """Retain matched kernel facts without inventing a charged-cost scalar."""
    if (not isinstance(base_tier, spread_profile.FrozenSpreadTier)
            or not isinstance(stress_tier, spread_profile.FrozenSpreadTier)
            or base_tier.tier != "P75" or stress_tier.tier != "P95"
            or type(result.get("trades")) is not list):
        _fail("ROUTE_A_MATCHED_BASE_FACTS_UNAVAILABLE")
    arrays, attestations, spread_metadata = {}, {}, {}
    for name, tier in (("P75", base_tier), ("P95", stress_tier)):
        entry, intrabar, metadata = backtester._build_half_spread_arrays(
            features, spec, spread_tier=tier, timeframe=timeframe)
        arrays[name] = (entry, intrabar)
        spread_metadata[name] = metadata
        attestations[name] = backtester._numerical_execution_cost_attestation(
            entry_half_spread=entry, intrabar_half_spread=intrabar,
            spread_meta=metadata, spec=spec)
    if attestations["P75"] != result.get("numerical_execution_cost_attestation"):
        _fail("ROUTE_A_MATCHED_BASE_COST_ATTESTATION_DRIFT")
    non_improvement = _route_a_spread_non_improvement({tier: {
        "entry_half_spread": pair[0], "intrabar_half_spread": pair[1]} for tier, pair in arrays.items()})
    if non_improvement["status"] == "REFUSED_P95_BELOW_P75":
        _fail("ROUTE_A_FULL_PANEL_P95_SPREAD_NON_IMPROVEMENT_VIOLATION")
    identities = build_backtest_trade_identity_pnl_evidence(result["trades"])
    session = None
    session_axis = None
    session_getter = getattr(strategy, "session_spec", None)
    session_spec = session_getter(spec.symbol) if callable(session_getter) else None
    if session_spec is not None and "session_execution" not in result:
        _fail("ROUTE_A_MATCHED_SESSION_CAPTURE_MISSING")
    if "session_execution" in result:
        session = _session_execution_context(result["session_execution"], bar_count=len(features),
            trades=result["trades"], index=features.index,
            owner_specs=[session_spec] if strategy is not None else None)
        if len(session["owner_spec_codes"]) != 1 or session["owner_spec_codes"][0] < 0:
            _fail("ROUTE_A_MATCHED_SESSION_OWNER_INVALID")
        session_axis = session["specs"][session["owner_spec_codes"][0]]
    if exact_trade_capture is not None and (type(exact_trade_capture) is not dict
            or exact_trade_capture.get("schema_version") != "fxpm.backtest-exact-executed-trades.v1"
            or exact_trade_capture.get("swap_accrual_contract") != result.get("swap_accrual_contract")
            or type(exact_trade_capture.get("trades")) is not list):
        _fail("ROUTE_A_EXACT_EXECUTED_TRADE_CAPTURE_INVALID")
    exact_rows = [] if exact_trade_capture is None else exact_trade_capture["trades"]
    rows = []
    for ordinal, (trade, identity) in enumerate(zip(result["trades"], identities, strict=True)):
        if identity["direction"] not in {"LONG", "SHORT"}:
            _fail("ROUTE_A_MATCHED_BASE_TRADE_INVALID", ordinal)
        direction = 1 if identity["direction"] == "LONG" else -1
        opened, closed = identity["entry_bar"], identity["exit_bar"]
        if (type(opened) is not int or type(closed) is not int
                or not 0 <= opened <= closed < len(features)):
            _fail("ROUTE_A_MATCHED_BASE_TRADE_AXIS_INVALID", ordinal)
        if session_axis is not None and identity["signal_bar"] != opened - 1:
            _fail("ROUTE_A_MATCHED_SESSION_SIGNAL_BINDING_CHANGED")
        try:
            entry_price, stop_price = float(trade["entry_price"]), float(trade["stop_loss"])
            volume = float(trade["position_size"])
            recorded_risk = float(spec.calculate_loss_at_stop(
                entry_price, stop_price, volume, direction))
            if (not all(math.isfinite(value) for value in (entry_price, stop_price, volume, recorded_risk))
                    or volume <= 0 or recorded_risk <= 0):
                raise ValueError("invalid executed risk geometry")
            spread_sides = {}
            for tier, (entry, intrabar) in arrays.items():
                values = {"entry_half_spread_at_entry": float(entry[opened]),
                    "intrabar_half_spread_at_entry": float(intrabar[opened]),
                    "entry_half_spread_at_exit": float(entry[closed]),
                    "intrabar_half_spread_at_exit": float(intrabar[closed])}
                if any(not math.isfinite(value) or value < 0 for value in values.values()):
                    raise ValueError("invalid spread side")
                spread_sides[tier] = values
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise AuthoritativeSearchExecutionError(
                "ROUTE_A_MATCHED_BASE_RECORDED_RISK_UNAVAILABLE", (str(ordinal),)) from exc
        if identity["exit_reason"] == "closed_session":
            if (session is None or session["exit_half_spreads"][ordinal] != float(arrays["P75"][1][closed])
                    or session["exit_open_prices"][ordinal] != float(features["Open"].iloc[closed])):
                _fail("ROUTE_A_MATCHED_SESSION_OPEN_ARRAY_CHANGED")
        fill = _route_a_base_fill_witness(trade=trade, identity=identity, features=features,
            base_entry_half=arrays["P75"][0], base_intrabar_half=arrays["P75"][1], session_axis=session_axis)
        rows.append({"base_trade_identity": identity,
            "entry_price": entry_price, "recorded_stop_price": stop_price,
            "executed_volume": volume, "risk_at_recorded_stop_dollars": recorded_risk,
            "actual_initial_risk_dollars": None,
            "initial_stop_status": "UNPROVEN_BACKEND_CONTROL_BINDING",
            "executed_price_swap_capture": copy.deepcopy(exact_rows[ordinal]) if ordinal < len(exact_rows) else None,
            "base_return_components": None,
            "base_fill_witness": fill, "base_position_event": None,
            "base_actual_initial_risk_net_r": None, "matched_p95_spread_cost_r": None,
            "spread_sides": spread_sides})
    events = result.get("position_events")
    position_join = type(events) is list and len(events) == len(rows)
    seen_ids = set()
    if position_join:
        try:
            for row, event in zip(rows, events, strict=True):
                identity = row["base_trade_identity"]
                if (type(event) is not dict or type(event.get("position_id")) is not str
                        or event.get("trade_record_schema_version") != pm_core.BACKTEST_POSITION_EVENT_SCHEMA_VERSION
                        or raw_winner._BACKTEST_POSITION_ID.fullmatch(event["position_id"]) is None
                        or event["position_id"] in seen_ids
                        or pd.Timestamp(event.get("entry_time")) != pd.Timestamp(identity["entry_time"])
                        or pd.Timestamp(event.get("exit_time")) != pd.Timestamp(identity["exit_time"])
                        or event.get("entry_event_order") != identity["entry_bar"] * 4 + 1
                        or event.get("exit_event_order") != identity["exit_bar"] * 4 + 3
                        or event.get("pnl_dollars") != identity["pnl_dollars"]
                        or event.get("risk_amount") != identity["risk_amount"]):
                    position_join = False
                    break
                row["base_position_event"] = copy.deepcopy(event)
                seen_ids.add(event["position_id"])
        except (KeyError, TypeError, ValueError, OverflowError):
            position_join = False
    reasons = ([] if position_join else ["BASE_POSITION_EVENT_BIJECTION_INVALID"])
    if exact_trade_capture is not None and len(exact_rows) != len(identities):
        reasons.append("EXACT_EXECUTED_PRICE_SWAP_CAPTURE_COUNT_MISMATCH")
    reasons.extend(row["base_fill_witness"]["reason_code"] for row in rows
        if row["base_fill_witness"]["status"] != "VERIFIED")
    return json.loads(_canonical_bytes({
        "schema_version": "fxpm.route-a-matched-base-kernel-facts.v3",
        "partition_role": partition_role,
        "exact_execution_capture_schema": "fxpm.backtest-exact-executed-trades.v1",
        "exact_swap_accrual_contract": copy.deepcopy(result.get("swap_accrual_contract")),
        "exact_swap_model_enabled": None if exact_trade_capture is None else exact_trade_capture.get("swap_model_enabled"),
        "exact_capture_backend": None if exact_trade_capture is None else exact_trade_capture.get("backend"),
        "exact_capture_count_matches": exact_trade_capture is not None and len(exact_rows) == len(identities),
        "base_return_policy_reference": "E-00005908",
        "swap_conversion_scope": ("SWAP_MODE_1_PRICE_RATIO_FX_FREE" if getattr(spec, "swap_mode", None) == 1
            else "CURRENT_TERMS_SWAP_TICK_VALUE_SNAPSHOT_NO_DATED_FX_PROOF"),
        "trade_power_partition_axis": result["trade_power_partition_axis"],
        "numerical_execution_cost_attestations": attestations,
        "half_spread_arrays": {tier: {"entry_half_spread": _route_a_encode_spread_array(pair[0], "entry_half_spread"),
            "intrabar_half_spread": _route_a_encode_spread_array(pair[1], "intrabar_half_spread")} for tier, pair in arrays.items()},
        "use_spread": bool(backtester.config.use_spread),
        "spread_model_metadata": spread_metadata,
        "instrument_spec": vars(spec), "trades": rows,
        **({"session_execution": session} if session is not None else {}),
        "base_position_events": copy.deepcopy(events),
        "position_event_bijection_status": "VERIFIED" if position_join else "MISMATCH",
        "base_fill_witness_status": ("NO_COMPLETED_BASE_TRADES" if not rows else "VERIFIED" if not reasons else "MISMATCH"),
        "base_fill_witness_sha256": _sha256([row["base_fill_witness"] for row in rows]),
        "tier_non_improvement": non_improvement,
        "floor_reason_codes": sorted(set(reasons)),
        "matched_cost_policy_reference": "E-00005870",
        "stress_cost_scalar": None, "stress_cost_status": "PER_TRADE_PRICE_RATIO_PENDING_OWNER_BINDING",
        "completed_position_ids": None, "completed_position_open_utc": None,
        "completed_position_close_utc": None, "completed_position_open_event_order": None,
        "completed_position_close_event_order": None, "base_actual_initial_risk_net_r": None,
        "matched_p95_spread_cost_r": None,
        "dated_fx_vector": None, "dated_fx_status": "NOT_REQUIRED_PRICE_RATIO_FLOOR",
        "cash_conversion_limitation": "INSTRUMENT_SPEC_ACQUISITION_SNAPSHOT_NO_DATED_FX_SERIES",
        "floor_status": "UNAVAILABLE_OWNER_INITIAL_RISK_BINDING_REQUIRED"}).decode("ascii"))


class _FullPanelLaneCapture:
    """Owner-local collector; never returned with a prepared evidence stage."""

    def __init__(self, require_current):
        self._require_current = require_current
        self.seen = []
        self.base_facts = {}

    def require_current(self):
        if self._require_current is None:
            _fail("ROUTE_A_FULL_PANEL_CAPTURE_CLOSED")
        self._require_current()

    def record_lane(self, fold_ordinal, role, tier, decision, matched_base_facts):
        self.require_current()
        key = fold_ordinal, role
        self.seen.append((fold_ordinal, role, tier))
        if tier == "P75":
            if (key in self.base_facts or type(matched_base_facts) is not dict
                    or matched_base_facts.get("partition_role") != role):
                _fail("ROUTE_A_FULL_PANEL_BASE_FACTS_INVALID")
            self.base_facts[key] = copy.deepcopy(matched_base_facts)
        elif (tier != "P95" or key not in self.base_facts
                or self.base_facts[key]["numerical_execution_cost_attestations"]["P95"]
                    != decision["numerical_execution_cost_attestation"]):
            _fail("ROUTE_A_FULL_PANEL_STRESS_ATTESTATION_DRIFT")

    def close(self):
        self._require_current = None


@dataclass(frozen=True, slots=True)
class _PreparedRouteAFullPanelStage:
    rows: tuple[_StagedAttemptEvidence, ...]
    declared_unit_binding_sha256: str
    preparation_contract_sha256: str
    unit_identities_bytes: bytes

    def _envelope(self, ordinal):
        if type(ordinal) is not int or not 0 <= ordinal < len(self.rows):
            _fail("ROUTE_A_FULL_PANEL_UNIT_INVALID")
        row = self.rows[ordinal]
        envelope = _reopen_staged_attempt(row)
        if (row.ordinal != ordinal or set(envelope) != {"schema_version",
                "declared_unit_binding_sha256", "preparation_contract_sha256",
                "attempt_evidence", "matched_base_facts"}
                or envelope["schema_version"] != "fxpm.route-a-prepared-full-panel-unit.v1"
                or envelope["declared_unit_binding_sha256"] != self.declared_unit_binding_sha256
                or envelope["preparation_contract_sha256"] != self.preparation_contract_sha256):
            _fail("ROUTE_A_FULL_PANEL_STAGE_BINDING_CHANGED", ordinal)
        return envelope

    def unit_body(self, unit):
        if type(unit) is not dict or type(unit.get("ordinal")) is not int:
            _fail("ROUTE_A_FULL_PANEL_UNIT_INVALID")
        envelope = self._envelope(unit["ordinal"])
        identities = json.loads(self.unit_identities_bytes.decode("ascii"))
        if (type(identities) is not list or len(identities) != len(self.rows)
                or unit != identities[unit["ordinal"]]):
            _fail("ROUTE_A_FULL_PANEL_UNIT_IDENTITY_CHANGED", unit["ordinal"])
        body = envelope["attempt_evidence"]
        expected_keys = {"attempt_id", "outcome"} if unit["status"] == "PROCESS_REJECTED" else {"attempt_id", "folds"}
        if (type(body) is not dict or set(body) != expected_keys
                or body["attempt_id"] != unit["raw_attempt_id"]):
            _fail("ROUTE_A_FULL_PANEL_UNIT_IDENTITY_CHANGED", unit["ordinal"])
        return body

    def matched_base_facts(self, ordinal):
        return self._envelope(ordinal)["matched_base_facts"]


class _FullPanelOwnerPreparation:
    def __init__(self, *, capture, handle, binding, stage_custody, reservation,
                 staged_rows, source_observation_references):
        self.capture, self.handle, self.binding = capture, handle, binding
        self.custody, self.reservation, self.rows = stage_custody, reservation, staged_rows
        self.source_observation_references = copy.deepcopy(source_observation_references)
        self.completed = None

    def prepare_owned(self, *, require_current, prepare_attempt,
                      private_attempts, private_plan, private_precommitments, owner_snapshot):
        import pm_production_search_planner as planner

        require_current()
        if (_file_sha256(Path(planner.__file__)) != authoritative_search_execution_contract()[
                "dependency_source_sha256"]["pm_production_search_planner"]):
            _fail("ROUTE_A_FULL_PANEL_PLANNER_CODE_IDENTITY_MISMATCH")
        capture, handle, custody = self.capture, self.handle, self.custody
        body = capture.request.to_dict()
        if (type(custody) is not _StageCapacityCustody or type(self.rows) is not list or self.rows
                or _canonical_bytes(owner_snapshot["declared_unit_binding"]) != _canonical_bytes(self.binding)
                or self.binding["executor_release_sha256"] != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
                or custody.root.resolve(strict=True) != Path(capture.context.root).resolve(strict=True)
                or custody.binding.get("generation_context_sha256") != capture.context.context_sha256
                or custody.binding.get("producer_request_sha256") != capture.request.receipt_sha256
                or custody.binding.get("frozen_receipt_descriptor") != dict(handle.frozen_receipt_descriptor)):
            _fail("ROUTE_A_FULL_PANEL_STAGE_CUSTODY_INVALID")
        stage = custody.root / custody.binding["original_stage_directory"]
        info = stage.lstat()
        if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or _is_reparse(info)
                or (info.st_dev, info.st_ino) != custody.stage_identity
                or stage.resolve(strict=True).parent != custody.root.resolve(strict=True)):
            _fail("ROUTE_A_FULL_PANEL_STAGE_CUSTODY_INVALID")
        for name in ("raw-completion.json", "evidence-completed.json"):
            if _read_declared_unit_json(capture.context, _declared_unit_relative(body, name)) is not None:
                _fail("ROUTE_A_FULL_PANEL_EXISTING_UNIT_REENTRY_REFUSED")
        for ordinal in range(len(private_attempts)):
            if _read_declared_unit_json(capture.context,
                    _declared_unit_relative(body, f"{ordinal:06d}.json")) is not None:
                _fail("ROUTE_A_FULL_PANEL_EXISTING_UNIT_REENTRY_REFUSED", ordinal)
        contract = planner._route_a_full_panel_preparation_contract(
            owner_snapshot=owner_snapshot, partition_cost_precommitments=private_precommitments)
        contract_sha = _sha256(contract)
        binding_sha = _sha256(self.binding)
        for ordinal, attempt in enumerate(private_attempts):
            require_current()
            raw_id = owner_snapshot["attempt_roster"][ordinal]["raw_attempt_id"]
            if attempt.plan_attempt["status"] == "PROCESS_REJECTED":
                outcome = _process_rejection_outcome(attempt=attempt,
                    symbol=private_plan["cell"]["symbol"], timeframe=private_plan["cell"]["timeframe"],
                    regime=private_plan["cell"]["regime"])
                evidence = {"attempt_id": raw_id, "outcome": outcome.to_dict()}
                facts = []
            else:
                evidence, facts = prepare_attempt(ordinal)
                if (evidence.get("attempt_id") != raw_id or len(evidence.get("folds", [])) != 4
                        or len(facts) != 12):
                    _fail("ROUTE_A_FULL_PANEL_EVALUATION_ROSTER_CHANGED", ordinal)
            require_current()
            envelope = {"schema_version": "fxpm.route-a-prepared-full-panel-unit.v1",
                "declared_unit_binding_sha256": binding_sha,
                "preparation_contract_sha256": contract_sha,
                "attempt_evidence": evidence, "matched_base_facts": facts}
            row = _write_staged_attempt(stage=stage, ordinal=ordinal, value=envelope,
                reservation=self.reservation)
            self.rows.append(row)
        require_current()
        identities = [_declared_unit_identity(attempt,
            owner_snapshot["attempt_roster"][ordinal]["raw_attempt_id"])
            for ordinal, attempt in enumerate(private_attempts)]
        self.completed = _PreparedRouteAFullPanelStage(tuple(self.rows), binding_sha, contract_sha,
            _canonical_bytes(identities))
        return self.completed


def _prepare_route_a_full_panel_owner_evidence(*, config, handle, search_process,
        cell_plan, binding, refreshed, attempts, request_body, declared_header,
        capture, normalized_regime, fitted, raw_ids, plan_body, timeframe, fold_plan,
        symbol, stage_custody, reservation, staged_rows, source_observation_references):
    """Prepare a fresh full panel; caller retains existing stage custody/cleanup.

    Unit publication may start only after this function returns and the owner
    evaluator has closed. Legacy/default callers do not invoke this entry.
    """
    if (type(capture) is not historical_adapter.DeclaredSlotSearchCapture
            or not historical_adapter._verify_declared_slot_capture(capture)
            or fitted is not capture.verified_fitted_timeframe
            or config is not capture.verified_pipeline_config):
        _fail("ROUTE_A_FULL_PANEL_ORIGINAL_OWNER_REQUIRED")
    if (_canonical_bytes(source_observation_references)
            != _canonical_bytes(declared_header.get("source_observation_references"))):
        _fail("ROUTE_A_FULL_PANEL_SOURCE_OBSERVATION_BINDING_CHANGED")
    consumer = _FullPanelOwnerPreparation(capture=capture, handle=handle, binding=binding,
        stage_custody=stage_custody, reservation=reservation, staged_rows=staged_rows,
        source_observation_references=source_observation_references)
    result = _run_route_a_owned_unit_continuation(config=config, handle=handle,
        search_process=search_process, cell_plan=cell_plan, binding=binding,
        refreshed=refreshed, attempts=attempts, request_body=request_body,
        declared_header=declared_header, capture=capture, normalized_regime=normalized_regime,
        route_a_consumer=consumer, fitted=fitted, raw_ids=raw_ids, plan_body=plan_body,
        timeframe=timeframe, fold_plan=fold_plan, symbol=symbol)
    if type(result) is not _PreparedRouteAFullPanelStage or result is not consumer.completed:
        _fail("ROUTE_A_FULL_PANEL_PREPARATION_INCOMPLETE")
    return result

__all__.extend(["execute_route_a_full_panel_historical_observed_cell",
    "execute_independent_r_historical_observed_cell", "execute_independent_r_current_observed_cell"])



def _route_a_full_panel_specifications(*, snapshot, contract, attempt_ordinal):
    """Derive the exact frozen production footprint, never caller-selected rows."""
    if (type(attempt_ordinal) is not int or not 0 <= attempt_ordinal < len(snapshot["attempt_roster"])
            or _canonical_bytes(contract["declared_unit_binding"])
                != _canonical_bytes(snapshot["declared_unit_binding"])):
        _fail("ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED")
    attempt = snapshot["attempt_roster"][attempt_ordinal]
    if attempt["ordinal"] != attempt_ordinal or attempt["status"] != "PLANNED":
        _fail("ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED")
    domain = next(value for value in snapshot["frozen_domains"] if value["strategy_id"] == attempt["strategy_id"])
    parameters = decode_parameter_mapping(attempt["parameters"], copy.deepcopy(domain["parameter_domain"]["defaults"]))
    footprint = [row for row in contract["requests"] if row["attempt_ordinal"] == attempt_ordinal]
    expected = [(fold, role, tier) for fold in range(4)
        for role in ("optimizer_train", "tuning_validation", "final_lockbox") for tier in ("P75", "P95")]
    if ([(row["fold_ordinal"], row["partition_role"], row["spread_tier"]) for row in footprint] != expected
            or any(row["raw_attempt_id"] != attempt["raw_attempt_id"]
                or row["horizon_attempt_id"] != attempt["horizon_attempt_id"] for row in footprint)):
        _fail("ROUTE_A_FULL_PANEL_REQUEST_ROSTER_CHANGED")
    return tuple({"request_ordinal": ordinal, "strategy_id": attempt["strategy_id"],
        "parameters": copy.deepcopy(parameters), "fold_ordinal": row["fold_ordinal"],
        "partition_role": row["partition_role"], "spread_tier": row["spread_tier"],
        "bookkeeping": {"request_ordinal": ordinal, "attempt_ordinal": attempt_ordinal,
            "horizon_attempt_id": attempt["horizon_attempt_id"]}}
        for ordinal, row in enumerate(footprint))


def _route_a_detached_owner_partition(partition):
    return _PartitionSlice(role=partition.role, frame=partition.frame.copy(deep=True),
        warmup_bars=partition.warmup_bars, scored_start_utc=partition.scored_start_utc,
        end_exclusive_utc=partition.end_exclusive_utc,
        geometry_receipt=copy.deepcopy(partition.geometry_receipt))


def _route_a_owner_fold_partitions(*, frame, fold, attempt, dependency_warmup_bars,
        fold_ordinal, templates, partition_role=None):
    """Retain at most the current private attempt's four fold templates."""
    recipe = getattr(attempt.artifacts, "refit_recipe", None)
    requirements = recipe.get("data_requirements") if isinstance(recipe, Mapping) else None
    recipe_warmup = requirements.get("warmup_bars") if isinstance(requirements, Mapping) else None
    bounds = tuple(fold.get(name) for name in (
        "fit_start_utc", "fit_end_exclusive_utc", "validation_warmup_start_utc",
        "validation_start_utc", "validation_end_exclusive_utc"))
    if (type(templates) is not dict or type(fold_ordinal) is not int
            or not 0 <= fold_ordinal < 4 or not all(type(value) is str for value in bounds)
            or type(recipe_warmup) is not int or type(dependency_warmup_bars) is not int):
        partitions = _fold_partitions(frame=frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=dependency_warmup_bars)
        return (partitions if partition_role is None else next(
            partition for partition in partitions if partition.role == partition_role))
    if templates.get("attempt") is not attempt or templates.get("frame") is not frame:
        templates.clear()
        templates.update(attempt=attempt, frame=frame, folds={})
    key = (*bounds, recipe_warmup, dependency_warmup_bars)
    cached = templates["folds"].get(fold_ordinal)
    if cached is None or cached[0] != key:
        partitions = _fold_partitions(frame=frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=dependency_warmup_bars)
        templates["folds"][fold_ordinal] = (key, partitions)
    else:
        partitions = cached[1]
    if partition_role is not None:
        return _route_a_detached_owner_partition(next(
            partition for partition in partitions if partition.role == partition_role))
    return tuple(_route_a_detached_owner_partition(partition) for partition in partitions)


def _route_a_rebind_full_panel_owner_membership(*, specification, snapshot, attempts,
        owner_attempts, raw_ids, frame, dependency_warmup_bars, precommitments,
        _partition_templates=None, _partition_row_lookup=None):
    """Rebind a closed production row to both original owners and V2 geometry."""
    try:
        flags = _route_a_diagnostic_authorities()
        fields = {"request_ordinal", "strategy_id", "parameters", "fold_ordinal",
            "partition_role", "spread_tier", "bookkeeping"}
        if (type(snapshot) is not dict
                or snapshot["schema_version"] != "fxpm.route-a-owner-membership-snapshot.v2"
                or snapshot["provenance_scope"] != "CURRENT_OWNER_METADATA_PROJECTION_ONLY"
                or any(snapshot[name] is not False for name in flags)
                or type(specification) is not dict or set(specification) != fields
                or type(specification["bookkeeping"]) is not dict
                or set(specification["bookkeeping"]) != {"request_ordinal", "attempt_ordinal", "horizon_attempt_id"}
                or type(specification["request_ordinal"]) is not int
                or not 0 <= specification["request_ordinal"] < 24
                or specification["bookkeeping"]["request_ordinal"] != specification["request_ordinal"]
                or type(specification["fold_ordinal"]) is not int or not 0 <= specification["fold_ordinal"] < 4
                or specification["partition_role"] not in ("optimizer_train", "tuning_validation", "final_lockbox")
                or specification["spread_tier"] not in ("P75", "P95")):
            raise ValueError("invalid closed production row")
        index = specification["bookkeeping"]["attempt_ordinal"]
        if (type(index) is not int or not 0 <= index < len(attempts)
                or len(attempts) != len(owner_attempts) or len(attempts) != len(raw_ids)
                or len(attempts) != snapshot["original_trial_count"]
                or len(attempts) != len(snapshot["attempt_roster"])):
            raise ValueError("crossed production roster")
        row, selected, original = snapshot["attempt_roster"][index], attempts[index], owner_attempts[index]
        if (row["ordinal"] != index or row["status"] != "PLANNED"
                or row["raw_attempt_id"] != raw_ids[index]
                or row["horizon_attempt_id"] != specification["bookkeeping"]["horizon_attempt_id"]
                or row["strategy_id"] != specification["strategy_id"]):
            raise ValueError("crossed original attempt")
        domain = next(value for value in snapshot["frozen_domains"] if value["strategy_id"] == row["strategy_id"])
        defaults = copy.deepcopy(domain["parameter_domain"]["defaults"])
        parameters = _route_a_mapping_validate_member(domain["parameter_domain"], specification["parameters"])
        for current in (selected, original):
            if (type(current.ordinal) is not int or current.ordinal != index
                    or current.plan_attempt["status"] != "PLANNED"
                    or current.plan_attempt["attempt_id"] != row["horizon_attempt_id"]
                    or _sha256(current.plan_attempt) != row["plan_attempt_sha256"]
                    or _canonical_bytes(current.plan_attempt) != _canonical_bytes(row["source_plan_attempt"])
                    or _canonical_bytes(current.candidate["folds"]) != _canonical_bytes(row["source_folds"])
                    or current.artifacts.winner_process["strategy_id"] != row["strategy_id"]
                    or _canonical_bytes(decode_parameter_mapping(current.artifacts.trial_definition["parameters"], defaults))
                        != _canonical_bytes(parameters)):
                raise ValueError("original attempt changed")
        if _canonical_bytes(decode_parameter_mapping(row["parameters"], defaults)) != _canonical_bytes(parameters):
            raise ValueError("crossed frozen parameters")
        fold_ordinal = specification["fold_ordinal"]
        fold = _causal_folds(selected)[fold_ordinal]
        partition = _route_a_owner_fold_partitions(frame=frame, fold=fold, attempt=selected,
            dependency_warmup_bars=dependency_warmup_bars, fold_ordinal=fold_ordinal,
            templates=_partition_templates, partition_role=specification["partition_role"])
        identity = _route_a_owner_partition_identity(precommitments=precommitments,
            attempt_ordinal=index, fold_ordinal=fold_ordinal, partition_role=partition.role,
            _partition_row_lookup=_partition_row_lookup)
        expected = row["folds"][fold_ordinal]
        geometry = partition.geometry_receipt
        if (fold["fold_id"] != expected["fold_id"]
                or _canonical_bytes(identity) != _canonical_bytes(expected["partitions"][partition.role])
                or geometry["receipt_sha256"] != identity["partition_geometry_receipt_sha256"]
                or geometry["frame_sha256"] != identity["partition_frame_sha256"]
                or _sha256({key: value for key, value in geometry.items() if key != "receipt_sha256"})
                    != geometry["receipt_sha256"]):
            raise ValueError("original geometry changed")
    except (AuthoritativeSearchExecutionError, ValueError, TypeError, KeyError, AttributeError,
            IndexError, StopIteration, OverflowError, RecursionError) as exc:
        raise AuthoritativeSearchExecutionError("ROUTE_A_FULL_PANEL_OWNER_REBIND_INVALID") from exc
    return selected, fold, partition, parameters


def _route_a_assemble_full_panel_attempt(*, attempt, raw_attempt_id, plan_body,
        fitted, entries, partition_cost_precommitments, source_observation_references,
        _partition_row_lookup=None):
    """Assemble strict Raw from private verified entries without another execution."""
    expected = {(fold, role, tier) for fold in range(4)
        for role in ("optimizer_train", "tuning_validation", "final_lockbox") for tier in ("P75", "P95")}
    if set(entries) != expected:
        _fail("ROUTE_A_FULL_PANEL_EVALUATION_ROSTER_CHANGED", attempt.ordinal)
    folds, facts = [], []
    for fold_ordinal, fold in enumerate(_causal_folds(attempt)):
        partitions = _fold_partitions(frame=fitted._frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=plan_body["derived_dependency_geometry"]["warmup_bars"])
        lanes, partition_signals = {}, {}
        for partition in partitions:
            base = entries[fold_ordinal, partition.role, "P75"]
            stress = entries[fold_ordinal, partition.role, "P95"]
            matched = base["matched_base_facts"]
            identities = build_backtest_trade_identity_pnl_evidence(base["result"]["trades"])
            expected_binding = {"executor_release_sha256": AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
                "backtest_source_contract_sha256": _source_identity(fitted.source_contract_seeds["P75"]),
                "instrument_spec_sha256": _sha256(vars(fitted.instrument_spec)),
                "caller": "pm_authoritative_search_execution._run_tier_partition",
                "backtester": "pm_core.Backtester.run", "governance_policy": None,
                "entry_schedule": None, "terminal_policy": "LIQUIDATE",
                "stop_provenance": "ORIGINAL_ENTRY_STOP_PRESERVED_BOTH_FIXED_CONTROL_BACKENDS"}
            if (type(matched) is not dict or matched.get("partition_role") != partition.role
                    or matched.get("initial_risk_source_binding") != expected_binding
                    or _canonical_bytes(matched.get("instrument_spec")) != _canonical_bytes(vars(fitted.instrument_spec))
                    or matched.get("trade_power_partition_axis") != base["result"]["trade_power_partition_axis"]
                    or matched.get("numerical_execution_cost_attestations") != {
                        "P75": base["result"]["numerical_execution_cost_attestation"],
                        "P95": stress["result"]["numerical_execution_cost_attestation"]}
                    or len(matched.get("trades", [])) != len(identities)):
                _fail("ROUTE_A_FULL_PANEL_MATCHED_BASE_WITNESS_CHANGED", attempt.ordinal)
            for witness, trade, identity in zip(matched["trades"], base["result"]["trades"], identities, strict=True):
                direction = 1 if identity["direction"] == "LONG" else -1
                actual_risk = float(fitted.instrument_spec.calculate_loss_at_stop(
                    float(trade["entry_price"]), float(trade["stop_loss"]), float(trade["position_size"]), direction))
                if (witness["base_trade_identity"] != identity
                        or witness["entry_price"] != trade["entry_price"]
                        or witness["recorded_stop_price"] != trade["stop_loss"]
                        or witness["executed_volume"] != trade["position_size"]
                        or witness["initial_stop_status"] != "ORIGINAL_ENTRY_STOP_FIXED_CONTROL"
                        or not math.isfinite(actual_risk) or actual_risk <= 0
                        or witness["actual_initial_risk_dollars"] != actual_risk
                        or witness["risk_at_recorded_stop_dollars"] != actual_risk):
                    _fail("ROUTE_A_FULL_PANEL_MATCHED_BASE_WITNESS_CHANGED", attempt.ordinal)
            for tier in ("P75", "P95"):
                entry = entries[fold_ordinal, partition.role, tier]
                lanes[f"{partition.role}_{tier.lower()}"] = copy.deepcopy(entry["decision"])
                if (entry["pair_check"]["status"] != "VERIFIED"
                        or not entry["signals"].equals(base["signals"])):
                    _fail("ROUTE_A_FULL_PANEL_PAIR_CHECK_INCOMPLETE", attempt.ordinal)
            partition_signals[partition.role] = base["signals"].copy(deep=True)
            facts.append({"attempt_ordinal": attempt.ordinal, "raw_attempt_id": raw_attempt_id,
                "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
                "fold_ordinal": fold_ordinal, "partition_role": partition.role,
                "partition_geometry_receipt_sha256": partition.geometry_receipt["receipt_sha256"],
                "partition_frame_sha256": partition.geometry_receipt["frame_sha256"],
                "partition_axis_sha256": _sha256(matched["trade_power_partition_axis"]),
                "cost_precommitment_receipt_sha256": partition_cost_precommitments["receipt_sha256"],
                "facts": copy.deepcopy(matched)})
        receipt = _independent_tier_execution_receipt(lanes, fitted.frozen_tier_pair_contract)
        provenance = _fold_provenance(plan_body=plan_body, plan_receipt_sha256=_sha256(plan_body),
            attempt=attempt, fold=fold, fold_ordinal=fold_ordinal, partitions=partitions,
            lanes=lanes, fitted=fitted, independent_tier_execution_receipt=receipt,
            partition_signals=partition_signals,
            source_observation_receipts=({partition.role: _source_observation_receipt(fitted, partition.frame.index)
                for partition in partitions} if source_observation_references is None else None),
            source_observation_references=({partition.role: _partition_source_observation_reference(
                references=source_observation_references, precommitments=partition_cost_precommitments,
                pair=fitted.frozen_tier_pair_contract, attempt=attempt,
                fold_ordinal=fold_ordinal, partition=partition,
                _partition_row_lookup=_partition_row_lookup) for partition in partitions}
                if source_observation_references is not None else None))
        folds.append({"fold_ordinal": fold_ordinal, "provenance": provenance, "lanes": lanes})
    return {"attempt_id": raw_attempt_id, "folds": folds}, facts



def _route_a_base_fill_witness(*, trade, identity, features, base_entry_half, base_intrabar_half, session_axis=None):
    """Capture actual inputs and reproduce the fixed-control fill before projection."""
    try:
        opened, closed = identity["entry_bar"], identity["exit_bar"]
        if (not 0 <= opened <= closed < len(features)
                or len(base_entry_half) != len(features) or len(base_intrabar_half) != len(features)):
            raise ValueError("invalid actual bar axis")
        inputs = {"direction": identity["direction"], "entry_bar": opened, "exit_bar": closed,
            "partition_bar_count": len(features), "entry_time_utc": identity["entry_time"],
            "exit_time_utc": identity["exit_time"], "entry_mid_open": float(features["Open"].iloc[opened]),
            "p75_entry_half_price": float(base_entry_half[opened]),
            "observed_entry_price": float(trade["entry_price"]), "observed_exit_price": float(trade["exit_price"]),
            "original_stop_price": float(trade["stop_loss"]), "original_target_price": float(trade["take_profit"]),
            "exit_reason": trade["exit_reason"], "held_bar_inputs": [
                {"bar": bar, "open_utc": pd.Timestamp(features.index[bar]).isoformat(),
                 "ohlc": {name: float(features[name].iloc[bar]) for name in ("Open", "High", "Low", "Close")},
                 "p75_intrabar_half_price": float(base_intrabar_half[bar])}
                for bar in range(opened, closed + 1)]}
        if session_axis is not None:
            if opened < 1:
                raise ValueError("session entry has no prior signal bar")
            inputs["session_signal_open_utc"] = pd.Timestamp(features.index[opened - 1]).isoformat()
        _canonical_bytes(inputs)
        return {**inputs, **_route_a_replay_base_fill_inputs(inputs, session_axis=session_axis),
            "held_bar_inputs_sha256": _sha256(inputs["held_bar_inputs"])}
    except (KeyError, TypeError, ValueError, IndexError, OverflowError):
        return {"status": "MISMATCH", "reason_code": "BASE_FILL_WITNESS_INPUT_INVALID"}


def _route_a_replay_base_fill_inputs(inputs, *, session_axis=None):
    """Pure exact branch reference shared by capture and serialized verification."""
    try:
        opened, closed, count = (inputs[key] for key in ("entry_bar", "exit_bar", "partition_bar_count"))
        if (any(type(value) is not int for value in (opened, closed, count))
                or not 0 <= opened <= closed < count or inputs["direction"] not in {"LONG", "SHORT"}):
            raise ValueError("invalid literal axis")
        direction = 1 if inputs["direction"] == "LONG" else -1
        entry, exit_price, stop, target, entry_mid, entry_half = (float(inputs[key]) for key in (
            "observed_entry_price", "observed_exit_price", "original_stop_price", "original_target_price", "entry_mid_open", "p75_entry_half_price"))
        path = inputs["held_bar_inputs"]
        if (not all(math.isfinite(value) for value in (entry, exit_price, stop, target, entry_mid, entry_half))
                or entry_half < 0 or direction * (entry - stop) <= 0 or direction * (target - entry) <= 0
                or type(path) is not list or len(path) != closed - opened + 1
                or [row["bar"] for row in path] != list(range(opened, closed + 1))
                or pd.Timestamp(path[0]["open_utc"]) != pd.Timestamp(inputs["entry_time_utc"])
                or pd.Timestamp(path[-1]["open_utc"]) != pd.Timestamp(inputs["exit_time_utc"])
                or float(path[0]["ohlc"]["Open"]) != entry_mid):
            raise ValueError("invalid literal input")
        if session_axis is not None:
            if not _session_entry_allowed(session_axis, opened - 1, opened):
                raise ValueError("session signal and fill do not share an active key")
            recorded = [pd.Timestamp(inputs["session_signal_open_utc"])] + [pd.Timestamp(row["open_utc"]) for row in path]
            stamps = pd.DatetimeIndex([stamp.tz_localize(None) if stamp.tzinfo is not None else stamp
                for stamp in recorded])
            inside, keys = pm_core._recorded_session_arrays(stamps,
                (session_axis["anchor_minutes"], session_axis["length_minutes"]))
            bars = [opened - 1] + list(range(opened, closed + 1))
            if (inside.tolist() != [session_axis["in_session"][bar] for bar in bars]
                    or keys.tolist() != [session_axis["session_keys"][bar] for bar in bars]):
                raise ValueError("session witness clock changed")
        expected_entry = entry_mid + entry_half if direction == 1 else entry_mid - entry_half
        if expected_entry != entry:
            return {"status": "MISMATCH", "reason_code": "BASE_ENTRY_FILL_MISMATCH"}
        trigger, scanned = None, 0
        for row in path:
            mid, half = row["ohlc"], float(row["p75_intrabar_half_price"])
            session_open = session_axis is not None and _session_boundary_at(session_axis, opened, row["bar"])
            if (type(mid) is not dict or set(mid) != {"Open", "High", "Low", "Close"}
                    or not math.isfinite(float(mid["Open"])) or not math.isfinite(half) or half < 0
                    or not session_open and not all(math.isfinite(float(value)) for value in mid.values())):
                raise ValueError("invalid trigger input")
            scanned += 1
            if session_open:
                quote_open = float(mid["Open"]) - direction * half
                trigger = (row["bar"], "SESSION_OPEN", quote_open, "closed_session", mid, half)
            elif direction == 1:
                quote_open, adverse, favorable = mid["Open"] - half, mid["Low"] - half, mid["High"] - half
                if quote_open <= stop:
                    trigger = (row["bar"], "GAP_SL", quote_open, pm_core.TradeStatus.CLOSED_SL.value, mid, half)
                elif adverse <= stop:
                    trigger = (row["bar"], "TOUCH_SL", stop, pm_core.TradeStatus.CLOSED_SL.value, mid, half)
                elif favorable >= target:
                    trigger = (row["bar"], "TOUCH_TP", target, pm_core.TradeStatus.CLOSED_TP.value, mid, half)
            else:
                quote_open, adverse, favorable = mid["Open"] + half, mid["High"] + half, mid["Low"] + half
                if quote_open >= stop:
                    trigger = (row["bar"], "GAP_SL", quote_open, pm_core.TradeStatus.CLOSED_SL.value, mid, half)
                elif adverse >= stop:
                    trigger = (row["bar"], "TOUCH_SL", stop, pm_core.TradeStatus.CLOSED_SL.value, mid, half)
                elif favorable <= target:
                    trigger = (row["bar"], "TOUCH_TP", target, pm_core.TradeStatus.CLOSED_TP.value, mid, half)
            if trigger is not None:
                break
        if trigger is None:
            if closed != count - 1 or inputs["exit_reason"] != pm_core.TradeStatus.CLOSED_EOD.value:
                return {"status": "MISMATCH", "reason_code": "BASE_EXIT_TRIGGER_UNAVAILABLE"}
            expected_exit = mid["Close"] - half if direction == 1 else mid["Close"] + half
            trigger_bar, branch, reason = closed, "EOD", pm_core.TradeStatus.CLOSED_EOD.value
        else:
            trigger_bar, branch, expected_exit, reason, mid, half = trigger
        if trigger_bar != closed or reason != inputs["exit_reason"] or expected_exit != exit_price:
            return {"status": "MISMATCH", "reason_code": "BASE_FIRST_TRIGGER_OR_EXIT_FILL_MISMATCH"}
        return {"status": "VERIFIED", "reason_code": None, "branch": branch, "same_bar": opened == closed,
            "held_bars_scanned": scanned, "expected_entry_price": expected_entry,
            "exit_bar_ohlc": mid.copy(), "p75_exit_intrabar_half_price": half,
            "expected_exit_price": expected_exit}
    except (KeyError, TypeError, ValueError, IndexError, OverflowError):
        return {"status": "MISMATCH", "reason_code": "BASE_FILL_WITNESS_INPUT_INVALID"}



def _verify_route_a_matched_base_facts(facts):
    """Recompute serialized witness data; the caller must authenticate its origin."""
    empty = {"completed_position_ids": None, "completed_position_open_utc": None,
        "completed_position_close_utc": None, "completed_position_open_event_order": None,
        "completed_position_close_event_order": None, "base_actual_initial_risk_net_r": None,
        "matched_p95_spread_cost_r": None}
    unavailable = {"floor_status": "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH",
        "floor_reason_codes": [], **empty, "authenticates_origin": False}
    reasons = []
    try:
        if (type(facts) is not dict or facts.get("schema_version") != "fxpm.route-a-matched-base-kernel-facts.v3"
                or type(facts.get("use_spread")) is not bool
                or facts.get("matched_cost_policy_reference") != "E-00005870"
                or facts.get("base_return_policy_reference") != "E-00005908"
                or facts.get("exact_execution_capture_schema") != "fxpm.backtest-exact-executed-trades.v1"
                or type(facts.get("exact_swap_model_enabled")) is not bool
                or facts.get("exact_capture_count_matches") is not True):
            raise ValueError("invalid witness schema")
        spec = InstrumentSpec(**facts["instrument_spec"])
        if _canonical_bytes(vars(spec)) != _canonical_bytes(facts["instrument_spec"]):
            raise ValueError("instrument snapshot changed")
        encoded_arrays = facts["half_spread_arrays"]
        if type(encoded_arrays) is not dict or set(encoded_arrays) != {"P75", "P95"}:
            raise ValueError("invalid spread roster")
        arrays, count = {}, None
        for tier in ("P75", "P95"):
            encoded_pair = encoded_arrays[tier]
            if type(encoded_pair) is not dict or set(encoded_pair) != {"entry_half_spread", "intrabar_half_spread"}:
                raise ValueError("invalid spread vector pair")
            pair = {role: _route_a_decode_spread_array(encoded_pair[role], role)
                for role in ("entry_half_spread", "intrabar_half_spread")}
            arrays[tier] = pair
            for values in pair.values():
                if count is None:
                    count = len(values)
                elif count != len(values):
                    raise ValueError("crossed spread axes")
            rebuilt = trade_power.build_numerical_execution_cost_attestation(
                use_spread=facts["use_spread"],
                spread_mode=pm_core.build_backtest_spread_mode(facts["spread_model_metadata"][tier]),
                entry_half_spread_values_sha256=Backtester._spread_array_sha256("entry_half_spread", pair["entry_half_spread"]),
                intrabar_half_spread_values_sha256=Backtester._spread_array_sha256("intrabar_half_spread", pair["intrabar_half_spread"]),
                instrument_spec_sha256=Backtester._cost_attestation_json_sha256({
                    "schema_version": pm_core.BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION, "instrument_spec": vars(spec)}))
            if rebuilt != facts["numerical_execution_cost_attestations"][tier]:
                raise ValueError("spread vector attestation changed")
        session = None
        session_axis = None
        if "session_execution" in facts:
            session = _session_execution_context(facts["session_execution"], bar_count=count,
                trades=[row["base_trade_identity"] for row in facts["trades"]])
            if len(session["owner_spec_codes"]) != 1 or session["owner_spec_codes"][0] < 0:
                raise ValueError("invalid matched session owner")
            session_axis = session["specs"][session["owner_spec_codes"][0]]
        non_improvement = _route_a_spread_non_improvement(arrays)
        if non_improvement["status"] == "REFUSED_P95_BELOW_P75":
            return {**unavailable, "floor_status": "REFUSED_P95_BELOW_P75", "floor_reason_codes": ["P95_BELOW_P75"]}
        if non_improvement != facts["tier_non_improvement"]:
            raise ValueError("tier comparison changed")
        rows, events = facts["trades"], facts["base_position_events"]
        if type(rows) is not list or type(events) is not list:
            raise ValueError("invalid completed position roster")
        joined = len(rows) == len(events)
        ids, opens, closes, open_orders, close_orders, net_r, costs_r = [], [], [], [], [], [], []
        source = facts["initial_risk_source_binding"]
        if (source.get("instrument_spec_sha256") != _sha256(vars(spec))
                or source.get("caller") != "pm_authoritative_search_execution._run_tier_partition"
                or source.get("backtester") != "pm_core.Backtester.run"
                or source.get("governance_policy") is not None or source.get("entry_schedule") is not None
                or source.get("terminal_policy") != "LIQUIDATE"
                or source.get("stop_provenance") != "ORIGINAL_ENTRY_STOP_PRESERVED_BOTH_FIXED_CONTROL_BACKENDS"):
            raise ValueError("fixed-control source semantics changed")
        for ordinal, row in enumerate(rows):
            identity = row["base_trade_identity"]
            if build_backtest_trade_identity_pnl_evidence([identity]) != [identity]:
                raise ValueError("invalid trade identity")
            direction = 1 if identity["direction"] == "LONG" else -1
            actual_risk = float(spec.calculate_loss_at_stop(row["entry_price"], row["recorded_stop_price"], row["executed_volume"], direction))
            if (row["executed_volume"] != identity["position_size"] or not math.isfinite(actual_risk) or actual_risk <= 0
                    or row["actual_initial_risk_dollars"] != actual_risk
                    or row["risk_at_recorded_stop_dollars"] != actual_risk
                    or row["initial_stop_status"] != "ORIGINAL_ENTRY_STOP_FIXED_CONTROL"):
                raise ValueError("initial risk changed")
            components = _route_a_exact_base_return_components(row, spec=spec, ordinal=ordinal,
                swap_model_enabled=facts["exact_swap_model_enabled"])
            calculated_net = components["canonical_net_r"]
            if (row["base_return_components"] != components
                    or row["base_actual_initial_risk_net_r"] != calculated_net
                    or row["executed_price_swap_capture"]["backend"] != facts["exact_capture_backend"]):
                raise ValueError("exact BASE price/swap R changed")
            witness = row["base_fill_witness"]
            input_fields = {"direction", "entry_bar", "exit_bar", "partition_bar_count", "entry_time_utc",
                "exit_time_utc", "entry_mid_open", "p75_entry_half_price", "observed_entry_price", "observed_exit_price",
                "original_stop_price", "original_target_price", "exit_reason", "held_bar_inputs"}
            if session_axis is not None:
                input_fields.add("session_signal_open_utc")
            if not input_fields <= set(witness):
                reasons.append("BASE_FILL_WITNESS_INPUT_INVALID")
                continue
            inputs = {key: witness[key] for key in input_fields}
            replay = _route_a_replay_base_fill_inputs(inputs, session_axis=session_axis)
            expected_witness = {**inputs, **replay, "held_bar_inputs_sha256": _sha256(inputs["held_bar_inputs"])}
            if _canonical_bytes(expected_witness) != _canonical_bytes(witness):
                raise ValueError("fill verdict or captured path changed")
            opened, closed = identity["entry_bar"], identity["exit_bar"]
            if session_axis is not None and identity["signal_bar"] != opened - 1:
                raise ValueError("session signal-bar identity changed")
            if (inputs["direction"] != identity["direction"] or inputs["entry_bar"] != opened or inputs["exit_bar"] != closed
                    or inputs["partition_bar_count"] != count
                    or pd.Timestamp(inputs["entry_time_utc"]) != pd.Timestamp(identity["entry_time"])
                    or pd.Timestamp(inputs["exit_time_utc"]) != pd.Timestamp(identity["exit_time"])
                    or inputs["observed_entry_price"] != row["entry_price"]
                    or inputs["original_stop_price"] != row["recorded_stop_price"]
                    or inputs["exit_reason"] != identity["exit_reason"]
                    or inputs["p75_entry_half_price"] != arrays["P75"]["entry_half_spread"][opened]
                    or any(item["p75_intrabar_half_price"] != arrays["P75"]["intrabar_half_spread"][item["bar"]]
                        for item in inputs["held_bar_inputs"])):
                raise ValueError("fill inputs cross trade/array membership")
            if identity["exit_reason"] == "closed_session":
                if (session is None or session["exit_half_spreads"][ordinal] != arrays["P75"]["intrabar_half_spread"][closed]
                        or session["exit_open_prices"][ordinal] != inputs["held_bar_inputs"][-1]["ohlc"]["Open"]):
                    raise ValueError("session OPEN array capture changed")
            for tier in ("P75", "P95"):
                expected_sides = {"entry_half_spread_at_entry": arrays[tier]["entry_half_spread"][opened],
                    "intrabar_half_spread_at_entry": arrays[tier]["intrabar_half_spread"][opened],
                    "entry_half_spread_at_exit": arrays[tier]["entry_half_spread"][closed],
                    "intrabar_half_spread_at_exit": arrays[tier]["intrabar_half_spread"][closed]}
                if row["spread_sides"][tier] != expected_sides:
                    raise ValueError("selected spread sides changed")
            if replay["status"] != "VERIFIED":
                reasons.append(replay["reason_code"])
            else:
                numerator = float(arrays["P95"]["entry_half_spread"][opened]) + float(arrays["P95"]["intrabar_half_spread"][closed])
                distance = abs(row["entry_price"] - row["recorded_stop_price"])
                expected_inputs = {"entry_half_price": arrays["P95"]["entry_half_spread"][opened],
                    "exit_intrabar_half_price": arrays["P95"]["intrabar_half_spread"][closed],
                    "numerator_price": numerator, "original_initial_stop_distance_price": distance}
                cost_r = pm_core.canonical_backtest_winner_r(numerator, distance)
                if (row["matched_p95_cost_inputs"] != expected_inputs
                        or row["matched_p95_spread_cost_r_unrounded"] != numerator / distance
                        or row["matched_p95_spread_cost_r"] != cost_r):
                    raise ValueError("matched price-ratio cost changed")
                costs_r.append(cost_r)
            event = events[ordinal] if ordinal < len(events) else None
            if (type(event) is not dict or event.get("trade_record_schema_version") != pm_core.BACKTEST_POSITION_EVENT_SCHEMA_VERSION
                    or type(event.get("position_id")) is not str
                    or raw_winner._BACKTEST_POSITION_ID.fullmatch(event["position_id"]) is None or event["position_id"] in ids
                    or pd.Timestamp(event.get("entry_time")) != pd.Timestamp(identity["entry_time"])
                    or pd.Timestamp(event.get("exit_time")) != pd.Timestamp(identity["exit_time"])
                    or event.get("entry_event_order") != opened * 4 + 1 or event.get("exit_event_order") != closed * 4 + 3
                    or event.get("pnl_dollars") != identity["pnl_dollars"] or event.get("risk_amount") != identity["risk_amount"]
                    or row.get("base_position_event") != event):
                joined = False
            else:
                ids.append(event["position_id"])
                opens.append(pd.Timestamp(event["entry_time"]).strftime("%Y-%m-%dT%H:%M:%SZ"))
                closes.append(pd.Timestamp(event["exit_time"]).strftime("%Y-%m-%dT%H:%M:%SZ"))
                open_orders.append(event["entry_event_order"]); close_orders.append(event["exit_event_order"])
            net_r.append(calculated_net)
        if facts["base_fill_witness_sha256"] != _sha256([row["base_fill_witness"] for row in rows]):
            raise ValueError("fill witness roster changed")
        if not joined:
            reasons.append("BASE_POSITION_EVENT_BIJECTION_INVALID")
        if not rows:
            return {"floor_status": "UNAVAILABLE_NO_COMPLETED_BASE_TRADES", "floor_reason_codes": sorted(set(reasons)), **empty, "authenticates_origin": False}
        if reasons:
            return {**unavailable, "floor_reason_codes": sorted(set(reasons))}
        return {"floor_status": "AVAILABLE_MATCHED_BASE_VERIFIED", "floor_reason_codes": [],
            "completed_position_ids": ids, "completed_position_open_utc": opens, "completed_position_close_utc": closes,
            "completed_position_open_event_order": open_orders, "completed_position_close_event_order": close_orders,
            "base_actual_initial_risk_net_r": net_r, "matched_p95_spread_cost_r": costs_r, "authenticates_origin": False}
    except (KeyError, TypeError, ValueError, AttributeError, IndexError, OverflowError):
        return {**unavailable, "floor_reason_codes": ["SERIALIZED_MATCHED_BASE_FACTS_INVALID"]}



def _route_a_encode_spread_array(raw, role):
    """Keep the exact attested little-endian binary64 bytes, including sentinels."""
    import base64
    array = np.asarray(raw, dtype=np.float64)
    if array.ndim != 1:
        raise ValueError("spread vector must have one axis")
    exact = np.ascontiguousarray(array.astype("<f8", copy=False))
    return {"dtype": "<f8", "shape": [int(exact.size)],
        "values_base64": base64.b64encode(exact.tobytes(order="C")).decode("ascii"),
        "array_sha256": Backtester._spread_array_sha256(role, exact)}


def _route_a_decode_spread_array(encoded, role):
    """Decode without changing signed zeros, NaNs, sentinels or bar indexes."""
    import base64
    if (type(encoded) is not dict or set(encoded) != {"dtype", "shape", "values_base64", "array_sha256"}
            or encoded["dtype"] != "<f8" or type(encoded["shape"]) is not list or len(encoded["shape"]) != 1
            or type(encoded["shape"][0]) is not int or encoded["shape"][0] < 0
            or type(encoded["values_base64"]) is not str):
        raise ValueError("invalid binary spread vector")
    raw = base64.b64decode(encoded["values_base64"], validate=True)
    if len(raw) != encoded["shape"][0] * 8:
        raise ValueError("binary spread vector shape changed")
    array = np.frombuffer(raw, dtype="<f8")
    if Backtester._spread_array_sha256(role, array) != encoded["array_sha256"]:
        raise ValueError("binary spread vector hash changed")
    return array



def _route_a_spread_non_improvement(arrays):
    """Check every finite bar; retain nonfinite sentinel ordering as unknown."""
    unknown = {}
    for role in ("entry_half_spread", "intrabar_half_spread"):
        base = np.asarray(arrays["P75"][role], dtype=np.float64)
        stress = np.asarray(arrays["P95"][role], dtype=np.float64)
        if base.ndim != 1 or stress.shape != base.shape:
            raise ValueError("tier comparison axes differ")
        finite = np.isfinite(base) & np.isfinite(stress)
        inversion = np.flatnonzero(finite & (stress < base))
        if inversion.size:
            return {"status": "REFUSED_P95_BELOW_P75", "role": role, "bar": int(inversion[0])}
        missing = np.flatnonzero(~finite)
        if missing.size:
            unknown[role] = [int(index) for index in missing]
    return ({"status": "NO_FINITE_INVERSION_WITH_SENTINELS", "unknown_indices": unknown}
        if unknown else {"status": "VERIFIED_ALL_BARS"})


def _verify_route_a_prepared_stage_origin(stage, *, capture, frozen, unit, raw_body):
    """Keep each floor vector attached to its actual selected unit and Raw body."""
    if (type(stage) is not _PreparedRouteAFullPanelStage
            or type(capture) is not historical_adapter.DeclaredSlotSearchCapture
            or not historical_adapter._verify_declared_slot_capture(capture)
            or not capture._winner_generation.matched_floor_enabled
            or capture._active is None or capture._active.frozen is not frozen):
        _fail("ROUTE_A_FULL_PANEL_STAGE_ORIGIN_INVALID")
    handle = capture._active
    header = _read_declared_unit_header(capture.context, capture.request_body)
    if header is None or _sha256(header["binding"]) != stage.declared_unit_binding_sha256:
        _fail("ROUTE_A_FULL_PANEL_STAGE_ORIGINAL_BINDING_CHANGED")
    original = stage.unit_body(unit)
    if _canonical_bytes(original) != _canonical_bytes(raw_body):
        _fail("ROUTE_A_FULL_PANEL_STAGE_RAW_BODY_CHANGED")
    facts = stage.matched_base_facts(unit["ordinal"])
    row = stage.rows[unit["ordinal"]]
    origin = {"schema_version": "fxpm.matched-floor-stage-origin-binding.v1",
        "producer_request_sha256": capture.request.receipt_sha256,
        "process_receipt_sha256": capture.process.receipt_sha256,
        "frozen_receipt_sha256": handle.frozen_receipt_descriptor["sha256"],
        "precommitment_sha256": _sha256(frozen.precommitment),
        "declared_unit_binding_sha256": stage.declared_unit_binding_sha256,
        "preparation_contract_sha256": stage.preparation_contract_sha256,
        "unit": dict(unit), "stage_content_sha256": row.sha256, "stage_size_bytes": row.size_bytes,
        "raw_body_sha256": _sha256(original), "matched_base_facts_sha256": _sha256(facts)}
    return facts, origin




def _reopen_route_a_matched_source_material(context, *, request, process, frozen,
        frozen_pipeline_config, m5_fitted=None, _bound_fit_metadata_out=None):
    """Reopen retained source for verification only; no fitting or execution grant."""
    import pm_production_search_planner as planner
    planner.verify_frozen_production_search_process(context, process)
    if (frozen.selection_generation.selection_version != planner.source_edition_info(process).winner_generation_id
            or frozen.bound_fitted_state_entry is None):
        _fail("MATCHED_FLOOR_FROZEN_SOURCE_EDITION_REQUIRED")
    frozen_pipeline_config.verify_current(context)
    config = frozen_pipeline_config.materialize(context)
    state, delta = current_refit.read_bound_fitted_state_reference(context, frozen.bound_fitted_state_entry,
        artifact_namespace=frozen.artifact_namespace)
    meta, receipts = state["metadata"], state["metadata"]["receipts"]
    independent = frozen.selection_generation.independent_signal_r_enabled
    current = independent and meta.get("binding_mode") == "CURRENT_OBSERVED_INDEPENDENT_R"
    if current:
        if (meta["binding"]["producer_request_sha256"] != meta["binding"]["cell_plan_receipt_sha256"]
                or frozen.bound_fitted_state_entry["producer_request_sha256"] != meta["binding"]["producer_request_sha256"]):
            _fail("INDEPENDENT_R_CURRENT_SOURCE_BINDING_CHANGED")
        body = None
    else:
        body = historical.verify_declared_slot_producer_request(context, request, process=process)
    if ((not current and meta["binding"]["producer_request_sha256"] != request.receipt_sha256)
            or meta["binding"]["process_sha256"] != process.receipt_sha256
            or meta["binding"]["effective_config_sha256"] != frozen_pipeline_config.effective_config_sha256
            or _canonical_bytes(meta["fold_plan"]) != _canonical_bytes(frozen.precommitment["causal_fold_plan"])
            or _canonical_bytes(receipts["source_contract_seeds"]) != _canonical_bytes(frozen.source_contract_seeds)
            or _canonical_bytes(receipts["frozen_tier_pair_contract"]) != _canonical_bytes(frozen.frozen_tier_pair_contract)):
        _fail("MATCHED_FLOOR_RETAINED_SOURCE_BINDING_CHANGED")
    if body is not None and historical.declared_slot_source_kind(body) == "m5_derived":
        if (m5_fitted is None or _frame_sha256(m5_fitted._frame) != meta["frame_sha256"]
                or _canonical_bytes(asdict(m5_fitted.instrument_spec)) != _canonical_bytes(receipts["instrument_spec"])
                or _canonical_bytes(m5_fitted.frozen_tier_pair_contract) != _canonical_bytes(frozen.frozen_tier_pair_contract)):
            _fail("MATCHED_FLOOR_VERIFIED_M5_SOURCE_REQUIRED")
        result = (m5_fitted._frame, m5_fitted.instrument_spec, m5_fitted._spread_tiers, config)
        if _bound_fit_metadata_out is not None:
            _bound_fit_metadata_out.update(meta)
        return (*result, m5_fitted._entry_spread_observations) if independent else result
    if m5_fitted is not None:
        _fail("MATCHED_FLOOR_NATIVE_SOURCE_REJECTS_M5")
    target = _bound_fit_open_carrier(context, config, meta["target_carrier"])
    fine = _bound_fit_open_carrier(context, config, meta["fine_carrier"])
    base = native_prefix.open_verified_prefix_frame(target)
    if (not base.index.equals(delta.index)
            or [{"name": name, "dtype": str(dtype)} for name, dtype in base.dtypes.items()] != meta["base_columns"]):
        _fail("MATCHED_FLOOR_ORIGINAL_FRAME_CHANGED")
    frame = base.copy(deep=True)
    for name in _REGIME_COLUMNS:
        frame[name] = delta[name]
    if _frame_sha256(frame) != meta["frame_sha256"]:
        _fail("MATCHED_FLOOR_ORIGINAL_FRAME_CHANGED")
    spec, spec_receipt = _freeze_bound_execution_spec(product=target, config=config)
    tiers, observations, verified_fine, _ = _native_spread_inputs(product=target, config=config,
        frame=base, spec=spec, _reopened_fine_product=fine)
    if (verified_fine.index_sha256 != meta["fine_carrier"]["index_sha256"]
            or hashlib.sha256(pd.util.hash_pandas_object(observations, index=True).values.tobytes()).hexdigest()
                != meta["entry_observations_sha256"]
            or _canonical_bytes(spec_receipt) != _canonical_bytes(receipts["execution_spec_receipt"])
            or _canonical_bytes(asdict(spec)) != _canonical_bytes(receipts["instrument_spec"])
            or any(level["frozen_spread_tier"] != tiers[level["tier"]].descriptor()
                for level in frozen.frozen_tier_pair_contract["tiers"])):
        _fail("MATCHED_FLOOR_ORIGINAL_COST_SOURCE_CHANGED")
    result = frame, spec, tiers, config
    if _bound_fit_metadata_out is not None:
        _bound_fit_metadata_out.update(meta)
    return (*result, observations) if independent else result


def _verify_independent_r_fact_source(facts, geometry, *, source_frame, spec, tiers,
        config, timeframe, _prepared_source_cache=None, entry_observations=None, target_regime=None):
    """Replay the pure method over one authenticated prefix, bounded to this window."""
    from types import SimpleNamespace
    import pm_production_config_binding as config_binding
    index, batches = facts["index"], facts["batches"]
    cache = {} if _prepared_source_cache is None else _prepared_source_cache
    source_key = ("independent-source", id(source_frame))
    source_sha = cache.get(source_key)
    if source_sha is None:
        source_sha = _frame_sha256(source_frame)
        cache[source_key] = source_sha
    config_sha = _independent_r_configuration_sha256(config, cache)
    if (source_sha != index["source_prefix_frame_sha256"]
            or config_sha != index["configuration_sha256"]
            or _canonical_bytes(asdict(spec)) != _canonical_bytes(index["instrument_spec"])
            or _canonical_bytes(geometry) != _canonical_bytes(index["partition_geometry"])):
        _fail("INDEPENDENT_R_SOURCE_BINDING_CHANGED")
    strategy = StrategyRegistry.get(index["strategy_id"], **index["parameters"])
    if config_binding._json_value(_independent_r_session_state(strategy, spec.symbol)) != index["session_spec"]:
        _fail("INDEPENDENT_R_SESSION_SOURCE_CHANGED")
    recipe = (index["strategy_id"], _canonical_bytes(index["parameters"]),
        _canonical_bytes(index["regime_parameters"]), index["regime_parameters_sha256"])
    surface_key = ("independent-surface", id(source_frame), config_sha, recipe)
    active_key = ("independent-surface-current", id(source_frame))
    previous = cache.get(active_key)
    if previous != surface_key:
        if previous is not None:
            cache.pop(previous, None)
        cache[active_key] = surface_key
    before = _route_a_plain_strategy_state(strategy)
    cached = cache.get(surface_key) if before is not None else None
    if cached is None or cached[0] != before:
        fitted = SimpleNamespace(regime_parameters=index["regime_parameters"],
            regime_fit_decision=SimpleNamespace(to_dict=lambda: {
                "regime_params_sha256": index["regime_parameters_sha256"]}))
        surface = _build_full_prefix_strategy_surface(frame=source_frame, strategy=strategy,
            fitted=fitted, symbol=spec.symbol, timeframe=timeframe)
        if not surface.features.loc[:, list(source_frame.columns)].equals(source_frame):
            _fail("FULL_PREFIX_STRATEGY_INPUT_MUTATED")
        after = _route_a_plain_strategy_state(strategy)
        surface = replace(surface, _content_sha256=_independent_r_surface_sha(surface))
        if before is not None and after is not None:
            cache[surface_key] = (before, after, surface)
    else:
        strategy.__dict__.clear()
        strategy.__dict__.update(copy.deepcopy(cached[1]))
        surface = cached[2]
    surface_sha = _independent_r_surface_sha(surface)
    if surface_sha != index["surface_sha256"]:
        _fail("INDEPENDENT_R_SIGNAL_SURFACE_CHANGED")
    mask = ((source_frame.index >= pd.Timestamp(geometry["frame_start_utc"]))
        & (source_frame.index < pd.Timestamp(geometry["end_exclusive_utc"])))
    base = source_frame.loc[mask]
    if len(base) != geometry["frame_rows"] or _frame_sha256(base) != geometry["frame_sha256"]:
        _fail("INDEPENDENT_R_PARTITION_SOURCE_CHANGED")
    features, signals = surface.features.loc[base.index].copy(deep=True), surface.raw_signals.loc[base.index].copy(deep=True)
    if target_regime is not None:
        signals = _independent_r_cell_signals(features, signals, target_regime)
    if entry_observations is None or not isinstance(entry_observations, pd.Series):
        _fail("INDEPENDENT_R_VERIFIED_ADMISSIONS_REQUIRED")
    observations = entry_observations.loc[base.index]
    episodes_key = ("independent-episodes", id(source_frame), index["source_prefix_frame_sha256"])
    episodes = cache.get(episodes_key)
    if episodes is None:
        episodes = signal_opportunities.independent_entry_episodes(source_frame,
            symbol=spec.symbol, timeframe=timeframe, cutoff_utc=source_frame["_BAR_CLOSE_TS"].iloc[-1])
        cache[episodes_key] = episodes
    for tier in ("P75", "P95"):
        cost_key = ("independent-pricing", id(source_frame), config_sha,
            geometry["receipt_sha256"], tier, tiers[tier]._json)
        prepared = cache.get(cost_key)
        if prepared is None:
            pricing, values = Backtester(config), {}
            pricing.build_evidence_source_seed_result(base, spec.symbol, spec=spec,
                timeframe=timeframe, spread_tier=tiers[tier], entry_spread_observations=observations,
                _prepared_inputs_out=values)
            prepared = pricing, values
            cache[cost_key] = prepared
        pricing, values = prepared
        probe = values["source_probe"]
        arrays = {name: _route_a_encode_spread_array(values[name], name)
            for name in ("entry_half_spread", "intrabar_half_spread")}
        if (arrays != index["half_spread_arrays"][tier]
                or probe["numerical_execution_cost_attestation"] != index["numerical_execution_cost_attestations"][tier]
                or probe["spread_model"] != index["spread_model_metadata"][tier]):
            _fail("INDEPENDENT_R_COST_SOURCE_CHANGED")
        provenance = signal_opportunities.OpportunityProvenance(strategy.name, config_sha,
            surface_sha, tier, _sha256(probe["numerical_execution_cost_attestation"]),
            _sha256(probe["spread_admission"]))
        expected = signal_opportunities.evaluate_independent_signal_r(features, signals,
            pricing=pricing, episode_ids=episodes.loc[base.index], strategy=strategy,
            spec=spec, symbol=spec.symbol, timeframe=timeframe,
            cutoff_utc=geometry["end_exclusive_utc"],
            signal_start_utc=features["_BAR_CLOSE_TS"].iloc[max(1, geometry["warmup_bars"]) - 1],
            entry_half_spreads=values["entry_half_spread"],
            exit_half_spreads=values["intrabar_half_spread"],
            entry_admission=values["admission_states"], provenance=provenance)
        if _canonical_bytes(asdict(expected)) != _canonical_bytes(batches[tier]):
            _fail("INDEPENDENT_R_OUTCOME_SOURCE_CHANGED")
    return True


def _route_a_source_column_number(frame, columns, name, bar):
    if not isinstance(bar, (int, np.integer)) or isinstance(bar, (bool, np.bool_)):
        return float(frame.iloc[bar][name])
    if name not in columns:
        column = frame[name].to_numpy(copy=False).view()
        column.setflags(write=False)
        columns[name] = column
    return float(columns[name][bar])


def _verify_route_a_matched_fact_source(facts, geometry, *, source_frame, spec, tiers, config, timeframe,
        _prepared_source_cache=None):
    """Compare witness OHLC and complete half arrays to reopened source without a model run."""
    spec_bytes = _canonical_bytes(vars(spec))
    if spec_bytes != _canonical_bytes(facts["instrument_spec"]):
        _fail("MATCHED_FLOOR_PARTITION_SOURCE_CHANGED")
    pricing = None
    prepared = None
    if _prepared_source_cache is not None:
        key = (id(source_frame), _canonical_bytes(geometry), id(config),
            pickle.dumps(vars(config), protocol=5), id(spec), spec_bytes, timeframe,
            tuple((tier, id(tiers[tier]), tiers[tier]._json) for tier in ("P75", "P95")))
        prepared = _prepared_source_cache.get(key)
    if prepared is None:
        frame = source_frame.loc[(source_frame.index >= pd.Timestamp(geometry["frame_start_utc"]))
            & (source_frame.index < pd.Timestamp(geometry["end_exclusive_utc"]))]
        if len(frame) != geometry["frame_rows"] or _frame_sha256(frame) != geometry["frame_sha256"]:
            _fail("MATCHED_FLOOR_PARTITION_SOURCE_CHANGED")
        pricing = Backtester(config)
        expected_tiers = []
        for tier in ("P75", "P95"):
            entry, intrabar, metadata = pricing._build_half_spread_arrays(frame, spec,
                spread_tier=tiers[tier], timeframe=timeframe)
            expected = {"entry_half_spread": _route_a_encode_spread_array(entry, "entry_half_spread"),
                "intrabar_half_spread": _route_a_encode_spread_array(intrabar, "intrabar_half_spread")}
            expected_tiers.append((tier, _canonical_bytes(expected), _canonical_bytes(metadata)))
        prepared = frame, tuple(expected_tiers), {}, pricing, ({} if _prepared_source_cache is not None else None)
        if _prepared_source_cache is not None:
            _prepared_source_cache[key] = prepared
    frame, expected_tiers, columns, pricing, swap_preparation = prepared
    for tier, expected, metadata in expected_tiers:
        if (expected != _canonical_bytes(facts["half_spread_arrays"][tier])
                or metadata != _canonical_bytes(facts["spread_model_metadata"][tier])):
            _fail("MATCHED_FLOOR_SPREAD_ARRAY_SOURCE_CHANGED")
    if "session_execution" in facts:
        _session_execution_context(facts["session_execution"], bar_count=len(frame), index=frame.index,
            trades=[row["base_trade_identity"] for row in facts["trades"]])
    for row in facts["trades"]:
        fill = row["base_fill_witness"]
        inputs = fill
        if "held_bar_inputs" not in inputs:
            if facts["floor_status"] == "AVAILABLE_MATCHED_BASE_VERIFIED":
                _fail("MATCHED_FLOOR_SOURCE_INPUTS_MISSING")
            continue
        entry_bar = inputs["entry_bar"]
        if (not 0 <= entry_bar < len(frame)
                or inputs["entry_mid_open"] != _route_a_source_column_number(frame, columns, "Open", entry_bar)
                or pd.Timestamp(inputs["entry_time_utc"]) != frame.index[entry_bar]):
            _fail("MATCHED_FLOOR_ENTRY_SOURCE_CHANGED")
        if "session_execution" in facts and (entry_bar < 1
                or pd.Timestamp(inputs["session_signal_open_utc"]) != frame.index[entry_bar - 1]):
            _fail("MATCHED_FLOOR_SESSION_SIGNAL_SOURCE_CHANGED")
        for held in inputs["held_bar_inputs"]:
            bar = held["bar"]
            if (not 0 <= bar < len(frame)
                    or pd.Timestamp(held["open_utc"]) != frame.index[bar]
                    or held["ohlc"] != {name: _route_a_source_column_number(frame, columns, name, bar) for name in ("Open", "High", "Low", "Close")}):
                _fail("MATCHED_FLOOR_HELD_BAR_SOURCE_CHANGED")
    _verify_route_a_exact_swap_source(facts, frame, spec=spec, pricing=pricing, timeframe=timeframe,
        _static_preparation=swap_preparation)
    return True


def _route_a_intern_matched_codec_strings(facts, pool):
    """Reuse exact immutable strings only; every owner still verifies arrays and attestations."""
    if facts is None:
        return
    for tier, legs in facts["half_spread_arrays"].items():
        for role, codec in legs.items():
            value = codec["values_base64"]
            key = (role, codec["dtype"], tuple(codec["shape"]), codec["array_sha256"],
                hashlib.sha256(value.encode("ascii")).hexdigest())
            previous = pool.get(key)
            if previous is None:
                pool[key] = value
            elif previous == value:
                codec["values_base64"] = previous
            else:
                _fail("ROUTE_A_FULL_PANEL_CODEC_INTERN_IDENTITY_CHANGED")


def _route_a_exact_base_return_components(row, *, spec, ordinal, swap_model_enabled):
    """Pure executed-price/swap arithmetic; this data alone authenticates no source."""
    exact, identity = row["executed_price_swap_capture"], row["base_trade_identity"]
    fields = {"schema_version", "ordinal", "backend", "signal_bar", "entry_bar", "exit_bar", "direction",
        "entry_price", "exit_price", "initial_stop_price", "executed_volume", "exit_reason",
        "swap_dollars_exact", "swap_rollover_units", "swap_source_kind", "swap_cash_steps"}
    if (type(exact) is not dict or set(exact) != fields or exact["schema_version"] != "fxpm.backtest-exact-executed-trade.v1"
            or type(exact["ordinal"]) is not int or exact["ordinal"] != ordinal or exact["backend"] not in {"PYTHON", "NUMBA"}
            or any(exact[key] != identity[key] for key in ("signal_bar", "entry_bar", "exit_bar", "direction", "exit_reason"))
            or exact["entry_price"] != row["entry_price"] or exact["initial_stop_price"] != row["recorded_stop_price"]
            or exact["executed_volume"] != row["executed_volume"]
            or exact["executed_volume"] != identity["position_size"]
            or exact["exit_price"] != row["base_fill_witness"].get("observed_exit_price")):
        raise ValueError("exact trade identity changed")
    direction = 1 if exact["direction"] == "LONG" else -1
    values = [exact[key] for key in ("entry_price", "exit_price", "initial_stop_price", "executed_volume", "swap_dollars_exact")]
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
        raise ValueError("nonfinite exact trade values")
    distance = abs(exact["entry_price"] - exact["initial_stop_price"])
    risk = float(spec.calculate_loss_at_stop(exact["entry_price"], exact["initial_stop_price"], exact["executed_volume"], direction))
    if distance <= 0 or risk <= 0 or not math.isfinite(risk) or exact["executed_volume"] <= 0:
        raise ValueError("invalid executed risk")
    kind, steps = exact["swap_source_kind"], exact["swap_cash_steps"]
    if type(steps) is not list or type(exact["swap_rollover_units"]) is not int or exact["swap_rollover_units"] < 0:
        raise ValueError("invalid exact swap steps")
    if kind not in {"CAUSAL_KERNEL", "LEGACY_MODEL", "DISABLED_OR_PENDING_LEGACY"}:
        raise ValueError("invalid exact swap source kind")
    if (kind == "DISABLED_OR_PENDING_LEGACY" and (swap_model_enabled or steps or exact["swap_dollars_exact"] != 0)
            or not swap_model_enabled and kind != "DISABLED_OR_PENDING_LEGACY"):
        raise ValueError("unproven enabled swap")
    cash, units, previous = 0.0, 0, None
    for step in steps:
        if type(step) is not dict or set(step) != {"bar", "phase", "rollover_units", "rate_per_lot_dollars", "cash"}:
            raise ValueError("invalid swap step")
        phase = {"PRE_OPEN": 0, "CLOSE": 1, "LEGACY_AT_EXIT": 2}.get(step["phase"])
        key = (step["bar"], phase)
        if (type(step["bar"]) is not int or phase is None or type(step["rollover_units"]) is not int
                or step["rollover_units"] <= 0 or not exact["entry_bar"] <= step["bar"] <= exact["exit_bar"]
                or previous is not None and key <= previous
                or kind == "CAUSAL_KERNEL" and (phase == 2
                    or phase == 0 and step["bar"] <= exact["entry_bar"]
                    or phase == 1 and step["bar"] == exact["exit_bar"] and exact["exit_reason"] != pm_core.TradeStatus.CLOSED_EOD.value)
                or kind == "LEGACY_MODEL" and (phase != 2 or step["bar"] != exact["exit_bar"])
                or type(step["rate_per_lot_dollars"]) not in (int, float)
                or not math.isfinite(step["rate_per_lot_dollars"])):
            raise ValueError("invalid ordered swap step")
        expected = step["rate_per_lot_dollars"] * exact["executed_volume"] * step["rollover_units"]
        if step["cash"] != expected:
            raise ValueError("swap cash multiplication changed")
        cash += expected;units += step["rollover_units"];previous = key
    if cash != exact["swap_dollars_exact"] or units != exact["swap_rollover_units"]:
        raise ValueError("exact swap accumulation changed")
    price_r = direction * (exact["exit_price"] - exact["entry_price"]) / distance
    swap_r = cash / risk
    return {"price_move_r": price_r, "swap_cash_exact": cash, "swap_r": swap_r,
        "executed_initial_stop_risk_cash": risk,
        "canonical_net_r": pm_core.canonical_backtest_winner_r(price_r + swap_r, 1.0)}


def _verify_route_a_exact_swap_source(facts, frame, *, spec, pricing, timeframe, _static_preparation=None):
    """Rebuild captured swap inputs from reopened source; no strategy or execution replay."""
    enabled = bool(getattr(pricing.config, "swap_model_enabled", True))
    if facts.get("exact_swap_model_enabled") is not enabled:
        _fail("MATCHED_FLOOR_SWAP_CONFIG_SOURCE_CHANGED")
    entries = np.full(len(frame), np.nan, dtype=np.float64)
    for row in facts["trades"]:
        exact = row["executed_price_swap_capture"]
        bar = exact["entry_bar"]
        if type(bar) is not int or not 0 <= bar < len(frame) or np.isfinite(entries[bar]):
            _fail("MATCHED_FLOOR_SWAP_ENTRY_SOURCE_CHANGED")
        entries[bar] = exact["entry_price"]
    pre_open, close, longs, shorts, causal, contract = pricing._build_causal_swap_inputs(
        frame, spec, entries, timeframe=timeframe,
        **({"_static_preparation": _static_preparation} if _static_preparation is not None else {}))
    if _canonical_bytes(contract) != _canonical_bytes(facts.get("exact_swap_accrual_contract")):
        _fail("MATCHED_FLOOR_SWAP_CONTRACT_SOURCE_CHANGED")
    for ordinal, row in enumerate(facts["trades"]):
        exact = row["executed_price_swap_capture"]
        _route_a_exact_base_return_components(row, spec=spec, ordinal=ordinal, swap_model_enabled=enabled)
        expected = {"trades": []}
        pm_core._append_exact_backtest_trade(expected, backend=exact["backend"], signal_bar=exact["signal_bar"],
            entry_bar=exact["entry_bar"], exit_bar=exact["exit_bar"],
            direction=1 if exact["direction"] == "LONG" else -1,
            entry_price=exact["entry_price"], exit_price=exact["exit_price"], initial_stop=exact["initial_stop_price"],
            volume=exact["executed_volume"], exit_reason=exact["exit_reason"], swap_cash=0.0, swap_units=0,
            causal_swap_enabled=causal, pre_open_units=pre_open, close_units=close, long_rates=longs, short_rates=shorts)
        rebuilt = expected["trades"][0]
        rebuilt["ordinal"] = ordinal
        if causal:
            cash, units = 0.0, 0
            for step in rebuilt["swap_cash_steps"]:
                cash += step["cash"]; units += step["rollover_units"]
            rebuilt["swap_dollars_exact"], rebuilt["swap_rollover_units"] = cash, units
        elif enabled:
            identity = row["base_trade_identity"]
            schedule = pricing._calendar_swap_rollover_schedule(identity, spec=spec)
            units = (sum(int(weight) for _timestamp, weight in schedule) if schedule is not None
                else pricing._swap_rollover_units(identity, frame, timeframe=timeframe, spec=spec))
            rate = (pricing._swap_value_per_lot_dollars(spec,
                spec.swap_long if exact["direction"] == "LONG" else spec.swap_short,
                entry_price=exact["entry_price"]) if units > 0 else 0.0)
            cash = rate * exact["executed_volume"] * units if units > 0 else 0.0
            rebuilt.update(swap_source_kind="LEGACY_MODEL", swap_dollars_exact=cash,
                swap_rollover_units=int(units), swap_cash_steps=[] if units <= 0 else [{
                    "bar": exact["exit_bar"], "phase": "LEGACY_AT_EXIT", "rollover_units": int(units),
                    "rate_per_lot_dollars": rate, "cash": cash}])
        if _canonical_bytes(rebuilt) != _canonical_bytes(exact):
            _fail("MATCHED_FLOOR_EXACT_SWAP_SOURCE_CHANGED")
    return True
