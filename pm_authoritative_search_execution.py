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

import hashlib
import hmac
import io
import json
import math
import os
import secrets
import stat
import tempfile
import time
from dataclasses import asdict, dataclass, replace
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
from pm_strategies import StrategyRegistry


UTC: Final = timezone.utc
EXECUTION_CONTRACT_VERSION: Final = "fxpm.authoritative-search-execution.v5"
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
MAX_CELL_EVIDENCE_BYTES: Final = 8 * 1024**3
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


@dataclass(frozen=True, slots=True)
class _StagedAttemptEvidence:
    ordinal: int
    kind: str
    path: Path | None
    size_bytes: int
    sha256: str


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
    frame: pd.DataFrame, spec: InstrumentSpec,
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
        if source_timeframe == native["timeframe"]:
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
        fine_frame = native_prefix.open_verified_prefix_frame(fine)
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
    window = {
        "schema_version": "fxpm.historical-regime-fit-window.v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "prefix_content_sha256": native["prefix_content_sha256"],
        "prefix_start_utc": native["prefix_start_utc"],
        "prefix_end_exclusive_utc": native["prefix_end_exclusive_utc"],
        "prefix_rows": native["prefix_rows"],
    }
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
        reason_code="AUTHENTICATED_NATIVE_PREFIX_AVAILABLE",
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
    try:
        detector = pm_regime.MarketRegimeDetector(
            pm_regime.RegimeParams.from_dict(parameters)
        )
        surface = detector.compute_regime_scores(frame)
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
    )
    detector_record = book.create_regime_detector_record(
        source_group_decision=source,
        fit_cutoff_utc=_book_canonical_utc(
            native["prefix_end_exclusive_utc"],
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
    evidence = {
        "schema_version": REGIME_FIT_EVIDENCE_VERSION,
        "source_group_decision_sha256": source.receipt_sha256,
        "native_prefix_index_sha256": product.index_sha256,
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "detector_record_sha256": detector_record["record_sha256"],
    }
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
    )


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
    normalized_regime = str(regime or "").strip().upper()
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
        or not 1 <= len(planned_attempts) <= MAX_ATTEMPTS
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
        _utc(prefix["prefix_end_exclusive_utc"], "PREFIX_END_INVALID")
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
            != _utc(prefix["prefix_end_exclusive_utc"], "PREFIX_END_INVALID")
            or _utc(
                horizon_body["latest_closed_bar_utc"],
                "ATTEMPT_LATEST_CLOSE_INVALID",
            )
            != _utc(prefix["prefix_last_close_utc"], "PREFIX_LAST_CLOSE_INVALID")
            or horizon_body["source_snapshot_sha256"] != prefix["prefix_content_sha256"]
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
) -> _PartitionSlice:
    start = pd.Timestamp(_utc(frame_start_utc, "PARTITION_FRAME_START_INVALID"))
    scored = pd.Timestamp(_utc(scored_start_utc, "PARTITION_SCORE_START_INVALID"))
    end = pd.Timestamp(_utc(end_exclusive_utc, "PARTITION_END_INVALID"))
    if not start <= scored < end:
        _fail("PARTITION_INTERVAL_INVALID", role)
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
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
    )
    tuning = _partition_slice(
        frame,
        role="tuning_validation",
        frame_start_utc=tuning_frame_start.isoformat(),
        scored_start_utc=tuning_score_start.isoformat(),
        end_exclusive_utc=fit_end.isoformat(),
        minimum_warmup_bars=minimum_warmup,
    )
    final = _partition_slice(
        frame,
        role="final_lockbox",
        frame_start_utc=fold.get("validation_warmup_start_utc"),
        scored_start_utc=fold.get("validation_start_utc"),
        end_exclusive_utc=fold.get("validation_end_exclusive_utc"),
        minimum_warmup_bars=minimum_warmup,
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
    source_cache = {} if source_observation_cache is None else source_observation_cache
    rows = []
    for attempt in attempts:
        if attempt.plan_attempt.get("status") != "PLANNED":
            continue
        causal_folds = _causal_folds(attempt)
        if len(causal_folds) != len(fold_plan):
            _fail("PARTITION_COST_PRECOMMIT_FOLD_ROSTER_INVALID", attempt.ordinal)
        for fold_ordinal, fold in enumerate(causal_folds):
            partitions = _fold_partitions(frame=fitted._frame, fold=fold, attempt=attempt,
                dependency_warmup_bars=geometry["warmup_bars"])
            for partition in partitions:
                identity = partition.geometry_receipt["receipt_sha256"]
                tiers = cache.get(identity)
                if tiers is None:
                    observations = fitted._entry_spread_observations.loc[partition.frame.index]
                    probes = {tier: backtester.build_evidence_source_seed_result(
                        partition.frame, plan_body["cell"]["symbol"],
                        spec=fitted.instrument_spec, timeframe=plan_body["cell"]["timeframe"],
                        spread_tier=bound_tier, entry_spread_observations=observations,
                    ) for tier, bound_tier in fitted._spread_tiers.items()}
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


def _partition_source_observation_reference(*, references, precommitments, pair,
        attempt, fold_ordinal, partition):
    geometry = partition.geometry_receipt
    reference = references.get(geometry["receipt_sha256"])
    rows = precommitments.get("rows")
    matches = [row for row in rows if (row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition"))
        == (attempt.ordinal, fold_ordinal, partition.role)] if type(rows) is list else []
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
) -> None:
    rows = precommitments.get("rows")
    matches = [row for row in rows if (row.get("attempt_ordinal"), row.get("fold_ordinal"), row.get("partition"))
        == (attempt.ordinal, fold_ordinal, partition.role)] if type(rows) is list else []
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
) -> tuple[Any, dict[str, Any]]:
    strategy_id = attempt.artifacts.winner_process["strategy_id"]
    base = StrategyRegistry.get(strategy_id)
    try:
        parameters = decode_parameter_mapping(
            attempt.artifacts.trial_definition["parameters"],
            base.get_default_params(),
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


def _build_full_prefix_strategy_surface(
    *,
    frame: pd.DataFrame,
    strategy: Any,
    symbol: str,
    timeframe: str,
) -> _FullPrefixStrategySurface:
    """Replay path state once over the authenticated prefix before slicing."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        _fail("FULL_PREFIX_FRAME_INVALID")
    try:
        features = FeatureComputer.compute_for_strategy(
            frame,
            strategy,
            symbol=symbol,
            timeframe=timeframe,
        )
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
    _decision_relevant_result(result, partition.role)
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
        "position_events_are_genuine_backtester_v2": True,
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
) -> dict[str, Any]:
    strategy, _parameters = _runtime_strategy(attempt)
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
        frame=frame, strategy=strategy, symbol=symbol, timeframe=timeframe)
    backtester = Backtester(config)
    folds = []
    for fold_ordinal, fold in enumerate(_causal_folds(attempt)):
        partitions = _fold_partitions(frame=frame, fold=fold, attempt=attempt,
            dependency_warmup_bars=geometry["warmup_bars"])
        lanes, partition_signals = {}, {}
        for partition in partitions:
            actual_results = {}
            for tier in ("P75", "P95"):
                result, _features, signals, _backtester = _run_tier_partition(
                    partition=partition, full_prefix_surface=full_prefix_surface,
                    backtester=backtester, strategy=strategy, symbol=symbol,
                    timeframe=timeframe, regime=regime, config=config,
                    spec=fitted.instrument_spec,
                    expected_source_contract_sha256=_source_identity(fitted.source_contract_seeds[tier]),
                    spread_tier=fitted._spread_tiers[tier],
                    entry_spread_observations=fitted._entry_spread_observations,
                )
                _verify_partition_precommitment(precommitments=partition_cost_precommitments,
                    attempt=attempt, fold_ordinal=fold_ordinal, partition=partition,
                    tier=tier, result=result, pair=fitted.frozen_tier_pair_contract)
                if partition.role in partition_signals and not partition_signals[partition.role].equals(signals):
                    _fail("PARTITION_SIGNAL_VECTOR_TIER_MISMATCH")
                partition_signals[partition.role] = signals
                name = f"{partition.role}_{tier.lower()}"
                lanes[name] = _decision_relevant_result(result, name)
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


def _reverify_fitted_timeframe(
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> FittedNativeSearchTimeframe:
    """Re-open every caller-held byte and recompute the bound regime surface."""

    if type(fitted) is not FittedNativeSearchTimeframe:
        _fail("FITTED_TIMEFRAME_INVALID")
    source = fitted.source_group_decision.to_dict()
    attempts = source.get("acquisition_attempts_exact")
    if type(attempts) is not int or attempts <= 0:
        _fail("SOURCE_ACQUISITION_ATTEMPT_COUNT_INVALID")
    refreshed = fit_native_timeframe_for_search(
        product=fitted.product,
        config=config,
        acquisition_attempts_exact=attempts,
    )
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


def _reverify_cell_state_error_envelope(
    *,
    cell_plan: Any,
    fitted: FittedNativeSearchTimeframe,
) -> None:
    """Recompute the planner's preprotected envelope before any evaluation."""

    try:
        import pm_production_search_planner as planner

        receipt = cell_plan.cell_receipt
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
    remaining_cell_bytes: int,
    reservation: Any,
) -> _StagedAttemptEvidence:
    if type(ordinal) is not int or ordinal < 0 or not isinstance(value, Mapping):
        _fail("STAGE_ATTEMPT_INPUT_INVALID")
    if remaining_cell_bytes <= 0:
        _fail("CELL_EVIDENCE_BYTE_BOUND_EXCEEDED")
    expected_sha256, expected_size = _canonical_digest_and_size(value)
    if (
        expected_size > MAX_ATTEMPT_EVIDENCE_BYTES
        or expected_size > remaining_cell_bytes
    ):
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
                    if size > MAX_ATTEMPT_EVIDENCE_BYTES or size > remaining_cell_bytes:
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

    refreshed = _reverify_fitted_timeframe(fitted, config)
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
    """Derive CURRENT v4 from the released Horizon<->Winner bijection."""

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
    if (
        historical_body.get("schema_version")
        != (raw_winner.HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4
            if frozen.selection_generation is winner.V16_GENERATION
            else raw_winner.HISTORICAL_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V3)
    ):
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
            "schema_version": (raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5
                if frozen.selection_generation is winner.V16_GENERATION
                else raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V4),
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
        attempt_artifacts=cell_plan.attempt_artifacts(), fitted=refreshed, regime=cell_plan.regime)
    if len(attempts) != MAX_ATTEMPTS or MAX_ATTEMPTS != 300:
        _fail("SIGNAL_DESIGN_COMPLETE_300_ROSTER_REQUIRED")
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
        "candidate_domain_sha256": process.to_dict()["candidate_domain_sha256"],
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
            strategy, _ = _runtime_strategy(attempt)
            surface = _build_full_prefix_strategy_surface(frame=frame, strategy=strategy,
                symbol=cell_plan.symbol, timeframe=cell_plan.timeframe)
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
        if (totals["roster_count"] != 300 or totals["evaluated_attempt_count"]
                + totals["presearch_rejected_count"] != 300):
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


def _execute_current_observed_cell(
    *, process: Any, sink: authority_spool.SymbolAuthoritySink, cell_plan: Any,
    fitted: FittedNativeSearchTimeframe, config: PipelineConfig, _generation: Any,
) -> CurrentObservedCellExecution:
    """Execute one sealed latest-cutoff observed cell through raw Winner.

    The independently fitted native frame is byte-replayed and cross-bound to
    the sealed current cell plan before any strategy evaluation.  The exact current
    Horizon/Raw-Winner binding is then persisted before evaluation begins.
    Infrastructure failures raise and abort the current generation; only a
    precommitted source-boundary shortfall can return without raw production.
    """

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
        )
        normalized_regime = str(cell_plan.regime).upper()
        plan_body, attempts = _verify_attempt_roster(
            plan=cell_plan.horizon_plan,
            attempt_artifacts=cell_plan.attempt_artifacts(),
            fitted=refreshed,
            regime=normalized_regime,
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
        partition_cost_precommitments = _partition_cost_precommitments(
            attempts=attempts,
            fold_plan=fold_plan,
            plan_body=plan_body,
            fitted=refreshed,
            config=config,
            source_observation_cache=source_observation_cache,
        )
        precommitted_at = _now_utc_seconds()
        attempts_frozen_at = _next_observed_utc_second(precommitted_at)
        freeze = (raw_winner.freeze_winner_search_plan_v5 if _generation is winner.V16_GENERATION
                  else raw_winner.freeze_winner_search_plan_v4)
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
            risk_unit_contract=raw_winner.build_precommitted_risk_unit_contract(),
            partition_cost_precommitments=partition_cost_precommitments,
            artifact_namespace=receipt["artifact_namespace"],
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
        total_bytes = 0
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
                total_bytes += outcome_size
                if total_bytes > MAX_CELL_EVIDENCE_BYTES:
                    _fail("CELL_EVIDENCE_BYTE_BOUND_EXCEEDED")
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
            )
            row = _write_staged_attempt(
                stage=stage,
                ordinal=attempt.ordinal,
                value=body,
                remaining_cell_bytes=MAX_CELL_EVIDENCE_BYTES - total_bytes,
                reservation=stage_reservation,
            )
            staged.append(row)
            total_bytes += row.size_bytes
            del body
        if len(staged) != len(attempts) or total_bytes > MAX_CELL_EVIDENCE_BYTES:
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

        produce = (raw_winner.produce_winner_v16_raw_evidence if _generation is winner.V16_GENERATION
                   else raw_winner.produce_winner_v15_raw_evidence)
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

        verified = production_planner.verify_production_cell_search_plan(
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
) -> HistoricalObservedCellExecution:
    """Run one observed historical cell through genuine raw Winner evidence.

    The released Horizon plan and executable attempt bodies are verified before
    a separately timed raw-Winner precommitment is frozen.  All Backtester
    lanes are then completed into bounded immutable staging files before the
    raw builder is allowed to consume the first attempt.
    """

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
    try:
        refreshed = _reverify_fitted_timeframe(fitted, config)
        _reverify_cell_state_error_envelope(cell_plan=cell_plan, fitted=refreshed)
        plan_body, attempts = _verify_attempt_roster(
            plan=horizon_plan,
            attempt_artifacts=attempt_artifacts,
            fitted=refreshed,
            regime=normalized_regime,
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
        partition_cost_precommitments = _partition_cost_precommitments(
            attempts=attempts,
            fold_plan=fold_plan,
            plan_body=plan_body,
            fitted=refreshed,
            config=config,
            source_observation_cache=source_observation_cache,
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
            risk_unit_contract=raw_winner.build_precommitted_risk_unit_contract(),
            partition_cost_precommitments=partition_cost_precommitments,
        )
        raw_ids = _raw_attempt_ids(handle.frozen, attempts)
        latest_boundary = _utc(
            handle.frozen.precommitment["latest_evidence_boundary_utc"],
            "LATEST_EVIDENCE_BOUNDARY_INVALID",
        )
        now = datetime.now(UTC).replace(microsecond=0)
        if now < datetime.fromisoformat(latest_boundary.replace("Z", "+00:00")):
            _fail("EVIDENCE_BOUNDARY_NOT_CLOSED")
        evidence_started = _next_observed_utc_second(attempts_frozen_at)
        source_observation_references = _require_source_observation_references(
            _publish_source_observation_references(
                capture.sink, frozen=handle.frozen, source_observation_cache=source_observation_cache))
        reservation_scope = authority_spool.reserve_transient_publication(
            capture.context
        )
        stage_reservation = reservation_scope.__enter__()
        stage_reservation_scope = reservation_scope
        stage = _stage_directory(capture, handle)
        total_bytes = 0
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
                total_bytes += outcome_size
                if total_bytes > MAX_CELL_EVIDENCE_BYTES:
                    _fail("CELL_EVIDENCE_BYTE_BOUND_EXCEEDED")
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
            )
            row = _write_staged_attempt(
                stage=stage,
                ordinal=attempt.ordinal,
                value=body,
                remaining_cell_bytes=MAX_CELL_EVIDENCE_BYTES - total_bytes,
                reservation=stage_reservation,
            )
            staged.append(row)
            total_bytes += row.size_bytes
            del body
        if len(staged) != len(attempts) or total_bytes > MAX_CELL_EVIDENCE_BYTES:
            _fail("STAGED_ATTEMPT_ROSTER_INCOMPLETE")
        evidence_completed = _now_utc_seconds()

        def evidence_stream() -> Any:
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

        production = capture.complete_observed_cell(
            handle,
            attempt_evidence=evidence_stream(),
            evidence_access_started_at_utc=evidence_started,
            evidence_access_completed_at_utc=evidence_completed,
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
        _close_reserved_stage(
            stage=stage,
            rows=staged,
            reservation=stage_reservation,
            reservation_scope=stage_reservation_scope,
            primary_error=primary_error,
        )
    if result is None:
        _fail("HISTORICAL_CELL_EXECUTION_TERMINAL_STATE_MISSING")
    return result


def execute_transport_historical_observed_cell(**kwargs: Any) -> HistoricalObservedCellExecution:
    """Run the explicit v16 historical capture without changing legacy defaults."""
    capture = kwargs.get("capture")
    if getattr(capture, "_winner_generation", None) is not winner.V16_GENERATION:
        _fail("TRANSPORT_CAPTURE_GENERATION_REQUIRED")
    return execute_historical_observed_cell(**kwargs)


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
        "regime_surface_recomputed_before_search": True,
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
        "maximum_cell_evidence_bytes": MAX_CELL_EVIDENCE_BYTES,
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
