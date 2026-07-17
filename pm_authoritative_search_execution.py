"""Genuine native-prefix search execution for historical production authority.

This module owns the decision-time seam between released Horizon plans and the
raw Winner V11 builder.  It accepts only a byte-reopened activated native
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
import pm_trade_power as trade_power
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner_v11
from pm_core import (
    BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION,
    Backtester,
    FeatureComputer,
    InstrumentSpec,
    PipelineConfig,
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
EXECUTION_CONTRACT_VERSION: Final = "fxpm.authoritative-search-execution.v2"
REGIME_FIT_POLICY_VERSION: Final = "fxpm.historical-regime-fit-policy.v1"
REGIME_FIT_EVIDENCE_VERSION: Final = "fxpm.historical-regime-fit-evidence.v1"
PARTITION_GEOMETRY_VERSION: Final = "fxpm.winner-partition-geometry.v1"
SIGNAL_VECTOR_VERSION: Final = "fxpm.authoritative-search-signal-vector.v1"
PROVENANCE_RECEIPT_VERSION: Final = "fxpm.authoritative-search-provenance.v1"
MAX_PREFIX_ROWS: Final = 10_000_000
MAX_ATTEMPTS: Final = winner_v11.MAX_ATTEMPTS
MAX_FOLDS: Final = winner_v11.MAX_FOLDS
MIN_FOLDS: Final = int(winner_v11.PINNED_SELECTION_POLICY["min_causal_folds"])
MIN_DAYS_PER_FOLD: Final = int(winner_v11.PINNED_SELECTION_POLICY["min_days_per_fold"])
MAX_DAYS_PER_FOLD: Final = winner_v11.MAX_DAYS_PER_FOLD
MIN_PARTITION_SCORED_BARS: Final = 2
MAX_ATTEMPT_EVIDENCE_BYTES: Final = 512 * 1024**2
MAX_CELL_EVIDENCE_BYTES: Final = 8 * 1024**3
MAX_DEPLOYMENT_CONFIG_BYTES: Final = 32 * 1024**2
MAX_BROKER_SPECS_BYTES: Final = 64 * 1024**2
EXECUTION_SPEC_RECEIPT_SCHEMA: Final = "fxpm.execution-instrument-spec-receipt.v1"
TERMINAL_SOURCE_CONTRACT_SCHEMA: Final = (
    "fxpm.terminal-source-non-evaluation-contract.v1"
)
TERMINAL_REGIME_FIT_POLICY_SCHEMA: Final = "fxpm.terminal-regime-fit-policy.v1"
TERMINAL_SOURCE_REASON_CODE: Final = "AUTHENTICATED_NATIVE_PREFIX_SOURCE_UNAVAILABLE"
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
    "position_events",
    "winner_daily_r_surface",
    "trade_power_partition_axis",
    "execution_cost_timing_contract",
    "numerical_execution_cost_attestation",
    "swap_accrual_contract",
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
    base_cost_policy: Mapping[str, Any]
    stress_cost_policy: Mapping[str, Any]
    regime_parameters: Mapping[str, Any]
    train_regime_support: Mapping[str, Any]
    instrument_spec: InstrumentSpec
    execution_spec_receipt: Mapping[str, Any]
    frame_sha256: str
    _frame: pd.DataFrame

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

    Only ``SOURCE_UNAVAILABLE`` yields Book decisions; a native
    ``TECHNICAL_FAILURE`` never reaches this object because
    :func:`fit_terminal_native_timeframe` raises before construction.
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
        minimum_observations=trade_power.PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
    )
    if sample.source_contract is None:
        _fail("TRADE_POWER_SOURCE_CONTRACT_INVALID", *sample.reason_codes)
    return _hex64(
        sample.source_contract.source_contract_sha256,
        "TRADE_POWER_SOURCE_IDENTITY_INVALID",
    )


def _cost_policy_from_seed(
    seed_result: Mapping[str, Any],
    *,
    lane: str,
) -> dict[str, Any]:
    attestation = seed_result.get("numerical_execution_cost_attestation")
    if not isinstance(attestation, Mapping):
        _fail(f"{lane}_COST_ATTESTATION_MISSING")
    try:
        return raw_winner.build_frozen_numerical_execution_cost_policy(
            lane=lane,
            use_spread=attestation.get("use_spread"),
            use_slippage=attestation.get("use_slippage"),
            use_commission=attestation.get("use_commission"),
            spread_mode=attestation.get("spread_mode"),
            commission_inputs_sha256=attestation.get("commission_inputs_sha256"),
            instrument_spec_sha256=attestation.get("instrument_spec_sha256"),
            slippage_pips=attestation.get("slippage_pips_hex"),
            slippage_price=attestation.get("slippage_price_hex"),
        )
    except raw_winner.WinnerRawEvidenceError as exc:
        raise AuthoritativeSearchExecutionError(
            f"{lane}_COST_POLICY_BUILD_FAILED",
            (exc.code,),
        ) from exc


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
    if labels.empty or labels.isna().any():
        _fail("REGIME_SUPPORT_EMPTY")
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
        "first_ts": pd.Timestamp(labels.index[0]).isoformat(),
        "last_ts": pd.Timestamp(labels.index[-1]).isoformat(),
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
) -> tuple[dict[str, Any], str, int]:
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
    return value, hashlib.sha256(raw).hexdigest(), len(raw)


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


def _merged_config_instrument_record(
    config_body: Mapping[str, Any],
    symbol: str,
) -> tuple[dict[str, Any], str]:
    specs = config_body.get("instrument_specs")
    defaults = config_body.get("instrument_spec_defaults")
    if type(specs) is not dict or type(defaults) is not dict:
        _fail("EXECUTION_SPEC_CONFIG_INSTRUMENT_SURFACE_INVALID")

    def resolve(name: str, seen: frozenset[str]) -> dict[str, Any]:
        normalized = _normalized_symbol(name)
        if normalized in seen:
            _fail("EXECUTION_SPEC_CONFIG_INHERITANCE_CYCLE", normalized)
        matches = [
            dict(raw)
            for key, raw in specs.items()
            if type(raw) is dict
            and (
                _normalized_symbol(key) == normalized
                or _normalized_symbol(raw.get("symbol")) == normalized
            )
        ]
        if len(matches) > 1 or (
            not matches and normalized != _normalized_symbol(symbol)
        ):
            _fail(
                "EXECUTION_SPEC_CONFIG_SYMBOL_CARDINALITY_INVALID",
                normalized,
                len(matches),
            )
        row = matches[0] if matches else {}
        inherited: dict[str, Any] = {}
        parent = row.get("inherit")
        if parent:
            inherited = resolve(str(parent), seen | {normalized})
        merged = {**inherited, **dict(defaults)}
        merged.update({key: value for key, value in row.items() if key != "inherit"})
        return merged

    merged = resolve(symbol, frozenset())
    return merged, _sha256(merged)


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
    symbol = str(native.get("symbol") or "").upper()
    instrument = dict(product.instrument_snapshot or {})
    if instrument != dict(product.index["source"].get("instrument_snapshot") or {}):
        _fail("EXECUTION_SPEC_NATIVE_INSTRUMENT_BINDING_INVALID")
    config_raw = str(getattr(config, "_deployment_config_path", "") or "").strip()
    if not config_raw:
        _fail("EXECUTION_SPEC_DEPLOYMENT_CONFIG_UNBOUND")
    config_path = Path(config_raw)
    config_body, config_sha, config_size = _read_bound_json_file(
        config_path,
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
    broker_path = Path(broker_raw)
    if not broker_path.is_absolute():
        broker_path = config_path.resolve(strict=True).parent / broker_path
    broker_body, broker_sha, broker_size = _read_bound_json_file(
        broker_path,
        maximum_bytes=MAX_BROKER_SPECS_BYTES,
        code="EXECUTION_SPEC_BROKER_SPECS",
    )
    broker_key, broker = _matched_symbol_record(
        broker_body,
        symbol,
        code="EXECUTION_SPEC_BROKER_SYMBOL_CARDINALITY_INVALID",
    )
    configured, configured_sha = _merged_config_instrument_record(config_body, symbol)

    if (
        _normalized_symbol(instrument.get("symbol")) != _normalized_symbol(symbol)
        or _normalized_symbol(broker.get("symbol") or broker_key)
        != _normalized_symbol(symbol)
        or str(instrument.get("path")) != str(broker.get("path"))
        or instrument.get("digits") != broker.get("digits")
    ):
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
            _fail("EXECUTION_SPEC_NATIVE_BROKER_STRUCTURAL_MISMATCH", native_name)

    digits = int(instrument["digits"])
    point = _finite_number(instrument["point"], "EXECUTION_SPEC_POINT_INVALID")
    tick_size = _finite_number(
        instrument["trade_tick_size"], "EXECUTION_SPEC_TICK_SIZE_INVALID"
    )
    tick_value = _finite_number(
        instrument["trade_tick_value"], "EXECUTION_SPEC_TICK_VALUE_INVALID"
    )
    _finite_number(broker.get("tick_value"), "EXECUTION_SPEC_BROKER_TICK_VALUE_INVALID")
    contract_size = _finite_number(
        instrument["trade_contract_size"], "EXECUTION_SPEC_CONTRACT_SIZE_INVALID"
    )
    pip_position = digits - 1 if digits in {3, 5} else digits
    pip_size = _finite_number(
        broker.get("pip_size") or (point * 10.0 if digits in {3, 5} else point),
        "EXECUTION_SPEC_PIP_SIZE_INVALID",
    )
    commission = _finite_number(
        configured.get("commission_per_lot"),
        "EXECUTION_SPEC_COMMISSION_INVALID",
        allow_zero=True,
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
        commission_per_lot=commission,
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
            "configured_instrument_record_sha256": configured_sha,
            "native_prefix_index_sha256": product.index_sha256,
            "native_provenance_sha256": native["provenance_sha256"],
            "native_instrument_snapshot_sha256": _sha256(instrument),
            "tick_value_source": "ACTIVATED_ACQUISITION_INSTRUMENT_SNAPSHOT",
            "commission_source": "FROZEN_DEPLOYMENT_CONFIG_INSTRUMENT_SURFACE",
            "volume_source": "FROZEN_BROKER_SPECS_RECORD",
            "instrument_spec": spec_body,
            "instrument_spec_sha256": Backtester._cost_attestation_json_sha256(
                {
                    "schema_version": "fxpm-backtester-instrument-spec-v1",
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
    symbol = str(native.get("symbol") or "").upper()
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
    stress_mode = str(config.oos_evidence_execution_stress_mode).strip().lower()
    if stress_mode not in {"stress_p90", "stress_p95", "stress_max"}:
        _fail("STRESS_SPREAD_POLICY_INVALID")

    base_backtester = Backtester(config)
    stress_backtester = Backtester(replace(config, backtest_spread_mode=stress_mode))
    try:
        base_seed_result = base_backtester.build_evidence_source_seed_result(
            frame,
            symbol,
            spec=instrument_spec,
            timeframe=timeframe,
        )
        stress_seed_result = stress_backtester.build_evidence_source_seed_result(
            frame,
            symbol,
            spec=instrument_spec,
            timeframe=timeframe,
        )
        source_seed = raw_winner.build_trade_source_contract_from_backtester_result(
            base_seed_result
        )
    except Exception as exc:
        raise AuthoritativeSearchExecutionError(
            "SOURCE_CONTRACT_SEED_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    source_contract_sha256 = _source_identity(source_seed)
    base_policy = _cost_policy_from_seed(base_seed_result, lane="BASE")
    stress_policy = _cost_policy_from_seed(stress_seed_result, lane="STRESS")
    if _sha256(base_policy) == _sha256(stress_policy):
        _fail("BASE_STRESS_COST_POLICY_NOT_DISTINCT")

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
        fit_cutoff_utc=native["prefix_end_exclusive_utc"],
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
        source_contract_seed=json.loads(_canonical_bytes(source_seed).decode("ascii")),
        base_cost_policy=json.loads(_canonical_bytes(base_policy).decode("ascii")),
        stress_cost_policy=json.loads(_canonical_bytes(stress_policy).decode("ascii")),
        regime_parameters=json.loads(_canonical_bytes(parameters).decode("ascii")),
        train_regime_support=json.loads(_canonical_bytes(support).decode("ascii")),
        instrument_spec=replace(instrument_spec),
        execution_spec_receipt=json.loads(
            _canonical_bytes(execution_spec_receipt).decode("ascii")
        ),
        frame_sha256=digest,
        _frame=combined,
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

    ``SOURCE_UNAVAILABLE`` yields a ``TERMINAL_UNAVAILABLE`` source-group
    decision plus a ``REGIME_NOT_APPLICABLE`` regime-fit decision, both bound
    to a freshly derived non-evaluation contract (never caller hashes, never
    the AVAILABLE activation fields).  A native ``TECHNICAL_FAILURE`` is
    reverified and then raised typed: infrastructure failure aborts the
    generation and never becomes a market/no-trade state (seq1386/seq1468).
    """

    if type(product) is not native_prefix.NativePrefixProduct:
        _fail("NATIVE_PREFIX_PRODUCT_INVALID")
    if getattr(product, "_seal", None) is not native_prefix._PRODUCT_SEAL:
        _fail("NATIVE_PREFIX_PRODUCT_UNSEALED")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    state = product.source_state
    if state not in {native_prefix.SOURCE_UNAVAILABLE, native_prefix.TECHNICAL_FAILURE}:
        _fail("TERMINAL_NATIVE_PRODUCT_NOT_TERMINAL", state)
    native = dict(product.native_prefix)
    if native.get("source_state") != state:
        _fail("TERMINAL_NATIVE_STATE_MISMATCH")
    symbol = str(_terminal_field(native, "symbol", "TERMINAL_SYMBOL_INVALID")).upper()
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

    # SOURCE_UNAVAILABLE: derive the exact non-evaluation contract from the
    # reverified terminal product bytes.
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
        "source_state": native_prefix.SOURCE_UNAVAILABLE,
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
    contract_sha = _sha256(contract)

    source = book.create_historical_source_group_decision(
        symbol=symbol,
        timeframe=timeframe,
        availability=book.TERMINAL_UNAVAILABLE,
        reason_code=TERMINAL_SOURCE_REASON_CODE,
        source_contract_sha256=contract_sha,
        source_group_evidence_sha256=product.index_sha256,
        acquisition_attempts_exact=1,
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
        "fit_state": book.REGIME_NOT_APPLICABLE,
        "detector_execution": "FORBIDDEN",
    }
    fit = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=book.REGIME_NOT_APPLICABLE,
        reason_code=TERMINAL_SOURCE_REASON_CODE,
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
    if (
        type(policy) is not dict
        or policy.get("evaluator_release_sha256")
        != AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        or policy.get("cost_contract_sha256") != _sha256(fitted.base_cost_policy)
        or policy.get("cost_stress_contract_sha256")
        != _sha256(fitted.stress_cost_policy)
    ):
        _fail("HORIZON_COST_POLICY_BINDING_INVALID")

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
        candidate_matches = [
            item
            for item in candidates
            if item.get("lookback_kind") == horizon_process.get("lookback_kind")
            and item.get("lookback_value") == horizon_process.get("lookback_value")
            and _utc(item.get("realized_start_utc"), "CANDIDATE_START_INVALID")
            == _utc(horizon_process.get("fit_start_utc"), "ATTEMPT_FIT_START_INVALID")
        ]
        if len(candidate_matches) != 1:
            _fail("ATTEMPT_HORIZON_SEMANTIC_MAPPING_INVALID", ordinal)
        candidate = candidate_matches[0]
        fit_start = pd.Timestamp(
            _utc(horizon_process.get("fit_start_utc"), "ATTEMPT_FIT_START_INVALID")
        )
        fit_end = pd.Timestamp(
            _utc(horizon_process.get("fit_end_utc"), "ATTEMPT_FIT_END_INVALID")
        )
        frame_opens = pd.DatetimeIndex(
            pd.to_datetime(fitted._frame.index, utc=True, errors="coerce")
        )
        source_bar_count = int(
            ((frame_opens >= fit_start) & (frame_opens < fit_end)).sum()
        )
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
    minimum_required = 2 * minimum_warmup + 2 * MIN_PARTITION_SCORED_BARS
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
) -> None:
    try:
        entry, intrabar, meta = backtester._build_half_spread_arrays(features, spec)
        slippage_price = (
            spec.pips_to_price(backtester.config.slippage_pips)
            if backtester.config.use_slippage
            else 0.0
        )
        rebuilt = backtester._numerical_execution_cost_attestation(
            entry_half_spread=entry,
            intrabar_half_spread=intrabar,
            spread_meta=meta,
            slippage_price=slippage_price,
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
    body = {name: result.get(name) for name in _RAW_LANE_KEYS}
    if (
        type(body["position_events"]) is not list
        or type(body["winner_daily_r_surface"]) is not dict
        or body["winner_daily_r_surface"].get("status") != "AVAILABLE"
        or type(body["trade_power_partition_axis"]) is not dict
        or type(body["execution_cost_timing_contract"]) is not dict
        or type(body["numerical_execution_cost_attestation"]) is not dict
        or type(body["swap_accrual_contract"]) is not dict
    ):
        _fail("BACKTEST_DECISION_RELEVANT_RESULT_INCOMPLETE", role)
    return json.loads(_canonical_bytes(body).decode("ascii"))


def _run_base_partition(
    *,
    partition: _PartitionSlice,
    strategy: Any,
    symbol: str,
    timeframe: str,
    regime: str,
    config: PipelineConfig,
    spec: InstrumentSpec,
    expected_source_contract_sha256: str,
) -> tuple[dict[str, Any], pd.DataFrame, pd.Series, Backtester]:
    try:
        features = FeatureComputer.compute_for_strategy(
            partition.frame,
            strategy,
            symbol=symbol,
            timeframe=timeframe,
        )
        if not features.index.equals(partition.frame.index):
            _fail("FEATURE_AXIS_CHANGED", partition.role)
        raw_signals = strategy.generate_signals(features, symbol)
        if not isinstance(raw_signals, pd.Series) or not raw_signals.index.equals(
            features.index
        ):
            _fail("STRATEGY_SIGNAL_AXIS_INVALID", partition.role)
        numeric = pd.to_numeric(raw_signals, errors="coerce")
        if numeric.isna().any() or bool((~numeric.isin([-1, 0, 1])).any()):
            _fail("STRATEGY_SIGNAL_VALUES_INVALID", partition.role)
        labels = features["REGIME_LIVE"]
        if labels.isna().iloc[partition.warmup_bars :].any():
            _fail("REGIME_SIGNAL_SURFACE_MISSING", partition.role)
        signals = numeric.astype(np.int32).where(labels == regime, 0).astype(np.int32)
        backtester = Backtester(config)
        result = backtester.run(
            features,
            signals,
            symbol,
            strategy,
            spec=spec,
            timeframe=timeframe,
            warmup_bars=partition.warmup_bars,
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
        spec=spec,
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
    stress_method_receipt: Mapping[str, Any],
    final_signals: pd.Series,
) -> dict[str, dict[str, Any]]:
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
        PROVENANCE_RECEIPT_VERSION,
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
        PROVENANCE_RECEIPT_VERSION,
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
        PROVENANCE_RECEIPT_VERSION,
        {
            **common,
            "receipt_role": "anchor_trial_ledger",
            "trial_registry_sha256": plan_body["trial_registry_sha256"],
            "attempt_roster_sha256": plan_body["attempt_roster_sha256"],
            "attempted_trials_exact": len(plan_body["attempts"]),
        },
    )
    selected_trial = _receipt(
        PROVENANCE_RECEIPT_VERSION,
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
        PROVENANCE_RECEIPT_VERSION,
        {
            **common,
            "receipt_role": "anchor_refit_receipt",
            "refit_recipe_sha256": artifacts.refit_recipe_sha256,
            "fit_start_utc": fold["fit_start_utc"],
            "fit_end_exclusive_utc": fold["fit_end_exclusive_utc"],
            "post_selection_search": False,
        },
    )
    if not isinstance(final_signals, pd.Series) or not len(final_signals):
        _fail("FINAL_SIGNAL_VECTOR_INVALID", attempt.ordinal, fold_ordinal)
    signal_numeric = pd.to_numeric(final_signals, errors="coerce")
    if signal_numeric.isna().any() or bool((~signal_numeric.isin([-1, 0, 1])).any()):
        _fail("FINAL_SIGNAL_VECTOR_INVALID", attempt.ordinal, fold_ordinal)
    signal_values = signal_numeric.astype(np.int32).tolist()
    signal_sha256 = hashlib.sha256(
        np.asarray(signal_values, dtype="<i4").tobytes(order="C")
    ).hexdigest()
    if signal_sha256 != stress_method_receipt.get("signal_vector_sha256"):
        _fail("FINAL_SIGNAL_VECTOR_STRESS_MISMATCH", attempt.ordinal, fold_ordinal)
    signal_vector_receipt = _receipt(
        SIGNAL_VECTOR_VERSION,
        {
            "encoding": "little-endian-signed-int32",
            "observation_count": len(signal_values),
            "values": signal_values,
            "signal_vector_sha256": signal_sha256,
        },
    )
    replay_receipt = _receipt(
        PROVENANCE_RECEIPT_VERSION,
        {
            **common,
            "receipt_role": "attempt_replay_receipt",
            "lane_sha256": lane_hashes,
            "stress_method_receipt_sha256": stress_method_receipt["receipt_sha256"],
            # Persist the genuine Backtester-owned method receipt itself, not
            # only a caller-asserted digest.  The raw Winner seam reconstructs
            # its self-hash and binds its lane attestations/position rosters.
            "stress_method_receipt": json.loads(
                _canonical_bytes(dict(stress_method_receipt)).decode("ascii")
            ),
            "signal_vector_receipt": signal_vector_receipt,
            "position_events_are_genuine_backtester_v2": True,
        },
    )
    source_decision = fitted.source_group_decision.to_dict()
    oos_source = _receipt(
        PROVENANCE_RECEIPT_VERSION,
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
    *,
    attempt: _VerifiedAttempt,
    raw_attempt_id: str,
    plan_body: Mapping[str, Any],
    plan_receipt_sha256: str,
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> dict[str, Any]:
    strategy, _parameters = _runtime_strategy(attempt)
    frame = fitted._frame
    symbol = plan_body["cell"]["symbol"]
    timeframe = plan_body["cell"]["timeframe"]
    regime = plan_body["cell"]["regime"]
    source_sha = fitted.source_group_decision.to_dict()["source_contract_sha256"]
    stress_mode = str(config.oos_evidence_execution_stress_mode).strip().lower()
    geometry = plan_body.get("derived_dependency_geometry")
    if type(geometry) is not dict:
        _fail("DEPENDENCY_GEOMETRY_MISSING")
    dependency_warmup_bars = geometry.get("warmup_bars")
    if type(dependency_warmup_bars) is not int or dependency_warmup_bars < 0:
        _fail("DEPENDENCY_WARMUP_INVALID")
    folds: list[dict[str, Any]] = []
    for fold_ordinal, fold in enumerate(_causal_folds(attempt)):
        train, tuning, final = _fold_partitions(
            frame=frame,
            fold=fold,
            attempt=attempt,
            dependency_warmup_bars=dependency_warmup_bars,
        )
        train_result, _train_features, _train_signals, _train_backtester = (
            _run_base_partition(
                partition=train,
                strategy=strategy,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                config=config,
                spec=fitted.instrument_spec,
                expected_source_contract_sha256=source_sha,
            )
        )
        tuning_result, _tuning_features, _tuning_signals, _tuning_backtester = (
            _run_base_partition(
                partition=tuning,
                strategy=strategy,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                config=config,
                spec=fitted.instrument_spec,
                expected_source_contract_sha256=source_sha,
            )
        )
        final_result, final_features, final_signals, final_backtester = (
            _run_base_partition(
                partition=final,
                strategy=strategy,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                config=config,
                spec=fitted.instrument_spec,
                expected_source_contract_sha256=source_sha,
            )
        )
        try:
            stress_result = final_backtester.replay_frozen_path_cost_stress(
                final_features,
                final_signals,
                symbol,
                strategy,
                final_result,
                stress_mode=stress_mode,
                spec=fitted.instrument_spec,
                timeframe=timeframe,
                warmup_bars=final.warmup_bars,
            )
        except Exception as exc:
            raise AuthoritativeSearchExecutionError(
                "FROZEN_PATH_COST_STRESS_FAILED",
                (str(attempt.ordinal), str(fold_ordinal), type(exc).__name__, str(exc)),
            ) from exc
        for result, producing_backtester, producing_features in (
            (train_result, _train_backtester, _train_features),
            (tuning_result, _tuning_backtester, _tuning_features),
            (final_result, final_backtester, final_features),
        ):
            _verify_result_cost_attestation(
                backtester=producing_backtester,
                result=result,
                features=producing_features,
                spec=fitted.instrument_spec,
            )
        stress_relevant = _decision_relevant_result(
            stress_result,
            "final_lockbox_stress",
        )
        lanes = {
            "optimizer_train_base": _decision_relevant_result(
                train_result, "optimizer_train"
            ),
            "tuning_validation_base": _decision_relevant_result(
                tuning_result, "tuning_validation"
            ),
            "final_lockbox_base": _decision_relevant_result(
                final_result, "final_lockbox"
            ),
            "final_lockbox_stress": stress_relevant,
        }
        provenance = _fold_provenance(
            plan_body=plan_body,
            plan_receipt_sha256=plan_receipt_sha256,
            attempt=attempt,
            fold=fold,
            fold_ordinal=fold_ordinal,
            partitions=(train, tuning, final),
            lanes=lanes,
            fitted=fitted,
            stress_method_receipt=stress_result["cost_stress_frozen_path_receipt"],
            final_signals=final_signals,
        )
        folds.append(
            {
                "fold_ordinal": fold_ordinal,
                "provenance": provenance,
                "lanes": lanes,
            }
        )
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
        (dict(fitted.base_cost_policy), dict(refreshed.base_cost_policy)),
        (dict(fitted.stress_cost_policy), dict(refreshed.stress_cost_policy)),
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
    return refreshed


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
        evidence={
            "horizon_attempt_id": attempt.plan_attempt["attempt_id"],
            "candidate_id": attempt.candidate["candidate_id"],
            "trial_id": attempt.trial["trial_id"],
            "decision_was_pre_search": True,
        },
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
) -> _StagedAttemptEvidence:
    if type(ordinal) is not int or ordinal < 0 or not isinstance(value, Mapping):
        _fail("STAGE_ATTEMPT_INPUT_INVALID")
    if remaining_cell_bytes <= 0:
        _fail("CELL_EVIDENCE_BYTE_BOUND_EXCEEDED")
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
        or binding.get("train_regime_support_receipt_sha256") != support_receipt
        or binding.get("plan_receipt_sha256") != _sha256(plan_body)
    ):
        _fail("CURRENT_FITTED_PLAN_BINDING_MISMATCH")
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
    """Derive CURRENT v2 from the released Horizon<->Winner bijection."""

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
        != raw_winner.HORIZON_WINNER_CROSS_BINDING_SCHEMA
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
            "schema_version": raw_winner.HORIZON_WINNER_CROSS_BINDING_SCHEMA_V2,
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


def execute_current_observed_cell(
    *,
    process: Any,
    sink: authority_spool.SymbolAuthoritySink,
    cell_plan: Any,
    fitted: FittedNativeSearchTimeframe,
    config: PipelineConfig,
) -> CurrentObservedCellExecution:
    """Execute one sealed latest-cutoff observed cell through raw Winner.

    The independently fitted native frame is byte-replayed and cross-bound to
    the sealed current cell plan before any strategy evaluation.  The exact v2
    Horizon/Raw-Winner binding is then persisted before evaluation begins.
    Infrastructure failures raise and abort the current generation; only a
    precommitted source-boundary shortfall can return without raw production.
    """

    stage: Path | None = None
    staged: list[_StagedAttemptEvidence] = []
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
        minimum_folds = int(winner_v11.PINNED_SELECTION_POLICY["min_causal_folds"])
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
        precommitted_at = _now_utc_seconds()
        attempts_frozen_at = _next_observed_utc_second(precommitted_at)
        frozen = raw_winner.freeze_winner_search_plan_v3(
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
            source_contract_seed=refreshed.source_contract_seed,
            base_cost_contract=refreshed.base_cost_policy,
            stress_cost_contract=refreshed.stress_cost_policy,
            risk_unit_contract=raw_winner.build_precommitted_risk_unit_contract(),
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
            )
            row = _write_staged_attempt(
                stage=stage,
                ordinal=attempt.ordinal,
                value=body,
                remaining_cell_bytes=MAX_CELL_EVIDENCE_BYTES - total_bytes,
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

        production = raw_winner.produce_winner_v11_raw_evidence(
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
        try:
            _cleanup_stage(stage, staged)
        except Exception as cleanup_exc:
            if primary_error is not None:
                raise cleanup_exc from primary_error
            raise


def execute_historical_observed_cell(
    *,
    capture: (
        historical_adapter.HistoricalSymbolSearchCapture
        | historical_adapter.BoundedRawWinnerCanaryCapture
    ),
    fitted: FittedNativeSearchTimeframe,
    regime: str,
    horizon_plan: Any,
    attempt_artifacts: Sequence[Mapping[str, Any]],
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
    }:
        _fail("HISTORICAL_CAPTURE_INVALID")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    if (
        type(capture)
        is historical_adapter.BoundedRawWinnerCanaryCapture
        and (
            not historical_adapter._verify_bounded_raw_winner_canary_capture(
                capture
            )
            or fitted is not capture.verified_fitted_timeframe
            or config is not capture.verified_pipeline_config
        )
    ):
        _fail("CANARY_EXECUTION_IDENTITY_SUBSTITUTION")
    normalized_regime = str(regime or "").strip().upper()
    if normalized_regime not in book.REGIME_ORDER:
        _fail("REGIME_INVALID", normalized_regime)
    source = fitted.source_group_decision.to_dict()
    symbol = str(source.get("symbol") or "")
    timeframe = str(source.get("timeframe") or "")
    attempted_exact = (
        len(attempt_artifacts) if isinstance(attempt_artifacts, Sequence) else 0
    )
    handle: historical_adapter.FrozenHistoricalCell | None = None
    stage: Path | None = None
    staged: list[_StagedAttemptEvidence] = []
    result: HistoricalObservedCellExecution | None = None
    primary_error: BaseException | None = None
    try:
        refreshed = _reverify_fitted_timeframe(fitted, config)
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
        minimum_folds = int(winner_v11.PINNED_SELECTION_POLICY["min_causal_folds"])
        if len(fold_plan) < minimum_folds:
            _fail("WINNER_FOLD_PLAN_BELOW_EVIDENCE_FLOOR")
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
            source_contract_seed=refreshed.source_contract_seed,
            base_cost_contract=refreshed.base_cost_policy,
            stress_cost_contract=refreshed.stress_cost_policy,
            risk_unit_contract=raw_winner.build_precommitted_risk_unit_contract(),
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
            )
            row = _write_staged_attempt(
                stage=stage,
                ordinal=attempt.ordinal,
                value=body,
                remaining_cell_bytes=MAX_CELL_EVIDENCE_BYTES - total_bytes,
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
        capture.record_raw_evidence_failure(handle, exc)
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
            capture.record_raw_evidence_failure(handle, failure)
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
            _cleanup_stage(stage, staged)
        except Exception as cleanup_exc:
            if primary_error is not None:
                raise cleanup_exc from primary_error
            raise
    if result is None:
        _fail("HISTORICAL_CELL_EXECUTION_TERMINAL_STATE_MISSING")
    return result


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

    def _tag(key: bytes, schema: str, body_json: bytes, bindings: Mapping[str, Any]) -> str:
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
            "spool_entry_sha256": hashlib.sha256(
                value._spool_entry_json
            ).hexdigest(),
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
            or source_body.get("generation_context_sha256")
            != context.context_sha256
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
            source_session._frozen_pipeline_config.verify_current(context)
            refreshed = _reverify_fitted_timeframe(
                source_session._fitted, source_session._pipeline_config
            )
        except AuthoritativeSearchExecutionError:
            raise
        except Exception as exc:
            raise AuthoritativeSearchExecutionError(
                "CANARY_SOURCE_SESSION_REVERIFY_FAILED",
                (type(exc).__name__, str(exc)),
            ) from exc
        if (
            refreshed.frame_sha256 != source_session._fitted.frame_sha256
            or source_body.get("fitted_frame_sha256") != refreshed.frame_sha256
            or source_body.get("native_prefix_index_sha256")
            != source_session._product.index_sha256
            or source_body.get("train_regime_support_receipt_sha256")
            != refreshed.train_regime_support.get("receipt_sha256")
        ):
            _fail("CANARY_SOURCE_SESSION_REPLAY_MISMATCH")
        return request_body, source_body, state

    def _verify_plan_permit(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: Any,
        process: Any,
        source_session: Any,
        plan_permit: Any,
        consume: bool,
    ) -> tuple[dict[str, Any], BoundedRawWinnerCanarySourceSession, PipelineConfig, Any]:
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
            or permit_body.get("generation_context_sha256")
            != context.context_sha256
            or state.get("plan_permit_receipt_sha256")
            != plan_permit.receipt_sha256
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
    ) -> tuple[dict[str, Any], BoundedRawWinnerCanarySourceSession, PipelineConfig, Any]:
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
            state.get("state") not in {
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
    ) -> tuple[dict[str, Any], authority_spool.SymbolAuthoritySink, BoundedRawWinnerCanarySourceSession, PipelineConfig, Any]:
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
            or allowed_state not in {
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
    ) -> tuple[dict[str, Any], authority_spool.SymbolAuthoritySink, BoundedRawWinnerCanarySourceSession, PipelineConfig, Any]:
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

        request_body = historical.verify_bounded_raw_winner_canary_request(
            context, request
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

            if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
                _fail("CANARY_FROZEN_PIPELINE_CONFIG_REQUIRED")
            frozen_pipeline_config.verify_current(context)
            config = frozen_pipeline_config.materialize(context)
            source_accessed_at_utc = _now_utc_seconds()
            if (
                source_accessed_at_utc
                < _utc(
                    request_body["earliest_source_accessed_at_utc"],
                    "CANARY_SOURCE_BOUNDARY_INVALID",
                )
                or source_accessed_at_utc
                > _utc(
                    request_body["causal_evidence_access_deadline_utc"],
                    "CANARY_CAUSAL_DEADLINE_INVALID",
                )
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
            if (
                native.get("source_accessed_at_utc") != source_accessed_at_utc
                or native.get("symbol") != request_body["symbol"]
                or native.get("timeframe") != request_body["timeframe"]
                or native.get("prefix_end_exclusive_utc")
                != request_body["anchor"]["selection_cutoff_utc"]
                or native.get("activation_id") != request_body["activation_id"]
                or native.get("activation_manifest_sha256")
                != request_body["activation_manifest_sha256"]
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
            source_entry = request.symbol_sink().put_json(
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
                    "frozen_pipeline_config_identity": id(
                        frozen_pipeline_config
                    ),
                    "spool_entry_sha256": hashlib.sha256(
                        source_entry_json
                    ).hexdigest(),
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
            unsigned_capture = _BoundedRawWinnerCanaryCapturePermit(
                capture_json, ""
            )
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
                horizon_plan=plan.horizon_plan,
                attempt_artifacts=plan.attempt_artifacts(),
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
        "pm_winner_selection": _file_sha256(Path(winner_v11.__file__)),
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
        "cost_stress": BACKTEST_FROZEN_PATH_COST_STRESS_SCHEMA_VERSION,
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
    "FittedNativeSearchTimeframe",
    "HistoricalObservedCellExecution",
    "TERMINAL_REGIME_FIT_POLICY_SCHEMA",
    "TERMINAL_SOURCE_CONTRACT_SCHEMA",
    "TerminalNativeTimeframe",
    "authoritative_search_execution_contract",
    "execute_current_observed_cell",
    "execute_bounded_raw_winner_canary",
    "execute_historical_observed_cell",
    "fit_native_timeframe_for_search",
    "fit_terminal_native_timeframe",
    "reverify_fitted_native_timeframe_for_search",
]
