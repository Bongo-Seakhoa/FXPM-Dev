"""Canonical search artifacts for raw current-refit replay."""

from __future__ import annotations

import hashlib
import inspect
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from pm_core import FeatureComputer
from pm_authority_spool import SymbolAuthoritySink
from pm_current_refit_parameters import (
    CurrentRefitParameterCodecError,
    canonical_parameter_bytes,
    canonicalize_parameter_mapping,
    decode_parameter_mapping,
    parameter_values_equal,
)
from pm_strategies import StrategyRegistry


WINNER_PROCESS_SCHEMA = "fxpm.current-refit-winner-process.v1"
HORIZON_PROCESS_SCHEMA = "fxpm.current-refit-horizon-process.v2"
TRIAL_DEFINITION_SCHEMA = "fxpm.current-refit-trial-definition.v1"
REFIT_RECIPE_SCHEMA = "fxpm.current-refit-recipe.v1"
SPECIALIST_OUTPUT_SCHEMA = "fxpm.current-refit-specialist.v1"

_TIMEFRAMES = frozenset({"M5", "M15", "M30", "H1", "H4", "D1"})
_REGIMES = frozenset({"TREND", "RANGE", "BREAKOUT", "CHOP"})
_LOOKBACK_KINDS = frozenset(
    {"NATIVE_OBSERVATIONS", "ACTIVE_SESSIONS", "CALENDAR_INTERVAL"}
)
_BAR_COLUMNS = (
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
)
_HEX = frozenset("0123456789abcdef")
_MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
_WINNER_PROCESS_FIELDS = frozenset(
    {
        "schema_version",
        "process_candidate_id",
        "strategy_id",
        "strategy_family",
        "strategy_module",
        "strategy_class",
        "strategy_code_sha256",
        "entry_rule_identity_sha256",
        "exit_rule_identity_sha256",
        "regime_conditioning",
    }
)
_HORIZON_PROCESS_FIELDS = frozenset(
    {
        "schema_version",
        "symbol",
        "timeframe",
        "lookback_kind",
        "lookback_value",
        "source_bar_count",
        "fit_start_utc",
        "fit_end_utc",
        "latest_closed_bar_utc",
        "source_snapshot_sha256",
    }
)
_TRIAL_DEFINITION_FIELDS = frozenset(
    {
        "schema_version",
        "strategy_id",
        "strategy_family",
        "process_sha256",
        "horizon_process_sha256",
        "feature_contract_sha256",
        "indicator_contract_sha256",
        "parameter_count",
        "parameters",
    }
)
_REFIT_RECIPE_FIELDS = frozenset(
    {
        "schema_version",
        "strategy_id",
        "winner_process_sha256",
        "winner_horizon_process_sha256",
        "winner_trial_definition_sha256",
        "refit_mode",
        "parameter_source",
        "source_validation",
        "fit_window_rule",
        "deterministic_fit_procedure_id",
        "seed",
        "version_pins",
        "data_requirements",
        "attempts",
        "post_selection_search",
        "output_schema_version",
    }
)


class CurrentRefitArtifactError(ValueError):
    """Typed fail-closed producer error."""


@dataclass(frozen=True)
class CurrentRefitSearchArtifacts:
    process_candidate_id: str
    winner_process: dict[str, Any]
    winner_process_sha256: str
    horizon_process: dict[str, Any]
    horizon_process_sha256: str
    trial_definition: dict[str, Any]
    trial_definition_sha256: str
    refit_recipe: dict[str, Any]
    refit_recipe_sha256: str


@dataclass(frozen=True)
class CurrentRefitSpoolWrite:
    artifacts: CurrentRefitSearchArtifacts
    entries: dict[str, dict[str, Any]]


def _fail(code: str) -> None:
    raise CurrentRefitArtifactError(code)


def canonical_artifact_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise CurrentRefitArtifactError("ARTIFACT_NOT_CANONICAL_JSON") from exc
    if not encoded.isascii():
        _fail("ARTIFACT_NON_ASCII")
    # Winner V11 uses ensure_ascii=True.  Once the ASCII law above holds, its
    # serializer and the current-refit serializer must be byte-identical.
    escaped = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if escaped != encoded:
        _fail("ARTIFACT_ASCII_CANONICALIZATION_MISMATCH")
    return encoded


def canonical_artifact_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_artifact_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_file_sha256(value: Any) -> str:
    try:
        path = Path(inspect.getsourcefile(value) or "").resolve(strict=True)
    except (OSError, TypeError, ValueError) as exc:
        raise CurrentRefitArtifactError("SOURCE_FILE_UNRESOLVED") from exc
    return _file_sha256(path)


def _callable_sha256(value: Any) -> str:
    try:
        source = inspect.getsource(value).replace("\r\n", "\n").encode("utf-8")
    except (OSError, TypeError) as exc:
        raise CurrentRefitArtifactError("CALLABLE_SOURCE_UNRESOLVED") from exc
    return hashlib.sha256(source).hexdigest()


def _sha256(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        _fail(code)
    return value


def _identity(value: Any, code: str, *, maximum: int = 128) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail(code)
    if (
        not value.isascii()
        or len(value.encode("ascii")) > maximum
        or any(ord(char) < 32 for char in value)
    ):
        _fail(code)
    return value


def _cell_identity(symbol: Any, timeframe: Any, regime: Any) -> tuple[str, str, str]:
    normalized_symbol = _identity(symbol, "SYMBOL_INVALID").upper()
    normalized_timeframe = _identity(timeframe, "TIMEFRAME_INVALID").upper()
    normalized_regime = _identity(regime, "REGIME_INVALID").upper()
    if normalized_timeframe not in _TIMEFRAMES:
        _fail("TIMEFRAME_INVALID")
    if normalized_regime not in _REGIMES:
        _fail("REGIME_INVALID")
    return normalized_symbol, normalized_timeframe, normalized_regime


def _utc(value: Any, code: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CurrentRefitArtifactError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalized_parameters(
    strategy_id: str, parameters: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        supplied = canonicalize_parameter_mapping(parameters)
    except CurrentRefitParameterCodecError as exc:
        raise CurrentRefitArtifactError("PARAMETERS_INVALID") from exc
    base = StrategyRegistry.get(strategy_id)
    try:
        defaults = canonicalize_parameter_mapping(base.get_default_params())
    except CurrentRefitParameterCodecError as exc:
        raise CurrentRefitArtifactError("STRATEGY_DEFAULTS_INVALID") from exc
    grid = base.get_param_grid()
    if not set(supplied) <= (set(defaults) | set(grid)):
        _fail("PARAMETER_NAME_OUTSIDE_PROCESS")
    try:
        runtime = decode_parameter_mapping(supplied, base.get_default_params())
        fitted = StrategyRegistry.get(strategy_id, **runtime)
        normalized = canonicalize_parameter_mapping(fitted.get_params())
    except Exception as exc:
        raise CurrentRefitArtifactError("STRATEGY_PARAMETERS_INVALID") from exc
    if canonical_parameter_bytes(normalized) != canonical_parameter_bytes(supplied):
        _fail("PARAMETERS_NOT_FULL_CANONICAL_SET")
    for name, choices in grid.items():
        if name not in normalized:
            continue
        selected = normalized[name]
        if not any(parameter_values_equal(selected, choice) for choice in choices):
            _fail("PARAMETER_VALUE_OUTSIDE_PROCESS")
    for name, expected in defaults.items():
        if name not in grid and not parameter_values_equal(
            normalized.get(name), expected
        ):
            _fail("PARAMETER_FIXED_DEFAULT_MISMATCH")
    canonical_artifact_bytes(normalized)
    return normalized


def _candidate_id(
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    parameters: Mapping[str, Any],
    horizon_process_sha256: str,
) -> str:
    horizon_sha256 = _sha256(
        horizon_process_sha256, "HORIZON_PROCESS_SHA256_INVALID"
    )
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "strategy_name": strategy_id,
        "parameters": dict(parameters),
        "horizon_process_sha256": horizon_sha256,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _feature_contract_sha256(strategy: Any) -> str:
    return canonical_artifact_sha256(
        {
            "schema_version": "fxpm.current-refit-feature-contract.v1",
            "required_features": sorted(strategy.get_feature_request()),
            "minimum_bars": int(strategy.MIN_BARS),
        }
    )


def _strategy(strategy_id: Any) -> tuple[str, Any]:
    name = _identity(strategy_id, "STRATEGY_ID_INVALID", maximum=256)
    if name not in StrategyRegistry.list_all():
        _fail("STRATEGY_ID_UNKNOWN")
    strategy = StrategyRegistry.get(name)
    if type(strategy).__module__ != "pm_strategies":
        _fail("STRATEGY_MODULE_INVALID")
    return name, strategy


def build_winner_process_artifact_v2(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    parameters: Mapping[str, Any],
    horizon_process_sha256: str,
) -> tuple[dict[str, Any], str]:
    """Build the exact executable process body used by raw refit replay."""

    symbol, timeframe, regime = _cell_identity(symbol, timeframe, regime)
    name, strategy = _strategy(strategy_id)
    normalized = _normalized_parameters(name, parameters)
    body = {
        "schema_version": WINNER_PROCESS_SCHEMA,
        "process_candidate_id": _candidate_id(
            symbol,
            timeframe,
            regime,
            name,
            normalized,
            horizon_process_sha256,
        ),
        "strategy_id": name,
        "strategy_family": strategy.category.value,
        "strategy_module": "pm_strategies",
        "strategy_class": type(strategy).__name__,
        "strategy_code_sha256": _source_file_sha256(type(strategy)),
        "entry_rule_identity_sha256": _callable_sha256(type(strategy).generate_signals),
        "exit_rule_identity_sha256": _callable_sha256(type(strategy).calculate_stops),
        "regime_conditioning": "EXACT_SYMBOL_TIMEFRAME_REGIME_CELL",
    }
    return body, canonical_artifact_sha256(body)


def build_horizon_process_artifact_v2(
    *,
    symbol: str,
    timeframe: str,
    lookback_kind: str,
    lookback_value: int,
    fit_start_utc: str,
    fit_end_utc: str,
    latest_closed_bar_utc: str,
    source_snapshot_sha256: str,
    source_bar_count: int,
) -> tuple[dict[str, Any], str]:
    """Build the selected native-prefix horizon bound to one source snapshot."""

    symbol, timeframe, _ = _cell_identity(symbol, timeframe, "TREND")
    if type(source_bar_count) is not int or not 2 <= source_bar_count <= 10_000_000:
        _fail("SOURCE_BAR_COUNT_INVALID")
    if type(lookback_kind) is not str or lookback_kind not in _LOOKBACK_KINDS:
        _fail("LOOKBACK_KIND_INVALID")
    if type(lookback_value) is not int or not 1 <= lookback_value <= 10_000_000_000:
        _fail("LOOKBACK_VALUE_INVALID")
    if lookback_kind == "NATIVE_OBSERVATIONS" and lookback_value != source_bar_count:
        _fail("NATIVE_LOOKBACK_SOURCE_COUNT_MISMATCH")
    start = _utc(fit_start_utc, "FIT_START_INVALID")
    end = _utc(fit_end_utc, "FIT_END_INVALID")
    latest = _utc(latest_closed_bar_utc, "LATEST_CLOSED_BAR_INVALID")
    if (
        not datetime.fromisoformat(start)
        < datetime.fromisoformat(latest)
        <= datetime.fromisoformat(end)
    ):
        _fail("FIT_INTERVAL_INVALID")
    body = {
        "schema_version": HORIZON_PROCESS_SCHEMA,
        "symbol": symbol,
        "timeframe": timeframe,
        "lookback_kind": lookback_kind,
        "lookback_value": lookback_value,
        "source_bar_count": source_bar_count,
        "fit_start_utc": start,
        "fit_end_utc": end,
        "latest_closed_bar_utc": latest,
        "source_snapshot_sha256": _sha256(
            source_snapshot_sha256, "SOURCE_SNAPSHOT_SHA256_INVALID"
        ),
    }
    return body, canonical_artifact_sha256(body)


def build_trial_definition_artifact_v2(
    *,
    strategy_id: str,
    parameters: Mapping[str, Any],
    winner_process: Mapping[str, Any],
    horizon_process: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    """Build one exact full-parameter trial bound to process and horizon bytes."""

    name, strategy = _strategy(strategy_id)
    normalized = _normalized_parameters(name, parameters)
    process = dict(winner_process)
    horizon = dict(horizon_process)
    if (
        process.get("schema_version") != WINNER_PROCESS_SCHEMA
        or process.get("strategy_id") != name
    ):
        _fail("WINNER_PROCESS_BINDING_INVALID")
    if horizon.get("schema_version") != HORIZON_PROCESS_SCHEMA:
        _fail("HORIZON_PROCESS_BINDING_INVALID")
    body = {
        "schema_version": TRIAL_DEFINITION_SCHEMA,
        "strategy_id": name,
        "strategy_family": strategy.category.value,
        "process_sha256": canonical_artifact_sha256(process),
        "horizon_process_sha256": canonical_artifact_sha256(horizon),
        "feature_contract_sha256": _feature_contract_sha256(strategy),
        "indicator_contract_sha256": _source_file_sha256(FeatureComputer),
        "parameter_count": len(normalized),
        "parameters": normalized,
    }
    return body, canonical_artifact_sha256(body)


def build_refit_recipe_artifact_v2(
    *,
    strategy_id: str,
    winner_process: Mapping[str, Any],
    horizon_process: Mapping[str, Any],
    trial_definition: Mapping[str, Any],
    source_bar_count: int,
    seed: int,
) -> tuple[dict[str, Any], str]:
    """Build the one-touch stateless materialization recipe."""

    name, strategy = _strategy(strategy_id)
    if type(seed) is not int or not 0 <= seed <= 0x7FFFFFFF:
        _fail("REFIT_SEED_INVALID")
    if type(source_bar_count) is not int or source_bar_count < max(
        2, int(strategy.MIN_BARS)
    ):
        _fail("REFIT_SOURCE_TOO_SHORT")
    process = dict(winner_process)
    horizon = dict(horizon_process)
    trial = dict(trial_definition)
    if process.get("strategy_id") != name or trial.get("strategy_id") != name:
        _fail("REFIT_STRATEGY_BINDING_INVALID")
    if horizon.get("source_bar_count") != source_bar_count:
        _fail("REFIT_HORIZON_BINDING_INVALID")
    body = {
        "schema_version": REFIT_RECIPE_SCHEMA,
        "strategy_id": name,
        "winner_process_sha256": canonical_artifact_sha256(process),
        "winner_horizon_process_sha256": canonical_artifact_sha256(horizon),
        "winner_trial_definition_sha256": canonical_artifact_sha256(trial),
        "refit_mode": "STATELESS_RULE_MATERIALIZATION",
        "parameter_source": "CURRENT_WINNER_TRIAL",
        "source_validation": "FULL_NATIVE_PREFIX_STREAM",
        "fit_window_rule": "HORIZON_NATIVE_PREFIX_THROUGH_CURRENT_CUTOFF",
        "deterministic_fit_procedure_id": "FXPM_STATELESS_RULE_MATERIALIZE_V1",
        "seed": seed,
        "version_pins": {
            "python": (
                f"{sys.version_info.major}.{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            ),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pm_core_sha256": _source_file_sha256(FeatureComputer),
            "pm_strategies_sha256": _source_file_sha256(type(strategy)),
        },
        "data_requirements": {
            "columns": list(_BAR_COLUMNS),
            "minimum_bars": max(2, int(strategy.MIN_BARS)),
            "warmup_bars": max(0, int(strategy.MIN_BARS)),
        },
        "attempts": 1,
        "post_selection_search": False,
        "output_schema_version": SPECIALIST_OUTPUT_SCHEMA,
    }
    return body, canonical_artifact_sha256(body)


def build_current_refit_search_artifacts_v2(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    parameters: Mapping[str, Any],
    lookback_kind: str,
    lookback_value: int,
    fit_start_utc: str,
    fit_end_utc: str,
    latest_closed_bar_utc: str,
    source_snapshot_sha256: str,
    source_bar_count: int,
    seed: int,
) -> CurrentRefitSearchArtifacts:
    """Build the four mutually bound artifacts for one current search attempt."""

    horizon, horizon_sha = build_horizon_process_artifact_v2(
        symbol=symbol,
        timeframe=timeframe,
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        fit_start_utc=fit_start_utc,
        fit_end_utc=fit_end_utc,
        latest_closed_bar_utc=latest_closed_bar_utc,
        source_snapshot_sha256=source_snapshot_sha256,
        source_bar_count=source_bar_count,
    )
    process, process_sha = build_winner_process_artifact_v2(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_id=strategy_id,
        parameters=parameters,
        horizon_process_sha256=horizon_sha,
    )
    trial, trial_sha = build_trial_definition_artifact_v2(
        strategy_id=strategy_id,
        parameters=parameters,
        winner_process=process,
        horizon_process=horizon,
    )
    recipe, recipe_sha = build_refit_recipe_artifact_v2(
        strategy_id=strategy_id,
        winner_process=process,
        horizon_process=horizon,
        trial_definition=trial,
        source_bar_count=source_bar_count,
        seed=seed,
    )
    return CurrentRefitSearchArtifacts(
        process_candidate_id=process["process_candidate_id"],
        winner_process=process,
        winner_process_sha256=process_sha,
        horizon_process=horizon,
        horizon_process_sha256=horizon_sha,
        trial_definition=trial,
        trial_definition_sha256=trial_sha,
        refit_recipe=recipe,
        refit_recipe_sha256=recipe_sha,
    )


def _parse_artifact_bytes(
    raw: Any,
    *,
    fields: frozenset[str],
    fields_code: str,
) -> dict[str, Any]:
    if type(raw) is not bytes or not 0 < len(raw) <= _MAX_ARTIFACT_BYTES:
        _fail("ARTIFACT_BYTES_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise CurrentRefitArtifactError("ARTIFACT_JSON_INVALID") from exc
    if type(value) is not dict:
        _fail("ARTIFACT_JSON_OBJECT_REQUIRED")
    if canonical_artifact_bytes(value) != raw:
        _fail("ARTIFACT_BYTES_NONCANONICAL")
    if set(value) != fields:
        _fail(fields_code)
    return value


def verify_current_refit_search_artifacts_bytes_v2(
    *,
    winner_process_bytes: bytes,
    horizon_process_bytes: bytes,
    trial_definition_bytes: bytes,
    refit_recipe_bytes: bytes,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    lookback_kind: str,
    lookback_value: int,
    fit_start_utc: str,
    fit_end_utc: str,
    latest_closed_bar_utc: str,
    source_snapshot_sha256: str,
    source_bar_count: int,
    seed: int,
) -> CurrentRefitSearchArtifacts:
    """Verify canonical raw bytes without importing the production executor.

    The caller supplies the independently known cell, source-prefix, and seed
    identity.  Trial parameters are decoded from the raw trial and replayed
    through the same exact registry/default/grid codec used by the producer.
    The expected four-artifact chain is then reconstructed so every code pin,
    candidate identity, regime condition, and cross-artifact SHA is checked.
    """

    process = _parse_artifact_bytes(
        winner_process_bytes,
        fields=_WINNER_PROCESS_FIELDS,
        fields_code="WINNER_PROCESS_FIELDS_INVALID",
    )
    horizon = _parse_artifact_bytes(
        horizon_process_bytes,
        fields=_HORIZON_PROCESS_FIELDS,
        fields_code="HORIZON_PROCESS_FIELDS_INVALID",
    )
    trial = _parse_artifact_bytes(
        trial_definition_bytes,
        fields=_TRIAL_DEFINITION_FIELDS,
        fields_code="TRIAL_DEFINITION_FIELDS_INVALID",
    )
    recipe = _parse_artifact_bytes(
        refit_recipe_bytes,
        fields=_REFIT_RECIPE_FIELDS,
        fields_code="REFIT_RECIPE_FIELDS_INVALID",
    )

    expected = build_current_refit_search_artifacts_v2(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_id=strategy_id,
        parameters=trial["parameters"],
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        fit_start_utc=fit_start_utc,
        fit_end_utc=fit_end_utc,
        latest_closed_bar_utc=latest_closed_bar_utc,
        source_snapshot_sha256=source_snapshot_sha256,
        source_bar_count=source_bar_count,
        seed=seed,
    )
    observed = (
        (
            process,
            winner_process_bytes,
            expected.winner_process,
            expected.winner_process_sha256,
            "WINNER_PROCESS_SEMANTICS_INVALID",
            "WINNER_PROCESS_SHA256_INVALID",
        ),
        (
            horizon,
            horizon_process_bytes,
            expected.horizon_process,
            expected.horizon_process_sha256,
            "HORIZON_PROCESS_SEMANTICS_INVALID",
            "HORIZON_PROCESS_SHA256_INVALID",
        ),
        (
            trial,
            trial_definition_bytes,
            expected.trial_definition,
            expected.trial_definition_sha256,
            "TRIAL_DEFINITION_SEMANTICS_INVALID",
            "TRIAL_DEFINITION_SHA256_INVALID",
        ),
        (
            recipe,
            refit_recipe_bytes,
            expected.refit_recipe,
            expected.refit_recipe_sha256,
            "REFIT_RECIPE_SEMANTICS_INVALID",
            "REFIT_RECIPE_SHA256_INVALID",
        ),
    )
    for body, raw, expected_body, expected_sha256, semantics_code, sha_code in observed:
        if body != expected_body:
            _fail(semantics_code)
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            _fail(sha_code)

    return CurrentRefitSearchArtifacts(
        process_candidate_id=process["process_candidate_id"],
        winner_process=process,
        winner_process_sha256=hashlib.sha256(winner_process_bytes).hexdigest(),
        horizon_process=horizon,
        horizon_process_sha256=hashlib.sha256(horizon_process_bytes).hexdigest(),
        trial_definition=trial,
        trial_definition_sha256=hashlib.sha256(trial_definition_bytes).hexdigest(),
        refit_recipe=recipe,
        refit_recipe_sha256=hashlib.sha256(refit_recipe_bytes).hexdigest(),
    )


def spool_current_refit_search_attempt_v2(
    sink: SymbolAuthoritySink,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    parameters: Mapping[str, Any],
    lookback_kind: str,
    lookback_value: int,
    fit_start_utc: str,
    fit_end_utc: str,
    latest_closed_bar_utc: str,
    source_snapshot_sha256: str,
    source_bar_count: int,
    seed: int,
) -> CurrentRefitSpoolWrite:
    """Persist one complete executable search-attempt definition before compaction."""

    if type(sink) is not SymbolAuthoritySink:
        _fail("AUTHORITY_SINK_INVALID")
    normalized_symbol, normalized_timeframe, normalized_regime = _cell_identity(
        symbol, timeframe, regime
    )
    if sink.symbol != normalized_symbol:
        _fail("AUTHORITY_SINK_SYMBOL_MISMATCH")
    artifacts = build_current_refit_search_artifacts_v2(
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        regime=normalized_regime,
        strategy_id=strategy_id,
        parameters=parameters,
        lookback_kind=lookback_kind,
        lookback_value=lookback_value,
        fit_start_utc=fit_start_utc,
        fit_end_utc=fit_end_utc,
        latest_closed_bar_utc=latest_closed_bar_utc,
        source_snapshot_sha256=source_snapshot_sha256,
        source_bar_count=source_bar_count,
        seed=seed,
    )
    prefix = (
        f"current-search/{normalized_timeframe}/{normalized_regime}/"
        f"{artifacts.process_candidate_id}"
    )
    bodies = {
        "winner_process": (
            "current_refit_winner_process",
            artifacts.winner_process,
            artifacts.winner_process_sha256,
        ),
        "horizon_process": (
            "current_refit_horizon_process",
            artifacts.horizon_process,
            artifacts.horizon_process_sha256,
        ),
        "trial_definition": (
            "current_refit_trial_definition",
            artifacts.trial_definition,
            artifacts.trial_definition_sha256,
        ),
        "refit_recipe": (
            "current_refit_recipe",
            artifacts.refit_recipe,
            artifacts.refit_recipe_sha256,
        ),
    }
    entries: dict[str, dict[str, Any]] = {}
    for name, (artifact_type, body, expected_sha256) in bodies.items():
        entry = sink.put_json(f"{prefix}/{name}", artifact_type, body)
        if entry["descriptor"]["sha256"] != expected_sha256:
            _fail("SPOOL_CANONICAL_HASH_MISMATCH")
        entries[name] = entry
    return CurrentRefitSpoolWrite(artifacts=artifacts, entries=entries)


__all__ = [
    "CurrentRefitArtifactError",
    "CurrentRefitSearchArtifacts",
    "CurrentRefitSpoolWrite",
    "HORIZON_PROCESS_SCHEMA",
    "REFIT_RECIPE_SCHEMA",
    "TRIAL_DEFINITION_SCHEMA",
    "WINNER_PROCESS_SCHEMA",
    "build_current_refit_search_artifacts_v2",
    "build_horizon_process_artifact_v2",
    "build_refit_recipe_artifact_v2",
    "build_trial_definition_artifact_v2",
    "build_winner_process_artifact_v2",
    "canonical_artifact_bytes",
    "canonical_artifact_sha256",
    "spool_current_refit_search_attempt_v2",
    "verify_current_refit_search_artifacts_bytes_v2",
]
