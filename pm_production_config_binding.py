"""Immutable production binding for the effective optimizer configuration.

The raw deployment JSON is the authority surface, while search execution uses
``PipelineConfig``.  This module deterministically bridges those two forms once
before evidence access and stores only canonical bytes.  Historical and
current search can therefore materialize independent owned objects without a
caller-held mutable ``PipelineConfig`` changing the process between phases.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Final, Mapping, Sequence

import pm_authority_spool as authority_spool
import pm_cap_calibration as cap
from pm_core import PipelineConfig
from pm_position import PositionConfig


BINDING_SCHEMA: Final = "fxpm.production-effective-pipeline-config.v1"
FUNDED_SCOPE: Final = "funded_live_v1"
MAX_CONFIG_BYTES: Final = 16 * 1024 * 1024
MAX_SNAPSHOT_BYTES: Final = 4 * 1024 * 1024
_SEAL: Final = object()


class ProductionConfigBindingError(RuntimeError):
    """Typed non-economic configuration failure."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in tuple(details)[:16])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ProductionConfigBindingError(code, details)


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            _fail("ACTIVE_CONFIG_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    _fail("ACTIVE_CONFIG_NONFINITE_NUMBER", value)


def _canonical_bytes(value: Any, *, maximum: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ProductionConfigBindingError(
            "PRODUCTION_CONFIG_NOT_CANONICAL_JSON",
            (type(exc).__name__, str(exc)),
        ) from exc
    if not encoded or len(encoded) > maximum:
        _fail("PRODUCTION_CONFIG_CANONICAL_SIZE_INVALID", len(encoded), maximum)
    return encoded


def _owned(value: Mapping[str, Any], code: str) -> dict[str, Any]:
    if type(value) is not dict:
        _fail(code)
    result = json.loads(
        _canonical_bytes(value, maximum=MAX_CONFIG_BYTES).decode("ascii")
    )
    if type(result) is not dict:
        _fail(code)
    return result


def _read_config(path: Path) -> tuple[dict[str, Any], Path]:
    try:
        absolute = Path(path).resolve(strict=True)
        before = absolute.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or not 0 < before.st_size <= MAX_CONFIG_BYTES
        ):
            _fail("ACTIVE_CONFIG_FILE_INVALID")
        with absolute.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_size != before.st_size
                or opened.st_mtime_ns != before.st_mtime_ns
            ):
                _fail("ACTIVE_CONFIG_FILE_CHANGED")
            raw = stream.read(before.st_size + 1)
        after = absolute.lstat()
        if (
            len(raw) != before.st_size
            or opened.st_size != after.st_size
            or opened.st_mtime_ns != after.st_mtime_ns
        ):
            _fail("ACTIVE_CONFIG_FILE_CHANGED")
        payload = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_pairs,
            parse_constant=_reject_constant,
        )
    except ProductionConfigBindingError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ProductionConfigBindingError(
            "ACTIVE_CONFIG_READ_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if type(payload) is not dict:
        _fail("ACTIVE_CONFIG_OBJECT_REQUIRED")
    return _owned(payload, "ACTIVE_CONFIG_OBJECT_REQUIRED"), absolute


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _snapshot(config: PipelineConfig) -> dict[str, Any]:
    if type(config) is not PipelineConfig:
        _fail("PIPELINE_CONFIG_TYPE_INVALID")
    body = {
        item.name: _json_value(getattr(config, item.name))
        for item in fields(PipelineConfig)
    }
    return _owned(body, "PIPELINE_CONFIG_SNAPSHOT_INVALID")


def _normalized_storage(payload: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {item.name for item in fields(PipelineConfig)}
    result: dict[str, Any] = {}
    for key, value in payload.items():
        if key in allowed:
            result[key] = value
        elif f"storage_{key}" in allowed:
            result[f"storage_{key}"] = value
    return result


def _derive_pipeline_config(payload: Mapping[str, Any], path: Path) -> PipelineConfig:
    pipeline = payload.get("pipeline")
    storage = payload.get("storage") or {}
    if type(pipeline) is not dict or type(storage) is not dict:
        _fail("PRODUCTION_PIPELINE_CONFIG_SECTION_INVALID")
    merged = dict(pipeline)
    merged.update(_normalized_storage(storage))
    allowed = {item.name for item in fields(PipelineConfig)}
    try:
        result = PipelineConfig(
            **{key: value for key, value in merged.items() if key in allowed}
        )
    except Exception as exc:
        raise ProductionConfigBindingError(
            "PRODUCTION_PIPELINE_CONFIG_RECONSTRUCTION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc

    metadata = payload.get("_meta") or {}
    if type(metadata) is not dict:
        _fail("PRODUCTION_CONFIG_METADATA_INVALID")
    if str(metadata.get("scope") or "").strip().lower() == FUNDED_SCOPE:
        result.regime_scoring_model = "legacy"
        result.oos_validation_mode = "fixed_holdout"

    # ``pm_main`` always projects the independently loaded PositionConfig TP
    # surface onto PipelineConfig after applying the funded cost contract.  TP
    # multipliers are the only PositionConfig values retained by the optimizer
    # object, so reproduce that projection exactly instead of silently relying
    # on today's coincidentally neutral defaults.
    position_payload = payload.get("position") or {}
    if type(position_payload) is not dict:
        _fail("PRODUCTION_POSITION_CONFIG_SECTION_INVALID")
    position_allowed = {item.name for item in fields(PositionConfig)}
    try:
        position = PositionConfig(
            **{
                key: value
                for key, value in position_payload.items()
                if key in position_allowed
            }
        )
    except Exception as exc:
        raise ProductionConfigBindingError(
            "PRODUCTION_POSITION_CONFIG_RECONSTRUCTION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if str(metadata.get("scope") or "").strip().lower() == FUNDED_SCOPE:
        position.use_spread = bool(result.use_spread)
        position.use_slippage = bool(result.use_slippage)
        position.slippage_pips = float(result.slippage_pips or 0.0)
    result.regime_tp_multipliers = dict(position.regime_tp_multipliers or {})

    result._deployment_config_path = str(path)
    raw_data_dir = Path(result.data_dir)
    result.data_dir = (
        raw_data_dir.resolve()
        if raw_data_dir.is_absolute()
        else (path.parent / raw_data_dir).resolve()
    )
    manual_path = str(result.manual_kill_switch_path or "").strip()
    if manual_path:
        raw_manual = Path(manual_path)
        result.manual_kill_switch_path = str(
            raw_manual.resolve()
            if raw_manual.is_absolute()
            else (path.parent / raw_manual).resolve()
        )
    result._funded_live_execution = False
    if (
        bool(result.false_discovery_control_enabled)
        and int(result.false_discovery_symbol_count or 0) <= 0
    ):
        symbols = payload.get("symbols")
        if type(symbols) is not list or not symbols:
            _fail("PRODUCTION_CONFIG_SYMBOL_ROSTER_INVALID")
        result.false_discovery_symbol_count = len(symbols)
    return result


def _verify_context(
    context: authority_spool.GenerationSpoolContext,
    config_sha256: str,
) -> None:
    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    try:
        observed = authority_spool.reopen_generation_context(
            context.root,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=config_sha256,
        )
    except Exception as exc:
        raise ProductionConfigBindingError(
            "GENERATION_CONTEXT_REOPEN_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if observed.context_sha256 != context.context_sha256:
        _fail("GENERATION_CONTEXT_CHANGED")


@dataclass(frozen=True, slots=True)
class FrozenProductionPipelineConfig:
    active_config_path: Path
    config_sha256: str
    effective_config_sha256: str
    _payload_json: bytes
    _snapshot_json: bytes
    _seal: object

    def verify_current(
        self,
        context: authority_spool.GenerationSpoolContext,
    ) -> bool:
        if self._seal is not _SEAL:
            _fail("FROZEN_PIPELINE_CONFIG_OBJECT_INVALID")
        _verify_context(context, self.config_sha256)
        payload, observed_path = _read_config(self.active_config_path)
        if observed_path != self.active_config_path:
            _fail("ACTIVE_CONFIG_PATH_CHANGED")
        payload_json = _canonical_bytes(payload, maximum=MAX_CONFIG_BYTES)
        if (
            payload_json != self._payload_json
            or cap.canonical_json_sha256(payload) != self.config_sha256
        ):
            _fail("ACTIVE_CONFIG_BYTES_CHANGED")
        if (
            hashlib.sha256(self._snapshot_json).hexdigest()
            != self.effective_config_sha256
        ):
            _fail("EFFECTIVE_PIPELINE_CONFIG_SNAPSHOT_CHANGED")
        envelope = json.loads(self._snapshot_json.decode("ascii"))
        if (
            type(envelope) is not dict
            or set(envelope) != {"schema_version", "pipeline_config"}
            or envelope.get("schema_version") != BINDING_SCHEMA
            or type(envelope.get("pipeline_config")) is not dict
        ):
            _fail("EFFECTIVE_PIPELINE_CONFIG_ENVELOPE_INVALID")
        return True

    def materialize(
        self,
        context: authority_spool.GenerationSpoolContext,
    ) -> PipelineConfig:
        self.verify_current(context)
        envelope = json.loads(self._snapshot_json.decode("ascii"))
        snapshot = envelope["pipeline_config"]
        try:
            result = PipelineConfig(**snapshot)
        except Exception as exc:
            raise ProductionConfigBindingError(
                "FROZEN_PIPELINE_CONFIG_MATERIALIZATION_FAILED",
                (type(exc).__name__, str(exc)),
            ) from exc
        result._deployment_config_path = str(self.active_config_path)
        result._funded_live_execution = False
        if _snapshot(result) != snapshot:
            _fail("FROZEN_PIPELINE_CONFIG_ROUNDTRIP_MISMATCH")
        return result

    def payload(self) -> dict[str, Any]:
        return json.loads(self._payload_json.decode("ascii"))


def freeze_production_pipeline_config(
    context: authority_spool.GenerationSpoolContext,
    *,
    active_config_path: Path,
    config_payload: Mapping[str, Any],
    supplied_config: PipelineConfig,
) -> FrozenProductionPipelineConfig:
    """Bind exact raw bytes and freeze one deterministic effective config."""

    observed, path = _read_config(active_config_path)
    supplied_payload = _owned(config_payload, "ACTIVE_CONFIG_PAYLOAD_INVALID")
    if observed != supplied_payload:
        _fail("ACTIVE_CONFIG_PAYLOAD_MISMATCH")
    config_sha256 = cap.canonical_json_sha256(observed)
    if config_sha256 != context.config_sha256:
        _fail("ACTIVE_CONFIG_CONTEXT_SHA256_MISMATCH")
    _verify_context(context, config_sha256)
    configured_path = str(
        getattr(supplied_config, "_deployment_config_path", "") or ""
    ).strip()
    try:
        supplied_path = Path(configured_path).resolve(strict=True)
    except Exception as exc:
        raise ProductionConfigBindingError("PIPELINE_CONFIG_SOURCE_UNBOUND") from exc
    if supplied_path != path:
        _fail("PIPELINE_CONFIG_SOURCE_MISMATCH")
    if bool(getattr(supplied_config, "_funded_live_execution", False)):
        _fail("OPTIMIZER_PIPELINE_CONFIG_FUNDED_EXECUTION_FORBIDDEN")

    expected = _derive_pipeline_config(observed, path)
    expected_snapshot = _snapshot(expected)
    supplied_snapshot = _snapshot(supplied_config)
    if supplied_snapshot != expected_snapshot:
        changed = sorted(
            key
            for key in expected_snapshot
            if supplied_snapshot.get(key) != expected_snapshot[key]
        )
        _fail("EFFECTIVE_PIPELINE_CONFIG_MISMATCH", *changed[:16])
    payload_json = _canonical_bytes(observed, maximum=MAX_CONFIG_BYTES)
    snapshot_json = _canonical_bytes(
        {
            "schema_version": BINDING_SCHEMA,
            "pipeline_config": expected_snapshot,
        },
        maximum=MAX_SNAPSHOT_BYTES,
    )
    result = FrozenProductionPipelineConfig(
        active_config_path=path,
        config_sha256=config_sha256,
        effective_config_sha256=hashlib.sha256(snapshot_json).hexdigest(),
        _payload_json=payload_json,
        _snapshot_json=snapshot_json,
        _seal=_SEAL,
    )
    result.verify_current(context)
    result.materialize(context)
    return result


__all__ = [
    "BINDING_SCHEMA",
    "FrozenProductionPipelineConfig",
    "ProductionConfigBindingError",
    "freeze_production_pipeline_config",
]
