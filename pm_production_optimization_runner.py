"""Single production ``--optimize`` authority-chain runner.

This module is the narrow caller between the optimizer command and the
accepted evidence producers.  It deliberately does not publish the active
winner ledger.  A successful call returns the coordinator-reverified ledger
bytes and authority assembly; the pipeline remains responsible for its final
compare-and-swap.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
from pathlib import Path
import shutil
import secrets
import stat
import threading
from typing import Any, Final, Mapping, Sequence

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal_evidence
import pm_current_refit_search_producer as current_refit
import pm_historical_production_adapter as historical_adapter
import pm_historical_search_producer as historical_search
import pm_production_anchor_plan as anchor_plan
import pm_production_authority_coordinator as coordinator
import pm_production_capital_bootstrap as capital_bootstrap
import pm_production_config_binding as config_binding
import pm_production_search_planner as search_planner
from pm_core import PipelineConfig


RUNNER_SCHEMA: Final = "fxpm.production-optimization-runner.v1"
MAX_CONFIG_SNAPSHOT_BYTES: Final = 16 * 1024 * 1024
MAX_STAGED_LEDGER_BYTES: Final = coordinator.MAX_CURRENT_LEDGER_BYTES
EVIDENCE_ACCESS_WINDOW_SECONDS: Final = 7 * 24 * 60 * 60
_BYTES_PER_GIB: Final = 1024**3
_UTC: Final = timezone.utc
_RESULT_SEAL: Final = object()
_CLEANUP_CAPABILITY_KEY: Final = secrets.token_bytes(32)
_CONCRETE_PATH_TYPE: Final = type(Path("."))


class ProductionOptimizationRunnerError(RuntimeError):
    """Typed technical failure; never a market or ``NO_TRADE`` verdict."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ":".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


class ProductionOptimizationNotPromotable(RuntimeError):
    """Evidence-complete generation that must preserve the last-good ledger."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ":".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ProductionOptimizationRunnerError(code, details)


def _remove_owned_tree(path: Path, expected_parent: Path) -> None:
    """Remove only one exact runner-owned tree without following a reparse point."""

    path = Path(path)
    if path.parent != expected_parent:
        raise RuntimeError("OWNED_TREE_PARENT_MISMATCH")
    try:
        if path.parent.resolve(strict=True) != expected_parent:
            raise RuntimeError("OWNED_TREE_PARENT_CHANGED")
        metadata = path.lstat()
    except FileNotFoundError:
        return
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    is_reparse = bool(
        stat.S_ISLNK(metadata.st_mode)
        or int(getattr(metadata, "st_file_attributes", 0) or 0) & reparse_flag
    )
    if is_reparse:
        path.unlink()
    elif stat.S_ISDIR(metadata.st_mode):
        shutil.rmtree(path)
    else:
        raise RuntimeError("OWNED_TREE_NOT_DIRECTORY")
    if path.exists() or path.is_symlink():
        raise RuntimeError("OWNED_TREE_REMOVE_INCOMPLETE")


class _FailedRunCleanup:
    """Own exactly the paths created by this invocation until it succeeds."""

    __slots__ = ("_armed", "_run_root", "_target_path")

    def __init__(self) -> None:
        self._armed = False
        self._run_root: Path | None = None
        self._target_path: Path | None = None

    def arm(self, *, run_root: Path, target_path: Path) -> None:
        if self._armed:
            _fail("FAILED_RUN_CLEANUP_ALREADY_ARMED")
        self._run_root = Path(run_root)
        self._target_path = Path(target_path)
        self._armed = True

    def release(self) -> None:
        self._armed = False

    def cleanup(self) -> tuple[str, ...]:
        if not self._armed:
            return ()
        errors: list[str] = []
        paths = (
            (
                self._target_path,
                self._target_path.parent if self._target_path else None,
            ),
            (self._run_root, self._run_root.parent if self._run_root else None),
        )
        for path, parent in paths:
            if path is None or parent is None:
                continue
            try:
                _remove_owned_tree(path, parent)
            except Exception as exc:
                errors.append(f"{path.name}:{type(exc).__name__}:{exc}")
        self._armed = False
        return tuple(errors)


def _canonical_bytes(value: Any, maximum: int) -> bytes:
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ProductionOptimizationRunnerError("RUNNER_JSON_INVALID") from exc
    if not 0 < len(raw) <= maximum:
        _fail("RUNNER_JSON_SIZE_INVALID", len(raw), maximum)
    return raw


def _snapshot_object(value: Any, *, code: str, maximum: int) -> dict[str, Any]:
    raw = _canonical_bytes(value, maximum)
    parsed = json.loads(raw.decode("ascii"))
    if type(parsed) is not dict:
        _fail(code)
    return parsed


def _cleanup_capability_sha256(
    *,
    schema_version: str,
    generation_id: str,
    run_id: str,
    run_root: Path,
    authority_assembly_path: Path,
    staged_ledger_source_path: Path,
    staged_ledger_content_sha256: str,
) -> str:
    binding = _canonical_bytes(
        {
            "schema_version": schema_version,
            "generation_id": generation_id,
            "run_id": run_id,
            "run_root": str(run_root),
            "authority_assembly_path": str(authority_assembly_path),
            "staged_ledger_source_path": str(staged_ledger_source_path),
            "staged_ledger_content_sha256": staged_ledger_content_sha256,
        },
        16 * 1024,
    )
    return hmac.new(
        _CLEANUP_CAPABILITY_KEY,
        binding,
        hashlib.sha256,
    ).hexdigest()


def _decode_canonical_object(
    raw: Any, *, code: str, maximum: int
) -> tuple[dict[str, Any], bytes]:
    if type(raw) is not bytes or not 0 < len(raw) <= maximum:
        _fail(code)
    try:
        parsed = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionOptimizationRunnerError(code) from exc
    if type(parsed) is not dict or _canonical_bytes(parsed, maximum) != raw:
        _fail(code)
    return parsed, raw


def _identity(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 512
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        _fail(code)
    return value


def _utc_text() -> str:
    return datetime.now(_UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionOptimizationRunnerError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return parsed.astimezone(_UTC)


def _require_evidence_access_window(payload: Mapping[str, Any]) -> None:
    """Require an already-open issuer clock window; never sleep around drift."""

    not_before = _parse_utc(
        payload.get("evidence_access_not_before_utc"),
        "EVIDENCE_ACCESS_NOT_BEFORE_INVALID",
    )
    deadline = _parse_utc(
        payload.get("evidence_access_deadline_utc"),
        "EVIDENCE_ACCESS_DEADLINE_INVALID",
    )
    if not not_before < deadline:
        _fail("EVIDENCE_ACCESS_WINDOW_INVALID")
    now = datetime.now(_UTC)
    if now < not_before:
        _fail("EVIDENCE_ACCESS_WINDOW_NOT_OPEN")
    if now >= deadline:
        _fail("EVIDENCE_ACCESS_WINDOW_EXPIRED")


@dataclass(frozen=True, slots=True)
class ProductionOptimizationRequest:
    """Outcome-free inputs already held by the production optimizer caller."""

    active_config_path: Path
    config: PipelineConfig
    config_payload: Mapping[str, Any]
    requested_symbols: Sequence[str]
    native_universe: Any
    generation_id: str
    authorization_scope: str = capital_bootstrap.DEMO_PROBATION_SCOPE
    funded_boundary: capital_bootstrap.FundedAuthorityBoundary | None = None
    spool_limits: authority_spool.SpoolLimits | None = None


@dataclass(frozen=True, slots=True)
class ProductionOptimizationResult:
    """Exact pre-CAS result.  Possession of this object grants no authority."""

    schema_version: str
    generation_id: str
    run_id: str
    run_root: Path
    authority_assembly_path: Path
    staged_ledger_source_path: Path
    staged_ledger_bytes: bytes
    staged_ledger_content_sha256: str
    physical_ledger_sha256: str
    anchor_plan_receipt_sha256: str
    search_process_receipt_sha256: str
    causal_precommit_receipt_sha256: str
    capital_bootstrap_identity_sha256: str
    historical_run_receipt_sha256: str
    historical_process_decision_sha256: str
    current_roster_sha256: str
    capital_decision_sha256: str
    selected_policy_point_id: str
    _authority_chain_receipt_json: bytes
    _authority_artifacts_json: bytes
    _cleanup_capability_sha256: str
    _seal: object

    @property
    def authority_chain_receipt(self) -> dict[str, Any]:
        return json.loads(self._authority_chain_receipt_json.decode("ascii"))

    @property
    def authority_artifacts(self) -> dict[str, Any]:
        return json.loads(self._authority_artifacts_json.decode("ascii"))

    def manifest_fields(self) -> dict[str, Any]:
        """Return only the fields consumed by the pipeline's pre-CAS gate."""

        if self._seal is not _RESULT_SEAL:
            _fail("PRODUCTION_OPTIMIZATION_RESULT_INVALID")
        return {
            "authority_chain_receipt": self.authority_chain_receipt,
            "authority_artifacts": self.authority_artifacts,
            "authority_assembly_path": str(self.authority_assembly_path),
            "historical_run_receipt_sha256": self.historical_run_receipt_sha256,
            "historical_process_decision_sha256": (
                self.historical_process_decision_sha256
            ),
            "current_roster_sha256": self.current_roster_sha256,
            "capital_decision_sha256": self.capital_decision_sha256,
            "selected_policy_point_id": self.selected_policy_point_id,
            "production_authority_runner_schema": self.schema_version,
        }


def discard_unpublished_production_result(
    result: ProductionOptimizationResult,
) -> bool:
    """Delete an exact sealed result that the caller has not published by CAS.

    The pipeline owns the CAS decision.  It must never call this after a
    successful publication; this helper only makes the pre-CAS cleanup narrow,
    geometry-checked, idempotent, and incapable of deleting another run.
    """

    if (
        type(result) is not ProductionOptimizationResult
        or result._seal is not _RESULT_SEAL
        or type(result.schema_version) is not str
        or result.schema_version != RUNNER_SCHEMA
        or type(result.run_root) is not _CONCRETE_PATH_TYPE
        or type(result.authority_assembly_path) is not _CONCRETE_PATH_TYPE
        or type(result.staged_ledger_source_path) is not _CONCRETE_PATH_TYPE
        or type(result.staged_ledger_content_sha256) is not str
    ):
        _fail("UNPUBLISHED_PRODUCTION_RESULT_INVALID")
    generation_id = _identity(result.generation_id, "GENERATION_ID_INVALID")
    run_id = _identity(result.run_id, "RUN_ID_INVALID")
    expected_capability = _cleanup_capability_sha256(
        schema_version=result.schema_version,
        generation_id=generation_id,
        run_id=run_id,
        run_root=result.run_root,
        authority_assembly_path=result.authority_assembly_path,
        staged_ledger_source_path=result.staged_ledger_source_path,
        staged_ledger_content_sha256=result.staged_ledger_content_sha256,
    )
    try:
        receipt_generation_id = result.authority_chain_receipt.get("generation_id")
    except Exception as exc:
        raise ProductionOptimizationRunnerError(
            "UNPUBLISHED_PRODUCTION_RESULT_CAPABILITY_INVALID"
        ) from exc
    if (
        type(result._cleanup_capability_sha256) is not str
        or not hmac.compare_digest(
            result._cleanup_capability_sha256,
            expected_capability,
        )
        or receipt_generation_id != generation_id
    ):
        _fail("UNPUBLISHED_PRODUCTION_RESULT_CAPABILITY_INVALID")
    digest = hashlib.sha256(generation_id.encode("utf-8")).hexdigest()
    run_root = Path(result.run_root)
    assembly_path = Path(result.authority_assembly_path)
    if (
        not run_root.is_absolute()
        or run_root.name != f"run-{digest}"
        or run_root.parent.name != "fxpm-production-runs"
    ):
        _fail("UNPUBLISHED_RUN_ROOT_GEOMETRY_INVALID")
    deployment_root = run_root.parent.parent
    if (
        not assembly_path.is_absolute()
        or assembly_path.parent != deployment_root
        or assembly_path.name != f"fxpm-authority-{digest}"
    ):
        _fail("UNPUBLISHED_AUTHORITY_PATH_GEOMETRY_INVALID")
    errors: list[str] = []
    for path, parent in (
        (assembly_path, deployment_root),
        (run_root, run_root.parent),
    ):
        try:
            _remove_owned_tree(path, parent)
        except Exception as exc:
            errors.append(f"{path.name}:{type(exc).__name__}:{exc}")
    if errors:
        _fail("UNPUBLISHED_PRODUCTION_RESULT_CLEANUP_FAILED", *errors)
    return True


class _LazyCurrentRefitProducer:
    """Start the bounded symbol scheduler only after the coordinator proves PASS."""

    __slots__ = (
        "_active_config_path",
        "_config_payload",
        "_context",
        "_cutoff_utc",
        "_current_selection_window",
        "_data_dir",
        "_lock",
        "_output_root",
        "_pipeline_config",
        "_search_process",
        "_used",
    )

    def __init__(
        self,
        *,
        context: authority_spool.GenerationSpoolContext,
        active_config_path: Path,
        config_payload: Mapping[str, Any],
        data_dir: Path,
        cutoff_utc: str,
        current_selection_window: Mapping[str, str],
        search_process: search_planner.FrozenProductionSearchProcess,
        pipeline_config: PipelineConfig,
        output_root: Path,
    ) -> None:
        self._context = context
        self._active_config_path = Path(active_config_path)
        self._config_payload = _snapshot_object(
            config_payload,
            code="ACTIVE_CONFIG_PAYLOAD_INVALID",
            maximum=MAX_CONFIG_SNAPSHOT_BYTES,
        )
        self._data_dir = Path(data_dir)
        self._cutoff_utc = str(cutoff_utc)
        self._current_selection_window = _snapshot_object(
            current_selection_window,
            code="CURRENT_SELECTION_WINDOW_INVALID",
            maximum=4096,
        )
        self._search_process = search_process
        self._pipeline_config = pipeline_config
        self._output_root = Path(output_root)
        self._lock = threading.Lock()
        self._used = False

    def __call__(self, request: coordinator.CurrentRefitRequest) -> Any:
        with self._lock:
            if self._used:
                _fail("CURRENT_REFIT_FACTORY_ALREADY_CONSUMED")
            # Consume before the first latest-cutoff source access.  Partial
            # current evidence can never be retried inside the generation.
            self._used = True
        producer = current_refit.build_current_refit_search_producer(
            current_refit.CurrentRefitSearchProductionInputs(
                context=self._context,
                search_process=self._search_process,
                active_config_path=self._active_config_path,
                config_payload=self._config_payload,
                pipeline_config=self._pipeline_config,
                native_products={},
                current_selection_window=self._current_selection_window,
                output_root=self._output_root,
                symbol_scheduler=current_refit.CurrentRefitSymbolSchedulerInputs(
                    active_config_path=self._active_config_path,
                    data_dir=self._data_dir,
                    cutoff_utc=self._cutoff_utc,
                    source_accessed_at_utc=_utc_text(),
                    requested_workers=max(
                        1,
                        int(
                            getattr(
                                self._pipeline_config,
                                "optimization_max_workers",
                                1,
                            )
                            or 1
                        ),
                    ),
                ),
            )
        )
        result = producer(request)
        return result


def _validate_request(
    request: ProductionOptimizationRequest,
) -> tuple[
    Path,
    Path,
    PipelineConfig,
    dict[str, Any],
    tuple[str, ...],
    str,
]:
    if type(request) is not ProductionOptimizationRequest:
        _fail("PRODUCTION_OPTIMIZATION_REQUEST_INVALID")
    if not isinstance(request.config, PipelineConfig):
        _fail("PRODUCTION_PIPELINE_CONFIG_INVALID")
    try:
        config_path = Path(request.active_config_path).resolve(strict=True)
        config_mode = config_path.stat().st_mode
    except (OSError, RuntimeError) as exc:
        raise ProductionOptimizationRunnerError(
            "ACTIVE_DEPLOYMENT_CONFIG_INVALID"
        ) from exc
    if not stat.S_ISREG(config_mode):
        _fail("ACTIVE_DEPLOYMENT_CONFIG_NOT_REGULAR")
    deployment_root = config_path.parent.resolve(strict=True)
    payload = _snapshot_object(
        request.config_payload,
        code="ACTIVE_CONFIG_PAYLOAD_INVALID",
        maximum=MAX_CONFIG_SNAPSHOT_BYTES,
    )
    expected_symbols = tuple(book.PINNED_SYMBOL_ORDER)
    try:
        requested = tuple(request.requested_symbols)
    except TypeError as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_SYMBOL_ROSTER_INVALID"
        ) from exc
    if requested != expected_symbols or payload.get("symbols") != list(
        expected_symbols
    ):
        _fail("PRODUCTION_SYMBOL_ROSTER_NOT_EXACT")
    expected_timeframes = tuple(book.TIMEFRAME_ORDER)
    pipeline_payload = payload.get("pipeline")
    if (
        type(pipeline_payload) is not dict
        or pipeline_payload.get("timeframes") != list(expected_timeframes)
        or tuple(request.config.timeframes) != expected_timeframes
    ):
        _fail("PRODUCTION_TIMEFRAME_ROSTER_NOT_EXACT")
    generation_id = _identity(request.generation_id, "GENERATION_ID_INVALID")
    if request.authorization_scope not in {
        capital_bootstrap.DEMO_PROBATION_SCOPE,
        capital_bootstrap.FUNDED_LIVE_SCOPE,
    }:
        _fail("AUTHORIZATION_SCOPE_INVALID")
    funded_execution = getattr(request.config, "_funded_live_execution", None) is True
    if funded_execution != (
        request.authorization_scope == capital_bootstrap.FUNDED_LIVE_SCOPE
    ):
        _fail("AUTHORIZATION_SCOPE_RUNTIME_MISMATCH")
    if (
        request.spool_limits is not None
        and type(request.spool_limits) is not authority_spool.SpoolLimits
    ):
        _fail("SPOOL_LIMITS_INVALID")
    configured_path = str(
        getattr(request.config, "_deployment_config_path", "") or ""
    ).strip()
    try:
        bound_path = Path(configured_path).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ProductionOptimizationRunnerError(
            "PIPELINE_CONFIG_SOURCE_UNBOUND"
        ) from exc
    if bound_path != config_path:
        _fail("PIPELINE_CONFIG_SOURCE_MISMATCH")
    return (
        config_path,
        deployment_root,
        request.config,
        payload,
        requested,
        generation_id,
    )


def _effective_spool_limits(
    *,
    config: PipelineConfig,
    deployment_root: Path,
    requested: authority_spool.SpoolLimits | None,
) -> authority_spool.SpoolLimits:
    """Bind one exact generation budget to launch-time disk authority.

    Caller-supplied limits may tighten the production policy but can never
    weaken either the configured critical-free reserve or the live physical
    capacity.  The resulting immutable limits are embedded in the generation
    context before any producer starts.
    """

    base = authority_spool.validate_spool_limits(
        requested or authority_spool.SpoolLimits()
    )
    configured_gib = getattr(config, "storage_critical_free_gb", None)
    if (
        isinstance(configured_gib, bool)
        or not isinstance(configured_gib, (int, float))
        or not math.isfinite(float(configured_gib))
        or float(configured_gib) < 0.0
    ):
        _fail("PRODUCTION_SPOOL_FREE_RESERVE_INVALID")
    configured_reserve = int(math.ceil(float(configured_gib) * _BYTES_PER_GIB))
    reserve = max(configured_reserve, int(base.min_free_bytes))
    try:
        free_bytes = int(shutil.disk_usage(deployment_root).free)
    except (OSError, ValueError, TypeError) as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_SPOOL_FREE_SPACE_UNAVAILABLE",
            (type(exc).__name__, str(exc)),
        ) from exc
    available = free_bytes - reserve
    maximum = min(int(base.max_generation_bytes), available)
    if maximum <= 0:
        _fail(
            "PRODUCTION_SPOOL_FREE_SPACE_RESERVE_UNAVAILABLE",
            free_bytes,
            reserve,
        )
    artifact = min(int(base.max_artifact_bytes), maximum)
    symbol = min(int(base.max_symbol_bytes), maximum)
    return authority_spool.validate_spool_limits(
        authority_spool.SpoolLimits(
            max_artifact_bytes=artifact,
            max_symbol_bytes=symbol,
            max_artifacts_per_symbol=base.max_artifacts_per_symbol,
            max_manifest_bytes=min(base.max_manifest_bytes, artifact),
            max_json_bytes=min(base.max_json_bytes, artifact),
            stream_chunk_bytes=min(base.stream_chunk_bytes, artifact),
            max_generation_bytes=maximum,
            min_free_bytes=reserve,
        )
    )


def _read_exact_staged_ledger(
    run: coordinator.ProductionAuthorityRun,
    *,
    deployment_root: Path,
) -> tuple[Path, bytes]:
    try:
        path = Path(run.staged_ledger_path).resolve(strict=True)
        path.relative_to(deployment_root)
        before = path.stat()
        if not stat.S_ISREG(before.st_mode) or not (
            0 < before.st_size <= MAX_STAGED_LEDGER_BYTES
        ):
            raise ValueError("size")
        raw = path.read_bytes()
        after = path.stat()
        if (
            len(raw) != before.st_size
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ino != after.st_ino
        ):
            raise ValueError("changed")
    except Exception as exc:
        raise ProductionOptimizationRunnerError(
            "COORDINATOR_STAGED_LEDGER_INVALID"
        ) from exc
    observed = hashlib.sha256(raw).hexdigest()
    if observed != run.staged_ledger_content_sha256:
        _fail("COORDINATOR_STAGED_LEDGER_HASH_MISMATCH")
    return path, raw


def _run_production_optimization_once(
    request: ProductionOptimizationRequest,
    cleanup: _FailedRunCleanup,
) -> ProductionOptimizationResult:
    """Run the complete evidence chain exactly once, stopping before CAS."""

    (
        config_path,
        deployment_root,
        config,
        config_payload,
        requested_symbols,
        generation_id,
    ) = _validate_request(request)
    spool_limits = _effective_spool_limits(
        config=config,
        deployment_root=deployment_root,
        requested=request.spool_limits,
    )
    digest = hashlib.sha256(generation_id.encode("utf-8")).hexdigest()
    run_id = f"production-run-{digest[:32]}"
    process_id = f"production-process-{digest[:32]}"
    target_name = f"fxpm-authority-{digest}"
    target_path = deployment_root / target_name
    if target_path.exists() or target_path.is_symlink():
        _fail("AUTHORITY_TARGET_ALREADY_EXISTS")

    try:
        environment = coordinator.derive_production_environment_authority(
            active_config_path=config_path,
            config=config,
            config_payload=config_payload,
            requested_symbols=requested_symbols,
            native_universe=request.native_universe,
        )
    except Exception as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_ENVIRONMENT_DERIVATION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    # Authority assembly deliberately rejects hidden relative paths.  Every
    # raw current/capital artifact referenced by its indexes must therefore
    # live below a non-hidden deployment-relative root.
    runs_root = deployment_root / "fxpm-production-runs"
    runs_root.mkdir(mode=0o700, exist_ok=True)
    run_root = runs_root / f"run-{digest}"
    try:
        run_root.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_GENERATION_WORK_ROOT_EXISTS"
        ) from exc
    except OSError as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_GENERATION_WORK_ROOT_CREATE_FAILED"
        ) from exc
    cleanup.arm(run_root=run_root, target_path=target_path)
    try:
        current_output_root = run_root / "current-refit"
        current_output_root.mkdir(mode=0o700)
    except OSError as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_GENERATION_WORK_ROOT_CREATE_FAILED"
        ) from exc

    try:
        context = authority_spool.freeze_generation_context(
            run_root / "spool",
            generation_id=generation_id,
            native_universe_sha256=environment["native_universe_sha256"],
            config_sha256=environment["config_sha256"],
            limits=spool_limits,
        )
        frozen_config = config_binding.freeze_production_pipeline_config(
            context,
            active_config_path=config_path,
            config_payload=config_payload,
            supplied_config=config,
        )
        historical_config = frozen_config.materialize(context)
        current_config = frozen_config.materialize(context)
        assembly_config = frozen_config.materialize(context)
        config_payload = frozen_config.payload()
        try:
            data_dir = Path(historical_config.data_dir).resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ProductionOptimizationRunnerError("NATIVE_DATA_ROOT_INVALID") from exc
        if not data_dir.is_dir():
            _fail("NATIVE_DATA_ROOT_NOT_DIRECTORY")
        plan_receipt = anchor_plan.derive_production_anchor_plan(data_dir)
        plan = anchor_plan.verify_production_anchor_plan_receipt(
            data_dir,
            plan_receipt,
        )
        native_identity = environment.get("native_identity")
        if (
            type(native_identity) is not dict
            or plan.get("activation_id") != native_identity.get("native_activation_id")
            or plan.get("activation_manifest_sha256")
            != native_identity.get("native_activation_manifest_sha256")
        ):
            _fail("ANCHOR_PLAN_ENVIRONMENT_IDENTITY_MISMATCH")
        search_process = search_planner.freeze_production_search_process(
            context,
            run_id=run_id,
        )
        causal_precommit = causal_evidence.freeze_causal_process_precommit_v8(
            context=context,
            search_process=search_process,
            process_id=process_id,
            anchors=plan["anchor_plan"],
            evidence_access_window_seconds=EVIDENCE_ACCESS_WINDOW_SECONDS,
            repository_root=Path(__file__).resolve().parent,
        )
        causal_payload = causal_precommit.to_dict()
        capital = capital_bootstrap.bootstrap_production_capital_authority(
            bootstrap_root=run_root / "capital-bootstrap",
            output_root=run_root / "capital-replay",
            authorization_scope=request.authorization_scope,
            generation_context=context,
            config_payload=config_payload,
            native_source_identity=environment["native_identity"],
            process_precommitment_payload=causal_payload,
            inner_selection_windows_by_anchor=plan["inner_selection_windows_by_anchor"],
            current_selection_window=plan["current_selection_window"],
            contract_lineage=environment["capital_contract"],
            margin_terms_sha256=environment["margin_terms_sha256"],
            broker_terms_sha256=environment["broker_terms_sha256"],
            funded_boundary=request.funded_boundary,
        )
    except Exception as exc:
        raise ProductionOptimizationRunnerError(
            "PRE_EVIDENCE_AUTHORITY_FREEZE_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc

    _require_evidence_access_window(causal_payload)
    try:
        preplanned = historical_search.preplan_historical_search(
            data_dir=data_dir,
            context=context,
            config=historical_config,
            process=search_process,
            causal_precommit_payload=causal_payload,
            run_id=run_id,
            anchors=plan["anchor_plan"],
            symbol_scheduler=historical_search.HistoricalSymbolSchedulerInputs(
                active_config_path=config_path,
                requested_workers=max(
                    1,
                    int(
                        getattr(
                            historical_config,
                            "optimization_max_workers",
                            1,
                        )
                        or 1
                    ),
                ),
            ),
        )
        causal_factory = causal_evidence.BoundedCausalEvidenceFactory(
            context=context,
            causal_precommit_payload=causal_payload,
            capital_phase_binding=capital.forward_phase_binding,
            forward_product_resolver=preplanned,
            config=historical_config,
        )
        current_factory = _LazyCurrentRefitProducer(
            context=context,
            active_config_path=config_path,
            config_payload=config_payload,
            data_dir=data_dir,
            cutoff_utc=plan["current_refit_cutoff_utc"],
            current_selection_window=plan["current_selection_window"],
            search_process=search_process,
            pipeline_config=current_config,
            output_root=current_output_root,
        )
        historical_inputs = coordinator.HistoricalAuthorityInputs(
            context=context,
            run_id=run_id,
            causal_precommit_payload=causal_payload,
            anchors=preplanned.anchors,
            optimizer_producer_release_sha256=(
                historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
            ),
            anchor_producers=preplanned.anchor_producers(),
            causal_evidence_factory=causal_factory,
        )
        assembly_inputs = coordinator.AssemblyInputs(
            deployment_root=deployment_root,
            target_name=target_name,
            generation_id=generation_id,
            authorization_scope=request.authorization_scope,
            active_config_path=config_path,
            config=assembly_config,
            config_payload=config_payload,
            requested_symbols=requested_symbols,
            native_universe=request.native_universe,
            current_selection_window=plan["current_selection_window"],
            validation_time_utc=None,
        )
        authority_run = coordinator.coordinate_production_authority(
            historical_inputs=historical_inputs,
            current_refit_factory=current_factory,
            capital_factory=capital.capital_factory,
            assembly_inputs=assembly_inputs,
        )
    except current_refit.CurrentRefitSearchNotPromotable as exc:
        raise ProductionOptimizationNotPromotable(
            "CURRENT_REFIT_NOT_PROMOTABLE",
            (exc.code, *exc.details),
        ) from exc
    except Exception as exc:
        raise ProductionOptimizationRunnerError(
            "PRODUCTION_AUTHORITY_COORDINATION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if type(authority_run) is not coordinator.ProductionAuthorityRun:
        _fail("PRODUCTION_AUTHORITY_RUN_INVALID")
    if authority_run.assembly.directory.resolve(strict=True) != target_path.resolve(
        strict=True
    ):
        _fail("PRODUCTION_AUTHORITY_ASSEMBLY_PATH_MISMATCH")
    ledger_path, ledger_bytes = _read_exact_staged_ledger(
        authority_run,
        deployment_root=deployment_root,
    )
    receipt, receipt_raw = _decode_canonical_object(
        authority_run.assembly.chain_receipt_canonical_json,
        code="AUTHORITY_CHAIN_RECEIPT_INVALID",
        maximum=4 * 1024 * 1024,
    )
    artifacts, artifacts_raw = _decode_canonical_object(
        authority_run.assembly.descriptors_canonical_json,
        code="AUTHORITY_ARTIFACT_DESCRIPTORS_INVALID",
        maximum=16 * 1024 * 1024,
    )
    if (
        receipt.get("generation_id") != generation_id
        or receipt.get("authorization_scope") != request.authorization_scope
        or receipt.get("requested_symbols") != list(requested_symbols)
        or receipt.get("ledger_sha256") != authority_run.staged_ledger_content_sha256
    ):
        _fail("AUTHORITY_CHAIN_RECEIPT_BINDING_MISMATCH")
    return ProductionOptimizationResult(
        schema_version=RUNNER_SCHEMA,
        generation_id=generation_id,
        run_id=run_id,
        run_root=run_root,
        authority_assembly_path=authority_run.assembly.directory,
        staged_ledger_source_path=ledger_path,
        staged_ledger_bytes=ledger_bytes,
        staged_ledger_content_sha256=authority_run.staged_ledger_content_sha256,
        physical_ledger_sha256=authority_run.physical_ledger_sha256,
        anchor_plan_receipt_sha256=plan_receipt.receipt_sha256,
        search_process_receipt_sha256=search_process.receipt_sha256,
        causal_precommit_receipt_sha256=causal_precommit.receipt_sha256,
        capital_bootstrap_identity_sha256=capital.identity_sha256,
        historical_run_receipt_sha256=(authority_run.historical_run_receipt_sha256),
        historical_process_decision_sha256=(
            authority_run.historical_process_decision_sha256
        ),
        current_roster_sha256=authority_run.current_roster_sha256,
        capital_decision_sha256=authority_run.capital_decision_sha256,
        selected_policy_point_id=authority_run.selected_policy_point_id,
        _authority_chain_receipt_json=receipt_raw,
        _authority_artifacts_json=artifacts_raw,
        _cleanup_capability_sha256=_cleanup_capability_sha256(
            schema_version=RUNNER_SCHEMA,
            generation_id=generation_id,
            run_id=run_id,
            run_root=run_root,
            authority_assembly_path=authority_run.assembly.directory,
            staged_ledger_source_path=ledger_path,
            staged_ledger_content_sha256=(authority_run.staged_ledger_content_sha256),
        ),
        _seal=_RESULT_SEAL,
    )


def run_production_optimization(
    request: ProductionOptimizationRequest,
) -> ProductionOptimizationResult:
    """Run once and reclaim every path owned by a failed invocation."""

    cleanup = _FailedRunCleanup()
    try:
        result = _run_production_optimization_once(request, cleanup)
    except BaseException as original:
        errors = cleanup.cleanup()
        if errors and isinstance(original, Exception):
            raise ProductionOptimizationRunnerError(
                "FAILED_PRODUCTION_RUN_CLEANUP_FAILED",
                (type(original).__name__, str(original), *errors),
            ) from original
        raise
    cleanup.release()
    return result


__all__ = [
    "EVIDENCE_ACCESS_WINDOW_SECONDS",
    "ProductionOptimizationRequest",
    "ProductionOptimizationResult",
    "ProductionOptimizationNotPromotable",
    "ProductionOptimizationRunnerError",
    "RUNNER_SCHEMA",
    "discard_unpublished_production_result",
    "run_production_optimization",
]
