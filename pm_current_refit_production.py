"""Immutable current-refit capture adapter for production authority."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pm_book_multiplicity as book
import pm_current_refit_execution as execution
import pm_portfolio_replay as replay
from pm_current_refit_parameters import (
    canonicalize_parameter_mapping,
    decode_parameter_mapping,
)
from pm_strategies import StrategyRegistry
from pm_authority_spool import (
    CANONICAL_JSON_FORMAT,
    CURRENT_REFIT_MANIFEST_DOMAIN,
    AuthoritySpoolError,
    SymbolAuthoritySink,
    verify_symbol_receipt,
)
from pm_model_lifecycle import (
    CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    canonical_receipt_sha256,
    open_typed_receipt,
)


CAPTURE_CONTRACT_VERSION = "fxpm.current-refit-production-capture.v2"
CANDIDATE_LEDGER_VERSION = "fxpm.current-refit-candidate-source-ledger.v2"
PORTFOLIO_SOURCE_LEDGER_VERSION = "fxpm.current-refit-portfolio-source-ledger.v2"
SCHEDULE_DEFAULT_VALIDITY_BASIS = "CONFIG_RETRAIN_SCHEDULE_DEFAULT"

ART_CURRENT_BOOK = "current_refit_book_v9_decision"
ART_ACCEPTED_BOOK = "accepted_current_process_book_generation"
ART_REFIT_RECEIPT = "current_refit_execution_receipt"
ART_SOURCE_BUNDLE = "current_refit_source_snapshot_bundle"
ART_REGIME_BUNDLE = "current_refit_regime_params_bundle"
ART_HORIZON_BUNDLE = "current_refit_horizon_decisions_bundle"
ART_SOURCE = "current_refit_source_snapshot"
ART_BARS = "current_refit_native_bars_csv"
ART_WINNER_PROCESS = "current_refit_winner_process"
ART_HORIZON_PROCESS = "current_refit_horizon_process"
ART_TRIAL = "current_refit_trial_definition"
ART_RECIPE = "current_refit_recipe"
ART_BOOK_ENTRY = "current_refit_winner_book_entry"
ART_CANDIDATE_RECORDS = "current_refit_replay_candidate_records"
ART_CANDIDATE_LEDGER = "current_refit_replay_candidate_ledger"
ART_PORTFOLIO_SOURCE_LEDGER = "current_refit_portfolio_source_ledger"

REQUIRED_GENERATION_ARTIFACT_TYPES = frozenset(
    {
        ART_CURRENT_BOOK,
        ART_ACCEPTED_BOOK,
        ART_REFIT_RECEIPT,
        ART_SOURCE_BUNDLE,
        ART_REGIME_BUNDLE,
        ART_HORIZON_BUNDLE,
        ART_CANDIDATE_RECORDS,
        ART_CANDIDATE_LEDGER,
        ART_PORTFOLIO_SOURCE_LEDGER,
    }
)
REQUIRED_CELL_ARTIFACT_TYPES = frozenset(
    {
        ART_SOURCE,
        ART_BARS,
        ART_WINNER_PROCESS,
        ART_HORIZON_PROCESS,
        ART_TRIAL,
        ART_RECIPE,
        ART_BOOK_ENTRY,
    }
)

_SHA = frozenset("0123456789abcdef")
_MAX_LEDGER_BYTES = 256 * 1024**2
_MAX_JSON_ARTIFACT_BYTES = 512 * 1024**2
_MAX_CANDIDATE_LINE_BYTES = 16 * 1024**2
_CANDIDATE_FIELDS = frozenset(
    {
        "candidate_id",
        "cell_id",
        "symbol",
        "timeframe",
        "regime",
        "strategy_name",
        "promotion_status",
        "asset_class",
        "risk_groups",
        "entry_time",
        "direction",
        "stop_risk_per_lot",
        "margin_per_lot",
        "volume_min",
        "volume_max",
        "volume_step",
        "priority_score",
        "legs",
        "marks",
        "source_schema",
        "margin_model",
    }
)
_LEG_FIELDS = frozenset(
    {
        "exit_time",
        "fraction",
        "net_pnl_per_entry_lot",
        "price_pnl_per_entry_lot",
        "swap_per_entry_lot",
        "exit_reason",
    }
)
_MARK_FIELDS = frozenset({"time", "pnl_per_current_lot", "observation_time"})
_CANDIDATE_LEDGER_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "staged_ledger_content_sha256",
        "physical_ledger_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
        "current_cutoff_utc",
        "records_content_sha256",
        "records_size_bytes",
        "candidate_count",
        "candidate_input_receipt",
        "candidate_records_sha256",
        "candidate_cell_ids",
        "replayed_cell_ids",
        "ledger_sha256",
    }
)
_PORTFOLIO_SOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "staged_ledger_content_sha256",
        "physical_ledger_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
        "candidate_records_sha256",
        "current_refit_execution_access_id",
        "replayed_cell_ids",
        "candidate_cell_ids",
        "window",
        "native_sources",
        "valuation_sources",
        "source_ledger_sha256",
    }
)
_RELEVANT_ARTIFACT_TYPES = (
    REQUIRED_GENERATION_ARTIFACT_TYPES | REQUIRED_CELL_ARTIFACT_TYPES
)
_CURRENT_REFIT_BINDING_FIELDS = (
    "historical_process_decision_sha256",
    "accepted_book_generation_receipt_sha256",
    "current_refit_cell_id",
    "winner_decision_sha256",
    "winner_book_entry_sha256",
    "source_snapshot_sha256",
    "parameter_sha256",
    "output_artifact_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
)
_RAW_FORMATS = {
    ART_ACCEPTED_BOOK: "fxpm.current-process-book-receipt.v1",
    ART_SOURCE_BUNDLE: "fxpm.current-refit-source-bundle.v1",
    ART_REGIME_BUNDLE: execution.CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    ART_HORIZON_BUNDLE: "fxpm.current-refit-horizon-bundle.v1",
    ART_SOURCE: "fxpm.current-refit-source-snapshot.v1",
    ART_BARS: "fxpm.current-refit-bars.csv.v1",
    ART_WINNER_PROCESS: "fxpm.current-refit-winner-process.v1",
    ART_HORIZON_PROCESS: "fxpm.current-refit-horizon-process.v2",
    ART_TRIAL: "fxpm.current-refit-trial-definition.v1",
    ART_RECIPE: "fxpm.current-refit-recipe.v1",
    ART_BOOK_ENTRY: "fxpm.book-multiplicity-cell.v9",
}


class CurrentRefitProductionCaptureError(ValueError):
    """A raw production capture is missing, stale, or contradictory."""

    def __init__(self, code: str, details: Sequence[str] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


@dataclass(frozen=True, slots=True)
class ProductionCaptureInputs:
    spool_root: Path
    compact_symbol_receipts: Mapping[str, Any]
    output_root: Path
    generation_id: str
    native_universe_sha256: str
    config_sha256: str
    required_valid_until_utc: datetime


@dataclass(frozen=True, slots=True)
class DerivedPhysicalCurrentLedger:
    path: Path
    payload: Mapping[str, Any]
    content_sha256: str
    physical_ledger_sha256: str


@dataclass(frozen=True, slots=True)
class CapturedArtifact:
    artifact_id: str
    artifact_type: str
    relative_path: str
    size_bytes: int
    content_sha256: str
    spool_format: str
    semantic_sha256: str
    format_version: str

    def raw_descriptor(self) -> dict[str, Any]:
        return execution.build_raw_artifact_descriptor(
            relative_path=self.relative_path,
            size_bytes=self.size_bytes,
            content_sha256=self.content_sha256,
            semantic_sha256=self.semantic_sha256,
            format_version=self.format_version,
        )


@dataclass(frozen=True, slots=True)
class _FrozenFile:
    path: Path
    size_bytes: int
    sha256: str

    def assert_unchanged(self, code: str) -> None:
        try:
            observed = _file_sha256(self.path, maximum_bytes=self.size_bytes)
        except (CurrentRefitProductionCaptureError, OSError):
            raise CurrentRefitProductionCaptureError(code) from None
        if observed != self.sha256:
            raise CurrentRefitProductionCaptureError(code)


def _fail(code: str, *details: str) -> None:
    raise CurrentRefitProductionCaptureError(code, details)


def _is_sha(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(c in _SHA for c in value)


def _required_aware_utc_deadline(value: Any) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        _fail("PHYSICAL_LEDGER_VALID_UNTIL_NOT_AWARE_UTC")
    return value.astimezone(timezone.utc)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            _fail("JSON_DUPLICATE_KEY", key)
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    _fail("JSON_NONFINITE_CONSTANT", value)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CurrentRefitProductionCaptureError(
            "JSON_CANONICALIZATION_FAILED"
        ) from exc


def _canonical_list_sha256(values: Iterable[Any]) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, value in enumerate(values):
        if index:
            digest.update(b",")
        digest.update(_canonical_bytes(value))
    digest.update(b"]")
    return digest.hexdigest()


def _candidate_records_sha256(candidates: Sequence[replay.CandidateTrade]) -> str:
    return _canonical_list_sha256(
        replay.current_refit_candidate_record(candidate) for candidate in candidates
    )


def _strict_json(raw: bytes, code: str) -> Any:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=_reject_constant,
        )
    except CurrentRefitProductionCaptureError:
        raise
    except Exception as exc:
        raise CurrentRefitProductionCaptureError(code) from exc
    return value


def _file_sha256(path: Path, *, maximum_bytes: int | None = None) -> str:
    target = Path(path).resolve(strict=True)
    size = target.stat().st_size
    if (
        not target.is_file()
        or size <= 0
        or (maximum_bytes is not None and size > maximum_bytes)
    ):
        _fail("CAPTURE_FILE_SIZE_INVALID", str(target))
    digest = hashlib.sha256()
    observed = 0
    with target.open("rb") as handle:
        before = os.fstat(handle.fileno())
        while chunk := handle.read(8 * 1024 * 1024):
            observed += len(chunk)
            if maximum_bytes is not None and observed > maximum_bytes:
                _fail("CAPTURE_FILE_SIZE_INVALID", str(target))
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    if before.st_size != size or after.st_size != size or observed != size:
        _fail("CAPTURE_FILE_CHANGED", str(target))
    return digest.hexdigest()


def _load_file_json(path: Path, *, maximum_bytes: int, canonical: bool) -> Any:
    target = Path(path).resolve(strict=True)
    size = target.stat().st_size
    if not target.is_file() or not 0 < size <= maximum_bytes:
        _fail("CAPTURE_JSON_SIZE_INVALID", str(target))
    with target.open("rb") as handle:
        before = os.fstat(handle.fileno())
        raw = handle.read(maximum_bytes + 1)
        after = os.fstat(handle.fileno())
    if before.st_size != size or after.st_size != size or len(raw) != size:
        _fail("CAPTURE_JSON_CHANGED", str(target))
    value = _strict_json(raw, "CAPTURE_JSON_INVALID")
    if canonical and _canonical_bytes(value) != raw:
        _fail("CAPTURE_JSON_NONCANONICAL", str(target))
    return value


def _artifact_path(root: Path, artifact: CapturedArtifact) -> Path:
    root = Path(root).resolve(strict=True)
    candidate = Path(artifact.relative_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        _fail("CAPTURE_ARTIFACT_PATH_INVALID", artifact.artifact_id)
    try:
        path = (root / candidate).resolve(strict=True)
        path.relative_to(root)
    except Exception as exc:
        raise CurrentRefitProductionCaptureError(
            "CAPTURE_ARTIFACT_PATH_INVALID", (artifact.artifact_id,)
        ) from exc
    if (
        not path.is_file()
        or path.stat().st_size != artifact.size_bytes
        or _file_sha256(path, maximum_bytes=artifact.size_bytes)
        != artifact.content_sha256
    ):
        _fail("CAPTURE_ARTIFACT_CHANGED", artifact.artifact_id)
    return path


def _artifact_json(root: Path, artifact: CapturedArtifact) -> dict[str, Any]:
    if artifact.spool_format != CANONICAL_JSON_FORMAT:
        _fail("CAPTURE_ARTIFACT_FORMAT_INVALID", artifact.artifact_id)
    value = _load_file_json(
        _artifact_path(root, artifact),
        maximum_bytes=min(_MAX_JSON_ARTIFACT_BYTES, artifact.size_bytes),
        canonical=True,
    )
    if type(value) is not dict:
        _fail("CAPTURE_ARTIFACT_NOT_OBJECT", artifact.artifact_id)
    return value


def _freeze_staged_ledger(path: Path) -> tuple[_FrozenFile, dict[str, Any], str]:
    target = Path(path).resolve(strict=True)
    ledger = _load_file_json(target, maximum_bytes=_MAX_LEDGER_BYTES, canonical=False)
    if type(ledger) is not dict or not ledger:
        _fail("STAGED_LEDGER_INVALID")
    size = target.stat().st_size
    content_sha = _file_sha256(target, maximum_bytes=_MAX_LEDGER_BYTES)
    physical_sha = replay.canonical_json_sha256(ledger)
    return _FrozenFile(target, size, content_sha), ledger, physical_sha


def _manifest_artifact_json(
    root: Path,
    descriptor: Mapping[str, Any],
    *,
    expected_format_version: str,
) -> dict[str, Any]:
    if (
        not isinstance(descriptor, Mapping)
        or descriptor.get("format_version") != expected_format_version
        or not _is_sha(descriptor.get("content_sha256"))
        or not _is_sha(descriptor.get("semantic_sha256"))
        or type(descriptor.get("size_bytes")) is not int
    ):
        _fail("PHYSICAL_LEDGER_ARTIFACT_DESCRIPTOR_INVALID")
    artifact = CapturedArtifact(
        artifact_id=str(descriptor.get("artifact_id") or ""),
        artifact_type="physical-ledger-input",
        relative_path=str(descriptor.get("relative_path") or ""),
        size_bytes=int(descriptor["size_bytes"]),
        content_sha256=str(descriptor["content_sha256"]),
        spool_format=CANONICAL_JSON_FORMAT,
        semantic_sha256=str(descriptor["semantic_sha256"]),
        format_version=str(descriptor["format_version"]),
    )
    return _artifact_json(root, artifact)


def _selected_book_cells(book_body: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    cells = book_body.get("cells")
    if not isinstance(cells, list):
        _fail("PHYSICAL_LEDGER_BOOK_CELL_ROSTER_INVALID")
    selected: dict[str, dict[str, Any]] = {}
    for raw in cells:
        if not isinstance(raw, Mapping):
            _fail("PHYSICAL_LEDGER_BOOK_CELL_ROSTER_INVALID")
        if str(raw.get("historical_state") or "").upper() != "WINNER":
            continue
        slot = "|".join(
            (
                str(raw.get("symbol") or ""),
                str(raw.get("timeframe") or "").upper(),
                str(raw.get("regime") or "").upper(),
            )
        )
        if (
            slot.count("|") != 2
            or slot.startswith("|")
            or slot.endswith("|")
            or slot in selected
        ):
            _fail("PHYSICAL_LEDGER_BOOK_WINNER_IDENTITY_INVALID", slot)
        selected[slot] = dict(raw)
    if not selected:
        _fail("PHYSICAL_LEDGER_SELECTED_ROSTER_EMPTY")
    return selected


def _build_physical_current_refit_ledger_payload(
    *,
    book_body: Mapping[str, Any],
    accepted_payload: Mapping[str, Any],
    accepted_receipt_sha256: str,
    refit_payload: Mapping[str, Any],
    refit_receipt_sha256: str,
    raw_manifest: Mapping[str, Any],
    artifact_root: Path,
    required_valid_until_utc: datetime,
) -> dict[str, Any]:
    """Derive the sparse physical book solely from raw-reverified winners."""

    run = refit_payload.get("execution")
    authorization = refit_payload.get("authorization")
    cells = run.get("cell_executions") if isinstance(run, Mapping) else None
    requests = raw_manifest.get("cell_requests")
    descriptors_raw = raw_manifest.get("artifact_descriptors")
    if (
        not isinstance(run, Mapping)
        or not isinstance(authorization, Mapping)
        or not isinstance(cells, list)
        or not cells
        or not isinstance(requests, list)
        or len(requests) != len(cells)
        or not isinstance(descriptors_raw, list)
    ):
        _fail("PHYSICAL_LEDGER_RAW_CURRENT_REFIT_INVALID")
    if (
        accepted_payload.get("book_multiplicity_receipt_sha256")
        != book_body.get("receipt_sha256")
        or authorization.get("accepted_book_generation_receipt_sha256")
        != accepted_receipt_sha256
        or accepted_payload.get("selected_winner_count") != len(cells)
        or run.get("output_book_sha256") in (None, "")
    ):
        _fail("PHYSICAL_LEDGER_CURRENT_BOOK_BINDING_MISMATCH")
    descriptors: dict[str, dict[str, Any]] = {}
    for raw in descriptors_raw:
        if not isinstance(raw, Mapping):
            _fail("PHYSICAL_LEDGER_ARTIFACT_DESCRIPTOR_INVALID")
        artifact_id = raw.get("artifact_id")
        if (
            type(artifact_id) is not str
            or not artifact_id
            or artifact_id in descriptors
        ):
            _fail("PHYSICAL_LEDGER_ARTIFACT_DESCRIPTOR_INVALID")
        descriptors[artifact_id] = dict(raw)

    selected = _selected_book_cells(book_body)
    refit_slots = [
        "|".join(
            (
                str(cell.get("symbol") or ""),
                str(cell.get("timeframe") or "").upper(),
                str(cell.get("regime") or "").upper(),
            )
        )
        for cell in cells
        if isinstance(cell, Mapping)
    ]
    if (
        len(refit_slots) != len(cells)
        or len(set(refit_slots)) != len(refit_slots)
        or set(refit_slots) != set(selected)
    ):
        _fail("PHYSICAL_LEDGER_SELECTED_ROSTER_MISMATCH")

    from pm_pipeline import RegimeConfig, SymbolConfig

    completed_at = str(run.get("completed_at_utc") or "")
    try:
        trained_at = datetime.fromisoformat(
            completed_at[:-1] + "+00:00" if completed_at.endswith("Z") else completed_at
        )
    except (TypeError, ValueError) as exc:
        raise CurrentRefitProductionCaptureError(
            "PHYSICAL_LEDGER_REFIT_TIME_INVALID"
        ) from exc
    if trained_at.tzinfo is None or trained_at.utcoffset() != timedelta(0):
        _fail("PHYSICAL_LEDGER_REFIT_TIME_NOT_AWARE_UTC")
    trained_at = trained_at.astimezone(timezone.utc)
    valid_until = _required_aware_utc_deadline(required_valid_until_utc)
    if valid_until <= trained_at:
        _fail("PHYSICAL_LEDGER_VALID_UNTIL_NOT_AFTER_REFIT")
    request_by_id: dict[str, Mapping[str, Any]] = {}
    for raw in requests:
        if not isinstance(raw, Mapping):
            _fail("PHYSICAL_LEDGER_CELL_REQUEST_INVALID")
        cell_id = raw.get("current_refit_cell_id")
        if type(cell_id) is not str or not cell_id or cell_id in request_by_id:
            _fail("PHYSICAL_LEDGER_CELL_REQUEST_INVALID")
        request_by_id[cell_id] = raw

    symbols: dict[str, SymbolConfig] = {}
    for cell in cells:
        if not isinstance(cell, Mapping):
            _fail("PHYSICAL_LEDGER_REFIT_CELL_INVALID")
        symbol = str(cell.get("symbol") or "")
        timeframe = str(cell.get("timeframe") or "").upper()
        regime = str(cell.get("regime") or "").upper()
        slot = f"{symbol}|{timeframe}|{regime}"
        book_cell = selected[slot]
        if book_cell.get("winner_decision_sha256") != cell.get(
            "winner_decision_sha256"
        ) or book_cell.get("historical_book_entry_sha256") != cell.get(
            "winner_book_entry_sha256"
        ):
            _fail("PHYSICAL_LEDGER_SELECTED_BOOK_ENTRY_MISMATCH", slot)
        request = request_by_id.get(str(cell.get("current_refit_cell_id") or ""))
        artifact_ids = (
            request.get("artifact_ids") if isinstance(request, Mapping) else None
        )
        if not isinstance(artifact_ids, Mapping):
            _fail("PHYSICAL_LEDGER_CELL_REQUEST_INVALID", slot)
        trial_descriptor = descriptors.get(artifact_ids.get("trial_definition"))
        entry_descriptor = descriptors.get(artifact_ids.get("winner_book_entry"))
        if trial_descriptor is None or entry_descriptor is None:
            _fail("PHYSICAL_LEDGER_CELL_ARTIFACT_MISSING", slot)
        trial = _manifest_artifact_json(
            artifact_root,
            trial_descriptor,
            expected_format_version=_RAW_FORMATS[ART_TRIAL],
        )
        entry = _manifest_artifact_json(
            artifact_root,
            entry_descriptor,
            expected_format_version=_RAW_FORMATS[ART_BOOK_ENTRY],
        )
        if (
            trial_descriptor.get("semantic_sha256")
            != cell.get("winner_trial_definition_sha256")
            or entry_descriptor.get("semantic_sha256")
            != cell.get("winner_book_entry_sha256")
            or entry.get("historical_book_entry_sha256")
            != cell.get("winner_book_entry_sha256")
            or entry.get("winner_decision_sha256") != cell.get("winner_decision_sha256")
            or trial.get("strategy_id") != cell.get("strategy_id")
        ):
            _fail("PHYSICAL_LEDGER_CELL_ARTIFACT_BINDING_MISMATCH", slot)
        wire_parameters = canonicalize_parameter_mapping(trial.get("parameters"))
        try:
            template = StrategyRegistry.get(str(cell["strategy_id"]))
            runtime_parameters = decode_parameter_mapping(
                wire_parameters, template.get_default_params()
            )
            fitted = StrategyRegistry.get(
                str(cell["strategy_id"]), **runtime_parameters
            )
            round_trip = canonicalize_parameter_mapping(fitted.get_params())
        except Exception as exc:
            raise CurrentRefitProductionCaptureError(
                "PHYSICAL_LEDGER_PARAMETER_DECODE_FAILED", (slot,)
            ) from exc
        if round_trip != wire_parameters:
            _fail("PHYSICAL_LEDGER_PARAMETER_ROUND_TRIP_MISMATCH", slot)
        parameter_payload = {
            "schema_version": "fxpm.current-refit-parameters.v1",
            "strategy_id": cell["strategy_id"],
            "parameters": wire_parameters,
        }
        if hashlib.sha256(_canonical_bytes(parameter_payload)).hexdigest() != cell.get(
            "parameter_sha256"
        ):
            _fail("PHYSICAL_LEDGER_PARAMETER_BINDING_MISMATCH", slot)
        binding = {name: cell.get(name) for name in _CURRENT_REFIT_BINDING_FIELDS}
        if any(not _is_sha(value) for value in binding.values()):
            _fail("PHYSICAL_LEDGER_REFIT_BINDING_INVALID", slot)
        artifact_meta = {
            "artifact_version": CAPTURE_CONTRACT_VERSION,
            "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
            "current_refit_binding": binding,
            "winner_process_candidate_id": cell.get("winner_process_candidate_id"),
            "winner_process_sha256": cell.get("winner_process_sha256"),
            "winner_horizon_process_sha256": cell.get("winner_horizon_process_sha256"),
            "winner_trial_definition_sha256": cell.get(
                "winner_trial_definition_sha256"
            ),
            "winner_refit_recipe_sha256": cell.get("winner_refit_recipe_sha256"),
            "current_refit_receipt_sha256": refit_receipt_sha256,
            "current_refit_execution_sha256": refit_payload.get("execution_sha256"),
            "current_refit_output_book_sha256": run.get("output_book_sha256"),
            "selected_winner_roster_sha256": authorization.get(
                "selected_winner_roster_sha256"
            ),
            "parameter_level_oos_evidence_claimed": False,
            "valid_until_basis": SCHEDULE_DEFAULT_VALIDITY_BASIS,
            "evidence_led_validity_horizon_claimed": False,
        }
        regime_config = RegimeConfig(
            strategy_name=str(cell["strategy_id"]),
            parameters=wire_parameters,
            quality_score=0.0,
            trained_at=trained_at,
            valid_until=valid_until,
            artifact_meta=artifact_meta,
            validation_evidence={
                "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
                "authority_scope": "DEMO_PROBATION_ONLY",
                "parameter_level_oos_evidence_claimed": False,
            },
            promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
            promotion_reasons=[
                "HISTORICAL_PROCESS_VALIDATED",
                "RAW_CURRENT_REFIT_REVERIFIED",
                "PROBATION_ONLY",
            ],
        )
        symbol_config = symbols.setdefault(
            symbol,
            SymbolConfig(
                symbol=symbol,
                is_validated=True,
                validation_reason="PROCESS_VALIDATED_CURRENT_REFIT_PROBATION",
                optimized_at=trained_at,
                valid_until=valid_until,
                artifact_meta={
                    "artifact_version": CAPTURE_CONTRACT_VERSION,
                    "book_role": "PROCESS_VALIDATED_CURRENT_REFIT",
                    "accepted_book_generation_receipt_sha256": (
                        accepted_receipt_sha256
                    ),
                    "current_refit_receipt_sha256": refit_receipt_sha256,
                    "current_refit_execution_sha256": refit_payload.get(
                        "execution_sha256"
                    ),
                    "current_refit_output_book_sha256": run.get("output_book_sha256"),
                    "valid_until_basis": SCHEDULE_DEFAULT_VALIDITY_BASIS,
                    "evidence_led_validity_horizon_claimed": False,
                },
            ),
        )
        timeframe_grid = symbol_config.regime_configs.setdefault(timeframe, {})
        if regime in timeframe_grid:
            _fail("PHYSICAL_LEDGER_CELL_DUPLICATE", slot)
        timeframe_grid[regime] = regime_config

    payload = {symbol: symbols[symbol].to_dict() for symbol in sorted(symbols)}
    projected = {
        f"{symbol}|{timeframe}|{regime}"
        for symbol, symbol_payload in payload.items()
        for timeframe, regimes in symbol_payload.get("regime_configs", {}).items()
        for regime in regimes
    }
    if projected != set(selected):
        _fail("PHYSICAL_LEDGER_SELECTED_ROSTER_MISMATCH")
    return payload


def derive_physical_current_refit_ledger(
    *,
    current_book_decision: Mapping[str, Any],
    accepted_book_generation_receipt: Mapping[str, Any],
    current_refit_receipt: Mapping[str, Any],
    raw_manifest_path: Path,
    artifact_root: Path,
    output_root: Path,
    required_valid_until_utc: datetime,
) -> DerivedPhysicalCurrentLedger:
    """Raw-reverify and materialize one immutable sparse current ledger."""

    verification = execution.verify_raw_current_refit_execution(
        current_refit_receipt,
        raw_manifest_path,
        artifact_root=artifact_root,
    )
    if verification.get("verified") is not True or verification.get("errors") != []:
        _fail(
            "PHYSICAL_LEDGER_RAW_REFIT_REVERIFY_FAILED", *verification.get("errors", ())
        )
    book_body = _owned_book(current_book_decision)
    try:
        accepted_payload, accepted_sha = open_typed_receipt(
            accepted_book_generation_receipt,
            expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
        )
        refit_payload, refit_sha = open_typed_receipt(
            current_refit_receipt,
            expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
        )
        manifest = execution.load_bounded_raw_current_refit_manifest(raw_manifest_path)
    except Exception as exc:
        raise CurrentRefitProductionCaptureError(
            "PHYSICAL_LEDGER_RECEIPT_OR_MANIFEST_INVALID"
        ) from exc
    payload = _build_physical_current_refit_ledger_payload(
        book_body=book_body,
        accepted_payload=accepted_payload,
        accepted_receipt_sha256=accepted_sha,
        refit_payload=refit_payload,
        refit_receipt_sha256=refit_sha,
        raw_manifest=manifest,
        artifact_root=Path(artifact_root).resolve(strict=True),
        required_valid_until_utc=required_valid_until_utc,
    )
    encoded = _canonical_bytes(payload)
    content_sha = hashlib.sha256(encoded).hexdigest()
    physical_sha = replay.canonical_json_sha256(payload)
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"physical_current_refit_ledger_{content_sha}.json"
    try:
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if path.read_bytes() != encoded:
            _fail("PHYSICAL_LEDGER_IMMUTABLE_PATH_CONFLICT")
    frozen, observed_payload, observed_physical = _freeze_staged_ledger(path)
    if (
        frozen.sha256 != content_sha
        or observed_payload != payload
        or observed_physical != physical_sha
    ):
        _fail("PHYSICAL_LEDGER_WRITE_REVERIFY_FAILED")
    return DerivedPhysicalCurrentLedger(
        path=path,
        payload=observed_payload,
        content_sha256=content_sha,
        physical_ledger_sha256=physical_sha,
    )


def _assert_pipeline_format_contract() -> None:
    import pm_pipeline as pipeline

    observed = {
        ART_ACCEPTED_BOOK: pipeline.CURRENT_PROCESS_BOOK_ARTIFACT_VERSION,
        ART_SOURCE_BUNDLE: pipeline.CURRENT_REFIT_SOURCE_BUNDLE_VERSION,
        ART_REGIME_BUNDLE: execution.CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
        ART_HORIZON_BUNDLE: pipeline.CURRENT_REFIT_HORIZON_BUNDLE_VERSION,
        ART_SOURCE: pipeline.CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION,
        ART_BARS: pipeline.CURRENT_REFIT_BAR_FILE_VERSION,
        ART_WINNER_PROCESS: pipeline.CURRENT_REFIT_PROCESS_VERSION,
        ART_HORIZON_PROCESS: pipeline.CURRENT_REFIT_HORIZON_VERSION,
        ART_TRIAL: pipeline.CURRENT_REFIT_TRIAL_VERSION,
        ART_RECIPE: pipeline.CURRENT_REFIT_RECIPE_VERSION,
        ART_BOOK_ENTRY: pipeline.CURRENT_REFIT_BOOK_ENTRY_VERSION,
    }
    if observed != _RAW_FORMATS:
        _fail("CURRENT_REFIT_RAW_FORMAT_CONTRACT_DRIFT")


def _semantic_and_version(
    root: Path,
    *,
    artifact_type: str,
    descriptor: Mapping[str, Any],
) -> tuple[str, str]:
    content_sha = descriptor.get("sha256")
    if not _is_sha(content_sha):
        _fail("CAPTURE_ARTIFACT_CONTENT_SHA_INVALID")
    if artifact_type in {ART_BARS, ART_CANDIDATE_RECORDS}:
        version = (
            _RAW_FORMATS[ART_BARS]
            if artifact_type == ART_BARS
            else "fxpm.current-refit-candidate-records.jsonl.v1"
        )
        return content_sha, version
    relative = descriptor.get("relative_path")
    candidate = CapturedArtifact(
        artifact_id="semantic-probe",
        artifact_type=artifact_type,
        relative_path=str(relative),
        size_bytes=int(descriptor.get("size_bytes", 0)),
        content_sha256=content_sha,
        spool_format=str(descriptor.get("format") or ""),
        semantic_sha256=content_sha,
        format_version="probe",
    )
    body = _artifact_json(root, candidate)
    if artifact_type == ART_BOOK_ENTRY:
        semantic = body.get("historical_book_entry_sha256")
    else:
        semantic = content_sha
    if not _is_sha(semantic):
        _fail("CAPTURE_ARTIFACT_SEMANTIC_SHA_INVALID", artifact_type)
    version = _RAW_FORMATS.get(artifact_type)
    if version is None:
        version = str(body.get("schema_version") or "")
    if not version:
        _fail("CAPTURE_ARTIFACT_SCHEMA_MISSING", artifact_type)
    return semantic, version


def _verified_artifacts(
    inputs: ProductionCaptureInputs,
) -> tuple[CapturedArtifact, ...]:
    root = Path(inputs.spool_root).resolve(strict=True)
    receipts = dict(inputs.compact_symbol_receipts)
    if not receipts:
        _fail("CAPTURE_RECEIPT_ROSTER_EMPTY")
    captured: dict[tuple[str, str], CapturedArtifact] = {}
    types: set[str] = set()
    for symbol in sorted(receipts):
        try:
            manifest = verify_symbol_receipt(
                root,
                receipts[symbol],
                expected_generation_id=inputs.generation_id,
                expected_symbol=symbol,
                expected_native_universe_sha256=inputs.native_universe_sha256,
                expected_config_sha256=inputs.config_sha256,
                expected_manifest_domain=CURRENT_REFIT_MANIFEST_DOMAIN,
            )
        except AuthoritySpoolError as exc:
            raise CurrentRefitProductionCaptureError(
                "CAPTURE_SYMBOL_RECEIPT_INVALID", (symbol, str(exc))
            ) from exc
        for entry in manifest["artifacts"]:
            artifact_type = str(entry["artifact_type"])
            if artifact_type not in _RELEVANT_ARTIFACT_TYPES:
                continue
            descriptor = dict(entry["descriptor"])
            semantic, version = _semantic_and_version(
                root,
                artifact_type=artifact_type,
                descriptor=descriptor,
            )
            item = CapturedArtifact(
                artifact_id=str(entry["artifact_id"]),
                artifact_type=artifact_type,
                relative_path=str(descriptor["relative_path"]),
                size_bytes=int(descriptor["size_bytes"]),
                content_sha256=str(descriptor["sha256"]),
                spool_format=str(descriptor["format"]),
                semantic_sha256=semantic,
                format_version=version,
            )
            key = (artifact_type, item.artifact_id)
            existing = captured.get(key)
            if existing is not None and existing != item:
                _fail("CAPTURE_ARTIFACT_ID_CONFLICT", artifact_type, item.artifact_id)
            captured[key] = item
            types.add(artifact_type)
    missing = sorted(
        (REQUIRED_GENERATION_ARTIFACT_TYPES | REQUIRED_CELL_ARTIFACT_TYPES) - types
    )
    if missing:
        _fail("CAPTURE_ARTIFACT_TYPES_MISSING", *missing)
    return tuple(captured[key] for key in sorted(captured))


def _one_type(
    artifacts: Sequence[CapturedArtifact], artifact_type: str
) -> CapturedArtifact:
    values = [item for item in artifacts if item.artifact_type == artifact_type]
    if len(values) != 1:
        _fail("CAPTURE_GENERATION_ARTIFACT_CARDINALITY_INVALID", artifact_type)
    return values[0]


def _one_semantic(
    artifacts: Sequence[CapturedArtifact], artifact_type: str, semantic: str
) -> CapturedArtifact:
    values = [
        item
        for item in artifacts
        if item.artifact_type == artifact_type and item.semantic_sha256 == semantic
    ]
    if len(values) != 1:
        _fail("CAPTURE_CELL_ARTIFACT_CARDINALITY_INVALID", artifact_type, semantic)
    return values[0]


def _owned_book(value: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(value)
    receipt = body.pop("receipt_sha256", None)
    if not _is_sha(receipt):
        _fail("CURRENT_BOOK_RECEIPT_INVALID")
    encoded = json.dumps(
        body,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    decision = book.BookMultiplicityDecision(encoded, receipt)
    if not book.verify_historical_book_multiplicity_decision(decision):
        _fail("CURRENT_BOOK_DECISION_INVALID")
    return decision.to_dict()


def _raw_artifact(
    artifacts: Sequence[CapturedArtifact], artifact_type: str, semantic: str
) -> tuple[CapturedArtifact, dict[str, Any]]:
    item = _one_semantic(artifacts, artifact_type, semantic)
    return item, item.raw_descriptor()


def assemble_raw_current_refit_manifest(
    *,
    current_refit_receipt: Mapping[str, Any],
    artifacts: Sequence[CapturedArtifact],
    artifact_root: Path,
    output_root: Path,
) -> Path:
    """Assemble and replay-verify the raw manifest from captured bytes."""

    _assert_pipeline_format_contract()
    payload, _ = open_typed_receipt(
        current_refit_receipt,
        expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    authorization = payload.get("authorization")
    run = payload.get("execution")
    cells = run.get("cell_executions") if isinstance(run, Mapping) else None
    if (
        not isinstance(authorization, Mapping)
        or not isinstance(cells, list)
        or not cells
    ):
        _fail("CURRENT_REFIT_RECEIPT_BODY_INVALID")

    bundle_specs = {
        "accepted_book_generation": (
            ART_ACCEPTED_BOOK,
            authorization["accepted_book_generation_receipt_sha256"],
        ),
        "source_snapshot_bundle": (
            ART_SOURCE_BUNDLE,
            authorization["source_snapshot_bundle_sha256"],
        ),
        "regime_params_bundle": (
            ART_REGIME_BUNDLE,
            authorization["regime_params_sha256"],
        ),
        "horizon_decisions_bundle": (
            ART_HORIZON_BUNDLE,
            authorization["horizon_decisions_sha256"],
        ),
    }
    selected: dict[str, dict[str, Any]] = {}
    bundle_ids: dict[str, str] = {}
    for role, (artifact_type, semantic) in bundle_specs.items():
        item, descriptor = _raw_artifact(artifacts, artifact_type, semantic)
        selected[descriptor["artifact_id"]] = descriptor
        bundle_ids[role] = descriptor["artifact_id"]

    role_fields = {
        "source_snapshot": (ART_SOURCE, "source_snapshot_sha256"),
        "winner_process": (ART_WINNER_PROCESS, "winner_process_sha256"),
        "horizon_process": (
            ART_HORIZON_PROCESS,
            "winner_horizon_process_sha256",
        ),
        "trial_definition": (ART_TRIAL, "winner_trial_definition_sha256"),
        "refit_recipe": (ART_RECIPE, "winner_refit_recipe_sha256"),
        "winner_book_entry": (ART_BOOK_ENTRY, "winner_book_entry_sha256"),
    }
    request_fields = (
        "current_refit_cell_id",
        "symbol",
        "timeframe",
        "regime",
        "strategy_id",
        "fit_start_utc",
        "fit_end_utc",
        "latest_closed_bar_utc",
        "source_snapshot_sha256",
        "winner_process_sha256",
        "winner_horizon_process_sha256",
        "winner_trial_definition_sha256",
        "winner_refit_recipe_sha256",
        "winner_book_entry_sha256",
        "regime_fit_receipt_sha256",
        "regime_detector_record_sha256",
        "regime_detector_parameters_sha256",
        "regime_detector_code_sha256",
    )
    requests: list[dict[str, Any]] = []
    bar_by_content = {
        item.content_sha256: item
        for item in artifacts
        if item.artifact_type == ART_BARS
    }
    for cell in cells:
        artifact_ids: dict[str, str] = {}
        for role, (artifact_type, field) in role_fields.items():
            item, descriptor = _raw_artifact(artifacts, artifact_type, str(cell[field]))
            selected[descriptor["artifact_id"]] = descriptor
            artifact_ids[role] = descriptor["artifact_id"]
            if role == "source_snapshot":
                source = _artifact_json(artifact_root, item)
                bar_item = bar_by_content.get(source.get("bars_content_sha256"))
                if bar_item is None:
                    _fail(
                        "CAPTURE_SOURCE_BAR_MISSING", str(cell["current_refit_cell_id"])
                    )
                bar_descriptor = bar_item.raw_descriptor()
                if source.get("bars_artifact_id") != bar_descriptor["artifact_id"]:
                    _fail(
                        "CAPTURE_SOURCE_BAR_DESCRIPTOR_NOT_PREBOUND",
                        str(cell["current_refit_cell_id"]),
                    )
                selected[bar_descriptor["artifact_id"]] = bar_descriptor
        request = {name: cell[name] for name in request_fields}
        request["artifact_ids"] = artifact_ids
        requests.append(request)

    manifest = execution.build_raw_current_refit_manifest(
        current_refit_receipt=current_refit_receipt,
        bundle_artifact_ids=bundle_ids,
        artifact_descriptors=[selected[key] for key in sorted(selected)],
        cell_requests=requests,
    )
    output = Path(output_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / f"raw_current_refit_{manifest['manifest_sha256']}.json"
    encoded = _canonical_bytes(manifest)
    try:
        with manifest_path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if manifest_path.read_bytes() != encoded:
            _fail("RAW_MANIFEST_IMMUTABLE_PATH_CONFLICT")
    verification = execution.verify_raw_current_refit_execution(
        current_refit_receipt,
        manifest_path,
        artifact_root=artifact_root,
    )
    if verification.get("verified") is not True or verification.get("errors") != []:
        _fail("RAW_CURRENT_REFIT_REVERIFY_FAILED", *verification.get("errors", ()))
    return manifest_path


def _candidate_from_record(record: Any) -> replay.CandidateTrade:
    if not isinstance(record, Mapping) or frozenset(record) != _CANDIDATE_FIELDS:
        _fail("CANDIDATE_RECORD_FIELDS_INVALID")
    legs = record.get("legs")
    marks = record.get("marks")
    groups = record.get("risk_groups")
    if (
        not isinstance(legs, list)
        or not isinstance(marks, list)
        or not isinstance(groups, list)
        or any(not isinstance(value, str) for value in groups)
        or any(
            not isinstance(row, Mapping) or frozenset(row) != _LEG_FIELDS
            for row in legs
        )
        or any(
            not isinstance(row, Mapping) or frozenset(row) != _MARK_FIELDS
            for row in marks
        )
    ):
        _fail("CANDIDATE_RECORD_COLLECTION_INVALID")
    try:
        candidate = replay.CandidateTrade(
            candidate_id=record["candidate_id"],
            cell_id=record["cell_id"],
            symbol=record["symbol"],
            timeframe=record["timeframe"],
            regime=record["regime"],
            strategy_name=record["strategy_name"],
            promotion_status=record["promotion_status"],
            asset_class=record["asset_class"],
            risk_groups=tuple(groups),
            entry_time=record["entry_time"],
            direction=record["direction"],
            stop_risk_per_lot=record["stop_risk_per_lot"],
            margin_per_lot=record["margin_per_lot"],
            volume_min=record["volume_min"],
            volume_max=record["volume_max"],
            volume_step=record["volume_step"],
            priority_score=record["priority_score"],
            legs=tuple(replay.ExitLeg(**dict(row)) for row in legs),
            marks=tuple(replay.MarkPoint(**dict(row)) for row in marks),
            source_schema=record["source_schema"],
            margin_model=record["margin_model"],
        ).normalized()
    except Exception as exc:
        raise CurrentRefitProductionCaptureError("CANDIDATE_RECORD_INVALID") from exc
    if replay.current_refit_candidate_record(candidate) != dict(record):
        _fail("CANDIDATE_RECORD_NONCANONICAL")
    return candidate


def _load_candidate_records(
    root: Path,
    artifact: CapturedArtifact,
    *,
    expected_count: int,
) -> tuple[replay.CandidateTrade, ...]:
    if not 1 <= expected_count <= replay.MAX_PORTFOLIO_REPLAY_CANDIDATES:
        _fail("CANDIDATE_COUNT_INVALID")
    path = _artifact_path(root, artifact)
    values: list[replay.CandidateTrade] = []
    with path.open("rb") as handle:
        while raw := handle.readline(_MAX_CANDIDATE_LINE_BYTES + 1):
            if len(raw) > _MAX_CANDIDATE_LINE_BYTES or not raw.endswith(b"\n"):
                _fail("CANDIDATE_RECORD_LINE_INVALID")
            body = raw[:-1]
            record = _strict_json(body, "CANDIDATE_RECORD_JSON_INVALID")
            if _canonical_bytes(record) != body:
                _fail("CANDIDATE_RECORD_NONCANONICAL")
            values.append(_candidate_from_record(record))
            if len(values) > expected_count:
                _fail("CANDIDATE_RECORD_COUNT_MISMATCH")
    if len(values) != expected_count:
        _fail("CANDIDATE_RECORD_COUNT_MISMATCH")
    return tuple(values)


def build_candidate_source_ledger(
    *,
    generation_id: str,
    staged_ledger_content_sha256: str,
    physical_ledger_sha256: str,
    accepted_book_generation_receipt_sha256: str,
    current_refit_receipt_sha256: str,
    current_refit_execution_sha256: str,
    current_cutoff_utc: str,
    records_descriptor: Mapping[str, Any],
    candidates: Sequence[replay.CandidateTrade],
    replayed_cell_ids: Sequence[str],
) -> dict[str, Any]:
    """Seal the genuine candidate stream without an authority claim."""

    normalized = tuple(
        sorted(
            (value.normalized() for value in candidates),
            key=lambda value: value.candidate_id,
        )
    )
    if not 1 <= len(normalized) <= replay.MAX_PORTFOLIO_REPLAY_CANDIDATES:
        _fail("CANDIDATE_COUNT_INVALID")
    if len({value.candidate_id for value in normalized}) != len(normalized):
        _fail("CANDIDATE_ID_DUPLICATE")
    records_sha = records_descriptor.get("sha256")
    records_size = records_descriptor.get("size_bytes")
    if not _is_sha(records_sha) or type(records_size) is not int or records_size <= 0:
        _fail("CANDIDATE_RECORD_DESCRIPTOR_INVALID")
    replayed = [str(value) for value in replayed_cell_ids]
    if replayed != sorted(replayed) or len(replayed) != len(set(replayed)):
        _fail("CANDIDATE_REPLAYED_CELL_ROSTER_INVALID")
    candidate_cells = sorted({value.cell_id for value in normalized})
    if not set(candidate_cells).issubset(set(replayed)):
        _fail("CANDIDATE_REPLAYED_CELL_ROSTER_INVALID")
    body = {
        "schema_version": CANDIDATE_LEDGER_VERSION,
        "generation_id": str(generation_id),
        "staged_ledger_content_sha256": staged_ledger_content_sha256,
        "physical_ledger_sha256": physical_ledger_sha256,
        "accepted_book_generation_receipt_sha256": (
            accepted_book_generation_receipt_sha256
        ),
        "current_refit_receipt_sha256": current_refit_receipt_sha256,
        "current_refit_execution_sha256": current_refit_execution_sha256,
        "current_cutoff_utc": str(current_cutoff_utc),
        "records_content_sha256": records_sha,
        "records_size_bytes": records_size,
        "candidate_count": len(normalized),
        "candidate_input_receipt": replay.portfolio_replay_candidate_roster_receipt(
            normalized
        ),
        "candidate_records_sha256": _candidate_records_sha256(normalized),
        "candidate_cell_ids": candidate_cells,
        "replayed_cell_ids": replayed,
    }
    for name in (
        "staged_ledger_content_sha256",
        "physical_ledger_sha256",
        "accepted_book_generation_receipt_sha256",
        "current_refit_receipt_sha256",
        "current_refit_execution_sha256",
    ):
        if not _is_sha(body[name]):
            _fail("CANDIDATE_LEDGER_HASH_INVALID", name)
    body["ledger_sha256"] = canonical_receipt_sha256(body)
    return body


def build_portfolio_source_ledger(
    *,
    generation_id: str,
    candidate_ledger: Mapping[str, Any],
    current_refit_execution_access_id: str,
    window: Mapping[str, Any],
    native_sources: Mapping[str, Any],
    valuation_sources: Mapping[str, Any],
) -> dict[str, Any]:
    """Seal native source facts consumed by the evidence producer."""

    if frozenset(candidate_ledger) != _CANDIDATE_LEDGER_FIELDS:
        _fail("CANDIDATE_LEDGER_FIELDS_INVALID")
    body = {
        "schema_version": PORTFOLIO_SOURCE_LEDGER_VERSION,
        "generation_id": str(generation_id),
        "staged_ledger_content_sha256": candidate_ledger[
            "staged_ledger_content_sha256"
        ],
        "physical_ledger_sha256": candidate_ledger["physical_ledger_sha256"],
        "accepted_book_generation_receipt_sha256": candidate_ledger[
            "accepted_book_generation_receipt_sha256"
        ],
        "current_refit_receipt_sha256": candidate_ledger[
            "current_refit_receipt_sha256"
        ],
        "current_refit_execution_sha256": candidate_ledger[
            "current_refit_execution_sha256"
        ],
        "candidate_records_sha256": candidate_ledger["candidate_records_sha256"],
        "current_refit_execution_access_id": str(current_refit_execution_access_id),
        "replayed_cell_ids": list(candidate_ledger["replayed_cell_ids"]),
        "candidate_cell_ids": list(candidate_ledger["candidate_cell_ids"]),
        "window": dict(window),
        "native_sources": dict(native_sources),
        "valuation_sources": dict(valuation_sources),
    }
    if not body["current_refit_execution_access_id"].strip():
        _fail("PORTFOLIO_SOURCE_ACCESS_ID_MISSING")
    _canonical_bytes(body)
    body["source_ledger_sha256"] = canonical_receipt_sha256(body)
    return body


def spool_current_refit_replay_capture(
    sink: SymbolAuthoritySink,
    *,
    staged_ledger_content_sha256: str,
    physical_ledger_sha256: str,
    accepted_book_generation_receipt_sha256: str,
    current_refit_receipt_sha256: str,
    current_refit_execution_sha256: str,
    current_cutoff_utc: str,
    candidates: Sequence[replay.CandidateTrade],
    replayed_cell_ids: Sequence[str],
    current_refit_execution_access_id: str,
    window: Mapping[str, Any],
    native_sources: Mapping[str, Any],
    valuation_sources: Mapping[str, Any],
) -> dict[str, Any]:
    """Spool paths first, then seal both small binding ledgers."""

    if type(sink) is not SymbolAuthoritySink:
        _fail("AUTHORITY_SINK_INVALID")
    values = tuple(
        sorted(
            (value.normalized() for value in candidates),
            key=lambda value: value.candidate_id,
        )
    )

    def chunks() -> Iterable[bytes]:
        for candidate in values:
            yield (
                _canonical_bytes(replay.current_refit_candidate_record(candidate))
                + b"\n"
            )

    records = sink.put_opaque(
        "current-refit/replay/candidate-records",
        ART_CANDIDATE_RECORDS,
        chunks(),
        format="canonical-jsonl",
    )
    candidate_ledger = build_candidate_source_ledger(
        generation_id=sink.context.generation_id,
        staged_ledger_content_sha256=staged_ledger_content_sha256,
        physical_ledger_sha256=physical_ledger_sha256,
        accepted_book_generation_receipt_sha256=(
            accepted_book_generation_receipt_sha256
        ),
        current_refit_receipt_sha256=current_refit_receipt_sha256,
        current_refit_execution_sha256=current_refit_execution_sha256,
        current_cutoff_utc=current_cutoff_utc,
        records_descriptor=records["descriptor"],
        candidates=values,
        replayed_cell_ids=replayed_cell_ids,
    )
    candidate_entry = sink.put_json(
        "current-refit/replay/candidate-ledger",
        ART_CANDIDATE_LEDGER,
        candidate_ledger,
    )
    source_ledger = build_portfolio_source_ledger(
        generation_id=sink.context.generation_id,
        candidate_ledger=candidate_ledger,
        current_refit_execution_access_id=current_refit_execution_access_id,
        window=window,
        native_sources=native_sources,
        valuation_sources=valuation_sources,
    )
    source_entry = sink.put_json(
        "current-refit/replay/source-ledger",
        ART_PORTFOLIO_SOURCE_LEDGER,
        source_ledger,
    )
    return {
        "candidate_records": records,
        "candidate_ledger": candidate_entry,
        "portfolio_source_ledger": source_entry,
    }


def _validated_candidate_ledger(
    body: Mapping[str, Any],
    *,
    generation_id: str,
    staged_content_sha: str,
    physical_ledger_sha: str,
    accepted_receipt_sha: str,
    current_receipt_sha: str,
    current_execution_sha: str,
    records: CapturedArtifact,
    candidates: Sequence[replay.CandidateTrade],
) -> dict[str, Any]:
    if frozenset(body) != _CANDIDATE_LEDGER_FIELDS:
        _fail("CANDIDATE_LEDGER_FIELDS_INVALID")
    unsigned = dict(body)
    declared = unsigned.pop("ledger_sha256", None)
    if declared != canonical_receipt_sha256(unsigned):
        _fail("CANDIDATE_LEDGER_SEAL_INVALID")
    expected = {
        "schema_version": CANDIDATE_LEDGER_VERSION,
        "generation_id": generation_id,
        "staged_ledger_content_sha256": staged_content_sha,
        "physical_ledger_sha256": physical_ledger_sha,
        "accepted_book_generation_receipt_sha256": accepted_receipt_sha,
        "current_refit_receipt_sha256": current_receipt_sha,
        "current_refit_execution_sha256": current_execution_sha,
        "records_content_sha256": records.content_sha256,
        "records_size_bytes": records.size_bytes,
        "candidate_count": len(candidates),
    }
    for name, value in expected.items():
        if body.get(name) != value:
            _fail("CANDIDATE_LEDGER_BINDING_MISMATCH", name)
    normalized = sorted(
        (candidate.normalized() for candidate in candidates),
        key=lambda x: x.candidate_id,
    )
    if body.get(
        "candidate_input_receipt"
    ) != replay.portfolio_replay_candidate_roster_receipt(normalized):
        _fail("CANDIDATE_LEDGER_INPUT_RECEIPT_MISMATCH")
    if body.get("candidate_records_sha256") != _candidate_records_sha256(normalized):
        _fail("CANDIDATE_LEDGER_RECORD_HASH_MISMATCH")
    candidate_cells = sorted({value.cell_id for value in normalized})
    replayed = body.get("replayed_cell_ids")
    if (
        body.get("candidate_cell_ids") != candidate_cells
        or not isinstance(replayed, list)
        or replayed != sorted(replayed)
        or len(replayed) != len(set(replayed))
        or not set(candidate_cells).issubset(set(replayed))
    ):
        _fail("CANDIDATE_LEDGER_CELL_ROSTER_INVALID")
    return dict(body)


def _validated_portfolio_source(
    body: Mapping[str, Any],
    *,
    generation_id: str,
    staged_content_sha: str,
    physical_ledger_sha: str,
    accepted_receipt_sha: str,
    current_receipt_sha: str,
    current_execution_sha: str,
    candidate_ledger: Mapping[str, Any],
) -> dict[str, Any]:
    if frozenset(body) != _PORTFOLIO_SOURCE_FIELDS:
        _fail("PORTFOLIO_SOURCE_LEDGER_FIELDS_INVALID")
    unsigned = dict(body)
    declared = unsigned.pop("source_ledger_sha256", None)
    if declared != canonical_receipt_sha256(unsigned):
        _fail("PORTFOLIO_SOURCE_LEDGER_SEAL_INVALID")
    expected = {
        "schema_version": PORTFOLIO_SOURCE_LEDGER_VERSION,
        "generation_id": generation_id,
        "staged_ledger_content_sha256": staged_content_sha,
        "physical_ledger_sha256": physical_ledger_sha,
        "accepted_book_generation_receipt_sha256": accepted_receipt_sha,
        "current_refit_receipt_sha256": current_receipt_sha,
        "current_refit_execution_sha256": current_execution_sha,
        "candidate_records_sha256": candidate_ledger["candidate_records_sha256"],
        "replayed_cell_ids": candidate_ledger["replayed_cell_ids"],
        "candidate_cell_ids": candidate_ledger["candidate_cell_ids"],
    }
    for name, value in expected.items():
        if body.get(name) != value:
            _fail("PORTFOLIO_SOURCE_LEDGER_BINDING_MISMATCH", name)
    if not str(body.get("current_refit_execution_access_id") or "").strip():
        _fail("PORTFOLIO_SOURCE_ACCESS_ID_MISSING")
    for name in ("window", "native_sources", "valuation_sources"):
        if not isinstance(body.get(name), Mapping):
            _fail("PORTFOLIO_SOURCE_LEDGER_OBJECT_INVALID", name)
    return dict(body)


class SealedPortfolioSourceLedgerProducer:
    """Packages frozen source facts around coordinator-recomputed replay output."""

    def __init__(
        self,
        source_ledger: Mapping[str, Any],
        candidate_ledger: Mapping[str, Any],
        frozen_ledger: _FrozenFile,
    ) -> None:
        if (
            frozenset(source_ledger) != _PORTFOLIO_SOURCE_FIELDS
            or frozenset(candidate_ledger) != _CANDIDATE_LEDGER_FIELDS
            or type(frozen_ledger) is not _FrozenFile
        ):
            _fail("PORTFOLIO_SOURCE_PRODUCER_INPUT_INVALID")
        source_unsigned = dict(source_ledger)
        source_seal = source_unsigned.pop("source_ledger_sha256", None)
        candidate_unsigned = dict(candidate_ledger)
        candidate_seal = candidate_unsigned.pop("ledger_sha256", None)
        if source_seal != canonical_receipt_sha256(
            source_unsigned
        ) or candidate_seal != canonical_receipt_sha256(candidate_unsigned):
            _fail("PORTFOLIO_SOURCE_PRODUCER_SEAL_INVALID")
        for name in (
            "generation_id",
            "staged_ledger_content_sha256",
            "physical_ledger_sha256",
            "accepted_book_generation_receipt_sha256",
            "current_refit_receipt_sha256",
            "current_refit_execution_sha256",
            "candidate_records_sha256",
            "replayed_cell_ids",
            "candidate_cell_ids",
        ):
            if source_ledger.get(name) != candidate_ledger.get(name):
                _fail("PORTFOLIO_SOURCE_PRODUCER_BINDING_MISMATCH", name)
        if source_ledger.get("staged_ledger_content_sha256") != frozen_ledger.sha256:
            _fail("PORTFOLIO_SOURCE_PRODUCER_STAGED_LEDGER_MISMATCH")
        self._source = json.loads(_canonical_bytes(dict(source_ledger)))
        self._candidates = json.loads(_canonical_bytes(dict(candidate_ledger)))
        self._frozen_ledger = frozen_ledger

    def __call__(self, request: Any) -> dict[str, Any]:
        self._frozen_ledger.assert_unchanged("STAGED_LEDGER_CHANGED_AFTER_CAPTURE")
        try:
            body = request.to_dict()
        except Exception as exc:
            raise CurrentRefitProductionCaptureError(
                "PORTFOLIO_EVIDENCE_REQUEST_INVALID"
            ) from exc
        required = {
            "schema_version",
            "roster",
            "current_refit_successor",
            "scenarios",
            "expected_lineage",
            "expected_cost_contract",
            "expected_policies",
            "authority_claimed",
        }
        if (
            not isinstance(body, Mapping)
            or set(body) != required
            or body.get("authority_claimed") is not False
        ):
            _fail("PORTFOLIO_EVIDENCE_REQUEST_INVALID")
        roster = body.get("roster")
        successor = body.get("current_refit_successor")
        lineage = body.get("expected_lineage")
        if not all(
            isinstance(value, Mapping) for value in (roster, successor, lineage)
        ):
            _fail("PORTFOLIO_EVIDENCE_REQUEST_INVALID")
        checks = {
            "physical_ledger_sha256": self._source["physical_ledger_sha256"],
            "candidate_records_sha256": self._source["candidate_records_sha256"],
        }
        if roster.get("physical_ledger_sha256") != checks["physical_ledger_sha256"]:
            _fail("PORTFOLIO_SOURCE_ROSTER_LEDGER_MISMATCH")
        if lineage.get("physical_ledger_sha256") != checks["physical_ledger_sha256"]:
            _fail("PORTFOLIO_SOURCE_LINEAGE_LEDGER_MISMATCH")
        if (
            successor.get("candidate_records_sha256")
            != checks["candidate_records_sha256"]
        ):
            _fail("PORTFOLIO_SOURCE_CANDIDATE_RECORD_MISMATCH")
        if (
            successor.get("candidate_records") is None
            or _canonical_list_sha256(successor["candidate_records"])
            != checks["candidate_records_sha256"]
        ):
            _fail("PORTFOLIO_SOURCE_CANDIDATE_RECORD_MISMATCH")
        if (
            sorted(roster.get("tradeable_cell_ids") or [])
            != self._source["replayed_cell_ids"]
        ):
            _fail("PORTFOLIO_SOURCE_REPLAYED_CELL_MISMATCH")
        current_lineage = roster.get("current_refit_lineage")
        if not isinstance(current_lineage, Mapping):
            _fail("PORTFOLIO_SOURCE_CURRENT_LINEAGE_MISSING")
        expected_lineage = {
            "accepted_book_generation_receipt_sha256": self._source[
                "accepted_book_generation_receipt_sha256"
            ],
            "current_refit_receipt_sha256": self._source[
                "current_refit_receipt_sha256"
            ],
            "current_refit_execution_sha256": self._source[
                "current_refit_execution_sha256"
            ],
        }
        for name, value in expected_lineage.items():
            if current_lineage.get(name) != value:
                _fail("PORTFOLIO_SOURCE_CURRENT_LINEAGE_MISMATCH", name)
        replay_body = {
            "candidate_schema_version": replay.PORTFOLIO_REPLAY_CANDIDATE_SCHEMA_VERSION,
            "portfolio_schema_version": replay.PORTFOLIO_REPLAY_SCHEMA_VERSION,
            "window": self._source["window"],
            "partial_run": False,
            "allow_cell_errors": False,
            "cell_generation_errors": [],
            "prelockbox_source_status": "PROCESS_VALIDATED_CURRENT_REFIT",
            "evidence_only_status_allowlist": ["PROCESS_VALIDATED_CURRENT_REFIT"],
            "current_refit_execution_access_id": self._source[
                "current_refit_execution_access_id"
            ],
            "replayed_cell_ids": self._source["replayed_cell_ids"],
            "candidate_cell_ids": self._source["candidate_cell_ids"],
            "current_refit_roster_sha256": roster.get("roster_sha256"),
            "cost_contract": body["expected_cost_contract"],
            "mark_cost_contract": replay.portfolio_mark_cost_contract(
                body["expected_cost_contract"]
            ),
            "native_sources": self._source["native_sources"],
            "valuation_sources": self._source["valuation_sources"],
            "current_refit_successor": successor,
            "scenarios": body["scenarios"],
        }
        return replay.seal_portfolio_replay_evidence(
            {
                "verdict": "PASS",
                "failures": [],
                "lineage": lineage,
                "roster": roster,
                "replay": replay_body,
            }
        )


class CurrentRefitProductionAdapter:
    """Build a coordinator product lazily after historical causal PASS.

    Construction intentionally performs no filesystem access.  The first raw
    evidence read occurs only when the coordinator invokes this object with a
    causal-pass-bound ``CurrentRefitRequest``.
    """

    def __init__(self, inputs: ProductionCaptureInputs) -> None:
        if type(inputs) is not ProductionCaptureInputs:
            _fail("PRODUCTION_CAPTURE_INPUTS_INVALID")
        self.inputs = inputs

    def __call__(self, request: Any) -> Any:
        from pm_production_authority_coordinator import CurrentRefitProduct

        try:
            request_body = request.to_dict()
        except Exception as exc:
            raise CurrentRefitProductionCaptureError(
                "CURRENT_REFIT_REQUEST_INVALID"
            ) from exc
        expected_request_fields = {
            "schema_version",
            "historical_run_receipt_sha256",
            "historical_process_decision_sha256",
            "historical_book_receipt_sha256",
            "authority_claimed",
        }
        if (
            not isinstance(request_body, Mapping)
            or set(request_body) != expected_request_fields
            or request_body.get("authority_claimed") is not False
            or not _is_sha(request_body.get("historical_process_decision_sha256"))
        ):
            _fail("CURRENT_REFIT_REQUEST_INVALID")

        artifacts = _verified_artifacts(self.inputs)
        root = Path(self.inputs.spool_root).resolve(strict=True)
        book_body = _owned_book(
            _artifact_json(root, _one_type(artifacts, ART_CURRENT_BOOK))
        )
        accepted = _artifact_json(root, _one_type(artifacts, ART_ACCEPTED_BOOK))
        accepted_payload, accepted_sha = open_typed_receipt(
            accepted,
            expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
        )
        refit = _artifact_json(root, _one_type(artifacts, ART_REFIT_RECEIPT))
        refit_payload, refit_sha = open_typed_receipt(
            refit,
            expected_receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
        )
        run = refit_payload.get("execution")
        authorization = refit_payload.get("authorization")
        if not isinstance(run, Mapping) or not isinstance(authorization, Mapping):
            _fail("CURRENT_REFIT_RECEIPT_BODY_INVALID")
        if (
            accepted_payload.get("historical_process_decision_sha256")
            != request_body["historical_process_decision_sha256"]
            or accepted_payload.get("book_multiplicity_receipt_sha256")
            != book_body["receipt_sha256"]
            or accepted_payload.get("full_cell_outcome_roster_sha256")
            != book_body.get("book_generation_sha256")
            or authorization.get("accepted_book_generation_receipt_sha256")
            != accepted_sha
        ):
            _fail("CURRENT_REFIT_CAPTURE_LINEAGE_MISMATCH")

        manifest_path = assemble_raw_current_refit_manifest(
            current_refit_receipt=refit,
            artifacts=artifacts,
            artifact_root=root,
            output_root=self.inputs.output_root,
        )
        derived_ledger = derive_physical_current_refit_ledger(
            current_book_decision=book_body,
            accepted_book_generation_receipt=accepted,
            current_refit_receipt=refit,
            raw_manifest_path=manifest_path,
            artifact_root=root,
            output_root=self.inputs.output_root,
            required_valid_until_utc=self.inputs.required_valid_until_utc,
        )
        frozen_ledger, observed_ledger, physical_sha = _freeze_staged_ledger(
            derived_ledger.path
        )
        if (
            observed_ledger != derived_ledger.payload
            or frozen_ledger.sha256 != derived_ledger.content_sha256
            or physical_sha != derived_ledger.physical_ledger_sha256
        ):
            _fail("PHYSICAL_LEDGER_DERIVATION_CHANGED")
        candidate_records = _one_type(artifacts, ART_CANDIDATE_RECORDS)
        candidate_ledger_body = _artifact_json(
            root, _one_type(artifacts, ART_CANDIDATE_LEDGER)
        )
        candidate_count = candidate_ledger_body.get("candidate_count")
        if type(candidate_count) is not int:
            _fail("CANDIDATE_COUNT_INVALID")
        candidates = _load_candidate_records(
            root, candidate_records, expected_count=candidate_count
        )
        candidate_ledger = _validated_candidate_ledger(
            candidate_ledger_body,
            generation_id=self.inputs.generation_id,
            staged_content_sha=frozen_ledger.sha256,
            physical_ledger_sha=physical_sha,
            accepted_receipt_sha=accepted_sha,
            current_receipt_sha=refit_sha,
            current_execution_sha=str(refit_payload.get("execution_sha256")),
            records=candidate_records,
            candidates=candidates,
        )
        source_body = _artifact_json(
            root, _one_type(artifacts, ART_PORTFOLIO_SOURCE_LEDGER)
        )
        source = _validated_portfolio_source(
            source_body,
            generation_id=self.inputs.generation_id,
            staged_content_sha=frozen_ledger.sha256,
            physical_ledger_sha=physical_sha,
            accepted_receipt_sha=accepted_sha,
            current_receipt_sha=refit_sha,
            current_execution_sha=str(refit_payload.get("execution_sha256")),
            candidate_ledger=candidate_ledger,
        )
        regime_bundle = _artifact_json(root, _one_type(artifacts, ART_REGIME_BUNDLE))
        detector_rows = tuple(
            dict(item["payload"]["regime_detector_record"])
            for item in regime_bundle.get("regime_fits", [])
        )
        if not detector_rows:
            _fail("CURRENT_REFIT_DETECTOR_ROSTER_EMPTY")

        return CurrentRefitProduct(
            current_book_decision=book_body,
            accepted_book_generation_receipt=accepted,
            current_refit_receipt=refit,
            raw_manifest_path=manifest_path,
            artifact_root=root,
            detector_ledger_records=detector_rows,
            replay_candidates=candidates,
            replayed_cell_ids=tuple(candidate_ledger["replayed_cell_ids"]),
            portfolio_evidence_producer=SealedPortfolioSourceLedgerProducer(
                source, candidate_ledger, frozen_ledger
            ),
            staged_ledger_path=derived_ledger.path,
            staged_ledger=derived_ledger.payload,
            staged_ledger_content_sha256=derived_ledger.content_sha256,
            physical_ledger_sha256=derived_ledger.physical_ledger_sha256,
        )


def production_capture_requirements() -> dict[str, Any]:
    return {
        "schema_version": CAPTURE_CONTRACT_VERSION,
        "generation_artifact_types": sorted(REQUIRED_GENERATION_ARTIFACT_TYPES),
        "cell_artifact_types": sorted(REQUIRED_CELL_ARTIFACT_TYPES),
        "capture_seams": [
            "native-prefix-freeze: bars then source snapshot prebound to raw descriptor",
            "shared-regime-fit: fit receipts and detector records",
            "book-v9-close: full 2400-cell decision, winner entries, accepted receipt",
            "current-refit-close: typed execution receipt and raw bundles",
            "management-replay-close: candidate JSONL and sealed source ledger",
        ],
        "current_search_spool_is_sufficient": False,
    }


__all__ = [
    "ART_ACCEPTED_BOOK",
    "ART_BARS",
    "ART_BOOK_ENTRY",
    "ART_CANDIDATE_LEDGER",
    "ART_CANDIDATE_RECORDS",
    "ART_CURRENT_BOOK",
    "ART_HORIZON_BUNDLE",
    "ART_HORIZON_PROCESS",
    "ART_PORTFOLIO_SOURCE_LEDGER",
    "ART_RECIPE",
    "ART_REFIT_RECEIPT",
    "ART_REGIME_BUNDLE",
    "ART_SOURCE",
    "ART_SOURCE_BUNDLE",
    "ART_TRIAL",
    "ART_WINNER_PROCESS",
    "CANDIDATE_LEDGER_VERSION",
    "CAPTURE_CONTRACT_VERSION",
    "CapturedArtifact",
    "DerivedPhysicalCurrentLedger",
    "CurrentRefitProductionAdapter",
    "CurrentRefitProductionCaptureError",
    "PORTFOLIO_SOURCE_LEDGER_VERSION",
    "ProductionCaptureInputs",
    "SealedPortfolioSourceLedgerProducer",
    "assemble_raw_current_refit_manifest",
    "build_candidate_source_ledger",
    "build_portfolio_source_ledger",
    "derive_physical_current_refit_ledger",
    "production_capture_requirements",
    "spool_current_refit_replay_capture",
]
