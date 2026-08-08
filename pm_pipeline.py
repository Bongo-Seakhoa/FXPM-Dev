"""
FX Portfolio Manager - Pipeline Module
=======================================

Main pipeline for the Portfolio Manager that handles:
- Strategy selection: Test all strategies, select best for each symbol
- Timeframe selection: Evaluate multiple timeframes, select optimal
- Hyperparameter optimization: Optuna TPE-based Bayesian optimization
- Retrain period selection: Determine optimal retraining frequency
- Validation: Out-of-sample testing with robustness checks
- Continuous loop: Retrain when period expires

Version: 3.1 (Portfolio Manager with Optuna TPE Optimization)
"""

import csv
import json
import hashlib
import inspect
import logging
import math
import os
import shutil
import stat
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace

import pandas as pd
import numpy as np
from scipy.stats import norm as _scipy_norm

from pm_core import (
    PipelineConfig,
    DataLoader,
    FeatureComputer,
    DataSplitter,
    Backtester,
    StrategyScorer,
    Timer,
    get_instrument_spec,
    InstrumentSpec
)
from pm_evidence import (
    CALENDAR_SPLIT_MODE,
    EVIDENCE_CONTRACT_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    EvidenceVerdict,
    build_calendar_boundaries,
    calendar_window_meta,
    derive_purge_duration,
    ensure_utc_index,
    latest_closed_bar_end,
    trim_to_closed_bars,
    validate_timeframe_evidence,
)
from pm_enhancement_seams import EnhancementSeams, create_default_enhancement_seams
from pm_strategies import (
    StrategyRegistry,
    BaseStrategy,
    _STRATEGY_MIGRATION,
)
from pm_position import PositionConfig, PositionManager
from pm_regime import (
    load_regime_params,
    detector_code_sha256,
    load_regime_params_cell,
    recompute_train_regime_support_digest,
    materialize_regime_params_from_symbol_config,
    regime_params_provenance_path,
    validate_regime_params_provenance,
    validated_regime_params_for_timeframe,
)
from pm_order_governance import candidate_policy_names, make_policy, policy_name_from_artifact
from pm_fairness_audit import build_fairness_evidence, build_fairness_report
from pm_regime_audit import audit_regime_profitability
from pm_statistics import (
    DAILY_NET_MTM_CONTRACT_VERSION,
    ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION,
    EXECUTION_COST_TIMING_CONTRACT,
    LockboxAccessRegistry,
    STATISTICS_CONTRACT_VERSION,
    SWAP_ACCRUAL_TIMING_CONTRACT_VERSION,
    aligned_cell_pbo,
    aligned_validation_dsr_population,
    block_bootstrap_evidence,
    deflated_sharpe_evidence,
)
from pm_model_lifecycle import (
    CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    EDGE_CHALLENGER_ROLE,
    build_edge_challenger_lineage,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
    build_prelockbox_champion_lineage,
    canonical_receipt_sha256,
    compute_current_refit_cell_id,
    compute_current_winner_roster_sha256,
    open_typed_receipt,
    temporal_role_block_reason,
)
from pm_current_refit_execution import (
    CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    RAW_CURRENT_REFIT_EXECUTOR_VERSION,
    RAW_CURRENT_REFIT_REPLAY_REQUEST_VERSION,
    RAW_CURRENT_REFIT_REPLAY_RESULT_VERSION,
    validate_current_regime_params_bundle,
)
from pm_current_refit_parameters import (
    CurrentRefitParameterCodecError,
    canonical_parameter_bytes,
    canonicalize_parameter_mapping,
    decode_parameter_mapping,
    parameter_values_equal,
)
from pm_book_multiplicity import BOOK_MULTIPLICITY_CONTRACT_VERSION
from pm_two_phase_lockbox import FROZEN_PRELOCKBOX_STATUS
from pm_native_activation import (
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_PRODUCTION_TIMEFRAMES,
    NATIVE_SOURCE_UNAVAILABLE,
    resolve_active_native_universe,
    validate_native_cell_exclusion_no_trade,
    validate_native_exclusion_no_trade_grid,
)
from pm_memory_governor import (
    MemoryGovernor,
    MemoryTask,
    is_memory_failure,
)
from pm_search_outcomes import (
    InvalidStrategyParameterError,
    SearchInfrastructureError,
    SearchOutcomeKind,
    SearchStage,
    TrialOutcome,
    aggregate_family_cell_outcomes,
    classify_search_exception,
)
from pm_trade_power import (
    REQUIRED_ENTRY_SLIPPAGE_CONTRACT,
    REQUIRED_SWAP_ACCRUAL_CONTRACT,
    REQUIRED_SWAP_PROJECTION_CONTRACT,
    REQUIRED_TRADE_RECORD_SCHEMA,
    TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
    TRADE_POWER_SCHEMA_VERSION,
    TRADE_POWER_DECISION_SCHEMA_VERSION,
    TRADE_POWER_PARTITION_VERSION,
    TRADE_SAMPLE_SCHEMA_VERSION,
    TradePowerDecision,
    TradePowerPolicy,
    TradePowerRequirement,
    build_trade_power_multiplicity_receipt,
    build_trade_power_partition_contract,
    calibrate_train_trade_power,
    derive_trade_power_requirement,
    evaluate_trade_power_partition,
    requirement_source_provenance_valid,
    trade_sample_identity_sha256,
)

# Import Optuna TPE optimizer
try:
    from pm_optuna import (
        OptunaTPEOptimizer,
        OptunaConfig,
        OptimizationResult,
        OptimizationStats,
        is_optuna_available,
        get_optimization_method,
        create_optimizer,
        OPTUNA_AVAILABLE
    )
except ImportError:
    OPTUNA_AVAILABLE = False
    OptunaTPEOptimizer = None
    OptunaConfig = None
    OptimizationResult = None
    def is_optuna_available():
        return False
    def get_optimization_method():
        return "Grid Search (fallback)"
    def create_optimizer(*args, **kwargs):
        return None

# Progress bar (optional)
try:
    from tqdm import tqdm
    _TQDM = True
except ImportError:
    _TQDM = False

# Configure module logger
logger = logging.getLogger(__name__)


def _optimization_ledger_lock_path(filepath: Any) -> Path:
    ledger_path = Path(filepath)
    if not ledger_path.is_absolute():
        ledger_path = Path.cwd() / ledger_path
    ledger_path = ledger_path.resolve()
    return ledger_path.with_name(f".{ledger_path.name}.generation.lock")
PIPELINE_ARTIFACT_VERSION = "2026-07-13-trade-power-derived-binding-deployment-v7"

# Runtime-only optimizer resource contract.  These values control scheduling
# and failure isolation only; they never alter bars, features, trials, scoring,
# or promotion decisions and are intentionally absent from PipelineConfig.
OPTIMIZER_RECEIPT_SCHEMA = "fxpm.optimizer-symbol-receipt.v1"
OPTIMIZER_RECEIPT_POINTER_SCHEMA = "fxpm.optimizer-symbol-receipt-pointer.v1"
OPTIMIZER_DATA_STATE_RECEIPT_SCHEMA = "fxpm.optimizer-symbol-data-state.v1"

CURRENT_REFIT_RAW_EXECUTOR_VERSION = RAW_CURRENT_REFIT_EXECUTOR_VERSION
CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION = "fxpm.current-refit-source-snapshot.v1"
CURRENT_REFIT_SOURCE_BUNDLE_VERSION = "fxpm.current-refit-source-bundle.v1"
CURRENT_REFIT_BAR_FILE_VERSION = "fxpm.current-refit-bars.csv.v1"
CURRENT_REFIT_PROCESS_VERSION = "fxpm.current-refit-winner-process.v1"
CURRENT_REFIT_HORIZON_VERSION = "fxpm.current-refit-horizon-process.v2"
CURRENT_REFIT_HORIZON_BUNDLE_VERSION = "fxpm.current-refit-horizon-bundle.v1"
CURRENT_REFIT_TRIAL_VERSION = "fxpm.current-refit-trial-definition.v1"
CURRENT_REFIT_RECIPE_VERSION = "fxpm.current-refit-recipe.v1"
CURRENT_REFIT_BOOK_ENTRY_VERSION = "fxpm.book-multiplicity-cell.v9"
CURRENT_REFIT_PARAMETER_VERSION = "fxpm.current-refit-parameters.v1"
CURRENT_REFIT_SPECIALIST_VERSION = "fxpm.current-refit-specialist.v1"
CURRENT_REFIT_CELL_TRACE_VERSION = "fxpm.current-refit-cell-trace.v1"
CURRENT_REFIT_OUTPUT_BOOK_VERSION = "fxpm.current-refit-output-book.v1"
CURRENT_REFIT_EXECUTION_TRACE_VERSION = "fxpm.current-refit-execution-trace.v1"
CURRENT_PROCESS_BOOK_ARTIFACT_VERSION = "fxpm.current-process-book-receipt.v1"

_CURRENT_REFIT_MAX_JSON_BYTES = 16 * 1024 * 1024
_CURRENT_REFIT_MAX_BAR_ROWS = 10_000_000
_CURRENT_REFIT_BAR_COLUMNS = (
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
)
_CURRENT_REFIT_DESCRIPTOR_FIELDS = {
    "artifact_id",
    "relative_path",
    "size_bytes",
    "content_sha256",
    "format_version",
}
_CURRENT_REFIT_REQUEST_FIELDS = {
    "current_refit_cell_id",
    "symbol",
    "timeframe",
    "regime",
    "strategy_id",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "artifact_ids",
}
_CURRENT_REFIT_CELL_ARTIFACT_ROLES = {
    "source_snapshot",
    "winner_process",
    "horizon_process",
    "trial_definition",
    "refit_recipe",
    "winner_book_entry",
}
_CURRENT_REFIT_BUNDLE_ROLES = {
    "accepted_book_generation",
    "source_snapshot_bundle",
    "regime_params_bundle",
    "horizon_decisions_bundle",
}
_CURRENT_REFIT_RESULT_CELL_FIELDS = {
    "current_refit_cell_id",
    "parameter_sha256",
    "output_artifact_sha256",
    "execution_trace_sha256",
    "source_snapshot_sha256",
    "winner_horizon_process_sha256",
    "winner_trial_definition_sha256",
    "winner_refit_recipe_sha256",
    "regime_fit_receipt_sha256",
    "regime_detector_record_sha256",
    "regime_detector_parameters_sha256",
    "regime_detector_code_sha256",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
}
_CURRENT_REFIT_SOURCE_FIELDS = {
    "schema_version",
    "symbol",
    "timeframe",
    "fit_start_utc",
    "fit_end_utc",
    "latest_closed_bar_utc",
    "first_bar_utc",
    "last_bar_utc",
    "bar_count",
    "bars_artifact_id",
    "bars_content_sha256",
    "activation_id",
    "activation_manifest_sha256",
    "native_source_sha256",
    "source_contract_sha256",
    "continuity_receipt_sha256",
    "columns",
    "timestamp_encoding",
}
_CURRENT_REFIT_PROCESS_FIELDS = {
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
_CURRENT_REFIT_HORIZON_FIELDS = {
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
_CURRENT_REFIT_TRIAL_FIELDS = {
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
_CURRENT_REFIT_RECIPE_FIELDS = {
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
_CURRENT_REFIT_BOOK_ENTRY_FIELDS = {
    "ordinal",
    "cell_id",
    "symbol",
    "timeframe",
    "regime",
    "source_group_id",
    "source_group_decision_sha256",
    "regime_receipt_sha256",
    "regime_detector_record_sha256",
    "train_regime_support_receipt_sha256",
    "historical_state",
    "state_reason",
    "historical_outcome_receipt_sha256",
    "winner_request_sha256",
    "winner_attempt_ledger_payload_sha256",
    "winner_decision_sha256",
    "winner_evidence_ledger_payload_sha256",
    "local_process_candidate_id",
    "local_process_sha256",
    "process_state_payload",
    "process_state_receipt_sha256",
    "within_cell_attempted_trials_exact",
    "effective_trials",
    "marginal_p_value",
    "adjusted_q_value",
    "by_qualified",
    "historical_local_candidate_pending_causal",
    "historical_book_entry_sha256",
}
_CURRENT_REFIT_SOURCE_BUNDLE_FIELDS = {
    "schema_version",
    "current_cutoff_utc",
    "entries",
}
_CURRENT_REFIT_SOURCE_BUNDLE_ENTRY_FIELDS = {
    "symbol",
    "timeframe",
    "source_snapshot_sha256",
}
_CURRENT_REFIT_HORIZON_BUNDLE_FIELDS = {
    "schema_version",
    "current_cutoff_utc",
    "entries",
}
_CURRENT_REFIT_HORIZON_BUNDLE_ENTRY_FIELDS = {
    "symbol",
    "timeframe",
    "regime",
    "horizon_process_sha256",
}
_CURRENT_REFIT_TIMEFRAME_ORDER = {
    timeframe: index
    for index, timeframe in enumerate(("M5", "M15", "M30", "H1", "H4", "D1"))
}


def _current_refit_canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _current_refit_sha256(value: Any) -> str:
    return hashlib.sha256(_current_refit_canonical_bytes(value)).hexdigest()


def _current_refit_is_sha(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _current_refit_exact(value: Any, fields: Set[str], code: str) -> Dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError(code)
    return value


def _current_refit_utc(value: Any, code: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        raise ValueError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        normalized = parsed.astimezone(timezone.utc).isoformat()
    except Exception as exc:
        raise ValueError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or normalized != value:
        raise ValueError(code)
    return value


def _current_refit_artifact_path(root: Path, descriptor: Mapping[str, Any]) -> Path:
    relative = descriptor["relative_path"]
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("CURRENT_REFIT_ARTIFACT_PATH_INVALID")
    path = (root / candidate).resolve(strict=True)
    path.relative_to(root)
    current = root
    for part in path.relative_to(root).parts:
        current = current / part
        info = current.lstat()
        attributes = int(getattr(info, "st_file_attributes", 0) or 0)
        if current.is_symlink() or attributes & 0x400:
            raise ValueError("CURRENT_REFIT_ARTIFACT_REPARSE_POINT")
    if not path.is_file():
        raise ValueError("CURRENT_REFIT_ARTIFACT_NOT_FILE")
    return path


def _current_refit_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _current_refit_read_json(
    root: Path,
    descriptor: Mapping[str, Any],
    *,
    format_version: str,
    maximum_bytes: int = _CURRENT_REFIT_MAX_JSON_BYTES,
) -> Dict[str, Any]:
    if set(descriptor) != _CURRENT_REFIT_DESCRIPTOR_FIELDS:
        raise ValueError("CURRENT_REFIT_DESCRIPTOR_FIELDS_INVALID")
    size = descriptor["size_bytes"]
    if (
        descriptor["format_version"] != format_version
        or type(size) is not int
        or not 0 < size <= maximum_bytes
        or not _current_refit_is_sha(descriptor["content_sha256"])
    ):
        raise ValueError("CURRENT_REFIT_DESCRIPTOR_INVALID")
    path = _current_refit_artifact_path(root, descriptor)
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno()).st_size
        raw = handle.read(maximum_bytes + 1)
        after = os.fstat(handle.fileno()).st_size
    if (
        before != size
        or after != size
        or len(raw) != size
        or hashlib.sha256(raw).hexdigest() != descriptor["content_sha256"]
    ):
        raise ValueError("CURRENT_REFIT_ARTIFACT_CHANGED")
    try:
        value = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError("CURRENT_REFIT_ARTIFACT_JSON_INVALID") from exc
    if type(value) is not dict or _current_refit_canonical_bytes(value) != raw:
        raise ValueError("CURRENT_REFIT_ARTIFACT_JSON_NONCANONICAL")
    return value


def _current_refit_strategy_code_sha256(strategy_id: str) -> str:
    if strategy_id not in StrategyRegistry.list_all():
        raise ValueError("CURRENT_REFIT_STRATEGY_UNKNOWN")
    strategy = StrategyRegistry.get(strategy_id)
    source_path = Path(inspect.getsourcefile(type(strategy)) or "").resolve(strict=True)
    return _current_refit_file_sha256(source_path)


def _current_refit_core_code_sha256() -> str:
    source_path = Path(inspect.getsourcefile(FeatureComputer) or "").resolve(strict=True)
    return _current_refit_file_sha256(source_path)


def _current_refit_callable_sha256(value: Any) -> str:
    source = inspect.getsource(value).replace("\r\n", "\n").encode("utf-8")
    return hashlib.sha256(source).hexdigest()


def _current_refit_feature_contract(strategy: BaseStrategy) -> str:
    return _current_refit_sha256(
        {
            "schema_version": "fxpm.current-refit-feature-contract.v1",
            "required_features": sorted(strategy.get_feature_request()),
            "minimum_bars": int(strategy.MIN_BARS),
        }
    )


def _current_refit_json_parameters(value: Any) -> Dict[str, Any]:
    try:
        normalized = canonicalize_parameter_mapping(value)
    except CurrentRefitParameterCodecError as exc:
        raise ValueError("CURRENT_REFIT_PARAMETERS_INVALID") from exc
    _current_refit_canonical_bytes(normalized)
    return normalized


def _current_refit_process_candidate_id(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_id: str,
    parameters: Mapping[str, Any],
    horizon_process_sha256: str,
) -> str:
    """Re-derive the search-time candidate identity from executable facts."""

    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "strategy_name": strategy_id,
        "parameters": dict(parameters),
        "horizon_process_sha256": horizon_process_sha256,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _current_refit_validate_bars(
    root: Path,
    source: Mapping[str, Any],
    descriptors: Mapping[str, Mapping[str, Any]],
) -> None:
    descriptor = descriptors.get(source["bars_artifact_id"])
    if descriptor is None or set(descriptor) != _CURRENT_REFIT_DESCRIPTOR_FIELDS:
        raise ValueError("CURRENT_REFIT_BAR_ARTIFACT_MISSING")
    if (
        descriptor["format_version"] != CURRENT_REFIT_BAR_FILE_VERSION
        or descriptor["content_sha256"] != source["bars_content_sha256"]
    ):
        raise ValueError("CURRENT_REFIT_BAR_ARTIFACT_SPLICE")
    path = _current_refit_artifact_path(root, descriptor)
    if (
        path.stat().st_size != descriptor["size_bytes"]
        or _current_refit_file_sha256(path) != descriptor["content_sha256"]
    ):
        raise ValueError("CURRENT_REFIT_BAR_ARTIFACT_CHANGED")
    expected_count = source["bar_count"]
    if type(expected_count) is not int or not 2 <= expected_count <= _CURRENT_REFIT_MAX_BAR_ROWS:
        raise ValueError("CURRENT_REFIT_BAR_COUNT_INVALID")
    old_limit = csv.field_size_limit()
    csv.field_size_limit(4096)
    count = 0
    first_timestamp = None
    previous_timestamp = None
    try:
        with path.open("r", encoding="utf-8", errors="strict", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != list(_CURRENT_REFIT_BAR_COLUMNS):
                raise ValueError("CURRENT_REFIT_BAR_COLUMNS_INVALID")
            for row in reader:
                if set(row) != set(_CURRENT_REFIT_BAR_COLUMNS):
                    raise ValueError("CURRENT_REFIT_BAR_ROW_INVALID")
                timestamp = _current_refit_utc(
                    row["timestamp_utc"], "CURRENT_REFIT_BAR_TIMESTAMP_INVALID"
                )
                parsed = datetime.fromisoformat(timestamp)
                if previous_timestamp is not None and parsed <= previous_timestamp:
                    raise ValueError("CURRENT_REFIT_BAR_ORDER_INVALID")
                if first_timestamp is None:
                    first_timestamp = timestamp
                previous_timestamp = parsed
                try:
                    open_price, high, low, close, volume = (
                        float(row[name])
                        for name in ("open", "high", "low", "close", "volume")
                    )
                except Exception as exc:
                    raise ValueError("CURRENT_REFIT_BAR_VALUE_INVALID") from exc
                if (
                    not all(
                        math.isfinite(item)
                        for item in (open_price, high, low, close, volume)
                    )
                    or min(open_price, high, low, close) <= 0.0
                    or low > min(open_price, close)
                    or high < max(open_price, close)
                    or high < low
                    or volume < 0.0
                ):
                    raise ValueError("CURRENT_REFIT_BAR_VALUE_INVALID")
                count += 1
                if count > expected_count:
                    raise ValueError("CURRENT_REFIT_BAR_COUNT_INVALID")
    finally:
        csv.field_size_limit(old_limit)
    last_timestamp = previous_timestamp.isoformat() if previous_timestamp else None
    if (
        count != expected_count
        or first_timestamp != source["first_bar_utc"]
        or last_timestamp != source["last_bar_utc"]
        or _current_refit_file_sha256(path) != descriptor["content_sha256"]
    ):
        raise ValueError("CURRENT_REFIT_BAR_SNAPSHOT_MISMATCH")


def _current_refit_validate_source(
    source: Any,
    *,
    request: Mapping[str, Any],
    root: Path,
    descriptors: Mapping[str, Mapping[str, Any]],
) -> str:
    source = _current_refit_exact(
        source, _CURRENT_REFIT_SOURCE_FIELDS, "CURRENT_REFIT_SOURCE_FIELDS_INVALID"
    )
    if (
        source["schema_version"] != CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION
        or source["symbol"] != request["symbol"]
        or source["timeframe"] != request["timeframe"]
        or source["fit_start_utc"] != request["fit_start_utc"]
        or source["fit_end_utc"] != request["fit_end_utc"]
        or source["latest_closed_bar_utc"] != request["latest_closed_bar_utc"]
        or source["columns"] != list(_CURRENT_REFIT_BAR_COLUMNS)
        or source["timestamp_encoding"] != "UTC_ISO8601_PLUS00"
        or not _current_refit_is_sha(source["bars_artifact_id"])
        or not _current_refit_is_sha(source["bars_content_sha256"])
        or type(source["activation_id"]) is not str
        or not 1 <= len(source["activation_id"]) <= 256
        or any(
            not _current_refit_is_sha(source[name])
            for name in (
                "activation_manifest_sha256",
                "native_source_sha256",
                "source_contract_sha256",
                "continuity_receipt_sha256",
            )
        )
    ):
        raise ValueError("CURRENT_REFIT_SOURCE_INVALID")
    fit_start = datetime.fromisoformat(
        _current_refit_utc(source["fit_start_utc"], "CURRENT_REFIT_SOURCE_TIME_INVALID")
    )
    fit_end = datetime.fromisoformat(
        _current_refit_utc(source["fit_end_utc"], "CURRENT_REFIT_SOURCE_TIME_INVALID")
    )
    first = datetime.fromisoformat(
        _current_refit_utc(source["first_bar_utc"], "CURRENT_REFIT_SOURCE_TIME_INVALID")
    )
    last = datetime.fromisoformat(
        _current_refit_utc(source["last_bar_utc"], "CURRENT_REFIT_SOURCE_TIME_INVALID")
    )
    latest = datetime.fromisoformat(
        _current_refit_utc(
            source["latest_closed_bar_utc"], "CURRENT_REFIT_SOURCE_TIME_INVALID"
        )
    )
    if not fit_start == first < last == latest <= fit_end:
        raise ValueError("CURRENT_REFIT_SOURCE_TIME_INVALID")
    _current_refit_validate_bars(root, source, descriptors)
    return _current_refit_sha256(source)


def _current_refit_validate_bundle(
    value: Any,
    *,
    fields: Set[str],
    entry_fields: Set[str],
    schema_version: str,
    current_cutoff_utc: str,
    kind: str,
) -> Tuple[Dict[str, Any], str]:
    bundle = _current_refit_exact(value, fields, f"CURRENT_REFIT_{kind}_FIELDS_INVALID")
    if (
        bundle["schema_version"] != schema_version
        or bundle["current_cutoff_utc"] != current_cutoff_utc
        or type(bundle["entries"]) is not list
        or not 1 <= len(bundle["entries"]) <= 2400
    ):
        raise ValueError(f"CURRENT_REFIT_{kind}_INVALID")
    entries = []
    for raw in bundle["entries"]:
        entry = _current_refit_exact(
            raw, entry_fields, f"CURRENT_REFIT_{kind}_ENTRY_INVALID"
        )
        if (
            type(entry["symbol"]) is not str
            or not 1 <= len(entry["symbol"]) <= 64
            or entry["timeframe"] not in _CURRENT_REFIT_TIMEFRAME_ORDER
            or not _current_refit_is_sha(
                entry[
                    "source_snapshot_sha256"
                    if kind == "SOURCE_BUNDLE"
                    else "horizon_process_sha256"
                ]
            )
        ):
            raise ValueError(f"CURRENT_REFIT_{kind}_ENTRY_INVALID")
        if kind == "HORIZON_BUNDLE" and entry["regime"] not in {
            "TREND",
            "RANGE",
            "BREAKOUT",
            "CHOP",
        }:
            raise ValueError("CURRENT_REFIT_HORIZON_BUNDLE_ENTRY_INVALID")
        entries.append(dict(entry))
    def sort_key(item: Mapping[str, Any]) -> Tuple[Any, ...]:
        return (
            item["symbol"],
            _CURRENT_REFIT_TIMEFRAME_ORDER[item["timeframe"]],
            item.get("regime", ""),
        )
    if entries != sorted(entries, key=sort_key):
        raise ValueError(f"CURRENT_REFIT_{kind}_ORDER_INVALID")
    identities = [
        (item["symbol"], item["timeframe"], item.get("regime")) for item in entries
    ]
    if len(set(identities)) != len(identities):
        raise ValueError(f"CURRENT_REFIT_{kind}_DUPLICATE")
    return dict(bundle), _current_refit_sha256(bundle)


def _current_refit_validate_trial(
    value: Any,
    *,
    strategy_id: str,
    process_sha256: str,
    horizon_sha256: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    trial = _current_refit_exact(
        value, _CURRENT_REFIT_TRIAL_FIELDS, "CURRENT_REFIT_TRIAL_FIELDS_INVALID"
    )
    parameters = _current_refit_json_parameters(trial["parameters"])
    base = StrategyRegistry.get(strategy_id)
    if (
        trial["schema_version"] != CURRENT_REFIT_TRIAL_VERSION
        or trial["strategy_id"] != strategy_id
        or trial["strategy_family"] != base.category.value
        or trial["process_sha256"] != process_sha256
        or trial["horizon_process_sha256"] != horizon_sha256
        or trial["feature_contract_sha256"] != _current_refit_feature_contract(base)
        or trial["indicator_contract_sha256"] != _current_refit_core_code_sha256()
        or type(trial["parameter_count"]) is not int
        or trial["parameter_count"] != len(parameters)
    ):
        raise ValueError("CURRENT_REFIT_TRIAL_INVALID")
    allowed_keys = set(base.get_default_params()) | set(base.get_param_grid())
    if not set(parameters) <= allowed_keys:
        raise ValueError("CURRENT_REFIT_TRIAL_PARAMETER_UNKNOWN")
    try:
        runtime_parameters = decode_parameter_mapping(
            parameters, base.get_default_params()
        )
        fitted = StrategyRegistry.get(strategy_id, **runtime_parameters)
    except Exception as exc:
        raise ValueError("CURRENT_REFIT_TRIAL_PARAMETERS_NONCANONICAL") from exc
    normalized = _current_refit_json_parameters(fitted.get_params())
    if canonical_parameter_bytes(normalized) != canonical_parameter_bytes(parameters):
        raise ValueError("CURRENT_REFIT_TRIAL_PARAMETERS_NONCANONICAL")
    defaults = base.get_default_params()
    grid = base.get_param_grid()
    for name, choices in grid.items():
        if name in parameters and not any(
            parameter_values_equal(parameters[name], choice) for choice in choices
        ):
            raise ValueError("CURRENT_REFIT_TRIAL_PARAMETER_OUTSIDE_PROCESS")
    for name, expected in defaults.items():
        if name not in grid and not parameter_values_equal(
            parameters.get(name), expected
        ):
            raise ValueError("CURRENT_REFIT_TRIAL_FIXED_DEFAULT_MISMATCH")
    return dict(trial), parameters


def _current_refit_validate_process(
    value: Any,
    *,
    strategy_id: str,
) -> Tuple[Dict[str, Any], str]:
    process = _current_refit_exact(
        value, _CURRENT_REFIT_PROCESS_FIELDS, "CURRENT_REFIT_PROCESS_FIELDS_INVALID"
    )
    strategy = StrategyRegistry.get(strategy_id)
    if (
        process["schema_version"] != CURRENT_REFIT_PROCESS_VERSION
        or process["strategy_id"] != strategy_id
        or process["strategy_family"] != strategy.category.value
        or process["strategy_module"] != "pm_strategies"
        or process["strategy_class"] != type(strategy).__name__
        or type(process["process_candidate_id"]) is not str
        or not 1 <= len(process["process_candidate_id"]) <= 512
        or process["strategy_code_sha256"]
        != _current_refit_strategy_code_sha256(strategy_id)
        or process["entry_rule_identity_sha256"]
        != _current_refit_callable_sha256(type(strategy).generate_signals)
        or process["exit_rule_identity_sha256"]
        != _current_refit_callable_sha256(type(strategy).calculate_stops)
        or process["regime_conditioning"]
        != "EXACT_SYMBOL_TIMEFRAME_REGIME_CELL"
    ):
        raise ValueError("CURRENT_REFIT_PROCESS_INVALID")
    return dict(process), _current_refit_sha256(process)


def _current_refit_validate_horizon(
    value: Any,
    *,
    request: Mapping[str, Any],
    source_sha256: str,
    source_bar_count: int,
) -> Tuple[Dict[str, Any], str]:
    horizon = _current_refit_exact(
        value, _CURRENT_REFIT_HORIZON_FIELDS, "CURRENT_REFIT_HORIZON_FIELDS_INVALID"
    )
    if (
        horizon["schema_version"] != CURRENT_REFIT_HORIZON_VERSION
        or horizon["symbol"] != request["symbol"]
        or horizon["timeframe"] != request["timeframe"]
        or horizon["lookback_kind"]
        not in {"NATIVE_OBSERVATIONS", "ACTIVE_SESSIONS", "CALENDAR_INTERVAL"}
        or type(horizon["lookback_value"]) is not int
        or not 1 <= horizon["lookback_value"] <= 10_000_000_000
        or type(horizon["source_bar_count"]) is not int
        or horizon["source_bar_count"] != source_bar_count
        or (
            horizon["lookback_kind"] == "NATIVE_OBSERVATIONS"
            and horizon["lookback_value"] != source_bar_count
        )
        or horizon["fit_start_utc"] != request["fit_start_utc"]
        or horizon["fit_end_utc"] != request["fit_end_utc"]
        or horizon["latest_closed_bar_utc"] != request["latest_closed_bar_utc"]
        or horizon["source_snapshot_sha256"] != source_sha256
    ):
        raise ValueError("CURRENT_REFIT_HORIZON_INVALID")
    return dict(horizon), _current_refit_sha256(horizon)


def _current_refit_validate_recipe(
    value: Any,
    *,
    strategy_id: str,
    process_sha256: str,
    horizon_sha256: str,
    trial_sha256: str,
    source_bar_count: int,
) -> Tuple[Dict[str, Any], str]:
    recipe = _current_refit_exact(
        value, _CURRENT_REFIT_RECIPE_FIELDS, "CURRENT_REFIT_RECIPE_FIELDS_INVALID"
    )
    strategy = StrategyRegistry.get(strategy_id)
    seed = recipe.get("seed")
    if type(seed) is not int or not 0 <= seed <= 0x7FFFFFFF:
        raise ValueError("CURRENT_REFIT_RECIPE_SEED_INVALID")
    expected = {
        "schema_version": CURRENT_REFIT_RECIPE_VERSION,
        "strategy_id": strategy_id,
        "winner_process_sha256": process_sha256,
        "winner_horizon_process_sha256": horizon_sha256,
        "winner_trial_definition_sha256": trial_sha256,
        "refit_mode": "STATELESS_RULE_MATERIALIZATION",
        "parameter_source": "CURRENT_WINNER_TRIAL",
        "source_validation": "FULL_NATIVE_PREFIX_STREAM",
        "fit_window_rule": "HORIZON_NATIVE_PREFIX_THROUGH_CURRENT_CUTOFF",
        "deterministic_fit_procedure_id": "FXPM_STATELESS_RULE_MATERIALIZE_V1",
        "seed": seed,
        "version_pins": {
            "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pm_core_sha256": _current_refit_core_code_sha256(),
            "pm_strategies_sha256": _current_refit_strategy_code_sha256(strategy_id),
        },
        "data_requirements": {
            "columns": list(_CURRENT_REFIT_BAR_COLUMNS),
            "minimum_bars": max(2, int(strategy.MIN_BARS)),
            "warmup_bars": max(0, int(strategy.MIN_BARS)),
        },
        "attempts": 1,
        "post_selection_search": False,
        "output_schema_version": CURRENT_REFIT_SPECIALIST_VERSION,
    }
    if recipe != expected:
        raise ValueError("CURRENT_REFIT_RECIPE_INVALID")
    if source_bar_count < expected["data_requirements"]["minimum_bars"]:
        raise ValueError("CURRENT_REFIT_RECIPE_SOURCE_TOO_SHORT")
    return dict(recipe), _current_refit_sha256(recipe)


def _current_refit_validate_book_entry(
    value: Any,
    *,
    request: Mapping[str, Any],
) -> Tuple[Dict[str, Any], str]:
    entry = _current_refit_exact(
        value,
        _CURRENT_REFIT_BOOK_ENTRY_FIELDS,
        "CURRENT_REFIT_BOOK_ENTRY_FIELDS_INVALID",
    )
    identity = {
        name: value
        for name, value in entry.items()
        if name
        not in {
            "adjusted_q_value",
            "by_qualified",
            "historical_local_candidate_pending_causal",
            "historical_book_entry_sha256",
        }
    }
    derived_sha = _current_refit_sha256(
        {"book_contract_version": BOOK_MULTIPLICITY_CONTRACT_VERSION, **identity}
    )
    if (
        any(entry[name] != request[name] for name in ("symbol", "timeframe", "regime"))
        or entry["historical_state"] != "WINNER"
        or entry["historical_local_candidate_pending_causal"] is not True
        or type(entry["local_process_candidate_id"]) is not str
        or not 1 <= len(entry["local_process_candidate_id"]) <= 256
        or not _current_refit_is_sha(entry["local_process_sha256"])
        or not _current_refit_is_sha(entry["winner_decision_sha256"])
        or not _current_refit_is_sha(entry["regime_receipt_sha256"])
        or not _current_refit_is_sha(entry["regime_detector_record_sha256"])
        or entry["historical_book_entry_sha256"] != derived_sha
    ):
        raise ValueError("CURRENT_REFIT_BOOK_ENTRY_INVALID")
    return dict(entry), derived_sha


def _current_refit_regime_map(
    regime_bundle: Mapping[str, Any],
) -> Tuple[
    Dict[Tuple[str, str], Dict[str, str]],
    Dict[Tuple[str, str], Dict[str, str]],
]:
    result: Dict[Tuple[str, str], Dict[str, str]] = {}
    source_bindings: Dict[Tuple[str, str], Dict[str, str]] = {}
    for envelope in regime_bundle.get("regime_fits", []):
        if type(envelope) is not dict or type(envelope.get("payload")) is not dict:
            raise ValueError("CURRENT_REFIT_REGIME_BUNDLE_INVALID")
        payload = envelope["payload"]
        record = payload.get("regime_detector_record")
        if type(record) is not dict:
            raise ValueError("CURRENT_REFIT_REGIME_BUNDLE_INVALID")
        group = (payload.get("symbol"), payload.get("timeframe"))
        if group in result:
            raise ValueError("CURRENT_REFIT_REGIME_BUNDLE_DUPLICATE")
        result[group] = {
            "regime_fit_receipt_sha256": envelope.get("receipt_sha256"),
            "regime_detector_record_sha256": record.get("record_sha256"),
            "regime_detector_parameters_sha256": record.get("parameters_sha256"),
            "regime_detector_code_sha256": record.get("detector_code_sha256"),
        }
        source_bindings[group] = {
            "activation_id": record.get("activation_id"),
            "activation_manifest_sha256": record.get("activation_manifest_sha256"),
            "native_source_sha256": record.get("native_source_sha256"),
            "source_contract_sha256": record.get("source_contract_sha256"),
        }
    return result, source_bindings


def replay_current_refit_execution_from_raw_manifest(
    plan: Mapping[str, Any],
    *,
    artifact_root: Path,
) -> Dict[str, Any]:
    """Recompute one current specialist book from physical inputs."""
    if type(plan) is not dict or plan.get("schema_version") != RAW_CURRENT_REFIT_REPLAY_REQUEST_VERSION:
        raise ValueError("CURRENT_REFIT_REPLAY_PLAN_INVALID")
    if plan.get("executor_contract_version") != CURRENT_REFIT_RAW_EXECUTOR_VERSION:
        raise ValueError("CURRENT_REFIT_EXECUTOR_VERSION_INVALID")
    root = Path(artifact_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("CURRENT_REFIT_ARTIFACT_ROOT_INVALID")
    raw_descriptors = plan.get("artifact_descriptors")
    if type(raw_descriptors) is not list or not 1 <= len(raw_descriptors) <= 16_384:
        raise ValueError("CURRENT_REFIT_DESCRIPTOR_ROSTER_INVALID")
    descriptors: Dict[str, Mapping[str, Any]] = {}
    for raw in raw_descriptors:
        descriptor = _current_refit_exact(
            raw,
            _CURRENT_REFIT_DESCRIPTOR_FIELDS,
            "CURRENT_REFIT_DESCRIPTOR_FIELDS_INVALID",
        )
        artifact_id = descriptor["artifact_id"]
        if not _current_refit_is_sha(artifact_id) or artifact_id in descriptors:
            raise ValueError("CURRENT_REFIT_DESCRIPTOR_ID_INVALID")
        descriptors[artifact_id] = descriptor
    bundle_ids = _current_refit_exact(
        plan.get("bundle_artifact_ids"),
        _CURRENT_REFIT_BUNDLE_ROLES,
        "CURRENT_REFIT_BUNDLE_ROSTER_INVALID",
    )
    if any(value not in descriptors for value in bundle_ids.values()):
        raise ValueError("CURRENT_REFIT_BUNDLE_ARTIFACT_MISSING")
    cutoff = _current_refit_utc(
        plan.get("current_cutoff_utc"), "CURRENT_REFIT_CUTOFF_INVALID"
    )

    raw_book = _current_refit_read_json(
        root,
        descriptors[bundle_ids["accepted_book_generation"]],
        format_version=CURRENT_PROCESS_BOOK_ARTIFACT_VERSION,
    )
    book_payload, book_receipt_sha256 = open_typed_receipt(
        raw_book, expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE
    )
    if type(book_payload) is not dict or book_payload.get("current_cutoff_utc") != cutoff:
        raise ValueError("CURRENT_REFIT_ACCEPTED_BOOK_INVALID")
    book_identity = dict(book_payload)
    generation_sha256 = book_identity.pop("generation_sha256", None)
    if (
        not _current_refit_is_sha(generation_sha256)
        or generation_sha256 != canonical_receipt_sha256(book_identity)
        or not _current_refit_is_sha(
            book_payload.get("historical_process_decision_sha256")
        )
    ):
        raise ValueError("CURRENT_REFIT_ACCEPTED_BOOK_INVALID")

    raw_source_bundle = _current_refit_read_json(
        root,
        descriptors[bundle_ids["source_snapshot_bundle"]],
        format_version=CURRENT_REFIT_SOURCE_BUNDLE_VERSION,
    )
    source_bundle, source_bundle_sha256 = _current_refit_validate_bundle(
        raw_source_bundle,
        fields=_CURRENT_REFIT_SOURCE_BUNDLE_FIELDS,
        entry_fields=_CURRENT_REFIT_SOURCE_BUNDLE_ENTRY_FIELDS,
        schema_version=CURRENT_REFIT_SOURCE_BUNDLE_VERSION,
        current_cutoff_utc=cutoff,
        kind="SOURCE_BUNDLE",
    )
    raw_horizon_bundle = _current_refit_read_json(
        root,
        descriptors[bundle_ids["horizon_decisions_bundle"]],
        format_version=CURRENT_REFIT_HORIZON_BUNDLE_VERSION,
    )
    horizon_bundle, horizon_bundle_sha256 = _current_refit_validate_bundle(
        raw_horizon_bundle,
        fields=_CURRENT_REFIT_HORIZON_BUNDLE_FIELDS,
        entry_fields=_CURRENT_REFIT_HORIZON_BUNDLE_ENTRY_FIELDS,
        schema_version=CURRENT_REFIT_HORIZON_BUNDLE_VERSION,
        current_cutoff_utc=cutoff,
        kind="HORIZON_BUNDLE",
    )
    raw_regime_bundle = _current_refit_read_json(
        root,
        descriptors[bundle_ids["regime_params_bundle"]],
        format_version=CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    )
    regime_by_group, regime_sources = _current_refit_regime_map(raw_regime_bundle)

    requests = plan.get("cell_requests")
    if type(requests) is not list or not 1 <= len(requests) <= 2400:
        raise ValueError("CURRENT_REFIT_REQUEST_ROSTER_INVALID")
    source_cache: Dict[str, Tuple[Dict[str, Any], str]] = {}
    full_cells: List[Dict[str, Any]] = []
    result_cells: List[Dict[str, Any]] = []
    specialist_refs: List[Dict[str, str]] = []
    trace_refs: List[Dict[str, str]] = []
    used_sources: Dict[Tuple[str, str], str] = {}
    used_horizons: Dict[Tuple[str, str, str], str] = {}
    seen_cell_ids: Set[str] = set()

    for raw_request in requests:
        request = _current_refit_exact(
            raw_request,
            _CURRENT_REFIT_REQUEST_FIELDS,
            "CURRENT_REFIT_REQUEST_FIELDS_INVALID",
        )
        if (
            request["timeframe"] not in _CURRENT_REFIT_TIMEFRAME_ORDER
            or request["regime"] not in {"TREND", "RANGE", "BREAKOUT", "CHOP"}
            or not _current_refit_is_sha(request["current_refit_cell_id"])
            or request["current_refit_cell_id"] in seen_cell_ids
        ):
            raise ValueError("CURRENT_REFIT_REQUEST_INVALID")
        seen_cell_ids.add(request["current_refit_cell_id"])
        for name in ("fit_start_utc", "fit_end_utc", "latest_closed_bar_utc"):
            _current_refit_utc(request[name], "CURRENT_REFIT_REQUEST_TIME_INVALID")
        artifact_ids = _current_refit_exact(
            request["artifact_ids"],
            _CURRENT_REFIT_CELL_ARTIFACT_ROLES,
            "CURRENT_REFIT_CELL_ARTIFACT_ROSTER_INVALID",
        )
        if any(value not in descriptors for value in artifact_ids.values()):
            raise ValueError("CURRENT_REFIT_CELL_ARTIFACT_MISSING")

        source_id = artifact_ids["source_snapshot"]
        if source_id not in source_cache:
            source = _current_refit_read_json(
                root,
                descriptors[source_id],
                format_version=CURRENT_REFIT_SOURCE_SNAPSHOT_VERSION,
            )
            source_sha256 = _current_refit_validate_source(
                source, request=request, root=root, descriptors=descriptors
            )
            source_cache[source_id] = (source, source_sha256)
        source, source_sha256 = source_cache[source_id]
        if any(
            source[name] != request[name]
            for name in (
                "symbol",
                "timeframe",
                "fit_start_utc",
                "fit_end_utc",
                "latest_closed_bar_utc",
            )
        ):
            raise ValueError("CURRENT_REFIT_SHARED_SOURCE_SPLICE")

        process, process_sha256 = _current_refit_validate_process(
            _current_refit_read_json(
                root,
                descriptors[artifact_ids["winner_process"]],
                format_version=CURRENT_REFIT_PROCESS_VERSION,
            ),
            strategy_id=request["strategy_id"],
        )
        horizon, horizon_sha256 = _current_refit_validate_horizon(
            _current_refit_read_json(
                root,
                descriptors[artifact_ids["horizon_process"]],
                format_version=CURRENT_REFIT_HORIZON_VERSION,
            ),
            request=request,
            source_sha256=source_sha256,
            source_bar_count=source["bar_count"],
        )
        trial, parameters = _current_refit_validate_trial(
            _current_refit_read_json(
                root,
                descriptors[artifact_ids["trial_definition"]],
                format_version=CURRENT_REFIT_TRIAL_VERSION,
            ),
            strategy_id=request["strategy_id"],
            process_sha256=process_sha256,
            horizon_sha256=horizon_sha256,
        )
        expected_process_candidate_id = _current_refit_process_candidate_id(
            symbol=request["symbol"],
            timeframe=request["timeframe"],
            regime=request["regime"],
            strategy_id=request["strategy_id"],
            parameters=parameters,
            horizon_process_sha256=horizon_sha256,
        )
        if process["process_candidate_id"] != expected_process_candidate_id:
            raise ValueError("CURRENT_REFIT_PROCESS_CANDIDATE_ID_MISMATCH")
        trial_sha256 = _current_refit_sha256(trial)
        recipe, recipe_sha256 = _current_refit_validate_recipe(
            _current_refit_read_json(
                root,
                descriptors[artifact_ids["refit_recipe"]],
                format_version=CURRENT_REFIT_RECIPE_VERSION,
            ),
            strategy_id=request["strategy_id"],
            process_sha256=process_sha256,
            horizon_sha256=horizon_sha256,
            trial_sha256=trial_sha256,
            source_bar_count=source["bar_count"],
        )
        book_entry, book_entry_sha256 = _current_refit_validate_book_entry(
            _current_refit_read_json(
                root,
                descriptors[artifact_ids["winner_book_entry"]],
                format_version=CURRENT_REFIT_BOOK_ENTRY_VERSION,
            ),
            request=request,
        )
        if (
            book_entry["local_process_sha256"] != process_sha256
            or book_entry["local_process_candidate_id"]
            != process["process_candidate_id"]
        ):
            raise ValueError("CURRENT_REFIT_PROCESS_CANDIDATE_SPLICE")
        detector = regime_by_group.get((request["symbol"], request["timeframe"]))
        if detector is None or (
            book_entry["regime_receipt_sha256"]
            != detector["regime_fit_receipt_sha256"]
            or book_entry["regime_detector_record_sha256"]
            != detector["regime_detector_record_sha256"]
        ):
            raise ValueError("CURRENT_REFIT_REGIME_BINDING_SPLICE")
        source_binding = regime_sources.get((request["symbol"], request["timeframe"]))
        if source_binding is None or any(
            source[name] != value for name, value in source_binding.items()
        ):
            raise ValueError("CURRENT_REFIT_SOURCE_ACTIVATION_SPLICE")

        parameter_payload = {
            "schema_version": CURRENT_REFIT_PARAMETER_VERSION,
            "strategy_id": request["strategy_id"],
            "parameters": parameters,
        }
        parameter_sha256 = _current_refit_sha256(parameter_payload)
        output_payload = {
            "schema_version": CURRENT_REFIT_SPECIALIST_VERSION,
            "symbol": request["symbol"],
            "timeframe": request["timeframe"],
            "regime": request["regime"],
            "strategy_id": request["strategy_id"],
            "parameters": parameters,
            "parameter_sha256": parameter_sha256,
            "source_snapshot_sha256": source_sha256,
            "winner_process_sha256": process_sha256,
            "winner_horizon_process_sha256": horizon_sha256,
            "winner_trial_definition_sha256": trial_sha256,
            "winner_refit_recipe_sha256": recipe_sha256,
            "winner_book_entry_sha256": book_entry_sha256,
            **detector,
            "fit_start_utc": request["fit_start_utc"],
            "fit_end_utc": request["fit_end_utc"],
            "latest_closed_bar_utc": request["latest_closed_bar_utc"],
        }
        output_artifact_sha256 = _current_refit_sha256(output_payload)
        identity = {
            "historical_process_decision_sha256": book_payload[
                "historical_process_decision_sha256"
            ],
            "accepted_book_generation_receipt_sha256": book_receipt_sha256,
            "symbol": request["symbol"],
            "timeframe": request["timeframe"],
            "regime": request["regime"],
            "strategy_id": request["strategy_id"],
            "winner_process_candidate_id": process["process_candidate_id"],
            "winner_process_sha256": process_sha256,
            "winner_horizon_process_sha256": horizon_sha256,
            "winner_trial_definition_sha256": trial_sha256,
            "winner_refit_recipe_sha256": recipe_sha256,
            "winner_decision_sha256": book_entry["winner_decision_sha256"],
            "winner_book_entry_sha256": book_entry_sha256,
            **detector,
            "fit_start_utc": request["fit_start_utc"],
            "fit_end_utc": request["fit_end_utc"],
            "latest_closed_bar_utc": request["latest_closed_bar_utc"],
            "source_snapshot_sha256": source_sha256,
            "parameter_sha256": parameter_sha256,
            "output_artifact_sha256": output_artifact_sha256,
        }
        cell_id = compute_current_refit_cell_id(identity)
        if cell_id != request["current_refit_cell_id"]:
            raise ValueError("CURRENT_REFIT_CELL_ID_REPLAY_MISMATCH")
        input_content_sha256 = {
            role: descriptors[artifact_ids[role]]["content_sha256"]
            for role in sorted(artifact_ids)
        }
        trace_payload = {
            "schema_version": CURRENT_REFIT_CELL_TRACE_VERSION,
            "current_refit_cell_id": cell_id,
            "input_content_sha256": input_content_sha256,
            "parameter_sha256": parameter_sha256,
            "output_artifact_sha256": output_artifact_sha256,
            "execution_mode": "STATELESS_RULE_MATERIALIZATION",
        }
        execution_trace_sha256 = _current_refit_sha256(trace_payload)
        full_cell = {
            "current_refit_cell_id": cell_id,
            **identity,
            "execution_trace_sha256": execution_trace_sha256,
            "attempts": 1,
            "status": "COMPLETE",
            "refit_process_frozen": True,
            "parameter_level_oos_evidence_claimed": False,
        }
        result_cell = {
            "current_refit_cell_id": cell_id,
            "parameter_sha256": parameter_sha256,
            "output_artifact_sha256": output_artifact_sha256,
            "execution_trace_sha256": execution_trace_sha256,
            "source_snapshot_sha256": source_sha256,
            "winner_horizon_process_sha256": horizon_sha256,
            "winner_trial_definition_sha256": trial_sha256,
            "winner_refit_recipe_sha256": recipe_sha256,
            **detector,
            "fit_start_utc": request["fit_start_utc"],
            "fit_end_utc": request["fit_end_utc"],
            "latest_closed_bar_utc": request["latest_closed_bar_utc"],
        }
        if set(result_cell) != _CURRENT_REFIT_RESULT_CELL_FIELDS:
            raise AssertionError("CURRENT_REFIT_INTERNAL_RESULT_FIELDS")
        full_cells.append(full_cell)
        result_cells.append(result_cell)
        specialist_refs.append(
            {
                "current_refit_cell_id": cell_id,
                "parameter_sha256": parameter_sha256,
                "output_artifact_sha256": output_artifact_sha256,
            }
        )
        trace_refs.append(
            {
                "current_refit_cell_id": cell_id,
                "execution_trace_sha256": execution_trace_sha256,
            }
        )
        used_sources[(request["symbol"], request["timeframe"])] = source_sha256
        used_horizons[
            (request["symbol"], request["timeframe"], request["regime"])
        ] = horizon_sha256

    expected_source_entries = [
        {"symbol": symbol, "timeframe": timeframe, "source_snapshot_sha256": digest}
        for (symbol, timeframe), digest in sorted(
            used_sources.items(),
            key=lambda item: (
                item[0][0],
                _CURRENT_REFIT_TIMEFRAME_ORDER[item[0][1]],
            ),
        )
    ]
    expected_horizon_entries = [
        {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "horizon_process_sha256": digest,
        }
        for (symbol, timeframe, regime), digest in sorted(
            used_horizons.items(),
            key=lambda item: (
                item[0][0],
                _CURRENT_REFIT_TIMEFRAME_ORDER[item[0][1]],
                item[0][2],
            ),
        )
    ]
    if source_bundle["entries"] != expected_source_entries:
        raise ValueError("CURRENT_REFIT_SOURCE_BUNDLE_ROSTER_MISMATCH")
    if horizon_bundle["entries"] != expected_horizon_entries:
        raise ValueError("CURRENT_REFIT_HORIZON_BUNDLE_ROSTER_MISMATCH")
    normalized_regime_bundle = validate_current_regime_params_bundle(
        raw_regime_bundle,
        current_cutoff_utc=cutoff,
        selected_cells=full_cells,
    )
    regime_bundle_sha256 = canonical_receipt_sha256(normalized_regime_bundle)
    if (
        book_payload.get("source_snapshot_bundle_sha256") != source_bundle_sha256
        or book_payload.get("regime_params_sha256") != regime_bundle_sha256
        or book_payload.get("horizon_decisions_sha256") != horizon_bundle_sha256
        or book_payload.get("selected_winner_roster_sha256")
        != compute_current_winner_roster_sha256(full_cells)
    ):
        raise ValueError("CURRENT_REFIT_ACCEPTED_BOOK_ROSTER_MISMATCH")

    cell_bundle_sha256 = canonical_receipt_sha256({"cell_executions": full_cells})
    output_book_sha256 = _current_refit_sha256(
        {
            "schema_version": CURRENT_REFIT_OUTPUT_BOOK_VERSION,
            "current_cutoff_utc": cutoff,
            "accepted_book_generation_receipt_sha256": book_receipt_sha256,
            "specialists": specialist_refs,
        }
    )
    execution_trace_sha256 = _current_refit_sha256(
        {
            "schema_version": CURRENT_REFIT_EXECUTION_TRACE_VERSION,
            "current_cutoff_utc": cutoff,
            "accepted_book_generation_receipt_sha256": book_receipt_sha256,
            "source_snapshot_bundle_sha256": source_bundle_sha256,
            "regime_params_sha256": regime_bundle_sha256,
            "horizon_decisions_sha256": horizon_bundle_sha256,
            "cell_execution_traces": trace_refs,
        }
    )
    return {
        "schema_version": RAW_CURRENT_REFIT_REPLAY_RESULT_VERSION,
        "executor_contract_version": CURRENT_REFIT_RAW_EXECUTOR_VERSION,
        "current_cutoff_utc": cutoff,
        "cell_results": result_cells,
        "cell_execution_bundle_sha256": cell_bundle_sha256,
        "output_book_sha256": output_book_sha256,
        "execution_trace_sha256": execution_trace_sha256,
    }

_DATA_STATE_KIND_REASONS = {
    "INSUFFICIENT_TRAIN_HISTORY": {
        "INSUFFICIENT_OPTIMIZER_TRAIN_ROWS",
        "INSUFFICIENT_DETECTOR_HOLDOUT_ROWS",
    },
}


def _validated_symbol_data_state(value: Any) -> Optional[Dict[str, Any]]:
    """Strict shared validator for the typed all-NON_SEARCH data-state.

    Only sidecar-bound TERMINAL kinds are admissible; anything else is
    technical.  Used identically by builder, worker and consumer (seq1035).
    """
    if not isinstance(value, dict):
        return None
    if set(value) != {"kind", "timeframes", "decisions", "generation_terminal"}:
        return None
    if value["kind"] != "ALL_TIMEFRAMES_NON_SEARCH":
        return None
    decisions = value["decisions"]
    if not isinstance(decisions, dict) or not decisions:
        return None
    timeframes = value["timeframes"]
    if not isinstance(timeframes, list) or timeframes != sorted(decisions):
        return None
    for tf_key, decision in decisions.items():
        if not isinstance(decision, dict) or set(decision) != {
            "decision_kind",
            "decision_sha256",
            "reason_code",
            "generation_terminal",
        }:
            return None
        kind = str(decision["decision_kind"] or "")
        reason = str(decision["reason_code"] or "")
        if kind not in _DATA_STATE_KIND_REASONS:
            return None
        if reason not in _DATA_STATE_KIND_REASONS[kind]:
            return None
        sha = str(decision["decision_sha256"] or "").lower()
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            return None
        if decision["generation_terminal"] is not True:
            return None
    if value["generation_terminal"] is not True:
        return None
    return dict(value)

FEATURE_PRECOMPUTE_RESULT_SCHEMA = "fxpm.feature-precompute-result.v1"
OPTIMIZER_TERMINATION_GRACE_SECONDS = 5.0

# One immutable production evidence policy.  These are effect-size and
# precision requirements in strict post-cost R units, not folklore raw-trade
# floors.  Train dispersion and exact attempted-trial multiplicity determine
# the cell-specific effective-observation requirement.
PRODUCTION_TRADE_POWER_POLICY = TradePowerPolicy(
    minimum_economic_edge_r=0.25,
    maximum_ci_half_width_r=0.25,
    target_power=0.80,
)
PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS = 8


def _stable_seed(*parts: Any) -> int:
    """Deterministic non-negative seed derived from stable context parts."""
    payload = "|".join(str(part) for part in parts).encode("utf-8", errors="replace")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFF


def _stable_hash(payload: Any) -> str:
    """Create a stable SHA1 hash for JSON-serializable payloads."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _signal_vector_sha256(signals: pd.Series) -> str:
    """Canonical bounded identity for an exact gated signal vector."""
    series = signals if isinstance(signals, pd.Series) else pd.Series(signals)
    values = np.asarray(pd.to_numeric(series, errors="raise"), dtype="<f8")
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("signal vector must be one-dimensional and finite")
    index_hashes = pd.util.hash_pandas_object(series.index, index=True).to_numpy(
        dtype="<u8", copy=False
    )
    digest = hashlib.sha256()
    digest.update(b"fxpm-gated-signal-vector-v1\0")
    digest.update(str(len(series)).encode("ascii"))
    digest.update(b"\0")
    digest.update(index_hashes.tobytes(order="C"))
    digest.update(values.tobytes(order="C"))
    return digest.hexdigest()


_OOS_BASELINE_IMMUTABLE_KEYS = (
    "format_version",
    "candidate_id",
    "symbol",
    "timeframe",
    "regime",
    "strategy_name",
    "parameters",
    "oos_surface",
    "return_index",
    "period_count",
    "period_returns",
    "baseline_gated_signals_sha256",
    "trade_path_evidence",
    "equity_path_evidence",
    "metrics",
    "train_metrics",
    "train_regime_metrics",
    "val_regime_metrics",
    "parameter_stability_evidence",
    "generalization_metrics",
    "window",
)


def _oos_baseline_evidence_sha256(record: Mapping[str, Any]) -> str:
    """Digest immutable output of the single frozen validation replay."""
    def canonical(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): canonical(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [canonical(item) for item in value]
        if isinstance(value, (np.integer, np.floating)):
            return canonical(value.item())
        if isinstance(value, float):
            if math.isnan(value):
                raise ValueError("NaN is not valid OOS baseline evidence")
            if math.isinf(value):
                return "__FXPM_POSITIVE_INFINITY__" if value > 0 else "__FXPM_NEGATIVE_INFINITY__"
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        raise TypeError(f"unsupported OOS baseline evidence type: {type(value).__name__}")

    payload = canonical({
        key: record.get(key) for key in _OOS_BASELINE_IMMUTABLE_KEYS
    })
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8", errors="strict")
    return hashlib.sha256(encoded).hexdigest()


def _bounded_cartesian_combinations(
    axes: Sequence[Sequence[Any]],
    *,
    limit: int,
    seed: int,
) -> Tuple[List[Tuple[Any, ...]], int]:
    """Decode at most ``limit`` Cartesian points without materializing the grid."""
    values = [tuple(axis) for axis in axes]
    if not values or any(not axis for axis in values):
        return [], 0
    total = int(math.prod(len(axis) for axis in values))
    count = min(total, max(0, int(limit)))
    if count <= 0:
        return [], total
    start = 0 if count == total else int(seed % total)
    step = 1 if count == total else (int((seed // max(total, 1)) % total) or 1)
    while math.gcd(step, total) != 1:
        step = (step + 1) % total or 1

    def decode(encoded: int) -> Tuple[Any, ...]:
        result: List[Any] = [None] * len(values)
        remainder = int(encoded)
        for index in range(len(values) - 1, -1, -1):
            axis = values[index]
            result[index] = axis[remainder % len(axis)]
            remainder //= len(axis)
        return tuple(result)

    return [decode((start + offset * step) % total) for offset in range(count)], total


class _BoundedNativeSource:
    """Lazy one-timeframe-resident native source (seq1019/seq1055).

    Preflight computed and released every frame; each pop/iteration reloads
    exactly one immutable timeframe and verifies it against the preflight
    fingerprint (meta equality, trimmed rows, boundary timestamps).
    """

    def __init__(self, pipeline, symbol, as_of_utc, expected):
        self._pipeline = pipeline
        self._symbol = str(symbol)
        self._as_of_utc = as_of_utc
        self._expected = dict(expected)

    def keys(self):
        return list(self._expected)

    def __len__(self):
        return len(self._expected)

    def __bool__(self):
        return bool(self._expected)

    def __contains__(self, key):
        return key in self._expected

    def __iter__(self):
        return iter(list(self._expected))

    def _reload(self, timeframe):
        spec = self._expected[timeframe]
        frame, meta = self._pipeline.data_loader.load_native_timeframe(
            self._symbol, timeframe
        )
        if dict(meta or {}) != spec["meta"]:
            raise RuntimeError(
                f"NATIVE_SOURCE_RELOAD_META_MISMATCH:{timeframe}"
            )
        data = trim_to_closed_bars(
            ensure_utc_index(frame, copy=False),
            timeframe,
            as_of_utc=self._as_of_utc,
            copy=False,
        )
        if (
            data is None
            or len(data) != spec["rows"]
            or str(data.index[0].isoformat()) != spec["first_ts"]
            or str(data.index[-1].isoformat()) != spec["last_ts"]
        ):
            raise RuntimeError(
                f"NATIVE_SOURCE_RELOAD_FRAME_MISMATCH:{timeframe}"
            )
        return data

    def pop(self, timeframe):
        data = self._reload(timeframe)
        del self._expected[timeframe]
        return data

    def items(self):
        for timeframe in list(self._expected):
            yield timeframe, self._reload(timeframe)


def _aggregate_search_trial_telemetry(
    search_cell_outcomes_by_tf: Mapping[str, Sequence[Mapping[str, Any]]],
    oos_evidence_records: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """seq932/seq950 exact trial telemetry.

    Primary source is the per-timeframe search-cell outcome ledger
    (``RegimeOptimizer._last_search_cell_outcomes``): probe and deep-tuning
    cells carry exact planned ``search_trials`` and ``completed_trials`` and
    exist even when every candidate is gated out before any OOS evidence
    record is produced (the seq950 all-searched-all-rejected case).  OOS
    evidence records supply the survivor-side cross-check only.  A timeframe
    that planned trials but completed none is a typed anomaly, never a
    silent zero.
    """
    planned_by_tf: Dict[str, int] = {}
    completed_by_tf: Dict[str, int] = {}
    eligible_by_tf: Dict[str, int] = {}
    rejected_by_tf: Dict[str, int] = {}
    anomalies: List[str] = []
    fatal_errors: List[str] = []

    def _count(cell: Mapping[str, Any], key: str) -> int:
        value = cell.get(key, 0)
        if value is None:
            value = 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{key}={value!r}")
        if value < 0:
            raise ValueError(f"{key}={value!r}")
        return value

    for raw_tf, cells in sorted((search_cell_outcomes_by_tf or {}).items()):
        record_tf = str(raw_tf or "").upper()
        planned = completed = eligible = 0
        for index, cell in enumerate(cells):
            try:
                cell_planned = _count(cell, "search_trials")
                cell_completed = _count(cell, "completed_trials")
                cell_eligible = _count(cell, "eligible_train_trials")
            except ValueError as exc:
                # seq979: an impossible count is an institutional invariant
                # violation, never advisory — the run must fail typed before
                # any SymbolConfig can be constructed or promoted.
                fatal_errors.append(
                    f"INVALID_SEARCH_CELL_COUNTS:{record_tf}:cell={index}:{exc}"
                )
                continue
            planned += cell_planned
            completed += cell_completed
            eligible += cell_eligible
        planned_by_tf[record_tf] = planned
        completed_by_tf[record_tf] = completed
        eligible_by_tf[record_tf] = eligible
        if completed > planned:
            fatal_errors.append(
                f"COMPLETED_EXCEEDS_PLANNED:{record_tf}:"
                f"completed={completed}:planned={planned}"
            )
        if eligible > completed:
            fatal_errors.append(
                f"ELIGIBLE_EXCEEDS_COMPLETED:{record_tf}:"
                f"eligible={eligible}:completed={completed}"
            )
            rejected_by_tf[record_tf] = 0
        else:
            rejected_by_tf[record_tf] = completed - eligible
        if planned > 0 and completed == 0:
            anomalies.append(
                f"SEARCHED_BUT_ZERO_COMPLETED_TRIALS:{record_tf}:planned={planned}"
            )
    surviving_sum = 0
    survivor_local_totals_by_tf: Dict[str, int] = {}
    for record in oos_evidence_records:
        record_tf = str(record.get("timeframe", "") or "").upper()
        survivor_local_totals_by_tf[record_tf] = max(
            survivor_local_totals_by_tf.get(record_tf, 0),
            int(record.get("local_search_trials_total", 0) or 0),
        )
        surviving_sum += int(record.get("search_trials", 0) or 0)
    return {
        "planned_search_trials_total": int(sum(planned_by_tf.values())),
        "completed_search_trials_total": int(sum(completed_by_tf.values())),
        "eligible_train_trials_total": int(sum(eligible_by_tf.values())),
        "rejected_train_trials_total": int(sum(rejected_by_tf.values())),
        "planned_search_trials_by_timeframe": {
            key: int(value) for key, value in sorted(planned_by_tf.items())
        },
        "completed_search_trials_by_timeframe": {
            key: int(value) for key, value in sorted(completed_by_tf.items())
        },
        "eligible_train_trials_by_timeframe": {
            key: int(value) for key, value in sorted(eligible_by_tf.items())
        },
        "rejected_train_trials_by_timeframe": {
            key: int(value) for key, value in sorted(rejected_by_tf.items())
        },
        "telemetry_anomalies": anomalies,
        "fatal_telemetry_errors": fatal_errors,
        # Survivor-side cross-check (candidate-declared cell totals).
        "survivor_local_search_trials_by_timeframe": {
            key: int(value)
            for key, value in sorted(survivor_local_totals_by_tf.items())
        },
        "surviving_candidate_search_trials_sum": int(surviving_sum),
        "oos_candidate_evidence_records": int(len(oos_evidence_records)),
    }


def _classify_staged_search_exception(
    exc: BaseException,
    *,
    stage: SearchStage,
    symbol: str,
    timeframe: str,
    regime: str,
    strategy_family: str,
    parameter_hash: str,
) -> TrialOutcome:
    """Keep invalid-parameter abstentions confined to parameter construction."""
    outcome = classify_search_exception(
        exc,
        stage=stage,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        strategy_family=strategy_family,
        parameter_hash=parameter_hash,
    )
    if (
        outcome.kind is SearchOutcomeKind.INVALID_PARAM
        and stage not in {
            SearchStage.PARAMETER_VALIDATION,
            SearchStage.STRATEGY_CONSTRUCTION,
        }
    ):
        return TrialOutcome(
            kind=SearchOutcomeKind.INFRA_FAILURE,
            stage=stage,
            reason_code="INVALID_PARAMETER_EXCEPTION_OUTSIDE_PARAMETER_STAGE",
            reason=outcome.reason,
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_family=strategy_family,
            parameter_hash=parameter_hash,
            exception_type=outcome.exception_type,
        )
    return outcome


def _deployment_contract_fingerprint(config: PipelineConfig) -> Dict[str, Any]:
    """Hash broker execution specs and the funded position/deployment surface.

    Direct unit/research construction has no source config and receives a stable
    unbound token. Runs created through ``pm_main`` bind the exact funded JSON,
    broker-spec bytes, symbol roster, and PositionConfig payload without ever
    including MT5 credentials.
    """
    source_raw = str(getattr(config, "_deployment_config_path", "") or "").strip()
    if not source_raw:
        return {"contract": "unbound_research_deployment_v1"}
    source_path = Path(source_raw)
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {
            "contract": "funded_live_deployment_v1",
            "status": "UNAVAILABLE",
            "reason": f"config unreadable: {type(exc).__name__}",
        }
    broker_raw = str(payload.get("broker_specs_path", "") or "").strip()
    broker_path = Path(broker_raw) if broker_raw else None
    if broker_path is not None and not broker_path.is_absolute():
        broker_path = source_path.resolve().parent / broker_path
    broker_sha = None
    broker_size = None
    if broker_path is not None:
        try:
            broker_bytes = broker_path.read_bytes()
            broker_sha = hashlib.sha256(broker_bytes).hexdigest()
            broker_size = len(broker_bytes)
        except Exception:
            broker_sha = None
    contract_payload = {
        "scope": str((payload.get("_meta") or {}).get("scope", "") or ""),
        "symbols": [str(value) for value in (payload.get("symbols") or [])],
        "position": dict(payload.get("position") or {}),
        "instrument_spec_defaults": dict(payload.get("instrument_spec_defaults") or {}),
        "instrument_specs": dict(payload.get("instrument_specs") or {}),
        "broker_specs_path": broker_raw,
        "broker_specs_sha256": broker_sha,
        "broker_specs_size": broker_size,
    }
    return {
        "contract": "funded_live_deployment_v1",
        "status": "PASS" if broker_sha else "UNAVAILABLE",
        "sha1": _stable_hash(contract_payload),
        "broker_specs_sha256": broker_sha,
        "broker_specs_size": broker_size,
    }


def _regime_params_fingerprint(filepath: str) -> Dict[str, Any]:
    try:
        path = Path(filepath)
        stat = path.stat()
        return {
            "path": str(path.resolve()),
            "mtime_ns": stat.st_mtime_ns,
            "size": stat.st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    except Exception:
        return {"path": str(filepath), "mtime_ns": None, "size": None, "sha256": None}


def _regime_provenance_fingerprint(params_filepath: str) -> Dict[str, Any]:
    """Fingerprint the train-only detector-fit sidecar independently."""
    path = regime_params_provenance_path(params_filepath)
    try:
        stat = path.stat()
        return {
            "path": str(path.resolve()),
            "mtime_ns": stat.st_mtime_ns,
            "size": stat.st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    except Exception:
        return {"path": str(path), "mtime_ns": None, "size": None, "sha256": None}


def _scheduled_valid_until(config: PipelineConfig, from_dt: Optional[datetime] = None) -> datetime:
    """Calendar-anchored config validity boundary for production artifacts."""
    return config.get_next_retrain_at(from_dt or datetime.now())


_DEFAULT_STRATEGY_SURFACE_HASH: Optional[str] = None


def _strategy_surface_fingerprint(strategies: Optional[List[BaseStrategy]] = None) -> str:
    """Hash the discoverable strategy surface once per process.

    Enumerating every strategy, parameter grid, and class source is intentionally
    comprehensive but expensive.  The default registry cannot change safely
    inside a running optimization/live process, so recomputing its hash on every
    ledger/readiness check adds latency without detecting any additional drift.
    Explicit strategy lists remain uncached for tests and bounded research runs.
    """
    global _DEFAULT_STRATEGY_SURFACE_HASH
    if strategies is None and _DEFAULT_STRATEGY_SURFACE_HASH is not None:
        return _DEFAULT_STRATEGY_SURFACE_HASH

    strategy_list = strategies if strategies is not None else StrategyRegistry.get_all_instances()
    module_file_hashes: Dict[str, str] = {}
    surface = []
    for strategy in strategy_list:
        try:
            try:
                source_file = inspect.getsourcefile(strategy.__class__)
                if source_file:
                    source_key = str(Path(source_file).resolve())
                    file_hash = module_file_hashes.get(source_key)
                    if file_hash is None:
                        file_hash = hashlib.sha256(Path(source_file).read_bytes()).hexdigest()
                        module_file_hashes[source_key] = file_hash
                    # Hash a module once, then bind each class identity to those
                    # exact bytes.  This is stricter (a sibling implementation
                    # change invalidates the whole strategy surface) and avoids
                    # 100 repeated, very slow inspect.getsource calls on Windows.
                    implementation = (
                        f"file_sha256={file_hash};"
                        f"class={strategy.__class__.__module__}."
                        f"{strategy.__class__.__qualname__}"
                    )
                else:
                    implementation = inspect.getsource(strategy.__class__)
            except (OSError, TypeError):
                implementation = (
                    f"{strategy.__class__.__module__}."
                    f"{strategy.__class__.__qualname__}"
                )
            surface.append({
                "name": strategy.name,
                "params": strategy.get_params(),
                "grid": strategy.get_param_grid(),
                "implementation_sha256": hashlib.sha256(
                    implementation.encode("utf-8", errors="replace")
                ).hexdigest(),
            })
        except Exception:
            surface.append({"name": getattr(strategy, "name", strategy.__class__.__name__)})
    surface.sort(key=lambda item: item["name"])
    fingerprint = _stable_hash(surface)
    if strategies is None:
        _DEFAULT_STRATEGY_SURFACE_HASH = fingerprint
    return fingerprint


def build_artifact_meta(config: PipelineConfig,
                        strategies: Optional[List[BaseStrategy]] = None) -> Dict[str, Any]:
    """Build a semantic fingerprint for persisted optimization artifacts."""
    split_mode = str(
        getattr(config, "production_split_mode", CALENDAR_SPLIT_MODE)
        or CALENDAR_SPLIT_MODE
    ).strip().lower()
    return {
        "artifact_version": PIPELINE_ARTIFACT_VERSION,
        "research_mode": bool(getattr(config, "research_mode", False)),
        "deployment_contract": _deployment_contract_fingerprint(config),
        "feature_cache_version": FeatureComputer.FEATURE_CACHE_VERSION,
        "split_contract_version": (
            EVIDENCE_CONTRACT_VERSION
            if split_mode == CALENDAR_SPLIT_MODE
            else DataSplitter.SPLIT_CONTRACT_VERSION
        ),
        "max_bars": int(getattr(config, "max_bars", 300000)),
        "max_bars_applied": split_mode != CALENDAR_SPLIT_MODE,
        "screening_atr_anchor_enabled": bool(getattr(config, "screening_atr_anchor_enabled", False)),
        "screening_atr_anchor_multipliers": [
            float(value) for value in (getattr(config, "screening_atr_anchor_multipliers", [2.0]) or [2.0])
        ],
        "regime_family_probe_trials": int(
            getattr(config, "regime_family_probe_trials", 5) or 5
        ),
        "regime_hyperparam_top_k": int(
            getattr(config, "regime_hyperparam_top_k", 3) or 3
        ),
        "regime_hyperparam_max_combos": int(
            getattr(config, "regime_hyperparam_max_combos", 150) or 150
        ),
        "optuna_family_size_aware_budget": bool(
            getattr(config, "optuna_family_size_aware_budget", False)
        ),
        "optuna_min_trials_per_strategy": int(
            getattr(config, "optuna_min_trials_per_strategy", 50) or 50
        ),
        "optuna_max_trials_per_strategy": int(
            getattr(config, "optuna_max_trials_per_strategy", 5000) or 5000
        ),
        "optuna_target_coverage_pct": float(
            getattr(config, "optuna_target_coverage_pct", 0.10) or 0.10
        ),
        "historical_stress_audit_bars": int(getattr(config, "historical_stress_audit_bars", 50000)),
        "active_universe_bars": int(getattr(config, "active_universe_bars", 250000)),
        "active_stage2_pct": float(getattr(config, "active_stage2_pct", 50.0)),
        "production_split_mode": str(
            getattr(config, "production_split_mode", CALENDAR_SPLIT_MODE)
            or CALENDAR_SPLIT_MODE
        ),
        "tuning_validation_bars": int(getattr(config, "tuning_validation_bars", 50000) or 0),
        "final_lockbox_bars": int(getattr(config, "final_lockbox_bars", 50000) or 0),
        "split_embargo_bars": int(getattr(config, "split_embargo_bars", 300) or 0),
        "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
        "evidence_sufficiency_enabled": bool(
            getattr(config, "evidence_sufficiency_enabled", True)
        ),
        "evidence_require_native_timeframes": bool(
            getattr(config, "evidence_require_native_timeframes", True)
        ),
        "evidence_require_native_provenance": bool(
            getattr(config, "evidence_require_native_provenance", True)
        ),
        "evidence_require_session_coverage": bool(
            getattr(config, "evidence_require_session_coverage", True)
        ),
        "evidence_min_expected_session_coverage": float(
            getattr(config, "evidence_min_expected_session_coverage", 0.95)
        ),
        "evidence_boundary_tolerance_days": float(
            getattr(config, "evidence_boundary_tolerance_days", 7.0)
        ),
        "evidence_purge_days": float(getattr(config, "evidence_purge_days", 7.0)),
        "evidence_warmup_days": float(getattr(config, "evidence_warmup_days", 400.0)),
        "evidence_as_of_utc": str(getattr(config, "evidence_as_of_utc", "") or ""),
        "evidence_window_years": dict(
            getattr(config, "evidence_window_years", {}) or {}
        ),
        "evidence_min_native_bars": dict(
            getattr(config, "evidence_min_native_bars", {}) or {}
        ),
        "walk_forward_validation_bars": int(getattr(config, "walk_forward_validation_bars", 50000) or 0),
        "clean_holdout_pct": float(getattr(config, "clean_holdout_pct", 0.0)),
        "overlap_pct": float(config.overlap_pct),
        "discovery_risk_per_trade_pct": float(getattr(config, "discovery_risk_per_trade_pct", 1.0)),
        "use_spread": bool(getattr(config, "use_spread", True)),
        "backtest_spread_mode": str(getattr(config, "backtest_spread_mode", "fixed_spec") or "fixed_spec"),
        "use_commission": bool(getattr(config, "use_commission", True)),
        "use_slippage": bool(getattr(config, "use_slippage", True)),
        "slippage_pips": float(getattr(config, "slippage_pips", 0.0) or 0.0),
        "swap_model_enabled": bool(getattr(config, "swap_model_enabled", True)),
        "swap_model_min_holding_hours": float(getattr(config, "swap_model_min_holding_hours", 20.0) or 0.0),
        "swap_model_triple_rollover_day": str(
            getattr(config, "swap_model_triple_rollover_day", "wednesday") or "wednesday"
        ).lower(),
        "walk_forward_audit_enabled": bool(getattr(config, "walk_forward_audit_enabled", True)),
        "walk_forward_audit_windows": int(getattr(config, "walk_forward_audit_windows", 3)),
        "false_discovery_control_enabled": bool(
            getattr(config, "false_discovery_control_enabled", False)
        ),
        "false_discovery_min_dsr": float(getattr(config, "false_discovery_min_dsr", 0.0) or 0.0),
        "false_discovery_confidence_floor": float(
            getattr(config, "false_discovery_confidence_floor", 0.35) or 0.0
        ),
        "false_discovery_scope_power": float(
            getattr(config, "false_discovery_scope_power", 0.25) or 0.0
        ),
        "false_discovery_symbol_count": int(getattr(config, "false_discovery_symbol_count", 0) or 0),
        "statistics_contract_version": STATISTICS_CONTRACT_VERSION,
        "entry_slippage_timing_contract_version": (
            ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION
        ),
        "swap_accrual_timing_contract_version": (
            SWAP_ACCRUAL_TIMING_CONTRACT_VERSION
        ),
        "false_discovery_min_daily_observations": int(
            getattr(config, "false_discovery_min_daily_observations", 252) or 252
        ),
        "false_discovery_min_effective_observations": float(
            getattr(config, "false_discovery_min_effective_observations", 60.0) or 60.0
        ),
        "false_discovery_max_pbo": float(
            getattr(config, "false_discovery_max_pbo", 0.50) or 0.50
        ),
        "false_discovery_pbo_min_candidates": int(
            getattr(config, "false_discovery_pbo_min_candidates", 5) or 5
        ),
        "false_discovery_pbo_blocks": int(
            getattr(config, "false_discovery_pbo_blocks", 10) or 10
        ),
        "false_discovery_require_bootstrap": bool(
            getattr(config, "false_discovery_require_bootstrap", True)
        ),
        "false_discovery_bootstrap_repetitions": int(
            getattr(config, "false_discovery_bootstrap_repetitions", 300) or 300
        ),
        "false_discovery_bootstrap_confidence": float(
            getattr(config, "false_discovery_bootstrap_confidence", 0.95) or 0.95
        ),
        "lockbox_one_shot_registry_enabled": bool(
            getattr(config, "lockbox_one_shot_registry_enabled", True)
        ),
        "false_discovery_holdout_policy": str(
            getattr(config, "false_discovery_holdout_policy", "off") or "off"
        ).lower(),
        "false_discovery_holdout_min_trades": int(
            getattr(config, "false_discovery_holdout_min_trades", 5) or 0
        ),
        "false_discovery_holdout_min_profit_factor": float(
            getattr(config, "false_discovery_holdout_min_profit_factor", 1.0) or 0.0
        ),
        "false_discovery_holdout_min_return_pct": float(
            getattr(config, "false_discovery_holdout_min_return_pct", 0.0) or 0.0
        ),
        "false_discovery_holdout_max_drawdown_pct": float(
            getattr(config, "false_discovery_holdout_max_drawdown_pct", 20.0) or 0.0
        ),
        "oos_validation_mode": str(getattr(config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout"),
        "oos_kfold_splits": int(getattr(config, "oos_kfold_splits", 5) or 5),
        "oos_embargo_bars": int(getattr(config, "oos_embargo_bars", 1) or 0),
        "oos_min_trades": int(getattr(config, "oos_min_trades", 5) or 0),
        "oos_aggregate_policy": str(
            getattr(config, "oos_aggregate_policy", "aggregate_with_worst_fold_floor")
            or "aggregate_with_worst_fold_floor"
        ),
        "oos_evidence_export_enabled": bool(getattr(config, "oos_evidence_export_enabled", False)),
        "oos_evidence_periods": int(getattr(config, "oos_evidence_periods", 64) or 64),
        "oos_evidence_min_periods": int(getattr(config, "oos_evidence_min_periods", 8) or 8),
        "oos_evidence_dirname": str(getattr(config, "oos_evidence_dirname", "oos_evidence") or "oos_evidence"),
        "oos_evidence_record_schema": "oos_candidate_returns_v4_one_touch_evidence",
        "oos_evidence_trade_path_max_points": int(
            getattr(config, "oos_evidence_trade_path_max_points", 256) or 0
        ),
        "oos_evidence_include_execution_cost_survival": bool(
            getattr(config, "oos_evidence_include_execution_cost_survival", False)
        ),
        "oos_evidence_execution_stress_mode": str(
            getattr(config, "oos_evidence_execution_stress_mode", "stress_p95") or "stress_p95"
        ),
        "oos_evidence_execution_cost_max_candidates_per_tf": int(
            getattr(config, "oos_evidence_execution_cost_max_candidates_per_tf", 0) or 0
        ),
        "threadspark_selection_surface": str(
            getattr(config, "threadspark_selection_surface", "validation") or "validation"
        ),
        "threadspark_use_historical_cost_evidence": bool(
            getattr(config, "threadspark_use_historical_cost_evidence", True)
        ),
        "threadspark_historical_cost_scope": str(
            getattr(config, "threadspark_historical_cost_scope", "workflow") or "workflow"
        ),
        "threadspark_historical_chunk_count": int(
            getattr(config, "threadspark_historical_chunk_count", 5) or 5
        ),
        "scoring_use_consistency": bool(getattr(config, "scoring_use_consistency", False)),
        "scoring_use_continuous_dd": bool(getattr(config, "scoring_use_continuous_dd", False)),
        "scoring_use_sortino_blend": bool(getattr(config, "scoring_use_sortino_blend", False)),
        "scoring_use_tail_risk": bool(getattr(config, "scoring_use_tail_risk", False)),
        "scoring_use_trade_frequency_bonus": bool(getattr(config, "scoring_use_trade_frequency_bonus", False)),
        "legacy_confidence_bonus_cap": float(getattr(config, "legacy_confidence_bonus_cap", 0.08) or 0.0),
        "regime_scoring_model": str(getattr(config, "regime_scoring_model", "legacy") or "legacy"),
        "specialist_spokes_v2_cost_frequency_strength": float(
            getattr(config, "specialist_spokes_v2_cost_frequency_strength", 0.50) or 0.0
        ),
        "specialist_spokes_v2_default_fallback_enabled": bool(
            getattr(config, "specialist_spokes_v2_default_fallback_enabled", True)
        ),
        "specialist_spokes_v2_allow_promote_small_default": bool(
            getattr(config, "specialist_spokes_v2_allow_promote_small_default", True)
        ),
        "specialist_spokes_v2_max_validation_sharpe": float(
            getattr(config, "specialist_spokes_v2_max_validation_sharpe", 0.0) or 0.0
        ),
        "regime_trade_stability_floor": float(getattr(config, "regime_trade_stability_floor", 0.7) or 0.7),
        "local_governance_tournament_enabled": bool(getattr(config, "local_governance_tournament_enabled", False)),
        "local_governance_candidate_policies": candidate_policy_names(
            getattr(config, "local_governance_candidate_policies", None)
        ),
        "local_governance_require_baseline_improvement": bool(
            getattr(config, "local_governance_require_baseline_improvement", True)
        ),
        "local_governance_min_score_improvement": float(
            getattr(config, "local_governance_min_score_improvement", 0.0) or 0.0
        ),
        "risk_management_optimization_enabled": bool(
            getattr(config, "risk_management_optimization_enabled", False)
        ),
        "risk_management_selection_stage": str(
            getattr(config, "risk_management_selection_stage", "stage3")
        ),
        "exit_surface_contract": "neutral_discovery_sl_tp_v1",
        "regime_params": _regime_params_fingerprint(getattr(config, "regime_params_file", "regime_params.json")),
        "regime_provenance": _regime_provenance_fingerprint(
            getattr(config, "regime_params_file", "regime_params.json")
        ),
        "strategy_surface_hash": _strategy_surface_fingerprint(strategies),
        "metric_contract": "net_dollar_mtm_equity_v1",
    }


def _artifact_contract_view(stored_artifact: Dict[str, Any],
                            current_artifact_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return only the semantic artifact keys that participate in invalidation.

    Persisted artifacts may carry operational metadata such as ledger completion
    state or no-trade explanations. Those fields are useful audit evidence but
    must not make a freshly saved config look stale on the next status check.
    """
    stored = stored_artifact or {}
    current = current_artifact_meta or {}
    return {key: stored.get(key) for key in current.keys()}


def _regime_params_contract_token(meta: Any) -> Any:
    """Normalize materialized no-file detector rows to the validation contract."""
    if not isinstance(meta, dict):
        return meta
    if meta.get("mtime_ns") is None and meta.get("size") is None:
        return {"contract": "validation_no_file_timeframe_surface"}
    path_raw = str(meta.get("path", "") or "").strip()
    if not path_raw:
        return meta
    path = Path(path_raw)
    if not path.exists():
        return meta
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return meta
    if not isinstance(payload, dict) or not payload:
        return meta
    for symbol, tf_map in payload.items():
        if not isinstance(tf_map, dict) or not tf_map:
            return meta
        for timeframe, params in tf_map.items():
            if not isinstance(params, dict):
                return meta
            expected = validated_regime_params_for_timeframe(str(symbol), str(timeframe)).to_dict()
            if params != expected:
                return meta
    return {"contract": "validation_no_file_timeframe_surface"}


def artifact_contract_matches(stored_artifact: Dict[str, Any],
                              current_artifact_meta: Dict[str, Any]) -> bool:
    """Return True when the persisted artifact matches the current contract."""
    if not current_artifact_meta:
        return True
    stored_view = _artifact_contract_view(stored_artifact, current_artifact_meta)
    current_view = dict(current_artifact_meta)
    if "regime_params" in current_view:
        stored_view["regime_params"] = _regime_params_contract_token(stored_view.get("regime_params"))
        current_view["regime_params"] = _regime_params_contract_token(current_view.get("regime_params"))
    return stored_view == current_view


def _default_live_observability() -> Dict[str, Any]:
    return {
        "last_seen_drift": {},
        "recent_consecutive_losses": 0,
        "last_review_ts": None,
        "operator_note": "",
        "operator_state": "ACTIVE",
    }


def _default_governance_policy() -> Dict[str, Any]:
    return {
        "selected_policy": "control_fixed",
        "selection_basis": "baseline",
        "optimization_stage": None,
        "selection_surface": None,
        "selected_at": None,
        "candidate_results": {},
    }


def _default_walk_forward_audit() -> Dict[str, Any]:
    return {
        "enabled": False,
        "window_count": 0,
        "summary": {},
        "windows": [],
        "audited_at": None,
    }


def _default_historical_audit() -> Dict[str, Any]:
    return {
        "enabled": False,
        "window": "historical_stress_audit",
        "total_trades": 0,
        "severe_failure": False,
        "failure_reasons": [],
        "metrics": {},
        "audited_at": None,
    }


def classify_promotion_status(regime_config: "RegimeConfig", config: PipelineConfig) -> Tuple[str, List[str]]:
    """Classify whether a local winner can trade live, paper only, or is blocked."""
    reasons: List[str] = []
    if regime_config is None or regime_config.is_no_trade_marker():
        return "NO_TRADE", ["no trade marker"]

    temporal_evidence = dict(
        (getattr(regime_config, "validation_evidence", {}) or {}).get(
            "temporal_lineage", {}
        ) or {}
    )
    temporal_role = str(temporal_evidence.get("book_role", "") or "").upper()
    if temporal_role == EDGE_CHALLENGER_ROLE:
        return "PAPER_ONLY", [
            "latest-data edge challenger requires future non-overlapping forward evidence"
        ]
    temporal_block = temporal_role_block_reason(
        getattr(regime_config, "validation_evidence", {}) or {}
    )
    if temporal_block:
        reasons.append(temporal_block)

    scoring_model = str(getattr(config, "regime_scoring_model", "legacy") or "legacy").lower()
    historical = dict(getattr(regime_config, "historical_audit_metrics", {}) or {})
    historical_severe_reasons: List[str] = []
    if bool(historical.get("severe_failure", False)):
        historical_severe_reasons.extend(str(reason) for reason in historical.get("failure_reasons", []) or [])
        if scoring_model != "specialist_spokes_v2":
            reasons.extend(historical_severe_reasons)
    regime_profitability_audit = historical.get("regime_profitability_audit")
    if (
        scoring_model != "specialist_spokes_v2" and
        isinstance(regime_profitability_audit, dict) and
        bool(regime_profitability_audit.get("severe_failure", False))
    ):
        reasons.extend(str(reason) for reason in regime_profitability_audit.get("warnings", []) or [])

    walk_forward = dict(getattr(regime_config, "walk_forward_audit", {}) or {})
    wf_policy = str(getattr(config, "walk_forward_promotion_policy", "promotion_gate") or "").lower()
    wf_live_blocking = bool(getattr(config, "walk_forward_live_blocking_enabled", False))
    wf_promote_small_reasons: List[str] = []
    if wf_policy == "promotion_gate":
        summary = walk_forward.get("summary") if isinstance(walk_forward.get("summary"), dict) else {}
        windows = walk_forward.get("windows") if isinstance(walk_forward.get("windows"), list) else []
        audited = [window for window in windows if int(window.get("total_trades", 0) or 0) > 0]
        if not audited:
            reason = str(summary.get("reason", "") or "no_regime_trades")
            wf_promote_small_reasons.append(f"walk_forward insufficient promotion evidence: {reason}")
        else:
            negative = [window for window in audited if float(window.get("total_return_pct", 0.0) or 0.0) < 0.0]
            non_positive = [window for window in audited if float(window.get("total_return_pct", 0.0) or 0.0) <= 0.0]
            min_score = float(summary.get("min_score", 0.0) or 0.0)
            avg_pf = float(summary.get("avg_profit_factor", 0.0) or 0.0)
            avg_return = float(summary.get("avg_total_return_pct", 0.0) or 0.0)
            if len(negative) == len(audited):
                reasons.append("walk_forward all audited windows negative")
            if min_score < 0.0:
                reasons.append(f"walk_forward min_score {min_score:.2f} < 0")
            if not reasons and non_positive:
                wf_promote_small_reasons.append(
                    f"walk_forward non-positive windows {len(non_positive)}/{len(audited)}"
                )
            if not reasons and avg_pf < 1.0:
                wf_promote_small_reasons.append(f"walk_forward avg PF {avg_pf:.2f} < 1.00")
            if not reasons and avg_return <= 0.0:
                wf_promote_small_reasons.append(f"walk_forward avg return {avg_return:.1f}% <= 0.0%")
    elif wf_live_blocking and wf_policy == "block_severe_only":
        summary = walk_forward.get("summary") if isinstance(walk_forward.get("summary"), dict) else {}
        windows = walk_forward.get("windows") if isinstance(walk_forward.get("windows"), list) else []
        audited = [window for window in windows if int(window.get("total_trades", 0) or 0) > 0]
        if audited:
            negative = [window for window in audited if float(window.get("total_return_pct", 0.0) or 0.0) < 0.0]
            if len(negative) == len(audited):
                reasons.append("walk_forward all audited windows negative")
            min_score = float(summary.get("min_score", 0.0) or 0.0)
            if min_score < 0.0:
                reasons.append(f"walk_forward min_score {min_score:.2f} < 0")

    if reasons:
        return "BLOCKED", reasons

    holdout = dict(getattr(regime_config, "holdout_metrics", {}) or {})
    holdout_trades = int(holdout.get("total_trades", 0) or 0)
    holdout_return = float(holdout.get("total_return_pct", 0.0) or 0.0)
    false_discovery_enabled = bool(getattr(config, "false_discovery_control_enabled", False))
    split_mode = str(
        getattr(config, "production_split_mode", CALENDAR_SPLIT_MODE)
        or CALENDAR_SPLIT_MODE
    ).lower()
    false_discovery_reasons: List[str] = []
    if false_discovery_enabled:
        evidence = dict(getattr(regime_config, "validation_evidence", {}) or {})
        false_discovery = dict(evidence.get("false_discovery", {}) or {})
        min_dsr = float(getattr(config, "false_discovery_min_dsr", 0.95) or 0.95)
        if split_mode == CALENDAR_SPLIT_MODE:
            if not false_discovery:
                false_discovery_reasons.append("false-discovery evidence unavailable")
            else:
                dsr = float(false_discovery.get("deflated_sharpe", 0.0) or 0.0)
                dsr_status = str(
                    (false_discovery.get("dsr_evidence") or {}).get("status", "UNAVAILABLE")
                ).upper()
                dsr_detail = dict(false_discovery.get("dsr_evidence") or {})
                dsr_selection_passed = (
                    dsr_status == "PASS"
                    or (
                        dsr_status == "DIAGNOSTIC_ONLY"
                        and dsr_detail.get("diagnostic_threshold_passed") is True
                    )
                )
                if not dsr_selection_passed or dsr < min_dsr:
                    false_discovery_reasons.append(
                        f"false-discovery daily-net-MTM DSR {dsr_status} {dsr:.4f} < {min_dsr:.4f}"
                    )
                pbo_status = str(false_discovery.get("pbo_status", "unavailable")).upper()
                if pbo_status != "PASS":
                    false_discovery_reasons.append(
                        f"aligned exact-cell PBO {pbo_status}: {false_discovery.get('pbo')}"
                    )
                if bool(getattr(config, "false_discovery_require_bootstrap", True)):
                    bootstrap_status = str(
                        (false_discovery.get("bootstrap_evidence") or {}).get(
                            "status", "UNAVAILABLE"
                        )
                    ).upper()
                    if bootstrap_status != "PASS":
                        false_discovery_reasons.append(
                            f"validation block bootstrap {bootstrap_status}"
                        )
        elif false_discovery and min_dsr > 0.0:
            dsr = float(false_discovery.get("deflated_sharpe", 0.0) or 0.0)
            if dsr < min_dsr:
                false_discovery_reasons.append(
                    f"false-discovery DSR {dsr:.4f} < {min_dsr:.4f}"
                )
    holdout_gate_enabled = (
        false_discovery_enabled
        or scoring_model == "specialist_spokes_v2"
        or split_mode in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}
    )
    if holdout_gate_enabled:
        holdout_policy = str(
            getattr(config, "false_discovery_holdout_policy", "off") or "off"
        ).lower()
        if (
            split_mode == "bleeding_edge"
            and not holdout
            and str(getattr(config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout").lower()
            != "purged_kfold"
        ):
            holdout_policy = "off"
        if holdout_policy != "off":
            min_holdout_trades = int(
                holdout.get(
                    "min_trades_required",
                    getattr(config, "false_discovery_holdout_min_trades", 5),
                )
                or 0
            )
            if str(holdout.get("validation_mode", "") or "").lower() == "purged_kfold":
                min_holdout_trades = int(getattr(config, "oos_min_trades", min_holdout_trades) or 0)
            if 0 < min_holdout_trades and holdout_trades < min_holdout_trades:
                reason = f"holdout trades {holdout_trades} < {min_holdout_trades}"
                if scoring_model == "specialist_spokes_v2" and historical_severe_reasons:
                    return "BLOCKED", [
                        "historical severe confirmed by weak holdout",
                        *historical_severe_reasons,
                        reason,
                    ]
                if holdout_policy == "block":
                    return "BLOCKED", [reason]
                return "PROMOTE_SMALL", [reason]
            if str(holdout.get("policy_status", "") or "").upper() == "FAIL":
                reason = str(holdout.get("policy_reason", "") or "purged-kfold OOS policy failed")
                if scoring_model == "specialist_spokes_v2" and historical_severe_reasons:
                    return "BLOCKED", [
                        "historical severe confirmed by weak holdout",
                        *historical_severe_reasons,
                        reason,
                    ]
                if holdout_policy == "block":
                    return "BLOCKED", [reason]
                return "PROMOTE_SMALL", [reason]
            if holdout_trades >= min_holdout_trades > 0:
                holdout_pf = float(holdout.get("profit_factor", 0.0) or 0.0)
                min_holdout_pf = float(
                    getattr(config, "false_discovery_holdout_min_profit_factor", 1.0) or 0.0
                )
                min_holdout_return = float(
                    getattr(config, "false_discovery_holdout_min_return_pct", 0.0) or 0.0
                )
                max_holdout_drawdown = float(
                    getattr(config, "false_discovery_holdout_max_drawdown_pct", 20.0) or 0.0
                )
                holdout_drawdown = float(holdout.get("max_drawdown_pct", 100.0) or 0.0)
                holdout_reasons = []
                if holdout_pf < min_holdout_pf:
                    holdout_reasons.append(
                        f"holdout PF {holdout_pf:.2f} < {min_holdout_pf:.2f}"
                    )
                if holdout_return < min_holdout_return:
                    holdout_reasons.append(
                        f"holdout return {holdout_return:.1f}% < {min_holdout_return:.1f}%"
                    )
                if max_holdout_drawdown > 0.0 and holdout_drawdown > max_holdout_drawdown:
                    holdout_reasons.append(
                        f"holdout max DD {holdout_drawdown:.1f}% > {max_holdout_drawdown:.1f}%"
                    )
                if holdout_reasons:
                    if scoring_model == "specialist_spokes_v2" and historical_severe_reasons:
                        return "BLOCKED", [
                            "historical severe confirmed by weak holdout",
                            *historical_severe_reasons,
                            *holdout_reasons,
                        ]
                    if holdout_policy == "block":
                        return "BLOCKED", holdout_reasons
                    return "PROMOTE_SMALL", holdout_reasons

    if false_discovery_reasons:
        return "BLOCKED", false_discovery_reasons

    if holdout_trades > 0 and holdout_return <= 0.0:
        if scoring_model == "specialist_spokes_v2" and historical_severe_reasons:
            return "BLOCKED", [
                "historical severe confirmed by weak holdout",
                *historical_severe_reasons,
                "freshest holdout non-positive; risk derated",
            ]
        return "PROMOTE_SMALL", ["freshest holdout non-positive; risk derated"]
    if wf_promote_small_reasons:
        return "PROMOTE_SMALL", wf_promote_small_reasons
    return "PROMOTED", []


def _pipeline_config_to_dict(config: PipelineConfig) -> Dict[str, Any]:
    """Serialize PipelineConfig to a plain dict for multiprocessing."""
    cfg = {f.name: getattr(config, f.name) for f in fields(PipelineConfig)}
    # Normalize Paths to strings for pickling and re-init
    if isinstance(cfg.get("data_dir"), Path):
        cfg["data_dir"] = str(cfg["data_dir"])
    if isinstance(cfg.get("output_dir"), Path):
        cfg["output_dir"] = str(cfg["output_dir"])
    if isinstance(cfg.get("log_dir"), Path):
        cfg["log_dir"] = str(cfg["log_dir"])
    return cfg


def _pipeline_runtime_bindings(config: PipelineConfig) -> Dict[str, Any]:
    """Serialize the only non-dataclass deployment bindings children require."""

    return {
        "deployment_config_path": str(
            getattr(config, "_deployment_config_path", "") or ""
        ),
        "funded_live_execution": bool(
            getattr(config, "_funded_live_execution", False)
        ),
    }


def _restore_pipeline_runtime_bindings(
    config: PipelineConfig,
    bindings: Mapping[str, Any],
) -> None:
    """Restore an exact allowlist; arbitrary parent attributes never cross spawn."""

    expected = {"deployment_config_path", "funded_live_execution"}
    if not isinstance(bindings, Mapping) or set(bindings) != expected:
        raise RuntimeError("OPTIMIZER_RUNTIME_BINDINGS_INVALID")
    config._deployment_config_path = str(bindings["deployment_config_path"] or "")
    config._funded_live_execution = bool(bindings["funded_live_execution"])


def _new_optimizer_memory_governor(requested_workers: int) -> MemoryGovernor:
    """Construct the fresh-process scheduler with no RAM admission policy."""

    return MemoryGovernor(
        requested_workers=max(1, int(requested_workers)),
        termination_grace_seconds=OPTIMIZER_TERMINATION_GRACE_SECONDS,
    )


def _optimizer_receipt_base(config_file: Path) -> Path:
    ledger_path = Path(config_file)
    if not ledger_path.is_absolute():
        ledger_path = Path.cwd() / ledger_path
    ledger_path = ledger_path.resolve()
    return ledger_path.with_name(f".{ledger_path.name}.optimizer-receipts")


def _optimizer_receipt_root(config_file: Path, generation_id: str) -> Path:
    generation_digest = hashlib.sha256(
        str(generation_id).encode("utf-8", errors="strict")
    ).hexdigest()[:24]
    return _optimizer_receipt_base(config_file) / generation_digest


def _optimizer_receipt_path(
    config_file: Path,
    generation_id: str,
    symbol: str,
) -> Path:
    symbol_digest = hashlib.sha256(
        str(symbol).encode("utf-8", errors="strict")
    ).hexdigest()[:24]
    return (
        _optimizer_receipt_root(config_file, generation_id)
        / f"{symbol_digest}.receipt.json"
    )


def _optimizer_receipt_temp_path(receipt_path: Path) -> Path:
    return Path(receipt_path).with_name(f".{Path(receipt_path).name}.tmp")


def _cleanup_optimizer_receipts(paths: Iterable[Path]) -> None:
    """Remove only the exact fixed files allocated by this optimization run."""

    roots = set()
    for candidate in paths:
        path = Path(candidate)
        roots.add(path.parent)
        for exact in (path, _optimizer_receipt_temp_path(path)):
            try:
                exact.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not remove optimizer receipt scratch file %s", exact)
    for root in roots:
        try:
            root.rmdir()
        except OSError:
            # A non-empty directory may contain a receipt owned by another run
            # or an unrelated file.  Never recurse or delete it.
            pass
        try:
            root.parent.rmdir()
        except OSError:
            pass


def _atomic_write_optimizer_receipt(path: Path, payload: Mapping[str, Any]) -> Tuple[str, int]:
    """Write one fixed receipt atomically and return its content identity."""

    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = _optimizer_receipt_temp_path(target)
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    try:
        with temp.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
    finally:
        temp.unlink(missing_ok=True)
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


def _raise_worker_failure(message: str) -> None:
    detail = str(message or "optimizer returned an unsuccessful result")
    if is_memory_failure(detail):
        raise MemoryError(detail)
    raise RuntimeError(detail)


def _governed_optimize_symbol_worker(
    config_dict: Dict[str, Any],
    runtime_bindings: Mapping[str, Any],
    symbol: str,
    generation_id: str,
    receipt_path: str,
) -> Dict[str, Any]:
    """Spawn-safe complete-symbol worker returning only a compact receipt pointer."""

    try:
        cfg = PipelineConfig(**config_dict)
        _restore_pipeline_runtime_bindings(cfg, runtime_bindings)
        pipeline = OptimizationPipeline(cfg)
        result = pipeline.run_for_symbol(symbol)
        data_state = _validated_symbol_data_state(
            (result.data_window_meta or {}).get("symbol_data_state")
        )
        if not result.success and result.config is None and data_state:
            # seq1028: typed terminal data-state receipt; never an exception.
            receipt = {
                "schema_version": OPTIMIZER_DATA_STATE_RECEIPT_SCHEMA,
                "generation_id": str(generation_id),
                "symbol": str(symbol),
                "data_state": data_state,
            }
            resolved_receipt = Path(receipt_path).resolve()
            digest, byte_count = _atomic_write_optimizer_receipt(
                resolved_receipt, receipt
            )
            return {
                "schema_version": OPTIMIZER_RECEIPT_POINTER_SCHEMA,
                "generation_id": str(generation_id),
                "symbol": str(symbol),
                "path": str(resolved_receipt),
                "sha256": digest,
                "bytes": int(byte_count),
            }
        if not result.success or result.config is None:
            _raise_worker_failure(result.error_message)
        counters = {
            "duration_seconds": float(result.duration_seconds),
            "strategies_tested": int(result.strategies_tested),
            "timeframes_tested": int(result.timeframes_tested),
            "param_combos_tested": int(result.param_combos_tested),
            "regime_winners": int(result.regime_winners),
        }
        receipt = {
            "schema_version": OPTIMIZER_RECEIPT_SCHEMA,
            "generation_id": str(generation_id),
            "symbol": str(symbol),
            "config": result.config.to_dict(),
            "counters": counters,
        }
        resolved_receipt = Path(receipt_path).resolve()
        digest, byte_count = _atomic_write_optimizer_receipt(
            resolved_receipt,
            receipt,
        )
        return {
            "schema_version": OPTIMIZER_RECEIPT_POINTER_SCHEMA,
            "generation_id": str(generation_id),
            "symbol": str(symbol),
            "path": str(resolved_receipt),
            "sha256": digest,
            "bytes": int(byte_count),
        }
    except Exception as exc:
        if isinstance(exc, MemoryError):
            raise
        if is_memory_failure(type(exc).__name__) or is_memory_failure(str(exc)):
            raise MemoryError(str(exc)) from exc
        raise
    finally:
        FeatureComputer.clear_cache()


def _governed_precompute_feature_cache_worker(
    config_dict: Dict[str, Any],
    runtime_bindings: Mapping[str, Any],
    symbol: str,
) -> Dict[str, Any]:
    """Spawn-safe feature precompute worker with explicit technical failures."""

    try:
        cfg = PipelineConfig(**config_dict)
        _restore_pipeline_runtime_bindings(cfg, runtime_bindings)
        pipeline = OptimizationPipeline(cfg)
        stats = pipeline.precompute_feature_cache([symbol])
        errors = dict(stats.get("errors", {}) or {})
        if int(stats.get("symbols_failed", 0) or 0) or errors:
            detail = str(errors.get(symbol) or next(iter(errors.values()), "feature precompute failed"))
            _raise_worker_failure(detail)
        return {
            "schema_version": FEATURE_PRECOMPUTE_RESULT_SCHEMA,
            "symbol": str(symbol),
            "symbols_completed": int(stats.get("symbols_completed", 0) or 0),
            "timeframes_computed_or_retrieved": int(
                stats.get("timeframes_computed_or_retrieved", 0) or 0
            ),
            "feature_cache": dict(stats.get("feature_cache", {}) or {}),
        }
    except Exception as exc:
        if isinstance(exc, MemoryError):
            raise
        if is_memory_failure(type(exc).__name__) or is_memory_failure(str(exc)):
            raise MemoryError(str(exc)) from exc
        raise
    finally:
        FeatureComputer.clear_cache()


def _consume_optimizer_receipt(
    pointer: Any,
    *,
    expected_generation_id: str,
    expected_symbol: str,
    expected_path: Path,
) -> 'PipelineResult':
    """Verify, deserialize, and immediately unlink one child receipt."""

    path = Path(expected_path).resolve()
    try:
        if not isinstance(pointer, Mapping):
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_NOT_OBJECT")
        expected_pointer_keys = {
            "schema_version", "generation_id", "symbol", "path", "sha256", "bytes"
        }
        if set(pointer) != expected_pointer_keys:
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_FIELDS_INVALID")
        if str(pointer.get("schema_version")) != OPTIMIZER_RECEIPT_POINTER_SCHEMA:
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_SCHEMA_INVALID")
        if str(pointer.get("generation_id")) != str(expected_generation_id):
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_GENERATION_MISMATCH")
        if str(pointer.get("symbol")) != str(expected_symbol):
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_SYMBOL_MISMATCH")
        try:
            observed_pointer_path = Path(str(pointer.get("path"))).resolve()
        except (OSError, ValueError) as exc:
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_PATH_INVALID") from exc
        if observed_pointer_path != path:
            raise RuntimeError("OPTIMIZER_RECEIPT_POINTER_PATH_MISMATCH")
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("OPTIMIZER_RECEIPT_FILE_MISSING_OR_UNSAFE")
        encoded = path.read_bytes()
        if int(pointer.get("bytes", -1)) != len(encoded):
            raise RuntimeError("OPTIMIZER_RECEIPT_SIZE_MISMATCH")
        if hashlib.sha256(encoded).hexdigest() != str(pointer.get("sha256") or "").lower():
            raise RuntimeError("OPTIMIZER_RECEIPT_HASH_MISMATCH")
        try:
            receipt = json.loads(encoded.decode("utf-8"))
        except Exception as exc:
            raise RuntimeError("OPTIMIZER_RECEIPT_JSON_INVALID") from exc
        if (
            isinstance(receipt, dict)
            and str(receipt.get("schema_version"))
            == OPTIMIZER_DATA_STATE_RECEIPT_SCHEMA
        ):
            # seq1028 typed data-state: exact keys + typed decision fields.
            if set(receipt) != {
                "schema_version", "generation_id", "symbol", "data_state"
            }:
                raise RuntimeError("OPTIMIZER_DATA_STATE_FIELDS_INVALID")
            if str(receipt.get("generation_id")) != str(expected_generation_id):
                raise RuntimeError("OPTIMIZER_DATA_STATE_GENERATION_MISMATCH")
            if str(receipt.get("symbol")) != str(expected_symbol):
                raise RuntimeError("OPTIMIZER_DATA_STATE_SYMBOL_MISMATCH")
            ds = _validated_symbol_data_state(receipt.get("data_state"))
            if ds is None:
                raise RuntimeError("OPTIMIZER_DATA_STATE_PAYLOAD_INVALID")
            return PipelineResult(
                symbol=str(expected_symbol),
                success=False,
                error_message="TYPED_DATA_STATE",
                data_window_meta={"symbol_data_state": dict(ds)},
            )
        expected_receipt_keys = {
            "schema_version", "generation_id", "symbol", "config", "counters"
        }
        if not isinstance(receipt, dict) or set(receipt) != expected_receipt_keys:
            raise RuntimeError("OPTIMIZER_RECEIPT_FIELDS_INVALID")
        if str(receipt.get("schema_version")) != OPTIMIZER_RECEIPT_SCHEMA:
            raise RuntimeError("OPTIMIZER_RECEIPT_SCHEMA_INVALID")
        if str(receipt.get("generation_id")) != str(expected_generation_id):
            raise RuntimeError("OPTIMIZER_RECEIPT_GENERATION_MISMATCH")
        if str(receipt.get("symbol")) != str(expected_symbol):
            raise RuntimeError("OPTIMIZER_RECEIPT_SYMBOL_MISMATCH")
        config_payload = receipt.get("config")
        if not isinstance(config_payload, dict):
            raise RuntimeError("OPTIMIZER_RECEIPT_CONFIG_INVALID")
        config = SymbolConfig.from_dict(config_payload)
        if str(config.symbol) != str(expected_symbol):
            raise RuntimeError("OPTIMIZER_RECEIPT_CONFIG_SYMBOL_MISMATCH")
        counters = receipt.get("counters")
        expected_counter_keys = {
            "duration_seconds", "strategies_tested", "timeframes_tested",
            "param_combos_tested", "regime_winners",
        }
        if not isinstance(counters, dict) or set(counters) != expected_counter_keys:
            raise RuntimeError("OPTIMIZER_RECEIPT_COUNTERS_INVALID")
        return PipelineResult(
            symbol=str(expected_symbol),
            success=True,
            config=config,
            duration_seconds=float(counters["duration_seconds"]),
            strategies_tested=int(counters["strategies_tested"]),
            timeframes_tested=int(counters["timeframes_tested"]),
            param_combos_tested=int(counters["param_combos_tested"]),
            regime_winners=int(counters["regime_winners"]),
            data_window_meta=dict(config.data_window_meta or {}),
        )
    finally:
        path.unlink(missing_ok=True)
        _optimizer_receipt_temp_path(path).unlink(missing_ok=True)


# =============================================================================
# CONFIG LEDGER - STATEFUL OPTIMIZATION PERSISTENCE
# =============================================================================

class ConfigLedger:
    """
    Stateful optimization ledger for pm_configs.json.
    
    Provides:
    - Atomic writes (temp file + rename pattern)
    - Incremental persistence (save after each symbol)
    - Skip valid configs (re-optimize only if expired/missing/invalid)
    - Failure safety (never lose configs on errors)
    
    Usage:
        ledger = ConfigLedger(filepath="pm_configs.json")
        ledger.load()  # Load existing configs
        
        for symbol in symbols:
            if not ledger.should_optimize(symbol, overwrite=False):
                continue  # Skip valid
            
            # Run optimization...
            if success:
                ledger.update_symbol(symbol, config)  # Saves atomically
    """
    
    # Version for future migration support
    LEDGER_VERSION = "2.0"
    IO_RETRIES = 3
    IO_RETRY_SECONDS = 0.15
    
    def __init__(self, filepath: str = "pm_configs.json"):
        """
        Initialize config ledger.
        
        Args:
            filepath: Path to config JSON file
        """
        self.filepath = Path(filepath)
        self.configs: Dict[str, SymbolConfig] = {}
        self._loaded = False
        self._file_signature: Optional[Tuple[int, int]] = None

    def _current_file_signature(self) -> Optional[Tuple[int, int]]:
        """Return a cheap change signature for the on-disk ledger."""
        if not self.filepath.exists():
            return None
        stat = self.filepath.stat()
        return (int(stat.st_mtime_ns), int(stat.st_size))

    def _refresh_if_changed(self) -> None:
        """Reload the ledger when another process has atomically replaced it."""
        if not self._loaded:
            self.load()
            return
        current_signature = self._current_file_signature()
        if current_signature != self._file_signature:
            self.load()

    def _governed_publication_active(self) -> bool:
        """Return whether symbol-wise writes are forbidden for this ledger."""

        path = self.filepath
        if not path.is_absolute():
            path = Path.cwd() / path
        path = path.resolve()
        journal_path = path.with_name(
            f".{path.name}.generation.promotion.json"
        )
        if journal_path.exists() or journal_path.is_symlink():
            return True
        active_manifest = path.with_name(f"{path.stem}.generation.json")
        if not active_manifest.exists():
            return False
        try:
            if active_manifest.stat().st_size > 64 * 1024 * 1024:
                return True
            payload = json.loads(active_manifest.read_text(encoding="utf-8"))
        except Exception:
            return True
        if type(payload) is not dict:
            return True
        return bool(
            payload.get("generation_status") == "PROMOTED"
            and type(payload.get("authority_chain_receipt")) is dict
            and type(payload.get("ledger_status")) is dict
            and payload["ledger_status"].get("complete_current_contract_universe")
            is True
        )
    
    def load(self) -> int:
        """
        Load existing configs from file.
        
        Returns:
            Number of configs loaded
            
        Raises:
            RuntimeError: If file is corrupted JSON
        """
        if not self.filepath.exists():
            logger.info(f"Config file not found: {self.filepath} (starting fresh)")
            self.configs = {}
            self._loaded = True
            self._file_signature = None
            return 0
        
        data = None
        for attempt in range(1, self.IO_RETRIES + 1):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                break
            except json.JSONDecodeError as e:
                if attempt < self.IO_RETRIES:
                    time.sleep(self.IO_RETRY_SECONDS * attempt)
                    continue
                raise RuntimeError(
                    f"Corrupted JSON in {self.filepath}: {e}\n"
                    f"Please fix or remove the file manually, or backup to .corrupt and start fresh."
                )
            except OSError as e:
                if attempt < self.IO_RETRIES:
                    time.sleep(self.IO_RETRY_SECONDS * attempt)
                    continue
                raise RuntimeError(f"Failed to load {self.filepath}: {e}")

        try:
            if data is None:
                raise RuntimeError(f"Failed to load {self.filepath}: no data read")
            self.configs = {}
            for symbol, config_dict in data.items():
                try:
                    self.configs[symbol] = SymbolConfig.from_dict(config_dict)
                except Exception as e:
                    logger.warning(f"Could not parse config for {symbol}: {e}")
            
            self._loaded = True
            self._file_signature = self._current_file_signature()
            logger.info(f"Loaded {len(self.configs)} existing configs from {self.filepath}")
            return len(self.configs)
        except Exception as e:
            raise RuntimeError(f"Failed to load {self.filepath}: {e}")
    
    def _atomic_save(self):
        """Save while sharing the production publication lock."""

        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(_optimization_ledger_lock_path(self.filepath)):
            if self._governed_publication_active():
                raise RuntimeError(
                    "GOVERNED_PRODUCTION_LEDGER_REQUIRES_FULL_GENERATION_CAS"
                )
            self._atomic_save_locked()

    def _atomic_save_locked(self):
        """
        Save configs atomically using temp file + rename pattern.
        
        This ensures configs are never corrupted if the process is interrupted.
        """
        temp_path = self.filepath.with_name(
            f".{self.filepath.name}.{os.getpid()}.{time.time_ns()}.tmp"
        )
        
        # Serialize all configs
        data = {s: c.to_dict() for s, c in self.configs.items()}
        
        try:
            # Write to temp file
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
                f.flush()
                # fsync for durability (optional but recommended)
                os.fsync(f.fileno())
            
            # Atomic replace; retry handles transient Windows/OneDrive locks.
            for attempt in range(1, self.IO_RETRIES + 1):
                try:
                    temp_path.replace(self.filepath)
                    self._file_signature = self._current_file_signature()
                    return
                except OSError:
                    if attempt >= self.IO_RETRIES:
                        raise
                    time.sleep(self.IO_RETRY_SECONDS * attempt)
            
        except Exception as e:
            # Clean up temp file on failure
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError as cleanup_exc:
                    logger.debug("Failed to remove temporary config file %s: %s", temp_path, cleanup_exc)
            raise RuntimeError(f"Failed to save configs: {e}")
    
    def update_symbol(self, symbol: str, config: 'SymbolConfig') -> bool:
        """
        Update a single symbol's config and save atomically.
        
        This is the core incremental persistence method.
        
        Args:
            symbol: Symbol name
            config: SymbolConfig to save
            
        Returns:
            True if saved successfully
        """
        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(_optimization_ledger_lock_path(self.filepath)):
            if not self._loaded:
                self.load()
            else:
                self._refresh_if_changed()
            if self._governed_publication_active():
                logger.error(
                    "Refusing symbol-wise write to governed production ledger %s",
                    self.filepath,
                )
                return False

            previous = self.configs.get(symbol)
            self.configs[symbol] = config

            try:
                self._atomic_save_locked()
                logger.debug(f"Saved {symbol} to {self.filepath} (atomic)")
                return True
            except Exception as e:
                if previous is None:
                    self.configs.pop(symbol, None)
                else:
                    self.configs[symbol] = previous
                logger.error(f"Failed to save {symbol}: {e}")
                return False
    
    def get_config(self, symbol: str) -> Optional['SymbolConfig']:
        """Get config for a symbol."""
        self._refresh_if_changed()
        return self.configs.get(symbol)
    
    def has_valid_config(self, symbol: str) -> Tuple[bool, str]:
        """
        Check if a symbol has a valid (non-expired) config.
        
        Returns:
            Tuple of (is_valid, reason_string)
        """
        self._refresh_if_changed()
        
        if symbol not in self.configs:
            return False, "missing"
        
        config = self.configs[symbol]
        has_tradeable_regime_winners = config.has_tradeable_regime_winners()
        
        # Check validation status
        if not config.is_validated and not has_tradeable_regime_winners:
            return False, f"invalid ({config.validation_reason or 'not validated'})"
        
        # Check expiry
        if config.valid_until is None:
            return False, "no expiry date"
        
        now = datetime.now()
        if now >= config.valid_until:
            days_expired = (now - config.valid_until).days
            return False, f"expired {days_expired} days ago"
        
        # Valid
        days_remaining = (config.valid_until - now).days
        return True, f"valid until {config.valid_until.strftime('%Y-%m-%d %H:%M')} ({days_remaining} days remaining)"
    
    def should_optimize(self, symbol: str, overwrite: bool = False,
                        current_regime_version: Optional[Dict[str, Any]] = None,
                        current_artifact_meta: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """
        Determine if a symbol should be optimized.

        Args:
            symbol: Symbol to check
            overwrite: If True, always optimize (ignore validity)
            current_regime_version: Current regime detection params dict (for version mismatch check)

        Returns:
            Tuple of (should_optimize, reason_string)
        """
        if overwrite:
            return True, "overwrite enabled"

        is_valid, reason = self.has_valid_config(symbol)

        if is_valid:
            if current_artifact_meta and symbol in self.configs:
                stored_artifact = getattr(self.configs[symbol], "artifact_meta", {}) or {}
                if not artifact_contract_matches(stored_artifact, current_artifact_meta):
                    return True, "artifact fingerprint changed"
                if bool(current_artifact_meta.get("oos_evidence_export_enabled", False)):
                    sidecar_ok, sidecar_reason = self._oos_evidence_sidecar_valid(
                        self.configs[symbol],
                        current_artifact_meta,
                    )
                    if not sidecar_ok:
                        return True, sidecar_reason
            # Extra check: regime detection params changed since optimization
            if current_regime_version and symbol in self.configs:
                stored = self.configs[symbol].regime_detection_version
                if stored and stored != current_regime_version:
                    return True, "regime detection params changed"
            return False, reason  # Skip - config is valid

        return True, reason  # Optimize - config is missing/expired/invalid

    @staticmethod
    def _oos_evidence_sidecar_valid(config: 'SymbolConfig',
                                    current_artifact_meta: Dict[str, Any]) -> Tuple[bool, str]:
        pointer = dict((getattr(config, "data_window_meta", {}) or {}).get("oos_candidate_evidence") or {})
        manifest_value = pointer.get("path")
        if not manifest_value:
            return False, "OOS evidence sidecar missing"
        manifest_path = Path(str(manifest_value))
        if not manifest_path.exists():
            return False, "OOS evidence manifest missing"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return False, "OOS evidence manifest unreadable"
        if manifest.get("format_version") != "oos_candidate_matrix_v1":
            return False, "OOS evidence manifest version changed"
        if not artifact_contract_matches(
            dict(manifest.get("artifact_meta") or {}),
            current_artifact_meta,
        ):
            return False, "OOS evidence artifact fingerprint changed"
        matrix_value = manifest.get("matrix_path")
        if not matrix_value:
            return False, "OOS evidence matrix missing"
        matrix_path = Path(str(matrix_value))
        if not matrix_path.exists():
            fallback = manifest_path.parent / matrix_path.name
            matrix_path = fallback if fallback.exists() else matrix_path
        if not matrix_path.exists():
            return False, "OOS evidence matrix missing"
        try:
            with np.load(matrix_path) as matrix:
                returns = np.asarray(matrix["returns"], dtype=np.float32)
                candidate_ids = np.asarray(matrix["candidate_ids"])
        except Exception:
            return False, "OOS evidence matrix unreadable"
        contract = dict(manifest.get("matrix_contract") or {})
        expected_shape = contract.get("returns_shape")
        if expected_shape and list(returns.shape) != list(expected_shape):
            return False, "OOS evidence matrix shape mismatch"
        record_count = int(manifest.get("record_count", 0) or 0)
        pointer_count = int(pointer.get("record_count", record_count) or 0)
        if record_count <= 0 or returns.ndim != 2 or returns.shape[1] != record_count:
            return False, "OOS evidence record count mismatch"
        if pointer_count != record_count or len(candidate_ids) != record_count:
            return False, "OOS evidence pointer count mismatch"
        digest_payload = returns.tobytes() + json.dumps(
            candidate_ids.tolist(),
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha1(digest_payload).hexdigest()
        if digest != str(manifest.get("matrix_digest_sha1", "")):
            return False, "OOS evidence matrix digest mismatch"
        return True, "OOS evidence sidecar valid"
    
    def get_symbols_to_optimize(self,
                                symbols: List[str],
                                overwrite: bool = False,
                                current_artifact_meta: Optional[Dict[str, Any]] = None
                                ) -> Tuple[List[str], List[str]]:
        """
        Partition symbols into those needing optimization and those to skip.
        
        Args:
            symbols: All symbols to consider
            overwrite: If True, optimize all
            
        Returns:
            Tuple of (symbols_to_optimize, symbols_to_skip)
        """
        self._refresh_if_changed()
        
        to_optimize = []
        to_skip = []
        
        for symbol in symbols:
            should_opt, reason = self.should_optimize(
                symbol, overwrite, current_artifact_meta=current_artifact_meta
            )
            if should_opt:
                to_optimize.append(symbol)
                logger.info(f"OPTIMIZE {symbol}: {reason}")
            else:
                to_skip.append(symbol)
                logger.info(f"SKIP {symbol}: {reason}")
        
        return to_optimize, to_skip
    
    def get_all_configs(self) -> Dict[str, 'SymbolConfig']:
        """Get all loaded configs."""
        self._refresh_if_changed()
        return self.configs.copy()
    
    def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol's config (use with caution).
        
        Args:
            symbol: Symbol to remove
            
        Returns:
            True if removed and saved
        """
        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(_optimization_ledger_lock_path(self.filepath)):
            self._refresh_if_changed()
            if self._governed_publication_active():
                logger.error(
                    "Refusing symbol-wise removal from governed production ledger %s",
                    self.filepath,
                )
                return False

            if symbol not in self.configs:
                return False

            previous = self.configs[symbol]
            del self.configs[symbol]

            try:
                self._atomic_save_locked()
                logger.info(f"Removed {symbol} from {self.filepath}")
                return True
            except Exception as e:
                self.configs[symbol] = previous
                logger.error(f"Failed to remove {symbol}: {e}")
                return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get ledger statistics."""
        self._refresh_if_changed()
        
        now = datetime.now()
        valid = 0
        expired = 0
        invalid = 0
        artifact_contract_counts: Dict[str, int] = {}
        
        for config in self.configs.values():
            artifact = getattr(config, "artifact_meta", {}) or {}
            artifact_key = "|".join([
                str(artifact.get("artifact_version", "missing")),
                str(artifact.get("split_contract_version", "missing")),
            ])
            artifact_contract_counts[artifact_key] = artifact_contract_counts.get(artifact_key, 0) + 1
            if not config.is_validated:
                invalid += 1
            elif not config.valid_until:
                invalid += 1
            elif config.valid_until and now >= config.valid_until:
                expired += 1
            else:
                valid += 1
        
        return {
            'total': len(self.configs),
            'valid': valid,
            'expired': expired,
            'invalid': invalid,
            'artifact_contract_counts': artifact_contract_counts,
            'filepath': str(self.filepath)
        }


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RegimeConfig:
    """
    Configuration for a single (timeframe, regime) winner.
    
    Stores the best strategy+params for a specific regime on a specific timeframe.
    """
    strategy_name: str
    parameters: Dict[str, Any]
    quality_score: float  # Normalized 0-1 for cross-TF comparison
    
    # Performance metrics for this regime bucket
    train_metrics: Dict[str, Any] = field(default_factory=dict)
    val_metrics: Dict[str, Any] = field(default_factory=dict)
    holdout_metrics: Dict[str, Any] = field(default_factory=dict)
    historical_audit_metrics: Dict[str, Any] = field(default_factory=_default_historical_audit)
    
    # Regime-specific stats
    regime_train_trades: int = 0
    regime_val_trades: int = 0
    
    # Timestamps
    trained_at: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    artifact_meta: Dict[str, Any] = field(default_factory=dict)
    validation_evidence: Dict[str, Any] = field(default_factory=dict)
    live_observability: Dict[str, Any] = field(default_factory=_default_live_observability)
    governance_policy: Dict[str, Any] = field(default_factory=_default_governance_policy)
    walk_forward_audit: Dict[str, Any] = field(default_factory=_default_walk_forward_audit)
    # A candidate becomes live-eligible only after the promotion pipeline writes
    # an explicit verdict. Missing/legacy status is never treated as approval.
    promotion_status: str = "BLOCKED"
    promotion_reasons: List[str] = field(default_factory=list)
    
    def is_no_trade_marker(self) -> bool:
        """Check if this config is a NO_TRADE marker (no valid strategy found)."""
        return (
            self.strategy_name == "NO_TRADE" or
            self.strategy_name == "" or
            bool((self.artifact_meta or {}).get("no_trade"))
        )
    
    def is_valid_for_live(
        self,
        min_pf: float = 1.0,
        min_return: float = 5.0,
        max_dd: float = 35.0,
        min_return_dd_ratio: float = 1.0,
    ) -> bool:
        """
        Check if this config passes the live quality gate.
        
        A config is valid for live trading if:
        - It's not a NO_TRADE marker
        - Validation profit_factor >= min_pf
        - Validation return >= min_return
        - Validation drawdown <= max_dd
        - Validation return/DD ratio >= min_return_dd_ratio
        
        Args:
            min_pf: Minimum validation profit factor (default 1.0)
            min_return: Minimum validation return % (default 5.0)
            max_dd: Maximum validation drawdown % (default 35.0)
            min_return_dd_ratio: Minimum validation return/DD ratio (default 1.0)
            
        Returns:
            True if this config is valid for live trading
        """
        if self.is_no_trade_marker():
            return False
        if temporal_role_block_reason(self.validation_evidence or {}):
            return False
        
        val_pf = self.val_metrics.get('profit_factor', 0.0)
        val_return = self.val_metrics.get('total_return_pct', -100.0)
        val_dd = self.val_metrics.get('max_drawdown_pct', 100.0)

        ratio = val_return / max(val_dd, 0.5)
        return (
            val_pf >= min_pf and
            val_return >= min_return and
            val_dd <= max_dd and
            ratio >= min_return_dd_ratio
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'strategy_name': self.strategy_name,
            'parameters': self.parameters,
            'quality_score': self.quality_score,
            'regime_train_trades': self.regime_train_trades,
            'regime_val_trades': self.regime_val_trades,
            'trained_at': self.trained_at.isoformat() if self.trained_at else None,
            'valid_until': self.valid_until.isoformat() if self.valid_until else None,
            'is_validated': (
                not self.is_no_trade_marker()
                and str(self.promotion_status or "BLOCKED").upper() in {"PROMOTED", "PROMOTE_SMALL"}
            ),
            'train_metrics': {k: v for k, v in self.train_metrics.items() 
                            if k not in ['equity_curve', 'trades']},
            'val_metrics': {k: v for k, v in self.val_metrics.items() 
                          if k not in ['equity_curve', 'trades']},
            'holdout_metrics': {k: v for k, v in self.holdout_metrics.items()
                                if k not in ['equity_curve', 'trades']},
            'historical_audit_metrics': {
                k: v for k, v in (self.historical_audit_metrics or _default_historical_audit()).items()
                if k not in ['equity_curve', 'trades']
            },
            'artifact_meta': self.artifact_meta,
            'validation_evidence': dict(self.validation_evidence or {}),
            'live_observability': dict(self.live_observability or _default_live_observability()),
            'governance_policy': dict(self.governance_policy or _default_governance_policy()),
            'walk_forward_audit': dict(self.walk_forward_audit or _default_walk_forward_audit()),
            'promotion_status': str(self.promotion_status or "BLOCKED"),
            'promotion_reasons': list(self.promotion_reasons or []),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegimeConfig':
        """Create from dictionary (applies strategy name migration for retired strategies)."""
        raw_name = data['strategy_name']
        strategy_name = _STRATEGY_MIGRATION.get(raw_name, raw_name)
        if strategy_name != raw_name:
            logger.info(f"Migrated retired strategy {raw_name} → {strategy_name}")
        config = cls(
            strategy_name=strategy_name,
            parameters=data['parameters'],
            quality_score=data.get('quality_score', 0.0),
            train_metrics=data.get('train_metrics', {}),
            val_metrics=data.get('val_metrics', {}),
            holdout_metrics=data.get('holdout_metrics', {}),
            historical_audit_metrics=data.get('historical_audit_metrics') or _default_historical_audit(),
            regime_train_trades=data.get('regime_train_trades', 0),
            regime_val_trades=data.get('regime_val_trades', 0),
            artifact_meta=data.get('artifact_meta', {}),
            validation_evidence=data.get('validation_evidence', {}),
            live_observability=data.get('live_observability') or _default_live_observability(),
            governance_policy=data.get('governance_policy') or _default_governance_policy(),
            walk_forward_audit=data.get('walk_forward_audit') or _default_walk_forward_audit(),
            promotion_status=data.get('promotion_status', 'BLOCKED'),
            promotion_reasons=list(data.get('promotion_reasons', []) or []),
        )
        
        if data.get('trained_at'):
            config.trained_at = datetime.fromisoformat(data['trained_at'])
        if data.get('valid_until'):
            config.valid_until = datetime.fromisoformat(data['valid_until'])
        
        return config


@dataclass
class SymbolConfig:
    """
    Optimized configuration for a single symbol with regime-aware strategy selection.
    
    Structure:
    - regime_configs: Dict[timeframe][regime] -> RegimeConfig (winners per tf/regime)
    - default_config: RegimeConfig (fallback when no regime winner exists)
    """
    symbol: str
    
    # Per-(timeframe, regime) winners
    # Structure: {"H1": {"TREND": RegimeConfig, "RANGE": RegimeConfig, ...}, ...}
    regime_configs: Dict[str, Dict[str, RegimeConfig]] = field(default_factory=dict)
    
    # Default fallback (best overall, used when no regime winner exists)
    default_config: Optional[RegimeConfig] = None
    
    # Legacy fields (populated from default_config for compatibility)
    strategy_name: str = ""
    timeframe: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    retrain_days: int = 14  # Compatibility alias; live validity is schedule-driven.

    # Performance metrics (from default)
    train_metrics: Dict[str, Any] = field(default_factory=dict)
    val_metrics: Dict[str, Any] = field(default_factory=dict)
    holdout_metrics: Dict[str, Any] = field(default_factory=dict)
    historical_audit_metrics: Dict[str, Any] = field(default_factory=_default_historical_audit)
    data_window_meta: Dict[str, Any] = field(default_factory=dict)
    # A physically separate, non-funded book for a latest-data refit.  It must
    # never replace regime_configs, which is the lockbox-validated champion.
    edge_challenger_book: Dict[str, Any] = field(default_factory=dict)
    composite_score: float = 0.0
    robustness_ratio: float = 0.0
    
    # Validation status
    is_validated: bool = False
    validation_reason: str = ""
    validation_evidence: Dict[str, Any] = field(default_factory=dict)

    # Regime detection version stamp for invalidation when params change.
    regime_detection_version: Optional[Dict[str, Any]] = None
    artifact_meta: Dict[str, Any] = field(default_factory=dict)
    live_observability: Dict[str, Any] = field(default_factory=_default_live_observability)
    walk_forward_audit: Dict[str, Any] = field(default_factory=_default_walk_forward_audit)

    # Timestamps
    optimized_at: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    
    def has_regime_configs(self) -> bool:
        """Check if this config has regime configs."""
        return bool(self.regime_configs)
    
    def get_regime_config(self, timeframe: str, regime: str) -> Optional[RegimeConfig]:
        """Get config for a specific (timeframe, regime) pair."""
        if timeframe in self.regime_configs:
            return self.regime_configs[timeframe].get(regime)
        return None
    
    def get_available_timeframes(self) -> List[str]:
        """Get list of timeframes with regime configs."""
        return list(self.regime_configs.keys())
    
    def get_regimes_for_timeframe(self, timeframe: str) -> List[str]:
        """Get list of regimes configured for a timeframe."""
        if timeframe in self.regime_configs:
            return list(self.regime_configs[timeframe].keys())
        return []
    
    def count_regime_winners(self) -> int:
        """Count validated tradeable regime winners across all timeframes."""
        non_live_statuses = {"BLOCKED", "PAPER_ONLY", "NO_TRADE"}
        return sum(
            1
            for regimes in self.regime_configs.values()
            for cfg in regimes.values()
            if (
                cfg is not None
                and not cfg.is_no_trade_marker()
                and str(getattr(cfg, "promotion_status", "BLOCKED") or "BLOCKED").upper()
                not in non_live_statuses
            )
        )

    def has_tradeable_regime_winners(self) -> bool:
        """True when at least one regime/timeframe cell is live-tradeable."""
        return self.has_regime_configs() and self.count_regime_winners() > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            'symbol': self.symbol,
            'strategy_name': self.strategy_name,
            'timeframe': self.timeframe,
            'parameters': self.parameters,
            'retrain_days': self.retrain_days,
            'composite_score': self.composite_score,
            'robustness_ratio': self.robustness_ratio,
            'is_validated': bool(self.is_validated or self.has_tradeable_regime_winners()),
            'validation_reason': self.validation_reason,
            'validation_evidence': dict(self.validation_evidence or {}),
            'train_return': float(self.train_metrics.get('total_return_pct', 0.0)) if self.train_metrics else 0.0,
            'train_sharpe': float(self.train_metrics.get('sharpe_ratio', 0.0)) if self.train_metrics else 0.0,
            'val_return': float(self.val_metrics.get('total_return_pct', 0.0)) if self.val_metrics else 0.0,
            'val_sharpe': float(self.val_metrics.get('sharpe_ratio', 0.0)) if self.val_metrics else 0.0,
            'optimized_at': self.optimized_at.isoformat() if self.optimized_at else None,
            'valid_until': self.valid_until.isoformat() if self.valid_until else None,
            'train_metrics': {k: v for k, v in self.train_metrics.items() 
                            if k not in ['equity_curve', 'trades']},
            'val_metrics': {k: v for k, v in self.val_metrics.items() 
                          if k not in ['equity_curve', 'trades']},
            'holdout_metrics': {k: v for k, v in self.holdout_metrics.items()
                                if k not in ['equity_curve', 'trades']},
            'historical_audit_metrics': {
                k: v for k, v in (self.historical_audit_metrics or _default_historical_audit()).items()
                if k not in ['equity_curve', 'trades']
            },
            'data_window_meta': dict(self.data_window_meta or {}),
            'edge_challenger_book': dict(self.edge_challenger_book or {}),
            'artifact_meta': self.artifact_meta,
            'live_observability': dict(self.live_observability or _default_live_observability()),
            'walk_forward_audit': dict(self.walk_forward_audit or _default_walk_forward_audit()),
        }
        if self.default_config:
            result['promotion_status'] = str(self.default_config.promotion_status or "BLOCKED")
            result['promotion_reasons'] = list(self.default_config.promotion_reasons or [])

        if self.regime_detection_version:
            result['regime_detection_version'] = self.regime_detection_version

        # Add regime configs
        if self.regime_configs:
            result['regime_configs'] = {}
            for tf, regime_dict in self.regime_configs.items():
                result['regime_configs'][tf] = {}
                for regime, cfg in regime_dict.items():
                    result['regime_configs'][tf][regime] = cfg.to_dict()
        
        # Default config
        if self.default_config:
            result['default_config'] = self.default_config.to_dict()
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SymbolConfig':
        """Create from dictionary (applies strategy name migration for retired strategies)."""
        raw_name = data.get('strategy_name', '')
        strategy_name = _STRATEGY_MIGRATION.get(raw_name, raw_name)
        config = cls(
            symbol=data['symbol'],
            strategy_name=strategy_name,
            timeframe=data.get('timeframe', ''),
            parameters=data.get('parameters', {}),
            retrain_days=data.get('retrain_days', 14),
            composite_score=data.get('composite_score', 0.0),
            robustness_ratio=data.get('robustness_ratio', 0.0),
            is_validated=data.get('is_validated', False),
            validation_reason=data.get('validation_reason', ''),
            validation_evidence=data.get('validation_evidence', {}),
            train_metrics=data.get('train_metrics', {}),
            val_metrics=data.get('val_metrics', {}),
            holdout_metrics=data.get('holdout_metrics', {}),
            historical_audit_metrics=data.get('historical_audit_metrics') or _default_historical_audit(),
            data_window_meta=data.get('data_window_meta', {}),
            edge_challenger_book=data.get('edge_challenger_book', {}),
            artifact_meta=data.get('artifact_meta', {}),
            live_observability=data.get('live_observability') or _default_live_observability(),
            walk_forward_audit=data.get('walk_forward_audit') or _default_walk_forward_audit(),
        )
        
        if data.get('optimized_at'):
            config.optimized_at = datetime.fromisoformat(data['optimized_at'])
        if data.get('valid_until'):
            config.valid_until = datetime.fromisoformat(data['valid_until'])
        config.regime_detection_version = data.get('regime_detection_version')
        
        # Load regime configs
        if 'regime_configs' in data and data['regime_configs']:
            config.regime_configs = {}
            for tf, regime_dict in data['regime_configs'].items():
                config.regime_configs[tf] = {}
                for regime, cfg_data in regime_dict.items():
                    config.regime_configs[tf][regime] = RegimeConfig.from_dict(cfg_data)
        
        # Default config
        if 'default_config' in data and data['default_config']:
            config.default_config = RegimeConfig.from_dict(data['default_config'])
        
        return config


@dataclass
class PipelineResult:
    """Result of a pipeline run."""
    symbol: str
    success: bool
    config: Optional[SymbolConfig] = None
    error_message: str = ""
    duration_seconds: float = 0.0
    
    # Intermediate results
    strategies_tested: int = 0
    timeframes_tested: int = 0
    param_combos_tested: int = 0
    regime_winners: int = 0
    data_window_meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OptimizationGenerationRequest:
    """Code-owned target set for one governed, atomic ledger generation."""

    kind: str
    target_symbols: Tuple[str, ...]
    overwrite_targets: bool = False
    require_fresh_native_activation: bool = False
    supervised_generation_id: Optional[str] = None


@dataclass
class OptimizationGenerationOutcome:
    """Bounded operator result for an optimization generation."""

    generation_id: str
    status: str
    requested_symbols: Tuple[str, ...]
    effective_symbols: Tuple[str, ...]
    retained_symbols: Tuple[str, ...]
    excluded_symbols: Tuple[str, ...]
    results: Dict[str, PipelineResult] = field(default_factory=dict)
    technical_failures: Dict[str, str] = field(default_factory=dict)
    promoted: bool = False
    ledger_changed: bool = False
    base_ledger_sha256: str = ""
    promoted_ledger_sha256: str = ""
    manifest_path: str = ""
    replay_evidence: Dict[str, Any] = field(default_factory=dict)
    non_promotable: Dict[str, Any] = field(default_factory=dict)
    authority_chain_receipt: Dict[str, Any] = field(default_factory=dict)
    authority_artifacts: Dict[str, Any] = field(default_factory=dict)
    authority_assembly_path: str = ""


# =============================================================================
# PIPELINE STAGES
# =============================================================================


class StrategySelector:
    """
    Strategy selector for each symbol.

    Tests all strategies on training data for each timeframe.
    In fx_backtester mode, selection is validation-aware: evaluate top-K candidates on validation.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.backtester = Backtester(config)
        self.scorer = StrategyScorer(config)

    def select_best(self,
                    symbol: str,
                    train_data_by_tf: Dict[str, pd.DataFrame],
                    strategies: List[BaseStrategy],
                    val_data_by_tf: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[Optional[str], Optional[str], Dict]:
            """
            Select best strategy and timeframe for a symbol.

            In fx_backtester mode, this is validation-aware without being expensive:
              1) score all (strategy,timeframe) candidates on TRAIN
              2) take top-K by train score
              3) evaluate those K candidates on VALIDATION
              4) pick the winner by a generalization-aware score (val-first + gap penalty + robustness boost)

            Args:
                symbol: Symbol name
                train_data_by_tf: Dict mapping timeframe -> training DataFrame
                strategies: List of strategy instances to test
                val_data_by_tf: Optional dict mapping timeframe -> validation DataFrame (recommended)

            Returns:
                (best_strategy_name, best_timeframe, best_metrics_train)
            """
            best_strategy: Optional[str] = None
            best_timeframe: Optional[str] = None
            best_metrics: Dict[str, Any] = {}
            best_final_score: float = -float("inf")

            logger.info(f"[{symbol}] Testing {len(strategies)} strategies x {len(train_data_by_tf)} timeframes")

            # Feature cache: compute once per timeframe for train/val
            train_feat_by_tf: Dict[str, pd.DataFrame] = {}
            val_feat_by_tf: Dict[str, pd.DataFrame] = {}

            # ------------------------------------------------------------
            # Pass 1: evaluate TRAIN only (cheap) and collect candidates
            # ------------------------------------------------------------
            candidates: List[Dict[str, Any]] = []
            for timeframe, train_data in train_data_by_tf.items():
                if train_data is None or len(train_data) < 100:
                    logger.warning(f"[{symbol}] Skipping {timeframe}: insufficient training data")
                    continue

                if timeframe not in train_feat_by_tf:
                    train_feat_by_tf[timeframe] = FeatureComputer.compute_all(train_data)
                train_features = train_feat_by_tf[timeframe]

                for strategy in strategies:
                    try:
                        signals = strategy.generate_signals(train_features, symbol)
                        metrics = self.backtester.run(
                            features=train_features,
                            signals=signals,
                            symbol=symbol,
                            strategy=strategy
                        )

                        if metrics.get('total_trades', 0) < self.config.min_trades:
                            continue
                        train_score = float(self.scorer.score(metrics, purpose="selection"))
                        candidates.append({
                            "strategy_name": strategy.name,
                            "timeframe": timeframe,
                            "train_metrics": metrics,
                            "train_score": train_score
                        })
                    except Exception as e:
                        logger.warning(f"  [{symbol}] Error testing {strategy.name} {timeframe}: {e}")
                        continue

            if not candidates:
                logger.warning(f"[{symbol}] No valid strategy candidates found")
                return None, None, {}

            # ------------------------------------------------------------
            # Pass 2: validation-aware selection
            # ------------------------------------------------------------
            if val_data_by_tf is not None:
                top_k = int(getattr(self.config, "fx_selection_top_k", 5))
                top_k = max(1, top_k)

                candidates.sort(key=lambda d: d["train_score"], reverse=True)
                shortlist = candidates[:top_k]

                for cand in shortlist:
                    tf = cand["timeframe"]
                    val_df = val_data_by_tf.get(tf) if val_data_by_tf else None
                    if val_df is None or len(val_df) < 50:
                        continue

                    if tf not in val_feat_by_tf:
                        val_feat_by_tf[tf] = FeatureComputer.compute_all(val_df)
                    val_features = val_feat_by_tf[tf]

                    try:
                        strat = StrategyRegistry.get(cand["strategy_name"])
                        signals = strat.generate_signals(val_features, symbol)
                        val_metrics = self.backtester.run(val_features, signals, symbol, strat)

                        # Enforce basic validation guardrails
                        val_trades = int(val_metrics.get("total_trades", 0))
                        val_dd = float(val_metrics.get("max_drawdown_pct", 100.0))
                        if val_trades < self.config.fx_val_min_trades:
                            continue
                        if val_dd >= self.config.fx_val_max_drawdown:
                            continue

                        final_score, train_score, val_score, rr = self.scorer.fx_generalization_score(
                            cand["train_metrics"], val_metrics, purpose="selection"
                        )

                        if final_score > best_final_score:
                            best_final_score = final_score
                            best_strategy = cand["strategy_name"]
                            best_timeframe = tf
                            best_metrics = cand["train_metrics"]

                    except Exception as e:
                        logger.debug(f"[{symbol}] Validation eval failed for {cand['strategy_name']} {tf}: {e}")
                        continue

                if best_strategy:
                    logger.info(f"[{symbol}] Selected: {best_strategy} @ {best_timeframe} "
                                f"(GenScore: {best_final_score:.1f})")
                    return best_strategy, best_timeframe, best_metrics

                logger.info(f"[{symbol}] No validation-qualified selection winner")
                return None, None, {}

            # ------------------------------------------------------------
            # No validation data: fall back to best train score
            # ------------------------------------------------------------
            best_train = max(candidates, key=lambda d: d["train_score"])
            logger.info(f"[{symbol}] Selected: {best_train['strategy_name']} @ {best_train['timeframe']} "
                        f"(Score: {best_train['train_score']:.1f})")
            return best_train["strategy_name"], best_train["timeframe"], best_train["train_metrics"]

class HyperparameterOptimizer:
    """
    Stage 2: Hyperparameter Optimization
    
    Uses Optuna TPE (Tree-structured Parzen Estimator) for efficient
    hyperparameter search. Falls back to grid search when Optuna unavailable.
    
    TPE is well-suited for:
    - Discrete/categorical parameter spaces (period lengths, multipliers)
    - Parameter constraints (fast_period < slow_period)
    - Mixed parameter types
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.backtester = Backtester(config)
        self.scorer = StrategyScorer(config)
        
        # Initialize Optuna optimizer if available
        if OPTUNA_AVAILABLE and OptunaTPEOptimizer is not None:
            self.optuna_optimizer = create_optimizer(
                config, self.backtester, self.scorer, StrategyRegistry
            )
            logger.debug(f"HyperparameterOptimizer using: {get_optimization_method()}")
        else:
            self.optuna_optimizer = None
            logger.debug("HyperparameterOptimizer using: Grid Search (Optuna not available)")
    
    def optimize(self,
                 symbol: str,
                 strategy_name: str,
                 train_features: pd.DataFrame,
                 val_features: pd.DataFrame,
                 timeframe: str = "") -> Tuple[Dict[str, Any], Dict, Dict]:
        """
        Optimize strategy parameters using Optuna TPE.
        
        Args:
            symbol: Symbol name
            strategy_name: Name of strategy to optimize
            train_features: Training data with features
            val_features: Validation data with features
            
        Returns:
            Tuple of (best_params, train_metrics, val_metrics)
        """
        # Get strategy and param grid
        strategy = StrategyRegistry.get(strategy_name)
        param_grid = strategy.get_param_grid()
        
        if not param_grid:
            # No params to optimize, use defaults
            logger.info(f"[{symbol}] No param grid for {strategy_name}, using defaults")
            
            signals = strategy.generate_signals(train_features, symbol)
            train_metrics = self.backtester.run(
                train_features, signals, symbol, strategy, timeframe=timeframe
            )
            
            if val_features is not None and len(val_features) >= 50:
                signals = strategy.generate_signals(val_features, symbol)
                val_metrics = self.backtester.run(
                    val_features, signals, symbol, strategy, timeframe=timeframe
                )
            else:
                val_metrics = {'total_return_pct': 0.0, 'sharpe_ratio': 0.0,
                               'max_drawdown_pct': 0.0, 'total_trades': 0}
            
            return strategy.get_params(), train_metrics, val_metrics
        
        # Use Optuna TPE if available
        if self.optuna_optimizer is not None:
            return self._optimize_with_optuna(
                symbol, strategy_name, param_grid, train_features, val_features, timeframe
            )
        
        # Fallback to grid search
        return self._optimize_grid_search(
            symbol, strategy_name, param_grid, train_features, val_features, timeframe
        )
    
    def _optimize_with_optuna(self,
                              symbol: str,
                              strategy_name: str,
                              param_grid: Dict[str, List],
                              train_features: pd.DataFrame,
                              val_features: pd.DataFrame,
                              timeframe: str = "") -> Tuple[Dict[str, Any], Dict, Dict]:
        """Optimize using Optuna TPE."""
        
        # FX backtester scoring: validation-aware when enough val trades, otherwise train-with-discount
        def scoring_fn(train_metrics: Dict, val_metrics: Dict) -> float:
            if val_metrics.get('total_trades', 0) >= self.config.fx_val_min_trades:
                val_dd = float(val_metrics.get('max_drawdown_pct', 100.0))
                if val_dd >= self.config.fx_val_max_drawdown:
                    return -500.0  # Penalty for excessive drawdown
                final_score, _, _, _ = self.scorer.fx_generalization_score(
                    train_metrics, val_metrics, purpose="opt"
                )
                return final_score
            # Not enough validation trades - use train with discount
            return self.scorer.score(train_metrics, purpose="opt") * 0.7
        
        # Get n_trials from config
        n_trials = getattr(self.config, 'max_param_combos', 100)
        
        # Update optimizer config
        if hasattr(self.optuna_optimizer, 'config'):
            self.optuna_optimizer.config.n_trials = n_trials
        
        # Run optimization
        result = self.optuna_optimizer.optimize(
            symbol=symbol,
            timeframe=timeframe,
            strategy_name=strategy_name,
            param_grid=param_grid,
            train_features=train_features,
            val_features=val_features,
            scoring_fn=scoring_fn,
            min_trades=self.config.fx_opt_min_trades
        )
        
        logger.info(f"[{symbol}] Optuna optimization for {strategy_name}: "
                   f"{result.stats.n_trials} trials, best_score={result.best_score:.2f}")
        
        return result.best_params, result.train_metrics, result.val_metrics
    
    def _optimize_grid_search(self,
                              symbol: str,
                              strategy_name: str,
                              param_grid: Dict[str, List],
                              train_features: pd.DataFrame,
                              val_features: pd.DataFrame,
                              timeframe: str = "") -> Tuple[Dict[str, Any], Dict, Dict]:
        """Fallback grid search optimization."""
        strategy = StrategyRegistry.get(strategy_name)
        default_params = strategy.get_params()
        
        # Decode at most the configured cap; never allocate the full grid.
        param_names = list(param_grid.keys())
        param_values = [list(param_grid[name] or []) for name in param_names]
        _seed = _stable_seed(
            symbol,
            strategy_name,
            "grid",
            len(train_features),
            str(train_features.index[0]) if len(train_features) else "",
            str(train_features.index[-1]) if len(train_features) else "",
            len(val_features),
        )
        combinations, _total_space = _bounded_cartesian_combinations(
            param_values,
            limit=self.config.max_param_combos,
            seed=_seed,
        )
        
        logger.info(f"[{symbol}] Grid search {strategy_name}: {len(combinations)} combinations")

        # FX backtester selection: train-score shortlist, then validation-aware tiebreak.
        candidates: List[Dict[str, Any]] = []

        for combo in combinations:
            params = dict(zip(param_names, combo))
            test_params = {**default_params, **params}
            test_strategy = StrategyRegistry.get(strategy_name, **test_params)

            try:
                signals = test_strategy.generate_signals(train_features, symbol)
                train_metrics = self.backtester.run(
                    train_features, signals, symbol, test_strategy, timeframe=timeframe
                )

                if train_metrics.get('total_trades', 0) < self.config.fx_opt_min_trades:
                    continue

                train_score = float(self.scorer.score(train_metrics, purpose="opt"))
                candidates.append({
                    "params": dict(test_params),
                    "train_metrics": train_metrics,
                    "train_score": train_score
                })
            except Exception as e:
                logger.debug(f"[{symbol}] Grid opt {strategy_name} combo failed: {e}")
                continue

        if not candidates:
            candidates.append({
                "params": dict(default_params),
                "train_metrics": {},
                "train_score": -float("inf")
            })

        candidates.sort(key=lambda d: d["train_score"], reverse=True)
        top_k = max(1, int(getattr(self.config, "fx_opt_top_k", 5)))
        shortlist = candidates[:top_k]

        best_final = -float("inf")
        best_params = dict(default_params)
        best_train_metrics: Dict[str, Any] = {}
        best_val_metrics: Dict[str, Any] = {}

        for cand in shortlist:
            test_params = cand["params"]
            test_strategy = StrategyRegistry.get(strategy_name, **test_params)
            train_metrics = cand.get("train_metrics") or {}

            if not train_metrics:
                try:
                    signals = test_strategy.generate_signals(train_features, symbol)
                    train_metrics = self.backtester.run(
                        train_features, signals, symbol, test_strategy, timeframe=timeframe
                    )
                except Exception as e:
                    logger.debug(f"[{symbol}] Val-shortlist train {strategy_name} failed: {e}")
                    continue

            if val_features is None or len(val_features) < 50:
                val_metrics = {'total_return_pct': 0.0, 'sharpe_ratio': 0.0,
                               'max_drawdown_pct': 0.0, 'total_trades': 0}
            else:
                try:
                    signals = test_strategy.generate_signals(val_features, symbol)
                    val_metrics = self.backtester.run(
                        val_features, signals, symbol, test_strategy, timeframe=timeframe
                    )
                except Exception as e:
                    logger.debug(f"[{symbol}] Val-shortlist val {strategy_name} failed: {e}")
                    continue

            val_trades = int(val_metrics.get("total_trades", 0))
            val_dd = float(val_metrics.get("max_drawdown_pct", 100.0))
            if val_trades < self.config.fx_val_min_trades:
                continue
            if val_dd >= self.config.fx_val_max_drawdown:
                continue

            final_score, _, _, _ = self.scorer.fx_generalization_score(
                train_metrics, val_metrics, purpose="opt"
            )

            if final_score > best_final:
                best_final = final_score
                best_params = dict(test_params)
                best_train_metrics = train_metrics
                best_val_metrics = val_metrics

        if not best_train_metrics:
            best_params = dict(shortlist[0]["params"])
            best_strategy = StrategyRegistry.get(strategy_name, **best_params)
            signals = best_strategy.generate_signals(train_features, symbol)
            best_train_metrics = self.backtester.run(train_features, signals, symbol, best_strategy)
            if val_features is not None and len(val_features) >= 50:
                signals = best_strategy.generate_signals(val_features, symbol)
                best_val_metrics = self.backtester.run(val_features, signals, symbol, best_strategy)
            else:
                best_val_metrics = {'total_return_pct': 0.0, 'sharpe_ratio': 0.0,
                                    'max_drawdown_pct': 0.0, 'total_trades': 0}

        return self._clean_params(best_params), best_train_metrics, best_val_metrics
    
    def _clean_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Clean parameters for JSON serialization."""
        clean = {}
        for k, v in params.items():
            if hasattr(v, "item"):
                v = v.item()
            elif isinstance(v, (np.integer, np.floating)):
                v = float(v) if isinstance(v, np.floating) else int(v)
            elif isinstance(v, np.ndarray):
                v = v.tolist()
            elif isinstance(v, (datetime, pd.Timestamp)):
                v = v.isoformat() if hasattr(v, 'isoformat') else str(v)
            clean[k] = v
        return clean


class RetrainPeriodSelector:
    """
    Stage 3: Retrain Period Selection
    
    Determines optimal retraining frequency by testing
    different lookback windows.
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.backtester = Backtester(config)
        self.scorer = StrategyScorer(config)
    
    def select_period(self,
                      symbol: str,
                      strategy_name: str,
                      params: Dict[str, Any],
                      full_data: pd.DataFrame,
                      timeframe: str) -> int:
        """
        Select optimal retrain period.
        
        Tests different lookback windows and selects the one
        that gives most consistent out-of-sample performance.
        
        OPTIMIZED: Computes features once on full data, then slices for each window.
        This provides 3-5x speedup over recomputing features for each window.
        
        Args:
            symbol: Symbol name
            strategy_name: Strategy name
            params: Strategy parameters
            full_data: Full dataset
            timeframe: Timeframe for bar count conversion
            
        Returns:
            Optimal retrain period in days
        """
        # Convert retrain periods to approximate bar counts
        bars_per_day = {
            'M5': 288,
            'M15': 96,
            'M30': 48,
            'H1': 24,
            'H4': 6,
            'D1': 1
        }
        
        bpd = bars_per_day.get(timeframe, 24)
        
        # OPTIMIZATION: Compute features once on full data, then slice
        # This is much faster than recomputing for each window
        full_features = FeatureComputer.compute_all(full_data, symbol=symbol, timeframe=timeframe)
        
        results = {}
        
        for retrain_days in self.config.retrain_periods:
            lookback_bars = retrain_days * bpd
            
            if lookback_bars > len(full_data) * 0.8:
                continue
            
            # Simulate walk-forward validation
            # Use last 30% of data for testing
            test_start = int(len(full_data) * 0.7)
            step_size = max(1, lookback_bars // 4)  # Overlap windows
            
            scores = []
            
            for start in range(test_start, len(full_data) - lookback_bars, step_size):
                train_end = start
                train_start = max(0, train_end - lookback_bars)
                test_end = min(len(full_data), start + step_size)
                
                if train_end - train_start < 50 or test_end - start < 10:
                    continue
                
                try:
                    # OPTIMIZATION: Slice from precomputed features instead of recomputing
                    train_features = full_features.iloc[train_start:train_end].copy()
                    test_features = full_features.iloc[start:test_end].copy()
                    
                    strategy = StrategyRegistry.get(strategy_name, **params)
                    
                    signals = strategy.generate_signals(test_features, symbol)
                    metrics = self.backtester.run(test_features, signals, symbol, strategy)
                    
                    score = self.scorer.score(metrics, purpose="selection")
                    # Keep window scoring consistent with the selection metric for the active mode
                    if metrics.get('total_trades', 0) >= self.config.min_trades and score is not None:
                        scores.append(score)
                
                except Exception as e:
                    logger.debug(f"[{symbol}] Walk-forward window {strategy_name} failed: {e}")
                    continue

            if scores:
                avg_score = np.mean(scores)
                # Need at least 2 scores for meaningful std calculation
                std_score = np.std(scores, ddof=1) if len(scores) > 1 else 0.0
                consistency = avg_score / (std_score + 1) if std_score > 0 else avg_score
                results[retrain_days] = {
                    'avg_score': avg_score,
                    'std_score': std_score,
                    'consistency': consistency,
                    'windows': len(scores)
                }
        
        # Select period with best consistency
        if not results:
            best_period = 30  # Default
        else:
            best_period = max(results.keys(), key=lambda k: results[k]['consistency'])
        
        logger.info(f"[{symbol}] Selected retrain period: {best_period} days")
        
        return best_period


class Validator:
    """
    Stage 4: Validation
    
    Performs final validation checks:
    - Minimum criteria check on validation data
    - Robustness ratio calculation
    - Final approval for live trading
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.scorer = StrategyScorer(config)
    
    def validate(self,
                 train_metrics: Dict[str, Any],
                 val_metrics: Dict[str, Any]) -> Tuple[bool, str, float]:
        """
        Validate strategy configuration with FX backtester-aligned rules.

        Args:
            train_metrics: Training period metrics
            val_metrics: Validation period metrics

        Returns:
            Tuple of (is_valid, reason, robustness_ratio)
        """
        robustness = self.scorer.calculate_fx_score_robustness_ratio(train_metrics, val_metrics, purpose="selection")

        val_sharpe = float(val_metrics.get('sharpe_ratio', 0.0))
        val_dd = float(val_metrics.get('max_drawdown_pct', 100.0))
        val_trades = int(val_metrics.get('total_trades', 0))

        is_valid = (
            (robustness >= getattr(self.config, "fx_min_robustness_ratio", 0.85) or val_sharpe > self.config.fx_val_sharpe_override)
            and val_dd < self.config.fx_val_max_drawdown
            and val_trades >= self.config.fx_val_min_trades
        )

        if is_valid:
            return True, "Validated successfully", robustness

        if val_trades < self.config.fx_val_min_trades:
            return False, f"Validation failed: Insufficient trades: {val_trades} < {self.config.fx_val_min_trades}", robustness
        if val_dd >= self.config.fx_val_max_drawdown:
            return False, f"Validation failed: High drawdown: {val_dd:.2f}% >= {self.config.fx_val_max_drawdown}%", robustness
        return False, f"Validation failed: Robustness {robustness:.2f} < {getattr(self.config, 'fx_min_robustness_ratio', 0.85)} and Sharpe {val_sharpe:.2f} <= {self.config.fx_val_sharpe_override}", robustness


# =============================================================================
# REGIME-AWARE OPTIMIZATION
# =============================================================================

class RegimeOptimizer:
    """
    Regime-aware strategy selection and optimization.
    
    Finds the best (strategy, params) for each (timeframe, regime) combination.
    All strategies compete for all regimes - winners are selected per (tf, regime).
    
    Flow:
    1. Equal-budget train-only B0 family probe per exact regime cell
    2. Independent train-only Optuna/grid study for shortlisted family-regime cells
    3. Final selection: pick best tuned config per regime
    4. Validation: apply fx_backtester validation rules to each winner
    
    Trade bucketing uses REGIME_LIVE at the signal bar for backtest/live parity.
    """
    
    # Regime types
    REGIMES = ['TREND', 'RANGE', 'BREAKOUT', 'CHOP']
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.backtester = Backtester(config)
        self.scorer = StrategyScorer(config)
        
        # Minimum trades per regime for validation
        self.min_train_trades = getattr(config, 'regime_min_train_trades', 25)
        self.min_val_trades = getattr(config, 'regime_min_val_trades', 10)
        
        # Hyperparameter tuning settings
        self.enable_hyperparam_tuning = getattr(config, 'regime_enable_hyperparam_tuning', True)
        self.hyperparam_top_k = getattr(config, 'regime_hyperparam_top_k', 3)  # Top K strategies to tune per regime
        self.hyperparam_max_combos = getattr(config, 'regime_hyperparam_max_combos', 30)  # Max param combos per strategy
        self.family_probe_trials = max(
            1, int(getattr(config, 'regime_family_probe_trials', 5) or 5)
        )
        self._last_tuning_search_meta: Dict[str, Any] = {}
        self._last_family_probe_cells: List[Dict[str, Any]] = []
        self._last_search_cell_outcomes: List[Dict[str, Any]] = []
        
        # Validation thresholds (from fx_backtester config)
        self.val_max_drawdown = getattr(config, 'fx_val_max_drawdown', 20.0)
        self.val_min_sharpe_override = getattr(config, 'fx_val_sharpe_override', 0.3)
        self.min_robustness_ratio = getattr(config, 'fx_min_robustness_ratio', 0.85)
        
        # Max candidates to attempt validation on (descent through scored list)
        self.regime_validation_top_k = getattr(config, 'regime_validation_top_k', 5)

        # Validation profitability gates for stored regime winners.
        self.min_val_profit_factor = getattr(config, 'regime_min_val_profit_factor', 1.0)
        self.min_val_return_pct = getattr(config, 'regime_min_val_return_pct', 5.0)
        self.allow_losing_winners = getattr(config, 'regime_allow_losing_winners', False)
        self.no_winner_marker = getattr(config, 'regime_no_winner_marker', 'NO_TRADE')
        self.screening_atr_anchor_enabled = bool(
            getattr(config, 'screening_atr_anchor_enabled', False)
        )
        self.screening_atr_anchor_multipliers = [
            float(value)
            for value in (getattr(config, 'screening_atr_anchor_multipliers', [2.0]) or [2.0])
        ]

        # Return-to-DD efficiency gate (unconditional hard gate)
        _raw_ratio = float(getattr(config, 'regime_min_val_return_dd_ratio', 1.0))
        self.min_val_return_dd_ratio = _raw_ratio if math.isfinite(_raw_ratio) and _raw_ratio > 0 else 1.0
        
        self.training_gate_telemetry: Dict[str, Any] = {}
        self.oos_candidate_evidence_records: List[Dict[str, Any]] = []
        self._trade_power_multiplicity_cache: Dict[
            Tuple[str, str, str, str, int], Dict[str, Any]
        ] = {}
        
        # Initialize Optuna optimizer for regime tuning
        if OPTUNA_AVAILABLE and self.enable_hyperparam_tuning:
            self.optuna_optimizer = create_optimizer(
                config, self.backtester, self.scorer, StrategyRegistry
            )
            # Update n_trials for regime optimization
            if self.optuna_optimizer and hasattr(self.optuna_optimizer, 'config'):
                self.optuna_optimizer.config.n_trials = self.hyperparam_max_combos
            logger.debug(f"RegimeOptimizer using: {get_optimization_method()}")
        else:
            self.optuna_optimizer = None
            if self.enable_hyperparam_tuning:
                # Tuning is enabled but Optuna is missing: this silently degrades
                # search quality to grid search. Surface it loudly, not at debug.
                logger.warning(
                    "Hyperparameter tuning is enabled but Optuna is unavailable; "
                    "falling back to grid search (reduced search quality). "
                    "Install optuna to restore TPE optimization."
                )

    def _min_val_trades_for_timeframe(self, timeframe: Optional[str] = None) -> int:
        """Return the validation trade floor for a timeframe."""
        base = max(0, int(self.min_val_trades or 0))
        if not bool(getattr(self.config, "regime_timeframe_val_trade_floors_enabled", False)):
            return base
        tf = str(timeframe or "").strip().upper()
        floors = getattr(self.config, "regime_timeframe_val_trade_floors", {}) or {}
        floor = floors.get(tf)
        if floor is None:
            if tf == "D1":
                floor = 15
            elif tf in {"H1", "H4"}:
                floor = 25
            elif tf in {"M1", "M5", "M15", "M30"}:
                floor = 30
            else:
                floor = base
        try:
            return max(base, int(floor))
        except (TypeError, ValueError, OverflowError):
            return base

    def _make_no_trade_marker(
        self,
        reason: str,
        now: Optional[datetime] = None,
        *,
        search_outcome_evidence: Optional[Mapping[str, Any]] = None,
    ) -> RegimeConfig:
        """Create an explicit non-tradeable marker for a regime slot with no validated winner."""
        marker_time = now or datetime.now()
        typed_evidence = dict(search_outcome_evidence or {})
        return RegimeConfig(
            strategy_name=self.no_winner_marker or "NO_TRADE",
            parameters={},
            quality_score=0.0,
            train_metrics={"total_trades": 0},
            val_metrics={"total_trades": 0, "validation_status": "no_trade", "reason": reason},
            regime_train_trades=0,
            regime_val_trades=0,
            trained_at=marker_time,
            valid_until=_scheduled_valid_until(self.config, marker_time),
            artifact_meta={
                "no_trade": True,
                "reason": reason,
                "search_outcome_evidence": typed_evidence,
            },
            validation_evidence={
                "validation_reason": reason,
                "tradeable": False,
                "no_trade": True,
                "search_outcome_evidence": typed_evidence,
            },
            promotion_status="NO_TRADE",
            promotion_reasons=[reason],
        )

    @staticmethod
    def _summarize_search_cell_outcomes(
        cells: Sequence[Mapping[str, Any]],
        *,
        regime: str,
    ) -> Dict[str, Any]:
        """Persist a bounded typed audit summary without retaining trial payloads."""
        selected = [
            dict(cell)
            for cell in cells
            if str(cell.get("regime") or "").upper() == str(regime).upper()
        ]
        kind_counts: Dict[str, int] = {}
        reason_counts: Dict[str, int] = {}
        validation_status_counts: Dict[str, int] = {}
        validation_kind_counts: Dict[str, int] = {}
        validation_reason_counts: Dict[str, int] = {}
        canonical: List[Dict[str, Any]] = []
        for cell in selected:
            outcome = dict(cell.get("family_outcome") or {})
            kind = str(outcome.get("kind") or "MISSING")
            reason = str(outcome.get("reason_code") or "MISSING")
            kind_counts[kind] = int(kind_counts.get(kind, 0)) + 1
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
            validation_status = str(
                cell.get("trade_power_validation_status") or "NOT_EVALUATED"
            )
            validation_kind = str(
                cell.get("trade_power_validation_kind") or "MISSING"
            )
            validation_reason = str(
                cell.get("trade_power_validation_reason_code") or "NOT_EVALUATED"
            )
            validation_status_counts[validation_status] = int(
                validation_status_counts.get(validation_status, 0)
            ) + 1
            validation_kind_counts[validation_kind] = int(
                validation_kind_counts.get(validation_kind, 0)
            ) + 1
            validation_reason_counts[validation_reason] = int(
                validation_reason_counts.get(validation_reason, 0)
            ) + 1
            canonical.append({
                "phase": str(cell.get("phase") or ""),
                "strategy_family": str(cell.get("strategy_family") or ""),
                "kind": kind,
                "reason_code": reason,
                "validation_status": validation_status,
                "validation_kind": validation_kind,
                "validation_reason_code": validation_reason,
                "search_trials": int(cell.get("search_trials", 0) or 0),
                "completed_trials": int(cell.get("completed_trials", 0) or 0),
                "eligible_train_trials": int(
                    cell.get("eligible_train_trials", 0) or 0
                ),
            })
        canonical.sort(
            key=lambda item: (item["phase"], item["strategy_family"])
        )
        return {
            "schema_version": "fxpm-search-cell-summary-v2",
            "regime": str(regime).upper(),
            "cell_count": int(len(canonical)),
            "planned_trials": int(sum(item["search_trials"] for item in canonical)),
            "completed_trials": int(
                sum(item["completed_trials"] for item in canonical)
            ),
            "eligible_train_trials": int(
                sum(item["eligible_train_trials"] for item in canonical)
            ),
            "outcome_kind_counts": dict(sorted(kind_counts.items())),
            "reason_code_counts": dict(sorted(reason_counts.items())),
            "validation_status_counts": dict(
                sorted(validation_status_counts.items())
            ),
            "validation_kind_counts": dict(
                sorted(validation_kind_counts.items())
            ),
            "validation_reason_code_counts": dict(
                sorted(validation_reason_counts.items())
            ),
            "cell_outcomes_sha256": hashlib.sha256(
                json.dumps(
                    canonical,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        }
    
    @staticmethod
    def _compute_return_dd_ratio(val_return: float, val_dd: float, eps: float = 0.5) -> float:
        """Return-to-drawdown efficiency ratio with epsilon floor."""
        return val_return / max(val_dd, eps)

    def optimize_symbol(self,
                        symbol: str,
                        train_features_by_tf: Dict[str, pd.DataFrame],
                        val_features_by_tf: Dict[str, pd.DataFrame],
                        strategies: List[BaseStrategy],
                        val_warmup_by_tf: Optional[Dict[str, int]] = None,
                        train_warmup_by_tf: Optional[Dict[str, int]] = None,
                        eligibility_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None,
                        selection_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None,
                        selection_warmup_by_tf: Optional[Dict[str, int]] = None,
                        selection_surface_by_tf: Optional[Dict[str, str]] = None,
                        historical_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None,
                        allowed_regimes_by_tf: Optional[Dict[str, Sequence[str]]] = None) -> Tuple[Dict[str, Dict[str, RegimeConfig]], Optional[RegimeConfig], int, int]:
        """
        Optimize for all (timeframe, regime) combinations with validation.
        
        Args:
            symbol: Symbol name
            train_features_by_tf: Dict mapping timeframe -> optimization features (with regime columns)
            val_features_by_tf: Dict mapping timeframe -> validation/audit features (with regime columns)
            strategies: List of strategy instances
            
        Returns:
            Tuple of:
            - Dict[timeframe][regime] -> RegimeConfig (validated winners only)
            - Optional[RegimeConfig] for default fallback (best validated overall)
            - int: count of validated winners
            - int: count of unvalidated (rejected) winners
        """
        logger.info(f"[{symbol}] Regime Optimization: {len(strategies)} strategies x {len(train_features_by_tf)} timeframes x {len(self.REGIMES)} regimes")
        self.oos_candidate_evidence_records = []
        
        # Result structure: {timeframe: {regime: RegimeConfig}}
        regime_configs: Dict[str, Dict[str, RegimeConfig]] = {}
        
        # Track best overall for default fallback
        best_overall_score = -float('inf')
        best_overall_config: Optional[RegimeConfig] = None
        
        # Validation counters
        validated_winners = 0
        unvalidated_winners = 0
        
        tf_iter = tqdm(train_features_by_tf.items(), desc=f"[{symbol}] Timeframes", unit="tf", leave=False) if _TQDM else train_features_by_tf.items()
        for tf, train_features in tf_iter:
            # seq1005: receipt-unobserved regimes are skipped BEFORE search.
            active_regimes = tuple(
                regime
                for regime in self.REGIMES
                if allowed_regimes_by_tf is None
                or allowed_regimes_by_tf.get(tf) is None
                or regime in set(allowed_regimes_by_tf[tf])
            )
            val_features = val_features_by_tf.get(tf)
            eligibility_features = (
                None if eligibility_features_by_tf is None else eligibility_features_by_tf.get(tf)
            )
            train_warmup_bars = (
                0 if train_warmup_by_tf is None
                else int(train_warmup_by_tf.get(tf, 0) or 0)
            )
            val_warmup_bars = 0 if val_warmup_by_tf is None else int(val_warmup_by_tf.get(tf, 0) or 0)
            selection_features = None if selection_features_by_tf is None else selection_features_by_tf.get(tf)
            selection_warmup_bars = (
                0 if selection_warmup_by_tf is None else int(selection_warmup_by_tf.get(tf, 0) or 0)
            )
            selection_surface = (
                "validation" if selection_surface_by_tf is None else str(selection_surface_by_tf.get(tf, "validation") or "validation")
            )
            historical_features = None if historical_features_by_tf is None else historical_features_by_tf.get(tf)
            
            if (
                train_features is None
                or max(0, len(train_features) - train_warmup_bars) < 200
            ):
                logger.debug(f"[{symbol}] [{tf}] Skipping - insufficient data")
                continue
            
            # Check if regime columns exist
            if 'REGIME_LIVE' not in train_features.columns:
                logger.warning(f"[{symbol}] [{tf}] No REGIME_LIVE column - regime features not computed")
                continue
            
            regime_configs[tf] = {}
            
            # Collect all candidates for this timeframe
            tf_candidates = self._collect_candidates(
                symbol,
                tf,
                train_features,
                val_features,
                strategies,
                train_warmup_bars=train_warmup_bars,
                val_warmup_bars=val_warmup_bars,
                eligibility_features=eligibility_features,
                selection_features=selection_features,
                selection_warmup_bars=selection_warmup_bars,
                selection_surface=selection_surface,
                historical_features=historical_features,
                allowed_regimes=active_regimes,
            )

            if not tf_candidates:
                logger.debug(f"[{symbol}] [{tf}] No valid candidates")
                now = datetime.now()
                for regime in active_regimes:
                    search_evidence = self._summarize_search_cell_outcomes(
                        getattr(self, "_last_search_cell_outcomes", []) or [],
                        regime=regime,
                    )
                    kinds = set(search_evidence.get("outcome_kind_counts", {}))
                    if kinds and kinds.issubset({
                        SearchOutcomeKind.EXPECTED_INELIGIBLE.value,
                        SearchOutcomeKind.INVALID_PARAM.value,
                    }):
                        reason = (
                            "All planned family-cell trials were expected-ineligible "
                            "or invalid under the typed train evidence policy"
                        )
                    else:
                        reason = (
                            "No strategies passed training eligibility or produced trades"
                        )
                    regime_configs[tf][regime] = self._make_no_trade_marker(
                        reason,
                        now=now,
                        search_outcome_evidence=search_evidence,
                    )
                    unvalidated_winners += 1
                continue
            
            # Select best per regime (with validation)
            for regime in active_regimes:
                best_config, is_valid, val_reason = self._select_best_for_regime(
                    symbol, tf, regime, tf_candidates
                )

                if best_config is not None:
                    # Only include validated winners (or all if validation disabled)
                    if is_valid:
                        regime_configs[tf][regime] = best_config
                        validated_winners += 1

                        # Track best overall (only from validated)
                        if best_config.quality_score > best_overall_score:
                            best_overall_score = best_config.quality_score
                            best_overall_config = best_config

                        # Check if this was a tuned candidate
                        is_tuned = any(
                            c.get('is_tuned', False) and
                            c['strategy_name'] == best_config.strategy_name and
                            c['params'] == best_config.parameters
                            for c in tf_candidates
                        )
                        tuned_tag = " [TUNED]" if is_tuned else ""

                        logger.info(f"[{symbol}] [{tf}] [{regime}] Winner: {best_config.strategy_name}{tuned_tag} "
                                   f"(quality={best_config.quality_score:.3f}, "
                                   f"train={best_config.regime_train_trades}, "
                                   f"val={best_config.regime_val_trades}) "
                                   f"[VALIDATED: {val_reason}]")
                    else:
                        unvalidated_winners += 1
                        search_evidence = self._summarize_search_cell_outcomes(
                            getattr(self, "_last_search_cell_outcomes", []) or [],
                            regime=regime,
                        )
                        regime_configs[tf][regime] = self._make_no_trade_marker(
                            val_reason,
                            search_outcome_evidence=search_evidence,
                        )
                        logger.debug(f"[{symbol}] [{tf}] [{regime}] Candidate {best_config.strategy_name} "
                                    f"FAILED validation: {val_reason}")
                else:
                    # NO WINNER - No fallback to best train
                    unvalidated_winners += 1
                    search_evidence = self._summarize_search_cell_outcomes(
                        getattr(self, "_last_search_cell_outcomes", []) or [],
                        regime=regime,
                    )
                    regime_configs[tf][regime] = self._make_no_trade_marker(
                        val_reason,
                        search_outcome_evidence=search_evidence,
                    )
                    logger.warning(
                        f"[{symbol}] [{tf}] [{regime}] No validated winner - {val_reason}. "
                        f"NO TRADE for this regime (no fallback to best train)."
                    )
        
        # --- Reason-coded summary telemetry ---
        total_slots = sum(len(self.REGIMES) for _ in train_features_by_tf)
        logger.info(
            f"[{symbol}] Regime optimization summary: "
            f"{validated_winners}/{total_slots} validated, "
            f"{unvalidated_winners} unvalidated/no-winner, "
            f"{total_slots - validated_winners - unvalidated_winners} skipped (data)"
        )

        return regime_configs, best_overall_config, validated_winners, unvalidated_winners

    def consume_training_gate_telemetry(self) -> Dict[str, Any]:
        """Return and clear compact pre-tuning gate telemetry for the latest symbol run."""
        telemetry = dict(self.training_gate_telemetry or {})
        self.training_gate_telemetry = {}
        return telemetry

    def consume_oos_candidate_evidence(self) -> List[Dict[str, Any]]:
        """Return and clear compact OOS return evidence for the latest symbol run."""
        records = list(self.oos_candidate_evidence_records or [])
        self.oos_candidate_evidence_records = []
        return records

    @staticmethod
    def _clean_candidate_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        clean: Dict[str, Any] = {}
        for key, value in dict(params or {}).items():
            if hasattr(value, "item"):
                value = value.item()
            if isinstance(value, np.integer):
                value = int(value)
            elif isinstance(value, np.floating):
                value = float(value)
            clean[str(key)] = value
        return clean

    @staticmethod
    def _candidate_evaluation_regime(candidate: Dict[str, Any]) -> str:
        """Resolve the one regime cell represented by a frozen candidate."""
        for field_name in ("evaluation_regime", "tuned_for_regime"):
            value = str(candidate.get(field_name, "") or "").strip().upper()
            if value:
                return value
        metric_regimes = {
            str(value).strip().upper()
            for surface_name in ("train_regime_metrics", "val_regime_metrics")
            for value in (candidate.get(surface_name) or {}).keys()
            if str(value).strip()
        }
        return next(iter(metric_regimes)) if len(metric_regimes) == 1 else ""

    @staticmethod
    def _exact_cell_candidate_id(
        symbol: str,
        timeframe: str,
        regime: str,
        candidate: Mapping[str, Any],
        *,
        horizon_process_sha256: Optional[str] = None,
    ) -> str:
        """Return export-independent identity for one frozen exact-cell model."""
        horizon_sha256 = (
            horizon_process_sha256
            if horizon_process_sha256 is not None
            else candidate.get("horizon_process_sha256")
        )
        return _stable_hash({
            "symbol": str(symbol or "").upper(),
            "timeframe": str(timeframe or "").upper(),
            "regime": str(regime or "").upper(),
            "strategy_name": str(candidate.get("strategy_name", "Unknown")),
            "parameters": RegimeOptimizer._clean_candidate_params(
                candidate.get("params", {})
            ),
            "horizon_process_sha256": horizon_sha256,
        })

    @staticmethod
    def _sha256_attestation(value: Any) -> bool:
        text = str(value or "").strip().lower()
        return len(text) == 64 and all(
            character in "0123456789abcdef" for character in text
        )

    @classmethod
    def _admissible_swap_statistical_source(
        cls,
        swap_contract: Mapping[str, Any],
    ) -> bool:
        """Accept only immutable, hash-bound financing evidence."""
        contract = dict(swap_contract or {})
        status = str(contract.get("status") or "").upper()
        provenance = str(contract.get("provenance_status") or "").upper()
        if status == "DISABLED":
            return (
                provenance == "AUTHENTICATED"
                and bool(contract.get("source_provenance_authenticated", False))
                and bool(
                    contract.get("broker_swap_disabled_authenticated", False)
                )
                and cls._sha256_attestation(
                    contract.get("broker_disabled_proof_sha256")
                )
            )
        if status != "AVAILABLE":
            return False
        required_bindings = (
            "price_activation_manifest_sha256",
            "terms_activation_manifest_sha256",
            "terms_snapshot_sha256",
            "schedule_sha256",
            "target_bars_sha256",
            "target_sidecar_sha256",
            "projection_sha256",
        )
        if not all(
            cls._sha256_attestation(contract.get(name))
            for name in required_bindings
        ):
            return False
        if provenance == "AUTHENTICATED":
            return (
                bool(contract.get("source_provenance_authenticated", False))
                and bool(
                    contract.get("historical_rates_authenticated", False)
                )
                and cls._sha256_attestation(
                    contract.get("historical_rates_sha256")
                )
            )
        if provenance == "STRESS_APPROVED":
            return (
                bool(
                    contract.get(
                        "stress_policy_explicitly_approved", False
                    )
                )
                and bool(contract.get("stress_policy_immutable", False))
                and cls._sha256_attestation(
                    contract.get("stress_policy_sha256")
                )
            )
        return False

    @staticmethod
    def _admissible_validation_daily_net_mtm(
        metrics: Mapping[str, Any],
    ) -> Tuple[List[float], List[str]]:
        """Read only the versioned tuning-validation daily MTM surface."""
        contract = dict(metrics.get("daily_net_mtm_contract") or {})
        swap_contract = dict(contract.get("swap_accrual_contract") or {})
        swap_status = str(swap_contract.get("status") or "").upper()
        swap_source_admissible = (
            RegimeOptimizer._admissible_swap_statistical_source(
                swap_contract
            )
        )
        if (
            str(contract.get("contract_version") or "")
            != DAILY_NET_MTM_CONTRACT_VERSION
            or str(contract.get("status") or "").upper() != "AVAILABLE"
            or str(contract.get("partition") or "") != "tuning_validation"
            or str(contract.get("origin") or "")
            != "backtester_final_post_cost_equity_curve"
            or bool(contract.get("uses_final_lockbox", False))
            or dict(contract.get("execution_cost_timing_contract") or {})
            != EXECUTION_COST_TIMING_CONTRACT
            or swap_status not in {"AVAILABLE", "DISABLED"}
            or not swap_source_admissible
            or not bool(swap_contract.get("execution_state_compounding", False))
            or (
                swap_status == "AVAILABLE"
                and str(swap_contract.get("timestamp_source") or "")
                != "_BAR_CLOSE_TS"
            )
        ):
            return [], []
        raw_returns = list(metrics.get("daily_net_mtm_returns", []) or [])
        dates = [str(value) for value in (metrics.get("daily_net_mtm_dates", []) or [])]
        if (
            not raw_returns
            or len(raw_returns) != len(dates)
            or len(dates) != len(set(dates))
            or int(contract.get("observations", -1) or -1) != len(raw_returns)
        ):
            return [], []
        try:
            returns = np.asarray(raw_returns, dtype=np.float64)
        except Exception:
            return [], []
        if not np.isfinite(returns).all() or bool((returns <= -1.0).any()):
            return [], []
        return [float(value) for value in returns.tolist()], dates

    @staticmethod
    def _trade_power_source_contract(result: Mapping[str, Any]) -> Dict[str, Any]:
        """Bind strict net-R evidence to the exact Backtester source contract."""
        payload = dict(result or {})
        swap = dict(payload.get("swap_accrual_contract") or {})
        source_binding = {
            "price_activation_manifest_sha256": swap.get(
                "price_activation_manifest_sha256"
            ),
            "terms_activation_manifest_sha256": swap.get(
                "terms_activation_manifest_sha256"
            ),
            "terms_snapshot_sha256": swap.get("terms_snapshot_sha256"),
            "schedule_sha256": swap.get("schedule_sha256"),
            "target_bars_sha256": swap.get("target_bars_sha256"),
            "target_sidecar_sha256": swap.get("target_sidecar_sha256"),
        }
        for name in (
            "historical_rates_sha256",
            "broker_disabled_proof_sha256",
        ):
            if swap.get(name) not in (None, ""):
                source_binding[name] = swap.get(name)
        # The broker-terms contract uses the more explicit authenticated name;
        # the evidence validator consumes this normalized semantic flag.
        if "broker_swap_disabled" not in swap:
            swap["broker_swap_disabled"] = bool(
                swap.get("broker_swap_disabled_authenticated", False)
            )
        return {
            "schema_version": TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION,
            "source_binding": source_binding,
            "trade_record_schema_version": REQUIRED_TRADE_RECORD_SCHEMA,
            "execution_cost_timing_contract": dict(
                payload.get("execution_cost_timing_contract") or {}
            ),
            "numerical_execution_cost_attestation": dict(
                payload.get("numerical_execution_cost_attestation") or {}
            ),
            "swap_accrual_contract": swap,
        }

    @staticmethod
    def _trade_power_sha256(value: Any) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
        ).hexdigest()

    @staticmethod
    def _valid_trade_power_interval(start: Any, end: Any) -> bool:
        try:
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
        except (TypeError, ValueError, OverflowError):
            return False
        return bool(
            not pd.isna(start_ts)
            and not pd.isna(end_ts)
            and start_ts.tzinfo is not None
            and end_ts.tzinfo is not None
            and start_ts < end_ts
        )

    @staticmethod
    def _sample_events_within_contract(
        sample: Mapping[str, Any],
        contract: Mapping[str, Any],
    ) -> bool:
        try:
            event_start = pd.Timestamp(sample.get("event_start_utc"))
            event_end = pd.Timestamp(sample.get("event_end_utc"))
            contract_start = pd.Timestamp(contract.get("start_utc"))
            contract_end = pd.Timestamp(contract.get("end_utc"))
        except (TypeError, ValueError, OverflowError):
            return False
        return bool(
            not any(
                pd.isna(value)
                for value in (event_start, event_end, contract_start, contract_end)
            )
            and event_start.tzinfo is not None
            and event_end.tzinfo is not None
            and contract_start.tzinfo is not None
            and contract_end.tzinfo is not None
            and contract_start <= event_start <= event_end < contract_end
        )

    def _search_multiplicity_receipt(
        self,
        *,
        effective_trials: int,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_family: str,
    ) -> Dict[str, Any]:
        count = max(1, int(effective_trials))
        key = (
            str(symbol).upper(),
            str(timeframe).upper(),
            str(regime).upper(),
            str(strategy_family),
            count,
        )
        cached = self._trade_power_multiplicity_cache.get(key)
        if cached is not None:
            return dict(cached)
        attempts = [
            {
                "attempt_id": self._trade_power_sha256(
                    {
                        "schema_version": "fxpm.search-attempt-identity.v1",
                        "symbol": key[0],
                        "timeframe": key[1],
                        "regime": key[2],
                        "strategy_family": key[3],
                        "ordinal": ordinal,
                    }
                )
            }
            for ordinal in range(count)
        ]
        ledger = {
            "contract_version": "fxpm.winner-attempt-ledger.v2",
            "attempt_count": count,
            "attempts_sha256": self._trade_power_sha256(attempts),
            "attempts": attempts,
        }
        receipt = build_trade_power_multiplicity_receipt(ledger)
        if len(self._trade_power_multiplicity_cache) >= 4096:
            self._trade_power_multiplicity_cache.pop(
                next(iter(self._trade_power_multiplicity_cache))
            )
        self._trade_power_multiplicity_cache[key] = dict(receipt)
        return dict(receipt)

    def _search_partition_contract(
        self,
        result: Mapping[str, Any],
        *,
        partition: str,
        multiplicity_receipt: Mapping[str, Any],
        prior_partition_contract_sha256: str = "",
    ) -> Dict[str, Any]:
        axis = result.get("trade_power_partition_axis")
        if isinstance(axis, Mapping):
            start_utc = axis.get("start_utc")
            end_utc = axis.get("end_utc")
            horizon_sha = axis.get("horizon_receipt_sha256")
            event_axis_sha = axis.get("event_axis_sha256")
        else:
            rows = result.get("position_events")
            if rows is None:
                rows = result.get("trades")
            if not isinstance(rows, list) or not rows:
                raise ValueError("TRADE_POWER_PARTITION_AXIS_MISSING")
            starts = [pd.Timestamp(item.get("entry_time")) for item in rows]
            ends = [pd.Timestamp(item.get("exit_time")) for item in rows]
            if any(pd.isna(value) for value in [*starts, *ends]):
                raise ValueError("TRADE_POWER_PARTITION_AXIS_INVALID")
            start = min(starts)
            end = max(ends) + pd.Timedelta(microseconds=1)
            start = (
                start.tz_localize("UTC")
                if start.tzinfo is None
                else start.tz_convert("UTC")
            )
            end = (
                end.tz_localize("UTC")
                if end.tzinfo is None
                else end.tz_convert("UTC")
            )
            start_utc = start.isoformat()
            end_utc = end.isoformat()
            event_axis_sha = self._trade_power_sha256(
                [
                    {
                        "entry_time": str(item.get("entry_time")),
                        "exit_time": str(item.get("exit_time")),
                        "position_id": str(item.get("position_id") or ""),
                    }
                    for item in rows
                ]
            )
            horizon_sha = self._trade_power_sha256(
                {
                    "schema_version": "fxpm.search-partition-fallback.v1",
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "event_axis_sha256": event_axis_sha,
                }
            )
        return build_trade_power_partition_contract(
            partition=partition,
            start_utc=start_utc,
            end_utc=end_utc,
            horizon_receipt_sha256=str(horizon_sha or ""),
            event_axis_sha256=str(event_axis_sha or ""),
            multiplicity_receipt=multiplicity_receipt,
            prior_partition_contract_sha256=prior_partition_contract_sha256,
        )

    @staticmethod
    def _power_trial_outcome(
        decision: TradePowerDecision,
        *,
        stage: SearchStage,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_family: str,
        parameter_hash: str,
    ) -> TrialOutcome:
        reason_codes = tuple(decision.reason_codes or ())
        return TrialOutcome(
            kind=decision.outcome_kind,
            stage=stage,
            reason_code=(
                str(reason_codes[0]) if reason_codes else str(decision.status)
            ),
            reason=str(decision.reason),
            symbol=str(symbol),
            timeframe=str(timeframe),
            regime=str(regime),
            strategy_family=str(strategy_family),
            parameter_hash=str(parameter_hash),
            evidence={
                "partition": decision.partition,
                "status": decision.status,
                "observed_effective_trades": decision.observed_effective_trades,
                "required_effective_trades": decision.required_effective_trades,
                "reason_codes": list(reason_codes),
            },
        )

    def _calibrate_train_power_result(
        self,
        result: Mapping[str, Any],
        *,
        effective_trials: int,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_family: str,
        parameter_hash: str,
    ) -> TradePowerDecision:
        """Calibrate and freeze one train-only effective-trade requirement."""
        position_events = result.get("position_events")
        if position_events is None:
            position_events = result.get("trades")
        multiplicity_receipt = self._search_multiplicity_receipt(
            effective_trials=effective_trials,
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_family=strategy_family,
        )
        partition_contract = self._search_partition_contract(
            result,
            partition="optimizer_train",
            multiplicity_receipt=multiplicity_receipt,
        )
        decision = calibrate_train_trade_power(
            position_events,
            policy=PRODUCTION_TRADE_POWER_POLICY,
            multiplicity_receipt=multiplicity_receipt,
            partition_contract=partition_contract,
            source_contract=self._trade_power_source_contract(result),
            minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
        )
        if decision.outcome_kind is SearchOutcomeKind.INFRA_FAILURE:
            raise SearchInfrastructureError(self._power_trial_outcome(
                decision,
                stage=SearchStage.TRAIN_EVIDENCE,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_family,
                parameter_hash=parameter_hash,
            ))
        return decision

    def _evaluate_partition_power_result(
        self,
        result: Mapping[str, Any],
        *,
        frozen_requirement: TradePowerRequirement,
        partition: str = "tuning_validation",
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_family: str,
        parameter_hash: str,
        prior_partition_decision: Optional[Mapping[str, Any]] = None,
    ) -> TradePowerDecision:
        """Apply one immutable train requirement to an out-of-train partition."""
        position_events = result.get("position_events")
        if position_events is None:
            position_events = result.get("trades")
        multiplicity_receipt = self._search_multiplicity_receipt(
            effective_trials=frozen_requirement.attempted_trials_exact,
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_family=strategy_family,
        )
        if (
            multiplicity_receipt.get("attempt_ledger_payload_sha256")
            != frozen_requirement.attempt_ledger_payload_sha256
            or multiplicity_receipt.get("attempts_sha256")
            != frozen_requirement.attempts_sha256
            or multiplicity_receipt.get("attempted_trials_exact")
            != frozen_requirement.attempted_trials_exact
            or multiplicity_receipt.get("receipt_sha256")
            != frozen_requirement.multiplicity_receipt_sha256
        ):
            raise ValueError("TRADE_POWER_MULTIPLICITY_LINEAGE_DRIFT")
        train_contract = build_trade_power_partition_contract(
            partition="optimizer_train",
            start_utc=frozen_requirement.calibration_start_utc,
            end_utc=frozen_requirement.calibration_end_utc,
            horizon_receipt_sha256=(
                frozen_requirement.calibration_horizon_receipt_sha256
            ),
            event_axis_sha256=frozen_requirement.calibration_event_axis_sha256,
            multiplicity_receipt=multiplicity_receipt,
        )
        if (
            train_contract.get("contract_sha256")
            != frozen_requirement.calibration_partition_contract_sha256
        ):
            raise ValueError("TRADE_POWER_TRAIN_PARTITION_LINEAGE_DRIFT")
        prior_contract: Mapping[str, Any] = train_contract
        if partition == "final_lockbox":
            if not isinstance(prior_partition_decision, Mapping):
                raise ValueError("TUNING_PARTITION_DECISION_MISSING")
            prior_state, prior_reason = self._trade_power_decision_state(
                prior_partition_decision,
                partition="tuning_validation",
            )
            if prior_state != "PASS":
                raise ValueError(
                    prior_reason or "TUNING_PARTITION_DECISION_NOT_PASSING"
                )
            prior_payload = prior_partition_decision.get("partition_contract")
            if not isinstance(prior_payload, Mapping):
                raise ValueError("TUNING_PARTITION_CONTRACT_MISSING")
            prior_contract = dict(prior_payload)
        current_contract = self._search_partition_contract(
            result,
            partition=partition,
            multiplicity_receipt=multiplicity_receipt,
            prior_partition_contract_sha256=str(
                prior_contract.get("contract_sha256") or ""
            ),
        )
        decision = evaluate_trade_power_partition(
            position_events,
            frozen_requirement=frozen_requirement,
            partition=partition,
            partition_contract=current_contract,
            prior_partition_contract=prior_contract,
            source_contract=self._trade_power_source_contract(result),
            minimum_observations=PRODUCTION_TRADE_POWER_MINIMUM_OBSERVATIONS,
        )
        if decision.outcome_kind is SearchOutcomeKind.INFRA_FAILURE:
            raise SearchInfrastructureError(self._power_trial_outcome(
                decision,
                stage=SearchStage.FROZEN_VALIDATION,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_family,
                parameter_hash=parameter_hash,
            ))
        return decision

    def _train_trial_economic_ineligibility(
        self,
        metrics: Mapping[str, Any],
    ) -> Optional[str]:
        """Return the predeclared train-only economic rejection, if any."""
        surface_failure = self._selection_surface_failure(
            dict(metrics or {}),
            label="train",
        )
        if surface_failure:
            raise ValueError(f"TRAIN_SELECTION_SURFACE_INVALID: {surface_failure}")
        try:
            profit_factor = float(metrics.get("profit_factor"))
            total_return = float(metrics.get("total_return_pct"))
            drawdown = float(metrics.get("max_drawdown_pct"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("TRAIN_ECONOMIC_METRICS_INVALID") from exc
        if (
            math.isnan(profit_factor)
            or profit_factor < 0.0
            or not math.isfinite(total_return)
            or not math.isfinite(drawdown)
            or drawdown < 0.0
        ):
            raise ValueError("TRAIN_ECONOMIC_METRICS_INVALID")
        if profit_factor < float(self.config.train_min_profit_factor):
            return "TRAIN_PROFIT_FACTOR_GATE"
        if total_return < float(self.config.train_min_return_pct):
            return "TRAIN_RETURN_GATE"
        drawdown_ceiling = min(
            float(self.config.train_max_drawdown),
            float(self.val_max_drawdown) * 1.25,
        )
        if (
            drawdown > drawdown_ceiling
        ):
            return "TRAIN_DRAWDOWN_GATE"
        return None

    @classmethod
    def _trade_power_decision_state(
        cls,
        value: Any,
        *,
        partition: str,
    ) -> Tuple[str, str]:
        """Validate a persisted power decision as PASS, INELIGIBLE, or INFRA."""
        if not isinstance(value, Mapping):
            return "INFRA", "TRADE_POWER_DECISION_MISSING"
        expected_keys = {
            "schema_version",
            "status",
            "outcome_kind",
            "partition",
            "passed",
            "reason",
            "reason_codes",
            "observed_effective_trades",
            "required_effective_trades",
            "requirement_recalibrated",
            "requirement_sigma_source",
            "sample_evidence",
            "frozen_requirement",
            "partition_contract",
        }
        if (
            set(value) != expected_keys
            or str(value.get("schema_version") or "")
            != TRADE_POWER_DECISION_SCHEMA_VERSION
            or str(value.get("partition") or "") != partition
            or value.get("requirement_recalibrated") is not False
            or str(value.get("requirement_sigma_source") or "")
            != "optimizer_train_frozen"
            or not isinstance(value.get("reason"), str)
            or not isinstance(value.get("reason_codes"), list)
        ):
            return "INFRA", "TRADE_POWER_DECISION_CONTRACT_INVALID"
        status = str(value.get("status") or "")
        kind = str(value.get("outcome_kind") or "")
        passed = value.get("passed") is True
        if status == "INFRA_FAILURE" or kind == SearchOutcomeKind.INFRA_FAILURE.value:
            return "INFRA", "PERSISTED_TRADE_POWER_INFRA_FAILURE"
        pass_shape = (
            status == "PASS"
            and kind == SearchOutcomeKind.VALID.value
            and passed
        )
        ineligible_shape = (
            status == "EXPECTED_INELIGIBLE"
            and kind == SearchOutcomeKind.EXPECTED_INELIGIBLE.value
            and not passed
        )
        if not pass_shape and not ineligible_shape:
            return "INFRA", "TRADE_POWER_DECISION_CONTRACT_INVALID"

        try:
            observed = float(value.get("observed_effective_trades"))
            required_raw = value.get("required_effective_trades")
            if isinstance(required_raw, bool):
                raise ValueError("required effective trades is boolean")
            required = int(required_raw)
            if float(required) != float(required_raw):
                raise ValueError("required effective trades is not integral")
        except (TypeError, ValueError, OverflowError):
            return "INFRA", "TRADE_POWER_DECISION_CONTRACT_INVALID"
        if (
            not math.isfinite(observed)
            or observed < 0.0
            or required <= 0
            or (pass_shape and observed < float(required))
            or (ineligible_shape and observed >= float(required))
        ):
            return "INFRA", "TRADE_POWER_DECISION_CONTRACT_INVALID"

        requirement_payload = value.get("frozen_requirement")
        sample = value.get("sample_evidence")
        if not isinstance(requirement_payload, Mapping) or not isinstance(sample, Mapping):
            return "INFRA", "TRADE_POWER_DECISION_EVIDENCE_MISSING"
        try:
            requirement = cls._frozen_requirement_from_validation_evidence({
                "trade_power": {
                    "train": {"frozen_requirement": dict(requirement_payload)}
                }
            })
        except (TypeError, ValueError, OverflowError):
            return "INFRA", "TRADE_POWER_DECISION_EVIDENCE_INVALID"
        required_field = {
            "optimizer_train": requirement.required_effective_train_trades,
            "tuning_validation": requirement.required_effective_tuning_trades,
            "final_lockbox": requirement.required_effective_final_trades,
        }.get(partition)
        derived_requirement = derive_trade_power_requirement(
            sigma_r_robust=requirement.sigma_r_robust,
            effective_trials=requirement.effective_trials,
            policy=PRODUCTION_TRADE_POWER_POLICY,
        )
        policy_exact = (
            requirement.minimum_economic_edge_r
            == PRODUCTION_TRADE_POWER_POLICY.minimum_economic_edge_r
            and requirement.maximum_ci_half_width_r
            == PRODUCTION_TRADE_POWER_POLICY.maximum_ci_half_width_r
            and requirement.target_power
            == PRODUCTION_TRADE_POWER_POLICY.target_power
            and requirement.alpha_train_nominal
            == PRODUCTION_TRADE_POWER_POLICY.alpha_train
            and requirement.alpha_tuning
            == PRODUCTION_TRADE_POWER_POLICY.alpha_tuning
            and requirement.alpha_final
            == PRODUCTION_TRADE_POWER_POLICY.alpha_final
            and requirement.multiple_testing_adjustment
            == PRODUCTION_TRADE_POWER_POLICY.multiple_testing_adjustment
            and requirement.pilot_dispersion_method
            == PRODUCTION_TRADE_POWER_POLICY.pilot_dispersion
            and requirement.policy_schema_version
            == PRODUCTION_TRADE_POWER_POLICY.schema_version
        )
        derived_exact = (
            derived_requirement.passed
            and requirement.status == derived_requirement.status
            and requirement.reason == derived_requirement.reason
            and requirement.reason_codes == derived_requirement.reason_codes
            and requirement.sigma_r_robust == derived_requirement.sigma_r_robust
            and requirement.effective_trials == derived_requirement.effective_trials
            and requirement.alpha_train_adjusted
            == derived_requirement.alpha_train_adjusted
            and requirement.required_effective_train_trades
            == derived_requirement.required_effective_train_trades
            and requirement.required_effective_tuning_trades
            == derived_requirement.required_effective_tuning_trades
            and requirement.required_effective_final_trades
            == derived_requirement.required_effective_final_trades
            and requirement.multiplicity_method == "raw_attempt_ledger_count_v1"
        )
        required_hashes = (
            requirement.calibration_sample_sha256,
            requirement.calibration_source_contract_sha256,
            requirement.calibration_partition_source_contract_sha256,
            requirement.target_bars_sha256,
            requirement.target_sidecar_sha256,
            requirement.financing_schedule_sha256,
            requirement.numerical_execution_cost_policy_sha256,
            requirement.calibration_numerical_execution_cost_attestation_sha256,
            requirement.attempt_ledger_payload_sha256,
            requirement.attempts_sha256,
            requirement.multiplicity_receipt_sha256,
            requirement.calibration_partition_contract_sha256,
            requirement.calibration_horizon_receipt_sha256,
            requirement.calibration_event_axis_sha256,
            requirement.calibration_net_r_values_sha256,
            requirement.calibration_holding_intervals_sha256,
        )
        if (
            not requirement.passed
            or not requirement.frozen
            or not policy_exact
            or not derived_exact
            or not requirement_source_provenance_valid(requirement)
            or requirement.schema_version != TRADE_POWER_SCHEMA_VERSION
            or requirement.calibration_partition != "optimizer_train"
            or requirement.source_contract_schema_version
            != TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION
            or requirement.projection_contract_version
            != REQUIRED_SWAP_PROJECTION_CONTRACT
            or requirement.entry_slippage_contract_version
            != REQUIRED_ENTRY_SLIPPAGE_CONTRACT
            or requirement.swap_accrual_contract_version
            != REQUIRED_SWAP_ACCRUAL_CONTRACT
            or requirement.trade_record_schema_version
            != REQUIRED_TRADE_RECORD_SCHEMA
            or type(requirement.attempted_trials_exact) is not int
            or requirement.attempted_trials_exact <= 0
            or requirement.effective_trials
            != float(requirement.attempted_trials_exact)
            or not cls._valid_trade_power_interval(
                requirement.calibration_start_utc,
                requirement.calibration_end_utc,
            )
            or required_field != required
            or not all(cls._sha256_attestation(item) for item in required_hashes)
        ):
            return "INFRA", "TRADE_POWER_DECISION_REQUIREMENT_INVALID"

        contract = value.get("partition_contract")
        contract_keys = {
            "schema_version",
            "partition",
            "start_utc",
            "end_utc",
            "horizon_receipt_sha256",
            "event_axis_sha256",
            "attempt_ledger_payload_sha256",
            "attempts_sha256",
            "attempted_trials_exact",
            "multiplicity_receipt_sha256",
            "prior_partition_contract_sha256",
            "contract_sha256",
        }
        if not isinstance(contract, Mapping) or set(contract) != contract_keys:
            return "INFRA", "TRADE_POWER_PARTITION_CONTRACT_INVALID"
        contract_body = {
            key: contract.get(key)
            for key in contract_keys - {"contract_sha256"}
        }
        prior_hash = str(contract.get("prior_partition_contract_sha256") or "")
        try:
            contract_start = pd.Timestamp(contract.get("start_utc"))
            contract_end = pd.Timestamp(contract.get("end_utc"))
        except (TypeError, ValueError, OverflowError):
            return "INFRA", "TRADE_POWER_PARTITION_CONTRACT_INVALID"
        if (
            str(contract.get("schema_version") or "")
            != TRADE_POWER_PARTITION_VERSION
            or str(contract.get("partition") or "") != partition
            or pd.isna(contract_start)
            or pd.isna(contract_end)
            or contract_start.tzinfo is None
            or contract_end.tzinfo is None
            or contract_start >= contract_end
            or contract.get("contract_sha256")
            != cls._trade_power_sha256(contract_body)
            or not cls._sha256_attestation(
                contract.get("horizon_receipt_sha256")
            )
            or not cls._sha256_attestation(contract.get("event_axis_sha256"))
            or contract.get("attempt_ledger_payload_sha256")
            != requirement.attempt_ledger_payload_sha256
            or contract.get("attempts_sha256") != requirement.attempts_sha256
            or contract.get("attempted_trials_exact")
            != requirement.attempted_trials_exact
            or contract.get("multiplicity_receipt_sha256")
            != requirement.multiplicity_receipt_sha256
            or (
                partition == "optimizer_train"
                and (
                    prior_hash
                    or contract.get("contract_sha256")
                    != requirement.calibration_partition_contract_sha256
                    or contract.get("start_utc")
                    != requirement.calibration_start_utc
                    or contract.get("end_utc")
                    != requirement.calibration_end_utc
                    or contract.get("horizon_receipt_sha256")
                    != requirement.calibration_horizon_receipt_sha256
                    or contract.get("event_axis_sha256")
                    != requirement.calibration_event_axis_sha256
                )
            )
            or (
                partition == "tuning_validation"
                and (
                    prior_hash
                    != requirement.calibration_partition_contract_sha256
                    or contract_start
                    < pd.Timestamp(requirement.calibration_end_utc)
                    or contract.get("event_axis_sha256")
                    == requirement.calibration_event_axis_sha256
                )
            )
            or (
                partition == "final_lockbox"
                and (
                    not cls._sha256_attestation(prior_hash)
                    or prior_hash
                    == requirement.calibration_partition_contract_sha256
                    or contract_start
                    < pd.Timestamp(requirement.calibration_end_utc)
                    or contract.get("event_axis_sha256")
                    == requirement.calibration_event_axis_sha256
                )
            )
        ):
            return "INFRA", "TRADE_POWER_PARTITION_CONTRACT_INVALID"

        source = sample.get("source_contract")
        if not isinstance(source, Mapping):
            return "INFRA", "TRADE_POWER_DECISION_SAMPLE_INVALID"
        sample_outcome = str(sample.get("outcome_kind") or "")
        sample_status = str(sample.get("status") or "")
        expected_sample_keys = {
            "schema_version",
            "status",
            "outcome_kind",
            "reason",
            "reason_codes",
            "raw_trade_records",
            "usable_trades",
            "minimum_observations_required",
            "mean_net_r",
            "sigma_r_robust",
            "effective_trades",
            "net_r_min",
            "net_r_max",
            "net_r_values_sha256",
            "holding_intervals_sha256",
            "position_ids_sha256",
            "event_order_sha256",
            "event_start_utc",
            "event_end_utc",
            "sample_sha256",
            "source_contract",
            "net_r_contract",
            "dependence_evidence",
        }
        measured_sample = (
            sample_status == "MEASURED"
            and sample_outcome == SearchOutcomeKind.VALID.value
        )
        try:
            sample_effective = float(sample.get("effective_trades"))
            sample_sigma = (
                float(sample.get("sigma_r_robust"))
                if measured_sample
                else None
            )
            raw_trade_records = int(sample.get("raw_trade_records"))
            usable_trades = int(sample.get("usable_trades"))
            minimum_observations = int(
                sample.get("minimum_observations_required")
            )
            expected_sample_hash = trade_sample_identity_sha256(sample)
            dependence = sample.get("dependence_evidence")
            if measured_sample:
                if not isinstance(dependence, Mapping):
                    raise ValueError("measured sample dependence evidence is missing")
                dependence_effective = float(
                    dependence.get("effective_observations")
                )
                dependence_raw = int(dependence.get("raw_observations"))
            else:
                dependence_effective = None
                dependence_raw = None
        except (TypeError, ValueError, OverflowError):
            return "INFRA", "TRADE_POWER_DECISION_SAMPLE_INVALID"
        source_hashes = [
            sample.get("sample_sha256"),
            source.get("source_contract_sha256"),
            source.get("partition_source_contract_sha256"),
            source.get("target_bars_sha256"),
            source.get("target_sidecar_sha256"),
            source.get("financing_schedule_sha256"),
            source.get("numerical_execution_cost_policy_sha256"),
            source.get("numerical_execution_cost_attestation_sha256"),
        ]
        if measured_sample:
            source_hashes.extend([
                sample.get("net_r_values_sha256"),
                sample.get("holding_intervals_sha256"),
                sample.get("position_ids_sha256"),
                sample.get("event_order_sha256"),
            ])
        if (
            str(sample.get("schema_version") or "") != TRADE_SAMPLE_SCHEMA_VERSION
            or set(sample) != expected_sample_keys
            or not math.isfinite(sample_effective)
            or sample_effective < 0.0
            or raw_trade_records < 0
            or usable_trades < 0
            or minimum_observations < 3
            or (
                measured_sample
                and (
                    sample_sigma is None
                    or not math.isfinite(sample_sigma)
                    or sample_sigma <= 0.0
                )
            )
            or not math.isclose(sample_effective, observed, rel_tol=1e-12, abs_tol=1e-12)
            or sample.get("sample_sha256") != expected_sample_hash
            or (
                measured_sample
                and (
                    raw_trade_records != usable_trades
                    or raw_trade_records < minimum_observations
                    or dependence_raw != raw_trade_records
                    or dependence_effective is None
                    or not math.isfinite(dependence_effective)
                    or not math.isclose(
                        sample_effective,
                        dependence_effective,
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    )
                    or str((dependence or {}).get("status") or "") != "MEASURED"
                    or not cls._sample_events_within_contract(sample, contract)
                )
            )
            or (
                partition == "optimizer_train"
                and (
                    not measured_sample
                    or sample_sigma is None
                    or not math.isclose(
                        sample_sigma,
                        float(requirement.sigma_r_robust),
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    )
                    or sample.get("sample_sha256")
                    != requirement.calibration_sample_sha256
                )
            )
            or not all(cls._sha256_attestation(item) for item in source_hashes)
            or str(source.get("schema_version") or "")
            != TRADE_EVIDENCE_SOURCE_CONTRACT_VERSION
            or source.get("source_contract_sha256")
            != requirement.calibration_source_contract_sha256
            or source.get("target_bars_sha256") != requirement.target_bars_sha256
            or source.get("target_sidecar_sha256") != requirement.target_sidecar_sha256
            or source.get("financing_schedule_sha256")
            != requirement.financing_schedule_sha256
            or source.get("projection_contract_version")
            != requirement.projection_contract_version
            or source.get("numerical_execution_cost_policy_sha256")
            != requirement.numerical_execution_cost_policy_sha256
            or source.get("entry_slippage_contract_version")
            != requirement.entry_slippage_contract_version
            or source.get("swap_accrual_contract_version")
            != requirement.swap_accrual_contract_version
            or source.get("trade_record_schema_version")
            != requirement.trade_record_schema_version
            or source.get("swap_provenance_class")
            != requirement.swap_provenance_class
            or source.get("historical_financing_provenance")
            != requirement.historical_financing_provenance
            or source.get("execution_state_compounding") is not True
            or source.get("swap_status")
            != (
                "DISABLED"
                if requirement.swap_provenance_class
                == "AUTHENTICATED_BROKER_DISABLED"
                else "AVAILABLE"
            )
            or (
                partition == "optimizer_train"
                and (
                    source.get("partition_source_contract_sha256")
                    != requirement.calibration_partition_source_contract_sha256
                    or source.get("numerical_execution_cost_attestation_sha256")
                    != requirement.calibration_numerical_execution_cost_attestation_sha256
                )
            )
        ):
            return "INFRA", "TRADE_POWER_DECISION_SAMPLE_INVALID"
        if pass_shape and not (
            sample_status == "MEASURED"
            and sample_outcome == SearchOutcomeKind.VALID.value
        ):
            return "INFRA", "TRADE_POWER_DECISION_SAMPLE_INVALID"
        if ineligible_shape and sample_outcome not in {
            SearchOutcomeKind.VALID.value,
            SearchOutcomeKind.EXPECTED_INELIGIBLE.value,
        }:
            return "INFRA", "TRADE_POWER_DECISION_SAMPLE_INVALID"
        return (
            ("PASS", "")
            if pass_shape
            else ("INELIGIBLE", "TRADE_POWER_UNDERPOWERED")
        )

    @staticmethod
    def _frozen_requirement_from_validation_evidence(
        evidence: Mapping[str, Any],
    ) -> TradePowerRequirement:
        trade_power = evidence.get("trade_power")
        train = trade_power.get("train") if isinstance(trade_power, Mapping) else None
        payload = train.get("frozen_requirement") if isinstance(train, Mapping) else None
        if not isinstance(payload, Mapping):
            raise ValueError("frozen trade-power requirement is absent")
        values = dict(payload)
        values["reason_codes"] = tuple(values.get("reason_codes") or ())
        allowed = set(TradePowerRequirement.__dataclass_fields__)
        if set(values) - allowed:
            raise ValueError("frozen trade-power requirement has unknown fields")
        return TradePowerRequirement(**values)

    @staticmethod
    def _compact_backtest_result(result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Keep scalar metrics only after trades were bucketed."""
        if not isinstance(result, dict):
            return {}
        return {
            key: value
            for key, value in result.items()
            if key not in {"equity_curve", "trades", "position_events"}
        }

    @staticmethod
    def _compact_metric_tree(value: Any) -> Any:
        """JSON-safe scalar/nested metric snapshot for evidence manifests."""
        if isinstance(value, dict):
            return {
                str(key): RegimeOptimizer._compact_metric_tree(item)
                for key, item in value.items()
                if key not in {"equity_curve", "trades", "position_events"}
            }
        if isinstance(value, (list, tuple)):
            return [RegimeOptimizer._compact_metric_tree(item) for item in value]
        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                pass
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            value = float(value)
        if isinstance(value, float):
            return value if math.isfinite(value) else 0.0
        if isinstance(value, (int, str, bool)) or value is None:
            return value
        return str(value)

    @staticmethod
    def _period_returns_from_equity(equity_curve: Any,
                                    periods: int,
                                    min_periods: int) -> List[float]:
        try:
            if equity_curve is None:
                return []
            equity = np.asarray(equity_curve, dtype=np.float64)
        except Exception:
            return []
        if equity.size < 3:
            return []
        base = np.where(np.abs(equity[:-1]) > 1e-12, equity[:-1], 1.0)
        returns = np.diff(equity) / base
        returns = returns[np.isfinite(returns)]
        if returns.size < min_periods:
            return []
        block_count = min(max(2, int(periods)), int(returns.size))
        if block_count < min_periods:
            return []
        period_returns = []
        for block in np.array_split(returns, block_count):
            if block.size == 0:
                continue
            value = float(np.prod(1.0 + block) - 1.0)
            if math.isfinite(value):
                period_returns.append(value)
        if len(period_returns) < min_periods:
            return []
        return period_returns

    @staticmethod
    def _trim_equity_curve_for_warmup(equity_curve: Any, warmup_bars: int) -> List[float]:
        """Return the scored equity path while keeping the pre-score baseline point."""

        try:
            equity = np.asarray(equity_curve, dtype=np.float64)
        except Exception:
            return []
        if equity.size <= 2:
            return equity.tolist()
        warmup = max(0, int(warmup_bars or 0))
        if warmup <= 0:
            return equity.tolist()
        start = max(0, min(warmup - 1, int(equity.size) - 2))
        return equity[start:].tolist()

    @staticmethod
    def _trade_path_evidence(trades: Any, max_points: int = 256) -> Dict[str, Any]:
        """Compact trade-index evidence for path/spoke discovery.

        Uses R multiples when present so the path is comparable across instruments.
        This is evidence only; it does not change candidate selection.
        """
        if not isinstance(trades, list) or not trades:
            return {
                "status": "no_trades",
                "trade_count": 0,
                "r_multiple_sequence": [],
                "cumulative_r_blocks": [],
            }
        r_values: List[float] = []
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            value = trade.get("r_multiple")
            if value is None:
                pnl = trade.get("pnl_dollars")
                risk = trade.get("risk_amount")
                try:
                    value = float(pnl) / float(risk) if abs(float(risk)) > 1e-12 else 0.0
                except Exception:
                    value = 0.0
            try:
                value = float(value)
            except Exception:
                value = 0.0
            if math.isfinite(value):
                r_values.append(value)
        if not r_values:
            return {
                "status": "unavailable",
                "trade_count": len(trades),
                "r_multiple_sequence": [],
                "cumulative_r_blocks": [],
            }
        capped = max(0, int(max_points or 0))
        sequence = r_values[-capped:] if capped > 0 and len(r_values) > capped else list(r_values)
        cumulative = np.cumsum(np.asarray(sequence, dtype=np.float64))
        block_count = min(64, max(1, len(cumulative)))
        blocks = [
            float(block[-1]) if len(block) else 0.0
            for block in np.array_split(cumulative, block_count)
        ]
        gains = np.maximum(np.asarray(sequence, dtype=np.float64), 0.0)
        total_gain = float(gains.sum())
        last_quarter_gain = float(gains[int(max(0, len(gains) * 0.75)):].sum()) if gains.size else 0.0
        cluster = 3 if len(sequence) >= 3 else max(1, len(sequence))
        rolling_cluster = [
            float(sum(sequence[idx:idx + cluster]))
            for idx in range(0, max(1, len(sequence) - cluster + 1))
        ]
        return {
            "status": "available",
            "trade_count": len(r_values),
            "stored_points": len(sequence),
            "truncated": bool(len(sequence) < len(r_values)),
            "r_multiple_sequence": [round(float(value), 6) for value in sequence],
            "cumulative_r_blocks": [round(float(value), 6) for value in blocks],
            "mean_r": round(float(np.mean(sequence)), 6),
            "median_r": round(float(np.median(sequence)), 6),
            "positive_trade_fraction": round(float((np.asarray(sequence) > 0.0).mean()), 6),
            "worst_trade_r": round(float(np.min(sequence)), 6),
            "worst_cluster_r": round(float(min(rolling_cluster)), 6) if rolling_cluster else 0.0,
            "late_gain_share": round(float(last_quarter_gain / total_gain), 6) if total_gain > 1e-12 else 0.0,
        }

    @staticmethod
    def _equity_path_evidence(equity_curve: Any, max_points: int = 256) -> Dict[str, Any]:
        """Compact equity evidence with higher-is-better capital preservation blocks."""

        try:
            equity = np.asarray(equity_curve, dtype=np.float64)
        except Exception:
            equity = np.asarray([], dtype=np.float64)
        equity = equity[np.isfinite(equity)]
        if equity.size < 2:
            return {
                "status": "unavailable",
                "stored_points": 0,
                "return_pct_blocks": [],
                "capital_preservation_blocks": [],
            }
        capped = max(0, int(max_points or 0))
        path = equity[-capped:] if capped > 0 and equity.size > capped else equity
        if path.size < 2:
            return {
                "status": "unavailable",
                "stored_points": int(path.size),
                "return_pct_blocks": [],
                "capital_preservation_blocks": [],
            }
        start = float(path[0]) if abs(float(path[0])) > 1e-12 else 1.0
        returns_pct = ((path / start) - 1.0) * 100.0
        peak = np.maximum.accumulate(path)
        drawdown_pct = np.where(peak > 1e-12, (peak - path) / peak, 0.0) * 100.0
        capital_preservation = np.clip(1.0 - (drawdown_pct / 100.0), 0.0, 1.0)
        block_count = min(64, max(2, int(path.size)))

        def _last_blocks(values: np.ndarray) -> List[float]:
            return [
                round(float(block[-1]), 6) if len(block) else 0.0
                for block in np.array_split(values, block_count)
            ]

        return {
            "status": "available",
            "stored_points": int(path.size),
            "truncated": bool(path.size < equity.size),
            "return_pct_blocks": _last_blocks(returns_pct),
            "drawdown_pct_blocks": _last_blocks(drawdown_pct),
            "capital_preservation_blocks": _last_blocks(capital_preservation),
            "max_drawdown_pct": round(float(np.max(drawdown_pct)), 6),
            "time_underwater_fraction": round(float((drawdown_pct > 1e-9).mean()), 6),
        }

    @staticmethod
    def _execution_cost_survival_evidence(baseline: Dict[str, Any],
                                          stress: Optional[Dict[str, Any]],
                                          stress_mode: str) -> Dict[str, Any]:
        if not isinstance(stress, dict):
            return {"status": "not_requested", "stress_mode": stress_mode}
        status = str(stress.get("status", "") or "").strip().lower()
        if status in {"capped", "error"}:
            payload = {"status": status, "stress_mode": stress_mode}
            if stress.get("error"):
                payload["error"] = str(stress.get("error"))[:240]
            return payload
        base_ret = float(baseline.get("total_return_pct", 0.0) or 0.0)
        stress_ret = float(stress.get("total_return_pct", 0.0) or 0.0)
        base_pf = float(baseline.get("profit_factor", 0.0) or 0.0)
        stress_pf = float(stress.get("profit_factor", 0.0) or 0.0)
        base_dd = float(baseline.get("max_drawdown_pct", 0.0) or 0.0)
        stress_dd = float(stress.get("max_drawdown_pct", 0.0) or 0.0)
        return {
            "status": "available",
            "stress_mode": stress_mode,
            "baseline_return_pct": round(base_ret, 6),
            "stress_return_pct": round(stress_ret, 6),
            "baseline_profit_factor": round(base_pf, 6),
            "stress_profit_factor": round(stress_pf, 6),
            "baseline_max_drawdown_pct": round(base_dd, 6),
            "stress_max_drawdown_pct": round(stress_dd, 6),
            "return_retention": round(float(stress_ret / base_ret), 6)
            if abs(base_ret) > 1e-9 else 0.0,
            "profit_factor_retention": round(float(stress_pf / base_pf), 6)
            if base_pf > 1e-9 else 0.0,
            "drawdown_delta_pct": round(float(stress_dd - base_dd), 6),
            "survives_positive_return": bool(stress_ret > 0.0),
            "survives_pf_ge_1": bool(stress_pf >= 1.0),
        }

    def _build_oos_evidence_record(self,
                                   symbol: str,
                                   timeframe: str,
                                   candidate: Dict[str, Any],
                                   val_result: Dict[str, Any],
                                   val_features: pd.DataFrame,
                                   val_warmup_bars: int,
                                   stress_result: Optional[Dict[str, Any]] = None,
                                   stress_mode: str = "") -> Optional[Dict[str, Any]]:
        periods = int(getattr(self.config, "oos_evidence_periods", 64) or 64)
        min_periods = int(getattr(self.config, "oos_evidence_min_periods", 8) or 8)
        scored_equity_curve = self._trim_equity_curve_for_warmup(
            val_result.get("equity_curve", []),
            val_warmup_bars,
        )
        period_returns = self._period_returns_from_equity(
            scored_equity_curve,
            periods=periods,
            min_periods=min_periods,
        )
        if not period_returns and int(val_result.get("total_trades", 0) or 0) == 0:
            try:
                usable_returns = max(0, int(len(val_features)) - int(val_warmup_bars or 0) - 1)
            except Exception:
                usable_returns = 0
            zero_periods = min(max(2, periods), usable_returns)
            if zero_periods >= min_periods:
                period_returns = [0.0] * zero_periods
        if not period_returns:
            return None
        params = self._clean_candidate_params(candidate.get("params", {}))
        strategy_name = str(candidate.get("strategy_name", "Unknown"))
        evaluation_regime = self._candidate_evaluation_regime(candidate)
        candidate_id = self._exact_cell_candidate_id(
            symbol,
            timeframe,
            evaluation_regime,
            {**candidate, "strategy_name": strategy_name, "params": params},
        )
        start_ts = None
        end_ts = None
        try:
            if val_features is not None and len(val_features.index):
                start_ts = pd.Timestamp(val_features.index[0]).isoformat()
                end_ts = pd.Timestamp(val_features.index[-1]).isoformat()
        except Exception:
            start_ts = None
            end_ts = None
        train_metrics = self._compact_metric_tree(candidate.get("train_result", {}))
        train_regime_metrics = self._compact_metric_tree(candidate.get("train_regime_metrics", {}))
        val_regime_metrics = self._compact_metric_tree(candidate.get("val_regime_metrics", {}))
        val_metrics = {
            "total_trades": int(val_result.get("total_trades", 0) or 0),
            "profit_factor": float(val_result.get("profit_factor", 0.0) or 0.0),
            "total_return_pct": float(val_result.get("total_return_pct", 0.0) or 0.0),
            "max_drawdown_pct": float(val_result.get("max_drawdown_pct", 0.0) or 0.0),
            "win_rate": float(val_result.get("win_rate", 0.0) or 0.0),
        }
        train_return = float(train_metrics.get("total_return_pct", 0.0) or 0.0) if isinstance(train_metrics, dict) else 0.0
        train_pf = float(train_metrics.get("profit_factor", 0.0) or 0.0) if isinstance(train_metrics, dict) else 0.0
        val_return = float(val_metrics.get("total_return_pct", 0.0) or 0.0)
        val_pf = float(val_metrics.get("profit_factor", 0.0) or 0.0)
        return_retention = val_return / train_return if abs(train_return) > 1e-9 else 0.0
        pf_retention = val_pf / train_pf if train_pf > 1e-9 else 0.0
        baseline_signal_sha256 = str(
            val_result.get("_baseline_gated_signals_sha256", "") or ""
        )
        expected_signal_sha256 = str(
            candidate.get("_baseline_gated_signals_sha256", "") or ""
        )
        if (
            not self._sha256_attestation(baseline_signal_sha256)
            or (
                bool(candidate.get("trade_power_contract_enforced", False))
                and expected_signal_sha256 != baseline_signal_sha256
            )
        ):
            raise ValueError("frozen validation signal attestation is missing or mismatched")
        candidate["_baseline_gated_signals_sha256"] = baseline_signal_sha256
        record = {
            "format_version": "oos_candidate_returns_v4",
            "candidate_id": candidate_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": evaluation_regime,
            "strategy_name": strategy_name,
            "parameters": params,
            "is_tuned": bool(candidate.get("is_tuned", False)),
            "tuned_for_regime": candidate.get("tuned_for_regime"),
            "search_trials": int(candidate.get("search_trials", 1) or 1),
            "local_search_trials_total": int(candidate.get("local_search_trials_total", 0) or 0),
            "local_search_cells_total": int(candidate.get("local_search_cells_total", 0) or 0),
            "family_probe_search_trials_total": int(
                candidate.get("family_probe_search_trials_total", 0) or 0
            ),
            "family_probe_search_cells_total": int(
                candidate.get("family_probe_search_cells_total", 0) or 0
            ),
            "tuning_search_trials_total": int(
                candidate.get("tuning_search_trials_total", 0) or 0
            ),
            "tuning_search_cells_total": int(
                candidate.get("tuning_search_cells_total", 0) or 0
            ),
            "multiplicity_scope": candidate.get("multiplicity_scope"),
            "strategy_roster_count": int(candidate.get("strategy_roster_count", 0) or 0),
            "oos_surface": "stage3_selection",
            "return_index": "relative_equal_blocks",
            "period_count": int(len(period_returns)),
            "period_returns": period_returns,
            "baseline_gated_signals_sha256": baseline_signal_sha256,
            "trade_path_evidence": self._trade_path_evidence(
                val_result.get("trades", []),
                max_points=int(getattr(self.config, "oos_evidence_trade_path_max_points", 256) or 0),
            ),
            "equity_path_evidence": self._equity_path_evidence(
                scored_equity_curve,
                max_points=int(getattr(self.config, "oos_evidence_trade_path_max_points", 256) or 0),
            ),
            "metrics": val_metrics,
            "train_metrics": train_metrics,
            "train_regime_metrics": train_regime_metrics,
            "val_regime_metrics": val_regime_metrics,
            "execution_cost_survival": self._execution_cost_survival_evidence(
                val_metrics,
                stress_result,
                stress_mode or str(getattr(self.config, "oos_evidence_execution_stress_mode", "stress_p95") or "stress_p95"),
            ),
            "parameter_stability_evidence": self._compact_metric_tree(
                candidate.get("parameter_stability_evidence", {"status": "not_available"})
            ),
            "generalization_metrics": {
                "train_total_return_pct": train_return,
                "val_total_return_pct": val_return,
                "return_retention": return_retention if math.isfinite(return_retention) else 0.0,
                "train_profit_factor": train_pf,
                "val_profit_factor": val_pf,
                "profit_factor_retention": pf_retention if math.isfinite(pf_retention) else 0.0,
            },
            "window": {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "bars": int(len(val_features)) if val_features is not None else 0,
                "warmup_bars": int(val_warmup_bars or 0),
            },
        }
        baseline_evidence_sha256 = _oos_baseline_evidence_sha256(record)
        record["baseline_evidence_sha256"] = baseline_evidence_sha256
        candidate["_oos_baseline_attestation_sha256"] = baseline_evidence_sha256
        return record

    def _validated_existing_oos_record(
        self,
        record: Mapping[str, Any],
        *,
        symbol: str,
        timeframe: str,
        candidate: Mapping[str, Any],
        val_features: pd.DataFrame,
        val_warmup_bars: int,
    ) -> Dict[str, Any]:
        """Authenticate one in-run baseline record and refresh final metadata."""
        regime = self._candidate_evaluation_regime(candidate)
        strategy_name = str(candidate.get("strategy_name", ""))
        params = self._clean_candidate_params(candidate.get("params", {}))
        expected_id = self._exact_cell_candidate_id(
            symbol,
            timeframe,
            regime,
            {**dict(candidate), "strategy_name": strategy_name, "params": params},
        )
        try:
            expected_start = pd.Timestamp(val_features.index[0]).isoformat()
            expected_end = pd.Timestamp(val_features.index[-1]).isoformat()
        except Exception as exc:
            raise ValueError("validation window identity is unavailable") from exc
        window = record.get("window")
        period_returns = record.get("period_returns")
        required_maps = (
            "metrics",
            "train_metrics",
            "train_regime_metrics",
            "val_regime_metrics",
            "trade_path_evidence",
            "equity_path_evidence",
            "execution_cost_survival",
            "parameter_stability_evidence",
            "generalization_metrics",
        )
        try:
            finite_returns = [float(value) for value in period_returns]
            recomputed_baseline_sha256 = _oos_baseline_evidence_sha256(record)
            metrics = record.get("metrics")
            metric_keys = {
                "total_trades",
                "profit_factor",
                "total_return_pct",
                "max_drawdown_pct",
                "win_rate",
            }
            if not isinstance(metrics, Mapping) or set(metrics) != metric_keys:
                raise ValueError("OOS baseline metrics schema is invalid")
            total_trades = int(metrics.get("total_trades"))
            profit_factor = float(metrics.get("profit_factor"))
            total_return = float(metrics.get("total_return_pct"))
            drawdown = float(metrics.get("max_drawdown_pct"))
            win_rate = float(metrics.get("win_rate"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("OOS baseline evidence is malformed") from exc
        if (
            str(record.get("format_version") or "") != "oos_candidate_returns_v4"
            or str(record.get("candidate_id") or "") != expected_id
            or str(record.get("symbol") or "") != str(symbol)
            or str(record.get("timeframe") or "").upper() != str(timeframe).upper()
            or str(record.get("regime") or "") != regime
            or str(record.get("strategy_name") or "") != strategy_name
            or record.get("parameters") != params
            or str(record.get("oos_surface") or "") != "stage3_selection"
            or str(record.get("return_index") or "") != "relative_equal_blocks"
            or not self._sha256_attestation(
                record.get("baseline_gated_signals_sha256")
            )
            or not self._sha256_attestation(record.get("baseline_evidence_sha256"))
            or record.get("baseline_evidence_sha256")
            != recomputed_baseline_sha256
            or candidate.get("_oos_baseline_attestation_sha256")
            != recomputed_baseline_sha256
            or candidate.get("_baseline_gated_signals_sha256")
            != record.get("baseline_gated_signals_sha256")
            or not isinstance(window, Mapping)
            or window.get("start_ts") != expected_start
            or window.get("end_ts") != expected_end
            or int(window.get("bars", -1)) != int(len(val_features))
            or int(window.get("warmup_bars", -1)) != int(val_warmup_bars or 0)
            or int(record.get("period_count", -1)) != len(finite_returns)
            or len(finite_returns) < int(
                getattr(self.config, "oos_evidence_min_periods", 8) or 8
            )
            or not all(math.isfinite(value) for value in finite_returns)
            or total_trades < 0
            or math.isnan(profit_factor)
            or not math.isfinite(total_return)
            or not math.isfinite(drawdown)
            or drawdown < 0.0
            or not math.isfinite(win_rate)
            or not 0.0 <= win_rate <= 100.0
            or any(not isinstance(record.get(key), Mapping) for key in required_maps)
        ):
            raise ValueError("OOS baseline record identity or evidence is invalid")
        refreshed = dict(record)
        refreshed["is_tuned"] = bool(candidate.get("is_tuned", False))
        refreshed["tuned_for_regime"] = candidate.get("tuned_for_regime")
        for key in (
            "search_trials",
            "local_search_trials_total",
            "local_search_cells_total",
            "family_probe_search_trials_total",
            "family_probe_search_cells_total",
            "tuning_search_trials_total",
            "tuning_search_cells_total",
            "strategy_roster_count",
        ):
            default = 1 if key == "search_trials" else 0
            refreshed[key] = int(candidate.get(key, default) or default)
        refreshed["multiplicity_scope"] = candidate.get("multiplicity_scope")
        return refreshed

    def _attach_oos_evidence_to_candidates(self,
                                           symbol: str,
                                           timeframe: str,
                                           val_features: Optional[pd.DataFrame],
                                           val_warmup_bars: int,
                                           candidates: List[Dict[str, Any]]) -> None:
        if not bool(getattr(self.config, "oos_evidence_export_enabled", False)):
            return
        if val_features is None or len(val_features) <= max(2, int(val_warmup_bars or 0)):
            return
        records_by_id: Dict[str, Dict[str, Any]] = {}
        stress_enabled = bool(getattr(self.config, "oos_evidence_include_execution_cost_survival", False))
        stress_mode = str(getattr(self.config, "oos_evidence_execution_stress_mode", "stress_p95") or "stress_p95")
        stress_cap = int(getattr(self.config, "oos_evidence_execution_cost_max_candidates_per_tf", 0) or 0)
        stress_backtester = None
        stress_attempts = 0
        if stress_enabled:
            try:
                stress_config = replace(self.config, backtest_spread_mode=stress_mode)
                stress_backtester = Backtester(stress_config)
            except Exception as exc:
                logger.warning(
                    f"[{symbol}] [{timeframe}] Execution-cost evidence disabled: "
                    f"failed to build stress backtester ({exc})"
                )
                stress_enabled = False
        for cand in candidates:
            try:
                evaluation_regime = self._candidate_evaluation_regime(cand)
                if not evaluation_regime:
                    logger.debug(
                        f"[{symbol}] [{timeframe}] OOS evidence skipped for "
                        f"{cand.get('strategy_name', 'Unknown')}: candidate has no exact regime identity"
                    )
                    continue
                existing_record = cand.get("oos_evidence")
                if isinstance(existing_record, Mapping):
                    try:
                        existing_record = self._validated_existing_oos_record(
                            existing_record,
                            symbol=symbol,
                            timeframe=timeframe,
                            candidate=cand,
                            val_features=val_features,
                            val_warmup_bars=val_warmup_bars,
                        )
                    except Exception as record_exc:
                        raise SearchInfrastructureError(TrialOutcome(
                            kind=SearchOutcomeKind.INFRA_FAILURE,
                            stage=SearchStage.FROZEN_VALIDATION,
                            reason_code="FROZEN_VALIDATION_REPLAY_EVIDENCE_INVALID",
                            reason=f"{type(record_exc).__name__}: {record_exc}",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=evaluation_regime,
                            strategy_family=str(cand.get("strategy_name", "")),
                        )) from record_exc
                if (
                    not isinstance(existing_record, Mapping)
                    and bool(cand.get("trade_power_contract_enforced", False))
                ):
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FROZEN_VALIDATION,
                        reason_code="FROZEN_VALIDATION_REPLAY_EVIDENCE_MISSING",
                        reason=(
                            "governed candidate did not retain evidence from its "
                            "single tuning-validation evaluation"
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=evaluation_regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                    ))
                strategy = None
                signals = None
                if stress_enabled or not isinstance(existing_record, Mapping):
                    strategy = (
                        StrategyRegistry.get(
                            cand.get("strategy_name", ""),
                            **(cand.get("params", {}) or {}),
                        )
                        if isinstance(existing_record, Mapping)
                        else cand.get("strategy")
                    )
                    if strategy is None:
                        strategy = StrategyRegistry.get(
                            cand.get("strategy_name", ""),
                            **(cand.get("params", {}) or {}),
                        )
                    raw_signals = strategy.generate_signals(val_features, symbol)
                    signals = self._gate_signals_to_exact_regime(
                        raw_signals,
                        val_features,
                        evaluation_regime,
                    )
                    if (
                        isinstance(existing_record, Mapping)
                        and _signal_vector_sha256(signals)
                        != existing_record.get("baseline_gated_signals_sha256")
                    ):
                        raise SearchInfrastructureError(TrialOutcome(
                            kind=SearchOutcomeKind.INFRA_FAILURE,
                            stage=SearchStage.FROZEN_VALIDATION,
                            reason_code="FROZEN_VALIDATION_SIGNAL_REPLAY_MISMATCH",
                            reason="stress replay signal vector differs from frozen baseline",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=evaluation_regime,
                            strategy_family=str(cand.get("strategy_name", "")),
                        ))
                val_result = None
                if not isinstance(existing_record, Mapping):
                    val_result = self.backtester.run(
                        val_features,
                        signals,
                        symbol,
                        strategy,
                        timeframe=timeframe,
                        warmup_bars=val_warmup_bars,
                    )
                    val_result["_baseline_gated_signals_sha256"] = (
                        _signal_vector_sha256(signals)
                    )
                stress_result = None
                if stress_enabled and stress_backtester is not None:
                    if stress_cap > 0 and stress_attempts >= stress_cap:
                        stress_result = {"status": "capped", "stress_mode": stress_mode}
                    else:
                        stress_attempts += 1
                        try:
                            stress_result = stress_backtester.run(
                                val_features,
                                signals,
                                symbol,
                                strategy,
                                timeframe=timeframe,
                                warmup_bars=val_warmup_bars,
                            )
                        except Exception as stress_exc:
                            stress_result = {
                                "status": "error",
                                "stress_mode": stress_mode,
                                "error": f"{type(stress_exc).__name__}: {stress_exc}",
                            }
                if isinstance(existing_record, Mapping):
                    record = dict(existing_record)
                    if stress_result is not None:
                        baseline = dict(record.get("metrics") or {})
                        record["execution_cost_survival"] = (
                            self._execution_cost_survival_evidence(
                                baseline,
                                stress_result,
                                stress_mode,
                            )
                        )
                else:
                    record = self._build_oos_evidence_record(
                        symbol,
                        timeframe,
                        cand,
                        dict(val_result or {}),
                        val_features,
                        val_warmup_bars,
                        stress_result=stress_result,
                        stress_mode=stress_mode,
                    )
                if record:
                    if record["candidate_id"] in records_by_id:
                        raise SearchInfrastructureError(TrialOutcome(
                            kind=SearchOutcomeKind.INFRA_FAILURE,
                            stage=SearchStage.FAMILY_CELL,
                            reason_code="DUPLICATE_OOS_CANDIDATE_ID",
                            reason="multiple OOS records share one exact candidate identity",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=evaluation_regime,
                            strategy_family=str(cand.get("strategy_name", "")),
                        ))
                    cand["oos_evidence"] = record
                    cost_evidence = record.get("execution_cost_survival")
                    if isinstance(cost_evidence, dict):
                        cand["selection_execution_cost_survival"] = cost_evidence
                    records_by_id[record["candidate_id"]] = record
            except Exception as exc:
                if isinstance(exc, SearchInfrastructureError):
                    raise
                outcome = _classify_staged_search_exception(
                    exc,
                    stage=SearchStage.FROZEN_VALIDATION,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=self._candidate_evaluation_regime(cand),
                    strategy_family=str(cand.get("strategy_name", "")),
                    parameter_hash=_stable_hash(
                        self._clean_candidate_params(cand.get("params", {}))
                    ),
                )
                raise SearchInfrastructureError(outcome) from exc
        if records_by_id:
            self.oos_candidate_evidence_records.extend(records_by_id.values())

    def _attach_threadspark_selection_evidence_to_candidates(self,
                                                             symbol: str,
                                                             timeframe: str,
                                                             selection_features: Optional[pd.DataFrame],
                                                             selection_warmup_bars: int,
                                                             historical_features: Optional[pd.DataFrame],
                                                             candidates: List[Dict[str, Any]],
                                                             *,
                                                             selection_surface: str) -> None:
        """Attach post-search evidence used only by the ThreadSpark selector.

        This runs after the Optuna candidate pool is fixed. It deliberately writes
        evidence onto candidate dictionaries instead of mutating train/validation
        metrics, so the inner TPE objective remains stationary and leak-clean.
        """

        scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
        if scoring_model != "specialist_spokes_threadspark":
            return

        surface_name = str(selection_surface or "validation")
        has_selection = selection_features is not None and len(selection_features) > max(2, int(selection_warmup_bars or 0))
        if has_selection and "REGIME_LIVE" not in selection_features.columns:
            has_selection = False
        requested_clean_holdout_missing = surface_name == "clean_holdout" and not has_selection
        has_historical = (
            bool(getattr(self.config, "threadspark_use_historical_cost_evidence", True))
            and historical_features is not None
            and len(historical_features) >= 50
            and "REGIME_LIVE" in historical_features.columns
        )
        if not has_selection and not has_historical and not requested_clean_holdout_missing:
            return

        historical_chunk_count = max(1, int(getattr(self.config, "threadspark_historical_chunk_count", 5) or 1))

        def _historical_chunk_buckets(
            trades: List[Dict[str, Any]],
            features: pd.DataFrame,
        ) -> List[Dict[str, Dict[str, Any]]]:
            if historical_chunk_count <= 1 or features is None or len(features) < 100:
                return []
            chunks = [chunk for chunk in np.array_split(np.arange(len(features)), historical_chunk_count) if len(chunk)]
            chunk_buckets: List[Dict[str, Dict[str, Any]]] = []
            for chunk in chunks:
                start = int(chunk[0])
                end = int(chunk[-1]) + 1
                if end - start < 20:
                    continue
                adjusted_trades: List[Dict[str, Any]] = []
                for trade in trades:
                    raw_signal = trade.get("signal_bar", trade.get("entry_bar", None))
                    try:
                        signal_bar = int(raw_signal)
                    except (TypeError, ValueError):
                        continue
                    if not (start <= signal_bar < end):
                        continue
                    adjusted = dict(trade)
                    for key in ("signal_bar", "entry_bar", "exit_bar"):
                        if key in adjusted:
                            try:
                                adjusted[key] = int(adjusted[key]) - start
                            except (TypeError, ValueError):
                                pass
                    adjusted_trades.append(adjusted)
                if not adjusted_trades:
                    continue
                chunk_features = features.iloc[start:end].copy()
                chunk_buckets.append(
                    self._bucket_trades_by_regime(
                        adjusted_trades,
                        chunk_features,
                        timeframe=timeframe,
                    )
                )
            return chunk_buckets

        for cand in candidates:
            cand["threadspark_selection_requested_surface"] = surface_name
            cand["threadspark_selection_missing"] = bool(requested_clean_holdout_missing)
            if requested_clean_holdout_missing:
                cand["threadspark_selection_surface"] = "clean_holdout_missing"
                cand["threadspark_selection_missing_reason"] = (
                    "requested_clean_holdout_unavailable_or_too_thin"
                )
            try:
                evaluation_regime = self._candidate_evaluation_regime(cand)
                if not evaluation_regime:
                    raise ValueError("candidate has no exact regime identity")
                strategy = cand.get("strategy")
                if strategy is None:
                    strategy = StrategyRegistry.get(cand.get("strategy_name", ""), **(cand.get("params", {}) or {}))
            except Exception as exc:
                logger.debug(
                    f"[{symbol}] [{timeframe}] ThreadSpark evidence skipped for "
                    f"{cand.get('strategy_name', 'Unknown')}: {exc}"
                )
                continue

            if has_selection:
                try:
                    raw_signals = strategy.generate_signals(selection_features, symbol)
                    signals = self._gate_signals_to_exact_regime(
                        raw_signals,
                        selection_features,
                        evaluation_regime,
                    )
                    result = self.backtester.run(
                        selection_features,
                        signals,
                        symbol,
                        strategy,
                        timeframe=timeframe,
                        warmup_bars=selection_warmup_bars,
                    )
                    cand["threadspark_selection_regime_metrics"] = {
                        evaluation_regime: self._compact_exact_regime_result(
                            result,
                            evaluation_regime,
                        )
                    }
                    cand["threadspark_selection_surface"] = surface_name
                    cand["threadspark_selection_missing"] = False
                    record = self._build_oos_evidence_record(
                        symbol,
                        timeframe,
                        cand,
                        result,
                        selection_features,
                        selection_warmup_bars,
                    )
                    if record:
                        record["oos_surface"] = f"threadspark_{surface_name}"
                        cand["threadspark_selection_evidence"] = record
                except Exception as exc:
                    logger.debug(
                        f"[{symbol}] [{timeframe}] ThreadSpark selection surface failed for "
                        f"{cand.get('strategy_name', 'Unknown')}: {exc}"
                    )

            if has_historical:
                try:
                    raw_hist_signals = strategy.generate_signals(historical_features, symbol)
                    hist_signals = self._gate_signals_to_exact_regime(
                        raw_hist_signals,
                        historical_features,
                        evaluation_regime,
                    )
                    hist_result = self.backtester.run(
                        historical_features,
                        hist_signals,
                        symbol,
                        strategy,
                        timeframe=timeframe,
                    )
                    hist_trades = hist_result.get("trades", [])
                    cand["threadspark_historical_regime_metrics"] = {
                        evaluation_regime: self._compact_exact_regime_result(
                            hist_result,
                            evaluation_regime,
                        )
                    }
                    historical_chunks = _historical_chunk_buckets(hist_trades, historical_features)
                    if historical_chunks:
                        cand["threadspark_historical_chunk_regime_metrics"] = historical_chunks
                        cand["threadspark_historical_chunk_count"] = int(len(historical_chunks))
                except Exception as exc:
                    logger.debug(
                        f"[{symbol}] [{timeframe}] ThreadSpark historical surface failed for "
                        f"{cand.get('strategy_name', 'Unknown')}: {exc}"
                    )

    def _apply_training_eligibility_gates(self,
                                          candidates: List[Dict[str, Any]],
                                          symbol: str,
                                          timeframe: str) -> List[Dict[str, Any]]:
        """
        Hard thresholds that strategies must pass on TRAINING data before screening.
        Prevents "obviously bad" strategies from entering the optimization pool.

        This is applied BEFORE hyperparameter tuning to save compute on losing strategies.
        Thresholds must be LENIENT since strategies run with default (untuned) params.

        Checks (applied only to the frozen optimizer-train B0 candidate):
        - train_min_profit_factor: Default 0.5 (reject only catastrophic)
        - train_min_return_pct: Default -30.0% (allow moderate losses)
        - train_max_drawdown: Default 60.0% (reject only blowups)

        Args:
            candidates: List of candidate dicts from screening
            symbol: Symbol name for logging
            timeframe: Timeframe for logging

        Returns:
            Filtered list of eligible candidates
        """
        eligible = []
        rejected_reasons = []
        rejected_by_reason: Dict[str, int] = {}
        rescued_count = 0

        # Get configurable thresholds (defined in PipelineConfig dataclass)
        train_pf_floor = float(self.config.train_min_profit_factor)
        train_return_floor = float(self.config.train_min_return_pct)
        train_dd_ceiling = float(self.config.train_max_drawdown)

        for cand in candidates:
            # Candidate admission is model selection and therefore may inspect
            # optimizer-train only. Compatibility eligibility fields must not
            # override the frozen train result.
            train_result = cand.get('train_result', {})

            train_pf = float(train_result.get('profit_factor', 0))
            train_return = float(train_result.get('total_return_pct', 0))
            train_dd = float(train_result.get('max_drawdown_pct', 100))

            # Check training eligibility on full-sample metrics
            full_sample_pass = True
            reason = None

            if train_pf < train_pf_floor:
                full_sample_pass = False
                reason = f"train PF {train_pf:.2f} < {train_pf_floor}"
            elif train_return < train_return_floor:
                full_sample_pass = False
                reason = f"train return {train_return:.1f}% < {train_return_floor}"
            elif train_dd > train_dd_ceiling:
                full_sample_pass = False
                reason = f"train DD {train_dd:.1f}% > {train_dd_ceiling}"

            if full_sample_pass:
                eligible.append(cand)
            else:
                # Regime-local rescue: preserve strategies that are strong in at
                # least one regime even if full-sample is weak (regime specialists)
                regime_metrics = cand.get('train_regime_metrics', {})
                rescued = False
                for _rname, rm in regime_metrics.items():
                    regime_pf = float(rm.get('profit_factor', 0))
                    regime_return = float(rm.get('total_return_pct', 0))
                    regime_dd = float(rm.get('max_drawdown_pct', 100))
                    regime_trades = int(rm.get('total_trades', 0))
                    if (
                        regime_trades >= self.min_train_trades and
                        regime_pf >= max(train_pf_floor, 1.0) and
                        regime_return >= max(train_return_floor, 0.0) and
                        regime_dd <= train_dd_ceiling
                    ):
                        rescued = True
                        break
                if rescued:
                    rescued_count += 1
                    eligible.append(cand)
                else:
                    rejected_reasons.append(f"{cand['strategy_name']}: {reason}")
                    rejected_by_reason[str(reason or "unknown")] = rejected_by_reason.get(str(reason or "unknown"), 0) + 1

        if rejected_reasons:
            logger.info(
                f"[{symbol}] [{timeframe}] Training eligibility rejected {len(rejected_reasons)}/{len(candidates)}: "
                f"{rejected_reasons[:3]}{'...' if len(rejected_reasons) > 3 else ''}"
            )

        self.training_gate_telemetry[f"{symbol}:{timeframe}"] = {
            "candidates": len(candidates),
            "eligible": len(eligible),
            "rejected": len(rejected_reasons),
            "rescued": rescued_count,
            "rejected_by_reason": rejected_by_reason,
        }

        return eligible

    def _screening_variants_for_strategy(self, strategy: BaseStrategy) -> List[Tuple[BaseStrategy, Optional[float]]]:
        """Return first-pass strategy variants used before deeper optimization.

        Discovery can evaluate coarse paired ATR exit anchors at screening time
        so a strategy is not rejected solely because its default 2xATR exit was
        the wrong starting range.
        """
        if not self.screening_atr_anchor_enabled:
            return [(strategy, None)]

        variants: List[Tuple[BaseStrategy, Optional[float]]] = []
        base_params = dict(strategy.get_params() or {})
        for anchor in self.screening_atr_anchor_multipliers:
            params = dict(base_params)
            params['sl_atr_mult'] = float(anchor)
            params['tp_atr_mult'] = float(anchor)
            try:
                variants.append((StrategyRegistry.get(strategy.name, **params), float(anchor)))
            except InvalidStrategyParameterError as exc:
                logger.debug(
                    f"{strategy.name} screening ATR anchor {anchor} skipped: {exc}"
                )
            except Exception:
                raise
        return variants or [(strategy, None)]

    @staticmethod
    def _gate_signals_to_exact_regime(
        signals: pd.Series,
        features: pd.DataFrame,
        regime: str,
    ) -> pd.Series:
        """Mask entries outside one regime, matching optimizer/lockbox/live."""
        if not isinstance(signals, pd.Series):
            signals = pd.Series(signals, index=features.index)
        gated = signals.reindex(features.index).fillna(0).copy()
        if "REGIME_LIVE" not in features.columns:
            gated.iloc[:] = 0
            return gated
        mask = (
            features["REGIME_LIVE"]
            .reindex(gated.index)
            .astype(str)
            .str.upper()
            .eq(str(regime or "").upper())
            .fillna(False)
        )
        gated.loc[~mask] = 0
        return gated

    @staticmethod
    def _compact_exact_regime_result(
        result: Optional[Dict[str, Any]],
        regime: str,
    ) -> Dict[str, Any]:
        """Compact direct backtest metrics while preserving bucket aliases."""
        compact = RegimeOptimizer._compact_backtest_result(result)
        if "sharpe_approx" not in compact and "sharpe_ratio" in compact:
            compact["sharpe_approx"] = compact["sharpe_ratio"]
        if "sortino_approx" not in compact and "sortino_ratio" in compact:
            compact["sortino_approx"] = compact["sortino_ratio"]
        if "avg_win" not in compact and "avg_win_dollars" in compact:
            compact["avg_win"] = compact["avg_win_dollars"]
        if "avg_loss" not in compact and "avg_loss_dollars" in compact:
            compact["avg_loss"] = compact["avg_loss_dollars"]
        compact["regime_gated"] = True
        compact["evaluation_regime"] = str(regime)
        return compact

    def _build_daily_net_mtm_surface(
        self,
        *,
        result: Dict[str, Any],
        features: pd.DataFrame,
        timeframe: str,
        warmup_bars: int,
        partition: str,
    ) -> Dict[str, Any]:
        """Build one dated UTC-daily surface from final post-cost equity marks."""
        partition_name = str(partition or "").strip().lower() or "unspecified"
        attested_cost_contract = dict(
            result.get("execution_cost_timing_contract") or {}
        )
        swap_accrual_contract = dict(result.get("swap_accrual_contract") or {})
        timestamp_source = (
            "_BAR_CLOSE_TS"
            if isinstance(features, pd.DataFrame)
            and "_BAR_CLOSE_TS" in features.columns
            else "feature_index_utc"
        )
        contract: Dict[str, Any] = {
            "contract_version": DAILY_NET_MTM_CONTRACT_VERSION,
            "status": "UNAVAILABLE",
            "origin": "backtester_final_post_cost_equity_curve",
            "partition": partition_name,
            "timeframe": str(timeframe or "").upper(),
            "timezone": "UTC",
            "timestamp_source": timestamp_source,
            "timestamp_axis_contract": "explicit_observable_bar_close_v1",
            "daily_mark": "last_available_bar_mark_per_utc_date",
            "return_type": "simple_net_equity_return",
            "includes_zero_return_active_days": True,
            "cost_contract": {
                "spread_slippage_commission": "already_in_equity_curve",
                "entry_slippage": ENTRY_SLIPPAGE_TIMING_CONTRACT_VERSION,
                "swap_accrual": SWAP_ACCRUAL_TIMING_CONTRACT_VERSION,
                "swap": "already_in_equity_curve_causal_rollover_accrual",
            },
            "execution_cost_timing_contract": dict(attested_cost_contract),
            "swap_accrual_contract": dict(swap_accrual_contract),
            "uses_final_lockbox": partition_name == "final_lockbox",
            "required_execution_cost_timing_contract": dict(
                EXECUTION_COST_TIMING_CONTRACT
            ),
        }

        def unavailable(reason: str) -> Dict[str, Any]:
            return {
                "daily_net_mtm_returns": [],
                "daily_net_mtm_dates": [],
                "daily_net_mtm_observations": 0,
                "daily_net_mtm_contract": {
                    **contract,
                    "reason": str(reason),
                },
            }

        allowed_partitions = {
            "optimizer_train",
            "tuning_validation",
            "final_lockbox",
            "edge_challenger_fit",
        }
        if partition_name not in allowed_partitions:
            return unavailable("partition contract is unspecified or unsupported")
        if not isinstance(features, pd.DataFrame) or len(features) == 0:
            return unavailable("feature frame is empty")
        if timestamp_source != "_BAR_CLOSE_TS":
            return unavailable(
                "explicit observable _BAR_CLOSE_TS is required for causal post-close marks"
            )
        if attested_cost_contract != EXECUTION_COST_TIMING_CONTRACT:
            return unavailable(
                "Backtester result does not attest the required execution-cost timing contract"
            )
        swap_enabled = bool(getattr(self.config, "swap_model_enabled", True))
        required_swap_status = "AVAILABLE" if swap_enabled else "DISABLED"
        observed_swap_status = str(
            swap_accrual_contract.get("status") or ""
        ).upper()
        if (
            not bool(
                swap_accrual_contract.get("execution_state_compounding", False)
            )
            or (
                swap_enabled
                and str(swap_accrual_contract.get("timestamp_source") or "")
                != "_BAR_CLOSE_TS"
            )
        ):
            return unavailable(
                "Backtester swap accrual attestation is not causal on the explicit close axis"
            )
        if observed_swap_status != required_swap_status:
            return unavailable(
                "Backtester swap source contract is unavailable: "
                + str(
                    swap_accrual_contract.get("reason")
                    or f"expected {required_swap_status}, observed {observed_swap_status}"
                )
            )
        if swap_enabled and not self._admissible_swap_statistical_source(
            swap_accrual_contract
        ):
            return unavailable(
                "Backtester swap schedule/rate history lacks authenticated "
                "source provenance or an explicitly approved immutable stress policy"
            )
        if not swap_enabled and not self._admissible_swap_statistical_source(
            swap_accrual_contract
        ):
            return unavailable(
                "Configured swap disablement is not authenticated proof that "
                "the broker charges no financing"
            )
        try:
            if timestamp_source == "_BAR_CLOSE_TS":
                timestamps = pd.DatetimeIndex(
                    pd.to_datetime(features["_BAR_CLOSE_TS"], utc=True, errors="coerce")
                )
                bar_opens = pd.DatetimeIndex(
                    pd.to_datetime(features.index, utc=True, errors="coerce")
                )
            else:
                timestamps = pd.DatetimeIndex(
                    pd.to_datetime(features.index, utc=True, errors="coerce")
                )
                bar_opens = timestamps
        except Exception as exc:
            return unavailable(f"timestamp conversion failed: {type(exc).__name__}")
        if len(timestamps) != len(features) or timestamps.isna().any():
            return unavailable("timestamp axis is incomplete")
        if timestamps.has_duplicates:
            return unavailable("timestamp axis contains duplicates")
        if not timestamps.is_monotonic_increasing:
            return unavailable("timestamp axis is not monotonic increasing")
        if (
            timestamp_source == "_BAR_CLOSE_TS"
            and (
                len(bar_opens) != len(timestamps)
                or bar_opens.isna().any()
                or bool((timestamps <= bar_opens).any())
            )
        ):
            return unavailable("explicit bar-close timestamps are invalid")

        try:
            equity = np.asarray(result.get("equity_curve"), dtype=np.float64)
        except Exception:
            return unavailable("equity curve is non-numeric")
        if equity.ndim != 1 or equity.size != len(features):
            return unavailable(
                f"equity/features length mismatch: {int(equity.size)} != {len(features)}"
            )
        if not np.isfinite(equity).all():
            return unavailable("equity curve contains non-finite marks")

        scored_start = max(0, int(warmup_bars or 0))
        if scored_start >= len(features):
            return unavailable("warmup consumes the complete partition")
        baseline = (
            float(getattr(self.config, "initial_capital", 10000.0))
            if scored_start == 0
            else float(equity[scored_start - 1])
        )
        if not math.isfinite(baseline) or baseline <= 0.0:
            return unavailable("baseline equity is non-positive or non-finite")

        scored_timestamps = timestamps[scored_start:]
        scored_equity = equity[scored_start:]
        marks = pd.Series(scored_equity, index=scored_timestamps, dtype=float)
        daily_marks = marks.groupby(marks.index.normalize(), sort=True).last()
        if daily_marks.empty:
            return unavailable("no scored UTC-daily marks")
        previous = np.concatenate(
            ([baseline], daily_marks.to_numpy(dtype=np.float64)[:-1])
        )
        if not np.isfinite(previous).all() or bool((previous <= 0.0).any()):
            return unavailable("daily return denominator is non-positive or non-finite")
        returns = daily_marks.to_numpy(dtype=np.float64) / previous - 1.0
        if (
            not np.isfinite(returns).all()
            or bool((returns <= -1.0).any())
        ):
            return unavailable("daily return vector contains ruin or non-finite values")
        dates = [pd.Timestamp(value).isoformat() for value in daily_marks.index]
        if len(dates) != len(set(dates)):
            return unavailable("daily return axis contains duplicates")

        return {
            "daily_net_mtm_returns": [float(value) for value in returns.tolist()],
            "daily_net_mtm_dates": dates,
            "daily_net_mtm_observations": int(len(returns)),
            "daily_net_mtm_contract": {
                **contract,
                "status": "AVAILABLE",
                "reason": "final post-cost equity curve aggregated on UTC active days",
                "scored_start_bar": int(scored_start),
                "warmup_bars_excluded": int(scored_start),
                "baseline_equity": baseline,
                "scored_start_ts": pd.Timestamp(scored_timestamps[0]).isoformat(),
                "scored_end_ts": pd.Timestamp(scored_timestamps[-1]).isoformat(),
                "daily_axis_start": dates[0],
                "daily_axis_end": dates[-1],
                "observations": int(len(returns)),
                "zero_return_days": int(np.count_nonzero(np.abs(returns) <= 1e-15)),
                "equity_curve_points": int(equity.size),
                "trade_count": int(result.get("total_trades", 0) or 0),
            },
        }

    def _run_one_exact_regime(
        self,
        *,
        symbol: str,
        timeframe: str,
        features: pd.DataFrame,
        strategy: BaseStrategy,
        regime: str,
        warmup_bars: int = 0,
        run_zero_signal_path: bool = False,
        partition: str = "unspecified",
        capture_trade_power_inputs: bool = False,
    ) -> Tuple[Dict[str, Any], bool]:
        """Run one exact-regime cell and report whether Backtester.run executed."""
        family = str(getattr(strategy, "name", type(strategy).__name__))
        validation_partition = str(partition) == "tuning_validation"
        parameter_hash = ""
        try:
            parameter_hash = _stable_hash(
                self._clean_candidate_params(strategy.get_params())
            )
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=(
                    SearchStage.FROZEN_VALIDATION
                    if validation_partition
                    else SearchStage.TRAIN_EVIDENCE
                ),
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=family,
                parameter_hash=parameter_hash,
            )
            raise SearchInfrastructureError(outcome) from exc
        try:
            raw_signals = strategy.generate_signals(features, symbol)
            gated = self._gate_signals_to_exact_regime(raw_signals, features, regime)
            gated_signal_sha256 = (
                _signal_vector_sha256(gated)
                if capture_trade_power_inputs
                else ""
            )
            warmup = min(len(gated), max(0, int(warmup_bars or 0)))
            signal_start = max(0, warmup - 1)
            if gated.iloc[signal_start:].abs().sum() == 0 and not run_zero_signal_path:
                return {
                    "total_trades": 0,
                    "regime_gated": True,
                    "evaluation_regime": str(regime),
                }, False
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=(
                    SearchStage.FROZEN_VALIDATION
                    if validation_partition
                    else SearchStage.SIGNAL_GENERATION
                ),
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=family,
                parameter_hash=parameter_hash,
            )
            raise SearchInfrastructureError(outcome) from exc
        try:
            result = self.backtester.run(
                features,
                gated,
                symbol,
                strategy,
                warmup_bars=warmup,
                timeframe=timeframe,
            )
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=(
                    SearchStage.FROZEN_VALIDATION
                    if validation_partition
                    else SearchStage.TRAIN_BACKTEST
                ),
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=family,
                parameter_hash=parameter_hash,
            )
            raise SearchInfrastructureError(outcome) from exc
        try:
            compact = self._compact_exact_regime_result(result, regime)
            compact.update(
                self._build_daily_net_mtm_surface(
                    result=result,
                    features=features,
                    timeframe=timeframe,
                    warmup_bars=warmup,
                    partition=partition,
                )
            )
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=(
                    SearchStage.FROZEN_VALIDATION
                    if validation_partition
                    else SearchStage.TRAIN_EVIDENCE
                ),
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=family,
                parameter_hash=parameter_hash,
            )
            raise SearchInfrastructureError(outcome) from exc
        if capture_trade_power_inputs:
            # This transient payload is consumed and popped by the immediate
            # search callsite.  Candidate artifacts retain only the compact,
            # hash-bound decision, so memory does not scale with all trials.
            result["_baseline_gated_signals_sha256"] = gated_signal_sha256
            compact["_trade_power_raw_result"] = result
        return compact, True

    @staticmethod
    def _pop_trade_power_raw_result(metrics: Dict[str, Any]) -> Dict[str, Any]:
        raw = metrics.pop("_trade_power_raw_result", None)
        if not isinstance(raw, dict):
            raise RuntimeError("BACKTEST_TRADE_POWER_INPUT_MISSING")
        return raw

    def _run_exact_regime_screening(
        self,
        *,
        symbol: str,
        timeframe: str,
        features: Optional[pd.DataFrame],
        strategy: BaseStrategy,
        warmup_bars: int = 0,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Return raw and compact direct backtests for each exact regime."""
        if (
            features is None
            or len(features) < 2
            or "REGIME_LIVE" not in features.columns
        ):
            return {}, {}
        raw_signals = strategy.generate_signals(features, symbol)
        raw_results: Dict[str, Dict[str, Any]] = {}
        compact_results: Dict[str, Dict[str, Any]] = {}
        for regime in self.REGIMES:
            gated = self._gate_signals_to_exact_regime(raw_signals, features, regime)
            warmup = min(len(gated), max(0, int(warmup_bars or 0)))
            signal_start = max(0, warmup - 1)
            if gated.iloc[signal_start:].abs().sum() == 0:
                compact_results[regime] = {
                    "total_trades": 0,
                    "regime_gated": True,
                    "evaluation_regime": str(regime),
                }
                continue
            result = self.backtester.run(
                features,
                gated,
                symbol,
                strategy,
                warmup_bars=warmup,
                timeframe=timeframe,
            )
            raw_results[regime] = result
            compact_results[regime] = self._compact_exact_regime_result(
                result, regime
            )
        return raw_results, compact_results

    def _family_probe_param_specs(
        self,
        strategy: BaseStrategy,
        *,
        symbol: str,
        timeframe: str,
        regime: str,
    ) -> List[Tuple[Dict[str, Any], Optional[float]]]:
        """Equal-budget deterministic B0 design for one family-regime cell.

        Mandatory default/ATR anchors are seeded first. Remaining points walk
        the cartesian grid by a deterministic coprime mixed-radix cycle, which
        gives bounded uniform coverage without materializing a large grid.
        """
        specs: List[Tuple[Dict[str, Any], Optional[float]]] = []
        seen: set = set()

        def add(params: Dict[str, Any], anchor: Optional[float]) -> None:
            clean = self._clean_candidate_params(params)
            identity = json.dumps(clean, sort_keys=True, separators=(",", ":"), default=str)
            if identity in seen:
                return
            seen.add(identity)
            specs.append((clean, anchor))

        for seeded_strategy, anchor in self._screening_variants_for_strategy(strategy):
            add(dict(seeded_strategy.get_params() or {}), anchor)
        base_params = dict(strategy.get_params() or {})
        add(base_params, None)
        target_budget = max(int(self.family_probe_trials), len(specs), 1)
        param_grid = dict(strategy.get_param_grid() or {})
        param_names = list(param_grid.keys())
        choices_by_name = [list(param_grid[name] or []) for name in param_names]
        if not param_names or any(not choices for choices in choices_by_name):
            return specs
        total_space = int(math.prod(len(choices) for choices in choices_by_name))
        if total_space <= 0:
            return specs
        seed = _stable_seed(
            symbol,
            timeframe,
            regime,
            strategy.name,
            "family_probe_b0",
            total_space,
        )
        start = int(seed % total_space)
        step = int((seed // max(total_space, 1)) % total_space) or 1
        while math.gcd(step, total_space) != 1:
            step = (step + 1) % total_space or 1
        for offset in range(total_space):
            if len(specs) >= target_budget:
                break
            encoded = int((start + offset * step) % total_space)
            decoded = dict(base_params)
            remainder = encoded
            for name, choices in reversed(list(zip(param_names, choices_by_name))):
                decoded[name] = choices[remainder % len(choices)]
                remainder //= len(choices)
            add(decoded, None)
        return specs

    def _collect_family_probe_candidates(
        self,
        *,
        symbol: str,
        timeframe: str,
        train_features: pd.DataFrame,
        strategies: List[BaseStrategy],
        train_warmup_bars: int = 0,
        allowed_regimes: Optional[Sequence[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Freeze the train-best B0 candidate independently for every cell."""
        candidates: List[Dict[str, Any]] = []
        cell_records: List[Dict[str, Any]] = []
        # seq1028: receipt-unobserved regimes consume zero probes/attempts.
        probe_regimes = tuple(
            r for r in self.REGIMES
            if allowed_regimes is None or r in set(allowed_regimes)
        )
        for base_strategy in strategies:
            for regime in probe_regimes:
                try:
                    specs = self._family_probe_param_specs(
                        base_strategy,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                    )
                except Exception as exc:
                    if isinstance(exc, SearchInfrastructureError):
                        raise
                    outcome = _classify_staged_search_exception(
                        exc,
                        stage=SearchStage.PARAMETER_VALIDATION,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(getattr(base_strategy, "name", "")),
                        parameter_hash="",
                    )
                    raise SearchInfrastructureError(outcome) from exc
                best_score = float("-inf")
                best_effective_trades = float("-inf")
                best_candidate: Optional[Dict[str, Any]] = None
                best_parameter_hash = ""
                completed_trials = 0
                eligible_train_trials = 0
                trial_outcomes: List[TrialOutcome] = []
                for params, atr_anchor in specs:
                    current_stage = SearchStage.PARAMETER_VALIDATION
                    parameter_hash = ""
                    try:
                        parameter_hash = _stable_hash(
                            self._clean_candidate_params(params)
                        )
                        current_stage = SearchStage.STRATEGY_CONSTRUCTION
                        probe_strategy = StrategyRegistry.get(base_strategy.name, **params)
                        current_stage = SearchStage.TRAIN_BACKTEST
                        metrics, ran = self._run_one_exact_regime(
                            symbol=symbol,
                            timeframe=timeframe,
                            features=train_features,
                            strategy=probe_strategy,
                            regime=regime,
                            warmup_bars=train_warmup_bars,
                            run_zero_signal_path=True,
                            partition="optimizer_train",
                            capture_trade_power_inputs=True,
                        )
                        completed_trials += int(ran)
                        current_stage = SearchStage.TRAIN_EVIDENCE
                        raw_result = self._pop_trade_power_raw_result(metrics)
                        power_decision = self._calibrate_train_power_result(
                            raw_result,
                            effective_trials=len(specs),
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=base_strategy.name,
                            parameter_hash=parameter_hash,
                        )
                        power_outcome = self._power_trial_outcome(
                            power_decision,
                            stage=SearchStage.TRAIN_EVIDENCE,
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=base_strategy.name,
                            parameter_hash=parameter_hash,
                        )
                        if not power_decision.passed:
                            trial_outcomes.append(power_outcome)
                            continue
                        metrics["effective_trades"] = float(
                            power_decision.observed_effective_trades
                        )
                        current_stage = SearchStage.TRAIN_SCORE
                        economic_reason = self._train_trial_economic_ineligibility(
                            metrics
                        )
                        if economic_reason:
                            trial_outcomes.append(TrialOutcome(
                                kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
                                stage=SearchStage.TRAIN_SCORE,
                                reason_code=economic_reason,
                                reason="trial failed a predeclared train economic gate",
                                symbol=symbol,
                                timeframe=timeframe,
                                regime=regime,
                                strategy_family=base_strategy.name,
                                parameter_hash=parameter_hash,
                            ))
                            continue
                        trial_outcomes.append(power_outcome)
                        score = float(self._compute_train_regime_score(metrics, {}))
                        if not math.isfinite(score):
                            raise SearchInfrastructureError(TrialOutcome(
                                kind=SearchOutcomeKind.INFRA_FAILURE,
                                stage=SearchStage.TRAIN_SCORE,
                                reason_code="NONFINITE_TRAIN_SCORE",
                                reason="train score is non-finite",
                                symbol=symbol,
                                timeframe=timeframe,
                                regime=regime,
                                strategy_family=base_strategy.name,
                                parameter_hash=parameter_hash,
                            ))
                        eligible_train_trials += 1
                        effective_trades = float(
                            power_decision.observed_effective_trades
                        )
                        if (
                            score > best_score
                            or (
                                score == best_score
                                and (
                                    effective_trades > best_effective_trades
                                    or (
                                        effective_trades == best_effective_trades
                                        and (
                                            not best_parameter_hash
                                            or parameter_hash < best_parameter_hash
                                        )
                                    )
                                )
                            )
                        ):
                            best_score = score
                            best_effective_trades = effective_trades
                            best_parameter_hash = parameter_hash
                            clean_params = self._clean_candidate_params(
                                probe_strategy.get_params()
                            )
                            best_candidate = {
                                "strategy": probe_strategy,
                                "strategy_name": probe_strategy.name,
                                "params": clean_params,
                                "timeframe": str(timeframe).upper(),
                                "candidate_id": self._exact_cell_candidate_id(
                                    symbol,
                                    timeframe,
                                    regime,
                                    {
                                        "strategy_name": probe_strategy.name,
                                        "params": clean_params,
                                    },
                                ),
                                "train_result": dict(metrics),
                                "train_regime_metrics": {regime: dict(metrics)},
                                "eligibility_result": dict(metrics),
                                "eligibility_regime_metrics": {regime: dict(metrics)},
                                "val_regime_metrics": {regime: {}},
                                "is_tuned": True,
                                "is_family_probe": True,
                                "tuned_for_regime": str(regime),
                                "evaluation_regime": str(regime),
                                "screening_atr_anchor_mult": atr_anchor,
                                "search_trials": int(len(specs)),
                                "search_completed_trials": int(completed_trials),
                                "eligible_train_trials": int(eligible_train_trials),
                                "search_cell_count": 1,
                                "search_phase": "family_probe_b0",
                                "search_contract": "independent_regime_family_train_only_v1",
                                "regime_gated": True,
                                "family_probe_budget": int(self.family_probe_trials),
                                "trade_power_contract_enforced": True,
                                "trade_power_train": power_decision.to_dict(),
                                "_trade_power_requirement": (
                                    power_decision.frozen_requirement
                                ),
                            }
                    except Exception as exc:
                        if isinstance(exc, SearchInfrastructureError):
                            raise
                        outcome = _classify_staged_search_exception(
                            exc,
                            stage=current_stage,
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=base_strategy.name,
                            parameter_hash=parameter_hash,
                        )
                        trial_outcomes.append(outcome)
                        if outcome.kind is SearchOutcomeKind.INVALID_PARAM:
                            continue
                        raise SearchInfrastructureError(outcome) from exc
                family_outcome = aggregate_family_cell_outcomes(
                    trial_outcomes,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=base_strategy.name,
                )
                if family_outcome.kind is SearchOutcomeKind.INFRA_FAILURE:
                    raise SearchInfrastructureError(family_outcome)
                if (
                    family_outcome.kind is SearchOutcomeKind.VALID
                    and best_candidate is None
                ):
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code="VALID_OUTCOME_WITHOUT_CANDIDATE",
                        reason="valid family outcome has no frozen candidate",
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=base_strategy.name,
                    ))
                if best_candidate is not None:
                    best_candidate["search_completed_trials"] = int(completed_trials)
                    best_candidate["eligible_train_trials"] = int(eligible_train_trials)
                    candidates.append(best_candidate)
                cell_records.append({
                    "phase": "family_probe_b0",
                    "strategy_family": str(base_strategy.name),
                    "regime": str(regime),
                    "search_trials": int(len(specs)),
                    "completed_trials": int(completed_trials),
                    "eligible_train_trials": int(eligible_train_trials),
                    "frozen_candidate": bool(best_candidate is not None),
                    "tuning_validation_evaluations": 0,
                    "best_train_score": (
                        float(best_score) if math.isfinite(best_score) else None
                    ),
                    "family_outcome": family_outcome.to_dict(),
                })
        return candidates, cell_records

    def _attach_frozen_probe_validation(
        self,
        *,
        symbol: str,
        timeframe: str,
        val_features: Optional[pd.DataFrame],
        val_warmup_bars: int,
        candidates: List[Dict[str, Any]],
        cell_records: List[Dict[str, Any]],
    ) -> None:
        """Evaluate each train-frozen B0 cell once on tuning validation."""
        records = {
            (str(record.get("strategy_family")), str(record.get("regime"))): record
            for record in cell_records
        }
        for candidate in candidates:
            regime = str(candidate.get("evaluation_regime", ""))
            family = str(candidate.get("strategy_name", ""))
            evaluations = 0
            error: Optional[str] = None
            parameter_hash = _stable_hash(
                self._clean_candidate_params(candidate.get("params", {}))
            )
            if (
                val_features is None
                or max(0, len(val_features) - int(val_warmup_bars or 0)) < 50
            ):
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FROZEN_VALIDATION,
                    reason_code="TUNING_VALIDATION_SURFACE_UNAVAILABLE",
                    reason="frozen tuning-validation surface is missing or too short",
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=family,
                    parameter_hash=parameter_hash,
                ))
            requirement = candidate.get("_trade_power_requirement")
            if not isinstance(requirement, TradePowerRequirement):
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FROZEN_VALIDATION,
                    reason_code="FROZEN_TRADE_POWER_REQUIREMENT_MISSING",
                    reason="train-frozen trade-power requirement is missing",
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=family,
                    parameter_hash=parameter_hash,
                ))
            evaluations = 1
            try:
                frozen_strategy = StrategyRegistry.get(
                    family, **dict(candidate.get("params", {}) or {})
                )
                candidate["strategy"] = frozen_strategy
                metrics, _ = self._run_one_exact_regime(
                    symbol=symbol,
                    timeframe=timeframe,
                    features=val_features,
                    strategy=frozen_strategy,
                    regime=regime,
                    warmup_bars=val_warmup_bars,
                    run_zero_signal_path=True,
                    partition="tuning_validation",
                    capture_trade_power_inputs=True,
                )
                raw_result = self._pop_trade_power_raw_result(metrics)
                candidate["_baseline_gated_signals_sha256"] = str(
                    raw_result.get("_baseline_gated_signals_sha256", "") or ""
                )
                power_decision = self._evaluate_partition_power_result(
                    raw_result,
                    frozen_requirement=requirement,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=family,
                    parameter_hash=parameter_hash,
                )
            except Exception as exc:
                if isinstance(exc, SearchInfrastructureError):
                    raise
                outcome = classify_search_exception(
                    exc,
                    stage=SearchStage.FROZEN_VALIDATION,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=family,
                    parameter_hash=parameter_hash,
                )
                if outcome.kind is SearchOutcomeKind.INVALID_PARAM:
                    outcome = TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FROZEN_VALIDATION,
                        reason_code="FROZEN_PARAMETER_RECONSTRUCTION_DRIFT",
                        reason=outcome.reason,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=family,
                        parameter_hash=parameter_hash,
                        exception_type=outcome.exception_type,
                    )
                raise SearchInfrastructureError(outcome) from exc
            candidate["trade_power_validation"] = power_decision.to_dict()
            metrics["effective_trades"] = float(
                power_decision.observed_effective_trades
            )
            candidate.pop("_trade_power_requirement", None)
            candidate["val_regime_metrics"] = {regime: metrics}
            if bool(getattr(
                getattr(self, "config", None),
                "oos_evidence_export_enabled",
                False,
            )):
                oos_record = self._build_oos_evidence_record(
                    symbol,
                    timeframe,
                    candidate,
                    raw_result,
                    val_features,
                    val_warmup_bars,
                )
                if oos_record is None:
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FROZEN_VALIDATION,
                        reason_code="FROZEN_VALIDATION_REPLAY_EVIDENCE_UNAVAILABLE",
                        reason="single tuning-validation replay produced no OOS evidence",
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=family,
                        parameter_hash=parameter_hash,
                    ))
                candidate["oos_evidence"] = oos_record
            candidate["tuning_validation_evaluations"] = int(evaluations)
            candidate["tuning_validation_evaluation_error"] = error
            record = records.get((family, regime))
            if record is not None:
                record["tuning_validation_evaluations"] = int(evaluations)
                record["tuning_validation_evaluation_error"] = error
                record["trade_power_validation_status"] = power_decision.status
                record["trade_power_validation_kind"] = (
                    power_decision.outcome_kind.value
                )
                record["trade_power_validation_reason_code"] = (
                    str(power_decision.reason_codes[0])
                    if power_decision.reason_codes
                    else ("POWER_PASS" if power_decision.passed else power_decision.status)
                )

    @staticmethod
    def _attach_local_search_multiplicity(
        candidates: List[Dict[str, Any]],
        *,
        probe_cells: List[Dict[str, Any]],
        tuning_summaries: List[Dict[str, Any]],
        roster_count: int,
    ) -> None:
        """Attach exact attempted cell/trial totals used by DSR."""
        probe_trials = int(sum(
            int(cell.get("search_trials", 0) or 0) for cell in probe_cells
        ))
        tuning_trials = int(sum(
            int(summary.get("search_trials_total", 0) or 0)
            for summary in tuning_summaries
        ))
        probe_cell_count = int(len(probe_cells))
        tuning_cell_count = int(sum(
            int(summary.get("cell_count", 0) or 0)
            for summary in tuning_summaries
        ))
        validation_evaluations = int(
            sum(int(cell.get("tuning_validation_evaluations", 0) or 0) for cell in probe_cells)
            + sum(
                int(summary.get("tuning_validation_evaluations", 0) or 0)
                for summary in tuning_summaries
            )
        )
        for candidate in candidates:
            candidate["strategy_roster_count"] = int(roster_count)
            candidate["family_probe_search_trials_total"] = probe_trials
            candidate["family_probe_search_cells_total"] = probe_cell_count
            candidate["tuning_search_trials_total"] = tuning_trials
            candidate["tuning_search_cells_total"] = tuning_cell_count
            candidate["local_search_trials_total"] = int(probe_trials + tuning_trials)
            candidate["local_search_cells_total"] = int(probe_cell_count + tuning_cell_count)
            candidate["local_search_trial_count_exact"] = True
            candidate["multiplicity_scope"] = "symbol_timeframe_all_regime_cells"
            candidate["frozen_tuning_validation_evaluations_total"] = validation_evaluations

    @staticmethod
    def _representative_exact_regime_result(
        metrics_by_regime: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Compatibility summary chosen only from direct regime-gated metrics."""
        populated = [
            (regime, dict(metrics or {}))
            for regime, metrics in (metrics_by_regime or {}).items()
            if int((metrics or {}).get("total_trades", 0) or 0) > 0
        ]
        if not populated:
            return {"total_trades": 0, "regime_gated": True}
        regime, metrics = max(
            populated,
            key=lambda item: (
                int(item[1].get("total_trades", 0) or 0),
                float(item[1].get("total_return_pct", -1e12) or -1e12),
                float(item[1].get("profit_factor", 0.0) or 0.0),
                -float(item[1].get("max_drawdown_pct", 100.0) or 100.0),
                str(item[0]),
            ),
        )
        metrics["representative_regime"] = str(regime)
        metrics["regime_gated"] = True
        return metrics

    def _collect_candidates(self,
                            symbol: str,
                            timeframe: str,
                            train_features: pd.DataFrame,
                            val_features: Optional[pd.DataFrame],
                            strategies: List[BaseStrategy],
                            val_warmup_bars: int = 0,
                            train_warmup_bars: int = 0,
                            eligibility_features: Optional[pd.DataFrame] = None,
                            selection_features: Optional[pd.DataFrame] = None,
                            selection_warmup_bars: int = 0,
                            selection_surface: str = "validation",
                            historical_features: Optional[pd.DataFrame] = None,
                            allowed_regimes: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
        """
        Collect all strategy candidates with per-regime metrics.
        
        Phase B0: give every family-regime cell the same bounded train-only probe.
        Phase B1: run independent deeper train-only studies for B0 top-K cells.
        
        Each frozen train winner is evaluated once on tuning validation only
        after its parameters are fixed. Returns exact-regime candidate mappings.
        """
        # ``eligibility_features`` remains accepted for API compatibility, but
        # no family gate or shortlist may score rows outside optimizer train.
        # Authenticated prefix rows may initialize causal strategy state only.
        _ = eligibility_features
        screening_candidates, probe_cells = self._collect_family_probe_candidates(
            symbol=symbol,
            timeframe=timeframe,
            train_features=train_features,
            strategies=strategies,
            train_warmup_bars=train_warmup_bars,
            allowed_regimes=allowed_regimes,
        )
        self._last_family_probe_cells = [dict(cell) for cell in probe_cells]
        self._last_search_cell_outcomes = [dict(cell) for cell in probe_cells]

        # ===== NEW: Apply training eligibility gates =====
        # Filter out strategies that are "obviously bad" on training data
        # This saves compute by avoiding hyperparameter tuning on losing strategies
        eligible_candidates = self._apply_training_eligibility_gates(
            screening_candidates, symbol, timeframe
        )

        if not eligible_candidates:
            logger.warning(
                f"[{symbol}] [{timeframe}] No strategies passed training eligibility gates. "
                f"All {len(screening_candidates)} candidates rejected. Skipping optimization."
            )
            return []  # Return empty list - no candidates to optimize

        if not self.enable_hyperparam_tuning:
            self._attach_frozen_probe_validation(
                symbol=symbol,
                timeframe=timeframe,
                val_features=val_features,
                val_warmup_bars=val_warmup_bars,
                candidates=eligible_candidates,
                cell_records=probe_cells,
            )
            self._attach_local_search_multiplicity(
                eligible_candidates,
                probe_cells=probe_cells,
                tuning_summaries=[],
                roster_count=len(strategies),
            )
            self._attach_oos_evidence_to_candidates(
                symbol,
                timeframe,
                val_features,
                val_warmup_bars,
                eligible_candidates,
            )
            if self._uses_threadspark_selector():
                self._attach_threadspark_selection_evidence_to_candidates(
                    symbol,
                    timeframe,
                    selection_features,
                    selection_warmup_bars,
                    historical_features,
                    eligible_candidates,
                    selection_surface=selection_surface,
                )
            return eligible_candidates

        # Phase 2: Hyperparameter tuning for top-K strategies per regime
        # Identify top-K strategies for each regime based on screening scores
        # Use ELIGIBLE candidates (post-gates) instead of all screening candidates
        regime_top_strategies = self._identify_top_strategies_per_regime(
            eligible_candidates, top_k=self.hyperparam_top_k
        )
        
        # Preserve the exact shortlisted family-regime cells. Taking only the
        # family union would silently tune a shortlisted family in regimes where
        # it did not earn a train-only top-K place.
        strategy_regimes_to_tune: Dict[str, set] = {}
        for regime, strategy_names in regime_top_strategies.items():
            for strategy_name in strategy_names:
                strategy_regimes_to_tune.setdefault(strategy_name, set()).add(regime)
        
        if not strategy_regimes_to_tune:
            logger.debug(f"[{symbol}] [{timeframe}] No strategies eligible for hyperparameter tuning")
            self._attach_frozen_probe_validation(
                symbol=symbol,
                timeframe=timeframe,
                val_features=val_features,
                val_warmup_bars=val_warmup_bars,
                candidates=eligible_candidates,
                cell_records=probe_cells,
            )
            self._attach_local_search_multiplicity(
                eligible_candidates,
                probe_cells=probe_cells,
                tuning_summaries=[],
                roster_count=len(strategies),
            )
            self._attach_oos_evidence_to_candidates(
                symbol,
                timeframe,
                val_features,
                val_warmup_bars,
                eligible_candidates,
            )
            if self._uses_threadspark_selector():
                self._attach_threadspark_selection_evidence_to_candidates(
                    symbol,
                    timeframe,
                    selection_features,
                    selection_warmup_bars,
                    historical_features,
                    eligible_candidates,
                    selection_surface=selection_surface,
                )
            return eligible_candidates

        logger.info(
            f"[{symbol}] [{timeframe}] Hyperparameter tuning "
            f"{len(strategy_regimes_to_tune)} families across "
            f"{sum(len(values) for values in strategy_regimes_to_tune.values())} independent cells"
        )

        # Run hyperparameter tuning for each strategy
        tuned_candidates = []
        tuning_summaries: List[Dict[str, Any]] = []
        deep_replaced_cells: set[Tuple[str, str]] = set()
        probe_trials_by_cell = {
            (
                str(cell.get("strategy_family", "")),
                str(cell.get("regime", "")),
            ): max(0, int(cell.get("search_trials", 0) or 0))
            for cell in probe_cells
        }
        for strategy_name in sorted(strategy_regimes_to_tune):
            target_regimes = [
                regime for regime in self.REGIMES
                if regime in strategy_regimes_to_tune[strategy_name]
            ]
            # Find the original strategy from eligible candidates (post-gates)
            original_cand = next((c for c in eligible_candidates if c['strategy_name'] == strategy_name), None)
            if original_cand is None:
                continue

            # Get param grid
            param_grid = original_cand['strategy'].get_param_grid()
            if not param_grid:
                # No params to tune, keep original
                continue

            prior_trials = {
                regime: probe_trials_by_cell.get((strategy_name, regime), 0)
                for regime in target_regimes
            }
            deep_replaced_cells.update(
                (strategy_name, regime) for regime in target_regimes
            )

            # Run grid search
            tuned_results = self._tune_strategy_params(
                symbol, timeframe, strategy_name, param_grid,
                train_features, val_features,
                val_warmup_bars=val_warmup_bars,
                train_warmup_bars=train_warmup_bars,
                regimes=target_regimes,
                prior_effective_trials_by_regime=prior_trials,
            )

            tuned_candidates.extend(tuned_results)
            if self._last_tuning_search_meta:
                tuning_summaries.append(dict(self._last_tuning_search_meta))

        # A deeper cell search supersedes its B0 probe hypothesis. Keeping both
        # would admit the same model under two multiplicity requirements and can
        # let the weaker B0 contract survive. Non-deep B0 cells are validated
        # only now; every retained exact identity therefore touches validation
        # exactly once.
        retained_probe_candidates = [
            candidate
            for candidate in eligible_candidates
            if (
                str(candidate.get("strategy_name", "")),
                self._candidate_evaluation_regime(candidate),
            ) not in deep_replaced_cells
        ]
        self._attach_frozen_probe_validation(
            symbol=symbol,
            timeframe=timeframe,
            val_features=val_features,
            val_warmup_bars=val_warmup_bars,
            candidates=retained_probe_candidates,
            cell_records=probe_cells,
        )
        all_candidates = retained_probe_candidates + tuned_candidates
        seen_candidate_ids: set[str] = set()
        for candidate in all_candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            if not candidate_id or candidate_id in seen_candidate_ids:
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="DUPLICATE_OR_MISSING_EXACT_CANDIDATE_ID",
                    reason="post-search candidate hypotheses are not uniquely identified",
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=self._candidate_evaluation_regime(candidate),
                    strategy_family=str(candidate.get("strategy_name", "")),
                ))
            seen_candidate_ids.add(candidate_id)
        deep_cells = [
            dict(cell)
            for summary in tuning_summaries
            for cell in (summary.get("cells") or [])
            if isinstance(cell, Mapping)
        ]
        self._last_search_cell_outcomes = [
            *[dict(cell) for cell in probe_cells],
            *deep_cells,
        ]
        self._attach_local_search_multiplicity(
            all_candidates,
            probe_cells=probe_cells,
            tuning_summaries=tuning_summaries,
            roster_count=len(strategies),
        )
        self._attach_oos_evidence_to_candidates(
            symbol,
            timeframe,
            val_features,
            val_warmup_bars,
            all_candidates,
        )
        if self._uses_threadspark_selector():
            self._attach_threadspark_selection_evidence_to_candidates(
                symbol,
                timeframe,
                selection_features,
                selection_warmup_bars,
                historical_features,
                all_candidates,
                selection_surface=selection_surface,
            )

        return all_candidates

    def _uses_threadspark_selector(self) -> bool:
        """Return whether candidate ranking consumes ThreadSpark evidence.

        Building ThreadSpark evidence replays every candidate on an additional
        selection surface and optional historical chunks.  Those computations
        have no decision effect for the other scoring models, so production
        must not pay their runtime and memory cost unless ThreadSpark is the
        configured selector.  The research model remains fully supported.
        """
        scoring_model = str(
            getattr(self.config, "regime_scoring_model", "legacy") or "legacy"
        ).strip().lower()
        return scoring_model == "specialist_spokes_threadspark"
    
    def _identify_top_strategies_per_regime(self,
                                            candidates: List[Dict[str, Any]],
                                            top_k: int = 3) -> Dict[str, List[str]]:
        """
        Identify top-K strategies for each regime based on screening scores.
        
        Returns dict mapping regime -> list of strategy names to tune.
        """
        regime_scores: Dict[str, Dict[str, float]] = {r: {} for r in self.REGIMES}
        screening_trade_floor = max(1, int(self.min_train_trades or 0) // 2)
        
        for cand in candidates:
            for regime in self.REGIMES:
                train_metrics = cand.get('train_regime_metrics', {}).get(regime, {})
                if not train_metrics:
                    continue
                
                train_trades = train_metrics.get('total_trades', 0)
                
                # Production candidates already passed the dependence-aware
                # train ESS gate.  The raw floor remains compatibility-only
                # for injected/legacy research candidates.
                if (
                    not bool(cand.get("trade_power_contract_enforced", False))
                    and train_trades < screening_trade_floor
                ):
                    continue
                
                score = self._compute_train_regime_score(train_metrics, {})
                strategy_name = cand['strategy_name']
                previous_score = regime_scores[regime].get(strategy_name)
                if previous_score is None or score > previous_score:
                    regime_scores[regime][strategy_name] = score
        
        # Select top-K for each regime
        result = {}
        for regime, scores in regime_scores.items():
            if scores:
                ranked = sorted(
                    scores.items(), key=lambda item: (-float(item[1]), str(item[0]))
                )
                result[regime] = [name for name, _ in ranked[:top_k]]
            else:
                result[regime] = []
        
        return result
    
    def _tune_strategy_params(self,
                              symbol: str,
                              timeframe: str,
                              strategy_name: str,
                              param_grid: Dict[str, List],
                              train_features: pd.DataFrame,
                              val_features: Optional[pd.DataFrame],
                              val_warmup_bars: int = 0,
                              train_warmup_bars: int = 0,
                              regimes: Optional[List[str]] = None,
                              prior_effective_trials_by_regime: Optional[Mapping[str, int]] = None) -> List[Dict[str, Any]]:
        """
        Tune hyperparameters for a strategy using Optuna TPE.
        
        Uses multi-regime optimization to find best params for each regime
        in a single optimization run.
        
        Returns list of tuned candidate dicts with regime metrics.
        """
        target_regimes = [
            regime for regime in self.REGIMES
            if regime in set(regimes or self.REGIMES)
        ]
        self._last_tuning_search_meta = {}
        if not target_regimes:
            return []

        # Use Optuna if available
        if self.optuna_optimizer is not None:
            regime_results = self.optuna_optimizer.optimize_for_regimes(
                symbol=symbol,
                timeframe=timeframe,
                strategy_name=strategy_name,
                param_grid=param_grid,
                train_features=train_features,
                val_features=val_features,
                regimes=target_regimes,
                bucket_trades_fn=self._bucket_trades_by_regime,
                compute_score_fn=self._compute_train_regime_score,
                min_train_trades=self.min_train_trades,
                max_drawdown_pct=self.val_max_drawdown,  # Pass DD threshold for early rejection
                train_warmup_bars=train_warmup_bars,
                val_warmup_bars=val_warmup_bars,
                calibrate_train_power_fn=self._calibrate_train_power_result,
                evaluate_validation_power_fn=self._evaluate_partition_power_result,
                train_economic_gate_fn=self._train_trial_economic_ineligibility,
                prior_effective_trials_by_regime=prior_effective_trials_by_regime,
            )
            
            # Convert to candidate list format
            tuned_candidates = []
            raw_search_meta = getattr(
                self.optuna_optimizer, "last_regime_search_metadata", {}
            )
            if not isinstance(regime_results, Mapping) or not isinstance(
                raw_search_meta, Mapping
            ):
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="OPTUNA_RESULT_CONTAINER_INVALID",
                    reason="Optuna search result or metadata container is malformed",
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy_family=strategy_name,
                ))
            self._last_tuning_search_meta = dict(raw_search_meta)
            for regime, candidate in regime_results.items():
                current_stage = SearchStage.TRAIN_EVIDENCE
                parameter_hash = ""
                try:
                    parameter_hash = _stable_hash(
                        self._clean_candidate_params(candidate.get("params", {}))
                    )
                    candidate['tuned_for_regime'] = regime
                    raw_train = dict(
                        (candidate.get("train_regime_metrics") or {}).get(regime, {})
                        or candidate.get("train_result")
                        or {}
                    )
                    compact_train = self._compact_exact_regime_result(raw_train, regime)
                    compact_train.update(
                        self._build_daily_net_mtm_surface(
                            result=raw_train,
                            features=train_features,
                            timeframe=timeframe,
                            warmup_bars=train_warmup_bars,
                            partition="optimizer_train",
                        )
                    )
                    current_stage = SearchStage.FROZEN_VALIDATION
                    raw_val = dict(
                        (candidate.get("val_regime_metrics") or {}).get(regime, {}) or {}
                    )
                    compact_val = self._compact_exact_regime_result(raw_val, regime)
                    compact_val.update(
                        self._build_daily_net_mtm_surface(
                            result=raw_val,
                            features=val_features,
                            timeframe=timeframe,
                            warmup_bars=val_warmup_bars,
                            partition="tuning_validation",
                        )
                        if val_features is not None
                        else {
                            "daily_net_mtm_returns": [],
                            "daily_net_mtm_dates": [],
                            "daily_net_mtm_observations": 0,
                            "daily_net_mtm_contract": {
                                "contract_version": DAILY_NET_MTM_CONTRACT_VERSION,
                                "status": "UNAVAILABLE",
                                "origin": "backtester_final_post_cost_equity_curve",
                                "partition": "tuning_validation",
                                "reason": "validation features unavailable",
                            },
                        }
                    )
                    current_stage = SearchStage.FAMILY_CELL
                    candidate["train_result"] = dict(compact_train)
                    candidate["train_regime_metrics"] = {regime: compact_train}
                    candidate["val_regime_metrics"] = {regime: compact_val}
                    candidate["timeframe"] = str(timeframe).upper()
                    candidate["candidate_id"] = self._exact_cell_candidate_id(
                        symbol,
                        timeframe,
                        regime,
                        candidate,
                    )
                    candidate['family_search_metadata'] = dict(
                        self._last_tuning_search_meta
                    )
                    current_stage = SearchStage.FROZEN_VALIDATION
                    if bool(getattr(
                        getattr(self, "config", None),
                        "oos_evidence_export_enabled",
                        False,
                    )):
                        if val_features is None:
                            raise SearchInfrastructureError(TrialOutcome(
                                kind=SearchOutcomeKind.INFRA_FAILURE,
                                stage=SearchStage.FROZEN_VALIDATION,
                                reason_code="TUNING_VALIDATION_SURFACE_UNAVAILABLE",
                                reason="Optuna frozen validation has no feature surface",
                                symbol=symbol,
                                timeframe=timeframe,
                                regime=regime,
                                strategy_family=strategy_name,
                            ))
                        oos_record = self._build_oos_evidence_record(
                            symbol,
                            timeframe,
                            candidate,
                            raw_val,
                            val_features,
                            val_warmup_bars,
                        )
                        if oos_record is None:
                            raise SearchInfrastructureError(TrialOutcome(
                                kind=SearchOutcomeKind.INFRA_FAILURE,
                                stage=SearchStage.FROZEN_VALIDATION,
                                reason_code="FROZEN_VALIDATION_REPLAY_EVIDENCE_UNAVAILABLE",
                                reason="Optuna single validation replay produced no OOS evidence",
                                symbol=symbol,
                                timeframe=timeframe,
                                regime=regime,
                                strategy_family=strategy_name,
                            ))
                        candidate["oos_evidence"] = oos_record
                    tuned_candidates.append(candidate)
                except Exception as exc:
                    if isinstance(exc, SearchInfrastructureError):
                        raise
                    outcome = _classify_staged_search_exception(
                        exc,
                        stage=current_stage,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=str(regime),
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    )
                    raise SearchInfrastructureError(outcome) from exc
            
            if tuned_candidates:
                logger.debug(f"[{symbol}] [{timeframe}] {strategy_name}: "
                           f"{len(tuned_candidates)} tuned variants (Optuna TPE)")
            
            return tuned_candidates
        
        # Fallback to grid search
        return self._tune_strategy_params_grid(
            symbol,
            timeframe,
            strategy_name,
            param_grid,
            train_features,
            val_features,
            val_warmup_bars=val_warmup_bars,
            train_warmup_bars=train_warmup_bars,
            regimes=target_regimes,
            prior_effective_trials_by_regime=prior_effective_trials_by_regime,
        )
    
    def _tune_strategy_params_grid(self,
                                   symbol: str,
                                   timeframe: str,
                                   strategy_name: str,
                                   param_grid: Dict[str, List],
                                   train_features: pd.DataFrame,
                                   val_features: Optional[pd.DataFrame],
                                   val_warmup_bars: int = 0,
                                   train_warmup_bars: int = 0,
                                   regimes: Optional[List[str]] = None,
                                   prior_effective_trials_by_regime: Optional[Mapping[str, int]] = None) -> List[Dict[str, Any]]:
        """
        Fallback grid search for hyperparameter tuning.
        
        Returns list of tuned candidate dicts with regime metrics.
        """
        # Generate only the bounded deterministic design; never materialize the
        # full Cartesian domain before applying the configured cap.
        try:
            param_names = list(param_grid.keys())
            param_values = [list(param_grid[name] or []) for name in param_names]
        except Exception as exc:
            outcome = _classify_staged_search_exception(
                exc,
                stage=SearchStage.PARAMETER_VALIDATION,
                symbol=symbol,
                timeframe=timeframe,
                regime="",
                strategy_family=strategy_name,
                parameter_hash="",
            )
            raise SearchInfrastructureError(outcome) from exc
        try:
            base_strategy = StrategyRegistry.get(strategy_name)
            default_params = base_strategy.get_params()
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=SearchStage.STRATEGY_CONSTRUCTION,
                symbol=symbol,
                timeframe=timeframe,
                regime="",
                strategy_family=strategy_name,
                parameter_hash="",
            )
            raise SearchInfrastructureError(outcome) from exc
        target_regimes = [
            regime for regime in self.REGIMES
            if regime in set(regimes or self.REGIMES)
        ]
        tuned_candidates: List[Dict[str, Any]] = []
        cell_records: List[Dict[str, Any]] = []
        for regime in target_regimes:
            seed = _stable_seed(
                symbol,
                timeframe,
                regime,
                strategy_name,
                "regime_grid_train_only",
                len(train_features),
            )
            combos, _total_space = _bounded_cartesian_combinations(
                param_values,
                limit=self.hyperparam_max_combos,
                seed=seed,
            )
            best_score = float("-inf")
            best_effective_trades = float("-inf")
            best_candidate: Optional[Dict[str, Any]] = None
            best_parameter_hash = ""
            completed_trials = 0
            eligible_train_trials = 0
            early_dd_rejections = 0
            trial_outcomes: List[TrialOutcome] = []
            for combo in combos:
                params = {**default_params, **dict(zip(param_names, combo))}
                current_stage = SearchStage.PARAMETER_VALIDATION
                parameter_hash = ""
                try:
                    parameter_hash = _stable_hash(
                        self._clean_candidate_params(params)
                    )
                    current_stage = SearchStage.STRATEGY_CONSTRUCTION
                    test_strategy = StrategyRegistry.get(strategy_name, **params)
                    current_stage = SearchStage.TRAIN_BACKTEST
                    train_m, ran = self._run_one_exact_regime(
                        symbol=symbol,
                        timeframe=timeframe,
                        features=train_features,
                        strategy=test_strategy,
                        regime=regime,
                        warmup_bars=train_warmup_bars,
                        run_zero_signal_path=True,
                        partition="optimizer_train",
                        capture_trade_power_inputs=True,
                    )
                    completed_trials += int(ran)
                    current_stage = SearchStage.TRAIN_EVIDENCE
                    raw_result = self._pop_trade_power_raw_result(train_m)
                    power_decision = self._calibrate_train_power_result(
                        raw_result,
                        effective_trials=(
                            len(combos)
                            + max(
                                0,
                                int((prior_effective_trials_by_regime or {}).get(
                                    regime, 0
                                ) or 0),
                            )
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    )
                    if not power_decision.passed:
                        trial_outcomes.append(self._power_trial_outcome(
                            power_decision,
                            stage=SearchStage.TRAIN_EVIDENCE,
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        ))
                        continue
                    train_m["effective_trades"] = float(
                        power_decision.observed_effective_trades
                    )
                    current_stage = SearchStage.TRAIN_SCORE
                    economic_reason = self._train_trial_economic_ineligibility(train_m)
                    if economic_reason:
                        if economic_reason == "TRAIN_DRAWDOWN_GATE":
                            early_dd_rejections += 1
                        trial_outcomes.append(TrialOutcome(
                            kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
                            stage=SearchStage.TRAIN_SCORE,
                            reason_code=economic_reason,
                            reason="trial failed a predeclared train economic gate",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        ))
                        continue
                    score = float(self._compute_train_regime_score(train_m, {}))
                    if not math.isfinite(score):
                        raise SearchInfrastructureError(TrialOutcome(
                            kind=SearchOutcomeKind.INFRA_FAILURE,
                            stage=SearchStage.TRAIN_SCORE,
                            reason_code="NONFINITE_TRAIN_SCORE",
                            reason="train score is non-finite",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        ))
                    trial_outcomes.append(self._power_trial_outcome(
                        power_decision,
                        stage=SearchStage.TRAIN_EVIDENCE,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    ))
                    eligible_train_trials += 1
                    effective_trades = float(
                        power_decision.observed_effective_trades
                    )
                    if (
                        score > best_score
                        or (
                            score == best_score
                            and (
                                effective_trades > best_effective_trades
                                or (
                                    effective_trades == best_effective_trades
                                    and (
                                        not best_parameter_hash
                                        or parameter_hash < best_parameter_hash
                                    )
                                )
                            )
                        )
                    ):
                        best_score = score
                        best_effective_trades = effective_trades
                        best_parameter_hash = parameter_hash
                        best_candidate = {
                            "strategy": test_strategy,
                            "strategy_name": strategy_name,
                            "params": self._clean_candidate_params(
                                test_strategy.get_params()
                            ),
                            "train_result": dict(train_m),
                            "train_regime_metrics": {regime: dict(train_m)},
                            "val_regime_metrics": {regime: {}},
                            "is_tuned": True,
                            "tuned_for_regime": str(regime),
                            "search_trials": int(len(combos)),
                            "search_completed_trials": int(completed_trials),
                            "eligible_train_trials": int(eligible_train_trials),
                            "search_cell_count": 1,
                            "search_phase": "deep_grid",
                            "search_contract": "independent_regime_family_train_only_v1",
                            "regime_gated": True,
                            "evaluation_regime": str(regime),
                            "parameter_stability_evidence": {
                                "status": "not_available",
                                "reason": "deterministic_grid_fallback",
                            },
                            "trade_power_contract_enforced": True,
                            "trade_power_train": power_decision.to_dict(),
                            "_trade_power_requirement": (
                                power_decision.frozen_requirement
                            ),
                        }
                        best_candidate["timeframe"] = str(timeframe).upper()
                        best_candidate["candidate_id"] = self._exact_cell_candidate_id(
                            symbol,
                            timeframe,
                            regime,
                            best_candidate,
                        )
                except Exception as exc:
                    if isinstance(exc, SearchInfrastructureError):
                        raise
                    outcome = _classify_staged_search_exception(
                        exc,
                        stage=current_stage,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    )
                    trial_outcomes.append(outcome)
                    if outcome.kind is SearchOutcomeKind.INVALID_PARAM:
                        continue
                    raise SearchInfrastructureError(outcome) from exc
            family_outcome = aggregate_family_cell_outcomes(
                trial_outcomes,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
            )
            if family_outcome.kind is SearchOutcomeKind.INFRA_FAILURE:
                raise SearchInfrastructureError(family_outcome)
            evaluations = 0
            evaluation_error: Optional[str] = None
            if best_candidate is not None:
                best_candidate["search_completed_trials"] = int(completed_trials)
                best_candidate["eligible_train_trials"] = int(eligible_train_trials)
                self._attach_frozen_probe_validation(
                    symbol=symbol,
                    timeframe=timeframe,
                    val_features=val_features,
                    val_warmup_bars=val_warmup_bars,
                    candidates=[best_candidate],
                    cell_records=[],
                )
                evaluations = int(
                    best_candidate.get("tuning_validation_evaluations", 0) or 0
                )
                tuned_candidates.append(best_candidate)
            cell_records.append({
                "phase": "deep_grid",
                "strategy_family": str(strategy_name),
                "regime": str(regime),
                "search_trials": int(len(combos)),
                "completed_trials": int(completed_trials),
                "eligible_train_trials": int(eligible_train_trials),
                "early_dd_rejections": int(early_dd_rejections),
                "frozen_candidate": bool(best_candidate is not None),
                "tuning_validation_evaluations": int(evaluations),
                "best_train_score": (
                    float(best_score) if math.isfinite(best_score) else None
                ),
                "family_outcome": family_outcome.to_dict(),
            })
        self._last_tuning_search_meta = {
            "contract": "independent_regime_family_train_only_v1",
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "strategy_family": str(strategy_name),
            "method": "deterministic_grid_fallback",
            "cell_count": int(len(cell_records)),
            "search_trials_total": int(sum(
                int(cell.get("search_trials", 0) or 0) for cell in cell_records
            )),
            "completed_trials_total": int(sum(
                int(cell.get("completed_trials", 0) or 0) for cell in cell_records
            )),
            "eligible_train_trials_total": int(sum(
                int(cell.get("eligible_train_trials", 0) or 0) for cell in cell_records
            )),
            "tuning_validation_evaluations": int(sum(
                int(cell.get("tuning_validation_evaluations", 0) or 0)
                for cell in cell_records
            )),
            "cells": [dict(cell) for cell in cell_records],
        }
        for candidate in tuned_candidates:
            candidate["family_search_metadata"] = dict(
                self._last_tuning_search_meta
            )
        if tuned_candidates:
            logger.debug(
                "[%s] [%s] %s: %d frozen variants from %d train-only grid cells",
                symbol,
                timeframe,
                strategy_name,
                len(tuned_candidates),
                len(cell_records),
            )
        return tuned_candidates
    
    def _bucket_trades_by_regime(self,
                                  trades: List[Dict],
                                  features: pd.DataFrame,
                                  timeframe: str = "") -> Dict[str, Dict[str, Any]]:
        """
        Bucket trades by REGIME_LIVE at the signal bar.
        
        Uses REGIME_LIVE which is the shifted regime (what the system knows at
        decision time). The signal bar is the correct attribution point because
        live PM chooses the regime winner before the next-bar execution fill.
        
        Returns dict mapping regime -> metrics for trades in that regime.
        """
        regime_buckets = {regime: [] for regime in self.REGIMES}
        
        for trade in trades:
            signal_bar = trade.get('signal_bar', trade.get('entry_bar', 0))
            try:
                signal_bar = int(signal_bar)
            except (TypeError, ValueError):
                continue
            
            # Get regime at signal time using REGIME_LIVE (decision-time parity).
            if 0 <= signal_bar < len(features) and 'REGIME_LIVE' in features.columns:
                regime = features['REGIME_LIVE'].iloc[signal_bar]
                
                if pd.notna(regime) and regime in regime_buckets:
                    regime_buckets[regime].append(trade)
        
        # Compute metrics for each bucket
        regime_metrics = {}
        for regime, bucket_trades in regime_buckets.items():
            if bucket_trades:
                regime_metrics[regime] = self._compute_bucket_metrics(
                    bucket_trades,
                    features=features,
                    timeframe=timeframe,
                )
            else:
                regime_metrics[regime] = {'total_trades': 0}
        
        return regime_metrics
    
    def _compute_bucket_metrics(self,
                                trades: List[Dict],
                                initial_capital: float = None,
                                features: Optional[pd.DataFrame] = None,
                                timeframe: str = "") -> Dict[str, Any]:
        """
        Compute metrics for a bucket of trades including proper drawdown.
        
        Args:
            trades: List of trade dicts with pnl_dollars, entry_bar, etc.
            initial_capital: Starting capital for return calculation (from config)
            
        Returns:
            Dict with comprehensive metrics including max_drawdown_pct
        """
        if not trades:
            return {
                'total_trades': 0,
                'max_drawdown_pct': 0.0,
                'total_return_pct': 0.0,
                'return_drawdown_ratio': 0.0,
                'calmar_ratio_annualized': 0.0,
                'annualized_return_pct': 0.0,
                'calmar_ratio_basis': "cumulative_return_pct_over_max_drawdown_pct",
            }
        
        if initial_capital is None:
            initial_capital = getattr(self.config, 'initial_capital', 10000.0)
        sorted_trades = sorted(trades, key=lambda t: t.get('entry_bar', t.get('signal_bar', 0)))

        # Bucket metrics historically supplied one equity observation per trade
        # to a routine that annualized those observations as timeframe bars. A
        # 16-trade H1 bucket was therefore treated like 16 hourly returns and
        # could report Sharpe values in the hundreds. Reconstruct a dated daily
        # realized-equity series instead so every timeframe has the same metric
        # clock. Exact-regime selection now uses direct gated backtests; this
        # reconstruction remains for diagnostics and legacy evidence only.
        metric_features = features
        metric_timeframe = "D1"
        daily_realized_returns: List[float] = []
        daily_return_dates: List[str] = []
        if features is not None and len(features) > 0 and isinstance(features.index, pd.DatetimeIndex):
            active_days = pd.DatetimeIndex(features.index.normalize().unique()).sort_values()
            pnl_by_day = pd.Series(0.0, index=active_days, dtype=float)
            for trade in sorted_trades:
                exit_time = trade.get("exit_time")
                if exit_time is None:
                    try:
                        exit_bar = int(trade.get("exit_bar", trade.get("entry_bar", 0)) or 0)
                        exit_bar = max(0, min(exit_bar, len(features) - 1))
                        exit_time = features.index[exit_bar]
                    except (TypeError, ValueError, IndexError):
                        exit_time = active_days[-1]
                exit_day = pd.Timestamp(exit_time).normalize()
                if exit_day not in pnl_by_day.index:
                    eligible_days = active_days[active_days <= exit_day]
                    exit_day = eligible_days[-1] if len(eligible_days) else active_days[0]
                pnl_by_day.loc[exit_day] += float(trade.get('pnl_dollars', 0.0) or 0.0)
            daily_equity = float(initial_capital) + pnl_by_day.cumsum()
            prior_equity = pd.Series(
                [float(initial_capital), *daily_equity.iloc[:-1].astype(float).tolist()],
                index=daily_equity.index,
                dtype=float,
            )
            daily_returns_series = (daily_equity.astype(float) - prior_equity) / prior_equity.replace(0.0, np.nan)
            daily_returns_series = daily_returns_series.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            daily_realized_returns = [float(value) for value in daily_returns_series.tolist()]
            daily_return_dates = [pd.Timestamp(value).isoformat() for value in daily_returns_series.index]
            equity_curve = [float(initial_capital), *daily_equity.astype(float).tolist()]
            metric_features = pd.DataFrame(index=pd.DatetimeIndex([
                active_days[0] - pd.Timedelta(days=1), *active_days.tolist()
            ]))
            equity = float(equity_curve[-1])
        else:
            equity = float(initial_capital)
            equity_curve = [equity]
            for trade in sorted_trades:
                equity += float(trade.get('pnl_dollars', 0.0) or 0.0)
                equity_curve.append(equity)

        peak = initial_capital
        max_drawdown_pct = 0.0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            if peak > 0:
                max_drawdown_pct = max(max_drawdown_pct, ((peak - eq) / peak) * 100.0)

        metrics = self.backtester._calculate_metrics(
            sorted_trades,
            equity,
            equity_curve,
            max_drawdown_pct,
            features=metric_features,
            timeframe=metric_timeframe,
        )
        return {
            'bars': int(len(features)) if features is not None else 0,
            'total_trades': metrics.get('total_trades', 0),
            'win_rate': metrics.get('win_rate', 0.0),
            'profit_factor': metrics.get('profit_factor', 0.0),
            'total_pnl': metrics.get('total_pnl', 0.0),
            'gross_profit': metrics.get('gross_profit', 0.0),
            'gross_loss': metrics.get('gross_loss', 0.0),
            'avg_win': metrics.get('avg_win_dollars', 0.0),
            'avg_loss': metrics.get('avg_loss_dollars', 0.0),
            'sharpe_approx': metrics.get('sharpe_ratio', 0.0),
            'sortino_approx': metrics.get('sortino_ratio', 0.0),
            'calmar_ratio': metrics.get('calmar_ratio', 0.0),
            'return_drawdown_ratio': metrics.get('return_drawdown_ratio', metrics.get('calmar_ratio', 0.0)),
            'calmar_ratio_annualized': metrics.get('calmar_ratio_annualized', 0.0),
            'annualized_return_pct': metrics.get('annualized_return_pct', 0.0),
            'calmar_ratio_basis': metrics.get('calmar_ratio_basis', "cumulative_return_pct_over_max_drawdown_pct"),
            'metric_contract_version': metrics.get('metric_contract_version'),
            'risk_return_frequency': 'daily_realized',
            'daily_realized_returns': daily_realized_returns,
            'daily_return_dates': daily_return_dates,
            'daily_return_observations': int(len(daily_realized_returns)),
            'std_weekly_return': metrics.get('std_weekly_return', 0.0),
            'r_count': metrics.get('r_count', 0),
            'mean_r': metrics.get('mean_r', 0.0),
            'median_r': metrics.get('median_r', 0.0),
            'std_r': metrics.get('std_r', 0.0),
            'se_r': metrics.get('se_r', 0.0),
            'edge_t_stat_r': metrics.get('edge_t_stat_r', 0.0),
            'pct_positive_r': metrics.get('pct_positive_r', 0.0),
            'worst_5pct_r': metrics.get('worst_5pct_r', 0.0),
            'r_downside_deviation': metrics.get('r_downside_deviation', 0.0),
            'r_block_downside_deviation': metrics.get('r_block_downside_deviation', 0.0),
            'r_block_cvar': metrics.get('r_block_cvar', 0.0),
            'max_consecutive_losses': metrics.get('max_consecutive_losses', 0),
            'max_drawdown_pct': metrics.get('max_drawdown_pct', 0.0),
            'total_return_pct': metrics.get('total_return_pct', 0.0),
        }

    @staticmethod
    def _build_purged_kfold_ranges(n_bars: int,
                                   scored_start: int = 0,
                                   *,
                                   splits: int = 5,
                                   embargo_bars: int = 1,
                                   warmup_bars: int = 0) -> List[Dict[str, Any]]:
        """Build half-open fold scoring windows with adjacent purge/embargo metadata."""
        try:
            n_bars = max(0, int(n_bars))
            scored_start = max(0, min(n_bars, int(scored_start or 0)))
            splits = max(2, int(splits or 5))
            embargo_bars = max(0, int(embargo_bars or 0))
            warmup_bars = max(0, int(warmup_bars or 0))
        except Exception:
            return []
        scored_count = n_bars - scored_start
        if scored_count < 2:
            return []
        fold_count = min(splits, scored_count)
        chunks = [chunk for chunk in np.array_split(np.arange(scored_start, n_bars), fold_count) if chunk.size]
        ranges: List[Dict[str, Any]] = []
        for idx, chunk in enumerate(chunks, start=1):
            test_start = int(chunk[0])
            test_end = int(chunk[-1]) + 1
            warmup_start = max(0, test_start - warmup_bars)
            purge_start = max(scored_start, test_start - embargo_bars)
            embargo_end = min(n_bars, test_end + embargo_bars)
            train_segments = []
            if scored_start < purge_start:
                train_segments.append({"start": int(scored_start), "end": int(purge_start)})
            if embargo_end < n_bars:
                train_segments.append({"start": int(embargo_end), "end": int(n_bars)})
            ranges.append({
                "fold": idx,
                "fold_count": len(chunks),
                "test_start": test_start,
                "test_end": test_end,
                "warmup_start": warmup_start,
                "warmup_bars": int(test_start - warmup_start),
                "purge_start": purge_start,
                "embargo_end": embargo_end,
                "purge_before_bars": int(test_start - purge_start),
                "embargo_after_bars": int(embargo_end - test_end),
                "train_segments": train_segments,
            })
        return ranges

    @staticmethod
    def _aggregate_purged_kfold_metrics(fold_metrics: List[Dict[str, Any]],
                                        *,
                                        initial_capital: float,
                                        min_trades: int,
                                        min_profit_factor: float,
                                        min_return_pct: float,
                                        aggregate_policy: str) -> Dict[str, Any]:
        """Aggregate independent OOS folds without pretending they form one equity path."""
        folds = list(fold_metrics or [])
        fold_count = len(folds)
        if fold_count <= 0:
            return {
                "validation_mode": "purged_kfold",
                "total_trades": 0,
                "fold_count": 0,
                "policy_status": "FAIL",
                "policy_reason": "no purged-kfold folds available",
            }

        def _f(metric: Dict[str, Any], key: str, default: float = 0.0) -> float:
            try:
                value = float(metric.get(key, default) or default)
            except Exception:
                return default
            return value if math.isfinite(value) else default

        def _fold_profit_factor(metric: Dict[str, Any]) -> float:
            gross_profit_value = max(0.0, _f(metric, "gross_profit"))
            gross_loss_value = abs(_f(metric, "gross_loss"))
            if gross_loss_value > 1e-12:
                return gross_profit_value / gross_loss_value
            if gross_profit_value > 0.0:
                return float("inf")
            return _f(metric, "profit_factor")

        total_trades = sum(int(metric.get("total_trades", 0) or 0) for metric in folds)
        gross_profit = sum(max(0.0, _f(metric, "gross_profit")) for metric in folds)
        gross_loss = sum(abs(_f(metric, "gross_loss")) for metric in folds)
        total_pnl = sum(_f(metric, "total_pnl") for metric in folds)
        max_drawdown = max((_f(metric, "max_drawdown_pct") for metric in folds), default=0.0)
        weighted_win_rate_n = sum(
            _f(metric, "win_rate") * int(metric.get("total_trades", 0) or 0)
            for metric in folds
        )
        win_rate = weighted_win_rate_n / total_trades if total_trades > 0 else 0.0
        if gross_loss > 1e-12:
            profit_factor = gross_profit / gross_loss
        elif gross_profit > 0.0:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0
        base_capital = max(float(initial_capital or 0.0), 1.0)
        total_return_pct = (total_pnl / base_capital) * 100.0

        traded_folds = [metric for metric in folds if int(metric.get("total_trades", 0) or 0) > 0]
        passing_folds = [
            metric for metric in traded_folds
            if (
                int(metric.get("total_trades", 0) or 0) >= int(min_trades or 0)
                and _fold_profit_factor(metric) >= float(min_profit_factor or 0.0)
                and _f(metric, "total_return_pct") >= float(min_return_pct or 0.0)
            )
        ]
        soft_passing_folds = [
            metric for metric in traded_folds
            if (
                _fold_profit_factor(metric) >= float(min_profit_factor or 0.0)
                and _f(metric, "total_return_pct") >= float(min_return_pct or 0.0)
            )
        ]
        worst_fold = min(
            folds,
            key=lambda metric: (
                _f(metric, "total_return_pct"),
                _fold_profit_factor(metric),
                -int(metric.get("total_trades", 0) or 0),
            ),
        )
        aggregate_pass = (
            total_trades >= int(min_trades or 0)
            and profit_factor >= float(min_profit_factor or 0.0)
            and total_return_pct >= float(min_return_pct or 0.0)
        )
        policy = str(aggregate_policy or "aggregate_with_worst_fold_floor").lower()
        if policy == "all_folds":
            policy_pass = (
                aggregate_pass
                and len(traded_folds) == fold_count
                and len(passing_folds) == fold_count
            )
            policy_reason = (
                "all folds meet trade/PF/return floors"
                if policy_pass else "not every fold meets trade/PF/return floors"
            )
        else:
            worst_floor_pass = len(soft_passing_folds) == len(traded_folds)
            policy_pass = aggregate_pass and worst_floor_pass
            policy_reason = (
                "aggregate OOS passes and every traded fold clears PF/return floors"
                if policy_pass else "aggregate OOS or traded-fold PF/return floor failed"
            )

        return {
            "validation_mode": "purged_kfold",
            "aggregate_policy": policy,
            "fold_count": int(fold_count),
            "folds_with_trades": int(len(traded_folds)),
            "folds_passing": int(len(passing_folds)),
            "fold_pass_rate": round(float(len(passing_folds) / fold_count), 6) if fold_count else 0.0,
            "fold_profitability_pass_rate": (
                round(float(len(soft_passing_folds) / len(traded_folds)), 6) if traded_folds else 0.0
            ),
            "total_trades": int(total_trades),
            "profit_factor": round(float(profit_factor), 6) if math.isfinite(profit_factor) else profit_factor,
            "total_return_pct": round(float(total_return_pct), 6),
            "max_drawdown_pct": round(float(max_drawdown), 6),
            "capital_preservation": round(max(0.0, 1.0 - (float(max_drawdown) / 100.0)), 6),
            "total_pnl": round(float(total_pnl), 6),
            "gross_profit": round(float(gross_profit), 6),
            "gross_loss": round(float(gross_loss), 6),
            "win_rate": round(float(win_rate), 6),
            "worst_fold": {
                "fold": int(worst_fold.get("fold", 0) or 0),
                "total_trades": int(worst_fold.get("total_trades", 0) or 0),
                "profit_factor": (
                    round(float(_fold_profit_factor(worst_fold)), 6)
                    if math.isfinite(_fold_profit_factor(worst_fold)) else _fold_profit_factor(worst_fold)
                ),
                "total_return_pct": _f(worst_fold, "total_return_pct"),
                "max_drawdown_pct": _f(worst_fold, "max_drawdown_pct"),
            },
            "policy_status": "PASS" if policy_pass else "FAIL",
            "policy_reason": policy_reason,
            "min_trades": int(min_trades or 0),
            "min_profit_factor": float(min_profit_factor or 0.0),
            "min_return_pct": float(min_return_pct or 0.0),
        }

    def evaluate_purged_kfold_oos(self,
                                  symbol: str,
                                  timeframe: str,
                                  regime: str,
                                  cfg: RegimeConfig,
                                  features: pd.DataFrame,
                                  scored_start: int) -> Optional[Dict[str, Any]]:
        """Evaluate one selected specialist over purged/embargoed OOS folds."""
        if cfg is None or cfg.is_no_trade_marker():
            return None
        if features is None or len(features) < 3 or "REGIME_LIVE" not in features.columns:
            return None
        ranges = self._build_purged_kfold_ranges(
            len(features),
            scored_start=scored_start,
            splits=int(getattr(self.config, "oos_kfold_splits", 5) or 5),
            embargo_bars=int(getattr(self.config, "oos_embargo_bars", 1) or 0),
            warmup_bars=int(scored_start or 0),
        )
        if not ranges:
            return None

        try:
            strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
        except Exception as exc:
            logger.warning(f"[{symbol}] [{timeframe}] [{regime}] Purged-kfold strategy load failed: {exc}")
            return None

        fold_metrics: List[Dict[str, Any]] = []
        for fold_range in ranges:
            test_start = int(fold_range["test_start"])
            test_end = int(fold_range["test_end"])
            warmup_start = int(fold_range["warmup_start"])
            fold_features = features.iloc[warmup_start:test_end].copy()
            fold_warmup = int(test_start - warmup_start)
            try:
                signals = strategy.generate_signals(fold_features, symbol)
                fold_result = self.backtester.run(
                    fold_features,
                    signals,
                    symbol,
                    strategy,
                    timeframe=timeframe,
                    warmup_bars=fold_warmup,
                )
                bucket = self._bucket_trades_by_regime(
                    fold_result.get("trades", []),
                    fold_features,
                    timeframe=timeframe,
                )
                metrics = self._compact_metric_tree(bucket.get(regime, {"total_trades": 0}))
            except Exception as exc:
                logger.debug(
                    f"[{symbol}] [{timeframe}] [{regime}] Purged-kfold fold "
                    f"{fold_range.get('fold')} skipped: {exc}"
                )
                metrics = {"total_trades": 0, "error": f"{type(exc).__name__}: {exc}"}

            try:
                start_ts = pd.Timestamp(features.index[test_start]).isoformat()
                end_ts = pd.Timestamp(features.index[test_end - 1]).isoformat()
            except Exception:
                start_ts = None
                end_ts = None
            metrics.update({
                "fold": int(fold_range["fold"]),
                "fold_count": int(fold_range["fold_count"]),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "scored_bars": int(test_end - test_start),
                "warmup_bars": int(fold_warmup),
                "purge_before_bars": int(fold_range["purge_before_bars"]),
                "embargo_after_bars": int(fold_range["embargo_after_bars"]),
                "test_start": int(test_start),
                "test_end": int(test_end),
                "train_segments": fold_range.get("train_segments", []),
            })
            fold_metrics.append(metrics)

        aggregate = self._aggregate_purged_kfold_metrics(
            fold_metrics,
            initial_capital=float(getattr(self.config, "initial_capital", 10000.0) or 10000.0),
            min_trades=int(getattr(self.config, "oos_min_trades", 5) or 0),
            min_profit_factor=float(getattr(self.config, "false_discovery_holdout_min_profit_factor", 1.0) or 0.0),
            min_return_pct=float(getattr(self.config, "false_discovery_holdout_min_return_pct", 0.0) or 0.0),
            aggregate_policy=str(
                getattr(self.config, "oos_aggregate_policy", "aggregate_with_worst_fold_floor")
                or "aggregate_with_worst_fold_floor"
            ),
        )
        aggregate["folds"] = fold_metrics
        aggregate["window"] = {
            "start_ts": fold_metrics[0].get("start_ts") if fold_metrics else None,
            "end_ts": fold_metrics[-1].get("end_ts") if fold_metrics else None,
            "bars": int(len(features)),
            "scored_start": int(scored_start or 0),
            "embargo_bars": int(getattr(self.config, "oos_embargo_bars", 1) or 0),
        }
        return aggregate

    @staticmethod
    def _deflated_sharpe_ratio(sharpe: float, n_trades: int, skew: float = 0.0,
                                kurt: float = 3.0, n_trials: int = 1,
                                sr_benchmark: float = 0.0) -> float:
        """
        Deflated Sharpe Ratio (Bailey & Lopez de Prado, 2014).

        Adjusts the observed Sharpe for multiple-testing bias using the number
        of independent trials.  Returns DSR in [0, 1] — the probability that
        the observed SR exceeds the expected maximum SR of *n_trials* iid tests.

        A DSR below ~0.05 is indistinguishable from noise.
        """
        if n_trades < 5 or n_trials < 1:
            return 0.0
        T = float(n_trades)
        sr = float(sharpe)
        # Standard error of the Sharpe Ratio
        se_sr_sq = (1.0 + 0.5 * sr ** 2 - skew * sr + ((kurt - 3.0) / 4.0) * sr ** 2) / T
        if se_sr_sq <= 0:
            return 0.0
        se_sr = se_sr_sq ** 0.5
        # Expected max SR under the null (iid normal trials)
        if n_trials > 1:
            euler_mascheroni = 0.5772156649
            e_max_sr = sr_benchmark + _scipy_norm.ppf(1.0 - 1.0 / n_trials) * (1.0 + euler_mascheroni / np.log(n_trials))
        else:
            e_max_sr = sr_benchmark
        # DSR = P(SR > E[max SR])
        dsr = float(_scipy_norm.cdf((sr - e_max_sr) / se_sr))
        return max(0.0, min(1.0, dsr))

    @staticmethod
    def _effective_search_trials(candidates: List[Dict[str, Any]]) -> int:
        declared_totals = [
            int(cand.get('local_search_trials_total', 0) or 0)
            for cand in candidates
            if int(cand.get('local_search_trials_total', 0) or 0) > 0
        ]
        if declared_totals:
            return max(max(declared_totals), 1)
        total = 0
        for cand in candidates:
            total += max(int(cand.get('search_trials', 1) or 1), 1)
        return max(total, 1)

    def _false_discovery_scope_multiplier(self, *, local_includes_regimes: bool = False) -> int:
        symbol_count = int(getattr(self.config, 'false_discovery_symbol_count', 0) or 0)
        symbol_count = max(1, symbol_count)
        try:
            timeframe_count = len(getattr(self.config, 'timeframes', []) or [])
        except Exception:
            timeframe_count = 1
        timeframe_count = max(1, timeframe_count)
        regime_count = 1 if local_includes_regimes else len(self.REGIMES)
        return max(1, symbol_count * timeframe_count * regime_count)

    def _build_false_discovery_evidence(self,
                                        val_metrics: Dict[str, Any],
                                        candidates: List[Dict[str, Any]],
                                        candidate: Dict[str, Any],
                                        regime: str = "",
                                        pbo_evidence: Optional[Dict[str, Any]] = None,
                                        validation_trials: int = 1,
                                        dsr_population_evidence: Optional[
                                            Dict[str, Any]
                                        ] = None) -> Dict[str, Any]:
        enabled = bool(getattr(self.config, 'false_discovery_control_enabled', False))
        local_trials = self._effective_search_trials(candidates)
        local_includes_regimes = any(
            str(cand.get("multiplicity_scope", ""))
            == "symbol_timeframe_all_regime_cells"
            for cand in candidates
        )
        scope_multiplier = self._false_discovery_scope_multiplier(
            local_includes_regimes=local_includes_regimes
        )
        local_cells = max(
            [int(cand.get("local_search_cells_total", 0) or 0) for cand in candidates]
            or [0]
        )
        candidate_trials = max(int(candidate.get('search_trials', 1) or 1), 1)
        # Validation multiplicity is the exact number of unique frozen models
        # that reached this cell's validation competition. The potentially much
        # larger inner train-only search belongs to train stability evidence and
        # must not be charged again to the non-overlapping validation DSR.
        effective_trials = max(int(validation_trials or 0), 1)
        min_daily = int(
            getattr(self.config, "false_discovery_min_daily_observations", 252) or 252
        )
        min_effective = float(
            getattr(self.config, "false_discovery_min_effective_observations", 60.0)
            or 60.0
        )
        min_dsr = float(getattr(self.config, 'false_discovery_min_dsr', 0.95) or 0.95)
        daily_returns, daily_dates = self._admissible_validation_daily_net_mtm(
            val_metrics
        )
        dsr_evidence = deflated_sharpe_evidence(
            daily_returns,
            attempted_trials=effective_trials,
            candidate_id=str(candidate.get("candidate_id") or ""),
            population_evidence=dsr_population_evidence,
            min_observations=min_daily,
            min_effective_observations=min_effective,
            min_dsr=min_dsr,
        )
        if pbo_evidence is None:
            pbo_evidence = aligned_cell_pbo(
                candidates,
                regime,
                min_candidates=int(
                    getattr(self.config, "false_discovery_pbo_min_candidates", 5) or 5
                ),
                min_observations=min_daily,
                max_pbo=float(getattr(self.config, "false_discovery_max_pbo", 0.50) or 0.50),
                blocks=int(getattr(self.config, "false_discovery_pbo_blocks", 10) or 10),
            )
        pbo_evidence = dict(pbo_evidence or {})
        candidate_pbo_id = str(
            candidate.get("candidate_id")
            or (candidate.get("oos_evidence") or {}).get("candidate_id")
            or ""
        )
        pbo_candidate_ids = [
            str(value) for value in (pbo_evidence.get("candidate_ids") or [])
        ]
        pbo_membership_required = (
            str(pbo_evidence.get("alignment", ""))
            == "exact_regime_identical_daily_axis"
        )
        pbo_candidate_included = (
            bool(candidate_pbo_id and candidate_pbo_id in pbo_candidate_ids)
            if pbo_membership_required
            else None
        )
        if pbo_membership_required and not pbo_candidate_included:
            pbo_evidence.update({
                "status": "UNAVAILABLE",
                "passed": False,
                "reason": "evaluated candidate is absent from the exact PBO matrix",
            })
        pbo_evidence["evaluated_candidate_id"] = candidate_pbo_id or None
        pbo_evidence["evaluated_candidate_included"] = pbo_candidate_included
        bootstrap = block_bootstrap_evidence(
            daily_returns,
            repetitions=int(
                getattr(self.config, "false_discovery_bootstrap_repetitions", 300) or 300
            ),
            confidence=float(
                getattr(self.config, "false_discovery_bootstrap_confidence", 0.95) or 0.95
            ),
            seed=_stable_seed(
                candidate.get("strategy_name", ""),
                candidate.get("params", {}),
                regime,
            ),
            min_observations=min_daily,
            min_effective_observations=min_effective,
            min_mean_lower=float(
                getattr(self.config, "false_discovery_bootstrap_min_mean_lower", 0.0)
                or 0.0
            ),
            min_profit_factor_lower=float(
                getattr(self.config, "false_discovery_bootstrap_min_pf_lower", 1.0)
                or 1.0
            ),
            max_drawdown_upper=float(
                getattr(self.config, "false_discovery_bootstrap_max_dd_upper", 0.20)
                or 0.20
            ),
        )
        dsr = float(dsr_evidence.get("deflated_sharpe", 0.0) or 0.0)
        independent_validation_trials = float(
            dsr_evidence.get("effective_independent_trials", 0.0) or 0.0
        )
        val_sharpe = float(dsr_evidence.get("sharpe_annualized", 0.0) or 0.0)
        val_trades = int(val_metrics.get('total_trades', 0) or 0)
        try:
            confidence_floor = float(getattr(self.config, 'false_discovery_confidence_floor', 0.35) or 0.0)
        except Exception:
            confidence_floor = 0.35
        confidence_floor = max(0.0, min(1.0, confidence_floor))
        confidence_multiplier = (
            confidence_floor + (1.0 - confidence_floor) * dsr
            if enabled and val_sharpe > 0.0
            else 1.0
        )
        return {
            "enabled": enabled,
            "statistics_contract_version": dsr_evidence.get("contract_version"),
            "pbo_status": str(pbo_evidence.get("status", "UNAVAILABLE")).lower(),
            "pbo": pbo_evidence.get("pbo"),
            "pbo_evidence": dict(pbo_evidence),
            "pbo_candidate_included": pbo_candidate_included,
            "bootstrap_evidence": dict(bootstrap),
            "dsr_evidence": dict(dsr_evidence),
            "dsr_population_evidence": dict(
                dsr_population_evidence or {}
            ),
            "val_sharpe": val_sharpe,
            "val_trades": val_trades,
            "local_trials": int(local_trials),
            "local_search_cells": int(local_cells),
            "local_scope_includes_regimes": bool(local_includes_regimes),
            "candidate_trials": int(candidate_trials),
            "validation_frozen_candidates": int(effective_trials),
            "scope_multiplier": int(scope_multiplier),
            "effective_trials": independent_validation_trials,
            "daily_net_mtm_observations": int(len(daily_returns)),
            "daily_net_mtm_axis_start": daily_dates[0] if daily_dates else None,
            "daily_net_mtm_axis_end": daily_dates[-1] if daily_dates else None,
            "multiplicity_scope": {
                "train_inner_attempted_trials_exact": int(local_trials),
                "validation_frozen_candidates_exact": int(effective_trials),
                "local_search_cells_exact": int(local_cells),
                "global_roster_attempted_trials_exact": None,
                "global_roster_scope_status": "UNAVAILABLE",
                "global_roster_scope_reason": (
                    "book-level attempted-trial ledger is not yet bound to this cell"
                ),
                "reported_symbol_timeframe_regime_scope_multiplier": int(
                    scope_multiplier
                ),
            },
            "deflated_sharpe": round(float(dsr), 6),
            "confidence_floor": float(confidence_floor),
            "confidence_multiplier": round(float(confidence_multiplier), 6),
            "min_dsr": min_dsr,
        }

    def _call_regime_score(self,
                           train_metrics: Dict[str, Any],
                           val_metrics: Dict[str, Any],
                           candidate: Optional[Dict[str, Any]] = None) -> float:
        score_fn = self._compute_regime_score
        if candidate is not None:
            try:
                return float(score_fn(train_metrics, val_metrics, candidate=candidate))
            except TypeError as exc:
                message = str(exc)
                if "candidate" not in message and "unexpected keyword" not in message:
                    raise
        return float(score_fn(train_metrics, val_metrics))

    def _build_legacy_confidence_evidence(
        self,
        val_metrics: Dict[str, Any],
        candidate: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build positive-edge confidence evidence for post-search legacy reranking."""
        _, governed_val, _ = self._governed_scoring_inputs(
            {}, val_metrics, candidate
        )
        scoring_metrics = governed_val if governed_val else dict(val_metrics or {})
        r_count = int(
            scoring_metrics.get(
                "r_count", scoring_metrics.get("total_trades", 0)
            )
            or 0
        )
        mean_r = float(scoring_metrics.get("mean_r", 0.0) or 0.0)
        std_r = float(scoring_metrics.get("std_r", 0.0) or 0.0)
        se_r = float(scoring_metrics.get("se_r", 0.0) or 0.0)
        if (
            isinstance(candidate, Mapping)
            and bool(candidate.get("trade_power_contract_enforced", False))
        ):
            se_r = std_r / math.sqrt(float(r_count)) if r_count >= 2 and std_r > 1e-12 else 0.0
        elif se_r <= 1e-12 and r_count >= 2 and std_r > 1e-12:
            se_r = std_r / math.sqrt(float(r_count))
        t_stat = mean_r / se_r if mean_r > 0.0 and se_r > 1e-12 else 0.0
        confidence = max(0.0, min(1.0, 1.0 - (1.0 / t_stat))) if t_stat > 1.0 else 0.0
        cap = max(0.0, min(0.50, float(getattr(self.config, "legacy_confidence_bonus_cap", 0.08) or 0.0)))
        multiplier = 1.0 + cap * confidence
        return {
            "enabled": True,
            "surface": "validation",
            "positive_edge_only": True,
            "r_count": r_count,
            "mean_r": round(mean_r, 6),
            "std_r": round(std_r, 6),
            "se_r": round(se_r, 6),
            "edge_t_stat_r": round(t_stat, 6),
            "confidence": round(confidence, 6),
            "bonus_cap": round(cap, 6),
            "multiplier": round(multiplier, 6),
        }

    @staticmethod
    def _selection_surface_failure(
        metrics: Dict[str, Any],
        *,
        label: str,
    ) -> Optional[str]:
        """Return a fail-closed reason for a malformed core metric surface."""
        try:
            trades_raw = float(metrics.get("total_trades"))
            total_return = float(metrics.get("total_return_pct"))
            drawdown = float(metrics.get("max_drawdown_pct"))
            profit_factor = float(metrics.get("profit_factor"))
            win_rate = float(metrics.get("win_rate", 0.0))
        except (TypeError, ValueError, OverflowError):
            return f"{label} core metrics are missing or non-numeric"
        if not math.isfinite(trades_raw) or trades_raw < 0.0 or not trades_raw.is_integer():
            return f"{label} total_trades is invalid"
        if not math.isfinite(total_return):
            return f"{label} total_return_pct is non-finite"
        if not math.isfinite(drawdown) or drawdown < 0.0:
            return f"{label} max_drawdown_pct is invalid"
        # Positive infinity is a valid PF when a sufficiently populated sample
        # has no losing trades; NaN, negative values, and -inf are not.
        if math.isnan(profit_factor) or profit_factor < 0.0:
            return f"{label} profit_factor is invalid"
        if not math.isfinite(win_rate) or not 0.0 <= win_rate <= 100.0:
            return f"{label} win_rate is outside [0, 100]"
        return None

    @staticmethod
    def _candidate_selection_sample_strength(
        candidate: Mapping[str, Any],
        regime: str,
    ) -> float:
        """Use effective observations for governed candidate score ties."""
        if bool(candidate.get("trade_power_contract_enforced", False)):
            values: List[float] = []
            for key in ("trade_power_train", "trade_power_validation"):
                decision = candidate.get(key)
                if not isinstance(decision, Mapping):
                    return float("-inf")
                try:
                    value = float(decision.get("observed_effective_trades"))
                except (TypeError, ValueError, OverflowError):
                    return float("-inf")
                if not math.isfinite(value) or value < 0.0:
                    return float("-inf")
                values.append(value)
            return min(values) if values else float("-inf")
        try:
            return float(
                (candidate.get("train_regime_metrics") or {})
                .get(regime, {})
                .get("total_trades", 0)
                or 0
            )
        except (TypeError, ValueError, OverflowError):
            return float("-inf")

    @staticmethod
    def _effective_count_surface(
        metrics: Mapping[str, Any],
        effective_trades: float,
    ) -> Dict[str, Any]:
        """Replace raw-count scoring authority with one authenticated ESS value."""
        surface = dict(metrics or {})
        surface["total_trades"] = float(effective_trades)
        surface["r_count"] = float(effective_trades)
        surface["effective_trades"] = float(effective_trades)
        return surface

    @classmethod
    def _governed_scoring_inputs(
        cls,
        train_metrics: Mapping[str, Any],
        val_metrics: Mapping[str, Any],
        candidate: Optional[Mapping[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]]]:
        """Return score-only surfaces whose sample size is invariant to overlap."""
        if not (
            isinstance(candidate, Mapping)
            and bool(candidate.get("trade_power_contract_enforced", False))
        ):
            return dict(train_metrics or {}), dict(val_metrics or {}), (
                dict(candidate) if isinstance(candidate, Mapping) else None
            )
        try:
            train_effective = float(
                (candidate.get("trade_power_train") or {}).get(
                    "observed_effective_trades"
                )
            )
            val_effective = float(
                (candidate.get("trade_power_validation") or {}).get(
                    "observed_effective_trades"
                )
            )
        except (TypeError, ValueError, OverflowError):
            return {}, {}, None
        if (
            not math.isfinite(train_effective)
            or train_effective < 0.0
            or not math.isfinite(val_effective)
            or val_effective < 0.0
        ):
            return {}, {}, None

        sanitized_candidate = dict(candidate)

        def sanitize_aux_surface(value: Any) -> Any:
            if not isinstance(value, Mapping):
                return value
            surface = dict(value)
            raw_effective = surface.get("effective_trades")
            try:
                auxiliary_effective = float(raw_effective)
            except (TypeError, ValueError, OverflowError):
                auxiliary_effective = 0.0
            if not math.isfinite(auxiliary_effective) or auxiliary_effective < 0.0:
                auxiliary_effective = 0.0
            return cls._effective_count_surface(surface, auxiliary_effective)

        for key in (
            "threadspark_selection_regime_metrics",
            "threadspark_historical_regime_metrics",
        ):
            by_regime = sanitized_candidate.get(key)
            if isinstance(by_regime, Mapping):
                sanitized_candidate[key] = {
                    str(regime): sanitize_aux_surface(surface)
                    for regime, surface in by_regime.items()
                }
        chunks = sanitized_candidate.get("threadspark_historical_chunk_regime_metrics")
        if isinstance(chunks, list):
            sanitized_candidate["threadspark_historical_chunk_regime_metrics"] = [
                {
                    str(regime): sanitize_aux_surface(surface)
                    for regime, surface in chunk.items()
                }
                if isinstance(chunk, Mapping)
                else chunk
                for chunk in chunks
            ]
        return (
            cls._effective_count_surface(train_metrics, train_effective),
            cls._effective_count_surface(val_metrics, val_effective),
            sanitized_candidate,
        )

    def _select_best_for_regime(self,
                                 symbol: str,
                                 timeframe: str,
                                 regime: str,
                                 candidates: List[Dict[str, Any]]) -> Tuple[Optional[RegimeConfig], bool, str]:
        """
        Select best candidate for a specific regime with validation.
        
        Applies minimum trade thresholds, profitability gates, scores candidates, 
        and validates winner.
        
        Returns:
            Tuple of (RegimeConfig or None, is_validated, validation_reason)
        """
        scored_candidates = []  # List of (score, candidate, false_discovery, legacy_confidence) tuples
        rejection_reasons = []  # Track why candidates were rejected at gate level
        best_score = -float('inf')
        best_candidate = None
        min_val_trades = self._min_val_trades_for_timeframe(timeframe)
        false_discovery_enabled = bool(
            getattr(self.config, "false_discovery_control_enabled", False)
        )
        train_eligible: List[
            Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]
        ] = []
        for cand in candidates:
            cand["timeframe"] = str(timeframe).upper()
            cand["candidate_id"] = self._exact_cell_candidate_id(
                symbol,
                timeframe,
                regime,
                cand,
            )
            train_metrics = dict(
                (cand.get('train_regime_metrics') or {}).get(regime, {}) or {}
            )
            val_metrics = dict(
                (cand.get('val_regime_metrics') or {}).get(regime, {}) or {}
            )
            train_surface_failure = self._selection_surface_failure(
                train_metrics,
                label="train",
            )
            if train_surface_failure:
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="TRAIN_SELECTION_SURFACE_INVALID",
                    reason=train_surface_failure,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=str(cand.get("strategy_name", "")),
                    parameter_hash=_stable_hash(
                        self._clean_candidate_params(cand.get("params", {}))
                    ),
                ))

            power_enforced = bool(
                cand.get("trade_power_contract_enforced", False)
            )
            if power_enforced:
                train_power = cand.get("trade_power_train")
                validation_power = cand.get("trade_power_validation")
                train_power_state, train_power_reason = (
                    self._trade_power_decision_state(
                        train_power,
                        partition="optimizer_train",
                    )
                )
                validation_power_state, validation_power_reason = (
                    self._trade_power_decision_state(
                        validation_power,
                        partition="tuning_validation",
                    )
                )
                if train_power_state == "INFRA" or validation_power_state == "INFRA":
                    reason_code = (
                        train_power_reason
                        if train_power_state == "INFRA"
                        else validation_power_reason
                    )
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code=reason_code,
                        reason=(
                            "candidate carries a missing, malformed, or infrastructure-"
                            "failed trade-power decision"
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                        parameter_hash=_stable_hash(
                            self._clean_candidate_params(cand.get("params", {}))
                        ),
                    ))
                train_requirement = (
                    train_power.get("frozen_requirement")
                    if isinstance(train_power, Mapping)
                    else None
                )
                validation_requirement = (
                    validation_power.get("frozen_requirement")
                    if isinstance(validation_power, Mapping)
                    else None
                )
                if (
                    not isinstance(train_requirement, Mapping)
                    or not isinstance(validation_requirement, Mapping)
                    or dict(train_requirement) != dict(validation_requirement)
                ):
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code="TRADE_POWER_FROZEN_REQUIREMENT_DRIFT",
                        reason=(
                            "train and tuning-validation decisions do not share "
                            "the exact frozen requirement"
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                        parameter_hash=_stable_hash(
                            self._clean_candidate_params(cand.get("params", {}))
                        ),
                    ))
                try:
                    train_observed = float(
                        train_power.get("observed_effective_trades")
                    )
                    validation_observed = float(
                        validation_power.get("observed_effective_trades")
                    )
                    train_metric_effective = float(
                        train_metrics.get("effective_trades")
                    )
                    validation_metric_effective = float(
                        val_metrics.get("effective_trades")
                    )
                except (TypeError, ValueError, OverflowError) as exc:
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code="TRADE_POWER_METRIC_EFFECTIVE_COUNT_DRIFT",
                        reason="candidate metric surfaces lack authenticated effective counts",
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                    )) from exc
                if (
                    not math.isfinite(train_metric_effective)
                    or train_metric_effective < 0.0
                    or not math.isfinite(validation_metric_effective)
                    or validation_metric_effective < 0.0
                    or not math.isclose(
                        train_metric_effective,
                        train_observed,
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    )
                    or not math.isclose(
                        validation_metric_effective,
                        validation_observed,
                        rel_tol=1e-12,
                        abs_tol=1e-12,
                    )
                ):
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code="TRADE_POWER_METRIC_EFFECTIVE_COUNT_DRIFT",
                        reason=(
                            "candidate metric effective counts differ from the "
                            "authenticated power decisions"
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                    ))
                if train_power_state != "PASS":
                    rejection_reasons.append(
                        f"{cand.get('strategy_name', 'Unknown')}: train effective trades underpowered"
                    )
                    continue

            train_trades = train_metrics.get('total_trades', 0)
            if not power_enforced and train_trades < self.min_train_trades:
                continue
            train_dd = train_metrics.get('max_drawdown_pct', 100.0)
            if train_dd > self.val_max_drawdown * 1.25:
                continue
            # The multiplicity/PBO roster freezes here, before any full-period
            # validation outcome (PF/return/DD/trade count) is inspected.
            train_eligible.append((cand, train_metrics, val_metrics))

        min_daily = int(
            getattr(self.config, "false_discovery_min_daily_observations", 252)
            or 252
        )
        pbo_candidates: List[Dict[str, Any]] = []
        pbo_candidate_ids: set = set()
        if false_discovery_enabled:
            for cand, _train_metrics, _val_metrics in train_eligible:
                candidate_id = str(cand.get("candidate_id") or "")
                if not candidate_id:
                    pbo_candidates.append(cand)
                    continue
                if candidate_id in pbo_candidate_ids:
                    continue
                pbo_candidate_ids.add(candidate_id)
                pbo_candidates.append(cand)
            cell_pbo_evidence = aligned_cell_pbo(
                pbo_candidates,
                regime,
                min_candidates=int(
                    getattr(self.config, "false_discovery_pbo_min_candidates", 5) or 5
                ),
                min_observations=min_daily,
                max_pbo=float(
                    getattr(self.config, "false_discovery_max_pbo", 0.50) or 0.50
                ),
                blocks=int(
                    getattr(self.config, "false_discovery_pbo_blocks", 10) or 10
                ),
            )
            cell_pbo_evidence = {
                **dict(cell_pbo_evidence or {}),
                "population_contract": (
                    "predeclared_train_eligible_frozen_validation_roster_v1"
                ),
                "input_candidate_count": int(len(candidates)),
                "train_predeclared_eligible_count": int(len(train_eligible)),
                "unique_predeclared_frozen_validation_candidate_count": int(
                    len(pbo_candidates)
                ),
            }
            cell_dsr_population = aligned_validation_dsr_population(
                pbo_candidates,
                regime,
                min_observations=min_daily,
            )
        else:
            cell_pbo_evidence = {
                "status": "DISABLED",
                "passed": True,
                "pbo": None,
                "population_contract": "disabled",
            }
            cell_dsr_population = {
                "status": "DISABLED",
                "contract_version": "disabled",
            }
        validation_trials = max(1, len(pbo_candidate_ids))

        for cand, train_metrics, val_metrics in train_eligible:
            if bool(cand.get("trade_power_contract_enforced", False)):
                validation_power_state, validation_power_reason = (
                    self._trade_power_decision_state(
                        cand.get("trade_power_validation"),
                        partition="tuning_validation",
                    )
                )
                if validation_power_state == "INFRA":
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code=validation_power_reason,
                        reason=(
                            "candidate validation trade-power decision became invalid "
                            "after the frozen multiplicity roster was built"
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                    ))
                if validation_power_state != "PASS":
                    rejection_reasons.append(
                        f"{cand.get('strategy_name', 'Unknown')}: validation effective trades underpowered"
                    )
                    continue
            validation_surface_failure = self._selection_surface_failure(
                val_metrics,
                label="validation",
            )
            if validation_surface_failure:
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="VALIDATION_SELECTION_SURFACE_INVALID",
                    reason=validation_surface_failure,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_family=str(cand.get("strategy_name", "")),
                    parameter_hash=_stable_hash(
                        self._clean_candidate_params(cand.get("params", {}))
                    ),
                ))
            val_trades = val_metrics.get('total_trades', 0)
            if (
                not bool(cand.get("trade_power_contract_enforced", False))
                and val_trades < min_val_trades
            ):
                continue
            val_dd = val_metrics.get('max_drawdown_pct', 100.0)
            if val_dd > self.val_max_drawdown:
                continue
            val_return_early = val_metrics.get('total_return_pct', -100.0)
            val_return_dd_ratio = self._compute_return_dd_ratio(
                val_return_early,
                val_dd,
            )
            if val_return_dd_ratio < self.min_val_return_dd_ratio:
                rejection_reasons.append(
                    f"{cand['strategy_name']}: return/DD={val_return_dd_ratio:.2f} < "
                    f"{self.min_val_return_dd_ratio:.2f} "
                    f"(ret={val_return_early:.1f}%, dd={val_dd:.1f}%)"
                )
                continue
            val_pf = val_metrics.get('profit_factor', 0.0)
            val_return = val_metrics.get('total_return_pct', -100.0)
            if not self.allow_losing_winners:
                if val_pf < self.min_val_profit_factor:
                    rejection_reasons.append(
                        f"{cand['strategy_name']}: PF={val_pf:.2f} < "
                        f"{self.min_val_profit_factor}"
                    )
                    continue
                if val_return < self.min_val_return_pct:
                    rejection_reasons.append(
                        f"{cand['strategy_name']}: return={val_return:.1f}% < "
                        f"{self.min_val_return_pct}"
                    )
                    continue

            # Apply Deflated Sharpe Ratio through one explicit confidence channel only.
            # Do not mutate validation Sharpe in the metric surface; that created
            # model-dependent double weighting and also ran when false discovery was off.
            val_sharpe = val_metrics.get('sharpe_approx', val_metrics.get('sharpe_ratio', 0.0))
            false_discovery = self._build_false_discovery_evidence(
                val_metrics,
                candidates,
                cand,
                regime=regime,
                pbo_evidence=cell_pbo_evidence,
                validation_trials=validation_trials,
                dsr_population_evidence=cell_dsr_population,
            )
            min_dsr = float(false_discovery.get("min_dsr", 0.0) or 0.0)
            dsr = float(false_discovery.get("deflated_sharpe", 0.0) or 0.0)
            if bool(false_discovery.get("enabled", False)):
                dsr_evidence = dict(false_discovery.get("dsr_evidence") or {})
                dsr_status = str(
                    dsr_evidence.get("status", "UNAVAILABLE")
                ).upper()
                dsr_selection_passed = (
                    dsr_status == "PASS"
                    or (
                        dsr_status == "DIAGNOSTIC_ONLY"
                        and dsr_evidence.get("diagnostic_threshold_passed") is True
                    )
                )
                if not dsr_selection_passed or dsr < min_dsr:
                    rejection_reasons.append(
                        f"{cand['strategy_name']}: daily-net-MTM DSR {dsr_status} "
                        f"value={dsr:.4f} threshold={min_dsr:.4f} "
                        f"(actual_trials={false_discovery.get('effective_trials')})"
                    )
                    continue
                pbo_status = str(false_discovery.get("pbo_status", "unavailable")).upper()
                if pbo_status != "PASS":
                    rejection_reasons.append(
                        f"{cand['strategy_name']}: aligned cell PBO {pbo_status} "
                        f"value={false_discovery.get('pbo')}"
                    )
                    continue
                if bool(
                    getattr(self.config, "false_discovery_require_bootstrap", True)
                ):
                    bootstrap_status = str(
                        (false_discovery.get("bootstrap_evidence") or {}).get(
                            "status", "UNAVAILABLE"
                        )
                    ).upper()
                    if bootstrap_status != "PASS":
                        rejection_reasons.append(
                            f"{cand['strategy_name']}: block bootstrap {bootstrap_status}"
                        )
                        continue

            scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
            if scoring_model == "specialist_spokes_threadspark":
                scoring_train, scoring_val, scoring_candidate = (
                    self._governed_scoring_inputs(
                        train_metrics,
                        val_metrics,
                        cand,
                    )
                )
                if not scoring_train or not scoring_val or scoring_candidate is None:
                    raise SearchInfrastructureError(TrialOutcome(
                        kind=SearchOutcomeKind.INFRA_FAILURE,
                        stage=SearchStage.FAMILY_CELL,
                        reason_code="TRADE_POWER_SCORING_SURFACE_INVALID",
                        reason="governed scoring inputs could not be authenticated",
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        strategy_family=str(cand.get("strategy_name", "")),
                    ))
                cand["_threadspark_l1_base_score"] = self._compute_threadspark_l1_score(
                    scoring_train,
                    scoring_val,
                    scoring_candidate,
                )
                score = self._compute_threadspark_spoke_score(
                    scoring_train,
                    scoring_val,
                    candidate=scoring_candidate,
                    regime=regime,
                )
            else:
                score = self._call_regime_score(train_metrics, val_metrics, candidate=cand)
            legacy_confidence = None
            if scoring_model == "legacy_confidence":
                legacy_confidence = self._build_legacy_confidence_evidence(
                    val_metrics,
                    candidate=cand,
                )
                score *= float(legacy_confidence.get("multiplier", 1.0) or 1.0)
            if bool(false_discovery.get("enabled", False)):
                score *= float(false_discovery.get("confidence_multiplier", 1.0) or 1.0)
            if not math.isfinite(float(score)):
                rejection_reasons.append(
                    f"{cand.get('strategy_name', 'Unknown')}: selection score is non-finite"
                )
                continue
            scored_candidates.append((score, cand, false_discovery, legacy_confidence))

            if score > best_score:
                best_score = score
                best_candidate = cand

        if not scored_candidates or best_candidate is None:
            reason = "No candidates met minimum trade/drawdown/profitability thresholds"
            if rejection_reasons:
                reason += (
                    f" (rejected: {len(rejection_reasons)} candidates by gate checks; "
                    f"first={rejection_reasons[0]})"
                )
            return None, False, reason

        scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
        if scoring_model == "specialist_spokes_threadspark":
            scored_candidates.sort(
                key=lambda item: (
                    -float(item[1].get("_threadspark_l1_base_score", item[0]) or 0.0),
                    int(item[1].get('search_trials', 1) or 1),
                    str(item[1].get('strategy_name', '')),
                    _stable_hash(self._clean_candidate_params(item[1].get('params', {}))),
                )
            )
            l1_top_k = min(self.regime_validation_top_k, len(scored_candidates))
            scored_candidates = sorted(
                scored_candidates[:l1_top_k],
                key=lambda item: (
                    -float(item[0]),
                    -float(item[1].get("_threadspark_l1_base_score", item[0]) or 0.0),
                    int(item[1].get('search_trials', 1) or 1),
                    str(item[1].get('strategy_name', '')),
                    _stable_hash(self._clean_candidate_params(item[1].get('params', {}))),
                ),
            )
        else:
            scored_candidates.sort(
                key=lambda item: (
                    -float(item[0]),
                    -self._candidate_selection_sample_strength(item[1], regime),
                    int(item[1].get('search_trials', 1) or 1),
                    str(item[1].get('strategy_name', '')),
                    _stable_hash(self._clean_candidate_params(item[1].get('params', {}))),
                )
            )

        # Every candidate in this list already paid the cost of frozen tuning
        # validation and metric scoring.  Truncating descent here saved no
        # compute and could manufacture NO_TRADE when rank K+1 was the first
        # candidate to pass the final generalization gates.  Descend the full
        # scored list.  ThreadSpark's research-only L1 shortlist, when used,
        # has already been applied above and remains a distinct model choice.
        descent_count = len(scored_candidates)

        logger.debug(
            f"[{symbol}] [{timeframe}] [{regime}] Candidate descent: "
            f"validating all {descent_count} already-scored, gate-passed candidates"
        )

        # Return the highest-scored candidate that passes every final gate.
        validation_failures = []
        for rank, (score, cand, false_discovery, legacy_confidence) in enumerate(
            scored_candidates,
            start=1,
        ):
            train_metrics = cand['train_regime_metrics'].get(regime, {})
            val_metrics = cand['val_regime_metrics'].get(regime, {})
            strategy_name = cand.get('strategy_name', 'Unknown')

            if bool(getattr(self.config, "regime_timeframe_val_trade_floors_enabled", False)):
                is_validated, validation_reason = self._validate_regime_winner(
                    train_metrics, val_metrics, regime, strategy_name, timeframe=timeframe,
                    trade_power_enforced=bool(cand.get("trade_power_contract_enforced", False)),
                )
            else:
                is_validated, validation_reason = self._validate_regime_winner(
                    train_metrics, val_metrics, regime, strategy_name,
                    trade_power_enforced=bool(cand.get("trade_power_contract_enforced", False)),
                )

            if is_validated:
                if rank > 1:
                    logger.info(
                        f"[{symbol}] [{timeframe}] [{regime}] Descent: "
                        f"rank #{rank} {strategy_name} passed validation "
                        f"(top {rank - 1} failed)"
                    )
                else:
                    logger.debug(
                        f"[{symbol}] [{timeframe}] [{regime}] Descent: "
                        f"rank #1 {strategy_name} passed validation"
                    )

                quality_score = self._normalize_score(score)
                now = datetime.now()

                config = RegimeConfig(
                    strategy_name=strategy_name,
                    parameters=cand['params'],
                    quality_score=quality_score,
                    train_metrics=train_metrics,
                    val_metrics=val_metrics,
                    regime_train_trades=train_metrics.get('total_trades', 0),
                    regime_val_trades=val_metrics.get('total_trades', 0),
                    trained_at=now,
                    valid_until=_scheduled_valid_until(self.config, now),
                    validation_evidence=self._build_validation_evidence(
                        train_metrics,
                        val_metrics,
                        score=score,
                        validation_reason=validation_reason,
                        candidate=cand,
                        false_discovery_evidence=false_discovery,
                        legacy_confidence_evidence=legacy_confidence,
                    ),
                )
                if scoring_model == "specialist_spokes_threadspark":
                    config.validation_evidence["threadspark_selector"] = self._compute_threadspark_spoke_evidence(
                        train_metrics,
                        val_metrics,
                        cand,
                        regime=regime,
                    )

                return config, True, validation_reason
            else:
                validation_failures.append(f"#{rank} {strategy_name}: {validation_reason}")
                logger.debug(
                    f"[{symbol}] [{timeframe}] [{regime}] Descent: "
                    f"rank #{rank} {strategy_name} FAILED: {validation_reason}"
                )

        # Every scored candidate failed validation - return the best one as unvalidated
        best_score, best_candidate, false_discovery, legacy_confidence = scored_candidates[0]
        train_metrics = best_candidate['train_regime_metrics'].get(regime, {})
        val_metrics = best_candidate['val_regime_metrics'].get(regime, {})
        
        # Validate the winner using fx_backtester rules
        if bool(getattr(self.config, "regime_timeframe_val_trade_floors_enabled", False)):
            is_validated, validation_reason = self._validate_regime_winner(
                train_metrics,
                val_metrics,
                regime,
                best_candidate['strategy_name'],
                timeframe=timeframe,
                trade_power_enforced=bool(
                    best_candidate.get("trade_power_contract_enforced", False)
                ),
            )
        else:
            is_validated, validation_reason = self._validate_regime_winner(
                train_metrics,
                val_metrics,
                regime,
                best_candidate['strategy_name'],
                trade_power_enforced=bool(
                    best_candidate.get("trade_power_contract_enforced", False)
                ),
            )
        
        # Normalize quality score to 0-1 using sigmoid mapping
        quality_score = self._normalize_score(best_score)
        now = datetime.now()

        config = RegimeConfig(
            strategy_name=best_candidate['strategy_name'],
            parameters=best_candidate['params'],
            quality_score=quality_score,
            train_metrics=train_metrics,
            val_metrics=val_metrics,
            regime_train_trades=train_metrics.get('total_trades', 0),
            regime_val_trades=val_metrics.get('total_trades', 0),
            trained_at=now,
            valid_until=_scheduled_valid_until(self.config, now),
            validation_evidence=self._build_validation_evidence(
                train_metrics,
                val_metrics,
                score=best_score,
                validation_reason=validation_reason,
                candidate=best_candidate,
                false_discovery_evidence=false_discovery,
                legacy_confidence_evidence=legacy_confidence,
            ),
        )
        if str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower() == "specialist_spokes_threadspark":
            config.validation_evidence["threadspark_selector"] = self._compute_threadspark_spoke_evidence(
                train_metrics,
                val_metrics,
                best_candidate,
                regime=regime,
            )

        descent_summary = "; ".join(validation_failures)
        final_reason = (
            f"All {len(validation_failures)} descent candidates failed validation "
            f"[{descent_summary}]"
        )
        logger.info(
            f"[{symbol}] [{timeframe}] [{regime}] Descent exhausted: "
            f"{len(validation_failures)}/{descent_count} candidates failed validation"
        )

        return config, False, final_reason
    
    def _validate_regime_winner(self,
                                 train_metrics: Dict[str, Any],
                                 val_metrics: Dict[str, Any],
                                 regime: str,
                                 candidate_name: str = "Unknown",
                                 timeframe: Optional[str] = None,
                                 trade_power_enforced: bool = False) -> Tuple[bool, str]:
        """
        Validate a regime winner against fx_backtester thresholds.

        NEW: Supports weak train exception - allows train PF < 1.0 or train return < 0
        ONLY if validation is exceptional (configurable thresholds).

        Checks (ALL enforced unless allow_losing_winners=True):
        - Validation trade count >= fx_val_min_trades
        - Validation drawdown < fx_val_max_drawdown
        - Validation profit_factor >= regime_min_val_profit_factor (default 1.0)
        - Validation return_pct >= regime_min_val_return_pct (default 0.0)
        - Robustness ratio >= fx_min_robustness_ratio OR val_sharpe > override

        Exception for weak train:
        - If train PF < 1.0 OR train return < 0, require:
          - val_pf >= exceptional_val_profit_factor (default 1.3)
          - val_return >= exceptional_val_return_pct (default 2.0%)
          - val_trades >= 2x min_val_trades

        Args:
            train_metrics: Training regime bucket metrics
            val_metrics: Validation regime bucket metrics
            regime: Regime name for logging
            candidate_name: Strategy name for logging

        Returns:
            (is_valid, reason)
        """
        val_trades = val_metrics.get('total_trades', 0)
        val_sharpe = val_metrics.get('sharpe_approx', 0)
        val_drawdown = val_metrics.get('max_drawdown_pct', 100.0)  # Default to worst-case
        val_pf = val_metrics.get('profit_factor', 0.0)
        val_return = val_metrics.get('total_return_pct', -100.0)

        train_pf = train_metrics.get('profit_factor', 0.0)
        train_return = train_metrics.get('total_return_pct', -100.0)
        min_val_trades = self._min_val_trades_for_timeframe(timeframe)

        # Check minimum validation trades
        if not trade_power_enforced and val_trades < min_val_trades:
            return False, f"Insufficient val trades: {val_trades} < {min_val_trades}"

        # Enforce drawdown limits before winner validation.
        if val_drawdown > self.val_max_drawdown:
            return False, f"Excessive val drawdown: {val_drawdown:.1f}% > {self.val_max_drawdown}%"

        # Also check training drawdown as early rejection
        train_drawdown = train_metrics.get('max_drawdown_pct', 100.0)
        # Allow slightly higher train DD (1.25x) since we expect some overfitting
        train_dd_threshold = self.val_max_drawdown * 1.25
        if train_drawdown > train_dd_threshold:
            return False, f"Excessive train drawdown: {train_drawdown:.1f}% > {train_dd_threshold:.1f}%"

        # ===== NEW: Weak Train Exception =====
        # Check if training was weak (losing on train data)
        weak_train = train_pf < 1.0 or train_return < 0

        if weak_train:
            # Require EXCEPTIONAL validation to allow weak train (findings.html §5).
            # In addition to the original PF/return/trades gates we now demand:
            #   * val_sharpe ≥ 0.5  — risk-adjusted return must be real, not noise
            #   * val_trades ≥ max(2·min_val_trades, 50)  — sample size large enough
            #   * val_win_rate > 50% — edge must come from hit-rate, not lucky tail
            exceptional_val_pf = float(self.config.exceptional_val_profit_factor)
            exceptional_val_return = float(self.config.exceptional_val_return_pct)
            exceptional_val_min_trades = (
                0 if trade_power_enforced else max(min_val_trades * 2, 50)
            )
            exceptional_val_min_sharpe = 0.5
            exceptional_val_min_win_rate = 50.0
            val_win_rate = float(val_metrics.get('win_rate', 0.0) or 0.0)

            if (val_pf >= exceptional_val_pf and
                val_return >= exceptional_val_return and
                val_trades >= exceptional_val_min_trades and
                val_sharpe >= exceptional_val_min_sharpe and
                val_win_rate > exceptional_val_min_win_rate):

                logger.info(
                    f"[{regime}] {candidate_name}: Allowing weak train "
                    f"(train PF {train_pf:.2f}, train return {train_return:.1f}%) "
                    f"due to exceptional validation "
                    f"(val PF {val_pf:.2f}, val return {val_return:.1f}%, "
                    f"val trades {val_trades}, val sharpe {val_sharpe:.2f}, "
                    f"val WR {val_win_rate:.1f}%)"
                )
                # Continue to other validation checks below
            else:
                return False, (
                    f"Weak train rejected: train PF {train_pf:.2f}, "
                    f"train return {train_return:.1f}% — validation not exceptional "
                    f"(val PF {val_pf:.2f}/{exceptional_val_pf}, "
                    f"val return {val_return:.1f}%/{exceptional_val_return}%, "
                    f"val trades {val_trades}/{exceptional_val_min_trades}, "
                    f"val sharpe {val_sharpe:.2f}/{exceptional_val_min_sharpe}, "
                    f"val WR {val_win_rate:.1f}%/{exceptional_val_min_win_rate}%)"
                )

        # Final profitability gates for validated winners.
        if not self.allow_losing_winners:
            # Check profit factor >= threshold (default 1.0 = must be profitable)
            if val_pf < self.min_val_profit_factor:
                return False, f"Unprofitable: val PF {val_pf:.3f} < {self.min_val_profit_factor:.2f}"

            # Check return >= threshold (default 0.0 = must be non-negative)
            if val_return < self.min_val_return_pct:
                return False, f"Negative return: val return {val_return:.2f}% < {self.min_val_return_pct:.1f}%"

        # Return-to-DD efficiency gate (unconditional risk-efficiency rule, same category as DD cap)
        val_return_dd_ratio = self._compute_return_dd_ratio(val_return, val_drawdown)
        min_return_dd = self.min_val_return_dd_ratio
        if val_return_dd_ratio < min_return_dd:
            prefix = "Weak train rejected: " if weak_train else ""
            return False, (
                f"{prefix}Poor return/DD efficiency: {val_return_dd_ratio:.2f} "
                f"(return {val_return:.1f}% / DD {val_drawdown:.1f}%) "
                f"< {min_return_dd:.2f}"
            )

        # Compute robustness ratio
        if trade_power_enforced:
            try:
                train_effective = float(train_metrics.get("effective_trades"))
                val_effective = float(val_metrics.get("effective_trades"))
            except (TypeError, ValueError, OverflowError):
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="TRADE_POWER_METRIC_EFFECTIVE_COUNT_DRIFT",
                    reason="trade-power effective counts are invalid during validation",
                    regime=regime,
                    strategy_family=candidate_name,
                ))
            if (
                not math.isfinite(train_effective)
                or train_effective < 0.0
                or not math.isfinite(val_effective)
                or val_effective < 0.0
            ):
                raise SearchInfrastructureError(TrialOutcome(
                    kind=SearchOutcomeKind.INFRA_FAILURE,
                    stage=SearchStage.FAMILY_CELL,
                    reason_code="TRADE_POWER_METRIC_EFFECTIVE_COUNT_DRIFT",
                    reason="trade-power effective counts are invalid during validation",
                    regime=regime,
                    strategy_family=candidate_name,
                ))
            train_score_surface = self._effective_count_surface(
                train_metrics, train_effective
            )
            val_score_surface = self._effective_count_surface(
                val_metrics, val_effective
            )
        else:
            train_score_surface = dict(train_metrics)
            val_score_surface = dict(val_metrics)
        train_full = self._bucket_to_full_metrics(train_score_surface)
        val_full = self._bucket_to_full_metrics(val_score_surface)
        robustness = self.scorer.calculate_fx_score_robustness_ratio(
            train_full, val_full, purpose="selection"
        )

        # Check robustness OR Sharpe override
        if robustness < self.min_robustness_ratio and val_sharpe <= self.val_min_sharpe_override:
            return False, f"Low robustness {robustness:.2f} < {self.min_robustness_ratio} and Sharpe {val_sharpe:.2f} <= {self.val_min_sharpe_override}"
        
        return True, f"Validated (PF={val_pf:.2f}, ret={val_return:.1f}%, robustness={robustness:.2f}, dd={val_drawdown:.1f}%)"

    def _build_validation_evidence(self,
                                   train_metrics: Dict[str, Any],
                                   val_metrics: Dict[str, Any],
                                   *,
                                    score: Optional[float] = None,
                                    validation_reason: str = "",
                                    candidate: Optional[Dict[str, Any]] = None,
                                    false_discovery_evidence: Optional[Dict[str, Any]] = None,
                                    legacy_confidence_evidence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build compact, persisted validation evidence for dashboards and review."""
        score_train_metrics = dict(train_metrics or {})
        score_val_metrics = dict(val_metrics or {})
        if (
            isinstance(candidate, Mapping)
            and bool(candidate.get("trade_power_contract_enforced", False))
        ):
            score_train_metrics, score_val_metrics, _ = self._governed_scoring_inputs(
                train_metrics,
                val_metrics,
                candidate,
            )
            if not score_train_metrics or not score_val_metrics:
                raise ValueError("governed validation evidence has invalid effective counts")
        train_full = self._bucket_to_full_metrics(score_train_metrics)
        val_full = self._bucket_to_full_metrics(score_val_metrics)
        robustness = self.scorer.calculate_fx_score_robustness_ratio(
            train_full, val_full, purpose="selection"
        )
        val_return = float(val_metrics.get("total_return_pct", 0.0) or 0.0)
        val_dd = float(val_metrics.get("max_drawdown_pct", 0.0) or 0.0)
        evidence = {
            "robustness_ratio": round(float(robustness), 6),
            "return_dd_ratio": round(self._compute_return_dd_ratio(val_return, val_dd), 6),
            "train_trades": int(train_metrics.get("total_trades", 0) or 0),
            "val_trades": int(val_metrics.get("total_trades", 0) or 0),
            "val_profit_factor": float(val_metrics.get("profit_factor", 0.0) or 0.0),
            "val_return_pct": val_return,
            "val_max_drawdown_pct": val_dd,
            "validation_reason": validation_reason,
            "fresh_edge_policy": {
                "winner_source": (
                    "threadspark_post_search_surface"
                    if str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
                    == "specialist_spokes_threadspark"
                    else "active_recent_stage3"
                ),
                "no_fallback_to_best_train": True,
                "holdout_does_not_displace_latest_surface": bool(
                    getattr(self.config, "walk_forward_do_not_displace_latest_surface", True)
                ) and str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
                != "specialist_spokes_threadspark",
            },
        }
        if score is not None:
            evidence["selection_score_raw"] = float(score)
        if isinstance(candidate, dict):
            candidate_id = str(candidate.get("candidate_id") or "")
            if not candidate_id:
                raise ValueError(
                    "validation evidence requires the exact selected candidate_id"
                )
            try:
                strategy = StrategyRegistry.get(candidate.get("strategy_name", ""), **(candidate.get("params", {}) or {}))
            except Exception:
                strategy = None
            evidence["fairness_evidence"] = build_fairness_evidence(candidate, strategy=strategy)
            oos_evidence = dict(candidate.get("oos_evidence") or {})
            oos_candidate_id = str(oos_evidence.get("candidate_id") or "")
            if oos_candidate_id and oos_candidate_id != candidate_id:
                raise ValueError(
                    "OOS evidence candidate_id does not match the exact selected candidate"
                )
            evidence["candidate_id"] = candidate_id
            evidence["evaluation_regime"] = self._candidate_evaluation_regime(candidate)
            if bool(candidate.get("trade_power_contract_enforced", False)):
                evidence["trade_power"] = {
                    "policy_role": (
                        "declared_material_effect_and_precision_policy"
                    ),
                    "policy": {
                        "minimum_economic_edge_r": (
                            PRODUCTION_TRADE_POWER_POLICY.minimum_economic_edge_r
                        ),
                        "maximum_ci_half_width_r": (
                            PRODUCTION_TRADE_POWER_POLICY.maximum_ci_half_width_r
                        ),
                        "target_power": PRODUCTION_TRADE_POWER_POLICY.target_power,
                        "observed_mean_is_separate_economic_gate": True,
                    },
                    "train": dict(candidate.get("trade_power_train") or {}),
                    "tuning_validation": dict(
                        candidate.get("trade_power_validation") or {}
                    ),
                }
        if isinstance(false_discovery_evidence, dict):
            evidence["false_discovery"] = dict(false_discovery_evidence)
        if isinstance(legacy_confidence_evidence, dict):
            evidence["legacy_confidence"] = dict(legacy_confidence_evidence)
        return evidence

    @staticmethod
    def _geo_mean(values: np.ndarray) -> float:
        arr = np.asarray(values, dtype=np.float64)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return 0.0
        arr = np.clip(arr, 1e-9, 1.0)
        return float(np.exp(np.mean(np.log(arr))))

    @staticmethod
    def _face_balance(values: np.ndarray) -> float:
        arr = np.asarray(values, dtype=np.float64)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return 0.0
        arr = np.clip(arr, 1e-9, 1.0)
        mean = float(np.mean(arr))
        return float(RegimeOptimizer._geo_mean(arr) / mean) if mean > 1e-12 else 0.0

    SPECIALIST_SPOKES = (
        "edge_survival",
        "profit_structure_quality",
        "capital_preservation",
        "tail_risk_control",
        "temporal_consistency",
        "generalization_survival",
        "execution_cost_survival",
        "parameter_stability",
    )
    THREADSPARK_SPOKES = (
        "edge_survival",
        "profit_structure_quality",
        "capital_preservation",
        "tail_risk_control",
        "path_persistence",
        "generalization_survival",
        "execution_cost_survival",
        "parameter_stability",
    )

    @staticmethod
    def _execution_cost_axis(metrics: Dict[str, Any]) -> Tuple[float, str]:
        """Return a non-leaky execution-cost spoke from the current metric surface."""

        ret = float(metrics.get("total_return_pct", 0.0) or 0.0)
        gross_profit_present = "gross_profit" in metrics
        total_pnl_present = "total_pnl" in metrics
        gross_profit = max(0.0, float(metrics.get("gross_profit", 0.0) or 0.0))
        total_pnl = float(metrics.get("total_pnl", 0.0) or 0.0)
        if gross_profit > 1e-9:
            return max(0.0, min(1.0, total_pnl / gross_profit)), "net_to_gross_available"
        if not gross_profit_present and not total_pnl_present and ret > 0.0:
            return 1.0, "not_available_neutral"
        return 0.0, "not_available_zero"

    @staticmethod
    def _surface_spoke_axes(metrics: Dict[str, Any], *, drawdown_limit: float) -> Dict[str, float]:
        """Non-leaky single-metric specialist axes from one evaluation surface."""

        trades = max(0, int(metrics.get("total_trades", 0) or 0))
        ret = float(metrics.get("total_return_pct", 0.0) or 0.0)
        dd = max(0.0, float(metrics.get("max_drawdown_pct", 100.0) or 0.0))
        pf = max(0.0, float(metrics.get("profit_factor", 0.0) or 0.0))
        worst_5 = float(metrics.get("worst_5pct_r", 0.0) or 0.0)
        r_downside = max(0.0, float(metrics.get("r_downside_deviation", 0.0) or 0.0))
        r_block_downside = max(0.0, float(metrics.get("r_block_downside_deviation", 0.0) or 0.0))
        r_block_cvar = max(0.0, float(metrics.get("r_block_cvar", 0.0) or 0.0))
        max_consec = max(0, int(metrics.get("max_consecutive_losses", 0) or 0))
        mean_r = float(metrics.get("mean_r", 0.0) or 0.0)
        pct_positive_r = float(metrics.get("pct_positive_r", metrics.get("win_rate", 0.0)) or 0.0)
        positive_fraction = max(0.0, min(1.0, pct_positive_r / 100.0 if pct_positive_r > 1.0 else pct_positive_r))
        std_weekly = max(0.0, float(metrics.get("std_weekly_return", 0.0) or 0.0))

        edge = ret / (ret + dd + 1.0) if ret > 0.0 else 0.0
        profit_structure = (1.0 - (1.0 / pf)) if pf > 1.0 else 0.0
        boundary = max(1e-9, float(drawdown_limit or 1.0))
        capital_preservation = max(0.0, min(1.0, 1.0 - (dd / boundary)))
        if r_block_downside > 1e-12 or r_block_cvar > 1e-12 or r_downside > 1e-12:
            block_downside = r_block_downside if r_block_downside > 1e-12 else r_downside
            block_cvar = r_block_cvar if r_block_cvar > 1e-12 else max(0.0, -worst_5)
            tail_risk_control = 0.55 * (1.0 / (1.0 + block_downside)) + 0.45 * (1.0 / (1.0 + block_cvar))
        else:
            tail_risk_control = 1.0 / (1.0 + max(0.0, -worst_5))
        temporal_consistency = (
            1.0 / (1.0 + (float(max_consec) / math.sqrt(float(trades))))
            if trades > 0 else 0.0
        )
        edge_reappearance = RegimeOptimizer._sigmoid_score(positive_fraction, center=0.50, scale=0.12)
        expectancy_reappearance = RegimeOptimizer._sigmoid_score(mean_r, center=0.0, scale=0.25)
        variance_drag = math.exp(-min(10.0, std_weekly * 20.0))
        path_persistence = 0.55 * edge_reappearance + 0.25 * expectancy_reappearance + 0.20 * variance_drag
        execution_cost_survival, _cost_status = RegimeOptimizer._execution_cost_axis(metrics)
        return {
            "edge_survival": max(0.0, min(1.0, edge)),
            "profit_structure_quality": max(0.0, min(1.0, profit_structure)),
            "capital_preservation": max(0.0, min(1.0, capital_preservation)),
            "tail_risk_control": max(0.0, min(1.0, tail_risk_control)),
            "temporal_consistency": max(0.0, min(1.0, temporal_consistency)),
            "path_persistence": max(0.0, min(1.0, path_persistence)),
            "execution_cost_survival": max(0.0, min(1.0, execution_cost_survival)),
        }

    @staticmethod
    def _generalization_axis(train_metrics: Dict[str, Any], val_metrics: Dict[str, Any]) -> float:
        train_ret = float(train_metrics.get("total_return_pct", 0.0) or 0.0)
        val_ret = float(val_metrics.get("total_return_pct", 0.0) or 0.0)
        if train_ret <= 0.0 or val_ret <= 0.0:
            return 0.0
        return float(max(0.0, min(1.0, val_ret / train_ret)))

    @staticmethod
    def _dent_health(train_face: np.ndarray, final_face: np.ndarray) -> float:
        ratios = []
        for before, after in zip(np.asarray(train_face, dtype=np.float64), np.asarray(final_face, dtype=np.float64)):
            if before > 1e-9:
                ratios.append(max(0.0, min(1.0, float(after / before))))
            else:
                ratios.append(1.0 if after <= 1e-9 else 0.0)
        return float(np.mean(ratios)) if ratios else 0.0

    @staticmethod
    def _parameter_stability_axis(candidate: Optional[Dict[str, Any]]) -> Tuple[float, str]:
        if not isinstance(candidate, dict):
            return 1.0, "not_available_neutral"
        evidence = candidate.get("parameter_stability_evidence") or {}
        if isinstance(evidence, dict) and evidence.get("status") == "available":
            value = evidence.get("nearest_neighbor_median_score_ratio")
            try:
                return max(0.0, min(1.0, float(value))), "available"
            except Exception:
                return 1.0, "invalid_neutral"
        return 1.0, str(evidence.get("status", "not_available_neutral")) if isinstance(evidence, dict) else "not_available_neutral"

    ASYM_SPECIALIST_SPOKES = (
        "risk_adjusted_edge",
        "trade_quality",
        "gain_pain_efficiency",
        "capital_preservation",
        "tail_risk_control",
        "generalization_survival",
        "execution_cost_survival",
        "parameter_stability",
    )
    ASYM_DIAGNOSTIC_SPOKES = (
        "conditional_upside_capture",
        "path_persistence",
    )
    ASYM_ALPHA_SPOKES = (
        "risk_adjusted_edge",
        "trade_quality",
        "gain_pain_efficiency",
        "generalization_survival",
    )
    ASYM_RISK_SPOKES = (
        "capital_preservation",
        "tail_risk_control",
    )

    @staticmethod
    def _metric_float(metrics: Dict[str, Any], key: str, default: float = 0.0) -> float:
        try:
            raw_value = metrics.get(key, default)
            if raw_value is None:
                raw_value = default
            value = float(raw_value)
            return value if math.isfinite(value) else default
        except Exception:
            return default

    @staticmethod
    def _bounded_positive_metric(metrics: Dict[str, Any],
                                 key: str,
                                 *,
                                 default: float = 0.0,
                                 inf_value: float = 10.0,
                                 cap: float = 10.0) -> float:
        try:
            raw_value = metrics.get(key, default)
            if raw_value is None:
                raw_value = default
            value = float(raw_value)
            if math.isinf(value):
                value = inf_value if value > 0.0 else default
            if math.isnan(value):
                value = default
            return max(0.0, min(float(cap), value))
        except Exception:
            return default

    @staticmethod
    def _sigmoid_score(value: float, *, center: float, scale: float) -> float:
        scale = max(1e-9, float(scale or 1.0))
        x = max(-60.0, min(60.0, (float(value) - float(center)) / scale))
        return float(1.0 / (1.0 + math.exp(-x)))

    @staticmethod
    def _smoothstep_score(value: float, *, floor: float, ceiling: float) -> float:
        if ceiling <= floor:
            return 1.0 if value >= ceiling else 0.0
        x = max(0.0, min(1.0, (float(value) - float(floor)) / float(ceiling - floor)))
        return float(x * x * (3.0 - 2.0 * x))

    @staticmethod
    def _trimmed_mean(values: np.ndarray, trim: float = 0.15) -> float:
        arr = np.asarray(values, dtype=np.float64)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return 0.0
        arr = np.sort(np.clip(arr, 0.0, 1.0))
        n_trim = int(math.floor(arr.size * max(0.0, min(0.45, trim))))
        if n_trim > 0 and arr.size > n_trim * 2:
            arr = arr[n_trim:-n_trim]
        return float(np.mean(arr)) if arr.size else 0.0

    @staticmethod
    def _execution_cost_survival_axis(metrics: Dict[str, Any],
                                      candidate: Optional[Dict[str, Any]]) -> Tuple[float, str]:
        evidence: Any = {}
        if isinstance(candidate, dict):
            evidence = candidate.get("selection_execution_cost_survival") or {}
        if isinstance(evidence, dict) and evidence.get("status") == "available":
            base_ret = RegimeOptimizer._metric_float(evidence, "baseline_return_pct", 0.0)
            stress_ret = RegimeOptimizer._metric_float(evidence, "stress_return_pct", 0.0)
            base_pf = RegimeOptimizer._metric_float(evidence, "baseline_profit_factor", 0.0)
            stress_pf = RegimeOptimizer._metric_float(evidence, "stress_profit_factor", 0.0)
            ret_retention = max(0.0, min(1.0, stress_ret / base_ret)) if base_ret > 1e-9 else 0.0
            pf_retention = max(0.0, min(1.0, stress_pf / base_pf)) if base_pf > 1e-9 else 0.0
            survives = 1.0 if bool(evidence.get("survives_positive_return")) and bool(evidence.get("survives_pf_ge_1")) else 0.0
            score = 0.45 * ret_retention + 0.35 * pf_retention + 0.20 * survives
            return max(0.0, min(1.0, score)), "stress_degradation_available"
        fallback, status = RegimeOptimizer._execution_cost_axis(metrics)
        return fallback, f"{status}_fallback"

    @staticmethod
    def _asym_surface_spoke_axes(metrics: Dict[str, Any],
                                 *,
                                 drawdown_limit: float,
                                 train_metrics: Optional[Dict[str, Any]] = None,
                                 candidate: Optional[Dict[str, Any]] = None,
                                 parameter_stability: float = 1.0) -> Tuple[Dict[str, float], Dict[str, str]]:
        trades = max(0, int(metrics.get("total_trades", 0) or 0))
        ret = RegimeOptimizer._metric_float(metrics, "total_return_pct", 0.0)
        dd = max(0.0, RegimeOptimizer._metric_float(metrics, "max_drawdown_pct", 100.0))
        pf = RegimeOptimizer._bounded_positive_metric(metrics, "profit_factor", default=0.0, inf_value=10.0, cap=10.0)
        calmar = RegimeOptimizer._metric_float(metrics, "calmar_ratio", 0.0)
        sortino = RegimeOptimizer._metric_float(
            metrics,
            "sortino_approx",
            RegimeOptimizer._metric_float(metrics, "sortino_ratio", 0.0),
        )
        mean_r = RegimeOptimizer._metric_float(metrics, "mean_r", 0.0)
        median_r = RegimeOptimizer._metric_float(metrics, "median_r", 0.0)
        pct_positive_r = RegimeOptimizer._metric_float(metrics, "pct_positive_r", metrics.get("win_rate", 0.0)) / 100.0
        worst_5 = RegimeOptimizer._metric_float(metrics, "worst_5pct_r", 0.0)
        r_downside_dev = max(0.0, RegimeOptimizer._metric_float(metrics, "r_downside_deviation", 0.0))
        r_block_downside_dev = max(0.0, RegimeOptimizer._metric_float(metrics, "r_block_downside_deviation", 0.0))
        r_block_cvar = max(0.0, RegimeOptimizer._metric_float(metrics, "r_block_cvar", 0.0))
        max_consec = max(0, int(metrics.get("max_consecutive_losses", 0) or 0))
        std_weekly = max(0.0, RegimeOptimizer._metric_float(metrics, "std_weekly_return", 0.0))

        return_dd = ret / (dd + 1.0) if ret > 0.0 else 0.0
        risk_adjusted_edge = (
            0.45 * RegimeOptimizer._sigmoid_score(return_dd, center=1.0, scale=1.25) +
            0.35 * RegimeOptimizer._sigmoid_score(calmar, center=1.0, scale=2.0) +
            0.20 * RegimeOptimizer._sigmoid_score(sortino, center=0.5, scale=2.0)
        )
        if ret <= 0.0:
            risk_adjusted_edge *= 0.35

        trade_quality = (
            0.55 * RegimeOptimizer._sigmoid_score(mean_r, center=0.05, scale=0.25) +
            0.30 * RegimeOptimizer._sigmoid_score(median_r, center=0.05, scale=0.35) +
            0.15 * RegimeOptimizer._sigmoid_score(pct_positive_r, center=0.50, scale=0.12)
        )
        if mean_r <= 0.0 and median_r <= 0.0:
            trade_quality *= 0.55

        gross_profit = max(0.0, RegimeOptimizer._metric_float(metrics, "gross_profit", 0.0))
        gross_loss = abs(RegimeOptimizer._metric_float(metrics, "gross_loss", 0.0))
        total_pnl = RegimeOptimizer._metric_float(metrics, "total_pnl", ret)
        if gross_loss > 1e-9:
            gain_pain = gross_profit / gross_loss
        else:
            gain_pain = 3.0 if gross_profit > 0.0 else 0.0
        net_to_gross = max(0.0, min(1.0, total_pnl / gross_profit)) if gross_profit > 1e-9 else 0.0
        gain_pain_efficiency = (
            0.70 * RegimeOptimizer._sigmoid_score(gain_pain, center=1.20, scale=0.75) +
            0.30 * RegimeOptimizer._sigmoid_score(net_to_gross, center=0.25, scale=0.25)
        )
        if ret <= 0.0 or total_pnl <= 0.0:
            gain_pain_efficiency *= 0.50

        boundary = max(1e-9, float(drawdown_limit or 1.0))
        capital_preservation = max(0.0, min(1.0, 1.0 - (dd / boundary)))
        if dd <= 1e-12 and 0 < trades < 50:
            confidence = min(1.0, math.sqrt(float(trades) / 50.0))
            capital_preservation = min(capital_preservation, 0.90 + 0.10 * confidence)

        block_downside = r_block_downside_dev if r_block_downside_dev > 1e-12 else r_downside_dev
        block_cvar = r_block_cvar if r_block_cvar > 1e-12 else max(0.0, -worst_5)
        downside_score = 1.0 / (1.0 + math.exp(max(-60.0, min(60.0, (block_downside - 0.65) / 0.25))))
        block_cvar_score = 1.0 / (1.0 + math.exp(max(-60.0, min(60.0, (block_cvar - 1.10) / 0.40))))
        cluster_score = 1.0 / (1.0 + (float(max_consec) / math.sqrt(float(trades)))) if trades > 0 else 0.0
        tail_risk_control = 0.60 * downside_score + 0.30 * block_cvar_score + 0.10 * cluster_score
        if block_downside <= 1e-12 and block_cvar <= 1e-12 and 0 < trades < 50:
            confidence = min(1.0, math.sqrt(float(trades) / 50.0))
            tail_risk_control = min(tail_risk_control, 0.85 + 0.15 * confidence)

        avg_win = max(0.0, RegimeOptimizer._metric_float(metrics, "avg_win", 0.0))
        avg_loss = abs(RegimeOptimizer._metric_float(metrics, "avg_loss", 0.0))
        if avg_loss <= 1e-9:
            payoff_ratio = 3.0 if avg_win > 0.0 else 0.0
        else:
            payoff_ratio = avg_win / avg_loss
        payoff_score = RegimeOptimizer._sigmoid_score(payoff_ratio, center=1.15, scale=0.75)
        upside_base = (
            0.60 * payoff_score +
            0.25 * RegimeOptimizer._sigmoid_score(mean_r, center=0.05, scale=0.25) +
            0.15 * RegimeOptimizer._sigmoid_score(ret, center=2.0, scale=5.0)
        )
        downside_guard = math.sqrt(max(0.0, min(1.0, capital_preservation)) * max(0.0, min(1.0, tail_risk_control)))
        conditional_upside_capture = upside_base * downside_guard

        positive_fraction = max(0.0, min(1.0, pct_positive_r))
        edge_reappearance = RegimeOptimizer._sigmoid_score(positive_fraction, center=0.50, scale=0.12)
        expectancy_reappearance = RegimeOptimizer._sigmoid_score(mean_r, center=0.0, scale=0.25)
        variance_drag = math.exp(-min(10.0, std_weekly * 20.0))
        path_persistence = 0.55 * edge_reappearance + 0.25 * expectancy_reappearance + 0.20 * variance_drag

        if train_metrics is None:
            generalization_survival = 1.0 if ret > 0.0 else 0.0
        else:
            train_ret = RegimeOptimizer._metric_float(train_metrics, "total_return_pct", 0.0)
            train_pf = RegimeOptimizer._bounded_positive_metric(train_metrics, "profit_factor", default=0.0, inf_value=10.0, cap=10.0)
            ret_retention = max(0.0, min(1.0, ret / train_ret)) if train_ret > 0.0 and ret > 0.0 else 0.0
            pf_retention = max(0.0, min(1.0, pf / train_pf)) if train_pf > 1.0 and pf > 1.0 else 0.0
            generalization_survival = 0.70 * ret_retention + 0.30 * pf_retention

        execution_cost_survival, cost_status = RegimeOptimizer._execution_cost_survival_axis(metrics, candidate)
        axes = {
            "risk_adjusted_edge": risk_adjusted_edge,
            "trade_quality": trade_quality,
            "gain_pain_efficiency": gain_pain_efficiency,
            "capital_preservation": capital_preservation,
            "tail_risk_control": tail_risk_control,
            "generalization_survival": generalization_survival,
            "execution_cost_survival": execution_cost_survival,
            "parameter_stability": parameter_stability,
            "conditional_upside_capture": conditional_upside_capture,
            "path_persistence": path_persistence,
        }
        return (
            {name: max(0.0, min(1.0, float(value))) for name, value in axes.items()},
            {"execution_cost_survival": cost_status},
        )

    def _asym_specialist_spoke_vector(self,
                                      train_metrics: Dict[str, Any],
                                      val_metrics: Dict[str, Any],
                                      candidate: Optional[Dict[str, Any]] = None
                                      ) -> Tuple[np.ndarray, np.ndarray, Dict[str, str], Dict[str, float], Dict[str, float]]:
        stability, stability_status = self._parameter_stability_axis(candidate)
        train_axes, train_status = self._asym_surface_spoke_axes(
            train_metrics,
            drawdown_limit=float(getattr(self.config, "train_max_drawdown", 20.0) or 20.0),
            train_metrics=None,
            candidate=candidate,
            parameter_stability=stability,
        )
        val_has_evidence = max(0, int(val_metrics.get("total_trades", 0) or 0)) >= self.min_val_trades
        final_surface = val_metrics if val_has_evidence else train_metrics
        final_axes, final_status = self._asym_surface_spoke_axes(
            final_surface,
            drawdown_limit=float(getattr(self.config, "fx_val_max_drawdown", 12.0) or 12.0)
            if val_has_evidence else float(getattr(self.config, "train_max_drawdown", 20.0) or 20.0),
            train_metrics=train_metrics if val_has_evidence else None,
            candidate=candidate,
            parameter_stability=stability,
        )
        status = {
            name: "available"
            for name in self.ASYM_SPECIALIST_SPOKES
        }
        status["generalization_survival"] = "available" if val_has_evidence else "train_only"
        status["execution_cost_survival"] = final_status.get(
            "execution_cost_survival",
            train_status.get("execution_cost_survival", "unknown"),
        )
        status["parameter_stability"] = stability_status
        return (
            np.asarray([train_axes[name] for name in self.ASYM_SPECIALIST_SPOKES], dtype=np.float64),
            np.asarray([final_axes[name] for name in self.ASYM_SPECIALIST_SPOKES], dtype=np.float64),
            status,
            {name: float(train_axes[name]) for name in self.ASYM_DIAGNOSTIC_SPOKES},
            {name: float(final_axes[name]) for name in self.ASYM_DIAGNOSTIC_SPOKES},
        )

    def _asym_shape_aggregation(self, train_face: np.ndarray, final_face: np.ndarray) -> Dict[str, float]:
        values = {
            name: float(final_face[idx])
            for idx, name in enumerate(self.ASYM_SPECIALIST_SPOKES)
        }
        train_values = {
            name: float(train_face[idx])
            for idx, name in enumerate(self.ASYM_SPECIALIST_SPOKES)
        }
        core_names = [name for name in self.ASYM_SPECIALIST_SPOKES if name != "parameter_stability"]
        core = np.asarray([values[name] for name in core_names], dtype=np.float64)
        central_contour = float(np.median(core)) if core.size else 0.0
        corridor = 0.05
        inward = np.asarray([max(0.0, central_contour - values[name] - corridor) for name in core_names], dtype=np.float64)
        outward = np.asarray([max(0.0, values[name] - central_contour - corridor) for name in self.ASYM_ALPHA_SPOKES], dtype=np.float64)
        inward_divot = float(np.mean(inward)) if inward.size else 0.0
        outward_extension = float(np.mean(outward)) if outward.size else 0.0
        core_level = self._trimmed_mean(core)
        base_shape = max(0.0, min(1.0, core_level + 0.30 * min(1.0, outward_extension / 0.20) - 0.60 * min(1.0, inward_divot / 0.30)))

        capital_gate = self._smoothstep_score(values["capital_preservation"], floor=0.20, ceiling=0.65)
        tail_gate = self._smoothstep_score(values["tail_risk_control"], floor=0.20, ceiling=0.65)
        risk_gate = capital_gate * tail_gate
        generalization_gate = 0.65 + 0.35 * self._smoothstep_score(values["generalization_survival"], floor=0.10, ceiling=0.65)
        cost_gate = 0.75 + 0.25 * self._smoothstep_score(values["execution_cost_survival"], floor=0.10, ceiling=0.65)
        stability_factor = 0.80 + 0.20 * values["parameter_stability"]
        outpush_credit = 1.0 + 0.20 * min(1.0, outward_extension / 0.20)

        critical_dents = []
        for name in ("capital_preservation", "tail_risk_control", "generalization_survival", "execution_cost_survival"):
            before = train_values[name]
            after = values[name]
            critical_dents.append(max(0.0, before - after - corridor))
        critical_dent = float(np.mean(critical_dents)) if critical_dents else 0.0
        dent_health = max(0.65, 1.0 - 0.70 * min(1.0, critical_dent / 0.30))
        score = base_shape * risk_gate * generalization_gate * cost_gate * stability_factor * outpush_credit * dent_health
        return {
            "core_level": float(core_level),
            "central_contour": float(central_contour),
            "inward_divot": float(inward_divot),
            "outward_extension": float(outward_extension),
            "base_shape_quality": float(base_shape),
            "capital_gate": float(capital_gate),
            "tail_gate": float(tail_gate),
            "risk_gate": float(risk_gate),
            "generalization_gate": float(generalization_gate),
            "cost_gate": float(cost_gate),
            "stability_factor": float(stability_factor),
            "outpush_credit": float(outpush_credit),
            "critical_dent": float(critical_dent),
            "dent_health": float(dent_health),
            "directional_topology_score": float(max(0.0, min(1.5, score))),
        }

    def _compute_asym_specialist_spoke_evidence(self,
                                                train_metrics: Dict[str, Any],
                                                val_metrics: Dict[str, Any],
                                                candidate: Optional[Dict[str, Any]] = None
                                                ) -> Dict[str, Any]:
        train_trades = max(0, int(train_metrics.get("total_trades", 0) or 0))
        val_trades = max(0, int(val_metrics.get("total_trades", 0) or 0))
        train_face, final_face, status, train_diagnostics, final_diagnostics = self._asym_specialist_spoke_vector(
            train_metrics,
            val_metrics,
            candidate,
        )
        if val_trades >= self.min_val_trades:
            target = max(1, self.min_val_trades * 2)
            evidence_confidence = min(1.0, math.sqrt(float(val_trades) / float(target)))
        else:
            target = max(1, self.min_train_trades * 2)
            evidence_confidence = 0.70 * min(1.0, math.sqrt(float(train_trades) / float(target)))
        shape = self._asym_shape_aggregation(train_face, final_face)
        score = 100.0 * evidence_confidence * float(shape["directional_topology_score"])
        score = float(score if math.isfinite(score) else 0.0)
        return {
            "spokes": {
                name: float(final_face[idx])
                for idx, name in enumerate(self.ASYM_SPECIALIST_SPOKES)
            },
            "train_spokes": {
                name: float(train_face[idx])
                for idx, name in enumerate(self.ASYM_SPECIALIST_SPOKES)
            },
            "diagnostic_spokes": final_diagnostics,
            "diagnostic_train_spokes": train_diagnostics,
            "spoke_status": status,
            "derived_shape_features": {
                **shape,
                "evidence_confidence": float(evidence_confidence),
            },
            "score": score,
        }

    def _compute_asym_specialist_spoke_score(self,
                                             train_metrics: Dict[str, Any],
                                             val_metrics: Dict[str, Any],
                                             candidate: Optional[Dict[str, Any]] = None) -> float:
        return float(
            self._compute_asym_specialist_spoke_evidence(
                train_metrics,
                val_metrics,
                candidate,
            )["score"]
        )

    def _specialist_spoke_vector(self,
                                 train_metrics: Dict[str, Any],
                                 val_metrics: Dict[str, Any],
                                 candidate: Optional[Dict[str, Any]] = None,
                                 spoke_names: Optional[Tuple[str, ...]] = None
                                 ) -> Tuple[np.ndarray, np.ndarray, Dict[str, str]]:
        names = tuple(spoke_names or self.SPECIALIST_SPOKES)
        train_axes = self._surface_spoke_axes(
            train_metrics,
            drawdown_limit=float(getattr(self.config, "train_max_drawdown", 20.0) or 20.0),
        )
        val_has_evidence = max(0, int(val_metrics.get("total_trades", 0) or 0)) >= self.min_val_trades
        final_surface = val_metrics if val_has_evidence else train_metrics
        final_axes = self._surface_spoke_axes(
            final_surface,
            drawdown_limit=float(getattr(self.config, "fx_val_max_drawdown", 12.0) or 12.0)
            if val_has_evidence else float(getattr(self.config, "train_max_drawdown", 20.0) or 20.0),
        )
        final_axes["generalization_survival"] = (
            self._generalization_axis(train_metrics, val_metrics)
            if val_has_evidence else
            (1.0 if float(train_metrics.get("total_return_pct", 0.0) or 0.0) > 0.0 else 0.0)
        )
        train_axes["generalization_survival"] = 1.0 if float(train_metrics.get("total_return_pct", 0.0) or 0.0) > 0.0 else 0.0
        stability, stability_status = self._parameter_stability_axis(candidate)
        train_axes["parameter_stability"] = stability
        final_axes["parameter_stability"] = stability
        _cost_value, cost_status = self._execution_cost_axis(final_surface)
        status = {
            "edge_survival": "available",
            "profit_structure_quality": "available",
            "capital_preservation": "available",
            "tail_risk_control": "available",
            "temporal_consistency": "available",
            "path_persistence": "available",
            "generalization_survival": "available" if val_has_evidence else "train_only",
            "execution_cost_survival": cost_status,
            "parameter_stability": stability_status,
        }
        return (
            np.asarray([train_axes[name] for name in names], dtype=np.float64),
            np.asarray([final_axes[name] for name in names], dtype=np.float64),
            status,
        )

    def _compute_specialist_spoke_evidence(self,
                                           train_metrics: Dict[str, Any],
                                           val_metrics: Dict[str, Any],
                                           candidate: Optional[Dict[str, Any]] = None,
                                           spoke_names: Optional[Tuple[str, ...]] = None
                                           ) -> Dict[str, Any]:
        names = tuple(spoke_names or self.SPECIALIST_SPOKES)
        train_trades = max(0, int(train_metrics.get("total_trades", 0) or 0))
        val_trades = max(0, int(val_metrics.get("total_trades", 0) or 0))
        train_face, final_face, status = self._specialist_spoke_vector(
            train_metrics,
            val_metrics,
            candidate,
            spoke_names=names,
        )
        if val_trades >= self.min_val_trades:
            target = max(1, self.min_val_trades * 2)
            evidence_confidence = min(1.0, math.sqrt(float(val_trades) / float(target)))
        else:
            target = max(1, self.min_train_trades * 2)
            evidence_confidence = 0.70 * min(1.0, math.sqrt(float(train_trades) / float(target)))

        final_quality = self._geo_mean(final_face)
        train_quality = self._geo_mean(train_face)
        path_quality = 0.5 * (train_quality + final_quality)
        balance = self._face_balance(final_face)
        dent_health = self._dent_health(train_face, final_face)
        score = 100.0 * evidence_confidence * final_quality * path_quality * balance * dent_health
        score = float(score if math.isfinite(score) else 0.0)
        return {
            "spokes": {
                name: float(final_face[idx])
                for idx, name in enumerate(names)
            },
            "train_spokes": {
                name: float(train_face[idx])
                for idx, name in enumerate(names)
            },
            "spoke_status": status,
            "derived_shape_features": {
                "final_face_quality": float(final_quality),
                "path_average_quality": float(path_quality),
                "cross_axis_balance": float(balance),
                "dent_health": float(dent_health),
                "evidence_confidence": float(evidence_confidence),
            },
            "score": score,
        }

    def _compute_specialist_spoke_score(self,
                                        train_metrics: Dict[str, Any],
                                        val_metrics: Dict[str, Any],
                                        candidate: Optional[Dict[str, Any]] = None) -> float:
        """Experimental specialist-first scorer for like-for-like discovery runs.

        This is an optimization-time approximation of the evidence geometry:
        final face quality, path average quality, shape balance, dent health,
        and saturating evidence confidence. It is not a promotion gate.
        """

        return float(
            self._compute_specialist_spoke_evidence(
                train_metrics,
                val_metrics,
                candidate,
            )["score"]
        )

    def _compute_threadspark_l1_score(self,
                                      train_metrics: Dict[str, Any],
                                      val_metrics: Dict[str, Any],
                                      candidate: Optional[Dict[str, Any]] = None) -> float:
        """ThreadSpark shortlist score using the volume-neutral spoke set."""

        return float(
            self._compute_specialist_spoke_evidence(
                train_metrics,
                val_metrics,
                candidate,
                spoke_names=self.THREADSPARK_SPOKES,
            )["score"]
        )

    def _threadspark_face(self,
                          train_metrics: Dict[str, Any],
                          surface_metrics: Dict[str, Any],
                          candidate: Optional[Dict[str, Any]],
                          *,
                          drawdown_limit: float,
                          surface_name: str) -> Tuple[np.ndarray, Dict[str, str]]:
        has_evidence = max(0, int(surface_metrics.get("total_trades", 0) or 0)) >= self.min_val_trades
        axes = self._surface_spoke_axes(
            surface_metrics,
            drawdown_limit=float(drawdown_limit or 1.0),
        )
        axes["generalization_survival"] = (
            self._generalization_axis(train_metrics, surface_metrics)
            if has_evidence else 0.0
        )
        stability, stability_status = self._parameter_stability_axis(candidate)
        axes["parameter_stability"] = stability
        _cost_value, cost_status = self._execution_cost_axis(surface_metrics)
        status = {
            "surface": surface_name,
            "evidence": "available" if has_evidence else "thin_or_missing",
            "execution_cost_survival": cost_status,
            "parameter_stability": stability_status,
        }
        return np.asarray([axes[name] for name in self.THREADSPARK_SPOKES], dtype=np.float64), status

    def _compute_threadspark_spoke_evidence(self,
                                            train_metrics: Dict[str, Any],
                                            val_metrics: Dict[str, Any],
                                            candidate: Optional[Dict[str, Any]] = None,
                                            regime: Optional[str] = None) -> Dict[str, Any]:
        """Post-search candidate x evidence_step x spoke selector evidence.

        Inner Optuna calls may not have post-search surfaces yet; in that case
        this safely falls back to the old two-face spoke approximation. Once
        `_attach_threadspark_selection_evidence_to_candidates()` has run, the
        final selector uses train -> validation -> selection -> historical-cost
        faces as the evidence path.
        """

        if not isinstance(candidate, dict) or not regime:
            evidence = self._compute_specialist_spoke_evidence(train_metrics, val_metrics, candidate)
            evidence["selector_model"] = "specialist_spokes_threadspark_fallback"
            return evidence

        selection_by_regime = candidate.get("threadspark_selection_regime_metrics") or {}
        historical_by_regime = candidate.get("threadspark_historical_regime_metrics") or {}
        historical_chunk_sets = candidate.get("threadspark_historical_chunk_regime_metrics") or []
        selection_metrics = selection_by_regime.get(regime) if isinstance(selection_by_regime, dict) else None
        historical_metrics = historical_by_regime.get(regime) if isinstance(historical_by_regime, dict) else None

        train_face = self._surface_spoke_axes(
            train_metrics,
            drawdown_limit=float(getattr(self.config, "train_max_drawdown", 20.0) or 20.0),
        )
        train_face["generalization_survival"] = (
            1.0 if float(train_metrics.get("total_return_pct", 0.0) or 0.0) > 0.0 else 0.0
        )
        stability, stability_status = self._parameter_stability_axis(candidate)
        train_face["parameter_stability"] = stability
        faces: List[np.ndarray] = [
            np.asarray([train_face[name] for name in self.THREADSPARK_SPOKES], dtype=np.float64)
        ]
        statuses: List[Dict[str, str]] = [{
            "surface": "train",
            "evidence": "available" if int(train_metrics.get("total_trades", 0) or 0) >= self.min_train_trades else "thin_or_missing",
            "execution_cost_survival": self._execution_cost_axis(train_metrics)[1],
            "parameter_stability": stability_status,
        }]

        val_face, val_status = self._threadspark_face(
            train_metrics,
            val_metrics,
            candidate,
            drawdown_limit=float(getattr(self.config, "fx_val_max_drawdown", 12.0) or 12.0),
            surface_name="validation",
        )
        faces.append(val_face)
        statuses.append(val_status)

        final_surface_name = "validation"
        final_metrics = val_metrics
        final_face_index = len(faces) - 1
        selection_missing = bool(candidate.get("threadspark_selection_missing", False))
        if isinstance(selection_metrics, dict) and selection_metrics:
            final_surface_name = str(candidate.get("threadspark_selection_surface", "selection") or "selection")
            final_metrics = selection_metrics
            selection_face, selection_status = self._threadspark_face(
                train_metrics,
                selection_metrics,
                candidate,
                drawdown_limit=float(getattr(self.config, "fx_val_max_drawdown", 12.0) or 12.0),
                surface_name=final_surface_name,
            )
            faces.append(selection_face)
            statuses.append(selection_status)
            final_face_index = len(faces) - 1
        elif selection_missing:
            final_surface_name = str(candidate.get("threadspark_selection_surface", "clean_holdout_missing") or "clean_holdout_missing")
            faces.append(np.zeros(len(self.THREADSPARK_SPOKES), dtype=np.float64))
            statuses.append({
                "surface": final_surface_name,
                "evidence": "missing_requested_surface",
                "execution_cost_survival": "unmeasurable",
                "parameter_stability": stability_status,
            })
            final_face_index = len(faces) - 1

        if isinstance(historical_chunk_sets, list):
            for chunk_idx, chunk_by_regime in enumerate(historical_chunk_sets, start=1):
                if not isinstance(chunk_by_regime, dict):
                    continue
                chunk_metrics = chunk_by_regime.get(regime)
                if not isinstance(chunk_metrics, dict) or int(chunk_metrics.get("total_trades", 0) or 0) <= 0:
                    continue
                chunk_face, chunk_status = self._threadspark_face(
                    train_metrics,
                    chunk_metrics,
                    candidate,
                    drawdown_limit=float(getattr(self.config, "historical_audit_max_drawdown", 35.0) or 35.0),
                    surface_name=f"historical_cost_chunk_{chunk_idx}",
                )
                faces.append(chunk_face)
                statuses.append(chunk_status)

        if isinstance(historical_metrics, dict) and int(historical_metrics.get("total_trades", 0) or 0) > 0:
            historical_face, historical_status = self._threadspark_face(
                train_metrics,
                historical_metrics,
                candidate,
                drawdown_limit=float(getattr(self.config, "historical_audit_max_drawdown", 35.0) or 35.0),
                surface_name="historical_cost",
            )
            faces.append(historical_face)
            statuses.append(historical_status)

        tensor = np.vstack(faces)
        face_qualities = np.asarray([self._geo_mean(face) for face in tensor], dtype=np.float64)
        final_face = tensor[max(0, min(final_face_index, tensor.shape[0] - 1))]
        final_quality = self._geo_mean(final_face)
        path_quality = float(np.mean(face_qualities)) if face_qualities.size else 0.0
        balance = self._face_balance(final_face)
        dent_values = [
            self._dent_health(tensor[idx - 1], tensor[idx])
            for idx in range(1, tensor.shape[0])
        ]
        dent_health = float(min(dent_values)) if dent_values else 0.0
        selection_trades = max(0, int(final_metrics.get("total_trades", 0) or 0))
        if selection_trades >= self.min_val_trades:
            target = max(1, self.min_val_trades * 2)
            evidence_confidence = min(1.0, math.sqrt(float(selection_trades) / float(target)))
        else:
            target = max(1, self.min_train_trades * 2)
            train_trades = max(0, int(train_metrics.get("total_trades", 0) or 0))
            evidence_confidence = 0.55 * min(1.0, math.sqrt(float(train_trades) / float(target)))
        if selection_missing:
            evidence_confidence *= 0.35

        score = 100.0 * evidence_confidence * final_quality * path_quality * balance * dent_health
        score = float(score if math.isfinite(score) else 0.0)
        return {
            "selector_model": "specialist_spokes_threadspark",
            "score": score,
            "spokes": {
                name: float(final_face[idx])
                for idx, name in enumerate(self.THREADSPARK_SPOKES)
            },
            "train_spokes": {
                name: float(tensor[0, idx])
                for idx, name in enumerate(self.THREADSPARK_SPOKES)
            },
            "evidence_steps": [
                {
                    "surface": statuses[idx].get("surface", f"step_{idx}"),
                    "quality": float(face_qualities[idx]) if idx < len(face_qualities) else 0.0,
                    "spokes": {
                        name: float(tensor[idx, spoke_idx])
                        for spoke_idx, name in enumerate(self.THREADSPARK_SPOKES)
                    },
                    "status": statuses[idx],
                }
                for idx in range(tensor.shape[0])
            ],
            "derived_shape_features": {
                "final_face_quality": float(final_quality),
                "path_average_quality": float(path_quality),
                "cross_axis_balance": float(balance),
                "dent_health": float(dent_health),
                "evidence_confidence": float(evidence_confidence),
                "evidence_step_count": int(tensor.shape[0]),
                "final_surface": final_surface_name,
                "selection_surface_available": bool(
                    isinstance(selection_metrics, dict) and bool(selection_metrics)
                ),
                "selection_missing_reason": str(
                    candidate.get("threadspark_selection_missing_reason", "")
                ),
            },
        }

    def _compute_threadspark_spoke_score(self,
                                         train_metrics: Dict[str, Any],
                                         val_metrics: Dict[str, Any],
                                         candidate: Optional[Dict[str, Any]] = None,
                                         regime: Optional[str] = None) -> float:
        return float(
            self._compute_threadspark_spoke_evidence(
                train_metrics,
                val_metrics,
                candidate,
                regime=regime,
            )["score"]
        )

    @staticmethod
    def _cost_frequency_surface(metrics: Dict[str, Any], *, min_trades: int) -> Dict[str, float]:
        trades = max(0, int(metrics.get("total_trades", 0) or 0))
        ret = RegimeOptimizer._metric_float(metrics, "total_return_pct", 0.0)
        pf = RegimeOptimizer._bounded_positive_metric(metrics, "profit_factor", default=0.0, inf_value=10.0, cap=10.0)
        mean_r = RegimeOptimizer._metric_float(metrics, "mean_r", 0.0)
        gross_profit = max(0.0, RegimeOptimizer._metric_float(metrics, "gross_profit", 0.0))
        total_pnl = RegimeOptimizer._metric_float(metrics, "total_pnl", ret)
        net_to_gross = max(0.0, min(1.0, total_pnl / gross_profit)) if gross_profit > 1e-9 else (1.0 if ret > 0.0 else 0.0)
        target_trades = max(1, int(min_trades) * 2)
        excess = max(0.0, (float(trades) - float(target_trades)) / float(target_trades))
        edge_quality = (
            0.40 * net_to_gross +
            0.35 * RegimeOptimizer._smoothstep_score(mean_r, floor=0.0, ceiling=0.25) +
            0.25 * RegimeOptimizer._smoothstep_score(pf, floor=1.0, ceiling=1.8)
        )
        return {
            "trades": float(trades),
            "target_trades": float(target_trades),
            "excess_ratio": float(excess),
            "net_to_gross": float(net_to_gross),
            "mean_r": float(mean_r),
            "profit_factor": float(pf),
            "edge_quality": float(max(0.0, min(1.0, edge_quality))),
        }

    def _specialist_spokes_v2_cost_frequency_factor(self,
                                                    train_metrics: Dict[str, Any],
                                                    val_metrics: Dict[str, Any]) -> Dict[str, Any]:
        val_trades = max(0, int(val_metrics.get("total_trades", 0) or 0))
        val_has_evidence = val_trades >= self.min_val_trades
        train_surface = self._cost_frequency_surface(train_metrics, min_trades=self.min_train_trades)
        validation_surface = (
            self._cost_frequency_surface(val_metrics, min_trades=self.min_val_trades)
            if val_has_evidence else None
        )
        strength = float(getattr(self.config, "specialist_spokes_v2_cost_frequency_strength", 0.50) or 0.0)

        def _surface_factor(surface: Dict[str, float]) -> float:
            weakness = max(0.0, 0.75 - float(surface["edge_quality"])) / 0.75
            excess_pressure = min(1.0, float(surface["excess_ratio"]))
            factor = 1.0 - strength * excess_pressure * weakness
            return float(max(0.35, min(1.0, factor)))

        train_factor = _surface_factor(train_surface)
        validation_factor = _surface_factor(validation_surface) if validation_surface is not None else 1.0
        factor = min(train_factor, validation_factor)
        limiting_surface = "train" if train_factor <= validation_factor else "validation"
        active_surface = validation_surface if val_has_evidence else train_surface
        return {
            **dict(active_surface),
            "surface": "validation" if val_has_evidence else "train",
            "strength": float(strength),
            "train": {**train_surface, "factor": train_factor},
            "validation": (
                {**validation_surface, "factor": validation_factor}
                if validation_surface is not None else None
            ),
            "limiting_surface": limiting_surface,
            "factor": float(factor),
        }

    def _compute_specialist_spoke_v2_evidence(self,
                                              train_metrics: Dict[str, Any],
                                              val_metrics: Dict[str, Any],
                                              candidate: Optional[Dict[str, Any]] = None
                                              ) -> Dict[str, Any]:
        evidence = self._compute_specialist_spoke_evidence(train_metrics, val_metrics, candidate)
        cost_frequency = self._specialist_spokes_v2_cost_frequency_factor(train_metrics, val_metrics)
        base_score = float(evidence.get("score", 0.0) or 0.0)
        score = base_score * float(cost_frequency["factor"])
        evidence["score"] = float(score if math.isfinite(score) else 0.0)
        evidence["derived_shape_features"] = {
            **dict(evidence.get("derived_shape_features", {}) or {}),
            "base_old_spoke_score": float(base_score),
            "cost_frequency_factor": float(cost_frequency["factor"]),
        }
        evidence["cost_frequency_evidence"] = cost_frequency
        return evidence

    def _compute_specialist_spoke_v2_score(self,
                                           train_metrics: Dict[str, Any],
                                           val_metrics: Dict[str, Any],
                                           candidate: Optional[Dict[str, Any]] = None) -> float:
        return float(
            self._compute_specialist_spoke_v2_evidence(
                train_metrics,
                val_metrics,
                candidate,
            )["score"]
        )

    def _compute_train_regime_score(
        self,
        train_metrics: Dict[str, Any],
        _unused_validation_metrics: Optional[Dict[str, Any]] = None,
    ) -> float:
        """One-surface score for B0, shortlist, and train-only studies.

        Final configured scoring models may use tuning/generalization spokes;
        those are intentionally unavailable before parameters are frozen.
        """
        metrics = dict(train_metrics or {})
        trades = float(
            metrics.get(
                "effective_trades",
                metrics.get("total_trades", 0),
            )
            or 0.0
        )
        if not math.isfinite(trades) or trades < 0.0:
            return float("-inf")
        scoring_surface = (
            self._effective_count_surface(metrics, trades)
            if "effective_trades" in metrics
            else metrics
        )
        train_full = self._bucket_to_full_metrics(scoring_surface)
        base_score = float(self.scorer.score(train_full, purpose="selection"))
        target = max(1, int(self.min_train_trades or 0) * 2)
        stability = min(1.0, math.log1p(trades) / math.log1p(target))
        floor = max(
            0.0,
            min(1.0, float(getattr(self.config, "regime_trade_stability_floor", 0.7))),
        )
        return base_score * (floor + (1.0 - floor) * stability)

    def _compute_regime_score(self,
                               train_metrics: Dict[str, Any],
                               val_metrics: Dict[str, Any],
                               candidate: Optional[Dict[str, Any]] = None) -> float:
        """
        Compute score for a regime bucket using fx_backtester generalization scoring.

        Uses:
        - Gap penalty for train->val degradation
        - Robustness ratio boost
        - Validation-first scoring
        - Trade count stability factor (Gap C fix)
        """
        # ===== FIX Gap C: Trade count stability factor =====
        # Prefer candidates with more trades (more statistical confidence)
        # Uses log scaling: log1p(trades) / log1p(target) capped at 1.0
        power_governed = bool(
            isinstance(candidate, Mapping)
            and candidate.get("trade_power_contract_enforced", False)
        )
        if power_governed:
            try:
                train_trades = float(
                    (candidate.get("trade_power_train") or {}).get(
                        "observed_effective_trades"
                    )
                )
                val_trades = float(
                    (candidate.get("trade_power_validation") or {}).get(
                        "observed_effective_trades"
                    )
                )
            except (TypeError, ValueError, OverflowError):
                return float("-inf")
            if (
                not math.isfinite(train_trades)
                or train_trades < 0.0
                or not math.isfinite(val_trades)
                or val_trades < 0.0
            ):
                return float("-inf")
        else:
            train_trades = float(train_metrics.get('total_trades', 0) or 0)
            val_trades = float(val_metrics.get('total_trades', 0) or 0)

        scoring_train, scoring_val, scoring_candidate = (
            self._governed_scoring_inputs(
                train_metrics,
                val_metrics,
                candidate,
            )
        )
        if power_governed and (
            not scoring_train or not scoring_val or scoring_candidate is None
        ):
            return float("-inf")

        scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
        if scoring_model == "specialist_spokes":
            return self._compute_specialist_spoke_score(
                scoring_train, scoring_val, scoring_candidate
            )
        if scoring_model == "specialist_spokes_threadspark":
            return self._compute_threadspark_spoke_score(
                scoring_train, scoring_val, scoring_candidate
            )
        if scoring_model == "specialist_spokes_v2":
            return self._compute_specialist_spoke_v2_score(
                scoring_train, scoring_val, scoring_candidate
            )
        if scoring_model == "specialist_spokes_asym":
            return self._compute_asym_specialist_spoke_score(
                scoring_train, scoring_val, scoring_candidate
            )

        # Target trade counts for full stability bonus
        # (2x minimum is considered "healthy")
        target_train = self.min_train_trades * 2
        target_val = self.min_val_trades * 2

        # Calculate stability factors using log scaling
        train_stability = min(1.0, math.log1p(train_trades) / math.log1p(target_train)) if target_train > 0 else 0.5
        val_stability = min(1.0, math.log1p(val_trades) / math.log1p(target_val)) if target_val > 0 else 0.5

        # Combined stability factor (weighted average favoring validation)
        stability_factor = 0.3 * train_stability + 0.7 * val_stability
        # Legacy floor is 0.7. Phase-1 can raise this to test less raw-count reward
        # without changing the trade-count hard floors.
        stability_floor = max(0.0, min(1.0, float(getattr(self.config, "regime_trade_stability_floor", 0.7))))
        stability_factor = stability_floor + (1.0 - stability_floor) * stability_factor

        # Build pseudo-metrics dicts for the scorer (regime bucket metrics -> full metrics format)
        train_full = self._bucket_to_full_metrics(scoring_train)
        val_full = self._bucket_to_full_metrics(scoring_val)

        # Use validation metrics if sufficient trades
        if power_governed or val_trades >= self.min_val_trades:
            # Use fx_generalization_score for proper gap penalty + robustness boost
            final_score, _, _, _ = self.scorer.fx_generalization_score(
                train_full, val_full, purpose="selection"
            )
            return final_score * stability_factor

        # Not enough validation trades - use train score with discount
        return self.scorer.score(train_full, purpose="selection") * 0.7 * stability_factor
    
    def _bucket_to_full_metrics(self, bucket_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert regime bucket metrics to full metrics format expected by StrategyScorer.
        
        Maps bucket fields to standard backtest result fields.
        Now uses properly computed max_drawdown_pct and total_return_pct from bucket.
        """
        # Get properly computed values (now available from _compute_bucket_metrics)
        max_dd = bucket_metrics.get('max_drawdown_pct', 100.0)  # Default to worst-case, not 0
        total_return = bucket_metrics.get('total_return_pct', 0.0)  # Now properly computed
        
        return {
            'total_trades': bucket_metrics.get('total_trades', 0),
            'win_rate': bucket_metrics.get('win_rate', 0),
            'profit_factor': bucket_metrics.get('profit_factor', 0),
            'total_return_pct': total_return,  # FIX: Use properly computed value
            'sharpe_ratio': bucket_metrics.get(
                'sharpe_ratio', bucket_metrics.get('sharpe_approx', 0)
            ),
            'max_drawdown_pct': max_dd,  # FIX: Use properly computed value (default worst-case)
            'gross_profit': bucket_metrics.get('gross_profit', 0),
            'gross_loss': bucket_metrics.get('gross_loss', 0),
            'total_pnl': bucket_metrics.get('total_pnl', 0),
            'expectancy_pips': bucket_metrics.get(
                'expectancy_pips',
                bucket_metrics.get('avg_win_pips', 0) * bucket_metrics.get('win_rate', 0) / 100
                - bucket_metrics.get('avg_loss_pips', 0) * (1 - bucket_metrics.get('win_rate', 0) / 100),
            ),
            'sortino_ratio': bucket_metrics.get(
                'sortino_ratio', bucket_metrics.get('sortino_approx', 0)
            ),
            'calmar_ratio': bucket_metrics.get('calmar_ratio', 0),
            'std_weekly_return': bucket_metrics.get('std_weekly_return', 0.0),
            'r_count': bucket_metrics.get('r_count', bucket_metrics.get('total_trades', 0)),
            'mean_r': bucket_metrics.get('mean_r', 0),
            'median_r': bucket_metrics.get('median_r', 0),
            'std_r': bucket_metrics.get('std_r', 0),
            'se_r': bucket_metrics.get('se_r', 0),
            'edge_t_stat_r': bucket_metrics.get('edge_t_stat_r', 0),
            'pct_positive_r': bucket_metrics.get('pct_positive_r', 0),
            'worst_5pct_r': bucket_metrics.get('worst_5pct_r', 0),
            'r_downside_deviation': bucket_metrics.get('r_downside_deviation', 0),
            'r_block_downside_deviation': bucket_metrics.get('r_block_downside_deviation', 0),
            'r_block_cvar': bucket_metrics.get('r_block_cvar', 0),
            'max_consecutive_losses': bucket_metrics.get('max_consecutive_losses', 0),
        }
    
    def _normalize_score(self, score: float) -> float:
        """
        Normalize score to 0-1 range using sigmoid mapping.

        This prevents saturation at extreme scores.
        Calibrated for the continuous-DD scoring distribution where
        typical scores land in the 25-55 range.
        """
        try:
            score = float(score)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if not math.isfinite(score):
            return 0.0

        # Sigmoid centered at 45 with scale 30
        # score=15 -> ~0.27, score=30 -> ~0.38, score=45 -> 0.5,
        # score=60 -> ~0.62, score=80 -> ~0.76
        try:
            normalized = 1.0 / (1.0 + math.exp(-(score - 45) / 30))
        except OverflowError:
            normalized = 0.0 if score < 0 else 1.0

        return max(0.0, min(1.0, normalized))


# =============================================================================
# MAIN PIPELINE
# =============================================================================

class OptimizationPipeline:
    """
    Main optimization pipeline with regime-aware strategy selection.
    
    Orchestrates all stages:
    1. Data loading and splitting
    2. Feature computation (including regime detection)
    3. Regime-aware strategy selection per (timeframe, regime)
    4. Retrain period selection
    5. Validation and config generation
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        FeatureComputer.configure_cache(
            getattr(config, "feature_cache_max_entries", 24),
            disk_enabled=getattr(config, "feature_disk_cache_enabled", False),
            disk_dir=getattr(config, "feature_disk_cache_dir", ""),
            disk_max_gb=getattr(config, "feature_disk_cache_max_gb", 0.0),
            disk_min_free_gb=getattr(config, "feature_disk_cache_min_free_gb", 0.0),
        )
        self.data_loader = DataLoader(config.data_dir)
        self.splitter = DataSplitter(config)
        self.enhancement_seams: EnhancementSeams = create_default_enhancement_seams(config)
        
        # Legacy pipeline surfaces removed from production path (F1).
        # Classes StrategySelector, HyperparameterOptimizer, Validator, and
        # RetrainPeriodSelector remain defined for offline research use but are
        # no longer instantiated by the production pipeline.

        # Regime-aware optimizer (sole production path)
        self.regime_optimizer = RegimeOptimizer(config)
        registry_path = str(
            getattr(config, "lockbox_access_registry_path", "") or ""
        ).strip()
        if not registry_path:
            registry_path = str(Path(config.output_dir) / "lockbox_access_registry.json")
        self.lockbox_access_registry = LockboxAccessRegistry(Path(registry_path))

    @staticmethod
    def _locate_default_origin(default_config: Optional[RegimeConfig],
                               regime_configs: Dict[str, Dict[str, RegimeConfig]]
                               ) -> Tuple[Optional[str], Optional[str]]:
        """Locate the timeframe/regime source of the chosen default config."""
        if default_config is None:
            return None, None
        for tf, regimes in regime_configs.items():
            for regime, cfg in regimes.items():
                if cfg is default_config:
                    return tf, regime
        for tf, regimes in regime_configs.items():
            for regime, cfg in regimes.items():
                if (
                    cfg.strategy_name == default_config.strategy_name and
                    cfg.parameters == default_config.parameters
                ):
                    return tf, regime
        return None, None

    @staticmethod
    def _index_at(df: pd.DataFrame, idx: int) -> Optional[pd.Timestamp]:
        """Return the timestamp at an integer boundary, or None for open-ended slices."""
        if df is None or len(df) == 0:
            return None
        if idx <= 0:
            return pd.Timestamp(df.index[0])
        if idx >= len(df):
            return None
        return pd.Timestamp(df.index[int(idx)])

    @staticmethod
    def _slice_time_window(df: pd.DataFrame,
                           start_ts: Optional[pd.Timestamp],
                           end_ts: Optional[pd.Timestamp]) -> pd.DataFrame:
        """Slice by a source-bar-clean half-open window ``[start_ts, end_ts)``.

        Higher-timeframe candles carry the first and last M5 source timestamps
        used to build them.  Requiring the complete source interval to fall in
        the requested partition prevents a candle from leaking observations
        across train/validation/lockbox boundaries.
        """
        if df is None or len(df) == 0:
            return pd.DataFrame()
        mask = pd.Series(True, index=df.index)
        first_source = (
            pd.to_datetime(df["_SOURCE_FIRST_TS"], errors="coerce")
            if "_SOURCE_FIRST_TS" in df.columns else pd.Series(df.index, index=df.index)
        )
        source_last_is_open = "_SOURCE_LAST_TS" in df.columns
        native_close_is_boundary = (
            not source_last_is_open and "_BAR_CLOSE_TS" in df.columns
        )
        if source_last_is_open:
            last_source = pd.to_datetime(
                df["_SOURCE_LAST_TS"], errors="coerce"
            )
        elif native_close_is_boundary:
            last_source = pd.to_datetime(
                df["_BAR_CLOSE_TS"], errors="coerce", utc=True
            )
        else:
            last_source = pd.Series(df.index, index=df.index)
        if start_ts is not None:
            mask &= first_source >= start_ts
        if end_ts is not None:
            if native_close_is_boundary:
                mask &= last_source <= end_ts
            else:
                mask &= last_source < end_ts
        return df.loc[mask].copy()

    @classmethod
    def _strategy_evaluation_context(
        cls,
        df: pd.DataFrame,
        *,
        context_start: Optional[pd.Timestamp],
        evaluation_start: Optional[pd.Timestamp],
        evaluation_end: Optional[pd.Timestamp],
    ) -> Tuple[pd.DataFrame, int]:
        """Return causal prefix plus a non-scoring evaluation boundary.

        Parameterized strategies must see authenticated rows preceding a scored
        partition so recursive/discrete state matches a full-prefix run.  The
        returned warm-up count is passed unchanged to ``Backtester.run``; rows
        before ``evaluation_start`` may initialize state but cannot open trades.
        """
        start = context_start if context_start is not None else evaluation_start
        if start is not None and evaluation_start is not None:
            if pd.Timestamp(start) > pd.Timestamp(evaluation_start):
                raise ValueError("strategy context cannot start after evaluation")
        frame = cls._slice_time_window(df, start, evaluation_end)
        if frame.empty or evaluation_start is None:
            return frame, 0
        warmup_bars = int(
            (pd.DatetimeIndex(frame.index) < pd.Timestamp(evaluation_start)).sum()
        )
        return frame, warmup_bars

    @classmethod
    def _selection_feature_input(
        cls,
        data: pd.DataFrame,
        *,
        split_mode: str,
        final_lockbox_start: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """Return data on which Phase-A features are legally computable."""
        if (
            str(split_mode or "").lower() == CALENDAR_SPLIT_MODE
            and final_lockbox_start is not None
        ):
            return cls._slice_time_window(data, None, final_lockbox_start)
        return data

    @staticmethod
    def _candidate_eligibility_bounds(
        split_mode: str,
        base_start: Optional[pd.Timestamp],
        base_end: Optional[pd.Timestamp],
        optimizer_train_start: Optional[pd.Timestamp],
        optimizer_train_end: Optional[pd.Timestamp],
    ) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Return the only time range candidate screening may inspect."""
        if str(split_mode or "").lower() in {
            "chronological_lockbox_v1",
            CALENDAR_SPLIT_MODE,
        }:
            return optimizer_train_start, optimizer_train_end
        return base_start, base_end

    def _timeframe_evidence_verdict(
        self,
        timeframe: str,
        partition_frames: Dict[str, pd.DataFrame],
        data_window_meta: Dict[str, Any],
    ) -> EvidenceVerdict:
        """Return the necessary pre-search evidence verdict for one timeframe."""
        source_meta = dict(
            (data_window_meta.get("timeframe_sources", {}) or {}).get(timeframe, {}) or {}
        )
        tf_window_payload = dict(
            (data_window_meta.get("timeframe_windows", {}) or {}).get(
                str(timeframe).upper(), {}
            )
            or {}
        )
        windows = dict(
            tf_window_payload.get("windows")
            or data_window_meta.get("windows", {})
            or {}
        )
        partition_bounds: Dict[str, Tuple[Any, Any]] = {}
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
            meta = dict(windows.get(partition, {}) or {})
            start = meta.get("start_ts")
            end = meta.get("end_ts") or meta.get("end_ts_exclusive")
            if start not in (None, "") and end not in (None, ""):
                partition_bounds[partition] = (start, end)
        calendar_contract = dict(
            data_window_meta.get("native_calendar_contract", {}) or {}
        )
        is_v3 = str(
            tf_window_payload.get("split_contract_version")
            or calendar_contract.get("window_contract_version")
            or ""
        ) == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        return validate_timeframe_evidence(
            timeframe,
            partition_frames,
            source_meta=source_meta,
            window_years=(
                None if is_v3 else getattr(self.config, "evidence_window_years", None)
            ),
            timeframe_window_years=(
                calendar_contract.get("timeframe_window_years")
                or getattr(self.config, "evidence_timeframe_window_years", None)
                if is_v3
                else None
            ),
            min_native_bars=getattr(self.config, "evidence_min_native_bars", None),
            partition_bounds=partition_bounds,
            require_native=bool(
                getattr(self.config, "evidence_require_native_timeframes", True)
            ),
            require_provenance=bool(
                getattr(self.config, "evidence_require_native_provenance", True)
            ),
            require_session_coverage=bool(
                getattr(self.config, "evidence_require_session_coverage", True)
            ),
            min_expected_session_coverage=float(
                getattr(self.config, "evidence_min_expected_session_coverage", 0.95)
            ),
            boundary_tolerance_days=float(
                getattr(self.config, "evidence_boundary_tolerance_days", 7.0)
            ),
        )

    # _evidence_no_trade_markers was deleted (seq961/seq972): a technical
    # EvidenceVerdict failure must never manufacture NO_TRADE leaves.

    def _native_cell_exclusion_markers(
        self,
        binding: Dict[str, Any],
    ) -> Dict[str, RegimeConfig]:
        """Create four immutable, activation-bound NO_TRADE regime leaves.

        Native availability is a (symbol, timeframe) decision.  Binding the
        decision to each regime leaf keeps the ledger's complete 6x4 shape
        without pretending that an unavailable timeframe entered model search.
        """
        exact_binding = dict(binding or {})
        reasons = [str(value) for value in exact_binding.get("reason_codes", []) or []]
        reason = (
            "Native timeframe excluded before model search: "
            + (", ".join(reasons) or "manifest-bound source insufficiency")
        )
        now = datetime.now()
        regimes = tuple(getattr(self.regime_optimizer, "REGIMES", ()) or ())
        if not regimes:
            regimes = ("TREND", "RANGE", "BREAKOUT", "CHOP")
        markers: Dict[str, RegimeConfig] = {}
        for regime in regimes:
            marker = self.regime_optimizer._make_no_trade_marker(reason, now=now)
            marker.artifact_meta = {
                **dict(marker.artifact_meta or {}),
                "native_cell_exclusion_binding": exact_binding,
            }
            marker.validation_evidence = {
                **dict(marker.validation_evidence or {}),
                "native_cell_exclusion_binding": exact_binding,
            }
            markers[str(regime).upper()] = marker
        return markers

    def _build_calendar_workflow_meta(
        self,
        base_m5: pd.DataFrame,
        *,
        as_of_utc: Any,
        base_timeframe: str = "M5",
        calendar_contract: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build common timestamp windows without fixed-M5-row truncation.

        When an authenticated activation calendar is supplied it is the time
        authority.  The reference frame may then be any active native
        timeframe; M5 is not required merely to recover the common cutoff.
        """
        ordered = ensure_utc_index(base_m5, copy=False)
        if ordered.empty:
            return {}
        contract = dict(calendar_contract or {})
        contract_as_of = contract.get("common_as_of_utc")
        as_of = pd.Timestamp(contract_as_of if contract_as_of not in (None, "") else as_of_utc)
        if as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        else:
            as_of = as_of.tz_convert("UTC")
        purge = derive_purge_duration(
            configured_days=getattr(self.config, "evidence_purge_days", 7.0),
            split_embargo_bars=getattr(self.config, "split_embargo_bars", 0),
        )
        contract_version = str(
            contract.get("window_contract_version") or EVIDENCE_CONTRACT_VERSION
        ).strip()
        timeframe_boundary_map = contract.get("timeframe_calendar_boundaries")
        if contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            if not isinstance(timeframe_boundary_map, dict):
                raise ValueError("authenticated v3 timeframe boundary map is missing")
            raw_boundaries = timeframe_boundary_map.get(
                str(base_timeframe or "M5").upper()
            )
            if not isinstance(raw_boundaries, dict):
                raise ValueError(
                    f"authenticated timeframe boundary missing: {base_timeframe}"
                )
        else:
            raw_boundaries = contract.get("common_calendar_boundaries")
        if isinstance(raw_boundaries, dict):
            parsed_boundaries: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}
            for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
                raw = raw_boundaries.get(partition)
                if not isinstance(raw, dict):
                    raise ValueError(f"authenticated calendar boundary missing: {partition}")
                start = pd.Timestamp(raw.get("start_ts"))
                end = pd.Timestamp(raw.get("end_ts"))
                start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
                end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
                if end <= start:
                    raise ValueError(f"authenticated calendar boundary is non-positive: {partition}")
                parsed_boundaries[partition] = (start, end)
            train_start, train_end = parsed_boundaries["optimizer_train"]
            validation_start, validation_end = parsed_boundaries["tuning_validation"]
            lockbox_start, lockbox_end = parsed_boundaries["final_lockbox"]
            boundaries_payload: Dict[str, Any] = {
                "contract_version": contract_version,
                "as_of_utc": as_of.isoformat(),
                "embargo_seconds": float(purge.total_seconds()),
                "windows": {
                    name: {
                        "start_ts": start.isoformat(),
                        "end_ts": end.isoformat(),
                    }
                    for name, (start, end) in parsed_boundaries.items()
                },
                "source": "authenticated_native_activation",
            }
        else:
            boundaries = build_calendar_boundaries(
                as_of,
                window_years=getattr(self.config, "evidence_window_years", None),
                embargo=purge,
            )
            train_start, train_end = boundaries.windows["optimizer_train"]
            validation_start, validation_end = boundaries.windows["tuning_validation"]
            lockbox_start, lockbox_end = boundaries.windows["final_lockbox"]
            boundaries_payload = boundaries.to_dict()
        first_ts = pd.Timestamp(ordered.index[0])
        warmup = pd.Timedelta(
            days=max(0.0, float(getattr(self.config, "evidence_warmup_days", 400.0)))
        )

        def _clip_start(value: pd.Timestamp) -> pd.Timestamp:
            return max(first_ts, pd.Timestamp(value))

        optimizer_warm_start = _clip_start(train_start - warmup)
        validation_warm_start = _clip_start(validation_start - warmup)
        lockbox_warm_start = _clip_start(lockbox_start - warmup)

        intervals: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {
            "workflow_window": (first_ts, as_of),
            "historical_stress_audit": (first_ts, train_start),
            "active_universe": (train_start, as_of),
            "base_selection": (train_start, as_of),
            "optimizer_train": (train_start, train_end),
            "optimizer_train_warmup": (optimizer_warm_start, train_start),
            "optimizer_train_with_warmup": (optimizer_warm_start, train_end),
            "embargo_train_validation": (train_end, validation_start),
            "tuning_validation": (validation_start, validation_end),
            "tuning_validation_warmup": (validation_warm_start, validation_start),
            "tuning_validation_with_warmup": (validation_warm_start, validation_end),
            "embargo_validation_lockbox": (validation_end, lockbox_start),
            "final_lockbox": (lockbox_start, lockbox_end),
            "final_lockbox_warmup": (lockbox_warm_start, lockbox_start),
            "final_lockbox_with_warmup": (lockbox_warm_start, lockbox_end),
            # Compatibility aliases retained for downstream consumers.
            "optimization_train": (train_start, train_end),
            "optimization_train_with_warmup": (optimizer_warm_start, train_end),
            "walk_forward_validation": (validation_start, validation_end),
            "stage2_selection": (validation_start, validation_end),
            "stage2_selection_with_warmup": (validation_warm_start, validation_end),
            "stage2_training_selection": (validation_start, validation_end),
            "stage2_training_selection_with_warmup": (validation_warm_start, validation_end),
            "clean_holdout": (lockbox_start, lockbox_end),
            "clean_holdout_with_warmup": (lockbox_warm_start, lockbox_end),
        }
        # A short or newly activated file can begin after one or more calendar
        # boundaries.  Those missing periods are evidence failures, not
        # metadata-construction exceptions.  Represent a wholly pre-history
        # interval as an explicit empty half-open window and let the native
        # evidence gate report the missing span/bar count.
        intervals = {
            name: ((end, end) if end < start else (start, end))
            for name, (start, end) in intervals.items()
        }
        windows = {
            name: calendar_window_meta(ordered, start, end)
            for name, (start, end) in intervals.items()
        }
        return {
            "base_timeframe": str(base_timeframe or "M5").upper(),
            "production_split_mode": CALENDAR_SPLIT_MODE,
            "split_contract_version": contract_version,
            "evidence_contract_version": contract_version,
            "as_of_utc": as_of.isoformat(),
            "max_bars": int(getattr(self.config, "max_bars", 300000)),
            "max_bars_applied": False,
            "workflow_rows": int(len(ordered)),
            "historical_stress_audit_bars": int(
                getattr(self.config, "historical_stress_audit_bars", 50000)
            ),
            "active_universe_bars": int(getattr(self.config, "active_universe_bars", 250000)),
            "evidence_purge_days": float(purge.total_seconds() / 86_400.0),
            "evidence_purge_seconds": float(purge.total_seconds()),
            "evidence_warmup_days": float(
                getattr(self.config, "evidence_warmup_days", 400.0)
            ),
            "evidence_window_years": (
                dict(
                    (contract.get("timeframe_window_years") or {}).get(
                        str(base_timeframe or "M5").upper()
                    )
                    or {}
                )
                if contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                else dict(getattr(self.config, "evidence_window_years", {}) or {})
            ),
            "calendar_boundaries": boundaries_payload,
            "native_calendar_contract": contract,
            "windows": windows,
        }

    def _build_m5_workflow_meta(
        self,
        base_m5: pd.DataFrame,
        *,
        as_of_utc: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Build per-symbol workflow metadata from the base M5 data window."""
        split_mode = str(
            getattr(self.config, "production_split_mode", CALENDAR_SPLIT_MODE)
            or CALENDAR_SPLIT_MODE
        ).strip().lower()
        if split_mode == CALENDAR_SPLIT_MODE:
            cutoff = as_of_utc
            if cutoff in (None, ""):
                configured = str(getattr(self.config, "evidence_as_of_utc", "") or "").strip()
                not_after = configured or datetime.now(timezone.utc)
                cutoff = latest_closed_bar_end(
                    base_m5,
                    "M5",
                    not_after_utc=not_after,
                )
            if cutoff is None:
                return {}
            return self._build_calendar_workflow_meta(base_m5, as_of_utc=cutoff)

        indices = self.splitter.get_workflow_indices(len(base_m5))

        def _meta(name: str) -> Dict[str, Any]:
            start, end = indices[name]
            frame = base_m5.iloc[start:end]
            end_exclusive = (
                base_m5.index[int(end)].isoformat()
                if 0 <= int(end) < len(base_m5)
                else None
            )
            return {
                "start_index": int(start),
                "end_index": int(end),
                "rows": int(max(0, end - start)),
                "start_ts": frame.index[0].isoformat() if len(frame) else None,
                # `end_ts` remains the compatibility field, but is now truly
                # exclusive to match _slice_time_window's contract.
                "end_ts": end_exclusive,
                "end_ts_exclusive": end_exclusive,
                "end_ts_inclusive": frame.index[-1].isoformat() if len(frame) else None,
            }

        return {
            "base_timeframe": "M5",
            "split_contract_version": DataSplitter.SPLIT_CONTRACT_VERSION,
            "max_bars_applied": True,
            "max_bars": int(getattr(self.config, "max_bars", 300000)),
            "historical_stress_audit_bars": int(getattr(self.config, "historical_stress_audit_bars", 50000)),
            "active_universe_bars": int(getattr(self.config, "active_universe_bars", 250000)),
            "active_stage2_pct": float(getattr(self.config, "active_stage2_pct", 50.0)),
            "production_split_mode": str(
                getattr(self.config, "production_split_mode", CALENDAR_SPLIT_MODE)
                or CALENDAR_SPLIT_MODE
            ),
            "tuning_validation_bars": int(getattr(self.config, "tuning_validation_bars", 50000) or 0),
            "final_lockbox_bars": int(getattr(self.config, "final_lockbox_bars", 50000) or 0),
            "split_embargo_bars": int(getattr(self.config, "split_embargo_bars", 300) or 0),
            "walk_forward_validation_bars": int(getattr(self.config, "walk_forward_validation_bars", 50000) or 0),
            "clean_holdout_pct": float(getattr(self.config, "clean_holdout_pct", 0.0)),
            "effective_clean_holdout_pct": (
                0.0
                if str(getattr(self.config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout").lower()
                == "purged_kfold"
                else float(getattr(self.config, "clean_holdout_pct", 0.0))
            ),
            "oos_validation_mode": str(getattr(self.config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout"),
            "windows": {
                "workflow_window": _meta("workflow_window"),
                "historical_stress_audit": _meta("historical_stress_audit"),
                "active_universe": _meta("active_universe"),
                "base_selection": _meta("base_selection"),
                "walk_forward_validation": _meta("walk_forward_validation"),
                "tuning_validation": _meta("tuning_validation"),
                "tuning_validation_with_warmup": _meta("tuning_validation_with_warmup"),
                "optimizer_train": _meta("optimizer_train"),
                "optimizer_train_with_warmup": _meta("optimizer_train_with_warmup"),
                "optimization_train": _meta("optimization_train"),
                "optimization_train_with_warmup": _meta("optimization_train_with_warmup"),
                "stage2_selection": _meta("stage2_selection"),
                "stage2_selection_with_warmup": _meta("stage2_selection_with_warmup"),
                "stage2_training_selection": _meta("stage2_training_selection"),
                "stage2_training_selection_with_warmup": _meta("stage2_training_selection_with_warmup"),
                "clean_holdout": _meta("clean_holdout"),
                "clean_holdout_with_warmup": _meta("clean_holdout_with_warmup"),
                "final_lockbox": _meta("final_lockbox"),
                "final_lockbox_with_warmup": _meta("final_lockbox_with_warmup"),
                "embargo_train_validation": _meta("embargo_train_validation"),
                "embargo_validation_lockbox": _meta("embargo_validation_lockbox"),
            },
        }

    def _load_symbol_timeframe_data(self, symbol: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        """
        Load the base-M5 workflow plus broker-native signal timeframes.

        Calendar-lockbox production is strictly broker-native: all configured
        signal timeframes are resolved and provenance-checked before a legacy
        M5 file, resampler, feature computation, or optimizer can run.  Older
        research modes retain the explicit legacy/resampled diagnostic path.
        """
        split_mode = str(
            getattr(self.config, "production_split_mode", CALENDAR_SPLIT_MODE)
            or CALENDAR_SPLIT_MODE
        ).strip().lower()
        configured_timeframes = tuple(
            dict.fromkeys(
                str(value or "").strip().upper()
                for value in (getattr(self.config, "timeframes", []) or [])
                if str(value or "").strip()
            )
        )
        strict_native = split_mode == CALENDAR_SPLIT_MODE
        preloaded_native: Dict[str, pd.DataFrame] = {}
        preloaded_meta: Dict[str, Dict[str, Any]] = {}
        timeframe_window_meta: Dict[str, Dict[str, Any]] = {}
        native_gate_errors: Dict[str, List[str]] = {}
        active_timeframes = set(configured_timeframes)
        source_unavailable_timeframes: set[str] = set()
        excluded_timeframes: set[str] = set()
        excluded_bindings: Dict[str, Dict[str, Any]] = {}
        native_cell_states: Dict[str, str] = {
            timeframe: NATIVE_EVIDENCE_CANDIDATE
            for timeframe in configured_timeframes
        }
        calendar_contract: Dict[str, Any] = {}
        universe = None

        # A production activation partitions cells, not symbols.  Research
        # tests without an ACTIVE pointer retain the explicit legacy behavior,
        # while any present pointer must validate completely and fail closed.
        if strict_native:
            native_root = Path(self.data_loader.data_dir) / "native"
            if (native_root / "ACTIVE.json").exists():
                universe = resolve_active_native_universe(
                    Path(self.data_loader.data_dir),
                    required_timeframes=configured_timeframes,
                    production_contract_required=True,
                )
                if universe.errors:
                    return {}, {
                        "production_split_mode": split_mode,
                        "native_source_gate": {
                            "passed": False,
                            "strict_no_fallback": True,
                            "errors_by_timeframe": {
                                "__ACTIVATION__": list(universe.errors)
                            },
                        },
                    }
                if universe.explicit_contract:
                    active_timeframes = {
                        str(tf).upper() for tf in universe.active_timeframes(symbol)
                    }
                    source_unavailable_reader = getattr(
                        universe, "source_unavailable_timeframes", None
                    )
                    source_unavailable_timeframes = {
                        str(tf).upper()
                        for tf in (
                            source_unavailable_reader(symbol)
                            if callable(source_unavailable_reader)
                            else ()
                        )
                    }
                    excluded_timeframes = {
                        str(tf).upper() for tf in universe.excluded_timeframes(symbol)
                    }
                    partition = (
                        active_timeframes
                        | source_unavailable_timeframes
                        | excluded_timeframes
                    )
                    if (
                        partition != set(configured_timeframes)
                        or active_timeframes & source_unavailable_timeframes
                        or active_timeframes & excluded_timeframes
                        or source_unavailable_timeframes & excluded_timeframes
                    ):
                        return {}, {
                            "production_split_mode": split_mode,
                            "native_source_gate": {
                                "passed": False,
                                "strict_no_fallback": True,
                                "errors_by_timeframe": {
                                    "__ACTIVATION__": ["NATIVE_CELL_PARTITION_INCOMPLETE"]
                                },
                            },
                        }
                    excluded_bindings = {
                        tf: dict(universe.cell_exclusion_binding(symbol, tf) or {})
                        for tf in sorted(excluded_timeframes)
                    }
                    native_cell_states = {
                        timeframe: str(universe.cell_status(symbol, timeframe)).lower()
                        for timeframe in configured_timeframes
                    }
                    calendar_contract = dict(
                        getattr(universe, "calendar_contract", {}) or {}
                    )

            # seq1019/seq1055 bounded preflight: exactly one frame lives at
            # a time; gate facts are recorded and the frame is released.
            native_rows_by_tf: Dict[str, int] = {}
            for tf_name in configured_timeframes:
                if tf_name not in active_timeframes:
                    continue
                try:
                    native_data, native_meta = self.data_loader.load_native_timeframe(
                        symbol, tf_name
                    )
                except Exception as exc:
                    native_data = None
                    native_meta = {
                        "source_kind": "error",
                        "symbol": symbol,
                        "timeframe": tf_name,
                        "provenance_valid": False,
                        "provenance_errors": [f"NATIVE_LOAD_RAISED:{type(exc).__name__}"],
                    }
                timeframe_meta = dict(native_meta or {})
                preloaded_meta[tf_name] = timeframe_meta
                reasons: List[str] = []
                if native_data is None or len(native_data) == 0:
                    reasons.append("NATIVE_FRAME_MISSING_OR_EMPTY")
                if str(timeframe_meta.get("source_kind") or "").lower() != "broker_native":
                    reasons.append("NATIVE_SOURCE_KIND_REQUIRED")
                if not bool(timeframe_meta.get("provenance_valid", False)):
                    reasons.extend(
                        str(value)
                        for value in (
                            timeframe_meta.get("provenance_errors")
                            or ["NATIVE_PROVENANCE_REQUIRED"]
                        )
                    )
                if reasons:
                    native_gate_errors[tf_name] = list(dict.fromkeys(reasons))
                else:
                    native_rows_by_tf[tf_name] = int(len(native_data))
                del native_data

            if native_gate_errors:
                logger.error(
                    "[%s] Active native-cell source gate failed: %s",
                    symbol,
                    "; ".join(
                        f"{tf}: {', '.join(reasons)}"
                        for tf, reasons in sorted(native_gate_errors.items())
                    ),
                )
                return {}, {
                    "production_split_mode": split_mode,
                    "native_source_gate": {
                        "passed": False,
                        "strict_no_fallback": True,
                        "errors_by_timeframe": native_gate_errors,
                    },
                    "native_cell_eligibility": {
                        "active_timeframes": sorted(active_timeframes),
                        "evidence_candidate_timeframes": sorted(active_timeframes),
                        "source_unavailable_timeframes": sorted(
                            source_unavailable_timeframes
                        ),
                        "excluded_timeframes": sorted(excluded_timeframes),
                        "excluded_bindings": excluded_bindings,
                        "cell_states": dict(native_cell_states),
                        "acquisition_emits_trading_verdicts": False,
                    },
                    "timeframe_sources": preloaded_meta,
                }

            if not active_timeframes:
                return {}, {
                    "production_split_mode": split_mode,
                    "native_source_gate": {
                        "passed": True,
                        "strict_no_fallback": True,
                        "timeframes": [],
                    },
                    "native_cell_eligibility": {
                        "active_timeframes": [],
                        "evidence_candidate_timeframes": [],
                        "source_unavailable_timeframes": sorted(
                            source_unavailable_timeframes
                        ),
                        "excluded_timeframes": sorted(excluded_timeframes),
                        "excluded_bindings": excluded_bindings,
                        "no_evidence_candidates": True,
                        "cell_states": dict(native_cell_states),
                        "acquisition_emits_trading_verdicts": False,
                        "activation_id": getattr(universe, "activation_id", None),
                        "activation_manifest_sha256": getattr(
                            universe, "activation_manifest_sha256", None
                        ),
                    },
                    "native_calendar_contract": calendar_contract,
                    "timeframe_sources": {},
                }

            reference_tf = (
                "M5" if "M5" in active_timeframes else
                next(tf for tf in configured_timeframes if tf in active_timeframes)
            )
            reference_data = ensure_utc_index(
                self.data_loader.load_native_timeframe(symbol, reference_tf)[0],
                copy=False,
            )
            if calendar_contract:
                try:
                    as_of_utc = pd.Timestamp(calendar_contract["common_as_of_utc"])
                    as_of_utc = (
                        as_of_utc.tz_localize("UTC")
                        if as_of_utc.tzinfo is None else as_of_utc.tz_convert("UTC")
                    )
                except Exception:
                    return {}, {
                        "production_split_mode": split_mode,
                        "native_source_gate": {
                            "passed": False,
                            "strict_no_fallback": True,
                            "errors_by_timeframe": {
                                "__ACTIVATION__": ["NATIVE_AUTHENTICATED_CALENDAR_INVALID"]
                            },
                        },
                    }
            else:
                configured_cutoff = str(
                    getattr(self.config, "evidence_as_of_utc", "") or ""
                ).strip()
                as_of_utc = latest_closed_bar_end(
                    reference_data,
                    reference_tf,
                    not_after_utc=configured_cutoff or datetime.now(timezone.utc),
                )
            if as_of_utc is None:
                return {}, {
                    "production_split_mode": split_mode,
                    "native_source_gate": {
                        "passed": False,
                        "strict_no_fallback": True,
                        "errors_by_timeframe": {
                            reference_tf: ["NATIVE_REFERENCE_HAS_NO_CLOSED_BAR"]
                        },
                    },
                }
            reference_data = trim_to_closed_bars(
                reference_data,
                reference_tf,
                as_of_utc=as_of_utc,
                copy=False,
            )
            if reference_data is None or len(reference_data) == 0:
                return {}, {
                    "production_split_mode": split_mode,
                    "native_source_gate": {
                        "passed": False,
                        "strict_no_fallback": True,
                        "errors_by_timeframe": {
                            reference_tf: ["NATIVE_REFERENCE_EMPTY_AFTER_CUTOFF"]
                        },
                    },
                }
            workflow_meta = self._build_calendar_workflow_meta(
                reference_data,
                as_of_utc=as_of_utc,
                base_timeframe=reference_tf,
                calendar_contract=calendar_contract,
            )
            base_source_meta = dict(preloaded_meta.get(reference_tf) or {})
            # Release the reference frame before any other TF loads.
            del reference_data
            import gc as _gc

            _gc.collect()
            source_fingerprints: Dict[str, Dict[str, Any]] = {}
            timeframe_sources: Dict[str, Dict[str, Any]] = {}
            for tf_name in configured_timeframes:
                if tf_name not in active_timeframes:
                    continue
                try:
                    data = trim_to_closed_bars(
                        ensure_utc_index(
                            self.data_loader.load_native_timeframe(
                                symbol, tf_name
                            )[0],
                            copy=False,
                        ),
                        tf_name,
                        as_of_utc=as_of_utc,
                        copy=False,
                    )
                    if data is None or len(data) == 0:
                        raise ValueError("closed native workflow is empty")
                    timeframe_meta = dict(preloaded_meta.get(tf_name) or {})
                    timeframe_meta["workflow_rows"] = int(len(data))
                    timeframe_meta["as_of_utc"] = as_of_utc.isoformat()
                    timeframe_sources[tf_name] = timeframe_meta
                    source_fingerprints[tf_name] = {
                        "meta": dict(preloaded_meta.get(tf_name) or {}),
                        "rows": int(len(data)),
                        "first_ts": str(data.index[0].isoformat()),
                        "last_ts": str(data.index[-1].isoformat()),
                    }
                    tf_workflow_meta = self._build_calendar_workflow_meta(
                        data,
                        as_of_utc=as_of_utc,
                        base_timeframe=tf_name,
                        calendar_contract=calendar_contract,
                    )
                    timeframe_window_meta[tf_name] = {
                        "split_contract_version": tf_workflow_meta.get(
                            "split_contract_version"
                        ),
                        "evidence_contract_version": tf_workflow_meta.get(
                            "evidence_contract_version"
                        ),
                        "calendar_boundaries": tf_workflow_meta.get(
                            "calendar_boundaries", {}
                        ),
                        "windows": tf_workflow_meta.get("windows", {}),
                    }
                    del data
                except Exception as exc:
                    native_gate_errors[tf_name] = [
                        f"NATIVE_CLOSED_WORKFLOW_PREPARATION_FAILED:{type(exc).__name__}"
                    ]
            data_by_tf = _BoundedNativeSource(
                self, symbol, as_of_utc, source_fingerprints
            )
            if native_gate_errors:
                workflow_meta["timeframe_sources"] = timeframe_sources
                workflow_meta["native_source_gate"] = {
                    "passed": False,
                    "strict_no_fallback": True,
                    "errors_by_timeframe": native_gate_errors,
                }
                return {}, workflow_meta
        else:
            # Research compatibility path. Production calendar mode above never
            # scans legacy data or resamples an excluded/failed native cell.
            base_m5, base_source_meta = self.data_loader.load_native_timeframe(
                symbol, "M5"
            )
            if base_m5 is None:
                base_m5 = self.data_loader.get_data(symbol, "M5")
                legacy_meta = dict(
                    getattr(self.data_loader, "_source_meta", {}).get(
                        f"{symbol}_M5", {}
                    ) or {}
                )
                base_source_meta = {
                    **legacy_meta,
                    "source_kind": "legacy_m5_csv",
                    "provenance_valid": False,
                    "provenance_errors": ["NATIVE_M5_PROVENANCE_MISSING"],
                }
            base_source_meta = dict(base_source_meta or {})
            if base_m5 is None or len(base_m5) < 200:
                return {}, {
                    "production_split_mode": split_mode,
                    "native_source_gate": {
                        "passed": False,
                        "strict_no_fallback": False,
                        "errors_by_timeframe": {
                            "M5": ["M5_WORKFLOW_REQUIRES_AT_LEAST_200_BARS"]
                        },
                    },
                }
            base_m5 = base_m5.sort_index()
            max_bars = int(getattr(self.config, "max_bars", 300000) or 300000)
            if max_bars > 0 and len(base_m5) > max_bars:
                base_m5 = base_m5.tail(max_bars).copy()
            workflow_meta = self._build_m5_workflow_meta(base_m5)
            data_by_tf = {}
            timeframe_sources = {}
            for tf_name in configured_timeframes:
                if tf_name == "M5":
                    data = base_m5
                    timeframe_meta = dict(base_source_meta)
                else:
                    native_data, native_meta = self.data_loader.load_native_timeframe(
                        symbol, tf_name
                    )
                    if native_data is not None and len(native_data) > 0:
                        data = native_data
                        timeframe_meta = dict(native_meta or {})
                    else:
                        data = self.data_loader.resample(base_m5, tf_name)
                        timeframe_meta = {
                            "source_kind": "m5_resampled",
                            "symbol": symbol,
                            "timeframe": tf_name,
                            "path": base_source_meta.get("path"),
                            "sha256": base_source_meta.get("sha256"),
                            "provenance_valid": False,
                            "provenance_errors": list(
                                (native_meta or {}).get("provenance_errors", [])
                                or ["NATIVE_TIMEFRAME_FILE_MISSING"]
                            ),
                            "parent_m5_sha256": base_source_meta.get("sha256"),
                        }
                timeframe_meta["workflow_rows"] = int(len(data)) if data is not None else 0
                timeframe_sources[tf_name] = timeframe_meta
                if data is not None and len(data) > 0:
                    data_by_tf[tf_name] = data.copy()

        if not workflow_meta:
            return {}, {}
        workflow_window = (workflow_meta.get("windows", {}) or {}).get("workflow_window", {})
        workflow_meta["source"] = {
            "path": base_source_meta.get("path"),
            "mtime": base_source_meta.get("mtime"),
            "size": base_source_meta.get("size"),
            "rows": base_source_meta.get("rows"),
            "last_index": base_source_meta.get("last_index"),
            "sha256": base_source_meta.get("sha256"),
            "source_kind": base_source_meta.get("source_kind"),
            "provenance_valid": bool(base_source_meta.get("provenance_valid", False)),
            "provenance_errors": list(base_source_meta.get("provenance_errors", []) or []),
        }
        if strict_native:
            workflow_meta["native_source_gate"] = {
                "passed": True,
                "strict_no_fallback": True,
                "timeframes": sorted(active_timeframes),
                "source_unavailable_timeframes": sorted(
                    source_unavailable_timeframes
                ),
                "excluded_timeframes": sorted(excluded_timeframes),
            }
            workflow_meta["native_cell_eligibility"] = {
                "active_timeframes": sorted(active_timeframes),
                "evidence_candidate_timeframes": sorted(active_timeframes),
                "source_unavailable_timeframes": sorted(
                    source_unavailable_timeframes
                ),
                "excluded_timeframes": sorted(excluded_timeframes),
                "excluded_bindings": excluded_bindings,
                "cell_states": dict(native_cell_states),
                "acquisition_emits_trading_verdicts": False,
                "activation_id": getattr(universe, "activation_id", None),
                "activation_manifest_sha256": getattr(
                    universe, "activation_manifest_sha256", None
                ),
            }
        workflow_meta["timeframe_sources"] = timeframe_sources
        if timeframe_window_meta:
            workflow_meta["timeframe_windows"] = timeframe_window_meta
        workflow_meta["evidence_contract_version"] = workflow_meta.get(
            "split_contract_version", EVIDENCE_CONTRACT_VERSION
        )
        workflow_meta["data_window_fingerprint"] = _stable_hash({
            "symbol": symbol,
            "source": workflow_meta["source"],
            "timeframe_sources": timeframe_sources,
            "native_cell_eligibility": workflow_meta.get("native_cell_eligibility", {}),
            "workflow_window": workflow_window,
            "as_of_utc": workflow_meta.get("as_of_utc"),
            "windows": workflow_meta.get("windows", {}),
            "timeframe_windows": workflow_meta.get("timeframe_windows", {}),
        })
        return data_by_tf, workflow_meta

    def precompute_feature_cache(self, symbols: List[str]) -> Dict[str, Any]:
        """Warm the disk-backed full-feature cache without producing strategy evidence."""
        started = time.time()
        stats: Dict[str, Any] = {
            "symbols_requested": len(symbols or []),
            "symbols_completed": 0,
            "symbols_failed": 0,
            "timeframes_computed_or_retrieved": 0,
            "errors": {},
        }
        regime_params_file = getattr(self.config, 'regime_params_file', 'regime_params.json')

        for symbol in symbols or []:
            try:
                data_by_tf, _data_window_meta = self._load_symbol_timeframe_data(symbol)
                if not data_by_tf:
                    stats["symbols_failed"] += 1
                    stats["errors"][symbol] = "No valid data available"
                    continue
                for tf, data in data_by_tf.items():
                    FeatureComputer.compute_all(
                        data,
                        symbol=symbol,
                        timeframe=tf,
                        regime_params_file=regime_params_file,
                    )
                    stats["timeframes_computed_or_retrieved"] += 1
                stats["symbols_completed"] += 1
            except Exception as exc:
                stats["symbols_failed"] += 1
                stats["errors"][symbol] = str(exc)
            finally:
                self.data_loader.clear_cache()
                FeatureComputer.clear_cache()

        stats["duration_seconds"] = time.time() - started
        stats["feature_cache"] = FeatureComputer.get_cache_stats(reset=True)
        return stats

    def _build_edge_challengers_for_timeframe(
        self,
        *,
        symbol: str,
        timeframe: str,
        champion_configs: Dict[str, RegimeConfig],
        full_features: pd.DataFrame,
        fit_start: pd.Timestamp,
        fit_end: pd.Timestamp,
        champion_temporal_lineage: Dict[str, Any],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        """Refit only champion families on a latest trailing in-sample window.

        The output is deliberately serialized into the separate paper-only
        challenger book.  It has no validation metrics, cannot inherit the
        champion lockbox verdict, and requires observations strictly after the
        fit cutoff before any later promotion process may consider it.
        """
        failures: List[str] = []
        output: Dict[str, Dict[str, Any]] = {}
        edge_features = self._slice_time_window(full_features, fit_start, fit_end)
        if edge_features is None or len(edge_features) < 200:
            return {}, [
                f"{timeframe}:edge fit rows {0 if edge_features is None else len(edge_features)} < 200"
            ]

        family_regimes: Dict[str, List[str]] = {}
        eligible_champions: Dict[str, RegimeConfig] = {}
        for regime, champion in (champion_configs or {}).items():
            if champion is None or champion.is_no_trade_marker():
                continue
            status, reasons = classify_promotion_status(champion, self.config)
            if status not in {"PROMOTED", "PROMOTE_SMALL"}:
                failures.append(
                    f"{timeframe}|{regime}:champion_not_promotion_eligible:{status}:"
                    f"{'; '.join(reasons[:2])}"
                )
                continue
            family = str(champion.strategy_name or "")
            if not family:
                continue
            eligible_champions[str(regime).upper()] = champion
            family_regimes.setdefault(family, []).append(str(regime).upper())

        for family, regimes in sorted(family_regimes.items()):
            family_candidates: Dict[str, Dict[str, Any]] = {}
            try:
                reference = next(
                    eligible_champions[regime]
                    for regime in regimes
                    if regime in eligible_champions
                )
                strategy = StrategyRegistry.get(family, **dict(reference.parameters or {}))
                param_grid = strategy.get_param_grid()
                if param_grid:
                    tuned = self.regime_optimizer._tune_strategy_params(
                        symbol,
                        timeframe,
                        family,
                        param_grid,
                        edge_features,
                        None,
                        0,
                        regimes=regimes,
                    )
                    family_candidates = {
                        str(candidate.get("tuned_for_regime", "") or "").upper(): candidate
                        for candidate in tuned
                        if str(candidate.get("tuned_for_regime", "") or "").strip()
                    }
                else:
                    # There is nothing to estimate for a parameterless family;
                    # still reassess its frozen rule on the latest fit window.
                    for regime in regimes:
                        champion = eligible_champions[regime]
                        fixed = StrategyRegistry.get(
                            family, **dict(champion.parameters or {})
                        )
                        metrics, ran = self.regime_optimizer._run_one_exact_regime(
                            symbol=symbol,
                            timeframe=timeframe,
                            features=edge_features,
                            strategy=fixed,
                            regime=regime,
                            partition="edge_challenger_fit",
                        )
                        if not ran:
                            continue
                        family_candidates[regime] = {
                            "strategy_name": family,
                            "params": self.regime_optimizer._clean_candidate_params(
                                fixed.get_params()
                            ),
                            "train_regime_metrics": {regime: dict(metrics)},
                            "tuned_for_regime": regime,
                            "is_tuned": False,
                            "search_trials": 1,
                            "search_phase": "latest_fixed_family_reassessment",
                        }
            except Exception as exc:
                failures.append(
                    f"{timeframe}|{family}:edge_refit_failed:{type(exc).__name__}:{exc}"
                )
                continue

            for regime in regimes:
                candidate = family_candidates.get(regime)
                if not isinstance(candidate, dict):
                    failures.append(f"{timeframe}|{regime}|{family}:no_edge_candidate")
                    continue
                train_metrics = dict(
                    (candidate.get("train_regime_metrics") or {}).get(regime, {}) or {}
                )
                if int(train_metrics.get("total_trades", 0) or 0) < int(
                    self.regime_optimizer.min_train_trades
                ):
                    failures.append(f"{timeframe}|{regime}|{family}:insufficient_edge_trades")
                    continue
                try:
                    train_score = float(
                        self.regime_optimizer._compute_train_regime_score(
                            train_metrics, {}
                        )
                    )
                except Exception:
                    train_score = 0.0
                temporal = build_edge_challenger_lineage(
                    fit_start_utc=fit_start,
                    fit_end_utc=fit_end,
                    parent_champion=champion_temporal_lineage,
                )
                champion = eligible_champions[regime]
                challenger_parameters = dict(candidate.get("params", {}) or {})
                candidate_id = _stable_hash({
                    "book_role": EDGE_CHALLENGER_ROLE,
                    "symbol": str(symbol).upper(),
                    "timeframe": str(timeframe).upper(),
                    "regime": str(regime).upper(),
                    "strategy_name": family,
                    "parameters": challenger_parameters,
                    "fit_cutoff_utc": temporal.get("fit_cutoff_utc"),
                })
                challenger = RegimeConfig(
                    strategy_name=family,
                    parameters=challenger_parameters,
                    quality_score=self.regime_optimizer._normalize_score(train_score),
                    train_metrics=train_metrics,
                    val_metrics={},
                    holdout_metrics={},
                    regime_train_trades=int(train_metrics.get("total_trades", 0) or 0),
                    regime_val_trades=0,
                    trained_at=datetime.now(),
                    valid_until=None,
                    artifact_meta={
                        "edge_challenger": True,
                        "funded_role_eligible": False,
                        "candidate_id": candidate_id,
                    },
                    validation_evidence={
                        "temporal_lineage": temporal,
                        "edge_refit": {
                            "selection_surface": "latest_trailing_5y_in_sample_refit",
                            "family_fixed_from_champion": family,
                            "champion_parameters": dict(champion.parameters or {}),
                            "search_trials": int(candidate.get("search_trials", 0) or 0),
                            "search_phase": str(candidate.get("search_phase", "") or ""),
                            "fit_rows": int(len(edge_features)),
                            "forward_evidence_status": "NOT_STARTED",
                            "candidate_id": candidate_id,
                        },
                    },
                    promotion_status="PAPER_ONLY",
                    promotion_reasons=[
                        "latest-data refit has no observations after its fit cutoff",
                        "champion lockbox verdict is not inherited",
                    ],
                )
                output[regime] = challenger.to_dict()
        return output, failures

    def _attach_holdout_metrics(self,
                                symbol: str,
                                regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                holdout_features_by_tf: Dict[str, pd.DataFrame],
                                holdout_warmup_by_tf: Dict[str, int],
                                data_window_meta: Optional[Dict[str, Any]] = None) -> None:
        """Evaluate each frozen winner on its exact final-lockbox regime cell.

        Signals outside the promoted regime are zeroed before the backtest. This
        matches live entry eligibility and avoids contaminating a cell's equity
        path with trades that live execution assigns to other specialists.
        """
        for tf, regimes in regime_configs.items():
            holdout_features = holdout_features_by_tf.get(tf)
            holdout_warmup = int(holdout_warmup_by_tf.get(tf, 0) or 0)
            if holdout_features is None or len(holdout_features) <= holdout_warmup:
                continue
            if 'REGIME_LIVE' not in holdout_features.columns:
                continue
            for regime, cfg in regimes.items():
                if cfg.is_no_trade_marker():
                    continue
                lockbox_access: Dict[str, Any] = {"status": "NOT_REQUIRED"}
                if (
                    bool(getattr(self.config, "lockbox_one_shot_registry_enabled", True))
                    and data_window_meta is not None
                ):
                    timeframe_source = dict(
                        (data_window_meta.get("timeframe_sources") or {}).get(tf, {}) or {}
                    )
                    lockbox_window = dict(
                        (data_window_meta.get("windows") or {}).get("final_lockbox", {}) or {}
                    )
                    lineage = {
                        "source_sha256": timeframe_source.get("sha256"),
                        "source_kind": timeframe_source.get("source_kind"),
                        "window_start": lockbox_window.get("start_ts"),
                        "window_end": lockbox_window.get("end_ts"),
                        "data_window_fingerprint": data_window_meta.get("data_window_fingerprint"),
                        "evidence_contract_version": data_window_meta.get("evidence_contract_version"),
                    }
                    if not all(
                        lineage.get(key)
                        for key in (
                            "source_sha256",
                            "window_start",
                            "window_end",
                            "data_window_fingerprint",
                        )
                    ):
                        cfg.holdout_metrics = {
                            "validation_mode": "final_lockbox_regime_gated",
                            "timeframe": str(tf).upper(),
                            "target_regime": str(regime).upper(),
                            "total_trades": 0,
                            "policy_status": "FAIL",
                            "policy_reason": "one-shot lockbox lineage incomplete",
                            "lockbox_access": {"status": "UNAVAILABLE", "lineage": lineage},
                        }
                        continue
                    try:
                        decision = self.lockbox_access_registry.consume(
                            cell={
                                "symbol": symbol,
                                "timeframe": str(tf).upper(),
                                "regime": str(regime).upper(),
                            },
                            lineage=lineage,
                        )
                    except Exception as exc:
                        cfg.holdout_metrics = {
                            "validation_mode": "final_lockbox_regime_gated",
                            "timeframe": str(tf).upper(),
                            "target_regime": str(regime).upper(),
                            "total_trades": 0,
                            "policy_status": "FAIL",
                            "policy_reason": f"lockbox registry unavailable: {type(exc).__name__}: {exc}",
                            "lockbox_access": {"status": "ERROR"},
                        }
                        continue
                    lockbox_access = {
                        "status": "CONSUMED" if decision.allowed else "REUSED_BLOCKED",
                        "access_id": decision.access_id,
                        "reason": decision.reason,
                        "conflict": dict(decision.metadata or {}),
                    }
                    if not decision.allowed:
                        cfg.holdout_metrics = {
                            "validation_mode": "final_lockbox_regime_gated",
                            "timeframe": str(tf).upper(),
                            "target_regime": str(regime).upper(),
                            "total_trades": 0,
                            "policy_status": "FAIL",
                            "policy_reason": "final lockbox already consumed for this lineage/cell",
                            "lockbox_access": lockbox_access,
                        }
                        continue
                try:
                    strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
                    signals = strategy.generate_signals(holdout_features, symbol)
                    regime_mask = (
                        holdout_features["REGIME_LIVE"]
                        .reindex(signals.index)
                        .astype(str)
                        .str.upper()
                        .eq(str(regime).upper())
                        .fillna(False)
                    )
                    signals = signals.copy()
                    signals.loc[~regime_mask] = 0
                    holdout_result = self.regime_optimizer.backtester.run(
                        holdout_features,
                        signals,
                        symbol,
                        strategy,
                        warmup_bars=holdout_warmup,
                        timeframe=tf,
                    )
                    compact = self.regime_optimizer._compact_backtest_result(holdout_result)
                    power_policy_passed = False
                    power_policy_reason = (
                        "final-lockbox trade-power evidence or its frozen train "
                        "requirement is missing"
                    )
                    validation_evidence = dict(
                        getattr(cfg, "validation_evidence", {}) or {}
                    )
                    if isinstance(validation_evidence.get("trade_power"), Mapping):
                        frozen_requirement = (
                            self.regime_optimizer
                            ._frozen_requirement_from_validation_evidence(
                                validation_evidence
                            )
                        )
                        final_power_decision = (
                            self.regime_optimizer._evaluate_partition_power_result(
                                holdout_result,
                                frozen_requirement=frozen_requirement,
                                partition="final_lockbox",
                                symbol=symbol,
                                timeframe=tf,
                                regime=regime,
                                strategy_family=cfg.strategy_name,
                                parameter_hash=_stable_hash(
                                    self.regime_optimizer._clean_candidate_params(
                                        cfg.parameters
                                    )
                                ),
                                prior_partition_decision=(
                                    validation_evidence.get("trade_power", {})
                                    .get("tuning_validation")
                                ),
                            )
                        )
                        compact["trade_power_final"] = final_power_decision.to_dict()
                        compact["effective_trades"] = float(
                            final_power_decision.observed_effective_trades
                        )
                        power_policy_passed = final_power_decision.passed
                        power_policy_reason = final_power_decision.reason
                    compact.update(
                        self.regime_optimizer._build_daily_net_mtm_surface(
                            result=holdout_result,
                            features=holdout_features,
                            timeframe=tf,
                            warmup_bars=holdout_warmup,
                            partition="final_lockbox",
                        )
                    )
                    compact.update({
                        "validation_mode": "final_lockbox_regime_gated",
                        "timeframe": str(tf).upper(),
                        "target_regime": str(regime).upper(),
                        "regime_gated": True,
                        "regime_matching_bars": int(regime_mask.sum()),
                        "min_trades_required": int(
                            self.regime_optimizer._min_val_trades_for_timeframe(tf)
                        ),
                        "lockbox_access": lockbox_access,
                    })
                    lockbox_bootstrap = block_bootstrap_evidence(
                        compact.get("daily_net_mtm_returns", []) or [],
                        repetitions=int(
                            getattr(self.config, "false_discovery_bootstrap_repetitions", 300)
                            or 300
                        ),
                        confidence=float(
                            getattr(self.config, "false_discovery_bootstrap_confidence", 0.95)
                            or 0.95
                        ),
                        seed=_stable_seed(symbol, tf, regime, cfg.strategy_name, cfg.parameters, "lockbox"),
                        min_observations=int(
                            getattr(self.config, "false_discovery_min_daily_observations", 252)
                            or 252
                        ),
                        min_effective_observations=float(
                            getattr(self.config, "false_discovery_min_effective_observations", 60.0)
                            or 60.0
                        ),
                        min_mean_lower=float(
                            getattr(self.config, "false_discovery_bootstrap_min_mean_lower", 0.0)
                            or 0.0
                        ),
                        min_profit_factor_lower=float(
                            getattr(self.config, "false_discovery_bootstrap_min_pf_lower", 1.0)
                            or 1.0
                        ),
                        max_drawdown_upper=float(
                            getattr(self.config, "false_discovery_bootstrap_max_dd_upper", 0.20)
                            or 0.20
                        ),
                    )
                    compact["lockbox_bootstrap_evidence"] = lockbox_bootstrap
                    bootstrap_passed = (
                        str(lockbox_bootstrap.get("status", "UNAVAILABLE")).upper()
                        == "PASS"
                    )
                    if not power_policy_passed:
                        compact["policy_status"] = "FAIL"
                        compact["policy_reason"] = power_policy_reason
                    elif not bootstrap_passed:
                        compact["policy_status"] = "FAIL"
                        compact["policy_reason"] = (
                            "final-lockbox block bootstrap did not establish positive edge"
                        )
                    else:
                        compact["policy_status"] = "PASS"
                        compact["policy_reason"] = (
                            "final-lockbox effective-trade and block-bootstrap policies passed"
                        )
                    cfg.holdout_metrics = compact
                except Exception as exc:
                    cfg.holdout_metrics = {
                        "validation_mode": "final_lockbox_regime_gated",
                        "timeframe": str(tf).upper(),
                        "target_regime": str(regime).upper(),
                        "regime_gated": True,
                        "total_trades": 0,
                        "min_trades_required": int(
                            self.regime_optimizer._min_val_trades_for_timeframe(tf)
                        ),
                        "policy_status": "FAIL",
                        "policy_reason": "final-lockbox evaluation infrastructure failure",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                    logger.warning(f"[{symbol}] [{tf}] [{regime}] Holdout evaluation failed: {exc}")

    def _attach_purged_kfold_oos_metrics(self,
                                         symbol: str,
                                         regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                         oos_features_by_tf: Dict[str, pd.DataFrame],
                                         oos_warmup_by_tf: Dict[str, int]) -> None:
        """Attach opt-in purged/embargoed k-fold OOS evidence to selected specialists."""
        for tf, regimes in regime_configs.items():
            oos_features = oos_features_by_tf.get(tf)
            scored_start = int(oos_warmup_by_tf.get(tf, 0) or 0)
            if oos_features is None or len(oos_features) <= max(scored_start + 1, 3):
                continue
            if "REGIME_LIVE" not in oos_features.columns:
                continue
            for regime, cfg in regimes.items():
                if cfg.is_no_trade_marker():
                    continue
                try:
                    evidence = self.regime_optimizer.evaluate_purged_kfold_oos(
                        symbol,
                        tf,
                        regime,
                        cfg,
                        oos_features,
                        scored_start,
                    )
                    if not evidence:
                        continue
                    cfg.holdout_metrics = {
                        key: value for key, value in evidence.items()
                        if key != "folds"
                    }
                    validation_evidence = dict(getattr(cfg, "validation_evidence", {}) or {})
                    validation_evidence["oos_validation"] = evidence
                    cfg.validation_evidence = validation_evidence
                except Exception as exc:
                    logger.warning(f"[{symbol}] [{tf}] [{regime}] Purged-kfold OOS evaluation failed: {exc}")

    def _attach_historical_stress_audit(self,
                                        symbol: str,
                                        regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                        historical_features_by_tf: Dict[str, pd.DataFrame]) -> None:
        """Evaluate selected winners on the older historical audit window."""
        # Fallbacks mirror the PipelineConfig defaults. They must never sit below
        # breakeven: a getattr default of 0.60 here would silently reinstate the
        # gate that promoted a net-losing winner in the 2026-06-18 audit.
        min_trades = int(getattr(self.config, "historical_audit_min_trades", 20) or 0)
        max_dd = float(getattr(self.config, "historical_audit_max_drawdown", 25.0) or 25.0)
        min_pf = float(getattr(self.config, "historical_audit_min_profit_factor", 1.10) or 1.10)
        for tf, regimes in regime_configs.items():
            audit_features = historical_features_by_tf.get(tf)
            if audit_features is None or len(audit_features) < 50:
                continue
            if 'REGIME_LIVE' not in audit_features.columns:
                continue
            for regime, cfg in regimes.items():
                if cfg.is_no_trade_marker():
                    continue
                payload = _default_historical_audit()
                payload["enabled"] = True
                payload["audited_at"] = datetime.now().isoformat()
                try:
                    strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
                    signals = strategy.generate_signals(audit_features, symbol)
                    audit_result = self.regime_optimizer.backtester.run(
                        audit_features,
                        signals,
                        symbol,
                        strategy,
                        timeframe=tf,
                    )
                    bucket = self.regime_optimizer._bucket_trades_by_regime(
                        audit_result.get("trades", []),
                        audit_features,
                        timeframe=tf,
                    )
                    metrics = bucket.get(regime, {"total_trades": 0})
                    if bool(getattr(self.config, "regime_profitability_audit_enabled", True)):
                        regime_audit = audit_regime_profitability(
                            {regime: metrics},
                            features=audit_features,
                            trades=audit_result.get("trades", []),
                            min_trades=min_trades,
                            min_profit_factor=min_pf,
                            min_return_pct=0.0,
                            low_gap_warn=float(getattr(self.config, "regime_min_live_gap_warn", 0.05) or 0.05),
                            include_boundary_audit=bool(
                                getattr(self.config, "regime_boundary_audit_enabled", True)
                            ),
                        ).to_dict()
                        payload["regime_profitability_audit"] = regime_audit
                        validation_evidence = dict(getattr(cfg, "validation_evidence", {}) or {})
                        validation_evidence["regime_audit"] = regime_audit
                        cfg.validation_evidence = validation_evidence
                    total_trades = int(metrics.get("total_trades", 0) or 0)
                    pf = float(metrics.get("profit_factor", 0.0) or 0.0)
                    dd = float(metrics.get("max_drawdown_pct", 0.0) or 0.0)
                    reasons: List[str] = []
                    if total_trades < min_trades:
                        reasons.append(f"trades {total_trades} < {min_trades}")
                    if dd > max_dd:
                        reasons.append(f"drawdown {dd:.1f}% > {max_dd:.1f}%")
                    if total_trades > 0 and pf < min_pf:
                        reasons.append(f"profit_factor {pf:.2f} < {min_pf:.2f}")
                    payload.update(
                        {
                            "total_trades": total_trades,
                            "severe_failure": bool(reasons),
                            "failure_reasons": reasons,
                            "metrics": {
                                k: v for k, v in metrics.items()
                                if k not in {"equity_curve", "trades"}
                            },
                        }
                    )
                except Exception as exc:
                    payload.update(
                        {
                            "severe_failure": True,
                            "failure_reasons": [f"audit_failed:{exc}"],
                        }
                    )
                    logger.warning(f"[{symbol}] [{tf}] [{regime}] Historical audit failed: {exc}")
                cfg.historical_audit_metrics = payload

    def _build_walk_forward_audit(self,
                                  symbol: str,
                                  timeframe: str,
                                  regime: str,
                                  cfg: RegimeConfig,
                                  validation_features: Optional[pd.DataFrame],
                                  validation_warmup: int) -> Dict[str, Any]:
        audit = _default_walk_forward_audit()
        audit["enabled"] = bool(getattr(self.config, "walk_forward_audit_enabled", True))
        if not audit["enabled"]:
            return audit
        if validation_features is None or len(validation_features) <= max(1, validation_warmup):
            audit["summary"] = {"reason": "insufficient_validation_window"}
            return audit
        try:
            window_count = max(1, int(getattr(self.config, "walk_forward_audit_windows", 3)))
        except Exception:
            window_count = 3
        scored_start = max(0, int(validation_warmup))
        scored_len = len(validation_features) - scored_start
        if scored_len <= 1:
            audit["summary"] = {"reason": "no_scored_window"}
            return audit
        try:
            strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
        except Exception as exc:
            audit["summary"] = {"reason": f"strategy_load_failed:{exc}"}
            return audit
        cut_points = np.linspace(scored_start, len(validation_features), num=window_count + 1, dtype=int)
        windows: List[Dict[str, Any]] = []
        for idx in range(window_count):
            scored_window_start = int(cut_points[idx])
            scored_window_end = int(cut_points[idx + 1])
            if scored_window_end - scored_window_start <= 1:
                continue
            window_start = max(0, scored_window_start - scored_start)
            window_features = validation_features.iloc[window_start:scored_window_end].copy()
            window_warmup = max(0, scored_window_start - window_start)
            if len(window_features) <= window_warmup:
                continue
            try:
                signals = strategy.generate_signals(window_features, symbol)
                result = self.regime_optimizer.backtester.run(
                    window_features,
                    signals,
                    symbol,
                    strategy,
                    timeframe=timeframe,
                    warmup_bars=window_warmup,
                )
                bucket = self.regime_optimizer._bucket_trades_by_regime(
                    result.get("trades", []),
                    window_features,
                    timeframe=timeframe,
                )
                metrics = bucket.get(regime, {"total_trades": 0})
            except Exception as exc:
                metrics = {"total_trades": 0, "error": str(exc)}
            windows.append(
                {
                    "window_index": idx,
                    "scored_start_bar": scored_window_start,
                    "scored_end_bar": scored_window_end,
                    "total_trades": int(metrics.get("total_trades", 0) or 0),
                    "profit_factor": float(metrics.get("profit_factor", 0.0) or 0.0),
                    "total_return_pct": float(metrics.get("total_return_pct", 0.0) or 0.0),
                    "max_drawdown_pct": float(metrics.get("max_drawdown_pct", 0.0) or 0.0),
                    "score": float(self.regime_optimizer.scorer.score(metrics, purpose="selection"))
                    if metrics.get("total_trades", 0) else 0.0,
                }
            )
        audited_windows = [window for window in windows if window.get("total_trades", 0) > 0]
        if audited_windows:
            pf_values = [float(window["profit_factor"]) for window in audited_windows]
            return_values = [float(window["total_return_pct"]) for window in audited_windows]
            score_values = [float(window["score"]) for window in audited_windows]
            audit["summary"] = {
                "audited_windows": len(audited_windows),
                "avg_profit_factor": round(float(np.mean(pf_values)), 4),
                "avg_total_return_pct": round(float(np.mean(return_values)), 4),
                "min_score": round(float(np.min(score_values)), 4),
                "max_score": round(float(np.max(score_values)), 4),
                "positive_windows": int(sum(1 for value in return_values if value > 0)),
            }
        else:
            audit["summary"] = {"reason": "no_regime_trades"}
        audit["window_count"] = len(windows)
        audit["windows"] = windows
        audit["audited_at"] = datetime.now().isoformat()
        return audit

    def _attach_walk_forward_audit(self,
                                   symbol: str,
                                   regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                   validation_features_by_tf: Dict[str, pd.DataFrame],
                                   validation_warmup_by_tf: Dict[str, int]) -> None:
        if not bool(getattr(self.config, "walk_forward_audit_enabled", True)):
            return
        for timeframe, regimes in regime_configs.items():
            validation_features = validation_features_by_tf.get(timeframe)
            validation_warmup = int(validation_warmup_by_tf.get(timeframe, 0) or 0)
            for regime, cfg in regimes.items():
                if cfg.is_no_trade_marker():
                    continue
                cfg.walk_forward_audit = self._build_walk_forward_audit(
                    symbol,
                    timeframe,
                    regime,
                    cfg,
                    validation_features,
                    validation_warmup,
                )

    def _select_governance_policy(self,
                                  symbol: str,
                                  timeframe: str,
                                  regime: str,
                                  cfg: RegimeConfig,
                                  evaluation_features: Optional[pd.DataFrame],
                                  evaluation_warmup: int,
                                  *,
                                  selection_surface: str = "stage3_selection") -> Dict[str, Any]:
        risk_opt_enabled = bool(getattr(self.config, "risk_management_optimization_enabled", False))
        tournament_enabled = bool(getattr(self.config, "local_governance_tournament_enabled", False))
        if cfg.is_no_trade_marker():
            payload = _default_governance_policy()
            payload["selection_basis"] = "no_trade_marker"
            return payload
        if not risk_opt_enabled and not tournament_enabled:
            payload = _default_governance_policy()
            payload["selection_basis"] = "disabled"
            return payload
        if evaluation_features is None or len(evaluation_features) <= max(1, evaluation_warmup):
            payload = _default_governance_policy()
            payload["selection_basis"] = f"{selection_surface}_unavailable"
            return payload
        try:
            strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
            signals = strategy.generate_signals(evaluation_features, symbol)
        except Exception as exc:
            payload = _default_governance_policy()
            payload["selection_basis"] = f"strategy_load_failed:{exc}"
            return payload

        candidate_results: Dict[str, Dict[str, Any]] = {}
        best_name = "control_fixed"
        best_metrics: Dict[str, Any] = {"total_trades": 0}
        best_rank: Tuple[float, float, float] = (-float("inf"), -float("inf"), -float("inf"))
        baseline_metrics: Optional[Dict[str, Any]] = None
        baseline_rank: Tuple[float, float, float] = (-float("inf"), -float("inf"), -float("inf"))
        for name in candidate_policy_names(getattr(self.config, "local_governance_candidate_policies", None)):
            try:
                policy = make_policy(name)
                result = self.regime_optimizer.backtester.run(
                    evaluation_features,
                    signals,
                    symbol,
                    strategy,
                    timeframe=timeframe,
                    warmup_bars=evaluation_warmup,
                    governance_policy=policy,
                )
                bucket = self.regime_optimizer._bucket_trades_by_regime(
                    result.get("trades", []),
                    evaluation_features,
                    timeframe=timeframe,
                )
                metrics = bucket.get(regime, {"total_trades": 0})
            except Exception as exc:
                metrics = {"total_trades": 0, "error": str(exc)}
            if name == "control_fixed":
                baseline_metrics = dict(metrics)
            score = (
                float(self.regime_optimizer.scorer.score(metrics, purpose="selection"))
                if int(metrics.get("total_trades", 0) or 0) > 0 else -float("inf")
            )
            rank = (
                score,
                float(metrics.get("total_return_pct", 0.0) or 0.0),
                -float(metrics.get("max_drawdown_pct", 0.0) or 0.0),
            )
            candidate_results[name] = {
                "score": None if not math.isfinite(score) else round(score, 6),
                "total_trades": int(metrics.get("total_trades", 0) or 0),
                "profit_factor": float(metrics.get("profit_factor", 0.0) or 0.0),
                "total_return_pct": float(metrics.get("total_return_pct", 0.0) or 0.0),
                "max_drawdown_pct": float(metrics.get("max_drawdown_pct", 0.0) or 0.0),
                "win_rate": float(metrics.get("win_rate", 0.0) or 0.0),
            }
            if name == "control_fixed":
                baseline_rank = rank
            if rank > best_rank:
                best_name = name
                best_metrics = metrics
                best_rank = rank
        baseline_metrics = baseline_metrics or {"total_trades": 0}
        stage = str(getattr(self.config, "risk_management_selection_stage", "stage3"))
        selection_basis = f"{selection_surface}_risk_management_tournament"
        if (
            best_name != "control_fixed"
            and bool(getattr(self.config, "local_governance_require_baseline_improvement", True))
        ):
            min_improvement = float(getattr(self.config, "local_governance_min_score_improvement", 0.0) or 0.0)
            baseline_score = baseline_rank[0]
            best_score = best_rank[0]
            selected_return = float(best_metrics.get("total_return_pct", 0.0) or 0.0)
            baseline_return = float(baseline_metrics.get("total_return_pct", 0.0) or 0.0)
            selected_pf = float(best_metrics.get("profit_factor", 0.0) or 0.0)
            baseline_pf = float(baseline_metrics.get("profit_factor", 0.0) or 0.0)
            baseline_trades = int(baseline_metrics.get("total_trades", 0) or 0)
            score_improved = (
                math.isfinite(best_score)
                and (
                    not math.isfinite(baseline_score)
                    or best_score >= baseline_score + min_improvement
                )
            )
            profitability_preserved = (
                baseline_trades <= 0
                or (
                    selected_return >= baseline_return
                    and selected_pf >= baseline_pf
                )
            )
            if not score_improved or not profitability_preserved:
                best_name = "control_fixed"
                best_metrics = baseline_metrics
                selection_basis = "baseline_not_improved"
            else:
                selection_basis = f"{selection_surface}_baseline_improved"
        payload = _default_governance_policy()
        payload.update(
            {
                "selected_policy": best_name,
                "selection_basis": selection_basis,
                "optimization_stage": stage,
                "selection_surface": selection_surface,
                "selected_at": datetime.now().isoformat(),
                "candidate_results": candidate_results,
                "selected_metrics": {
                    "total_trades": int(best_metrics.get("total_trades", 0) or 0),
                    "profit_factor": float(best_metrics.get("profit_factor", 0.0) or 0.0),
                    "total_return_pct": float(best_metrics.get("total_return_pct", 0.0) or 0.0),
                    "max_drawdown_pct": float(best_metrics.get("max_drawdown_pct", 0.0) or 0.0),
                    "win_rate": float(best_metrics.get("win_rate", 0.0) or 0.0),
                },
                "baseline_metrics": {
                    "total_trades": int(baseline_metrics.get("total_trades", 0) or 0),
                    "profit_factor": float(baseline_metrics.get("profit_factor", 0.0) or 0.0),
                    "total_return_pct": float(baseline_metrics.get("total_return_pct", 0.0) or 0.0),
                    "max_drawdown_pct": float(baseline_metrics.get("max_drawdown_pct", 0.0) or 0.0),
                    "win_rate": float(baseline_metrics.get("win_rate", 0.0) or 0.0),
                },
            }
        )
        return payload

    def _attach_governance_policies(self,
                                    symbol: str,
                                    regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                    evaluation_features_by_tf: Dict[str, pd.DataFrame],
                                    evaluation_warmup_by_tf: Dict[str, int],
                                    *,
                                    selection_surface: str = "stage3_selection") -> None:
        for timeframe, regimes in regime_configs.items():
            evaluation_features = evaluation_features_by_tf.get(timeframe)
            evaluation_warmup = int(evaluation_warmup_by_tf.get(timeframe, 0) or 0)
            for regime, cfg in regimes.items():
                cfg.governance_policy = self._select_governance_policy(
                    symbol,
                    timeframe,
                    regime,
                    cfg,
                    evaluation_features,
                    evaluation_warmup,
                    selection_surface=selection_surface,
                )

    def _write_oos_candidate_evidence(self,
                                      symbol: str,
                                      records: List[Dict[str, Any]],
                                      *,
                                      artifact_meta: Dict[str, Any],
                                      data_window_meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        evidence_dir = Path(self.config.output_dir) / str(
            getattr(self.config, "oos_evidence_dirname", "oos_evidence") or "oos_evidence"
        )
        evidence_dir.mkdir(parents=True, exist_ok=True)
        safe_symbol = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(symbol))
        unique_records = {str(record.get("candidate_id")): record for record in records if record.get("candidate_id")}
        ordered_records = sorted(unique_records.values(), key=lambda item: str(item.get("candidate_id", "")))
        manifest_path = evidence_dir / f"{safe_symbol}_oos_manifest.json"
        tmp_manifest_path = manifest_path.with_suffix(".json.tmp")
        if not ordered_records:
            returns = np.zeros((0, 0), dtype=np.float32)
            candidate_ids = np.asarray([], dtype=str)
            digest_payload = returns.tobytes() + json.dumps(
                candidate_ids.tolist(),
                separators=(",", ":"),
            ).encode("utf-8")
            matrix_digest = hashlib.sha1(digest_payload).hexdigest()
            matrix_path = evidence_dir / f"{safe_symbol}_oos_returns_{matrix_digest[:12]}.npz"
            if not matrix_path.exists():
                tmp_matrix_path = evidence_dir / f"{safe_symbol}_oos_returns_{matrix_digest[:12]}.tmp.npz"
                np.savez_compressed(
                    tmp_matrix_path,
                    returns=returns,
                    candidate_ids=candidate_ids,
                )
                tmp_matrix_path.replace(matrix_path)
            payload = {
                "format_version": "oos_candidate_matrix_v1",
                "scope": "final_candidate_pool",
                "purpose": "C3_PBO_false_discovery_measurement",
                "symbol": symbol,
                "created_at": datetime.now().isoformat(),
                "record_count": 0,
                "return_index": "relative_equal_blocks",
                "record_schema": "oos_candidate_returns_v4_one_touch_evidence",
                "completion_status": "NO_CANDIDATE_RECORDS",
                "completion_reason": "no candidates survived training eligibility/evidence gates",
                "matrix_contract": {
                    "returns_shape": [0, 0],
                    "rows": "no OOS periods because no candidates survived",
                    "columns": "no candidate columns",
                    "alignment": "not_applicable_empty_candidate_pool",
                    "regime_scope": "all_regimes_validation_equity",
                },
                "matrix_path": str(matrix_path),
                "matrix_digest_sha1": matrix_digest,
                "artifact_meta": artifact_meta,
                "data_window_fingerprint": (data_window_meta or {}).get("data_window_fingerprint"),
                "records": [],
            }
            with tmp_manifest_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, default=str)
            tmp_manifest_path.replace(manifest_path)
            logger.info(
                f"[{symbol}] Wrote empty OOS candidate evidence: {manifest_path} "
                "matrix=0x0 (no candidate records)"
            )
            return {
                "path": str(manifest_path),
                "format_version": "oos_candidate_matrix_v1",
                "record_count": 0,
                "returns_shape": [0, 0],
                "matrix_path": str(matrix_path),
                "matrix_digest_sha1": matrix_digest,
                "completion_status": "NO_CANDIDATE_RECORDS",
            }
        min_periods = min(int(record.get("period_count", 0) or 0) for record in ordered_records)
        min_required = int(getattr(self.config, "oos_evidence_min_periods", 8) or 8)
        if min_periods < min_required:
            logger.info(
                f"[{symbol}] OOS candidate evidence skipped: common periods {min_periods} < {min_required}"
            )
            return None

        returns = np.column_stack([
            np.asarray(record.get("period_returns", [])[-min_periods:], dtype=np.float32)
            for record in ordered_records
        ]).astype(np.float32, copy=False)
        candidate_ids = np.asarray([record["candidate_id"] for record in ordered_records], dtype=str)
        digest_payload = returns.tobytes() + json.dumps(
            candidate_ids.tolist(),
            separators=(",", ":"),
        ).encode("utf-8")
        matrix_digest = hashlib.sha1(digest_payload).hexdigest()
        matrix_path = evidence_dir / f"{safe_symbol}_oos_returns_{matrix_digest[:12]}.npz"
        if not matrix_path.exists():
            tmp_matrix_path = evidence_dir / f"{safe_symbol}_oos_returns_{matrix_digest[:12]}.tmp.npz"
            np.savez_compressed(
                tmp_matrix_path,
                returns=returns,
                candidate_ids=candidate_ids,
            )
            tmp_matrix_path.replace(matrix_path)

        manifest_records = []
        for record in ordered_records:
            compact = {key: value for key, value in record.items() if key != "period_returns"}
            compact["matrix_column"] = len(manifest_records)
            manifest_records.append(compact)
        payload = {
            "format_version": "oos_candidate_matrix_v1",
            "scope": "final_candidate_pool",
            "purpose": "C3_PBO_false_discovery_measurement",
            "symbol": symbol,
            "created_at": datetime.now().isoformat(),
            "record_count": len(ordered_records),
            "return_index": "relative_equal_blocks",
            "record_schema": "oos_candidate_returns_v4_one_touch_evidence",
            "matrix_contract": {
                "returns_shape": [int(returns.shape[0]), int(returns.shape[1])],
                "rows": "common trailing OOS periods",
                "columns": "candidate_id in manifest order",
                "alignment": "per-candidate relative OOS periods truncated to a common T",
                "regime_scope": "all_regimes_validation_equity",
            },
            "matrix_path": str(matrix_path),
            "matrix_digest_sha1": matrix_digest,
            "artifact_meta": artifact_meta,
            "data_window_fingerprint": (data_window_meta or {}).get("data_window_fingerprint"),
            "records": manifest_records,
        }
        with tmp_manifest_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        tmp_manifest_path.replace(manifest_path)
        logger.info(
            f"[{symbol}] Wrote OOS candidate evidence: {manifest_path} "
            f"matrix={returns.shape[0]}x{returns.shape[1]}"
        )
        return {
            "path": str(manifest_path),
            "format_version": "oos_candidate_matrix_v1",
            "record_count": len(ordered_records),
            "returns_shape": [int(returns.shape[0]), int(returns.shape[1])],
            "matrix_path": str(matrix_path),
            "matrix_digest_sha1": matrix_digest,
        }

    def _apply_promotion_statuses(self,
                                  regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                  default_config: Optional[RegimeConfig]) -> None:
        for regimes in regime_configs.values():
            for cfg in regimes.values():
                status, reasons = classify_promotion_status(cfg, self.config)
                cfg.promotion_status = status
                cfg.promotion_reasons = reasons
        if default_config is not None:
            status, reasons = classify_promotion_status(default_config, self.config)
            default_config.promotion_status = status
            default_config.promotion_reasons = reasons

    @staticmethod
    def _historical_severe_reasons(cfg: RegimeConfig) -> List[str]:
        historical = dict(getattr(cfg, "historical_audit_metrics", {}) or {})
        reasons: List[str] = []
        if bool(historical.get("severe_failure", False)):
            reasons.extend(str(reason) for reason in historical.get("failure_reasons", []) or [])
        return reasons

    @staticmethod
    def _regime_profitability_severe_reasons(cfg: RegimeConfig) -> List[str]:
        historical = dict(getattr(cfg, "historical_audit_metrics", {}) or {})
        reasons: List[str] = []
        regime_audit = historical.get("regime_profitability_audit")
        if isinstance(regime_audit, dict) and bool(regime_audit.get("severe_failure", False)):
            reasons.extend(str(reason) for reason in regime_audit.get("warnings", []) or [])
        return reasons

    def _v2_holdout_quality_reasons(self, cfg: RegimeConfig) -> List[str]:
        holdout = dict(getattr(cfg, "holdout_metrics", {}) or {})
        policy = str(getattr(self.config, "false_discovery_holdout_policy", "off") or "off").lower()
        split_mode = str(getattr(self.config, "production_split_mode", "bleeding_edge") or "bleeding_edge").lower()
        if split_mode == "bleeding_edge" and not holdout:
            return []
        if policy == "off":
            return []
        min_trades = int(getattr(self.config, "false_discovery_holdout_min_trades", 5) or 0)
        if str(holdout.get("validation_mode", "") or "").lower() == "purged_kfold":
            min_trades = int(getattr(self.config, "oos_min_trades", min_trades) or 0)
        trades = int(holdout.get("total_trades", 0) or 0)
        reasons: List[str] = []
        if 0 < min_trades and trades < min_trades:
            reasons.append(f"holdout trades {trades} < {min_trades}")
        if str(holdout.get("policy_status", "") or "").upper() == "FAIL":
            reasons.append(str(holdout.get("policy_reason", "") or "purged-kfold OOS policy failed"))
        if trades >= min_trades > 0:
            pf = float(holdout.get("profit_factor", 0.0) or 0.0)
            ret = float(holdout.get("total_return_pct", 0.0) or 0.0)
            min_pf = float(getattr(self.config, "false_discovery_holdout_min_profit_factor", 1.0) or 0.0)
            min_ret = float(getattr(self.config, "false_discovery_holdout_min_return_pct", 0.0) or 0.0)
            if pf < min_pf:
                reasons.append(f"holdout PF {pf:.2f} < {min_pf:.2f}")
            if ret < min_ret:
                reasons.append(f"holdout return {ret:.1f}% < {min_ret:.1f}%")
        return reasons

    def _v2_default_rejection_reasons(self,
                                      cfg: Optional[RegimeConfig],
                                      *,
                                      allow_promote_small: Optional[bool] = None) -> List[str]:
        if cfg is None or cfg.is_no_trade_marker():
            return ["no trade marker"]
        reasons: List[str] = []
        status = str(getattr(cfg, "promotion_status", "BLOCKED") or "BLOCKED").upper()
        allow_small = (
            bool(getattr(self.config, "specialist_spokes_v2_allow_promote_small_default", True))
            if allow_promote_small is None else bool(allow_promote_small)
        )
        if status == "BLOCKED":
            reasons.append("promotion BLOCKED")
        elif status == "PROMOTE_SMALL" and not allow_small:
            reasons.append("promotion PROMOTE_SMALL not allowed for v2 default")
        historical_severe_reasons = self._historical_severe_reasons(cfg)
        holdout_reasons = self._v2_holdout_quality_reasons(cfg)
        if historical_severe_reasons and holdout_reasons:
            reasons.append("historical severe confirmed by weak holdout")
            reasons.extend(historical_severe_reasons)
            reasons.extend(holdout_reasons)
        regime_severe_reasons = self._regime_profitability_severe_reasons(cfg)
        if regime_severe_reasons and holdout_reasons:
            reasons.append("regime severe confirmed by weak holdout")
            reasons.extend(regime_severe_reasons)
            reasons.extend(holdout_reasons)

        evidence = dict(getattr(cfg, "validation_evidence", {}) or {})
        false_discovery = dict(evidence.get("false_discovery", {}) or {})
        max_val_sharpe = float(getattr(self.config, "specialist_spokes_v2_max_validation_sharpe", 0.0) or 0.0)
        if max_val_sharpe > 0.0:
            if false_discovery and "val_sharpe" in false_discovery:
                val_sharpe = float(false_discovery.get("val_sharpe", 0.0) or 0.0)
            else:
                val_sharpe = float(
                    (getattr(cfg, "val_metrics", {}) or {}).get(
                        "sharpe_approx",
                        (getattr(cfg, "val_metrics", {}) or {}).get("sharpe_ratio", 0.0),
                    ) or 0.0
                )
            pbo_status = str(false_discovery.get("pbo_status", "") or "").lower()
            if val_sharpe > max_val_sharpe and pbo_status in {"", "not_available", "unavailable"}:
                reasons.append(f"validation Sharpe {val_sharpe:.2f} > {max_val_sharpe:.2f} with PBO unavailable")
        return reasons

    def _select_v2_symbol_default(self,
                                  regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                  current_default: Optional[RegimeConfig]
                                  ) -> Tuple[Optional[RegimeConfig], Optional[str], Optional[str], Dict[str, Any]]:
        candidates: List[Tuple[float, str, str, RegimeConfig]] = []
        for tf, regimes in regime_configs.items():
            for regime, cfg in regimes.items():
                if cfg is not None and not cfg.is_no_trade_marker():
                    candidates.append((float(cfg.quality_score or 0.0), tf, regime, cfg))
        candidates.sort(key=lambda item: item[0], reverse=True)

        def _reject_record(tf: str, regime: str, cfg: RegimeConfig, reasons: List[str]) -> Dict[str, Any]:
            return {
                "tf": tf,
                "regime": regime,
                "strategy": cfg.strategy_name,
                "promotion_status": str(getattr(cfg, "promotion_status", "BLOCKED") or "BLOCKED"),
                "quality_score": float(cfg.quality_score or 0.0),
                "reasons": reasons,
            }

        strict_rejected: List[Dict[str, Any]] = []
        for _score, tf, regime, cfg in candidates:
            reasons = self._v2_default_rejection_reasons(cfg, allow_promote_small=False)
            if not reasons:
                return cfg, tf, regime, {
                    "selected_tf": tf,
                    "selected_regime": regime,
                    "selected_strategy": cfg.strategy_name,
                    "selection_pass": "promoted_only",
                    "rejected_before_selection": strict_rejected,
                }
            strict_rejected.append(_reject_record(tf, regime, cfg, reasons))

        allow_small = bool(getattr(self.config, "specialist_spokes_v2_allow_promote_small_default", True))
        small_rejected: List[Dict[str, Any]] = []
        if allow_small:
            for _score, tf, regime, cfg in candidates:
                reasons = self._v2_default_rejection_reasons(cfg, allow_promote_small=True)
                if not reasons:
                    return cfg, tf, regime, {
                        "selected_tf": tf,
                        "selected_regime": regime,
                        "selected_strategy": cfg.strategy_name,
                        "selection_pass": "promote_small_allowed",
                        "rejected_before_selection": strict_rejected,
                        "fallback_rejected_before_selection": small_rejected,
                    }
                small_rejected.append(_reject_record(tf, regime, cfg, reasons))

        current_reasons = self._v2_default_rejection_reasons(
            current_default,
            allow_promote_small=allow_small,
        )
        if current_default is not None and not current_reasons:
            return current_default, None, None, {
                "selected_strategy": current_default.strategy_name,
                "selected_current_default": True,
                "selection_pass": "current_default",
                "rejected_before_selection": strict_rejected,
                "fallback_rejected_before_selection": small_rejected,
            }
        return None, None, None, {
            "selected_strategy": None,
            "rejected_before_selection": strict_rejected,
            "fallback_rejected_before_selection": small_rejected,
            "no_acceptable_default": True,
        }
    
    def run_for_symbol(self, symbol: str) -> PipelineResult:
        """
        Run regime-aware optimization pipeline for a single symbol.
        
        Finds best (strategy, params) for each (timeframe, regime) combination.
        
        Args:
            symbol: Symbol to optimize
            
        Returns:
            PipelineResult with SymbolConfig containing regime_configs
        """
        start_time = time.time()
        result = PipelineResult(symbol=symbol, success=False)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"OPTIMIZING: {symbol} (Regime-Aware)")
        logger.info(f"{'='*60}")
        
        try:
            # Load one base-M5 workflow window and derive all higher timeframes
            # from it so every timeframe shares the same calendar boundary.
            data_by_tf, data_window_meta = self._load_symbol_timeframe_data(symbol)
            result.data_window_meta = dict(data_window_meta or {})
            
            if not data_by_tf:
                native_gate = (
                    data_window_meta.get("native_source_gate")
                    if isinstance(data_window_meta, dict)
                    and isinstance(data_window_meta.get("native_source_gate"), dict)
                    else {}
                )
                gate_errors = native_gate.get("errors_by_timeframe") or {}
                if gate_errors:
                    detail = "; ".join(
                        f"{tf}: {', '.join(str(value) for value in reasons)}"
                        for tf, reasons in sorted(gate_errors.items())
                    )
                    result.error_message = f"Native source gate failed before search: {detail}"
                else:
                    result.error_message = "No valid data available"
                return result
            
            result.timeframes_tested = len(data_by_tf)
            
            # Get all strategies
            strategies = StrategyRegistry.get_all_instances()
            result.strategies_tested = len(strategies)
            artifact_meta = build_artifact_meta(self.config, strategies)
            
            # Stream one timeframe through optimization and audits at a time.
            # Holding M5 plus five higher-TF feature frames at once overwhelms
            # low-RAM discovery machines and is unnecessary for per-TF winners.
            regime_configs: Dict[str, Dict[str, RegimeConfig]] = {}
            default_config: Optional[RegimeConfig] = None
            best_overall_score = -float("inf")
            validated_count = 0
            unvalidated_count = 0
            oos_evidence_records: List[Dict[str, Any]] = []
            search_cell_outcomes_by_tf: Dict[str, List[Dict[str, Any]]] = {}
            edge_challenger_configs: Dict[str, Dict[str, Dict[str, Any]]] = {}
            edge_challenger_failures: List[str] = []
            
            regime_params_file = getattr(self.config, 'regime_params_file', 'regime_params.json')

            # The active-recent M5 workflow always emits data_window_meta; bail if it
            # didn't (means _load_symbol_timeframe_data returned an empty window).
            if not data_window_meta:
                logger.warning(f"[{symbol}] Empty workflow window; skipping symbol")
                result.error_message = "Empty workflow window"
                return result

            native_cell_eligibility = dict(
                data_window_meta.get("native_cell_eligibility", {}) or {}
            )
            excluded_bindings = dict(
                native_cell_eligibility.get("excluded_bindings", {}) or {}
            )
            for tf, binding in excluded_bindings.items():
                regime_configs[str(tf).upper()] = self._native_cell_exclusion_markers(
                    dict(binding or {})
                )

            windows = data_window_meta.get("windows", {})
            hist_meta = windows.get("historical_stress_audit", {})
            active_meta = windows.get("active_universe", {})
            base_meta = windows.get("base_selection", active_meta)
            wf_meta = windows.get("tuning_validation", windows.get("walk_forward_validation", {}))
            opt_train_meta = windows.get("optimizer_train", windows.get("optimization_train", active_meta))
            opt_train_warm_meta = windows.get(
                "optimizer_train_with_warmup",
                windows.get("optimization_train_with_warmup", opt_train_meta),
            )
            stage2_meta = windows.get("stage2_selection", {})
            stage2_warm_meta = windows.get("stage2_selection_with_warmup", {})
            stage2_train_meta = windows.get("stage2_training_selection", stage2_meta)
            stage2_train_warm_meta = windows.get("stage2_training_selection_with_warmup", stage2_warm_meta)
            holdout_meta = windows.get("final_lockbox", windows.get("clean_holdout", {}))
            holdout_warm_meta = windows.get(
                "final_lockbox_with_warmup",
                windows.get("clean_holdout_with_warmup", {}),
            )

            hist_start = pd.Timestamp(hist_meta["start_ts"]) if hist_meta.get("start_ts") else None
            hist_end = pd.Timestamp(active_meta["start_ts"]) if active_meta.get("start_ts") else None
            active_start = pd.Timestamp(active_meta["start_ts"]) if active_meta.get("start_ts") else None
            active_end = pd.Timestamp(active_meta["end_ts"]) if active_meta.get("end_ts") else None
            base_start = pd.Timestamp(base_meta["start_ts"]) if base_meta.get("start_ts") else active_start
            base_end = pd.Timestamp(base_meta["end_ts"]) if base_meta.get("end_ts") else active_end
            wf_start = pd.Timestamp(wf_meta["start_ts"]) if wf_meta.get("start_ts") else None
            wf_end = pd.Timestamp(wf_meta["end_ts"]) if wf_meta.get("end_ts") else None
            opt_train_start = pd.Timestamp(opt_train_meta["start_ts"]) if opt_train_meta.get("start_ts") else active_start
            opt_train_end = pd.Timestamp(opt_train_meta["end_ts"]) if opt_train_meta.get("end_ts") else None
            opt_train_warm_start = (
                pd.Timestamp(opt_train_warm_meta["start_ts"])
                if opt_train_warm_meta.get("start_ts") else opt_train_start
            )
            holdout_start = pd.Timestamp(holdout_meta["start_ts"]) if holdout_meta.get("start_ts") else None
            holdout_end = pd.Timestamp(holdout_meta["end_ts"]) if holdout_meta.get("end_ts") else active_end
            stage2_start = pd.Timestamp(stage2_meta["start_ts"]) if stage2_meta.get("start_ts") else active_start
            stage2_warm_start = (
                pd.Timestamp(stage2_warm_meta["start_ts"])
                if stage2_warm_meta.get("start_ts") else stage2_start
            )
            stage2_train_end = (
                pd.Timestamp(stage2_train_meta["end_ts"])
                if stage2_train_meta.get("end_ts")
                else (holdout_start if holdout_start is not None else None)
            )
            stage2_train_start = (
                pd.Timestamp(stage2_train_meta["start_ts"])
                if stage2_train_meta.get("start_ts") else stage2_start
            )
            stage2_train_warm_start = (
                pd.Timestamp(stage2_train_warm_meta["start_ts"])
                if stage2_train_warm_meta.get("start_ts") else stage2_warm_start
            )
            holdout_warm_start = (
                pd.Timestamp(holdout_warm_meta["start_ts"])
                if holdout_warm_meta.get("start_ts") else holdout_start
            )
            split_mode = str(
                getattr(self.config, "production_split_mode", CALENDAR_SPLIT_MODE)
                or CALENDAR_SPLIT_MODE
            ).lower()

            per_timeframe_windows = dict(
                data_window_meta.get("timeframe_windows", {}) or {}
            )

            def _timeframe_geometry(timeframe: str) -> Dict[str, Optional[pd.Timestamp]]:
                tf_payload = dict(per_timeframe_windows.get(str(timeframe).upper()) or {})
                tf_windows = dict(tf_payload.get("windows") or windows)

                def _meta(primary: str, fallback: Optional[str] = None) -> Dict[str, Any]:
                    value = tf_windows.get(primary)
                    if not isinstance(value, dict) and fallback:
                        value = tf_windows.get(fallback)
                    return dict(value or {}) if isinstance(value, dict) else {}

                def _ts(meta: Dict[str, Any], key: str) -> Optional[pd.Timestamp]:
                    value = meta.get(key)
                    return pd.Timestamp(value) if value not in (None, "") else None

                tf_active = _meta("active_universe")
                tf_base = _meta("base_selection") or tf_active
                tf_validation = _meta("tuning_validation", "walk_forward_validation")
                tf_train = _meta("optimizer_train", "optimization_train") or tf_active
                tf_train_warm = (
                    _meta("optimizer_train_with_warmup", "optimization_train_with_warmup")
                    or tf_train
                )
                tf_stage2 = _meta("stage2_selection") or tf_validation
                tf_stage2_warm = _meta("stage2_selection_with_warmup") or tf_stage2
                tf_stage2_train = _meta("stage2_training_selection") or tf_stage2
                tf_stage2_train_warm = (
                    _meta("stage2_training_selection_with_warmup") or tf_stage2_warm
                )
                tf_holdout = _meta("final_lockbox", "clean_holdout")
                tf_holdout_warm = _meta(
                    "final_lockbox_with_warmup", "clean_holdout_with_warmup"
                )
                tf_hist = _meta("historical_stress_audit")
                return {
                    "hist_start": _ts(tf_hist, "start_ts"),
                    "hist_end": _ts(tf_active, "start_ts"),
                    "active_start": _ts(tf_active, "start_ts"),
                    "active_end": _ts(tf_active, "end_ts"),
                    "base_start": _ts(tf_base, "start_ts"),
                    "base_end": _ts(tf_base, "end_ts"),
                    "validation_start": _ts(tf_validation, "start_ts"),
                    "validation_end": _ts(tf_validation, "end_ts"),
                    "opt_train_start": _ts(tf_train, "start_ts"),
                    "opt_train_end": _ts(tf_train, "end_ts"),
                    "opt_train_warm_start": (
                        _ts(tf_train_warm, "start_ts") or _ts(tf_train, "start_ts")
                    ),
                    "stage2_start": _ts(tf_stage2, "start_ts"),
                    "stage2_warm_start": (
                        _ts(tf_stage2_warm, "start_ts") or _ts(tf_stage2, "start_ts")
                    ),
                    "stage2_train_start": (
                        _ts(tf_stage2_train, "start_ts") or _ts(tf_stage2, "start_ts")
                    ),
                    "stage2_train_end": (
                        _ts(tf_stage2_train, "end_ts") or _ts(tf_holdout, "start_ts")
                    ),
                    "stage2_train_warm_start": (
                        _ts(tf_stage2_train_warm, "start_ts")
                        or _ts(tf_stage2_warm, "start_ts")
                    ),
                    "holdout_start": _ts(tf_holdout, "start_ts"),
                    "holdout_end": (
                        _ts(tf_holdout, "end_ts") or _ts(tf_active, "end_ts")
                    ),
                    "holdout_warm_start": (
                        _ts(tf_holdout_warm, "start_ts")
                        or _ts(tf_holdout, "start_ts")
                    ),
                }

            # Regime labels determine every specialist bucket and are therefore
            # part of model selection.  In chronological-lockbox mode, reject
            # the symbol before computing a feature or ranking a candidate when
            # the detector parameters cannot be proven to come from this exact
            # optimizer-train source/window. Calendar v2 is native-timeframe
            # authoritative; legacy v1 remains M5-derived.
            non_search_timeframes: Set[str] = set()
            fit_observed_by_tf: Dict[str, List[str]] = {}
            if split_mode in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
                provenance_checks: Dict[str, Dict[str, Any]] = {}
                current_source = dict(data_window_meta.get("source", {}) or {})
                timeframe_sources = dict(data_window_meta.get("timeframe_sources", {}) or {})
                for tf, tf_data in data_by_tf.items():
                    if split_mode == CALENDAR_SPLIT_MODE:
                        geometry = _timeframe_geometry(tf)
                        expected_source = dict(timeframe_sources.get(tf, {}) or {})
                        expected_train_window = calendar_window_meta(
                            tf_data,
                            geometry["opt_train_start"],
                            geometry["opt_train_end"],
                        )
                        expected_contract = str(
                            (
                                per_timeframe_windows.get(str(tf).upper(), {}) or {}
                            ).get("split_contract_version")
                            or data_window_meta.get("split_contract_version")
                            or EVIDENCE_CONTRACT_VERSION
                        )
                    else:
                        expected_source = current_source
                        expected_train_window = opt_train_meta
                        expected_contract = DataSplitter.SPLIT_CONTRACT_VERSION
                    check = validate_regime_params_provenance(
                        symbol,
                        tf,
                        regime_params_file,
                        expected_source=expected_source,
                        expected_train_window=expected_train_window,
                        expected_split_contract=expected_contract,
                        expected_activation_id=(
                            native_cell_eligibility.get("activation_id")
                            if split_mode == CALENDAR_SPLIT_MODE
                            else None
                        ),
                        expected_activation_manifest_sha256=(
                            native_cell_eligibility.get(
                                "activation_manifest_sha256"
                            )
                            if split_mode == CALENDAR_SPLIT_MODE
                            else None
                        ),
                        require_complete_generation=(
                            split_mode == CALENDAR_SPLIT_MODE
                        ),
                    )
                    # seq1024: never trust the returned receipt - recompute
                    # counts/vector from the exact optimizer-train surface.
                    support = check.get("train_regime_support")
                    if (
                        check.get("valid")
                        and check.get("status") == "FIT"
                        and support is not None
                        and split_mode == CALENDAR_SPLIT_MODE
                    ):
                        train_index = ensure_utc_index(tf_data, copy=False).index
                        s_idx = int(
                            train_index.searchsorted(
                                pd.Timestamp(geometry["opt_train_start"]),
                                side="left",
                            )
                        )
                        e_idx = int(
                            train_index.searchsorted(
                                pd.Timestamp(geometry["opt_train_end"]),
                                side="left",
                            )
                        )
                        recomputed = recompute_train_regime_support_digest(
                            tf_data.iloc[s_idx:e_idx],
                            load_regime_params_cell(
                                regime_params_file, symbol, tf
                            )
                            or {},
                        )
                        record_body = check.get("record") or {}
                        bindings = (support or {}).get("bindings") or {}
                        record_source_sha = str(
                            ((record_body.get("source") or {}).get("sha256"))
                            or ""
                        ).lower()
                        record_train_sha = hashlib.sha256(
                            json.dumps(
                                record_body.get("train") or {},
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        ).hexdigest()
                        code_sha = detector_code_sha256()
                        if (
                            recomputed is None
                            or recomputed["counts"] != support["counts"]
                            or recomputed["vector_sha256"]
                            != support["vector_sha256"]
                            or recomputed["eligible_rows"]
                            != support["eligible_rows"]
                            or recomputed["first_ts"] != support["first_ts"]
                            or recomputed["last_ts"] != support["last_ts"]
                            or str(bindings.get("source_sha256") or "").lower()
                            != record_source_sha
                            or bindings.get("train_window_sha256")
                            != record_train_sha
                            or code_sha is None
                            or bindings.get("detector_code_sha256") != code_sha
                        ):
                            check = {
                                "valid": False,
                                "status": "FIT",
                                "reason": (
                                    "regime train support recompute mismatch"
                                ),
                            }
                    provenance_checks[str(tf).upper()] = {
                        "valid": bool(check.get("valid", False)),
                        "status": str(check.get("status", "") or ""),
                        "reason": str(check.get("reason", "unknown")),
                        "decision_kind": check.get("decision_kind"),
                        # seq981: exact binding so later grid validation can
                        # re-verify the typed NON_SEARCH decision by hash.
                        "decision_sha256": (
                            (check.get("record") or {}).get("decision_sha256")
                        ),
                        "generation_terminal": (
                            (check.get("record") or {}).get("generation_terminal")
                        ),
                        "reason_code": (
                            (check.get("record") or {}).get("reason_code")
                        ),
                        # seq1005: exact train-support binding for FIT cells.
                        "support_receipt_sha256": (
                            (check.get("train_regime_support") or {}).get(
                                "receipt_sha256"
                            )
                        ),
                        "observed_regimes": list(
                            (check.get("train_regime_support") or {}).get(
                                "observed_regimes", []
                            )
                        ),
                        "unobserved_regimes": list(
                            (check.get("train_regime_support") or {}).get(
                                "unobserved_regimes", []
                            )
                        ),
                        "provenance_path": check.get("provenance_path"),
                    }
                data_window_meta = dict(data_window_meta or {})
                data_window_meta["regime_provenance_gate"] = provenance_checks
                result.data_window_meta = dict(data_window_meta)
                # seq936/937: a typed NON_SEARCH decision is a VALID
                # authorization outcome, not a gate failure.  The cell is
                # skipped from strategy search with the ledger untouched;
                # it is never an economic NO_TRADE.
                non_search_timeframes = {
                    tf
                    for tf, check in provenance_checks.items()
                    if check["valid"] and check["status"] == "NON_SEARCH"
                }
                # seq1005: receipt-observed labels per FIT timeframe.
                fit_observed_by_tf = {
                    tf: list(check.get("observed_regimes") or [])
                    for tf, check in provenance_checks.items()
                    if check["valid"]
                    and check["status"] == "FIT"
                    and check.get("support_receipt_sha256")
                }
                if non_search_timeframes:
                    logger.info(
                        f"[{symbol}] Typed NON_SEARCH timeframes (skipped from "
                        f"search, ledger unchanged): "
                        f"{sorted(non_search_timeframes)}"
                    )
                failed = {
                    tf: check for tf, check in provenance_checks.items()
                    if not check["valid"]
                }
                if failed:
                    reasons = "; ".join(
                        f"{tf}: {check['reason']}" for tf, check in sorted(failed.items())
                    )
                    result.error_message = f"Regime detector provenance gate failed: {reasons}"
                    logger.error(f"[{symbol}] {result.error_message}")
                    return result

            import gc

            for tf in tuple(data_by_tf):
                # Transfer ownership of one native frame into this iteration.
                # Completed timeframes are released instead of retaining all
                # six full histories until the symbol finishes.
                data = data_by_tf.pop(tf)
                if str(tf).upper() in non_search_timeframes:
                    # Typed NON_SEARCH authorization (seq936): no strategy
                    # search, no ledger leaves, never NO_TRADE.  The typed
                    # decision already lives in the provenance sidecar and in
                    # data_window_meta["regime_provenance_gate"].
                    del data
                    continue
                geometry = _timeframe_geometry(tf)
                hist_start = geometry["hist_start"]
                hist_end = geometry["hist_end"]
                active_start = geometry["active_start"]
                active_end = geometry["active_end"]
                base_start = geometry["base_start"] or active_start
                base_end = geometry["base_end"] or active_end
                opt_train_start = geometry["opt_train_start"] or active_start
                opt_train_end = geometry["opt_train_end"]
                opt_train_warm_start = (
                    geometry["opt_train_warm_start"] or opt_train_start
                )
                stage2_start = geometry["stage2_start"]
                stage2_warm_start = geometry["stage2_warm_start"] or stage2_start
                stage2_train_start = (
                    geometry["stage2_train_start"] or stage2_start
                )
                stage2_train_end = geometry["stage2_train_end"]
                stage2_train_warm_start = (
                    geometry["stage2_train_warm_start"] or stage2_warm_start
                )
                holdout_start = geometry["holdout_start"]
                holdout_end = geometry["holdout_end"] or active_end
                holdout_warm_start = (
                    geometry["holdout_warm_start"] or holdout_start
                )
                validation_start = (
                    geometry["validation_start"] or stage2_train_start
                )
                validation_end = geometry["validation_end"] or stage2_train_end
                if bool(getattr(self.config, "evidence_sufficiency_enabled", True)):
                    evidence_frames = {
                        "optimizer_train": self._slice_time_window(
                            data, opt_train_start, opt_train_end
                        ),
                        "tuning_validation": self._slice_time_window(
                            data, validation_start, validation_end
                        ),
                        "final_lockbox": (
                            self._slice_time_window(data, holdout_start, holdout_end)
                            if holdout_start is not None
                            else pd.DataFrame()
                        ),
                    }
                    evidence_verdict = self._timeframe_evidence_verdict(
                        tf, evidence_frames, data_window_meta
                    )
                    data_window_meta.setdefault("timeframe_evidence", {})[tf] = (
                        evidence_verdict.to_dict()
                    )
                    result.data_window_meta = dict(data_window_meta)
                    del evidence_frames
                    if not evidence_verdict.passed:
                        # seq931 invariant: a failed EvidenceVerdict is a
                        # TECHNICAL/data-integrity state, never a trading
                        # verdict.  It must not become NO_TRADE leaves and the
                        # winner ledger must stay byte-unchanged; the symbol
                        # run reports unsuccessful with a typed reason so the
                        # cell can be repaired and re-run.
                        result.error_message = (
                            "TECHNICAL_EVIDENCE_FAILURE:"
                            f"{str(tf).upper()}:{evidence_verdict.concise_reason()}"
                        )
                        logger.error(
                            f"[{symbol}] [{tf}] Evidence gate failed before "
                            f"feature/search; typed technical failure, ledger "
                            f"unchanged: {evidence_verdict.concise_reason()}"
                        )
                        return result

                # Phase A never computes features on final-lockbox prices.
                # This is stronger than relying on every current feature to be
                # causal and prevents a future whole-series transform from
                # leaking holdout outcomes into selection.  Raw final rows are
                # used above only for provenance/count sufficiency metadata.
                feature_input = self._selection_feature_input(
                    data,
                    split_mode=split_mode,
                    final_lockbox_start=holdout_start,
                )
                full_features = FeatureComputer.compute_all(
                    feature_input, symbol=symbol, timeframe=tf,
                    regime_params_file=regime_params_file
                )
                # Candidate eligibility is part of model selection. In lockbox
                # mode it may only see optimizer-train data; exposing validation
                # or final-lockbox rows here would contaminate the candidate pool
                # before strategy/parameter selection even begins.
                eligibility_start, eligibility_end = self._candidate_eligibility_bounds(
                    split_mode,
                    base_start,
                    base_end,
                    opt_train_start,
                    opt_train_end,
                )
                eligibility_context_start = eligibility_start
                if (
                    opt_train_warm_start is not None
                    and (
                        eligibility_context_start is None
                        or opt_train_warm_start < eligibility_context_start
                    )
                ):
                    eligibility_context_start = opt_train_warm_start
                eligibility_features, eligibility_warmup_bars = (
                    self._strategy_evaluation_context(
                        full_features,
                        context_start=eligibility_context_start,
                        evaluation_start=eligibility_start,
                        evaluation_end=eligibility_end,
                    )
                )
                active_features, train_warmup_bars = self._strategy_evaluation_context(
                    full_features,
                    context_start=opt_train_warm_start,
                    evaluation_start=opt_train_start,
                    evaluation_end=opt_train_end,
                )
                validation_context_start = (
                    stage2_train_warm_start or stage2_warm_start
                )
                stage2_features, validation_warmup_bars = (
                    self._strategy_evaluation_context(
                        full_features,
                        context_start=validation_context_start,
                        evaluation_start=validation_start,
                        evaluation_end=validation_end,
                    )
                )
                historical_features = self._slice_time_window(full_features, hist_start, hist_end)
                holdout_features = (
                    self._slice_time_window(full_features, holdout_warm_start, holdout_end)
                    if holdout_start is not None else pd.DataFrame()
                )

                scored_train_rows = max(0, len(active_features) - train_warmup_bars)
                if scored_train_rows < 200:
                    # seq1035: runtime feature-row insufficiency has no
                    # authenticated typed schema, so it stays TECHNICAL:
                    # unsuccessful, typed, ledger untouched, retryable.
                    result.error_message = (
                        "TECHNICAL_SEARCH_PREREQUISITE:"
                        f"{str(tf).upper()}:train_feature_rows="
                        f"{scored_train_rows}<200"
                    )
                    logger.error(f"[{symbol}] {result.error_message}")
                    del full_features, eligibility_features, active_features, stage2_features, historical_features, holdout_features
                    FeatureComputer.clear_cache()
                    gc.collect()
                    return result

                train_features_by_tf = {tf: active_features}
                train_warmup_by_tf = {tf: train_warmup_bars}
                scored_eligibility_rows = max(
                    0, len(eligibility_features) - eligibility_warmup_bars
                )
                eligibility_features_by_tf = (
                    {tf: eligibility_features}
                    if scored_eligibility_rows >= 200 else {}
                )
                val_features_by_tf: Dict[str, pd.DataFrame] = {}
                val_warmup_by_tf: Dict[str, int] = {}
                selection_features_by_tf: Dict[str, pd.DataFrame] = {}
                selection_warmup_by_tf: Dict[str, int] = {}
                selection_surface_by_tf: Dict[str, str] = {}
                scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
                threadspark_surface = str(
                    getattr(self.config, "threadspark_selection_surface", "validation") or "validation"
                ).lower()
                # A final lockbox is a one-shot verdict surface. It must never be
                # exposed to a candidate-ranking model such as ThreadSpark.
                if split_mode in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
                    threadspark_surface = "validation"
                threadspark_history_scope = str(
                    getattr(self.config, "threadspark_historical_cost_scope", "workflow") or "workflow"
                ).lower()
                threadspark_historical_features = (
                    full_features
                    if (
                        split_mode not in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}
                        and scoring_model == "specialist_spokes_threadspark"
                        and threadspark_history_scope == "workflow"
                    )
                    else historical_features
                )
                scored_validation_rows = max(
                    0, len(stage2_features) - validation_warmup_bars
                )
                if scored_validation_rows >= 50:
                    val_features_by_tf[tf] = stage2_features
                    val_warmup_by_tf[tf] = validation_warmup_bars
                    if not (
                        scoring_model == "specialist_spokes_threadspark"
                        and threadspark_surface == "clean_holdout"
                    ):
                        selection_features_by_tf[tf] = stage2_features
                        selection_warmup_by_tf[tf] = val_warmup_by_tf[tf]
                        selection_surface_by_tf[tf] = "validation"

                if (
                    threadspark_surface == "clean_holdout"
                    and holdout_start is not None
                    and len(holdout_features) >= 50
                ):
                    selection_features_by_tf[tf] = holdout_features
                    selection_warmup_by_tf[tf] = int((holdout_features.index < holdout_start).sum())
                    selection_surface_by_tf[tf] = "clean_holdout"
                elif (
                    scoring_model == "specialist_spokes_threadspark"
                    and threadspark_surface == "clean_holdout"
                ):
                    selection_surface_by_tf[tf] = "clean_holdout"

                with Timer(f"[{symbol}] [{tf}] Regime Optimization"):
                    tf_regime_configs, tf_default_config, tf_validated, tf_unvalidated = self.regime_optimizer.optimize_symbol(
                        symbol,
                        train_features_by_tf,
                        val_features_by_tf,
                        strategies,
                        train_warmup_by_tf=train_warmup_by_tf,
                        val_warmup_by_tf=val_warmup_by_tf,
                        eligibility_features_by_tf=eligibility_features_by_tf,
                        selection_features_by_tf=selection_features_by_tf,
                        selection_warmup_by_tf=selection_warmup_by_tf,
                        selection_surface_by_tf=selection_surface_by_tf,
                        historical_features_by_tf={tf: threadspark_historical_features} if len(threadspark_historical_features) >= 50 else {},
                        allowed_regimes_by_tf=(
                            {tf: fit_observed_by_tf[tf]}
                            if tf in fit_observed_by_tf
                            else None
                        ),
                    )

                regime_configs.update(tf_regime_configs)
                validated_count += int(tf_validated or 0)
                unvalidated_count += int(tf_unvalidated or 0)
                oos_evidence_records.extend(self.regime_optimizer.consume_oos_candidate_evidence())
                # seq950: capture the exact per-cell search outcome ledger for
                # this timeframe NOW — it survives even when every candidate
                # is gated out before any OOS evidence record exists.
                search_cell_outcomes_by_tf[str(tf).upper()] = [
                    dict(cell)
                    for cell in (
                        getattr(
                            self.regime_optimizer,
                            "_last_search_cell_outcomes",
                            [],
                        )
                        or []
                    )
                ]

                if tf_default_config is not None and tf_default_config.quality_score > best_overall_score:
                    best_overall_score = float(tf_default_config.quality_score)
                    default_config = tf_default_config

                if tf_regime_configs:
                    oos_mode = str(getattr(self.config, "oos_validation_mode", "fixed_holdout") or "fixed_holdout").lower()
                    if oos_mode == "purged_kfold":
                        self._attach_purged_kfold_oos_metrics(
                            symbol,
                            tf_regime_configs,
                            val_features_by_tf,
                            val_warmup_by_tf,
                        )
                    elif (
                        split_mode != CALENDAR_SPLIT_MODE
                        and holdout_start is not None
                        and len(holdout_features) >= 50
                    ):
                        self._attach_holdout_metrics(
                            symbol,
                            tf_regime_configs,
                            {tf: holdout_features},
                            {tf: int((holdout_features.index < holdout_start).sum())},
                            data_window_meta=data_window_meta,
                        )
                    if len(historical_features) >= 50:
                        self._attach_historical_stress_audit(
                            symbol,
                            tf_regime_configs,
                            {tf: historical_features},
                        )
                    self._attach_walk_forward_audit(
                        symbol,
                        tf_regime_configs,
                        val_features_by_tf,
                        val_warmup_by_tf,
                    )
                    risk_selection_stage = str(
                        getattr(self.config, "risk_management_selection_stage", "stage3") or "stage3"
                    ).lower()
                    if risk_selection_stage == "stage2":
                        governance_features_by_tf = eligibility_features_by_tf or train_features_by_tf
                        governance_warmup_by_tf = {
                            tf: (
                                eligibility_warmup_bars
                                if eligibility_features_by_tf
                                else train_warmup_bars
                            )
                        }
                        governance_surface = "stage2_active_universe"
                    else:
                        governance_features_by_tf = train_features_by_tf
                        governance_warmup_by_tf = {tf: train_warmup_bars}
                        governance_surface = "bleeding_edge_optimization"
                    self._attach_governance_policies(
                        symbol,
                        tf_regime_configs,
                        governance_features_by_tf,
                        governance_warmup_by_tf,
                        selection_surface=governance_surface,
                    )

                    # A calendar-lockbox production run is Phase A only.  It
                    # must not read final outcomes or create a latest-data
                    # refit.  Phase B is the sole synchronized one-shot access
                    # to that partition and can only accept/reject this roster.
                    # A later latest-data challenger is built only after the
                    # joint verdict and remains a physically separate demo book.

                self._log_regime_coverage(symbol, train_features_by_tf, val_features_by_tf)
                del train_features_by_tf, train_warmup_by_tf
                del val_features_by_tf, val_warmup_by_tf
                del eligibility_features_by_tf
                del full_features, eligibility_features, active_features, stage2_features, historical_features, holdout_features
                FeatureComputer.clear_cache()
                gc.collect()

            oos_evidence_pointer = self._write_oos_candidate_evidence(
                symbol,
                oos_evidence_records,
                artifact_meta=artifact_meta,
                data_window_meta=data_window_meta,
            ) if bool(getattr(self.config, "oos_evidence_export_enabled", False)) else None
            if oos_evidence_pointer:
                data_window_meta = dict(data_window_meta or {})
                data_window_meta["oos_candidate_evidence"] = oos_evidence_pointer
            
            # Count only tradeable validated winners; explicit NO_TRADE markers are slots, not winners.
            total_winners = sum(
                1
                for regimes in regime_configs.values()
                for cfg in regimes.values()
                if cfg is not None and not cfg.is_no_trade_marker()
            )
            result.regime_winners = total_winners

            # seq932/seq950: exact trial telemetry from the search outcome
            # ledger (primary) plus OOS records (survivor cross-check).
            # param_combos_tested reports COMPLETED trials.
            telemetry = _aggregate_search_trial_telemetry(
                search_cell_outcomes_by_tf, oos_evidence_records
            )
            result.param_combos_tested = int(
                telemetry["completed_search_trials_total"]
            )
            data_window_meta = dict(data_window_meta or {})
            data_window_meta["search_trial_telemetry"] = telemetry
            result.data_window_meta = dict(data_window_meta)
            for anomaly in telemetry["telemetry_anomalies"]:
                logger.warning(f"[{symbol}] search telemetry anomaly: {anomaly}")
            if telemetry["fatal_telemetry_errors"]:
                # seq979: impossible/garbage trial accounting is an
                # institutional invariant violation.  Fail typed BEFORE any
                # SymbolConfig exists so governed generation preserves the
                # live winner ledger; never promote on advisory warnings.
                result.error_message = (
                    "TECHNICAL_SEARCH_TELEMETRY_FAILURE:"
                    + ";".join(telemetry["fatal_telemetry_errors"])
                )
                logger.error(f"[{symbol}] {result.error_message}")
                return result

            configured_grid = tuple(
                str(value or "").strip().upper()
                for value in (getattr(self.config, "timeframes", []) or [])
                if str(value or "").strip()
            )
            strategy_timeframes = set(configured_grid)
            if split_mode == CALENDAR_SPLIT_MODE and native_cell_eligibility:
                candidate_timeframes = {
                    str(value).upper()
                    for value in (
                        native_cell_eligibility.get(
                            "evidence_candidate_timeframes",
                            native_cell_eligibility.get("active_timeframes", []),
                        )
                        or []
                    )
                }
                unavailable_timeframes = {
                    str(value).upper()
                    for value in (
                        native_cell_eligibility.get(
                            "source_unavailable_timeframes", []
                        )
                        or []
                    )
                }
                legacy_excluded_timeframes = {
                    str(value).upper()
                    for value in (
                        native_cell_eligibility.get("excluded_timeframes", []) or []
                    )
                }
                if (
                    candidate_timeframes
                    | unavailable_timeframes
                    | legacy_excluded_timeframes
                ) != set(configured_grid) or (
                    candidate_timeframes & unavailable_timeframes
                    or candidate_timeframes & legacy_excluded_timeframes
                    or unavailable_timeframes & legacy_excluded_timeframes
                ):
                    result.error_message = (
                        "Authenticated native-cell partition changed during optimization"
                    )
                    logger.error(f"[{symbol}] {result.error_message}")
                    return result
                if (
                    unavailable_timeframes
                    and native_cell_eligibility.get(
                        "acquisition_emits_trading_verdicts"
                    )
                    is not False
                ):
                    result.error_message = (
                        "Native data-state receipt permits acquisition trading verdicts"
                    )
                    logger.error(f"[{symbol}] {result.error_message}")
                    return result
                strategy_timeframes = (
                    candidate_timeframes | legacy_excluded_timeframes
                )
            expected_regimes = {"TREND", "RANGE", "BREAKOUT", "CHOP"}
            # seq981: search-authorized timeframes = native candidates MINUS
            # activation-bound validated NON_SEARCH decisions.  A NON_SEARCH
            # timeframe must have NO strategy leaves; its typed decision is
            # its complete accounting, and it never counts as missing.
            search_authorized_timeframes = (
                set(strategy_timeframes) - non_search_timeframes
            )
            non_search_with_leaves = sorted(
                non_search_timeframes & set(regime_configs)
            )
            missing_timeframes = sorted(
                search_authorized_timeframes - set(regime_configs)
            )
            unexpected_timeframes = sorted(
                set(regime_configs) - search_authorized_timeframes
            )
            # seq1005: leaf keys must EXACTLY equal the receipt-observed
            # labels for FIT timeframes (honest no-winner leaves included);
            # receipt-unobserved labels must be absent.  Leaf inference is
            # never the support authority; TFs without a receipt (legacy)
            # keep the nonempty-subset rule.
            def _tf_leaves_valid(tf: str) -> bool:
                leaves = set(regime_configs.get(tf, {}) or {})
                if tf in fit_observed_by_tf:
                    return leaves == set(fit_observed_by_tf[tf])
                return bool(leaves) and leaves <= expected_regimes

            malformed_timeframes = sorted(
                tf
                for tf in search_authorized_timeframes & set(regime_configs)
                if not _tf_leaves_valid(tf)
            )
            regime_support = {
                tf: {
                    "supported": (
                        list(fit_observed_by_tf[tf])
                        if tf in fit_observed_by_tf
                        else sorted(regime_configs.get(tf, {}))
                    ),
                    "unsupported": sorted(
                        expected_regimes
                        - set(
                            fit_observed_by_tf.get(
                                tf, list(regime_configs.get(tf, {}))
                            )
                        )
                    ),
                    "authority": (
                        "train_regime_support_receipt"
                        if tf in fit_observed_by_tf
                        else "legacy_leaf_keys"
                    ),
                }
                for tf in sorted(search_authorized_timeframes & set(regime_configs))
            }
            data_window_meta = dict(data_window_meta or {})
            data_window_meta["regime_support"] = regime_support
            result.data_window_meta = dict(data_window_meta)
            if (
                missing_timeframes
                or unexpected_timeframes
                or malformed_timeframes
                or non_search_with_leaves
            ):
                result.error_message = (
                    "Independent-cell grid incomplete after optimization: "
                    f"missing_timeframes={missing_timeframes}, "
                    f"unexpected_timeframes={unexpected_timeframes}, "
                    f"malformed_timeframes={malformed_timeframes}, "
                    f"non_search_with_leaves={non_search_with_leaves}"
                )
                logger.error(f"[{symbol}] {result.error_message}")
                return result

            # seq981: an all-NON_SEARCH symbol is a typed data/search-
            # authorization state.  It must never manufacture a default
            # NO_TRADE (the forbidden economic verdict) and never touch the
            # winner ledger; it returns unsuccessful with a typed reason so
            # governed generation records it for repair/re-fit.
            if non_search_timeframes and not search_authorized_timeframes:
                kinds = sorted(
                    str(
                        (data_window_meta.get("regime_provenance_gate", {}) or {})
                        .get(tf, {})
                        .get("decision_kind")
                        or (data_window_meta.get("non_search_decisions", {}) or {})
                        .get(tf, {})
                        .get("decision_kind")
                        or "UNKNOWN"
                    )
                    for tf in sorted(non_search_timeframes)
                )
                gate = dict(
                    data_window_meta.get("regime_provenance_gate", {}) or {}
                )
                decisions = {
                    tf_key: {
                        "decision_kind": (gate.get(tf_key, {}) or {}).get(
                            "decision_kind"
                        ),
                        "decision_sha256": (gate.get(tf_key, {}) or {}).get(
                            "decision_sha256"
                        ),
                        "reason_code": (gate.get(tf_key, {}) or {}).get(
                            "reason_code"
                        ),
                        "generation_terminal": (gate.get(tf_key, {}) or {}).get(
                            "generation_terminal"
                        ),
                    }
                    for tf_key in sorted(non_search_timeframes)
                }
                candidate_state = {
                    "kind": "ALL_TIMEFRAMES_NON_SEARCH",
                    "timeframes": sorted(non_search_timeframes),
                    "decisions": decisions,
                    "generation_terminal": all(
                        d.get("generation_terminal") is True
                        for d in decisions.values()
                    ),
                }
                typed_state = _validated_symbol_data_state(candidate_state)
                if typed_state is None:
                    # Unbound/retryable/incoherent decisions: technical.
                    result.error_message = (
                        "TECHNICAL_DATA_STATE_UNBOUND:"
                        + ",".join(sorted(non_search_timeframes))
                    )
                    logger.error(f"[{symbol}] {result.error_message}")
                    return result
                data_window_meta["symbol_data_state"] = typed_state
                result.data_window_meta = dict(data_window_meta)
                result.error_message = (
                    "NON_SEARCH_ALL_TIMEFRAMES:"
                    + ",".join(sorted(non_search_timeframes))
                    + ":kinds="
                    + ",".join(kinds)
                )
                logger.warning(f"[{symbol}] {result.error_message}")
                return result

            # A complete search that honestly finds no carrier is a semantic
            # all-NO_TRADE result, not an optimizer/infrastructure failure.
            if total_winners == 0 and default_config is None:
                no_winner_reason = (
                    f"No validated regime winners after complete cell evaluation "
                    f"({unvalidated_count} rejected/abstained cells)"
                )
                default_config = self.regime_optimizer._make_no_trade_marker(
                    no_winner_reason
                )
            
            best_overall_tf, best_overall_regime = self._locate_default_origin(default_config, regime_configs)
            if best_overall_tf and best_overall_regime and default_config is not None:
                matched_cfg = regime_configs[best_overall_tf][best_overall_regime]
                default_config.holdout_metrics = matched_cfg.holdout_metrics
                default_config.historical_audit_metrics = dict(matched_cfg.historical_audit_metrics)
                default_config.live_observability = dict(matched_cfg.live_observability)
                default_config.walk_forward_audit = dict(matched_cfg.walk_forward_audit)
                default_config.governance_policy = dict(matched_cfg.governance_policy)
                default_config.promotion_status = str(matched_cfg.promotion_status or "BLOCKED")
                default_config.promotion_reasons = list(matched_cfg.promotion_reasons or [])

            # Calendar production stops at a globally frozen Phase-A book.
            # Only the synchronized Phase-B command may observe final-lockbox
            # outcomes.  Other modes preserve the historical single-run
            # lineage for research compatibility.
            champion_temporal_lineage = (
                build_prelockbox_champion_lineage(data_window_meta or {})
                if split_mode == CALENDAR_SPLIT_MODE
                else build_lockbox_champion_lineage(data_window_meta or {})
            )
            for regimes in regime_configs.values():
                for cfg in regimes.values():
                    cfg.validation_evidence = {
                        **dict(cfg.validation_evidence or {}),
                        "temporal_lineage": dict(champion_temporal_lineage),
                    }
            if default_config is not None:
                default_config.validation_evidence = {
                    **dict(default_config.validation_evidence or {}),
                    "temporal_lineage": dict(champion_temporal_lineage),
                }

            if split_mode == CALENDAR_SPLIT_MODE:
                freeze_reason = (
                    "Phase-A winner frozen before final lockbox; awaiting the "
                    "complete joint roster verdict"
                )
                for regimes in regime_configs.values():
                    for cfg in regimes.values():
                        if cfg.is_no_trade_marker():
                            cfg.promotion_status = "NO_TRADE"
                            if not cfg.promotion_reasons:
                                cfg.promotion_reasons = ["training/tuning produced no eligible specialist"]
                        else:
                            cfg.holdout_metrics = {}
                            cfg.promotion_status = FROZEN_PRELOCKBOX_STATUS
                            cfg.promotion_reasons = [freeze_reason]
                if default_config is not None:
                    if default_config.is_no_trade_marker():
                        default_config.promotion_status = "NO_TRADE"
                    else:
                        default_config.holdout_metrics = {}
                        default_config.promotion_status = FROZEN_PRELOCKBOX_STATUS
                        default_config.promotion_reasons = [freeze_reason]
            else:
                self._apply_promotion_statuses(regime_configs, default_config)

            scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
            default_selection_evidence: Dict[str, Any] = {}
            if (
                scoring_model == "specialist_spokes_v2"
                and bool(getattr(self.config, "specialist_spokes_v2_default_fallback_enabled", True))
            ):
                selected_default, selected_tf, selected_regime, default_selection_evidence = self._select_v2_symbol_default(
                    regime_configs,
                    default_config,
                )
                if selected_default is None:
                    result.error_message = "No acceptable v2 symbol default after catastrophe fallback"
                    result.regime_winners = total_winners
                    logger.warning(f"[{symbol}] No acceptable specialist_spokes_v2 default after fallback")
                    return result
                if selected_default is not default_config:
                    logger.info(
                        f"[{symbol}] specialist_spokes_v2 default fallback: "
                        f"{getattr(default_config, 'strategy_name', 'None')} -> "
                        f"{selected_default.strategy_name} @ {selected_tf}/{selected_regime}"
                    )
                default_config = selected_default
                if selected_tf and selected_regime:
                    best_overall_tf, best_overall_regime = selected_tf, selected_regime
                else:
                    best_overall_tf, best_overall_regime = self._locate_default_origin(default_config, regime_configs)
                best_overall_score = float(default_config.quality_score or 0.0)

            for regimes in regime_configs.values():
                for cfg in regimes.values():
                    cfg.artifact_meta = {
                        **artifact_meta,
                        **dict(cfg.artifact_meta or {}),
                        "optimization_lifecycle_phase": (
                            "PRELOCKBOX_SELECTION_FROZEN"
                            if split_mode == CALENDAR_SPLIT_MODE
                            else "LEGACY_SINGLE_RUN"
                        ),
                    }
            if default_config is not None:
                default_config.artifact_meta = {
                    **artifact_meta,
                    **dict(default_config.artifact_meta or {}),
                    "optimization_lifecycle_phase": (
                        "PRELOCKBOX_SELECTION_FROZEN"
                        if split_mode == CALENDAR_SPLIT_MODE
                        else "LEGACY_SINGLE_RUN"
                    ),
                }

            default_validation_evidence = (
                dict(getattr(default_config, "validation_evidence", {}) or {})
                if default_config is not None else {}
            )
            if default_selection_evidence:
                default_validation_evidence["default_selection_evidence"] = default_selection_evidence
            optimizer_telemetry = {
                "training_gates": self.regime_optimizer.consume_training_gate_telemetry(),
                "resample_cache": self.data_loader.get_resample_cache_stats(reset=False),
                "feature_cache": FeatureComputer.get_cache_stats(reset=False),
            }
            default_validation_evidence["optimizer_telemetry"] = optimizer_telemetry
            default_validation_evidence["fairness_report"] = build_fairness_report(regime_configs)
            selected_robustness = float(default_validation_evidence.get("robustness_ratio", 0.0) or 0.0)

            retrain_days = int(getattr(self.config, 'fixed_retrain_days', getattr(self.config, 'optimization_valid_days', 14)))
            now = datetime.now()
            valid_until = _scheduled_valid_until(self.config, now)
            edge_challenger_book = build_empty_edge_challenger_book(
                data_window_meta or {}
            )
            edge_challenger_book.update({
                "generation_status": (
                    "COMPLETE"
                    if edge_challenger_configs and not edge_challenger_failures
                    else "PARTIAL"
                    if edge_challenger_configs
                    else "NO_ELIGIBLE_CHALLENGER"
                ),
                "funded_role_eligible": False,
                "paper_only": True,
                "regime_configs": edge_challenger_configs,
                "generated_cells": int(sum(len(v) for v in edge_challenger_configs.values())),
                "generation_failures": list(edge_challenger_failures),
            })
            config = SymbolConfig(
                symbol=symbol,
                regime_configs=regime_configs,
                default_config=default_config,
                # Legacy fields (from default)
                strategy_name=default_config.strategy_name if default_config else "",
                timeframe=best_overall_tf or "",
                parameters=default_config.parameters if default_config else {},
                retrain_days=retrain_days,
                train_metrics=default_config.train_metrics if default_config else {},
                val_metrics=default_config.val_metrics if default_config else {},
                holdout_metrics=default_config.holdout_metrics if default_config else {},
                historical_audit_metrics=default_config.historical_audit_metrics if default_config else _default_historical_audit(),
                data_window_meta=data_window_meta,
                edge_challenger_book=edge_challenger_book,
                composite_score=default_config.quality_score * 100 if default_config else 0.0,
                robustness_ratio=selected_robustness,
                is_validated=total_winners > 0,
                validation_reason=f"{total_winners} validated winners ({unvalidated_count} rejected) across {len(regime_configs)} timeframes",
                validation_evidence=default_validation_evidence,
                regime_detection_version=artifact_meta.get('regime_params'),
                artifact_meta=artifact_meta,
                live_observability=dict(default_config.live_observability) if default_config else _default_live_observability(),
                walk_forward_audit=dict(default_config.walk_forward_audit) if default_config else _default_walk_forward_audit(),
                optimized_at=now,
                valid_until=valid_until,
            )
            
            result.success = True
            result.config = config
            result.duration_seconds = time.time() - start_time
            
            # Log summary
            logger.info(f"\n[{symbol}] OPTIMIZATION COMPLETE")
            logger.info(f"  Timeframes:  {len(regime_configs)}")
            logger.info(f"  Validated:   {total_winners}")
            logger.info(f"  Rejected:    {unvalidated_count}")
            if default_config:
                logger.info(f"  Best:        {default_config.strategy_name} @ {best_overall_tf}/{best_overall_regime}")
            logger.info(f"  Retrain:     {self.config.describe_retrain_schedule()}")
            logger.info(f"  Next due:    {valid_until.strftime('%Y-%m-%d %H:%M')}")
            logger.info(f"  Duration:    {result.duration_seconds:.1f}s")
            
            # Log per-timeframe summary
            for tf in sorted(regime_configs.keys()):
                regimes = regime_configs[tf]
                if regimes:
                    regime_summary = ", ".join(f"{r}:{cfg.strategy_name[:12]}" for r, cfg in regimes.items())
                    logger.info(f"  [{tf}] {regime_summary}")

            # findings.html §C6: regime coverage report — one line per (symbol, tf)
            
        except Exception as e:
            logger.exception(f"[{symbol}] Pipeline error: {e}")
            result.error_message = str(e)
            result.duration_seconds = time.time() - start_time

        return result

    @staticmethod
    def _log_regime_coverage(symbol: str,
                             train_features_by_tf: Dict[str, "pd.DataFrame"],
                             val_features_by_tf: Dict[str, "pd.DataFrame"]) -> None:
        """findings.html §C6: per-(symbol, tf) regime distribution one-liner.

        Combines train+val features (the population the optimizer actually saw)
        and emits TREND % | RANGE % | BREAKOUT % | CHOP %. Silent if the regime
        column is absent.
        """
        regime_order = ['TREND', 'RANGE', 'BREAKOUT', 'CHOP']
        for tf in sorted(set(train_features_by_tf.keys()) | set(val_features_by_tf.keys())):
            try:
                frames = []
                tr = train_features_by_tf.get(tf)
                vl = val_features_by_tf.get(tf)
                if tr is not None and 'REGIME' in tr.columns:
                    frames.append(tr['REGIME'])
                if vl is not None and 'REGIME' in vl.columns:
                    frames.append(vl['REGIME'])
                if not frames:
                    continue
                combined = pd.concat(frames).dropna()
                total = int(combined.shape[0])
                if total == 0:
                    continue
                counts = combined.value_counts()
                parts = []
                for r in regime_order:
                    pct = float(counts.get(r, 0)) / total * 100.0
                    parts.append(f"{r} {pct:.1f}%")
                logger.info(f"  [{tf}] regime coverage: {' | '.join(parts)}")
            except Exception as exc:
                logger.debug(f"[{symbol}/{tf}] regime coverage report skipped: {exc}")

    def run_for_all(self, symbols: List[str]) -> Dict[str, PipelineResult]:
        """Reject the obsolete parent-process batch optimizer.

        Production and research batches must enter through ``PortfolioManager``
        so every symbol runs in a fresh governed child and every ledger mutation
        is receipt-verified, full-roster staged, replayed and CAS-promoted.
        """
        del symbols
        raise RuntimeError(
            "UNSAFE_UNGOVERNED_BATCH_API_DISABLED_USE_PORTFOLIO_MANAGER"
        )
    
    def save_configs(self, results: Dict[str, PipelineResult], filepath: str):
        """Reject direct config writes that bypass generation/CAS validation."""
        del results, filepath
        raise RuntimeError(
            "UNSAFE_DIRECT_CONFIG_SAVE_DISABLED_USE_PORTFOLIO_MANAGER"
        )
    
    def load_configs(self, filepath: str) -> Dict[str, SymbolConfig]:
        """Load configurations from JSON file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        configs = {}
        for symbol, config_dict in data.items():
            configs[symbol] = SymbolConfig.from_dict(config_dict)
        
        logger.info(f"Loaded {len(configs)} configurations from {filepath}")
        return configs


# =============================================================================
# PORTFOLIO MANAGER
# =============================================================================

class PortfolioManager:
    """
    Main Portfolio Manager class with stateful optimization ledger.
    
    Manages the complete lifecycle:
    1. Initial optimization of all symbols (with skip/resume support)
    2. Continuous monitoring and trading
    3. Automatic retraining when periods expire
    4. Incremental configuration persistence (atomic saves)
    
    Key Features:
    - Skips re-optimizing symbols with valid (non-expired) configs
    - Persists progress after each symbol (never loses work)
    - Atomic writes prevent config corruption
    - Explicit overwrite mode for full re-optimization
    """
    
    def __init__(
        self,
        config: PipelineConfig,
        symbols: List[str],
        config_file: str = "pm_configs.json",
    ):
        """
        Initialize Portfolio Manager.
        
        Args:
            config: Pipeline configuration
            symbols: List of symbols to manage
            config_file: Path to configuration file
        """
        self.config = config
        self.symbols = symbols
        if (
            bool(getattr(self.config, "false_discovery_control_enabled", False))
            and int(getattr(self.config, "false_discovery_symbol_count", 0) or 0) <= 0
        ):
            self.config.false_discovery_symbol_count = len(symbols or [])
        self.config_file = Path(config_file)
        
        # Pipeline
        self.pipeline = OptimizationPipeline(config)
        self.enhancement_seams: EnhancementSeams = self.pipeline.enhancement_seams
        
        # Config ledger for stateful persistence
        self.ledger = ConfigLedger(str(self.config_file))
        
        # Active configurations (loaded from ledger)
        self.symbol_configs: Dict[str, SymbolConfig] = {}
        self.last_optimization_generation: Optional[OptimizationGenerationOutcome] = None
        
        # Load existing configs via ledger
        self._last_promotion_recovery_status = (
            self._reconcile_interrupted_promotion()
        )
        self._load_configs()
    
    def _load_configs(self):
        """Load configurations from ledger."""
        try:
            self.ledger.load()
            self.symbol_configs = self.ledger.get_all_configs()
        except RuntimeError as e:
            # Corrupted JSON - propagate with clear message
            logger.error(f"Config load failed: {e}")
            raise
        except Exception as e:
            logger.warning(f"Could not load configs: {e}")
            self.symbol_configs = {}

    def verify_live_promotion_authorization(self):
        """Re-read and verify the detached funded-live authorization.

        This intentionally performs fresh file/hash/native-activation checks on
        every call.  Startup uses it once for the whole ledger and the order
        path calls it again immediately before a funded entry.
        """
        from pm_promotion_authorization import (
            PromotionAuthorizationResult,
            verify_promotion_authorization,
        )

        mode = str(
            getattr(self.config, "production_promotion_gate_mode", "ledger_status") or ""
        ).strip().lower()
        if mode in {"immutable", "authorization", "authorization_exact"}:
            mode = "immutable_exact"
        if mode != "immutable_exact":
            result = PromotionAuthorizationResult(
                valid=False,
                errors=[f"PROMOTION_GATE_MODE_NOT_IMMUTABLE_EXACT:{mode or 'missing'}"],
            )
            self._last_promotion_authorization = result
            return result

        self.symbol_configs = self.ledger.get_all_configs()
        source_config = str(getattr(self.config, "_deployment_config_path", "") or "").strip()
        result = verify_promotion_authorization(
            self.config,
            self.symbol_configs,
            ledger_path=self.ledger.filepath,
            config_path=Path(source_config) if source_config else None,
            verify_active_native=True,
        )
        self._last_promotion_authorization = result
        return result

    def is_exact_cell_authorized_for_live(
        self,
        symbol: str,
        timeframe: str,
        regime: str,
        *,
        refresh: bool = True,
    ) -> Tuple[bool, str]:
        """Return funded entry authority for exactly one independent cell."""
        if not bool(getattr(self.config, "production_require_promotion_gate", True)):
            if bool(getattr(self.config, "_funded_live_execution", False)):
                return False, "funded execution cannot disable the promotion gate"
            return True, "promotion gate disabled for research"
        mode = str(
            getattr(self.config, "production_promotion_gate_mode", "ledger_status") or ""
        ).strip().lower()
        if mode in {"immutable", "authorization", "authorization_exact"}:
            mode = "immutable_exact"
        if mode != "immutable_exact":
            if bool(getattr(self.config, "_funded_live_execution", False)):
                return False, f"funded execution rejects weak promotion gate mode {mode!r}"
            return True, f"legacy promotion mode {mode!r} retained for research"
        verification = None if refresh else getattr(self, "_last_promotion_authorization", None)
        if verification is None:
            try:
                verification = self.verify_live_promotion_authorization()
            except Exception as exc:
                return False, f"promotion authorization verification raised {type(exc).__name__}: {exc}"
        if not verification.valid:
            return False, verification.concise_reason()
        if not verification.authorizes(symbol, timeframe, regime):
            cell_id = "|".join(
                str(value or "").strip().upper()
                for value in (symbol, timeframe, regime)
            )
            return False, f"exact cell {cell_id} is not approved"
        return True, f"authorized by {verification.authorization_id}"

    def _build_ledger_status(
        self,
        configs: Optional[Dict[str, SymbolConfig]] = None,
        *,
        required_symbols: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """Build operator-facing completion metadata for the active winner ledger."""
        source_configs = self.symbol_configs if configs is None else configs
        configured_symbols = [str(symbol) for symbol in (self.symbols or [])]
        required_winner_symbols = list(
            dict.fromkeys(
                str(symbol)
                for symbol in (
                    configured_symbols if required_symbols is None else required_symbols
                )
            )
        )
        optimized_symbols = sorted(str(symbol) for symbol in source_configs.keys())
        artifact_contract_counts: Dict[str, int] = {}
        artifact_contract_symbols: Dict[str, set] = {}
        current_artifact_meta = build_artifact_meta(self.config)
        current_artifact_key = "|".join([
            str(current_artifact_meta["artifact_version"]),
            str(current_artifact_meta["split_contract_version"]),
        ])
        for symbol, cfg in source_configs.items():
            artifact = getattr(cfg, "artifact_meta", {}) or {}
            key = "|".join([
                str(artifact.get("artifact_version", "missing")),
                str(artifact.get("split_contract_version", "missing")),
            ])
            artifact_contract_counts[key] = artifact_contract_counts.get(key, 0) + 1
            artifact_contract_symbols.setdefault(key, set()).add(str(symbol).upper())
        configured_set = {symbol.upper() for symbol in configured_symbols}
        required_set = {symbol.upper() for symbol in required_winner_symbols}
        if not required_set.issubset(configured_set):
            raise RuntimeError("WINNER_LEDGER_REQUIRED_SYMBOL_OUTSIDE_CONFIGURED_UNIVERSE")
        optimized_set = {symbol.upper() for symbol in optimized_symbols}
        current_contract_set = artifact_contract_symbols.get(current_artifact_key, set())
        missing_symbol_entries = sorted(required_set - optimized_set)
        missing_current_contract = sorted(required_set - current_contract_set)
        # seq1049 e2e: an all-data-state generation has an EMPTY required
        # set by typed removal; completeness is then vacuous so the
        # authenticated empty ledger can promote atomically.
        complete_symbol_universe = required_set.issubset(optimized_set)
        complete_current_contract = required_set.issubset(current_contract_set)
        data_state_only_symbols = sorted(configured_set - required_set)
        return {
            "configured_symbol_count": len(configured_symbols),
            "required_winner_symbol_count": len(required_set),
            "required_winner_symbols": sorted(required_set),
            "data_state_only_symbol_count": len(data_state_only_symbols),
            "data_state_only_symbols": data_state_only_symbols,
            "optimized_symbol_count": len(optimized_symbols),
            "current_artifact_contract": current_artifact_key,
            "current_contract_symbol_count": len(current_contract_set),
            "complete_universe": complete_current_contract,
            "complete_symbol_universe": complete_symbol_universe,
            "complete_current_contract_universe": complete_current_contract,
            "missing_symbols": missing_current_contract,
            "missing_symbol_entries": missing_symbol_entries,
            "missing_current_contract_symbols": missing_current_contract,
            "artifact_contract_counts": artifact_contract_counts,
            "updated_at": datetime.now().isoformat(),
        }
    
    def _save_symbol_config(self, symbol: str, config: SymbolConfig):
        """
        Save a single symbol's config atomically.
        
        This is the incremental persistence method.
        """
        self.symbol_configs[symbol] = config
        ledger_status = self._build_ledger_status()
        config.artifact_meta = {
            **dict(config.artifact_meta or {}),
            "ledger_status": ledger_status,
        }
        if not self.ledger.update_symbol(symbol, config):
            logger.warning(f"Failed to persist config for {symbol}")
            return
        try:
            added = materialize_regime_params_from_symbol_config(
                config,
                filepath=str(getattr(self.config, "regime_params_file", "regime_params.json") or "regime_params.json"),
            )
            if added:
                logger.info(
                    "Materialized %d regime detector row(s) for %s into %s",
                    added,
                    symbol,
                    getattr(self.config, "regime_params_file", "regime_params.json"),
                )
        except Exception as exc:
            logger.warning("Failed to materialize regime detector rows for %s: %s", symbol, exc)

    def _save_no_trade_config(
        self,
        symbol: str,
        reason: str,
        *,
        failure_kind: str,
        data_window_meta: Optional[Dict[str, Any]] = None,
        native_exclusion_binding: Optional[Dict[str, Any]] = None,
    ) -> SymbolConfig:
        """Persist an explicit NO_TRADE config for symbols that cannot be safely traded."""
        now = datetime.now()
        regime_optimizer = self.pipeline.regime_optimizer
        failure_kind = str(failure_kind or "optimization_failure")
        regime_configs: Dict[str, Dict[str, RegimeConfig]] = {}
        exclusion_binding = dict(native_exclusion_binding or {})
        for timeframe in list(getattr(self.config, "timeframes", []) or []):
            regime_configs[timeframe] = {}
            for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP"):
                marker = regime_optimizer._make_no_trade_marker(reason, now=now)
                if exclusion_binding:
                    marker.artifact_meta = {
                        **dict(marker.artifact_meta or {}),
                        "native_exclusion_binding": exclusion_binding,
                    }
                    marker.validation_evidence = {
                        **dict(marker.validation_evidence or {}),
                        "native_exclusion_binding": exclusion_binding,
                    }
                regime_configs[timeframe][regime] = marker

        default_config = regime_optimizer._make_no_trade_marker(reason, now=now)
        if exclusion_binding:
            default_config.artifact_meta = {
                **dict(default_config.artifact_meta or {}),
                "native_exclusion_binding": exclusion_binding,
            }
            default_config.validation_evidence = {
                **dict(default_config.validation_evidence or {}),
                "native_exclusion_binding": exclusion_binding,
            }
        validation_evidence = {
            "validation_reason": reason,
            "tradeable": False,
            "no_trade": True,
            failure_kind: True,
        }
        if exclusion_binding:
            validation_evidence["native_exclusion_binding"] = exclusion_binding
        artifact_meta = {
            **build_artifact_meta(self.config),
            "no_trade": True,
            "reason": reason,
            failure_kind: True,
        }
        if exclusion_binding:
            artifact_meta["native_exclusion_binding"] = exclusion_binding
        marker_data_window_meta = dict(data_window_meta or {})
        marker_data_window_meta.update({failure_kind: True, "reason": reason})
        if exclusion_binding:
            marker_data_window_meta["native_exclusion_binding"] = exclusion_binding
        config = SymbolConfig(
            symbol=symbol,
            regime_configs=regime_configs,
            default_config=default_config,
            strategy_name="NO_TRADE",
            timeframe="",
            parameters={},
            retrain_days=int(getattr(self.config, "fixed_retrain_days", 14) or 14),
            train_metrics={"total_trades": 0},
            val_metrics={"total_trades": 0, "validation_status": "no_trade", "reason": reason},
            holdout_metrics={},
            historical_audit_metrics=_default_historical_audit(),
            data_window_meta=marker_data_window_meta,
            composite_score=0.0,
            robustness_ratio=0.0,
            is_validated=False,
            validation_reason=reason,
            validation_evidence=validation_evidence,
            artifact_meta=artifact_meta,
            optimized_at=now,
            valid_until=_scheduled_valid_until(self.config, now),
        )
        self._save_symbol_config(symbol, config)
        logger.info(f"SAVED {symbol} NO_TRADE {failure_kind} marker to {self.config_file} (atomic)")
        return config

    def save_data_no_trade_config(self, symbol: str, reason: str) -> SymbolConfig:
        """Reject the retired acquisition-to-winner-ledger mutation path."""
        raise RuntimeError(
            "ACQUISITION_DATA_STATE_WINNER_LEDGER_MUTATION_FORBIDDEN:"
            f"{str(symbol).upper()}:{reason}"
        )

    def save_native_exclusion_no_trade_config(
        self,
        symbol: str,
        native_exclusion_binding: Dict[str, Any],
    ) -> SymbolConfig:
        """Persist a full 6x4 NO_TRADE grid pinned to one activation decision."""
        reasons = list((native_exclusion_binding or {}).get("reason_codes") or [])
        reason = (
            "Native history is structurally unavailable for the full production calendar: "
            + (", ".join(str(value) for value in reasons) or "availability evidence missing")
            + ". The symbol is excluded before model search; no performance result was used."
        )
        return self._save_no_trade_config(
            symbol,
            reason,
            failure_kind="native_availability_exclusion",
            native_exclusion_binding=native_exclusion_binding,
        )

    @staticmethod
    def _native_calendar_marker_meta(calendar_contract: Dict[str, Any]) -> Dict[str, Any]:
        """Materialize common windows for a symbol with zero loadable cells."""
        contract = dict(calendar_contract or {})
        boundaries = contract.get("common_calendar_boundaries")
        if not isinstance(boundaries, dict):
            return {"native_calendar_contract": contract, "windows": {}}

        def _leaf(name: str) -> Dict[str, Any]:
            raw = boundaries.get(name)
            if not isinstance(raw, dict):
                return {}
            return {
                "start_index": 0,
                "end_index": 0,
                "rows": 0,
                "start_ts": raw.get("start_ts"),
                "end_ts": raw.get("end_ts"),
                "end_ts_exclusive": raw.get("end_ts"),
                "end_ts_inclusive": None,
                "first_observed_ts": None,
                "last_observed_ts": None,
            }

        train = _leaf("optimizer_train")
        tuning = _leaf("tuning_validation")
        final = _leaf("final_lockbox")
        return {
            "production_split_mode": CALENDAR_SPLIT_MODE,
            "split_contract_version": EVIDENCE_CONTRACT_VERSION,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "as_of_utc": contract.get("common_as_of_utc"),
            "native_calendar_contract": contract,
            "windows": {
                "optimizer_train": dict(train),
                "optimization_train": dict(train),
                "optimizer_train_with_warmup": dict(train),
                "optimization_train_with_warmup": dict(train),
                "tuning_validation": dict(tuning),
                "walk_forward_validation": dict(tuning),
                "stage2_selection": dict(tuning),
                "stage2_selection_with_warmup": dict(tuning),
                "stage2_training_selection": dict(tuning),
                "stage2_training_selection_with_warmup": dict(tuning),
                "final_lockbox": dict(final),
                "final_lockbox_with_warmup": dict(final),
                "clean_holdout": dict(final),
                "clean_holdout_with_warmup": dict(final),
            },
        }

    def _build_fully_excluded_native_config(
        self,
        symbol: str,
        universe: Any,
    ) -> SymbolConfig:
        """Build 24 exact cell-bound markers without mutating the ledger."""
        now = datetime.now()
        regime_configs: Dict[str, Dict[str, RegimeConfig]] = {}
        bindings: Dict[str, Dict[str, Any]] = {}
        for raw_timeframe in list(getattr(self.config, "timeframes", []) or []):
            timeframe = str(raw_timeframe or "").strip().upper()
            binding = dict(universe.cell_exclusion_binding(symbol, timeframe) or {})
            if not binding:
                raise RuntimeError(
                    f"NATIVE_CELL_EXCLUSION_BINDING_MISSING:{str(symbol).upper()}|{timeframe}"
                )
            bindings[timeframe] = binding
            regime_configs[timeframe] = self.pipeline._native_cell_exclusion_markers(binding)
        reason = "Every configured native timeframe is manifest-excluded before model search"
        default_config = self.pipeline.regime_optimizer._make_no_trade_marker(reason, now=now)
        data_window_meta = self._native_calendar_marker_meta(
            dict(getattr(universe, "calendar_contract", {}) or {})
        )
        data_window_meta["native_cell_eligibility"] = {
            "active_timeframes": [],
            "excluded_timeframes": sorted(bindings),
            "excluded_bindings": bindings,
            "all_cells_excluded": True,
            "activation_id": getattr(universe, "activation_id", None),
            "activation_manifest_sha256": getattr(
                universe, "activation_manifest_sha256", None
            ),
        }
        config = SymbolConfig(
            symbol=symbol,
            regime_configs=regime_configs,
            default_config=default_config,
            strategy_name="NO_TRADE",
            timeframe="",
            parameters={},
            retrain_days=int(getattr(self.config, "fixed_retrain_days", 14) or 14),
            train_metrics={"total_trades": 0},
            val_metrics={"total_trades": 0, "validation_status": "no_trade", "reason": reason},
            holdout_metrics={},
            historical_audit_metrics=_default_historical_audit(),
            data_window_meta=data_window_meta,
            composite_score=0.0,
            robustness_ratio=0.0,
            is_validated=False,
            validation_reason=reason,
            validation_evidence={
                "validation_reason": reason,
                "tradeable": False,
                "no_trade": True,
                "native_cell_exclusions": bindings,
            },
            artifact_meta={
                **build_artifact_meta(self.config),
                "no_trade": True,
                "native_cell_exclusions": bindings,
            },
            optimized_at=now,
            valid_until=_scheduled_valid_until(self.config, now),
        )
        return config

    def save_fully_excluded_native_config(self, symbol: str, universe: Any) -> SymbolConfig:
        """Persist 24 exact cell-bound markers without launching model search."""
        config = self._build_fully_excluded_native_config(symbol, universe)
        self._save_symbol_config(symbol, config)
        logger.info(
            "SAVED %s: 24 manifest-bound native cell exclusions (no search)",
            symbol,
        )
        return config

    def save_optimization_no_trade_config(
        self,
        symbol: str,
        reason: str,
        data_window_meta: Optional[Dict[str, Any]] = None,
    ) -> SymbolConfig:
        """Persist an explicit NO_TRADE config when optimization cannot produce a live-safe winner."""
        return self._save_no_trade_config(
            symbol,
            reason,
            failure_kind="optimization_failure",
            data_window_meta=data_window_meta,
        )

    def _native_universe_for_optimization(self):
        """Resolve the production availability roster before any model search."""
        data_dir = Path(getattr(self.config, "data_dir", "data"))
        source_config = str(
            getattr(self.config, "_deployment_config_path", "") or ""
        ).strip()
        if not data_dir.is_absolute() and source_config:
            data_dir = Path(source_config).resolve().parent / data_dir
        active_pointer = data_dir / "native" / "ACTIVE.json"
        production_native = bool(source_config) and bool(
            getattr(self.config, "evidence_require_native_timeframes", True)
        )
        configured_timeframes = tuple(
            str(value or "").strip().upper()
            for value in (getattr(self.config, "timeframes", []) or [])
            if str(value or "").strip()
        )
        if production_native and configured_timeframes != NATIVE_PRODUCTION_TIMEFRAMES:
            raise RuntimeError(
                "NATIVE_PRODUCTION_TIMEFRAME_ROSTER_INVALID: calendar production requires "
                f"exactly {list(NATIVE_PRODUCTION_TIMEFRAMES)!r}, got "
                f"{list(configured_timeframes)!r}"
            )
        if not active_pointer.exists():
            if production_native:
                raise RuntimeError(
                    "NATIVE_UNIVERSE_ACTIVATION_REQUIRED: acquire and activate the immutable "
                    "six-timeframe roster before production optimization"
                )
            return None
        universe = resolve_active_native_universe(
            data_dir,
            self.symbols,
            required_timeframes=configured_timeframes,
            production_contract_required=True,
        )
        if universe.errors:
            raise RuntimeError(
                "NATIVE_UNIVERSE_ACTIVATION_INVALID: "
                + "; ".join(universe.errors[:20])
            )
        if production_native and not universe.explicit_contract:
            raise RuntimeError(
                "NATIVE_UNIVERSE_ELIGIBILITY_CONTRACT_MISSING: production optimization "
                "requires an immutable availability-only eligible/excluded roster"
            )
        return universe

    def _resolve_deployment_path(self, raw_path: Any) -> Path:
        """Resolve a pipeline artifact relative to the deployed config file."""
        path = Path(str(raw_path or "").strip())
        if path.is_absolute():
            return path.resolve()
        source_config = str(
            getattr(self.config, "_deployment_config_path", "") or ""
        ).strip()
        base = Path(source_config).resolve().parent if source_config else Path.cwd()
        return (base / path).resolve()

    def _preflight_active_regime_provenance(
        self,
        universe: Any,
        symbols: Optional[List[str]] = None,
    ) -> None:
        """Fail before workers or ledger writes when active detector inputs are absent."""
        if universe is None or not getattr(universe, "explicit_contract", False):
            return
        split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        if split_mode not in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
            return
        expected_contract = (
            str(
                (getattr(universe, "calendar_contract", {}) or {}).get(
                    "window_contract_version"
                )
                or EVIDENCE_CONTRACT_VERSION
            )
            if split_mode == CALENDAR_SPLIT_MODE
            else DataSplitter.SPLIT_CONTRACT_VERSION
        )
        params_file = str(
            self._resolve_deployment_path(
                getattr(self.config, "regime_params_file", "regime_params.json")
                or "regime_params.json"
            )
        )
        failures: List[str] = []
        target_symbols = list(symbols if symbols is not None else self.symbols)
        target_active_cells = [
            (str(symbol or "").strip().upper(), str(timeframe).upper())
            for symbol in target_symbols
            for timeframe in universe.active_timeframes(
                str(symbol or "").strip().upper()
            )
        ]
        # Availability-only NO_TRADE materialization has no detector input and
        # must remain possible even when no regime generation exists yet.
        if not target_active_cells:
            return
        if split_mode == CALENDAR_SPLIT_MODE:
            expected_cells = sorted(
                f"{str(symbol).upper()}|{str(timeframe).upper()}"
                for symbol in (
                    getattr(universe, "requested_symbols", ()) or self.symbols
                )
                for timeframe in universe.active_timeframes(symbol)
            )
            sidecar_path = regime_params_provenance_path(params_file)
            try:
                sidecar_payload = json.loads(
                    sidecar_path.read_text(encoding="utf-8")
                )
            except Exception as exc:
                sidecar_payload = {}
                failures.append(f"GENERATION:sidecar unreadable: {exc}")
            declared_cells = sidecar_payload.get("required_active_cells")
            if not isinstance(declared_cells, list) or sorted(
                str(value) for value in declared_cells
            ) != expected_cells:
                failures.append(
                    "GENERATION:required active-cell roster does not match activation"
                )
            if int(sidecar_payload.get("required_active_cell_count") or -1) != len(
                expected_cells
            ):
                failures.append("GENERATION:required active-cell count mismatch")
            # seq1028: FIT + terminal NON_SEARCH together resolve the roster;
            # any TECHNICAL_EXCLUSION keeps the generation incomplete.
            fitted_count = int(
                sidecar_payload.get("fitted_active_cell_count") or 0
            )
            non_search_count = int(
                sidecar_payload.get("non_search_active_cell_count") or 0
            )
            technical_exclusions = [
                f"{sym}|{tf}"
                for sym, tfs in (
                    sidecar_payload.get("non_search_cells", {}) or {}
                ).items()
                for tf, rec in (tfs or {}).items()
                if isinstance(rec, dict)
                and (
                    str(rec.get("decision_kind", "")).upper()
                    == "TECHNICAL_EXCLUSION"
                    or rec.get("generation_terminal") is False
                )
            ]
            if technical_exclusions:
                failures.append(
                    "GENERATION:retryable technical exclusions keep generation "
                    f"incomplete: {sorted(technical_exclusions)}"
                )
            if fitted_count + non_search_count != len(expected_cells):
                failures.append(
                    "GENERATION:fitted+non_search cell count does not resolve "
                    f"the active roster: {fitted_count}+{non_search_count}"
                    f"!={len(expected_cells)}"
                )
        for symbol in target_symbols:
            canonical = str(symbol or "").strip().upper()
            for timeframe in universe.active_timeframes(canonical):
                check = validate_regime_params_provenance(
                    canonical,
                    str(timeframe).upper(),
                    params_file,
                    expected_split_contract=expected_contract,
                    expected_activation_id=getattr(universe, "activation_id", None),
                    expected_activation_manifest_sha256=(
                        getattr(universe, "activation_manifest_sha256", None)
                    ),
                    require_complete_generation=(
                        split_mode == CALENDAR_SPLIT_MODE
                    ),
                )
                if not check.get("valid", False):
                    failures.append(
                        f"{canonical}|{str(timeframe).upper()}:"
                        f"{check.get('reason', 'unknown')}"
                    )
        if failures:
            preview = "; ".join(failures[:20])
            if len(failures) > 20:
                preview += f"; +{len(failures) - 20} more"
            raise RuntimeError(
                "ACTIVE_REGIME_PROVENANCE_PREFLIGHT_FAILED: " + preview
            )

    def _ensure_active_regime_provenance(
        self,
        universe: Any,
        symbols: Optional[List[str]] = None,
    ) -> bool:
        """Validate or atomically rebuild the complete detector generation.

        ``--optimize`` is an end-to-end production command.  A missing or stale
        detector sidecar must therefore be repaired before any optimization
        worker starts, rather than surfacing later as one failure per symbol.
        The tuner itself stages both artifacts and only promotes a complete
        activation-bound generation, so an unsuccessful repair cannot alter
        the previous parameter book or the winner ledger.

        Returns ``True`` only when this call rebuilt the detector generation.
        """
        try:
            self._preflight_active_regime_provenance(universe, symbols)
            return False
        except RuntimeError as exc:
            if not str(exc).startswith(
                "ACTIVE_REGIME_PROVENANCE_PREFLIGHT_FAILED:"
            ):
                raise
            preflight_error = exc

        split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        if split_mode != CALENDAR_SPLIT_MODE:
            raise preflight_error
        if universe is None or not getattr(universe, "explicit_contract", False):
            raise preflight_error

        activated_symbols = list(
            dict.fromkeys(
                str(value or "").strip().upper()
                for value in (
                    getattr(universe, "requested_symbols", ()) or self.symbols
                )
                if str(value or "").strip()
            )
        )
        activated_timeframes = list(
            dict.fromkeys(
                str(value or "").strip().upper()
                for value in (
                    getattr(universe, "required_timeframes", ())
                    or getattr(self.config, "timeframes", ())
                )
                if str(value or "").strip()
            )
        )
        candidate_count = sum(
            len(tuple(universe.active_timeframes(symbol)))
            for symbol in activated_symbols
        )
        if candidate_count <= 0:
            # The preflight has already established that a generation was
            # unexpectedly required; never invent an empty detector book.
            raise RuntimeError(
                "ACTIVE_REGIME_PROVENANCE_REBUILD_FORBIDDEN_NO_CANDIDATES"
            ) from preflight_error

        data_dir = self._resolve_deployment_path(
            getattr(self.config, "data_dir", "data") or "data"
        )
        params_path = self._resolve_deployment_path(
            getattr(self.config, "regime_params_file", "regime_params.json")
            or "regime_params.json"
        )
        logger.warning(
            "Regime provenance is absent or stale; rebuilding all %d authenticated "
            "evidence-candidate detector cells before optimization",
            candidate_count,
        )

        # Lazy import keeps the standalone tuner independent of pipeline module
        # import order while making this prerequisite part of the command.
        from pm_regime_tuner import RegimeParamTuner

        tuner = None
        try:
            tuner = RegimeParamTuner(data_dir, config=self.config)
            tuner.tune_all(
                activated_symbols,
                activated_timeframes,
                output_path=str(params_path),
            )
        except Exception as exc:
            raise RuntimeError(
                "ACTIVE_REGIME_PROVENANCE_REBUILD_FAILED: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        finally:
            if tuner is not None:
                for loader_name in ("data_loader", "native_data_loader"):
                    loader = getattr(tuner, loader_name, None)
                    clear = getattr(loader, "clear_cache", None)
                    if callable(clear):
                        clear()

        # Re-read both promoted artifacts and bind them to the activation using
        # the same fail-closed validator that guards every optimizer worker.
        self._preflight_active_regime_provenance(universe, symbols)
        logger.info(
            "Regime prerequisite rebuilt and revalidated for activation %s",
            getattr(universe, "activation_id", "unknown"),
        )
        return True

    def _has_exact_native_exclusion_grid(
        self,
        config: Optional[SymbolConfig],
        binding: Dict[str, Any],
    ) -> bool:
        if config is None:
            return False
        return not validate_native_exclusion_no_trade_grid(
            config.to_dict(),
            binding,
            required_timeframes=tuple(
                str(value or "").strip().upper()
                for value in (getattr(self.config, "timeframes", []) or [])
                if str(value or "").strip()
            ),
        )

    def _has_exact_native_cell_grid(
        self,
        symbol: str,
        config: Optional[SymbolConfig],
        universe: Any,
    ) -> bool:
        """Check the strategy grid against authenticated per-cell data states.

        Evidence-candidate cells must have exactly four evaluated regime leaves.
        Source-unavailable cells must have no strategy leaves at all: acquisition
        cannot manufacture a trading verdict.  The old ``excluded_no_trade``
        branch remains readable solely for pre-v4 activation compatibility.
        """
        if config is None:
            return False
        stored_eligibility = dict(
            (config.data_window_meta or {}).get("native_cell_eligibility", {}) or {}
        )
        if str(stored_eligibility.get("activation_id") or "") != str(
            getattr(universe, "activation_id", None) or ""
        ):
            return False
        if str(stored_eligibility.get("activation_manifest_sha256") or "") != str(
            getattr(universe, "activation_manifest_sha256", None) or ""
        ):
            return False
        configured_timeframes = {
            str(value or "").strip().upper()
            for value in (getattr(self.config, "timeframes", []) or [])
            if str(value or "").strip()
        }
        expected_states = {
            timeframe: str(universe.cell_status(symbol, timeframe) or "").lower()
            for timeframe in configured_timeframes
        }
        stored_states = {
            str(timeframe).upper(): str(status).lower()
            for timeframe, status in dict(
                stored_eligibility.get("cell_states", {}) or {}
            ).items()
        }
        legacy_partition = any(
            status == "excluded_no_trade" for status in expected_states.values()
        )
        if stored_states and stored_states != expected_states:
            return False
        if not stored_states and not legacy_partition:
            return False
        if (
            not legacy_partition
            and stored_eligibility.get("acquisition_emits_trading_verdicts") is not False
        ):
            return False
        expected_candidates = {
            timeframe
            for timeframe, status in expected_states.items()
            if status in {"active", NATIVE_EVIDENCE_CANDIDATE}
        }
        expected_unavailable = {
            timeframe
            for timeframe, status in expected_states.items()
            if status == NATIVE_SOURCE_UNAVAILABLE
        }
        if {
            str(value).upper()
            for value in (
                stored_eligibility.get(
                    "evidence_candidate_timeframes",
                    stored_eligibility.get("active_timeframes", []),
                )
                or []
            )
        } != expected_candidates:
            return False
        if {
            str(value).upper()
            for value in (
                stored_eligibility.get("source_unavailable_timeframes", []) or []
            )
        } != expected_unavailable:
            return False

        grid = config.regime_configs or {}
        expected_grid_timeframes: set[str] = set()
        for raw_timeframe in list(getattr(self.config, "timeframes", []) or []):
            timeframe = str(raw_timeframe or "").strip().upper()
            status = expected_states.get(timeframe, "")
            if status == "excluded_no_trade":
                expected_grid_timeframes.add(timeframe)
                regimes = grid.get(timeframe)
                if not isinstance(regimes, dict) or set(regimes) != {
                    "TREND", "RANGE", "BREAKOUT", "CHOP"
                }:
                    return False
                expected = dict(universe.cell_exclusion_binding(symbol, timeframe) or {})
                if not expected:
                    return False
                serialized = {
                    regime: cell.to_dict() if cell is not None else None
                    for regime, cell in regimes.items()
                }
                if validate_native_cell_exclusion_no_trade(serialized, expected):
                    return False
            elif status in {"active", NATIVE_EVIDENCE_CANDIDATE}:
                stored_gate_check = dict(
                    (
                        (config.data_window_meta or {}).get(
                            "regime_provenance_gate", {}
                        )
                        or {}
                    ).get(timeframe, {})
                    or {}
                )
                if (
                    stored_gate_check.get("valid")
                    and stored_gate_check.get("status") == "NON_SEARCH"
                ):
                    # seq981: an activation-bound NON_SEARCH timeframe must
                    # have NO strategy leaves, and its typed decision must
                    # still be verifiable against the CURRENT regime sidecar
                    # by exact decision hash — never trusted from stale
                    # stored metadata alone.
                    if timeframe in grid:
                        return False
                    current = validate_regime_params_provenance(
                        symbol,
                        timeframe,
                        getattr(
                            self.config,
                            "regime_params_file",
                            "regime_params.json",
                        ),
                        expected_activation_id=str(
                            getattr(universe, "activation_id", None) or ""
                        ),
                        expected_activation_manifest_sha256=str(
                            getattr(
                                universe, "activation_manifest_sha256", None
                            )
                            or ""
                        ),
                        require_complete_generation=True,
                    )
                    if not (
                        current.get("valid")
                        and current.get("status") == "NON_SEARCH"
                        and (current.get("record") or {}).get("decision_sha256")
                        == stored_gate_check.get("decision_sha256")
                    ):
                        return False
                    continue
                expected_grid_timeframes.add(timeframe)
                regimes = grid.get(timeframe)
                # Empirical-regime rule: nonempty SUBSET of the four.
                if (
                    not isinstance(regimes, dict)
                    or not regimes
                    or not set(regimes)
                    <= {"TREND", "RANGE", "BREAKOUT", "CHOP"}
                ):
                    return False
                # seq1035: current explicit-contract activations REQUIRE the
                # support receipt; legacy leaf inference is not acceptance.
                if not stored_gate_check.get("support_receipt_sha256") and bool(
                    getattr(universe, "explicit_contract", False)
                ):
                    return False
                # seq1005: FIT support receipt is revalidated live; leaves
                # must equal receipt-observed labels exactly.
                if stored_gate_check.get("support_receipt_sha256"):
                    current = validate_regime_params_provenance(
                        symbol,
                        timeframe,
                        getattr(
                            self.config,
                            "regime_params_file",
                            "regime_params.json",
                        ),
                        expected_activation_id=str(
                            getattr(universe, "activation_id", None) or ""
                        ),
                        expected_activation_manifest_sha256=str(
                            getattr(
                                universe, "activation_manifest_sha256", None
                            )
                            or ""
                        ),
                        require_complete_generation=True,
                    )
                    support = current.get("train_regime_support") or {}
                    if not (
                        current.get("valid")
                        and current.get("status") == "FIT"
                        and support.get("receipt_sha256")
                        == stored_gate_check.get("support_receipt_sha256")
                        and set(regimes)
                        == set(support.get("observed_regimes") or [])
                    ):
                        return False
                for cell in regimes.values():
                    if cell is not None and (
                        (cell.artifact_meta or {}).get("native_cell_exclusion_binding")
                        or (cell.validation_evidence or {}).get(
                            "native_cell_exclusion_binding"
                        )
                    ):
                        return False
            elif status == NATIVE_SOURCE_UNAVAILABLE:
                if timeframe in grid:
                    return False
            else:
                return False
        return set(grid) == expected_grid_timeframes
    
    def needs_retraining(self, symbol: str) -> bool:
        """
        Check if symbol needs retraining.
        
        Uses ledger validity check.
        """
        artifact_meta = build_artifact_meta(self.config)
        should_optimize, _ = self.ledger.should_optimize(symbol, current_artifact_meta=artifact_meta)
        return should_optimize
    
    def get_symbols_needing_retrain(self) -> List[str]:
        """Get ledger-expired or active-native-contract-stale symbols."""
        due = [symbol for symbol in self.symbols if self.needs_retraining(symbol)]
        universe = self._native_universe_for_optimization()
        if universe is not None and universe.explicit_contract:
            for symbol in self.symbols:
                canonical = str(symbol).upper()
                if not universe.active_timeframes(canonical):
                    continue
                existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(
                    canonical
                )
                if (
                    not self._has_exact_native_cell_grid(canonical, existing, universe)
                    and symbol not in due
                ):
                    due.append(symbol)
        return due

    def retrain_symbol(self, symbol: str, force: bool = False) -> bool:
        """Run one manual target through the governed atomic generation path."""
        outcome = self._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="MANUAL_RETRAIN",
                target_symbols=(str(symbol),),
                overwrite_targets=bool(force),
                require_fresh_native_activation=False,
            )
        )
        if outcome.status == "NOOP":
            return True
        result = outcome.results.get(str(symbol)) or outcome.results.get(
            str(symbol).upper()
        )
        return bool(
            outcome.status == "PROMOTED"
            and outcome.promoted
            and (result is None or result.success)
        )
    
    def retrain_all_needed(self) -> Dict[str, bool]:
        """
        Retrain all symbols that need it.
        
        Returns:
            Dict mapping symbol to success status
        """
        results = {}
        symbols_to_retrain = self.get_symbols_needing_retrain()
        
        if not symbols_to_retrain:
            logger.info("No symbols need retraining")
            return results
        
        logger.info(f"Retraining {len(symbols_to_retrain)} symbols atomically...")
        production_native = bool(
            str(getattr(self.config, "_deployment_config_path", "") or "").strip()
        ) and str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower() == CALENDAR_SPLIT_MODE
        outcome = self._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="SCHEDULED_RETRAIN",
                target_symbols=tuple(symbols_to_retrain),
                overwrite_targets=False,
                require_fresh_native_activation=production_native,
            )
        )
        for symbol in symbols_to_retrain:
            result = outcome.results.get(symbol)
            results[symbol] = bool(
                outcome.status == "NOOP"
                or (
                    outcome.status == "PROMOTED"
                    and outcome.promoted
                    and (result is None or result.success)
                )
            )
        return results

    def _precompute_feature_cache_for_symbols(
        self,
        symbols: List[str],
        universe: Any = None,
    ) -> Dict[str, Any]:
        """Precompute disk feature frames before strategy optimization starts."""
        if not symbols:
            return {}
        workers = int(getattr(self.config, "feature_disk_cache_precompute_workers", 1) or 1)
        workers = max(1, min(workers, len(symbols)))
        started = time.time()
        summary: Dict[str, Any] = {
            "symbols_requested": len(symbols),
            "symbols_completed": 0,
            "symbols_failed": 0,
            "timeframes_computed_or_retrieved": 0,
            "workers": workers,
            "errors": {},
            "feature_cache": {
                "disk_hits": 0,
                "disk_misses": 0,
                "disk_writes": 0,
                "disk_skipped_storage": 0,
                "disk_write_failures": 0,
                "disk_read_failures": 0,
                "disk_invalidations": 0,
                "disk_bytes_read": 0,
                "disk_bytes_written": 0,
            },
        }

        def _merge_feature_cache_stats(stats: Dict[str, Any]) -> None:
            feature_stats = dict(stats.get("feature_cache", {}) or {})
            summary_stats = summary["feature_cache"]
            for key in list(summary_stats.keys()):
                try:
                    summary_stats[key] += int(feature_stats.get(key, 0) or 0)
                except Exception:
                    continue

        logger.info(
            f"Precomputing feature disk cache for {len(symbols)} symbols "
            f"(workers={workers}, dir={getattr(self.config, 'feature_disk_cache_dir', '')})"
        )

        cfg_dict = _pipeline_config_to_dict(self.config)
        runtime_bindings = _pipeline_runtime_bindings(self.config)
        work = [str(symbol) for symbol in symbols]
        governor = _new_optimizer_memory_governor(workers)

        def _tasks() -> Iterable[MemoryTask]:
            for symbol in work:
                yield MemoryTask(
                    key=symbol,
                    args=(cfg_dict, runtime_bindings, symbol),
                )

        outcomes = governor.iter_completed(
            _governed_precompute_feature_cache_worker,
            _tasks(),
        )
        outcome_iter = (
            tqdm(outcomes, total=len(work), desc="Feature cache", unit="sym")
            if _TQDM else outcomes
        )
        requested_symbols = set(work)
        terminal_symbols = set()
        precompute_aborted = False
        try:
            for outcome in outcome_iter:
                symbol = str(outcome.task_key)
                if symbol not in requested_symbols:
                    summary["errors"]["__GOVERNOR__"] = (
                        "GOVERNOR_RETURNED_UNKNOWN_PRECOMPUTE_TASK_KEY"
                    )
                    precompute_aborted = True
                    break
                terminal_symbols.add(symbol)
                if outcome.status != "COMPLETED":
                    summary["symbols_failed"] += 1
                    summary["errors"][symbol] = (
                        f"{outcome.status}:{outcome.error_type}:{outcome.error_message}"
                    )
                    precompute_aborted = True
                    break
                value = outcome.value
                if (
                    not isinstance(value, Mapping)
                    or str(value.get("schema_version")) != FEATURE_PRECOMPUTE_RESULT_SCHEMA
                    or str(value.get("symbol")) != str(symbol)
                ):
                    summary["symbols_failed"] += 1
                    summary["errors"][symbol] = "INVALID_FEATURE_PRECOMPUTE_RESULT"
                    precompute_aborted = True
                    break
                stats = dict(value)
                summary["symbols_completed"] += int(
                    stats.get("symbols_completed", 0) or 0
                )
                summary["timeframes_computed_or_retrieved"] += int(
                    stats.get("timeframes_computed_or_retrieved", 0) or 0
                )
                _merge_feature_cache_stats(stats)
        finally:
            if hasattr(outcomes, "close"):
                outcomes.close()

        if precompute_aborted:
            for symbol in work:
                if symbol in terminal_symbols:
                    continue
                summary["symbols_failed"] += 1
                summary["errors"][symbol] = (
                    "technical_not_run: feature precompute stopped after first "
                    "technical failure"
                )

        summary["duration_seconds"] = time.time() - started
        logger.info(
            "Feature disk cache precompute complete: "
            f"{summary['symbols_completed']}/{summary['symbols_requested']} symbols, "
            f"{summary['timeframes_computed_or_retrieved']} timeframe frames, "
            f"hits={summary['feature_cache']['disk_hits']}, "
            f"writes={summary['feature_cache']['disk_writes']}, "
            f"skipped={summary['feature_cache']['disk_skipped_storage']}, "
            f"write_failures={summary['feature_cache']['disk_write_failures']}, "
            f"failed={summary['symbols_failed']}, duration={summary['duration_seconds']:.1f}s"
        )
        if summary["errors"]:
            first_errors = list(summary["errors"].items())[:3]
            logger.warning(f"Feature disk cache precompute errors: {first_errors}")
        if (
            summary["feature_cache"]["disk_skipped_storage"]
            or summary["feature_cache"]["disk_write_failures"]
            or summary["feature_cache"]["disk_read_failures"]
        ):
            logger.warning(f"Feature disk cache precompute had cache-level issues: {summary['feature_cache']}")
        return summary

    def _run_production_replay_audit(
        self,
        ledger_override: Optional[Path] = None,
        *,
        mutate_manager: bool = True,
    ) -> Dict[str, Any]:
        """Run the full-roster replay gate against live or staged ledger bytes."""
        staged_override = getattr(self, "_staged_replay_ledger_override", None)
        if ledger_override is None and staged_override is not None:
            ledger_override = Path(staged_override)
            mutate_manager = False
        if str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower() == CALENDAR_SPLIT_MODE:
            # Phase A must remain frozen.  Per-cell full-history/replay gates
            # cannot prune it; only the one-shot synchronized Phase B verdict
            # may release or reject the complete roster.
            return {
                "enabled": False,
                "reason": "two_phase_calendar_lockbox_uses_joint_replay_only",
            }
        enabled = bool(getattr(self.config, "production_replay_audit_enabled", True))
        if not enabled:
            return {"enabled": False, "reason": "disabled"}

        mode = str(getattr(self.config, "production_replay_audit_mode", "apply") or "apply").strip().lower()
        if mode in {"report", "report-only", "report_only", "dry_run", "dry-run"}:
            mode = "report_only"
        elif mode in {"apply", "block", "enforce"}:
            mode = "apply"
        else:
            message = f"invalid production_replay_audit_mode={mode!r}; expected report_only or apply"
            if bool(getattr(self.config, "production_replay_audit_fail_on_error", True)):
                raise RuntimeError(message)
            logger.warning(message)
            return {"enabled": True, "ok": False, "error": message}

        try:
            from agent_audit.performance.scripts import run_promoted_cell_replay as replay_tools
            from tools import promotion_replay_audit as replay_audit
        except Exception as exc:
            message = f"production replay audit tools unavailable: {type(exc).__name__}: {exc}"
            if bool(getattr(self.config, "production_replay_audit_fail_on_error", True)):
                raise RuntimeError(message) from exc
            logger.warning(message)
            return {"enabled": True, "ok": False, "error": message}

        ledger_path = Path(ledger_override) if ledger_override is not None else Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        try:
            ledger_source_bytes = ledger_path.read_bytes()
            ledger_source_sha = hashlib.sha256(ledger_source_bytes).hexdigest()
            ledger = json.loads(ledger_source_bytes.decode("utf-8-sig"))
        except Exception as exc:
            message = f"production replay audit could not read ledger {ledger_path}: {type(exc).__name__}: {exc}"
            if bool(getattr(self.config, "production_replay_audit_fail_on_error", True)):
                raise RuntimeError(message) from exc
            logger.warning(message)
            return {"enabled": True, "ok": False, "error": message}

        cells = list(replay_tools.iter_promoted_cells(ledger))
        if not cells:
            logger.info("Production replay audit skipped: no logical promoted cells")
            return {"enabled": True, "ok": True, "cells": 0, "mode": mode}

        starting_balance = float(getattr(self.config, "production_replay_audit_starting_balance", 10000.0) or 10000.0)
        min_pf = float(getattr(self.config, "production_replay_audit_min_profit_factor", 1.05) or 0.0)
        max_dd_pct = float(getattr(self.config, "production_replay_audit_max_drawdown_pct", 30.0) or 0.0)
        min_trades = int(getattr(self.config, "production_replay_audit_min_trades", 30) or 0)
        block_unverified = bool(getattr(self.config, "production_replay_audit_block_unverified", False))
        thresholds = {
            "min_pf": min_pf,
            "max_dd_pct": max_dd_pct,
            "min_trades": min_trades,
            "starting_balance": starting_balance,
        }

        audit_config = replace(self.config, initial_capital=starting_balance)
        loader = DataLoader(audit_config.data_dir)
        results: List[Dict[str, Any]] = []
        logger.info(
            "Production replay audit starting: "
            f"cells={len(cells)}, mode={mode}, min_pf={min_pf:.2f}, "
            f"max_dd={max_dd_pct:.1f}%, min_trades={min_trades}"
        )
        for idx, cell in enumerate(cells, start=1):
            row = replay_tools.replay_cell(audit_config, loader, cell)
            verdict, reasons = replay_audit.evaluate_gates(
                row,
                min_pf=min_pf,
                max_dd_pct=max_dd_pct,
                min_trades=min_trades,
            )
            evidence = replay_audit._audit_evidence(row, verdict, reasons, thresholds)
            results.append(
                {
                    "cell": cell,
                    "row": row,
                    "verdict": verdict,
                    "reasons": reasons,
                    "evidence": evidence,
                }
            )
            logger.info(
                "Production replay audit [%s/%s] %s %s %s %s -> %s%s",
                idx,
                len(cells),
                cell.get("symbol"),
                cell.get("timeframe"),
                cell.get("regime"),
                cell.get("strategy_name"),
                verdict,
                f" ({'; '.join(reasons)})" if reasons else "",
            )

        out_dir_raw = str(getattr(self.config, "production_replay_audit_out_dir", "") or "").strip()
        if out_dir_raw:
            out_dir = Path(out_dir_raw)
            if not out_dir.is_absolute():
                out_dir = Path.cwd() / out_dir
        else:
            out_dir = Path(self.config.output_dir) / "replay_audit"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = out_dir / f"production_replay_audit_{stamp}.csv"
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "symbol",
                    "timeframe",
                    "regime",
                    "strategy",
                    "promotion_status",
                    "verdict",
                    "reasons",
                    "net_profit",
                    "return_pct",
                    "profit_factor",
                    "max_drawdown_pct",
                    "total_trades",
                    "expectancy_pips",
                ]
            )
            for result in results:
                cell = result["cell"]
                row = result["row"]
                writer.writerow(
                    [
                        cell.get("symbol"),
                        cell.get("timeframe"),
                        cell.get("regime"),
                        cell.get("strategy_name"),
                        cell.get("promotion_status"),
                        result["verdict"],
                        "; ".join(result["reasons"]),
                        row.get("net_profit"),
                        row.get("return_pct"),
                        row.get("profit_factor"),
                        row.get("max_drawdown_pct"),
                        row.get("total_trades"),
                        row.get("expectancy_pips"),
                    ]
                )

        summary: Dict[str, Any] = {
            "enabled": True,
            "ok": True,
            "mode": mode,
            "cells": len(results),
            "pass": sum(1 for item in results if item["verdict"] == "PASS"),
            "fail": sum(1 for item in results if item["verdict"] == "FAIL"),
            "unverified": sum(1 for item in results if item["verdict"] == "UNVERIFIED"),
            "report_path": str(report_path),
        }

        if mode == "apply":
            from pm_storage import _exclusive_file_lock

            with _exclusive_file_lock(_optimization_ledger_lock_path(ledger_path)):
                live_ledger_path = Path(self.config_file)
                if not live_ledger_path.is_absolute():
                    live_ledger_path = Path.cwd() / live_ledger_path
                if (
                    ledger_path.resolve() == live_ledger_path.resolve()
                    and ConfigLedger(str(ledger_path))._governed_publication_active()
                ):
                    raise RuntimeError(
                        "PRODUCTION_REPLAY_AUDIT_APPLY_FORBIDDEN_GOVERNED_LEDGER"
                    )
                current_bytes = ledger_path.read_bytes()
                if hashlib.sha256(current_bytes).hexdigest() != ledger_source_sha:
                    raise RuntimeError(
                        "PRODUCTION_REPLAY_AUDIT_LEDGER_CHANGED_DURING_REPLAY"
                    )
                backup = None
                if ledger_override is None:
                    backup = ledger_path.with_name(
                        f"{ledger_path.stem}.backup_replay_audit_{stamp}{ledger_path.suffix}"
                    )
                    shutil.copy2(ledger_path, backup)
                counters = replay_audit.apply_audit(
                    ledger,
                    results,
                    block_unverified=block_unverified,
                )
                tmp_path = ledger_path.with_suffix(
                    f"{ledger_path.suffix}.tmp_replay_audit"
                )
                tmp_path.write_text(
                    json.dumps(ledger, indent=2, default=str),
                    encoding="utf-8",
                )
                tmp_path.replace(ledger_path)
                if mutate_manager and ledger_override is None:
                    self.ledger.load()
                    self.symbol_configs = self.ledger.get_all_configs()
                if backup is not None:
                    summary["backup_path"] = str(backup)
                summary["counters"] = counters

        logger.info(
            "Production replay audit complete: "
            f"PASS={summary['pass']} FAIL={summary['fail']} UNVERIFIED={summary['unverified']} "
            f"mode={mode} report={report_path}"
        )
        return summary

    def _optimization_generation_paths(self) -> Tuple[Path, Path, Path]:
        """Return fixed-size staging and active-manifest paths for the ledger."""
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        stage_path = ledger_path.with_name(f".{ledger_path.name}.generation.staging")
        stage_manifest = ledger_path.with_name(
            f".{ledger_path.name}.generation.staging.manifest.json"
        )
        active_manifest = ledger_path.with_name(
            f"{ledger_path.stem}.generation.json"
        )
        return stage_path, stage_manifest, active_manifest

    @staticmethod
    def _is_digest_owned_name(name: str, prefix: str) -> bool:
        suffix = str(name)[len(prefix) :] if str(name).startswith(prefix) else ""
        return bool(
            len(suffix) == 64
            and all(character in "0123456789abcdef" for character in suffix)
        )

    @classmethod
    def _production_generation_retention_descriptor(
        cls,
        manifest: Mapping[str, Any],
        *,
        deployment_root: Path,
    ) -> Optional[Dict[str, str]]:
        """Derive exact current/rollback roots from one authority manifest."""

        if type(manifest) is not dict:
            return None
        generation_id = manifest.get("generation_id")
        ledger_sha = str(manifest.get("ledger_sha256") or "").lower()
        receipt = manifest.get("authority_chain_receipt")
        is_retention_descriptor = (
            manifest.get("schema_version")
            == "fxpm.production-generation-retention.v1"
        )
        run_value = manifest.get("production_authority_run_root")
        assembly_value = manifest.get("authority_assembly_path")
        if (
            type(generation_id) is not str
            or not generation_id
            or len(ledger_sha) != 64
            or any(character not in "0123456789abcdef" for character in ledger_sha)
            or (
                not is_retention_descriptor
                and (
                    type(receipt) is not dict
                    or receipt.get("generation_id") != generation_id
                    or str(receipt.get("ledger_sha256") or "").lower()
                    != ledger_sha
                )
            )
            or type(run_value) is not str
            or type(assembly_value) is not str
            or type(manifest.get("production_staged_ledger_source_path"))
            is not str
            or not manifest.get("production_staged_ledger_source_path")
        ):
            return None
        try:
            root = Path(deployment_root).resolve(strict=True)
            runs_root = (root / "fxpm-production-runs").resolve(strict=True)
            run_root = Path(run_value).resolve(strict=True)
            assembly_root = Path(assembly_value).resolve(strict=True)
            expected_digest = hashlib.sha256(generation_id.encode("utf-8")).hexdigest()
            if (
                run_root.parent != runs_root
                or assembly_root.parent != root
                or not cls._is_digest_owned_name(run_root.name, "run-")
                or not cls._is_digest_owned_name(
                    assembly_root.name, "fxpm-authority-"
                )
                or run_root.name[4:] != expected_digest
                or assembly_root.name[15:] != expected_digest
                or not run_root.is_dir()
                or not assembly_root.is_dir()
            ):
                return None
            source_value = manifest.get("production_staged_ledger_source_path")
            source = Path(source_value).resolve(strict=True)
            source.relative_to(run_root)
            if (
                not source.is_file()
                or source.stat().st_size > 64 * 1024 * 1024
                or hashlib.sha256(source.read_bytes()).hexdigest() != ledger_sha
            ):
                return None
            source_path = str(source)
            snapshot_path = ""
            snapshot_sha = ""
            if is_retention_descriptor:
                snapshot_value = manifest.get(
                    "previous_active_manifest_snapshot_path"
                )
                snapshot_sha = str(
                    manifest.get("previous_active_manifest_snapshot_sha256") or ""
                ).lower()
                snapshot = Path(snapshot_value).resolve(strict=True)
                relative_snapshot = snapshot.relative_to(runs_root)
                if (
                    not snapshot.is_file()
                    or snapshot.stat().st_size > 64 * 1024 * 1024
                ):
                    return None
                snapshot_raw = snapshot.read_bytes()
                snapshot_manifest = json.loads(snapshot_raw.decode("utf-8"))
                if (
                    type(snapshot_value) is not str
                    or len(snapshot_sha) != 64
                    or len(relative_snapshot.parts) != 3
                    or not cls._is_digest_owned_name(
                        relative_snapshot.parts[0], "run-"
                    )
                    or relative_snapshot.parts[1] != "rollback"
                    or snapshot.name != f"manifest-{snapshot_sha}.json"
                    or not snapshot.is_file()
                    or snapshot.stat().st_size > 64 * 1024 * 1024
                    or hashlib.sha256(snapshot_raw).hexdigest() != snapshot_sha
                    or type(snapshot_manifest) is not dict
                    or snapshot_manifest.get("generation_status") != "PROMOTED"
                    or snapshot_manifest.get("generation_id") != generation_id
                    or str(snapshot_manifest.get("ledger_sha256") or "").lower()
                    != ledger_sha
                    or type(snapshot_manifest.get("authority_chain_receipt"))
                    is not dict
                    or snapshot_manifest["authority_chain_receipt"].get(
                        "generation_id"
                    )
                    != generation_id
                    or str(
                        snapshot_manifest["authority_chain_receipt"].get(
                            "ledger_sha256"
                        )
                        or ""
                    ).lower()
                    != ledger_sha
                    or str(
                        snapshot_manifest.get("production_authority_run_root")
                        or ""
                    )
                    != str(run_root)
                    or str(snapshot_manifest.get("authority_assembly_path") or "")
                    != str(assembly_root)
                    or str(
                        snapshot_manifest.get(
                            "production_staged_ledger_source_path"
                        )
                        or ""
                    )
                    != source_path
                ):
                    return None
                snapshot_path = str(snapshot)
        except (OSError, RuntimeError, ValueError):
            return None
        return {
            "schema_version": "fxpm.production-generation-retention.v1",
            "generation_id": generation_id,
            "ledger_sha256": ledger_sha,
            "production_authority_run_root": str(run_root),
            "authority_assembly_path": str(assembly_root),
            "production_staged_ledger_source_path": source_path,
            **(
                {
                    "previous_active_manifest_snapshot_path": snapshot_path,
                    "previous_active_manifest_snapshot_sha256": snapshot_sha,
                }
                if is_retention_descriptor
                else {}
            ),
        }

    @staticmethod
    def _remove_owned_generation_directory(path: Path, parent: Path) -> None:
        parent = Path(parent).resolve(strict=True)
        path = Path(path)
        metadata = path.lstat()
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        if (
            stat.S_ISLNK(metadata.st_mode)
            or int(getattr(metadata, "st_file_attributes", 0) or 0)
            & reparse_flag
            or not stat.S_ISDIR(metadata.st_mode)
            or path.resolve(strict=True).parent != parent
        ):
            raise RuntimeError("PRODUCTION_RETENTION_OWNED_DIRECTORY_INVALID")
        shutil.rmtree(path)
        if path.exists() or path.is_symlink():
            raise RuntimeError("PRODUCTION_RETENTION_REMOVE_INCOMPLETE")

    @staticmethod
    def _remove_owned_generation_directory_verified(
        path: Path,
        parent: Path,
    ) -> None:
        """Remove one exact tree iteratively with bounded entry geometry."""

        parent = Path(parent).resolve(strict=True)
        root = Path(path)
        metadata = root.lstat()
        reparse_flag = getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
        )
        if (
            stat.S_ISLNK(metadata.st_mode)
            or int(getattr(metadata, "st_file_attributes", 0) or 0)
            & reparse_flag
            or not stat.S_ISDIR(metadata.st_mode)
            or root.resolve(strict=True).parent != parent
        ):
            raise RuntimeError(
                "PRODUCTION_RETENTION_OWNED_DIRECTORY_INVALID"
            )
        stack: list[tuple[Path, Any]] = [(root, None)]
        entries_seen = 0
        try:
            while stack:
                current, stream = stack[-1]
                if stream is None:
                    current_metadata = current.lstat()
                    if (
                        stat.S_ISLNK(current_metadata.st_mode)
                        or int(
                            getattr(
                                current_metadata,
                                "st_file_attributes",
                                0,
                            )
                            or 0
                        )
                        & reparse_flag
                        or not stat.S_ISDIR(current_metadata.st_mode)
                    ):
                        raise RuntimeError(
                            "PRODUCTION_RETENTION_OWNED_DIRECTORY_INVALID"
                        )
                    stream = os.scandir(current)
                    stack[-1] = (current, stream)
                try:
                    entry = next(stream)
                except StopIteration:
                    stream.close()
                    stack.pop()
                    current.rmdir()
                    continue
                entries_seen += 1
                if entries_seen > 2_000_000:
                    raise RuntimeError(
                        "SUPERVISED_GENERATION_RECOVERY_ENTRY_BOUND_EXCEEDED"
                    )
                child = Path(entry.path)
                child_metadata = child.lstat()
                if (
                    entry.is_symlink()
                    or int(
                        getattr(
                            child_metadata,
                            "st_file_attributes",
                            0,
                        )
                        or 0
                    )
                    & reparse_flag
                ):
                    raise RuntimeError(
                        "SUPERVISED_GENERATION_RECOVERY_REPARSE_FORBIDDEN"
                    )
                if stat.S_ISDIR(child_metadata.st_mode):
                    stack.append((child, None))
                elif stat.S_ISREG(child_metadata.st_mode):
                    child.unlink()
                else:
                    raise RuntimeError(
                        "SUPERVISED_GENERATION_RECOVERY_NONREGULAR_FORBIDDEN"
                    )
        finally:
            for _path, stream in stack:
                if stream is not None:
                    stream.close()
        if root.exists() or root.is_symlink():
            raise RuntimeError("PRODUCTION_RETENTION_REMOVE_INCOMPLETE")

    @staticmethod
    def _remove_owned_generation_file(path: Path, parent: Path) -> None:
        parent = Path(parent).resolve(strict=True)
        path = Path(path)
        metadata = path.lstat()
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        if (
            stat.S_ISLNK(metadata.st_mode)
            or int(getattr(metadata, "st_file_attributes", 0) or 0)
            & reparse_flag
            or not stat.S_ISREG(metadata.st_mode)
            or path.resolve(strict=True).parent != parent
        ):
            raise RuntimeError("PRODUCTION_RETENTION_OWNED_FILE_INVALID")
        path.unlink()
        if path.exists() or path.is_symlink():
            raise RuntimeError("PRODUCTION_RETENTION_FILE_REMOVE_INCOMPLETE")

    def _prune_production_generation_storage_locked(
        self,
        promoted_manifest: Mapping[str, Any],
    ) -> Dict[str, int]:
        """Discharge the journaled exact-old-predecessor cleanup intent."""

        policy = promoted_manifest.get("production_retention_policy")
        if type(policy) is not dict:
            return {
                "retained_generation_count": 0,
                "removed_run_root_count": 0,
                "removed_authority_assembly_count": 0,
                "removed_manifest_snapshot_count": 0,
            }
        if policy != {
            "schema_version": "fxpm.production-retention-policy.v1",
            "maximum_complete_generations": 2,
            "keep_current_and_previous": True,
        }:
            raise RuntimeError("PRODUCTION_RETENTION_POLICY_INVALID")
        deployment_config = str(
            getattr(self.config, "_deployment_config_path", "") or ""
        )
        root = Path(deployment_config).resolve(strict=True).parent
        runs_root = (root / "fxpm-production-runs").resolve(strict=True)
        current = self._production_generation_retention_descriptor(
            promoted_manifest,
            deployment_root=root,
        )
        if current is None:
            raise RuntimeError("PRODUCTION_RETENTION_CURRENT_INVALID")
        previous = promoted_manifest.get("previous_production_generation")
        if previous is not None:
            verified_previous = self._production_generation_retention_descriptor(
                previous,
                deployment_root=root,
            )
            if verified_previous != previous:
                raise RuntimeError("PRODUCTION_RETENTION_PREVIOUS_INVALID")
        _stage, _stage_manifest, active_manifest_path = (
            self._optimization_generation_paths()
        )
        if active_manifest_path.stat().st_size > 64 * 1024 * 1024:
            raise RuntimeError("PRODUCTION_RETENTION_ACTIVE_MANIFEST_TOO_LARGE")
        active = json.loads(active_manifest_path.read_text(encoding="utf-8"))
        active_current = self._production_generation_retention_descriptor(
            active,
            deployment_root=root,
        )
        if (
            active_current != current
            or active.get("previous_production_generation") != previous
        ):
            raise RuntimeError("PRODUCTION_RETENTION_ACTIVE_BINDING_MISMATCH")
        obsolete = promoted_manifest.get("obsolete_production_generation")
        if obsolete is None:
            return {
                "retained_generation_count": 1 + int(previous is not None),
                "removed_run_root_count": 0,
                "removed_authority_assembly_count": 0,
                "removed_manifest_snapshot_count": 0,
            }
        if type(obsolete) is not dict:
            raise RuntimeError("PRODUCTION_RETENTION_OBSOLETE_INVALID")
        if previous is None:
            raise RuntimeError("PRODUCTION_RETENTION_OBSOLETE_WITHOUT_PREVIOUS")
        previous_snapshot = Path(
            previous["previous_active_manifest_snapshot_path"]
        )
        previous_snapshot_payload = json.loads(
            previous_snapshot.read_text(encoding="utf-8")
        )
        if previous_snapshot_payload.get("previous_production_generation") != obsolete:
            raise RuntimeError(
                "PRODUCTION_RETENTION_OBSOLETE_SNAPSHOT_BINDING_MISMATCH"
            )
        protected = {
            str(current["production_authority_run_root"]),
            str(current["authority_assembly_path"]),
        }
        if previous is not None:
            protected.update(
                {
                    str(previous["production_authority_run_root"]),
                    str(previous["authority_assembly_path"]),
                }
            )
        obsolete_run = str(obsolete.get("production_authority_run_root") or "")
        obsolete_assembly = str(obsolete.get("authority_assembly_path") or "")
        if obsolete_run in protected or obsolete_assembly in protected:
            raise RuntimeError("PRODUCTION_RETENTION_PROTECTED_PATH_SELECTED")
        removed_assembly = 0
        removed_run = 0
        removed_snapshot = 0
        assembly_path = Path(obsolete_assembly)
        run_path = Path(obsolete_run)
        snapshot = Path(
            str(obsolete.get("previous_active_manifest_snapshot_path") or "")
        )
        obsolete_generation = str(obsolete.get("generation_id") or "")
        obsolete_digest = hashlib.sha256(
            obsolete_generation.encode("utf-8")
        ).hexdigest()
        expected_snapshot_parent = (
            Path(previous["production_authority_run_root"]) / "rollback"
        ).resolve(strict=True)
        if (
            run_path.parent.resolve(strict=True) != runs_root
            or run_path.name != f"run-{obsolete_digest}"
            or assembly_path.parent.resolve(strict=True) != root
            or assembly_path.name != f"fxpm-authority-{obsolete_digest}"
            or snapshot.parent.resolve(strict=True) != expected_snapshot_parent
            or snapshot.name
            != "manifest-"
            + str(obsolete.get("previous_active_manifest_snapshot_sha256") or "")
            + ".json"
            or snapshot == previous_snapshot
        ):
            raise RuntimeError("PRODUCTION_RETENTION_OBSOLETE_GEOMETRY_INVALID")
        if snapshot.exists():
            if snapshot.stat().st_size > 64 * 1024 * 1024:
                raise RuntimeError(
                    "PRODUCTION_RETENTION_OBSOLETE_SNAPSHOT_TOO_LARGE"
                )
            snapshot_raw = snapshot.read_bytes()
            snapshot_payload = json.loads(snapshot_raw.decode("utf-8"))
            snapshot_sha = str(
                obsolete.get("previous_active_manifest_snapshot_sha256") or ""
            )
            if (
                hashlib.sha256(snapshot_raw).hexdigest() != snapshot_sha
                or snapshot_payload.get("generation_status") != "PROMOTED"
                or snapshot_payload.get("generation_id") != obsolete_generation
                or str(snapshot_payload.get("ledger_sha256") or "").lower()
                != str(obsolete.get("ledger_sha256") or "").lower()
                or snapshot_payload.get("production_authority_run_root")
                != obsolete_run
                or snapshot_payload.get("authority_assembly_path")
                != obsolete_assembly
                or snapshot_payload.get("production_staged_ledger_source_path")
                != obsolete.get("production_staged_ledger_source_path")
                or type(snapshot_payload.get("authority_chain_receipt"))
                is not dict
                or snapshot_payload["authority_chain_receipt"].get(
                    "generation_id"
                )
                != obsolete_generation
            ):
                raise RuntimeError(
                    "PRODUCTION_RETENTION_OBSOLETE_SNAPSHOT_INVALID"
                )
        if run_path.exists() and assembly_path.exists() and snapshot.exists():
            verified_obsolete = (
                self._production_generation_retention_descriptor(
                    obsolete,
                    deployment_root=root,
                )
            )
            if verified_obsolete != obsolete:
                raise RuntimeError("PRODUCTION_RETENTION_OBSOLETE_VERIFY_FAILED")
        elif not snapshot.exists() and (run_path.exists() or assembly_path.exists()):
            raise RuntimeError(
                "PRODUCTION_RETENTION_OBSOLETE_PROOF_MISSING"
            )
        if assembly_path.exists() or assembly_path.is_symlink():
            self._remove_owned_generation_directory(
                assembly_path,
                root,
            )
            removed_assembly = 1
        if run_path.exists() or run_path.is_symlink():
            self._remove_owned_generation_directory(
                run_path,
                runs_root,
            )
            removed_run = 1
        if snapshot.exists() or snapshot.is_symlink():
            self._remove_owned_generation_file(snapshot, snapshot.parent)
            removed_snapshot = 1
        return {
            "retained_generation_count": 1 + int(previous is not None),
            "removed_run_root_count": removed_run,
            "removed_authority_assembly_count": removed_assembly,
            "removed_manifest_snapshot_count": removed_snapshot,
        }

    def _optimization_promotion_journal_path(self) -> Path:
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        return ledger_path.with_name(
            f".{ledger_path.name}.generation.promotion.json"
        )

    def _optimization_failure_manifest_path(self) -> Path:
        """Return the bounded diagnostic path that never aliases live staging."""

        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        return ledger_path.with_name(
            f".{ledger_path.name}.generation.last-failure.json"
        )

    def _optimization_promotion_lock_path(self) -> Path:
        return _optimization_ledger_lock_path(self.config_file)

    @staticmethod
    def _promotion_journal_digest(payload: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _promotion_journal_authorizes(
        self,
        *,
        base_ledger_sha256: str,
        target_ledger_sha256: str,
        generation_id: str,
    ) -> bool:
        """Return whether the durable journal authorizes this exact promotion.

        Hash equality alone cannot prove that a compare-and-swap happened: a
        repeat optimization may legitimately reproduce the live ledger bytes.
        Recovery therefore requires the journal that is written only after the
        raw-authority gate and immediately before the ledger replacement.
        """

        journal_path = self._optimization_promotion_journal_path()
        try:
            if not journal_path.exists() or journal_path.stat().st_size > 64 * 1024 * 1024:
                return False
            raw = journal_path.read_bytes()
            if len(raw) > 64 * 1024 * 1024:
                return False
            payload = json.loads(raw.decode("utf-8"))
            if type(payload) is not dict:
                return False
            digest = payload.get("content_sha256")
            body = dict(payload)
            body.pop("content_sha256", None)
            if (
                payload.get("schema_version")
                != "fxpm.optimization-promotion-journal.v1"
                or type(digest) is not str
                or self._promotion_journal_digest(body) != digest
            ):
                return False
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            ledger_path = ledger_path.resolve()
            _stage, stage_manifest, active_manifest = (
                self._optimization_generation_paths()
            )
            promoted_manifest = payload.get("promoted_manifest")
            base_sha = str(base_ledger_sha256 or "").lower()
            target_sha = str(target_ledger_sha256 or "").lower()
            expected_generation = str(generation_id or "")
            authority_receipt = (
                promoted_manifest.get("authority_chain_receipt")
                if type(promoted_manifest) is dict
                else None
            )
            return bool(
                payload.get("ledger_path") == str(ledger_path)
                and payload.get("active_manifest_path")
                == str(active_manifest.resolve())
                and payload.get("stage_manifest_path")
                == str(stage_manifest.resolve())
                and str(payload.get("base_ledger_sha256") or "").lower()
                == base_sha
                and str(payload.get("target_ledger_sha256") or "").lower()
                == target_sha
                and type(promoted_manifest) is dict
                and promoted_manifest.get("generation_status") == "PROMOTED"
                and promoted_manifest.get("generation_id") == expected_generation
                and type(authority_receipt) is dict
                and authority_receipt.get("generation_id") == expected_generation
                and str(promoted_manifest.get("ledger_sha256") or "").lower()
                == target_sha
            )
        except Exception:
            return False

    def _active_production_generation_reference_locked(
        self,
        *,
        generation_id: str,
        ledger_sha256: str,
        run_root: Path,
        authority_assembly_path: Path,
        staged_ledger_source_path: Path,
    ) -> Optional[str]:
        """Return ACTIVE/PREVIOUS only for an exact durable generation ref."""

        _stage, _stage_manifest, active_path = self._optimization_generation_paths()
        if not active_path.exists():
            return None
        try:
            if active_path.stat().st_size > 64 * 1024 * 1024:
                return None
            active = json.loads(active_path.read_text(encoding="utf-8"))
            exact = {
                "generation_id": str(generation_id),
                "ledger_sha256": str(ledger_sha256).lower(),
                "production_authority_run_root": str(Path(run_root).resolve(strict=True)),
                "authority_assembly_path": str(
                    Path(authority_assembly_path).resolve(strict=True)
                ),
                "production_staged_ledger_source_path": str(
                    Path(staged_ledger_source_path).resolve(strict=True)
                ),
            }
            deployment_config = str(
                getattr(self.config, "_deployment_config_path", "") or ""
            )
            deployment_root = Path(deployment_config).resolve(strict=True).parent
            live_ledger = Path(self.config_file)
            if not live_ledger.is_absolute():
                live_ledger = Path.cwd() / live_ledger
            live_bytes = live_ledger.resolve(strict=True).read_bytes()
            if hashlib.sha256(live_bytes).hexdigest() != str(
                active.get("ledger_sha256") or ""
            ).lower():
                return None
            receipt = active.get("authority_chain_receipt")
            active_descriptor = (
                self._production_generation_retention_descriptor(
                    active,
                    deployment_root=deployment_root,
                )
            )
            if (
                active.get("generation_status") == "PROMOTED"
                and active.get("generation_id") == exact["generation_id"]
                and str(active.get("ledger_sha256") or "").lower()
                == exact["ledger_sha256"]
                and active.get("production_authority_run_root")
                == exact["production_authority_run_root"]
                and active.get("authority_assembly_path")
                == exact["authority_assembly_path"]
                and active.get("production_staged_ledger_source_path")
                == exact["production_staged_ledger_source_path"]
                and type(receipt) is dict
                and receipt.get("generation_id") == exact["generation_id"]
                and str(receipt.get("ledger_sha256") or "").lower()
                == exact["ledger_sha256"]
                and type(active.get("ledger_status")) is dict
                and active["ledger_status"].get(
                    "complete_current_contract_universe"
                )
                is True
                and active_descriptor is not None
            ):
                return "ACTIVE"
            previous = active.get("previous_production_generation")
            verified_previous = (
                self._production_generation_retention_descriptor(
                    previous,
                    deployment_root=deployment_root,
                )
                if type(previous) is dict
                else None
            )
            if (
                active_descriptor is not None
                and active.get("generation_status") == "PROMOTED"
                and type(active.get("ledger_status")) is dict
                and active["ledger_status"].get(
                    "complete_current_contract_universe"
                )
                is True
                and verified_previous is not None
                and all(
                verified_previous.get(key) == value
                for key, value in exact.items()
                )
            ):
                return "PREVIOUS"
        except Exception:
            return None
        return None

    def _promotion_journal_reference_locked(
        self,
        *,
        generation_id: str,
        ledger_sha256: str,
        run_root: Path,
        authority_assembly_path: Path,
        staged_ledger_source_path: Path,
    ) -> str:
        """Classify the durable journal as NONE/EXACT/UNRELATED/AMBIGUOUS."""

        journal_path = self._optimization_promotion_journal_path()
        if not journal_path.exists() and not journal_path.is_symlink():
            return "NONE"
        try:
            if journal_path.stat().st_size > 64 * 1024 * 1024:
                return "AMBIGUOUS"
            payload = json.loads(journal_path.read_text(encoding="utf-8"))
            if type(payload) is not dict:
                return "AMBIGUOUS"
            digest = payload.get("content_sha256")
            body = dict(payload)
            body.pop("content_sha256", None)
            _stage, stage_manifest, active_manifest = (
                self._optimization_generation_paths()
            )
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            ledger_path = ledger_path.resolve()
            promoted = payload.get("promoted_manifest")
            if (
                payload.get("schema_version")
                != "fxpm.optimization-promotion-journal.v1"
                or type(digest) is not str
                or self._promotion_journal_digest(body) != digest
                or payload.get("ledger_path") != str(ledger_path)
                or payload.get("active_manifest_path")
                != str(active_manifest.resolve())
                or payload.get("stage_manifest_path")
                != str(stage_manifest.resolve())
                or type(promoted) is not dict
                or promoted.get("generation_status") != "PROMOTED"
            ):
                return "AMBIGUOUS"
            receipt = promoted.get("authority_chain_receipt")
            journal_generation = promoted.get("generation_id")
            target_sha = str(payload.get("target_ledger_sha256") or "").lower()
            promoted_sha = str(promoted.get("ledger_sha256") or "").lower()
            base_sha = str(payload.get("base_ledger_sha256") or "").lower()
            if (
                type(journal_generation) is not str
                or not journal_generation
                or any(
                    len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                    for value in (base_sha, target_sha, promoted_sha)
                )
                or target_sha != promoted_sha
                or type(receipt) is not dict
                or receipt.get("generation_id") != journal_generation
                or str(receipt.get("ledger_sha256") or "").lower() != target_sha
            ):
                return "AMBIGUOUS"
            deployment_config = str(
                getattr(self.config, "_deployment_config_path", "") or ""
            )
            deployment_root = Path(deployment_config).resolve(strict=True).parent
            journal_descriptor = self._production_generation_retention_descriptor(
                promoted,
                deployment_root=deployment_root,
            )
            if journal_descriptor is None:
                return "AMBIGUOUS"
            candidate_paths = {
                str(Path(run_root).resolve(strict=True)),
                str(Path(authority_assembly_path).resolve(strict=True)),
                str(Path(staged_ledger_source_path).resolve(strict=True)),
            }
            journal_paths = {
                str(journal_descriptor["production_authority_run_root"]),
                str(journal_descriptor["authority_assembly_path"]),
                str(journal_descriptor["production_staged_ledger_source_path"]),
            }
            if journal_generation != generation_id:
                # A journal for another generation is harmless only when its
                # complete, content-bound storage is disjoint.  Treat any alias
                # or malformed descriptor as ambiguous so cleanup fails closed.
                return (
                    "UNRELATED"
                    if candidate_paths.isdisjoint(journal_paths)
                    else "AMBIGUOUS"
                )
            exact = bool(
                target_sha == str(ledger_sha256).lower()
                and journal_descriptor["production_authority_run_root"]
                == str(Path(run_root).resolve(strict=True))
                and journal_descriptor["authority_assembly_path"]
                == str(Path(authority_assembly_path).resolve(strict=True))
                and journal_descriptor["production_staged_ledger_source_path"]
                == str(Path(staged_ledger_source_path).resolve(strict=True))
            )
            return "EXACT" if exact else "AMBIGUOUS"
        except Exception:
            return "AMBIGUOUS"

    def _write_promotion_journal(
        self,
        *,
        base_ledger_sha256: str,
        target_ledger_sha256: str,
        stage_manifest_path: Path,
        promoted_manifest: Mapping[str, Any],
    ) -> Path:
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        _stage, expected_stage_manifest, active_manifest = (
            self._optimization_generation_paths()
        )
        if Path(stage_manifest_path).resolve() != expected_stage_manifest.resolve():
            raise RuntimeError("OPTIMIZATION_PROMOTION_STAGE_MANIFEST_PATH_INVALID")
        body: Dict[str, Any] = {
            "schema_version": "fxpm.optimization-promotion-journal.v1",
            "ledger_path": str(ledger_path),
            "active_manifest_path": str(active_manifest.resolve()),
            "stage_manifest_path": str(expected_stage_manifest.resolve()),
            "base_ledger_sha256": str(base_ledger_sha256).lower(),
            "target_ledger_sha256": str(target_ledger_sha256).lower(),
            "promoted_manifest": dict(promoted_manifest),
        }
        for field in ("base_ledger_sha256", "target_ledger_sha256"):
            value = body[field]
            if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
                raise RuntimeError("OPTIMIZATION_PROMOTION_JOURNAL_HASH_INVALID")
        body["content_sha256"] = self._promotion_journal_digest(body)
        journal_path = self._optimization_promotion_journal_path()
        if journal_path.exists() or journal_path.is_symlink():
            raise RuntimeError("OPTIMIZATION_PROMOTION_JOURNAL_ALREADY_EXISTS")
        self._atomic_write_json(journal_path, body)
        return journal_path

    def _reconcile_interrupted_promotion(
        self,
        *,
        lock_timeout_seconds: Optional[float] = None,
    ) -> str:
        """Serialize recovery with every writer of this exact live ledger."""

        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(
            self._optimization_promotion_lock_path(),
            timeout_seconds=lock_timeout_seconds,
        ):
            return self._reconcile_interrupted_promotion_locked()

    def _supervised_generation_reference_state_locked(
        self,
        *,
        generation_id: str,
        deployment_root: Path,
    ) -> str:
        """Classify exact durable references without collapsing corruption.

        ``DISJOINT`` is returned only after the live ledger, active manifest,
        current authority, and optional predecessor all re-open coherently.
        Any unreadable or partly valid state is ``AMBIGUOUS`` and is never a
        deletion authorization.
        """

        _stage, _stage_manifest, active_path = (
            self._optimization_generation_paths()
        )
        if not active_path.exists() and not active_path.is_symlink():
            return "DISJOINT"
        try:
            metadata = active_path.lstat()
            reparse_flag = getattr(
                stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
            )
            if (
                stat.S_ISLNK(metadata.st_mode)
                or int(
                    getattr(metadata, "st_file_attributes", 0) or 0
                )
                & reparse_flag
                or not stat.S_ISREG(metadata.st_mode)
                or not 0 < metadata.st_size <= 64 * 1024 * 1024
            ):
                return "AMBIGUOUS"
            raw = active_path.read_bytes()
            if len(raw) != metadata.st_size:
                return "AMBIGUOUS"
            active = json.loads(raw.decode("utf-8"))
            if (
                type(active) is not dict
                or active.get("generation_status") != "PROMOTED"
            ):
                return "AMBIGUOUS"
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            ledger_path = ledger_path.resolve(strict=True)
            live_raw = ledger_path.read_bytes()
            active_sha = str(active.get("ledger_sha256") or "").lower()
            if (
                len(active_sha) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in active_sha
                )
                or hashlib.sha256(live_raw).hexdigest() != active_sha
            ):
                return "AMBIGUOUS"
            current = self._production_generation_retention_descriptor(
                active,
                deployment_root=deployment_root,
            )
            if current is None:
                return "AMBIGUOUS"
            previous_payload = active.get(
                "previous_production_generation"
            )
            previous = None
            if previous_payload is not None:
                if type(previous_payload) is not dict:
                    return "AMBIGUOUS"
                previous = (
                    self._production_generation_retention_descriptor(
                        previous_payload,
                        deployment_root=deployment_root,
                    )
                )
                if previous is None or previous != previous_payload:
                    return "AMBIGUOUS"
            if current.get("generation_id") == generation_id:
                return "REFERENCED_ACTIVE"
            if (
                previous is not None
                and previous.get("generation_id") == generation_id
            ):
                return "REFERENCED_PREVIOUS"
            obsolete = active.get("obsolete_production_generation")
            if type(obsolete) is dict and (
                obsolete.get("generation_id") == generation_id
            ):
                return "REFERENCED_OBSOLETE"
            return "DISJOINT"
        except Exception:
            return "AMBIGUOUS"

    def _discard_supervised_fixed_stage_locked(
        self,
        *,
        generation_id: str,
    ) -> str:
        """Discard fixed staging only when its complete identity is exact."""

        stage_path, stage_manifest_path, _active = (
            self._optimization_generation_paths()
        )
        stage_exists = stage_path.exists() or stage_path.is_symlink()
        manifest_exists = (
            stage_manifest_path.exists()
            or stage_manifest_path.is_symlink()
        )
        if not stage_exists and not manifest_exists:
            return "NONE"
        # A stage without its manifest has no ownership proof.  A manifest
        # without its stage is different: fixed-stage cleanup deliberately
        # unlinks the stage first, so that exact intermediate state must be
        # recoverable after an ordinary interruption.
        if stage_exists and not manifest_exists:
            return "AMBIGUOUS"
        try:
            reparse_flag = getattr(
                stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
            )
            manifest_metadata = stage_manifest_path.lstat()
            if (
                stat.S_ISLNK(manifest_metadata.st_mode)
                or int(
                    getattr(
                        manifest_metadata,
                        "st_file_attributes",
                        0,
                    )
                    or 0
                )
                & reparse_flag
                or not stat.S_ISREG(manifest_metadata.st_mode)
                or not 0
                < manifest_metadata.st_size
                <= 64 * 1024 * 1024
            ):
                return "AMBIGUOUS"
            payload = json.loads(
                stage_manifest_path.read_text(encoding="utf-8")
            )
            ledger_sha = str(
                payload.get("ledger_sha256") if type(payload) is dict else ""
            ).lower()
            if (
                type(payload) is not dict
                or type(payload.get("generation_id")) is not str
                or len(ledger_sha) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in ledger_sha
                )
                or payload.get("ledger_staging_path") != str(stage_path)
            ):
                return "AMBIGUOUS"
            if payload["generation_id"] != generation_id:
                return "UNRELATED"
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            live_raw = (
                ledger_path.resolve().read_bytes()
                if ledger_path.resolve().exists()
                else b""
            )
            if hashlib.sha256(live_raw).hexdigest() == ledger_sha:
                return "AMBIGUOUS"
            if not stage_exists:
                stage_manifest_path.unlink()
                if (
                    stage_manifest_path.exists()
                    or stage_manifest_path.is_symlink()
                ):
                    return "AMBIGUOUS"
                return "DISCARDED_MANIFEST_ONLY"
            stage_metadata = stage_path.lstat()
            if (
                stat.S_ISLNK(stage_metadata.st_mode)
                or int(
                    getattr(
                        stage_metadata,
                        "st_file_attributes",
                        0,
                    )
                    or 0
                )
                & reparse_flag
                or not stat.S_ISREG(stage_metadata.st_mode)
                or not 0
                < stage_metadata.st_size
                <= 64 * 1024 * 1024
                or hashlib.sha256(stage_path.read_bytes()).hexdigest()
                != ledger_sha
            ):
                return "AMBIGUOUS"
            if not self._discard_fixed_stage_for_generation_locked(
                generation_id=generation_id,
                ledger_sha256=ledger_sha,
            ):
                return "AMBIGUOUS"
            return "DISCARDED"
        except Exception:
            return "AMBIGUOUS"

    def _recover_abandoned_supervised_generation(
        self,
        generation_id: str,
        *,
        lock_timeout_seconds: float = 0.0,
    ) -> str:
        """Reconcile and reclaim one exact, process-tree-empty generation.

        The caller is responsible for proving that the supervised process tree
        is gone.  This method adds the independent durable-state proof needed
        before any exact runner-owned directory can be removed.
        """

        if (
            type(generation_id) is not str
            or len(generation_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in generation_id
            )
        ):
            raise ValueError(
                "SUPERVISED_GENERATION_RECOVERY_ID_INVALID"
            )
        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(
            self._optimization_promotion_lock_path(),
            timeout_seconds=lock_timeout_seconds,
        ):
            reconciliation = self._reconcile_interrupted_promotion_locked()
            journal_path = self._optimization_promotion_journal_path()
            if journal_path.exists() or journal_path.is_symlink():
                raise RuntimeError(
                    "SUPERVISED_GENERATION_RECOVERY_JOURNAL_REMAINS"
                )
            deployment_config = str(
                getattr(self.config, "_deployment_config_path", "") or ""
            )
            deployment_root = Path(deployment_config).resolve(
                strict=True
            ).parent
            reference = (
                self._supervised_generation_reference_state_locked(
                    generation_id=generation_id,
                    deployment_root=deployment_root,
                )
            )
            if reference == "AMBIGUOUS":
                raise RuntimeError(
                    "SUPERVISED_GENERATION_RECOVERY_REFERENCE_AMBIGUOUS"
                )
            if reference.startswith("REFERENCED_"):
                return f"{reference}:{reconciliation}"
            stage_state = self._discard_supervised_fixed_stage_locked(
                generation_id=generation_id,
            )
            if stage_state == "AMBIGUOUS":
                raise RuntimeError(
                    "SUPERVISED_GENERATION_RECOVERY_STAGE_AMBIGUOUS"
                )
            digest = hashlib.sha256(
                generation_id.encode("utf-8")
            ).hexdigest()
            runs_root = deployment_root / "fxpm-production-runs"
            run_root = runs_root / f"run-{digest}"
            assembly = deployment_root / f"fxpm-authority-{digest}"
            removed: list[str] = []
            for path, parent, label in (
                (assembly, deployment_root, "ASSEMBLY"),
                (run_root, runs_root, "RUN_ROOT"),
            ):
                if not path.exists() and not path.is_symlink():
                    continue
                if not parent.exists() or not parent.is_dir():
                    raise RuntimeError(
                        "SUPERVISED_GENERATION_RECOVERY_PARENT_INVALID"
                    )
                self._remove_owned_generation_directory_verified(
                    path,
                    parent,
                )
                removed.append(label)
            status = "REMOVED" if removed else "ABSENT"
            return (
                f"{status}:{','.join(removed) or 'NONE'}:"
                f"stage={stage_state}:journal={reconciliation}"
            )

    def _reconcile_interrupted_promotion_locked(self) -> str:
        """Finish or safely abort a journaled ledger/manifest publication."""
        journal_path = self._optimization_promotion_journal_path()
        if not journal_path.exists():
            return "NONE"
        try:
            if journal_path.stat().st_size > 64 * 1024 * 1024:
                raise ValueError("PROMOTION_JOURNAL_TOO_LARGE")
            raw = journal_path.read_bytes()
            if len(raw) > 64 * 1024 * 1024:
                raise ValueError("PROMOTION_JOURNAL_TOO_LARGE")
            payload = json.loads(raw.decode("utf-8"))
            if type(payload) is not dict:
                raise ValueError("PROMOTION_JOURNAL_INVALID")
            digest = payload.get("content_sha256")
            body = dict(payload)
            body.pop("content_sha256", None)
            if (
                payload.get("schema_version")
                != "fxpm.optimization-promotion-journal.v1"
                or type(digest) is not str
                or self._promotion_journal_digest(body) != digest
            ):
                raise ValueError("PROMOTION_JOURNAL_CONTENT_INVALID")
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            ledger_path = ledger_path.resolve()
            _stage, stage_manifest, active_manifest = (
                self._optimization_generation_paths()
            )
            if (
                payload.get("ledger_path") != str(ledger_path)
                or payload.get("active_manifest_path")
                != str(active_manifest.resolve())
                or payload.get("stage_manifest_path")
                != str(stage_manifest.resolve())
            ):
                raise ValueError("PROMOTION_JOURNAL_PATH_BINDING_INVALID")
            base_sha = str(payload.get("base_ledger_sha256") or "").lower()
            target_sha = str(payload.get("target_ledger_sha256") or "").lower()
            promoted_manifest = payload.get("promoted_manifest")
            if (
                len(base_sha) != 64
                or len(target_sha) != 64
                or type(promoted_manifest) is not dict
                or promoted_manifest.get("generation_status") != "PROMOTED"
                or str(promoted_manifest.get("ledger_sha256") or "").lower()
                != target_sha
            ):
                raise ValueError("PROMOTION_JOURNAL_SEMANTICS_INVALID")
            live_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
            live_sha = hashlib.sha256(live_bytes).hexdigest()
            if live_sha == target_sha:
                self._atomic_write_json(active_manifest, promoted_manifest)
                self._prune_production_generation_storage_locked(
                    promoted_manifest
                )
                stage_manifest.unlink(missing_ok=True)
                journal_path.unlink()
                self._last_promotion_authorization = None
                return "COMPLETED"
            if live_sha == base_sha:
                journal_path.unlink()
                return "ABORTED_BEFORE_CAS"
            raise RuntimeError(
                "OPTIMIZATION_PROMOTION_EXTERNAL_LEDGER_CHANGE:" + live_sha
            )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"OPTIMIZATION_PROMOTION_JOURNAL_INVALID:{type(exc).__name__}:{exc}"
            ) from exc

    def rollback_to_previous_production_generation(self) -> Dict[str, Any]:
        """Re-publish the retained predecessor through the normal raw/CAS gate.

        Rollback is deliberately one generation deep.  It re-verifies the
        predecessor's retained ledger and authority artifacts, retains the
        displaced current generation as the new predecessor, and uses the same
        durable journal as an optimization promotion.  A crash therefore leaves
        either the old current generation or a recoverable rollback publication.
        """

        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(self._optimization_promotion_lock_path()):
            self._reconcile_interrupted_promotion_locked()
            ledger_path = Path(self.config_file)
            if not ledger_path.is_absolute():
                ledger_path = Path.cwd() / ledger_path
            ledger_path = ledger_path.resolve(strict=True)
            stage_path, stage_manifest_path, active_path = (
                self._optimization_generation_paths()
            )
            if not active_path.exists() or active_path.stat().st_size > 64 * 1024 * 1024:
                raise RuntimeError("PRODUCTION_ROLLBACK_ACTIVE_MANIFEST_UNAVAILABLE")
            active_raw = active_path.read_bytes()
            active = json.loads(active_raw.decode("utf-8"))
            if type(active) is not dict or active.get("generation_status") != "PROMOTED":
                raise RuntimeError("PRODUCTION_ROLLBACK_ACTIVE_MANIFEST_INVALID")
            live_raw = ledger_path.read_bytes()
            live_sha = hashlib.sha256(live_raw).hexdigest()
            active_ledger_sha = str(active.get("ledger_sha256") or "").lower()
            if live_sha != active_ledger_sha:
                raise RuntimeError("PRODUCTION_ROLLBACK_LIVE_ACTIVE_MISMATCH")
            if (
                type(active.get("ledger_status")) is not dict
                or active["ledger_status"].get("complete_current_contract_universe")
                is not True
            ):
                raise RuntimeError("PRODUCTION_ROLLBACK_CURRENT_LEDGER_INCOMPLETE")
            policy = active.get("production_retention_policy")
            expected_policy = {
                "schema_version": "fxpm.production-retention-policy.v1",
                "maximum_complete_generations": 2,
                "keep_current_and_previous": True,
            }
            if policy != expected_policy:
                raise RuntimeError("PRODUCTION_ROLLBACK_RETENTION_POLICY_INVALID")
            deployment_config = str(
                getattr(self.config, "_deployment_config_path", "") or ""
            )
            deployment_root = Path(deployment_config).resolve(strict=True).parent
            current_descriptor = self._production_generation_retention_descriptor(
                active,
                deployment_root=deployment_root,
            )
            previous = active.get("previous_production_generation")
            previous_descriptor = (
                self._production_generation_retention_descriptor(
                    previous,
                    deployment_root=deployment_root,
                )
                if type(previous) is dict
                else None
            )
            if current_descriptor is None or previous_descriptor != previous:
                raise RuntimeError("PRODUCTION_ROLLBACK_RETAINED_GENERATION_INVALID")
            previous_snapshot_path = Path(
                previous_descriptor["previous_active_manifest_snapshot_path"]
            ).resolve(strict=True)
            previous_snapshot_raw = previous_snapshot_path.read_bytes()
            previous_snapshot = json.loads(previous_snapshot_raw.decode("utf-8"))
            if (
                type(previous_snapshot) is not dict
                or previous_snapshot.get("generation_status") != "PROMOTED"
                or type(previous_snapshot.get("ledger_status")) is not dict
                or previous_snapshot["ledger_status"].get(
                    "complete_current_contract_universe"
                )
                is not True
            ):
                raise RuntimeError("PRODUCTION_ROLLBACK_PREVIOUS_MANIFEST_INVALID")
            previous_source = Path(
                previous_descriptor["production_staged_ledger_source_path"]
            ).resolve(strict=True)
            if previous_source.stat().st_size > 64 * 1024 * 1024:
                raise RuntimeError("PRODUCTION_ROLLBACK_LEDGER_TOO_LARGE")
            previous_ledger_raw = previous_source.read_bytes()
            previous_ledger_sha = hashlib.sha256(previous_ledger_raw).hexdigest()
            if previous_ledger_sha != previous_descriptor["ledger_sha256"]:
                raise RuntimeError("PRODUCTION_ROLLBACK_LEDGER_HASH_MISMATCH")

            active_sha = hashlib.sha256(active_raw).hexdigest()
            displaced_snapshot = (
                Path(previous_descriptor["production_authority_run_root"])
                / "rollback"
                / f"manifest-{active_sha}.json"
            )
            self._atomic_write_exact_bytes(displaced_snapshot, active_raw)
            if hashlib.sha256(displaced_snapshot.read_bytes()).hexdigest() != active_sha:
                raise RuntimeError("PRODUCTION_ROLLBACK_CURRENT_SNAPSHOT_MISMATCH")
            displaced_descriptor = {
                **dict(current_descriptor),
                "previous_active_manifest_snapshot_path": str(
                    displaced_snapshot.resolve(strict=True)
                ),
                "previous_active_manifest_snapshot_sha256": active_sha,
            }
            rollback_manifest = dict(previous_snapshot)
            for stale_key in (
                "previous_production_generation",
                "obsolete_production_generation",
                "promoted_at",
                "ledger_path",
            ):
                rollback_manifest.pop(stale_key, None)
            rollback_manifest.update(
                {
                    "generation_status": "COMPLETE",
                    "base_ledger_sha256": live_sha,
                    "base_active_manifest_sha256": active_sha,
                    "ledger_sha256": previous_ledger_sha,
                    "previous_production_generation": displaced_descriptor,
                    "production_retention_policy": expected_policy,
                    "rollback_publication": {
                        "schema_version": "fxpm.production-rollback.v1",
                        "rolled_back_from_generation_id": active.get("generation_id"),
                        "rolled_back_to_generation_id": previous_snapshot.get(
                            "generation_id"
                        ),
                        "source_manifest_snapshot_sha256": previous_descriptor[
                            "previous_active_manifest_snapshot_sha256"
                        ],
                        "requested_at": datetime.now(timezone.utc).isoformat(),
                    },
                }
            )
            rollback_descriptor = self._production_generation_retention_descriptor(
                rollback_manifest,
                deployment_root=deployment_root,
            )
            descriptor_keys = (
                "generation_id",
                "ledger_sha256",
                "production_authority_run_root",
                "authority_assembly_path",
                "production_staged_ledger_source_path",
            )
            if (
                rollback_descriptor is None
                or any(
                    rollback_descriptor.get(key) != previous_descriptor.get(key)
                    for key in descriptor_keys
                )
                or self._production_generation_retention_descriptor(
                    displaced_descriptor,
                    deployment_root=deployment_root,
                )
                != displaced_descriptor
            ):
                raise RuntimeError("PRODUCTION_ROLLBACK_DESCRIPTOR_REVERIFY_FAILED")

            self._atomic_write_exact_bytes(stage_path, previous_ledger_raw)
            self._atomic_write_json(
                stage_manifest_path,
                {**rollback_manifest, "ledger_staging_path": str(stage_path)},
            )
            self._promote_staged_generation_locked(
                stage_path,
                stage_manifest_path,
                rollback_manifest,
                universe=self._native_universe_for_optimization(),
            )
            published = json.loads(active_path.read_text(encoding="utf-8"))
            return {
                "schema_version": "fxpm.production-rollback-result.v1",
                "status": "ROLLED_BACK_TO_PREVIOUS",
                "rolled_back_from_generation_id": active.get("generation_id"),
                "active_generation_id": published.get("generation_id"),
                "active_ledger_sha256": published.get("ledger_sha256"),
                "ledger_changed": previous_ledger_sha != live_sha,
                "active_manifest_path": str(active_path.resolve(strict=True)),
            }

    @staticmethod
    def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(
            f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
        )
        try:
            with temp.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True, default=str)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)

    @staticmethod
    def _atomic_write_exact_bytes(path: Path, payload: bytes) -> None:
        """Atomically stage already-verified bytes without reserialization."""

        if type(payload) is not bytes or not payload:
            raise RuntimeError("OPTIMIZATION_EXACT_STAGE_BYTES_INVALID")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            with temp.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)

    def _stage_optimization_generation(
        self,
        configs: Dict[str, SymbolConfig],
        manifest: Dict[str, Any],
    ) -> Tuple[Path, Path, str]:
        """Write one bounded, non-live generation checkpoint atomically."""
        stage_path, stage_manifest, _active_manifest = (
            self._optimization_generation_paths()
        )
        payload = {
            symbol: config.to_dict()
            for symbol, config in sorted(configs.items())
        }
        encoded = json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        temp = stage_path.with_name(
            f".{stage_path.name}.{os.getpid()}.{time.time_ns()}.tmp"
        )
        stage_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with temp.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, stage_path)
        finally:
            temp.unlink(missing_ok=True)
        ledger_sha = hashlib.sha256(encoded).hexdigest()
        staged_manifest = {
            **dict(manifest or {}),
            "ledger_staging_path": str(stage_path),
            "ledger_sha256": ledger_sha,
            "staged_at": datetime.now(timezone.utc).isoformat(),
        }
        self._atomic_write_json(stage_manifest, staged_manifest)
        return stage_path, stage_manifest, ledger_sha

    def _discard_fixed_stage_for_generation_locked(
        self,
        *,
        generation_id: str,
        ledger_sha256: str,
    ) -> bool:
        """Delete fixed staging only when its manifest proves exact ownership."""

        stage_path, stage_manifest_path, _active = self._optimization_generation_paths()
        if not stage_manifest_path.exists() or not stage_path.exists():
            return False
        try:
            if stage_manifest_path.stat().st_size > 64 * 1024 * 1024:
                return False
            payload = json.loads(stage_manifest_path.read_text(encoding="utf-8"))
            expected_sha = str(ledger_sha256 or "").lower()
            if (
                type(payload) is not dict
                or payload.get("generation_id") != generation_id
                or str(payload.get("ledger_sha256") or "").lower() != expected_sha
                or payload.get("ledger_staging_path") != str(stage_path)
                or hashlib.sha256(stage_path.read_bytes()).hexdigest() != expected_sha
            ):
                return False
            stage_path.unlink()
            stage_manifest_path.unlink()
            return True
        except (OSError, RuntimeError, ValueError):
            return False

    def _validate_staged_generation(
        self,
        stage_path: Path,
        expected_configs: Dict[str, SymbolConfig],
        universe: Any,
        *,
        required_symbols: Optional[Iterable[str]] = None,
    ) -> None:
        """Deserialize and prove the complete staged roster before promotion."""
        try:
            raw = json.loads(Path(stage_path).read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(
                f"OPTIMIZATION_GENERATION_STAGE_UNREADABLE:{type(exc).__name__}:{exc}"
            ) from exc
        if not isinstance(raw, dict):
            raise RuntimeError("OPTIMIZATION_GENERATION_STAGE_NOT_OBJECT")
        configured_symbols = {str(symbol) for symbol in self.symbols}
        expected_serialized_symbols = {str(symbol) for symbol in expected_configs}
        required_winner_symbols = {
            str(symbol)
            for symbol in (
                self.symbols if required_symbols is None else required_symbols
            )
        }
        if not required_winner_symbols.issubset(configured_symbols):
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_REQUIRED_SYMBOL_OUTSIDE_CONFIGURED_UNIVERSE"
            )
        if set(raw) != expected_serialized_symbols:
            missing = sorted(expected_serialized_symbols - set(raw))
            extra = sorted(set(raw) - expected_serialized_symbols)
            raise RuntimeError(
                f"OPTIMIZATION_GENERATION_ROSTER_MISMATCH:missing={missing}:extra={extra}"
            )
        missing_required = sorted(required_winner_symbols - set(raw))
        # seq987: staged roster equals the required set exactly; a stale
        # data-state-only symbol inside the configured universe is rejected.
        extra_config = sorted(set(raw) - required_winner_symbols)
        if missing_required or extra_config:
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_REQUIRED_ROSTER_MISMATCH:"
                f"missing={missing_required}:extra={extra_config}"
            )
        parsed: Dict[str, SymbolConfig] = {}
        for symbol in expected_configs:
            try:
                parsed[symbol] = SymbolConfig.from_dict(raw[symbol])
            except Exception as exc:
                raise RuntimeError(
                    f"OPTIMIZATION_GENERATION_CELL_UNREADABLE:{symbol}:{type(exc).__name__}"
                ) from exc
        if universe is not None and getattr(universe, "explicit_contract", False):
            invalid = [
                symbol
                for symbol, config in parsed.items()
                if universe.active_timeframes(symbol)
                and not self._has_exact_native_cell_grid(symbol, config, universe)
            ]
            if invalid:
                raise RuntimeError(
                    "OPTIMIZATION_GENERATION_CELL_GRID_INVALID:" + ",".join(invalid)
                )
        if set(expected_configs) != set(parsed):
            raise RuntimeError("OPTIMIZATION_GENERATION_IN_MEMORY_ROSTER_MISMATCH")

    def _promote_staged_generation(
        self,
        stage_path: Path,
        stage_manifest_path: Path,
        manifest: Dict[str, Any],
        universe: Any = None,
    ) -> None:
        """Serialize final raw verification and publication for one ledger."""

        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(self._optimization_promotion_lock_path()):
            self._promote_staged_generation_locked(
                stage_path,
                stage_manifest_path,
                manifest,
                universe,
            )

    def _promote_staged_generation_locked(
        self,
        stage_path: Path,
        stage_manifest_path: Path,
        manifest: Dict[str, Any],
        universe: Any = None,
    ) -> None:
        """Atomically replace the live ledger once the entire generation passes."""
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        _stage, _stage_manifest, active_manifest = self._optimization_generation_paths()
        expected_sha = str(manifest.get("ledger_sha256") or "").lower()
        observed_sha = hashlib.sha256(Path(stage_path).read_bytes()).hexdigest()
        if not expected_sha or observed_sha != expected_sha:
            raise RuntimeError("OPTIMIZATION_GENERATION_STAGE_HASH_MISMATCH")
        expected_base_sha = str(manifest.get("base_ledger_sha256") or "").lower()
        expected_base_manifest_sha = str(
            manifest.get("base_active_manifest_sha256") or ""
        ).lower()
        if expected_base_manifest_sha and (
            len(expected_base_manifest_sha) != 64
            or any(
                character not in "0123456789abcdef"
                for character in expected_base_manifest_sha
            )
        ):
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_BASE_MANIFEST_HASH_INVALID"
            )
        current_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
        if hashlib.sha256(current_bytes).hexdigest() != expected_base_sha:
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_LIVE_LEDGER_CHANGED_DURING_RUN"
            )
        if expected_base_manifest_sha:
            _stage, _stage_manifest, active_manifest = (
                self._optimization_generation_paths()
            )
            active_bytes = (
                active_manifest.read_bytes() if active_manifest.exists() else b""
            )
            if hashlib.sha256(active_bytes).hexdigest() != expected_base_manifest_sha:
                raise RuntimeError(
                    "OPTIMIZATION_GENERATION_ACTIVE_MANIFEST_CHANGED_DURING_RUN"
                )
        self._verify_staged_authority_before_cas(
            stage_path,
            manifest,
            universe=universe,
        )
        # The raw verifier can be slow.  Re-read under the same interprocess
        # publication lock so a newer ledger can never be overwritten after
        # the first base check.
        current_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
        if hashlib.sha256(current_bytes).hexdigest() != expected_base_sha:
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_LIVE_LEDGER_CHANGED_DURING_VERIFY"
            )
        if expected_base_manifest_sha:
            active_bytes = (
                active_manifest.read_bytes() if active_manifest.exists() else b""
            )
            if hashlib.sha256(active_bytes).hexdigest() != expected_base_manifest_sha:
                raise RuntimeError(
                    "OPTIMIZATION_GENERATION_ACTIVE_MANIFEST_CHANGED_DURING_VERIFY"
                )
        final_stage_bytes = Path(stage_path).read_bytes()
        if hashlib.sha256(final_stage_bytes).hexdigest() != expected_sha:
            raise RuntimeError(
                "OPTIMIZATION_GENERATION_STAGE_CHANGED_DURING_VERIFY"
            )
        if self._requires_production_authority_runner():
            self._validated_sparse_current_ledger(
                final_stage_bytes,
                validation_time_utc=datetime.now(timezone.utc),
            )
        promoted_manifest = {
            **dict(manifest),
            "generation_status": "PROMOTED",
            "promoted_at": datetime.now(timezone.utc).isoformat(),
            "ledger_path": str(ledger_path),
        }
        journal_path = self._write_promotion_journal(
            base_ledger_sha256=expected_base_sha,
            target_ledger_sha256=expected_sha,
            stage_manifest_path=stage_manifest_path,
            promoted_manifest=promoted_manifest,
        )
        os.replace(stage_path, ledger_path)
        # A detached funded-live authorization is bound to the pre-CAS ledger
        # hash. Never allow refresh=False entry checks to reuse it after the
        # winner book changes.
        self._last_promotion_authorization = None
        self._atomic_write_json(active_manifest, promoted_manifest)
        self.ledger.load()
        self.symbol_configs = self.ledger.get_all_configs()
        self._prune_production_generation_storage_locked(promoted_manifest)
        Path(stage_manifest_path).unlink(missing_ok=True)
        journal_path.unlink()

    def _stage_and_promote_exact_production_generation(
        self,
        *,
        stage_path: Path,
        stage_manifest_path: Path,
        staged_bytes: bytes,
        manifest: Dict[str, Any],
        universe: Any,
    ) -> None:
        """Hold one lock from exact staging through publication completion."""

        from pm_storage import _exclusive_file_lock

        with _exclusive_file_lock(self._optimization_promotion_lock_path()):
            try:
                self._atomic_write_exact_bytes(stage_path, staged_bytes)
                observed = stage_path.read_bytes()
                expected_sha = str(manifest.get("ledger_sha256") or "").lower()
                if (
                    observed != staged_bytes
                    or hashlib.sha256(observed).hexdigest() != expected_sha
                ):
                    raise RuntimeError(
                        "PRODUCTION_AUTHORITY_EXACT_STAGE_REVERIFY_FAILED"
                    )
                self._atomic_write_json(
                    stage_manifest_path,
                    {**manifest, "ledger_staging_path": str(stage_path)},
                )
            except Exception as exc:
                raise RuntimeError(
                    "PRODUCTION_AUTHORITY_EXACT_STAGE_FAILED:"
                    f"{type(exc).__name__}:{exc}"
                ) from exc
            self._promote_staged_generation_locked(
                stage_path,
                stage_manifest_path,
                manifest,
                universe,
            )

    def _verify_staged_authority_before_cas(
        self,
        stage_path: Path,
        manifest: Mapping[str, Any],
        *,
        universe: Any,
    ) -> None:
        """Reverify the raw authority chain against staged bytes before CAS."""

        deployment_config = str(
            getattr(self.config, "_deployment_config_path", "") or ""
        ).strip()
        if not deployment_config:
            return
        split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        calendar_modes = {
            CALENDAR_SPLIT_MODE,
            "calendar",
            "calendar_lockbox",
            "lockbox",
            "three_way",
        }
        explicit_research = getattr(self.config, "research_mode", None) is True
        funded_execution = (
            getattr(self.config, "_funded_live_execution", None) is True
        )
        # A deployment-bound generation is production unless it is explicitly
        # configured as non-funded research.  The raw authority gate must not
        # depend on one exact split-mode spelling: stale aliases, legacy
        # deployment modes, and unknown production labels all fail closed.
        # Calendar modes retain their existing authority requirement even if a
        # caller accidentally carries a research flag into that context.
        if (
            explicit_research
            and not funded_execution
            and split_mode not in calendar_modes
        ):
            return
        if universe is None:
            raise RuntimeError("OPTIMIZATION_AUTHORITY_NATIVE_UNIVERSE_REQUIRED")
        try:
            from pm_optimization_authority import (
                ARTIFACTS,
                verify_live_readiness_authority_chain,
            )

            config_path = Path(deployment_config).resolve(strict=True)
            root = config_path.parent.resolve(strict=True)
            config_payload = json.loads(config_path.read_text(encoding="utf-8"))
            ledger_path = Path(stage_path).resolve(strict=True)
            ledger_path.relative_to(root)
            ledger_payload = json.loads(ledger_path.read_text(encoding="utf-8"))
            receipt = manifest.get("authority_chain_receipt")
            descriptors = manifest.get("authority_artifacts")
            if type(receipt) is not dict or type(descriptors) is not dict:
                raise ValueError("AUTHORITY_ASSEMBLY_MISSING")
            if set(descriptors) != set(ARTIFACTS):
                raise ValueError("AUTHORITY_ARTIFACT_ROSTER_INVALID")
            if str(receipt.get("generation_id") or "") != str(
                manifest.get("generation_id") or ""
            ):
                raise ValueError("AUTHORITY_GENERATION_MISMATCH")
            if str(receipt.get("ledger_sha256") or "").lower() != hashlib.sha256(
                ledger_path.read_bytes()
            ).hexdigest():
                raise ValueError("AUTHORITY_LEDGER_MISMATCH")
            requested = receipt.get("requested_symbols")
            canonical_requested = tuple(
                dict.fromkeys(
                    str(symbol).strip()
                    for symbol in self.symbols
                    if str(symbol).strip()
                )
            )
            if type(requested) is not list or tuple(requested) != canonical_requested:
                raise ValueError("AUTHORITY_REQUESTED_ROSTER_MISMATCH")
            index_paths: Dict[str, Path] = {}
            for name in ARTIFACTS:
                descriptor = descriptors.get(name)
                relative = (
                    descriptor.get("relative_path")
                    if type(descriptor) is dict
                    else None
                )
                if type(relative) is not str or not relative:
                    raise ValueError("AUTHORITY_ARTIFACT_PATH_INVALID")
                path = (root / relative).resolve(strict=True)
                path.relative_to(root)
                index_paths[name] = path
            probe_manifest = {
                **dict(manifest),
                "generation_status": "PROMOTED",
                "technical_failures": {},
            }
            funded = receipt.get("authorization_scope") == "FUNDED_LIVE"
            probe_path = root / (
                f".{ledger_path.name}.{manifest.get('generation_id', 'unknown')}."
                f"authority-probe-{os.getpid()}-{time.time_ns()}.json"
            )
            try:
                self._atomic_write_json(probe_path, probe_manifest)
                result = verify_live_readiness_authority_chain(
                    manifest=probe_manifest,
                    manifest_path=probe_path,
                    authority_index_paths=index_paths,
                    ledger=ledger_payload,
                    ledger_path=ledger_path,
                    config=self.config,
                    config_payload=config_payload,
                    config_path=config_path,
                    requested_symbols=tuple(requested),
                    native_universe=universe,
                    validation_time_utc=datetime.now(timezone.utc),
                    require_funded_authorization=funded,
                )
            finally:
                probe_path.unlink(missing_ok=True)
            expected = (
                "FUNDED_AUTHORIZED" if funded else "DEMO_PROBATION_AUTHORIZED"
            )
            if (
                result.get("status") != expected
                or result.get("errors") != []
                or set(result.get("raw_reverification", {}).values()) != {True}
            ):
                detail = ",".join(str(value) for value in result.get("errors", [])[:8])
                raise ValueError(f"AUTHORITY_RAW_REVERIFY_BLOCKED:{detail}")
        except Exception as exc:
            if isinstance(exc, RuntimeError) and str(exc).startswith(
                "OPTIMIZATION_AUTHORITY_"
            ):
                raise
            raise RuntimeError(
                "OPTIMIZATION_AUTHORITY_PRE_CAS_BLOCKED:"
                f"{type(exc).__name__}:{exc}"
            ) from exc

    def _requires_production_authority_runner(self) -> bool:
        """Classify the same production surface as the final pre-CAS gate."""

        deployment_config = str(
            getattr(self.config, "_deployment_config_path", "") or ""
        ).strip()
        if not deployment_config:
            return False
        split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        calendar_modes = {
            CALENDAR_SPLIT_MODE,
            "calendar",
            "calendar_lockbox",
            "lockbox",
            "three_way",
        }
        if split_mode in calendar_modes:
            return True
        explicit_research = getattr(self.config, "research_mode", None) is True
        funded_execution = (
            getattr(self.config, "_funded_live_execution", None) is True
        )
        return not (explicit_research and not funded_execution)

    def _validated_sparse_current_ledger(
        self,
        raw: bytes,
        *,
        validation_time_utc: Optional[datetime] = None,
    ) -> Tuple[Dict[str, SymbolConfig], int]:
        """Validate the sparse physical winners; coverage lives in raw Book evidence."""

        try:
            payload = json.loads(raw.decode("ascii"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "PRODUCTION_AUTHORITY_LEDGER_JSON_INVALID"
            ) from exc
        if type(payload) is not dict or not payload or len(payload) > len(self.symbols):
            raise RuntimeError("PRODUCTION_AUTHORITY_LEDGER_ROSTER_INVALID")
        configured = {str(symbol).strip() for symbol in self.symbols}
        if set(payload) - configured:
            raise RuntimeError("PRODUCTION_AUTHORITY_LEDGER_SYMBOL_OUTSIDE_UNIVERSE")
        probe = validation_time_utc or datetime.now(timezone.utc)
        if (
            not isinstance(probe, datetime)
            or probe.tzinfo is None
            or probe.utcoffset() != timedelta(0)
        ):
            raise RuntimeError("PRODUCTION_AUTHORITY_VALIDATION_TIME_NOT_UTC")
        probe = probe.astimezone(timezone.utc)
        parsed: Dict[str, SymbolConfig] = {}
        cell_count = 0
        generation_optimized_at: Optional[datetime] = None
        generation_valid_until: Optional[datetime] = None
        for symbol, body in payload.items():
            try:
                config = SymbolConfig.from_dict(body)
            except Exception as exc:
                raise RuntimeError(
                    "PRODUCTION_AUTHORITY_LEDGER_SYMBOL_INVALID:"
                    f"{symbol}:{type(exc).__name__}"
                ) from exc
            if str(config.symbol).strip() != symbol:
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_SYMBOL_IDENTITY_MISMATCH:{symbol}"
                )
            optimized_at = config.optimized_at
            valid_until = config.valid_until
            if (
                not isinstance(optimized_at, datetime)
                or optimized_at.tzinfo is None
                or optimized_at.utcoffset() != timedelta(0)
            ):
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_OPTIMIZED_AT_NOT_UTC:{symbol}"
                )
            if (
                not isinstance(valid_until, datetime)
                or valid_until.tzinfo is None
                or valid_until.utcoffset() != timedelta(0)
            ):
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_VALID_UNTIL_NOT_UTC:{symbol}"
                )
            optimized_at = optimized_at.astimezone(timezone.utc)
            valid_until = valid_until.astimezone(timezone.utc)
            try:
                expected_valid_until = self.config.get_next_retrain_at_utc(
                    optimized_at
                )
            except Exception as exc:
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_RETRAIN_SCHEDULE_INVALID:{symbol}"
                ) from exc
            if valid_until != expected_valid_until:
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_VALID_UNTIL_MISMATCH:{symbol}"
                )
            if valid_until <= probe:
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_VALID_UNTIL_EXPIRED:{symbol}"
                )
            if generation_optimized_at is None:
                generation_optimized_at = optimized_at
                generation_valid_until = valid_until
            elif (
                optimized_at != generation_optimized_at
                or valid_until != generation_valid_until
            ):
                raise RuntimeError(
                    "PRODUCTION_AUTHORITY_LEDGER_GENERATION_DEADLINE_MISMATCH"
                )
            symbol_cells = 0
            for regimes in (config.regime_configs or {}).values():
                for candidate in (regimes or {}).values():
                    role = str(
                        (candidate.artifact_meta or {}).get("book_role", "") or ""
                    ).strip().upper()
                    status = str(candidate.promotion_status or "").strip().upper()
                    if (
                        role != "PROCESS_VALIDATED_CURRENT_REFIT"
                        or status != "PROCESS_VALIDATED_CURRENT_REFIT"
                    ):
                        raise RuntimeError(
                            "PRODUCTION_AUTHORITY_LEDGER_CELL_ROLE_INVALID:"
                            f"{symbol}"
                        )
                    if candidate.trained_at != optimized_at:
                        raise RuntimeError(
                            "PRODUCTION_AUTHORITY_LEDGER_CELL_TRAINED_AT_MISMATCH:"
                            f"{symbol}"
                        )
                    if candidate.valid_until != valid_until:
                        raise RuntimeError(
                            "PRODUCTION_AUTHORITY_LEDGER_CELL_VALID_UNTIL_MISMATCH:"
                            f"{symbol}"
                        )
                    symbol_cells += 1
            if symbol_cells <= 0:
                raise RuntimeError(
                    f"PRODUCTION_AUTHORITY_LEDGER_SYMBOL_EMPTY:{symbol}"
                )
            parsed[symbol] = config
            cell_count += symbol_cells
        return parsed, cell_count

    def _run_production_authority_generation(
        self,
        request: OptimizationGenerationRequest,
    ) -> OptimizationGenerationOutcome:
        """Run genuine anchors/current/replay/Capital once, then atomically CAS."""

        from pm_production_capital_bootstrap import DEMO_PROBATION_SCOPE
        from pm_production_optimization_runner import (
            ProductionOptimizationNotPromotable,
            ProductionOptimizationRequest,
            discard_unpublished_production_result,
            run_production_optimization,
        )

        request_kind = str(request.kind or "").strip().upper()
        started_at = datetime.now(timezone.utc).isoformat()
        total_start = time.time()
        # A retry in the same process must resolve the prior publication before
        # it can derive a new base hash or write another promotion journal.
        self._reconcile_interrupted_promotion(
            lock_timeout_seconds=0.0,
        )
        configured_symbols = tuple(str(symbol) for symbol in self.symbols)
        configured_exact = {str(symbol).strip() for symbol in configured_symbols}
        for raw_symbol in request.target_symbols:
            exact_symbol = str(raw_symbol or "").strip()
            if exact_symbol not in configured_exact:
                raise ValueError(
                    f"OPTIMIZATION_GENERATION_TARGET_UNKNOWN:{exact_symbol}"
                )
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        base_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
        base_sha = hashlib.sha256(base_bytes).hexdigest()
        config_path = Path(
            str(getattr(self.config, "_deployment_config_path", "") or "")
        ).resolve(strict=True)
        try:
            config_payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            raise RuntimeError(
                "PRODUCTION_ACTIVE_CONFIG_READ_FAILED:"
                f"{type(exc).__name__}:{exc}"
            ) from exc
        universe = self._native_universe_for_optimization()
        supervised_generation_id = request.supervised_generation_id
        if supervised_generation_id is not None:
            if (
                type(supervised_generation_id) is not str
                or len(supervised_generation_id) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in supervised_generation_id
                )
            ):
                raise ValueError(
                    "OPTIMIZATION_SUPERVISED_GENERATION_ID_INVALID"
                )
            generation_id = supervised_generation_id
        else:
            generation_id = _stable_hash(
                {
                    "schema": "fxpm.optimization-generation.v2",
                    "runner": "fxpm.production-optimization-runner.v1",
                    "started_at": started_at,
                    "time_ns": time.time_ns(),
                    "pid": os.getpid(),
                    "base_ledger_sha256": base_sha,
                    "activation_id": getattr(universe, "activation_id", None),
                    "activation_manifest_sha256": getattr(
                        universe, "activation_manifest_sha256", None
                    ),
                    "kind": request_kind,
                    "requested_symbols": list(configured_symbols),
                    "overwrite": bool(request.overwrite_targets),
                }
            )
        stage_path, stage_manifest_path, active_manifest_path = (
            self._optimization_generation_paths()
        )
        failure_manifest_path = self._optimization_failure_manifest_path()
        previous_active_manifest: Dict[str, Any] = {}
        previous_active_manifest_raw = b""
        if active_manifest_path.exists():
            try:
                if active_manifest_path.stat().st_size > 64 * 1024 * 1024:
                    raise ValueError("ACTIVE_MANIFEST_TOO_LARGE")
                previous_active_manifest_raw = active_manifest_path.read_bytes()
                previous_active_manifest = json.loads(
                    previous_active_manifest_raw.decode("utf-8")
                )
                if type(previous_active_manifest) is not dict:
                    raise ValueError("ACTIVE_MANIFEST_NOT_OBJECT")
            except Exception as exc:
                raise RuntimeError(
                    "PRODUCTION_ACTIVE_MANIFEST_READ_FAILED:"
                    f"{type(exc).__name__}:{exc}"
                ) from exc
        base_active_manifest_sha = hashlib.sha256(
            previous_active_manifest_raw
        ).hexdigest()
        previous_retention = self._production_generation_retention_descriptor(
            previous_active_manifest,
            deployment_root=config_path.parent,
        )
        if previous_active_manifest and any(
            key in previous_active_manifest
            for key in (
                "production_authority_run_root",
                "authority_assembly_path",
                "production_staged_ledger_source_path",
            )
        ) and previous_retention is None:
            raise RuntimeError("PRODUCTION_ACTIVE_RETENTION_DESCRIPTOR_INVALID")
        obsolete_retention = None
        if "previous_production_generation" in previous_active_manifest:
            obsolete_value = previous_active_manifest.get(
                "previous_production_generation"
            )
            obsolete_retention = (
                self._production_generation_retention_descriptor(
                    obsolete_value,
                    deployment_root=config_path.parent,
                )
            )
            if obsolete_retention is None:
                raise RuntimeError(
                    "PRODUCTION_OBSOLETE_RETENTION_DESCRIPTOR_INVALID"
                )

        def _outcome(
            status: str,
            *,
            results: Optional[Dict[str, PipelineResult]] = None,
            failures: Optional[Dict[str, str]] = None,
            promoted: bool = False,
            ledger_changed: bool = False,
            promoted_sha: str = "",
            manifest_path: Optional[Path] = None,
            authority_fields: Optional[Mapping[str, Any]] = None,
            replay_evidence: Optional[Mapping[str, Any]] = None,
            non_promotable: Optional[Mapping[str, Any]] = None,
        ) -> OptimizationGenerationOutcome:
            fields = dict(authority_fields or {})
            value = OptimizationGenerationOutcome(
                generation_id=generation_id,
                status=status,
                requested_symbols=configured_symbols,
                effective_symbols=configured_symbols,
                retained_symbols=(),
                excluded_symbols=(),
                results=dict(results or {}),
                technical_failures=dict(failures or {}),
                promoted=promoted,
                ledger_changed=ledger_changed,
                base_ledger_sha256=base_sha,
                promoted_ledger_sha256=promoted_sha,
                manifest_path=str(
                    manifest_path
                    if manifest_path is not None
                    else active_manifest_path if promoted else stage_manifest_path
                ),
                replay_evidence=dict(replay_evidence or {}),
                non_promotable=dict(non_promotable or {}),
                authority_chain_receipt=dict(
                    fields.get("authority_chain_receipt", {}) or {}
                ),
                authority_artifacts=dict(fields.get("authority_artifacts", {}) or {}),
                authority_assembly_path=str(
                    fields.get("authority_assembly_path", "") or ""
                ),
            )
            self.last_optimization_generation = value
            return value

        if request_kind == "SCHEDULED_RETRAIN" and request.require_fresh_native_activation:
            current_activation = str(getattr(universe, "activation_id", "") or "")
            previous_activation = ""
            if previous_active_manifest:
                previous_activation = str(
                    (
                        previous_active_manifest.get("authority_chain_receipt", {})
                        or {}
                    ).get("native_activation_id", "")
                    or ""
                )
            if not current_activation or current_activation == previous_activation:
                failure = {
                    "__GENERATION__": "BLOCKED_STALE_NATIVE_ACTIVATION"
                }
                manifest = {
                    "schema_version": "fxpm.optimization-generation.v2",
                    "generation_id": generation_id,
                    "generation_status": "BLOCKED_STALE_SOURCE",
                    "generation_kind": request_kind,
                    "base_ledger_sha256": base_sha,
                    "requested_symbols": list(configured_symbols),
                    "technical_failures": failure,
                }
                from pm_storage import _exclusive_file_lock

                with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                    current = ledger_path.read_bytes() if ledger_path.exists() else b""
                    if hashlib.sha256(current).hexdigest() != base_sha:
                        raise RuntimeError(
                            "PRODUCTION_STALE_SOURCE_FOUND_EXTERNAL_LEDGER_CHANGE"
                        )
                    self._atomic_write_json(failure_manifest_path, manifest)
                return _outcome(
                    "BLOCKED_STALE_SOURCE",
                    failures=failure,
                    manifest_path=failure_manifest_path,
                )

        try:
            production = run_production_optimization(
                ProductionOptimizationRequest(
                    active_config_path=config_path,
                    config=self.config,
                    config_payload=config_payload,
                    requested_symbols=configured_symbols,
                    native_universe=universe,
                    generation_id=generation_id,
                    authorization_scope=DEMO_PROBATION_SCOPE,
                )
            )
        except ProductionOptimizationNotPromotable as exc:
            terminal = {
                "code": str(exc.code),
                "details": [str(value)[:512] for value in exc.details[:8]],
                "preserved_last_good_ledger": True,
                "confers_publication_or_trading_authority": False,
            }
            manifest = {
                "schema_version": "fxpm.optimization-generation.v2",
                "generation_id": generation_id,
                "generation_status": "NOT_PROMOTABLE",
                "generation_kind": request_kind,
                "base_ledger_sha256": base_sha,
                "base_active_manifest_sha256": base_active_manifest_sha,
                "requested_symbols": list(configured_symbols),
                "technical_failures": {},
                "non_promotable": terminal,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            from pm_storage import _exclusive_file_lock

            with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                current = ledger_path.read_bytes() if ledger_path.exists() else b""
                current_active = (
                    active_manifest_path.read_bytes()
                    if active_manifest_path.exists()
                    else b""
                )
                if (
                    hashlib.sha256(current).hexdigest() != base_sha
                    or hashlib.sha256(current_active).hexdigest()
                    != base_active_manifest_sha
                ):
                    raise RuntimeError(
                        "PRODUCTION_NOT_PROMOTABLE_FOUND_EXTERNAL_STATE_CHANGE"
                    ) from exc
                self._atomic_write_json(failure_manifest_path, manifest)
            return _outcome(
                "NOT_PROMOTABLE",
                manifest_path=failure_manifest_path,
                non_promotable=terminal,
            )
        except Exception as exc:
            detail = f"PRODUCTION_AUTHORITY_RUN_FAILED:{type(exc).__name__}:{exc}"
            failures = {"__GENERATION__": detail}
            manifest = {
                "schema_version": "fxpm.optimization-generation.v2",
                "generation_id": generation_id,
                "generation_status": "FAILED_TECHNICAL",
                "generation_kind": request_kind,
                "base_ledger_sha256": base_sha,
                "requested_symbols": list(configured_symbols),
                "technical_failures": failures,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            from pm_storage import _exclusive_file_lock

            with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                current = ledger_path.read_bytes() if ledger_path.exists() else b""
                if hashlib.sha256(current).hexdigest() != base_sha:
                    raise RuntimeError(
                        "PRODUCTION_AUTHORITY_FAILURE_FOUND_EXTERNAL_LEDGER_CHANGE"
                    ) from exc
                self._atomic_write_json(failure_manifest_path, manifest)
            return _outcome(
                "FAILED_TECHNICAL",
                failures=failures,
                manifest_path=failure_manifest_path,
            )

        try:
            if production.generation_id != generation_id:
                raise RuntimeError("PRODUCTION_AUTHORITY_GENERATION_ID_MISMATCH")
            staged_bytes = production.staged_ledger_bytes
            if type(staged_bytes) is not bytes or not staged_bytes:
                raise RuntimeError("PRODUCTION_AUTHORITY_STAGED_LEDGER_BYTES_INVALID")
            staged_sha = hashlib.sha256(staged_bytes).hexdigest()
            if staged_sha != production.staged_ledger_content_sha256:
                raise RuntimeError("PRODUCTION_AUTHORITY_STAGED_LEDGER_HASH_MISMATCH")
            parsed, winner_cell_count = self._validated_sparse_current_ledger(
                staged_bytes
            )
            result_rows = {
                symbol: PipelineResult(
                    symbol=symbol,
                    success=True,
                    config=config,
                    duration_seconds=max(0.0, time.time() - total_start),
                    timeframes_tested=len(config.regime_configs or {}),
                    regime_winners=sum(
                        len(regimes or {})
                        for regimes in (config.regime_configs or {}).values()
                    ),
                    data_window_meta={
                        "evaluation_scope": "COMPLETE_100x6x4_RAW_BOOK",
                        "physical_current_winner": True,
                    },
                )
                for symbol, config in parsed.items()
            }
            authority_fields = production.manifest_fields()
            ledger_status = {
                "schema_version": "fxpm.production-ledger-status.v2",
                "complete_current_contract_universe": True,
                "coverage_authority": "RAW_REVERIFIED_COMPLETE_100x6x4_BOOK",
                "configured_symbol_count": len(configured_symbols),
                "configured_cell_count": len(configured_symbols) * 6 * 4,
                "physical_winner_symbol_count": len(parsed),
                "physical_winner_cell_count": winner_cell_count,
                "sparse_physical_winner_book": True,
            }
            replay_evidence = {
                "ok": True,
                "mode": "SYNCHRONIZED_AUTHORITY_REPLAY_AND_CAPITAL_V8",
                "capital_decision_sha256": production.capital_decision_sha256,
                "selected_policy_point_id": production.selected_policy_point_id,
            }
            manifest: Dict[str, Any] = {
                "schema_version": "fxpm.optimization-generation.v2",
                "generation_id": generation_id,
                "generation_status": "COMPLETE",
                "generation_kind": request_kind,
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "base_ledger_sha256": base_sha,
                "base_active_manifest_sha256": base_active_manifest_sha,
                "ledger_sha256": staged_sha,
                "configured_symbols": list(configured_symbols),
                "requested_symbols": list(configured_symbols),
                "effective_symbols": list(configured_symbols),
                "completed_symbols": list(configured_symbols),
                "physical_winner_symbols": sorted(parsed),
                "activation_id": getattr(universe, "activation_id", None),
                "activation_manifest_sha256": getattr(
                    universe, "activation_manifest_sha256", None
                ),
                "technical_failures": {},
                "ledger_status": ledger_status,
                "replay_evidence": replay_evidence,
                "physical_ledger_sha256": production.physical_ledger_sha256,
                "production_authority_run_root": str(production.run_root),
                "production_staged_ledger_source_path": str(
                    production.staged_ledger_source_path
                ),
                **authority_fields,
            }
            previous_retention_for_manifest = None
            if previous_retention is not None:
                rollback_snapshot = (
                    Path(production.run_root)
                    / "rollback"
                    / f"manifest-{base_active_manifest_sha}.json"
                )
                self._atomic_write_exact_bytes(
                    rollback_snapshot,
                    previous_active_manifest_raw,
                )
                if (
                    hashlib.sha256(rollback_snapshot.read_bytes()).hexdigest()
                    != base_active_manifest_sha
                ):
                    raise RuntimeError(
                        "PRODUCTION_PREVIOUS_MANIFEST_SNAPSHOT_MISMATCH"
                    )
                previous_retention_for_manifest = {
                    **dict(previous_retention),
                    "previous_active_manifest_snapshot_path": str(
                        rollback_snapshot.resolve(strict=True)
                    ),
                    "previous_active_manifest_snapshot_sha256": (
                        base_active_manifest_sha
                    ),
                }
                manifest["previous_production_generation"] = dict(
                    previous_retention_for_manifest
                )
            if obsolete_retention is not None:
                manifest["obsolete_production_generation"] = dict(
                    obsolete_retention
                )
            manifest["production_retention_policy"] = {
                "schema_version": "fxpm.production-retention-policy.v1",
                "maximum_complete_generations": 2,
                "keep_current_and_previous": True,
            }
            current_retention = (
                self._production_generation_retention_descriptor(
                    manifest,
                    deployment_root=config_path.parent,
                )
            )
            if current_retention is None:
                raise RuntimeError(
                    "PRODUCTION_CURRENT_RETENTION_DESCRIPTOR_INVALID"
                )
        except Exception as exc:
            cleanup_status = "NOT_ATTEMPTED"
            try:
                cleanup_status = (
                    "DISCARDED"
                    if discard_unpublished_production_result(production)
                    else "ALREADY_ABSENT"
                )
            except Exception as cleanup_exc:
                cleanup_status = (
                    "FAILED:"
                    f"{type(cleanup_exc).__name__}:{cleanup_exc}"
                )
            failures = {
                "__POST_RUN__": "PRODUCTION_AUTHORITY_POST_RUN_FAILED:"
                f"{type(exc).__name__}:{exc}"
            }
            failed_manifest = {
                "schema_version": "fxpm.optimization-generation.v2",
                "generation_id": generation_id,
                "generation_status": "FAILED_POST_RUN_TECHNICAL",
                "generation_kind": request_kind,
                "base_ledger_sha256": base_sha,
                "requested_symbols": list(configured_symbols),
                "technical_failures": failures,
                "unpublished_artifact_cleanup": cleanup_status,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            from pm_storage import _exclusive_file_lock

            with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                live = ledger_path.read_bytes() if ledger_path.exists() else b""
                live_sha = hashlib.sha256(live).hexdigest()
                if live_sha != base_sha:
                    failed_manifest["generation_status"] = (
                        "FAILED_POST_RUN_SUPERSEDED"
                    )
                    failed_manifest["superseded_by_ledger_sha256"] = live_sha
                self._atomic_write_json(failure_manifest_path, failed_manifest)
            return _outcome(
                failed_manifest["generation_status"],
                failures=failures,
                manifest_path=failure_manifest_path,
            )

        try:
            self._stage_and_promote_exact_production_generation(
                stage_path=stage_path,
                stage_manifest_path=stage_manifest_path,
                staged_bytes=staged_bytes,
                manifest=manifest,
                universe=universe,
            )
        except Exception as exc:
            from pm_storage import _exclusive_file_lock

            with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                journal_reference = self._promotion_journal_reference_locked(
                    generation_id=generation_id,
                    ledger_sha256=staged_sha,
                    run_root=production.run_root,
                    authority_assembly_path=Path(
                        authority_fields["authority_assembly_path"]
                    ),
                    staged_ledger_source_path=(
                        production.staged_ledger_source_path
                    ),
                )
                journal_authorized = bool(
                    journal_reference == "EXACT"
                    and self._promotion_journal_authorizes(
                        base_ledger_sha256=base_sha,
                        target_ledger_sha256=staged_sha,
                        generation_id=generation_id,
                    )
                )
                if journal_authorized:
                    try:
                        self._reconcile_interrupted_promotion_locked()
                    except Exception:
                        pass
                live = ledger_path.read_bytes() if ledger_path.exists() else b""
                live_sha = hashlib.sha256(live).hexdigest()
                active_reference = (
                    self._active_production_generation_reference_locked(
                        generation_id=generation_id,
                        ledger_sha256=staged_sha,
                        run_root=production.run_root,
                        authority_assembly_path=Path(
                            authority_fields["authority_assembly_path"]
                        ),
                        staged_ledger_source_path=(
                            production.staged_ledger_source_path
                        ),
                    )
                )
                cas_applied = bool(
                    live_sha == staged_sha
                    and (
                        journal_authorized
                        or active_reference == "ACTIVE"
                    )
                )
                generation_referenced = bool(
                    journal_reference in {"EXACT", "AMBIGUOUS"}
                    or active_reference in {"ACTIVE", "PREVIOUS"}
                )
                if active_reference in {"ACTIVE", "PREVIOUS"}:
                    self.ledger.load()
                    self.symbol_configs = self.ledger.get_all_configs()
                    return _outcome(
                        (
                            "PROMOTED"
                            if active_reference == "ACTIVE"
                            else "PROMOTED_SUPERSEDED"
                        ),
                        results=result_rows,
                        promoted=True,
                        ledger_changed=staged_sha != base_sha,
                        promoted_sha=staged_sha,
                        authority_fields=authority_fields,
                        replay_evidence=replay_evidence,
                    )
                if not generation_referenced:
                    self._discard_fixed_stage_for_generation_locked(
                        generation_id=generation_id,
                        ledger_sha256=staged_sha,
                    )
            unpublished_cleanup = "NOT_SAFE_TO_DISCARD"
            if not generation_referenced:
                try:
                    unpublished_cleanup = (
                        "DISCARDED"
                        if discard_unpublished_production_result(production)
                        else "ALREADY_ABSENT"
                    )
                except Exception as cleanup_exc:
                    unpublished_cleanup = (
                        "FAILED:"
                        f"{type(cleanup_exc).__name__}:{cleanup_exc}"
                    )
            if cas_applied:
                # The ledger is already the durable truth even when active-
                # manifest recovery remains pending.  Runtime state must never
                # continue trading the superseded in-memory book.
                self.ledger.load()
                self.symbol_configs = self.ledger.get_all_configs()
            pre_cas_stage_failure = str(exc).startswith(
                "PRODUCTION_AUTHORITY_EXACT_STAGE_FAILED:"
            )
            if pre_cas_stage_failure:
                failure_key = "__POST_RUN__"
                failure_detail = (
                    "PRODUCTION_AUTHORITY_POST_RUN_FAILED:"
                    f"{type(exc).__name__}:{exc}"
                )
            else:
                failure_key = "__PROMOTION__"
                failure_detail = (
                    "PRODUCTION_PROMOTION_FAILED:"
                    f"{type(exc).__name__}:{exc}"
                )
            failures = {failure_key: failure_detail}
            failed = {
                **manifest,
                "generation_status": (
                    "PROMOTED_RECOVERY_REQUIRED"
                    if cas_applied
                    else "FAILED_POST_RUN_TECHNICAL"
                    if pre_cas_stage_failure
                    else "FAILED_PROMOTION_TECHNICAL"
                ),
                "technical_failures": failures,
                "promotion_failure_evidence": {
                    "cas_applied": cas_applied,
                    "observed_live_ledger_sha256": live_sha,
                    "target_ledger_sha256": staged_sha,
                    "unpublished_artifact_cleanup": unpublished_cleanup,
                },
            }
            with _exclusive_file_lock(self._optimization_promotion_lock_path()):
                self._atomic_write_json(failure_manifest_path, failed)
            return _outcome(
                failed["generation_status"],
                results=result_rows,
                failures=failures,
                promoted=cas_applied,
                ledger_changed=cas_applied and staged_sha != base_sha,
                promoted_sha=staged_sha if cas_applied else "",
                manifest_path=failure_manifest_path,
                authority_fields=authority_fields,
                replay_evidence=replay_evidence,
            )
        return _outcome(
            "PROMOTED",
            results=result_rows,
            promoted=True,
            ledger_changed=staged_sha != base_sha,
            promoted_sha=staged_sha,
            authority_fields=authority_fields,
            replay_evidence=replay_evidence,
        )

    def _run_governed_symbol_generation(
        self,
        request: OptimizationGenerationRequest,
    ) -> OptimizationGenerationOutcome:
        """Run one receipt-verified, full-roster, atomic ledger generation."""
        results = {}

        request_kind = str(request.kind or "").strip().upper()
        allowed_kinds = {"INITIAL", "MANUAL_RETRAIN", "SCHEDULED_RETRAIN"}
        if request_kind not in allowed_kinds:
            raise ValueError(
                f"OPTIMIZATION_GENERATION_KIND_UNKNOWN:{request_kind or 'EMPTY'}"
            )
        production_native_context = bool(
            str(getattr(self.config, "_deployment_config_path", "") or "").strip()
        ) and str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower() == CALENDAR_SPLIT_MODE
        if request_kind == "SCHEDULED_RETRAIN" and request.overwrite_targets:
            raise ValueError("SCHEDULED_RETRAIN_OVERWRITE_FORBIDDEN")
        if (
            request_kind != "SCHEDULED_RETRAIN"
            and request.require_fresh_native_activation
        ):
            raise ValueError(
                "FRESH_NATIVE_ACTIVATION_POLICY_ONLY_APPLIES_TO_SCHEDULED_RETRAIN"
            )
        if (
            request_kind == "SCHEDULED_RETRAIN"
            and request.require_fresh_native_activation
            and not production_native_context
        ):
            raise ValueError(
                "FRESH_NATIVE_ACTIVATION_POLICY_REQUIRES_PRODUCTION_CALENDAR_CONTEXT"
            )
        if self._requires_production_authority_runner():
            return self._run_production_authority_generation(request)
        require_fresh_native_activation = bool(
            request_kind == "SCHEDULED_RETRAIN" and production_native_context
        )

        configured_by_canonical = {
            str(symbol).strip().upper(): str(symbol)
            for symbol in self.symbols
        }
        requested: List[str] = []
        for raw_symbol in request.target_symbols:
            canonical = str(raw_symbol or "").strip().upper()
            if canonical not in configured_by_canonical:
                raise ValueError(f"OPTIMIZATION_GENERATION_TARGET_UNKNOWN:{canonical}")
            configured = configured_by_canonical[canonical]
            if configured not in requested:
                requested.append(configured)

        total_start = time.time()
        
        # Freeze the authenticated data-state universe before model search.
        # Only evidence candidates enter optimization. Source-unavailable cells
        # remain outside the winner ledger, and a later transition to candidate
        # state is forced through evaluation.
        universe = self._native_universe_for_optimization()
        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        base_ledger_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
        base_ledger_sha256 = hashlib.sha256(base_ledger_bytes).hexdigest()
        staged_configs: Dict[str, SymbolConfig] = {}
        for configured_symbol in self.symbols:
            canonical = str(configured_symbol).upper()
            existing = (
                self.symbol_configs.get(configured_symbol)
                or self.symbol_configs.get(canonical)
            )
            if existing is not None:
                staged_configs[configured_symbol] = SymbolConfig.from_dict(
                    existing.to_dict()
                )
        generation_started_at = datetime.now(timezone.utc).isoformat()
        generation_id = _stable_hash({
            "schema": "fxpm.optimization-generation.v2",
            "started_at": generation_started_at,
            "pid": os.getpid(),
            "base_ledger_sha256": base_ledger_sha256,
            "activation_id": getattr(universe, "activation_id", None),
            "activation_manifest_sha256": getattr(
                universe, "activation_manifest_sha256", None
            ),
            "kind": request_kind,
            "requested_symbols": requested,
            "overwrite": bool(request.overwrite_targets),
        })
        generation_manifest: Dict[str, Any] = {
            "schema_version": "fxpm.optimization-generation.v2",
            "generation_id": generation_id,
            "generation_status": "RUNNING",
            "started_at": generation_started_at,
            "base_ledger_sha256": base_ledger_sha256,
            "configured_symbols": [str(symbol) for symbol in self.symbols],
            "generation_kind": request_kind,
            "requested_symbols": list(requested),
            "activation_id": getattr(universe, "activation_id", None),
            "activation_manifest_sha256": getattr(
                universe, "activation_manifest_sha256", None
            ),
            "overwrite": bool(request.overwrite_targets),
            "fresh_native_activation_required": require_fresh_native_activation,
            "completed_symbols": [],
            "technical_failures": {},
        }
        required_winner_symbols = list(self.symbols)
        if universe is not None and getattr(universe, "explicit_contract", False):
            required_timeframes = tuple(
                str(value or "").strip().upper()
                for value in (getattr(self.config, "timeframes", []) or [])
                if str(value or "").strip()
            )
            receipt_cells = {
                str(symbol).upper(): {
                    timeframe: str(
                        universe.cell_status(str(symbol).upper(), timeframe)
                    ).lower()
                    for timeframe in required_timeframes
                }
                for symbol in self.symbols
            }
            legacy_trading_verdicts = any(
                status == "excluded_no_trade"
                for cells in receipt_cells.values()
                for status in cells.values()
            )
            data_state_receipt: Dict[str, Any] = {
                "schema_version": "fxpm.native-data-state-receipt.v1",
                "activation_id": getattr(universe, "activation_id", None),
                "activation_manifest_sha256": getattr(
                    universe, "activation_manifest_sha256", None
                ),
                "acquisition_emits_trading_verdicts": legacy_trading_verdicts,
                "statistical_sufficiency_authority": (
                    "authenticated_horizon_ess_trade_power_and_multiplicity_v1"
                ),
                "cells": receipt_cells,
            }
            data_state_receipt["receipt_sha256"] = _stable_hash(
                data_state_receipt
            )
            generation_manifest["native_data_state_receipt"] = data_state_receipt
            required_winner_symbols = [
                str(symbol)
                for symbol in self.symbols
                if universe.active_timeframes(str(symbol).upper())
                or any(
                    str(universe.cell_status(str(symbol).upper(), timeframe)).lower()
                    == "excluded_no_trade"
                    for timeframe in required_timeframes
                )
            ]
        generation_manifest["required_winner_symbols"] = list(
            required_winner_symbols
        )
        # seq987: staged roster == required winner-symbol set.  Formerly
        # tradeable symbols that are now data-state-only are dropped before
        # promotion, never carried forward and re-stamped current.
        required_upper = {str(s).upper() for s in required_winner_symbols}
        dropped_stale = sorted(
            key for key in staged_configs
            if str(key).upper() not in required_upper
        )
        for key in dropped_stale:
            staged_configs.pop(key, None)
        if dropped_stale:
            generation_manifest["dropped_data_state_symbols"] = dropped_stale
            logger.warning(
                f"Generation drops {len(dropped_stale)} data-state-only "
                f"symbols from staged roster: {dropped_stale}"
            )

        effective_symbols = list(requested)
        if universe is not None and universe.explicit_contract:
            for symbol in self.symbols:
                canonical = str(symbol).upper()
                if not universe.active_timeframes(canonical):
                    continue
                existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(canonical)
                if existing is not None and not self._has_exact_native_cell_grid(
                    canonical, existing, universe
                ) and symbol not in effective_symbols:
                    effective_symbols.append(symbol)
        regime_generation_rebuilt = self._ensure_active_regime_provenance(
            universe,
            effective_symbols,
        )
        generation_manifest["regime_generation_preflight"] = {
            "status": (
                "REBUILT_AND_VALIDATED"
                if regime_generation_rebuilt
                else "VALIDATED_EXISTING"
            ),
            "activation_id": getattr(universe, "activation_id", None),
            "activation_manifest_sha256": getattr(
                universe, "activation_manifest_sha256", None
            ),
            "params_path": str(
                self._resolve_deployment_path(
                    getattr(
                        self.config,
                        "regime_params_file",
                        "regime_params.json",
                    )
                    or "regime_params.json"
                )
            ),
        }
        generation_manifest["effective_symbols"] = list(effective_symbols)

        def _outcome(
            status: str,
            *,
            promoted: bool = False,
            ledger_changed: bool = False,
            promoted_sha: str = "",
            replay_evidence: Optional[Dict[str, Any]] = None,
            manifest_path_override: Optional[Path] = None,
        ) -> OptimizationGenerationOutcome:
            _stage, stage_manifest, active_manifest = self._optimization_generation_paths()
            manifest_path = (
                Path(manifest_path_override)
                if manifest_path_override is not None
                else active_manifest if promoted else stage_manifest
            )
            outcome = OptimizationGenerationOutcome(
                generation_id=generation_id,
                status=str(status),
                requested_symbols=tuple(requested),
                effective_symbols=tuple(effective_symbols),
                retained_symbols=tuple(
                    symbol for symbol in self.symbols if symbol not in effective_symbols
                ),
                excluded_symbols=tuple(excluded_now),
                results=dict(results),
                technical_failures=dict(
                    generation_manifest.get("technical_failures", {}) or {}
                ),
                promoted=bool(promoted),
                ledger_changed=bool(ledger_changed),
                base_ledger_sha256=base_ledger_sha256,
                promoted_ledger_sha256=str(promoted_sha or ""),
                manifest_path=str(manifest_path),
                replay_evidence=dict(replay_evidence or {}),
                authority_chain_receipt=dict(
                    generation_manifest.get("authority_chain_receipt", {}) or {}
                ),
                authority_artifacts=dict(
                    generation_manifest.get("authority_artifacts", {}) or {}
                ),
                authority_assembly_path=str(
                    generation_manifest.get("authority_assembly_path", "") or ""
                ),
            )
            self.last_optimization_generation = outcome
            return outcome

        generation_dirty = False
        optimization_symbols = list(effective_symbols)
        stale_exclusions_now_eligible: List[str] = []
        excluded_now: List[str] = []
        if require_fresh_native_activation:
            current_activation_id = str(
                getattr(universe, "activation_id", None) or ""
            )
            stale_source_symbols: List[str] = []
            if not current_activation_id:
                stale_source_symbols = list(requested)
            else:
                for symbol in requested:
                    existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(
                        str(symbol).upper()
                    )
                    eligibility = dict(
                        (getattr(existing, "data_window_meta", {}) or {}).get(
                            "native_cell_eligibility", {}
                        )
                        if existing is not None
                        else {}
                    )
                    if str(eligibility.get("activation_id") or "") == current_activation_id:
                        stale_source_symbols.append(symbol)
            if stale_source_symbols:
                for symbol in stale_source_symbols:
                    results[symbol] = PipelineResult(
                        symbol=symbol,
                        success=False,
                        error_message=(
                            "BLOCKED_STALE_NATIVE_ACTIVATION: scheduled production "
                            "retraining requires a newly activated native source"
                        ),
                    )
                generation_manifest["generation_status"] = "BLOCKED_STALE_SOURCE"
                generation_manifest["blocking_failures"] = {
                    symbol: results[symbol].error_message
                    for symbol in stale_source_symbols
                }
                self._stage_optimization_generation(
                    staged_configs,
                    generation_manifest,
                )
                return _outcome("BLOCKED_STALE_SOURCE")
        if universe is not None and universe.explicit_contract:
            eligible_set = set(
                getattr(universe, "symbols_with_active_cells", ())
                or universe.eligible_symbols
            )
            excluded_set = set(
                getattr(universe, "fully_excluded_symbols", ())
                or universe.excluded_symbols
            )
            optimization_symbols = [
                symbol for symbol in effective_symbols if str(symbol).upper() in eligible_set
            ]
            for symbol in effective_symbols:
                canonical = str(symbol).upper()
                existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(canonical)
                if canonical in excluded_set:
                    excluded_now.append(symbol)
                    if not self._has_exact_native_cell_grid(
                        canonical, existing, universe
                    ):
                        staged_configs[symbol] = (
                            self._build_fully_excluded_native_config(symbol, universe)
                        )
                        generation_manifest["completed_symbols"].append(symbol)
                        generation_dirty = True
                elif (
                    canonical in eligible_set
                    and existing is not None
                    and not self._has_exact_native_cell_grid(
                        canonical, existing, universe
                    )
                ):
                    stale_exclusions_now_eligible.append(symbol)

        # Determine which eligible symbols to optimize.
        artifact_meta = build_artifact_meta(self.config)
        to_optimize, to_skip = self.ledger.get_symbols_to_optimize(
            optimization_symbols,
            overwrite=request.overwrite_targets,
            current_artifact_meta=artifact_meta,
        )
        for symbol in stale_exclusions_now_eligible:
            if symbol not in to_optimize:
                to_optimize.append(symbol)
            if symbol in to_skip:
                to_skip.remove(symbol)
        to_skip.extend(symbol for symbol in excluded_now if symbol not in to_skip)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"PORTFOLIO MANAGER - STATEFUL OPTIMIZATION")
        logger.info(f"Total symbols: {len(self.symbols)}")
        if excluded_now:
            logger.info(
                "Availability-excluded before model search: %d (%s)",
                len(excluded_now),
                ", ".join(str(value) for value in excluded_now[:12]),
            )
        logger.info(f"To optimize: {len(to_optimize)}")
        valid_skip_count = len([symbol for symbol in to_skip if symbol not in excluded_now])
        logger.info(f"Skipping valid configs: {valid_skip_count}")
        logger.info(f"Skipping fully excluded symbols: {len(excluded_now)}")
        if request.overwrite_targets:
            logger.info(f"OVERWRITE MODE: ignoring validity checks")
        logger.info(f"Strategies: {StrategyRegistry.count()}")
        logger.info(f"Timeframes: {self.config.timeframes}")
        logger.info(f"{'#'*60}")
        
        if not to_optimize and not generation_dirty:
            logger.info("All symbols have valid configs. Nothing to optimize.")
            generation_manifest["generation_status"] = "NOOP"
            generation_manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            self._stage_optimization_generation(staged_configs, generation_manifest)
            return _outcome("NOOP")
        
        max_workers = int(getattr(self.config, "optimization_max_workers", 1) or 1)
        precompute_failed = False
        if (
            bool(getattr(self.config, "feature_disk_cache_enabled", False))
            and bool(getattr(self.config, "feature_disk_cache_precompute_enabled", False))
        ):
            precompute_stats = self._precompute_feature_cache_for_symbols(
                to_optimize,
                universe,
            )
            logger.info(f"Feature cache precompute summary: {precompute_stats}")
            for symbol, detail in dict(precompute_stats.get("errors", {}) or {}).items():
                precompute_failed = True
                message = f"FEATURE_PRECOMPUTE_TECHNICAL:{detail}"
                generation_manifest["technical_failures"][str(symbol)] = message
                results[str(symbol)] = PipelineResult(
                    symbol=str(symbol),
                    success=False,
                    error_message=message,
                )

        cfg_dict = _pipeline_config_to_dict(self.config)
        runtime_bindings = _pipeline_runtime_bindings(self.config)
        work = [str(symbol) for symbol in to_optimize]
        receipt_paths = {
            symbol: _optimizer_receipt_path(
                self.config_file,
                generation_id,
                symbol,
            )
            for symbol in work
        }
        _cleanup_optimizer_receipts(receipt_paths.values())
        outcome_stream = None
        optimization_aborted = bool(precompute_failed)
        try:
            if not precompute_failed:
                logger.info(
                    "Process-isolated optimization enabled: requested_workers=%d",
                    max_workers,
                )
                governor = _new_optimizer_memory_governor(max_workers)

                def _tasks() -> Iterable[MemoryTask]:
                    for symbol in work:
                        yield MemoryTask(
                            key=symbol,
                            args=(
                                cfg_dict,
                                runtime_bindings,
                                symbol,
                                generation_id,
                                str(receipt_paths[symbol].resolve()),
                            ),
                        )

                outcome_stream = governor.iter_completed(
                    _governed_optimize_symbol_worker,
                    _tasks(),
                )
                outcome_iter = (
                    tqdm(
                        outcome_stream,
                        total=len(work),
                        desc="Optimizing",
                        unit="sym",
                    )
                    if _TQDM else outcome_stream
                )
                completed = 0
                for outcome in outcome_iter:
                    symbol = str(outcome.task_key)
                    completed += 1
                    if not _TQDM:
                        logger.info(f"\nProgress: {completed}/{len(work)}")
                    if symbol not in receipt_paths:
                        generation_manifest["technical_failures"]["__GOVERNOR__"] = (
                            "GOVERNOR_RETURNED_UNKNOWN_TASK_KEY"
                        )
                        self._stage_optimization_generation(
                            staged_configs,
                            generation_manifest,
                        )
                        optimization_aborted = True
                        break
                    if outcome.status != "COMPLETED":
                        detail = (
                            f"{outcome.status}:{outcome.error_type}:"
                            f"{outcome.error_message}"
                        )
                        result = PipelineResult(
                            symbol=symbol,
                            success=False,
                            error_message=detail,
                        )
                    else:
                        try:
                            result = _consume_optimizer_receipt(
                                outcome.value,
                                expected_generation_id=generation_id,
                                expected_symbol=symbol,
                                expected_path=receipt_paths[symbol],
                            )
                        except Exception as exc:
                            detail = f"INVALID_OPTIMIZER_RECEIPT:{type(exc).__name__}:{exc}"
                            result = PipelineResult(
                                symbol=symbol,
                                success=False,
                                error_message=detail,
                            )
                    results[symbol] = result

                    # Checkpoint into the non-live generation. The production
                    # ledger remains byte-identical until the full roster passes.
                    if result.success and result.config is not None:
                        staged_configs[symbol] = result.config
                        generation_dirty = True
                        if symbol not in generation_manifest["completed_symbols"]:
                            generation_manifest["completed_symbols"].append(symbol)
                        self._stage_optimization_generation(
                            staged_configs,
                            generation_manifest,
                        )
                        logger.info(
                            "STAGED %s in generation %s (live ledger unchanged)",
                            symbol,
                            generation_id,
                        )
                    elif (
                        (
                            _typed_state := _validated_symbol_data_state(
                                (result.data_window_meta or {}).get(
                                    "symbol_data_state"
                                )
                            )
                        )
                        is not None
                        and universe is not None
                        # seq1049: the terminal receipt must cover the
                        # activation's EXACT active candidate TF roster in
                        # canonical vocabulary; anything else is technical
                        # and preserves the live ledger.
                        and _typed_state["timeframes"]
                        == sorted(
                            str(tf_value).upper()
                            for tf_value in (
                                universe.active_timeframes(
                                    str(symbol).upper()
                                )
                                or []
                            )
                        )
                        and all(
                            tf_value
                            in {"M5", "M15", "M30", "H1", "H4", "D1"}
                            for tf_value in _typed_state["timeframes"]
                        )
                    ):
                        # seq1024: statistical/structural all-NON_SEARCH is a
                        # typed terminal data-state: symbol leaves the required
                        # roster, siblings continue, never technical.
                        generation_manifest.setdefault(
                            "data_state_symbols", {}
                        )[symbol] = dict(
                            (result.data_window_meta or {}).get(
                                "symbol_data_state", {}
                            )
                        )
                        staged_configs.pop(symbol, None)
                        if symbol in required_winner_symbols:
                            required_winner_symbols = [
                                s for s in required_winner_symbols if s != symbol
                            ]
                            generation_manifest["required_winner_symbols"] = list(
                                required_winner_symbols
                            )
                        generation_dirty = True
                        self._stage_optimization_generation(
                            staged_configs,
                            generation_manifest,
                        )
                        logger.info(
                            "DATA-STATE %s: %s (removed from required roster, "
                            "generation continues)",
                            symbol,
                            result.error_message,
                        )
                        continue
                    else:
                        detail = str(
                            result.error_message
                            or "no validated strategy config produced"
                        )
                        generation_manifest["technical_failures"][symbol] = detail
                        self._stage_optimization_generation(
                            staged_configs,
                            generation_manifest,
                        )
                        logger.error(
                            "FAILED %s: %s; ledger left unchanged because technical "
                            "failures are not trading verdicts",
                            symbol,
                            detail,
                        )
                        optimization_aborted = True
                        break
        finally:
            if outcome_stream is not None and hasattr(outcome_stream, "close"):
                outcome_stream.close()
            _cleanup_optimizer_receipts(receipt_paths.values())
            self.pipeline.data_loader.clear_cache()
            FeatureComputer.clear_cache()

        technical_failures = dict(
            generation_manifest.get("technical_failures", {}) or {}
        )
        missing_results = [
            symbol for symbol in to_optimize
            if symbol not in results
        ]
        for symbol in missing_results:
            detail = (
                "technical_not_run: optimizer generation stopped after first "
                "technical failure"
                if optimization_aborted
                else "optimizer produced no terminal result"
            )
            technical_failures[symbol] = detail
            results[symbol] = PipelineResult(
                symbol=symbol,
                success=False,
                error_message=detail,
            )
        generation_manifest["technical_failures"] = technical_failures

        if technical_failures:
            generation_manifest["generation_status"] = "FAILED_TECHNICAL"
            generation_manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            self._stage_optimization_generation(
                staged_configs,
                generation_manifest,
            )
            current_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
            if hashlib.sha256(current_bytes).hexdigest() != base_ledger_sha256:
                raise RuntimeError(
                    "OPTIMIZATION_GENERATION_ABORT_FOUND_EXTERNAL_LEDGER_CHANGE"
                )
            logger.error(
                "Optimization generation %s aborted: %d technical failure(s); "
                "production ledger remained unchanged",
                generation_id,
                len(technical_failures),
            )
            return _outcome("FAILED_TECHNICAL")

        configured_symbol_set = {str(symbol) for symbol in self.symbols}
        required_symbol_set = {str(symbol) for symbol in required_winner_symbols}
        missing_staged = sorted(required_symbol_set - set(staged_configs))
        extra_staged = sorted(set(staged_configs) - configured_symbol_set)
        if missing_staged or extra_staged:
            generation_manifest["generation_status"] = "FAILED_INCOMPLETE_ROSTER"
            generation_manifest["technical_failures"] = {
                "__GENERATION__": (
                    f"missing={missing_staged};extra={extra_staged}"
                )
            }
            self._stage_optimization_generation(staged_configs, generation_manifest)
            logger.error(
                "Optimization generation %s incomplete; live ledger unchanged",
                generation_id,
            )
            return _outcome("FAILED_INCOMPLETE_ROSTER")

        ledger_status = self._build_ledger_status(
            staged_configs,
            required_symbols=required_winner_symbols,
        )
        if not bool(ledger_status.get("complete_current_contract_universe", False)):
            generation_manifest["generation_status"] = "FAILED_ARTIFACT_CONTRACT"
            generation_manifest["technical_failures"] = {
                "__GENERATION__": (
                    "staged roster is not complete under the current artifact contract"
                )
            }
            self._stage_optimization_generation(staged_configs, generation_manifest)
            logger.error(
                "Optimization generation %s has an incomplete artifact contract; "
                "live ledger unchanged",
                generation_id,
            )
            return _outcome("FAILED_ARTIFACT_CONTRACT")

        generation_binding = {
            "schema_version": "fxpm.optimization-generation.v2",
            "generation_id": generation_id,
            "generation_status": "COMPLETE",
            "generation_kind": request_kind,
            "requested_symbols": list(requested),
            "effective_symbols": list(effective_symbols),
            "activation_id": getattr(universe, "activation_id", None),
            "activation_manifest_sha256": getattr(
                universe, "activation_manifest_sha256", None
            ),
            "configured_symbol_count": len(self.symbols),
            "required_winner_symbol_count": len(required_winner_symbols),
            "configured_cell_count": len(self.symbols)
            * len(getattr(self.config, "timeframes", []) or [])
            * 4,
        }
        for config in staged_configs.values():
            config.artifact_meta = {
                **dict(config.artifact_meta or {}),
                "ledger_status": ledger_status,
                "optimization_generation": generation_binding,
            }

        generation_manifest["generation_status"] = "COMPLETE"
        generation_manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        generation_manifest["completed_symbols"] = sorted(required_symbol_set)
        stage_path, stage_manifest_path, ledger_sha = (
            self._stage_optimization_generation(
                staged_configs,
                generation_manifest,
            )
        )
        generation_manifest["ledger_sha256"] = ledger_sha
        generation_manifest["ledger_status"] = ledger_status
        self._atomic_write_json(stage_manifest_path, {
            **generation_manifest,
            "ledger_staging_path": str(stage_path),
        })
        self._validate_staged_generation(
            stage_path,
            staged_configs,
            universe,
            required_symbols=required_winner_symbols,
        )

        # Replay the complete resulting roster before the compare-and-swap.
        # Apply-mode audit mutations are confined to staging, then parsed,
        # revalidated and rehashed before promotion.
        try:
            self._staged_replay_ledger_override = stage_path
            try:
                replay_evidence = self._run_production_replay_audit()
            finally:
                self._staged_replay_ledger_override = None
        except Exception as exc:
            detail = f"STAGED_REPLAY_TECHNICAL:{type(exc).__name__}:{exc}"
            generation_manifest["generation_status"] = "FAILED_REPLAY_TECHNICAL"
            generation_manifest["technical_failures"] = {"__REPLAY__": detail}
            self._stage_optimization_generation(staged_configs, generation_manifest)
            return _outcome("FAILED_REPLAY_TECHNICAL")
        if replay_evidence.get("ok") is False:
            detail = str(replay_evidence.get("error") or "staged replay incomplete")
            generation_manifest["generation_status"] = "FAILED_REPLAY_TECHNICAL"
            generation_manifest["technical_failures"] = {"__REPLAY__": detail}
            self._stage_optimization_generation(staged_configs, generation_manifest)
            return _outcome(
                "FAILED_REPLAY_TECHNICAL",
                replay_evidence=replay_evidence,
            )

        try:
            audited_payload = json.loads(stage_path.read_text(encoding="utf-8"))
            staged_configs = {
                symbol: SymbolConfig.from_dict(payload)
                for symbol, payload in audited_payload.items()
            }
        except Exception as exc:
            generation_manifest["generation_status"] = "FAILED_REPLAY_ARTIFACT"
            generation_manifest["technical_failures"] = {
                "__REPLAY__": f"STAGED_REPLAY_ARTIFACT_INVALID:{type(exc).__name__}:{exc}"
            }
            self._stage_optimization_generation(staged_configs, generation_manifest)
            return _outcome("FAILED_REPLAY_ARTIFACT", replay_evidence=replay_evidence)

        ledger_status = self._build_ledger_status(
            staged_configs,
            required_symbols=required_winner_symbols,
        )
        stage_path, stage_manifest_path, ledger_sha = self._stage_optimization_generation(
            staged_configs,
            generation_manifest,
        )
        generation_manifest["ledger_sha256"] = ledger_sha
        generation_manifest["ledger_status"] = ledger_status
        generation_manifest["replay_evidence"] = replay_evidence
        self._atomic_write_json(stage_manifest_path, {
            **generation_manifest,
            "ledger_staging_path": str(stage_path),
        })
        # seq987: identical required set on every validation incl. post-replay.
        self._validate_staged_generation(
            stage_path,
            staged_configs,
            universe,
            required_symbols=required_winner_symbols,
        )
        try:
            self._promote_staged_generation(
                stage_path,
                stage_manifest_path,
                generation_manifest,
                universe,
            )
        except Exception as exc:
            detail = f"PROMOTION_TECHNICAL:{type(exc).__name__}:{exc}"
            live_bytes = ledger_path.read_bytes() if ledger_path.exists() else b""
            live_sha = hashlib.sha256(live_bytes).hexdigest()
            cas_applied = live_sha == ledger_sha
            external_change_preserved = (
                live_sha != base_ledger_sha256 and not cas_applied
            )
            recovery_status = (
                "PROMOTED_RECOVERY_REQUIRED"
                if cas_applied
                else "FAILED_PROMOTION_TECHNICAL"
            )
            reconciled = False
            publication_reconciled = False
            reconciliation_error = ""
            if cas_applied:
                # The target bytes are already live and the pre-CAS journal is
                # the only recovery authority. Complete the exact manifest
                # publication immediately when possible; never restore a stale
                # pre-run snapshot over a completed compare-and-swap.
                self._last_promotion_authorization = None
                try:
                    journal_status = self._reconcile_interrupted_promotion()
                    if journal_status != "COMPLETED":
                        raise RuntimeError(
                            "OPTIMIZATION_PROMOTION_JOURNAL_NOT_COMPLETED:"
                            f"{journal_status}"
                        )
                    self.ledger.load()
                    self.symbol_configs = self.ledger.get_all_configs()
                    reconciled = True
                    publication_reconciled = True
                except Exception as reconcile_exc:
                    reconciliation_error = (
                        f"{type(reconcile_exc).__name__}:{reconcile_exc}"
                    )
                    # The compare-and-swap is already durable even if the
                    # active-manifest write is still recoverable from the
                    # journal.  Runtime memory must immediately follow live
                    # bytes and must never keep trading the superseded book.
                    try:
                        self.ledger.load()
                        self.symbol_configs = self.ledger.get_all_configs()
                        reconciled = True
                    except Exception as reload_exc:
                        reconciliation_error += (
                            "|RUNTIME_RELOAD_FAILED:"
                            f"{type(reload_exc).__name__}:{reload_exc}"
                        )
                if publication_reconciled:
                    logger.warning(
                        "Recovered post-CAS promotion %s from its durable journal "
                        "after %s",
                        generation_id,
                        detail,
                    )
                    return _outcome(
                        "PROMOTED",
                        promoted=True,
                        ledger_changed=True,
                        promoted_sha=ledger_sha,
                        replay_evidence=replay_evidence,
                    )
            generation_manifest["generation_status"] = recovery_status
            generation_manifest["technical_failures"] = {
                "__PROMOTION__": detail
            }
            generation_manifest["promotion_failure_evidence"] = {
                "cas_applied": bool(cas_applied),
                "external_ledger_change_preserved": bool(
                    external_change_preserved
                ),
                "observed_live_ledger_sha256": live_sha,
                "target_ledger_sha256": ledger_sha,
                "manager_reconciled": bool(reconciled),
                "publication_reconciled": bool(publication_reconciled),
                "manager_reconciliation_error": reconciliation_error,
            }
            # Persist only the detached generation manifest. If os.replace()
            # already completed, never attempt to reconstruct or roll back the
            # live ledger; operator evidence records that state explicitly.
            try:
                self._atomic_write_json(
                    stage_manifest_path,
                    {
                        **generation_manifest,
                        "ledger_staging_path": str(stage_path),
                    },
                )
            except Exception as manifest_exc:
                logger.error(
                    "Could not persist promotion recovery manifest for %s: %s",
                    generation_id,
                    manifest_exc,
                )
            return _outcome(
                recovery_status,
                promoted=cas_applied,
                ledger_changed=cas_applied,
                promoted_sha=ledger_sha if cas_applied else "",
                replay_evidence=replay_evidence,
                manifest_path_override=stage_manifest_path,
            )
        logger.info(
            "PROMOTED complete optimization generation %s (%d symbols) atomically",
            generation_id,
            len(staged_configs),
        )

        # Summary
        total_time = time.time() - total_start
        successful = sum(1 for r in results.values() if r.success)
        
        # Include skipped symbols in final counts
        total_validated = len(self.get_validated_configs())
        total_live_eligible = len(self.get_live_eligible_configs())
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"OPTIMIZATION COMPLETE")
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Optimized: {successful}/{len(to_optimize)}")
        logger.info(f"Skipped valid configs: {valid_skip_count}")
        logger.info(f"Fully excluded without search: {len(excluded_now)}")
        logger.info(f"Total validated: {total_validated}/{len(self.symbols)}")
        logger.info(f"Live eligible: {total_live_eligible}/{len(self.symbols)}")
        logger.info(f"{'#'*60}")

        return _outcome(
            "PROMOTED",
            promoted=True,
            ledger_changed=True,
            promoted_sha=ledger_sha,
            replay_evidence=replay_evidence,
        )

    def initial_optimization(
        self,
        overwrite: bool = False,
    ) -> Dict[str, PipelineResult]:
        """Compatibility wrapper over the sole governed generation engine."""
        outcome = self._run_governed_symbol_generation(
            OptimizationGenerationRequest(
                kind="INITIAL",
                target_symbols=tuple(str(symbol) for symbol in self.symbols),
                overwrite_targets=bool(overwrite),
                require_fresh_native_activation=False,
                supervised_generation_id=getattr(
                    self,
                    "_supervised_generation_id",
                    None,
                ),
            )
        )
        return dict(outcome.results)
    
    def get_validated_configs(self) -> Dict[str, SymbolConfig]:
        """Get only validated configurations."""
        # Refresh from ledger to ensure consistency
        self.symbol_configs = self.ledger.get_all_configs()
        return {
            s: c
            for s, c in self.symbol_configs.items()
            if c.is_validated or c.has_tradeable_regime_winners()
        }

    def get_edge_challenger_configs(
        self,
        *,
        log_rejections: bool = False,
    ) -> Dict[str, SymbolConfig]:
        """Return the executable demo-probation view of the edge book.

        This selector deliberately does *not* reuse funded-live eligibility.
        An edge refit has no validation/lockbox verdict to inherit, so its only
        legal execution surface is the separately authorized demo path.  The
        returned ``SymbolConfig`` objects contain only structurally complete
        PAPER_ONLY challenger cells; funded champions are never copied into
        this view and the persisted ledger is never mutated.
        """
        self.symbol_configs = self.ledger.get_all_configs()
        selected: Dict[str, SymbolConfig] = {}
        rejections: Dict[str, str] = {}

        for symbol, champion_book in self.symbol_configs.items():
            book = dict(getattr(champion_book, "edge_challenger_book", {}) or {})
            book_role = str(book.get("book_role", "") or "").strip().upper()
            if book_role != EDGE_CHALLENGER_ROLE:
                rejections[symbol] = "edge challenger book role missing or invalid"
                continue
            if not bool(book.get("paper_only", False)):
                rejections[symbol] = "edge challenger book is not explicitly paper-only"
                continue
            if bool(book.get("funded_role_eligible", True)):
                rejections[symbol] = "edge challenger book claims funded eligibility"
                continue

            raw_timeframes = book.get("regime_configs") or {}
            if not isinstance(raw_timeframes, dict):
                rejections[symbol] = "edge challenger regime_configs is not an object"
                continue
            parsed_timeframes: Dict[str, Dict[str, RegimeConfig]] = {}
            invalid_cells: List[str] = []
            for timeframe, raw_regimes in raw_timeframes.items():
                if not isinstance(raw_regimes, dict):
                    invalid_cells.append(f"{timeframe}:regime map invalid")
                    continue
                parsed_regimes: Dict[str, RegimeConfig] = {}
                for regime, raw_candidate in raw_regimes.items():
                    try:
                        candidate = (
                            RegimeConfig.from_dict(raw_candidate)
                            if isinstance(raw_candidate, dict)
                            else RegimeConfig.from_dict(raw_candidate.to_dict())
                        )
                    except Exception as exc:
                        invalid_cells.append(
                            f"{timeframe}|{regime}:deserialize:{type(exc).__name__}"
                        )
                        continue
                    temporal = dict(
                        (candidate.validation_evidence or {}).get("temporal_lineage", {})
                        or {}
                    )
                    edge_refit = dict(
                        (candidate.validation_evidence or {}).get("edge_refit", {})
                        or {}
                    )
                    candidate_id = str(
                        (candidate.artifact_meta or {}).get("candidate_id")
                        or edge_refit.get("candidate_id")
                        or ""
                    ).strip()
                    fit_cutoff_raw = str(temporal.get("fit_cutoff_utc", "") or "").strip()
                    errors: List[str] = []
                    if candidate.is_no_trade_marker():
                        errors.append("NO_TRADE")
                    if str(candidate.promotion_status or "").upper() != "PAPER_ONLY":
                        errors.append("status is not PAPER_ONLY")
                    if not bool((candidate.artifact_meta or {}).get("edge_challenger", False)):
                        errors.append("edge artifact flag missing")
                    if bool((candidate.artifact_meta or {}).get("funded_role_eligible", True)):
                        errors.append("candidate claims funded eligibility")
                    if str(temporal.get("book_role", "") or "").upper() != EDGE_CHALLENGER_ROLE:
                        errors.append("temporal role mismatch")
                    if str(temporal.get("contract_status", "") or "").upper() != "PAPER_ONLY":
                        errors.append("temporal contract is not PAPER_ONLY")
                    if not bool(temporal.get("edge_refit_performed", False)):
                        errors.append("latest-data refit not proven")
                    if bool(temporal.get("champion_lockbox_verdict_inherited", True)):
                        errors.append("champion verdict inheritance is not prohibited")
                    if not fit_cutoff_raw:
                        errors.append("fit cutoff missing")
                    else:
                        try:
                            fit_cutoff = datetime.fromisoformat(
                                fit_cutoff_raw.replace("Z", "+00:00")
                            )
                            if fit_cutoff.tzinfo is None:
                                fit_cutoff = fit_cutoff.replace(tzinfo=timezone.utc)
                            fit_cutoff = fit_cutoff.astimezone(timezone.utc)
                        except (TypeError, ValueError, OverflowError):
                            errors.append("fit cutoff is invalid")
                        else:
                            if fit_cutoff > datetime.now(timezone.utc):
                                errors.append("fit cutoff is in the future")
                            if str(
                                temporal.get("requires_forward_evidence_strictly_after_utc", "")
                                or ""
                            ).strip() != fit_cutoff_raw:
                                errors.append("forward evidence boundary differs from fit cutoff")
                    if not bool(temporal.get("requires_future_nonoverlapping_forward_evidence", False)):
                        errors.append("future evidence contract missing")
                    if list(temporal.get("contract_errors", []) or []):
                        errors.append("temporal contract carries errors")
                    if not candidate_id:
                        errors.append("candidate id missing")
                    else:
                        expected_candidate_id = _stable_hash({
                            "book_role": EDGE_CHALLENGER_ROLE,
                            "symbol": str(symbol).upper(),
                            "timeframe": str(timeframe).upper(),
                            "regime": str(regime).upper(),
                            "strategy_name": str(candidate.strategy_name or ""),
                            "parameters": dict(candidate.parameters or {}),
                            "fit_cutoff_utc": fit_cutoff_raw or None,
                        })
                        if candidate_id != expected_candidate_id:
                            errors.append("candidate id does not match cell payload")
                    if int((candidate.train_metrics or {}).get("total_trades", 0) or 0) <= 0:
                        errors.append("latest-data fit has no trades")
                    if errors:
                        invalid_cells.append(
                            f"{timeframe}|{regime}:" + ", ".join(errors)
                        )
                        continue
                    parsed_regimes[str(regime).upper()] = candidate
                if parsed_regimes:
                    parsed_timeframes[str(timeframe).upper()] = parsed_regimes

            if not parsed_timeframes:
                reason = "no executable edge challenger cells"
                if invalid_cells:
                    reason += f" ({'; '.join(invalid_cells[:4])})"
                rejections[symbol] = reason
                continue

            selected[symbol] = SymbolConfig(
                symbol=symbol,
                regime_configs=parsed_timeframes,
                default_config=None,
                is_validated=False,
                validation_reason="demo-only latest-data edge challenger",
                data_window_meta=dict(champion_book.data_window_meta or {}),
                edge_challenger_book=book,
                artifact_meta={
                    "book_role": EDGE_CHALLENGER_ROLE,
                    "funded_role_eligible": False,
                    "paper_only": True,
                },
                regime_detection_version=champion_book.regime_detection_version,
                optimized_at=champion_book.optimized_at,
                valid_until=None,
            )

        self._last_edge_challenger_rejections = dict(rejections)
        if log_rejections:
            for symbol, reason in sorted(rejections.items()):
                logger.warning(f"EDGE DEMO BLOCK {symbol}: {reason}")
        return selected

    def _live_eligibility_reason(self,
                                 symbol: str,
                                 config: SymbolConfig,
                                 *,
                                 now: datetime,
                                 current_artifact_meta: Dict[str, Any],
                                 artifact_policy: str,
                                 expiry_grace_minutes: int) -> Tuple[bool, str]:
        """Evaluate the live-trading eligibility contract for one config."""
        has_tradeable_regime_winners = config.has_tradeable_regime_winners()
        if not config.is_validated and not has_tradeable_regime_winners:
            return False, f"invalid ({config.validation_reason or 'not validated'})"
        if config.valid_until is None:
            return False, "no expiry date"
        grace = max(0, int(expiry_grace_minutes or 0))
        valid_deadline = config.valid_until + pd.Timedelta(minutes=grace).to_pytimedelta()
        if now >= valid_deadline:
            return False, f"expired at {config.valid_until.strftime('%Y-%m-%d %H:%M')}"
        if config.has_regime_configs() and not has_tradeable_regime_winners:
            return False, "no tradeable regime winners"
        if not config.has_regime_configs():
            default_cfg = config.default_config
            if default_cfg is not None and default_cfg.is_no_trade_marker():
                return False, "default config is NO_TRADE"
            if str(config.strategy_name or "").upper() == "NO_TRADE" or bool((config.artifact_meta or {}).get("no_trade")):
                return False, "symbol config is NO_TRADE"
            if not config.strategy_name and default_cfg is None:
                return False, "no strategy"

        live_split_mode = str(
            getattr(self.config, "production_split_mode", "") or ""
        ).strip().lower()
        if live_split_mode in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
            required_timeframes = {
                str(timeframe).upper()
                for timeframe, regime_map in (config.regime_configs or {}).items()
                if isinstance(regime_map, dict)
                and any(
                    candidate is not None and not candidate.is_no_trade_marker()
                    for candidate in regime_map.values()
                )
            }
            if not required_timeframes and str(config.timeframe or "").strip():
                required_timeframes.add(str(config.timeframe).strip().upper())
            if not required_timeframes:
                return False, "no detector timeframe for provenance validation"
            params_file = str(
                getattr(self.config, "regime_params_file", "regime_params.json")
                or "regime_params.json"
            )
            expected_contract = (
                EVIDENCE_CONTRACT_VERSION
                if live_split_mode == CALENDAR_SPLIT_MODE
                else DataSplitter.SPLIT_CONTRACT_VERSION
            )
            for timeframe in sorted(required_timeframes):
                provenance = validate_regime_params_provenance(
                    symbol,
                    timeframe,
                    params_file,
                    expected_split_contract=expected_contract,
                )
                if not provenance.get("valid", False):
                    return False, (
                        f"regime detector provenance invalid for {timeframe}: "
                        f"{provenance.get('reason', 'unknown')}"
                    )

        if current_artifact_meta and artifact_policy == "block":
            stored_artifact = getattr(config, "artifact_meta", {}) or {}
            if not artifact_contract_matches(stored_artifact, current_artifact_meta):
                return False, "artifact fingerprint changed"

        return True, "live eligible"

    def get_live_eligible_configs(self,
                                  *,
                                  current_artifact_meta: Optional[Dict[str, Any]] = None,
                                  log_rejections: bool = False,
                                  now: Optional[datetime] = None) -> Dict[str, SymbolConfig]:
        """
        Get configurations eligible for live trading.

        This is intentionally stricter than `get_validated_configs()`: live
        trading must honor expiry/no-winner state and the configured artifact
        drift policy, while raw validation remains useful for optimization and
        ledger reporting.
        """
        self.symbol_configs = self.ledger.get_all_configs()
        artifact_meta = current_artifact_meta if current_artifact_meta is not None else build_artifact_meta(self.config)
        artifact_policy = str(getattr(self.config, "live_artifact_drift_policy", "block") or "block").strip().lower()
        if artifact_policy not in {"block", "warn", "ignore"}:
            artifact_policy = "block"
        expiry_grace = int(getattr(self.config, "live_config_expiry_grace_minutes", 0) or 0)
        probe = now or datetime.now()

        eligible: Dict[str, SymbolConfig] = {}
        rejections: Dict[str, str] = {}
        drift_warnings: Dict[str, str] = {}

        gate_mode = str(
            getattr(self.config, "production_promotion_gate_mode", "ledger_status") or ""
        ).strip().lower()
        if gate_mode in {"immutable", "authorization", "authorization_exact"}:
            gate_mode = "immutable_exact"
        if bool(getattr(self.config, "production_require_promotion_gate", True)) and gate_mode == "immutable_exact":
            try:
                authorization = self.verify_live_promotion_authorization()
            except Exception as exc:
                authorization = None
                authorization_reason = (
                    f"promotion authorization verification raised {type(exc).__name__}: {exc}"
                )
            else:
                authorization_reason = authorization.concise_reason()
            if authorization is None or not authorization.valid:
                for symbol in self.symbol_configs:
                    rejections[symbol] = f"immutable promotion authorization rejected: {authorization_reason}"
                self._last_live_eligibility_rejections = dict(rejections)
                self._last_live_eligibility_warnings = {}
                if log_rejections:
                    for symbol, reason in sorted(rejections.items()):
                        logger.warning(f"LIVE BLOCK {symbol}: {reason}")
                return {}

        for symbol, config in self.symbol_configs.items():
            ok, reason = self._live_eligibility_reason(
                symbol,
                config,
                now=probe,
                current_artifact_meta=artifact_meta,
                artifact_policy=artifact_policy,
                expiry_grace_minutes=expiry_grace,
            )
            if ok:
                if artifact_policy == "warn" and artifact_meta:
                    stored_artifact = getattr(config, "artifact_meta", {}) or {}
                    if not artifact_contract_matches(stored_artifact, artifact_meta):
                        drift_warnings[symbol] = "artifact fingerprint changed"
                eligible[symbol] = config
            else:
                rejections[symbol] = reason

        self._last_live_eligibility_rejections = dict(rejections)
        self._last_live_eligibility_warnings = dict(drift_warnings)

        if log_rejections:
            for symbol, reason in sorted(rejections.items()):
                logger.warning(f"LIVE BLOCK {symbol}: {reason}")
            for symbol, reason in sorted(drift_warnings.items()):
                logger.warning(f"LIVE WARN {symbol}: {reason}; trading allowed by policy")

        return eligible
    
    def get_active_strategy(self, symbol: str) -> Optional[BaseStrategy]:
        """
        Get active strategy instance for a symbol.
        
        Args:
            symbol: Symbol name
            
        Returns:
            Strategy instance or None
        """
        if symbol not in self.symbol_configs:
            return None
        
        config = self.symbol_configs[symbol]
        
        if not config.is_validated:
            return None
        
        return StrategyRegistry.get(config.strategy_name, **config.parameters)

    def _resolve_regime_config(self,
                               symbol: str,
                               timeframe: Optional[str] = None,
                               regime: Optional[str] = None) -> Optional[RegimeConfig]:
        symbol_cfg = self.symbol_configs.get(symbol)
        if symbol_cfg is None:
            return None
        tf = str(timeframe or "").upper()
        rg = str(regime or "").upper()
        if tf and rg and symbol_cfg.has_regime_configs():
            return symbol_cfg.get_regime_config(tf, rg)
        return symbol_cfg.default_config

    def get_governance_policy(self,
                              symbol: str,
                              timeframe: Optional[str] = None,
                              regime: Optional[str] = None) -> Dict[str, Any]:
        cfg = self._resolve_regime_config(symbol, timeframe, regime)
        if cfg is None:
            return _default_governance_policy()
        return dict(cfg.governance_policy or _default_governance_policy())

    def live_eligibility_report(self,
                                symbol: str,
                                timeframe: Optional[str] = None,
                                regime: Optional[str] = None,
                                at_dt: Optional[datetime] = None) -> Dict[str, Any]:
        probe = at_dt or datetime.now()
        report = {
            "symbol": symbol,
            "timeframe": str(timeframe or "").upper(),
            "regime": str(regime or "").upper(),
            "config_present": False,
            "config_age_days": None,
            "cycle_position": None,
            "last_retrain_ts": None,
            "next_retrain_due": None,
            "freshness_band": "missing",
        }
        symbol_cfg = self.symbol_configs.get(symbol)
        if symbol_cfg is None:
            return report
        tf = str(timeframe or "").upper()
        rg = str(regime or "").upper()
        if tf and rg and symbol_cfg.has_regime_configs():
            cfg = symbol_cfg.get_regime_config(tf, rg)
        else:
            cfg = self._resolve_regime_config(symbol, timeframe, regime)
        if cfg is None:
            return report
        last_retrain_ts = cfg.trained_at or symbol_cfg.optimized_at
        next_retrain_due = symbol_cfg.valid_until or self.config.get_next_retrain_at(probe)
        last_slot = self.config.get_last_retrain_slot(probe)
        cycle_span_seconds = max((next_retrain_due - last_slot).total_seconds(), 1.0)
        cycle_position = min(max((probe - last_slot).total_seconds() / cycle_span_seconds, 0.0), 1.0)
        if cycle_position < (1.0 / 3.0):
            freshness_band = "fresh"
        elif cycle_position < (2.0 / 3.0):
            freshness_band = "mid_cycle"
        else:
            freshness_band = "end_of_cycle"
        report.update(
            {
                "config_present": True,
                "config_age_days": None if last_retrain_ts is None else round(
                    max((probe - last_retrain_ts).total_seconds(), 0.0) / 86400.0, 3
                ),
                "cycle_position": round(cycle_position, 4),
                "last_retrain_ts": last_retrain_ts.isoformat() if last_retrain_ts else None,
                "next_retrain_due": next_retrain_due.isoformat() if next_retrain_due else None,
                "freshness_band": freshness_band,
            }
        )
        return report

    def update_live_observability(self,
                                  symbol: str,
                                  timeframe: Optional[str],
                                  regime: Optional[str],
                                  updates: Dict[str, Any],
                                  *,
                                  save: bool = False) -> Dict[str, Any]:
        if save:
            raise RuntimeError(
                "RUNTIME_OBSERVABILITY_AUTHORITY_MUTATION_FORBIDDEN"
            )
        symbol_cfg = self.symbol_configs.get(symbol)
        tf = str(timeframe or "").upper()
        rg = str(regime or "").upper()
        if symbol_cfg is not None and tf and rg and symbol_cfg.has_regime_configs():
            cfg = symbol_cfg.get_regime_config(tf, rg)
        else:
            cfg = self._resolve_regime_config(symbol, timeframe, regime)
        if symbol_cfg is None or cfg is None or not isinstance(updates, dict):
            return {}
        merged = _default_live_observability()
        merged.update(dict(cfg.live_observability or {}))
        merged.update(dict(updates))
        cfg.live_observability = merged
        if symbol_cfg.default_config is cfg:
            symbol_cfg.live_observability = dict(merged)
        return dict(merged)
    
    def get_ledger_stats(self) -> Dict[str, Any]:
        """Get ledger statistics for monitoring."""
        return self.ledger.get_stats()
    
    def print_status(self):
        """Print current portfolio status."""
        stats = self.ledger.get_stats()
        artifact_meta = build_artifact_meta(self.config)
        live_eligible = self.get_live_eligible_configs(current_artifact_meta=artifact_meta)
        live_rejections = getattr(self, "_last_live_eligibility_rejections", {}) or {}
        
        print("\n" + "=" * 70)
        print("PORTFOLIO MANAGER STATUS")
        print("=" * 70)
        print(f"Config file: {stats['filepath']}")
        print(f"Symbols managed: {len(self.symbols)}")
        print(f"Configured: {stats['total']}")
        print(f"Valid: {stats['valid']}")
        print(f"Live eligible: {len(live_eligible)}")
        print(f"Expired: {stats['expired']}")
        print(f"Invalid: {stats['invalid']}")
        print(f"Need optimization: {len(self.get_symbols_needing_retrain())}")
        print(f"Live artifact policy: {getattr(self.config, 'live_artifact_drift_policy', 'block')}")
        print()
        
        now = datetime.now()
        for symbol in self.symbols:
            if symbol in self.symbol_configs:
                c = self.symbol_configs[symbol]
                should_optimize, optimize_reason = self.ledger.should_optimize(
                    symbol,
                    current_artifact_meta=artifact_meta,
                )
                
                # Determine status indicator
                if not c.is_validated:
                    status = "NO"
                elif c.valid_until and now >= c.valid_until:
                    status = "EX"  # Expired
                elif symbol not in live_eligible:
                    status = "BL"
                elif should_optimize:
                    status = "DU"
                else:
                    status = "OK"
                
                # Calculate days remaining/expired
                if symbol in live_rejections:
                    expire_str = f"LIVE BLOCK: {live_rejections[symbol]}"
                elif should_optimize and optimize_reason:
                    expire_str = f"DUE: {optimize_reason}"
                elif c.valid_until:
                    days = (c.valid_until - now).days
                    if days >= 0:
                        expire_str = f"{c.valid_until.strftime('%Y-%m-%d %H:%M')} ({days}d)"
                    else:
                        expire_str = f"EXPIRED {-days}d ago"
                else:
                    expire_str = "N/A"
                
                print(f"  {status} {symbol:8} | {c.strategy_name:25} | {c.timeframe:3} | "
                      f"Score: {c.composite_score:5.1f} | Expires: {expire_str}")
            else:
                print(f"  -- {symbol:8} | Not configured")
        
        print("=" * 70)


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Data classes
    'SymbolConfig',
    'RegimeConfig',
    'PipelineResult',
    # Config persistence
    'ConfigLedger',
    # Optimization stages
    'StrategySelector',
    'HyperparameterOptimizer',
    'RetrainPeriodSelector',
    'Validator',
    'RegimeOptimizer',
    # Main pipeline
    'OptimizationPipeline',
    'PortfolioManager',
    # Optuna utilities (re-exported)
    'OPTUNA_AVAILABLE',
    'is_optuna_available',
    'get_optimization_method',
]


if __name__ == "__main__":
    # Test pipeline
    logging.basicConfig(level=logging.INFO)
    
    # Log optimization method
    logger.info(f"Optimization method: {get_optimization_method()}")
    logger.info(f"Optuna available: {is_optuna_available()}")
    
    config = PipelineConfig(
        data_dir=Path("./data"),
        timeframes=['H1', 'H4'],
        production_retrain_mode="notify",
        production_retrain_interval_weeks=4,
    )
    
    pm = PortfolioManager(
        config=config,
        symbols=['EURUSD', 'GBPUSD']
    )
    
    pm.print_status()
    print("\npm_pipeline.py loaded successfully!")

