"""Fail-closed live-readiness validation for FXPM."""

from __future__ import annotations

import json
import hashlib
import math
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


SPARSE_AUTHORITY_RESULT_SCHEMA = "fxpm.live-readiness-authority-result.v2"
SPARSE_AUTHORITY_MANIFEST_MAX_BYTES = 4 * 1024 * 1024
SPARSE_WINNER_LEDGER_MAX_BYTES = 512 * 1024 * 1024
SPARSE_AUTHORITY_INDEX_MAX_BYTES = 64 * 1024 * 1024
SPARSE_AUTHORITY_TOTAL_INDEX_MAX_BYTES = 256 * 1024 * 1024
SPARSE_AUTHORITY_MAX_SYMBOLS = 1_000
SPARSE_AUTHORITY_ARTIFACTS = (
    "regime_support",
    "causal_process",
    "historical_book",
    "current_process_book",
    "current_refit_execution",
    "synchronized_portfolio_replay",
    "capital_calibration",
)
SPARSE_AUTHORITY_ARTIFACT_SCHEMAS = {
    name: f"fxpm.{name.replace('_', '-')}-authority-index.v1"
    for name in SPARSE_AUTHORITY_ARTIFACTS
}
SPARSE_AUTHORITY_RAW_REVERIFICATION = (
    "native_data_state",
    *SPARSE_AUTHORITY_ARTIFACTS,
)
_SPARSE_ARTIFACT_DESCRIPTOR_KEYS = frozenset(
    {"relative_path", "sha256", "size_bytes", "schema_version"}
)
_CURRENT_REFIT_BINDING_FIELDS = frozenset(
    {
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
    }
)
_NON_WINNER_STRATEGY_NAMES = frozenset(
    {
        "BLOCKED",
        "NO_LOCAL_WINNER",
        "NO_TRADE",
        "NO_WINNER",
        "PAPER_ONLY",
        "REGIME_UNOBSERVED",
        "SOURCE_UNAVAILABLE",
        "TECHNICAL_FAILURE",
        "UNSEARCHED",
    }
)


@dataclass
class ReadinessFinding:
    severity: str
    code: str
    message: str
    scope: str = "evidence"


@dataclass
class ReadinessReport:
    ok: bool
    findings: List[ReadinessFinding] = field(default_factory=list)

    @property
    def errors(self) -> List[ReadinessFinding]:
        return [finding for finding in self.findings if finding.severity == "error"]

    @property
    def warnings(self) -> List[ReadinessFinding]:
        return [finding for finding in self.findings if finding.severity == "warning"]

    @property
    def evidence_errors(self) -> List[ReadinessFinding]:
        return [finding for finding in self.errors if finding.scope == "evidence"]

    @property
    def runtime_errors(self) -> List[ReadinessFinding]:
        return [finding for finding in self.errors if finding.scope == "runtime"]


def _load_json_file(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _sha256_file(path: Path) -> Optional[str]:
    try:
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except Exception:
        return None


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _load_bounded_json_file(path: Path, maximum_bytes: int) -> Optional[Any]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > maximum_bytes:
            return None
        raw = path.read_bytes()
        if len(raw) != size:
            return None
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _path_uses_reparse_point(root: Path, path: Path) -> bool:
    try:
        root = root.resolve(strict=True)
        path = path.resolve(strict=True)
        path.relative_to(root)
        current = root
        parts = path.relative_to(root).parts
        for part in parts:
            current = current / part
            info = current.lstat()
            attributes = int(getattr(info, "st_file_attributes", 0) or 0)
            if current.is_symlink() or attributes & 0x400:
                return True
        return False
    except Exception:
        return True


def _resolve_sparse_authority_indexes(
    manifest: Mapping[str, Any], manifest_path: Path
) -> Tuple[Dict[str, Path], List[str]]:
    artifacts = manifest.get("authority_artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(
        SPARSE_AUTHORITY_ARTIFACTS
    ):
        return {}, ["AUTHORITY_ARTIFACT_ROSTER_INVALID"]

    root = manifest_path.resolve().parent
    resolved: Dict[str, Path] = {}
    errors: List[str] = []
    total_bytes = 0
    for name in SPARSE_AUTHORITY_ARTIFACTS:
        descriptor = artifacts.get(name)
        if (
            not isinstance(descriptor, Mapping)
            or set(descriptor) != _SPARSE_ARTIFACT_DESCRIPTOR_KEYS
        ):
            errors.append(f"AUTHORITY_ARTIFACT_DESCRIPTOR_INVALID:{name}")
            continue
        relative = descriptor.get("relative_path")
        if not isinstance(relative, str) or not relative.strip():
            errors.append(f"AUTHORITY_ARTIFACT_PATH_INVALID:{name}")
            continue
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            errors.append(f"AUTHORITY_ARTIFACT_PATH_INVALID:{name}")
            continue
        path = root / candidate
        try:
            resolved_path = path.resolve(strict=True)
            resolved_path.relative_to(root)
            size = resolved_path.stat().st_size
        except Exception:
            errors.append(f"AUTHORITY_ARTIFACT_UNREADABLE:{name}")
            continue
        declared_size = descriptor.get("size_bytes")
        if (
            type(declared_size) is not int
            or declared_size <= 0
            or declared_size > SPARSE_AUTHORITY_INDEX_MAX_BYTES
            or size != declared_size
            or not resolved_path.is_file()
            or _path_uses_reparse_point(root, resolved_path)
        ):
            errors.append(f"AUTHORITY_ARTIFACT_SIZE_OR_TYPE_INVALID:{name}")
            continue
        declared_sha = str(descriptor.get("sha256") or "").lower()
        if not _is_sha256(declared_sha) or _sha256_file(resolved_path) != declared_sha:
            errors.append(f"AUTHORITY_ARTIFACT_SHA256_MISMATCH:{name}")
            continue
        schema = descriptor.get("schema_version")
        if schema != SPARSE_AUTHORITY_ARTIFACT_SCHEMAS[name]:
            errors.append(f"AUTHORITY_ARTIFACT_SCHEMA_INVALID:{name}")
            continue
        total_bytes += size
        resolved[name] = resolved_path
    if total_bytes > SPARSE_AUTHORITY_TOTAL_INDEX_MAX_BYTES:
        errors.append("AUTHORITY_ARTIFACT_INDEX_BUDGET_EXCEEDED")
    return resolved, list(dict.fromkeys(errors))


def _ledger_symbol_payloads(
    ledger: Any,
) -> Tuple[Dict[str, Mapping[str, Any]], List[str]]:
    if not isinstance(ledger, Mapping):
        return {}, ["LEDGER_NOT_OBJECT"]
    root: Any = ledger
    if isinstance(ledger.get("symbols"), Mapping):
        root = ledger["symbols"]
    elif isinstance(ledger.get("symbol_configs"), Mapping):
        root = ledger["symbol_configs"]
    if not isinstance(root, Mapping):
        return {}, ["LEDGER_SYMBOL_MAP_INVALID"]

    payloads: Dict[str, Mapping[str, Any]] = {}
    errors: List[str] = []
    for raw_symbol, payload in root.items():
        if not isinstance(payload, Mapping):
            errors.append(f"LEDGER_SYMBOL_PAYLOAD_INVALID:{raw_symbol}")
            continue
        symbol = str(payload.get("symbol") or raw_symbol).strip().upper()
        if not symbol or symbol in payloads:
            errors.append(f"LEDGER_SYMBOL_IDENTITY_INVALID:{symbol or raw_symbol}")
            continue
        payloads[symbol] = payload
    return payloads, errors


def _sparse_current_refit_projection(
    ledger: Any, requested_symbols: Sequence[str]
) -> Tuple[Set[str], List[str]]:
    payloads, errors = _ledger_symbol_payloads(ledger)
    requested = set(requested_symbols)
    timeframes = {"M5", "M15", "M30", "H1", "H4", "D1"}
    regimes = {"TREND", "RANGE", "BREAKOUT", "CHOP"}
    extra_symbols = sorted(set(payloads) - requested)
    errors.extend(f"LEDGER_SYMBOL_OUTSIDE_BOOK:{symbol}" for symbol in extra_symbols)

    selected: Set[str] = set()
    for symbol, payload in sorted(payloads.items()):
        if symbol not in requested:
            continue
        grid = payload.get("regime_configs")
        if not isinstance(grid, Mapping):
            errors.append(f"LEDGER_WINNER_GRID_INVALID:{symbol}")
            continue
        if not grid:
            errors.append(f"LEDGER_EMPTY_SYMBOL_PLACEHOLDER_FORBIDDEN:{symbol}")
            continue
        for raw_timeframe, timeframe_payload in grid.items():
            timeframe = str(raw_timeframe or "").strip().upper()
            if timeframe not in timeframes or not isinstance(
                timeframe_payload, Mapping
            ):
                errors.append(f"LEDGER_TIMEFRAME_INVALID:{symbol}|{timeframe}")
                continue
            for raw_regime, cell in timeframe_payload.items():
                regime = str(raw_regime or "").strip().upper()
                cell_id = f"{symbol}|{timeframe}|{regime}"
                if regime not in regimes or not isinstance(cell, Mapping):
                    errors.append(f"LEDGER_CELL_INVALID:{cell_id}")
                    continue
                strategy = str(cell.get("strategy_name") or "").strip()
                artifact = (
                    cell.get("artifact_meta")
                    if isinstance(cell.get("artifact_meta"), Mapping)
                    else {}
                )
                if (
                    not strategy
                    or strategy.upper() in _NON_WINNER_STRATEGY_NAMES
                    or bool(artifact.get("no_trade"))
                ):
                    errors.append(f"LEDGER_NON_WINNER_PLACEHOLDER_FORBIDDEN:{cell_id}")
                    continue
                status = str(cell.get("promotion_status") or "").strip().upper()
                if status != "PROCESS_VALIDATED_CURRENT_REFIT":
                    errors.append(
                        f"LEDGER_CURRENT_REFIT_STATUS_INVALID:{cell_id}:{status or 'MISSING'}"
                    )
                    continue
                binding = artifact.get("current_refit_binding")
                if (
                    not isinstance(binding, Mapping)
                    or set(binding) != _CURRENT_REFIT_BINDING_FIELDS
                    or any(not _is_sha256(binding.get(field)) for field in binding)
                ):
                    errors.append(f"LEDGER_CURRENT_REFIT_BINDING_INVALID:{cell_id}")
                    continue
                if cell_id in selected:
                    errors.append(f"LEDGER_CURRENT_REFIT_CELL_DUPLICATE:{cell_id}")
                    continue
                selected.add(cell_id)
    if not selected:
        errors.append("LEDGER_HAS_NO_CURRENT_REFIT_WINNERS")
    return selected, list(dict.fromkeys(errors))


def _readiness_scope(code: str) -> str:
    runtime_prefixes = (
        "account_",
        "manual_kill_switch_",
        "manual_new_entries_",
        "min_lot_",
        "mt5_",
        "storage_",
        "supervisor_",
    )
    return "runtime" if str(code).startswith(runtime_prefixes) else "evidence"


def _verify_sparse_live_authority(
    *,
    config: Any,
    config_path: str,
    config_payload: Mapping[str, Any],
    ledger: Any,
    ledger_path: Path,
    requested_symbols: Sequence[str],
    selected_cells: Set[str],
    native_universe: Any,
    validation_time_utc: datetime,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if (
        not 1 <= len(requested_symbols) <= SPARSE_AUTHORITY_MAX_SYMBOLS
        or any(
            not isinstance(symbol, str)
            or not symbol
            or len(symbol) > 128
            or "|" in symbol
            for symbol in requested_symbols
        )
        or len(requested_symbols) != len(set(requested_symbols))
    ):
        return None, ["LIVE_AUTHORITY_REQUESTED_SYMBOL_ROSTER_INVALID"]
    if (
        not isinstance(selected_cells, set)
        or len(selected_cells) > len(requested_symbols) * 6 * 4
        or any(not isinstance(cell_id, str) for cell_id in selected_cells)
    ):
        return None, ["LIVE_AUTHORITY_SELECTED_CELL_ROSTER_INVALID"]
    manifest_path = ledger_path.with_name(f"{ledger_path.stem}.generation.json")
    manifest = _load_bounded_json_file(
        manifest_path, SPARSE_AUTHORITY_MANIFEST_MAX_BYTES
    )
    if not isinstance(manifest, Mapping):
        return None, ["OPTIMIZATION_AUTHORITY_MANIFEST_MISSING_OR_UNBOUNDED"]

    errors: List[str] = []
    if manifest.get("schema_version") != "fxpm.optimization-generation.v2":
        errors.append("OPTIMIZATION_AUTHORITY_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("generation_status") or "").upper() != "PROMOTED":
        errors.append("OPTIMIZATION_AUTHORITY_GENERATION_NOT_PROMOTED")
    if manifest.get("technical_failures"):
        errors.append("OPTIMIZATION_AUTHORITY_TECHNICAL_FAILURES_PRESENT")
    ledger_sha = _sha256_file(ledger_path)
    if not ledger_sha or str(manifest.get("ledger_sha256") or "").lower() != ledger_sha:
        errors.append("OPTIMIZATION_AUTHORITY_LEDGER_SHA256_MISMATCH")
    if not isinstance(manifest.get("authority_chain_receipt"), Mapping):
        errors.append("OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_MISSING")

    index_paths, index_errors = _resolve_sparse_authority_indexes(
        manifest, manifest_path
    )
    errors.extend(index_errors)
    if errors:
        return None, list(dict.fromkeys(errors))

    try:
        from pm_optimization_authority import verify_live_readiness_authority_chain
    except Exception:
        return None, [
            "CURRENT_REFIT_RAW_VERIFIER_UNAVAILABLE",
            "LIVE_AUTHORITY_CHAIN_VERIFIER_UNAVAILABLE",
        ]

    try:
        result = verify_live_readiness_authority_chain(
            manifest=dict(manifest),
            manifest_path=manifest_path,
            authority_index_paths=index_paths,
            ledger=ledger,
            ledger_path=ledger_path,
            config=config,
            config_payload=dict(config_payload),
            config_path=Path(config_path).resolve(),
            requested_symbols=tuple(requested_symbols),
            native_universe=native_universe,
            validation_time_utc=validation_time_utc,
            require_funded_authorization=True,
        )
    except Exception as exc:
        return None, [f"LIVE_AUTHORITY_CHAIN_REVERIFY_FAILED:{type(exc).__name__}"]

    required_result_keys = {
        "schema_version",
        "status",
        "errors",
        "raw_reverification",
        "selected_cell_ids",
        "possible_slot_count",
        "source_unavailable_cell_ids",
        "regime_unobserved_cell_ids",
        "no_local_winner_cell_ids",
        "insufficient_evidence_cell_ids",
        "technical_cell_ids",
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "ledger_sha256",
        "selected_capital_envelope",
        "capital_config_matches",
        "capital_execution_enforcement_reverified",
        "funded_live_authorized",
    }
    if not isinstance(result, Mapping) or set(result) != required_result_keys:
        return None, ["LIVE_AUTHORITY_CHAIN_RESULT_SCHEMA_INVALID"]
    normalized = dict(result)
    if normalized.get("schema_version") != SPARSE_AUTHORITY_RESULT_SCHEMA:
        errors.append("LIVE_AUTHORITY_CHAIN_RESULT_VERSION_INVALID")
    reported_errors = normalized.get("errors")
    if not isinstance(reported_errors, list) or any(
        not isinstance(value, str) or not value for value in reported_errors
    ):
        errors.append("LIVE_AUTHORITY_CHAIN_ERROR_LIST_INVALID")
    else:
        errors.extend(reported_errors)
    raw = normalized.get("raw_reverification")
    if (
        not isinstance(raw, Mapping)
        or set(raw) != set(SPARSE_AUTHORITY_RAW_REVERIFICATION)
        or any(
            raw.get(name) is not True for name in SPARSE_AUTHORITY_RAW_REVERIFICATION
        )
    ):
        errors.append("LIVE_AUTHORITY_RAW_REVERIFICATION_INCOMPLETE")
        if (
            not isinstance(raw, Mapping)
            or raw.get("current_refit_execution") is not True
        ):
            errors.append("CURRENT_REFIT_RAW_EXECUTION_UNVERIFIED")

    expected_slots = len(requested_symbols) * 6 * 4
    possible_cells = {
        f"{symbol}|{timeframe}|{regime}"
        for symbol in requested_symbols
        for timeframe in ("M5", "M15", "M30", "H1", "H4", "D1")
        for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
    }
    if normalized.get("possible_slot_count") != expected_slots:
        errors.append("LIVE_AUTHORITY_POSSIBLE_SLOT_ACCOUNTING_INVALID")
    validated_rosters: Dict[str, Set[str]] = {}
    reported_selected = normalized.get("selected_cell_ids")
    if (
        not isinstance(reported_selected, list)
        or any(not isinstance(value, str) for value in reported_selected)
        or len(reported_selected) != len(set(reported_selected))
        or reported_selected != sorted(reported_selected)
    ):
        errors.append("LIVE_AUTHORITY_SELECTED_CURRENT_REFIT_ROSTER_MISMATCH")
        validated_rosters["selected_cell_ids"] = set()
    else:
        reported_selected_set = set(reported_selected)
        validated_rosters["selected_cell_ids"] = reported_selected_set
        if reported_selected_set != selected_cells:
            errors.append("LIVE_AUTHORITY_SELECTED_CURRENT_REFIT_ROSTER_MISMATCH")
        if not reported_selected_set.issubset(possible_cells):
            errors.append("LIVE_AUTHORITY_CELL_OUTSIDE_POSSIBLE_BOOK:selected_cell_ids")
    for roster_field in (
        "source_unavailable_cell_ids",
        "regime_unobserved_cell_ids",
        "no_local_winner_cell_ids",
        "insufficient_evidence_cell_ids",
        "technical_cell_ids",
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
    ):
        values = normalized.get(roster_field)
        if (
            not isinstance(values, list)
            or any(not isinstance(value, str) for value in values)
            or len(values) != len(set(values))
            or values != sorted(values)
        ):
            errors.append(f"LIVE_AUTHORITY_CELL_ROSTER_INVALID:{roster_field}")
            validated_rosters[roster_field] = set()
        else:
            value_set = set(values)
            validated_rosters[roster_field] = value_set
            if not value_set.issubset(possible_cells):
                errors.append(
                    f"LIVE_AUTHORITY_CELL_OUTSIDE_POSSIBLE_BOOK:{roster_field}"
                )
    partition_fields = (
        "selected_cell_ids",
        "source_unavailable_cell_ids",
        "regime_unobserved_cell_ids",
        "no_local_winner_cell_ids",
        "insufficient_evidence_cell_ids",
        "technical_cell_ids",
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
    )
    partition = {name: validated_rosters.get(name, set()) for name in partition_fields}
    seen: Set[str] = set()
    for name in partition_fields:
        if seen & partition[name]:
            errors.append(f"LIVE_AUTHORITY_CELL_PARTITION_OVERLAP:{name}")
        seen.update(partition[name])
    if seen != possible_cells:
        errors.append("LIVE_AUTHORITY_CELL_PARTITION_INCOMPLETE")
    # A valid authority result can only place the exact activation-authenticated
    # terminal-technical roster here.  Those cells made zero search attempts and
    # are complete non-economic abstentions; they must not veto valid sibling
    # cells.  Reconcile the roster again at this final boundary so a forged
    # authority result cannot relabel an ordinary technical failure.
    try:
        expected_terminal_technical = {
            f"{symbol}|{timeframe}|{regime}"
            for symbol in requested_symbols
            for timeframe in ("M5", "M15", "M30", "H1", "H4", "D1")
            if native_universe.cell_status(symbol, timeframe) == "technical_error"
            for regime in ("TREND", "RANGE", "BREAKOUT", "CHOP")
        }
    except Exception:
        expected_terminal_technical = set()
        errors.append("LIVE_AUTHORITY_NATIVE_TERMINAL_TECHNICAL_ROSTER_UNAVAILABLE")
    if partition["technical_cell_ids"] != expected_terminal_technical:
        errors.append("LIVE_AUTHORITY_NATIVE_TERMINAL_TECHNICAL_ROSTER_MISMATCH")
    for roster_field in (
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
    ):
        if normalized.get(roster_field):
            errors.append(f"LIVE_AUTHORITY_BLOCKING_CELLS_PRESENT:{roster_field}")
    if normalized.get("native_activation_id") != getattr(
        native_universe, "activation_id", None
    ) or normalized.get("native_activation_manifest_sha256") != getattr(
        native_universe, "activation_manifest_sha256", None
    ):
        errors.append("LIVE_AUTHORITY_NATIVE_ACTIVATION_MISMATCH")
    if normalized.get("ledger_sha256") != ledger_sha:
        errors.append("LIVE_AUTHORITY_LEDGER_SHA256_MISMATCH")
    envelope = normalized.get("selected_capital_envelope")
    envelope_keys = {
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
    }
    if not isinstance(envelope, Mapping) or set(envelope) != envelope_keys:
        errors.append("LIVE_AUTHORITY_CAPITAL_ENVELOPE_INVALID")
    else:
        # The signed/replay-selected envelope is admission truth; static config
        # only constrained roster construction upstream and must not re-veto a
        # precommitted selection here (seq1644/1684).  Full envelope internal
        # coherence (margin/free reciprocals, hierarchy) is already enforced
        # upstream via capital_config_matches (_capital_config_errors); here we
        # require only the schema (above) and positive axes, never equality with
        # static config fields.
        for name in envelope_keys:
            if _to_float(envelope.get(name), -2.0) <= 0.0:
                errors.append(f"LIVE_AUTHORITY_CAPITAL_ENVELOPE_INVALID:{name}")
    if normalized.get("capital_config_matches") is not True:
        errors.append("LIVE_AUTHORITY_CAPITAL_CONFIG_MISMATCH")
    if normalized.get("capital_execution_enforcement_reverified") is not True:
        errors.append("LIVE_AUTHORITY_CAPITAL_EXECUTION_ENFORCEMENT_UNVERIFIED")
    if (
        normalized.get("status") != "FUNDED_AUTHORIZED"
        or normalized.get("funded_live_authorized") is not True
    ):
        errors.append("FUNDED_LIVE_AUTHORITY_NOT_GRANTED")
    return normalized, list(dict.fromkeys(errors))


def _resolve_relative_to_config(path_raw: Any, config_path: str) -> Path:
    path = Path(str(path_raw))
    if path.is_absolute():
        return path
    return Path(config_path).resolve().parent / path


def _broker_specs_by_symbol(specs: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for key, payload in specs.items():
        if not isinstance(payload, dict):
            continue
        for candidate in (key, payload.get("symbol"), payload.get("broker_symbol")):
            if str(candidate or "").strip():
                mapping[str(candidate).upper()] = payload
    return mapping


def _broker_spec_field_issues(
    specs: Dict[str, Any], symbols: Iterable[str]
) -> List[str]:
    required_positive = (
        "point",
        "tick_size",
        "tick_value",
        "contract_size",
        "volume_min",
        "volume_max",
        "volume_step",
    )
    issues: List[str] = []
    spec_by_symbol = _broker_specs_by_symbol(specs)
    for symbol in symbols:
        payload = spec_by_symbol.get(str(symbol).upper())
        if not isinstance(payload, dict):
            continue
        if payload.get("digits") is None:
            issues.append(f"{symbol}: missing digits")
        else:
            try:
                if int(payload.get("digits")) < 0:
                    issues.append(f"{symbol}: invalid digits={payload.get('digits')}")
            except Exception:
                issues.append(f"{symbol}: invalid digits={payload.get('digits')}")
        if payload.get("spread") is None:
            issues.append(f"{symbol}: missing spread")
        else:
            try:
                if float(payload.get("spread")) < 0.0:
                    issues.append(f"{symbol}: invalid spread={payload.get('spread')}")
            except Exception:
                issues.append(f"{symbol}: invalid spread={payload.get('spread')}")
        for field_name in required_positive:
            try:
                value = float(payload.get(field_name, 0.0) or 0.0)
            except Exception:
                value = 0.0
            if value <= 0.0:
                issues.append(
                    f"{symbol}: invalid {field_name}={payload.get(field_name)}"
                )
    return issues


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return (
        out
        if out == out and out not in (float("inf"), float("-inf"))
        else float(default)
    )


def _position_slippage_buffer_issues(
    position_config: Any,
) -> List[Tuple[str, str]]:
    """Validate the independent funded-position C2 slippage buffer."""
    issues: List[Tuple[str, str]] = []
    if getattr(position_config, "use_slippage", None) is not True:
        issues.append(
            (
                "position_slippage_buffer_disabled",
                "the funded position slippage buffer must be enabled",
            )
        )

    raw_pips = getattr(position_config, "slippage_pips", None)
    try:
        slippage_pips = float(raw_pips)
    except (TypeError, ValueError, OverflowError):
        slippage_pips = float("nan")
    if (
        isinstance(raw_pips, bool)
        or not math.isfinite(slippage_pips)
        or slippage_pips < 0.0
    ):
        issues.append(
            (
                "position_slippage_buffer_invalid",
                "the funded position slippage buffer must be finite and nonnegative",
            )
        )
    return issues


def _pip_size_from_broker_spec(payload: Dict[str, Any]) -> float:
    point = _to_float(payload.get("point"), 0.0)
    digits_raw = payload.get("digits")
    try:
        digits = int(digits_raw)
    except Exception:
        digits = 0
    if point <= 0.0:
        return 0.0
    return point * 10.0 if digits in {3, 5} else point


def _extract_static_stop_pips(payload: Dict[str, Any]) -> Optional[float]:
    params = (
        payload.get("parameters") if isinstance(payload.get("parameters"), dict) else {}
    )
    candidates = (
        "sl_pips",
        "stop_pips",
        "stop_loss_pips",
        "fixed_sl_pips",
        "initial_sl_pips",
        "stop_distance_pips",
    )
    for key in candidates:
        if key in params:
            value = _to_float(params.get(key), 0.0)
            if value > 0.0:
                return value
        if key in payload:
            value = _to_float(payload.get(key), 0.0)
            if value > 0.0:
                return value
    metrics = (
        payload.get("val_metrics")
        if isinstance(payload.get("val_metrics"), dict)
        else {}
    )
    value = _to_float(metrics.get("avg_loss_pips"), 0.0)
    return abs(value) if value else None


def _min_lot_risk_pct_from_broker_spec(
    broker_spec: Dict[str, Any],
    *,
    stop_pips: float,
    equity: float,
) -> Optional[float]:
    if equity <= 0.0 or stop_pips <= 0.0:
        return None
    min_lot = _to_float(broker_spec.get("volume_min"), 0.0)
    if min_lot <= 0.0:
        return None
    tick_size = _to_float(broker_spec.get("tick_size"), 0.0)
    tick_value = _to_float(broker_spec.get("tick_value"), 0.0)
    pip_size = _pip_size_from_broker_spec(broker_spec)
    loss_per_lot = 0.0
    if tick_size > 0.0 and tick_value > 0.0 and pip_size > 0.0:
        loss_per_lot = (stop_pips * pip_size / tick_size) * tick_value
    elif _to_float(broker_spec.get("pip_value"), 0.0) > 0.0:
        loss_per_lot = stop_pips * _to_float(broker_spec.get("pip_value"), 0.0)
    if loss_per_lot <= 0.0:
        return None
    return (loss_per_lot * min_lot / equity) * 100.0


def _iter_tradeable_cell_payloads(ledger: Any) -> Iterable[Dict[str, Any]]:
    """Yield tradeable winner cells with symbol/timeframe/regime context."""

    def _tradeable(payload: Dict[str, Any]) -> bool:
        name = str(payload.get("strategy_name", "") or "").upper()
        meta = (
            payload.get("artifact_meta")
            if isinstance(payload.get("artifact_meta"), dict)
            else {}
        )
        status = str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper()
        return bool(
            name
            and name != "NO_TRADE"
            and not meta.get("no_trade")
            and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
        )

    for symbol_payload in _iter_symbol_configs(ledger):
        symbol = str(symbol_payload.get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        regimes = symbol_payload.get("regime_configs")
        if isinstance(regimes, dict):
            for timeframe, regime_payload in regimes.items():
                if not isinstance(regime_payload, dict):
                    continue
                for regime, payload in regime_payload.items():
                    if isinstance(payload, dict) and _tradeable(payload):
                        row = dict(payload)
                        row["symbol"] = symbol
                        row["timeframe"] = str(timeframe or "").strip().upper()
                        row["regime"] = str(regime or "").strip().upper()
                        yield row
            continue
        if _tradeable(symbol_payload):
            row = dict(symbol_payload)
            row["symbol"] = symbol
            row["timeframe"] = (
                str(symbol_payload.get("timeframe", "") or "LEGACY").strip().upper()
            )
            row["regime"] = (
                str(symbol_payload.get("regime", "") or "LEGACY").strip().upper()
            )
            yield row


def _iter_symbol_configs(ledger: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(ledger, dict):
        if isinstance(ledger.get("symbols"), dict):
            for value in ledger["symbols"].values():
                if isinstance(value, dict):
                    yield value
            return
        if isinstance(ledger.get("symbol_configs"), dict):
            for value in ledger["symbol_configs"].values():
                if isinstance(value, dict):
                    yield value
            return
        if all(isinstance(value, dict) for value in ledger.values()):
            for value in ledger.values():
                yield value
            return
    if isinstance(ledger, list):
        for value in ledger:
            if isinstance(value, dict):
                yield value


def _config_has_tradeable_winner(config: Dict[str, Any]) -> bool:
    def _tradeable(payload: Dict[str, Any]) -> bool:
        name = str(payload.get("strategy_name", "") or "").upper()
        meta = (
            payload.get("artifact_meta")
            if isinstance(payload.get("artifact_meta"), dict)
            else {}
        )
        status = str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper()
        non_live_statuses = {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
        return bool(
            name
            and name != "NO_TRADE"
            and not meta.get("no_trade")
            and status not in non_live_statuses
        )

    has_nested_configs = False
    if isinstance(config.get("default_config"), dict) and _tradeable(
        config["default_config"]
    ):
        return True
    if isinstance(config.get("default_config"), dict):
        has_nested_configs = True
    regimes = config.get("regime_configs")
    if isinstance(regimes, dict):
        has_nested_configs = True
        for tf_payload in regimes.values():
            if not isinstance(tf_payload, dict):
                continue
            for regime_payload in tf_payload.values():
                if isinstance(regime_payload, dict) and _tradeable(regime_payload):
                    return True
    if has_nested_configs:
        return False
    return _tradeable(config)


def _tradeable_cell_ids_from_ledger(ledger: Any) -> Set[str]:
    """Return deployable SYMBOL|TIMEFRAME|REGIME cells from a winner ledger."""

    def _tradeable(payload: Dict[str, Any]) -> bool:
        name = str(payload.get("strategy_name", "") or "").upper()
        meta = (
            payload.get("artifact_meta")
            if isinstance(payload.get("artifact_meta"), dict)
            else {}
        )
        status = str(payload.get("promotion_status", "BLOCKED") or "BLOCKED").upper()
        return bool(
            name
            and name != "NO_TRADE"
            and not meta.get("no_trade")
            and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
        )

    cells: Set[str] = set()
    for symbol_payload in _iter_symbol_configs(ledger):
        symbol = str(symbol_payload.get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        regimes = symbol_payload.get("regime_configs")
        if isinstance(regimes, dict):
            for timeframe, regime_payload in regimes.items():
                tf = str(timeframe or "").strip().upper()
                if not isinstance(regime_payload, dict) or not tf:
                    continue
                for regime, payload in regime_payload.items():
                    rg = str(regime or "").strip().upper()
                    if isinstance(payload, dict) and rg and _tradeable(payload):
                        cells.add(f"{symbol}|{tf}|{rg}")
            continue
        if _tradeable(symbol_payload):
            tf = (
                str(symbol_payload.get("timeframe", "") or "").strip().upper()
                or "LEGACY"
            )
            cells.add(f"{symbol}|{tf}|LEGACY")
    return cells


def _cells_missing_tuned_regime_params(
    cells: Iterable[str], regime_params_file: str
) -> Set[str]:
    """Return deployable regime cells that lack explicit tuned regime params."""
    try:
        from pm_regime import has_tuned_regime_params
    except Exception:
        return {str(cell) for cell in cells}

    missing: Set[str] = set()
    for cell in cells:
        parts = str(cell or "").split("|")
        if len(parts) != 3:
            missing.add(str(cell))
            continue
        symbol, timeframe, _regime = (part.strip().upper() for part in parts)
        if not symbol or not timeframe:
            missing.add(str(cell))
            continue
        if timeframe == "LEGACY":
            continue
        if not has_tuned_regime_params(
            symbol,
            timeframe,
            regime_params_file,
            require_provenance=True,
        ):
            missing.add(f"{symbol}|{timeframe}|{_regime}")
    return missing


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not str(value or "").strip():
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _ledger_live_expiry_issues(
    ledger: Any, now: datetime, grace_minutes: int = 0
) -> List[str]:
    issues: List[str] = []
    grace = timedelta(minutes=max(0, int(grace_minutes or 0)))
    for symbol_payload in _iter_symbol_configs(ledger):
        if not _config_has_tradeable_winner(symbol_payload):
            continue
        symbol = _symbol_label(symbol_payload)
        valid_until = _parse_iso_datetime(symbol_payload.get("valid_until"))
        if valid_until is None:
            issues.append(f"{symbol}: no expiry date")
            continue
        selected_cells = tuple(
            _iter_tradeable_cell_payloads({symbol: symbol_payload})
        )
        if selected_cells and (
            valid_until.tzinfo is None or valid_until.utcoffset() != timedelta(0)
        ):
            issues.append(f"{symbol}: expiry is not aware UTC")
            continue
        for cell in selected_cells:
            cell_id = "|".join(
                (
                    symbol,
                    str(cell.get("timeframe") or "").upper(),
                    str(cell.get("regime") or "").upper(),
                )
            )
            cell_valid_until = _parse_iso_datetime(cell.get("valid_until"))
            if cell_valid_until is None:
                issues.append(f"{cell_id}: no expiry date")
            elif (
                cell_valid_until.tzinfo is None
                or cell_valid_until.utcoffset() != timedelta(0)
            ):
                issues.append(f"{cell_id}: expiry is not aware UTC")
            elif cell_valid_until != valid_until:
                issues.append(f"{cell_id}: expiry does not match symbol deadline")
        probe = now
        if valid_until.tzinfo is not None and probe.tzinfo is None:
            # Interpret a naive runtime clock in the host's local timezone,
            # then compare on the artifact's aware timeline.
            probe = probe.astimezone(valid_until.tzinfo)
        elif valid_until.tzinfo is None and probe.tzinfo is not None:
            # Legacy ledgers used naive local timestamps; preserve that
            # historical convention without raising on mixed datetime types.
            probe = probe.replace(tzinfo=None)
        if probe >= valid_until + grace:
            issues.append(
                f"{symbol}: expired at {valid_until.strftime('%Y-%m-%d %H:%M')}"
            )
    return issues


def _promote_cell_ids_from_review(review_payload: Any) -> Set[str]:
    cells: Set[str] = set()
    if not isinstance(review_payload, dict):
        return cells
    for row in review_payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("final_verdict", "") or "").upper() != "PROMOTE":
            continue
        symbol = str(row.get("symbol", "") or "").strip().upper()
        timeframe = str(row.get("timeframe", "") or "").strip().upper()
        regime = str(row.get("regime", "") or "").strip().upper()
        if symbol and timeframe and regime:
            cells.add(f"{symbol}|{timeframe}|{regime}")
    return cells


def _preview_set(values: Iterable[str], limit: int = 12) -> str:
    ordered = sorted(str(value) for value in values)
    preview = ", ".join(ordered[:limit])
    if len(ordered) > limit:
        preview += f", +{len(ordered) - limit} more"
    return preview


def _symbol_label(config: Dict[str, Any], fallback: str = "unknown") -> str:
    value = str(config.get("symbol", "") or "").strip().upper()
    return value or fallback


def _quarter_percent_units(value: Any) -> Optional[int]:
    """Return exact 0.25%-units in the solvent structural range, else none."""

    if type(value) is int:
        if value <= 0 or value >= 100:
            return None
        return value * 4
    if type(value) is not float or not math.isfinite(value):
        return None
    if not 0.0 < value < 100.0:
        return None
    numerator, denominator = value.as_integer_ratio()
    scaled_numerator = numerator * 4
    if scaled_numerator % denominator:
        return None
    units = scaled_numerator // denominator
    return units if 1 <= units <= 399 else None


def _capital_stop_risk_config_issues(
    total_stop_cap: Any, factor_stop_cap: Any
) -> List[ReadinessFinding]:
    """Validate representation only; evidence, not this gate, selects the caps."""

    findings: List[ReadinessFinding] = []
    total_units = _quarter_percent_units(total_stop_cap)
    if total_units is None:
        findings.append(
            ReadinessFinding(
                severity="error",
                code="total_stop_risk_cap_contract_mismatch",
                message=(
                    "configured total initial-stop risk must be finite, positive, "
                    "strictly below 100% of equity, and aligned to the 0.25% "
                    "per-trade risk quantum; a separate reviewed capital authority "
                    "must select and match the numeric value"
                ),
            )
        )

    factor_units = _quarter_percent_units(factor_stop_cap)
    if factor_units is None or total_units is None or factor_units > total_units:
        findings.append(
            ReadinessFinding(
                severity="error",
                code="factor_group_stop_risk_cap_contract_mismatch",
                message=(
                    "configured gross factor-group stop risk must be finite, "
                    "positive, 0.25%-aligned, and no greater than total stop risk; "
                    "a separate reviewed capital authority must select and match it"
                ),
            )
        )
    return findings


def _capital_numeric_authority_available() -> bool:
    """Return whether the reviewed capital module exposes numeric authority."""

    try:
        from pm_cap_calibration import (
            CURRENT_BOOK_ADAPTER_CONTRACT_VERSION,
            current_book_authority_adapter_contract,
        )

        contract = current_book_authority_adapter_contract()
    except Exception:
        return False
    return bool(
        type(contract) is dict
        and set(contract)
        == {
            "schema_version",
            "implementation_status",
            "numeric_cap_authority",
            "required_inputs",
        }
        and contract["schema_version"] == CURRENT_BOOK_ADAPTER_CONTRACT_VERSION
        and contract["implementation_status"] == "AVAILABLE_AT_SIGNED_V8_BOUNDARY"
        and contract["numeric_cap_authority"] is True
        and contract["required_inputs"]
        == [
            "signed_precommitment",
            "signed_complete_attempt_ledger",
            "exact_current_book_and_refit_lineage",
            "exact_cost_native_replay_margin_lineage",
            "external_capital_authorization_for_live_use",
        ]
    )


def _validate_legacy_live_readiness(
    config: Any,
    *,
    symbols: Iterable[str],
    config_path: str = "config.json",
    winner_ledger_path: Optional[str] = None,
    mt5_config: Optional[Any] = None,
    account_info: Optional[Any] = None,
    position_config: Optional[Any] = None,
) -> ReadinessReport:
    findings: List[ReadinessFinding] = []
    synchronized_portfolio_evidence_valid = False

    def add(severity: str, code: str, message: str) -> None:
        findings.append(
            ReadinessFinding(
                severity=severity,
                code=code,
                message=message,
                scope=_readiness_scope(code),
            )
        )

    def require(condition: bool, code: str, message: str) -> None:
        if not condition:
            add("error", code, message)

    def number(name: str, default: float = 0.0) -> float:
        try:
            value = float(getattr(config, name, default))
        except (TypeError, ValueError, OverflowError):
            return default
        return value if math.isfinite(value) else default

    # One funded-production contract. Alternative modes and feature switches
    # remain available for research, but none may weaken this launch gate.
    require(
        not bool(getattr(config, "research_mode", False)),
        "research_mode_not_live",
        "research_mode must be false for funded live trading",
    )
    require(
        str(getattr(config, "production_split_mode", "") or "").lower()
        == "calendar_lockbox_v2",
        "production_split_not_calendar_lockbox",
        "funded live requires production_split_mode='calendar_lockbox_v2'",
    )
    configured_timeframes = [
        str(value or "").strip().upper()
        for value in (getattr(config, "timeframes", []) or [])
        if str(value or "").strip()
    ]
    required_timeframes = ["M5", "M15", "M30", "H1", "H4", "D1"]
    require(
        configured_timeframes == required_timeframes,
        "production_timeframe_contract_mismatch",
        f"funded live requires exactly {required_timeframes}, got {configured_timeframes}",
    )
    for field_name in (
        "evidence_sufficiency_enabled",
        "evidence_require_native_timeframes",
        "evidence_require_native_provenance",
        "evidence_require_session_coverage",
    ):
        require(
            bool(getattr(config, field_name, False)),
            f"{field_name}_required",
            f"{field_name} must be enabled for funded live",
        )
    require(
        math.isclose(
            number("evidence_min_expected_session_coverage"),
            0.95,
            rel_tol=0.0,
            abs_tol=1e-12,
        ),
        "native_session_coverage_policy_mismatch",
        "evidence_min_expected_session_coverage must equal the pinned 0.95 policy",
    )
    require(
        number("evidence_purge_days") >= 7.0,
        "calendar_purge_too_short",
        "calendar evidence requires at least a seven-day purge between search and verdict windows",
    )
    window_years = dict(getattr(config, "evidence_window_years", {}) or {})
    required_windows = {
        "optimizer_train": 5,
        "tuning_validation": 2,
        "final_lockbox": 2,
    }
    require(
        all(
            int(window_years.get(name, 0) or 0) >= years
            for name, years in required_windows.items()
        ),
        "calendar_window_contract_too_short",
        f"evidence_window_years must meet or exceed {required_windows}",
    )
    require(
        bool(getattr(config, "false_discovery_control_enabled", False)),
        "false_discovery_control_required",
        "false-discovery control must be enabled for funded live",
    )
    require(
        number("false_discovery_min_dsr") >= 0.95,
        "deflated_sharpe_threshold_too_low",
        "false_discovery_min_dsr must be at least 0.95",
    )
    require(
        number("false_discovery_min_daily_observations") >= 252,
        "daily_observation_floor_too_low",
        "at least 252 daily observations are required for promotion statistics",
    )
    require(
        number("false_discovery_min_effective_observations") >= 60.0,
        "effective_observation_floor_too_low",
        "at least 60 effective daily observations are required",
    )
    require(
        number("false_discovery_max_pbo", 1.0) <= 0.50,
        "pbo_threshold_too_permissive",
        "false_discovery_max_pbo must be no greater than 0.50",
    )
    require(
        int(number("false_discovery_pbo_min_candidates")) >= 5,
        "pbo_candidate_floor_too_low",
        "aligned PBO requires at least five candidates",
    )
    require(
        bool(getattr(config, "false_discovery_require_bootstrap", False)),
        "bootstrap_evidence_required",
        "moving-block bootstrap evidence must be required",
    )
    require(
        int(number("false_discovery_bootstrap_repetitions")) >= 300,
        "bootstrap_repetitions_too_low",
        "at least 300 bootstrap repetitions are required",
    )
    require(
        str(getattr(config, "false_discovery_holdout_policy", "") or "").lower()
        == "block",
        "lockbox_policy_not_blocking",
        "false_discovery_holdout_policy must be 'block'",
    )
    require(
        str(getattr(config, "oos_validation_mode", "") or "").lower()
        == "fixed_holdout",
        "final_lockbox_bypassed",
        "oos_validation_mode must be 'fixed_holdout'; purged k-fold cannot replace the final lockbox",
    )
    require(
        bool(getattr(config, "lockbox_one_shot_registry_enabled", False)),
        "lockbox_one_shot_registry_required",
        "one-shot lockbox access registry must be enabled",
    )
    require(
        bool(getattr(config, "oos_evidence_export_enabled", False)),
        "oos_evidence_export_required",
        "candidate OOS evidence export must be enabled",
    )
    require(
        bool(getattr(config, "oos_evidence_include_execution_cost_survival", False)),
        "execution_cost_survival_required",
        "execution-cost survival evidence must be enabled",
    )
    for field_name in ("use_spread", "swap_model_enabled"):
        require(
            bool(getattr(config, field_name, False)),
            f"{field_name}_required",
            f"{field_name} cannot be disabled in funded-production evidence",
        )
    require(
        bool(getattr(config, "production_require_promotion_gate", False)),
        "production_promotion_gate_required",
        "an explicit production promotion gate is required",
    )
    require(
        bool(getattr(config, "production_replay_audit_enabled", False))
        and bool(getattr(config, "production_replay_audit_block_unverified", False))
        and bool(getattr(config, "production_replay_audit_fail_on_error", False)),
        "production_replay_audit_not_fail_closed",
        "production replay must be enabled, block unverified cells, and fail on errors",
    )
    require(
        bool(getattr(config, "production_portfolio_replay_evidence_required", False)),
        "synchronized_portfolio_evidence_not_required",
        "funded live must require sealed synchronized shared-capital replay evidence",
    )
    require(
        int(number("production_portfolio_replay_min_calendar_days")) >= 730,
        "synchronized_portfolio_horizon_too_short",
        "synchronized portfolio replay must cover at least 730 common final-lockbox calendar days",
    )
    require(
        int(number("production_portfolio_replay_min_positions")) >= 100,
        "synchronized_portfolio_position_floor_too_low",
        "synchronized portfolio replay must require at least 100 accepted positions per capital scenario",
    )
    require(
        number("production_portfolio_replay_min_profit_factor") >= 1.05,
        "synchronized_portfolio_profit_factor_too_low",
        "synchronized portfolio replay profit-factor threshold must be at least 1.05",
    )
    require(
        number("production_portfolio_replay_min_return_pct", -1.0) >= 0.0,
        "synchronized_portfolio_return_floor_negative",
        "synchronized portfolio replay may not pass a negative net return",
    )
    require(
        0.0 < number("production_portfolio_replay_max_drawdown_pct", 100.0) <= 20.0,
        "synchronized_portfolio_drawdown_cap_too_high",
        "synchronized portfolio replay MTM drawdown cap must be positive and no greater than 20%",
    )
    require(
        bool(getattr(config, "production_portfolio_replay_require_mtm", False)),
        "synchronized_portfolio_mtm_not_required",
        "funded replay evidence must require complete mark-to-market paths",
    )
    require(
        bool(getattr(config, "production_require_full_symbol_ledger", False)),
        "full_symbol_ledger_required",
        "the funded profile requires an explicit ledger entry for every requested symbol",
    )
    require(
        not bool(getattr(config, "live_allow_partial_symbol_ledger", False)),
        "partial_symbol_ledger_not_live",
        "live_allow_partial_symbol_ledger must be false",
    )
    require(
        str(getattr(config, "hard_block_reopen_mode", "") or "").lower()
        == "when_clear",
        "automatic_safety_reopen_not_live",
        "funded hard breakers must rearm only after fresh loss/drawdown metrics clear",
    )
    require(
        str(getattr(config, "institutional_supervisor_mode", "") or "").lower()
        == "circuit_breaker",
        "supervisor_mode_not_circuit_breaker",
        "funded live requires institutional_supervisor_mode='circuit_breaker'",
    )
    require(
        str(getattr(config, "portfolio_control_policy", "") or "").lower()
        == "hard_breakpoints_only",
        "portfolio_control_policy_not_hard_breakpoints",
        "funded live requires portfolio_control_policy='hard_breakpoints_only'",
    )
    funded_breaker_thresholds = {
        "session_loss_hard_block_pct": 1.5,
        "daily_loss_hard_block_pct": 2.0,
        "weekly_loss_hard_block_pct": 5.0,
        "open_drawdown_hard_block_pct": 3.0,
    }
    breaker_threshold_mismatches = {
        name: {"expected": expected, "observed": number(name)}
        for name, expected in funded_breaker_thresholds.items()
        if abs(number(name) - expected) > 1e-12
    }
    require(
        not breaker_threshold_mismatches,
        "account_breaker_threshold_contract_mismatch",
        "funded account breakers must be session/daily/weekly/open-drawdown "
        "= 1.5/2.0/5.0/3.0% exactly; mismatches="
        f"{breaker_threshold_mismatches}",
    )
    require(
        bool(getattr(config, "live_outcome_feedback_enabled", False))
        and bool(getattr(config, "live_outcome_require_position_history", False))
        and bool(getattr(config, "live_outcome_feed_cycle_drift", False)),
        "live_outcome_feedback_required",
        "durable close accounting and cycle-drift feedback are required",
    )
    require(
        bool(getattr(config, "cycle_drift_monitor_enabled", False)),
        "cycle_drift_monitor_required",
        "cycle drift monitor must be enabled",
    )
    require(
        bool(getattr(config, "probation_enabled", False))
        and int(number("probation_min_live_outcomes")) >= 30
        and number("probation_min_mean_r", -1.0) >= 0.0
        and number("probation_min_profit_factor") >= 1.0,
        "forward_probation_contract_too_weak",
        "probation must require at least 30 exact live outcomes, positive mean R, and PF >= 1",
    )
    require(
        str(getattr(config, "local_governance_live_mode", "off") or "off").lower()
        in {"off", "shadow"},
        "governance_overlay_unproven",
        "order-governance overlays may be off or shadow-only until lockbox-tested",
    )
    require(
        not bool(getattr(config, "local_governance_tournament_enabled", False))
        and not bool(getattr(config, "risk_management_optimization_enabled", False)),
        "post_lockbox_policy_search_not_live",
        "funded artifacts must not select governance/risk policies after lockbox evaluation",
    )
    require(
        str(getattr(config, "live_risk_scalars_mode", "off") or "off").lower()
        in {"off", "shadow"},
        "risk_scalar_overlay_unproven",
        "live risk scalars may be off or shadow-only until independently validated",
    )
    require(
        str(getattr(config, "market_driven_exit_pack_mode", "off") or "off").lower()
        in {"off", "paper"},
        "market_exit_overlay_unproven",
        "market-driven exit pack may be off or paper-only until independently validated",
    )
    total_stop_cap = getattr(config, "max_total_open_risk_pct", None)
    factor_stop_cap = getattr(config, "max_factor_group_open_risk_pct", None)
    findings.extend(_capital_stop_risk_config_issues(total_stop_cap, factor_stop_cap))
    require(
        number("max_new_entries_when_total_open_risk_above_pct") == 0.0
        and str(getattr(config, "open_risk_action", "") or "").lower() == "observe",
        "soft_open_risk_gate_not_neutral",
        "funded allocation uses one frozen hard stop-risk cap; the soft threshold must be disabled at 0 with observe",
    )
    margin_reopen = number("margin_reopen_level")
    margin_block = number("margin_entry_block_level")
    margin_recovery = number("margin_recovery_start_level")
    margin_panic = number("margin_panic_level")
    require(
        margin_reopen >= margin_block > margin_recovery > margin_panic > 0.0,
        "margin_threshold_order_invalid",
        "margin thresholds must satisfy reopen >= entry_block > recovery_start > panic > 0",
    )
    if position_config is not None:
        for code, message in _position_slippage_buffer_issues(position_config):
            add("error", code, message)
        require(
            not bool(getattr(position_config, "auto_widen_sl", False)),
            "auto_widen_sl_not_live",
            "funded live must reject broker-incompatible frozen stops instead of widening them",
        )
        require(
            not bool(getattr(position_config, "allow_min_lot_risk_clamp", False)),
            "min_lot_risk_clamp_not_live",
            "funded live must skip unsafe minimum lots instead of clamping upward",
        )
        tp_multipliers = dict(
            getattr(position_config, "regime_tp_multipliers", {}) or {}
        )
        require(
            set(tp_multipliers) == {"TREND", "RANGE", "BREAKOUT", "CHOP"}
            and all(
                abs(float(value) - 1.0) <= 1e-12 for value in tp_multipliers.values()
            ),
            "regime_tp_overlay_unproven",
            "regime TP multipliers must remain neutral (1.0) until lockbox-tested",
        )

    if (
        str(getattr(config, "live_artifact_drift_policy", "block") or "").lower()
        != "block"
    ):
        add(
            "error",
            "artifact_drift_not_blocking",
            "live_artifact_drift_policy must be 'block' for live trading",
        )

    manual_entries = bool(getattr(config, "manual_new_entries_enabled", True))
    if not manual_entries:
        add(
            "error",
            "manual_new_entries_disabled",
            "manual_new_entries_enabled is false; live entry startup is intentionally blocked",
        )
    manual_path_raw = str(getattr(config, "manual_kill_switch_path", "") or "").strip()
    if manual_path_raw:
        manual_path = _resolve_relative_to_config(manual_path_raw, config_path)
        if manual_path.exists():
            try:
                content = manual_path.read_text(encoding="utf-8").strip().lower()
            except Exception:
                content = "unreadable"
            if content in {
                "0",
                "false",
                "off",
                "block",
                "blocked",
                "pause",
                "paused",
                "kill",
            }:
                add(
                    "error",
                    "manual_kill_switch_active",
                    f"manual kill switch blocks new entries: {manual_path}",
                )

    if mt5_config is not None:
        try:
            expected_login = int(getattr(mt5_config, "expected_login", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            expected_login = 0
        expected_server = str(getattr(mt5_config, "expected_server", "") or "").strip()
        expected_currency = str(
            getattr(mt5_config, "expected_currency", "") or ""
        ).strip()
        identity_fully_pinned = bool(
            expected_login > 0 and expected_server and expected_currency
        )
        if not identity_fully_pinned:
            add(
                "error",
                "mt5_account_identity_not_pinned",
                "funded live trading requires a nonzero mt5.expected_login plus "
                "nonempty expected_server and expected_currency pins",
            )
        if identity_fully_pinned:
            if account_info is None:
                add(
                    "error",
                    "mt5_account_identity_unverified",
                    "expected MT5 account identity is configured but account_info is unavailable",
                )
            else:
                actual_login = int(getattr(account_info, "login", 0) or 0)
                actual_server = str(getattr(account_info, "server", "") or "").strip()
                actual_currency = str(
                    getattr(account_info, "currency", "") or ""
                ).strip()
                if actual_login != expected_login:
                    add(
                        "error",
                        "mt5_account_login_mismatch",
                        f"MT5 account login mismatch: expected {expected_login}, actual {actual_login}",
                    )
                if actual_server.lower() != expected_server.lower():
                    add(
                        "error",
                        "mt5_account_server_mismatch",
                        f"MT5 server mismatch: expected {expected_server}, actual {actual_server or 'unknown'}",
                    )
                if actual_currency.upper() != expected_currency.upper():
                    add(
                        "error",
                        "mt5_account_currency_mismatch",
                        f"MT5 account currency mismatch: expected {expected_currency}, actual {actual_currency or 'unknown'}",
                    )

    min_account_equity = 0.0
    try:
        min_account_equity = float(
            getattr(config, "live_min_account_equity", 0.0) or 0.0
        )
    except (TypeError, ValueError, OverflowError):
        min_account_equity = 0.0
    if min_account_equity > 0.0:
        if account_info is None:
            add(
                "error",
                "account_equity_unverified",
                "live_min_account_equity is configured but account_info is unavailable",
            )
        else:
            try:
                actual_equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            except (TypeError, ValueError, OverflowError):
                actual_equity = 0.0
            if actual_equity < min_account_equity:
                add(
                    "error",
                    "account_equity_below_live_minimum",
                    f"connected MT5 equity {actual_equity:.2f} is below live_min_account_equity {min_account_equity:.2f}",
                )
            min_currency = str(
                getattr(config, "live_min_account_equity_currency", "") or ""
            ).strip()
            actual_currency = str(getattr(account_info, "currency", "") or "").strip()
            if (
                min_currency
                and actual_currency
                and actual_currency.upper() != min_currency.upper()
            ):
                add(
                    "error",
                    "account_equity_currency_mismatch",
                    f"connected MT5 currency {actual_currency!r} does not match live minimum currency {min_currency!r}",
                )

    if position_config is not None and account_info is not None:
        try:
            equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            risk_pct = float(getattr(position_config, "risk_per_trade_pct", 0.0) or 0.0)
            max_risk_pct = float(getattr(position_config, "max_risk_pct", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            equity = risk_pct = max_risk_pct = 0.0
        if equity > 0.0 and risk_pct > 0.0:
            risk_amount = equity * risk_pct / 100.0
            if risk_amount <= 0.0:
                add(
                    "error",
                    "account_risk_budget_invalid",
                    f"risk budget is non-positive at equity={equity:.2f}, risk_per_trade_pct={risk_pct:.4f}",
                )
            if max_risk_pct > 0.0 and risk_pct > max_risk_pct:
                add(
                    "error",
                    "risk_pct_exceeds_hard_cap",
                    f"risk_per_trade_pct {risk_pct:.4f}% exceeds max_risk_pct {max_risk_pct:.4f}%",
                )

    if not bool(getattr(config, "institutional_supervisor_enabled", True)):
        add("error", "supervisor_disabled", "institutional supervisor is disabled")
    else:
        # Check whether a previous run latched the supervisor, while preserving
        # the supervisor's manual_or_next_cycle reopen semantics.
        try:
            raw_log_dir = getattr(config, "log_dir", "logs")
            log_dir = Path(
                raw_log_dir if isinstance(raw_log_dir, (str, Path)) else "logs"
            )
            if not log_dir.is_absolute():
                log_dir = Path(config_path).resolve().parent / log_dir
            supervisor_state_path = log_dir / "institutional_supervisor_state.json"
            if supervisor_state_path.exists():
                state = _load_json_file(supervisor_state_path)
                if not isinstance(state, dict):
                    add(
                        "error",
                        "supervisor_state_unreadable",
                        f"institutional supervisor state is corrupt or unreadable: {supervisor_state_path}",
                    )
                    state = {}
                if bool(state.get("active_block")):
                    reopen_mode = (
                        str(
                            getattr(
                                config, "hard_block_reopen_mode", "manual_or_next_cycle"
                            )
                            or ""
                        )
                        .strip()
                        .lower()
                    )
                    blocked_cycle = str(state.get("cycle_id", "") or "")
                    try:
                        current_cycle = str(
                            config.get_last_retrain_slot(datetime.now()).isoformat()
                        )
                    except Exception:
                        current_cycle = ""
                    next_cycle_reopens = (
                        reopen_mode in {"manual_or_next_cycle", "next_cycle"}
                        and bool(current_cycle)
                        and bool(blocked_cycle)
                        and current_cycle != blocked_cycle
                    )
                    state_reasons = [
                        str(reason).lower()
                        for reason in (state.get("block_reasons") or [])
                    ]
                    stale_transient_latch = (
                        reopen_mode == "when_clear"
                        and bool(state_reasons)
                        and all(
                            (
                                "total_open_risk" in reason
                                or "existing_open_risk" in reason
                            )
                            for reason in state_reasons
                        )
                    )
                    if stale_transient_latch:
                        add(
                            "warning",
                            "supervisor_legacy_transient_latch_clearable",
                            "legacy capacity latch is transient and will be cleared on fresh supervisor evaluation",
                        )
                    elif next_cycle_reopens:
                        add(
                            "warning",
                            "supervisor_latch_from_previous_cycle",
                            "institutional supervisor latch belongs to a previous cycle; supervisor will reopen on evaluation",
                        )
                    else:
                        reasons = (
                            ", ".join(
                                str(r) for r in (state.get("block_reasons") or [])
                            )
                            or "unknown"
                        )
                        add(
                            "error",
                            "supervisor_latched_from_previous_run",
                            f"institutional supervisor is latched (reasons: {reasons}); reset before live trading",
                        )
        except Exception as exc:
            add(
                "error",
                "supervisor_state_check_failed",
                "institutional supervisor state could not be verified: "
                f"{type(exc).__name__}: {exc}",
            )
    if not bool(getattr(config, "walk_forward_audit_enabled", False)):
        add(
            "error",
            "walk_forward_disabled",
            "walk-forward audit is disabled; funded promotion cannot fall back to weaker evidence",
        )
    if (
        str(getattr(config, "walk_forward_promotion_policy", "") or "").strip().lower()
        != "promotion_gate"
    ):
        add(
            "error",
            "walk_forward_promotion_policy_weakened",
            "funded promotion requires walk_forward_promotion_policy='promotion_gate'",
        )
    if not bool(getattr(config, "walk_forward_live_blocking_enabled", False)):
        add(
            "error",
            "walk_forward_live_blocking_disabled",
            "funded walk-forward failures must remain live-blocking",
        )
    try:
        raw_log_dir = getattr(config, "log_dir", "logs")
        log_dir = Path(raw_log_dir if isinstance(raw_log_dir, (str, Path)) else "logs")
        drift_state_path = log_dir / "cycle_drift_state.json"
        if drift_state_path.exists() and not isinstance(
            _load_json_file(drift_state_path), dict
        ):
            add(
                "error",
                "cycle_drift_state_unreadable",
                f"cycle drift state is corrupt or unreadable: {drift_state_path}",
            )
    except Exception as exc:
        add(
            "error",
            "cycle_drift_state_check_failed",
            f"could not verify cycle drift state: {type(exc).__name__}: {exc}",
        )

    storage_floor_raw = getattr(config, "storage_pause_entries_below_free_gb", None)
    if storage_floor_raw not in (None, "", False):
        try:
            storage_floor = float(storage_floor_raw)
        except Exception:
            storage_floor = 0.0
        if storage_floor > 0.0:
            try:
                check_root = Path(config_path).resolve().parent
                free_gb = shutil.disk_usage(check_root).free / (1024**3)
                if free_gb < storage_floor:
                    add(
                        "warning",
                        "storage_free_space_below_advisory_floor",
                        f"free disk space {free_gb:.2f}GB is below storage advisory floor {storage_floor:.2f}GB",
                    )
            except Exception as exc:
                add(
                    "warning",
                    "storage_free_space_check_failed",
                    f"could not verify free disk space before launch: {type(exc).__name__}: {exc}",
                )

    regime_params_file = str(getattr(config, "regime_params_file", "") or "")
    regime_params_missing = bool(
        regime_params_file and not Path(regime_params_file).exists()
    )

    requested_symbols = sorted(
        {str(symbol).upper() for symbol in symbols if str(symbol).strip()}
    )
    broker_specs: Optional[Dict[str, Any]] = None
    symbol_pool_target = 0
    try:
        symbol_pool_target = int(
            getattr(config, "production_symbol_pool_target_count", 0) or 0
        )
    except (TypeError, ValueError, OverflowError):
        symbol_pool_target = 0
    if symbol_pool_target > 0 and len(requested_symbols) != symbol_pool_target:
        add(
            "error",
            "production_symbol_pool_count_mismatch",
            f"configured symbol pool has {len(requested_symbols)} symbols, expected {symbol_pool_target}",
        )
    config_payload = _load_json_file(Path(config_path)) or {}
    broker_specs_raw = (
        config_payload.get("broker_specs_path")
        if isinstance(config_payload, dict)
        else None
    )
    if not broker_specs_raw:
        add(
            "warning",
            "broker_specs_path_missing",
            "broker_specs_path is not configured; backtest/live contract parity cannot be verified before launch",
        )
    else:
        broker_specs_path = _resolve_relative_to_config(broker_specs_raw, config_path)
        broker_specs = _load_json_file(broker_specs_path)
        if not isinstance(broker_specs, dict) or not broker_specs:
            add(
                "error",
                "broker_specs_unreadable",
                f"broker specs file is missing, empty, or unreadable: {broker_specs_path}",
            )
        else:
            covered_symbols = set(_broker_specs_by_symbol(broker_specs))
            missing_specs = sorted(set(requested_symbols) - covered_symbols)
            if missing_specs:
                add(
                    "error",
                    "broker_specs_missing_symbols",
                    f"broker specs missing symbols: {', '.join(missing_specs)}",
                )
            field_issues = _broker_spec_field_issues(broker_specs, requested_symbols)
            if field_issues:
                preview = "; ".join(field_issues[:8])
                if len(field_issues) > 8:
                    preview += f"; +{len(field_issues) - 8} more"
                add(
                    "error",
                    "broker_specs_invalid_fields",
                    f"broker specs contain invalid execution fields: {preview}",
                )

    ledger_raw = winner_ledger_path or getattr(
        config, "winner_ledger_path", "pm_configs.json"
    )
    ledger_path = Path(str(ledger_raw))
    if not ledger_path.is_absolute():
        ledger_path = Path(config_path).resolve().parent / ledger_path
    ledger = _load_json_file(ledger_path)
    if ledger is None:
        add(
            "error",
            "winner_ledger_unreadable",
            f"winner ledger is missing or unreadable: {ledger_path}",
        )
    else:
        symbol_configs = list(_iter_symbol_configs(ledger))
        if not symbol_configs:
            add(
                "error",
                "winner_ledger_empty",
                f"winner ledger has no symbol configs: {ledger_path}",
            )
        elif not any(
            _config_has_tradeable_winner(payload) for payload in symbol_configs
        ):
            add(
                "error",
                "winner_ledger_no_tradeable_winners",
                "winner ledger contains no tradeable winners",
            )
        expiry_issues = _ledger_live_expiry_issues(
            ledger,
            datetime.now(),
            grace_minutes=int(
                getattr(config, "live_config_expiry_grace_minutes", 0) or 0
            ),
        )
        if expiry_issues:
            preview = "; ".join(expiry_issues[:8])
            if len(expiry_issues) > 8:
                preview += f"; +{len(expiry_issues) - 8} more"
            add(
                "error",
                "winner_ledger_not_live_current",
                f"winner ledger is not current for live trading: {preview}",
            )
        ledger_symbols = {
            str(payload.get("symbol", "") or "").upper()
            for payload in symbol_configs
            if str(payload.get("symbol", "") or "").strip()
        }
        missing = sorted(set(requested_symbols) - ledger_symbols)
        if missing:
            allow_partial_live = bool(
                getattr(config, "live_allow_partial_symbol_ledger", False)
            )
            severity = (
                "warning"
                if allow_partial_live
                else (
                    "error"
                    if bool(
                        getattr(config, "production_require_full_symbol_ledger", False)
                    )
                    else "warning"
                )
            )
            code = (
                "production_symbol_ledger_incomplete"
                if severity == "error"
                else "symbols_missing_from_ledger"
            )
            preview = ", ".join(missing[:20])
            if len(missing) > 20:
                preview += f", +{len(missing) - 20} more"
            add(severity, code, f"symbols missing from winner ledger: {preview}")

        # Validate only physical specialist leaves. Regime absence is decided
        # by empirical support, not by filling a four-regime grid.
        payload_by_symbol = {
            str(payload.get("symbol", "") or "").strip().upper(): payload
            for payload in symbol_configs
            if str(payload.get("symbol", "") or "").strip()
        }
        required_regimes = ("TREND", "RANGE", "BREAKOUT", "CHOP")
        required_timeframes = ("M5", "M15", "M30", "H1", "H4", "D1")
        try:
            from pm_native_activation import resolve_active_native_universe

            native_data_dir = Path(getattr(config, "data_dir", "data"))
            if not native_data_dir.is_absolute():
                native_data_dir = Path(config_path).resolve().parent / native_data_dir
            native_universe = resolve_active_native_universe(
                native_data_dir,
                requested_symbols,
                required_timeframes=required_timeframes,
                production_contract_required=True,
            )
        except Exception as exc:
            native_universe = None
            add(
                "error",
                "native_universe_eligibility_check_failed",
                f"native universe eligibility verification raised {type(exc).__name__}: {exc}",
            )
        if native_universe is not None and native_universe.explicit_contract:
            exclusion_grid_errors: List[str] = []
            for eligible_symbol in sorted(native_universe.eligible_symbols):
                payload = payload_by_symbol.get(eligible_symbol)
                artifact = (
                    payload.get("artifact_meta")
                    if isinstance(payload, dict)
                    and isinstance(payload.get("artifact_meta"), dict)
                    else {}
                )
                if artifact.get("native_exclusion_binding"):
                    exclusion_grid_errors.append(
                        f"NATIVE_ELIGIBLE_SYMBOL_HAS_STALE_EXCLUSION:{eligible_symbol}"
                    )
            if exclusion_grid_errors:
                add(
                    "error",
                    "native_exclusion_ledger_binding_invalid",
                    "eligible symbols contain stale native-exclusion bindings: "
                    f"{_preview_set(exclusion_grid_errors)}",
                )
        invalid_cells: List[str] = []
        source_unavailable_winners: List[str] = []
        missing_statuses: List[str] = []
        temporal_lineage_issues: List[str] = []
        challenger_leakage: List[str] = []
        magic_owners: Dict[int, str] = {}
        magic_collisions: List[str] = []
        for symbol in requested_symbols:
            symbol_payload = payload_by_symbol.get(symbol)
            if not isinstance(symbol_payload, dict):
                continue
            challenger_book = symbol_payload.get("edge_challenger_book")
            if isinstance(challenger_book, dict):
                if bool(challenger_book.get("funded_role_eligible", False)):
                    challenger_leakage.append(f"{symbol}|book_marked_funded")
                challenger_grid = challenger_book.get("regime_configs")
                if isinstance(challenger_grid, dict):
                    for challenger_tf, challenger_regimes in challenger_grid.items():
                        if not isinstance(challenger_regimes, dict):
                            continue
                        for (
                            challenger_regime,
                            challenger_cell,
                        ) in challenger_regimes.items():
                            if not isinstance(challenger_cell, dict):
                                continue
                            challenger_status = str(
                                challenger_cell.get("promotion_status", "BLOCKED")
                                or "BLOCKED"
                            ).upper()
                            if challenger_status not in {
                                "BLOCKED",
                                "PAPER_ONLY",
                                "NO_TRADE",
                            }:
                                challenger_leakage.append(
                                    f"{symbol}|{challenger_tf}|{challenger_regime}|{challenger_status}"
                                )
            regime_grid = symbol_payload.get("regime_configs")
            if not isinstance(regime_grid, dict):
                invalid_cells.append(f"{symbol}|GRID")
                continue
            for raw_timeframe, timeframe_payload in regime_grid.items():
                timeframe = str(raw_timeframe or "").strip().upper()
                if timeframe not in required_timeframes or not isinstance(
                    timeframe_payload, dict
                ):
                    invalid_cells.append(f"{symbol}|{timeframe or 'INVALID_TF'}")
                    continue
                if not timeframe_payload:
                    invalid_cells.append(f"{symbol}|{timeframe}|EMPTY")
                    continue
                for raw_regime, cell in timeframe_payload.items():
                    regime = str(raw_regime or "").strip().upper()
                    cell_id = f"{symbol}|{timeframe}|{regime}"
                    if regime not in required_regimes or not isinstance(cell, dict):
                        invalid_cells.append(cell_id)
                        continue
                    strategy_name = str(cell.get("strategy_name", "") or "").upper()
                    if not strategy_name:
                        invalid_cells.append(cell_id)
                        continue
                    if strategy_name in {"NO_LOCAL_WINNER", "NO_TRADE", "NO_WINNER"}:
                        no_winner_status = (
                            str(cell.get("promotion_status", "") or "").strip().upper()
                        )
                        if no_winner_status not in {
                            "NO_LOCAL_WINNER",
                            "NO_TRADE",
                            "NO_WINNER",
                        }:
                            invalid_cells.append(cell_id)
                        continue
                    if strategy_name in _NON_WINNER_STRATEGY_NAMES:
                        invalid_cells.append(cell_id)
                        continue
                    if not str(cell.get("promotion_status", "") or "").strip():
                        missing_statuses.append(cell_id)
                    if (
                        native_universe is not None
                        and native_universe.explicit_contract
                        and native_universe.cell_status(symbol, timeframe)
                        == "source_unavailable"
                    ):
                        source_unavailable_winners.append(cell_id)
                    else:
                        try:
                            from pm_position import TradeTagEncoder

                            magic = TradeTagEncoder.encode_magic(
                                symbol, timeframe, regime
                            )
                            prior_owner = magic_owners.get(magic)
                            if prior_owner is not None and prior_owner != cell_id:
                                magic_collisions.append(
                                    f"magic={magic}:{prior_owner}<->{cell_id}"
                                )
                            else:
                                magic_owners[magic] = cell_id
                        except Exception as exc:
                            magic_collisions.append(
                                f"{cell_id}:magic_generation_failed:{type(exc).__name__}"
                            )
                        validation_evidence = cell.get("validation_evidence")
                        temporal = (
                            validation_evidence.get("temporal_lineage")
                            if isinstance(validation_evidence, dict)
                            else None
                        )
                        role = str(
                            temporal.get("book_role", "")
                            if isinstance(temporal, dict)
                            else ""
                        ).upper()
                        status = str(
                            temporal.get("contract_status", "")
                            if isinstance(temporal, dict)
                            else ""
                        ).upper()
                        frozen = bool(
                            temporal.get(
                                "parameters_frozen_during_final_lockbox", False
                            )
                            if isinstance(temporal, dict)
                            else False
                        )
                        if (
                            role != "LOCKBOX_VALIDATED_CHAMPION"
                            or status != "PASS"
                            or not frozen
                        ):
                            temporal_lineage_issues.append(cell_id)
        if invalid_cells:
            add(
                "error",
                "physical_cell_ledger_invalid",
                "physical ledger contains invalid or accounting-only leaves: "
                f"{_preview_set(invalid_cells)}",
            )
        if source_unavailable_winners:
            add(
                "error",
                "winner_uses_source_unavailable_cell",
                "physical winners require authenticated native sources: "
                f"{_preview_set(source_unavailable_winners)}",
            )
        if missing_statuses:
            add(
                "error",
                "promotion_status_missing",
                "non-NO_TRADE cells require an explicit promotion_status; missing for "
                f"{_preview_set(missing_statuses)}",
            )
        if temporal_lineage_issues:
            add(
                "error",
                "funded_temporal_lineage_invalid",
                "tradeable cells must occupy the frozen lockbox-champion book; invalid for "
                f"{_preview_set(temporal_lineage_issues)}",
            )
        if challenger_leakage:
            add(
                "error",
                "edge_challenger_funded_leakage",
                "latest-data challenger book is paper-only and cannot be funded: "
                f"{_preview_set(challenger_leakage)}",
            )
        if magic_collisions:
            add(
                "error",
                "specialist_magic_collision",
                "deterministic MT5 magic identifiers must be unique for exact-cell attribution: "
                f"{_preview_set(magic_collisions)}",
            )
        try:
            from pm_pipeline import artifact_contract_matches, build_artifact_meta

            current_artifact_meta = build_artifact_meta(config)
            deployment_contract = dict(
                current_artifact_meta.get("deployment_contract", {}) or {}
            )
            if (
                deployment_contract.get("contract") == "funded_live_deployment_v1"
                and str(deployment_contract.get("status", "") or "").upper() != "PASS"
            ):
                add(
                    "error",
                    "deployment_contract_unavailable",
                    "broker/position deployment contract could not be fingerprinted",
                )
            drifted_symbols = []
            regime_missing_matches_artifact = True
            for idx, payload in enumerate(symbol_configs):
                if not _config_has_tradeable_winner(payload):
                    continue
                stored_artifact = (
                    payload.get("artifact_meta")
                    if isinstance(payload.get("artifact_meta"), dict)
                    else {}
                )
                if not artifact_contract_matches(
                    stored_artifact, current_artifact_meta
                ):
                    drifted_symbols.append(
                        _symbol_label(payload, fallback=f"entry_{idx}")
                    )
                stored_regime = stored_artifact.get("regime_params")
                if regime_params_missing and isinstance(stored_regime, dict):
                    if (
                        stored_regime.get("mtime_ns") is not None
                        or stored_regime.get("size") is not None
                    ):
                        regime_missing_matches_artifact = False
                elif regime_params_missing:
                    regime_missing_matches_artifact = False
            if drifted_symbols:
                preview = ", ".join(drifted_symbols[:12])
                if len(drifted_symbols) > 12:
                    preview += f", +{len(drifted_symbols) - 12} more"
                add(
                    "error",
                    "winner_ledger_artifact_drift",
                    f"winner ledger artifacts do not match the current optimization/cost contract: {preview}",
                )
            if regime_params_missing and not regime_missing_matches_artifact:
                add(
                    "warning",
                    "regime_params_missing",
                    f"regime params file not found: {regime_params_file}",
                )
        except Exception as exc:
            add(
                "error",
                "winner_ledger_artifact_check_failed",
                f"could not verify winner ledger artifact contract: {type(exc).__name__}: {exc}",
            )
            if regime_params_missing:
                add(
                    "warning",
                    "regime_params_missing",
                    f"regime params file not found: {regime_params_file}",
                )

    # Phase A is a complete global roster frozen before any final outcome is
    # observed.  Both tuning cap selection and the joint one-shot Phase B
    # verdict must bind these exact bytes; the funded ledger is permitted only
    # an all-or-none promotion-status release of that roster.
    try:
        from pm_cap_calibration import cap_neutral_config_sha256
        from pm_pipeline import build_artifact_meta
        from pm_portfolio_replay import (
            active_native_portfolio_contract,
            build_ledger_cell_roster,
            canonical_json_sha256,
        )
        from pm_two_phase_lockbox import (
            critical_module_code_contract,
            load_pinned_prelockbox_roster,
            validate_released_ledger_against_prelockbox,
        )

        if not str(getattr(config, "_deployment_config_path", "") or "").strip():
            setattr(config, "_deployment_config_path", str(Path(config_path).resolve()))
        pre_artifact_meta = build_artifact_meta(config)
        pre_deployment = dict(pre_artifact_meta.get("deployment_contract", {}) or {})
        pre_data_dir = Path(getattr(config, "data_dir", "data"))
        if not pre_data_dir.is_absolute():
            pre_data_dir = Path(config_path).resolve().parent / pre_data_dir
        pre_native, pre_native_errors = active_native_portfolio_contract(
            pre_data_dir, requested_symbols
        )
        pre_broker_sha = None
        if broker_specs_raw:
            pre_broker_sha = _sha256_file(
                _resolve_relative_to_config(broker_specs_raw, config_path)
            )
        pre_code_contract = critical_module_code_contract(
            Path(__file__).resolve().parent
        )
        prelockbox_code_contract_sha = str(
            pre_code_contract.get("aggregate_sha256") or ""
        )
        expected_pre_lineage = {
            "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
            "artifact_contract_sha256": canonical_json_sha256(pre_artifact_meta),
            "deployment_contract_sha256": canonical_json_sha256(pre_deployment),
            "broker_specs_sha256": pre_broker_sha,
            "native_activation_id": pre_native.get("activation_id"),
            "native_activation_manifest_sha256": pre_native.get(
                "activation_manifest_sha256"
            ),
            "native_activation_roster_sha256": pre_native.get(
                "activation_roster_sha256"
            ),
            "native_bars_sha256_by_source": pre_native.get("bars_sha256_by_source"),
            "native_sidecar_sha256_by_source": pre_native.get(
                "sidecar_sha256_by_source"
            ),
            "critical_module_code_contract": pre_code_contract,
            "critical_module_code_contract_sha256": prelockbox_code_contract_sha,
        }
        (
            prelockbox_artifact,
            prelockbox_artifact_path,
            pre_errors,
        ) = load_pinned_prelockbox_roster(
            config_payload=config_payload,
            config_path=Path(config_path),
            expected_symbols=requested_symbols,
            expected_lineage=expected_pre_lineage,
        )
        pre_errors = list(pre_native_errors) + list(pre_errors)
        if isinstance(prelockbox_artifact, dict):
            prelockbox_artifact_file_sha = _sha256_file(prelockbox_artifact_path)
            frozen_ledger = prelockbox_artifact.get("frozen_ledger") or {}
            prelockbox_standard_roster, standard_errors = build_ledger_cell_roster(
                frozen_ledger,
                requested_symbols,
                allowed_promotion_statuses=("FROZEN_PRELOCKBOX",),
            )
            pre_errors.extend(standard_errors)
            if ledger is None:
                pre_errors.append("RELEASED_LEDGER_UNREADABLE")
            else:
                release_evidence_sha = None
                release_evidence_raw = str(
                    getattr(config, "production_portfolio_replay_evidence_path", "")
                    or ""
                ).strip()
                if release_evidence_raw:
                    release_evidence_path = _resolve_relative_to_config(
                        release_evidence_raw, config_path
                    )
                    release_evidence_sha = _sha256_file(release_evidence_path)
                pre_errors.extend(
                    validate_released_ledger_against_prelockbox(
                        ledger,
                        prelockbox_artifact,
                        expected_portfolio_evidence_sha256=release_evidence_sha,
                    )
                )
        if pre_errors:
            add(
                "error",
                "prelockbox_roster_invalid_or_stale",
                "sealed Phase-A roster or all-or-none released ledger failed: "
                f"{_preview_set(pre_errors)}",
            )
        else:
            prelockbox_roster_valid = True
    except Exception as exc:
        add(
            "error",
            "prelockbox_roster_check_failed",
            f"could not verify the sealed Phase-A roster: {type(exc).__name__}: {exc}",
        )

    # The funded total stop-risk cap is not a discretionary constant.  It must
    # be selected on the tuning-validation partition, frozen before any joint
    # final-lockbox access, and byte-pinned here.  The final replay may reject
    # that cap but cannot choose or increase it.
    cap_calibration_path_raw = str(
        getattr(config, "production_cap_calibration_path", "") or ""
    ).strip()
    if not cap_calibration_path_raw:
        add(
            "error",
            "capital_cap_calibration_missing",
            "max_total_open_risk_pct remains provisional until a sealed tuning-only "
            "cap calibration is frozen and configured",
        )
    elif not prelockbox_roster_valid or not isinstance(prelockbox_artifact, dict):
        add(
            "error",
            "capital_cap_calibration_unverifiable",
            "cap calibration cannot be checked without the valid sealed Phase-A roster",
        )
    else:
        cap_calibration_path = _resolve_relative_to_config(
            cap_calibration_path_raw, config_path
        )
        cap_calibration = _load_json_file(cap_calibration_path)
        configured_cap_sha = (
            str(getattr(config, "production_cap_calibration_sha256", "") or "")
            .strip()
            .lower()
        )
        cap_calibration_file_sha = _sha256_file(cap_calibration_path)
        if not isinstance(cap_calibration, dict):
            add(
                "error",
                "capital_cap_calibration_unreadable",
                f"sealed tuning cap calibration is missing or unreadable: {cap_calibration_path}",
            )
        elif (
            len(configured_cap_sha) != 64
            or cap_calibration_file_sha != configured_cap_sha
        ):
            add(
                "error",
                "capital_cap_calibration_hash_mismatch",
                "production_cap_calibration_sha256 does not pin the exact configured artifact bytes",
            )
        else:
            try:
                from pm_cap_calibration import (
                    cap_neutral_config_sha256,
                    cap_neutral_policy_contract_sha256,
                    validate_cap_calibration_artifact,
                )
                from pm_portfolio_replay import (
                    active_native_portfolio_contract,
                )
                from pm_two_phase_lockbox import (
                    prelockbox_evidence_policy_from_config_payload,
                    validate_global_final_lockbox_access,
                )

                raw_balances = list(
                    getattr(
                        config,
                        "production_portfolio_replay_required_starting_balances",
                        [],
                    )
                    or []
                )
                calibration_balances = sorted(
                    {
                        _to_float(value, 0.0)
                        for value in raw_balances
                        if _to_float(value, 0.0) > 0.0
                    }
                )
                calibration_policies = {
                    balance: prelockbox_evidence_policy_from_config_payload(
                        config_payload, balance
                    )
                    for balance in calibration_balances
                }
                calibration_data_dir = Path(getattr(config, "data_dir", "data"))
                if not calibration_data_dir.is_absolute():
                    calibration_data_dir = (
                        Path(config_path).resolve().parent / calibration_data_dir
                    )
                calibration_native_contract, calibration_native_errors = (
                    active_native_portfolio_contract(
                        calibration_data_dir,
                        requested_symbols,
                    )
                )
                calibration_broker_sha = None
                if broker_specs_raw:
                    calibration_broker_sha = _sha256_file(
                        _resolve_relative_to_config(broker_specs_raw, config_path)
                    )
                calibration_expected_lineage = {
                    "cap_neutral_config_sha256": cap_neutral_config_sha256(
                        config_payload
                    ),
                    "prelockbox_roster_sha256": (
                        (prelockbox_artifact.get("roster") or {}).get("roster_sha256")
                    ),
                    "prelockbox_artifact_file_sha256": prelockbox_artifact_file_sha,
                    "prelockbox_artifact_seal_sha256": (
                        (prelockbox_artifact.get("content_seal") or {}).get("sha256")
                    ),
                    "critical_module_code_contract_sha256": prelockbox_code_contract_sha,
                    "broker_specs_sha256": calibration_broker_sha,
                    "native_activation_id": calibration_native_contract.get(
                        "activation_id"
                    ),
                    "native_activation_manifest_sha256": calibration_native_contract.get(
                        "activation_manifest_sha256"
                    ),
                    "native_activation_roster_sha256": calibration_native_contract.get(
                        "activation_roster_sha256"
                    ),
                    "native_bars_sha256_by_source": calibration_native_contract.get(
                        "bars_sha256_by_source"
                    ),
                    "native_sidecar_sha256_by_source": calibration_native_contract.get(
                        "sidecar_sha256_by_source"
                    ),
                    "base_policy_contract_sha256": cap_neutral_policy_contract_sha256(
                        calibration_policies
                    ),
                    "required_starting_balances": calibration_balances,
                }
                calibration_errors = list(calibration_native_errors)
                calibration_errors.extend(
                    validate_cap_calibration_artifact(
                        cap_calibration,
                        expected_lineage=calibration_expected_lineage,
                        expected_selected_cap_pct=number("max_total_open_risk_pct"),
                    )
                )
                if calibration_errors:
                    add(
                        "error",
                        "capital_cap_calibration_invalid_or_stale",
                        "sealed tuning cap calibration failed the current deployment contract: "
                        f"{_preview_set(calibration_errors)}",
                    )
                else:
                    cap_calibration_valid = True
                    selection = cap_calibration.get("selection")
                    if isinstance(selection, dict):
                        cap_calibration_selected_cap = _to_float(
                            selection.get("selected_max_total_stop_risk_pct"),
                            0.0,
                        )
            except Exception as exc:
                add(
                    "error",
                    "capital_cap_calibration_check_failed",
                    "could not verify tuning cap calibration: "
                    f"{type(exc).__name__}: {exc}",
                )

    # A per-cell replay can show that each leaf survives in isolation while the
    # combined book still breaches shared capital, concentration, margin or
    # minimum-lot constraints.  Funded live therefore requires a separate,
    # content-addressed synchronized replay whose exact ledger, deployment,
    # broker-spec and native-activation hashes still match this launch.
    portfolio_evidence_path_raw = str(
        getattr(config, "production_portfolio_replay_evidence_path", "") or ""
    ).strip()
    if not portfolio_evidence_path_raw:
        add(
            "error",
            "synchronized_portfolio_evidence_missing",
            "production_portfolio_replay_evidence_path is empty; funded live has no shared-capital proof",
        )
    else:
        portfolio_evidence_path = _resolve_relative_to_config(
            portfolio_evidence_path_raw, config_path
        )
        portfolio_evidence = _load_json_file(portfolio_evidence_path)
        if not isinstance(portfolio_evidence, dict):
            add(
                "error",
                "synchronized_portfolio_evidence_unreadable",
                f"sealed synchronized replay evidence is missing or unreadable: {portfolio_evidence_path}",
            )
        elif ledger is None:
            add(
                "error",
                "synchronized_portfolio_evidence_unverifiable",
                "synchronized replay evidence cannot be checked without a readable winner ledger",
            )
        else:
            try:
                from pm_pipeline import build_artifact_meta
                from pm_portfolio_replay import (
                    active_native_portfolio_contract,
                    build_ledger_cell_roster,
                    canonical_json_sha256,
                    portfolio_cost_contract,
                    validate_funded_portfolio_replay_evidence,
                )
                from pm_two_phase_lockbox import (
                    prelockbox_evidence_policy_from_config_payload,
                    validate_global_final_lockbox_access,
                )

                if not str(
                    getattr(config, "_deployment_config_path", "") or ""
                ).strip():
                    setattr(
                        config,
                        "_deployment_config_path",
                        str(Path(config_path).resolve()),
                    )
                current_artifact_meta = build_artifact_meta(config)
                deployment_contract = dict(
                    current_artifact_meta.get("deployment_contract", {}) or {}
                )
                allowed_statuses = list(
                    getattr(config, "live_allowed_promotion_statuses", []) or []
                )
                current_roster, roster_errors = build_ledger_cell_roster(
                    ledger,
                    requested_symbols,
                    allowed_promotion_statuses=allowed_statuses,
                )
                if roster_errors:
                    add(
                        "error",
                        "synchronized_portfolio_roster_invalid",
                        "current independent-cell roster is incomplete: "
                        f"{_preview_set(roster_errors)}",
                    )

                data_dir = Path(getattr(config, "data_dir", "data"))
                if not data_dir.is_absolute():
                    data_dir = Path(config_path).resolve().parent / data_dir
                native_contract, native_errors = active_native_portfolio_contract(
                    data_dir,
                    requested_symbols,
                )
                # Re-hash every source that can actually place a funded trade.
                # The full activation roster is manifest-pinned above; this
                # narrower byte verification catches post-activation mutation
                # without hashing dormant NO_TRADE files at every preflight.
                from pm_native_activation import resolve_active_native_artifact

                tradeable_sources = {
                    tuple(str(cell_id).split("|")[:2])
                    for cell_id in (current_roster.get("tradeable_cell_ids") or [])
                    if len(str(cell_id).split("|")) == 3
                }
                for symbol, timeframe in sorted(tradeable_sources):
                    resolution = resolve_active_native_artifact(
                        data_dir,
                        symbol,
                        timeframe,
                        production_contract_required=True,
                    )
                    if not resolution.valid:
                        reasons = ",".join(resolution.errors) or "unresolved"
                        native_errors.append(
                            f"NATIVE_TRADEABLE_SOURCE_INVALID:{symbol}|{timeframe}:{reasons}"
                        )
                if native_errors:
                    add(
                        "error",
                        "synchronized_portfolio_native_lineage_invalid",
                        "active native history cannot support funded portfolio evidence: "
                        f"{_preview_set(native_errors)}",
                    )

                broker_sha = None
                if broker_specs_raw:
                    broker_path = _resolve_relative_to_config(
                        broker_specs_raw, config_path
                    )
                    broker_sha = _sha256_file(broker_path)
                expected_lineage = {
                    "artifact_contract_sha256": canonical_json_sha256(
                        current_artifact_meta
                    ),
                    "deployment_contract_sha256": canonical_json_sha256(
                        deployment_contract
                    ),
                    "prelockbox_roster_sha256": (
                        (prelockbox_artifact.get("roster") or {}).get("roster_sha256")
                        if isinstance(prelockbox_artifact, dict)
                        else None
                    ),
                    "prelockbox_artifact_file_sha256": prelockbox_artifact_file_sha,
                    "prelockbox_artifact_seal_sha256": (
                        (prelockbox_artifact.get("content_seal") or {}).get("sha256")
                        if isinstance(prelockbox_artifact, dict)
                        else None
                    ),
                    "critical_module_code_contract_sha256": prelockbox_code_contract_sha,
                    "broker_specs_sha256": broker_sha,
                    "native_activation_id": native_contract.get("activation_id"),
                    "native_activation_manifest_sha256": native_contract.get(
                        "activation_manifest_sha256"
                    ),
                    "native_activation_roster_sha256": native_contract.get(
                        "activation_roster_sha256"
                    ),
                    "native_bars_sha256_by_source": native_contract.get(
                        "bars_sha256_by_source"
                    ),
                    "native_sidecar_sha256_by_source": native_contract.get(
                        "sidecar_sha256_by_source"
                    ),
                    "cap_calibration_sha256": cap_calibration_file_sha,
                    "cap_calibration_selected_max_total_stop_risk_pct": (
                        cap_calibration_selected_cap
                    ),
                }

                raw_balances = list(
                    getattr(
                        config,
                        "production_portfolio_replay_required_starting_balances",
                        [],
                    )
                    or []
                )
                required_balances: List[float] = []
                for raw_balance in raw_balances:
                    balance = _to_float(raw_balance, 0.0)
                    if balance > 0.0 and not any(
                        abs(balance - existing) <= 1e-6
                        for existing in required_balances
                    ):
                        required_balances.append(balance)
                required_reference_balances = [
                    number("production_replay_audit_starting_balance"),
                    number("live_min_account_equity"),
                ]
                for reference in required_reference_balances:
                    if reference > 0.0 and not any(
                        abs(reference - balance) <= 1e-6
                        for balance in required_balances
                    ):
                        add(
                            "error",
                            "synchronized_portfolio_required_balance_missing",
                            f"portfolio replay balances must include deployment reference equity {reference:.2f}",
                        )
                if not required_balances:
                    add(
                        "error",
                        "synchronized_portfolio_required_balances_invalid",
                        "production_portfolio_replay_required_starting_balances must contain positive balances",
                    )
                expected_policies = {
                    balance: asdict(
                        prelockbox_evidence_policy_from_config_payload(
                            config_payload, balance
                        )
                    )
                    for balance in required_balances
                }
                evidence_errors = validate_funded_portfolio_replay_evidence(
                    portfolio_evidence,
                    expected_lineage=expected_lineage,
                    expected_roster=prelockbox_standard_roster,
                    expected_cost_contract=portfolio_cost_contract(config_payload),
                    expected_policies=expected_policies,
                    minimum_calendar_days=number(
                        "production_portfolio_replay_min_calendar_days"
                    ),
                    minimum_accepted_positions=int(
                        number("production_portfolio_replay_min_positions")
                    ),
                    minimum_profit_factor=number(
                        "production_portfolio_replay_min_profit_factor"
                    ),
                    minimum_return_pct=number(
                        "production_portfolio_replay_min_return_pct"
                    ),
                    maximum_drawdown_pct=number(
                        "production_portfolio_replay_max_drawdown_pct"
                    ),
                    require_mtm=bool(
                        getattr(
                            config, "production_portfolio_replay_require_mtm", False
                        )
                    ),
                    maximum_bootstrap_ruin_upper_95pct=_to_float(
                        (cap_calibration.get("constraints") or {}).get(
                            "maximum_bootstrap_ruin_upper_95pct"
                        )
                        if isinstance(cap_calibration, dict)
                        and isinstance(cap_calibration.get("constraints"), dict)
                        else None,
                        float("nan"),
                    ),
                    maximum_bootstrap_p95_drawdown_pct=_to_float(
                        (cap_calibration.get("constraints") or {}).get(
                            "maximum_bootstrap_p95_drawdown_pct"
                        )
                        if isinstance(cap_calibration, dict)
                        and isinstance(cap_calibration.get("constraints"), dict)
                        else None,
                        float("nan"),
                    ),
                    minimum_bootstrap_daily_return_observations=_to_float(
                        (cap_calibration.get("constraints") or {}).get(
                            "minimum_bootstrap_daily_return_observations"
                        )
                        if isinstance(cap_calibration, dict)
                        and isinstance(cap_calibration.get("constraints"), dict)
                        else None,
                        float("nan"),
                    ),
                )
                replay_payload = (
                    portfolio_evidence.get("replay")
                    if isinstance(portfolio_evidence.get("replay"), dict)
                    else {}
                )
                if isinstance(prelockbox_artifact, dict):
                    evidence_errors.extend(
                        validate_global_final_lockbox_access(
                            config=config,
                            config_path=Path(config_path),
                            access_id=str(
                                replay_payload.get("global_lockbox_access_id") or ""
                            ),
                            frozen_ledger=(
                                prelockbox_artifact.get("frozen_ledger") or {}
                            ),
                            roster_sha256=str(
                                (prelockbox_artifact.get("roster") or {}).get(
                                    "roster_sha256"
                                )
                                or ""
                            ),
                            prelockbox_file_sha256=str(
                                prelockbox_artifact_file_sha or ""
                            ),
                        )
                    )
                if evidence_errors:
                    add(
                        "error",
                        "synchronized_portfolio_evidence_invalid_or_stale",
                        "sealed synchronized replay evidence failed the current deployment contract: "
                        f"{_preview_set(evidence_errors)}",
                    )
                else:
                    synchronized_portfolio_evidence_valid = True
            except Exception as exc:
                add(
                    "error",
                    "synchronized_portfolio_evidence_check_failed",
                    "could not verify synchronized portfolio evidence: "
                    f"{type(exc).__name__}: {exc}",
                )

    require(
        prelockbox_roster_valid,
        "funded_prelockbox_roster_unproven",
        "funded launch requires the sealed pre-final global specialist roster and an all-or-none released ledger",
    )
    require(
        cap_calibration_valid,
        "funded_total_stop_risk_cap_unproven",
        "funded launch requires a tuning-selected, sealed and current total stop-risk cap",
    )
    require(
        synchronized_portfolio_evidence_valid,
        "same_symbol_multitimeframe_concurrency_unproven",
        "specialist-cell concurrency requires sealed synchronized replay evidence "
        "matching the current replay schema and exact funded policy",
    )

    tradeable_cells = (
        _tradeable_cell_ids_from_ledger(ledger) if ledger is not None else set()
    )
    if (
        ledger is not None
        and isinstance(broker_specs, dict)
        and broker_specs
        and account_info is not None
        and position_config is not None
    ):
        equity = _to_float(getattr(account_info, "equity", 0.0), 0.0)
        target_risk_pct = _to_float(
            getattr(position_config, "risk_per_trade_pct", 0.0), 0.0
        )
        hard_cap_pct = _to_float(getattr(position_config, "max_risk_pct", 0.0), 0.0)
        spec_by_symbol = _broker_specs_by_symbol(broker_specs)
        unpriced_cells: List[str] = []
        target_excess: List[str] = []
        hard_cap_excess: List[str] = []
        for cell in _iter_tradeable_cell_payloads(ledger):
            symbol = str(cell.get("symbol", "") or "").upper()
            spec_payload = spec_by_symbol.get(symbol)
            if not isinstance(spec_payload, dict):
                continue
            stop_pips = _extract_static_stop_pips(cell)
            label = f"{symbol}|{cell.get('timeframe', '')}|{cell.get('regime', '')}"
            if stop_pips is None:
                unpriced_cells.append(label)
                continue
            min_lot_risk_pct = _min_lot_risk_pct_from_broker_spec(
                spec_payload,
                stop_pips=stop_pips,
                equity=equity,
            )
            if min_lot_risk_pct is None:
                unpriced_cells.append(label)
                continue
            if hard_cap_pct > 0.0 and min_lot_risk_pct > hard_cap_pct + 1e-9:
                hard_cap_excess.append(
                    f"{label} min_lot_risk={min_lot_risk_pct:.2f}% > hard_cap={hard_cap_pct:.2f}%"
                )
            elif target_risk_pct > 0.0 and min_lot_risk_pct > target_risk_pct + 1e-9:
                target_excess.append(
                    f"{label} min_lot_risk={min_lot_risk_pct:.2f}% > target={target_risk_pct:.2f}%"
                )
        if hard_cap_excess:
            add(
                "error",
                "min_lot_risk_exceeds_hard_cap",
                "at current equity, broker min-lot risk exceeds the per-trade hard cap for "
                f"{_preview_set(hard_cap_excess)}",
            )
        if target_excess:
            add(
                "error",
                "min_lot_risk_exceeds_target",
                "at current equity, broker min-lot risk exceeds target risk for "
                f"{_preview_set(target_excess)}",
            )
        if unpriced_cells:
            add(
                "error",
                "min_lot_tradeability_unpriced",
                "could not pre-price min-lot risk for live tradeable cells lacking static stop/avg-loss evidence: "
                f"{_preview_set(unpriced_cells)}",
            )
    if (
        bool(getattr(config, "live_require_tuned_regime_params", False))
        and tradeable_cells
    ):
        missing_tuned_params = _cells_missing_tuned_regime_params(
            tradeable_cells, regime_params_file
        )
        if missing_tuned_params:
            add(
                "error",
                "live_tuned_regime_params_missing",
                "live entries require explicitly tuned regime params; missing tuned entries for "
                f"{_preview_set(missing_tuned_params)}"
                + (
                    f" in {regime_params_file}"
                    if regime_params_file
                    else " because regime_params_file is empty"
                ),
            )

    if bool(getattr(config, "production_require_promotion_gate", False)):
        gate_mode = (
            str(
                getattr(
                    config, "production_promotion_gate_mode", "external_review_exact"
                )
                or ""
            )
            .strip()
            .lower()
        )
        if gate_mode in {"immutable", "authorization", "authorization_exact"}:
            gate_mode = "immutable_exact"
        elif gate_mode in {"ledger", "current_ledger", "ledger_authoritative"}:
            gate_mode = "ledger_status"
        elif gate_mode in {"external", "exact", "review", "review_exact"}:
            gate_mode = "external_review_exact"
        if gate_mode not in {
            "immutable_exact",
            "external_review_exact",
            "ledger_status",
        }:
            add(
                "error",
                "production_promotion_gate_mode_invalid",
                f"invalid production_promotion_gate_mode: {getattr(config, 'production_promotion_gate_mode', None)!r}",
            )
            gate_mode = "external_review_exact"
        if gate_mode != "immutable_exact":
            add(
                "error",
                "production_promotion_gate_not_immutable_exact",
                "funded live requires the hash-pinned immutable_exact promotion authorization; "
                f"{gate_mode!r} is a research/legacy mode",
            )

        if gate_mode == "immutable_exact":
            if ledger is None:
                add(
                    "error",
                    "production_promotion_authorization_ledger_unavailable",
                    "cannot verify immutable promotion authorization without a readable winner ledger",
                )
            else:
                try:
                    from pm_promotion_authorization import (
                        verify_promotion_authorization,
                    )

                    authorization = verify_promotion_authorization(
                        config,
                        ledger,
                        ledger_path=ledger_path,
                        config_path=Path(config_path),
                        verify_active_native=True,
                    )
                except Exception as exc:
                    add(
                        "error",
                        "production_promotion_authorization_check_failed",
                        f"immutable promotion authorization verification failed: {type(exc).__name__}: {exc}",
                    )
                else:
                    for reason in authorization.errors:
                        reason_code = "".join(
                            ch.lower() if ch.isalnum() else "_"
                            for ch in str(reason).split(":", 1)[0]
                        ).strip("_")
                        add(
                            "error",
                            f"production_{reason_code or 'promotion_authorization_invalid'}",
                            f"immutable promotion authorization rejected: {reason}",
                        )
            return ReadinessReport(
                ok=not any(f.severity == "error" for f in findings), findings=findings
            )

        if gate_mode == "ledger_status":
            if ledger is not None and not tradeable_cells:
                add(
                    "error",
                    "production_promotion_ledger_no_tradeable_cells",
                    "production promotion gate uses ledger_status but the winner ledger has no tradeable promoted cells",
                )
            return ReadinessReport(
                ok=not any(f.severity == "error" for f in findings), findings=findings
            )

        review_raw = str(
            getattr(config, "production_promotion_review_path", "") or ""
        ).strip()
        safety_raw = str(
            getattr(config, "production_promotion_cell_safety_path", "") or ""
        ).strip()
        marker_raw = str(
            getattr(config, "production_completion_marker_path", "") or ""
        ).strip()

        if not review_raw:
            add(
                "error",
                "production_promotion_review_missing",
                "production_require_promotion_gate is enabled but production_promotion_review_path is empty",
            )
            review_payload = None
        else:
            review_path = _resolve_relative_to_config(review_raw, config_path)
            review_payload = _load_json_file(review_path)
            if not isinstance(review_payload, dict):
                add(
                    "error",
                    "production_promotion_review_unreadable",
                    f"promotion review artifact is missing or unreadable: {review_path}",
                )

        promote_cells = _promote_cell_ids_from_review(review_payload)
        if review_raw and isinstance(review_payload, dict) and not promote_cells:
            add(
                "error",
                "production_promotion_review_no_promotes",
                "promotion review artifact contains no final PROMOTE cells",
            )

        expected_cells = {
            str(cell or "").strip().upper()
            for cell in (getattr(config, "production_expected_promote_cells", []) or [])
            if str(cell or "").strip()
        }
        if expected_cells and promote_cells and promote_cells != expected_cells:
            add(
                "error",
                "production_expected_promote_cells_mismatch",
                "expected production promote cells do not match promotion review: "
                f"missing={_preview_set(expected_cells - promote_cells) or 'none'}; "
                f"extra={_preview_set(promote_cells - expected_cells) or 'none'}",
            )
        if promote_cells and tradeable_cells != promote_cells:
            add(
                "error",
                "production_winner_ledger_promotion_mismatch",
                "tradeable winner ledger cells do not exactly match corrected promotion PROMOTE cells: "
                f"ledger_only={_preview_set(tradeable_cells - promote_cells) or 'none'}; "
                f"review_only={_preview_set(promote_cells - tradeable_cells) or 'none'}",
            )

        if not safety_raw:
            add(
                "error",
                "production_promotion_safety_missing",
                "production_require_promotion_gate is enabled but production_promotion_cell_safety_path is empty",
            )
        else:
            safety_path = _resolve_relative_to_config(safety_raw, config_path)
            safety_payload = _load_json_file(safety_path)
            if not isinstance(safety_payload, dict):
                add(
                    "error",
                    "production_promotion_safety_unreadable",
                    f"promotion cell-safety artifact is missing or unreadable: {safety_path}",
                )
            else:
                summary = (
                    safety_payload.get("navigation_counts")
                    if isinstance(safety_payload.get("navigation_counts"), dict)
                    else None
                )
                if summary is None:
                    summary = (
                        safety_payload.get("summary")
                        if isinstance(safety_payload.get("summary"), dict)
                        else safety_payload
                    )

                def _safety_count(name: str) -> int:
                    value = summary.get(name, safety_payload.get(name, 0))
                    if isinstance(value, list):
                        return len(value)
                    return int(value or 0)

                unsafe = _safety_count("unsafe_promotes")
                fail_not_reject = _safety_count("fail_not_reject")
                final_promotes = _safety_count("final_promotes")
                if unsafe or fail_not_reject:
                    add(
                        "error",
                        "production_promotion_safety_failed",
                        f"promotion/cell safety artifact reports unsafe_promotes={unsafe}, fail_not_reject={fail_not_reject}",
                    )
                if promote_cells and final_promotes != len(promote_cells):
                    add(
                        "error",
                        "production_promotion_safety_count_mismatch",
                        f"promotion safety final_promotes={final_promotes} but review PROMOTE count={len(promote_cells)}",
                    )

        if not marker_raw:
            add(
                "error",
                "production_completion_marker_missing",
                "production_require_promotion_gate is enabled but production_completion_marker_path is empty",
            )
        else:
            marker_path = _resolve_relative_to_config(marker_raw, config_path)
            marker_payload = _load_json_file(marker_path)
            if not isinstance(marker_payload, dict):
                add(
                    "error",
                    "production_completion_marker_unreadable",
                    f"completion marker is missing or unreadable: {marker_path}",
                )
            else:
                verdict = str(marker_payload.get("verdict", "") or "").upper()
                if not bool(marker_payload.get("review_ready", False)):
                    add(
                        "error",
                        "production_completion_marker_not_review_ready",
                        "completion marker is not marked review_ready",
                    )
                if bool(marker_payload.get("stale")):
                    add(
                        "error",
                        "production_completion_marker_stale",
                        "completion marker is marked stale",
                    )
                if bool(marker_payload.get("pid_alive")):
                    add(
                        "error",
                        "production_completion_marker_active_pid",
                        "completion marker still reports pid_alive=true",
                    )
                if "CELL_SAFE" not in verdict:
                    add(
                        "error",
                        "production_completion_marker_not_cell_safe",
                        f"completion marker verdict is not cell-safe: {marker_payload.get('verdict')}",
                    )

    return ReadinessReport(
        ok=not any(f.severity == "error" for f in findings), findings=findings
    )


def validate_live_readiness(
    config: Any,
    *,
    symbols: Iterable[str],
    config_path: str = "config.json",
    winner_ledger_path: Optional[str] = None,
    mt5_config: Optional[Any] = None,
    account_info: Optional[Any] = None,
    position_config: Optional[Any] = None,
) -> ReadinessReport:
    """Validate the sparse current-refit funded deployment contract."""
    findings: List[ReadinessFinding] = []

    def add(severity: str, code: str, message: str) -> None:
        findings.append(
            ReadinessFinding(severity, code, message, _readiness_scope(code))
        )

    def require(condition: bool, code: str, message: str) -> None:
        if not condition:
            add("error", code, message)

    def number(name: str, default: float = 0.0) -> float:
        try:
            value = float(getattr(config, name, default))
        except (TypeError, ValueError, OverflowError):
            return default
        return value if math.isfinite(value) else default

    requested_symbols = list(
        dict.fromkeys(
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        )
    )
    required_timeframes = ("M5", "M15", "M30", "H1", "H4", "D1")
    configured_timeframes = tuple(
        str(value).strip().upper()
        for value in (getattr(config, "timeframes", []) or [])
        if str(value).strip()
    )
    require(
        bool(requested_symbols),
        "production_symbol_pool_empty",
        "funded readiness requires a nonempty symbol universe",
    )
    target_count = int(number("production_symbol_pool_target_count"))
    if target_count > 0:
        require(
            len(requested_symbols) == target_count,
            "production_symbol_pool_count_mismatch",
            f"symbol pool has {len(requested_symbols)} symbols, expected {target_count}",
        )
    require(
        not bool(getattr(config, "research_mode", False)),
        "research_mode_not_live",
        "research_mode must be false for funded live trading",
    )
    require(
        str(getattr(config, "production_split_mode", "") or "").lower()
        == "calendar_lockbox_v2",
        "production_split_not_calendar_lockbox",
        "funded live requires calendar_lockbox_v2",
    )
    require(
        configured_timeframes == required_timeframes,
        "production_timeframe_contract_mismatch",
        "funded live requires M5, M15, M30, H1, H4, D1 in order",
    )
    for name in (
        "evidence_sufficiency_enabled",
        "evidence_require_native_timeframes",
        "evidence_require_native_provenance",
        "evidence_require_session_coverage",
    ):
        require(
            bool(getattr(config, name, False)),
            f"{name}_disabled",
            f"{name} must be enabled",
        )
    require(
        bool(getattr(config, "use_spread", True)),
        "use_spread_disabled",
        "use_spread cannot be disabled for funded trading",
    )
    require(
        bool(getattr(config, "swap_model_enabled", False)),
        "swap_model_disabled",
        "swap costs must be modeled",
    )
    require(
        str(getattr(config, "backtest_spread_mode", "") or "").lower()
        == "conservative_max",
        "backtest_spread_mode_not_conservative",
        "funded evidence requires conservative_max spread modeling",
    )
    require(
        str(getattr(config, "live_artifact_drift_policy", "block") or "").lower()
        == "block",
        "artifact_drift_not_blocking",
        "live artifact drift must block",
    )
    require(
        bool(getattr(config, "false_discovery_control_enabled", False)),
        "false_discovery_control_required",
        "false-discovery control must be enabled",
    )
    require(
        number("false_discovery_min_dsr") >= 0.95,
        "deflated_sharpe_threshold_too_low",
        "the configured deflated-Sharpe threshold must be at least 0.95",
    )
    require(
        number("false_discovery_min_daily_observations") >= 252,
        "daily_observation_floor_too_low",
        "promotion evidence requires at least 252 daily observations",
    )
    require(
        number("false_discovery_min_effective_observations") >= 60.0,
        "effective_observation_floor_too_low",
        "promotion evidence requires at least 60 effective observations",
    )
    require(
        number("false_discovery_max_pbo", 1.0) <= 0.5,
        "pbo_threshold_too_permissive",
        "the configured PBO threshold must be no greater than 0.5",
    )
    require(
        int(number("false_discovery_pbo_min_candidates")) >= 5,
        "pbo_candidate_floor_too_low",
        "PBO requires at least five candidates",
    )
    require(
        bool(getattr(config, "false_discovery_require_bootstrap", False)),
        "bootstrap_evidence_required",
        "moving-block bootstrap evidence must be required",
    )
    require(
        int(number("false_discovery_bootstrap_repetitions")) >= 300,
        "bootstrap_repetitions_too_low",
        "bootstrap evidence requires at least 300 repetitions",
    )
    require(
        str(getattr(config, "false_discovery_holdout_policy", "") or "").lower()
        == "block",
        "lockbox_policy_not_blocking",
        "final-lockbox failures must block",
    )
    require(
        bool(getattr(config, "lockbox_one_shot_registry_enabled", False)),
        "lockbox_one_shot_registry_required",
        "one-shot lockbox access accounting must be enabled",
    )
    require(
        bool(getattr(config, "production_require_full_symbol_ledger", False)),
        "full_symbol_ledger_required",
        "funded readiness requires complete requested-symbol accounting",
    )
    require(
        not bool(getattr(config, "live_allow_partial_symbol_ledger", False)),
        "partial_symbol_ledger_not_live",
        "partial winner-ledger mode is not funded-live eligible",
    )
    require(
        bool(getattr(config, "walk_forward_audit_enabled", False)),
        "walk_forward_disabled",
        "repeated historical process validation must be enabled",
    )
    require(
        str(getattr(config, "walk_forward_promotion_policy", "") or "").strip().lower()
        == "promotion_gate",
        "walk_forward_promotion_policy_weakened",
        "historical process validation must remain promotion-gating",
    )
    require(
        bool(getattr(config, "walk_forward_live_blocking_enabled", False)),
        "walk_forward_live_blocking_disabled",
        "failed historical process validation must block funded entry",
    )
    require(
        bool(getattr(config, "live_outcome_feedback_enabled", False))
        and bool(getattr(config, "live_outcome_require_position_history", False))
        and bool(getattr(config, "live_outcome_feed_cycle_drift", False)),
        "live_outcome_feedback_required",
        "durable close accounting and drift feedback are required",
    )
    require(
        bool(getattr(config, "cycle_drift_monitor_enabled", False)),
        "cycle_drift_monitor_required",
        "cycle-drift monitoring must be enabled",
    )
    require(
        bool(getattr(config, "probation_enabled", False))
        and int(number("probation_min_live_outcomes")) >= 30
        and number("probation_min_mean_r", -1.0) >= 0.0
        and number("probation_min_profit_factor") >= 1.0,
        "forward_probation_contract_too_weak",
        "probation requires 30 outcomes, nonnegative mean R, and PF at least 1",
    )

    findings.extend(
        _capital_stop_risk_config_issues(
            getattr(config, "max_total_open_risk_pct", None),
            getattr(config, "max_factor_group_open_risk_pct", None),
        )
    )
    combined_units = _quarter_percent_units(
        getattr(config, "max_combined_risk_pct", None)
    )
    total_units = _quarter_percent_units(
        getattr(config, "max_total_open_risk_pct", None)
    )
    require(
        combined_units is not None
        and total_units is not None
        and combined_units <= total_units,
        "same_symbol_stop_risk_cap_contract_mismatch",
        "same-symbol stop risk must be positive, 0.25%-aligned, and no greater than total stop risk",
    )
    require(
        number("max_new_entries_when_total_open_risk_above_pct") == 0.0
        and str(getattr(config, "open_risk_action", "") or "").lower() == "observe",
        "soft_open_risk_gate_not_neutral",
        "only the evidence-selected hard capital envelope may gate open risk",
    )
    margin_levels = (
        number("margin_reopen_level"),
        number("margin_entry_block_level"),
        number("margin_recovery_start_level"),
        number("margin_panic_level"),
    )
    require(
        margin_levels[0] >= margin_levels[1] > margin_levels[2] > margin_levels[3] > 0,
        "margin_threshold_order_invalid",
        "margin thresholds must be ordered reopen, block, recovery, panic",
    )
    require(
        bool(getattr(config, "institutional_supervisor_enabled", True)),
        "supervisor_disabled",
        "institutional supervisor is disabled",
    )
    require(
        str(getattr(config, "institutional_supervisor_mode", "") or "").lower()
        == "circuit_breaker",
        "supervisor_mode_not_circuit_breaker",
        "funded live requires circuit_breaker supervisor mode",
    )
    require(
        str(getattr(config, "portfolio_control_policy", "") or "").lower()
        == "hard_breakpoints_only",
        "portfolio_control_policy_not_hard_breakpoints",
        "funded live requires hard_breakpoints_only",
    )
    funded_breakers = {
        "session_loss_hard_block_pct": 1.5,
        "daily_loss_hard_block_pct": 2.0,
        "weekly_loss_hard_block_pct": 5.0,
        "open_drawdown_hard_block_pct": 3.0,
    }
    breaker_mismatches = {
        name: {"expected": expected, "observed": number(name)}
        for name, expected in funded_breakers.items()
        if abs(number(name) - expected) > 1e-12
    }
    require(
        not breaker_mismatches,
        "account_breaker_threshold_contract_mismatch",
        f"funded breaker thresholds differ from policy: {breaker_mismatches}",
    )
    require(
        str(getattr(config, "local_governance_live_mode", "off") or "off").lower()
        in {"off", "shadow"},
        "governance_overlay_unproven",
        "governance overlays may be off or shadow-only",
    )
    require(
        str(getattr(config, "live_risk_scalars_mode", "off") or "off").lower()
        in {"off", "shadow"},
        "risk_scalar_overlay_unproven",
        "risk scalars may be off or shadow-only",
    )
    require(
        str(getattr(config, "market_driven_exit_pack_mode", "off") or "off").lower()
        in {"off", "paper"},
        "market_exit_overlay_unproven",
        "market-driven exits may be off or paper-only",
    )
    require(
        not bool(getattr(config, "local_governance_tournament_enabled", False))
        and not bool(getattr(config, "risk_management_optimization_enabled", False)),
        "post_validation_policy_search_not_live",
        "post-validation policy search cannot run in funded live",
    )

    if position_config is not None:
        for code, message in _position_slippage_buffer_issues(position_config):
            add("error", code, message)
        require(
            not bool(getattr(position_config, "auto_widen_sl", False)),
            "auto_widen_sl_not_live",
            "broker-incompatible frozen stops must be rejected",
        )
        require(
            not bool(getattr(position_config, "allow_min_lot_risk_clamp", False)),
            "min_lot_risk_clamp_not_live",
            "unsafe minimum lots must be skipped",
        )
        risk_pct = _to_float(getattr(position_config, "risk_per_trade_pct", None), 0.0)
        hard_risk_pct = _to_float(getattr(position_config, "max_risk_pct", None), 0.0)
        require(
            risk_pct > 0.0 and hard_risk_pct > 0.0 and risk_pct <= hard_risk_pct,
            "risk_pct_exceeds_hard_cap",
            "per-trade risk must be positive and no greater than its hard cap",
        )

    config_file = Path(config_path).resolve()
    config_payload = _load_bounded_json_file(
        config_file, SPARSE_AUTHORITY_MANIFEST_MAX_BYTES
    )
    if not isinstance(config_payload, Mapping):
        config_payload = {}
        add(
            "error",
            "deployment_config_unreadable",
            f"config is unreadable: {config_file}",
        )

    broker_specs: Optional[Dict[str, Any]] = None
    broker_specs_raw = config_payload.get("broker_specs_path")
    if not broker_specs_raw:
        add("error", "broker_specs_path_missing", "broker_specs_path is required")
    else:
        broker_specs_path = _resolve_relative_to_config(broker_specs_raw, config_path)
        broker_specs = _load_bounded_json_file(
            broker_specs_path, SPARSE_AUTHORITY_INDEX_MAX_BYTES
        )
        if not isinstance(broker_specs, dict) or not broker_specs:
            add(
                "error",
                "broker_specs_unreadable",
                f"broker specs are unreadable: {broker_specs_path}",
            )
        else:
            missing_specs = sorted(
                set(requested_symbols) - set(_broker_specs_by_symbol(broker_specs))
            )
            if missing_specs:
                add(
                    "error",
                    "broker_specs_missing_symbols",
                    f"broker specs missing symbols: {_preview_set(missing_specs)}",
                )
            spec_issues = _broker_spec_field_issues(broker_specs, requested_symbols)
            if spec_issues:
                add(
                    "error",
                    "broker_specs_invalid_fields",
                    f"broker specs contain invalid fields: {_preview_set(spec_issues)}",
                )

    ledger_raw = winner_ledger_path or getattr(
        config, "winner_ledger_path", "pm_configs.json"
    )
    ledger_path = _resolve_relative_to_config(ledger_raw, config_path)
    ledger = _load_bounded_json_file(ledger_path, SPARSE_WINNER_LEDGER_MAX_BYTES)
    selected_cells: Set[str] = set()
    legacy_tradeable_cells: Set[str] = set()
    if not isinstance(ledger, Mapping):
        add(
            "error",
            "winner_ledger_unreadable",
            f"winner ledger is unreadable: {ledger_path}",
        )
    else:
        selected_cells, ledger_errors = _sparse_current_refit_projection(
            ledger, requested_symbols
        )
        legacy_tradeable_cells = _tradeable_cell_ids_from_ledger(ledger)
        for error in ledger_errors:
            code = error.split(":", 1)[0].lower()
            add("error", code, f"sparse current-refit ledger rejected: {error}")
        if not selected_cells and not legacy_tradeable_cells:
            add(
                "error",
                "winner_ledger_no_tradeable_winners",
                "winner ledger contains no selected executable specialists",
            )
        ledger_symbols = {
            str(symbol).strip().upper() for symbol in ledger if str(symbol).strip()
        }
        missing_symbol_entries = sorted(set(requested_symbols) - ledger_symbols)
        if missing_symbol_entries:
            add(
                "warning",
                "symbols_missing_from_ledger",
                "requested symbols lack physical winner entries: "
                f"{_preview_set(missing_symbol_entries)}",
            )
        if (
            missing_symbol_entries
            and bool(getattr(config, "production_require_full_symbol_ledger", False))
            and not bool(getattr(config, "live_allow_partial_symbol_ledger", False))
        ):
            add(
                "error",
                "production_symbol_ledger_incomplete",
                "winner ledger omits requested symbols: "
                f"{_preview_set(missing_symbol_entries)}",
            )
        expiry_issues = _ledger_live_expiry_issues(
            ledger,
            datetime.now(),
            grace_minutes=int(
                getattr(config, "live_config_expiry_grace_minutes", 0) or 0
            ),
        )
        if expiry_issues:
            add(
                "error",
                "winner_ledger_not_live_current",
                f"winner ledger is stale: {_preview_set(expiry_issues)}",
            )
        magic_owners: Dict[int, str] = {}
        collisions: List[str] = []
        try:
            from pm_position import TradeTagEncoder

            for cell_id in sorted(selected_cells):
                symbol, timeframe, regime = cell_id.split("|")
                magic = TradeTagEncoder.encode_magic(symbol, timeframe, regime)
                prior = magic_owners.get(magic)
                if prior is not None and prior != cell_id:
                    collisions.append(f"{prior}<->{cell_id}")
                magic_owners[magic] = cell_id
        except Exception as exc:
            collisions.append(f"magic_generation_failed:{type(exc).__name__}")
        if collisions:
            add(
                "error",
                "specialist_magic_collision",
                f"specialist identifiers collide: {_preview_set(collisions)}",
            )
        if legacy_tradeable_cells and not selected_cells:
            try:
                from pm_pipeline import artifact_contract_matches, build_artifact_meta

                current_artifact = build_artifact_meta(config)
                drifted = []
                for symbol, payload in ledger.items():
                    if not isinstance(
                        payload, Mapping
                    ) or not _config_has_tradeable_winner(dict(payload)):
                        continue
                    stored = payload.get("artifact_meta")
                    if not artifact_contract_matches(
                        dict(stored or {}), current_artifact
                    ):
                        drifted.append(str(symbol).upper())
                if drifted:
                    add(
                        "error",
                        "winner_ledger_artifact_drift",
                        "winner artifacts differ from the current execution contract: "
                        f"{_preview_set(drifted)}",
                    )
            except Exception as exc:
                add(
                    "error",
                    "winner_ledger_artifact_check_failed",
                    f"winner artifact verification failed: {type(exc).__name__}",
                )

    native_universe: Any = None
    try:
        from pm_native_activation import (
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_SOURCE_UNAVAILABLE,
            resolve_active_native_artifact,
            resolve_active_native_universe,
        )

        data_dir = Path(getattr(config, "data_dir", "data"))
        if not data_dir.is_absolute():
            data_dir = config_file.parent / data_dir
        native_universe = resolve_active_native_universe(
            data_dir,
            requested_symbols,
            required_timeframes=required_timeframes,
            production_contract_required=True,
        )
        if not native_universe.valid or not native_universe.explicit_contract:
            native_errors = list(getattr(native_universe, "errors", []) or [])
            add(
                "error",
                "native_universe_authority_invalid",
                f"active native universe is invalid: {_preview_set(native_errors)}",
            )
        allowed_states = {
            "active",
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_SOURCE_UNAVAILABLE,
            "technical_error",
        }
        bad_states = [
            f"{symbol}|{timeframe}:{native_universe.cell_status(symbol, timeframe)}"
            for symbol in requested_symbols
            for timeframe in required_timeframes
            if native_universe.cell_status(symbol, timeframe) not in allowed_states
        ]
        if bad_states:
            add(
                "error",
                "native_universe_cell_state_invalid",
                f"native cells are unbound or legacy-excluded: {_preview_set(bad_states)}",
            )
        for cell_id in sorted(selected_cells):
            symbol, timeframe, _regime = cell_id.split("|")
            if (
                native_universe.cell_status(symbol, timeframe)
                == NATIVE_SOURCE_UNAVAILABLE
            ):
                add(
                    "error",
                    "winner_uses_source_unavailable_cell",
                    f"selected winner has no authenticated source: {cell_id}",
                )
                continue
            resolution = resolve_active_native_artifact(
                data_dir,
                symbol,
                timeframe,
                production_contract_required=True,
            )
            if not resolution.valid:
                add(
                    "error",
                    "winner_native_source_invalid",
                    f"selected winner source failed revalidation: {cell_id}",
                )
    except Exception as exc:
        add(
            "error",
            "native_universe_authority_check_failed",
            f"native authority check failed: {type(exc).__name__}: {exc}",
        )

    sparse_authority_valid = False
    synchronized_portfolio_evidence_valid = False
    if isinstance(ledger, Mapping) and native_universe is not None:
        authority, authority_errors = _verify_sparse_live_authority(
            config=config,
            config_path=config_path,
            config_payload=config_payload,
            ledger=ledger,
            ledger_path=ledger_path,
            requested_symbols=requested_symbols,
            selected_cells=selected_cells,
            native_universe=native_universe,
            validation_time_utc=datetime.now(timezone.utc),
        )
        for error in authority_errors:
            code = error.split(":", 1)[0].lower()
            add("error", code, f"funded authority rejected: {error}")
        sparse_authority_valid = authority is not None and not authority_errors
        synchronized_portfolio_evidence_valid = bool(
            sparse_authority_valid
            and authority["raw_reverification"]["synchronized_portfolio_replay"]
        )
    require(
        sparse_authority_valid,
        "funded_sparse_authority_unproven",
        "funded live requires raw-reverified sparse Book/current-refit authority",
    )
    require(
        synchronized_portfolio_evidence_valid,
        "synchronized_portfolio_evidence_unproven",
        "funded live requires raw-replayed shared-capital evidence",
    )

    regime_params_file = str(getattr(config, "regime_params_file", "") or "")
    if bool(getattr(config, "live_require_tuned_regime_params", False)):
        missing_params = _cells_missing_tuned_regime_params(
            selected_cells | legacy_tradeable_cells, regime_params_file
        )
        if missing_params:
            add(
                "error",
                "live_tuned_regime_params_missing",
                f"selected winners lack tuned train-only regime params: {_preview_set(missing_params)}",
            )

    if (
        isinstance(ledger, Mapping)
        and isinstance(broker_specs, dict)
        and account_info is not None
        and position_config is not None
    ):
        equity = _to_float(getattr(account_info, "equity", 0.0), 0.0)
        target_risk_pct = _to_float(
            getattr(position_config, "risk_per_trade_pct", 0.0), 0.0
        )
        hard_cap_pct = _to_float(getattr(position_config, "max_risk_pct", 0.0), 0.0)
        specs = _broker_specs_by_symbol(broker_specs)
        unpriced: List[str] = []
        target_excess: List[str] = []
        hard_excess: List[str] = []
        for cell in _iter_tradeable_cell_payloads(ledger):
            symbol = str(cell.get("symbol") or "").upper()
            stop_pips = _extract_static_stop_pips(cell)
            spec = specs.get(symbol)
            label = f"{symbol}|{cell.get('timeframe')}|{cell.get('regime')}"
            if stop_pips is None or not isinstance(spec, dict):
                unpriced.append(label)
                continue
            risk = _min_lot_risk_pct_from_broker_spec(
                spec, stop_pips=stop_pips, equity=equity
            )
            if risk is None:
                unpriced.append(label)
            elif hard_cap_pct > 0 and risk > hard_cap_pct + 1e-9:
                hard_excess.append(label)
            elif target_risk_pct > 0 and risk > target_risk_pct + 1e-9:
                target_excess.append(label)
        if hard_excess:
            add("error", "min_lot_risk_exceeds_hard_cap", _preview_set(hard_excess))
        if target_excess:
            add("error", "min_lot_risk_exceeds_target", _preview_set(target_excess))
        if unpriced:
            add("error", "min_lot_tradeability_unpriced", _preview_set(unpriced))

    require(
        bool(getattr(config, "manual_new_entries_enabled", True)),
        "manual_new_entries_disabled",
        "manual new entries are disabled",
    )
    kill_switch = str(getattr(config, "manual_kill_switch_path", "") or "").strip()
    if kill_switch:
        switch_path = _resolve_relative_to_config(kill_switch, config_path)
        try:
            if switch_path.stat().st_size > 4096:
                raise ValueError("oversized")
            switch_value = switch_path.read_text(encoding="utf-8").strip().lower()
        except FileNotFoundError:
            switch_value = ""
        except Exception:
            switch_value = "unreadable"
        if switch_value in {
            "0",
            "false",
            "off",
            "block",
            "blocked",
            "pause",
            "paused",
            "kill",
            "unreadable",
        }:
            add(
                "error",
                "manual_kill_switch_active",
                f"kill switch blocks: {switch_path}",
            )

    if mt5_config is not None:
        try:
            expected_login = int(getattr(mt5_config, "expected_login", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            expected_login = 0
        expected_server = str(getattr(mt5_config, "expected_server", "") or "").strip()
        expected_currency = str(
            getattr(mt5_config, "expected_currency", "") or ""
        ).strip()
        pinned = expected_login > 0 and bool(expected_server and expected_currency)
        require(
            pinned,
            "mt5_account_identity_not_pinned",
            "MT5 login, server, and currency must be pinned",
        )
        if pinned and account_info is None:
            add(
                "error", "mt5_account_identity_unverified", "MT5 account is unavailable"
            )
        elif pinned:
            if int(getattr(account_info, "login", 0) or 0) != expected_login:
                add("error", "mt5_account_login_mismatch", "MT5 login differs from pin")
            if (
                str(getattr(account_info, "server", "") or "").lower()
                != expected_server.lower()
            ):
                add(
                    "error",
                    "mt5_account_server_mismatch",
                    "MT5 server differs from pin",
                )
            if (
                str(getattr(account_info, "currency", "") or "").upper()
                != expected_currency.upper()
            ):
                add(
                    "error",
                    "mt5_account_currency_mismatch",
                    "MT5 currency differs from pin",
                )

    minimum_equity = number("live_min_account_equity")
    if minimum_equity > 0:
        if account_info is None:
            add("error", "account_equity_unverified", "account equity is unavailable")
        elif _to_float(getattr(account_info, "equity", 0.0), 0.0) < minimum_equity:
            add(
                "error",
                "account_equity_below_live_minimum",
                "account equity is below the live minimum",
            )

    log_dir = Path(getattr(config, "log_dir", "logs") or "logs")
    if not log_dir.is_absolute():
        log_dir = config_file.parent / log_dir
    supervisor_path = log_dir / "institutional_supervisor_state.json"
    if supervisor_path.exists():
        try:
            state = _load_bounded_json_file(supervisor_path, 1024 * 1024)
            if not isinstance(state, Mapping):
                add(
                    "error",
                    "supervisor_state_unreadable",
                    "supervisor state is unreadable",
                )
            elif bool(state.get("active_block")):
                add(
                    "error",
                    "supervisor_latched_from_previous_run",
                    "institutional supervisor is latched",
                )
                add(
                    "warning",
                    "supervisor_latch_from_previous_cycle",
                    "a persisted supervisor latch requires explicit clearance",
                )
                reopen_mode = str(
                    getattr(config, "hard_block_reopen_mode", "") or ""
                ).lower()
                if reopen_mode != "when_clear":
                    add(
                        "error",
                        "automatic_safety_reopen_not_live",
                        "latched safety breakers may reopen only after metrics clear",
                    )
        except Exception as exc:
            add(
                "error",
                "supervisor_state_check_failed",
                f"supervisor state verification failed: {type(exc).__name__}",
            )

    storage_floor = number("storage_pause_entries_below_free_gb")
    if storage_floor > 0:
        try:
            free_gb = shutil.disk_usage(config_file.parent).free / (1024**3)
            if free_gb < storage_floor:
                add(
                    "warning",
                    "storage_free_space_below_advisory_floor",
                    f"free space {free_gb:.2f}GB is below {storage_floor:.2f}GB",
                )
        except Exception:
            add("warning", "storage_free_space_check_failed", "free space check failed")

    if bool(getattr(config, "production_require_promotion_gate", False)):
        mode = str(getattr(config, "production_promotion_gate_mode", "") or "").lower()
        if mode in {"immutable", "authorization", "authorization_exact"}:
            mode = "immutable_exact"
        require(
            mode == "immutable_exact",
            "production_promotion_gate_not_immutable_exact",
            "funded promotion requires immutable_exact authorization",
        )
        if mode == "immutable_exact" and isinstance(ledger, Mapping):
            try:
                from pm_promotion_authorization import verify_promotion_authorization

                authorization = verify_promotion_authorization(
                    config,
                    ledger,
                    ledger_path=ledger_path,
                    config_path=config_file,
                    verify_active_native=True,
                )
                for error in authorization.errors:
                    add(
                        "error",
                        "production_promotion_authorization_invalid",
                        f"promotion authorization rejected: {error}",
                    )
            except Exception as exc:
                add(
                    "error",
                    "production_promotion_authorization_check_failed",
                    f"promotion authorization failed: {type(exc).__name__}",
                )

    return ReadinessReport(
        not any(item.severity == "error" for item in findings), findings
    )


def log_readiness_report(logger: Any, report: ReadinessReport) -> None:
    for finding in report.findings:
        line = f"Live readiness {finding.severity.upper()} [{finding.code}]: {finding.message}"
        if finding.severity == "error":
            logger.error(line)
        elif finding.severity == "warning":
            logger.warning(line)
        else:
            logger.info(line)
