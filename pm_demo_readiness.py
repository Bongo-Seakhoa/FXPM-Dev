"""Fail-closed readiness for current-refit DEMO probation."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    ContextManager,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from pm_live_readiness import (
    SPARSE_AUTHORITY_MANIFEST_MAX_BYTES,
    SPARSE_AUTHORITY_MAX_SYMBOLS,
    SPARSE_AUTHORITY_RAW_REVERIFICATION,
    SPARSE_AUTHORITY_RESULT_SCHEMA,
    SPARSE_WINNER_LEDGER_MAX_BYTES,
    ReadinessFinding,
    ReadinessReport,
    _ledger_symbol_payloads,
    _ledger_live_expiry_issues,
    _load_bounded_json_file,
    _resolve_relative_to_config,
    _resolve_sparse_authority_indexes,
    _sparse_current_refit_projection,
    _to_float,
)


DEMO_PROBATION_AUTHORITY_STATUS = "DEMO_PROBATION_AUTHORIZED"
DEMO_EXECUTION_BINDING_SCHEMA = "fxpm.demo-execution-binding.v1"
DEMO_MANAGEMENT_BINDING_SCHEMA = "fxpm.demo-management-runtime-binding.v1"
DEMO_MANAGEMENT_STORE_RELATIVE = Path("runtime/demo_management_authority")


@dataclass(frozen=True)
class AuthorizedDemoAccountIdentity:
    """The sole broker account authorized for executable DEMO probation."""

    login: int
    server: str
    currency: str
    trade_mode: int


AUTHORIZED_DEMO_ACCOUNT_IDENTITY = AuthorizedDemoAccountIdentity(
    login=106165989,
    server="FBS-Demo",
    currency="USD",
    trade_mode=0,
)
DEMO_PROBATION_CALLER_CONTRACT = {
    "ledger_role": "PROCESS_VALIDATED_CURRENT_REFIT",
    "account_login": AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login,
    "account_server": AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server,
    "account_currency": AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency,
    "account_trade_mode": "DEMO",
    "require_funded_authorization": False,
    "funded_live_eligible": False,
    "startup_order": "VALIDATE_BEFORE_TRADER_CONSTRUCTION",
    "pre_send_account_revalidation": True,
    "management_capsule_required": True,
    "management_raw_reverification": True,
    "management_retention_pins_required": True,
}


@dataclass(frozen=True)
class _DemoExecutionBinding:
    schema_version: str
    ledger_path: str
    ledger_sha256: str
    projection_sha256: str
    detector_projection_sha256: str
    detector_projection_json: str
    config_path: str
    config_sha256: str
    capital_config_sha256: str
    position_config_sha256: str
    selected_capital_envelope: Tuple[Tuple[str, float], ...]
    native_activation_id: str
    native_activation_manifest_sha256: str
    authority_result_sha256: str
    requested_symbols: Tuple[str, ...]
    selected_cell_ids: Tuple[str, ...]
    authority_result_json: str = ""


@dataclass(frozen=True)
class _DemoManagementBinding:
    schema_version: str
    store_root: str
    capsule_id: str
    generation_id: str
    management_context_sha256: str
    management_context_json: str


@dataclass
class DemoProbationReadinessReport(ReadinessReport):
    _execution_binding: Optional[_DemoExecutionBinding] = None
    _management_binding: Optional[_DemoManagementBinding] = None

    @property
    def execution_ledger_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.ledger_sha256 if binding is not None else None

    @property
    def execution_selected_cell_ids(self) -> Tuple[str, ...]:
        binding = self._execution_binding
        return binding.selected_cell_ids if binding is not None else ()

    @property
    def execution_projection_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.projection_sha256 if binding is not None else None

    @property
    def execution_detector_projection_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.detector_projection_sha256 if binding is not None else None

    @property
    def execution_detector_projection(self) -> Dict[str, Dict[str, Any]]:
        binding = self._execution_binding
        if binding is None:
            return {}
        payload = json.loads(binding.detector_projection_json)
        return dict(payload.get("records") or {})

    @property
    def execution_authority_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.authority_result_sha256 if binding is not None else None

    @property
    def execution_capital_envelope(self) -> Dict[str, float]:
        binding = self._execution_binding
        return dict(binding.selected_capital_envelope) if binding is not None else {}

    @property
    def execution_capital_config_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.capital_config_sha256 if binding is not None else None

    @property
    def execution_position_config_sha256(self) -> Optional[str]:
        binding = self._execution_binding
        return binding.position_config_sha256 if binding is not None else None

    @property
    def management_capsule_id(self) -> Optional[str]:
        binding = self._management_binding
        return binding.capsule_id if binding is not None else None

    @property
    def management_generation_id(self) -> Optional[str]:
        binding = self._management_binding
        return binding.generation_id if binding is not None else None

    @property
    def management_store_root(self) -> Optional[str]:
        binding = self._management_binding
        return binding.store_root if binding is not None else None

    @property
    def management_context_sha256(self) -> Optional[str]:
        binding = self._management_binding
        return binding.management_context_sha256 if binding is not None else None

    @property
    def management_context(self) -> Dict[str, Any]:
        binding = self._management_binding
        if binding is None:
            return {}
        value = json.loads(binding.management_context_json)
        return dict(value) if isinstance(value, dict) else {}


_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
_REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
_ROSTER_FIELDS = (
    "selected_cell_ids",
    "source_unavailable_cell_ids",
    "regime_unobserved_cell_ids",
    "no_local_winner_cell_ids",
    "insufficient_evidence_cell_ids",
    "technical_cell_ids",
    "unsearched_cell_ids",
    "missing_observed_cell_ids",
)
_POSITION_FLOAT_FIELDS = (
    "risk_per_trade_pct",
    "max_position_size",
    "min_position_size",
    "max_risk_pct",
    "risk_tolerance_pct",
    "trailing_stop_pips",
    "trailing_activation_pips",
    "breakeven_trigger_pips",
    "breakeven_offset_pips",
    "slippage_pips",
)
_POSITION_BOOL_FIELDS = (
    "auto_widen_sl",
    "allow_min_lot_risk_clamp",
    "use_trailing_stop",
    "use_breakeven_stop",
    "use_spread",
    "use_slippage",
)
_POSITION_POLICY_FIELDS = frozenset(
    {
        *_POSITION_FLOAT_FIELDS,
        *_POSITION_BOOL_FIELDS,
        "risk_basis",
        "max_trade_duration_bars",
        "regime_tp_multipliers",
    }
)
_DETECTOR_RECORD_SCHEMA = "fxpm.regime-detector-record/v1"
_DETECTOR_PROJECTION_SCHEMA = "fxpm.demo-regime-detector-projection.v1"
_DETECTOR_RECORD_KEYS = frozenset(
    {
        "schema_version",
        "symbol",
        "timeframe",
        "source_group_id",
        "source_group_decision_sha256",
        "source_contract_sha256",
        "source_group_evidence_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "native_source_sha256",
        "fit_cutoff_utc",
        "detector_version",
        "detector_code_sha256",
        "parameters_canonical_json",
        "parameters_sha256",
        "record_sha256",
    }
)
_DETECTOR_PARAMETER_KEYS = (
    "k_confirm",
    "gap_min",
    "k_hold",
    "adx_period",
    "atr_period",
    "bb_period",
    "bb_std",
    "efficiency_period",
    "bb_squeeze_lookback",
    "bb_squeeze_percentile",
    "atr_lookback",
    "structure_period",
    "structure_atr_mult",
    "trend_adx_weight",
    "trend_efficiency_weight",
    "trend_slope_weight",
    "range_adx_weight",
    "range_efficiency_weight",
    "range_containment_weight",
    "breakout_squeeze_weight",
    "breakout_atr_weight",
    "breakout_structure_weight",
    "chop_adx_weight",
    "chop_efficiency_weight",
    "chop_whipsaw_weight",
    "adx_trend_threshold",
)
_DETECTOR_INTEGER_PARAMETERS = frozenset(
    {
        "k_confirm",
        "k_hold",
        "adx_period",
        "atr_period",
        "bb_period",
        "efficiency_period",
        "bb_squeeze_lookback",
        "atr_lookback",
        "structure_period",
    }
)
_AUTHORITY_RESULT_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "errors",
        "raw_reverification",
        *_ROSTER_FIELDS,
        "possible_slot_count",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "ledger_sha256",
        "selected_capital_envelope",
        "capital_config_matches",
        "capital_execution_enforcement_reverified",
        "funded_live_authorized",
    }
)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _reject_duplicate_keys(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_bounded_json_snapshot(
    path: Path, maximum_bytes: int
) -> Tuple[Any, str, Path]:
    resolved = Path(path).resolve(strict=True)
    with resolved.open("rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("not a regular file")
        if before.st_size <= 0 or before.st_size > maximum_bytes:
            raise ValueError("file size outside bound")
        raw = handle.read(maximum_bytes + 1)
        after = os.fstat(handle.fileno())
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if identity_before != identity_after or len(raw) != before.st_size:
        raise ValueError("file changed while reading")
    payload = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )
    return payload, hashlib.sha256(raw).hexdigest(), resolved


def _canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json_text(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and value == value.lower()
        and all(char in "0123456789abcdef" for char in value)
    )


def demo_management_store_root(winner_ledger_path: Any) -> Path:
    raw = os.fspath(winner_ledger_path)
    if type(raw) is not str or not raw:
        raise ValueError("DEMO_MANAGEMENT_LEDGER_PATH_INVALID")
    ledger_path = Path(os.path.abspath(raw))
    return ledger_path.parent / DEMO_MANAGEMENT_STORE_RELATIVE


def list_demo_management_retention_pins(
    winner_ledger_path: Any,
) -> Dict[str, Any]:
    from pm_demo_management import list_demo_management_external_pins

    return list_demo_management_external_pins(
        demo_management_store_root(winner_ledger_path)
    )


def hold_demo_management_retention_for_ledger(
    winner_ledger_path: Any,
    candidate_paths: Sequence[Any],
) -> ContextManager[Dict[str, Any]]:
    from pm_demo_management import hold_demo_management_retention_guard

    return hold_demo_management_retention_guard(
        demo_management_store_root(winner_ledger_path), candidate_paths
    )


def _detector_parameters(value: Any) -> Dict[str, Any]:
    if type(value) is not dict or set(value) != set(_DETECTOR_PARAMETER_KEYS):
        raise ValueError("DEMO_DETECTOR_PARAMETERS_SCHEMA_INVALID")
    normalized: Dict[str, Any] = {}
    for key in _DETECTOR_PARAMETER_KEYS:
        item = value[key]
        if key in _DETECTOR_INTEGER_PARAMETERS:
            if type(item) is not int or not 1 <= item <= 1_000_000:
                raise ValueError("DEMO_DETECTOR_INTEGER_PARAMETER_INVALID")
        elif type(item) is not float or not math.isfinite(item) or abs(item) > 1e9:
            raise ValueError("DEMO_DETECTOR_FLOAT_PARAMETER_INVALID")
        normalized[key] = item
    return normalized


def _native_cell_entry(
    native_universe: Any, symbol: str, timeframe: str
) -> Mapping[str, Any]:
    entries = getattr(native_universe, "entries", None)
    if not isinstance(entries, Mapping):
        raise ValueError("DEMO_DETECTOR_NATIVE_ENTRIES_MISSING")
    symbol_entries = entries.get(symbol)
    if not isinstance(symbol_entries, Mapping):
        raise ValueError(f"DEMO_DETECTOR_NATIVE_SYMBOL_MISSING:{symbol}")
    entry = symbol_entries.get(timeframe)
    if not isinstance(entry, Mapping):
        raise ValueError(f"DEMO_DETECTOR_NATIVE_CELL_MISSING:{symbol}|{timeframe}")
    return entry


def _execution_detector_projection(
    ledger: Mapping[str, Any],
    selected_cell_ids: Sequence[str],
    native_universe: Any,
) -> Dict[str, Any]:
    source_payloads, errors = _ledger_symbol_payloads(ledger)
    if errors:
        raise ValueError(";".join(errors))
    selected: Dict[str, Set[str]] = {}
    cell_bindings: Dict[str, List[Mapping[str, Any]]] = {}
    for cell_id in selected_cell_ids:
        symbol, timeframe, regime = cell_id.split("|")
        selected.setdefault(symbol, set()).add(timeframe)
        payload = source_payloads.get(symbol)
        grid = payload.get("regime_configs") if isinstance(payload, Mapping) else None
        timeframe_grid = grid.get(timeframe) if isinstance(grid, Mapping) else None
        cell = (
            timeframe_grid.get(regime) if isinstance(timeframe_grid, Mapping) else None
        )
        artifact = cell.get("artifact_meta") if isinstance(cell, Mapping) else None
        binding = (
            artifact.get("current_refit_binding")
            if isinstance(artifact, Mapping)
            else None
        )
        if not isinstance(binding, Mapping):
            raise ValueError(f"DEMO_DETECTOR_CELL_BINDING_MISSING:{cell_id}")
        cell_bindings.setdefault(f"{symbol}|{timeframe}", []).append(binding)

    try:
        from pm_regime import detector_code_sha256

        runtime_code_sha = detector_code_sha256()
    except Exception as exc:
        raise ValueError("DEMO_DETECTOR_RUNTIME_CODE_UNAVAILABLE") from exc
    if not _is_sha256(runtime_code_sha):
        raise ValueError("DEMO_DETECTOR_RUNTIME_CODE_INVALID")

    activation_id = getattr(native_universe, "activation_id", None)
    activation_sha = getattr(native_universe, "activation_manifest_sha256", None)
    if (
        type(activation_id) is not str
        or not activation_id
        or not _is_sha256(activation_sha)
    ):
        raise ValueError("DEMO_DETECTOR_NATIVE_ACTIVATION_INVALID")

    records: Dict[str, Dict[str, Any]] = {}
    for symbol, timeframes in sorted(selected.items()):
        payload = source_payloads.get(symbol)
        roster = (
            payload.get("regime_detector_records")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(roster, Mapping):
            raise ValueError(f"DEMO_DETECTOR_ROSTER_MISSING:{symbol}")
        normalized_keys = {str(key).strip().upper(): key for key in roster}
        if len(normalized_keys) != len(roster) or set(normalized_keys) != timeframes:
            raise ValueError(f"DEMO_DETECTOR_ROSTER_INVALID:{symbol}")
        for timeframe in sorted(timeframes):
            key = f"{symbol}|{timeframe}"
            record = roster[normalized_keys[timeframe]]
            if type(record) is not dict or set(record) != _DETECTOR_RECORD_KEYS:
                raise ValueError(f"DEMO_DETECTOR_RECORD_SCHEMA_INVALID:{key}")
            if (
                record["schema_version"] != _DETECTOR_RECORD_SCHEMA
                or record["symbol"] != symbol
                or record["timeframe"] != timeframe
            ):
                raise ValueError(f"DEMO_DETECTOR_RECORD_IDENTITY_INVALID:{key}")
            expected_group = _canonical_json_sha256(
                {
                    "identity_contract": "FXPM_SYMBOL_TIMEFRAME_SOURCE_GROUP_V1",
                    "symbol": symbol,
                    "timeframe": timeframe,
                }
            )
            if record["source_group_id"] != expected_group:
                raise ValueError(f"DEMO_DETECTOR_SOURCE_GROUP_INVALID:{key}")
            for field in (
                "source_group_decision_sha256",
                "source_contract_sha256",
                "source_group_evidence_sha256",
                "activation_manifest_sha256",
                "native_source_sha256",
                "detector_code_sha256",
                "parameters_sha256",
                "record_sha256",
            ):
                if not _is_sha256(record[field]):
                    raise ValueError(f"DEMO_DETECTOR_HASH_INVALID:{key}:{field}")
            if (
                record["activation_id"] != activation_id
                or record["activation_manifest_sha256"] != activation_sha
            ):
                raise ValueError(f"DEMO_DETECTOR_ACTIVATION_SPLICE:{key}")
            native_entry = _native_cell_entry(native_universe, symbol, timeframe)
            if record["native_source_sha256"] != native_entry.get("bars_sha256"):
                raise ValueError(f"DEMO_DETECTOR_NATIVE_SOURCE_SPLICE:{key}")
            if (
                type(record["detector_version"]) is not str
                or not record["detector_version"]
                or len(record["detector_version"].encode("utf-8")) > 256
            ):
                raise ValueError(f"DEMO_DETECTOR_VERSION_INVALID:{key}")
            try:
                cutoff = datetime.fromisoformat(record["fit_cutoff_utc"])
                canonical_cutoff = cutoff.astimezone(timezone.utc).isoformat()
            except Exception as exc:
                raise ValueError(f"DEMO_DETECTOR_CUTOFF_INVALID:{key}") from exc
            if (
                cutoff.tzinfo is None
                or cutoff.utcoffset() != timedelta(0)
                or record["fit_cutoff_utc"] != canonical_cutoff
                or cutoff > datetime.now(timezone.utc) + timedelta(minutes=5)
            ):
                raise ValueError(f"DEMO_DETECTOR_CUTOFF_INVALID:{key}")
            parameters_text = record["parameters_canonical_json"]
            if (
                type(parameters_text) is not str
                or not parameters_text.isascii()
                or not 1 <= len(parameters_text.encode("ascii")) <= 8192
            ):
                raise ValueError(f"DEMO_DETECTOR_PARAMETERS_PAYLOAD_INVALID:{key}")
            try:
                parameters = json.loads(
                    parameters_text,
                    object_pairs_hook=_reject_duplicate_keys,
                    parse_constant=_reject_json_constant,
                )
                parameters = _detector_parameters(parameters)
            except Exception as exc:
                raise ValueError(
                    f"DEMO_DETECTOR_PARAMETERS_PAYLOAD_INVALID:{key}"
                ) from exc
            if _canonical_json_text(parameters) != parameters_text:
                raise ValueError(f"DEMO_DETECTOR_PARAMETERS_NONCANONICAL:{key}")
            if (
                hashlib.sha256(parameters_text.encode("ascii")).hexdigest()
                != record["parameters_sha256"]
            ):
                raise ValueError(f"DEMO_DETECTOR_PARAMETERS_HASH_MISMATCH:{key}")
            record_body = dict(record)
            record_body.pop("record_sha256")
            if _canonical_json_sha256(record_body) != record["record_sha256"]:
                raise ValueError(f"DEMO_DETECTOR_RECORD_HASH_MISMATCH:{key}")
            if record["detector_code_sha256"] != runtime_code_sha:
                raise ValueError(f"DEMO_DETECTOR_CODE_DRIFT:{key}")
            bindings = cell_bindings.get(key) or []
            if not bindings:
                raise ValueError(f"DEMO_DETECTOR_SELECTED_LINK_MISSING:{key}")
            fit_receipts = {item.get("regime_fit_receipt_sha256") for item in bindings}
            record_hashes = {
                item.get("regime_detector_record_sha256") for item in bindings
            }
            parameter_hashes = {
                item.get("regime_detector_parameters_sha256") for item in bindings
            }
            code_hashes = {item.get("regime_detector_code_sha256") for item in bindings}
            if (
                len(fit_receipts) != 1
                or not _is_sha256(next(iter(fit_receipts)))
                or record_hashes != {record["record_sha256"]}
                or parameter_hashes != {record["parameters_sha256"]}
                or code_hashes != {record["detector_code_sha256"]}
            ):
                raise ValueError(f"DEMO_DETECTOR_SELECTED_LINK_INVALID:{key}")
            records[key] = {
                "regime_fit_receipt_sha256": next(iter(fit_receipts)),
                "record_sha256": record["record_sha256"],
                "parameters_sha256": record["parameters_sha256"],
                "detector_code_sha256": record["detector_code_sha256"],
                "parameters": parameters,
            }
    return {"schema_version": _DETECTOR_PROJECTION_SCHEMA, "records": records}


def _execution_symbol_payloads(
    ledger: Mapping[str, Any], selected_cell_ids: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    source_payloads, errors = _ledger_symbol_payloads(ledger)
    if errors:
        raise ValueError(";".join(errors))
    selected: Dict[str, Dict[str, Set[str]]] = {}
    for cell_id in selected_cell_ids:
        symbol, timeframe, regime = cell_id.split("|")
        selected.setdefault(symbol, {}).setdefault(timeframe, set()).add(regime)

    projected: Dict[str, Dict[str, Any]] = {}
    for symbol, timeframe_roster in sorted(selected.items()):
        source = source_payloads.get(symbol)
        if not isinstance(source, Mapping):
            raise ValueError(f"DEMO_EXECUTION_SYMBOL_MISSING:{symbol}")
        payload = json.loads(json.dumps(source, allow_nan=False))
        for field in (
            "default_config",
            "edge_challenger_book",
            "strategy_name",
            "timeframe",
            "parameters",
            "promotion_status",
            "promotion_reasons",
            "regime_detector_records",
        ):
            payload.pop(field, None)
        source_grid = source.get("regime_configs")
        if not isinstance(source_grid, Mapping):
            raise ValueError(f"DEMO_EXECUTION_WINNER_GRID_INVALID:{symbol}")
        grid: Dict[str, Dict[str, Any]] = {}
        for timeframe, regime_roster in sorted(timeframe_roster.items()):
            source_timeframe = next(
                (
                    value
                    for key, value in source_grid.items()
                    if str(key).strip().upper() == timeframe
                ),
                None,
            )
            if not isinstance(source_timeframe, Mapping):
                raise ValueError(
                    f"DEMO_EXECUTION_TIMEFRAME_MISSING:{symbol}|{timeframe}"
                )
            grid[timeframe] = {}
            for regime in sorted(regime_roster):
                source_cell = next(
                    (
                        value
                        for key, value in source_timeframe.items()
                        if str(key).strip().upper() == regime
                    ),
                    None,
                )
                if not isinstance(source_cell, Mapping):
                    raise ValueError(
                        f"DEMO_EXECUTION_CELL_MISSING:{symbol}|{timeframe}|{regime}"
                    )
                grid[timeframe][regime] = json.loads(
                    json.dumps(source_cell, allow_nan=False)
                )
        payload["symbol"] = symbol
        payload["regime_configs"] = grid
        projected[symbol] = payload
    return projected


def _capital_config_sha256(config: Any) -> str:
    payload = {
        "max_total_open_risk_pct": _to_float(
            getattr(config, "max_total_open_risk_pct", None), -1.0
        ),
        "max_factor_group_open_risk_pct": _to_float(
            getattr(config, "max_factor_group_open_risk_pct", None), -1.0
        ),
        "max_combined_risk_pct": _to_float(
            getattr(config, "max_combined_risk_pct", None), -1.0
        ),
        "margin_entry_block_level": _to_float(
            getattr(config, "margin_entry_block_level", None), -1.0
        ),
    }
    return _canonical_json_sha256(payload)


def demo_capital_config_sha256(config: Any) -> str:
    return _capital_config_sha256(config)


def _position_policy_payload(position_config: Any) -> Dict[str, Any]:
    if position_config is None:
        raise ValueError("POSITION_CONFIG_MISSING")
    payload: Dict[str, Any] = {}
    for name in _POSITION_FLOAT_FIELDS:
        raw = getattr(position_config, name)
        if isinstance(raw, bool):
            raise ValueError(f"POSITION_CONFIG_FIELD_INVALID:{name}")
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(f"POSITION_CONFIG_FIELD_INVALID:{name}")
        payload[name] = value
    for name in _POSITION_BOOL_FIELDS:
        raw = getattr(position_config, name)
        if type(raw) is not bool:
            raise ValueError(f"POSITION_CONFIG_FIELD_INVALID:{name}")
        payload[name] = raw
    basis = str(getattr(position_config, "risk_basis") or "").strip().lower()
    if basis not in {"balance", "equity"}:
        raise ValueError("POSITION_CONFIG_FIELD_INVALID:risk_basis")
    payload["risk_basis"] = basis
    duration = getattr(position_config, "max_trade_duration_bars")
    if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
        raise ValueError("POSITION_CONFIG_FIELD_INVALID:max_trade_duration_bars")
    payload["max_trade_duration_bars"] = duration
    raw_multipliers = getattr(position_config, "regime_tp_multipliers")
    if not isinstance(raw_multipliers, Mapping):
        raise ValueError("POSITION_CONFIG_FIELD_INVALID:regime_tp_multipliers")
    expected_regimes = {"TREND", "RANGE", "BREAKOUT", "CHOP"}
    multipliers = {
        str(key).strip().upper(): value for key, value in raw_multipliers.items()
    }
    if set(multipliers) != expected_regimes:
        raise ValueError("POSITION_CONFIG_FIELD_INVALID:regime_tp_multipliers")
    normalized: Dict[str, float] = {}
    for key, raw in multipliers.items():
        if isinstance(raw, bool):
            raise ValueError("POSITION_CONFIG_FIELD_INVALID:regime_tp_multipliers")
        value = float(raw)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError("POSITION_CONFIG_FIELD_INVALID:regime_tp_multipliers")
        normalized[key] = value
    payload["regime_tp_multipliers"] = dict(sorted(normalized.items()))
    if set(payload) != _POSITION_POLICY_FIELDS:
        raise ValueError("POSITION_CONFIG_FIELDS_INVALID")
    return payload


def _expected_position_policy(
    config_payload: Mapping[str, Any], config: Any
) -> Dict[str, Any]:
    from pm_position import PositionConfig

    raw = config_payload.get("position") or {}
    if not isinstance(raw, Mapping):
        raise ValueError("POSITION_CONFIG_SOURCE_INVALID")
    field_names = set(PositionConfig.__dataclass_fields__)
    expected = PositionConfig(
        **{key: value for key, value in raw.items() if key in field_names}
    )
    if "risk_per_trade_pct" not in raw:
        expected.risk_per_trade_pct = float(
            getattr(config, "risk_per_trade_pct", expected.risk_per_trade_pct)
        )
    scope = str(((config_payload.get("_meta") or {}).get("scope") or "")).lower()
    if scope == "funded_live_v1":
        expected.use_spread = bool(getattr(config, "use_spread", True))
    return _position_policy_payload(expected)


def _position_policy_errors(payload: Mapping[str, Any], config: Any) -> List[str]:
    errors: List[str] = []
    risk = float(payload["risk_per_trade_pct"])
    hard = float(payload["max_risk_pct"])
    minimum = float(payload["min_position_size"])
    maximum = float(payload["max_position_size"])
    if not 0.0 < risk <= hard <= float(getattr(config, "max_combined_risk_pct", -1.0)):
        errors.append("DEMO_POSITION_RISK_HIERARCHY_INVALID")
    if minimum <= 0.0 or (maximum != 0.0 and maximum < minimum):
        errors.append("DEMO_POSITION_SIZE_BOUNDS_INVALID")
    if float(payload["risk_tolerance_pct"]) < 0.0:
        errors.append("DEMO_POSITION_RISK_TOLERANCE_INVALID")
    if payload["auto_widen_sl"] or payload["allow_min_lot_risk_clamp"]:
        errors.append("DEMO_POSITION_RISK_RELAXATION_FORBIDDEN")
    if (
        payload["use_trailing_stop"]
        or payload["use_breakeven_stop"]
        or int(payload["max_trade_duration_bars"]) != 0
        or any(
            not math.isclose(float(value), 1.0, rel_tol=0.0, abs_tol=1e-12)
            for value in payload["regime_tp_multipliers"].values()
        )
    ):
        errors.append("DEMO_POSITION_UNVALIDATED_OVERRIDE_FORBIDDEN")
    if payload["use_spread"] != bool(getattr(config, "use_spread", True)):
        errors.append("DEMO_POSITION_SPREAD_CONTRACT_MISMATCH")
    if not payload["use_slippage"]:
        errors.append("DEMO_POSITION_SLIPPAGE_BUFFER_DISABLED")
    if float(payload["slippage_pips"]) < 0.0:
        errors.append("DEMO_POSITION_SLIPPAGE_BUFFER_INVALID")
    return errors


def demo_position_config_sha256(position_config: Any) -> str:
    return _canonical_json_sha256(_position_policy_payload(position_config))


def _requested_symbol_errors(symbols: Sequence[str]) -> List[str]:
    if not 1 <= len(symbols) <= SPARSE_AUTHORITY_MAX_SYMBOLS:
        return ["DEMO_SYMBOL_ROSTER_INVALID"]
    if any(
        not isinstance(symbol, str) or not symbol or len(symbol) > 128 or "|" in symbol
        for symbol in symbols
    ):
        return ["DEMO_SYMBOL_ROSTER_INVALID"]
    normalized = [str(symbol).strip().upper() for symbol in symbols]
    if len(symbols) != len(set(symbols)) or list(symbols) != normalized:
        return ["DEMO_SYMBOL_ROSTER_NOT_CANONICAL"]
    return []


def _partition_errors(
    result: Mapping[str, Any],
    *,
    requested_symbols: Sequence[str],
    selected_cells: Set[str],
    native_universe: Any,
) -> List[str]:
    possible = {
        f"{symbol}|{timeframe}|{regime}"
        for symbol in requested_symbols
        for timeframe in _TIMEFRAMES
        for regime in _REGIMES
    }
    errors: List[str] = []
    if result.get("possible_slot_count") != len(possible):
        errors.append("DEMO_AUTHORITY_POSSIBLE_SLOT_COUNT_INVALID")
    rosters: Dict[str, Set[str]] = {}
    for field in _ROSTER_FIELDS:
        values = result.get(field)
        if (
            not isinstance(values, list)
            or any(not isinstance(value, str) for value in values)
            or len(values) != len(set(values))
            or values != sorted(values)
        ):
            errors.append(f"DEMO_AUTHORITY_CELL_ROSTER_INVALID:{field}")
            rosters[field] = set()
            continue
        roster = set(values)
        rosters[field] = roster
        if not roster.issubset(possible):
            errors.append(f"DEMO_AUTHORITY_CELL_OUTSIDE_BOOK:{field}")
    if rosters.get("selected_cell_ids", set()) != selected_cells:
        errors.append("DEMO_AUTHORITY_SELECTED_ROSTER_MISMATCH")
    seen: Set[str] = set()
    for field in _ROSTER_FIELDS:
        if seen & rosters[field]:
            errors.append(f"DEMO_AUTHORITY_CELL_PARTITION_OVERLAP:{field}")
        seen.update(rosters[field])
    if seen != possible:
        errors.append("DEMO_AUTHORITY_CELL_PARTITION_INCOMPLETE")
    # Only activation-authenticated terminal-technical cells may survive the
    # raw authority verifier.  They are complete zero-attempt abstentions, not a
    # whole-book veto.  Reconcile them independently here so a mocked or forged
    # result cannot dress an ordinary failure as terminal evidence.
    try:
        expected_terminal_technical = {
            f"{symbol}|{timeframe}|{regime}"
            for symbol in requested_symbols
            for timeframe in _TIMEFRAMES
            if native_universe.cell_status(symbol, timeframe) == "technical_error"
            for regime in _REGIMES
        }
    except Exception:
        expected_terminal_technical = set()
        errors.append("DEMO_AUTHORITY_NATIVE_TERMINAL_TECHNICAL_ROSTER_UNAVAILABLE")
    if rosters["technical_cell_ids"] != expected_terminal_technical:
        errors.append("DEMO_AUTHORITY_NATIVE_TERMINAL_TECHNICAL_ROSTER_MISMATCH")
    for field in (
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
    ):
        if rosters[field]:
            errors.append(f"DEMO_AUTHORITY_BLOCKING_CELLS_PRESENT:{field}")
    return errors


def _capital_envelope_errors(
    envelope: Any,
    *,
    config: Any,
) -> List[str]:
    keys = {
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
    }
    if not isinstance(envelope, Mapping) or set(envelope) != keys:
        return ["DEMO_AUTHORITY_CAPITAL_ENVELOPE_INVALID"]
    values = {name: _to_float(envelope.get(name), -1.0) for name in keys}
    total = values["max_total_stop_risk_pct"]
    factor = values["max_factor_stop_risk_pct"]
    symbol = values["max_same_symbol_stop_risk_pct"]
    margin = values["max_margin_utilization_pct"]
    errors: List[str] = []
    if not 0.0 < symbol <= factor <= total < 100.0:
        errors.append("DEMO_AUTHORITY_CAPITAL_HIERARCHY_INVALID")
    if not 0.0 < margin < 100.0:
        errors.append("DEMO_AUTHORITY_MARGIN_ENVELOPE_INVALID")
    else:
        if not math.isclose(
            values["min_margin_level_pct"],
            10_000.0 / margin,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            errors.append("DEMO_AUTHORITY_MARGIN_LEVEL_NOT_RECIPROCAL")
        if not math.isclose(
            values["min_free_margin_pct"],
            100.0 - margin,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            errors.append("DEMO_AUTHORITY_FREE_MARGIN_NOT_RECIPROCAL")
    if values["gap_stress_multiple"] < 1.0:
        errors.append("DEMO_AUTHORITY_GAP_STRESS_INVALID")
    # The signed/replay-selected envelope is admission truth; static config only
    # constrained roster construction upstream and must not re-veto a
    # precommitted selection here (seq1644/1684).  The hierarchy, margin/free
    # reciprocal and gap-stress checks above are the envelope's own internal
    # coherence; no equality with static config fields.
    _ = config  # retained for public-API stability; not a post-selection veto
    return errors


def demo_capital_envelope_errors(envelope: Any, config: Any) -> List[str]:
    return _capital_envelope_errors(envelope, config=config)


def verify_demo_probation_authority(
    *,
    config: Any,
    config_path: str,
    config_payload: Mapping[str, Any],
    ledger: Mapping[str, Any],
    ledger_path: Path,
    requested_symbols: Sequence[str],
    selected_cells: Set[str],
    native_universe: Any,
    validation_time_utc: datetime,
    ledger_sha256: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    errors = _requested_symbol_errors(requested_symbols)
    if errors:
        return None, errors
    try:
        snapshot, snapshot_sha, resolved_ledger_path = _read_bounded_json_snapshot(
            ledger_path, SPARSE_WINNER_LEDGER_MAX_BYTES
        )
    except Exception:
        return None, ["DEMO_AUTHORITY_LEDGER_SNAPSHOT_INVALID"]
    if snapshot != ledger:
        errors.append("DEMO_AUTHORITY_LEDGER_SNAPSHOT_MISMATCH")
    if ledger_sha256 is not None and ledger_sha256 != snapshot_sha:
        errors.append("DEMO_AUTHORITY_LEDGER_SHA256_MISMATCH")
    ledger_sha = snapshot_sha
    manifest_path = resolved_ledger_path.with_name(
        f"{resolved_ledger_path.stem}.generation.json"
    )
    manifest = _load_bounded_json_file(
        manifest_path, SPARSE_AUTHORITY_MANIFEST_MAX_BYTES
    )
    if not isinstance(manifest, Mapping):
        return None, ["DEMO_AUTHORITY_MANIFEST_MISSING_OR_UNBOUNDED"]
    if manifest.get("schema_version") != "fxpm.optimization-generation.v2":
        errors.append("DEMO_AUTHORITY_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("generation_status") or "").upper() != "PROMOTED":
        errors.append("DEMO_AUTHORITY_GENERATION_NOT_PROMOTED")
    if manifest.get("technical_failures"):
        errors.append("DEMO_AUTHORITY_TECHNICAL_FAILURES_PRESENT")
    if not ledger_sha or str(manifest.get("ledger_sha256") or "").lower() != ledger_sha:
        errors.append("DEMO_AUTHORITY_LEDGER_SHA256_MISMATCH")
    if not isinstance(manifest.get("authority_chain_receipt"), Mapping):
        errors.append("DEMO_AUTHORITY_CHAIN_RECEIPT_MISSING")
    index_paths, index_errors = _resolve_sparse_authority_indexes(
        manifest, manifest_path
    )
    errors.extend(index_errors)
    if errors:
        return None, list(dict.fromkeys(errors))

    try:
        from pm_optimization_authority import verify_live_readiness_authority_chain
    except Exception:
        return None, ["DEMO_RAW_AUTHORITY_VERIFIER_UNAVAILABLE"]
    try:
        result = verify_live_readiness_authority_chain(
            manifest=dict(manifest),
            manifest_path=manifest_path,
            authority_index_paths=index_paths,
            ledger=ledger,
            ledger_path=resolved_ledger_path,
            config=config,
            config_payload=dict(config_payload),
            config_path=Path(config_path).resolve(),
            requested_symbols=tuple(requested_symbols),
            native_universe=native_universe,
            validation_time_utc=validation_time_utc,
            require_funded_authorization=False,
        )
    except Exception as exc:
        return None, [f"DEMO_RAW_AUTHORITY_REVERIFY_FAILED:{type(exc).__name__}"]
    try:
        post_snapshot, post_sha, post_path = _read_bounded_json_snapshot(
            resolved_ledger_path, SPARSE_WINNER_LEDGER_MAX_BYTES
        )
    except Exception:
        return None, ["DEMO_AUTHORITY_LEDGER_CHANGED_DURING_REVERIFY"]
    if (
        post_path != resolved_ledger_path
        or post_sha != ledger_sha
        or post_snapshot != ledger
    ):
        return None, ["DEMO_AUTHORITY_LEDGER_CHANGED_DURING_REVERIFY"]
    if not isinstance(result, Mapping) or set(result) != _AUTHORITY_RESULT_KEYS:
        return None, ["DEMO_AUTHORITY_RESULT_SCHEMA_INVALID"]
    normalized = dict(result)
    if normalized.get("schema_version") != SPARSE_AUTHORITY_RESULT_SCHEMA:
        errors.append("DEMO_AUTHORITY_RESULT_VERSION_INVALID")
    reported_errors = normalized.get("errors")
    if not isinstance(reported_errors, list) or any(
        not isinstance(value, str) or not value for value in reported_errors
    ):
        errors.append("DEMO_AUTHORITY_ERROR_LIST_INVALID")
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
        errors.append("DEMO_AUTHORITY_RAW_REVERIFICATION_INCOMPLETE")
    errors.extend(
        _partition_errors(
            normalized,
            requested_symbols=requested_symbols,
            selected_cells=selected_cells,
            native_universe=native_universe,
        )
    )
    if normalized.get("native_activation_id") != getattr(
        native_universe, "activation_id", None
    ) or normalized.get("native_activation_manifest_sha256") != getattr(
        native_universe, "activation_manifest_sha256", None
    ):
        errors.append("DEMO_AUTHORITY_NATIVE_ACTIVATION_MISMATCH")
    if normalized.get("ledger_sha256") != ledger_sha:
        errors.append("DEMO_AUTHORITY_LEDGER_SHA256_MISMATCH")
    errors.extend(
        _capital_envelope_errors(
            normalized.get("selected_capital_envelope"), config=config
        )
    )
    if normalized.get("capital_config_matches") is not True:
        errors.append("DEMO_AUTHORITY_CAPITAL_CONFIG_UNVERIFIED")
    if normalized.get("capital_execution_enforcement_reverified") is not True:
        errors.append("DEMO_AUTHORITY_CAPITAL_ENFORCEMENT_UNVERIFIED")
    if normalized.get("status") != DEMO_PROBATION_AUTHORITY_STATUS:
        errors.append("DEMO_PROBATION_AUTHORITY_NOT_GRANTED")
    if normalized.get("funded_live_authorized") is not False:
        errors.append("DEMO_AUTHORITY_FUNDED_SCOPE_LEAKAGE")
    return normalized, list(dict.fromkeys(errors))


def demo_account_errors(account_info: Any) -> List[str]:
    if account_info is None:
        return ["DEMO_ACCOUNT_UNAVAILABLE"]

    def value(name: str, default: Any = None) -> Any:
        if isinstance(account_info, Mapping):
            return account_info.get(name, default)
        return getattr(account_info, name, default)

    errors: List[str] = []
    login = value("login")
    server = value("server")
    currency = value("currency")
    if type(login) is not int or login <= 0:
        errors.append("DEMO_ACCOUNT_LOGIN_INVALID")
    elif login != AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login:
        errors.append("DEMO_ACCOUNT_LOGIN_NOT_AUTHORIZED")
    if (
        type(server) is not str
        or not server
        or server != server.strip()
        or any(ord(character) < 32 for character in server)
        or len(server.encode("utf-8")) > 256
    ):
        errors.append("DEMO_ACCOUNT_SERVER_INVALID")
    elif server != AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server:
        errors.append("DEMO_ACCOUNT_SERVER_NOT_AUTHORIZED")
    if (
        type(currency) is not str
        or not currency
        or currency != currency.strip()
        or any(ord(character) < 32 for character in currency)
        or len(currency.encode("utf-8")) > 32
    ):
        errors.append("DEMO_ACCOUNT_CURRENCY_INVALID")
    elif currency != AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency:
        errors.append("DEMO_ACCOUNT_CURRENCY_NOT_AUTHORIZED")
    if (
        type(value("trade_mode")) is not int
        or value("trade_mode") != AUTHORIZED_DEMO_ACCOUNT_IDENTITY.trade_mode
    ):
        errors.append("DEMO_ACCOUNT_NOT_BROKER_DEMO")
    if value("trade_allowed") is not True:
        errors.append("DEMO_ACCOUNT_TRADING_DISABLED")
    if value("trade_expert") is not True:
        errors.append("DEMO_ACCOUNT_EXPERT_TRADING_DISABLED")
    if _to_float(value("equity"), 0.0) <= 0.0:
        errors.append("DEMO_ACCOUNT_EQUITY_INVALID")
    if type(value("margin_mode")) is not int or value("margin_mode") != 2:
        errors.append("DEMO_ACCOUNT_NOT_RETAIL_HEDGING")
    if value("hedge_allowed") is not True:
        errors.append("DEMO_ACCOUNT_HEDGING_DISABLED")
    return errors


def validate_demo_probation_readiness(
    config: Any,
    *,
    symbols: Iterable[str],
    config_path: str = "config.json",
    winner_ledger_path: Optional[str] = None,
    account_info: Any = None,
    position_config: Any = None,
) -> ReadinessReport:
    findings: List[ReadinessFinding] = []

    def add(severity: str, code: str, message: str, scope: str = "evidence") -> None:
        findings.append(ReadinessFinding(severity, code, message, scope))

    def require(condition: bool, code: str, message: str) -> None:
        if not condition:
            add("error", code, message)

    requested_symbols = list(
        dict.fromkeys(
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        )
    )
    for error in _requested_symbol_errors(requested_symbols):
        add("error", error.lower(), error)
    try:
        target_count = int(
            getattr(config, "production_symbol_pool_target_count", 0) or 0
        )
    except (TypeError, ValueError, OverflowError):
        target_count = -1
    require(
        target_count == 0 or target_count == len(requested_symbols),
        "demo_symbol_pool_count_mismatch",
        "DEMO probation must cover the configured production symbol pool",
    )
    require(
        tuple(
            str(value).strip().upper()
            for value in (getattr(config, "timeframes", []) or [])
            if str(value).strip()
        )
        == _TIMEFRAMES,
        "demo_timeframe_contract_invalid",
        "DEMO probation requires the six production timeframes",
    )
    require(
        not bool(getattr(config, "research_mode", False))
        and str(getattr(config, "production_split_mode", "") or "").lower()
        == "calendar_lockbox_v2",
        "demo_process_contract_invalid",
        "DEMO probation requires the validated calendar process",
    )
    for field in (
        "evidence_sufficiency_enabled",
        "evidence_require_native_timeframes",
        "evidence_require_native_provenance",
        "evidence_require_session_coverage",
    ):
        require(
            bool(getattr(config, field, False)),
            f"demo_{field}_disabled",
            f"{field} must be enabled for DEMO evidence",
        )
    require(
        bool(getattr(config, "use_spread", True)),
        "demo_use_spread_disabled",
        "use_spread cannot be disabled for DEMO evidence",
    )
    require(
        bool(getattr(config, "swap_model_enabled", False))
        and str(getattr(config, "backtest_spread_mode", "") or "").lower()
        == "conservative_max",
        "demo_cost_contract_invalid",
        "DEMO probation requires spread and swap modeling",
    )

    config_file = Path(config_path).resolve()
    config_sha: Optional[str] = None
    try:
        config_payload, config_sha, config_file = _read_bounded_json_snapshot(
            config_file, SPARSE_AUTHORITY_MANIFEST_MAX_BYTES
        )
    except Exception:
        config_payload = {}
        add("error", "demo_config_unreadable", f"config is unreadable: {config_file}")
    if not isinstance(config_payload, Mapping):
        config_payload = {}
        add(
            "error", "demo_config_not_object", f"config is not an object: {config_file}"
        )
    position_config_sha: Optional[str] = None
    try:
        position_policy = _position_policy_payload(position_config)
        expected_position_policy = _expected_position_policy(config_payload, config)
        if position_policy != expected_position_policy:
            add(
                "error",
                "demo_position_config_source_mismatch",
                "DEMO_POSITION_CONFIG_SOURCE_MISMATCH",
            )
        for error in _position_policy_errors(position_policy, config):
            add("error", error.lower(), error)
        position_config_sha = _canonical_json_sha256(position_policy)
    except Exception as exc:
        add(
            "error",
            "demo_position_config_invalid",
            f"DEMO_POSITION_CONFIG_INVALID:{type(exc).__name__}",
        )
    ledger_raw = winner_ledger_path or getattr(
        config, "winner_ledger_path", "pm_configs.json"
    )
    ledger_path = _resolve_relative_to_config(ledger_raw, config_path)
    ledger_sha: Optional[str] = None
    resolved_ledger_path: Optional[Path] = None
    try:
        ledger, ledger_sha, resolved_ledger_path = _read_bounded_json_snapshot(
            ledger_path, SPARSE_WINNER_LEDGER_MAX_BYTES
        )
        ledger_path = resolved_ledger_path
    except Exception:
        ledger = None
    selected_cells: Set[str] = set()
    if not isinstance(ledger, Mapping):
        add("error", "demo_probation_ledger_unreadable", str(ledger_path))
    else:
        selected_cells, ledger_errors = _sparse_current_refit_projection(
            ledger, requested_symbols
        )
        for error in ledger_errors:
            add("error", error.split(":", 1)[0].lower(), error)
        expiry = _ledger_live_expiry_issues(
            ledger,
            datetime.now(),
            grace_minutes=int(
                getattr(config, "live_config_expiry_grace_minutes", 0) or 0
            ),
        )
        if expiry:
            add("error", "demo_probation_ledger_stale", ",".join(expiry[:20]))

    native_universe: Any = None
    detector_projection: Optional[Dict[str, Any]] = None
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
            required_timeframes=_TIMEFRAMES,
            production_contract_required=True,
        )
        if not native_universe.valid or not native_universe.explicit_contract:
            add("error", "demo_native_universe_invalid", "native authority invalid")
        allowed = {
            "active",
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_SOURCE_UNAVAILABLE,
            "technical_error",
        }
        if any(
            native_universe.cell_status(symbol, timeframe) not in allowed
            for symbol in requested_symbols
            for timeframe in _TIMEFRAMES
        ):
            add("error", "demo_native_cell_state_invalid", "native state unresolved")
        for cell_id in sorted(selected_cells):
            symbol, timeframe, _regime = cell_id.split("|")
            if (
                native_universe.cell_status(symbol, timeframe)
                == NATIVE_SOURCE_UNAVAILABLE
            ):
                add("error", "demo_winner_source_unavailable", cell_id)
                continue
            resolution = resolve_active_native_artifact(
                data_dir,
                symbol,
                timeframe,
                production_contract_required=True,
            )
            if not resolution.valid:
                add("error", "demo_winner_source_invalid", cell_id)
        if isinstance(ledger, Mapping) and selected_cells:
            detector_projection = _execution_detector_projection(
                ledger,
                tuple(sorted(selected_cells)),
                native_universe,
            )
    except Exception as exc:
        add(
            "error",
            "demo_native_authority_failed",
            f"{type(exc).__name__}:{exc}",
        )

    authority: Optional[Dict[str, Any]] = None
    if isinstance(ledger, Mapping) and native_universe is not None:
        authority, authority_errors = verify_demo_probation_authority(
            config=config,
            config_path=config_path,
            config_payload=config_payload,
            ledger=ledger,
            ledger_path=ledger_path,
            requested_symbols=requested_symbols,
            selected_cells=selected_cells,
            native_universe=native_universe,
            validation_time_utc=datetime.now(timezone.utc),
            ledger_sha256=ledger_sha,
        )
        for error in authority_errors:
            add("error", error.split(":", 1)[0].lower(), error)
    else:
        add("error", "demo_sparse_authority_unavailable", "authority inputs missing")

    require(
        bool(getattr(config, "manual_new_entries_enabled", True)),
        "demo_manual_entries_disabled",
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
                "demo_manual_kill_switch_active",
                f"kill switch blocks: {switch_path}",
                "runtime",
            )
    for error in demo_account_errors(account_info):
        add("error", error.lower(), error, "runtime")
    execution_binding: Optional[_DemoExecutionBinding] = None
    if not any(finding.severity == "error" for finding in findings):
        try:
            if (
                not isinstance(ledger, Mapping)
                or not isinstance(authority, Mapping)
                or resolved_ledger_path is None
                or ledger_sha is None
                or config_sha is None
                or not isinstance(detector_projection, Mapping)
            ):
                raise ValueError("authority projection unavailable")
            projection = _execution_symbol_payloads(
                ledger, tuple(sorted(selected_cells))
            )
            envelope = authority.get("selected_capital_envelope")
            if not isinstance(envelope, Mapping):
                raise ValueError("capital envelope unavailable")
            execution_binding = _DemoExecutionBinding(
                schema_version=DEMO_EXECUTION_BINDING_SCHEMA,
                ledger_path=str(resolved_ledger_path),
                ledger_sha256=ledger_sha,
                projection_sha256=_canonical_json_sha256(projection),
                detector_projection_sha256=_canonical_json_sha256(detector_projection),
                detector_projection_json=_canonical_json_text(detector_projection),
                config_path=str(config_file),
                config_sha256=config_sha,
                capital_config_sha256=_capital_config_sha256(config),
                position_config_sha256=str(position_config_sha or ""),
                selected_capital_envelope=tuple(
                    sorted((str(key), float(value)) for key, value in envelope.items())
                ),
                native_activation_id=str(authority.get("native_activation_id") or ""),
                native_activation_manifest_sha256=str(
                    authority.get("native_activation_manifest_sha256") or ""
                ),
                authority_result_sha256=_canonical_json_sha256(authority),
                requested_symbols=tuple(requested_symbols),
                selected_cell_ids=tuple(sorted(selected_cells)),
                authority_result_json=_canonical_json_text(authority),
            )
        except Exception as exc:
            add("error", "demo_execution_projection_invalid", type(exc).__name__)
    return DemoProbationReadinessReport(
        ok=not any(finding.severity == "error" for finding in findings),
        findings=findings,
        _execution_binding=execution_binding,
    )


def _capture_demo_management_authority(
    *,
    report: DemoProbationReadinessReport,
    binding: _DemoExecutionBinding,
    projection: Mapping[str, Any],
    position_config: Any,
    account_info: Any,
    native_universe: Any,
) -> _DemoManagementBinding:
    from pm_demo_management import (
        CAPSULE_WRITTEN,
        MANAGEMENT_ONLY,
        MANAGEMENT_PERMISSIONS,
        create_demo_management_capsule,
        list_demo_management_external_pins,
        reverify_demo_management_capsule,
    )

    if not report.ok or report._execution_binding != binding:
        raise ValueError("DEMO_MANAGEMENT_EXECUTION_AUTHORITY_INVALID")
    authority = json.loads(binding.authority_result_json)
    if (
        not isinstance(authority, dict)
        or _canonical_json_sha256(authority) != binding.authority_result_sha256
    ):
        raise ValueError("DEMO_MANAGEMENT_AUTHORITY_RESULT_INVALID")
    store = demo_management_store_root(binding.ledger_path)
    authorized_at = datetime.now(timezone.utc)
    created = create_demo_management_capsule(
        store,
        authorization_report=report,
        authority_result=authority,
        execution_projection=dict(projection),
        position_config=position_config,
        account_info=account_info,
        native_universe=native_universe,
        authorized_at_utc=authorized_at,
    )
    capsule_id = created.get("capsule_id") if isinstance(created, Mapping) else None
    generation_id = (
        created.get("generation_id") if isinstance(created, Mapping) else None
    )
    if (
        not isinstance(created, Mapping)
        or created.get("status") != CAPSULE_WRITTEN
        or not _is_sha256(capsule_id)
        or type(generation_id) is not str
        or not generation_id
        or generation_id != generation_id.strip()
        or len(generation_id.encode("utf-8")) > 256
        or any(ord(character) < 32 for character in generation_id)
    ):
        errors = created.get("errors") if isinstance(created, Mapping) else []
        raise ValueError(
            str(errors[0])
            if isinstance(errors, list) and errors
            else "CAPSULE_WRITE_BLOCKED"
        )
    verified = reverify_demo_management_capsule(
        store,
        capsule_id,
        account_info=account_info,
        native_universe=native_universe,
        validation_time_utc=datetime.now(timezone.utc),
    )
    context = (
        verified.get("management_context") if isinstance(verified, Mapping) else None
    )
    if (
        not isinstance(verified, Mapping)
        or verified.get("status") != MANAGEMENT_ONLY
        or verified.get("capsule_id") != capsule_id
        or verified.get("generation_id") != generation_id
        or verified.get("permissions") != MANAGEMENT_PERMISSIONS
        or not isinstance(context, dict)
        or context.get("status") != MANAGEMENT_ONLY
        or context.get("capsule_id") != capsule_id
        or context.get("generation_id") != generation_id
        or context.get("permissions") != MANAGEMENT_PERMISSIONS
    ):
        errors = verified.get("errors") if isinstance(verified, Mapping) else []
        raise ValueError(
            str(errors[0])
            if isinstance(errors, list) and errors
            else "CAPSULE_REVERIFY_BLOCKED"
        )
    pins = list_demo_management_external_pins(store)
    if (
        not isinstance(pins, Mapping)
        or pins.get("status") != MANAGEMENT_ONLY
        or capsule_id not in pins.get("capsule_ids", [])
        or generation_id not in pins.get("generation_ids", [])
    ):
        raise ValueError("DEMO_MANAGEMENT_RETENTION_PINS_UNAVAILABLE")
    context_json = _canonical_json_text(context)
    return _DemoManagementBinding(
        schema_version=DEMO_MANAGEMENT_BINDING_SCHEMA,
        store_root=str(store.resolve(strict=True)),
        capsule_id=capsule_id,
        generation_id=generation_id,
        management_context_sha256=_canonical_json_sha256(context),
        management_context_json=context_json,
    )


def authorize_demo_probation_execution(
    config: Any,
    *,
    symbols: Iterable[str],
    config_path: str = "config.json",
    winner_ledger_path: Optional[str] = None,
    account_info: Any = None,
    position_config: Any = None,
) -> Tuple[DemoProbationReadinessReport, Optional[Dict[str, Dict[str, Any]]]]:
    first_report = validate_demo_probation_readiness(
        config,
        symbols=symbols,
        config_path=config_path,
        winner_ledger_path=winner_ledger_path,
        account_info=account_info,
        position_config=position_config,
    )
    if not first_report.ok or first_report._execution_binding is None:
        return first_report, None
    report = validate_demo_probation_readiness(
        config,
        symbols=symbols,
        config_path=config_path,
        winner_ledger_path=winner_ledger_path,
        account_info=account_info,
        position_config=position_config,
    )
    binding = report._execution_binding
    if not report.ok or binding is None:
        return report, None
    if binding != first_report._execution_binding:
        report.findings.append(
            ReadinessFinding(
                "error",
                "demo_execution_authority_changed_during_reverification",
                "DEMO_EXECUTION_AUTHORITY_CHANGED_DURING_REVERIFICATION",
                "evidence",
            )
        )
        report.ok = False
        report._execution_binding = None
        return report, None
    try:
        if binding.schema_version != DEMO_EXECUTION_BINDING_SCHEMA:
            raise ValueError("DEMO_EXECUTION_BINDING_MISSING")
        _config_payload, config_sha, config_resolved = _read_bounded_json_snapshot(
            Path(binding.config_path), SPARSE_AUTHORITY_MANIFEST_MAX_BYTES
        )
        if str(config_resolved) != binding.config_path:
            raise ValueError("DEMO_EXECUTION_CONFIG_PATH_MISMATCH")
        if config_sha != binding.config_sha256:
            raise ValueError("DEMO_EXECUTION_CONFIG_SHA256_MISMATCH")
        if _capital_config_sha256(config) != binding.capital_config_sha256:
            raise ValueError("DEMO_EXECUTION_CAPITAL_CONFIG_CHANGED")
        if _capital_envelope_errors(
            dict(binding.selected_capital_envelope), config=config
        ):
            raise ValueError("DEMO_EXECUTION_CAPITAL_ENVELOPE_CHANGED")
        current_position_policy = _position_policy_payload(position_config)
        if (
            _canonical_json_sha256(current_position_policy)
            != binding.position_config_sha256
        ):
            raise ValueError("DEMO_EXECUTION_POSITION_CONFIG_CHANGED")
        if current_position_policy != _expected_position_policy(
            _config_payload, config
        ):
            raise ValueError("DEMO_EXECUTION_POSITION_CONFIG_SOURCE_MISMATCH")
        if _position_policy_errors(current_position_policy, config):
            raise ValueError("DEMO_EXECUTION_POSITION_CONFIG_INVALID")
        ledger, ledger_sha, ledger_resolved = _read_bounded_json_snapshot(
            Path(binding.ledger_path), SPARSE_WINNER_LEDGER_MAX_BYTES
        )
        if str(ledger_resolved) != binding.ledger_path:
            raise ValueError("DEMO_EXECUTION_LEDGER_PATH_MISMATCH")
        if ledger_sha != binding.ledger_sha256:
            raise ValueError("DEMO_EXECUTION_LEDGER_SHA256_MISMATCH")
        if not isinstance(ledger, Mapping):
            raise ValueError("DEMO_EXECUTION_LEDGER_INVALID")
        selected, errors = _sparse_current_refit_projection(
            ledger, binding.requested_symbols
        )
        if errors:
            raise ValueError(";".join(errors))
        if tuple(sorted(selected)) != binding.selected_cell_ids:
            raise ValueError("DEMO_EXECUTION_SELECTED_ROSTER_MISMATCH")
        projection = _execution_symbol_payloads(ledger, binding.selected_cell_ids)
        if not projection:
            raise ValueError("DEMO_EXECUTION_PROJECTION_EMPTY")
        if _canonical_json_sha256(projection) != binding.projection_sha256:
            raise ValueError("DEMO_EXECUTION_PROJECTION_SHA256_MISMATCH")
        from pm_native_activation import resolve_active_native_universe

        data_dir = Path(getattr(config, "data_dir", "data"))
        if not data_dir.is_absolute():
            data_dir = config_resolved.parent / data_dir
        native_universe = resolve_active_native_universe(
            data_dir,
            binding.requested_symbols,
            required_timeframes=_TIMEFRAMES,
            production_contract_required=True,
        )
        if (
            not native_universe.valid
            or native_universe.activation_id != binding.native_activation_id
            or native_universe.activation_manifest_sha256
            != binding.native_activation_manifest_sha256
        ):
            raise ValueError("DEMO_EXECUTION_NATIVE_AUTHORITY_CHANGED")
        detector_projection = _execution_detector_projection(
            ledger,
            binding.selected_cell_ids,
            native_universe,
        )
        if (
            _canonical_json_sha256(detector_projection)
            != binding.detector_projection_sha256
            or _canonical_json_text(detector_projection)
            != binding.detector_projection_json
        ):
            raise ValueError("DEMO_EXECUTION_DETECTOR_PROJECTION_CHANGED")
    except Exception as exc:
        report.findings.append(
            ReadinessFinding(
                "error",
                "demo_execution_projection_reverification_failed",
                str(exc),
                "evidence",
            )
        )
        report.ok = False
        report._execution_binding = None
        return report, None
    try:
        report._management_binding = _capture_demo_management_authority(
            report=report,
            binding=binding,
            projection=projection,
            position_config=position_config,
            account_info=account_info,
            native_universe=native_universe,
        )
    except Exception as exc:
        report.findings.append(
            ReadinessFinding(
                "error",
                "demo_management_authority_failed",
                str(exc),
                "evidence",
            )
        )
        report.ok = False
        report._execution_binding = None
        report._management_binding = None
        return report, None
    return report, projection


__all__ = [
    "AUTHORIZED_DEMO_ACCOUNT_IDENTITY",
    "AuthorizedDemoAccountIdentity",
    "DEMO_EXECUTION_BINDING_SCHEMA",
    "DEMO_MANAGEMENT_BINDING_SCHEMA",
    "DEMO_MANAGEMENT_STORE_RELATIVE",
    "DEMO_PROBATION_AUTHORITY_STATUS",
    "DEMO_PROBATION_CALLER_CONTRACT",
    "DemoProbationReadinessReport",
    "authorize_demo_probation_execution",
    "demo_account_errors",
    "demo_capital_config_sha256",
    "demo_capital_envelope_errors",
    "demo_management_store_root",
    "demo_position_config_sha256",
    "hold_demo_management_retention_for_ledger",
    "list_demo_management_retention_pins",
    "validate_demo_probation_readiness",
    "verify_demo_probation_authority",
]
