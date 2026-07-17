"""
FX Portfolio Manager - Main Application
========================================

Main entry point for the FX Portfolio Manager.

This application:
1. Loads or optimizes strategy configurations for all symbols
2. Monitors markets for entry signals on each bar close
3. Executes trades based on optimized strategies
4. Applies the fixed production retrain schedule from config.json
5. Logs all activity and trades

Stateful Optimization:
- Configs are persisted incrementally (never lost on interruption)
- Valid configs are skipped (re-run resumes where it left off)
- Use --overwrite to force re-optimization of all symbols

Usage:
    python pm_main.py --optimize              # Optimize (skip valid configs)
    python pm_main.py --optimize --overwrite  # Force re-optimize all
    python pm_main.py --trade                 # Live trading with existing configs
    python pm_main.py --trade --paper         # Current-refit demo probation
    python pm_main.py --preflight             # Readiness checks only, no trading loop
    python pm_main.py --preflight --paper     # DEMO readiness only, no trading loop
    python pm_main.py --status                # Show current status

Version: 3.1 (Portfolio Manager with Stateful Optimization)
"""

import argparse
import copy
import hashlib
import json
import logging
import math
import os
import signal
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields

import pandas as pd
import numpy as np

ACTIONABLE_SCHEMA_VERSION = "2026-06-18-entry-slippage-v1"
MARGIN_PROTECTION_STATE_SCHEMA_VERSION = "2026-07-11-margin-protection-v1"
CHAMPION_EXECUTION_BOOK = "LOCKBOX_CHAMPION"
CURRENT_REFIT_DEMO_EXECUTION_BOOK = "PROCESS_VALIDATED_CURRENT_REFIT_DEMO"
_MAX_OPTIMIZATION_MANIFEST_BYTES = 16 * 1024 * 1024
RUNTIME_OBSERVABILITY_SCHEMA_VERSION = "2026-07-15-runtime-observability-v1"
_MAX_RUNTIME_OBSERVABILITY_RECORDS = 100 * 6 * 4
_MAX_RUNTIME_OBSERVABILITY_RECORD_BYTES = 64 * 1024
_MAX_RUNTIME_OBSERVABILITY_FILE_BYTES = 32 * 1024 * 1024
_DEMO_PREFLIGHT_MAX_MT5_TIMEOUT_MS = 60_000


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_bounded_json(path: Path, max_bytes: int) -> Any:
    if path.stat().st_size > max_bytes:
        raise ValueError("JSON_FILE_TOO_LARGE")
    with path.open("rb") as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("JSON_FILE_TOO_LARGE")
    return json.loads(raw.decode("utf-8"))


class RuntimeObservabilityStore:
    """Bounded runtime telemetry kept outside the promoted authority ledger."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._records: Dict[str, Dict[str, Any]] = {}
        self._load()

    @staticmethod
    def _cell_id(symbol: str, timeframe: str, regime: str) -> str:
        parts = tuple(
            str(value or "").strip().upper()
            for value in (symbol, timeframe, regime)
        )
        if any(not value or len(value) > 128 or "|" in value for value in parts):
            raise ValueError("RUNTIME_OBSERVABILITY_CELL_ID_INVALID")
        return "|".join(parts)

    @staticmethod
    def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

    @classmethod
    def _payload(cls, records: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "schema_version": RUNTIME_OBSERVABILITY_SCHEMA_VERSION,
            "records": {key: dict(records[key]) for key in sorted(records)},
        }
        body["content_sha256"] = hashlib.sha256(cls._canonical_bytes(body)).hexdigest()
        return body

    @classmethod
    def _validated_records(cls, payload: Any) -> Dict[str, Dict[str, Any]]:
        if type(payload) is not dict:
            raise ValueError("RUNTIME_OBSERVABILITY_PAYLOAD_INVALID")
        digest = payload.get("content_sha256")
        body = dict(payload)
        body.pop("content_sha256", None)
        if (
            payload.get("schema_version") != RUNTIME_OBSERVABILITY_SCHEMA_VERSION
            or type(digest) is not str
            or hashlib.sha256(cls._canonical_bytes(body)).hexdigest() != digest
        ):
            raise ValueError("RUNTIME_OBSERVABILITY_CONTENT_HASH_INVALID")
        raw_records = payload.get("records")
        if type(raw_records) is not dict or len(raw_records) > _MAX_RUNTIME_OBSERVABILITY_RECORDS:
            raise ValueError("RUNTIME_OBSERVABILITY_ROSTER_INVALID")
        records: Dict[str, Dict[str, Any]] = {}
        for key, record in raw_records.items():
            if type(key) is not str or type(record) is not dict:
                raise ValueError("RUNTIME_OBSERVABILITY_RECORD_INVALID")
            expected = cls._cell_id(
                record.get("symbol"), record.get("timeframe"), record.get("regime")
            )
            if key != expected:
                raise ValueError("RUNTIME_OBSERVABILITY_CELL_ID_MISMATCH")
            if len(cls._canonical_bytes(record)) > _MAX_RUNTIME_OBSERVABILITY_RECORD_BYTES:
                raise ValueError("RUNTIME_OBSERVABILITY_RECORD_TOO_LARGE")
            if type(record.get("observability")) is not dict:
                raise ValueError("RUNTIME_OBSERVABILITY_RECORD_INVALID")
            records[key] = dict(record)
        return records

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            payload = _load_bounded_json(
                self.path, _MAX_RUNTIME_OBSERVABILITY_FILE_BYTES
            )
            self._records = self._validated_records(payload)
        except Exception as exc:
            logging.getLogger(__name__).error(
                "Runtime observability state rejected (%s): %s",
                type(exc).__name__,
                exc,
            )
            self._records = {}

    def get(self, symbol: str, timeframe: str, regime: str) -> Dict[str, Any]:
        key = self._cell_id(symbol, timeframe, regime)
        with self._lock:
            record = self._records.get(key) or {}
            return dict(record.get("observability") or {})

    def update(
        self,
        symbol: str,
        timeframe: str,
        regime: str,
        observability: Mapping[str, Any],
    ) -> Dict[str, Any]:
        if type(observability) is not dict:
            raise ValueError("RUNTIME_OBSERVABILITY_UPDATE_INVALID")
        key = self._cell_id(symbol, timeframe, regime)
        record = {
            "symbol": str(symbol).strip().upper(),
            "timeframe": str(timeframe).strip().upper(),
            "regime": str(regime).strip().upper(),
            "observability": dict(observability),
            "updated_at_utc": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
        }
        if len(self._canonical_bytes(record)) > _MAX_RUNTIME_OBSERVABILITY_RECORD_BYTES:
            raise ValueError("RUNTIME_OBSERVABILITY_RECORD_TOO_LARGE")
        with self._lock:
            candidate = dict(self._records)
            candidate[key] = record
            if len(candidate) > _MAX_RUNTIME_OBSERVABILITY_RECORDS:
                raise ValueError("RUNTIME_OBSERVABILITY_ROSTER_TOO_LARGE")
            payload = self._payload(candidate)
            encoded = json.dumps(
                payload,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8") + b"\n"
            if len(encoded) > _MAX_RUNTIME_OBSERVABILITY_FILE_BYTES:
                raise ValueError("RUNTIME_OBSERVABILITY_FILE_TOO_LARGE")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp = self.path.with_name(
                f".{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            try:
                with temp.open("xb") as handle:
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp, self.path)
            finally:
                temp.unlink(missing_ok=True)
            self._records = candidate
            return dict(observability)


def _live_file_identity(path: Path) -> tuple[bool, str]:
    """Read-only identity used to detect, never overwrite, external changes."""

    target = Path(path).resolve()
    if not target.exists():
        return False, hashlib.sha256(b"").hexdigest()
    return True, _stream_sha256(target)

# Import PM modules


def _filter_dataclass_kwargs(cls, data: Dict[str, Any]) -> Dict[str, Any]:
    """Filter dict keys to those accepted by a dataclass constructor."""
    allowed = {f.name for f in fields(cls)}
    return {k: v for k, v in (data or {}).items() if k in allowed}


def _normalize_storage_section_for_pipeline(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a nested `storage` config section into PipelineConfig-compatible keys.

    Supports both already-prefixed keys (`storage_observe_only`) and the natural
    nested form (`observe_only`) so config remains authoritative without silent
    drops.
    """
    payload = dict(data or {})
    if not payload:
        return {}
    allowed = {f.name for f in fields(PipelineConfig)}
    normalized: Dict[str, Any] = {}
    for key, value in payload.items():
        if key in allowed:
            normalized[key] = value
            continue
        prefixed = f"storage_{key}"
        if prefixed in allowed:
            normalized[prefixed] = value
    return normalized


def load_config_json(path: str) -> Dict[str, Any]:
    """Load config JSON. Returns empty dict if missing."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise RuntimeError(f"Failed to load config file '{path}': {e}")


def _resolve_config_relative_path(value: Any, config_path: Any) -> Path:
    """Resolve one runtime path using the selected config file as authority."""
    path = Path(str(value or ""))
    if path.is_absolute():
        return path.resolve()
    return (Path(config_path).resolve().parent / path).resolve()


def _phase_a_optimization_complete(
    ledger_payload: Any,
    symbols: List[str],
) -> Tuple[bool, List[str]]:
    """Recognize a complete frozen roster without making it live eligible."""
    try:
        from pm_two_phase_lockbox import build_frozen_roster

        _roster, errors = build_frozen_roster(ledger_payload, symbols)
    except Exception as exc:
        return False, [f"PHASE_A_ROSTER_CHECK_FAILED:{type(exc).__name__}:{exc}"]
    return not errors, list(errors)


def _governed_generation_cli_verdict(
    outcome: Any,
    ledger_path: Any,
    expected_manifest_path: Any = None,
) -> Tuple[bool, List[str]]:
    """Verify the exact promoted generation used for the CLI exit code."""
    errors: List[str] = []
    if outcome is None:
        return False, ["OPTIMIZATION_GENERATION_OUTCOME_MISSING"]

    status = str(getattr(outcome, "status", "") or "").strip().upper()
    if status != "PROMOTED":
        errors.append(
            f"OPTIMIZATION_GENERATION_STATUS_NOT_PROMOTED:{status or 'EMPTY'}"
        )
    if not bool(getattr(outcome, "promoted", False)):
        errors.append("OPTIMIZATION_GENERATION_PROMOTED_FLAG_FALSE")
    # A genuinely PROMOTED authority generation over byte-identical winner bytes
    # honestly reports ledger_changed=False (seq1686/1691).  Promotion is proven
    # by status==PROMOTED + the promoted flag + the exact promoted_ledger_sha256
    # match below; ledger_changed only reports the book-byte delta and governs
    # winner-config cache invalidation, not the CLI verdict, so it is NOT a
    # rejection criterion here.
    if dict(getattr(outcome, "technical_failures", {}) or {}):
        errors.append("OPTIMIZATION_GENERATION_TECHNICAL_FAILURES_PRESENT")

    expected_sha = (
        str(getattr(outcome, "promoted_ledger_sha256", "") or "").strip().lower()
    )
    if len(expected_sha) != 64 or any(
        ch not in "0123456789abcdef" for ch in expected_sha
    ):
        errors.append("OPTIMIZATION_GENERATION_LEDGER_SHA256_INVALID")

    target = Path(ledger_path).resolve()
    try:
        observed_sha = _stream_sha256(target)
    except Exception as exc:
        errors.append(f"OPTIMIZATION_GENERATION_LEDGER_UNREADABLE:{type(exc).__name__}")
    else:
        if expected_sha and observed_sha != expected_sha:
            errors.append("OPTIMIZATION_GENERATION_LEDGER_SHA256_MISMATCH")

    manifest_path = Path(str(getattr(outcome, "manifest_path", "") or ""))
    if expected_manifest_path is not None:
        try:
            if manifest_path.resolve() != Path(expected_manifest_path).resolve():
                errors.append("OPTIMIZATION_GENERATION_MANIFEST_PATH_MISMATCH")
        except Exception as exc:
            errors.append(
                f"OPTIMIZATION_GENERATION_MANIFEST_PATH_INVALID:{type(exc).__name__}"
            )
    try:
        manifest = _load_bounded_json(
            manifest_path,
            _MAX_OPTIMIZATION_MANIFEST_BYTES,
        )
    except Exception as exc:
        errors.append(
            f"OPTIMIZATION_GENERATION_MANIFEST_UNREADABLE:{type(exc).__name__}"
        )
        manifest = {}
    if not isinstance(manifest, dict):
        errors.append("OPTIMIZATION_GENERATION_MANIFEST_NOT_OBJECT")
    else:
        if manifest.get("schema_version") != "fxpm.optimization-generation.v2":
            errors.append("OPTIMIZATION_GENERATION_MANIFEST_SCHEMA_INVALID")
        if str(manifest.get("generation_status") or "").upper() != "PROMOTED":
            errors.append("OPTIMIZATION_GENERATION_MANIFEST_NOT_PROMOTED")
        if str(manifest.get("generation_id") or "") != str(
            getattr(outcome, "generation_id", "") or ""
        ):
            errors.append("OPTIMIZATION_GENERATION_ID_MISMATCH")
        if str(manifest.get("ledger_sha256") or "").lower() != expected_sha:
            errors.append("OPTIMIZATION_GENERATION_MANIFEST_LEDGER_SHA256_MISMATCH")
        ledger_status = manifest.get("ledger_status")
        if not isinstance(ledger_status, dict) or not bool(
            ledger_status.get("complete_current_contract_universe", False)
        ):
            errors.append("OPTIMIZATION_GENERATION_LEDGER_CONTRACT_INCOMPLETE")
        if dict(manifest.get("technical_failures") or {}):
            errors.append("OPTIMIZATION_GENERATION_MANIFEST_FAILURES_PRESENT")
        manifest_receipt = manifest.get("authority_chain_receipt")
        outcome_receipt = getattr(outcome, "authority_chain_receipt", None)
        if not isinstance(manifest_receipt, dict):
            errors.append("OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_MISSING")
        elif outcome_receipt != manifest_receipt:
            errors.append("OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_MISMATCH")
        else:
            try:
                from pm_optimization_authority import (
                    verify_optimization_authority_chain,
                )
            except Exception:
                errors.append("OPTIMIZATION_AUTHORITY_CHAIN_VERIFIER_UNAVAILABLE")
            else:
                try:
                    authority_errors = verify_optimization_authority_chain(
                        manifest=manifest,
                        outcome=outcome,
                        ledger_path=target,
                    )
                except Exception as exc:
                    errors.append(
                        "OPTIMIZATION_AUTHORITY_CHAIN_REVERIFY_FAILED:"
                        f"{type(exc).__name__}"
                    )
                else:
                    if not isinstance(authority_errors, (list, tuple)):
                        errors.append(
                            "OPTIMIZATION_AUTHORITY_CHAIN_VERIFIER_RESULT_INVALID"
                        )
                    else:
                        errors.extend(str(value) for value in authority_errors)
    return not errors, list(dict.fromkeys(errors))


def _is_research_discovery_only_config(config_data: Dict[str, Any]) -> bool:
    """Return True when a config is explicitly marked as non-trading research."""
    meta = (config_data or {}).get("_meta") or {}
    scope = str(meta.get("scope", "") or "").strip().lower()
    return scope == "research_discovery_only_not_live_trading"


FUNDED_LIVE_CONFIG_SCOPE = "funded_live_v1"

# Versioned, positive configuration surface for funded production.  The
# research dataclasses intentionally retain historical knobs for reproducible
# experiments, but those knobs must not silently become funded-live behavior.
# Adding a new production knob therefore requires an explicit contract review
# here instead of being admitted merely because somebody added a dataclass
# field.  These are the keys shipped by config.json after fixed invariants and
# research-only row split controls are removed.
FUNDED_PIPELINE_CONFIG_KEYS = frozenset(
    """
    allow_d1_plus_lower_tf backtest_spread_mode daily_loss_hard_block_pct
    data_dir discovery_risk_per_trade_pct evidence_as_of_utc
    evidence_boundary_tolerance_days evidence_min_expected_session_coverage
    evidence_min_native_bars evidence_purge_days
    evidence_require_native_provenance evidence_require_native_timeframes
    evidence_require_session_coverage evidence_sufficiency_enabled
    evidence_timeframe_window_years evidence_warmup_days evidence_window_years
    native_history_window_contract exceptional_val_profit_factor
    exceptional_val_return_pct false_discovery_bootstrap_confidence
    false_discovery_bootstrap_max_dd_upper
    false_discovery_bootstrap_min_mean_lower
    false_discovery_bootstrap_min_pf_lower
    false_discovery_bootstrap_repetitions false_discovery_confidence_floor
    false_discovery_control_enabled false_discovery_holdout_max_drawdown_pct
    false_discovery_holdout_min_profit_factor
    false_discovery_holdout_min_return_pct false_discovery_holdout_min_trades
    false_discovery_holdout_policy false_discovery_max_pbo
    false_discovery_min_daily_observations false_discovery_min_dsr
    false_discovery_min_effective_observations false_discovery_pbo_blocks
    false_discovery_pbo_min_candidates false_discovery_require_bootstrap
    feature_cache_max_entries feature_disk_cache_dir feature_disk_cache_enabled
    feature_disk_cache_max_gb feature_disk_cache_min_free_gb
    fx_gap_penalty_lambda fx_min_robustness_ratio
    fx_robustness_boost fx_stability_penalty_k
    fx_val_max_drawdown fx_val_sharpe_override
    hard_block_reopen_mode initial_capital
    live_allow_partial_symbol_ledger live_allowed_promotion_statuses
    live_artifact_drift_policy live_min_account_equity
    live_min_account_equity_currency live_outcome_feed_cycle_drift
    live_outcome_feedback_enabled live_outcome_ledger_path
    live_outcome_require_position_history live_outcome_state_path
    live_outcome_volume_tolerance live_require_tuned_regime_params
    lockbox_access_registry_path lockbox_one_shot_registry_enabled log_dir
    manual_kill_switch_path manual_new_entries_enabled margin_entry_block_level
    margin_panic_level margin_recovery_start_level margin_reopen_level
    max_combined_risk_pct max_new_entries_when_total_open_risk_above_pct
    max_factor_group_open_risk_pct max_total_open_risk_pct
    oos_evidence_execution_stress_mode
    oos_evidence_export_enabled oos_evidence_include_execution_cost_survival
    oos_evidence_min_periods oos_evidence_periods
    oos_validation_mode open_drawdown_hard_block_pct
    open_risk_action optimization_max_workers optuna_family_size_aware_budget
    optuna_max_trials_per_strategy optuna_min_trials_per_strategy
    optuna_target_coverage_pct output_dir probation_days probation_enabled
    probation_min_live_outcomes probation_min_mean_r
    probation_min_profit_factor probation_risk_multiplier
    production_prelockbox_roster_path production_prelockbox_roster_sha256
    production_cap_calibration_path production_cap_calibration_sha256
    production_portfolio_replay_evidence_path
    production_portfolio_replay_evidence_required
    production_portfolio_replay_max_drawdown_pct
    production_portfolio_replay_min_calendar_days
    production_portfolio_replay_min_positions
    production_portfolio_replay_min_profit_factor
    production_portfolio_replay_min_return_pct
    production_portfolio_replay_require_mtm
    production_portfolio_replay_required_starting_balances
    production_promotion_authorization_max_age_days
    production_promotion_authorization_path
    production_promotion_authorization_sha256 production_promotion_gate_mode
    production_replay_audit_block_unverified production_replay_audit_enabled
    production_replay_audit_mode production_require_full_symbol_ledger
    production_require_promotion_gate production_retrain_anchor_date
    production_retrain_interval_weeks production_retrain_mode
    production_retrain_time production_retrain_weekday production_split_mode
    production_symbol_pool_target_count
    regime_enable_hyperparam_tuning regime_hyperparam_max_combos
    regime_hyperparam_top_k regime_min_train_trades
    regime_min_val_profit_factor regime_min_val_return_pct
    regime_min_val_trades regime_params_file regime_scoring_model
    regime_timeframe_val_trade_floors
    regime_timeframe_val_trade_floors_enabled regime_trade_stability_floor
    scoring_use_consistency scoring_use_continuous_dd scoring_use_sortino_blend
    scoring_use_tail_risk screening_atr_anchor_enabled
    screening_atr_anchor_multipliers secondary_trade_max_risk_pct
    session_loss_hard_block_pct slippage_pips
    storage_pause_entries_below_free_gb swap_model_enabled
    swap_model_min_holding_hours swap_model_triple_rollover_day
    timeframes train_max_drawdown train_min_profit_factor train_min_return_pct
    walk_forward_audit_windows
    weekly_loss_hard_block_pct winner_ledger_path
    """.split()
)

# Raw, versioned policy objects consumed by strict downstream authorities.
# They intentionally are not PipelineConfig fields: flattening a structured
# precommitment into the legacy runtime dataclass would silently discard its
# schema and candidate-roster boundaries.  The funded JSON validator admits
# only these named pass-through objects; each owning authority then performs
# its full exact-field and semantic verification before use.
FUNDED_RAW_PIPELINE_POLICY_KEYS = frozenset({"production_capital_authority"})

# Live position policy has one short, reviewed surface.  Trailing-stop,
# breakeven, time-exit, duplicate cost, stop-widening, minimum-lot clamp, and
# TP-geometry switches remain available to research objects but cannot be
# introduced through a funded JSON profile.
FUNDED_POSITION_CONFIG_KEYS = frozenset(
    {
        "risk_per_trade_pct",
        "max_position_size",
        "min_position_size",
        "risk_basis",
        "max_risk_pct",
        "risk_tolerance_pct",
    }
)

# Evidence-backed funded invariants. Alternate scoring models and purged-kfold
# workflows remain constructible for research, but all completed honest
# per-cell comparisons retained frequency-off legacy as the production scorer,
# and the native two-phase contract requires one fixed final lockbox.
FUNDED_FIXED_PIPELINE_VALUES = {
    "regime_scoring_model": "legacy",
    "oos_validation_mode": "fixed_holdout",
}


def _funded_storage_config_keys() -> Set[str]:
    """Return only real storage settings, never arbitrary pipeline fields."""
    pipeline_names = {item.name for item in fields(PipelineConfig)}
    prefixed = {name for name in pipeline_names if name.startswith("storage_")}
    natural = {name[len("storage_") :] for name in prefixed}
    filenames = {"state_filename", "manifest_filename", "ledger_filename"}
    return prefixed | natural | filenames


def _config_scope(config_data: Dict[str, Any]) -> str:
    meta = (config_data or {}).get("_meta") or {}
    return str(meta.get("scope", "") or "").strip().lower()


def _apply_funded_position_cost_contract(
    config_data: Dict[str, Any],
    pipeline_config: "PipelineConfig",
    position_config: "PositionConfig",
) -> None:
    """Bind live position math to the funded backtest cost contract.

    PositionConfig retains its historic cost fields for research compatibility,
    but funded JSON cannot override them independently.  Copying the resolved
    production values prevents the backtest and live risk projection from
    drifting if either dataclass default changes in the future.
    """
    if _config_scope(config_data) != FUNDED_LIVE_CONFIG_SCOPE:
        return
    position_config.use_spread = bool(getattr(pipeline_config, "use_spread", True))
    position_config.use_slippage = bool(getattr(pipeline_config, "use_slippage", True))
    position_config.slippage_pips = float(
        getattr(pipeline_config, "slippage_pips", 0.0) or 0.0
    )


def _apply_funded_pipeline_contract(
    config_data: Dict[str, Any],
    pipeline_config: "PipelineConfig",
) -> None:
    """Bind omitted funded invariants to reviewed production values."""
    if _config_scope(config_data) != FUNDED_LIVE_CONFIG_SCOPE:
        return
    for field_name, expected in FUNDED_FIXED_PIPELINE_VALUES.items():
        setattr(pipeline_config, field_name, expected)


def _validate_funded_config_schema(config_data: Dict[str, Any]) -> None:
    """Reject unknown material keys in the versioned funded-live profile.

    Research configs retain tolerant loading for historical reproducibility. A
    funded profile must fail loudly when a safety key is misspelled instead of
    silently falling back to a dataclass default.
    """
    if _config_scope(config_data) != FUNDED_LIVE_CONFIG_SCOPE:
        raise ValueError(
            f"funded execution requires _meta.scope={FUNDED_LIVE_CONFIG_SCOPE!r}"
        )
    allowed_top_level = {
        "_meta",
        "pipeline",
        "broker_specs_path",
        "instrument_spec_defaults",
        "instrument_specs",
        "symbols",
        "position",
        "storage",
        "mt5",
    }
    unknown_top = sorted(set(config_data or {}) - allowed_top_level)
    if unknown_top:
        raise ValueError(f"unknown funded-live top-level config keys: {unknown_top}")

    checks = (
        (
            "pipeline",
            FUNDED_PIPELINE_CONFIG_KEYS | FUNDED_RAW_PIPELINE_POLICY_KEYS,
        ),
        ("position", FUNDED_POSITION_CONFIG_KEYS),
        ("mt5", {item.name for item in fields(MT5Config)} | {"terminal_path"}),
    )
    for section_name, allowed in checks:
        payload = (config_data or {}).get(section_name) or {}
        if not isinstance(payload, dict):
            raise ValueError(
                f"funded-live config section {section_name!r} must be an object"
            )
        unknown = sorted(set(payload) - allowed)
        if unknown:
            raise ValueError(
                f"unsupported funded-live {section_name} config keys: {unknown}. "
                "Research-only, internal, and fixed-policy dataclass fields are "
                "not funded-profile overrides."
            )

    pipeline_payload = (config_data or {}).get("pipeline") or {}
    for field_name, expected in FUNDED_FIXED_PIPELINE_VALUES.items():
        if field_name in pipeline_payload and pipeline_payload[field_name] != expected:
            raise ValueError(
                f"funded-live pipeline {field_name!r} is fixed to {expected!r}; "
                f"received {pipeline_payload[field_name]!r}"
            )

    storage_payload = (config_data or {}).get("storage") or {}
    if not isinstance(storage_payload, dict):
        raise ValueError("funded-live config section 'storage' must be an object")
    allowed_storage = _funded_storage_config_keys()
    unknown_storage = sorted(
        key for key in storage_payload if key not in allowed_storage
    )
    if unknown_storage:
        raise ValueError(f"unknown funded-live storage config keys: {unknown_storage}")


def log_resolved_config_summary(
    logger: logging.Logger,
    config_path: str,
    config_data: Dict[str, Any],
    pipeline_config: "PipelineConfig",
    position_config: "PositionConfig",
    mt5_config: "MT5Config",
) -> None:
    """Log a concise, sanitized summary of the resolved configuration."""
    try:
        provided_sections = sorted((config_data or {}).keys())
        sections_str = ", ".join(provided_sections) if provided_sections else "none"

        logger.info(f"Resolved config summary (source={config_path})")
        logger.info(f"  sections: {sections_str}")

        # Pipeline (most impactful runtime levers)
        p = pipeline_config
        tf_str = ",".join(getattr(p, "timeframes", []) or [])
        logger.info(
            "  pipeline: "
            f"bars={getattr(p, 'max_bars', None)} | "
            f"tfs={tf_str} | "
            f"live={getattr(p, 'live_bars_count', None)}/{getattr(p, 'live_min_bars', None)} | "
            f"min_trade={getattr(p, 'min_trade_risk_pct', None)}% | "
            f"risk_scalars={getattr(p, 'live_risk_scalars_mode', None) or ('on' if getattr(p, 'live_risk_scalars_enabled', False) else 'off')} | "
            f"exit_pack={getattr(p, 'market_driven_exit_pack_mode', 'off')} | "
            f"d1+lower={getattr(p, 'allow_d1_plus_lower_tf', None)} | "
            f"secondary_cap={getattr(p, 'secondary_trade_max_risk_pct', None)}% | "
            f"max_sym_risk={getattr(p, 'max_combined_risk_pct', None)}% | "
            f"opt_workers={getattr(p, 'optimization_max_workers', None)}"
        )
        logger.info(
            "            "
            f"validation: opt_min={getattr(p, 'fx_opt_min_trades', None)} | "
            f"val_min={getattr(p, 'fx_val_min_trades', None)} | "
            f"val_dd<={getattr(p, 'fx_val_max_drawdown', None)} | "
            f"pf>={getattr(p, 'regime_min_val_profit_factor', None)} | "
            f"ret>={getattr(p, 'regime_min_val_return_pct', None)} | "
            f"ret/dd>={getattr(p, 'regime_min_val_return_dd_ratio', None)}"
        )
        logger.info(
            "            "
            f"retrain: mode={getattr(p, 'production_retrain_mode', None)} | "
            f"schedule={p.describe_retrain_schedule()} | "
            f"anchor={getattr(p, 'production_retrain_anchor_date', None)} | "
            f"poll={getattr(p, 'production_retrain_poll_seconds', None)}s | "
            f"winner_ledger={getattr(p, 'winner_ledger_path', None)} | "
            f"data_dir={getattr(p, 'data_dir', None)} | "
            f"output_dir={getattr(p, 'output_dir', None)} | "
            f"log_dir={getattr(p, 'log_dir', None)}"
        )
        logger.info(
            "            "
            f"spread_filter: enabled={bool(getattr(p, 'execution_spread_filter_enabled', True))} | "
            f"min_edge={getattr(p, 'execution_spread_min_edge_mult', None)}x | "
            f"spike={getattr(p, 'execution_spread_spike_mult', None)}x | "
            f"penalty_start={getattr(p, 'execution_spread_penalty_start_mult', None)}"
        )
        logger.info(
            "            "
            f"storage: enabled={bool(getattr(p, 'storage_enabled', True))} | "
            f"observe_only={bool(getattr(p, 'storage_observe_only', True))} | "
            f"warn={getattr(p, 'storage_warn_free_gb', None)}GB | "
            f"critical={getattr(p, 'storage_critical_free_gb', None)}GB | "
            f"cache_quota={getattr(p, 'storage_resample_cache_max_gb', None)}GB | "
            f"local_data_first={bool(getattr(p, 'storage_local_data_first_enabled', True))} | "
            f"live_sync_bars={getattr(p, 'storage_live_sync_bars', None)} | "
            f"live_overlap_bars={getattr(p, 'storage_live_sync_overlap_bars', None)}"
        )

        # Position config (risk + execution safety)
        pos_cfg = position_config
        logger.info(
            "  position: "
            f"risk={getattr(pos_cfg, 'risk_per_trade_pct', None)}% | "
            f"basis={getattr(pos_cfg, 'risk_basis', None)} | "
            f"hard_cap={getattr(pos_cfg, 'max_risk_pct', None)}% | "
            f"size={getattr(pos_cfg, 'min_position_size', None)}-{getattr(pos_cfg, 'max_position_size', None)} | "
            f"auto_widen_sl={getattr(pos_cfg, 'auto_widen_sl', None)}"
        )

        # MT5 summary (do not log credentials)
        logger.info(
            "  mt5: "
            f"server={getattr(mt5_config, 'server', '') or 'n/a'} | "
            f"path={getattr(mt5_config, 'path', '') or 'n/a'} | "
            f"timeout={getattr(mt5_config, 'timeout', None)} | "
            f"portable={getattr(mt5_config, 'portable', None)}"
        )

        # Instrument specs config
        spec_overrides = (config_data or {}).get("instrument_specs", {}) or {}
        spec_defaults = (config_data or {}).get("instrument_spec_defaults", {}) or {}
        broker_specs_path = (config_data or {}).get("broker_specs_path", None)
        defaults_list = ",".join(spec_defaults.keys()) if spec_defaults else "none"
        logger.info(
            "  instruments: "
            f"overrides={len(spec_overrides)} | "
            f"defaults={defaults_list} | "
            f"broker_specs={broker_specs_path or 'n/a'}"
        )

        logger.info("-" * 60)
    except Exception as e:
        logger.warning(f"Failed to log resolved config summary: {e}")


from pm_core import (
    PipelineConfig,
    DataLoader,
    FeatureComputer,
    Timer,
    get_instrument_spec,
    sync_instrument_spec_from_mt5,
    set_broker_specs_path,
    set_instrument_specs,
    load_broker_specs,
)
from pm_strategies import (
    StrategyRegistry,
    BaseStrategy,
    TradeIntent,
    set_regime_tp_multipliers,
)
from pm_position import PositionConfig, PositionCalculator
from pm_pipeline import PortfolioManager, SymbolConfig
from pm_enhancement_seams import (
    create_default_enhancement_seams,
    RiskScalarContext,
    ExecutionQualityContext,
    PortfolioObservationContext,
)
from pm_order_governance import (
    GovernanceContext,
    evaluate_policy,
    make_policy,
    policy_name_from_artifact,
)
from pm_institutional_supervisor import InstitutionalSupervisor, SupervisorContext
from pm_cycle_drift import CycleDriftContext, CycleDriftMonitor
from pm_live_outcomes import LiveOutcomeError, LiveOutcomeLedger
from pm_model_lifecycle import (
    LOCKBOX_CHAMPION_ROLE,
    PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
)
from pm_risk_admission import (
    GrossRiskLimits,
    StopRiskExposure,
    conservative_risk_capital,
    evaluate_gross_stop_risk,
    evaluate_projected_margin,
)
from pm_event_context import EventCalendar, observe_event_context
from pm_live_readiness import (
    ReadinessFinding,
    log_readiness_report,
    validate_live_readiness,
)
from pm_storage import StorageManager
from pm_mt5 import (
    MT5_AVAILABLE,
    MT5Config,
    MT5Connector,
    MT5OrderResult,
    MT5Position,
    OrderType,
)


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================


def setup_logging(
    log_dir: str = "logs", log_level: str = "INFO", console_level: str = "INFO"
) -> logging.Logger:
    """Setup logging configuration."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_filename = f"pm_{datetime.now().strftime('%Y%m%d')}.log"
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_format)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# =============================================================================
# DEFAULT SYMBOLS
# =============================================================================

DEFAULT_SYMBOLS = [
    # Majors
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "NZDUSD",
    # Crosses
    "AUDNZD",
    "EURGBP",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",
    "EURAUD",
    "EURCHF",
    "EURCAD",
    "EURNZD",
    "GBPAUD",
    "GBPCAD",
    "GBPCHF",
    "CADJPY",
    "NZDJPY",
    # Added Crosses / Minors
    "AUDCAD",
    "AUDCHF",
    "CADCHF",
    "CHFJPY",
    "NZDCAD",
    "NZDCHF",
    "GBPNZD",
    # Exotics (and USD minors)
    "USDNOK",
    "USDMXN",
    "USDSGD",
    "USDZAR",
    "USDPLN",
    "USDSEK",
    # Added Exotic / Minor
    "EURZAR",
    "GBPZAR",
    "USDCNH",
    "EURPLN",
    # Added Nordic + selected replacements
    "EURNOK",
    "EURSEK",
    "EURCNH",
    "GBPNOK",
    "GBPSEK",
    "EURTRY",
    "Platinum",
    "Palladium",
    "EURMXN",
    "NOKJPY",
    # Commodities (Metals + Energy)
    "XAUUSD",
    "XAGUSD",
    "XAUEUR",
    "XAUGBP",
    "XAUAUD",
    "XAGEUR",
    "XRX",
    "XTIUSD",
    "XBRUSD",
    "XNGUSD",
    # Indices
    "US100",
    "US30",
    "DE30",
    "EU50",
    "UK100",
    "JP225",
    "US500",
    "FR40",
    "ES35",
    "HK50",
    "AU200",
    # Crypto (CFDs)
    "BTCUSD",
    "ETHUSD",
    "LTCUSD",
    "SOLUSD",
    "BCHUSD",
    "DOGUSD",
    "TRXUSD",
    "XRPUSD",
    "TONUSD",
    "BTCETH",
    "BTCXAU",
]


# =============================================================================
# Lot-normalization risk gate
# =============================================================================


def classify_lot_normalization_drift(
    actual_risk_pct: float,
    target_risk_pct: float,
    max_risk_pct: float,
    tolerance_pct: float,
) -> str:
    """Classify the severity of lot-normalization risk drift.

    Returns one of:
      * "ok"    — actual at or below target
      * "warn"  — actual above target but inside the allowed envelope
      * "block" — invalid input or actual above the target/tolerance/hard-cap envelope

    Inputs are all percentages of basis (balance/equity). `tolerance_pct` is
    interpreted as a percentage of `target_risk_pct` (e.g. tolerance=10.0 →
    target × 1.10 is the maximum allowed boundary). Negative tolerance is clamped to 0.
    """
    try:
        actual, target, hard_cap, tolerance = map(
            float,
            (actual_risk_pct, target_risk_pct, max_risk_pct, tolerance_pct),
        )
    except (TypeError, ValueError, OverflowError):
        return "block"
    if (
        not all(math.isfinite(value) for value in (actual, target, hard_cap, tolerance))
        or actual <= 0.0
        or target <= 0.0
        or hard_cap <= 0.0
    ):
        return "block"
    eps = 1e-9
    allowed = min(hard_cap, target * (1.0 + max(0.0, tolerance) / 100.0))
    if actual > allowed + eps:
        return "block"
    if actual > target + eps:
        return "warn"
    return "ok"


# =============================================================================
# DECISION THROTTLE (per-symbol, bar-time-aware suppression)
# =============================================================================


@dataclass
class DecisionRecord:
    """
    Immutable record of a decision that was made for one symbol on one bar.

    Persisted to ``last_trade_log.json`` so state survives restarts.
    """

    symbol: str
    decision_key: str  # hash that uniquely identifies the decision
    bar_time: str  # ISO-8601 of the bar timestamp used for the signal
    timeframe: str
    regime: str
    strategy_name: str
    direction: int  # 1 = LONG, -1 = SHORT, 0 = no signal
    action: str  # EXECUTED, PAPER, SKIPPED_RISK_CAP, SKIPPED_MIN_VOLUME,
    # SKIPPED_SPREAD, SKIPPED_NO_SIGNAL, FAILED, …
    action_time: str  # ISO-8601 wall-clock time
    decision_keys: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)


class DecisionThrottle:
    """
    Per-symbol decision cache with bar-time awareness.

    **Rules enforced:**

    1. If the current *decision_key* has already been attempted on the same bar
       → suppress (do not re-attempt, do not re-log).
    2. A new attempt is allowed only when:
       a) a new bar arrives for that timeframe, **or**
       b) the decision_key genuinely changes (different strategy, direction,
          timeframe, regime).

    The cache is flushed to ``last_trade_log.json`` on every write so that a
    restart does not cause immediate re-spam.

    NOTE: There is NO cooldown/expiry mechanism. Suppression is strictly based
    on (decision_key, bar_time) matching. A decision is evaluated exactly once
    per bar per unique decision identity.
    """

    def __init__(
        self, log_path: str = "last_trade_log.json", max_age_hours: float = 24.0
    ):
        if str(log_path) == "last_trade_log.json":
            log_path = os.environ.get("PM_DECISION_THROTTLE_LOG_PATH", log_path)
        self._log_path = log_path
        # symbol → DecisionRecord
        self._cache: Dict[str, DecisionRecord] = {}
        self._max_age_hours = (
            float(max_age_hours) if max_age_hours and max_age_hours > 0 else 0.0
        )
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def make_decision_key(
        symbol: str,
        strategy_name: str,
        timeframe: str,
        regime: str,
        direction: int,
        bar_time_iso: str,
    ) -> str:
        """
        Create a deterministic, short hash that uniquely identifies a decision.

        Components: symbol, strategy, timeframe, regime, direction, bar_time.
        """
        raw = (
            f"{symbol}|{strategy_name}|{timeframe}|{regime}|{direction}|{bar_time_iso}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def should_suppress(
        self, symbol: str, decision_key: str, bar_time_iso: str
    ) -> bool:
        """
        Return ``True`` if this decision should be suppressed (no action,
        no log output).

        A decision is suppressed when ALL of the following hold:
        * We already have a cached record for this symbol.
        * The cached bar_time matches the current one.
        * The decision_key was already seen for that bar.

        There is NO cooldown expiry - suppression is strictly bar-time based.
        """
        prev = self._cache.get(symbol)
        if prev is None:
            return False

        # Different bar -> allow
        if prev.bar_time != bar_time_iso:
            return False

        # Same bar: suppress only if this decision_key was already seen
        if decision_key in (prev.decision_keys or []):
            return True

        return False

    def record_decision(
        self,
        symbol: str,
        decision_key: str,
        bar_time_iso: str,
        timeframe: str,
        regime: str,
        strategy_name: str,
        direction: int,
        action: str,
        cooldown_seconds: int = 0,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Store a decision and persist to disk.

        Parameters
        ----------
        cooldown_seconds : int
            IGNORED - kept for backward compatibility but has no effect.
            Suppression is purely bar-time based.
        """
        # Update or create record. Keep a per-bar list of seen decision keys.
        prev = self._cache.get(symbol)
        ctx = context or {}
        if prev is not None and prev.bar_time == bar_time_iso:
            if decision_key not in prev.decision_keys:
                prev.decision_keys.append(decision_key)
            prev.decision_key = decision_key
            prev.timeframe = timeframe
            prev.regime = regime
            prev.strategy_name = strategy_name
            prev.direction = direction
            prev.action = action
            prev.action_time = datetime.now().isoformat()
            prev.context = ctx
            record = prev
        else:
            record = DecisionRecord(
                symbol=symbol,
                decision_key=decision_key,
                bar_time=bar_time_iso,
                timeframe=timeframe,
                regime=regime,
                strategy_name=strategy_name,
                direction=direction,
                action=action,
                action_time=datetime.now().isoformat(),
                decision_keys=[decision_key],
                context=ctx,
            )
        self._cache[symbol] = record
        self._save()

    def clear_symbol(self, symbol: str) -> None:
        """Remove cached decision for a symbol (e.g. after position close)."""
        if symbol in self._cache:
            del self._cache[symbol]
            self._save()

    def clear_all(self) -> None:
        """Wipe the entire cache."""
        self._cache.clear()
        self._save()

    # ------------------------------------------------------------------
    # Persistence (JSON file – not in-memory-only)
    # ------------------------------------------------------------------

    def _save(self) -> None:
        """Persist cache to ``last_trade_log.json`` (atomic write)."""
        try:
            data = {}
            for sym, rec in self._cache.items():
                data[sym] = {
                    "symbol": rec.symbol,
                    "decision_key": rec.decision_key,
                    "decision_keys": rec.decision_keys,
                    "bar_time": rec.bar_time,
                    "timeframe": rec.timeframe,
                    "regime": rec.regime,
                    "strategy_name": rec.strategy_name,
                    "direction": rec.direction,
                    "action": rec.action,
                    "action_time": rec.action_time,
                    "context": rec.context,
                }
            parent_dir = os.path.dirname(self._log_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
            tmp_path = f"{self._log_path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, self._log_path)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                f"DecisionThrottle: failed to save cache: {exc}"
            )

    def _load(self) -> None:
        """Load cache from ``last_trade_log.json`` if it exists.

        Entries older than ``self._max_age_hours`` (default 24h) are dropped at
        load time so a long process restart does not resurrect stale
        suppressions whose `bar_time` strings can no longer be reasoned about
        relative to the current run.
        """
        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = datetime.now()
            max_age_hours = self._max_age_hours
            purged = 0
            for sym, rec_dict in data.items():
                decision_keys = rec_dict.get("decision_keys")
                if not isinstance(decision_keys, list):
                    dk = rec_dict.get("decision_key", "")
                    decision_keys = [dk] if dk else []

                action_time_iso = rec_dict.get("action_time", "") or ""
                if max_age_hours > 0 and action_time_iso:
                    try:
                        action_dt = datetime.fromisoformat(action_time_iso)
                    except ValueError:
                        action_dt = None
                    if action_dt is not None:
                        age_hours = (now - action_dt).total_seconds() / 3600.0
                        if age_hours > max_age_hours:
                            purged += 1
                            continue

                self._cache[sym] = DecisionRecord(
                    symbol=rec_dict.get("symbol", sym),
                    decision_key=rec_dict.get("decision_key", ""),
                    bar_time=rec_dict.get("bar_time", ""),
                    timeframe=rec_dict.get("timeframe", ""),
                    regime=rec_dict.get("regime", ""),
                    strategy_name=rec_dict.get("strategy_name", ""),
                    direction=rec_dict.get("direction", 0),
                    action=rec_dict.get("action", ""),
                    action_time=rec_dict.get("action_time", ""),
                    decision_keys=decision_keys,
                    context=rec_dict.get("context", {}) or {},
                )
            if purged > 0:
                logging.getLogger(__name__).info(
                    f"DecisionThrottle: purged {purged} stale entries (>{max_age_hours:.0f}h old) on load"
                )
                # Persist the trimmed cache so the file no longer carries stale
                # Keep the on-disk artifact aligned with what the
                # in-memory cache actually contains.
                self._save()
        except FileNotFoundError:
            pass
        except Exception as exc:
            logging.getLogger(__name__).debug(
                f"DecisionThrottle: failed to load cache: {exc}"
            )


# =============================================================================
# ACTIONABLE DECISION LOG (DASHBOARD FEED)
# =============================================================================


class ActionableDecisionLog:
    """
    Persist dashboard-relevant trade outcomes so the dashboard can always show
    the most recent decision per symbol.

    This log is *not* used for throttling. It is purely a read-only feed for
    the dashboard and is never overwritten by NO_ACTIONABLE_SIGNAL events.
    """

    def __init__(self, log_path: str = "last_actionable_log.json") -> None:
        self._log_path = log_path
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load()

    def record(self, symbol: str, record: Dict[str, Any]) -> None:
        if not symbol:
            return
        self._cache[symbol] = record
        self._save()

    def _save(self) -> None:
        try:
            parent_dir = os.path.dirname(self._log_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
            tmp_path = f"{self._log_path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)
            os.replace(tmp_path, self._log_path)
        except Exception as exc:
            logging.getLogger(__name__).debug(
                f"ActionableDecisionLog: failed to save cache: {exc}"
            )

    def _load(self) -> None:
        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._cache = data
        except FileNotFoundError:
            pass
        except Exception as exc:
            logging.getLogger(__name__).debug(
                f"ActionableDecisionLog: failed to load cache: {exc}"
            )


# =============================================================================
# DRIFT MONITOR
# =============================================================================


class DriftMonitor:
    """
    Tracks live performance vs backtest validation metrics and warns on drift.

    Compares rolling live win rate, avg R-multiple, and max drawdown against
    the stored val_metrics for each symbol. Logs WARNING when drift exceeds
    configurable thresholds.
    """

    def __init__(
        self,
        logger: logging.Logger,
        win_rate_threshold: float = 15.0,
        r_mult_threshold: float = 0.5,
        dd_threshold: float = 10.0,
    ):
        """
        Args:
            logger: Logger instance
            win_rate_threshold: Warn if live win rate deviates by this many pct points
            r_mult_threshold: Warn if live avg R-multiple deviates by this amount
            dd_threshold: Warn if live max DD exceeds backtest DD + this
        """
        self.logger = logger
        self.win_rate_threshold = win_rate_threshold
        self.r_mult_threshold = r_mult_threshold
        self.dd_threshold = dd_threshold
        # Per-symbol rolling trade stats
        self._trades: Dict[str, List[Dict[str, Any]]] = {}

    def record_trade(
        self,
        symbol: str,
        pnl_dollars: float,
        pnl_pips: Optional[float] = None,
        r_multiple: Optional[float] = None,
    ):
        """Record a completed trade for drift tracking."""
        if symbol not in self._trades:
            self._trades[symbol] = []
        self._trades[symbol].append(
            {
                "pnl_dollars": pnl_dollars,
                "pnl_pips": pnl_pips,
                "r_multiple": r_multiple,
                "time": datetime.now(),
            }
        )

    def check_drift(self, symbol: str, val_metrics: Dict[str, Any]):
        """Compare live performance against validation metrics. Log warnings."""
        trades = self._trades.get(symbol, [])
        if len(trades) < 5:
            return  # Not enough data to judge

        wins = sum(1 for t in trades if t.get("pnl_dollars", 0.0) > 0)
        live_wr = (wins / len(trades)) * 100
        live_r_values = [
            t["r_multiple"] for t in trades if t.get("r_multiple") is not None
        ]
        live_avg_r = np.mean(live_r_values) if live_r_values else None

        val_wr = val_metrics.get("win_rate", 50.0)
        val_avg_r = val_metrics.get("mean_r", val_metrics.get("avg_r_multiple", 1.0))

        if abs(live_wr - val_wr) > self.win_rate_threshold:
            self.logger.warning(
                f"[DRIFT] [{symbol}] Win rate drift: live={live_wr:.1f}% vs "
                f"backtest={val_wr:.1f}% (threshold={self.win_rate_threshold}%)"
            )
        if (
            live_avg_r is not None
            and abs(live_avg_r - val_avg_r) > self.r_mult_threshold
        ):
            self.logger.warning(
                f"[DRIFT] [{symbol}] R-multiple drift: live={live_avg_r:.2f} vs "
                f"backtest={val_avg_r:.2f} (threshold={self.r_mult_threshold})"
            )

    def get_summary(self, symbol: str) -> Dict[str, Any]:
        """Get drift monitoring summary for a symbol."""
        trades = self._trades.get(symbol, [])
        if not trades:
            return {"trade_count": 0}
        wins = sum(1 for t in trades if t.get("pnl_dollars", 0.0) > 0)
        live_r_values = [
            t["r_multiple"] for t in trades if t.get("r_multiple") is not None
        ]
        return {
            "trade_count": len(trades),
            "live_win_rate": (wins / len(trades)) * 100,
            "live_avg_r": np.mean(live_r_values) if live_r_values else None,
        }


# =============================================================================
# LIVE TRADER CLASS
# =============================================================================


class LiveTrader:
    """
    Live trading engine.

    Monitors markets and executes trades based on optimized configurations.
    """

    # Minimum seconds between orders for the same symbol
    ORDER_RATE_LIMIT_SECONDS = 5

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
    _CURRENT_REFIT_DETECTOR_FIELDS = frozenset(
        {
            "regime_fit_receipt_sha256",
            "record_sha256",
            "parameters_sha256",
            "detector_code_sha256",
            "parameters",
        }
    )

    @staticmethod
    def _canonical_payload_sha256(payload: Any) -> str:
        raw = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    @classmethod
    def _parse_current_refit_demo_projection(
        cls, projection: Mapping[str, Mapping[str, Any]]
    ) -> Tuple[Dict[str, SymbolConfig], Dict[str, str], Dict[str, str]]:
        if not isinstance(projection, Mapping) or not projection:
            raise ValueError("current-refit demo projection is empty")
        raw_copy = json.loads(json.dumps(projection, allow_nan=False))
        configs: Dict[str, SymbolConfig] = {}
        cell_bindings: Dict[str, str] = {}
        cell_fingerprints: Dict[str, str] = {}
        for raw_symbol, payload in sorted(raw_copy.items()):
            if not isinstance(payload, dict):
                raise ValueError(f"invalid current-refit symbol payload: {raw_symbol}")
            symbol = str(raw_symbol or "").strip().upper()
            if str(payload.get("symbol") or "").strip().upper() != symbol:
                raise ValueError(f"current-refit symbol mismatch: {raw_symbol}")
            if any(payload.get(field) for field in ("default_config", "edge_challenger_book")):
                raise ValueError(f"current-refit fallback field present: {symbol}")
            config = SymbolConfig.from_dict(copy.deepcopy(payload))
            if config.default_config is not None or config.edge_challenger_book:
                raise ValueError(f"current-refit fallback parsed: {symbol}")
            if not config.regime_configs:
                raise ValueError(f"current-refit symbol has no selected cells: {symbol}")
            for timeframe, regimes in config.regime_configs.items():
                if not regimes:
                    raise ValueError(f"current-refit timeframe is empty: {symbol}|{timeframe}")
                for regime, cell in regimes.items():
                    cell_id = f"{symbol}|{str(timeframe).upper()}|{str(regime).upper()}"
                    if (
                        cell is None
                        or cell.is_no_trade_marker()
                        or str(cell.promotion_status or "").strip().upper()
                        != PROCESS_VALIDATED_CURRENT_REFIT_ROLE
                    ):
                        raise ValueError(f"current-refit cell is not executable: {cell_id}")
                    binding = dict((cell.artifact_meta or {}).get("current_refit_binding") or {})
                    if set(binding) != cls._CURRENT_REFIT_BINDING_FIELDS:
                        raise ValueError(f"current-refit binding invalid: {cell_id}")
                    if any(
                        not isinstance(binding.get(field), str)
                        or len(binding[field]) != 64
                        or binding[field] != binding[field].lower()
                        or any(ch not in "0123456789abcdef" for ch in binding[field])
                        for field in binding
                    ):
                        raise ValueError(f"current-refit binding hash invalid: {cell_id}")
                    cell_bindings[cell_id] = binding["current_refit_cell_id"]
                    cell_fingerprints[cell_id] = cls._canonical_payload_sha256(
                        cell.to_dict()
                    )
            configs[symbol] = config
        if not cell_bindings:
            raise ValueError("current-refit demo projection has no selected cells")
        return configs, cell_bindings, cell_fingerprints

    @staticmethod
    def _plain_sha256(value: Any) -> bool:
        return (
            type(value) is str
            and len(value) == 64
            and value == value.lower()
            and all(char in "0123456789abcdef" for char in value)
        )

    @classmethod
    def _parse_current_refit_demo_detectors(
        cls,
        projection: Mapping[str, Mapping[str, Any]],
        configs: Mapping[str, SymbolConfig],
    ) -> Tuple[Dict[str, Dict[str, Any]], str]:
        if not isinstance(projection, Mapping) or not projection:
            raise ValueError("current-refit detector projection is empty")
        raw_copy = json.loads(json.dumps(projection, allow_nan=False))
        expected = {
            f"{symbol}|{str(timeframe).upper()}"
            for symbol, config in configs.items()
            for timeframe, regimes in config.regime_configs.items()
            if regimes
        }
        if set(raw_copy) != expected:
            raise ValueError("current-refit detector roster mismatch")
        from pm_regime import detector_code_sha256

        runtime_code_sha = detector_code_sha256()
        if not cls._plain_sha256(runtime_code_sha):
            raise ValueError("current-refit detector code is unavailable")
        parsed: Dict[str, Dict[str, Any]] = {}
        for group_id, record in sorted(raw_copy.items()):
            if not isinstance(record, dict) or set(record) != cls._CURRENT_REFIT_DETECTOR_FIELDS:
                raise ValueError(f"current-refit detector binding invalid: {group_id}")
            for field in (
                "regime_fit_receipt_sha256",
                "record_sha256",
                "parameters_sha256",
                "detector_code_sha256",
            ):
                if not cls._plain_sha256(record.get(field)):
                    raise ValueError(f"current-refit detector hash invalid: {group_id}")
            params, params_sha = FeatureComputer._validate_regime_params_override(
                record.get("parameters"),
                record.get("parameters_sha256"),
            )
            if params_sha != record["parameters_sha256"]:
                raise ValueError(f"current-refit detector parameters invalid: {group_id}")
            if record["detector_code_sha256"] != runtime_code_sha:
                raise ValueError(f"current-refit detector code drift: {group_id}")
            symbol, timeframe = group_id.split("|")
            config = configs.get(symbol)
            regimes = (
                config.regime_configs.get(timeframe)
                if config is not None and config.regime_configs
                else None
            )
            if not regimes:
                raise ValueError(f"current-refit detector group missing: {group_id}")
            for regime, cell in regimes.items():
                binding = dict((cell.artifact_meta or {}).get("current_refit_binding") or {})
                if (
                    binding.get("regime_fit_receipt_sha256")
                    != record["regime_fit_receipt_sha256"]
                    or binding.get("regime_detector_record_sha256")
                    != record["record_sha256"]
                    or binding.get("regime_detector_parameters_sha256")
                    != record["parameters_sha256"]
                    or binding.get("regime_detector_code_sha256")
                    != record["detector_code_sha256"]
                ):
                    raise ValueError(
                        f"current-refit detector cell splice: {group_id}|{regime}"
                    )
            parsed[group_id] = {
                **{key: record[key] for key in record if key != "parameters"},
                "parameters": params.to_dict(),
            }
        return parsed, cls._canonical_payload_sha256(parsed)

    def __init__(
        self,
        mt5_connector: MT5Connector,
        portfolio_manager: PortfolioManager,
        position_config: "PositionConfig",
        enable_trading: bool = True,
        close_on_opposite_signal: bool = False,
        pipeline_config: "PipelineConfig" = None,
        storage_manager: Optional[StorageManager] = None,
        execution_book: str = CHAMPION_EXECUTION_BOOK,
        demo_symbols: Optional[Sequence[str]] = None,
        demo_winner_ledger_path: Optional[str] = None,
    ):
        """
        Initialize live trader.

        Args:
            mt5_connector: MT5 connection
            portfolio_manager: Portfolio manager with configs
            position_config: Position management configuration
            enable_trading: Enable actual trade execution
            close_on_opposite_signal: Close positions on opposite signal (default: False)
            pipeline_config: Pipeline configuration (for regime params, etc.)
            execution_book: Funded champion or current-refit demo book.
        """
        self.mt5 = mt5_connector
        self.pm = portfolio_manager
        self.position_config = position_config
        self.enable_trading = enable_trading
        self.close_on_opposite_signal = close_on_opposite_signal
        self.pipeline_config = pipeline_config
        self.storage_manager = storage_manager
        normalized_book = str(execution_book or CHAMPION_EXECUTION_BOOK).strip().upper()
        if normalized_book not in {
            CHAMPION_EXECUTION_BOOK,
            CURRENT_REFIT_DEMO_EXECUTION_BOOK,
        }:
            raise ValueError(f"unsupported execution book: {execution_book!r}")
        self.execution_book = normalized_book
        self._is_current_refit_demo = (
            normalized_book == CURRENT_REFIT_DEMO_EXECUTION_BOOK
        )
        self._demo_management_capsule_id = ""
        self._demo_management_generation_id = ""
        if self._is_current_refit_demo:
            config_path = str(
                getattr(self.pipeline_config, "_deployment_config_path", "") or ""
            ).strip()
            ledger_path = str(demo_winner_ledger_path or "").strip()
            requested_symbols = sorted(
                {
                    str(symbol or "").strip().upper()
                    for symbol in (demo_symbols or ())
                    if str(symbol or "").strip()
                }
            )
            if not config_path or not ledger_path or not requested_symbols:
                raise ValueError("current-refit demo authority inputs are incomplete")
            account_info = self.mt5.get_account_info()
            account_reason = self._current_refit_demo_account_block_reason(account_info)
            if account_reason:
                raise ValueError(account_reason)
            from pm_demo_readiness import authorize_demo_probation_execution

            demo_report, demo_projection = authorize_demo_probation_execution(
                self.pipeline_config,
                symbols=requested_symbols,
                config_path=config_path,
                winner_ledger_path=ledger_path,
                account_info=account_info,
                position_config=self.position_config,
            )
            if not bool(getattr(demo_report, "ok", False)) or not demo_projection:
                errors = [
                    str(getattr(finding, "code", "demo_authority_failed"))
                    for finding in list(getattr(demo_report, "findings", ()) or ())
                    if str(getattr(finding, "severity", "")).lower() == "error"
                ]
                raise ValueError(
                    "current-refit demo authority denied: "
                    + ",".join(errors or ["unknown"])
                )
            self.bind_demo_management_authority(
                str(getattr(demo_report, "management_capsule_id", "") or ""),
                str(
                    getattr(demo_report, "management_generation_id", "") or ""
                ),
            )
            capital_sha = getattr(
                demo_report, "execution_capital_config_sha256", None
            )
            capital_envelope = dict(
                getattr(demo_report, "execution_capital_envelope", {}) or {}
            )
            if not isinstance(capital_sha, str) or len(capital_sha) != 64:
                raise ValueError("current-refit demo capital authority is missing")
            position_sha = getattr(
                demo_report, "execution_position_config_sha256", None
            )
            if not isinstance(position_sha, str) or len(position_sha) != 64:
                raise ValueError("current-refit demo position authority is missing")
            detector_authority_sha = getattr(
                demo_report, "execution_detector_projection_sha256", None
            )
            detector_projection = getattr(
                demo_report, "execution_detector_projection", None
            )
            if not self._plain_sha256(detector_authority_sha) or not isinstance(
                detector_projection, Mapping
            ):
                raise ValueError("current-refit demo detector authority is missing")
            self._current_refit_demo_capital_config_sha256 = capital_sha
            self._current_refit_demo_capital_envelope = capital_envelope
            self._current_refit_demo_position_config_sha256 = position_sha
            (
                self._current_refit_demo_configs,
                self._current_refit_demo_cell_bindings,
                self._current_refit_demo_cell_fingerprints,
            ) = self._parse_current_refit_demo_projection(
                demo_projection
            )
            (
                self._current_refit_demo_detectors,
                self._current_refit_demo_detector_fingerprint,
            ) = self._parse_current_refit_demo_detectors(
                detector_projection,
                self._current_refit_demo_configs,
            )
            self._current_refit_demo_detector_authority_sha256 = (
                detector_authority_sha
            )
            self._current_refit_demo_detector_roster = tuple(
                sorted(self._current_refit_demo_detectors)
            )
            self._current_refit_demo_detector_record_fingerprints = {
                key: self._canonical_payload_sha256(value)
                for key, value in self._current_refit_demo_detectors.items()
            }
        else:
            if demo_symbols is not None or demo_winner_ledger_path is not None:
                raise ValueError("funded execution cannot receive demo authority inputs")
            self._current_refit_demo_configs = {}
            self._current_refit_demo_cell_bindings = {}
            self._current_refit_demo_cell_fingerprints = {}
            self._current_refit_demo_capital_config_sha256 = ""
            self._current_refit_demo_capital_envelope = {}
            self._current_refit_demo_position_config_sha256 = ""
            self._current_refit_demo_detectors = {}
            self._current_refit_demo_detector_fingerprint = ""
            self._current_refit_demo_detector_authority_sha256 = ""
            self._current_refit_demo_detector_roster = ()
            self._current_refit_demo_detector_record_fingerprints = {}

        self.position_calc = PositionCalculator(position_config)

        # State
        self._running = False
        self._shutdown_event = threading.Event()
        self._last_bar_times: Dict[str, datetime] = {}
        self._last_order_times: Dict[str, datetime] = {}  # For rate limiting
        self._ambiguous_order_block_until: Dict[str, datetime] = {}
        self._unknown_tf_position_warnings: Set[int] = set()
        self._tf_recovery_cache: Dict[int, Optional[str]] = {}
        self._margin_state: str = "NORMAL"
        self._margin_reopen_required: bool = False
        self._last_margin_close_ts: Optional[datetime] = None
        self._margin_cooldown_notice_logged_ts: Optional[datetime] = None
        self._margin_state_persistence_lock = threading.RLock()
        self._margin_state_persistence_error: str = ""

        # Decision throttle – prevents re-evaluation / re-logging of the
        # same decision on the same bar (e.g. risk-cap skips for XAUUSD).
        raw_runtime_dir = getattr(self.pipeline_config, "output_dir", "pm_outputs")
        if not isinstance(raw_runtime_dir, (str, Path)):
            raw_runtime_dir = "pm_outputs"
        runtime_state_dir = Path(raw_runtime_dir or "pm_outputs")
        if not runtime_state_dir.is_absolute():
            source_config = str(
                getattr(self.pipeline_config, "_deployment_config_path", "") or ""
            ).strip()
            runtime_root = (
                Path(source_config).resolve().parent if source_config else Path.cwd()
            )
            runtime_state_dir = (runtime_root / runtime_state_dir).resolve()
        if self._is_current_refit_demo:
            runtime_state_dir = runtime_state_dir / "current_refit_demo"
            decision_log_path = runtime_state_dir / "last_trade_log.json"
            actionable_log_path = runtime_state_dir / "last_actionable_log.json"
        else:
            decision_log_path = Path("last_trade_log.json")
            actionable_log_path = Path("last_actionable_log.json")
        self._runtime_state_dir = runtime_state_dir
        self._runtime_observability = RuntimeObservabilityStore(
            runtime_state_dir / "live_observability.json"
        )
        self._margin_protection_state_path = (
            runtime_state_dir / "margin_protection_state.json"
        )
        self._margin_state_persistence_enabled = bool(
            isinstance(self.pipeline_config, PipelineConfig)
            and (
                bool(
                    getattr(
                        self.pipeline_config,
                        "_funded_live_execution",
                        False,
                    )
                )
                or self._is_current_refit_demo
            )
        )
        self._decision_throttle = DecisionThrottle(log_path=str(decision_log_path))
        self._actionable_log = ActionableDecisionLog(log_path=str(actionable_log_path))

        # Feature/signal cache – avoids recomputing features and signals
        # when no new bar has arrived. Cache key: (symbol, timeframe, bar_time)
        # Cache stores: {'features': df, 'signal': int, 'regime': str, 'regime_strength': float}
        self._candidate_cache: Dict[str, Dict[str, Any]] = {}

        # Trade log
        self.trade_log: List[Dict] = []
        self.drift_monitor = DriftMonitor(logging.getLogger(__name__))
        self._seen_closing_deals: Set[int] = set()
        self._seen_cycle_closure_keys: Set[str] = set()
        self._recent_closing_deals: List[Dict[str, Any]] = []
        self._recent_closing_deals_lookback_hours: int = 0
        # Distinguish a successful empty broker-history query from transport or
        # connection failure.  Hard loss breakers must never interpret the
        # latter as proof of zero realized loss.
        self._recent_closing_deals_available: bool = False
        self._live_session_started_at = datetime.now()

        # Cache statistics for monitoring
        self._cache_hits = 0
        self._cache_misses = 0
        self._max_cache_size = 100  # Limit cache to prevent memory bloat
        # Warn once per fallback path when precise MT5 contract math is unavailable.
        self._mt5_risk_fallback_logged: Set[str] = set()
        self._equity_peak = 0.0
        self._sweep_recent_m5_windows: Dict[str, pd.DataFrame] = {}
        self._latest_bar_time_by_tf: Dict[str, pd.Timestamp] = {}
        self._last_bar_probe_times: Dict[str, Tuple[str, pd.Timestamp]] = {}
        self._pending_bar_probe_times: Dict[str, Tuple[str, pd.Timestamp]] = {}
        # Cross-horizon signals admitted from the same bar are serialized so
        # the second decision sees the first fill in a fresh broker snapshot.
        self._pending_specialist_timeframes: Dict[str, Set[str]] = {}
        self._last_governance_bar_times: Dict[str, pd.Timestamp] = {}
        self._daily_advisory_state: Dict[str, str] = {}
        self._last_portfolio_observation: Dict[str, Any] = {}
        self._order_governance_state: Dict[int, Dict[str, Any]] = {}

        self.logger = logging.getLogger(__name__)
        self._restore_margin_protection_state()

        # Enhancement seams (risk scalars, spread filter, exit pack)
        self._enhancement_seams = create_default_enhancement_seams(self.pipeline_config)
        supervisor_state_path = None
        drift_state_path = None
        if self._is_current_refit_demo:
            supervisor_state_path = (
                runtime_state_dir / "institutional_supervisor_state.json"
            )
            drift_state_path = runtime_state_dir / "cycle_drift_state.json"
        self._institutional_supervisor = InstitutionalSupervisor(
            self.pipeline_config,
            state_path=supervisor_state_path,
        )
        self._cycle_drift_monitor = CycleDriftMonitor(
            self.pipeline_config,
            state_path=drift_state_path,
        )
        # Only an authoritative resolved PipelineConfig may enable persistent
        # feedback. Loose mocks/legacy embedding callers keep the prior
        # observation path and cannot accidentally create production files.
        self._live_outcome_feedback_initialized = isinstance(
            self.pipeline_config, PipelineConfig
        )
        self._live_outcome_feedback_error = ""
        self._live_outcome_ledger: Optional[LiveOutcomeLedger] = None
        if self._live_outcome_feedback_initialized and bool(
            getattr(self.pipeline_config, "live_outcome_feedback_enabled", True)
        ):
            try:
                if self._is_current_refit_demo:
                    self._live_outcome_ledger = LiveOutcomeLedger(
                        self.pipeline_config,
                        ledger_path=runtime_state_dir / "closed_positions.jsonl",
                        state_path=runtime_state_dir / "closed_positions.state.json",
                    )
                else:
                    self._live_outcome_ledger = LiveOutcomeLedger(self.pipeline_config)
            except LiveOutcomeError as exc:
                # Corrupt accounting evidence is a hard entry blocker. Existing
                # positions remain manageable, but no new risk may be added.
                self._live_outcome_feedback_error = str(exc)
                self.logger.error(f"Live outcome feedback unavailable: {exc}")
        self._event_calendar: Optional[EventCalendar] = None
        if bool(getattr(self.pipeline_config, "event_calendar_enabled", False)):
            try:
                event_path = str(
                    getattr(self.pipeline_config, "event_calendar_path", "") or ""
                )
                self._event_calendar = (
                    EventCalendar.from_csv(event_path)
                    if event_path
                    else EventCalendar()
                )
            except Exception as exc:
                self.logger.warning(f"Event calendar initialization failed: {exc}")
                self._event_calendar = EventCalendar()

        # === MT5 SPEC SYNCHRONIZATION ===
        # Update InstrumentSpec with live MT5 values for accurate position sizing
        if self.mt5 and self.mt5.is_connected():
            self.logger.info("Synchronizing instrument specs from MT5...")

            sync_count = 0
            fail_count = 0

            for symbol in self.pm.symbols:
                broker_symbol = self.mt5.find_broker_symbol(symbol)

                if not broker_symbol:
                    self.logger.warning(
                        f"[{symbol}] Broker symbol not found, using config values"
                    )
                    fail_count += 1
                    continue

                mt5_info = self.mt5.get_symbol_info(broker_symbol)

                if not mt5_info:
                    self.logger.warning(
                        f"[{symbol}] MT5 info not available, using config values"
                    )
                    fail_count += 1
                    continue

                # Get spec and sync
                spec = get_instrument_spec(symbol)

                # Store original values for comparison
                orig_tick_value = spec.tick_value
                orig_volume_step = spec.volume_step
                orig_spread = spec.spread_avg

                # Sync from MT5
                from pm_core import sync_instrument_spec_from_mt5

                sync_instrument_spec_from_mt5(spec, mt5_info)

                # Log changes
                self.logger.info(
                    f"[{symbol}] Synced from MT5: "
                    f"tick_value={spec.tick_value:.4f} (was {orig_tick_value:.4f}), "
                    f"volume_step={spec.volume_step} (was {orig_volume_step}), "
                    f"spread={spec.spread_avg:.1f}pips (was {orig_spread:.1f}pips), "
                    f"min_lot={spec.min_lot}, max_lot={spec.max_lot}"
                )

                sync_count += 1

            self.logger.info(
                f"MT5 spec sync complete: {sync_count} synced, {fail_count} failed/unavailable"
            )
        else:
            self.logger.warning(
                "MT5 not connected, using config.json instrument specs "
                "(may be inaccurate for cross pairs and crypto)"
            )
        # === END SYNCHRONIZATION ===

    def _prune_cache(self):
        """Prune cache if it exceeds max size (LRU-style, just clear oldest)."""
        if len(self._candidate_cache) > self._max_cache_size:
            # Remove half the entries (oldest first by insertion order in Python 3.7+)
            keys_to_remove = list(self._candidate_cache.keys())[
                : len(self._candidate_cache) // 2
            ]
            for k in keys_to_remove:
                del self._candidate_cache[k]
            self.logger.debug(
                f"Pruned feature cache: removed {len(keys_to_remove)} entries"
            )

    def invalidate_runtime_caches(self) -> None:
        """Clear runtime caches that should not survive reconnect/retrain boundaries."""
        self._candidate_cache.clear()
        self._last_bar_times.clear()
        self._last_bar_probe_times.clear()
        self._pending_bar_probe_times.clear()
        self._pending_specialist_timeframes.clear()
        self._last_governance_bar_times.clear()
        self._sweep_recent_m5_windows.clear()
        self._get_latest_bar_time_store().clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self.logger.debug("Cleared live runtime caches")

    def _get_pipeline_data_loader(self) -> Optional[DataLoader]:
        """Return the shared portfolio DataLoader when available."""
        pm_obj = getattr(self, "pm", None)
        pipeline = getattr(pm_obj, "pipeline", None)
        loader = getattr(pipeline, "data_loader", None)
        return loader if isinstance(loader, DataLoader) else None

    def _get_live_cache_path(self, symbol: str, timeframe: str) -> Path:
        """Path for the bounded research-mode live cache."""
        live_dir = Path(self.pipeline_config.data_dir) / ".live"
        live_dir.mkdir(parents=True, exist_ok=True)
        return live_dir / f"{symbol}_{str(timeframe or 'M5').upper()}.csv"

    def _write_live_cache(self, cache_path: Path, bars: pd.DataFrame) -> None:
        """Atomically write a live cache file."""
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_name(
            f"{cache_path.stem}.tmp.{os.getpid()}.{threading.get_ident()}{cache_path.suffix}"
        )
        try:
            bars.to_csv(tmp_path, index_label="time")
            os.replace(tmp_path, cache_path)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _get_latest_bar_time_store(self) -> Dict[str, pd.Timestamp]:
        store = getattr(self, "_latest_bar_time_by_tf", None)
        if not isinstance(store, dict):
            store = {}
            self._latest_bar_time_by_tf = store
        return store

    def _record_latest_bar_time(
        self, timeframe: str, bars: Optional[pd.DataFrame]
    ) -> None:
        """Track the latest observed bar timestamp per timeframe for due-time scheduling."""
        if bars is None or len(bars) == 0:
            return
        try:
            latest = pd.Timestamp(bars.index[-1])
        except Exception:
            return
        tf = str(timeframe or "").upper()
        store = self._get_latest_bar_time_store()
        current = store.get(tf)
        if current is None or latest > current:
            store[tf] = latest

    def _get_symbol_timeframe_due_at(
        self, symbol: str, timeframe: str, *, now: Optional[datetime] = None
    ) -> datetime:
        """
        Return the next due-time for a symbol/timeframe evaluation.

        Once a symbol/timeframe has been evaluated for the current forming bar, we
        do not need to reload it again until the next closed bar should exist.
        """
        probe = now or datetime.now()
        tf = str(timeframe or "").upper()
        if not tf:
            return probe
        last_bar_store = getattr(self, "_last_bar_times", None)
        if not isinstance(last_bar_store, dict):
            return probe
        last_bar = last_bar_store.get(f"{symbol}_{tf}")
        if last_bar is None:
            return probe
        try:
            last_bar_ts = pd.Timestamp(last_bar)
        except Exception:
            return probe
        settle_seconds = max(
            0, int(getattr(self.pipeline_config, "live_bar_settle_seconds", 5) or 5)
        )
        tf_minutes = int(DataLoader.TIMEFRAME_MINUTES.get(tf, 5))
        return last_bar_ts.to_pydatetime() + timedelta(
            minutes=tf_minutes, seconds=settle_seconds
        )

    def _is_symbol_timeframe_due(
        self, symbol: str, timeframe: str, *, now: Optional[datetime] = None
    ) -> bool:
        probe = now or datetime.now()
        return probe >= self._get_symbol_timeframe_due_at(symbol, timeframe, now=probe)

    def _load_cached_live_bars(
        self, symbol: str, timeframe: str, count: int, min_required: int = 0
    ) -> Optional[pd.DataFrame]:
        """Load bars from the bounded live cache for a specific timeframe."""
        loader = self._get_pipeline_data_loader()
        if loader is None:
            return None
        cache_path = self._get_live_cache_path(symbol, timeframe)
        if not cache_path.exists():
            return None
        try:
            bars = loader.get_recent_data(
                symbol,
                timeframe,
                count=count,
                min_required=min_required,
                base_timeframe=str(timeframe or "M5").upper(),
                source_path=cache_path,
            )
        except Exception as exc:
            self.logger.debug(
                f"[{symbol}] Live cache load failed for {timeframe}: {exc}"
            )
            return None
        if bars is None or len(bars) == 0:
            return None
        return bars.copy()

    def _load_recent_canonical_m5(
        self, symbol: str, *, row_count: int
    ) -> Optional[pd.DataFrame]:
        """Load a bounded recent M5 window from canonical local history."""
        loader = self._get_pipeline_data_loader()
        if loader is None:
            return None
        try:
            bars = loader.get_recent_data(
                symbol,
                "M5",
                count=max(1, int(row_count or 0)),
                min_required=0,
                base_timeframe="M5",
            )
        except Exception as exc:
            self.logger.debug(f"[{symbol}] Canonical M5 recent load failed: {exc}")
            return None
        if bars is None or len(bars) == 0:
            return None
        return bars.copy()

    def _load_direct_live_bars_from_mt5(
        self, broker_symbol: str, timeframe: str, *, required_bars: int
    ) -> Optional[pd.DataFrame]:
        """
        Load a fresh contiguous live window directly from MT5.

        This is used as a repair path when the local canonical/live caches are too
        stale to be safely bridged by a small delta window.
        """
        if not self.mt5 or not self.mt5.is_connected():
            return None
        tf = str(timeframe or "M5").upper()
        count = max(1, int(required_bars or 0))
        if tf == "M5":
            sync_floor = max(
                1,
                int(
                    getattr(self.pipeline_config, "storage_live_sync_bars", count)
                    or count
                ),
            )
            count = max(count, sync_floor)
        bars = self.mt5.get_bars(broker_symbol, tf, count=count)
        if not isinstance(bars, pd.DataFrame) or len(bars) == 0:
            return None
        if len(bars) > required_bars:
            bars = bars.tail(required_bars)
        return bars.copy()

    def _get_recent_m5_window(
        self,
        symbol: str,
        broker_symbol: str,
        *,
        timeframe: str,
        min_target_bars: int = 2,
        min_source_rows: int = 0,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch a bounded recent M5 window once per symbol/sweep.

        The window is sized to regenerate the trailing bars for the requested
        timeframe, not to shadow the full canonical M5 history.
        """
        if not self.mt5 or not self.mt5.is_connected():
            return None
        loader = self._get_pipeline_data_loader()
        if loader is None:
            return None
        target_bars = max(1, int(min_target_bars or 0))
        try:
            required_rows = loader.estimate_source_rows(
                timeframe, target_bars, base_timeframe="M5"
            )
        except Exception:
            required_rows = target_bars
        overlap_rows = max(
            0,
            int(
                getattr(self.pipeline_config, "storage_live_sync_overlap_bars", 100)
                or 100
            ),
        )
        source_floor = max(0, int(min_source_rows or 0))
        fetch_rows = max(required_rows, overlap_rows, source_floor)

        cached = self._sweep_recent_m5_windows.get(symbol)
        if isinstance(cached, pd.DataFrame) and len(cached) >= fetch_rows:
            return cached.tail(fetch_rows).copy()

        recent = self.mt5.get_bars(broker_symbol, "M5", count=fetch_rows)
        if not isinstance(recent, pd.DataFrame) or len(recent) == 0:
            return None
        self._sweep_recent_m5_windows[symbol] = recent.copy()
        return recent.tail(fetch_rows).copy()

    def _uses_native_calendar_live_sources(self) -> bool:
        """Return whether live cells must retain broker-native timeframe semantics."""
        return (
            str(getattr(self.pipeline_config, "production_split_mode", "") or "")
            .strip()
            .lower()
            == "calendar_lockbox_v2"
        )

    def _sync_live_timeframe_cache(
        self, symbol: str, broker_symbol: str, timeframe: str, *, required_bars: int
    ) -> Optional[pd.DataFrame]:
        """Refresh a timeframe-specific live cache from canonical seed plus recent M5 delta."""
        if not bool(
            getattr(self.pipeline_config, "storage_local_data_first_enabled", True)
        ):
            return None
        loader = self._get_pipeline_data_loader()
        if loader is None:
            return None
        tf = str(timeframe or "M5").upper()
        required_bars = max(1, int(required_bars or 0))
        cache_path = self._get_live_cache_path(symbol, tf)
        existing_cache = self._load_cached_live_bars(
            symbol, tf, count=required_bars, min_required=0
        )

        try:
            required_source_rows = loader.estimate_source_rows(
                tf, required_bars, base_timeframe="M5"
            )
        except Exception:
            required_source_rows = required_bars

        overlap_rows = max(
            0,
            int(
                getattr(self.pipeline_config, "storage_live_sync_overlap_bars", 100)
                or 100
            ),
        )
        canonical_m5 = self._load_recent_canonical_m5(
            symbol, row_count=required_source_rows
        )

        use_direct_reseed = False
        if self.mt5 and self.mt5.is_connected():
            if canonical_m5 is None or len(canonical_m5) == 0:
                use_direct_reseed = True
            else:
                try:
                    last_local = pd.Timestamp(canonical_m5.index[-1]).to_pydatetime()
                    allowed_gap = timedelta(
                        minutes=max(1, overlap_rows)
                        * int(DataLoader.TIMEFRAME_MINUTES.get("M5", 5))
                    )
                    use_direct_reseed = (datetime.now() - last_local) > allowed_gap
                except Exception:
                    use_direct_reseed = True

        combined: Optional[pd.DataFrame] = None

        if use_direct_reseed:
            combined = self._load_direct_live_bars_from_mt5(
                broker_symbol,
                tf,
                required_bars=required_bars,
            )
            if combined is not None and len(combined) > 0:
                self.logger.debug(
                    f"[{symbol}] Re-seeded {tf} live cache from direct MT5 bars "
                    f"after canonical gap exceeded overlap"
                )

        if combined is None or len(combined) == 0:
            source_m5 = canonical_m5
            recent_m5 = self._get_recent_m5_window(
                symbol,
                broker_symbol,
                timeframe=tf,
                min_target_bars=2,
            )
            if isinstance(recent_m5, pd.DataFrame) and len(recent_m5) > 0:
                if source_m5 is None or len(source_m5) == 0:
                    source_m5 = recent_m5.copy()
                else:
                    source_m5 = pd.concat([source_m5, recent_m5])
                    source_m5 = source_m5[
                        ~source_m5.index.duplicated(keep="last")
                    ].sort_index()
                    if len(source_m5) > required_source_rows:
                        source_m5 = source_m5.tail(required_source_rows)
            if source_m5 is not None and len(source_m5) > 0:
                if tf == "M5":
                    combined = source_m5.copy()
                else:
                    try:
                        combined = loader.resample(source_m5, tf)
                    except Exception as exc:
                        self.logger.debug(
                            f"[{symbol}] Live delta resample failed for {tf}: {exc}"
                        )
                        combined = None

        if combined is None or len(combined) == 0:
            if existing_cache is not None and len(existing_cache) > 0:
                return existing_cache.copy()
            return None
        if len(combined) > required_bars:
            combined = combined.tail(required_bars)

        cache_exists = cache_path.exists()
        should_write = not cache_exists
        if cache_exists and existing_cache is not None and len(existing_cache) > 0:
            try:
                should_write = not combined.equals(existing_cache)
            except Exception:
                should_write = True
        if should_write:
            self._write_live_cache(cache_path, combined)
        return combined.copy()

    def _load_local_bars(
        self,
        symbol: str,
        broker_symbol: str,
        timeframe: str,
        count: int,
        min_required: int,
    ) -> Optional[pd.DataFrame]:
        """Load bounded live bars, refreshing the timeframe cache when possible."""
        tf = str(timeframe or "M5").upper()
        required_bars = max(int(count or 0), int(min_required or 0), 1)
        try:
            bars = self._sync_live_timeframe_cache(
                symbol,
                broker_symbol,
                tf,
                required_bars=required_bars,
            )
        except Exception as exc:
            self.logger.debug(f"[{symbol}] Local cache sync failed for {tf}: {exc}")
            bars = None
        if bars is None or len(bars) == 0:
            bars = self._load_cached_live_bars(
                symbol, tf, count=count, min_required=min_required
            )
        if bars is None or len(bars) == 0:
            return None
        if count > 0 and len(bars) > count:
            bars = bars.tail(count)
        return bars.copy()

    def _get_live_bars(
        self,
        symbol: str,
        broker_symbol: str,
        timeframe: str,
        *,
        count: int,
        min_required: int,
    ) -> Optional[pd.DataFrame]:
        """
        Load the exact production source or the explicit research compatibility path.
        """
        if self._uses_native_calendar_live_sources():
            required_bars = max(int(count or 0), int(min_required or 0), 1)
            if not self.mt5 or not self.mt5.is_connected():
                return None
            bars = self.mt5.get_bars(
                broker_symbol,
                str(timeframe or "M5").upper(),
                count=required_bars,
            )
            if not isinstance(bars, pd.DataFrame) or len(bars) < max(
                int(min_required or 0), 1
            ):
                self._record_latest_bar_time(timeframe, bars)
                return None
            if len(bars) > required_bars:
                bars = bars.tail(required_bars)
            self._record_latest_bar_time(timeframe, bars)
            return bars.copy()

        use_local = bool(
            getattr(self.pipeline_config, "storage_local_data_first_enabled", True)
        )
        local_bars: Optional[pd.DataFrame] = None
        if use_local:
            local_bars = self._load_local_bars(
                symbol, broker_symbol, timeframe, count, min_required
            )
            required_bars = max(int(count or 0), int(min_required or 0), 1)
            if local_bars is not None and len(local_bars) >= required_bars:
                self._record_latest_bar_time(timeframe, local_bars)
                return local_bars
            if local_bars is not None and len(local_bars) > 0:
                self.logger.debug(
                    f"[{symbol}] Local {timeframe} data has only {len(local_bars)} bars; "
                    f"target={required_bars}, minimum={min_required}. Falling back to MT5."
                )

        bars = self.mt5.get_bars(broker_symbol, timeframe, count=count)
        if isinstance(bars, pd.DataFrame) and len(bars) >= max(
            int(min_required or 0), 1
        ):
            self._record_latest_bar_time(timeframe, bars)
            return bars
        if local_bars is not None and len(local_bars) >= min_required:
            self._record_latest_bar_time(timeframe, local_bars)
            return local_bars
        self._record_latest_bar_time(timeframe, bars)
        return bars

    @staticmethod
    def _deadline_expired(deadline: Any, *, grace_minutes: int = 0) -> bool:
        if not isinstance(deadline, datetime):
            return False
        probe = datetime.now(timezone.utc)
        value = deadline
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return probe >= value + timedelta(minutes=max(0, int(grace_minutes or 0)))

    def _current_refit_cell_integrity_reason(
        self, symbol: str, timeframe: str, regime: str, cell: Any
    ) -> Optional[str]:
        cell_id = f"{str(symbol).upper()}|{str(timeframe).upper()}|{str(regime).upper()}"
        expected_id = self._current_refit_demo_cell_bindings.get(cell_id)
        expected_sha = self._current_refit_demo_cell_fingerprints.get(cell_id)
        if not expected_id or not expected_sha or cell is None:
            return f"unselected current-refit cell: {cell_id}"
        if (
            cell.is_no_trade_marker()
            or str(cell.promotion_status or "").strip().upper()
            != PROCESS_VALIDATED_CURRENT_REFIT_ROLE
        ):
            return f"current-refit status changed: {cell_id}"
        binding = dict((cell.artifact_meta or {}).get("current_refit_binding") or {})
        if (
            set(binding) != self._CURRENT_REFIT_BINDING_FIELDS
            or binding.get("current_refit_cell_id") != expected_id
        ):
            return f"current-refit binding changed: {cell_id}"
        try:
            actual_sha = self._canonical_payload_sha256(cell.to_dict())
        except Exception:
            return f"current-refit payload unreadable: {cell_id}"
        if actual_sha != expected_sha:
            return f"current-refit payload changed: {cell_id}"
        return None

    def _current_refit_demo_detector(
        self, symbol: str, timeframe: str
    ) -> Tuple[Dict[str, Any], str]:
        records = getattr(self, "_current_refit_demo_detectors", None)
        roster = getattr(self, "_current_refit_demo_detector_roster", ())
        fingerprints = getattr(
            self, "_current_refit_demo_detector_record_fingerprints", None
        )
        if (
            not isinstance(records, dict)
            or not isinstance(fingerprints, dict)
            or tuple(sorted(records)) != tuple(roster)
        ):
            raise ValueError("current-refit detector authority is missing")
        key = f"{str(symbol).upper()}|{str(timeframe).upper()}"
        record = records.get(key)
        if (
            not isinstance(record, dict)
            or self._canonical_payload_sha256(record) != fingerprints.get(key)
        ):
            raise ValueError(f"current-refit detector missing: {key}")
        from pm_regime import detector_code_sha256

        if record.get("detector_code_sha256") != detector_code_sha256():
            raise ValueError(f"current-refit detector code drift: {key}")
        params, params_sha = FeatureComputer._validate_regime_params_override(
            copy.deepcopy(record.get("parameters")),
            record.get("parameters_sha256"),
        )
        return params.to_dict(), params_sha

    def _get_current_refit_demo_entry_configs(self) -> Dict[str, SymbolConfig]:
        grace = int(
            getattr(self.pipeline_config, "live_config_expiry_grace_minutes", 0) or 0
        )
        selected: Dict[str, SymbolConfig] = {}
        for symbol, config in self._current_refit_demo_configs.items():
            if self._deadline_expired(config.valid_until, grace_minutes=grace):
                continue
            grid: Dict[str, Dict[str, Any]] = {}
            for timeframe, regimes in config.regime_configs.items():
                try:
                    self._current_refit_demo_detector(symbol, timeframe)
                except Exception as exc:
                    if log := getattr(self, "logger", None):
                        log.error(str(exc))
                    continue
                for regime, cell in regimes.items():
                    if self._deadline_expired(cell.valid_until, grace_minutes=grace):
                        continue
                    reason = self._current_refit_cell_integrity_reason(
                        symbol, timeframe, regime, cell
                    )
                    if reason:
                        if log := getattr(self, "logger", None):
                            log.error(reason)
                        continue
                    grid.setdefault(str(timeframe).upper(), {})[
                        str(regime).upper()
                    ] = cell
            if grid:
                view = copy.copy(config)
                view.regime_configs = grid
                view.default_config = None
                view.edge_challenger_book = {}
                selected[symbol] = view
        return selected

    def _get_live_configs(
        self, *, log_rejections: bool = False
    ) -> Dict[str, SymbolConfig]:
        """Return configs for exactly the selected execution book."""
        if bool(getattr(self, "_is_current_refit_demo", False)):
            return self._get_current_refit_demo_entry_configs()
        if not self.pm:
            return {}

        # Funded entry eligibility is an authority boundary.  Never fall back
        # to get_validated_configs(): that broader management view intentionally
        # includes expired/frozen configs so existing positions can still be
        # governed, and is not authority to add risk.
        selector = getattr(self.pm, "get_live_eligible_configs", None)
        if not callable(selector):
            self.logger.error(
                "Funded live-eligibility selector unavailable; refusing all new entries"
            )
            return {}
        try:
            configs = selector(log_rejections=log_rejections)
        except Exception as exc:
            self.logger.error(
                "Funded live-eligibility selector failed closed: %s: %s",
                type(exc).__name__,
                exc,
            )
            return {}
        if not isinstance(configs, dict):
            self.logger.error(
                "Funded live-eligibility selector returned a non-mapping result; "
                "refusing all new entries"
            )
            return {}
        return configs

    def _get_raw_validated_configs(self) -> Dict[str, SymbolConfig]:
        """Return raw validated configs for management-only surfaces."""
        if not self.pm or not hasattr(self.pm, "get_validated_configs"):
            return {}
        configs = self.pm.get_validated_configs()
        return configs if isinstance(configs, dict) else {}

    def _get_management_configs(self) -> Dict[str, SymbolConfig]:
        """
        Return configs usable for open-position management.

        New entries are restricted to `_get_live_configs()`. Existing open
        positions still need margin protection, attribution, and governance even
        if the entry ledger expires before the next retrain is complete.
        """
        if bool(getattr(self, "_is_current_refit_demo", False)):
            return dict(self._current_refit_demo_configs)
        raw_validated = self._get_raw_validated_configs()
        if raw_validated:
            return raw_validated
        return self._get_live_configs()

    def _uses_scheduled_trigger(self) -> bool:
        """True when live evaluations should use the legacy wall-clock due-time fallback."""
        mode = (
            str(getattr(self.pipeline_config, "live_loop_trigger_mode", "bar") or "bar")
            .strip()
            .lower()
        )
        return mode == "scheduled"

    @staticmethod
    def _looks_like_mock(value: Any) -> bool:
        """Avoid treating unconstrained unittest mocks as real MT5 data."""
        try:
            return type(value).__module__.startswith("unittest.mock")
        except Exception:
            return False

    @staticmethod
    def _get_config_timeframes(config: Any) -> List[str]:
        try:
            if config.has_regime_configs():
                raw_timeframes = config.get_available_timeframes()
            else:
                raw_timeframes = [getattr(config, "timeframe", "")]
        except Exception:
            raw_timeframes = [getattr(config, "timeframe", "")]
        result: List[str] = []
        seen: Set[str] = set()
        for timeframe in raw_timeframes or []:
            normalized = str(timeframe or "").upper()
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return result

    def _resolve_broker_symbol_for_live(self, symbol: str) -> str:
        broker_symbol = str(symbol)
        try:
            resolved = self.mt5.find_broker_symbol(broker_symbol)
            if resolved and not self._looks_like_mock(resolved):
                broker_symbol = str(resolved)
        except Exception as exc:
            self.logger.debug(
                f"[{symbol}] Broker symbol resolution failed during live bar probe: {exc}"
            )
        return broker_symbol

    def _probe_latest_bar_time(
        self, broker_symbol: str, timeframe: str
    ) -> Optional[pd.Timestamp]:
        """
        Read a small MT5 bar probe and return the latest available bar timestamp.

        The PM's signal surface is bar-based. This probe is intentionally small:
        it asks the broker whether the relevant symbol/timeframe bar stream has
        advanced, then the full live path loads the configured bar window only
        for branches that are actually eligible to evaluate.
        """
        if not self.mt5:
            return None
        tf = str(timeframe or "").upper()
        if not tf:
            return None
        try:
            bars = self.mt5.get_bars(broker_symbol, tf, count=3)
        except Exception as exc:
            self.logger.debug(
                f"[{broker_symbol}] Live bar probe failed for {tf}: {exc}"
            )
            return None
        if not isinstance(bars, pd.DataFrame) or len(bars) == 0:
            return None
        try:
            latest = pd.Timestamp(bars.index[-1])
        except Exception:
            return None
        if pd.isna(latest):
            return None
        return latest

    def get_symbols_with_new_bars(
        self,
        configs: Optional[Dict[str, SymbolConfig]] = None,
    ) -> Dict[str, Set[str]]:
        """
        Return canonical symbols/timeframes whose MT5 bar stream advanced.

        The first valid bar observation is treated as advanced so startup can
        initialize from broker data immediately. The strategy layer still uses
        the closed decision bar (`iloc[-2]`) after the full bar window is loaded.
        """
        live_configs = configs if configs is not None else self._get_live_configs()
        if not isinstance(live_configs, dict) or not live_configs:
            return {}
        if not self.mt5:
            return {}

        changed: Dict[str, Set[str]] = {}
        for symbol, config in live_configs.items():
            canonical_symbol = str(symbol)
            broker_symbol = self._resolve_broker_symbol_for_live(canonical_symbol)
            for timeframe in self._get_config_timeframes(config):
                latest = self._probe_latest_bar_time(broker_symbol, timeframe)
                if latest is None:
                    continue
                cache_key = f"{canonical_symbol}_{timeframe}"
                current = (broker_symbol, latest)
                if self._last_bar_probe_times.get(cache_key) != current:
                    self._pending_bar_probe_times[cache_key] = current
                    changed.setdefault(canonical_symbol, set()).add(timeframe)
        # A second independent horizon from an already observed bar is a
        # pending admission, not a new market bar.  Keep it schedulable until
        # _process_symbol consumes the exact timeframe on the following sweep.
        for symbol, timeframes in getattr(
            self, "_pending_specialist_timeframes", {}
        ).items():
            if symbol in live_configs and timeframes:
                changed.setdefault(symbol, set()).update(set(timeframes))
        return changed

    def has_new_bar(self, configs: Optional[Dict[str, SymbolConfig]] = None) -> bool:
        """Compatibility helper for callers that only need a yes/no bar gate."""
        return bool(self.get_symbols_with_new_bars(configs))

    def commit_bar_probe_times(self, changed_by_symbol: Dict[str, Set[str]]) -> None:
        """Mark probed bars as consumed after the live cycle survives safety snapshots."""
        if not isinstance(changed_by_symbol, dict):
            return
        for symbol, timeframes in changed_by_symbol.items():
            for timeframe in timeframes or set():
                key = f"{symbol}_{str(timeframe or '').upper()}"
                pending = self._pending_bar_probe_times.pop(key, None)
                if pending is not None:
                    self._last_bar_probe_times[key] = pending

    def get_next_sweep_due(
        self, active_timeframes: List[str], *, now: Optional[datetime] = None
    ) -> datetime:
        """Return the next due-time for a live sweep based on symbol/timeframe state."""
        probe = now or datetime.now()
        settle_seconds = max(
            0, int(getattr(self.pipeline_config, "live_bar_settle_seconds", 5) or 5)
        )
        stale_retry_seconds = max(
            1, int(getattr(self.pipeline_config, "live_stale_retry_seconds", 15) or 15)
        )

        due_candidates: List[datetime] = []

        validated = self._get_live_configs()
        # A queued independent horizon is work on an already observed bar, not
        # an unknown/missing scheduler state.  _process_symbol deliberately
        # removes that exact timeframe from _last_bar_times when it serializes
        # D1 + lower-TF admissions; scheduled mode must therefore wake now so
        # the deferred specialist sees the first fill in a fresh broker
        # snapshot.  Ordinary missing symbol/timeframe entries remain ignored
        # below to avoid a permanent stale-retry loop.
        pending_specialists = getattr(self, "_pending_specialist_timeframes", {})
        if isinstance(pending_specialists, dict) and any(
            symbol in (validated or {}) and bool(timeframes)
            for symbol, timeframes in pending_specialists.items()
        ):
            return probe
        for symbol, config in (validated or {}).items():
            try:
                if config.has_regime_configs():
                    timeframes = config.get_available_timeframes()
                else:
                    timeframes = [getattr(config, "timeframe", "")]
            except Exception:
                timeframes = [getattr(config, "timeframe", "")]

            for tf in timeframes:
                normalized = str(tf or "").upper()
                if not normalized:
                    continue
                if f"{symbol}_{normalized}" not in self._last_bar_times:
                    continue
                due_candidates.append(
                    self._get_symbol_timeframe_due_at(symbol, normalized, now=probe)
                )

        if not due_candidates:
            seen: Set[str] = set()
            latest_bar_store = self._get_latest_bar_time_store()
            for tf in active_timeframes or []:
                normalized = str(tf or "").upper()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                latest_bar = latest_bar_store.get(normalized)
                if latest_bar is None:
                    continue
                tf_minutes = int(DataLoader.TIMEFRAME_MINUTES.get(normalized, 5))
                due_candidates.append(
                    latest_bar.to_pydatetime()
                    + timedelta(minutes=tf_minutes, seconds=settle_seconds)
                )

        if not due_candidates:
            return probe + timedelta(seconds=stale_retry_seconds)

        next_due = min(due_candidates)
        if next_due <= probe:
            return probe + timedelta(seconds=stale_retry_seconds)
        return next_due

    def process_all_symbols(
        self,
        positions_snapshot: Optional[List[Any]] = None,
        account_info: Optional[Any] = None,
        symbols_filter: Optional[Set[str]] = None,
        timeframes_filter: Optional[Dict[str, Set[str]]] = None,
    ) -> bool:
        """Process all validated symbols for one live iteration."""
        sweep_started = time.perf_counter()
        self._sweep_recent_m5_windows.clear()
        entry_configs = self._get_live_configs()
        management_configs = self._get_management_configs()
        active_configs = management_configs or entry_configs
        active_symbols = list(active_configs.keys())
        signal_configs = entry_configs
        if symbols_filter is not None:
            wanted = {str(symbol) for symbol in symbols_filter}
            signal_configs = {
                symbol: config
                for symbol, config in entry_configs.items()
                if str(symbol) in wanted
            }
        runtime_only = symbols_filter is not None and not signal_configs
        storage_manager = getattr(self, "storage_manager", None)
        if storage_manager is not None:
            try:
                storage_manager.set_active_symbols(active_symbols)
            except Exception as exc:
                self.logger.debug(f"Storage active-symbol update failed: {exc}")

        if positions_snapshot is None:
            positions_snapshot = self.mt5.get_positions()
        if positions_snapshot is None:
            self.logger.warning(
                "Live position snapshot unavailable; skipping this iteration."
            )
            return False

        if bool(getattr(self, "_is_current_refit_demo", False)):
            position_book_reason = self._current_refit_demo_position_block_reason(
                list(positions_snapshot or []),
                management_configs,
            )
            if position_book_reason:
                self.logger.error(
                    f"Current-refit demo session blocked: {position_book_reason}"
                )
                return False

        if account_info is None:
            account_info = self.mt5.get_account_info()
        if account_info is None:
            self.logger.warning(
                "Live account snapshot unavailable; skipping this iteration."
            )
            return False
        if bool(getattr(self, "_is_current_refit_demo", False)):
            demo_account_reason = self._current_refit_demo_account_block_reason(
                account_info
            )
            if demo_account_reason:
                self.logger.error(
                    f"Current-refit demo session blocked: {demo_account_reason}"
                )
                return False
        live_equity = 0.0
        try:
            live_equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            if live_equity > 0:
                self._equity_peak = max(self._equity_peak, live_equity)
        except Exception:
            pass
        if not getattr(account_info, "trade_allowed", True) or not getattr(
            account_info, "trade_expert", True
        ):
            self.logger.warning(
                "Live account is not tradable; skipping this iteration."
            )
            return False

        margin_positions_changed = self._run_margin_protection_cycle(
            account_info=account_info,
            positions_snapshot=positions_snapshot,
        )
        refreshed_after_margin = False
        if margin_positions_changed:
            try:
                refreshed_positions = self.mt5.get_positions()
                if refreshed_positions is not None:
                    positions_snapshot = refreshed_positions
                    refreshed_after_margin = True
                refreshed_account = self.mt5.get_account_info()
                if refreshed_account is not None:
                    account_info = refreshed_account
                    try:
                        live_equity = float(
                            getattr(account_info, "equity", live_equity) or live_equity
                        )
                    except Exception:
                        pass
            except Exception as exc:
                self.logger.warning(
                    f"Margin protection refresh failed; skipping this iteration: {exc}"
                )
                return False
        if getattr(self, "_margin_state", "NORMAL") in {"RECOVERY", "PANIC"}:
            self.logger.warning(
                f"Margin protection remains in {self._margin_state}; skipping new entries this iteration."
            )
            return False
        if refreshed_after_margin:
            self.logger.debug("Refreshed live snapshots after margin protection cycle.")

        recent_deals: List[Dict[str, Any]] = []
        try:
            recent_lookback_hours = 24 * 8
            broker_deals = self.mt5.get_recent_closing_deals(
                lookback_hours=recent_lookback_hours
            )
            if broker_deals is None:
                self._recent_closing_deals_available = False
                self._recent_closing_deals_lookback_hours = 0
            else:
                recent_deals = list(broker_deals)
                self._recent_closing_deals_available = True
                self._recent_closing_deals_lookback_hours = recent_lookback_hours
        except Exception as exc:
            self._recent_closing_deals_available = False
            self._recent_closing_deals_lookback_hours = 0
            self.logger.warning(f"Recent closing deals unavailable: {exc}")
        self._recent_closing_deals = list(recent_deals or [])

        self._run_order_governance_cycle(active_configs, positions_snapshot)

        hard_loss_reasons = self._realized_loss_hard_block_reasons(account_info)
        if hard_loss_reasons:
            self.logger.warning(
                "Hard realized-loss block active; skipping new entries: "
                f"{'; '.join(hard_loss_reasons)}"
            )
            self._sync_drift_monitor(
                active_configs,
                recent_deals=recent_deals,
                open_positions_snapshot=positions_snapshot,
            )
            self._evaluate_daily_loss_advisory(account_info, recent_deals)
            self._run_portfolio_observatory(
                active_configs, positions_snapshot, account_info
            )
            return False

        for symbol, config in signal_configs.items():
            try:
                self._process_symbol(
                    symbol,
                    config,
                    positions_snapshot=positions_snapshot,
                    account_info=account_info,
                    timeframes_filter=(
                        timeframes_filter.get(str(symbol))
                        if isinstance(timeframes_filter, dict)
                        else None
                    ),
                )
            except Exception as e:
                self.logger.error(f"[{symbol}] Error: {e}")

        self._sync_drift_monitor(
            active_configs,
            recent_deals=recent_deals,
            open_positions_snapshot=positions_snapshot,
        )
        self._evaluate_daily_loss_advisory(account_info, recent_deals)
        self._run_portfolio_observatory(
            active_configs, positions_snapshot, account_info
        )
        sweep_duration = time.perf_counter() - sweep_started
        open_positions = (
            len(positions_snapshot)
            if isinstance(positions_snapshot, (list, tuple))
            else 0
        )
        if runtime_only:
            self.logger.debug(
                "Live management cycle complete: "
                f"{len(active_configs)} active symbol(s) in {sweep_duration:.1f}s "
                f"| open_positions={open_positions} | equity={live_equity:.2f}"
            )
        else:
            self.logger.info(
                "Live sweep complete: "
                f"{len(signal_configs)} signal symbol(s), {len(active_configs)} active symbol(s) "
                f"in {sweep_duration:.1f}s "
                f"| open_positions={open_positions} | equity={live_equity:.2f}"
            )
        if storage_manager is not None and not runtime_only:
            try:
                storage_manager.on_sweep_complete(
                    symbol_count=len(active_configs),
                    open_positions=open_positions,
                    sweep_duration=sweep_duration,
                    live_equity=live_equity,
                )
                storage_manager.prune_order_governance_state(
                    [
                        int(getattr(pos, "ticket", 0) or 0)
                        for pos in (positions_snapshot or [])
                    ]
                )
            except Exception as exc:
                self.logger.debug(f"Storage sweep hook failed: {exc}")
        return True

    def _resolve_live_outcome_cell(
        self,
        context: Dict[str, Any],
        validated: Dict[str, SymbolConfig],
    ) -> Dict[str, Any]:
        """Prove one exact current symbol/timeframe/regime/strategy cell."""
        from pm_position import TradeTagEncoder

        intent = dict(context.get("entry_intent") or {})
        decoded = dict(context.get("decoded_comment") or {})
        broker_symbol = str(context.get("broker_symbol", "") or "")
        symbol = str(intent.get("symbol") or decoded.get("symbol") or "").upper()
        if symbol not in validated:
            symbol = str(
                self._resolve_symbol_from_broker(broker_symbol, validated) or ""
            ).upper()
        if not symbol or symbol not in validated:
            return {
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": "symbol not in live-eligible ledger",
            }

        symbol_config = validated[symbol]
        magic = int(context.get("entry_magic", 0) or intent.get("magic", 0) or 0)
        magic_to_tf, magic_to_regime = self._build_magic_lookup(symbol, symbol_config)
        timeframe = str(
            intent.get("timeframe")
            or magic_to_tf.get(magic, "")
            or decoded.get("timeframe", "")
        ).upper()
        regime = str(intent.get("regime") or magic_to_regime.get(magic, "")).upper()

        if symbol_config.has_regime_configs():
            if not timeframe or not regime:
                return {
                    "cell_match_status": "UNAVAILABLE",
                    "cell_match_reason": "timeframe/regime attribution incomplete",
                }
            regime_cfg = symbol_config.get_regime_config(timeframe, regime)
        else:
            regime_cfg = symbol_config.default_config
            timeframe = (
                timeframe or str(getattr(symbol_config, "timeframe", "") or "").upper()
            )
            regime = regime or "LEGACY"
        if (
            regime_cfg is None
            or getattr(regime_cfg, "is_no_trade_marker", lambda: False)()
        ):
            return {
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": "attributed cell is absent or NO_TRADE",
            }

        strategy_name = str(getattr(regime_cfg, "strategy_name", "") or "")
        intended_strategy = str(intent.get("strategy_name", "") or "")
        if intended_strategy and intended_strategy != strategy_name:
            return {
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": "entry intent strategy differs from current cell",
            }
        strategy_code = str(decoded.get("strategy_code", "") or "")
        if strategy_code and strategy_code != TradeTagEncoder._strategy_code(
            strategy_name, max_len=5
        ):
            return {
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": "broker comment strategy code mismatch",
            }
        expected_role = (
            PROCESS_VALIDATED_CURRENT_REFIT_ROLE
            if bool(getattr(self, "_is_current_refit_demo", False))
            else LOCKBOX_CHAMPION_ROLE
        )
        intent_role = str(intent.get("book_role", "") or "").upper()
        decoded_role = str(decoded.get("book_role", "") or "").upper()
        binding = dict(
            (getattr(regime_cfg, "artifact_meta", {}) or {}).get(
                "current_refit_binding"
            )
            or {}
        )
        current_candidate_id = str(binding.get("current_refit_cell_id") or "")
        intended_candidate_id = str(intent.get("candidate_id", "") or "")
        if expected_role == PROCESS_VALIDATED_CURRENT_REFIT_ROLE:
            if intent_role != expected_role or decoded_role != expected_role:
                return {
                    "cell_match_status": "UNAVAILABLE",
                    "cell_match_reason": "current-refit entry lacks matching book tags",
                }
            if (
                not current_candidate_id
                or intended_candidate_id != current_candidate_id
            ):
                return {
                    "cell_match_status": "UNAVAILABLE",
                    "cell_match_reason": "current-refit cell id differs from entry intent",
                }
        elif (
            intent_role in {
                PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
                "LATEST_DATA_EDGE_CHALLENGER",
            }
            or decoded_role in {
                PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
                "LATEST_DATA_EDGE_CHALLENGER",
            }
        ):
            return {
                "cell_match_status": "UNAVAILABLE",
                "cell_match_reason": "demo outcome cannot resolve against funded champions",
            }
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "strategy_name": strategy_name,
            "book_role": expected_role,
            "candidate_id": current_candidate_id
            if expected_role == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
            else "",
            "cell_match_status": "EXACT",
            "cell_match_reason": "durable intent/comment/magic matched current cell",
        }

    def _record_runtime_observability(
        self,
        symbol: str,
        timeframe: str,
        regime: str,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update runtime risk state without ever persisting winner authority."""
        merged = self.pm.update_live_observability(
            symbol,
            timeframe,
            regime,
            dict(updates),
            save=False,
        )
        if not merged:
            merged = dict(updates)
        try:
            self._runtime_observability.update(
                symbol,
                timeframe,
                regime,
                merged,
            )
        except Exception as exc:
            self.logger.error(
                "[%s] Runtime observability persistence failed for %s|%s: %s: %s",
                symbol,
                timeframe,
                regime,
                type(exc).__name__,
                exc,
            )
        return dict(merged)

    def _sync_drift_monitor(
        self,
        validated_configs: Optional[Dict[str, SymbolConfig]] = None,
        recent_deals: Optional[List[Dict[str, Any]]] = None,
        open_positions_snapshot: Optional[List[Any]] = None,
    ) -> None:
        """Pull recent closing deals into DriftMonitor and compare against validation metrics."""
        if validated_configs is not None:
            validated = validated_configs
        elif bool(getattr(self, "_is_current_refit_demo", False)):
            validated = self._get_management_configs()
        else:
            validated = self._get_live_configs()
        if not validated:
            return

        if recent_deals is None:
            try:
                deals = list(self.mt5.get_recent_closing_deals() or [])
            except Exception as exc:
                self.logger.debug(f"Drift monitor sync skipped: {exc}")
                return
        else:
            deals = list(recent_deals or [])
        if not isinstance(deals, (list, tuple)):
            return

        if bool(getattr(self, "_is_current_refit_demo", False)):
            from pm_position import TradeTagEncoder

            demo_magics: Set[int] = set()
            for demo_symbol, demo_config in (validated or {}).items():
                for demo_tf, demo_regimes in (demo_config.regime_configs or {}).items():
                    for demo_regime in demo_regimes or {}:
                        demo_magics.add(
                            self._encode_specialist_magic(
                                demo_symbol, demo_tf, demo_regime
                            )
                        )
            filtered_deals: List[Dict[str, Any]] = []
            for deal in deals:
                magic = int(deal.get("magic", 0) or 0)
                decoded = TradeTagEncoder.decode_comment(
                    str(deal.get("comment", "") or "")
                )
                if (
                    magic in demo_magics
                    and str((decoded or {}).get("book_role", "") or "").upper()
                    == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
                ):
                    filtered_deals.append(deal)
            deals = filtered_deals

        outcome_ledger = getattr(self, "_live_outcome_ledger", None)
        if not deals and outcome_ledger is None:
            return
        if outcome_ledger is not None:
            open_ids: Optional[Set[int]] = None
            snapshot = open_positions_snapshot
            if snapshot is None and hasattr(self.mt5, "get_positions"):
                try:
                    snapshot = self.mt5.get_positions()
                except Exception:
                    snapshot = None
            if isinstance(snapshot, (list, tuple)):
                open_ids = {
                    int(
                        getattr(position, "identifier", 0)
                        or getattr(position, "ticket", 0)
                        or 0
                    )
                    for position in snapshot
                    if int(
                        getattr(position, "identifier", 0)
                        or getattr(position, "ticket", 0)
                        or 0
                    )
                    > 0
                }
            provider = getattr(self.mt5, "get_position_history", None)
            sync_result = outcome_ledger.sync(
                closing_deals=deals,
                open_position_ids=open_ids,
                position_history_provider=provider if callable(provider) else None,
                resolve_cell=lambda ctx: self._resolve_live_outcome_cell(
                    ctx, validated
                ),
                drift_monitor=self._cycle_drift_monitor,
            )
            for record in sync_result.emitted_records:
                symbol = str(record.get("symbol", "") or "")
                r_value = record.get("realized_r")
                self.drift_monitor.record_trade(
                    symbol=symbol,
                    pnl_dollars=float(record.get("realized_net_pnl", 0.0) or 0.0),
                    r_multiple=(float(r_value) if r_value is not None else None),
                )
                if symbol in validated:
                    cfg = validated[symbol]
                    _tf, _rg, regime_cfg = self._resolve_exact_regime_config(
                        symbol,
                        cfg,
                        record.get("timeframe"),
                        record.get("regime"),
                    )
                    self.drift_monitor.check_drift(
                        symbol,
                        getattr(regime_cfg, "val_metrics", None) or cfg.val_metrics,
                    )
            if sync_result.fail_closed_reasons:
                self._live_outcome_feedback_error = "; ".join(
                    sync_result.fail_closed_reasons[:8]
                )
                self.logger.error(
                    "Live outcome feedback pending/fail-closed: "
                    f"{self._live_outcome_feedback_error}"
                )
            else:
                self._live_outcome_feedback_error = ""
            return

        from pm_position import TradeTagEncoder

        open_position_ids: Set[int] = set()
        if hasattr(self.mt5, "get_positions"):
            try:
                open_positions = self.mt5.get_positions()
                if isinstance(open_positions, (list, tuple)):
                    for position in open_positions:
                        identifier = int(
                            getattr(position, "identifier", 0)
                            or getattr(position, "ticket", 0)
                            or 0
                        )
                        if identifier > 0:
                            open_position_ids.add(identifier)
            except Exception as exc:
                self.logger.debug(
                    f"Cycle drift open-position snapshot unavailable: {exc}"
                )

        cycle_groups: Dict[str, Dict[str, Any]] = {}

        for deal in deals:
            ticket = int(deal.get("ticket", 0) or 0)
            if ticket <= 0 or ticket in self._seen_closing_deals:
                continue
            self._seen_closing_deals.add(ticket)

            comment = deal.get("comment", "") or ""
            metadata = TradeTagEncoder.decode_comment(comment) if comment else None
            symbol = metadata.get("symbol") if metadata else None

            if not symbol:
                broker_symbol = deal.get("symbol", "")
                for candidate in validated.keys():
                    if self.mt5.find_broker_symbol(candidate) == broker_symbol:
                        symbol = candidate
                        break

            if not symbol or symbol not in validated:
                continue

            symbol_config = validated[symbol]
            timeframe = metadata.get("timeframe") if metadata else ""
            magic = int(deal.get("magic", 0) or 0)
            magic_to_tf, magic_to_regime = self._build_magic_lookup(
                symbol, symbol_config
            )
            if not timeframe:
                timeframe = magic_to_tf.get(magic, "")
            regime = magic_to_regime.get(
                magic, "LEGACY" if not symbol_config.has_regime_configs() else ""
            )
            timeframe, regime, regime_cfg = self._resolve_exact_regime_config(
                symbol,
                symbol_config,
                timeframe,
                regime,
            )

            self.drift_monitor.record_trade(
                symbol=symbol,
                pnl_dollars=float(deal.get("profit", 0.0) or 0.0),
                r_multiple=None,
            )
            position_id = int(deal.get("position_id", 0) or 0)
            closure_key = (
                f"position:{position_id}" if position_id > 0 else f"deal:{ticket}"
            )
            grouped = cycle_groups.setdefault(
                closure_key,
                {
                    "position_id": position_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "regime": regime,
                    "strategy_name": str(
                        getattr(regime_cfg, "strategy_name", "") or ""
                    ),
                    "pnl_dollars": 0.0,
                    "tickets": [],
                },
            )
            grouped["pnl_dollars"] = float(
                grouped.get("pnl_dollars", 0.0) or 0.0
            ) + float(deal.get("profit", 0.0) or 0.0)
            grouped.setdefault("tickets", []).append(ticket)
            drift_summary = dict(self.drift_monitor.get_summary(symbol))
            drift_summary["updated_at"] = datetime.now().isoformat()
            if regime_cfg is not None:
                try:
                    existing_badge = self._runtime_observability.get(
                        symbol, timeframe, regime
                    )
                except Exception:
                    existing_badge = {}
                if not existing_badge:
                    existing_badge = dict(
                        getattr(regime_cfg, "live_observability", {}) or {}
                    )
                consecutive_losses = int(
                    existing_badge.get("recent_consecutive_losses", 0) or 0
                )
                if float(deal.get("profit", 0.0) or 0.0) < 0.0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
                self._record_runtime_observability(
                    symbol,
                    timeframe,
                    regime,
                    {
                        "last_seen_drift": drift_summary,
                        "recent_consecutive_losses": consecutive_losses,
                    },
                )
            self.drift_monitor.check_drift(
                symbol,
                getattr(regime_cfg, "val_metrics", None)
                or validated[symbol].val_metrics,
            )

        seen_cycle_closure_keys = getattr(self, "_seen_cycle_closure_keys", set())
        for closure_key, grouped in cycle_groups.items():
            if closure_key in seen_cycle_closure_keys:
                continue
            position_id = int(grouped.get("position_id", 0) or 0)
            if position_id > 0 and position_id in open_position_ids:
                continue
            try:
                self._cycle_drift_monitor.record_trade(
                    symbol=str(grouped.get("symbol", "") or ""),
                    timeframe=str(grouped.get("timeframe", "") or ""),
                    regime=str(grouped.get("regime", "") or ""),
                    strategy_name=str(grouped.get("strategy_name", "") or ""),
                    pnl_dollars=float(grouped.get("pnl_dollars", 0.0) or 0.0),
                )
                seen_cycle_closure_keys.add(closure_key)
                self._seen_cycle_closure_keys = seen_cycle_closure_keys
            except Exception as exc:
                self.logger.debug(
                    f"[{grouped.get('symbol', '')}] Cycle drift trade sync failed: {exc}"
                )

    def _resolve_symbol_from_broker(
        self, broker_symbol: str, validated_configs: Dict[str, SymbolConfig]
    ) -> Optional[str]:
        for candidate in validated_configs.keys():
            if self.mt5.find_broker_symbol(candidate) == broker_symbol:
                return candidate
        return None

    def _resolve_exact_regime_config(
        self,
        symbol: str,
        config: SymbolConfig,
        timeframe: Optional[str],
        regime: Optional[str],
    ) -> Tuple[str, str, Optional[Any]]:
        tf = str(timeframe or "").upper()
        rg = str(regime or "").upper()
        if config.has_regime_configs():
            if tf and rg:
                return tf, rg, config.get_regime_config(tf, rg)
            return tf, rg, None
        if config.default_config is not None:
            return (
                tf or str(config.timeframe or "").upper(),
                rg or "LEGACY",
                config.default_config,
            )
        return tf, rg, config.default_config

    def _get_order_governance_state(self, ticket: int) -> Dict[str, Any]:
        ticket_id = int(ticket or 0)
        if ticket_id <= 0:
            return {}
        storage_manager = getattr(self, "storage_manager", None)
        if storage_manager is not None:
            return storage_manager.get_order_governance_state(ticket_id)
        return dict(self._order_governance_state.get(ticket_id, {}))

    def _set_order_governance_state(self, ticket: int, state: Dict[str, Any]) -> None:
        ticket_id = int(ticket or 0)
        if ticket_id <= 0 or not isinstance(state, dict):
            return
        storage_manager = getattr(self, "storage_manager", None)
        if storage_manager is not None:
            storage_manager.set_order_governance_state(ticket_id, state)
            return
        self._order_governance_state[ticket_id] = dict(state)

    def _symbol_info_to_instrument_spec(self, symbol: str, symbol_info: Any) -> Any:
        """Convert MT5 symbol info while preserving configured instrument specs."""
        base_spec = get_instrument_spec(symbol)
        if symbol_info is None or not hasattr(symbol_info, "to_instrument_spec"):
            return base_spec
        try:
            return symbol_info.to_instrument_spec(base_spec=base_spec)
        except TypeError:
            # Compatibility for older test doubles/adapters that predate the
            # configured-commission argument.
            try:
                return symbol_info.to_instrument_spec()
            except Exception:
                return base_spec
        except Exception:
            return base_spec

    def _run_order_governance_cycle(
        self,
        validated_configs: Dict[str, SymbolConfig],
        positions_snapshot: Optional[List[Any]],
    ) -> None:
        raw_mode = getattr(self.pipeline_config, "local_governance_live_mode", "off")
        mode = (
            str(raw_mode or "off").strip().lower()
            if isinstance(raw_mode, str)
            else "off"
        )
        if mode == "off" or not positions_snapshot:
            return
        regime_params_file = getattr(
            self.pipeline_config, "regime_params_file", "regime_params.json"
        )
        for position in list(positions_snapshot or []):
            ticket_id = int(getattr(position, "ticket", 0) or 0)
            broker_symbol = str(getattr(position, "symbol", "") or "")
            symbol = self._resolve_symbol_from_broker(broker_symbol, validated_configs)
            if not symbol:
                continue
            config = validated_configs.get(symbol)
            if config is None:
                continue
            magic = int(getattr(position, "magic", 0) or 0)
            pos_timeframe = self._infer_position_timeframe(symbol, config, position)
            _, magic_to_regime = self._build_magic_lookup(symbol, config)
            pos_regime = magic_to_regime.get(
                magic, "LEGACY" if not config.has_regime_configs() else ""
            )
            pos_timeframe, pos_regime, regime_cfg = self._resolve_exact_regime_config(
                symbol,
                config,
                pos_timeframe,
                pos_regime,
            )
            if regime_cfg is None:
                continue
            raw_policy_payload = getattr(regime_cfg, "governance_policy", {}) or {}
            policy_payload = (
                dict(raw_policy_payload) if isinstance(raw_policy_payload, dict) else {}
            )
            policy_name = policy_name_from_artifact(policy_payload)
            if policy_name == "control_fixed":
                continue
            bars = self._get_live_bars(
                symbol,
                broker_symbol,
                pos_timeframe,
                count=int(getattr(self.pipeline_config, "live_bars_count", 200)),
                min_required=max(
                    50, int(getattr(self.pipeline_config, "live_min_bars", 100) or 100)
                ),
            )
            if bars is None or len(bars) < 3:
                continue
            current_bar_time = pd.Timestamp(bars.index[-1])
            cache_key = f"{ticket_id}:{symbol}:{pos_timeframe}:{pos_regime}"
            last_applied_bar = self._last_governance_bar_times.get(cache_key)
            if last_applied_bar is not None and current_bar_time <= pd.Timestamp(
                last_applied_bar
            ):
                continue
            detector_params = None
            detector_params_sha = None
            if bool(getattr(self, "_is_current_refit_demo", False)):
                try:
                    detector_params, detector_params_sha = (
                        self._current_refit_demo_detector(symbol, pos_timeframe)
                    )
                except Exception as exc:
                    self.logger.error(f"[{symbol}] [{pos_timeframe}] {exc}")
                    continue
            try:
                features = FeatureComputer.compute_all(
                    bars,
                    symbol=symbol,
                    timeframe=pos_timeframe,
                    regime_params_file=regime_params_file,
                    regime_params_override=detector_params,
                    regime_params_sha256=detector_params_sha,
                )
            except Exception as exc:
                self.logger.debug(
                    f"[{symbol}] Governance feature build failed for {pos_timeframe}: {exc}"
                )
                continue
            if len(features) < 2 or "ATR_14" not in features.columns:
                self._last_governance_bar_times[cache_key] = current_bar_time
                continue
            closed_features = features.iloc[:-1].copy()
            if closed_features.empty:
                self._last_governance_bar_times[cache_key] = current_bar_time
                continue
            entry_time_raw = getattr(position, "time", None)
            if entry_time_raw is None:
                continue
            entry_time = pd.Timestamp(entry_time_raw)
            if entry_time >= pd.Timestamp(closed_features.index[-1]):
                continue
            start_idx = max(
                0, int(closed_features.index.searchsorted(entry_time, side="left"))
            )
            since_entry = closed_features.iloc[start_idx:]
            if since_entry.empty:
                continue
            symbol_info = self.mt5.get_symbol_info(broker_symbol)
            spec = self._symbol_info_to_instrument_spec(symbol, symbol_info)
            min_stop_distance = 0.0
            if symbol_info is not None:
                point = float(getattr(symbol_info, "point", 0.0) or 0.0)
                stop_distance = (
                    float(getattr(symbol_info, "trade_stops_level", 0) or 0) * point
                )
                freeze_distance = (
                    float(getattr(symbol_info, "trade_freeze_level", 0) or 0) * point
                )
                min_stop_distance = max(stop_distance, freeze_distance)
            elif getattr(spec, "point", 0.0) and getattr(spec, "stops_level", 0):
                min_stop_distance = float(getattr(spec, "stops_level", 0) or 0) * float(
                    getattr(spec, "point", 0.0) or 0.0
                )
            half_spread = (
                spec.get_half_spread_price()
                if getattr(self.pipeline_config, "use_spread", True)
                else 0.0
            )
            prev_close = float(closed_features["Close"].iloc[-1])
            is_long = int(getattr(position, "type", 1)) == 0
            current_price = None
            try:
                tick = (
                    self.mt5.get_symbol_tick(broker_symbol)
                    if hasattr(self.mt5, "get_symbol_tick")
                    else None
                )
            except Exception as exc:
                self.logger.debug(
                    f"[{symbol}] Governance tick unavailable for {broker_symbol}: {exc}"
                )
                tick = None
            if tick is not None and not self._looks_like_mock(tick):
                try:
                    quote_price = float(
                        getattr(tick, "bid", 0.0)
                        if is_long
                        else getattr(tick, "ask", 0.0)
                    )
                    if math.isfinite(quote_price) and quote_price > 0.0:
                        current_price = quote_price
                except Exception:
                    current_price = None
            if current_price is None:
                current_price = (
                    prev_close - half_spread
                    if is_long and getattr(self.pipeline_config, "use_spread", True)
                    else (
                        prev_close + half_spread
                        if (not is_long)
                        and getattr(self.pipeline_config, "use_spread", True)
                        else prev_close
                    )
                )
            state = self._get_order_governance_state(ticket_id)
            if not state:
                state = {
                    "initial_stop_loss": float(getattr(position, "sl", 0.0) or 0.0),
                    "initial_take_profit": float(getattr(position, "tp", 0.0) or 0.0),
                    "tp_released": False,
                    "shadow_stop_loss": float(getattr(position, "sl", 0.0) or 0.0),
                    "shadow_take_profit": float(getattr(position, "tp", 0.0) or 0.0),
                    "shadow_tp_released": False,
                    "partial_taken": False,
                    "shadow_partial_taken": False,
                }
            shadow_mode = mode == "shadow"
            current_stop_loss = (
                float(
                    state.get("shadow_stop_loss", getattr(position, "sl", 0.0)) or 0.0
                )
                if shadow_mode
                else float(getattr(position, "sl", 0.0) or 0.0)
            )
            current_take_profit = (
                float(
                    state.get("shadow_take_profit", getattr(position, "tp", 0.0)) or 0.0
                )
                if shadow_mode
                else float(getattr(position, "tp", 0.0) or 0.0)
            )
            tp_released = bool(
                state.get("shadow_tp_released", False)
                if shadow_mode
                else state.get("tp_released", False)
            )
            structure_lookback = int(policy_payload.get("structure_lookback", 20) or 20)
            recent_window = since_entry.tail(max(1, structure_lookback))
            current_regime = str(pos_regime or "")
            if "REGIME_LIVE" in closed_features.columns:
                current_regime = str(closed_features["REGIME_LIVE"].iloc[-1])
            elif "REGIME" in closed_features.columns:
                current_regime = str(closed_features["REGIME"].iloc[-1])
            current_regime_gap = 0.0
            if "REGIME_GAP" in closed_features.columns:
                try:
                    current_regime_gap = float(
                        pd.to_numeric(
                            closed_features["REGIME_GAP"].iloc[-1], errors="coerce"
                        )
                    )
                except Exception:
                    current_regime_gap = 0.0
            current_atr = 0.0
            try:
                current_atr = float(
                    pd.to_numeric(closed_features["ATR_14"].iloc[-1], errors="coerce")
                )
                if not math.isfinite(current_atr):
                    current_atr = 0.0
            except Exception:
                current_atr = 0.0
            realized_vol_ratio = None
            try:
                atr_window = (
                    pd.to_numeric(closed_features["ATR_14"], errors="coerce")
                    .dropna()
                    .tail(50)
                )
                atr_mean = float(atr_window.mean()) if not atr_window.empty else 0.0
                if atr_mean > 0.0:
                    realized_vol_ratio = current_atr / atr_mean
            except Exception:
                realized_vol_ratio = None
            context = GovernanceContext(
                symbol=symbol,
                timeframe=pos_timeframe,
                regime=pos_regime,
                direction=1 if is_long else -1,
                entry_price=float(getattr(position, "price_open", 0.0) or 0.0),
                current_stop_loss=current_stop_loss,
                current_take_profit=current_take_profit,
                initial_stop_loss=float(
                    state.get("initial_stop_loss", getattr(position, "sl", 0.0)) or 0.0
                ),
                initial_take_profit=float(
                    state.get("initial_take_profit", getattr(position, "tp", 0.0))
                    or 0.0
                ),
                current_price=current_price,
                current_atr=current_atr,
                highest_since_entry=float(
                    pd.to_numeric(since_entry["High"], errors="coerce").max()
                ),
                lowest_since_entry=float(
                    pd.to_numeric(since_entry["Low"], errors="coerce").min()
                ),
                pip_size=float(getattr(spec, "pip_size", 0.0) or 0.0),
                price_step=float(
                    getattr(symbol_info, "point", 0.0)
                    or getattr(spec, "point", 0.0)
                    or getattr(spec, "pip_size", 0.0)
                    or 0.0
                ),
                min_stop_distance=min_stop_distance,
                tp_released=tp_released,
                bars_held=len(since_entry),
                entry_regime=pos_regime,
                current_regime=current_regime,
                current_regime_gap=current_regime_gap,
                recent_high=float(
                    pd.to_numeric(recent_window["High"], errors="coerce").max()
                ),
                recent_low=float(
                    pd.to_numeric(recent_window["Low"], errors="coerce").min()
                ),
                realized_vol_ratio=realized_vol_ratio,
                partial_taken=bool(
                    state.get("shadow_partial_taken", False)
                    if shadow_mode
                    else state.get("partial_taken", False)
                ),
            )
            decision = evaluate_policy(policy_payload, context)
            self._last_governance_bar_times[cache_key] = current_bar_time
            new_sl = decision.stop_loss
            new_tp = None
            if decision.take_profit is not None:
                new_tp = (
                    0.0
                    if math.isinf(float(decision.take_profit))
                    else float(decision.take_profit)
                )
            if mode == "shadow":
                if (
                    new_sl is not None
                    or new_tp is not None
                    or decision.tp_released
                    or decision.partial_close_pct > 0
                ):
                    self.logger.info(
                        f"[{symbol}] Governance SHADOW {policy_name} @ {pos_timeframe}/{pos_regime}: "
                        f"sl={f'{new_sl:.5f}' if new_sl is not None else 'keep'} "
                        f"tp={'release' if new_tp == 0.0 else (f'{new_tp:.5f}' if new_tp is not None else 'keep')} "
                        f"partial={decision.partial_close_pct:.1f}%"
                    )
                if new_sl is not None:
                    state["shadow_stop_loss"] = float(new_sl)
                if new_tp is not None:
                    state["shadow_take_profit"] = float(new_tp)
                state["shadow_tp_released"] = bool(
                    state.get("shadow_tp_released", False) or decision.tp_released
                )
                if decision.partial_close_pct > 0:
                    state["shadow_partial_taken"] = True
                state["last_policy_name"] = policy_name
                state["last_bar_time"] = current_bar_time.isoformat()
                self._set_order_governance_state(ticket_id, state)
                continue
            if decision.partial_close_pct > 0 and not state.get("partial_taken", False):
                if self.enable_trading and hasattr(self.mt5, "close_position_partial"):
                    partial_result = self.mt5.close_position_partial(
                        position,
                        percent=decision.partial_close_pct,
                        comment=f"Governance partial {policy_name}",
                    )
                    if getattr(partial_result, "success", False):
                        state["partial_taken"] = True
                        state["last_policy_name"] = policy_name
                        state["last_bar_time"] = current_bar_time.isoformat()
                        state["last_partial_close_at"] = datetime.now().isoformat()
                        self._set_order_governance_state(ticket_id, state)
                        self.logger.info(
                            f"[{symbol}] Governance {policy_name} partial close "
                            f"{decision.partial_close_pct:.1f}% applied @ {pos_timeframe}/{pos_regime}"
                        )
                    else:
                        self.logger.warning(
                            f"[{symbol}] Governance {policy_name} partial close failed: "
                            f"{getattr(partial_result, 'retcode_description', 'unknown error')}"
                        )
                else:
                    self.logger.info(
                        f"[{symbol}] Governance {policy_name} partial close signal "
                        f"{decision.partial_close_pct:.1f}% (paper/no connector support)"
                    )
                    state["partial_taken"] = True
                    state["last_policy_name"] = policy_name
                    state["last_bar_time"] = current_bar_time.isoformat()
                    state["last_partial_close_at"] = datetime.now().isoformat()
                    self._set_order_governance_state(ticket_id, state)
            if decision.exit_now:
                if self.enable_trading and hasattr(self.mt5, "close_position"):
                    close_result = self.mt5.close_position(
                        position,
                        comment=f"Governance exit {policy_name}",
                    )
                    if getattr(close_result, "success", False):
                        self.logger.info(
                            f"[{symbol}] Governance {policy_name} exited position @ {pos_timeframe}/{pos_regime}"
                        )
                        state["last_policy_name"] = policy_name
                        state["last_bar_time"] = current_bar_time.isoformat()
                        state["last_applied_at"] = datetime.now().isoformat()
                        self._set_order_governance_state(ticket_id, state)
                        continue
                    self.logger.warning(
                        f"[{symbol}] Governance {policy_name} exit failed: "
                        f"{getattr(close_result, 'retcode_description', 'unknown error')}"
                    )
                else:
                    self.logger.info(
                        f"[{symbol}] Governance {policy_name} exit signal (paper/no connector support)"
                    )
                    state["last_policy_name"] = policy_name
                    state["last_bar_time"] = current_bar_time.isoformat()
                    self._set_order_governance_state(ticket_id, state)
                    continue
            if (
                new_sl is None
                and new_tp is None
                and not decision.tp_released
                and not decision.exit_now
            ):
                state["last_policy_name"] = policy_name
                state["last_bar_time"] = current_bar_time.isoformat()
                self._set_order_governance_state(ticket_id, state)
                continue
            result = self.mt5.modify_position(position, sl=new_sl, tp=new_tp)
            if getattr(result, "success", False):
                self.logger.info(
                    f"[{symbol}] Governance {policy_name} applied @ {pos_timeframe}/{pos_regime}: "
                    f"sl={f'{new_sl:.5f}' if new_sl is not None else 'keep'} "
                    f"tp={'release' if new_tp == 0.0 else (f'{new_tp:.5f}' if new_tp is not None else 'keep')}"
                )
                state["tp_released"] = bool(
                    state.get("tp_released", False) or decision.tp_released
                )
                if new_sl is not None:
                    state["shadow_stop_loss"] = float(new_sl)
                if new_tp is not None:
                    state["shadow_take_profit"] = float(new_tp)
                state["shadow_tp_released"] = bool(
                    state.get("shadow_tp_released", False) or decision.tp_released
                )
                state["last_policy_name"] = policy_name
                state["last_bar_time"] = current_bar_time.isoformat()
                state["last_applied_at"] = datetime.now().isoformat()
                self._set_order_governance_state(ticket_id, state)
            else:
                self.logger.warning(
                    f"[{symbol}] Governance {policy_name} modify failed @ {pos_timeframe}/{pos_regime}: "
                    f"{getattr(result, 'retcode_description', 'unknown error')}"
                )

    def _run_portfolio_observatory(
        self,
        validated_configs: Dict[str, SymbolConfig],
        positions_snapshot: Optional[List[Any]],
        account_info: Optional[Any],
    ) -> None:
        observatory = getattr(self._enhancement_seams, "portfolio_observatory", None)
        if observatory is None or not getattr(observatory, "enabled", False):
            return
        estimated_risk_by_symbol: Dict[str, float] = {}
        for position in list(positions_snapshot or []):
            broker_symbol = str(getattr(position, "symbol", "") or "")
            symbol = (
                self._resolve_symbol_from_broker(broker_symbol, validated_configs)
                or broker_symbol
            )
            risk_pct = self._estimate_position_risk_pct(
                position,
                account_info=account_info,
                canonical_symbol=symbol,
                broker_symbol=broker_symbol,
            )
            if risk_pct is not None:
                estimated_risk_by_symbol[symbol] = estimated_risk_by_symbol.get(
                    symbol, 0.0
                ) + float(risk_pct)
        snapshot = observatory.snapshot(
            PortfolioObservationContext(
                positions=list(positions_snapshot or []),
                estimated_risk_by_symbol=estimated_risk_by_symbol,
            )
        )
        self._last_portfolio_observation = snapshot
        if snapshot.get("clusters"):
            preview = ", ".join(
                f"{item['cluster']}:{item['open_positions']}"
                for item in snapshot["clusters"][:4]
            )
            self.logger.info(
                f"Portfolio observatory: open={snapshot.get('open_positions', 0)} "
                f"symbols={snapshot.get('symbols_with_positions', 0)} | {preview}"
            )

    def _evaluate_daily_loss_advisory(
        self, account_info: Optional[Any], recent_deals: Optional[List[Dict[str, Any]]]
    ) -> None:
        def _safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        if account_info is None:
            return
        daily_threshold = _safe_float(
            getattr(self.pipeline_config, "daily_loss_advisory_pct", 0.0), 0.0
        )
        session_threshold = _safe_float(
            getattr(self.pipeline_config, "session_loss_advisory_pct", 0.0), 0.0
        )
        if daily_threshold <= 0 and session_threshold <= 0:
            return
        deals = list(recent_deals or [])
        if not deals:
            return
        balance = _safe_float(getattr(account_info, "balance", 0.0), 0.0)
        if balance <= 0:
            return
        now = datetime.now()
        day_start = datetime(now.year, now.month, now.day)
        cycle_start = self.pipeline_config.get_last_retrain_slot(now)
        daily_realized = sum(
            float(deal.get("profit", 0.0) or 0.0)
            for deal in deals
            if deal.get("time") and deal["time"] >= day_start
        )
        cycle_realized = sum(
            float(deal.get("profit", 0.0) or 0.0)
            for deal in deals
            if deal.get("time") and deal["time"] >= cycle_start
        )
        thresholds = [
            ("daily", daily_threshold, daily_realized),
            ("session", session_threshold, cycle_realized),
        ]
        for label, threshold_pct, realized in thresholds:
            if threshold_pct <= 0:
                continue
            loss_pct = max(0.0, (-float(realized) / balance) * 100.0)
            state = "tripped" if loss_pct >= threshold_pct else "clear"
            prior_state = self._daily_advisory_state.get(label)
            if state == "tripped" and prior_state != "tripped":
                self.logger.info(
                    f"{label.upper()} LOSS ADVISORY: realized={realized:.2f} ({loss_pct:.2f}% of balance) "
                    f">= threshold {threshold_pct:.2f}% | entries continue"
                )
            self._daily_advisory_state[label] = state

    def _realized_loss_hard_block_reasons(
        self, account_info: Optional[Any]
    ) -> List[str]:
        """Fail-closed realized-loss guard independent of the optional supervisor."""
        if account_info is None:
            return []

        reasons: List[str] = []
        for period, attr in (
            ("daily", "daily_loss_hard_block_pct"),
            ("session", "session_loss_hard_block_pct"),
            ("weekly", "weekly_loss_hard_block_pct"),
        ):
            threshold = (
                self._safe_finite_float(
                    getattr(self.pipeline_config, attr, 0.0),
                    default=0.0,
                )
                or 0.0
            )
            threshold = max(0.0, float(threshold))
            if threshold <= 0.0:
                continue
            loss_pct = self._estimate_realized_loss_pct(period, account_info)
            if loss_pct is None:
                reasons.append(f"{period}_loss unavailable; fail-closed")
                continue
            if loss_pct >= threshold:
                reasons.append(f"{period}_loss {loss_pct:.2f}% >= {threshold:.2f}%")
        return reasons

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        total = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total * 100) if total > 0 else 0
        return {
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "hit_rate_pct": round(hit_rate, 1),
            "cache_size": len(self._candidate_cache),
        }

    def _build_magic_lookup(
        self, symbol: str, config: SymbolConfig
    ) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Build magic->timeframe/regime lookup for fast position inference."""
        magic_to_tf: Dict[int, str] = {}
        magic_to_regime: Dict[int, str] = {}

        try:
            if config and config.has_regime_configs():
                for tf in config.get_available_timeframes():
                    for regime in config.get_regimes_for_timeframe(tf):
                        magic = self._encode_specialist_magic(symbol, tf, regime)
                        magic_to_tf[magic] = tf
                        magic_to_regime[magic] = regime
            elif config and config.timeframe:
                magic = self._encode_specialist_magic(
                    symbol, config.timeframe, "LEGACY"
                )
                magic_to_tf[magic] = config.timeframe
                magic_to_regime[magic] = "LEGACY"
        except Exception:
            pass

        return magic_to_tf, magic_to_regime

    def _infer_position_timeframe(self, symbol: str, config, position) -> Optional[str]:
        """
        Infer the timeframe of an open position using a strict priority chain.

        Priority:
        1. Manual override from pipeline_config.position_timeframe_overrides
           (keyed by "ticket:<n>" or "magic:<n>").
        2. Decode the live MT5 comment (full or truncated PM2/PM3/PM1 formats).
        3. Match a legacy "PM_<tag>" strategy tag against the symbol's regime_configs.
        4. Lookup magic number in the deterministic CRC32 table.
        5. MT5 history-based recovery: query the opening deal/order by
           POSITION_IDENTIFIER for the original comment/magic, then re-decode.
        6. Return None (caller keeps fail-closed behavior).
        """
        from pm_position import TradeTagEncoder

        ticket = getattr(position, "ticket", 0) or 0
        magic = int(getattr(position, "magic", 0) or 0)
        comment = getattr(position, "comment", "") or ""

        # --- Priority 1: Manual override ---
        overrides = {}
        pc = getattr(self, "pipeline_config", None)
        if pc:
            overrides = getattr(pc, "position_timeframe_overrides", None) or {}
        if not isinstance(overrides, dict):
            overrides = {}
        if ticket and overrides.get(f"ticket:{ticket}"):
            return overrides[f"ticket:{ticket}"]
        if magic and overrides.get(f"magic:{magic}"):
            return overrides[f"magic:{magic}"]

        # --- Priority 2: Decode live comment ---
        decoded = TradeTagEncoder.decode_comment(comment) if comment else None
        if decoded and decoded.get("timeframe"):
            return decoded["timeframe"]

        # --- Priority 3: Legacy strategy tag match ---
        if decoded and decoded.get("strategy_tag") and config:
            tag = decoded["strategy_tag"].lower()
            matched_tfs = set()
            try:
                if hasattr(config, "regime_configs") and config.regime_configs:
                    for tf, regimes in config.regime_configs.items():
                        if isinstance(regimes, dict):
                            for _regime, rc in regimes.items():
                                sname = getattr(rc, "strategy_name", "") or ""
                                if sname and tag in sname.lower():
                                    matched_tfs.add(tf)
            except Exception:
                pass
            if len(matched_tfs) == 1:
                return matched_tfs.pop()

        # --- Priority 4: Magic number lookup ---
        magic_to_tf, _ = self._build_magic_lookup(symbol, config)
        tf_from_magic = magic_to_tf.get(magic)
        if tf_from_magic:
            return tf_from_magic

        # --- Priority 5: MT5 history recovery (cached per session) ---
        cache = getattr(self, "_tf_recovery_cache", None)
        if cache is None:
            self._tf_recovery_cache = {}
            cache = self._tf_recovery_cache
        identifier = getattr(position, "identifier", 0) or ticket
        if identifier:
            cache_key = int(identifier)
            if cache_key in cache:
                return cache[cache_key]  # May be None (negative cache)
            mt5_conn = getattr(self, "mt5", None)
            if mt5_conn and hasattr(mt5_conn, "get_position_opening_metadata"):
                try:
                    meta = mt5_conn.get_position_opening_metadata(cache_key)
                    if meta:
                        # Try decoding the historical comment
                        hist_comment = meta.get("comment", "")
                        hist_decoded = (
                            TradeTagEncoder.decode_comment(hist_comment)
                            if hist_comment
                            else None
                        )
                        if hist_decoded and hist_decoded.get("timeframe"):
                            cache[cache_key] = hist_decoded["timeframe"]
                            return hist_decoded["timeframe"]
                        # Try the historical magic against our lookup
                        hist_magic = meta.get("magic", 0)
                        if hist_magic and hist_magic in magic_to_tf:
                            cache[cache_key] = magic_to_tf[hist_magic]
                            return magic_to_tf[hist_magic]
                except Exception:
                    pass
            # Negative cache: don't re-query failed lookups
            cache[cache_key] = None

        # --- Priority 6: Unknown ---
        return None

    def _log_no_actionable_signal(
        self,
        symbol: str,
        message: str,
        best_candidate: Dict,
        bar_time_iso: str,
        action_type: str,
    ) -> None:
        """
        Log no actionable signal with throttle suppression to prevent duplicate logs.

        This helper prevents log spam when the same no-signal decision occurs
        multiple times within the same bar during repeated live runtime cycles.

        Args:
            symbol: Trading symbol
            message: Log message to display
            best_candidate: Dictionary containing candidate info (strategy_name, timeframe, regime)
            bar_time_iso: Bar time in ISO format
            action_type: Action type for throttle record (e.g., "NO_ACTIONABLE_WINNER_SIGNAL")
        """
        # Build decision key with direction=0 for no-signal cases
        dk = DecisionThrottle.make_decision_key(
            symbol,
            best_candidate.get("strategy_name", "UNKNOWN"),
            best_candidate.get("timeframe", "?"),
            best_candidate.get("regime", "?"),
            0,  # direction=0 is correct for no-signal
            bar_time_iso,
        )

        # Check if we should suppress this log (already logged in this bar)
        if self._decision_throttle.should_suppress(symbol, dk, bar_time_iso):
            return  # Silent return - already logged this decision for this bar

        # Surface throttled no-trade outcomes at INFO so quiet live periods remain observable.
        self.logger.info(f"[{symbol}] {message}")

        # Record the decision in throttle
        self._decision_throttle.record_decision(
            symbol=symbol,
            decision_key=dk,
            bar_time_iso=bar_time_iso,
            timeframe=best_candidate.get("timeframe", "?"),
            regime=best_candidate.get("regime", "?"),
            strategy_name=best_candidate.get("strategy_name", "UNKNOWN"),
            direction=0,
            action=action_type,
        )

    def _candidate_rank_key(
        self, candidate: Dict[str, Any]
    ) -> Tuple[float, float, float, int, str]:
        """Deterministic candidate ranking key for live winner selection."""
        try:
            selection_score = float(candidate.get("selection_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            selection_score = 0.0
        try:
            quality_score = float(candidate.get("quality_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            quality_score = 0.0
        try:
            regime_strength = float(candidate.get("regime_strength", 0.0) or 0.0)
        except (TypeError, ValueError):
            regime_strength = 0.0
        signal_rank = 1 if int(candidate.get("signal", 0) or 0) != 0 else 0
        candidate_id = "|".join(
            [
                str(candidate.get("timeframe", "") or "").upper(),
                str(candidate.get("regime", "") or ""),
                str(candidate.get("strategy_name", "") or ""),
            ]
        )
        return (
            selection_score,
            quality_score,
            regime_strength,
            signal_rank,
            candidate_id,
        )

    def _plan_symbol_specialist_admissions(
        self,
        candidates: List[Dict[str, Any]],
        *,
        allow_d1_plus_lower: bool,
        open_position_count: int,
    ) -> List[Dict[str, Any]]:
        """Return every distinct specialist cell in deterministic rank order.

        ``allow_d1_plus_lower`` and ``open_position_count`` remain accepted for
        compatibility but have no admission authority.  Topology is one open
        position per authenticated ``symbol|timeframe|regime`` cell; aggregate
        exposure is governed later by the signed Capital envelope.
        """
        _ = allow_d1_plus_lower, open_position_count
        ranked = sorted(
            list(candidates or []),
            key=self._candidate_rank_key,
            reverse=True,
        )
        admitted: List[Dict[str, Any]] = []
        identity_by_slot: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
        for candidate in ranked:
            timeframe = str(candidate.get("timeframe", "") or "").upper()
            regime = str(candidate.get("regime", "") or "").upper()
            strategy = str(candidate.get("strategy_name", "") or "")
            if not timeframe or not regime or not strategy:
                raise ValueError("SPECIALIST_CELL_IDENTITY_INVALID")
            slot = (timeframe, regime)
            identity = (timeframe, regime, strategy)
            existing = identity_by_slot.get(slot)
            if existing is not None:
                if existing != identity:
                    raise ValueError(
                        "SPECIALIST_CELL_SLOT_IDENTITY_COLLISION:"
                        f"{timeframe}|{regime}"
                    )
                continue
            identity_by_slot[slot] = identity
            admitted.append(candidate)
        return admitted

    def _queue_specialist_timeframe(self, symbol: str, timeframe: str) -> None:
        pending = getattr(self, "_pending_specialist_timeframes", None)
        if pending is None:
            pending = {}
            self._pending_specialist_timeframes = pending
        normalized_symbol = str(symbol or "")
        normalized_tf = str(timeframe or "").upper()
        if normalized_symbol and normalized_tf:
            pending.setdefault(normalized_symbol, set()).add(normalized_tf)

    def _consume_specialist_timeframes(
        self,
        symbol: str,
        timeframes: Optional[Set[str]],
    ) -> None:
        pending = getattr(self, "_pending_specialist_timeframes", None)
        if not isinstance(pending, dict):
            return
        symbol_pending = pending.get(str(symbol or ""))
        if not symbol_pending:
            return
        normalized = {
            str(timeframe or "").upper()
            for timeframe in (timeframes or set())
            if str(timeframe or "").strip()
        }
        if normalized:
            symbol_pending.difference_update(normalized)
        if not symbol_pending:
            pending.pop(str(symbol or ""), None)

    @staticmethod
    def _specialist_rate_limit_key(symbol: str, magic: int) -> str:
        """Return the exact-cell key used by the order submission limiter."""
        return f"{str(symbol or '').upper()}|{int(magic)}"

    def _encode_specialist_magic(self, symbol: str, timeframe: str, regime: str) -> int:
        """Encode an exact-cell magic in the selected book namespace."""
        from pm_position import TradeTagEncoder

        regime_key = str(regime or "").upper()
        if bool(getattr(self, "_is_current_refit_demo", False)):
            regime_key = f"CURRENT_REFIT::{regime_key}"
        return TradeTagEncoder.encode_magic(symbol, timeframe, regime_key)

    @staticmethod
    def _current_refit_demo_account_block_reason(
        account_info: Any,
    ) -> Optional[str]:
        """Require the one authorized, tradable broker DEMO hedging account."""
        from pm_demo_readiness import demo_account_errors

        errors = demo_account_errors(account_info)
        return errors[0] if errors else None

    def bind_demo_management_authority(
        self, capsule_id: str, generation_id: str
    ) -> None:
        """Bind one verified management capsule to this demo runtime."""
        if not bool(getattr(self, "_is_current_refit_demo", False)):
            raise ValueError("management authority is only valid for current-refit demo")
        if not self._plain_sha256(capsule_id):
            raise ValueError("invalid demo management capsule id")
        if (
            type(generation_id) is not str
            or not generation_id
            or generation_id != generation_id.strip()
            or len(generation_id.encode("utf-8")) > 256
            or any(ord(character) < 32 for character in generation_id)
        ):
            raise ValueError("invalid demo management generation id")
        existing = (
            str(getattr(self, "_demo_management_capsule_id", "") or ""),
            str(getattr(self, "_demo_management_generation_id", "") or ""),
        )
        requested = (capsule_id, generation_id)
        if any(existing) and existing != requested:
            raise LiveOutcomeError("demo management authority cannot be rebound")
        self._demo_management_capsule_id = capsule_id
        self._demo_management_generation_id = generation_id

    def _current_refit_demo_position_block_reason(
        self,
        positions: Optional[List[Any]],
        configs: Dict[str, SymbolConfig],
    ) -> Optional[str]:
        """Require only exactly attributed current-refit positions."""
        if not bool(getattr(self, "_is_current_refit_demo", False)):
            return None
        if positions is None:
            return "position snapshot unavailable"
        from pm_position import TradeTagEncoder

        allowed: Dict[Tuple[str, int], Tuple[str, str, str]] = {}
        for symbol, config in (configs or {}).items():
            broker_symbol = self._resolve_broker_symbol_for_live(symbol)
            for timeframe, regimes in (config.regime_configs or {}).items():
                for regime, cell in (regimes or {}).items():
                    magic = self._encode_specialist_magic(symbol, timeframe, regime)
                    key = (str(broker_symbol), magic)
                    identity = (
                        str(symbol).upper(),
                        str(timeframe).upper(),
                        TradeTagEncoder._strategy_code(cell.strategy_name, max_len=5),
                    )
                    if key in allowed:
                        return f"demo magic collision: {symbol}|{timeframe}|{regime}"
                    allowed[key] = identity
        for position in list(positions or []):
            magic = int(getattr(position, "magic", 0) or 0)
            comment = str(getattr(position, "comment", "") or "")
            decoded = TradeTagEncoder.decode_comment(comment) if comment else None
            broker_symbol = str(getattr(position, "symbol", "") or "")
            expected = allowed.get((broker_symbol, magic))
            actual = (
                str((decoded or {}).get("symbol", "") or "").upper(),
                str((decoded or {}).get("timeframe", "") or "").upper(),
                str((decoded or {}).get("strategy_code", "") or ""),
            )
            role = str((decoded or {}).get("book_role", "") or "").upper()
            if expected is None or actual != expected or role != PROCESS_VALIDATED_CURRENT_REFIT_ROLE:
                ticket = int(getattr(position, "ticket", 0) or 0)
                return (
                    f"position ticket={ticket or '<unknown>'} is not an exactly attributed "
                    "current-refit demo position; use a dedicated clean DEMO account"
                )
        return None

    @staticmethod
    def _multi_position_account_block_reason(account_info: Any) -> Optional[str]:
        """Require MT5 retail-hedging accounting for independent specialists."""
        if account_info is None:
            return "account mode unavailable for same-symbol multi-position admission"
        try:
            margin_mode = int(getattr(account_info, "margin_mode"))
        except (AttributeError, TypeError, ValueError, OverflowError):
            return "ACCOUNT_MARGIN_MODE unavailable for same-symbol multi-position admission"
        if margin_mode != 2:
            return (
                f"ACCOUNT_MARGIN_MODE={margin_mode} is netting/exchange; "
                "independent specialist positions require RETAIL_HEDGING (2)"
            )
        hedge_allowed = getattr(account_info, "hedge_allowed", None)
        if hedge_allowed is None:
            return "ACCOUNT_HEDGE_ALLOWED unavailable for same-symbol multi-position admission"
        if not bool(hedge_allowed):
            return "ACCOUNT_HEDGE_ALLOWED is false for same-symbol multi-position admission"
        return None

    def _same_symbol_specialist_position_block(
        self,
        symbol: str,
        config: SymbolConfig,
        positions: Optional[List[Any]],
        account_info: Any,
        *,
        candidate_magic: Optional[int],
    ) -> Optional[Tuple[str, str]]:
        """Validate exact-cell uniqueness and hedging attribution."""

        if positions is None:
            return (
                "BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE",
                "same-symbol position snapshot unavailable",
            )
        observed = list(positions or [])
        if candidate_magic is not None and any(
            int(getattr(position, "magic", 0) or 0) == int(candidate_magic)
            for position in observed
        ):
            return (
                "BLOCKED_SPECIALIST_CELL_ALREADY_OPEN",
                "the exact symbol|timeframe|regime specialist cell is already open",
            )
        if not observed:
            return None
        account_reason = self._multi_position_account_block_reason(account_info)
        if account_reason:
            return "BLOCKED_ACCOUNT_POSITION_MODE", account_reason
        for position in observed:
            if not str(
                self._infer_position_timeframe(symbol, config, position) or ""
            ).upper():
                return (
                    "BLOCKED_POSITION_TIMEFRAME_UNKNOWN",
                    "same-symbol position is not attributable to a specialist timeframe",
                )
        return None

    def _promotion_status_is_entry_eligible(self, status: Any) -> bool:
        """Apply the exact selected-book status contract."""
        normalized = str(status or "BLOCKED").strip().upper() or "BLOCKED"
        never_eligible = {
            "",
            "BLOCKED",
            "PAPER_ONLY",
            "NO_TRADE",
            "UNKNOWN",
            PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
        }
        if bool(getattr(self, "_is_current_refit_demo", False)):
            return normalized == PROCESS_VALIDATED_CURRENT_REFIT_ROLE
        if not bool(getattr(self, "enable_trading", False)):
            # Paper mode remains an observability surface for PROMOTE_SMALL.
            return normalized in {"PROMOTED", "PROMOTE_SMALL"}

        configured = getattr(
            self.pipeline_config,
            "live_allowed_promotion_statuses",
            ["PROMOTED"],
        )
        if isinstance(configured, str):
            configured = configured.split(",")
        allowed = {
            str(value or "").strip().upper()
            for value in (configured or ["PROMOTED"])
            if str(value or "").strip()
        }
        allowed -= never_eligible
        return normalized in (allowed or {"PROMOTED"})

    def _regime_config_is_entry_eligible(
        self,
        regime_config: Any,
        *,
        min_pf: float,
        min_return: float,
        max_dd: float,
        min_return_dd_ratio: float,
        symbol: str = "",
        timeframe: str = "",
        regime: str = "",
    ) -> bool:
        """Validate a cell within its selected execution book."""
        if regime_config is None or regime_config.is_no_trade_marker():
            return False
        if not bool(getattr(self, "_is_current_refit_demo", False)):
            return bool(
                regime_config.is_valid_for_live(
                    min_pf,
                    min_return,
                    max_dd,
                    min_return_dd_ratio,
                )
            )
        if not symbol or not timeframe or not regime:
            return False
        return (
            self._current_refit_cell_integrity_reason(
                symbol, timeframe, regime, regime_config
            )
            is None
        )

    def _promotion_authorization_block_reason(
        self,
        symbol: str,
        timeframe: str,
        regime: str,
    ) -> Optional[str]:
        """Fresh exact-cell check at the final send boundary."""
        if bool(getattr(self, "_is_current_refit_demo", False)):
            capital_reason = self._current_refit_demo_capital_block_reason()
            if capital_reason:
                return capital_reason
            config = self._get_live_configs().get(str(symbol).upper())
            if config is None:
                return "current-refit symbol is not entry-eligible"
            cell = config.get_regime_config(
                str(timeframe).upper(), str(regime).upper()
            )
            if cell is None:
                return "current-refit cell is not entry-eligible"
            return self._current_refit_cell_integrity_reason(
                symbol, timeframe, regime, cell
            )
        if not bool(getattr(self, "enable_trading", False)):
            return None
        gate_required = bool(
            getattr(self.pipeline_config, "production_require_promotion_gate", True)
        )
        gate_mode = (
            str(
                getattr(
                    self.pipeline_config,
                    "production_promotion_gate_mode",
                    "ledger_status",
                )
                or ""
            )
            .strip()
            .lower()
        )
        if gate_mode in {"immutable", "authorization", "authorization_exact"}:
            gate_mode = "immutable_exact"
        funded_execution = bool(
            getattr(self.pipeline_config, "_funded_live_execution", False)
        )
        if funded_execution and (not gate_required or gate_mode != "immutable_exact"):
            return (
                "funded entry requires the immutable_exact promotion gate "
                f"(required={gate_required}, mode={gate_mode!r})"
            )
        if not gate_required or gate_mode != "immutable_exact":
            return None
        manager = getattr(self, "pm", None)
        checker = getattr(manager, "is_exact_cell_authorized_for_live", None)
        if not callable(checker):
            return "exact-cell promotion checker unavailable"
        try:
            authorized, reason = checker(
                symbol,
                timeframe,
                regime,
                refresh=True,
            )
        except Exception as exc:
            return f"authorization check raised {type(exc).__name__}: {exc}"
        return None if authorized else str(reason or "exact cell is not approved")

    def _current_refit_demo_capital_block_reason(self) -> Optional[str]:
        if not bool(getattr(self, "_is_current_refit_demo", False)):
            return None
        try:
            from pm_demo_readiness import (
                demo_capital_config_sha256,
                demo_capital_envelope_errors,
                demo_position_config_sha256,
            )

            expected = str(
                getattr(self, "_current_refit_demo_capital_config_sha256", "") or ""
            )
            if demo_capital_config_sha256(self.pipeline_config) != expected:
                return "current-refit demo capital config changed"
            errors = demo_capital_envelope_errors(
                getattr(self, "_current_refit_demo_capital_envelope", None),
                self.pipeline_config,
            )
            if errors:
                return f"current-refit demo capital envelope invalid: {errors[0]}"
            expected_position = str(
                getattr(self, "_current_refit_demo_position_config_sha256", "") or ""
            )
            if demo_position_config_sha256(self.position_config) != expected_position:
                return "current-refit demo position config changed"
            records = getattr(self, "_current_refit_demo_detectors", None)
            authority_sha = getattr(
                self, "_current_refit_demo_detector_authority_sha256", ""
            )
            wrapper = {
                "schema_version": "fxpm.demo-regime-detector-projection.v1",
                "records": records,
            }
            if (
                not isinstance(records, dict)
                or self._canonical_payload_sha256(wrapper) != authority_sha
            ):
                return "current-refit demo detector authority changed"
            for group_id in records:
                symbol, timeframe = group_id.split("|")
                self._current_refit_demo_detector(symbol, timeframe)
        except Exception as exc:
            return f"current-refit demo capital verification failed: {type(exc).__name__}"
        return None

    def _fresh_entry_account_validation(
        self,
        baseline_account: Any,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Refresh and validate account identity/permissions immediately pre-send."""
        try:
            fresh = self.mt5.get_account_info()
        except Exception as exc:
            return None, f"fresh account snapshot raised {type(exc).__name__}: {exc}"
        if fresh is None:
            return None, "fresh account snapshot unavailable"

        if bool(getattr(self, "_is_current_refit_demo", False)):
            demo_mode_reason = self._current_refit_demo_account_block_reason(fresh)
            if demo_mode_reason:
                return None, demo_mode_reason

        equity = self._safe_finite_float(getattr(fresh, "equity", None))
        if equity is None or equity <= 0.0:
            return None, "fresh account equity is missing, non-finite, or non-positive"
        if not bool(getattr(fresh, "trade_allowed", True)):
            return None, "fresh account disallows trading"
        if not bool(getattr(fresh, "trade_expert", True)):
            return None, "fresh account disallows expert trading"

        minimum_equity = (
            self._safe_finite_float(
                getattr(self.pipeline_config, "live_min_account_equity", 0.0),
                0.0,
            )
            or 0.0
        )
        if minimum_equity > 0.0 and equity < minimum_equity:
            return (
                None,
                f"fresh equity {equity:.2f} below live minimum {minimum_equity:.2f}",
            )

        fresh_currency = str(getattr(fresh, "currency", "") or "").strip().upper()
        minimum_currency = (
            str(
                getattr(self.pipeline_config, "live_min_account_equity_currency", "")
                or ""
            )
            .strip()
            .upper()
        )
        if minimum_currency and fresh_currency != minimum_currency:
            return None, (
                f"fresh account currency {fresh_currency or '<missing>'} != "
                f"required {minimum_currency}"
            )

        connector_config = getattr(self.mt5, "config", None)
        if self._looks_like_mock(connector_config):
            connector_config = None
        is_current_refit_demo = bool(
            getattr(self, "_is_current_refit_demo", False)
        )
        if is_current_refit_demo:
            from pm_demo_readiness import AUTHORIZED_DEMO_ACCOUNT_IDENTITY

            expected_login = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login
            expected_server = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server
            expected_currency = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency
        else:
            expected_login = 0
            try:
                expected_login = int(
                    getattr(connector_config, "expected_login", 0) or 0
                )
            except (TypeError, ValueError, OverflowError):
                return None, "configured expected_login is invalid"
            expected_server = str(
                getattr(connector_config, "expected_server", "") or ""
            ).strip()
            expected_currency = (
                str(getattr(connector_config, "expected_currency", "") or "")
                .strip()
                .upper()
            )
        fresh_login = 0
        try:
            fresh_login = int(getattr(fresh, "login", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return None, "fresh account login is invalid"
        if expected_login <= 0:
            return None, "MT5 account identity requires a nonzero expected_login pin"
        if fresh_login != expected_login:
            return (
                None,
                f"fresh account login {fresh_login} != expected {expected_login}",
            )

        fresh_server = str(getattr(fresh, "server", "") or "").strip()
        if expected_server and fresh_server.casefold() != expected_server.casefold():
            return None, (
                f"fresh account server {fresh_server or '<missing>'} != expected {expected_server}"
            )
        if not expected_server:
            return None, "MT5 account identity requires a nonempty expected_server pin"
        if not expected_currency:
            return (
                None,
                "MT5 account identity requires a nonempty expected_currency pin",
            )
        if fresh_currency != expected_currency:
            return None, (
                f"fresh account currency {fresh_currency or '<missing>'} != "
                f"expected {expected_currency}"
            )

        if baseline_account is not None:
            baseline_login = getattr(baseline_account, "login", 0)
            try:
                baseline_login = int(baseline_login or 0)
            except (TypeError, ValueError, OverflowError):
                baseline_login = 0
            if baseline_login > 0 and fresh_login > 0 and baseline_login != fresh_login:
                return (
                    None,
                    f"account login changed during decision: {baseline_login} -> {fresh_login}",
                )
            for attr, label in (("server", "server"), ("currency", "currency")):
                before = (
                    str(getattr(baseline_account, attr, "") or "").strip().casefold()
                )
                after = str(getattr(fresh, attr, "") or "").strip().casefold()
                if before and after and before != after:
                    return None, f"account {label} changed during decision"

        return fresh, None

    def _commission_amount_for_volume(
        self, symbol: str, volume: float, *, spec: Optional[Any] = None
    ) -> float:
        """Estimate configured round-trip commission for the proposed volume."""
        pipeline_config = getattr(self, "pipeline_config", None)
        if not bool(getattr(pipeline_config, "use_commission", False)):
            return 0.0
        try:
            volume_value = max(0.0, float(volume))
        except (TypeError, ValueError):
            return 0.0
        if volume_value <= 0.0:
            return 0.0
        resolved_spec = spec
        if resolved_spec is None:
            try:
                resolved_spec = get_instrument_spec(symbol)
            except Exception:
                resolved_spec = None
        try:
            commission_per_lot = float(
                getattr(resolved_spec, "commission_per_lot", 0.0) or 0.0
            )
        except (TypeError, ValueError):
            commission_per_lot = 0.0
        if commission_per_lot <= 0.0:
            return 0.0
        return commission_per_lot * volume_value

    def _remaining_exit_commission_amount(
        self, symbol: str, volume: float, *, spec: Optional[Any] = None
    ) -> float:
        """Return the uncharged exit side of the configured round-trip commission."""
        return self._commission_amount_for_volume(symbol, volume, spec=spec) * 0.5

    def _estimate_stop_pnl_amount(
        self,
        order_type: int,
        symbol: str,
        volume: float,
        entry_price: float,
        stop_price: float,
        *,
        accrued_swap: float,
        spec: Optional[Any] = None,
        symbol_info: Optional[Any] = None,
    ) -> Tuple[Optional[float], Optional[str]]:
        """Estimate signed net P&L at an open position's effective stop.

        MT5 contract math is authoritative.  Deterministic tick- and pip-value
        fallbacks preserve direction instead of applying ``abs``.  The returned
        amount includes accrued swap and the remaining one-way exit commission;
        positive values therefore represent capital already locked by the stop.
        """
        try:
            volume_value = float(volume)
            entry_value = float(entry_price)
            stop_value = float(stop_price)
            swap_value = float(accrued_swap)
        except (TypeError, ValueError, OverflowError):
            return None, None
        if (
            volume_value <= 0.0
            or entry_value <= 0.0
            or stop_value <= 0.0
            or not all(
                math.isfinite(value)
                for value in (volume_value, entry_value, stop_value, swap_value)
            )
        ):
            return None, None
        try:
            normalized_order_type = int(order_type)
        except (TypeError, ValueError, OverflowError):
            return None, None
        buy_type = int(OrderType.BUY.value)
        sell_type = int(OrderType.SELL.value)
        if normalized_order_type not in {buy_type, sell_type}:
            return None, None

        price_pnl: Optional[float] = None
        path: Optional[str] = None
        if self.mt5 is not None:
            try:
                raw_pnl = self.mt5.order_calc_profit(
                    order_type,
                    symbol,
                    volume_value,
                    entry_value,
                    stop_value,
                )
                candidate = float(raw_pnl)
            except Exception:
                candidate = float("nan")
            if math.isfinite(candidate):
                price_pnl = candidate
                path = "mt5"

        direction = 1.0 if normalized_order_type == buy_type else -1.0
        info = symbol_info
        if price_pnl is None:
            if info is None and self.mt5 is not None and symbol:
                try:
                    info = self.mt5.get_symbol_info(symbol)
                except Exception:
                    info = None
            if info is not None:
                tick_size = self._safe_finite_float(
                    getattr(info, "trade_tick_size", None)
                )
                tick_value = self._safe_finite_float(
                    getattr(info, "trade_tick_value", None)
                )
                if tick_size and tick_size > 0.0 and tick_value and tick_value > 0.0:
                    price_pnl = (
                        ((stop_value - entry_value) * direction / tick_size)
                        * tick_value
                        * volume_value
                    )
                    path = "tick_value"

        resolved_spec = spec
        if price_pnl is None:
            if resolved_spec is None and symbol:
                try:
                    resolved_spec = get_instrument_spec(symbol)
                except Exception:
                    resolved_spec = None
            if resolved_spec is not None:
                pip_size = self._safe_finite_float(
                    getattr(resolved_spec, "pip_size", None)
                )
                pip_value = self._safe_finite_float(
                    getattr(resolved_spec, "pip_value", None)
                )
                if pip_size and pip_size > 0.0 and pip_value and pip_value > 0.0:
                    price_pnl = (
                        ((stop_value - entry_value) * direction / pip_size)
                        * pip_value
                        * volume_value
                    )
                    path = "pip_value"

        if price_pnl is None or not math.isfinite(price_pnl):
            return None, path
        exit_commission = self._remaining_exit_commission_amount(
            symbol,
            volume_value,
            spec=resolved_spec,
        )
        stop_pnl = price_pnl + swap_value - exit_commission
        return (float(stop_pnl), path) if math.isfinite(stop_pnl) else (None, path)

    def _estimate_stop_loss_amount(
        self,
        order_type: int,
        symbol: str,
        volume: float,
        entry_price: float,
        stop_price: float,
        *,
        spec: Optional[Any] = None,
        symbol_info: Optional[Any] = None,
        include_commission: bool = True,
    ) -> Tuple[Optional[float], Optional[str]]:
        """Estimate stop-loss amount using MT5 first, then deterministic fallbacks."""
        try:
            volume_value = float(volume)
            entry_value = float(entry_price)
            stop_value = float(stop_price)
        except (TypeError, ValueError):
            return None, None
        if volume_value <= 0.0 or entry_value <= 0.0 or stop_value <= 0.0:
            return None, None

        risk_amount: Optional[float] = None
        path: Optional[str] = None
        if self.mt5 is not None:
            risk_amount_raw = self.mt5.calc_loss_amount(
                order_type,
                symbol,
                volume_value,
                entry_value,
                stop_value,
            )
            try:
                risk_amount = float(risk_amount_raw)
            except (TypeError, ValueError):
                risk_amount = None
            if risk_amount is not None and risk_amount > 0.0:
                path = "mt5"

        if risk_amount is None or risk_amount <= 0.0:
            info = symbol_info
            if info is None and self.mt5 is not None and symbol:
                info = self.mt5.get_symbol_info(symbol)
            if info is not None:
                try:
                    tick_size = float(getattr(info, "trade_tick_size", 0.0) or 0.0)
                except (TypeError, ValueError):
                    tick_size = 0.0
                try:
                    tick_value = float(getattr(info, "trade_tick_value", 0.0) or 0.0)
                except (TypeError, ValueError):
                    tick_value = 0.0
                if tick_size > 0.0 and tick_value > 0.0:
                    ticks = abs(entry_value - stop_value) / tick_size
                    risk_amount = ticks * tick_value * volume_value
                    if risk_amount > 0.0:
                        path = "tick_value"

        if risk_amount is None or risk_amount <= 0.0:
            resolved_spec = spec
            if resolved_spec is None and symbol:
                try:
                    resolved_spec = get_instrument_spec(symbol)
                except Exception:
                    resolved_spec = None
            if resolved_spec is not None:
                try:
                    pip_size = float(getattr(resolved_spec, "pip_size", 0.0) or 0.0)
                except (TypeError, ValueError):
                    pip_size = 0.0
                try:
                    pip_value = float(getattr(resolved_spec, "pip_value", 0.0) or 0.0)
                except (TypeError, ValueError):
                    pip_value = 0.0
                if pip_size > 0.0 and pip_value > 0.0:
                    sl_pips = abs(entry_value - stop_value) / pip_size
                    risk_amount = sl_pips * pip_value * volume_value
                    if risk_amount > 0.0:
                        path = "pip_value"
                spec = resolved_spec

        if risk_amount is None or risk_amount <= 0.0:
            return None, path

        if include_commission:
            risk_amount += self._commission_amount_for_volume(
                symbol, volume_value, spec=spec
            )
        return float(risk_amount), path

    def _estimate_position_risk_pct(
        self,
        position: Any,
        *,
        account_info: Optional[Any] = None,
        canonical_symbol: str = "",
        broker_symbol: str = "",
        default_risk_pct: Optional[float] = None,
    ) -> Optional[float]:
        """
        Estimate the live risk percentage of an open position.

        Priority:
        1. MT5 contract-loss math from entry/SL/volume
        2. Tick-value fallback from broker symbol info
        3. Pip-value fallback from instrument spec
        4. Comment metadata risk tag
        5. Default fallback if explicitly supplied

        Observability: when steps 1-3 all return zero/None geometry AND step 4
        comment-decode also fails, the function logs WARNING before falling
        back to the default. This makes silent risk understatement visible
        (TECHNICAL_DIRECTION §1.5: risk is budgeted forward, comment tags are
        informational).
        """
        from pm_position import TradeTagEncoder

        account = account_info or (self.mt5.get_account_info() if self.mt5 else None)
        equity = (
            float(getattr(account, "equity", 0.0) or 0.0)
            if account is not None
            else 0.0
        )

        pos_symbol = broker_symbol or str(getattr(position, "symbol", "") or "")
        comment = getattr(position, "comment", "") or ""
        decoded = None
        comment_decode_error: Optional[str] = None
        if comment:
            try:
                decoded = TradeTagEncoder.decode_comment(comment)
            except Exception as exc:
                comment_decode_error = f"{type(exc).__name__}: {exc}"
                decoded = None
        comment_risk = None
        if decoded:
            try:
                if decoded.get("risk_pct") is not None:
                    comment_risk = float(decoded.get("risk_pct"))
            except (TypeError, ValueError):
                comment_risk = None
            if not canonical_symbol:
                canonical_symbol = str(decoded.get("symbol", "") or "")

        try:
            entry_price = float(getattr(position, "price_open", 0.0) or 0.0)
            stop_price = float(getattr(position, "sl", 0.0) or 0.0)
            volume = float(getattr(position, "volume", 0.0) or 0.0)
        except (TypeError, ValueError):
            entry_price = stop_price = volume = 0.0

        geometry_attempted = (
            equity > 0.0 and entry_price > 0.0 and stop_price > 0.0 and volume > 0.0
        )

        if geometry_attempted:
            pos_type = int(getattr(position, "type", 1) or 1)
            order_type = OrderType.BUY.value if pos_type == 0 else OrderType.SELL.value
            risk_amount, path = self._estimate_stop_loss_amount(
                order_type,
                pos_symbol or canonical_symbol,
                volume,
                entry_price,
                stop_price,
                spec=(
                    get_instrument_spec(canonical_symbol) if canonical_symbol else None
                ),
                symbol_info=(
                    self.mt5.get_symbol_info(pos_symbol)
                    if self.mt5 and pos_symbol
                    else None
                ),
                include_commission=True,
            )
            mt5_path_failed = path != "mt5"
            fallback_path: Optional[str] = path if path not in {None, "mt5"} else None

            # Surface MT5 contract-path failures once per (symbol, fallback).
            if mt5_path_failed and fallback_path is not None:
                dedup = getattr(self, "_mt5_risk_fallback_logged", None)
                if dedup is None:
                    dedup = set()
                    self._mt5_risk_fallback_logged = dedup
                dedup_key = f"{pos_symbol or canonical_symbol or '?'}:{fallback_path}"
                if dedup_key not in dedup:
                    dedup.add(dedup_key)
                    self.logger.warning(
                        f"[{pos_symbol or canonical_symbol or '?'}] MT5 calc_loss_amount "
                        f"returned None/0 — risk estimate using {fallback_path} fallback "
                        f"(less accurate for cross pairs / CFDs); subsequent occurrences "
                        f"on this symbol via this path will be silent"
                    )

            if risk_amount is not None and risk_amount > 0.0:
                return (float(risk_amount) / equity) * 100.0

        if comment_risk is not None:
            return comment_risk

        # Geometry path either was not attemptable (missing entry/SL/volume/equity)
        # or returned zero/None at every step; comment metadata also did not yield
        # a usable risk tag. Surface this loudly so operators see when the
        # secondary-trade risk check is leaning on a default rather than measured
        # exposure. Silent understatement is the failure mode this log prevents.
        ticket = getattr(position, "ticket", None)
        reason_bits: List[str] = []
        if not geometry_attempted:
            missing = []
            if equity <= 0.0:
                missing.append("equity")
            if entry_price <= 0.0:
                missing.append("entry_price")
            if stop_price <= 0.0:
                missing.append("stop_price")
            if volume <= 0.0:
                missing.append("volume")
            reason_bits.append(
                f"geometry_unavailable[{','.join(missing) or 'unknown'}]"
            )
        else:
            reason_bits.append("geometry_returned_zero")
        if comment_decode_error:
            reason_bits.append(f"comment_decode_error[{comment_decode_error}]")
        elif comment:
            reason_bits.append("comment_no_risk_tag")
        else:
            reason_bits.append("comment_empty")

        log_label = pos_symbol or canonical_symbol or "?"
        if default_risk_pct is not None:
            try:
                fallback_value = float(default_risk_pct)
            except (TypeError, ValueError):
                fallback_value = None
            if fallback_value is not None:
                self.logger.warning(
                    f"[{log_label}] Position risk estimate falling back to default "
                    f"{fallback_value:.3f}% (ticket={ticket}, reasons={'; '.join(reason_bits)})"
                )
                return fallback_value
        else:
            self.logger.warning(
                f"[{log_label}] Position risk estimate unavailable and no default supplied "
                f"(ticket={ticket}, reasons={'; '.join(reason_bits)})"
            )
        return None

    def _bound_capital_admission_envelope(self) -> Optional[Dict[str, float]]:
        """Return the signed/replay-selected seven-axis capital envelope that
        governs live admission for this execution book, or None if none is
        bound and validated.

        Static ``pipeline_config`` risk fields are NOT a production fallback:
        the selected envelope is the admission authority (seq1644/1684), so
        every admission gate must fail closed (block the entry) when this
        returns None.  The demo/current-refit book reads the envelope bound at
        construction; the funded book reads its own binding when present and
        otherwise fails closed until that binding exists.
        """
        if getattr(self, "_is_current_refit_demo", False):
            raw = getattr(self, "_current_refit_demo_capital_envelope", None)
        else:
            raw = getattr(self, "_funded_capital_envelope", None)
        required = (
            "max_total_stop_risk_pct",
            "max_factor_stop_risk_pct",
            "max_same_symbol_stop_risk_pct",
            "max_margin_utilization_pct",
            "min_margin_level_pct",
            "min_free_margin_pct",
            "gap_stress_multiple",
        )
        if not isinstance(raw, Mapping) or set(raw) != set(required):
            return None
        values: Dict[str, float] = {}
        for key in required:
            value = raw.get(key)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
                or float(value) <= 0.0
            ):
                return None
            values[key] = float(value)
        # Internal coherence defense (seq1720): the risk hierarchy, the
        # margin/free-margin reciprocals, and the gap-stress floor must hold, or
        # this is not a valid selected point and admission fails closed.
        total = values["max_total_stop_risk_pct"]
        factor = values["max_factor_stop_risk_pct"]
        same_symbol = values["max_same_symbol_stop_risk_pct"]
        margin = values["max_margin_utilization_pct"]
        level = values["min_margin_level_pct"]
        free = values["min_free_margin_pct"]
        if not 0.0 < same_symbol <= factor <= total < 100.0:
            return None
        if not 0.0 < margin <= 100.0:
            return None
        if not math.isclose(margin, 10_000.0 / level, rel_tol=0.0, abs_tol=1e-9):
            return None
        if not math.isclose(free, 100.0 - margin, rel_tol=0.0, abs_tol=1e-9):
            return None
        if values["gap_stress_multiple"] < 1.0:
            return None
        return values

    def _check_symbol_combined_risk_cap(
        self,
        symbol: str,
        new_trade_risk_pct: float,
        broker_symbol: str,
        positions_snapshot: Optional[List[Any]] = None,
        account_info: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """
        Enforce max_combined_risk_pct across all open positions on this symbol.
        This is a same-symbol combined exposure check, not a portfolio-wide cap.

        Args:
            symbol: Canonical symbol (e.g., "EURUSD")
            new_trade_risk_pct: Risk % for proposed new trade
            broker_symbol: Broker-specific symbol (e.g., "EURUSD.a")

        Returns:
            (can_trade: bool, reason: str)
        """
        from pm_position import TradeTagEncoder

        # Same-symbol combined cap comes from the signed selected capital
        # envelope, never static config; fail closed if no envelope is bound
        # (seq1644/1684; funded fail-closed confirmed seq1713).
        _cap_env = self._bound_capital_admission_envelope()
        if _cap_env is None:
            return False, "No bound capital envelope for same-symbol admission"
        max_combined = _cap_env["max_same_symbol_stop_risk_pct"]

        # Get current equity
        account_info = account_info or self.mt5.get_account_info()
        if not account_info:
            return False, "Cannot get account info"

        equity = account_info.equity
        if equity <= 0:
            return False, "Zero equity"

        # Sum existing risk for THIS SYMBOL across all timeframes
        existing_risk_pct = 0.0
        position_details = []

        # Use the provided sweep snapshot when available; otherwise fetch once.
        positions = positions_snapshot
        if positions is None:
            positions = self.mt5.get_positions(symbol=broker_symbol)
        if positions is None:
            return False, f"Position snapshot unavailable for {symbol}"

        for pos in positions:
            if getattr(pos, "symbol", None) != broker_symbol:
                continue

            comment = getattr(pos, "comment", "") or ""
            metadata = TradeTagEncoder.decode_comment(comment)
            if metadata and metadata.get("symbol") and metadata.get("symbol") != symbol:
                continue

            pos_risk_pct = self._estimate_position_risk_pct(
                pos,
                account_info=account_info,
                canonical_symbol=symbol,
                broker_symbol=broker_symbol,
                default_risk_pct=float(
                    getattr(self.position_config, "risk_per_trade_pct", 1.0)
                ),
            )
            if pos_risk_pct is None:
                continue

            pos_tf = metadata.get("timeframe", "?") if metadata else "?"
            pos_direction = (
                metadata.get(
                    "direction", "LONG" if getattr(pos, "type", 1) == 0 else "SHORT"
                )
                if metadata
                else ("LONG" if getattr(pos, "type", 1) == 0 else "SHORT")
            )

            existing_risk_pct += pos_risk_pct
            position_details.append(
                {
                    "timeframe": pos_tf,
                    "direction": pos_direction,
                    "risk_pct": pos_risk_pct,
                    "ticket": getattr(pos, "ticket", 0),
                }
            )

            self.logger.debug(
                f"[{symbol}] Open position: {pos_tf} {pos_direction} "
                f"risk={pos_risk_pct:.2f}% (ticket={getattr(pos, 'ticket', 0)})"
            )

        # Check if new trade would breach cap FOR THIS SYMBOL
        total_risk_pct = existing_risk_pct + new_trade_risk_pct

        if total_risk_pct > max_combined:
            details_str = ", ".join(
                f"{p['timeframe']}:{p['direction']}={p['risk_pct']:.2f}%"
                for p in position_details
            )

            return False, (
                f"Symbol risk cap exceeded for {symbol}: "
                f"existing {existing_risk_pct:.2f}% ({len(position_details)} positions: {details_str}) + "
                f"new {new_trade_risk_pct:.2f}% = {total_risk_pct:.2f}% > "
                f"max {max_combined:.2f}%"
            )

        return True, (
            f"Symbol risk OK for {symbol}: {total_risk_pct:.2f}% / {max_combined:.2f}% "
            f"({len(position_details)} open positions, adding 1 new)"
        )

    def _check_portfolio_risk_cap(
        self,
        symbol: str,
        new_trade_risk_pct: float,
        broker_symbol: str,
        positions_snapshot: Optional[List[Any]] = None,
        account_info: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """Backward-compatible alias for the same-symbol combined risk check."""
        return self._check_symbol_combined_risk_cap(
            symbol=symbol,
            new_trade_risk_pct=new_trade_risk_pct,
            broker_symbol=broker_symbol,
            positions_snapshot=positions_snapshot,
            account_info=account_info,
        )

    def _estimate_total_open_risk_pct(
        self,
        positions_snapshot: Optional[List[Any]],
        account_info: Optional[Any],
        *,
        default_risk_pct: Optional[float] = None,
    ) -> float:
        total = 0.0
        if not positions_snapshot:
            return total
        for pos in positions_snapshot:
            broker_symbol = str(getattr(pos, "symbol", "") or "")
            risk_pct = self._estimate_position_risk_pct(
                pos,
                account_info=account_info,
                broker_symbol=broker_symbol,
                default_risk_pct=default_risk_pct,
            )
            if risk_pct is not None and math.isfinite(float(risk_pct)):
                total += max(0.0, float(risk_pct))
        return total

    def _resolve_live_position_canonical_symbol(
        self,
        position: Any,
        *,
        candidate_symbol: str,
        candidate_broker_symbol: str,
    ) -> Tuple[Optional[str], str]:
        """Resolve one fresh broker position without guessing across symbols."""
        from pm_position import TradeTagEncoder
        from pm_symbol import normalize_symbol

        broker_symbol = str(getattr(position, "symbol", "") or "").strip()
        if not broker_symbol:
            return None, "open position has no broker symbol"
        if broker_symbol == str(candidate_broker_symbol or ""):
            return str(candidate_symbol).upper(), "candidate_broker_match"

        comment = str(getattr(position, "comment", "") or "")
        decoded = TradeTagEncoder.decode_comment(comment) if comment else None
        decoded_symbol = str((decoded or {}).get("symbol", "") or "").strip().upper()
        if decoded_symbol:
            try:
                resolved_broker = self.mt5.find_broker_symbol(decoded_symbol)
            except Exception:
                resolved_broker = None
            if resolved_broker == broker_symbol or (
                normalize_symbol(broker_symbol) == normalize_symbol(decoded_symbol)
            ):
                return decoded_symbol, "trade_tag"
            return None, (
                f"position tag symbol {decoded_symbol} does not match broker symbol "
                f"{broker_symbol}"
            )

        validated = self._get_management_configs()
        if validated:
            resolved = self._resolve_symbol_from_broker(broker_symbol, validated)
            if resolved:
                return str(resolved).upper(), "validated_broker_map"
        return (
            None,
            f"cannot resolve canonical symbol for broker position {broker_symbol}",
        )

    def _build_live_stop_risk_exposures(
        self,
        positions_snapshot: List[Any],
        account_info: Any,
        *,
        candidate_symbol: str,
        candidate_broker_symbol: str,
    ) -> Tuple[List[StopRiskExposure], Optional[float], str]:
        """Measure open SL loss against replay-equivalent conservative capital."""
        equity = self._safe_finite_float(getattr(account_info, "equity", None))
        balance = self._safe_finite_float(getattr(account_info, "balance", None))
        if equity is None or equity <= 0.0 or balance is None or balance <= 0.0:
            return (
                [],
                None,
                "fresh account balance/equity unavailable for gross stop-risk admission",
            )

        measured: List[Tuple[str, str, str, float, float, float]] = []
        for index, position in enumerate(list(positions_snapshot or [])):
            canonical, resolution = self._resolve_live_position_canonical_symbol(
                position,
                candidate_symbol=candidate_symbol,
                candidate_broker_symbol=candidate_broker_symbol,
            )
            if not canonical:
                return [], None, resolution
            broker_symbol = str(getattr(position, "symbol", "") or "").strip()
            try:
                entry_price = float(getattr(position, "price_open", 0.0) or 0.0)
                stop_price = float(getattr(position, "sl", 0.0) or 0.0)
                volume = float(getattr(position, "volume", 0.0) or 0.0)
                raw_position_type = getattr(position, "type", 1)
                position_type = int(
                    1 if raw_position_type is None else raw_position_type
                )
            except (TypeError, ValueError, OverflowError):
                return [], None, f"open position {index} has invalid SL geometry"
            if entry_price <= 0.0 or stop_price <= 0.0 or volume <= 0.0:
                return (
                    [],
                    None,
                    (
                        f"open position {getattr(position, 'ticket', index)} lacks "
                        "positive entry, stop or volume"
                    ),
                )
            order_type = (
                OrderType.BUY.value if position_type == 0 else OrderType.SELL.value
            )
            try:
                spec = get_instrument_spec(canonical)
            except Exception as exc:
                return [], None, f"instrument spec unavailable for {canonical}: {exc}"
            current_profit = self._safe_finite_float(getattr(position, "profit", None))
            accrued_swap = self._safe_finite_float(getattr(position, "swap", None))
            if current_profit is None or accrued_swap is None:
                return (
                    [],
                    None,
                    (
                        f"open position {getattr(position, 'ticket', index)} current MTM "
                        "profit/swap is unavailable"
                    ),
                )
            stop_pnl, _path = self._estimate_stop_pnl_amount(
                order_type,
                broker_symbol,
                volume,
                entry_price,
                stop_price,
                accrued_swap=accrued_swap,
                spec=spec,
                symbol_info=self.mt5.get_symbol_info(broker_symbol),
            )
            if stop_pnl is None or not math.isfinite(float(stop_pnl)):
                return (
                    [],
                    None,
                    (
                        f"open position {getattr(position, 'ticket', index)} signed stop P&L "
                        "cannot be measured"
                    ),
                )
            current_mtm = float(current_profit) + float(accrued_swap)
            gross_stop_loss = max(0.0, -float(stop_pnl))
            measured.append(
                (
                    canonical,
                    str(getattr(spec, "asset_class", "unknown") or "unknown"),
                    str(getattr(position, "ticket", index) or index),
                    float(stop_pnl),
                    current_mtm,
                    gross_stop_loss,
                )
            )

        portfolio_stop_equity = equity + sum(
            stop_pnl - current_mtm
            for _canonical, _asset_class, _exposure_id, stop_pnl, current_mtm, _gross_loss in measured
        )
        gross_open_stop_risk = sum(item[5] for item in measured)
        capital = conservative_risk_capital(
            balance=balance,
            equity=equity,
            portfolio_stop_equity=portfolio_stop_equity,
            open_stop_risk_amount=gross_open_stop_risk,
        )
        if not capital.allow:
            return [], None, capital.reason
        exposures: List[StopRiskExposure] = []
        for (
            canonical,
            asset_class,
            exposure_id,
            _stop_pnl,
            _current_mtm,
            risk_amount,
        ) in measured:
            if risk_amount <= 0.0:
                continue
            exposures.append(
                StopRiskExposure(
                    symbol=canonical,
                    stop_risk_pct=risk_amount / capital.risk_capital * 100.0,
                    asset_class=asset_class,
                    exposure_id=exposure_id,
                )
            )
        return exposures, capital.risk_capital, ""

    @staticmethod
    def _estimate_open_drawdown_pct(account_info: Optional[Any]) -> float:
        if account_info is None:
            return 0.0
        balance = (
            LiveTrader._safe_finite_float(
                getattr(account_info, "balance", 0.0), default=0.0
            )
            or 0.0
        )
        equity = (
            LiveTrader._safe_finite_float(
                getattr(account_info, "equity", balance), default=balance
            )
            or balance
        )
        if balance <= 0.0 or equity >= balance:
            return 0.0
        return (balance - equity) / balance * 100.0

    def _estimate_realized_loss_pct(
        self, period: str, account_info: Optional[Any]
    ) -> Optional[float]:
        """Estimate realized loss percent from recent broker closing deals."""
        if account_info is None:
            return None
        balance = (
            self._safe_finite_float(getattr(account_info, "balance", 0.0), default=0.0)
            or 0.0
        )
        if balance <= 0.0:
            return None

        normalized_period = str(period or "daily").strip().lower()
        now = datetime.now()
        if normalized_period == "weekly":
            boundary = now - timedelta(days=7)
            lookback_hours = 24 * 8
        elif normalized_period == "session":
            boundary = getattr(self, "_live_session_started_at", None) or now
            lookback_hours = max(
                1, int(math.ceil((now - boundary).total_seconds() / 3600.0)) + 1
            )
        else:
            boundary = datetime(now.year, now.month, now.day)
            lookback_hours = 48

        deals = list(getattr(self, "_recent_closing_deals", []) or [])
        cached_lookback_hours = int(
            getattr(self, "_recent_closing_deals_lookback_hours", 0) or 0
        )
        cache_available = bool(
            getattr(
                self,
                "_recent_closing_deals_available",
                # Compatibility for tests/embedded callers that pre-populate a
                # trustworthy cache without constructing LiveTrader normally.
                bool(deals) or cached_lookback_hours > 0,
            )
        )
        cache_too_short = (
            not cache_available
            or cached_lookback_hours <= 0
            or cached_lookback_hours < lookback_hours
        )
        if (
            self.mt5 is not None
            and hasattr(self.mt5, "get_recent_closing_deals")
            and (not deals or cache_too_short)
        ):
            try:
                broker_deals = self.mt5.get_recent_closing_deals(
                    lookback_hours=lookback_hours
                )
                if broker_deals is None:
                    self._recent_closing_deals_available = False
                    self._recent_closing_deals_lookback_hours = 0
                    return None
                deals = list(broker_deals)
                self._recent_closing_deals = list(deals)
                self._recent_closing_deals_lookback_hours = lookback_hours
                self._recent_closing_deals_available = True
            except Exception as exc:
                self._recent_closing_deals_available = False
                self._recent_closing_deals_lookback_hours = 0
                self.logger.warning(
                    f"Realized loss snapshot unavailable for {normalized_period}: {exc}"
                )
                return None
        elif not cache_available:
            return None
        if not deals:
            return 0.0

        realized = 0.0
        for deal in deals:
            deal_time = deal.get("time")
            if deal_time is None:
                continue
            try:
                deal_timestamp = pd.Timestamp(deal_time)
                if deal_timestamp.tzinfo is not None:
                    local_tz = datetime.now().astimezone().tzinfo
                    deal_timestamp = deal_timestamp.tz_convert(local_tz).tz_localize(
                        None
                    )
                timestamp = deal_timestamp.to_pydatetime()
            except Exception:
                continue
            if timestamp >= boundary:
                net_cash = 0.0
                for field_name in ("profit", "commission", "swap", "fee"):
                    component = self._safe_finite_float(deal.get(field_name, 0.0))
                    if component is None:
                        self.logger.warning(
                            "Realized loss snapshot contains invalid %s for deal %s",
                            field_name,
                            deal.get("ticket", "<unknown>"),
                        )
                        return None
                    net_cash += component
                realized += net_cash
        return max(0.0, (-realized / balance) * 100.0)

    @staticmethod
    def _safe_finite_float(
        value: Any, default: Optional[float] = None
    ) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return default
        return result if math.isfinite(result) else default

    @staticmethod
    def _account_margin_used(account_info: Any) -> bool:
        margin = LiveTrader._safe_finite_float(
            getattr(account_info, "margin", 0.0), default=0.0
        )
        return bool(margin is not None and margin > 0.0)

    def _manual_new_entries_block_reason(self) -> Optional[str]:
        """Return an operator-facing reason when manual controls pause new entries."""
        if not bool(getattr(self.pipeline_config, "manual_new_entries_enabled", True)):
            return "manual_new_entries_enabled=false"
        path_raw = str(
            getattr(self.pipeline_config, "manual_kill_switch_path", "") or ""
        ).strip()
        if not path_raw:
            return None
        path = Path(path_raw)
        if not path.is_absolute():
            source_config = str(
                getattr(self.pipeline_config, "_deployment_config_path", "") or ""
            ).strip()
            base = Path(source_config).resolve().parent if source_config else Path.cwd()
            path = (base / path).resolve()
        if not path.exists():
            return None
        try:
            content = path.read_text(encoding="utf-8").strip().lower()
        except Exception as exc:
            return f"manual kill switch unreadable at {path}: {exc}"
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
            return f"manual kill switch active at {path}"
        return None

    @staticmethod
    def _safe_account_margin_level(account_info: Any) -> Optional[float]:
        return LiveTrader._safe_finite_float(
            getattr(account_info, "margin_level", None)
        )

    @staticmethod
    def _margin_utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_margin_timestamp(value: Any, field_name: str) -> datetime:
        if isinstance(value, datetime):
            parsed = value
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                # Backward-compatible in-memory callers historically used
                # datetime.now(). Durable JSON timestamps remain strictly
                # offset-aware below.
                return parsed.astimezone(timezone.utc)
        else:
            text = str(value or "").strip()
            if not text:
                raise ValueError(f"{field_name} is missing")
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field_name} must be offset-aware")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _margin_state_digest(payload: Dict[str, Any]) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _margin_state_payload(self) -> Dict[str, Any]:
        last_close = getattr(self, "_last_margin_close_ts", None)
        if last_close is not None:
            last_close = self._normalize_margin_timestamp(
                last_close,
                "last_forced_close_utc",
            )
        payload: Dict[str, Any] = {
            "schema_version": MARGIN_PROTECTION_STATE_SCHEMA_VERSION,
            "execution_book": self.execution_book,
            "margin_reopen_required": self._is_margin_reopen_required(),
            "last_forced_close_utc": (
                last_close.isoformat().replace("+00:00", "Z")
                if last_close is not None
                else None
            ),
            "updated_at_utc": self._margin_utc_now().isoformat().replace("+00:00", "Z"),
        }
        payload["content_sha256"] = self._margin_state_digest(payload)
        return payload

    def _persist_margin_protection_state(self) -> bool:
        if not bool(getattr(self, "_margin_state_persistence_enabled", False)):
            return True
        path = Path(self._margin_protection_state_path)
        temp_path: Optional[Path] = None
        try:
            with self._margin_state_persistence_lock:
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = self._margin_state_payload()
                temp_path = path.with_name(
                    f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
                )
                serialized = json.dumps(
                    payload,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
                with temp_path.open("x", encoding="utf-8", newline="\n") as handle:
                    handle.write(serialized)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_path, path)
                temp_path = None
                self._margin_state_persistence_error = ""
            return True
        except Exception as exc:
            self._margin_state_persistence_error = (
                f"margin protection state save failed: {type(exc).__name__}: {exc}"
            )
            self.logger.error(self._margin_state_persistence_error)
            return False
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def _fail_closed_margin_protection_state(self, reason: str) -> None:
        self._margin_state = "BLOCKED"
        self._margin_reopen_required = True
        # A missing/corrupt state could be the only remaining record of a
        # just-completed forced close. Restart the entire cooldown rather than
        # allowing deletion or truncation to reopen exposure.
        self._last_margin_close_ts = self._margin_utc_now()
        self._margin_state_persistence_error = str(reason or "unknown state error")
        self.logger.error(
            "MARGIN PROTECTION STATE FAIL-CLOSED: %s; "
            "reopen latch armed and cooldown restarted",
            self._margin_state_persistence_error,
        )
        self._persist_margin_protection_state()

    def _restore_margin_protection_state(self) -> None:
        if not bool(getattr(self, "_margin_state_persistence_enabled", False)):
            return
        path = Path(self._margin_protection_state_path)
        if not path.exists():
            self._fail_closed_margin_protection_state(
                f"margin protection state missing at {path}"
            )
            return
        try:
            if not path.is_file() or path.is_symlink():
                raise ValueError("state path is not a regular non-symlink file")
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("state payload is not an object")
            expected_fields = {
                "schema_version",
                "execution_book",
                "margin_reopen_required",
                "last_forced_close_utc",
                "updated_at_utc",
                "content_sha256",
            }
            if set(payload) != expected_fields:
                raise ValueError("state fields do not match the durable contract")
            if payload.get("schema_version") != MARGIN_PROTECTION_STATE_SCHEMA_VERSION:
                raise ValueError("state schema is unsupported")
            if str(payload.get("execution_book") or "") != self.execution_book:
                raise ValueError("state execution book does not match this runtime")
            reopen_required = payload.get("margin_reopen_required")
            if not isinstance(reopen_required, bool):
                raise ValueError("margin_reopen_required is not boolean")
            declared_digest = str(payload.get("content_sha256") or "").lower()
            unsigned = dict(payload)
            unsigned.pop("content_sha256", None)
            if len(
                declared_digest
            ) != 64 or declared_digest != self._margin_state_digest(unsigned):
                raise ValueError("state content digest is invalid")
            updated_at = self._normalize_margin_timestamp(
                payload.get("updated_at_utc"),
                "updated_at_utc",
            )
            now = self._margin_utc_now()
            if updated_at > now + timedelta(minutes=5):
                raise ValueError("state update timestamp is implausibly in the future")
            last_close_raw = payload.get("last_forced_close_utc")
            last_close = None
            if last_close_raw is not None:
                last_close = self._normalize_margin_timestamp(
                    last_close_raw,
                    "last_forced_close_utc",
                )
                if last_close > now + timedelta(minutes=5):
                    raise ValueError(
                        "last forced-close timestamp is implausibly in the future"
                    )
                if last_close > updated_at + timedelta(seconds=1):
                    raise ValueError("last forced close occurs after the state update")
            self._margin_reopen_required = reopen_required
            self._last_margin_close_ts = last_close
            self._margin_state = "BLOCKED" if reopen_required else "NORMAL"
            self._margin_state_persistence_error = ""
            self.logger.info(
                "Restored margin protection state from %s "
                "(reopen_required=%s, last_forced_close_utc=%s)",
                path,
                reopen_required,
                (
                    last_close.isoformat().replace("+00:00", "Z")
                    if last_close is not None
                    else "none"
                ),
            )
        except Exception as exc:
            self._fail_closed_margin_protection_state(
                f"margin protection state load failed: {type(exc).__name__}: {exc}"
            )

    def _arm_margin_reopen_required(
        self,
        *,
        last_forced_close: Optional[datetime] = None,
    ) -> bool:
        changed = not self._is_margin_reopen_required()
        if last_forced_close is not None:
            normalized = self._normalize_margin_timestamp(
                last_forced_close,
                "last_forced_close_utc",
            )
            changed = changed or normalized != self._last_margin_close_ts
            self._last_margin_close_ts = normalized
        self._margin_reopen_required = True
        if changed:
            return self._persist_margin_protection_state()
        return self._margin_state_persistence_block_reason() is None

    def _margin_cooldown_remaining_minutes(
        self,
        *,
        now: Optional[datetime] = None,
    ) -> float:
        try:
            cooldown_minutes = float(
                getattr(
                    self.pipeline_config,
                    "margin_reopen_cooldown_minutes",
                    15.0,
                )
                or 0.0
            )
        except (TypeError, ValueError, OverflowError):
            cooldown_minutes = 15.0
        if not math.isfinite(cooldown_minutes) or cooldown_minutes <= 0.0:
            return 0.0
        last_close = getattr(self, "_last_margin_close_ts", None)
        if last_close is None:
            return 0.0
        try:
            normalized_last = self._normalize_margin_timestamp(
                last_close,
                "last_forced_close_utc",
            )
            normalized_now = self._normalize_margin_timestamp(
                now or self._margin_utc_now(),
                "margin_cooldown_now",
            )
        except (TypeError, ValueError, OverflowError):
            # An in-memory timestamp failure cannot be interpreted as an
            # elapsed cooldown. Hold the full configured interval.
            return cooldown_minutes
        elapsed = max(
            0.0,
            (normalized_now - normalized_last).total_seconds() / 60.0,
        )
        return max(0.0, cooldown_minutes - elapsed)

    def _margin_state_persistence_block_reason(self) -> Optional[str]:
        error = str(getattr(self, "_margin_state_persistence_error", "") or "")
        if bool(getattr(self, "_margin_state_persistence_enabled", False)) and error:
            return error
        return None

    def _classify_margin_state(self, margin_level: float) -> str:
        """Classify account stress state from MT5-native margin level."""
        level = self._safe_finite_float(margin_level)
        if level is None:
            return "BLOCKED"

        # The entry/block threshold is the signed selected envelope's
        # min_margin_level_pct (seq1720).  Recovery/panic remain explicit
        # operational overlays so stress management keeps working even with no
        # bound envelope; with no envelope the entry threshold is unreachable so
        # a healthy account is never NORMAL (entry fails closed) while still
        # being classifiable into RECOVERY/PANIC.
        _cap_env = self._bound_capital_admission_envelope()
        block_level = (
            float(_cap_env["min_margin_level_pct"])
            if _cap_env is not None
            else float("inf")
        )
        recovery_level = float(
            getattr(self.pipeline_config, "margin_recovery_start_level", 80.0)
        )
        panic_level = float(getattr(self.pipeline_config, "margin_panic_level", 65.0))

        if level >= block_level:
            return "NORMAL"
        if level >= recovery_level:
            return "BLOCKED"
        if level >= panic_level:
            return "RECOVERY"
        return "PANIC"

    def _is_margin_reopen_required(self) -> bool:
        return bool(getattr(self, "_margin_reopen_required", False))

    def _clear_margin_reopen_if_recovered(self, margin_level: Optional[float]) -> None:
        margin_level = self._safe_finite_float(margin_level)
        if margin_level is None:
            return
        reopen_level = float(
            getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0
        )
        if margin_level >= reopen_level and self._is_margin_reopen_required():
            self._margin_reopen_required = False
            self._persist_margin_protection_state()

    def _run_margin_protection_cycle(
        self,
        account_info: Optional[Any] = None,
        positions_snapshot: Optional[List[Any]] = None,
    ) -> bool:
        """Run one margin-protection pass and return True when positions changed."""

        def _set_state(new_state: str, margin_level: Optional[float]) -> None:
            previous = getattr(self, "_margin_state", "NORMAL")
            if previous != new_state:
                level_text = (
                    "unavailable" if margin_level is None else f"{margin_level:.1f}%"
                )
                self.logger.warning(
                    f"MARGIN STATE CHANGE: {previous} -> {new_state} (margin_level={level_text})"
                )
            self._margin_state = new_state

        account = (
            account_info
            if account_info is not None
            else (self.mt5.get_account_info() if self.mt5 else None)
        )
        if account is None:
            _set_state("BLOCKED", None)
            return False

        margin_level = self._safe_account_margin_level(account)
        margin_used = self._account_margin_used(account)
        if margin_level is None:
            _set_state("BLOCKED" if margin_used else "NORMAL", None)
            return False
        if margin_level <= 0:
            _set_state("BLOCKED" if margin_used else "NORMAL", margin_level)
            return False

        state = self._classify_margin_state(margin_level)
        if state != "NORMAL":
            self._arm_margin_reopen_required()
        elif self._is_margin_reopen_required():
            reopen_level = float(
                getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0
            )
            if margin_level < reopen_level:
                _set_state("BLOCKED", margin_level)
                return False
            self._clear_margin_reopen_if_recovered(margin_level)
        _set_state(state, margin_level)
        if state in {"NORMAL", "BLOCKED"}:
            return False

        positions = (
            positions_snapshot
            if positions_snapshot is not None
            else (self.mt5.get_positions() if self.mt5 else None)
        )
        positions = list(positions or [])
        if not positions:
            return False

        recovery_limit = max(
            1, int(getattr(self.pipeline_config, "margin_recovery_closes_per_cycle", 1))
        )
        panic_limit = max(
            1, int(getattr(self.pipeline_config, "margin_panic_closes_per_cycle", 3))
        )

        if state == "RECOVERY":
            candidates = sorted(
                [
                    pos
                    for pos in positions
                    if float(getattr(pos, "profit", 0.0) or 0.0) < 0.0
                ],
                key=lambda pos: float(getattr(pos, "profit", 0.0) or 0.0),
            )
            max_attempts = recovery_limit
        else:
            losers = sorted(
                [
                    pos
                    for pos in positions
                    if float(getattr(pos, "profit", 0.0) or 0.0) < 0.0
                ],
                key=lambda pos: float(getattr(pos, "profit", 0.0) or 0.0),
            )
            candidates = losers or sorted(
                positions,
                key=lambda pos: float(getattr(pos, "volume", 0.0) or 0.0),
                reverse=True,
            )
            max_attempts = panic_limit

        attempts = 0
        positions_changed = False
        for pos in candidates:
            if attempts >= max_attempts:
                break
            attempts += 1

            # Durably arm both recovery hysteresis and the complete cooldown
            # before asking the broker to close. If the process terminates
            # after MT5 accepts the close but before the post-success update,
            # restart still observes this conservative precommit. A failed
            # close may therefore impose an unnecessary cooldown; that is the
            # safe side of the ambiguity.
            precommit_at = self._margin_utc_now()
            if not self._arm_margin_reopen_required(
                last_forced_close=precommit_at,
            ):
                self.logger.error(
                    "MARGIN CLOSE DEFERRED: durable cooldown precommit failed; "
                    "new entries remain blocked"
                )
                break
            result = self.mt5.close_position(pos)
            if not getattr(result, "success", False):
                self.logger.warning(
                    f"MARGIN CLOSE FAILED: ticket={getattr(pos, 'ticket', 0)} "
                    f"symbol={getattr(pos, 'symbol', '')} reason={getattr(result, 'retcode_description', '')}"
                )
            else:
                # Arm the re-entry cooldown on every successful forced close so
                # a margin cycle cannot immediately re-open the same exposure
                # once margin_level bounces above the block threshold
                # Use a short re-open buffer after forced margin closures.
                self._arm_margin_reopen_required(
                    last_forced_close=self._margin_utc_now(),
                )
                positions_changed = True

            account = self.mt5.get_account_info() if self.mt5 else None
            if account is None:
                _set_state("BLOCKED", None)
                break

            margin_level = self._safe_account_margin_level(account)
            margin_used = self._account_margin_used(account)
            if margin_level is None:
                _set_state("BLOCKED" if margin_used else "NORMAL", None)
                break
            if margin_level <= 0:
                _set_state("BLOCKED" if margin_used else "NORMAL", margin_level)
                break

            state = self._classify_margin_state(margin_level)
            if state != "NORMAL":
                self._arm_margin_reopen_required()
            elif self._is_margin_reopen_required():
                reopen_level = float(
                    getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0
                )
                if margin_level < reopen_level:
                    _set_state("BLOCKED", margin_level)
                    break
                self._clear_margin_reopen_if_recovered(margin_level)
            _set_state(state, margin_level)
            if state in {"NORMAL", "BLOCKED"}:
                break
        return positions_changed

    def stop(self):
        """Stop the trading loop."""
        self.logger.info("Stopping trader...")
        self._running = False
        self._shutdown_event.set()

    def _process_symbol(
        self,
        symbol: str,
        config: SymbolConfig,
        positions_snapshot: Optional[List[Any]] = None,
        account_info: Optional[Any] = None,
        timeframes_filter: Optional[Set[str]] = None,
    ):
        """
        Process a single symbol with regime-aware strategy selection.

        For each timeframe:
        1. Check for new bar
        2. Compute features (including regime)
        3. Select best (tf, regime, strategy) based on strength * quality_score * freshness
        4. Generate signal and execute if conditions met

        Every distinct symbol/timeframe/regime specialist may hold one position
        on a hedging account. Aggregate exposure is enforced by the signed
        Capital envelope at the immediate pre-send boundary.

        Decision throttle: each (symbol, strategy, tf, regime, direction, bar)
        combination is attempted at most once.  Skips/failures are cached and
        suppressed until a new bar arrives or the decision key changes.
        """
        from pm_position import TradeTagEncoder

        # Find broker symbol
        broker_symbol = self.mt5.find_broker_symbol(symbol)
        if broker_symbol is None:
            return

        # Use the sweep snapshot if provided; otherwise fetch once and fail closed.
        all_positions = positions_snapshot
        if all_positions is None:
            all_positions = self.mt5.get_positions()
        if all_positions is None:
            self.logger.warning(
                f"[{symbol}] Position snapshot unavailable; skipping this iteration."
            )
            return

        if account_info is None:
            account_info = self.mt5.get_account_info()
        if account_info is None:
            self.logger.warning(
                f"[{symbol}] Account snapshot unavailable; skipping this iteration."
            )
            return
        if bool(getattr(self, "_is_current_refit_demo", False)):
            demo_mode_reason = self._current_refit_demo_account_block_reason(
                account_info
            )
            if demo_mode_reason:
                self.logger.error(
                    f"[{symbol}] Current-refit demo blocked: {demo_mode_reason}"
                )
                return
        if not getattr(account_info, "trade_allowed", True) or not getattr(
            account_info, "trade_expert", True
        ):
            self.logger.warning(
                f"[{symbol}] Account is not tradable; skipping this iteration."
            )
            return

        # Analyze open positions for the D1 + lower-TF pairing rules.
        symbol_positions = [
            p for p in all_positions if getattr(p, "symbol", None) == broker_symbol
        ]
        magic_to_tf, magic_to_regime = self._build_magic_lookup(symbol, config)
        open_magics = {int(getattr(p, "magic", 0) or 0) for p in symbol_positions}

        # Retained as a compatibility input only; it has no topology authority.
        allow_d1_plus_lower = (
            getattr(self.pipeline_config, "allow_d1_plus_lower_tf", False)
            if self.pipeline_config
            else False
        )

        # Analyze existing positions
        has_d1_position = False
        has_non_d1_position = False
        has_unknown_position = False
        open_timeframes = set()
        tag_source_counts = {"comment": 0, "magic": 0, "unknown": 0}
        position_details = []

        for pos in symbol_positions:
            # Unified timeframe inference with full priority chain
            pos_tf = self._infer_position_timeframe(symbol, config, pos)
            pos_regime = ""

            # Determine how the timeframe was resolved (for diagnostics)
            comment = getattr(pos, "comment", "") or ""
            pos_magic = int(getattr(pos, "magic", 0) or 0)
            if pos_tf:
                # Classify tag source for diagnostics
                decoded = TradeTagEncoder.decode_comment(comment) if comment else None
                if decoded and decoded.get("timeframe"):
                    tag_source = "comment"
                elif pos_magic in magic_to_tf:
                    tag_source = "magic"
                else:
                    tag_source = "recovered"  # override, legacy tag, or history
            else:
                tag_source = "unknown"
                has_unknown_position = True
                # Throttled warning: log once per ticket per session
                pos_ticket = int(getattr(pos, "ticket", 0) or 0)
                if pos_ticket and pos_ticket not in self._unknown_tf_position_warnings:
                    self._unknown_tf_position_warnings.add(pos_ticket)
                    self.logger.warning(
                        f"[{symbol}] Position ticket={pos_ticket} magic={pos_magic} "
                        f"comment={comment!r}: timeframe unknown; blocking secondary trade"
                    )

            if pos_tf:
                pos_regime = magic_to_regime.get(pos_magic, "")
                open_timeframes.add(pos_tf)

            if pos_tf == "D1":
                has_d1_position = True
            elif pos_tf:
                has_non_d1_position = True
            else:
                # Unknown trade tag - assume it's a non-D1 trade for safety
                has_non_d1_position = True

            tag_source_counts[tag_source] = tag_source_counts.get(tag_source, 0) + 1
            position_details.append(
                {
                    "magic": pos_magic,
                    "timeframe": pos_tf or "",
                    "regime": pos_regime or "",
                    "tag_source": tag_source,
                    "direction": "LONG" if getattr(pos, "type", 1) == 0 else "SHORT",
                }
            )

        # Determine if we can open a new trade and what constraints apply
        can_open_trade = True
        is_secondary_trade = False
        block_reason = None

        num_positions = len(symbol_positions)
        position_context = {
            "open_positions_total": num_positions,
            "open_timeframes": sorted(open_timeframes),
            "open_has_d1": has_d1_position,
            "open_has_non_d1": has_non_d1_position,
            "open_has_unknown": has_unknown_position,
            "open_tag_sources": tag_source_counts,
            "open_positions": position_details,
        }
        secondary_reason = ""

        if has_unknown_position:
            can_open_trade = False
            block_reason = "Position timeframe unknown; blocking additional specialist"
        elif num_positions > 0:
            is_secondary_trade = True
            secondary_reason = "additional_specialist_slot"

        # If we can't open any trade, skip
        if not can_open_trade:
            if block_reason:
                self.logger.debug(f"[{symbol}] {block_reason}")
            pending_scope = (
                set(timeframes_filter or set())
                if timeframes_filter is not None
                else set(
                    getattr(self, "_pending_specialist_timeframes", {}).get(
                        symbol, set()
                    )
                )
            )
            self._consume_specialist_timeframes(symbol, pending_scope)
            return

        # Evaluate candidates across timeframes (winners-only)
        candidates, eval_stats = self._evaluate_regime_candidates(
            symbol,
            broker_symbol,
            config,
            timeframes_filter=timeframes_filter,
        )
        pending_scope = (
            set(timeframes_filter or set())
            if timeframes_filter is not None
            else set(
                getattr(self, "_pending_specialist_timeframes", {}).get(symbol, set())
            )
        )
        self._consume_specialist_timeframes(symbol, pending_scope)

        if not candidates:
            if eval_stats:
                self.logger.debug(
                    f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
                    f"{eval_stats.get('timeframes_total', 0)} | winners=0 | actionable=0 "
                    f"(no_winner={eval_stats.get('no_winner', 0)}, "
                    f"failed_gate={eval_stats.get('winner_failed_gate', 0)}, "
                    f"missing_regime_live={eval_stats.get('missing_regime_live', 0)}, "
                    f"insufficient_bars={eval_stats.get('insufficient_bars', 0)}, "
                    f"stale_skipped={eval_stats.get('timeframes_skipped_stale', 0)})"
                )
            return

        if open_magics:
            original_count = len(candidates)
            candidates = [
                c
                for c in candidates
                if self._encode_specialist_magic(symbol, c["timeframe"], c["regime"])
                not in open_magics
            ]
            if len(candidates) < original_count:
                self.logger.debug(
                    f"[{symbol}] Filtered out candidates matching existing open position magic"
                )

        if not candidates:
            self.logger.debug(
                f"[{symbol}] No valid candidates after secondary/open-position constraints"
            )
            return

        # Select among independently actionable cells. A stronger non-signalling
        # cell must not suppress a signal from another timeframe/regime.

        #

        # A non-signalling leader is irrelevant to the current admission
        # decision. Portfolio/risk constraints still arbitrate simultaneous
        # signals after the independent-cell filter.

        best_overall = max(candidates, key=self._candidate_rank_key)

        best_overall_score = float(best_overall.get("selection_score", 0.0))

        # actionable_score_margin only makes sense as a quality
        # floor when the best score is positive. If the leader scores ≤ 0 the
        # multiplicative threshold collapses (or inverts), gate immediately so
        # we never trade against a non-positive selection score.
        if best_overall_score <= 0.0:
            bar_time_iso = str(best_overall.get("bar_time", ""))
            self._log_no_actionable_signal(
                symbol=symbol,
                message=(
                    f"NO_ACTIONABLE_BEST_SCORE_NONPOSITIVE "
                    f"(best_overall_score={best_overall_score:.4f} ≤ 0)"
                ),
                best_candidate=best_overall,
                bar_time_iso=bar_time_iso,
                action_type="NO_ACTIONABLE_BEST_SCORE_NONPOSITIVE",
            )
            return

        actionable = [c for c in candidates if int(c.get("signal", 0)) != 0]
        self.logger.debug(
            f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
            f"{eval_stats.get('timeframes_total', 0)} | winners={len(candidates)} | "
            f"actionable={len(actionable)} | "
            f"missing_regime_live={eval_stats.get('missing_regime_live', 0)} | "
            f"stale_skipped={eval_stats.get('timeframes_skipped_stale', 0)}"
        )

        if not actionable:
            # No actionable winner signals on any timeframe for this bar.
            # Use helper to prevent duplicate log spam within same bar.

            bar_time_iso = str(best_overall.get("bar_time", ""))

            self._log_no_actionable_signal(
                symbol=symbol,
                message="NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)",
                best_candidate=best_overall,
                bar_time_iso=bar_time_iso,
                action_type="NO_ACTIONABLE_WINNER_SIGNAL",
            )

            return

        min_score = 0.0

        eligible = [
            c for c in actionable if float(c.get("selection_score", 0.0)) > min_score
        ]

        if not eligible:
            # There were signals, but none retained a positive selection score.
            # Use helper to prevent duplicate log spam within same bar.

            bar_time_iso = str(best_overall.get("bar_time", ""))

            self._log_no_actionable_signal(
                symbol=symbol,
                message="NO_ACTIONABLE_SIGNAL_WITH_POSITIVE_SCORE",
                best_candidate=best_overall,
                bar_time_iso=bar_time_iso,
                action_type="NO_ACTIONABLE_SIGNAL_WITH_POSITIVE_SCORE",
            )

            return

        # Serialize every independently actionable cell across fresh broker
        # frames.  No specialist is dropped; each later sweep re-runs exact
        # duplicate, signed exposure, and margin admission against new state.
        admission_plan = self._plan_symbol_specialist_admissions(
            eligible,
            allow_d1_plus_lower=bool(allow_d1_plus_lower),
            open_position_count=num_positions,
        )
        best = admission_plan[0]
        for deferred in admission_plan[1:]:
            deferred_tf = str(deferred.get("timeframe", "") or "")
            self._last_bar_times.pop(f"{symbol}_{deferred_tf}", None)
            self._queue_specialist_timeframe(symbol, deferred_tf)
            self.logger.info(
                f"[{symbol}] Queued independent specialist for next sweep: "
                f"{deferred.get('strategy_name', 'UNKNOWN')} @ "
                f"{deferred_tf}/{deferred.get('regime', 'UNKNOWN')}"
            )
        # ── Throttle check ──────────────────────────────────────────────
        # Build a decision key from the candidate's identifying attributes
        # so that the *same* signal on the *same* bar is only acted on once.
        bar_time_iso = str(best.get("bar_time", ""))
        decision_key = DecisionThrottle.make_decision_key(
            symbol,
            best["strategy_name"],
            best["timeframe"],
            best["regime"],
            int(best["signal"]),
            bar_time_iso,
        )

        if self._decision_throttle.should_suppress(symbol, decision_key, bar_time_iso):
            # Already handled this exact decision on this bar – stay quiet
            return
        # ────────────────────────────────────────────────────────────────

        # Get symbol info
        symbol_info = self.mt5.get_symbol_info(broker_symbol)
        if symbol_info is None:
            return
        spec = self._symbol_info_to_instrument_spec(symbol, symbol_info)

        # Create magic number for this specific trade
        # Include timeframe and regime so we can identify the config used
        magic = self._encode_specialist_magic(symbol, best["timeframe"], best["regime"])
        signal_bar_index = (
            max(len(best["features"]) - 2, 0)
            if len(best["features"]) > 1
            else max(len(best["features"]) - 1, 0)
        )
        trade_intent = best["strategy"].build_trade_intent(
            best["features"],
            symbol=symbol,
            timeframe=best["timeframe"],
            regime=best["regime"],
            signal=int(best["signal"]),
            spec=spec,
            bar_index=signal_bar_index,
            signal_strength=float(best.get("regime_strength", 0.0) or 0.0),
            selection_score=float(best.get("selection_score", 0.0) or 0.0),
            quality_score=float(best.get("quality_score", 0.0) or 0.0),
            governance_hint=dict(
                getattr(best.get("regime_config"), "governance_policy", {}) or {}
            ),
            metadata={
                "bar_time": bar_time_iso,
                "freshness": float(best.get("freshness", 0.0) or 0.0),
            },
        )

        # Mark secondary trades clearly in the live log.
        trade_type_tag = "[SECONDARY] " if is_secondary_trade else ""

        # Log selection
        self.logger.info(
            f"[{symbol}] {trade_type_tag}Selected: {best['strategy_name']} @ {best['timeframe']}/{best['regime']} "
            f"(strength={best['regime_strength']:.2f}, quality={best['quality_score']:.2f}, "
            f"freshness={best['freshness']:.2f}, score={best['selection_score']:.3f})"
        )

        if is_secondary_trade:
            self.logger.debug(
                f"[{symbol}] D1 trade open; allowed second trade on {best['timeframe']}"
            )

        if self.close_on_opposite_signal and symbol_positions:
            opposite_positions = []
            best_signal = int(best.get("signal", 0))
            for pos in symbol_positions:
                if best_signal == 1 and getattr(pos, "type", 1) == 1:
                    opposite_positions.append(pos)
                elif best_signal == -1 and getattr(pos, "type", 1) == 0:
                    opposite_positions.append(pos)

            if opposite_positions:
                self.logger.info(
                    f"[{symbol}] Closing {len(opposite_positions)} opposite-signal position(s) "
                    f"before new entry on {best['timeframe']}/{best['regime']}"
                )
                for pos in opposite_positions:
                    self._close_position_on_signal(pos, best["features"], spec)
                return

        # Execute entry (passes decision_key so _execute_entry can record
        # the outcome into the throttle)
        self._execute_entry(
            symbol=symbol,
            broker_symbol=broker_symbol,
            signal=int(best["signal"]),
            strategy=best["strategy"],
            trade_intent=trade_intent,
            features=best["features"],
            spec=spec,
            magic=magic,
            config=config,
            decision_key=decision_key,
            bar_time_iso=bar_time_iso,
            best_candidate=best,
            is_secondary_trade=is_secondary_trade,
            position_context=position_context,
            secondary_reason=secondary_reason,
            positions_snapshot=all_positions,
            account_info=account_info,
        )

    def _evaluate_regime_candidates(
        self,
        symbol: str,
        broker_symbol: str,
        config: SymbolConfig,
        timeframes_filter: Optional[Set[str]] = None,
    ) -> tuple:
        """
        Evaluate all (timeframe, regime) candidates for entry.

        Winners-only policy (production correctness):
        - Tiered fallbacks are NOT allowed in live trading.
        - Only a validated winner for the exact (timeframe, regime) may trade.
        - If no winner exists for that (tf, regime), skip that timeframe.
        - If a winner exists but fails the live gate, skip that timeframe.

        Performance optimization: caches features/signals per (symbol, tf, bar_time)
        to avoid recomputing when no new bar has arrived.

        Returns (candidates, stats).
        """
        from pm_strategies import StrategyRegistry

        candidates: List[Dict[str, Any]] = []
        stats = {
            "timeframes_total": 0,
            "timeframes_evaluated": 0,
            "timeframes_skipped_stale": 0,
            "winner_candidates": 0,
            "no_winner": 0,
            "winner_failed_gate": 0,
            "insufficient_bars": 0,
            "missing_regime_live": 0,
            "missing_tuned_regime_params": 0,
        }

        # Check if config has regime configs
        if not config.has_regime_configs():
            if bool(getattr(self, "_is_current_refit_demo", False)):
                return candidates, stats
            # Fallback to legacy single-strategy mode
            return self._evaluate_legacy_candidate(
                symbol,
                broker_symbol,
                config,
                timeframes_filter=timeframes_filter,
            ), stats

        available_timeframes = config.get_available_timeframes()
        stats["timeframes_total"] = len(available_timeframes)
        regime_params_file = (
            getattr(self.pipeline_config, "regime_params_file", "regime_params.json")
            if self.pipeline_config
            else "regime_params.json"
        )
        live_require_tuned_regime_params = bool(
            getattr(self.pipeline_config, "live_require_tuned_regime_params", False)
        )
        is_current_refit_demo = bool(
            getattr(self, "_is_current_refit_demo", False)
        )
        has_tuned_regime_params = None
        if live_require_tuned_regime_params and not is_current_refit_demo:
            from pm_regime import has_tuned_regime_params as _has_tuned_regime_params

            has_tuned_regime_params = _has_tuned_regime_params
        probe_now = datetime.now()

        # Get live quality gate thresholds from config
        min_pf = (
            getattr(self.pipeline_config, "regime_min_val_profit_factor", 1.0)
            if self.pipeline_config
            else 1.0
        )
        min_return = (
            getattr(self.pipeline_config, "regime_min_val_return_pct", 5.0)
            if self.pipeline_config
            else 5.0
        )
        max_dd = (
            getattr(self.pipeline_config, "fx_val_max_drawdown", 35.0)
            if self.pipeline_config
            else 35.0
        )
        min_return_dd_ratio = (
            getattr(self.pipeline_config, "regime_min_val_return_dd_ratio", 1.0)
            if self.pipeline_config
            else 1.0
        )

        normalized_filter = {
            str(tf or "").upper()
            for tf in (timeframes_filter or set())
            if str(tf or "").strip()
        }

        def _timeframe_has_possible_live_winner(timeframe: str) -> bool:
            regimes = getattr(config, "regime_configs", {}) or {}
            tf_payload = regimes.get(timeframe)
            if not isinstance(tf_payload, dict):
                return False
            for candidate_cfg in tf_payload.values():
                if candidate_cfg is None or candidate_cfg.is_no_trade_marker():
                    continue
                status = str(
                    getattr(candidate_cfg, "promotion_status", "BLOCKED") or "BLOCKED"
                ).upper()
                if self._promotion_status_is_entry_eligible(status):
                    return True
            return False

        for tf in available_timeframes:
            normalized_tf = str(tf or "").upper()
            if normalized_filter and normalized_tf not in normalized_filter:
                stats["timeframes_skipped_stale"] += 1
                continue
            if self._uses_scheduled_trigger() and not self._is_symbol_timeframe_due(
                symbol, tf, now=probe_now
            ):
                stats["timeframes_skipped_stale"] += 1
                continue
            if not _timeframe_has_possible_live_winner(tf):
                stats["no_winner"] += 1
                self.logger.debug(
                    f"[{symbol}] [{tf}] No promoted live winner exists for this timeframe; skipping"
                )
                continue
            # Get bars for this timeframe (need at least the latest bar time)
            bars = self._get_live_bars(
                symbol,
                broker_symbol,
                tf,
                count=int(getattr(self.pipeline_config, "live_bars_count", 200)),
                min_required=int(getattr(self.pipeline_config, "live_min_bars", 100)),
            )
            if bars is None or len(bars) < int(
                getattr(self.pipeline_config, "live_min_bars", 100)
            ):
                stats["insufficient_bars"] += 1
                continue

            # Check for new bar (freshness tracking)
            current_bar_time = bars.index[-1]
            cache_key = f"{symbol}_{tf}"
            last_bar_time = self._last_bar_times.get(cache_key)

            # Determine if this is a new bar
            is_new_bar = last_bar_time is None or current_bar_time > last_bar_time
            if not is_new_bar:
                stats["timeframes_skipped_stale"] += 1
                continue
            stats["timeframes_evaluated"] += 1
            freshness = 1.0
            detector_params = None
            detector_params_sha = None
            if is_current_refit_demo:
                try:
                    detector_params, detector_params_sha = (
                        self._current_refit_demo_detector(symbol, tf)
                    )
                except Exception as exc:
                    stats["missing_tuned_regime_params"] += 1
                    self.logger.error(f"[{symbol}] [{tf}] {exc}")
                    self._last_bar_times[cache_key] = current_bar_time
                    continue
            feature_cache_key = (
                f"{symbol}_{tf}_{current_bar_time}_{detector_params_sha or 'file'}"
            )
            if (
                not is_current_refit_demo
                and live_require_tuned_regime_params
                and has_tuned_regime_params is not None
                and not has_tuned_regime_params(
                    symbol,
                    tf,
                    regime_params_file,
                    require_provenance=True,
                )
            ):
                stats["missing_tuned_regime_params"] += 1
                logged_missing = getattr(
                    self, "_live_missing_regime_params_logged", None
                )
                if logged_missing is None:
                    logged_missing = set()
                    setattr(self, "_live_missing_regime_params_logged", logged_missing)
                missing_key = f"{symbol}|{tf}"
                if missing_key not in logged_missing:
                    logged_missing.add(missing_key)
                    self.logger.info(
                        f"[{symbol}] [{tf}] No tuned regime params; default is NO_TRADE for live"
                    )
                self._last_bar_times[cache_key] = current_bar_time
                self._candidate_cache[feature_cache_key] = {
                    "features": None,
                    "regime": None,
                    "regime_strength": 0.0,
                    "signal": 0,
                    "strategy": None,
                    "regime_config": None,
                }
                self._prune_cache()
                continue
            cached = self._candidate_cache.get(feature_cache_key)

            if cached is not None:
                # Cache HIT - use cached features and signal (skip expensive computation)
                self._cache_hits += 1
                features = cached["features"]
                current_regime = cached["regime"]
                regime_strength = cached["regime_strength"]
                current_signal = cached["signal"]
                strategy = cached["strategy"]
                regime_config = cached["regime_config"]
                # Still need to check if regime_config exists (it was cached)
                if regime_config is None:
                    continue
            else:
                # Cache MISS - compute features and signal
                self._cache_misses += 1
                self._last_bar_times[cache_key] = current_bar_time
                # Clear old cache entries for this symbol/tf
                old_keys = [
                    k
                    for k in self._candidate_cache.keys()
                    if k.startswith(f"{symbol}_{tf}_")
                ]
                for k in old_keys:
                    del self._candidate_cache[k]

                # Compute features with regime detection
                features = FeatureComputer.compute_all(
                    bars,
                    symbol=symbol,
                    timeframe=tf,
                    regime_params_file=regime_params_file,
                    regime_params_override=detector_params,
                    regime_params_sha256=detector_params_sha,
                )

                # Get current regime from the decision-time surface on the
                # last closed bar. REGIME_LIVE is the optimizer/backtester
                # contract, so live trading fails closed if it is unavailable.
                if len(features) < 2:
                    continue
                if "REGIME_LIVE" not in features.columns:
                    stats["missing_regime_live"] += 1
                    self.logger.warning(
                        f"[{symbol}] [{tf}] REGIME_LIVE missing, refusing to trade this bar"
                    )
                    continue

                current_regime = features["REGIME_LIVE"].iloc[-2]
                strength_col = (
                    "REGIME_STRENGTH_LIVE"
                    if "REGIME_STRENGTH_LIVE" in features.columns
                    else None
                )
                regime_strength = (
                    features[strength_col].iloc[-2] if strength_col else 0.5
                )

                if current_regime is None or pd.isna(current_regime):
                    stats["missing_regime_live"] += 1
                    self.logger.warning(
                        f"[{symbol}] [{tf}] REGIME_LIVE is empty on the decision bar, refusing to trade"
                    )
                    continue
                if bool(
                    getattr(self.pipeline_config, "regime_boundary_audit_enabled", True)
                ):
                    ambiguity_policy = str(
                        getattr(
                            self.pipeline_config,
                            "regime_ambiguity_policy",
                            "observe_only",
                        )
                        or "observe_only"
                    ).lower()
                    if ambiguity_policy == "block" and "REGIME_GAP" in features.columns:
                        try:
                            current_gap = abs(
                                float(
                                    pd.to_numeric(
                                        features["REGIME_GAP"].iloc[-2], errors="coerce"
                                    )
                                )
                            )
                        except Exception:
                            current_gap = 0.0
                        min_gap = float(
                            getattr(
                                self.pipeline_config, "regime_min_live_gap_warn", 0.05
                            )
                            or 0.05
                        )
                        if current_gap < min_gap:
                            stats["winner_failed_gate"] += 1
                            self.logger.info(
                                f"[{symbol}] [{tf}] [{current_regime}] Regime ambiguity block: "
                                f"gap {current_gap:.4f} < {min_gap:.4f}"
                            )
                            self._candidate_cache[feature_cache_key] = {
                                "features": features,
                                "regime": current_regime,
                                "regime_strength": float(regime_strength),
                                "signal": 0,
                                "strategy": None,
                                "regime_config": None,
                            }
                            self._prune_cache()
                            continue

                # ===== WINNERS-ONLY LADDER =====
                # Only use the validated winner for this exact (tf, regime).
                regime_config = config.get_regime_config(tf, current_regime)
                if regime_config is None:
                    stats["no_winner"] += 1
                    self.logger.debug(
                        f"[{symbol}] [{tf}] [{current_regime}] No validated winner for timeframe/regime; skipping"
                    )
                    # Cache the "no config" result to avoid recomputing
                    self._candidate_cache[feature_cache_key] = {
                        "features": features,
                        "regime": current_regime,
                        "regime_strength": float(regime_strength),
                        "signal": 0,
                        "strategy": None,
                        "regime_config": None,
                    }
                    self._prune_cache()
                    continue
                if regime_config.is_no_trade_marker():
                    stats["no_winner"] += 1
                    marker_reason = (
                        getattr(regime_config, "artifact_meta", {}) or {}
                    ).get("reason", "optimizer stored an explicit no-trade marker")
                    self.logger.debug(
                        f"[{symbol}] [{tf}] [{current_regime}] Explicit NO_TRADE marker; skipping "
                        f"({marker_reason})"
                    )
                    self._candidate_cache[feature_cache_key] = {
                        "features": features,
                        "regime": current_regime,
                        "regime_strength": float(regime_strength),
                        "signal": 0,
                        "strategy": None,
                        "regime_config": None,
                    }
                    self._prune_cache()
                    continue
                promotion_status = str(
                    getattr(regime_config, "promotion_status", "BLOCKED") or "BLOCKED"
                ).upper()
                if not self._promotion_status_is_entry_eligible(promotion_status):
                    stats["winner_failed_gate"] += 1
                    reasons = "; ".join(
                        getattr(regime_config, "promotion_reasons", []) or []
                    )
                    self.logger.debug(
                        f"[{symbol}] [{tf}] [{current_regime}] Winner promotion status "
                        f"{promotion_status}; skipping live entry ({reasons})"
                    )
                    self._candidate_cache[feature_cache_key] = {
                        "features": features,
                        "regime": current_regime,
                        "regime_strength": float(regime_strength),
                        "signal": 0,
                        "strategy": None,
                        "regime_config": None,
                    }
                    self._prune_cache()
                    continue
                if not self._regime_config_is_entry_eligible(
                    regime_config,
                    min_pf=min_pf,
                    min_return=min_return,
                    max_dd=max_dd,
                    min_return_dd_ratio=min_return_dd_ratio,
                    symbol=symbol,
                    timeframe=tf,
                    regime=current_regime,
                ):
                    val_ret = regime_config.val_metrics.get("total_return_pct", -100)
                    val_dd = regime_config.val_metrics.get("max_drawdown_pct", 100)
                    val_ratio = val_ret / max(val_dd, 0.5)
                    stats["winner_failed_gate"] += 1
                    self.logger.debug(
                        f"[{symbol}] [{tf}] [{current_regime}] Winner failed live gate "
                        f"(PF={regime_config.val_metrics.get('profit_factor', 0):.2f}, "
                        f"ret={val_ret:.1f}%, "
                        f"dd={val_dd:.1f}%, "
                        f"ret/dd={val_ratio:.2f})"
                    )
                    self._candidate_cache[feature_cache_key] = {
                        "features": features,
                        "regime": current_regime,
                        "regime_strength": float(regime_strength),
                        "signal": 0,
                        "strategy": None,
                        "regime_config": None,
                    }
                    self._prune_cache()
                    continue

                # Get strategy instance
                try:
                    strategy = StrategyRegistry.get(
                        regime_config.strategy_name, **regime_config.parameters
                    )
                except Exception as e:
                    self.logger.warning(
                        f"[{symbol}] Failed to get strategy {regime_config.strategy_name}: {e}"
                    )
                    continue

                # Generate signal
                signals = strategy.generate_signals(features, symbol)
                current_signal = signals.iloc[-2] if len(signals) > 1 else 0
                if pd.isna(current_signal):
                    current_signal = 0

                # Cache the computed result
                self._candidate_cache[feature_cache_key] = {
                    "features": features,
                    "regime": current_regime,
                    "regime_strength": float(regime_strength),
                    "signal": int(current_signal),
                    "strategy": strategy,
                    "regime_config": regime_config,
                }
                self._prune_cache()  # Prevent unbounded growth
            # ─────────────────────────────────────────────────────────────────

            promotion_status = str(
                getattr(regime_config, "promotion_status", "BLOCKED") or "BLOCKED"
            ).upper()
            if not self._promotion_status_is_entry_eligible(promotion_status):
                stats["winner_failed_gate"] += 1
                continue

            # Compute selection score = strength * quality_score * freshness
            quality_score = (
                float(regime_config.quality_score)
                if getattr(regime_config, "quality_score", None) is not None
                else 0.5
            )
            selection_score = float(regime_strength) * quality_score * float(freshness)
            eligibility_report = {}
            portfolio_manager = getattr(self, "pm", None)
            if bool(getattr(self, "_is_current_refit_demo", False)):
                binding = dict(
                    (regime_config.artifact_meta or {}).get(
                        "current_refit_binding"
                    )
                    or {}
                )
                eligibility_report = {
                    "execution_book": CURRENT_REFIT_DEMO_EXECUTION_BOOK,
                    "book_role": PROCESS_VALIDATED_CURRENT_REFIT_ROLE,
                    "funded_live_eligible": False,
                    "candidate_id": str(binding.get("current_refit_cell_id") or ""),
                }
            elif portfolio_manager is not None and hasattr(
                portfolio_manager, "live_eligibility_report"
            ):
                eligibility_report = portfolio_manager.live_eligibility_report(
                    symbol, tf, current_regime
                )

            base_promo_mult = (
                float(
                    getattr(self.pipeline_config, "promote_small_risk_multiplier", 0.5)
                )
                if str(getattr(regime_config, "promotion_status", "") or "").upper()
                == "PROMOTE_SMALL"
                else 1.0
            )
            probation_mult, on_probation = self._probation_multiplier(
                regime_config,
                config,
                symbol=symbol,
                timeframe=tf,
                regime=current_regime,
            )
            candidates.append(
                {
                    "timeframe": tf,
                    "regime": current_regime,
                    "regime_strength": float(regime_strength),
                    "quality_score": quality_score,
                    "freshness": freshness,
                    "selection_score": selection_score,
                    "strategy": strategy,
                    "strategy_name": regime_config.strategy_name,
                    "signal": int(current_signal),
                    "features": features,
                    "regime_config": regime_config,
                    "eligibility_report": eligibility_report,
                    "promotion_status": str(
                        getattr(regime_config, "promotion_status", "BLOCKED")
                        or "BLOCKED"
                    ),
                    "promotion_risk_multiplier": base_promo_mult * probation_mult,
                    "on_probation": on_probation,
                    "bar_time": str(features.index[-2]) if len(features) > 1 else "",
                }
            )
            stats["winner_candidates"] += 1

        return candidates, stats

    def _probation_multiplier(
        self,
        regime_config: Any,
        config: Any,
        *,
        symbol: str = "",
        timeframe: str = "",
        regime: str = "",
    ) -> Tuple[float, bool]:
        """Probation tier (agent_audit/comms/CHANNEL.md [004]).

        Freshly (re)trained winners have no forward evidence, and stored-window metrics
        correlate poorly with full-sample profitability (Spearman <= ~0.30), so cells whose
        training timestamp is within `probation_days` trade at `probation_risk_multiplier`.
        Graduation requires both the minimum age and durable, exactly attributed
        live outcomes with positive mean R and profit factor. Missing provenance
        or missing feedback remains probationary.
        """
        cfg = self.pipeline_config
        if not bool(getattr(cfg, "probation_enabled", True)):
            return 1.0, False
        days = int(getattr(cfg, "probation_days", 14) or 0)
        if days <= 0:
            return 1.0, False
        stamp = getattr(regime_config, "trained_at", None) or getattr(
            config, "optimized_at", None
        )
        if isinstance(stamp, str):
            try:
                encoded = stamp.strip()
                if encoded.endswith(("Z", "z")):
                    encoded = encoded[:-1] + "+00:00"
                stamp = datetime.fromisoformat(encoded)
            except (TypeError, ValueError):
                stamp = None
        try:
            mult = float(getattr(cfg, "probation_risk_multiplier", 0.5) or 0.5)
        except (TypeError, ValueError):
            mult = 0.5
        probationary = (max(0.05, min(1.0, mult)), True)
        if not isinstance(stamp, datetime):
            return probationary
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        else:
            stamp = stamp.astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if stamp > now_utc or (now_utc - stamp) < timedelta(days=days):
            return probationary
        ledger = getattr(self, "_live_outcome_ledger", None)
        if ledger is None or not all((symbol, timeframe, regime)):
            return probationary
        try:
            summary = ledger.exact_cell_summary(
                symbol,
                timeframe,
                regime,
                str(getattr(regime_config, "strategy_name", "") or ""),
            )
            minimum = max(1, int(getattr(cfg, "probation_min_live_outcomes", 30) or 30))
            min_mean_r = float(getattr(cfg, "probation_min_mean_r", 0.0) or 0.0)
            min_pf = float(getattr(cfg, "probation_min_profit_factor", 1.0) or 1.0)
            mean_r = summary.get("mean_r")
            if (
                int(summary.get("exact_closed_positions", 0) or 0) < minimum
                or int(summary.get("r_observations", 0) or 0) < minimum
                or mean_r is None
                or float(mean_r) <= min_mean_r
                or float(summary.get("profit_factor_r", 0.0) or 0.0) < min_pf
            ):
                return probationary
        except Exception:
            return probationary
        return 1.0, False

    def _evaluate_legacy_candidate(
        self,
        symbol: str,
        broker_symbol: str,
        config: SymbolConfig,
        timeframes_filter: Optional[Set[str]] = None,
    ) -> List[Dict]:
        """
        Fallback for legacy configs without regime_configs.

        Uses the single strategy_name/timeframe/parameters from config.
        """
        from pm_strategies import StrategyRegistry

        # Winners-only live gate for legacy configs
        min_pf = (
            getattr(self.pipeline_config, "regime_min_val_profit_factor", 1.0)
            if self.pipeline_config
            else 1.0
        )
        min_return = (
            getattr(self.pipeline_config, "regime_min_val_return_pct", 5.0)
            if self.pipeline_config
            else 5.0
        )
        max_dd = (
            getattr(self.pipeline_config, "fx_val_max_drawdown", 35.0)
            if self.pipeline_config
            else 35.0
        )
        min_return_dd_ratio = (
            getattr(self.pipeline_config, "regime_min_val_return_dd_ratio", 1.0)
            if self.pipeline_config
            else 1.0
        )
        val_pf = config.val_metrics.get("profit_factor", 0.0)
        val_return = config.val_metrics.get("total_return_pct", -100.0)
        val_dd = config.val_metrics.get("max_drawdown_pct", 100.0)
        val_return_dd_ratio = val_return / max(val_dd, 0.5)
        default_config = getattr(config, "default_config", None)
        legacy_status = str(
            getattr(default_config, "promotion_status", "BLOCKED")
            if default_config is not None
            else "BLOCKED"
        ).upper()
        if legacy_status in {"BLOCKED", "PAPER_ONLY", "NO_TRADE"}:
            reasons = (
                "; ".join(getattr(default_config, "promotion_reasons", []) or [])
                if default_config
                else ""
            )
            self.logger.debug(
                f"[{symbol}] Legacy winner promotion status {legacy_status}; skipping live entry ({reasons})"
            )
            return []
        if (
            not config.is_validated
            or val_pf < min_pf
            or val_return < min_return
            or val_dd > max_dd
            or val_return_dd_ratio < min_return_dd_ratio
        ):
            self.logger.debug(
                f"[{symbol}] Legacy config failed live gate "
                f"(validated={config.is_validated}, PF={val_pf:.2f}, ret={val_return:.1f}%, "
                f"dd={val_dd:.1f}%, ret/dd={val_return_dd_ratio:.2f})"
            )
            return []

        timeframe = config.timeframe
        if not timeframe:
            return []

        normalized_filter = {
            str(tf or "").upper()
            for tf in (timeframes_filter or set())
            if str(tf or "").strip()
        }
        if normalized_filter and str(timeframe or "").upper() not in normalized_filter:
            return []

        if self._uses_scheduled_trigger() and not self._is_symbol_timeframe_due(
            symbol, timeframe
        ):
            return []

        # Get bars
        bars = self._get_live_bars(
            symbol,
            broker_symbol,
            timeframe,
            count=int(getattr(self.pipeline_config, "live_bars_count", 200)),
            min_required=50,
        )
        if bars is None or len(bars) < 50:
            return []

        # Check for new bar
        current_bar_time = bars.index[-1]
        cache_key = f"{symbol}_{timeframe}"
        last_bar_time = self._last_bar_times.get(cache_key)

        if last_bar_time is not None and current_bar_time <= last_bar_time:
            return []  # Not a new bar

        self._last_bar_times[cache_key] = current_bar_time

        # Get strategy
        try:
            strategy = StrategyRegistry.get(config.strategy_name, **config.parameters)
        except Exception as e:
            self.logger.warning(
                f"[{symbol}] Failed to get strategy {config.strategy_name}: {e}"
            )
            return []

        # Compute features
        features = FeatureComputer.compute_all(bars, symbol=symbol, timeframe=timeframe)

        # Generate signal
        signals = strategy.generate_signals(features, symbol)
        current_signal = signals.iloc[-2] if len(signals) > 1 else 0
        if pd.isna(current_signal):
            current_signal = 0
        eligibility_report = {}
        portfolio_manager = getattr(self, "pm", None)
        if portfolio_manager is not None and hasattr(
            portfolio_manager, "live_eligibility_report"
        ):
            eligibility_report = portfolio_manager.live_eligibility_report(
                symbol, timeframe, "LEGACY"
            )

        return [
            {
                "timeframe": timeframe,
                "regime": "LEGACY",
                "regime_strength": 1.0,
                "quality_score": (
                    float(config.composite_score) / 100.0
                    if getattr(config, "composite_score", None) is not None
                    else 0.5
                ),
                "freshness": 1.0,
                "selection_score": 1.0,
                "strategy": strategy,
                "strategy_name": config.strategy_name,
                "signal": int(current_signal),
                "features": features,
                "regime_config": None,
                "eligibility_report": eligibility_report,
                "bar_time": str(features.index[-2]) if len(features) > 1 else "",
            }
        ]

    def _execute_entry(
        self,
        symbol: str,
        signal: int,
        strategy: BaseStrategy,
        features: pd.DataFrame,
        spec,
        magic: int,
        config: SymbolConfig,
        decision_key: str = "",
        bar_time_iso: str = "",
        best_candidate: Dict = None,
        is_secondary_trade: bool = False,
        position_context: Optional[Dict[str, Any]] = None,
        secondary_reason: str = "",
        trade_intent: Optional[TradeIntent] = None,
        broker_symbol: str = "",
        positions_snapshot: Optional[List[Any]] = None,
        account_info: Optional[Any] = None,
    ):
        """
        Execute an entry trade.

        Records every outcome (success, skip, failure) into the decision
        throttle so that the same signal on the same bar is never re-attempted
        or re-logged.

        Args:
            is_secondary_trade: If True, this is a secondary trade (D1 already open).
                                Risk will be reduced according to config.
            position_context: Snapshot of open-position context for downstream logs.
            secondary_reason: Reason for secondary trade classification.
        """
        from pm_position import TradeTagEncoder

        broker_symbol = broker_symbol or symbol
        is_long = signal == 1
        direction = "LONG" if is_long else "SHORT"
        direction_label = "BUY" if is_long else "SELL"
        position_context = position_context or {}
        secondary_reason = secondary_reason or ""

        # Helper to extract throttle metadata from the best_candidate dict
        _tf = best_candidate.get("timeframe", "") if best_candidate else ""
        _regime = best_candidate.get("regime", "") if best_candidate else ""
        _strat_name = best_candidate.get("strategy_name", "") if best_candidate else ""
        regime_candidate = (
            best_candidate.get("regime_config") if best_candidate else None
        )
        execution_book_role = (
            PROCESS_VALIDATED_CURRENT_REFIT_ROLE
            if bool(getattr(self, "_is_current_refit_demo", False))
            else LOCKBOX_CHAMPION_ROLE
        )
        current_refit_binding = dict(
            (getattr(regime_candidate, "artifact_meta", {}) or {}).get(
                "current_refit_binding"
            )
            or {}
        )
        execution_candidate_id = str(
            current_refit_binding.get("current_refit_cell_id") or ""
        )

        decision_context = {
            "secondary_trade": bool(is_secondary_trade),
            "secondary_reason": secondary_reason,
            "position_context": position_context,
            "execution_book": str(
                getattr(self, "execution_book", CHAMPION_EXECUTION_BOOK)
            ),
            "book_role": execution_book_role,
            "candidate_id": execution_candidate_id,
        }
        try:
            event_observation = observe_event_context(
                symbol,
                self.pipeline_config,
                observed_at=datetime.now(),
                calendar=getattr(self, "_event_calendar", None),
            )
            if event_observation.nearby_events:
                decision_context["event_calendar"] = {
                    "action": event_observation.action,
                    "observed_at": event_observation.observed_at.isoformat(),
                    "nearby_events": list(event_observation.nearby_events),
                }
        except Exception as exc:
            self.logger.debug(f"[{symbol}] Event calendar observation failed: {exc}")

        def _record_throttle(action: str, cooldown: int = 0) -> None:
            """Record this decision outcome into the throttle."""
            if decision_key:
                self._decision_throttle.record_decision(
                    symbol=symbol,
                    decision_key=decision_key,
                    bar_time_iso=bar_time_iso,
                    timeframe=_tf,
                    regime=_regime,
                    strategy_name=_strat_name,
                    direction=signal,
                    action=action,
                    cooldown_seconds=cooldown,
                    context=decision_context,
                )

        def _safe_float(value: Any) -> Optional[float]:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def _looks_like_position(value: Any) -> bool:
            if value is None:
                return False
            if type(value).__module__.startswith("unittest.mock"):
                return False
            return any(
                hasattr(value, attr)
                for attr in ("ticket", "symbol", "magic", "volume", "comment")
            )

        def _is_ambiguous_order_result(value: Any) -> bool:
            if value is None:
                return True
            if bool(getattr(value, "ambiguous", False)):
                return True
            retcode = getattr(value, "retcode", None)
            try:
                if int(retcode) == 10012:
                    return True
            except (TypeError, ValueError):
                pass
            text = f"{getattr(value, 'retcode_description', '')} {getattr(value, 'error_message', '')}".lower()
            return "timeout" in text or "timed out" in text

        def _is_definitive_no_fill_result(value: Any) -> bool:
            return (
                getattr(value, "success", None) is False
                and not _is_ambiguous_order_result(value)
            )

        def _find_live_position_by_magic() -> Optional[Any]:
            if hasattr(self.mt5, "get_position_by_symbol_magic"):
                try:
                    found = self.mt5.get_position_by_symbol_magic(broker_symbol, magic)
                except Exception as exc:
                    self.logger.debug(
                        f"[{symbol}] Ambiguous-send exact-position reconciliation failed: {exc}"
                    )
                    found = None
                if _looks_like_position(found):
                    return found
            if hasattr(self.mt5, "get_positions"):
                try:
                    positions = self.mt5.get_positions(symbol=broker_symbol)
                except Exception as exc:
                    self.logger.debug(
                        f"[{symbol}] Ambiguous-send position snapshot failed: {exc}"
                    )
                    positions = None
                for pos in list(positions or []):
                    if int(getattr(pos, "magic", 0) or 0) == magic:
                        return pos
            return None

        selection_score = (
            _safe_float(best_candidate.get("selection_score"))
            if best_candidate
            else None
        )
        quality_score = (
            _safe_float(best_candidate.get("quality_score")) if best_candidate else None
        )
        freshness_score = (
            _safe_float(best_candidate.get("freshness")) if best_candidate else None
        )
        regime_strength_score = (
            _safe_float(best_candidate.get("regime_strength"))
            if best_candidate
            else None
        )
        decision_context.update(
            {
                "selection_score": selection_score,
                "quality_score": quality_score,
                "freshness": freshness_score,
                "regime_strength": regime_strength_score,
            }
        )
        if trade_intent is not None:
            decision_context["governance_policy"] = policy_name_from_artifact(
                getattr(trade_intent, "governance_hint", {}) or {}
            )

        def _record_actionable(
            action: str,
            entry_price_value: Optional[float] = None,
            sl_value: Optional[float] = None,
            tp_value: Optional[float] = None,
            volume_value: Optional[float] = None,
            target_risk_pct_value: Optional[float] = None,
            actual_risk_pct_value: Optional[float] = None,
            expected_entry_price_value: Optional[float] = None,
            filled_entry_price_value: Optional[float] = None,
            entry_slippage_pips_value: Optional[float] = None,
            entry_slippage_r_value: Optional[float] = None,
            entry_slippage_direction_value: Optional[str] = None,
            context: Optional[Dict[str, Any]] = None,
        ) -> None:
            """Persist actionable outcomes for dashboard consumption."""
            if not decision_key:
                return
            try:
                payload = {
                    "symbol": symbol,
                    "broker_symbol": broker_symbol,
                    "decision_key": decision_key,
                    "bar_time": bar_time_iso,
                    "timeframe": _tf,
                    "regime": _regime,
                    "strategy_name": _strat_name,
                    "direction": direction_label,
                    "action": action,
                    "actionable_schema_version": ACTIONABLE_SCHEMA_VERSION,
                    "action_time": datetime.now().isoformat(),
                    "entry_price": entry_price_value,
                    "stop_loss_price": sl_value,
                    "take_profit_price": tp_value,
                    "volume": volume_value,
                    "target_risk_pct": target_risk_pct_value,
                    "actual_risk_pct": actual_risk_pct_value,
                    "expected_entry_price": expected_entry_price_value,
                    "filled_entry_price": filled_entry_price_value,
                    "entry_slippage_pips": entry_slippage_pips_value,
                    "entry_slippage_r": entry_slippage_r_value,
                    "entry_slippage_direction": entry_slippage_direction_value,
                    "secondary_trade": bool(is_secondary_trade),
                    "secondary_reason": secondary_reason,
                    "position_context": position_context,
                    "score": selection_score,
                    "quality": quality_score,
                    "freshness": freshness_score,
                    "regime_strength": regime_strength_score,
                    "execution_book": str(
                        getattr(self, "execution_book", CHAMPION_EXECUTION_BOOK)
                    ),
                    "book_role": execution_book_role,
                    "candidate_id": execution_candidate_id,
                }
                payload_context: Dict[str, Any] = {}
                if isinstance(decision_context.get("event_calendar"), dict):
                    payload_context["event_calendar"] = dict(
                        decision_context["event_calendar"]
                    )
                if isinstance(context, dict) and context:
                    payload_context.update(dict(context))
                if payload_context:
                    payload["context"] = payload_context
                self._actionable_log.record(symbol, payload)
                storage_manager = getattr(self, "storage_manager", None)
                if storage_manager is not None:
                    storage_manager.record_actionable(payload)
            except Exception as exc:
                self.logger.debug(f"[{symbol}] Actionable log failed: {exc}")

        storage_manager = getattr(self, "storage_manager", None)
        if (
            bool(getattr(self, "_live_outcome_feedback_initialized", False))
            and bool(
                getattr(self.pipeline_config, "live_outcome_feedback_enabled", True)
            )
            and (
                getattr(self, "_live_outcome_ledger", None) is None
                or bool(getattr(self, "_live_outcome_feedback_error", ""))
            )
        ):
            reason = str(
                getattr(self, "_live_outcome_feedback_error", "")
                or "durable live outcome ledger unavailable"
            )
            self.logger.error(
                f"[{symbol}] Skipping trade; live outcome feedback is fail-closed: {reason}"
            )
            _record_throttle("BLOCKED_LIVE_OUTCOME_FEEDBACK")
            _record_actionable(
                action="BLOCKED_LIVE_OUTCOME_FEEDBACK",
                context={"live_outcome_feedback_error": reason},
            )
            return
        manual_block_reason = self._manual_new_entries_block_reason()
        if manual_block_reason:
            self.logger.warning(f"[{symbol}] Skipping trade; {manual_block_reason}")
            _record_throttle("BLOCKED_MANUAL_KILL_SWITCH")
            _record_actionable(
                action="BLOCKED_MANUAL_KILL_SWITCH",
                context={"manual_block_reason": manual_block_reason},
            )
            return

        if storage_manager is not None and storage_manager.should_pause_new_entries():
            self.logger.warning(
                f"[{symbol}] Storage pressure advisory active; continuing with live entry"
            )

        ambiguous_blocks = getattr(self, "_ambiguous_order_block_until", None)
        if ambiguous_blocks is None:
            ambiguous_blocks = {}
            self._ambiguous_order_block_until = ambiguous_blocks
        block_until = ambiguous_blocks.get(symbol)
        if block_until is not None:
            now = datetime.now()
            if now < block_until:
                remaining = max(1, int((block_until - now).total_seconds()))
                self.logger.warning(
                    f"[{symbol}] Skipping trade; ambiguous order reconciliation pending "
                    f"({remaining}s remaining)"
                )
                _record_throttle("BLOCKED_AMBIGUOUS_ORDER_PENDING", cooldown=remaining)
                _record_actionable(
                    action="BLOCKED_AMBIGUOUS_ORDER_PENDING",
                    context={"ambiguous_order_block_remaining_seconds": remaining},
                )
                return
            ambiguous_blocks.pop(symbol, None)

        # Rate-limit the exact specialist, not the whole symbol.  The symbol
        # controller may legitimately admit an independent D1/lower-TF pair;
        # a symbol-wide timer silently discarded the second specialist even
        # though fresh position and institution gates explicitly allowed it.
        rate_limit_key = self._specialist_rate_limit_key(symbol, magic)
        last_order_time = self._last_order_times.get(rate_limit_key)
        if last_order_time:
            time_since_last = (datetime.now() - last_order_time).total_seconds()
            if time_since_last < self.ORDER_RATE_LIMIT_SECONDS:
                remaining = max(0.0, self.ORDER_RATE_LIMIT_SECONDS - time_since_last)
                self.logger.info(
                    f"[{symbol}] Skipping trade; rate limited ({remaining:.1f}s remaining)"
                )
                _record_throttle(
                    "SKIPPED_RATE_LIMIT", cooldown=int(max(1.0, remaining))
                )
                _record_actionable(action="SKIPPED_RATE_LIMIT")
                return

        # Re-verify no position exists right before execution (race condition prevention)
        existing = None
        fresh_symbol_positions = None
        if positions_snapshot is not None:
            for pos in positions_snapshot:
                if (
                    getattr(pos, "symbol", None) == broker_symbol
                    and int(getattr(pos, "magic", 0) or 0) == magic
                ):
                    existing = pos
                    break
        if existing is None and hasattr(self.mt5, "get_position_by_symbol_magic"):
            fresh_exact = self.mt5.get_position_by_symbol_magic(broker_symbol, magic)
            if _looks_like_position(fresh_exact):
                existing = fresh_exact
        if existing is None and hasattr(self.mt5, "get_positions"):
            fresh_positions = self.mt5.get_positions(symbol=broker_symbol)
            if fresh_positions is None:
                self.logger.warning(
                    f"[{symbol}] Trade skipped: fresh position check unavailable for {broker_symbol}"
                )
                _record_throttle("BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE")
                _record_actionable(action="BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE")
                return
            fresh_symbol_positions = list(fresh_positions or [])
            for pos in fresh_symbol_positions:
                if int(getattr(pos, "magic", 0) or 0) == magic:
                    existing = pos
                    break
        if existing:
            self.logger.info(
                f"[{symbol}] Trade skipped: position exists for magic {magic} "
                f"(tf={_tf}, regime={_regime})"
            )
            _record_throttle("SKIPPED_POSITION_EXISTS")
            _record_actionable(action="SKIPPED_POSITION_EXISTS")
            return

        if fresh_symbol_positions is not None:
            fresh_symbol_positions = [
                pos
                for pos in fresh_symbol_positions
                if getattr(pos, "symbol", None) == broker_symbol
            ]
            fresh_open_timeframes: Set[str] = set()
            fresh_has_d1 = False
            fresh_has_non_d1 = False
            fresh_has_unknown = False

            for pos in fresh_symbol_positions:
                pos_tf = self._infer_position_timeframe(symbol, config, pos)
                pos_tf = str(pos_tf or "").upper()
                if pos_tf:
                    fresh_open_timeframes.add(pos_tf)
                    if pos_tf == "D1":
                        fresh_has_d1 = True
                    else:
                        fresh_has_non_d1 = True
                else:
                    fresh_has_unknown = True
                    fresh_has_non_d1 = True

            position_context.update(
                {
                    "fresh_open_positions_total": len(fresh_symbol_positions),
                    "fresh_open_timeframes": sorted(fresh_open_timeframes),
                    "fresh_open_has_d1": fresh_has_d1,
                    "fresh_open_has_non_d1": fresh_has_non_d1,
                    "fresh_open_has_unknown": fresh_has_unknown,
                }
            )

            def _block_fresh_position_state(action: str, reason: str) -> None:
                self.logger.info(
                    f"[{symbol}] Trade blocked by fresh position state: {reason}"
                )
                _record_throttle(action)
                _record_actionable(
                    action=action, context={"position_state_reason": reason}
                )

            fresh_position_block = self._same_symbol_specialist_position_block(
                symbol,
                config,
                fresh_symbol_positions,
                account_info,
                candidate_magic=magic,
            )
            if fresh_position_block:
                _block_fresh_position_state(*fresh_position_block)
                return
            if fresh_symbol_positions:
                is_secondary_trade = True
                secondary_reason = secondary_reason or "additional_specialist_slot"
            elif is_secondary_trade:
                is_secondary_trade = False
                secondary_reason = ""

            decision_context.update(
                {
                    "secondary_trade": bool(is_secondary_trade),
                    "secondary_reason": secondary_reason,
                    "position_context": position_context,
                }
            )

        if trade_intent is None:
            signal_bar_index = (
                max(len(features) - 2, 0)
                if len(features) > 1
                else max(len(features) - 1, 0)
            )
            trade_intent = strategy.build_trade_intent(
                features,
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                signal=signal,
                spec=spec,
                bar_index=signal_bar_index,
                signal_strength=float(regime_strength_score or 0.0),
                selection_score=float(selection_score or 0.0),
                quality_score=float(quality_score or 0.0),
                metadata={"bar_time": bar_time_iso},
            )
        sl_pips = float(getattr(trade_intent, "stop_loss_pips", 0.0) or 0.0)
        tp_pips = float(getattr(trade_intent, "take_profit_pips", 0.0) or 0.0)

        # Get symbol info for sizing and broker constraints
        symbol_info = self.mt5.get_symbol_info(broker_symbol)
        if symbol_info is None:
            self.logger.warning(f"[{symbol}] Skipping trade; symbol info unavailable")
            _record_throttle("SKIPPED_NO_SYMBOL_INFO")
            _record_actionable(action="SKIPPED_NO_SYMBOL_INFO")
            return

        # Reject untradable broker symbols early.
        if not getattr(symbol_info, "visible", True):
            self.logger.warning(
                f"[{symbol}] Symbol not visible on broker; skipping trade"
            )
            _record_throttle("SKIPPED_SYMBOL_NOT_VISIBLE")
            _record_actionable(
                action="SKIPPED_SYMBOL_NOT_VISIBLE",
                context={"broker_symbol": broker_symbol},
            )
            return
        if getattr(symbol_info, "trade_mode", 2) == 0:  # SYMBOL_TRADE_MODE_DISABLED
            self.logger.warning(
                f"[{symbol}] Symbol trade mode disabled; skipping trade"
            )
            _record_throttle("SKIPPED_SYMBOL_TRADE_DISABLED")
            _record_actionable(
                action="SKIPPED_SYMBOL_TRADE_DISABLED",
                context={
                    "broker_symbol": broker_symbol,
                    "trade_mode": getattr(symbol_info, "trade_mode", 0),
                },
            )
            return

        # Risk basis (balance/equity)
        account = account_info or self.mt5.get_account_info()
        if account is None:
            self.logger.warning(f"[{symbol}] Skipping trade; account info unavailable")
            _record_throttle("SKIPPED_NO_ACCOUNT_INFO")
            _record_actionable(action="SKIPPED_NO_ACCOUNT_INFO")
            return

        # --- Margin protection entry gate (black-swan guard) ---
        margin_state_error = self._margin_state_persistence_block_reason()
        if margin_state_error:
            self.logger.error(
                f"[{symbol}] MARGIN STATE UNAVAILABLE: {margin_state_error}; "
                "entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_STATE_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_STATE_UNAVAILABLE",
                context={"margin_state_error": margin_state_error},
            )
            return
        margin_level = self._safe_account_margin_level(account)
        margin_used = self._account_margin_used(account)
        # The margin entry/block threshold is the signed selected envelope's
        # min_margin_level_pct, never static config; fail closed if no envelope
        # is bound (seq1644/1684; funded fail-closed seq1713; pre-veto fix
        # seq1720).  The reopen level stays an operational hysteresis overlay
        # keyed off the selected block level.
        _cap_env = self._bound_capital_admission_envelope()
        if _cap_env is None:
            self.logger.info(
                f"[{symbol}] ENTRY BLOCKED: no bound capital envelope for margin admission"
            )
            _record_throttle("SKIPPED_CAPITAL_ENVELOPE_UNAVAILABLE")
            _record_actionable(action="SKIPPED_CAPITAL_ENVELOPE_UNAVAILABLE")
            return
        margin_block = float(_cap_env["min_margin_level_pct"])
        margin_reopen = float(
            getattr(self.pipeline_config, "margin_reopen_level", margin_block)
            or margin_block
        )
        if margin_used and (margin_level is None or margin_level <= 0.0):
            self.logger.warning(
                f"[{symbol}] MARGIN DATA UNAVAILABLE: margin is in use but margin_level is unavailable; entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(action="SKIPPED_MARGIN_UNAVAILABLE")
            return
        # margin_level == 0 means no positions open (undefined, not stressed).
        if (
            margin_level is not None
            and margin_level > 0
            and margin_level < margin_block
        ):
            self.logger.info(
                f"[{symbol}] MARGIN BLOCKED: margin_level={margin_level:.1f}% "
                f"< {margin_block:.0f}% — entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_BLOCKED")
            _record_actionable(action="SKIPPED_MARGIN_BLOCKED")
            return
        if (
            margin_level is not None
            and margin_level > 0
            and self._is_margin_reopen_required()
            and margin_level < margin_reopen
        ):
            self.logger.info(
                f"[{symbol}] MARGIN REOPEN WAIT: margin_level={margin_level:.1f}% "
                f"< reopen {margin_reopen:.0f}% - entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_REOPEN_WAIT")
            _record_actionable(action="SKIPPED_MARGIN_REOPEN_WAIT")
            return
        if margin_level is not None and margin_level >= margin_reopen:
            self._clear_margin_reopen_if_recovered(margin_level)

        # --- Margin re-open cooldown ---
        # Even if margin_level recovered above the block threshold, refuse to
        # re-arm new entries for `margin_reopen_cooldown_minutes` after the most
        # recent forced close. Prevents the panic-cycle from re-opening the
        # exact exposure that triggered the close in the first place.
        last_margin_close_ts = getattr(self, "_last_margin_close_ts", None)
        remaining = self._margin_cooldown_remaining_minutes()
        if remaining > 0.0 and last_margin_close_ts is not None:
            notice_now = self._margin_utc_now()
            last_notice = getattr(self, "_margin_cooldown_notice_logged_ts", None)
            notice_due = True
            if last_notice is not None:
                try:
                    notice_due = (
                        notice_now
                        - self._normalize_margin_timestamp(
                            last_notice,
                            "margin_cooldown_notice_utc",
                        )
                    ).total_seconds() > 60.0
                except (TypeError, ValueError, OverflowError):
                    notice_due = True
            if notice_due:
                self.logger.info(
                    f"[{symbol}] MARGIN COOLDOWN: {remaining:.1f} min remaining "
                    f"(forced close at {last_margin_close_ts.isoformat(timespec='seconds')}) — entry blocked"
                )
                self._margin_cooldown_notice_logged_ts = notice_now
            _record_throttle("SKIPPED_MARGIN_COOLDOWN")
            _record_actionable(action="SKIPPED_MARGIN_COOLDOWN")
            return

        basis_pref = getattr(self.position_config, "risk_basis", "balance")
        basis_value = account.balance if basis_pref == "balance" else account.equity
        if basis_value <= 0:
            self.logger.warning(
                f"[{symbol}] Invalid risk basis value ({basis_value}); skipping trade"
            )
            _record_throttle("SKIPPED_INVALID_BASIS")
            _record_actionable(action="SKIPPED_INVALID_BASIS")
            return

        # Get current price (same basis used for execution)
        tick = self.mt5.get_symbol_tick(broker_symbol)
        if tick is None:
            self.logger.warning(f"[{symbol}] Skipping trade; tick data unavailable")
            _record_throttle("SKIPPED_NO_TICK")
            _record_actionable(action="SKIPPED_NO_TICK")
            return
        entry_price = tick.ask if is_long else tick.bid

        # Spread-aware execution overlay
        _spread_overlay = self._enhancement_seams.execution_quality_overlay
        _execution_risk_mult = 1.0
        _cycle_spread_degradation_mult = 0.0
        if hasattr(_spread_overlay, "enabled") and _spread_overlay.enabled:
            _pip_size = spec.pip_size if spec.pip_size > 0 else 1e-5
            _spread_pips = (tick.ask - tick.bid) / _pip_size
            _atr_val = 0.0
            if (
                features is not None
                and "ATR_14" in features.columns
                and len(features) > 1
            ):
                _atr_val = float(features["ATR_14"].iloc[-2]) / _pip_size
            _rolling_spread_median = float(spec.spread_avg)
            if (
                features is not None
                and "Spread" in features.columns
                and len(features) > 2
            ):
                _spread_window = (
                    pd.to_numeric(features["Spread"].iloc[:-1], errors="coerce")
                    .dropna()
                    .tail(100)
                )
                if not _spread_window.empty:
                    _rolling_window_median = float(
                        (_spread_window * float(spec.point) / _pip_size).median()
                    )
                    # Anchor against the larger of rolling-window median and the
                    # instrument's baseline spread_avg. Pre-news compression can
                    # collapse the rolling median and arm the spike blocker on
                    # routine widenings; post-news persistence can inflate it
                    # and hide real spikes. Taking max() keeps the spike test
                    # honest in both regimes.
                    _rolling_spread_median = max(
                        _rolling_window_median, float(spec.spread_avg)
                    )
            _eq_ctx = ExecutionQualityContext(
                symbol=symbol,
                timeframe=_tf,
                spread_pips=_spread_pips,
                atr_pips=_atr_val,
                rolling_spread_median=_rolling_spread_median,
            )
            if _rolling_spread_median > 0:
                _cycle_spread_degradation_mult = float(_spread_pips) / float(
                    _rolling_spread_median
                )
            _eq_decision = _spread_overlay.evaluate(_eq_ctx)
            if not _eq_decision.allow_trade:
                self.logger.info(
                    f"[{symbol}] Trade blocked by spread filter: {'; '.join(_eq_decision.notes)}"
                )
                _record_throttle("BLOCKED_SPREAD_FILTER")
                _record_actionable(
                    action="BLOCKED_SPREAD_FILTER",
                    entry_price_value=entry_price,
                )
                return
            _execution_risk_mult = float(
                max(0.0, min(1.0, _eq_decision.score_multiplier))
            )
            if _eq_decision.notes:
                self.logger.debug(
                    f"[{symbol}] Spread overlay: {'; '.join(_eq_decision.notes)}"
                )

        # Calculate stop prices from pips
        sl_price, tp_price = self.position_calc.calculate_stop_prices(
            entry_price, sl_pips, tp_pips, is_long, spec
        )

        # Capture original R-multiple for preservation when SL is widened
        original_sl_dist = abs(entry_price - sl_price)
        original_tp_dist = abs(tp_price - entry_price)
        original_r_multiple = (
            (original_tp_dist / original_sl_dist) if original_sl_dist > 0 else 0.0
        )

        # Enforce broker minimum stop distance (auto widen SL if too close)
        min_stop_dist = (
            float(symbol_info.trade_stops_level) * float(symbol_info.point)
            if symbol_info.trade_stops_level
            else 0.0
        )
        if min_stop_dist > 0 and abs(entry_price - sl_price) < min_stop_dist:
            if not bool(getattr(self.position_config, "auto_widen_sl", False)):
                reason = (
                    f"frozen stop distance {abs(entry_price - sl_price):.8f} is below "
                    f"broker minimum {min_stop_dist:.8f}"
                )
                self.logger.warning(f"[{symbol}] Skipping trade; {reason}")
                _record_throttle("SKIPPED_BROKER_STOP_DISTANCE")
                _record_actionable(
                    action="SKIPPED_BROKER_STOP_DISTANCE",
                    entry_price_value=entry_price,
                    sl_value=sl_price,
                    tp_value=tp_price,
                    context={
                        "broker_min_stop_distance": min_stop_dist,
                        "reason": reason,
                    },
                )
                return
            if is_long:
                sl_price = entry_price - min_stop_dist
            else:
                sl_price = entry_price + min_stop_dist
            # Research/paper compatibility only: preserve the original R ratio.
            if original_r_multiple > 0:
                new_sl_dist = abs(entry_price - sl_price)
                new_tp_dist = original_r_multiple * new_sl_dist
                tp_price = (
                    (entry_price + new_tp_dist)
                    if is_long
                    else (entry_price - new_tp_dist)
                )
            self.logger.debug(
                f"[{symbol}] Widened SL to satisfy min stop distance ({min_stop_dist}), TP adjusted to preserve R={original_r_multiple:.2f}"
            )

        # Target risk (deposit currency)
        # -------------------------------------------------------------
        # Winners-only risk policy: use base risk directly.
        # PositionConfig.max_risk_pct acts as the hard safety cap.
        # Secondary trade: additional cap + combined risk cap per symbol.
        # -------------------------------------------------------------
        from pm_position import TradeTagEncoder

        configured_risk_pct = float(
            getattr(self.position_config, "risk_per_trade_pct", 1.0)
        )
        min_trade_risk = float(getattr(self.pipeline_config, "min_trade_risk_pct", 0.1))
        target_risk_pct = configured_risk_pct
        promotion_risk_multiplier = 1.0
        if best_candidate:
            promotion_risk_multiplier = float(
                best_candidate.get("promotion_risk_multiplier", 1.0) or 1.0
            )
        if promotion_risk_multiplier < 1.0:
            prior_risk = target_risk_pct
            target_risk_pct *= max(0.0, min(1.0, promotion_risk_multiplier))
            self.logger.info(
                f"[{symbol}] Promotion risk derate: {prior_risk:.3f}% -> {target_risk_pct:.3f}% "
                f"(status={best_candidate.get('promotion_status') if best_candidate else 'unknown'})"
            )

        # Enforce PositionConfig max_risk_pct (hard safety cap)
        max_risk_cap = float(getattr(self.position_config, "max_risk_pct", 2.0))
        target_risk_pct = min(target_risk_pct, max_risk_cap)

        # Optional risk-scalar stack
        _risk_stack = self._enhancement_seams.risk_scalar_stack
        if _risk_stack.overlays:
            _atr_price = 0.0
            if (
                features is not None
                and "ATR_14" in features.columns
                and len(features) > 1
            ):
                _atr_price = float(features["ATR_14"].iloc[-2])
            _open_count = len(positions_snapshot) if positions_snapshot else 0
            _peak_equity = max(
                float(getattr(account, "equity", 0.0) or 0.0),
                float(self._equity_peak or 0.0),
            )
            _open_exposure_pct = 0.0
            if positions_snapshot:
                for _pos in positions_snapshot:
                    _risk_pct = self._estimate_position_risk_pct(
                        _pos,
                        account_info=account,
                        canonical_symbol=str(getattr(_pos, "symbol", "") or ""),
                        broker_symbol=str(getattr(_pos, "symbol", "") or ""),
                    )
                    if _risk_pct is not None:
                        _open_exposure_pct += float(_risk_pct)
            _regime_metrics = {}
            _regime_cfg = (
                best_candidate.get("regime_config") if best_candidate else None
            )
            if _regime_cfg is not None:
                _regime_metrics = dict(getattr(_regime_cfg, "val_metrics", {}) or {})
                if not _regime_metrics:
                    _regime_metrics = dict(
                        getattr(_regime_cfg, "train_metrics", {}) or {}
                    )
            _rs_ctx = RiskScalarContext(
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                base_risk_pct=target_risk_pct,
                account_equity=float(account.equity),
                account_peak_equity=_peak_equity,
                current_atr=_atr_price,
                current_price=float(entry_price),
                target_annual_vol=float(
                    getattr(self.pipeline_config, "target_annual_vol", 0.10)
                ),
                open_position_count=_open_count,
                open_exposure_pct=_open_exposure_pct,
                historical_win_rate=float(_regime_metrics.get("win_rate", 0.0)) / 100.0,
                historical_avg_win=float(
                    _regime_metrics.get(
                        "avg_win_dollars", _regime_metrics.get("avg_win", 0.0)
                    )
                ),
                historical_avg_loss=float(
                    _regime_metrics.get(
                        "avg_loss_dollars", _regime_metrics.get("avg_loss", 0.0)
                    )
                ),
                metrics=_regime_metrics,
            )
            pre_scalar_risk = target_risk_pct
            # Shadow mode reports the would-be scalar without mutating live risk.
            if getattr(_risk_stack, "shadow_mode", False):
                shadow_risk = _risk_stack.compute(target_risk_pct, _rs_ctx)
                if abs(shadow_risk - pre_scalar_risk) > 1e-6:
                    self.logger.info(
                        f"[{symbol}] Risk scalar SHADOW: would size "
                        f"{pre_scalar_risk:.3f}% -> {shadow_risk:.3f}% "
                        f"(delta {shadow_risk - pre_scalar_risk:+.3f}%); "
                        f"live target unchanged"
                    )
            else:
                target_risk_pct = _risk_stack.apply(target_risk_pct, _rs_ctx)
                if abs(target_risk_pct - pre_scalar_risk) > 1e-6:
                    self.logger.debug(
                        f"[{symbol}] Risk scalar stack: {pre_scalar_risk:.3f}% -> {target_risk_pct:.3f}%"
                    )
        if _execution_risk_mult < 1.0 and target_risk_pct > 0:
            pre_overlay_risk = target_risk_pct
            target_risk_pct *= _execution_risk_mult
            self.logger.debug(
                f"[{symbol}] Spread overlay risk scaling: {pre_overlay_risk:.3f}% -> "
                f"{target_risk_pct:.3f}% (mult={_execution_risk_mult:.2f})"
            )

        # Additional same-symbol specialists retain the same per-trade sizing
        # process as the synchronized replay.  Legacy D1-specific multiplier
        # and static secondary caps are compatibility-only configuration: the
        # replay-selected Capital envelope is the sole cross-position limit.
        if is_secondary_trade:
            original_risk = target_risk_pct
            # Additional-specialist same-symbol combined cap is the signed selected
            # envelope's max_same_symbol_stop_risk_pct, never static config; a
            # static cap must not pre-veto/shrink a secondary trade the selected
            # envelope would admit (seq1720).  Fail closed if unbound.
            _sec_cap_env = self._bound_capital_admission_envelope()
            if _sec_cap_env is None:
                self.logger.info(
                    f"[{symbol}] Secondary trade blocked: no bound capital envelope"
                )
                _record_throttle("BLOCKED_CAPITAL_ENVELOPE_UNAVAILABLE")
                _record_actionable(action="BLOCKED_CAPITAL_ENVELOPE_UNAVAILABLE")
                return
            max_combined_risk = float(_sec_cap_env["max_same_symbol_stop_risk_pct"])

            # Sum existing risk for this symbol from tagged positions.
            # If a position has no parseable risk tag, assume base_risk_pct for safety.
            existing_risk = 0.0
            try:
                source_positions = positions_snapshot
                if source_positions is None:
                    source_positions = self.mt5.get_positions(symbol=broker_symbol)
                if source_positions is None:
                    self.logger.warning(
                        f"[{symbol}] Position snapshot unavailable for secondary risk check; skipping trade"
                    )
                    _record_throttle("BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE")
                    _record_actionable(
                        action="BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE",
                        entry_price_value=entry_price,
                        sl_value=sl_price,
                        tp_value=tp_price,
                        volume_value=None,
                        target_risk_pct_value=target_risk_pct,
                        actual_risk_pct_value=None,
                    )
                    return
                for pos in source_positions:
                    if getattr(pos, "symbol", None) != broker_symbol:
                        continue
                    r = self._estimate_position_risk_pct(
                        pos,
                        account_info=account,
                        canonical_symbol=config.symbol,
                        broker_symbol=broker_symbol,
                        default_risk_pct=configured_risk_pct,
                    )
                    if r is not None:
                        existing_risk += float(r)
            except Exception:
                existing_risk = configured_risk_pct

            available = max_combined_risk - existing_risk
            if available <= 0:
                self.logger.info(
                    f"[{symbol}] Secondary trade blocked: combined risk cap reached ({existing_risk:.2f}% >= {max_combined_risk:.2f}%)"
                )
                self._decision_throttle.record_decision(
                    symbol=symbol,
                    decision_key=decision_key,
                    bar_time_iso=bar_time_iso,
                    timeframe=best_candidate.get("timeframe"),
                    regime=best_candidate.get("regime"),
                    strategy_name=best_candidate.get("strategy_name"),
                    direction=int(best_candidate.get("signal", 0)),
                    action="BLOCKED_RISK_CAP",
                    context=decision_context,
                )
                _record_actionable(
                    action="BLOCKED_RISK_CAP",
                    entry_price_value=entry_price,
                    sl_value=sl_price,
                    tp_value=tp_price,
                    volume_value=None,
                    target_risk_pct_value=target_risk_pct,
                    actual_risk_pct_value=None,
                )
                return

            target_risk_pct = min(target_risk_pct, available)
            self.logger.info(
                f"[{symbol}] Additional specialist: risk {original_risk:.2f}% -> {target_risk_pct:.2f}% "
                f"(existing={existing_risk:.2f}%, selected-cap={max_combined_risk:.2f}%)"
            )

        cycle_drift_monitor = getattr(
            self, "_cycle_drift_monitor", CycleDriftMonitor(self.pipeline_config)
        )
        drift_decision = cycle_drift_monitor.evaluate(
            CycleDriftContext(
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                strategy_name=_strat_name,
                spread_degradation_mult=_cycle_spread_degradation_mult,
            )
        )
        if drift_decision.reasons:
            decision_context["cycle_drift"] = {
                "action": drift_decision.action,
                "reasons": list(drift_decision.reasons),
                "observed_only": bool(drift_decision.observed_only),
            }
        if not drift_decision.allow_trade:
            self.logger.warning(
                f"[{symbol}] Trade blocked by cycle drift sentinel: {'; '.join(drift_decision.reasons)}"
            )
            _record_throttle("BLOCKED_CYCLE_DRIFT")
            _record_actionable(
                action="BLOCKED_CYCLE_DRIFT",
                context={"cycle_drift": decision_context.get("cycle_drift")},
            )
            return
        if drift_decision.risk_multiplier < 1.0 and target_risk_pct > 0.0:
            prior_risk = target_risk_pct
            target_risk_pct *= max(0.0, min(1.0, float(drift_decision.risk_multiplier)))
            self.logger.warning(
                f"[{symbol}] Cycle drift risk derate: {prior_risk:.3f}% -> {target_risk_pct:.3f}% "
                f"({'; '.join(drift_decision.reasons)})"
            )

        open_positions_for_supervisor = positions_snapshot
        if self.enable_trading and hasattr(self.mt5, "get_positions"):
            try:
                refreshed_portfolio_positions = self.mt5.get_positions()
            except Exception:
                refreshed_portfolio_positions = None
            if refreshed_portfolio_positions is None:
                self.logger.warning(
                    f"[{symbol}] Trade skipped: fresh portfolio position snapshot unavailable"
                )
                _record_throttle("BLOCKED_PORTFOLIO_SNAPSHOT_UNAVAILABLE")
                _record_actionable(
                    action="BLOCKED_PORTFOLIO_SNAPSHOT_UNAVAILABLE",
                    target_risk_pct_value=target_risk_pct,
                    context={
                        "institutional_supervisor": {"stage": "pre_size_snapshot"}
                    },
                )
                return
            open_positions_for_supervisor = list(refreshed_portfolio_positions or [])
        elif open_positions_for_supervisor is None and hasattr(
            self.mt5, "get_positions"
        ):
            open_positions_for_supervisor = self.mt5.get_positions()
        try:
            supervisor_cycle_id = self.pipeline_config.get_last_retrain_slot(
                datetime.now()
            ).isoformat()
        except Exception:
            supervisor_cycle_id = datetime.now().date().isoformat()
        total_open_risk_pct = self._estimate_total_open_risk_pct(
            open_positions_for_supervisor,
            account,
            default_risk_pct=configured_risk_pct,
        )
        institutional_supervisor = getattr(
            self,
            "_institutional_supervisor",
            InstitutionalSupervisor(self.pipeline_config),
        )
        supervisor_decision = institutional_supervisor.evaluate(
            SupervisorContext(
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                strategy_name=_strat_name,
                total_open_risk_pct=total_open_risk_pct,
                new_trade_risk_pct=target_risk_pct,
                max_total_stop_risk_pct=_cap_env[
                    "max_total_stop_risk_pct"
                ],
                daily_loss_pct=self._estimate_realized_loss_pct("daily", account),
                session_loss_pct=self._estimate_realized_loss_pct("session", account),
                weekly_loss_pct=self._estimate_realized_loss_pct("weekly", account),
                open_drawdown_pct=self._estimate_open_drawdown_pct(account),
                margin_level=margin_level or 0.0,
                metadata={"cycle_id": supervisor_cycle_id},
            )
        )
        if supervisor_decision.reasons:
            decision_context["institutional_supervisor"] = {
                "action": supervisor_decision.action,
                "reasons": list(supervisor_decision.reasons),
                "observed_only": bool(supervisor_decision.observed_only),
                "total_open_risk_pct": total_open_risk_pct,
            }
        if not supervisor_decision.allow_trade:
            self.logger.warning(
                f"[{symbol}] Trade blocked by institutional supervisor: "
                f"{'; '.join(supervisor_decision.reasons)}"
            )
            _record_throttle("BLOCKED_INSTITUTIONAL_SUPERVISOR")
            _record_actionable(
                action="BLOCKED_INSTITUTIONAL_SUPERVISOR",
                target_risk_pct_value=target_risk_pct,
                context={
                    "institutional_supervisor": decision_context.get(
                        "institutional_supervisor"
                    )
                },
            )
            return
        if supervisor_decision.risk_multiplier < 1.0 and target_risk_pct > 0.0:
            prior_risk = target_risk_pct
            target_risk_pct *= max(
                0.0, min(1.0, float(supervisor_decision.risk_multiplier))
            )
            self.logger.warning(
                f"[{symbol}] Institutional supervisor risk derate: "
                f"{prior_risk:.3f}% -> {target_risk_pct:.3f}%"
            )
        elif supervisor_decision.observed_only and supervisor_decision.reasons:
            self.logger.warning(
                f"[{symbol}] Institutional supervisor OBSERVE: {'; '.join(supervisor_decision.reasons)}"
            )

        # Ensure non-zero (and not absurdly tiny) risk if we proceed
        if target_risk_pct < min_trade_risk:
            self.logger.info(
                f"[{symbol}] Trade blocked: computed risk {target_risk_pct:.3f}% below min_trade_risk_pct={min_trade_risk:.3f}%"
            )
            _record_throttle("BLOCKED_TOO_LOW_RISK")
            _record_actionable(
                action="BLOCKED_TOO_LOW_RISK",
                context=decision_context,
            )
            return

        target_risk_amount = basis_value * (target_risk_pct / 100.0)

        # Volume bounds (combine config + broker)
        min_vol = max(
            float(self.position_config.min_position_size), float(symbol_info.volume_min)
        )
        # max_position_size = 0 means no config limit, use broker max only
        config_max = float(self.position_config.max_position_size)
        broker_max = float(symbol_info.volume_max)
        max_vol = min(config_max, broker_max) if config_max > 0 else broker_max

        order_type = OrderType.BUY if is_long else OrderType.SELL

        loss_per_lot, loss_per_lot_path = self._estimate_stop_loss_amount(
            order_type.value,
            broker_symbol,
            1.0,
            entry_price,
            sl_price,
            spec=spec,
            symbol_info=symbol_info,
            include_commission=True,
        )
        if loss_per_lot_path == "tick_value" and loss_per_lot:
            self.logger.info(
                f"[{symbol}] Using tick-based fallback: loss_per_lot=${loss_per_lot:.2f}"
            )
        elif loss_per_lot_path == "pip_value" and loss_per_lot:
            self.logger.warning(
                f"[{symbol}] Using pip-value fallback (less reliable): loss_per_lot=${loss_per_lot:.2f}"
            )

        if loss_per_lot is None or loss_per_lot <= 0:
            self.logger.warning(
                f"[{symbol}] Could not compute loss_per_lot; skipping trade"
            )
            _record_throttle("SKIPPED_NO_LOSS_CALC")
            _record_actionable(
                action="SKIPPED_NO_LOSS_CALC",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=None,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=None,
            )
            return

        # Raw volume for target risk
        volume_raw = target_risk_amount / loss_per_lot

        # If volume exceeds max_vol, optionally widen SL to fit within max volume (keeps risk constant)
        if volume_raw > max_vol and getattr(
            self.position_config, "auto_widen_sl", True
        ):
            widen_factor = 1.25
            max_iters = 6
            for _ in range(max_iters):
                dist = abs(entry_price - sl_price) * widen_factor
                sl_price = (entry_price - dist) if is_long else (entry_price + dist)
                # Preserve validated R-multiple: adjust TP proportionally
                if original_r_multiple > 0:
                    new_tp_dist = original_r_multiple * dist
                    tp_price = (
                        (entry_price + new_tp_dist)
                        if is_long
                        else (entry_price - new_tp_dist)
                    )
                loss_try, _loss_try_path = self._estimate_stop_loss_amount(
                    order_type.value,
                    broker_symbol,
                    1.0,
                    entry_price,
                    sl_price,
                    spec=spec,
                    symbol_info=symbol_info,
                    include_commission=True,
                )
                if loss_try is None or loss_try <= 0:
                    break
                loss_per_lot = loss_try
                volume_raw = target_risk_amount / loss_per_lot
                if volume_raw <= max_vol:
                    self.logger.debug(
                        f"[{symbol}] Widened SL to fit max volume constraint (max_vol={max_vol}), TP adjusted to preserve R={original_r_multiple:.2f}"
                    )
                    break

        risk_target_volume = volume_raw

        # The raw risk-sized volume is an exposure ceiling.  Cap it downward at
        # the configured/broker maximum, then floor it to the broker grid.  A
        # sub-minimum result is a no-order decision; never promote it to the
        # broker minimum because doing so exceeds the requested risk budget.
        volume_ceiling = min(max_vol, risk_target_volume)
        normalized_volume = self.mt5.normalize_volume(volume_ceiling, symbol_info)
        volume = _safe_float(normalized_volume)
        invalid_normalized_volume = (
            volume is None
            or not math.isfinite(volume)
            or volume <= 0.0
            or volume < min_vol - 1e-12
            or volume > volume_ceiling + 1e-12
            or volume > risk_target_volume + 1e-12
        )
        if invalid_normalized_volume:
            self.logger.info(
                f"[{symbol}] Trade blocked: no broker-valid volume fits at or "
                f"below requested volume {risk_target_volume:.8f} "
                f"(min={min_vol:.8f}, max={max_vol:.8f})"
            )
            _record_throttle("BLOCKED_MIN_LOT_EXCEEDS_CAP")
            _record_actionable(
                action="BLOCKED_MIN_LOT_EXCEEDS_CAP",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=(
                    volume
                    if volume is not None and math.isfinite(volume) and volume > 0.0
                    else None
                ),
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=None,
                context={
                    "risk_cap_reason": "no_broker_valid_volume_at_or_below_target",
                    "requested_volume": risk_target_volume,
                    "normalization_ceiling": volume_ceiling,
                    "normalized_volume": volume,
                    "min_volume": min_vol,
                    "max_volume": max_vol,
                },
            )
            return

        actual_risk_amount, _actual_risk_path = self._estimate_stop_loss_amount(
            order_type.value,
            broker_symbol,
            volume,
            entry_price,
            sl_price,
            spec=spec,
            symbol_info=symbol_info,
            include_commission=True,
        )
        if actual_risk_amount is None or actual_risk_amount <= 0:
            self.logger.warning(
                f"[{symbol}] Could not compute actual_risk_amount; skipping trade"
            )
            _record_throttle("SKIPPED_NO_LOSS_CALC")
            _record_actionable(
                action="SKIPPED_NO_LOSS_CALC",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=None,
            )
            return

        actual_risk_pct = (
            (actual_risk_amount / basis_value) * 100.0
            if basis_value > 0
            else float("inf")
        )
        max_risk_pct = float(getattr(self.position_config, "max_risk_pct", 2.0))
        risk_tolerance_pct = float(
            getattr(self.position_config, "risk_tolerance_pct", 2.0)
        )
        allowed_risk_pct = min(
            max_risk_pct,
            target_risk_pct * (1.0 + max(0.0, risk_tolerance_pct) / 100.0),
        )
        # Lot-normalization drift gate:
        # at/below target -> OK; inside target tolerance -> WARN;
        # outside target/tolerance/hard-cap envelope -> BLOCK.
        drift_severity = classify_lot_normalization_drift(
            actual_risk_pct=actual_risk_pct,
            target_risk_pct=target_risk_pct,
            max_risk_pct=max_risk_pct,
            tolerance_pct=risk_tolerance_pct,
        )
        if actual_risk_pct > target_risk_pct + 1e-9:
            if risk_target_volume < min_vol - 1e-9:
                drift_reason = "broker min-lot floor"
            elif volume > risk_target_volume + 1e-9:
                drift_reason = "volume-step normalization"
            else:
                drift_reason = "sizing normalization"
            drift_msg = (
                f"[{symbol}] Actual risk {actual_risk_pct:.2f}% is above effective target {target_risk_pct:.2f}% "
                f"(tolerance={risk_tolerance_pct:.1f}%, configured={configured_risk_pct:.2f}%) "
                f"due to {drift_reason} (requested_vol={risk_target_volume:.4f}, "
                f"placed_vol={volume:.4f}, hard_cap={max_risk_pct:.2f}%)"
            )
            if drift_severity == "warn":
                self.logger.warning(drift_msg)
            else:
                self.logger.info(drift_msg)
        if drift_severity == "block":
            risk_cap_action = (
                "BLOCKED_MIN_LOT_EXCEEDS_CAP"
                if risk_target_volume < min_vol - 1e-9
                else "SKIPPED_RISK_CAP"
            )
            self.logger.warning(
                f"[{symbol}] Skipping trade; actual risk {actual_risk_pct:.2f}% exceeds "
                f"allowed risk envelope {allowed_risk_pct:.2f}% "
                f"(configured={configured_risk_pct:.2f}%, effective_target={target_risk_pct:.2f}%, "
                f"vol={volume:.4f}, sl={sl_price:.5f})"
            )
            # ── Record into throttle so this is not repeated ─────────
            _record_throttle("SKIPPED_RISK_CAP")
            _record_actionable(
                action=risk_cap_action,
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "risk_cap_reason": drift_reason
                    if actual_risk_pct > target_risk_pct + 1e-9
                    else "hard_cap",
                    "requested_volume": risk_target_volume,
                    "min_volume": min_vol,
                },
            )
            return

        # ===== SAME-SYMBOL COMBINED RISK CHECK =====
        # Check if adding this trade would exceed max_combined_risk_pct for this symbol
        # This applies to ALL trades (not just secondary), preventing excessive exposure
        # when multiple positions exist on same symbol across different timeframes
        can_trade, risk_reason = self._check_symbol_combined_risk_cap(
            config.symbol,  # Canonical symbol for PM3: comment matching
            actual_risk_pct,  # Use actual risk (post-normalization) not target
            broker_symbol,  # Broker symbol for MT5 position lookup
            positions_snapshot=fresh_symbol_positions
            if fresh_symbol_positions is not None
            else positions_snapshot,
            account_info=account,
        )

        if not can_trade:
            self.logger.warning(f"[{symbol}] {risk_reason}")
            _record_throttle("BLOCKED_SYMBOL_RISK_CAP")
            _record_actionable(
                action="BLOCKED_SYMBOL_RISK_CAP",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
            )
            return

        self.logger.info(f"[{symbol}] {risk_reason}")

        margin_required = None
        margin_required_unavailable = False
        if self.mt5 is not None and hasattr(self.mt5, "calc_margin_required"):
            margin_required_raw = self.mt5.calc_margin_required(
                order_type.value,
                broker_symbol,
                volume,
                entry_price,
            )
            margin_required = self._safe_finite_float(margin_required_raw)
            margin_required_unavailable = (
                margin_required_raw is not None and margin_required is None
            )
        free_margin = self._safe_finite_float(getattr(account, "margin_free", None))
        has_free_margin = free_margin is not None
        if margin_required_unavailable:
            self.logger.warning(
                f"[{symbol}] Skipping trade; required margin calculation returned a non-finite value"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_UNAVAILABLE",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"required_margin": None, "free_margin": free_margin},
            )
            return
        if (
            margin_required is not None
            and margin_required > 0.0
            and not has_free_margin
        ):
            self.logger.warning(
                f"[{symbol}] Skipping trade; free margin unavailable while required margin is {margin_required:.2f}"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_UNAVAILABLE",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"required_margin": margin_required, "free_margin": None},
            )
            return
        if (
            margin_required is not None
            and margin_required > 0.0
            and has_free_margin
            and margin_required > free_margin
        ):
            self.logger.warning(
                f"[{symbol}] Skipping trade; required margin {margin_required:.2f} exceeds free margin {free_margin:.2f}"
            )
            _record_throttle("SKIPPED_MARGIN_REQUIRED")
            _record_actionable(
                action="SKIPPED_MARGIN_REQUIRED",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "required_margin": margin_required,
                    "free_margin": free_margin,
                },
            )
            return

        # Final executable quote refresh immediately before paper/live submit.
        # Signal discovery is bar-based, but order geometry must be anchored to
        # the latest broker quote so SL/TP, risk, and margin match the executable
        # surface as closely as possible.
        final_tick = self.mt5.get_symbol_tick(broker_symbol)
        if final_tick is None:
            self.logger.warning(
                f"[{symbol}] Skipping trade; final tick data unavailable"
            )
            _record_throttle("SKIPPED_NO_TICK")
            _record_actionable(
                action="SKIPPED_NO_TICK",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"stage": "final_quote_refresh"},
            )
            return
        final_entry_price = self._safe_finite_float(
            final_tick.ask if is_long else final_tick.bid
        )
        if final_entry_price is None or final_entry_price <= 0.0:
            self.logger.warning(
                f"[{symbol}] Skipping trade; final executable price unavailable"
            )
            _record_throttle("SKIPPED_NO_TICK")
            _record_actionable(
                action="SKIPPED_NO_TICK",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"stage": "final_quote_refresh"},
            )
            return

        previous_entry_price = entry_price
        sl_distance = abs(entry_price - sl_price)
        tp_distance = abs(tp_price - entry_price)
        if sl_distance <= 0.0 or tp_distance <= 0.0:
            self.logger.warning(
                f"[{symbol}] Skipping trade; invalid final SL/TP distance"
            )
            _record_throttle("SKIPPED_INVALID_STOPS")
            _record_actionable(
                action="SKIPPED_INVALID_STOPS",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
            )
            return

        entry_price = final_entry_price
        sl_price = (
            (entry_price - sl_distance) if is_long else (entry_price + sl_distance)
        )
        tp_price = (
            (entry_price + tp_distance) if is_long else (entry_price - tp_distance)
        )
        if min_stop_dist > 0 and sl_distance < min_stop_dist:
            if not bool(getattr(self.position_config, "auto_widen_sl", False)):
                _record_throttle("SKIPPED_BROKER_STOP_DISTANCE")
                _record_actionable(
                    action="SKIPPED_BROKER_STOP_DISTANCE",
                    entry_price_value=entry_price,
                    sl_value=sl_price,
                    tp_value=tp_price,
                    volume_value=volume,
                    target_risk_pct_value=target_risk_pct,
                    actual_risk_pct_value=actual_risk_pct,
                    context={
                        "stage": "final_quote_refresh",
                        "broker_min_stop_distance": min_stop_dist,
                        "frozen_stop_distance": sl_distance,
                    },
                )
                return
            prior_sl_distance = sl_distance
            final_r_multiple = (
                (tp_distance / prior_sl_distance) if prior_sl_distance > 0.0 else 0.0
            )
            sl_distance = min_stop_dist
            sl_price = (
                (entry_price - sl_distance) if is_long else (entry_price + sl_distance)
            )
            if final_r_multiple > 0.0:
                tp_distance = final_r_multiple * sl_distance
                tp_price = (
                    (entry_price + tp_distance)
                    if is_long
                    else (entry_price - tp_distance)
                )

        actual_risk_amount, _actual_risk_path = self._estimate_stop_loss_amount(
            order_type.value,
            broker_symbol,
            volume,
            entry_price,
            sl_price,
            spec=spec,
            symbol_info=symbol_info,
            include_commission=True,
        )
        if actual_risk_amount is None or actual_risk_amount <= 0:
            self.logger.warning(
                f"[{symbol}] Could not compute final actual_risk_amount; skipping trade"
            )
            _record_throttle("SKIPPED_NO_LOSS_CALC")
            _record_actionable(
                action="SKIPPED_NO_LOSS_CALC",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=None,
                context={"stage": "final_quote_refresh"},
            )
            return
        actual_risk_pct = (
            (actual_risk_amount / basis_value) * 100.0
            if basis_value > 0
            else float("inf")
        )
        drift_severity = classify_lot_normalization_drift(
            actual_risk_pct=actual_risk_pct,
            target_risk_pct=target_risk_pct,
            max_risk_pct=max_risk_pct,
            tolerance_pct=risk_tolerance_pct,
        )
        if drift_severity == "block":
            risk_cap_action = (
                "BLOCKED_MIN_LOT_EXCEEDS_CAP"
                if risk_target_volume < min_vol - 1e-9
                else "SKIPPED_RISK_CAP"
            )
            self.logger.warning(
                f"[{symbol}] Skipping trade after final quote refresh; actual risk "
                f"{actual_risk_pct:.2f}% exceeds allowed risk envelope {allowed_risk_pct:.2f}% "
                f"(vol={volume:.4f}, entry={entry_price:.5f}, sl={sl_price:.5f})"
            )
            _record_throttle("SKIPPED_RISK_CAP")
            _record_actionable(
                action=risk_cap_action,
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"stage": "final_quote_refresh"},
            )
            return

        _pip_size_exec = float(getattr(spec, "pip_size", 0.0) or 0.0) or 1e-5
        _entry_adjustment_pips = (
            abs(float(entry_price) - float(previous_entry_price)) / _pip_size_exec
        )
        _slippage_baseline = max(float(getattr(spec, "spread_avg", 0.0) or 0.0), 1e-9)
        final_drift_decision = cycle_drift_monitor.evaluate(
            CycleDriftContext(
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                strategy_name=_strat_name,
                execution_slippage_mult=_entry_adjustment_pips / _slippage_baseline,
                spread_degradation_mult=_cycle_spread_degradation_mult,
                metadata={"stage": "final_quote_refresh"},
            )
        )
        if final_drift_decision.reasons:
            decision_context["cycle_drift_final"] = {
                "action": final_drift_decision.action,
                "reasons": list(final_drift_decision.reasons),
                "observed_only": bool(final_drift_decision.observed_only),
            }
        if not final_drift_decision.allow_trade or (
            final_drift_decision.risk_multiplier < 1.0
            and not final_drift_decision.observed_only
        ):
            self.logger.warning(
                f"[{symbol}] Trade blocked by final cycle drift check: "
                f"{'; '.join(final_drift_decision.reasons)}"
            )
            _record_throttle("BLOCKED_CYCLE_DRIFT")
            _record_actionable(
                action="BLOCKED_CYCLE_DRIFT",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"cycle_drift": decision_context.get("cycle_drift_final")},
            )
            return

        final_open_positions_for_supervisor = open_positions_for_supervisor
        if self.enable_trading and hasattr(self.mt5, "get_positions"):
            try:
                refreshed_final_positions = self.mt5.get_positions()
            except Exception:
                refreshed_final_positions = None
            if refreshed_final_positions is None:
                self.logger.warning(
                    f"[{symbol}] Trade skipped: final portfolio position snapshot unavailable"
                )
                _record_throttle("BLOCKED_PORTFOLIO_SNAPSHOT_UNAVAILABLE")
                _record_actionable(
                    action="BLOCKED_PORTFOLIO_SNAPSHOT_UNAVAILABLE",
                    entry_price_value=entry_price,
                    sl_value=sl_price,
                    tp_value=tp_price,
                    volume_value=volume,
                    target_risk_pct_value=target_risk_pct,
                    actual_risk_pct_value=actual_risk_pct,
                    context={
                        "institutional_supervisor": {"stage": "final_quote_refresh"}
                    },
                )
                return
            final_open_positions_for_supervisor = list(refreshed_final_positions or [])
        final_total_open_risk_pct = self._estimate_total_open_risk_pct(
            final_open_positions_for_supervisor,
            account,
            default_risk_pct=configured_risk_pct,
        )
        final_supervisor = getattr(
            self,
            "_institutional_supervisor",
            InstitutionalSupervisor(self.pipeline_config),
        )
        final_supervisor_decision = final_supervisor.evaluate(
            SupervisorContext(
                symbol=symbol,
                timeframe=_tf,
                regime=_regime,
                strategy_name=_strat_name,
                total_open_risk_pct=final_total_open_risk_pct,
                new_trade_risk_pct=actual_risk_pct,
                max_total_stop_risk_pct=_cap_env[
                    "max_total_stop_risk_pct"
                ],
                daily_loss_pct=self._estimate_realized_loss_pct("daily", account),
                session_loss_pct=self._estimate_realized_loss_pct("session", account),
                weekly_loss_pct=self._estimate_realized_loss_pct("weekly", account),
                open_drawdown_pct=self._estimate_open_drawdown_pct(account),
                margin_level=margin_level or 0.0,
                metadata={
                    "stage": "final_quote_refresh",
                    "cycle_id": supervisor_cycle_id,
                },
            )
        )
        if final_supervisor_decision.reasons:
            decision_context["institutional_supervisor_final"] = {
                "action": final_supervisor_decision.action,
                "reasons": list(final_supervisor_decision.reasons),
                "observed_only": bool(final_supervisor_decision.observed_only),
                "total_open_risk_pct": final_total_open_risk_pct,
            }
        if not final_supervisor_decision.allow_trade or (
            final_supervisor_decision.risk_multiplier < 1.0
            and not final_supervisor_decision.observed_only
        ):
            self.logger.warning(
                f"[{symbol}] Trade blocked by final institutional supervisor check: "
                f"{'; '.join(final_supervisor_decision.reasons)}"
            )
            _record_throttle("BLOCKED_INSTITUTIONAL_SUPERVISOR")
            _record_actionable(
                action="BLOCKED_INSTITUTIONAL_SUPERVISOR",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "institutional_supervisor": decision_context.get(
                        "institutional_supervisor_final"
                    )
                },
            )
            return

        can_trade, risk_reason = self._check_symbol_combined_risk_cap(
            config.symbol,
            actual_risk_pct,
            broker_symbol,
            positions_snapshot=fresh_symbol_positions
            if fresh_symbol_positions is not None
            else positions_snapshot,
            account_info=account,
        )
        if not can_trade:
            self.logger.warning(f"[{symbol}] {risk_reason}")
            _record_throttle("BLOCKED_SYMBOL_RISK_CAP")
            _record_actionable(
                action="BLOCKED_SYMBOL_RISK_CAP",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"stage": "final_quote_refresh"},
            )
            return

        if abs(entry_price - previous_entry_price) > 1e-12:
            self.logger.debug(
                f"[{symbol}] Final quote refresh adjusted entry "
                f"{previous_entry_price:.5f} -> {entry_price:.5f}"
            )

        if self.mt5 is not None and hasattr(self.mt5, "calc_margin_required"):
            margin_required_raw = self.mt5.calc_margin_required(
                order_type.value,
                broker_symbol,
                volume,
                entry_price,
            )
            margin_required = self._safe_finite_float(margin_required_raw)
            margin_required_unavailable = (
                margin_required_raw is not None and margin_required is None
            )
        if margin_required_unavailable:
            self.logger.warning(
                f"[{symbol}] Skipping trade; final required margin calculation returned a non-finite value"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_UNAVAILABLE",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "required_margin": None,
                    "free_margin": free_margin,
                    "stage": "final_quote_refresh",
                },
            )
            return
        if (
            margin_required is not None
            and margin_required > 0.0
            and not has_free_margin
        ):
            self.logger.warning(
                f"[{symbol}] Skipping trade; free margin unavailable while final required margin is {margin_required:.2f}"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_UNAVAILABLE",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "required_margin": margin_required,
                    "free_margin": None,
                    "stage": "final_quote_refresh",
                },
            )
            return
        if (
            margin_required is not None
            and margin_required > 0.0
            and has_free_margin
            and margin_required > free_margin
        ):
            self.logger.warning(
                f"[{symbol}] Skipping trade; final required margin {margin_required:.2f} exceeds free margin {free_margin:.2f}"
            )
            _record_throttle("SKIPPED_MARGIN_REQUIRED")
            _record_actionable(
                action="SKIPPED_MARGIN_REQUIRED",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "required_margin": margin_required,
                    "free_margin": free_margin,
                    "stage": "final_quote_refresh",
                },
            )
            return

        # Log risk details for auditability
        self.logger.info(
            f"[{symbol}] {order_type.name} | basis={basis_value:.2f} ({basis_pref}) | "
            f"configured_risk={configured_risk_pct:.2f}% | "
            f"effective_target={target_risk_pct:.2f}% (${target_risk_amount:.2f}) | "
            f"actual_risk={actual_risk_pct:.2f}% (${actual_risk_amount:.2f}) | "
            f"hard_cap={max_risk_pct:.2f}% | "
            f"vol_raw={volume_raw:.4f} | vol={volume:.4f} | entry={entry_price:.5f} | sl={sl_price:.5f} | tp={tp_price:.5f}"
        )

        if not self.enable_trading:
            self.logger.info(
                f"[{symbol}] [PAPER] Would execute {direction}: "
                f"{volume:.4f} lots @ {entry_price:.5f} | SL={sl_price:.5f} | TP={tp_price:.5f}"
            )
            self._log_trade(
                symbol,
                direction,
                volume,
                entry_price,
                sl_price,
                tp_price,
                magic,
                "PAPER",
                candidate_id=execution_candidate_id,
            )
            _record_throttle("PAPER")
            _record_actionable(
                action="PAPER",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
            )
            return

        def _block_pre_send(
            action: str, reason: str, context: Optional[Dict[str, Any]] = None
        ) -> None:
            book_label = (
                "current-refit-demo"
                if bool(getattr(self, "_is_current_refit_demo", False))
                else "funded"
            )
            self.logger.warning(
                f"[{symbol}] {book_label} entry blocked immediately pre-send: {reason}"
            )
            _record_throttle(action)
            _record_actionable(
                action=action,
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={
                    "stage": "immediate_pre_send",
                    "reason": reason,
                    **(context or {}),
                },
            )

        # Complete the expensive whole-ledger/native authorization before the
        # final broker snapshot.  Nothing between the subsequent account,
        # position, stop-risk, margin and loss checks and send may perform a
        # full artifact-tree walk: otherwise broker state could change while
        # authorization hashes hundreds of immutable sources.
        if (
            bool(getattr(self, "_is_current_refit_demo", False))
            and not execution_candidate_id
        ):
            _block_pre_send(
                "BLOCKED_CURRENT_REFIT_CELL_ID",
                "current-refit winner lacks its bound cell id",
            )
            return
        promotion_block_reason = self._promotion_authorization_block_reason(
            symbol,
            _tf,
            _regime,
        )
        if promotion_block_reason:
            self.logger.error(
                f"[{symbol}] Skipping trade; immutable promotion authorization rejected: "
                f"{promotion_block_reason}"
            )
            _record_throttle("BLOCKED_PROMOTION_AUTHORIZATION")
            _record_actionable(
                action="BLOCKED_PROMOTION_AUTHORIZATION",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"promotion_authorization_error": promotion_block_reason},
            )
            return

        final_manual_block_reason = self._manual_new_entries_block_reason()
        if final_manual_block_reason:
            _block_pre_send(
                "BLOCKED_MANUAL_KILL_SWITCH",
                final_manual_block_reason,
            )
            return

        fresh_account, fresh_account_error = self._fresh_entry_account_validation(
            account
        )
        if fresh_account_error or fresh_account is None:
            _block_pre_send(
                "BLOCKED_FRESH_ACCOUNT",
                fresh_account_error or "fresh account validation failed",
            )
            return
        fresh_basis = self._safe_finite_float(
            getattr(
                fresh_account,
                "balance" if basis_pref == "balance" else "equity",
                None,
            )
        )
        if fresh_basis is None or fresh_basis <= 0.0:
            _block_pre_send(
                "BLOCKED_FRESH_ACCOUNT",
                f"fresh {basis_pref} risk basis unavailable",
            )
            return
        fresh_actual_risk_pct = float(actual_risk_amount) / fresh_basis * 100.0

        try:
            fresh_pre_send_positions = self.mt5.get_positions()
        except Exception as exc:
            fresh_pre_send_positions = None
            fresh_positions_error = f"{type(exc).__name__}: {exc}"
        else:
            fresh_positions_error = ""
        if fresh_pre_send_positions is None:
            _block_pre_send(
                "BLOCKED_PORTFOLIO_SNAPSHOT_UNAVAILABLE",
                f"fresh position snapshot unavailable {fresh_positions_error}".strip(),
            )
            return

        if bool(getattr(self, "_is_current_refit_demo", False)):
            dedicated_book_reason = self._current_refit_demo_position_block_reason(
                list(fresh_pre_send_positions or []),
                self._get_management_configs(),
            )
            if dedicated_book_reason:
                _block_pre_send(
                    "BLOCKED_CURRENT_REFIT_POSITION_BOOK",
                    dedicated_book_reason,
                )
                return

        # Re-apply exact-cell uniqueness and account attribution to the final
        # broker snapshot. Aggregate risk remains the signed envelope's job.
        final_symbol_positions = [
            position
            for position in list(fresh_pre_send_positions or [])
            if getattr(position, "symbol", None) == broker_symbol
        ]
        final_position_block = self._same_symbol_specialist_position_block(
            symbol,
            config,
            final_symbol_positions,
            fresh_account,
            candidate_magic=magic,
        )
        if final_position_block:
            _block_pre_send(*final_position_block)
            return

        # Shared-account hierarchy: every open position and the candidate are
        # measured from replay-equivalent conservative capital:
        # min(fresh MTM equity, fresh balance - reconstructed open stop loss).
        # Gross factor risk is deliberately not direction-netted. Margin is
        # evaluated separately and is never reported as loss-at-stop risk.
        if isinstance(self.pipeline_config, PipelineConfig):
            (
                open_stop_exposures,
                conservative_capital,
                exposure_error,
            ) = self._build_live_stop_risk_exposures(
                list(fresh_pre_send_positions or []),
                fresh_account,
                candidate_symbol=config.symbol,
                candidate_broker_symbol=broker_symbol,
            )
            if exposure_error:
                _block_pre_send(
                    "BLOCKED_GROSS_STOP_RISK_INPUT",
                    exposure_error,
                )
                return
            if conservative_capital is None or conservative_capital <= 0.0:
                _block_pre_send(
                    "BLOCKED_GROSS_STOP_RISK_INPUT",
                    "positive conservative risk capital unavailable",
                )
                return

            # Replay sizes each chronological admission from this same capital.
            # Funded live may only reduce the already-computed volume at this
            # last snapshot; it never increases exposure immediately pre-send.
            final_loss_per_lot = float(actual_risk_amount) / float(volume)
            conservative_target_amount = conservative_capital * target_risk_pct / 100.0
            conservative_raw_volume = conservative_target_amount / final_loss_per_lot
            if conservative_raw_volume < min_vol - 1e-12:
                _block_pre_send(
                    "BLOCKED_MIN_LOT_EXCEEDS_CAP",
                    "broker minimum volume exceeds conservative risk-capital target",
                    {
                        "conservative_risk_capital": conservative_capital,
                        "conservative_raw_volume": conservative_raw_volume,
                        "minimum_volume": min_vol,
                    },
                )
                return
            normalized_conservative_raw = self.mt5.normalize_volume(
                conservative_raw_volume,
                symbol_info,
            )
            normalized_conservative_volume = _safe_float(normalized_conservative_raw)
            if (
                normalized_conservative_volume is None
                or not math.isfinite(normalized_conservative_volume)
                or normalized_conservative_volume <= 0.0
                or normalized_conservative_volume < min_vol - 1e-12
                or normalized_conservative_volume > conservative_raw_volume + 1e-12
            ):
                _block_pre_send(
                    "BLOCKED_MIN_LOT_EXCEEDS_CAP",
                    "no positive broker-valid volume fits conservative risk capital",
                    {
                        "conservative_risk_capital": conservative_capital,
                        "conservative_raw_volume": conservative_raw_volume,
                        "normalized_conservative_volume": normalized_conservative_volume,
                        "minimum_volume": min_vol,
                    },
                )
                return
            conservative_volume = min(
                float(volume),
                normalized_conservative_volume,
            )
            if conservative_volume < float(volume) - 1e-12:
                volume = conservative_volume
                actual_risk_amount, _actual_risk_path = self._estimate_stop_loss_amount(
                    order_type.value,
                    broker_symbol,
                    volume,
                    entry_price,
                    sl_price,
                    spec=spec,
                    symbol_info=symbol_info,
                    include_commission=True,
                )
                if (
                    actual_risk_amount is None
                    or not math.isfinite(float(actual_risk_amount))
                    or actual_risk_amount <= 0.0
                ):
                    _block_pre_send(
                        "BLOCKED_GROSS_STOP_RISK_INPUT",
                        "resized candidate stop risk is unavailable",
                    )
                    return
            candidate_conservative_stop_risk_pct = (
                float(actual_risk_amount) / conservative_capital * 100.0
            )
            conservative_drift = classify_lot_normalization_drift(
                actual_risk_pct=candidate_conservative_stop_risk_pct,
                target_risk_pct=target_risk_pct,
                max_risk_pct=max_risk_pct,
                tolerance_pct=risk_tolerance_pct,
            )
            if conservative_drift == "block":
                _block_pre_send(
                    "SKIPPED_RISK_CAP",
                    (
                        f"conservative-capital candidate risk "
                        f"{candidate_conservative_stop_risk_pct:.4f}% exceeds the "
                        "per-trade envelope"
                    ),
                    {"conservative_risk_capital": conservative_capital},
                )
                return
            # From this point all audit percentages and the order volume refer
            # to the final conservative-capital sizing decision.
            actual_risk_pct = candidate_conservative_stop_risk_pct

            same_symbol_open_risk_pct = sum(
                exposure.stop_risk_pct
                for exposure in open_stop_exposures
                if str(exposure.symbol).upper() == str(config.symbol).upper()
            )
            _cap_env = self._bound_capital_admission_envelope()
            if _cap_env is None:
                _block_pre_send(
                    "BLOCKED_CAPITAL_ENVELOPE_UNAVAILABLE",
                    "no signed selected capital envelope bound for admission",
                    {"execution_book": self.execution_book},
                )
                return
            max_same_symbol_risk_pct = _cap_env["max_same_symbol_stop_risk_pct"]
            if (
                max_same_symbol_risk_pct <= 0.0
                or same_symbol_open_risk_pct + actual_risk_pct
                > max_same_symbol_risk_pct + 1e-10
            ):
                _block_pre_send(
                    "BLOCKED_SYMBOL_RISK_CAP",
                    "same-symbol conservative stop risk exceeds the selected cap",
                    {
                        "existing_same_symbol_risk_pct": same_symbol_open_risk_pct,
                        "candidate_risk_pct": actual_risk_pct,
                        "max_same_symbol_risk_pct": max_same_symbol_risk_pct,
                    },
                )
                return
            candidate_asset_class = str(
                getattr(spec, "asset_class", "unknown") or "unknown"
            )
            gross_risk_decision = evaluate_gross_stop_risk(
                open_stop_exposures,
                StopRiskExposure(
                    symbol=config.symbol,
                    stop_risk_pct=actual_risk_pct,
                    asset_class=candidate_asset_class,
                    exposure_id=f"candidate:{config.symbol}:{_tf}:{_regime}",
                ),
                GrossRiskLimits(
                    max_total_stop_risk_pct=_cap_env["max_total_stop_risk_pct"],
                    max_factor_group_stop_risk_pct=_cap_env[
                        "max_factor_stop_risk_pct"
                    ],
                ),
            )
            decision_context["gross_stop_risk_admission"] = asdict(gross_risk_decision)
            decision_context["conservative_risk_capital"] = {
                "risk_capital": conservative_capital,
                "denominator": "min(fresh_equity, fresh_balance - reconstructed_open_stop_risk)",
            }
            if not gross_risk_decision.allow:
                _block_pre_send(
                    "BLOCKED_GROSS_STOP_RISK_ADMISSION",
                    gross_risk_decision.reason,
                    {"gross_stop_risk_admission": asdict(gross_risk_decision)},
                )
                return

            # Volume may have been reduced after the earlier margin estimate.
            if not hasattr(self.mt5, "calc_margin_required"):
                _block_pre_send(
                    "SKIPPED_MARGIN_UNAVAILABLE",
                    "broker margin calculator unavailable after conservative resizing",
                )
                return
            margin_required_raw = self.mt5.calc_margin_required(
                order_type.value,
                broker_symbol,
                volume,
                entry_price,
            )
            margin_required = self._safe_finite_float(margin_required_raw)
            if margin_required is None or margin_required <= 0.0:
                _block_pre_send(
                    "SKIPPED_MARGIN_UNAVAILABLE",
                    "fresh required margin unavailable after conservative resizing",
                )
                return

        fresh_margin_free = self._safe_finite_float(
            getattr(fresh_account, "margin_free", None)
        )
        if isinstance(self.pipeline_config, PipelineConfig):
            _margin_cap_env = self._bound_capital_admission_envelope()
            if _margin_cap_env is None:
                _block_pre_send(
                    "BLOCKED_CAPITAL_ENVELOPE_UNAVAILABLE",
                    "no signed selected capital envelope bound for margin admission",
                    {"execution_book": self.execution_book},
                )
                return
            # Margin admission reads the selected envelope, never static config.
            max_margin_utilization_pct = _margin_cap_env["max_margin_utilization_pct"]
            projected_margin_decision = evaluate_projected_margin(
                equity=getattr(fresh_account, "equity", None),
                current_used_margin=getattr(fresh_account, "margin", None),
                candidate_required_margin=margin_required,
                max_margin_utilization_pct=max_margin_utilization_pct,
            )
            decision_context["projected_margin_admission"] = asdict(
                projected_margin_decision
            )
            if not projected_margin_decision.allow:
                _block_pre_send(
                    "BLOCKED_PROJECTED_MARGIN_ADMISSION",
                    projected_margin_decision.reason,
                    {"projected_margin_admission": asdict(projected_margin_decision)},
                )
                return
        if margin_required is not None and margin_required > 0.0:
            if fresh_margin_free is None:
                _block_pre_send(
                    "SKIPPED_MARGIN_UNAVAILABLE",
                    "fresh free margin unavailable for a margin-requiring order",
                    {"required_margin": margin_required},
                )
                return
            if margin_required > fresh_margin_free:
                _block_pre_send(
                    "SKIPPED_MARGIN_REQUIRED",
                    (
                        f"required margin {margin_required:.2f} exceeds fresh free "
                        f"margin {fresh_margin_free:.2f}"
                    ),
                    {
                        "required_margin": margin_required,
                        "fresh_margin_free": fresh_margin_free,
                    },
                )
                return

        hard_loss_reasons = self._realized_loss_hard_block_reasons(fresh_account)
        if hard_loss_reasons:
            _block_pre_send(
                "BLOCKED_REALIZED_LOSS_LIMIT",
                "; ".join(hard_loss_reasons),
            )
            return

        # From this point onward, audit and any post-fill checks use the exact
        # account snapshot that authorized the send.
        account = fresh_account
        if isinstance(self.pipeline_config, PipelineConfig):
            basis_value = conservative_capital
            target_risk_amount = conservative_capital * (target_risk_pct / 100.0)
        else:
            basis_value = fresh_basis
            actual_risk_pct = fresh_actual_risk_pct
            target_risk_amount = fresh_basis * (target_risk_pct / 100.0)
        free_margin = fresh_margin_free

        # Encode enough metadata for later position attribution.
        trade_comment = TradeTagEncoder.encode_comment(
            symbol=config.symbol,  # Use original symbol, not broker symbol
            timeframe=_tf,
            strategy_name=_strat_name,
            direction=direction,
            risk_pct=max(target_risk_pct, actual_risk_pct),
            book_role=execution_book_role,
        )

        def _entry_slippage_values(fill_price_candidate: Any) -> Dict[str, Any]:
            values = {
                "expected_entry_price_value": None,
                "filled_entry_price_value": None,
                "entry_slippage_pips_value": None,
                "entry_slippage_r_value": None,
                "entry_slippage_direction_value": None,
            }
            try:
                expected_entry = float(entry_price)
                filled_entry = float(fill_price_candidate)
                stop_for_r = float(sl_price)
                pip_size_exec = float(getattr(spec, "pip_size", 0.0) or 0.0)
            except Exception:
                return values
            if (
                not math.isfinite(expected_entry)
                or not math.isfinite(filled_entry)
                or not math.isfinite(stop_for_r)
                or expected_entry <= 0.0
                or filled_entry <= 0.0
                or pip_size_exec <= 0.0
            ):
                return values

            adverse_slippage_pips = (
                (filled_entry - expected_entry) / pip_size_exec
                if is_long
                else (expected_entry - filled_entry) / pip_size_exec
            )
            entry_stop_pips = abs(filled_entry - stop_for_r) / pip_size_exec
            slippage_direction = "flat"
            if adverse_slippage_pips > 1e-9:
                slippage_direction = "adverse"
            elif adverse_slippage_pips < -1e-9:
                slippage_direction = "favorable"

            values.update(
                {
                    "expected_entry_price_value": expected_entry,
                    "filled_entry_price_value": filled_entry,
                    "entry_slippage_pips_value": adverse_slippage_pips,
                    "entry_slippage_r_value": (
                        adverse_slippage_pips / entry_stop_pips
                        if entry_stop_pips > 0.0
                        else None
                    ),
                    "entry_slippage_direction_value": slippage_direction,
                }
            )
            return values

        reservation_id = ""

        def _set_live_outcome_error(prefix: str, exc: Exception) -> None:
            self._live_outcome_feedback_error = (
                f"{prefix}: {type(exc).__name__}: {exc}"
            )
            self.logger.error(f"[{symbol}] {self._live_outcome_feedback_error}")

        def _retain_ambiguous_reservation(
            reason: str, *, broker_order_ticket: int = 0
        ) -> None:
            ledger = getattr(self, "_live_outcome_ledger", None)
            if ledger is None or not reservation_id:
                return
            try:
                ledger.retain_ambiguous_pre_send_intent(
                    reservation_id,
                    reason=reason,
                    broker_order_ticket=broker_order_ticket,
                )
            except Exception as exc:
                _set_live_outcome_error("ambiguous reservation persistence failed", exc)

        def _bind_live_entry_intent(
            *,
            position_id: int = 0,
            deal_ticket: int = 0,
            order_ticket: int = 0,
            fill_price: float,
            fill_volume: float,
            risk_cash: Optional[float],
        ) -> bool:
            ledger = getattr(self, "_live_outcome_ledger", None)
            if ledger is None:
                return True
            try:
                if not reservation_id:
                    raise LiveOutcomeError("pre-send reservation is missing")
                ledger.bind_pre_send_intent(
                    reservation_id,
                    position_id=position_id,
                    opening_deal_ticket=deal_ticket,
                    opening_order_ticket=order_ticket,
                    fill_price=fill_price,
                    fill_volume=fill_volume,
                    risk_cash=risk_cash,
                )
                return True
            except Exception as exc:
                _retain_ambiguous_reservation(
                    "filled order could not be durably bound",
                    broker_order_ticket=max(0, int(order_ticket or 0)),
                )
                _set_live_outcome_error("entry intent binding failed", exc)
                return False

        def _cancel_definitive_reservation(result_value: Any) -> None:
            ledger = getattr(self, "_live_outcome_ledger", None)
            if ledger is None or not reservation_id:
                return
            try:
                ledger.cancel_pre_send_intent(
                    reservation_id,
                    definitive_no_fill=True,
                    reason=(
                        f"broker rejected order with retcode "
                        f"{getattr(result_value, 'retcode', 'unknown')}"
                    ),
                )
            except Exception as exc:
                _set_live_outcome_error("reservation cancellation failed", exc)

        # Artifact authorization was completed before the final broker-state
        # transaction above.  Recheck the broker-authoritative DEMO mode from
        # that final account snapshot, then submit without another heavyweight
        # filesystem operation reopening the capital/identity race.
        if bool(getattr(self, "_is_current_refit_demo", False)):
            demo_mode_reason = self._current_refit_demo_account_block_reason(account)
            if demo_mode_reason:
                _block_pre_send("BLOCKED_CURRENT_REFIT_DEMO_ACCOUNT", demo_mode_reason)
                return

        management_capsule_id = ""
        management_generation_id = ""
        if bool(getattr(self, "_is_current_refit_demo", False)):
            management_capsule_id = str(
                getattr(self, "_demo_management_capsule_id", "") or ""
            )
            management_generation_id = str(
                getattr(self, "_demo_management_generation_id", "") or ""
            )
            if (
                not self._plain_sha256(management_capsule_id)
                or not management_generation_id
                or management_generation_id != management_generation_id.strip()
                or len(management_generation_id.encode("utf-8")) > 256
                or any(ord(character) < 32 for character in management_generation_id)
            ):
                _block_pre_send(
                    "BLOCKED_DEMO_MANAGEMENT_AUTHORITY",
                    "exact demo management capsule and generation are unavailable",
                )
                return

        outcome_ledger = getattr(self, "_live_outcome_ledger", None)
        durable_feedback_required = bool(
            getattr(self, "_live_outcome_feedback_initialized", False)
        ) or bool(getattr(self, "_is_current_refit_demo", False))
        if outcome_ledger is None and durable_feedback_required:
            _block_pre_send(
                "BLOCKED_PRE_SEND_RESERVATION",
                "durable live outcome ledger is unavailable",
            )
            return
        if outcome_ledger is not None:
            try:
                reservation = outcome_ledger.reserve_pre_send_intent(
                    symbol=symbol,
                    timeframe=_tf,
                    regime=_regime,
                    strategy_name=_strat_name,
                    magic=magic,
                    entry_price=float(entry_price),
                    volume=float(volume),
                    initial_stop=float(sl_price),
                    risk_cash=float(actual_risk_amount),
                    book_role=execution_book_role,
                    candidate_id=execution_candidate_id,
                    management_capsule_id=management_capsule_id,
                    management_generation_id=management_generation_id,
                )
                reservation_id = str(reservation.get("reservation_id") or "")
                if (
                    len(reservation_id) != 32
                    or reservation_id != reservation_id.lower()
                    or any(char not in "0123456789abcdef" for char in reservation_id)
                ):
                    raise LiveOutcomeError("invalid durable reservation identity")
            except Exception as exc:
                _set_live_outcome_error("pre-send reservation failed", exc)
                _block_pre_send(
                    "BLOCKED_PRE_SEND_RESERVATION",
                    "durable pre-send attribution could not be persisted",
                )
                return

        try:
            result = self.mt5.send_market_order(
                symbol=broker_symbol,
                order_type=order_type,
                volume=volume,
                sl=sl_price,
                tp=tp_price,
                deviation=30,
                magic=magic,
                comment=trade_comment,
                price=entry_price,
                symbol_info=symbol_info,
            )
        except Exception as exc:
            self.logger.error(
                f"[{symbol}] Broker send raised {type(exc).__name__}: {exc}"
            )
            result = MT5OrderResult(
                success=False,
                retcode=-1,
                retcode_description=f"send raised {type(exc).__name__}",
                error_message=type(exc).__name__,
                ambiguous=True,
            )

        if result.success:
            self.logger.info(
                f"[OK] [{symbol}] {direction} executed: "
                f"{result.volume} lots @ {result.price:.5f}"
            )
            self._log_trade(
                symbol,
                direction,
                result.volume,
                result.price,
                sl_price,
                tp_price,
                magic,
                "EXECUTED",
                candidate_id=execution_candidate_id,
            )
            # Record order time for rate limiting
            order_time = datetime.now()
            self._last_order_times[rate_limit_key] = order_time
            self._last_order_times[symbol] = order_time  # diagnostic summary
            _record_throttle("EXECUTED")
            actual_risk_pct_exec = actual_risk_pct
            risk_exec: Optional[float] = None
            try:
                risk_exec, _risk_exec_path = self._estimate_stop_loss_amount(
                    order_type.value,
                    broker_symbol,
                    result.volume,
                    result.price,
                    sl_price,
                    spec=spec,
                    symbol_info=symbol_info,
                    include_commission=True,
                )
                if risk_exec is not None and risk_exec > 0 and basis_value > 0:
                    actual_risk_pct_exec = (risk_exec / basis_value) * 100.0
            except Exception:
                pass
            _bind_live_entry_intent(
                deal_ticket=int(getattr(result, "deal", 0) or 0),
                order_ticket=int(getattr(result, "order", 0) or 0),
                fill_price=float(result.price),
                fill_volume=float(result.volume),
                risk_cash=risk_exec,
            )
            _record_actionable(
                action="EXECUTED",
                entry_price_value=result.price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=result.volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct_exec,
                **_entry_slippage_values(result.price),
            )
        else:
            if _is_ambiguous_order_result(result) or not _is_definitive_no_fill_result(
                result
            ):
                reconciled_position = _find_live_position_by_magic()
                if _looks_like_position(reconciled_position):
                    self.logger.warning(
                        f"[{symbol}] Ambiguous order result reconciled to an open position "
                        f"(retcode={getattr(result, 'retcode', None)}, magic={magic})"
                    )
                    self._log_trade(
                        symbol,
                        direction,
                        float(getattr(reconciled_position, "volume", volume) or volume),
                        float(
                            getattr(reconciled_position, "price_open", entry_price)
                            or entry_price
                        ),
                        sl_price,
                        tp_price,
                        magic,
                        "EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND",
                        candidate_id=execution_candidate_id,
                    )
                    order_time = datetime.now()
                    self._last_order_times[rate_limit_key] = order_time
                    self._last_order_times[symbol] = order_time  # diagnostic summary
                    reconciled_volume = float(
                        getattr(reconciled_position, "volume", volume) or volume
                    )
                    reconciled_price = float(
                        getattr(reconciled_position, "price_open", entry_price)
                        or entry_price
                    )
                    reconciled_risk: Optional[float] = None
                    try:
                        reconciled_risk, _ = self._estimate_stop_loss_amount(
                            order_type.value,
                            broker_symbol,
                            reconciled_volume,
                            reconciled_price,
                            sl_price,
                            spec=spec,
                            symbol_info=symbol_info,
                            include_commission=True,
                        )
                    except Exception:
                        pass
                    _bind_live_entry_intent(
                        position_id=int(
                            getattr(reconciled_position, "identifier", 0)
                            or getattr(reconciled_position, "ticket", 0)
                            or 0
                        ),
                        fill_price=reconciled_price,
                        fill_volume=reconciled_volume,
                        risk_cash=reconciled_risk,
                    )
                    ambiguous_blocks = getattr(self, "_ambiguous_order_block_until", {})
                    ambiguous_blocks.pop(symbol, None)
                    _record_throttle("EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND")
                    _record_actionable(
                        action="EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND",
                        entry_price_value=float(
                            getattr(reconciled_position, "price_open", entry_price)
                            or entry_price
                        ),
                        sl_value=sl_price,
                        tp_value=tp_price,
                        volume_value=float(
                            getattr(reconciled_position, "volume", volume) or volume
                        ),
                        target_risk_pct_value=target_risk_pct,
                        actual_risk_pct_value=actual_risk_pct,
                        **_entry_slippage_values(
                            float(
                                getattr(reconciled_position, "price_open", entry_price)
                                or entry_price
                            )
                        ),
                        context={
                            "ambiguous_retcode": getattr(result, "retcode", None),
                            "position_ticket": getattr(
                                reconciled_position, "ticket", None
                            ),
                        },
                    )
                    return

                cooldown = int(
                    max(
                        self.ORDER_RATE_LIMIT_SECONDS,
                        float(
                            getattr(
                                self.pipeline_config,
                                "ambiguous_order_cooldown_seconds",
                                60.0,
                            )
                            or 60.0
                        ),
                    )
                )
                ambiguous_blocks = getattr(self, "_ambiguous_order_block_until", None)
                if ambiguous_blocks is None:
                    ambiguous_blocks = {}
                    self._ambiguous_order_block_until = ambiguous_blocks
                ambiguous_blocks[symbol] = datetime.now() + timedelta(seconds=cooldown)
                order_time = datetime.now()
                self._last_order_times[rate_limit_key] = order_time
                self._last_order_times[symbol] = order_time  # diagnostic summary
                self.logger.error(
                    f"[{symbol}] Ambiguous order result could not be reconciled; "
                    f"blocking duplicate entries for {cooldown}s "
                    f"(retcode={getattr(result, 'retcode', None)})"
                )
                self._log_trade(
                    symbol,
                    direction,
                    volume,
                    entry_price,
                    sl_price,
                    tp_price,
                    magic,
                    f"AMBIGUOUS_UNRECONCILED: {getattr(result, 'retcode_description', '')}",
                    candidate_id=execution_candidate_id,
                )
                _retain_ambiguous_reservation(
                    f"broker send outcome is ambiguous (retcode={getattr(result, 'retcode', None)})",
                    broker_order_ticket=max(
                        0, int(getattr(result, "order", 0) or 0)
                    ),
                )
                _record_throttle(
                    "BLOCKED_AMBIGUOUS_ORDER_UNRECONCILED", cooldown=cooldown
                )
                _record_actionable(
                    action="BLOCKED_AMBIGUOUS_ORDER_UNRECONCILED",
                    entry_price_value=entry_price,
                    sl_value=sl_price,
                    tp_value=tp_price,
                    volume_value=volume,
                    target_risk_pct_value=target_risk_pct,
                    actual_risk_pct_value=actual_risk_pct,
                    context={
                        "ambiguous_retcode": getattr(result, "retcode", None),
                        "ambiguous_cooldown_seconds": cooldown,
                    },
                )
                return

            _cancel_definitive_reservation(result)
            self.logger.warning(
                f"[FAIL] [{symbol}] Order failed: {result.retcode} - {result.retcode_description}"
            )
            self._log_trade(
                symbol,
                direction,
                volume,
                entry_price,
                sl_price,
                tp_price,
                magic,
                f"FAILED: {result.retcode_description}",
                candidate_id=execution_candidate_id,
            )
            _record_throttle(f"FAILED_{result.retcode}")
            _record_actionable(
                action=f"FAILED_{result.retcode}",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
            )

    def _close_position_on_signal(
        self, position: MT5Position, features: pd.DataFrame, spec
    ):
        """Close position on opposite signal."""
        if not self.enable_trading:
            self.logger.info(f"[{position.symbol}] [PAPER] Would close position")
            return

        result = self.mt5.close_position(
            position=position, deviation=30, comment="PM_SignalExit"
        )

        if result.success:
            self.logger.info(
                f"[OK] [{position.symbol}] Position closed on signal reversal"
            )
        else:
            self.logger.warning(
                f"[FAIL] [{position.symbol}] Close failed: {result.retcode_description}"
            )

    def _log_trade(
        self,
        symbol: str,
        direction: str,
        volume: float,
        price: float,
        sl: float,
        tp: float,
        magic: int,
        status: str,
        *,
        candidate_id: str = "",
    ):
        """Log trade to history."""
        self.trade_log.append(
            {
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol,
                "direction": direction,
                "volume": volume,
                "price": price,
                "sl": sl,
                "tp": tp,
                "magic": magic,
                "status": status,
                "execution_book": str(
                    getattr(self, "execution_book", CHAMPION_EXECUTION_BOOK)
                ),
                "book_role": (
                    PROCESS_VALIDATED_CURRENT_REFIT_ROLE
                    if bool(getattr(self, "_is_current_refit_demo", False))
                    else LOCKBOX_CHAMPION_ROLE
                ),
                "candidate_id": str(candidate_id or ""),
            }
        )

    def save_trade_log(self, filepath: str):
        """Save trade log to file."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(
            f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.trade_log, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        self.logger.info(f"Saved {len(self.trade_log)} trades to {filepath}")

    def print_status(self):
        """Print current trading status."""
        print("\n" + "=" * 60)
        print("LIVE TRADER STATUS")
        print("=" * 60)
        print(f"Running: {self._running}")
        print(f"Trading enabled: {self.enable_trading}")
        print(f"Trades executed: {len(self.trade_log)}")

        # Account info
        if self.mt5 and self.mt5.is_connected():
            account = self.mt5.get_account_info()
            if account:
                print(f"\nAccount: {account.login}")
                print(f"Balance: {account.balance:.2f} {account.currency}")
                print(f"Equity: {account.equity:.2f} {account.currency}")

            # Open positions
            positions = self.mt5.get_positions()
            if positions is None:
                print("\nOpen positions: unavailable (snapshot failed)")
            else:
                print(f"\nOpen positions: {len(positions)}")
                for pos in positions:
                    direction = "LONG" if pos.type == 0 else "SHORT"
                    print(
                        f"  {pos.symbol} | {direction} | {pos.volume} lots | P/L: {pos.profit:.2f}"
                    )

        print("=" * 60)


# =============================================================================
# MAIN APPLICATION
# =============================================================================


class FXPortfolioManagerApp:
    """
    Main application class.

    Orchestrates the entire Portfolio Manager:
    - Initial optimization
    - Live trading
    - Automatic retraining
    """

    def __init__(
        self,
        symbols: List[str] = None,
        config: PipelineConfig = None,
        pipeline_config: "PipelineConfig" = None,
        mt5_config: "MT5Config" = None,
        position_config: "PositionConfig" = None,
        data_dir: Optional[str] = None,
        output_dir: Optional[str] = None,
        config_file: Optional[str] = None,
    ):
        """
        Initialize application.

        Args:
            symbols: List of symbols to trade
            config: Pipeline configuration (alias for pipeline_config)
            pipeline_config: Pipeline configuration
            mt5_config: MT5 connection configuration
            position_config: Position management configuration
            data_dir: Optional explicit override for data directory
            output_dir: Optional explicit override for output directory
            config_file: Path to the validated-winner ledger file
        """
        # Accept either name (keeps full compatibility)
        if config is None and pipeline_config is not None:
            config = pipeline_config

        self.symbols = symbols or DEFAULT_SYMBOLS

        # Configurations
        self.pipeline_config = config or PipelineConfig(
            data_dir=Path(data_dir or "./data"),
            output_dir=Path(output_dir or "./pm_outputs"),
        )
        if data_dir is not None:
            self.pipeline_config.data_dir = Path(data_dir)
            self.pipeline_config.data_dir.mkdir(parents=True, exist_ok=True)
        if output_dir is not None:
            self.pipeline_config.output_dir = Path(output_dir)
            self.pipeline_config.output_dir.mkdir(parents=True, exist_ok=True)
        self.mt5_config = mt5_config or MT5Config()
        self.position_config = position_config or PositionConfig()
        self.pipeline_config.regime_tp_multipliers = dict(
            getattr(self.position_config, "regime_tp_multipliers", {}) or {}
        )

        # Install regime-aware TP multipliers before any strategy is evaluated.
        set_regime_tp_multipliers(
            getattr(self.position_config, "regime_tp_multipliers", None)
        )

        # Paths follow the resolved pipeline config unless explicitly overridden above.
        self.data_dir = Path(self.pipeline_config.data_dir)
        self.output_dir = Path(self.pipeline_config.output_dir)
        self.log_dir = Path(self.pipeline_config.log_dir)
        resolved_config_file = config_file or str(
            getattr(self.pipeline_config, "winner_ledger_path", "pm_configs.json")
        )
        self.config_file = resolved_config_file

        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Components
        self.mt5: Optional[MT5Connector] = None
        self.portfolio_manager: Optional[PortfolioManager] = None
        self.trader: Optional[LiveTrader] = None
        self.storage_manager = StorageManager(
            pipeline_config=self.pipeline_config,
            data_dir=self.data_dir,
            output_dir=self.output_dir,
            log_dir=self.log_dir,
            logger=logging.getLogger(__name__),
            active_servers=[getattr(self.mt5_config, "server", "")],
            active_symbols=self.symbols,
        )
        self._last_retrain_schedule_notice: str = ""

        self.logger = logging.getLogger(__name__)

    def initialize(self) -> bool:
        """Initialize all components."""
        self.logger.info("=" * 60)
        self.logger.info("FX PORTFOLIO MANAGER v3.1")
        self.logger.info("=" * 60)

        # Check MT5 availability
        if not MT5_AVAILABLE:
            self.logger.warning("MetaTrader5 package not available")
            self.logger.warning("Running in offline/optimization-only mode")
        else:
            # Connect to MT5
            self.logger.info("Connecting to MetaTrader 5...")
            self.mt5 = MT5Connector(self.mt5_config)
            if not self.mt5.connect():
                self.logger.warning(
                    "Failed to connect to MT5 - running in offline mode"
                )
            else:
                self.logger.info("[OK] Connected to MT5")
                if self.storage_manager:
                    self.storage_manager.add_active_server(
                        getattr(self.mt5_config, "server", "")
                    )

        # Initialize portfolio manager
        self.portfolio_manager = PortfolioManager(
            config=self.pipeline_config,
            symbols=self.symbols,
            config_file=self.config_file,
        )

        self.logger.info("[OK] Portfolio Manager initialized")
        self.logger.info(f"  Symbols: {len(self.symbols)}")
        self.logger.info(f"  Strategies: {StrategyRegistry.count()}")
        self.logger.info(
            f"  Existing configs: {len(self.portfolio_manager.symbol_configs)}"
        )

        return True

    def run_optimization(self, overwrite: bool = False) -> bool:
        """
        Run optimization for all symbols with stateful skip/resume support.

        Args:
            overwrite: If True, re-optimize all symbols ignoring validity

        Returns:
            True if any valid configs exist after optimization
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("RUNNING OPTIMIZATION")
        if overwrite:
            self.logger.info("MODE: OVERWRITE (re-optimizing all symbols)")
        else:
            self.logger.info("MODE: INCREMENTAL (skipping valid configs)")
        self.logger.info("=" * 60)

        fetch_stats = None
        original_pm_symbols = list(getattr(self.portfolio_manager, "symbols", []) or [])
        native_calendar_mode = (
            str(getattr(self.pipeline_config, "production_split_mode", "") or "")
            .strip()
            .lower()
            == "calendar_lockbox_v2"
        )

        # Calendar production has one authority: the immutable active native
        # six-timeframe manifest. Do not even invoke the legacy live-MT5 M5
        # downloader here; its bar-count roster must never pre-empt the
        # activation's eligible/excluded universe decision.
        if native_calendar_mode:
            self.logger.info(
                "Calendar optimization uses the active immutable six-timeframe native "
                "universe; bypassing legacy MT5 M5 fetch and readiness filtering."
            )
        elif self.mt5 and self.mt5.is_connected():
            fetch_stats = self._fetch_historical_data()
        else:
            self.logger.info("Using existing data files (MT5 not connected)")

        if isinstance(fetch_stats, dict):
            incomplete = dict(fetch_stats.get("incomplete_symbols") or {})
            no_data = list(fetch_stats.get("no_data_symbols") or [])
            ready_symbols = list(fetch_stats.get("ready_symbols") or [])
            if incomplete or no_data:
                preview_items = [
                    f"{symbol}({rows})"
                    for symbol, rows in list(incomplete.items())[:12]
                ] + list(no_data[:12])
                preview = ", ".join(preview_items)
                omitted = max(0, len(incomplete) + len(no_data) - len(preview_items))
                if omitted:
                    preview += f", +{omitted} more"
                self.logger.warning(
                    "Data fetch incomplete; excluding %d symbols from this optimization pass "
                    "without changing their existing winner-ledger entries: %s",
                    len(incomplete) + len(no_data),
                    preview,
                )
            if len(ready_symbols) < len(original_pm_symbols):
                self.portfolio_manager.symbols = ready_symbols

        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        ledger_path = ledger_path.resolve()
        active_manifest_path = ledger_path.with_name(
            f"{ledger_path.stem}.generation.json"
        )
        legacy_snapshots = (
            ledger_path.with_name(f".{ledger_path.name}.optimize-snapshot"),
            active_manifest_path.with_name(
                f".{active_manifest_path.name}.optimize-snapshot"
            ),
        )
        if any(path.exists() for path in legacy_snapshots):
            self.logger.error(
                "Optimization refused: legacy recovery snapshots exist and require "
                "manual forensic handling: %s",
                ", ".join(str(path) for path in legacy_snapshots if path.exists()),
            )
            self._save_results(
                {},
                generation_outcome=None,
                filename="optimization_attempt_summary.csv",
            )
            return False
        try:
            reconcile = getattr(
                self.portfolio_manager, "_reconcile_interrupted_promotion", None
            )
            if callable(reconcile):
                reconciliation_status = reconcile()
                if reconciliation_status == "COMPLETED":
                    self.logger.warning(
                        "Completed a journaled promotion before starting a new run"
                    )
        except Exception as exc:
            self.logger.error(
                "Optimization refused: promotion journal reconciliation failed: %s",
                f"{type(exc).__name__}:{exc}",
            )
            self._save_results(
                {},
                generation_outcome=None,
                filename="optimization_attempt_summary.csv",
            )
            return False
        ledger_baseline = _live_file_identity(ledger_path)
        manifest_baseline = _live_file_identity(active_manifest_path)

        results = {}
        optimization_error = None
        try:
            try:
                results = self.portfolio_manager.initial_optimization(overwrite=overwrite)
            except Exception as exc:
                optimization_error = exc
        finally:
            if original_pm_symbols:
                self.portfolio_manager.symbols = original_pm_symbols

        outcome = None
        if optimization_error is None:
            try:
                outcome = getattr(
                    self.portfolio_manager,
                    "last_optimization_generation",
                    None,
                )
            except Exception as exc:
                optimization_error = exc
        try:
            self._save_results(
                results,
                generation_outcome=outcome,
                filename="optimization_attempt_summary.csv",
            )
        except Exception as exc:
            self.logger.warning(
                "Could not write optimization attempt diagnostics: %s",
                f"{type(exc).__name__}:{exc}",
            )
        try:
            self.portfolio_manager.print_status()
        except Exception as exc:
            self.logger.warning("Could not print optimization status: %s", exc)

        if optimization_error is None:
            try:
                succeeded, generation_errors = _governed_generation_cli_verdict(
                    outcome,
                    ledger_path,
                    expected_manifest_path=active_manifest_path,
                )
            except Exception as exc:
                succeeded = False
                generation_errors = [
                    "OPTIMIZATION_GENERATION_VERDICT_FAILED:"
                    f"{type(exc).__name__}:{exc}"
                ]
        else:
            succeeded = False
            generation_errors = [
                "OPTIMIZATION_GENERATION_UNHANDLED_EXCEPTION:"
                f"{type(optimization_error).__name__}:{optimization_error}"
            ]
        if not succeeded:
            try:
                ledger_changed = _live_file_identity(ledger_path) != ledger_baseline
                manifest_changed = (
                    _live_file_identity(active_manifest_path) != manifest_baseline
                )
                if ledger_changed or manifest_changed:
                    generation_errors.append(
                        "OPTIMIZATION_REJECTED_LIVE_ARTIFACT_CHANGE_PRESERVED_FOR_FORENSICS"
                    )
            except Exception as exc:
                generation_errors.append(
                    "OPTIMIZATION_REJECTED_ARTIFACT_IDENTITY_FAILED:"
                    f"{type(exc).__name__}"
                )
            self.logger.error(
                "Optimization generation rejected: %s",
                ", ".join(generation_errors[:12]),
            )
            return False

        try:
            self._save_results(results, generation_outcome=outcome)
        except Exception as exc:
            self.logger.warning(
                "Could not write optimization success diagnostics: %s",
                f"{type(exc).__name__}:{exc}",
            )
        if self.storage_manager:
            try:
                self.storage_manager.on_optimization_complete()
            except Exception as exc:
                self.logger.warning(
                    "Could not record optimization completion: %s",
                    f"{type(exc).__name__}:{exc}",
                )
        self.logger.info(
            "Optimization generation %s promoted with an exact ledger receipt",
            getattr(outcome, "generation_id", ""),
        )
        return True

    def _check_production_retrain_schedule(self) -> None:
        """Run or announce the config-driven production retrain schedule."""
        if not self.portfolio_manager:
            return
        mode = (
            str(
                getattr(self.pipeline_config, "production_retrain_mode", "auto")
                or "auto"
            )
            .strip()
            .lower()
        )
        if mode == "off":
            return

        symbols_to_retrain = self.portfolio_manager.get_symbols_needing_retrain()
        if not symbols_to_retrain:
            return

        now = datetime.now()
        due_slot = self.pipeline_config.get_last_retrain_slot(now)
        due_str = due_slot.strftime("%Y-%m-%d %H:%M")

        if mode == "notify":
            if self._last_retrain_schedule_notice == due_str:
                return
            self.logger.warning(
                f"Production retrain due since {due_str} for {len(symbols_to_retrain)} symbols. "
                f"Please run: python pm_main.py --optimize --config {os.environ.get('PM_CONFIG_PATH', 'config.json')}"
            )
            self._last_retrain_schedule_notice = due_str
            return

        self.logger.info(
            f"Production retrain due since {due_str}; refreshing {len(symbols_to_retrain)} symbols "
            f"on the fixed schedule ({self.pipeline_config.describe_retrain_schedule()})"
        )
        production_native = (
            bool(
                str(
                    getattr(self.pipeline_config, "_deployment_config_path", "") or ""
                ).strip()
            )
            and str(getattr(self.pipeline_config, "production_split_mode", "") or "")
            .strip()
            .lower()
            == "calendar_lockbox_v2"
        )
        if production_native:
            self.logger.info(
                "Scheduled native retraining will use only a newly activated immutable "
                "six-timeframe source; legacy M5 refresh is intentionally skipped"
            )
        else:
            self._fetch_historical_data(symbols_to_retrain)
        retrain_results = self.portfolio_manager.retrain_all_needed()
        self._last_retrain_schedule_notice = due_str
        outcome = getattr(
            self.portfolio_manager,
            "last_optimization_generation",
            None,
        )
        promoted = outcome is not None and bool(getattr(outcome, "promoted", False))
        ledger_changed = outcome is not None and bool(
            getattr(outcome, "ledger_changed", False)
        )
        if promoted:
            # Completion bookkeeping gates on the authorized promotion, not on the
            # book-byte delta: a genuine promotion over byte-identical winner
            # bytes still completes (seq1686/1691).  ledger_changed governs ONLY
            # the runtime winner-config cache invalidation.
            if ledger_changed and self.trader:
                self.trader.invalidate_runtime_caches()
            if self.storage_manager:
                self.storage_manager.on_optimization_complete()
        elif retrain_results:
            self.logger.warning(
                "Scheduled retraining did not promote a new ledger generation; "
                "runtime caches and storage completion hooks were left untouched"
            )

    def _get_active_live_timeframes(self) -> List[str]:
        """Return the unique active timeframes from the current validated configs."""
        if self.trader is not None:
            validated = self.trader._get_live_configs()
        else:
            validated = (
                self.portfolio_manager.get_live_eligible_configs()
                if self.portfolio_manager
                else {}
            )
        seen: Set[str] = set()
        ordered: List[str] = []
        for config in validated.values():
            try:
                if config.has_regime_configs():
                    timeframes = config.get_available_timeframes()
                else:
                    timeframes = [getattr(config, "timeframe", "")]
            except Exception:
                timeframes = [getattr(config, "timeframe", "")]
            for tf in timeframes:
                normalized = str(tf or "").upper()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    ordered.append(normalized)
        if not ordered:
            for tf in getattr(self.pipeline_config, "timeframes", []) or []:
                normalized = str(tf or "").upper()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    ordered.append(normalized)
        return sorted(
            ordered, key=lambda tf: int(DataLoader.TIMEFRAME_MINUTES.get(tf, 10**6))
        )

    def _wait_for_next_due(
        self,
        shutdown_event: threading.Event,
        due_at: datetime,
        *,
        now: Optional[datetime] = None,
    ) -> None:
        """
        Comparator-based idle wait for the next due action.

        Business cadence is controlled by `due_at`; the wait itself is bounded so
        the loop remains responsive to shutdowns and state changes.
        """
        probe = now or datetime.now()
        timeout = max(0.0, (due_at - probe).total_seconds())
        shutdown_event.wait(timeout=min(timeout, 1.0))

    def _attempt_reconnect(
        self, reconnect_state: Dict[str, Any], *, now: Optional[datetime] = None
    ) -> bool:
        """Run one reconnect attempt when its due-time has arrived."""
        probe = now or datetime.now()
        next_attempt_at = reconnect_state.get("next_attempt_at") or probe
        if probe < next_attempt_at:
            return False

        max_attempts = max(
            1, int(getattr(self.pipeline_config, "reconnect_max_attempts", 5) or 5)
        )
        retry_seconds = max(
            1,
            int(
                getattr(self.pipeline_config, "reconnect_attempt_interval_seconds", 5)
                or 5
            ),
        )
        cooldown_seconds = max(
            1,
            int(
                getattr(self.pipeline_config, "reconnect_failure_retry_seconds", 30)
                or 30
            ),
        )

        attempt_number = int(reconnect_state.get("attempts", 0) or 0) + 1
        self.logger.info(f"Reconnection attempt {attempt_number}/{max_attempts}")
        try:
            self.mt5.disconnect()
        except Exception:
            pass

        if self.mt5.connect():
            self.logger.info("Reconnected successfully")
            reconnect_state["attempts"] = 0
            reconnect_state["next_attempt_at"] = probe
            if self.trader:
                self.trader.invalidate_runtime_caches()
            return True

        if attempt_number >= max_attempts:
            self.logger.error(
                f"Failed to reconnect after {max_attempts} attempts; retrying in {cooldown_seconds}s"
            )
            reconnect_state["attempts"] = 0
            reconnect_state["next_attempt_at"] = probe + timedelta(
                seconds=cooldown_seconds
            )
        else:
            reconnect_state["attempts"] = attempt_number
            reconnect_state["next_attempt_at"] = probe + timedelta(
                seconds=retry_seconds
            )
        return False

    def run_trading(
        self,
        enable_trading: bool = True,
        close_on_opposite_signal: bool = False,
        execution_book: str = CHAMPION_EXECUTION_BOOK,
    ):
        """
        Run live trading loop.
        """
        if not self.mt5 or not self.mt5.is_connected():
            self.logger.error("MT5 not connected. Cannot run live trading.")
            return False

        self.logger.info("\n" + "=" * 60)
        normalized_book = str(execution_book or CHAMPION_EXECUTION_BOOK).strip().upper()
        if normalized_book not in {
            CHAMPION_EXECUTION_BOOK,
            CURRENT_REFIT_DEMO_EXECUTION_BOOK,
        }:
            self.logger.error(f"Unsupported execution book: {normalized_book}")
            return False
        is_current_refit_demo = (
            normalized_book == CURRENT_REFIT_DEMO_EXECUTION_BOOK
        )
        self.logger.info(
            "STARTING CURRENT-REFIT DEMO PROBATION"
            if is_current_refit_demo
            else "STARTING LIVE TRADING"
        )
        self.logger.info(f"Trading enabled: {enable_trading}")
        self.logger.info(f"Execution book: {normalized_book}")
        if is_current_refit_demo:
            self.logger.info("Production retrain: disabled for fixed demo projection")
        else:
            self.logger.info(
                f"Production retrain: {self.pipeline_config.production_retrain_mode} "
                f"({self.pipeline_config.describe_retrain_schedule()})"
            )
        self.logger.info(f"Close on opposite signal: {close_on_opposite_signal}")
        # Surface any regime-parameter fallbacks observed so far so
        # operators see tuning gaps without having to grep the log.
        try:
            from pm_regime import get_regime_fallback_record

            fallbacks = get_regime_fallback_record()
            if not fallbacks:
                self.logger.info(
                    "regime_params: no fallbacks recorded (all observed (symbol,TF) pairs tuned)"
                )
            else:
                preview = ", ".join(
                    f"{s}/{tf}" for (s, tf) in sorted(fallbacks.keys())[:10]
                )
                more = "" if len(fallbacks) <= 10 else f" (+{len(fallbacks) - 10} more)"
                self.logger.info(
                    f"regime_params: {len(fallbacks)} fallback(s) -> {preview}{more}"
                )
        except Exception as exc:
            self.logger.debug(f"regime_params fallback summary unavailable: {exc}")
        self.logger.info("=" * 60)

        if is_current_refit_demo:
            live_eligible_configs = None
        else:
            live_eligible_configs = self.portfolio_manager.get_live_eligible_configs(
                log_rejections=True
            )
        if not is_current_refit_demo and not live_eligible_configs:
            open_positions = None
            try:
                open_positions = self.mt5.get_positions()
            except Exception as exc:
                self.logger.debug(
                    f"Open-position check unavailable during live startup: {exc}"
                )
            if open_positions:
                self.logger.warning(
                    "No live-eligible configurations for new entries, but open positions exist. "
                    "Starting in management-only mode until live eligibility is restored."
                )
            else:
                self.logger.warning(
                    "No live-eligible configurations. Run optimization or review eligibility policy first."
                )
                return False

        try:
            self.trader = LiveTrader(
                mt5_connector=self.mt5,
                portfolio_manager=self.portfolio_manager,
                position_config=self.position_config,
                enable_trading=enable_trading,
                close_on_opposite_signal=close_on_opposite_signal,
                pipeline_config=self.pipeline_config,
                storage_manager=self.storage_manager,
                execution_book=normalized_book,
                demo_symbols=(
                    sorted(str(symbol).upper() for symbol in self.symbols)
                    if is_current_refit_demo
                    else None
                ),
                demo_winner_ledger_path=(
                    self.config_file if is_current_refit_demo else None
                ),
            )
        except Exception as exc:
            self.logger.error(
                "Trader construction failed: %s: %s", type(exc).__name__, exc
            )
            self.trader = None
            return False
        if is_current_refit_demo:
            live_eligible_configs = self.trader._get_live_configs()
            startup_positions = self.mt5.get_positions()
            startup_position_reason = (
                self.trader._current_refit_demo_position_block_reason(
                    startup_positions,
                    self.trader._get_management_configs(),
                )
            )
            if startup_position_reason:
                self.logger.error(
                    f"Refusing current-refit demo: {startup_position_reason}"
                )
                self.trader = None
                return False
            if not live_eligible_configs:
                if startup_positions:
                    self.logger.warning(
                        "Current-refit cells expired; starting management-only"
                    )
                else:
                    self.logger.error(
                        "Current-refit demo has no unexpired selected cells"
                    )
                    self.trader = None
                    return False
        self.trader._running = True

        live_loop_trigger_mode = (
            str(getattr(self.pipeline_config, "live_loop_trigger_mode", "bar") or "bar")
            .strip()
            .lower()
        )
        loop_aliases = {
            "tick": "bar",
            "quote": "bar",
            "quotes": "bar",
            "per_quote": "bar",
            "per-tick": "bar",
            "bars": "bar",
            "candle": "bar",
            "candles": "bar",
            "data": "bar",
            "timer": "scheduled",
            "time": "scheduled",
        }
        live_loop_trigger_mode = loop_aliases.get(
            live_loop_trigger_mode, live_loop_trigger_mode
        )
        if live_loop_trigger_mode not in {"bar", "scheduled"}:
            self.logger.warning(
                f"Invalid live_loop_trigger_mode={live_loop_trigger_mode!r}; using bar trigger."
            )
            live_loop_trigger_mode = "bar"
        try:
            live_bar_poll_seconds = max(
                0.05,
                float(
                    getattr(self.pipeline_config, "live_bar_poll_seconds", 0.25) or 0.25
                ),
            )
        except Exception:
            try:
                live_bar_poll_seconds = max(
                    0.05,
                    float(
                        getattr(self.pipeline_config, "live_tick_poll_seconds", 0.25)
                        or 0.25
                    ),
                )
            except Exception:
                live_bar_poll_seconds = 0.25
        self.logger.info(
            f"Live loop trigger: {live_loop_trigger_mode}"
            + (
                f" (MT5 bar-gated, poll idle={live_bar_poll_seconds:.2f}s)"
                if live_loop_trigger_mode == "bar"
                else " (scheduled bar due-time fallback)"
            )
        )

        next_retrain_check_at = (
            datetime.max if is_current_refit_demo else datetime.now()
        )
        next_sweep_at = datetime.now()
        reconnect_state: Dict[str, Any] = {
            "attempts": 0,
            "next_attempt_at": datetime.now(),
        }
        shutdown_event = self.trader._shutdown_event
        retrain_check_interval = timedelta(
            seconds=max(
                1,
                int(
                    getattr(self.pipeline_config, "production_retrain_poll_seconds", 60)
                    or 60
                ),
            )
        )

        try:
            while not shutdown_event.is_set():
                now = datetime.now()
                if not self.mt5.is_connected():
                    self._attempt_reconnect(reconnect_state, now=now)
                    self._wait_for_next_due(
                        shutdown_event,
                        reconnect_state.get(
                            "next_attempt_at", now + timedelta(seconds=1)
                        ),
                        now=now,
                    )
                    continue

                reconnect_state["attempts"] = 0
                reconnect_state["next_attempt_at"] = now

                if not is_current_refit_demo and now >= next_retrain_check_at:
                    self._check_production_retrain_schedule()
                    next_retrain_check_at = datetime.now() + retrain_check_interval

                if live_loop_trigger_mode == "bar":
                    try:
                        live_configs = self.trader._get_live_configs()
                        changed_by_symbol = self.trader.get_symbols_with_new_bars(
                            live_configs
                        )
                        processed = self.trader.process_all_symbols(
                            symbols_filter=set(changed_by_symbol.keys()),
                            timeframes_filter=changed_by_symbol,
                        )
                        if processed:
                            self.trader.commit_bar_probe_times(changed_by_symbol)
                    except Exception as e:
                        self.logger.error(f"Error in trading loop: {e}")

                    next_idle_at = datetime.now() + timedelta(
                        seconds=live_bar_poll_seconds
                    )
                    next_due = min(next_retrain_check_at, next_idle_at)
                    self._wait_for_next_due(
                        shutdown_event, next_due, now=datetime.now()
                    )
                    continue

                if now >= next_sweep_at:
                    try:
                        self.trader.process_all_symbols()
                    except Exception as e:
                        self.logger.error(f"Error in trading loop: {e}")
                    active_timeframes = self._get_active_live_timeframes()
                    next_sweep_at = self.trader.get_next_sweep_due(
                        active_timeframes,
                        now=datetime.now(),
                    )

                next_due = min(next_retrain_check_at, next_sweep_at)
                self._wait_for_next_due(shutdown_event, next_due, now=datetime.now())

        except KeyboardInterrupt:
            self.logger.info("Shutdown requested...")
        finally:
            self.shutdown()

    @staticmethod
    def _merge_history_frames(
        *frames: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        valid_frames = [
            frame
            for frame in frames
            if isinstance(frame, pd.DataFrame) and not frame.empty
        ]
        if not valid_frames:
            return None
        merged = pd.concat(valid_frames)
        merged.index = pd.to_datetime(merged.index)
        return merged[~merged.index.duplicated(keep="last")].sort_index()

    def _try_deep_m5_backfill(
        self,
        symbol: str,
        broker_symbol: str,
        existing_df: Optional[pd.DataFrame],
        bars: Optional[pd.DataFrame],
        bars_to_fetch: int,
        required_bars: int,
    ) -> Optional[pd.DataFrame]:
        deep_enabled = bool(
            getattr(
                self.pipeline_config, "historical_fetch_deep_backfill_enabled", False
            )
        )
        if not deep_enabled or bars_to_fetch <= 0:
            return bars

        required_bars = int(required_bars or bars_to_fetch or 0)
        current_len = 0 if bars is None else len(bars)
        if required_bars > 0 and current_len >= required_bars:
            return bars

        years = int(
            getattr(self.pipeline_config, "historical_fetch_deep_backfill_years", 18)
            or 18
        )
        years = max(1, years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=int(math.ceil(365.25 * years)))
        self.logger.info(
            f"  {symbol}: Deep backfill request from {start_date.date()} "
            f"because {current_len} M5 bars is below required {required_bars}."
        )

        range_bars = self.mt5.get_bars_range(
            broker_symbol, "M5", start_date, end_date, sync_retry=True
        )
        merged = self._merge_history_frames(existing_df, bars, range_bars)
        merged_len = 0 if merged is None else len(merged)
        if merged_len > current_len:
            self.logger.info(
                f"  {symbol}: Deep backfill expanded history to {merged_len} M5 bars."
            )

        if required_bars > 0 and merged_len < required_bars:
            hydrated = self.mt5.get_bars(
                broker_symbol, "M5", count=bars_to_fetch, force_full=True
            )
            merged = self._merge_history_frames(merged, hydrated)
            hydrated_len = 0 if merged is None else len(merged)
            if hydrated_len > merged_len:
                self.logger.info(
                    f"  {symbol}: Post-backfill count fetch expanded history to {hydrated_len} M5 bars."
                )

        return merged

    def _write_history_csv_atomic(
        self,
        symbol: str,
        bars: pd.DataFrame,
        filepath: Path,
        attempts: int = 3,
    ) -> bool:
        """Persist refreshed history without corrupting the existing CSV on write failure."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        attempts = max(1, int(attempts or 1))
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            tmp_path = filepath.with_name(
                f".{filepath.name}.{os.getpid()}.{attempt}.tmp"
            )
            try:
                bars.to_csv(tmp_path, index_label="time")
                os.replace(tmp_path, filepath)
                return True
            except Exception as exc:
                last_error = exc
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
                if attempt < attempts:
                    self.logger.warning(
                        f"  {symbol}: History CSV write attempt {attempt}/{attempts} failed: "
                        f"{type(exc).__name__}: {exc}; retrying."
                    )
                    time.sleep(min(2.0, 0.25 * attempt))
        self.logger.error(
            f"  {symbol}: Failed to write refreshed M5 history to {filepath} after "
            f"{attempts} attempt(s): {type(last_error).__name__ if last_error else 'unknown'}: {last_error}"
        )
        return False

    def _effective_min_m5_bars(self) -> int:
        """Validity floor for downloaded M5 history.

        Fractional rule is the source of truth: a symbol is valid when it has at
        least `optimization_min_m5_bars_fraction` of `max_bars` (e.g. 0.85 of 300k
        = 255k). Keeps the 300k standard while admitting close-but-short symbols;
        the absolute `optimization_min_m5_bars` is used only when no fraction is set.
        """
        max_bars = int(getattr(self.pipeline_config, "max_bars", 0) or 0)
        frac = float(
            getattr(self.pipeline_config, "optimization_min_m5_bars_fraction", 0.0)
            or 0.0
        )
        if frac > 0.0 and max_bars > 0:
            return int(math.ceil(max_bars * frac))
        return int(getattr(self.pipeline_config, "optimization_min_m5_bars", 0) or 0)

    def _fetch_historical_data(self, symbols: List[str] = None):
        """Fetch historical data from MT5 and save to data directory."""
        if (
            str(
                getattr(self.pipeline_config, "production_split_mode", "") or ""
            ).lower()
            == "calendar_lockbox_v2"
        ):
            self.logger.info(
                "Calendar production uses the immutable activated native-timeframe run; "
                "skipping the legacy M5 count/backfill workflow. Use "
                "tools/acquire_native_history.py and tools/audit_activate_native_history.py "
                "to refresh it explicitly."
            )
            return None
        if not self.mt5 or not self.mt5.is_connected():
            return

        symbols = symbols or self.symbols

        self.logger.info(f"Fetching historical data for {len(symbols)} symbols...")
        stats = {
            "symbols_requested": len(symbols),
            "ready_symbols": [],
            "incomplete_symbols": {},
            "no_data_symbols": [],
            "write_failed_symbols": [],
        }
        min_m5_bars = self._effective_min_m5_bars()
        retry_attempts = max(
            1,
            int(
                getattr(self.pipeline_config, "historical_fetch_retry_attempts", 1) or 1
            ),
        )
        retry_sleep = max(
            0.0,
            float(
                getattr(
                    self.pipeline_config, "historical_fetch_retry_sleep_seconds", 0.0
                )
                or 0.0
            ),
        )

        for symbol in symbols:
            broker_symbol = self.mt5.find_broker_symbol(symbol)
            if broker_symbol is None:
                self.logger.warning(f"Symbol not found: {symbol}")
                stats["no_data_symbols"].append(symbol)
                continue

            filepath = self.data_dir / f"{symbol}_M5.csv"
            bars_to_fetch = int(getattr(self.pipeline_config, "max_bars", 300000))
            overlap_minutes = int(
                getattr(
                    self.pipeline_config, "storage_delta_sync_overlap_minutes", 1440
                )
            )
            existing_df: Optional[pd.DataFrame] = None
            bars: Optional[pd.DataFrame] = None

            if filepath.exists():
                try:
                    existing_df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                    existing_df.index = pd.to_datetime(existing_df.index)
                    existing_df = existing_df.sort_index()
                except Exception as exc:
                    self.logger.debug(
                        f"[{symbol}] Existing history reload failed, refetching full file: {exc}"
                    )
                    existing_df = None

            if (
                existing_df is not None
                and not existing_df.empty
                and bars_to_fetch > 0
                and len(existing_df) < bars_to_fetch
            ):
                missing_bars = max(0, bars_to_fetch - len(existing_df))
                if len(existing_df) > 1:
                    observed_span = existing_df.index[-1] - existing_df.index[0]
                    # Cap per-bar seconds so a gappy/corrupt span (e.g. a stray old
                    # row at index[0]) cannot inflate the backfill window. M5 with
                    # weekend gaps averages well under 900s/bar.
                    seconds_per_bar = min(
                        900.0,
                        max(
                            60.0,
                            observed_span.total_seconds()
                            / max(1, len(existing_df) - 1),
                        ),
                    )
                else:
                    seconds_per_bar = 5.0 * 60.0
                padding_bars = max(1000, int(math.ceil(overlap_minutes / 5.0)))
                backfill_seconds = (
                    seconds_per_bar * (missing_bars + padding_bars) * 1.25
                )
                backfill_start = existing_df.index[0] - timedelta(
                    seconds=backfill_seconds
                )
                # Clamp to a valid floor: never before the deep-backfill horizon or
                # the epoch (MT5/Windows reject pre-1970 dates with Errno 22).
                _years = max(
                    1,
                    int(
                        getattr(
                            self.pipeline_config,
                            "historical_fetch_deep_backfill_years",
                            18,
                        )
                        or 18
                    ),
                )
                _floor = pd.Timestamp(datetime.now() - timedelta(days=365.25 * _years))
                if _floor.year < 1971:
                    _floor = pd.Timestamp(year=1971, month=1, day=1)
                if pd.Timestamp(backfill_start) < _floor:
                    backfill_start = _floor
                self.logger.info(
                    f"  {symbol}: Existing history has {len(existing_df)} M5 bars; "
                    f"target is {bars_to_fetch}. Backfilling from {backfill_start}."
                )
                bars = self.mt5.get_bars_range(
                    broker_symbol,
                    "M5",
                    backfill_start.to_pydatetime()
                    if hasattr(backfill_start, "to_pydatetime")
                    else backfill_start,
                    datetime.now(),
                )
                if bars is None or len(bars) == 0:
                    self.logger.warning(
                        f"  {symbol}: Range backfill returned no data; trying full target count fetch."
                    )
                    bars = self.mt5.get_bars(
                        broker_symbol, "M5", count=bars_to_fetch, force_full=True
                    )
                if bars is not None and len(bars) > 0:
                    bars = pd.concat([existing_df, bars])
                else:
                    self.logger.warning(
                        f"  {symbol}: Target backfill returned no data; "
                        "falling back to existing history."
                    )
                    bars = existing_df
            elif existing_df is not None and not existing_df.empty:
                start_date = existing_df.index[-1] - timedelta(
                    minutes=max(1, overlap_minutes)
                )
                bars = self.mt5.get_bars_range(
                    broker_symbol, "M5", start_date, datetime.now()
                )
                if bars is not None and len(bars) > 0:
                    bars = pd.concat([existing_df, bars])
                else:
                    bars = existing_df
            else:
                bars = self.mt5.get_bars(
                    broker_symbol, "M5", count=bars_to_fetch, force_full=True
                )

            best_len = 0 if bars is None else len(bars)
            for attempt in range(2, retry_attempts + 1):
                if (
                    bars is not None
                    and len(bars) > 0
                    and (min_m5_bars <= 0 or len(bars) >= min_m5_bars)
                ):
                    break
                reason = (
                    "no data"
                    if bars is None or len(bars) == 0
                    else f"{len(bars)} bars below required {min_m5_bars}"
                )
                self.logger.warning(
                    f"  {symbol}: Fetch attempt {attempt - 1}/{retry_attempts} ended with {reason}; retrying full target count fetch."
                )
                if retry_sleep > 0:
                    time.sleep(retry_sleep)
                retry_bars = self.mt5.get_bars(
                    broker_symbol, "M5", count=bars_to_fetch, force_full=True
                )
                retry_len = 0 if retry_bars is None else len(retry_bars)
                if retry_len > best_len:
                    bars = retry_bars
                    best_len = retry_len

            bars = self._try_deep_m5_backfill(
                symbol,
                broker_symbol,
                existing_df,
                bars,
                bars_to_fetch,
                min_m5_bars,
            )

            if bars is None or len(bars) == 0:
                self.logger.warning(f"  {symbol}: No data available")
                stats["no_data_symbols"].append(symbol)
                continue

            bars = bars[~bars.index.duplicated(keep="last")].sort_index()
            if bars_to_fetch > 0 and len(bars) > bars_to_fetch:
                bars = bars.iloc[-bars_to_fetch:]
            if not self._write_history_csv_atomic(symbol, bars, filepath, attempts=3):
                stats["write_failed_symbols"].append(symbol)
                continue
            self.logger.info(f"  {symbol}: {len(bars)} bars saved")
            if min_m5_bars > 0 and len(bars) < min_m5_bars:
                self.logger.warning(
                    f"  {symbol}: Only {len(bars)} M5 bars after {retry_attempts} fetch attempt(s); "
                    f"minimum for optimization is {min_m5_bars}. Leaving symbol for retry."
                )
                stats["incomplete_symbols"][symbol] = len(bars)
            else:
                stats["ready_symbols"].append(symbol)

        data_loader = getattr(
            getattr(self.portfolio_manager, "pipeline", None), "data_loader", None
        )
        if isinstance(data_loader, DataLoader):
            data_loader.clear_cache()
        return stats

    def _save_results(
        self,
        results: Dict,
        generation_outcome: Any = None,
        *,
        filename: str = "optimization_summary.csv",
    ):
        """Write one bounded, atomic optimization diagnostic."""
        summary = [
            {
                "record_type": "generation",
                "generation_id": str(
                    getattr(generation_outcome, "generation_id", "") or ""
                ),
                "generation_status": str(
                    getattr(generation_outcome, "status", "MISSING") or "MISSING"
                ),
                "promoted": bool(getattr(generation_outcome, "promoted", False)),
                "ledger_changed": bool(
                    getattr(generation_outcome, "ledger_changed", False)
                ),
                "technical_failures": json.dumps(
                    dict(getattr(generation_outcome, "technical_failures", {}) or {}),
                    sort_keys=True,
                )[:4000],
            }
        ]
        for symbol, result in results.items():
            config = getattr(result, "config", None)
            summary.append(
                {
                    "record_type": "symbol",
                    "generation_id": str(
                        getattr(generation_outcome, "generation_id", "") or ""
                    ),
                    "generation_status": str(
                        getattr(generation_outcome, "status", "MISSING") or "MISSING"
                    ),
                    "symbol": str(symbol),
                    "success": bool(getattr(result, "success", False)),
                    "error": str(getattr(result, "error_message", "") or "")[:2000],
                    "strategy": getattr(config, "strategy_name", "") if config else "",
                    "timeframe": getattr(config, "timeframe", "") if config else "",
                    "score": getattr(config, "composite_score", "") if config else "",
                    "validated": bool(getattr(config, "is_validated", False))
                    if config
                    else False,
                    "valid_until": (
                        config.valid_until.isoformat()
                        if config is not None and getattr(config, "valid_until", None)
                        else ""
                    ),
                    "train_trades": (
                        dict(getattr(config, "train_metrics", {}) or {}).get(
                            "total_trades", 0
                        )
                        if config
                        else 0
                    ),
                    "val_trades": (
                        dict(getattr(config, "val_metrics", {}) or {}).get(
                            "total_trades", 0
                        )
                        if config
                        else 0
                    ),
                }
            )

        summary_df = pd.DataFrame(summary)
        summary_path = self.output_dir / str(filename)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = summary_path.with_name(f".{summary_path.name}.{os.getpid()}.tmp")
        try:
            summary_df.to_csv(temp_path, index=False)
            os.replace(temp_path, summary_path)
        finally:
            temp_path.unlink(missing_ok=True)
        self.logger.info(f"Saved optimization summary to {summary_path}")

    def shutdown(self):
        """Shutdown the application."""
        self.logger.info("Shutting down...")

        if self.trader:
            self.trader.stop()

            # Save trade log
            trade_log_dir = self.output_dir
            if bool(getattr(self.trader, "_is_current_refit_demo", False)):
                trade_log_dir = trade_log_dir / "current_refit_demo"
            log_path = (
                trade_log_dir
                / f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            self.trader.save_trade_log(str(log_path))
        if self.storage_manager:
            self.storage_manager.on_shutdown()

        if self.mt5:
            self.mt5.disconnect()

        self.logger.info("Shutdown complete")

    def print_status(self):
        """Print application status."""
        if self.portfolio_manager:
            self.portfolio_manager.print_status()

        if self.trader:
            self.trader.print_status()


# =============================================================================
# SIGNAL HANDLERS
# =============================================================================

_app_instance: Optional[FXPortfolioManagerApp] = None


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global _app_instance
    print("\nShutdown signal received...")
    if _app_instance:
        _app_instance.shutdown()
    sys.exit(0)


def _run_bounded_demo_preflight(
    *,
    logger: logging.Logger,
    pipeline_config: PipelineConfig,
    position_config: PositionConfig,
    mt5_config: MT5Config,
    symbols: Sequence[str],
    config_path: str,
) -> int:
    """Validate DEMO probation without constructing the trading application.

    The probe deliberately attaches to the terminal's existing session. It
    never forwards configured credentials, because preflight must observe and
    reject the wrong account rather than change the terminal account to pass.
    """

    from pm_demo_readiness import (
        DemoProbationReadinessReport,
        demo_account_errors,
        validate_demo_probation_readiness,
    )

    account_info = None
    probe_errors: List[str] = []
    connector: Optional[MT5Connector] = None
    try:
        if MT5_AVAILABLE:
            requested_timeout = int(
                getattr(mt5_config, "timeout", _DEMO_PREFLIGHT_MAX_MT5_TIMEOUT_MS)
                or _DEMO_PREFLIGHT_MAX_MT5_TIMEOUT_MS
            )
            existing_session_config = MT5Config(
                path=str(getattr(mt5_config, "path", "") or ""),
                timeout=max(
                    1_000,
                    min(requested_timeout, _DEMO_PREFLIGHT_MAX_MT5_TIMEOUT_MS),
                ),
                portable=bool(getattr(mt5_config, "portable", False)),
            )
            connector = MT5Connector(existing_session_config)
            if connector.connect():
                account_info = connector.get_account_info()
            else:
                logger.warning(
                    "Could not attach to the existing MT5 session for DEMO preflight"
                )
        else:
            logger.warning("MetaTrader5 package not available for DEMO preflight")
    except Exception as exc:
        logger.warning(
            "Could not fetch MT5 account info for DEMO preflight validation: %s",
            exc,
        )
    finally:
        if connector is not None:
            try:
                connector.disconnect()
            except Exception as exc:
                logger.warning("Could not disconnect DEMO preflight probe: %s", exc)
                probe_errors.append("DEMO_PREFLIGHT_MT5_DISCONNECT_FAILED")

    preflight_errors = [*demo_account_errors(account_info), *probe_errors]
    if preflight_errors:
        # Account identity is the first executable safety boundary. There is no
        # value in spending roughly a full-universe verification pass after it
        # has already failed, and doing so makes an operator identity check look
        # hung. The full evidence/readiness chain still runs on the exact Demo.
        readiness = DemoProbationReadinessReport(
            ok=False,
            findings=[
                ReadinessFinding("error", error.lower(), error, "runtime")
                for error in preflight_errors
            ],
        )
    else:
        readiness = validate_demo_probation_readiness(
            pipeline_config,
            symbols=list(symbols),
            config_path=config_path,
            winner_ledger_path=getattr(pipeline_config, "winner_ledger_path", None),
            account_info=account_info,
            position_config=position_config,
        )
    log_readiness_report(logger, readiness)
    logger.info(
        "Preflight complete: %d error(s), %d warning(s)",
        len(readiness.errors),
        len(readiness.warnings),
    )
    return 0 if readiness.ok else 2


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def main():
    """Main entry point."""
    global _app_instance

    parser = argparse.ArgumentParser(
        description="FX Portfolio Manager - Automated Trading System"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Path to config JSON (default: config.json)",
    )

    parser.add_argument(
        "--optimize", action="store_true", help="Run initial optimization"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force re-optimization of all symbols (ignore valid configs)",
    )
    parser.add_argument("--trade", action="store_true", help="Start live trading")
    parser.add_argument(
        "--paper",
        action="store_true",
        help=(
            "Execute the authorized current-refit book on a broker-verified "
            "MT5 DEMO account"
        ),
    )
    parser.add_argument(
        "--close-on-opposite-signal",
        action="store_true",
        help="Close open positions when an opposite signal appears (default: disabled)",
    )

    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="Symbols to trade (default: all)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Optional override for data directory (defaults to config.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional override for output directory (defaults to config.json)",
    )
    parser.add_argument(
        "--winner-ledger",
        type=str,
        default=None,
        help="Optional override for the validated-winner ledger file (defaults to config.json or pm_configs.json)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--status", action="store_true", help="Print current status and exit"
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help=(
            "Run funded live-readiness checks, or DEMO readiness with --paper, "
            "and exit without starting the trading loop"
        ),
    )

    args = parser.parse_args()

    # Load runtime configuration before logger setup so config paths remain authoritative.
    resolved_config_path = Path(args.config).resolve()
    config_data = load_config_json(str(resolved_config_path))
    os.environ["PM_CONFIG_PATH"] = str(resolved_config_path)
    if args.trade and _is_research_discovery_only_config(config_data):
        print(
            f"Refusing --trade with research-only discovery config: {args.config}. "
            "Use this config with --optimize only."
        )
        return 2
    funded_contract_requested = bool(
        (args.preflight and not args.paper) or (args.trade and not args.paper)
    )
    if (
        funded_contract_requested
        or _config_scope(config_data) == FUNDED_LIVE_CONFIG_SCOPE
    ):
        try:
            _validate_funded_config_schema(config_data)
        except ValueError as exc:
            print(f"Refusing funded-live configuration {args.config}: {exc}")
            return 2

    # Prefer symbols from config.json unless CLI overrides
    symbols = args.symbols or config_data.get("symbols") or DEFAULT_SYMBOLS

    # Normalize MT5 config (allow terminal_path alias)
    mt5_section = dict(config_data.get("mt5") or {})
    if "terminal_path" in mt5_section and "path" not in mt5_section:
        mt5_section["path"] = mt5_section["terminal_path"]
    if "mt5" in config_data:
        config_data["mt5"] = mt5_section

    pipeline_section = dict(config_data.get("pipeline") or {})
    storage_section = dict(config_data.get("storage") or {})
    if storage_section:
        pipeline_section.update(
            _normalize_storage_section_for_pipeline(storage_section)
        )

    pipeline_config = PipelineConfig(
        **_filter_dataclass_kwargs(PipelineConfig, pipeline_section)
    )
    position_config = PositionConfig(
        **_filter_dataclass_kwargs(PositionConfig, config_data.get("position", {}))
    )
    mt5_config = MT5Config(**_filter_dataclass_kwargs(MT5Config, mt5_section))
    _apply_funded_pipeline_contract(config_data, pipeline_config)
    _apply_funded_position_cost_contract(config_data, pipeline_config, position_config)
    # Bind persisted optimization artifacts to the exact funded deployment
    # surface (broker specs, symbol roster, and PositionConfig). This attribute
    # is intentionally runtime-only and is consumed by build_artifact_meta.
    pipeline_config._deployment_config_path = str(resolved_config_path)
    pipeline_config.data_dir = _resolve_config_relative_path(
        pipeline_config.data_dir,
        resolved_config_path,
    )
    manual_kill_switch_path = str(
        getattr(pipeline_config, "manual_kill_switch_path", "") or ""
    ).strip()
    if manual_kill_switch_path:
        pipeline_config.manual_kill_switch_path = str(
            _resolve_config_relative_path(
                manual_kill_switch_path,
                resolved_config_path,
            )
        )
    # Runtime-only authority bit used by the final pre-send gate.  Paper and
    # research runs remain usable, while a funded entry can never fall back to
    # a mutable ledger status or the legacy review files.
    pipeline_config._funded_live_execution = bool(args.trade and not args.paper)
    if (
        bool(getattr(pipeline_config, "false_discovery_control_enabled", False))
        and int(getattr(pipeline_config, "false_discovery_symbol_count", 0) or 0) <= 0
    ):
        pipeline_config.false_discovery_symbol_count = len(symbols)

    # If position risk not set explicitly, inherit from pipeline risk (backward compatible)
    if "risk_per_trade_pct" not in (config_data.get("position") or {}) and hasattr(
        pipeline_config, "risk_per_trade_pct"
    ):
        position_config.risk_per_trade_pct = pipeline_config.risk_per_trade_pct
    pipeline_config.regime_tp_multipliers = dict(
        getattr(position_config, "regime_tp_multipliers", {}) or {}
    )

    if args.data_dir:
        pipeline_config.data_dir = _resolve_config_relative_path(
            args.data_dir,
            resolved_config_path,
        )
        pipeline_config.data_dir.mkdir(parents=True, exist_ok=True)
    if args.output_dir:
        pipeline_config.output_dir = Path(args.output_dir)
        pipeline_config.output_dir.mkdir(parents=True, exist_ok=True)
    if args.winner_ledger:
        pipeline_config.winner_ledger_path = args.winner_ledger

    # Setup logging after config resolution so log_dir is config-driven.
    setup_logging(log_dir=str(pipeline_config.log_dir), log_level=args.log_level)
    logger = logging.getLogger(__name__)

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Apply instrument spec config (single source of truth)
    broker_specs_path = config_data.get("broker_specs_path")
    if broker_specs_path:
        runtime_broker_specs_path = str(
            _resolve_config_relative_path(
                broker_specs_path,
                resolved_config_path,
            )
        )
        set_broker_specs_path(runtime_broker_specs_path)
        specs_loaded = {}
        for attempt in range(1, 4):
            # Force reload on each attempt
            set_broker_specs_path(runtime_broker_specs_path)
            specs_loaded = load_broker_specs(runtime_broker_specs_path)
            if specs_loaded:
                break
        if not specs_loaded and os.path.exists(runtime_broker_specs_path):
            logger.warning(
                "Broker specs file found but empty/unreadable after 3 attempts: "
                f"{runtime_broker_specs_path}"
            )
    set_instrument_specs(
        specs=config_data.get("instrument_specs"),
        defaults=config_data.get("instrument_spec_defaults"),
    )

    log_resolved_config_summary(
        logger, args.config, config_data, pipeline_config, position_config, mt5_config
    )

    if args.preflight and args.paper:
        # Keep this before FXPortfolioManagerApp construction. DEMO preflight
        # must be a bounded, order-free identity/readiness probe; it must not
        # initialize PortfolioManager or enter any execution path.
        return _run_bounded_demo_preflight(
            logger=logger,
            pipeline_config=pipeline_config,
            position_config=position_config,
            mt5_config=mt5_config,
            symbols=symbols,
            config_path=str(resolved_config_path),
        )

    _app_instance = FXPortfolioManagerApp(
        symbols=symbols,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        pipeline_config=pipeline_config,
        position_config=position_config,
        mt5_config=mt5_config,
        config_file=str(
            getattr(pipeline_config, "winner_ledger_path", "pm_configs.json")
        ),
    )

    try:
        # Initialize
        if not _app_instance.initialize():
            logger.error("Initialization failed")
            return 1

        # Status only
        if args.status:
            _app_instance.print_status()
            return 0

        if args.preflight:
            account_info = None
            try:
                if _app_instance.mt5 is not None and _app_instance.mt5.is_connected():
                    account_info = _app_instance.mt5.get_account_info()
            except Exception as exc:
                logger.warning(
                    f"Could not fetch MT5 account info for preflight validation: {exc}"
                )
            readiness = validate_live_readiness(
                pipeline_config,
                symbols=symbols,
                config_path=args.config,
                winner_ledger_path=getattr(
                    pipeline_config, "winner_ledger_path", None
                ),
                mt5_config=mt5_config,
                account_info=account_info,
                position_config=position_config,
            )
            log_readiness_report(logger, readiness)
            logger.info(
                "Preflight complete: %d error(s), %d warning(s)",
                len(readiness.errors),
                len(readiness.warnings),
            )
            return 0 if readiness.ok else 2

        # Run optimization
        if args.optimize:
            if not _app_instance.run_optimization(overwrite=args.overwrite):
                logger.error(
                    "Optimization generation did not complete and promote; "
                    "refusing a success exit status or subsequent trading startup"
                )
                return 2

        # Run trading
        if args.trade:
            if args.close_on_opposite_signal:
                if args.paper:
                    logger.error(
                        "Current-refit probation refuses --close-on-opposite-signal because "
                        "it would alter the refit's executable policy and contaminate forward evidence."
                    )
                else:
                    logger.error(
                        "Funded live refuses --close-on-opposite-signal because that exit "
                        "was not part of the frozen lockbox contract."
                    )
                return 2
            if not args.paper:
                account_info = None
                try:
                    if (
                        _app_instance.mt5 is not None
                        and _app_instance.mt5.is_connected()
                    ):
                        account_info = _app_instance.mt5.get_account_info()
                except Exception as exc:
                    logger.warning(
                        f"Could not fetch MT5 account info for readiness validation: {exc}"
                    )
                readiness = validate_live_readiness(
                    pipeline_config,
                    symbols=symbols,
                    config_path=args.config,
                    winner_ledger_path=getattr(
                        pipeline_config, "winner_ledger_path", None
                    ),
                    mt5_config=mt5_config,
                    account_info=account_info,
                    position_config=position_config,
                )
                log_readiness_report(logger, readiness)
                if not readiness.ok:
                    logger.error(
                        "Live readiness failed; refusing to start live trading."
                    )
                    return 2
            trading_started = _app_instance.run_trading(
                # --paper is executable demo forward probation, not a log-only
                # simulator. Broker-reported ACCOUNT_TRADE_MODE==DEMO remains
                # mandatory at startup, every sweep, and immediately pre-send.
                enable_trading=True,
                close_on_opposite_signal=False,
                execution_book=(
                    CURRENT_REFIT_DEMO_EXECUTION_BOOK
                    if args.paper
                    else CHAMPION_EXECUTION_BOOK
                ),
            )
            if trading_started is False:
                return 2

        # Default: show status
        if not args.optimize and not args.trade:
            _app_instance.print_status()
            print("\nUsage:")
            print(
                "  python pm_main.py --optimize              # Run optimization (skip valid)"
            )
            print("  python pm_main.py --optimize --overwrite  # Force re-optimize all")
            print("  python pm_main.py --trade                 # Start live trading")
            print(
                "  python pm_main.py --trade --paper         # Current-refit demo probation"
            )
            print("  python pm_main.py --preflight             # Readiness checks only")
            print(
                "  python pm_main.py --preflight --paper     # DEMO readiness only"
            )
            print(
                f"  production retrain schedule: {pipeline_config.describe_retrain_schedule()}"
            )

        return 0

    except Exception as e:
        logger.exception(f"Application error: {e}")
        return 1
    finally:
        if _app_instance:
            _app_instance.shutdown()


if __name__ == "__main__":
    sys.exit(main())
