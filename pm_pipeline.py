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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import product
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
    materialize_regime_params_from_symbol_config,
    regime_params_provenance_path,
    validate_regime_params_provenance,
    validated_regime_params_for_timeframe,
)
from pm_order_governance import candidate_policy_names, make_policy, policy_name_from_artifact
from pm_fairness_audit import build_fairness_evidence, build_fairness_report
from pm_regime_audit import audit_regime_profitability
from pm_statistics import (
    LockboxAccessRegistry,
    STATISTICS_CONTRACT_VERSION,
    aligned_cell_pbo,
    block_bootstrap_evidence,
    deflated_sharpe_evidence,
)
from pm_model_lifecycle import (
    EDGE_CHALLENGER_ROLE,
    build_edge_challenger_lineage,
    build_empty_edge_challenger_book,
    build_lockbox_champion_lineage,
    build_prelockbox_champion_lineage,
    temporal_role_block_reason,
)
from pm_two_phase_lockbox import FROZEN_PRELOCKBOX_STATUS
from pm_native_activation import (
    NATIVE_PRODUCTION_TIMEFRAMES,
    resolve_active_native_universe,
    validate_native_exclusion_no_trade_grid,
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
PIPELINE_ARTIFACT_VERSION = "2026-07-11-two-book-nonoverlap-deployment-v5"


def _stable_seed(*parts: Any) -> int:
    """Deterministic non-negative seed derived from stable context parts."""
    payload = "|".join(str(part) for part in parts).encode("utf-8", errors="replace")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFF


def _stable_hash(payload: Any) -> str:
    """Create a stable SHA1 hash for JSON-serializable payloads."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


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
        "oos_evidence_record_schema": "oos_candidate_returns_v3_spoke_evidence",
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
                if dsr_status != "PASS" or dsr < min_dsr:
                    false_discovery_reasons.append(
                        f"false-discovery daily-return DSR {dsr_status} {dsr:.4f} < {min_dsr:.4f}"
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


def _optimize_symbol_worker(config_dict: Dict[str, Any], symbol: str) -> Tuple[str, 'PipelineResult']:
    """Worker for parallel optimization."""
    try:
        cfg = PipelineConfig(**config_dict)
        pipeline = OptimizationPipeline(cfg)
        result = pipeline.run_for_symbol(symbol)
        return symbol, result
    except Exception as exc:
        return symbol, PipelineResult(symbol=symbol, success=False, error_message=str(exc))
    finally:
        FeatureComputer.clear_cache()


def _precompute_feature_cache_worker(config_dict: Dict[str, Any], symbol: str) -> Tuple[str, Dict[str, Any]]:
    """Worker for disk-backed feature-cache precompute."""
    try:
        cfg = PipelineConfig(**config_dict)
        pipeline = OptimizationPipeline(cfg)
        stats = pipeline.precompute_feature_cache([symbol])
        return symbol, stats
    except Exception as exc:
        return symbol, {
            "symbols_requested": 1,
            "symbols_failed": 1,
            "errors": {symbol: str(exc)},
        }
    finally:
        FeatureComputer.clear_cache()


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
        if not self._loaded:
            self.load()
        else:
            self._refresh_if_changed()
        
        previous = self.configs.get(symbol)
        self.configs[symbol] = config
        
        try:
            self._atomic_save()
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
        self._refresh_if_changed()
        
        if symbol not in self.configs:
            return False
        
        del self.configs[symbol]
        
        try:
            self._atomic_save()
            logger.info(f"Removed {symbol} from {self.filepath}")
            return True
        except Exception as e:
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
        
        # Generate combinations
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        combinations = list(product(*param_values))
        
        # Limit combinations
        if len(combinations) > self.config.max_param_combos:
            # Deterministic but unique seed derived from strategy context (X2)
            _seed = _stable_seed(
                symbol,
                strategy_name,
                "grid",
                len(train_features),
                str(train_features.index[0]) if len(train_features) else "",
                str(train_features.index[-1]) if len(train_features) else "",
                len(val_features),
            )
            np.random.seed(_seed)
            indices = np.random.choice(len(combinations), self.config.max_param_combos, replace=False)
            combinations = [combinations[i] for i in indices]
        
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

    def _make_no_trade_marker(self, reason: str, now: Optional[datetime] = None) -> RegimeConfig:
        """Create an explicit non-tradeable marker for a regime slot with no validated winner."""
        marker_time = now or datetime.now()
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
            artifact_meta={"no_trade": True, "reason": reason},
            validation_evidence={
                "validation_reason": reason,
                "tradeable": False,
                "no_trade": True,
            },
            promotion_status="NO_TRADE",
            promotion_reasons=[reason],
        )
    
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
                        eligibility_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None,
                        selection_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None,
                        selection_warmup_by_tf: Optional[Dict[str, int]] = None,
                        selection_surface_by_tf: Optional[Dict[str, str]] = None,
                        historical_features_by_tf: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[Dict[str, Dict[str, RegimeConfig]], Optional[RegimeConfig], int, int]:
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
            val_features = val_features_by_tf.get(tf)
            eligibility_features = (
                None if eligibility_features_by_tf is None else eligibility_features_by_tf.get(tf)
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
            
            if train_features is None or len(train_features) < 200:
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
                val_warmup_bars=val_warmup_bars,
                eligibility_features=eligibility_features,
                selection_features=selection_features,
                selection_warmup_bars=selection_warmup_bars,
                selection_surface=selection_surface,
                historical_features=historical_features,
            )
            
            if not tf_candidates:
                logger.debug(f"[{symbol}] [{tf}] No valid candidates")
                now = datetime.now()
                for regime in self.REGIMES:
                    reason = "No strategies passed training eligibility or produced trades"
                    regime_configs[tf][regime] = self._make_no_trade_marker(reason, now=now)
                    unvalidated_winners += 1
                continue
            
            # Select best per regime (with validation)
            for regime in self.REGIMES:
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
                        regime_configs[tf][regime] = self._make_no_trade_marker(val_reason)
                        logger.debug(f"[{symbol}] [{tf}] [{regime}] Candidate {best_config.strategy_name} "
                                    f"FAILED validation: {val_reason}")
                else:
                    # NO WINNER - No fallback to best train
                    unvalidated_winners += 1
                    regime_configs[tf][regime] = self._make_no_trade_marker(val_reason)
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
    def _compact_backtest_result(result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Keep scalar metrics only after trades were bucketed."""
        if not isinstance(result, dict):
            return {}
        return {
            key: value
            for key, value in result.items()
            if key not in {"equity_curve", "trades"}
        }

    @staticmethod
    def _compact_metric_tree(value: Any) -> Any:
        """JSON-safe scalar/nested metric snapshot for evidence manifests."""
        if isinstance(value, dict):
            return {
                str(key): RegimeOptimizer._compact_metric_tree(item)
                for key, item in value.items()
                if key not in {"equity_curve", "trades"}
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
        candidate_key = {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": evaluation_regime,
            "strategy_name": strategy_name,
            "parameters": params,
        }
        candidate_id = _stable_hash(candidate_key)
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
        return {
            "format_version": "oos_candidate_returns_v3",
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
                strategy = cand.get("strategy")
                if strategy is None:
                    strategy = StrategyRegistry.get(cand.get("strategy_name", ""), **(cand.get("params", {}) or {}))
                raw_signals = strategy.generate_signals(val_features, symbol)
                signals = self._gate_signals_to_exact_regime(
                    raw_signals,
                    val_features,
                    evaluation_regime,
                )
                val_result = self.backtester.run(
                    val_features,
                    signals,
                    symbol,
                    strategy,
                    timeframe=timeframe,
                    warmup_bars=val_warmup_bars,
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
                record = self._build_oos_evidence_record(
                    symbol,
                    timeframe,
                    cand,
                    val_result,
                    val_features,
                    val_warmup_bars,
                    stress_result=stress_result,
                    stress_mode=stress_mode,
                )
                if record:
                    cand["oos_evidence"] = record
                    cost_evidence = record.get("execution_cost_survival")
                    if isinstance(cost_evidence, dict):
                        cand["selection_execution_cost_survival"] = cost_evidence
                    records_by_id[record["candidate_id"]] = record
            except Exception as exc:
                logger.debug(
                    f"[{symbol}] [{timeframe}] OOS evidence skipped for "
                    f"{cand.get('strategy_name', 'Unknown')}: {exc}"
                )
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
            except Exception as exc:
                logger.debug(
                    f"{strategy.name} screening ATR anchor {anchor} skipped: {exc}"
                )
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
    ) -> Tuple[Dict[str, Any], bool]:
        """Run one exact-regime cell and report whether Backtester.run executed."""
        raw_signals = strategy.generate_signals(features, symbol)
        gated = self._gate_signals_to_exact_regime(raw_signals, features, regime)
        if gated.abs().sum() == 0 and not run_zero_signal_path:
            return {
                "total_trades": 0,
                "regime_gated": True,
                "evaluation_regime": str(regime),
            }, False
        result = self.backtester.run(
            features,
            gated,
            symbol,
            strategy,
            warmup_bars=int(max(0, warmup_bars)),
            timeframe=timeframe,
        )
        return self._compact_exact_regime_result(result, regime), True

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
            if gated.abs().sum() == 0:
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
                warmup_bars=int(max(0, warmup_bars)),
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
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Freeze the train-best B0 candidate independently for every cell."""
        candidates: List[Dict[str, Any]] = []
        cell_records: List[Dict[str, Any]] = []
        for base_strategy in strategies:
            for regime in self.REGIMES:
                specs = self._family_probe_param_specs(
                    base_strategy,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                )
                best_score = float("-inf")
                best_candidate: Optional[Dict[str, Any]] = None
                completed_trials = 0
                eligible_train_trials = 0
                for params, atr_anchor in specs:
                    try:
                        probe_strategy = StrategyRegistry.get(base_strategy.name, **params)
                        metrics, ran = self._run_one_exact_regime(
                            symbol=symbol,
                            timeframe=timeframe,
                            features=train_features,
                            strategy=probe_strategy,
                            regime=regime,
                        )
                        completed_trials += int(ran)
                        if int(metrics.get("total_trades", 0) or 0) <= 0:
                            continue
                        score = float(self._compute_train_regime_score(metrics, {}))
                        if not math.isfinite(score):
                            continue
                        eligible_train_trials += 1
                        if score > best_score:
                            best_score = score
                            clean_params = self._clean_candidate_params(
                                probe_strategy.get_params()
                            )
                            best_candidate = {
                                "strategy": probe_strategy,
                                "strategy_name": probe_strategy.name,
                                "params": clean_params,
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
                            }
                    except Exception as exc:
                        logger.debug(
                            "[%s] [%s] [%s] B0 probe failed for %s: %s",
                            symbol, timeframe, regime, base_strategy.name, exc,
                        )
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
            metrics: Dict[str, Any] = {"total_trades": 0}
            if val_features is not None and len(val_features) >= 50:
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
                    )
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    metrics = {"total_trades": 0, "evaluation_error": error}
            candidate["val_regime_metrics"] = {regime: metrics}
            candidate["tuning_validation_evaluations"] = int(evaluations)
            candidate["tuning_validation_evaluation_error"] = error
            record = records.get((family, regime))
            if record is not None:
                record["tuning_validation_evaluations"] = int(evaluations)
                record["tuning_validation_evaluation_error"] = error

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
                            eligibility_features: Optional[pd.DataFrame] = None,
                            selection_features: Optional[pd.DataFrame] = None,
                            selection_warmup_bars: int = 0,
                            selection_surface: str = "validation",
                            historical_features: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
        """
        Collect all strategy candidates with per-regime metrics.
        
        Phase B0: give every family-regime cell the same bounded train-only probe.
        Phase B1: run independent deeper train-only studies for B0 top-K cells.
        
        Each frozen train winner is evaluated once on tuning validation only
        after its parameters are fixed. Returns exact-regime candidate mappings.
        """
        # ``eligibility_features`` remains accepted for API compatibility, but
        # no family gate or shortlist may inspect rows outside optimizer train.
        _ = eligibility_features
        screening_candidates, probe_cells = self._collect_family_probe_candidates(
            symbol=symbol,
            timeframe=timeframe,
            train_features=train_features,
            strategies=strategies,
        )

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

        # Validation is touched only after the B0 train winner in each cell is frozen.
        self._attach_frozen_probe_validation(
            symbol=symbol,
            timeframe=timeframe,
            val_features=val_features,
            val_warmup_bars=val_warmup_bars,
            candidates=eligible_candidates,
            cell_records=probe_cells,
        )

        if not self.enable_hyperparam_tuning:
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

            # Run grid search
            tuned_results = self._tune_strategy_params(
                symbol, timeframe, strategy_name, param_grid,
                train_features, val_features, val_warmup_bars,
                regimes=target_regimes,
            )

            tuned_candidates.extend(tuned_results)
            if self._last_tuning_search_meta:
                tuning_summaries.append(dict(self._last_tuning_search_meta))

        # Combine eligible candidates with tuned candidates
        # The tuned candidates may replace or augment the defaults
        all_candidates = eligible_candidates + tuned_candidates
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
                
                train_trades = train_metrics.get('total_trades', 0)
                
                # Only consider strategies with enough trades in this regime
                if train_trades < screening_trade_floor:  # Relaxed, but never admits an empty cell.
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
                ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
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
                              regimes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
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
                val_warmup_bars=val_warmup_bars,
            )
            
            # Convert to candidate list format
            tuned_candidates = []
            self._last_tuning_search_meta = dict(
                getattr(self.optuna_optimizer, "last_regime_search_metadata", {}) or {}
            )
            for regime, candidate in regime_results.items():
                candidate['tuned_for_regime'] = regime
                candidate['train_result'] = self._compact_backtest_result(
                    candidate.get('train_result')
                )
                candidate['family_search_metadata'] = dict(
                    self._last_tuning_search_meta
                )
                tuned_candidates.append(candidate)
            
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
            val_warmup_bars,
            regimes=target_regimes,
        )
    
    def _tune_strategy_params_grid(self,
                                   symbol: str,
                                   timeframe: str,
                                   strategy_name: str,
                                   param_grid: Dict[str, List],
                                   train_features: pd.DataFrame,
                                   val_features: Optional[pd.DataFrame],
                                   val_warmup_bars: int = 0,
                                   regimes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fallback grid search for hyperparameter tuning.
        
        Returns list of tuned candidate dicts with regime metrics.
        """
        from itertools import product
        
        # Generate param combinations
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        all_combos = list(product(*param_values))
        
        # Get default params as base
        base_strategy = StrategyRegistry.get(strategy_name)
        default_params = base_strategy.get_params()
        target_regimes = [
            regime for regime in self.REGIMES
            if regime in set(regimes or self.REGIMES)
        ]
        tuned_candidates: List[Dict[str, Any]] = []
        cell_records: List[Dict[str, Any]] = []
        for regime in target_regimes:
            if len(all_combos) > self.hyperparam_max_combos:
                seed = _stable_seed(
                    symbol,
                    timeframe,
                    regime,
                    strategy_name,
                    "regime_grid_train_only",
                    len(train_features),
                )
                rng = np.random.default_rng(seed)
                indices = rng.choice(
                    len(all_combos), self.hyperparam_max_combos, replace=False
                )
                combos = [all_combos[int(index)] for index in indices]
            else:
                combos = list(all_combos)
            best_score = float("-inf")
            best_candidate: Optional[Dict[str, Any]] = None
            completed_trials = 0
            eligible_train_trials = 0
            early_dd_rejections = 0
            for combo in combos:
                params = {**default_params, **dict(zip(param_names, combo))}
                try:
                    test_strategy = StrategyRegistry.get(strategy_name, **params)
                    train_m, ran = self._run_one_exact_regime(
                        symbol=symbol,
                        timeframe=timeframe,
                        features=train_features,
                        strategy=test_strategy,
                        regime=regime,
                    )
                    completed_trials += int(ran)
                    if int(train_m.get("total_trades", 0) or 0) < self.min_train_trades:
                        continue
                    if float(train_m.get("max_drawdown_pct", 100.0) or 100.0) > (
                        self.val_max_drawdown * 1.25
                    ):
                        early_dd_rejections += 1
                        continue
                    score = float(self._compute_train_regime_score(train_m, {}))
                    if not math.isfinite(score):
                        continue
                    eligible_train_trials += 1
                    if score > best_score:
                        best_score = score
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
                        }
                except Exception as exc:
                    logger.debug(
                        "[%s] [%s] [%s] %s train-only grid trial failed: %s",
                        symbol, timeframe, regime, strategy_name, exc,
                    )
            evaluations = 0
            evaluation_error: Optional[str] = None
            if best_candidate is not None:
                best_candidate["search_completed_trials"] = int(completed_trials)
                best_candidate["eligible_train_trials"] = int(eligible_train_trials)
                val_m: Dict[str, Any] = {"total_trades": 0}
                if val_features is not None and len(val_features) >= 50:
                    evaluations = 1
                    try:
                        frozen_strategy = StrategyRegistry.get(
                            strategy_name, **dict(best_candidate["params"])
                        )
                        best_candidate["strategy"] = frozen_strategy
                        val_m, _ = self._run_one_exact_regime(
                            symbol=symbol,
                            timeframe=timeframe,
                            features=val_features,
                            strategy=frozen_strategy,
                            regime=regime,
                            warmup_bars=val_warmup_bars,
                            run_zero_signal_path=True,
                        )
                    except Exception as exc:
                        evaluation_error = f"{type(exc).__name__}: {exc}"
                        val_m = {
                            "total_trades": 0,
                            "evaluation_error": evaluation_error,
                        }
                best_candidate["val_regime_metrics"] = {regime: val_m}
                best_candidate["tuning_validation_evaluations"] = int(evaluations)
                best_candidate["tuning_validation_evaluation_error"] = evaluation_error
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
                                        pbo_evidence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        # Multiplicity is the actual attempted train-only trial count persisted
        # by the optimizer. Estimated symbol/timeframe multipliers are reported
        # separately but are never presented as observations that did not run.
        effective_trials = max(local_trials, candidate_trials, 1)
        min_daily = int(
            getattr(self.config, "false_discovery_min_daily_observations", 252) or 252
        )
        min_effective = float(
            getattr(self.config, "false_discovery_min_effective_observations", 60.0)
            or 60.0
        )
        min_dsr = float(getattr(self.config, 'false_discovery_min_dsr', 0.95) or 0.95)
        daily_returns = list(val_metrics.get("daily_realized_returns", []) or [])
        dsr_evidence = deflated_sharpe_evidence(
            daily_returns,
            attempted_trials=effective_trials,
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
            (candidate.get("oos_evidence") or {}).get("candidate_id")
            or candidate.get("candidate_id")
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
            "val_sharpe": val_sharpe,
            "val_trades": val_trades,
            "local_trials": int(local_trials),
            "local_search_cells": int(local_cells),
            "local_scope_includes_regimes": bool(local_includes_regimes),
            "candidate_trials": int(candidate_trials),
            "scope_multiplier": int(scope_multiplier),
            "effective_trials": int(effective_trials),
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

    def _build_legacy_confidence_evidence(self, val_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Build positive-edge confidence evidence for post-search legacy reranking."""
        r_count = int(val_metrics.get("r_count", val_metrics.get("total_trades", 0)) or 0)
        mean_r = float(val_metrics.get("mean_r", 0.0) or 0.0)
        std_r = float(val_metrics.get("std_r", 0.0) or 0.0)
        se_r = float(val_metrics.get("se_r", 0.0) or 0.0)
        if se_r <= 1e-12 and r_count >= 2 and std_r > 1e-12:
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
        cell_pbo_evidence = (
            aligned_cell_pbo(
                candidates,
                regime,
                min_candidates=int(
                    getattr(self.config, "false_discovery_pbo_min_candidates", 5) or 5
                ),
                min_observations=int(
                    getattr(self.config, "false_discovery_min_daily_observations", 252)
                    or 252
                ),
                max_pbo=float(
                    getattr(self.config, "false_discovery_max_pbo", 0.50) or 0.50
                ),
                blocks=int(
                    getattr(self.config, "false_discovery_pbo_blocks", 10) or 10
                ),
            )
            if false_discovery_enabled
            else {"status": "DISABLED", "passed": True, "pbo": None}
        )

        for cand in candidates:
            train_metrics = dict(
                (cand.get('train_regime_metrics') or {}).get(regime, {}) or {}
            )
            val_metrics = dict(
                (cand.get('val_regime_metrics') or {}).get(regime, {}) or {}
            )
            surface_failure = self._selection_surface_failure(
                train_metrics,
                label="train",
            ) or self._selection_surface_failure(
                val_metrics,
                label="validation",
            )
            if surface_failure:
                rejection_reasons.append(
                    f"{cand.get('strategy_name', 'Unknown')}: {surface_failure}"
                )
                continue

            train_trades = train_metrics.get('total_trades', 0)
            val_trades = val_metrics.get('total_trades', 0)

            # Check minimum trades
            if train_trades < self.min_train_trades:
                continue
            if val_trades < min_val_trades:
                continue

            # ===== EARLY REJECTION: Check drawdown before scoring =====
            train_dd = train_metrics.get('max_drawdown_pct', 100.0)
            val_dd = val_metrics.get('max_drawdown_pct', 100.0)

            # Reject if validation drawdown exceeds threshold
            if val_dd > self.val_max_drawdown:
                continue

            # Reject if training drawdown is excessively high (1.25x threshold)
            if train_dd > self.val_max_drawdown * 1.25:
                continue

            # Return-to-DD efficiency gate (unconditional, same category as DD caps)
            val_return_early = val_metrics.get('total_return_pct', -100.0)
            val_return_dd_ratio = self._compute_return_dd_ratio(val_return_early, val_dd)
            if val_return_dd_ratio < self.min_val_return_dd_ratio:
                rejection_reasons.append(
                    f"{cand['strategy_name']}: return/DD={val_return_dd_ratio:.2f} < "
                    f"{self.min_val_return_dd_ratio:.2f} (ret={val_return_early:.1f}%, dd={val_dd:.1f}%)"
                )
                continue

            # Reject clearly losing validation candidates before scoring.
            val_pf = val_metrics.get('profit_factor', 0.0)
            val_return = val_metrics.get('total_return_pct', -100.0)

            if not self.allow_losing_winners:
                if val_pf < self.min_val_profit_factor:
                    rejection_reasons.append(f"{cand['strategy_name']}: PF={val_pf:.2f} < {self.min_val_profit_factor}")
                    continue
                if val_return < self.min_val_return_pct:
                    rejection_reasons.append(f"{cand['strategy_name']}: return={val_return:.1f}% < {self.min_val_return_pct}")
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
            )
            min_dsr = float(false_discovery.get("min_dsr", 0.0) or 0.0)
            dsr = float(false_discovery.get("deflated_sharpe", 0.0) or 0.0)
            if bool(false_discovery.get("enabled", False)):
                dsr_status = str(
                    (false_discovery.get("dsr_evidence") or {}).get("status", "UNAVAILABLE")
                ).upper()
                if dsr_status != "PASS" or dsr < min_dsr:
                    rejection_reasons.append(
                        f"{cand['strategy_name']}: daily-return DSR {dsr_status} "
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
                cand["_threadspark_l1_base_score"] = self._compute_threadspark_l1_score(
                    train_metrics,
                    val_metrics,
                    cand,
                )
                score = self._compute_threadspark_spoke_score(
                    train_metrics,
                    val_metrics,
                    candidate=cand,
                    regime=regime,
                )
            else:
                score = self._call_regime_score(train_metrics, val_metrics, candidate=cand)
            legacy_confidence = None
            if scoring_model == "legacy_confidence":
                legacy_confidence = self._build_legacy_confidence_evidence(val_metrics)
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
                    -int(item[1]['train_regime_metrics'].get(regime, {}).get('total_trades', 0) or 0),
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
                    train_metrics, val_metrics, regime, strategy_name, timeframe=timeframe
                )
            else:
                is_validated, validation_reason = self._validate_regime_winner(
                    train_metrics, val_metrics, regime, strategy_name
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
                train_metrics, val_metrics, regime, best_candidate['strategy_name'], timeframe=timeframe
            )
        else:
            is_validated, validation_reason = self._validate_regime_winner(
                train_metrics, val_metrics, regime, best_candidate['strategy_name']
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
                                 timeframe: Optional[str] = None) -> Tuple[bool, str]:
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
        if val_trades < min_val_trades:
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
            exceptional_val_min_trades = max(min_val_trades * 2, 50)
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
        train_full = self._bucket_to_full_metrics(train_metrics)
        val_full = self._bucket_to_full_metrics(val_metrics)
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
        train_full = self._bucket_to_full_metrics(train_metrics or {})
        val_full = self._bucket_to_full_metrics(val_metrics or {})
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
            try:
                strategy = StrategyRegistry.get(candidate.get("strategy_name", ""), **(candidate.get("params", {}) or {}))
            except Exception:
                strategy = None
            evidence["fairness_evidence"] = build_fairness_evidence(candidate, strategy=strategy)
            oos_evidence = dict(candidate.get("oos_evidence") or {})
            if oos_evidence.get("candidate_id"):
                evidence["candidate_id"] = str(oos_evidence["candidate_id"])
            evidence["evaluation_regime"] = self._candidate_evaluation_regime(candidate)
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
        train_full = self._bucket_to_full_metrics(train_metrics or {})
        base_score = float(self.scorer.score(train_full, purpose="selection"))
        trades = int((train_metrics or {}).get("total_trades", 0) or 0)
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
        train_trades = train_metrics.get('total_trades', 0)
        val_trades = val_metrics.get('total_trades', 0)

        scoring_model = str(getattr(self.config, "regime_scoring_model", "legacy") or "legacy").lower()
        if scoring_model == "specialist_spokes":
            return self._compute_specialist_spoke_score(train_metrics, val_metrics, candidate)
        if scoring_model == "specialist_spokes_threadspark":
            return self._compute_threadspark_spoke_score(train_metrics, val_metrics, candidate)
        if scoring_model == "specialist_spokes_v2":
            return self._compute_specialist_spoke_v2_score(train_metrics, val_metrics, candidate)
        if scoring_model == "specialist_spokes_asym":
            return self._compute_asym_specialist_spoke_score(train_metrics, val_metrics, candidate)

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
        train_full = self._bucket_to_full_metrics(train_metrics)
        val_full = self._bucket_to_full_metrics(val_metrics)

        # Use validation metrics if sufficient trades
        if val_trades >= self.min_val_trades:
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
        last_source = (
            pd.to_datetime(df["_SOURCE_LAST_TS"], errors="coerce")
            if "_SOURCE_LAST_TS" in df.columns else pd.Series(df.index, index=df.index)
        )
        if start_ts is not None:
            mask &= first_source >= start_ts
        if end_ts is not None:
            mask &= last_source < end_ts
        return df.loc[mask].copy()

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
        windows = dict(data_window_meta.get("windows", {}) or {})
        partition_bounds: Dict[str, Tuple[Any, Any]] = {}
        for partition in ("optimizer_train", "tuning_validation", "final_lockbox"):
            meta = dict(windows.get(partition, {}) or {})
            start = meta.get("start_ts")
            end = meta.get("end_ts") or meta.get("end_ts_exclusive")
            if start not in (None, "") and end not in (None, ""):
                partition_bounds[partition] = (start, end)
        return validate_timeframe_evidence(
            timeframe,
            partition_frames,
            source_meta=source_meta,
            window_years=getattr(self.config, "evidence_window_years", None),
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

    def _evidence_no_trade_markers(self, verdict: EvidenceVerdict) -> Dict[str, RegimeConfig]:
        """Create an explicit NO_TRADE slot for every regime without optimizing."""
        reason = verdict.concise_reason()
        now = datetime.now()
        regimes = tuple(getattr(self.regime_optimizer, "REGIMES", ()) or ())
        if not regimes:
            regimes = ("TREND", "RANGE", "BREAKOUT", "CHOP")
        return {
            regime: self.regime_optimizer._make_no_trade_marker(reason, now=now)
            for regime in regimes
        }

    def _build_calendar_workflow_meta(
        self,
        base_m5: pd.DataFrame,
        *,
        as_of_utc: Any,
    ) -> Dict[str, Any]:
        """Build common timestamp windows without fixed-M5-row truncation."""
        ordered = ensure_utc_index(base_m5)
        if ordered.empty:
            return {}
        as_of = pd.Timestamp(as_of_utc)
        if as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        else:
            as_of = as_of.tz_convert("UTC")
        purge = derive_purge_duration(
            configured_days=getattr(self.config, "evidence_purge_days", 7.0),
            split_embargo_bars=getattr(self.config, "split_embargo_bars", 0),
        )
        boundaries = build_calendar_boundaries(
            as_of,
            window_years=getattr(self.config, "evidence_window_years", None),
            embargo=purge,
        )
        train_start, train_end = boundaries.windows["optimizer_train"]
        validation_start, validation_end = boundaries.windows["tuning_validation"]
        lockbox_start, lockbox_end = boundaries.windows["final_lockbox"]
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
            "base_timeframe": "M5",
            "production_split_mode": CALENDAR_SPLIT_MODE,
            "split_contract_version": EVIDENCE_CONTRACT_VERSION,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
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
            "evidence_window_years": dict(
                getattr(self.config, "evidence_window_years", {}) or {}
            ),
            "calendar_boundaries": boundaries.to_dict(),
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
        native_gate_errors: Dict[str, List[str]] = {}

        if strict_native:
            if "M5" not in configured_timeframes:
                native_gate_errors["M5"] = ["NATIVE_M5_TIMEFRAME_REQUIRED"]
            for tf_name in configured_timeframes:
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
                        "provenance_errors": [
                            f"NATIVE_LOAD_RAISED:{type(exc).__name__}"
                        ],
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
                    preloaded_native[tf_name] = native_data.copy()

            if native_gate_errors:
                logger.error(
                    "[%s] Calendar native source gate failed before legacy data/search: %s",
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
                    "timeframe_sources": preloaded_meta,
                }
            base_m5 = preloaded_native["M5"].copy()
            base_source_meta = dict(preloaded_meta.get("M5") or {})
        else:
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
                    "strict_no_fallback": strict_native,
                    "errors_by_timeframe": {
                        "M5": ["M5_WORKFLOW_REQUIRES_AT_LEAST_200_BARS"]
                    },
                },
                "timeframe_sources": preloaded_meta,
            }
        as_of_utc: Optional[pd.Timestamp] = None
        if strict_native:
            base_m5 = ensure_utc_index(base_m5)
            configured_cutoff = str(
                getattr(self.config, "evidence_as_of_utc", "") or ""
            ).strip()
            as_of_utc = latest_closed_bar_end(
                base_m5,
                "M5",
                not_after_utc=configured_cutoff or datetime.now(timezone.utc),
            )
            if as_of_utc is None:
                logger.warning(f"[{symbol}] No fully closed native M5 bar at evidence cutoff")
                return {}, {}
            base_m5 = trim_to_closed_bars(base_m5, "M5", as_of_utc=as_of_utc)
        else:
            base_m5 = base_m5.sort_index()
        max_bars = int(getattr(self.config, "max_bars", 300000) or 300000)
        if (
            split_mode != CALENDAR_SPLIT_MODE
            and max_bars > 0
            and len(base_m5) > max_bars
        ):
            base_m5 = base_m5.tail(max_bars).copy()

        workflow_meta = self._build_m5_workflow_meta(base_m5, as_of_utc=as_of_utc)
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
        data_by_tf: Dict[str, pd.DataFrame] = {}
        timeframe_sources: Dict[str, Dict[str, Any]] = {}
        for tf_name in configured_timeframes:
            try:
                if tf_name == "M5":
                    data = base_m5.copy()
                    timeframe_meta = dict(base_source_meta or {})
                elif strict_native:
                    data = preloaded_native[tf_name].copy()
                    timeframe_meta = dict(preloaded_meta.get(tf_name) or {})
                else:
                    native_data, native_meta = self.data_loader.load_native_timeframe(symbol, tf_name)
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
                if split_mode == CALENDAR_SPLIT_MODE:
                    data = trim_to_closed_bars(data, tf_name, as_of_utc=as_of_utc)
                timeframe_meta["workflow_rows"] = int(len(data)) if data is not None else 0
                timeframe_meta["as_of_utc"] = (
                    as_of_utc.isoformat() if as_of_utc is not None else None
                )
                timeframe_sources[tf_name] = timeframe_meta
                if data is not None and len(data) > 0:
                    data_by_tf[tf_name] = data.copy()
            except Exception as exc:
                logger.warning(f"[{symbol}] [{tf_name}] Data preparation failed: {exc}")
                timeframe_sources[tf_name] = {
                    "source_kind": "error",
                    "symbol": symbol,
                    "timeframe": tf_name,
                    "provenance_valid": False,
                    "provenance_errors": [f"DATA_PREPARATION_FAILED:{type(exc).__name__}"],
                }
        if strict_native:
            empty_after_cutoff = sorted(set(configured_timeframes) - set(data_by_tf))
            if empty_after_cutoff:
                for tf_name in empty_after_cutoff:
                    native_gate_errors[tf_name] = [
                        "NATIVE_CLOSED_WORKFLOW_EMPTY_OR_PREPARATION_FAILED"
                    ]
                workflow_meta["timeframe_sources"] = timeframe_sources
                workflow_meta["native_source_gate"] = {
                    "passed": False,
                    "strict_no_fallback": True,
                    "errors_by_timeframe": native_gate_errors,
                }
                return {}, workflow_meta
            workflow_meta["native_source_gate"] = {
                "passed": True,
                "strict_no_fallback": True,
                "timeframes": list(configured_timeframes),
            }
        workflow_meta["timeframe_sources"] = timeframe_sources
        workflow_meta["evidence_contract_version"] = EVIDENCE_CONTRACT_VERSION
        workflow_meta["data_window_fingerprint"] = _stable_hash({
            "symbol": symbol,
            "source": workflow_meta["source"],
            "timeframe_sources": timeframe_sources,
            "workflow_window": workflow_window,
            "as_of_utc": workflow_meta.get("as_of_utc"),
            "windows": workflow_meta.get("windows", {}),
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
                    bucket_metrics = self.regime_optimizer._compute_bucket_metrics(
                        list(holdout_result.get("trades", []) or []),
                        features=holdout_features.iloc[holdout_warmup:].copy(),
                        timeframe=tf,
                    )
                    lockbox_bootstrap = block_bootstrap_evidence(
                        bucket_metrics.get("daily_realized_returns", []) or [],
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
                    if str(lockbox_bootstrap.get("status", "UNAVAILABLE")).upper() != "PASS":
                        compact["policy_status"] = "FAIL"
                        compact["policy_reason"] = (
                            "final-lockbox block bootstrap did not establish positive edge"
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
        min_trades = int(getattr(self.config, "historical_audit_min_trades", 5) or 0)
        max_dd = float(getattr(self.config, "historical_audit_max_drawdown", 35.0) or 35.0)
        min_pf = float(getattr(self.config, "historical_audit_min_profit_factor", 0.60) or 0.0)
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
                "record_schema": "oos_candidate_returns_v3_spoke_evidence",
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
            "record_schema": "oos_candidate_returns_v3_spoke_evidence",
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
            edge_challenger_configs: Dict[str, Dict[str, Dict[str, Any]]] = {}
            edge_challenger_failures: List[str] = []
            
            regime_params_file = getattr(self.config, 'regime_params_file', 'regime_params.json')

            # The active-recent M5 workflow always emits data_window_meta; bail if it
            # didn't (means _load_symbol_timeframe_data returned an empty window).
            if not data_window_meta:
                logger.warning(f"[{symbol}] Empty workflow window; skipping symbol")
                result.error_message = "Empty workflow window"
                return result

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

            # Regime labels determine every specialist bucket and are therefore
            # part of model selection.  In chronological-lockbox mode, reject
            # the symbol before computing a feature or ranking a candidate when
            # the detector parameters cannot be proven to come from this exact
            # optimizer-train source/window. Calendar v2 is native-timeframe
            # authoritative; legacy v1 remains M5-derived.
            if split_mode in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
                provenance_checks: Dict[str, Dict[str, Any]] = {}
                current_source = dict(data_window_meta.get("source", {}) or {})
                timeframe_sources = dict(data_window_meta.get("timeframe_sources", {}) or {})
                expected_contract = (
                    EVIDENCE_CONTRACT_VERSION
                    if split_mode == CALENDAR_SPLIT_MODE
                    else DataSplitter.SPLIT_CONTRACT_VERSION
                )
                for tf, tf_data in data_by_tf.items():
                    if split_mode == CALENDAR_SPLIT_MODE:
                        expected_source = dict(timeframe_sources.get(tf, {}) or {})
                        expected_train_window = calendar_window_meta(
                            tf_data,
                            opt_train_start,
                            opt_train_end,
                        )
                    else:
                        expected_source = current_source
                        expected_train_window = opt_train_meta
                    check = validate_regime_params_provenance(
                        symbol,
                        tf,
                        regime_params_file,
                        expected_source=expected_source,
                        expected_train_window=expected_train_window,
                        expected_split_contract=expected_contract,
                    )
                    provenance_checks[str(tf).upper()] = {
                        "valid": bool(check.get("valid", False)),
                        "reason": str(check.get("reason", "unknown")),
                        "provenance_path": check.get("provenance_path"),
                    }
                data_window_meta = dict(data_window_meta or {})
                data_window_meta["regime_provenance_gate"] = provenance_checks
                result.data_window_meta = dict(data_window_meta)
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

            for tf, data in data_by_tf.items():
                validation_start = wf_start if wf_start is not None else stage2_train_start
                validation_end = wf_end if wf_end is not None else stage2_train_end
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
                    if not evidence_verdict.passed:
                        markers = self._evidence_no_trade_markers(evidence_verdict)
                        regime_configs[tf] = markers
                        unvalidated_count += len(markers)
                        logger.warning(
                            f"[{symbol}] [{tf}] Evidence gate failed before feature/search: "
                            f"{evidence_verdict.concise_reason()}"
                        )
                        continue

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
                eligibility_features = self._slice_time_window(
                    full_features, eligibility_start, eligibility_end
                )
                active_features = self._slice_time_window(full_features, opt_train_start, opt_train_end)
                stage2_features = self._slice_time_window(full_features, validation_start, validation_end)
                historical_features = self._slice_time_window(full_features, hist_start, hist_end)
                holdout_features = (
                    self._slice_time_window(full_features, holdout_warm_start, holdout_end)
                    if holdout_start is not None else pd.DataFrame()
                )

                if len(active_features) < 200:
                    logger.debug(f"[{symbol}] [{tf}] Skipping - insufficient active data")
                    del full_features, eligibility_features, active_features, stage2_features, historical_features, holdout_features
                    FeatureComputer.clear_cache()
                    gc.collect()
                    continue

                train_features_by_tf = {tf: active_features}
                eligibility_features_by_tf = {tf: eligibility_features} if len(eligibility_features) >= 200 else {}
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
                if len(stage2_features) >= 50:
                    val_features_by_tf[tf] = stage2_features
                    val_warmup_by_tf[tf] = int((stage2_features.index < validation_start).sum()) if validation_start is not None else 0
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
                        val_warmup_by_tf=val_warmup_by_tf,
                        eligibility_features_by_tf=eligibility_features_by_tf,
                        selection_features_by_tf=selection_features_by_tf,
                        selection_warmup_by_tf=selection_warmup_by_tf,
                        selection_surface_by_tf=selection_surface_by_tf,
                        historical_features_by_tf={tf: threadspark_historical_features} if len(threadspark_historical_features) >= 50 else {},
                    )

                regime_configs.update(tf_regime_configs)
                validated_count += int(tf_validated or 0)
                unvalidated_count += int(tf_unvalidated or 0)
                oos_evidence_records.extend(self.regime_optimizer.consume_oos_candidate_evidence())

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
                        governance_warmup_by_tf = {tf: 0}
                        governance_surface = "stage2_active_universe"
                    else:
                        governance_features_by_tf = train_features_by_tf
                        governance_warmup_by_tf = {tf: 0}
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
                del train_features_by_tf, val_features_by_tf, val_warmup_by_tf
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
            
            if total_winners == 0 and default_config is None:
                if unvalidated_count > 0:
                    result.error_message = f"No validated regime winners ({unvalidated_count} candidates failed validation)"
                else:
                    result.error_message = "No valid regime winners found"
                return result
            
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
        """
        Run optimization for all symbols.
        
        Args:
            symbols: List of symbols to optimize
            
        Returns:
            Dict mapping symbol to PipelineResult
        """
        results = {}
        if (
            bool(getattr(self.config, "false_discovery_control_enabled", False))
            and int(getattr(self.config, "false_discovery_symbol_count", 0) or 0) <= 0
        ):
            self.config.false_discovery_symbol_count = len(symbols or [])
        
        total_start = time.time()
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"PORTFOLIO MANAGER - OPTIMIZATION PIPELINE")
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Strategies: {StrategyRegistry.count()}")
        logger.info(f"Timeframes: {self.config.timeframes}")
        logger.info(f"{'#'*60}")
        
        for i, symbol in enumerate(symbols):
            logger.info(f"\nProgress: {i+1}/{len(symbols)}")
            result = self.run_for_symbol(symbol)
            results[symbol] = result
            
            # Clear data cache after each symbol to prevent memory leaks during long runs
            self.data_loader.clear_cache()
        
        # Summary
        total_time = time.time() - total_start
        successful = sum(1 for r in results.values() if r.success)
        validated = sum(1 for r in results.values() if r.config and r.config.is_validated)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"OPTIMIZATION COMPLETE")
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Successful: {successful}/{len(symbols)}")
        logger.info(f"Validated:  {validated}/{len(symbols)}")
        logger.info(f"{'#'*60}")
        
        return results
    
    def save_configs(self, results: Dict[str, PipelineResult], filepath: str):
        """Save validated configurations to JSON file."""
        configs = {}
        for symbol, result in results.items():
            if result.success and result.config:
                configs[symbol] = result.config.to_dict()
        
        with open(filepath, 'w') as f:
            json.dump(configs, f, indent=2)
        
        logger.info(f"Saved {len(configs)} configurations to {filepath}")
    
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
    
    def __init__(self, 
                 config: PipelineConfig,
                 symbols: List[str],
                 config_file: str = "pm_configs.json"):
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
        
        # Load existing configs via ledger
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

    def _build_ledger_status(self) -> Dict[str, Any]:
        """Build operator-facing completion metadata for the active winner ledger."""
        configured_symbols = [str(symbol) for symbol in (self.symbols or [])]
        optimized_symbols = sorted(str(symbol) for symbol in self.symbol_configs.keys())
        artifact_contract_counts: Dict[str, int] = {}
        artifact_contract_symbols: Dict[str, set] = {}
        current_artifact_meta = build_artifact_meta(self.config)
        current_artifact_key = "|".join([
            str(current_artifact_meta["artifact_version"]),
            str(current_artifact_meta["split_contract_version"]),
        ])
        for symbol, cfg in self.symbol_configs.items():
            artifact = getattr(cfg, "artifact_meta", {}) or {}
            key = "|".join([
                str(artifact.get("artifact_version", "missing")),
                str(artifact.get("split_contract_version", "missing")),
            ])
            artifact_contract_counts[key] = artifact_contract_counts.get(key, 0) + 1
            artifact_contract_symbols.setdefault(key, set()).add(str(symbol).upper())
        configured_set = {symbol.upper() for symbol in configured_symbols}
        optimized_set = {symbol.upper() for symbol in optimized_symbols}
        current_contract_set = artifact_contract_symbols.get(current_artifact_key, set())
        missing_symbol_entries = sorted(configured_set - optimized_set)
        missing_current_contract = sorted(configured_set - current_contract_set)
        complete_symbol_universe = bool(configured_set) and configured_set.issubset(optimized_set)
        complete_current_contract = bool(configured_set) and configured_set.issubset(current_contract_set)
        return {
            "configured_symbol_count": len(configured_symbols),
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
        """Persist an explicit NO_TRADE config for symbols with insufficient historical data."""
        return self._save_no_trade_config(symbol, reason, failure_kind="data_failure")

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
    
    def needs_retraining(self, symbol: str) -> bool:
        """
        Check if symbol needs retraining.
        
        Uses ledger validity check.
        """
        artifact_meta = build_artifact_meta(self.config)
        should_optimize, _ = self.ledger.should_optimize(symbol, current_artifact_meta=artifact_meta)
        return should_optimize
    
    def get_symbols_needing_retrain(self) -> List[str]:
        """Get list of symbols that need retraining."""
        return [s for s in self.symbols if self.needs_retraining(s)]
    
    def retrain_symbol(self, symbol: str, force: bool = False) -> bool:
        """
        Retrain a single symbol.
        
        Args:
            symbol: Symbol to retrain
            force: If True, retrain even if config is valid
            
        Returns:
            True if successful
        """
        # Scheduled/manual retraining is an optimization entry point too.  It
        # must honor the current immutable availability roster before checking
        # ledger age or invoking the optimizer; otherwise an expired exclusion
        # could enter the legacy data path, and a newly eligible symbol could
        # remain stuck behind its still-valid old NO_TRADE artifact.
        universe = self._native_universe_for_optimization()
        if universe is not None and universe.explicit_contract:
            canonical = str(symbol or "").strip().upper()
            existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(canonical)
            if canonical in set(universe.excluded_symbols):
                binding = universe.exclusion_binding(canonical)
                if not self._has_exact_native_exclusion_grid(existing, binding):
                    self.save_native_exclusion_no_trade_config(symbol, binding)
                logger.info(
                    "SKIP %s: immutable native availability exclusion %s",
                    symbol,
                    universe.activation_id,
                )
                return True
            if canonical not in set(universe.eligible_symbols):
                raise RuntimeError(
                    f"NATIVE_UNIVERSE_SYMBOL_UNBOUND:{canonical}"
                )
            if existing is not None and bool(
                (existing.artifact_meta or {}).get("native_exclusion_binding")
            ):
                force = True

        # Check if we should skip
        if not force:
            artifact_meta = build_artifact_meta(self.config)
            should_optimize, reason = self.ledger.should_optimize(symbol, current_artifact_meta=artifact_meta)
            if not should_optimize:
                logger.info(f"SKIP {symbol}: {reason}")
                return True  # Already valid
        
        logger.info(f"RETRAIN {symbol}: running optimization...")
        
        result = self.pipeline.run_for_symbol(symbol)
        
        if result.success and result.config:
            self._save_symbol_config(symbol, result.config)
            logger.info(f"SAVED {symbol}: optimization complete")
            return True
        else:
            # Keep existing config on failure
            logger.warning(f"FAILED {symbol}: keeping existing config")
            return False
    
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
        
        logger.info(f"Retraining {len(symbols_to_retrain)} symbols...")
        
        for i, symbol in enumerate(symbols_to_retrain):
            logger.info(f"Progress: {i+1}/{len(symbols_to_retrain)}")
            results[symbol] = self.retrain_symbol(symbol)
        
        return results

    def _precompute_feature_cache_for_symbols(self, symbols: List[str]) -> Dict[str, Any]:
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

        if workers > 1 and len(symbols) > 1:
            cfg_dict = _pipeline_config_to_dict(self.config)
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(_precompute_feature_cache_worker, cfg_dict, symbol): symbol
                    for symbol in symbols
                }
                future_iter = tqdm(as_completed(future_map), total=len(future_map), desc="Feature cache", unit="sym") if _TQDM else as_completed(future_map)
                for future in future_iter:
                    symbol = future_map[future]
                    try:
                        _sym, stats = future.result()
                    except Exception as exc:
                        stats = {"symbols_failed": 1, "errors": {symbol: str(exc)}}
                    summary["symbols_completed"] += int(stats.get("symbols_completed", 0) or 0)
                    summary["symbols_failed"] += int(stats.get("symbols_failed", 0) or 0)
                    summary["timeframes_computed_or_retrieved"] += int(
                        stats.get("timeframes_computed_or_retrieved", 0) or 0
                    )
                    summary["errors"].update(dict(stats.get("errors", {}) or {}))
                    _merge_feature_cache_stats(stats)
        else:
            stats = self.pipeline.precompute_feature_cache(symbols)
            summary["symbols_completed"] += int(stats.get("symbols_completed", 0) or 0)
            summary["symbols_failed"] += int(stats.get("symbols_failed", 0) or 0)
            summary["timeframes_computed_or_retrieved"] += int(
                stats.get("timeframes_computed_or_retrieved", 0) or 0
            )
            summary["errors"].update(dict(stats.get("errors", {}) or {}))
            _merge_feature_cache_stats(stats)

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

    def _run_production_replay_audit(self) -> Dict[str, Any]:
        """Run the post-optimization full-sample replay gate against the winner ledger."""
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

        ledger_path = Path(self.config_file)
        if not ledger_path.is_absolute():
            ledger_path = Path.cwd() / ledger_path
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
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
            backup = ledger_path.with_name(f"{ledger_path.stem}.backup_replay_audit_{stamp}{ledger_path.suffix}")
            shutil.copy2(ledger_path, backup)
            counters = replay_audit.apply_audit(
                ledger,
                results,
                block_unverified=block_unverified,
            )
            tmp_path = ledger_path.with_suffix(f"{ledger_path.suffix}.tmp_replay_audit")
            tmp_path.write_text(json.dumps(ledger, indent=2, default=str), encoding="utf-8")
            tmp_path.replace(ledger_path)
            self.ledger.load()
            self.symbol_configs = self.ledger.get_all_configs()
            summary["backup_path"] = str(backup)
            summary["counters"] = counters

        logger.info(
            "Production replay audit complete: "
            f"PASS={summary['pass']} FAIL={summary['fail']} UNVERIFIED={summary['unverified']} "
            f"mode={mode} report={report_path}"
        )
        return summary
    
    def initial_optimization(self, overwrite: bool = False) -> Dict[str, PipelineResult]:
        """
        Run initial optimization for all symbols with skip/resume support.
        
        This is the main entry point for optimization that:
        - Skips symbols with valid configs (unless overwrite=True)
        - Saves progress after each symbol (incremental persistence)
        - Never loses existing configs on failure
        
        Args:
            overwrite: If True, re-optimize all symbols ignoring validity
            
        Returns:
            Dict of pipeline results (only for symbols that were optimized)
        """
        results = {}
        
        total_start = time.time()
        
        # Freeze the availability-only universe before model search. Excluded
        # symbols receive a complete, manifest-bound NO_TRADE grid and never
        # enter scoring/optimization. A prior exclusion that becomes eligible
        # under a later activation is forced back through optimization.
        universe = self._native_universe_for_optimization()
        optimization_symbols = list(self.symbols)
        stale_exclusions_now_eligible: List[str] = []
        excluded_now: List[str] = []
        if universe is not None and universe.explicit_contract:
            eligible_set = set(universe.eligible_symbols)
            excluded_set = set(universe.excluded_symbols)
            optimization_symbols = [
                symbol for symbol in self.symbols if str(symbol).upper() in eligible_set
            ]
            for symbol in self.symbols:
                canonical = str(symbol).upper()
                existing = self.symbol_configs.get(symbol) or self.symbol_configs.get(canonical)
                if canonical in excluded_set:
                    excluded_now.append(symbol)
                    binding = universe.exclusion_binding(canonical)
                    if not self._has_exact_native_exclusion_grid(existing, binding):
                        self.save_native_exclusion_no_trade_config(symbol, binding)
                elif existing is not None and bool(
                    (existing.artifact_meta or {}).get("native_exclusion_binding")
                ):
                    stale_exclusions_now_eligible.append(symbol)

        # Determine which eligible symbols to optimize.
        artifact_meta = build_artifact_meta(self.config)
        to_optimize, to_skip = self.ledger.get_symbols_to_optimize(
            optimization_symbols,
            overwrite=overwrite,
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
        logger.info(f"Skipping: {len(to_skip)} (valid configs)")
        if overwrite:
            logger.info(f"OVERWRITE MODE: ignoring validity checks")
        logger.info(f"Strategies: {StrategyRegistry.count()}")
        logger.info(f"Timeframes: {self.config.timeframes}")
        logger.info(f"{'#'*60}")
        
        if not to_optimize:
            logger.info("All symbols have valid configs. Nothing to optimize.")
            self._run_production_replay_audit()
            return results
        
        max_workers = int(getattr(self.config, "optimization_max_workers", 1) or 1)
        if (
            bool(getattr(self.config, "feature_disk_cache_enabled", False))
            and bool(getattr(self.config, "feature_disk_cache_precompute_enabled", False))
        ):
            precompute_stats = self._precompute_feature_cache_for_symbols(to_optimize)
            logger.info(f"Feature cache precompute summary: {precompute_stats}")

        if max_workers > 1 and len(to_optimize) > 1:
            cfg_dict = _pipeline_config_to_dict(self.config)
            logger.info(f"Parallel optimization enabled: workers={max_workers}")

            completed = 0
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(_optimize_symbol_worker, cfg_dict, symbol): symbol
                    for symbol in to_optimize
                }

                future_iter = tqdm(as_completed(future_map), total=len(future_map), desc="Optimizing", unit="sym") if _TQDM else as_completed(future_map)
                for future in future_iter:
                    symbol = future_map[future]
                    completed += 1
                    if not _TQDM:
                        logger.info(f"\nProgress: {completed}/{len(to_optimize)}")

                    try:
                        sym, result = future.result()
                        results[sym] = result
                        symbol_for_save = sym
                    except Exception as e:
                        logger.exception(f"[{symbol}] Optimization error: {e}")
                        results[symbol] = PipelineResult(symbol=symbol, success=False, error_message=str(e))
                        symbol_for_save = symbol

                    # Incremental persistence: save immediately after each symbol
                    result = results.get(symbol_for_save)
                    if result and result.success and result.config:
                        self._save_symbol_config(symbol_for_save, result.config)
                        logger.info(f"SAVED {symbol_for_save} to {self.config_file} (atomic)")
                    else:
                        detail = str(getattr(result, "error_message", "") or "no validated strategy config produced")
                        reason = (
                            f"Optimization failed or produced no live-safe winner: {detail}. "
                            "Live trading disabled for this symbol."
                        )
                        self.save_optimization_no_trade_config(
                            symbol_for_save,
                            reason,
                            data_window_meta=getattr(result, "data_window_meta", None),
                        )
                        logger.warning(f"FAILED {symbol_for_save}: saved NO_TRADE optimization-failure marker")

            # Clear any in-process cache after parallel run
            self.pipeline.data_loader.clear_cache()
            FeatureComputer.clear_cache()
        else:
            # Process symbols needing optimization sequentially
            symbol_iter = tqdm(to_optimize, desc="Optimizing", unit="sym") if _TQDM else to_optimize
            for i, symbol in enumerate(symbol_iter):
                if not _TQDM:
                    logger.info(f"\nProgress: {i+1}/{len(to_optimize)}")
                
                try:
                    result = self.pipeline.run_for_symbol(symbol)
                    results[symbol] = result
                    
                    # Incremental persistence: save immediately after each symbol
                    if result.success and result.config:
                        self._save_symbol_config(symbol, result.config)
                        logger.info(f"SAVED {symbol} to {self.config_file} (atomic)")
                    else:
                        detail = str(getattr(result, "error_message", "") or "no validated strategy config produced")
                        reason = (
                            f"Optimization failed or produced no live-safe winner: {detail}. "
                            "Live trading disabled for this symbol."
                        )
                        self.save_optimization_no_trade_config(
                            symbol,
                            reason,
                            data_window_meta=getattr(result, "data_window_meta", None),
                        )
                        logger.warning(f"FAILED {symbol}: saved NO_TRADE optimization-failure marker")
                    
                except Exception as e:
                    logger.exception(f"[{symbol}] Optimization error: {e}")
                    results[symbol] = PipelineResult(symbol=symbol, error_message=str(e))
                    reason = (
                        f"Optimization raised an exception: {e}. "
                        "Live trading disabled for this symbol."
                    )
                    self.save_optimization_no_trade_config(symbol, reason)
                
                # Clear data cache after each symbol to prevent memory leaks
                self.pipeline.data_loader.clear_cache()
                FeatureComputer.clear_cache()
        
        # Summary
        total_time = time.time() - total_start
        successful = sum(1 for r in results.values() if r.success)
        validated = sum(1 for r in results.values() if r.config and r.config.is_validated)
        
        # Include skipped symbols in final counts
        total_validated = len(self.get_validated_configs())
        total_live_eligible = len(self.get_live_eligible_configs())
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"OPTIMIZATION COMPLETE")
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Optimized: {successful}/{len(to_optimize)}")
        logger.info(f"Skipped: {len(to_skip)} (already valid)")
        logger.info(f"Total validated: {total_validated}/{len(self.symbols)}")
        logger.info(f"Live eligible: {total_live_eligible}/{len(self.symbols)}")
        logger.info(f"{'#'*60}")

        self._run_production_replay_audit()
        
        return results
    
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
        if save:
            self._save_symbol_config(symbol, symbol_cfg)
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

