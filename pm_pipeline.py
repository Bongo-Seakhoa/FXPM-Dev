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

import json
import hashlib
import logging
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import product
from dataclasses import dataclass, field, fields

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
from pm_enhancement_seams import EnhancementSeams, create_default_enhancement_seams
from pm_strategies import (
    StrategyRegistry,
    BaseStrategy,
    _STRATEGY_MIGRATION,
    get_regime_tp_multipliers,
    set_regime_tp_multipliers,
)
from pm_position import PositionConfig, PositionManager
from pm_regime import load_regime_params
from pm_order_governance import candidate_policy_names, make_policy, policy_name_from_artifact

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
PIPELINE_ARTIFACT_VERSION = "2026-04-25-active-recent-risk-mgmt"


def _stable_seed(*parts: Any) -> int:
    """Deterministic non-negative seed derived from stable context parts."""
    payload = "|".join(str(part) for part in parts).encode("utf-8", errors="replace")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFF


def _stable_hash(payload: Any) -> str:
    """Create a stable SHA1 hash for JSON-serializable payloads."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _regime_params_fingerprint(filepath: str) -> Dict[str, Any]:
    try:
        path = Path(filepath)
        stat = path.stat()
        return {
            "path": str(path.resolve()),
            "mtime_ns": stat.st_mtime_ns,
            "size": stat.st_size,
        }
    except Exception:
        return {"path": str(filepath), "mtime_ns": None, "size": None}


def _scheduled_valid_until(config: PipelineConfig, from_dt: Optional[datetime] = None) -> datetime:
    """Calendar-anchored config validity boundary for production artifacts."""
    return config.get_next_retrain_at(from_dt or datetime.now())


def _strategy_surface_fingerprint(strategies: Optional[List[BaseStrategy]] = None) -> str:
    strategy_list = strategies if strategies is not None else StrategyRegistry.get_all_instances()
    surface = []
    for strategy in strategy_list:
        try:
            surface.append({
                "name": strategy.name,
                "params": strategy.get_params(),
                "grid": strategy.get_param_grid(),
            })
        except Exception:
            surface.append({"name": getattr(strategy, "name", strategy.__class__.__name__)})
    surface.sort(key=lambda item: item["name"])
    return _stable_hash(surface)


def build_artifact_meta(config: PipelineConfig,
                        strategies: Optional[List[BaseStrategy]] = None) -> Dict[str, Any]:
    """Build a semantic fingerprint for persisted optimization artifacts."""
    return {
        "artifact_version": PIPELINE_ARTIFACT_VERSION,
        "feature_cache_version": FeatureComputer.FEATURE_CACHE_VERSION,
        "split_contract_version": DataSplitter.SPLIT_CONTRACT_VERSION,
        "data_workflow_mode": str(getattr(config, "data_workflow_mode", "active_recent_m5")),
        "max_bars": int(getattr(config, "max_bars", 300000)),
        "historical_stress_audit_bars": int(getattr(config, "historical_stress_audit_bars", 50000)),
        "active_universe_bars": int(getattr(config, "active_universe_bars", 250000)),
        "active_stage2_pct": float(getattr(config, "active_stage2_pct", 50.0)),
        "train_pct": float(config.train_pct),
        "val_pct": float(config.val_pct),
        "overlap_pct": float(config.overlap_pct),
        "holdout_pct": None if config.holdout_pct is None else float(config.holdout_pct),
        "scoring_mode": str(config.scoring_mode),
        "fixed_retrain_days": int(getattr(config, "fixed_retrain_days", getattr(config, "optimization_valid_days", 14))),
        "production_retrain_mode": str(getattr(config, "production_retrain_mode", "auto")),
        "production_retrain_interval_weeks": int(getattr(config, "production_retrain_interval_weeks", 2)),
        "production_retrain_weekday": str(getattr(config, "production_retrain_weekday", "sunday")),
        "production_retrain_time": str(getattr(config, "production_retrain_time", "00:01")),
        "production_retrain_anchor_date": str(getattr(config, "production_retrain_anchor_date", "2026-03-29")),
        "walk_forward_audit_enabled": bool(getattr(config, "walk_forward_audit_enabled", False)),
        "walk_forward_audit_windows": int(getattr(config, "walk_forward_audit_windows", 3)),
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
            getattr(config, "risk_management_optimization_enabled", True)
        ),
        "risk_management_selection_stage": str(
            getattr(config, "risk_management_selection_stage", "stage3")
        ),
        "exit_surface_contract": "trade_intent_regime_tp_v1",
        "regime_tp_multipliers": dict(
            getattr(config, "regime_tp_multipliers", None) or get_regime_tp_multipliers()
        ),
        "regime_params": _regime_params_fingerprint(getattr(config, "regime_params_file", "regime_params.json")),
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


def artifact_contract_matches(stored_artifact: Dict[str, Any],
                              current_artifact_meta: Dict[str, Any]) -> bool:
    """Return True when the persisted artifact matches the current contract."""
    if not current_artifact_meta:
        return True
    return _artifact_contract_view(stored_artifact, current_artifact_meta) == current_artifact_meta


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
    
    def __init__(self, filepath: str = "pm_configs.json"):
        """
        Initialize config ledger.
        
        Args:
            filepath: Path to config JSON file
        """
        self.filepath = Path(filepath)
        self.configs: Dict[str, SymbolConfig] = {}
        self._loaded = False
    
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
            return 0
        
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.configs = {}
            for symbol, config_dict in data.items():
                try:
                    self.configs[symbol] = SymbolConfig.from_dict(config_dict)
                except Exception as e:
                    logger.warning(f"Could not parse config for {symbol}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self.configs)} existing configs from {self.filepath}")
            return len(self.configs)
            
        except json.JSONDecodeError as e:
            # Corrupted file - fail fast with clear message
            raise RuntimeError(
                f"Corrupted JSON in {self.filepath}: {e}\n"
                f"Please fix or remove the file manually, or backup to .corrupt and start fresh."
            )
        except Exception as e:
            raise RuntimeError(f"Failed to load {self.filepath}: {e}")
    
    def _atomic_save(self):
        """
        Save configs atomically using temp file + rename pattern.
        
        This ensures configs are never corrupted if the process is interrupted.
        """
        temp_path = self.filepath.with_suffix('.json.tmp')
        
        # Serialize all configs
        data = {s: c.to_dict() for s, c in self.configs.items()}
        
        try:
            # Write to temp file
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
                f.flush()
                # fsync for durability (optional but recommended)
                import os
                os.fsync(f.fileno())
            
            # Atomic rename (on POSIX systems)
            temp_path.replace(self.filepath)
            
        except Exception as e:
            # Clean up temp file on failure
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except:
                    pass
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
        if not self._loaded:
            self.load()
        return self.configs.get(symbol)
    
    def has_valid_config(self, symbol: str) -> Tuple[bool, str]:
        """
        Check if a symbol has a valid (non-expired) config.
        
        Returns:
            Tuple of (is_valid, reason_string)
        """
        if not self._loaded:
            self.load()
        
        if symbol not in self.configs:
            return False, "missing"
        
        config = self.configs[symbol]
        
        # Check validation status
        if not config.is_validated:
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
            # Extra check: regime detection params changed since optimization
            if current_regime_version and symbol in self.configs:
                stored = self.configs[symbol].regime_detection_version
                if stored and stored != current_regime_version:
                    return True, "regime detection params changed"
            return False, reason  # Skip - config is valid

        return True, reason  # Optimize - config is missing/expired/invalid
    
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
        if not self._loaded:
            self.load()
        
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
        if not self._loaded:
            self.load()
        return self.configs.copy()
    
    def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol's config (use with caution).
        
        Args:
            symbol: Symbol to remove
            
        Returns:
            True if removed and saved
        """
        if not self._loaded:
            self.load()
        
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
        if not self._loaded:
            self.load()
        
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
        return sum(
            1
            for regimes in self.regime_configs.values()
            for cfg in regimes.values()
            if cfg is not None and not cfg.is_no_trade_marker()
        )
    
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
            'is_validated': self.is_validated,
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
            'artifact_meta': self.artifact_meta,
            'live_observability': dict(self.live_observability or _default_live_observability()),
            'walk_forward_audit': dict(self.walk_forward_audit or _default_walk_forward_audit()),
        }

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

                        if self.config.scoring_mode == "fx_backtester":
                            if metrics.get('total_trades', 0) < self.config.min_trades:
                                continue
                            train_score = float(self.scorer.score(metrics, purpose="selection"))
                            candidates.append({
                                "strategy_name": strategy.name,
                                "timeframe": timeframe,
                                "train_metrics": metrics,
                                "train_score": train_score
                            })
                        else:
                            # Original behavior: strict minimum criteria + composite
                            passes, _ = self.scorer.passes_minimum_criteria(metrics)
                            if not passes:
                                continue
                            train_score = float(self.scorer.calculate_composite_score(metrics))
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
            # Pass 2: validation-aware selection (fx_backtester)
            # ------------------------------------------------------------
            if self.config.scoring_mode == "fx_backtester" and val_data_by_tf is not None:
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
            # Non-fx_backtester: choose best train score (original)
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
                 val_features: pd.DataFrame) -> Tuple[Dict[str, Any], Dict, Dict]:
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
            train_metrics = self.backtester.run(train_features, signals, symbol, strategy)
            
            if val_features is not None and len(val_features) >= 50:
                signals = strategy.generate_signals(val_features, symbol)
                val_metrics = self.backtester.run(val_features, signals, symbol, strategy)
            else:
                val_metrics = {'total_return_pct': 0.0, 'sharpe_ratio': 0.0,
                               'max_drawdown_pct': 0.0, 'total_trades': 0}
            
            return strategy.get_params(), train_metrics, val_metrics
        
        # Use Optuna TPE if available
        if self.optuna_optimizer is not None:
            return self._optimize_with_optuna(
                symbol, strategy_name, param_grid, train_features, val_features
            )
        
        # Fallback to grid search
        return self._optimize_grid_search(
            symbol, strategy_name, param_grid, train_features, val_features
        )
    
    def _optimize_with_optuna(self,
                              symbol: str,
                              strategy_name: str,
                              param_grid: Dict[str, List],
                              train_features: pd.DataFrame,
                              val_features: pd.DataFrame) -> Tuple[Dict[str, Any], Dict, Dict]:
        """Optimize using Optuna TPE."""
        
        # Define scoring function based on config mode
        def scoring_fn(train_metrics: Dict, val_metrics: Dict) -> float:
            if self.config.scoring_mode == "fx_backtester":
                # Use fx_generalization_score for validation-aware scoring
                if val_metrics.get('total_trades', 0) >= self.config.fx_val_min_trades:
                    # Check validation guardrails
                    val_dd = float(val_metrics.get('max_drawdown_pct', 100.0))
                    if val_dd >= self.config.fx_val_max_drawdown:
                        return -500.0  # Penalty for excessive drawdown
                    
                    final_score, _, _, _ = self.scorer.fx_generalization_score(
                        train_metrics, val_metrics, purpose="opt"
                    )
                    return final_score
                else:
                    # Not enough validation trades - use train with discount
                    train_score = self.scorer.score(train_metrics, purpose="opt")
                    return train_score * 0.7
            else:
                # Legacy pm_weighted mode
                train_score = self.scorer.calculate_composite_score(train_metrics)
                val_score = self.scorer.calculate_composite_score(val_metrics)
                return 0.4 * train_score + 0.6 * val_score
        
        # Get n_trials from config
        n_trials = getattr(self.config, 'max_param_combos', 100)
        
        # Update optimizer config
        if hasattr(self.optuna_optimizer, 'config'):
            self.optuna_optimizer.config.n_trials = n_trials
        
        # Run optimization
        result = self.optuna_optimizer.optimize(
            symbol=symbol,
            strategy_name=strategy_name,
            param_grid=param_grid,
            train_features=train_features,
            val_features=val_features,
            scoring_fn=scoring_fn,
            min_trades=self.config.fx_opt_min_trades if self.config.scoring_mode == "fx_backtester" 
                       else self.config.min_trades
        )
        
        logger.info(f"[{symbol}] Optuna optimization for {strategy_name}: "
                   f"{result.stats.n_trials} trials, best_score={result.best_score:.2f}")
        
        return result.best_params, result.train_metrics, result.val_metrics
    
    def _optimize_grid_search(self,
                              symbol: str,
                              strategy_name: str,
                              param_grid: Dict[str, List],
                              train_features: pd.DataFrame,
                              val_features: pd.DataFrame) -> Tuple[Dict[str, Any], Dict, Dict]:
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
        
        # fx_backtester mode with validation
        if self.config.scoring_mode == "fx_backtester":
            candidates: List[Dict[str, Any]] = []
            
            for combo in combinations:
                params = dict(zip(param_names, combo))
                test_params = {**default_params, **params}
                test_strategy = StrategyRegistry.get(strategy_name, **test_params)
                
                try:
                    signals = test_strategy.generate_signals(train_features, symbol)
                    train_metrics = self.backtester.run(train_features, signals, symbol, test_strategy)
                    
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
                        train_metrics = self.backtester.run(train_features, signals, symbol, test_strategy)
                    except Exception as e:
                        logger.debug(f"[{symbol}] Val-shortlist train {strategy_name} failed: {e}")
                        continue

                if val_features is None or len(val_features) < 50:
                    val_metrics = {'total_return_pct': 0.0, 'sharpe_ratio': 0.0,
                                   'max_drawdown_pct': 0.0, 'total_trades': 0}
                else:
                    try:
                        signals = test_strategy.generate_signals(val_features, symbol)
                        val_metrics = self.backtester.run(val_features, signals, symbol, test_strategy)
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
        
        # Legacy pm_weighted mode
        best_score = -float('inf')
        best_params = default_params
        best_train_metrics = {}
        best_val_metrics = {}
        
        for combo in combinations:
            params = dict(zip(param_names, combo))
            test_strategy = StrategyRegistry.get(strategy_name, **params)
            
            try:
                signals = test_strategy.generate_signals(train_features, symbol)
                train_metrics = self.backtester.run(train_features, signals, symbol, test_strategy)
                
                passes, _ = self.scorer.passes_minimum_criteria(train_metrics)
                if not passes:
                    continue
                
                signals = test_strategy.generate_signals(val_features, symbol)
                val_metrics = self.backtester.run(val_features, signals, symbol, test_strategy)
                
                train_score = self.scorer.calculate_composite_score(train_metrics)
                val_score = self.scorer.calculate_composite_score(val_metrics)
                combined_score = 0.4 * train_score + 0.6 * val_score
                
                if combined_score > best_score:
                    best_score = combined_score
                    best_params = test_strategy.get_params()
                    best_train_metrics = train_metrics
                    best_val_metrics = val_metrics
                    
            except Exception as e:
                logger.debug(f"[{symbol}] Param combo failed: {params} - {e}")
                continue
        
        if not best_train_metrics:
            logger.warning(f"[{symbol}] No valid params found, using defaults")
            signals = strategy.generate_signals(train_features, symbol)
            best_train_metrics = self.backtester.run(train_features, signals, symbol, strategy)
            signals = strategy.generate_signals(val_features, symbol)
            best_val_metrics = self.backtester.run(val_features, signals, symbol, strategy)
        
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
        Validate strategy configuration.
        
        Args:
            train_metrics: Training period metrics
            val_metrics: Validation period metrics
            
        Returns:
            Tuple of (is_valid, reason, robustness_ratio)
        """
        # Backtester-aligned validation rules
        if self.config.scoring_mode == "fx_backtester":
            robustness = self.scorer.calculate_fx_score_robustness_ratio(train_metrics, val_metrics, purpose="selection")

            val_sharpe = float(val_metrics.get('sharpe_ratio', 0.0))
            val_dd = float(val_metrics.get('max_drawdown_pct', 100.0))
            val_trades = int(val_metrics.get('total_trades', 0))

            is_valid = (
                (robustness >= getattr(self.config, "fx_min_robustness_ratio", 0.85) or val_sharpe > self.config.fx_val_sharpe_override)
                and val_dd < self.config.fx_val_max_drawdown
                and val_trades >= self.config.fx_val_min_trades
            )

            if not is_valid:
                if val_trades < self.config.fx_val_min_trades:
                    return False, f"Validation failed: Insufficient trades: {val_trades} < {self.config.fx_val_min_trades}", robustness
                if val_dd >= self.config.fx_val_max_drawdown:
                    return False, f"Validation failed: High drawdown: {val_dd:.2f}% >= {self.config.fx_val_max_drawdown}%", robustness
                return False, f"Validation failed: Robustness {robustness:.2f} < {getattr(self.config, 'fx_min_robustness_ratio', 0.85)} and Sharpe {val_sharpe:.2f} <= {self.config.fx_val_sharpe_override}", robustness

            return True, "Validated successfully", robustness

        # Check validation metrics pass minimum criteria
        passes, reason = self.scorer.passes_minimum_criteria(val_metrics)
        if not passes:
            return False, f"Validation failed: {reason}", 0.0
        
        # Calculate robustness ratio
        robustness = self.scorer.calculate_robustness_ratio(train_metrics, val_metrics)
        
        # Check robustness
        if robustness < self.config.min_robustness:
            return False, f"Low robustness: {robustness:.2f} < {self.config.min_robustness}", robustness
        
        return True, "Validated successfully", robustness


# =============================================================================
# REGIME-AWARE OPTIMIZATION
# =============================================================================

class RegimeOptimizer:
    """
    Regime-aware strategy selection and optimization.
    
    Finds the best (strategy, params) for each (timeframe, regime) combination.
    All strategies compete for all regimes - winners are selected per (tf, regime).
    
    Flow:
    1. Quick screening with default params to identify top-K strategies per regime
    2. Hyperparameter tuning using Optuna TPE (simultaneous multi-regime optimization)
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

        # Return-to-DD efficiency gate (unconditional hard gate)
        _raw_ratio = float(getattr(config, 'regime_min_val_return_dd_ratio', 1.0))
        self.min_val_return_dd_ratio = _raw_ratio if math.isfinite(_raw_ratio) and _raw_ratio > 0 else 1.0
        
        # Use fx_backtester scoring mode
        self.use_fx_scoring = getattr(config, 'scoring_mode', 'pm_weighted') == 'fx_backtester'
        self.training_gate_telemetry: Dict[str, Any] = {}
        
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
                logger.debug("RegimeOptimizer using: Grid Search (Optuna not available)")

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
                        val_warmup_by_tf: Optional[Dict[str, int]] = None) -> Tuple[Dict[str, Dict[str, RegimeConfig]], Optional[RegimeConfig], int, int]:
        """
        Optimize for all (timeframe, regime) combinations with validation.
        
        Args:
            symbol: Symbol name
            train_features_by_tf: Dict mapping timeframe -> training features (with regime columns)
            val_features_by_tf: Dict mapping timeframe -> validation features (with regime columns)
            strategies: List of strategy instances
            
        Returns:
            Tuple of:
            - Dict[timeframe][regime] -> RegimeConfig (validated winners only)
            - Optional[RegimeConfig] for default fallback (best validated overall)
            - int: count of validated winners
            - int: count of unvalidated (rejected) winners
        """
        logger.info(f"[{symbol}] Regime Optimization: {len(strategies)} strategies x {len(train_features_by_tf)} timeframes x {len(self.REGIMES)} regimes")
        
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
            val_warmup_bars = 0 if val_warmup_by_tf is None else int(val_warmup_by_tf.get(tf, 0) or 0)
            
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
                symbol, tf, train_features, val_features, strategies, val_warmup_bars=val_warmup_bars
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

    def _apply_training_eligibility_gates(self,
                                          candidates: List[Dict[str, Any]],
                                          symbol: str,
                                          timeframe: str) -> List[Dict[str, Any]]:
        """
        Hard thresholds that strategies must pass on TRAINING data before screening.
        Prevents "obviously bad" strategies from entering the optimization pool.

        This is applied BEFORE hyperparameter tuning to save compute on losing strategies.
        Thresholds must be LENIENT since strategies run with default (untuned) params.

        Checks (applied to full training backtest, NOT regime buckets):
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
            # Use FULL training metrics as primary gate
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

    def _collect_candidates(self,
                            symbol: str,
                            timeframe: str,
                            train_features: pd.DataFrame,
                            val_features: Optional[pd.DataFrame],
                            strategies: List[BaseStrategy],
                            val_warmup_bars: int = 0) -> List[Dict[str, Any]]:
        """
        Collect all strategy candidates with per-regime metrics.
        
        Phase 1 (Screening): Run all strategies with default params, bucket by regime.
        Phase 2 (Tuning): For top-K strategies per regime, run hyperparameter optimization.
        
        Returns list of candidate dicts with bucketed metrics (including tuned variants).
        """
        # Phase 1: Quick screening with default params
        screening_candidates = []
        
        for strategy in strategies:
            try:
                # Run backtest on full training data with default params
                signals = strategy.generate_signals(train_features, symbol)
                train_result = self.backtester.run(
                    train_features, signals, symbol, strategy
                )
                
                # Skip if no trades
                if train_result.get('total_trades', 0) == 0:
                    continue
                
                # Bucket trades by regime
                train_trades = train_result.get('trades', [])
                train_regime_metrics = self._bucket_trades_by_regime(
                    train_trades, train_features, timeframe=timeframe
                )
                
                # Run validation if available
                val_regime_metrics = {}
                if val_features is not None and len(val_features) >= 50:
                    val_signals = strategy.generate_signals(val_features, symbol)
                    val_result = self.backtester.run(
                        val_features, val_signals, symbol, strategy, warmup_bars=val_warmup_bars
                    )
                    val_trades = val_result.get('trades', [])
                    val_regime_metrics = self._bucket_trades_by_regime(
                        val_trades, val_features, timeframe=timeframe
                    )
                
                screening_candidates.append({
                    'strategy': strategy,
                    'strategy_name': strategy.name,
                    'params': strategy.get_params(),
                    'train_result': train_result,
                    'train_regime_metrics': train_regime_metrics,
                    'val_regime_metrics': val_regime_metrics,
                    'is_tuned': False,
                    'search_trials': 1,
                })
                
            except Exception as e:
                logger.debug(f"[{symbol}] [{timeframe}] {strategy.name} failed: {e}")
                continue

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
            return eligible_candidates

        # Phase 2: Hyperparameter tuning for top-K strategies per regime
        # Identify top-K strategies for each regime based on screening scores
        # Use ELIGIBLE candidates (post-gates) instead of all screening candidates
        regime_top_strategies = self._identify_top_strategies_per_regime(
            eligible_candidates, top_k=self.hyperparam_top_k
        )
        
        # Collect all unique strategies that need tuning
        strategies_to_tune = set()
        for regime, strategy_names in regime_top_strategies.items():
            strategies_to_tune.update(strategy_names)
        
        if not strategies_to_tune:
            logger.debug(f"[{symbol}] [{timeframe}] No strategies eligible for hyperparameter tuning")
            return eligible_candidates

        logger.info(f"[{symbol}] [{timeframe}] Hyperparameter tuning {len(strategies_to_tune)} strategies")

        # Run hyperparameter tuning for each strategy
        tuned_candidates = []
        for strategy_name in strategies_to_tune:
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
                train_features, val_features, val_warmup_bars
            )

            tuned_candidates.extend(tuned_results)

        # Combine eligible candidates with tuned candidates
        # The tuned candidates may replace or augment the defaults
        all_candidates = eligible_candidates + tuned_candidates

        return all_candidates
    
    def _identify_top_strategies_per_regime(self,
                                            candidates: List[Dict[str, Any]],
                                            top_k: int = 3) -> Dict[str, List[str]]:
        """
        Identify top-K strategies for each regime based on screening scores.
        
        Returns dict mapping regime -> list of strategy names to tune.
        """
        regime_scores: Dict[str, List[Tuple[str, float]]] = {r: [] for r in self.REGIMES}
        
        for cand in candidates:
            for regime in self.REGIMES:
                train_metrics = cand['train_regime_metrics'].get(regime, {})
                
                train_trades = train_metrics.get('total_trades', 0)
                
                # Only consider strategies with enough trades in this regime
                if train_trades < self.min_train_trades // 2:  # Relaxed threshold for screening
                    continue
                
                score = self._compute_regime_score(train_metrics, {})
                regime_scores[regime].append((cand['strategy_name'], score))
        
        # Select top-K for each regime
        result = {}
        for regime, scores in regime_scores.items():
            if scores:
                scores.sort(key=lambda x: x[1], reverse=True)
                result[regime] = [name for name, _ in scores[:top_k]]
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
                              val_warmup_bars: int = 0) -> List[Dict[str, Any]]:
        """
        Tune hyperparameters for a strategy using Optuna TPE.
        
        Uses multi-regime optimization to find best params for each regime
        in a single optimization run.
        
        Returns list of tuned candidate dicts with regime metrics.
        """
        # Use Optuna if available
        if self.optuna_optimizer is not None:
            regime_results = self.optuna_optimizer.optimize_for_regimes(
                symbol=symbol,
                timeframe=timeframe,
                strategy_name=strategy_name,
                param_grid=param_grid,
                train_features=train_features,
                val_features=val_features,
                regimes=self.REGIMES,
                bucket_trades_fn=self._bucket_trades_by_regime,
                compute_score_fn=self._compute_regime_score,
                min_train_trades=self.min_train_trades,
                max_drawdown_pct=self.val_max_drawdown,  # Pass DD threshold for early rejection
                val_warmup_bars=val_warmup_bars,
            )
            
            # Convert to candidate list format
            tuned_candidates = []
            for regime, candidate in regime_results.items():
                candidate['tuned_for_regime'] = regime
                tuned_candidates.append(candidate)
            
            if tuned_candidates:
                logger.debug(f"[{symbol}] [{timeframe}] {strategy_name}: "
                           f"{len(tuned_candidates)} tuned variants (Optuna TPE)")
            
            return tuned_candidates
        
        # Fallback to grid search
        return self._tune_strategy_params_grid(
            symbol, timeframe, strategy_name, param_grid, train_features, val_features, val_warmup_bars
        )
    
    def _tune_strategy_params_grid(self,
                                   symbol: str,
                                   timeframe: str,
                                   strategy_name: str,
                                   param_grid: Dict[str, List],
                                   train_features: pd.DataFrame,
                                   val_features: Optional[pd.DataFrame],
                                   val_warmup_bars: int = 0) -> List[Dict[str, Any]]:
        """
        Fallback grid search for hyperparameter tuning.
        
        Returns list of tuned candidate dicts with regime metrics.
        """
        from itertools import product
        
        # Generate param combinations
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        all_combos = list(product(*param_values))
        
        # Limit combinations with context-derived seed (X2)
        if len(all_combos) > self.hyperparam_max_combos:
            _seed = _stable_seed(
                symbol,
                timeframe,
                strategy_name,
                "regime_grid",
                len(train_features),
                len(val_features) if val_features is not None else 0,
            )
            np.random.seed(_seed)
            indices = np.random.choice(len(all_combos), self.hyperparam_max_combos, replace=False)
            all_combos = [all_combos[i] for i in indices]
        
        # Get default params as base
        base_strategy = StrategyRegistry.get(strategy_name)
        default_params = base_strategy.get_params()
        
        tuned_candidates = []
        best_scores_by_regime: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        
        for combo in all_combos:
            params = {**default_params, **dict(zip(param_names, combo))}
            
            try:
                # Create strategy with these params
                test_strategy = StrategyRegistry.get(strategy_name, **params)
                
                # Run backtest
                signals = test_strategy.generate_signals(train_features, symbol)
                train_result = self.backtester.run(
                    train_features, signals, symbol, test_strategy
                )
                
                if train_result.get('total_trades', 0) < self.config.min_trades // 2:
                    continue
                
                # Bucket by regime
                train_trades = train_result.get('trades', [])
                train_regime_metrics = self._bucket_trades_by_regime(
                    train_trades, train_features, timeframe=timeframe
                )
                
                # Validation
                val_regime_metrics = {}
                if val_features is not None and len(val_features) >= 50:
                    val_signals = test_strategy.generate_signals(val_features, symbol)
                    val_result = self.backtester.run(
                        val_features, val_signals, symbol, test_strategy, warmup_bars=val_warmup_bars
                    )
                    val_trades = val_result.get('trades', [])
                    val_regime_metrics = self._bucket_trades_by_regime(
                        val_trades, val_features, timeframe=timeframe
                    )
                
                # Score per regime and track best
                for regime in self.REGIMES:
                    train_m = train_regime_metrics.get(regime, {})
                    val_m = val_regime_metrics.get(regime, {})
                    
                    if train_m.get('total_trades', 0) < self.min_train_trades:
                        continue
                    
                    score = self._compute_regime_score(train_m, val_m)
                    
                    if regime not in best_scores_by_regime or score > best_scores_by_regime[regime][0]:
                        # Clean params for JSON serialization
                        clean_params = {}
                        for k, v in params.items():
                            if hasattr(v, "item"):
                                v = v.item()
                            elif isinstance(v, (np.integer, np.floating)):
                                v = float(v) if isinstance(v, np.floating) else int(v)
                            clean_params[k] = v
                        
                        best_scores_by_regime[regime] = (score, {
                            'strategy': test_strategy,
                            'strategy_name': strategy_name,
                            'params': clean_params,
                            'train_result': train_result,
                            'train_regime_metrics': train_regime_metrics,
                            'val_regime_metrics': val_regime_metrics,
                            'is_tuned': True,
                            'tuned_for_regime': regime,
                            'search_trials': int(len(all_combos)),
                        })
                
            except Exception as e:
                logger.debug(f"[{symbol}] [{timeframe}] {strategy_name} param combo failed: {e}")
                continue
        
        # Collect best tuned candidates (one per regime this strategy is good for)
        for regime, (score, candidate) in best_scores_by_regime.items():
            tuned_candidates.append(candidate)
        
        if tuned_candidates:
            logger.debug(f"[{symbol}] [{timeframe}] {strategy_name}: "
                        f"{len(tuned_candidates)} tuned variants (grid search)")
        
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
            return {'total_trades': 0, 'max_drawdown_pct': 0.0, 'total_return_pct': 0.0}
        
        if initial_capital is None:
            initial_capital = getattr(self.config, 'initial_capital', 10000.0)
        sorted_trades = sorted(trades, key=lambda t: t.get('entry_bar', t.get('signal_bar', 0)))
        equity = initial_capital
        equity_curve = [equity]
        for trade in sorted_trades:
            equity += trade.get('pnl_dollars', 0.0)
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
            features=features,
            timeframe=timeframe,
        )
        return {
            'total_trades': metrics.get('total_trades', 0),
            'win_rate': metrics.get('win_rate', 0.0),
            'profit_factor': metrics.get('profit_factor', 0.0),
            'total_pnl': metrics.get('total_pnl', 0.0),
            'gross_profit': metrics.get('gross_profit', 0.0),
            'gross_loss': metrics.get('gross_loss', 0.0),
            'avg_win': metrics.get('avg_win_dollars', 0.0),
            'avg_loss': metrics.get('avg_loss_dollars', 0.0),
            'sharpe_approx': metrics.get('sharpe_ratio', 0.0),
            'max_drawdown_pct': metrics.get('max_drawdown_pct', 0.0),
            'total_return_pct': metrics.get('total_return_pct', 0.0),
        }
    
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
        total = 0
        for cand in candidates:
            total += max(int(cand.get('search_trials', 1) or 1), 1)
        return max(total, 1)

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
        scored_candidates = []  # List of (score, candidate) tuples
        rejection_reasons = []  # Track why candidates were rejected at gate level
        best_score = -float('inf')
        best_candidate = None

        for cand in candidates:
            train_metrics = cand['train_regime_metrics'].get(regime, {})
            val_metrics = cand['val_regime_metrics'].get(regime, {})

            train_trades = train_metrics.get('total_trades', 0)
            val_trades = val_metrics.get('total_trades', 0)

            # Check minimum trades
            if train_trades < self.min_train_trades:
                continue
            if val_trades < self.min_val_trades:
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
            
            # Apply Deflated Sharpe Ratio as a Sharpe-only confidence adjustment.
            adjusted_val_metrics = dict(val_metrics)

            # This keeps the multiple-testing penalty aligned to the metric DSR models.
            # Positive Sharpe inputs are deflated; non-positive Sharpe is left unchanged.
            val_sharpe = val_metrics.get('sharpe_approx', val_metrics.get('sharpe_ratio', 0.0))
            n_trials = self._effective_search_trials(candidates)
            dsr = self._deflated_sharpe_ratio(
                sharpe=val_sharpe,
                n_trades=val_trades,
                n_trials=n_trials,
            )
            # Score based on validation metrics (with train as fallback).
            # DSR=1.0 → no penalty; DSR=0.0 → score halved
            if val_sharpe > 0:
                adjusted_val_metrics['sharpe_approx'] = val_sharpe * dsr
                adjusted_val_metrics['sharpe_ratio'] = adjusted_val_metrics['sharpe_approx']
            score = self._compute_regime_score(train_metrics, adjusted_val_metrics)
            scored_candidates.append((score, cand))

            if score > best_score:
                best_score = score
                best_candidate = cand

        if not scored_candidates or best_candidate is None:
            reason = "No candidates met minimum trade/drawdown/profitability thresholds"
            if rejection_reasons:
                reason += f" (rejected: {len(rejection_reasons)} candidates by gate checks)"
            return None, False, reason

        # Sort descending by score
        scored_candidates.sort(key=lambda x: x[0], reverse=True)

        # Cap the number of validation attempts
        top_k = min(self.regime_validation_top_k, len(scored_candidates))

        logger.debug(
            f"[{symbol}] [{timeframe}] [{regime}] Candidate descent: "
            f"{len(scored_candidates)} gate-passed, validating top-{top_k}"
        )

        # Iterate top-K candidates, return first that passes validation
        validation_failures = []
        for rank, (score, cand) in enumerate(scored_candidates[:top_k], start=1):
            train_metrics = cand['train_regime_metrics'].get(regime, {})
            val_metrics = cand['val_regime_metrics'].get(regime, {})
            strategy_name = cand.get('strategy_name', 'Unknown')

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
                    ),
                )

                return config, True, validation_reason
            else:
                validation_failures.append(f"#{rank} {strategy_name}: {validation_reason}")
                logger.debug(
                    f"[{symbol}] [{timeframe}] [{regime}] Descent: "
                    f"rank #{rank} {strategy_name} FAILED: {validation_reason}"
                )

        # All top-K candidates failed validation - return the best one as unvalidated
        best_score, best_candidate = scored_candidates[0]
        train_metrics = best_candidate['train_regime_metrics'].get(regime, {})
        val_metrics = best_candidate['val_regime_metrics'].get(regime, {})
        
        # Validate the winner using fx_backtester rules
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
            ),
        )

        descent_summary = "; ".join(validation_failures)
        final_reason = (
            f"All {len(validation_failures)} descent candidates failed validation "
            f"[{descent_summary}]"
        )
        logger.info(
            f"[{symbol}] [{timeframe}] [{regime}] Descent exhausted: "
            f"{len(validation_failures)}/{top_k} candidates failed validation"
        )

        return config, False, final_reason
    
    def _validate_regime_winner(self,
                                 train_metrics: Dict[str, Any],
                                 val_metrics: Dict[str, Any],
                                 regime: str,
                                 candidate_name: str = "Unknown") -> Tuple[bool, str]:
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

        # Check minimum validation trades
        if val_trades < self.min_val_trades:
            return False, f"Insufficient val trades: {val_trades} < {self.min_val_trades}"

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
            exceptional_val_min_trades = max(self.min_val_trades * 2, 50)
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
        
        if self.use_fx_scoring:
            robustness = self.scorer.calculate_fx_score_robustness_ratio(
                train_full, val_full, purpose="selection"
            )
        else:
            # Simple profit factor ratio (train_pf already declared above)
            robustness = val_pf / (train_pf + 0.1) if train_pf > 0 else 0.5
        
        # Check robustness OR Sharpe override
        if robustness < self.min_robustness_ratio and val_sharpe <= self.val_min_sharpe_override:
            return False, f"Low robustness {robustness:.2f} < {self.min_robustness_ratio} and Sharpe {val_sharpe:.2f} <= {self.val_min_sharpe_override}"
        
        return True, f"Validated (PF={val_pf:.2f}, ret={val_return:.1f}%, robustness={robustness:.2f}, dd={val_drawdown:.1f}%)"

    def _build_validation_evidence(self,
                                   train_metrics: Dict[str, Any],
                                   val_metrics: Dict[str, Any],
                                   *,
                                   score: Optional[float] = None,
                                   validation_reason: str = "") -> Dict[str, Any]:
        """Build compact, persisted validation evidence for dashboards and review."""
        train_full = self._bucket_to_full_metrics(train_metrics or {})
        val_full = self._bucket_to_full_metrics(val_metrics or {})
        if self.use_fx_scoring:
            robustness = self.scorer.calculate_fx_score_robustness_ratio(
                train_full, val_full, purpose="selection"
            )
        else:
            train_pf = float(train_metrics.get("profit_factor", 0.0) or 0.0)
            val_pf = float(val_metrics.get("profit_factor", 0.0) or 0.0)
            robustness = val_pf / (train_pf + 0.1) if train_pf > 0 else 0.5
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
        }
        if score is not None:
            evidence["selection_score_raw"] = float(score)
        return evidence
    
    def _compute_regime_score(self,
                               train_metrics: Dict[str, Any],
                               val_metrics: Dict[str, Any]) -> float:
        """
        Compute score for a regime bucket using fx_backtester generalization scoring.
        
        When scoring_mode='fx_backtester', uses:
        - Gap penalty for train->val degradation
        - Robustness ratio boost
        - Validation-first scoring
        - Trade count stability factor (Gap C fix)
        
        Falls back to simple composite for pm_weighted mode.
        """
        # ===== FIX Gap C: Trade count stability factor =====
        # Prefer candidates with more trades (more statistical confidence)
        # Uses log scaling: log1p(trades) / log1p(target) capped at 1.0
        train_trades = train_metrics.get('total_trades', 0)
        val_trades = val_metrics.get('total_trades', 0)
        
        # Target trade counts for full stability bonus
        # (2x minimum is considered "healthy")
        target_train = self.min_train_trades * 2
        target_val = self.min_val_trades * 2
        
        # Calculate stability factors using log scaling
        import math
        train_stability = min(1.0, math.log1p(train_trades) / math.log1p(target_train)) if target_train > 0 else 0.5
        val_stability = min(1.0, math.log1p(val_trades) / math.log1p(target_val)) if target_val > 0 else 0.5
        
        # Combined stability factor (weighted average favoring validation)
        stability_factor = 0.3 * train_stability + 0.7 * val_stability
        # Clamp to reasonable range [0.7, 1.0] - don't over-penalize low counts
        stability_factor = 0.7 + 0.3 * stability_factor
        
        if self.use_fx_scoring:
            # Build pseudo-metrics dicts for the scorer (regime bucket metrics -> full metrics format)
            train_full = self._bucket_to_full_metrics(train_metrics)
            val_full = self._bucket_to_full_metrics(val_metrics)
            
            # Use validation metrics if sufficient trades
            if val_trades >= self.min_val_trades:
                # Use fx_generalization_score for proper gap penalty + robustness boost
                final_score, train_score, val_score, rr = self.scorer.fx_generalization_score(
                    train_full, val_full, purpose="selection"
                )
                # Apply stability factor (Gap C fix)
                return final_score * stability_factor
            else:
                # Not enough validation trades - use train score with discount
                train_score = self.scorer.score(train_full, purpose="selection")
                return train_score * 0.7 * stability_factor  # Discount for no validation
        
        # Legacy pm_weighted scoring (original behavior)
        if val_trades >= self.min_val_trades:
            metrics = val_metrics
            train_pf = train_metrics.get('profit_factor', 1.0)
            val_pf = metrics.get('profit_factor', 1.0)
            robustness = min(1.0, val_pf / (train_pf + 0.1)) if train_pf > 0 else 0.5
        else:
            metrics = train_metrics
            robustness = 0.7
        
        if metrics.get('total_trades', 0) == 0:
            return -float('inf')
        
        pf = metrics.get('profit_factor', 0)
        wr = metrics.get('win_rate', 0) / 100
        sharpe = metrics.get('sharpe_approx', 0)
        total_pnl = metrics.get('total_pnl', 0)
        
        score = (
            min(pf, 5.0) * 20 +
            wr * 30 +
            min(max(sharpe, -2), 3) * 15 +
            min(total_pnl / 50, 20) +
            robustness * 20
        )
        
        # Apply stability factor (Gap C fix)
        return score * stability_factor
    
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
            'sharpe_ratio': bucket_metrics.get('sharpe_approx', 0),
            'max_drawdown_pct': max_dd,  # FIX: Use properly computed value (default worst-case)
            'gross_profit': bucket_metrics.get('gross_profit', 0),
            'gross_loss': bucket_metrics.get('gross_loss', 0),
            'total_pnl': bucket_metrics.get('total_pnl', 0),
            'expectancy_pips': bucket_metrics.get('avg_win', 0) * bucket_metrics.get('win_rate', 0) / 100
                             - bucket_metrics.get('avg_loss', 0) * (1 - bucket_metrics.get('win_rate', 0) / 100),
            'sortino_ratio': bucket_metrics.get('sortino_approx', 0),
            'calmar_ratio': bucket_metrics.get('calmar_ratio', 0),
            'std_weekly_return': bucket_metrics.get('std_weekly_return', 0.0),
            'mean_r': bucket_metrics.get('mean_r', 0),
            'worst_5pct_r': bucket_metrics.get('worst_5pct_r', 0),
            'max_consecutive_losses': bucket_metrics.get('max_consecutive_losses', 0),
        }
    
    def _normalize_score(self, score: float) -> float:
        """
        Normalize score to 0-1 range using sigmoid mapping.

        This prevents saturation at extreme scores.
        Calibrated for the continuous-DD scoring distribution where
        typical scores land in the 25-55 range.
        """
        # Sigmoid centered at 45 with scale 30
        # score=15 -> ~0.27, score=30 -> ~0.38, score=45 -> 0.5,
        # score=60 -> ~0.62, score=80 -> ~0.76
        import math
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
        set_regime_tp_multipliers(getattr(config, "regime_tp_multipliers", None))
        self.data_loader = DataLoader(config.data_dir)
        self.splitter = DataSplitter(config)
        self.enhancement_seams: EnhancementSeams = create_default_enhancement_seams(config)
        
        # Legacy pipeline surfaces removed from production path (F1).
        # Classes StrategySelector, HyperparameterOptimizer, Validator, and
        # RetrainPeriodSelector remain defined for offline research use but are
        # no longer instantiated by the production pipeline.

        # Regime-aware optimizer (sole production path)
        self.regime_optimizer = RegimeOptimizer(config)

    @staticmethod
    def _locate_default_origin(default_config: Optional[RegimeConfig],
                               regime_configs: Dict[str, Dict[str, RegimeConfig]]
                               ) -> Tuple[Optional[str], Optional[str]]:
        """Locate the timeframe/regime source of the chosen default config."""
        if default_config is None:
            return None, None
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
        """Slice by half-open timestamp window [start_ts, end_ts)."""
        if df is None or len(df) == 0:
            return pd.DataFrame()
        mask = pd.Series(True, index=df.index)
        if start_ts is not None:
            mask &= df.index >= start_ts
        if end_ts is not None:
            mask &= df.index < end_ts
        return df.loc[mask].copy()

    def _build_m5_workflow_meta(self, base_m5: pd.DataFrame) -> Dict[str, Any]:
        """Build per-symbol workflow metadata from the base M5 data window."""
        indices = self.splitter.get_workflow_indices(len(base_m5))

        def _meta(name: str) -> Dict[str, Any]:
            start, end = indices[name]
            frame = base_m5.iloc[start:end]
            return {
                "start_index": int(start),
                "end_index": int(end),
                "rows": int(max(0, end - start)),
                "start_ts": frame.index[0].isoformat() if len(frame) else None,
                "end_ts": frame.index[-1].isoformat() if len(frame) else None,
            }

        return {
            "mode": str(getattr(self.config, "data_workflow_mode", "active_recent_m5")),
            "base_timeframe": "M5",
            "max_bars": int(getattr(self.config, "max_bars", 300000)),
            "historical_stress_audit_bars": int(getattr(self.config, "historical_stress_audit_bars", 50000)),
            "active_universe_bars": int(getattr(self.config, "active_universe_bars", 250000)),
            "active_stage2_pct": float(getattr(self.config, "active_stage2_pct", 50.0)),
            "windows": {
                "workflow_window": _meta("workflow_window"),
                "historical_stress_audit": _meta("historical_stress_audit"),
                "active_universe": _meta("active_universe"),
                "stage2_selection": _meta("stage2_selection"),
                "stage2_selection_with_warmup": _meta("stage2_selection_with_warmup"),
            },
        }

    def _load_symbol_timeframe_data(self, symbol: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        """
        Load the latest base-M5 workflow window and derive higher timeframes
        from the same timestamp-bounded source.
        """
        base_m5 = self.data_loader.get_data(symbol, "M5")
        if base_m5 is None or len(base_m5) < 200:
            return {}, {}
        base_m5 = base_m5.sort_index()
        max_bars = int(getattr(self.config, "max_bars", 300000) or 300000)
        if max_bars > 0 and len(base_m5) > max_bars:
            base_m5 = base_m5.tail(max_bars).copy()

        workflow_meta = self._build_m5_workflow_meta(base_m5)
        source_meta = dict(getattr(self.data_loader, "_source_meta", {}).get(f"{symbol}_M5", {}) or {})
        workflow_window = (workflow_meta.get("windows", {}) or {}).get("workflow_window", {})
        workflow_meta["source"] = {
            "path": source_meta.get("path"),
            "mtime": source_meta.get("mtime"),
            "rows": source_meta.get("rows"),
            "last_index": source_meta.get("last_index"),
        }
        workflow_meta["data_window_fingerprint"] = _stable_hash({
            "symbol": symbol,
            "source": workflow_meta["source"],
            "workflow_window": workflow_window,
        })
        data_by_tf: Dict[str, pd.DataFrame] = {}
        for tf in self.config.timeframes:
            tf_name = str(tf).upper()
            try:
                if tf_name == "M5":
                    data = base_m5.copy()
                else:
                    data = self.data_loader.resample(base_m5, tf_name)
                if data is not None and len(data) >= 200:
                    data_by_tf[tf_name] = data.copy()
            except Exception as exc:
                logger.warning(f"[{symbol}] [{tf_name}] Data preparation failed: {exc}")
        return data_by_tf, workflow_meta

    def _attach_holdout_metrics(self,
                                symbol: str,
                                regime_configs: Dict[str, Dict[str, RegimeConfig]],
                                holdout_features_by_tf: Dict[str, pd.DataFrame],
                                holdout_warmup_by_tf: Dict[str, int]) -> None:
        """Evaluate validated winners on the freshest holdout window."""
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
                try:
                    strategy = StrategyRegistry.get(cfg.strategy_name, **cfg.parameters)
                    signals = strategy.generate_signals(holdout_features, symbol)
                    holdout_result = self.regime_optimizer.backtester.run(
                        holdout_features,
                        signals,
                        symbol,
                        strategy,
                        warmup_bars=holdout_warmup,
                    )
                    holdout_bucket = self.regime_optimizer._bucket_trades_by_regime(
                        holdout_result.get('trades', []),
                        holdout_features,
                        timeframe=tf,
                    )
                    cfg.holdout_metrics = holdout_bucket.get(regime, {'total_trades': 0})
                except Exception as exc:
                    logger.warning(f"[{symbol}] [{tf}] [{regime}] Holdout evaluation failed: {exc}")

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
        audit["enabled"] = bool(getattr(self.config, "walk_forward_audit_enabled", False))
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
        if not bool(getattr(self.config, "walk_forward_audit_enabled", False)):
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
        risk_opt_enabled = bool(getattr(self.config, "risk_management_optimization_enabled", True))
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
            
            if not data_by_tf:
                result.error_message = "No valid data available"
                return result
            
            result.timeframes_tested = len(data_by_tf)
            
            # Get all strategies
            strategies = StrategyRegistry.get_all_instances()
            result.strategies_tested = len(strategies)
            artifact_meta = build_artifact_meta(self.config, strategies)
            
            # Split data and compute features once on the full series.
            train_features_by_tf = {}
            val_features_by_tf = {}
            val_warmup_by_tf = {}
            holdout_features_by_tf = {}
            holdout_warmup_by_tf = {}
            historical_features_by_tf = {}
            stage3_features_by_tf = {}
            stage3_warmup_by_tf = {}
            
            regime_params_file = getattr(self.config, 'regime_params_file', 'regime_params.json')
            active_mode = self.splitter.workflow_enabled()
            
            for tf, data in data_by_tf.items():
                full_features = FeatureComputer.compute_all(
                    data, symbol=symbol, timeframe=tf,
                    regime_params_file=regime_params_file
                )
                if active_mode and data_window_meta:
                    windows = data_window_meta.get("windows", {})
                    hist_meta = windows.get("historical_stress_audit", {})
                    active_meta = windows.get("active_universe", {})
                    stage2_meta = windows.get("stage2_selection", {})
                    stage2_warm_meta = windows.get("stage2_selection_with_warmup", {})

                    hist_start = pd.Timestamp(hist_meta["start_ts"]) if hist_meta.get("start_ts") else None
                    hist_end = pd.Timestamp(active_meta["start_ts"]) if active_meta.get("start_ts") else None
                    active_start = pd.Timestamp(active_meta["start_ts"]) if active_meta.get("start_ts") else None
                    stage2_start = pd.Timestamp(stage2_meta["start_ts"]) if stage2_meta.get("start_ts") else active_start
                    stage2_warm_start = (
                        pd.Timestamp(stage2_warm_meta["start_ts"])
                        if stage2_warm_meta.get("start_ts") else stage2_start
                    )

                    active_features = self._slice_time_window(full_features, active_start, None)
                    stage2_features = self._slice_time_window(full_features, stage2_warm_start, None)
                    historical_features = self._slice_time_window(full_features, hist_start, hist_end)
                    if len(active_features) >= 200:
                        train_features_by_tf[tf] = active_features
                    if len(stage2_features) >= 50:
                        val_features_by_tf[tf] = stage2_features
                        stage3_features_by_tf[tf] = stage2_features.copy()
                        val_warmup_by_tf[tf] = int((stage2_features.index < stage2_start).sum()) if stage2_start is not None else 0
                        stage3_warmup_by_tf[tf] = val_warmup_by_tf[tf]
                    if len(historical_features) >= 50:
                        historical_features_by_tf[tf] = historical_features
                else:
                    split_idx = self.splitter.get_split_indices(len(data))
                    train_start, train_end = split_idx['train']
                    val_start, val_end = split_idx['validation_with_warmup']
                    holdout_start, holdout_end = split_idx['holdout_with_warmup']

                    train_features_by_tf[tf] = full_features.iloc[train_start:train_end].copy()
                    val_features_by_tf[tf] = full_features.iloc[val_start:val_end].copy()
                    val_warmup_by_tf[tf] = max(0, split_idx['validation'][0] - val_start)
                    holdout_features_by_tf[tf] = full_features.iloc[holdout_start:holdout_end].copy()
                    holdout_warmup_by_tf[tf] = max(0, split_idx['holdout'][0] - holdout_start)
                    stage3_features_by_tf[tf] = holdout_features_by_tf[tf].copy()
                    stage3_warmup_by_tf[tf] = holdout_warmup_by_tf[tf]
            
            # Run regime-aware optimization
            with Timer(f"[{symbol}] Regime Optimization"):
                regime_configs, default_config, validated_count, unvalidated_count = self.regime_optimizer.optimize_symbol(
                    symbol, train_features_by_tf, val_features_by_tf, strategies,
                    val_warmup_by_tf=val_warmup_by_tf,
                )
            
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
            
            if holdout_features_by_tf:
                self._attach_holdout_metrics(symbol, regime_configs, holdout_features_by_tf, holdout_warmup_by_tf)
            self._attach_historical_stress_audit(symbol, regime_configs, historical_features_by_tf)
            self._attach_walk_forward_audit(symbol, regime_configs, val_features_by_tf, val_warmup_by_tf)
            risk_selection_stage = str(
                getattr(self.config, "risk_management_selection_stage", "stage3") or "stage3"
            ).lower()
            if risk_selection_stage == "stage2":
                governance_features_by_tf = train_features_by_tf
                governance_warmup_by_tf = {tf: 0 for tf in train_features_by_tf}
                governance_surface = "stage2_active_universe"
            else:
                governance_features_by_tf = stage3_features_by_tf
                governance_warmup_by_tf = stage3_warmup_by_tf
                governance_surface = "stage3_selection" if active_mode else "freshest_tail_holdout"
            self._attach_governance_policies(
                symbol,
                regime_configs,
                governance_features_by_tf,
                governance_warmup_by_tf,
                selection_surface=governance_surface,
            )
            best_overall_tf, best_overall_regime = self._locate_default_origin(default_config, regime_configs)
            if best_overall_tf and best_overall_regime and default_config is not None:
                matched_cfg = regime_configs[best_overall_tf][best_overall_regime]
                default_config.holdout_metrics = matched_cfg.holdout_metrics
                default_config.historical_audit_metrics = dict(matched_cfg.historical_audit_metrics)
                default_config.live_observability = dict(matched_cfg.live_observability)
                default_config.walk_forward_audit = dict(matched_cfg.walk_forward_audit)
                default_config.governance_policy = dict(matched_cfg.governance_policy)

            for regimes in regime_configs.values():
                for cfg in regimes.values():
                    cfg.artifact_meta = {**artifact_meta, **dict(cfg.artifact_meta or {})}
            if default_config is not None:
                default_config.artifact_meta = {**artifact_meta, **dict(default_config.artifact_meta or {})}

            default_validation_evidence = (
                dict(getattr(default_config, "validation_evidence", {}) or {})
                if default_config is not None else {}
            )
            optimizer_telemetry = {
                "training_gates": self.regime_optimizer.consume_training_gate_telemetry(),
                "resample_cache": self.data_loader.get_resample_cache_stats(reset=False),
            }
            default_validation_evidence["optimizer_telemetry"] = optimizer_telemetry
            selected_robustness = float(default_validation_evidence.get("robustness_ratio", 0.0) or 0.0)

            retrain_days = int(getattr(self.config, 'fixed_retrain_days', getattr(self.config, 'optimization_valid_days', 14)))
            now = datetime.now()
            valid_until = _scheduled_valid_until(self.config, now)
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
            # showing the realized regime distribution across train+val features.
            self._log_regime_coverage(symbol, train_features_by_tf, val_features_by_tf)
            
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

    def _build_ledger_status(self) -> Dict[str, Any]:
        """Build operator-facing completion metadata for the active winner ledger."""
        configured_symbols = [str(symbol) for symbol in (self.symbols or [])]
        optimized_symbols = sorted(str(symbol) for symbol in self.symbol_configs.keys())
        artifact_contract_counts: Dict[str, int] = {}
        for cfg in self.symbol_configs.values():
            artifact = getattr(cfg, "artifact_meta", {}) or {}
            key = "|".join([
                str(artifact.get("artifact_version", "missing")),
                str(artifact.get("split_contract_version", "missing")),
            ])
            artifact_contract_counts[key] = artifact_contract_counts.get(key, 0) + 1
        configured_set = {symbol.upper() for symbol in configured_symbols}
        optimized_set = {symbol.upper() for symbol in optimized_symbols}
        return {
            "configured_symbol_count": len(configured_symbols),
            "optimized_symbol_count": len(optimized_symbols),
            "complete_universe": bool(configured_set) and configured_set.issubset(optimized_set),
            "missing_symbols": sorted(configured_set - optimized_set),
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
        
        # Determine which symbols to optimize
        artifact_meta = build_artifact_meta(self.config)
        to_optimize, to_skip = self.ledger.get_symbols_to_optimize(
            self.symbols,
            overwrite=overwrite,
            current_artifact_meta=artifact_meta,
        )
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"PORTFOLIO MANAGER - STATEFUL OPTIMIZATION")
        logger.info(f"Total symbols: {len(self.symbols)}")
        logger.info(f"To optimize: {len(to_optimize)}")
        logger.info(f"Skipping: {len(to_skip)} (valid configs)")
        if overwrite:
            logger.info(f"OVERWRITE MODE: ignoring validity checks")
        logger.info(f"Strategies: {StrategyRegistry.count()}")
        logger.info(f"Timeframes: {self.config.timeframes}")
        logger.info(f"{'#'*60}")
        
        if not to_optimize:
            logger.info("All symbols have valid configs. Nothing to optimize.")
            return results
        
        max_workers = int(getattr(self.config, "optimization_max_workers", 1) or 1)

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
                        logger.warning(f"FAILED {symbol_for_save}: keeping existing config (if any)")

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
                        # Keep existing config on failure (if any)
                        logger.warning(f"FAILED {symbol}: keeping existing config (if any)")
                    
                except Exception as e:
                    logger.exception(f"[{symbol}] Optimization error: {e}")
                    # Create a failure result but don't overwrite existing config
                    from pm_pipeline import PipelineResult
                    results[symbol] = PipelineResult(symbol=symbol, error_message=str(e))
                
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
        
        return results
    
    def get_validated_configs(self) -> Dict[str, SymbolConfig]:
        """Get only validated configurations."""
        # Refresh from ledger to ensure consistency
        self.symbol_configs = self.ledger.get_all_configs()
        return {s: c for s, c in self.symbol_configs.items() if c.is_validated}

    def _live_eligibility_reason(self,
                                 symbol: str,
                                 config: SymbolConfig,
                                 *,
                                 now: datetime,
                                 current_artifact_meta: Dict[str, Any],
                                 artifact_policy: str,
                                 expiry_grace_minutes: int) -> Tuple[bool, str]:
        """Evaluate the live-trading eligibility contract for one config."""
        if not config.is_validated:
            return False, f"invalid ({config.validation_reason or 'not validated'})"
        if config.valid_until is None:
            return False, "no expiry date"
        grace = max(0, int(expiry_grace_minutes or 0))
        valid_deadline = config.valid_until + pd.Timedelta(minutes=grace).to_pytimedelta()
        if now >= valid_deadline:
            return False, f"expired at {config.valid_until.strftime('%Y-%m-%d %H:%M')}"
        if config.has_regime_configs() and config.count_regime_winners() <= 0:
            return False, "no tradeable regime winners"
        if not config.has_regime_configs():
            default_cfg = config.default_config
            if default_cfg is not None and default_cfg.is_no_trade_marker():
                return False, "default config is NO_TRADE"
            if str(config.strategy_name or "").upper() == "NO_TRADE" or bool((config.artifact_meta or {}).get("no_trade")):
                return False, "symbol config is NO_TRADE"
            if not config.strategy_name and default_cfg is None:
                return False, "no strategy"

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
        production_retrain_interval_weeks=2,
    )
    
    pm = PortfolioManager(
        config=config,
        symbols=['EURUSD', 'GBPUSD']
    )
    
    pm.print_status()
    print("\npm_pipeline.py loaded successfully!")

