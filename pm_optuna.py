"""
FX Portfolio Manager - Optuna TPE Optimization Module
======================================================

Production-ready implementation of Optuna-based hyperparameter optimization
using Tree-structured Parzen Estimators (TPE) for efficient search.

Key Features:
- TPE sampler optimized for discrete/categorical parameter spaces
- Parameter constraint handling (e.g., fast_period < slow_period)
- Multi-objective regime-aware optimization
- Robust logging with trial statistics
- Reproducible results via seed control
- Graceful fallback when Optuna unavailable

Integration Points:
- HyperparameterOptimizer: Single-strategy optimization
- RegimeOptimizer: Multi-regime simultaneous optimization
- RetrainPeriodSelector: Retrain period optimization

Version: 1.1 (Updated with ExperimentalWarning suppression)
"""

import logging
import hashlib
import math
import time
from typing import Dict, List, Any, Optional, Tuple, Callable, Union, Sequence, Mapping
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

from pm_search_outcomes import (
    SearchInfrastructureError,
    SearchOutcomeKind,
    SearchStage,
    TrialOutcome,
    aggregate_family_cell_outcomes,
    classify_search_exception,
)

# Optuna import with graceful fallback
try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import MedianPruner, HyperbandPruner
    from optuna.trial import TrialState
    from optuna.exceptions import ExperimentalWarning  # Import for suppression
    import warnings  # Import for suppression
    
    OPTUNA_AVAILABLE = True
    
    # Configure Optuna logging
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    
    # Suppress Optuna ExperimentalWarning (specifically for multivariate TPE)
    # This keeps logs clean while using the advanced correlation features
    warnings.filterwarnings("ignore", category=ExperimentalWarning)
    
except ImportError:
    OPTUNA_AVAILABLE = False
    optuna = None
    TPESampler = None
    MedianPruner = None
    HyperbandPruner = None
    TrialState = None

logger = logging.getLogger(__name__)


def _stable_seed(*parts: Any) -> int:
    """Deterministic non-negative seed derived from stable context parts."""
    payload = "|".join(str(part) for part in parts).encode("utf-8", errors="replace")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFF


def _parameter_hash(params: Dict[str, Any]) -> str:
    """Stable order-invariant identity for one cleaned parameter set."""
    import json
    payload = json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8", errors="strict")
    return hashlib.sha256(payload).hexdigest()


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


def _bounded_cartesian_combinations(
    axes: Sequence[Sequence[Any]],
    *,
    limit: int,
    seed: int,
) -> tuple[list[tuple[Any, ...]], int]:
    """Decode a bounded deterministic Cartesian design in O(limit) memory."""
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

    def decode(encoded: int) -> tuple[Any, ...]:
        result: list[Any] = [None] * len(values)
        remainder = int(encoded)
        for index in range(len(values) - 1, -1, -1):
            axis = values[index]
            result[index] = axis[remainder % len(axis)]
            remainder //= len(axis)
        return tuple(result)

    return [decode((start + offset * step) % total) for offset in range(count)], total


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


def _bounded_warmup(features: Optional[pd.DataFrame], warmup_bars: int) -> int:
    """Normalize an authenticated non-scoring prefix for one feature frame."""
    length = 0 if features is None else len(features)
    return min(length, max(0, int(warmup_bars or 0)))


def _has_scored_signal(signals: pd.Series, warmup_bars: int) -> bool:
    """Return whether a signal can enter at or after the evaluation boundary."""
    warmup = min(len(signals), max(0, int(warmup_bars or 0)))
    # Backtester enters on the bar after a signal. The final context-bar signal
    # is therefore a legal first-boundary entry, not a warm-up trade.
    signal_start = max(0, warmup - 1)
    return bool(signals.iloc[signal_start:].abs().sum() > 0)


def _scored_rows(features: Optional[pd.DataFrame], warmup_bars: int) -> int:
    """Return rows legally available to metrics after the context prefix."""
    length = 0 if features is None else len(features)
    return max(0, length - _bounded_warmup(features, warmup_bars))


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class OptunaConfig:
    """Configuration for Optuna optimization."""
    # Trial budget
    n_trials: int = 100
    
    # TPE sampler settings
    n_startup_trials: int = 10  # Random trials before TPE kicks in
    seed: int = 0
    multivariate: bool = True  # Consider parameter correlations
    
    # Pruning settings
    use_pruning: bool = False  # Disabled by default - full eval needed for trading
    pruning_warmup_steps: int = 5
    
    # Early rejection thresholds (configurable)
    train_dd_multiplier: float = 1.25
    val_dd_multiplier: float = 1.0
    penalty_dd_rejection: float = -500.0
    penalty_no_trades: float = -1000.0
    # Progressive rejection: relax thresholds during early exploration
    progressive_rejection: bool = True
    progressive_warmup_frac: float = 0.20
    progressive_warmup_multiplier: float = 1.5
    
    # Early rejection thresholds (configurable)
    train_dd_multiplier: float = 1.25       # Training DD allowed = max_dd * this
    val_dd_multiplier: float = 1.0          # Validation DD allowed = max_dd * this
    penalty_dd_rejection: float = -500.0    # Score returned for DD rejection
    penalty_no_trades: float = -1000.0      # Score returned for no/min trades
    # Progressive rejection: relax thresholds during early exploration
    progressive_rejection: bool = True
    progressive_warmup_frac: float = 0.20   # First 20% of trials get relaxed thresholds
    progressive_warmup_multiplier: float = 1.5  # Extra multiplier during warmup phase

    # Logging
    log_interval: int = 25  # Log progress every N trials

    # Phase D3 — family-size-aware search budget (opt-in)
    family_size_aware_budget: bool = False
    min_trials_per_strategy: int = 50
    max_trials_per_strategy: int = 5000
    target_coverage_pct: float = 0.10  # 10% of cartesian space

    @classmethod
    def from_pipeline_config(cls, config) -> 'OptunaConfig':
        """Create OptunaConfig from PipelineConfig."""
        return cls(
            n_trials=getattr(config, 'regime_hyperparam_max_combos', 100),
            n_startup_trials=min(10, getattr(config, 'regime_hyperparam_max_combos', 100) // 10),
            seed=int(getattr(config, 'optimization_seed', 0) or 0),
            multivariate=True,
            family_size_aware_budget=bool(getattr(config, 'optuna_family_size_aware_budget', False)),
            min_trials_per_strategy=int(getattr(config, 'optuna_min_trials_per_strategy', 50)),
            max_trials_per_strategy=int(getattr(config, 'optuna_max_trials_per_strategy', 5000)),
            target_coverage_pct=float(getattr(config, 'optuna_target_coverage_pct', 0.10)),
        )

    def resolve_trial_budget(self, search_space_size: int) -> Tuple[int, str]:
        """Resolve effective trial budget for a given cartesian search-space size.

        Returns (n_trials, mode) where mode is one of:
        - "flat":             flat n_trials (legacy behavior); capped at search_space_size
        - "family_aware":     coverage-based; clamp(min, max, ceil(coverage * size))
        """
        if not self.family_size_aware_budget:
            return min(self.n_trials, search_space_size), "flat"
        target = max(
            self.min_trials_per_strategy,
            int(round(self.target_coverage_pct * search_space_size)),
        )
        capped = min(self.max_trials_per_strategy, target, search_space_size)
        return capped, "family_aware"


# =============================================================================
# PARAMETER SPACE DEFINITION
# =============================================================================

class ParameterSpace:
    """
    Defines and samples from strategy parameter spaces for Optuna.
    
    Handles:
    - Discrete/categorical parameters from strategy param grids
    - Parameter constraints (e.g., slow_period > fast_period)
    - Type conversion for Optuna suggestions
    """
    
    # Known parameter constraints
    CONSTRAINTS = {
        # (param1, param2, relation): param1 relation param2
        ('fast_period', 'slow_period', 'lt'),  # fast_period < slow_period
        ('tenkan_period', 'kijun_period', 'lt'),  # tenkan < kijun (Ichimoku)
        ('short_period', 'long_period', 'lt'),  # short < long
        ('signal_period', 'slow_period', 'lt'),  # signal < slow (MACD)
    }
    
    def __init__(self, param_grid: Dict[str, List], default_params: Dict[str, Any]):
        """
        Initialize parameter space from strategy param grid.
        
        Args:
            param_grid: Dict mapping param names to lists of possible values
            default_params: Default parameter values from strategy
        """
        self.param_grid = param_grid
        self.default_params = default_params
        self.param_info = self._analyze_params()
    
    def _analyze_params(self) -> Dict[str, Dict[str, Any]]:
        """Analyze parameter grid to determine types and ranges."""
        info = {}
        
        for name, values in self.param_grid.items():
            if not values:
                continue
            
            sample = values[0]
            sorted_vals = sorted(values) if all(isinstance(v, (int, float)) for v in values) else values
            
            if isinstance(sample, bool):
                info[name] = {
                    'type': 'categorical',
                    'choices': values,
                }
            elif isinstance(sample, int) and not isinstance(sample, bool):
                # Check if values form a continuous range
                if len(values) >= 2 and all(isinstance(v, int) for v in values):
                    info[name] = {
                        'type': 'int',
                        'low': min(values),
                        'high': max(values),
                        'choices': sorted_vals,  # Keep for constraint checking
                    }
                else:
                    info[name] = {
                        'type': 'categorical',
                        'choices': values,
                    }
            elif isinstance(sample, float):
                info[name] = {
                    'type': 'float',
                    'low': min(values),
                    'high': max(values),
                    'choices': sorted_vals,
                }
            else:
                info[name] = {
                    'type': 'categorical',
                    'choices': values,
                }
        
        return info
    
    @staticmethod
    def _is_scalar_choice(value: Any) -> bool:
        """Optuna categoricals accept only None/bool/int/float/str (incl. numpy scalars)."""
        return value is None or np.isscalar(value)

    def _suggest_categorical(self, trial: 'optuna.Trial', name: str, choices) -> Any:
        """Suggest one value from a discrete grid.

        Non-scalar choices (e.g. regime-set tuples like ('BREAKOUT','TREND')) are
        suggested by index and mapped back, so Optuna only ever receives scalar
        choices (tuples are unsupported and emit persistent-storage warnings)
        while the returned value remains the real choice.
        """
        choices = list(choices)
        if not choices:
            return None
        if all(self._is_scalar_choice(c) for c in choices):
            return trial.suggest_categorical(name, choices)
        idx = trial.suggest_categorical(name, list(range(len(choices))))
        return choices[int(idx)]

    def suggest(self, trial: 'optuna.Trial') -> Dict[str, Any]:
        """
        Suggest parameters using Optuna trial.

        Handles parameter constraints by adjusting dependent parameters.

        Args:
            trial: Optuna trial object

        Returns:
            Dict of suggested parameters merged with defaults
        """
        params = self.default_params.copy()

        # int/float grids and categoricals all sample the discrete grid; tuple/list
        # choices are index-encoded inside the helper so Optuna only sees scalars.
        for name, info in self.param_info.items():
            if info['type'] in ('categorical', 'int', 'float'):
                params[name] = self._suggest_categorical(trial, name, info['choices'])

        # Apply constraints
        params = self._apply_constraints(params)

        return params
    
    def _apply_constraints(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply parameter constraints to ensure valid configurations."""
        # EMA/MA crossover: slow > fast
        if 'fast_period' in params and 'slow_period' in params:
            if params['slow_period'] <= params['fast_period']:
                # Adjust slow_period to be greater
                fast = params['fast_period']
                slow_choices = self.param_info.get('slow_period', {}).get('choices', [])
                valid_slow = [s for s in slow_choices if s > fast]
                if valid_slow:
                    params['slow_period'] = min(valid_slow)
                else:
                    params['slow_period'] = fast + 5
        
        # Ichimoku: kijun > tenkan
        if 'tenkan_period' in params and 'kijun_period' in params:
            if params['kijun_period'] <= params['tenkan_period']:
                tenkan = params['tenkan_period']
                kijun_choices = self.param_info.get('kijun_period', {}).get('choices', [])
                valid_kijun = [k for k in kijun_choices if k > tenkan]
                if valid_kijun:
                    params['kijun_period'] = min(valid_kijun)
                else:
                    params['kijun_period'] = tenkan + 5
        
        # MACD: signal < slow
        if 'signal_period' in params and 'slow_period' in params:
            if params['signal_period'] >= params['slow_period']:
                slow = params['slow_period']
                signal_choices = self.param_info.get('signal_period', {}).get('choices', [])
                valid_signal = [s for s in signal_choices if s < slow]
                if valid_signal:
                    params['signal_period'] = max(valid_signal)
                else:
                    params['signal_period'] = max(1, slow - 1)
        
        # Stochastic: d <= k
        if 'k_period' in params and 'd_period' in params:
            if params['d_period'] > params['k_period']:
                params['d_period'] = params['k_period']
        
        return params
    
    def get_search_space_size(self) -> int:
        """Calculate total size of parameter search space."""
        size = 1
        for info in self.param_info.values():
            if 'choices' in info:
                size *= len(info['choices'])
        return size


# =============================================================================
# OPTIMIZATION RESULTS
# =============================================================================

@dataclass
class OptimizationStats:
    """Statistics from an optimization run."""
    n_trials: int = 0
    n_completed: int = 0
    n_pruned: int = 0
    n_failed: int = 0
    best_score: float = float('-inf')
    optimization_time_sec: float = 0.0
    method: str = "optuna_tpe"
    early_dd_rejections: int = 0  # Count of trials rejected due to high drawdown
    
    def __str__(self) -> str:
        base = (f"Trials: {self.n_trials} (completed={self.n_completed}, "
                f"pruned={self.n_pruned}, failed={self.n_failed}), "
                f"best_score={self.best_score:.4f}, time={self.optimization_time_sec:.1f}s")
        if self.early_dd_rejections > 0:
            base += f", early_dd_rejects={self.early_dd_rejections}"
        return base


@dataclass
class OptimizationResult:
    """Result from hyperparameter optimization."""
    best_params: Dict[str, Any]
    best_score: float
    train_metrics: Dict[str, Any]
    val_metrics: Dict[str, Any]
    stats: OptimizationStats
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'stats': {
                'n_trials': self.stats.n_trials,
                'n_completed': self.stats.n_completed,
                'n_pruned': self.stats.n_pruned,
                'optimization_time_sec': self.stats.optimization_time_sec,
                'method': self.stats.method,
                'early_dd_rejections': self.stats.early_dd_rejections,
            }
        }


# =============================================================================
# CORE OPTIMIZER
# =============================================================================

class OptunaTPEOptimizer:
    """
    Core Optuna TPE optimizer for hyperparameter tuning.
    
    Uses Tree-structured Parzen Estimators (TPE) which are well-suited for:
    - Discrete/categorical parameter spaces
    - Mixed parameter types
    - Parameter constraints
    
    TPE models P(x|y) and P(y) separately, making it efficient for
    trading strategy optimization where evaluations are expensive.
    """
    
    REGIME_SEARCH_CONTRACT = "independent_regime_family_train_only_v1"

    def __init__(self, 
                 config: OptunaConfig,
                 backtester,
                 scorer,
                 strategy_registry):
        """
        Initialize the optimizer.
        
        Args:
            config: OptunaConfig with optimization settings
            backtester: Backtester instance for strategy evaluation
            scorer: StrategyScorer instance for computing scores
            strategy_registry: StrategyRegistry for creating strategy instances
        """
        self.config = config
        self.backtester = backtester
        self.scorer = scorer
        self.strategy_registry = strategy_registry
        self.last_regime_search_metadata: Dict[str, Any] = {}
        
        if not OPTUNA_AVAILABLE:
            logger.warning("Optuna not available. Install with: pip install optuna")

    @staticmethod
    def _gate_signals_to_regime(
        signals: pd.Series,
        features: pd.DataFrame,
        regime: str,
    ) -> pd.Series:
        """Fail-closed entry gating for an exact regime specialist.

        A post-hoc trade bucket is not equivalent: trades opened in other
        regimes alter compounding and can occupy the single-position backtester,
        suppressing entries for the target specialist. Every selection surface
        must therefore run the same entry gate used by lockbox/live evaluation.
        """
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

    def _evaluate_frozen_regime_candidate(
        self,
        candidate: Dict[str, Any],
        *,
        symbol: str,
        timeframe: str,
        strategy_name: str,
        regime: str,
        val_features: Optional[pd.DataFrame],
        val_warmup_bars: int,
        evaluate_validation_power_fn: Optional[Callable] = None,
    ) -> int:
        """Evaluate one frozen train winner on tuning exactly once.

        The result is evidence only: it never changes the parameters selected
        inside the train-only study. Technical errors abort the cell; honest
        underpowered evidence remains a typed ineligible decision.
        """
        params = dict(candidate.get("params", {}) or {})
        parameter_hash = _parameter_hash(self._clean_params(params))
        frozen_strategy = candidate.get("strategy")
        try:
            frozen_strategy = self.strategy_registry.get(strategy_name, **params)
        except Exception as exc:
            outcome = classify_search_exception(
                exc,
                stage=SearchStage.FROZEN_VALIDATION,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
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
                    strategy_family=strategy_name,
                    parameter_hash=parameter_hash,
                    exception_type=outcome.exception_type,
                )
            raise SearchInfrastructureError(outcome) from exc
        candidate["strategy"] = frozen_strategy
        if _scored_rows(val_features, val_warmup_bars) < 50:
            raise SearchInfrastructureError(TrialOutcome(
                kind=SearchOutcomeKind.INFRA_FAILURE,
                stage=SearchStage.FROZEN_VALIDATION,
                reason_code="TUNING_VALIDATION_SURFACE_UNAVAILABLE",
                reason="frozen tuning-validation surface is missing or too short",
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
                parameter_hash=parameter_hash,
            ))
        if evaluate_validation_power_fn is None:
            raise SearchInfrastructureError(TrialOutcome(
                kind=SearchOutcomeKind.INFRA_FAILURE,
                stage=SearchStage.FROZEN_VALIDATION,
                reason_code="TRADE_POWER_VALIDATION_CALLBACK_MISSING",
                reason="trade-power validation callback is missing",
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
                parameter_hash=parameter_hash,
            ))
        evaluations = 1
        try:
            raw_val_signals = frozen_strategy.generate_signals(val_features, symbol)
            val_signals = self._gate_signals_to_regime(
                raw_val_signals, val_features, regime
            )
            val_result = self.backtester.run(
                val_features,
                val_signals,
                symbol,
                frozen_strategy,
                warmup_bars=_bounded_warmup(
                    val_features, val_warmup_bars
                ),
                timeframe=timeframe,
            )
            val_result["_baseline_gated_signals_sha256"] = (
                _signal_vector_sha256(val_signals)
            )
            candidate["_baseline_gated_signals_sha256"] = val_result[
                "_baseline_gated_signals_sha256"
            ]
            val_metrics = dict(val_result or self._empty_val_metrics())
            requirement = candidate.get("_trade_power_requirement")
            power_decision = evaluate_validation_power_fn(
                val_result,
                frozen_requirement=requirement,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
                parameter_hash=parameter_hash,
            )
        except Exception as exc:
            if isinstance(exc, SearchInfrastructureError):
                raise
            outcome = _classify_staged_search_exception(
                exc,
                stage=SearchStage.FROZEN_VALIDATION,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
                parameter_hash=parameter_hash,
            )
            raise SearchInfrastructureError(outcome) from exc
        candidate["tuning_validation_evaluations"] = int(evaluations)
        candidate["tuning_validation_evaluation_error"] = None
        candidate["trade_power_validation"] = power_decision.to_dict()
        val_metrics["effective_trades"] = float(
            power_decision.observed_effective_trades
        )
        candidate["val_regime_metrics"] = {str(regime): val_metrics}
        candidate.pop("_trade_power_requirement", None)
        candidate["search_contract"] = self.REGIME_SEARCH_CONTRACT
        return int(evaluations)

    def _record_regime_search_metadata(
        self,
        *,
        symbol: str,
        timeframe: str,
        strategy_name: str,
        method: str,
        cells: List[Dict[str, Any]],
    ) -> None:
        """Publish exact per-family cell/trial counts for multiplicity control."""
        self.last_regime_search_metadata = {
            "contract": self.REGIME_SEARCH_CONTRACT,
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "strategy_family": str(strategy_name),
            "method": str(method),
            "cell_count": int(len(cells)),
            "search_trials_total": int(sum(
                int(cell.get("search_trials", 0) or 0) for cell in cells
            )),
            "completed_trials_total": int(sum(
                int(cell.get("completed_trials", 0) or 0) for cell in cells
            )),
            "eligible_train_trials_total": int(sum(
                int(cell.get("eligible_train_trials", 0) or 0) for cell in cells
            )),
            "tuning_validation_evaluations": int(sum(
                int(cell.get("tuning_validation_evaluations", 0) or 0)
                for cell in cells
            )),
            "cells": [dict(cell) for cell in cells],
        }
    
    def optimize(self,
                 symbol: str,
                 timeframe: str,
                 strategy_name: str,
                 param_grid: Dict[str, List],
                 train_features: pd.DataFrame,
                 val_features: Optional[pd.DataFrame],
                 scoring_fn: Callable[[Dict, Dict], float],
                 min_trades: int = 10,
                 max_drawdown_pct: float = 30.0,
                 train_warmup_bars: int = 0,
                 val_warmup_bars: int = 0) -> OptimizationResult:
        """
        Optimize strategy parameters using Optuna TPE with early rejection.
        
        Args:
            symbol: Trading symbol
            strategy_name: Name of strategy to optimize
            param_grid: Parameter grid from strategy
            train_features: Training data
            val_features: Validation data (can be None)
            scoring_fn: Function(train_metrics, val_metrics) -> score
            min_trades: Minimum trades required for valid evaluation
            max_drawdown_pct: Maximum drawdown threshold for early rejection
            
        Returns:
            OptimizationResult with best parameters and metrics
        """
        if not OPTUNA_AVAILABLE:
            return self._fallback_random_search(
                symbol, strategy_name, param_grid, train_features, 
                val_features, scoring_fn, min_trades, max_drawdown_pct,
                timeframe=timeframe,
                train_warmup_bars=train_warmup_bars,
                val_warmup_bars=val_warmup_bars,
            )
        
        start_time = time.time()
        
        # Get default params
        try:
            base_strategy = self.strategy_registry.get(strategy_name)
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
        
        # Handle empty param grid
        if not param_grid:
            return self._evaluate_default_only(
                symbol, strategy_name, default_params,
                train_features, val_features, scoring_fn,
                timeframe=timeframe,
                train_warmup_bars=train_warmup_bars,
                val_warmup_bars=val_warmup_bars,
            )
        
        # Create parameter space
        try:
            param_space = ParameterSpace(param_grid, default_params)
            search_space_size = param_space.get_search_space_size()
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

        # D4 — emit one-time WARN if cartesian > threshold (observability only)
        try:
            base_strategy.warn_if_param_grid_large()
        except Exception as exc:
            logger.debug("Param-grid size warning hook failed for %s: %s", strategy_name, exc)

        # Resolve effective trial budget — flat by default, family-size-aware when enabled (D3)
        try:
            n_trials, budget_mode = self.config.resolve_trial_budget(search_space_size)
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

        if budget_mode == "family_aware":
            coverage = (n_trials / search_space_size) if search_space_size else 1.0
            logger.info(
                f"[{symbol}] Optuna TPE optimization for {strategy_name}: "
                f"search_space={search_space_size}, trials={n_trials} "
                f"(family-aware budget: {coverage:.1%} coverage, "
                f"min={self.config.min_trials_per_strategy}, max={self.config.max_trials_per_strategy})"
            )
        else:
            logger.info(f"[{symbol}] Optuna TPE optimization for {strategy_name}: "
                       f"search_space={search_space_size}, trials={n_trials}")
        
        # Track the best optimizer-train result. Validation is unavailable to
        # the objective and is evaluated only once after parameters are frozen.
        best_result = {
            'params': default_params.copy(),
            'score': float('-inf'),
            'train_metrics': {},
            'val_metrics': {},
        }
        
        # Trial counter and early rejection counter for logging
        trial_count = [0]
        early_rejections = [0]
        
        def objective(trial: optuna.Trial) -> float:
            """Optuna objective function with drawdown-based early rejection."""
            trial_count[0] += 1
            
            # Suggest parameters
            params = param_space.suggest(trial)
            
            try:
                # Create strategy
                strategy = self.strategy_registry.get(strategy_name, **params)
                
                # Evaluate on training data
                signals = strategy.generate_signals(train_features, symbol)

                # C1: Skip backtest entirely if no signals generated
                if not _has_scored_signal(signals, train_warmup_bars):
                    return self.config.penalty_no_trades

                train_metrics = self.backtester.run(
                    train_features, signals, symbol, strategy,
                    timeframe=timeframe,
                    warmup_bars=_bounded_warmup(
                        train_features, train_warmup_bars
                    ),
                )

                # EARLY REJECTION 1: Minimum trades
                if train_metrics.get('total_trades', 0) < min_trades:
                    return self.config.penalty_no_trades

                # Progressive rejection: relax the train DD threshold in early trials.
                progress = trial_count[0] / max(n_trials, 1)
                if self.config.progressive_rejection and progress < self.config.progressive_warmup_frac:
                    dd_train_mult = self.config.train_dd_multiplier * self.config.progressive_warmup_multiplier
                else:
                    dd_train_mult = self.config.train_dd_multiplier

                # EARLY REJECTION 2: Training drawdown
                train_dd = train_metrics.get('max_drawdown_pct', 100.0)
                if train_dd > max_drawdown_pct * dd_train_mult:
                    early_rejections[0] += 1
                    return self.config.penalty_dd_rejection

                # Parameter choice is optimizer-train only.
                score = float(scoring_fn(train_metrics, {}))
                
                # Update the frozen train winner.
                if score > best_result['score']:
                    best_result['score'] = score
                    best_result['params'] = self._clean_params(params)
                    best_result['train_metrics'] = train_metrics
                    best_result['val_metrics'] = self._empty_val_metrics()
                
                # Log progress
                if trial_count[0] % self.config.log_interval == 0:
                    logger.debug(f"[{symbol}] Trial {trial_count[0]}/{n_trials}: "
                               f"score={score:.4f}, best={best_result['score']:.4f}, "
                               f"dd_rejects={early_rejections[0]}")
                
                return score
                
            except Exception as e:
                logger.debug(f"[{symbol}] Trial {trial_count[0]} failed: {e}")
                return -1000.0
        
        # Create sampler with deterministic context-specific seed.
        sampler_seed = _stable_seed(
            self.config.seed,
            symbol,
            strategy_name,
            "optuna_single",
            len(train_features),
            str(train_features.index[0]) if len(train_features) else "",
            str(train_features.index[-1]) if len(train_features) else "",
        )
        sampler = TPESampler(
            seed=sampler_seed,
            n_startup_trials=min(self.config.n_startup_trials, n_trials // 2),
            multivariate=self.config.multivariate,
        )
        
        # Create study
        study = optuna.create_study(
            direction='maximize',
            sampler=sampler,
        )
        
        # Run optimization
        study.optimize(
            objective,
            n_trials=n_trials,
            show_progress_bar=False,
            catch=(Exception,),
        )
        
        # Compile statistics
        elapsed = time.time() - start_time
        stats = self._compile_stats(study, elapsed)
        stats.early_dd_rejections = int(early_rejections[0])

        # One post-freeze validation evaluation; it cannot change best_params.
        frozen_val_metrics = self._empty_val_metrics()
        if (
            best_result['train_metrics']
            and _scored_rows(val_features, val_warmup_bars) >= 50
        ):
            try:
                frozen_strategy = self.strategy_registry.get(
                    strategy_name, **dict(best_result['params'])
                )
                val_signals = frozen_strategy.generate_signals(val_features, symbol)
                frozen_val_metrics = self.backtester.run(
                    val_features,
                    val_signals,
                    symbol,
                    frozen_strategy,
                    timeframe=timeframe,
                    warmup_bars=_bounded_warmup(
                        val_features, val_warmup_bars
                    ),
                )
            except Exception as exc:
                frozen_val_metrics = self._empty_val_metrics()
                frozen_val_metrics['evaluation_error'] = f"{type(exc).__name__}: {exc}"
                logger.debug(
                    "[%s] Frozen single-strategy validation failed for %s: %s",
                    symbol, strategy_name, exc,
                )
        best_result['val_metrics'] = frozen_val_metrics
        
        logger.info(f"[{symbol}] Optimization complete: {stats}")
        
        return OptimizationResult(
            best_params=best_result['params'],
            best_score=best_result['score'],
            train_metrics=best_result['train_metrics'],
            val_metrics=best_result['val_metrics'],
            stats=stats,
        )
    
    def optimize_for_regimes(self,
                             symbol: str,
                             timeframe: str,
                             strategy_name: str,
                             param_grid: Dict[str, List],
                             train_features: pd.DataFrame,
                             val_features: Optional[pd.DataFrame],
                             regimes: List[str],
                             bucket_trades_fn: Callable,
                             compute_score_fn: Callable,
                             min_train_trades: int = 25,
                             max_drawdown_pct: float = 25.0,
                             val_warmup_bars: int = 0,
                             train_warmup_bars: int = 0,
                              calibrate_train_power_fn: Optional[Callable] = None,
                              evaluate_validation_power_fn: Optional[Callable] = None,
                              train_economic_gate_fn: Optional[Callable] = None,
                              prior_effective_trials_by_regime: Optional[Mapping[str, int]] = None) -> Dict[str, Dict[str, Any]]:
        """Run independent train-only studies for exact regime-family cells.

        Tuning-validation rows are never generated or backtested inside a
        trial. Each regime gets its own sampler/study/objective. Only after the
        train-best parameters are frozen is that one candidate evaluated once
        on tuning validation.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe being optimized
            strategy_name: Strategy name
            param_grid: Parameter grid
            train_features: Training data with REGIME_LIVE column
            val_features: Validation data
            regimes: List of regime names
            bucket_trades_fn: Deprecated compatibility argument; not called.
            compute_score_fn: Train-only score function called as ``fn(train, {})``.
            min_train_trades: Minimum trades per regime
            max_drawdown_pct: Maximum allowed drawdown (early rejection threshold)
            
        Returns:
            Dict mapping regime -> best candidate dict
        """
        if not OPTUNA_AVAILABLE:
            return self._fallback_regime_search(
                symbol, timeframe, strategy_name, param_grid,
                train_features, val_features, regimes,
                bucket_trades_fn, compute_score_fn, min_train_trades,
                max_drawdown_pct, val_warmup_bars, train_warmup_bars,
                calibrate_train_power_fn, evaluate_validation_power_fn,
                train_economic_gate_fn, prior_effective_trials_by_regime,
            )
        
        self.last_regime_search_metadata = {}
        start_time = time.time()
        try:
            base_strategy = self.strategy_registry.get(strategy_name)
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
        
        if not param_grid:
            return {}
        if (
            calibrate_train_power_fn is None
            or evaluate_validation_power_fn is None
            or train_economic_gate_fn is None
        ):
            raise SearchInfrastructureError(TrialOutcome(
                kind=SearchOutcomeKind.INFRA_FAILURE,
                stage=SearchStage.FAMILY_CELL,
                reason_code="TRADE_POWER_CALLBACK_MISSING",
                reason="production regime search requires train and validation power callbacks",
                symbol=symbol,
                timeframe=timeframe,
                strategy_family=strategy_name,
            ))
        
        # Create parameter space
        try:
            param_space = ParameterSpace(param_grid, default_params)
            search_space_size = param_space.get_search_space_size()
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

        # D4 — emit one-time WARN if cartesian > threshold (observability only)
        try:
            base_strategy.warn_if_param_grid_large()
        except Exception as exc:
            logger.debug("Param-grid size warning hook failed for %s: %s", strategy_name, exc)

        # Resolve effective trial budget — flat by default, family-size-aware when enabled (D3)
        try:
            n_trials, budget_mode = self.config.resolve_trial_budget(search_space_size)
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
        if budget_mode == "family_aware":
            coverage = (n_trials / search_space_size) if search_space_size else 1.0
            logger.info(
                f"[{symbol}] Optuna independent-cell optimization for {strategy_name}: "
                f"search_space={search_space_size}, trials={n_trials} "
                f"(family-aware budget: {coverage:.1%} coverage, "
                f"min={self.config.min_trials_per_strategy}, max={self.config.max_trials_per_strategy})"
            )

        results: Dict[str, Dict[str, Any]] = {}
        cell_records: List[Dict[str, Any]] = []
        for regime in [str(value) for value in regimes]:
            cell_start = time.time()
            best_score = [float("-inf")]
            best_effective_trades = [float("-inf")]
            best_parameter_hash = [""]
            best_candidate: List[Optional[Dict[str, Any]]] = [None]
            trial_count = [0]
            eligible_train_trials = [0]
            early_rejections = [0]
            trial_outcomes: List[TrialOutcome] = []

            def objective(trial: optuna.Trial, target_regime: str = regime) -> float:
                """One exact-regime, optimizer-train-only objective."""
                trial_count[0] += 1
                params: Dict[str, Any] = {}
                clean_params: Dict[str, Any] = {}
                parameter_hash = ""
                current_stage = SearchStage.PARAMETER_VALIDATION
                try:
                    params = param_space.suggest(trial)
                    clean_params = self._clean_params(params)
                    parameter_hash = _parameter_hash(clean_params)
                    current_stage = SearchStage.STRATEGY_CONSTRUCTION
                    strategy = self.strategy_registry.get(strategy_name, **params)
                    current_stage = SearchStage.SIGNAL_GENERATION
                    raw_train_signals = strategy.generate_signals(train_features, symbol)
                    train_signals = self._gate_signals_to_regime(
                        raw_train_signals, train_features, target_regime
                    )
                    current_stage = SearchStage.TRAIN_BACKTEST
                    train_result = self.backtester.run(
                        train_features,
                        train_signals,
                        symbol,
                        strategy,
                        timeframe=timeframe,
                        warmup_bars=_bounded_warmup(
                            train_features, train_warmup_bars
                        ),
                    )
                    current_stage = SearchStage.TRAIN_EVIDENCE
                    train_m = dict(train_result or {})
                    power_decision = calibrate_train_power_fn(
                        train_result,
                        effective_trials=(
                            n_trials
                            + max(
                                0,
                                int((prior_effective_trials_by_regime or {}).get(
                                    target_regime, 0
                                ) or 0),
                            )
                        ),
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=target_regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    )
                    if power_decision.outcome_kind is not SearchOutcomeKind.VALID:
                        outcome = TrialOutcome(
                            kind=power_decision.outcome_kind,
                            stage=SearchStage.TRAIN_EVIDENCE,
                            reason_code=(
                                str(power_decision.reason_codes[0])
                                if power_decision.reason_codes
                                else str(power_decision.status)
                            ),
                            reason=str(power_decision.reason),
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=target_regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                            evidence={
                                "observed_effective_trades": power_decision.observed_effective_trades,
                                "required_effective_trades": power_decision.required_effective_trades,
                            },
                        )
                        trial_outcomes.append(outcome)
                        trial.set_user_attr("search_outcome", outcome.to_dict())
                        return float(self.config.penalty_no_trades)
                    train_m["effective_trades"] = float(
                        power_decision.observed_effective_trades
                    )
                    current_stage = SearchStage.TRAIN_SCORE
                    economic_reason = train_economic_gate_fn(train_m)
                    if economic_reason:
                        if economic_reason == "TRAIN_DRAWDOWN_GATE":
                            early_rejections[0] += 1
                        outcome = TrialOutcome(
                            kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
                            stage=SearchStage.TRAIN_SCORE,
                            reason_code=str(economic_reason),
                            reason="trial failed a predeclared train economic gate",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=target_regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        )
                        trial_outcomes.append(outcome)
                        trial.set_user_attr("search_outcome", outcome.to_dict())
                        return float(self.config.penalty_dd_rejection)
                    score = float(compute_score_fn(train_m, {}))
                    if not math.isfinite(score):
                        raise SearchInfrastructureError(TrialOutcome(
                            kind=SearchOutcomeKind.INFRA_FAILURE,
                            stage=SearchStage.TRAIN_SCORE,
                            reason_code="NONFINITE_TRAIN_SCORE",
                            reason="train score is non-finite",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=target_regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        ))
                    eligible_train_trials[0] += 1
                    valid_outcome = TrialOutcome(
                        kind=SearchOutcomeKind.VALID,
                        stage=SearchStage.TRAIN_EVIDENCE,
                        reason_code="POWER_PASS",
                        reason="strict post-cost effective-trade power gate passed",
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=target_regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                        evidence={
                            "observed_effective_trades": power_decision.observed_effective_trades,
                            "required_effective_trades": power_decision.required_effective_trades,
                        },
                    )
                    trial_outcomes.append(valid_outcome)
                    trial.set_user_attr("search_outcome", valid_outcome.to_dict())
                    trial.set_user_attr("clean_params", clean_params)
                    trial.set_user_attr("regime_scores", {target_regime: score})
                    effective_trades = float(
                        power_decision.observed_effective_trades
                    )
                    if (
                        score > best_score[0]
                        or (
                            score == best_score[0]
                            and (
                                effective_trades > best_effective_trades[0]
                                or (
                                    effective_trades == best_effective_trades[0]
                                    and (
                                        not best_parameter_hash[0]
                                        or parameter_hash < best_parameter_hash[0]
                                    )
                                )
                            )
                        )
                    ):
                        best_score[0] = score
                        best_effective_trades[0] = effective_trades
                        best_parameter_hash[0] = parameter_hash
                        best_candidate[0] = {
                            "strategy": strategy,
                            "strategy_name": strategy_name,
                            "params": clean_params,
                            "train_result": train_result,
                            "train_regime_metrics": {target_regime: train_m},
                            "val_regime_metrics": {target_regime: {}},
                            "regime_gated": True,
                            "evaluation_regime": target_regime,
                            "is_tuned": True,
                            "trade_power_contract_enforced": True,
                            "trade_power_train": power_decision.to_dict(),
                            "_trade_power_requirement": (
                                power_decision.frozen_requirement
                            ),
                        }
                    return score
                except Exception as exc:
                    if isinstance(exc, SearchInfrastructureError):
                        raise
                    outcome = _classify_staged_search_exception(
                        exc,
                        stage=current_stage,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=target_regime,
                        strategy_family=strategy_name,
                        parameter_hash=parameter_hash,
                    )
                    trial_outcomes.append(outcome)
                    trial.set_user_attr("search_outcome", outcome.to_dict())
                    if outcome.kind is SearchOutcomeKind.INVALID_PARAM:
                        return float(self.config.penalty_no_trades)
                    raise SearchInfrastructureError(outcome) from exc

            sampler_seed = _stable_seed(
                self.config.seed,
                symbol,
                timeframe,
                regime,
                strategy_name,
                "optuna_regime_train_only",
                len(train_features),
                str(train_features.index[0]) if len(train_features) else "",
                str(train_features.index[-1]) if len(train_features) else "",
            )
            sampler = TPESampler(
                seed=sampler_seed,
                n_startup_trials=min(self.config.n_startup_trials, n_trials // 2),
                multivariate=self.config.multivariate,
            )
            study = optuna.create_study(direction="maximize", sampler=sampler)
            study.optimize(
                objective,
                n_trials=n_trials,
                show_progress_bar=False,
                catch=(),
            )
            family_outcome = aggregate_family_cell_outcomes(
                trial_outcomes,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_family=strategy_name,
            )
            if family_outcome.kind is SearchOutcomeKind.INFRA_FAILURE:
                raise SearchInfrastructureError(family_outcome)
            stats = self._compile_stats(study, time.time() - cell_start)
            stats.early_dd_rejections = early_rejections[0]
            actual_trials = int(len(getattr(study, "trials", []) or []))
            validation_evaluations = 0
            candidate = best_candidate[0]
            if candidate is not None:
                candidate["search_trials"] = actual_trials
                candidate["search_completed_trials"] = int(stats.n_completed)
                candidate["eligible_train_trials"] = int(eligible_train_trials[0])
                candidate["search_cell_count"] = 1
                candidate["parameter_stability_evidence"] = (
                    self._build_parameter_stability_evidence(
                        study,
                        regime,
                        candidate.get("params", {}),
                        float(best_score[0]),
                        param_space,
                    )
                )
                validation_evaluations = self._evaluate_frozen_regime_candidate(
                    candidate,
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy_name=strategy_name,
                    regime=regime,
                    val_features=val_features,
                    val_warmup_bars=val_warmup_bars,
                    evaluate_validation_power_fn=evaluate_validation_power_fn,
                )
                results[regime] = candidate
            cell_records.append({
                "regime": regime,
                "strategy_family": str(strategy_name),
                "search_trials": actual_trials,
                "completed_trials": int(stats.n_completed),
                "eligible_train_trials": int(eligible_train_trials[0]),
                "early_dd_rejections": int(early_rejections[0]),
                "frozen_candidate": bool(candidate is not None),
                "tuning_validation_evaluations": int(validation_evaluations),
                "trade_power_validation_status": str(
                    ((candidate or {}).get("trade_power_validation") or {}).get(
                        "status", "NOT_EVALUATED"
                    )
                ),
                "trade_power_validation_kind": str(
                    ((candidate or {}).get("trade_power_validation") or {}).get(
                        "outcome_kind", "MISSING"
                    )
                ),
                "trade_power_validation_reason_code": str(
                    next(
                        iter(
                            ((candidate or {}).get("trade_power_validation") or {}).get(
                                "reason_codes", []
                            )
                        ),
                        (
                            "POWER_PASS"
                            if ((candidate or {}).get("trade_power_validation") or {}).get(
                                "passed"
                            ) is True
                            else "NOT_EVALUATED"
                        ),
                    )
                ),
                "best_train_score": (
                    float(best_score[0]) if math.isfinite(best_score[0]) else None
                ),
                "family_outcome": family_outcome.to_dict(),
            })
            logger.debug(
                "[%s] [%s] [%s] %s independent train-only study: %s",
                symbol, timeframe, regime, strategy_name, stats,
            )

        self._record_regime_search_metadata(
            symbol=symbol,
            timeframe=timeframe,
            strategy_name=strategy_name,
            method="optuna_tpe",
            cells=cell_records,
        )
        logger.debug(
            "[%s] [%s] %s independent-cell tuning complete in %.1fs: %s",
            symbol,
            timeframe,
            strategy_name,
            time.time() - start_time,
            self.last_regime_search_metadata,
        )
        return results

    @staticmethod
    def _param_distance(params_a: Dict[str, Any],
                        params_b: Dict[str, Any],
                        param_space: ParameterSpace) -> float:
        distances: List[float] = []
        for name, info in param_space.param_info.items():
            choices = list(info.get("choices", []))
            if not choices:
                continue
            a = params_a.get(name)
            b = params_b.get(name)
            if a == b:
                distances.append(0.0)
                continue
            try:
                ia = choices.index(a)
                ib = choices.index(b)
                distances.append(abs(float(ia - ib)) / max(1.0, float(len(choices) - 1)))
            except Exception:
                distances.append(1.0)
        if not distances:
            return 0.0
        return float(np.sqrt(np.mean(np.asarray(distances, dtype=np.float64) ** 2)))

    @staticmethod
    def _local_neighbor_radius(param_space: ParameterSpace) -> float:
        """One-grid-step radius in the normalized parameter-distance space."""

        active_dims = 0
        one_step_widths: List[float] = []
        for info in param_space.param_info.values():
            choices = list(info.get("choices", []))
            if len(choices) < 2:
                continue
            active_dims += 1
            one_step_widths.append(1.0 / max(1.0, float(len(choices) - 1)))
        if active_dims <= 0 or not one_step_widths:
            return 0.0
        # A single one-step move in the widest normalized dimension, with a tiny
        # tolerance for floating arithmetic. Multi-step/multi-param moves remain
        # diagnostic, but not plateau evidence.
        return float(max(one_step_widths) / math.sqrt(float(active_dims)) * 1.05)

    def _build_parameter_stability_evidence(self,
                                            study: 'optuna.Study',
                                            regime: str,
                                            best_params: Dict[str, Any],
                                            best_score: float,
                                            param_space: ParameterSpace) -> Dict[str, Any]:
        """Compact Optuna landscape evidence for plateau-vs-needle analysis.

        This is raw discovery evidence. The scorer decides later whether it predicts
        forward survival; we do not turn it into a production score here.
        """
        if not OPTUNA_AVAILABLE or study is None:
            return {"status": "not_available", "reason": "optuna_unavailable"}
        rows: List[Dict[str, Any]] = []
        for trial in getattr(study, "trials", []):
            if getattr(trial, "state", None) != TrialState.COMPLETE:
                continue
            attrs = getattr(trial, "user_attrs", {}) or {}
            scores = attrs.get("regime_scores") or {}
            if regime not in scores:
                continue
            params = attrs.get("clean_params") or getattr(trial, "params", {}) or {}
            try:
                score = float(scores.get(regime))
            except Exception:
                continue
            if not math.isfinite(score):
                continue
            rows.append({
                "score": score,
                "distance": self._param_distance(best_params, params, param_space),
            })
        if not rows:
            return {
                "status": "insufficient_trials",
                "reason": "no_completed_trials_for_regime",
                "regime": regime,
            }
        scores = np.asarray([row["score"] for row in rows], dtype=np.float64)
        distances = np.asarray([row["distance"] for row in rows], dtype=np.float64)
        finite_best = float(best_score) if math.isfinite(float(best_score)) else float(np.max(scores))
        denom = abs(finite_best) if abs(finite_best) > 1e-9 else 1.0
        nearest = sorted(rows, key=lambda item: (float(item["distance"]), -float(item["score"])))
        nearest_non_exact = [row for row in nearest if float(row["distance"]) > 1e-12]
        local_radius = self._local_neighbor_radius(param_space)
        local_neighbors = [
            row for row in nearest_non_exact
            if local_radius > 0.0 and float(row["distance"]) <= local_radius
        ]
        min_local_neighbors = 3
        sample_source = local_neighbors if len(local_neighbors) >= min_local_neighbors else nearest_non_exact
        sample = sample_source[: min(12, len(sample_source))]
        sample_scores = np.asarray([row["score"] for row in sample], dtype=np.float64) if sample else np.asarray([])
        status = "available" if len(local_neighbors) >= min_local_neighbors else "insufficient_neighbors"
        return {
            "status": status,
            "method": "optuna_nearest_neighbor_scores_v1",
            "regime": regime,
            "completed_regime_trials": int(len(rows)),
            "best_score": round(finite_best, 6),
            "score_percentile": round(float((scores <= finite_best).mean()), 6),
            "exact_param_trial_count": int((distances <= 1e-12).sum()),
            "local_neighbor_radius": round(float(local_radius), 6),
            "local_neighbor_count": int(len(local_neighbors)),
            "min_local_neighbors": int(min_local_neighbors),
            "nearest_neighbor_count": int(len(sample)),
            "nearest_neighbor_mean_distance": round(float(np.mean([row["distance"] for row in sample])), 6)
            if sample else None,
            "nearest_neighbor_median_score_ratio": round(float(np.median(sample_scores) / denom), 6)
            if sample_scores.size else None,
            "nearest_neighbor_min_score_ratio": round(float(np.min(sample_scores) / denom), 6)
            if sample_scores.size else None,
            "nearest_neighbors": [
                {
                    "distance": round(float(row["distance"]), 6),
                    "score": round(float(row["score"]), 6),
                    "score_ratio": round(float(row["score"]) / denom, 6),
                }
                for row in sample
            ],
        }
    
    def _evaluate_default_only(self,
                               symbol: str,
                               strategy_name: str,
                               default_params: Dict[str, Any],
                               train_features: pd.DataFrame,
                               val_features: Optional[pd.DataFrame],
                               scoring_fn: Callable,
                               timeframe: str,
                               train_warmup_bars: int = 0,
                               val_warmup_bars: int = 0) -> OptimizationResult:
        """Evaluate strategy with default parameters only."""
        start_time = time.time()
        
        strategy = self.strategy_registry.get(strategy_name, **default_params)
        
        signals = strategy.generate_signals(train_features, symbol)
        train_metrics = self.backtester.run(
            train_features, signals, symbol, strategy,
            timeframe=timeframe,
            warmup_bars=_bounded_warmup(train_features, train_warmup_bars),
        )
        
        # The only parameter set is frozen after its train evaluation. Score it
        # on train only, then attach exactly one validation evaluation.
        score = float(scoring_fn(train_metrics, {}))
        if _scored_rows(val_features, val_warmup_bars) >= 50:
            val_signals = strategy.generate_signals(val_features, symbol)
            val_metrics = self.backtester.run(
                val_features, val_signals, symbol, strategy,
                timeframe=timeframe,
                warmup_bars=_bounded_warmup(val_features, val_warmup_bars),
            )
        else:
            val_metrics = self._empty_val_metrics()
        
        stats = OptimizationStats(
            n_trials=1,
            n_completed=1,
            best_score=score,
            optimization_time_sec=time.time() - start_time,
            method="default_params",
        )
        
        return OptimizationResult(
            best_params=self._clean_params(default_params),
            best_score=score,
            train_metrics=train_metrics,
            val_metrics=val_metrics,
            stats=stats,
        )
    
    def _fallback_random_search(self,
                                symbol: str,
                                strategy_name: str,
                                param_grid: Dict[str, List],
                                train_features: pd.DataFrame,
                                val_features: Optional[pd.DataFrame],
                                scoring_fn: Callable,
                                min_trades: int,
                                max_drawdown_pct: float = 30.0,
                                timeframe: str = "",
                                train_warmup_bars: int = 0,
                                val_warmup_bars: int = 0) -> OptimizationResult:
        """Fallback to bounded deterministic search when Optuna is unavailable."""
        
        start_time = time.time()
        
        try:
            base_strategy = self.strategy_registry.get(strategy_name)
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
        
        if not param_grid:
            return self._evaluate_default_only(
                symbol, strategy_name, default_params,
                train_features, val_features, scoring_fn,
                timeframe=timeframe,
                train_warmup_bars=train_warmup_bars,
                val_warmup_bars=val_warmup_bars,
            )
        
        # Decode only the configured number of deterministic Cartesian points.
        param_names = list(param_grid.keys())
        param_values = [list(param_grid[name] or []) for name in param_names]
        _seed = _stable_seed(
            symbol,
            strategy_name,
            "grid_fallback_train_only",
            len(train_features),
        )
        combos, _total_space = _bounded_cartesian_combinations(
            param_values,
            limit=self.config.n_trials,
            seed=_seed,
        )
        n_trials = len(combos)

        best_score = float('-inf')
        best_params = default_params.copy()
        best_train_metrics = {}
        best_val_metrics = self._empty_val_metrics()
        completed = 0
        early_rejections = 0
        
        for idx, combo in enumerate(combos):
            params = {**default_params, **dict(zip(param_names, combo))}

            # Progressive rejection for random fallback
            progress = (idx + 1) / max(len(combos), 1)
            if self.config.progressive_rejection and progress < self.config.progressive_warmup_frac:
                dd_train_mult = self.config.train_dd_multiplier * self.config.progressive_warmup_multiplier
            else:
                dd_train_mult = self.config.train_dd_multiplier

            try:
                strategy = self.strategy_registry.get(strategy_name, **params)

                signals = strategy.generate_signals(train_features, symbol)
                train_metrics = self.backtester.run(
                    train_features, signals, symbol, strategy,
                    timeframe=timeframe,
                    warmup_bars=_bounded_warmup(
                        train_features, train_warmup_bars
                    ),
                )

                # EARLY REJECTION 1: Minimum trades
                if train_metrics.get('total_trades', 0) < min_trades:
                    continue

                # EARLY REJECTION 2: Training drawdown
                train_dd = train_metrics.get('max_drawdown_pct', 100.0)
                if train_dd > max_drawdown_pct * dd_train_mult:
                    early_rejections += 1
                    continue

                score = float(scoring_fn(train_metrics, {}))
                completed += 1
                
                if score > best_score:
                    best_score = score
                    best_params = self._clean_params(params)
                    best_train_metrics = train_metrics
                    
            except Exception:
                continue
        
        if early_rejections > 0:
            logger.debug(f"[{symbol}] Random search: {early_rejections} early DD rejections")
        
        elapsed = time.time() - start_time

        # Evaluate only the frozen train winner on validation.
        if (
            best_train_metrics
            and _scored_rows(val_features, val_warmup_bars) >= 50
        ):
            try:
                frozen_strategy = self.strategy_registry.get(
                    strategy_name, **dict(best_params)
                )
                val_signals = frozen_strategy.generate_signals(val_features, symbol)
                best_val_metrics = self.backtester.run(
                    val_features,
                    val_signals,
                    symbol,
                    frozen_strategy,
                    timeframe=timeframe,
                    warmup_bars=_bounded_warmup(
                        val_features, val_warmup_bars
                    ),
                )
            except Exception as exc:
                best_val_metrics = self._empty_val_metrics()
                best_val_metrics['evaluation_error'] = f"{type(exc).__name__}: {exc}"
        
        stats = OptimizationStats(
            n_trials=len(combos),
            n_completed=completed,
            n_failed=len(combos) - completed,
            best_score=best_score,
            optimization_time_sec=elapsed,
            method="random_search_fallback",
        )
        
        logger.info(f"[{symbol}] Random search fallback: {stats}")
        
        return OptimizationResult(
            best_params=best_params,
            best_score=best_score,
            train_metrics=best_train_metrics,
            val_metrics=best_val_metrics,
            stats=stats,
        )
    
    def _fallback_regime_search(self,
                                symbol: str,
                                timeframe: str,
                                strategy_name: str,
                                param_grid: Dict[str, List],
                                train_features: pd.DataFrame,
                                val_features: Optional[pd.DataFrame],
                                regimes: List[str],
                                bucket_trades_fn: Callable,
                                compute_score_fn: Callable,
                                min_train_trades: int,
                                max_drawdown_pct: float = 25.0,
                                val_warmup_bars: int = 0,
                                train_warmup_bars: int = 0,
                                calibrate_train_power_fn: Optional[Callable] = None,
                                evaluate_validation_power_fn: Optional[Callable] = None,
                                train_economic_gate_fn: Optional[Callable] = None,
                                prior_effective_trials_by_regime: Optional[Mapping[str, int]] = None) -> Dict[str, Dict[str, Any]]:
        """Independent train-only grid cells, each followed by one frozen tuning eval."""
        
        try:
            base_strategy = self.strategy_registry.get(strategy_name)
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
        
        if not param_grid:
            return {}
        if (
            calibrate_train_power_fn is None
            or evaluate_validation_power_fn is None
            or train_economic_gate_fn is None
        ):
            raise SearchInfrastructureError(TrialOutcome(
                kind=SearchOutcomeKind.INFRA_FAILURE,
                stage=SearchStage.FAMILY_CELL,
                reason_code="TRADE_POWER_CALLBACK_MISSING",
                reason="production fallback search requires trade-power callbacks",
                symbol=symbol,
                timeframe=timeframe,
                strategy_family=strategy_name,
            ))
        
        try:
            param_names = list(param_grid.keys())
            param_values = [list(param_grid[name] or []) for name in param_names]
            total_space = int(math.prod(len(values) for values in param_values))
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
        
        self.last_regime_search_metadata = {}
        n_trials = min(self.config.n_trials, total_space)
        results: Dict[str, Dict[str, Any]] = {}
        cell_records: List[Dict[str, Any]] = []
        for regime in [str(value) for value in regimes]:
            seed = _stable_seed(
                symbol,
                timeframe,
                regime,
                strategy_name,
                "regime_grid_train_only",
                len(train_features),
            )
            combos, _ = _bounded_cartesian_combinations(
                param_values,
                limit=n_trials,
                seed=seed,
            )
            best_score = float("-inf")
            best_effective_trades = float("-inf")
            best_parameter_hash = ""
            best_candidate: Optional[Dict[str, Any]] = None
            completed_trials = 0
            eligible_train_trials = 0
            early_rejections = 0
            trial_outcomes: List[TrialOutcome] = []
            for idx, combo in enumerate(combos):
                params: Dict[str, Any] = {}
                clean_params: Dict[str, Any] = {}
                parameter_hash = ""
                current_stage = SearchStage.PARAMETER_VALIDATION
                try:
                    params = {**default_params, **dict(zip(param_names, combo))}
                    clean_params = self._clean_params(params)
                    parameter_hash = _parameter_hash(clean_params)
                    current_stage = SearchStage.STRATEGY_CONSTRUCTION
                    strategy = self.strategy_registry.get(strategy_name, **params)
                    current_stage = SearchStage.SIGNAL_GENERATION
                    raw_train_signals = strategy.generate_signals(train_features, symbol)
                    train_signals = self._gate_signals_to_regime(
                        raw_train_signals, train_features, regime
                    )
                    current_stage = SearchStage.TRAIN_BACKTEST
                    train_result = self.backtester.run(
                        train_features,
                        train_signals,
                        symbol,
                        strategy,
                        timeframe=timeframe,
                        warmup_bars=_bounded_warmup(
                            train_features, train_warmup_bars
                        ),
                    )
                    completed_trials += 1
                    current_stage = SearchStage.TRAIN_EVIDENCE
                    train_m = dict(train_result or {})
                    power_decision = calibrate_train_power_fn(
                        train_result,
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
                    if power_decision.outcome_kind is not SearchOutcomeKind.VALID:
                        trial_outcomes.append(TrialOutcome(
                            kind=power_decision.outcome_kind,
                            stage=SearchStage.TRAIN_EVIDENCE,
                            reason_code=(
                                str(power_decision.reason_codes[0])
                                if power_decision.reason_codes else str(power_decision.status)
                            ),
                            reason=str(power_decision.reason),
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
                    economic_reason = train_economic_gate_fn(train_m)
                    if economic_reason:
                        if economic_reason == "TRAIN_DRAWDOWN_GATE":
                            early_rejections += 1
                        trial_outcomes.append(TrialOutcome(
                            kind=SearchOutcomeKind.EXPECTED_INELIGIBLE,
                            stage=SearchStage.TRAIN_SCORE,
                            reason_code=str(economic_reason),
                            reason="trial failed a predeclared train economic gate",
                            symbol=symbol,
                            timeframe=timeframe,
                            regime=regime,
                            strategy_family=strategy_name,
                            parameter_hash=parameter_hash,
                        ))
                        continue
                    score = float(compute_score_fn(train_m, {}))
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
                    trial_outcomes.append(TrialOutcome(
                        kind=SearchOutcomeKind.VALID,
                        stage=SearchStage.TRAIN_EVIDENCE,
                        reason_code="POWER_PASS",
                        reason="strict post-cost effective-trade power gate passed",
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
                            "strategy": strategy,
                            "strategy_name": strategy_name,
                            "params": self._clean_params(params),
                            "train_result": train_result,
                            "train_regime_metrics": {regime: train_m},
                            "val_regime_metrics": {regime: {}},
                            "regime_gated": True,
                            "evaluation_regime": regime,
                            "is_tuned": True,
                            "trade_power_contract_enforced": True,
                            "trade_power_train": power_decision.to_dict(),
                            "_trade_power_requirement": power_decision.frozen_requirement,
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
            validation_evaluations = 0
            if best_candidate is not None:
                best_candidate["search_trials"] = int(len(combos))
                best_candidate["search_completed_trials"] = int(completed_trials)
                best_candidate["eligible_train_trials"] = int(eligible_train_trials)
                best_candidate["search_cell_count"] = 1
                validation_evaluations = self._evaluate_frozen_regime_candidate(
                    best_candidate,
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy_name=strategy_name,
                    regime=regime,
                    val_features=val_features,
                    val_warmup_bars=val_warmup_bars,
                    evaluate_validation_power_fn=evaluate_validation_power_fn,
                )
                results[regime] = best_candidate
            cell_records.append({
                "regime": regime,
                "strategy_family": str(strategy_name),
                "search_trials": int(len(combos)),
                "completed_trials": int(completed_trials),
                "eligible_train_trials": int(eligible_train_trials),
                "early_dd_rejections": int(early_rejections),
                "frozen_candidate": bool(best_candidate is not None),
                "tuning_validation_evaluations": int(validation_evaluations),
                "trade_power_validation_status": str(
                    ((best_candidate or {}).get("trade_power_validation") or {}).get(
                        "status", "NOT_EVALUATED"
                    )
                ),
                "trade_power_validation_kind": str(
                    ((best_candidate or {}).get("trade_power_validation") or {}).get(
                        "outcome_kind", "MISSING"
                    )
                ),
                "trade_power_validation_reason_code": str(
                    next(
                        iter(
                            ((best_candidate or {}).get("trade_power_validation") or {}).get(
                                "reason_codes", []
                            )
                        ),
                        (
                            "POWER_PASS"
                            if ((best_candidate or {}).get("trade_power_validation") or {}).get(
                                "passed"
                            ) is True
                            else "NOT_EVALUATED"
                        ),
                    )
                ),
                "best_train_score": (
                    float(best_score) if math.isfinite(best_score) else None
                ),
                "family_outcome": family_outcome.to_dict(),
            })
        self._record_regime_search_metadata(
            symbol=symbol,
            timeframe=timeframe,
            strategy_name=strategy_name,
            method="deterministic_grid_fallback",
            cells=cell_records,
        )
        return results
    
    def _compile_stats(self, study: 'optuna.Study', elapsed: float) -> OptimizationStats:
        """Compile optimization statistics from Optuna study."""
        trials = study.trials
        
        n_completed = len([t for t in trials if t.state == TrialState.COMPLETE])
        n_pruned = len([t for t in trials if t.state == TrialState.PRUNED])
        n_failed = len([t for t in trials if t.state == TrialState.FAIL])
        
        best_score = study.best_value if study.best_trial else float('-inf')
        
        return OptimizationStats(
            n_trials=len(trials),
            n_completed=n_completed,
            n_pruned=n_pruned,
            n_failed=n_failed,
            best_score=best_score,
            optimization_time_sec=elapsed,
            method="optuna_tpe",
        )
    
    def _clean_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Clean parameters for JSON serialization."""
        clean = {}
        for k, v in params.items():
            if hasattr(v, 'item'):
                v = v.item()
            elif isinstance(v, (np.integer, np.floating)):
                v = float(v) if isinstance(v, np.floating) else int(v)
            elif isinstance(v, np.ndarray):
                v = v.tolist()
            clean[k] = v
        return clean
    
    def _empty_val_metrics(self) -> Dict[str, Any]:
        """Return empty validation metrics."""
        return {
            'total_trades': 0,
            'total_return_pct': 0.0,
            'sharpe_ratio': 0.0,
            'max_drawdown_pct': 0.0,
            'profit_factor': 0.0,
            'win_rate': 0.0,
        }


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def is_optuna_available() -> bool:
    """Check if Optuna is available."""
    return OPTUNA_AVAILABLE


def get_optimization_method() -> str:
    """Get current optimization method string."""
    if OPTUNA_AVAILABLE:
        return "Optuna TPE"
    return "Random Search (fallback)"


def create_optimizer(config, backtester, scorer, strategy_registry) -> OptunaTPEOptimizer:
    """
    Factory function to create optimizer with proper configuration.
    
    Args:
        config: PipelineConfig
        backtester: Backtester instance
        scorer: StrategyScorer instance
        strategy_registry: StrategyRegistry class
        
    Returns:
        Configured OptunaTPEOptimizer
    """
    optuna_config = OptunaConfig.from_pipeline_config(config)
    return OptunaTPEOptimizer(optuna_config, backtester, scorer, strategy_registry)


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'OptunaConfig',
    'OptunaTPEOptimizer',
    'OptimizationResult',
    'OptimizationStats',
    'ParameterSpace',
    'is_optuna_available',
    'get_optimization_method',
    'create_optimizer',
    'OPTUNA_AVAILABLE',
]
