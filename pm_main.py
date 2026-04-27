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
    python pm_main.py --trade --paper         # Paper trading mode
    python pm_main.py --status                # Show current status

Version: 3.1 (Portfolio Manager with Stateful Optimization)
"""

import argparse
import hashlib
import json
import logging
import math
import os
import signal
import sys
import time
import threading
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field, fields

import pandas as pd
import numpy as np

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
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise RuntimeError(f"Failed to load config file '{path}': {e}")

def log_resolved_config_summary(logger: logging.Logger,
                               config_path: str,
                               config_data: Dict[str, Any],
                               pipeline_config: "PipelineConfig",
                               position_config: "PositionConfig",
                               mt5_config: "MT5Config") -> None:
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
            f"scoring={getattr(p, 'scoring_mode', None)} | "
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
    load_broker_specs
)
from pm_strategies import StrategyRegistry, BaseStrategy, TradeIntent, set_regime_tp_multipliers
from pm_position import PositionConfig, PositionCalculator
from pm_pipeline import PortfolioManager, SymbolConfig
from pm_enhancement_seams import (
    create_default_enhancement_seams,
    RiskScalarContext,
    ExecutionQualityContext,
    PortfolioObservationContext,
)
from pm_order_governance import GovernanceContext, evaluate_policy, make_policy, policy_name_from_artifact
from pm_storage import StorageManager
from pm_mt5 import (
    MT5_AVAILABLE,
    MT5Config,
    MT5Connector,
    MT5Position,
    OrderType
)


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

def setup_logging(log_dir: str = "logs",
                  log_level: str = "INFO",
                  console_level: str = "INFO") -> logging.Logger:
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
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_format = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
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
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    # Crosses
    "AUDNZD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "EURAUD",
    "EURCHF", "EURCAD", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF",
    "CADJPY", "NZDJPY",
    # Added Crosses / Minors
    "AUDCAD", "AUDCHF", "CADCHF", "CHFJPY", "NZDCAD", "NZDCHF", "GBPNZD",
    # Exotics (and USD minors)
    "USDNOK", "USDMXN", "USDSGD", "USDZAR", "USDPLN", "USDSEK",
    # Added Exotic / Minor
    "EURZAR", "GBPZAR", "USDCNH", "EURPLN",
    # Added Nordic + selected replacements
    "EURNOK", "EURSEK", "EURCNH", "GBPNOK", "GBPSEK", "EURTRY",
    "Platinum", "Palladium", "EURMXN", "NOKJPY",
    # Commodities (Metals + Energy)
    "XAUUSD", "XAGUSD", "XAUEUR", "XAUGBP", "XAUAUD", "XAGEUR", "XRX", "XTIUSD", "XBRUSD", "XNGUSD",
    # Indices
    "US100", "US30", "DE30", "EU50", "UK100", "JP225",
    "US500", "FR40", "ES35", "HK50", "AU200",
    # Crypto (CFDs)
    "BTCUSD", "ETHUSD", "LTCUSD", "SOLUSD", "BCHUSD",
    "DOGUSD", "TRXUSD", "XRPUSD", "TONUSD", "BTCETH", "BTCXAU",
]


# =============================================================================
# Lot-normalization risk gate
# =============================================================================

def classify_lot_normalization_drift(actual_risk_pct: float,
                                     target_risk_pct: float,
                                     max_risk_pct: float,
                                     tolerance_pct: float) -> str:
    """Classify the severity of lot-normalization risk drift.

    Returns one of:
      * "ok"    — actual within tolerance of target
      * "warn"  — actual above tolerance band, still ≤ hard cap
      * "block" — actual above per-trade hard cap

    Inputs are all percentages of basis (balance/equity). `tolerance_pct` is
    interpreted as a percentage of `target_risk_pct` (e.g. tolerance=10.0 →
    target × 1.10 is the warn boundary). Negative tolerance is clamped to 0.
    """
    eps = 1e-9
    if actual_risk_pct > max_risk_pct + eps:
        return "block"
    drift_threshold = target_risk_pct * (1.0 + max(0.0, tolerance_pct) / 100.0)
    if actual_risk_pct > drift_threshold + eps:
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
    decision_key: str          # hash that uniquely identifies the decision
    bar_time: str              # ISO-8601 of the bar timestamp used for the signal
    timeframe: str
    regime: str
    strategy_name: str
    direction: int             # 1 = LONG, -1 = SHORT, 0 = no signal
    action: str                # EXECUTED, PAPER, SKIPPED_RISK_CAP, SKIPPED_MIN_VOLUME,
                               # SKIPPED_SPREAD, SKIPPED_NO_SIGNAL, FAILED, …
    action_time: str           # ISO-8601 wall-clock time
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

    def __init__(self, log_path: str = "last_trade_log.json",
                 max_age_hours: float = 24.0):
        if str(log_path) == "last_trade_log.json":
            log_path = os.environ.get("PM_DECISION_THROTTLE_LOG_PATH", log_path)
        self._log_path = log_path
        # symbol → DecisionRecord
        self._cache: Dict[str, DecisionRecord] = {}
        self._max_age_hours = float(max_age_hours) if max_age_hours and max_age_hours > 0 else 0.0
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def make_decision_key(symbol: str, strategy_name: str, timeframe: str,
                          regime: str, direction: int, bar_time_iso: str) -> str:
        """
        Create a deterministic, short hash that uniquely identifies a decision.

        Components: symbol, strategy, timeframe, regime, direction, bar_time.
        """
        raw = f"{symbol}|{strategy_name}|{timeframe}|{regime}|{direction}|{bar_time_iso}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def should_suppress(self, symbol: str, decision_key: str,
                        bar_time_iso: str) -> bool:
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

    def record_decision(self, symbol: str, decision_key: str,
                        bar_time_iso: str, timeframe: str, regime: str,
                        strategy_name: str, direction: int, action: str,
                        cooldown_seconds: int = 0,
                        context: Optional[Dict[str, Any]] = None) -> None:
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

    def __init__(self, logger: logging.Logger,
                 win_rate_threshold: float = 15.0,
                 r_mult_threshold: float = 0.5,
                 dd_threshold: float = 10.0):
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

    def record_trade(self,
                     symbol: str,
                     pnl_dollars: float,
                     pnl_pips: Optional[float] = None,
                     r_multiple: Optional[float] = None):
        """Record a completed trade for drift tracking."""
        if symbol not in self._trades:
            self._trades[symbol] = []
        self._trades[symbol].append({
            'pnl_dollars': pnl_dollars,
            'pnl_pips': pnl_pips,
            'r_multiple': r_multiple,
            'time': datetime.now(),
        })

    def check_drift(self, symbol: str, val_metrics: Dict[str, Any]):
        """Compare live performance against validation metrics. Log warnings."""
        trades = self._trades.get(symbol, [])
        if len(trades) < 5:
            return  # Not enough data to judge

        wins = sum(1 for t in trades if t.get('pnl_dollars', 0.0) > 0)
        live_wr = (wins / len(trades)) * 100
        live_r_values = [t['r_multiple'] for t in trades if t.get('r_multiple') is not None]
        live_avg_r = np.mean(live_r_values) if live_r_values else None

        val_wr = val_metrics.get('win_rate', 50.0)
        val_avg_r = val_metrics.get('mean_r', val_metrics.get('avg_r_multiple', 1.0))

        if abs(live_wr - val_wr) > self.win_rate_threshold:
            self.logger.warning(
                f"[DRIFT] [{symbol}] Win rate drift: live={live_wr:.1f}% vs "
                f"backtest={val_wr:.1f}% (threshold={self.win_rate_threshold}%)"
            )
        if live_avg_r is not None and abs(live_avg_r - val_avg_r) > self.r_mult_threshold:
            self.logger.warning(
                f"[DRIFT] [{symbol}] R-multiple drift: live={live_avg_r:.2f} vs "
                f"backtest={val_avg_r:.2f} (threshold={self.r_mult_threshold})"
            )

    def get_summary(self, symbol: str) -> Dict[str, Any]:
        """Get drift monitoring summary for a symbol."""
        trades = self._trades.get(symbol, [])
        if not trades:
            return {'trade_count': 0}
        wins = sum(1 for t in trades if t.get('pnl_dollars', 0.0) > 0)
        live_r_values = [t['r_multiple'] for t in trades if t.get('r_multiple') is not None]
        return {
            'trade_count': len(trades),
            'live_win_rate': (wins / len(trades)) * 100,
            'live_avg_r': np.mean(live_r_values) if live_r_values else None,
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
    
    def __init__(self,
                 mt5_connector: MT5Connector,
                 portfolio_manager: PortfolioManager,
                 position_config: "PositionConfig",
                 enable_trading: bool = True,
                 close_on_opposite_signal: bool = False,
                 pipeline_config: 'PipelineConfig' = None,
                 storage_manager: Optional[StorageManager] = None):
        """
        Initialize live trader.
        
        Args:
            mt5_connector: MT5 connection
            portfolio_manager: Portfolio manager with configs
            position_config: Position management configuration
            enable_trading: Enable actual trade execution
            close_on_opposite_signal: Close positions on opposite signal (default: False)
            pipeline_config: Pipeline configuration (for regime params, etc.)
        """
        self.mt5 = mt5_connector
        self.pm = portfolio_manager
        self.position_config = position_config
        self.enable_trading = enable_trading
        self.close_on_opposite_signal = close_on_opposite_signal
        self.pipeline_config = pipeline_config
        self.storage_manager = storage_manager
        
        self.position_calc = PositionCalculator(position_config)
        
        # State
        self._running = False
        self._shutdown_event = threading.Event()
        self._last_bar_times: Dict[str, datetime] = {}
        self._last_order_times: Dict[str, datetime] = {}  # For rate limiting
        self._unknown_tf_position_warnings: Set[int] = set()
        self._tf_recovery_cache: Dict[int, Optional[str]] = {}
        self._margin_state: str = "NORMAL"
        self._last_margin_close_ts: Optional[datetime] = None
        self._margin_cooldown_notice_logged_ts: Optional[datetime] = None
        
        # Decision throttle – prevents re-evaluation / re-logging of the
        # same decision on the same bar (e.g. risk-cap skips for XAUUSD).
        self._decision_throttle = DecisionThrottle(log_path="last_trade_log.json")
        self._actionable_log = ActionableDecisionLog(log_path="last_actionable_log.json")
        
        # Feature/signal cache – avoids recomputing features and signals 
        # when no new bar has arrived. Cache key: (symbol, timeframe, bar_time)
        # Cache stores: {'features': df, 'signal': int, 'regime': str, 'regime_strength': float}
        self._candidate_cache: Dict[str, Dict[str, Any]] = {}
        
        # Trade log
        self.trade_log: List[Dict] = []
        self.drift_monitor = DriftMonitor(logging.getLogger(__name__))
        self._seen_closing_deals: Set[int] = set()
        
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
        self._last_governance_bar_times: Dict[str, pd.Timestamp] = {}
        self._daily_advisory_state: Dict[str, str] = {}
        self._last_portfolio_observation: Dict[str, Any] = {}
        self._order_governance_state: Dict[int, Dict[str, Any]] = {}

        self.logger = logging.getLogger(__name__)

        # Enhancement seams (risk scalars, spread filter, exit pack)
        self._enhancement_seams = create_default_enhancement_seams(self.pipeline_config)

        # === MT5 SPEC SYNCHRONIZATION ===
        # Update InstrumentSpec with live MT5 values for accurate position sizing
        if self.mt5 and self.mt5.is_connected():
            self.logger.info("Synchronizing instrument specs from MT5...")

            sync_count = 0
            fail_count = 0

            for symbol in self.pm.symbols:
                broker_symbol = self.mt5.find_broker_symbol(symbol)

                if not broker_symbol:
                    self.logger.warning(f"[{symbol}] Broker symbol not found, using config values")
                    fail_count += 1
                    continue

                mt5_info = self.mt5.get_symbol_info(broker_symbol)

                if not mt5_info:
                    self.logger.warning(f"[{symbol}] MT5 info not available, using config values")
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
            keys_to_remove = list(self._candidate_cache.keys())[:len(self._candidate_cache) // 2]
            for k in keys_to_remove:
                del self._candidate_cache[k]
            self.logger.debug(f"Pruned feature cache: removed {len(keys_to_remove)} entries")

    def invalidate_runtime_caches(self) -> None:
        """Clear runtime caches that should not survive reconnect/retrain boundaries."""
        self._candidate_cache.clear()
        self._last_bar_times.clear()
        self._last_bar_probe_times.clear()
        self._pending_bar_probe_times.clear()
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
        """Path for the bounded live cache used during trading sweeps."""
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

    def _record_latest_bar_time(self, timeframe: str, bars: Optional[pd.DataFrame]) -> None:
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

    def _get_symbol_timeframe_due_at(self,
                                     symbol: str,
                                     timeframe: str,
                                     *,
                                     now: Optional[datetime] = None) -> datetime:
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
        settle_seconds = max(0, int(getattr(self.pipeline_config, "live_bar_settle_seconds", 5) or 5))
        tf_minutes = int(DataLoader.TIMEFRAME_MINUTES.get(tf, 5))
        return last_bar_ts.to_pydatetime() + timedelta(minutes=tf_minutes, seconds=settle_seconds)

    def _is_symbol_timeframe_due(self,
                                 symbol: str,
                                 timeframe: str,
                                 *,
                                 now: Optional[datetime] = None) -> bool:
        probe = now or datetime.now()
        return probe >= self._get_symbol_timeframe_due_at(symbol, timeframe, now=probe)

    def _load_cached_live_bars(self,
                               symbol: str,
                               timeframe: str,
                               count: int,
                               min_required: int = 0) -> Optional[pd.DataFrame]:
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
            self.logger.debug(f"[{symbol}] Live cache load failed for {timeframe}: {exc}")
            return None
        if bars is None or len(bars) == 0:
            return None
        return bars.copy()

    def _load_recent_canonical_m5(self,
                                  symbol: str,
                                  *,
                                  row_count: int) -> Optional[pd.DataFrame]:
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

    def _load_direct_live_bars_from_mt5(self,
                                        broker_symbol: str,
                                        timeframe: str,
                                        *,
                                        required_bars: int) -> Optional[pd.DataFrame]:
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
            sync_floor = max(1, int(getattr(self.pipeline_config, "storage_live_sync_bars", count) or count))
            count = max(count, sync_floor)
        bars = self.mt5.get_bars(broker_symbol, tf, count=count)
        if not isinstance(bars, pd.DataFrame) or len(bars) == 0:
            return None
        if len(bars) > required_bars:
            bars = bars.tail(required_bars)
        return bars.copy()

    def _get_recent_m5_window(self,
                              symbol: str,
                              broker_symbol: str,
                              *,
                              timeframe: str,
                              min_target_bars: int = 2,
                              min_source_rows: int = 0) -> Optional[pd.DataFrame]:
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
            required_rows = loader.estimate_source_rows(timeframe, target_bars, base_timeframe="M5")
        except Exception:
            required_rows = target_bars
        overlap_rows = max(0, int(getattr(self.pipeline_config, "storage_live_sync_overlap_bars", 100) or 100))
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

    def _sync_live_timeframe_cache(self,
                                   symbol: str,
                                   broker_symbol: str,
                                   timeframe: str,
                                   *,
                                   required_bars: int) -> Optional[pd.DataFrame]:
        """Refresh a timeframe-specific live cache from canonical seed plus recent M5 delta."""
        if not bool(getattr(self.pipeline_config, "storage_local_data_first_enabled", True)):
            return None
        loader = self._get_pipeline_data_loader()
        if loader is None:
            return None
        tf = str(timeframe or "M5").upper()
        required_bars = max(1, int(required_bars or 0))
        cache_path = self._get_live_cache_path(symbol, tf)
        existing_cache = self._load_cached_live_bars(symbol, tf, count=required_bars, min_required=0)

        try:
            required_source_rows = loader.estimate_source_rows(tf, required_bars, base_timeframe="M5")
        except Exception:
            required_source_rows = required_bars

        overlap_rows = max(0, int(getattr(self.pipeline_config, "storage_live_sync_overlap_bars", 100) or 100))
        canonical_m5 = self._load_recent_canonical_m5(symbol, row_count=required_source_rows)

        use_direct_reseed = False
        if self.mt5 and self.mt5.is_connected():
            if canonical_m5 is None or len(canonical_m5) == 0:
                use_direct_reseed = True
            else:
                try:
                    last_local = pd.Timestamp(canonical_m5.index[-1]).to_pydatetime()
                    allowed_gap = timedelta(
                        minutes=max(1, overlap_rows) * int(DataLoader.TIMEFRAME_MINUTES.get("M5", 5))
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
                    source_m5 = source_m5[~source_m5.index.duplicated(keep="last")].sort_index()
                    if len(source_m5) > required_source_rows:
                        source_m5 = source_m5.tail(required_source_rows)
            if source_m5 is not None and len(source_m5) > 0:
                if tf == "M5":
                    combined = source_m5.copy()
                else:
                    try:
                        combined = loader.resample(source_m5, tf)
                    except Exception as exc:
                        self.logger.debug(f"[{symbol}] Live delta resample failed for {tf}: {exc}")
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

    def _load_local_bars(self,
                         symbol: str,
                         broker_symbol: str,
                         timeframe: str,
                         count: int,
                         min_required: int) -> Optional[pd.DataFrame]:
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
            bars = self._load_cached_live_bars(symbol, tf, count=count, min_required=min_required)
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
        Prefer PM-local canonical data for live evaluation, with MT5 fallback.
        """
        use_local = bool(getattr(self.pipeline_config, "storage_local_data_first_enabled", True))
        local_bars: Optional[pd.DataFrame] = None
        if use_local:
            local_bars = self._load_local_bars(symbol, broker_symbol, timeframe, count, min_required)
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
        if isinstance(bars, pd.DataFrame) and len(bars) >= max(int(min_required or 0), 1):
            self._record_latest_bar_time(timeframe, bars)
            return bars
        if local_bars is not None and len(local_bars) >= min_required:
            self._record_latest_bar_time(timeframe, local_bars)
            return local_bars
        self._record_latest_bar_time(timeframe, bars)
        return bars

    def _get_live_configs(self, *, log_rejections: bool = False) -> Dict[str, SymbolConfig]:
        """Return live-eligible configs, with a legacy fallback for test doubles."""
        if not self.pm:
            return {}
        if hasattr(self.pm, "get_live_eligible_configs"):
            try:
                configs = self.pm.get_live_eligible_configs(log_rejections=log_rejections)
            except TypeError:
                configs = self.pm.get_live_eligible_configs()
            if isinstance(configs, dict):
                return configs
        if hasattr(self.pm, "get_validated_configs"):
            configs = self.pm.get_validated_configs()
            return configs if isinstance(configs, dict) else {}
        return {}

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
        raw_validated = self._get_raw_validated_configs()
        if raw_validated:
            return raw_validated
        return self._get_live_configs()

    def _uses_scheduled_trigger(self) -> bool:
        """True when live evaluations should use the legacy wall-clock due-time fallback."""
        mode = str(getattr(self.pipeline_config, "live_loop_trigger_mode", "bar") or "bar").strip().lower()
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
            self.logger.debug(f"[{symbol}] Broker symbol resolution failed during live bar probe: {exc}")
        return broker_symbol

    def _probe_latest_bar_time(self, broker_symbol: str, timeframe: str) -> Optional[pd.Timestamp]:
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
            self.logger.debug(f"[{broker_symbol}] Live bar probe failed for {tf}: {exc}")
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

    def get_next_sweep_due(self,
                           active_timeframes: List[str],
                           *,
                           now: Optional[datetime] = None) -> datetime:
        """Return the next due-time for a live sweep based on symbol/timeframe state."""
        probe = now or datetime.now()
        settle_seconds = max(0, int(getattr(self.pipeline_config, "live_bar_settle_seconds", 5) or 5))
        stale_retry_seconds = max(1, int(getattr(self.pipeline_config, "live_stale_retry_seconds", 15) or 15))

        due_candidates: List[datetime] = []

        validated = self._get_live_configs()
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
                    latest_bar.to_pydatetime() +
                    timedelta(minutes=tf_minutes, seconds=settle_seconds)
                )

        if not due_candidates:
            return probe + timedelta(seconds=stale_retry_seconds)

        next_due = min(due_candidates)
        if next_due <= probe:
            return probe + timedelta(seconds=stale_retry_seconds)
        return next_due

    def process_all_symbols(self,
                            positions_snapshot: Optional[List[Any]] = None,
                            account_info: Optional[Any] = None,
                            symbols_filter: Optional[Set[str]] = None,
                            timeframes_filter: Optional[Dict[str, Set[str]]] = None) -> bool:
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
            self.logger.warning("Live position snapshot unavailable; skipping this iteration.")
            return False

        if account_info is None:
            account_info = self.mt5.get_account_info()
        if account_info is None:
            self.logger.warning("Live account snapshot unavailable; skipping this iteration.")
            return False
        live_equity = 0.0
        try:
            live_equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            if live_equity > 0:
                self._equity_peak = max(self._equity_peak, live_equity)
        except Exception:
            pass
        if not getattr(account_info, "trade_allowed", True) or not getattr(account_info, "trade_expert", True):
            self.logger.warning("Live account is not tradable; skipping this iteration.")
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
                        live_equity = float(getattr(account_info, "equity", live_equity) or live_equity)
                    except Exception:
                        pass
            except Exception as exc:
                self.logger.warning(f"Margin protection refresh failed; skipping this iteration: {exc}")
                return False
        if getattr(self, "_margin_state", "NORMAL") in {"RECOVERY", "PANIC"}:
            self.logger.warning(
                f"Margin protection remains in {self._margin_state}; skipping new entries this iteration."
            )
            return False
        if refreshed_after_margin:
            self.logger.debug("Refreshed live snapshots after margin protection cycle.")

        recent_deals: List[Dict[str, Any]] = []
        if not runtime_only:
            try:
                recent_deals = list(self.mt5.get_recent_closing_deals() or [])
            except Exception as exc:
                self.logger.debug(f"Recent closing deals unavailable: {exc}")

        self._run_order_governance_cycle(active_configs, positions_snapshot)

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

        if not runtime_only:
            self._sync_drift_monitor(active_configs, recent_deals=recent_deals)
            self._evaluate_daily_loss_advisory(account_info, recent_deals)
            self._run_portfolio_observatory(active_configs, positions_snapshot, account_info)
        sweep_duration = time.perf_counter() - sweep_started
        open_positions = len(positions_snapshot) if isinstance(positions_snapshot, (list, tuple)) else 0
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
                    [int(getattr(pos, "ticket", 0) or 0) for pos in (positions_snapshot or [])]
                )
            except Exception as exc:
                self.logger.debug(f"Storage sweep hook failed: {exc}")
        return True

    def _sync_drift_monitor(self,
                            validated_configs: Optional[Dict[str, SymbolConfig]] = None,
                            recent_deals: Optional[List[Dict[str, Any]]] = None) -> None:
        """Pull recent closing deals into DriftMonitor and compare against validation metrics."""
        validated = validated_configs or self._get_live_configs()
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
        if not deals:
            return
        if not isinstance(deals, (list, tuple)):
            return

        from pm_position import TradeTagEncoder

        for deal in deals:
            ticket = int(deal.get('ticket', 0) or 0)
            if ticket <= 0 or ticket in self._seen_closing_deals:
                continue
            self._seen_closing_deals.add(ticket)

            comment = deal.get('comment', '') or ''
            metadata = TradeTagEncoder.decode_comment(comment) if comment else None
            symbol = metadata.get('symbol') if metadata else None

            if not symbol:
                broker_symbol = deal.get('symbol', '')
                for candidate in validated.keys():
                    if self.mt5.find_broker_symbol(candidate) == broker_symbol:
                        symbol = candidate
                        break

            if not symbol or symbol not in validated:
                continue

            symbol_config = validated[symbol]
            timeframe = metadata.get('timeframe') if metadata else ""
            magic = int(deal.get('magic', 0) or 0)
            magic_to_tf, magic_to_regime = self._build_magic_lookup(symbol, symbol_config)
            if not timeframe:
                timeframe = magic_to_tf.get(magic, "")
            regime = magic_to_regime.get(magic, "LEGACY" if not symbol_config.has_regime_configs() else "")
            timeframe, regime, regime_cfg = self._resolve_exact_regime_config(
                symbol,
                symbol_config,
                timeframe,
                regime,
            )

            self.drift_monitor.record_trade(
                symbol=symbol,
                pnl_dollars=float(deal.get('profit', 0.0) or 0.0),
                r_multiple=None,
            )
            drift_summary = dict(self.drift_monitor.get_summary(symbol))
            drift_summary["updated_at"] = datetime.now().isoformat()
            if regime_cfg is not None:
                existing_badge = dict(getattr(regime_cfg, "live_observability", {}) or {})
                consecutive_losses = int(existing_badge.get("recent_consecutive_losses", 0) or 0)
                if float(deal.get("profit", 0.0) or 0.0) < 0.0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
                self.pm.update_live_observability(
                    symbol,
                    timeframe,
                    regime,
                    {
                        "last_seen_drift": drift_summary,
                        "recent_consecutive_losses": consecutive_losses,
                    },
                    save=True,
                )
            self.drift_monitor.check_drift(
                symbol,
                getattr(regime_cfg, "val_metrics", None) or validated[symbol].val_metrics,
            )

    def _resolve_symbol_from_broker(self,
                                    broker_symbol: str,
                                    validated_configs: Dict[str, SymbolConfig]) -> Optional[str]:
        for candidate in validated_configs.keys():
            if self.mt5.find_broker_symbol(candidate) == broker_symbol:
                return candidate
        return None

    def _resolve_exact_regime_config(self,
                                     symbol: str,
                                     config: SymbolConfig,
                                     timeframe: Optional[str],
                                     regime: Optional[str]) -> Tuple[str, str, Optional[Any]]:
        tf = str(timeframe or "").upper()
        rg = str(regime or "").upper()
        if config.has_regime_configs():
            if tf and rg:
                return tf, rg, config.get_regime_config(tf, rg)
            return tf, rg, None
        if config.default_config is not None:
            return tf or str(config.timeframe or "").upper(), rg or "LEGACY", config.default_config
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

    def _run_order_governance_cycle(self,
                                    validated_configs: Dict[str, SymbolConfig],
                                    positions_snapshot: Optional[List[Any]]) -> None:
        raw_mode = getattr(self.pipeline_config, "local_governance_live_mode", "off")
        mode = str(raw_mode or "off").strip().lower() if isinstance(raw_mode, str) else "off"
        if mode == "off" or not positions_snapshot:
            return
        regime_params_file = getattr(self.pipeline_config, "regime_params_file", "regime_params.json")
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
            pos_regime = magic_to_regime.get(magic, "LEGACY" if not config.has_regime_configs() else "")
            pos_timeframe, pos_regime, regime_cfg = self._resolve_exact_regime_config(
                symbol,
                config,
                pos_timeframe,
                pos_regime,
            )
            if regime_cfg is None:
                continue
            raw_policy_payload = getattr(regime_cfg, "governance_policy", {}) or {}
            policy_payload = dict(raw_policy_payload) if isinstance(raw_policy_payload, dict) else {}
            policy_name = policy_name_from_artifact(policy_payload)
            if policy_name == "control_fixed":
                continue
            bars = self._get_live_bars(
                symbol,
                broker_symbol,
                pos_timeframe,
                count=int(getattr(self.pipeline_config, "live_bars_count", 200)),
                min_required=max(50, int(getattr(self.pipeline_config, "live_min_bars", 100) or 100)),
            )
            if bars is None or len(bars) < 3:
                continue
            current_bar_time = pd.Timestamp(bars.index[-1])
            cache_key = f"{ticket_id}:{symbol}:{pos_timeframe}:{pos_regime}"
            last_applied_bar = self._last_governance_bar_times.get(cache_key)
            if last_applied_bar is not None and current_bar_time <= pd.Timestamp(last_applied_bar):
                continue
            try:
                features = FeatureComputer.compute_all(
                    bars,
                    symbol=symbol,
                    timeframe=pos_timeframe,
                    regime_params_file=regime_params_file,
                )
            except Exception as exc:
                self.logger.debug(f"[{symbol}] Governance feature build failed for {pos_timeframe}: {exc}")
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
            start_idx = max(0, int(closed_features.index.searchsorted(entry_time, side="left")))
            since_entry = closed_features.iloc[start_idx:]
            if since_entry.empty:
                continue
            symbol_info = self.mt5.get_symbol_info(broker_symbol)
            spec = self._symbol_info_to_instrument_spec(symbol, symbol_info)
            min_stop_distance = 0.0
            if symbol_info is not None:
                point = float(getattr(symbol_info, "point", 0.0) or 0.0)
                stop_distance = float(getattr(symbol_info, "trade_stops_level", 0) or 0) * point
                freeze_distance = float(getattr(symbol_info, "trade_freeze_level", 0) or 0) * point
                min_stop_distance = max(stop_distance, freeze_distance)
            elif getattr(spec, "point", 0.0) and getattr(spec, "stops_level", 0):
                min_stop_distance = float(getattr(spec, "stops_level", 0) or 0) * float(getattr(spec, "point", 0.0) or 0.0)
            half_spread = spec.get_half_spread_price() if getattr(self.pipeline_config, "use_spread", True) else 0.0
            prev_close = float(closed_features["Close"].iloc[-1])
            is_long = int(getattr(position, "type", 1)) == 0
            current_price = None
            try:
                tick = self.mt5.get_symbol_tick(broker_symbol) if hasattr(self.mt5, "get_symbol_tick") else None
            except Exception as exc:
                self.logger.debug(f"[{symbol}] Governance tick unavailable for {broker_symbol}: {exc}")
                tick = None
            if tick is not None and not self._looks_like_mock(tick):
                try:
                    quote_price = float(getattr(tick, "bid", 0.0) if is_long else getattr(tick, "ask", 0.0))
                    if math.isfinite(quote_price) and quote_price > 0.0:
                        current_price = quote_price
                except Exception:
                    current_price = None
            if current_price is None:
                current_price = (
                    prev_close - half_spread if is_long and getattr(self.pipeline_config, "use_spread", True) else (
                        prev_close + half_spread if (not is_long) and getattr(self.pipeline_config, "use_spread", True) else prev_close
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
                }
            shadow_mode = mode == "shadow"
            current_stop_loss = (
                float(state.get("shadow_stop_loss", getattr(position, "sl", 0.0)) or 0.0)
                if shadow_mode else float(getattr(position, "sl", 0.0) or 0.0)
            )
            current_take_profit = (
                float(state.get("shadow_take_profit", getattr(position, "tp", 0.0)) or 0.0)
                if shadow_mode else float(getattr(position, "tp", 0.0) or 0.0)
            )
            tp_released = bool(
                state.get("shadow_tp_released", False)
                if shadow_mode else state.get("tp_released", False)
            )
            context = GovernanceContext(
                symbol=symbol,
                timeframe=pos_timeframe,
                regime=pos_regime,
                direction=1 if is_long else -1,
                entry_price=float(getattr(position, "price_open", 0.0) or 0.0),
                current_stop_loss=current_stop_loss,
                current_take_profit=current_take_profit,
                initial_stop_loss=float(state.get("initial_stop_loss", getattr(position, "sl", 0.0)) or 0.0),
                initial_take_profit=float(state.get("initial_take_profit", getattr(position, "tp", 0.0)) or 0.0),
                current_price=current_price,
                current_atr=float(pd.to_numeric(closed_features["ATR_14"].iloc[-1], errors="coerce") or 0.0),
                highest_since_entry=float(pd.to_numeric(since_entry["High"], errors="coerce").max()),
                lowest_since_entry=float(pd.to_numeric(since_entry["Low"], errors="coerce").min()),
                pip_size=float(getattr(spec, "pip_size", 0.0) or 0.0),
                price_step=float(getattr(symbol_info, "point", 0.0) or getattr(spec, "point", 0.0) or getattr(spec, "pip_size", 0.0) or 0.0),
                min_stop_distance=min_stop_distance,
                tp_released=tp_released,
            )
            decision = evaluate_policy(policy_payload, context)
            self._last_governance_bar_times[cache_key] = current_bar_time
            new_sl = decision.stop_loss
            new_tp = None
            if decision.take_profit is not None:
                new_tp = 0.0 if math.isinf(float(decision.take_profit)) else float(decision.take_profit)
            if mode == "shadow":
                if new_sl is not None or new_tp is not None or decision.tp_released:
                    self.logger.info(
                        f"[{symbol}] Governance SHADOW {policy_name} @ {pos_timeframe}/{pos_regime}: "
                        f"sl={f'{new_sl:.5f}' if new_sl is not None else 'keep'} "
                        f"tp={'release' if new_tp == 0.0 else (f'{new_tp:.5f}' if new_tp is not None else 'keep')}"
                    )
                if new_sl is not None:
                    state["shadow_stop_loss"] = float(new_sl)
                if new_tp is not None:
                    state["shadow_take_profit"] = float(new_tp)
                state["shadow_tp_released"] = bool(state.get("shadow_tp_released", False) or decision.tp_released)
                state["last_policy_name"] = policy_name
                state["last_bar_time"] = current_bar_time.isoformat()
                self._set_order_governance_state(ticket_id, state)
                continue
            if new_sl is None and new_tp is None and not decision.tp_released:
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
                state["tp_released"] = bool(state.get("tp_released", False) or decision.tp_released)
                if new_sl is not None:
                    state["shadow_stop_loss"] = float(new_sl)
                if new_tp is not None:
                    state["shadow_take_profit"] = float(new_tp)
                state["shadow_tp_released"] = bool(state.get("shadow_tp_released", False) or decision.tp_released)
                state["last_policy_name"] = policy_name
                state["last_bar_time"] = current_bar_time.isoformat()
                state["last_applied_at"] = datetime.now().isoformat()
                self._set_order_governance_state(ticket_id, state)
            else:
                self.logger.warning(
                    f"[{symbol}] Governance {policy_name} modify failed @ {pos_timeframe}/{pos_regime}: "
                    f"{getattr(result, 'retcode_description', 'unknown error')}"
                )

    def _run_portfolio_observatory(self,
                                   validated_configs: Dict[str, SymbolConfig],
                                   positions_snapshot: Optional[List[Any]],
                                   account_info: Optional[Any]) -> None:
        observatory = getattr(self._enhancement_seams, "portfolio_observatory", None)
        if observatory is None or not getattr(observatory, "enabled", False):
            return
        estimated_risk_by_symbol: Dict[str, float] = {}
        for position in list(positions_snapshot or []):
            broker_symbol = str(getattr(position, "symbol", "") or "")
            symbol = self._resolve_symbol_from_broker(broker_symbol, validated_configs) or broker_symbol
            risk_pct = self._estimate_position_risk_pct(
                position,
                account_info=account_info,
                canonical_symbol=symbol,
                broker_symbol=broker_symbol,
            )
            if risk_pct is not None:
                estimated_risk_by_symbol[symbol] = estimated_risk_by_symbol.get(symbol, 0.0) + float(risk_pct)
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

    def _evaluate_daily_loss_advisory(self,
                                      account_info: Optional[Any],
                                      recent_deals: Optional[List[Dict[str, Any]]]) -> None:
        def _safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        if account_info is None:
            return
        daily_threshold = _safe_float(getattr(self.pipeline_config, "daily_loss_advisory_pct", 0.0), 0.0)
        session_threshold = _safe_float(getattr(self.pipeline_config, "session_loss_advisory_pct", 0.0), 0.0)
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
        daily_realized = sum(float(deal.get("profit", 0.0) or 0.0) for deal in deals if deal.get("time") and deal["time"] >= day_start)
        cycle_realized = sum(float(deal.get("profit", 0.0) or 0.0) for deal in deals if deal.get("time") and deal["time"] >= cycle_start)
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

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        total = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total * 100) if total > 0 else 0
        return {
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'hit_rate_pct': round(hit_rate, 1),
            'cache_size': len(self._candidate_cache),
        }

    def _build_magic_lookup(self, symbol: str, config: SymbolConfig) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Build magic->timeframe/regime lookup for fast position inference."""
        from pm_position import TradeTagEncoder

        magic_to_tf: Dict[int, str] = {}
        magic_to_regime: Dict[int, str] = {}

        try:
            if config and config.has_regime_configs():
                for tf in config.get_available_timeframes():
                    for regime in config.get_regimes_for_timeframe(tf):
                        magic = TradeTagEncoder.encode_magic(symbol, tf, regime)
                        magic_to_tf[magic] = tf
                        magic_to_regime[magic] = regime
            elif config and config.timeframe:
                magic = TradeTagEncoder.encode_magic(symbol, config.timeframe, "LEGACY")
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

        ticket = getattr(position, 'ticket', 0) or 0
        magic = int(getattr(position, 'magic', 0) or 0)
        comment = getattr(position, 'comment', '') or ''

        # --- Priority 1: Manual override ---
        overrides = {}
        pc = getattr(self, 'pipeline_config', None)
        if pc:
            overrides = getattr(pc, 'position_timeframe_overrides', None) or {}
        if not isinstance(overrides, dict):
            overrides = {}
        if ticket and overrides.get(f"ticket:{ticket}"):
            return overrides[f"ticket:{ticket}"]
        if magic and overrides.get(f"magic:{magic}"):
            return overrides[f"magic:{magic}"]

        # --- Priority 2: Decode live comment ---
        decoded = TradeTagEncoder.decode_comment(comment) if comment else None
        if decoded and decoded.get('timeframe'):
            return decoded['timeframe']

        # --- Priority 3: Legacy strategy tag match ---
        if decoded and decoded.get('strategy_tag') and config:
            tag = decoded['strategy_tag'].lower()
            matched_tfs = set()
            try:
                if hasattr(config, 'regime_configs') and config.regime_configs:
                    for tf, regimes in config.regime_configs.items():
                        if isinstance(regimes, dict):
                            for _regime, rc in regimes.items():
                                sname = getattr(rc, 'strategy_name', '') or ''
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
        cache = getattr(self, '_tf_recovery_cache', None)
        if cache is None:
            self._tf_recovery_cache = {}
            cache = self._tf_recovery_cache
        identifier = getattr(position, 'identifier', 0) or ticket
        if identifier:
            cache_key = int(identifier)
            if cache_key in cache:
                return cache[cache_key]  # May be None (negative cache)
            mt5_conn = getattr(self, 'mt5', None)
            if mt5_conn and hasattr(mt5_conn, 'get_position_opening_metadata'):
                try:
                    meta = mt5_conn.get_position_opening_metadata(cache_key)
                    if meta:
                        # Try decoding the historical comment
                        hist_comment = meta.get('comment', '')
                        hist_decoded = TradeTagEncoder.decode_comment(hist_comment) if hist_comment else None
                        if hist_decoded and hist_decoded.get('timeframe'):
                            cache[cache_key] = hist_decoded['timeframe']
                            return hist_decoded['timeframe']
                        # Try the historical magic against our lookup
                        hist_magic = meta.get('magic', 0)
                        if hist_magic and hist_magic in magic_to_tf:
                            cache[cache_key] = magic_to_tf[hist_magic]
                            return magic_to_tf[hist_magic]
                except Exception:
                    pass
            # Negative cache: don't re-query failed lookups
            cache[cache_key] = None

        # --- Priority 6: Unknown ---
        return None

    def _log_no_actionable_signal(self, symbol: str, message: str,
                                   best_candidate: Dict, bar_time_iso: str,
                                   action_type: str) -> None:
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
            best_candidate.get('strategy_name', 'UNKNOWN'),
            best_candidate.get('timeframe', '?'),
            best_candidate.get('regime', '?'),
            0,  # direction=0 is correct for no-signal
            bar_time_iso
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
            timeframe=best_candidate.get('timeframe', '?'),
            regime=best_candidate.get('regime', '?'),
            strategy_name=best_candidate.get('strategy_name', 'UNKNOWN'),
            direction=0,
            action=action_type,
        )

    def _candidate_rank_key(self, candidate: Dict[str, Any]) -> Tuple[float, float, float, int, str]:
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
        candidate_id = "|".join([
            str(candidate.get("timeframe", "") or "").upper(),
            str(candidate.get("regime", "") or ""),
            str(candidate.get("strategy_name", "") or ""),
        ])
        return (selection_score, quality_score, regime_strength, signal_rank, candidate_id)

    def _commission_amount_for_volume(self,
                                      symbol: str,
                                      volume: float,
                                      *,
                                      spec: Optional[Any] = None) -> float:
        """Estimate one-way commission for the proposed volume."""
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
            commission_per_lot = float(getattr(resolved_spec, "commission_per_lot", 0.0) or 0.0)
        except (TypeError, ValueError):
            commission_per_lot = 0.0
        if commission_per_lot <= 0.0:
            return 0.0
        return commission_per_lot * volume_value

    def _estimate_stop_loss_amount(self,
                                   order_type: int,
                                   symbol: str,
                                   volume: float,
                                   entry_price: float,
                                   stop_price: float,
                                   *,
                                   spec: Optional[Any] = None,
                                   symbol_info: Optional[Any] = None,
                                   include_commission: bool = True) -> Tuple[Optional[float], Optional[str]]:
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
            risk_amount += self._commission_amount_for_volume(symbol, volume_value, spec=spec)
        return float(risk_amount), path

    def _estimate_position_risk_pct(self,
                                    position: Any,
                                    *,
                                    account_info: Optional[Any] = None,
                                    canonical_symbol: str = "",
                                    broker_symbol: str = "",
                                    default_risk_pct: Optional[float] = None) -> Optional[float]:
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
        equity = float(getattr(account, "equity", 0.0) or 0.0) if account is not None else 0.0

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
                spec=(get_instrument_spec(canonical_symbol) if canonical_symbol else None),
                symbol_info=(self.mt5.get_symbol_info(pos_symbol) if self.mt5 and pos_symbol else None),
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
            reason_bits.append(f"geometry_unavailable[{','.join(missing) or 'unknown'}]")
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

    def _check_symbol_combined_risk_cap(self, symbol: str, new_trade_risk_pct: float,
                                        broker_symbol: str,
                                        positions_snapshot: Optional[List[Any]] = None,
                                        account_info: Optional[Any] = None) -> Tuple[bool, str]:
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

        # Get max combined risk from config (defaults to 3.0%)
        max_combined = float(getattr(self.pipeline_config, 'max_combined_risk_pct', 3.0))

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
            if getattr(pos, 'symbol', None) != broker_symbol:
                continue

            comment = getattr(pos, 'comment', '') or ''
            metadata = TradeTagEncoder.decode_comment(comment)
            if metadata and metadata.get('symbol') and metadata.get('symbol') != symbol:
                continue

            pos_risk_pct = self._estimate_position_risk_pct(
                pos,
                account_info=account_info,
                canonical_symbol=symbol,
                broker_symbol=broker_symbol,
                default_risk_pct=float(getattr(self.position_config, 'risk_per_trade_pct', 1.0)),
            )
            if pos_risk_pct is None:
                continue

            pos_tf = metadata.get('timeframe', '?') if metadata else '?'
            pos_direction = metadata.get('direction', 'LONG' if getattr(pos, 'type', 1) == 0 else 'SHORT') if metadata else (
                'LONG' if getattr(pos, 'type', 1) == 0 else 'SHORT'
            )

            existing_risk_pct += pos_risk_pct
            position_details.append({
                'timeframe': pos_tf,
                'direction': pos_direction,
                'risk_pct': pos_risk_pct,
                'ticket': getattr(pos, 'ticket', 0),
            })

            self.logger.debug(
                f"[{symbol}] Open position: {pos_tf} {pos_direction} "
                f"risk={pos_risk_pct:.2f}% (ticket={getattr(pos, 'ticket', 0)})"
            )

        # Check if new trade would breach cap FOR THIS SYMBOL
        total_risk_pct = existing_risk_pct + new_trade_risk_pct

        if total_risk_pct > max_combined:
            details_str = ', '.join(
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

    def _check_portfolio_risk_cap(self, symbol: str, new_trade_risk_pct: float,
                                   broker_symbol: str,
                                   positions_snapshot: Optional[List[Any]] = None,
                                   account_info: Optional[Any] = None) -> Tuple[bool, str]:
        """Backward-compatible alias for the same-symbol combined risk check."""
        return self._check_symbol_combined_risk_cap(
            symbol=symbol,
            new_trade_risk_pct=new_trade_risk_pct,
            broker_symbol=broker_symbol,
            positions_snapshot=positions_snapshot,
            account_info=account_info,
        )

    @staticmethod
    def _safe_finite_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return default
        return result if math.isfinite(result) else default

    @staticmethod
    def _account_margin_used(account_info: Any) -> bool:
        margin = LiveTrader._safe_finite_float(getattr(account_info, "margin", 0.0), default=0.0)
        return bool(margin is not None and margin > 0.0)

    @staticmethod
    def _safe_account_margin_level(account_info: Any) -> Optional[float]:
        return LiveTrader._safe_finite_float(getattr(account_info, "margin_level", None))

    def _classify_margin_state(self, margin_level: float) -> str:
        """Classify account stress state from MT5-native margin level."""
        level = self._safe_finite_float(margin_level)
        if level is None:
            return "BLOCKED"

        block_level = float(getattr(self.pipeline_config, "margin_entry_block_level", 100.0))
        recovery_level = float(getattr(self.pipeline_config, "margin_recovery_start_level", 80.0))
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
        reopen_level = float(getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0)
        if margin_level >= reopen_level:
            self._margin_reopen_required = False

    def _run_margin_protection_cycle(self,
                                     account_info: Optional[Any] = None,
                                     positions_snapshot: Optional[List[Any]] = None) -> bool:
        """Run one margin-protection pass and return True when positions changed."""

        def _set_state(new_state: str, margin_level: Optional[float]) -> None:
            previous = getattr(self, "_margin_state", "NORMAL")
            if previous != new_state:
                level_text = "unavailable" if margin_level is None else f"{margin_level:.1f}%"
                self.logger.warning(
                    f"MARGIN STATE CHANGE: {previous} -> {new_state} (margin_level={level_text})"
                )
            self._margin_state = new_state

        account = account_info if account_info is not None else (self.mt5.get_account_info() if self.mt5 else None)
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
            self._margin_reopen_required = True
        elif self._is_margin_reopen_required():
            reopen_level = float(getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0)
            if margin_level < reopen_level:
                _set_state("BLOCKED", margin_level)
                return False
            self._clear_margin_reopen_if_recovered(margin_level)
        _set_state(state, margin_level)
        if state in {"NORMAL", "BLOCKED"}:
            return False

        positions = positions_snapshot if positions_snapshot is not None else (self.mt5.get_positions() if self.mt5 else None)
        positions = list(positions or [])
        if not positions:
            return False

        recovery_limit = max(1, int(getattr(self.pipeline_config, "margin_recovery_closes_per_cycle", 1)))
        panic_limit = max(1, int(getattr(self.pipeline_config, "margin_panic_closes_per_cycle", 3)))

        if state == "RECOVERY":
            candidates = sorted(
                [pos for pos in positions if float(getattr(pos, "profit", 0.0) or 0.0) < 0.0],
                key=lambda pos: float(getattr(pos, "profit", 0.0) or 0.0),
            )
            max_attempts = recovery_limit
        else:
            losers = sorted(
                [pos for pos in positions if float(getattr(pos, "profit", 0.0) or 0.0) < 0.0],
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
                self._last_margin_close_ts = datetime.now()
                self._margin_reopen_required = True
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
                self._margin_reopen_required = True
            elif self._is_margin_reopen_required():
                reopen_level = float(getattr(self.pipeline_config, "margin_reopen_level", 100.0) or 100.0)
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
    
    def _process_symbol(self, symbol: str, config: SymbolConfig,
                        positions_snapshot: Optional[List[Any]] = None,
                        account_info: Optional[Any] = None,
                        timeframes_filter: Optional[Set[str]] = None):
        """
        Process a single symbol with regime-aware strategy selection.
        
        For each timeframe:
        1. Check for new bar
        2. Compute features (including regime)
        3. Select best (tf, regime, strategy) based on strength * quality_score * freshness
        4. Generate signal and execute if conditions met
        
        D1 + lower-TF pairing rules:
        - If no position exists: allow entry on any timeframe
        - If exactly one D1 position exists: allow one additional non-D1 trade
        - If exactly one non-D1 position exists: block new trades (old behavior)
        - If two positions exist: block new trades
        
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
            self.logger.warning(f"[{symbol}] Position snapshot unavailable; skipping this iteration.")
            return

        if account_info is None:
            account_info = self.mt5.get_account_info()
        if account_info is None:
            self.logger.warning(f"[{symbol}] Account snapshot unavailable; skipping this iteration.")
            return
        if not getattr(account_info, "trade_allowed", True) or not getattr(account_info, "trade_expert", True):
            self.logger.warning(f"[{symbol}] Account is not tradable; skipping this iteration.")
            return

        # Analyze open positions for the D1 + lower-TF pairing rules.
        symbol_positions = [p for p in all_positions if getattr(p, 'symbol', None) == broker_symbol]
        magic_to_tf, magic_to_regime = self._build_magic_lookup(symbol, config)
        open_magics = {int(getattr(p, 'magic', 0) or 0) for p in symbol_positions}
        
        # Check if D1+lower-TF mode is enabled
        allow_d1_plus_lower = getattr(self.pipeline_config, 'allow_d1_plus_lower_tf', True) if self.pipeline_config else True
        
        # Analyze existing positions
        has_d1_position = False
        has_non_d1_position = False
        has_unknown_position = False
        d1_position_direction = None  # Track direction for logging
        open_timeframes = set()
        tag_source_counts = {"comment": 0, "magic": 0, "unknown": 0}
        position_details = []

        for pos in symbol_positions:
            # Unified timeframe inference with full priority chain
            pos_tf = self._infer_position_timeframe(symbol, config, pos)
            pos_regime = ""

            # Determine how the timeframe was resolved (for diagnostics)
            comment = getattr(pos, 'comment', '') or ''
            pos_magic = int(getattr(pos, 'magic', 0) or 0)
            if pos_tf:
                # Classify tag source for diagnostics
                decoded = TradeTagEncoder.decode_comment(comment) if comment else None
                if decoded and decoded.get('timeframe'):
                    tag_source = "comment"
                elif pos_magic in magic_to_tf:
                    tag_source = "magic"
                else:
                    tag_source = "recovered"  # override, legacy tag, or history
            else:
                tag_source = "unknown"
                has_unknown_position = True
                # Throttled warning: log once per ticket per session
                pos_ticket = int(getattr(pos, 'ticket', 0) or 0)
                if pos_ticket and pos_ticket not in self._unknown_tf_position_warnings:
                    self._unknown_tf_position_warnings.add(pos_ticket)
                    self.logger.warning(
                        f"[{symbol}] Position ticket={pos_ticket} magic={pos_magic} "
                        f"comment={comment!r}: timeframe unknown; blocking secondary trade"
                    )

            if pos_tf:
                pos_regime = magic_to_regime.get(pos_magic, "")
                open_timeframes.add(pos_tf)

            if pos_tf == 'D1':
                has_d1_position = True
                d1_position_direction = "LONG" if pos.type == 0 else "SHORT"  # 0=BUY, 1=SELL
            elif pos_tf:
                has_non_d1_position = True
            else:
                # Unknown trade tag - assume it's a non-D1 trade for safety
                has_non_d1_position = True

            tag_source_counts[tag_source] = tag_source_counts.get(tag_source, 0) + 1
            position_details.append({
                "magic": pos_magic,
                "timeframe": pos_tf or "",
                "regime": pos_regime or "",
                "tag_source": tag_source,
                "direction": "LONG" if getattr(pos, 'type', 1) == 0 else "SHORT",
            })
        
        # Determine if we can open a new trade and what constraints apply
        can_open_trade = True
        allowed_timeframes = None  # None = all timeframes allowed
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
        
        if num_positions >= 2:
            # Two trades max per symbol
            can_open_trade = False
            block_reason = "2 positions already open"
            
        elif num_positions == 1:
            if allow_d1_plus_lower and has_unknown_position:
                # Avoid inconsistent state where a trade is selected but then blocked
                # later by magic-level duplicate checks.
                can_open_trade = False
                block_reason = "Position timeframe unknown; blocking secondary trade"
            elif allow_d1_plus_lower and has_d1_position:
                # D1 position open - allow one additional non-D1 trade
                can_open_trade = True
                allowed_timeframes = ['M5', 'M15', 'M30', 'H1', 'H4']  # Exclude D1
                is_secondary_trade = True
                secondary_reason = "d1_plus_lower"
                self.logger.debug(f"[{symbol}] D1 trade open ({d1_position_direction}); allowing secondary non-D1 trade")
            elif allow_d1_plus_lower and has_non_d1_position:
                # Non-D1 position open - allow one additional D1 trade
                can_open_trade = True
                allowed_timeframes = ['D1']
                is_secondary_trade = True
                secondary_reason = "unknown_existing_position" if has_unknown_position else "lower_plus_d1"
                self.logger.debug(f"[{symbol}] Non-D1 trade open; allowing secondary D1 trade")
            else:
                # Fallback: block new trades if dual-trade mode is disabled
                can_open_trade = False
                block_reason = "Position open; blocking additional trades"

        # If we can't open any trade, skip
        if not can_open_trade:
            if block_reason:
                self.logger.debug(f"[{symbol}] {block_reason}")
            return

        # Evaluate candidates across timeframes (winners-only)
        candidates, eval_stats = self._evaluate_regime_candidates(
            symbol,
            broker_symbol,
            config,
            timeframes_filter=timeframes_filter,
        )
        
        if not candidates:
            if eval_stats:
                self.logger.debug(
                    f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
                    f"{eval_stats.get('timeframes_total', 0)} | winners=0 | actionable=0 "
                    f"(no_winner={eval_stats.get('no_winner', 0)}, "
                    f"failed_gate={eval_stats.get('winner_failed_gate', 0)}, "
                    f"insufficient_bars={eval_stats.get('insufficient_bars', 0)}, "
                    f"stale_skipped={eval_stats.get('timeframes_skipped_stale', 0)})"
                )
            return
        
        # Apply any timeframe restrictions implied by the existing open position.
        if allowed_timeframes is not None:
            original_count = len(candidates)
            candidates = [c for c in candidates if c['timeframe'] in allowed_timeframes]
            if len(candidates) < original_count:
                self.logger.debug(f"[{symbol}] Filtered out D1 candidates (secondary trade must be non-D1)")

        if open_magics:
            original_count = len(candidates)
            candidates = [
                c for c in candidates
                if TradeTagEncoder.encode_magic(symbol, c['timeframe'], c['regime']) not in open_magics
            ]
            if len(candidates) < original_count:
                self.logger.debug(f"[{symbol}] Filtered out candidates matching existing open position magic")

        if not candidates:
            self.logger.debug(f"[{symbol}] No valid candidates after secondary/open-position constraints")
            return
        
        # Select the best feasible candidate using an "actionable-within-margin" policy.
        
        #
        
        # Rationale:
        
        # - The single top-ranked candidate can legitimately have signal==0 on a given bar.
        
        # - In that case, we should not abort immediately if another high-quality candidate
        
        #   has an actionable signal (LONG/SHORT) and is close in score to the best overall.
        
        #
        
        # Policy:
        
        # 1) Compute best_overall_score across all candidates (even if signal==0)
        
        # 2) Filter to actionable candidates (signal != 0)
        
        # 3) Choose the best actionable candidate with selection_score >= best_overall_score * margin
        
        # 4) If none qualify, record a throttled no-trade decision and return
        
        best_overall = max(candidates, key=self._candidate_rank_key)

        best_overall_score = float(best_overall.get('selection_score', 0.0))

        # actionable_score_margin only makes sense as a quality
        # floor when the best score is positive. If the leader scores ≤ 0 the
        # multiplicative threshold collapses (or inverts), gate immediately so
        # we never trade against a non-positive selection score.
        if best_overall_score <= 0.0:
            bar_time_iso = str(best_overall.get('bar_time', ''))
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

        margin = float(self.pipeline_config.actionable_score_margin)
        
        # Clamp margin defensively: [0.0, 1.0]
        
        if margin < 0.0:
        
            margin = 0.0
        
        if margin > 1.0:
        
            margin = 1.0

        
        actionable = [c for c in candidates if int(c.get('signal', 0)) != 0]
        self.logger.debug(
            f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
            f"{eval_stats.get('timeframes_total', 0)} | winners={len(candidates)} | "
            f"actionable={len(actionable)} | stale_skipped={eval_stats.get('timeframes_skipped_stale', 0)}"
        )
        
        if not actionable:

            # No actionable winner signals on any timeframe for this bar.
            # Use helper to prevent duplicate log spam within same bar.

            bar_time_iso = str(best_overall.get('bar_time', ''))

            self._log_no_actionable_signal(
                symbol=symbol,
                message="NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)",
                best_candidate=best_overall,
                bar_time_iso=bar_time_iso,
                action_type="NO_ACTIONABLE_WINNER_SIGNAL"
            )

            return

        
        min_score = best_overall_score * margin
        
        eligible = [c for c in actionable if float(c.get('selection_score', 0.0)) >= min_score]
        
        if not eligible:

            # There were signals, but none were close enough to the best overall score.
            # Use helper to prevent duplicate log spam within same bar.

            bar_time_iso = str(best_overall.get('bar_time', ''))

            self._log_no_actionable_signal(
                symbol=symbol,
                message="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN (actionable below margin)",
                best_candidate=best_overall,
                bar_time_iso=bar_time_iso,
                action_type="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN"
            )

            return

        
        # Choose the best actionable candidate (highest selection score) within the margin band.
        
        best = max(eligible, key=self._candidate_rank_key)
        # ── Throttle check ──────────────────────────────────────────────
        # Build a decision key from the candidate's identifying attributes
        # so that the *same* signal on the *same* bar is only acted on once.
        bar_time_iso = str(best.get('bar_time', ''))
        decision_key = DecisionThrottle.make_decision_key(
            symbol, best['strategy_name'], best['timeframe'],
            best['regime'], int(best['signal']), bar_time_iso
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
        magic = TradeTagEncoder.encode_magic(symbol, best['timeframe'], best['regime'])
        signal_bar_index = max(len(best['features']) - 2, 0) if len(best['features']) > 1 else max(len(best['features']) - 1, 0)
        trade_intent = best['strategy'].build_trade_intent(
            best['features'],
            symbol=symbol,
            timeframe=best['timeframe'],
            regime=best['regime'],
            signal=int(best['signal']),
            spec=spec,
            bar_index=signal_bar_index,
            signal_strength=float(best.get('regime_strength', 0.0) or 0.0),
            selection_score=float(best.get('selection_score', 0.0) or 0.0),
            quality_score=float(best.get('quality_score', 0.0) or 0.0),
            governance_hint=dict(
                getattr(best.get('regime_config'), 'governance_policy', {}) or {}
            ),
            metadata={
                "bar_time": bar_time_iso,
                "freshness": float(best.get('freshness', 0.0) or 0.0),
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
            self.logger.debug(f"[{symbol}] D1 trade open; allowed second trade on {best['timeframe']}")

        if self.close_on_opposite_signal and symbol_positions:
            opposite_positions = []
            best_signal = int(best.get('signal', 0))
            for pos in symbol_positions:
                if best_signal == 1 and getattr(pos, 'type', 1) == 1:
                    opposite_positions.append(pos)
                elif best_signal == -1 and getattr(pos, 'type', 1) == 0:
                    opposite_positions.append(pos)

            if opposite_positions:
                self.logger.info(
                    f"[{symbol}] Closing {len(opposite_positions)} opposite-signal position(s) "
                    f"before new entry on {best['timeframe']}/{best['regime']}"
                )
                for pos in opposite_positions:
                    self._close_position_on_signal(pos, best['features'], spec)
                return

        # Execute entry (passes decision_key so _execute_entry can record
        # the outcome into the throttle)
        self._execute_entry(
            symbol=symbol,
            broker_symbol=broker_symbol,
            signal=int(best['signal']),
            strategy=best['strategy'],
            trade_intent=trade_intent,
            features=best['features'],
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
    
    def _evaluate_regime_candidates(self,
                                     symbol: str,
                                     broker_symbol: str,
                                     config: SymbolConfig,
                                     timeframes_filter: Optional[Set[str]] = None) -> tuple:
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
        }
        
        # Check if config has regime configs
        if not config.has_regime_configs():
            # Fallback to legacy single-strategy mode
            return self._evaluate_legacy_candidate(
                symbol,
                broker_symbol,
                config,
                timeframes_filter=timeframes_filter,
            ), stats
        
        available_timeframes = config.get_available_timeframes()
        stats["timeframes_total"] = len(available_timeframes)
        regime_params_file = getattr(self.pipeline_config, 'regime_params_file', 'regime_params.json') if self.pipeline_config else 'regime_params.json'
        probe_now = datetime.now()
        
        # Get live quality gate thresholds from config
        min_pf = getattr(self.pipeline_config, 'regime_min_val_profit_factor', 1.0) if self.pipeline_config else 1.0
        min_return = getattr(self.pipeline_config, 'regime_min_val_return_pct', 5.0) if self.pipeline_config else 5.0
        max_dd = getattr(self.pipeline_config, 'fx_val_max_drawdown', 35.0) if self.pipeline_config else 35.0
        min_return_dd_ratio = (
            getattr(self.pipeline_config, 'regime_min_val_return_dd_ratio', 1.0)
            if self.pipeline_config else 1.0
        )
        
        normalized_filter = {
            str(tf or "").upper()
            for tf in (timeframes_filter or set())
            if str(tf or "").strip()
        }

        for tf in available_timeframes:
            normalized_tf = str(tf or "").upper()
            if normalized_filter and normalized_tf not in normalized_filter:
                stats["timeframes_skipped_stale"] += 1
                continue
            if self._uses_scheduled_trigger() and not self._is_symbol_timeframe_due(symbol, tf, now=probe_now):
                stats["timeframes_skipped_stale"] += 1
                continue
            # Get bars for this timeframe (need at least the latest bar time)
            bars = self._get_live_bars(
                symbol,
                broker_symbol,
                tf,
                count=int(getattr(self.pipeline_config, 'live_bars_count', 200)),
                min_required=int(getattr(self.pipeline_config, 'live_min_bars', 100)),
            )
            if bars is None or len(bars) < int(getattr(self.pipeline_config, 'live_min_bars', 100)):
                stats["insufficient_bars"] += 1
                continue
            
            # Check for new bar (freshness tracking)
            current_bar_time = bars.index[-1]
            cache_key = f"{symbol}_{tf}"
            last_bar_time = self._last_bar_times.get(cache_key)
            
            # Determine if this is a new bar
            is_new_bar = (last_bar_time is None or current_bar_time > last_bar_time)
            if not is_new_bar:
                stats["timeframes_skipped_stale"] += 1
                continue
            stats["timeframes_evaluated"] += 1
            freshness = 1.0
            # Feature/signal cache logic
            # Build cache key including bar_time to ensure we invalidate on new bar
            feature_cache_key = f"{symbol}_{tf}_{current_bar_time}"
            cached = self._candidate_cache.get(feature_cache_key)
            
            if cached is not None:
                # Cache HIT - use cached features and signal (skip expensive computation)
                self._cache_hits += 1
                features = cached['features']
                current_regime = cached['regime']
                regime_strength = cached['regime_strength']
                current_signal = cached['signal']
                strategy = cached['strategy']
                regime_config = cached['regime_config']
                # Still need to check if regime_config exists (it was cached)
                if regime_config is None:
                    continue
            else:
                # Cache MISS - compute features and signal
                self._cache_misses += 1
                self._last_bar_times[cache_key] = current_bar_time
                # Clear old cache entries for this symbol/tf
                old_keys = [k for k in self._candidate_cache.keys()
                           if k.startswith(f"{symbol}_{tf}_")]
                for k in old_keys:
                    del self._candidate_cache[k]
                
                # Compute features with regime detection
                features = FeatureComputer.compute_all(
                    bars, symbol=symbol, timeframe=tf,
                    regime_params_file=regime_params_file
                )
                
                # Get current regime from the decision-time surface on the
                # last closed bar. REGIME_LIVE is the optimizer/backtester
                # contract; REGIME remains a legacy fallback for old fixtures.
                if len(features) < 2:
                    continue
                regime_col = 'REGIME_LIVE' if 'REGIME_LIVE' in features.columns else (
                    'REGIME' if 'REGIME' in features.columns else None
                )
                strength_col = 'REGIME_STRENGTH_LIVE' if 'REGIME_STRENGTH_LIVE' in features.columns else (
                    'REGIME_STRENGTH' if 'REGIME_STRENGTH' in features.columns else None
                )
                if regime_col is None:
                    continue

                current_regime = features[regime_col].iloc[-2]
                regime_strength = features[strength_col].iloc[-2] if strength_col else 0.5
                if (current_regime is None or pd.isna(current_regime)) and regime_col == 'REGIME_LIVE' and 'REGIME' in features.columns:
                    current_regime = features['REGIME'].iloc[-2]
                    if 'REGIME_STRENGTH' in features.columns:
                        regime_strength = features['REGIME_STRENGTH'].iloc[-2]
                
                if current_regime is None or pd.isna(current_regime):
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
                        'features': features,
                        'regime': current_regime,
                        'regime_strength': float(regime_strength),
                        'signal': 0,
                        'strategy': None,
                        'regime_config': None,
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
                        'features': features,
                        'regime': current_regime,
                        'regime_strength': float(regime_strength),
                        'signal': 0,
                        'strategy': None,
                        'regime_config': None,
                    }
                    self._prune_cache()
                    continue
                if not regime_config.is_valid_for_live(min_pf, min_return, max_dd, min_return_dd_ratio):
                    val_ret = regime_config.val_metrics.get('total_return_pct', -100)
                    val_dd = regime_config.val_metrics.get('max_drawdown_pct', 100)
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
                        'features': features,
                        'regime': current_regime,
                        'regime_strength': float(regime_strength),
                        'signal': 0,
                        'strategy': None,
                        'regime_config': None,
                    }
                    self._prune_cache()
                    continue
                
                # Get strategy instance
                try:
                    strategy = StrategyRegistry.get(regime_config.strategy_name, **regime_config.parameters)
                except Exception as e:
                    self.logger.warning(f"[{symbol}] Failed to get strategy {regime_config.strategy_name}: {e}")
                    continue
                
                # Generate signal
                signals = strategy.generate_signals(features, symbol)
                current_signal = signals.iloc[-2] if len(signals) > 1 else 0
                if pd.isna(current_signal):
                    current_signal = 0

                # Cache the computed result
                self._candidate_cache[feature_cache_key] = {
                    'features': features,
                    'regime': current_regime,
                    'regime_strength': float(regime_strength),
                    'signal': int(current_signal),
                    'strategy': strategy,
                    'regime_config': regime_config,
                }
                self._prune_cache()  # Prevent unbounded growth
            # ─────────────────────────────────────────────────────────────────
            
            # Compute selection score = strength * quality_score * freshness
            quality_score = (
                float(regime_config.quality_score)
                if getattr(regime_config, "quality_score", None) is not None
                else 0.5
            )
            selection_score = float(regime_strength) * quality_score * float(freshness)
            eligibility_report = {}
            portfolio_manager = getattr(self, "pm", None)
            if portfolio_manager is not None and hasattr(portfolio_manager, "live_eligibility_report"):
                eligibility_report = portfolio_manager.live_eligibility_report(symbol, tf, current_regime)
            
            candidates.append({
                'timeframe': tf,
                'regime': current_regime,
                'regime_strength': float(regime_strength),
                'quality_score': quality_score,
                'freshness': freshness,
                'selection_score': selection_score,
                'strategy': strategy,
                'strategy_name': regime_config.strategy_name,
                'signal': int(current_signal),
                'features': features,
                'regime_config': regime_config,
                'eligibility_report': eligibility_report,
                'bar_time': str(features.index[-2]) if len(features) > 1 else '',
            })
            stats["winner_candidates"] += 1
        
        return candidates, stats
    
    def _evaluate_legacy_candidate(self,
                                   symbol: str,
                                   broker_symbol: str,
                                   config: SymbolConfig,
                                   timeframes_filter: Optional[Set[str]] = None) -> List[Dict]:
        """
        Fallback for legacy configs without regime_configs.
        
        Uses the single strategy_name/timeframe/parameters from config.
        """
        from pm_strategies import StrategyRegistry
        
        # Winners-only live gate for legacy configs
        min_pf = getattr(self.pipeline_config, 'regime_min_val_profit_factor', 1.0) if self.pipeline_config else 1.0
        min_return = getattr(self.pipeline_config, 'regime_min_val_return_pct', 5.0) if self.pipeline_config else 5.0
        max_dd = getattr(self.pipeline_config, 'fx_val_max_drawdown', 35.0) if self.pipeline_config else 35.0
        min_return_dd_ratio = (
            getattr(self.pipeline_config, 'regime_min_val_return_dd_ratio', 1.0)
            if self.pipeline_config else 1.0
        )
        val_pf = config.val_metrics.get('profit_factor', 0.0)
        val_return = config.val_metrics.get('total_return_pct', -100.0)
        val_dd = config.val_metrics.get('max_drawdown_pct', 100.0)
        val_return_dd_ratio = val_return / max(val_dd, 0.5)
        if (
            not config.is_validated or
            val_pf < min_pf or
            val_return < min_return or
            val_dd > max_dd or
            val_return_dd_ratio < min_return_dd_ratio
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

        if self._uses_scheduled_trigger() and not self._is_symbol_timeframe_due(symbol, timeframe):
            return []
        
        # Get bars
        bars = self._get_live_bars(
            symbol,
            broker_symbol,
            timeframe,
            count=int(getattr(self.pipeline_config, 'live_bars_count', 200)),
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
            self.logger.warning(f"[{symbol}] Failed to get strategy {config.strategy_name}: {e}")
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
        if portfolio_manager is not None and hasattr(portfolio_manager, "live_eligibility_report"):
            eligibility_report = portfolio_manager.live_eligibility_report(symbol, timeframe, 'LEGACY')

        return [{
            'timeframe': timeframe,
            'regime': 'LEGACY',
            'regime_strength': 1.0,
            'quality_score': (
                float(config.composite_score) / 100.0
                if getattr(config, "composite_score", None) is not None
                else 0.5
            ),
            'freshness': 1.0,
            'selection_score': 1.0,
            'strategy': strategy,
            'strategy_name': config.strategy_name,
            'signal': int(current_signal),
            'features': features,
            'regime_config': None,
            'eligibility_report': eligibility_report,
            'bar_time': str(features.index[-2]) if len(features) > 1 else '',
        }]
    
    def _execute_entry(self,
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
                       account_info: Optional[Any] = None):
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
        _tf = best_candidate.get('timeframe', '') if best_candidate else ''
        _regime = best_candidate.get('regime', '') if best_candidate else ''
        _strat_name = best_candidate.get('strategy_name', '') if best_candidate else ''
        
        decision_context = {
            "secondary_trade": bool(is_secondary_trade),
            "secondary_reason": secondary_reason,
            "position_context": position_context,
        }

        def _record_throttle(action: str, cooldown: int = 0) -> None:
            """Record this decision outcome into the throttle."""
            if decision_key:
                self._decision_throttle.record_decision(
                    symbol=symbol, decision_key=decision_key,
                    bar_time_iso=bar_time_iso,
                    timeframe=_tf, regime=_regime,
                    strategy_name=_strat_name,
                    direction=signal, action=action,
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
            return any(hasattr(value, attr) for attr in ("ticket", "symbol", "magic", "volume", "comment"))

        selection_score = _safe_float(best_candidate.get('selection_score')) if best_candidate else None
        quality_score = _safe_float(best_candidate.get('quality_score')) if best_candidate else None
        freshness_score = _safe_float(best_candidate.get('freshness')) if best_candidate else None
        regime_strength_score = _safe_float(best_candidate.get('regime_strength')) if best_candidate else None
        decision_context.update({
            "selection_score": selection_score,
            "quality_score": quality_score,
            "freshness": freshness_score,
            "regime_strength": regime_strength_score,
        })
        if trade_intent is not None:
            decision_context["governance_policy"] = policy_name_from_artifact(
                getattr(trade_intent, "governance_hint", {}) or {}
            )

        def _record_actionable(action: str,
                               entry_price_value: Optional[float] = None,
                               sl_value: Optional[float] = None,
                               tp_value: Optional[float] = None,
                               volume_value: Optional[float] = None,
                               target_risk_pct_value: Optional[float] = None,
                               actual_risk_pct_value: Optional[float] = None,
                               context: Optional[Dict[str, Any]] = None) -> None:
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
                    "action_time": datetime.now().isoformat(),
                    "entry_price": entry_price_value,
                    "stop_loss_price": sl_value,
                    "take_profit_price": tp_value,
                    "volume": volume_value,
                    "target_risk_pct": target_risk_pct_value,
                    "actual_risk_pct": actual_risk_pct_value,
                    "secondary_trade": bool(is_secondary_trade),
                    "secondary_reason": secondary_reason,
                    "position_context": position_context,
                    "score": selection_score,
                    "quality": quality_score,
                    "freshness": freshness_score,
                    "regime_strength": regime_strength_score,
                }
                if isinstance(context, dict) and context:
                    payload["context"] = dict(context)
                self._actionable_log.record(symbol, payload)
                storage_manager = getattr(self, "storage_manager", None)
                if storage_manager is not None:
                    storage_manager.record_actionable(payload)
            except Exception as exc:
                self.logger.debug(f"[{symbol}] Actionable log failed: {exc}")

        storage_manager = getattr(self, "storage_manager", None)
        if storage_manager is not None and storage_manager.should_pause_new_entries():
            self.logger.warning(f"[{symbol}] Skipping trade; storage pressure gate active")
            _record_throttle("SKIPPED_STORAGE_PRESSURE")
            _record_actionable(action="SKIPPED_STORAGE_PRESSURE")
            return
        
        # Check rate limit - prevent rapid order submission
        last_order_time = self._last_order_times.get(symbol)
        if last_order_time:
            time_since_last = (datetime.now() - last_order_time).total_seconds()
            if time_since_last < self.ORDER_RATE_LIMIT_SECONDS:
                remaining = max(0.0, self.ORDER_RATE_LIMIT_SECONDS - time_since_last)
                self.logger.info(f"[{symbol}] Skipping trade; rate limited ({remaining:.1f}s remaining)")
                _record_throttle("SKIPPED_RATE_LIMIT", cooldown=int(max(1.0, remaining)))
                _record_actionable(action="SKIPPED_RATE_LIMIT")
                return
        
        # Re-verify no position exists right before execution (race condition prevention)
        existing = None
        fresh_symbol_positions = None
        if positions_snapshot is not None:
            for pos in positions_snapshot:
                if getattr(pos, 'symbol', None) == broker_symbol and int(getattr(pos, 'magic', 0) or 0) == magic:
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
                if int(getattr(pos, 'magic', 0) or 0) == magic:
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
                pos for pos in fresh_symbol_positions
                if getattr(pos, 'symbol', None) == broker_symbol
            ]
            current_tf = str(_tf or "").upper()
            allow_d1_plus_lower = bool(
                getattr(self.pipeline_config, 'allow_d1_plus_lower_tf', True)
            ) if self.pipeline_config else True
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

            position_context.update({
                "fresh_open_positions_total": len(fresh_symbol_positions),
                "fresh_open_timeframes": sorted(fresh_open_timeframes),
                "fresh_open_has_d1": fresh_has_d1,
                "fresh_open_has_non_d1": fresh_has_non_d1,
                "fresh_open_has_unknown": fresh_has_unknown,
            })

            def _block_fresh_position_state(action: str, reason: str) -> None:
                self.logger.info(f"[{symbol}] Trade blocked by fresh position state: {reason}")
                _record_throttle(action)
                _record_actionable(action=action, context={"position_state_reason": reason})

            if len(fresh_symbol_positions) >= 2:
                _block_fresh_position_state(
                    "BLOCKED_SYMBOL_POSITION_LIMIT",
                    "2 positions already open",
                )
                return
            if len(fresh_symbol_positions) == 1:
                if not allow_d1_plus_lower:
                    _block_fresh_position_state(
                        "BLOCKED_SYMBOL_POSITION_EXISTS",
                        "additional entries disabled while a same-symbol position is open",
                    )
                    return
                if fresh_has_unknown:
                    _block_fresh_position_state(
                        "BLOCKED_POSITION_TIMEFRAME_UNKNOWN",
                        "fresh same-symbol position timeframe is unknown",
                    )
                    return
                if fresh_has_d1 and current_tf != "D1":
                    is_secondary_trade = True
                    secondary_reason = secondary_reason or "d1_plus_lower"
                elif fresh_has_non_d1 and current_tf == "D1":
                    is_secondary_trade = True
                    secondary_reason = secondary_reason or "lower_plus_d1"
                else:
                    _block_fresh_position_state(
                        "BLOCKED_SYMBOL_POSITION_LIMIT",
                        "fresh same-symbol position is not a valid D1/lower-TF pairing",
                    )
                    return
            elif is_secondary_trade:
                is_secondary_trade = False
                secondary_reason = ""

            decision_context.update({
                "secondary_trade": bool(is_secondary_trade),
                "secondary_reason": secondary_reason,
                "position_context": position_context,
            })

        if trade_intent is None:
            signal_bar_index = max(len(features) - 2, 0) if len(features) > 1 else max(len(features) - 1, 0)
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
        if not getattr(symbol_info, 'visible', True):
            self.logger.warning(f"[{symbol}] Symbol not visible on broker; skipping trade")
            _record_throttle("SKIPPED_SYMBOL_NOT_VISIBLE")
            _record_actionable(
                action="SKIPPED_SYMBOL_NOT_VISIBLE",
                context={"broker_symbol": broker_symbol},
            )
            return
        if getattr(symbol_info, 'trade_mode', 2) == 0:  # SYMBOL_TRADE_MODE_DISABLED
            self.logger.warning(f"[{symbol}] Symbol trade mode disabled; skipping trade")
            _record_throttle("SKIPPED_SYMBOL_TRADE_DISABLED")
            _record_actionable(
                action="SKIPPED_SYMBOL_TRADE_DISABLED",
                context={
                    "broker_symbol": broker_symbol,
                    "trade_mode": getattr(symbol_info, 'trade_mode', 0),
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
        margin_level = self._safe_account_margin_level(account)
        margin_used = self._account_margin_used(account)
        margin_block = float(getattr(
            self.pipeline_config, 'margin_entry_block_level', 100.0))
        margin_reopen = float(getattr(
            self.pipeline_config, 'margin_reopen_level', margin_block) or margin_block)
        if margin_used and (margin_level is None or margin_level <= 0.0):
            self.logger.warning(
                f"[{symbol}] MARGIN DATA UNAVAILABLE: margin is in use but margin_level is unavailable; entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(action="SKIPPED_MARGIN_UNAVAILABLE")
            return
        # margin_level == 0 means no positions open (undefined, not stressed).
        if margin_level is not None and margin_level > 0 and margin_level < margin_block:
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
        cooldown_minutes = float(getattr(
            self.pipeline_config, 'margin_reopen_cooldown_minutes', 15.0) or 0.0)
        last_margin_close_ts = getattr(self, '_last_margin_close_ts', None)
        if cooldown_minutes > 0 and last_margin_close_ts is not None:
            elapsed = (datetime.now() - last_margin_close_ts).total_seconds() / 60.0
            if elapsed < cooldown_minutes:
                remaining = cooldown_minutes - elapsed
                last_notice = getattr(self, '_margin_cooldown_notice_logged_ts', None)
                if (last_notice is None or
                        (datetime.now() - last_notice).total_seconds() > 60.0):
                    self.logger.info(
                        f"[{symbol}] MARGIN COOLDOWN: {remaining:.1f} min remaining "
                        f"(forced close at {last_margin_close_ts.isoformat(timespec='seconds')}) — entry blocked"
                    )
                    self._margin_cooldown_notice_logged_ts = datetime.now()
                _record_throttle("SKIPPED_MARGIN_COOLDOWN")
                _record_actionable(action="SKIPPED_MARGIN_COOLDOWN")
                return

        basis_pref = getattr(self.position_config, "risk_basis", "balance")
        basis_value = account.balance if basis_pref == "balance" else account.equity
        if basis_value <= 0:
            self.logger.warning(f"[{symbol}] Invalid risk basis value ({basis_value}); skipping trade")
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
        if hasattr(_spread_overlay, 'enabled') and _spread_overlay.enabled:
            _pip_size = spec.pip_size if spec.pip_size > 0 else 1e-5
            _spread_pips = (tick.ask - tick.bid) / _pip_size
            _atr_val = 0.0
            if features is not None and 'ATR_14' in features.columns and len(features) > 1:
                _atr_val = float(features['ATR_14'].iloc[-2]) / _pip_size
            _rolling_spread_median = float(spec.spread_avg)
            if features is not None and 'Spread' in features.columns and len(features) > 2:
                _spread_window = pd.to_numeric(features['Spread'].iloc[:-1], errors='coerce').dropna().tail(100)
                if not _spread_window.empty:
                    _rolling_window_median = float((_spread_window * float(spec.point) / _pip_size).median())
                    # Anchor against the larger of rolling-window median and the
                    # instrument's baseline spread_avg. Pre-news compression can
                    # collapse the rolling median and arm the spike blocker on
                    # routine widenings; post-news persistence can inflate it
                    # and hide real spikes. Taking max() keeps the spike test
                    # honest in both regimes.
                    _rolling_spread_median = max(_rolling_window_median, float(spec.spread_avg))
            _eq_ctx = ExecutionQualityContext(
                symbol=symbol, timeframe=_tf,
                spread_pips=_spread_pips,
                atr_pips=_atr_val,
                rolling_spread_median=_rolling_spread_median,
            )
            _eq_decision = _spread_overlay.evaluate(_eq_ctx)
            if not _eq_decision.allow_trade:
                self.logger.info(f"[{symbol}] Trade blocked by spread filter: {'; '.join(_eq_decision.notes)}")
                _record_throttle("BLOCKED_SPREAD_FILTER")
                _record_actionable(
                    action="BLOCKED_SPREAD_FILTER",
                    entry_price_value=entry_price,
                )
                return
            _execution_risk_mult = float(max(0.0, min(1.0, _eq_decision.score_multiplier)))
            if _eq_decision.notes:
                self.logger.debug(f"[{symbol}] Spread overlay: {'; '.join(_eq_decision.notes)}")

        # Calculate stop prices from pips
        sl_price, tp_price = self.position_calc.calculate_stop_prices(entry_price, sl_pips, tp_pips, is_long, spec)

        # Capture original R-multiple for preservation when SL is widened
        original_sl_dist = abs(entry_price - sl_price)
        original_tp_dist = abs(tp_price - entry_price)
        original_r_multiple = (original_tp_dist / original_sl_dist) if original_sl_dist > 0 else 0.0

        # Enforce broker minimum stop distance (auto widen SL if too close)
        min_stop_dist = float(symbol_info.trade_stops_level) * float(symbol_info.point) if symbol_info.trade_stops_level else 0.0
        if min_stop_dist > 0 and abs(entry_price - sl_price) < min_stop_dist and getattr(self.position_config, "auto_widen_sl", True):
            if is_long:
                sl_price = entry_price - min_stop_dist
            else:
                sl_price = entry_price + min_stop_dist
            # Preserve validated R-multiple: recalculate TP to match original reward:risk
            if original_r_multiple > 0:
                new_sl_dist = abs(entry_price - sl_price)
                new_tp_dist = original_r_multiple * new_sl_dist
                tp_price = (entry_price + new_tp_dist) if is_long else (entry_price - new_tp_dist)
            self.logger.debug(f"[{symbol}] Widened SL to satisfy min stop distance ({min_stop_dist}), TP adjusted to preserve R={original_r_multiple:.2f}")

        # Target risk (deposit currency)
        # -------------------------------------------------------------
        # Winners-only risk policy: use base risk directly.
        # PositionConfig.max_risk_pct acts as the hard safety cap.
        # Secondary trade: additional cap + combined risk cap per symbol.
        # -------------------------------------------------------------
        from pm_position import TradeTagEncoder

        configured_risk_pct = float(getattr(self.position_config, 'risk_per_trade_pct', 1.0))
        min_trade_risk = float(getattr(self.pipeline_config, 'min_trade_risk_pct', 0.1))
        target_risk_pct = configured_risk_pct

        # Enforce PositionConfig max_risk_pct (hard safety cap)
        max_risk_cap = float(getattr(self.position_config, 'max_risk_pct', 2.0))
        target_risk_pct = min(target_risk_pct, max_risk_cap)

        # Optional risk-scalar stack
        _risk_stack = self._enhancement_seams.risk_scalar_stack
        if _risk_stack.overlays:
            _atr_price = 0.0
            if features is not None and 'ATR_14' in features.columns and len(features) > 1:
                _atr_price = float(features['ATR_14'].iloc[-2])
            _open_count = len(positions_snapshot) if positions_snapshot else 0
            _peak_equity = max(float(getattr(account, 'equity', 0.0) or 0.0), float(self._equity_peak or 0.0))
            _open_exposure_pct = 0.0
            if positions_snapshot:
                for _pos in positions_snapshot:
                    _risk_pct = self._estimate_position_risk_pct(
                        _pos,
                        account_info=account,
                        canonical_symbol=str(getattr(_pos, 'symbol', '') or ''),
                        broker_symbol=str(getattr(_pos, 'symbol', '') or ''),
                    )
                    if _risk_pct is not None:
                        _open_exposure_pct += float(_risk_pct)
            _regime_metrics = {}
            _regime_cfg = best_candidate.get('regime_config') if best_candidate else None
            if _regime_cfg is not None:
                _regime_metrics = dict(getattr(_regime_cfg, 'val_metrics', {}) or {})
                if not _regime_metrics:
                    _regime_metrics = dict(getattr(_regime_cfg, 'train_metrics', {}) or {})
            _rs_ctx = RiskScalarContext(
                symbol=symbol, timeframe=_tf, regime=_regime,
                base_risk_pct=target_risk_pct,
                account_equity=float(account.equity),
                account_peak_equity=_peak_equity,
                current_atr=_atr_price,
                current_price=float(entry_price),
                target_annual_vol=float(getattr(self.pipeline_config, 'target_annual_vol', 0.10)),
                open_position_count=_open_count,
                open_exposure_pct=_open_exposure_pct,
                historical_win_rate=float(_regime_metrics.get('win_rate', 0.0)) / 100.0,
                historical_avg_win=float(_regime_metrics.get('avg_win_dollars', _regime_metrics.get('avg_win', 0.0))),
                historical_avg_loss=float(_regime_metrics.get('avg_loss_dollars', _regime_metrics.get('avg_loss', 0.0))),
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

        # Secondary trade adjustments + combined risk cap per symbol
        if is_secondary_trade:
            original_risk = target_risk_pct
            secondary_mult = float(getattr(self.pipeline_config, 'd1_secondary_risk_multiplier', 1.0))
            secondary_cap = float(getattr(self.pipeline_config, 'secondary_trade_max_risk_pct', 0.9))
            max_combined_risk = float(getattr(self.pipeline_config, 'max_combined_risk_pct', 3.0))

            target_risk_pct = target_risk_pct * secondary_mult
            target_risk_pct = min(target_risk_pct, secondary_cap)

            # Sum existing risk for this symbol from tagged positions.
            # If a position has no parseable risk tag, assume base_risk_pct for safety.
            existing_risk = 0.0
            try:
                source_positions = positions_snapshot
                if source_positions is None:
                    source_positions = self.mt5.get_positions(symbol=broker_symbol)
                if source_positions is None:
                    self.logger.warning(f"[{symbol}] Position snapshot unavailable for secondary risk check; skipping trade")
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
                    if getattr(pos, 'symbol', None) != broker_symbol:
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
                self.logger.info(f"[{symbol}] Secondary trade blocked: combined risk cap reached ({existing_risk:.2f}% >= {max_combined_risk:.2f}%)")
                self._decision_throttle.record_decision(
                    symbol=symbol, decision_key=decision_key,
                    bar_time_iso=bar_time_iso,
                    timeframe=best_candidate.get('timeframe'), regime=best_candidate.get('regime'),
                    strategy_name=best_candidate.get('strategy_name'), direction=int(best_candidate.get('signal', 0)),
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
            self.logger.info(f"[{symbol}] Secondary trade: risk {original_risk:.2f}% -> {target_risk_pct:.2f}% (existing={existing_risk:.2f}%, cap={max_combined_risk:.2f}%)")

        # Ensure non-zero (and not absurdly tiny) risk if we proceed
        if target_risk_pct < min_trade_risk:
            self.logger.info(f"[{symbol}] Trade blocked: computed risk {target_risk_pct:.3f}% below min_trade_risk_pct={min_trade_risk:.3f}%")
            _record_throttle("BLOCKED_TOO_LOW_RISK")
            _record_actionable(
                action="BLOCKED_TOO_LOW_RISK",
                context=decision_context,
            )
            return
        
        target_risk_amount = basis_value * (target_risk_pct / 100.0)

        # Volume bounds (combine config + broker)
        min_vol = max(float(self.position_config.min_position_size), float(symbol_info.volume_min))
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
            self.logger.info(f"[{symbol}] Using tick-based fallback: loss_per_lot=${loss_per_lot:.2f}")
        elif loss_per_lot_path == "pip_value" and loss_per_lot:
            self.logger.warning(f"[{symbol}] Using pip-value fallback (less reliable): loss_per_lot=${loss_per_lot:.2f}")

        if loss_per_lot is None or loss_per_lot <= 0:
            self.logger.warning(f"[{symbol}] Could not compute loss_per_lot; skipping trade")
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
        if volume_raw > max_vol and getattr(self.position_config, "auto_widen_sl", True):
            widen_factor = 1.25
            max_iters = 6
            for _ in range(max_iters):
                dist = abs(entry_price - sl_price) * widen_factor
                sl_price = (entry_price - dist) if is_long else (entry_price + dist)
                # Preserve validated R-multiple: adjust TP proportionally
                if original_r_multiple > 0:
                    new_tp_dist = original_r_multiple * dist
                    tp_price = (entry_price + new_tp_dist) if is_long else (entry_price - new_tp_dist)
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
                    self.logger.debug(f"[{symbol}] Widened SL to fit max volume constraint (max_vol={max_vol}), TP adjusted to preserve R={original_r_multiple:.2f}")
                    break

        risk_target_volume = volume_raw

        # Clamp and normalize volume (risk-safe floor to step)
        volume_raw = max(min_vol, min(max_vol, volume_raw))
        volume = self.mt5.normalize_volume(volume_raw, symbol_info)

        # If normalization pushed below min, force to min_vol (may exceed target; enforce cap below)
        if volume < min_vol:
            volume = min_vol

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
            self.logger.warning(f"[{symbol}] Could not compute actual_risk_amount; skipping trade")
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

        actual_risk_pct = (actual_risk_amount / basis_value) * 100.0 if basis_value > 0 else float('inf')
        max_risk_pct = float(getattr(self.position_config, 'max_risk_pct', 2.0))
        risk_tolerance_pct = float(getattr(self.position_config, 'risk_tolerance_pct', 2.0))
        # Lot-normalization drift gate:
        # within tolerance -> debug only
        # over tolerance but inside cap -> operator-visible warning
        # over hard cap -> block
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
            risk_cap_action = "BLOCKED_MIN_LOT_EXCEEDS_CAP" if risk_target_volume < min_vol - 1e-9 else "SKIPPED_RISK_CAP"
            self.logger.warning(
                f"[{symbol}] Skipping trade; actual risk {actual_risk_pct:.2f}% exceeds "
                f"per-trade hard cap {max_risk_pct:.2f}% "
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
                    "risk_cap_reason": drift_reason if actual_risk_pct > target_risk_pct + 1e-9 else "hard_cap",
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
            config.symbol,    # Canonical symbol for PM3: comment matching
            actual_risk_pct,  # Use actual risk (post-normalization) not target
            broker_symbol,    # Broker symbol for MT5 position lookup
            positions_snapshot=fresh_symbol_positions if fresh_symbol_positions is not None else positions_snapshot,
            account_info=account,
        )

        if not can_trade:
            self.logger.warning(f"[{symbol}] {risk_reason}")
            _record_throttle('BLOCKED_SYMBOL_RISK_CAP')
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
            margin_required_unavailable = margin_required_raw is not None and margin_required is None
        free_margin = self._safe_finite_float(getattr(account, "margin_free", None))
        has_free_margin = free_margin is not None
        if margin_required_unavailable:
            self.logger.warning(f"[{symbol}] Skipping trade; required margin calculation returned a non-finite value")
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
        if margin_required is not None and margin_required > 0.0 and not has_free_margin:
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
        if margin_required is not None and margin_required > 0.0 and has_free_margin and margin_required > free_margin:
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
            self.logger.warning(f"[{symbol}] Skipping trade; final tick data unavailable")
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
        final_entry_price = self._safe_finite_float(final_tick.ask if is_long else final_tick.bid)
        if final_entry_price is None or final_entry_price <= 0.0:
            self.logger.warning(f"[{symbol}] Skipping trade; final executable price unavailable")
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
            self.logger.warning(f"[{symbol}] Skipping trade; invalid final SL/TP distance")
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
        sl_price = (entry_price - sl_distance) if is_long else (entry_price + sl_distance)
        tp_price = (entry_price + tp_distance) if is_long else (entry_price - tp_distance)
        if min_stop_dist > 0 and sl_distance < min_stop_dist and getattr(self.position_config, "auto_widen_sl", True):
            sl_distance = min_stop_dist
            sl_price = (entry_price - sl_distance) if is_long else (entry_price + sl_distance)
            final_r_multiple = (tp_distance / sl_distance) if sl_distance > 0.0 else 0.0
            if final_r_multiple > 0.0:
                tp_distance = final_r_multiple * sl_distance
                tp_price = (entry_price + tp_distance) if is_long else (entry_price - tp_distance)

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
            self.logger.warning(f"[{symbol}] Could not compute final actual_risk_amount; skipping trade")
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
        actual_risk_pct = (actual_risk_amount / basis_value) * 100.0 if basis_value > 0 else float('inf')
        drift_severity = classify_lot_normalization_drift(
            actual_risk_pct=actual_risk_pct,
            target_risk_pct=target_risk_pct,
            max_risk_pct=max_risk_pct,
            tolerance_pct=risk_tolerance_pct,
        )
        if drift_severity == "block":
            risk_cap_action = "BLOCKED_MIN_LOT_EXCEEDS_CAP" if risk_target_volume < min_vol - 1e-9 else "SKIPPED_RISK_CAP"
            self.logger.warning(
                f"[{symbol}] Skipping trade after final quote refresh; actual risk "
                f"{actual_risk_pct:.2f}% exceeds per-trade hard cap {max_risk_pct:.2f}% "
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

        can_trade, risk_reason = self._check_symbol_combined_risk_cap(
            config.symbol,
            actual_risk_pct,
            broker_symbol,
            positions_snapshot=fresh_symbol_positions if fresh_symbol_positions is not None else positions_snapshot,
            account_info=account,
        )
        if not can_trade:
            self.logger.warning(f"[{symbol}] {risk_reason}")
            _record_throttle('BLOCKED_SYMBOL_RISK_CAP')
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
            margin_required_unavailable = margin_required_raw is not None and margin_required is None
        if margin_required_unavailable:
            self.logger.warning(f"[{symbol}] Skipping trade; final required margin calculation returned a non-finite value")
            _record_throttle("SKIPPED_MARGIN_UNAVAILABLE")
            _record_actionable(
                action="SKIPPED_MARGIN_UNAVAILABLE",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
                context={"required_margin": None, "free_margin": free_margin, "stage": "final_quote_refresh"},
            )
            return
        if margin_required is not None and margin_required > 0.0 and not has_free_margin:
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
                context={"required_margin": margin_required, "free_margin": None, "stage": "final_quote_refresh"},
            )
            return
        if margin_required is not None and margin_required > 0.0 and has_free_margin and margin_required > free_margin:
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
                context={"required_margin": margin_required, "free_margin": free_margin, "stage": "final_quote_refresh"},
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
            self._log_trade(symbol, direction, volume, entry_price,
                            sl_price, tp_price, magic, "PAPER")
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

        # Encode enough metadata for later position attribution.
        trade_comment = TradeTagEncoder.encode_comment(
            symbol=config.symbol,  # Use original symbol, not broker symbol
            timeframe=_tf,
            strategy_name=_strat_name,
            direction=direction,
            risk_pct=target_risk_pct,
        )

        # Execute order
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
        
        if result.success:
            self.logger.info(
                f"[OK] [{symbol}] {direction} executed: "
                f"{result.volume} lots @ {result.price:.5f}"
            )
            self._log_trade(symbol, direction, result.volume, result.price,
                           sl_price, tp_price, magic, "EXECUTED")
            # Record order time for rate limiting
            self._last_order_times[symbol] = datetime.now()
            _record_throttle("EXECUTED")
            actual_risk_pct_exec = actual_risk_pct
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
            _record_actionable(
                action="EXECUTED",
                entry_price_value=result.price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=result.volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct_exec,
            )
        else:
            self.logger.warning(
                f"[FAIL] [{symbol}] Order failed: {result.retcode} - {result.retcode_description}"
            )
            self._log_trade(symbol, direction, volume, entry_price,
                           sl_price, tp_price, magic, f"FAILED: {result.retcode_description}")
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
    
    def _close_position_on_signal(self, position: MT5Position, features: pd.DataFrame, spec):
        """Close position on opposite signal."""
        if not self.enable_trading:
            self.logger.info(f"[{position.symbol}] [PAPER] Would close position")
            return
        
        result = self.mt5.close_position(
            position=position,
            deviation=30,
            comment="PM_SignalExit"
        )
        
        if result.success:
            self.logger.info(f"[OK] [{position.symbol}] Position closed on signal reversal")
        else:
            self.logger.warning(f"[FAIL] [{position.symbol}] Close failed: {result.retcode_description}")
    
    def _log_trade(self, symbol: str, direction: str, volume: float,
                   price: float, sl: float, tp: float, magic: int, status: str):
        """Log trade to history."""
        self.trade_log.append({
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'direction': direction,
            'volume': volume,
            'price': price,
            'sl': sl,
            'tp': tp,
            'magic': magic,
            'status': status
        })
    
    def save_trade_log(self, filepath: str):
        """Save trade log to file."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
        with open(tmp_path, 'w', encoding='utf-8') as f:
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
                    print(f"  {pos.symbol} | {direction} | {pos.volume} lots | P/L: {pos.profit:.2f}")
        
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
    
    def __init__(self,
                 symbols: List[str] = None,
                 config: PipelineConfig = None,
                 pipeline_config: "PipelineConfig" = None,
                 mt5_config: "MT5Config" = None,
                 position_config: "PositionConfig" = None,
                 data_dir: Optional[str] = None,
                 output_dir: Optional[str] = None,
                 config_file: Optional[str] = None):
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
            output_dir=Path(output_dir or "./pm_outputs")
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
            getattr(self.position_config, 'regime_tp_multipliers', {}) or {}
        )

        # Install regime-aware TP multipliers before any strategy is evaluated.
        set_regime_tp_multipliers(getattr(self.position_config, 'regime_tp_multipliers', None))

        # Paths follow the resolved pipeline config unless explicitly overridden above.
        self.data_dir = Path(self.pipeline_config.data_dir)
        self.output_dir = Path(self.pipeline_config.output_dir)
        self.log_dir = Path(self.pipeline_config.log_dir)
        resolved_config_file = config_file or str(getattr(self.pipeline_config, "winner_ledger_path", "pm_configs.json"))
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
                self.logger.warning("Failed to connect to MT5 - running in offline mode")
            else:
                self.logger.info("[OK] Connected to MT5")
                if self.storage_manager:
                    self.storage_manager.add_active_server(getattr(self.mt5_config, "server", ""))
        
        # Initialize portfolio manager
        self.portfolio_manager = PortfolioManager(
            config=self.pipeline_config,
            symbols=self.symbols,
            config_file=self.config_file
        )
        
        self.logger.info(f"[OK] Portfolio Manager initialized")
        self.logger.info(f"  Symbols: {len(self.symbols)}")
        self.logger.info(f"  Strategies: {StrategyRegistry.count()}")
        self.logger.info(f"  Existing configs: {len(self.portfolio_manager.symbol_configs)}")
        
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
        
        # Fetch data if MT5 connected
        if self.mt5 and self.mt5.is_connected():
            self._fetch_historical_data()
        else:
            self.logger.info("Using existing data files (MT5 not connected)")
        
        # Run optimization with stateful persistence
        results = self.portfolio_manager.initial_optimization(overwrite=overwrite)
        
        # Save results (legacy - now handled incrementally by ledger)
        self._save_results(results)
        if self.storage_manager:
            self.storage_manager.on_optimization_complete()
        
        # Print summary
        self.portfolio_manager.print_status()
        
        return len(self.portfolio_manager.get_live_eligible_configs()) > 0

    def _check_production_retrain_schedule(self) -> None:
        """Run or announce the config-driven production retrain schedule."""
        if not self.portfolio_manager:
            return
        mode = str(getattr(self.pipeline_config, "production_retrain_mode", "auto") or "auto").strip().lower()
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
        self._fetch_historical_data(symbols_to_retrain)
        self.portfolio_manager.retrain_all_needed()
        self._last_retrain_schedule_notice = due_str
        if self.trader:
            self.trader.invalidate_runtime_caches()
        if self.storage_manager:
            self.storage_manager.on_optimization_complete()

    def _get_active_live_timeframes(self) -> List[str]:
        """Return the unique active timeframes from the current validated configs."""
        validated = self.portfolio_manager.get_live_eligible_configs() if self.portfolio_manager else {}
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
        return sorted(ordered, key=lambda tf: int(DataLoader.TIMEFRAME_MINUTES.get(tf, 10 ** 6)))

    def _wait_for_next_due(self,
                           shutdown_event: threading.Event,
                           due_at: datetime,
                           *,
                           now: Optional[datetime] = None) -> None:
        """
        Comparator-based idle wait for the next due action.

        Business cadence is controlled by `due_at`; the wait itself is bounded so
        the loop remains responsive to shutdowns and state changes.
        """
        probe = now or datetime.now()
        timeout = max(0.0, (due_at - probe).total_seconds())
        shutdown_event.wait(timeout=min(timeout, 1.0))

    def _attempt_reconnect(self,
                           reconnect_state: Dict[str, Any],
                           *,
                           now: Optional[datetime] = None) -> bool:
        """Run one reconnect attempt when its due-time has arrived."""
        probe = now or datetime.now()
        next_attempt_at = reconnect_state.get("next_attempt_at") or probe
        if probe < next_attempt_at:
            return False

        max_attempts = max(1, int(getattr(self.pipeline_config, "reconnect_max_attempts", 5) or 5))
        retry_seconds = max(1, int(getattr(self.pipeline_config, "reconnect_attempt_interval_seconds", 5) or 5))
        cooldown_seconds = max(1, int(getattr(self.pipeline_config, "reconnect_failure_retry_seconds", 30) or 30))

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
            reconnect_state["next_attempt_at"] = probe + timedelta(seconds=cooldown_seconds)
        else:
            reconnect_state["attempts"] = attempt_number
            reconnect_state["next_attempt_at"] = probe + timedelta(seconds=retry_seconds)
        return False
    
    def run_trading(self, 
                    enable_trading: bool = True,
                    close_on_opposite_signal: bool = False):
        """
        Run live trading loop.
        """
        if not self.mt5 or not self.mt5.is_connected():
            self.logger.error("MT5 not connected. Cannot run live trading.")
            return
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("STARTING LIVE TRADING")
        self.logger.info(f"Trading enabled: {enable_trading}")
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
                self.logger.info("regime_params: no fallbacks recorded (all observed (symbol,TF) pairs tuned)")
            else:
                preview = ", ".join(f"{s}/{tf}" for (s, tf) in sorted(fallbacks.keys())[:10])
                more = "" if len(fallbacks) <= 10 else f" (+{len(fallbacks) - 10} more)"
                self.logger.info(f"regime_params: {len(fallbacks)} fallback(s) -> {preview}{more}")
        except Exception as exc:
            self.logger.debug(f"regime_params fallback summary unavailable: {exc}")
        self.logger.info("=" * 60)
        
        # Check entry eligibility. Existing positions may still require PM-side
        # margin/governance management even after the entry ledger has expired.
        live_eligible_configs = self.portfolio_manager.get_live_eligible_configs(log_rejections=True)
        if not live_eligible_configs:
            open_positions = None
            try:
                open_positions = self.mt5.get_positions()
            except Exception as exc:
                self.logger.debug(f"Open-position check unavailable during live startup: {exc}")
            if open_positions:
                self.logger.warning(
                    "No live-eligible configurations for new entries, but open positions exist. "
                    "Starting in management-only mode until live eligibility is restored."
                )
            else:
                self.logger.warning("No live-eligible configurations. Run optimization or review live eligibility policy first.")
                return
        
        # Create trader
        self.trader = LiveTrader(
            mt5_connector=self.mt5,
            portfolio_manager=self.portfolio_manager,
            position_config=self.position_config,
            enable_trading=enable_trading,
            close_on_opposite_signal=close_on_opposite_signal,
            pipeline_config=self.pipeline_config,
            storage_manager=self.storage_manager,
        )
        self.trader._running = True

        live_loop_trigger_mode = str(
            getattr(self.pipeline_config, "live_loop_trigger_mode", "bar") or "bar"
        ).strip().lower()
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
        live_loop_trigger_mode = loop_aliases.get(live_loop_trigger_mode, live_loop_trigger_mode)
        if live_loop_trigger_mode not in {"bar", "scheduled"}:
            self.logger.warning(
                f"Invalid live_loop_trigger_mode={live_loop_trigger_mode!r}; using bar trigger."
            )
            live_loop_trigger_mode = "bar"
        try:
            live_bar_poll_seconds = max(
                0.05,
                float(getattr(self.pipeline_config, "live_bar_poll_seconds", 0.25) or 0.25),
            )
        except Exception:
            try:
                live_bar_poll_seconds = max(
                    0.05,
                    float(getattr(self.pipeline_config, "live_tick_poll_seconds", 0.25) or 0.25),
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

        next_retrain_check_at = datetime.now()
        next_sweep_at = datetime.now()
        reconnect_state: Dict[str, Any] = {
            "attempts": 0,
            "next_attempt_at": datetime.now(),
        }
        shutdown_event = self.trader._shutdown_event
        retrain_check_interval = timedelta(
            seconds=max(1, int(getattr(self.pipeline_config, "production_retrain_poll_seconds", 60) or 60))
        )
        
        try:
            while not shutdown_event.is_set():
                now = datetime.now()
                if not self.mt5.is_connected():
                    self._attempt_reconnect(reconnect_state, now=now)
                    self._wait_for_next_due(
                        shutdown_event,
                        reconnect_state.get("next_attempt_at", now + timedelta(seconds=1)),
                        now=now,
                    )
                    continue

                reconnect_state["attempts"] = 0
                reconnect_state["next_attempt_at"] = now

                if now >= next_retrain_check_at:
                    self._check_production_retrain_schedule()
                    next_retrain_check_at = datetime.now() + retrain_check_interval

                if live_loop_trigger_mode == "bar":
                    try:
                        live_configs = self.trader._get_live_configs()
                        changed_by_symbol = self.trader.get_symbols_with_new_bars(live_configs)
                        processed = self.trader.process_all_symbols(
                            symbols_filter=set(changed_by_symbol.keys()),
                            timeframes_filter=changed_by_symbol,
                        )
                        if processed:
                            self.trader.commit_bar_probe_times(changed_by_symbol)
                    except Exception as e:
                        self.logger.error(f"Error in trading loop: {e}")

                    next_idle_at = datetime.now() + timedelta(seconds=live_bar_poll_seconds)
                    next_due = min(next_retrain_check_at, next_idle_at)
                    self._wait_for_next_due(shutdown_event, next_due, now=datetime.now())
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
    
    def _fetch_historical_data(self, symbols: List[str] = None):
        """Fetch historical data from MT5 and save to data directory."""
        if not self.mt5 or not self.mt5.is_connected():
            return
        
        symbols = symbols or self.symbols
        
        self.logger.info(f"Fetching historical data for {len(symbols)} symbols...")
        
        for symbol in symbols:
            broker_symbol = self.mt5.find_broker_symbol(symbol)
            if broker_symbol is None:
                self.logger.warning(f"Symbol not found: {symbol}")
                continue

            filepath = self.data_dir / f"{symbol}_M5.csv"
            bars_to_fetch = int(getattr(self.pipeline_config, "max_bars", 300000))
            overlap_minutes = int(getattr(self.pipeline_config, "storage_delta_sync_overlap_minutes", 1440))
            existing_df: Optional[pd.DataFrame] = None
            bars: Optional[pd.DataFrame] = None

            if filepath.exists():
                try:
                    existing_df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                    existing_df.index = pd.to_datetime(existing_df.index)
                    existing_df = existing_df.sort_index()
                except Exception as exc:
                    self.logger.debug(f"[{symbol}] Existing history reload failed, refetching full file: {exc}")
                    existing_df = None

            if existing_df is not None and not existing_df.empty:
                start_date = existing_df.index[-1] - timedelta(minutes=max(1, overlap_minutes))
                bars = self.mt5.get_bars_range(broker_symbol, "M5", start_date, datetime.now())
                if bars is not None and len(bars) > 0:
                    bars = pd.concat([existing_df, bars])
                else:
                    bars = existing_df
            else:
                bars = self.mt5.get_bars(broker_symbol, "M5", count=bars_to_fetch)

            if bars is None or len(bars) == 0:
                self.logger.warning(f"  {symbol}: No data available")
                continue

            bars = bars[~bars.index.duplicated(keep="last")].sort_index()
            if bars_to_fetch > 0 and len(bars) > bars_to_fetch:
                bars = bars.iloc[-bars_to_fetch:]
            bars.to_csv(filepath, index_label="time")
            self.logger.info(f"  {symbol}: {len(bars)} bars saved")

        data_loader = getattr(getattr(self.portfolio_manager, "pipeline", None), "data_loader", None)
        if isinstance(data_loader, DataLoader):
            data_loader.clear_cache()
    
    def _save_results(self, results: Dict):
        """Save optimization results."""
        summary = []
        for symbol, result in results.items():
            if result.success and result.config:
                summary.append({
                    'symbol': symbol,
                    'strategy': result.config.strategy_name,
                    'timeframe': result.config.timeframe,
                    'score': result.config.composite_score,
                    'validated': result.config.is_validated,
                    'valid_until': result.config.valid_until.isoformat() if result.config.valid_until else "",
                    'train_trades': result.config.train_metrics.get('total_trades', 0),
                    'train_win_rate': result.config.train_metrics.get('win_rate', 0),
                    'val_trades': result.config.val_metrics.get('total_trades', 0),
                    'val_win_rate': result.config.val_metrics.get('win_rate', 0),
                })
        
        summary_df = pd.DataFrame(summary)
        summary_path = self.output_dir / "optimization_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        self.logger.info(f"Saved optimization summary to {summary_path}")
    
    def shutdown(self):
        """Shutdown the application."""
        self.logger.info("Shutting down...")
        
        if self.trader:
            self.trader.stop()
            
            # Save trade log
            log_path = self.output_dir / f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
        '--config',
        type=str,
        default='config.json',
        help='Path to config JSON (default: config.json)'
    )

    parser.add_argument(
        '--optimize',
        action='store_true',
        help='Run initial optimization'
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Force re-optimization of all symbols (ignore valid configs)'
    )
    parser.add_argument(
        '--trade',
        action='store_true',
        help='Start live trading'
    )
    parser.add_argument(
        '--paper',
        action='store_true',
        help='Paper trading mode (no real trades)'
    )
    parser.add_argument(
        '--close-on-opposite-signal',
        action='store_true',
        help='Close open positions when an opposite signal appears (default: disabled)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        default=None,
        help='Symbols to trade (default: all)'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=None,
        help='Optional override for data directory (defaults to config.json)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Optional override for output directory (defaults to config.json)'
    )
    parser.add_argument(
        '--winner-ledger',
        type=str,
        default=None,
        help='Optional override for the validated-winner ledger file (defaults to config.json or pm_configs.json)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Print current status and exit'
    )
    
    args = parser.parse_args()

    # Load runtime configuration before logger setup so config paths remain authoritative.
    config_data = load_config_json(args.config)
    os.environ["PM_CONFIG_PATH"] = args.config

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

    pipeline_config = PipelineConfig(**_filter_dataclass_kwargs(PipelineConfig, pipeline_section))
    position_config = PositionConfig(**_filter_dataclass_kwargs(PositionConfig, config_data.get("position", {})))
    mt5_config = MT5Config(**_filter_dataclass_kwargs(MT5Config, mt5_section))

    # If position risk not set explicitly, inherit from pipeline risk (backward compatible)
    if "risk_per_trade_pct" not in (config_data.get("position") or {}) and hasattr(pipeline_config, "risk_per_trade_pct"):
        position_config.risk_per_trade_pct = pipeline_config.risk_per_trade_pct
    pipeline_config.regime_tp_multipliers = dict(
        getattr(position_config, "regime_tp_multipliers", {}) or {}
    )

    if args.data_dir:
        pipeline_config.data_dir = Path(args.data_dir)
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
        set_broker_specs_path(broker_specs_path)
        specs_loaded = {}
        for attempt in range(1, 4):
            # Force reload on each attempt
            set_broker_specs_path(broker_specs_path)
            specs_loaded = load_broker_specs(broker_specs_path)
            if specs_loaded:
                break
        if not specs_loaded and os.path.exists(broker_specs_path):
            logger.warning(f"Broker specs file found but empty/unreadable after 3 attempts: {broker_specs_path}")
    set_instrument_specs(
        specs=config_data.get("instrument_specs"),
        defaults=config_data.get("instrument_spec_defaults")
    )

    log_resolved_config_summary(logger, args.config, config_data, pipeline_config, position_config, mt5_config)

    _app_instance = FXPortfolioManagerApp(
        symbols=symbols,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        pipeline_config=pipeline_config,
        position_config=position_config,
        mt5_config=mt5_config,
        config_file=str(getattr(pipeline_config, "winner_ledger_path", "pm_configs.json")),
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
        
        # Run optimization
        if args.optimize:
            if not _app_instance.run_optimization(overwrite=args.overwrite):
                logger.warning("Optimization produced no valid configs")
        
        # Run trading
        if args.trade:
            _app_instance.run_trading(
                enable_trading=not args.paper,
                close_on_opposite_signal=args.close_on_opposite_signal
            )
        
        # Default: show status
        if not args.optimize and not args.trade:
            _app_instance.print_status()
            print("\nUsage:")
            print("  python pm_main.py --optimize              # Run optimization (skip valid)")
            print("  python pm_main.py --optimize --overwrite  # Force re-optimize all")
            print("  python pm_main.py --trade                 # Start live trading")
            print("  python pm_main.py --trade --paper         # Paper trading")
            print(f"  production retrain schedule: {pipeline_config.describe_retrain_schedule()}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Application error: {e}")
        return 1
    finally:
        if _app_instance:
            _app_instance.shutdown()


if __name__ == "__main__":
    sys.exit(main())
