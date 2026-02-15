"""
FX Portfolio Manager - Main Application
========================================

Main entry point for the FX Portfolio Manager.

This application:
1. Loads or optimizes strategy configurations for all symbols
2. Monitors markets for entry signals on each bar close
3. Executes trades based on optimized strategies
4. Automatically retrains when retrain periods expire
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
    python pm_main.py --trade --auto-retrain  # Live trading with auto-retraining
    python pm_main.py --status                # Show current status

Version: 3.1 (Portfolio Manager with Stateful Optimization)
"""

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import time
import threading
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field, fields

import pandas as pd
import numpy as np

# Import PM modules

def _filter_dataclass_kwargs(cls, data: Dict[str, Any]) -> Dict[str, Any]:
    """Filter dict keys to those accepted by a dataclass constructor."""
    allowed = {f.name for f in fields(cls)}
    return {k: v for k, v in (data or {}).items() if k in allowed}

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
            f"risk={getattr(p, 'risk_per_trade_pct', None)}% | "
            f"d1+lower={getattr(p, 'allow_d1_plus_lower_tf', None)} | "
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
            f"optuna_val_obj={bool(getattr(p, 'optuna_use_val_in_objective', False))}"
        )

        # Position config (risk + execution safety)
        pos_cfg = position_config
        logger.info(
            "  position: "
            f"risk={getattr(pos_cfg, 'risk_per_trade_pct', None)}% | "
            f"basis={getattr(pos_cfg, 'risk_basis', None)} | "
            f"max_risk={getattr(pos_cfg, 'max_risk_pct', None)}% | "
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
from pm_strategies import StrategyRegistry, BaseStrategy
from pm_position import PositionConfig, PositionManager, PositionCalculator
from pm_pipeline import PortfolioManager, SymbolConfig
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
    file_handler = logging.FileHandler(log_path)
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
    # Added Nordic + TRY crosses
    "EURNOK", "EURSEK", "EURDKK", "GBPNOK", "GBPSEK", "EURTRY",
    # Commodities (Metals + Energy)
    "XAUUSD", "XAGUSD", "XAUEUR", "XAUGBP", "XAUAUD", "XAGEUR", "XRX", "XTIUSD", "XBRUSD", "XNGUSD",
    # Indices
    "US100", "US30", "DE30", "EU50", "UK100", "JP225",
    "US500", "FR40", "ES35", "HK50", "AU200",
    # Crypto (CFDs)
    "BTCUSD", "ETHUSD", "LTCUSD", "SOLUSD", "BCHUSD",
    "DOGUSD", "TRXUSD", "XRPUSD", "TONUSD", "BTCETH", "GBXUSD", "BTCXAU",
]


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

    def __init__(self, log_path: str = "last_trade_log.json"):
        self._log_path = log_path
        # symbol → DecisionRecord
        self._cache: Dict[str, DecisionRecord] = {}
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
            tmp_path = f"{self._log_path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, self._log_path)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                f"DecisionThrottle: failed to save cache: {exc}"
            )

    def _load(self) -> None:
        """Load cache from ``last_trade_log.json`` if it exists."""
        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for sym, rec_dict in data.items():
                decision_keys = rec_dict.get("decision_keys")
                if not isinstance(decision_keys, list):
                    dk = rec_dict.get("decision_key", "")
                    decision_keys = [dk] if dk else []
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
            tmp_path = f"{self._log_path}.tmp"
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
                 pipeline_config: 'PipelineConfig' = None):
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
        
        # Position management
        self.position_manager = PositionManager(position_config)
        self.position_calc = PositionCalculator(position_config)
        
        # State
        self._running = False
        self._shutdown_event = threading.Event()
        self._last_bar_times: Dict[str, datetime] = {}
        self._last_order_times: Dict[str, datetime] = {}  # For rate limiting
        self._unknown_tf_position_warnings: Set[int] = set()
        
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

        # Margin protection state (updated each cycle by _run_margin_protection_cycle)
        self._margin_state: str = "NORMAL"
        
        # Cache statistics for monitoring
        self._cache_hits = 0
        self._cache_misses = 0
        self._max_cache_size = 100  # Limit cache to prevent memory bloat
        
        self.logger = logging.getLogger(__name__)

        # Synchronize instrument specs from live MT5 metadata when connected.
        if self.mt5 and self.mt5.is_connected():
            self.logger.info("Synchronizing instrument specs from MT5...")
            sync_count = 0
            fail_count = 0

            for symbol in self.pm.symbols:
                broker_symbol = self.mt5.find_broker_symbol(symbol)
                if not broker_symbol:
                    self.logger.warning(f"[{symbol}] Broker symbol not found; using config spec defaults")
                    fail_count += 1
                    continue

                mt5_info = self.mt5.get_symbol_info(broker_symbol)
                if not mt5_info:
                    self.logger.warning(f"[{symbol}] MT5 symbol info unavailable; using config spec defaults")
                    fail_count += 1
                    continue

                spec = get_instrument_spec(symbol)
                old_tick_value = spec.tick_value
                old_volume_step = spec.volume_step
                old_spread = spec.spread_avg

                sync_instrument_spec_from_mt5(spec, mt5_info)

                self.logger.info(
                    f"[{symbol}] MT5 sync: tick_value={spec.tick_value:.4f} (was {old_tick_value:.4f}), "
                    f"volume_step={spec.volume_step} (was {old_volume_step}), "
                    f"spread={spec.spread_avg:.2f}p (was {old_spread:.2f}p), "
                    f"min_lot={spec.min_lot}, max_lot={spec.max_lot}"
                )
                sync_count += 1

            self.logger.info(f"MT5 spec sync complete: {sync_count} synced, {fail_count} unavailable")
        else:
            self.logger.warning("MT5 not connected; using config instrument specs only")
    
    def _prune_cache(self):
        """Prune cache if it exceeds max size (LRU-style, just clear oldest)."""
        if len(self._candidate_cache) > self._max_cache_size:
            # Remove half the entries (oldest first by insertion order in Python 3.7+)
            keys_to_remove = list(self._candidate_cache.keys())[:len(self._candidate_cache) // 2]
            for k in keys_to_remove:
                del self._candidate_cache[k]
            self.logger.debug(f"Pruned feature cache: removed {len(keys_to_remove)} entries")
    
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

    def _infer_position_timeframe(self, symbol: str, config: SymbolConfig, position: MT5Position) -> Optional[str]:
        """
        Infer a position timeframe from comment first, then from magic.

        Some brokers trim/alter comments, which can make comment-only decoding
        unreliable for D1+lower-TF secondary trade logic.
        """
        from pm_position import TradeTagEncoder

        try:
            pos_ticket = int(getattr(position, "ticket", 0) or 0)
        except (TypeError, ValueError):
            pos_ticket = 0
        try:
            pos_magic = int(getattr(position, "magic", 0) or 0)
        except (TypeError, ValueError):
            pos_magic = 0

        # Optional explicit overrides for legacy positions that carry no timeframe
        # metadata in comment/magic (for example old PM_* comments).
        overrides = getattr(getattr(self, "pipeline_config", None), "position_timeframe_overrides", None) or {}
        if isinstance(overrides, dict) and (pos_ticket > 0 or pos_magic > 0):
            key_candidates = []
            if pos_ticket > 0:
                key_candidates.extend([str(pos_ticket), f"ticket:{pos_ticket}", f"{symbol}:{pos_ticket}"])
            if pos_magic > 0:
                key_candidates.extend([str(pos_magic), f"magic:{pos_magic}", f"{symbol}:{pos_magic}"])
            for key in key_candidates:
                tf_override = overrides.get(key)
                if tf_override:
                    return str(tf_override).upper()

        comment = getattr(position, "comment", "") or ""
        tf_from_comment = TradeTagEncoder.get_timeframe_from_comment(comment)
        if tf_from_comment:
            return str(tf_from_comment).upper()

        # Backward compatibility for very old "PM_<strategy_tag>" comments:
        # if the strategy tag maps to exactly one timeframe in current config,
        # infer that timeframe conservatively.
        decoded_comment = TradeTagEncoder.decode_comment(comment) or {}
        strategy_tag = decoded_comment.get("strategy_tag")
        if strategy_tag:
            def _norm(text: Any) -> str:
                return "".join(ch for ch in str(text).lower() if ch.isalnum())

            tag_norm = _norm(strategy_tag)
            matched_timeframes = set()
            if tag_norm:
                try:
                    if config.has_regime_configs():
                        for timeframe, regime_map in (config.regime_configs or {}).items():
                            if not isinstance(regime_map, dict):
                                continue
                            for rc in regime_map.values():
                                strategy_name = getattr(rc, "strategy_name", "")
                                strategy_norm = _norm(strategy_name)
                                if tag_norm in strategy_norm or strategy_norm in tag_norm:
                                    matched_timeframes.add(str(timeframe).upper())
                                    break
                except Exception:
                    matched_timeframes.clear()

                # Legacy single-strategy fallback
                legacy_strategy_norm = _norm(getattr(config, "strategy_name", ""))
                legacy_timeframe = str(getattr(config, "timeframe", "") or "").upper()
                if legacy_timeframe and (tag_norm in legacy_strategy_norm or legacy_strategy_norm in tag_norm):
                    matched_timeframes.add(legacy_timeframe)

            if len(matched_timeframes) == 1:
                return next(iter(matched_timeframes))

        if pos_magic <= 0:
            return None

        matched_timeframes = set()

        try:
            if config.has_regime_configs():
                for timeframe, regime_map in (config.regime_configs or {}).items():
                    if not isinstance(regime_map, dict):
                        continue
                    for regime in regime_map.keys():
                        if TradeTagEncoder.encode_magic(symbol, timeframe, regime) == pos_magic:
                            matched_timeframes.add(str(timeframe).upper())
                            break
        except Exception:
            pass

        legacy_timeframe = str(getattr(config, "timeframe", "") or "").upper()
        if legacy_timeframe:
            try:
                if TradeTagEncoder.encode_magic(symbol, legacy_timeframe, "LEGACY") == pos_magic:
                    matched_timeframes.add(legacy_timeframe)
            except Exception:
                pass

        if len(matched_timeframes) == 1:
            return next(iter(matched_timeframes))

        return None
    
    def _check_portfolio_risk_cap(self,
                                  canonical_symbol: str,
                                  new_trade_risk_pct: float,
                                  broker_symbol: str) -> tuple[bool, str]:
        """
        Enforce max_combined_risk_pct across all open positions on this symbol.
        """
        from pm_position import TradeTagEncoder

        max_combined = float(getattr(self.pipeline_config, 'max_combined_risk_pct', 3.0))
        account_info = self.mt5.get_account_info()
        if not account_info:
            return False, "Cannot get account info"
        equity = float(account_info.equity)
        if equity <= 0:
            return False, "Zero equity"

        existing_risk_pct = 0.0
        position_details: List[str] = []

        def _estimate_position_risk_pct(position) -> float:
            sl = float(getattr(position, 'sl', 0) or 0)
            entry = float(getattr(position, 'price_open', 0) or 0)
            volume = float(getattr(position, 'volume', 0) or 0)
            if sl > 0 and entry > 0 and volume > 0:
                spec = get_instrument_spec(canonical_symbol)
                if spec.pip_size > 0:
                    sl_pips = abs(entry - sl) / spec.pip_size
                    risk_amount = sl_pips * spec.pip_value * volume
                    return (risk_amount / equity) * 100.0
            return float(getattr(self.position_config, "risk_per_trade_pct", 1.0))

        for pos in self.mt5.get_positions(symbol=broker_symbol):
            comment = getattr(pos, 'comment', '') or ''
            metadata = TradeTagEncoder.decode_comment(comment)

            if metadata and metadata.get('symbol', '') == canonical_symbol:
                pos_risk_pct = float(metadata.get('risk_pct', 0.0) or 0.0)
                if pos_risk_pct <= 0.0:
                    pos_risk_pct = _estimate_position_risk_pct(pos)
                pos_tf = str(metadata.get('timeframe', '?'))
                pos_direction = str(metadata.get('direction', '?'))
                existing_risk_pct += pos_risk_pct
                position_details.append(f"{pos_tf}:{pos_direction}={pos_risk_pct:.2f}%")
                continue

            pos_risk_pct = _estimate_position_risk_pct(pos)
            existing_risk_pct += pos_risk_pct

        total_risk_pct = existing_risk_pct + float(new_trade_risk_pct)
        if total_risk_pct > max_combined:
            details = ", ".join(position_details) if position_details else "unlabeled positions"
            return False, (
                f"Symbol risk cap exceeded for {canonical_symbol}: "
                f"existing {existing_risk_pct:.2f}% ({details}) + new {new_trade_risk_pct:.2f}% "
                f"= {total_risk_pct:.2f}% > max {max_combined:.2f}%"
            )

        return True, (
            f"Symbol risk OK for {canonical_symbol}: {total_risk_pct:.2f}% / {max_combined:.2f}%"
        )

    # -----------------------------------------------------------------
    # Margin protection (black-swan guard), configured via PipelineConfig thresholds.
    # -----------------------------------------------------------------

    def _classify_margin_state(self, margin_level: float) -> str:
        """Classify current margin level into an operating state.

        States (from configured margin thresholds):
            NORMAL   – margin_level >= entry_block_level (default 100%)
            BLOCKED  – recovery_start <= margin_level < entry_block_level
            RECOVERY – panic_level <= margin_level < recovery_start
            PANIC    – margin_level < panic_level
        """
        cfg = self.pipeline_config
        entry_block = float(getattr(cfg, 'margin_entry_block_level', 100.0))
        recovery_start = float(getattr(cfg, 'margin_recovery_start_level', 80.0))
        panic = float(getattr(cfg, 'margin_panic_level', 65.0))

        if margin_level >= entry_block:
            return "NORMAL"
        if margin_level >= recovery_start:
            return "BLOCKED"
        if margin_level >= panic:
            return "RECOVERY"
        return "PANIC"

    def _run_margin_protection_cycle(self):
        """Run one cycle of margin protection (entry block + forced deleveraging).

        Called once at the start of each ``_process_all_symbols()`` iteration.
        Sets ``self._margin_state`` which the entry gate reads later.
        """
        cfg = self.pipeline_config
        acct = self.mt5.get_account_info()
        if acct is None:
            # Cannot determine margin – assume worst-case to protect account.
            self._margin_state = "BLOCKED"
            self.logger.warning("MARGIN: account info unavailable – blocking entries this cycle")
            return

        ml = float(getattr(acct, 'margin_level', 0.0) or 0.0)

        # margin_level == 0 means no open positions (margin used is 0, level undefined).
        # Treat as unrestricted.
        if ml == 0:
            self._margin_state = "NORMAL"
            return

        state = self._classify_margin_state(ml)
        prev_state = getattr(self, '_margin_state', "NORMAL")

        # Log state transitions.
        if state != prev_state:
            pos_count = len(self.mt5.get_positions() or [])
            self.logger.warning(
                f"MARGIN STATE CHANGE: {prev_state} -> {state} | "
                f"margin_level={ml:.1f}% | open_positions={pos_count}"
            )

        self._margin_state = state

        if state in ("NORMAL", "BLOCKED"):
            # BLOCKED: no forced closures, entries will be gated later.
            return

        # --- RECOVERY or PANIC: attempt forced deleveraging ---
        positions = self.mt5.get_positions()
        if not positions:
            return

        max_closes = int(getattr(cfg, 'margin_panic_closes_per_cycle', 3)) if state == "PANIC" \
            else int(getattr(cfg, 'margin_recovery_closes_per_cycle', 1))

        # Sort candidates: losers first (most negative profit), then largest volume.
        losers = [p for p in positions if float(getattr(p, 'profit', 0.0)) < 0.0]
        if losers:
            ordered = sorted(
                losers,
                key=lambda p: (float(getattr(p, 'profit', 0.0)),
                               -float(getattr(p, 'volume', 0.0)))
            )
        elif state == "PANIC":
            # Panic fallback: close largest positions even if profitable.
            ordered = sorted(
                positions,
                key=lambda p: -float(getattr(p, 'volume', 0.0))
            )
        else:
            # RECOVERY with no losers – nothing safe to close.
            ordered = []

        closed = 0
        for pos in ordered:
            if closed >= max_closes:
                break

            ticket = getattr(pos, 'ticket', '?')
            symbol = getattr(pos, 'symbol', '?')
            profit = float(getattr(pos, 'profit', 0.0))
            volume = float(getattr(pos, 'volume', 0.0))

            self.logger.warning(
                f"MARGIN_FORCE_CLOSE: attempting close | ticket={ticket} "
                f"symbol={symbol} profit={profit:.2f} volume={volume}"
            )

            result = self.mt5.close_position(pos, deviation=30, comment="PM_MARGIN_PROTECT")
            if result and result.success:
                closed += 1
                self.logger.warning(
                    f"MARGIN_FORCE_CLOSE: SUCCESS | ticket={ticket} symbol={symbol}"
                )
            else:
                err = getattr(result, 'comment', 'unknown') if result else 'no result'
                self.logger.error(
                    f"MARGIN_FORCE_CLOSE: FAILED | ticket={ticket} symbol={symbol} error={err}"
                )

            # Re-check margin after each close – stop if recovered.
            acct2 = self.mt5.get_account_info()
            if acct2:
                ml2 = float(getattr(acct2, 'margin_level', 0.0) or 0.0)
                reopen = float(getattr(cfg, 'margin_reopen_level', 100.0))
                if ml2 > 0 and ml2 >= reopen:
                    self.logger.info(
                        f"MARGIN: recovered to {ml2:.1f}% (>= {reopen}%) – "
                        f"stopping forced closures ({closed} closed this cycle)"
                    )
                    self._margin_state = self._classify_margin_state(ml2)
                    break

        if closed > 0:
            self.logger.warning(
                f"MARGIN: forced {closed} closure(s) this cycle in {state} mode"
            )

    def start(self):
        """Start the trading loop."""
        self._running = True
        self._shutdown_event.clear()
        
        self.logger.info("Starting live trading loop...")
        self.logger.info(f"Trading enabled: {self.enable_trading}")
        self.logger.info(f"Symbols: {len(self.pm.symbols)}")
        self.logger.info(f"Validated configs: {len(self.pm.get_validated_configs())}")
        
        while self._running and not self._shutdown_event.is_set():
            try:
                # Check connection
                if not self.mt5.is_connected():
                    self.logger.warning("Lost MT5 connection, attempting reconnect...")
                    if not self._reconnect():
                        time.sleep(10)
                        continue
                
                # Process each symbol
                self._process_all_symbols()
                
                # Sleep until next check
                time.sleep(1)
                
            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                self.logger.exception(f"Error in trading loop: {e}")
                time.sleep(5)
        
        self.logger.info("Trading loop stopped")
    
    def stop(self):
        """Stop the trading loop."""
        self.logger.info("Stopping trader...")
        self._running = False
        self._shutdown_event.set()
    
    def _process_all_symbols(self):
        """Process all symbols for signals."""
        # --- Margin protection: evaluate once per cycle ---
        self._run_margin_protection_cycle()

        validated_configs = self.pm.get_validated_configs()

        for symbol, config in validated_configs.items():
            try:
                self._process_symbol(symbol, config)
            except Exception as e:
                self.logger.error(f"[{symbol}] Error: {e}")
    
    def _process_symbol(self, symbol: str, config: SymbolConfig):
        """
        Process a single symbol with regime-aware strategy selection.
        
        For each timeframe:
        1. Check for new bar
        2. Compute features (including regime)
        3. Select best (tf, regime, strategy) based on strength * quality_score * freshness
        4. Generate signal and execute if conditions met
        
        FIX #2: D1 + Lower-TF Dual Trade Logic:
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
        
        # ===== FIX #2: D1 + Lower-TF Position Analysis =====
        all_positions = self.mt5.get_positions()
        symbol_positions = [p for p in all_positions if p.symbol == broker_symbol]
        
        # Check if D1+lower-TF mode is enabled
        allow_d1_plus_lower = getattr(self.pipeline_config, 'allow_d1_plus_lower_tf', True) if self.pipeline_config else True
        
        # Analyze existing positions
        has_d1_position = False
        has_non_d1_position = False
        d1_position_direction = None  # Track direction for logging
        has_unknown_position_timeframe = False

        for pos in symbol_positions:
            pos_tf = self._infer_position_timeframe(symbol, config, pos)

            if pos_tf == 'D1':
                has_d1_position = True
                d1_position_direction = "LONG" if pos.type == 0 else "SHORT"  # 0=BUY, 1=SELL
            elif pos_tf:
                has_non_d1_position = True
            else:
                has_unknown_position_timeframe = True
                ticket = int(getattr(pos, "ticket", 0) or 0)
                if ticket not in self._unknown_tf_position_warnings:
                    comment_preview = (getattr(pos, "comment", "") or "").strip()
                    if len(comment_preview) > 48:
                        comment_preview = f"{comment_preview[:45]}..."
                    self.logger.info(
                        f"[{symbol}] Existing position metadata incomplete "
                        f"(ticket={ticket or '?'}, magic={getattr(pos, 'magic', '?')}, "
                        f"comment={comment_preview!r}); cannot infer timeframe"
                    )
                    self._unknown_tf_position_warnings.add(ticket)
        
        # Determine if we can open a new trade and what constraints apply
        can_open_trade = True
        allowed_timeframes = None  # None = all timeframes allowed
        is_secondary_trade = False
        block_reason = None
        
        num_positions = len(symbol_positions)
        
        if num_positions >= 2:
            # Two trades max per symbol
            can_open_trade = False
            block_reason = "2 positions already open"
            
        elif num_positions == 1:
            if allow_d1_plus_lower and has_unknown_position_timeframe:
                # Avoid inconsistent state where a trade is selected but then blocked
                # later by magic-level duplicate checks.
                can_open_trade = False
                block_reason = "Position timeframe unknown; blocking secondary trade"
            elif allow_d1_plus_lower and has_d1_position:
                # D1 position open - allow one additional non-D1 trade
                can_open_trade = True
                allowed_timeframes = ['M5', 'M15', 'M30', 'H1', 'H4']  # Exclude D1
                is_secondary_trade = True
                self.logger.debug(f"[{symbol}] D1 trade open ({d1_position_direction}); allowing secondary non-D1 trade")
            elif allow_d1_plus_lower and has_non_d1_position:
                # Non-D1 position open - allow one additional D1 trade
                can_open_trade = True
                allowed_timeframes = ['D1']
                is_secondary_trade = True
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
        candidates, eval_stats = self._evaluate_regime_candidates(symbol, broker_symbol, config)
        
        if not candidates:
            if eval_stats:
                self.logger.debug(
                    f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
                    f"{eval_stats.get('timeframes_total', 0)} | winners=0 | actionable=0 "
                    f"(no_winner={eval_stats.get('no_winner', 0)}, "
                    f"failed_gate={eval_stats.get('winner_failed_gate', 0)}, "
                    f"insufficient_bars={eval_stats.get('insufficient_bars', 0)})"
                )
            return
        
        # ===== FIX #2: Filter candidates based on allowed timeframes =====
        if allowed_timeframes is not None:
            original_count = len(candidates)
            candidates = [c for c in candidates if c['timeframe'] in allowed_timeframes]
            if len(candidates) < original_count:
                self.logger.debug(f"[{symbol}] Filtered out D1 candidates (secondary trade must be non-D1)")
        
        if not candidates:
            self.logger.debug(f"[{symbol}] No valid candidates after timeframe constraints (secondary trade filter)")
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
        
        best_overall = max(candidates, key=lambda c: c['selection_score'])
        
        best_overall_score = float(best_overall.get('selection_score', 0.0))

        
        margin = float(getattr(self.pipeline_config, 'actionable_score_margin', 0.95) or 0.95)
        
        # Clamp margin defensively: [0.0, 1.0]
        
        if margin < 0.0:
        
            margin = 0.0
        
        if margin > 1.0:
        
            margin = 1.0

        
        actionable = [c for c in candidates if int(c.get('signal', 0)) != 0]
        self.logger.debug(
            f"[{symbol}] Winners-only eval: tfs={eval_stats.get('timeframes_evaluated', 0)}/"
            f"{eval_stats.get('timeframes_total', 0)} | winners={len(candidates)} | "
            f"actionable={len(actionable)}"
        )
        
        if not actionable:
        
            # No actionable winner signals on any timeframe for this bar.
        
            bar_time_iso = str(best_overall.get('bar_time', ''))
        
            dk = DecisionThrottle.make_decision_key(
        
                symbol, best_overall['strategy_name'], best_overall['timeframe'],
        
                best_overall['regime'], 0, bar_time_iso
        
            )
        
            self._decision_throttle.record_decision(
        
                symbol=symbol, decision_key=dk,
        
                bar_time_iso=bar_time_iso,
        
                timeframe=best_overall['timeframe'], regime=best_overall['regime'],
        
                strategy_name=best_overall['strategy_name'], direction=0,
        
                action="NO_ACTIONABLE_WINNER_SIGNAL",
        
            )
            self.logger.debug(f"[{symbol}] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)")
        
            return

        
        min_score = best_overall_score * margin
        
        eligible = [c for c in actionable if float(c.get('selection_score', 0.0)) >= min_score]
        
        if not eligible:
        
            # There were signals, but none were close enough to the best overall score.
        
            bar_time_iso = str(best_overall.get('bar_time', ''))
        
            dk = DecisionThrottle.make_decision_key(
        
                symbol, best_overall['strategy_name'], best_overall['timeframe'],
        
                best_overall['regime'], 0, bar_time_iso
        
            )
        
            self._decision_throttle.record_decision(
        
                symbol=symbol, decision_key=dk,
        
                bar_time_iso=bar_time_iso,
        
                timeframe=best_overall['timeframe'], regime=best_overall['regime'],
        
                strategy_name=best_overall['strategy_name'], direction=0,
        
                action="NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN",
        
            )
            self.logger.debug(f"[{symbol}] NO_ACTIONABLE_SIGNAL_WITHIN_MARGIN (actionable below margin)")
        
            return

        
        # Choose the best actionable candidate (highest selection score) within the margin band.
        
        best = max(eligible, key=lambda c: c['selection_score'])
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
        spec = symbol_info.to_instrument_spec()
        
        # Create magic number for this specific trade
        # Include timeframe and regime so we can identify the config used
        magic = TradeTagEncoder.encode_magic(symbol, best['timeframe'], best['regime'])
        
        # ===== FIX #2: Log clearly if this is a secondary trade =====
        trade_type_tag = "[SECONDARY] " if is_secondary_trade else ""
        
        # Log selection
        self.logger.info(
            f"[{symbol}] {trade_type_tag}Selected: {best['strategy_name']} @ {best['timeframe']}/{best['regime']} "
            f"(strength={best['regime_strength']:.2f}, quality={best['quality_score']:.2f}, "
            f"freshness={best['freshness']:.2f}, score={best['selection_score']:.3f})"
        )
        
        if is_secondary_trade:
            self.logger.debug(f"[{symbol}] D1 trade open; allowed second trade on {best['timeframe']}")
        
        # Execute entry (passes decision_key so _execute_entry can record
        # the outcome into the throttle)
        self._execute_entry(
            symbol=broker_symbol,
            signal=int(best['signal']),
            strategy=best['strategy'],
            features=best['features'],
            spec=spec,
            magic=magic,
            config=config,
            decision_key=decision_key,
            bar_time_iso=bar_time_iso,
            best_candidate=best,
            is_secondary_trade=is_secondary_trade,
        )
    
    def _evaluate_regime_candidates(self, symbol: str, broker_symbol: str,
                                     config: SymbolConfig) -> tuple:
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
            "winner_candidates": 0,
            "no_winner": 0,
            "winner_failed_gate": 0,
            "insufficient_bars": 0,
        }
        
        # Check if config has regime configs
        if not config.has_regime_configs():
            # Fallback to legacy single-strategy mode
            return self._evaluate_legacy_candidate(symbol, broker_symbol, config), stats
        
        available_timeframes = config.get_available_timeframes()
        stats["timeframes_total"] = len(available_timeframes)
        regime_params_file = getattr(self.pipeline_config, 'regime_params_file', 'regime_params.json') if self.pipeline_config else 'regime_params.json'
        freshness_decay = getattr(self.pipeline_config, 'regime_freshness_decay', 0.85) if self.pipeline_config else 0.85
        
        # Get live quality gate thresholds from config
        min_pf = getattr(self.pipeline_config, 'regime_min_val_profit_factor', 1.0) if self.pipeline_config else 1.0
        min_return = getattr(self.pipeline_config, 'regime_min_val_return_pct', 0.0) if self.pipeline_config else 0.0
        max_dd = getattr(self.pipeline_config, 'fx_val_max_drawdown', 35.0) if self.pipeline_config else 35.0
        
        for tf in available_timeframes:
            # Get bars for this timeframe (need at least the latest bar time)
            bars = self.mt5.get_bars(broker_symbol, tf, count=int(getattr(self.pipeline_config, 'live_bars_count', 200)))
            if bars is None or len(bars) < int(getattr(self.pipeline_config, 'live_min_bars', 100)):
                stats["insufficient_bars"] += 1
                continue
            stats["timeframes_evaluated"] += 1
            
            # Check for new bar (freshness tracking)
            current_bar_time = bars.index[-1]
            cache_key = f"{symbol}_{tf}"
            last_bar_time = self._last_bar_times.get(cache_key)
            
            # Determine if this is a new bar
            is_new_bar = (last_bar_time is None or current_bar_time > last_bar_time)
            freshness = 1.0 if is_new_bar else freshness_decay
            
            # ── Feature/Signal Cache Logic ───────────────────────────────────
            # Build cache key including bar_time to ensure we invalidate on new bar
            feature_cache_key = f"{symbol}_{tf}_{current_bar_time}"
            cached = self._candidate_cache.get(feature_cache_key)
            
            if cached is not None and not is_new_bar:
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
                
                if is_new_bar:
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
                
                # Get current regime from last closed bar
                # Use index -2 because -1 is the forming bar
                if 'REGIME' not in features.columns or len(features) < 2:
                    continue
                
                current_regime = features['REGIME'].iloc[-2]
                regime_strength = features['REGIME_STRENGTH'].iloc[-2] if 'REGIME_STRENGTH' in features.columns else 0.5
                
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
                if not regime_config.is_valid_for_live(min_pf, min_return, max_dd):
                    stats["winner_failed_gate"] += 1
                    self.logger.debug(
                        f"[{symbol}] [{tf}] [{current_regime}] Winner failed live gate "
                        f"(PF={regime_config.val_metrics.get('profit_factor', 0):.2f}, "
                        f"ret={regime_config.val_metrics.get('total_return_pct', -100):.1f}%, "
                        f"dd={regime_config.val_metrics.get('max_drawdown_pct', 100):.1f}%)"
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
            quality_score = float(regime_config.quality_score) if regime_config.quality_score else 0.5
            selection_score = float(regime_strength) * quality_score * float(freshness)
            
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
                'bar_time': str(features.index[-2]) if len(features) > 1 else '',
            })
            stats["winner_candidates"] += 1
        
        return candidates, stats
    
    def _evaluate_legacy_candidate(self, symbol: str, broker_symbol: str,
                                    config: SymbolConfig) -> List[Dict]:
        """
        Fallback for legacy configs without regime_configs.
        
        Uses the single strategy_name/timeframe/parameters from config.
        """
        from pm_strategies import StrategyRegistry
        
        # Winners-only live gate for legacy configs
        min_pf = getattr(self.pipeline_config, 'regime_min_val_profit_factor', 1.0) if self.pipeline_config else 1.0
        min_return = getattr(self.pipeline_config, 'regime_min_val_return_pct', 0.0) if self.pipeline_config else 0.0
        max_dd = getattr(self.pipeline_config, 'fx_val_max_drawdown', 35.0) if self.pipeline_config else 35.0
        val_pf = config.val_metrics.get('profit_factor', 0.0)
        val_return = config.val_metrics.get('total_return_pct', -100.0)
        val_dd = config.val_metrics.get('max_drawdown_pct', 100.0)
        if not config.is_validated or val_pf < min_pf or val_return < min_return or val_dd > max_dd:
            self.logger.debug(
                f"[{symbol}] Legacy config failed live gate "
                f"(validated={config.is_validated}, PF={val_pf:.2f}, ret={val_return:.1f}%, dd={val_dd:.1f}%)"
            )
            return []
        
        timeframe = config.timeframe
        if not timeframe:
            return []
        
        # Get bars
        bars = self.mt5.get_bars(broker_symbol, timeframe, count=int(getattr(self.pipeline_config, 'live_bars_count', 200)))
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
        
        return [{
            'timeframe': timeframe,
            'regime': 'LEGACY',
            'regime_strength': 1.0,
            'quality_score': config.composite_score / 100 if config.composite_score else 0.5,
            'freshness': 1.0,
            'selection_score': 1.0,
            'strategy': strategy,
            'strategy_name': config.strategy_name,
            'signal': int(current_signal),
            'features': features,
            'regime_config': None,
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
                       is_secondary_trade: bool = False):
        """
        Execute an entry trade.
        
        Records every outcome (success, skip, failure) into the decision
        throttle so that the same signal on the same bar is never re-attempted
        or re-logged.
        
        Args:
            is_secondary_trade: If True, this is a secondary trade (D1 already open).
                                Risk will be reduced according to config.
        """
        from pm_position import TradeTagEncoder
        
        is_long = signal == 1
        direction = "LONG" if is_long else "SHORT"
        direction_label = "BUY" if is_long else "SELL"
        
        # Helper to extract throttle metadata from the best_candidate dict
        _tf = best_candidate.get('timeframe', '') if best_candidate else ''
        _regime = best_candidate.get('regime', '') if best_candidate else ''
        _strat_name = best_candidate.get('strategy_name', '') if best_candidate else ''
        
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

        selection_score = _safe_float(best_candidate.get('selection_score')) if best_candidate else None
        quality_score = _safe_float(best_candidate.get('quality_score')) if best_candidate else None
        freshness_score = _safe_float(best_candidate.get('freshness')) if best_candidate else None
        regime_strength_score = _safe_float(best_candidate.get('regime_strength')) if best_candidate else None
        decision_context: Dict[str, Any] = {
            "secondary_trade": bool(is_secondary_trade),
            "selection_score": selection_score,
            "quality_score": quality_score,
            "freshness": freshness_score,
            "regime_strength": regime_strength_score,
        }

        def _record_actionable(action: str,
                               entry_price_value: Optional[float] = None,
                               sl_value: Optional[float] = None,
                               tp_value: Optional[float] = None,
                               volume_value: Optional[float] = None,
                               target_risk_pct_value: Optional[float] = None,
                               actual_risk_pct_value: Optional[float] = None) -> None:
            """Persist actionable outcomes for dashboard consumption."""
            if not decision_key:
                return
            try:
                self._actionable_log.record(
                    symbol,
                    {
                        "symbol": symbol,
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
                        "score": selection_score,
                        "quality": quality_score,
                        "freshness": freshness_score,
                        "regime_strength": regime_strength_score,
                    },
                )
            except Exception as exc:
                self.logger.debug(f"[{symbol}] Actionable log failed: {exc}")
        
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
        existing = self.mt5.get_position_by_symbol_magic(symbol, magic)
        if existing:
            existing_comment = getattr(existing, "comment", "") or ""
            existing_tf = TradeTagEncoder.get_timeframe_from_comment(existing_comment) or "UNKNOWN"
            self.logger.info(
                f"[{symbol}] Skipping trade; position already exists for magic {magic} "
                f"(ticket={getattr(existing, 'ticket', '?')}, tf={existing_tf})"
            )
            _record_throttle("SKIPPED_POSITION_EXISTS")
            _record_actionable(action="SKIPPED_POSITION_EXISTS")
            return
        
        # Calculate stops (use MT5-derived spec for live parity)
        sl_pips, tp_pips = strategy.calculate_stops(features, signal, symbol, spec=spec)

        # Get symbol info for sizing and broker constraints
        symbol_info = self.mt5.get_symbol_info(symbol)
        if symbol_info is None:
            self.logger.warning(f"[{symbol}] Skipping trade; symbol info unavailable")
            _record_throttle("SKIPPED_NO_SYMBOL_INFO")
            _record_actionable(action="SKIPPED_NO_SYMBOL_INFO")
            return

        # Risk basis (balance/equity)
        account = self.mt5.get_account_info()
        if account is None:
            self.logger.warning(f"[{symbol}] Skipping trade; account info unavailable")
            _record_throttle("SKIPPED_NO_ACCOUNT_INFO")
            _record_actionable(action="SKIPPED_NO_ACCOUNT_INFO")
            return

        # --- Margin protection entry gate (black-swan guard) ---
        margin_level = float(getattr(account, 'margin_level', 0.0) or 0.0)
        margin_block = float(getattr(
            self.pipeline_config, 'margin_entry_block_level', 100.0))
        # margin_level == 0 means no positions open (undefined, not stressed).
        if margin_level > 0 and margin_level < margin_block:
            self.logger.info(
                f"[{symbol}] MARGIN BLOCKED: margin_level={margin_level:.1f}% "
                f"< {margin_block:.0f}% — entry blocked"
            )
            _record_throttle("SKIPPED_MARGIN_BLOCKED")
            _record_actionable(action="SKIPPED_MARGIN_BLOCKED")
            return

        basis_pref = getattr(self.position_config, "risk_basis", "balance")
        basis_value = account.balance if basis_pref == "balance" else account.equity
        if basis_value <= 0:
            self.logger.warning(f"[{symbol}] Invalid risk basis value ({basis_value}); skipping trade")
            _record_throttle("SKIPPED_INVALID_BASIS")
            _record_actionable(action="SKIPPED_INVALID_BASIS")
            return

        # Get current price (same basis used for execution)
        tick = self.mt5.get_symbol_tick(symbol)
        if tick is None:
            self.logger.warning(f"[{symbol}] Skipping trade; tick data unavailable")
            _record_throttle("SKIPPED_NO_TICK")
            _record_actionable(action="SKIPPED_NO_TICK")
            return
        entry_price = tick.ask if is_long else tick.bid

        # Calculate stop prices from pips
        sl_price, tp_price = self.position_calc.calculate_stop_prices(entry_price, sl_pips, tp_pips, is_long, spec)

        # Enforce broker minimum stop distance (auto widen SL if too close)
        min_stop_dist = float(symbol_info.trade_stops_level) * float(symbol_info.point) if symbol_info.trade_stops_level else 0.0
        if min_stop_dist > 0 and abs(entry_price - sl_price) < min_stop_dist and getattr(self.position_config, "auto_widen_sl", True):
            if is_long:
                sl_price = entry_price - min_stop_dist
            else:
                sl_price = entry_price + min_stop_dist
            self.logger.debug(f"[{symbol}] Widened SL to satisfy min stop distance ({min_stop_dist})")

        # Target risk (deposit currency)
        # -------------------------------------------------------------
        # Winners-only live risk policy (single-path, non-tiered):
        # 1) start from base risk
        # 2) apply live multiplier
        # 3) cap by live max risk
        # 4) hard-cap by PositionConfig max_risk_pct
        # -------------------------------------------------------------
        base_risk_pct = float(getattr(self.position_config, 'risk_per_trade_pct', 1.0))
        live_risk_mult = float(
            getattr(
                self.pipeline_config,
                'live_risk_multiplier',
                1.0,
            )
        )
        live_max_risk = float(
            getattr(
                self.pipeline_config,
                'live_max_risk_pct',
                5.0,
            )
        )
        min_trade_risk = float(getattr(self.pipeline_config, 'min_trade_risk_pct', 0.1))

        target_risk_pct = min(base_risk_pct * live_risk_mult, live_max_risk)

        # Enforce PositionConfig max_risk_pct if present (hard safety)
        max_risk_cap = float(getattr(self.position_config, 'max_risk_pct', target_risk_pct))
        target_risk_pct = min(target_risk_pct, max_risk_cap)

        # Secondary trade adjustments + combined risk cap per symbol
        if is_secondary_trade:
            original_risk = target_risk_pct
            secondary_mult = float(getattr(self.pipeline_config, 'd1_secondary_risk_multiplier', 1.0))
            secondary_cap = float(getattr(self.pipeline_config, 'secondary_trade_max_risk_pct', 1.0))
            max_combined_risk = float(getattr(self.pipeline_config, 'max_combined_risk_pct', 3.0))

            target_risk_pct = target_risk_pct * secondary_mult
            target_risk_pct = min(target_risk_pct, secondary_cap)

            # Sum existing risk for this symbol from tagged positions.
            # If a position has no parseable risk tag, assume base_risk_pct for safety.
            existing_risk = 0.0
            try:
                for pos in self.mt5.get_positions():
                    if getattr(pos, 'symbol', None) != symbol:
                        continue
                    comment = getattr(pos, 'comment', '') or ''
                    r = TradeTagEncoder.get_risk_pct_from_comment(comment)
                    existing_risk += float(r) if r is not None else base_risk_pct
            except Exception:
                existing_risk = base_risk_pct

            available = max_combined_risk - existing_risk
            if available <= 0:
                self.logger.info(f"[{symbol}] Secondary trade blocked: combined risk cap reached ({existing_risk:.2f}% >= {max_combined_risk:.2f}%)")
                _record_throttle("BLOCKED_RISK_CAP")
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
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=None,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=None,
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

        # Compute loss per 1.0 lot using MT5 contract math (robust for indices/metals)
        loss_per_lot = self.mt5.calc_loss_amount(order_type.value, symbol, 1.0, entry_price, sl_price)
        
        # Fallback hierarchy if MT5 calc fails:
        # 1. Tick-based math from broker symbol info (reliable for CFDs/indices)
        # 2. Pip-value math (last resort)
        if loss_per_lot is None or loss_per_lot <= 0:
            # Tick-based fallback using broker-provided symbol specs
            price_diff = abs(entry_price - sl_price)
            if symbol_info.trade_tick_size > 0 and symbol_info.trade_tick_value > 0:
                ticks = price_diff / symbol_info.trade_tick_size
                loss_per_lot = ticks * symbol_info.trade_tick_value
                self.logger.info(f"[{symbol}] Using tick-based fallback: loss_per_lot=${loss_per_lot:.2f}")
            else:
                # Only now fall back to pip-value (with warning)
                loss_per_lot = abs(float(sl_pips)) * float(spec.pip_value)
                self.logger.warning(f"[{symbol}] Using pip-value fallback (less reliable): loss_per_lot=${loss_per_lot:.2f}")

        if loss_per_lot <= 0:
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
                loss_try = self.mt5.calc_loss_amount(order_type.value, symbol, 1.0, entry_price, sl_price)
                if loss_try is None or loss_try <= 0:
                    break
                loss_per_lot = loss_try
                volume_raw = target_risk_amount / loss_per_lot
                if volume_raw <= max_vol:
                    self.logger.debug(f"[{symbol}] Widened SL to fit max volume constraint (max_vol={max_vol})")
                    break

        # Clamp and normalize volume (risk-safe floor to step)
        volume_raw = max(min_vol, min(max_vol, volume_raw))
        volume = self.mt5.normalize_volume(volume_raw, symbol_info)

        # If normalization pushed below min, force to min_vol (may exceed target; enforce cap below)
        if volume < min_vol:
            volume = min_vol

        # Recompute actual risk after normalization, enforce hard cap
        # Use same fallback hierarchy as initial sizing
        actual_risk_amount = self.mt5.calc_loss_amount(order_type.value, symbol, volume, entry_price, sl_price)
        if actual_risk_amount is None or actual_risk_amount <= 0:
            # Tick-based fallback using broker-provided symbol specs
            price_diff = abs(entry_price - sl_price)
            if symbol_info.trade_tick_size > 0 and symbol_info.trade_tick_value > 0:
                ticks = price_diff / symbol_info.trade_tick_size
                actual_risk_amount = ticks * symbol_info.trade_tick_value * float(volume)
            else:
                # Pip-value fallback
                actual_risk_amount = abs(float(sl_pips)) * float(spec.pip_value) * float(volume)

        actual_risk_pct = (actual_risk_amount / basis_value) * 100.0 if basis_value > 0 else float('inf')
        max_risk_pct = float(getattr(self.position_config, "max_risk_pct", 5.0))
        if actual_risk_pct > max_risk_pct + 1e-9:
            self.logger.warning(
                f"[{symbol}] Skipping trade; risk {actual_risk_pct:.2f}% exceeds cap {max_risk_pct:.2f}% "
                f"(vol={volume:.4f}, sl={sl_price:.5f})"
            )
            # ── Record into throttle so this is not repeated ─────────
            _record_throttle("SKIPPED_RISK_CAP")
            _record_actionable(
                action="SKIPPED_RISK_CAP",
                entry_price_value=entry_price,
                sl_value=sl_price,
                tp_value=tp_price,
                volume_value=volume,
                target_risk_pct_value=target_risk_pct,
                actual_risk_pct_value=actual_risk_pct,
            )
            return

        # Enforce symbol-level combined risk cap across all open positions.
        can_trade, risk_reason = self._check_portfolio_risk_cap(
            config.symbol,      # canonical symbol (for PM comment metadata)
            actual_risk_pct,    # post-normalization actual risk
            symbol,             # broker symbol (for MT5 lookup)
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

        # Log risk details for auditability
        self.logger.info(
            f"[{symbol}] {order_type.name} | basis={basis_value:.2f} ({basis_pref}) | "
            f"target_risk={target_risk_pct:.2f}% (${target_risk_amount:.2f}) | "
            f"actual_risk={actual_risk_pct:.2f}% (${actual_risk_amount:.2f}) | "
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

        # ===== FIX #2: Encode trade comment with full metadata =====
        # This allows position analysis for D1 + lower-TF logic
        trade_comment = TradeTagEncoder.encode_comment(
            symbol=config.symbol,  # Use original symbol, not broker symbol
            timeframe=_tf,
            strategy_name=_strat_name,
            direction=direction,
            risk_pct=target_risk_pct,
        )

        # Execute order
        result = self.mt5.send_market_order(
            symbol=symbol,
            order_type=order_type,
            volume=volume,
            sl=sl_price,
            tp=tp_price,
            deviation=30,
            magic=magic,
            comment=trade_comment
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
                risk_exec = self.mt5.calc_loss_amount(order_type.value, symbol, result.volume, result.price, sl_price)
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
    
    def _reconnect(self) -> bool:
        """Attempt to reconnect to MT5."""
        for attempt in range(5):
            self.logger.info(f"Reconnection attempt {attempt + 1}/5")
            self.mt5.disconnect()
            time.sleep(2)
            if self.mt5.connect():
                self.logger.info("Reconnected successfully")
                return True
            time.sleep(5)
        return False
    
    def save_trade_log(self, filepath: str):
        """Save trade log to file."""
        with open(filepath, 'w') as f:
            json.dump(self.trade_log, f, indent=2)
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
                 data_dir: str = "./data",
                 output_dir: str = "./pm_outputs",
                 config_file: str = "pm_configs.json"):
        """
        Initialize application.
        
        Args:
            symbols: List of symbols to trade
            config: Pipeline configuration (alias for pipeline_config)
            pipeline_config: Pipeline configuration
            mt5_config: MT5 connection configuration
            position_config: Position management configuration
            data_dir: Directory for data files
            output_dir: Directory for output files
            config_file: Name of saved configurations file
        """
        # Accept either name (keeps full compatibility)
        if config is None and pipeline_config is not None:
            config = pipeline_config
        
        self.symbols = symbols or DEFAULT_SYMBOLS
        
        # Configurations
        self.pipeline_config = config or PipelineConfig(
            data_dir=Path(data_dir),
            output_dir=Path(output_dir)
        )
        self.mt5_config = mt5_config or MT5Config()
        self.position_config = position_config or PositionConfig()
        
        # Paths
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.config_file = config_file
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Components
        self.mt5: Optional[MT5Connector] = None
        self.portfolio_manager: Optional[PortfolioManager] = None
        self.trader: Optional[LiveTrader] = None
        
        self.logger = logging.getLogger(__name__)
    
    def initialize(self) -> bool:
        """Initialize all components."""
        self.logger.info("=" * 60)
        self.logger.info("FX PORTFOLIO MANAGER v3.0")
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
        
        # Print summary
        self.portfolio_manager.print_status()
        
        return len(self.portfolio_manager.get_validated_configs()) > 0
    
    def run_trading(self, 
                    enable_trading: bool = True,
                    auto_retrain: bool = False,
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
        self.logger.info(f"Auto-retrain: {auto_retrain}")
        self.logger.info(f"Close on opposite signal: {close_on_opposite_signal}")
        self.logger.info("=" * 60)
        
        # Check for validated configs
        if not self.portfolio_manager.get_validated_configs():
            self.logger.warning("No validated configurations. Run optimization first.")
            return
        
        # Create trader
        self.trader = LiveTrader(
            mt5_connector=self.mt5,
            portfolio_manager=self.portfolio_manager,
            position_config=self.position_config,
            enable_trading=enable_trading,
            close_on_opposite_signal=close_on_opposite_signal,
            pipeline_config=self.pipeline_config,
        )
        
        # Main loop with reconnection handling
        last_retrain_check = datetime.now()
        retrain_check_interval = timedelta(hours=1)
        
        try:
            while True:
                # Check MT5 connection
                if not self.mt5.is_connected():
                    self.logger.warning("Lost MT5 connection, attempting reconnect...")
                    reconnected = False
                    for attempt in range(5):
                        self.logger.info(f"Reconnection attempt {attempt + 1}/5")
                        self.mt5.disconnect()
                        time.sleep(2)
                        if self.mt5.connect():
                            self.logger.info("Reconnected successfully")
                            reconnected = True
                            break
                        time.sleep(5)
                    if not reconnected:
                        self.logger.error("Failed to reconnect after 5 attempts")
                        time.sleep(30)
                        continue
                
                # Check for retraining
                if auto_retrain and datetime.now() - last_retrain_check > retrain_check_interval:
                    symbols_to_retrain = self.portfolio_manager.get_symbols_needing_retrain()
                    if symbols_to_retrain:
                        self.logger.info(f"Retraining {len(symbols_to_retrain)} symbols...")
                        self._fetch_historical_data(symbols_to_retrain)
                        self.portfolio_manager.retrain_all_needed()
                    last_retrain_check = datetime.now()
                
                # Run trading iteration
                try:
                    self.trader._process_all_symbols()
                except Exception as e:
                    self.logger.error(f"Error in trading loop: {e}")
                
                # Sleep
                time.sleep(1)
                
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
            
            # Fetch M5 data (base timeframe) - approximately 4.8 years crypto 6.89 years
            bars_to_fetch = int(getattr(self.pipeline_config, "max_bars", 300000))
            bars = self.mt5.get_bars(broker_symbol, "M5", count=bars_to_fetch)

            if bars is not None and len(bars) > 0:
                filepath = self.data_dir / f"{symbol}_M5.csv"
                bars.to_csv(filepath)
                self.logger.info(f"  {symbol}: {len(bars)} bars saved")
            else:
                self.logger.warning(f"  {symbol}: No data available")
    
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
                    'retrain_days': result.config.retrain_days,
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
        '--auto-retrain',
        action='store_true',
        help='Enable automatic retraining'
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
        default='./data',
        help='Data directory'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./pm_outputs',
        help='Output directory'
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
    
    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = logging.getLogger(__name__)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Load runtime configuration
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

    pipeline_config = PipelineConfig(**_filter_dataclass_kwargs(PipelineConfig, config_data.get("pipeline", {})))
    position_config = PositionConfig(**_filter_dataclass_kwargs(PositionConfig, config_data.get("position", {})))
    mt5_config = MT5Config(**_filter_dataclass_kwargs(MT5Config, mt5_section))

    # If position risk not set explicitly, inherit from pipeline risk (backward compatible)
    if "risk_per_trade_pct" not in (config_data.get("position") or {}) and hasattr(pipeline_config, "risk_per_trade_pct"):
        position_config.risk_per_trade_pct = pipeline_config.risk_per_trade_pct

    log_resolved_config_summary(logger, args.config, config_data, pipeline_config, position_config, mt5_config)

    _app_instance = FXPortfolioManagerApp(
        symbols=symbols,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        pipeline_config=pipeline_config,
        position_config=position_config,
        mt5_config=mt5_config
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
                auto_retrain=args.auto_retrain,
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
            print("  python pm_main.py --trade --auto-retrain  # With auto-retraining")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Application error: {e}")
        return 1
    finally:
        if _app_instance:
            _app_instance.shutdown()


if __name__ == "__main__":
    sys.exit(main())
