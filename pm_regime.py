"""
FX Portfolio Manager - Regime Detection Module
===============================================

Market regime detection with:
- 4 regimes: TREND, RANGE, BREAKOUT, CHOP
- Per-regime strength scores (0-1)
- Gap calculation (top - second) for crossover awareness
- Hysteresis state machine (k_confirm, gap_min, k_hold)
- Causal computation (no lookahead)
- REGIME_LIVE shift for backtest/live parity
- Numba JIT compilation for performance-critical loops

This module is called from the pipeline layer (not FeatureComputer) so that
symbol/timeframe-specific parameters can be loaded and applied.

Version: 3.1 (Portfolio Manager - Numba optimizations)
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

import numpy as np
import pandas as pd
from pm_evidence import CALENDAR_SPLIT_MODE, EVIDENCE_CONTRACT_VERSION

logger = logging.getLogger(__name__)

# =============================================================================
# NUMBA JIT COMPILATION (optional - graceful fallback if not available)
# =============================================================================

try:
    from numba import jit, prange
    NUMBA_AVAILABLE = True
    logger.debug("Numba JIT compilation available - using optimized loops")
except Exception:
    NUMBA_AVAILABLE = False
    logger.debug("Numba not available - using pure Python loops")
    
    # Create no-op decorator for graceful fallback
    def jit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    prange = range


# =============================================================================
# NUMBA JIT-COMPILED FUNCTIONS (10x speedup for critical loops)
# =============================================================================

@jit(nopython=True, cache=True)
def _hysteresis_loop_numba(regime_raw_int: np.ndarray, 
                           gap: np.ndarray,
                           k_confirm: int, 
                           gap_min: float, 
                           k_hold: int) -> np.ndarray:
    """
    Numba JIT-compiled hysteresis state machine.
    
    Provides ~10x speedup over pure Python for large arrays.
    
    Args:
        regime_raw_int: Integer-encoded regime labels (0=TREND, 1=RANGE, 2=BREAKOUT, 3=CHOP)
        gap: Gap between top and second regime scores
        k_confirm: Bars required to confirm switch
        gap_min: Minimum gap required to switch
        k_hold: Minimum bars to hold regime
        
    Returns:
        Integer-encoded final regime labels
    """
    n = len(regime_raw_int)
    regime_final = np.zeros(n, dtype=np.int32)
    
    if n == 0:
        return regime_final
    
    # State tracking
    current_regime = regime_raw_int[0]
    bars_in_current = 0
    candidate_regime = -1
    candidate_count = 0
    
    for i in range(n):
        raw = regime_raw_int[i]
        
        # First bar: initialize
        if i == 0:
            current_regime = raw if raw >= 0 else 3  # Default to CHOP
            bars_in_current = 1
            regime_final[i] = current_regime
            continue
        
        bars_in_current += 1
        
        # Check if raw regime is different from current
        if raw != current_regime and raw >= 0:
            # Check if gap is sufficient
            if gap[i] >= gap_min:
                # Track candidate
                if raw == candidate_regime:
                    candidate_count += 1
                else:
                    candidate_regime = raw
                    candidate_count = 1
                
                # Check if confirmed
                if candidate_count >= k_confirm and bars_in_current >= k_hold:
                    current_regime = candidate_regime
                    bars_in_current = 1
                    candidate_regime = -1
                    candidate_count = 0
            else:
                # Gap too small, reset candidate
                candidate_regime = -1
                candidate_count = 0
        else:
            # Same regime or invalid, reset candidate
            candidate_regime = -1
            candidate_count = 0
        
        regime_final[i] = current_regime
    
    return regime_final


@jit(nopython=True, cache=True)
def _compute_whipsaw_numba(open_p: np.ndarray, high: np.ndarray, 
                           low: np.ndarray, close: np.ndarray, 
                           period: int) -> np.ndarray:
    """
    Numba JIT-compiled whipsaw/wickiness computation.
    
    Replaces nested Python loops with fast native code.
    """
    n = len(close)
    whipsaw = np.zeros(n)
    
    for i in range(period, n):
        total_wick = 0.0
        total_range = 0.0
        
        for j in range(i - period + 1, i + 1):
            bar_range = high[j] - low[j]
            
            if bar_range > 1e-10:
                body_top = max(open_p[j], close[j])
                body_bottom = min(open_p[j], close[j])
                upper_wick = high[j] - body_top
                lower_wick = body_bottom - low[j]
                total_wick += upper_wick + lower_wick
                total_range += bar_range
        
        if total_range > 1e-10:
            whipsaw[i] = total_wick / total_range
    
    return whipsaw


@jit(nopython=True, cache=True)
def _compute_direction_flips_numba(close: np.ndarray, period: int) -> np.ndarray:
    """
    Numba JIT-compiled direction flip frequency computation.
    
    Replaces nested Python loops with fast native code.
    """
    n = len(close)
    flips = np.zeros(n)
    
    for i in range(period + 1, n):
        flip_count = 0
        for j in range(i - period + 1, i):
            prev_change = close[j] - close[j - 1]
            curr_change = close[j + 1] - close[j]
            
            # Determine directions
            if prev_change > 0:
                prev_dir = 1
            elif prev_change < 0:
                prev_dir = -1
            else:
                prev_dir = 0
            
            if curr_change > 0:
                curr_dir = 1
            elif curr_change < 0:
                curr_dir = -1
            else:
                curr_dir = 0
            
            if prev_dir != 0 and curr_dir != 0 and prev_dir != curr_dir:
                flip_count += 1
        
        flips[i] = flip_count / max(1, period - 1)
    
    return flips


@jit(nopython=True, cache=True)
def _compute_structure_break_numba(high: np.ndarray, low: np.ndarray, 
                                    close: np.ndarray, atr: np.ndarray,
                                    period: int, atr_mult: float) -> np.ndarray:
    """
    Numba JIT-compiled structure break detection.
    """
    n = len(close)
    structure_break = np.zeros(n)
    
    for i in range(period, n):
        # Find recent high/low
        recent_high = high[i - period]
        recent_low = low[i - period]
        for j in range(i - period + 1, i):
            if high[j] > recent_high:
                recent_high = high[j]
            if low[j] < recent_low:
                recent_low = low[j]
        
        margin = atr[i] * atr_mult if atr[i] > 0 else 0
        
        if close[i] > recent_high + margin:
            structure_break[i] = 1.0
        elif close[i] < recent_low - margin:
            structure_break[i] = 1.0
        elif margin > 0:
            # Partial score based on proximity
            dist_high = (close[i] - recent_high) / (margin + 1e-10)
            dist_low = (recent_low - close[i]) / (margin + 1e-10)
            max_dist = max(dist_high, dist_low)
            structure_break[i] = max(0.0, min(1.0, max_dist))
    
    return structure_break


# =============================================================================
# REGIME TYPES
# =============================================================================

class RegimeType:
    """Regime type constants."""
    TREND = "TREND"
    RANGE = "RANGE"
    BREAKOUT = "BREAKOUT"
    CHOP = "CHOP"
    
    ALL = [TREND, RANGE, BREAKOUT, CHOP]
    
    # Integer encoding for Numba (must match indices in ALL)
    _TO_INT = {"TREND": 0, "RANGE": 1, "BREAKOUT": 2, "CHOP": 3}
    _FROM_INT = {0: "TREND", 1: "RANGE", 2: "BREAKOUT", 3: "CHOP"}
    
    @classmethod
    def to_int(cls, regime: str) -> int:
        """Convert regime string to integer for Numba."""
        return cls._TO_INT.get(regime, 3)  # Default to CHOP
    
    @classmethod
    def from_int(cls, idx: int) -> str:
        """Convert integer back to regime string."""
        return cls._FROM_INT.get(idx, cls.CHOP)


# =============================================================================
# REGIME PARAMETERS
# =============================================================================

@dataclass
class RegimeParams:
    """
    Parameters for regime detection.
    
    Can be customized per symbol/timeframe via regime_params.json.
    """
    # Hysteresis parameters
    k_confirm: int = 3          # Bars required to confirm regime switch
    gap_min: float = 0.10       # Minimum gap (top - second) to switch
    k_hold: int = 5             # Minimum bars to hold a regime
    
    # Lookback windows
    adx_period: int = 14        # ADX calculation period
    atr_period: int = 14        # ATR calculation period
    bb_period: int = 20         # Bollinger Band period
    bb_std: float = 2.0         # Bollinger Band std deviation
    efficiency_period: int = 10 # Directional efficiency lookback
    
    # Adaptive thresholds
    bb_squeeze_lookback: int = 200  # Lookback for BB width percentile
    bb_squeeze_percentile: float = 20.0  # Percentile for "squeeze" detection (0-100)
    atr_lookback: int = 100     # Lookback for ATR percentile
    structure_period: int = 20  # Period for structure break detection
    structure_atr_mult: float = 0.1  # ATR multiplier for structure break margin
    
    # Score weights (for each regime)
    trend_adx_weight: float = 0.40
    trend_efficiency_weight: float = 0.35
    trend_slope_weight: float = 0.25
    
    range_adx_weight: float = 0.35
    range_efficiency_weight: float = 0.30
    range_containment_weight: float = 0.35
    
    breakout_squeeze_weight: float = 0.35
    breakout_atr_weight: float = 0.30
    breakout_structure_weight: float = 0.35
    
    chop_adx_weight: float = 0.30
    chop_efficiency_weight: float = 0.30
    chop_whipsaw_weight: float = 0.40

    # Asset-class defaults can override this unless regime_params.json pins it.
    adx_trend_threshold: float = 25.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegimeParams':
        """Create from dictionary, ignoring unknown keys."""
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


# Default parameters by timeframe.
DEFAULT_PARAMS_BY_TIMEFRAME: Dict[str, RegimeParams] = {
    'M5':  RegimeParams(k_confirm=3, gap_min=0.10, k_hold=5, bb_squeeze_lookback=50),
    'M15': RegimeParams(k_confirm=3, gap_min=0.10, k_hold=4, bb_squeeze_lookback=80),
    'M30': RegimeParams(k_confirm=2, gap_min=0.10, k_hold=4),
    'H1':  RegimeParams(k_confirm=2, gap_min=0.10, k_hold=3, bb_squeeze_lookback=50),
    'H4':  RegimeParams(k_confirm=2, gap_min=0.08, k_hold=2, bb_squeeze_lookback=50),
    # D1 uses slightly stronger hysteresis to avoid one-bar regime flips.
    'D1':  RegimeParams(k_confirm=1, gap_min=0.08, k_hold=3, bb_squeeze_lookback=60),
}


# Asset-class defaults for the ADX trend anchor. JSON-pinned values win.
DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS: Dict[str, float] = {
    'fx':     25.0,
    'metal':  22.0,
    'crypto': 30.0,
    'index':  25.0,
}


def _infer_asset_class(symbol: str) -> str:
    """Infer asset class from symbol name (pure string-match; no I/O).

    Returns one of ``{'fx', 'metal', 'crypto', 'index'}``. Unknown symbols
    default to ``'fx'`` so the classic `25.0` threshold still applies — safest
    fallback for a broad-discovery engine that shouldn't silently shift a
    symbol's trend anchor on a name collision.
    """
    up = (symbol or "").upper().strip()
    if not up:
        return 'fx'
    # Metals — XAU/XAG/XPT/XPD plus legacy GOLD/SILVER spellings.
    if up.startswith(('XAU', 'XAG', 'XPT', 'XPD')) or up in {'GOLD', 'SILVER'}:
        return 'metal'
    # Crypto — prefix match covers majors (BTC, ETH, LTC, XRP, SOL, ADA, DOT,
    # DOGE, BCH) plus any pair that crosses two crypto tickers (BTCETH, BTCXAU
    # is treated as crypto because its volatility profile is crypto-driven).
    crypto_prefixes = ('BTC', 'ETH', 'LTC', 'XRP', 'SOL', 'ADA', 'DOT', 'DOGE', 'DOG', 'BCH', 'TRX', 'TON')
    if up.startswith(crypto_prefixes):
        return 'crypto'
    # Indices — common ticker shorthand used by MT5 brokers.
    index_tickers = {
        'SP500', 'SPX500', 'US500', 'US30', 'US100', 'NAS100', 'USTEC',
        'DAX', 'DE30', 'DE40', 'GER30', 'GER40', 'FTSE', 'UK100',
        'NIKKEI', 'JP225', 'AUS200', 'AU200', 'HK50', 'CHINA50',
        'EU50', 'ES35', 'FR40',
    }
    if up in index_tickers or 'INDEX' in up:
        return 'index'
    return 'fx'


def _resolve_adx_trend_threshold(symbol: str) -> float:
    """Asset-class-derived default for `RegimeParams.adx_trend_threshold`."""
    return DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS.get(
        _infer_asset_class(symbol),
        DEFAULT_ADX_TREND_THRESHOLD_BY_ASSET_CLASS['fx'],
    )


# Global cache for loaded regime params
_REGIME_PARAMS_CACHE: Dict[str, Dict[str, RegimeParams]] = {}
_REGIME_PARAMS_LOADED: bool = False
_REGIME_PARAMS_LOADED_PATH: Optional[str] = None
# Track fallback use so the startup summary can surface tuning gaps once.
_REGIME_FALLBACK_LOGGED: Set[Tuple[str, str]] = set()
_REGIME_FALLBACK_RECORD: Dict[Tuple[str, str], str] = {}
# Pairs whose JSON explicitly pinned adx_trend_threshold.
_REGIME_ADX_THRESHOLD_EXPLICIT: Set[Tuple[str, str]] = set()


# A regime detector is part of the model-selection surface: changing its
# parameters changes which trades belong to every regime bucket.  An explicit
# JSON row is therefore not evidence that the row was fitted without seeing
# tuning-validation or final-lockbox observations.  The companion provenance
# artifact below makes that boundary machine-verifiable.
REGIME_PROVENANCE_SCHEMA_VERSION = "fxpm.regime-detector-fit-provenance/v1"
REGIME_PROVENANCE_FIT_SCOPE = "optimizer_train"
_REGIME_SOURCE_DIGEST_CACHE: Dict[Tuple[str, int, int], str] = {}


def regime_params_provenance_path(filepath: str = "regime_params.json") -> Path:
    """Return the deterministic sidecar path for a regime-parameter artifact."""
    params_path = Path(str(filepath or "regime_params.json"))
    return params_path.with_name(f"{params_path.stem}.provenance.json")


def regime_params_sha256(params: Dict[str, Any]) -> str:
    """Hash one symbol/timeframe parameter row using canonical JSON."""
    encoded = json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _case_insensitive_nested_cell(payload: Any, symbol: str, timeframe: str) -> Any:
    """Read ``payload[symbol][timeframe]`` without depending on broker-key case."""
    if not isinstance(payload, dict):
        return None
    symbol_key = next(
        (
            key for key in payload
            if str(key or "").strip().upper() == str(symbol or "").strip().upper()
        ),
        None,
    )
    if symbol_key is None or not isinstance(payload.get(symbol_key), dict):
        return None
    tf_map = payload[symbol_key]
    timeframe_key = next(
        (
            key for key in tf_map
            if str(key or "").strip().upper() == str(timeframe or "").strip().upper()
        ),
        None,
    )
    return tf_map.get(timeframe_key) if timeframe_key is not None else None


def _normalized_timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return pd.Timestamp(value).isoformat()
    except Exception:
        return None


def _source_sha256(path: Path) -> str:
    stat = path.stat()
    key = (str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns))
    cached = _REGIME_SOURCE_DIGEST_CACHE.get(key)
    if cached:
        return cached
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    value = digest.hexdigest()
    _REGIME_SOURCE_DIGEST_CACHE[key] = value
    return value


def validate_regime_params_provenance(
    symbol: str,
    timeframe: str,
    filepath: str = "regime_params.json",
    *,
    provenance_filepath: Optional[str] = None,
    expected_source: Optional[Dict[str, Any]] = None,
    expected_train_window: Optional[Dict[str, Any]] = None,
    expected_split_contract: Optional[str] = None,
) -> Dict[str, Any]:
    """Fail-closed validation of one detector-fit provenance record.

    ``expected_source`` and ``expected_train_window`` are supplied by the
    chronological pipeline.  With them, validation proves that the detector
    was fitted on the exact same M5 optimizer-train partition the strategy
    optimizer is about to use.  Without them (the lightweight live-readiness
    check), the function still verifies the fit scope, split geometry,
    parameter hash, and current source-file hash.
    """
    params_path = Path(str(filepath or "regime_params.json"))
    sidecar_path = (
        Path(provenance_filepath)
        if provenance_filepath
        else regime_params_provenance_path(str(params_path))
    )

    def reject(reason: str, record: Any = None) -> Dict[str, Any]:
        return {
            "valid": False,
            "reason": reason,
            "symbol": str(symbol),
            "timeframe": str(timeframe).upper(),
            "params_path": str(params_path),
            "provenance_path": str(sidecar_path),
            "record": record if isinstance(record, dict) else {},
        }

    try:
        params_payload = json.loads(params_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return reject("regime params file is missing")
    except Exception as exc:
        return reject(f"regime params file is unreadable: {exc}")
    params_cell = _case_insensitive_nested_cell(params_payload, symbol, timeframe)
    if not isinstance(params_cell, dict):
        return reject("regime params cell is missing")

    try:
        provenance_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return reject("regime provenance sidecar is missing")
    except Exception as exc:
        return reject(f"regime provenance sidecar is unreadable: {exc}")
    if not isinstance(provenance_payload, dict):
        return reject("regime provenance sidecar is not an object")
    if provenance_payload.get("schema_version") != REGIME_PROVENANCE_SCHEMA_VERSION:
        return reject("regime provenance schema mismatch")

    record = _case_insensitive_nested_cell(
        provenance_payload.get("cells", {}), symbol, timeframe
    )
    if not isinstance(record, dict):
        return reject("regime provenance cell is missing")
    if record.get("schema_version") != REGIME_PROVENANCE_SCHEMA_VERSION:
        return reject("regime provenance cell schema mismatch", record)
    if str(record.get("symbol", "")).upper() != str(symbol).upper():
        return reject("regime provenance symbol mismatch", record)
    if str(record.get("timeframe", "")).upper() != str(timeframe).upper():
        return reject("regime provenance timeframe mismatch", record)
    if str(record.get("fit_scope", "")).strip().lower() != REGIME_PROVENANCE_FIT_SCOPE:
        return reject("regime detector was not fitted on optimizer_train", record)
    if str(record.get("status", "")).strip().upper() != "FIT":
        return reject("regime detector provenance is not a successful fit", record)
    if record.get("params_sha256") != regime_params_sha256(params_cell):
        return reject("regime parameter hash mismatch", record)

    source = record.get("source")
    if not isinstance(source, dict):
        return reject("regime provenance source metadata is missing", record)
    source_path_raw = str(source.get("path", "") or "").strip()
    source_sha = str(source.get("sha256", "") or "").strip().lower()
    try:
        source_size = int(source.get("size"))
    except (TypeError, ValueError):
        source_size = -1
    if not source_path_raw or source_size < 0 or len(source_sha) != 64:
        return reject("regime provenance source fingerprint is incomplete", record)
    source_path = Path(source_path_raw)
    try:
        current_size = int(source_path.stat().st_size)
    except OSError as exc:
        return reject(f"regime provenance source is unavailable: {exc}", record)
    if current_size != source_size:
        return reject("regime provenance source size mismatch", record)

    if expected_source is not None:
        expected_path_raw = str(expected_source.get("path", "") or "").strip()
        expected_sha = str(expected_source.get("sha256", "") or "").strip().lower()
        try:
            expected_size = int(expected_source.get("size"))
        except (TypeError, ValueError):
            expected_size = -1
        if not expected_path_raw or expected_size < 0 or len(expected_sha) != 64:
            return reject("current pipeline source fingerprint is incomplete", record)
        try:
            paths_match = source_path.resolve() == Path(expected_path_raw).resolve()
        except OSError:
            paths_match = source_path.absolute() == Path(expected_path_raw).absolute()
        if not paths_match or expected_size != source_size or expected_sha != source_sha:
            return reject("regime provenance does not match current pipeline source", record)
    else:
        try:
            current_sha = _source_sha256(source_path).lower()
        except OSError as exc:
            return reject(f"regime provenance source cannot be hashed: {exc}", record)
        if current_sha != source_sha:
            return reject("regime provenance source SHA256 mismatch", record)

    train = record.get("train")
    split = record.get("split")
    if not isinstance(train, dict) or not isinstance(split, dict):
        return reject("regime provenance train/split metadata is missing", record)
    try:
        start_index = int(train.get("start_index"))
        end_index = int(train.get("end_index"))
        train_rows = int(train.get("rows"))
    except (TypeError, ValueError):
        return reject("regime provenance train indices are invalid", record)
    if start_index < 0 or end_index <= start_index or train_rows != end_index - start_index:
        return reject("regime provenance train geometry is invalid", record)
    if not _normalized_timestamp(train.get("start_ts")):
        return reject("regime provenance train start timestamp is missing", record)
    if not _normalized_timestamp(train.get("end_ts_inclusive")):
        return reject("regime provenance train inclusive end timestamp is missing", record)
    if not _normalized_timestamp(train.get("end_ts_exclusive")):
        return reject("regime provenance train exclusive end timestamp is missing", record)

    try:
        split_train = [int(value) for value in split.get("optimizer_train_indices", [])]
        split_validation = [int(value) for value in split.get("tuning_validation_indices", [])]
        split_lockbox = [int(value) for value in split.get("final_lockbox_indices", [])]
    except (TypeError, ValueError):
        return reject("regime provenance split indices are invalid", record)
    if split_train != [start_index, end_index]:
        return reject("regime provenance train indices disagree with split metadata", record)
    if len(split_validation) != 2 or len(split_lockbox) != 2:
        return reject("regime provenance validation/lockbox indices are missing", record)
    if not (
        end_index <= split_validation[0]
        < split_validation[1]
        <= split_lockbox[0]
        < split_lockbox[1]
    ):
        return reject("regime provenance leaks beyond optimizer_train", record)
    split_mode = str(split.get("production_split_mode", "")).lower()
    if split_mode not in {"chronological_lockbox_v1", CALENDAR_SPLIT_MODE}:
        return reject("regime provenance was not produced by a lockbox split", record)
    if split_mode == CALENDAR_SPLIT_MODE:
        if str(split.get("source_authority", "")).lower() != "broker_native_timeframe":
            return reject(
                "calendar regime provenance is not native-timeframe authoritative",
                record,
            )
        if str(source.get("source_kind", "")).lower() != "broker_native":
            return reject("calendar regime provenance source is not broker-native", record)
        if split.get("contract_version") != EVIDENCE_CONTRACT_VERSION:
            return reject("calendar regime provenance contract is stale", record)
    top_contract = provenance_payload.get("split_contract_version")
    if top_contract and top_contract != split.get("contract_version"):
        return reject("regime provenance top-level split contract mismatch", record)
    if expected_split_contract and split.get("contract_version") != expected_split_contract:
        return reject("regime provenance split-contract mismatch", record)

    if expected_train_window is not None:
        expected_values = {
            "start_index": expected_train_window.get("start_index"),
            "end_index": expected_train_window.get("end_index"),
            "rows": expected_train_window.get("rows"),
        }
        try:
            expected_values = {key: int(value) for key, value in expected_values.items()}
        except (TypeError, ValueError):
            return reject("current optimizer-train indices are incomplete", record)
        if expected_values != {
            "start_index": start_index,
            "end_index": end_index,
            "rows": train_rows,
        }:
            return reject("regime provenance optimizer-train window mismatch", record)
        for key in ("start_ts", "end_ts_inclusive", "end_ts_exclusive"):
            expected_ts = _normalized_timestamp(expected_train_window.get(key))
            record_ts = _normalized_timestamp(train.get(key))
            if expected_ts is None or record_ts != expected_ts:
                return reject(f"regime provenance optimizer-train {key} mismatch", record)

    return {
        "valid": True,
        "reason": "ok",
        "symbol": str(symbol),
        "timeframe": str(timeframe).upper(),
        "params_path": str(params_path),
        "provenance_path": str(sidecar_path),
        "record": record,
    }


def _ensure_regime_params_loaded(filepath: str = "regime_params.json") -> None:
    """Load tuned regime params once for the requested path without recording fallbacks."""
    global _REGIME_PARAMS_CACHE, _REGIME_PARAMS_LOADED, _REGIME_PARAMS_LOADED_PATH

    cache_path = str(filepath or "")
    if _REGIME_PARAMS_LOADED and _REGIME_PARAMS_LOADED_PATH == cache_path:
        return

    _REGIME_PARAMS_CACHE = {}
    _REGIME_ADX_THRESHOLD_EXPLICIT.clear()
    try:
        with open(cache_path, 'r') as f:
            raw_data = json.load(f)

        for sym, tf_dict in raw_data.items():
            _REGIME_PARAMS_CACHE[sym] = {}
            if not isinstance(tf_dict, dict):
                continue
            for tf, params_dict in tf_dict.items():
                _REGIME_PARAMS_CACHE[sym][tf] = RegimeParams.from_dict(params_dict)
                # Preserve JSON-pinned ADX anchors for this pair.
                if isinstance(params_dict, dict) and 'adx_trend_threshold' in params_dict:
                    _REGIME_ADX_THRESHOLD_EXPLICIT.add((sym, tf))

        logger.info(f"Loaded regime params for {len(_REGIME_PARAMS_CACHE)} symbols from {cache_path}")
    except FileNotFoundError:
        logger.debug(f"No regime_params.json found at {cache_path}, using defaults")
    except Exception as e:
        logger.warning(f"Failed to load regime params from {cache_path}: {e}")

    _REGIME_PARAMS_LOADED = True
    _REGIME_PARAMS_LOADED_PATH = cache_path


def _find_tuned_regime_params(symbol: str, timeframe: str) -> Optional[Tuple[Tuple[str, str], RegimeParams]]:
    """Return the tuned JSON entry for symbol/timeframe without falling back."""
    symbol_key = str(symbol or "").strip()
    timeframe_key = str(timeframe or "").strip()
    if not symbol_key or not timeframe_key:
        return None

    if symbol_key in _REGIME_PARAMS_CACHE and timeframe_key in _REGIME_PARAMS_CACHE[symbol_key]:
        return (symbol_key, timeframe_key), _REGIME_PARAMS_CACHE[symbol_key][timeframe_key]

    symbol_upper = symbol_key.upper()
    timeframe_upper = timeframe_key.upper()
    for cached_symbol, tf_map in _REGIME_PARAMS_CACHE.items():
        if str(cached_symbol or "").upper() != symbol_upper or not isinstance(tf_map, dict):
            continue
        for cached_tf, params in tf_map.items():
            if str(cached_tf or "").upper() == timeframe_upper:
                return (cached_symbol, cached_tf), params
    return None


def has_tuned_regime_params(symbol: str, timeframe: str,
                            filepath: str = "regime_params.json",
                            *,
                            require_provenance: bool = False,
                            provenance_filepath: Optional[str] = None) -> bool:
    """
    True only when regime params contain an explicit tuned entry.

    This intentionally does not call ``load_regime_params`` because live
    readiness must distinguish trained params from research/default fallbacks.
    """
    _ensure_regime_params_loaded(filepath)
    if _find_tuned_regime_params(symbol, timeframe) is None:
        return False
    if not require_provenance:
        return True
    return bool(validate_regime_params_provenance(
        symbol,
        timeframe,
        filepath,
        provenance_filepath=provenance_filepath,
    ).get("valid", False))


def validated_regime_params_for_timeframe(symbol: str, timeframe: str) -> RegimeParams:
    """
    Return the explicit detector row matching the optimizer's no-file surface.

    When optimization validated winners without a tuned ``regime_params.json``
    file, the operative detector was the timeframe regime surface plus the
    asset-class ADX anchor. Materializing that row into JSON makes live carry
    over the same surface without relying on implicit fallback behavior.
    """
    tf_key = str(timeframe or "").strip().upper()
    base = DEFAULT_PARAMS_BY_TIMEFRAME.get(tf_key, RegimeParams())
    return _with_asset_class_adx_threshold(base, str(symbol or "").strip().upper())


def materialize_regime_params_for_cells(
    cells: List[Tuple[str, str]],
    filepath: str = "regime_params.json",
) -> int:
    """
    Ensure ``filepath`` has explicit regime params for each symbol/timeframe.

    Existing tuned rows are preserved. Missing rows are filled with the exact
    no-file detector surface used during optimization/backtesting.
    """
    path_raw = str(filepath or "").strip()
    if not path_raw:
        return 0
    path = Path(path_raw)
    payload: Dict[str, Dict[str, Any]] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = {
                    str(sym): dict(tf_map)
                    for sym, tf_map in loaded.items()
                    if isinstance(tf_map, dict)
                }
        except Exception as exc:
            logger.warning("Failed to read existing regime params from %s: %s", path, exc)

    added = 0
    # Preserve the broker's original symbol case so regime-param keys match the
    # ledger exactly (live lookup is case-insensitive, but the persisted keys
    # should mirror config/ledger casing, e.g. "Platinum" not "PLATINUM").
    for symbol, timeframe in sorted({(str(s).strip(), str(tf).strip().upper()) for s, tf in cells}):
        if not symbol or not timeframe:
            continue
        symbol_payload = payload.setdefault(symbol, {})
        if timeframe in symbol_payload:
            continue
        symbol_payload[timeframe] = validated_regime_params_for_timeframe(symbol, timeframe).to_dict()
        added += 1

    if added <= 0:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)
    clear_regime_params_cache()
    logger.info("Materialized %d explicit validated regime-param rows into %s", added, path)
    return added


def _object_get(payload: Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _regime_payload_tradeable(payload: Any) -> bool:
    name = str(_object_get(payload, "strategy_name", "") or "").upper()
    if not name or name == "NO_TRADE":
        return False
    artifact_meta = _object_get(payload, "artifact_meta", {}) or {}
    if isinstance(artifact_meta, dict) and artifact_meta.get("no_trade"):
        return False
    status = str(_object_get(payload, "promotion_status", "BLOCKED") or "BLOCKED").upper()
    return status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}


def materialize_regime_params_from_symbol_config(
    symbol_config: Any,
    filepath: str = "regime_params.json",
) -> int:
    """Materialize explicit detector rows for a saved symbol winner config."""
    symbol = str(_object_get(symbol_config, "symbol", "") or "").strip()
    regimes = _object_get(symbol_config, "regime_configs", {}) or {}
    cells: Set[Tuple[str, str]] = set()
    if symbol and isinstance(regimes, dict):
        for timeframe, regime_map in regimes.items():
            if not isinstance(regime_map, dict):
                continue
            if any(_regime_payload_tradeable(cfg) for cfg in regime_map.values()):
                cells.add((symbol, str(timeframe or "").strip().upper()))
    return materialize_regime_params_for_cells(list(cells), filepath=filepath)


def load_regime_params(symbol: str, timeframe: str, 
                       filepath: str = "regime_params.json") -> RegimeParams:
    """
    Load regime parameters for a symbol/timeframe.
    
    Priority:
    1. regime_params.json (symbol -> timeframe -> params)
    2. DEFAULT_PARAMS_BY_TIMEFRAME
    3. RegimeParams() defaults
    
    Args:
        symbol: Symbol name
        timeframe: Timeframe string
        filepath: Path to regime_params.json
        
    Returns:
        RegimeParams for the symbol/timeframe
    """
    _ensure_regime_params_loaded(filepath)

    # Try symbol + timeframe specific
    tuned = _find_tuned_regime_params(symbol, timeframe)
    if tuned is not None:
        tuned_key, cached = tuned
        # Respect explicit JSON thresholds; otherwise apply the asset-class default.
        if tuned_key in _REGIME_ADX_THRESHOLD_EXPLICIT:
            return cached
        return _with_asset_class_adx_threshold(cached, symbol)

    # Record fallback sources for the startup summary.
    fallback_key = (symbol, timeframe)
    if timeframe in DEFAULT_PARAMS_BY_TIMEFRAME:
        _record_regime_fallback(fallback_key, source="timeframe_default")
        return _with_asset_class_adx_threshold(
            DEFAULT_PARAMS_BY_TIMEFRAME[timeframe], symbol
        )

    _record_regime_fallback(fallback_key, source="hardcoded_default")
    return _with_asset_class_adx_threshold(RegimeParams(), symbol)


def _with_asset_class_adx_threshold(base: RegimeParams, symbol: str) -> RegimeParams:
    """Return a copy of `base` with the asset-class ADX threshold applied."""
    target = _resolve_adx_trend_threshold(symbol)
    if float(base.adx_trend_threshold) == float(target):
        return base
    # RegimeParams is a plain dataclass — copy via replace().
    from dataclasses import replace
    return replace(base, adx_trend_threshold=target)


def _record_regime_fallback(key: Tuple[str, str], source: str) -> None:
    """Log INFO once per (symbol, timeframe) fallback and stash for the startup summary."""
    if key in _REGIME_FALLBACK_LOGGED:
        return
    _REGIME_FALLBACK_LOGGED.add(key)
    _REGIME_FALLBACK_RECORD[key] = source
    symbol, timeframe = key
    logger.info(
        "regime_params fallback for %s/%s -> %s (no tuned entry; using defaults)",
        symbol, timeframe, source,
    )


def get_regime_fallback_record() -> Dict[Tuple[str, str], str]:
    """Snapshot of (symbol, timeframe) → fallback source for the startup summary."""
    return dict(_REGIME_FALLBACK_RECORD)


def clear_regime_fallback_log() -> None:
    """Reset the fallback log (used by tests)."""
    _REGIME_FALLBACK_LOGGED.clear()
    _REGIME_FALLBACK_RECORD.clear()


def save_regime_params(params_dict: Dict[str, Dict[str, RegimeParams]], 
                       filepath: str = "regime_params.json"):
    """Save regime parameters to JSON file."""
    output = {}
    for symbol, tf_dict in params_dict.items():
        output[symbol] = {}
        for tf, params in tf_dict.items():
            output[symbol][tf] = params.to_dict()
    
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    
    logger.info(f"Saved regime params to {filepath}")


def clear_regime_params_cache():
    """Clear the cached regime parameters (useful for reloading)."""
    global _REGIME_PARAMS_CACHE, _REGIME_PARAMS_LOADED, _REGIME_PARAMS_LOADED_PATH
    _REGIME_PARAMS_CACHE = {}
    _REGIME_PARAMS_LOADED = False
    _REGIME_PARAMS_LOADED_PATH = None
    _REGIME_ADX_THRESHOLD_EXPLICIT.clear()
    _REGIME_SOURCE_DIGEST_CACHE.clear()
    clear_regime_fallback_log()


# =============================================================================
# REGIME DETECTOR
# =============================================================================

class MarketRegimeDetector:
    """
    Market regime detector with hysteresis.
    
    Computes 4 regime scores (TREND, RANGE, BREAKOUT, CHOP) and applies
    a state machine with confirmation delay to prevent rapid flipping.
    
    All computations are causal (no lookahead).
    """
    
    def __init__(self, params: Optional[RegimeParams] = None):
        """
        Initialize detector.

        Args:
            params: Regime parameters (uses defaults if None)
        """
        self.params = params or RegimeParams()

    @property
    def warmup_bars(self) -> int:
        """Number of bars required for warmup before regime scores are meaningful."""
        return max(self.params.bb_squeeze_lookback, self.params.atr_lookback, 50)
    
    def compute_regime_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute regime scores and labels for all bars.
        
        This is the main entry point. Requires a DataFrame with at minimum
        Open, High, Low, Close columns. If ADX/ATR are already computed,
        they will be used; otherwise they are computed here.
        
        Args:
            df: DataFrame with OHLCV data (and optionally precomputed indicators)
            
        Returns:
            DataFrame with columns:
            - TREND_SCORE, RANGE_SCORE, BREAKOUT_SCORE, CHOP_SCORE (0-1)
            - REGIME_RAW (highest score regime, no hysteresis)
            - REGIME (with hysteresis applied)
            - REGIME_STRENGTH (top score)
            - REGIME_GAP (top - second)
            - REGIME_LIVE (shifted by 1 for decision-time parity)
            - REGIME_STRENGTH_LIVE (shifted by 1)
        """
        n = len(df)
        p = self.params
        
        if n < max(p.bb_squeeze_lookback, p.atr_lookback, 50) + 10:
            # Not enough data for meaningful regime detection
            return self._create_empty_regime_df(df.index)
        
        # Initialize output arrays
        trend_scores = np.zeros(n)
        range_scores = np.zeros(n)
        breakout_scores = np.zeros(n)
        chop_scores = np.zeros(n)
        
        # =====================================================================
        # COMPUTE RAW INDICATOR VALUES
        # =====================================================================
        
        # Seed ADX warmup NaNs at the local trend anchor.
        adx_threshold = float(getattr(p, 'adx_trend_threshold', 25.0))
        if 'ADX' in df.columns:
            adx = df['ADX'].values.copy()
            adx = np.nan_to_num(adx, nan=adx_threshold)
        else:
            adx = self._compute_adx(df, p.adx_period)
        
        # ATR (use precomputed if available)
        atr_col = f'ATR_{p.atr_period}'
        if atr_col in df.columns:
            atr = df[atr_col].values.copy()
            atr = np.nan_to_num(atr, nan=0.0)
        elif 'ATR_14' in df.columns:
            atr = df['ATR_14'].values.copy()
            atr = np.nan_to_num(atr, nan=0.0)
        else:
            atr = self._compute_atr(df, p.atr_period)
        
        # Bollinger Band width
        bb_width = self._compute_bb_width(df, p.bb_period, p.bb_std)
        
        # Directional efficiency
        efficiency = self._compute_directional_efficiency(df, p.efficiency_period)
        
        # MA slope consistency
        slope_consistency = self._compute_slope_consistency(df, period=20)
        
        # Band containment (for RANGE)
        containment = self._compute_band_containment(df, p.bb_period, p.bb_std)
        
        # BB squeeze detection (adaptive percentile)
        bb_squeeze = self._compute_bb_squeeze(bb_width, p.bb_squeeze_lookback, p.bb_squeeze_percentile)
        
        # ATR percentile (for BREAKOUT)
        atr_percentile = self._compute_atr_percentile(atr, p.atr_lookback)
        
        # Structure break detection
        structure_break = self._compute_structure_break(df, atr, p.structure_period, p.structure_atr_mult)
        
        # Whipsaw/wickiness (for CHOP)
        whipsaw = self._compute_whipsaw(df, period=10)
        
        # Direction flip frequency (for CHOP)
        direction_flips = self._compute_direction_flips(df, period=10)
        
        # =====================================================================
        # COMPUTE REGIME SCORES (0-1) - VECTORIZED
        # =====================================================================
        
        warmup = max(p.bb_squeeze_lookback, p.atr_lookback, 50)
        
        # Anchor both curves at the local ADX threshold.
        adx_norm = self._normalize_adx_vectorized(adx, adx_threshold)
        adx_mid = self._normalize_adx_mid_vectorized(adx, adx_threshold)
        squeeze_release = self._compute_squeeze_release_vectorized(bb_squeeze)
        
        # Vectorized score computation (warmup region stays zero)
        # --- TREND SCORE ---
        trend_scores[warmup:] = (
            p.trend_adx_weight * adx_norm[warmup:] +
            p.trend_efficiency_weight * efficiency[warmup:] +
            p.trend_slope_weight * slope_consistency[warmup:]
        )
        
        # --- RANGE SCORE ---
        range_scores[warmup:] = (
            p.range_adx_weight * (1.0 - adx_norm[warmup:]) +
            p.range_efficiency_weight * (1.0 - efficiency[warmup:]) +
            p.range_containment_weight * containment[warmup:]
        )
        
        # --- BREAKOUT SCORE ---
        breakout_scores[warmup:] = (
            p.breakout_squeeze_weight * squeeze_release[warmup:] +
            p.breakout_atr_weight * atr_percentile[warmup:] +
            p.breakout_structure_weight * structure_break[warmup:]
        )
        
        # --- CHOP SCORE ---
        chop_scores[warmup:] = (
            p.chop_adx_weight * adx_mid[warmup:] +
            p.chop_efficiency_weight * (1.0 - efficiency[warmup:]) +
            p.chop_whipsaw_weight * (whipsaw[warmup:] * 0.5 + direction_flips[warmup:] * 0.5)
        )
        
        # Vectorized normalization to sum to 1
        total_scores = trend_scores + range_scores + breakout_scores + chop_scores
        # Avoid division by zero
        valid_mask = total_scores > 1e-10
        
        trend_scores[valid_mask] /= total_scores[valid_mask]
        range_scores[valid_mask] /= total_scores[valid_mask]
        breakout_scores[valid_mask] /= total_scores[valid_mask]
        chop_scores[valid_mask] /= total_scores[valid_mask]
        
        # Set defaults for invalid regions
        invalid_mask = ~valid_mask
        trend_scores[invalid_mask] = 0.25
        range_scores[invalid_mask] = 0.25
        breakout_scores[invalid_mask] = 0.25
        chop_scores[invalid_mask] = 0.25
        
        # =====================================================================
        # DETERMINE RAW REGIME (highest score)
        # =====================================================================
        
        regime_raw = [None] * n
        regime_strength = np.zeros(n)
        regime_gap = np.zeros(n)
        
        for i in range(n):
            if i < warmup:
                regime_raw[i] = RegimeType.CHOP  # Default during warmup
                regime_strength[i] = 0.25
                regime_gap[i] = 0.0
                continue
            
            scores = {
                RegimeType.TREND: trend_scores[i],
                RegimeType.RANGE: range_scores[i],
                RegimeType.BREAKOUT: breakout_scores[i],
                RegimeType.CHOP: chop_scores[i]
            }
            
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            top_regime, top_score = sorted_scores[0]
            second_score = sorted_scores[1][1]
            
            regime_raw[i] = top_regime
            regime_strength[i] = top_score
            regime_gap[i] = top_score - second_score
        
        # =====================================================================
        # APPLY HYSTERESIS STATE MACHINE
        # =====================================================================
        
        regime_final = self._apply_hysteresis(
            regime_raw, regime_strength, regime_gap,
            p.k_confirm, p.gap_min, p.k_hold
        )
        
        # =====================================================================
        # BUILD OUTPUT DATAFRAME
        # =====================================================================
        
        result = pd.DataFrame(index=df.index)
        result['TREND_SCORE'] = trend_scores
        result['RANGE_SCORE'] = range_scores
        result['BREAKOUT_SCORE'] = breakout_scores
        result['CHOP_SCORE'] = chop_scores
        result['REGIME_RAW'] = regime_raw
        result['REGIME'] = regime_final
        result['REGIME_STRENGTH'] = regime_strength
        result['REGIME_GAP'] = regime_gap
        result['REGIME_WARMUP'] = False
        result.iloc[:warmup, result.columns.get_loc('REGIME_WARMUP')] = True

        # REGIME_LIVE = REGIME.shift(1) for decision-time parity
        result['REGIME_LIVE'] = result['REGIME'].shift(1)
        result['REGIME_STRENGTH_LIVE'] = result['REGIME_STRENGTH'].shift(1)

        return result
    
    def _create_empty_regime_df(self, index: pd.Index) -> pd.DataFrame:
        """Create empty regime DataFrame for insufficient data."""
        n = len(index)
        result = pd.DataFrame(index=index)
        result['TREND_SCORE'] = 0.25
        result['RANGE_SCORE'] = 0.25
        result['BREAKOUT_SCORE'] = 0.25
        result['CHOP_SCORE'] = 0.25
        result['REGIME_RAW'] = RegimeType.CHOP
        result['REGIME'] = RegimeType.CHOP
        result['REGIME_STRENGTH'] = 0.25
        result['REGIME_GAP'] = 0.0
        result['REGIME_WARMUP'] = True  # All bars are warmup when data insufficient
        result['REGIME_LIVE'] = RegimeType.CHOP
        result['REGIME_STRENGTH_LIVE'] = 0.25
        return result
    
    # =========================================================================
    # INDICATOR COMPUTATION METHODS
    # =========================================================================
    
    def _compute_adx(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """Compute ADX using Wilder's smoothing."""
        high = df['High'].values
        low = df['Low'].values
        close = df['Close'].values
        n = len(df)
        
        tr = np.zeros(n)
        plus_dm = np.zeros(n)
        minus_dm = np.zeros(n)
        
        for i in range(1, n):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr[i] = max(hl, hc, lc)
            
            up_move = high[i] - high[i-1]
            down_move = low[i-1] - low[i]
            
            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
        
        # Wilder smoothing
        atr = self._wilder_smooth(tr, period)
        smoothed_plus_dm = self._wilder_smooth(plus_dm, period)
        smoothed_minus_dm = self._wilder_smooth(minus_dm, period)
        
        # Directional indicators
        plus_di = np.zeros(n)
        minus_di = np.zeros(n)
        dx = np.zeros(n)
        
        for i in range(period, n):
            if atr[i] > 1e-10:
                plus_di[i] = 100 * smoothed_plus_dm[i] / atr[i]
                minus_di[i] = 100 * smoothed_minus_dm[i] / atr[i]
            
            di_sum = plus_di[i] + minus_di[i]
            if di_sum > 1e-10:
                dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / di_sum
        
        adx = self._wilder_smooth(dx, period)
        
        return adx
    
    def _compute_atr(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """Compute ATR using Wilder's smoothing."""
        high = df['High'].values
        low = df['Low'].values
        close = df['Close'].values
        n = len(df)
        
        tr = np.zeros(n)
        for i in range(1, n):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr[i] = max(hl, hc, lc)
        
        return self._wilder_smooth(tr, period)
    
    def _wilder_smooth(self, data: np.ndarray, period: int) -> np.ndarray:
        """Wilder's smoothing (EMA with alpha = 1/period)."""
        n = len(data)
        result = np.zeros(n)
        alpha = 1.0 / period
        
        # Initialize with SMA
        if n >= period:
            result[period-1] = np.mean(data[:period])
            
            for i in range(period, n):
                result[i] = alpha * data[i] + (1 - alpha) * result[i-1]
        
        return result
    
    def _compute_bb_width(self, df: pd.DataFrame, period: int, std: float) -> np.ndarray:
        """Compute Bollinger Band width (normalized).
        
        Optimized using pandas rolling operations for 2-3x speedup.
        """
        close = df['Close']
        n = len(df)
        
        # Use pandas rolling which is highly optimized
        ma = close.rolling(window=period, min_periods=period).mean()
        sigma = close.rolling(window=period, min_periods=period).std(ddof=0)
        
        upper = ma + std * sigma
        lower = ma - std * sigma
        
        # Avoid division by zero
        width = np.where(ma > 1e-10, (upper - lower) / ma, 0.0)
        
        return width.values if hasattr(width, 'values') else np.array(width)
    
    def _compute_directional_efficiency(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """
        Compute directional efficiency ratio (vectorized).
        
        Efficiency = |net_move| / sum(|individual_moves|)
        High efficiency = trending, low efficiency = choppy
        
        Optimized with NumPy operations for 3-5x speedup over loop-based version.
        """
        close = df['Close'].values
        n = len(df)
        
        efficiency = np.zeros(n)
        
        if n <= period:
            return efficiency
        
        # Vectorized: compute price changes
        price_changes = np.abs(np.diff(close))
        
        # Rolling sum of absolute moves using cumsum trick
        cumsum_moves = np.cumsum(np.concatenate([[0], price_changes]))
        
        for i in range(period, n):
            net_move = abs(close[i] - close[i-period])
            sum_moves = cumsum_moves[i] - cumsum_moves[i-period]
            
            if sum_moves > 1e-10:
                efficiency[i] = net_move / sum_moves
        
        return efficiency
    
    def _compute_slope_consistency(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """
        Compute MA slope consistency (vectorized).
        
        Measures how consistent the direction of short-term price changes is.
        
        Optimized with NumPy operations for 3-5x speedup.
        """
        close = df['Close'].values
        n = len(df)
        
        consistency = np.zeros(n)
        
        if n <= period:
            return consistency
        
        # Vectorized: compute sign of changes
        changes = np.diff(close)
        positive_changes = (changes > 0).astype(np.float64)
        negative_changes = (changes < 0).astype(np.float64)
        
        # Rolling sums using cumsum
        cumsum_pos = np.cumsum(np.concatenate([[0], positive_changes]))
        cumsum_neg = np.cumsum(np.concatenate([[0], negative_changes]))
        
        for i in range(period, n):
            positive = cumsum_pos[i] - cumsum_pos[i-period]
            negative = cumsum_neg[i] - cumsum_neg[i-period]
            consistency[i] = max(positive, negative) / period
        
        return consistency
    
    def _compute_band_containment(self, df: pd.DataFrame, period: int, std: float) -> np.ndarray:
        """
        Compute band containment score.
        
        Measures how well price stays within Bollinger Bands.
        """
        close = df['Close'].values
        high = df['High'].values
        low = df['Low'].values
        n = len(df)
        
        containment = np.zeros(n)
        lookback = 10
        
        for i in range(max(period, lookback), n):
            window = close[i-period+1:i+1]
            ma = np.mean(window)
            sigma = np.std(window, ddof=0)
            
            upper = ma + std * sigma
            lower = ma - std * sigma
            
            if sigma > 1e-10:
                contained = 0
                for j in range(i-lookback+1, i+1):
                    if high[j] <= upper and low[j] >= lower:
                        contained += 1
                
                containment[i] = contained / lookback
        
        return containment
    
    def _compute_bb_squeeze(self, bb_width: np.ndarray, lookback: int, 
                            percentile: float) -> np.ndarray:
        """
        Detect Bollinger Band squeeze using adaptive percentile.
        
        Returns score 0-1 where 1.0 = in squeeze.
        """
        n = len(bb_width)
        squeeze = np.zeros(n)
        
        for i in range(lookback, n):
            window = bb_width[max(0, i-lookback+1):i+1]
            window = window[window > 0]  # Filter zeros
            
            if len(window) > 10:
                threshold = np.percentile(window, percentile)
                
                if bb_width[i] <= threshold and bb_width[i] > 0:
                    squeeze[i] = 1.0
                elif bb_width[i] > 0:
                    ratio = threshold / bb_width[i]
                    squeeze[i] = max(0.0, min(1.0, ratio))
        
        return squeeze
    
    def _compute_squeeze_release(self, squeeze: np.ndarray, idx: int) -> float:
        """
        Detect squeeze release (transition from squeeze to expansion).
        
        Returns score 0-1 based on recent squeeze and current expansion.
        """
        if idx < 5:
            return 0.0
        
        # Was in squeeze recently (last 5 bars)?
        recent_squeeze = np.max(squeeze[max(0, idx-5):idx])
        
        # Currently NOT in squeeze
        current_squeeze = squeeze[idx]
        
        # Release = was squeezed AND now expanding
        if recent_squeeze > 0.5 and current_squeeze < 0.5:
            return min(1.0, recent_squeeze * (1.0 - current_squeeze) * 2)
        
        return 0.0
    
    def _compute_atr_percentile(self, atr: np.ndarray, lookback: int) -> np.ndarray:
        """
        Compute ATR percentile (0-1) relative to recent history.
        
        High percentile = volatility expanding.
        """
        n = len(atr)
        percentile = np.zeros(n)
        
        for i in range(lookback, n):
            # Exclude current bar from percentile window to avoid self-reference bias
            window = atr[max(0, i-lookback+1):i]
            window = window[window > 0]

            if len(window) > 10 and atr[i] > 0:
                rank = np.sum(window <= atr[i]) / len(window)
                percentile[i] = rank

        return percentile
    
    def _compute_structure_break(self, df: pd.DataFrame, atr: np.ndarray,
                                  period: int, atr_mult: float) -> np.ndarray:
        """
        Detect structure break (price beyond recent high/low + ATR margin).
        
        Returns 1.0 for breakout, 0.0 for contained.
        
        Uses Numba JIT compilation when available for ~5x speedup.
        """
        close = df['Close'].values.astype(np.float64)
        high = df['High'].values.astype(np.float64)
        low = df['Low'].values.astype(np.float64)
        atr_arr = atr.astype(np.float64)
        
        if NUMBA_AVAILABLE:
            return _compute_structure_break_numba(high, low, close, atr_arr, period, atr_mult)
        
        # Fallback: Pure Python implementation
        n = len(df)
        structure_break = np.zeros(n)
        
        for i in range(period, n):
            recent_high = np.max(high[i-period:i])
            recent_low = np.min(low[i-period:i])
            margin = atr_arr[i] * atr_mult if atr_arr[i] > 0 else 0
            
            if close[i] > recent_high + margin:
                structure_break[i] = 1.0
            elif close[i] < recent_low - margin:
                structure_break[i] = 1.0
            elif margin > 0:
                # Partial score based on proximity
                dist_high = (close[i] - recent_high) / (margin + 1e-10)
                dist_low = (recent_low - close[i]) / (margin + 1e-10)
                structure_break[i] = max(0.0, min(1.0, max(dist_high, dist_low)))
        
        return structure_break
    
    def _compute_whipsaw(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """
        Compute whipsaw/wickiness score.
        
        High wickiness = large wicks relative to body = choppy price action.
        
        Uses Numba JIT compilation when available for ~5x speedup.
        """
        open_p = df['Open'].values.astype(np.float64)
        high = df['High'].values.astype(np.float64)
        low = df['Low'].values.astype(np.float64)
        close = df['Close'].values.astype(np.float64)
        
        if NUMBA_AVAILABLE:
            return _compute_whipsaw_numba(open_p, high, low, close, period)
        
        # Fallback: Pure Python implementation
        n = len(df)
        whipsaw = np.zeros(n)
        
        for i in range(period, n):
            total_wick = 0.0
            total_range = 0.0
            
            for j in range(i-period+1, i+1):
                bar_range = high[j] - low[j]
                
                if bar_range > 1e-10:
                    upper_wick = high[j] - max(open_p[j], close[j])
                    lower_wick = min(open_p[j], close[j]) - low[j]
                    total_wick += upper_wick + lower_wick
                    total_range += bar_range
            
            if total_range > 1e-10:
                whipsaw[i] = total_wick / total_range
        
        return whipsaw
    
    def _compute_direction_flips(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """
        Compute direction flip frequency.
        
        High flip frequency = choppy, indecisive market.
        
        Uses Numba JIT compilation when available for ~5x speedup.
        """
        close = df['Close'].values.astype(np.float64)
        
        if NUMBA_AVAILABLE:
            return _compute_direction_flips_numba(close, period)
        
        # Fallback: Pure Python implementation
        n = len(df)
        flips = np.zeros(n)
        
        for i in range(period + 1, n):
            flip_count = 0
            for j in range(i-period+1, i):
                prev_change = close[j] - close[j-1]
                curr_change = close[j+1] - close[j]
                
                prev_dir = 1 if prev_change > 0 else (-1 if prev_change < 0 else 0)
                curr_dir = 1 if curr_change > 0 else (-1 if curr_change < 0 else 0)
                
                if prev_dir != 0 and curr_dir != 0 and prev_dir != curr_dir:
                    flip_count += 1
            
            flips[i] = flip_count / max(1, period - 1)
        
        return flips
    
    # =========================================================================
    # NORMALIZATION HELPERS
    # =========================================================================
    
    def _normalize_adx(self, adx_value: float, threshold: float = 25.0) -> float:
        """
        Normalize ADX to 0-1 range, anchored at `threshold`.

        Knees are defined in terms of the trend anchor so the curve re-scales
        for asset classes whose natural trend floor differs from the classic
        Wilder 25. At threshold=25 the knees collapse to the original 20/40
        bands, preserving legacy behavior for FX / indices.

        Knees (as multiples of threshold):
            adx < 0.8·T  → weak (0.0 → 0.5)
            adx < 1.6·T  → moderate (0.5 → 1.0)
            adx ≥ 1.6·T  → strong (0.75 → 1.0, saturates at 2.4·T)
        """
        low = 0.8 * threshold
        high = 1.6 * threshold
        tail = 3.2 * threshold
        if adx_value < low:
            return adx_value / high
        elif adx_value < high:
            return 0.5 + (adx_value - low) / high
        else:
            return min(1.0, 0.75 + (adx_value - high) / tail)

    def _normalize_adx_vectorized(self, adx: np.ndarray, threshold: float = 25.0) -> np.ndarray:
        """Vectorized ADX normalization anchored at `threshold`."""
        low = 0.8 * threshold
        high = 1.6 * threshold
        tail = 3.2 * threshold
        result = np.zeros_like(adx, dtype=float)

        # adx < 0.8·T
        mask1 = adx < low
        result[mask1] = adx[mask1] / high

        # 0.8·T <= adx < 1.6·T
        mask2 = (adx >= low) & (adx < high)
        result[mask2] = 0.5 + (adx[mask2] - low) / high

        # adx >= 1.6·T
        mask3 = adx >= high
        result[mask3] = np.minimum(1.0, 0.75 + (adx[mask3] - high) / tail)

        return result

    def _normalize_adx_mid(self, adx_value: float, threshold: float = 25.0) -> float:
        """
        Normalize ADX for CHOP detection, anchored at `threshold`.

        Peaks at `0.9·T` (mid of the 0.6·T — 1.2·T band) — the regime where
        ADX shows meaningful movement but no clear direction. At threshold=25
        this collapses to the original 15 / 22.5 / 30 knees.
        """
        low_mid = 0.6 * threshold
        peak = 0.9 * threshold
        high_mid = 1.2 * threshold
        tail_mid = 1.6 * threshold
        if adx_value < low_mid:
            return adx_value / high_mid
        elif adx_value < high_mid:
            return 0.5 + (high_mid - abs(adx_value - peak)) / high_mid
        else:
            return max(0.0, 1.0 - (adx_value - high_mid) / tail_mid)

    def _normalize_adx_mid_vectorized(self, adx: np.ndarray, threshold: float = 25.0) -> np.ndarray:
        """Vectorized ADX mid-range normalization anchored at `threshold`."""
        low_mid = 0.6 * threshold
        peak = 0.9 * threshold
        high_mid = 1.2 * threshold
        tail_mid = 1.6 * threshold
        result = np.zeros_like(adx, dtype=float)

        # adx < 0.6·T
        mask1 = adx < low_mid
        result[mask1] = adx[mask1] / high_mid

        # 0.6·T <= adx < 1.2·T
        mask2 = (adx >= low_mid) & (adx < high_mid)
        result[mask2] = 0.5 + (high_mid - np.abs(adx[mask2] - peak)) / high_mid

        # adx >= 1.2·T
        mask3 = adx >= high_mid
        result[mask3] = np.maximum(0.0, 1.0 - (adx[mask3] - high_mid) / tail_mid)

        return result
    
    def _compute_squeeze_release_vectorized(self, squeeze: np.ndarray) -> np.ndarray:
        """Vectorized squeeze release computation."""
        n = len(squeeze)
        release = np.zeros(n)
        
        if n < 5:
            return release
        
        # For each position, compute max of last 5 bars using rolling
        for i in range(5, n):
            recent_squeeze = np.max(squeeze[i-5:i])
            current_squeeze = squeeze[i]
            
            if recent_squeeze > 0.5 and current_squeeze < 0.5:
                release[i] = min(1.0, recent_squeeze * (1.0 - current_squeeze) * 2)
        
        return release
    
    # =========================================================================
    # HYSTERESIS STATE MACHINE
    # =========================================================================
    
    def _apply_hysteresis(self, regime_raw: List[str], 
                          strength: np.ndarray, gap: np.ndarray,
                          k_confirm: int, gap_min: float, k_hold: int) -> List[str]:
        """
        Apply hysteresis to regime labels.
        
        Rules:
        1. Require k_confirm consecutive bars of new regime before switching
        2. Require gap >= gap_min before switching
        3. Hold current regime for at least k_hold bars
        
        Uses Numba JIT compilation when available for ~10x speedup.
        
        Args:
            regime_raw: Raw regime labels (highest score)
            strength: Regime strength scores
            gap: Gap between top and second regime
            k_confirm: Bars required to confirm switch
            gap_min: Minimum gap required to switch
            k_hold: Minimum bars to hold regime
            
        Returns:
            Regime labels with hysteresis applied
        """
        n = len(regime_raw)
        
        if n == 0:
            return [None] * n
        
        if NUMBA_AVAILABLE:
            # Convert string regimes to integers for Numba
            regime_raw_int = np.array([
                RegimeType.to_int(r) if r else 3 for r in regime_raw
            ], dtype=np.int32)
            
            gap_arr = gap.astype(np.float64)
            
            # Call Numba JIT-compiled function
            regime_final_int = _hysteresis_loop_numba(
                regime_raw_int, gap_arr, k_confirm, gap_min, k_hold
            )
            
            # Convert back to strings
            return [RegimeType.from_int(i) for i in regime_final_int]
        
        # Fallback: Pure Python implementation
        regime_final = [None] * n
        
        # State tracking
        current_regime = regime_raw[0]
        bars_in_current = 0
        candidate_regime = None
        candidate_count = 0
        
        for i in range(n):
            raw = regime_raw[i]
            
            # First bar or None: initialize
            if current_regime is None:
                current_regime = raw if raw else RegimeType.CHOP
                bars_in_current = 1
                regime_final[i] = current_regime
                continue
            
            bars_in_current += 1
            
            # Check if raw regime is different from current
            if raw != current_regime and raw is not None:
                # Check if gap is sufficient
                if gap[i] >= gap_min:
                    # Track candidate
                    if raw == candidate_regime:
                        candidate_count += 1
                    else:
                        candidate_regime = raw
                        candidate_count = 1
                    
                    # Check if confirmed (k_confirm bars + k_hold elapsed)
                    if candidate_count >= k_confirm and bars_in_current >= k_hold:
                        # Switch regime
                        current_regime = candidate_regime
                        bars_in_current = 1
                        candidate_regime = None
                        candidate_count = 0
                else:
                    # Gap too small, reset candidate
                    candidate_regime = None
                    candidate_count = 0
            else:
                # Same as current, reset candidate
                candidate_regime = None
                candidate_count = 0
            
            regime_final[i] = current_regime
        
        return regime_final


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def compute_regime_for_features(df: pd.DataFrame, 
                                 symbol: str = "", 
                                 timeframe: str = "H1",
                                 params_file: str = "regime_params.json") -> pd.DataFrame:
    """
    Compute regime features for a DataFrame.
    
    This is the main entry point for adding regime columns to a feature DataFrame.
    It loads symbol/timeframe-specific parameters if available.
    
    Args:
        df: DataFrame with OHLCV data (and optionally indicators)
        symbol: Symbol name (for parameter lookup)
        timeframe: Timeframe string (for parameter lookup)
        params_file: Path to regime_params.json
        
    Returns:
        DataFrame with all original columns plus regime columns
    """
    # Load params for this symbol/timeframe
    params = load_regime_params(symbol, timeframe, params_file)
    
    # Create detector and compute
    detector = MarketRegimeDetector(params)
    regime_df = detector.compute_regime_scores(df)
    
    # Merge regime columns into original df
    result = df.copy()
    for col in regime_df.columns:
        result[col] = regime_df[col]
    
    return result


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'RegimeType',
    'RegimeParams',
    'MarketRegimeDetector',
    'load_regime_params',
    'has_tuned_regime_params',
    'validate_regime_params_provenance',
    'regime_params_provenance_path',
    'regime_params_sha256',
    'REGIME_PROVENANCE_SCHEMA_VERSION',
    'REGIME_PROVENANCE_FIT_SCOPE',
    'validated_regime_params_for_timeframe',
    'materialize_regime_params_for_cells',
    'materialize_regime_params_from_symbol_config',
    'save_regime_params',
    'clear_regime_params_cache',
    'compute_regime_for_features',
    'DEFAULT_PARAMS_BY_TIMEFRAME',
]


if __name__ == "__main__":
    # Test regime detection
    logging.basicConfig(level=logging.INFO)
    
    print("Regime Detection Module")
    print(f"Regime types: {RegimeType.ALL}")
    
    # Test with synthetic data
    np.random.seed(42)
    n = 500
    
    # Generate trending data
    trend = np.cumsum(np.random.randn(n) * 0.5 + 0.1)
    
    df = pd.DataFrame({
        'Open': 100 + trend,
        'High': 100 + trend + np.random.rand(n) * 2,
        'Low': 100 + trend - np.random.rand(n) * 2,
        'Close': 100 + trend + np.random.randn(n) * 0.5,
        'Volume': np.random.randint(1000, 10000, n)
    }, index=pd.date_range('2024-01-01', periods=n, freq='1h'))
    
    detector = MarketRegimeDetector()
    result = detector.compute_regime_scores(df)
    
    print(f"\nSample regime detection (last 10 bars):")
    print(result[['REGIME', 'REGIME_STRENGTH', 'REGIME_GAP']].tail(10))
    
    print("\nRegime distribution:")
    print(result['REGIME'].value_counts())
    
    print("\npm_regime.py loaded successfully!")
